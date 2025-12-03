#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Pairs — Best (Session × w1..w2) per Pair — Expectancy Recap (1 ligne par paire)
+ Breakdown Pair × Jour de la semaine × TP (TP unique, TP1 et TP2 uniquement) au format CSV.

SESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI
TOKYO,INDEX,GBPCAD,TP2,Y,N,N,N,Y

RÈGLE (mise à jour) :
- On détermine, pour chaque paire, le meilleur moment (session) pour LANCER un trade.
- Par session, on prend AU PLUS 1 trade par (paire, jour) si l'entrée se produit dans la fenêtre de la session.
- Peu importe quand le trade se termine (SL/RR2), on NE bloque PAS le jour suivant (pas d'anti-overlap cross-day).

Entrées:
- Break strict, pullback antagoniste, entrée wick.
- SL = extrême (low/high) depuis le pullback (inclus).

Cibles:
- RR1 / RR2 uniquement. (TP1 = 1R, TP2 = 2R)
- STOP au premier SL ou RR2.

WIN/LOSS:
- WIN = TP1 avant SL.
- LOSS = SL avant TP1.

R-multiple:
- Partiels w1, w2 (somme = 1).
- 1R si TP1, 2R si TP2, sinon -1R sur le reliquat.

Sorties:
- Recap final : meilleure combinaison pour chaque paire.
- Breakdown Pair × Jour × TP (TP1 / TP2 uniquement).
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext
from zoneinfo import ZoneInfo

UTC = timezone.utc
WEEKDAYS = ["MON","TUE","WED","THU","FRI","SAT","SUN"]
LONDON_TZ = ZoneInfo("Europe/London")

# ---------------- ENV / DB ----------------
load_dotenv()
PG_HOST     = os.getenv("PG_HOST","127.0.0.1")
PG_PORT     = int(os.getenv("PG_PORT","5432"))
PG_DB       = os.getenv("PG_DB","postgres")
PG_USER     = os.getenv("PG_USER","postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD","postgres")
PG_SSLMODE  = os.getenv("PG_SSLMODE","disable")

def get_pg_conn():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} sslmode={PG_SSLMODE}"
    conn = psycopg2.connect(dsn)
    conn.set_isolation_level(pg_ext.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

# ---------------- Time utils ----------------
def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000,tz=UTC).isoformat(timespec="seconds").replace("+00:00","Z")

def parse_date(d: str) -> date:
    return datetime.strptime(d,"%Y-%m-%d").date()

def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)

def day_ms_bounds(d: date) -> Tuple[int,int]:
    start = datetime(d.year,d.month,d.day,0,0,tzinfo=UTC)
    end   = start + timedelta(days=1)
    return int(start.timestamp()*1000), int(end.timestamp()*1000)

# ---- Sessions (fenêtres UTC) ----
def tokyo_signal_window(d: date) -> Tuple[int,int]:
    base = datetime(d.year,d.month,d.day,tzinfo=UTC)
    start = int((base+timedelta(hours=1)).timestamp()*1000)               # 01:00
    end   = int((base+timedelta(hours=5,minutes=45)).timestamp()*1000)   # 05:45
    return start,end

def london_signal_window(d: date) -> Tuple[int,int]:
    local_start = datetime(d.year,d.month,d.day,8,0,tzinfo=LONDON_TZ)
    local_end   = datetime(d.year,d.month,d.day,12,45,tzinfo=LONDON_TZ)
    start_utc = local_start.astimezone(UTC)
    end_utc   = local_end.astimezone(UTC)
    return int(start_utc.timestamp()*1000), int(end_utc.timestamp()*1000)

def ny_signal_window(d: date) -> Tuple[int,int]:
    base = datetime(d.year,d.month,d.day,tzinfo=UTC)
    start = int((base+timedelta(hours=13)).timestamp()*1000)
    end   = int((base+timedelta(hours=17,minutes=45)).timestamp()*1000)
    return start,end

def window_for_session(session: str, d: date) -> Tuple[int,int]:
    s = (session or "").strip().upper()
    if s=="TOKYO":  return tokyo_signal_window(d)
    if s=="LONDON": return london_signal_window(d)
    if s in ("NY","NEWYORK","NEW_YORK"): return ny_signal_window(d)
    return tokyo_signal_window(d)

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+","_",pair.lower()).strip("_")

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    core = pair.upper().split(".")[0]
    return 0.001 if core.endswith("JPY") else 0.00001

def pip_size_for(pair: str) -> float:
    core = pair.upper().split(".")[0]
    if core.startswith("XAU"):
        return 0.01
    return 0.01 if core.endswith("JPY") else 0.0001

def infer_type(pair: str) -> str:
    up = pair.upper()
    if up.startswith("XAU") or up.startswith("XAG") or up.startswith("XPT") or up.startswith("XPD"):
        return "METAL"
    index_symbols = {
        "NAS100","US30","US500","SPX500","GER40","UK100","FRA40",
        "JPN225","JP225","HK50"
    }
    if up in index_symbols:
        return "INDEX"
    crypto_symbols = {
        "BTCUSD","ETHUSD","LTCUSD","XRPUSD","ADAUSD","SOLUSD"
    }
    if up in crypto_symbols:
        return "CRYPTO"
    return "FOREX"

# ---------------- DB Readers ----------------
def read_first_1h(conn,pair: str,d: date) -> Optional[Dict]:
    t1h = table_name(pair,"1h")
    day_start,_ = day_ms_bounds(d)
    sql = f"SELECT ts,open,high,low,close FROM {t1h} WHERE ts=%s LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql,(day_start,))
            row = cur.fetchone()
            if not row: return None
            ts,o,h,l,c = row
            return {"ts":int(ts),"open":float(o),"high":float(h),"low":float(l),"close":float(c)}
    except Exception:
        conn.rollback(); return None

def read_15m_in(conn,pair: str,start_ms: int,end_ms: int) -> List[Dict]:
    t15 = table_name(pair,"15m")
    sql = f"""
        SELECT ts,open,high,low,close
        FROM {t15}
        WHERE ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql,(start_ms,end_ms))
            rows = cur.fetchall()
            return [{"ts":int(ts),"open":float(o),"high":float(h),"low":float(l),"close":float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

def read_15m_from(conn,pair: str,start_ms: int) -> List[Dict]:
    t15 = table_name(pair,"15m")
    sql = f"""
        SELECT ts,open,high,low,close
        FROM {t15}
        WHERE ts > %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql,(start_ms,))
            rows = cur.fetchall()
            return [{"ts":int(ts),"open":float(o),"high":float(h),"low":float(l),"close":float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

# ---------------- FSM / Trade ----------------
@dataclass
class Trade:
    side: str
    entry_ts: int
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    long_active=False
    long_hh=None
    long_pullback_idx=None
    long_min_low_since_pullback=None

    short_active=False
    short_ll=None
    short_pullback_idx=None
    short_max_high_since_pullback=None

    for i,b in enumerate(c15):
        ts,o,h,l,c = b["ts"],b["open"],b["high"],b["low"],b["close"]

        if (not long_active) and (c > range_high):
            long_active=True
            long_hh=h
            long_pullback_idx=None
            long_min_low_since_pullback=None

        if (not short_active) and (c < range_low):
            short_active=True
            short_ll=l
            short_pullback_idx=None
            short_max_high_since_pullback=None

        # -------- LONG --------
        if long_active:
            prev_hh = long_hh
            # pullback antagoniste
            if long_pullback_idx is None and (c < o):
                long_pullback_idx=i
                long_min_low_since_pullback=l
            if long_pullback_idx is not None and i>=1:
                prev_low = c15[i-1]["low"]
                long_min_low_since_pullback = prev_low if long_min_low_since_pullback is None else min(long_min_low_since_pullback,prev_low)
            # wick trigger
            if (prev_hh is not None) and (long_pullback_idx is not None) and (i>long_pullback_idx) and (h > prev_hh) and (i>=1):
                entry_price = prev_hh
                sl_price = long_min_low_since_pullback if long_min_low_since_pullback is not None else c15[i-1]["low"]
                return Trade("LONG",ts,entry_price,sl_price)
            if (long_hh is None) or (h > long_hh):
                long_hh=h

        # -------- SHORT --------
        if short_active:
            prev_ll = short_ll
            if short_pullback_idx is None and (c > o):
                short_pullback_idx=i
                short_max_high_since_pullback=h
            if short_pullback_idx is not None and i>=1:
                prev_high = c15[i-1]["high"]
                short_max_high_since_pullback = prev_high if short_max_high_since_pullback is None else max(short_max_high_since_pullback,prev_high)
            if (prev_ll is not None) and (short_pullback_idx is not None) and (i>short_pullback_idx) and (l < prev_ll) and (i>=1):
                entry_price = prev_ll
                sl_price = short_max_high_since_pullback if short_max_high_since_pullback is not None else c15[i-1]["high"]
                return Trade("SHORT",ts,entry_price,sl_price)
            if (short_ll is None) or (l < short_ll):
                short_ll=l

    return None

# ---------------- After-entry evaluation — RR1 / RR2 only ----------------
def evaluate_trade_after_entry(conn,pair: str,tr: Trade):
    eps = pip_eps_for(pair)
    entry,sl = tr.entry,tr.sl
    r = abs(entry - sl)
    if r<=0:
        targets = {"RR1":entry,"RR2":entry}
        results = {"RR1":"SL","RR2":"SL"}
        return targets,results,{"SL":None,"RR1":None,"RR2":None},None

    if tr.side=="LONG":
        t1 = entry + 1.0*r
        t2 = entry + 2.0*r
    else:
        t1 = entry - 1.0*r
        t2 = entry - 2.0*r

    targets={"RR1":t1,"RR2":t2}
    hit_time={"SL":None,"RR1":None,"RR2":None}

    future = read_15m_from(conn,pair,tr.entry_ts)
    for b in future:
        ts,h,l = b["ts"],b["high"],b["low"]

        if tr.side=="LONG":
            sl_hit  = (l <= sl+eps)
            rr1_hit = (h >= t1-eps)
            rr2_hit = (h >= t2-eps)
        else:
            sl_hit  = (h >= sl-eps)
            rr1_hit = (l <= t1+eps)
            rr2_hit = (l <= t2+eps)

        if hit_time["SL"] is None and sl_hit:  hit_time["SL"]=ts
        if hit_time["RR1"] is None and rr1_hit: hit_time["RR1"]=ts
        if hit_time["RR2"] is None and rr2_hit: hit_time["RR2"]=ts

        # STOP au premier SL ou RR2
        if (hit_time["SL"] is not None) or (hit_time["RR2"] is not None):
            break

    results={}
    sl_time = hit_time["SL"]
    for key in ["RR1","RR2"]:
        ttime = hit_time[key]
        results[key] = "TP" if (ttime is not None and (sl_time is None or ttime < sl_time)) else "SL"

    closed_ts = sl_time if sl_time is not None else hit_time["RR2"]
    return targets,results,hit_time,closed_ts

# ---------------- Partials (w1,w2) -> R-multiple ----------------
def compute_r_and_close(hit_time: Dict[str, Optional[int]],
                        w1: float, w2: float) -> float:
    """
    Partiels TP1 / TP2 uniquement :
      TP1: +w1 * 1R
      TP2: +w2 * 2R
      SL : -1R sur le reliquat
    """
    t_sl = hit_time.get("SL")
    t1   = hit_time.get("RR1")
    t2   = hit_time.get("RR2")

    events: List[Tuple[int,str]] = []
    if t1 is not None: events.append((t1,"TP1"))
    if t2 is not None: events.append((t2,"TP2"))
    if t_sl is not None: events.append((t_sl,"SL"))
    events.sort(key=lambda x: x[0])

    rem = 1.0
    r   = 0.0

    for ts,ev in events:
        if ev=="TP1" and w1>0:
            r   += w1*1.0
            rem -= w1
            if rem<=1e-12: break
        elif ev=="TP2" and w2>0:
            r   += w2*2.0
            rem -= w2
            if rem<=1e-12: break
        elif ev=="SL":
            if rem>0:
                r += (-1.0)*rem
                rem = 0.0
            break

    return r

def reached_before(hits: Dict[str, Optional[int]], key: str) -> bool:
    t  = hits.get(key)
    sl = hits.get("SL")
    return t is not None and (sl is None or t < sl)


# ---------------- Core: générer les trades (inchangé) ----------------
@dataclass
class BareTrade:
    hits: Dict[str, Optional[int]]
    entry_ts: int

def collect_trades_for_session(conn,pair: str,start: date,end: date,session: str) -> List[BareTrade]:
    trades=[]
    block_until_ts=None

    for d in daterange(start,end):
        s,e = window_for_session(session,d)

        if block_until_ts is not None and s <= block_until_ts:
            continue

        c1 = read_first_1h(conn,pair,d)
        if not c1:
            continue
        rh,rl = c1["high"],c1["low"]

        c15 = read_15m_in(conn,pair,s,e)
        if not c15:
            continue

        tr = detect_first_trade_for_day(c15,rh,rl)
        if not tr:
            continue

        _,_,hits,closed_ts = evaluate_trade_after_entry(conn,pair,tr)
        trades.append(BareTrade(hits=hits, entry_ts=tr.entry_ts))

        block_until_ts = closed_ts if closed_ts is not None else (2**62)

    return trades


# ---------------- Grille des poids (TP1 + TP2 ONLY) ----------------
def weight_grid(step: float = 0.1):
    """
    Génère (w1, w2) avec w1 + w2 = 1
    """
    vals = [round(i*step,1) for i in range(int(1/step)+1)]
    for w1 in vals:
        w2 = round(1.0 - w1, 1)
        if 0.0 <= w2 <= 1.0:
            yield (w1, w2)


# ---------------- Stats pour une combinaison (TP1 + TP2) ----------------
def stats_for_weights(trades: List[BareTrade], w1: float, w2: float) -> Dict[str, Any]:
    total = len(trades)
    if total==0:
        return {
            "trades":0, "winrate":0.0,
            "avg_win":0.0, "avg_loss":0.0, "exp":0.0,
            "p1":0.0, "p2":0.0
        }

    r_wins=[]
    r_losses_abs=[]
    tp1_cnt=0
    tp2_cnt=0

    for bt in trades:
        hits = bt.hits
        if reached_before(hits,"RR1"): tp1_cnt += 1
        if reached_before(hits,"RR2"): tp2_cnt += 1

        r_mult = compute_r_and_close(hits,w1,w2)

        if reached_before(hits,"RR1"):
            r_wins.append(r_mult)
        else:
            r_losses_abs.append(-r_mult)

    wins = len(r_wins)
    losses = len(r_losses_abs)
    winrate = wins/total if total>0 else 0.0
    avg_win = (sum(r_wins)/wins) if wins>0 else 0.0
    avg_loss = (sum(r_losses_abs)/losses) if losses>0 else 0.0
    expectancy = winrate*avg_win - (1-winrate)*avg_loss

    p1 = tp1_cnt/total
    p2 = tp2_cnt/total

    return {
        "trades":total,
        "winrate":winrate,
        "avg_win":avg_win,
        "avg_loss":avg_loss,
        "exp":expectancy,
        "p1":p1, "p2":p2
    }


# ---------------- Breakdown Pair / Jour / TP (TP1–TP2 ONLY) ----------------
def build_breakdown_rows_for_pair(pair: str, session: str, trades: List[BareTrade]) -> List[Dict[str, Any]]:
    buckets = defaultdict(lambda: {"trades":0,"wins":0,"sum_r":0.0})
    valid_days=["MON","TUE","WED","THU","FRI"]

    for bt in trades:
        hits = bt.hits
        dt = datetime.fromtimestamp(bt.entry_ts/1000,tz=UTC)
        dow_idx = dt.weekday()
        dow_name = WEEKDAYS[dow_idx]
        if dow_name not in valid_days:
            continue

        t_sl = hits.get("SL")

        for tp_key,k,tp_label in [
            ("RR1",1,"TP1"),
            ("RR2",2,"TP2"),
        ]:
            bucket_key = (dow_name,tp_label)
            buckets[bucket_key]["trades"] += 1

            t_tp = hits.get(tp_key)
            if (t_tp is not None) and (t_sl is None or t_tp < t_sl):
                r=float(k)
                buckets[bucket_key]["wins"] += 1
            else:
                r=-1.0

            buckets[bucket_key]["sum_r"] += r

    rows=[]
    def sort_key(item):
        (dow_name,tp_label)=item[0]
        return (valid_days.index(dow_name), tp_label)

    for (dow_name,tp_label),agg in sorted(buckets.items(), key=sort_key):
        total=agg["trades"]
        if total==0: continue

        wins=agg["wins"]
        winrate=wins/total
        avg_r=agg["sum_r"]/total

        rows.append({
            "pair":pair,
            "session":session,
            "dow":dow_name,
            "tp":tp_label,
            "trades":total,
            "winrate":winrate,
            "expectancy":avg_r
        })

    return rows

# ---------------- Impression du recap final (1 ligne / paire) ----------------
def print_final_best_table(rows: List[Dict[str, Any]]):
    try:
        from prettytable import PrettyTable
    except Exception:
        PrettyTable=None

    rows_sorted = sorted(rows, key=lambda r: r["exp"], reverse=True)

    if PrettyTable:
        t=PrettyTable()
        t.field_names = [
            "Pair","Session","w1","w2",
            "Trades","Winrate","AvgWinR","AvgLossR","ExpectancyR",
            "TP1%","TP2%"
        ]
        for r in rows_sorted:
            t.add_row([
                r["pair"],
                r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}",
                r["trades"],
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}R",
                f"{r['avg_loss']:.3f}R",
                f"{r['exp']:+.3f}R",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%"
            ])
        print("\n===== BEST COMBO PAR PAIRE — trié par Expectancy (R) =====")
        print(t)
        print("==========================================================")
    else:
        print("\nPair\tSession\tw1\tw2\tTrades\tWinrate\tAvgWinR\tAvgLossR\tExpectancyR\tTP1%\tTP2%")
        for r in rows_sorted:
            print("\t".join([
                r["pair"], r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}",
                str(r["trades"]),
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}",
                f"{r['avg_loss']:.3f}",
                f"{r['exp']:+.3f}",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%"
            ]))
        print("==========================================================")


# ---------------- Impression breakdown CSV Pair / Jour / TP (TP1 / TP2 ONLY) ----------------
def print_breakdown_table(rows: List[Dict[str, Any]], exp_threshold: float = 0.4):
    """
    SESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI

    - Un seul TP possible par jour/pair/session (TP1 ou TP2)
    - On met Y uniquement si expectancy > exp_threshold
    - On supprime les lignes 100% N
    """
    if not rows:
        print("\nAucun trade pour le breakdown pair/jour/TP.")
        return

    valid_days=["MON","TUE","WED","THU","FRI"]

    # (session,pair) -> day -> tp -> expectancy
    by_sp_day={}

    for r in rows:
        session=r["session"]
        pair=r["pair"]
        dow =r["dow"]
        tp  =r["tp"]
        exp =r["expectancy"]

        if dow not in valid_days:
            continue

        key=(session,pair)
        if key not in by_sp_day:
            by_sp_day[key]={d:{} for d in valid_days}
        by_sp_day[key][dow][tp]=exp

    # Choix du meilleur TP (TP1 ou TP2)
    best_tp_per_spd={}

    for (session,pair),day_map in by_sp_day.items():
        for d in valid_days:
            tps=day_map.get(d,{})
            best_tp=None
            best_exp=None
            for tp in ["TP1","TP2"]:
                e=tps.get(tp)
                if e is None: continue
                if (best_exp is None) or (e>best_exp):
                    best_exp=e
                    best_tp=tp
            best_tp_per_spd[(session,pair,d)] = (best_tp,best_exp)

    # final structure
    final_flags={}

    for (session,pair,d),(tp_best,exp_best) in best_tp_per_spd.items():
        for tp in ["TP1","TP2"]:
            key=(session,pair,tp)
            if key not in final_flags:
                final_flags[key]={day:"N" for day in valid_days}

            if tp_best==tp and exp_best is not None and exp_best>exp_threshold:
                final_flags[key][d]="Y"

    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")

    for (session,pair,tp),day_flags in sorted(final_flags.items(),key=lambda x:(x[0][0],x[0][1],x[0][2])):
        flags_list=[day_flags[d] for d in valid_days]
        if not any(f=="Y" for f in flags_list):
            continue
        pair_type=infer_type(pair)
        line=f"{session},{pair_type},{pair},{tp}," + ",".join(flags_list)
        print(line)


# ---------------- CSV global haut rendement ----------------
def print_high_exp_pairs_csv(best_rows: List[Dict[str, Any]], best_exp_threshold: float = 0.15):
    """
    VERSION TP1 / TP2 ONLY
    - On choisit TP1 si w1 > w2, sinon TP2
    """
    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")

    for r in best_rows:
        if r["exp"] < best_exp_threshold:
            continue

        session=r["session"]
        if session=="-": continue

        pair=r["pair"]
        pair_type=infer_type(pair)

        tp = "TP1" if r["w1"] >= r["w2"] else "TP2"

        print(f"{session},{pair_type},{pair},{tp},Y,Y,Y,Y,Y")


# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs-file", default="pairs_5ers.txt")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date",   default="2025-12-31")
    ap.add_argument("--step", type=float, default=0.1, choices=[0.1])
    ap.add_argument("--exp-threshold", type=float, default=0.15)
    ap.add_argument("--session",
        default="TOKYO",
        choices=["TOKYO","LONDON","NY"])
    ap.add_argument("--best-exp-threshold",type=float,default=0.15)

    args=ap.parse_args()

    pairs = load_pairs_from_file(args.pairs_file)
    if not pairs:
        print("Aucune paire trouvée."); sys.exit(0)

    d0=parse_date(args.start_date)
    d1=parse_date(args.end_date)

    sessions=[args.session.upper()]

    best_rows=[]
    breakdown_rows=[]

    with get_pg_conn() as c:
        for pair in pairs:
            pair=pair.strip().upper()
            if not pair: continue

            session_trades={}
            for sess in sessions:
                print(f"[{pair}] Collecte trades — {sess} ...")
                session_trades[sess]=collect_trades_for_session(c,pair,d0,d1,sess)

            best_for_pair=None

            for sess in sessions:
                trades=session_trades[sess]
                for (w1,w2) in weight_grid(step=args.step):
                    st=stats_for_weights(trades,w1,w2)
                    row={
                        "pair":pair,
                        "session":sess,
                        "w1":w1,"w2":w2,
                        **st
                    }
                    if (best_for_pair is None) or (row["exp"] > best_for_pair["exp"]):
                        best_for_pair=row

            if best_for_pair and best_for_pair["trades"]>0:
                best_rows.append(best_for_pair)
            else:
                best_rows.append({
                    "pair":pair,"session":"-",
                    "w1":0.0,"w2":1.0,
                    "trades":0,"winrate":0.0,
                    "avg_win":0.0,"avg_loss":0.0,"exp":0.0,
                    "p1":0.0,"p2":0.0
                })

            for sess in sessions:
                trades=session_trades[sess]
                breakdown_rows.extend(
                    build_breakdown_rows_for_pair(pair,sess,trades)
                )

    print_final_best_table(best_rows)
    print_breakdown_table(breakdown_rows, exp_threshold=args.exp_threshold)
    print_high_exp_pairs_csv(best_rows, best_exp_threshold=args.best_exp_threshold)


if __name__=="__main__":
    main()
