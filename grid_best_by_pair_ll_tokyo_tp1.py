#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Pairs — Best (Session × w1..w5) per Pair — Expectancy Recap (1 ligne par paire)
+ Breakdown Pair × Jour de la semaine × TP (TP unique) au format CSV :

SESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI
TOKYO,INDEX,GBPCAD,TP3,Y,N,N,N,Y

RÈGLE (mise à jour) :
- On détermine, pour chaque paire, le meilleur moment (session) pour LANCER un trade.
- Par session (TOKYO/LONDON/NY), on prend AU PLUS 1 trade par (paire, jour) si l'entrée se produit dans la fenêtre de la session.
- Peu importe quand le trade se termine (SL/RR5), on NE bloque PAS le jour suivant (pas d'anti-overlap cross-day).

Entrées & cibles (inchangé, sauf SL de l’entrée cf. plus bas) :
- Entrées: mêmes règles (break strict, pullback antagoniste, entrée wick), SL = extrême (low/high) depuis le pullback (inclus).
- Cibles: RR1 uniquement (les RR2..RR5 sont ignorés).
- WIN/LOSS: WIN si TP1 < SL, sinon LOSS (indépendant des poids).
- R-multiple: application événementielle uniquement sur TP1 (w1=1, autres ignorés).
- Sessions: TOKYO / LONDON / NY.
- Sortie 1: tableau final trié par Expectancy (R) — 1 ligne = la meilleure combinaison par paire.
- Sortie 2: breakdown Pair × Jour de la semaine × TP (TP1 uniquement) au format CSV, en ne gardant que les lignes
           où au moins un jour a une expectancy > 0.1.

I/O:
- Lit les paires depuis --pairs-file (default: pairs_5ers.txt). Format simple: une paire par ligne,
  ou CSV avec une colonne "pair"/"pairs". Dédoublonnage automatique.
- Pas de sizing ni de frais: optimisation pure en R.

Usage:
  python grid_best_by_pair.py --pairs-file pairs_5ers.txt --start-date 2025-01-01 --end-date 2025-12-31
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
WEEKDAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
LONDON_TZ = ZoneInfo("Europe/London")

# ---------------- ENV / DB ----------------
load_dotenv()
PG_HOST     = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "postgres")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_SSLMODE  = os.getenv("PG_SSLMODE", "disable")

def get_pg_conn():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} sslmode={PG_SSLMODE}"
    conn = psycopg2.connect(dsn)
    conn.set_isolation_level(pg_ext.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn

# ---------------- Time utils ----------------
def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def parse_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()

def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)

def day_ms_bounds(d: date) -> Tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, tzinfo=UTC)
    end   = start + timedelta(days=1)
    return int(start.timestamp()*1000), int(end.timestamp()*1000)

# ---- Sessions (fenêtres UTC) ----
def tokyo_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=1)).timestamp()*1000)
    end   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)
    return start, end

def london_signal_window(d: date) -> Tuple[int, int]:
    local_start = datetime(d.year, d.month, d.day, 8, 0, tzinfo=LONDON_TZ)
    local_end   = datetime(d.year, d.month, d.day, 12, 45, tzinfo=LONDON_TZ)
    return int(local_start.astimezone(UTC).timestamp() * 1000), int(local_end.astimezone(UTC).timestamp() * 1000)

def ny_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=13)).timestamp()*1000)
    end   = int((base + timedelta(hours=17, minutes=45)).timestamp()*1000)
    return start, end

def window_for_session(session: str, d: date) -> Tuple[int, int]:
    s = (session or "").strip().upper()
    if s == "TOKYO":  return tokyo_signal_window(d)
    if s == "LONDON": return london_signal_window(d)
    if s in ("NY", "NEWYORK", "NEW_YORK"): return ny_signal_window(d)
    return tokyo_signal_window(d)

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", pair.lower()).strip("_")

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
    if up.startswith(("XAU", "XAG", "XPT", "XPD")):
        return "METAL"
    if up in {"NAS100","US30","US500","SPX500","GER40","UK100","FRA40","JPN225","JP225","HK50"}:
        return "INDEX"
    if up in {"BTCUSD","ETHUSD","LTCUSD","XRPUSD","ADAUSD","SOLUSD"}:
        return "CRYPTO"
    return "FOREX"

# ---------------- DB Readers ----------------
def read_first_1h(conn, pair: str, d: date) -> Optional[Dict]:
    t1h = table_name(pair, "1h")
    day_start, _ = day_ms_bounds(d)
    sql = f"SELECT ts, open, high, low, close FROM {t1h} WHERE ts = %s LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (day_start,))
            row = cur.fetchone()
            if not row: return None
            ts, o, h, l, c = row
            return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
    except Exception:
        conn.rollback(); return None

def read_15m_in(conn, pair: str, start_ms: int, end_ms: int) -> List[Dict]:
    t15 = table_name(pair, "15m")
    sql = f"SELECT ts, open, high, low, close FROM {t15} WHERE ts >= %s AND ts <= %s ORDER BY ts ASC"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms, end_ms))
            rows = cur.fetchall()
            return [{"ts": int(ts), "open": float(o), "high": float(h),"low": float(l), "close": float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

def read_15m_from(conn, pair: str, start_ms: int) -> List[Dict]:
    t15 = table_name(pair, "15m")
    sql = f"SELECT ts, high, low FROM {t15} WHERE ts > %s ORDER BY ts ASC"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms,))
            rows = cur.fetchall()
            return [{"ts": int(ts), "high": float(h), "low": float(l)} for ts,h,l in rows]
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
    long_active = short_active = False
    long_hh = short_ll = None
    long_pullback_idx = short_pullback_idx = None
    long_min_low_since_pullback = short_max_high_since_pullback = None
    for i, b in enumerate(c15):
        ts, o, h, l, c = b["ts"], b["open"], b["high"], b["low"], b["close"]
        if not long_active and c > range_high:
            long_active = True; long_hh = h
        if not short_active and c < range_low:
            short_active = True; short_ll = l
        if long_active:
            if long_pullback_idx is None and c < o:
                long_pullback_idx = i; long_min_low_since_pullback = l
            if long_pullback_idx is not None and i > long_pullback_idx and h > long_hh:
                entry = long_hh; sl = long_min_low_since_pullback or l
                return Trade("LONG", ts, entry, sl)
            long_hh = max(long_hh or h, h)
        if short_active:
            if short_pullback_idx is None and c > o:
                short_pullback_idx = i; short_max_high_since_pullback = h
            if short_pullback_idx is not None and i > short_pullback_idx and l < short_ll:
                entry = short_ll; sl = short_max_high_since_pullback or h
                return Trade("SHORT", ts, entry, sl)
            short_ll = min(short_ll or l, l)
    return None

# ---------------- After-entry evaluation ----------------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        targets = {"RR1": entry, "RR2": entry, "RR3": entry, "RR4": entry, "RR5": entry}
        results = {k: "SL" for k in targets}
        return targets, results, {"SL": None, "RR1": None, "RR2": None, "RR3": None, "RR4": None, "RR5": None}, None
    # seules les cibles RR1 sont utilisées
    if tr.side == "LONG": t1 = entry + r
    else: t1 = entry - r
    targets = {"RR1": t1, "RR2": t1, "RR3": t1, "RR4": t1, "RR5": t1}
    hit_time = {"SL": None, "RR1": None, "RR2": None, "RR3": None, "RR4": None, "RR5": None}
    future = read_15m_from(conn, pair, tr.entry_ts)
    for b in future:
        ts, h, l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            if l <= sl + eps and hit_time["SL"] is None: hit_time["SL"] = ts
            if h >= t1 - eps and hit_time["RR1"] is None: hit_time["RR1"] = ts
        else:
            if h >= sl - eps and hit_time["SL"] is None: hit_time["SL"] = ts
            if l <= t1 + eps and hit_time["RR1"] is None: hit_time["RR1"] = ts
        if hit_time["SL"] or hit_time["RR1"]: break
    results = {k: "SL" for k in targets}
    sl_time = hit_time["SL"]
    for key in ["RR1"]:
        ttime = hit_time[key]
        results[key] = "TP" if (ttime and (not sl_time or ttime < sl_time)) else "SL"
    closed_ts = hit_time["SL"] or hit_time["RR1"]
    return targets, results, hit_time, closed_ts

# ---------------- compute_r_and_close ----------------
def compute_r_and_close(hit_time: Dict[str, Optional[int]], w1: float, w2: float, w3: float, w4: float, w5: float) -> float:
    t_sl = hit_time.get("SL"); t1 = hit_time.get("RR1")
    rem = 1.0; r = 0.0
    if t1 is not None and (t_sl is None or t1 < t_sl):
        r += w1 * 1.0; rem -= w1
    elif t_sl is not None:
        r -= rem
    return r

def reached_before(hits: Dict[str, Optional[int]], key: str) -> bool:
    t = hits.get(key); sl = hits.get("SL")
    return t is not None and (sl is None or t < sl)

# ---------------- Core Trade Generation ----------------
@dataclass
class BareTrade:
    hits: Dict[str, Optional[int]]
    entry_ts: int

def collect_trades_for_session(conn, pair: str, start: date, end: date, session: str) -> List[BareTrade]:
    trades = []
    for d in daterange(start, end):
        s, e = window_for_session(session, d)
        c1 = read_first_1h(conn, pair, d)
        if not c1: continue
        rh, rl = c1["high"], c1["low"]
        c15 = read_15m_in(conn, pair, s, e)
        if not c15: continue
        tr = detect_first_trade_for_day(c15, rh, rl)
        if not tr: continue
        _, _, hits, closed_ts = evaluate_trade_after_entry(conn, pair, tr)
        trades.append(BareTrade(hits=hits, entry_ts=tr.entry_ts))
    return trades

# ---------------- weight_grid (inchangé) ----------------
def weight_grid(step: float = 0.1):
    vals = [round(i * step, 1) for i in range(int(1/step) + 1)]
    for w1 in vals:
        yield (w1, 0.0, 0.0, 0.0, round(1.0 - w1, 1))

# ---------------- stats_for_weights ----------------
def stats_for_weights(trades: List[BareTrade], w1: float, w2: float, w3: float, w4: float, w5: float) -> Dict[str, Any]:
    total = len(trades)
    if total == 0:
        return {"trades": 0,"winrate": 0.0,"avg_win": 0.0,"avg_loss": 0.0,"exp": 0.0,"p1": 0.0,"p2": 0.0,"p3": 0.0,"p4": 0.0,"p5": 0.0}
    r_wins, r_losses_abs = [], []
    tp1_cnt = 0
    for bt in trades:
        hits = bt.hits
        if reached_before(hits, "RR1"): tp1_cnt += 1
        r_mult = compute_r_and_close(hits, w1, 0, 0, 0, 0)
        if reached_before(hits, "RR1"): r_wins.append(r_mult)
        else: r_losses_abs.append(-r_mult)
    wins = len(r_wins); losses = len(r_losses_abs)
    winrate = wins/total if total>0 else 0
    avg_win = sum(r_wins)/wins if wins>0 else 0
    avg_loss = sum(r_losses_abs)/losses if losses>0 else 0
    expectancy = winrate*avg_win - (1-winrate)*avg_loss
    return {"trades": total,"winrate": winrate,"avg_win": avg_win,"avg_loss": avg_loss,"exp": expectancy,"p1": tp1_cnt/total,"p2": 0,"p3": 0,"p4": 0,"p5": 0}

# ---------------- Breakdown TP1 only ----------------
def build_breakdown_rows_for_pair(pair: str, session: str, trades: List[BareTrade]) -> List[Dict[str, Any]]:
    buckets = defaultdict(lambda: {"trades": 0,"wins": 0,"sum_r": 0.0})
    for bt in trades:
        hits = bt.hits
        dt = datetime.fromtimestamp(bt.entry_ts / 1000, tz=UTC)
        dow_name = WEEKDAYS[dt.weekday()]
        t_sl = hits.get("SL"); t_tp = hits.get("RR1")
        bucket_key = (dow_name, "TP1")
        buckets[bucket_key]["trades"] += 1
        if (t_tp and (not t_sl or t_tp < t_sl)):
            r = 1.0; buckets[bucket_key]["wins"] += 1
        else: r = -1.0
        buckets[bucket_key]["sum_r"] += r
    rows=[]
    for (dow_name,tp_label),agg in sorted(buckets.items(),key=lambda x:(WEEKDAYS.index(x[0][0]),x[0][1])):
        total=agg["trades"]; 
        if total==0: continue
        wins=agg["wins"]; avg_r=agg["sum_r"]/total
        rows.append({"pair":pair,"session":session,"dow":dow_name,"tp":tp_label,"trades":total,"winrate":wins/total,"expectancy":avg_r})
    return rows

# ---------------- Chargement des paires ----------------
def load_pairs_from_file(path: str) -> List[str]:
    pairs: List[str] = []
    if not os.path.exists(path):
        print(f"Erreur: {path} introuvable.")
        return pairs
    # Essaye CSV avec en-tête
    try:
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and any(h.lower() in ("pair","pairs") for h in reader.fieldnames):
                for rec in reader:
                    p = (rec.get("pair") or rec.get("PAIR") or rec.get("pairs") or rec.get("PAIRS") or "").strip()
                    if p:
                        up = p.upper()
                        if up not in pairs:
                            pairs.append(up)
                if pairs:
                    return pairs
    except Exception:
        pass
    # Fallback: une paire par ligne
    with open(path, "r") as f:
        for line in f:
            p = line.strip()
            if not p or p.startswith("#"):
                continue
            up = p.upper()
            if up not in pairs:
                pairs.append(up)
    return pairs

# ---------------- Impression du recap final (1 ligne / paire) ----------------
def print_final_best_table(rows: List[Dict[str, Any]]):
    try:
        from prettytable import PrettyTable
    except Exception:
        PrettyTable = None

    # Tri par expectancy décroissante
    rows_sorted = sorted(rows, key=lambda r: r["exp"], reverse=True)

    if PrettyTable:
        t = PrettyTable()
        t.field_names = [
            "Pair","Session","w1","w2","w3","w4","w5",
            "Trades","Winrate","AvgWinR","AvgLossR","ExpectancyR",
            "TP1%","TP2%","TP3%","TP4%","TP5%"
        ]
        for r in rows_sorted:
            t.add_row([
                r["pair"],
                r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}", f"{r['w4']:.1f}", f"{r['w5']:.1f}",
                r["trades"],
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}R",
                f"{r['avg_loss']:.3f}R",
                f"{r['exp']:+.3f}R",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%",
                f"{r['p4']*100:.2f}%",
                f"{r['p5']*100:.2f}%"
            ])
        print("\n===== BEST COMBO PAR PAIRE — trié par Expectancy (R) =====")
        print(t)
        print("==========================================================")
    else:
        # Fallback
        print("\nPair\tSession\tw1\tw2\tw3\tw4\tw5\tTrades\tWinrate\tAvgWinR\tAvgLossR\tExpectancyR\tTP1%\tTP2%\tTP3%\tTP4%\tTP5%")
        for r in rows_sorted:
            print("\t".join([
                r["pair"], r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}", f"{r['w4']:.1f}", f"{r['w5']:.1f}",
                str(r["trades"]),
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}",
                f"{r['avg_loss']:.3f}",
                f"{r['exp']:+.3f}",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%",
                f"{r['p4']*100:.2f}%",
                f"{r['p5']*100:.2f}%"
            ]))
        print("==========================================================")

# ---------------- Impression breakdown CSV Pair / Jour / TP ----------------
def print_breakdown_table(rows: List[Dict[str, Any]], exp_threshold: float = 0.4):
    """
    Affiche au format :

    SESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI

    Règle:
    - Pour chaque (session, pair, jour), on choisit le TP (TP1..TP5) avec la meilleure expectancy.
    - On met Y sur ce TP si son expectancy > exp_threshold, sinon N.
    - Un seul TP peut être Y par jour et par paire/session.
    - On ne garde que les lignes (session, pair, TP) avec au moins un Y.
    """
    if not rows:
        print("\nAucun trade pour le breakdown pair/jour/TP.")
        return

    # 1) On regroupe par (session, pair, day, tp) -> expectancy
    # structure : by_sp_day[(session, pair)][day][tp] = expectancy
    by_sp_day: Dict[Tuple[str, str], Dict[str, Dict[str, Optional[float]]]] = {}

    valid_days = ["MON", "TUE", "WED", "THU", "FRI"]

    for r in rows:
        session = r["session"]
        pair    = r["pair"]
        dow     = r["dow"]
        tp      = r["tp"]   # "TP1" ... "TP5"
        exp     = r["expectancy"]

        if dow not in valid_days:
            continue  # on ignore le weekend dans ce CSV

        key = (session, pair)
        if key not in by_sp_day:
            by_sp_day[key] = {d: {} for d in valid_days}
        by_sp_day[key][dow][tp] = exp

    # 2) Pour chaque (session, pair, day), déterminer le TP avec la meilleure expectancy
    # best_tp_per_spd[(session, pair, day)] = (tp_best, exp_best) ou (None, None)
    best_tp_per_spd: Dict[Tuple[str, str, str], Tuple[Optional[str], Optional[float]]] = {}

    for (session, pair), day_map in by_sp_day.items():
        for d in valid_days:
            tps = day_map.get(d, {})
            best_tp = None
            best_exp = None
            for tp in ["TP1", "TP2", "TP3", "TP4", "TP5"]:
                e = tps.get(tp)
                if e is None:
                    continue
                if (best_exp is None) or (e > best_exp):
                    best_exp = e
                    best_tp = tp
            best_tp_per_spd[(session, pair, d)] = (best_tp, best_exp)

    # 3) Construire la structure finale par (session, pair, tp) -> flags par jour
    # final_flags[(session, pair, tp)] = {day: "Y"/"N"}
    final_flags: Dict[Tuple[str, str, str], Dict[str, str]] = {}

    for (session, pair, d), (tp_best, exp_best) in best_tp_per_spd.items():
        for tp in ["TP1", "TP2", "TP3", "TP4", "TP5"]:
            key = (session, pair, tp)
            if key not in final_flags:
                final_flags[key] = {day: "N" for day in valid_days}

            # Si ce TP est le meilleur du jour et dépasse le seuil -> Y, sinon N (on laisse comme N)
            if tp_best == tp and exp_best is not None and exp_best > exp_threshold:
                final_flags[key][d] = "Y"

    # 4) Impression CSV : on ne garde que les lignes avec au moins un Y
    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")

    # tri par SESSION, PAIR, TP
    for (session, pair, tp), day_flags in sorted(final_flags.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        flags_list = [day_flags[d] for d in valid_days]
        if not any(f == "Y" for f in flags_list):
            continue  # on skip les lignes full N

        pair_type = infer_type(pair)
        line = f"{session},{pair_type},{pair},{tp}," + ",".join(flags_list)
        print(line)

def print_high_exp_pairs_csv(best_rows: List[Dict[str, Any]], best_exp_threshold: float = 0.15):
    """
    CSV global basé sur le tableau recap (best_rows) :

    - On filtre les paires dont l'expectancy globale (exp) >= best_exp_threshold.
    - On choisit le TP correspondant AU POIDS DOMINANT (w1..w5) de cette paire.
      -> w1 max  => TP1
      -> w2 max  => TP2
      -> ...
      -> w5 max  => TP5
    - On affiche : SESSION,TYPE,PAIR,TP,Y,Y,Y,Y,Y
    """

    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")

    for r in best_rows:
        # 1) Filtre sur l'ExpectancyR globale du recap
        if r["exp"] < best_exp_threshold:
            continue

        session = r["session"]
        if session == "-":
            continue

        pair = r["pair"]
        pair_type = infer_type(pair)

        # 2) Choix du TP via le POIDS dominant w1..w5
        weights = [
            ("TP1", r["w1"]),
            ("TP2", r["w2"]),
            ("TP3", r["w3"]),
            ("TP4", r["w4"]),
            ("TP5", r["w5"]),
        ]
        best_tp, _ = max(weights, key=lambda x: x[1])  # max sur le poids

        # 3) Impression console
        print(f"{session},{pair_type},{pair},{best_tp},Y,Y,Y,Y,Y")





# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs-file", default="pairs_5ers.txt", help="Fichier des paires (une par ligne ou CSV avec colonne pair/pairs)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date",   default="2025-12-31")
    ap.add_argument("--step", type=float, default=0.1, choices=[0.1], help="Pas de grille (fixé à 0.1)")
    ap.add_argument("--exp-threshold", type=float, default=0.15, help="Seuil d'expectancy pour marquer Y et filtrer les lignes")
    ap.add_argument(
        "--session",
        default="TOKYO",
        choices=["TOKYO", "LONDON", "NY"],
        help="Session à tester (TOKYO, LONDON ou NY). Défaut: TOKYO."
    )
    ap.add_argument(
        "--best-exp-threshold",
        type=float,
        default=0.15,
        help="Seuil d'expectancy globale (R) pour le CSV 'all days = Y' (expR >= ce seuil)."
    )
    args = ap.parse_args()

    pairs = load_pairs_from_file(args.pairs_file)
    if not pairs:
        print("Aucune paire trouvée.")
        sys.exit(0)

    d0 = parse_date(args.start_date)
    d1 = parse_date(args.end_date)

    # --- TOKYO ONLY (ajoute LONDON/NY si besoin) ---
    sessions = [args.session.upper()]

    best_rows: List[Dict[str, Any]] = []
    breakdown_rows: List[Dict[str, Any]] = []

    with get_pg_conn() as c:
        for pair in pairs:
            pair = pair.strip().upper()
            if not pair:
                continue

            # 1) Collecte des trades par session
            session_trades: Dict[str, List[BareTrade]] = {}
            for sess in sessions:
                print(f"[{pair}] Collecte trades — {sess} ...")
                session_trades[sess] = collect_trades_for_session(c, pair, d0, d1, sess)

            # 2) Parcourt la grille (w1..w5) pour chaque session et retient la meilleure combinaison
            best_for_pair: Optional[Dict[str, Any]] = None

            for sess in sessions:
                trades = session_trades[sess]
                for (w1, w2, w3, w4, w5) in weight_grid(step=args.step):
                    st = stats_for_weights(trades, w1, w2, w3, w4, w5)
                    row = {
                        "pair": pair,
                        "session": sess,
                        "w1": w1, "w2": w2, "w3": w3, "w4": w4, "w5": w5,
                        **st
                    }
                    if (best_for_pair is None) or (row["exp"] > best_for_pair["exp"]):
                        best_for_pair = row

            # 3) Empile la meilleure ligne de la paire si au moins 1 trade
            if best_for_pair and best_for_pair["trades"] > 0:
                best_rows.append(best_for_pair)
            else:
                # Ajoute quand même une ligne neutre pour visibilité
                best_rows.append({
                    "pair": pair, "session": "-",
                    "w1": 0.0, "w2": 0.0, "w3": 0.0, "w4": 0.0, "w5": 1.0,
                    "trades": 0, "winrate": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "exp": 0.0,
                    "p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0, "p5": 0.0
                })

            # 4) Breakdown par paire / jour de la semaine / TP (TP unique)
            for sess in sessions:
                trades = session_trades[sess]
                breakdown_rows.extend(
                    build_breakdown_rows_for_pair(pair, sess, trades)
                )

    # 4) Affichage final (1 ligne par paire)
    print_final_best_table(best_rows)

    # 5) Affichage breakdown pair / jour / TP au format CSV
    print_breakdown_table(breakdown_rows, exp_threshold=args.exp_threshold)
    
    # 6) Affichage CSV des paires "haut rendement" (expR globale >= seuil) avec tous les jours = Y
    print_high_exp_pairs_csv(best_rows, best_exp_threshold=args.best_exp_threshold)

if __name__ == "__main__":
    main()

