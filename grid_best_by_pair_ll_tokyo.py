#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Pairs — Best (Session × w1..w5) per Pair — Expectancy Recap (1 ligne par paire)

RÈGLE (mise à jour) :
- On détermine, pour chaque paire, le meilleur moment (session) pour LANCER un trade.
- Par session (TOKYO/LONDON/NY), on prend AU PLUS 1 trade par (paire, jour) si l'entrée se produit dans la fenêtre de la session.
- Peu importe quand le trade se termine (SL/RR5), on NE bloque PAS le jour suivant (pas d'anti-overlap cross-day).

Entrées & cibles (inchangé côté détection, sauf extension cibles) :
- Entrées: mêmes règles (break strict, pullback antagoniste, entrée wick), SL = extrême (low/high) depuis le pullback (inclus).
- Cibles: RR1 / RR2 / RR3 / RR4 / RR5 (timestamps), arrêt au 1er SL ou RR5 (pour l’évaluation des hits).
- WIN/LOSS: WIN si TP1 < SL, sinon LOSS (indépendant des poids).
- R-multiple: application événementielle des partiels (w1..w5), w1+…+w5=1.
- Sessions: TOKYO / LONDON / NY.
- Sortie: tableau final trié par Expectancy (R) — 1 ligne = la meilleure combinaison par paire.

I/O:
- Lit les paires depuis --pairs-file (default: pairs.txt). Format simple: une paire par ligne,
  ou CSV avec une colonne "pair"/"pairs". Dédoublonnage automatique.
- Pas de sizing ni de frais: optimisation pure en R.

Usage:
  python grid_best_by_pair.py --pairs-file pairs.txt --start-date 2025-01-01 --end-date 2025-12-31
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext

UTC = timezone.utc

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
    start = int((base + timedelta(hours=1)).timestamp()*1000)                # 01:00
    end   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)   # 05:45
    return start, end

def london_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=8)).timestamp()*1000)                 # 08:00
    end   = int((base + timedelta(hours=14, minutes=45)).timestamp()*1000)   # 14:45
    return start, end

def ny_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=13)).timestamp()*1000)               # 13:00
    end   = int((base + timedelta(hours=17, minutes=45)).timestamp()*1000)   # 17:45
    return start, end

def window_for_session(session: str, d: date) -> Tuple[int, int]:
    s = (session or "").strip().upper()
    if s == "TOKYO":  return tokyo_signal_window(d)
    if s == "LONDON": return london_signal_window(d)
    if s in ("NY","NEWYORK","NEW_YORK"): return ny_signal_window(d)
    return tokyo_signal_window(d)

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    return 0.001 if pair.upper().endswith("JPY") else 0.00001

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
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t15}
        WHERE ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms, end_ms))
            rows = cur.fetchall()
            return [{"ts": int(ts), "open": float(o), "high": float(h),
                     "low": float(l), "close": float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

def read_15m_from(conn, pair: str, start_ms: int) -> List[Dict]:
    t15 = table_name(pair, "15m")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t15}
        WHERE ts > %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms,))
            rows = cur.fetchall()
            return [{"ts": int(ts), "open": float(o), "high": float(h),
                     "low": float(l), "close": float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

# ---------------- FSM / Trade (CORE LOGIC — DO NOT CHANGE sauf SL depuis pullback) ----------------
@dataclass
class Trade:
    side: str              # "LONG" | "SHORT"
    entry_ts: int          # ts OPEN UTC of trigger bar
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    # Activation long/short + pullback antagoniste + wick trigger; SL = lowest/highest depuis pullback (inclus)
    long_active = False
    long_hh: Optional[float] = None
    long_pullback_idx: Optional[int] = None
    long_min_low_since_pullback: Optional[float] = None

    short_active = False
    short_ll: Optional[float] = None
    short_pullback_idx: Optional[int] = None
    short_max_high_since_pullback: Optional[float] = None

    for i, b in enumerate(c15):
        ts, o, h, l, c = b["ts"], b["open"], b["high"], b["low"], b["close"]

        if (not long_active) and (c > range_high):
            long_active = True
            long_hh = h
            long_pullback_idx = None
            long_min_low_since_pullback = None

        if (not short_active) and (c < range_low):
            short_active = True
            short_ll = l
            short_pullback_idx = None
            short_max_high_since_pullback = None

        # -------- LONG --------
        if long_active:
            prev_hh = long_hh
            if long_pullback_idx is None and (c < o):
                long_pullback_idx = i
                long_min_low_since_pullback = l  # inclut la bougie de pullback
            if long_pullback_idx is not None and i >= 1:
                prev_low = c15[i-1]["low"]
                long_min_low_since_pullback = prev_low if long_min_low_since_pullback is None else min(long_min_low_since_pullback, prev_low)
            if (prev_hh is not None) and (long_pullback_idx is not None) and (i > long_pullback_idx) and (h > prev_hh) and (i >= 1):
                entry_price = prev_hh
                sl_price = long_min_low_since_pullback if long_min_low_since_pullback is not None else c15[i-1]["low"]
                return Trade("LONG", ts, entry_price, sl_price)
            if (long_hh is None) or (h > long_hh):
                long_hh = h

        # -------- SHORT --------
        if short_active:
            prev_ll = short_ll
            if short_pullback_idx is None and (c > o):
                short_pullback_idx = i
                short_max_high_since_pullback = h  # inclut la bougie de pullback
            if short_pullback_idx is not None and i >= 1:
                prev_high = c15[i-1]["high"]
                short_max_high_since_pullback = prev_high if short_max_high_since_pullback is None else max(short_max_high_since_pullback, prev_high)
            if (prev_ll is not None) and (short_pullback_idx is not None) and (i > short_pullback_idx) and (l < prev_ll) and (i >= 1):
                entry_price = prev_ll
                sl_price = short_max_high_since_pullback if short_max_high_since_pullback is not None else c15[i-1]["high"]
                return Trade("SHORT", ts, entry_price, sl_price)
            if (short_ll is None) or (l < short_ll):
                short_ll = l

    return None

# ---------------- After-entry evaluation (CORE LOGIC — étendu à RR5) ----------------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    """
    Enregistre les timestamps de RR1..RR5/SL; stop au premier SL ou RR5.
    """
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        targets = {"RR1": entry, "RR2": entry, "RR3": entry, "RR4": entry, "RR5": entry}
        results = {k: "SL" for k in ["RR1","RR2","RR3","RR4","RR5"]}
        return targets, results, {"SL": None, "RR1": None, "RR2": None, "RR3": None, "RR4": None, "RR5": None}, None

    if tr.side == "LONG":
        t1, t2, t3, t4, t5 = (entry + n*r for n in (1.0, 2.0, 3.0, 4.0, 5.0))
    else:
        t1, t2, t3, t4, t5 = (entry - n*r for n in (1.0, 2.0, 3.0, 4.0, 5.0))

    targets = {"RR1": t1, "RR2": t2, "RR3": t3, "RR4": t4, "RR5": t5}
    hit_time: Dict[str, Optional[int]] = {"SL": None, "RR1": None, "RR2": None, "RR3": None, "RR4": None, "RR5": None}

    future = read_15m_from(conn, pair, tr.entry_ts)
    for b in future:
        ts,h,l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            sl_hit  = (l <= sl + eps)
            rr1_hit = (h >= t1 - eps)
            rr2_hit = (h >= t2 - eps)
            rr3_hit = (h >= t3 - eps)
            rr4_hit = (h >= t4 - eps)
            rr5_hit = (h >= t5 - eps)
        else:
            sl_hit  = (h >= sl - eps)
            rr1_hit = (l <= t1 + eps)
            rr2_hit = (l <= t2 + eps)
            rr3_hit = (l <= t3 + eps)
            rr4_hit = (l <= t4 + eps)
            rr5_hit = (l <= t5 + eps)

        if hit_time["SL"]  is None and sl_hit:  hit_time["SL"]  = ts
        if hit_time["RR1"] is None and rr1_hit: hit_time["RR1"] = ts
        if hit_time["RR2"] is None and rr2_hit: hit_time["RR2"] = ts
        if hit_time["RR3"] is None and rr3_hit: hit_time["RR3"] = ts
        if hit_time["RR4"] is None and rr4_hit: hit_time["RR4"] = ts
        if hit_time["RR5"] is None and rr5_hit: hit_time["RR5"] = ts

        if (hit_time["SL"] is not None) or (hit_time["RR5"] is not None):
            break

    results: Dict[str, str] = {}
    sl_time = hit_time["SL"]
    for key in ["RR1","RR2","RR3","RR4","RR5"]:
        ttime = hit_time[key]
        results[key] = "TP" if (ttime is not None and (sl_time is None or ttime < sl_time)) else "SL"

    closed_ts = sl_time if sl_time is not None else hit_time["RR5"]
    return targets, results, hit_time, closed_ts

# ---------------- Partials (w1..w5) -> R-multiple ----------------
def compute_r_and_close(hit_time: Dict[str, Optional[int]], w1: float, w2: float, w3: float, w4: float, w5: float) -> float:
    """
    Application temporelle des sorties partielles (w1+…+w5=1):
      TP1: +w1 * 1R ; rem -= w1
      TP2: +w2 * 2R ; rem -= w2
      TP3: +w3 * 3R ; rem -= w3
      TP4: +w4 * 4R ; rem -= w4
      TP5: +w5 * 5R ; rem -= w5
      SL : -1R * rem
    Renvoie le R-multiple total.
    """
    t_sl = hit_time.get("SL")
    t1   = hit_time.get("RR1")
    t2   = hit_time.get("RR2")
    t3   = hit_time.get("RR3")
    t4   = hit_time.get("RR4")
    t5   = hit_time.get("RR5")

    events: List[Tuple[int, str]] = []
    if t1 is not None: events.append((t1, "TP1"))
    if t2 is not None: events.append((t2, "TP2"))
    if t3 is not None: events.append((t3, "TP3"))
    if t4 is not None: events.append((t4, "TP4"))
    if t5 is not None: events.append((t5, "TP5"))
    if t_sl is not None: events.append((t_sl, "SL"))
    events.sort(key=lambda x: x[0])

    rem = 1.0
    r   = 0.0

    for ts, ev in events:
        if ev == "TP1" and w1 > 0:
            r   += w1 * 1.0; rem -= w1
            if rem <= 1e-12: break
        elif ev == "TP2" and w2 > 0:
            r   += w2 * 2.0; rem -= w2
            if rem <= 1e-12: break
        elif ev == "TP3" and w3 > 0:
            r   += w3 * 3.0; rem -= w3
            if rem <= 1e-12: break
        elif ev == "TP4" and w4 > 0:
            r   += w4 * 4.0; rem -= w4
            if rem <= 1e-12: break
        elif ev == "TP5" and w5 > 0:
            r   += w5 * 5.0; rem -= w5
            if rem <= 1e-12: break
        elif ev == "SL":
            if rem > 0:
                r += (-1.0) * rem
                rem = 0.0
            break

    return r

def reached_before(hits: Dict[str, Optional[int]], key: str) -> bool:
    t = hits.get(key)
    sl = hits.get("SL")
    return t is not None and (sl is None or t < sl)

# ---------------- Core: générer les trades (par session) ----------------
@dataclass
class BareTrade:
    hits: Dict[str, Optional[int]]  # {"SL": ts|None, "RR1": ts|None, ..., "RR5": ts|None}

def collect_trades_for_session(conn, pair: str, start: date, end: date, session: str) -> List[BareTrade]:
    """
    Règle: on prend AU PLUS UN trade par (paire, jour) si l'entrée est dans la fenêtre de la session.
    **MOD TOKYO ONLY**: tant que le trade n'est pas clôturé (TP5 ou SL), on BLOQUE les jours suivants.
    """
    trades: List[BareTrade] = []
    block_until_ts: Optional[int] = None  # ts de clôture (SL ou RR5) du dernier trade

    for d in daterange(start, end):
        s, e = window_for_session(session, d)

        if block_until_ts is not None and s <= block_until_ts:
            continue

        c1 = read_first_1h(conn, pair, d)
        if not c1:
            continue
        rh, rl = c1["high"], c1["low"]

        c15 = read_15m_in(conn, pair, s, e)
        if not c15:
            continue

        tr = detect_first_trade_for_day(c15, rh, rl)
        if not tr:
            continue

        _, _, hits, closed_ts = evaluate_trade_after_entry(conn, pair, tr)
        trades.append(BareTrade(hits=hits))

        block_until_ts = closed_ts if closed_ts is not None else (2**62)
        continue

    return trades

# ---------------- Grille des poids (w1..w5) ----------------
def weight_grid(step: float = 0.1):
    # Full TP only: (1,0,0,0,0) .. (0,0,0,0,1)
    options = [
        (1.0, 0.0, 0.0, 0.0, 0.0),  # full TP1
        (0.0, 1.0, 0.0, 0.0, 0.0),  # full TP2
        (0.0, 0.0, 1.0, 0.0, 0.0),  # full TP3
        (0.0, 0.0, 0.0, 1.0, 0.0),  # full TP4
        (0.0, 0.0, 0.0, 0.0, 1.0),  # full TP5
    ]
    for w1, w2, w3, w4, w5 in options:
        yield (w1, w2, w3, w4, w5)
        
# ---------------- Impression format session-file ----------------
# ---------------- Impression format session-file ----------------
def print_session_csv(rows: List[Dict[str, Any]]):
    """
    Génère un CSV au format :
    SESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI

    Basé sur le même filtre que "paires à trader actuellement" :
      - trades > 0
      - PF >= 1.5
      - MDD_R < 8
    """
    # Filtre identique à print_simple_tp_table
    selected: List[Dict[str, Any]] = []
    for r in rows:
        if r["trades"] <= 0:
            continue
        if r["pf"] < 1.5 or r["mdd_r"] >= 8.0:
            continue
        selected.append(r)

    # Tri par score décroissant comme le tableau simplifié
    selected_sorted = sorted(selected, key=lambda x: x["score"], reverse=True)

    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")
    for r in selected_sorted:
        # TP cible en fonction du poids
        if abs(r["w1"] - 1.0) < 1e-9:
            tp = "TP1"
        elif abs(r["w2"] - 1.0) < 1e-9:
            tp = "TP2"
        elif abs(r["w3"] - 1.0) < 1e-9:
            tp = "TP3"
        elif abs(r["w4"] - 1.0) < 1e-9:
            tp = "TP4"
        else:
            tp = "TP5"

        # TYPE — ton script ne stocke pas les types → on met FOREX par défaut
        typ = "FOREX"

        print(f"{r['session']},{typ},{r['pair']},{tp},Y,Y,Y,Y,Y")


# ---------------- Stats pour une combinaison ----------------
def stats_for_weights(trades: List[BareTrade], w1: float, w2: float, w3: float, w4: float, w5: float) -> Dict[str, Any]:
    total = len(trades)
    if total == 0:
        return {
            "trades": 0,
            "winrate": 0.0,
            "avg_win": 0.0,
            "avg_loss": 0.0,
            "exp": 0.0,
            "p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0, "p5": 0.0,
            "pf": 0.0,
            "mdd_r": 0.0,
            "score": 0.0,
        }

    r_wins: List[float] = []
    r_losses_abs: List[float] = []
    r_all: List[float] = []

    tp1_cnt = tp2_cnt = tp3_cnt = tp4_cnt = tp5_cnt = 0

    for bt in trades:
        hits = bt.hits
        if reached_before(hits, "RR1"): tp1_cnt += 1
        if reached_before(hits, "RR2"): tp2_cnt += 1
        if reached_before(hits, "RR3"): tp3_cnt += 1
        if reached_before(hits, "RR4"): tp4_cnt += 1
        if reached_before(hits, "RR5"): tp5_cnt += 1

        r_mult = compute_r_and_close(hits, w1, w2, w3, w4, w5)
        r_all.append(r_mult)

        # WIN/LOSS = TP1 avant SL
        if reached_before(hits, "RR1"):
            r_wins.append(r_mult)
        else:
            r_losses_abs.append(-r_mult)

    wins   = len(r_wins)
    losses = len(r_losses_abs)

    winrate = (wins / total) if total > 0 else 0.0
    avg_win = (sum(r_wins)/wins) if wins > 0 else 0.0
    avg_loss = (sum(r_losses_abs)/losses) if losses > 0 else 0.0
    expectancy = winrate * avg_win - (1.0 - winrate) * avg_loss

    p1 = tp1_cnt / total
    p2 = tp2_cnt / total
    p3 = tp3_cnt / total
    p4 = tp4_cnt / total
    p5 = tp5_cnt / total

    # Profit factor
    sum_wins   = sum(r_wins)
    sum_losses = sum(r_losses_abs)
    if sum_losses > 1e-9:
        pf = sum_wins / sum_losses
    else:
        # quasi pas de pertes : on borne un peu
        pf = sum_wins if sum_wins > 0 else 0.0

    # Max drawdown en R sur la séquence de trades
    equity = 0.0
    peak   = 0.0
    mdd_r  = 0.0
    for r in r_all:
        equity += r
        if equity > peak:
            peak = equity
        dd = peak - equity
        if dd > mdd_r:
            mdd_r = dd

    # Score composite (Condition 2)
    bonus = 0.3 if winrate > 0.45 else 0.0
    score = 2.0 * expectancy + pf - 0.5 * mdd_r + bonus

    return {
        "trades": total,
        "winrate": winrate,
        "avg_win": avg_win,
        "avg_loss": avg_loss,
        "exp": expectancy,
        "p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5,
        "pf": pf,
        "mdd_r": mdd_r,
        "score": score,
    }

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

    rows_sorted = sorted(rows, key=lambda r: r["score"], reverse=True)

    if PrettyTable:
        t = PrettyTable()
        t.field_names = [
            "Pair","Session","w1","w2","w3","w4","w5",
            "Trades","Winrate","AvgWinR","AvgLossR","ExpectancyR",
            "PF","MDD_R","Score",
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
                f"{r['pf']:.2f}",
                f"{r['mdd_r']:.2f}R",
                f"{r['score']:+.3f}",
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
        print("\nPair\tSession\tw1\tw2\tw3\tw4\tw5\tTrades\tWinrate\tAvgWinR\tAvgLossR\tExpectancyR\tPF\tMDD_R\tScore\tTP1%\tTP2%\tTP3%\tTP4%\tTP5%")
        for r in rows_sorted:
            print("\t".join([
                r["pair"], r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}", f"{r['w4']:.1f}", f"{r['w5']:.1f}",
                str(r["trades"]),
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}",
                f"{r['avg_loss']:.3f}",
                f"{r['exp']:+.3f}",
                f"{r['pf']:.2f}",
                f"{r['mdd_r']:.2f}",
                f"{r['score']:+.3f}",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%",
                f"{r['p4']*100:.2f}%",
                f"{r['p5']*100:.2f}%"
            ]))
        print("==========================================================")

# ---------------- Tableau simplifié final (pair / TP cible) ----------------
def print_simple_tp_table(rows: List[Dict[str, Any]]):
    """
    Tableau final simplifié des paires à trader :
      - PF >= 1.5
      - MDD_R < 8
    Colonnes: Pair / Session / TP cible / Trades / ExpectancyR / PF / MDD_R
    """
    try:
        from prettytable import PrettyTable
    except Exception:
        PrettyTable = None

    selected: List[Dict[str, Any]] = []
    for r in rows:
        if r["trades"] <= 0:
            continue
        if r["pf"] < 1.5 or r["mdd_r"] >= 8.0:
            continue

        # Déterminer le TP cible en fonction du poids
        tp = "-"
        if abs(r["w1"] - 1.0) < 1e-9:
            tp = "TP1"
        elif abs(r["w2"] - 1.0) < 1e-9:
            tp = "TP2"
        elif abs(r["w3"] - 1.0) < 1e-9:
            tp = "TP3"
        elif abs(r["w4"] - 1.0) < 1e-9:
            tp = "TP4"
        elif abs(r["w5"] - 1.0) < 1e-9:
            tp = "TP5"

        r2 = dict(r)
        r2["tp"] = tp
        selected.append(r2)

    selected_sorted = sorted(selected, key=lambda x: x["score"], reverse=True)

    if PrettyTable:
        t = PrettyTable()
        t.field_names = ["Pair", "Session", "TP", "Trades", "ExpectancyR", "PF", "MDD_R"]
        for r in selected_sorted:
            t.add_row([
                r["pair"],
                r["session"],
                r["tp"],
                r["trades"],
                f"{r['exp']:+.3f}R",
                f"{r['pf']:.2f}",
                f"{r['mdd_r']:.2f}R",
            ])
        print("\n===== PAIRES À TRADER ACTUELLEMENT (PF>=1.5, MDD_R<8) =====")
        print(t)
        print("============================================================")
    else:
        print("\n===== PAIRES À TRADER ACTUELLEMENT (PF>=1.5, MDD_R<8) =====")
        print("Pair\tSession\tTP\tTrades\tExpectancyR\tPF\tMDD_R")
        for r in selected_sorted:
            print("\t".join([
                r["pair"],
                r["session"],
                r["tp"],
                str(r["trades"]),
                f"{r['exp']:+.3f}",
                f"{r['pf']:.2f}",
                f"{r['mdd_r']:.2f}",
            ]))
        print("============================================================")

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs-file", default="pairs_5ers.txt", help="Fichier des paires (une par ligne ou CSV avec colonne pair/pairs)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date",   default="2025-12-31")
    ap.add_argument("--step", type=float, default=0.1, choices=[0.1], help="Pas de grille (fixé à 0.1)")
    args = ap.parse_args()

    pairs = load_pairs_from_file(args.pairs_file)
    if not pairs:
        print("Aucune paire trouvée.")
        sys.exit(0)

    d0 = parse_date(args.start_date)
    d1 = parse_date(args.end_date)

    # --- TOKYO ONLY ---
    sessions = ["TOKYO"]

    best_rows: List[Dict[str, Any]] = []

    with get_pg_conn() as c:
        for pair in pairs:
            pair = pair.strip().upper()
            if not pair:
                continue

            # 1) Collecte des trades par session (TOKYO only)
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
                    if (best_for_pair is None) or (row["score"] > best_for_pair["score"]):
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
                    "p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0, "p5": 0.0,
                    "pf": 0.0, "mdd_r": 0.0, "score": 0.0,
                })

    # 4) Affichage final (1 ligne par paire)
    print_final_best_table(best_rows)

    # 5) Tableau simplifié final (pair / TP cible)
    print_simple_tp_table(best_rows)
    
    print_session_csv(best_rows)

if __name__ == "__main__":
    main()
