#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Pairs — Best (Session × w1,w2,w3) per Pair — Expectancy Recap (1 ligne par paire)

RÈGLE (mise à jour) :
- On détermine, pour chaque paire, le meilleur moment (session) pour LANCER un trade.
- Par session (TOKYO/LONDON/NY), on prend AU PLUS 1 trade par (paire, jour) si l'entrée se produit dans la fenêtre de la session.
- Peu importe quand le trade se termine (SL/RR3), on NE bloque PAS le jour suivant (pas d'anti-overlap cross-day).

Entrées & cibles (inchangé, sauf SL de l’entrée cf. plus bas) :
- Entrées: mêmes règles (break strict, pullback antagoniste, entrée wick), SL = extrême (low/high) depuis le pullback (inclus).
- Cibles: RR1 / RR2 / RR3 (timestamps), arrêt au 1er SL ou RR3 (pour l’évaluation des hits).
- WIN/LOSS: WIN si TP1 < SL, sinon LOSS (indépendant des poids).
- R-multiple: application événementielle des partiels (w1,w2,w3), w1+w2+w3=1.
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
    start = int((base + timedelta(hours=1)).timestamp()*1000)                # 01:00
    end   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)   # 05:45
    return start, end

def _uk_last_sunday(year: int, month: int) -> date:
    # returns date of last sunday of given month
    # start from last day of month and go backwards
    last_day = date(year, month, 1)
    # move to first day of next month then step back one day to get last day
    if month == 12:
        last_day = date(year, 12, 31)
    else:
        last_day = date(year, month+1, 1) - timedelta(days=1)
    # go back until sunday
    while last_day.weekday() != 6:  # Sunday == 6
        last_day -= timedelta(days=1)
    return last_day

def uk_is_dst(d: date) -> bool:
    """
    UK DST (BST) runs from last Sunday in March (inclusive) to last Sunday in October (exclusive).
    Return True if date d is in DST (BST).
    """
    year = d.year
    start = _uk_last_sunday(year, 3)   # last sunday march
    end   = _uk_last_sunday(year, 10)  # last sunday october
    # DST: from start (inclusive) to end (exclusive)
    return (d >= start) and (d < end)

def london_signal_window(d: date) -> Tuple[int, int]:
    """
    Session windows for LONDON adapted to UK DST:
    - If DST (summer / BST): first H1 starts at 07:00 UTC (which is local 08:00 BST).
      Scan 15m bars between 08:00 - 12:45 UTC.
    - If non-DST (winter / GMT): first H1 starts at 08:00 UTC (local 08:00 GMT).
      Scan 15m bars between 09:00 - 13:45 UTC.
    Returns (start_ms, end_ms) for the 15m scanning window.
    Note: the H1 "reference" candle is read by read_session_1h which uses the H1 start hour below.
    """
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    if uk_is_dst(d):
        # summer / BST
        start = int((base + timedelta(hours=8)).timestamp()*1000)               # 08:00 UTC window start
        end   = int((base + timedelta(hours=12, minutes=45)).timestamp()*1000)  # 12:45 UTC window end
    else:
        # winter / GMT
        start = int((base + timedelta(hours=9)).timestamp()*1000)               # 09:00 UTC window start
        end   = int((base + timedelta(hours=13, minutes=45)).timestamp()*1000)  # 13:45 UTC window end
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

# ---------- Read session-specific 1H reference ----------
def read_session_1h(conn, pair: str, d: date, session: str) -> Optional[Dict]:
    """
    Read the H1 'reference' candle for the given session:
    - TOKYO: first H1 = 01:00 UTC (same as previous logic)
    - LONDON: H1 start depends on UK DST:
        * DST (summer/BST): H1 = 07:00 UTC (local 08:00)
        * non-DST (winter/GMT): H1 = 08:00 UTC (local 08:00)
    - NY: first H1 = 13:00 UTC (same as previous logic if needed)
    Returns dict with ts, open, high, low, close or None.
    """
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    s = (session or "").strip().upper()
    if s == "TOKYO":
        h = 1
    elif s == "LONDON":
        # in summer (BST), first local 08:00 is 07:00 UTC -> we want H1 starting at 07:00 UTC
        # user requested: first H1 at 07:00 UTC in summer, 08:00 UTC in winter.
        if uk_is_dst(d):
            h = 7
        else:
            h = 8
    elif s in ("NY","NEWYORK","NEW_YORK"):
        h = 13
    else:
        h = 1

    ts_h1 = int((base + timedelta(hours=h)).timestamp()*1000)
    t1h = table_name(pair, "1h")
    sql = f"SELECT ts, open, high, low, close FROM {t1h} WHERE ts = %s LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ts_h1,))
            row = cur.fetchone()
            if not row:
                return None
            ts, o, h_high, l, c = row
            return {"ts": int(ts), "open": float(o), "high": float(h_high), "low": float(l), "close": float(c)}
    except Exception:
        conn.rollback()
        return None

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    return 0.001 if pair.upper().endswith("JPY") else 0.00001

def pip_size_for(pair: str) -> float:
    if pair.upper().startswith("XAU"):
        return 0.01
    return 0.01 if pair.upper().endswith("JPY") else 0.0001

# ---------------- DB Readers ----------------
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
    long_min_low_since_pullback: Optional[float] = None  # suivi du plus bas depuis pullback (inclus)

    short_active = False
    short_ll: Optional[float] = None
    short_pullback_idx: Optional[int] = None
    short_max_high_since_pullback: Optional[float] = None  # suivi du plus haut depuis pullback (inclus)

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

# ---------------- After-entry evaluation (CORE LOGIC — DO NOT CHANGE) ----------------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    """
    Enregistre les timestamps de RR1/RR2/RR3/SL; stop au premier SL ou RR3.
    """
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        targets = {"RR1": entry, "RR2": entry, "RR3": entry}
        results = {k: "SL" for k in ["RR1","RR2","RR3"]}
        return targets, results, {"SL": None, "RR1": None, "RR2": None, "RR3": None}, None

    if tr.side == "LONG":
        t1, t2, t3 = entry + 1.0*r, entry + 2.0*r, entry + 3.0*r
    else:
        t1, t2, t3 = entry - 1.0*r, entry - 2.0*r, entry - 3.0*r

    targets = {"RR1": t1, "RR2": t2, "RR3": t3}
    hit_time: Dict[str, Optional[int]] = {"SL": None, "RR1": None, "RR2": None, "RR3": None}

    future = read_15m_from(conn, pair, tr.entry_ts)
    for b in future:
        ts,h,l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            sl_hit  = (l <= sl + eps)
            rr1_hit = (h >= t1 - eps)
            rr2_hit = (h >= t2 - eps)
            rr3_hit = (h >= t3 - eps)
        else:
            sl_hit  = (h >= sl - eps)
            rr1_hit = (l <= t1 + eps)
            rr2_hit = (l <= t2 + eps)
            rr3_hit = (l <= t3 + eps)

        if hit_time["SL"]  is None and sl_hit:  hit_time["SL"]  = ts
        if hit_time["RR1"] is None and rr1_hit: hit_time["RR1"] = ts
        if hit_time["RR2"] is None and rr2_hit: hit_time["RR2"] = ts
        if hit_time["RR3"] is None and rr3_hit: hit_time["RR3"] = ts

        if (hit_time["SL"] is not None) or (hit_time["RR3"] is not None):
            break

    results: Dict[str, str] = {}
    sl_time = hit_time["SL"]
    for key in ["RR1","RR2","RR3"]:
        ttime = hit_time[key]
        results[key] = "TP" if (ttime is not None and (sl_time is None or ttime < sl_time)) else "SL"

    closed_ts = sl_time if sl_time is not None else hit_time["RR3"]
    return targets, results, hit_time, closed_ts

# ---------------- Partials (w1,w2,w3) -> R-multiple ----------------
def compute_r_and_close(hit_time: Dict[str, Optional[int]], w1: float, w2: float, w3: float) -> float:
    """
    Application temporelle des sorties partielles (w1+w2+w3=1):
      TP1: +w1 * 1R ; rem -= w1
      TP2: +w2 * 2R ; rem -= w2
      TP3: +w3 * 3R ; rem -= w3
      SL : -1R * rem
    Renvoie le R-multiple total.
    """
    t_sl = hit_time.get("SL")
    t1   = hit_time.get("RR1")
    t2   = hit_time.get("RR2")
    t3   = hit_time.get("RR3")

    events: List[Tuple[int, str]] = []
    if t1 is not None: events.append((t1, "TP1"))
    if t2 is not None: events.append((t2, "TP2"))
    if t3 is not None: events.append((t3, "TP3"))
    if t_sl is not None: events.append((t_sl, "SL"))
    events.sort(key=lambda x: x[0])

    rem = 1.0
    r   = 0.0

    for ts, ev in events:
        if ev == "TP1" and w1 > 0:
            r   += w1 * 1.0
            rem -= w1
            if rem <= 1e-12: break
        elif ev == "TP2" and w2 > 0:
            r   += w2 * 2.0
            rem -= w2
            if rem <= 1e-12: break
        elif ev == "TP3" and w3 > 0:
            r   += w3 * 3.0
            rem -= w3
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
    hits: Dict[str, Optional[int]]  # {"SL": ts|None, "RR1": ts|None, "RR2": ts|None, "RR3": ts|None}

def collect_trades_for_session(conn, pair: str, start: date, end: date, session: str) -> List[BareTrade]:
    """
    Règle: on prend AU PLUS UN trade par (paire, jour) si l'entrée est dans la fenêtre de la session.
    Cross-day blocking while last trade is open (behavior kept for compatibility).
    """
    trades: List[BareTrade] = []

    block_until_ts: Optional[int] = None  # ts de clôture (SL ou RR3) du dernier trade

    for d in daterange(start, end):
        # 2) Fenêtre 15m de la session pour ce jour
        s, e = window_for_session(session, d)

        # Si un trade précédent est encore "ouvert" au début de cette fenêtre, on saute ce jour
        if block_until_ts is not None and s <= block_until_ts:
            continue

        # 1) Range H1 de session (varie selon session; pour LONDON on prend 07:00 UTC en été / 08:00 UTC en hiver)
        c1 = read_session_1h(conn, pair, d, session)
        if not c1:
            continue
        rh, rl = c1["high"], c1["low"]

        c15 = read_15m_in(conn, pair, s, e)
        if not c15:
            continue

        # 3) Détecte le PREMIER trade dans la fenêtre (un seul par jour)
        tr = detect_first_trade_for_day(c15, rh, rl)
        if not tr:
            continue

        # 4) Enregistre les hits pour calculer R/TP% ET récupérer le closed_ts
        _, _, hits, closed_ts = evaluate_trade_after_entry(conn, pair, tr)
        trades.append(BareTrade(hits=hits))

        # 5) Cross-day blocking: BLOQUE jusqu'à SL ou RR3
        block_until_ts = closed_ts if closed_ts is not None else (2**62)

        # 6) Passe au jour suivant (jamais de 2e trade ce jour)
        continue

    return trades

# ---------------- Grille des poids ----------------
def weight_grid(step: float = 0.1):
    vals = [round(i*step, 1) for i in range(int(1/step)+1)]
    for w1 in vals:
        for w2 in vals:
            w3 = round(1.0 - w1 - w2, 1)
            if w3 < -1e-9:
                continue
            if abs(w1 + w2 + w3 - 1.0) <= 1e-9 and (0.0 <= w3 <= 1.0):
                yield (w1, w2, w3)

# ---------------- Stats pour une combinaison ----------------
def stats_for_weights(trades: List[BareTrade], w1: float, w2: float, w3: float) -> Dict[str, Any]:
    total = len(trades)
    if total == 0:
        return {"trades": 0, "winrate": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "exp": 0.0, "p1": 0.0, "p2": 0.0, "p3": 0.0}

    r_wins: List[float] = []
    r_losses_abs: List[float] = []

    tp1_cnt = tp2_cnt = tp3_cnt = 0

    for bt in trades:
        hits = bt.hits
        if reached_before(hits, "RR1"): tp1_cnt += 1
        if reached_before(hits, "RR2"): tp2_cnt += 1
        if reached_before(hits, "RR3"): tp3_cnt += 1

        r_mult = compute_r_and_close(hits, w1, w2, w3)

        # WIN/LOSS = TP1 avant SL
        if reached_before(hits, "RR1"):
            r_wins.append(r_mult)
        else:
            r_losses_abs.append(-r_mult)

    wins = len(r_wins)
    losses = len(r_losses_abs)
    winrate = (wins / total) if total > 0 else 0.0
    avg_win = (sum(r_wins)/wins) if wins > 0 else 0.0
    avg_loss = (sum(r_losses_abs)/losses) if losses > 0 else 0.0
    expectancy = winrate * avg_win - (1.0 - winrate) * avg_loss

    p1 = tp1_cnt / total
    p2 = tp2_cnt / total
    p3 = tp3_cnt / total

    return {"trades": total, "winrate": winrate, "avg_win": avg_win, "avg_loss": avg_loss, "exp": expectancy, "p1": p1, "p2": p2, "p3": p3}

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
            "Pair","Session","w1","w2","w3",
            "Trades","Winrate","AvgWinR","AvgLossR","ExpectancyR",
            "TP1%","TP2%","TP3%"
        ]
        for r in rows_sorted:
            t.add_row([
                r["pair"],
                r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}",
                r["trades"],
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}R",
                f"{r['avg_loss']:.3f}R",
                f"{r['exp']:+.3f}R",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%"
            ])
        print("\n===== BEST COMBO PAR PAIRE — trié par Expectancy (R) =====")
        print(t)
        print("==========================================================")
    else:
        # Fallback
        print("\nPair\tSession\tw1\tw2\tw3\tTrades\tWinrate\tAvgWinR\tAvgLossR\tExpectancyR\tTP1%\tTP2%\tTP3%")
        for r in rows_sorted:
            print("\t".join([
                r["pair"], r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}",
                str(r["trades"]),
                f"{r['winrate']*100:.2f}%",
                f"{r['avg_win']:.3f}",
                f"{r['avg_loss']:.3f}",
                f"{r['exp']:+.3f}",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%"
            ]))
        print("==========================================================")

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

    # --- LONDON ONLY (changed per request) ---
    sessions = ["LONDON"]

    best_rows: List[Dict[str, Any]] = []

    with get_pg_conn() as c:
        for pair in pairs:
            pair = pair.strip().upper()
            if not pair:
                continue

            # 1) Collecte des trades par session (LONDON only)
            session_trades: Dict[str, List[BareTrade]] = {}
            for sess in sessions:
                print(f"[{pair}] Collecte trades — {sess} ...")
                session_trades[sess] = collect_trades_for_session(c, pair, d0, d1, sess)

            # 2) Parcourt la grille (w1,w2,w3) pour chaque session et retient la meilleure combinaison
            best_for_pair: Optional[Dict[str, Any]] = None

            for sess in sessions:
                trades = session_trades[sess]
                for (w1, w2, w3) in weight_grid(step=args.step):
                    st = stats_for_weights(trades, w1, w2, w3)
                    row = {
                        "pair": pair,
                        "session": sess,
                        "w1": w1, "w2": w2, "w3": w3,
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
                    "w1": 0.0, "w2": 0.0, "w3": 1.0,
                    "trades": 0, "winrate": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "exp": 0.0,
                    "p1": 0.0, "p2": 0.0, "p3": 0.0
                })

    # 4) Affichage final (1 ligne par paire)
    print_final_best_table(best_rows)

if __name__ == "__main__":
    main()
