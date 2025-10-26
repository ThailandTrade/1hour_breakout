#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Pairs — Best (Session × w1,w2,w3) per Pair — Expectancy Recap (1 line per pair)

Rules (UTC, fixed):
- Sessions:
    * TOKYO  : start 00:00, duration 6h → 1H base = 00:00–01:00, 15m window = 01:00–06:00
    * LONDON : start 07:00, duration 6h → 1H base = 07:00–08:00, 15m window = 08:00–13:00
    * NY     : start 12:00, duration 6h → 1H base = 12:00–13:00, 15m window = 13:00–18:00
- Outside these windows: ignored (no default session).

Strategy:
- Entry logic: strict break of 1H range, antagonistic pullback close, entry on wick >/< memorized extreme,
  SL = previous bar (i-1). First valid trade per day & session. No overlap: next entry must be after previous close.
- Targets: RR1/RR2/RR3 timestamps; stop at first SL or when RR3 is hit.
- WIN/LOSS label: WIN if TP1 < SL, else LOSS (independent of weights).
- Partials: event-ordered (w1,w2,w3), w1+w2+w3=1; SL applies -1R on remaining fraction.

I/O:
- Read pairs from --pairs-file (default: pairs.txt); one per line, or CSV with 'pair'/'pairs' column. Deduped.
- No sizing, no fees: pure R optimization.

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

# ----------- USER TUNABLES (top-level params) -----------
# Minimum allowed stop size (in pips). Trades with smaller stop are ignored.
MIN_PIPS = 0.0
# --------------------------------------------------------

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

# ---- Sessions (fixed UTC windows) ----
SESSION_SPECS = {
    "TOKYO":  {"start_hour": 0,  "duration_h": 6},
    "LONDON": {"start_hour": 7,  "duration_h": 6},
    "NY":     {"start_hour": 12, "duration_h": 6},
}

def session_bounds_utc(session: str, d: date) -> Tuple[int, int, int, int]:
    """
    Returns (base_start_ms, base_end_ms, m15_start_ms, m15_end_ms) for the given session/day.
    - base (1H)  : [session_start, session_start+1h)
    - m15 window : [base_end, session_start+duration)  (end exclusive)
    Raises ValueError if session is invalid.
    """
    s = (session or "").strip().upper()
    if s not in SESSION_SPECS:
        raise ValueError(f"Invalid session '{session}'. Valid: TOKYO, LONDON, NY")
    spec = SESSION_SPECS[s]
    base_dt = datetime(d.year, d.month, d.day, spec["start_hour"], 0, tzinfo=UTC)
    base_start = int(base_dt.timestamp()*1000)
    base_end   = int((base_dt + timedelta(hours=1)).timestamp()*1000)
    m15_start  = base_end
    m15_end    = int((base_dt + timedelta(hours=spec["duration_h"]+1)).timestamp()*1000)  # +1h already in m15_start
    return base_start, base_end, m15_start, m15_end

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    up = pair.upper()
    if up.startswith("XAU"):
        return 0.01   # align with XAU pip granularity
    return 0.001 if up.endswith("JPY") else 0.0001

def pip_size_for(pair: str) -> float:
    if pair.upper().startswith("XAU"):
        return 0.01
    return 0.01 if pair.upper().endswith("JPY") else 0.0001

def pips_between(pair: str, a: float, b: float) -> float:
    """Absolute distance between two prices expressed in pips."""
    return abs(a - b) / pip_size_for(pair)

# ---------------- DB Readers ----------------
def read_exact_1h_by_ts(conn, pair: str, ts_ms: int) -> Optional[Dict]:
    t1h = table_name(pair, "1h")
    sql = f"SELECT ts, open, high, low, close FROM {t1h} WHERE ts = %s LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ts_ms,))
            row = cur.fetchone()
            if not row: return None
            ts, o, h, l, c = row
            return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
    except Exception:
        conn.rollback(); return None

def read_15m_between(conn, pair: str, start_ms: int, end_ms_excl: int) -> List[Dict]:
    """
    Select closed 15m bars with ts in [start_ms, end_ms_excl).
    """
    t15 = table_name(pair, "15m")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t15}
        WHERE ts >= %s AND ts < %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms, end_ms_excl))
            rows = cur.fetchall()
            return [{"ts": int(ts), "open": float(o), "high": float(h),
                     "low": float(l), "close": float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

def read_15m_from_inclusive(conn, pair: str, start_ms: int) -> List[Dict]:
    """
    Select future 15m bars with ts >= start_ms (includes the trigger bar).
    """
    t15 = table_name(pair, "15m")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t15}
        WHERE ts >= %s
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

# ---------------- FSM / Trade (CORE LOGIC — DO NOT CHANGE) ----------------
@dataclass
class Trade:
    side: str              # "LONG" | "SHORT"
    entry_ts: int          # ts OPEN UTC of trigger bar
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    """
    Activation long/short + antagonistic pullback (close opposite) + wick trigger; SL = bar i-1.
    First valid detection in the provided 15m window.
    """
    long_active = False
    long_hh: Optional[float] = None
    long_pullback_idx: Optional[int] = None

    short_active = False
    short_ll: Optional[float] = None
    short_pullback_idx: Optional[int] = None

    for i, b in enumerate(c15):
        ts, o, h, l, c = b["ts"], b["open"], b["high"], b["low"], b["close"]

        # Break activation
        if (not long_active) and (c > range_high):
            long_active = True
            long_hh = h
            long_pullback_idx = None

        if (not short_active) and (c < range_low):
            short_active = True
            short_ll = l
            short_pullback_idx = None

        # Long path
        if long_active:
            prev_hh = long_hh
            if long_pullback_idx is None and (c < o):
                long_pullback_idx = i
            if (prev_hh is not None) and (long_pullback_idx is not None) and (i > long_pullback_idx) and (h > prev_hh) and (i >= 1):
                entry_price = prev_hh
                sl_price = c15[i-1]["low"]
                return Trade("LONG", ts, entry_price, sl_price)
            if (long_hh is None) or (h > long_hh):
                long_hh = h

        # Short path
        if short_active:
            prev_ll = short_ll
            if short_pullback_idx is None and (c > o):
                short_pullback_idx = i
            if (prev_ll is not None) and (short_pullback_idx is not None) and (i > short_pullback_idx) and (l < prev_ll) and (i >= 1):
                entry_price = prev_ll
                sl_price = c15[i-1]["high"]
                return Trade("SHORT", ts, entry_price, sl_price)
            if (short_ll is None) or (l < short_ll):
                short_ll = l

    return None

# ---------------- After-entry evaluation (CORE LOGIC — DO NOT CHANGE) ----------------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    """
    Record timestamps of RR1/RR2/RR3/SL; stop at first SL or when RR3 is hit.
    Includes the trigger bar (ts >= entry_ts). SL priority if timestamps tie.
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

    future = read_15m_from_inclusive(conn, pair, tr.entry_ts)
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
    Event-ordered partial exits (w1+w2+w3=1):
      TP1: +w1 * 1R ; remaining -= w1
      TP2: +w2 * 2R ; remaining -= w2
      TP3: +w3 * 3R ; remaining -= w3
      SL : -1R * remaining
    Returns total R multiple.
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

# ---------------- Core: generate trades (by session) ----------------
@dataclass
class BareTrade:
    hits: Dict[str, Optional[int]]  # {"SL": ts|None, "RR1": ts|None, "RR2": ts|None, "RR3": ts|None}

def collect_trades_for_session(conn, pair: str, start: date, end: date, session: str) -> List[BareTrade]:
    trades: List[BareTrade] = []
    last_close_ts: Optional[int] = None

    for d in daterange(start, end):
        try:
            base_start, base_end, m15_start, m15_end = session_bounds_utc(session, d)
        except ValueError:
            continue

        # Read the 1H base candle (must exist exactly at base_start)
        c1 = read_exact_1h_by_ts(conn, pair, base_start)
        if not c1:
            continue
        range_high, range_low = c1["high"], c1["low"]

        # 15m window within session: [base_end, session_end) — end exclusive
        c15 = read_15m_between(conn, pair, m15_start, m15_end)
        if not c15:
            continue

        tr = detect_first_trade_for_day(c15, range_high, range_low)
        if not tr:
            continue

        # ---- MIN PIPS FILTER (ignore tiny-stop trades) ----
        stop_pips = pips_between(pair, tr.entry, tr.sl)
        if stop_pips < MIN_PIPS:
            # Skip this trade due to too small stop size
            continue
        # ---------------------------------------------------

        # Anti-overlap: do not open if this entry is before the previous trade is closed
        if last_close_ts is not None and tr.entry_ts <= last_close_ts:
            continue

        _, _, hits, closed_ts = evaluate_trade_after_entry(conn, pair, tr)
        trades.append(BareTrade(hits=hits))

        if closed_ts is not None:
            last_close_ts = closed_ts

    return trades

# ---------------- Weight grid ----------------
def weight_grid(step: float = 0.1):
    vals = [round(i*step, 1) for i in range(int(1/step)+1)]
    for w1 in vals:
        for w2 in vals:
            w3 = round(1.0 - w1 - w2, 1)
            if w3 < -1e-9:
                continue
            if abs(w1 + w2 + w3 - 1.0) <= 1e-9 and (0.0 <= w3 <= 1.0):
                yield (w1, w2, w3)

# ---------------- Stats for a weight combo ----------------
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

        # WIN/LOSS = TP1 before SL
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

# ---------------- Load pairs ----------------
def load_pairs_from_file(path: str) -> List[str]:
    pairs: List[str] = []
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        return pairs
    # Try CSV with header
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
    # Fallback: one pair per line
    with open(path, "r") as f:
        for line in f:
            p = line.strip()
            if not p or p.startswith("#"):
                continue
            up = p.upper()
            if up not in pairs:
                pairs.append(up)
    return pairs

# ---------------- Print final recap (1 line / pair) ----------------
def print_final_best_table(rows: List[Dict[str, Any]]):
    try:
        from prettytable import PrettyTable
    except Exception:
        PrettyTable = None

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
        print("\n===== BEST COMBO PER PAIR — sorted by Expectancy (R) =====")
        print(t)
        print("==========================================================")
    else:
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
    ap.add_argument("--pairs-file", default="pairs.txt", help="Pairs file (one per line or CSV with pair/pairs column)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date",   default="2025-12-31")
    ap.add_argument("--step", type=float, default=0.1, choices=[0.1], help="Grid step (fixed to 0.1)")
    args = ap.parse_args()

    pairs = load_pairs_from_file(args.pairs_file)
    if not pairs:
        print("No pairs found.")
        sys.exit(0)

    d0 = parse_date(args.start_date)
    d1 = parse_date(args.end_date)
    sessions = ["TOKYO", "LONDON", "NY"]  # only these; nothing else

    best_rows: List[Dict[str, Any]] = []

    with get_pg_conn() as c:
        for pair in pairs:
            pair = pair.strip().upper()
            if not pair:
                continue

            # 1) Collect trades once per session
            session_trades: Dict[str, List[BareTrade]] = {}
            for sess in sessions:
                print(f"[{pair}] Collect trades — {sess} ...")
                session_trades[sess] = collect_trades_for_session(c, pair, d0, d1, sess)

            # 2) Sweep weight grid per session and keep the best combo for the pair
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

            # 3) Append the best line for the pair (even if 0 trades, for visibility)
            if best_for_pair and best_for_pair["trades"] > 0:
                best_rows.append(best_for_pair)
            else:
                best_rows.append({
                    "pair": pair, "session": "-",
                    "w1": 0.0, "w2": 0.0, "w3": 1.0,
                    "trades": 0, "winrate": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "exp": 0.0,
                    "p1": 0.0, "p2": 0.0, "p3": 0.0
                })

    # 4) Final display (1 line per pair)
    print_final_best_table(best_rows)

if __name__ == "__main__":
    main()
