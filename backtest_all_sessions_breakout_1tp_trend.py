#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New York Breakout + Pullback — Multi-session backtester (single TP per pair)
Now with a 4H trend filter at each session start:
- Read the last CLOSED 4H candle before the session start.
- If bull (close > open) → only allow LONGs in that session; if bear → only SHORTs; if doji → no filter.

Key rules:
- File session_pairs.txt: lines "SESSION,PAIR,TPx" (e.g., NY,EURUSD,TP1).
- Each pair has a single objective: TP1 or TP2 or TP3.
- Binary outcome: TP (before SL) else SL.
- Per-trade R-multiple: +k R if TPk is hit before SL, else -1 R.
- Sizing: risk % on available capital (equity - open risks).
- Fees: 2.5 USD per lot per transaction (entry and exit).

Outputs:
- Final table: TP (price) and Result (TP/SL).
- Global summary (trades, winrate, expectancy R, fees, final capital, MDD).
- Max Daily Drawdown (worst day, in $ and %).
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext

# ---------- TOGGLES ----------
SHOW_TRADES  = False
SHOW_MONTHLY = False

# ---------- PARAMETERS ----------
MIN_STOP_PIPS = 6.0
FEE_PER_LOT   = 2.5  # USD per lot per transaction (entry + exit both charged)

UTC = timezone.utc

# ---------- ENV / DB ----------
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

# ---------- Time ----------
def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def hm_utc(ms: int) -> str:
    dt = datetime.fromtimestamp(ms/1000, tz=UTC)
    return f"{dt.hour:02d}:{dt.minute:02d}"

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

# ---------- Session windows (UTC) ----------
def tokyo_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    debut = int((base + timedelta(hours=1)).timestamp()*1000)               # 01:00
    fin   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)   # 05:45
    return debut, fin

def london_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    debut = int((base + timedelta(hours=7)).timestamp()*1000)               # 07:00
    fin   = int((base + timedelta(hours=11, minutes=45)).timestamp()*1000)  # 11:45
    return debut, fin

def ny_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    debut = int((base + timedelta(hours=12)).timestamp()*1000)              # 12:00
    fin   = int((base + timedelta(hours=16, minutes=45)).timestamp()*1000)  # 16:45
    return debut, fin

def window_for_session(session: str, d: date) -> Tuple[int, int]:
    s = (session or "").strip().upper()
    if s == "TOKYO":
        return tokyo_signal_window(d)
    if s == "LONDON":
        return london_signal_window(d)
    if s in ("NY", "NEWYORK", "NEW_YORK"):
        return ny_signal_window(d)
    return tokyo_signal_window(d)

# ---------- Pair / price helpers ----------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    return 0.001 if pair.upper().endswith("JPY") else 0.00001

def pip_size_for(pair: str) -> float:
    p = pair.upper()
    if p.startswith("XAU"):
        return 0.01
    return 0.01 if p.endswith("JPY") else 0.0001

def contract_size_for(pair: str) -> float:
    return 100.0 if pair.upper().startswith("XAU") else 100_000.0

def fmt_price(pair: str, x: float) -> str:
    if pair.upper().endswith("JPY"):
        return f"{x:.3f}"
    if pair.upper().startswith("XAU"):
        return f"{x:.5f}"
    return f"{x:.5f}"

def fmt_target(pair: str, x: float) -> str:
    return fmt_price(pair, x)

# ---------- DB reads ----------
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

# NEW: read last CLOSED 4H before session start
def read_last_4h_before(conn, pair: str, session_start_ms: int) -> Optional[Dict]:
    """
    Returns the last CLOSED 4H candle BEFORE the session start (ts is the 4H candle OPEN).
    """
    t4h = table_name(pair, "4h")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t4h}
        WHERE ts < %s
        ORDER BY ts DESC
        LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (session_start_ms,))
            row = cur.fetchone()
            if not row:
                return None
            ts, o, h, l, c = row
            return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
    except Exception:
        conn.rollback()
        return None

# ---------- Conversions / notionals ----------
def fx_close_at(conn, pair: str, ts_ms: int) -> Optional[float]:
    t = table_name(pair, "15m")
    sql = f"SELECT close FROM {t} WHERE ts <= %s ORDER BY ts DESC LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ts_ms,))
            row = cur.fetchone()
            return float(row[0]) if row else None
    except Exception:
        conn.rollback()
        return None

def pip_value_per_lot_usd_at(conn, pair: str, entry_ts: int, entry_price: float) -> float:
    p = pair.upper()
    if p.startswith("XAU") and p.endswith("USD"):
        return contract_size_for(pair) * pip_size_for(pair)  # 100 * 0.01 = 1
    if p.endswith("USD"):
        return 10.0
    if p.endswith("JPY"):
        usdjpy = fx_close_at(conn, "USDJPY", entry_ts) or entry_price
        return 1000.0 / max(usdjpy, 1e-9)
    if p.startswith("USD"):
        return 10.0
    return 10.0

def notional_usd_at(conn, pair: str, entry_ts: int, entry_price: float, lot_size: float) -> float:
    p = pair.upper()
    cs = contract_size_for(pair)
    if p.endswith("USD"):
        return lot_size * cs * entry_price
    if p.startswith("USD"):
        return lot_size * cs
    if p.endswith("JPY"):
        usdjpy = fx_close_at(conn, "USDJPY", entry_ts) or entry_price
        notion_jpy = lot_size * cs * entry_price
        return notion_jpy / max(usdjpy, 1e-9)
    return lot_size * cs

# ---------- FSM / Trade ----------
@dataclass
class Trade:
    side: str      # "LONG" | "SHORT"
    entry_ts: int  # trigger candle OPEN ts (UTC, ms)
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    """
    Activate long/short after a strict close beyond the 1H range,
    require an antagonistic pullback close, then trigger on a wick >/< memorized extreme,
    SL on bar i-1 relative to the trigger.
    """
    long_active = False
    long_hh: Optional[float] = None
    long_pullback_idx: Optional[int] = None

    short_active = False
    short_ll: Optional[float] = None
    short_pullback_idx: Optional[int] = None

    for i, b in enumerate(c15):
        ts, o, h, l, c = b["ts"], b["open"], b["high"], b["low"], b["close"]

        if (not long_active) and (c > range_high):
            long_active = True
            long_hh = h
            long_pullback_idx = None

        if (not short_active) and (c < range_low):
            short_active = True
            short_ll = l
            short_pullback_idx = None

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

# ---------- Post-entry evaluation: TP-only ----------
def evaluate_trade_tp_only(conn, pair: str, tr: Trade, tp_level: str):
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        return entry, "SL", tr.entry_ts, -1.0

    lvl = tp_level.strip().upper()
    if tr.side == "LONG":
        if lvl == "TP1": target = entry + 1.0*r
        elif lvl == "TP2": target = entry + 2.0*r
        else: target = entry + 3.0*r
    else:
        if lvl == "TP1": target = entry - 1.0*r
        elif lvl == "TP2": target = entry - 2.0*r
        else: target = entry - 3.0*r

    future = read_15m_from(conn, pair, tr.entry_ts)
    closed_ts = tr.entry_ts
    hit_tp = False
    for b in future:
        ts, h, l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            sl_hit = (l <= sl + eps)
            tp_hit = (h >= target - eps)
        else:
            sl_hit = (h >= sl - eps)
            tp_hit = (l <= target + eps)

        if sl_hit and not tp_hit:
            closed_ts = ts
            return target, "SL", closed_ts, -1.0
        if tp_hit:
            closed_ts = ts
            k = 1 if lvl == "TP1" else 2 if lvl == "TP2" else 3
            return target, "TP", closed_ts, float(k)

    return target, "SL", closed_ts, -1.0

# ---------- Display ----------
def show_table(rows):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = [
        "Pair","Session","TP aim",
        "1H High","1H Low",
        "Entry (UTC)","Entry","SL","Stop (pips)",
        "TP (price)","Result","R-mult",
        "Lots","Lev","Fees $","PnL $","Equity $",
        "Closed (UTC)"
    ]
    for r in rows:
        t.add_row(r)
    print(t)

def print_summary(total_trades:int, wins:int, losses:int,
                  expectancy_R: float,
                  start_cap:float, equity:float, total_fees:float,
                  mdd_abs:float, mdd_pct:float,
                  worst_daily_abs: float, worst_daily_pct: float, worst_day: Optional[str],
                  label:str="ALL"):
    wl_pct = (wins/(wins+losses)*100) if (wins+losses)>0 else 0.0
    wl_str = f"{wins}/{wins+losses} = {wl_pct:.2f}%" if (wins+losses)>0 else "N/A"
    print(f"\n===== SUMMARY ({label}) =====")
    print(f"Trades (entries found):   {total_trades}")
    print(f"Winrate:                   {wl_str}")
    print(f"Expectancy (R):            {expectancy_R:+.3f}R")
    print(f"Total fees:                ${total_fees:,.2f}")
    print(f"Starting capital:          ${start_cap:,.2f}")
    print(f"Final capital:             ${equity:,.2f}")
    print(f"Max Drawdown:              ${mdd_abs:,.2f}  ({mdd_pct:.2f}%)")
    if worst_day:
        print(f"Max Daily Drawdown:        ${worst_daily_abs:,.2f}  ({worst_daily_pct:,.2f}%)  on {worst_day}")
    else:
        print(f"Max Daily Drawdown:        ${worst_daily_abs:,.2f}  ({worst_daily_pct:,.2f}%)")
    print("=====================================")

def print_monthly_breakdown(monthly):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = ["Month","Trades","Wins","Losses","PnL $","Fees $","Return %"]
    months = sorted(monthly.keys())
    for m in months:
        st = monthly[m]
        ret_pct = (st["pnl"] / st["equity_start"] * 100.0) if st["equity_start"] else 0.0
        t.add_row([
            m,
            st["trades"],
            st["wins"],
            st["losses"],
            f"{st['pnl']:,.2f}",
            f"{st['fees']:,.2f}",
            f"{ret_pct:.2f}%"
        ])
    print("\n===== MONTHLY BREAKDOWN =====")
    print(t)
    print("=============================")

# ---------- Engine ----------
@dataclass
class PreparedTrade:
    pair: str
    session: str
    tp_level: str
    tr: Trade
    tp_price: float
    outcome: str        # "TP" | "SL"
    closed_ts: Optional[int]
    r_mult: float
    risk_amount: float = 0.0
    lot_size: float = 0.0
    fee_total: float = 0.0
    lev_used: float = 0.0
    pnl_gross: float = 0.0
    pnl_net: float = 0.0

def run_all(conn,
            session_pairs: List[Tuple[str, str, str]],
            start: date, end: date,
            start_capital: float, risk_pct: float,
            fee_per_lot: float,
            min_stop_pips: float):

    prepared: List[PreparedTrade] = []
    seen_sessions = set()  # max 1 trade per (SESSION, PAIR, DATE)

    for d in daterange(start, end):
        for sess, pair, tp_level in session_pairs:
            key = (sess.upper(), pair.upper(), d)
            if key in seen_sessions:
                continue

            c1 = read_first_1h(conn, pair, d)
            if not c1:
                continue
            rh, rl = c1["high"], c1["low"]

            s, e = window_for_session(sess, d)

            # ---- 4H TREND FILTER (recomputed at each session start) ----
            trend4 = read_last_4h_before(conn, pair, s)
            allowed_side: Optional[str] = None
            if trend4 is not None:
                if trend4["close"] > trend4["open"]:
                    allowed_side = "LONG"
                elif trend4["close"] < trend4["open"]:
                    allowed_side = "SHORT"
                else:
                    allowed_side = None  # doji → no filter

            c15 = read_15m_in(conn, pair, s, e)
            if not c15:
                continue

            tr = detect_first_trade_for_day(c15, rh, rl)
            if not tr:
                continue

            # consume this session/pair/day once a setup exists
            seen_sessions.add(key)

            # apply 4H filter
            if allowed_side is not None and tr.side != allowed_side:
                continue

            # min stop filter
            pip_sz = pip_size_for(pair)
            stop_pips = abs(tr.entry - tr.sl) / max(pip_sz, 1e-12)
            if stop_pips < min_stop_pips:
                continue

            tp_price, result, cl_ts, r_mult = evaluate_trade_tp_only(conn, pair, tr, tp_level)
            pt = PreparedTrade(pair=pair, session=sess, tp_level=tp_level,
                               tr=tr, tp_price=tp_price, outcome=result, closed_ts=cl_ts, r_mult=r_mult)
            prepared.append(pt)

    if not prepared:
        print("No trades found over the selected period.")
        print_summary(0, 0, 0, 0.0, start_capital, start_capital, 0.0, 0.0, 0.0, 0.0, 0.0, None, "ALL")
        return

    from collections import defaultdict
    evmap: Dict[int, List[Tuple[str, PreparedTrade]]] = defaultdict(list)
    for pt in prepared:
        evmap[pt.tr.entry_ts].append(("ENTRY", pt))
        if pt.closed_ts:
            evmap[pt.closed_ts].append(("CLOSE", pt))
    for ts in evmap:
        evmap[ts].sort(key=lambda x: (0 if x[0]=="ENTRY" else 1, x[1].pair))

    equity = float(start_capital)
    total_fees = 0.0
    peak_equity = equity
    max_dd_abs = 0.0
    max_dd_pct = 0.0
    total_trades = 0
    wins = 0
    losses = 0
    r_list: List[float] = []

    monthly: Dict[str, Dict[str, Any]] = {}

    def ensure_month(dt: date):
        key = f"{dt.year:04d}-{dt.month:02d}"
        if key not in monthly:
            monthly[key] = {
                "equity_start": None,
                "pnl": 0.0,
                "fees": 0.0,
                "trades": 0,
                "wins": 0,
                "losses": 0,
            }
        return key

    daily: Dict[str, Dict[str, float]] = {}
    worst_daily_dd_abs = 0.0
    worst_daily_dd_pct = 0.0
    worst_daily_day: Optional[str] = None

    hl_cache: Dict[Tuple[str, date], Tuple[float, float]] = {}
    for d in daterange(start, end):
        for _, pair, _ in session_pairs:
            c1 = read_first_1h(conn, pair, d)
            if c1: hl_cache[(pair, d)] = (c1["high"], c1["low"])

    open_trades: List[PreparedTrade] = []
    def open_risk_total() -> float:
        return sum(t.risk_amount for t in open_trades)

    final_rows_with_sort: List[Tuple[int, int, List[Any]]] = []

    for ts in sorted(evmap.keys()):
        # ENTRIES
        for etype, pt in evmap[ts]:
            if etype != "ENTRY":
                continue

            if any(ot.pair == pt.pair for ot in open_trades):
                continue

            available = max(equity - open_risk_total(), 0.0)
            risk_amount = available * (risk_pct/100.0)

            pip_sz = pip_size_for(pt.pair)
            stop_pips = abs(pt.tr.entry - pt.tr.sl) / max(pip_sz, 1e-12)

            pip_val = pip_value_per_lot_usd_at(conn, pt.pair, pt.tr.entry_ts, pt.tr.entry)
            lot_size = 0.0
            if stop_pips > 0 and pip_val > 0 and risk_amount > 0:
                lot_size = risk_amount / (stop_pips * pip_val)

            fee_total = float(fee_per_lot) * lot_size * 2.0
            notion_usd = notional_usd_at(conn, pt.pair, pt.tr.entry_ts, pt.tr.entry, lot_size)
            lev_used   = (notion_usd / equity) if equity > 0 else 0.0

            pnl_gross = risk_amount * pt.r_mult

            pt.risk_amount = risk_amount
            pt.lot_size    = lot_size
            pt.fee_total   = fee_total
            pt.lev_used    = lev_used
            pt.pnl_gross   = pnl_gross

            entry_date = datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date()
            mk = ensure_month(entry_date)
            if monthly[mk]["equity_start"] is None:
                monthly[mk]["equity_start"] = equity
            monthly[mk]["trades"] += 1
            total_trades += 1

            open_trades.append(pt)

        # CLOSES
        for etype, pt in evmap[ts]:
            if etype != "CLOSE":
                continue

            equity_before = equity
            pnl_net = pt.pnl_gross - pt.fee_total
            equity += pnl_net
            total_fees += pt.fee_total
            pt.pnl_net = pnl_net

            day_key = datetime.fromtimestamp((pt.closed_ts or ts)/1000, tz=UTC).date().isoformat()
            if day_key not in daily:
                daily[day_key] = {"start": equity_before, "min": equity_before, "end": equity_before}
            drec = daily[day_key]
            drec["end"] = equity
            if equity < drec["min"]:
                drec["min"] = equity

            if pt.outcome == "TP":
                wins += 1
            else:
                losses += 1
            r_list.append(pt.r_mult)

            if equity > peak_equity:
                peak_equity = equity
            dd_abs = peak_equity - equity
            if dd_abs > max_dd_abs:
                max_dd_abs = dd_abs
                max_dd_pct = (dd_abs / peak_equity) * 100.0 if peak_equity > 0 else 0.0

            entry_date = datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date()
            mk = ensure_month(entry_date)
            monthly[mk]["pnl"]  += pnl_net
            monthly[mk]["fees"] += pt.fee_total
            if pt.outcome == "TP": monthly[mk]["wins"] += 1
            else:                   monthly[mk]["losses"] += 1

            if pt in open_trades:
                open_trades.remove(pt)

            day = datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date()
            rh, rl = hl_cache.get((pt.pair, day), (pt.tr.entry, pt.tr.entry))
            pip_sz = pip_size_for(pt.pair)
            stop_pips = abs(pt.tr.entry - pt.tr.sl) / max(pip_sz, 1e-12)

            row = [
                pt.pair,
                pt.session,
                pt.tp_level.upper(),
                fmt_price(pt.pair, rh), fmt_price(pt.pair, rl),
                f"{iso_utc(pt.tr.entry_ts).split('T')[0]} {hm_utc(pt.tr.entry_ts)}",
                fmt_price(pt.pair, pt.tr.entry),
                fmt_price(pt.pair, pt.tr.sl),
                f"{stop_pips:.1f}",
                fmt_target(pt.pair, pt.tp_price),
                pt.outcome,
                f"{pt.r_mult:+.2f}R",
                f"{pt.lot_size:.3f}",
                f"{pt.lev_used:.2f}x",
                f"{pt.fee_total:.2f}",
                f"{pnl_net:+.2f}",
                f"{equity:,.2f}",
                f"{iso_utc(pt.closed_ts).split('T')[0]} {hm_utc(pt.closed_ts)}" if pt.closed_ts else ""
            ]
            final_rows_with_sort.append((pt.closed_ts or pt.tr.entry_ts, pt.tr.entry_ts, row))

    worst_daily_dd_abs = 0.0
    worst_daily_dd_pct = 0.0
    worst_daily_day = None
    for dk, rec in daily.items():
        start_e = rec["start"]
        min_e   = rec["min"]
        if start_e > 0:
            dd_abs = max(0.0, start_e - min_e)
            dd_pct = (dd_abs / start_e) * 100.0
            if dd_abs > worst_daily_dd_abs:
                worst_daily_dd_abs = dd_abs
                worst_daily_dd_pct = dd_pct
                worst_daily_day = dk

    if final_rows_with_sort and SHOW_TRADES:
        final_rows_with_sort.sort(key=lambda t: (t[0], t[1], t[2][0]))
        show_table([r for _, __, r in final_rows_with_sort])
    elif not final_rows_with_sort:
        print("No closed trades (unexpected).")

    expectancy_R = (sum(r_list) / len(r_list)) if r_list else 0.0

    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)

    print_summary(
        total_trades, wins, losses, expectancy_R,
        start_capital, equity, total_fees,
        max_dd_abs, max_dd_pct,
        worst_daily_dd_abs, worst_daily_dd_pct, worst_daily_day,
        "ALL"
    )

# ---------- Session file loader ----------
def load_session_file(path: str) -> List[Tuple[str, str, str]]:
    out: List[Tuple[str, str, str]] = []
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        return out

    with open(path, "r", newline="") as f:
        for raw in f:
            line = raw.strip()
            if (not line) or line.startswith("#"):
                continue
            parts = [x.strip() for x in line.split(",")]
            if len(parts) < 3:
                continue
            sess = parts[0].upper()
            pair = parts[1].upper()
            tp   = parts[2].upper()
            if tp not in ("TP1","TP2","TP3"):
                continue
            out.append((sess, pair, tp))
    return out

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-file", default="session_pairs.txt",
                    help="File with lines SESSION,PAIR,TPx (e.g. NY,EURUSD,TP1)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--capital-start", type=float, default=100000.0, help="Starting capital")
    ap.add_argument("--risk-pct", type=float, default=1.0, help="Risk per trade in % of available capital")
    ap.add_argument("--fee-per-lot", type=float, default=FEE_PER_LOT,
                    help=f"USD per lot per transaction (default {FEE_PER_LOT})")
    ap.add_argument("--min-stop-pips", type=float, default=MIN_STOP_PIPS,
                    help=f"Minimum stop size in pips to accept a trade (default {MIN_STOP_PIPS})")
    a = ap.parse_args()

    session_pairs = load_session_file(a.session_file)
    if not session_pairs:
        print("No valid lines in the session file (SESSION,PAIR,TPx).")
        sys.exit(0)

    pairs_list = ", ".join(sorted({p for _, p, _ in session_pairs}))
    print("\n==============================")
    print(f"RUNNING BACKTEST for: {pairs_list}")
    print("==============================")

    with get_pg_conn() as c:
        run_all(
            c,
            session_pairs=session_pairs,
            start=parse_date(a.start_date),
            end=parse_date(a.end_date),
            start_capital=a.capital_start,
            risk_pct=a.risk_pct,
            fee_per_lot=a.fee_per_lot,
            min_stop_pips=a.min_stop_pips
        )

if __name__ == "__main__":
    main()
