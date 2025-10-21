#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New York Breakout + Pullback — Backtester (multi-pairs with available capital sizing, single RR target)

- Setup/entry: rules validated (initial STRICT close break, antagonistic pullback close,
  entry on wick >/< memorized extreme, no pullback and entry on the same bar).
- SL: bar just BEFORE the trigger bar (LONG: i-1 low; SHORT: i-1 high).
- After entry: trade runs until SL or TP at RR_TARGET.
- Fees: ~3.5 USD per lot per transaction (entry/exit). Fee per trade ≈ 2 * 3.5 * lots.
- Additions: Outcome label, Max Drawdown, Monthly breakdown, Pips column, JPY rounding.
- Final table sorted by **Closed Date/Time** (then Entry, then Pair).
- Sizing: risk % applied to **available capital = equity − sum(open risks)**.
- Single-pair mode: final recap table with EXACTLY the summary metrics.

CHANGES IN THIS VERSION (RR simple):
- Single TP at configurable RR_TARGET (no partials, no multi-RR columns).
- One trade at a time **per pair** (not global).
- Outcome is strictly WIN (TP) or LOSS (SL). No "NO".
- Summary shows Winrate, Avg Win RR, Avg Loss RR (abs), Expectancy (R) = p*AvgWinRR − (1−p)*AvgLossRR.
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext

# ----------- TOGGLES (displays) -----------
SHOW_TRADES  = True
SHOW_MONTHLY = False
# ------------------------------------------

# ----------- STRATEGY PARAM -----------
RR_TARGET_DEFAULT = 4.0
# --------------------------------------

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

def tokyo_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=1)).timestamp()*1000)                # 01:00 OPEN
    end   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)   # 05:45 OPEN
    return start, end

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    return 0.001 if pair.upper().endswith("JPY") else 0.00001

def pip_size_for(pair: str) -> float:
    # "pip" (not the tick).
    # - JPY: 0.01
    # - XAU*: 0.01 (XAUUSD etc.)
    # - other FX: 0.0001
    p = pair.upper()
    if p.startswith("XAU"):
        return 0.01
    return 0.01 if p.endswith("JPY") else 0.0001

def fmt_price(pair: str, x: float) -> str:
    if pair.upper().endswith("JPY"):
        return f"{x:.3f}"
    if pair.upper().startswith("XAU"):
        return f"{x:.5f}"
    return f"{x:.5f}"

# ---------- DB Readers ----------
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

# ---------- Conversions / notional / pip value ----------
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

def contract_size_for(pair: str) -> float:
    return 100.0 if pair.upper().startswith("XAU") else 100_000.0

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

# ---------------- FSM / Trade ----------------
@dataclass
class Trade:
    side: str              # "LONG" | "SHORT"
    entry_ts: int          # ts OPEN UTC of trigger bar
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
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

# ---------------- After-entry evaluation (RR simple) ----------------
@dataclass
class AfterEval:
    tp_price: float
    hit_time: Dict[str, Optional[int]]  # {"SL": ts|None, "TP": ts|None}
    closed_ts: int
    outcome: str  # "WIN" or "LOSS"

def evaluate_trade_after_entry(conn, pair: str, tr: Trade, rr_target: float) -> AfterEval:
    """Run trade until SL or TP (RR target). First hit wins. If neither hit, LOSS at end of data."""
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        return AfterEval(tp_price=entry, hit_time={"SL": tr.entry_ts, "TP": None}, closed_ts=tr.entry_ts, outcome="LOSS")

    tp = entry + rr_target * r if tr.side == "LONG" else entry - rr_target * r

    hit_time: Dict[str, Optional[int]] = {"SL": None, "TP": None}
    future = read_15m_from(conn, pair, tr.entry_ts)
    for b in future:
        ts,h,l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            sl_hit = (l <= sl + eps)
            tp_hit = (h >= tp - eps)
        else:
            sl_hit = (h >= sl - eps)
            tp_hit = (l <= tp + eps)

        if hit_time["SL"] is None and sl_hit: hit_time["SL"] = ts
        if hit_time["TP"] is None and tp_hit: hit_time["TP"] = ts

        if (hit_time["SL"] is not None) or (hit_time["TP"] is not None):
            break

    t_sl, t_tp = hit_time["SL"], hit_time["TP"]
    if t_tp is not None and (t_sl is None or t_tp < t_sl):
        return AfterEval(tp_price=tp, hit_time=hit_time, closed_ts=t_tp, outcome="WIN")
    if t_sl is not None:
        return AfterEval(tp_price=tp, hit_time=hit_time, closed_ts=t_sl, outcome="LOSS")

    # end-of-data without hits => LOSS
    last_ts = future[-1]["ts"] if future else tr.entry_ts
    return AfterEval(tp_price=tp, hit_time=hit_time, closed_ts=last_ts, outcome="LOSS")

# ---------------- Output ----------------
def show_table(rows):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = [
        "Pair","Side","1H High","1H Low",
        "Entry Date","Entry HM","Entry","SL","TP","Pips",
        "Outcome","R-mult","Lots","Lev","Fees $","PnL $","Equity $",
        "Closed Date","Closed HM"
    ]
    for r in rows:
        t.add_row(r)
    print(t)

def print_summary(total_trades:int, wins:int, losses:int,
                  avg_win_R: float, avg_loss_R: float, expectancy_R: float,
                  start_cap:float, equity:float, total_fees:float,
                  mdd_abs:float, mdd_pct:float, label:str="ALL"):
    wl_pct = (wins/(wins+losses)*100) if (wins+losses)>0 else 0.0
    wl_str = f"{wins}/{wins+losses} = {wl_pct:.2f}%" if (wins+losses)>0 else "N/A"
    print(f"\n===== SUMMARY ({label}) =====")
    print(f"Trades:                {total_trades}")
    print(f"Winrate:               {wl_str}")
    print(f"Avg Win RR:            {avg_win_R:.3f}R")
    print(f"Avg Loss RR:           {avg_loss_R:.3f}R")
    print(f"Expectancy (R):        {expectancy_R:+.3f}R")
    print(f"Total fees:            ${total_fees:,.2f}")
    print(f"Start capital:         ${start_cap:,.2f}")
    print(f"Final capital:         ${equity:,.2f}")
    print(f"Max Drawdown:          ${mdd_abs:,.2f}  ({mdd_pct:.2f}%)")
    print("============================")

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

# ---------------- Multi-pair engine ----------------
@dataclass
class PreparedTrade:
    pair: str
    tr: Trade
    tp_price: float
    hits: Dict[str, Optional[int]]   # {"SL":..., "TP":...}
    closed_ts: Optional[int]
    outcome: str                     # "WIN" | "LOSS"
    risk_amount: float = 0.0
    lot_size: float = 0.0
    fee_total: float = 0.0
    lev_used: float = 0.0
    r_mult: float = 0.0
    pnl_gross: float = 0.0
    pnl_net: float = 0.0

def run_all_pairs(conn, pairs: List[str], start: date, end: date,
                  start_capital: float, risk_pct: float, fee_per_lot: float, rr_target: float):
    prepared: List[PreparedTrade] = []
    for d in daterange(start, end):
        for pair in pairs:
            c1 = read_first_1h(conn, pair, d)
            if not c1: continue
            rh, rl = c1["high"], c1["low"]
            s, e = tokyo_signal_window(d)
            c15 = read_15m_in(conn, pair, s, e)
            if not c15: continue
            tr = detect_first_trade_for_day(c15, rh, rl)
            if not tr: continue
            aft = evaluate_trade_after_entry(conn, pair, tr, rr_target)
            prepared.append(PreparedTrade(pair, tr, aft.tp_price, aft.hit_time, aft.closed_ts, aft.outcome))

    if not prepared:
        print("No trades found across selected pairs.")
        print_summary(0, 0, 0, 0.0, 0.0, 0.0, start_capital, start_capital, 0.0, 0.0, 0.0, "ALL")
        if SHOW_MONTHLY:
            print_monthly_breakdown({})
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

    r_win: List[float] = []
    r_loss_abs: List[float] = []

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

    hl_cache: Dict[Tuple[str, date], Tuple[float, float]] = {}
    for d in daterange(start, end):
        for pair in pairs:
            c1 = read_first_1h(conn, pair, d)
            if c1: hl_cache[(pair, d)] = (c1["high"], c1["low"])

    # One open position PER PAIR
    open_trades: List[PreparedTrade] = []
    def open_risk_total() -> float:
        return sum(t.risk_amount for t in open_trades)

    final_rows_with_sort: List[Tuple[int, int, List[str]]] = []

    for ts in sorted(evmap.keys()):
        # ENTRIES
        for etype, pt in evmap[ts]:
            if etype != "ENTRY": continue

            # only one open position per PAIR
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

            # R multiple: +RR_TARGET on WIN, -1 on LOSS
            r_mult    = rr_target if pt.outcome == "WIN" else -1.0
            pnl_gross = risk_amount * r_mult

            pt.risk_amount = risk_amount
            pt.lot_size    = lot_size
            pt.fee_total   = fee_total
            pt.lev_used    = lev_used
            pt.r_mult      = r_mult
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
            if etype != "CLOSE": continue
            if pt not in open_trades:
                continue

            pnl_net = pt.pnl_gross - pt.fee_total
            equity_before = equity
            equity += pnl_net
            total_fees += pt.fee_total
            pt.pnl_net = pnl_net

            if pt.outcome == "WIN":
                r_win.append(rr_target)
                wins += 1
                outcome_label = "WIN"
                monthly[ensure_month(datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date())]["wins"] += 1
            else:
                r_loss_abs.append(1.0)
                losses += 1
                outcome_label = "LOSS"
                monthly[ensure_month(datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date())]["losses"] += 1

            if pt in open_trades:
                open_trades.remove(pt)

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

            day = datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date()
            rh, rl = hl_cache.get((pt.pair, day), (pt.tr.entry, pt.tr.entry))
            pip_sz = pip_size_for(pt.pair)
            stop_pips = abs(pt.tr.entry - pt.tr.sl) / max(pip_sz, 1e-12)

            row = [
                pt.pair,
                pt.tr.side,
                fmt_price(pt.pair, rh), fmt_price(pt.pair, rl),
                iso_utc(pt.tr.entry_ts).split("T")[0], hm_utc(pt.tr.entry_ts),
                fmt_price(pt.pair, pt.tr.entry), fmt_price(pt.pair, pt.tr.sl),
                fmt_price(pt.pair, pt.tp_price),
                f"{stop_pips:.1f}",
                outcome_label,
                f"{pt.r_mult:+.2f}R",
                f"{pt.lot_size:.3f}",
                f"{pt.lev_used:.2f}x",
                f"{pt.fee_total:.2f}",
                f"{pnl_net:+.2f}",
                f"{equity:,.2f}",
                iso_utc(pt.closed_ts).split('T')[0] if pt.closed_ts else "",
                hm_utc(pt.closed_ts) if pt.closed_ts else ""
            ]
            final_rows_with_sort.append((pt.closed_ts or pt.tr.entry_ts, pt.tr.entry_ts, row))

    # --- Final display (trade table only if SHOW_TRADES) ---
    if final_rows_with_sort and SHOW_TRADES:
        final_rows_with_sort.sort(key=lambda t: (t[0], t[1], t[2][0]))
        show_table([r for _, __, r in final_rows_with_sort])
    elif not final_rows_with_sort:
        print("No trades closed (unexpected).")

    # Expectancy
    def safe_mean(arr: List[float]) -> float:
        return sum(arr) / len(arr) if arr else 0.0
    winrate = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0
    avg_win_R  = safe_mean(r_win)
    avg_loss_R = safe_mean(r_loss_abs)
    expectancy_R = winrate * avg_win_R - (1.0 - winrate) * avg_loss_R

    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)
    print_summary(
        total_trades, wins, losses,
        avg_win_R, avg_loss_R, expectancy_R,
        start_capital, equity, total_fees,
        max_dd_abs, max_dd_pct, "ALL"
    )

# ---------------- Single-pair engine ----------------
def run_for_single_pair(conn, pair: str, start: date, end: date,
                        start_capital: float, risk_pct: float, fee_per_lot: float, rr_target: float) -> Dict[str, Any]:
    equity = float(start_capital)
    total_trades = 0
    wins = 0
    losses = 0
    total_fees = 0.0
    peak_equity = equity
    max_dd_abs = 0.0
    max_dd_pct = 0.0
    monthly: Dict[str, Dict[str, float]] = {}

    r_win: List[float] = []
    r_loss_abs: List[float] = []

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

    open_risk: float = 0.0
    rows_with_sort: List[Tuple[int, int, List[str]]] = []

    # Prevent overlaps — memorize the closure of the last accepted trade
    last_close_ts: Optional[int] = None

    hl_cache: Dict[date, Tuple[float, float]] = {}
    for d in daterange(start, end):
        c1 = read_first_1h(conn, pair, d)
        if c1: hl_cache[d] = (c1["high"], c1["low"])

    for d in daterange(start, end):
        c1 = read_first_1h(conn, pair, d)
        if not c1: continue
        rh, rl = c1["high"], c1["low"]
        s, e = tokyo_signal_window(d)
        c15 = read_15m_in(conn, pair, s, e)
        if not c15: continue
        tr = detect_first_trade_for_day(c15, rh, rl)
        if not tr: continue

        # if a previous trade isn't closed yet (in time), skip entry
        if last_close_ts is not None and tr.entry_ts <= last_close_ts:
            continue

        aft = evaluate_trade_after_entry(conn, pair, tr, rr_target)
        if aft.closed_ts is not None:
            last_close_ts = aft.closed_ts

        available   = max(equity - open_risk, 0.0)
        risk_amount = available * (risk_pct/100.0)

        pip_sz = pip_size_for(pair)
        stop_pips = abs(tr.entry - tr.sl) / max(pip_sz, 1e-12)

        pip_val  = pip_value_per_lot_usd_at(conn, pair, tr.entry_ts, tr.entry)

        lot_size = 0.0
        if stop_pips > 0 and pip_val > 0 and risk_amount > 0:
            lot_size = risk_amount / (stop_pips * pip_val)

        fee = float(fee_per_lot) * lot_size * 2.0

        notion_usd = notional_usd_at(conn, pair, tr.entry_ts, tr.entry, lot_size)
        lev_used   = (notion_usd / equity) if equity > 0 else 0.0

        r_mult = rr_target if aft.outcome == "WIN" else -1.0
        pnl_gross = risk_amount * r_mult

        # Expectancy collectors
        if aft.outcome == "WIN":
            r_win.append(rr_target)
            outcome_label = "WIN"
            wins += 1
        else:
            r_loss_abs.append(1.0)
            outcome_label = "LOSS"
            losses += 1

        open_risk += risk_amount

        pnl_net = pnl_gross - fee
        equity_before = equity
        equity += pnl_net
        total_fees += fee
        open_risk -= risk_amount
        if open_risk < 0: open_risk = 0.0

        total_trades += 1

        if equity > peak_equity:
            peak_equity = equity
        dd_abs = peak_equity - equity
        if dd_abs > max_dd_abs:
            max_dd_abs = dd_abs
            max_dd_pct = (dd_abs / peak_equity) * 100.0 if peak_equity > 0 else 0.0

        entry_date = datetime.fromtimestamp(tr.entry_ts/1000, tz=UTC).date()
        mk = ensure_month(entry_date)
        if monthly[mk]["equity_start"] is None:
            monthly[mk]["equity_start"] = equity_before
        monthly[mk]["pnl"]   += pnl_net
        monthly[mk]["fees"]  += fee
        monthly[mk]["trades"] += 1
        if aft.outcome == "WIN": monthly[mk]["wins"] += 1
        else:                    monthly[mk]["losses"] += 1

        ed, eh = iso_utc(tr.entry_ts).split("T")[0], hm_utc(tr.entry_ts)
        cd, ch = (iso_utc(aft.closed_ts).split("T")[0], hm_utc(aft.closed_ts)) if aft.closed_ts else ("","")

        rh_d, rl_d = hl_cache.get(d, (rh, rl))

        row: List[str] = [
            pair,
            tr.side,
            fmt_price(pair, rh_d), fmt_price(pair, rl_d),
            ed, eh,
            fmt_price(pair, tr.entry), fmt_price(pair, tr.sl),
            fmt_price(pair, aft.tp_price),
            f"{stop_pips:.1f}",
            outcome_label,
            f"{r_mult:+.2f}R",
            f"{lot_size:.3f}",
            f"{lev_used:.2f}x",
            f"{fee:.2f}",
            f"{pnl_net:+.2f}",
            f"{equity:,.2f}",
            cd, ch
        ]
        rows_with_sort.append((aft.closed_ts or tr.entry_ts, tr.entry_ts, row))

    if rows_with_sort and SHOW_TRADES:
        rows_with_sort.sort(key=lambda t: (t[0], t[1], t[2][0]))
        show_table([r for _, __, r in rows_with_sort])
    elif not rows_with_sort:
        print("No trades found.")

    def safe_mean(arr: List[float]) -> float:
        return sum(arr) / len(arr) if arr else 0.0

    winrate = (wins / (wins + losses)) if (wins + losses) > 0 else 0.0
    avg_win_R  = safe_mean(r_win)
    avg_loss_R = safe_mean(r_loss_abs)
    expectancy_R = winrate * avg_win_R - (1.0 - winrate) * avg_loss_R

    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)

    print_summary(
        total_trades, wins, losses,
        avg_win_R, avg_loss_R, expectancy_R,
        start_capital, equity, total_fees, max_dd_abs, max_dd_pct, pair
    )

    return {
        "pair": pair,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "avg_win_R": avg_win_R,
        "avg_loss_R": avg_loss_R,
        "expectancy_R": expectancy_R,
        "total_fees": total_fees,
        "start_cap": start_capital,
        "final_cap": equity,
        "mdd_abs": max_dd_abs,
        "mdd_pct": max_dd_pct,
    }

# --------------- Final recap (same info as summary) ---------------
def print_final_pair_summary_table(summaries: List[Dict[str, Any]]):
    if not summaries:
        return

    summaries_sorted = sorted(summaries, key=lambda s: s["expectancy_R"], reverse=True)

    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = [
        "Pair",
        "Trades (entries found)",
        "Winrate",
        "Avg Win RR",
        "Avg Loss RR",
        "Expectancy (R)",
        "Total fees",
        "Start capital",
        "Final capital",
        "Max Drawdown $",
        "Max Drawdown %"
    ]
    for s in summaries_sorted:
        wins   = s["wins"]
        losses = s["losses"]
        wl_str = f"{wins}/{wins+losses} = {(wins/(wins+losses)*100):.2f}%" if (wins+losses)>0 else "N/A"
        t.add_row([
            s["pair"],
            s["total_trades"],
            wl_str,
            f"{s['avg_win_R']:.3f}R",
            f"{s['avg_loss_R']:.3f}R",
            f"{s['expectancy_R']:+.3f}R",
            f"${s['total_fees']:,.2f}",
            f"${s['start_cap']:,.2f}",
            f"${s['final_cap']:,.2f}",
            f"${s['mdd_abs']:,.2f}",
            f"{s['mdd_pct']:.2f}%"
        ])
    print("\n===== FINAL RECAP (per pair) — Sorted by Expectancy (R) =====")
    print(t)
    print("==================================================================")

# ---------------- CLI ----------------
def load_pairs_from_csv(path: str) -> List[str]:
    pairs: List[str] = []
    if not os.path.exists(path):
        print(f"Error: {path} not found and --pairs not provided.")
        return pairs
    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames and "pair" in [h.lower() for h in reader.fieldnames]:
            for rec in reader:
                p = (rec.get("pair") or rec.get("PAIR") or "").strip()
                if p:
                    up = p.upper()
                    if up not in pairs:
                        pairs.append(up)
        else:
            f.seek(0)
            for i, row in enumerate(csv.reader(f)):
                if i == 0 and any(h.lower() in ("pair","pairs") for h in row):
                    continue
                if len(row) >= 2:
                    p = row[1].strip().upper()
                    if p and p not in pairs:
                        pairs.append(p)
    return pairs

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pairs", nargs="*", default=None,
                    help="One or more pairs (space-separated or commas inside tokens). If omitted: sequential read from pairs.txt")
    ap.add_argument("--pairs-file", default="pairs.txt", help="CSV with columns type,pair (default: pairs.txt)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--capital-start", type=float, default=100_000.0, help="Starting capital (default 100000)")
    ap.add_argument("--risk-pct", type=float, default=1.0, help="Risk per trade in percent of AVAILABLE capital (equity - open risks)")
    ap.add_argument("--rr-target", type=float, default=RR_TARGET_DEFAULT, help="TP expressed in R multiples (default 2.0)")
    ap.add_argument("--fee-per-lot", type=float, default=3.5, help="Fee USD per lot per transaction (entry/exit), default 3.5")
    a=ap.parse_args()

    rr_target = float(a.rr_target)

    if a.pairs:
        raw = []
        for tok in a.pairs:
            raw.extend(tok.split(","))
        pairs = [p.strip().upper() for p in raw if p.strip()]
        mode_multi = True
    else:
        pairs = load_pairs_from_csv(a.pairs_file)
        if not pairs:
            print("No pairs found in pairs file.")
            sys.exit(0)
        mode_multi = False

    with get_pg_conn() as c:
        if mode_multi:
            print("\n==============================")
            print(f"RUNNING BACKTEST FOR (multi): {', '.join(pairs)}")
            print("==============================")
            run_all_pairs(
                c, pairs,
                parse_date(a.start_date),
                parse_date(a.end_date),
                start_capital=a.capital_start,
                risk_pct=a.risk_pct,
                fee_per_lot=a.fee_per_lot,
                rr_target=rr_target
            )
        else:
            final_summaries: List[Dict[str, Any]] = []
            for pr in pairs:
                print("\n==============================")
                print(f"RUNNING BACKTEST FOR: {pr}")
                print("==============================")
                s = run_for_single_pair(
                    c, pr,
                    parse_date(a.start_date),
                    parse_date(a.end_date),
                    start_capital=a.capital_start,
                    risk_pct=a.risk_pct,
                    fee_per_lot=a.fee_per_lot,
                    rr_target=rr_target
                )
                final_summaries.append(s)
            print_final_pair_summary_table(final_summaries)

if __name__=="__main__":
    main()
