#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New York Breakout + Pullback — Backtester multi-sessions (TP unique par paire)

CHANGES (TYPE-aware lot sizing, no per-symbol specs):
- Session file can optionally include a column TYPE (FOREX | METAL | INDEX | CRYPTO).
- TYPE overrides symbol-based inference for sizing (tick_size, point value, notional).
- No hard-coded INSTRUMENT_SPECS per symbol; we use CLASS_DEFAULTS per TYPE only.
- Display: adds 'Type' column in the final table for clarity.

Règles clés :
- Fichier session_pairs.txt : lignes "SESSION,PAIR,TP[,TYPE][,MON,TUE,WED,THU,FRI]".
- Chaque paire a un seul objectif : TP1 ou TP2 ou TP3.
- Outcome binaire : TP (avant SL) sinon SL.
- R-multiple par trade : +k R si TPk atteint avant SL, sinon -1 R.
- Sizing : risque % sur capital disponible (equity - risques ouverts).
- Frais : 3.5 USD par lot par transaction (entrée et sortie). (modifiable via --fee-per-lot)

Sorties :
- Tableau final : TP (prix) et Résultat (TP/SL) au lieu de colonnes multiples.
- Summary global (trades, winrate, expectancy R, fees, capital final, MDD).
- Max Daily Drawdown (pire journée, en $ et %).
- Breakdown par jour de la semaine (basé sur la date d'entrée).
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext

# ---------- TOGGLES ----------
SHOW_TRADES   = False    # Afficher la table des trades
SHOW_MONTHLY  = True    # Afficher le breakdown mensuel
SHOW_WEEKDAYS = True    # Afficher le breakdown par jour de la semaine

# ---------- PARAMÈTRES ----------
# Skip a trade if stop size < MIN_STOP_PIPS (consumes the session/pair/day anyway)
MIN_STOP_PIPS = 0.0
# Fees in USD per lot per transaction (entry and exit are both charged)
FEE_PER_LOT   = 2.5
# -----------------------------

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

# ---------- Temps ----------
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

# ---------- Fenêtres de session (UTC) ----------
def tokyo_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    debut = int((base + timedelta(hours=1)).timestamp()*1000)               # 01:00
    fin   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)   # 05:45
    return debut, fin

def london_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    debut = int((base + timedelta(hours=8)).timestamp()*1000)                # 08:00
    fin   = int((base + timedelta(hours=12, minutes=45)).timestamp()*1000)   # 12:45
    return debut, fin

def ny_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    debut = int((base + timedelta(hours=13)).timestamp()*1000)               # 13:00
    fin   = int((base + timedelta(hours=17, minutes=45)).timestamp()*1000)   # 17:45
    return debut, fin

def window_for_session(session: str, d: date) -> Tuple[int, int]:
    s = (session or "").strip().upper()
    if s == "TOKYO":
        return tokyo_signal_window(d)
    if s == "LONDON":
        return london_signal_window(d)
    if s in ("NY", "NEWYORK", "NEW_YORK"):
        return ny_signal_window(d)
    # défaut : Tokyo
    return tokyo_signal_window(d)

# ---------- Helpers paires / prix ----------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    return 0.001 if pair.upper().endswith("JPY") else 0.00001

# =====================  TYPE-aware / multi-asset sizing (no per-symbol specs)  =====================

# Global override map filled from session file when TYPE column exists
TYPE_OVERRIDE: Dict[str, str] = {}  # e.g., {"NAS100": "INDEX", "XAUUSD": "METAL"}

def instrument_type_for(symbol: str) -> str:
    """Return one of: FOREX | METAL | INDEX | CRYPTO (with session-file override if provided)."""
    s = (symbol or "").upper()
    # 1) explicit override from session file
    typ = TYPE_OVERRIDE.get(s)
    if typ:
        return typ

    # 2) heuristics by symbol (fallback only)
    if s in ("XAUUSD", "XAGUSD", "XPTUSD", "XPDUSD"):
        return "METAL"
    if any(s.startswith(p) for p in ("US30", "US500", "SP500", "NAS100", "DAX40", "GER40", "UK100", "FRA40", "JP225", "JPN225", "HK50", "AUS200", "ES35", "IT40", "CN50")):
        return "INDEX"
    if any(s.startswith(p) for p in ("BTC", "ETH", "LTC", "XRP")) or s in ("BTCUSD","ETHUSD"):
        return "CRYPTO"
    return "FOREX"

# Class defaults ONLY (no per-symbol table)
CLASS_DEFAULTS = {
    "FOREX":  {"tick_size": 0.0001, "tick_size_jpy": 0.01, "contract_size": 100_000.0, "point_value_usd_per_lot": None},
    "METAL":  {"tick_size": 0.01,   "contract_size": 100.0, "point_value_usd_per_lot": 1.0},   # $1 per 0.01
    "INDEX":  {"tick_size": 1.0,    "contract_size": 1.0,   "point_value_usd_per_lot": 1.0},   # $1 per 1.0
    "CRYPTO": {"tick_size": 0.01,   "contract_size": 1.0,   "point_value_usd_per_lot": 0.01},  # $0.01 per 0.01
}

def _get_spec(symbol: str) -> Dict[str, float]:
    typ = instrument_type_for(symbol)
    if typ == "FOREX":
        is_jpy = (symbol or "").upper().endswith("JPY")
        return {
            "tick_size": CLASS_DEFAULTS["FOREX"]["tick_size_jpy"] if is_jpy else CLASS_DEFAULTS["FOREX"]["tick_size"],
            "contract_size": CLASS_DEFAULTS["FOREX"]["contract_size"],
            "point_value_usd_per_lot": None
        }
    d = CLASS_DEFAULTS[typ]
    return {
        "tick_size": d["tick_size"],
        "contract_size": d["contract_size"],
        "point_value_usd_per_lot": d["point_value_usd_per_lot"],
    }

def pip_size_for(pair: str) -> float:
    """Return the tick size used to convert price distance to 'points' (your 'pips')."""
    return _get_spec(pair)["tick_size"]

def contract_size_for(pair: str) -> float:
    """Contract size for one lot."""
    return _get_spec(pair)["contract_size"]

def pip_value_per_lot_usd_at(conn, pair: str, entry_ts: int, entry_price: float) -> float:
    """
    USD account — USD per 'point' (tick_size) for 1 lot.
    - FOREX:
        * xxxUSD: value = contract_size * tick_size
        * USDxxx: value = (contract_size * tick_size) / price  (JPY: ≈ 1000 / USDJPY)
        * Crosses: convert via USD quote (use USDXXX or XXXUSD)
    - METAL/INDEX/CRYPTO: class defaults only.
    """
    spec = _get_spec(pair)
    tick = spec["tick_size"]
    cs   = spec["contract_size"]
    pv   = spec.get("point_value_usd_per_lot")
    p    = (pair or "").upper()
    typ  = instrument_type_for(p)

    if typ in ("METAL", "INDEX", "CRYPTO"):
        return pv  # class default

    # FOREX
    if p.endswith("USD"):
        return cs * tick
    if p.startswith("USD"):
        if p.endswith("JPY"):
            usdjpy = fx_close_at(conn, "USDJPY", entry_ts) or entry_price
            return 1000.0 / max(usdjpy, 1e-9)
        return (cs * tick) / max(entry_price, 1e-9)

    # Crosses (no USD side): convert via quote currency to USD
    quote = p[3:6]  # e.g., 'GBP' in 'EURGBP'
    conv1 = f"{quote}USD"
    conv2 = f"USD{quote}"
    px1 = fx_close_at(conn, conv1, entry_ts)
    if px1 is not None:
        return (cs * tick) * px1
    px2 = fx_close_at(conn, conv2, entry_ts)
    if px2 is not None:
        return (cs * tick) / max(px2, 1e-9)

    return cs * tick  # safe fallback

def notional_usd_at(conn, pair: str, entry_ts: int, entry_price: float, lot_size: float) -> float:
    """
    Approx notional in USD for leverage display. Uses per-class logic.
    """
    spec = _get_spec(pair)
    cs   = spec["contract_size"]
    typ  = instrument_type_for(pair)
    p    = (pair or "").upper()

    if typ in ("METAL", "INDEX", "CRYPTO"):
        pv = spec["point_value_usd_per_lot"]
        points = entry_price / max(spec["tick_size"], 1e-12)
        return lot_size * pv * points

    # FOREX (USD account)
    if p.endswith("USD"):
        return lot_size * cs * entry_price
    if p.startswith("USD"):
        return lot_size * cs
    if p.endswith("JPY"):
        usdjpy = fx_close_at(conn, "USDJPY", entry_ts) or entry_price
        notion_jpy = lot_size * cs * entry_price
        return notion_jpy / max(usdjpy, 1e-9)
    return lot_size * cs * entry_price

# ---------- Formatting ----------
def fmt_price(pair: str, x: float) -> str:
    if pair.upper().endswith("JPY"):
        return f"{x:.3f}"
    if pair.upper().startswith("XAU"):
        return f"{x:.5f}"
    return f"{x:.5f}"

def fmt_target(pair: str, x: float) -> str:
    return fmt_price(pair, x)

# ---------- Lectures DB ----------
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

# ---------- Conversions / notionnels ----------
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

# ---------- FSM / Trade ----------
@dataclass
class Trade:
    side: str      # "LONG" | "SHORT"
    entry_ts: int  # ts OPEN UTC de la bougie de trigger
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    """
    Break strict (close > high ou close < low), puis pullback antagoniste (close contraire),
    puis wick trigger (dépassement du hh/ll du break).
    SL = plus bas/haut MIN/MAX depuis le pullback (inclus).
    """
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
                long_min_low_since_pullback = l

            if long_pullback_idx is not None and i >= 1:
                prev_low = c15[i-1]["low"]
                long_min_low_since_pullback = (
                    prev_low
                    if long_min_low_since_pullback is None
                    else min(long_min_low_since_pullback, prev_low)
                )

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
                short_max_high_since_pullback = h

            if short_pullback_idx is not None and i >= 1:
                prev_high = c15[i-1]["high"]
                short_max_high_since_pullback = (
                    prev_high
                    if short_max_high_since_pullback is None
                    else max(short_max_high_since_pullback, prev_high)
                )

            if (prev_ll is not None) and (short_pullback_idx is not None) and (i > short_pullback_idx) and (l < prev_ll) and (i >= 1):
                entry_price = prev_ll
                sl_price = short_max_high_since_pullback if short_max_high_since_pullback is not None else c15[i-1]["high"]
                return Trade("SHORT", ts, entry_price, sl_price)

            if (short_ll is None) or (l < short_ll):
                short_ll = l

    return None

# ---------- Évaluation après entrée ----------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        targets = {"RR1": entry, "RR2": entry, "RR3": entry}
        hit_time: Dict[str, Optional[int]] = {"SL": None, "RR1": None, "RR2": None, "RR3": None}
        results = {k: "SL" for k in ["RR1","RR2","RR3"]}
        return targets, results, hit_time, None

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

def reached_before(hits: Dict[str, Optional[int]], key: str) -> bool:
    t = hits.get(key)
    sl = hits.get("SL")
    return t is not None and (sl is None or t < sl)

def evaluate_trade_tp_only(conn, pair: str, tr: Trade, tp_level: str):
    targets, _results, hits, _closed_ts_ref = evaluate_trade_after_entry(conn, pair, tr)
    lvl = (tp_level or "").strip().upper()
    key = "RR1" if lvl == "TP1" else "RR2" if lvl == "TP2" else "RR3"
    tp_price = targets[key]

    if reached_before(hits, key):
        return tp_price, "TP", hits[key], float(1 if key=="RR1" else 2 if key=="RR2" else 3)
    else:
        return tp_price, "SL", hits["SL"], -1.0

# ---------- Affichages ----------
def show_table(rows):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = [
        "Pair","Type","Session","TP visé",
        "1H High","1H Low",
        "Entry (UTC)","Entry","SL","Stop (pips)",
        "TP (prix)","Résultat","R-mult",
        "Lots","Lev","Frais $","PnL $","Equity $",
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
    print(f"Trades (entries trouvés) : {total_trades}")
    print(f"Winrate :                 {wl_str}")
    print(f"Expectancy (R) :          {expectancy_R:+.3f}R")
    print(f"Total frais :             ${total_fees:,.2f}")
    print(f"Capital départ :          ${start_cap:,.2f}")
    print(f"Capital final :           ${equity:,.2f}")
    print(f"Max Drawdown :            ${mdd_abs:,.2f}  ({mdd_pct:.2f}%)")
    if worst_day:
        print(f"Max Daily Drawdown :      ${worst_daily_abs:,.2f}  ({worst_daily_pct:,.2f}%)  le {worst_day}")
    else:
        print(f"Max Daily Drawdown :      ${worst_daily_abs:,.2f}  ({worst_daily_pct:,.2f}%)")
    print("=====================================")

def print_monthly_breakdown(monthly):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = ["Mois","Trades","Wins","Losses","PnL $","Frais $","Retour %"]
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
    print("\n===== BREAKDOWN MENSUEL =====")
    print(t)
    print("=============================")

def print_weekday_breakdown(weekday_stats: Dict[int, Dict[str, Any]]):
    from prettytable import PrettyTable
    names = ["Mon","Tue","Wed","Thu","Fri","Sat","Sun"]
    t = PrettyTable()
    t.field_names = ["Jour","Trades","Wins","Losses","Winrate %","Avg R","PnL $","Frais $"]
    for wd in range(7):
        st = weekday_stats.get(wd, {"trades":0,"wins":0,"losses":0,"pnl":0.0,"fees":0.0,"r_sum":0.0})
        trades = st["trades"]; wins = st["wins"]; losses = st["losses"]
        winrate = (wins/(wins+losses)*100.0) if (wins+losses)>0 else 0.0
        avg_r   = (st["r_sum"]/trades) if trades>0 else 0.0
        t.add_row([names[wd], trades, wins, losses, f"{winrate:.2f}", f"{avg_r:+.3f}", f"{st['pnl']:,.2f}", f"{st['fees']:,.2f}"])
    print("\n===== BREAKDOWN PAR JOUR DE LA SEMAINE (basé sur la date d'entrée) =====")
    print(t)
    print("=========================================================================")

# ---------- Moteur principal ----------
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
    instr_type: str = "FOREX"
    risk_amount: float = 0.0
    lot_size: float = 0.0
    fee_total: float = 0.0
    lev_used: float = 0.0
    pnl_gross: float = 0.0
    pnl_net: float = 0.0

def run_all(conn,
            session_pairs: List[Tuple[str, str, str, Dict[int, bool]]],
            start: date, end: date,
            start_capital: float, risk_pct: float,
            fee_per_lot: float,
            min_stop_pips: float):

    prepared: List[PreparedTrade] = []
    seen_sessions = set()
    last_close_by_key: Dict[Tuple[str, str], Optional[int]] = {}

    for d in daterange(start, end):
        wd = d.weekday()
        for sess, pair, tp_level, allowed_by_wd in session_pairs:
            if not allowed_by_wd.get(wd, False):
                continue

            key = (sess.upper(), pair.upper(), d)
            if key in seen_sessions:
                continue

            c1 = read_first_1h(conn, pair, d)
            if not c1:
                continue
            rh, rl = c1["high"], c1["low"]
            s, e = window_for_session(sess, d)
            c15 = read_15m_in(conn, pair, s, e)
            if not c15:
                continue
            tr = detect_first_trade_for_day(c15, rh, rl)
            if not tr:
                continue

            last_key = (sess.upper(), pair.upper())
            last_close_ts = last_close_by_key.get(last_key)
            if last_close_ts is not None and tr.entry_ts <= last_close_ts:
                seen_sessions.add(key)
                continue

            pip_sz = pip_size_for(pair)
            stop_pips = abs(tr.entry - tr.sl) / max(pip_sz, 1e-12)
            seen_sessions.add(key)
            if stop_pips < min_stop_pips:
                continue

            targets, _res, hits, closed_ts_ref = evaluate_trade_after_entry(conn, pair, tr)

            lvl = (tp_level or "").strip().upper()
            rr_key = "RR1" if lvl == "TP1" else "RR2" if lvl == "TP2" else "RR3"
            tp_price = targets[rr_key]

            if reached_before(hits, rr_key):
                outcome = "TP"; r_mult = float(1 if rr_key=="RR1" else 2 if rr_key=="RR2" else 3)
                closed_ts_user = hits[rr_key]
            else:
                outcome = "SL"; r_mult = -1.0
                closed_ts_user = hits["SL"]

            if closed_ts_ref is not None:
                last_close_by_key[last_key] = closed_ts_ref

            instr_type = instrument_type_for(pair)
            pt = PreparedTrade(pair=pair, session=sess, tp_level=tp_level,
                               tr=tr, tp_price=tp_price, outcome=outcome,
                               closed_ts=closed_ts_user, r_mult=r_mult,
                               instr_type=instr_type)
            prepared.append(pt)

    if not prepared:
        print("Aucun trade trouvé sur la période/sélection.")
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
            monthly[key] = {"equity_start": None, "pnl": 0.0, "fees": 0.0, "trades": 0, "wins": 0, "losses": 0}
        return key

    daily: Dict[str, Dict[str, float]] = {}
    worst_daily_dd_abs = 0.0
    worst_daily_dd_pct = 0.0
    worst_daily_day: Optional[str] = None

    weekday_stats: Dict[int, Dict[str, Any]] = {}

    hl_cache: Dict[Tuple[str, date], Tuple[float, float]] = {}
    for d in daterange(start, end):
        for _, pair, _, __ in session_pairs:
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
            if stop_pips > 0 and pip_val and risk_amount > 0:
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

            wd = entry_date.weekday()
            if wd not in weekday_stats:
                weekday_stats[wd] = {"trades":0,"wins":0,"losses":0,"pnl":0.0,"fees":0.0,"r_sum":0.0}
            weekday_stats[wd]["trades"] += 1

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

            wd = entry_date.weekday()
            wb = weekday_stats[wd]
            wb["pnl"]  += pnl_net
            wb["fees"] += pt.fee_total
            wb["r_sum"] += pt.r_mult
            if pt.outcome == "TP": wb["wins"] += 1
            else:                  wb["losses"] += 1

            if pt in open_trades:
                open_trades.remove(pt)

            day = datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date()
            rh, rl = hl_cache.get((pt.pair, day), (pt.tr.entry, pt.tr.entry))
            pip_sz = pip_size_for(pt.pair)
            stop_pips = abs(pt.tr.entry - pt.tr.sl) / max(pip_sz, 1e-12)

            row = [
                pt.pair,
                pt.instr_type,
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
                f"{pt.pnl_net:+.2f}",
                f"{equity:,.2f}",
                f"{iso_utc(pt.closed_ts).split('T')[0]} {hm_utc(pt.closed_ts)}" if pt.closed_ts else ""
            ]
            final_rows_with_sort.append((pt.closed_ts or pt.tr.entry_ts, pt.tr.entry_ts, row))

    worst_daily_dd_abs = 0.0
    worst_daily_dd_pct = 0.0
    worst_daily_day = None
    for dk, rec in daily.items():
        start_e = rec["start"]; min_e = rec["min"]
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
        print("Aucun trade clôturé (anormal).")

    expectancy_R = (sum(r_list) / len(r_list)) if r_list else 0.0

    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)

    if SHOW_WEEKDAYS:
        print_weekday_breakdown(weekday_stats)

    print_summary(
        total_trades, wins, losses, expectancy_R,
        start_capital, equity, total_fees,
        max_dd_abs, max_dd_pct,
        worst_daily_dd_abs, worst_daily_dd_pct, worst_daily_day,
        "ALL"
    )

# ---------- Chargement du fichier sessions ----------
def load_session_file(path: str) -> List[Tuple[str, str, str, Dict[int, bool]]]:
    """
    Lit un fichier CSV avec header:
      SESSION,PAIR,TP[,TYPE][,MON,TUE,WED,THU,FRI]

    Retourne une liste de tuples:
      (session, pair, tp_level, allowed_by_wd)

    Side-effect: remplit TYPE_OVERRIDE[{PAIR}] = TYPE si présent.
    """
    out: List[Tuple[str, str, str, Dict[int, bool]]] = []
    if not os.path.exists(path):
        print(f"Erreur: {path} introuvable.")
        return out

    global TYPE_OVERRIDE
    TYPE_OVERRIDE.clear()

    with open(path, "r", newline="") as f:
        reader = csv.DictReader(f)
        has_type_col = "TYPE" in (reader.fieldnames or [])

        for rec in reader:
            sess = (rec.get("SESSION") or "").strip().upper()
            pair = (rec.get("PAIR") or "").strip().upper()
            tp   = (rec.get("TP") or "TP3").strip().upper()
            if not sess or not pair or tp not in ("TP1","TP2","TP3"):
                continue

            # Optional TYPE override
            if has_type_col:
                typ = (rec.get("TYPE") or "").strip().upper()
                if typ in ("FOREX","METAL","INDEX","CRYPTO"):
                    TYPE_OVERRIDE[pair] = typ  # used by instrument_type_for()

            def yes(x): return (x or "").strip().upper().startswith("Y")
            allowed_by_wd = {
                0: yes(rec.get("MON")),
                1: yes(rec.get("TUE")),
                2: yes(rec.get("WED")),
                3: yes(rec.get("THU")),
                4: yes(rec.get("FRI")),
                5: False,  # Samedi
                6: False,  # Dimanche
            }

            out.append((sess, pair, tp, allowed_by_wd))
    return out

# ---------- CLI ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--session-file", default="session_pairs.txt",
                    help="Fichier avec lignes SESSION,PAIR,TP[,TYPE][,MON,TUE,WED,THU,FRI]")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--capital-start", type=float, default=100000.0, help="Capital initial (déf. 100000)")
    ap.add_argument("--risk-pct", type=float, default=1.0, help="Risque par trade en % du capital disponible")
    ap.add_argument("--fee-per-lot", type=float, default=FEE_PER_LOT,
                    help=f"Frais USD par lot par transaction (déf. {FEE_PER_LOT})")
    ap.add_argument("--min-stop-pips", type=float, default=MIN_STOP_PIPS,
                    help=f"Seuil minimum de stop en pips (tick_size units) pour accepter un trade (déf. {MIN_STOP_PIPS})")
    a = ap.parse_args()

    session_pairs = load_session_file(a.session_file)
    if not session_pairs:
        print("Aucune ligne valide dans le fichier de sessions (SESSION,PAIR,TP[,...]).")
        sys.exit(0)

    pairs_list = ", ".join(sorted({p for _, p, _, __ in session_pairs}))
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
