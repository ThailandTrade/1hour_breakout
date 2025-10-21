#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New York Breakout + Pullback — Backtester multi-sessions (TP unique par paire) + Bootcamp Scaling

Règles clés :
- Fichier session_pairs.txt : lignes "SESSION,PAIR,TPx" (ex: NY,EURUSD,TP1).
- Chaque paire a un seul objectif : TP1 ou TP2 ou TP3.
- Outcome binaire : TP (avant SL) sinon SL.
- R-multiple par trade : +k R si TPk atteint avant SL, sinon -1 R.
- Sizing : risque % sur capital disponible (equity - risques ouverts).
- Frais : FEE_PER_LOT USD par lot par transaction (entrée et sortie).
- Limites : 1 trade max par (SESSION, PAIR, DATE). Skip si stop < MIN_STOP_PIPS.
- Bootcamp : progression de palier en palier dès +BOOTCAMP_PROGRESS_PCT%, reset de l’equity au capital du palier suivant ;
             arrêt si drawdown du palier atteint -BOOTCAMP_MAX_LOSS_PCT%.

Sorties :
- Tableau final des trades.
- Breakdown mensuel.
- Summary global (trades, winrate, expectancy R, fees, capital final, MDD) + état Bootcamp (palier atteint).
"""

import os, sys, argparse
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext

# ---------- TOGGLES ----------
SHOW_TRADES  = True    # Afficher la table des trades
SHOW_MONTHLY = True    # Afficher le breakdown mensuel

# ---------- PARAMÈTRES ----------
# Skip un trade si stop (en pips) < MIN_STOP_PIPS (et consomme la session/paire/jour)
MIN_STOP_PIPS = 6.0
# Frais USD par lot par transaction (entrée ET sortie facturées)
FEE_PER_LOT   = 2.5

# Bootcamp plan (d’après ton tableau) — paliers en dollars (ordre croissant)
# NB: tu as listé les chemins $20k et $100k et $250k ; on inclut tout pour flexibilité.
BOOTCAMP_TIERS = [
    20000, 25000, 30000, 40000, 50000, 60000, 80000, 100000,
    125000, 150000, 175000, 200000,
    250000, 275000, 300000, 350000, 400000, 500000, 750000,
    1_000_000, 1_500_000, 2_000_000, 2_500_000, 3_000_000, 3_500_000, 4_000_000
]
# Cible de progression et max loss (en % du capital du palier)
BOOTCAMP_PROGRESS_PCT = 5.0   # avancer au palier suivant à +5%
BOOTCAMP_MAX_LOSS_PCT = 4.0   # échec du palier à -4%
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

def pip_size_for(pair: str) -> float:
    # XAU: 0.01 ; JPY: 0.01 ; sinon 0.0001
    p = pair.upper()
    if p.startswith("XAU"):
        return 0.01
    return 0.01 if p.endswith("JPY") else 0.0001

def contract_size_for(pair: str) -> float:
    # XAU 1 lot = 100 oz ; FX 1 lot = 100,000 unités de base
    return 100.0 if pair.upper().startswith("XAU") else 100_000.0

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

def pip_value_per_lot_usd_at(conn, pair: str, entry_ts: int, entry_price: float) -> float:
    """
    Compte USD — USD par pip pour 1 lot.
    - xxxUSD : 10
    - *JPY (incl. USDJPY) : 1000 / USDJPY
    - XAUUSD : 100 * 0.01 = 1
    - sinon : 10 (fallback)
    """
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
    entry_ts: int  # ts OPEN UTC de la bougie de trigger
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    """
    Activation long/short après close strict au-dessus/dessous du range 1H,
    pullback antagoniste (close contraire), puis déclenchement sur wick (>hh ou <ll),
    SL sur la bougie i-1.
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

# ---------- Évaluation après entrée : TP ciblé seulement ----------
def evaluate_trade_tp_only(conn, pair: str, tr: Trade, tp_level: str):
    """
    Calcule la trajectoire après l'entrée jusqu'à SL ou jusqu'au TP ciblé (TP1/TP2/TP3).
    Renvoie :
      - tp_price (float)
      - result ("TP" ou "SL")
      - closed_ts (int ms)
      - r_multiple (+1/+2/+3 si TP, sinon -1)
    """
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        return entry, "SL", tr.entry_ts, -1.0

    lvl = tp_level.strip().upper()
    if tr.side == "LONG":
        target = entry + (1 if lvl=="TP1" else 2 if lvl=="TP2" else 3)*r
    else:
        target = entry - (1 if lvl=="TP1" else 2 if lvl=="TP2" else 3)*r

    future = read_15m_from(conn, pair, tr.entry_ts)
    closed_ts = tr.entry_ts
    hit_tp = False
    for b in future:
        ts, h, l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            sl_hit = (l <= sl + eps); tp_hit = (h >= target - eps)
        else:
            sl_hit = (h >= sl - eps); tp_hit = (l <= target + eps)
        if sl_hit and not hit_tp:
            closed_ts = ts
            return target, "SL", closed_ts, -1.0
        if tp_hit:
            closed_ts = ts
            k = 1 if lvl=="TP1" else 2 if lvl=="TP2" else 3
            return target, "TP", closed_ts, float(k)

    # si jamais ni TP ni SL (données tronquées)
    return target, "SL", closed_ts, -1.0

# ---------- Affichages ----------
def show_table(rows):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = [
        "Pair","Session","TP visé",
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
                  label:str="ALL", bootcamp_info:str=""):
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
        print(f"Max Daily Drawdown :      ${worst_daily_abs:,.2f}  ({worst_daily_pct:.2f}%)  le {worst_day}")
    else:
        print(f"Max Daily Drawdown :      ${worst_daily_abs:,.2f}  ({worst_daily_pct:.2f}%)")
    if bootcamp_info:
        print(bootcamp_info)
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
    """
    session_pairs : liste de tuples (SESSION, PAIR, TPx)
    """
    # Préparation trades par fenêtre de session
    prepared: List[PreparedTrade] = []
    seen_sessions = set()  # 1 trade max par (SESSION, PAIR, DATE)

    for d in daterange(start, end):
        for sess, pair, tp_level in session_pairs:
            key = (sess.upper(), pair.upper(), d)
            if key in seen_sessions:
                continue

            c1 = read_first_1h(conn, pair, d)
            if not c1: continue
            rh, rl = c1["high"], c1["low"]
            s, e = window_for_session(sess, d)
            c15 = read_15m_in(conn, pair, s, e)
            if not c15: continue

            tr = detect_first_trade_for_day(c15, rh, rl)
            if not tr: continue

            # Skip si stop trop petit (consomme quand même la session/paire/jour)
            pip_sz = pip_size_for(pair)
            stop_pips = abs(tr.entry - tr.sl) / max(pip_sz, 1e-12)
            seen_sessions.add(key)
            if stop_pips < min_stop_pips:
                continue

            tp_price, result, cl_ts, r_mult = evaluate_trade_tp_only(conn, pair, tr, tp_level)
            prepared.append(PreparedTrade(
                pair=pair, session=sess, tp_level=tp_level,
                tr=tr, tp_price=tp_price, outcome=result, closed_ts=cl_ts, r_mult=r_mult
            ))

    if not prepared:
        print("Aucun trade trouvé sur la période/sélection.")
        print_summary(0, 0, 0, 0.0, start_capital, start_capital, 0.0, 0.0, 0.0, 0.0, 0.0, None, "ALL")
        return

    # -------- Bootcamp setup --------
    # Trouver l'index du palier de départ (exact match recommandé)
    try:
        tier_idx = BOOTCAMP_TIERS.index(int(round(start_capital)))
    except ValueError:
        # si non trouvé, choisir le palier le plus proche >= start_capital
        tier_idx = 0
        for i, bal in enumerate(BOOTCAMP_TIERS):
            if bal >= start_capital:
                tier_idx = i
                break
        start_capital = BOOTCAMP_TIERS[tier_idx]

    tier_balance = float(BOOTCAMP_TIERS[tier_idx])
    tier_target_abs = tier_balance * (BOOTCAMP_PROGRESS_PCT/100.0)
    tier_maxloss_abs = tier_balance * (BOOTCAMP_MAX_LOSS_PCT/100.0)

    bootcamp_transitions: List[str] = [f"Start tier: ${tier_balance:,.0f} (target +{BOOTCAMP_PROGRESS_PCT:.1f}% = ${tier_target_abs:,.0f}, max loss -{BOOTCAMP_MAX_LOSS_PCT:.1f}% = ${tier_maxloss_abs:,.0f})"]

    # -------- Simulation temporelle --------
    from collections import defaultdict
    evmap: Dict[int, List[Tuple[str, PreparedTrade]]] = defaultdict(list)
    for pt in prepared:
        evmap[pt.tr.entry_ts].append(("ENTRY", pt))
        if pt.closed_ts:
            evmap[pt.closed_ts].append(("CLOSE", pt))
    for ts in evmap:
        evmap[ts].sort(key=lambda x: (0 if x[0]=="ENTRY" else 1, x[1].pair))

    equity = float(tier_balance)  # equity = capital du palier en cours
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
    hl_cache: Dict[Tuple[str, date], Tuple[float, float]] = {}
    for d in daterange(start, end):
        for _, pair, _ in session_pairs:
            c1 = read_first_1h(conn, pair, d)
            if c1: hl_cache[(pair, d)] = (c1["high"], c1["low"])

    open_trades: List[PreparedTrade] = []
    def open_risk_total() -> float:
        return sum(t.risk_amount for t in open_trades)

    final_rows_with_sort: List[Tuple[int, int, List[Any]]] = []
    bootcamp_failed = False

    for ts in sorted(evmap.keys()):
        # ENTRIES
        for etype, pt in evmap[ts]:
            if etype != "ENTRY": 
                continue
            # 1 position max par paire en même temps
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
            pnl_gross  = risk_amount * pt.r_mult

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

            # Daily bucket
            day_key = datetime.fromtimestamp((pt.closed_ts or ts)/1000, tz=UTC).date().isoformat()
            if day_key not in daily:
                daily[day_key] = {"start": equity_before, "min": equity_before, "end": equity_before}
            drec = daily[day_key]
            drec["end"] = equity
            if equity < drec["min"]:
                drec["min"] = equity

            # Comptes win/loss
            if pt.outcome == "TP":
                wins += 1
            else:
                losses += 1
            r_list.append(pt.r_mult)

            # MDD global
            if equity > peak_equity:
                peak_equity = equity
            dd_abs = peak_equity - equity
            if dd_abs > max_dd_abs:
                max_dd_abs = dd_abs
                max_dd_pct = (dd_abs / peak_equity) * 100.0 if peak_equity > 0 else 0.0

            # Mensuel
            entry_date = datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date()
            mk = ensure_month(entry_date)
            monthly[mk]["pnl"]  += pnl_net
            monthly[mk]["fees"] += pt.fee_total
            if pt.outcome == "TP": monthly[mk]["wins"] += 1
            else:                   monthly[mk]["losses"] += 1

            if pt in open_trades:
                open_trades.remove(pt)

            # Ligne du tableau
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

            # -------- Bootcamp checks après chaque clôture --------
            progress = equity - tier_balance
            # palier atteint ?
            if progress >= tier_target_abs and tier_idx < len(BOOTCAMP_TIERS) - 1:
                old = tier_balance
                tier_idx += 1
                tier_balance = float(BOOTCAMP_TIERS[tier_idx])
                equity = tier_balance  # reset equity au nouveau palier
                peak_equity = equity   # reset du peak (par logique de palier)
                tier_target_abs = tier_balance * (BOOTCAMP_PROGRESS_PCT/100.0)
                tier_maxloss_abs = tier_balance * (BOOTCAMP_MAX_LOSS_PCT/100.0)
                bootcamp_transitions.append(
                    f"Tier up: ${old:,.0f} -> ${tier_balance:,.0f} (new target +{BOOTCAMP_PROGRESS_PCT:.1f}% = ${tier_target_abs:,.0f}, max loss ${tier_maxloss_abs:,.0f})"
                )
            # échec du palier ?
            elif -progress >= tier_maxloss_abs:
                bootcamp_transitions.append(
                    f"Tier failed at ${tier_balance:,.0f}: drawdown reached ${progress:-,.2f} (limit ${tier_maxloss_abs:,.0f})."
                )
                bootcamp_failed = True
                break  # stop simulation

        if bootcamp_failed:
            break

    # Max Daily Drawdown (après boucle)
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

    # Table
    if final_rows_with_sort and SHOW_TRADES:
        final_rows_with_sort.sort(key=lambda t: (t[0], t[1], t[2][0]))
        show_table([r for _, __, r in final_rows_with_sort])
    elif not final_rows_with_sort:
        print("Aucun trade clôturé (anormal).")

    # Expectancy R
    expectancy_R = (sum(r_list) / len(r_list)) if r_list else 0.0

    # Breakdown mensuel
    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)

    # Info Bootcamp
    boot_info = "\nBootcamp:\n- " + "\n- ".join(bootcamp_transitions)
    if bootcamp_failed:
        boot_info += "\n- Status: FAILED"
    else:
        boot_info += f"\n- Status: Reached ${BOOTCAMP_TIERS[tier_idx]:,.0f}"

    print_summary(
        total_trades, wins, losses, expectancy_R,
        BOOTCAMP_TIERS[0], equity, total_fees,
        max_dd_abs, max_dd_pct,
        worst_daily_dd_abs, worst_daily_dd_pct, worst_daily_day,
        "ALL", bootcamp_info=boot_info
    )

# ---------- Chargement du fichier sessions ----------
def load_session_file(path: str) -> List[Tuple[str, str, str]]:
    """
    Lit un fichier (CSV/texte) avec lignes : SESSION,PAIR,TPx
    - SESSION : TOKYO | LONDON | NY (NEWYORK/NEW_YORK acceptés)
    - PAIR    : ex EURUSD, USDJPY, XAUUSD...
    - TPx     : TP1 | TP2 | TP3  (OBLIGATOIRE)
    Saute lignes vides / commentées (#).
    """
    out: List[Tuple[str, str, str]] = []
    if not os.path.exists(path):
        print(f"Erreur: {path} introuvable.")
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
                    help="Fichier avec lignes SESSION,PAIR,TPx (ex: NY,EURUSD,TP1)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--capital-start", type=float, default=100000.0,
                    help="Capital de départ (doit correspondre à un palier Bootcamp, ex: 100000)")
    ap.add_argument("--risk-pct", type=float, default=1.0,
                    help="Risque par trade en % de l’equity disponible")
    ap.add_argument("--fee-per-lot", type=float, default=FEE_PER_LOT,
                    help=f"Frais USD par lot par transaction (déf. {FEE_PER_LOT})")
    ap.add_argument("--min-stop-pips", type=float, default=MIN_STOP_PIPS,
                    help=f"Seuil minimum de stop en pips pour accepter un trade (déf. {MIN_STOP_PIPS})")
    a = ap.parse_args()

    session_pairs = load_session_file(a.session_file)
    if not session_pairs:
        print("Aucune ligne valide dans le fichier de sessions (SESSION,PAIR,TPx).")
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
