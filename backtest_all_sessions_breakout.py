#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
New York Breakout + Pullback — Backtester (multi-pairs with available capital sizing)

- Setup/entry: règles validées (break initial STRICT close, pullback antagoniste close,
  entrée sur wick >/< extrême mémorisé, pas de pullback et d’entrée sur la même bougie).
- SL: bougie juste AVANT la bougie de trigger (LONG: low de i-1 ; SHORT: high de i-1).
- Après entrée: test indépendant RR=1/2/3 + SL (SL prioritaire en cas de conflit).
- Perf: partiels paramétrables w1/w2/w3 sur TP1/TP2/TP3, appliqués dans l’ordre temporel.
  (Ex: w1=1, w2=0, w3=0 ⇒ fermeture à TP1; jamais 0R si WIN)
- Fees: ~3.5 USD par lot par transaction (entrée/sortie). Fee par trade ≈ 2 * 3.5 * lots.
- Ajouts: Outcome label, Max Drawdown, Monthly breakdown, colonne Pips, arrondi JPY.
- Tableau final trié par **Closed Date/Heure** (puis Entry, puis Pair).
- Sizing: risk % appliqué au **capital disponible = equity - somme(risques ouverts)**.
- NEW: en mode “paire par paire”, un tableau récap final avec EXACTEMENT les infos du summary.

CHANGEMENTS dans CETTE VERSION :
- Suppression totale de RR1.5 : on ne garde que RR1 / RR2 / RR3.
- Outcome = WIN si TP1 avant SL, sinon LOSS (pas d’état NO).
- R-multiple calculé par événements (TP1→TP2→TP3→SL) avec pondérations w1/w2/w3.
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext

# ----------- TOGGLES (affichages) -----------
SHOW_TRADES  = False
SHOW_MONTHLY = True
# --------------------------------------------

# ----------- TP WEIGHTS (TP1, TP2, TP3) -----------
TP_WEIGHTS = (0.2, 0.5, 0.3)  # doivent sommer à 1.0
# ------------------------------------------

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

# ---- Sessions (fenêtres UTC) ----
def tokyo_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=1)).timestamp()*1000)                # 01:00 OPEN
    end   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)    # 05:45 OPEN
    return start, end

def london_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=7)).timestamp()*1000)                # 07:00 OPEN
    end   = int((base + timedelta(hours=11, minutes=45)).timestamp()*1000)   # 11:45 OPEN
    return start, end

def ny_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=12)).timestamp()*1000)               # 12:00 OPEN
    end   = int((base + timedelta(hours=16, minutes=45)).timestamp()*1000)   # 16:45 OPEN
    return start, end

def window_for_session(session: str, d: date) -> Tuple[int, int]:
    s = (session or "").strip().upper()
    if s == "TOKYO":
        return tokyo_signal_window(d)
    if s == "LONDON":
        return london_signal_window(d)
    if s in ("NY", "NEWYORK", "NEW_YORK"):
        return ny_signal_window(d)
    # fallback = Tokyo
    return tokyo_signal_window(d)

# ---------------- Helpers ----------------
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
    # XAU 1 lot = 100 oz ; FX 1 lot = 100,000 base units
    return 100.0 if pair.upper().startswith("XAU") else 100_000.0

def fmt_price(pair: str, x: float) -> str:
    if pair.upper().endswith("JPY"):
        return f"{x:.3f}"
    if pair.upper().startswith("XAU"):
        return f"{x:.5f}"
    return f"{x:.5f}"

def fmt_target(pair: str, x: float) -> str:
    return fmt_price(pair, x)

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

# ---------- Conversion & pip value ----------
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
    USD account — USD per pip per 1 lot.
    - XXXXUSD: 10
    - *JPY (incl. USDJPY): 1000 / USDJPY
    - XAUUSD: contract(100) * pip_size(0.01) = 1
    - others: 10 (fallback)
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

# ---------------- FSM / Trade ----------------
@dataclass
class Trade:
    side: str              # "LONG" | "SHORT"
    entry_ts: int          # ts OPEN UTC of trigger bar
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float) -> Optional[Trade]:
    # Long/short activation + pullback + wick trigger; SL sur bar i-1
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

# ---------------- After-entry evaluation ----------------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    """
    Scan jusqu’à SL ou RR3.
    On enregistre les timestamps de RR1/RR2/RR3/SL, puis on s’arrête au premier SL ou RR3.
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

    # Mapping TP/SL (pas de "NO")
    results: Dict[str, str] = {}
    sl_time = hit_time["SL"]
    for key in ["RR1","RR2","RR3"]:
        ttime = hit_time[key]
        results[key] = "TP" if (ttime is not None and (sl_time is None or ttime < sl_time)) else "SL"

    closed_ts = sl_time if sl_time is not None else hit_time["RR3"]
    return targets, results, hit_time, closed_ts

# ---------------- Event-based partials (pondérés) ----------------
def compute_r_and_close(hit_time: Dict[str, Optional[int]],
                        w1=TP_WEIGHTS[0], w2=TP_WEIGHTS[1], w3=TP_WEIGHTS[2]) -> Tuple[float, Optional[int]]:
    """
    Applique les sorties partielles dans l'ordre temporel:
      - TP1: +w1 * 1R ; rem -= w1
      - TP2: +w2 * 2R ; rem -= w2
      - TP3: +w3 * 3R ; rem -= w3
      - SL : -1R * rem
    Renvoie (R-multiple total, close_ts effectif). Si rem==0 après un TP, on ferme là.
    """
    t_sl = hit_time["SL"]
    t1   = hit_time["RR1"]
    t2   = hit_time["RR2"]
    t3   = hit_time["RR3"]

    events: List[Tuple[int, str]] = []
    if t1 is not None: events.append((t1, "TP1"))
    if t2 is not None: events.append((t2, "TP2"))
    if t3 is not None: events.append((t3, "TP3"))
    if t_sl is not None: events.append((t_sl, "SL"))
    events.sort(key=lambda x: x[0])

    rem = 1.0
    r   = 0.0
    close_ts: Optional[int] = None

    for ts, ev in events:
        if ev == "TP1" and w1 > 0:
            r   += w1 * 1.0
            rem -= w1
            if rem <= 1e-12:
                close_ts = ts
                break
        elif ev == "TP2" and w2 > 0:
            r   += w2 * 2.0
            rem -= w2
            if rem <= 1e-12:
                close_ts = ts
                break
        elif ev == "TP3" and w3 > 0:
            r   += w3 * 3.0
            rem -= w3
            if rem <= 1e-12:
                close_ts = ts
                break
        elif ev == "SL":
            if rem > 0:
                r += (-1.0) * rem
                rem = 0.0
            close_ts = ts
            break

    return r, close_ts

def partial_r_multiple(hit_time: Dict[str, Optional[int]],
                       w1=TP_WEIGHTS[0], w2=TP_WEIGHTS[1], w3=TP_WEIGHTS[2]) -> float:
    r, _ = compute_r_and_close(hit_time, w1, w2, w3)
    return r

# ---------------- Output ----------------
def show_table(rows):
    from prettytable import PrettyTable
    t = PrettyTable()
    # Pas de RR1.5, pas de Fees, dates compactées
    t.field_names = [
        "Pair","Side","1H High","1H Low",
        "Entry (UTC)","Entry","SL","Pips",
        "RR1","R1","RR2","R2","RR3","R3",
        "Outcome","R-mult","Lots","Lev","PnL $","Equity $","Closed (UTC)"
    ]
    for r in rows:
        # r indices (référence):
        # 0:Pair 1:Side 2:1H High 3:1H Low 4:Entry Date 5:Entry HM
        # 6:Entry 7:SL 8:Pips 9:RR1 10:R1 11:RR2 12:R2 13:RR3 14:R3
        # 15:ExitRR("mix") 16:Outcome 17:R-mult 18:Lots 19:Lev
        # 20:PnL $ 21:Equity $ 22:Closed Date 23:Closed HM
        entry_dt  = (f"{r[4]} {r[5]}").strip()
        closed_dt = (f"{r[22]} {r[23]}").strip()
        t.add_row([
            r[0], r[1], r[2], r[3],
            entry_dt, r[6], r[7], r[8],
            r[9], r[10], r[11], r[12], r[13], r[14],
            r[16], r[17], r[18], r[19], r[20], r[21], closed_dt
        ])
    print(t)


def print_summary(total_trades:int, wins:int, losses:int, avg_pct:float,
                  start_cap:float, equity:float, total_fees:float,
                  mdd_abs:float, mdd_pct:float, label:str="ALL"):
    wl = f"{wins}/{wins+losses} = { (wins/(wins+losses)*100):.2f}% " if (wins+losses)>0 else "N/A"
    print(f"\n===== SUMMARY ({label}) =====")
    print(f"Trades (entries found): {total_trades}")
    print(f"Winrate (excl. NO):    {wl}")
    print(f"Avg %PnL per trade:    {avg_pct:.3f}%")
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

# -------- Bref récap par paire (Winrate & Avg %PnL) --------
def print_pair_brief(pair_stats):
    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = ["Pair", "Winrate (excl. NO)", "Avg %PnL per trade"]
    for pair in sorted(pair_stats.keys()):
        s = pair_stats[pair]
        w, l = s["wins"], s["losses"]
        wl = f"{w}/{w+l} = {(w/(w+l)*100):.2f}%" if (w + l) > 0 else "N/A"
        avg = (s["pct_sum"] / s["pct_n"]) if s["pct_n"] > 0 else 0.0
        t.add_row([pair, wl, f"{avg:.3f}%"])
    print("\n===== PER-PAIR SUMMARY =====")
    print(t)
    print("===========================")

# ---------------- Multi-pair engine ----------------
@dataclass
class PreparedTrade:
    pair: str
    tr: Trade
    targets: Dict[str, float]
    results: Dict[str, str]
    hits: Dict[str, Optional[int]]
    closed_ts: Optional[int]
    risk_amount: float = 0.0
    lot_size: float = 0.0
    fee_total: float = 0.0
    lev_used: float = 0.0
    r_mult: float = 0.0
    pnl_gross: float = 0.0
    pnl_net: float = 0.0

def run_all_pairs(conn, pairs: List[str], start: date, end: date,
                  start_capital: float, risk_pct: float, fee_per_lot: float,
                  session_map: Optional[Dict[str, str]] = None):
    """
    session_map (optionnel): dict { 'EURUSD': 'NY', 'USDJPY': 'TOKYO', ... }
    Si None, comportement Tokyo par défaut (pour compat).
    """
    prepared: List[PreparedTrade] = []
    for d in daterange(start, end):
        for pair in pairs:
            c1 = read_first_1h(conn, pair, d)
            if not c1: continue
            rh, rl = c1["high"], c1["low"]

            if session_map and pair.upper() in session_map:
                s, e = window_for_session(session_map[pair.upper()], d)
            else:
                s, e = tokyo_signal_window(d)

            c15 = read_15m_in(conn, pair, s, e)
            if not c15: continue
            tr = detect_first_trade_for_day(c15, rh, rl)
            if not tr: continue
            targets, results, hits, cl_raw = evaluate_trade_after_entry(conn, pair, tr)

            # Calcul R & close pondérée par les poids (clôture à TP1 si w1=1, etc.)
            r_mult_pre, close_ts_weighted = compute_r_and_close(hits, *TP_WEIGHTS)
            cl = close_ts_weighted or cl_raw

            pt = PreparedTrade(pair, tr, targets, results, hits, cl)
            pt.r_mult = r_mult_pre
            prepared.append(pt)

    if not prepared:
        print("No trades found across selected pairs.")
        print_summary(0, 0, 0, 0.0, start_capital, start_capital, 0.0, 0.0, 0.0, "ALL")
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
    pct_changes: List[float] = []
    monthly: Dict[str, Dict[str, Any]] = {}
    from collections import defaultdict as _dd
    pair_stats = _dd(lambda: {"wins": 0, "losses": 0, "pct_sum": 0.0, "pct_n": 0})

    # --- Daily drawdown tracking ---
    daily: Dict[str, Dict[str, float]] = {}
    worst_daily_dd_abs = 0.0
    worst_daily_dd_pct = 0.0
    worst_daily_day: Optional[str] = None

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

    open_trades: List[PreparedTrade] = []
    def open_risk_total() -> float:
        return sum(t.risk_amount for t in open_trades)

    final_rows_with_sort: List[Tuple[int, int, List[str]]] = []

    for ts in sorted(evmap.keys()):
        # ENTRIES
        for etype, pt in evmap[ts]:
            if etype != "ENTRY": continue

            # une seule position ouverte par PAIRE
            if any(ot.pair == pt.pair for ot in open_trades):
                continue

            available = max(equity - open_risk_total(), 0.0)
            risk_amount = available * (risk_pct/100.0)

            pip_sz = pip_size_for(pt.pair)
            stop_pips = abs(pt.tr.entry - pt.tr.sl) / max(pip_sz, 1e-12)

            # Pip value précise (JPY/XAU)
            pip_val  = pip_value_per_lot_usd_at(conn, pt.pair, pt.tr.entry_ts, pt.tr.entry)

            lot_size = 0.0
            if stop_pips > 0 and pip_val > 0 and risk_amount > 0:
                lot_size = risk_amount / (stop_pips * pip_val)

            fee_total = float(fee_per_lot) * lot_size * 2.0

            # Notional USD (contrat + conversion JPY si besoin)
            p_up = pt.pair.upper()
            cs   = contract_size_for(pt.pair)
            if p_up.endswith("USD"):
                notion_usd = lot_size * cs * pt.tr.entry
            elif p_up.startswith("USD"):
                notion_usd = lot_size * cs
            elif p_up.endswith("JPY"):
                usdjpy = fx_close_at(conn, "USDJPY", pt.tr.entry_ts) or pt.tr.entry
                notion_jpy = lot_size * cs * pt.tr.entry
                notion_usd = notion_jpy / max(usdjpy, 1e-9)
            else:
                notion_usd = lot_size * cs

            lev_used   = (notion_usd / equity) if equity > 0 else 0.0

            # r_mult déjà calculé; PnL
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
            if etype != "CLOSE": continue
            pnl_net = pt.pnl_gross - pt.fee_total
            equity_before = equity

            # Daily bucket
            day_key = datetime.fromtimestamp((pt.closed_ts or ts)/1000, tz=UTC).date().isoformat()
            if day_key not in daily:
                daily[day_key] = {"start": equity_before, "min": equity_before, "end": equity_before}

            equity += pnl_net
            total_fees += pt.fee_total
            pt.pnl_net = pnl_net

            drec = daily[day_key]
            drec["end"] = equity
            if equity < drec["min"]:
                drec["min"] = equity

            # Outcome = WIN si TP1 avant SL
            tp1_before_sl = (pt.hits["RR1"] is not None) and (pt.hits["SL"] is None or pt.hits["RR1"] < pt.hits["SL"])
            if tp1_before_sl:
                wins += 1
                outcome_label = "WIN"
                monthly[ensure_month(datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date())]["wins"] += 1
                pair_stats[pt.pair]["wins"] += 1
            else:
                losses += 1
                outcome_label = "LOSS"
                monthly[ensure_month(datetime.fromtimestamp(pt.tr.entry_ts/1000, tz=UTC).date())]["losses"] += 1
                pair_stats[pt.pair]["losses"] += 1

            if equity_before > 0:
                pct = (pnl_net / equity_before) * 100.0
                pct_changes.append(pct)
                ps = pair_stats[pt.pair]
                ps["pct_sum"] += pct
                ps["pct_n"]   += 1

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

            if pt in open_trades:
                open_trades.remove(pt)

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
                f"{stop_pips:.1f}",
                fmt_target(pt.pair, pt.targets["RR1"]),   pt.results["RR1"],
                fmt_target(pt.pair, pt.targets["RR2"]),   pt.results["RR2"],
                fmt_target(pt.pair, pt.targets["RR3"]),   pt.results["RR3"],
                "mix",
                outcome_label,
                f"{pt.r_mult:+.2f}R",
                f"{pt.lot_size:.3f}",
                f"{pt.lev_used:.2f}x",
                f"{pnl_net:+.2f}",
                f"{equity:,.2f}",
                iso_utc(pt.closed_ts).split('T')[0] if pt.closed_ts else "",
                hm_utc(pt.closed_ts) if pt.closed_ts else ""
            ]
            final_rows_with_sort.append((pt.closed_ts or pt.tr.entry_ts, pt.tr.entry_ts, row))

    # --- Affichage final
    if final_rows_with_sort and SHOW_TRADES:
        final_rows_with_sort.sort(key=lambda t: (t[0], t[1], t[2][0]))
        show_table([r for _, __, r in final_rows_with_sort])
    elif not final_rows_with_sort:
        print("No trades closed (unexpected).")

    avg_pct = (sum(pct_changes)/len(pct_changes)) if pct_changes else 0.0
    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)

    # Worst daily DD
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

    print_pair_brief(pair_stats)
    print_summary(total_trades, wins, losses, avg_pct, start_capital, equity, total_fees, max_dd_abs, max_dd_pct, "ALL")
    print(f"Max Daily Drawdown:    ${worst_daily_dd_abs:,.2f}  ({worst_daily_dd_pct:.2f}%)" +
          (f" on {worst_daily_day}" if worst_daily_day else ""))

# ---------------- Mono-paire engine ----------------
def run_for_single_pair(conn, pair: str, start: date, end: date,
                        start_capital: float, risk_pct: float, fee_per_lot: float) -> Dict[str, Any]:
    equity = float(start_capital)
    total_trades = 0
    wins = 0
    losses = 0
    pct_changes: List[float] = []
    total_fees = 0.0
    peak_equity = equity
    max_dd_abs = 0.0
    max_dd_pct = 0.0
    monthly: Dict[str, Dict[str, float]] = {}

    # Daily DD
    daily: Dict[str, Dict[str, float]] = {}
    worst_daily_dd_abs = 0.0
    worst_daily_dd_pct = 0.0
    worst_daily_day: Optional[str] = None

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

    hl_cache: Dict[date, Tuple[float, float]] = {}
    for d in daterange(start, end):
        c1 = read_first_1h(conn, pair, d)
        if c1: hl_cache[d] = (c1["high"], c1["low"])

    for d in daterange(start, end):
        c1 = read_first_1h(conn, pair, d)
        if not c1: continue
        rh, rl = c1["high"], c1["low"]
        s, e = tokyo_signal_window(d)  # par défaut
        c15 = read_15m_in(conn, pair, s, e)
        if not c15: continue
        tr = detect_first_trade_for_day(c15, rh, rl)
        if not tr: continue

        targets, results, hits, cl_raw = evaluate_trade_after_entry(conn, pair, tr)
        r_mult, cl_weighted = compute_r_and_close(hits, *TP_WEIGHTS)
        cl = cl_weighted or cl_raw

        available   = max(equity - open_risk, 0.0)
        risk_amount = available * (risk_pct/100.0)

        pip_sz = pip_size_for(pair)
        stop_pips = abs(tr.entry - tr.sl) / max(pip_sz, 1e-12)

        pip_val  = pip_value_per_lot_usd_at(conn, pair, tr.entry_ts, tr.entry)

        lot_size = 0.0
        if stop_pips > 0 and pip_val > 0 and risk_amount > 0:
            lot_size = risk_amount / (stop_pips * pip_val)

        fee = float(fee_per_lot) * lot_size * 2.0

        # notionnel / levier
        p_up = pair.upper()
        cs   = contract_size_for(pair)
        if p_up.endswith("USD"):
            notion_usd = lot_size * cs * tr.entry
        elif p_up.startswith("USD"):
            notion_usd = lot_size * cs
        elif p_up.endswith("JPY"):
            usdjpy = fx_close_at(conn, "USDJPY", tr.entry_ts) or tr.entry
            notion_jpy = lot_size * cs * tr.entry
            notion_usd = notion_jpy / max(usdjpy, 1e-9)
        else:
            notion_usd = lot_size * cs

        lev_used   = (notion_usd / equity) if equity > 0 else 0.0

        pnl_gross = risk_amount * r_mult

        open_risk += risk_amount

        pnl_net = pnl_gross - fee
        equity_before = equity

        # Daily bucket
        day_key = datetime.fromtimestamp((cl or tr.entry_ts)/1000, tz=UTC).date().isoformat()
        if day_key not in daily:
            daily[day_key] = {"start": equity_before, "min": equity_before, "end": equity_before}

        equity += pnl_net
        total_fees += fee
        open_risk -= risk_amount
        if open_risk < 0: open_risk = 0.0

        drec = daily[day_key]
        drec["end"] = equity
        if equity < drec["min"]:
            drec["min"] = equity

        total_trades += 1

        # Outcome = WIN si TP1 avant SL
        tp1_before_sl = (hits["RR1"] is not None) and (hits["SL"] is None or hits["RR1"] < hits["SL"])
        outcome_label = "WIN" if tp1_before_sl else "LOSS"
        if tp1_before_sl: wins += 1
        else:             losses += 1

        if equity_before > 0:
            pct_changes.append((pnl_net / equity_before) * 100.0)

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
        if tp1_before_sl: monthly[mk]["wins"] += 1
        else:             monthly[mk]["losses"] += 1

        ed, eh = iso_utc(tr.entry_ts).split("T")[0], hm_utc(tr.entry_ts)
        cd, ch = (iso_utc(cl).split("T")[0], hm_utc(cl)) if cl else ("","")

        rh_d, rl_d = hl_cache.get(d, (rh, rl))
        row = [
            pair,
            tr.side,
            fmt_price(pair, rh_d), fmt_price(pair, rl_d),
            ed, eh,
            fmt_price(pair, tr.entry), fmt_price(pair, tr.sl),
            f"{stop_pips:.1f}",
            fmt_target(pair, targets["RR1"]),   results["RR1"],
            fmt_target(pair, targets["RR2"]),   results["RR2"],
            fmt_target(pair, targets["RR3"]),   results["RR3"],
            "mix",
            outcome_label,
            f"{r_mult:+.2f}R",
            f"{lot_size:.3f}",
            f"{lev_used:.2f}x",
            f"{pnl_net:+.2f}",
            f"{equity:,.2f}",
            cd, ch
        ]
        rows_with_sort.append((cl or tr.entry_ts, tr.entry_ts, row))

    if rows_with_sort and SHOW_TRADES:
        rows_with_sort.sort(key=lambda t: (t[0], t[1], t[2][0]))
        show_table([r for _, __, r in rows_with_sort])
    elif not rows_with_sort:
        print("No trades found.")

    avg_pct = sum(pct_changes)/len(pct_changes) if pct_changes else 0.0
    if SHOW_MONTHLY:
        print_monthly_breakdown(monthly)

    # Worst daily DD
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

    print_summary(total_trades, wins, losses, avg_pct, start_capital, equity, total_fees, max_dd_abs, max_dd_pct, pair)
    print(f"Max Daily Drawdown:    ${worst_daily_dd_abs:,.2f}  ({worst_daily_dd_pct:.2f}%)" +
          (f" on {worst_daily_day}" if worst_daily_day else ""))

    return {
        "pair": pair,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "avg_pct": avg_pct,
        "total_fees": total_fees,
        "start_cap": start_capital,
        "final_cap": equity,
        "mdd_abs": max_dd_abs,
        "mdd_pct": max_dd_pct,
    }

# --------------- Récap final (identique au summary) ---------------
def print_final_pair_summary_table(summaries: List[Dict[str, Any]]):
    if not summaries:
        return

    summaries_sorted = sorted(summaries, key=lambda s: s["avg_pct"], reverse=True)

    from prettytable import PrettyTable
    t = PrettyTable()
    t.field_names = [
        "Pair",
        "Trades (entries found)",
        "Winrate (excl. NO)",
        "Avg %PnL per trade",
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
            f"{s['avg_pct']:.3f}%",
            f"${s['total_fees']:,.2f}",
            f"${s['start_cap']:,.2f}",
            f"${s['final_cap']:,.2f}",
            f"${s['mdd_abs']:,.2f}",
            f"{s['mdd_pct']:.2f}%"
        ])
    print("\n===== FINAL RECAP (per pair) — Sorted by Avg %PnL per trade =====")
    print(t)
    print("==================================================================")

# ---------------- Utils: loads ----------------
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

def load_session_pairs(path: str) -> Tuple[List[str], Dict[str, str]]:
    """
    Lit un fichier texte (ou CSV) avec lignes: SESSION,PAIR
    Renvoie (liste_de_paires_uniques, session_map {PAIR: SESSION}).
    """
    pairs: List[str] = []
    sess_map: Dict[str, str] = {}
    if not os.path.exists(path):
        print(f"Error: {path} not found.")
        return pairs, sess_map
    with open(path, "r", newline="") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = [x.strip() for x in line.split(",")]
            if len(parts) < 2:
                continue
            sess = parts[0].upper()
            pair = parts[1].upper()
            if pair not in pairs:
                pairs.append(pair)
            sess_map[pair] = sess
    return pairs, sess_map

# ---------------- CLI ----------------
def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--pairs", nargs="*", default=None,
                    help="Une ou plusieurs paires (séparées par espace ou virgules dans les tokens). Si omis: lecture séquentielle de pairs.txt")
    ap.add_argument("--pairs-file", default="pairs.txt", help="CSV avec colonnes type,pair (default: pairs.txt)")
    ap.add_argument("--all", action="store_true",
                    help="Lire --session-file (format SESSION,PAIR) et backtester multi-sessions sans changer l'affichage.")
    ap.add_argument("--session-file", default="session_pairs.txt",
                    help="Fichier mapping sessions/paires pour --all (default: session_pairs.txt)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date", default="2025-12-31")
    ap.add_argument("--capital-start", type=float, default=100_000.0, help="Starting capital (default 100000)")
    ap.add_argument("--risk-pct", type=float, default=1.0, help="Risk per trade in percent of AVAILABLE capital (equity - risques ouverts)")
    ap.add_argument("--exit-rr", type=float, choices=[1.0,2.0,3.0], default=2.0, help="(non utilisé avec partiels pondérés)")
    ap.add_argument("--fee-per-lot", type=float, default=3.5, help="Fee USD par lot par transaction (entrée/exit), default 3.5")
    a=ap.parse_args()

    if a.all:
        pairs, sess_map = load_session_pairs(a.session_file)
        if not pairs:
            print("No session/pair lines found in session file.")
            sys.exit(0)
        with get_pg_conn() as c:
            print("\n==============================")
            print(f"RUNNING BACKTEST FOR (multi via --all): {', '.join(pairs)}")
            print("==============================")
            run_all_pairs(
                c, pairs,
                parse_date(a.start_date),
                parse_date(a.end_date),
                start_capital=a.capital_start,
                risk_pct=a.risk_pct,
                fee_per_lot=a.fee_per_lot,
                session_map=sess_map
            )
        return

    # Comportement historique (inchangé pour l'I/O)
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
                fee_per_lot=a.fee_per_lot
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
                    fee_per_lot=a.fee_per_lot
                )
                final_summaries.append(s)
            # RÉCAP FINAL (EXACTEMENT les mêmes infos que le summary)
            print_final_pair_summary_table(final_summaries)

if __name__=="__main__":
    main()
