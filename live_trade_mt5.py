#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LIVE Breakout + Pullback — session-aware, single-row per (pair, date) with heavy logging
MT5 STOP orders integration (BUY/SELL STOP with SL/TP, risk-based sizing).

Règles de statut UNIQUES:
- READY, TRIGGERED, CANCELLED, WIN, LOSS
- Jamais CLOSED (ni comme status, ni fallback)
- WIN/LOSS = déterminé exclusivement via MT5 (deals/history)
- Fin de fenêtre session: si non TRIGGERED → CANCELLED
"""

import os, sys, time, traceback
from datetime import datetime, timedelta, timezone, date
from typing import List, Tuple, Optional, Dict, Any

import psycopg2
from psycopg2 import extensions as pg_ext
from dotenv import load_dotenv


# ---------- RULES ----------
MIN_READY_PIPS = float(os.getenv("MIN_READY_PIPS", "6"))  # minimum stop distance (in pips) to allow READY

UTC = timezone.utc
FIFTEEN_MS = 15 * 60 * 1000
H1_MS = 60 * 60 * 1000

# ---------- ENV / DB ----------
load_dotenv()
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB", "postgres")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASS = os.getenv("PG_PASSWORD", "postgres")
PG_SSLMODE = os.getenv("PG_SSLMODE", "disable")

# Polling toutes les 15s (configurable)
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "15"))

# ---------- MT5 ----------
ENABLE_TRADING = os.getenv("ENABLE_TRADING", "true").lower() in ("1","true","yes","y")
RISK_PERCENT = float(os.getenv("RISK_PERCENT", "0.01"))
USE_EQUITY_FOR_RISK = os.getenv("USE_EQUITY_FOR_RISK","true").lower() in ("1","true","yes","y")
COMMENT_TAG = os.getenv("COMMENT_TAG", "LIVE_BPULL")
ALLOWED_DEVIATION_POINTS = int(os.getenv("ALLOWED_DEVIATION_POINTS","20"))
BROKER_MIN_DISTANCE_BUFFER_POINTS = int(os.getenv("BROKER_MIN_DISTANCE_BUFFER_POINTS","0"))
EXPIRATION_POLICY = os.getenv("EXPIRATION_POLICY","GTC").upper()   # "GTC" ou "SESSION_END"

MT5_TERMINAL_PATH = os.getenv("MT5_TERMINAL_PATH")
MT5_LOGIN = os.getenv("MT5_LOGIN")
MT5_PASSWORD = os.getenv("MT5_PASSWORD")
MT5_SERVER = os.getenv("MT5_SERVER")

# décalage horaire du serveur MT5 vs UTC (en heures)
MT5_SERVER_TZ_OFFSET_HOURS = int(os.getenv("MT5_SERVER_TZ_OFFSET_HOURS", "0"))

try:
    import MetaTrader5 as mt5
    MT5_ENABLED = True
except Exception:
    mt5 = None
    MT5_ENABLED = False

# ---------- Logging ----------
def now_iso():
    return datetime.now(tz=UTC).isoformat(timespec="seconds").replace("+00:00","Z")

def L(msg: str):
    print(f"[{now_iso()}] {msg}", flush=True)

# ---------- Helpers ----------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def candles_tbl(pair: str, tf: str) -> str:
    return f"public.candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def live_tbl() -> str:
    return "public.live_trades"

def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00","Z")

def ms_to_dt(ms: int) -> datetime:
    return datetime.fromtimestamp(ms/1000, tz=UTC)

def decimals_for(pair: str) -> int:
    p = pair.upper()
    return 3 if ("JPY" in p or p.startswith("XAU")) else 5

def round_price(pair: str, value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), decimals_for(pair))

def fmt_price(pair: str, value: Optional[float]) -> str:
    if value is None:
        return "None"
    nd = decimals_for(pair)
    fmt = f"{{:.{nd}f}}"
    return fmt.format(round_price(pair, value))

# --- Time helpers (MT5 -> UTC) ---
def mt5_time_to_utc_ms(t: Optional[int | float]) -> Optional[int]:
    if t is None:
        return None
    t_int = int(t)
    ms = t_int if t_int > 10_000_000_000 else t_int * 1000
    return ms - (MT5_SERVER_TZ_OFFSET_HOURS * 3600 * 1000)

def mt5_time_to_utc_date(t: Optional[int | float]) -> Optional[date]:
    ms = mt5_time_to_utc_ms(t)
    if ms is None:
        return None
    return datetime.fromtimestamp(ms/1000, tz=UTC).date()

# ---------- FENÊTRES DE SESSION ----------
def session_first_h1_window_utc_ms(session: str, d: date) -> Tuple[int,int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    s = session.upper()
    if s == "TOKYO":
        start = base + timedelta(hours=0)
        end   = base + timedelta(hours=9)
    elif s == "LONDON":
        start = base + timedelta(hours=7)
        end   = base + timedelta(hours=16)
    else:  # NY
        start = base + timedelta(hours=12)
        end   = base + timedelta(hours=21)
    return int(start.timestamp()*1000), int(end.timestamp()*1000)

def session_signal_window_utc_ms(session: str, d: date) -> Tuple[int,int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    s = session.upper()
    if s == "TOKYO":
        start = base + timedelta(hours=1)   # 01:00
        end   = base + timedelta(hours=6)   # 06:00
    elif s == "LONDON":
        start = base + timedelta(hours=8)   # 08:00
        end   = base + timedelta(hours=13)  # 13:00
    else:  # NY
        start = base + timedelta(hours=13)  # 13:00
        end   = base + timedelta(hours=18)  # 18:00
    return int(start.timestamp()*1000), int(end.timestamp()*1000)

# ---------- Session pairs file ----------
def load_session_file(path="session_pairs.txt") -> List[Tuple[str,str,str]]:
    out: List[Tuple[str,str,str]] = []
    if not os.path.exists(path):
        L(f"[ERR] session file '{path}' not found")
        return out
    with open(path,"r") as f:
        for raw in f:
            line = raw.strip()
            if not line or line.startswith("#"): continue
            parts = [x.strip() for x in line.split(",")]
            if len(parts) < 3: continue
            sess, pair, tp = parts[0].upper(), parts[1].upper(), parts[2].upper()
            if tp not in ("TP1","TP2","TP3"):
                L(f"[WARN] skip invalid TP tag '{tp}' in line: {line}")
                continue
            if sess in ("NEWYORK","NEW_YORK"): sess = "NY"
            if sess not in ("TOKYO","LONDON","NY"):
                L(f"[WARN] skip invalid SESSION '{sess}' in line: {line}")
                continue
            out.append((sess, pair, tp))
    L(f"SESS-FILE: loaded {len(out)} tuples")
    return out

# ---------- DB ----------
def get_pg_conn():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS} sslmode={PG_SSLMODE}"
    L(f"DB: connecting to {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB} (ssl={PG_SSLMODE}) ...")
    conn = psycopg2.connect(dsn)
    conn.autocommit = False
    with conn.cursor() as cur:
        cur.execute("SHOW search_path")
        (sp,) = cur.fetchone()
        L(f"DB: connected. search_path={sp}")
    return conn

def ensure_live_trades(conn):
    L("DB: ensuring public.live_trades ...")
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS public.live_trades (
            pair TEXT NOT NULL,
            trade_date DATE NOT NULL,
            session TEXT,
            range_1h_ts BIGINT,
            range_1h_at TIMESTAMPTZ,
            high_1h DOUBLE PRECISION,
            low_1h DOUBLE PRECISION,
            break_ts BIGINT,
            break_at TIMESTAMPTZ,
            side TEXT,
            extreme_price DOUBLE PRECISION,
            extreme_ts BIGINT,
            extreme_at TIMESTAMPTZ,
            pullback_ts BIGINT,
            pullback_at TIMESTAMPTZ,
            entry DOUBLE PRECISION,
            sl DOUBLE PRECISION,
            tp DOUBLE PRECISION,
            tp_level TEXT,
            status TEXT,
            opened_ts BIGINT,
            opened_at TIMESTAMPTZ,
            closed_ts BIGINT,
            result TEXT, -- non utilisé désormais
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            last_proc_15m_ts BIGINT,
            last_proc_15m_at TIMESTAMPTZ,
            PRIMARY KEY (pair, trade_date)
        );
        """); conn.commit()
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_live_trades_session_status
        ON public.live_trades(session, status);
        """); conn.commit()
        # colonnes MT5 idempotentes
        cur.execute(f"ALTER TABLE {live_tbl()} ADD COLUMN IF NOT EXISTS risk_pct DOUBLE PRECISION;"); conn.commit()
        cur.execute(f"ALTER TABLE {live_tbl()} ADD COLUMN IF NOT EXISTS lots DOUBLE PRECISION;"); conn.commit()
        cur.execute(f"ALTER TABLE {live_tbl()} ADD COLUMN IF NOT EXISTS mt5_order_id BIGINT;"); conn.commit()
        cur.execute(f"ALTER TABLE {live_tbl()} ADD COLUMN IF NOT EXISTS mt5_request_id BIGINT;"); conn.commit()
        cur.execute(f"ALTER TABLE {live_tbl()} ADD COLUMN IF NOT EXISTS mt5_order_type TEXT;"); conn.commit()
        cur.execute(f"ALTER TABLE {live_tbl()} ADD COLUMN IF NOT EXISTS order_status TEXT;"); conn.commit()
    L("DB: live_trades ready.")

# ---------- Candle readers ----------
def latest_15m_open_ts_for_pair(conn, pair: str) -> Optional[int]:
    t = candles_tbl(pair,"15m")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(ts) FROM {t}")
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except Exception:
        return None

def detect_current_session(conn, all_pairs: List[str]) -> Optional[Tuple[str,int]]:
    """
    Session via CLOSE of the last CLOSED 15m candle (UTC), with 1H wait before signals:

      TOKYO  : [01:15 .. 06:00]  inclusive
      LONDON : [08:15 .. 13:00]  inclusive
      NY     : [13:15 .. 18:00]  inclusive

    If the last 15m CLOSE falls outside these windows, return None (between sessions).
    """
    best_ts_open = None
    best_pair = None
    for p in all_pairs:
        ts = latest_15m_open_ts_for_pair(conn, p)
        if ts is None:
            continue
        if best_ts_open is None or ts > best_ts_open:
            best_ts_open, best_pair = ts, p

    if best_ts_open is None:
        L("[ERR] No 15m data found for any pair in session file.")
        return None

    close_ms = best_ts_open + FIFTEEN_MS
    dt = datetime.fromtimestamp(close_ms/1000, tz=UTC)
    h, m = dt.hour, dt.minute

    in_tokyo  = (h == 1 and m >= 15) or (1 < h < 6)  or (h == 6  and m == 0)
    in_london = (h == 8 and m >= 15) or (8 < h < 13) or (h == 13 and m == 0)
    in_ny     = (h == 13 and m >= 15) or (13 < h < 18) or (h == 18 and m == 0)

    if in_tokyo:
        sess = "TOKYO"
    elif in_london:
        sess = "LONDON"
    elif in_ny:
        sess = "NY"
    else:
        L(f"SESSION: between sessions — last15m_close={iso_utc(close_ms)} via {best_pair}")
        return None

    L(f"SESSION: detected={sess} using {best_pair} last15m_close={iso_utc(close_ms)}")
    return (sess, close_ms)


def read_first_h1_OPEN_in_session(conn, pair: str, session: str, d: date) -> Optional[Dict[str,Any]]:
    s, e = session_first_h1_window_utc_ms(session, d)
    t1h = candles_tbl(pair, "1h")
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT ts, open, high, low, close
            FROM {t1h}
            WHERE ts >= %s AND ts < %s
            ORDER BY ts ASC
            LIMIT 1
        """, (s, e))
        row = cur.fetchone()
    if not row:
        L(f"1H-FIRST(OPEN): none for {pair} in {session} [{iso_utc(s)}..{iso_utc(e)})")
        return None
    ts,o,h,l,c = row
    L(f"1H-FIRST(OPEN): {pair} ts(OPEN)={iso_utc(int(ts))} H={fmt_price(pair,h)} L={fmt_price(pair,l)}")
    return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}

def read_15m_in_open_window(conn, pair: str, start_open_ms: int, end_open_ms_excl: int) -> List[Dict[str,Any]]:
    t15 = candles_tbl(pair,"15m")
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT ts, open, high, low, close
            FROM {t15}
            WHERE ts >= %s AND ts < %s
            ORDER BY ts ASC
        """, (start_open_ms, end_open_ms_excl))
        rows = cur.fetchall()
    return [{"ts":int(ts), "open":float(o), "high":float(h), "low":float(l), "close":float(c)} for ts,o,h,l,c in rows]

def read_15m_after_open(conn, pair: str, start_open_ms: int, end_open_ms_excl: int) -> List[Dict[str,Any]]:
    t15 = candles_tbl(pair,"15m")
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT ts, open, high, low, close
            FROM {t15}
            WHERE ts > %s AND ts < %s
            ORDER BY ts ASC
        """, (start_open_ms, end_open_ms_excl))
        rows = cur.fetchall()
    return [{"ts":int(ts), "open":float(o), "high":float(h), "low":float(l), "close":float(c)} for ts,o,h,l,c in rows]

# ---------- Row helpers ----------
def upsert_base_row(conn, pair: str, trade_day: date, session: str, tp_level: str):
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {live_tbl()} WHERE pair=%s AND trade_date=%s", (pair, trade_day))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(f"""
                INSERT INTO {live_tbl()} (pair, trade_date, session, tp_level, updated_at)
                VALUES (%s,%s,%s,%s, NOW())
                ON CONFLICT (pair,trade_date) DO NOTHING
            """, (pair, trade_day, session, tp_level)); conn.commit()
            L(f"ROW: INSERT base {pair} {trade_day} session={session} tp={tp_level}")
        else:
            cur.execute(f"""
                UPDATE {live_tbl()}
                SET session=%s,
                    tp_level=%s,
                    updated_at=NOW()
                WHERE pair=%s AND trade_date=%s
            """, (session, tp_level, pair, trade_day)); conn.commit()
            L(f"ROW: UPDATE base {pair} {trade_day} session={session} tp={tp_level}")

def set_range_1h(conn, pair: str, trade_day: date, c1: Dict[str,Any]):
    open_ts = int(c1["ts"])
    h = round_price(pair, c1["high"])
    l = round_price(pair, c1["low"])
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET range_1h_ts=%s,
                range_1h_at=%s,
                high_1h=%s, low_1h=%s,
                updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (open_ts, ms_to_dt(open_ts), h, l, pair, trade_day)); conn.commit()
    L(f"ROW: set 1H range {pair} {trade_day} OPEN={iso_utc(open_ts)} H={fmt_price(pair,h)} L={fmt_price(pair,l)}")

def fetch_core(conn, pair: str, trade_day: date) -> Optional[Dict[str,Any]]:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT session, range_1h_ts, high_1h, low_1h, break_ts, side,
                   extreme_price, extreme_ts, pullback_ts, entry, sl, tp,
                   tp_level, status,
                   mt5_order_id, mt5_order_type, order_status, lots, risk_pct,
                   last_proc_15m_ts
            FROM {live_tbl()}
            WHERE pair=%s AND trade_date=%s
        """, (pair, trade_day))
        row = cur.fetchone()
    if not row:
        return None
    (session, range_ts, hh, ll, break_ts, side,
     extreme_price, extreme_ts, pullback_ts, entry, sl, tp,
     tp_level, status,
     mt5_order_id, mt5_order_type, order_status, lots, risk_pct,
     last_proc_15m_ts) = row
    return {
        "session": session, "range_1h_ts": range_ts, "high_1h": hh, "low_1h": ll,
        "break_ts": break_ts, "side": side, "extreme_price": extreme_price, "extreme_ts": extreme_ts,
        "pullback_ts": pullback_ts, "entry": entry, "sl": sl, "tp": tp,
        "tp_level": tp_level, "status": status,
        "mt5_order_id": mt5_order_id, "mt5_order_type": mt5_order_type, "order_status": order_status,
        "lots": lots, "risk_pct": risk_pct,
        "last_proc_15m_ts": last_proc_15m_ts
    }

def update_break_and_extreme(conn, pair: str, trade_day: date, side: str, break_open_ts: int, extreme_price: float):
    ex = round_price(pair, extreme_price)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET side=%s,
                break_ts=%s, break_at=%s,
                extreme_price=%s, extreme_ts=%s, extreme_at=%s,
                updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (side,
              break_open_ts, ms_to_dt(break_open_ts),
              ex, break_open_ts, ms_to_dt(break_open_ts),
              pair, trade_day)); conn.commit()
    L(f"BREAK: {pair} side={side} at OPEN={iso_utc(break_open_ts)} init_extreme={fmt_price(pair,ex)}")

def persist_extreme(conn, pair: str, trade_day: date, price: float, bar_open_ts: int):
    ex = round_price(pair, price)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET extreme_price=%s, extreme_ts=%s, extreme_at=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (ex, bar_open_ts, ms_to_dt(bar_open_ts), pair, trade_day)); conn.commit()
    L(f"EXTREME: {pair} updated extreme={fmt_price(pair,ex)} @ OPEN={iso_utc(bar_open_ts)}")

# ---------- Cursor 15m ----------
def bump_last_processed(conn, pair: str, trade_day: date, ts_open: int):
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET last_proc_15m_ts=%s,
                last_proc_15m_at=%s,
                updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (ts_open, ms_to_dt(ts_open), pair, trade_day)); conn.commit()
    L(f"CURSOR: {pair} last_proc_15m_ts → {iso_utc(ts_open)}")

# ---------- TP ----------
def compute_tp(entry: float, sl: float, side: str, tp_level: str) -> float:
    k = 1 if (tp_level or "").upper()=="TP1" else 2 if (tp_level or "").upper()=="TP2" else 3
    r = abs(entry - sl)
    return entry + k*r if (side or "").upper()=="LONG" else entry - k*r

# ---------- MT5 HELPERS ----------
def mt5_initialize_if_needed() -> bool:
    if not (ENABLE_TRADING and MT5_ENABLED):
        return False
    ok = mt5.initialize(MT5_TERMINAL_PATH) if MT5_TERMINAL_PATH else mt5.initialize()
    if not ok:
        L(f"[MT5] init failed: {mt5.last_error()}")
        return False
    if MT5_LOGIN and MT5_PASSWORD:
        if not mt5.login(int(MT5_LOGIN), MT5_PASSWORD, MT5_SERVER):
            L(f"[MT5] login failed: {mt5.last_error()}")
            return False
    return True

def mt5_preflight(symbol: str) -> bool:
    ti = mt5.terminal_info()
    ai = mt5.account_info()
    si = mt5.symbol_info(symbol)
    if not (ti and ai and si):
        L(f"[MT5] preflight: missing info: {mt5.last_error()}"); return False
    if not getattr(ti, "trade_allowed", True) or not getattr(ti, "trade_expert", True):
        L("[MT5] ❌ AutoTrading disabled in terminal."); return False
    if not getattr(ai, "trade_allowed", True):
        L("[MT5] ❌ Trading not allowed for this account."); return False
    if not si.visible and not mt5.symbol_select(symbol, True):
        L(f"[MT5] ❌ Cannot select {symbol}."); return False
    return True

def pip_size_for(symbol: str) -> float:
    return 0.01 if symbol.endswith("JPY") else 0.0001

def pip_value_per_lot_usd(symbol: str) -> Optional[float]:
    si = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if not (si and tick): return None
    price = tick.ask or tick.bid
    if not price or price <= 0: return None
    contract = si.trade_contract_size or 100000.0
    pipsz = pip_size_for(symbol)
    pv_quote = contract * pipsz
    return float(pv_quote / price) if symbol.endswith("JPY") else float(pv_quote)

def round_volume(volume: float, symbol: str) -> float:
    si = mt5.symbol_info(symbol)
    step = si.volume_step
    vmin = si.volume_min
    vmax = si.volume_max
    steps = int(volume // step)
    vol = max(vmin, min(steps * step, vmax))
    return round(vol, 3)

def calc_lots_risk(symbol: str, risk_pct: float, entry: float, sl: float) -> Optional[float]:
    ai = mt5.account_info()
    if not ai: return None
    capital = float(ai.equity) if USE_EQUITY_FOR_RISK else float(ai.margin_free)
    target_risk_usd = max(0.0, capital * risk_pct)
    pv = pip_value_per_lot_usd(symbol)
    if not pv or pv <= 0: return None
    pipsz = pip_size_for(symbol)
    sl_pips = abs(entry - sl) / pipsz
    if sl_pips <= 0: return None
    lots = target_risk_usd / (pv * sl_pips)
    return round_volume(lots, symbol)

def ensure_stop_type_and_distances(symbol: str, side: str, entry: float, sl: float, tp: float) -> Tuple[float,float,float,str]:
    si = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    p = si.point
    min_points = (si.trade_stops_level or 0) + BROKER_MIN_DISTANCE_BUFFER_POINTS
    ask = tick.ask; bid = tick.bid

    if side.upper() == "LONG":
        order_type_str = "BUY_STOP"
        min_entry = ask + (min_points * p)
        if entry < min_entry:
            entry = round(min_entry, si.digits)
            L(f"[MT5] Adjust BUY_STOP entry to satisfy stops_level → {entry}")
        if min_points > 0:
            if (entry - sl)/p < min_points:
                sl = round(entry - (min_points*p), si.digits)
            if (tp - entry)/p < min_points:
                tp = round(entry + (min_points*p), si.digits)
    else:
        order_type_str = "SELL_STOP"
        min_entry = bid - (min_points * p)
        if entry > min_entry:
            entry = round(min_entry, si.digits)
            L(f"[MT5] Adjust SELL_STOP entry to satisfy stops_level → {entry}")
        if min_points > 0:
            if (sl - entry)/p < min_points:
                sl = round(entry + (min_points*p), si.digits)
            if (entry - tp)/p < min_points:
                tp = round(entry - (min_points*p), si.digits)

    return (round(entry, si.digits), round(sl, si.digits), round(tp, si.digits), order_type_str)

def place_stop_order(symbol: str, side: str, entry: float, sl: float, tp: float,
                     lots: float, expiration_dt: Optional[datetime]) -> Tuple[bool, Optional[int], Optional[int], str]:
    if not mt5_preflight(symbol):
        return (False, None, None, "preflight")
    entry, sl, tp, order_type_str = ensure_stop_type_and_distances(symbol, side, entry, sl, tp)

    req = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": float(lots),
        "type": mt5.ORDER_TYPE_BUY_STOP if order_type_str=="BUY_STOP" else mt5.ORDER_TYPE_SELL_STOP,
        "price": entry,
        "sl": sl,
        "tp": tp,
        "comment": COMMENT_TAG,
    }
    if EXPIRATION_POLICY == "GTC":
        req["type_time"] = mt5.ORDER_TIME_GTC
    else:
        req["type_time"] = mt5.ORDER_TIME_SPECIFIED
        req["expiration"] = expiration_dt if expiration_dt is not None else None

    result = mt5.order_send(req)
    if result is None:
        L(f"[MT5] order_send returned None: {mt5.last_error()}")
        return (False, None, None, "send_none")
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        L(f"[MT5] ❌ order failed: retcode={result.retcode} comment={result.comment}")
        return (False, None, None, f"retcode_{result.retcode}")

    L(f"[MT5] ✅ {order_type_str} placed: lots={lots} entry={entry} sl={sl} tp={tp} | order={result.order}")
    return (True, int(result.order), int(result.request_id), order_type_str)

def modify_stop_order(order_id: int, sl: float, tp: float) -> bool:
    orders = mt5.orders_get(ticket=order_id)
    if not orders:
        L(f"[MT5] modify: order {order_id} not pending anymore."); return False
    o = orders[0]
    req = {
        "action": mt5.TRADE_ACTION_MODIFY,
        "order": order_id,
        "symbol": o.symbol,
        "price": o.price_open,
        "sl": sl,
        "tp": tp,
        "comment": COMMENT_TAG
    }
    res = mt5.order_send(req)
    if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
        L(f"[MT5] ❌ modify failed: {None if res is None else res.retcode} | {None if res is None else res.comment}")
        return False
    L(f"[MT5] 🔧 order modified: order={order_id} new SL={sl} TP={tp}")
    return True

# ---------- Cancel pending on flip ----------
def cancel_pending_mt5_order_if_any(conn, pair: str, trade_day: date):
    if not (ENABLE_TRADING and MT5_ENABLED and mt5_initialize_if_needed()):
        return
    with conn.cursor() as cur:
        cur.execute(f"SELECT mt5_order_id FROM {live_tbl()} WHERE pair=%s AND trade_date=%s", (pair, trade_day))
        row = cur.fetchone()
    if not row or not row[0]:
        return
    order_id = int(row[0])
    orders = mt5.orders_get(ticket=order_id)
    if not orders:
        L(f"[MT5] cancel: order {order_id} not pending anymore.")
    else:
        o = orders[0]
        req = {
            "action": mt5.TRADE_ACTION_REMOVE,
            "order": order_id,
            "symbol": o.symbol,
            "comment": COMMENT_TAG
        }
        res = mt5.order_send(req)
        if res is None or res.retcode != mt5.TRADE_RETCODE_DONE:
            L(f"[MT5] ❌ cancel failed: {None if res is None else res.retcode} | {None if res is None else res.comment}")
        else:
            L(f"[MT5] 🗑️ pending order cancelled: order={order_id}")
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET order_status='CANCELLED',
                mt5_order_id=NULL,
                mt5_request_id=NULL,
                mt5_order_type=NULL,
                updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (pair, trade_day)); conn.commit()

# ---------- Hooks (placement / modif) ----------
def place_mt5_stop_at_ready(conn, pair: str, trade_day: date, side: str,
                            entry_r: float, sl_r: float, tp_r: float, session_end_ms: int):
    if not (ENABLE_TRADING and MT5_ENABLED and mt5_initialize_if_needed()):
        return
    lots = calc_lots_risk(pair, RISK_PERCENT, entry_r, sl_r)
    if not lots or lots <= 0:
        L(f"[MT5] skip place: invalid lots for {pair}."); return

    expiration_dt = None
    if EXPIRATION_POLICY != "GTC":
        expiration_dt = ms_to_dt(session_end_ms)

    ok, order_id, request_id, order_type = place_stop_order(
        pair, side, entry_r, sl_r, tp_r, lots, expiration_dt
    )
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET risk_pct=%s, lots=%s,
                mt5_order_id=%s, mt5_request_id=%s, mt5_order_type=%s,
                order_status=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (RISK_PERCENT, lots,
              order_id, request_id, order_type,
              ("PLACED" if ok else "REJECTED"), pair, trade_day)); conn.commit()

def modify_mt5_stop_after_sl_update(conn, pair: str, trade_day: date, new_sl_r: float, new_tp_r: float):
    if not (ENABLE_TRADING and MT5_ENABLED and mt5_initialize_if_needed()):
        return
    with conn.cursor() as cur:
        cur.execute(f"SELECT mt5_order_id FROM {live_tbl()} WHERE pair=%s AND trade_date=%s", (pair, trade_day))
        row = cur.fetchone()
    if not row or not row[0]:
        return
    order_id = int(row[0])
    si = mt5.symbol_info(pair)
    sl = round(float(new_sl_r), si.digits)
    tp = round(float(new_tp_r), si.digits)
    ok = modify_stop_order(order_id, sl, tp)
    if ok:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {live_tbl()}
                SET order_status='MODIFIED', updated_at=NOW()
                WHERE pair=%s AND trade_date=%s
            """, (pair, trade_day)); conn.commit()

# ---------- DB row helpers (suite) ----------
def mark_ready(conn, pair: str, trade_day: date, pullback_open_ts: int,
               entry: float, sl: float, side: str, tp_level: str, session_end_ms: int):
    if pullback_open_ts >= session_end_ms:
        L(f"[MT5] skip place: session over for {pair} (DB-timed).")
        return

    entry_r = round_price(pair, entry)
    sl_r    = round_price(pair, sl)
    tp_raw  = compute_tp(entry_r, sl_r, side, tp_level)
    tp_r    = round_price(pair, tp_raw)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET pullback_ts=%s, pullback_at=%s,
                entry=%s, sl=%s, tp=%s,
                status=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (pullback_open_ts, ms_to_dt(pullback_open_ts),
              entry_r, sl_r, tp_r,
              'READY', pair, trade_day)); conn.commit()
    L(f"READY: {pair} pullback OPEN={iso_utc(pullback_open_ts)} entry={fmt_price(pair,entry_r)} sl={fmt_price(pair,sl_r)} tp={fmt_price(pair,tp_r)}")

    try:
        place_mt5_stop_at_ready(conn, pair, trade_day, side, entry_r, sl_r, tp_r, session_end_ms)
    except Exception as e:
        L(f"[MT5-READY-ERR] {pair}: {e}")
        traceback.print_exc()

def update_sl_and_tp(conn, pair: str, trade_day: date, new_sl: float, entry: float, side: str, tp_level: str, bar_open_ts: int):
    entry_r = round_price(pair, entry)
    sl_r    = round_price(pair, new_sl)
    tp_raw  = compute_tp(entry_r, sl_r, side, tp_level)
    tp_r    = round_price(pair, tp_raw)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET sl=%s, tp=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (sl_r, tp_r, pair, trade_day)); conn.commit()
    L(f"SL/TP-UPDATE: {pair} ts OPEN={iso_utc(bar_open_ts)} new_sl={fmt_price(pair,sl_r)} new_tp={fmt_price(pair,tp_r)}")

    try:
        modify_mt5_stop_after_sl_update(conn, pair, trade_day, sl_r, tp_r)
    except Exception as e:
        L(f"[MT5-UPDATE-ERR] {pair}: {e}")
        traceback.print_exc()

def mark_triggered(conn, pair: str, trade_day: date, trigger_open_ts: int):
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET status='TRIGGERED', opened_ts=%s, opened_at=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (trigger_open_ts, ms_to_dt(trigger_open_ts), pair, trade_day)); conn.commit()
    L(f"TRIGGERED: {pair} at OPEN={iso_utc(trigger_open_ts)}")

def mark_outcome(conn, pair: str, trade_day: date, closed_ms: int, outcome: str):
    assert outcome in ("WIN","LOSS","CANCELLED")
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET status=%s, closed_ts=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (outcome, closed_ms, pair, trade_day)); conn.commit()
    L(f"{outcome}: {pair} at {iso_utc(closed_ms)}")

# ---------- RESET helper pour FLIP ----------
def reset_state_on_flip(conn, pair: str, trade_day: date):
    try:
        cancel_pending_mt5_order_if_any(conn, pair, trade_day)
    except Exception as e:
        L(f"[MT5-CANCEL-ERR] {pair}: {e}")
        traceback.print_exc()
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET pullback_ts=NULL, pullback_at=NULL,
                entry=NULL, sl=NULL, tp=NULL,
                status=NULL,
                opened_ts=NULL, opened_at=NULL,
                updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (pair, trade_day)); conn.commit()
    L(f"RESET: {pair} post-break state cleared due to flip")

# --- WIN/LOSS from MT5 history ---
def infer_result_from_history(order_id: int,
                              symbol: str,
                              tp: Optional[float],
                              sl: Optional[float],
                              lookback_days: int = 7) -> Tuple[Optional[str], Optional[int]]:
    """
    Détermine l'issue d'un trade MT5 (WIN/LOSS) à partir de l'historique des deals.
    Utilise directement position_id = order_id (cas typique des ordres FILLED).
    """
    try:
        now_utc = datetime.now(tz=UTC)
        t_from = now_utc - timedelta(days=lookback_days)

        # 🔍 Récupère tous les deals récents liés à cette position/ticket
        deals = mt5.history_deals_get(position=order_id)
        if not deals:
            # fallback : intervalle large si pas trouvé
            deals = mt5.history_deals_get(t_from, now_utc)
        if not deals:
            return None, None

        DEAL_ENTRY_OUT = getattr(mt5, "DEAL_ENTRY_OUT", 1)
        DEAL_REASON_TP = getattr(mt5, "DEAL_REASON_TP", 6)
        DEAL_REASON_SL = getattr(mt5, "DEAL_REASON_SL", 7)

        out_deal = None
        for d in deals:
            if getattr(d, "symbol", "") != symbol:
                continue
            if getattr(d, "entry", None) == DEAL_ENTRY_OUT:
                if out_deal is None or getattr(d, "time_msc", 0) > getattr(out_deal, "time_msc", 0):
                    out_deal = d

        if out_deal is None:
            return None, None

        # 🎯 Détermination du résultat
        reason = getattr(out_deal, "reason", None)
        price  = float(getattr(out_deal, "price", 0.0) or 0.0)

        if reason == DEAL_REASON_TP:
            result = "WIN"
        elif reason == DEAL_REASON_SL:
            result = "LOSS"
        else:
            # fallback : comparer au SL/TP
            result = None
            tol = 1e-4
            if tp is not None and abs(price - float(tp)) <= tol:
                result = "WIN"
            elif sl is not None and abs(price - float(sl)) <= tol:
                result = "LOSS"

        # ⏰ Timestamp clôture UTC
        t_msc = getattr(out_deal, "time_msc", None)
        t_sec = getattr(out_deal, "time", None)
        closed_ms = mt5_time_to_utc_ms(t_msc if t_msc else t_sec)

        return result, closed_ms

    except Exception as e:
        L(f"[MT5-HISTORY-ERR] {symbol} {order_id}: {e}")
        traceback.print_exc()
        return None, None


# ---------- MT5 reconcile ----------
def reconcile_with_mt5(conn, pair: str, trade_day: date, last_close_ms: int):
    if not (ENABLE_TRADING and MT5_ENABLED and mt5_initialize_if_needed()):
        return
    row = fetch_core(conn, pair, trade_day)
    if not row:
        return

    status   = (row["status"] or "").upper() if row["status"] else None
    order_id = row["mt5_order_id"]

    # A) position live ?
    pos = None
    if order_id:
        try:
            pp = mt5.positions_get(ticket=int(order_id))
            pos = pp[0] if pp else None
        except Exception:
            pos = None
    if pos is None:
        try:
            plist = mt5.positions_get(symbol=pair)
            if plist and len(plist) == 1:
                pos = plist[0]
        except Exception:
            pass

    # B) passage TRIGGERED
    if pos:
        if status != "TRIGGERED":
            mark_triggered(conn, pair, trade_day, last_close_ms)
            try:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        UPDATE {live_tbl()}
                        SET order_status='FILLED', updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s
                    """, (pair, trade_day)); conn.commit()
            except Exception:
                pass
        return

    # C) position disparue et on était TRIGGERED → chercher WIN/LOSS
    if status == "TRIGGERED" and pos is None:
        tp = row.get("tp"); sl = row.get("sl")
        res, closed_ms_hist = infer_result_from_history(int(order_id) if order_id else 0, pair, tp, sl, lookback_days=7)
        if res in ("WIN","LOSS"):
            mark_outcome(conn, pair, trade_day, (closed_ms_hist or last_close_ms), res)
        else:
            L(f"[MT5] Outcome indéterminé (encore). On attend. pair={pair}")
        return

    # D) pendant la session: si READY mais plus d’ordre et pas de position → on note juste order_status (pas de CANCELLED de status ici)
    pending = None
    if order_id:
        try:
            pendings = mt5.orders_get(ticket=int(order_id))
            pending = pendings[0] if pendings else None
        except Exception:
            pending = None
    if status == "READY" and row.get("entry") is not None:
        if (order_id is not None) and (pending is None) and (pos is None):
            with conn.cursor() as cur:
                cur.execute(f"""
                    UPDATE {live_tbl()}
                    SET order_status='CANCELLED', updated_at=NOW()
                    WHERE pair=%s AND trade_date=%s
                """, (pair, trade_day)); conn.commit()
            L(f"[MT5] Pending missing & no position → order_status=CANCELLED (status inchangé) for {pair}")

# ---------- Fin de session ----------
def end_of_session_cleanup(conn, pair: str, trade_day: date, e_ms: int):
    """Fin fenêtre: si pas TRIGGERED/WIN/LOSS → annule le pending et marque CANCELLED (status)."""
    row = fetch_core(conn, pair, trade_day)
    if not row:
        return
    st = (row["status"] or "").upper() if row["status"] else None
    if st in ("TRIGGERED","WIN","LOSS"):
        # si TRIGGERED mais toujours pas WIN/LOSS, on laisse MT5 faire foi (pas d'annulation de position ici)
        return
    try:
        cancel_pending_mt5_order_if_any(conn, pair, trade_day)
    except Exception as e:
        L(f"[MT5-ENDSESSION-CANCEL-ERR] {pair}: {e}")
        traceback.print_exc()
    # Marque CANCELLED avec timestamp fin de fenêtre
    mark_outcome(conn, pair, trade_day, e_ms, "CANCELLED")

# ---------- Core logic ----------
def process_pair(conn, session: str, pair: str, tp_level: str, today: date, last_close_ms: int):
    upsert_base_row(conn, pair, today, session, tp_level)
    row = fetch_core(conn, pair, today)
    if not row:
        L(f"[WARN] {pair}: row missing after upsert"); return

    s_ms, e_ms = session_signal_window_utc_ms(session, today)

    # range H1 = 1ère H1 de la session
    if not row["range_1h_ts"]:
        c1 = read_first_h1_OPEN_in_session(conn, pair, session, today)
        if not c1:
            L(f"{pair}: waiting first H1 OPEN in session.")
            return
        set_range_1h(conn, pair, today, c1)
        row = fetch_core(conn, pair, today)

    high_1h, low_1h = row["high_1h"], row["low_1h"]

    # borne haute = dernier 15m fermé pour ce pair
    last_15m_open_for_pair = latest_15m_open_ts_for_pair(conn, pair)
    if last_15m_open_for_pair is None:
        L(f"{pair}: no 15m data in DB."); return
    end_excl = min(e_ms, last_15m_open_for_pair + FIFTEEN_MS)

    # si la session est déjà finie (DB), on ne traite pas les barres
    if last_close_ms >= e_ms:
        L(f"{pair}: session DB-ended at {iso_utc(e_ms)}; skipping bar logic.")
        return

    # curseur
    last_done_ts = row.get("last_proc_15m_ts") or (s_ms - 1)

    # 1) break initial
    side = (row["side"] or "").upper() if row["side"] else None
    break_open_ts = row["break_ts"]
    if not side or not break_open_ts:
        c15_all = read_15m_in_open_window(conn, pair, s_ms, end_excl)
        c15 = [b for b in c15_all if int(b["ts"]) > int(last_done_ts)]
        if not c15:
            L(f"{pair}: no new 15m to check for break."); return
        brk_side = None; brk_idx = None
        for i, b in enumerate(c15):
            if b["close"] > high_1h: brk_side, brk_idx = "LONG", i; break
            if b["close"] < low_1h:  brk_side, brk_idx = "SHORT", i; break
        bump_last_processed(conn, pair, today, int(c15[-1]["ts"]))
        if brk_side is None:
            L(f"{pair}: new bars scanned, still no break."); return
        b0 = c15[brk_idx]
        break_open_ts = int(b0["ts"])
        init_extreme  = b0["high"] if brk_side == "LONG" else b0["low"]
        update_break_and_extreme(conn, pair, today, brk_side, break_open_ts, init_extreme)
        row = fetch_core(conn, pair, today)
        side = brk_side
        last_done_ts = row.get("last_proc_15m_ts") or last_done_ts

    # 2) post-break
    extreme      = row["extreme_price"]
    pullback_ts  = row["pullback_ts"]
    status       = (row["status"] or "").upper() if row["status"] else None
    eff_tp_level = (row["tp_level"] or tp_level)

    c15_all = read_15m_after_open(conn, pair, break_open_ts, end_excl)
    c15 = [b for b in c15_all if int(b["ts"]) > int(last_done_ts)]
    if not c15:
        L(f"{pair}: no new 15m after break."); return

    ready_seen    = status == "READY" or (pullback_ts is not None and row["entry"] is not None and row["sl"] is not None)
    current_entry = row["entry"]
    current_sl    = row["sl"]

    for b in c15:
        o, h, l, c = b["open"], b["high"], b["low"], b["close"]
        ts_open = int(b["ts"])

        # FLIP (seulement si pas TRIGGERED)
        if status != "TRIGGERED":
            if side == "LONG" and c < low_1h:
                L(f"{pair}: FLIP → SHORT (close {fmt_price(pair,c)} < low_1h {fmt_price(pair,low_1h)}) @ {iso_utc(ts_open)}")
                reset_state_on_flip(conn, pair, today)
                side = "SHORT"; break_open_ts = ts_open; extreme = l
                update_break_and_extreme(conn, pair, today, side, break_open_ts, extreme)
                bump_last_processed(conn, pair, today, ts_open)
                continue
            if side == "SHORT" and c > high_1h:
                L(f"{pair}: FLIP → LONG (close {fmt_price(pair,c)} > high_1h {fmt_price(pair,high_1h)}) @ {iso_utc(ts_open)}")
                reset_state_on_flip(conn, pair, today)
                side = "LONG"; break_open_ts = ts_open; extreme = h
                update_break_and_extreme(conn, pair, today, side, break_open_ts, extreme)
                bump_last_processed(conn, pair, today, ts_open)
                continue

        if not ready_seen:
            if side == "LONG" and (extreme is None or h > extreme):
                extreme = h; persist_extreme(conn, pair, today, extreme, ts_open)
            if side == "SHORT" and (extreme is None or l < extreme):
                extreme = l; persist_extreme(conn, pair, today, extreme, ts_open)

            is_antagonistic = (c < o) if side == "LONG" else (c > o)
            if is_antagonistic:
                if side == "LONG" and h > extreme:
                    extreme = h; persist_extreme(conn, pair, today, extreme, ts_open)
                elif side == "SHORT" and l < extreme:
                    extreme = l; persist_extreme(conn, pair, today, extreme, ts_open)

                entry = float(extreme)
                sl    = float(l) if side == "LONG" else float(h)

                # --- NEW: skip READY if stop distance < MIN_READY_PIPS ---
                pipsz = pip_size_for(pair)  # 0.01 JPY, 0.0001 otherwise
                dist_pips = abs(entry - sl) / pipsz
                if dist_pips < MIN_READY_PIPS:
                    L(f"READY-SKIP: {pair} pullback OPEN={iso_utc(ts_open)} "
                      f"dist={dist_pips:.1f} pips < {MIN_READY_PIPS} → ignore trade")
                else:
                    mark_ready(conn, pair, today, ts_open, entry, sl, side, eff_tp_level, e_ms)
                    ready_seen    = True
                    current_entry = round_price(pair, entry)
                    current_sl    = round_price(pair, sl)

        # Trailing SL/TP tant que READY (pas TRIGGERED)
        if ready_seen and (status != "TRIGGERED"):
            new_sl = float(l if side == "LONG" else h)
            if current_sl is None or round_price(pair, new_sl) != current_sl:
                update_sl_and_tp(conn, pair, today, new_sl, current_entry, side, eff_tp_level, ts_open)
                current_sl = round_price(pair, new_sl)

        bump_last_processed(conn, pair, today, ts_open)


# ---------- Main ----------
def main():
    try:
        with get_pg_conn() as conn:
            ensure_live_trades(conn)

            while True:
                loop_ts = now_iso()
                try:
                    tuples = load_session_file()
                    if not tuples:
                        L("[ERR] No valid entries in session_pairs.txt")
                    else:
                        all_pairs = [p for _,p,_ in tuples]
                        sess_detect = detect_current_session(conn, all_pairs)
                        if not sess_detect:
                            L(f"RUN[{loop_ts}]: no active session — skipping.")
                        else:
                        if sess_detect:
                            current_session, last_close_ms = sess_detect
                            today = datetime.fromtimestamp(last_close_ms/1000, tz=UTC).date()
                            s_ms, e_ms = session_signal_window_utc_ms(current_session, today)
                            L(f"RUN[{loop_ts}]: session={current_session}, window=[{iso_utc(s_ms)}..{iso_utc(e_ms)})")

                            session_pairs = [(s,p,tp) for (s,p,tp) in tuples if (s == current_session)]
                            if session_pairs:
                                for _, pair, tp in session_pairs:
                                    upsert_base_row(conn, pair, today, current_session, tp)

                                # 1) Bar processing
                                for _, pair, tp in session_pairs:
                                    try:
                                        process_pair(conn, current_session, pair, tp, today, last_close_ms)
                                    except Exception as e:
                                        L(f"[PROC-ERR] {pair}: {e}")
                                        traceback.print_exc()

                                # 2) MT5 reconcile
                                for _, pair, _ in session_pairs:
                                    try:
                                        reconcile_with_mt5(conn, pair, today, last_close_ms)
                                    except Exception as e:
                                        L(f"[MT5-POLL-ERR] {pair}: {e}")
                                        traceback.print_exc()

                                # 3) End-of-session: cancel & mark CANCELLED si pas TRIGGERED
                                if last_close_ms >= e_ms:
                                    for _, pair, _ in session_pairs:
                                        try:
                                            end_of_session_cleanup(conn, pair, today, e_ms)
                                        except Exception as e:
                                            L(f"[ENDSESSION-ERR] {pair}: {e}")
                                            traceback.print_exc()
                            else:
                                L("RUN: nothing to process for this session.")
                except Exception as e:
                    L(f"[LOOP-ERR] {e}")
                    traceback.print_exc()

                time.sleep(POLL_SECONDS)

    except Exception as e:
        L(f"[FATAL] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
