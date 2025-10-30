#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LIVE Breakout + Pullback — aligned to backtest, FLIP+RESET pre-trigger
CHANGES:
- No BIGINT epoch in DB (only TIMESTAMPTZ *_at)
- No extreme_* columns in DB; we drive the 'entry' directly (tracks prev-extreme)

Logic:
- Windows (UTC): TOKYO 01:00–05:45 ; LONDON 08:00–12:45 ; NY 13:00–17:45
- H1 Range = first H1 of day (00:00–01:00 UTC)
- Break = 15m close > H1 High (LONG) / close < H1 Low (SHORT)
- After break: maintain 'entry' = current prev-extreme (HH for LONG / LL for SHORT)
- Pullback (antagonistic color) → READY: set SL anchor = last low/high BEFORE pullback (fixed), compute TPk from entry/SL
- Trail (while READY & not triggered): update SL only if current bar makes a new farther extreme vs current SL (LONG: lower low; SHORT: higher high)
- Flip allowed BEFORE TRIGGER: on opposite strict break → reset state, set new side/break_at, and continue
- One row per (pair, date); Status: READY, TRIGGERED, WIN, LOSS, CANCELLED
"""

import os, sys, time, csv, traceback, random
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Dict, Any, Tuple, List

import psycopg2
from psycopg2 import extensions as pg_ext
from dotenv import load_dotenv

UTC = timezone.utc
FIFTEEN_MS = 15*60*1000

# ---------- ENV ----------
load_dotenv()
PG_HOST=os.getenv("PG_HOST","127.0.0.1"); PG_PORT=int(os.getenv("PG_PORT","5432"))
PG_DB=os.getenv("PG_DB","postgres"); PG_USER=os.getenv("PG_USER","postgres")
PG_PASS=os.getenv("PG_PASSWORD","postgres"); PG_SSLMODE=os.getenv("PG_SSLMODE","disable")

POLL_SECONDS=int(os.getenv("POLL_SECONDS","15"))
ENABLE_TRADING=os.getenv("ENABLE_TRADING","true").lower() in ("1","true","yes","y")
RISK_PERCENT=float(os.getenv("RISK_PERCENT","0.05"))/100.0
USE_EQUITY_FOR_RISK=os.getenv("USE_EQUITY_FOR_RISK","true").lower() in ("1","true","yes","y")
COMMENT_TAG=os.getenv("COMMENT_TAG","LIVE_BPULL")
ALLOWED_DEV_POINTS=int(os.getenv("ALLOWED_DEVIATION_POINTS","20"))
BROKER_MIN_DIST_BUF_PTS=int(os.getenv("BROKER_MIN_DISTANCE_BUFFER_POINTS","0"))
MAX_LEVERAGE=float(os.getenv("MAX_LEVERAGE","10"))
MIN_READY_PIPS=float(os.getenv("MIN_READY_PIPS","0"))
FEE_PER_LOT=float(os.getenv("FEE_PER_LOT","2.5"))

# --- NEW: throttle tuning via env ---
ORDER_THROTTLE_MIN_S=int(os.getenv("ORDER_THROTTLE_MIN_SECONDS","10"))
ORDER_THROTTLE_MAX_S=int(os.getenv("ORDER_THROTTLE_MAX_SECONDS","30"))

MT5_TERMINAL_PATH=os.getenv("MT5_TERMINAL_PATH")
MT5_LOGIN=os.getenv("MT5_LOGIN"); MT5_PASSWORD=os.getenv("MT5_PASSWORD"); MT5_SERVER=os.getenv("MT5_SERVER")
MT5_SERVER_TZ_OFFSET_HOURS=int(os.getenv("MT5_SERVER_TZ_OFFSET_HOURS","0"))

try:
    import MetaTrader5 as mt5
    MT5_ENABLED=True
except Exception:
    mt5=None; MT5_ENABLED=False

# ---------- LOG ----------
def now_iso(): return datetime.now(tz=UTC).isoformat(timespec="seconds").replace("+00:00","Z")
def L(msg:str): print(f"[{now_iso()}] {msg}", flush=True)

# ---------- Helpers ----------
def sanitize_pair(pair:str)->str:
    import re; return re.sub(r"[^a-z0-9]","",pair.lower())
def candles_tbl(pair:str, tf:str)->str:
    return f"public.candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"
def live_tbl()->str: return "public.live_trades"

def ms_to_dt(ms:int)->datetime: return datetime.fromtimestamp(ms/1000,tz=UTC)
def iso_utc(ms:int)->str: return ms_to_dt(ms).isoformat(timespec="seconds").replace("+00:00","Z")

def decimals_for(pair:str)->int:
    p=pair.upper()
    if p.startswith("XAU") or p.endswith("JPY"): return 3
    return 5
def round_price(pair:str, v:Optional[float])->Optional[float]:
    if v is None: return None
    return round(float(v), decimals_for(pair))
def fmt_price(pair:str, v:Optional[float])->str:
    if v is None: return "None"
    return f"{round_price(pair,v):.{decimals_for(pair)}f}"

def pip_size_for(pair:str)->float:
    p=pair.upper()
    if p.startswith("XAU"): return 0.01
    return 0.01 if p.endswith("JPY") else 0.0001

TERMINAL_STATES = {"WIN","LOSS","CANCELLED"}
def is_terminal(st: Optional[str]) -> bool:
    return (st or "").upper() in TERMINAL_STATES

# ---------- Session windows (UTC) ----------
def tokyo_signal_window(d:date)->Tuple[int,int]:
    base=datetime(d.year,d.month,d.day,tzinfo=UTC)
    return int((base+timedelta(hours=1)).timestamp()*1000), int((base+timedelta(hours=5,minutes=45)).timestamp()*1000)
def london_signal_window(d:date)->Tuple[int,int]:
    base=datetime(d.year,d.month,d.day,tzinfo=UTC)
    return int((base+timedelta(hours=8)).timestamp()*1000), int((base+timedelta(hours=12,minutes=45)).timestamp()*1000)
def ny_signal_window(d:date)->Tuple[int,int]:
    base=datetime(d.year,d.month,d.day,tzinfo=UTC)
    return int((base+timedelta(hours=13)).timestamp()*1000), int((base+timedelta(hours=17,minutes=45)).timestamp()*1000)

def session_signal_window(session:str, d:date)->Tuple[int,int]:
    s=session.upper()
    if s=="TOKYO": return tokyo_signal_window(d)
    if s=="LONDON": return london_signal_window(d)
    return ny_signal_window(d)

# ====================== NEW: TYPE overrides + class defaults ======================
# A) TYPE override from session file (no tuple shape change elsewhere)
TYPE_OVERRIDE: Dict[str, str] = {}  # {"XAUUSD": "METAL", "NAS100": "INDEX", ...}

def instrument_type_for(symbol:str)->str:
    s=(symbol or "").upper()
    t=TYPE_OVERRIDE.get(s)
    if t in ("FOREX","METAL","INDEX","CRYPTO"): return t
    if s in ("XAUUSD","XAGUSD","XPTUSD","XPDUSD"): return "METAL"
    if any(s.startswith(p) for p in ("US30","US500","SP500","NAS100","DAX40","GER40","UK100","FRA40","JP225","JPN225","HK50","AUS200","ES35","IT40","CN50")):
        return "INDEX"
    if any(s.startswith(p) for p in ("BTC","ETH","LTC","XRP")) or s in ("BTCUSD","ETHUSD"):
        return "CRYPTO"
    return "FOREX"

# B) Class defaults (same esprit que le backtest ; pas de table par symbole)
CLASS_DEFAULTS = {
    "FOREX":  {"tick_size": 0.0001, "tick_size_jpy": 0.01, "contract_size": 100_000.0, "point_value_usd_per_lot": None},
    "METAL":  {"tick_size": 0.01,   "contract_size": 100.0, "point_value_usd_per_lot": 1.0},   # $1 per 0.01
    "INDEX":  {"tick_size": 1.0,    "contract_size": 1.0,   "point_value_usd_per_lot": 1.0},   # $1 per 1.0
    "CRYPTO": {"tick_size": 0.01,   "contract_size": 1.0,   "point_value_usd_per_lot": 0.01},  # $0.01 per 0.01
}

def _class_spec(symbol:str)->Dict[str,float]:
    typ=instrument_type_for(symbol)
    if typ=="FOREX":
        is_jpy=(symbol or "").upper().endswith("JPY")
        return {
            "tick_size": CLASS_DEFAULTS["FOREX"]["tick_size_jpy"] if is_jpy else CLASS_DEFAULTS["FOREX"]["tick_size"],
            "contract_size": CLASS_DEFAULTS["FOREX"]["contract_size"],
            "point_value_usd_per_lot": None
        }
    d=CLASS_DEFAULTS[typ]
    return {"tick_size": d["tick_size"], "contract_size": d["contract_size"], "point_value_usd_per_lot": d["point_value_usd_per_lot"]}
# ================================================================================

# ---------- Sessions file ----------
def _yes(x:str)->bool: return (x or "").strip().upper().startswith("Y")
def load_session_file(path="session_pairs.txt")->List[Tuple[str,str,str,Dict[int,bool]]]:
    out=[]
    if not os.path.exists(path):
        L(f"[ERR] session file '{path}' not found"); return out
    # --- NEW: clear TYPE overrides each load
    TYPE_OVERRIDE.clear()
    with open(path,"r",newline="") as f:
        reader=csv.DictReader(f)
        has_type = "TYPE" in (reader.fieldnames or [])
        for rec in reader:
            sess=(rec.get("SESSION") or "").strip().upper()
            pair=(rec.get("PAIR") or "").strip().upper()
            tp  =(rec.get("TP") or "TP3").strip().upper()
            if sess in ("NEWYORK","NEW_YORK"): sess="NY"
            if sess not in ("TOKYO","LONDON","NY"): continue
            if tp not in ("TP1","TP2","TP3"): continue

            # --- NEW: read TYPE override (without changing return tuple)
            if has_type:
                t=(rec.get("TYPE") or "").strip().upper()
                if t in ("FOREX","METAL","INDEX","CRYPTO"):
                    TYPE_OVERRIDE[pair]=t

            allowed={
                0:_yes(rec.get("MON")), 1:_yes(rec.get("TUE")),
                2:_yes(rec.get("WED")), 3:_yes(rec.get("THU")),
                4:_yes(rec.get("FRI")), 5:False, 6:False
            }
            out.append((sess,pair,tp,allowed))
    return out

# ---------- DB ----------
def get_pg_conn():
    dsn=f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASS} sslmode={PG_SSLMODE}"
    L(f"DB connect {PG_USER}@{PG_HOST}:{PG_PORT}/{PG_DB} ...")
    conn=psycopg2.connect(dsn); conn.autocommit=False
    return conn

def ensure_live_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {live_tbl()} (
            pair TEXT NOT NULL,
            trade_date DATE NOT NULL,
            session TEXT,
            -- H1 Range (00:00-01:00 UTC)
            range_1h_at TIMESTAMPTZ,
            high_1h DOUBLE PRECISION,
            low_1h DOUBLE PRECISION,

            -- Direction
            break_at TIMESTAMPTZ,
            side TEXT,

            -- Pullback & trailing
            pullback_at TIMESTAMPTZ,
            sl_anchor_since_pull DOUBLE PRECISION,

            -- Live order model
            entry DOUBLE PRECISION,
            sl DOUBLE PRECISION,
            tp DOUBLE PRECISION,
            tp_level TEXT,

            status TEXT,
            opened_at TIMESTAMPTZ,
            closed_at TIMESTAMPTZ,

            last_proc_15m_at TIMESTAMPTZ,

            -- MT5
            risk_pct DOUBLE PRECISION,
            lots DOUBLE PRECISION,
            mt5_order_id BIGINT,
            mt5_request_id BIGINT,
            mt5_order_type TEXT,
            order_status TEXT,

            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (pair, trade_date)
        );
        """); conn.commit()
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_live_session_status ON {live_tbl()}(session,status);"); conn.commit()

def upsert_base_row(conn, pair:str, d:date, session:str, tp_level:str):
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {live_tbl()} WHERE pair=%s AND trade_date=%s",(pair,d))
        ex=cur.fetchone() is not None
    if not ex:
        with conn.cursor() as cur:
            cur.execute(f"""INSERT INTO {live_tbl()}(pair,trade_date,session,tp_level,updated_at)
                            VALUES(%s,%s,%s,%s,NOW())
                            ON CONFLICT (pair,trade_date) DO NOTHING""",(pair,d,session,tp_level)); conn.commit()
    else:
        with conn.cursor() as cur:
            cur.execute(f"""UPDATE {live_tbl()} SET session=%s,tp_level=%s,updated_at=NOW()
                            WHERE pair=%s AND trade_date=%s""",(session,tp_level,pair,d)); conn.commit()

def fetch_core(conn, pair:str, d:date)->Optional[Dict[str,Any]]:
    with conn.cursor() as cur:
        cur.execute(f"""
        SELECT session,range_1h_at,high_1h,low_1h,break_at,side,
               pullback_at,sl_anchor_since_pull,
               entry,sl,tp,tp_level,status,
               opened_at,closed_at,
               mt5_order_id,order_status,lots,risk_pct,last_proc_15m_at
        FROM {live_tbl()} WHERE pair=%s AND trade_date=%s
        """,(pair,d))
        r=cur.fetchone()
    if not r: return None
    (session,range_at,hh,ll,break_at,side,
     pull_at,sl_anchor,
     entry,sl,tp,tp_level,status,
     opened_at,closed_at,
     order_id,order_status,lots,risk_pct,last_done)=r
    return dict(session=session,range_1h_at=range_at,high_1h=hh,low_1h=ll,
                break_at=break_at,side=(side or None),
                pullback_at=pull_at,sl_anchor_since_pull=sl_anchor,
                entry=entry,sl=sl,tp=tp,tp_level=tp_level,status=(status or None),
                opened_at=opened_at,closed_at=closed_at,
                mt5_order_id=order_id,order_status=order_status,lots=lots,risk_pct=risk_pct,
                last_proc_15m_at=last_done)

def set_range_1h_from_midnight(conn,pair:str,d:date):
    t=candles_tbl(pair,"1h")
    start_dt = datetime(d.year,d.month,d.day,tzinfo=UTC)
    start_ms = int(start_dt.timestamp()*1000)
    with conn.cursor() as cur:
        cur.execute(f"SELECT ts,open,high,low,close FROM {t} WHERE ts=%s LIMIT 1",(start_ms,))
        row=cur.fetchone()
    if not row:
        return False
    ts,o,h,l,c=row
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET range_1h_at=%s,high_1h=%s,low_1h=%s,updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",(ms_to_dt(ts),float(h),float(l),pair,d)); conn.commit()
    return True

def mark_break(conn,pair:str,d:date,side:str,break_ms:int,init_entry:float):
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET side=%s,break_at=%s,entry=%s,updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",
                    (side, ms_to_dt(break_ms), init_entry, pair, d)); conn.commit()

def persist_entry_only(conn,pair:str,d:date,new_entry:float):
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET entry=%s,updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",(new_entry,pair,d)); conn.commit()

def mark_pullback(conn,pair:str,d:date,pull_ms:int,sl_anchor:float,entry:float,sl:float,tp:float):
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET pullback_at=%s,sl_anchor_since_pull=%s,
                        entry=%s,sl=%s,tp=%s,status='READY',updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",
                    (ms_to_dt(pull_ms),sl_anchor,entry,sl,tp,pair,d)); conn.commit()

def update_ready_order_values(conn,pair:str,d:date,entry:float,sl:float,tp:float, sl_anchor:Optional[float]=None):
    with conn.cursor() as cur:
        if sl_anchor is None:
            cur.execute(f"""UPDATE {live_tbl()} SET entry=%s,sl=%s,tp=%s,updated_at=NOW()
                            WHERE pair=%s AND trade_date=%s""",(entry,sl,tp,pair,d))
        else:
            cur.execute(f"""UPDATE {live_tbl()} SET entry=%s,sl=%s,tp=%s,sl_anchor_since_pull=%s,updated_at=NOW()
                            WHERE pair=%s AND trade_date=%s""",(entry,sl,tp,sl_anchor,pair,d))
        conn.commit()

def mark_triggered(conn,pair:str,d:date,ts_ms:int):
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET status='TRIGGERED',opened_at=%s,updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",(ms_to_dt(ts_ms),pair,d)); conn.commit()

def mark_outcome(conn,pair:str,d:date,closed_ms:int,outcome:str):
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET status=%s,closed_at=%s,updated_at=%s
                        WHERE pair=%s AND trade_date=%s""",(outcome,ms_to_dt(closed_ms),now_iso(),pair,d)); conn.commit()

def bump_last_processed(conn,pair:str,d:date,ts_ms:int):
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET last_proc_15m_at=%s,updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",(ms_to_dt(ts_ms),pair,d)); conn.commit()

def reset_state_on_flip(conn,pair:str,d:date):
    cancel_pending_mt5_order_if_any(conn,pair,d)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()} SET
                pullback_at=NULL,
                sl_anchor_since_pull=NULL,
                sl=NULL, tp=NULL,
                status=NULL, opened_at=NULL, closed_at=NULL,
                order_status=NULL, mt5_order_id=NULL, mt5_request_id=NULL, mt5_order_type=NULL,
                updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """,(pair,d)); conn.commit()

# ---------- Candles ----------
def latest_15m_open_ts_for_pair(conn,pair:str)->Optional[int]:
    t=candles_tbl(pair,"15m")
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT MAX(ts) FROM {t}")
            row=cur.fetchone(); return int(row[0]) if row and row[0] is not None else None
    except Exception: return None

def read_15m_in(conn,pair:str,start_ms:int,end_ms:int)->List[Dict[str,Any]]:
    t=candles_tbl(pair,"15m")
    with conn.cursor() as cur:
        cur.execute(f"""SELECT ts,open,high,low,close FROM {t}
                        WHERE ts >= %s AND ts < %s ORDER BY ts ASC""",(start_ms,end_ms))
        rows=cur.fetchall()
    return [{"ts":int(ts),"open":float(o),"high":float(h),"low":float(l),"close":float(c)} for ts,o,h,l,c in rows]

# ---------- TP ----------
def compute_tp(entry:float, sl:float, side:str, tp_level:str)->float:
    k = 1 if tp_level.upper()=="TP1" else 2 if tp_level.upper()=="TP2" else 3
    r = abs(entry - sl)
    return round(entry + k*r, 10) if side.upper()=="LONG" else round(entry - k*r, 10)

# ---------- MT5 ----------
def mt5_initialize()->bool:
    if not (ENABLE_TRADING and MT5_ENABLED): return False
    ok=mt5.initialize(MT5_TERMINAL_PATH) if MT5_TERMINAL_PATH else mt5.initialize()
    if not ok: L(f"[MT5] init failed: {mt5.last_error()}"); return False
    if MT5_LOGIN and MT5_PASSWORD:
        if not mt5.login(int(MT5_LOGIN),MT5_PASSWORD,MT5_SERVER):
            L(f"[MT5] login failed: {mt5.last_error()}"); return False
    return True

def mt5_preflight(symbol:str)->bool:
    ti=mt5.terminal_info(); ai=mt5.account_info(); si=mt5.symbol_info(symbol)
    if not (ti and ai and si): L(f"[MT5] preflight missing info {mt5.last_error()}"); return False
    if not getattr(ti,"trade_allowed",True) or not getattr(ti,"trade_expert",True):
        L("[MT5] AutoTrading disabled."); return False
    if not getattr(ai,"trade_allowed",True):
        L("[MT5] Trading not allowed for account."); return False
    if not si.visible and not mt5.symbol_select(symbol,True):
        L(f"[MT5] cannot select {symbol}."); return False
    return True

def _usd_per_base(symbol:str)->Optional[float]:
    def _mid(sym)->Optional[float]:
        s=mt5.symbol_info(sym); 
        if not s: return None
        t=mt5.symbol_info_tick(sym); 
        if not t: return None
        b=float(getattr(t,"bid",0.0) or 0.0); a=float(getattr(t,"ask",0.0) or 0.0)
        if b>0 and a>0: return (b+a)/2.0
        last=float(getattr(t,"last",0.0) or 0.0); return last if last>0 else None
    base=symbol[:3].upper(); 
    d=_mid("USD"+base); 
    if d and d>0: return d
    inv=_mid(base+"USD"); 
    if inv and inv>0: return 1.0/inv
    return None

def _notional_usd_per_lot(symbol:str)->Optional[float]:
    si=mt5.symbol_info(symbol)
    if not si: return None
    contract=float(si.trade_contract_size or 100000.0)
    usd_per_base=_usd_per_base(symbol) or 1.0
    return contract*usd_per_base

def _risk_usd_per_lot(symbol:str, entry:float, sl:float)->Optional[float]:
    try:
        order_type = mt5.ORDER_TYPE_BUY if sl < entry else mt5.ORDER_TYPE_SELL
        p = mt5.order_calc_profit(order_type, symbol, 1.0, entry, sl)
        if p is None: return None
        return abs(float(p))
    except Exception: return None

def round_volume(vol:float,symbol:str)->float:
    si=mt5.symbol_info(symbol)
    step=float(si.volume_step); vmin=float(si.volume_min); vmax=float(si.volume_max)
    steps=int(vol/step); v=max(vmin, min(steps*step, vmax))
    return round(v, 3)

# ---- NEW helpers for FX conversion used by fallback ----
def _ccy_mid(symbol: str) -> Optional[float]:
    s = mt5.symbol_info(symbol)
    t = mt5.symbol_info_tick(symbol) if s else None
    if not (s and t): return None
    b = float(getattr(t, "bid", 0.0) or 0.0)
    a = float(getattr(t, "ask", 0.0) or 0.0)
    if b > 0 and a > 0: return (b + a) / 2.0
    last = float(getattr(t, "last", 0.0) or 0.0)
    return last if last > 0 else None

def _fx_rate(ccy_from: str, ccy_to: str) -> Optional[float]:
    # returns how many units of ccy_to for 1 unit of ccy_from
    ccy_from = (ccy_from or "USD").upper()
    ccy_to = (ccy_to or "USD").upper()
    if ccy_from == ccy_to: return 1.0
    direct = ccy_to + ccy_from   # e.g., USDCHF to convert CHF->USD
    r = _ccy_mid(direct)
    if r and r > 0: return r
    inv = ccy_from + ccy_to      # e.g., CHFUSD
    r = _ccy_mid(inv)
    if r and r > 0: return 1.0 / r
    return None
# ---- end NEW helpers ----

# ------------------- MODIFIED: TYPE-aware fallback lot sizing -------------------
def calc_lots(symbol:str, risk_frac:float, entry:float, sl:float)->Optional[float]:
    ai=mt5.account_info()
    if not ai: return None
    equity=float(ai.equity)
    capital = equity if USE_EQUITY_FOR_RISK else float(ai.margin_free)

    # 1) Preferred path: exact risk via MT5
    risk_per_lot=_risk_usd_per_lot(symbol,entry,sl)

    # 2) Fallback path: TYPE-aware, class defaults + account currency conversion
    if not risk_per_lot or risk_per_lot<=0:
        acct_ccy = str(getattr(ai, "currency", "USD") or "USD").upper()
        typ = instrument_type_for(symbol)
        spec = _class_spec(symbol)
        tick = spec["tick_size"]
        contract = spec["contract_size"]
        pv_usd = spec["point_value_usd_per_lot"]  # None for FOREX

        dist_points = abs(entry - sl) / max(tick, 1e-12)
        if dist_points <= 0:
            return None

        if typ == "FOREX":
            # pip value per lot in QUOTE ccy = contract * tick
            base = symbol[:3].upper()
            quote = symbol[3:6].upper()
            pip_val_quote = contract * tick

            # convert QUOTE -> account currency
            if quote != acct_ccy:
                rate = _fx_rate(quote, acct_ccy)  # 1 QUOTE => ? acct_ccy
                if not rate or rate <= 0:
                    return None
                pip_val_acct = pip_val_quote * rate
            else:
                pip_val_acct = pip_val_quote

            risk_per_lot = pip_val_acct * dist_points

        else:
            # METAL / INDEX / CRYPTO
            # class default point value is in USD per 'tick' (tick_size unit)
            if pv_usd is None:
                # should not happen for non-FX, but keep a safe fallback
                pv_usd = contract * tick

            # convert USD -> account currency if needed
            if acct_ccy != "USD":
                rate = _fx_rate("USD", acct_ccy)  # 1 USD => ? acct_ccy
                if not rate or rate <= 0:
                    return None
                pv_acct = pv_usd * rate
            else:
                pv_acct = pv_usd

            risk_per_lot = pv_acct * dist_points

    if not risk_per_lot or risk_per_lot <= 0:
        return None

    lots_risk = max(0.0, capital*risk_frac)/risk_per_lot

    notion_per_lot=_notional_usd_per_lot(symbol) or 0.0
    lots_lev = (equity*MAX_LEVERAGE)/notion_per_lot if notion_per_lot>0 else lots_risk

    lots = min(lots_risk, lots_lev)
    return round_volume(max(0.0,lots), symbol)
# -------------------------------------------------------------------------------

def ensure_stop_type_and_distances(symbol:str, side:str, entry:float, sl:float, tp:float)->Tuple[float,float,float,str]:
    si=mt5.symbol_info(symbol); tick=mt5.symbol_info_tick(symbol)
    p=si.point; min_pts=(si.trade_stops_level or 0)+BROKER_MIN_DIST_BUF_PTS
    ask=tick.ask; bid=tick.bid
    if side.upper()=="LONG":
        order_type="BUY_STOP"
        min_entry=ask+(min_pts*p)
        if entry<min_entry: entry=round(min_entry,si.digits)
        if min_pts>0:
            if (entry-sl)/p<min_pts: sl=round(entry-(min_pts*p),si.digits)
            if (tp-entry)/p<min_pts: tp=round(entry+(min_pts*p),si.digits)
    else:
        order_type="SELL_STOP"
        min_entry=bid-(min_pts*p)
        if entry>min_entry: entry=round(min_entry,si.digits)
        if min_pts>0:
            if (sl-entry)/p<min_pts: sl=round(entry+(min_pts*p),si.digits)
            if (entry-tp)/p<min_pts: tp=round(entry-(min_pts*p),si.digits)
    return (round(entry,si.digits),round(sl,si.digits),round(tp,si.digits),order_type)

# ---- NEW: simple per-loop throttle state ----
_SENDS_THIS_LOOP = 0
def _throttle_before_send(kind:str, symbol:str):
    """Sleep randomly (10–30s by default) if this loop already sent something."""
    global _SENDS_THIS_LOOP
    if _SENDS_THIS_LOOP > 0:
        d = random.uniform(ORDER_THROTTLE_MIN_S, ORDER_THROTTLE_MAX_S)
        L(f"[MT5] Throttle ({kind} {symbol}): sleeping {d:.1f}s")
        time.sleep(d)

def _count_send():
    global _SENDS_THIS_LOOP
    _SENDS_THIS_LOOP += 1
# ---- end NEW ----

def place_stop_order(symbol:str, side:str, entry:float, sl:float, tp:float)->Tuple[bool,Optional[int],Optional[int],str,Optional[float]]:
    if not mt5_preflight(symbol): return (False,None,None,"preflight",None)
    entry,sl,tp,otype=ensure_stop_type_and_distances(symbol,side,entry,sl,tp)
    lots=calc_lots(symbol,RISK_PERCENT,entry,sl)
    if not lots or lots<=0: return (False,None,None,"nolots",None)
    req={
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": float(lots),
        "type": mt5.ORDER_TYPE_BUY_STOP if otype=="BUY_STOP" else mt5.ORDER_TYPE_SELL_STOP,
        "price": entry, "sl": sl, "tp": tp,
        "deviation": ALLOWED_DEV_POINTS, "comment": COMMENT_TAG,
        "type_time": mt5.ORDER_TIME_GTC
    }

    # NEW: throttle before placing a new order
    _throttle_before_send("PLACE", symbol)
    res=mt5.order_send(req)
    _count_send()  # NEW: count this send

    if res is None or res.retcode!=mt5.TRADE_RETCODE_DONE:
        L(f"[MT5] order_send failed: {None if res is None else res.retcode} / {None if res is None else res.comment}")
        return (False,None,None,"send_fail" if res else "send_none",None)
    L(f"[MT5] PLACED {otype} {symbol} lots={lots} entry={entry} sl={sl} tp={tp} order={res.order}")
    return (True,int(res.order),int(res.request_id),otype,float(lots))

def modify_stop_order(order_id:int, new_price:Optional[float], new_sl:Optional[float], new_tp:Optional[float])->bool:
    os_ = mt5.orders_get(ticket=order_id)
    if not os_: return False
    o=os_[0]; si=mt5.symbol_info(o.symbol)
    req={"action": mt5.TRADE_ACTION_MODIFY, "order": order_id, "symbol": o.symbol,
         "price": o.price_open if new_price is None else round(new_price,si.digits),
         "sl": o.sl if new_sl is None else round(new_sl,si.digits),
         "tp": o.tp if new_tp is None else round(new_tp,si.digits),
         "comment": COMMENT_TAG}

    # NEW: throttle before modify
    _throttle_before_send("MODIFY", o.symbol)
    r=mt5.order_send(req)
    _count_send()  # NEW: count this send

    ok=(r is not None and r.retcode==mt5.TRADE_RETCODE_DONE)
    if ok: L(f"[MT5] MODIFIED order={order_id} price={req['price']} SL={req['sl']} TP={req['tp']}")
    return ok

def cancel_pending_mt5_order_if_any(conn,pair:str,d:date):
    row=fetch_core(conn,pair,d)
    if not row or not row.get("mt5_order_id"): return
    oid=int(row["mt5_order_id"])
    os_=mt5.orders_get(ticket=oid)
    if os_:
        o=os_[0]
        req={"action":mt5.TRADE_ACTION_REMOVE,"order":oid,"symbol":o.symbol,"comment":COMMENT_TAG}
        # NOTE: cancellations are NOT throttled on purpose
        r=mt5.order_send(req)
        if r is None or r.retcode!=mt5.TRADE_RETCODE_DONE:
            L(f"[MT5] cancel failed {None if r is None else r.retcode}")
        else:
            L(f"[MT5] CANCELLED pending order={oid}")
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET order_status='CANCELLED',
                        mt5_order_id=NULL, mt5_request_id=NULL, mt5_order_type=NULL, updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",(pair,d)); conn.commit()

def replace_pending_order_with(conn, pair:str, side:str, entry:float, sl:float, tp:float, today:date):
    # cancel (not throttled), then place (throttled inside place_stop_order)
    cancel_pending_mt5_order_if_any(conn, pair, today)
    ok, oid, reqid, otype, lots = place_stop_order(pair, side, entry, sl, tp)
    with conn.cursor() as cur:
        cur.execute(f"""UPDATE {live_tbl()} SET risk_pct=%s,lots=%s,
                        mt5_order_id=%s, mt5_request_id=%s, mt5_order_type=%s,
                        order_status=%s, updated_at=NOW()
                        WHERE pair=%s AND trade_date=%s""",
                    (RISK_PERCENT, lots, oid, reqid, otype,
                     ("PLACED" if ok else "REJECTED"), pair, today))
        conn.commit()
    return ok

# ---------- Outcome ----------
def infer_result_from_history(order_id:int, symbol:str, tp:Optional[float], sl:Optional[float], lookback_days:int=7)->Tuple[Optional[str],Optional[int]]:
    try:
        now=datetime.now(tz=UTC); t_from=now - timedelta(days=lookback_days)
        deals=mt5.history_deals_get(position=order_id) or mt5.history_deals_get(t_from,now)
        if not deals: return (None,None)
        DEAL_ENTRY_OUT=getattr(mt5,"DEAL_ENTRY_OUT",1)
        DEAL_REASON_TP=getattr(mt5,"DEAL_REASON_TP",6)
        DEAL_REASON_SL=getattr(mt5,"DEAL_REASON_SL",7)
        out=None
        for d in deals:
            if getattr(d,"symbol","")!=symbol: continue
            if getattr(d,"entry",None)==DEAL_ENTRY_OUT:
                if out is None or getattr(out,"time_msc",0)>getattr(out,"time_msc",0):
                    out=d
        if out is None: return (None,None)
        reason=getattr(out,"reason",None); price=float(getattr(out,"price",0.0) or 0.0)
        if reason==DEAL_REASON_TP: result="WIN"
        elif reason==DEAL_REASON_SL: result="LOSS"
        else:
            result=None; tol=1e-4
            if tp is not None and abs(price-float(tp))<=tol: result="WIN"
            elif sl is not None and abs(price-float(sl))<=tol: result="LOSS"
        t_msc=getattr(out,"time_msc",None) or getattr(out,"time",None)
        closed_ms=int(t_msc) if t_msc and t_msc>10_000_000_000 else int(t_msc)*1000 if t_msc else None
        if closed_ms is not None:
            closed_ms -= MT5_SERVER_TZ_OFFSET_HOURS*3600*1000
        return (result, closed_ms)
    except Exception:
        traceback.print_exc(); return (None,None)

# ---------- MT5 reconcile (off-bars) ----------
def reconcile_with_mt5(conn, pair:str, today:date):
    if not (ENABLE_TRADING and MT5_ENABLED and mt5_initialize()):
            return
    row = fetch_core(conn, pair, today)
    if not row:
        return
    status = (row["status"] or "").upper() if row["status"] else None

    if is_terminal(status):
        return

    oid    = row.get("mt5_order_id")

    pos = None
    try:
        plist = mt5.positions_get(symbol=pair) or []
        if len(plist) == 1:
            pos = plist[0]
        elif len(plist) > 1:
            pos = sorted(plist, key=lambda p: getattr(p, "time", 0), reverse=True)[0]
    except Exception:
        pos = None

    if pos and status != "TRIGGERED":
        t = getattr(pos, "time_msc", None) or getattr(pos, "time", None)
        opened_ms = int(t) if (t and int(t)>10_000_000_000) else (int(t)*1000 if t else int(datetime.now(tz=UTC).timestamp()*1000))
        mark_triggered(conn, pair, today, opened_ms)
        try:
            with conn.cursor() as cur:
                cur.execute(f"""UPDATE {live_tbl()} SET order_status='FILLED', updated_at=NOW()
                                WHERE pair=%s AND trade_date=%s""", (pair, today)); conn.commit()
        except Exception:
            pass
        return

    if status == "TRIGGERED" and (pos is None):
        tp=row.get("tp"); sl=row.get("sl")
        res,closed_ms = infer_result_from_history(int(oid) if oid else 0, pair, tp, sl, lookback_days=7)
        if res in ("WIN","LOSS"):
            mark_outcome(conn, pair, today, closed_ms or int(datetime.now(tz=UTC).timestamp()*1000), res)
            return
        return

    if (status == "READY") and (row.get("entry") is not None):
        pending=None
        if oid:
            try:
                pp=mt5.orders_get(ticket=int(oid)); pending=pp[0] if pp else None
            except Exception:
                pending=None
        if (oid is not None) and (pending is None) and (pos is None):
            with conn.cursor() as cur:
                cur.execute(f"""UPDATE {live_tbl()} SET order_status='CANCELLED', updated_at=NOW()
                                WHERE pair=%s AND trade_date=%s""",(pair,today)); conn.commit()

            # >>> fenêtre over → aligne status CANCELLED
            try:
                sess = (row.get("session") or "NY").upper()
                s_ms, e_ms = session_signal_window(sess, today)
                last_15m = latest_15m_open_ts_for_pair(conn, pair) or 0
                now_ms = int(datetime.now(tz=UTC).timestamp()*1000)

                window_is_over = (now_ms >= e_ms) or (last_15m + FIFTEEN_MS >= e_ms)
                if window_is_over:
                    cancel_pending_mt5_order_if_any(conn, pair, today)
                    mark_outcome(conn, pair, today, e_ms, "CANCELLED")
            except Exception:
                traceback.print_exc()

# ---------- Core ----------
def compute_tp(entry:float, sl:float, side:str, tp_level:str)->float:
    k = 1 if tp_level.upper()=="TP1" else 2 if tp_level.upper()=="TP2" else 3
    r = abs(entry - sl)
    return round(entry + k*r, 10) if side.upper()=="LONG" else round(entry - k*r, 10)

def process_pair(conn, session:str, pair:str, tp_level:str, today:date):
    upsert_base_row(conn, pair, today, session, tp_level)
    row = fetch_core(conn, pair, today)

    if is_terminal((row["status"] or "").upper() if row and row["status"] else None):
        return

    # --- NEW: if another day still has an open TRIGGERED trade for this pair, cancel today's line
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT 1
            FROM {live_tbl()}
            WHERE pair=%s
              AND trade_date <> %s
              AND status = 'TRIGGERED'
              AND closed_at IS NULL
            LIMIT 1
        """, (pair, today))
        _has_open_other_day = cur.fetchone() is not None
    if _has_open_other_day:
        with conn.cursor() as cur:
            cur.execute(f"""
                UPDATE {live_tbl()}
                SET status='CANCELLED',
                    closed_at=NOW(),
                    updated_at=NOW()
                WHERE pair=%s AND trade_date=%s
            """, (pair, today))
            conn.commit()
        return

    s_ms, e_ms = session_signal_window(session, today)

    if not row["range_1h_at"]:
        ok = set_range_1h_from_midnight(conn, pair, today)
        if not ok:
            L(f"{pair}: waiting 00:00 H1 available")
            return
        row = fetch_core(conn, pair, today)
        if is_terminal((row["status"] or "").upper() if row and row["status"] else None):
            return

    high_1h = float(row["high_1h"]); low_1h = float(row["low_1h"])

    last_15m_open = latest_15m_open_ts_for_pair(conn, pair)
    if last_15m_open is None:
        L(f"{pair}: no 15m in DB"); return
    end_excl = min(e_ms, last_15m_open + FIFTEEN_MS)

    last_done_ms = int((row["last_proc_15m_at"].timestamp()*1000) if row["last_proc_15m_at"] else (s_ms-1))
    bars = read_15m_in(conn, pair, s_ms, end_excl)
    bars = [b for b in bars if int(b["ts"]) > last_done_ms]
    if not bars:
        return

    def antagonistic(o, c, s):
        return (c < o) if s == "LONG" else (c > o)

    def update_base_entry_sl(entry_v: float, sl_v: float):
        with conn.cursor() as cur:
            cur.execute(f"""UPDATE {live_tbl()} SET entry=%s, sl=%s, updated_at=NOW()
                            WHERE pair=%s AND trade_date=%s""",
                        (round_price(pair, entry_v), round_price(pair, sl_v), pair, today))
            conn.commit()

    def should_update_sl(side:str, cur_sl:Optional[float], bar_low:float, bar_high:float)->Optional[float]:
        if cur_sl is None:
            return bar_low if side=="LONG" else bar_high
        if side == "LONG":
            return bar_low if bar_low < cur_sl else None
        else:
            return bar_high if bar_high > cur_sl else None

    def sync_ready(entry_v: float, sl_new: float):
        entry_r = round_price(pair, entry_v)
        sl_r = round_price(pair, sl_new)
        cur = fetch_core(conn, pair, today)
        side_loc = (cur["side"] or "").upper()
        tp_r = round_price(pair, compute_tp(entry_r, sl_r, side_loc, cur["tp_level"]))
        update_ready_order_values(conn, pair, today, entry_r, sl_r, tp_r)

        if ENABLE_TRADING and MT5_ENABLED and mt5_initialize():
            cur2 = fetch_core(conn, pair, today)
            oid = cur2.get("mt5_order_id") if cur2 else None
            prev_lots = float(cur2.get("lots") or 0.0)
            new_lots = calc_lots(pair, RISK_PERCENT, float(entry_r), float(sl_r))

            if new_lots is None or new_lots <= 0:
                if oid:
                    modify_stop_order(int(oid), entry_r, sl_r, tp_r)
                return

            if not oid:
                replace_pending_order_with(conn, pair, side_loc, float(entry_r), float(sl_r), float(tp_r), today)
                return

            si = mt5.symbol_info(pair); step = float(si.volume_step) if si else 0.01
            same_volume = abs(new_lots - prev_lots) < (step/2.0)
            if same_volume:
                modify_stop_order(int(oid), entry_r, sl_r, tp_r)
            else:
                replace_pending_order_with(conn, pair, side_loc, float(entry_r), float(sl_r), float(tp_r), today)

    row = fetch_core(conn, pair, today)
    side = (row["side"] or "").upper() if row["side"] else None
    status = (row["status"] or "").upper() if row["status"] else None
    break_at = row["break_at"]
    entry_base = float(row["entry"]) if row["entry"] is not None else None
    sl_base    = float(row["sl"])    if row["sl"]    is not None else None

    # 1) detect strict break (not READY yet)
    if not side or not break_at:
        for b in bars:
            ts = int(b["ts"]); o, h, l, c = b["open"], b["high"], b["low"], b["close"]
            if c > high_1h:
                side = "LONG"; entry_base = h; sl_base = l
                mark_break(conn, pair, today, side, ts, round_price(pair, entry_base))
                update_base_entry_sl(entry_base, sl_base)
                L(f"{pair}: BREAK LONG @ {iso_utc(ts)} base entry={fmt_price(pair,entry_base)} sl={fmt_price(pair,sl_base)}")
            elif c < low_1h:
                side = "SHORT"; entry_base = l; sl_base = h
                mark_break(conn, pair, today, side, ts, round_price(pair, entry_base))
                update_base_entry_sl(entry_base, sl_base)
                L(f"{pair}: BREAK SHORT @ {iso_utc(ts)} base entry={fmt_price(pair,entry_base)} sl={fmt_price(pair,sl_base)}")
            bump_last_processed(conn, pair, today, ts)

        row = fetch_core(conn, pair, today)
        side = (row["side"] or "").upper() if row and row["side"] else side
        status = (row["status"] or "").upper() if row and row["status"] else None
        if is_terminal(status): return
        entry_base = float(row["entry"]) if row and row["entry"] is not None else entry_base
        sl_base    = float(row["sl"])    if row and row["sl"]    is not None else sl_base
        if not side: return

    # 2) after break
    for b in bars:
        ts = int(b["ts"]); o, h, l, c = b["open"], b["high"], b["low"], b["close"]

        row_cur = fetch_core(conn, pair, today)
        st = (row_cur["status"] or "").upper() if row_cur and row_cur["status"] else None
        if is_terminal(st):
            bump_last_processed(conn, pair, today, ts); continue

        cur_entry = float(row_cur["entry"]) if row_cur and row_cur["entry"] is not None else entry_base
        cur_sl    = float(row_cur["sl"])    if row_cur and row_cur["sl"]    is not None else sl_base

        # FLIP allowed pre-trigger
        if st != "TRIGGERED":
            if side == "LONG" and c < low_1h:
                L(f"{pair}: FLIP → SHORT @ {iso_utc(ts)}")
                reset_state_on_flip(conn, pair, today)
                side = "SHORT"; entry_base = l; sl_base = h
                mark_break(conn, pair, today, side, ts, round_price(pair, entry_base))
                update_base_entry_sl(entry_base, sl_base)
                bump_last_processed(conn, pair, today, ts); continue
            if side == "SHORT" and c > high_1h:
                L(f"{pair}: FLIP → LONG @ {iso_utc(ts)}")
                reset_state_on_flip(conn, pair, today)
                side = "LONG"; entry_base = h; sl_base = l
                mark_break(conn, pair, today, side, ts, round_price(pair, entry_base))
                update_base_entry_sl(entry_base, sl_base)
                bump_last_processed(conn, pair, today, ts); continue

        if st in ("TRIGGERED","WIN","LOSS","CANCELLED"):
            bump_last_processed(conn, pair, today, ts); continue

        # --- Phase A: NOT READY (tracking)
        if st is None or st == "":
            if side == "LONG":
                if entry_base is None or h > entry_base:
                    entry_base = h
                    sl_base = l
            else:
                if entry_base is None or l < entry_base:
                    entry_base = l
                    sl_base = h

            update_base_entry_sl(entry_base, sl_base)

            if (c < o) if side=="LONG" else (c > o):
                entry_frozen = round_price(pair, entry_base)
                if side == "LONG":
                    sl_start = round_price(pair, min(sl_base, l))
                else:
                    sl_start = round_price(pair, max(sl_base, h))
                tp_r = round_price(pair, compute_tp(entry_frozen, sl_start, side, row_cur["tp_level"]))

                mark_pullback(conn, pair, today, ts, float(sl_start),
                              float(entry_frozen), float(sl_start), float(tp_r))

                if ENABLE_TRADING and MT5_ENABLED and mt5_initialize():
                    ok, oid, reqid, otype, lots = place_stop_order(
                        pair, side, float(entry_frozen), float(sl_start), float(tp_r)
                    )
                    with conn.cursor() as cur:
                        cur.execute(f"""UPDATE {live_tbl()} SET risk_pct=%s,lots=%s,mt5_order_id=%s,mt5_request_id=%s,
                                        mt5_order_type=%s,order_status=%s,updated_at=NOW()
                                        WHERE pair=%s AND trade_date=%s""",
                                    (RISK_PERCENT, lots, oid, reqid, otype,
                                     ("PLACED" if ok else "REJECTED"), pair, today)); conn.commit()
                bump_last_processed(conn, pair, today, ts); continue

        # --- Phase B: READY
        if st == "READY":
            cand = (l if side=="LONG" else h)
            if (cur_sl is None) or (side=="LONG" and cand < cur_sl) or (side=="SHORT" and cand > cur_sl):
                sync_ready(cur_entry, cand)

            if ENABLE_TRADING and MT5_ENABLED and mt5_initialize():
                pos = None
                try:
                    plist = mt5.positions_get(symbol=pair) or []
                    if plist:
                        pos = sorted(plist, key=lambda p: getattr(p, "time", 0), reverse=True)[0]
                except Exception:
                    pos = None
                if pos and st != "TRIGGERED":
                    mark_triggered(conn, pair, today, ts)

        row_now = fetch_core(conn, pair, today)
        st_now = (row_now["status"] or "").upper() if row_now and row_now["status"] else None
        if ENABLE_TRADING and MT5_ENABLED and st_now == "TRIGGERED":
            oid = row_now.get("mt5_order_id"); tp_val = row_now.get("tp"); sl_val = row_now.get("sl")
            poslist = mt5.positions_get(symbol=pair)
            if not poslist:
                res, closed_ms = infer_result_from_history(int(oid) if oid else 0, pair, tp_val, sl_val, lookback_days=7)
                if res in ("WIN","LOSS"):
                    mark_outcome(conn, pair, today, closed_ms or ts, res)
                    return

        bump_last_processed(conn, pair, today, ts)

    # End of window: cancel if nothing triggered
    row_end = fetch_core(conn, pair, today)
    st_end = (row_end["status"] or "").upper() if row_end and row_end["status"] else None

    if (latest_15m_open_ts_for_pair(conn, pair) or 0) + FIFTEEN_MS >= e_ms:
        if st_end in (None, "", "READY"):
            if ENABLE_TRADING and MT5_ENABLED and mt5_initialize():
                cancel_pending_mt5_order_if_any(conn, pair, today)
            mark_outcome(conn, pair, today, e_ms, "CANCELLED")
        return

# ---------- Main loop ----------
def main():
    try:
        with get_pg_conn() as conn:
            ensure_live_table(conn)
            if ENABLE_TRADING and MT5_ENABLED: mt5_initialize()
            while True:
                try:
                    global _SENDS_THIS_LOOP
                    _SENDS_THIS_LOOP = 0

                    tuples = load_session_file()
                    if not tuples:
                        L("[ERR] no valid rows in session file")
                        time.sleep(POLL_SECONDS); continue

                    now = datetime.now(tz=UTC); today = now.date()
                    now_ms = int(now.timestamp()*1000)

                    for sess, pair, tp, allowed in tuples:
                        if not allowed.get(today.weekday(), False):
                            continue

                        for delta in range(0, 3):  # today, today-1, today-2
                            d = today - timedelta(days=delta)
                            try:
                                reconcile_with_mt5(conn, pair, d)
                            except Exception as e:
                                L(f"[MT5-RECONCILE-ERR] {pair} d={d}: {e}")
                                traceback.print_exc()

                        s_ms, e_ms = session_signal_window(sess, today)
                        if not (s_ms <= now_ms < e_ms + FIFTEEN_MS):
                            continue
                        try:
                            process_pair(conn, sess, pair, tp, today)
                        except Exception as e:
                            L(f"[PROC-ERR] {pair}: {e}")
                            traceback.print_exc()

                except Exception as e:
                    L(f"[LOOP-ERR] {e}")
                    traceback.print_exc()
                time.sleep(POLL_SECONDS)
    except Exception as e:
        L(f"[FATAL] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
