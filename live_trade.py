#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
LIVE Breakout + Pullback — session-aware, single-row per (pair, date) with heavy logging (no MT5 orders)

Aligné backtest:
- Range H1 = PREMIERE heure de la session (OPEN dans [session_start, session_end))
- On valide au CLOSE, on stocke les OPEN ts.
- 15m: uniquement des bougies FERMÉES (fenêtre [start_open, end_open_excl) bornée par le dernier 15m fermé).
- Timestamps lisibles: colonnes *_at (TIMESTAMPTZ UTC) en plus des *_ts (BIGINT ms).
- Arrondis prix: 3 décimales pour JPY/XAU, 5 pour le reste. Arrondi appliqué à la BD ET aux logs.
"""

import os, sys, traceback
from datetime import datetime, timedelta, timezone, date
from typing import List, Tuple, Optional, Dict, Any

import psycopg2
from psycopg2 import extensions as pg_ext
from dotenv import load_dotenv

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

MIN_READY_PIPS = float(os.getenv("MIN_READY_PIPS", "6"))  # seuil pips pour valider READY

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

def session_window_utc_ms(session: str, d: date) -> Tuple[int,int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    s = session.upper()
    if s == "TOKYO":
        return int((base + timedelta(hours=0)).timestamp()*1000), int((base + timedelta(hours=9)).timestamp()*1000)
    if s == "LONDON":
        return int((base + timedelta(hours=7)).timestamp()*1000), int((base + timedelta(hours=16)).timestamp()*1000)
    # NY
    return int((base + timedelta(hours=12)).timestamp()*1000), int((base + timedelta(hours=21)).timestamp()*1000)

def latest_closed_15m_open_ms(now_ms: int) -> int:
    # OPEN de la DERNIÈRE 15m FERMÉE
    flo = (now_ms // FIFTEEN_MS) * FIFTEEN_MS
    return flo - FIFTEEN_MS

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
    conn.set_isolation_level(pg_ext.ISOLATION_LEVEL_AUTOCOMMIT)
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
            result TEXT,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            PRIMARY KEY (pair, trade_date)
        );
        """)
        cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_live_trades_session_status
        ON public.live_trades(session, status);
        """)
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
    # Détecte la session via l'heure du DERNIER CLOSE 15m.
    best_ts_open = None
    best_pair = None
    for p in all_pairs:
        ts = latest_15m_open_ts_for_pair(conn, p)
        if ts is None: 
            continue
        if best_ts_open is None or ts > best_ts_open:
            best_ts_open = ts
            best_pair = p
    if best_ts_open is None:
        L("[ERR] No 15m data found for any pair in session file.")
        return None
    close_ms = best_ts_open + FIFTEEN_MS
    dt = datetime.fromtimestamp(close_ms/1000, tz=UTC)
    # Close-based routing
    h = dt.hour
    if 12 <= h < 21: sess = "NY"
    elif 7 <= h < 16: sess = "LONDON"
    elif 0 <= h < 9: sess = "TOKYO"
    else: sess = "TOKYO"
    L(f"SESSION: detected current session={sess} using pair={best_pair} last15m_close={iso_utc(close_ms)}")
    return (sess, close_ms)

def read_first_h1_OPEN_in_session(conn, pair: str, session: str, d: date) -> Optional[Dict[str,Any]]:
    """
    PREMIERE H1 de la session: ts (OPEN) ∈ [s, e).
    """
    s, e = session_window_utc_ms(session, d)
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
    # Log arrondi
    L(f"1H-FIRST(OPEN): {pair} ts(OPEN)={iso_utc(int(ts))} H={fmt_price(pair,h)} L={fmt_price(pair,l)}")
    return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}

def read_15m_in_open_window(conn, pair: str, start_open_ms: int, end_open_ms_excl: int) -> List[Dict[str,Any]]:
    """15m fermées: OPEN ∈ [start_open_ms, end_open_ms_excl)."""
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
    """15m fermées STRICTEMENT après start_open_ms: OPEN ∈ (start_open_ms, end_open_ms_excl)."""
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

# ---------- DB row helpers ----------
def upsert_base_row(conn, pair: str, trade_day: date, session: str, tp_level: str):
    with conn.cursor() as cur:
        cur.execute(f"SELECT 1 FROM {live_tbl()} WHERE pair=%s AND trade_date=%s", (pair, trade_day))
        exists = cur.fetchone() is not None
        if not exists:
            cur.execute(f"""
                INSERT INTO {live_tbl()} (pair, trade_date, session, tp_level, updated_at)
                VALUES (%s,%s,%s,%s, NOW())
                ON CONFLICT (pair,trade_date) DO NOTHING
            """, (pair, trade_day, session, tp_level))
            L(f"ROW: INSERT base {pair} {trade_day} session={session} tp={tp_level}")
        else:
            cur.execute(f"""
                UPDATE {live_tbl()}
                SET session=%s,
                    tp_level=%s,
                    updated_at=NOW()
                WHERE pair=%s AND trade_date=%s
            """, (session, tp_level, pair, trade_day))
            L(f"ROW: UPDATE base {pair} {trade_day} session={session} tp={tp_level}")

def set_range_1h(conn, pair: str, trade_day: date, c1: Dict[str,Any]):
    open_ts = int(c1["ts"])
    # arrondis H/L pour écriture et logs
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
        """, (open_ts, ms_to_dt(open_ts), h, l, pair, trade_day))
    L(f"ROW: set 1H range {pair} {trade_day} OPEN={iso_utc(open_ts)} H={fmt_price(pair,h)} L={fmt_price(pair,l)}")

def fetch_core(conn, pair: str, trade_day: date) -> Optional[Dict[str,Any]]:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT session, range_1h_ts, high_1h, low_1h, break_ts, side,
                   extreme_price, extreme_ts, pullback_ts, entry, sl, tp,
                   tp_level, status
            FROM {live_tbl()}
            WHERE pair=%s AND trade_date=%s
        """, (pair, trade_day))
        row = cur.fetchone()
    if not row: return None
    (session, range_ts, hh, ll, break_ts, side,
     extreme_price, extreme_ts, pullback_ts, entry, sl, tp,
     tp_level, status) = row
    return {
        "session":session, "range_1h_ts":range_ts, "high_1h":hh, "low_1h":ll,
        "break_ts":break_ts, "side":side, "extreme_price":extreme_price, "extreme_ts":extreme_ts,
        "pullback_ts":pullback_ts, "entry":entry, "sl":sl, "tp":tp,
        "tp_level":tp_level, "status":status
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
              pair, trade_day))
    L(f"BREAK: {pair} side={side} at OPEN={iso_utc(break_open_ts)} init_extreme={fmt_price(pair,ex)}")

def persist_extreme(conn, pair: str, trade_day: date, price: float, bar_open_ts: int):
    ex = round_price(pair, price)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET extreme_price=%s, extreme_ts=%s, extreme_at=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (ex, bar_open_ts, ms_to_dt(bar_open_ts), pair, trade_day))
    L(f"EXTREME: {pair} updated extreme={fmt_price(pair,ex)} @ OPEN={iso_utc(bar_open_ts)}")

def mark_ready(conn, pair: str, trade_day: date, pullback_open_ts: int, entry: float, sl: float, side: str, tp_level: str):
    # arrondir entry/sl avant TP, puis TP et écriture
    entry_r = round_price(pair, entry)
    sl_r    = round_price(pair, sl)
    tp_raw  = compute_tp(entry_r, sl_r, side, tp_level)
    tp_r    = round_price(pair, tp_raw)
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET pullback_ts=%s, pullback_at=%s,
                entry=%s, sl=%s, tp=%s,
                status='READY', updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (pullback_open_ts, ms_to_dt(pullback_open_ts),
              entry_r, sl_r, tp_r, pair, trade_day))
    L(f"READY: {pair} pullback OPEN={iso_utc(pullback_open_ts)} entry={fmt_price(pair,entry_r)} sl={fmt_price(pair,sl_r)} tp={fmt_price(pair,tp_r)}")

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
        """, (sl_r, tp_r, pair, trade_day))
    L(f"SL/TP-UPDATE: {pair} ts OPEN={iso_utc(bar_open_ts)} new_sl={fmt_price(pair,sl_r)} new_tp={fmt_price(pair,tp_r)}")

def mark_triggered(conn, pair: str, trade_day: date, trigger_open_ts: int):
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE {live_tbl()}
            SET status='TRIGGERED', opened_ts=%s, opened_at=%s, updated_at=NOW()
            WHERE pair=%s AND trade_date=%s
        """, (trigger_open_ts, ms_to_dt(trigger_open_ts), pair, trade_day))
    L(f"TRIGGERED: {pair} at OPEN={iso_utc(trigger_open_ts)}")

# ---------- TP ----------
def compute_tp(entry: float, sl: float, side: str, tp_level: str) -> float:
    k = 1 if (tp_level or "").upper()=="TP1" else 2 if (tp_level or "").upper()=="TP2" else 3
    r = abs(entry - sl)
    return entry + k*r if (side or "").upper()=="LONG" else entry - k*r

# ---------- Core logic ----------
def process_pair(conn, session: str, pair: str, tp_level: str, today: date, now_ms: int):
    # base row
    upsert_base_row(conn, pair, today, session, tp_level)
    row = fetch_core(conn, pair, today)
    if not row:
        L(f"[WARN] {pair}: row missing after upsert"); return

    s_ms, e_ms = session_window_utc_ms(session, today)

    # range H1 = PREMIERE H1 de la session (OPEN dans [s,e))
    if not row["range_1h_ts"]:
        c1 = read_first_h1_OPEN_in_session(conn, pair, session, today)
        if not c1:
            L(f"{pair}: waiting first H1 OPEN in session.")
            return
        set_range_1h(conn, pair, today, c1)
        row = fetch_core(conn, pair, today)

    high_1h, low_1h = row["high_1h"], row["low_1h"]

    # Limite supérieure = dernier 15m FERME
    last_closed_15m_open = latest_closed_15m_open_ms(now_ms)
    end_excl = min(e_ms, last_closed_15m_open + FIFTEEN_MS)

    # 1) Break (si pas encore trouvé) : première 15m CLOSE hors range, on stocke l'OPEN
    side = (row["side"] or "").upper() if row["side"] else None
    break_open_ts = row["break_ts"]
    if not side or not break_open_ts:
        c15 = read_15m_in_open_window(conn, pair, s_ms, end_excl)
        brk_side = None; brk_idx = None
        for i, b in enumerate(c15):
            if b["close"] > high_1h: brk_side, brk_idx = "LONG", i; break
            if b["close"] < low_1h:  brk_side, brk_idx = "SHORT", i; break
        if brk_side is None:
            L(f"{pair}: no break yet.")
            return
        b0 = c15[brk_idx]
        break_open_ts = int(b0["ts"])
        init_extreme  = b0["high"] if brk_side == "LONG" else b0["low"]
        update_break_and_extreme(conn, pair, today, brk_side, break_open_ts, init_extreme)
        row = fetch_core(conn, pair, today)
        side = brk_side

    # 2) Post-break: suivi de l'extrême + pullback antagoniste
    extreme      = row["extreme_price"]
    pullback_ts  = row["pullback_ts"]
    status       = (row["status"] or "").upper() if row["status"] else None
    eff_tp_level = (row["tp_level"] or tp_level)

    # STRICTEMENT après la bougie de break
    c15 = read_15m_after_open(conn, pair, break_open_ts, end_excl)
    if not c15:
        L(f"{pair}: no 15m after break yet.")
        return

    ready_seen    = status == "READY" or (pullback_ts is not None and row["entry"] is not None and row["sl"] is not None)
    current_entry = row["entry"]
    current_sl    = row["sl"]

    for b in c15:
        o, h, l, c = b["open"], b["high"], b["low"], b["close"]
        ts_open = int(b["ts"])

        if not ready_seen:
            # maj extrême directionnelle
            if side == "LONG" and (extreme is None or h > extreme):
                extreme = h
                persist_extreme(conn, pair, today, extreme, ts_open)
            if side == "SHORT" and (extreme is None or l < extreme):
                extreme = l
                persist_extreme(conn, pair, today, extreme, ts_open)

            # antagoniste ?
            is_antagonistic = (c < o) if side == "LONG" else (c > o)
            if is_antagonistic:
                # wick au-delà de l'extrême ?
                if side == "LONG" and h > extreme:
                    extreme = h; persist_extreme(conn, pair, today, extreme, ts_open)
                elif side == "SHORT" and l < extreme:
                    extreme = l; persist_extreme(conn, pair, today, extreme, ts_open)

                entry = float(extreme)
                sl    = float(l) if side == "LONG" else float(h)

                # --- MIN READY PIPS check (nouveau) ---
                pipsz = pip_size_for(pair)  # 0.01 pour JPY, 0.0001 sinon
                dist_pips = abs(entry - sl) / pipsz
                if dist_pips < MIN_READY_PIPS:
                    L(f"READY-SKIP: {pair} OPEN={iso_utc(ts_open)} dist={dist_pips:.1f} pips < {MIN_READY_PIPS} → ignore")
                else:
                    mark_ready(conn, pair, today, ts_open, entry, sl, side, eff_tp_level)
                    ready_seen    = True
                    current_entry = round_price(pair, entry)
                    current_sl    = round_price(pair, sl)
                    L(f"{pair}: READY @ OPEN={iso_utc(ts_open)} entry={fmt_price(pair,current_entry)} sl={fmt_price(pair,current_sl)}")
                    continue  # pas de trigger sur la même barre

        # READY non déclenché → suiveur de SL tant que pas TRIGGERED
        if ready_seen and (status != "TRIGGERED"):
            triggered = (h >= current_entry) if side == "LONG" else (l <= current_entry)
            if triggered:
                mark_triggered(conn, pair, today, ts_open)  # OPEN de la barre de trigger
                status = "TRIGGERED"
                L(f"{pair}: TRIGGERED @ OPEN={iso_utc(ts_open)}")
                break
            else:
                new_sl = float(l if side == "LONG" else h)
                if current_sl is None or round_price(pair, new_sl) != current_sl:
                    update_sl_and_tp(conn, pair, today, new_sl, current_entry, side, eff_tp_level, ts_open)
                    current_sl = round_price(pair, new_sl)


# ---------- Main ----------
def main():
    try:
        with get_pg_conn() as conn:
            ensure_live_trades(conn)

            tuples = load_session_file()
            if not tuples:
                L("[ERR] No valid entries in session_pairs.txt"); return

            all_pairs = [p for _,p,_ in tuples]
            sess_detect = detect_current_session(conn, all_pairs)
            if not sess_detect:
                return
            current_session, last_close_ms = sess_detect
            today = datetime.fromtimestamp(last_close_ms/1000, tz=UTC).date()
            s_ms, e_ms = session_window_utc_ms(current_session, today)
            L(f"RUN: current_session={current_session}, window=[{iso_utc(s_ms)}..{iso_utc(e_ms)})")

            session_pairs = [(s,p,tp) for (s,p,tp) in tuples if (s == current_session)]
            L(f"RUN: {len(session_pairs)} pairs match current session: {', '.join(p for _,p,_ in session_pairs) if session_pairs else '(none)'}")
            if not session_pairs:
                L("RUN: nothing to process for this session."); return

            # Upsert base rows
            for _, pair, tp in session_pairs:
                upsert_base_row(conn, pair, today, current_session, tp)

            # Single pass (cron-friendly)
            now_ms = int(datetime.now(tz=UTC).timestamp()*1000)
            for _, pair, tp in session_pairs:
                try:
                    process_pair(conn, current_session, pair, tp, today, now_ms)
                except Exception as e:
                    L(f"[PROC-ERR] {pair}: {e}")
                    traceback.print_exc()

            L("DONE.")
    except Exception as e:
        L(f"[FATAL] {e}")
        traceback.print_exc()

if __name__ == "__main__":
    main()
