#!/usr/bin/env python3
# aoi_batch.py
"""
TOKYO-only AOI:
- At the first 1H close of the Tokyo session (01:00 UTC), record:
  * support   = LOW of that 1H candle
  * resistance= HIGH of that 1H candle
  Stored as two AOI rows with zero-width zones: [low, low] and [high, high].

Session (UTC, CLOSE-based):
- TOKYO window: 00:00–09:00
- First 1H close in session: 01:00 (candle 00:00–01:00)
- Pre-open is not used anymore.
- Cleanup at 09:00 (DELETE pair AOIs).

Notes:
- No trend logic, no intra-session refresh.
- Rounding: prices HALF_UP (5 decimals non-JPY, 3 decimals JPY).
- Table: public.aoi_zones (created if absent; no ALTER).
"""

import os, re, argparse
from datetime import datetime, timezone
from typing import Optional
from decimal import Decimal, ROUND_HALF_UP

import psycopg2, psycopg2.extras
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# ------------------------- CONFIG (TOKYO only) -------------------------
TOKYO_FIRST_H1_CLOSE_HOUR, TOKYO_FIRST_H1_CLOSE_MIN = 1, 0   # 01:00 CLOSE
TOKYO_CLOSE_HOUR, TOKYO_CLOSE_MIN = 9, 0                     # 09:00 CLOSE

FIFTEEN_MIN_MS = 15 * 60 * 1000
H1_MS = 60 * 60 * 1000

# ------------------------- PG connection -------------------------
def load_pg_conninfo_from_env() -> dict:
    load_dotenv()
    return {
        "host": os.getenv("PG_HOST", "localhost"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "dbname": os.getenv("PG_DB", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "postgres"),
        "sslmode": os.getenv("PG_SSLMODE", "disable"),
    }

def pg_conn():
    return psycopg2.connect(**load_pg_conninfo_from_env())

# ------------------------- Utils -------------------------
def sanitize_name(s): return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')
def table_name(exchange, pair, tf): return f"candles_{sanitize_name(exchange)}_{sanitize_name(pair)}_{sanitize_name(tf)}"
def pip_size(pair): return 0.01 if pair.upper().endswith("JPY") else 0.0001
def scale_for_pair(pair): return 3 if pair.upper().endswith("JPY") else 5
def qround_price(x, scale): return float(Decimal(str(x)).quantize(Decimal("1").scaleb(-scale), rounding=ROUND_HALF_UP))
def fmt_price_scaled(x, scale): return f"{x:.{scale}f}".rstrip("0").rstrip(".")
def iso_utc_from_ms(ms): return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat(timespec="seconds")

# ------------------------- DB (fresh schema; no ALTER) -------------------------
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS public.aoi_zones (
                pair               TEXT NOT NULL,
                tf                 TEXT NOT NULL,
                low                DOUBLE PRECISION NOT NULL,
                high               DOUBLE PRECISION NOT NULL,
                touches            INTEGER NOT NULL,
                height_pips        DOUBLE PRECISION NOT NULL,
                type               TEXT NOT NULL,               -- 'support' or 'resistance'
                session_cutoff_ts  BIGINT NOT NULL,            -- UTC ms of 15m CLOSE cutoff (here: the 01:00 close ms)
                updated_at         TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aoi_zones_pair_tf ON public.aoi_zones(pair, tf);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_aoi_zones_pair_cutoff ON public.aoi_zones(pair, session_cutoff_ts);")
    conn.commit()

def delete_pair(conn, pair):
    with conn.cursor() as cur:
        cur.execute("DELETE FROM public.aoi_zones WHERE pair=%s", (pair,))
    conn.commit()

def insert_rows(conn, rows):
    if not rows: return
    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO public.aoi_zones
                (pair, tf, low, high, touches, height_pips, type, session_cutoff_ts, updated_at)
            VALUES %s
        """, rows)
    conn.commit()

# ------------------------- 15m helpers -------------------------
def latest_15m_open_ts(conn, exchange, pair) -> Optional[int]:
    tbl = table_name(exchange, pair, "15m")
    with conn.cursor() as cur:
        cur.execute(f"SELECT MAX(ts) FROM {tbl}")
        row = cur.fetchone()
        return int(row[0]) if row and row[0] is not None else None

def is_exact_15m_close_time(ts_open_ms: int, hour_utc: int, minute_utc: int) -> bool:
    """
    Compare against the 15m CLOSE time -> OPEN + 15 minutes.
    """
    dt = datetime.fromtimestamp((ts_open_ms + FIFTEEN_MIN_MS)/1000, tz=timezone.utc)
    return dt.hour == hour_utc and dt.minute == minute_utc

# ------------------------- Candle read -------------------------
def read_h1_candle_at_close(conn, exchange, pair, close_ts_ms: int):
    """
    Read the 1H candle that CLOSES at close_ts_ms.
    Since H1 table uses OPEN timestamps, the OPEN is close_ts_ms - 1h.
    Returns (open_ts, open, high, low, close) or None.
    """
    open_ts = close_ts_ms - H1_MS
    tbl = table_name(exchange, pair, "1h")
    with conn.cursor() as cur:
        cur.execute(f"SELECT ts, open, high, low, close FROM {tbl} WHERE ts = %s", (open_ts,))
        row = cur.fetchone()
        if not row: return None
        ts, o, h, l, c = row
        return int(ts), float(o), float(h), float(l), float(c)

# ------------------------- Main -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs", default=None, help='Override list, e.g. "EURUSD GBPUSD" or "EURUSD,GBPUSD"')
    ap.add_argument("--exchange", default="mt5")
    args = ap.parse_args()

    with pg_conn() as conn:
        ensure_table(conn)
        pairs = ( [t.strip().upper() for t in re.split(r"[,\s]+", args.pairs.strip()) if t.strip()]
                  if args.pairs else
                  [r[0] for r in conn.cursor().execute("SELECT DISTINCT pair FROM pairs_structure WHERE timeframe='1h'") or []] )

        # Fallback discover_pairs if cursor().execute above doesn't return list
        if not pairs:
            with conn.cursor() as cur:
                cur.execute("SELECT DISTINCT pair FROM pairs_structure WHERE timeframe IN ('1d','4h','1h') ORDER BY pair ASC")
                pairs = [r[0] for r in cur.fetchall()]

        for pair in pairs:
            last_15m_open_ts = latest_15m_open_ts(conn, args.exchange, pair)
            if last_15m_open_ts is None:
                print(f"[{pair}] no 15m candles → skip")
                continue

            last_15m_close_ts = last_15m_open_ts + FIFTEEN_MIN_MS

            # --- CLEANUP at TOKYO close (09:00 CLOSE) ---
            if is_exact_15m_close_time(last_15m_open_ts, TOKYO_CLOSE_HOUR, TOKYO_CLOSE_MIN):
                print(f"[{pair}] TOKYO_CLOSE @ {iso_utc_from_ms(last_15m_close_ts)} -> DELETE ONLY")
                delete_pair(conn, pair)
                continue

            # --- RECORD levels at first H1 close in session (01:00 CLOSE) ---
            if is_exact_15m_close_time(last_15m_open_ts, TOKYO_FIRST_H1_CLOSE_HOUR, TOKYO_FIRST_H1_CLOSE_MIN):
                h1 = read_h1_candle_at_close(conn, args.exchange, pair, last_15m_close_ts)
                if not h1:
                    print(f"[{pair}] 01:00 CLOSE but missing 1H candle -> skip")
                    continue

                _, _o, h, l, _c = h1
                scale = scale_for_pair(pair)
                pipsz = pip_size(pair)

                sup = qround_price(l, scale)
                res = qround_price(h, scale)

                rows = [
                    (pair, "1h", sup, sup, 1, 0.0, "support",    last_15m_close_ts, datetime.now(timezone.utc)),
                    (pair, "1h", res, res, 1, 0.0, "resistance", last_15m_close_ts, datetime.now(timezone.utc)),
                ]

                # Replace pair-level AOIs for the new session’s first H1 close
                delete_pair(conn, pair)
                insert_rows(conn, rows)
                print(f"[OK] {pair}: TOKYO 01:00 CLOSE levels saved — support={fmt_price_scaled(sup,scale)} resistance={fmt_price_scaled(res,scale)}")
                continue

            # Else: not a relevant trigger → do nothing
            print(f"[{pair}] last 15m CLOSE {iso_utc_from_ms(last_15m_close_ts)} -> no-op")

if __name__=="__main__":
    main()
