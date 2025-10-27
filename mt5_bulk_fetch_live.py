#!/usr/bin/env python3
# mt5_bulk_fetch_to_pg.py
"""
MT5 -> Postgres ingestor (with EU DST handling) + EMA-50 on insert:
- Inserts all *closed* candles <= cap (if provided) and stores their OPEN UTC in ts/ts_utc.
- Per-candle conversion: ts_utc_ms = bar_open_server_ms - offset_ms(bar_time), where offset = +2 (winter) or +3 (summer) per EU DST (EET/EEST).
- GAPLESS: OPEN forced to previous CLOSE if available.
- Iterates pairs.txt (CSV with 'pair' column) and timeframes.txt (list).
- --to optional: ISO8601 (…Z) or epoch ms. Without --to: cap = last closed bar "now" on server.
- Adds/maintains ema_50 (NUMERIC) computed during ingestion (EMA(50) with SMA seed).
- NEW: --pairs allows overriding pairs.txt with a space- or comma-separated list.
"""

import os, re, csv, sys, time
import argparse
from datetime import datetime, timezone, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Optional, Tuple

import MetaTrader5 as mt5
from dotenv import load_dotenv
from sqlalchemy import create_engine, MetaData, Table, Column, BigInteger, String, Float, select, desc, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.types import Numeric

# -------------------
# Constants
# -------------------
UTC = timezone.utc

MIN_SERVER_OFFSET_HOURS = 2   # EET (winter)
MAX_SERVER_OFFSET_HOURS = 3   # EEST (summer)

BATCH_BARS = 10000

EMA_LEN = 50
EMA_ALPHA = Decimal("2") / Decimal(str(EMA_LEN + 1))  # 2/(N+1)

TF_MS = {
    "1m": 60_000, "3m": 180_000, "5m": 300_000, "15m": 900_000, "30m": 1_800_000,
    "1h": 3_600_000, "2h": 7_200_000, "4h": 14_400_000, "6h": 21_600_000,
    "8h": 28_800_000, "12h": 43_200_000, "1d": 86_400_000, "1w": 604_800_000,
}
TF_MT5 = {
    "1m": mt5.TIMEFRAME_M1, "3m": mt5.TIMEFRAME_M3, "5m": mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15, "30m": mt5.TIMEFRAME_M30, "1h": mt5.TIMEFRAME_H1,
    "2h": mt5.TIMEFRAME_H2, "4h": mt5.TIMEFRAME_H4, "6h": mt5.TIMEFRAME_H6,
    "8h": mt5.TIMEFRAME_H8, "12h": mt5.TIMEFRAME_H12,
    "1d": mt5.TIMEFRAME_D1, "1w": mt5.TIMEFRAME_W1,
}

# -------------------
# Generic helpers
# -------------------
def price_scale(base: str, quote: str) -> int:
    return 3 if ("JPY" in (base, quote)) else 5

def qround(x: float | Decimal, scale: int) -> Decimal:
    x = Decimal(str(x))
    return x.quantize(Decimal("1").scaleb(-scale), rounding=ROUND_HALF_UP)

def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat(timespec="seconds")

def sanitize_name(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

def get_pg_engine():
    load_dotenv()
    host = os.getenv("PG_HOST", "127.0.0.1")
    port = os.getenv("PG_PORT", "5432")
    db   = os.getenv("PG_DB", "postgres")
    user = os.getenv("PG_USER", "postgres")
    pwd  = os.getenv("PG_PASSWORD", "postgres")
    ssl  = os.getenv("PG_SSLMODE", "disable")
    uri = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}"
    engine = create_engine(uri, pool_pre_ping=True, future=True)
    return engine

def ensure_ema_column(engine, table_name: str, scale: int):
    """Ensure ema_50 column exists."""
    sql = text(f'ALTER TABLE IF EXISTS "{table_name}" ADD COLUMN IF NOT EXISTS ema_50 NUMERIC(20,{scale});')
    with engine.begin() as conn:
        conn.execute(sql)

def get_last_row(engine, table) -> Tuple[Optional[int], Optional[Decimal], Optional[Decimal]]:
    """Return (last_ts, last_close, last_ema_50)."""
    with engine.connect() as c:
        row = c.execute(
            select(table.c.ts, table.c.close, table.c.ema_50).order_by(desc(table.c.ts)).limit(1)
        ).fetchone()
        if not row:
            return None, None, None
        last_ts = int(row.ts)
        last_close = Decimal(row.close) if row.close is not None else None
        last_ema = Decimal(row.ema_50) if getattr(row, "ema_50", None) is not None else None
        return last_ts, last_close, last_ema

def fetch_recent_closes(engine, table, n: int, before_ts: Optional[int]) -> List[Decimal]:
    """Fetch up to n closes BEFORE before_ts (exclusive) ordered ASC."""
    with engine.connect() as c:
        if before_ts is None:
            q = select(table.c.close).order_by(table.c.ts.asc()).limit(n)
        else:
            q = select(table.c.close).where(table.c.ts < before_ts).order_by(table.c.ts.desc()).limit(n)
        rows = c.execute(q).fetchall()
    closes = [Decimal(r.close) for r in rows]
    if before_ts is not None:
        closes.reverse()
    return closes

def parse_pairs(path: str) -> List[str]:
    pairs = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            p = r.get("pair") or r.get("PAIR") or r.get("Pair")
            if p:
                pairs.append(p.strip().upper())
    return pairs

def parse_pairs_cli(s: str) -> List[str]:
    """
    Parse --pairs argument: accepts space- or comma-separated list.
    Examples:
      --pairs "EURUSD GBPUSD USDJPY"
      --pairs "EURUSD,GBPUSD,USDJPY"
    """
    toks = [t.strip().upper() for t in re.split(r"[,\s]+", s.strip()) if t.strip()]
    # Basic sanity filter: 6+ letters (handles metals too, e.g., XAUUSD)
    return [t for t in toks if len(t) >= 6]

def parse_timeframes(path: str) -> List[str]:
    tfs = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            tf = line.strip().lower()
            if tf and not tf.startswith("#"):
                tfs.append(tf)
    return tfs

def lookback_years(tf: str) -> int:
    if tf == "1w":
        return 10
    if tf == "1d":
        return 4
    return 2

def compute_initial_start_utc(tf: str) -> datetime:
    years = lookback_years(tf)
    days = int(years * 365.25) + 2
    return datetime.now(UTC) - timedelta(days=days)

# -------------------
# EU DST helpers
# -------------------
def last_sunday(year: int, month: int) -> datetime:
    if month == 12:
        next_month = datetime(year + 1, 1, 1, tzinfo=UTC)
    else:
        next_month = datetime(year, month + 1, 1, tzinfo=UTC)
    d = next_month - timedelta(days=1)
    while d.weekday() != 6:
        d -= timedelta(days=1)
    return d.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=UTC)

def eu_dst_start_utc(year: int) -> datetime:
    return last_sunday(year, 3) + timedelta(hours=1)

def eu_dst_end_utc(year: int) -> datetime:
    return last_sunday(year, 10) + timedelta(hours=1)

def server_offset_hours_for_server_ms(bar_open_server_ms: int, mode: str, fixed_hours: int) -> int:
    """Offset à utiliser quand on convertit un timestamp SERVEUR -> UTC pour chaque bougie."""
    if mode == "fixed":
        return fixed_hours
    prelim_utc_ms = bar_open_server_ms - (2 * 3600 * 1000)  # hypothèse min pour déterminer l'année/jour
    dt_utc = datetime.fromtimestamp(prelim_utc_ms / 1000, tz=UTC)
    start = eu_dst_start_utc(dt_utc.year)
    end   = eu_dst_end_utc(dt_utc.year)
    return 3 if (start <= dt_utc < end) else 2

def server_offset_hours_for_utc_ms(utc_ms: int, mode: str, fixed_hours: int) -> int:
    """Offset à utiliser quand on convertit un timestamp UTC -> SERVEUR (pour --to ou pour 'now')."""
    if mode == "fixed":
        return fixed_hours
    dt_utc = datetime.fromtimestamp(utc_ms / 1000, tz=UTC)
    start = eu_dst_start_utc(dt_utc.year)
    end   = eu_dst_end_utc(dt_utc.year)
    return 3 if (start <= dt_utc < end) else 2

# -------------------
# --to parsing
# -------------------
def parse_to_ms(to_arg: Optional[str]) -> Optional[int]:
    if not to_arg:
        return None
    s = to_arg.strip()
    if re.fullmatch(r"\d{10,13}", s):
        val = int(s)
        if len(s) == 10:
            val *= 1000
        return val
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    return int(datetime.fromisoformat(s).timestamp() * 1000)

# -------------------
# MT5
# -------------------
def mt5_now_server_ms_frozen(tz_mode: str, fixed_hours: int) -> int:
    """
    Fige 'now' côté SERVEUR une seule fois pour tout le run.
    On part de l'horloge UTC locale et on ajoute l'offset serveur (EU DST ou fixed).
    """
    utc_now_ms = int(time.time() * 1000)
    off_h = server_offset_hours_for_utc_ms(utc_now_ms, tz_mode, fixed_hours)
    return utc_now_ms + off_h * 3600 * 1000

def copy_rates_chunk(symbol: str, tf: str, start_naive, end_naive):
    data = mt5.copy_rates_range(symbol, TF_MT5[tf], start_naive, end_naive)
    if data is None or len(data) == 0:
        return []
    return list(data)

# -------------------
# Core
# -------------------
def fetch_and_store(engine, pair: str, tf: str, user_to_ms: Optional[int], tz_mode: str, fixed_hours: int, now_server_ms_fixed: int):
    base, quote = pair[:3], pair[3:]
    sym = None
    for cand in [pair, pair + ".a", pair + ".i", pair + ".pro", pair + ".ecn"]:
        if mt5.symbol_info(cand):
            mt5.symbol_select(cand, True)
            sym = cand
            break
    if not sym:
        print(f"[WARN] {pair}: not visible in MT5.")
        return

    meta = MetaData()
    scale = price_scale(base, quote)
    table_name = f"candles_mt5_{sanitize_name(pair)}_{sanitize_name(tf)}"
    table = Table(
        table_name, meta,
        Column("ts", BigInteger, primary_key=True),  # OPEN UTC (ms)
        Column("ts_utc", String),                    # ISO-UTC
        Column("open", Numeric(20, scale)),
        Column("high", Numeric(20, scale)),
        Column("low",  Numeric(20, scale)),
        Column("close", Numeric(20, scale)),
        Column("volume", Float),
        Column("exchange", String(16)),
        Column("symbol", String(32)),
        Column("base", String(8)),
        Column("quote", String(8)),
        Column("timeframe", String(8)),
        Column("ema_50", Numeric(20, scale)),        # EMA on insert
    )
    meta.create_all(engine, checkfirst=True)
    ensure_ema_column(engine, table_name, scale)

    # resume state
    last_ts, last_close, last_ema = get_last_row(engine, table)

    # Start point: UTC -> server-naive with minimal offset so we don't miss early bars
    if last_ts:
        start_utc = datetime.fromtimestamp((last_ts + 1) / 1000, tz=UTC)
        resume_mode = True
    else:
        start_utc = compute_initial_start_utc(tf)
        resume_mode = False

    start_server_naive = (start_utc + timedelta(hours=MIN_SERVER_OFFSET_HOURS)).replace(tzinfo=None)

    # ---- CAP: dernier OPEN de bougie FERMÉE à min(NOW_SERVEUR, --to_SERVEUR) ----
    tf_ms = TF_MS[tf]

    # cap "now" gelé
    cap_server_ms_now = (now_server_ms_fixed // tf_ms) * tf_ms - tf_ms

    # cap "--to" si fourni (UTC -> serveur)
    if user_to_ms is not None:
        off_h_to = server_offset_hours_for_utc_ms(user_to_ms, tz_mode, fixed_hours)
        to_server_ms = user_to_ms + off_h_to * 3600 * 1000
        cap_server_ms_to = (to_server_ms // tf_ms) * tf_ms - tf_ms
        last_closed_open_ms_cap = min(cap_server_ms_now, cap_server_ms_to)
    else:
        last_closed_open_ms_cap = cap_server_ms_now

    # Fenêtre MT5 de fin: cap +1s (pour ne pas rater la bougie exactement au cap)
    end_server_naive = datetime.fromtimestamp(last_closed_open_ms_cap / 1000).replace(tzinfo=None) + timedelta(seconds=1)

    if start_server_naive >= end_server_naive:
        cap_info = f", cap_to={iso_utc(user_to_ms)}" if user_to_ms is not None else ""
        print(f"[INFO] {pair} {tf}: no new closed bars. (start={start_utc.isoformat()} resume={resume_mode}{cap_info})")
        return

    # --- EMA state ---
    ema_prev: Optional[Decimal] = None
    seed_buffer: List[Decimal] = []

    if last_ema is not None:
        ema_prev = Decimal(last_ema)
    else:
        pre_closes = fetch_recent_closes(engine, table, EMA_LEN - 1, before_ts=(last_ts + 1) if last_ts else None)
        seed_buffer.extend(pre_closes)

    inserted_total = 0
    current_start = start_server_naive

    with engine.begin() as conn:
        prev_close = last_close  # Decimal or None
        while current_start < end_server_naive:
            window_end = current_start + timedelta(milliseconds=tf_ms * BATCH_BARS)
            if window_end > end_server_naive:
                window_end = end_server_naive

            rates = copy_rates_chunk(sym, tf, current_start, window_end)
            if rates:
                rows = []
                for r in rates:
                    bar_open_server_ms = int(r["time"]) * 1000

                    # Ne pas dépasser le dernier OPEN fermé au cap effectif
                    if bar_open_server_ms > last_closed_open_ms_cap:
                        continue

                    # Offset dynamique par bougie (serveur -> UTC)
                    off_h = server_offset_hours_for_server_ms(bar_open_server_ms, tz_mode, fixed_hours)
                    ts_utc_ms = bar_open_server_ms - off_h * 3600 * 1000

                    # (Sécurité additionnelle côté utilisateur, mais non nécessaire avec le cap serveur)
                    if user_to_ms is not None and ts_utc_ms > user_to_ms:
                        continue

                    # Reprise
                    if last_ts and ts_utc_ms <= last_ts:
                        continue

                    # OHLC (gapless open)
                    mt5_o = qround(r["open"],  scale)
                    mt5_h = qround(r["high"],  scale)
                    mt5_l = qround(r["low"],   scale)
                    mt5_c = qround(r["close"], scale)

                    o = prev_close if prev_close is not None else mt5_o
                    c = mt5_c
                    h = max(o, c, mt5_h)
                    l = min(o, c, mt5_l)

                    v = float(r["tick_volume"])  # FX: tick volume only

                    # --- EMA(50) computation ---
                    ema_val: Optional[Decimal] = None
                    if ema_prev is not None:
                        ema_val = qround(EMA_ALPHA * c + (Decimal(1) - EMA_ALPHA) * ema_prev, scale)
                        ema_prev = ema_val
                    else:
                        seed_buffer.append(c)
                        if len(seed_buffer) == EMA_LEN:
                            sma = qround(sum(seed_buffer) / Decimal(EMA_LEN), scale)
                            ema_prev = sma
                            ema_val = sma
                        else:
                            ema_val = None  # en cours de seeding

                    rows.append({
                        "ts": ts_utc_ms,
                        "ts_utc": iso_utc(ts_utc_ms),
                        "open": o, "high": h, "low": l, "close": c, "volume": v,
                        "exchange": "mt5", "symbol": sym,
                        "base": base, "quote": quote, "timeframe": tf,
                        "ema_50": ema_val
                    })

                    prev_close = c
                    last_ts = ts_utc_ms  # avance le curseur

                if rows:
                    res = conn.execute(
                        pg_insert(table).values(rows).on_conflict_do_nothing(index_elements=["ts"])
                    )
                    inserted = res.rowcount or 0
                    inserted_total += inserted

            current_start = window_end

    cap_info = f", cap_to={iso_utc(user_to_ms)}" if user_to_ms is not None else ""
    if inserted_total == 0:
        print(f"[INFO] {pair} {tf}: no new closed bars. (start={start_utc.isoformat()} resume={resume_mode}{cap_info})")
    else:
        print(f"[DONE] {pair} {tf}: +{inserted_total} closed candles inserted into {table_name}. (start={start_utc.isoformat()} resume={resume_mode}{cap_info})")

# -------------------
# Main
# -------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--to", help="Optional cap: ISO8601 UTC like 2025-01-01T00:00:00Z or epoch ms", default=None)
    ap.add_argument("--server-tz", choices=["eu", "fixed"], default=os.getenv("SERVER_TZ_MODE", "eu"),
                    help="Server timezone mode: 'eu' (EET/EEST DST) or 'fixed' (constant offset).")
    ap.add_argument("--server-offset-hours", type=int, default=int(os.getenv("SERVER_OFFSET_HOURS", "3")),
                    help="Server offset hours if --server-tz=fixed (e.g., 2 or 3).")
    ap.add_argument("--pairs-file", default=os.getenv("PAIRS_FILE", "session_pairs.txt"),
                    help="Path to pairs file (CSV with column 'pair').")
    ap.add_argument("--timeframes-file", default=os.getenv("TIMEFRAMES_FILE", "timeframes.txt"),
                    help="Path to timeframes file (list).")
    ap.add_argument("--pairs", type=str, default=None,
                    help="Override pairs list (space- or comma-separated), e.g. \"EURUSD GBPUSD\" or \"EURUSD,GBPUSD\". If omitted, read from --pairs-file.")
    args = ap.parse_args()

    user_to_ms = parse_to_ms(args.to)

    if not mt5.initialize():
        print("[ERR] MT5 init failed", mt5.last_error())
        sys.exit(1)

    engine = get_pg_engine()
    pairs_path = args.pairs_file
    tfs_path   = args.timeframes_file

    print(f"[INFO] Using timeframes file: {tfs_path}")
    if args.pairs:
        pairs = parse_pairs_cli(args.pairs)
        print(f"[INFO] Using pairs from --pairs: {', '.join(pairs)}")
    else:
        print(f"[INFO] Using pairs file: {pairs_path}")
        pairs = parse_pairs(pairs_path)

    if not pairs:
        print("[ERR] No pairs provided or found.")
        mt5.shutdown()
        sys.exit(2)

    tfs = parse_timeframes(tfs_path)
    if not tfs:
        print("[ERR] No timeframes found.")
        mt5.shutdown()
        sys.exit(3)

    print("[INIT] Live ingestion loop (every 15s)")

    try:
        while True:
            # Recalcule le cap “now serveur” à CHAQUE itération (même logique que ton script)
            now_server_ms_fixed = mt5_now_server_ms_frozen(args.server_tz, args.server_offset_hours)
            dt_now_srv = datetime.fromtimestamp(now_server_ms_fixed/1000, tz=UTC).isoformat(timespec="seconds")
            print(f"[LOOP] Server-now≈ {dt_now_srv}  tz={args.server_tz} off={args.server_offset_hours}h", flush=True)

            for pair in pairs:
                for tf in tfs:
                    fetch_and_store(
                        engine, pair, tf,
                        user_to_ms=user_to_ms,
                        tz_mode=args.server_tz,
                        fixed_hours=args.server_offset_hours,
                        now_server_ms_fixed=now_server_ms_fixed
                    )
            time.sleep(15)
    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")
    finally:
        mt5.shutdown()
        print("[DONE] Live loop stopped.")


if __name__ == "__main__":
    main()
