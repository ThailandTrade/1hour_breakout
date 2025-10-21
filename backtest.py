#!/usr/bin/env python3
# backtest.py — manual/auto backtesting (15m grid), pair selection, skip-day/week
# Pipeline per step: fetch → structure → aoi → notify (minimal: pair + to)

import os
import sys
import json
import subprocess
import csv
import re
from datetime import datetime, timedelta, timezone, date
from typing import Optional, Tuple, List, Dict

from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext, errors as pg_errors

UTC = timezone.utc
PAIRS_SELECTED_FILE = ".pairs_selected.csv"

load_dotenv()

# ---------------- External scripts (overridable via ENV) ----------------
PYTHON = os.getenv("PYTHON_BIN", sys.executable)
SCRIPTS = {
    "fetch":     os.getenv("FETCH_SCRIPT", "mt5_bulk_fetch_to_pg.py"),
    "structure": os.getenv("STRUCT_SCRIPT", "update_pairs_structure.py"),
    "aoi":       os.getenv("AOI_SCRIPT", "aoi_batch.py"),
}
DEFAULT_PAIRS_FILE = os.getenv("PAIRS_FILE", "pairs.txt")
TIMEFRAMES_FILE   = os.getenv("TIMEFRAMES_FILE", "timeframes.txt")

DEFAULT_STEP_MIN = int(os.getenv("BT_STEP_MIN", "15"))

# ---------------- DB ENV ----------------
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

# ---------------- Session config (must mirror aoi_batch.py) ----------------
def _env_bool(key: str, default: bool) -> bool:
    v = os.getenv(key)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on")

# Defaults chosen to match the sample you shared
ENABLE_TOKYO  = _env_bool("ENABLE_TOKYO",  True)
ENABLE_LONDON = _env_bool("ENABLE_LONDON", False)
ENABLE_NY     = _env_bool("ENABLE_NY",     False)

# Pre-open CLOSE times (UTC). The 15m candle that CLOSES at these times is the trigger.
TOKYO_PREOPEN_HM  = (23, 45)  # candle OPEN 23:30
LONDON_PREOPEN_HM = ( 6, 45)  # candle OPEN 06:30
NY_PREOPEN_HM     = (11, 45)  # candle OPEN 11:30
FIFTEEN_MIN_MS    = 15 * 60 * 1000

# ---------------- Table helpers ----------------
def _sanitize_pair_for_table(pair: str) -> str:
    return re.sub(r"[^a-z0-9]", "", pair.lower())

def table_name_for(pair: str, tf_up: str) -> str:
    pair_lower = _sanitize_pair_for_table(pair)
    tf_lower   = re.sub(r"\s+", "", tf_up.lower())
    name = f"candles_mt5_{pair_lower}_{tf_lower}"
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError("Invalid table name")
    return name

# ---------------- Minimal NOTIFY (pair + to) ----------------
def pg_notify_pairs_minimal(pairs: List[str], to_ms: int):
    if not pairs:
        return
    payloads = [json.dumps({"pair": (p or "").upper(), "to": int(to_ms)}, separators=(",", ":")) for p in pairs]
    try:
        with get_pg_conn() as conn, conn.cursor() as cur:
            for pl in payloads:
                cur.execute("SELECT pg_notify('tv_events', %s)", (pl,))
                print(f"[PG][NOTIFY] tv_events {pl}", flush=True)
    except Exception as e:
        print(f"[PG][WARN] notify failed: {e.__class__.__name__}: {e}", flush=True)

# ---------------- Time / market helpers ----------------
def parse_iso_or_epoch(s: str) -> int:
    s = s.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    try:
        if s.isdigit():
            v = int(s)
            return v if len(s) >= 13 else v * 1000
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return int(dt.timestamp() * 1000)
    except Exception:
        raise SystemExit(f"[ERR] Invalid timestamp: {s}")

def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

# Market open window: Sun 22:00 UTC → Fri 22:00 UTC
def is_weekend(ts_ms: int) -> bool:
    dt = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    wd = dt.weekday()
    if wd == 4 and (dt.hour > 22 or (dt.hour == 22 and (dt.minute > 0 or dt.second > 0))):
        return True
    if wd == 5:
        return True
    if wd == 6 and dt.hour < 22:
        return True
    return False

def next_session_open(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    days_ahead = (6 - dt.weekday()) % 7
    tgt = (dt.replace(hour=22, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead))
    if tgt <= dt:
        tgt += timedelta(days=7)
    return int(tgt.timestamp() * 1000)

def next_grid(ts_ms: int, step_min: int) -> int:
    interval = step_min * 60_000
    return ((ts_ms // interval) + 1) * interval

def align_to_next_open(ts_ms: int, step_min: int) -> int:
    interval = step_min * 60_000
    t = ts_ms
    if is_weekend(t):
        t = next_session_open(t)
    if (t % interval) != 0:
        t = next_grid(t, step_min)
    if is_weekend(t):
        t = next_session_open(t)
        if (t % interval) != 0:
            t = next_grid(t, step_min)
    return t

def next_trading_day(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    boundary = dt.replace(hour=22, minute=0, second=0, microsecond=0)
    if dt >= boundary:
        boundary += timedelta(days=1)
    return int(boundary.timestamp() * 1000)

def next_trading_week(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    days_ahead = (6 - dt.weekday()) % 7
    week_boundary = (dt.replace(hour=22, minute=0, second=0, microsecond=0) + timedelta(days=days_ahead))
    if week_boundary <= dt:
        week_boundary += timedelta(days=7)
    return int(week_boundary.timestamp() * 1000)

# -------- NEW: jump to next enabled session pre-open (aligned with aoi_batch.py) --------
def _session_preopen_open_dt(day: date, hm_close: Tuple[int, int]) -> datetime:
    """Return the OPEN datetime (UTC) of the 15m candle whose CLOSE equals hm_close on 'day'."""
    h, m = hm_close
    close_dt = datetime(day.year, day.month, day.day, h, m, tzinfo=UTC)
    open_dt = close_dt - timedelta(minutes=15)
    return open_dt

def _enabled_preopen_hms() -> List[Tuple[str, Tuple[int, int]]]:
    out: List[Tuple[str, Tuple[int, int]]] = []
    if ENABLE_TOKYO:
        out.append(("TOKYO", TOKYO_PREOPEN_HM))
    if ENABLE_LONDON:
        out.append(("LONDON", LONDON_PREOPEN_HM))
    if ENABLE_NY:
        out.append(("NY", NY_PREOPEN_HM))
    return out

def next_enabled_session_preopen_open_from(ts_ms: int, step_min: int) -> int:
    """
    Find the next 15m OPEN (UTC ms) such that its CLOSE matches an enabled session pre-open
    (23:45 / 06:45 / 11:45). Weekend-safe.
    """
    if step_min != 15:
        step_min = 15  # safety: logic relies on 15m grid
    t0 = align_to_next_open(ts_ms + 1, step_min)
    dt0 = datetime.fromtimestamp(t0/1000, tz=UTC)
    start_day = dt0.date()

    for d in range(0, 14):  # plenty of headroom
        cur_day = start_day + timedelta(days=d)
        candidates: List[Tuple[int, str, Tuple[int, int]]] = []
        for name, hm in _enabled_preopen_hms():
            open_dt = _session_preopen_open_dt(cur_day, hm)
            open_ms = int(open_dt.timestamp() * 1000)
            if open_ms <= t0:
                continue
            if is_weekend(open_ms):
                continue
            candidates.append((open_ms, name, hm))
        if candidates:
            candidates.sort(key=lambda x: x[0])
            next_ms, sess_name, hm = candidates[0]
            print(f"[BT] Next enabled session pre-open: {sess_name} (close {hm[0]:02d}:{hm[1]:02d}Z) → candle OPEN {iso_utc(next_ms)}", flush=True)
            return next_ms

    print("[BT][WARN] No next session pre-open found; advancing one grid.", flush=True)
    return align_to_next_open(t0 + step_min * 60 * 1000, step_min)

# ---------------- Pair helpers ----------------
def write_pairs_selected(pairs_arg: str) -> str:
    parts = [p.strip().upper() for p in pairs_arg.replace(";", ",").split(",") if p.strip()]
    if not parts:
        raise SystemExit("[ERR] --pairs provided but no valid pair found.")
    with open(PAIRS_SELECTED_FILE, "w", encoding="utf-8") as f:
        f.write("pair\n")
        for p in parts:
            f.write(p + "\n")
    return PAIRS_SELECTED_FILE

def load_pairs_from_csv(path: str) -> List[str]:
    if not os.path.exists(path):
        return []
    pairs: List[str] = []
    # Try CSV with header "pair"
    with open(path, "r", encoding="utf-8") as f:
        try:
            reader = csv.DictReader(f)
            fields = [c.strip().lower() for c in (reader.fieldnames or [])]
            if "pair" in fields:
                for row in reader:
                    p = (row.get("pair") or "").strip().upper()
                    if p and p not in pairs:
                        pairs.append(p)
                return pairs
        except Exception:
            pass
    # Fallback: simple lines
    with open(path, "r", encoding="utf-8") as f:
        for i, raw in enumerate(f):
            line = raw.strip()
            if not line:
                continue
            if i == 0 and ("pair" in line.lower() or "type,pair" in line.lower()):
                continue
            parts = [p.strip().upper() for p in line.split(",") if p.strip()]
            p = parts[-1] if parts else ""
            if p and p not in pairs:
                pairs.append(p)
    return pairs

def make_env(pairs_file_override: Optional[str]) -> dict:
    env = os.environ.copy()
    env.setdefault("PYTHONIOENCODING", "utf-8")
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONUNBUFFERED", "1")
    env["PAIRS_FILE"] = pairs_file_override or DEFAULT_PAIRS_FILE
    env["TIMEFRAMES_FILE"] = TIMEFRAMES_FILE
    return env

# ---------------- Runner ----------------
def run(cmd, name, env: dict):
    print(f"[RUN] {name}: {' '.join(cmd)}", flush=True)
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1, env=env)
    assert p.stdout is not None
    for line in p.stdout:
        print(line, end="")
    rc = p.wait()
    print(f"[DONE] {name}: rc={rc}", flush=True)
    return rc

def step_once(to_ms: int, env: dict, only_fetch=False) -> bool:
    """
    1) fetch --to <to_ms>
    2) structure
    3) aoi
    4) notify (MINIMAL) unless only_fetch=True
    """
    if run([PYTHON, SCRIPTS["fetch"], "--to", str(to_ms)], "fetch", env) != 0:
        return False
    if only_fetch:
        return True
    if run([PYTHON, SCRIPTS["structure"]], "structure", env) != 0:
        return False
    if run([PYTHON, SCRIPTS["aoi"]], "aoi", env) != 0:
        return False
    try:
        pairs_file_used = env.get("PAIRS_FILE", DEFAULT_PAIRS_FILE)
        pairs = load_pairs_from_csv(pairs_file_used)
        if not pairs:
            print("[PG][INFO] No pairs loaded for notify; skipped.", flush=True)
            return True
        pg_notify_pairs_minimal(pairs, to_ms)
    except Exception as e:
        print(f"[PG][WARN] notify block failed (non-fatal): {e.__class__.__name__}: {e}", flush=True)
    return True

# ---------------- Latest 15m candle & AOIs ----------------
def pip_eps_for(pair: str) -> float:
    return 0.001 if pair.upper().endswith("JPY") else 0.00001

def read_latest_15m_candle(conn, pair: str) -> Optional[Dict]:
    table = table_name_for(pair, "15M")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {table}
        ORDER BY ts DESC
        LIMIT 1
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            if not row:
                return None
            ts, o, h, l, c = row
            return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
    except pg_errors.UndefinedTable:
        conn.rollback()
        return None
    except Exception:
        conn.rollback()
        return None

def read_aois(conn, pair: str) -> List[Tuple[float, float, str]]:
    sql = """
        SELECT low, high, type
        FROM public.aoi_zones
        WHERE pair = %s AND tf IN ('1d','4h','1h')
        ORDER BY high DESC, low DESC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (pair.upper(),))
            rows = cur.fetchall()
            return [(float(lo), float(hi), (typ or "").lower()) for (lo, hi, typ) in rows]
    except Exception:
        conn.rollback()
        return []

# ---------------- Trend reading (pairs_structure) ----------------
def _derive_trend_from_hhlh(hh, hl, lh, ll) -> str:
    try:
        hh = float(hh or 0); hl = float(hl or 0); lh = float(lh or 0); ll = float(ll or 0)
    except Exception:
        return "neutral"
    if hh > 0 and hl > 0 and lh == 0 and ll == 0:
        return "bullish"
    if ll > 0 and lh > 0 and hh == 0 and hl == 0:
        return "bearish"
    return "neutral"

def read_trend(conn, pair: str, tf: str) -> Optional[str]:
    q1 = "SELECT trend FROM public.pairs_structure WHERE pair=%s AND timeframe=%s LIMIT 1"
    q2 = "SELECT trend FROM public.pairs_structure WHERE pair=%s AND tf=%s LIMIT 1"
    q3 = "SELECT hh,hl,lh,ll FROM public.pairs_structure WHERE pair=%s AND timeframe=%s LIMIT 1"
    q4 = "SELECT hh,hl,lh,ll FROM public.pairs_structure WHERE pair=%s AND tf=%s LIMIT 1"
    try:
        with conn.cursor() as cur:
            try:
                cur.execute(q1, (pair.upper(), tf))
                row = cur.fetchone()
            except Exception:
                conn.rollback()
                row = None
            if row and row[0]:
                return str(row[0]).lower()

            try:
                cur.execute(q2, (pair.upper(), tf))
                row = cur.fetchone()
            except Exception:
                conn.rollback()
                row = None
            if row and row[0]:
                return str(row[0]).lower()

            try:
                cur.execute(q3, (pair.upper(), tf))
                row = cur.fetchone()
            except Exception:
                conn.rollback()
                row = None
            if row:
                return _derive_trend_from_hhlh(*row)

            try:
                cur.execute(q4, (pair.upper(), tf))
                row = cur.fetchone()
            except Exception:
                conn.rollback()
                row = None
            if row:
                return _derive_trend_from_hhlh(*row)
    except Exception:
        conn.rollback()
    return None

# ---------------- Stop criteria (AOI + global trend align) ----------------
def candle_body_bounds(o: float, c: float) -> Tuple[float, float]:
    return (min(o, c), max(o, c))

def candle_vs_zone_overlap(cndl: Dict, pair: str, zone: Tuple[float, float, str], body_only: bool = False) -> bool:
    a_lo, a_hi, _ = zone
    eps = pip_eps_for(pair)
    if body_only:
        lo, hi = candle_body_bounds(cndl["open"], cndl["close"])
        lo -= eps; hi += eps
    else:
        lo = cndl["low"]  - eps
        hi = cndl["high"] + eps
    return (hi >= a_lo and lo <= a_hi)

def price_in_body(cndl: Dict, price: float, pair: str) -> bool:
    """True si 'price' est dans le corps (open..close) inclus, avec une petite tolérance."""
    body_lo, body_hi = candle_body_bounds(float(cndl["open"]), float(cndl["close"]))
    if body_lo > body_hi:
        body_lo, body_hi = body_hi, body_lo
    eps = max(pip_eps_for(pair), 1e-9)  # tolérance bord à bord
    return (price >= body_lo - eps) and (price <= body_hi + eps)

def evaluate_stop_criteria_15m(conn, pairs: List[str], *, stop_aoi: bool) -> Optional[str]:
    """
    STOP si l'un des prix d'AOI (low ou high) est DANS le corps (open–close) de la 15m (bords inclus).
    On ignore totalement les mèches et l’alignement de tendance.
    """
    if not stop_aoi:
        return None

    for p in pairs:
        cndl = read_latest_15m_candle(conn, p)
        if not cndl:
            continue

        body_lo, body_hi = candle_body_bounds(float(cndl["open"]), float(cndl["close"]))
        if body_lo > body_hi:
            body_lo, body_hi = body_hi, body_lo

        aois = read_aois(conn, p)
        print(f"[CHK] {p} @ {iso_utc(cndl['ts'])} BODY=[{body_lo}..{body_hi}] AOIs={len(aois)}", flush=True)

        for (lo, hi, ztype) in aois:
            lo = float(lo); hi = float(hi)
            # si zone est une ligne (support/resistance), low==high → ok aussi
            hit = price_in_body(cndl, lo, p) or price_in_body(cndl, hi, p)
            print(f"[CHK]    AOI[{ztype or '?'}] low={lo} high={hi} -> {'HIT' if hit else 'no'}", flush=True)
            if hit:
                return (f"STOP AUTO — {p} 15m: AOI price in BODY @ {iso_utc(cndl['ts'])} "
                        f"(AOI {ztype or '?'} low={lo} high={hi}, body [{body_lo}..{body_hi}]).")

    return None



# ---------------- Discover starting point (15m) ----------------
def last_15m_close_ts_for_pair(conn, pair: str) -> Optional[int]:
    table = table_name_for(pair, "15M")
    sql = f"SELECT MAX(ts) FROM {table}"
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            row = cur.fetchone()
            return int(row[0]) if row and row[0] is not None else None
    except pg_errors.UndefinedTable:
        conn.rollback()
        return None
    except Exception:
        conn.rollback()
        return None

def common_last_15m_ts(conn, pairs: List[str]) -> Optional[int]:
    maxes: List[int] = []
    for p in pairs:
        ts = last_15m_close_ts_for_pair(conn, p)
        if ts is not None:
            maxes.append(ts)
    if not maxes:
        return None
    return min(maxes)

# ---------------- HOTKEY: exit AUTO with 'x' (non-blocking) ----------------
def auto_break_requested() -> bool:
    """
    Returns True if user pressed 'x' (lower/upper) while in AUTO mode.
    Windows: uses msvcrt.kbhit().
    POSIX: tries select() on stdin (works in most terminals; falls back silently if not).
    """
    try:
        if os.name == "nt":
            import msvcrt
            hit = False
            while msvcrt.kbhit():
                ch = msvcrt.getwch()
                if ch and ch.lower() == "x":
                    hit = True
                # flush any extra buffered keys so a single press doesn't trigger multiple times
            return hit
        else:
            import select
            dr, _, _ = select.select([sys.stdin], [], [], 0)
            if dr:
                data = sys.stdin.read(1)
                return bool(data and data.lower() == "x")
            return False
    except Exception:
        return False

# ---------------- CLI loop ----------------
def main():
    import argparse
    ap = argparse.ArgumentParser(description="Manual/auto backtest (15m). ENTER to advance; 'a' auto; 'n' next session; 'x' exit auto.")
    ap.add_argument("--start", help="Starting point (ISO8601Z or epoch ms). Overrides auto 15m resume.", default=None)
    ap.add_argument("--to", help="Bootstrap target (ISO8601Z or epoch ms) if no 15m data.", default=None)
    ap.add_argument("--step-min", type=int, default=DEFAULT_STEP_MIN, help="Step in minutes (default=15).")
    ap.add_argument("--only-fetch", action="store_true", help="Run fetch only (no structure/AOI, no notify).")
    ap.add_argument("--pairs", help="Comma-separated pairs (e.g., EURUSD,GBPUSD). Default: from pairs.txt.", default=None)

    stop_aoi_group = ap.add_mutually_exclusive_group()
    stop_aoi_group.add_argument("--stop-aoi", dest="stop_aoi", action="store_true", default=True, help="Stop on AOI touch + aligned 1d & 4h trends (default).")
    stop_aoi_group.add_argument("--no-stop-aoi", dest="stop_aoi", action="store_false", help="Disable stop on AOI/global-trend.")

    args = ap.parse_args()

    pairs_file_override = write_pairs_selected(args.pairs) if args.pairs else None
    pairs = load_pairs_from_csv(pairs_file_override or DEFAULT_PAIRS_FILE)
    if not pairs:
        print("[ERR] No pairs to process. Provide --pairs or a valid pairs file.")
        sys.exit(2)
    print(f"[BT] Pairs: {', '.join(pairs)}", flush=True)

    env = make_env(pairs_file_override)

    if args.start:
        to_ms = align_to_next_open(parse_iso_or_epoch(args.start), args.step_min)
        print(f"[BT] Start override via --start → {iso_utc(to_ms)}", flush=True)
    else:
        with get_pg_conn() as conn:
            last15 = common_last_15m_ts(conn, pairs)
        if last15 is not None:
            to_ms = align_to_next_open(last15 + 1, args.step_min)
            print(f"[BT] Resuming from last common 15m close → {iso_utc(to_ms)}", flush=True)
        else:
            target = args.to or str(int(datetime.now(tz=UTC).timestamp() * 1000))
            target_ms = align_to_next_open(parse_iso_or_epoch(target), args.step_min)
            print(f"[BT] No 15m data found. Bootstrapping history up to {iso_utc(target_ms)} …", flush=True)
            ok = step_once(target_ms, env, only_fetch=False)
            if not ok:
                print("[FATAL] Bootstrap failed.", flush=True)
                sys.exit(1)
            to_ms = target_ms
            print(f"[BT] Bootstrap done @ {iso_utc(to_ms)}", flush=True)

    def stop_flags_str() -> str:
        return f"AOI+GlobalTrend={'ON' if args.stop_aoi else 'OFF'}"

    print("[BT] Started. ENTER=advance | a=auto | h=+1h | d=skip day | w=skip week | n=next session | x=exit auto | aoi=toggle AOI+GlobalTrend | q=quit | +<min>=custom | goto <ISO/ms> | ?=help",
          flush=True)
    print(f"[BT] Default step: {args.step_min} min | Stops: {stop_flags_str()} | to={iso_utc(to_ms)}", flush=True)

    auto_mode = False
    auto_cap  = 10_000

    while True:
        if not auto_mode:
            print(f"\n[BT] Running at --to {iso_utc(to_ms)} ...", flush=True)
            ok = step_once(to_ms, env, only_fetch=args.only_fetch)
            if not ok:
                print("[FATAL] A step failed. Stopping.", flush=True)
                sys.exit(1)

            try:
                cmd = input(f"[BT] ENTER=next | a=auto | h=+1h | d=day | w=week | n=next | x=exit auto | aoi | q=quit | +<min> | goto <ISO/ms> | ?=help  [{stop_flags_str()}] > ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\n[BT] Stop.", flush=True)
                break

            if cmd == "" or cmd == "+":
                to_ms = align_to_next_open(to_ms + 1, args.step_min)
                continue

            c = cmd.lower()

            if c in ("q", "quit", "exit"):
                print("[BT] Bye.", flush=True)
                break

            if c in ("a", "auto"):
                auto_mode = True
                print(f"[BT] AUTO mode ON ({args.step_min}m). Stops: {stop_flags_str()} — press 'x' to return to MANUAL.", flush=True)

                # First auto iteration (immediate)
                print(f"\n[BT/AUTO] Running at --to {iso_utc(to_ms)} ...", flush=True)
                ok = step_once(to_ms, env, only_fetch=args.only_fetch)
                if not ok:
                    print("[FATAL] A step failed. Stopping.", flush=True)
                    sys.exit(1)
                # Hotkey check
                if auto_break_requested():
                    print("[BT/AUTO] Hotkey 'x' detected → back to MANUAL.", flush=True)
                    auto_mode = False
                    continue

                with get_pg_conn() as conn:
                    reason = evaluate_stop_criteria_15m(conn, pairs, stop_aoi=args.stop_aoi)
                if reason:
                    print(reason, flush=True)
                    print("[BT/AUTO] Switching back to MANUAL.", flush=True)
                    auto_mode = False
                    continue
                to_ms = align_to_next_open(to_ms + args.step_min * 60 * 1000, args.step_min)
                auto_cap -= 1
                if auto_cap <= 0:
                    print("[BT/AUTO] Safety cap hit. Switching back to MANUAL.", flush=True)
                    auto_mode = False
                continue

            if c in ("h", "+1h", "hour"):
                to_ms = align_to_next_open(to_ms + 60 * 60 * 1000, args.step_min)
                print(f"[BT] Advanced +1h -> {iso_utc(to_ms)}", flush=True)
                continue

            if c in ("d", "day", "skip"):
                day_ts = next_trading_day(to_ms)
                to_ms  = align_to_next_open(day_ts, args.step_min)
                print(f"[BT] Skipped to next trading day @ {iso_utc(to_ms)}", flush=True)
                continue

            if c in ("w", "week", "+1w"):
                week_ts = next_trading_week(to_ms)
                to_ms   = align_to_next_open(week_ts, args.step_min)
                print(f"[BT] Skipped to next trading week @ {iso_utc(to_ms)}", flush=True)
                continue

            # NEW: jump to next enabled session pre-open
            if c in ("n", "next"):
                nxt = next_enabled_session_preopen_open_from(to_ms, args.step_min)
                to_ms = align_to_next_open(nxt, args.step_min)  # already on-grid, keep invariant
                print(f"[BT] Jumped to next session pre-open OPEN @ {iso_utc(to_ms)}", flush=True)
                continue

            if c == "x":
                # If pressed in MANUAL, just a no-op hint
                print("[BT] 'x' stops AUTO mode. You are already in MANUAL.", flush=True)
                continue

            if c == "aoi":
                args.stop_aoi = not args.stop_aoi
                print(f"[BT] Toggle AOI+GlobalTrend stop → {'ON' if args.stop_aoi else 'OFF'}", flush=True)
                continue

            if c.startswith("+"):
                try:
                    mins = int(cmd[1:])
                    to_ms = align_to_next_open(to_ms + mins * 60 * 1000, args.step_min)
                except ValueError:
                    print("[ERR] Expected +<minutes> (e.g., +60).")
                continue

            if c.startswith("goto "):
                tgt = cmd[5:].strip()
                try:
                    to_ms = align_to_next_open(parse_iso_or_epoch(tgt), args.step_min)
                except SystemExit as e:
                    print(e)
                continue

            if c in ("help", "?"):
                print(f"  ENTER          : next open {args.step_min}m candle")
                print(f"  a / auto       : AUTO ({args.step_min}m) — stop when AOI touched AND 1d&4h are aligned")
                print(  "  h / +1h        : advance +1 hour then snap to grid")
                print(  "  d / day / skip : next trading day (22:00 UTC) then snap")
                print(  "  w / week / +1w : next Sunday 22:00 UTC then snap")
                print(  "  n / next       : next enabled session pre-open (TOKYO/LONDON/NY) — CLOSE at 23:45/06:45/11:45")
                print(  "  x              : exit AUTO immediately (hotkey, non-blocking)")
                print(  "  aoi            : toggle AOI+GlobalTrend stop ON/OFF")
                print(  "  +<minutes>     : custom advance")
                print(  "  goto <ISO/ms>  : jump to time, then snap")
                print(  "  --pairs        : restrict pairs")
                print(  "  --to           : bootstrap if no 15m present")
                print(  "  --only-fetch   : fetch only (no structure/AOI, no notify)")
                print(  "  q              : quit")
                continue

            print("[INFO] Unknown command. Type '?' for help.")
            continue

        # ----- AUTO MODE LOOP -----
        # Non-blocking hotkey check before doing work
        if auto_break_requested():
            print("[BT/AUTO] Hotkey 'x' detected → back to MANUAL.", flush=True)
            auto_mode = False
            continue

        print(f"\n[BT/AUTO] Running at --to {iso_utc(to_ms)} ...", flush=True)
        ok = step_once(to_ms, env, only_fetch=args.only_fetch)
        if not ok:
            print("[FATAL] A step failed. Stopping.", flush=True)
            sys.exit(1)

        # Non-blocking hotkey check after step
        if auto_break_requested():
            print("[BT/AUTO] Hotkey 'x' detected → back to MANUAL.", flush=True)
            auto_mode = False
            continue

        with get_pg_conn() as conn:
            reason = evaluate_stop_criteria_15m(conn, pairs, stop_aoi=args.stop_aoi)

        if reason:
            print(reason, flush=True)
            print("[BT/AUTO] Switching back to MANUAL.", flush=True)
            auto_mode = False
            continue

        to_ms = align_to_next_open(to_ms + args.step_min * 60 * 1000, args.step_min)
        auto_cap -= 1
        if auto_cap <= 0:
            print("[BT/AUTO] Safety cap hit. Switching back to MANUAL.", flush=True)
            auto_mode = False

if __name__ == "__main__":
    main()
