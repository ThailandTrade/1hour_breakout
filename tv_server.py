#!/usr/bin/env python3
# tv_server.py — WebSocket TV server (dashboard + charts)
import os, csv, re, json, asyncio, contextlib, time, select, threading
from typing import Dict, List, Tuple, Optional

import psycopg2, psycopg2.extras
import websockets
from websockets.server import WebSocketServerProtocol
from dotenv import load_dotenv
from datetime import datetime, timezone

# -------------------- ENV --------------------
load_dotenv()

PG_HOST     = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT     = int(os.getenv("PG_PORT", "5432"))
PG_DB       = os.getenv("PG_DB", "postgres")
PG_USER     = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "postgres")
PG_SSLMODE  = os.getenv("PG_SSLMODE", "disable")

WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))

# kept for backward-compat env but no longer used for meta
PAIRS_FILE       = os.getenv("PAIRS_FILE", "pairs.txt")
TIMEFRAMES_FILE  = os.getenv("TIMEFRAMES_FILE", "timeframes.txt")

POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "2.0"))
ROLE_TIMEOUT_SEC  = float(os.getenv("ROLE_TIMEOUT_SEC", "2.0"))
ALLOWED_TOKEN     = os.getenv("WS_TOKEN", "")

SCHEMA_VERSION = "1.0"
SNAPSHOT_DEBOUNCE_SEC = float(os.getenv("SNAPSHOT_DEBOUNCE_SEC", "0.3"))

UTC = timezone.utc

# ---- Preferred TF ordering (configurable) ----
# Example: PREFERRED_TF_ORDER="1W,1D,12H,8H,6H,4H,2H,1H,30M,15M,5M,3M,1M"
_PREF_TF_ORDER_ENV = os.getenv("PREFERRED_TF_ORDER", "")
_DEFAULT_TF_ORDER = ["1W","1D","12H","8H","6H","4H","2H","1H","30M","15M","5M","3M","1M"]

def _parse_tf_order_env(s: str) -> List[str]:
    arr = [x.strip().upper() for x in s.split(",") if x.strip()]
    out = []
    for x in arr:
        if re.fullmatch(r"\d+[WDHMS]", x):
            out.append(x)
    return out

PREFERRED_TF_ORDER: List[str] = _parse_tf_order_env(_PREF_TF_ORDER_ENV) or _DEFAULT_TF_ORDER
_TF_RANK = {tf: i for i, tf in enumerate(PREFERRED_TF_ORDER)}

def _tf_seconds(tf_up: str) -> int:
    m = re.fullmatch(r"(\d+)([WDHMS])", tf_up)
    if not m:
        return 0
    n = int(m.group(1)); u = m.group(2)
    mult = {"S":1, "M":60, "H":3600, "D":86400, "W":604800}.get(u, 1)
    return n * mult

def _tf_sort_key(tf_up: str):
    # 1) explicit order first
    if tf_up in _TF_RANK:
        return (_TF_RANK[tf_up], 0)
    # 2) otherwise, sort after listed TFs by descending duration (long -> short)
    return (len(_TF_RANK) + 1, -_tf_seconds(tf_up))

# -------------------- Time helpers (optional for logs/boundary) --------------------
def is_daily_boundary(ts_ms: int) -> bool:
    dt = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    return (dt.hour, dt.minute, dt.second) == (22, 0, 0)

def is_weekly_boundary(ts_ms: int) -> bool:
    dt = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    return dt.weekday() == 6 and (dt.hour, dt.minute, dt.second) == (22, 0, 0)  # Sun 22:00 UTC

# ------- Global event broadcast (NOTIFY -> all chart connections) -------
EVENT_SUBS = set()  # set[asyncio.Queue]

def subscribe_events():
    q = asyncio.Queue()
    EVENT_SUBS.add(q)
    return q

def unsubscribe_events(q: asyncio.Queue):
    with contextlib.suppress(KeyError):
        EVENT_SUBS.remove(q)

async def broadcast_event(ev: dict):
    # fan-out to all chart connections
    for q in list(EVENT_SUBS):
        with contextlib.suppress(asyncio.QueueFull):
            await q.put(ev)

# -------------------- DB --------------------
def get_pg_conn():
    dsn = f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD} sslmode={PG_SSLMODE}"
    return psycopg2.connect(dsn)

# -------------------- Helpers: scan available pairs/TFs from DB --------------------
_TABLE_RE = re.compile(r"^candles_mt5_([a-z0-9]+)_([0-9]+[smhdw])$")

def _normalize_pair(s: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", s.upper())

def _tf_to_upper(tf: str) -> str:
    # '15m' -> '15M', '4h' -> '4H', etc.
    return tf.strip().upper()

def _tf_to_lower(tf: str) -> str:
    return tf.strip().lower()

def scan_available_pairs_tfs(min_rows: int = 1) -> Tuple[List[str], Dict[str, List[str]], List[str], Dict[str, List[str]]]:
    """
    Scans information_schema for tables like candles_mt5_<pair>_<tf>, keeps only those with >= min_rows.
    Returns:
      pairs_sorted (List[str]),
      pairs_by_type (Dict[str, List[str]])  # single bucket 'MAJOR' for backward-compat
      tfs_union_upper (List[str])           # union of TFs across all pairs (UPPER)
      per_pair_timeframes (Dict[str, List[str]])  # pair -> [TF_UPPER,...]
    """
    tables: List[str] = []
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND table_name LIKE 'candles_mt5\\_%' ESCAPE '\\'
        """)
        tables = [r[0] for r in cur.fetchall()]

    per_pair: Dict[str, List[str]] = {}
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor() as cur:
        for tname in tables:
            m = _TABLE_RE.match(tname)
            if not m:
                continue
            pair_lower, tf_lower = m.group(1), m.group(2)
            pair_up = _normalize_pair(pair_lower)
            tf_up   = _tf_to_upper(tf_lower)
            # Check there is at least one row
            try:
                cur.execute(f"SELECT 1 FROM {tname} LIMIT %s", (min_rows,))
                rows = cur.fetchall()
            except psycopg2.errors.UndefinedTable:
                conn.rollback(); continue
            except Exception:
                conn.rollback(); continue
            if len(rows) >= min_rows:
                per_pair.setdefault(pair_up, [])
                if tf_up not in per_pair[pair_up]:
                    per_pair[pair_up].append(tf_up)

    # sort TFs inside each pair with preferred order
    for p in per_pair:
        per_pair[p].sort(key=_tf_sort_key)

    pairs_sorted = sorted(per_pair.keys())
    # union TFs (UPPER), also sorted with preferred order
    tfs_union: List[str] = []
    for p in pairs_sorted:
        for tf in per_pair[p]:
            if tf not in tfs_union:
                tfs_union.append(tf)
    tfs_union.sort(key=_tf_sort_key)

    # single bucket for dashboard grouping (keeps payload shape alive)
    pairs_by_type = {"MAJOR": pairs_sorted, "MINOR": []}

    return pairs_sorted, pairs_by_type, tfs_union, per_pair

# -------------------- LOGGING / SEND --------------------
def ws_label(ws: WebSocketServerProtocol) -> str:
    addr = getattr(ws, "remote_address", None)
    if isinstance(addr, tuple) and len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"
    return "unknown"

async def send_json(ws: WebSocketServerProtocol, payload: dict, *, ctx: str = "") -> bool:
    msg = json.dumps(payload, separators=(",", ":"))
    ptype = payload.get("type", "unknown")
    label = ws_label(ws)
    try:
        await ws.send(msg)
        # light logs
        if ptype in ("candles", "snapshot", "delta", "aoi_event", "meta"):
            extra = ""
            if ptype == "candles":
                pair = payload.get("pair"); tf = payload.get("tf")
                nb = len(payload.get("candles", []))
                extra = f" | {pair} {tf} | n={nb}"
            elif ptype == "snapshot":
                majors = len(payload.get("majors", []) or [])
                minors = len(payload.get("minors", []) or [])
                tfs = len(payload.get("meta", {}).get("timeframes", []) or [])
                extra = f" | majors={majors} minors={minors} tfs={tfs}"
            print(f"[SEND] {ptype:<12} -> {label} [{ctx}]{extra}")
        return True
    except websockets.ConnectionClosed:
        print(f"[SEND][CLOSED] drop {ptype} -> {label} [{ctx}]")
        return False
    except Exception as e:
        print(f"[SEND][FAIL] type={ptype} -> {label} [{ctx}] | {e.__class__.__name__}: {e}")
        return True  # don't break caller loop

# -------------------- Structure (dashboard) --------------------
def fetch_pairs_structure_rows(pairs: List[str], tfs_low: List[str]) -> List[dict]:
    if not pairs or not tfs_low:
        return []
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT pair, timeframe, hh, hl, lh, ll
            FROM public.pairs_structure
            WHERE pair = ANY(%s) AND timeframe = ANY(%s)
        """, (pairs, tfs_low))
        rows = cur.fetchall()
    out = []
    for r in rows:
        out.append({
            "pair": (r["pair"] or "").upper(),
            "timeframe": (r["timeframe"] or "").lower(),
            "hh": float(r["hh"] or 0) if r["hh"] not in (None, "") else 0.0,
            "hl": float(r["hl"] or 0) if r["hl"] not in (None, "") else 0.0,
            "lh": float(r["lh"] or 0) if r["lh"] not in (None, "") else 0.0,
            "ll": float(r["ll"] or 0) if r["ll"] not in (None, "") else 0.0,
        })
    out.sort(key=lambda x: (x["pair"], x["timeframe"]))
    return out

def compute_trend_from_fields(hh: float, hl: float, lh: float, ll: float) -> Optional[str]:
    if hh>0 and hl>0 and lh==0 and ll==0: return "BULLISH"
    if lh>0 and ll>0 and hh==0 and hl==0: return "BEARISH"
    return None

def trends_map(rows: List[dict]) -> Dict[Tuple[str,str], Optional[str]]:
    out: Dict[Tuple[str,str], Optional[str]] = {}
    for r in rows:
        out[(r["pair"], r["timeframe"])] = compute_trend_from_fields(r["hh"], r["hl"], r["lh"], r["ll"])
    return out

def group_snapshot_rows(rows: List[dict], pairs_subset: List[str]) -> List[dict]:
    if not rows or not pairs_subset:
        return []
    rows_by_pair: Dict[str, Dict[str, dict]] = {}
    wanted = set(pairs_subset)
    for r in rows:
        p = r["pair"]
        if p not in wanted:
            continue
        rows_by_pair.setdefault(p, {})
        rows_by_pair[p][r["timeframe"]] = {"hh": r["hh"], "hl": r["hl"], "lh": r["lh"], "ll": r["ll"]}
    out = []
    for p in pairs_subset:
        tf_map = rows_by_pair.get(p, {})
        if tf_map:
            out.append({"pair": p, "by_tf_full": tf_map})
    return out

# -------- Global Trend helpers (server-side only) --------
def fetch_trends_for_pair(pair: str, tfs_low: List[str]) -> Dict[str, Optional[str]]:
    if not tfs_low:
        return {}
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT timeframe, hh, hl, lh, ll
            FROM public.pairs_structure
            WHERE pair = %s AND timeframe = ANY(%s)
        """, (pair.upper(), tfs_low))
        rows = cur.fetchall()
    out: Dict[str, Optional[str]] = {}
    for r in rows:
        tf = (r["timeframe"] or "").lower()
        trend = compute_trend_from_fields(float(r["hh"] or 0), float(r["hl"] or 0), float(r["lh"] or 0), float(r["ll"] or 0))
        out[tf] = trend
    return out

def compute_global_trend_for_pair(pair: str) -> dict:
    # *** Nouvelle logique : uniquement 1D et 4H ***
    wanted = ["1d", "4h"]
    trends = fetch_trends_for_pair(pair, wanted)
    t1d = trends.get("1d")
    t4h = trends.get("4h")

    if t1d and t4h and t1d == t4h:
        return {"value": t1d, "basis": ["1D", "4H"]}
    return {"value": "NONE", "basis": []}

# -------------------- AOIs --------------------
def fetch_aois(pair: str) -> List[dict]:
    """
    Return ALL AOIs for a given pair (regardless of timeframe),
    with the exact fields needed by the front: low, high, type, tf.
    """
    sql = """
      SELECT tf, low, high, type
      FROM public.aoi_zones
      WHERE pair = %s
      ORDER BY updated_at DESC NULLS LAST, high DESC, low DESC
    """
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (pair,))
        rows = cur.fetchall()

    out: List[dict] = []
    for r in rows:
        try:
            lo = float(r["low"])
            hi = float(r["high"])
        except Exception:
            continue
        tf_val = (r["tf"] or "mtf").upper()
        typ    = (r["type"] or "").lower()
        out.append({"tf": tf_val, "low": lo, "high": hi, "type": typ})
    return out


def fetch_aois_batch(pairs: List[str], tfs_low: List[str]) -> List[dict]:
    """
    Batch helper used for dashboard snapshot (kept as-is).
    Note: this function still groups by TF filters for the dashboard payload only.
    """
    if not pairs:
        return []
    tfs = set(tfs_low or []); tfs.add("mtf")
    tfs_list = sorted(tfs)
    sql = """
      SELECT pair, tf, low, high
      FROM public.aoi_zones
      WHERE pair = ANY(%s) AND tf = ANY(%s)
      ORDER BY pair ASC, tf ASC, updated_at DESC NULLS LAST, high DESC, low DESC
    """
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (pairs, tfs_list))
        rows = cur.fetchall()
    out = []
    for r in rows:
        try:
            lo = float(r["low"]); hi = float(r["high"])
        except Exception:
            continue
        out.append({
            "pair": (r["pair"] or "").upper(),
            "tf":   (r["tf"] or "mtf").lower(),
            "low":  lo,
            "high": hi
        })
    return out

def group_aois_for_payload(rows: List[dict], pairs_subset: List[str], tfs_low: List[str]) -> List[dict]:
    if not rows or not pairs_subset:
        return []
    wanted = set(pairs_subset)
    tfs = set(tfs_low or []); tfs.add("mtf")
    by_pair: Dict[str, Dict[str, List[dict]]] = {}
    for r in rows:
        p = r["pair"]; tf = r["tf"]
        if p not in wanted or tf not in tfs:
            continue
        by_pair.setdefault(p, {}).setdefault(tf, []).append({"low": r["low"], "high": r["high"]})
    out = []
    for p in pairs_subset:
        tf_map = by_pair.get(p, {})
        if tf_map:
            out.append({"pair": p, "by_tf": tf_map})
    return out

# -------------------- Candles / structure fetch --------------------
def table_name_for(pair: str, tf_up: str) -> str:
    pair_lower = re.sub(r"[^a-z0-9]", "", pair.lower())
    tf_lower   = re.sub(r"\s+", "", tf_up.lower())
    name = f"candles_mt5_{pair_lower}_{tf_lower}"
    if not re.fullmatch(r"[a-z0-9_]+", name):
        raise ValueError("Invalid table name")
    return name

def fetch_candles_all(table_name: str) -> List[dict]:
    sql = f"SELECT ts, open, high, low, close FROM {table_name} ORDER BY ts ASC"
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql)
        rows = cur.fetchall()
    return [{"time": int(r["ts"])//1000, "open": float(r["open"]), "high": float(r["high"]), "low": float(r["low"]), "close": float(r["close"])} for r in rows]

def fetch_candles_ema_volume(table_name: str) -> Tuple[List[dict], List[dict], List[dict]]:
    """
    Lit candles + ema_50 + volume depuis la table si les colonnes existent.
    AUCUN calcul côté serveur. Aucune clause SQL spéciale sur NULL.
    - ema_50: si NULL sur une ligne, on n'émet pas de point pour cette ligne.
    - volume: on tente d'abord 'volume', sinon 'tick_volume'; si NULL, pas de point émis.
    """
    candles: List[dict] = []
    ema50: List[dict] = []
    volumes: List[dict] = []

    # 1) Essai avec ema_50 + volume
    sql1 = f"SELECT ts, open, high, low, close, ema_50, volume FROM {table_name} ORDER BY ts ASC"
    try:
        with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql1)
            rows = cur.fetchall()
        for r in rows:
            ts_sec = int(r["ts"]) // 1000
            candles.append({
                "time": ts_sec,
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low":  float(r["low"]),
                "close":float(r["close"]),
            })
            v_ema = r.get("ema_50")
            if v_ema is not None:
                try: ema50.append({"time": ts_sec, "value": float(v_ema)})
                except Exception: pass
            v_vol = r.get("volume")
            if v_vol is not None:
                try: volumes.append({"time": ts_sec, "value": float(v_vol)})
                except Exception: pass
        return candles, ema50, volumes
    except psycopg2.errors.UndefinedColumn:
        pass

    # 2) Essai avec ema_50 + tick_volume
    sql2 = f"SELECT ts, open, high, low, close, ema_50, tick_volume FROM {table_name} ORDER BY ts ASC"
    try:
        with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql2)
            rows = cur.fetchall()
        for r in rows:
            ts_sec = int(r["ts"]) // 1000
            candles.append({
                "time": ts_sec,
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low":  float(r["low"]),
                "close":float(r["close"]),
            })
            v_ema = r.get("ema_50")
            if v_ema is not None:
                try: ema50.append({"time": ts_sec, "value": float(v_ema)})
                except Exception: pass
            v_vol = r.get("tick_volume")
            if v_vol is not None:
                try: volumes.append({"time": ts_sec, "value": float(v_vol)})
                except Exception: pass
        return candles, ema50, volumes
    except psycopg2.errors.UndefinedColumn:
        pass

    # 3) Fallback sans ema_50 ni volume
    sql3 = f"SELECT ts, open, high, low, close FROM {table_name} ORDER BY ts ASC"
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql3)
        rows = cur.fetchall()
    candles = [{"time": int(r["ts"])//1000, "open": float(r["open"]), "high": float(r["high"]), "low": float(r["low"]), "close": float(r["close"])} for r in rows]
    return candles, [], []

def fetch_structure_for(pair: str, tf_up: str) -> dict:
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            """SELECT hh, hl, lh, ll FROM public.pairs_structure WHERE pair=%s AND timeframe=%s LIMIT 1""",
            (pair, tf_up.lower(),),
        )
        row = cur.fetchone()
    struct = {"trend": None, "upper": None, "lower": None}
    if row:
        hh, hl, lh, ll = float(row["hh"] or 0), float(row["hl"] or 0), float(row["lh"] or 0), float(row["ll"] or 0)
        if hh>0 and hl>0 and lh==0 and ll==0:
            struct = {"trend":"BULLISH","upper":hh,"lower":hl}
        elif lh>0 and ll>0 and hh==0 and hl==0:
            struct = {"trend":"BEARISH","upper":lh,"lower":ll}
    return struct

# -------------------- DASHBOARD SENDERS --------------------
async def dashboard_send_snapshot(ws: WebSocketServerProtocol, pairs: List[str], tfs_low: List[str], pairs_by_type: Dict[str, List[str]]) -> Tuple[bool, List[dict]]:
    rows_struct = fetch_pairs_structure_rows(pairs, tfs_low)
    majors = group_snapshot_rows(rows_struct, pairs_by_type.get("MAJOR", []) or pairs_by_type.get("MAJORS", []) or [])
    minors = group_snapshot_rows(rows_struct, pairs_by_type.get("MINOR", []) or pairs_by_type.get("MINORS", []) or [])
    known_pairs = (pairs_by_type.get("MAJOR", []) or []) + (pairs_by_type.get("MINOR", []) or [])
    aoi_rows = fetch_aois_batch(known_pairs, tfs_low)
    aois_grouped = group_aois_for_payload(aoi_rows, known_pairs, tfs_low)
    payload = {
        "type": "snapshot",
        "meta": {"timeframes": tfs_low},  # keep as list (lower) like before
        "majors": majors,
        "minors": minors,
        "aois": aois_grouped
    }
    ok = await send_json(ws, payload, ctx="dashboard/snapshot")
    return ok, rows_struct

async def send_aoi_event(ws: WebSocketServerProtocol, pair: str, prev_ts: int, prev_close: float, curr_ts: int, curr_close: float, hits: List[dict]) -> bool:
    payload = {
        "type": "aoi_event",
        "pair": pair,
        "prev": {"ts": prev_ts, "close": prev_close},
        "curr": {"ts": curr_ts, "close": curr_close},
        "events": hits
    }
    return await send_json(ws, payload, ctx="dashboard/aoi_event")

# -------------------- DASHBOARD LOOP --------------------
def fetch_last_15m_ts_for_pairs(pairs: List[str]) -> Dict[str, int]:
    result: Dict[str, int] = {}
    if not pairs:
        return result
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor() as cur:
        for p in pairs:
            try:
                table = table_name_for(p, "15M")
                cur.execute(f"SELECT MAX(ts) FROM {table}")
                row = cur.fetchone()
                if row and row[0]:
                    result[p] = int(row[0])
            except psycopg2.errors.UndefinedTable:
                conn.rollback(); continue
            except Exception:
                conn.rollback(); continue
    return result

def fetch_last_two_15m_closes(pair: str) -> Optional[Tuple[int, float, int, float]]:
    try:
        table = table_name_for(pair, "15M")
    except ValueError:
        return None
    sql = f"SELECT ts, close FROM {table} ORDER BY ts DESC LIMIT 2"
    with contextlib.closing(get_pg_conn()) as conn, conn.cursor() as cur:
        try:
            cur.execute(sql)
            rows = cur.fetchall()
        except psycopg2.errors.UndefinedTable:
            conn.rollback(); return None
    if not rows or len(rows) < 2:
        return None
    curr_ts, curr_close = int(rows[0][0]), float(rows[0][1])
    prev_ts, prev_close = int(rows[1][0]), float(rows[1][1])
    return (prev_ts, prev_close, curr_ts, curr_close)

def any_new_15m(prev: Dict[str, int], curr: Dict[str, int]) -> List[str]:
    changed: List[str] = []
    for pair, curr_ts in curr.items():
        prev_ts = prev.get(pair)
        if prev_ts is None and curr_ts is not None:
            changed.append(pair)
        elif prev_ts is not None and curr_ts is not None and curr_ts > prev_ts:
            changed.append(pair)
    return changed

async def dashboard_delta_loop(ws: WebSocketServerProtocol, pairs: List[str], tfs_low: List[str], *, init_last15: Dict[str,int]):
    prev_trends: Dict[Tuple[str,str], Optional[str]] = {}
    last15_ts: Dict[str, int] = dict(init_last15 or {})
    last_snapshot_sent_at: float = 0.0

    try:
        init_rows = fetch_pairs_structure_rows(pairs, tfs_low)
        prev_trends = trends_map(init_rows)
    except Exception as e:
        ok = await send_json(ws, {"type":"error","message":f"initial_fetch_failed:{e.__class__.__name__}"}, ctx="dashboard/error")
        if not ok: return

    while True:
        await asyncio.sleep(POLL_INTERVAL_SEC)
        try:
            # AOI events on new 15m bar
            curr_last15 = fetch_last_15m_ts_for_pairs(pairs)
            changed_pairs = any_new_15m(last15_ts, curr_last15)
            if changed_pairs:
                for p in changed_pairs:
                    lt = fetch_last_two_15m_closes(p)
                    if not lt:
                        continue
                    prev_ts, prev_close, curr_ts, curr_close = lt
                    aois = fetch_aois(p)  # ALL AOIs (no TF filtering)
                    hits: List[dict] = []
                    for a in aois:
                        low, high = float(a.get("low")), float(a.get("high"))
                        # detect transitions against every AOI zone
                        prev_in = (low <= prev_close <= high)
                        curr_in = (low <= curr_close <= high)
                        if (prev_close < low and curr_close > high):
                            hits.append({"tf": a.get("tf","MTF"), "low": low, "high": high, "kind": "traverse", "direction": "up"})
                        elif (prev_close > high and curr_close < low):
                            hits.append({"tf": a.get("tf","MTF"), "low": low, "high": high, "kind": "traverse", "direction": "down"})
                        elif (not prev_in) and curr_in:
                            direction = "up" if curr_close > prev_close else "down" if curr_close < prev_close else "flat"
                            hits.append({"tf": a.get("tf","MTF"), "low": low, "high": high, "kind": "arrive", "direction": direction})
                    if hits:
                        ok = await send_aoi_event(ws, p, prev_ts, prev_close, curr_ts, curr_close, hits)
                        if not ok: return
                last15_ts = curr_last15

            # Structure: trend change -> snapshot (and rescan available pairs/TFs)
            curr_rows = fetch_pairs_structure_rows(pairs, tfs_low)
            curr_trends = trends_map(curr_rows)

            trend_changed = False
            keys = set(prev_trends.keys()) | set(curr_trends.keys())
            for k in keys:
                if prev_trends.get(k) != curr_trends.get(k):
                    trend_changed = True
                    break

            now = time.time()
            if trend_changed and (now - last_snapshot_sent_at) >= SNAPSHOT_DEBOUNCE_SEC:
                # RESCAN to keep only pairs/TFs that still have data
                pairs_scan, pairs_by_type_scan, tfs_union_up, _per_pair = scan_available_pairs_tfs()
                tfs_scan_low = [_tf_to_lower(tf) for tf in tfs_union_up]
                ok, rows_struct = await dashboard_send_snapshot(ws, pairs_scan, tfs_scan_low, pairs_by_type_scan)
                if not ok: return
                prev_trends = trends_map(rows_struct)
                last_snapshot_sent_at = time.time()
                # also update local filters
                pairs[:] = pairs_scan
                tfs_low[:] = tfs_scan_low
                continue

            prev_trends = curr_trends

        except websockets.ConnectionClosed:
            return
        except Exception as e:
            ok = await send_json(ws, {"type":"error","message":f"delta_fetch_failed:{e.__class__.__name__}"}, ctx="dashboard/error")
            if not ok: return

# -------------------- CHARTS: pull + notify-push --------------------
async def charts_send_meta(ws, pairs, pairs_by_type, ui_tfs_up, per_pair_tfs):
    # keep legacy fields; add per_pair_timeframes for finer UI control
    await send_json(ws, {
        "type": "meta",
        "pairs": pairs,
        "pairs_by_type": pairs_by_type,
        "timeframes": ui_tfs_up,
        "per_pair_timeframes": per_pair_tfs  # NEW (optional for front)
    }, ctx="charts/meta")

async def charts_send_candles(ws, pair: str, tf_up: str):
    try:
        table = table_name_for(pair, tf_up)
        # Read candles + ema_50 + volume from DB (no server-side computation)
        candles, ema50, volumes = fetch_candles_ema_volume(table)

        struct = fetch_structure_for(pair, tf_up)
        aois = fetch_aois(pair)  # ALL AOIs (no TF filtering)
        # attach global trend to candles payload
        global_trend = compute_global_trend_for_pair(pair)
        ok = await send_json(ws, {
            "type":"candles",
            "pair":pair,
            "tf":tf_up,
            "candles":candles,
            "structure":struct,
            "aois":aois,
            "ema50":ema50,
            "volumes":volumes,          # NEW (optionnel)
            "global_trend": global_trend
        }, ctx="charts/candles")
        return ok
    except psycopg2.errors.UndefinedTable:
        await send_json(ws, {"type":"error","message":"table_not_found","meta":{"pair":pair,"tf":tf_up}}, ctx="charts/error")
        return True
    except websockets.ConnectionClosed:
        return False
    except Exception as e:
        await send_json(ws, {"type":"error","message":f"db_error:{e.__class__.__name__}"}, ctx="charts/error")
        return True

class ChartsSubscriptions:
    """Subscriptions per connection: Set[(pair, TF_UPPER)]"""
    def __init__(self):
        self.subs: set[Tuple[str, str]] = set()
    def add(self, pair: str, tf_up: str):
        self.subs.add((pair, tf_up))
    def remove(self, pair: str, tf_up: str):
        self.subs.discard((pair, tf_up))
    def items(self):
        return list(self.subs)

async def charts_event_loop(ws: WebSocketServerProtocol, subs: ChartsSubscriptions, event_q: asyncio.Queue):
    label = ws_label(ws)
    print(f"[CHARTS] event loop started for {label}")
    try:
        while True:
            ev = await event_q.get()
            ev_pair = (ev.get("pair") or "").upper()
            ev_to   = int(ev.get("to") or 0)

            pushed_any = False
            for (pair, tf_up) in subs.items():
                if pair != ev_pair:
                    continue
                ok = await charts_send_candles(ws, pair, tf_up)
                if ok is False:
                    return
                pushed_any = True
                print(f"[CHARTS] pushed candles (notify) -> {label} | {pair} {tf_up}")

            if ev_to:
                if is_weekly_boundary(ev_to):
                    print(f"[CHARTS] note: weekly boundary @ {ev_to} for {ev_pair}")
                elif is_daily_boundary(ev_to):
                    print(f"[CHARTS] note: daily boundary @ {ev_to} for {ev_pair}")

    except asyncio.CancelledError:
        print(f"[CHARTS] event loop cancelled for {label}")
    except websockets.ConnectionClosed:
        print(f"[CHARTS] connection closed during event loop for {label}")
    except Exception as e:
        print(f"[CHARTS][ERROR] event loop for {label}: {e.__class__.__name__}: {e}")

# -------------------- Role handlers --------------------
async def handle_dashboard(ws: WebSocketServerProtocol):
    ok = await send_json(ws, {"type":"hello","mode":"dashboard","schema":SCHEMA_VERSION}, ctx="dashboard/hello")
    if not ok: return

    # NEW: scan DB instead of reading files
    pairs_scan, pairs_by_type, tfs_union_up, _per_pair = scan_available_pairs_tfs()
    tfs_low = [_tf_to_lower(tf) for tf in tfs_union_up]

    try:
        ok, _rows = await dashboard_send_snapshot(ws, pairs_scan, tfs_low, pairs_by_type)
        if not ok: return
    except Exception as e:
        ok = await send_json(ws, {"type":"error","message":f"snapshot_build_failed:{e.__class__.__name__}"}, ctx="dashboard/error")
        if not ok: return

    # Only track pairs that have 15M data
    pairs_with_15m = [p for p in pairs_scan if "15M" in (_per_pair.get(p) or [])]
    init_last15 = fetch_last_15m_ts_for_pairs(pairs_with_15m)

    try:
        await dashboard_delta_loop(ws, pairs_with_15m, tfs_low, init_last15=init_last15)
    except websockets.ConnectionClosed:
        print(f"[WS] dashboard client disconnected: {ws_label(ws)}")

async def handle_charts(ws: WebSocketServerProtocol):
    ok = await send_json(ws, {"type":"hello","mode":"charts","schema":SCHEMA_VERSION}, ctx="charts/hello")
    if not ok: return

    # NEW: scan DB instead of reading files
    pairs, pairs_by_type, ui_tfs_up, per_pair_tfs = scan_available_pairs_tfs()

    subs = ChartsSubscriptions()
    event_q = subscribe_events()
    event_task: Optional[asyncio.Task] = None

    def ensure_event_loop():
        nonlocal event_task
        if event_task is None or event_task.done():
            event_task = asyncio.create_task(charts_event_loop(ws, subs, event_q))

    try:
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except Exception:
                ok = await send_json(ws, {"type":"error","message":"invalid_json"}, ctx="charts/error")
                if not ok: return
                continue

            action = (msg.get("action") or "").strip().lower()

            if action == "list":
                ok = await charts_send_meta(ws, pairs, pairs_by_type, ui_tfs_up, per_pair_tfs)
                if ok is False: return
                continue

            if action == "load":
                pair = (msg.get("pair") or "").upper().strip()
                tf   = (msg.get("tf")   or "").upper().strip()
                if not pair or not tf:
                    ok = await send_json(ws, {"type":"error","message":"missing_pair_or_tf"}, ctx="charts/error")
                    if not ok: return
                    continue
                if pairs and pair not in pairs:
                    ok = await send_json(ws, {"type":"error","message":"unknown_pair","meta":{"pair":pair}}, ctx="charts/error")
                    if not ok: return
                    continue
                # validate TF against available TFs for THIS pair
                allowed_tfs = per_pair_tfs.get(pair, [])
                if allowed_tfs and tf not in allowed_tfs:
                    ok = await send_json(ws, {"type":"error","message":"unknown_timeframe","meta":{"tf":tf,"pair":pair}}, ctx="charts/error")
                    if not ok: return
                    continue

                ok = await charts_send_candles(ws, pair, tf)  # initial snapshot
                if ok is False: return

                subs.add(pair, tf)  # auto-subscribe
                ensure_event_loop()
                print(f"[CHARTS] auto-subscribed {ws_label(ws)} -> {pair} {tf}")
                continue

            if action == "subscribe":
                pair = (msg.get("pair") or "").upper().strip()
                tf   = (msg.get("tf")   or "").upper().strip()
                if not pair or not tf:
                    ok = await send_json(ws, {"type":"error","message":"missing_pair_or_tf"}, ctx="charts/error")
                    if not ok: return
                    continue
                if pairs and pair not in pairs:
                    ok = await send_json(ws, {"type":"error","message":"unknown_pair","meta":{"pair":pair}}, ctx="charts/error")
                    if not ok: return
                    continue
                allowed_tfs = per_pair_tfs.get(pair, [])
                if allowed_tfs and tf not in allowed_tfs:
                    ok = await send_json(ws, {"type":"error","message":"unknown_timeframe","meta":{"tf":tf,"pair":pair}}, ctx="charts/error")
                    if not ok: return
                    continue

                ok = await charts_send_candles(ws, pair, tf)
                if ok is False: return

                subs.add(pair, tf)
                ensure_event_loop()
                print(f"[CHARTS] subscribed {ws_label(ws)} -> {pair} {tf}")
                continue

            if action == "unsubscribe":
                pair = (msg.get("pair") or "").upper().strip()
                tf   = (msg.get("tf")   or "").upper().strip()
                if not pair or not tf:
                    ok = await send_json(ws, {"type":"error","message":"missing_pair_or_tf"}, ctx="charts/error")
                    if not ok: return
                    continue
                subs.remove(pair, tf)
                ok = await send_json(ws, {"type":"ok","message":"unsubscribed","pair":pair,"tf":tf}, ctx="charts/unsub")
                if not ok: return
                print(f"[CHARTS] unsubscribed {ws_label(ws)} -> {pair} {tf}")
                continue

            ok = await send_json(ws, {"type":"error","message":"unknown_action","meta":{"action":action}}, ctx="charts/error")
            if not ok: return
    except websockets.ConnectionClosed:
        print(f"[WS] charts client disconnected: {ws_label(ws)}")
    finally:
        if event_task and not event_task.done():
            event_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await event_task
        unsubscribe_events(event_q)

# -------------------- PG LISTEN loop (threaded, graceful stop) --------------------
def pg_listen_blocking(stop_event: threading.Event, loop: asyncio.AbstractEventLoop):
    """
    LISTEN on channel tv_events.
    Payload expected:
      {"pair":"EURUSD","to":1738706400000}
    """
    conn = None
    cur = None
    try:
        while not stop_event.is_set():
            try:
                conn = get_pg_conn()
                conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                cur = conn.cursor()
                cur.execute("LISTEN tv_events;")
                print("[PG] Listening on channel tv_events (thread)")

                while not stop_event.is_set():
                    if select.select([conn], [], [], 1.0) == ([], [], []):
                        continue
                    conn.poll()
                    while conn.notifies:
                        note = conn.notifies.pop(0)
                        try:
                            payload = json.loads(note.payload)
                            payload["pair"] = (payload.get("pair") or "").upper()
                            if "to" in payload:
                                try:
                                    payload["to"] = int(payload["to"])
                                except Exception:
                                    payload["to"] = 0
                            loop.call_soon_threadsafe(asyncio.create_task, broadcast_event(payload))
                            print(f"[PG] event -> {payload}")
                        except Exception as e:
                            print(f"[PG][WARN] bad payload: {note.payload} ({e})")
            except Exception as e:
                if stop_event.is_set():
                    break
                print(f"[PG][ERROR] listen loop: {e.__class__.__name__}: {e}")
                time.sleep(1.0)
            finally:
                try:
                    if cur: cur.close()
                except Exception:
                    pass
                try:
                    if conn: conn.close()
                except Exception:
                    pass
                cur = None; conn = None
    finally:
        print("[PG] listener thread exiting")

# -------------------- Handshake routing --------------------
async def handle_client(ws: WebSocketServerProtocol):
    print(f"[WS] client connected: {ws_label(ws)}")
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=ROLE_TIMEOUT_SEC)
    except asyncio.TimeoutError:
        await ws.close(code=1008, reason="missing role"); print(f"[WS] closed: missing role from {ws_label(ws)}"); return
    except websockets.ConnectionClosed:
        print(f"[WS] client closed before handshake: {ws_label(ws)}"); return
    try:
        hello = json.loads(raw)
    except Exception:
        await send_json(ws, {"type":"error","message":"invalid_json_handshake"}, ctx="handshake/error")
        await ws.close(code=1002, reason="invalid json"); return
    role = (hello.get("role") or "").strip().lower()
    token = (hello.get("token") or "").strip()
    print(f"[WS] handshake from {ws_label(ws)} | role={role or '-'} schema={hello.get('schema')}")
    if ALLOWED_TOKEN and token != ALLOWED_TOKEN:
        await send_json(ws, {"type":"error","message":"unauthorized"}, ctx="handshake/error")
        await ws.close(code=1008, reason="unauthorized"); print(f"[WS] unauthorized -> closed {ws_label(ws)}"); return
    if role == "dashboard":
        await handle_dashboard(ws); return
    if role == "charts":
        await handle_charts(ws); return
    await send_json(ws, {"type":"error","message":"unknown_role"}, ctx="handshake/error")
    await ws.close(code=1008, reason="unknown role"); print(f"[WS] unknown role -> closed {ws_label(ws)}")

# -------------------- Entrypoint --------------------
async def main():
    print(f"[WS] starting on {WS_HOST}:{WS_PORT}")
    loop = asyncio.get_running_loop()
    stop_event = threading.Event()
    listener_task = asyncio.create_task(asyncio.to_thread(pg_listen_blocking, stop_event, loop))
    async with websockets.serve(
        handle_client, WS_HOST, WS_PORT,
        ping_interval=30, ping_timeout=30, max_size=64*1024*1024
    ):
        try:
            await asyncio.Future()
        finally:
            stop_event.set()
            listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[WS] stopped by user")
