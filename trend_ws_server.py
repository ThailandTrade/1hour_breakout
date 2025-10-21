#!/usr/bin/env python3
# trend_ws_server.py
"""
Live trend + AOI dashboard + AOI 15m alerts (no DB history):
- Lit pairs.txt (MAJOR/MINOR) et timeframes.txt (ordre)
- Poll en continu:
    * pairs_structure (tendances)
    * aoi_zones (zones)
    * candles_mt5_{pair}_15m (bougies 15m, fermées)
- Au démarrage:
    * broadcast snapshot complet (trend + AOI) à tous les clients connectés
- À chaque connexion client:
    * envoie un snapshot frais (trend + AOI) à CE client
- Ensuite:
    * broadcast uniquement les deltas (trend + AOI)
    * calcule et broadcast des "aoi_alert" en temps réel (sans persistance)

ENV (.env):
  PG_HOST, PG_PORT, PG_DB, PG_USER, PG_PASSWORD, PG_SSLMODE
  WS_HOST (default 0.0.0.0)
  WS_PORT (default 8765)
  POLL_INTERVAL_SEC (default 2)
  PAIRS_FILE (default pairs.txt)
  TIMEFRAMES_FILE (default timeframes.txt)

ALERTS (facultatif, tous par défaut si non spécifiés):
  ALERTS_ENABLED=true
  ALERTS_ZONE_TFS=mtf,1d,1w
  ALERTS_TOP_N=5
  ALERTS_MIN_OVERLAP_PIPS=1.0
  ALERTS_COOLDOWN_MIN=60
  ALERTS_LOG_BODY=false

DB attendue:
  pairs_structure(pair TEXT, timeframe TEXT,
                  hh DOUBLE PRECISION, hl DOUBLE PRECISION,
                  lh DOUBLE PRECISION, ll DOUBLE PRECISION,
                  structure_point_low  DOUBLE PRECISION,
                  structure_point_high DOUBLE PRECISION,
                  last_analyzed_ts BIGINT,
                  PRIMARY KEY(pair,timeframe))

  aoi_zones(
      pair TEXT NOT NULL,
      tf   TEXT  NOT NULL,   -- 'mtf' (et potentiellement 1w/1d si un jour)
      low  DOUBLE PRECISION NOT NULL,
      high DOUBLE PRECISION NOT NULL,
      height_pips DOUBLE PRECISION NOT NULL,
      touches INTEGER NOT NULL,
      updated_at TIMESTAMPTZ
  )

Tables bougies (créées par ton fetch):
  candles_mt5_{pairlower}_{tf}  (ex: candles_mt5_eurusd_15m)
    -> colonnes: ts BIGINT, open, high, low, close (Float)

Messages WebSocket:
  - Trends:
      type="snapshot" (v3) puis type="delta"
  - AOI:
      type="aoi_snapshot" (v2) puis type="aoi_delta"
  - Alerts:
      type="aoi_alert" (évènements éphémères)
"""

import asyncio
import csv
import json
import os
from typing import Dict, List, Tuple, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import websockets
from websockets.server import WebSocketServerProtocol
from datetime import datetime, timezone, timedelta

# ---------------------------
# Config / IO helpers
# ---------------------------

def load_env():
    load_dotenv()
    def _bool(name, default=False):
        v = os.getenv(name)
        if v is None:
            return default
        return str(v).strip().lower() in ("1", "true", "yes", "on")
    def _float(name, default):
        v = os.getenv(name)
        try:
            return float(v) if v is not None else default
        except Exception:
            return default
    def _int(name, default):
        v = os.getenv(name)
        try:
            return int(v) if v is not None else default
        except Exception:
            return default

    alerts_zone_tfs = os.getenv("ALERTS_ZONE_TFS", "mtf,1d,1w")
    alerts_zone_tfs = [s.strip().lower() for s in alerts_zone_tfs.split(",") if s.strip()]

    return {
        "PG_HOST": os.getenv("PG_HOST", "127.0.0.1"),
        "PG_PORT": int(os.getenv("PG_PORT", "5432")),
        "PG_DB": os.getenv("PG_DB", "postgres"),
        "PG_USER": os.getenv("PG_USER", "postgres"),
        "PG_PASSWORD": os.getenv("PG_PASSWORD", "postgres"),
        "PG_SSLMODE": os.getenv("PG_SSLMODE", "disable"),
        "WS_HOST": os.getenv("WS_HOST", "0.0.0.0"),
        "WS_PORT": int(os.getenv("WS_PORT", "8765")),
        "POLL_INTERVAL_SEC": float(os.getenv("POLL_INTERVAL_SEC", "2")),
        "PAIRS_FILE": os.getenv("PAIRS_FILE", "pairs.txt"),
        "TIMEFRAMES_FILE": os.getenv("TIMEFRAMES_FILE", "timeframes.txt"),
        # Alerts config
        "ALERTS_ENABLED": _bool("ALERTS_ENABLED", True),
        "ALERTS_ZONE_TFS": alerts_zone_tfs,
        "ALERTS_TOP_N": _int("ALERTS_TOP_N", 5),
        "ALERTS_MIN_OVERLAP_PIPS": _float("ALERTS_MIN_OVERLAP_PIPS", 1.0),
        "ALERTS_COOLDOWN_MIN": _int("ALERTS_COOLDOWN_MIN", 60),
        "ALERTS_LOG_BODY": _bool("ALERTS_LOG_BODY", False),
    }

def pg_conn(cfg):
    return psycopg2.connect(
        host=cfg["PG_HOST"],
        port=cfg["PG_PORT"],
        dbname=cfg["PG_DB"],
        user=cfg["PG_USER"],
        password=cfg["PG_PASSWORD"],
        sslmode=cfg["PG_SSLMODE"],
    )

def parse_pairs(path: str) -> Tuple[List[str], Dict[str, str]]:
    seen = set()
    ordered_pairs = []
    pair_type: Dict[str, str] = {}
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            ptype = (row.get("type") or row.get("TYPE") or row.get("Type") or "").strip().upper()
            p = (row.get("pair") or row.get("PAIR") or row.get("Pair") or "").strip()
            if not p:
                continue
            p = p.upper().replace("/", "")
            if p not in seen:
                ordered_pairs.append(p)
                seen.add(p)
            if ptype:
                pair_type[p] = ptype
    if not ordered_pairs:
        raise ValueError("pairs.txt is empty or invalid. Expected header 'type,pair'.")
    return ordered_pairs, pair_type

def parse_timeframes(path: str) -> List[str]:
    tfs: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            code = line.strip().lower()
            if code and not code.startswith("#"):
                tfs.append(code)
    if not tfs:
        raise ValueError("timeframes.txt is empty.")
    return tfs

def ts_ms_to_iso(ts_ms: Optional[int]) -> Optional[str]:
    if ts_ms is None:
        return None
    try:
        return datetime.fromtimestamp(int(ts_ms)/1000, tz=timezone.utc).isoformat()
    except Exception:
        return None

# ---------------------------
# Trend logic (strict)
# ---------------------------

def compute_trend(hh: Optional[float], hl: Optional[float], lh: Optional[float], ll: Optional[float]) -> str:
    hh = hh or 0.0
    hl = hl or 0.0
    lh = lh or 0.0
    ll = ll or 0.0
    if hh > 0 and hl > 0 and lh == 0 and ll == 0:
        return "Bullish"
    if lh > 0 and ll > 0 and hh == 0 and hl == 0:
        return "Bearish"
    return "Neutral"

# ---------------------------
# DB fetch and state building (Trends)
# ---------------------------

def fetch_pairs_structure(conn, pairs: List[str], tfs: List[str]) -> List[psycopg2.extras.DictRow]:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT
              pair, timeframe,
              hh, hl, lh, ll,
              structure_point_low,
              structure_point_high,
              last_analyzed_ts
            FROM pairs_structure
            WHERE pair = ANY(%s) AND timeframe = ANY(%s)
        """, (pairs, tfs))
        return cur.fetchall()

def make_snapshot(rows, pairs: List[str], tfs: List[str], pair_type_map: Dict[str, str]) -> Dict:
    idx: Dict[Tuple[str, str], dict] = {}
    latest_by_pair_ts: Dict[str, Optional[int]] = {}

    for r in rows:
        pair = r["pair"].upper()
        tf = r["timeframe"].lower()
        trend = compute_trend(r["hh"], r["hl"], r["lh"], r["ll"])
        last_ts = int(r["last_analyzed_ts"] or 0)
        rec = {
            "trend": trend,
            "hh": r["hh"], "hl": r["hl"], "lh": r["lh"], "ll": r["ll"],
            "structure_point_low": r["structure_point_low"],
            "structure_point_high": r["structure_point_high"],
            "last_ts": last_ts,
            "last_ts_iso": ts_ms_to_iso(last_ts),
        }
        idx[(pair, tf)] = rec
        prev = latest_by_pair_ts.get(pair) or 0
        if last_ts > prev:
            latest_by_pair_ts[pair] = last_ts

    def row_for_pair(pair: str) -> dict:
        by_tf = {}
        by_tf_full = {}
        for tf in tfs:
            rec = idx.get((pair, tf))
            if rec:
                by_tf[tf] = rec["trend"]
                by_tf_full[tf] = {
                    "trend": rec["trend"],
                    "hh": rec["hh"], "hl": rec["hl"], "lh": rec["lh"], "ll": rec["ll"],
                    "structure_point_low": rec["structure_point_low"],
                    "structure_point_high": rec["structure_point_high"],
                    "last_ts": rec["last_ts"],
                    "last_ts_iso": rec["last_ts_iso"],
                }
            else:
                by_tf[tf] = "Bearish"
                by_tf_full[tf] = {
                    "trend": "Bearish",
                    "hh": None, "hl": None, "lh": None, "ll": None,
                    "structure_point_low": None,
                    "structure_point_high": None,
                    "last_ts": None,
                    "last_ts_iso": None,
                }

        pair_last_ts = latest_by_pair_ts.get(pair)
        return {
            "pair": pair,
            "by_tf": by_tf,
            "by_tf_full": by_tf_full,
            "last_ts": pair_last_ts,
            "last_ts_iso": ts_ms_to_iso(pair_last_ts) if pair_last_ts else None,
        }

    majors, minors, others = [], [], []
    for p in pairs:
        cat = pair_type_map.get(p, "")
        bucket = majors if cat == "MAJOR" else (minors if cat == "MINOR" else others)
        bucket.append(row_for_pair(p))

    return {
        "type": "snapshot",
        "meta": {"timeframes": tfs, "version": 3},
        "majors": majors,
        "minors": minors,
        "others": others,
    }

def build_cell_signature(trend: str, last_ts: Optional[int],
                         hh: Optional[float], hl: Optional[float],
                         lh: Optional[float], ll: Optional[float],
                         spl: Optional[float], sph: Optional[float]) -> str:
    return "|".join([
        trend or "",
        str(last_ts or 0),
        str(hh or 0.0), str(hl or 0.0), str(lh or 0.0), str(ll or 0.0),
        str(spl or 0.0), str(sph or 0.0),
    ])

def seed_prev_signatures(rows) -> Dict[Tuple[str, str], str]:
    prev: Dict[Tuple[str, str], str] = {}
    for r in rows:
        pair = r["pair"].upper()
        tf = r["timeframe"].lower()
        trend = compute_trend(r["hh"], r["hl"], r["lh"], r["ll"])
        sig = build_cell_signature(
            trend,
            int(r["last_analyzed_ts"] or 0),
            r["hh"], r["hl"], r["lh"], r["ll"],
            r["structure_point_low"], r["structure_point_high"]
        )
        prev[(pair, tf)] = sig
    return prev

def diff_rows(prev_idx: Dict[Tuple[str, str], str], rows) -> Tuple[List[dict], Dict[Tuple[str, str], str]]:
    changes: List[dict] = []
    curr_idx: Dict[Tuple[str, str], str] = {}

    existing_keys = set()
    for r in rows:
        pair = r["pair"].upper()
        tf = r["timeframe"].lower()
        trend = compute_trend(r["hh"], r["hl"], r["lh"], r["ll"])
        last_ts = int(r["last_analyzed_ts"] or 0)
        sig = build_cell_signature(
            trend, last_ts,
            r["hh"], r["hl"], r["lh"], r["ll"],
            r["structure_point_low"], r["structure_point_high"]
        )
        key = (pair, tf)
        curr_idx[key] = sig
        existing_keys.add(key)
        if prev_idx.get(key) != sig:
            changes.append({
                "pair": pair,
                "timeframe": tf,
                "trend": trend,
                "hh": r["hh"], "hl": r["hl"], "lh": r["lh"], "ll": r["ll"],
                "structure_point_low": r["structure_point_low"],
                "structure_point_high": r["structure_point_high"],
                "last_ts": last_ts,
                "last_ts_iso": ts_ms_to_iso(last_ts),
            })

    for key in list(prev_idx.keys()):
        if key not in existing_keys:
            pair, tf = key
            changes.append({
                "pair": pair,
                "timeframe": tf,
                "trend": "Neutral",
                "hh": None, "hl": None, "lh": None, "ll": None,
                "structure_point_low": None,
                "structure_point_high": None,
                "last_ts": None,
                "last_ts_iso": None,
            })
            curr_idx[key] = build_cell_signature("Neutral", 0, 0, 0, 0, 0, 0, 0)

    return changes, curr_idx

# ---------------------------
# AOI fetch / snapshot / diff (schéma minimal)
# ---------------------------

def fetch_aoi_rows(conn, pairs: List[str]):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT pair, tf, low, high, height_pips, touches
            FROM aoi_zones
            WHERE pair = ANY(%s)
        """, (pairs,))
        return cur.fetchall()

def _aoi_key(r) -> tuple:
    return (r["pair"].upper(), (r["tf"] or "").lower(), float(r["low"]), float(r["high"]))

def _aoi_sig(r) -> str:
    # signature basée sur les champs existants
    return "|".join([
        str(float(r["height_pips"] or 0)),
        str(int(r["touches"] or 0)),
    ])

def make_aoi_snapshot(rows):
    by_pair = {}
    for r in rows:
        p = r["pair"].upper()
        tf = (r["tf"] or "").lower()
        by_pair.setdefault(p, {}).setdefault(tf, []).append({
            "low": float(r["low"]),
            "high": float(r["high"]),
            "height_pips": float(r["height_pips"] or 0),
            "touches": int(r["touches"] or 0),
        })
    items = []
    for pair, tfmap in by_pair.items():
        items.append({
            "pair": pair,
            "zones": {
                "1w": tfmap.get("1w", []),
                "1d": tfmap.get("1d", []),
                "mtf": tfmap.get("mtf", []),
            }
        })
    return {"type": "aoi_snapshot", "meta": {"version": 2}, "items": items}

def seed_prev_aoi_signatures(rows):
    return { _aoi_key(r): _aoi_sig(r) for r in rows }

def diff_aoi_rows(prev_idx, rows):
    curr_idx = {}
    upserts = []
    deletes = []

    for r in rows:
        key = _aoi_key(r)
        sig = _aoi_sig(r)
        curr_idx[key] = sig
        if prev_idx.get(key) != sig:
            upserts.append({
                "pair": key[0],
                "tf": key[1],
                "low": float(r["low"]),
                "high": float(r["high"]),
                "height_pips": float(r["height_pips"] or 0),
                "touches": int(r["touches"] or 0),
            })

    for key in prev_idx.keys():
        if key not in curr_idx:
            deletes.append({
                "pair": key[0], "tf": key[1], "low": key[2], "high": key[3], "deleted": True
            })

    if not upserts and not deletes:
        return None, curr_idx

    payload = {"type": "aoi_delta", "changes": {"upserts": upserts, "deletes": deletes}}
    return payload, curr_idx

# ---------------------------
# Alerts 15m (in-memory, no DB)
# ---------------------------

def pip_size_for(pair: str) -> float:
    return 0.01 if pair.upper().endswith("JPY") else 0.0001

def tbl_15m_for(pair: str) -> str:
    return f"candles_mt5_{pair.lower()}_15m"

def overlap_len(a_low: float, a_high: float, b_low: float, b_high: float) -> float:
    return max(0.0, min(a_high, b_high) - max(a_low, b_low))

def classify_aoi_event(pair: str, zone_tf: str, zl: float, zh: float,
                       o: float, h: float, l: float, c: float,
                       min_overlap_pips: float) -> Optional[dict]:
    pip = pip_size_for(pair)
    body_low, body_high = (o, c) if o <= c else (c, o)

    body_overlap = overlap_len(body_low, body_high, zl, zh)
    wick_up_overlap = overlap_len(body_high, h, zl, zh) if h > body_high else 0.0
    wick_dn_overlap = overlap_len(l, body_low, zl, zh) if l < body_low else 0.0
    wick_overlap = wick_up_overlap + wick_dn_overlap

    body_overlap_pips = body_overlap / pip
    wick_overlap_pips = wick_overlap / pip

    # Traverse?
    if body_low < zl and body_high > zh:
        direction = "up" if c > o else ("down" if c < o else "through")
        return {
            "event_type": "BODY_TRAVERSE", "direction": direction,
            "body_overlap_pips": (zh - zl) / pip, "wick_overlap_pips": wick_overlap_pips
        }

    # Body inside?
    if body_overlap_pips >= min_overlap_pips:
        direction = "up" if c > o else ("down" if c < o else None)
        return {
            "event_type": "BODY_INSIDE", "direction": direction,
            "body_overlap_pips": body_overlap_pips, "wick_overlap_pips": wick_overlap_pips
        }

    # Wick touch / traverse?
    if wick_overlap_pips >= min_overlap_pips:
        if l < zl and h > zh:
            return {
                "event_type": "WICK_TRAVERSE", "direction": "through",
                "body_overlap_pips": body_overlap_pips, "wick_overlap_pips": wick_overlap_pips
            }
        else:
            direction = "up" if c > o else ("down" if c < o else None)
            return {
                "event_type": "WICK_TOUCH", "direction": direction,
                "body_overlap_pips": body_overlap_pips, "wick_overlap_pips": wick_overlap_pips
            }

    return None

def fetch_new_15m_candles(conn, pair: str, since_ts: int) -> List[Tuple[int, float, float, float, float]]:
    tname = tbl_15m_for(pair)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT ts, open, high, low, close
            FROM {tname}
            WHERE ts > %s
            ORDER BY ts ASC
        """, (since_ts,))
        rows = cur.fetchall()
    return [(int(ts), float(o), float(h), float(l), float(c)) for ts, o, h, l, c in rows]

def select_zones_for_alerts(aoi_rows: List[psycopg2.extras.DictRow],
                            zone_tfs: List[str], top_n: int) -> Dict[str, List[dict]]:
    """
    Renvoie dict pair -> list de zones filtrées (tf in zone_tfs).
    Tri: touches DESC, puis height_pips ASC. Limité à top_n.
    """
    by_pair: Dict[str, List[dict]] = {}
    for r in aoi_rows:
        p = r["pair"].upper()
        tf = (r["tf"] or "").lower()
        if tf not in zone_tfs:
            continue
        by_pair.setdefault(p, []).append({
            "tf": tf,
            "low": float(r["low"]),
            "high": float(r["high"]),
            "touches": int(r["touches"] or 0),
            "height_pips": float(r["height_pips"] or 0.0),
        })
    for p, lst in by_pair.items():
        lst.sort(key=lambda z: (-z["touches"], z["height_pips"]))
        by_pair[p] = lst[:max(1, top_n)]
    return by_pair

# ---------------------------
# Console logging helpers
# ---------------------------

def log_snapshot_to_console(snapshot: dict, rows_index: Dict[Tuple[str, str], dict]):
    print("\n[DEBUG] Initial snapshot:")
    for group_name in ("majors", "minors", "others"):
        group = snapshot.get(group_name, [])
        if not group:
            continue
        print(f"  > {group_name.upper()}:")
        for item in group:
            pair = item["pair"]
            by_tf = item.get("by_tf", {})
            for tf, tr in by_tf.items():
                rec = rows_index.get((pair, tf))
                if tr == "Bullish" and rec:
                    hh = rec.get("hh"); hl = rec.get("hl")
                    print(f"    {pair:>8} | {tf:>5} | {tr:<7} | HH={fmt_num(hh)} HL={fmt_num(hl)}")
                elif tr == "Bearish" and rec:
                    lh = rec.get("lh"); ll = rec.get("ll")
                    print(f"    {pair:>8} | {tf:>5} | {tr:<7} | LH={fmt_num(lh)} LL={fmt_num(ll)}")
                else:
                    print(f"    {pair:>8} | {tf:>5} | {tr:<7} | HH=- HL=- LH=- LL=-")
    print("-" * 60)

def log_deltas_to_console(changes: List[dict]):
    print("\n[DEBUG] Changed rows:")
    for c in changes:
        pair = c["pair"]; tf = c["timeframe"]; tr = c["trend"]; iso = c["last_ts_iso"]
        if tr == "Bullish":
            print(f"  {pair:>8} | {tf:>5} | {tr:<7} | HH={fmt_num(c.get('hh'))} HL={fmt_num(c.get('hl'))} | {iso}")
        elif tr == "Bearish":
            print(f"  {pair:>8} | {tf:>5} | {tr:<7} | LH={fmt_num(c.get('lh'))} LL={fmt_num(c.get('ll'))} | {iso}")
        else:
            print(f"  {pair:>8} | {tf:>5} | {tr:<7} | HH=- HL=- LH=- LL=- | {iso}")
    print("-" * 60)

def fmt_num(x):
    if x is None:
        return "-"
    try:
        return f"{float(x):.6f}".rstrip("0").rstrip(".")
    except Exception:
        return str(x)

# ---------------------------
# WebSocket server
# ---------------------------

class TrendWSServer:
    def __init__(self, host: str, port: int, cfg: dict, pairs: List[str], tfs: List[str], pair_type_map: Dict[str,str]):
        self.host = host
        self.port = port
        self.cfg = cfg
        self.pairs = pairs
        self.tfs = tfs
        self.pair_type_map = pair_type_map
        self.clients: set[WebSocketServerProtocol] = set()
        self.server = None

    def _client_label(self, ws: WebSocketServerProtocol) -> str:
        addr = getattr(ws, "remote_address", None)
        if isinstance(addr, tuple) and len(addr) >= 2:
            return f"{addr[0]}:{addr[1]}"
        return "unknown-client"

    async def handler(self, ws: WebSocketServerProtocol):
        self.clients.add(ws)
        label = self._client_label(ws)
        print(f"[WS] Client connected: {label} (total={len(self.clients)})")
        try:
            hello = {"type": "hello", "protocol": "trend-v3"}
            hello_msg = json.dumps(hello)
            await ws.send(hello_msg)
            print(f"[SEND] -> {label} | hello ({len(hello_msg)} bytes)")

            # Fresh trend snapshot to THIS client
            try:
                with pg_conn(self.cfg) as conn:
                    rows = fetch_pairs_structure(conn, self.pairs, self.tfs)
                snapshot = make_snapshot(rows, self.pairs, self.tfs, self.pair_type_map)
                snap_msg = json.dumps(snapshot, separators=(",", ":"))
                await ws.send(snap_msg)
                majors = len(snapshot.get("majors", []))
                minors = len(snapshot.get("minors", []))
                others = len(snapshot.get("others", []))
                tfs_count = len(snapshot.get("meta", {}).get("timeframes", []))
                print(f"[SEND] -> {label} | snapshot ({len(snap_msg)} bytes) "
                      f"| pairs: majors={majors}, minors={minors}, others={others} | tfs={tfs_count}")
            except Exception as e:
                err = {"type": "error", "message": f"snapshot failed: {e}"}
                await ws.send(json.dumps(err))
                print(f"[SEND] -> {label} | error snapshot: {e}")

            # Fresh AOI snapshot to THIS client
            try:
                with pg_conn(self.cfg) as conn:
                    aoi_rows = fetch_aoi_rows(conn, self.pairs)
                aoi_snap = make_aoi_snapshot(aoi_rows)
                aoi_msg = json.dumps(aoi_snap, separators=(",", ":"))
                await ws.send(aoi_msg)
                print(f"[SEND] -> {label} | aoi_snapshot ({len(aoi_msg)} bytes)")
            except Exception as e:
                err = {"type": "error", "message": f"aoi snapshot failed: {e}"}
                await ws.send(json.dumps(err))
                print(f"[SEND] -> {label} | error aoi_snapshot: {e}")

            async for _ in ws:  # ignore incoming messages
                pass
        finally:
            self.clients.discard(ws)
            print(f"[WS] Client disconnected: {label} (total={len(self.clients)})")

    async def start(self):
        self.server = await websockets.serve(self.handler, self.host, self.port, ping_interval=20, ping_timeout=20)
        print(f"[WS] Listening on ws://{self.host}:{self.port}")

    async def broadcast(self, payload: dict):
        if not self.clients:
            return
        msg = json.dumps(payload, separators=(",", ":"))
        ptype = payload.get("type", "unknown")
        print(f"[BROADCAST] {ptype} -> {len(self.clients)} client(s) | {len(msg)} bytes")
        await asyncio.gather(*(self._send_one(c, msg, ptype) for c in list(self.clients)))

    async def _send_one(self, ws: WebSocketServerProtocol, msg: str, ptype: str):
        label = self._client_label(ws)
        try:
            await ws.send(msg)
            print(f"[SEND] -> {label} | {ptype} ({len(msg)} bytes) OK")
        except Exception as e:
            print(f"[SEND] -> {label} | {ptype} FAILED: {e}")

# ---------------------------
# Main loop
# ---------------------------

async def main():
    cfg = load_env()
    pairs, pair_type_map = parse_pairs(cfg["PAIRS_FILE"])
    tfs = parse_timeframes(cfg["TIMEFRAMES_FILE"])

    ws_server = TrendWSServer(cfg["WS_HOST"], cfg["WS_PORT"], cfg, pairs, tfs, pair_type_map)
    await ws_server.start()

    print("[LOOP] Polling started.")

    # In-memory alert state
    last_15m_ts_by_pair: Dict[str, int] = {p: 0 for p in pairs}
    sent_alert_keys: set[Tuple[str, str, float, float, int]] = set()
    last_event_dt: Dict[Tuple[str, str, float, float], datetime] = {}

    # 1) Initial snapshots (broadcast to already-connected clients)
    try:
        with pg_conn(cfg) as conn:
            rows = fetch_pairs_structure(conn, pairs, tfs)
            aoi_rows = fetch_aoi_rows(conn, pairs)

        # For logging trends
        rows_index: Dict[Tuple[str, str], dict] = {}
        for r in rows:
            pair = r["pair"].upper()
            tf = r["timeframe"].lower()
            rows_index[(pair, tf)] = {"hh": r["hh"], "hl": r["hl"], "lh": r["lh"], "ll": r["ll"]}

        snapshot = make_snapshot(rows, pairs, tfs, pair_type_map)
        await ws_server.broadcast(snapshot)
        print("[UPDATE] Initial snapshot broadcasted.")
        log_snapshot_to_console(snapshot, rows_index)

        # AOI initial broadcast
        aoi_snap = make_aoi_snapshot(aoi_rows)
        await ws_server.broadcast(aoi_snap)
        print("[UPDATE] Initial AOI snapshot broadcasted.")

        # Seed signatures
        prev_cell_sig = seed_prev_signatures(rows)
        prev_aoi_sig = seed_prev_aoi_signatures(aoi_rows)

        # Seed last_15m_ts_by_pair
        with pg_conn(cfg) as conn:
            for p in pairs:
                tname = tbl_15m_for(p)
                try:
                    with conn.cursor() as cur:
                        cur.execute(f"SELECT COALESCE(MAX(ts), 0) FROM {tname}")
                        last_ts = int(cur.fetchone()[0] or 0)
                    last_15m_ts_by_pair[p] = last_ts
                except Exception as e:
                    print(f"[WARN] seed 15m ts for {p} failed: {e}")

    except Exception as e:
        print(f"[ERROR] Initial load failed: {e}")
        prev_cell_sig = {}
        prev_aoi_sig = {}

    # 2) Continuous delta updates + AOI alerts
    try:
        while True:
            await asyncio.sleep(cfg["POLL_INTERVAL_SEC"])

            # Trends
            try:
                with pg_conn(cfg) as conn:
                    rows = fetch_pairs_structure(conn, pairs, tfs)
                changes, prev_cell_sig = diff_rows(prev_cell_sig, rows)
                if changes:
                    log_deltas_to_console(changes)
                    await ws_server.broadcast({"type": "delta", "changes": changes})
                    print(f"[UPDATE] {len(changes)} trend change(s) broadcasted.")
            except Exception as e:
                print(f"[ERROR] Polling (trends) failed: {e}")

            # AOI (snapshot/delta)
            try:
                with pg_conn(cfg) as conn:
                    aoi_rows = fetch_aoi_rows(conn, pairs)
                aoi_delta, prev_aoi_sig = diff_aoi_rows(prev_aoi_sig, aoi_rows)
                if aoi_delta:
                    await ws_server.broadcast(aoi_delta)
                    up = len(aoi_delta["changes"]["upserts"])
                    dn = len(aoi_delta["changes"]["deletes"])
                    print(f"[AOI UPDATE] upserts={up}, deletes={dn}")
            except Exception as e:
                print(f"[ERROR] Polling (AOI) failed: {e}")

            # AOI Alerts 15m (no DB, ephemeral)
            try:
                if not cfg["ALERTS_ENABLED"]:
                    continue

                zones_by_pair = select_zones_for_alerts(
                    aoi_rows, cfg["ALERTS_ZONE_TFS"], cfg["ALERTS_TOP_N"]
                )
                now_dt = datetime.utcnow()
                cooldown_delta = timedelta(minutes=cfg["ALERTS_COOLDOWN_MIN"])

                with pg_conn(cfg) as conn:
                    for pair in pairs:
                        cand_new = fetch_new_15m_candles(conn, pair, last_15m_ts_by_pair.get(pair, 0))
                        if not cand_new:
                            continue
                        last_15m_ts_by_pair[pair] = max(last_15m_ts_by_pair.get(pair, 0), cand_new[-1][0])

                        zlist = zones_by_pair.get(pair, [])
                        if not zlist:
                            continue

                        for ts, o, h, l, c in cand_new:
                            for z in zlist:
                                zl, zh, ztf = z["low"], z["high"], z["tf"]
                                cache_key = (pair, ztf, zl, zh, ts)
                                if cache_key in sent_alert_keys:
                                    continue

                                evt = classify_aoi_event(pair, ztf, zl, zh, o, h, l, c,
                                                         cfg["ALERTS_MIN_OVERLAP_PIPS"])
                                if not evt:
                                    if cfg["ALERTS_LOG_BODY"]:
                                        print(f"[ALERT-DEBUG] {pair} {ztf} {zl}-{zh} @ {ts_ms_to_iso(ts)} no hit.")
                                    continue

                                zone_key = (pair, ztf, zl, zh)
                                last_dt = last_event_dt.get(zone_key)
                                if last_dt and (now_dt - last_dt) < cooldown_delta:
                                    if cfg["ALERTS_LOG_BODY"]:
                                        print(f"[ALERT-COOLDOWN] skip {pair} {ztf} {zl}-{zh} at {ts_ms_to_iso(ts)}")
                                    continue

                                payload = {
                                    "type": "aoi_alert",
                                    "event": {
                                        "pair": pair,
                                        "zone_tf": ztf,
                                        "low": zl,
                                        "high": zh,
                                        "candle_tf": "15m",
                                        "ts": ts,
                                        "ts_iso": ts_ms_to_iso(ts),
                                        "event_type": evt["event_type"],
                                        "direction": evt["direction"],
                                        "o": o, "h": h, "l": l, "c": c,
                                        "body_overlap_pips": evt["body_overlap_pips"],
                                        "wick_overlap_pips": evt["wick_overlap_pips"],
                                    }
                                }
                                await ws_server.broadcast(payload)
                                sent_alert_keys.add(cache_key)
                                last_event_dt[zone_key] = now_dt
                                print(f"[ALERT] {pair} {ztf} {zl}-{zh} {evt['event_type']} at {ts_ms_to_iso(ts)}")

            except Exception as e:
                print(f"[ERROR] Alerts 15m step failed: {e}")

    except asyncio.CancelledError:
        print("[EXIT] Cancelled.")
    except KeyboardInterrupt:
        print("[EXIT] KeyboardInterrupt.")
    finally:
        print("[EXIT] Stopping server.")

if __name__ == "__main__":
    asyncio.run(main())
