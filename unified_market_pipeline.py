#!/usr/bin/env python3
# unified_market_pipeline.py
"""
Pipeline unifié:
1) Fetch MT5 OHLCV (+Heikin-Ashi) en continu pour toutes (pair, timeframe) de pairs.txt / timeframes.txt.
2) Si nouvelles bougies: mise à jour incrémentale de pairs_structure (HH/HL/LH/LL + structure_point_* + last_analyzed_ts).
3) Calcul des AOI (Weekly 1W, Daily 1D) + MTF validées (1D∩1W) et stockage dans aoi_zones.
4) Push WS au front (snapshot initial, puis deltas dès qu'il y a des changements).

ENV (.env) supporte les 2 styles:
  PGHOST|PG_HOST, PGPORT|PG_PORT, PGDATABASE|PG_DB, PGUSER|PG_USER, PGPASSWORD|PG_PASSWORD, PGSSLMODE|PG_SSLMODE
  WS_HOST, WS_PORT, POLL_INTERVAL_SEC, PAIRS_FILE, TIMEFRAMES_FILE
  SERVER_OFFSET_HOURS (par défaut 3 pour MT5 UTC+3)

Dependencies:
  pip install MetaTrader5 psycopg2-binary python-dotenv websockets sqlalchemy
"""

import os, re, csv, sys, math, json, time, asyncio
from typing import Any, Dict, List, Tuple, Optional
from datetime import datetime, timezone, timedelta

import MetaTrader5 as mt5
import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import websockets
from websockets.server import WebSocketServerProtocol

# ---------- ENV & CONST ----------
load_dotenv()
UTC = timezone.utc
SERVER_OFFSET_HOURS = int(os.getenv("SERVER_OFFSET_HOURS", "3"))
SERVER_OFFSET = timedelta(hours=SERVER_OFFSET_HOURS)
SERVER_OFFSET_MS = SERVER_OFFSET_HOURS * 60 * 60 * 1000

WS_HOST = os.getenv("WS_HOST", "0.0.0.0")
WS_PORT = int(os.getenv("WS_PORT", "8765"))
POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "2"))
PAIRS_FILE = os.getenv("PAIRS_FILE", "pairs.txt")
TIMEFRAMES_FILE = os.getenv("TIMEFRAMES_FILE", "timeframes.txt")

BATCH_BARS = 5000
MAX_EMPTY_LOOPS = 3
TF_MS = {
    "1m":60_000,"3m":180_000,"5m":300_000,"15m":900_000,"30m":1_800_000,
    "1h":3_600_000,"2h":7_200_000,"4h":14_400_000,"6h":21_600_000,"8h":28_800_000,
    "12h":43_200_000,"1d":86_400_000,"1w":604_800_000
}
def timeframe_to_ms(tf:str)->int:
    if tf not in TF_MS: raise ValueError(f"Unsupported TF {tf}")
    return TF_MS[tf]

# MT5 TF mapping
TF_MT5 = {
    "1m": mt5.TIMEFRAME_M1, "3m": mt5.TIMEFRAME_M3, "5m": mt5.TIMEFRAME_M5, "15m": mt5.TIMEFRAME_M15,
    "30m": mt5.TIMEFRAME_M30, "1h": mt5.TIMEFRAME_H1, "2h": mt5.TIMEFRAME_H2, "4h": mt5.TIMEFRAME_H4,
    "6h": mt5.TIMEFRAME_H6, "8h": mt5.TIMEFRAME_H8, "12h": mt5.TIMEFRAME_H12,
    "1d": mt5.TIMEFRAME_D1, "1w": mt5.TIMEFRAME_W1
}
def timeframe_to_mt5(tf:str): 
    if tf not in TF_MT5: raise ValueError(f"Unsupported TF {tf}")
    return TF_MT5[tf]

# ---------- DB ----------
def _getenv_any(keys, default=None):
    for k in keys:
        v = os.getenv(k)
        if v not in (None, ""):
            return v
    return default

def pg_conn():
    return psycopg2.connect(
        host=_getenv_any(["PGHOST","PG_HOST"], "127.0.0.1"),
        port=int(_getenv_any(["PGPORT","PG_PORT"], "5432")),
        dbname=_getenv_any(["PGDATABASE","PG_DB"], "postgres"),
        user=_getenv_any(["PGUSER","PG_USER"], "postgres"),
        password=_getenv_any(["PGPASSWORD","PG_PASSWORD"], "postgres"),
        sslmode=_getenv_any(["PGSSLMODE","PG_SSLMODE"], "disable"),
    )

def sanitize_name(s:str)->str:
    return re.sub(r'[^a-z0-9]+','_',s.lower()).strip('_')[:55]

def split_symbol(sym:str)->Tuple[str,str,str]:
    s = sym.upper().replace("/", "").strip()
    return s, s[:3], s[3:]

def pip_size_for(pair:str)->float:
    return 0.01 if pair.upper().endswith("JPY") else 0.0001

def fmt_num(x):
    if x is None: return "-"
    try: return f"{float(x):.6f}".rstrip("0").rstrip(".")
    except: return str(x)

def ts_ms_to_iso(ts_ms: Optional[int]) -> Optional[str]:
    if not ts_ms: return None
    try: return datetime.fromtimestamp(int(ts_ms)/1000, tz=UTC).isoformat()
    except: return None

# ---------- DDL ----------
DDL_CANDLES = """
CREATE TABLE IF NOT EXISTS {tname}(
  ts BIGINT PRIMARY KEY,
  ts_timestamptz TIMESTAMPTZ,
  ts_utc TEXT,
  ts_bangkok TEXT,
  open DOUBLE PRECISION, high DOUBLE PRECISION, low DOUBLE PRECISION, close DOUBLE PRECISION,
  volume DOUBLE PRECISION,
  open_ha DOUBLE PRECISION, high_ha DOUBLE PRECISION, low_ha DOUBLE PRECISION, close_ha DOUBLE PRECISION,
  exchange TEXT, symbol TEXT, base TEXT, quote TEXT, timeframe TEXT
);
"""

DDL_PAIRS_STRUCTURE = """
CREATE TABLE IF NOT EXISTS pairs_structure(
  pair TEXT NOT NULL,
  timeframe TEXT NOT NULL,
  hh DOUBLE PRECISION NOT NULL DEFAULT 0,
  hl DOUBLE PRECISION NOT NULL DEFAULT 0,
  lh DOUBLE PRECISION NOT NULL DEFAULT 0,
  ll DOUBLE PRECISION NOT NULL DEFAULT 0,
  structure_point_low  DOUBLE PRECISION NOT NULL DEFAULT 0,
  structure_point_high DOUBLE PRECISION NOT NULL DEFAULT 0,
  last_analyzed_ts BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY(pair,timeframe)
);
"""

DDL_AOI_ZONES = """
CREATE TABLE IF NOT EXISTS aoi_zones(
  id BIGSERIAL PRIMARY KEY,
  pair TEXT NOT NULL,
  tf TEXT NOT NULL,               -- '1w', '1d', ou 'mtf'
  low DOUBLE PRECISION NOT NULL,
  high DOUBLE PRECISION NOT NULL,
  mid DOUBLE PRECISION NOT NULL,
  height_pips DOUBLE PRECISION NOT NULL,
  count_points INTEGER,
  score DOUBLE PRECISION,
  last_ts BIGINT,
  pts_w INTEGER,
  pts_d INTEGER,
  overlap_ratio DOUBLE PRECISION,
  score_w DOUBLE PRECISION,
  score_d DOUBLE PRECISION,
  score_final DOUBLE PRECISION,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
"""

def ensure_core_tables(conn, candle_tname:str):
    with conn.cursor() as cur:
        cur.execute(DDL_CANDLES.format(tname=candle_tname))
        cur.execute(DDL_PAIRS_STRUCTURE)
        cur.execute(DDL_AOI_ZONES)
    conn.commit()

# ---------- MT5 helpers ----------
def mt5_init_or_die():
    if not mt5.initialize():
        code, msg = mt5.last_error()
        raise SystemExit(f"[MT5] init failed: {code} {msg}")
    if mt5.account_info() is None:
        code, msg = mt5.last_error()
        raise SystemExit(f"[MT5] No account: {code} {msg}")

def resolve_mt5_symbol(base:str, quote:str)->Optional[str]:
    cands = [f"{base}{quote}", f"{base}{quote}.a", f"{base}{quote}.i", f"{base}{quote}.pro", f"{base}{quote}.ecn"]
    for c in cands:
        info = mt5.symbol_info(c)
        if info:
            mt5.symbol_select(c, True)
            return c
    return None

def copy_rates_range_server(symbol:str, tf_str:str, utc_from:datetime, utc_to:datetime):
    tf_mt5 = timeframe_to_mt5(tf_str)
    server_from = (utc_from + SERVER_OFFSET).replace(tzinfo=None)
    server_to   = (utc_to   + SERVER_OFFSET).replace(tzinfo=None)
    try:
        rates = mt5.copy_rates_range(symbol, tf_mt5, server_from, server_to)
    except Exception as e:
        print(f"[ERR] copy_rates_range({symbol},{tf_str}): {e} last={mt5.last_error()}", file=sys.stderr)
        return []
    return [] if rates is None else list(rates)

def filter_closed_bars_server(rates, tf_ms:int):
    now_utc = datetime.now(tz=UTC)
    now_server_ms = int(now_utc.timestamp()*1000) + SERVER_OFFSET_MS
    out=[]
    for r in rates:
        t_ms_server = int(r['time'])*1000
        if (t_ms_server + tf_ms) <= now_server_ms:
            out.append(r)
    return out

def compute_ha(o,h,l,c,prev_open_ha,prev_close_ha):
    close_ha = (o+h+l+c)/4.0
    open_ha  = (prev_open_ha+prev_close_ha)/2.0 if prev_open_ha is not None else (o+c)/2.0
    high_ha  = max(h, open_ha, close_ha)
    low_ha   = min(l, open_ha, close_ha)
    return open_ha, high_ha, low_ha, close_ha

# ---------- IO files ----------
def parse_pairs(path:str)->Tuple[List[str], Dict[str,str]]:
    seen=set(); ordered=[]; ptype={}
    with open(path, newline="", encoding="utf-8") as f:
        r = csv.DictReader(f)
        for row in r:
            t=(row.get("type") or row.get("TYPE") or row.get("Type") or "").strip().upper()
            p=(row.get("pair") or row.get("PAIR") or row.get("Pair") or "").strip()
            if not p: continue
            p=p.upper().replace("/","")
            if p not in seen:
                ordered.append(p); seen.add(p)
            if t: ptype[p]=t
    if not ordered: raise ValueError("pairs.txt invalid (need header type,pair)")
    return ordered, ptype

def parse_timeframes(path:str)->List[str]:
    out=[]
    with open(path,"r",encoding="utf-8") as f:
        for line in f:
            s=line.strip().lower()
            if s and not s.startswith("#"): out.append(s)
    if not out: raise ValueError("timeframes.txt empty.")
    return out

# ---------- Structure logic ----------
def compute_trend(hh,hl,lh,ll):
    hh,hl,lh,ll = hh or 0,hl or 0,lh or 0,ll or 0
    if hh>0 and hl>0 and lh==0 and ll==0: return "Bullish"
    if lh>0 and ll>0 and hh==0 and hl==0: return "Bearish"
    return "Neutral"

def get_existing_state(cur, pair:str, timeframe:str):
    cur.execute("""SELECT hh,hl,lh,ll,structure_point_low,structure_point_high,last_analyzed_ts
                   FROM pairs_structure WHERE pair=%s AND timeframe=%s""",(pair,timeframe))
    row=cur.fetchone()
    if row is None: return None
    return {
        "hh": float(row[0]), "hl": float(row[1]), "lh": float(row[2]), "ll": float(row[3]),
        "spl": float(row[4]), "sph": float(row[5]), "last_ts": int(row[6])
    }

def upsert_state(cur, pair, timeframe, hh,hl,lh,ll, spl,sph, last_ts):
    cur.execute("""
        INSERT INTO pairs_structure(pair,timeframe,hh,hl,lh,ll,structure_point_low,structure_point_high,last_analyzed_ts)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (pair,timeframe) DO UPDATE SET
          hh=EXCLUDED.hh, hl=EXCLUDED.hl, lh=EXCLUDED.lh, ll=EXCLUDED.ll,
          structure_point_low=EXCLUDED.structure_point_low,
          structure_point_high=EXCLUDED.structure_point_high,
          last_analyzed_ts=EXCLUDED.last_analyzed_ts
    """,(pair,timeframe,hh,hl,lh,ll,spl,sph,last_ts))

def incremental_update_structure(conn, tname:str, pair:str, timeframe:str)->int:
    """
    Reprend fidèlement ton update incrémental (open/close).
    Retourne le dernier ts traité (0 si rien).
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # ensure table exists
        cur.execute("""SELECT to_regclass(%s)""",(tname,))
        if cur.fetchone()[0] is None:
            print(f"[STRUCT] table {tname} missing, skip.")
            return 0

        # ensure pairs_structure table exists
        cur.execute(DDL_PAIRS_STRUCTURE)

        state = get_existing_state(cur, pair, timeframe)
        last_ts = state["last_ts"] if state else 0

        # previous_close
        previous_close=None
        if last_ts>0:
            cur.execute(f"SELECT close FROM {tname} WHERE ts=%s ORDER BY ts DESC LIMIT 1",(last_ts,))
            r=cur.fetchone()
            if r is None:
                cur.execute(f"SELECT close FROM {tname} WHERE ts<=%s ORDER BY ts DESC LIMIT 1",(last_ts,))
                r=cur.fetchone()
            if r and r[0] is not None:
                previous_close=float(r[0])

        # fetch rows
        if last_ts>0:
            cur.execute(f"SELECT ts,open,close FROM {tname} WHERE close IS NOT NULL AND ts>%s ORDER BY ts ASC",(last_ts,))
        else:
            cur.execute(f"SELECT ts,open,close FROM {tname} WHERE close IS NOT NULL ORDER BY ts ASC")
        rows=cur.fetchall()
        if not rows:
            if state is None:
                upsert_state(cur, pair, timeframe, 0,0,0,0, 0,0, last_ts)
            conn.commit()
            return last_ts

        # init bootstrap / resume
        if state is None:
            first_ts=int(rows[0]["ts"]); first_o=float(rows[0]["open"]); first_c=float(rows[0]["close"])
            if first_c>first_o:
                mode="bullish"; hh=first_c; hl=first_o; lh=0.0; ll=0.0; spl=hl; sph=hh
            else:
                mode="bearish"; hh=0.0; hl=0.0; sph=first_o; spl=first_c; lh=sph; ll=spl
            previous_close=first_c; last_ts=first_ts
            upsert_state(cur, pair, timeframe, hh,hl,lh,ll, spl,sph, last_ts)
            it=rows[1:]
        else:
            hh,hl,lh,ll = state["hh"],state["hl"],state["lh"],state["ll"]
            spl,sph = state["spl"],state["sph"]
            mode = "bullish" if hh>0 else "bearish"
            it=rows
            if previous_close is None and it:
                previous_close=float(it[0]["open"])

        for r in it:
            ts=int(r["ts"]); close=float(r["close"])
            has_change=False
            if mode=="bullish":
                if close>hh:
                    hh=close; hl=spl; lh=0.0; ll=0.0; has_change=True
                elif (close<hh and close>hl and previous_close is not None and close<previous_close):
                    spl=close; has_change=True
                elif (close<hh and close>hl and previous_close is not None and close>previous_close):
                    sph=close; has_change=True
                else:
                    hh=0.0; hl=0.0; lh=sph; ll=close; spl=close; mode="bearish"; has_change=True
            else:
                if close<ll:
                    hh=0.0; hl=0.0; lh=sph; ll=close; has_change=True
                elif (close<lh and close>ll and previous_close is not None and close<previous_close):
                    spl=close; has_change=True
                elif (close<lh and close>ll and previous_close is not None and close>previous_close):
                    sph=close; has_change=True
                else:
                    hh=close; hl=spl; lh=0.0; ll=0.0; mode="bullish"; has_change=True

            last_ts=ts; previous_close=close
            upsert_state(cur, pair, timeframe, hh,hl,lh,ll, spl,sph, last_ts)

        conn.commit()
        return last_ts

# ---------- AOI logic ----------
def candle_dir(o,c): return +1 if c>o else (-1 if c<o else 0)

def lookback_min_ts_ms(years:int)->int:
    now_ms=int(datetime.now(tz=UTC).timestamp()*1000)
    return now_ms - int(52*years*7*24*3600*1000)

def fetch_structure_bounds(cur, pair, timeframe)->Tuple[str,float,float,Optional[float],Optional[float]]:
    cur.execute("""SELECT hh,hl,lh,ll FROM pairs_structure WHERE pair=%s AND timeframe=%s""",(pair, timeframe))
    r=cur.fetchone()
    if not r: raise ValueError(f"No structure for {pair} {timeframe}")
    hh,hl,lh,ll = [float(x) for x in r]
    trend = compute_trend(hh,hl,lh,ll)
    if trend=="Bullish":
        return "Bullish", float(hl), float(hh), float(hl), None
    if trend=="Bearish":
        return "Bearish", float(ll), float(lh), None, float(lh)
    raise ValueError("Neutral → no AOI.")

def fetch_candles_for_aoi(cur, table, min_ts_ms):
    cur.execute(f"SELECT ts,open,high,low,close FROM {table} WHERE ts>=%s ORDER BY ts ASC",(min_ts_ms,))
    rows=cur.fetchall()
    candles=[(int(ts),float(o),float(h),float(l),float(c)) for ts,o,h,l,c in rows]
    for i in range(1,len(candles)):
        ts,_,h,l,c=candles[i]
        candles[i]=(ts,candles[i-1][4],h,l,c)
    return candles

def build_structure_points(candles):
    pts=[]
    for i in range(1,len(candles)):
        ts_prev,o_prev,h_prev,l_prev,c_prev=candles[i-1]
        ts_i,o_i,h_i,l_i,c_i=candles[i]
        d_prev,d_i=candle_dir(o_prev,c_prev),candle_dir(o_i,c_i)
        if d_prev!=0 and d_i!=0 and d_i==-d_prev:
            pts.append((ts_i,c_prev))
    return pts

def score_points(points, recency_k:float)->Tuple[float,int]:
    now=datetime.now(tz=UTC); s=0.0; last=0
    for ts,_ in points:
        last=max(last,ts)
        age_weeks=(now-datetime.fromtimestamp(ts/1000,tz=UTC)).days/7
        s+=math.exp(-recency_k*age_weeks)
    return s,last

def build_anchors_bullish(pts, hl, low_tf, high_tf):
    vals=set()
    if hl is not None and low_tf<=hl<=high_tf: vals.add(hl)
    for _,px in pts:
        if low_tf<=px<=high_tf: vals.add(px)
    anchors=sorted(vals, reverse=True)
    # exclure HH (bord haut)
    anchors=[a for a in anchors if a<high_tf or abs(a-high_tf)>0.0]
    return anchors

def build_anchors_bearish(pts, lh, low_tf, high_tf):
    vals=set()
    if lh is not None and low_tf<=lh<=high_tf: vals.add(lh)
    for _,px in pts:
        if low_tf<=px<=high_tf: vals.add(px)
    anchors=sorted(vals)
    # exclure LL (bord bas)
    anchors=[a for a in anchors if a>low_tf or abs(a-low_tf)>0.0]
    return anchors

def find_aois_bullish_top_down(pts, low_tf, high_tf, hl_val, pip_size, min_pips, max_pips, recency_k, max_results):
    P=[p for p in pts if low_tf<=p[1]<=high_tf]
    if not P and not (hl_val is not None and low_tf<=hl_val<=high_tf): return []
    anchors=build_anchors_bullish(P, hl_val, low_tf, high_tf)
    out=[]; ceiling=high_tf
    for a in anchors:
        if a>ceiling: continue
        best=None
        for h in range(int(min_pips), int(max_pips)+1):
            low=a-h*pip_size; high=a
            if low<low_tf: continue
            pts_win=[p for p in P if low<=p[1]<=high]
            if len(pts_win)<3: continue
            score,last=score_points(pts_win, recency_k)
            cand=(score,-h,low,high,len(pts_win),last)
            if best is None or cand>best: best=cand
        if best:
            score,neg_h,low,high,count,last=best; h=-neg_h
            out.append({"low":low,"high":high,"mid":(low+high)/2,"height_pips":float(h),
                        "count_points":int(count),"score":float(score),"last_ts":int(last)})
            ceiling=low
            if len(out)>=max_results: break
    return out

def find_aois_bearish_bottom_up(pts, low_tf, high_tf, lh_val, pip_size, min_pips, max_pips, recency_k, max_results):
    P=[p for p in pts if low_tf<=p[1]<=high_tf]
    if not P and not (lh_val is not None and low_tf<=lh_val<=high_tf): return []
    anchors=build_anchors_bearish(P, lh_val, low_tf, high_tf)
    out=[]; floor=low_tf
    for a in anchors:
        if a<floor: continue
        best=None
        for h in range(int(min_pips), int(max_pips)+1):
            low=a; high=a+h*pip_size
            if high>high_tf: continue
            pts_win=[p for p in P if low<=p[1]<=high]
            if len(pts_win)<3: continue
            score,last=score_points(pts_win, recency_k)
            cand=(score,-h,low,high,len(pts_win),last)
            if best is None or cand>best: best=cand
        if best:
            score,neg_h,low,high,count,last=best; h=-neg_h
            out.append({"low":low,"high":high,"mid":(low+high)/2,"height_pips":float(h),
                        "count_points":int(count),"score":float(score),"last_ts":int(last)})
            floor=high
            if len(out)>=max_results: break
    return out

def zone_intersection(a_low, a_high, b_low, b_high):
    low=max(a_low,b_low); high=min(a_high,b_high)
    return (low,high) if low<=high else None

def zone_length(low,high): return max(0.0, high-low)

def count_points_in_zone(points, low, high):
    return [(ts,px) for (ts,px) in points if (low<=px<=high)]

def build_mtf_validated_zones(aois_w, struct_w, aois_d, struct_d, pip_size, recency_k,
                              overlap_min=0.70, min_pts_w=3, min_pts_d=5,
                              wW=0.7, wD=0.3, compact_ceiling_pips=40.0,
                              global_min_gap_pips=20.0, global_max_overlap=0.30):
    cands=[]
    for d in aois_d:
        d_len = zone_length(d['low'], d['high'])
        if d_len<=0: continue
        best=None
        for w in aois_w:
            inter = zone_intersection(d['low'], d['high'], w['low'], w['high'])
            if not inter: continue
            i_low,i_high = inter
            inter_len = zone_length(i_low,i_high)
            if inter_len<=0: continue
            overlap_ratio = inter_len / d_len
            if overlap_ratio < overlap_min: continue
            pts_w = count_points_in_zone(struct_w, i_low, i_high)
            pts_d = count_points_in_zone(struct_d, i_low, i_high)
            if len(pts_w)<min_pts_w or len(pts_d)<min_pts_d: continue
            score_w,_ = score_points(pts_w, recency_k)
            score_d,_ = score_points(pts_d, recency_k)
            height_pips=(i_high-i_low)/pip_size
            compact_penalty = min(1.0, (compact_ceiling_pips/max(height_pips,1e-9)))
            score_final = (wW*score_w + wD*score_d)*compact_penalty
            cand={"low":i_low,"high":i_high,"pts_w":len(pts_w),"pts_d":len(pts_d),
                  "overlap_ratio":overlap_ratio,"score_w":score_w,"score_d":score_d,
                  "score_final":score_final}
            if best is None or cand["score_final"]>best["score_final"]: best=cand
        if best: cands.append(best)
    cands.sort(key=lambda x: -x["score_final"])
    # anti-chevauchement global (glouton)
    selected=[]
    for z in cands:
        ok=True
        for s in selected:
            inter=zone_intersection(z["low"],z["high"],s["low"],s["high"])
            if inter:
                inter_len=zone_length(*inter)
                overlap_rel = inter_len / max(1e-12, min(zone_length(z["low"],z["high"]), zone_length(s["low"],s["high"])))
                if overlap_rel>global_max_overlap: ok=False; break
            else:
                gap = max(s["low"]-z["high"], z["low"]-s["high"])
                if (gap/pip_size) < global_min_gap_pips: ok=False; break
        if ok: selected.append(z)
    return selected

def aoi_compute_and_store(conn, pair:str, exchange:str, recency_k=0.05, min_pips=5, max_pips=60,
                          overlap_min=0.70, min_pts_w=3, min_pts_d=5, wW=0.7, wD=0.3,
                          compact_ceiling_pips=40.0, global_min_gap_pips=20.0, global_max_overlap=0.30,
                          max_results=10):
    base = pair.split("/")[0] if "/" in pair else pair
    pip_sz = pip_size_for(pair)
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # structure + bougies W
        trend_w, low_w, high_w, hl_val, _ = fetch_structure_bounds(cur, pair, "1w")
        t_w = f"candles_{sanitize_name(exchange)}_{sanitize_name(base)}_1w"
        years_w=5; min_ts_w = lookback_min_ts_ms(years_w)
        candles_w = fetch_candles_for_aoi(cur, t_w, min_ts_w)
        struct_pts_w = build_structure_points(candles_w)
        if trend_w=="Bullish":
            aois_w = find_aois_bullish_top_down(struct_pts_w, low_w, high_w, hl_val, pip_sz, min_pips, max_pips, recency_k, max_results)
        else:
            # bearish uses LH value
            trend_w, low_w, high_w, _, lh_val = fetch_structure_bounds(cur, pair, "1w")
            aois_w = find_aois_bearish_bottom_up(struct_pts_w, low_w, high_w, lh_val, pip_sz, min_pips, max_pips, recency_k, max_results)

        # structure + bougies D
        trend_d, low_d, high_d, hl_d, _ = fetch_structure_bounds(cur, pair, "1d")
        t_d = f"candles_{sanitize_name(exchange)}_{sanitize_name(base)}_1d"
        years_d=2; min_ts_d = lookback_min_ts_ms(years_d)
        candles_d = fetch_candles_for_aoi(cur, t_d, min_ts_d)
        struct_pts_d = build_structure_points(candles_d)
        if trend_d=="Bullish":
            aois_d = find_aois_bullish_top_down(struct_pts_d, low_d, high_d, hl_d, pip_sz, min_pips, max_pips, recency_k, max_results)
        else:
            trend_d, low_d, high_d, _, lh_d = fetch_structure_bounds(cur, pair, "1d")
            aois_d = find_aois_bearish_bottom_up(struct_pts_d, low_d, high_d, lh_d, pip_sz, min_pips, max_pips, recency_k, max_results)

        # MTF
        aois_mtf = build_mtf_validated_zones(
            aois_w=aois_w, struct_w=struct_pts_w, aois_d=aois_d, struct_d=struct_pts_d,
            pip_size=pip_sz, recency_k=recency_k, overlap_min=overlap_min, min_pts_w=min_pts_w,
            min_pts_d=min_pts_d, wW=wW, wD=wD, compact_ceiling_pips=compact_ceiling_pips,
            global_min_gap_pips=global_min_gap_pips, global_max_overlap=global_max_overlap
        )

        # purge existant pour la paire et reinsert (simple et fiable)
        cur.execute("DELETE FROM aoi_zones WHERE pair=%s", (pair,))
        # insert 1w/1d
        rows=[]
        for a in aois_w:
            rows.append( (pair,"1w",a["low"],a["high"],a["mid"],a["height_pips"],a["count_points"],a["score"],a["last_ts"], None,None,None,None,None,None) )
        for a in aois_d:
            rows.append( (pair,"1d",a["low"],a["high"],a["mid"],a["height_pips"],a["count_points"],a["score"],a["last_ts"], None,None,None,None,None,None) )
        # insert mtf
        for z in aois_mtf:
            height_pips=(z["high"]-z["low"])/pip_sz
            rows.append( (pair,"mtf",z["low"],z["high"],(z["low"]+z["high"])/2, height_pips,
                          None,None,None, z["pts_w"],z["pts_d"], z["overlap_ratio"], z["score_w"], z["score_d"], z["score_final"]) )
        if rows:
            execute_values(cur, """
                INSERT INTO aoi_zones(pair,tf,low,high,mid,height_pips,count_points,score,last_ts,
                                      pts_w,pts_d,overlap_ratio,score_w,score_d,score_final)
                VALUES %s
            """, rows)
    conn.commit()

# ---------- Fetch & Insert candles ----------
def get_last_ts_and_ha_seed(cur, tname:str):
    cur.execute(f"SELECT ts,open_ha,close_ha FROM {tname} ORDER BY ts DESC LIMIT 1")
    r=cur.fetchone()
    if r: return int(r[0]), (r[1], r[2])
    return None, (None,None)

def format_ts_components(ts_ms:int):
    dt_utc = datetime.fromtimestamp(ts_ms/1000, tz=UTC)
    utc_iso = dt_utc.isoformat(timespec="seconds")
    try:
        from zoneinfo import ZoneInfo
        bkk = dt_utc.astimezone(ZoneInfo("Asia/Bangkok")).isoformat(timespec="seconds")
    except Exception:
        bkk = ""
    return dt_utc, utc_iso, bkk

def insert_candles(conn, tname:str, rows:List[dict])->int:
    if not rows: return 0
    with conn.cursor() as cur:
        tpl = "(" + ",".join(["%s"]*18) + ")"
        vals = [
            (r["ts"], r["ts_timestamptz"], r["ts_utc"], r["ts_bangkok"],
             r["open"], r["high"], r["low"], r["close"], r["volume"],
             r["open_ha"], r["high_ha"], r["low_ha"], r["close_ha"],
             r["exchange"], r["symbol"], r["base"], r["quote"], r["timeframe"])
            for r in rows
        ]
        execute_values(cur, f"""
            INSERT INTO {tname}(
              ts,ts_timestamptz,ts_utc,ts_bangkok,
              open,high,low,close,volume,
              open_ha,high_ha,low_ha,close_ha,
              exchange,symbol,base,quote,timeframe
            ) VALUES %s
            ON CONFLICT (ts) DO NOTHING
        """, vals)
    conn.commit()
    return len(rows)

def fetch_new_candles_for_pair_tf(conn, pair:str, tf:str, exchange:str="mt5")->Tuple[int,int]:
    """
    Retourne (inserted_count, last_ts)
    """
    sym_raw, base, quote = split_symbol(pair)
    mt5_symbol = resolve_mt5_symbol(base, quote)
    if not mt5_symbol:
        print(f"[WARN] MT5 symbol not found for {pair}.")
        return 0, 0

    tname = f"candles_{sanitize_name(exchange)}_{sanitize_name(base)}_{sanitize_name(tf)}"
    ensure_core_tables(conn, tname)
    tf_ms = timeframe_to_ms(tf)

    with conn.cursor() as cur:
        last_ts, (prev_oha, prev_cha) = get_last_ts_and_ha_seed(cur, tname)

    if last_ts is not None:
        start_dt_utc = datetime.fromtimestamp((last_ts+1)/1000, tz=UTC)
    else:
        start_dt_utc = datetime(2000,1,1,tzinfo=UTC)

    now_utc = datetime.now(tz=UTC)
    end_dt_utc = (now_utc + SERVER_OFFSET) - timedelta(milliseconds=tf_ms)
    if start_dt_utc >= end_dt_utc:
        return 0, last_ts or 0

    total_inserted=0; empty_loops=0
    cursor_dt = start_dt_utc
    prev_open_ha, prev_close_ha = prev_oha, prev_cha

    while cursor_dt < end_dt_utc:
        window_ms = tf_ms * BATCH_BARS
        window_to = min(end_dt_utc, cursor_dt + timedelta(milliseconds=window_ms))
        rates = copy_rates_range_server(mt5_symbol, tf, cursor_dt, window_to)
        if not rates:
            empty_loops+=1
            if empty_loops>MAX_EMPTY_LOOPS: break
            cursor_dt = window_to
            continue
        empty_loops=0
        closed = filter_closed_bars_server(rates, tf_ms)
        rows=[]
        start_ms = int(cursor_dt.timestamp()*1000)
        for r in closed:
            t_sec = int(r['time'])
            o=float(r['open']); h=float(r['high']); l=float(r['low']); c=float(r['close'])
            ts = t_sec*1000 - SERVER_OFFSET_MS
            if ts < start_ms: continue
            rv = float(r['real_volume']) if 'real_volume' in r and float(r['real_volume'])>0 else float(r.get('tick_volume',0.0))
            oha,hha,lha,cha = compute_ha(o,h,l,c, prev_open_ha, prev_close_ha)
            prev_open_ha, prev_close_ha = oha, cha
            dt_utc, utc_iso, bkk_iso = format_ts_components(ts)
            rows.append({
                "ts":ts, "ts_timestamptz":dt_utc, "ts_utc":utc_iso, "ts_bangkok":bkk_iso,
                "open":o,"high":h,"low":l,"close":c,"volume":rv,
                "open_ha":oha,"high_ha":hha,"low_ha":lha,"close_ha":cha,
                "exchange":exchange,"symbol":mt5_symbol,"base":base,"quote":quote,"timeframe":tf
            })
        inserted = insert_candles(conn, tname, rows)
        total_inserted += inserted
        if rows:
            last_ts = rows[-1]["ts"]
            cursor_dt = datetime.fromtimestamp((last_ts+1)/1000, tz=UTC)
        else:
            cursor_dt = window_to
        time.sleep(0.02)

    return total_inserted, last_ts or 0

# ---------- WS (snapshot/deltas) ----------
def compute_trend_row(rec)->str:
    hh,hl,lh,ll = rec["hh"],rec["hl"],rec["lh"],rec["ll"]
    return compute_trend(hh,hl,lh,ll)

def fetch_pairs_structure(conn, pairs:List[str], tfs:List[str]):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute("""
            SELECT pair,timeframe,hh,hl,lh,ll,structure_point_low,structure_point_high,last_analyzed_ts
            FROM pairs_structure WHERE pair = ANY(%s) AND timeframe = ANY(%s)
        """,(pairs,tfs))
        return cur.fetchall()

def build_cell_signature(trend:str,last_ts, hh,hl,lh,ll, spl,sph)->str:
    return "|".join([
        trend or "", str(last_ts or 0),
        str(hh or 0.0), str(hl or 0.0), str(lh or 0.0), str(ll or 0.0),
        str(spl or 0.0), str(sph or 0.0)
    ])

def make_snapshot(rows, pairs, tfs, pair_type_map):
    idx={}; latest_ts_by_pair={}
    for r in rows:
        pair=r["pair"].upper(); tf=r["timeframe"].lower()
        trend=compute_trend(r["hh"],r["hl"],r["lh"],r["ll"])
        last_ts=int(r["last_analyzed_ts"] or 0)
        idx[(pair,tf)] = {
            "trend":trend,"hh":r["hh"],"hl":r["hl"],"lh":r["lh"],"ll":r["ll"],
            "structure_point_low":r["structure_point_low"],"structure_point_high":r["structure_point_high"],
            "last_ts":last_ts,"last_ts_iso":ts_ms_to_iso(last_ts)
        }
        latest_ts_by_pair[pair]=max(latest_ts_by_pair.get(pair,0), last_ts)

    def row_for_pair(p):
        by_tf={}; by_tf_full={}
        for tf in tfs:
            rec=idx.get((p,tf))
            if rec:
                by_tf[tf]=rec["trend"]; by_tf_full[tf]=rec
            else:
                by_tf[tf]="Bearish"; by_tf_full[tf]={
                    "trend":"Bearish","hh":None,"hl":None,"lh":None,"ll":None,
                    "structure_point_low":None,"structure_point_high":None,
                    "last_ts":None,"last_ts_iso":None
                }
        return {"pair":p,"by_tf":by_tf,"by_tf_full":by_tf_full,
                "last_ts":latest_ts_by_pair.get(p), "last_ts_iso":ts_ms_to_iso(latest_ts_by_pair.get(p))}

    majors=[]; minors=[]; others=[]
    for p in pairs:
        cat = pair_type_map.get(p,"")
        bucket = majors if cat=="MAJOR" else (minors if cat=="MINOR" else others)
        bucket.append(row_for_pair(p))
    return {"type":"snapshot","meta":{"timeframes":tfs,"version":3},
            "majors":majors,"minors":minors,"others":others}

def seed_prev_signatures(rows):
    prev={}
    for r in rows:
        pair=r["pair"].upper(); tf=r["timeframe"].lower()
        trend=compute_trend(r["hh"],r["hl"],r["lh"],r["ll"])
        sig=build_cell_signature(trend,int(r["last_analyzed_ts"] or 0),
                                 r["hh"],r["hl"],r["lh"],r["ll"],
                                 r["structure_point_low"],r["structure_point_high"])
        prev[(pair,tf)]=sig
    return prev

def diff_rows(prev_idx, rows):
    changes=[]; curr={}
    existing=set()
    for r in rows:
        pair=r["pair"].upper(); tf=r["timeframe"].lower()
        trend=compute_trend(r["hh"],r["hl"],r["lh"],r["ll"])
        last_ts=int(r["last_analyzed_ts"] or 0)
        sig=build_cell_signature(trend,last_ts,r["hh"],r["hl"],r["lh"],r["ll"],r["structure_point_low"],r["structure_point_high"])
        key=(pair,tf); curr[key]=sig; existing.add(key)
        if prev_idx.get(key)!=sig:
            changes.append({
                "pair":pair,"timeframe":tf,"trend":trend,
                "hh":r["hh"],"hl":r["hl"],"lh":r["lh"],"ll":r["ll"],
                "structure_point_low":r["structure_point_low"],
                "structure_point_high":r["structure_point_high"],
                "last_ts":last_ts,"last_ts_iso":ts_ms_to_iso(last_ts)
            })
    for key in list(prev_idx.keys()):
        if key not in existing:
            pair,tf=key
            changes.append({"pair":pair,"timeframe":tf,"trend":"Neutral",
                            "hh":None,"hl":None,"lh":None,"ll":None,
                            "structure_point_low":None,"structure_point_high":None,
                            "last_ts":None,"last_ts_iso":None})
            curr[key]=build_cell_signature("Neutral",0,0,0,0,0,0,0)
    return changes, curr

class TrendWSServer:
    def __init__(self, host, port, pairs, tfs, pair_type_map):
        self.host=host; self.port=port
        self.pairs=pairs; self.tfs=tfs; self.pair_type_map=pair_type_map
        self.clients:set[WebSocketServerProtocol]=set()
        self.server=None

    async def start(self):
        self.server = await websockets.serve(self.handler, self.host, self.port, ping_interval=20, ping_timeout=20)
        print(f"[WS] Listening on ws://{self.host}:{self.port}")

    def _label(self, ws): 
        addr=getattr(ws,"remote_address",None)
        return f"{addr[0]}:{addr[1]}" if isinstance(addr,tuple) and len(addr)>=2 else "client"

    async def handler(self, ws:WebSocketServerProtocol):
        self.clients.add(ws); label=self._label(ws)
        print(f"[WS] + {label} (n={len(self.clients)})")
        try:
            await ws.send(json.dumps({"type":"hello","protocol":"trend-v3"}))
            # snapshot par client
            try:
                with pg_conn() as conn:
                    rows=fetch_pairs_structure(conn, self.pairs, self.tfs)
                    snap=make_snapshot(rows, self.pairs, self.tfs, self.pair_type_map)
                await ws.send(json.dumps(snap, separators=(",",":")))
            except Exception as e:
                await ws.send(json.dumps({"type":"error","message":f"snapshot failed: {e}"}))
            async for _ in ws: pass
        finally:
            self.clients.discard(ws); print(f"[WS] - {label} (n={len(self.clients)})")

    async def broadcast(self, payload:dict):
        if not self.clients: return
        msg=json.dumps(payload, separators=(",",":"))
        await asyncio.gather(*(self._send_one(c,msg,payload.get("type","msg")) for c in list(self.clients)))

    async def _send_one(self, ws, msg, ptype):
        label=self._label(ws)
        try:
            await ws.send(msg)
            print(f"[SEND] -> {label} | {ptype} OK")
        except Exception as e:
            print(f"[SEND] -> {label} | {ptype} ERR {e}")

# ---------- MAIN LOOP ----------
async def main():
    # MT5
    mt5_init_or_die()

    # config fichiers
    pairs, pair_type_map = parse_pairs(PAIRS_FILE)
    tfs = parse_timeframes(TIMEFRAMES_FILE)

    # WS
    ws_server = TrendWSServer(WS_HOST, WS_PORT, pairs, tfs, pair_type_map)
    await ws_server.start()

    # initial snapshot + seed signatures
    try:
        with pg_conn() as conn:
            rows = fetch_pairs_structure(conn, pairs, tfs)
        prev_cell_sig = seed_prev_signatures(rows)
        # broadcast snapshot initial
        snapshot = make_snapshot(rows, pairs, tfs, pair_type_map)
        await ws_server.broadcast(snapshot)
        print("[INIT] Snapshot broadcasted.")
    except Exception as e:
        print(f"[INIT] snapshot failed: {e}")
        prev_cell_sig = {}

    # boucle
    try:
        while True:
            changed_pairs=set()
            # 1) Fetch candles (pour chaque pair/tf)
            with pg_conn() as conn:
                for pair in pairs:
                    for tf in tfs:
                        try:
                            ins, last_ts = fetch_new_candles_for_pair_tf(conn, pair, tf, exchange="mt5")
                            if ins>0:
                                print(f"[FETCH] {pair} {tf}: +{ins} rows, last={ts_ms_to_iso(last_ts)}")
                                # 2) update structure pour ce (pair,tf)
                                tname = f"candles_mt5_{sanitize_name(split_symbol(pair)[1]+split_symbol(pair)[2])}_{tf}"
                                processed_last = incremental_update_structure(conn, tname, pair, tf)
                                changed_pairs.add(pair)
                        except Exception as e:
                            print(f"[ERR] fetch/structure {pair} {tf}: {e}")

                # 3) AOI compute pour paires changées
                for pair in changed_pairs:
                    try:
                        aoi_compute_and_store(
                            conn, pair, exchange="mt5",
                            recency_k=0.05, min_pips=5, max_pips=60,
                            overlap_min=0.70, min_pts_w=3, min_pts_d=5, wW=0.7, wD=0.3,
                            compact_ceiling_pips=40.0, global_min_gap_pips=20.0, global_max_overlap=0.30,
                            max_results=10
                        )
                        print(f"[AOI] {pair}: zones updated.")
                    except Exception as e:
                        print(f"[ERR] AOI {pair}: {e}")

            # 4) Deltas vers le front (si structure a bougé)
            try:
                with pg_conn() as conn:
                    rows = fetch_pairs_structure(conn, pairs, tfs)
                changes, prev_cell_sig = diff_rows(prev_cell_sig, rows)
                if changes:
                    await ws_server.broadcast({"type":"delta","changes":changes})
                    print(f"[UPDATE] {len(changes)} change(s) sent.")
            except Exception as e:
                print(f"[ERR] delta step: {e}")

            await asyncio.sleep(POLL_INTERVAL_SEC)

    except asyncio.CancelledError:
        print("[EXIT] cancelled")
    except KeyboardInterrupt:
        print("[EXIT] KeyboardInterrupt")
    finally:
        mt5.shutdown()
        print("[EXIT] MT5 closed")

if __name__=="__main__":
    asyncio.run(main())
