#!/usr/bin/env python3
# aoi_tf_oriented_preview.py
"""
AOI oriented by opposite side vs TF trend — console preview only (no DB writes)

New logic:
- If TF trend is Bullish -> find the LAST RESISTANCE zone before the session cutoff
- If TF trend is Bearish -> find the LAST SUPPORT zone before the session cutoff
- Session cutoff = 1 hour before London open (default). The AOI is frozen until next session.

Mechanics:
- Build structure points from candles_{exchange}_{pair}_{tf} using body-direction inversion (gapless body).
- For the wanted zone type:
    * iterate anchors of the corresponding direction (dir = -1 for RESISTANCE, +1 for SUPPORT)
      from the most recent point <= cutoff_ts backward
    * for each anchor, expand a symmetric band around anchor by step-pips until:
         - touches >= min_touches (default 3), or
         - width_pips > max_pips (hard cap = 40)
    * first anchor that yields a valid AOI wins (this enforces "last zone before session")

CLI:
  --pairs "EURUSD GBPUSD" or "EURUSD,GBPUSD" (optional; if omitted -> auto-discover from pairs_structure for TF in {1d,4h,1h})
  --exchange (default: mt5)
  --max-pips (default: 40)
  --min-touches (default: 3)
  --step-pips (default: 1)
  --show-points (flag): also list the points included inside the band
  --date YYYY-MM-DD (Europe/London date), default: today
  --open-hour (int, default 8) London local hour for session open
  --cutoff-offset-min (int, default -60) minutes relative to open (e.g., -60 means 1h before)
"""

import os
import re
import math
import argparse
from datetime import datetime, date, time, timezone
from typing import List, Tuple, Dict, Optional

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

try:
    from zoneinfo import ZoneInfo  # py3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # type: ignore

# ------------------------- PG connection -------------------------
def load_pg_conninfo_from_env() -> dict:
    load_dotenv()
    return {
        "host": os.getenv("PGHOST", os.getenv("PG_HOST", "localhost")),
        "port": int(os.getenv("PGPORT", os.getenv("PG_PORT", "5432"))),
        "dbname": os.getenv("PGDATABASE", os.getenv("PG_DB", "postgres")),
        "user": os.getenv("PGUSER", os.getenv("PG_USER", "postgres")),
        "password": os.getenv("PGPASSWORD", os.getenv("PG_PASSWORD", "postgres")),
        "sslmode": os.getenv("PGSSLMODE", os.getenv("PG_SSLMODE", "disable")),
    }

def pg_conn():
    return psycopg2.connect(**load_pg_conninfo_from_env())

# ------------------------- Utils -------------------------
def sanitize_name(s: str) -> str:
    return re.sub(r'[^a-z0-9]+', '_', s.lower()).strip('_')

def table_name(exchange: str, pair: str, tf: str) -> str:
    return f"candles_{sanitize_name(exchange)}_{sanitize_name(pair)}_{sanitize_name(tf)}"

def pip_size(pair: str) -> float:
    return 0.01 if pair.upper().endswith("JPY") else 0.0001

def iso_utc_from_ms(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=timezone.utc).isoformat(timespec="seconds")

def candle_dir(o, c) -> int:
    return 1 if c > o else (-1 if c < o else 0)

def fmt_price(x: float) -> str:
    return f"{x:.6f}".rstrip("0").rstrip(".")

# ------------------------- Session cutoff (Europe/London) -------------------------
def parse_london_date(s: Optional[str]) -> date:
    if not s:
        # today in Europe/London
        now_ldn = datetime.now(tz=ZoneInfo("Europe/London"))
        return now_ldn.date()
    return datetime.strptime(s, "%Y-%m-%d").date()

def london_cutoff_utc_ms(d: date, open_hour_local: int, cutoff_offset_min: int) -> int:
    """
    Build Europe/London datetime for 'open_hour_local' on date d,
    apply cutoff_offset_min (e.g., -60 => 1h before), convert to UTC ms.
    DST handled by Europe/London tz.
    """
    ldn = ZoneInfo("Europe/London")
    dt_open = datetime.combine(d, time(open_hour_local, 0, 0), tzinfo=ldn)
    dt_cut = dt_open + timedelta(minutes=cutoff_offset_min)
    return int(dt_cut.astimezone(timezone.utc).timestamp() * 1000)

# ------------------------- Structure trend -------------------------
def compute_trend(hh, hl, lh, ll) -> str:
    hh, hl, lh, ll = hh or 0, hl or 0, lh or 0, ll or 0
    if hh > 0 and hl > 0 and lh == 0 and ll == 0: return "Bullish"
    if lh > 0 and ll > 0 and hh == 0 and hl == 0: return "Bearish"
    return "Neutral"

def fetch_tf_trend(conn, pair: str, tf: str) -> str:
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(
            "SELECT hh, hl, lh, ll FROM pairs_structure WHERE pair=%s AND timeframe=%s",
            (pair.upper(), tf),
        )
        r = cur.fetchone()
        if not r:
            return "Missing"
        return compute_trend(r['hh'], r['hl'], r['lh'], r['ll'])

# ------------------------- Candles & structure points -------------------------
from datetime import timedelta  # placed here to keep imports together

def lookback_years_for_tf(tf: str) -> int:
    # Reasonable defaults just for point building
    if tf == "1d": return 5
    if tf == "4h": return 2
    return 1  # 1h

def min_ts_ms_years(years: int, cap_ts_ms: int) -> int:
    # Clip lookback to years but not beyond cap
    window_ms = int(years * 365.25 * 24 * 3600 * 1000)
    return max(0, cap_ts_ms - window_ms)

def fetch_candles_upto(conn, table: str, min_ts_ms: int, cap_ts_ms: int) -> List[Tuple[int,float,float,float,float]]:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT ts,open,high,low,close FROM {table} WHERE ts >= %s AND ts <= %s ORDER BY ts ASC",
            (min_ts_ms, cap_ts_ms)
        )
        rows = cur.fetchall()
    if not rows:
        return []
    # Enforce "gapless body": open_i = close_{i-1}
    candles = [(int(ts), float(o), float(h), float(l), float(c)) for ts,o,h,l,c in rows]
    for i in range(1, len(candles)):
        ts, _, h, l, c = candles[i]
        candles[i] = (ts, candles[i-1][4], h, l, c)
    return candles

def build_structure_points(candles: List[Tuple[int,float,float,float,float]]) -> List[Tuple[int,float,int]]:
    """
    Returns a list of (ts_i, price=close_prev, dir), where dir is the NEW body direction at i (+1 up, -1 down).
    Point occurs when body direction flips between i-1 and i (both non-zero).
    """
    pts: List[Tuple[int,float,int]] = []
    for i in range(1, len(candles)):
        ts_prev, o_prev, _, _, c_prev = candles[i - 1]
        ts_i, o_i, _, _, c_i = candles[i]
        d_prev, d_i = candle_dir(o_prev, c_prev), candle_dir(o_i, c_i)
        if d_prev != 0 and d_i != 0 and d_i == -d_prev:
            pts.append((ts_i, c_prev, d_i))  # price=previous close; dir=new
    return pts

# ------------------------- AOI search -------------------------
def count_points_in_band(points: List[Tuple[int,float,int]], low: float, high: float) -> List[Tuple[int,float,int]]:
    return [(ts,p,d) for (ts,p,d) in points if low <= p <= high]

def search_aoi_from_anchor(points: List[Tuple[int,float,int]],
                           anchor_price: float,
                           pip_sz: float,
                           min_touches: int,
                           max_pips: float,
                           step_pips: float) -> Optional[Dict[str, object]]:
    """
    Expand symmetric band by step_pips until touches >= min_touches or width > max_pips.
    Returns dict {low, high, width_pips, touches, used_points} or None.
    """
    # Start at the smallest non-zero width
    k = 0
    while True:
        k += 1
        low = anchor_price - k * step_pips * pip_sz
        high = anchor_price + k * step_pips * pip_sz
        width_pips = (high - low) / pip_sz
        if width_pips > max_pips + 1e-9:
            return None
        inside = count_points_in_band(points, low, high)
        if len(inside) >= min_touches:
            inside_sorted = sorted(inside, key=lambda x: x[0])  # by ts
            return {
                "low": low,
                "high": high,
                "width_pips": width_pips,
                "touches": len(inside_sorted),
                "used_points": inside_sorted
            }

def find_last_zone_before_cutoff(points: List[Tuple[int,float,int]],
                                 want_dir: int,
                                 pip_sz: float,
                                 min_touches: int,
                                 max_pips: float,
                                 step_pips: float,
                                 cutoff_ts: int) -> Optional[Dict[str, object]]:
    """
    Iterate anchors of dir==want_dir from the most recent <= cutoff_ts backward,
    return the first valid AOI found.
    """
    # Filter points up to cutoff
    pts = [pt for pt in points if pt[0] <= cutoff_ts]
    # Iterate from latest anchor backwards
    for ts, price, d in reversed(pts):
        if d != want_dir:
            continue
        aoi = search_aoi_from_anchor(pts, price, pip_sz, min_touches, max_pips, step_pips)
        if aoi:
            # attach anchor info
            aoi["anchor_ts"] = ts
            aoi["anchor_price"] = price
            aoi["anchor_dir"] = d
            return aoi
    return None

# ------------------------- Pair discovery -------------------------
def discover_pairs(conn) -> List[str]:
    """Find pairs having any of TF in ('1d','4h','1h') in pairs_structure."""
    with conn.cursor() as cur:
        cur.execute("""
            SELECT DISTINCT pair
            FROM pairs_structure
            WHERE timeframe IN ('1d','4h','1h')
            ORDER BY pair ASC
        """)
        rows = cur.fetchall()
    return [r[0] for r in rows]

def parse_pairs_cli(s: Optional[str]) -> Optional[List[str]]:
    if not s: return None
    toks = [t.strip().upper() for t in re.split(r"[,\s]+", s.strip()) if t.strip()]
    return [t for t in toks if len(t) >= 6] or None

# ------------------------- Main per TF -------------------------
def process_pair_tf(conn,
                    pair: str,
                    tf: str,
                    exchange: str,
                    max_pips: float,
                    min_touches: int,
                    step_pips: float,
                    cutoff_ts: int,
                    show_points: bool) -> None:
    trend = fetch_tf_trend(conn, pair, tf)
    if trend == "Missing":
        print(f"[{pair}][{tf}] pas de tendance (données manquantes) → skip")
        return
    if trend == "Neutral":
        print(f"[{pair}][{tf}][trend=NEUTRAL] pas d’AOI → skip")
        return

    # Opposite side vs trend
    # Bullish -> want RESISTANCE => anchors with dir = -1
    # Bearish -> want SUPPORT    => anchors with dir = +1
    want_dir = -1 if trend == "Bullish" else +1  # -1: resistance, +1: support
    zone_type = "RESISTANCE" if want_dir == -1 else "SUPPORT"

    pipsz = pip_size(pair)
    tbl = table_name(exchange, pair, tf)

    # Cap the window at cutoff_ts to freeze results for the session
    cap_ts = cutoff_ts
    min_ts = min_ts_ms_years(lookback_years_for_tf(tf), cap_ts)
    candles = fetch_candles_upto(conn, tbl, min_ts, cap_ts)
    if not candles:
        print(f"[{pair}][{tf}][trend={trend.upper()}] aucune bougie ≤ cutoff → skip")
        return

    points = build_structure_points(candles)
    if not points:
        print(f"[{pair}][{tf}][trend={trend.upper()}] aucun point de structure ≤ cutoff → skip")
        return

    aoi = find_last_zone_before_cutoff(points, want_dir, pipsz, min_touches, max_pips, step_pips, cap_ts)
    if not aoi:
        print(f"[{pair}][{tf}][trend={trend.upper()}][{zone_type}] pas d’AOI valide (≥{min_touches} touches, ≤{int(max_pips)} pips) avant cutoff")
        return

    # Round width to integer pips for display
    width_int = int(round(aoi["width_pips"]))
    low = fmt_price(aoi["low"])
    high = fmt_price(aoi["high"])
    touches = aoi["touches"]
    anchor_ts = aoi["anchor_ts"]
    anchor_price = aoi["anchor_price"]
    pol = "↘" if want_dir == -1 else "↗"

    print(f"[{pair}][{tf}][trend={trend.upper()}][{zone_type}] "
          f"AOI: low={low}  high={high}  largeur≈{width_int} pips  touches={touches}  "
          f"(ancre {pol} {fmt_price(anchor_price)} @ {iso_utc_from_ms(anchor_ts)} | cutoff={iso_utc_from_ms(cap_ts)})")

    if show_points:
        print("  points inclus:")
        for ts, price, d in sorted(aoi["used_points"], key=lambda x: x[0]):
            s = "↗" if d == 1 else "↘"
            print(f"   - {iso_utc_from_ms(ts)}  {fmt_price(price)}  {s}")

# ------------------------- Main -------------------------
def main():
    ap = argparse.ArgumentParser(description="AOI preview per TF (1d/4h/1h) — opposite of TF trend; frozen at London-session cutoff.")
    ap.add_argument("--pairs", default=None, help='Override pairs list: "EURUSD GBPUSD" or "EURUSD,GBPUSD"')
    ap.add_argument("--exchange", default="mt5", help="Exchange prefix in candle tables (default: mt5)")
    ap.add_argument("--max-pips", type=float, default=40.0, help="Maximum band width in pips (hard cap = 40)")
    ap.add_argument("--min-touches", type=int, default=3, help="Minimum touches (structure points) required (default 3)")
    ap.add_argument("--step-pips", type=float, default=1.0, help="Step size in pips when expanding the band (default 1)")
    ap.add_argument("--show-points", action="store_true", help="Also list structure points included in band")
    # Session params
    ap.add_argument("--date", default=None, help="London local date YYYY-MM-DD for the session (default: today in Europe/London)")
    ap.add_argument("--open-hour", type=int, default=8, help="London open hour (local Europe/London), default 8")
    ap.add_argument("--cutoff-offset-min", type=int, default=-60, help="Minutes offset relative to open (default -60 => 1h before)")
    args = ap.parse_args()

    # Enforce hard cap
    max_pips = min(float(args.max_pips), 40.0)
    min_touches = max(1, int(args.min_touches))
    step_pips = max(0.1, float(args.step_pips))

    # Compute cutoff_ts (Europe/London)
    d_ldn = parse_london_date(args.date)
    cutoff_ts = london_cutoff_utc_ms(d_ldn, args.open_hour, args.cutoff_offset_min)

    pairs = parse_pairs_cli(args.pairs)
    try:
        with pg_conn() as conn:
            if not pairs:
                pairs = discover_pairs(conn)
                if not pairs:
                    print("[WARN] Aucune paire trouvée (pairs_structure vide pour TF in {1d,4h,1h}).")
                    return
                print(f"[INFO] Paires auto-détectées: {len(pairs)}")

            tfs = ["1d", "4h", "1h"]
            print(f"[INFO] Exchange={args.exchange} | TFs={','.join(tfs)} | max_pips={int(max_pips)} | "
                  f"min_touches={min_touches} | step_pips={step_pips:g}")
            print(f"[INFO] Session cutoff (Europe/London): {d_ldn} open@{args.open_hour:02d}:00, offset={args.cutoff_offset_min} → cutoff_utc={iso_utc_from_ms(cutoff_ts)}")
            total = 0
            for pair in pairs:
                for tf in tfs:
                    process_pair_tf(conn, pair, tf, args.exchange, max_pips, min_touches, step_pips, cutoff_ts, args.show_points)
                    total += 1
            print(f"\n[SUMMARY] Combinaisons traitées: {total}")
    except Exception as e:
        print(f"[FATAL] {e}")

if __name__ == "__main__":
    main()
