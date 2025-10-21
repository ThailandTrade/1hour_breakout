#!/usr/bin/env python3
# mt5_probe_fields.py
"""
Probe MT5 fields for a given symbol/timeframe:
- Prints dtype names returned by copy_rates_* (what fields exist)
- Shows first/last few bars and key volume fields (real_volume, volume, tick_volume)
- Helps decide which volume column to use per broker/symbol
"""

import argparse
from datetime import datetime, timezone
import MetaTrader5 as mt5

UTC = timezone.utc

TF_MAP = {
    "1m": mt5.TIMEFRAME_M1,   "3m": mt5.TIMEFRAME_M3,   "5m": mt5.TIMEFRAME_M5,
    "15m": mt5.TIMEFRAME_M15, "30m": mt5.TIMEFRAME_M30, "1h": mt5.TIMEFRAME_H1,
    "2h": mt5.TIMEFRAME_H2,   "4h": mt5.TIMEFRAME_H4,   "6h": mt5.TIMEFRAME_H6,
    "8h": mt5.TIMEFRAME_H8,   "12h": mt5.TIMEFRAME_H12, "1d": mt5.TIMEFRAME_D1,
    "1w": mt5.TIMEFRAME_W1,
}

def iso_utc(sec: int) -> str:
    return datetime.fromtimestamp(sec, tz=UTC).isoformat(timespec="seconds")

def extract_names(arr):
    # numpy recarray: arr.dtype.names is a tuple of field names
    try:
        return tuple(arr.dtype.names or ())
    except Exception:
        return ()

def extract_safe(row, name, default=None):
    try:
        return row[name]
    except Exception:
        return default

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="EURUSD", help="Symbol to probe, e.g. EURUSD")
    ap.add_argument("--tf", default="1h", choices=TF_MAP.keys(), help="Timeframe code")
    ap.add_argument("--count", type=int, default=200, help="Number of bars to pull from the end")
    args = ap.parse_args()

    if not mt5.initialize():
        print("[ERR] MT5 init failed:", mt5.last_error())
        return

    # Ensure symbol is visible
    if not mt5.symbol_select(args.symbol, True):
        print(f"[ERR] Unable to select symbol {args.symbol}")
        mt5.shutdown()
        return

    tf = TF_MAP[args.tf]
    rates = mt5.copy_rates_from_pos(args.symbol, tf, 0, args.count)
    if rates is None or len(rates) == 0:
        print("[ERR] No data returned. Try opening the chart in MT5 and scroll left to load history.")
        mt5.shutdown()
        return

    names = extract_names(rates)
    print(f"[OK] Received {len(rates)} bars for {args.symbol} {args.tf}")
    print("Fields (dtype.names):", names)

    # Show a small sample: first 2 and last 2 bars
    sample_idx = [0, 1, max(0, len(rates)-2), len(rates)-1]
    seen = set()
    print("\nSample bars (timestamps are OPEN times, server-based seconds):")
    for i in sample_idx:
        if i < 0 or i >= len(rates) or i in seen:
            continue
        seen.add(i)
        r = rates[i]
        t = int(r["time"])
        o = extract_safe(r, "open")
        h = extract_safe(r, "high")
        l = extract_safe(r, "low")
        c = extract_safe(r, "close")
        tv = extract_safe(r, "tick_volume")
        rv = extract_safe(r, "real_volume")
        vv = extract_safe(r, "volume")
        sp = extract_safe(r, "spread")  # sometimes present

        print(f"- idx={i:>4}  time={t} ({iso_utc(t)})  O={o} H={h} L={l} C={c}  "
              f"tick_volume={tv} real_volume={rv} volume={vv} spread={sp}")

    # Quick summary for volume availability over the whole sample
    has_rv = 0
    has_v  = 0
    has_tv = 0
    for r in rates:
        if "real_volume" in names and r["real_volume"] not in (None, 0):
            has_rv += 1
        if "volume" in names and r["volume"] not in (None, 0):
            has_v += 1
        if "tick_volume" in names and r["tick_volume"] not in (None, 0):
            has_tv += 1

    print("\nVolume availability (non-zero counts in the pulled sample):")
    print(f"  real_volume > 0 : {has_rv}/{len(rates)}")
    print(f"  volume      > 0 : {has_v}/{len(rates)}")
    print(f"  tick_volume > 0 : {has_tv}/{len(rates)}")

    mt5.shutdown()

if __name__ == "__main__":
    main()
