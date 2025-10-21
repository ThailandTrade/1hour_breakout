#!/usr/bin/env python3
# mt5_trade_inspect.py
# Inspect a trade by ticket: checks live positions, pending orders, then history (orders & deals).
# Env vars: MT5_LOGIN, MT5_PASSWORD, MT5_SERVER, (optional) MT5_TERMINAL_PATH

import os
import sys
import argparse
from datetime import datetime, timedelta, timezone

UTC = timezone.utc

def iso(ms_or_sec):
    if ms_or_sec is None:
        return "-"
    # MetaTrader5 time fields are in seconds (time, time_msc is ms). We handle both.
    try:
        # If value is large, assume ms
        val = int(ms_or_sec)
    except Exception:
        return "-"
    if val > 10_000_000_000:  # ms
        dt = datetime.fromtimestamp(val/1000, tz=UTC)
    else:  # sec
        dt = datetime.fromtimestamp(val, tz=UTC)
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")

def fmt(x, nd=5):
    if x is None:
        return "-"
    try:
        return f"{float(x):.{nd}f}"
    except Exception:
        return str(x)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ticket", type=int, required=True, help="MT5 ticket id (order/position/deal ticket)")
    ap.add_argument("--days", type=int, default=7, help="History lookback window in days (default 7)")
    a = ap.parse_args()

    try:
        import MetaTrader5 as mt5
    except Exception as e:
        print(f"[ERR] MetaTrader5 module not available: {e}")
        sys.exit(1)

    # Init/login
    term = os.getenv("MT5_TERMINAL_PATH")
    ok = mt5.initialize(term) if term else mt5.initialize()
    if not ok:
        print(f"[ERR] MT5 initialize failed: {mt5.last_error()}")
        sys.exit(1)

    login = os.getenv("MT5_LOGIN")
    password = os.getenv("MT5_PASSWORD")
    server = os.getenv("MT5_SERVER")
    if login and password:
        if not mt5.login(int(login), password, server):
            print(f"[ERR] MT5 login failed: {mt5.last_error()}")
            sys.exit(1)

    ticket = a.ticket

    # 1) Try to find a LIVE POSITION with this ticket directly
    pos = None
    try:
        pos_list = mt5.positions_get()  # all positions
        if pos_list:
            for p in pos_list:
                # Positions have p.ticket (position ticket). Users sometimes pass order ticket; we'll handle that later.
                if getattr(p, "ticket", None) == ticket:
                    pos = p
                    break
    except Exception:
        pos_list = None

    if pos:
        print("=== POSITION (LIVE) FOUND ===")
        print(f"position_ticket : {pos.ticket}")
        print(f"symbol          : {pos.symbol}")
        print(f"type            : {'BUY' if pos.type==0 else 'SELL'}")
        print(f"volume          : {pos.volume}")
        print(f"price_open      : {fmt(pos.price_open)}")
        print(f"sl / tp         : {fmt(pos.sl)} / {fmt(pos.tp)}")
        print(f"price_current   : {fmt(pos.price_current)}")
        print(f"profit          : {fmt(pos.profit, 2)}")
        print(f"time            : {iso(getattr(pos, 'time', None))}")
        print(f"comment         : {getattr(pos, 'comment', '')}")
        mt5.shutdown()
        return

    # 2) Not a live position. Check if it's a CURRENT PENDING ORDER with that ticket
    pending = None
    try:
        od = mt5.orders_get(ticket=ticket)
        if od and len(od) > 0:
            pending = od[0]
    except Exception:
        pending = None

    if pending:
        print("=== PENDING ORDER (LIVE) FOUND ===")
        print(f"order_ticket    : {pending.ticket}")
        print(f"symbol          : {pending.symbol}")
        tmap = {
            mt5.ORDER_TYPE_BUY_LIMIT: "BUY_LIMIT",
            mt5.ORDER_TYPE_SELL_LIMIT: "SELL_LIMIT",
            mt5.ORDER_TYPE_BUY_STOP: "BUY_STOP",
            mt5.ORDER_TYPE_SELL_STOP: "SELL_STOP",
            mt5.ORDER_TYPE_BUY_STOP_LIMIT: "BUY_STOP_LIMIT",
            mt5.ORDER_TYPE_SELL_STOP_LIMIT: "SELL_STOP_LIMIT",
        }
        print(f"type            : {tmap.get(pending.type, str(pending.type))}")
        print(f"volume          : {pending.volume_current} / {pending.volume_initial}")
        print(f"price / sl / tp : {fmt(pending.price_open)} / {fmt(pending.sl)} / {fmt(pending.tp)}")
        print(f"expiration      : {iso(getattr(pending, 'expiration_time', None))}")
        print(f"time_setup      : {iso(getattr(pending, 'time_setup', None))}")
        print(f"comment         : {getattr(pending, 'comment','')}")
        mt5.shutdown()
        return

    # 3) Not live → look into HISTORY (orders & deals). This also lets us link an order ticket → position_id or closure.
    now = datetime.now(tz=UTC)
    t_from = now - timedelta(days=a.days)
    try:
        hist_orders = mt5.history_orders_get(t_from, now)
    except Exception:
        hist_orders = None
    try:
        hist_deals = mt5.history_deals_get(t_from, now)
    except Exception:
        hist_deals = None

    # Try: is this ticket a historical ORDER?
    ho = None
    if hist_orders:
        for o in hist_orders:
            if getattr(o, "ticket", None) == ticket:
                ho = o
                break

    if ho:
        print("=== ORDER (HISTORICAL) FOUND ===")
        print(f"order_ticket    : {ho.ticket}")
        print(f"symbol          : {ho.symbol}")
        print(f"type            : {ho.type}")
        print(f"state           : {ho.state}")  # e.g., 3=FILLED in many builds
        print(f"created         : {iso(getattr(ho, 'time_setup', None))}")
        print(f"done_time       : {iso(getattr(ho, 'time_done', None))}")
        print(f"price / sl / tp : {fmt(ho.price_open)} / {fmt(ho.sl)} / {fmt(ho.tp)}")
        print(f"volume          : {ho.volume_current} / {ho.volume_initial}")
        print(f"comment         : {getattr(ho, 'comment','')}")
        # Link to deals:
        link_deal = None
        link_position_id = None
        if hist_deals:
            for d in hist_deals:
                if getattr(d, "order", None) == ho.ticket:
                    link_deal = d
                    link_position_id = getattr(d, "position_id", None)
                    break
        if link_deal:
            print("\n--- LINKED DEAL ---")
            print(f"deal_ticket     : {link_deal.ticket}")
            print(f"position_id     : {link_position_id}")
            print(f"price / profit  : {fmt(link_deal.price)} / {fmt(link_deal.profit, 2)}")
            print(f"time            : {iso(getattr(link_deal, 'time_msc', None))}")
            # If position may still be live, try to fetch it by position_id:
            if link_position_id:
                live_pos = None
                try:
                    pos_list2 = mt5.positions_get()
                except Exception:
                    pos_list2 = None
                if pos_list2:
                    for p in pos_list2:
                        if getattr(p, "ticket", None) == link_position_id:
                            live_pos = p
                            break
                if live_pos:
                    print("\n>>> POSITION FROM DEAL STILL LIVE <<<")
                    print(f"position_ticket : {live_pos.ticket}")
                    print(f"symbol          : {live_pos.symbol}")
                    print(f"type            : {'BUY' if live_pos.type==0 else 'SELL'}")
                    print(f"volume          : {live_pos.volume}")
                    print(f"price_open      : {fmt(live_pos.price_open)}")
                    print(f"sl / tp         : {fmt(live_pos.sl)} / {fmt(live_pos.tp)}")
                    print(f"price_current   : {fmt(live_pos.price_current)}")
                    print(f"profit          : {fmt(live_pos.profit, 2)}")
        mt5.shutdown()
        return

    # Try: is this ticket a historical DEAL?
    hd = None
    if hist_deals:
        for d in hist_deals:
            if getattr(d, "ticket", None) == ticket:
                hd = d
                break

    if hd:
        print("=== DEAL (HISTORICAL) FOUND ===")
        print(f"deal_ticket     : {hd.ticket}")
        print(f"symbol          : {hd.symbol}")
        print(f"type            : {hd.type}")      # 0=BUY,1=SELL,2=BALANCE, etc.
        print(f"position_id     : {getattr(hd, 'position_id', None)}")
        print(f"order_ticket    : {getattr(hd, 'order', None)}")
        print(f"price / profit  : {fmt(hd.price)} / {fmt(hd.profit, 2)}")
        print(f"time            : {iso(getattr(hd, 'time_msc', None))}")
        print(f"comment         : {getattr(hd, 'comment','')}")
        mt5.shutdown()
        return

    print("No live position, no pending order, and not found in recent history with this ticket.")
    print("Try increasing --days or verify the ticket belongs to this account/server.")
    mt5.shutdown()

if __name__ == "__main__":
    main()
