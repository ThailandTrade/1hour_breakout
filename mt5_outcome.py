#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MT5 Ticket Tracker — retrace la "vie complète" d'un ticket MT5 (ordre → position → deals)

Usage :
  python mt5_ticket_tracker.py 1276742142

Ce script :
1. Vérifie si l'ordre est encore actif (mt5.orders_get)
2. Si non, consulte l'historique des ordres (mt5.history_orders_get)
3. Récupère la position associée (via position_id)
4. Liste tous les deals (mt5.history_deals_get) pour cette position
5. Classifie la sortie : TP, SL, fermeture manuelle, etc.
"""

import os, sys
from datetime import datetime, timedelta, timezone

UTC = timezone.utc

def iso(ts):
    if ts is None:
        return None
    return datetime.fromtimestamp(ts/1000, tz=UTC).isoformat().replace("+00:00","Z")

def mt5_init():
    import MetaTrader5 as mt5
    term = os.getenv("MT5_TERMINAL_PATH")
    ok = mt5.initialize(term) if term else mt5.initialize()
    if not ok:
        print(f"[ERR] init failed: {mt5.last_error()}")
        sys.exit(2)
    if os.getenv("MT5_LOGIN") and os.getenv("MT5_PASSWORD"):
        if not mt5.login(int(os.getenv("MT5_LOGIN")), os.getenv("MT5_PASSWORD"), os.getenv("MT5_SERVER")):
            print(f"[ERR] login failed: {mt5.last_error()}")
            sys.exit(2)
    return mt5

def to_ms(t_any, offset_h):
    if t_any is None: return None
    t_int = int(t_any)
    ms = t_int if t_int > 10_000_000_000 else t_int * 1000
    return ms - (offset_h * 3600 * 1000)

def track_ticket(ticket: int, lookback_days: int = 30):
    mt5 = mt5_init()
    tz_off = int(os.getenv("MT5_SERVER_TZ_OFFSET_HOURS", "0"))
    print(f"=== TRACKING TICKET {ticket} (lookback {lookback_days}d, offset={tz_off}h) ===")

    # 1️⃣ Pending order ?
    pending = mt5.orders_get(ticket=ticket)
    if pending:
        o = pending[0]
        print(f"[ORDER PENDING] {o.symbol} {o.type} price={o.price_open} volume={o.volume_current}")
        print(f"  time_setup={iso(to_ms(getattr(o,'time_setup_msc',getattr(o,'time_setup',None)), tz_off))}")
        return

    # 2️⃣ Position active ?
    pos = mt5.positions_get(ticket=ticket)
    if pos:
        p = pos[0]
        print(f"[POSITION ACTIVE] {p.symbol} side={'BUY' if p.type==0 else 'SELL'} volume={p.volume} price_open={p.price_open}")
        print(f"  time={iso(to_ms(getattr(p,'time_msc',getattr(p,'time',None)), tz_off))}")
        return

    # 3️⃣ Historique des ordres
    print("\n--- HISTORIQUE ORDRE ---")
    horders = mt5.history_orders_get(ticket=ticket)
    if not horders:
        now = datetime.now(tz=UTC)
        horders = mt5.history_orders_get(now - timedelta(days=lookback_days), now)
    found_order = None
    for o in horders or []:
        if getattr(o,"ticket",None) == ticket:
            found_order = o
            print(f"ORDER {ticket} {o.symbol} type={o.type} state={o.state} volume={o.volume_initial}→{o.volume_current}")
            print(f"  price_open={o.price_open} sl={o.sl} tp={o.tp}")
            print(f"  time_setup={iso(to_ms(getattr(o,'time_setup_msc',getattr(o,'time_setup',None)), tz_off))}")
            print(f"  position_id={getattr(o,'position_id',None)} reason={getattr(o,'reason',None)}")
            break

    if not found_order:
        print("Aucun ordre trouvé dans l'historique. Ticket inconnu ou trop ancien.")
        return

    pos_id = getattr(found_order, "position_id", 0) or 0
    if pos_id == 0:
        print("→ Aucun position_id associé (ordre probablement annulé ou expiré).")
        return

    # 4️⃣ Historique des deals pour la position
    print(f"\n--- DEALS POUR POSITION {pos_id} ---")
    now = datetime.now(tz=UTC)
    deals = mt5.history_deals_get(position=pos_id)
    if not deals:
        deals = mt5.history_deals_get(now - timedelta(days=lookback_days), now)

    if not deals:
        print("Aucun deal trouvé.")
        return

    DEAL_ENTRY_IN = getattr(mt5, "DEAL_ENTRY_IN", 0)
    DEAL_ENTRY_OUT = getattr(mt5, "DEAL_ENTRY_OUT", 1)

    reason_map = {
        getattr(mt5, "DEAL_REASON_TP", 6): "TP",
        getattr(mt5, "DEAL_REASON_SL", 7): "SL",
        getattr(mt5, "DEAL_REASON_SO", 8): "StopOut",
        getattr(mt5, "DEAL_REASON_CLIENT", 0): "Manual",
        getattr(mt5, "DEAL_REASON_EXPERT", 1): "Expert",
        getattr(mt5, "DEAL_REASON_MOBILE", 2): "Mobile"
    }

    last_out = None
    for d in deals:
        entry = getattr(d,"entry",None)
        rcode = getattr(d,"reason",None)
        rstr = reason_map.get(rcode, f"reason={rcode}")
        tms = to_ms(getattr(d,"time_msc",getattr(d,"time",None)), tz_off)
        print(f"{'ENTRY' if entry==DEAL_ENTRY_IN else 'EXIT ' if entry==DEAL_ENTRY_OUT else '----'} "
              f"{d.symbol} price={d.price} vol={d.volume} profit={d.profit} {rstr} time={iso(tms)}")
        if entry == DEAL_ENTRY_OUT:
            last_out = d

    if last_out:
        reason = getattr(last_out, "reason", None)
        if reason == getattr(mt5, "DEAL_REASON_TP", 6):
            print("\n✅ ISSUE = WIN (Take Profit atteint)")
        elif reason == getattr(mt5, "DEAL_REASON_SL", 7):
            print("\n❌ ISSUE = LOSS (Stop Loss atteint)")
        elif reason == getattr(mt5, "DEAL_REASON_SO", 8):
            print("\n⚠️ ISSUE = Stop Out / liquidation forcée")
        else:
            print(f"\nℹ️ ISSUE = Autre (reason={reason})")
    else:
        print("\n⚠️ Pas encore de deal de sortie → position peut être ouverte ou clôture manuelle non détectée.")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python mt5_ticket_tracker.py <ticket>")
        sys.exit(1)
    ticket = int(sys.argv[1])
    track_ticket(ticket)
