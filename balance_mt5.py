#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Place a pending BUY on USDJPY sized at 1% risk:
- If ENTRY_PRICE < Ask  -> BUY_LIMIT
- If ENTRY_PRICE >= Ask -> BUY_STOP
SL = 10 pips, TP = 20 pips (attached)
"""

import os, sys
from dataclasses import dataclass
from typing import Optional
import MetaTrader5 as mt5

SYMBOL = "USDJPY"
ENTRY_PRICE = 150.94
SL_PIPS = 10.0
TP_PIPS = 20.0
RISK_PERCENT = 0.01      # 1% risk of available capital
USE_EQUITY = True        # True: use equity; False: use margin_free

@dataclass
class AccountSnapshot:
    equity: float
    margin_free: float
    leverage: int
    currency: str

def init_mt5():
    term = os.getenv("MT5_TERMINAL_PATH")
    ok = mt5.initialize(term) if term else mt5.initialize()
    if not ok:
        print("MT5 init failed:", mt5.last_error(), file=sys.stderr)
    return ok

def login_if_needed():
    login = os.getenv("MT5_LOGIN")
    pwd   = os.getenv("MT5_PASSWORD")
    srv   = os.getenv("MT5_SERVER")
    if login and pwd:
        if not mt5.login(int(login), pwd, srv):
            print("Login failed:", mt5.last_error(), file=sys.stderr)
            return False
    return True

def get_snapshot() -> Optional[AccountSnapshot]:
    ai = mt5.account_info()
    if not ai:
        print("account_info() failed:", mt5.last_error(), file=sys.stderr)
        return None
    return AccountSnapshot(ai.equity, ai.margin_free, ai.leverage, ai.currency)

def ensure_symbol(symbol: str) -> Optional[mt5.SymbolInfo]:
    si = mt5.symbol_info(symbol)
    if not si:
        print(f"symbol_info({symbol}) failed:", mt5.last_error(), file=sys.stderr)
        return None
    if not si.visible and not mt5.symbol_select(symbol, True):
        print(f"symbol_select({symbol}) failed", file=sys.stderr)
        return None
    return si

def preflight(symbol: str) -> bool:
    ti = mt5.terminal_info()
    ai = mt5.account_info()
    si = mt5.symbol_info(symbol)
    if not (ti and ai and si):
        print("Preflight: terminal/account/symbol info missing.", file=sys.stderr)
        return False
    if not getattr(ti, "trade_allowed", True) or not getattr(ti, "trade_expert", True):
        print("❌ AutoTrading disabled in terminal. Enable the green 'AutoTrading' button.", file=sys.stderr)
        return False
    if not getattr(ai, "trade_allowed", True):
        print("❌ Trading not allowed for this account.", file=sys.stderr)
        return False
    if si.trade_mode not in (mt5.SYMBOL_TRADE_MODE_FULL, mt5.SYMBOL_TRADE_MODE_LONGONLY):
        print(f"❌ Symbol not tradable (trade_mode={si.trade_mode}).", file=sys.stderr)
        return False
    return True

def pip_value_per_lot_usd(symbol: str) -> Optional[float]:
    si = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if not (si and tick and tick.ask > 0):
        return None
    price = tick.ask
    contract = si.trade_contract_size or 100000.0
    pip_size = 0.01 if symbol.endswith("JPY") else 0.0001
    pip_val_in_quote = contract * pip_size
    return float(pip_val_in_quote / price) if symbol.endswith("JPY") else float(pip_val_in_quote)

def round_volume(volume: float, symbol: str) -> float:
    si = mt5.symbol_info(symbol)
    step = si.volume_step
    vmin = si.volume_min
    vmax = si.volume_max
    steps = int(volume // step)
    vol = max(vmin, min(steps * step, vmax))
    return round(vol, 3)

def calc_lots_risk(snapshot: AccountSnapshot, symbol: str, risk_pct: float, sl_pips: float) -> Optional[float]:
    pv = pip_value_per_lot_usd(symbol)
    if not pv or pv <= 0:
        print("Could not compute pip value per lot.", file=sys.stderr)
        return None
    capital = snapshot.equity if USE_EQUITY else snapshot.margin_free
    target_risk_usd = max(0.0, capital * risk_pct)
    lots = target_risk_usd / (pv * sl_pips)
    return round_volume(lots, symbol)

def points_distance(si: mt5.SymbolInfo, price_a: float, price_b: float) -> float:
    return abs(price_a - price_b) / si.point

def place_pending_buy(symbol: str, entry_price: float, sl_pips: float, tp_pips: float, lots: float) -> None:
    si = mt5.symbol_info(symbol)
    tick = mt5.symbol_info_tick(symbol)
    if not (si and tick):
        print("Missing symbol/tick info.", file=sys.stderr)
        return

    pip_size = 0.01 if symbol.endswith("JPY") else 0.0001

    # Normalize entry and compute SL/TP from entry (not from market price)
    entry = round(entry_price, si.digits)
    sl = round(entry - sl_pips * pip_size, si.digits)
    tp = round(entry + tp_pips * pip_size, si.digits)

    ask = tick.ask
    order_type = mt5.ORDER_TYPE_BUY_LIMIT if entry < ask else mt5.ORDER_TYPE_BUY_STOP
    order_type_str = "BUY_LIMIT" if order_type == mt5.ORDER_TYPE_BUY_LIMIT else "BUY_STOP"

    # Respect broker's minimal distances (trade_stops_level in points)
    min_points = getattr(si, "trade_stops_level", 0)
    if min_points and min_points > 0:
        if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
            dist_entry = points_distance(si, ask, entry)  # entry below ask
        else:
            dist_entry = points_distance(si, entry, ask)  # entry above ask
        dist_sl = points_distance(si, entry, sl)
        dist_tp = points_distance(si, tp, entry)
        if dist_entry < min_points:
            print(f"❌ Entry too close: {dist_entry:.0f} pts < min {min_points} pts.", file=sys.stderr)
            return
        if dist_sl < min_points:
            print(f"❌ SL too close: {dist_sl:.0f} pts < min {min_points} pts.", file=sys.stderr)
            return
        if dist_tp < min_points:
            print(f"❌ TP too close: {dist_tp:.0f} pts < min {min_points} pts.", file=sys.stderr)
            return

    request = {
        "action": mt5.TRADE_ACTION_PENDING,
        "symbol": symbol,
        "volume": lots,
        "type": order_type,
        "price": entry,
        "sl": sl,
        "tp": tp,
        "type_time": mt5.ORDER_TIME_GTC,    # good till cancel; change to ORDER_TIME_DAY + expiration if needed
        "comment": f"BUY_{order_type_str}_1pct_SL{int(sl_pips)}_TP{int(tp_pips)}",
    }

    result = mt5.order_send(request)
    if result is None:
        print("order_send() returned None:", mt5.last_error(), file=sys.stderr)
        return
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        print(f"❌ Pending order failed: retcode={result.retcode} comment={result.comment}", file=sys.stderr)
        return

    print(f"✅ {order_type_str} placed: lots={lots}, entry={entry}, SL={sl}, TP={tp}")
    print("Order ID:", result.order, "| Request ID:", result.request_id)

def main():
    if not init_mt5(): return
    if not login_if_needed(): 
        mt5.shutdown(); return

    si = ensure_symbol(SYMBOL)
    if not si:
        mt5.shutdown(); return

    if not preflight(SYMBOL):
        mt5.shutdown(); return

    snap = get_snapshot()
    if not snap:
        mt5.shutdown(); return

    print(f"Equity={snap.equity:.2f} | MarginFree={snap.margin_free:.2f} | Leverage={snap.leverage}")

    lots = calc_lots_risk(snap, SYMBOL, RISK_PERCENT, SL_PIPS)
    if not lots or lots <= 0:
        print("Invalid lot size computed.", file=sys.stderr)
        mt5.shutdown(); return
    print(f"Lot size for 1% risk @ {SL_PIPS} pips: {lots}")

    place_pending_buy(SYMBOL, ENTRY_PRICE, SL_PIPS, TP_PIPS, lots)
    mt5.shutdown()

if __name__ == "__main__":
    main()
