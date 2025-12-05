#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Multi-Pairs — Best (Session × w1..w5) per Pair — Expectancy Recap (1 ligne par paire)
+ Breakdown Pair × Jour de la semaine × TP (TP unique) au format CSV :

SESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI
TOKYO,INDEX,GBPCAD,TP3,Y,N,N,N,Y

RÈGLE (mise à jour) :
- On détermine, pour chaque paire, le meilleur moment (session) pour LANCER un trade.
- Par session (TOKYO/LONDON/NY), on prend AU PLUS 1 trade par (paire, jour) si l'entrée se produit dans la fenêtre de la session.
- Peu importe quand le trade se termine (SL/RR5), on NE bloque PAS le jour suivant (pas d'anti-overlap cross-day).

CRITÈRES DE SÉLECTION INCLUS (dans le tableau final) :
- Expectancy R (Exp)
- Winrate
- Profit Factor (PF)

Entrées & cibles :
- Entrées: mêmes règles (break strict, pullback antagoniste, entrée wick), SL = extrême (low/high) depuis le pullback (inclus).
- Cibles: RR1 / RR2 / RR3 / RR4 / RR5 (timestamps), arrêt au 1er SL ou RR5 (pour l’évaluation des hits).
- R-multiple: application événementielle des partiels (w1..w5), w1+...+w5=1.

*** AJOUT DU FILTRE EMA 200 DAILY (Lecture DB) : LONG si entrée > EMA, SHORT si entrée < EMA. ***

*** MISE À JOUR : Le breakdown quotidien applique maintenant le filtre (ExpR >= 0.15 ET PF >= 1.5) ***
"""

import os, sys, argparse, csv
from dataclasses import dataclass
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime, timedelta, timezone, date
from collections import defaultdict
from dotenv import load_dotenv
import psycopg2
from psycopg2 import extensions as pg_ext
from datetime import datetime, timedelta, timezone, date
from zoneinfo import ZoneInfo

UTC = timezone.utc
WEEKDAYS = ["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"]
LONDON_TZ = ZoneInfo("Europe/London")

# --- CONFIG NOM COLONNE DB ---
EMA_COL_NAME = "ema_200"

# ---------------- ENV / DB ----------------
load_dotenv()
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

# ---------------- Time utils ----------------
def iso_utc(ms: int) -> str:
    return datetime.fromtimestamp(ms/1000, tz=UTC).isoformat(timespec="seconds").replace("+00:00", "Z")

def parse_date(d: str) -> date:
    return datetime.strptime(d, "%Y-%m-%d").date()

def daterange(d0: date, d1: date):
    cur = d0
    while cur <= d1:
        yield cur
        cur += timedelta(days=1)

def day_ms_bounds(d: date) -> Tuple[int, int]:
    start = datetime(d.year, d.month, d.day, 0, 0, tzinfo=UTC)
    end   = start + timedelta(days=1)
    return int(start.timestamp()*1000), int(end.timestamp()*1000)

# ---- Sessions (fenêtres UTC) ----
def tokyo_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=1)).timestamp()*1000)        # 01:00
    end   = int((base + timedelta(hours=5, minutes=45)).timestamp()*1000)    # 05:45
    return start, end

def london_signal_window(d: date) -> Tuple[int, int]:
    """
    Fenêtre de signal LONDON définie en heure locale Londres (Europe/London),
    avec gestion automatique été/hiver.
    Exemple : 08:00–14:45 heure de Londres.
    """
    # Date + heure en heure locale Londres
    local_start = datetime(d.year, d.month, d.day, 8, 0, tzinfo=LONDON_TZ)
    local_end   = datetime(d.year, d.month, d.day, 12, 45, tzinfo=LONDON_TZ)

    # Conversion en UTC (avec le bon offset selon été/hiver)
    start_utc = local_start.astimezone(UTC)
    end_utc   = local_end.astimezone(UTC)

    return int(start_utc.timestamp() * 1000), int(end_utc.timestamp() * 1000)


def ny_signal_window(d: date) -> Tuple[int, int]:
    base = datetime(d.year, d.month, d.day, tzinfo=UTC)
    start = int((base + timedelta(hours=13)).timestamp()*1000)       # 13:00
    end   = int((base + timedelta(hours=17, minutes=45)).timestamp()*1000)   # 17:45
    return start, end

def window_for_session(session: str, d: date) -> Tuple[int, int]:
    s = (session or "").strip().upper()
    if s == "TOKYO":  return tokyo_signal_window(d)
    if s == "LONDON": return london_signal_window(d)
    if s in ("NY", "NEWYORK", "NEW_YORK"): return ny_signal_window(d)
    return tokyo_signal_window(d)

# ---------------- Helpers ----------------
def sanitize_pair(pair: str) -> str:
    import re
    return re.sub(r"[^a-z0-9]+", "_", pair.lower()).strip("_")

def table_name(pair: str, tf: str) -> str:
    return f"candles_mt5_{sanitize_pair(pair)}_{tf.lower()}"

def pip_eps_for(pair: str) -> float:
    core = pair.upper().split(".")[0]
    return 0.001 if core.endswith("JPY") else 0.00001

def pip_size_for(pair: str) -> float:
    core = pair.upper().split(".")[0]
    if core.startswith("XAU"):
        return 0.01
    return 0.01 if core.endswith("JPY") else 0.0001

def infer_type(pair: str) -> str:
    """
    Heuristique simple pour TYPE: FOREX / METAL / INDEX / CRYPTO
    """
    up = pair.upper()

    # Metals
    if up.startswith("XAU") or up.startswith("XAG") or up.startswith("XPT") or up.startswith("XPD"):
        return "METAL"

    # Index (liste à compléter si besoin)
    index_symbols = {
        "NAS100", "US30", "US500", "SPX500", "GER40", "UK100", "FRA40",
        "JPN225", "JP225", "HK50"
    }
    if up in index_symbols:
        return "INDEX"

    # Crypto (liste à compléter si besoin)
    crypto_symbols = {
        "BTCUSD", "ETHUSD", "LTCUSD", "XRPUSD", "ADAUSD", "SOLUSD"
    }
    if up in crypto_symbols:
        return "CRYPTO"

    # Par défaut
    return "FOREX"

# ---------------- DB Readers ----------------

def read_prev_daily_ema(conn, pair: str, current_day_start_ms: int) -> Optional[float]:
    """
    Récupère l'EMA 200 Daily directement depuis la DB.
    ATTENTION: Utilise le TF '1d'.
    """
    t_d1 = table_name(pair, "1d") 
    
    # On cherche la dernière bougie close avant le début de cette journée
    sql = f"SELECT {EMA_COL_NAME} FROM {t_d1} WHERE ts < %s ORDER BY ts DESC LIMIT 1"
    
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (current_day_start_ms,))
            row = cur.fetchone()
            if row and row[0] is not None:
                return float(row[0])
            return None
    except Exception:
        conn.rollback() 
        return None

def read_first_1h(conn, pair: str, d: date) -> Optional[Dict]:
    t1h = table_name(pair, "1h")
    day_start, _ = day_ms_bounds(d)
    sql = f"SELECT ts, open, high, low, close FROM {t1h} WHERE ts = %s LIMIT 1"
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (day_start,))
            row = cur.fetchone()
            if not row: return None
            ts, o, h, l, c = row
            return {"ts": int(ts), "open": float(o), "high": float(h), "low": float(l), "close": float(c)}
    except Exception:
        conn.rollback(); return None

def read_15m_in(conn, pair: str, start_ms: int, end_ms: int) -> List[Dict]:
    t15 = table_name(pair, "15m")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t15}
        WHERE ts >= %s AND ts <= %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms, end_ms))
            rows = cur.fetchall()
            return [{"ts": int(ts), "open": float(o), "high": float(h),
                     "low": float(l), "close": float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

def read_15m_from(conn, pair: str, start_ms: int) -> List[Dict]:
    t15 = table_name(pair, "15m")
    sql = f"""
        SELECT ts, open, high, low, close
        FROM {t15}
        WHERE ts > %s
        ORDER BY ts ASC
    """
    try:
        with conn.cursor() as cur:
            cur.execute(sql, (start_ms,))
            rows = cur.fetchall()
            return [{"ts": int(ts), "open": float(o), "high": float(h),
                     "low": float(l), "close": float(c)} for ts,o,h,l,c in rows]
    except Exception:
        conn.rollback(); return []

# ---------------- FSM / Trade (CORE LOGIC) ----------------
@dataclass
class Trade:
    side: str            # "LONG" | "SHORT"
    entry_ts: int        # ts OPEN UTC of trigger bar
    entry: float
    sl: float

def detect_first_trade_for_day(c15: List[Dict], range_high: float, range_low: float, ema_daily: Optional[float]) -> Optional[Trade]:
    # Activation long/short + pullback antagoniste + wick trigger; SL = lowest/highest depuis pullback (inclus)
    long_active = False
    long_hh: Optional[float] = None
    long_pullback_idx: Optional[int] = None
    long_min_low_since_pullback: Optional[float] = None  # suivi du plus bas depuis pullback (inclus)

    short_active = False
    short_ll: Optional[float] = None
    short_pullback_idx: Optional[int] = None
    short_max_high_since_pullback: Optional[float] = None  # suivi du plus haut depuis pullback (inclus)

    for i, b in enumerate(c15):
        ts, o, h, l, c = b["ts"], b["open"], b["high"], b["low"], b["close"]

        if (not long_active) and (c > range_high):
            long_active = True
            long_hh = h
            long_pullback_idx = None
            long_min_low_since_pullback = None

        if (not short_active) and (c < range_low):
            short_active = True
            short_ll = l
            short_pullback_idx = None
            short_max_high_since_pullback = None

        # -------- LONG --------
        if long_active:
            prev_hh = long_hh
            if long_pullback_idx is None and (c < o):
                long_pullback_idx = i
                long_min_low_since_pullback = l  # inclut la bougie de pullback
            if long_pullback_idx is not None and i >= 1:
                prev_low = c15[i-1]["low"]
                long_min_low_since_pullback = prev_low if long_min_low_since_pullback is None else min(long_min_low_since_pullback, prev_low)
            
            # TRIGGER LONG
            if (prev_hh is not None) and (long_pullback_idx is not None) and (i > long_pullback_idx) and (h > prev_hh) and (i >= 1):
                entry_price = prev_hh
                
                # --- FILTRE EMA LONG ---
                if ema_daily is not None and entry_price <= ema_daily:
                    pass 
                else:
                    sl_price = long_min_low_since_pullback if long_min_low_since_pullback is not None else c15[i-1]["low"]
                    return Trade("LONG", ts, entry_price, sl_price)

            if (long_hh is None) or (h > long_hh):
                long_hh = h

        # -------- SHORT --------
        if short_active:
            prev_ll = short_ll
            if short_pullback_idx is None and (c > o):
                short_pullback_idx = i
                short_max_high_since_pullback = h  # inclut la bougie de pullback
            if short_pullback_idx is not None and i >= 1:
                prev_high = c15[i-1]["high"]
                short_max_high_since_pullback = prev_high if short_max_high_since_pullback is None else max(short_max_high_since_pullback, prev_high)
            
            # TRIGGER SHORT
            if (prev_ll is not None) and (short_pullback_idx is not None) and (i > short_pullback_idx) and (l < prev_ll) and (i >= 1):
                entry_price = prev_ll

                # --- FILTRE EMA SHORT ---
                if ema_daily is not None and entry_price >= ema_daily:
                    pass
                else:
                    sl_price = short_max_high_since_pullback if short_max_high_since_pullback is not None else c15[i-1]["high"]
                    return Trade("SHORT", ts, entry_price, sl_price)

            if (short_ll is None) or (l < short_ll):
                short_ll = l

    return None

# ---------------- After-entry evaluation (CORE LOGIC) ----------------
def evaluate_trade_after_entry(conn, pair: str, tr: Trade):
    """
    Enregistre les timestamps de RR1/RR2/RR3/RR4/RR5/SL; stop au premier SL ou RR5.
    """
    eps = pip_eps_for(pair)
    entry, sl = tr.entry, tr.sl
    r = abs(entry - sl)
    if r <= 0:
        targets = {"RR1": entry, "RR2": entry, "RR3": entry, "RR4": entry, "RR5": entry}
        results = {k: "SL" for k in ["RR1", "RR2", "RR3", "RR4", "RR5"]}
        return targets, results, {"SL": None, "RR1": None, "RR2": None, "RR3": None, "RR4": None, "RR5": None}, None

    if tr.side == "LONG":
        t1 = entry + 1.0 * r
        t2 = entry + 2.0 * r
        t3 = entry + 3.0 * r
        t4 = entry + 4.0 * r
        t5 = entry + 5.0 * r
    else:
        t1 = entry - 1.0 * r
        t2 = entry - 2.0 * r
        t3 = entry - 3.0 * r
        t4 = entry - 4.0 * r
        t5 = entry - 5.0 * r

    targets = {"RR1": t1, "RR2": t2, "RR3": t3, "RR4": t4, "RR5": t5}
    hit_time: Dict[str, Optional[int]] = {"SL": None, "RR1": None, "RR2": None, "RR3": None, "RR4": None, "RR5": None}

    future = read_15m_from(conn, pair, tr.entry_ts)
    for b in future:
        ts, h, l = b["ts"], b["high"], b["low"]
        if tr.side == "LONG":
            sl_hit  = (l <= sl + eps)
            rr1_hit = (h >= t1 - eps)
            rr2_hit = (h >= t2 - eps)
            rr3_hit = (h >= t3 - eps)
            rr4_hit = (h >= t4 - eps)
            rr5_hit = (h >= t5 - eps)
        else:
            sl_hit  = (h >= sl - eps)
            rr1_hit = (l <= t1 + eps)
            rr2_hit = (l <= t2 + eps)
            rr3_hit = (l <= t3 + eps)
            rr4_hit = (l <= t4 + eps)
            rr5_hit = (l <= t5 + eps)

        if hit_time["SL"]  is None and sl_hit:  hit_time["SL"]  = ts
        if hit_time["RR1"] is None and rr1_hit: hit_time["RR1"] = ts
        if hit_time["RR2"] is None and rr2_hit: hit_time["RR2"] = ts
        if hit_time["RR3"] is None and rr3_hit: hit_time["RR3"] = ts
        if hit_time["RR4"] is None and rr4_hit: hit_time["RR4"] = ts
        if hit_time["RR5"] is None and rr5_hit: hit_time["RR5"] = ts

        # Stop au premier SL ou RR5
        if (hit_time["SL"] is not None) or (hit_time["RR5"] is not None):
            break

    results: Dict[str, str] = {}
    sl_time = hit_time["SL"]
    for key in ["RR1", "RR2", "RR3", "RR4", "RR5"]:
        ttime = hit_time[key]
        results[key] = "TP" if (ttime is not None and (sl_time is None or ttime < sl_time)) else "SL"

    closed_ts = sl_time if sl_time is not None else hit_time["RR5"]
    return targets, results, hit_time, closed_ts

# ---------------- Partials (w1..w5) -> R-multiple ----------------
def compute_r_and_close(hit_time: Dict[str, Optional[int]],
                        w1: float, w2: float, w3: float, w4: float, w5: float) -> float:
    """
    Application temporelle des sorties partielles (w1+...+w5=1):
    Renvoie le R-multiple total.
    """
    t_sl = hit_time.get("SL")
    t1   = hit_time.get("RR1")
    t2   = hit_time.get("RR2")
    t3   = hit_time.get("RR3")
    t4   = hit_time.get("RR4")
    t5   = hit_time.get("RR5")

    events: List[Tuple[int, str]] = []
    if t1 is not None: events.append((t1, "TP1"))
    if t2 is not None: events.append((t2, "TP2"))
    if t3 is not None: events.append((t3, "TP3"))
    if t4 is not None: events.append((t4, "TP4"))
    if t5 is not None: events.append((t5, "TP5"))
    if t_sl is not None: events.append((t_sl, "SL"))
    events.sort(key=lambda x: x[0])

    rem = 1.0
    r   = 0.0

    for ts, ev in events:
        if ev == "TP1" and w1 > 0:
            r   += w1 * 1.0
            rem -= w1
            if rem <= 1e-12: break
        elif ev == "TP2" and w2 > 0:
            r   += w2 * 2.0
            rem -= w2
            if rem <= 1e-12: break
        elif ev == "TP3" and w3 > 0:
            r   += w3 * 3.0
            rem -= w3
            if rem <= 1e-12: break
        elif ev == "TP4" and w4 > 0:
            r   += w4 * 4.0
            rem -= w4
            if rem <= 1e-12: break
        elif ev == "TP5" and w5 > 0:
            r   += w5 * 5.0
            rem -= w5
            if rem <= 1e-12: break
        elif ev == "SL":
            if rem > 0:
                r += (-1.0) * rem
                rem = 0.0
            break

    return r

def reached_before(hits: Dict[str, Optional[int]], key: str) -> bool:
    t = hits.get(key)
    sl = hits.get("SL")
    return t is not None and (sl is None or t < sl)

# ---------------- Core: générer les trades (par session) ----------------
@dataclass
class BareTrade:
    # {"SL": ts|None, "RR1": ts|None, ... "RR5": ts|None}
    hits: Dict[str, Optional[int]]
    entry_ts: int                     # ts OPEN UTC de la bougie de trigger

def collect_trades_for_session(conn, pair: str, start: date, end: date, session: str) -> List[BareTrade]:
    """
    Règle: on prend AU PLUS UN trade par (paire, jour) si l'entrée est dans la fenêtre de la session.
    """
    trades: List[BareTrade] = []

    # cross-day blocking
    block_until_ts: Optional[int] = None  # ts de clôture (SL ou RR5) du dernier trade

    for d in daterange(start, end):
        # 1) Fetch D1 EMA (200) depuis la DB pour ce jour
        day_start_ms, _ = day_ms_bounds(d)
        ema_val = read_prev_daily_ema(conn, pair, day_start_ms)

        # 2) Fenêtre 15m de la session pour ce jour
        s, e = window_for_session(session, d)

        # Si un trade précédent est encore "ouvert" au début de cette fenêtre, on saute ce jour
        #if block_until_ts is not None and s <= block_until_ts:
            #continue

        # 3) Range H1 du jour (00:00–01:00 UTC)
        c1 = read_first_1h(conn, pair, d)
        if not c1:
            continue
        rh, rl = c1["high"], c1["low"]

        c15 = read_15m_in(conn, pair, s, e)
        if not c15:
            continue

        # 4) Détecte le PREMIER trade dans la fenêtre (un seul par jour)
        # On passe ema_val pour filtrer
        tr = detect_first_trade_for_day(c15, rh, rl, ema_val)
        if not tr:
            continue

        # 5) Enregistre les hits pour calculer R/TP% ET récupérer le closed_ts
        _, _, hits, closed_ts = evaluate_trade_after_entry(conn, pair, tr)
        trades.append(BareTrade(hits=hits, entry_ts=tr.entry_ts))

        # 6) Cross-day blocking: BLOQUE jusqu'à SL ou RR5
        #block_until_ts = closed_ts if closed_ts is not None else (2**62)

        # 7) Passe au jour suivant (jamais de 2e trade ce jour)
        continue

    return trades

# ---------------- Grille des poids ----------------
def weight_grid(step: float = 0.1):
    """
    Génère toutes les combinaisons (w1..w5) avec pas 'step' telles que:
      w1 + w2 + w3 + w4 + w5 = 1
    """
    vals = [round(i * step, 1) for i in range(int(1/step) + 1)]
    for w1 in vals:
        for w2 in vals:
            for w3 in vals:
                for w4 in vals:
                    w5 = round(1.0 - w1 - w2 - w3 - w4, 1)
                    if w5 < -1e-9:
                        continue
                    if abs(w1 + w2 + w3 + w4 + w5 - 1.0) <= 1e-9 and (0.0 <= w5 <= 1.0):
                        yield (w1, w2, w3, w4, w5)

# ---------------- Stats pour une combinaison ----------------
def stats_for_weights(trades: List[BareTrade],
                      w1: float, w2: float, w3: float, w4: float, w5: float) -> Dict[str, Any]:
    total = len(trades)
    if total == 0:
        return {
            "trades": 0, "winrate": 0.0,
            "profit_factor": 0.0,
            "avg_win": 0.0, "avg_loss": 0.0, "exp": 0.0,
            "p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0, "p5": 0.0
        }

    r_wins: List[float] = []
    r_losses: List[float] = [] # Contient les valeurs R négatives

    tp1_cnt = tp2_cnt = tp3_cnt = tp4_cnt = tp5_cnt = 0

    for bt in trades:
        hits = bt.hits
        if reached_before(hits, "RR1"): tp1_cnt += 1
        if reached_before(hits, "RR2"): tp2_cnt += 1
        if reached_before(hits, "RR3"): tp3_cnt += 1
        if reached_before(hits, "RR4"): tp4_cnt += 1
        if reached_before(hits, "RR5"): tp5_cnt += 1

        r_mult = compute_r_and_close(hits, w1, w2, w3, w4, w5)

        # WIN/LOSS = TP1 avant SL
        if reached_before(hits, "RR1"):
            r_wins.append(r_mult)
        else:
            r_losses.append(r_mult)

    wins = len(r_wins)
    losses = len(r_losses)
    total_trades = total 
    
    # 1. Calcul des moyennes
    avg_win = (sum(r_wins) / wins) if wins > 0 else 0.0
    
    # La somme des pertes brutes est l'opposé de la somme des r_losses (qui sont des valeurs négatives)
    total_r_losses_gross = abs(sum(r_losses)) 
    avg_loss_r = (total_r_losses_gross / losses) if losses > 0 else 0.0
    
    winrate = (wins / total_trades) if total_trades > 0 else 0.0
    expectancy = winrate * avg_win - (1.0 - winrate) * avg_loss_r
    
    # 2. CALCUL DU PROFIT FACTOR (PF)
    total_r_gains_gross = sum(r_wins)
    
    if total_r_losses_gross > 1e-9: # Éviter la division par zéro
        profit_factor = total_r_gains_gross / total_r_losses_gross
    else:
        profit_factor = 999.0 if total_r_gains_gross > 0 else 0.0 # PF très élevé si zéro perte
    
    # 3. Finalisation des pourcentages
    p1 = tp1_cnt / total_trades
    p2 = tp2_cnt / total_trades
    p3 = tp3_cnt / total_trades
    p4 = tp4_cnt / total_trades
    p5 = tp5_cnt / total_trades

    return {
        "trades": total_trades,
        "winrate": winrate,
        "profit_factor": profit_factor, 
        "avg_win": avg_win,
        "avg_loss": avg_loss_r,
        "exp": expectancy,
        "p1": p1, "p2": p2, "p3": p3, "p4": p4, "p5": p5
    }

# ---------------- Breakdown Pair / Jour / TP (TP unique) ----------------
def build_breakdown_rows_for_pair(pair: str, session: str, trades: List[BareTrade]) -> List[Dict[str, Any]]:
    """
    Modifié pour calculer ExpR et PF pour chaque combinaison Jour/TP.
    """
    # Structure mise à jour pour le calcul du PF
    buckets = defaultdict(lambda: {"trades": 0, "wins": 0, "r_wins_sum": 0.0, "r_losses_sum_abs": 0.0})

    for bt in trades:
        hits = bt.hits
        dt = datetime.fromtimestamp(bt.entry_ts / 1000, tz=UTC)
        dow_idx = dt.weekday()            # 0=MON, 6=SUN
        dow_name = WEEKDAYS[dow_idx]

        t_sl = hits.get("SL")

        # Évaluation du trade comme s'il utilisait un TP unique
        for tp_key, k, tp_label in [
            ("RR1", 1, "TP1"), ("RR2", 2, "TP2"), ("RR3", 3, "TP3"),
            ("RR4", 4, "TP4"), ("RR5", 5, "TP5"),
        ]:
            bucket_key = (dow_name, tp_label)
            buckets[bucket_key]["trades"] += 1
            r = float(k)
            
            t_tp = hits.get(tp_key)
            if (t_tp is not None) and (t_sl is None or t_tp < t_sl):
                # WIN
                buckets[bucket_key]["wins"] += 1
                buckets[bucket_key]["r_wins_sum"] += r
            else:
                # LOSS (-1.0 R)
                r = -1.0
                buckets[bucket_key]["r_losses_sum_abs"] += abs(r)

    rows: List[Dict[str, Any]] = []

    def sort_key(item):
        (dow_name, tp_label) = item[0]
        return (WEEKDAYS.index(dow_name), tp_label)

    for (dow_name, tp_label), agg in sorted(buckets.items(), key=sort_key):
        total = agg["trades"]
        if total == 0: continue
        wins = agg["wins"]
        
        # Calcul des métriques
        total_r_gains_gross = agg["r_wins_sum"]
        total_r_losses_gross = agg["r_losses_sum_abs"]
        
        winrate = wins / total
        avg_r = (total_r_gains_gross - total_r_losses_gross) / total # Expectancy R

        if total_r_losses_gross > 1e-9:
            profit_factor = total_r_gains_gross / total_r_losses_gross
        else:
            profit_factor = 999.0 if total_r_gains_gross > 0 else 0.0

        rows.append({
            "pair": pair,
            "session": session,
            "dow": dow_name,
            "tp": tp_label,
            "trades": total,
            "winrate": winrate,
            "expectancy": avg_r,
            "profit_factor": profit_factor
        })

    return rows

# ---------------- Chargement des paires ----------------
def load_pairs_from_file(path: str) -> List[str]:
    pairs: List[str] = []
    if not os.path.exists(path):
        print(f"Erreur: {path} introuvable.")
        return pairs
    # Essaye CSV avec en-tête
    try:
        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames and any(h.lower() in ("pair","pairs") for h in reader.fieldnames):
                for rec in reader:
                    p = (rec.get("pair") or rec.get("PAIR") or rec.get("pairs") or rec.get("PAIRS") or "").strip()
                    if p:
                        up = p.upper()
                        if up not in pairs:
                            pairs.append(up)
                if pairs:
                    return pairs
    except Exception:
        pass
    # Fallback: une paire par ligne
    with open(path, "r") as f:
        for line in f:
            p = line.strip()
            if not p or p.startswith("#"):
                continue
            up = p.upper()
            if up not in pairs:
                pairs.append(up)
    return pairs

# ---------------- Impression du recap final (1 ligne / paire) ----------------
def print_final_best_table(rows: List[Dict[str, Any]]):
    try:
        from prettytable import PrettyTable
    except Exception:
        PrettyTable = None

    # Tri par expectancy décroissante
    rows_sorted = sorted(rows, key=lambda r: r["exp"], reverse=True)

    if PrettyTable:
        t = PrettyTable()
        t.field_names = [
            "Pair","Session","w1","w2","w3","w4","w5",
            "Trades","Winrate", "PF", "AvgWinR","AvgLossR","ExpectancyR",
            "TP1%","TP2%","TP3%","TP4%","TP5%"
        ]
        for r in rows_sorted:
            t.add_row([
                r["pair"],
                r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}", f"{r['w4']:.1f}", f"{r['w5']:.1f}",
                r["trades"],
                f"{r['winrate']*100:.2f}%",
                f"{r['profit_factor']:.2f}", # AFFICHAGE DU PF
                f"{r['avg_win']:.3f}R",
                f"{r['avg_loss']:.3f}R",
                f"{r['exp']:+.3f}R",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%",
                f"{r['p4']*100:.2f}%",
                f"{r['p5']*100:.2f}%"
            ])
        print("\n===== BEST COMBO PAR PAIRE — trié par Expectancy (R) =====")
        print(t)
        print("==========================================================")
    else:
        # Fallback
        print("\nPair\tSession\tw1\tw2\tw3\tw4\tw5\tTrades\tWinrate\tPF\tAvgWinR\tAvgLossR\tExpectancyR\tTP1%\tTP2%\tTP3%\tTP4%\tTP5%")
        for r in rows_sorted:
            print("\t".join([
                r["pair"], r["session"],
                f"{r['w1']:.1f}", f"{r['w2']:.1f}", f"{r['w3']:.1f}", f"{r['w4']:.1f}", f"{r['w5']:.1f}",
                str(r["trades"]),
                f"{r['winrate']*100:.2f}%",
                f"{r['profit_factor']:.2f}", # AFFICHAGE DU PF
                f"{r['avg_win']:.3f}",
                f"{r['avg_loss']:.3f}",
                f"{r['exp']:+.3f}",
                f"{r['p1']*100:.2f}%",
                f"{r['p2']*100:.2f}%",
                f"{r['p3']*100:.2f}%",
                f"{r['p4']*100:.2f}%",
                f"{r['p5']*100:.2f}%"
            ]))
        print("==========================================================")

# ---------------- IMPRESSION CSV DÉTAILLÉ JOUR/PAIRE/TP ----------------
def print_breakdown_table(rows: List[Dict[str, Any]], exp_threshold: float = 0.15, pf_threshold: float = 1.5):
    """
    Restaure la sortie CSV détaillée Jour/TP (Y/N), en filtrant par le double seuil local.
    """
    if not rows:
        print("\n[BREAKDOWN QUOTIDIEN] Aucune paire n'a passé le filtre global ou quotidien.")
        return
    
    # --- 1) Déterminer le meilleur TP par Jour/Paire qui passe le double filtre ---
    # best_tp_per_spd[(session, pair, day)] = (tp_best, exp_best, pf_best)
    best_tp_per_spd: Dict[Tuple[str, str, str], Tuple[Optional[str], Optional[float], Optional[float]]] = {}
    
    for r in rows:
        key_day = (r["session"], r["pair"], r["dow"])
        
        # Filtre local : ExpR >= seuil ET PF >= seuil
        if r["expectancy"] >= exp_threshold and r["profit_factor"] >= pf_threshold:
            # On utilise l'expectancy pour départager si plusieurs TP passent
            current_best_exp = best_tp_per_spd.get(key_day, (None, -999.0, 0.0))[1]
            
            if r["expectancy"] > current_best_exp:
                best_tp_per_spd[key_day] = (r["tp"], r["expectancy"], r["profit_factor"])
    
    # --- 2) Construction de la structure finale des drapeaux (CSV) ---
    # final_flags[(session, pair, tp)] = {day: "Y"/"N"}
    final_flags: Dict[Tuple[str, str, str], Dict[str, str]] = {}
    valid_days = ["MON", "TUE", "WED", "THU", "FRI"]
    
    all_sp_keys = sorted(list({(r["session"], r["pair"]) for r in rows}))

    for session, pair in all_sp_keys:
        for tp in ["TP1", "TP2", "TP3", "TP4", "TP5"]:
            key = (session, pair, tp)
            final_flags[key] = {day: "N" for day in valid_days}

        for d in valid_days:
            key_day = (session, pair, d)
            tp_best_for_day, _, _ = best_tp_per_spd.get(key_day, (None, None, None))
            
            if tp_best_for_day is not None:
                # Marquer 'Y' sur le TP qui a été identifié comme le meilleur du jour
                final_flags[(session, pair, tp_best_for_day)][d] = "Y"
                
    # --- 3) Impression CSV ---
    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")
    
    # tri par SESSION, PAIR, TP
    output_lines: List[str] = []
    
    for (session, pair, tp), day_flags in sorted(final_flags.items(), key=lambda x: (x[0][0], x[0][1], x[0][2])):
        flags_list = [day_flags[d] for d in valid_days]
        if not any(f == "Y" for f in flags_list):
            continue 

        pair_type = infer_type(pair)
        line = f"{session},{pair_type},{pair},{tp}," + ",".join(flags_list)
        output_lines.append(line)
    
    if output_lines:
        print('\n'.join(output_lines))
    else:
        # Affichage d'un message si aucun TP/Jour n'a passé le filtre local
        print(f"\n[BREAKDOWN QUOTIDIEN] Aucun TP/Jour n'a passé le double filtre local (ExpR >= {exp_threshold} et PF >= {pf_threshold}).")

def print_high_exp_pairs_csv(best_rows: List[Dict[str, Any]], 
                             best_exp_threshold: float = 0.15, 
                             pf_threshold: float = 1.5):
    """
    Restauration du CSV des paires 'haut rendement' (Global ExpR/PF filter).
    
    - On filtre les paires dont l'expectancy globale (exp) >= best_exp_threshold ET PF >= pf_threshold.
    - On choisit le TP correspondant AU POIDS DOMINANT (w1..w5) de cette paire.
    - On affiche : SESSION,TYPE,PAIR,TP,Y,Y,Y,Y,Y
    """

    print("\nSESSION,TYPE,PAIR,TP,MON,TUE,WED,THU,FRI")

    for r in best_rows:
        # 1) Filtre sur l'ExpectancyR globale du recap ET le Profit Factor
        if r["exp"] < best_exp_threshold or r["profit_factor"] < pf_threshold:
            continue

        session = r["session"]
        if session == "-":
            continue

        pair = r["pair"]
        pair_type = infer_type(pair)

        # 2) Choix du TP via le POIDS dominant w1..w5
        weights = [
            ("TP1", r["w1"]), ("TP2", r["w2"]), ("TP3", r["w3"]),
            ("TP4", r["w4"]), ("TP5", r["w5"]),
        ]
        best_tp, _ = max(weights, key=lambda x: x[1])  # max sur le poids

        # 3) Impression console
        print(f"{session},{pair_type},{pair},{best_tp},Y,Y,Y,Y,Y")


# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs-file", default="pairs_5ers.txt", help="Fichier des paires (une par ligne ou CSV avec colonne pair/pairs)")
    ap.add_argument("--start-date", default="2025-01-01")
    ap.add_argument("--end-date",   default="2025-12-31")
    ap.add_argument("--step", type=float, default=0.1, choices=[0.1], help="Pas de grille (fixé à 0.1)")
    ap.add_argument("--exp-threshold", type=float, default=0.15, help="Seuil d'expectancy pour marquer Y et filtrer les lignes")
    ap.add_argument(
        "--session",
        default="TOKYO",
        choices=["TOKYO", "LONDON", "NY"],
        help="Session à tester (TOKYO, LONDON ou NY). Défaut: TOKYO."
    )
    ap.add_argument(
        "--best-exp-threshold",
        type=float,
        default=0.15,
        help="Seuil d'expectancy globale (R) pour le CSV 'all days = Y' (expR >= ce seuil)."
    )
    ap.add_argument(
        "--pf-threshold",
        type=float,
        default=1.5,
        help="Seuil de Profit Factor (PF >= ce seuil) pour le filtrage final."
    )
    args = ap.parse_args()

    pairs = load_pairs_from_file(args.pairs_file)
    if not pairs:
        print("Aucune paire trouvée.")
        sys.exit(0)

    d0 = parse_date(args.start_date)
    d1 = parse_date(args.end_date)

    # --- TOKYO ONLY (ajoute LONDON/NY si besoin) ---
    sessions = [args.session.upper()]

    best_rows: List[Dict[str, Any]] = []
    breakdown_rows: List[Dict[str, Any]] = []

    with get_pg_conn() as c:
        for pair in pairs:
            pair = pair.strip().upper()
            if not pair:
                continue

            # 1) Collecte des trades par session
            session_trades: Dict[str, List[BareTrade]] = {}
            for sess in sessions:
                print(f"[{pair}] Collecte trades — {sess} ...")
                session_trades[sess] = collect_trades_for_session(c, pair, d0, d1, sess)

            # 2) Parcourt la grille (w1..w5) pour chaque session et retient la meilleure combinaison
            best_for_pair: Optional[Dict[str, Any]] = None

            for sess in sessions:
                trades = session_trades[sess]
                for (w1, w2, w3, w4, w5) in weight_grid(step=args.step):
                    st = stats_for_weights(trades, w1, w2, w3, w4, w5)
                    row = {
                        "pair": pair,
                        "session": sess,
                        "w1": w1, "w2": w2, "w3": w3, "w4": w4, "w5": w5,
                        **st
                    }
                    if (best_for_pair is None) or (row["exp"] > best_for_pair["exp"]):
                        best_for_pair = row

            # 3) Empile la meilleure ligne de la paire si au moins 1 trade
            if best_for_pair and best_for_pair["trades"] > 0:
                best_rows.append(best_for_pair)
            else:
                # Ajoute quand même une ligne neutre pour visibilité
                best_rows.append({
                    "pair": pair, "session": "-",
                    "w1": 0.0, "w2": 0.0, "w3": 0.0, "w4": 0.0, "w5": 1.0,
                    "trades": 0, "winrate": 0.0, "profit_factor": 0.0, "avg_win": 0.0, "avg_loss": 0.0, "exp": 0.0,
                    "p1": 0.0, "p2": 0.0, "p3": 0.0, "p4": 0.0, "p5": 0.0
                })

            # 4) Breakdown par paire / jour de la semaine / TP (TP unique)
            for sess in sessions:
                trades = session_trades[sess]
                breakdown_rows.extend(
                    build_breakdown_rows_for_pair(pair, sess, trades)
                )

    # 4) Affichage final (1 ligne par paire)
    print_final_best_table(best_rows)

    # --- FILTRE GLOBAL DES PAIRES PERFOMANTES (Pour le breakdown) ---
    filtered_pairs_set = {
        r["pair"] for r in best_rows 
        if r.get("exp", 0.0) >= args.best_exp_threshold and r.get("profit_factor", 0.0) >= args.pf_threshold
    }

    filtered_breakdown_rows = [
        r for r in breakdown_rows
        if r["pair"] in filtered_pairs_set
    ]
    # --- FIN DU FILTRE ---

    # 5) Affichage des actions quotidiennes recommandées (CSV DÉTAILLÉ)
    print_breakdown_table(
        filtered_breakdown_rows, 
        exp_threshold=args.best_exp_threshold,
        pf_threshold=args.pf_threshold
    )
    
    # 6) Affichage CSV des paires "haut rendement" (CSV SIMPLE)
    print_high_exp_pairs_csv(
        best_rows, 
        best_exp_threshold=args.best_exp_threshold,
        pf_threshold=args.pf_threshold
    )

if __name__ == "__main__":
    main()