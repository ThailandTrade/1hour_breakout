#!/usr/bin/env python3
# update_pairs_structure_batch_incremental.py
"""
Batch + Incrémental (Option A): parcourt toutes les paires et timeframes, calcule la structure
en respectant STRICTEMENT la logique existante, et reprend là où on s'est arrêté.

- pairs.txt      : CSV "type,pair" (au minimum une colonne 'pair', ex: MAJOR,EURUSD)
- timeframes.txt : une ligne par TF (ex: 1w, 1d, 4h, 2h, 1h, 30m, 15m)

Table source (Option A): candles_mt5_{pair_lower}_{tf}
Persistence dans pairs_structure:
  - hh, hl, lh, ll
  - structure_point_high, structure_point_low
  - last_analyzed_ts (BIGINT ms, ts de la DERNIÈRE bougie TRAITÉE)

Autocommit: ON (chaque upsert est immédiatement committé).

NOUVEAU:
- Plus de --all : par défaut, batch sur toutes les paires/timeframes.
- Filtre optionnel: --pair-filter "EURUSD,GBPUSD" (ou env PAIR_FILTER) en batch.
- Mode single toujours dispo via --pair + --timeframe + --source.
- NEW: --pairs "EURUSD GBPUSD" ou "EURUSD,GBPUSD" pour override pairs.txt en batch.
"""

import argparse
import csv
import os
import re
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
from decimal import Decimal, ROUND_HALF_UP

# -------------------- Connexion & utils --------------------
def load_pg_conninfo_from_env() -> dict:
    load_dotenv()
    return {
        "host": os.getenv("PG_HOST", "127.0.0.1"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "dbname": os.getenv("PG_DB", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "postgres"),
        "sslmode": os.getenv("PG_SSLMODE", "disable"),
    }

def sanitize_name(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s[:55]

# --- Arrondi stable ---
def make_rounder(decimals: int):
    q = Decimal(1).scaleb(-decimals)
    def _r(x):
        if x is None:
            return None
        return float(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))
    return _r

def ensure_pairs_structure(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pairs_structure (
            pair TEXT NOT NULL,
            timeframe TEXT NOT NULL,
            hh DOUBLE PRECISION NOT NULL DEFAULT 0,
            hl DOUBLE PRECISION NOT NULL DEFAULT 0,
            lh DOUBLE PRECISION NOT NULL DEFAULT 0,
            ll DOUBLE PRECISION NOT NULL DEFAULT 0,
            structure_point_high DOUBLE PRECISION,
            structure_point_low  DOUBLE PRECISION,
            last_analyzed_ts BIGINT,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (pair, timeframe)
        );
    """)
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='pairs_structure' AND column_name='structure_point_high'
            ) THEN
                ALTER TABLE pairs_structure ADD COLUMN structure_point_high DOUBLE PRECISION;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='pairs_structure' AND column_name='structure_point_low'
            ) THEN
                ALTER TABLE pairs_structure ADD COLUMN structure_point_low DOUBLE PRECISION;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name='pairs_structure' AND column_name='last_analyzed_ts'
            ) THEN
                ALTER TABLE pairs_structure ADD COLUMN last_analyzed_ts BIGINT;
            END IF;
        END$$;
    """)

def upsert_state_full(cur, pair: str, timeframe: str,
                      hh: float, hl: float, lh: float, ll: float,
                      structure_point_high: float, structure_point_low: float,
                      last_analyzed_ts: int):
    # arrondi avant écriture
    hh = rnd(hh); hl = rnd(hl); lh = rnd(lh); ll = rnd(ll)
    structure_point_high = rnd(structure_point_high)
    structure_point_low  = rnd(structure_point_low)
    cur.execute("""
        INSERT INTO pairs_structure
            (pair, timeframe, hh, hl, lh, ll, structure_point_high, structure_point_low, last_analyzed_ts, updated_at)
        VALUES
            (%s,   %s,        %s, %s, %s, %s, %s,                  %s,                 %s,               now())
        ON CONFLICT (pair, timeframe)
        DO UPDATE SET
            hh = EXCLUDED.hh,
            hl = EXCLUDED.hl,
            lh = EXCLUDED.lh,
            ll = EXCLUDED.ll,
            structure_point_high = EXCLUDED.structure_point_high,
            structure_point_low  = EXCLUDED.structure_point_low,
            last_analyzed_ts     = EXCLUDED.last_analyzed_ts,
            updated_at           = now();
    """, (pair, timeframe, hh, hl, lh, ll, structure_point_high, structure_point_low, last_analyzed_ts))

# -------------------- Lecture inputs --------------------
def read_pairs(pairs_file: str):
    out = []
    with open(pairs_file, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if "pair" not in (reader.fieldnames or []):
            raise RuntimeError("pairs.txt doit contenir une colonne 'pair' (ex: type,pair)")
        for row in reader:
            p = (row.get("pair") or "").strip().upper()
            if p:
                out.append(p)
    return out

def read_timeframes(tf_file: str):
    out = []
    with open(tf_file, "r", encoding="utf-8") as f:
        for line in f:
            tf = line.strip().lower()
            if tf:
                out.append(tf)
    return out

def parse_pair_filter(arg: str | None):
    """
    Retourne un set (uppercase) des paires à garder, ou None si pas de filtre.
    Accepte séparateurs ',' ou ';' et espaces. Lisible aussi via env PAIR_FILTER.
    """
    s = (arg or os.getenv("PAIR_FILTER") or "").strip()
    if not s:
        return None
    items = [x.strip().upper() for x in re.split(r"[;,]|\s+", s) if x.strip()]
    return set(items) if items else None

def parse_pairs_cli(arg: str | None):
    """
    Parse --pairs: liste d'override (espaces ou virgules).
    Exemples:
      --pairs "EURUSD GBPUSD USDJPY"
      --pairs "EURUSD,GBPUSD,USDJPY"
    """
    if not arg:
        return None
    tokens = [t.strip().upper() for t in re.split(r"[,\s]+", arg.strip()) if t.strip()]
    return [t for t in tokens if len(t) >= 6] or None

# -------------------- Cœur incrémental --------------------
def process_one_pair_tf_incremental(conn, pair: str, timeframe: str, source_table: str):
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        # 🔹 Déterminer précision selon paire
        decimals = 3 if "JPY" in pair.upper() else default_decimals
        global rnd
        rnd = make_rounder(decimals)

        # Vérif colonnes
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s
        """, (source_table,))
        cols = {r[0] for r in cur.fetchall()}
        if not {"ts", "close", "open"}.issubset(cols):
            print(f"[SKIP] {source_table} manque colonnes (ts, close, open) -> skip.")
            return

        ensure_pairs_structure(cur)

        # Récupère état précédent
        cur.execute("""
            SELECT hh, hl, lh, ll, structure_point_high, structure_point_low, last_analyzed_ts
            FROM pairs_structure
            WHERE pair=%s AND timeframe=%s
        """, (pair, timeframe))
        row_state = cur.fetchone()

        if row_state and row_state["last_analyzed_ts"] is not None:
            last_ts = int(row_state["last_analyzed_ts"])
            hh = float(row_state["hh"]) if row_state["hh"] is not None else 0.0
            hl = float(row_state["hl"]) if row_state["hl"] is not None else 0.0
            lh = float(row_state["lh"]) if row_state["lh"] is not None else 0.0
            ll = float(row_state["ll"]) if row_state["ll"] is not None else 0.0
            structure_point_high = float(row_state["structure_point_high"]) if row_state["structure_point_high"] is not None else 0.0
            structure_point_low  = float(row_state["structure_point_low"])  if row_state["structure_point_low"]  is not None else 0.0

            state = "bullish" if (hh > 0 or hl > 0) and (lh == 0 and ll == 0) else "bearish"

            cur.execute(f"SELECT close FROM {source_table} WHERE ts = %s", (last_ts,))
            rc = cur.fetchone()
            if rc and rc["close"] is not None:
                previous_close = float(rc["close"])
                cur.execute(f"""
                    SELECT ts, close, open FROM {source_table}
                    WHERE close IS NOT NULL AND ts > %s
                    ORDER BY ts ASC
                """, (last_ts,))
            else:
                cur.execute(f"""
                    SELECT ts, close, open FROM {source_table}
                    WHERE close IS NOT NULL AND ts <= %s
                    ORDER BY ts DESC LIMIT 1
                """, (last_ts,))
                prev = cur.fetchone()
                if not prev:
                    previous_close = None
                    cur.execute(f"""
                        SELECT ts, close, open FROM {source_table}
                        WHERE close IS NOT NULL ORDER BY ts ASC
                    """)
                else:
                    previous_close = float(prev["close"])
                    anchor_ts = int(prev["ts"])
                    cur.execute(f"""
                        SELECT ts, close, open FROM {source_table}
                        WHERE close IS NOT NULL AND ts > %s
                        ORDER BY ts ASC
                    """, (anchor_ts,))
            rows = cur.fetchall()

            if not rows:
                print(f"[UP-TO-DATE] {pair} {timeframe}: no new candles after ts={last_ts}.")
                return

            print(f"[RESUME] {pair} {timeframe} from ts={last_ts} (state={state})")

        else:
            cur.execute(f"""
                SELECT ts, close, open FROM {source_table}
                WHERE close IS NOT NULL ORDER BY ts ASC
            """)
            rows = cur.fetchall()
            if not rows:
                upsert_state_full(cur, pair, timeframe, 0, 0, 0, 0, 0, 0, None)
                print(f"[INIT-ZERO] {pair} {timeframe}: No candles.")
                return

            first_ts = int(rows[0]["ts"])
            first_close = float(rows[0]["close"])
            first_open = float(rows[0]["open"])

            if first_close > first_open:
                previous_close = first_close
                state = "bullish"
                hh = rnd(first_close); hl = rnd(first_open)
                lh = 0; ll = 0
                structure_point_high = hh; structure_point_low = hl
            else:
                previous_close = first_close
                state = "bearish"
                hh = 0; hl = 0
                current_high = first_open; current_low = first_close
                lh = rnd(current_high); ll = rnd(current_low)
                structure_point_high = lh; structure_point_low = ll

            rows = rows[1:]
            if not rows:
                upsert_state_full(cur, pair, timeframe, hh, hl, lh, ll,
                                  structure_point_high, structure_point_low, first_ts)
                print(f"[SNAPSHOT] {pair} {timeframe}: only first candle.")
                return

        last_ts_processed = None
        for r in rows:
            close = float(r["close"])
            candle_ts = int(r["ts"])

            if state == "bullish":
                if close > hh:
                    hh = rnd(close)
                    hl = rnd(structure_point_low)
                    lh = 0; ll = 0
                    structure_point_high = rnd(close)
                    upsert_state_full(cur, pair, timeframe, hh, hl, lh, ll,
                                      structure_point_high, structure_point_low, candle_ts)
                elif (close < hh and close > hl and close < previous_close):
                    structure_point_low = rnd(close)
                elif (close < hh and close > hl and close > previous_close):
                    structure_point_high = rnd(close)
                else:
                    hh = 0; hl = 0
                    lh = rnd(structure_point_high)
                    ll = rnd(close)
                    structure_point_low = rnd(close)
                    state = "bearish"
                    upsert_state_full(cur, pair, timeframe, hh, hl, lh, ll,
                                      structure_point_high, structure_point_low, candle_ts)

            elif state == "bearish":
                if close < ll:
                    hh = 0; hl = 0
                    lh = rnd(structure_point_high)
                    ll = rnd(close)
                    structure_point_low = rnd(close)
                    upsert_state_full(cur, pair, timeframe, hh, hl, lh, ll,
                                      structure_point_high, structure_point_low, candle_ts)
                elif (close < lh and close > ll and close < previous_close):
                    structure_point_low = rnd(close)
                elif (close < lh and close > ll and close > previous_close):
                    structure_point_high = rnd(close)
                else:
                    hh = rnd(close)
                    hl = rnd(structure_point_low)
                    lh = 0; ll = 0
                    structure_point_high = rnd(close)
                    state = "bullish"
                    upsert_state_full(cur, pair, timeframe, hh, hl, lh, ll,
                                      structure_point_high, structure_point_low, candle_ts)

            previous_close = close
            last_ts_processed = candle_ts

        if last_ts_processed is not None:
            hh = rnd(hh); hl = rnd(hl); lh = rnd(lh); ll = rnd(ll)
            structure_point_high = rnd(structure_point_high)
            structure_point_low  = rnd(structure_point_low)
            upsert_state_full(cur, pair, timeframe, hh, hl, lh, ll,
                              structure_point_high, structure_point_low, last_ts_processed)
            print(f"[SNAPSHOT] {pair} {timeframe} | last_ts={last_ts_processed}")

        print(f"[DONE] {pair} {timeframe} ({decimals} decimals)")

# -------------------- Main --------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pairs-file", default=os.getenv("PAIRS_FILE", "pairs.txt"))
    ap.add_argument("--timeframes-file", default=os.getenv("TIMEFRAMES_FILE", "timeframes.txt"))
    # override pairs list for batch
    ap.add_argument("--pairs", default=None,
                    help='Override de la liste des paires pour le batch, ex: "EURUSD GBPUSD" ou "EURUSD,GBPUSD"')
    # mode single (optionnel)
    ap.add_argument("--pair")
    ap.add_argument("--timeframe")
    ap.add_argument("--source")
    # options
    ap.add_argument("--decimals", type=int, default=5, help="Nombre de décimales par défaut (défaut 5).")
    ap.add_argument("--pair-filter", default=None,
                    help="Filtre paires en batch (ex: 'EURUSD,GBPUSD'). Peut aussi venir de PAIR_FILTER.")
    args = ap.parse_args()

    global default_decimals
    default_decimals = args.decimals

    conn = psycopg2.connect(**load_pg_conninfo_from_env())
    conn.autocommit = True

    try:
        # --- MODE SINGLE si les 3 sont fournis ---
        if args.pair and args.timeframe and args.source:
            pair = args.pair.upper()
            tf = args.timeframe.lower()
            src = sanitize_name(args.source)
            process_one_pair_tf_incremental(conn, pair, tf, src)
            return

        # --- MODE BATCH PAR DÉFAUT ---
        # 1) Détermine la liste de paires (override via --pairs sinon pairs.txt)
        if args.pairs:
            pairs = parse_pairs_cli(args.pairs) or []
            print(f"[INFO] Using pairs from --pairs: {', '.join(pairs) if pairs else '(empty)'}")
        else:
            pairs = read_pairs(args.pairs_file)
            print(f"[INFO] Using pairs file: {args.pairs_file}")

        if not pairs:
            print("[INFO] Aucune paire fournie/trouvée; rien à faire.")
            return

        # 2) Timeframes
        tfs = read_timeframes(args.timeframes_file)
        if not tfs:
            print("[INFO] Aucun timeframe trouvé; rien à faire.")
            return

        # 3) Applique un filtre éventuel
        filt = parse_pair_filter(args.pair_filter)
        if filt:
            before = len(pairs)
            pairs = [p for p in pairs if p.upper() in filt]
            print(f"[INFO] pair-filter appliqué: {before} -> {len(pairs)} paires")
            if not pairs:
                print("[INFO] Aucun match après pair-filter; rien à faire.")
                return

        # 4) Process
        total = 0
        for pair in pairs:
            pair_lower = pair.lower()
            for tf in tfs:
                source_table = sanitize_name(f"candles_mt5_{pair_lower}_{tf}")
                print(f"\n[RUN] {pair} {tf} -> {source_table}")
                try:
                    process_one_pair_tf_incremental(conn, pair, tf, source_table)
                    total += 1
                except Exception as e:
                    print(f"[ERROR] {pair} {tf} -> {e}")
        print(f"\n[SUMMARY] Combos traités: {total}")

    finally:
        conn.close()

if __name__ == "__main__":
    main()
