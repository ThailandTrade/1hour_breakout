#!/usr/bin/env python3
# orchestrator.py — realtime streaming logs, UTF-8, Windows-friendly

import os
import sys
import time
import subprocess
from dotenv import load_dotenv

load_dotenv()

INTERVAL_SEC = float(os.getenv("ORCH_INTERVAL_SEC", "30"))
PYTHON = os.getenv("PYTHON_BIN", sys.executable)

SCRIPTS = {
    "fetch": os.getenv("FETCH_SCRIPT", "mt5_bulk_fetch_to_pg.py"),
    "structure": os.getenv("STRUCT_SCRIPT", "update_pairs_structure.py"),
    "aoi": os.getenv("AOI_SCRIPT", "aoi_batch.py"),  # your latest batch script
}

PAIRS = os.getenv("PAIRS_FILE", "pairs.txt")
TFS   = os.getenv("TIMEFRAMES_FILE", "timeframes.txt")

# Optional AOI params (kept for future use; your current aoi_batch.py hardcodes 1D=10..40 & 1W=10..60)
AOI_MIN_PIPS  = os.getenv("AOI_MIN_PIPS", "10")
AOI_MAX_PIPS  = os.getenv("AOI_MAX_PIPS", "60")

def stream_run(cmd, name: str) -> int:
    print(f"[RUN] {name}: {' '.join(cmd)}", flush=True)
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env.setdefault("PYTHONUTF8", "1")
    env.setdefault("PYTHONUNBUFFERED", "1")

    # On Windows, universal_newlines=True + bufsize=1 gives line-buffered text mode
    p = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        env=env,
        text=True,
        bufsize=1,
        universal_newlines=True,
    )

    try:
        assert p.stdout is not None
        for line in p.stdout:
            # Forward child output as-is
            print(line, end="")
    finally:
        ret = p.wait()
        print(f"[DONE] {name}: rc={ret}", flush=True)
        return ret

def run_or_die(cmd, name: str) -> None:
    rc = stream_run(cmd, name)
    if rc != 0:
        print(f"[FATAL] Step '{name}' failed with rc={rc}. Aborting loop.", flush=True)
        sys.exit(rc)

def main():
    print("[ORCH] Started. Press Ctrl+C to stop.", flush=True)
    try:
        while True:
            # 1) FETCH
            run_or_die([PYTHON, SCRIPTS["fetch"], "--pairs", PAIRS, "--timeframes", TFS], "fetch")

            # 2) STRUCTURE
            run_or_die([PYTHON, SCRIPTS["structure"], "--all"], "structure")

            # 3) AOI (full delete + insert inside the script; adjust flags if needed)
            # If you want to limit delete to tf='mtf', add: "--delete-mode", "mtf"
            run_or_die([PYTHON, SCRIPTS["aoi"]], "aoi")

            print(f"[SLEEP] {INTERVAL_SEC}s\n---", flush=True)
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print("\n[ORCH] Stopped by user.", flush=True)

if __name__ == "__main__":
    main()
