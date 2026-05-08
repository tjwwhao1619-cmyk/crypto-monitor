#!/opt/crypto-monitor/.venv/bin/python
import argparse
import fcntl
import json
import logging
import os
import sqlite3
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


WORKDIR = Path("/opt/crypto-monitor")
DB_PATH = WORKDIR / "st_holder_rebuild_v2.sqlite3"
REBUILD_SCRIPT = WORKDIR / "rebuild_st_holders_continue_v2.py"
STATUS_PATH = WORKDIR / "st_holder_rebuild_autopilot_status.json"
LOG_PATH = WORKDIR / "st_holder_rebuild_autopilot.log"
LOCK_PATH = WORKDIR / "st_holder_rebuild_autopilot.lock"
PYTHON_BIN = WORKDIR / ".venv/bin/python"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def write_status(payload: dict) -> None:
    payload = dict(payload)
    payload["updated_at"] = utc_now()
    tmp = STATUS_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATUS_PATH)


def read_db_stats() -> dict:
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    counts = {
        "transfers": cur.execute("SELECT COUNT(*) FROM transfers").fetchone()[0],
        "address_stats": cur.execute("SELECT COUNT(*) FROM address_stats").fetchone()[0],
        "address_peers": cur.execute("SELECT COUNT(*) FROM address_peers").fetchone()[0],
    }
    status = dict(cur.execute("SELECT status, COUNT(*) FROM range_queue GROUP BY status").fetchall())
    meta = dict(cur.execute("SELECT key, value FROM meta").fetchall())
    conn.close()
    return {
        "counts": counts,
        "status": {
            "done": int(status.get("done", 0)),
            "pending": int(status.get("pending", 0)),
            "in_progress": int(status.get("in_progress", 0)),
            "split": int(status.get("split", 0)),
            "error": int(status.get("error", 0)),
        },
        "meta": {
            key: meta.get(key)
            for key in ("first_block", "latest_block", "last_run_at", "token_decimals", "api_calls_total")
        },
        "db_size": DB_PATH.stat().st_size if DB_PATH.exists() else 0,
    }


def run_batch(max_segments: int) -> subprocess.CompletedProcess:
    cmd = [str(PYTHON_BIN), str(REBUILD_SCRIPT), "resume", "--max-segments", str(max_segments)]
    return subprocess.run(cmd, cwd=WORKDIR, check=False)


def run_finalize() -> subprocess.CompletedProcess:
    cmd = [str(PYTHON_BIN), str(REBUILD_SCRIPT), "finalize"]
    return subprocess.run(cmd, cwd=WORKDIR, check=False)


def acquire_lock():
    handle = LOCK_PATH.open("w", encoding="utf-8")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise SystemExit("autopilot already running")
    handle.write(str(os.getpid()))
    handle.flush()
    return handle


def build_parser():
    parser = argparse.ArgumentParser(description="Unattended ST holder rebuild supervisor")
    parser.add_argument("--batch-segments", type=int, default=int(os.getenv("ST_HOLDER_AUTOPILOT_BATCH_SEGMENTS", "120")))
    parser.add_argument("--sleep-seconds", type=float, default=float(os.getenv("ST_HOLDER_AUTOPILOT_SLEEP_SECONDS", "2")))
    parser.add_argument("--max-idle-rounds", type=int, default=int(os.getenv("ST_HOLDER_AUTOPILOT_MAX_IDLE_ROUNDS", "8")))
    parser.add_argument("--max-load1", type=float, default=float(os.getenv("ST_HOLDER_AUTOPILOT_MAX_LOAD1", "8.0")))
    parser.add_argument("--pause-on-high-load", type=float, default=float(os.getenv("ST_HOLDER_AUTOPILOT_PAUSE_ON_HIGH_LOAD", "30")))
    return parser


def main():
    args = build_parser().parse_args()
    setup_logging()
    lock_handle = acquire_lock()
    idle_rounds = 0
    round_no = 0
    started_at = utc_now()

    logging.info(
        "autopilot start pid=%s batch_segments=%s sleep_seconds=%s max_idle_rounds=%s max_load1=%s",
        os.getpid(),
        args.batch_segments,
        args.sleep_seconds,
        args.max_idle_rounds,
        args.max_load1,
    )

    try:
        while True:
            before = read_db_stats()
            remaining = before["status"]["pending"] + before["status"]["in_progress"]
            load1 = os.getloadavg()[0]

            if remaining == 0:
                logging.info("no remaining ranges; running finalize")
                finalize = run_finalize()
                final_stats = read_db_stats()
                write_status(
                    {
                        "state": "completed",
                        "pid": os.getpid(),
                        "started_at": started_at,
                        "round": round_no,
                        "batch_segments": args.batch_segments,
                        "last_returncode": finalize.returncode,
                        "load1": load1,
                        "db": final_stats,
                    }
                )
                if finalize.returncode != 0:
                    raise SystemExit(f"finalize failed with code {finalize.returncode}")
                logging.info("autopilot complete; finalize succeeded")
                return

            if load1 > args.max_load1:
                logging.warning("load1 %.2f > %.2f; sleeping %.1fs", load1, args.max_load1, args.pause_on_high_load)
                write_status(
                    {
                        "state": "paused_high_load",
                        "pid": os.getpid(),
                        "started_at": started_at,
                        "round": round_no,
                        "batch_segments": args.batch_segments,
                        "load1": load1,
                        "db": before,
                    }
                )
                time.sleep(args.pause_on_high_load)
                continue

            round_no += 1
            logging.info(
                "round=%s start pending=%s done=%s split=%s error=%s transfers=%s load1=%.2f",
                round_no,
                before["status"]["pending"],
                before["status"]["done"],
                before["status"]["split"],
                before["status"]["error"],
                before["counts"]["transfers"],
                load1,
            )
            write_status(
                {
                    "state": "running",
                    "pid": os.getpid(),
                    "started_at": started_at,
                    "round": round_no,
                    "batch_segments": args.batch_segments,
                    "load1": load1,
                    "db_before": before,
                }
            )

            result = run_batch(args.batch_segments)
            after = read_db_stats()

            progress = {
                "transfers": after["counts"]["transfers"] - before["counts"]["transfers"],
                "address_stats": after["counts"]["address_stats"] - before["counts"]["address_stats"],
                "address_peers": after["counts"]["address_peers"] - before["counts"]["address_peers"],
                "done": after["status"]["done"] - before["status"]["done"],
                "pending": after["status"]["pending"] - before["status"]["pending"],
                "split": after["status"]["split"] - before["status"]["split"],
                "error": after["status"]["error"] - before["status"]["error"],
                "db_size": after["db_size"] - before["db_size"],
            }

            logging.info(
                "round=%s complete rc=%s progress=%s after_pending=%s after_done=%s after_split=%s after_error=%s",
                round_no,
                result.returncode,
                json.dumps(progress, ensure_ascii=False, sort_keys=True),
                after["status"]["pending"],
                after["status"]["done"],
                after["status"]["split"],
                after["status"]["error"],
            )
            write_status(
                {
                    "state": "running",
                    "pid": os.getpid(),
                    "started_at": started_at,
                    "round": round_no,
                    "batch_segments": args.batch_segments,
                    "last_returncode": result.returncode,
                    "progress": progress,
                    "db_after": after,
                    "load1": os.getloadavg()[0],
                }
            )

            if result.returncode != 0:
                raise SystemExit(f"resume batch failed with code {result.returncode}")
            if progress["error"] > 0 or after["status"]["error"] > 0:
                raise SystemExit("range_queue error count increased")

            if progress["transfers"] <= 0 and progress["done"] <= 0:
                idle_rounds += 1
                logging.warning("round=%s had no effective progress; idle_rounds=%s", round_no, idle_rounds)
            else:
                idle_rounds = 0

            if idle_rounds >= args.max_idle_rounds:
                write_status(
                    {
                        "state": "stopped_idle",
                        "pid": os.getpid(),
                        "started_at": started_at,
                        "round": round_no,
                        "batch_segments": args.batch_segments,
                        "idle_rounds": idle_rounds,
                        "db": after,
                    }
                )
                raise SystemExit(f"stopped after {idle_rounds} idle rounds")

            time.sleep(args.sleep_seconds)
    finally:
        try:
            lock_handle.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
