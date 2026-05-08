#!/opt/crypto-monitor/.venv/bin/python
import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, getcontext
from pathlib import Path

import requests

getcontext().prec = 60

TOKEN = "0x70be40667385500c5da7f108a022e21b606045dd"
ZERO = "0x0000000000000000000000000000000000000000"
ETHERSCAN_URL = "https://api.etherscan.io/v2/api"

OUT = Path("/opt/crypto-monitor/st_watch_addresses.json")
LEGACY_CACHE = Path("/opt/crypto-monitor/st_holder_rebuild_cache.json")
DB_PATH = Path("/opt/crypto-monitor/st_holder_rebuild_v2.sqlite3")
LOG_PATH = Path("/opt/crypto-monitor/st_holder_rebuild_v2.log")

OFFSET = int(os.getenv("ST_HOLDER_REBUILD_OFFSET", "1000"))
SLEEP = float(os.getenv("ST_HOLDER_REBUILD_SLEEP", "0.18"))
MAX_SEGMENTS = int(os.getenv("ST_HOLDER_REBUILD_MAX_SEGMENTS", "120"))
TOP_HOLDERS_LIMIT = int(os.getenv("ST_HOLDER_TOP_HOLDERS_LIMIT", "100"))
CORE_HOLDERS_LIMIT = int(os.getenv("ST_HOLDER_CORE_LIMIT", "20"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-st-holder-rebuild/v2"})
API_CALLS = 0


@dataclass
class RangeItem:
    start_block: int
    end_block: int
    priority: int


def utc_now():
    return datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG_PATH, encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
        force=True,
    )


def load_env_file(path="/etc/crypto-monitor.env"):
    p = Path(path)
    if not p.exists():
        return
    try:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception as exc:
        logging.warning("env file not readable: %s (%s)", p, exc)
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def load_json(path, default):
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("load json failed: %s", path)
    return default


def save_json(path, data):
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def connect_db(path: Path):
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=FULL")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def connect_existing_db(path: Path):
    uri = f"file:{path}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db(conn: sqlite3.Connection):
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS range_queue (
            start_block INTEGER NOT NULL,
            end_block INTEGER NOT NULL,
            status TEXT NOT NULL,
            priority INTEGER NOT NULL DEFAULT 0,
            attempts INTEGER NOT NULL DEFAULT 0,
            last_error TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL,
            PRIMARY KEY (start_block, end_block)
        );

        CREATE TABLE IF NOT EXISTS transfers (
            row_key TEXT PRIMARY KEY,
            block_number INTEGER NOT NULL,
            time_stamp INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            log_index INTEGER NOT NULL,
            src TEXT NOT NULL,
            dst TEXT NOT NULL,
            value_raw TEXT NOT NULL,
            token_decimal INTEGER NOT NULL,
            raw_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS address_stats (
            address TEXT PRIMARY KEY,
            balance_raw TEXT NOT NULL,
            total_in_raw TEXT NOT NULL,
            total_out_raw TEXT NOT NULL,
            in_count INTEGER NOT NULL,
            out_count INTEGER NOT NULL,
            first_ts INTEGER NOT NULL,
            last_ts INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS address_peers (
            address TEXT NOT NULL,
            peer TEXT NOT NULL,
            PRIMARY KEY (address, peer)
        );

        CREATE INDEX IF NOT EXISTS idx_transfers_block ON transfers(block_number);
        CREATE INDEX IF NOT EXISTS idx_range_queue_status ON range_queue(status, priority, start_block, end_block);
        """
    )
    conn.commit()


def meta_get(conn: sqlite3.Connection, key: str, default=None):
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def meta_set(conn: sqlite3.Connection, key: str, value):
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, str(value)),
    )


def env_key_present():
    return bool(os.getenv("ETHERSCAN_API_KEY") or os.getenv("BSCSCAN_API_KEY"))


def list_tables(conn: sqlite3.Connection):
    rows = conn.execute(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table'
        ORDER BY name
        """
    ).fetchall()
    return [row["name"] for row in rows]


def table_exists(conn: sqlite3.Connection, name: str):
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return bool(row)


def db_overview(conn: sqlite3.Connection):
    expected_tables = ["meta", "range_queue", "transfers", "address_stats", "address_peers"]
    existing_tables = list_tables(conn)
    has_meta = "meta" in existing_tables
    counts = {}
    for table in expected_tables:
        if table_exists(conn, table):
            counts[table] = int(conn.execute(f"SELECT COUNT(*) AS c FROM {table}").fetchone()["c"])
        else:
            counts[table] = None
    return {
        "tables_present": existing_tables,
        "expected_tables_ok": all(table in existing_tables for table in expected_tables),
        "table_counts": counts,
        "first_block": meta_get(conn, "first_block") if has_meta else None,
        "latest_block": meta_get(conn, "latest_block") if has_meta else None,
        "initialized_at": meta_get(conn, "initialized_at") if has_meta else None,
        "last_run_at": meta_get(conn, "last_run_at") if has_meta else None,
        "token_decimals": meta_get(conn, "token_decimals") if has_meta else None,
        "pending_ranges": count_status(conn, "pending") if table_exists(conn, "range_queue") else 0,
        "in_progress_ranges": count_status(conn, "in_progress") if table_exists(conn, "range_queue") else 0,
        "done_ranges": count_status(conn, "done") if table_exists(conn, "range_queue") else 0,
        "split_ranges": count_status(conn, "split") if table_exists(conn, "range_queue") else 0,
        "error_ranges": count_status(conn, "error") if table_exists(conn, "range_queue") else 0,
        "rows": count_transfers(conn) if table_exists(conn, "transfers") else 0,
    }


def api(params, retry=5):
    global API_CALLS
    last = None
    key = os.getenv("ETHERSCAN_API_KEY") or os.getenv("BSCSCAN_API_KEY")
    if not key:
        raise SystemExit("missing ETHERSCAN_API_KEY/BSCSCAN_API_KEY")
    for attempt in range(retry):
        API_CALLS += 1
        try:
            r = SESSION.get(
                ETHERSCAN_URL,
                params=params | {"apikey": key, "chainid": "56"},
                timeout=30,
            )
            j = r.json()
            msg = str(j.get("message") or "")
            res = str(j.get("result") or "")
            if "rate limit" in msg.lower() or "rate limit" in res.lower() or "Max rate" in res:
                time.sleep((attempt + 1) * 1.2)
                last = j
                continue
            return j
        except Exception as exc:
            last = exc
            time.sleep((attempt + 1) * 1.2)
    raise RuntimeError(f"api failed after retries: {last}")


def fetch_range(start, end, page=1):
    j = api(
        {
            "module": "account",
            "action": "tokentx",
            "contractaddress": TOKEN,
            "startblock": start,
            "endblock": end,
            "page": page,
            "offset": OFFSET,
            "sort": "asc",
        }
    )
    if j.get("status") == "1":
        return j.get("result") or [], j
    result = str(j.get("result") or "")
    message = str(j.get("message") or "")
    if "No transactions found" in result or message == "No transactions found":
        return [], j
    return None, j


def get_supply():
    j = api({"module": "stats", "action": "tokensupply", "contractaddress": TOKEN})
    if j.get("status") != "1":
        raise RuntimeError(f"tokensupply failed: {j}")
    return Decimal(str(j.get("result") or "0"))


def get_latest_block():
    j = api({"module": "proxy", "action": "eth_blockNumber"})
    result = j.get("result")
    if isinstance(result, str) and result.startswith("0x"):
        return int(result, 16)
    raise RuntimeError(f"latest block failed: {j}")


def row_key(row):
    return ":".join(
        [
            str(row.get("hash") or ""),
            str(row.get("logIndex") or row.get("transactionIndex") or ""),
            str(row.get("from") or ""),
            str(row.get("to") or ""),
            str(row.get("value") or ""),
        ]
    )


def short_addr(addr):
    addr = (addr or "").lower()
    return addr[:8] + "..." + addr[-6:] if len(addr) > 14 else addr


def serialize_decimal(value, digits=8):
    return str(round(value, digits))


def fetch_dex_pairs():
    try:
        r = SESSION.get(f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN}", timeout=20)
        data = r.json()
        pairs = data.get("pairs") or []
        out = {}
        for pair in pairs:
            pair_addr = str(pair.get("pairAddress") or "").lower()
            if not pair_addr:
                continue
            out[pair_addr] = {
                "dex": str(pair.get("dexId") or ""),
                "chain": str(pair.get("chainId") or ""),
                "base": str(((pair.get("baseToken") or {}).get("symbol")) or ""),
                "quote": str(((pair.get("quoteToken") or {}).get("symbol")) or ""),
                "liquidity_usd": float(((pair.get("liquidity") or {}).get("usd")) or 0),
            }
        return out
    except Exception:
        logging.exception("fetch dex pairs failed")
        return {}


def enqueue_range(conn: sqlite3.Connection, start: int, end: int, priority: int, status="pending"):
    conn.execute(
        """
        INSERT INTO range_queue(start_block, end_block, status, priority, updated_at)
        VALUES(?, ?, ?, ?, ?)
        ON CONFLICT(start_block, end_block)
        DO UPDATE SET
            priority = MIN(priority, excluded.priority),
            status = CASE
                WHEN range_queue.status = 'done' THEN range_queue.status
                ELSE excluded.status
            END,
            updated_at = excluded.updated_at
        """,
        (start, end, status, priority, utc_now()),
    )


def pop_pending_range(conn: sqlite3.Connection):
    row = conn.execute(
        """
        SELECT start_block, end_block, priority
        FROM range_queue
        WHERE status = 'pending'
        ORDER BY priority ASC, (end_block - start_block) DESC, start_block ASC
        LIMIT 1
        """
    ).fetchone()
    if not row:
        return None
    conn.execute(
        """
        UPDATE range_queue
        SET status = 'in_progress', attempts = attempts + 1, updated_at = ?
        WHERE start_block = ? AND end_block = ?
        """,
        (utc_now(), row["start_block"], row["end_block"]),
    )
    return RangeItem(row["start_block"], row["end_block"], row["priority"])


def reset_in_progress(conn: sqlite3.Connection):
    conn.execute(
        "UPDATE range_queue SET status = 'pending', updated_at = ? WHERE status = 'in_progress'",
        (utc_now(),),
    )
    conn.commit()


def update_range_status(conn: sqlite3.Connection, item: RangeItem, status: str, last_error: str = ""):
    conn.execute(
        """
        UPDATE range_queue
        SET status = ?, last_error = ?, updated_at = ?
        WHERE start_block = ? AND end_block = ?
        """,
        (status, last_error[:500], utc_now(), item.start_block, item.end_block),
    )


def ensure_initialized(conn: sqlite3.Connection):
    latest = meta_get(conn, "latest_block")
    first = meta_get(conn, "first_block")
    if latest and first:
        return int(first), int(latest)

    latest_block = get_latest_block()
    first_rows, first_j = fetch_range(0, latest_block, page=1)
    if first_rows is None or not first_rows:
        raise RuntimeError(f"cannot recover first block: {first_j}")
    first_block = int(first_rows[0].get("blockNumber") or 0)

    meta_set(conn, "first_block", first_block)
    meta_set(conn, "latest_block", latest_block)
    meta_set(conn, "initialized_at", utc_now())
    meta_set(conn, "token", TOKEN)
    enqueue_range(conn, first_block, latest_block, priority=0, status="pending")

    inserted = persist_rows(conn, first_rows)
    conn.commit()
    logging.info(
        "initialized cache first_block=%s latest_block=%s inserted_seed_rows=%s",
        first_block,
        latest_block,
        inserted,
    )
    return first_block, latest_block


def persist_rows(conn: sqlite3.Connection, rows):
    local_stats = defaultdict(
        lambda: {
            "balance_delta": 0,
            "in_raw": 0,
            "out_raw": 0,
            "in_count": 0,
            "out_count": 0,
            "first_ts": 0,
            "last_ts": 0,
        }
    )
    local_peers = set()
    inserted = 0

    for row in rows:
        key = row_key(row)
        src = (row.get("from") or "").lower()
        dst = (row.get("to") or "").lower()
        ts = int(row.get("timeStamp") or 0)
        value_raw = int(str(row.get("value") or "0"))
        token_decimal = int(row.get("tokenDecimal") or 18)
        log_index = int(row.get("logIndex") or row.get("transactionIndex") or 0)

        cur = conn.execute(
            """
            INSERT OR IGNORE INTO transfers(
                row_key, block_number, time_stamp, tx_hash, log_index, src, dst, value_raw, token_decimal, raw_json
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                key,
                int(row.get("blockNumber") or 0),
                ts,
                str(row.get("hash") or ""),
                log_index,
                src,
                dst,
                str(value_raw),
                token_decimal,
                json.dumps(row, ensure_ascii=False, separators=(",", ":")),
            ),
        )
        if cur.rowcount != 1:
            continue

        inserted += 1

        if src and src != ZERO:
            item = local_stats[src]
            item["balance_delta"] -= value_raw
            item["out_raw"] += value_raw
            item["out_count"] += 1
            item["first_ts"] = min(item["first_ts"] or ts, ts) if ts else item["first_ts"]
            item["last_ts"] = max(item["last_ts"], ts)
            if dst:
                local_peers.add((src, dst))

        if dst and dst != ZERO:
            item = local_stats[dst]
            item["balance_delta"] += value_raw
            item["in_raw"] += value_raw
            item["in_count"] += 1
            item["first_ts"] = min(item["first_ts"] or ts, ts) if ts else item["first_ts"]
            item["last_ts"] = max(item["last_ts"], ts)
            if src:
                local_peers.add((dst, src))

    if local_stats:
        flush_address_stats(conn, local_stats)
    if local_peers:
        conn.executemany(
            "INSERT OR IGNORE INTO address_peers(address, peer) VALUES(?, ?)",
            list(local_peers),
        )
    return inserted


def flush_address_stats(conn: sqlite3.Connection, local_stats):
    for address, delta in local_stats.items():
        row = conn.execute(
            """
            SELECT balance_raw, total_in_raw, total_out_raw, in_count, out_count, first_ts, last_ts
            FROM address_stats
            WHERE address = ?
            """,
            (address,),
        ).fetchone()

        current = {
            "balance_raw": int(row["balance_raw"]) if row else 0,
            "total_in_raw": int(row["total_in_raw"]) if row else 0,
            "total_out_raw": int(row["total_out_raw"]) if row else 0,
            "in_count": int(row["in_count"]) if row else 0,
            "out_count": int(row["out_count"]) if row else 0,
            "first_ts": int(row["first_ts"]) if row else 0,
            "last_ts": int(row["last_ts"]) if row else 0,
        }

        first_ts = current["first_ts"]
        if delta["first_ts"]:
            first_ts = min(first_ts or delta["first_ts"], delta["first_ts"])

        conn.execute(
            """
            INSERT INTO address_stats(
                address, balance_raw, total_in_raw, total_out_raw, in_count, out_count, first_ts, last_ts
            ) VALUES(?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(address) DO UPDATE SET
                balance_raw = excluded.balance_raw,
                total_in_raw = excluded.total_in_raw,
                total_out_raw = excluded.total_out_raw,
                in_count = excluded.in_count,
                out_count = excluded.out_count,
                first_ts = excluded.first_ts,
                last_ts = excluded.last_ts
            """,
            (
                address,
                str(current["balance_raw"] + delta["balance_delta"]),
                str(current["total_in_raw"] + delta["in_raw"]),
                str(current["total_out_raw"] + delta["out_raw"]),
                current["in_count"] + delta["in_count"],
                current["out_count"] + delta["out_count"],
                first_ts,
                max(current["last_ts"], delta["last_ts"]),
            ),
        )


def count_status(conn: sqlite3.Connection, status: str):
    row = conn.execute("SELECT COUNT(*) AS c FROM range_queue WHERE status = ?", (status,)).fetchone()
    return int(row["c"] or 0)


def count_transfers(conn: sqlite3.Connection):
    row = conn.execute("SELECT COUNT(*) AS c FROM transfers").fetchone()
    return int(row["c"] or 0)


def build_watch_payload(conn: sqlite3.Connection):
    watch = load_json(OUT, {})
    decimals_meta = meta_get(conn, "token_decimals")
    decimals = int(decimals_meta) if decimals_meta else None

    dex_pairs = fetch_dex_pairs()
    excluded = set(dex_pairs.keys()) | {ZERO}
    supply_raw = get_supply()

    peer_counts = {
        row["address"]: int(row["peer_count"])
        for row in conn.execute("SELECT address, COUNT(*) AS peer_count FROM address_peers GROUP BY address")
    }

    existing_known = {}
    for key in ("dex_pairs", "suspected_hubs", "candidate_accumulators", "candidate_distributors", "core_top_holders", "top_holders_estimated"):
        for addr in (watch.get(key) or {}):
            existing_known[str(addr).lower()] = key

    holders = []
    address_rows = conn.execute(
        """
        SELECT address, balance_raw, total_in_raw, total_out_raw, in_count, out_count, first_ts, last_ts
        FROM address_stats
        """
    ).fetchall()

    for row in address_rows:
        if row["address"] in excluded:
            continue
        balance_raw = int(row["balance_raw"])
        if balance_raw <= 0:
            continue
        row_decimals = decimals or int(meta_get(conn, "token_decimals", 18))
        balance = Decimal(balance_raw) / (Decimal(10) ** row_decimals)
        holders.append((row["address"], balance, row))

    holders.sort(key=lambda item: item[1], reverse=True)
    if holders and decimals is None:
        decimals = int(meta_get(conn, "token_decimals", 18))
    decimals = decimals or 18
    supply = supply_raw / (Decimal(10) ** decimals)

    top_holders = {}
    core_top_holders = {}
    suspected_hubs = {}
    candidate_accumulators = {}
    candidate_distributors = {}
    top_holder_lookup = {addr for addr, _, _ in holders[:TOP_HOLDERS_LIMIT]}
    remaining_count = count_status(conn, "pending") + count_status(conn, "in_progress")

    for rank, (addr, bal, row) in enumerate(holders[:TOP_HOLDERS_LIMIT], 1):
        pct = (bal / supply * Decimal(100)) if supply else Decimal(0)
        total_in = Decimal(int(row["total_in_raw"])) / (Decimal(10) ** decimals)
        total_out = Decimal(int(row["total_out_raw"])) / (Decimal(10) ** decimals)
        top_holders[addr] = {
            "rank": rank,
            "quantity": serialize_decimal(bal),
            "pct_supply": float(pct),
            "known_label": existing_known.get(addr, "unknown"),
            "in": serialize_decimal(total_in),
            "out": serialize_decimal(total_out),
            "in_count": int(row["in_count"]),
            "out_count": int(row["out_count"]),
            "peers": peer_counts.get(addr, 0),
            "note": "由ST tokentx按block range分段重建的前排持仓估算（sqlite v2）",
        }
        if rank <= CORE_HOLDERS_LIMIT and pct >= Decimal("0.05"):
            core_top_holders[addr] = {
                "rank": rank,
                "quantity": serialize_decimal(bal),
                "pct_supply": float(pct),
                "in": serialize_decimal(total_in),
                "out": serialize_decimal(total_out),
                "in_count": int(row["in_count"]),
                "out_count": int(row["out_count"]),
                "peers": peer_counts.get(addr, 0),
                "source_confidence": "estimated_partial" if remaining_count else "estimated_fuller",
            }

    for addr, _, row in holders:
        total_in = Decimal(int(row["total_in_raw"])) / (Decimal(10) ** decimals)
        total_out = Decimal(int(row["total_out_raw"])) / (Decimal(10) ** decimals)
        gross = total_in + total_out
        if gross <= 0:
            continue
        net = total_in - total_out
        peers = peer_counts.get(addr, 0)
        balance = Decimal(int(row["balance_raw"])) / (Decimal(10) ** decimals)
        balance_pct = (balance / supply * Decimal(100)) if supply and balance > 0 else Decimal(0)
        in_count = int(row["in_count"])
        out_count = int(row["out_count"])

        if peers >= 12 and in_count >= 8 and out_count >= 8:
            net_ratio = abs(net) / gross if gross else Decimal(0)
            if net_ratio <= Decimal("0.20"):
                suspected_hubs[addr] = {
                    "gross_flow": serialize_decimal(gross),
                    "net_flow": serialize_decimal(net),
                    "peers": peers,
                    "in_count": in_count,
                    "out_count": out_count,
                    "current_balance": serialize_decimal(balance),
                    "source_confidence": "heuristic_partial" if remaining_count else "heuristic",
                }

        if balance > 0 and (addr in top_holder_lookup or balance_pct >= Decimal("0.02")):
            if net > 0 and total_in >= Decimal("5000") and in_count >= max(3, out_count) and total_in >= total_out * Decimal("1.5"):
                candidate_accumulators[addr] = {
                    "current_balance": serialize_decimal(balance),
                    "pct_supply": float(balance_pct),
                    "net_inflow": serialize_decimal(net),
                    "gross_inflow": serialize_decimal(total_in),
                    "gross_outflow": serialize_decimal(total_out),
                    "in_count": in_count,
                    "out_count": out_count,
                    "source_confidence": "heuristic_partial" if remaining_count else "heuristic",
                }
            if total_out > 0 and out_count >= max(3, in_count) and total_out >= total_in * Decimal("1.5") and gross >= Decimal("5000"):
                candidate_distributors[addr] = {
                    "current_balance": serialize_decimal(balance),
                    "pct_supply": float(balance_pct),
                    "net_outflow": serialize_decimal(-net if net < 0 else Decimal(0)),
                    "gross_inflow": serialize_decimal(total_in),
                    "gross_outflow": serialize_decimal(total_out),
                    "in_count": in_count,
                    "out_count": out_count,
                    "source_confidence": "heuristic_partial" if remaining_count else "heuristic",
                }

    watch["dex_pairs"] = dex_pairs
    watch["top_holders_estimated"] = top_holders
    watch["core_top_holders"] = core_top_holders
    watch["suspected_hubs"] = dict(sorted(suspected_hubs.items(), key=lambda item: float(item[1].get("gross_flow") or 0), reverse=True)[:80])
    watch["candidate_accumulators"] = dict(sorted(candidate_accumulators.items(), key=lambda item: float(item[1].get("net_inflow") or 0), reverse=True)[:80])
    watch["candidate_distributors"] = dict(sorted(candidate_distributors.items(), key=lambda item: float(item[1].get("net_outflow") or 0), reverse=True)[:80])
    watch["top_holders_estimated_meta"] = {
        "rows": count_transfers(conn),
        "holders": len(holders),
        "decimals": decimals,
        "total_supply": serialize_decimal(supply),
        "latest_block": int(meta_get(conn, "latest_block", 0)),
        "first_block": int(meta_get(conn, "first_block", 0)),
        "done_ranges": count_status(conn, "done"),
        "remaining_ranges": remaining_count,
        "api_calls": API_CALLS,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "scan_mode": "sqlite_v2",
    }
    return watch, holders, supply


def cmd_status(args):
    data = {
        "db": str(DB_PATH),
        "exists": DB_PATH.exists(),
        "db_writable_parent": os.access(DB_PATH.parent, os.W_OK),
        "legacy_cache_exists": LEGACY_CACHE.exists(),
        "env_key_present": env_key_present(),
        "log": str(LOG_PATH),
        "out": str(OUT),
        "range_queue_ready": False,
    }
    if DB_PATH.exists():
        conn = connect_existing_db(DB_PATH)
        data["db_overview"] = db_overview(conn)
        data["range_queue_ready"] = bool(
            data["db_overview"]["pending_ranges"]
            or data["db_overview"]["in_progress_ranges"]
            or data["db_overview"]["done_ranges"]
            or data["db_overview"]["split_ranges"]
        )
    else:
        data["db_overview"] = {
            "tables_present": [],
            "expected_tables_ok": False,
            "table_counts": {},
            "first_block": None,
            "latest_block": None,
            "initialized_at": None,
            "last_run_at": None,
            "token_decimals": None,
            "pending_ranges": 0,
            "in_progress_ranges": 0,
            "done_ranges": 0,
            "split_ranges": 0,
            "error_ranges": 0,
            "rows": 0,
        }
    print(json.dumps(data, ensure_ascii=False, indent=2))


def cmd_dry_run(args):
    conn = connect_db(DB_PATH)
    init_db(conn)
    meta_set(conn, "token", TOKEN)
    meta_set(conn, "dry_run_at", utc_now())
    latest_block = None
    if args.probe_api:
        latest_block = get_latest_block()
        meta_set(conn, "dry_run_latest_block", latest_block)
    conn.commit()
    data = {
        "db": str(DB_PATH),
        "exists": DB_PATH.exists(),
        "log": str(LOG_PATH),
        "env_key_present": env_key_present(),
        "probe_api": args.probe_api,
        "latest_block_probe": latest_block,
        "db_overview": db_overview(conn),
        "note": "dry-run only created schema/meta and optional latest-block probe; no historical range scan started",
    }
    print(json.dumps(data, ensure_ascii=False, indent=2))


def cmd_finalize(args):
    conn = connect_db(DB_PATH)
    init_db(conn)
    watch, holders, supply = build_watch_payload(conn)
    save_json(OUT, watch)
    logging.info(
        "finalized watch rows=%s holders=%s top_holders=%s core_holders=%s supply=%s",
        count_transfers(conn),
        len(holders),
        len(watch.get("top_holders_estimated") or {}),
        len(watch.get("core_top_holders") or {}),
        serialize_decimal(supply),
    )


def cmd_resume(args):
    conn = connect_db(DB_PATH)
    init_db(conn)
    reset_in_progress(conn)
    first_block, latest_block = ensure_initialized(conn)
    segments = 0
    logging.info(
        "resume start first_block=%s latest_block=%s max_segments=%s db=%s",
        first_block,
        latest_block,
        args.max_segments,
        DB_PATH,
    )

    while segments < args.max_segments:
        item = pop_pending_range(conn)
        if not item:
            break
        segments += 1

        rows, detail = fetch_range(item.start_block, item.end_block, page=1)
        if rows is None:
            if item.end_block > item.start_block:
                mid = (item.start_block + item.end_block) // 2
                enqueue_range(conn, item.start_block, mid, item.priority + 1)
                enqueue_range(conn, mid + 1, item.end_block, item.priority + 1)
                update_range_status(conn, item, "split", str(detail))
                conn.commit()
                logging.info("split start=%s end=%s detail=%s", item.start_block, item.end_block, str(detail)[:240])
                continue
            update_range_status(conn, item, "error", str(detail))
            conn.commit()
            raise RuntimeError(f"range api issue {item.start_block}-{item.end_block}: {detail}")

        if len(rows) >= OFFSET and item.end_block > item.start_block:
            mid = (item.start_block + item.end_block) // 2
            enqueue_range(conn, item.start_block, mid, item.priority + 1)
            enqueue_range(conn, mid + 1, item.end_block, item.priority + 1)
            update_range_status(conn, item, "split", f"rows={len(rows)} span={item.end_block - item.start_block}")
            conn.commit()
            logging.info("split start=%s end=%s rows=%s span=%s", item.start_block, item.end_block, len(rows), item.end_block - item.start_block)
            continue

        complete_rows = list(rows)
        pages = 1
        if len(rows) >= OFFSET and item.end_block == item.start_block:
            for page in range(2, 11):
                more, more_detail = fetch_range(item.start_block, item.end_block, page=page)
                if more is None:
                    logging.warning("same-block page issue block=%s page=%s detail=%s", item.start_block, page, str(more_detail)[:240])
                    break
                complete_rows.extend(more)
                pages = page
                if len(more) < OFFSET:
                    break
                time.sleep(SLEEP)

        inserted = persist_rows(conn, complete_rows)
        if complete_rows:
            meta_set(conn, "token_decimals", complete_rows[0].get("tokenDecimal") or meta_get(conn, "token_decimals", 18))
        meta_set(conn, "latest_block", latest_block)
        meta_set(conn, "first_block", first_block)
        meta_set(conn, "last_run_at", utc_now())
        meta_set(conn, "api_calls_total", int(meta_get(conn, "api_calls_total", 0)) + API_CALLS)
        update_range_status(conn, item, "done", f"rows={len(complete_rows)} inserted={inserted} pages={pages}")
        conn.commit()

        logging.info(
            "done start=%s end=%s rows=%s inserted=%s pages=%s transfers=%s pending=%s done=%s",
            item.start_block,
            item.end_block,
            len(complete_rows),
            inserted,
            pages,
            count_transfers(conn),
            count_status(conn, "pending"),
            count_status(conn, "done"),
        )
        time.sleep(SLEEP)

    logging.info(
        "resume complete segments=%s transfers=%s pending=%s done=%s split=%s error=%s",
        segments,
        count_transfers(conn),
        count_status(conn, "pending"),
        count_status(conn, "done"),
        count_status(conn, "split"),
        count_status(conn, "error"),
    )


def build_parser():
    parser = argparse.ArgumentParser(description="ST holder rebuild v2 with sqlite checkpointing")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("status", help="show sqlite cache status")
    dry_run = sub.add_parser("dry-run", help="lightweight schema and API readiness check without historical rebuild")
    dry_run.add_argument("--probe-api", action="store_true", help="probe latest block from API during dry-run")
    sub.add_parser("finalize", help="build watch output from sqlite cache")

    resume = sub.add_parser("resume", help="continue rebuild using sqlite cache")
    resume.add_argument("--max-segments", type=int, default=MAX_SEGMENTS)
    return parser


def main():
    setup_logging()
    load_env_file()
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "status":
        cmd_status(args)
    elif args.command == "dry-run":
        cmd_dry_run(args)
    elif args.command == "finalize":
        cmd_finalize(args)
    elif args.command == "resume":
        cmd_resume(args)


if __name__ == "__main__":
    main()
