#!/opt/crypto-monitor/.venv/bin/python
import json
import logging
import os
import time
from collections import defaultdict, deque
from decimal import Decimal, getcontext
from pathlib import Path

import requests

getcontext().prec = 60

TOKEN = "0x302dfaf2cdbe51a18d97186a7384e87cf599877d"
OUT = Path("/opt/crypto-monitor/lyn_watch_addresses.json")
CACHE = Path("/opt/crypto-monitor/lyn_holder_rebuild_cache.json")
LOG = Path("/opt/crypto-monitor/lyn_holder_rebuild.log")
DEX_URL = f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN}"
OFFSET = int(os.getenv("LYN_HOLDER_REBUILD_OFFSET", "1000"))
SLEEP = float(os.getenv("LYN_HOLDER_REBUILD_SLEEP", "0.20"))
MAX_SEGMENTS = int(os.getenv("LYN_HOLDER_REBUILD_MAX_SEGMENTS", "120"))
SAVE_EVERY_SECONDS = int(os.getenv("LYN_HOLDER_REBUILD_SAVE_EVERY_SECONDS", "8"))
TOP_HOLDERS_LIMIT = int(os.getenv("LYN_HOLDER_TOP_HOLDERS_LIMIT", "100"))
CORE_HOLDERS_LIMIT = int(os.getenv("LYN_HOLDER_CORE_LIMIT", "20"))
ZERO = "0x0000000000000000000000000000000000000000"


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.FileHandler(LOG, encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


setup_logging()


def load_env_file(path="/etc/crypto-monitor.env"):
    p = Path(path)
    if not p.exists():
        return
    try:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        logging.warning("env file not readable: %s", p)
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


load_env_file()
KEY = os.getenv("ETHERSCAN_API_KEY") or os.getenv("BSCSCAN_API_KEY")
if not KEY:
    raise SystemExit("missing ETHERSCAN_API_KEY/BSCSCAN_API_KEY")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-lyn-holder-rebuild/continue"})
api_calls = 0


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


def api(params, retry=5):
    global api_calls
    last = None
    for attempt in range(retry):
        api_calls += 1
        try:
            r = SESSION.get(
                "https://api.etherscan.io/v2/api",
                params=params | {"apikey": KEY, "chainid": "56"},
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
        except Exception as e:
            last = e
            time.sleep((attempt + 1) * 1.2)
    raise RuntimeError(f"api failed after retries: {last}")


def fetch_range(start, end, page=1):
    j = api({
        "module": "account",
        "action": "tokentx",
        "contractaddress": TOKEN,
        "startblock": start,
        "endblock": end,
        "page": page,
        "offset": OFFSET,
        "sort": "asc",
    })
    if j.get("status") == "1":
        return j.get("result") or [], j
    result = str(j.get("result") or "")
    message = str(j.get("message") or "")
    if "No transactions found" in result or message == "No transactions found":
        return [], j
    return None, j


def row_key(row):
    return ":".join([
        str(row.get("hash") or ""),
        str(row.get("logIndex") or row.get("transactionIndex") or ""),
        str(row.get("from") or ""),
        str(row.get("to") or ""),
        str(row.get("value") or ""),
    ])


def get_supply():
    j = api({"module": "stats", "action": "tokensupply", "contractaddress": TOKEN})
    if j.get("status") != "1":
        raise SystemExit(f"tokensupply failed: {j}")
    return Decimal(str(j.get("result") or "0"))


def get_latest_block():
    j = api({"module": "proxy", "action": "eth_blockNumber"})
    result = j.get("result")
    if isinstance(result, str) and result.startswith("0x"):
        return int(result, 16)
    raise SystemExit(f"latest block failed: {j}")


def fetch_dex_pairs():
    try:
        r = SESSION.get(DEX_URL, timeout=20)
        data = r.json()
        pairs = data.get("pairs") or []
        out = {}
        for p in pairs:
            pair_addr = str(p.get("pairAddress") or "").lower()
            if not pair_addr:
                continue
            out[pair_addr] = {
                "dex": str(p.get("dexId") or ""),
                "chain": str(p.get("chainId") or ""),
                "base": str(((p.get("baseToken") or {}).get("symbol")) or ""),
                "quote": str(((p.get("quoteToken") or {}).get("symbol")) or ""),
                "liquidity_usd": float(((p.get("liquidity") or {}).get("usd")) or 0),
            }
        return out
    except Exception:
        logging.exception("fetch dex pairs failed")
        return {}


def short_addr(addr):
    addr = (addr or "").lower()
    return addr[:8] + "..." + addr[-6:] if len(addr) > 14 else addr


def serialize_decimal(value, digits=8):
    return str(round(value, digits))


def build_labels(rows, watch, latest, first_block, done_count, remaining_count):
    balances = defaultdict(Decimal)
    stats = defaultdict(lambda: {
        "in": Decimal(0),
        "out": Decimal(0),
        "in_count": 0,
        "out_count": 0,
        "peers": set(),
        "first_ts": 0,
        "last_ts": 0,
    })
    decimals = None

    for row in rows:
        if decimals is None:
            decimals = int(row.get("tokenDecimal") or 18)
        dec = int(row.get("tokenDecimal") or decimals or 18)
        amount = Decimal(str(row.get("value") or "0")) / (Decimal(10) ** dec)
        src = (row.get("from") or "").lower()
        dst = (row.get("to") or "").lower()
        ts = int(row.get("timeStamp") or 0)

        if src and src != ZERO:
            balances[src] -= amount
            stats[src]["out"] += amount
            stats[src]["out_count"] += 1
            stats[src]["first_ts"] = min(stats[src]["first_ts"] or ts, ts) if ts else stats[src]["first_ts"]
            stats[src]["last_ts"] = max(stats[src]["last_ts"], ts)
            if dst:
                stats[src]["peers"].add(dst)
        if dst and dst != ZERO:
            balances[dst] += amount
            stats[dst]["in"] += amount
            stats[dst]["in_count"] += 1
            stats[dst]["first_ts"] = min(stats[dst]["first_ts"] or ts, ts) if ts else stats[dst]["first_ts"]
            stats[dst]["last_ts"] = max(stats[dst]["last_ts"], ts)
            if src:
                stats[dst]["peers"].add(src)

    decimals = decimals or 18
    supply = get_supply() / (Decimal(10) ** decimals)
    dex_pairs = fetch_dex_pairs()
    excluded = set(dex_pairs.keys()) | {ZERO}

    holders = [(addr, bal) for addr, bal in balances.items() if bal > 0 and addr not in excluded]
    holders.sort(key=lambda x: x[1], reverse=True)

    existing_known = {}
    for key in ("suspected_hubs", "candidate_accumulators", "candidate_distributors", "core_top_holders", "top_holders_estimated"):
        for addr in (watch.get(key) or {}):
            existing_known[str(addr).lower()] = key

    top_holders = {}
    for rank, (addr, bal) in enumerate(holders[:TOP_HOLDERS_LIMIT], 1):
        pct = (bal / supply * Decimal(100)) if supply else Decimal(0)
        s = stats[addr]
        top_holders[addr] = {
            "rank": rank,
            "quantity": serialize_decimal(bal),
            "pct_supply": float(pct),
            "known_label": existing_known.get(addr, "unknown"),
            "in": serialize_decimal(s["in"]),
            "out": serialize_decimal(s["out"]),
            "in_count": s["in_count"],
            "out_count": s["out_count"],
            "peers": len(s["peers"]),
            "note": "由LYN tokentx按block range分段重建的前排持仓估算",
        }

    core_top_holders = {}
    for rank, (addr, bal) in enumerate(holders[:CORE_HOLDERS_LIMIT], 1):
        pct = (bal / supply * Decimal(100)) if supply else Decimal(0)
        if pct < Decimal("0.05"):
            continue
        s = stats[addr]
        core_top_holders[addr] = {
            "rank": rank,
            "quantity": serialize_decimal(bal),
            "pct_supply": float(pct),
            "in": serialize_decimal(s["in"]),
            "out": serialize_decimal(s["out"]),
            "in_count": s["in_count"],
            "out_count": s["out_count"],
            "peers": len(s["peers"]),
            "source_confidence": "estimated_partial" if remaining_count else "estimated_fuller",
        }

    suspected_hubs = {}
    candidate_accumulators = {}
    candidate_distributors = {}
    top_holder_lookup = {addr for addr, _ in holders[:TOP_HOLDERS_LIMIT]}

    for addr, s in stats.items():
        if addr in excluded:
            continue
        gross = s["in"] + s["out"]
        if gross <= 0:
            continue
        net = s["in"] - s["out"]
        peers = len(s["peers"])
        current_balance = balances.get(addr, Decimal(0))
        balance_pct = (current_balance / supply * Decimal(100)) if supply and current_balance > 0 else Decimal(0)
        in_count = int(s["in_count"])
        out_count = int(s["out_count"])

        if peers >= 12 and in_count >= 8 and out_count >= 8:
            net_ratio = abs(net) / gross if gross else Decimal(0)
            if net_ratio <= Decimal("0.20"):
                suspected_hubs[addr] = {
                    "gross_flow": serialize_decimal(gross),
                    "net_flow": serialize_decimal(net),
                    "peers": peers,
                    "in_count": in_count,
                    "out_count": out_count,
                    "current_balance": serialize_decimal(current_balance),
                    "source_confidence": "heuristic_partial" if remaining_count else "heuristic",
                }

        if current_balance > 0 and (addr in top_holder_lookup or balance_pct >= Decimal("0.02")):
            if net > 0 and s["in"] >= Decimal("5000") and in_count >= max(3, out_count) and s["in"] >= s["out"] * Decimal("1.5"):
                candidate_accumulators[addr] = {
                    "current_balance": serialize_decimal(current_balance),
                    "pct_supply": float(balance_pct),
                    "net_inflow": serialize_decimal(net),
                    "gross_inflow": serialize_decimal(s["in"]),
                    "gross_outflow": serialize_decimal(s["out"]),
                    "in_count": in_count,
                    "out_count": out_count,
                    "source_confidence": "heuristic_partial" if remaining_count else "heuristic",
                }
            if s["out"] > 0 and out_count >= max(3, in_count) and s["out"] >= s["in"] * Decimal("1.5") and gross >= Decimal("5000"):
                candidate_distributors[addr] = {
                    "current_balance": serialize_decimal(current_balance),
                    "pct_supply": float(balance_pct),
                    "net_outflow": serialize_decimal(-net if net < 0 else Decimal(0)),
                    "gross_inflow": serialize_decimal(s["in"]),
                    "gross_outflow": serialize_decimal(s["out"]),
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
        "rows": len(rows),
        "holders": len(holders),
        "decimals": decimals,
        "total_supply": serialize_decimal(supply),
        "latest_block": latest,
        "first_block": first_block,
        "done_ranges": done_count,
        "remaining_ranges": remaining_count,
        "api_calls": api_calls,
        "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        "scan_mode": "lightweight_continue",
    }
    return watch, holders, supply


def maybe_recommend(cache):
    remaining = len(cache.get("pending_ranges") or [])
    rows = len(cache.get("rows") or {})
    done = len(cache.get("done_ranges") or [])
    if remaining == 0:
        return "当前范围已扫完，可先观察标签质量，再决定是否扩大最近块高频复扫。"
    if done < 40 or rows < 5000:
        return "建议先继续扩大扫描范围，当前样本偏早期，标签稳定性有限。"
    if remaining > 200:
        return "建议继续按当前轻量配置续跑，暂不提高并发或单轮分段数。"
    return "可先观察当前标签；若命中率不足，再温和扩大扫描范围。"


def main():
    cache = load_json(CACHE, {})
    rows_cache = cache.setdefault("rows", {})
    done = {tuple(x) for x in cache.get("done_ranges", []) if isinstance(x, list) and len(x) == 2}
    pending = deque(tuple(x) for x in cache.get("pending_ranges", []) if isinstance(x, list) and len(x) == 2)

    latest = int(cache.get("latest_block") or 0) or get_latest_block()
    first_block = int(cache.get("first_block") or 0)
    if not first_block:
        first_rows, first_j = fetch_range(0, latest, page=1)
        if first_rows is None or not first_rows:
            raise SystemExit(f"cannot recover first block: {first_j}")
        first_block = int(first_rows[0].get("blockNumber") or 0)
        for row in first_rows:
            rows_cache[row_key(row)] = row

    if not pending:
        pending = deque([(first_block, latest)])

    segments = 0
    last_save = time.time()
    logging.info("LYN rebuild start unique=%s done_ranges=%s pending=%s first=%s latest=%s max_segments=%s", len(rows_cache), len(done), len(pending), first_block, latest, MAX_SEGMENTS)

    while pending and segments < MAX_SEGMENTS:
        start, end = pending.popleft()
        if (start, end) in done:
            continue

        segments += 1
        rows, j = fetch_range(start, end, page=1)
        if rows is None:
            logging.info("range issue split start=%s end=%s detail=%s", start, end, str(j)[:240])
            if end > start:
                mid = (start + end) // 2
                pending.appendleft((mid + 1, end))
                pending.appendleft((start, mid))
                continue
            raise SystemExit(f"range api issue {start}-{end}: {j}")

        if len(rows) >= OFFSET and end > start:
            mid = (start + end) // 2
            pending.appendleft((mid + 1, end))
            pending.appendleft((start, mid))
            logging.info("split start=%s end=%s rows=%s span=%s", start, end, len(rows), end - start)
        else:
            complete_rows = list(rows)
            pages = 1
            if len(rows) >= OFFSET and end == start:
                for page in range(2, 11):
                    more, more_j = fetch_range(start, end, page=page)
                    if more is None:
                        logging.info("same-block page issue block=%s page=%s detail=%s", start, page, str(more_j)[:240])
                        break
                    complete_rows.extend(more)
                    pages = page
                    if len(more) < OFFSET:
                        break
                    time.sleep(SLEEP)
            for row in complete_rows:
                rows_cache[row_key(row)] = row
            done.add((start, end))
            logging.info("done start=%s end=%s rows=%s unique=%s pages=%s pending=%s", start, end, len(complete_rows), len(rows_cache), pages, len(pending))

        cache["rows"] = rows_cache
        cache["done_ranges"] = [list(x) for x in sorted(done)]
        cache["pending_ranges"] = [list(x) for x in list(pending)]
        cache["latest_block"] = latest
        cache["first_block"] = first_block
        cache["last_run_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        cache["api_calls"] = api_calls
        cache["segments_this_run"] = segments

        if time.time() - last_save > SAVE_EVERY_SECONDS:
            watch = load_json(OUT, {})
            watch, holders, supply = build_labels(list(rows_cache.values()), watch, latest, first_block, len(done), len(pending))
            save_json(OUT, watch)
            save_json(CACHE, cache)
            last_save = time.time()
            logging.info(
                "saved progress unique=%s done=%s pending=%s holders=%s core=%s hubs=%s accum=%s dist=%s supply=%s api_calls=%s",
                len(rows_cache),
                len(done),
                len(pending),
                len(holders),
                len(watch.get("core_top_holders") or {}),
                len(watch.get("suspected_hubs") or {}),
                len(watch.get("candidate_accumulators") or {}),
                len(watch.get("candidate_distributors") or {}),
                serialize_decimal(supply),
                api_calls,
            )

        time.sleep(SLEEP)

    cache["rows"] = rows_cache
    cache["done_ranges"] = [list(x) for x in sorted(done)]
    cache["pending_ranges"] = [list(x) for x in list(pending)]
    cache["latest_block"] = latest
    cache["first_block"] = first_block
    cache["last_run_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
    cache["api_calls"] = api_calls
    cache["segments_this_run"] = segments

    watch = load_json(OUT, {})
    watch, holders, supply = build_labels(list(rows_cache.values()), watch, latest, first_block, len(done), len(pending))
    save_json(OUT, watch)
    save_json(CACHE, cache)

    logging.info("LYN rebuild complete rows=%s holders=%s done_ranges=%s remaining_ranges=%s api_calls=%s segments=%s", len(rows_cache), len(holders), len(done), len(pending), api_calls, segments)
    print("\n== LYN Holder Rebuild Summary ==")
    print("script", Path(__file__))
    print("log", LOG)
    print("cache", CACHE)
    print("out", OUT)
    print("rows", len(rows_cache))
    print("holders", len(holders))
    print("core_top_holders", len(watch.get("core_top_holders") or {}))
    print("top_holders_estimated", len(watch.get("top_holders_estimated") or {}))
    print("suspected_hubs", len(watch.get("suspected_hubs") or {}))
    print("candidate_accumulators", len(watch.get("candidate_accumulators") or {}))
    print("candidate_distributors", len(watch.get("candidate_distributors") or {}))
    print("first_block", first_block)
    print("latest_block", latest)
    print("done_ranges", len(done))
    print("remaining_ranges", len(pending))
    print("api_calls", api_calls)
    print("total_supply", serialize_decimal(supply))
    print("recommendation", maybe_recommend(cache))
    print("\n== Top holders preview ==")
    for rank, (addr, bal) in enumerate(holders[:15], 1):
        pct = (bal / supply * Decimal(100)) if supply else Decimal(0)
        print(f"{rank:02d}", short_addr(addr), f"qty={serialize_decimal(bal)}", f"pct={pct:.4f}%")


if __name__ == "__main__":
    main()
