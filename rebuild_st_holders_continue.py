import json
import os
import time
from collections import defaultdict, deque
from decimal import Decimal, getcontext
from pathlib import Path

import requests

getcontext().prec = 60

TOKEN = "0x70be40667385500c5da7f108a022e21b606045dd"
OUT = Path("/opt/crypto-monitor/st_watch_addresses.json")
CACHE = Path("/opt/crypto-monitor/st_holder_rebuild_cache.json")
OFFSET = 1000
SLEEP = float(os.getenv("ST_HOLDER_REBUILD_SLEEP", "0.18"))
MAX_SEGMENTS = int(os.getenv("ST_HOLDER_REBUILD_MAX_SEGMENTS", "4000"))
ZERO = "0x0000000000000000000000000000000000000000"

def load_env_file(path="/etc/crypto-monitor.env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
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
SESSION.headers.update({"User-Agent": "crypto-monitor-st-holder-rebuild/continue"})

api_calls = 0

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

if not CACHE.exists():
    raise SystemExit(f"cache not found: {CACHE}")

cache = json.loads(CACHE.read_text(encoding="utf-8"))
rows_cache = cache.setdefault("rows", {})
done = {tuple(x) for x in cache.get("done_ranges", []) if isinstance(x, list) and len(x) == 2}

latest = int(cache.get("latest_block") or 0) or get_latest_block()
first_block = int(cache.get("first_block") or 0)

if not first_block:
    first_rows, first_j = fetch_range(0, latest, page=1)
    if first_rows is None or not first_rows:
        raise SystemExit(f"cannot recover first block: {first_j}")
    first_block = int(first_rows[0].get("blockNumber") or 0)
    for row in first_rows:
        rows_cache[row_key(row)] = row

# 重建待扫区间：从 first-latest 开始，遇到 done 就跳过；遇到满 1000 就继续拆。
queue = deque([(first_block, latest)])
segments = 0
last_save = time.time()

print("loaded cache unique", len(rows_cache), "done_ranges", len(done), "first", first_block, "latest", latest)

while queue and segments < MAX_SEGMENTS:
    start, end = queue.popleft()
    if (start, end) in done:
        continue

    segments += 1
    rows, j = fetch_range(start, end, page=1)

    if rows is None:
        print("range issue split", start, end, str(j)[:240])
        if end > start:
            mid = (start + end) // 2
            queue.appendleft((mid + 1, end))
            queue.appendleft((start, mid))
            continue
        raise SystemExit(f"range api issue {start}-{end}: {j}")

    if len(rows) >= OFFSET and end > start:
        mid = (start + end) // 2
        queue.appendleft((mid + 1, end))
        queue.appendleft((start, mid))
        print(f"split {start}-{end} rows>=1000 span={end-start}")
    else:
        complete_rows = list(rows)
        pages = 1
        if len(rows) >= OFFSET and end == start:
            for page in range(2, 11):
                more, more_j = fetch_range(start, end, page=page)
                if more is None:
                    print("same-block page issue", start, page, str(more_j)[:240])
                    break
                complete_rows.extend(more)
                pages = page
                if len(more) < OFFSET:
                    break
                time.sleep(SLEEP)

        for row in complete_rows:
            rows_cache[row_key(row)] = row

        done.add((start, end))
        print(f"done {start}-{end} rows={len(complete_rows)} unique={len(rows_cache)} pages={pages}")

    cache["rows"] = rows_cache
    cache["done_ranges"] = [list(x) for x in done]
    cache["latest_block"] = latest
    cache["first_block"] = first_block

    if time.time() - last_save > 8:
        CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")
        last_save = time.time()
        print("saved cache unique", len(rows_cache), "done", len(done), "queue", len(queue), "api_calls", api_calls)

    time.sleep(SLEEP)

CACHE.write_text(json.dumps(cache, ensure_ascii=False), encoding="utf-8")

rows = list(rows_cache.values())
print("rebuild rows unique", len(rows), "api_calls", api_calls, "segments", segments, "queue_remaining", len(queue))

balances = defaultdict(Decimal)
stats = defaultdict(lambda: {"in": Decimal(0), "out": Decimal(0), "in_count": 0, "out_count": 0, "peers": set()})
decimals = None

for row in rows:
    if decimals is None:
        decimals = int(row.get("tokenDecimal") or 18)
    dec = int(row.get("tokenDecimal") or decimals or 18)
    amount = Decimal(str(row.get("value") or "0")) / (Decimal(10) ** dec)
    src = (row.get("from") or "").lower()
    dst = (row.get("to") or "").lower()

    if src and src != ZERO:
        balances[src] -= amount
        stats[src]["out"] += amount
        stats[src]["out_count"] += 1
        if dst:
            stats[src]["peers"].add(dst)
    if dst and dst != ZERO:
        balances[dst] += amount
        stats[dst]["in"] += amount
        stats[dst]["in_count"] += 1
        if src:
            stats[dst]["peers"].add(src)

decimals = decimals or 18
supply = get_supply() / (Decimal(10) ** decimals)

watch = json.loads(OUT.read_text(encoding="utf-8")) if OUT.exists() else {}
known = {}
for key in ["dex_pairs", "suspected_hubs", "candidate_accumulators", "candidate_distributors"]:
    for addr in (watch.get(key) or {}):
        known[addr.lower()] = key

holders = [(addr, bal) for addr, bal in balances.items() if bal > 0]
holders.sort(key=lambda x: x[1], reverse=True)

top_holders = {}
for rank, (addr, bal) in enumerate(holders[:100], 1):
    pct = (bal / supply * Decimal(100)) if supply else Decimal(0)
    s = stats[addr]
    top_holders[addr] = {
        "rank": rank,
        "quantity": str(round(bal, 8)),
        "pct_supply": float(pct),
        "known_label": known.get(addr, "unknown"),
        "in": str(round(s["in"], 8)),
        "out": str(round(s["out"], 8)),
        "in_count": s["in_count"],
        "out_count": s["out_count"],
        "peers": len(s["peers"]),
        "note": "由tokentx按block range分段重建的前排持仓估算",
    }

watch["top_holders_estimated"] = top_holders
watch["top_holders_estimated_meta"] = {
    "rows": len(rows),
    "holders": len(holders),
    "decimals": decimals,
    "total_supply": str(round(supply, 8)),
    "latest_block": latest,
    "first_block": first_block,
    "done_ranges": len(done),
    "remaining_ranges": len(queue),
    "api_calls": api_calls,
    "updated_at": time.strftime("%Y-%m-%d %H:%M:%S"),
}
OUT.write_text(json.dumps(watch, ensure_ascii=False, indent=2), encoding="utf-8")

print("\n== Summary ==")
print("rows", len(rows))
print("holders", len(holders))
print("total_supply", f"{supply:,.4f}")
print("done_ranges", len(done), "remaining_ranges", len(queue), "api_calls", api_calls)

print("\n== Top holders estimated ==")
for rank, (addr, bal) in enumerate(holders[:30], 1):
    pct = (bal / supply * Decimal(100)) if supply else Decimal(0)
    print(f"{rank:02d}", addr[:8] + "..." + addr[-6:], f"qty={bal:,.2f}", f"pct={pct:.2f}%", "known=" + known.get(addr, "unknown"))

for n in [1, 5, 10, 20, 50, 100]:
    part = sum(b for _, b in holders[:n])
    pct = (part / supply * Decimal(100)) if supply else Decimal(0)
    print(f"top{n}_pct={pct:.2f}% amount={part:,.2f}")

print("\nwrote", OUT)
print("cache", CACHE)
