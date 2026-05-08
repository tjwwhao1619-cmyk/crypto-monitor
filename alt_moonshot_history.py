import argparse
import csv
import datetime as dt
import json
import math
import statistics
import time
from pathlib import Path
from urllib.parse import urlencode

import requests


FAPI_BASE = "https://fapi.binance.com"
SPOT_BASE = "https://api.binance.com"
FUTURES_DATA_BASE = f"{FAPI_BASE}/futures/data"

MAINSTREAM_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "XRPUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "TONUSDT",
    "OPUSDT",
    "ARBUSDT",
}
PROTECTED_BASES = {"ST"}

CONTEXT_WINDOWS = {
    "15m": ("5m", 4),
    "30m": ("5m", 7),
    "1h": ("5m", 13),
    "4h": ("15m", 17),
    "24h": ("1h", 25),
}


def utc_now() -> dt.datetime:
    return dt.datetime.now(dt.UTC)


def ms(value: dt.datetime) -> int:
    return int(value.timestamp() * 1000)


def parse_ms(value) -> dt.datetime:
    return dt.datetime.fromtimestamp(int(value) / 1000, tz=dt.UTC)


def safe_float(value, default=None):
    try:
        if value is None:
            return default
        out = float(value)
        if math.isnan(out) or math.isinf(out):
            return default
        return out
    except Exception:
        return default


def pct(first, last):
    first = safe_float(first)
    last = safe_float(last)
    if first is None or last is None or first == 0:
        return None
    return (last - first) / first * 100


def avg(values):
    values = [value for value in values if value is not None]
    return sum(values) / len(values) if values else None


def median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


class BinanceCache:
    def __init__(self, cache_dir, sleep_seconds=0.08, no_network=False):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sleep_seconds = max(0.0, float(sleep_seconds))
        self.no_network = bool(no_network)
        self.session = requests.Session()
        self.last_request_at = 0.0
        self.hits = 0
        self.misses = 0
        self.requests = 0
        self.errors = 0

    def cache_path(self, base, path, params):
        clean_path = path.strip("/").replace("/", "_")
        query = urlencode(sorted((params or {}).items()))
        safe_query = "".join(ch if ch.isalnum() or ch in "._=-" else "_" for ch in query)
        if len(safe_query) > 180:
            safe_query = str(abs(hash(query)))
        base_name = "spot" if base == SPOT_BASE else "fapi"
        return self.cache_dir / f"{base_name}_{clean_path}_{safe_query}.json"

    def get(self, base, path, params=None):
        params = params or {}
        cache_path = self.cache_path(base, path, params)
        if cache_path.exists():
            self.hits += 1
            return json.loads(cache_path.read_text(encoding="utf-8"))
        self.misses += 1
        if self.no_network:
            raise RuntimeError(f"cache miss with --no-network: {path} {params}")
        elapsed = time.time() - self.last_request_at
        if elapsed < self.sleep_seconds:
            time.sleep(self.sleep_seconds - elapsed)
        self.last_request_at = time.time()
        response = self.session.get(f"{base}{path}", params=params, timeout=20)
        self.requests += 1
        if response.status_code == 429:
            retry_after = safe_float(response.headers.get("Retry-After"), 2.0)
            time.sleep(max(1.0, retry_after or 2.0))
            response = self.session.get(f"{base}{path}", params=params, timeout=20)
            self.requests += 1
        try:
            response.raise_for_status()
            payload = response.json()
        except Exception:
            self.errors += 1
            raise
        cache_path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")
        return payload


def futures_symbols(client):
    info = client.get(FAPI_BASE, "/fapi/v1/exchangeInfo")
    out = []
    for row in info.get("symbols", []):
        symbol = row.get("symbol", "")
        base = row.get("baseAsset", "")
        if row.get("contractType") != "PERPETUAL":
            continue
        if row.get("quoteAsset") != "USDT" or row.get("status") != "TRADING":
            continue
        if symbol in MAINSTREAM_SYMBOLS or base in PROTECTED_BASES:
            continue
        out.append(symbol)
    return sorted(set(out))


def fetch_klines_1h(client, symbol, start, end):
    rows = client.get(
        FAPI_BASE,
        "/fapi/v1/klines",
        {
            "symbol": symbol,
            "interval": "1h",
            "startTime": ms(start),
            "endTime": ms(end),
            "limit": 1500,
        },
    )
    return rows if isinstance(rows, list) else []


def find_moonshot_candidates(symbol, klines):
    rows = []
    n = len(klines)
    last_selected = -10_000
    for i in range(24, max(24, n - 1)):
        close = safe_float(klines[i][4])
        if not close or close <= 0:
            continue
        high_24 = max((safe_float(row[2], 0.0) or 0.0) for row in klines[i + 1 : min(n, i + 25)] or [klines[i]])
        high_48 = max((safe_float(row[2], 0.0) or 0.0) for row in klines[i + 1 : min(n, i + 49)] or [klines[i]])
        high_7d = max((safe_float(row[2], 0.0) or 0.0) for row in klines[i + 1 : min(n, i + 169)] or [klines[i]])
        mfe_24 = pct(close, high_24) or 0.0
        mfe_48 = pct(close, high_48) or 0.0
        mfe_7d = pct(close, high_7d) or 0.0
        qualifies = mfe_24 >= 30 or mfe_48 >= 50 or mfe_7d >= 100
        if not qualifies:
            continue
        if i - last_selected < 24:
            continue
        previous_24h_close = safe_float(klines[i - 24][4])
        price_change_24h = pct(previous_24h_close, close)
        prev_24h_low = min((safe_float(row[3], close) or close) for row in klines[i - 24 : i + 1])
        from_24h_low = pct(prev_24h_low, close)
        quote_now = safe_float(klines[i][7], 0.0) or 0.0
        prev_quote = [safe_float(row[7]) for row in klines[max(0, i - 24) : i]]
        prev_quote_avg = avg(prev_quote) or 0.0
        volume_ratio_24h = quote_now / prev_quote_avg if prev_quote_avg > 0 else None
        rows.append(
            {
                "symbol": symbol,
                "event_time": parse_ms(klines[i][0]).isoformat(),
                "entry_price": close,
                "price_change_24h": price_change_24h,
                "from_24h_low": from_24h_low,
                "quote_volume_1h": quote_now,
                "volume_ratio_24h": volume_ratio_24h,
                "future_mfe_24h": mfe_24,
                "future_mfe_48h": mfe_48,
                "future_mfe_7d": mfe_7d,
            }
        )
        last_selected = i
    return rows


def rows_until(client, endpoint, symbol, period, end_time, limit):
    try:
        rows = client.get(
            FUTURES_DATA_BASE,
            f"/{endpoint}",
            {
                "symbol": symbol,
                "period": period,
                "endTime": ms(end_time),
                "limit": limit,
            },
        )
    except Exception:
        return []
    return rows if isinstance(rows, list) else []


def latest_avg_ratio(client, endpoint, key, symbol, end_time, period, limit):
    rows = rows_until(client, endpoint, symbol, period, end_time, limit)
    return avg([safe_float(row.get(key)) for row in rows])


def enrich_contract_context(client, item):
    symbol = item["symbol"]
    end_time = dt.datetime.fromisoformat(item["event_time"])
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=dt.UTC)
    for label, (period, limit) in CONTEXT_WINDOWS.items():
        oi_rows = rows_until(client, "openInterestHist", symbol, period, end_time, limit)
        if len(oi_rows) >= 2:
            item[f"oi_change_{label}"] = pct(oi_rows[0].get("sumOpenInterestValue"), oi_rows[-1].get("sumOpenInterestValue"))
        else:
            item[f"oi_change_{label}"] = None
        item[f"global_ls_{label}"] = latest_avg_ratio(client, "globalLongShortAccountRatio", "longShortRatio", symbol, end_time, period, limit)
        item[f"top_position_ls_{label}"] = latest_avg_ratio(client, "topLongShortPositionRatio", "longShortRatio", symbol, end_time, period, limit)
        item[f"top_account_ls_{label}"] = latest_avg_ratio(client, "topLongShortAccountRatio", "longShortRatio", symbol, end_time, period, limit)
        taker_rows = rows_until(client, "takerlongshortRatio", symbol, period, end_time, limit)
        buy = sum(safe_float(row.get("buyVol"), 0.0) or 0.0 for row in taker_rows)
        sell = sum(safe_float(row.get("sellVol"), 0.0) or 0.0 for row in taker_rows)
        item[f"taker_buy_sell_{label}"] = buy / sell if sell > 0 else None
        price = safe_float(item.get("entry_price"), 0.0) or 0.0
        item[f"net_taker_flow_usd_{label}"] = (buy - sell) * price if buy or sell else None
    try:
        funding_rows = client.get(
            FAPI_BASE,
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "endTime": ms(end_time), "limit": 8},
        )
    except Exception:
        funding_rows = []
    funding_values = [safe_float(row.get("fundingRate")) * 100 for row in funding_rows if safe_float(row.get("fundingRate")) is not None]
    item["funding_latest"] = funding_values[-1] if funding_values else None
    item["funding_avg_24h"] = avg(funding_values[-3:]) if funding_values else None
    return item


def write_csv(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def fmt(value, digits=2):
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def build_report(rows, args, client):
    lines = []
    lines.append("[ALT MOONSHOT HISTORY]")
    lines.append(f"range_days={args.days} symbols_scanned={args.symbols_scanned} moonshot_events={len(rows)}")
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append("definition: 24h MFE>=30% OR 48h MFE>=50% OR 7d MFE>=100%; 1h futures klines.")
    protected = ", ".join(sorted(PROTECTED_BASES)) if PROTECTED_BASES else "none"
    lines.append(f"protected: skipped {protected} and mainstream symbols.")
    lines.append("")
    if rows:
        lines.append("总体特征中位数:")
        for key in (
            "price_change_24h",
            "from_24h_low",
            "volume_ratio_24h",
            "oi_change_15m",
            "oi_change_30m",
            "oi_change_1h",
            "oi_change_4h",
            "oi_change_24h",
            "taker_buy_sell_15m",
            "taker_buy_sell_1h",
            "funding_latest",
            "future_mfe_24h",
            "future_mfe_48h",
            "future_mfe_7d",
        ):
            values = [safe_float(row.get(key)) for row in rows]
            present = len([value for value in values if value is not None])
            lines.append(f"- {key}: median={fmt(median(values))} avg={fmt(avg(values))} present={present}/{len(rows)}")
        lines.append("")
        lines.append("未来涨幅最大的样本:")
        top = sorted(rows, key=lambda row: safe_float(row.get("future_mfe_7d"), 0.0) or 0.0, reverse=True)[:25]
        for row in top:
            lines.append(
                f"- {row['symbol']} {row['event_time']} "
                f"24h涨幅={fmt(row.get('price_change_24h'))}% "
                f"OI15m={fmt(row.get('oi_change_15m'))}% OI1h={fmt(row.get('oi_change_1h'))}% "
                f"taker15m={fmt(row.get('taker_buy_sell_15m'), 3)} funding={fmt(row.get('funding_latest'), 4)}% "
                f"MFE24={fmt(row.get('future_mfe_24h'))}% MFE48={fmt(row.get('future_mfe_48h'))}% MFE7d={fmt(row.get('future_mfe_7d'))}%"
            )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Build an offline moonshot startup sample set from Binance futures history.")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--sleep", type=float, default=0.08)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--max-events", type=int, default=120)
    parser.add_argument("--no-network", action="store_true")
    args = parser.parse_args()

    client = BinanceCache(args.cache_dir, sleep_seconds=args.sleep, no_network=args.no_network)
    end = utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=args.days)
    symbols = futures_symbols(client)
    if args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    args.symbols_scanned = len(symbols)

    candidates = []
    for idx, symbol in enumerate(symbols, start=1):
        try:
            klines = fetch_klines_1h(client, symbol, start, end)
        except Exception:
            continue
        candidates.extend(find_moonshot_candidates(symbol, klines))
        if idx % 25 == 0:
            print(f"scanned={idx}/{len(symbols)} candidates={len(candidates)} requests={client.requests}", flush=True)

    candidates = sorted(candidates, key=lambda row: safe_float(row.get("future_mfe_7d"), 0.0) or 0.0, reverse=True)
    if args.max_events > 0:
        candidates = candidates[: args.max_events]

    enriched = []
    for idx, item in enumerate(candidates, start=1):
        enriched.append(enrich_contract_context(client, dict(item)))
        if idx % 10 == 0:
            print(f"enriched={idx}/{len(candidates)} requests={client.requests}", flush=True)

    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"alt_moonshots_{args.days}d.csv"
    txt_path = out_dir / f"alt_moonshots_{args.days}d.txt"
    latest_path = out_dir / "latest_alt_moonshots.txt"
    write_csv(csv_path, enriched)
    report = build_report(enriched, args, client)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
