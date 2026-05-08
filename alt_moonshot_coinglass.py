import json
import os
import time
import datetime as dt
from pathlib import Path
from urllib.parse import urlencode

import requests

import alt_moonshot_history as hist


BASE_URL = "https://open-api-v4.coinglass.com"
EXCHANGE = "Binance"


class CoinGlassClient:
    def __init__(self, cache_dir=".cache/coinglass", sleep_seconds=0.05, no_network=False):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.sleep_seconds = max(0.0, float(sleep_seconds))
        self.no_network = bool(no_network)
        self.api_key = os.getenv("COINGLASS_API_KEY", "").strip()
        self.session = requests.Session()
        self.session.headers.update({"accept": "application/json"})
        if self.api_key:
            self.session.headers.update({"CG-API-KEY": self.api_key})
        self.last_request_at = 0.0
        self.hits = 0
        self.misses = 0
        self.requests = 0
        self.errors = 0
        self.disabled_reason = "" if self.api_key else "COINGLASS_API_KEY not configured"

    def cache_path(self, endpoint, params):
        clean_endpoint = endpoint.strip("/").replace("/", "_")
        query = urlencode(sorted((params or {}).items()))
        safe_query = "".join(ch if ch.isalnum() or ch in "._=-" else "_" for ch in query)
        if len(safe_query) > 180:
            safe_query = str(abs(hash(query)))
        return self.cache_dir / f"{clean_endpoint}_{safe_query}.json"

    def get(self, endpoint, params=None):
        if not self.api_key:
            return None
        params = params or {}
        path = self.cache_path(endpoint, params)
        if path.exists():
            self.hits += 1
            return json.loads(path.read_text(encoding="utf-8"))
        self.misses += 1
        if self.no_network:
            raise RuntimeError(f"CoinGlass cache miss with --no-network: {endpoint} {params}")
        elapsed = time.time() - self.last_request_at
        if elapsed < self.sleep_seconds:
            time.sleep(self.sleep_seconds - elapsed)
        self.last_request_at = time.time()
        response = self.session.get(f"{BASE_URL}{endpoint}", params=params, timeout=15)
        self.requests += 1
        try:
            payload = response.json()
        except ValueError:
            self.errors += 1
            return None
        code = str(payload.get("code", "")) if isinstance(payload, dict) else ""
        if response.status_code != 200 or code not in {"0", ""}:
            self.errors += 1
            return payload
        path.write_text(json.dumps(payload, separators=(",", ":"), ensure_ascii=False), encoding="utf-8")
        return payload


def coin_from_symbol(symbol):
    symbol = str(symbol or "").upper()
    if symbol.endswith("USDT"):
        return symbol[:-4]
    if symbol.endswith("USDC"):
        return symbol[:-4]
    return symbol


def data_rows(payload):
    if not isinstance(payload, dict):
        return []
    data = payload.get("data")
    return data if isinstance(data, list) else []


def fetch_history(client, endpoint, symbol, event_time, hours, interval="1h", **extra):
    if not client or not client.api_key:
        return []
    start_time = event_time - dt.timedelta(hours=hours)
    params = {
        "exchange": EXCHANGE,
        "symbol": symbol,
        "interval": interval,
        "limit": str(max(2, int(hours) + 2)),
        "start_time": hist.ms(start_time),
        "end_time": hist.ms(event_time),
    }
    params.update(extra)
    return data_rows(client.get(endpoint, params))


def fetch_coin_history(client, endpoint, symbol, event_time, hours, interval="1h", **extra):
    if not client or not client.api_key:
        return []
    start_time = event_time - dt.timedelta(hours=hours)
    params = {
        "symbol": coin_from_symbol(symbol),
        "interval": interval,
        "limit": str(max(2, int(hours) + 2)),
        "start_time": hist.ms(start_time),
        "end_time": hist.ms(event_time),
    }
    params.update(extra)
    return data_rows(client.get(endpoint, params))


def sum_field(rows, field):
    return sum(hist.safe_float(row.get(field), 0.0) or 0.0 for row in rows)


def close_change(rows):
    if len(rows) < 2:
        return None
    first = hist.safe_float(rows[0].get("open"))
    last = hist.safe_float(rows[-1].get("close"))
    return hist.pct(first, last)


def latest_close(rows):
    if not rows:
        return None
    return hist.safe_float(rows[-1].get("close"))


def avg_field(rows, field):
    return hist.avg([hist.safe_float(row.get(field)) for row in rows])


def context(client, symbol, event_time, hours=4):
    out = {}
    liq_1h = fetch_history(client, "/api/futures/liquidation/history", symbol, event_time, 1)
    liq_h = fetch_history(client, "/api/futures/liquidation/history", symbol, event_time, hours)
    oi_1h = fetch_history(client, "/api/futures/open-interest/history", symbol, event_time, 1, unit="usd")
    oi_h = fetch_history(client, "/api/futures/open-interest/history", symbol, event_time, hours, unit="usd")
    taker_1h = fetch_history(client, "/api/futures/v2/taker-buy-sell-volume/history", symbol, event_time, 1)
    taker_h = fetch_history(client, "/api/futures/v2/taker-buy-sell-volume/history", symbol, event_time, hours)
    ls_rows = fetch_history(client, "/api/futures/global-long-short-account-ratio/history", symbol, event_time, hours)
    funding_rows = fetch_history(client, "/api/futures/funding-rate/history", symbol, event_time, hours)

    for label, rows in (("1h", liq_1h), (f"{hours}h", liq_h)):
        long_liq = sum_field(rows, "long_liquidation_usd")
        short_liq = sum_field(rows, "short_liquidation_usd")
        out[f"cg_long_liq_{label}_usd"] = long_liq
        out[f"cg_short_liq_{label}_usd"] = short_liq
        out[f"cg_liq_total_{label}_usd"] = long_liq + short_liq
        out[f"cg_long_short_liq_ratio_{label}"] = long_liq / short_liq if short_liq > 0 else None
        out[f"cg_short_long_liq_ratio_{label}"] = short_liq / long_liq if long_liq > 0 else None

    out["cg_oi_change_1h"] = close_change(oi_1h)
    out[f"cg_oi_change_{hours}h"] = close_change(oi_h)

    for label, rows in (("1h", taker_1h), (f"{hours}h", taker_h)):
        buy = sum_field(rows, "taker_buy_volume_usd")
        sell = sum_field(rows, "taker_sell_volume_usd")
        out[f"cg_taker_buy_{label}_usd"] = buy
        out[f"cg_taker_sell_{label}_usd"] = sell
        out[f"cg_taker_buy_sell_{label}"] = buy / sell if sell > 0 else None

    out["cg_global_ls_ratio"] = avg_field(ls_rows, "global_account_long_short_ratio")
    out["cg_funding_latest"] = latest_close(funding_rows)
    return out
