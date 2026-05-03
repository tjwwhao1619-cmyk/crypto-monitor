#!/usr/bin/env python3
"""Standalone CoinGlass API probe.

This script is intentionally not wired into derivatives_monitor.py. It probes a
small matrix of CoinGlass v4 endpoints and prints compact response diagnostics
without exposing the API key.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import requests


API_KEY_ENV = "COINGLASS_API_KEY"
TIMEOUT_SECONDS = 15
BASE_URL = "https://open-api-v4.coinglass.com"

EXCHANGE = "Binance"
COIN = "BTC"
PAIR = "BTCUSDT"
INTERVAL = "1h"
LIMIT = "5"
RANGE = "24h"
LONG_RANGE_VALUES = ("24h", "3d", "7d", "30d")
LONG_RANGE_ENDPOINTS = (
    (
        "range probe taker buy/sell exchange list",
        "/api/futures/taker-buy-sell-volume/exchange-list",
        {"symbol": COIN},
    ),
    (
        "range probe exchange balance list",
        "/api/exchange/balance/list",
        {"symbol": COIN},
    ),
    (
        "range probe exchange balance chart",
        "/api/exchange/balance/chart",
        {"symbol": COIN},
    ),
    (
        "range probe open interest exchange list",
        "/api/futures/open-interest/exchange-list",
        {"symbol": COIN},
    ),
    (
        "range probe accumulated funding exchange list",
        "/api/futures/funding-rate/accumulated-exchange-list",
        {"symbol": COIN},
    ),
)


@dataclass(frozen=True)
class Probe:
    name: str
    endpoint: str
    params: dict[str, Any]


@dataclass(frozen=True)
class ProbeResult:
    name: str
    endpoint: str
    category: str
    detail: str


def type_name(value: Any) -> str:
    return type(value).__name__


def print_jsonish(label: str, value: Any) -> None:
    try:
        rendered = json.dumps(value, ensure_ascii=True, default=str)
    except TypeError:
        rendered = repr(value)
    print(f"{label}: {rendered}")


def render_sample(value: Any, limit: int = 800) -> str:
    try:
        rendered = json.dumps(value, ensure_ascii=True, default=str)
    except TypeError:
        rendered = repr(value)
    if len(rendered) > limit:
        return f"{rendered[:limit]}...<truncated>"
    return rendered


def classify_response(
    status_code: int,
    code: Any,
    msg: Any,
) -> tuple[str, str]:
    code_text = "" if code is None else str(code)
    msg_text = "" if msg is None else str(msg)
    msg_lower = msg_text.lower()

    if "upgrade plan" in msg_lower:
        return "upgrade required", f"code={code_text} msg={msg_text}"

    if status_code == 200 and code_text == "0":
        return "usable", f"code={code_text} msg={msg_text}"

    if code_text == "401" or "upgrade" in msg_lower:
        return "upgrade required", f"code={code_text} msg={msg_text}"

    return "failed", f"HTTP {status_code} code={code_text} msg={msg_text}"


def probe_request(
    session: requests.Session,
    probe: Probe,
) -> ProbeResult:
    url = f"{BASE_URL}{probe.endpoint}"
    print("=" * 80)
    print(f"request name: {probe.name}")
    print(f"endpoint: {probe.endpoint}")
    print_jsonish("params", probe.params)

    try:
        response = session.get(url, params=probe.params, timeout=TIMEOUT_SECONDS)
    except requests.RequestException as exc:
        print(f"request error: {exc}")
        return ProbeResult(probe.name, probe.endpoint, "failed", str(exc))

    print(f"HTTP status: {response.status_code}")

    try:
        payload = response.json()
    except ValueError:
        print(f"non-json response preview: {response.text[:500]}")
        return ProbeResult(
            probe.name,
            probe.endpoint,
            "failed",
            f"HTTP {response.status_code} non-json response",
        )

    if isinstance(payload, Mapping):
        print_jsonish("top-level fields", list(payload.keys()))
        code = payload.get("code")
        msg = payload.get("msg")
        data = payload.get("data")
    else:
        print(f"top-level type: {type_name(payload)}")
        code = None
        msg = None
        data = None

    print(f"code/msg: {code}/{msg}")
    print(f"data type: {type_name(data)}")
    print(f"data sample: {render_sample(data)}")

    category, detail = classify_response(response.status_code, code, msg)
    return ProbeResult(probe.name, probe.endpoint, category, detail)


def build_session(api_key: str) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "CG-API-KEY": api_key,
            "accept": "application/json",
        }
    )
    return session


def last_24h_window_ms() -> dict[str, int]:
    end_time = int(time.time() * 1000)
    start_time = end_time - 24 * 60 * 60 * 1000
    return {"start_time": start_time, "end_time": end_time}


def pair_history_params(**extra: Any) -> dict[str, Any]:
    params: dict[str, Any] = {
        "exchange": EXCHANGE,
        "symbol": PAIR,
        "interval": INTERVAL,
        "limit": LIMIT,
    }
    params.update(extra)
    return params


def coin_history_params(**extra: Any) -> dict[str, Any]:
    params: dict[str, Any] = {
        "symbol": COIN,
        "interval": INTERVAL,
        "limit": LIMIT,
    }
    params.update(extra)
    return params


def build_probes() -> list[Probe]:
    time_window = last_24h_window_ms()

    probes = [
        # Existing Startup-usable probes to keep tracking.
        Probe(
            name="liquidation history",
            endpoint="/api/futures/liquidation/history",
            params=pair_history_params(),
        ),
        Probe(
            name="aggregated liquidation history",
            endpoint="/api/futures/liquidation/aggregated-history",
            params=coin_history_params(exchange_list=EXCHANGE),
        ),
        Probe(
            name="global long-short account ratio history",
            endpoint="/api/futures/global-long-short-account-ratio/history",
            params=pair_history_params(),
        ),

        # Open interest candidates.
        Probe(
            name="open interest OHLC history",
            endpoint="/api/futures/open-interest/history",
            params=pair_history_params(unit="usd"),
        ),
        Probe(
            name="aggregated open interest history",
            endpoint="/api/futures/open-interest/aggregated-history",
            params=coin_history_params(unit="usd"),
        ),
        Probe(
            name="open interest exchange list",
            endpoint="/api/futures/open-interest/exchange-list",
            params={"symbol": COIN},
        ),
        Probe(
            name="aggregated stablecoin margin open interest history",
            endpoint="/api/futures/open-interest/aggregated-stablecoin-history",
            params=coin_history_params(exchange_list=EXCHANGE),
        ),
        Probe(
            name="aggregated coin margin open interest history",
            endpoint="/api/futures/open-interest/aggregated-coin-margin-history",
            params=coin_history_params(exchange_list=EXCHANGE),
        ),

        # Funding rate candidates.
        Probe(
            name="funding rate OHLC history",
            endpoint="/api/futures/funding-rate/history",
            params=pair_history_params(),
        ),
        Probe(
            name="OI-weighted funding rate history",
            endpoint="/api/futures/funding-rate/oi-weight-history",
            params=coin_history_params(),
        ),
        Probe(
            name="volume-weighted funding rate history",
            endpoint="/api/futures/funding-rate/vol-weight-history",
            params=coin_history_params(),
        ),
        Probe(
            name="funding rate exchange list",
            endpoint="/api/futures/funding-rate/exchange-list",
            params={"symbol": COIN},
        ),
        Probe(
            name="cumulative funding rate exchange list",
            endpoint="/api/futures/funding-rate/accumulated-exchange-list",
            params={"range": "1d"},
        ),

        # Taker buy/sell volume candidates.
        Probe(
            name="futures taker buy/sell volume history",
            endpoint="/api/futures/v2/taker-buy-sell-volume/history",
            params=pair_history_params(),
        ),
        Probe(
            name="futures taker buy/sell volume exchange list",
            endpoint="/api/futures/taker-buy-sell-volume/exchange-list",
            params={"symbol": COIN, "range": RANGE},
        ),
        Probe(
            name="spot aggregated taker buy/sell volume history",
            endpoint="/api/spot/aggregated-taker-buy-sell-volume/history",
            params=coin_history_params(exchange_list=EXCHANGE, unit="usd"),
        ),

        # Market snapshot / ticker-style candidates.
        Probe(
            name="futures pairs markets",
            endpoint="/api/futures/pairs-markets",
            params={"symbol": COIN},
        ),
        Probe(
            name="spot pairs markets",
            endpoint="/api/spot/pairs-markets",
            params={"symbol": COIN},
        ),
        Probe(
            name="spot coins markets",
            endpoint="/api/spot/coins-markets",
            params={"page": "1", "per_page": LIMIT},
        ),

        # ETF flow candidates.
        Probe(
            name="BTC ETF list",
            endpoint="/api/bitcoin/etf/list",
            params={},
        ),
        Probe(
            name="BTC ETF flow history",
            endpoint="/api/bitcoin/etf/flow-history",
            params={"interval": "1d", "limit": LIMIT},
        ),

        # Orderbook / depth candidates.
        Probe(
            name="spot orderbook heatmap history",
            endpoint="/api/spot/orderbook/history",
            params=pair_history_params(**time_window),
        ),
        Probe(
            name="spot orderbook bid ask range history",
            endpoint="/api/spot/orderbook/ask-bids-history",
            params=pair_history_params(range="1", **time_window),
        ),
        Probe(
            name="spot large limit orders",
            endpoint="/api/spot/orderbook/large-limit-order",
            params={"exchange": EXCHANGE, "symbol": PAIR},
        ),
        Probe(
            name="spot large limit order history",
            endpoint="/api/spot/orderbook/large-limit-order-history",
            params={
                "exchange": EXCHANGE,
                "symbol": PAIR,
                "state": "2",
                **time_window,
            },
        ),

        # Exchange balance / on-chain candidates.
        Probe(
            name="exchange balance list",
            endpoint="/api/exchange/balance/list",
            params={"symbol": COIN},
        ),
        Probe(
            name="exchange balance chart",
            endpoint="/api/exchange/balance/chart",
            params={"symbol": COIN},
        ),
        Probe(
            name="exchange assets",
            endpoint="/api/exchange/assets",
            params={"exchange": EXCHANGE, "page": "1", "per_page": LIMIT},
        ),
        Probe(
            name="exchange on-chain transfers ERC20",
            endpoint="/api/exchange/chain/tx/list",
            params={
                "symbol": "ETH",
                "page": "1",
                "per_page": LIMIT,
                "min_usd": "1000000",
                "start_time": time_window["start_time"],
            },
        ),
    ]
    probes.extend(build_long_range_probes())
    return probes


def build_long_range_probes() -> list[Probe]:
    probes: list[Probe] = []
    for name, endpoint, base_params in LONG_RANGE_ENDPOINTS:
        for range_value in LONG_RANGE_VALUES:
            params = dict(base_params)
            params["range"] = range_value
            probes.append(
                Probe(
                    name=f"{name} range={range_value}",
                    endpoint=endpoint,
                    params=params,
                )
            )
    return probes


def print_summary(results: list[ProbeResult]) -> None:
    buckets = {
        "usable": "usable endpoints",
        "upgrade required": "upgrade required endpoints",
        "failed": "failed endpoints",
    }

    print("=" * 80)
    print("summary:")
    for category, label in buckets.items():
        print(f"{label}:")
        category_results = [result for result in results if result.category == category]
        if not category_results:
            print("  none")
            continue
        for result in category_results:
            print(f"  - {result.name} | {result.endpoint} | {result.detail}")


def main() -> int:
    api_key = os.getenv(API_KEY_ENV)
    if not api_key:
        print(f"Missing {API_KEY_ENV}. Export it before running this probe.")
        return 1

    session = build_session(api_key)
    results: list[ProbeResult] = []

    for probe in build_probes():
        results.append(probe_request(session=session, probe=probe))

    print_summary(results)
    return 0


if __name__ == "__main__":
    sys.exit(main())
