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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import requests


API_KEY_ENV = "COINGLASS_API_KEY"
TIMEOUT_SECONDS = 15
BASE_URL = "https://open-api-v4.coinglass.com"

AGGREGATED_HEATMAP_ENDPOINT = (
    "/api/futures/liquidation/aggregated-heatmap/model1"
)
PAIR_HEATMAP_ENDPOINT = "/api/futures/liquidation/heatmap/model1"

COINS = ["BTC", "ETH", "KNC", "LAB", "TAC"]
PAIRS = ["BTCUSDT", "ETHUSDT", "KNCUSDT", "LABUSDT", "TACUSDT"]
RANGES = ["12h", "24h"]
EXCHANGE = "Binance"
INTERVAL = "1h"
LIMIT = "5"


@dataclass(frozen=True)
class Probe:
    name: str
    endpoint: str
    params: dict[str, str]


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

    print(f"code: {code}")
    print(f"msg: {msg}")
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


def build_probes() -> list[Probe]:
    probes: list[Probe] = []

    for range_value in RANGES:
        for coin in COINS:
            probes.append(
                Probe(
                    name=(
                        "aggregated liquidation heatmap model1 "
                        f"{coin} {range_value}"
                    ),
                    endpoint=AGGREGATED_HEATMAP_ENDPOINT,
                    params={"symbol": coin, "range": range_value},
                )
            )

    for range_value in RANGES:
        for pair in PAIRS:
            probes.append(
                Probe(
                    name=(
                        "pair liquidation heatmap model1 "
                        f"{EXCHANGE} {pair} {range_value}"
                    ),
                    endpoint=PAIR_HEATMAP_ENDPOINT,
                    params={
                        "exchange": EXCHANGE,
                        "symbol": pair,
                        "range": range_value,
                    },
                )
            )

    probes.extend(
        [
            Probe(
                name="open interest OHLC history",
                endpoint="/api/futures/openInterest/ohlc-history",
                params={
                    "exchange": EXCHANGE,
                    "symbol": "BTCUSDT",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="aggregated open interest history",
                endpoint="/api/futures/openInterest/aggregated-history",
                params={
                    "symbol": "BTC",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="open interest exchange list",
                endpoint="/api/futures/openInterest/exchange-list",
                params={"symbol": "BTC"},
            ),
            Probe(
                name="funding rate OHLC history",
                endpoint="/api/futures/fundingRate/ohlc-history",
                params={
                    "exchange": EXCHANGE,
                    "symbol": "BTCUSDT",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="OI-weighted funding rate OHLC history",
                endpoint="/api/futures/fundingRate/oi-weight-ohlc-history",
                params={
                    "symbol": "BTC",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="funding rate exchange list",
                endpoint="/api/futures/fundingRate/exchange-list",
                params={"symbol": "BTC"},
            ),
            Probe(
                name="pair liquidation history",
                endpoint="/api/futures/liquidation/history",
                params={
                    "exchange": EXCHANGE,
                    "symbol": "BTCUSDT",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="aggregated liquidation history",
                endpoint="/api/futures/liquidation/aggregated-history",
                params={
                    "exchange_list": EXCHANGE,
                    "symbol": "BTC",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="global long-short account ratio history",
                endpoint="/api/futures/global-long-short-account-ratio/history",
                params={
                    "exchange": EXCHANGE,
                    "symbol": "BTCUSDT",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
            Probe(
                name="bitcoin ETF flow history",
                endpoint="/api/bitcoin/etf/flow-history",
                params={
                    "symbol": "BTC",
                    "interval": INTERVAL,
                    "limit": LIMIT,
                },
            ),
        ]
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
