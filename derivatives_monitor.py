import argparse
import base64
import concurrent.futures
import csv
import datetime as dt
import hashlib
import json
import logging
import os
import re
import secrets
import shutil
import socket
import ssl
import struct
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
import yaml


BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_FUTURES_DATA_BASE = "https://fapi.binance.com/futures/data"
BINANCE_FORCE_ORDER_WS_HOST = "fstream.binance.com"
BINANCE_FORCE_ORDER_WS_PATH = "/ws/!forceOrder@arr"
COINGLASS_LIQUIDATION_HISTORY_URL = "https://open-api-v4.coinglass.com/api/futures/liquidation/history"
COINGLASS_LIQUIDATION_AGGREGATED_HISTORY_URL = "https://open-api-v4.coinglass.com/api/futures/liquidation/aggregated-history"
COINGLASS_SPOT_ORDERBOOK_ASK_BIDS_HISTORY_ENDPOINT = "/api/spot/orderbook/ask-bids-history"
COINGLASS_LIQUIDATION_CACHE_TTL_SECONDS = 300
COINGLASS_BASE_URL = "https://open-api-v4.coinglass.com"
COINGLASS_MARKET_CONTEXT_CACHE_TTL_SECONDS = 300
TELEGRAM_SNAPSHOT_CACHE_TTL_SECONDS = 600
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
FLOW_PERIODS = ["5m", "15m", "1h", "4h", "12h", "24h"]
COINGLASS_TAKER_LONG_RANGES = ("24h", "7d")
COINGLASS_BALANCE_LONG_RANGES = ("24h", "7d", "30d")
COINGLASS_FUNDING_ACCUMULATED_RANGES = ("24h", "7d")
DEFAULT_TELEGRAM_REALTIME_PRIORITIES = ("S", "A", "B")
DEFAULT_TELEGRAM_DIGEST_PRIORITIES = ("C", "D")
DEFAULT_TELEGRAM_DIGEST_INTERVAL_MINUTES = 30
DEFAULT_TELEGRAM_DIGEST_MAX_PER_PRIORITY = 8


@dataclass(frozen=True)
class MarketSnapshot:
    symbol: str
    price_change_percent: float
    oi_change_percent: float
    global_long_short_ratio: float | None
    top_position_ratio: float | None
    top_account_ratio: float | None
    taker_buy_sell_ratio: float | None
    funding_rate_percent: float | None
    confirm_price_change_percent: float | None
    confirm_oi_change_percent: float | None
    net_flow_usd: dict[str, float]
    net_flow_ratio: dict[str, float]
    price_position_24h: float | None
    high_24h: float | None
    low_24h: float | None
    quote_volume_24h: float | None
    volume_ratio_24h: float | None
    close_price: float


@dataclass(frozen=True)
class Signal:
    symbol: str
    kind: str
    score: int
    title: str
    message: str
    key: str
    snapshot: MarketSnapshot | None = None


@dataclass(frozen=True)
class TelegramSignalDigestItem:
    created_at: float
    symbol: str
    kind: str
    priority: str
    quality_score: int
    trap_score: int | str
    main_asset_score: int | None
    signal_score: int
    strength_score: float
    reason: str


@dataclass(frozen=True)
class MainAssetScore:
    total_score: int
    label: str
    components: dict[str, int]
    note: str


@dataclass(frozen=True)
class LiquidationEvent:
    symbol: str
    side: str
    amount_usd: float
    event_time: float


SIGNAL_LOG_FIELDS = [
    "time",
    "symbol",
    "kind",
    "score",
    "title",
    "strength_score",
    "price",
    "price_change_percent",
    "oi_change_percent",
    "global_long_short_ratio",
    "top_position_ratio",
    "top_account_ratio",
    "taker_buy_sell_ratio",
    "funding_rate_percent",
    "net_flow_5m_usd",
    "net_flow_15m_usd",
    "net_flow_1h_usd",
    "net_flow_4h_usd",
    "net_flow_12h_usd",
    "net_flow_24h_usd",
    "net_flow_5m_ratio",
    "net_flow_15m_ratio",
    "net_flow_1h_ratio",
    "net_flow_4h_ratio",
    "net_flow_12h_ratio",
    "net_flow_24h_ratio",
    "price_position_24h",
    "high_24h",
    "low_24h",
    "quote_volume_24h",
    "volume_ratio_24h",
    "short_term_score",
    "mid_term_score",
    "flow_alignment_score",
    "long_flow_alignment_score",
    "structure_label",
    "trade_plan",
    "main_asset_score",
    "main_asset_score_label",
    "main_asset_trend_score",
    "main_asset_flow_score",
    "main_asset_derivatives_score",
    "main_asset_spot_orderbook_score",
    "main_asset_risk_penalty",
    "trap_risk_score",
    "trap_risk_label",
    "trap_risk_reason",
    "signal_priority",
    "signal_quality_score",
    "signal_quality_reason",
    "suppressed_from_telegram",
]


class DerivativesMonitor:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.session = requests.Session()
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)
        api_key = os.getenv(str(config.get("binance_api_key_env", "BINANCE_API_KEY")))
        if api_key:
            self.session.headers.update({"X-MBX-APIKEY": api_key})
        self.period = str(config.get("period", "5m"))
        self.limit = int(config.get("limit", 24))
        self.confirmation_config = config.get("confirmation", {})
        self.market_filter_config = config.get("market_filter", {})
        self.flow_config = config.get("flow", {})
        self.benchmark_snapshot: MarketSnapshot | None = None
        self.poll_interval = int(config.get("poll_interval_seconds", 300))
        self.scan_workers = int(config.get("scan_workers", 8))
        self.log_each_symbol = bool(config.get("log_each_symbol", False))
        self.symbol_configs = config.get("symbols", {})
        self.rules = config.get("rules", {})
        self.active_signals: set[str] = set()
        self.screener_config = config.get("screener", {})
        self.last_screened_at = 0.0
        self.summary_config = config.get("summary", {})
        self.last_summary_at = 0.0
        self.latest_snapshots: dict[str, MarketSnapshot] = {}
        self.latest_snapshots_updated_at = 0.0
        self.alert_cooldown_seconds = int(config.get("alert_cooldown_seconds", 3600))
        self.alert_cooldowns = config.get("alert_cooldowns", {})
        self.state_path = str(config.get("state_path", "monitor_state.json"))
        self.last_alerted_at: dict[str, float] = {}
        self.load_state()
        self.signal_log_path = str(config.get("signal_log_path", "signals.csv"))
        self.telegram_commands_config = config.get("telegram_commands", {})
        self.telegram_command_thread_started = False
        self.telegram_update_offset = self.load_telegram_update_offset()
        self.pending_dev_confirmations: dict[str, tuple[str, str, float]] = {}
        self.liquidation_events: deque[LiquidationEvent] = deque()
        self.liquidation_lock = threading.Lock()
        self.liquidation_thread_started = False
        self.liquidation_stop_event = threading.Event()
        self.liquidation_stream_connected = False
        self.liquidation_stream_started_at = 0.0
        self.liquidation_last_event_at = 0.0
        self.liquidation_last_error = ""
        self.coinglass_api_key = os.getenv("COINGLASS_API_KEY", "").strip()
        self.coinglass_liquidation_cache: dict[str, tuple[float, dict[str, float]]] = {}
        self.coinglass_market_context_cache: dict[str, tuple[float, str]] = {}
        self.telegram_signal_cooldowns: dict[str, tuple[float, int]] = {}
        self.telegram_signal_digest_queue: list[TelegramSignalDigestItem] = []
        self.telegram_signal_digest_lock = threading.Lock()
        self.last_telegram_signal_digest_at = time.time()
        self.runtime_realtime_priorities_override: set[str] | None = None
        self.runtime_realtime_priorities_lock = threading.Lock()
        self.signal_quality_stats = {
            "total": 0,
            "realtime_sent": 0,
            "suppressed": 0,
            "by_priority": {priority: 0 for priority in ("S", "A", "B", "C", "D")},
            "by_kind": {},
            "by_symbol": {},
            "suppressed_by_priority": {priority: 0 for priority in ("S", "A", "B", "C", "D")},
        }
        self.signal_quality_stats_lock = threading.Lock()

    def run_forever(self) -> None:
        self.start_liquidation_stream_worker()
        self.start_telegram_command_worker()
        self.send_pending_dev_restart_status()
        self.refresh_symbols_if_due(force=True)
        logging.info("Monitoring %s derivatives symbols", len(self.symbol_configs))
        while True:
            started = time.monotonic()
            try:
                self.refresh_symbols_if_due()
                self.run_cycle()
                self.send_summary_if_due()
                self.flush_telegram_signal_digest_if_due()
            except Exception:
                logging.exception("Derivatives monitor cycle failed")

            elapsed = time.monotonic() - started
            time.sleep(max(5, self.poll_interval - elapsed))

    def run_once(self, refresh_symbols: bool = True) -> list[tuple[MarketSnapshot, list[Signal]]]:
        if refresh_symbols:
            self.refresh_symbols_if_due(force=True)

        results = []
        for symbol, symbol_config in self.symbol_configs.items():
            snapshot = self.fetch_snapshot(symbol)
            signals = self.evaluate_snapshot(snapshot, symbol_config)
            results.append((snapshot, signals))
        return results

    def start_liquidation_stream_worker(self) -> None:
        if self.liquidation_thread_started:
            return
        self.liquidation_thread_started = True
        thread = threading.Thread(target=self.liquidation_stream_loop, name="binance-force-order-stream", daemon=True)
        thread.start()

    def liquidation_stream_loop(self) -> None:
        backoff_seconds = 5
        while not self.liquidation_stop_event.is_set():
            sock: ssl.SSLSocket | None = None
            try:
                sock = open_binance_force_order_socket()
                with self.liquidation_lock:
                    self.liquidation_stream_connected = True
                    self.liquidation_stream_started_at = time.time()
                logging.info("Subscribed Binance force order stream !forceOrder@arr")
                backoff_seconds = 5
                while not self.liquidation_stop_event.is_set():
                    frame = websocket_read_frame(sock)
                    if frame is None:
                        break
                    opcode, payload = frame
                    if opcode == 0x1:
                        self.record_liquidation_payload(payload.decode("utf-8"))
                    elif opcode == 0x8:
                        break
                    elif opcode == 0x9:
                        websocket_send_frame(sock, 0xA, payload)
            except Exception as exc:
                with self.liquidation_lock:
                    self.liquidation_stream_connected = False
                    self.liquidation_last_error = f"{type(exc).__name__}: {exc}"
                logging.warning("Binance force order stream disconnected", exc_info=True)
            finally:
                with self.liquidation_lock:
                    self.liquidation_stream_connected = False
                if sock is not None:
                    try:
                        sock.close()
                    except Exception:
                        pass

            self.liquidation_stop_event.wait(backoff_seconds)
            backoff_seconds = min(backoff_seconds * 2, 60)

    def record_liquidation_payload(self, payload_text: str) -> None:
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError as exc:
            with self.liquidation_lock:
                self.liquidation_last_error = f"{type(exc).__name__}: {exc}"
            logging.debug("Invalid force order payload: %s", payload_text)
            return

        items = force_order_items(payload)
        for item in items:
            event = liquidation_event_from_order(item)
            if event is not None:
                self.add_liquidation_event(event)

    def add_liquidation_event(self, event: LiquidationEvent) -> None:
        cutoff = time.time() - 3600
        with self.liquidation_lock:
            self.liquidation_events.append(event)
            self.liquidation_last_event_at = time.time()
            self.prune_liquidation_events_locked(cutoff)

    def prune_liquidation_events_locked(self, cutoff: float) -> None:
        while self.liquidation_events and self.liquidation_events[0].event_time < cutoff:
            self.liquidation_events.popleft()

    def liquidation_stats(self, symbol: str) -> dict[str, dict[str, float]]:
        symbol = symbol.upper()
        now = time.time()
        cutoff_1h = now - 3600
        cutoff_15m = now - 900
        stats = {
            "15m": {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0},
            "1h": {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0},
        }

        with self.liquidation_lock:
            self.prune_liquidation_events_locked(cutoff_1h)
            events = list(self.liquidation_events)

        for event in events:
            if event.symbol != symbol:
                continue
            if event.event_time >= cutoff_1h:
                update_liquidation_stats_bucket(stats["1h"], event)
            if event.event_time >= cutoff_15m:
                update_liquidation_stats_bucket(stats["15m"], event)

        return stats

    def format_liquidation_stats(self, symbol: str) -> str:
        stats = self.liquidation_stats(symbol)
        stats_15m = stats["15m"]
        stats_1h = stats["1h"]
        coinglass_stats = self.fetch_coinglass_liquidation_history(symbol)
        coinglass_text = format_coinglass_liquidation_stats(coinglass_stats) if coinglass_stats else None
        judgement_stats = stats_1h if stats_1h["count"] > 0 else coinglass_stats
        if stats_1h["count"] <= 0:
            if coinglass_text:
                return f"真实强平: Binance实时暂无缓存；{coinglass_text}；判断: {liquidation_judgement(judgement_stats)}"
            return "真实强平: 近1h暂无明显强平数据"
        if not coinglass_text:
            return (
                "真实强平: "
                f"15m 多单强平 {format_usd(stats_15m['long_liq_usd'])} / "
                f"空单强平 {format_usd(stats_15m['short_liq_usd'])}；"
                f"1h 多单强平 {format_usd(stats_1h['long_liq_usd'])} / "
                f"空单强平 {format_usd(stats_1h['short_liq_usd'])}；"
                f"样本 {int(stats_1h['count'])}；"
                f"判断: {liquidation_judgement(judgement_stats)}"
            )
        return (
            "真实强平: "
            f"Binance实时 15m 多单强平 {format_usd(stats_15m['long_liq_usd'])} / "
            f"空单强平 {format_usd(stats_15m['short_liq_usd'])}；"
            f"Binance实时 1h 多单强平 {format_usd(stats_1h['long_liq_usd'])} / "
            f"空单强平 {format_usd(stats_1h['short_liq_usd'])}；"
            f"样本 {int(stats_1h['count'])}"
            f"；{coinglass_text}；"
            f"判断: {liquidation_judgement(judgement_stats)}"
        )

    def fetch_coinglass_liquidation_history(self, symbol: str) -> dict[str, float] | None:
        if not self.coinglass_api_key:
            return None

        symbol = symbol.upper()
        now = time.time()
        cached = self.coinglass_liquidation_cache.get(symbol)
        if cached and now - cached[0] < COINGLASS_LIQUIDATION_CACHE_TTL_SECONDS:
            return cached[1]

        headers = {"CG-API-KEY": self.coinglass_api_key, "accept": "application/json"}
        try:
            stats = None
            try:
                stats = self.fetch_coinglass_liquidation_history_pair(symbol, headers)
            except Exception:
                logging.debug("CoinGlass pair liquidation history fetch failed: %s", symbol, exc_info=True)
            if stats is None:
                try:
                    stats = self.fetch_coinglass_liquidation_history_aggregated(symbol, headers)
                except Exception:
                    logging.debug("CoinGlass aggregated liquidation history fetch failed: %s", symbol, exc_info=True)
            if stats is None:
                return None
            self.coinglass_liquidation_cache[symbol] = (now, stats)
            return stats
        except Exception:
            logging.debug("CoinGlass liquidation history fetch failed: %s", symbol, exc_info=True)
            return None

    def fetch_coinglass_liquidation_history_pair(self, symbol: str, headers: dict[str, str]) -> dict[str, float] | None:
        params = {"exchange": "Binance", "symbol": symbol, "interval": "1h", "limit": 5}
        response = self.session.get(COINGLASS_LIQUIDATION_HISTORY_URL, headers=headers, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        if not coinglass_response_ok(data):
            return None
        stats = coinglass_liquidation_stats_from_response(data)
        return stats

    def fetch_coinglass_liquidation_history_aggregated(self, symbol: str, headers: dict[str, str]) -> dict[str, float] | None:
        base_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        params = {"exchange_list": "Binance", "symbol": base_symbol, "interval": "1h", "limit": 5}
        response = self.session.get(
            COINGLASS_LIQUIDATION_AGGREGATED_HISTORY_URL,
            headers=headers,
            params=params,
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
        if not coinglass_response_ok(data):
            return None
        stats = coinglass_liquidation_stats_from_response(data)
        return stats

    def fetch_coinglass_market_context(self, symbol: str) -> dict[str, Any] | None:
        if not self.coinglass_api_key:
            return None

        symbol = symbol.upper()
        base_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        headers = {"CG-API-KEY": self.coinglass_api_key, "accept": "application/json"}
        context: dict[str, Any] = {"symbol": symbol, "base": base_symbol}

        oi_data = self.fetch_coinglass_json(
            "/api/futures/open-interest/exchange-list",
            {"symbol": base_symbol},
            headers,
        )
        oi_row = coinglass_find_exchange_row(oi_data, "All")
        if oi_row:
            context["open_interest"] = {
                "open_interest_usd": parse_float(oi_row.get("open_interest_usd")),
                "change_5m": parse_float(oi_row.get("open_interest_change_percent_5m")),
                "change_15m": parse_float(oi_row.get("open_interest_change_percent_15m")),
                "change_1h": parse_float(oi_row.get("open_interest_change_percent_1h")),
                "change_4h": parse_float(oi_row.get("open_interest_change_percent_4h")),
                "change_24h": parse_float(oi_row.get("open_interest_change_percent_24h")),
            }

        funding_history_data = self.fetch_coinglass_json(
            "/api/futures/funding-rate/oi-weight-history",
            {"symbol": base_symbol, "interval": "1h", "limit": 5},
            headers,
        )
        funding_rows = coinglass_rows(funding_history_data)
        if funding_rows:
            context["funding_oi_weight"] = parse_float(funding_rows[-1].get("close"))

        funding_distribution_data = self.fetch_coinglass_json(
            "/api/futures/funding-rate/exchange-list",
            {"symbol": base_symbol},
            headers,
        )
        funding_rows = coinglass_stablecoin_margin_rows_for_base(funding_distribution_data, base_symbol)
        if funding_rows:
            distribution = coinglass_funding_distribution(funding_rows)
            if distribution:
                context["funding_distribution"] = distribution
        else:
            logging.debug("CoinGlass funding exchange-list has no matching stablecoin rows for %s", base_symbol)

        taker_data = self.fetch_coinglass_json(
            "/api/futures/taker-buy-sell-volume/exchange-list",
            {"symbol": base_symbol, "range": "24h"},
            headers,
        )
        taker_row = coinglass_find_exchange_row(taker_data, "All") or coinglass_first_metric_row(
            taker_data,
            ["buy_ratio", "sell_ratio", "buy_vol_usd", "sell_vol_usd"],
        )
        if taker_row:
            context["taker_flow"] = {
                "buy_ratio": parse_float(taker_row.get("buy_ratio")),
                "sell_ratio": parse_float(taker_row.get("sell_ratio")),
                "buy_vol_usd": parse_float(taker_row.get("buy_vol_usd")),
                "sell_vol_usd": parse_float(taker_row.get("sell_vol_usd")),
            }

        if market_tier(symbol) in ("core", "large"):
            orderbook_context = self.fetch_coinglass_orderbook_context(symbol, headers)
            if orderbook_context:
                context["orderbook"] = orderbook_context

            long_context = self.fetch_coinglass_major_long_context(base_symbol, headers)
            if long_context:
                context["major_long"] = long_context

        useful_keys = {"open_interest", "funding_oi_weight", "funding_distribution", "taker_flow", "major_long", "orderbook"}
        return context if useful_keys.intersection(context) else None

    def fetch_coinglass_orderbook_context(self, symbol: str, headers: dict[str, str]) -> dict[str, float | None] | None:
        end_time = int(time.time() * 1000)
        start_time = end_time - 8 * 60 * 60 * 1000
        data = self.fetch_coinglass_json(
            COINGLASS_SPOT_ORDERBOOK_ASK_BIDS_HISTORY_ENDPOINT,
            {
                "exchange": "Binance",
                "symbol": symbol,
                "interval": "1h",
                "limit": 8,
                "range": 1,
                "start_time": start_time,
                "end_time": end_time,
            },
            headers,
        )
        return coinglass_orderbook_context_from_rows(coinglass_rows(data))

    def fetch_coinglass_major_long_context(self, base_symbol: str, headers: dict[str, str]) -> dict[str, Any]:
        long_context: dict[str, Any] = {}

        taker_ranges: dict[str, dict[str, float | None]] = {}
        for range_value in COINGLASS_TAKER_LONG_RANGES:
            taker_data = self.fetch_coinglass_json(
                "/api/futures/taker-buy-sell-volume/exchange-list",
                {"symbol": base_symbol, "range": range_value},
                headers,
            )
            taker_row = coinglass_find_exchange_row(taker_data, "All") or coinglass_first_metric_row(
                taker_data,
                ["buy_ratio", "sell_ratio", "buy_vol_usd", "sell_vol_usd"],
            )
            if taker_row:
                taker_ranges[range_value] = {
                    "buy_ratio": parse_float(taker_row.get("buy_ratio")),
                    "sell_ratio": parse_float(taker_row.get("sell_ratio")),
                    "buy_vol_usd": parse_float(taker_row.get("buy_vol_usd")),
                    "sell_vol_usd": parse_float(taker_row.get("sell_vol_usd")),
                }
        if taker_ranges:
            long_context["taker_ranges"] = taker_ranges

        balance_ranges = self.fetch_coinglass_balance_ranges(base_symbol, headers)
        if not balance_ranges:
            balance_ranges = {}
            for range_value in COINGLASS_BALANCE_LONG_RANGES:
                balance = self.fetch_coinglass_balance_range(base_symbol, range_value, headers)
                if balance:
                    balance_ranges[range_value] = balance
        if balance_ranges:
            long_context["balance_ranges"] = balance_ranges

        funding_ranges: dict[str, dict[str, float | None]] = {}
        for range_value in COINGLASS_FUNDING_ACCUMULATED_RANGES:
            funding_data = self.fetch_coinglass_json(
                "/api/futures/funding-rate/accumulated-exchange-list",
                {"symbol": base_symbol, "range": range_value},
                headers,
            )
            funding_row = coinglass_find_exchange_row(funding_data, "All") or coinglass_first_metric_row(
                funding_data,
                ["funding_rate", "funding_rate_percent", "accumulated_funding_rate", "accumulated_funding_rate_percent"],
            )
            if funding_row:
                funding_ranges[range_value] = {
                    "rate": coinglass_first_float(
                        funding_row,
                        [
                            "accumulated_funding_rate",
                            "accumulated_funding_rate_percent",
                            "funding_rate",
                            "funding_rate_percent",
                            "rate",
                        ],
                    )
                }
        if funding_ranges:
            long_context["funding_accumulated_ranges"] = funding_ranges

        return long_context

    def fetch_coinglass_balance_ranges(
        self,
        base_symbol: str,
        headers: dict[str, str],
    ) -> dict[str, dict[str, float | str | None]]:
        list_data = self.fetch_coinglass_json(
            "/api/exchange/balance/list",
            {"symbol": base_symbol},
            headers,
        )
        all_row = coinglass_find_exchange_row(list_data, "All")
        if all_row:
            ranges = coinglass_balance_ranges_from_row(all_row, source=None)
            if ranges:
                return ranges

        summed_ranges = coinglass_summed_balance_ranges(list_data, base_symbol)
        if summed_ranges:
            return summed_ranges

        binance_row = coinglass_find_exchange_row(list_data, "Binance")
        if binance_row:
            return coinglass_balance_ranges_from_row(binance_row, source="Binance")

        return {}

    def fetch_coinglass_balance_range(
        self,
        base_symbol: str,
        range_value: str,
        headers: dict[str, str],
    ) -> dict[str, float | None] | None:
        list_data = self.fetch_coinglass_json(
            "/api/exchange/balance/list",
            {"symbol": base_symbol, "range": range_value},
            headers,
        )
        row = coinglass_find_exchange_row(list_data, "All") or coinglass_find_exchange_row(list_data, "Binance")
        if row is None:
            row = coinglass_first_metric_row(
                list_data,
                ["balance", "balance_usd", "change_percent", "balance_change_percent"],
            )
        if row:
            return {
                "balance": coinglass_first_float(row, ["balance", "amount", "value", "total_balance"]),
                "balance_usd": coinglass_first_float(row, ["balance_usd", "usd", "value_usd", "total_balance_usd"]),
                "change_percent": coinglass_first_float(
                    row,
                    ["change_percent", "balance_change_percent", "change_percentage", "netflow_percent"],
                ),
                "change": coinglass_first_float(row, ["change", "balance_change", "netflow", "net_flow"]),
            }

        chart_data = self.fetch_coinglass_json(
            "/api/exchange/balance/chart",
            {"symbol": base_symbol, "range": range_value},
            headers,
        )
        chart_rows = coinglass_rows(chart_data)
        values = [
            value
            for value in (coinglass_first_float(item, ["balance", "amount", "value", "total_balance"]) for item in chart_rows)
            if value is not None
        ]
        if len(values) < 2:
            return None
        return {
            "balance": values[-1],
            "balance_usd": None,
            "change_percent": percent_change(values[0], values[-1]) if values[0] else None,
            "change": values[-1] - values[0],
        }

    def fetch_coinglass_json(self, endpoint: str, params: dict[str, Any], headers: dict[str, str]) -> Any:
        try:
            response = self.session.get(f"{COINGLASS_BASE_URL}{endpoint}", headers=headers, params=params, timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception:
            logging.debug("CoinGlass fetch failed: %s params=%s", endpoint, params, exc_info=True)
            return None
        if not coinglass_response_ok(data):
            return None
        return data.get("data") if isinstance(data, dict) else None

    def format_coinglass_market_context(self, symbol: str) -> str:
        symbol = symbol.upper()
        now = time.time()
        base_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        cache_key = f"{base_symbol}:{symbol}"
        cached = self.coinglass_market_context_cache.get(cache_key)
        if cached and now - cached[0] < COINGLASS_MARKET_CONTEXT_CACHE_TTL_SECONDS:
            return cached[1]

        context = self.fetch_coinglass_market_context(symbol)
        if not context:
            text = "CoinGlass聚合: n/a"
            if is_major_asset_tier(symbol):
                text = f"{text}\nCoinGlass订单簿: n/a"
        else:
            text = format_coinglass_market_context_text(context)
        self.coinglass_market_context_cache[cache_key] = (now, text)
        return text

    def liquidation_health_report(self, symbol: str | None = None) -> str:
        now = time.time()
        cutoff_1h = now - 3600
        cutoff_15m = now - 900
        all_15m = {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0}
        all_1h = {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0}
        by_symbol: dict[str, dict[str, dict[str, float]]] = {}

        with self.liquidation_lock:
            self.prune_liquidation_events_locked(cutoff_1h)
            connected = self.liquidation_stream_connected
            started_at = self.liquidation_stream_started_at
            last_event_at = self.liquidation_last_event_at
            last_error = self.liquidation_last_error
            events = list(self.liquidation_events)

        for event in events:
            symbol_stats = by_symbol.setdefault(
                event.symbol,
                {
                    "15m": {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0},
                    "1h": {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0},
                },
            )
            if event.event_time >= cutoff_1h:
                update_liquidation_stats_bucket(all_1h, event)
                update_liquidation_stats_bucket(symbol_stats["1h"], event)
            if event.event_time >= cutoff_15m:
                update_liquidation_stats_bucket(all_15m, event)
                update_liquidation_stats_bucket(symbol_stats["15m"], event)

        lines = [
            "强平流状态:",
            "数据说明: Binance实时=服务启动后缓存；CoinGlass历史=最近1h历史兜底",
            f"连接: {'已连接' if connected else '未连接'}",
            f"运行: {format_liquidation_age(now - started_at) if started_at > 0 else '暂无'}",
            f"最近事件: {format_liquidation_age(now - last_event_at) + '前' if last_event_at > 0 else '暂无'}",
            f"内存事件数: {len(events)}",
            f"最近错误: {last_error or '无'}",
            f"全市场近15m强平: 多单 {format_usd(all_15m['long_liq_usd'])} / 空单 {format_usd(all_15m['short_liq_usd'])} / 总计 {format_usd(all_15m['long_liq_usd'] + all_15m['short_liq_usd'])}",
            f"全市场近1h强平: 多单 {format_usd(all_1h['long_liq_usd'])} / 空单 {format_usd(all_1h['short_liq_usd'])} / 总计 {format_usd(all_1h['long_liq_usd'] + all_1h['short_liq_usd'])}",
            "近1h强平最多TOP10:",
        ]

        top_symbols = sorted(
            by_symbol.items(),
            key=lambda item: item[1]["1h"]["long_liq_usd"] + item[1]["1h"]["short_liq_usd"],
            reverse=True,
        )[:10]
        if top_symbols:
            for top_symbol, stats in top_symbols:
                stats_1h = stats["1h"]
                lines.append(
                    f"{top_symbol} 多单 {format_usd(stats_1h['long_liq_usd'])} "
                    f"空单 {format_usd(stats_1h['short_liq_usd'])} "
                    f"总 {format_usd(stats_1h['long_liq_usd'] + stats_1h['short_liq_usd'])}"
                )
        else:
            lines.append("暂无")

        if symbol:
            symbol = symbol.upper()
            stats = by_symbol.get(
                symbol,
                {
                    "15m": {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0},
                    "1h": {"long_liq_usd": 0.0, "short_liq_usd": 0.0, "count": 0.0},
                },
            )
            symbol_15m = stats["15m"]
            symbol_1h = stats["1h"]
            lines.extend(
                [
                    "",
                    f"{symbol} 强平:",
                    f"近15m 多单强平 {format_usd(symbol_15m['long_liq_usd'])} / 空单强平 {format_usd(symbol_15m['short_liq_usd'])}",
                    f"近1h 多单强平 {format_usd(symbol_1h['long_liq_usd'])} / 空单强平 {format_usd(symbol_1h['short_liq_usd'])}",
                ]
            )
            coinglass_stats = self.fetch_coinglass_liquidation_history(symbol)
            coinglass_text = format_coinglass_liquidation_stats(coinglass_stats) if coinglass_stats else None
            if coinglass_text:
                lines.append(coinglass_text)
            lines.append(f"判断: {liquidation_judgement(symbol_1h if symbol_1h['count'] > 0 else coinglass_stats)}")

        if not events:
            lines.extend(
                [
                    "",
                    "暂无缓存事件。注意：强平流只记录服务启动后事件，重启后需要重新积累。",
                ]
            )

        return "\n".join(lines)

    def refresh_symbols_if_due(self, force: bool = False) -> None:
        if not self.screener_config.get("enabled", False):
            return

        refresh_interval = int(self.screener_config.get("refresh_interval_seconds", 3600))
        now = time.time()
        if not force and now - self.last_screened_at < refresh_interval:
            return

        try:
            screened_symbols = self.screen_symbols()
        except Exception:
            logging.exception("Screener refresh failed; keeping existing monitor pool")
            self.last_screened_at = now
            return

        static_symbols = self.config.get("symbols", {})
        new_symbol_configs = {symbol: dict(config) for symbol, config in static_symbols.items()}
        default_mode = self.screener_config.get("default_mode", "both")

        for symbol in screened_symbols:
            new_symbol_configs.setdefault(symbol, {"mode": default_mode})

        added_symbols = sorted(set(new_symbol_configs) - set(self.symbol_configs))
        removed_symbols = set(self.symbol_configs) - set(new_symbol_configs)
        self.symbol_configs = dict(sorted(new_symbol_configs.items()))
        for symbol in removed_symbols:
            self.clear_stale_symbol_signals(symbol, [])

        self.last_screened_at = now
        logging.info("Screened %s symbols into monitor pool", len(self.symbol_configs))
        if added_symbols or removed_symbols:
            self.notify_status(
                "Crypto monitor pool updated",
                f"Monitoring {len(self.symbol_configs)} symbols. "
                f"Added: {', '.join(added_symbols) or '-'}; "
                f"Removed: {', '.join(sorted(removed_symbols)) or '-'}."
            )

    def screen_symbols(self) -> list[str]:
        exchange_symbols = self.fetch_usdt_perpetual_symbols()
        market_cap_filter_enabled = self.screener_config.get("market_cap_filter_enabled", False)
        market_caps = self.fetch_market_caps_by_symbol() if market_cap_filter_enabled else {}
        quote_volumes = self.fetch_24h_quote_volumes()
        oi_values = self.fetch_open_interest_values([row["symbol"] for row in exchange_symbols])
        main_symbols = {str(symbol).upper() for symbol in self.screener_config.get("main_symbols", [])}
        include_symbols = {str(symbol).upper() for symbol in self.screener_config.get("include_symbols", [])}
        exclude_symbols = {str(symbol).upper() for symbol in self.screener_config.get("exclude_symbols", [])}
        min_oi = float(self.screener_config.get("min_open_interest_value_usd", 4_000_000))
        min_volume = float(self.screener_config.get("min_24h_quote_volume_usd", 3_000_000))
        min_cap = float(self.screener_config.get("small_mid_market_cap_min", 15_000_000))
        max_cap = float(self.screener_config.get("small_mid_market_cap_max", 300_000_000))
        max_symbols = int(self.screener_config.get("max_symbols", 80))

        selected: list[tuple[str, float]] = []
        for symbol_info in exchange_symbols:
            symbol = symbol_info["symbol"]
            if symbol in exclude_symbols:
                continue

            oi_value = oi_values.get(symbol, 0.0)
            if oi_value < min_oi:
                continue

            if symbol in main_symbols or symbol in include_symbols:
                selected.append((symbol, oi_value))
                continue

            if not market_cap_filter_enabled:
                selected.append((symbol, oi_value))
                continue

            lookup_symbol = normalize_market_cap_symbol(symbol_info["baseAsset"])
            market_cap = market_caps.get(lookup_symbol)
            if market_cap is None:
                logging.debug("Skipping %s because no CoinGecko market cap matched %s", symbol, lookup_symbol)
                continue
            if min_cap <= market_cap <= max_cap:
                selected.append((symbol, oi_value))

        selected.sort(key=lambda item: item[1], reverse=True)
        return [symbol for symbol, _ in selected[:max_symbols]]

    def fetch_usdt_perpetual_symbols(self) -> list[dict[str, Any]]:
        quote_asset = str(self.screener_config.get("quote_asset", "USDT")).upper()
        data = self.get("/fapi/v1/exchangeInfo", {})
        symbols = []
        for row in data.get("symbols", []):
            if row.get("contractType") != "PERPETUAL":
                continue
            if row.get("quoteAsset") != quote_asset:
                continue
            if row.get("status") != "TRADING":
                continue
            symbols.append(row)
        return symbols

    def fetch_24h_quote_volumes(self) -> dict[str, float]:
        rows = self.get("/fapi/v1/ticker/24hr", {})
        volumes = {}
        for row in rows:
            symbol = row.get("symbol")
            quote_volume = row.get("quoteVolume")
            if symbol and quote_volume is not None:
                volumes[str(symbol)] = float(quote_volume)
        return volumes

    def fetch_market_caps_by_symbol(self) -> dict[str, float]:
        cached = self.load_market_cap_cache()
        if cached is not None:
            logging.info("Using cached CoinGecko market caps")
            return cached

        pages = int(self.screener_config.get("coingecko_pages", 8))
        per_page = int(self.screener_config.get("coingecko_per_page", 250))
        delay = float(self.screener_config.get("coingecko_request_delay_seconds", 8))
        market_caps: dict[str, float] = {}

        for page in range(1, pages + 1):
            if page > 1 and delay > 0:
                time.sleep(delay)
            response = self.session.get(
                COINGECKO_MARKETS_URL,
                params={
                    "vs_currency": "usd",
                    "order": "market_cap_desc",
                    "per_page": per_page,
                    "page": page,
                    "sparkline": "false",
                },
                timeout=15,
            )
            if response.status_code == 429:
                cached = self.load_market_cap_cache(ignore_ttl=True)
                if cached is not None:
                    logging.warning("CoinGecko rate limited; using stale market cap cache")
                    return cached
                raise RuntimeError("CoinGecko rate limited and no cache is available")
            response.raise_for_status()
            rows = response.json()
            if not rows:
                break
            for row in rows:
                symbol = str(row.get("symbol", "")).lower()
                market_cap = row.get("market_cap")
                if not symbol or market_cap is None:
                    continue
                current = market_caps.get(symbol)
                if current is None or float(market_cap) > current:
                    market_caps[symbol] = float(market_cap)
        self.save_market_cap_cache(market_caps)
        return market_caps

    def load_market_cap_cache(self, ignore_ttl: bool = False) -> dict[str, float] | None:
        path = self.market_cap_cache_path()
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            created_at = float(payload.get("created_at", 0))
            ttl = int(self.screener_config.get("market_cap_cache_ttl_seconds", 21600))
            if not ignore_ttl and time.time() - created_at > ttl:
                return None
            data = payload.get("market_caps", {})
            return {str(symbol): float(market_cap) for symbol, market_cap in data.items()}
        except Exception:
            logging.warning("Failed to read market cap cache", exc_info=True)
            return None

    def save_market_cap_cache(self, market_caps: dict[str, float]) -> None:
        path = self.market_cap_cache_path()
        payload = {"created_at": time.time(), "market_caps": market_caps}
        try:
            with path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, separators=(",", ":"))
        except Exception:
            logging.warning("Failed to write market cap cache", exc_info=True)

    def market_cap_cache_path(self):
        from pathlib import Path

        configured = self.screener_config.get("market_cap_cache_path", "market_cap_cache.json")
        path = Path(str(configured))
        if path.is_absolute():
            return path
        return Path.cwd() / path

    def fetch_open_interest_value(self, symbol: str) -> float:
        try:
            oi_row = self.get("/fapi/v1/openInterest", {"symbol": symbol})
            price_row = self.get("/fapi/v1/ticker/price", {"symbol": symbol})
            return float(oi_row["openInterest"]) * float(price_row["price"])
        except Exception:
            logging.debug("Failed to fetch open interest value for %s", symbol, exc_info=True)
            return 0.0

    def fetch_open_interest_values(self, symbols: list[str]) -> dict[str, float]:
        workers = int(self.screener_config.get("oi_fetch_workers", 12))
        values: dict[str, float] = {}
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_symbol = {
                executor.submit(self.fetch_open_interest_value, symbol): symbol
                for symbol in symbols
            }
            for future in concurrent.futures.as_completed(future_to_symbol):
                symbol = future_to_symbol[future]
                try:
                    values[symbol] = float(future.result())
                except Exception:
                    logging.debug("Failed to fetch open interest value for %s", symbol, exc_info=True)
                    values[symbol] = 0.0
        return values

    def run_cycle(self) -> None:
        started = time.monotonic()
        self.refresh_benchmark_snapshot()
        items = list(self.symbol_configs.items())
        processed = 0
        signal_count = 0
        if self.scan_workers <= 1:
            for symbol, symbol_config in items:
                signal_count += self.run_symbol_cycle(symbol, symbol_config)
                processed += 1
            self.latest_snapshots_updated_at = time.time()
            logging.info("Scan cycle completed: symbols=%s signals=%s elapsed=%.2fs", processed, signal_count, time.monotonic() - started)
            return

        with concurrent.futures.ThreadPoolExecutor(max_workers=self.scan_workers) as executor:
            future_to_item = {
                executor.submit(self.fetch_snapshot, symbol): (symbol, symbol_config)
                for symbol, symbol_config in items
            }
            for future in concurrent.futures.as_completed(future_to_item):
                symbol, symbol_config = future_to_item[future]
                try:
                    snapshot = future.result()
                except Exception:
                    logging.exception("Failed to fetch snapshot for %s", symbol)
                    continue
                signal_count += self.process_snapshot(symbol, symbol_config, snapshot)
                processed += 1
        self.latest_snapshots_updated_at = time.time()
        logging.info("Scan cycle completed: symbols=%s signals=%s elapsed=%.2fs", processed, signal_count, time.monotonic() - started)

    def run_symbol_cycle(self, symbol: str, symbol_config: dict[str, Any]) -> int:
        snapshot = self.fetch_snapshot(symbol)
        return self.process_snapshot(symbol, symbol_config, snapshot)

    def process_snapshot(self, symbol: str, symbol_config: dict[str, Any], snapshot: MarketSnapshot) -> int:
        if self.log_each_symbol or logging.getLogger().isEnabledFor(logging.DEBUG):
            logging.info(
                "%s 价格=%+.2f%% oi=%+.2f%% ls=%s taker=%s 资金费率=%s%%",
                symbol,
                snapshot.price_change_percent,
                snapshot.oi_change_percent,
                self.format_optional(snapshot.global_long_short_ratio),
                self.format_optional(snapshot.taker_buy_sell_ratio),
                self.format_optional(snapshot.funding_rate_percent),
            )
        signals = self.evaluate_snapshot(snapshot, symbol_config)
        self.latest_snapshots[symbol] = snapshot
        self.clear_stale_symbol_signals(symbol, signals)
        combined_signal = self.combined_signal(snapshot, signals)
        if combined_signal:
            signals = [combined_signal]
        for signal in signals:
            self.send_once(signal)
        return len(signals)

    def fetch_snapshot(self, symbol: str) -> MarketSnapshot:
        klines = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": self.period, "limit": self.limit})
        oi_rows = self.get_data("openInterestHist", {"symbol": symbol, "period": self.period, "limit": self.limit})
        global_ls = self.get_data("globalLongShortAccountRatio", {"symbol": symbol, "period": self.period, "limit": 2})
        top_position = self.get_data("topLongShortPositionRatio", {"symbol": symbol, "period": self.period, "limit": 2})
        top_account = self.get_data("topLongShortAccountRatio", {"symbol": symbol, "period": self.period, "limit": 2})
        taker = self.get_data("takerlongshortRatio", {"symbol": symbol, "period": self.period, "limit": 2})
        funding = self.get("/fapi/v1/premiumIndex", {"symbol": symbol})
        ticker_24h = self.get("/fapi/v1/ticker/24hr", {"symbol": symbol})

        confirm_price_change = None
        confirm_oi_change = None
        if self.confirmation_config.get("enabled", False):
            confirm_period = str(self.confirmation_config.get("period", self.period))
            confirm_limit = int(self.confirmation_config.get("limit", 3))
            confirm_klines = self.get("/fapi/v1/klines", {"symbol": symbol, "interval": confirm_period, "limit": confirm_limit})
            confirm_oi_rows = self.get_data("openInterestHist", {"symbol": symbol, "period": confirm_period, "limit": confirm_limit})
            confirm_price_change = percent_change(float(confirm_klines[0][4]), float(confirm_klines[-1][4]))
            confirm_oi_change = percent_change(float(confirm_oi_rows[0]["sumOpenInterestValue"]), float(confirm_oi_rows[-1]["sumOpenInterestValue"]))

        first_close = float(klines[0][4])
        last_close = float(klines[-1][4])
        first_oi = float(oi_rows[0]["sumOpenInterestValue"])
        last_oi = float(oi_rows[-1]["sumOpenInterestValue"])
        net_flow_usd, net_flow_ratio = self.fetch_flow_metrics(symbol, last_close)
        high_24h = float(ticker_24h.get("highPrice", last_close) or last_close)
        low_24h = float(ticker_24h.get("lowPrice", last_close) or last_close)
        if high_24h > low_24h:
            price_position_24h = (last_close - low_24h) / (high_24h - low_24h) * 100
        else:
            price_position_24h = None
        quote_volume_24h = float(ticker_24h.get("quoteVolume", 0) or 0)
        volume_ratio_24h = quote_volume_24h / last_oi if last_oi > 0 else None

        return MarketSnapshot(
            symbol=symbol,
            price_change_percent=percent_change(first_close, last_close),
            oi_change_percent=percent_change(first_oi, last_oi),
            global_long_short_ratio=latest_float(global_ls, "longShortRatio"),
            top_position_ratio=latest_float(top_position, "longShortRatio"),
            top_account_ratio=latest_float(top_account, "longShortRatio"),
            taker_buy_sell_ratio=latest_float(taker, "buySellRatio"),
            funding_rate_percent=float(funding["lastFundingRate"]) * 100 if funding.get("lastFundingRate") is not None else None,
            confirm_price_change_percent=confirm_price_change,
            confirm_oi_change_percent=confirm_oi_change,
            net_flow_usd=net_flow_usd,
            net_flow_ratio=net_flow_ratio,
            price_position_24h=price_position_24h,
            high_24h=high_24h,
            low_24h=low_24h,
            quote_volume_24h=quote_volume_24h,
            volume_ratio_24h=volume_ratio_24h,
            close_price=last_close,
        )

    def fetch_flow_metrics(self, symbol: str, price: float) -> tuple[dict[str, float], dict[str, float]]:
        if not self.flow_config.get("enabled", False):
            return {}, {}

        net_flow_usd = {}
        net_flow_ratio = {}
        configured_periods = [str(period) for period in self.flow_config.get("periods", FLOW_PERIODS)]
        periods = list(dict.fromkeys(configured_periods + FLOW_PERIODS))

        for period in periods:
            try:
                rows = self.get_data("takerlongshortRatio", {"symbol": symbol, "period": period, "limit": 1})
                if not rows:
                    continue
                row = rows[-1]
                buy_vol = float(row.get("buyVol", 0))
                sell_vol = float(row.get("sellVol", 0))
                ratio = float(row.get("buySellRatio", 0))
                net_flow_usd[str(period)] = (buy_vol - sell_vol) * price
                net_flow_ratio[str(period)] = ratio
            except Exception:
                logging.debug("Failed to fetch flow metrics for %s %s", symbol, period, exc_info=True)

        return net_flow_usd, net_flow_ratio

    def evaluate_snapshot(self, snapshot: MarketSnapshot, symbol_config: dict[str, Any]) -> list[Signal]:
        mode = symbol_config.get("mode", "both")
        signals = []
        if mode in ("both", "discovery"):
            discovery = self.discovery_signal(snapshot)
            if discovery:
                signals.append(discovery)
        if mode in ("both", "top_risk"):
            top_risk = self.top_risk_signal(snapshot)
            if top_risk:
                signals.append(top_risk)
            top_exhaustion = self.top_exhaustion_signal(snapshot)
            if top_exhaustion:
                signals.append(top_exhaustion)
            distribution = self.distribution_signal(snapshot)
            if distribution:
                signals.append(distribution)
            bottom_reversal = self.bottom_reversal_signal(snapshot)
            if bottom_reversal:
                signals.append(bottom_reversal)
        return signals

    def refresh_benchmark_snapshot(self) -> None:
        if not self.market_filter_config.get("enabled", False):
            self.benchmark_snapshot = None
            return
        symbol = str(self.market_filter_config.get("benchmark_symbol", "BTCUSDT")).upper()
        try:
            self.benchmark_snapshot = self.fetch_snapshot(symbol)
        except Exception:
            logging.warning("Failed to refresh benchmark snapshot for %s", symbol, exc_info=True)
            self.benchmark_snapshot = None

    def combined_signal(self, snapshot: MarketSnapshot, signals: list[Signal]) -> Signal | None:
        kinds = {signal.kind for signal in signals}
        if {"discovery", "top_risk"}.issubset(kinds):
            funding_hot = optional_gte(snapshot.funding_rate_percent, float(self.rules.get("top_risk", {}).get("min_funding_rate_percent", 0.03)))
            if not (self.flow_positive(snapshot, "15m") and (self.flow_positive(snapshot, "1h") or funding_hot)):
                return None
            score = max(signal.score for signal in signals) + 1
            return Signal(
                symbol=snapshot.symbol,
                kind="hot_breakout",
                score=score,
                title=f"{snapshot.symbol} hot breakout",
                message=self.describe(snapshot, "启动动能很强，但杠杆或资金费率已经偏热。"),
                key=f"{snapshot.symbol}:hot_breakout",
                snapshot=snapshot,
            )
        return None

    def discovery_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        rule = self.rules.get("discovery", {})
        price_ok = snapshot.price_change_percent >= tier_threshold(snapshot.symbol, rule, "min_price_change_percent", 1.2)
        oi_ok = snapshot.oi_change_percent >= tier_threshold(snapshot.symbol, rule, "min_oi_change_percent", 4.0)
        taker_ok = optional_gte(snapshot.taker_buy_sell_ratio, tier_threshold(snapshot.symbol, rule, "min_taker_buy_sell_ratio", 1.15))
        crowd_ok = optional_lte(snapshot.global_long_short_ratio, float(rule.get("max_global_long_short_ratio", 1.8)))
        confirm_ok = self.confirm_discovery(snapshot)
        market_ok = self.market_allows_discovery()
        flow_ok = self.flow_positive(snapshot, "15m")
        volume_ok = snapshot.volume_ratio_24h is None or snapshot.volume_ratio_24h >= 2

        if not (price_ok and oi_ok and taker_ok and confirm_ok and market_ok and flow_ok and volume_ok):
            return None

        score = sum([price_ok, oi_ok, taker_ok, crowd_ok])
        return Signal(
            symbol=snapshot.symbol,
            kind="discovery",
            score=score,
            title=f"{snapshot.symbol} possible early breakout",
            message=self.describe(snapshot, "价格和 OI 同步上升，主动买盘支持。"),
            key=f"{snapshot.symbol}:discovery",
            snapshot=snapshot,
        )

    def top_risk_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        rule = self.rules.get("top_risk", {})
        price_ok = snapshot.price_change_percent >= tier_threshold(snapshot.symbol, rule, "top_min_price_change_percent", float(rule.get("min_price_change_percent", 3.0)))
        oi_ok = snapshot.oi_change_percent >= tier_threshold(snapshot.symbol, rule, "top_min_oi_change_percent", float(rule.get("min_oi_change_percent", 8.0)))
        crowd_ok = optional_gte(snapshot.global_long_short_ratio, float(rule.get("min_global_long_short_ratio", 2.0)))
        crowd_extreme = snapshot.global_long_short_ratio is not None and snapshot.global_long_short_ratio >= 3
        taker_weak = optional_lte(snapshot.taker_buy_sell_ratio, float(rule.get("max_taker_buy_sell_ratio", 0.95)))
        funding_hot = optional_gte(snapshot.funding_rate_percent, float(rule.get("min_funding_rate_percent", 0.03)))
        confirm_ok = self.confirm_top_risk(snapshot)

        overheated = crowd_ok or funding_hot
        if not (price_ok and oi_ok and overheated and confirm_ok):
            return None

        score = sum([price_ok, oi_ok, crowd_ok, taker_weak, funding_hot])
        return Signal(
            symbol=snapshot.symbol,
            kind="top_risk",
            score=score,
            title=f"{snapshot.symbol} crowded 看多 top-risk",
            message=self.describe(snapshot, "Price has run up while leverage and 看多 crowding look elevated."),
            key=f"{snapshot.symbol}:top_risk",
            snapshot=snapshot,
        )

    def top_exhaustion_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        high_position = snapshot.price_position_24h is not None and snapshot.price_position_24h >= 80
        price_extended = snapshot.price_change_percent >= 4
        oi_hot = snapshot.oi_change_percent >= 8
        funding_hot = optional_gte(snapshot.funding_rate_percent, 0.03)
        taker_fading = optional_lte(snapshot.taker_buy_sell_ratio, 1.05)
        flow_fading = summary_flow_value(snapshot, "15m") < 0 or summary_flow_value(snapshot, "1h") < 0

        if not (high_position and price_extended and oi_hot and (funding_hot or taker_fading or flow_fading)):
            return None

        score = sum([high_position, price_extended, oi_hot, funding_hot, taker_fading, flow_fading])
        return Signal(
            symbol=snapshot.symbol,
            kind="top_exhaustion",
            score=score,
            title=f"{snapshot.symbol} top exhaustion",
            message=self.describe(snapshot, "高位拉升后出现衰竭迹象，追多风险升高。"),
            key=f"{snapshot.symbol}:top_exhaustion",
            snapshot=snapshot,
        )

    def bottom_reversal_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        low_position = snapshot.price_position_24h is not None and snapshot.price_position_24h <= 35
        price_oversold = snapshot.price_change_percent <= -2.5
        oi_not_expanding = snapshot.oi_change_percent <= 2
        funding_negative = optional_lte(snapshot.funding_rate_percent, -0.02)
        taker_recovering = optional_gte(snapshot.taker_buy_sell_ratio, 1.05)
        flow_recovering = summary_flow_value(snapshot, "15m") > 0
        one_hour_not_bad = summary_flow_value(snapshot, "1h") >= 0 or taker_recovering

        if not (low_position and price_oversold and oi_not_expanding and flow_recovering and one_hour_not_bad):
            return None

        score = sum([low_position, price_oversold, oi_not_expanding, funding_negative, taker_recovering, flow_recovering, one_hour_not_bad])
        return Signal(
            symbol=snapshot.symbol,
            kind="bottom_reversal",
            score=score,
            title=f"{snapshot.symbol} bottom reversal watch",
            message=self.describe(snapshot, "超跌后出现资金回流和止跌迹象，适合抄底观察。"),
            key=f"{snapshot.symbol}:bottom_reversal",
            snapshot=snapshot,
        )

    def distribution_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        rule = self.rules.get("distribution", {})
        price_is_higher = snapshot.price_change_percent >= float(rule.get("min_price_change_percent", 2.0))
        oi_is_falling = snapshot.oi_change_percent <= float(rule.get("max_oi_change_percent", -3.0))
        taker_is_weak = optional_lte(snapshot.taker_buy_sell_ratio, float(rule.get("max_taker_buy_sell_ratio", 0.9)))
        confirm_ok = self.confirm_distribution(snapshot)
        flow_ok = self.flow_negative(snapshot, "15m")
        score = sum([price_is_higher, oi_is_falling, taker_is_weak])

        if not (price_is_higher and oi_is_falling and taker_is_weak and confirm_ok and flow_ok):
            return None

        return Signal(
            symbol=snapshot.symbol,
            kind="distribution",
            score=score,
            title=f"{snapshot.symbol} possible distribution",
            message=self.describe(snapshot, "价格仍在高位，但 OI 和主动买盘走弱。"),
            key=f"{snapshot.symbol}:distribution",
            snapshot=snapshot,
        )

    def flow_positive(self, snapshot: MarketSnapshot, period: str) -> bool:
        if not self.flow_config.get("enabled", False):
            return True
        value = snapshot.net_flow_usd.get(period)
        return value is not None and value > 0

    def flow_negative(self, snapshot: MarketSnapshot, period: str) -> bool:
        if not self.flow_config.get("enabled", False):
            return True
        value = snapshot.net_flow_usd.get(period)
        return value is not None and value < 0

    def market_allows_discovery(self) -> bool:
        if not self.market_filter_config.get("enabled", False):
            return True
        if self.benchmark_snapshot is None:
            return True
        min_confirm = float(self.market_filter_config.get("discovery_min_benchmark_confirm_price_percent", -0.8))
        value = self.benchmark_snapshot.confirm_price_change_percent
        return value is None or value >= min_confirm

    def confirm_discovery(self, snapshot: MarketSnapshot) -> bool:
        if not self.confirmation_config.get("enabled", False):
            return True
        return (
            optional_gte(snapshot.confirm_price_change_percent, float(self.confirmation_config.get("discovery_min_price_change_percent", 0)))
            and optional_gte(snapshot.confirm_oi_change_percent, float(self.confirmation_config.get("discovery_min_oi_change_percent", 0)))
        )

    def confirm_top_risk(self, snapshot: MarketSnapshot) -> bool:
        if not self.confirmation_config.get("enabled", False):
            return True
        return optional_gte(snapshot.confirm_price_change_percent, float(self.confirmation_config.get("top_risk_min_price_change_percent", -1.5)))

    def confirm_distribution(self, snapshot: MarketSnapshot) -> bool:
        if not self.confirmation_config.get("enabled", False):
            return True
        return optional_lte(snapshot.confirm_oi_change_percent, float(self.confirmation_config.get("distribution_max_oi_change_percent", 0)))

    def describe(self, snapshot: MarketSnapshot, reason: str) -> str:
        return (
            f"{reason} 价格={snapshot.close_price:.8g}, "
            f"价格变化={snapshot.price_change_percent:+.2f}%, "
            f"OI变化={snapshot.oi_change_percent:+.2f}%, "
            f"全局多空比={self.format_optional(snapshot.global_long_short_ratio)}, "
            f"大户持仓多空比={self.format_optional(snapshot.top_position_ratio)}, "
            f"大户账户多空比={self.format_optional(snapshot.top_account_ratio)}, "
            f"主动买卖比={self.format_optional(snapshot.taker_buy_sell_ratio)}, "
            f"资金费率={self.format_optional(snapshot.funding_rate_percent)}%, "
            f"confirm_价格={self.format_optional(snapshot.confirm_price_change_percent)}%, "
            f"确认OI={self.format_optional(snapshot.confirm_oi_change_percent)}%, "
            f"5m资金流={format_usd(snapshot.net_flow_usd.get('5m'))}, "
            f"15m资金流={format_usd(snapshot.net_flow_usd.get('15m'))}, "
            f"1h资金流={format_usd(snapshot.net_flow_usd.get('1h'))}, "
            f"4h资金流={format_usd(snapshot.net_flow_usd.get('4h'))}, "
            f"12h资金流={format_usd(snapshot.net_flow_usd.get('12h'))}, "
            f"24h资金流={format_usd(snapshot.net_flow_usd.get('24h'))}."
        )

    def cooldown_seconds_for(self, signal: Signal) -> int:
        defaults = {
            "hot_breakout": 1200,
            "top_risk": 1800,
            "top_exhaustion": 1800,
            "distribution": 1800,
            "bottom_reversal": 1800,
            "discovery": 3600,
        }
        return int(self.alert_cooldowns.get(signal.kind, defaults.get(signal.kind, self.alert_cooldown_seconds)))

    def market_push_mode(self) -> str:
        snapshots = [
            snapshot for symbol, snapshot in self.latest_snapshots.items()
            if symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT")
        ]
        if len(snapshots) < 2:
            return "中性"

        strength = 0
        for snapshot in snapshots:
            if short_term_score(snapshot) >= 6:
                strength += 1
            if mid_term_score(snapshot) >= 6:
                strength += 1
            if summary_flow_value(snapshot, "15m") > 0:
                strength += 1
            if summary_flow_value(snapshot, "1h") > 0:
                strength += 1

        if strength >= 6:
            return "strong"
        if strength >= 3:
            return "中性"
        return "weak"

    def should_push_signal(self, signal: Signal) -> bool:
        if signal.kind in ("hot_breakout", "distribution", "bottom_reversal", "top_exhaustion"):
            return True
        score = signal_strength_score(signal)
        if signal.kind == "discovery":
            threshold = 22 if self.market_push_mode() == "weak" else 15
            return score >= threshold
        if signal.kind == "top_risk":
            return score >= 12
        return True

    def send_once(self, signal: Signal) -> None:
        now = time.time()
        last_alerted_at = self.last_alerted_at.get(signal.key, 0)
        if now - last_alerted_at < self.cooldown_seconds_for(signal):
            return
        self.active_signals.add(signal.key)
        self.last_alerted_at[signal.key] = now
        self.save_state()
        logging.warning("%s score=%s 强度=%.2f - %s", signal.title, signal.score, signal_strength_score(signal), signal.message)
        try:
            self.log_signal(signal)
        except Exception:
            logging.exception("Failed to write signal log")
        self.notify(signal)

    def log_signal(self, signal: Signal) -> None:
        snapshot = signal.snapshot
        main_score = main_asset_score(snapshot) if snapshot else None
        main_score_components = main_score.components if main_score else {}
        trap_score, trap_label, trap_reason = trap_risk_score(snapshot, signal) if snapshot else ("", "", "")
        priority, quality_score, quality_reason = signal_priority(signal, snapshot)
        suppressed_from_telegram, _suppressed_reason = self.telegram_signal_suppression(signal, priority, quality_score)
        row = {
            "time": dt.datetime.now(dt.UTC).isoformat(),
            "symbol": signal.symbol,
            "kind": signal.kind,
            "score": signal.score,
            "title": signal.title,
            "strength_score": signal_strength_score(signal),
            "price": snapshot.close_price if snapshot else "",
            "price_change_percent": snapshot.price_change_percent if snapshot else "",
            "oi_change_percent": snapshot.oi_change_percent if snapshot else "",
            "global_long_short_ratio": snapshot.global_long_short_ratio if snapshot else "",
            "top_position_ratio": snapshot.top_position_ratio if snapshot else "",
            "top_account_ratio": snapshot.top_account_ratio if snapshot else "",
            "taker_buy_sell_ratio": snapshot.taker_buy_sell_ratio if snapshot else "",
            "funding_rate_percent": snapshot.funding_rate_percent if snapshot else "",
            "net_flow_5m_usd": snapshot.net_flow_usd.get("5m", "") if snapshot else "",
            "net_flow_15m_usd": snapshot.net_flow_usd.get("15m", "") if snapshot else "",
            "net_flow_1h_usd": snapshot.net_flow_usd.get("1h", "") if snapshot else "",
            "net_flow_4h_usd": snapshot.net_flow_usd.get("4h", "") if snapshot else "",
            "net_flow_12h_usd": snapshot.net_flow_usd.get("12h", "") if snapshot else "",
            "net_flow_24h_usd": snapshot.net_flow_usd.get("24h", "") if snapshot else "",
            "net_flow_5m_ratio": snapshot.net_flow_ratio.get("5m", "") if snapshot else "",
            "net_flow_15m_ratio": snapshot.net_flow_ratio.get("15m", "") if snapshot else "",
            "net_flow_1h_ratio": snapshot.net_flow_ratio.get("1h", "") if snapshot else "",
            "net_flow_4h_ratio": snapshot.net_flow_ratio.get("4h", "") if snapshot else "",
            "net_flow_12h_ratio": snapshot.net_flow_ratio.get("12h", "") if snapshot else "",
            "net_flow_24h_ratio": snapshot.net_flow_ratio.get("24h", "") if snapshot else "",
            "price_position_24h": snapshot.price_position_24h if snapshot else "",
            "high_24h": snapshot.high_24h if snapshot else "",
            "low_24h": snapshot.low_24h if snapshot else "",
            "quote_volume_24h": snapshot.quote_volume_24h if snapshot else "",
            "volume_ratio_24h": snapshot.volume_ratio_24h if snapshot else "",
            "short_term_score": short_term_score(snapshot) if snapshot else "",
            "mid_term_score": mid_term_score(snapshot) if snapshot else "",
            "flow_alignment_score": flow_alignment_score(snapshot) if snapshot else "",
            "long_flow_alignment_score": long_flow_alignment_score(snapshot) if snapshot else "",
            "structure_label": market_structure_label(snapshot) if snapshot else "",
            "trade_plan": signal_trade_plan(signal) if snapshot else "",
            "main_asset_score": main_score.total_score if main_score else "",
            "main_asset_score_label": main_score.label if main_score else "",
            "main_asset_trend_score": main_score_components.get("趋势", ""),
            "main_asset_flow_score": main_score_components.get("资金", ""),
            "main_asset_derivatives_score": main_score_components.get("衍生品", ""),
            "main_asset_spot_orderbook_score": main_score_components.get("现货订单簿", ""),
            "main_asset_risk_penalty": main_score_components.get("风险扣分", ""),
            "trap_risk_score": trap_score,
            "trap_risk_label": trap_label,
            "trap_risk_reason": trap_reason,
            "signal_priority": priority,
            "signal_quality_score": quality_score,
            "signal_quality_reason": quality_reason,
            "suppressed_from_telegram": int(suppressed_from_telegram),
        }
        path = Path(self.signal_log_path)
        fieldnames, write_header = ensure_csv_schema(path, SIGNAL_LOG_FIELDS)
        with path.open("a", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
            if write_header:
                writer.writeheader()
            writer.writerow({field: row.get(field, "") for field in fieldnames})

    def load_state(self) -> None:
        path = Path(self.state_path)
        if not path.exists():
            return
        try:
            with path.open("r", encoding="utf-8") as file:
                payload = json.load(file)
            self.last_alerted_at = {
                str(key): float(value)
                for key, value in payload.get("last_alerted_at", {}).items()
            }
        except Exception:
            logging.warning("Failed to load monitor state", exc_info=True)

    def save_state(self) -> None:
        path = Path(self.state_path)
        payload = {"last_alerted_at": self.last_alerted_at}
        try:
            with path.open("w", encoding="utf-8") as file:
                json.dump(payload, file, separators=(",", ":"))
        except Exception:
            logging.warning("Failed to save monitor state", exc_info=True)

    def clear_stale_symbol_signals(self, symbol: str, signals: list[Signal]) -> None:
        current_keys = {signal.key for signal in signals}
        for key in list(self.active_signals):
            if key.startswith(f"{symbol}:") and key not in current_keys:
                self.active_signals.remove(key)

    def notify(self, signal: Signal) -> None:
        notifications = self.config.get("notifications", {})
        webhook_url = notifications.get("webhook_url")
        telegram = notifications.get("telegram", {})
        payload = {
            "title": signal.title,
            "message": signal.message,
            "symbol": signal.symbol,
            "kind": signal.kind,
            "score": signal.score,
            "time": dt.datetime.now(dt.UTC).isoformat(),
        }

        if webhook_url:
            self.post_json(webhook_url, payload)
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        if bot_token and chat_ids:
            priority, quality_score, quality_reason = signal_priority(signal, signal.snapshot)
            suppressed, suppressed_reason = self.telegram_signal_suppression(signal, priority, quality_score)
            self.update_signal_quality_stats(signal, priority, suppressed)
            if suppressed:
                logging.info(
                    "Telegram signal suppressed: %s %s priority=%s quality=%s reason=%s",
                    signal.symbol,
                    signal.kind,
                    priority,
                    quality_score,
                    suppressed_reason or quality_reason,
                )
                self.enqueue_telegram_signal_digest(signal, priority, quality_score)
                return
            for chat_id in split_chat_ids(chat_ids):
                self.send_telegram(bot_token, chat_id, signal, priority, quality_score, quality_reason)
            self.telegram_signal_cooldowns[self.telegram_signal_key(signal)] = (time.time(), quality_score)

    def update_signal_quality_stats(self, signal: Signal, priority: str, suppressed: bool) -> None:
        with self.signal_quality_stats_lock:
            self.signal_quality_stats["total"] += 1
            if suppressed:
                self.signal_quality_stats["suppressed"] += 1
            else:
                self.signal_quality_stats["realtime_sent"] += 1

            by_priority = self.signal_quality_stats["by_priority"]
            by_priority[priority] = by_priority.get(priority, 0) + 1

            by_kind = self.signal_quality_stats["by_kind"]
            by_kind[signal.kind] = by_kind.get(signal.kind, 0) + 1

            by_symbol = self.signal_quality_stats["by_symbol"]
            by_symbol[signal.symbol] = by_symbol.get(signal.symbol, 0) + 1

            if suppressed:
                suppressed_by_priority = self.signal_quality_stats["suppressed_by_priority"]
                suppressed_by_priority[priority] = suppressed_by_priority.get(priority, 0) + 1

    def get(self, path: str, params: dict[str, Any]) -> Any:
        response = self.session.get(f"{BINANCE_FAPI_BASE}{path}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def get_data(self, path: str, params: dict[str, Any]) -> Any:
        response = self.session.get(f"{BINANCE_FUTURES_DATA_BASE}/{path}", params=params, timeout=10)
        response.raise_for_status()
        return response.json()

    def post_json(self, url: str, payload: dict[str, Any]) -> None:
        try:
            response = self.session.post(url, json=payload, timeout=5)
            response.raise_for_status()
        except requests.Timeout:
            logging.warning("Notification request timed out")
        except Exception:
            logging.warning("Failed to send notification", exc_info=True)

    def telegram_signal_key(self, signal: Signal) -> str:
        return f"{signal.symbol}/{signal.kind}"

    def telegram_signal_filter_settings(self) -> tuple[set[str], list[str], int, int]:
        config = self.config.get("telegram_signal_filter", {})
        if not isinstance(config, dict):
            config = {}
        realtime_priorities = parse_priority_list(
            config.get("realtime_priorities"),
            DEFAULT_TELEGRAM_REALTIME_PRIORITIES,
        )
        digest_priorities = parse_priority_list(
            config.get("digest_priorities"),
            DEFAULT_TELEGRAM_DIGEST_PRIORITIES,
        )
        digest_interval_minutes = parse_positive_int(
            config.get("digest_interval_minutes"),
            DEFAULT_TELEGRAM_DIGEST_INTERVAL_MINUTES,
        )
        digest_max_per_priority = parse_positive_int(
            config.get("digest_max_per_priority"),
            DEFAULT_TELEGRAM_DIGEST_MAX_PER_PRIORITY,
        )
        with self.runtime_realtime_priorities_lock:
            if self.runtime_realtime_priorities_override is not None:
                realtime_priorities = list(self.runtime_realtime_priorities_override)
        return set(realtime_priorities), digest_priorities, digest_interval_minutes, digest_max_per_priority

    def configured_realtime_priorities(self) -> set[str]:
        config = self.config.get("telegram_signal_filter", {})
        if not isinstance(config, dict):
            config = {}
        return set(parse_priority_list(config.get("realtime_priorities"), DEFAULT_TELEGRAM_REALTIME_PRIORITIES))

    def runtime_realtime_priorities_status(self) -> tuple[set[str], bool]:
        with self.runtime_realtime_priorities_lock:
            override = self.runtime_realtime_priorities_override
            if override is None:
                return self.configured_realtime_priorities(), False
            return set(override), True

    def telegram_signal_suppression(self, signal: Signal, priority: str, quality_score: int) -> tuple[bool, str]:
        realtime_priorities, _digest_priorities, _digest_interval_minutes, _digest_max_per_priority = (
            self.telegram_signal_filter_settings()
        )
        if priority not in realtime_priorities:
            return True, f"priority {priority} below realtime Telegram threshold"

        key = self.telegram_signal_key(signal)
        last = self.telegram_signal_cooldowns.get(key)
        if last:
            last_pushed_at, last_quality_score = last
            age = time.time() - last_pushed_at
            if age < 1800 and quality_score < 75:
                return True, (
                    f"duplicate {key} within 30m quality={quality_score}<75 "
                    f"last_quality={last_quality_score}"
                )
        return False, ""

    def send_telegram(
        self,
        bot_token: str,
        chat_id: str,
        signal: Signal,
        priority: str | None = None,
        quality_score: int | None = None,
        quality_reason: str | None = None,
    ) -> None:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        liquidation_text = self.format_liquidation_stats(signal.symbol)
        if priority is None or quality_score is None or quality_reason is None:
            priority, quality_score, quality_reason = signal_priority(signal, signal.snapshot)
        payload = {
            "chat_id": chat_id,
            "text": format_signal_for_telegram(signal, liquidation_text, priority, quality_score, quality_reason),
        }
        self.post_json(url, payload)

    def enqueue_telegram_signal_digest(self, signal: Signal, priority: str, quality_score: int) -> None:
        _realtime_priorities, digest_priorities, _digest_interval_minutes, _digest_max_per_priority = (
            self.telegram_signal_filter_settings()
        )
        if priority not in set(digest_priorities):
            return

        snapshot = signal.snapshot
        trap_score: int | str = ""
        main_score_value: int | None = None
        if snapshot:
            trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)
            main_score = main_asset_score(snapshot)
            main_score_value = main_score.total_score if main_score else None

        item = TelegramSignalDigestItem(
            created_at=time.time(),
            symbol=signal.symbol,
            kind=signal.kind,
            priority=priority,
            quality_score=quality_score,
            trap_score=trap_score,
            main_asset_score=main_score_value,
            signal_score=signal.score,
            strength_score=signal_strength_score(signal),
            reason=compact_digest_reason(signal.message),
        )
        with self.telegram_signal_digest_lock:
            self.telegram_signal_digest_queue.append(item)

    def flush_telegram_signal_digest_if_due(self) -> None:
        _realtime_priorities, digest_priorities, digest_interval_minutes, digest_max_per_priority = (
            self.telegram_signal_filter_settings()
        )
        now = time.time()
        if now - self.last_telegram_signal_digest_at < digest_interval_minutes * 60:
            return
        self.last_telegram_signal_digest_at = now

        with self.telegram_signal_digest_lock:
            items = self.telegram_signal_digest_queue
            self.telegram_signal_digest_queue = []
        digest_priority_set = set(digest_priorities)
        digest_items = [item for item in items if item.priority in digest_priority_set]
        if not digest_items:
            return

        notifications = self.config.get("notifications", {})
        telegram = notifications.get("telegram", {})
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        if not bot_token or not chat_ids:
            return

        message = format_telegram_signal_digest(
            digest_items,
            digest_priorities,
            digest_interval_minutes,
            digest_max_per_priority,
        )
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        for chat_id in split_chat_ids(chat_ids):
            try:
                response = self.session.post(url, json={"chat_id": chat_id, "text": message}, timeout=5)
                response.raise_for_status()
            except Exception:
                logging.exception("Failed to send Telegram signal digest to chat_id=%s", chat_id)

    def notify_status(self, title: str, message: str) -> None:
        notifications = self.config.get("notifications", {})
        webhook_url = notifications.get("webhook_url")
        telegram = notifications.get("telegram", {})
        payload = {
            "title": title,
            "message": message,
            "kind": "status",
            "time": dt.datetime.now(dt.UTC).isoformat(),
        }
        if webhook_url:
            self.post_json(webhook_url, payload)
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        if bot_token and chat_ids:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            for chat_id in split_chat_ids(chat_ids):
                self.post_json(url, {"chat_id": chat_id, "text": f"{title}\n{message}"})

    def send_summary_if_due(self) -> None:
        if not self.summary_config.get("enabled", False):
            return
        interval = int(self.summary_config.get("interval_seconds", 3600))
        now = time.time()
        if now - self.last_summary_at < interval:
            return
        if not self.latest_snapshots:
            return

        self.last_summary_at = now
        message = format_summary_for_telegram(
            list(self.latest_snapshots.values()),
            int(self.summary_config.get("top_n", 5)),
        )
        self.notify_status("Crypto monitor hourly summary", message)

    def start_telegram_command_worker(self) -> None:
        if self.telegram_command_thread_started:
            return
        if not self.telegram_commands_config.get("enabled", False):
            return
        self.telegram_command_thread_started = True
        thread = threading.Thread(target=self.telegram_command_loop, daemon=True)
        thread.start()
        logging.info("Telegram command worker started")

    def telegram_command_loop(self) -> None:
        interval = int(self.telegram_commands_config.get("poll_interval_seconds", 5))
        while True:
            try:
                self.handle_telegram_commands()
            except Exception:
                logging.exception("Telegram command polling failed")
            time.sleep(max(2, interval))

    def handle_telegram_commands(self) -> None:
        telegram = self.config.get("notifications", {}).get("telegram", {})
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        if not bot_token or not chat_ids:
            return

        params = {"timeout": 5}
        if self.telegram_update_offset is not None:
            params["offset"] = self.telegram_update_offset

        response = self.session.get(f"https://api.telegram.org/bot{bot_token}/getUpdates", params=params, timeout=10)
        response.raise_for_status()

        allowed_chat_ids = set(split_chat_ids(chat_ids))
        for update in response.json().get("result", []):
            update_id = update.get("update_id")
            if update_id is not None:
                self.telegram_update_offset = int(update_id) + 1

            message = update.get("message") or update.get("edited_message") or {}
            chat = message.get("chat") or {}
            chat_id = str(chat.get("id", ""))
            if chat_id not in allowed_chat_ids:
                continue

            text = str(message.get("text", "")).strip()
            if not text:
                continue
            self.handle_telegram_command_text(bot_token, chat_id, text)

        self.save_telegram_update_offset()

    def handle_telegram_command_text(self, bot_token: str, chat_id: str, text: str) -> None:
        parts = text.split()
        command = parts[0].split("@", 1)[0].lower()

        if command in ("/help", "/start"):
            self.send_telegram_text(
                bot_token,
                chat_id,
                telegram_help_text(),
            )
            return

        if command == "/ask":
            self.handle_ask_command(bot_token, chat_id, parts[1:])
            return

        if command == "/liq":
            self.handle_liq_command(bot_token, chat_id, parts[1:])
            return

        if command == "/summary":
            self.handle_summary_command(bot_token, chat_id)
            return

        if command == "/hot":
            self.handle_hot_command(bot_token, chat_id)
            return

        if command == "/signals":
            self.handle_signals_command(bot_token, chat_id)
            return

        if command == "/quality":
            self.handle_quality_command(bot_token, chat_id)
            return

        if command == "/digest":
            self.handle_digest_command(bot_token, chat_id, parts[1:])
            return

        if command == "/topq":
            self.handle_topq_command(bot_token, chat_id)
            return

        if command == "/quiet":
            self.handle_quiet_command(bot_token, chat_id, parts[1:])
            return

        if command == "/why":
            self.handle_why_command(bot_token, chat_id, parts[1:])
            return

        if command == "/top":
            self.handle_top_command(bot_token, chat_id)
            return
        if command == "/review":
            self.handle_review_command(bot_token, chat_id)
            return
        if command == "/perf":
            self.handle_perf_command(bot_token, chat_id)
            return
        if command == "/regime":
            self.handle_regime_command(bot_token, chat_id)
            return
        if command == "/sectors":
            self.handle_sectors_command(bot_token, chat_id)
            return

        if command == "/dev":
            self.handle_dev_command(bot_token, chat_id, parts[1:])
            return

        if command not in ("/symbol", "/check"):
            return

        if len(parts) < 2:
            self.send_telegram_text(bot_token, chat_id, "用法: /symbol SIGNUSDT")
            return

        symbol = parts[1].upper()
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"

        try:
            snapshot, data_source_text, degradation_text = self.telegram_command_snapshot(symbol)
        except Exception:
            logging.exception("Failed to fetch Telegram command snapshot")
            self.send_telegram_text(bot_token, chat_id, "Binance接口限流或异常，请稍后再试。")
            return

        try:
            signals = self.evaluate_snapshot(snapshot, {"mode": "both"})
            combined_signal = self.combined_signal(snapshot, signals)
            if combined_signal:
                signals.append(combined_signal)
            liquidation_text = self.format_liquidation_stats(symbol)
            coinglass_text = self.format_coinglass_market_context(symbol)
            response_parts = [data_source_text]
            if degradation_text:
                response_parts.append(degradation_text)
            response_parts.append(format_symbol_diagnosis(snapshot, signals, liquidation_text, coinglass_text))
            self.send_telegram_text(bot_token, chat_id, "\n".join(response_parts))
        except Exception as exc:
            logging.exception("Failed to diagnose symbol from Telegram command")
            self.send_telegram_text(bot_token, chat_id, f"{symbol} 查询失败: {type(exc).__name__}: {exc}")

    def handle_quality_command(self, bot_token: str, chat_id: str) -> None:
        self.send_telegram_text(bot_token, chat_id, self.format_signal_quality_stats())

    def handle_digest_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        if not args or args[0].lower() != "now":
            return
        self.send_telegram_text(bot_token, chat_id, self.format_current_telegram_signal_digest())

    def format_current_telegram_signal_digest(self) -> str:
        _realtime_priorities, digest_priorities, digest_interval_minutes, digest_max_per_priority = (
            self.telegram_signal_filter_settings()
        )
        with self.telegram_signal_digest_lock:
            items = list(self.telegram_signal_digest_queue)

        digest_priority_set = set(digest_priorities)
        digest_items = [item for item in items if item.priority in digest_priority_set]
        if not digest_items:
            return "当前暂无静默信号。"

        return format_telegram_signal_digest(
            digest_items,
            digest_priorities,
            digest_interval_minutes,
            digest_max_per_priority,
            title="当前静默信号摘要预览",
        )

    def format_signal_quality_stats(self) -> str:
        with self.signal_quality_stats_lock:
            stats = {
                "total": self.signal_quality_stats["total"],
                "realtime_sent": self.signal_quality_stats["realtime_sent"],
                "suppressed": self.signal_quality_stats["suppressed"],
                "by_priority": dict(self.signal_quality_stats["by_priority"]),
                "by_kind": dict(self.signal_quality_stats["by_kind"]),
                "by_symbol": dict(self.signal_quality_stats["by_symbol"]),
                "suppressed_by_priority": dict(self.signal_quality_stats["suppressed_by_priority"]),
            }

        priorities = ("S", "A", "B", "C", "D")
        priority_text = " ".join(f"{priority}:{stats['by_priority'].get(priority, 0)}" for priority in priorities)
        suppressed_text = " ".join(
            f"{priority}:{stats['suppressed_by_priority'].get(priority, 0)}" for priority in priorities
        )
        top_kinds = format_top_quality_counts(stats["by_kind"])
        top_symbols = format_top_quality_counts(stats["by_symbol"])
        return "\n".join(
            [
                "信号质量统计（本次服务启动后）",
                f"总信号: {stats['total']}",
                f"实时推送: {stats['realtime_sent']}",
                f"静默: {stats['suppressed']}",
                f"等级分布: {priority_text}",
                f"静默分布: {suppressed_text}",
                f"Top kinds: {top_kinds}",
                f"Top symbols: {top_symbols}",
            ]
        )

    def handle_quiet_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        if not args:
            self.send_telegram_text(bot_token, chat_id, "用法: /quiet status|normal|strict|ultra")
            return

        mode = args[0].lower()
        if mode == "status":
            self.send_telegram_text(bot_token, chat_id, self.format_quiet_status())
            return
        if mode == "normal":
            with self.runtime_realtime_priorities_lock:
                self.runtime_realtime_priorities_override = None
            self.send_telegram_text(bot_token, chat_id, self.format_quiet_status())
            return
        if mode == "strict":
            with self.runtime_realtime_priorities_lock:
                self.runtime_realtime_priorities_override = {"S", "A"}
            self.send_telegram_text(bot_token, chat_id, self.format_quiet_status())
            return
        if mode == "ultra":
            with self.runtime_realtime_priorities_lock:
                self.runtime_realtime_priorities_override = {"S"}
            self.send_telegram_text(bot_token, chat_id, self.format_quiet_status())
            return

        self.send_telegram_text(bot_token, chat_id, "用法: /quiet status|normal|strict|ultra")

    def format_quiet_status(self) -> str:
        realtime_priorities, digest_priorities, _digest_interval_minutes, _digest_max_per_priority = (
            self.telegram_signal_filter_settings()
        )
        _current_realtime, override_enabled = self.runtime_realtime_priorities_status()
        return "\n".join(
            [
                "Telegram 临时静音等级",
                f"实时等级: {format_priority_set(realtime_priorities)}",
                f"摘要等级: {format_priority_set(set(digest_priorities))}",
                f"override: {'on' if override_enabled else 'off'}",
            ]
        )

    def handle_why_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        if not args:
            self.send_telegram_text(bot_token, chat_id, "用法: /why SYMBOL")
            return

        symbol = normalize_usdt_symbol(args[0])
        try:
            message = self.format_why_symbol(symbol)
        except Exception:
            logging.exception("Failed to build /why response for %s", symbol)
            self.send_telegram_text(bot_token, chat_id, f"{symbol} 查询失败，请查看服务日志。")
            return
        self.send_telegram_text(bot_token, chat_id, message)

    def format_why_symbol(self, symbol: str) -> str:
        snapshot = self.latest_snapshots.get(symbol)
        if snapshot is None:
            snapshot = self.fetch_snapshot(symbol)

        try:
            recent_rows = self.load_recent_symbol_signal_rows(symbol, 5, 300)
        except Exception:
            logging.exception("Failed to read recent symbol rows for /why: %s", symbol)
            recent_rows = []

        trap_score, trap_label, _trap_reason = trap_risk_score(snapshot, None)
        main_score = main_asset_score(snapshot)
        lines = [
            f"WHY {symbol}",
            "当前:",
            f"- 短线评分 {short_term_score(snapshot)}/10，中线评分 {mid_term_score(snapshot)}/10",
            f"- 资金流共振 {flow_alignment_score(snapshot)}/10，长周期 {long_flow_alignment_score(snapshot)}/9",
            f"- 诱多/诱空风险 {trap_score}/10 {trap_label}",
        ]
        if main_score:
            lines.append(f"- 主流评分 {main_score.total_score}/100")
        coinglass_text = self.cached_coinglass_judgement(symbol)
        if coinglass_text:
            lines.append(f"- CoinGlass判断 {coinglass_text}")

        lines.append("最近信号:")
        if recent_rows:
            for row in recent_rows:
                lines.append(f"- {format_why_signal_row(row)}")
        else:
            lines.append("- 最近暂无该币信号。")

        lines.append("结论:")
        lines.append(f"- {why_symbol_conclusion(recent_rows)}")
        return truncate_text("\n".join(lines), 3500)

    def cached_coinglass_judgement(self, symbol: str) -> str:
        base = symbol[:-4] if symbol.endswith("USDT") else symbol
        cache_key = f"{base}:{symbol}"
        cached = self.coinglass_market_context_cache.get(cache_key)
        if not cached:
            return ""
        cached_at, text = cached
        if time.time() - cached_at > COINGLASS_MARKET_CONTEXT_CACHE_TTL_SECONDS:
            return ""
        return extract_coinglass_judgement(text)

    def handle_ask_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        if not args:
            self.send_telegram_text(bot_token, chat_id, "用法: /ask KNC 或 /ask KNCUSDT full")
            return

        full_mode = any(arg.lower() in ("full", "--full") for arg in args[1:])
        symbol = args[0].upper()
        if not symbol.endswith("USDT"):
            symbol = f"{symbol}USDT"

        try:
            snapshot, data_source_text, degradation_text = self.telegram_command_snapshot(symbol)
        except Exception:
            logging.exception("Failed to fetch Telegram ask snapshot")
            self.send_telegram_text(bot_token, chat_id, "Binance接口限流或异常，请稍后再试。")
            return

        try:
            signals = self.evaluate_snapshot(snapshot, {"mode": "both"})
            combined_signal = self.combined_signal(snapshot, signals)
            display_signals = ([combined_signal] if combined_signal else []) + signals
            market_snapshots = self.ask_market_snapshots()
            recent_rows = self.load_recent_symbol_signal_rows(symbol, 3)
            liquidation_text = self.format_liquidation_stats(symbol)
            coinglass_text = self.format_coinglass_market_context(symbol)
            ask_data_source_text = format_ask_data_source_text(data_source_text, full=full_mode)
            context_text = format_ask_context(
                snapshot,
                display_signals,
                market_snapshots,
                recent_rows,
                liquidation_text,
                coinglass_text,
            )
            context_text = "\n".join([ask_data_source_text, context_text])
            if degradation_text:
                context_text = "\n".join([degradation_text, context_text])
            ai_review = ask_ai_review(context_text)
            response_parts = [ask_data_source_text]
            stale_cache_note = ask_stale_cache_note(data_source_text)
            if stale_cache_note:
                response_parts.append(stale_cache_note)
            if degradation_text:
                response_parts.append(degradation_text)
            response_parts.append(
                format_ask_response(
                    context_text,
                    ai_review,
                    snapshot,
                    display_signals,
                    market_snapshots,
                    liquidation_text,
                    coinglass_text,
                    full=full_mode,
                )
            )
            self.send_telegram_text(bot_token, chat_id, "\n".join(response_parts))
        except Exception as exc:
            logging.exception("Failed to build ask context from Telegram command")
            self.send_telegram_text(bot_token, chat_id, f"{symbol} /ask 查询失败: {type(exc).__name__}: {exc}")

    def handle_liq_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        symbol = None
        if args:
            symbol = args[0].upper()
            if not symbol.endswith("USDT"):
                symbol = f"{symbol}USDT"
        self.send_telegram_text(bot_token, chat_id, self.liquidation_health_report(symbol))

    def ask_market_snapshots(self) -> list[MarketSnapshot]:
        majors = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
        snapshots = [self.latest_snapshots[symbol] for symbol in majors if symbol in self.latest_snapshots]
        if len(snapshots) == len(majors):
            return snapshots

        by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
        for symbol in majors:
            if symbol in by_symbol:
                continue
            try:
                by_symbol[symbol] = self.fetch_snapshot(symbol)
            except Exception:
                logging.debug("Failed to fetch ask market snapshot: %s", symbol, exc_info=True)
        return [by_symbol[symbol] for symbol in majors if symbol in by_symbol]

    def telegram_command_snapshot(self, symbol: str) -> tuple[MarketSnapshot, str, str | None]:
        now = time.time()
        cached_snapshot = self.latest_snapshots.get(symbol)
        cache_age_seconds = self.snapshot_cache_age_seconds(now)
        if cached_snapshot and cache_age_seconds <= TELEGRAM_SNAPSHOT_CACHE_TTL_SECONDS:
            return cached_snapshot, f"数据来源: 缓存 {cache_age_seconds}秒前", None

        try:
            return self.fetch_snapshot(symbol), "数据来源: 实时", None
        except Exception:
            if not cached_snapshot:
                raise
            logging.warning("Live snapshot fetch failed for %s; using cached Telegram command snapshot", symbol, exc_info=True)
            return (
                cached_snapshot,
                f"数据来源: 缓存 {cache_age_seconds}秒前",
                f"数据缓存: {cache_age_seconds}秒前，实时接口失败，已降级使用缓存。",
            )

    def snapshot_cache_age_seconds(self, now: float | None = None) -> int:
        if now is None:
            now = time.time()
        if self.latest_snapshots_updated_at <= 0:
            return 0
        return max(0, int(now - self.latest_snapshots_updated_at))

    def load_recent_symbol_signal_rows(self, symbol: str, limit: int = 3, scan_limit: int = 1000) -> list[dict[str, str]]:
        rows = self.load_recent_signal_rows(scan_limit)
        return [row for row in rows if row.get("symbol", "").upper() == symbol][:limit]

    def handle_dev_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        if not args or args[0].lower() == "help":
            self.send_telegram_text(bot_token, chat_id, dev_help_text())
            return

        subcommand = args[0].lower()
        try:
            if subcommand == "status":
                ok, output = run_dev_command(["sudo", "-n", "systemctl", "status", "crypto-monitor", "--no-pager"], timeout=10)
                if not ok:
                    self.send_telegram_text(bot_token, chat_id, f"状态查询失败: {output}")
                    return
                self.send_telegram_text(bot_token, chat_id, format_systemctl_status(output))
                return

            if subcommand == "logs":
                ok, output = run_dev_command(["sudo", "-n", "journalctl", "-u", "crypto-monitor", "-n", "30", "--no-pager"], timeout=10)
                if not ok:
                    self.send_telegram_text(bot_token, chat_id, f"日志查询失败: {output}")
                    return
                self.send_telegram_text(bot_token, chat_id, truncate_text(f"最近日志:\n{output}", 3500))
                return

            if subcommand == "git":
                status_ok, status_output = run_dev_command(["git", "status", "--short"], timeout=10)
                log_ok, log_output = run_dev_command(["git", "log", "--oneline", "--max-count=3"], timeout=10)
                if not status_ok:
                    self.send_telegram_text(bot_token, chat_id, f"Git 状态失败: {status_output}")
                    return
                if not log_ok:
                    self.send_telegram_text(bot_token, chat_id, f"Git 日志失败: {log_output}")
                    return
                status_text = status_output.strip() or "工作区干净"
                self.send_telegram_text(bot_token, chat_id, truncate_text(f"Git 状态:\n{status_text}\n\n最近提交:\n{log_output}", 3500))
                return

            if subcommand == "backtest":
                ok, output = run_dev_command(
                    [
                        "/opt/crypto-monitor/.venv/bin/python",
                        "/opt/crypto-monitor/backtest_signals.py",
                        "-c",
                        "/opt/crypto-monitor/derivatives_config.yaml",
                        "--limit",
                        "80",
                    ],
                    timeout=60,
                )
                if not ok:
                    self.send_telegram_text(bot_token, chat_id, f"回测失败: {output}")
                    return
                if backtest_long_flow_sample_warning(output):
                    output = f"{output}\n提醒: 长周期资金分组样本仍少，暂不建议据此改推送规则。"
                self.send_telegram_text(bot_token, chat_id, truncate_text(f"回测摘要:\n{output}", 3500))
                return

            if subcommand == "restart":
                code = f"{secrets.randbelow(1_000_000):06d}"
                self.pending_dev_confirmations[chat_id] = ("restart", code, time.time() + 120)
                self.send_telegram_text(bot_token, chat_id, f"确认重启请在 2 分钟内发送:\n/dev confirm restart {code}")
                return

            if subcommand == "deploy":
                code = f"{secrets.randbelow(1_000_000):06d}"
                self.pending_dev_confirmations[chat_id] = ("deploy", code, time.time() + 120)
                self.send_telegram_text(bot_token, chat_id, f"确认部署请在 2 分钟内发送:\n/dev confirm deploy {code}")
                return

            if subcommand == "confirm" and len(args) >= 3 and args[1].lower() == "restart":
                self.handle_dev_restart_confirmation(bot_token, chat_id, args[2])
                return

            if subcommand == "confirm" and len(args) >= 3 and args[1].lower() == "deploy":
                self.handle_dev_deploy_confirmation(bot_token, chat_id, args[2])
                return

            self.send_telegram_text(bot_token, chat_id, "未知 /dev 命令。发送 /dev help 查看用法。")
        except Exception:
            logging.exception("Failed to handle Telegram dev command")
            self.send_telegram_text(bot_token, chat_id, "DevOps 命令执行失败，请查看服务日志。")

    def handle_dev_restart_confirmation(self, bot_token: str, chat_id: str, code: str) -> None:
        pending = self.get_pending_dev_confirmation(chat_id, "restart")
        if pending is None:
            self.send_telegram_text(bot_token, chat_id, "没有待确认的重启请求。")
            return

        expected_code = pending
        if code != expected_code:
            self.send_telegram_text(bot_token, chat_id, "确认码错误，未执行重启。")
            return

        self.pending_dev_confirmations.pop(chat_id, None)
        self.save_pending_dev_restart_notification(chat_id)
        self.send_telegram_text(bot_token, chat_id, "确认通过，正在重启服务...")
        ok, output = run_dev_command(["sudo", "-n", "systemctl", "restart", "crypto-monitor"], timeout=20)
        if not ok:
            self.clear_pending_dev_restart_notification()
            self.send_telegram_text(bot_token, chat_id, f"重启失败: {output}")
            return

        self.clear_pending_dev_restart_notification()
        status_ok, status_output = run_dev_command(["sudo", "-n", "systemctl", "status", "crypto-monitor", "--no-pager"], timeout=10)
        if not status_ok:
            self.send_telegram_text(bot_token, chat_id, f"已执行重启，但状态查询失败: {status_output}")
            return

        self.send_telegram_text(bot_token, chat_id, "重启完成。\n" + format_systemctl_status(status_output))

    def handle_dev_deploy_confirmation(self, bot_token: str, chat_id: str, code: str) -> None:
        pending = self.get_pending_dev_confirmation(chat_id, "deploy")
        if pending is None:
            self.send_telegram_text(bot_token, chat_id, "没有待确认的部署请求。")
            return

        expected_code = pending
        if code != expected_code:
            self.send_telegram_text(bot_token, chat_id, "确认码错误，未执行部署。")
            return

        self.pending_dev_confirmations.pop(chat_id, None)
        self.send_telegram_text(bot_token, chat_id, "确认通过，开始部署...")

        status_ok, status_output = run_dev_command(["git", "status", "--short"], timeout=10)
        if not status_ok:
            self.send_telegram_text(bot_token, chat_id, f"部署拒绝，Git 状态检查失败: {status_output}")
            return
        if status_output.strip():
            self.send_telegram_text(bot_token, chat_id, truncate_text(f"部署拒绝，工作区存在未提交改动:\n{status_output}", 3500))
            return

        pull_ok, pull_output = run_dev_command(["git", "pull", "--ff-only", "origin", "main"], timeout=60)
        if not pull_ok:
            self.send_telegram_text(bot_token, chat_id, truncate_text(f"部署失败，git pull --ff-only origin main 未通过:\n{pull_output}", 3500))
            return

        compile_ok, compile_summary = run_dev_compile_checks()
        if not compile_ok:
            self.send_telegram_text(
                bot_token,
                chat_id,
                truncate_text(f"部署失败，编译检查未通过。\n\nPull 输出:\n{pull_output or '(无输出)'}\n\n编译结果:\n{compile_summary}", 3500),
            )
            return

        self.save_pending_dev_deploy_notification(chat_id, pull_output, compile_summary)
        restart_ok, restart_output = run_dev_command(["sudo", "-n", "systemctl", "restart", "crypto-monitor"], timeout=20)
        if not restart_ok:
            self.clear_pending_dev_restart_notification()
            self.send_telegram_text(
                bot_token,
                chat_id,
                truncate_text(
                    f"部署失败，服务重启未通过。\n\nPull 输出:\n{pull_output or '(无输出)'}\n\n编译结果:\n{compile_summary}\n\n重启输出:\n{restart_output}",
                    3500,
                ),
            )
            return

        status_ok, status_output = run_dev_command(["sudo", "-n", "systemctl", "status", "crypto-monitor", "--no-pager"], timeout=10)
        if not status_ok:
            self.send_telegram_text(
                bot_token,
                chat_id,
                truncate_text(
                    f"部署已执行，但状态查询失败: {status_output}\n\nPull 输出:\n{pull_output or '(无输出)'}\n\n编译结果:\n{compile_summary}",
                    3500,
                ),
            )
            return

        self.clear_pending_dev_restart_notification()
        self.send_telegram_text(
            bot_token,
            chat_id,
            format_dev_deploy_summary(pull_output, compile_summary, format_systemctl_status(status_output)),
        )

    def get_pending_dev_confirmation(self, chat_id: str, action: str) -> str | None:
        pending = self.pending_dev_confirmations.get(chat_id)
        now = time.time()
        if not pending:
            return None

        pending_action, expected_code, expires_at = pending
        if now > expires_at:
            self.pending_dev_confirmations.pop(chat_id, None)
            return None

        if pending_action != action:
            return None

        return expected_code

    def dev_restart_notification_path(self) -> Path:
        return Path("/opt/crypto-monitor/dev_restart_notification.json")

    def save_pending_dev_restart_notification(self, chat_id: str) -> None:
        payload = {"type": "restart", "chat_id": chat_id, "created_at": time.time()}
        self.dev_restart_notification_path().write_text(json.dumps(payload), encoding="utf-8")

    def save_pending_dev_deploy_notification(self, chat_id: str, pull_output: str, compile_summary: str) -> None:
        payload = {
            "type": "deploy",
            "chat_id": chat_id,
            "created_at": time.time(),
            "pull_output": pull_output,
            "compile_summary": compile_summary,
        }
        self.dev_restart_notification_path().write_text(json.dumps(payload), encoding="utf-8")

    def clear_pending_dev_restart_notification(self) -> None:
        try:
            self.dev_restart_notification_path().unlink()
        except FileNotFoundError:
            pass
        except Exception:
            logging.warning("Failed to clear pending dev restart notification", exc_info=True)

    def send_pending_dev_restart_status(self) -> None:
        path = self.dev_restart_notification_path()
        if not path.exists():
            return

        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            notification_type = str(payload.get("type", "restart"))
            chat_id = str(payload.get("chat_id", ""))
            created_at = float(payload.get("created_at", 0))
        except Exception:
            logging.warning("Failed to read pending dev restart notification", exc_info=True)
            self.clear_pending_dev_restart_notification()
            return

        self.clear_pending_dev_restart_notification()
        if time.time() - created_at > 300:
            return

        telegram = self.config.get("notifications", {}).get("telegram", {})
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        if not bot_token or chat_id not in set(split_chat_ids(chat_ids or "")):
            return

        ok, output = run_dev_command(["sudo", "-n", "systemctl", "status", "crypto-monitor", "--no-pager"], timeout=10)
        if not ok:
            if notification_type == "deploy":
                self.send_telegram_text(bot_token, chat_id, f"部署后状态查询失败: {output}")
            else:
                self.send_telegram_text(bot_token, chat_id, f"重启后状态查询失败: {output}")
            return

        if notification_type == "deploy":
            pull_output = str(payload.get("pull_output", ""))
            compile_summary = str(payload.get("compile_summary", ""))
            self.send_telegram_text(bot_token, chat_id, format_dev_deploy_summary(pull_output, compile_summary, format_systemctl_status(output)))
            return

        self.send_telegram_text(bot_token, chat_id, "重启完成。\n" + format_systemctl_status(output))

    def handle_regime_command(self, bot_token: str, chat_id: str) -> None:
        self.send_telegram_text(bot_token, chat_id, "正在生成市场大方向，请稍等...")
        snapshots = []
        symbols = list(self.symbol_configs)
        if not symbols:
            self.refresh_symbols_if_due(force=True)
            symbols = list(self.symbol_configs)

        for symbol in symbols:
            try:
                snapshots.append(self.fetch_snapshot(symbol))
            except Exception:
                logging.debug("Failed to fetch snapshot for regime command: %s", symbol, exc_info=True)

        if not snapshots:
            self.send_telegram_text(bot_token, chat_id, "暂无可用市场数据。")
            return

        self.send_telegram_text(bot_token, chat_id, format_regime_for_telegram(snapshots))

    def handle_sectors_command(self, bot_token: str, chat_id: str) -> None:
        self.send_telegram_text(bot_token, chat_id, "正在生成板块热度，请稍等...")
        snapshots = []
        symbols = list(self.symbol_configs)
        if not symbols:
            self.refresh_symbols_if_due(force=True)
            symbols = list(self.symbol_configs)

        for symbol in symbols:
            try:
                snapshots.append(self.fetch_snapshot(symbol))
            except Exception:
                logging.debug("Failed to fetch snapshot for sectors command: %s", symbol, exc_info=True)

        if not snapshots:
            self.send_telegram_text(bot_token, chat_id, "暂无可用市场数据。")
            return

        self.send_telegram_text(bot_token, chat_id, format_sectors_for_telegram(snapshots, detail=True))

    def handle_summary_command(self, bot_token: str, chat_id: str) -> None:
        self.send_telegram_text(bot_token, chat_id, "正在生成市场摘要，请稍等...")
        snapshots = []
        symbols = list(self.symbol_configs)
        if not symbols:
            self.refresh_symbols_if_due(force=True)
            symbols = list(self.symbol_configs)

        for symbol in symbols:
            try:
                snapshots.append(self.fetch_snapshot(symbol))
            except Exception:
                logging.debug("Failed to fetch snapshot for summary command: %s", symbol, exc_info=True)

        if not snapshots:
            self.send_telegram_text(bot_token, chat_id, "暂无可用市场数据。")
            return

        top_n = int(self.summary_config.get("top_n", 5))
        self.send_telegram_text(bot_token, chat_id, format_summary_for_telegram(snapshots, top_n))

    def handle_hot_command(self, bot_token: str, chat_id: str) -> None:
        self.send_telegram_text(bot_token, chat_id, "正在生成强势过热候选，请稍等...")
        snapshots = []
        symbols = list(self.symbol_configs)
        if not symbols:
            self.refresh_symbols_if_due(force=True)
            symbols = list(self.symbol_configs)

        for symbol in symbols:
            try:
                snapshots.append(self.fetch_snapshot(symbol))
            except Exception:
                logging.debug("Failed to fetch snapshot for hot command: %s", symbol, exc_info=True)

        if not snapshots:
            self.send_telegram_text(bot_token, chat_id, "暂无可用市场数据。")
            return

        self.send_telegram_text(bot_token, chat_id, format_hot_watch_for_telegram(snapshots, 10))

    def handle_signals_command(self, bot_token: str, chat_id: str) -> None:
        path = Path(self.signal_log_path)
        if not path.exists():
            self.send_telegram_text(bot_token, chat_id, "暂无信号记录。")
            return

        try:
            with path.open("r", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
        except Exception as exc:
            self.send_telegram_text(bot_token, chat_id, f"读取信号记录失败: {type(exc).__name__}: {exc}")
            return

        if not rows:
            self.send_telegram_text(bot_token, chat_id, "暂无信号记录。")
            return

        lines = ["最近信号:"]
        for row in rows[-10:][::-1]:
            strength = row.get("strength_score") or "-"
            lines.append(
                f"{row.get('symbol', '-')}: {row.get('kind', '-')} "
                f"score={row.get('score', '-')} 强度={strength} "
                f"价格={format_csv_number(row.get('price_change_percent'))}% "
                f"OI={format_csv_number(row.get('oi_change_percent'))}%"
            )
        self.send_telegram_text(bot_token, chat_id, "\n".join(lines))

    def load_recent_signal_rows(self, limit: int = 10) -> list[dict[str, str]]:
        path = Path(self.signal_log_path)
        if not path.exists():
            return []
        with path.open("r", encoding="utf-8", newline="") as file:
            rows = list(csv.DictReader(file))
        return rows[-limit:][::-1]

    def handle_review_command(self, bot_token: str, chat_id: str) -> None:
        try:
            rows = self.load_recent_signal_rows(10)
            if not rows:
                self.send_telegram_text(bot_token, chat_id, "最近暂无信号记录")
                return

            lines = ["最近10条信号:"]
            for row in rows:
                time_text = row.get("time", "-").replace("T", " ")[:19]
                symbol = row.get("symbol", "-")
                kind = row.get("kind", "-")
                strength = format_csv_strength(row.get("strength_score"))
                price_change = format_csv_number(row.get("price_change_percent"))
                oi_change = format_csv_number(row.get("oi_change_percent"))
                flow_15m = review_float(row, "net_flow_15m_usd", 15) or 0
                position = review_float(row, "price_position_24h", None)
                看空_score = review_float(row, "short_term_score", None)
                mid_score = review_float(row, "mid_term_score", None)

                extra = ""
                if position is not None:
                    extra += f" pos24h={position:.1f}%"
                if 看空_score is not None and mid_score is not None:
                    extra += f" S/M={看空_score:.0f}/{mid_score:.0f}"

                lines.append(
                    f"{time_text} {symbol} {kind} 强度={strength} 价格={price_change}% OI={oi_change}% flow15m={format_usd(flow_15m)}{extra}"
                )

            self.send_telegram_text(bot_token, chat_id, "\n".join(lines))
        except Exception:
            logging.exception("Failed to handle review command")
            self.send_telegram_text(bot_token, chat_id, "读取最近信号失败，请查看服务日志。")


    def handle_perf_command(self, bot_token: str, chat_id: str) -> None:
        try:
            rows = self.load_recent_signal_rows(10)
            if not rows:
                self.send_telegram_text(bot_token, chat_id, "最近暂无信号记录")
                return

            lines = ["最近信号表现:"]
            hit_count = 0
            checked = 0
            for row in rows:
                symbol = row.get("symbol", "-")
                kind = row.get("kind", "-")
                signal_price = review_float(row, "price", 6)
                if not symbol or signal_price is None or signal_price <= 0:
                    continue

                current_price = self.fetch_last_price(symbol)
                change = percent_change(signal_price, current_price)

                hit = False
                if kind in ("discovery", "hot_breakout"):
                    hit = change > 0
                elif kind in ("top_risk", "distribution"):
                    hit = change < 0

                checked += 1
                if hit:
                    hit_count += 1

                mark = "命中" if hit else "观察"
                lines.append(f"{symbol} {kind} {change:+.2f}% {mark}")

            if checked:
                lines.append("")
                lines.append(f"最近命中: {hit_count}/{checked} ({hit_count / checked * 100:.1f}%)")
            self.send_telegram_text(bot_token, chat_id, "\n".join(lines))
        except Exception:
            logging.exception("Failed to handle perf command")
            self.send_telegram_text(bot_token, chat_id, "读取信号表现失败，请查看服务日志。")

    def fetch_last_price(self, symbol: str) -> float:
        ticker = self.get("/fapi/v1/ticker/price", {"symbol": symbol})
        return float(ticker["price"])

    def handle_top_command(self, bot_token: str, chat_id: str) -> None:
        path = Path(self.signal_log_path)
        if not path.exists():
            self.send_telegram_text(bot_token, chat_id, "暂无信号记录。")
            return

        try:
            with path.open("r", encoding="utf-8") as file:
                rows = list(csv.DictReader(file))
        except Exception as exc:
            self.send_telegram_text(bot_token, chat_id, f"读取信号记录失败: {type(exc).__name__}: {exc}")
            return

        rows = [row for row in rows if row.get("strength_score")]
        if not rows:
            self.send_telegram_text(bot_token, chat_id, "暂无带强度分的信号记录。")
            return

        rows.sort(key=lambda row: float(row.get("strength_score") or 0), reverse=True)

        lines = ["强度最高信号:"]
        for row in rows[:10]:
            lines.append(
                f"{row.get('symbol', '-')}: {row.get('kind', '-')} "
                f"强度={format_csv_strength(row.get('strength_score'))} ({strength_grade_from_csv(row.get('strength_score'))}) "
                f"score={row.get('score', '-')} "
                f"价格={format_csv_number(row.get('price_change_percent'))}% "
                f"OI={format_csv_number(row.get('oi_change_percent'))}%"
            )
        self.send_telegram_text(bot_token, chat_id, "\n".join(lines))

    def handle_topq_command(self, bot_token: str, chat_id: str) -> None:
        try:
            rows = self.load_recent_signal_rows(200)
        except Exception:
            logging.exception("Failed to read quality signal rows")
            self.send_telegram_text(bot_token, chat_id, "读取信号记录失败，请查看服务日志。")
            return

        scored_rows = []
        for row in rows:
            quality_score = parse_float(row.get("signal_quality_score"))
            if quality_score is None:
                continue
            scored_rows.append((quality_score, row))

        if not scored_rows:
            self.send_telegram_text(bot_token, chat_id, "暂无质量评分信号，请等待新信号积累。")
            return

        scored_rows.sort(key=lambda item: item[0], reverse=True)
        lines = ["高质量信号TOP10"]
        for index, (quality_score, row) in enumerate(scored_rows[:10], start=1):
            kind = row.get("kind", "-")
            direction = signal_direction_label(kind)
            priority = row.get("signal_priority") or "-"
            trap_score = format_csv_compact_number(row.get("trap_risk_score"), signed=False)
            price_change = format_csv_compact_number(row.get("price_change_percent"), signed=True)
            oi_change = format_csv_compact_number(row.get("oi_change_percent"), signed=True)
            strength = format_csv_compact_number(row.get("strength_score"), signed=False)
            suppressed = format_suppressed_status(row.get("suppressed_from_telegram"))
            reason = format_quality_reason_short(row.get("signal_quality_reason", "") or "")
            lines.append(
                f"{index}. {row.get('symbol', '-')} {direction} {kind} "
                f"{priority}/{quality_score:.0f} "
                f"trap{trap_score} {price_change}% OI{oi_change}%"
            )
            lines.append(
                f"   强度{strength} | {suppressed} | {reason}"
            )
        self.send_telegram_text(bot_token, chat_id, "\n".join(lines))

    def send_telegram_text(self, bot_token: str, chat_id: str, text: str) -> None:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self.post_json(url, {"chat_id": chat_id, "text": text})

    def load_telegram_update_offset(self) -> int | None:
        path = Path(str(self.telegram_commands_config.get("offset_path", "telegram_update_offset.txt")))
        if not path.exists():
            return None
        try:
            return int(path.read_text().strip())
        except Exception:
            return None

    def save_telegram_update_offset(self) -> None:
        if self.telegram_update_offset is None:
            return
        path = Path(str(self.telegram_commands_config.get("offset_path", "telegram_update_offset.txt")))
        path.write_text(str(self.telegram_update_offset))

    def send_telegram_test(self) -> None:
        telegram = self.config.get("notifications", {}).get("telegram", {})
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        if not bot_token or not chat_ids:
            raise ValueError("Telegram is not configured. Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID.")

        test_signal = Signal(
            symbol="TEST",
            kind="test",
            score=0,
            title="Crypto monitor Telegram test",
            message="Telegram notification is configured correctly.",
            key="TEST:test",
        )
        for chat_id in split_chat_ids(chat_ids):
            self.send_telegram(bot_token, chat_id, test_signal)
        print("Telegram test message sent.")

    @staticmethod
    def format_optional(value: float | None) -> str:
        if value is None:
            return "n/a"
        return f"{value:.4g}"



def base_symbol(symbol: str) -> str:
    base = str(symbol).upper()
    if base.endswith("USDT"):
        base = base[:-4]
    while base and base[0].isdigit():
        base = base[1:]
    return base


def market_tier(symbol: str) -> str:
    base = base_symbol(symbol)
    core = {"BTC", "ETH", "SOL", "BNB"}
    large = {
        "DOGE", "XRP", "ADA", "LINK", "TRX", "LTC", "BCH", "AVAX", "DOT", "SUI",
        "TON", "XLM", "HBAR", "UNI", "NEAR", "APT", "ARB", "OP", "ICP", "ETC",
        "FIL", "ATOM", "INJ", "RENDER", "AAVE", "TAO", "WLD", "SEI", "TIA",
    }
    if base in core:
        return "core"
    if base in large:
        return "large"
    return "normal"


def is_major_asset_tier(symbol: str) -> bool:
    return market_tier(symbol) in ("core", "major", "large")


def tier_threshold(symbol: str, rule: dict[str, Any], name: str, default: float) -> float:
    tier = market_tier(symbol)
    key = f"{tier}_{name}"
    if key in rule:
        return float(rule.get(key))
    if tier == "core":
        core_defaults = {
            "min_price_change_percent": 0.6,
            "min_oi_change_percent": 2.0,
            "min_taker_buy_sell_ratio": 1.08,
            "top_min_price_change_percent": 1.8,
            "top_min_oi_change_percent": 4.0,
        }
        if name in core_defaults:
            return float(core_defaults[name])
    if tier == "large":
        large_defaults = {
            "min_price_change_percent": 0.9,
            "min_oi_change_percent": 3.0,
            "min_taker_buy_sell_ratio": 1.12,
            "top_min_price_change_percent": 2.3,
            "top_min_oi_change_percent": 6.0,
        }
        if name in large_defaults:
            return float(large_defaults[name])
    return float(rule.get(name, default))


def percent_change(start: float, end: float) -> float:
    if start == 0:
        return 0
    return ((end - start) / start) * 100


def normalize_market_cap_symbol(base_asset: str) -> str:
    symbol = str(base_asset).lower()
    while symbol and symbol[0].isdigit():
        symbol = symbol[1:]
    return symbol


def latest_float(rows: list[dict[str, Any]], key: str) -> float | None:
    if not rows:
        return None
    value = rows[-1].get(key)
    if value is None:
        return None
    return float(value)


def parse_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def optional_gte(value: float | None, threshold: float) -> bool:
    return value is not None and value >= threshold


def optional_lte(value: float | None, threshold: float) -> bool:
    return value is not None and value <= threshold


def resolve_telegram_credentials(config: dict[str, Any]) -> tuple[str | None, str | None]:
    bot_token = config.get("bot_token")
    chat_id = config.get("chat_id")
    bot_token_env = config.get("bot_token_env")
    chat_id_env = config.get("chat_id_env")

    if not bot_token and bot_token_env:
        bot_token = os.getenv(str(bot_token_env))
    if not chat_id and chat_id_env:
        chat_id = os.getenv(str(chat_id_env))
    return bot_token, chat_id



def ensure_csv_schema(path: Path, required_fields: list[str]) -> tuple[list[str], bool]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        return list(required_fields), True

    with path.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        existing_fields = [field for field in (reader.fieldnames or []) if field]
        rows = list(reader)
        if not existing_fields:
            backup_path = backup_csv_before_schema_upgrade(path)
            logging.warning(
                "Signal CSV had no readable header; backed up before schema reset: path=%s backup=%s",
                path,
                backup_path,
            )
            rewrite_csv(path, required_fields, [])
            return list(required_fields), False

        fieldnames = existing_fields + [
            field for field in required_fields
            if field not in existing_fields
        ]
        if fieldnames == existing_fields:
            return fieldnames, False

    backup_path = backup_csv_before_schema_upgrade(path)
    rewrite_csv(path, fieldnames, rows)
    logging.info(
        "Upgraded signal CSV schema: path=%s backup=%s added_fields=%s",
        path,
        backup_path,
        ",".join(field for field in fieldnames if field not in existing_fields),
    )
    return fieldnames, False



def backup_csv_before_schema_upgrade(path: Path) -> Path:
    timestamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    backup_path = path.with_name(f"{path.name}.schema_backup_{timestamp}")
    suffix = 1
    while backup_path.exists():
        backup_path = path.with_name(f"{path.name}.schema_backup_{timestamp}_{suffix}")
        suffix += 1
    shutil.copy2(path, backup_path)
    return backup_path



def rewrite_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    temp_path = path.with_name(f".{path.name}.tmp")
    with temp_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})

    temp_path.replace(path)



def strength_grade_from_csv(value: str | None) -> str:
    if value in (None, ""):
        return "-"
    try:
        return strength_grade(float(value))
    except Exception:
        return "-"



def format_usd(value: float | None) -> str:
    if value is None:
        return "n/a"
    abs_value = abs(value)
    sign = "+" if value >= 0 else "-"
    if abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.1f}K"
    return f"{sign}{abs_value:.0f}"


def format_liquidation_age(seconds: float) -> str:
    seconds = max(0, int(seconds))
    if seconds < 60:
        return f"{seconds}秒"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}分钟"
    hours = minutes // 60
    remaining_minutes = minutes % 60
    if remaining_minutes:
        return f"{hours}小时{remaining_minutes}分钟"
    return f"{hours}小时"


def liquidation_judgement(stats_1h: dict[str, float] | None) -> str:
    if not stats_1h:
        return "近1h暂无明显强平数据"
    long_liq = stats_1h["long_liq_usd"]
    short_liq = stats_1h["short_liq_usd"]
    total = long_liq + short_liq
    if total < 50000:
        return "近1h暂无明显强平数据"
    if long_liq >= 100000 and short_liq >= 100000 and max(long_liq, short_liq) / min(long_liq, short_liq) < 2:
        return "双向强平/剧烈洗盘"
    if long_liq >= short_liq * 2 and long_liq >= 100000:
        return "多头强平主导"
    if short_liq >= long_liq * 2 and short_liq >= 100000:
        return "空头强平主导"
    if total >= 500000:
        return "强平活跃但方向分散"
    return "强平分散"


def coinglass_response_ok(data: Any) -> bool:
    if not isinstance(data, dict):
        return False
    code = data.get("code")
    return str(code) == "0"


def coinglass_liquidation_stats_from_response(data: Any) -> dict[str, float] | None:
    rows = coinglass_liquidation_rows(data.get("data") if isinstance(data, dict) else data)
    if not rows:
        return None

    long_liq = 0.0
    short_liq = 0.0
    count = 0.0
    for row in rows:
        row_long = parse_float(row.get("long_liquidation_usd"))
        row_short = parse_float(row.get("short_liquidation_usd"))
        if row_long is None and row_short is None:
            continue
        long_liq += row_long or 0.0
        short_liq += row_short or 0.0
        count += 1

    if count <= 0:
        return None
    return {"long_liq_usd": long_liq, "short_liq_usd": short_liq, "count": count}


def coinglass_liquidation_rows(data: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            rows.extend(coinglass_liquidation_rows(item))
    elif isinstance(data, dict):
        if "long_liquidation_usd" in data or "short_liquidation_usd" in data:
            rows.append(data)
        else:
            for value in data.values():
                rows.extend(coinglass_liquidation_rows(value))
    return rows


def coinglass_rows(data: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    if isinstance(data, list):
        for item in data:
            rows.extend(coinglass_rows(item))
    elif isinstance(data, dict):
        if any(not isinstance(value, (dict, list)) for value in data.values()):
            rows.append(data)
        for value in data.values():
            if isinstance(value, (dict, list)):
                rows.extend(coinglass_rows(value))
    return rows


def coinglass_find_exchange_row(data: Any, exchange: str) -> dict[str, Any] | None:
    target = exchange.lower()
    for row in coinglass_rows(data):
        names = [
            row.get("exchange"),
            row.get("exchange_name"),
            row.get("name"),
            row.get("symbol"),
        ]
        if any(str(name).lower() == target for name in names if name is not None):
            return row
    return None


def coinglass_first_metric_row(data: Any, keys: list[str]) -> dict[str, Any] | None:
    for row in coinglass_rows(data):
        if any(key in row for key in keys):
            return row
    return None


def coinglass_first_float(row: dict[str, Any], keys: list[str]) -> float | None:
    for key in keys:
        value = parse_float(row.get(key))
        if value is not None:
            return value
    return None


def coinglass_balance_ranges_from_row(
    row: dict[str, Any],
    source: str | None,
) -> dict[str, dict[str, float | str | None]]:
    ranges: dict[str, dict[str, float | str | None]] = {}
    for range_value, suffixes in {
        "24h": ("1d", "24h"),
        "7d": ("7d",),
        "30d": ("30d",),
    }.items():
        change = coinglass_first_float(row, coinglass_balance_change_keys(suffixes))
        change_percent = coinglass_first_float(row, coinglass_balance_change_percent_keys(suffixes))
        if change is None and change_percent is None:
            continue
        ranges[range_value] = {
            "balance": coinglass_first_float(row, ["balance", "amount", "value", "total_balance"]),
            "balance_usd": coinglass_first_float(row, ["balance_usd", "usd", "value_usd", "total_balance_usd"]),
            "change_percent": change_percent,
            "change": change,
            "source": source,
        }
    return ranges


def coinglass_summed_balance_ranges(data: Any, base_symbol: str) -> dict[str, dict[str, float | str | None]]:
    rows = [
        row
        for row in coinglass_rows(data)
        if not coinglass_node_symbol_mismatches(row, base_symbol)
        and coinglass_row_exchange_name(row).lower() != "all"
    ]
    ranges: dict[str, dict[str, float | str | None]] = {}
    for range_value, suffixes in {
        "24h": ("1d", "24h"),
        "7d": ("7d",),
        "30d": ("30d",),
    }.items():
        total = 0.0
        found = False
        for row in rows:
            change = coinglass_first_float(row, coinglass_balance_change_keys(suffixes))
            if change is None:
                continue
            total += change
            found = True
        if found:
            ranges[range_value] = {
                "balance": None,
                "balance_usd": None,
                "change_percent": None,
                "change": total,
                "source": None,
            }
    return ranges


def coinglass_balance_change_keys(suffixes: tuple[str, ...]) -> list[str]:
    keys: list[str] = []
    for suffix in suffixes:
        keys.extend(
            [
                f"balance_change_{suffix}",
                f"change_{suffix}",
                f"netflow_{suffix}",
                f"net_flow_{suffix}",
            ]
        )
    return keys


def coinglass_balance_change_percent_keys(suffixes: tuple[str, ...]) -> list[str]:
    keys: list[str] = []
    for suffix in suffixes:
        keys.extend(
            [
                f"balance_change_percent_{suffix}",
                f"balance_change_percentage_{suffix}",
                f"change_percent_{suffix}",
                f"change_percentage_{suffix}",
                f"netflow_percent_{suffix}",
                f"net_flow_percent_{suffix}",
            ]
        )
    return keys


def coinglass_stablecoin_margin_rows(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, dict):
        stablecoin_rows = data.get("stablecoin_margin_list")
        if isinstance(stablecoin_rows, list):
            return [row for row in stablecoin_rows if isinstance(row, dict)]
        for value in data.values():
            rows = coinglass_stablecoin_margin_rows(value)
            if rows:
                return rows
    if isinstance(data, list):
        for item in data:
            rows = coinglass_stablecoin_margin_rows(item)
            if rows:
                return rows
    return []


def coinglass_stablecoin_margin_rows_for_base(data: Any, base_symbol: str) -> list[dict[str, Any]]:
    base_symbol = base_symbol.upper()
    if not base_symbol:
        return []
    rows, mismatch_seen = _coinglass_stablecoin_margin_rows_for_base(data, base_symbol)
    if mismatch_seen and not rows:
        logging.debug("CoinGlass stablecoin_margin_list symbol mismatch for %s", base_symbol)
    return rows


def _coinglass_stablecoin_margin_rows_for_base(data: Any, base_symbol: str) -> tuple[list[dict[str, Any]], bool]:
    if isinstance(data, dict):
        stablecoin_rows = data.get("stablecoin_margin_list")
        if isinstance(stablecoin_rows, list):
            if coinglass_node_symbol_mismatches(data, base_symbol):
                return [], True
            rows = [
                row
                for row in stablecoin_rows
                if isinstance(row, dict) and not coinglass_node_symbol_mismatches(row, base_symbol)
            ]
            mismatch_seen = len(rows) < len([row for row in stablecoin_rows if isinstance(row, dict)])
            return rows, mismatch_seen
        mismatch_seen = False
        for value in data.values():
            child_rows, child_mismatch_seen = _coinglass_stablecoin_margin_rows_for_base(value, base_symbol)
            mismatch_seen = mismatch_seen or child_mismatch_seen
            if child_rows:
                return child_rows, mismatch_seen
        return [], mismatch_seen
    if isinstance(data, list):
        mismatch_seen = False
        for item in data:
            rows, child_mismatch_seen = _coinglass_stablecoin_margin_rows_for_base(item, base_symbol)
            mismatch_seen = mismatch_seen or child_mismatch_seen
            if rows:
                return rows, mismatch_seen
        return [], mismatch_seen
    return [], False


def coinglass_node_symbol_mismatches(row: dict[str, Any], base_symbol: str) -> bool:
    symbols = coinglass_node_symbols(row)
    if not symbols:
        return False
    return not any(coinglass_symbol_matches_base(symbol, base_symbol) for symbol in symbols)


def coinglass_node_symbols(row: dict[str, Any]) -> list[str]:
    symbols: list[str] = []
    for key in ("symbol", "base", "base_symbol", "coin", "currency"):
        value = row.get(key)
        if value is not None and str(value).strip():
            symbols.append(str(value).strip())
    return symbols


def coinglass_symbol_matches_base(value: str, base_symbol: str) -> bool:
    normalized = value.upper().replace("-", "").replace("_", "").replace("/", "")
    base_symbol = base_symbol.upper()
    if normalized == base_symbol:
        return True
    if not normalized.startswith(base_symbol):
        return False
    suffix = normalized[len(base_symbol):]
    return suffix.startswith(("USD", "USDT", "USDC", "PERP", "SWAP"))


def coinglass_row_exchange_name(row: dict[str, Any]) -> str:
    for key in ("exchange", "exchange_name", "name"):
        value = row.get(key)
        if value is not None and str(value).strip():
            return str(value).strip()
    return ""


def coinglass_row_funding(row: dict[str, Any]) -> float | None:
    for key in ("funding_rate", "current_funding_rate", "rate", "funding_rate_percent", "next_funding_rate"):
        value = parse_float(row.get(key))
        if value is not None:
            return value
    return None


def coinglass_funding_distribution(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    total = 0
    negative = 0
    positive = 0
    extreme = 0
    extreme_negative = 0
    extreme_positive = 0
    exchange_rates: dict[str, float] = {}
    for row in rows:
        funding = coinglass_row_funding(row)
        if funding is None:
            continue
        total += 1
        if funding < 0:
            negative += 1
        elif funding > 0:
            positive += 1
        if abs(funding) >= 0.01:
            extreme += 1
        if funding <= -0.01:
            extreme_negative += 1
        elif funding >= 0.01:
            extreme_positive += 1

        exchange_name = coinglass_row_exchange_name(row).lower()
        for target in ("binance", "okx", "bybit"):
            if target in exchange_name and target.title() not in exchange_rates:
                exchange_rates[target.title()] = funding

    if total <= 0:
        return None
    return {
        "total": total,
        "negative": negative,
        "positive": positive,
        "extreme": extreme,
        "extreme_negative": extreme_negative,
        "extreme_positive": extreme_positive,
        "exchange_rates": exchange_rates,
    }


def format_coinglass_market_context_text(context: dict[str, Any]) -> str:
    oi = context.get("open_interest") if isinstance(context.get("open_interest"), dict) else {}
    distribution = context.get("funding_distribution") if isinstance(context.get("funding_distribution"), dict) else {}
    taker = context.get("taker_flow") if isinstance(context.get("taker_flow"), dict) else {}
    funding_oi_weight = context.get("funding_oi_weight")

    oi_1h = oi.get("change_1h")
    oi_4h = oi.get("change_4h")
    oi_24h = oi.get("change_24h")
    has_distribution = bool(distribution and distribution.get("total"))
    negative = int(distribution.get("negative") or 0) if has_distribution else 0
    positive = int(distribution.get("positive") or 0) if has_distribution else 0
    total = int(distribution.get("total") or 0) if has_distribution else 0
    extreme = int(distribution.get("extreme") or 0) if has_distribution else 0
    extreme_negative = int(distribution.get("extreme_negative") or 0) if has_distribution else 0
    extreme_positive = int(distribution.get("extreme_positive") or 0) if has_distribution else 0
    buy_ratio = taker.get("buy_ratio")
    sell_ratio = taker.get("sell_ratio")

    judgement = coinglass_market_context_judgement(
        oi_1h,
        oi_4h,
        funding_oi_weight,
        distribution.get("exchange_rates"),
        sell_ratio,
    )
    exchange_text = format_coinglass_exchange_funding(distribution.get("exchange_rates"))
    funding_distribution_text = (
        f"Funding交易所分布 负费率交易所 {negative}/{total}，正费率 {positive}/{total}，极端 {extreme}"
        if has_distribution
        else "Funding交易所分布 n/a"
    )
    text = (
        "CoinGlass聚合: "
        f"OI 1h {format_percent_optional(oi_1h)} / 4h {format_percent_optional(oi_4h)} / 24h {format_percent_optional(oi_24h)}；"
        f"Funding OI加权 {format_percent_optional(funding_oi_weight)}{exchange_text}，"
        f"{funding_distribution_text}；"
        f"主动买卖 24h 买{format_ratio_percent(buy_ratio)} / 卖{format_ratio_percent(sell_ratio)}"
        f"{format_coinglass_major_long_suffix(context.get('major_long'))}"
        f"；判断: {judgement}"
    )
    if is_major_asset_tier(str(context.get("symbol") or "")):
        text = f"{text}\n{format_coinglass_orderbook_context_text(context.get('orderbook'))}"
    return text


def coinglass_orderbook_context_from_rows(rows: list[dict[str, Any]]) -> dict[str, float | None] | None:
    usable_rows = []
    for row in rows:
        bids_usd = parse_float(row.get("bids_usd"))
        asks_usd = parse_float(row.get("asks_usd"))
        if bids_usd is None and asks_usd is None:
            continue
        usable_rows.append(
            {
                "time": parse_float(row.get("time")),
                "bids_usd": bids_usd,
                "asks_usd": asks_usd,
                "bids_quantity": parse_float(row.get("bids_quantity")),
                "asks_quantity": parse_float(row.get("asks_quantity")),
            }
        )

    if not usable_rows:
        return None

    if all(row.get("time") is not None for row in usable_rows):
        usable_rows.sort(key=lambda row: float(row.get("time") or 0))

    recent = usable_rows[-1]
    recent_bids = recent.get("bids_usd")
    recent_asks = recent.get("asks_usd")
    last_4h = usable_rows[-4:]
    avg_bids_4h = average_optional([row.get("bids_usd") for row in last_4h])
    avg_asks_4h = average_optional([row.get("asks_usd") for row in last_4h])
    ratio = recent_bids / recent_asks if recent_bids is not None and recent_asks and recent_asks > 0 else None

    return {
        "bids_usd_1h": recent_bids,
        "asks_usd_1h": recent_asks,
        "bids_usd_avg_4h": avg_bids_4h,
        "asks_usd_avg_4h": avg_asks_4h,
        "bid_ask_ratio": ratio,
    }


def average_optional(values: list[float | None]) -> float | None:
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)


def format_coinglass_orderbook_context_text(orderbook: Any) -> str:
    if not isinstance(orderbook, dict):
        return "CoinGlass订单簿: n/a"
    return (
        "CoinGlass订单簿: "
        f"近1h 买盘{format_usd(orderbook.get('bids_usd_1h'))} / 卖盘{format_usd(orderbook.get('asks_usd_1h'))}；"
        f"4h均值 买盘{format_usd(orderbook.get('bids_usd_avg_4h'))} / 卖盘{format_usd(orderbook.get('asks_usd_avg_4h'))}；"
        f"判断: {coinglass_orderbook_judgement(orderbook.get('bid_ask_ratio'))}"
    )


def coinglass_orderbook_judgement(ratio: Any) -> str:
    if ratio is None:
        return "n/a"
    try:
        value = float(ratio)
    except (TypeError, ValueError):
        return "n/a"
    if value >= 1.25:
        return "下方承接偏强"
    if value <= 0.8:
        return "上方卖压偏强"
    return "买卖盘相对均衡"


def format_coinglass_major_long_suffix(major_long: Any) -> str:
    if not isinstance(major_long, dict):
        return ""
    taker_ranges = major_long.get("taker_ranges") if isinstance(major_long.get("taker_ranges"), dict) else {}
    balance_ranges = major_long.get("balance_ranges") if isinstance(major_long.get("balance_ranges"), dict) else {}
    funding_ranges = (
        major_long.get("funding_accumulated_ranges")
        if isinstance(major_long.get("funding_accumulated_ranges"), dict)
        else {}
    )
    balance_label = "交易所余额"
    balance_source = coinglass_balance_ranges_source(balance_ranges)
    if balance_source:
        balance_label = f"{balance_label}({balance_source})"
    return (
        f"；CoinGlass主动买卖 7d {format_taker_range(taker_ranges.get('7d'))}"
        f"；{balance_label}: 1d {format_balance_range(balance_ranges.get('24h'))} / "
        f"7d {format_balance_range(balance_ranges.get('7d'))} / "
        f"30d {format_balance_range(balance_ranges.get('30d'))}"
        f"；Funding累计: 1d {format_funding_accumulated_range(funding_ranges.get('24h'))} / "
        f"7d {format_funding_accumulated_range(funding_ranges.get('7d'))}"
    )


def format_major_long_cycle_context(snapshot: MarketSnapshot, coinglass_text: str | None = None) -> str:
    if not is_major_asset_tier(snapshot.symbol):
        return ""
    oi_text = extract_labeled_segment(coinglass_text, "OI ", "；") or "OI n/a"
    taker_24h = extract_labeled_segment(coinglass_text, "主动买卖 24h", "；")
    taker_7d = extract_labeled_segment(coinglass_text, "CoinGlass主动买卖 7d", "；")
    taker_text = " / ".join(part for part in (taker_24h, taker_7d) if part) or "n/a"
    balance_text = extract_labeled_segment(coinglass_text, "交易所余额", "；") or "交易所余额 n/a"
    funding_text = extract_labeled_segment(coinglass_text, "Funding累计", "；") or "Funding累计 n/a"
    return (
        "主流币长周期确认:\n"
        f"- 合约资金: 1h {format_usd(snapshot.net_flow_usd.get('1h'))} / "
        f"4h {format_usd(snapshot.net_flow_usd.get('4h'))} / "
        f"12h {format_usd(snapshot.net_flow_usd.get('12h'))} / "
        f"24h {format_usd(snapshot.net_flow_usd.get('24h'))}\n"
        f"- 长周期资金共振: {long_flow_alignment_score(snapshot)}/9\n"
        f"- CoinGlass OI: {oi_text}\n"
        f"- CoinGlass主动买卖: {taker_text}\n"
        f"- {balance_text}\n"
        f"- {funding_text}"
    )


def format_major_long_cycle_one_line(snapshot: MarketSnapshot, coinglass_text: str | None = None) -> str:
    if not is_major_asset_tier(snapshot.symbol):
        return ""
    oi_text = extract_labeled_segment(coinglass_text, "OI ", "；") or "OI n/a"
    balance_text = extract_labeled_segment(coinglass_text, "交易所余额", "；") or "交易所余额 n/a"
    return f"主流长周期: {oi_text}; {balance_text}; 资金共振 {long_flow_alignment_score(snapshot)}/9"


def extract_labeled_segment(text: str | None, start: str, end: str) -> str | None:
    if not text:
        return None
    index = text.find(start)
    if index < 0:
        return None
    segment = text[index:]
    end_index = segment.find(end)
    if end_index >= 0:
        segment = segment[:end_index]
    return segment.strip()


def format_taker_range(value: Any) -> str:
    if not isinstance(value, dict):
        return "n/a"
    return f"买{format_ratio_percent(value.get('buy_ratio'))} / 卖{format_ratio_percent(value.get('sell_ratio'))}"


def format_balance_range(value: Any) -> str:
    if not isinstance(value, dict):
        return "n/a"
    change_percent = value.get("change_percent")
    change = value.get("change")
    if change_percent is not None:
        return format_percent_optional(change_percent)
    if change is not None:
        return format_token_amount(change)
    return "n/a"


def format_funding_accumulated_range(value: Any) -> str:
    if not isinstance(value, dict):
        return "n/a"
    rate = value.get("rate")
    if rate is None:
        return "n/a"
    return format_percent_optional(rate)


def coinglass_balance_ranges_source(balance_ranges: dict[Any, Any]) -> str:
    sources = {
        str(value.get("source")).strip()
        for value in balance_ranges.values()
        if isinstance(value, dict) and value.get("source")
    }
    if len(sources) == 1:
        return next(iter(sources))
    return ""


def format_token_amount(value: float | None) -> str:
    if value is None:
        return "n/a"
    abs_value = abs(value)
    sign = "+" if value >= 0 else "-"
    if abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.1f}K"
    return f"{sign}{abs_value:.4g}"


def format_coinglass_exchange_funding(exchange_rates: Any) -> str:
    items = []
    for exchange in ("Binance", "OKX", "Bybit"):
        value = None
        if isinstance(exchange_rates, dict):
            for key, rate in exchange_rates.items():
                if str(key).lower() == exchange.lower():
                    value = rate
                    break
        items.append(f"{exchange} {format_percent_optional(value) if value is not None else 'n/a'}")
    return f" ({' / '.join(items)})" if items else ""


def format_ratio_percent(value: float | None) -> str:
    if value is None:
        return "-"
    if abs(value) <= 1:
        return f"{value * 100:.1f}%"
    return f"{value:.1f}%"


def coinglass_market_context_judgement(
    oi_1h: float | None,
    oi_4h: float | None,
    funding_oi_weight: float | None,
    exchange_rates: Any,
    sell_ratio: float | None,
) -> str:
    major_exchange_rates = coinglass_major_exchange_rates(exchange_rates)
    extreme_negative = sum(1 for value in major_exchange_rates if value <= -0.1)
    extreme_positive = sum(1 for value in major_exchange_rates if value >= 0.1)
    negative_crowded = (funding_oi_weight is not None and funding_oi_weight <= -0.1) or extreme_negative >= 2
    positive_crowded = (funding_oi_weight is not None and funding_oi_weight >= 0.1) or extreme_positive >= 2
    oi_rising = oi_1h is not None and oi_4h is not None and oi_1h > 0 and oi_4h > 0
    oi_falling = oi_1h is not None and oi_4h is not None and oi_1h < 0 and oi_4h < 0
    sell_pressure = False
    if sell_ratio is not None:
        sell_pressure_threshold = 0.53 if abs(sell_ratio) <= 1 else 53
        sell_pressure = sell_ratio >= sell_pressure_threshold

    judgement_parts: list[str] = []
    if oi_falling:
        judgement_parts.append("仓位退出/风险释放")
    if negative_crowded:
        judgement_parts.append("全市场空头拥挤")
    if positive_crowded:
        judgement_parts.append("全市场多头拥挤")
    if oi_rising and positive_crowded:
        judgement_parts.append("全市场杠杆升温/多头拥挤")

    if sell_pressure:
        judgement_parts.append("主动卖压偏强")
    if not judgement_parts:
        judgement_parts.append("全市场衍生品中性/分歧")
    return "；".join(judgement_parts)


def coinglass_major_exchange_rates(exchange_rates: Any) -> list[float]:
    if not isinstance(exchange_rates, dict):
        return []
    values = []
    for exchange in ("Binance", "OKX", "Bybit"):
        for key, rate in exchange_rates.items():
            if str(key).lower() != exchange.lower():
                continue
            value = parse_float(rate)
            if value is not None:
                values.append(value)
            break
    return values


def format_coinglass_liquidation_stats(stats: dict[str, float]) -> str:
    return (
        "CoinGlass历史 1h "
        f"多单强平 {format_usd(stats['long_liq_usd'])} / "
        f"空单强平 {format_usd(stats['short_liq_usd'])}"
    )


def force_order_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
        payload = payload["data"]
    if isinstance(payload, list):
        candidates = payload
    else:
        candidates = [payload]

    items = []
    for item in candidates:
        if not isinstance(item, dict):
            continue
        order = item.get("o")
        if isinstance(order, dict):
            items.append(order)
        else:
            items.append(item)
    return items


def liquidation_event_from_order(order: dict[str, Any]) -> LiquidationEvent | None:
    symbol = str(order.get("s", "")).upper()
    side = str(order.get("S", "")).upper()
    if not symbol or side not in ("BUY", "SELL"):
        return None

    amount = liquidation_order_amount_usd(order)
    if amount <= 0:
        return None

    event_ms = parse_float(order.get("T"))
    event_time = (event_ms / 1000) if event_ms and event_ms > 1_000_000_000_000 else time.time()
    return LiquidationEvent(symbol=symbol, side=side, amount_usd=amount, event_time=event_time)


def liquidation_order_amount_usd(order: dict[str, Any]) -> float:
    average_price = parse_float(order.get("ap"))
    filled_qty = parse_float(order.get("z")) or parse_float(order.get("l")) or parse_float(order.get("q"))
    if average_price is not None and filled_qty is not None:
        amount = average_price * filled_qty
        if amount > 0:
            return amount

    price = parse_float(order.get("p"))
    quantity = parse_float(order.get("q"))
    if price is not None and quantity is not None:
        return max(0.0, price * quantity)
    return 0.0


def update_liquidation_stats_bucket(bucket: dict[str, float], event: LiquidationEvent) -> None:
    if event.side == "SELL":
        bucket["long_liq_usd"] += event.amount_usd
    elif event.side == "BUY":
        bucket["short_liq_usd"] += event.amount_usd
    else:
        return
    bucket["count"] += 1



def review_float(row: dict[str, str], key: str, fallback_index: int | None = None) -> float | None:
    value = row.get(key)
    if (value is None or value == "") and fallback_index is not None:
        named_keys = [k for k in row.keys() if k is not None]
        extras = row.get(None) or []
        extra_index = fallback_index - len(named_keys)
        if 0 <= extra_index < len(extras):
            value = extras[extra_index]
        else:
            values = list(row.values())
            if len(values) > fallback_index and not isinstance(values[fallback_index], list):
                value = values[fallback_index]
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None



def format_csv_strength(value: str | None) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):.2f}"
    except Exception:
        return str(value)



def format_csv_number(value: str | None) -> str:
    if value in (None, ""):
        return "-"
    try:
        return f"{float(value):+.2f}"
    except Exception:
        return str(value)


def format_csv_compact_number(value: Any, signed: bool = False) -> str:
    if value in (None, ""):
        return "-"
    try:
        number = float(value)
    except Exception:
        return str(value)
    text = f"{number:+.2f}" if signed else f"{number:.2f}"
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def signal_direction_label(kind: str | None) -> str:
    raw = str(kind or "").strip().lower()
    normalized = raw.replace("_", " ").replace("-", " ")
    bullish = {
        "discovery",
        "hot breakout",
        "bottom reversal",
        "early breakout",
        "possible early breakout",
    }
    bearish = {
        "top risk",
        "top exhaustion",
        "distribution",
        "crowded top risk",
    }
    if raw in bullish or normalized in bullish:
        return "看多"
    if raw in bearish or normalized in bearish:
        return "看空"
    return "观察"


def format_suppressed_status(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    if text == "0":
        return "已推送"
    if text == "1":
        return "已静默"
    return "未知"


def funding_note(value: float | None) -> str:
    if value is None:
        return "未知"
    if value <= -0.5:
        return "极端负费率: 空头拥挤或异常波动，做多也要防插针"
    if value <= -0.05:
        return "偏负: 空头较多，若价格走强可能有挤空"
    if value >= 0.08:
        return "过热: 多头成本高，追多风险大"
    if value >= 0.03:
        return "偏热: 多头较拥挤，谨慎追高"
    return "正常"


def position_note(snapshot: MarketSnapshot) -> str:
    pos = snapshot.price_position_24h
    if pos is None:
        return "未知"
    if pos >= 85:
        return "24h高位: 追多风险高，适合等回踩"
    if pos >= 65:
        return "偏高: 已经涨了一段，追单要谨慎"
    if pos >= 35:
        return "中部: 位置相对中性"
    if pos >= 15:
        return "偏低: 位置不高，但要等资金确认"
    return "24h低位: 低位不等于能涨，需看资金进场"


def volume_note(snapshot: MarketSnapshot) -> str:
    ratio = snapshot.volume_ratio_24h
    if ratio is None:
        return "未知"
    if ratio >= 8:
        return "极活跃: 流动性充足，信号可信度更高"
    if ratio >= 4:
        return "活跃: 成交较好"
    if ratio >= 2:
        return "正常: 基本满足流动性"
    return "偏冷: 成交不足，OI信号容易失真"


def flow_alignment_note(score: int) -> str:
    if score >= 8:
        return "强共振: 多周期资金方向一致"
    if score >= 5:
        return "中性偏强: 有资金支持但不完全一致"
    if score >= 3:
        return "偏弱: 资金方向分歧"
    return "弱: 多周期资金不支持"


def score_note(score: int) -> str:
    if score >= 8:
        return "强: 可重点观察"
    if score >= 5:
        return "中: 有迹象但还需确认"
    if score >= 3:
        return "弱: 暂不主动追"
    return "差: 不支持当前方向"


def volume_label(snapshot: MarketSnapshot) -> str:
    ratio = snapshot.volume_ratio_24h
    if ratio is None:
        return "未知"
    if ratio >= 8:
        return "极活跃"
    if ratio >= 4:
        return "活跃"
    if ratio >= 2:
        return "正常"
    return "偏冷"


def price_position_label(snapshot: MarketSnapshot) -> str:
    position = snapshot.price_position_24h
    if position is None:
        return "未知"
    if position >= 85:
        return "24h高位"
    if position >= 65:
        return "偏高"
    if position >= 35:
        return "中部"
    if position >= 15:
        return "偏低"
    return "24h低位"


def short_term_score(snapshot: MarketSnapshot) -> int:
    score = 0
    if summary_flow_value(snapshot, "5m") > 0:
        score += 2
    if summary_flow_value(snapshot, "15m") > 0:
        score += 3
    if (snapshot.taker_buy_sell_ratio or 0) >= 1.15:
        score += 2
    if snapshot.oi_change_percent >= 2:
        score += 2
    if (snapshot.confirm_price_change_percent or 0) > 0:
        score += 1
    return min(score, 10)


def mid_term_score(snapshot: MarketSnapshot) -> int:
    score = 0
    if summary_flow_value(snapshot, "1h") > 0:
        score += 3
    if summary_flow_value(snapshot, "4h") > 0:
        score += 3
    if snapshot.oi_change_percent >= 4:
        score += 2
    if snapshot.price_change_percent > 0:
        score += 1
    if snapshot.price_position_24h is not None and snapshot.price_position_24h < 85:
        score += 1
    return min(score, 10)


def score_label(score: int) -> str:
    if score >= 8:
        return "强"
    if score >= 5:
        return "中"
    if score >= 3:
        return "弱"
    return "差"


def trend_reading(snapshot: MarketSnapshot) -> str:
    看空 = short_term_score(snapshot)
    mid = mid_term_score(snapshot)
    position = snapshot.price_position_24h

    if 看空 >= 7 and mid >= 6:
        return "短线和中线资金共振偏强，可继续重点观察。"
    if 看空 >= 7 and mid < 5:
        return "短线偏强但中线未确认，适合快进快出，不宜恋战。"
    if 看空 < 5 and mid >= 6:
        return "中线仍有承接，但短线动能不足，适合等回踩确认。"
    if position is not None and position >= 85 and 看空 < 6:
        return "位置偏高但短线动能不足，追高风险较大。"
    if summary_flow_value(snapshot, "15m") < 0 and summary_flow_value(snapshot, "1h") < 0:
        return "15m和1h资金流偏弱，当前更适合防守。"
    return "暂无明显共振，继续观察。"




_SPOT_CHAIN_CACHE: dict[str, tuple[float, str]] = {}


def spot_alpha_confirmation(symbol: str) -> str:
    # 名字先保留，避免改动调用处；实际逻辑是：现货优先，没有现货再查链上 DEX。
    symbol = str(symbol).upper()
    cached = _SPOT_CHAIN_CACHE.get(symbol)
    if cached and time.time() - cached[0] < 180:
        return cached[1]

    spot = fetch_spot_confirmation(symbol)
    if spot:
        result = spot
    else:
        chain = fetch_dexscreener_confirmation(symbol)
        result = chain or "无标准现货/高流动性DEX数据，仅按合约数据观察"

    _SPOT_CHAIN_CACHE[symbol] = (time.time(), result)
    return result


def cached_spot_alpha_confirmation(symbol: str) -> str:
    cached = _SPOT_CHAIN_CACHE.get(str(symbol).upper())
    if cached and time.time() - cached[0] < 180:
        return cached[1]
    return ""


def fetch_spot_confirmation(symbol: str) -> str | None:
    for spot_symbol in spot_symbol_candidates(symbol):
        summaries = []
        for label, limit in [("15m", 4), ("1h", 13), ("4h", 49)]:
            item = fetch_spot_period_confirmation(spot_symbol, label, limit)
            if item:
                summaries.append(item)
        if summaries:
            suffix = "" if spot_symbol == symbol else f"({spot_symbol}) "
            return "标准现货 " + suffix + " / ".join(summaries)
    return None


def spot_symbol_candidates(symbol: str) -> list[str]:
    symbol = str(symbol).upper()
    candidates = []
    if symbol.endswith("USDT"):
        base = symbol[:-4]
        stripped = base
        while stripped and stripped[0].isdigit():
            stripped = stripped[1:]
        if stripped and stripped != base:
            candidates.append(stripped + "USDT")
    candidates.append(symbol)
    return list(dict.fromkeys(candidates))


def fetch_spot_period_confirmation(symbol: str, label: str, limit: int) -> str | None:
    try:
        rows = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": symbol, "interval": "5m", "limit": limit},
            timeout=3,
        )
        if rows.status_code != 200:
            return None
        data = rows.json()
        if len(data) < 2:
            return None

        first_close = float(data[0][4])
        last_close = float(data[-1][4])
        quote_volume = sum(float(row[7]) for row in data)
        price_change = percent_change(first_close, last_close)

        if price_change > 0.3 and quote_volume >= 100000:
            state = "偏强"
        elif price_change < -0.3:
            state = "偏弱"
        else:
            state = "中性"
        return f"{label}{state}({price_change:+.2f}%, {format_usd(quote_volume)})"
    except Exception:
        return None


def fetch_dexscreener_confirmation(symbol: str) -> str | None:
    base = normalize_dex_symbol(symbol)
    if not base:
        return None
    try:
        response = requests.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": base},
            timeout=4,
        )
        if response.status_code != 200:
            return None
        payload = response.json()
        pairs = payload.get("pairs") if isinstance(payload, dict) else None
        if not pairs:
            return None

        pair = best_dex_pair(base, pairs)
        if not pair:
            return None

        price_change = pair.get("priceChange") or {}
        volume = pair.get("volume") or {}
        liquidity = pair.get("liquidity") or {}
        h1 = safe_float(price_change.get("h1"))
        h24 = safe_float(price_change.get("h24"))
        vol1h = safe_float(volume.get("h1"))
        vol24h = safe_float(volume.get("h24"))
        liq = safe_float(liquidity.get("usd"))

        h1_state = trend_state(h1)
        h24_state = trend_state(h24)
        chain = pair.get("chainId", "-")
        dex = pair.get("dexId", "-")
        quote = (pair.get("quoteToken") or {}).get("symbol", "-")

        return (
            f"链上DEX {h1_state} "
            f"1h={format_percent_optional(h1)} / 24h={format_percent_optional(h24)} "
            f"成交1h={format_usd(vol1h)} / 24h={format_usd(vol24h)} "
            f"流动性={format_usd(liq)} ({chain}/{dex}/{quote})"
        )
    except Exception:
        return None


def normalize_dex_symbol(symbol: str) -> str:
    base = str(symbol).upper()
    if base.endswith("USDT"):
        base = base[:-4]
    while base and base[0].isdigit():
        base = base[1:]
    return base


def best_dex_pair(base: str, pairs: list[dict[str, Any]]) -> dict[str, Any] | None:
    quote_priority = {"USDT", "USDC", "WETH", "WBNB", "ETH", "BNB", "SOL"}
    candidates = []
    for pair in pairs:
        base_token = pair.get("baseToken") or {}
        quote_token = pair.get("quoteToken") or {}
        base_symbol = str(base_token.get("symbol", "")).upper()
        quote_symbol = str(quote_token.get("symbol", "")).upper()
        if base_symbol != base:
            continue
        liquidity = safe_float((pair.get("liquidity") or {}).get("usd")) or 0.0
        volume_24h = safe_float((pair.get("volume") or {}).get("h24")) or 0.0
        if liquidity < 50000 and volume_24h < 50000:
            continue
        quote_bonus = 1_000_000 if quote_symbol in quote_priority else 0
        candidates.append((quote_bonus + liquidity + volume_24h * 0.2, pair))
    if not candidates:
        return None
    candidates.sort(key=lambda item: item[0], reverse=True)
    return candidates[0][1]


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def trend_state(value: float | None) -> str:
    if value is None:
        return "无方向"
    if value >= 1:
        return "偏强"
    if value <= -1:
        return "偏弱"
    return "中性"


def format_percent_optional(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:+.2f}%"




def signal_trade_levels(signal: Signal) -> str:
    return signal_trade_plan(signal)


def signal_trade_plan(signal: Signal) -> str:
    snapshot = signal.snapshot
    if snapshot is None:
        return "暂无交易计划参考"

    price = snapshot.close_price
    high = snapshot.high_24h or price
    low = snapshot.low_24h or price
    mid = (high + low) / 2
    span = max(high - low, price * 0.01)

    bullish = signal.kind in ("discovery", "bottom_reversal")
    bearish = signal.kind in ("top_risk", "distribution", "top_exhaustion")
    hot = signal.kind == "hot_breakout"

    if hot:
        pullback_low = max(low, price - span * 0.18)
        pullback_high = max(low, price - span * 0.08)
        return (
            f"方向: 过热观察，不建议追高；"
            f"回踩观察区: {pullback_low:.8g}-{pullback_high:.8g}；"
            f"上方阻力: {high:.8g}；"
            f"若15m资金转负或跌回 {price * 0.985:.8g}，热度降级。"
        )

    if bullish:
        entry_high = price
        entry_low = max(low, price - span * (0.10 if signal.kind == "bottom_reversal" else 0.08))
        stop = min(price * 0.985, entry_low - span * 0.04)
        support1 = max(low, entry_low)
        support2 = low
        resistance1 = max(high, price + span * 0.18)
        risk = max(entry_high - stop, price * 0.003)
        tp1 = max(price + risk * 1.2, min(resistance1, price + span * 0.35))
        tp2 = max(price + risk * 2.0, high)
        rr1 = (tp1 - entry_high) / risk
        rr2 = (tp2 - entry_high) / risk
        return (
            f"方向: 看多观察；"
            f"入场区: {entry_low:.8g}-{entry_high:.8g}；"
            f"止损: {stop:.8g}；"
            f"止盈: TP1 {tp1:.8g}({rr1:.1f}R) / TP2 {tp2:.8g}({rr2:.1f}R)；"
            f"支撑: {support1:.8g} / {support2:.8g}；"
            f"阻力: {resistance1:.8g} / {tp2:.8g}。"
        )

    if bearish:
        entry_low = price
        entry_high = min(high, price + span * 0.08)
        stop = max(price * 1.015, entry_high + span * 0.04)
        resistance1 = min(high, entry_high)
        resistance2 = high
        support1 = min(low if low < price else mid, price - span * 0.18)
        risk = max(stop - entry_low, price * 0.003)
        tp1 = min(price - risk * 1.2, max(support1, price - span * 0.35))
        tp2 = min(price - risk * 2.0, low)
        rr1 = (entry_low - tp1) / risk
        rr2 = (entry_low - tp2) / risk
        return (
            f"方向: 看空/减仓观察；"
            f"入场区: {entry_low:.8g}-{entry_high:.8g}；"
            f"止损: {stop:.8g}；"
            f"止盈: TP1 {tp1:.8g}({rr1:.1f}R) / TP2 {tp2:.8g}({rr2:.1f}R)；"
            f"阻力: {resistance1:.8g} / {resistance2:.8g}；"
            f"支撑: {support1:.8g} / {tp2:.8g}。"
        )

    return "暂无交易计划参考"


def market_structure_label(snapshot: MarketSnapshot) -> str:
    high_position = snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75
    low_position = snapshot.price_position_24h is not None and snapshot.price_position_24h <= 35
    flow15 = summary_flow_value(snapshot, "15m")
    flow1h = summary_flow_value(snapshot, "1h")
    taker = snapshot.taker_buy_sell_ratio or 1
    funding = snapshot.funding_rate_percent or 0

    bull_trap = (
        high_position
        and snapshot.price_change_percent >= 2.5
        and snapshot.oi_change_percent >= 5
        and (flow15 < 0 or flow1h < 0 or taker < 1.05 or funding >= 0.03)
    )
    if bull_trap:
        return "疑似诱多: 高位拉升且OI扩张，但资金流/主动买盘/资金费率出现风险。"

    bear_trap = (
        low_position
        and snapshot.price_change_percent <= -2.5
        and funding <= -0.03
        and (flow15 > 0 or taker >= 1.05)
    )
    if bear_trap:
        return "疑似诱空: 低位急跌且资金费率偏负，但短线资金或主动买盘开始回流。"

    washout = (
        snapshot.price_change_percent <= -1.5
        and snapshot.oi_change_percent <= 1.5
        and abs(funding) < 0.08
        and (flow1h >= 0 or flow_alignment_score(snapshot) >= 5)
    )
    if washout:
        return "疑似洗盘: 价格回落但OI没有继续恶化，中期资金未明显破坏。"

    if high_position and snapshot.oi_change_percent >= 8 and taker < 1:
        return "高位分歧: OI继续堆高但主动买盘不足，追多风险较高。"

    if low_position and flow15 > 0 and snapshot.oi_change_percent <= 0:
        return "低位承接: 跌后有短线资金回流，但还需要1h确认。"

    return "暂无明显诱多/诱空/洗盘结构。"


def liquidation_risk_label(snapshot: MarketSnapshot) -> str:
    high_position = snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75
    low_position = snapshot.price_position_24h is not None and snapshot.price_position_24h <= 35
    oi_hot = snapshot.oi_change_percent >= 8
    oi_expanding = snapshot.oi_change_percent >= 3
    funding = snapshot.funding_rate_percent or 0
    funding_hot = snapshot.funding_rate_percent is not None and snapshot.funding_rate_percent >= 0.03
    funding_negative = snapshot.funding_rate_percent is not None and snapshot.funding_rate_percent <= -0.03
    longs_crowded = (
        (snapshot.global_long_short_ratio is not None and snapshot.global_long_short_ratio >= 2)
        or (snapshot.top_position_ratio is not None and snapshot.top_position_ratio >= 2)
    )
    shorts_crowded = (
        (snapshot.global_long_short_ratio is not None and snapshot.global_long_short_ratio <= 0.75)
        or (snapshot.top_account_ratio is not None and snapshot.top_account_ratio <= 0.75)
    )
    taker_weak = snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio < 1
    taker_recover = snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio >= 1.15
    flow15 = summary_flow_value(snapshot, "15m")

    if oi_hot and abs(funding) >= 0.08 and abs(snapshot.price_change_percent) >= 5:
        return (
            "双向高波动: OI快速扩张，资金费率偏极端，价格大幅波动，"
            "上下插针概率都高。"
        )

    if high_position and oi_expanding and (funding_hot or longs_crowded) and (taker_weak or flow15 < 0):
        return (
            "下方扫多风险: 价格处24h高位且OI扩张，多头拥挤/费率偏热，"
            "短线买盘或资金流转弱。"
        )

    if low_position and oi_expanding and (funding_negative or shorts_crowded) and (taker_recover or flow15 > 0):
        return (
            "上方扫空风险: 价格处24h低位且OI扩张，空头拥挤/费率偏负，"
            "短线买盘或资金流回暖。"
        )

    return "暂无明显清算压力: OI、位置、费率和短线资金暂未形成同向挤压。"


def ai_signal_review(signal: Signal) -> str:
    snapshot = signal.snapshot
    if snapshot is None:
        return "暂无快照数据，仅记录信号。"

    positives = []
    risks = []
    flow_score = flow_alignment_score(snapshot)

    if flow_score >= 7:
        positives.append("资金流多周期共振较强")
    elif flow_score >= 5:
        positives.append("资金流中性偏强")
    else:
        risks.append("资金流共振不足")

    if (snapshot.taker_buy_sell_ratio or 0) >= 1.25:
        positives.append("主动买盘较强")
    elif (snapshot.taker_buy_sell_ratio or 0) < 1.05:
        risks.append("主动买盘不强")

    if snapshot.price_position_24h is not None and snapshot.price_position_24h >= 85:
        risks.append("24h位置偏高，追高风险大")
    elif snapshot.price_position_24h is not None and snapshot.price_position_24h <= 35:
        positives.append("位置不高，盈亏比相对更好")

    if snapshot.funding_rate_percent is not None and abs(snapshot.funding_rate_percent) >= 0.3:
        risks.append("资金费率极端，可能有插针波动")

    if signal.kind in ("discovery", "hot_breakout"):
        if snapshot.oi_change_percent >= 4 and snapshot.price_change_percent > 0:
            positives.append("价格和OI同步扩张")
        if signal.kind == "hot_breakout":
            risks.append("已进入过热状态，不适合无脑追高")

        if len(risks) == 0 and len(positives) >= 3:
            decision = "通过，偏多观察"
        elif len(risks) <= 1 and len(positives) >= 2:
            decision = "通过但谨慎，等回踩更稳"
        else:
            decision = "降级观察，暂不追高"
    elif signal.kind in ("top_risk", "distribution"):
        if snapshot.price_change_percent > 0 and snapshot.oi_change_percent > 0:
            positives.append("拉升后杠杆仍在增加")
        if (snapshot.funding_rate_percent or 0) >= 0.03:
            positives.append("资金费率偏热")
        if (snapshot.taker_buy_sell_ratio or 1) < 1:
            positives.append("主动买盘转弱")

        if len(positives) >= 3:
            decision = "风险确认，偏防守"
        elif len(positives) >= 2:
            decision = "风险升高，适合减仓观察"
        else:
            decision = "风险提示，等待确认"
    else:
        decision = "中性观察"

    detail = "；".join((positives + risks)[:4])
    return f"{decision}。依据: {detail or '暂无明显共振'}。"


def signal_priority(signal: Signal, snapshot: MarketSnapshot | None) -> tuple[str, int, str]:
    if snapshot is None:
        score = 50 + (15 if signal.score >= 7 else 8 if signal.score >= 5 else 0)
        priority = priority_from_quality_score(score)
        return priority, score, "no snapshot; base quality only"

    quality_score = 50
    reasons: list[str] = ["base 50"]

    def add(points: int, reason: str) -> None:
        nonlocal quality_score
        quality_score += points
        reasons.append(f"{points:+d} {reason}")

    strength = signal_strength_score(signal)
    flow_score = flow_alignment_score(snapshot)
    long_flow_score = long_flow_alignment_score(snapshot)
    short_score = short_term_score(snapshot)
    mid_score = mid_term_score(snapshot)
    main_score = main_asset_score(snapshot)
    main_total = main_score.total_score if main_score else None
    trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)
    spot_text = cached_spot_alpha_confirmation(snapshot.symbol)

    if signal.score >= 7:
        add(15, "signal.score>=7")
    elif signal.score >= 5:
        add(8, "signal.score>=5")

    if strength >= 30:
        add(10, "strength>=30")
    elif strength >= 20:
        add(5, "strength>=20")

    if flow_score >= 7:
        add(10, "flow_alignment>=7")
    if long_flow_score >= 6:
        add(10, "long_flow_alignment>=6")
    if short_score >= 7:
        add(8, "short_term>=7")
    if mid_score >= 7:
        add(8, "mid_term>=7")
    if main_total is not None and main_total >= 60:
        add(10, "main_asset_score>=60")
    if trap_score <= 2:
        add(8, "trap_risk<=2")
    if "偏强" in spot_text and "偏弱" not in spot_text:
        add(5, "spot/onchain strong")

    if trap_score >= 8:
        add(-30, "trap_risk>=8")
    elif trap_score >= 6:
        add(-20, "trap_risk>=6")
    if long_flow_score <= 3:
        add(-12, "long_flow_alignment<=3")
    if flow_score <= 3:
        add(-10, "flow_alignment<=3")
    if short_score <= 3:
        add(-8, "short_term<=3")
    if mid_score <= 3:
        add(-8, "mid_term<=3")
    if spot_confirmation_is_weak(spot_text):
        add(-8, "spot/onchain weak")
    if signal.kind in ("hot_breakout", "discovery") and long_flow_score <= 3:
        add(-15, "breakout without long-flow support")
    if signal.kind == "bottom_reversal" and ((snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio < 1) or summary_flow_value(snapshot, "1h") < 0):
        add(-12, "bottom_reversal weak taker or 1h flow")
    if signal.kind in ("top_risk", "top_exhaustion") and snapshot.price_position_24h is not None and snapshot.price_position_24h < 40:
        add(-10, "top signal below 40% 24h position")
    if "双向强平" in signal.message or "剧烈洗盘" in signal.message or "双向高波动" in liquidation_risk_label(snapshot):
        add(-10, "two-way liquidation/wash risk")

    quality_score = max(0, min(100, int(round(quality_score))))
    priority = priority_from_quality_score(quality_score)
    capped_priority = capped_signal_priority(priority, signal, snapshot, quality_score, trap_score, flow_score, long_flow_score)
    if capped_priority != priority:
        reasons.append(f"cap {priority}->{capped_priority}")
        priority = capped_priority

    return priority, quality_score, "; ".join(reasons)


def priority_from_quality_score(score: int) -> str:
    if score >= 85:
        return "S"
    if score >= 70:
        return "A"
    if score >= 55:
        return "B"
    if score >= 40:
        return "C"
    return "D"


def capped_signal_priority(
    priority: str,
    signal: Signal,
    snapshot: MarketSnapshot,
    quality_score: int,
    trap_score: int,
    flow_score: int,
    long_flow_score: int,
) -> str:
    order = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 0}
    reverse = {value: key for key, value in order.items()}
    max_priority = priority

    if trap_score >= 8 and not is_major_asset_tier(snapshot.symbol):
        max_priority = min_priority_cap(max_priority, "B", order, reverse)
    trade_plan = signal_trade_plan(signal)
    if signal.kind in ("discovery", "hot_breakout") and ("暂无交易计划" in trade_plan or "入场区" not in trade_plan):
        max_priority = min_priority_cap(max_priority, "B", order, reverse)
    if long_flow_score <= 3 and flow_score <= 3 and signal.score < 8:
        max_priority = min_priority_cap(max_priority, "C", order, reverse)

    return max_priority


def min_priority_cap(priority: str, cap: str, order: dict[str, int], reverse: dict[int, str]) -> str:
    return reverse[min(order.get(priority, 0), order.get(cap, 0))]


def compact_digest_reason(text: str) -> str:
    compact = " ".join(str(text).split())
    return truncate_text(compact, 120)


def format_quality_reason_short(reason: str, max_parts: int = 3) -> str:
    reason_labels = [
        ("trap_risk>=8", "极高诱捕", True),
        ("trap_risk>=6", "高诱捕", True),
        ("long_flow_alignment<=3", "长周期弱", True),
        ("flow_alignment<=3", "资金弱", True),
        ("short_term<=3", "短线弱", True),
        ("mid_term<=3", "中线弱", True),
        ("spot/onchain weak", "现货弱", True),
        ("breakout without long-flow support", "突破缺长周期", True),
        ("hot/discovery long_flow<=3", "突破缺长周期", True),
        ("bottom_reversal weak taker or 1h flow", "抄底买盘弱", True),
        ("bottom weak taker/flow", "抄底买盘弱", True),
        ("top signal below 40% 24h position", "低位做空风险", True),
        ("top signal below 40% position", "低位做空风险", True),
        ("two-way liquidation/wash risk", "洗盘风险", True),
        ("liquidation wash", "洗盘风险", True),
        ("flow_alignment>=7", "资金强", False),
        ("long_flow_alignment>=6", "长周期强", False),
        ("short_term>=7", "短线强", False),
        ("mid_term>=7", "中线强", False),
        ("trap_risk<=2", "低诱捕", False),
        ("strength>=30", "强度高", False),
        ("strength>=20", "强度中", False),
        ("signal.score>=7", "信号分高", False),
        ("signal.score>=5", "信号分中", False),
        ("main_asset_score>=60", "主流强", False),
        ("spot/onchain strong", "现货确认", False),
    ]

    penalties: list[str] = []
    positives: list[str] = []
    seen: set[str] = set()
    for part in str(reason or "").split(";"):
        text = " ".join(part.strip().lower().split())
        if not text or text == "base 50":
            continue
        for key, label, is_penalty in reason_labels:
            if key in text and label not in seen:
                (penalties if is_penalty else positives).append(label)
                seen.add(label)
                break

    labels = (penalties + positives)[:max_parts]
    return "/".join(labels) if labels else "简略通过"


def format_telegram_signal_digest(
    items: list[TelegramSignalDigestItem],
    digest_priorities: list[str],
    interval_minutes: int,
    max_per_priority: int,
    title: str | None = None,
) -> str:
    lines = [title or f"近{interval_minutes}分钟静默信号摘要"]
    counts = {priority: 0 for priority in digest_priorities}
    for item in items:
        if item.priority in counts:
            counts[item.priority] += 1

    for priority in digest_priorities:
        priority_items = [item for item in items if item.priority == priority]
        if not priority_items:
            continue
        lines.append("")
        lines.append(f"{priority}:")
        for item in priority_items[:max_per_priority]:
            main_text = str(item.main_asset_score) if item.main_asset_score is not None else "空"
            lines.append(
                f"{item.symbol} {item.kind} "
                f"q={item.quality_score} trap={item.trap_score} main={main_text} "
                f"score={item.signal_score} strength={item.strength_score:.1f} {item.reason}"
            )

    lines.append("")
    stats = [f"{priority}: {counts[priority]}" for priority in digest_priorities]
    lines.append(f"总静默: {sum(counts.values())}；" + "；".join(stats))
    return truncate_text("\n".join(lines), 3500)


def format_top_quality_counts(counts: dict[str, int], limit: int = 5) -> str:
    if not counts:
        return "-"
    top_items = sorted(counts.items(), key=lambda item: (-item[1], item[0]))[:limit]
    return ", ".join(f"{key} {value}" for key, value in top_items)


def format_priority_set(priorities: set[str]) -> str:
    order = ["S", "A", "B", "C", "D"]
    ordered = [priority for priority in order if priority in priorities]
    ordered.extend(sorted(priority for priority in priorities if priority not in order))
    return "/".join(ordered) if ordered else "-"


def normalize_usdt_symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    return symbol if symbol.endswith("USDT") else f"{symbol}USDT"


def extract_coinglass_judgement(text: str) -> str:
    if not text:
        return ""
    match = re.search(r"判断:\s*([^\n；]+)", text)
    if match:
        return match.group(1).strip()
    first_line = text.splitlines()[0].strip()
    return truncate_text(first_line, 120) if first_line and "n/a" not in first_line else ""


def format_why_signal_row(row: dict[str, str]) -> str:
    quality_score = row.get("signal_quality_score") or "-"
    priority = row.get("signal_priority") or "-"
    reason = compact_digest_reason(row.get("signal_quality_reason", "") or row.get("message", "") or "-")
    time_text = row.get("time", "-").replace("T", " ")[:19]
    return (
        f"{time_text} {row.get('kind', '-')} "
        f"q={priority}/{quality_score} "
        f"suppressed={row.get('suppressed_from_telegram', '-')} "
        f"trap={row.get('trap_risk_score', '-')} "
        f"main={row.get('main_asset_score', '-') or '-'} "
        f"score={row.get('score', '-')} "
        f"strength={format_csv_strength(row.get('strength_score'))} "
        f"{reason}"
    )


def why_symbol_conclusion(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "最近暂无该币信号。"

    high_quality = False
    low_quality_count = 0
    for row in rows:
        priority = (row.get("signal_priority") or "").upper()
        suppressed = str(row.get("suppressed_from_telegram") or "").strip() == "1"
        trap_score = parse_float(row.get("trap_risk_score"))
        if priority in ("A", "S") and (trap_score is None or trap_score <= 5):
            high_quality = True
        if priority in ("C", "D") or suppressed:
            low_quality_count += 1

    if high_quality:
        return "有较高质量信号，可重点盯确认位。"
    if low_quality_count >= max(1, (len(rows) + 1) // 2):
        return "最近信号质量偏低，适合观察不追。"
    return "最近信号质量中性，等待更明确确认。"


def format_signal_for_telegram(
    signal: Signal,
    liquidation_text: str | None = None,
    priority: str | None = None,
    quality_score: int | None = None,
    quality_reason: str | None = None,
) -> str:
    labels = {
        "discovery": ("🟢 [看多]", "发现启动信号"),
        "distribution": ("🟡 [减仓]", "疑似派发"),
        "top_risk": ("🔴 [看空]", "逃顶风险"),
        "hot_breakout": ("🔥 [过热]", "强势过热"),
        "bottom_reversal": ("🟢 [抄底]", "抄底观察"),
        "top_exhaustion": ("🔴 [逃顶]", "逃顶衰竭"),
        "test": ("⚪ [测试]", "测试推送"),
    }
    prefix, label = labels.get(signal.kind, ("⚪ [信号]", signal.kind))
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if priority is None or quality_score is None or quality_reason is None:
        priority, quality_score, quality_reason = signal_priority(signal, signal.snapshot)

    if signal.snapshot is None:
        return (
            f"{prefix} {label}\n\n{signal.title}\n{signal.message}\n"
            f"信号等级: {priority} 质量分 {quality_score} - {quality_reason}\n\n时间: {now}"
        )

    snapshot = signal.snapshot
    reason = {
        "discovery": "价格和 OI 同步上升，主动买盘支持，且多头拥挤度没有过高。",
        "distribution": "价格仍在高位或反弹中，但 OI 下降、主动买盘走弱，可能有减仓派发。",
        "top_risk": "价格快速拉升，同时杠杆和多头拥挤度偏高，追多风险上升。",
        "hot_breakout": "启动动能很强，但多头拥挤或 funding 已过热，适合重点观察，谨慎追高。",
        "bottom_reversal": "超跌后出现资金回流和止跌迹象，属于抄底观察，不是无脑接飞刀。",
        "top_exhaustion": "高位拉升后出现资金或主动买盘衰竭，追多风险高，适合防守。",
    }.get(signal.kind, signal.message)

    strength = signal_strength_label(signal)
    strength_score = signal_strength_score(signal)
    strength_badge = "🚨极强" if strength_score >= 60 else ("⭐强" if strength_score >= 30 else "普通")
    max_score = 7 if signal.kind in ("bottom_reversal", "top_exhaustion") else (5 if signal.kind == "top_risk" else 4)

    return (
        f"{prefix} {label}: {signal.symbol}\n"
        f"方向: {prefix} | 等级: {strength} | {strength_badge}\n\n"
        f"级别: {signal.score}/{max_score}\n"
        f"强度分: {strength_score:.2f} ({strength_grade(strength_score)})\n"
        f"信号等级: {priority} 质量分 {quality_score} - {quality_reason}\n"
        f"价格: {snapshot.close_price:.8g}\n"
        f"价格变化: {snapshot.price_change_percent:+.2f}%\n"
        f"OI变化: {snapshot.oi_change_percent:+.2f}%\n"
        f"全局多空比: {format_optional_value(snapshot.global_long_short_ratio)}\n"
        f"大户持仓多空比: {format_optional_value(snapshot.top_position_ratio)}\n"
        f"大户账户多空比: {format_optional_value(snapshot.top_account_ratio)}\n"
        f"主动买卖比: {format_optional_value(snapshot.taker_buy_sell_ratio)}\n"
        f"Funding: {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})\n"
        f"24h位置: {format_optional_value(snapshot.price_position_24h)}% ({price_position_label(snapshot)}) / 高 {format_optional_value(snapshot.high_24h)} / 低 {format_optional_value(snapshot.low_24h)}\n"
        f"资金流: {format_flow_summary(snapshot)}\n"
        f"资金流共振: {flow_alignment_score(snapshot)}/10 ({flow_alignment_note(flow_alignment_score(snapshot))})\n"
        f"长周期资金共振: {long_flow_alignment_score(snapshot)}/9 ({long_flow_alignment_note(long_flow_alignment_score(snapshot))})\n"
        f"现货/链上确认: {spot_alpha_confirmation(snapshot.symbol)}\n"
        f"短线评分: {short_term_score(snapshot)}/10 ({score_label(short_term_score(snapshot))})\n"
        f"中线评分: {mid_term_score(snapshot)}/10 ({score_label(mid_term_score(snapshot))})\n"
        f"AI共振复核: {ai_signal_review(signal)}\n"
        f"结构判断: {market_structure_label(snapshot)}\n"
        f"清算风险: {liquidation_risk_label(snapshot)}\n"
        f"{liquidation_text or '真实强平: n/a'}\n"
        f"交易计划: {signal_trade_plan(signal)}\n"
        f"判断: {reason}\n"
        f"时间: {now}"
    )


def format_optional_value(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.4g}"




def signal_strength_label(signal: Signal) -> str:
    if signal.score >= 5:
        return "高危"
    if signal.score >= 4:
        return "强信号"
    return "观察"


def hot_watch_score(snapshot: MarketSnapshot) -> float:
    funding = max(snapshot.funding_rate_percent or 0, 0)
    taker = max((snapshot.taker_buy_sell_ratio or 0) - 1, 0)
    crowd = max((snapshot.global_long_short_ratio or 1) - 1.8, 0)
    return snapshot.price_change_percent + snapshot.oi_change_percent + taker + funding * 20 + crowd * 2


def format_hot_watch_for_telegram(snapshots: list[MarketSnapshot], top_n: int) -> str:
    candidates = [
        snapshot for snapshot in snapshots
        if snapshot.price_change_percent > 0 and snapshot.oi_change_percent > 0
    ]
    ordered = sorted(candidates, key=hot_watch_score, reverse=True)[:top_n]
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    lines = ["[HOT WATCH] 强势过热候选", ""]
    if not ordered:
        lines.append("暂无候选。")
    for snapshot in ordered:
        lines.append(
            f"{snapshot.symbol}: score={hot_watch_score(snapshot):+.2f} "
            f"价格={snapshot.price_change_percent:+.2f}% "
            f"OI={snapshot.oi_change_percent:+.2f}% "
            f"LS={format_optional_value(snapshot.global_long_short_ratio)} "
            f"taker={format_optional_value(snapshot.taker_buy_sell_ratio)} "
            f"资金费率={format_optional_value(snapshot.funding_rate_percent)}%"
        )
    lines.extend(["", f"时间: {now}"])
    return "\n".join(lines)


def flow_alignment_score(snapshot: MarketSnapshot | None) -> int:
    if snapshot is None:
        return 0
    weights = {"5m": 1, "15m": 2, "1h": 3, "4h": 4}
    return sum(weight for period, weight in weights.items() if snapshot.net_flow_usd.get(period, 0) > 0)


def long_flow_alignment_score(snapshot: MarketSnapshot | None) -> int:
    if snapshot is None:
        return 0
    weights = {"4h": 2, "12h": 3, "24h": 4}
    return sum(weight for period, weight in weights.items() if snapshot.net_flow_usd.get(period, 0) > 0)


def long_flow_alignment_note(score: int) -> str:
    if score >= 7:
        return "强共振: 长周期资金方向一致"
    if score >= 4:
        return "中性偏强: 长周期资金有支持"
    if score >= 2:
        return "偏弱: 长周期资金分歧"
    return "弱: 长周期资金不支持"


def main_asset_score(
    snapshot: MarketSnapshot,
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
    market_text: str | None = None,
) -> MainAssetScore | None:
    if not is_major_asset_tier(snapshot.symbol):
        return None

    long_flow_score = long_flow_alignment_score(snapshot)
    trend = 20 if long_flow_score >= 7 else (10 if long_flow_score >= 4 else 0)
    flow_1h = snapshot.net_flow_usd.get("1h", 0)
    flow_4h = snapshot.net_flow_usd.get("4h", 0)
    if flow_1h > 0 and flow_4h > 0:
        trend += 10
    elif flow_1h < 0 and flow_4h < 0:
        trend += 0
    else:
        trend += 5
    if snapshot.price_position_24h is not None and 20 <= snapshot.price_position_24h <= 80:
        trend += 5
    trend = min(35, trend)

    oi_score = coinglass_oi_score(coinglass_text)
    taker_score = coinglass_taker_score(coinglass_text, snapshot)
    funds = min(25, oi_score + taker_score)

    derivatives = 0
    funding_extreme = is_extreme_funding(snapshot)
    if not funding_extreme:
        derivatives += 8
    derivatives += liquidation_score(liquidation_text)
    derivatives += crowding_score(snapshot)
    derivatives = min(20, derivatives)

    spot_orderbook = min(
        10,
        spot_confirmation_score(spot_alpha_confirmation(snapshot.symbol))
        + orderbook_confirmation_score(coinglass_text),
    )

    risk_deduction = 0
    risk_label = liquidation_risk_label(snapshot)
    if "双向高波动" in risk_label:
        risk_deduction -= 8
    if funding_extreme:
        risk_deduction -= 5
    if snapshot.oi_change_percent >= 3 and (long_flow_score <= 3 or (flow_1h <= 0 and flow_4h <= 0)):
        risk_deduction -= 5
    if market_text and "大盘风向: 偏弱" in market_text:
        risk_deduction -= 2
    risk_deduction = max(-20, risk_deduction)

    total = max(0, min(100, trend + funds + derivatives + spot_orderbook + risk_deduction))
    label = main_asset_score_label(total, risk_deduction, risk_label)
    components = {
        "趋势": trend,
        "资金": funds,
        "衍生品": derivatives,
        "现货订单簿": spot_orderbook,
        "风险扣分": risk_deduction,
    }
    return MainAssetScore(
        total_score=total,
        label=label,
        components=components,
        note=main_asset_score_note(label, components),
    )


def main_asset_score_label(total: int, risk_deduction: int, risk_label: str) -> str:
    if risk_deduction <= -13 or (total < 25 and "双向高波动" in risk_label):
        return "高风险"
    if total >= 75:
        return "偏强"
    if total >= 60:
        return "中性偏强"
    if total >= 45:
        return "中性"
    if total >= 35:
        return "中性偏弱"
    if total >= 25:
        return "偏弱"
    return "高风险"


def main_asset_score_note(label: str, components: dict[str, int]) -> str:
    if label == "偏强":
        return "趋势、资金与衍生品确认度较高，但仍需控制追高风险。"
    if label == "中性偏强":
        return "主流资金结构略偏多，适合等待回踩或信号确认。"
    if label == "中性":
        return "多空证据暂未充分一致，继续观察确认。"
    if label == "中性偏弱":
        return "资金或订单簿支持不足，反弹持续性需要验证。"
    if label == "偏弱":
        return "趋势与资金确认偏弱，优先防守观察。"
    return "风险项压过正向确认，短线不适合激进开仓。"


def coinglass_oi_score(coinglass_text: str | None) -> int:
    if not coinglass_text:
        return 0
    weights = {"1h": 5, "4h": 6, "24h": 6}
    score = 0
    for period, weight in weights.items():
        value = extract_coinglass_oi_change(coinglass_text, period)
        if value is None:
            continue
        if 0 < value <= 8:
            score += weight
        elif value > 8:
            score += max(1, weight - 2)
        elif value >= -1:
            score += 2
    return min(17, score)


def extract_coinglass_oi_change(text: str, period: str) -> float | None:
    match = re.search(rf"{re.escape(period)}\s*([+\-]?\d+(?:\.\d+)?)%", text)
    if not match:
        return None
    return parse_float(match.group(1))


def coinglass_taker_score(coinglass_text: str | None, snapshot: MarketSnapshot) -> int:
    buy_ratio, sell_ratio = extract_coinglass_taker_ratios(coinglass_text)
    if buy_ratio is not None and sell_ratio is not None:
        if buy_ratio > 52:
            return 8
        if sell_ratio > 52:
            return 0
        return 4
    taker = snapshot.taker_buy_sell_ratio
    if taker is None:
        return 4
    if taker >= 1.08:
        return 8
    if taker <= 0.92:
        return 0
    return 4


def extract_coinglass_taker_ratios(text: str | None) -> tuple[float | None, float | None]:
    if not text:
        return None, None
    match = re.search(
        r"主动买卖\s*24h\s*买\s*([+\-]?\d+(?:\.\d+)?)%\s*/\s*卖\s*([+\-]?\d+(?:\.\d+)?)%",
        text,
    )
    if not match:
        return None, None
    return parse_float(match.group(1)), parse_float(match.group(2))


def is_extreme_funding(snapshot: MarketSnapshot) -> bool:
    return snapshot.funding_rate_percent is not None and abs(snapshot.funding_rate_percent) >= 0.08


def trap_risk_score(snapshot: MarketSnapshot, signal: Signal | None) -> tuple[int, str, str]:
    score = 0
    reasons = []
    position = snapshot.price_position_24h
    high_position = position is not None and position > 75
    low_position = position is not None and position < 25
    flow_1h = summary_flow_value(snapshot, "1h")
    flow_4h = summary_flow_value(snapshot, "4h")
    flow_12h = summary_flow_value(snapshot, "12h")
    taker = snapshot.taker_buy_sell_ratio
    funding = snapshot.funding_rate_percent

    if high_position and (flow_4h <= 0 or flow_12h <= 0):
        score += 2
        reasons.append("高位但4h/12h资金流不支持")
    if low_position and ((taker is not None and taker < 1) or flow_1h <= 0 or flow_4h <= 0):
        score += 2
        reasons.append("低位但主动买盘或1h/4h资金流偏弱")
    if snapshot.oi_change_percent > 10 and taker is not None and taker < 1:
        score += 2
        reasons.append("OI扩张>10%但主动买卖比<1")
    if funding is not None and funding >= 0.08 and high_position:
        score += 2
        reasons.append("极端正Funding叠加高位")
    if funding is not None and funding <= -0.08 and low_position:
        score += 1
        reasons.append("极端负Funding叠加低位")

    long_flow_score = long_flow_alignment_score(snapshot)
    if long_flow_score <= 3:
        score += 2
        reasons.append("长周期资金共振<=3")
    flow_score = flow_alignment_score(snapshot)
    if flow_score <= 3:
        score += 1
        reasons.append("资金流共振<=3")

    spot_text = cached_spot_alpha_confirmation(snapshot.symbol)
    if spot_confirmation_is_weak(spot_text):
        score += 1
        reasons.append("现货/链上确认偏弱")

    structure_text = market_structure_label(snapshot)
    liquidation_text = liquidation_risk_label(snapshot)
    signal_text = f"{signal.title} {signal.message}" if signal else ""
    if any(item in f"{structure_text} {liquidation_text} {signal_text}" for item in ("洗盘", "双向强平/剧烈洗盘")):
        score += 1
        reasons.append("清算/结构提示洗盘风险")

    score = min(score, 10)
    label = trap_risk_label(score)
    reason = "；".join(reasons) if reasons else "暂无明显诱多/诱空过滤项"
    return score, label, reason


def trap_risk_label(score: int) -> str:
    if score <= 2:
        return "低"
    if score <= 5:
        return "中"
    if score <= 7:
        return "高"
    return "极高"


def spot_confirmation_is_weak(text: str) -> bool:
    if not text:
        return False
    return any(item in text for item in ("偏弱", "无标准现货", "无方向"))


def format_trap_risk_line(snapshot: MarketSnapshot, signal: Signal | None = None) -> str:
    score, label, reason = trap_risk_score(snapshot, signal)
    return f"诱多/诱空风险: {score}/10 {label} - {reason}"


def liquidation_score(liquidation_text: str | None) -> int:
    text = liquidation_text or ""
    if any(item in text for item in ("强平分散", "近1h暂无明显强平数据", "暂无明显强平")):
        return 6
    if "双向强平" in text or "剧烈洗盘" in text:
        return 2
    if "多头强平主导" in text or "空头强平主导" in text:
        return 4
    return 6


def crowding_score(snapshot: MarketSnapshot) -> int:
    ratios = [
        snapshot.global_long_short_ratio,
        snapshot.top_position_ratio,
        snapshot.top_account_ratio,
    ]
    usable = [ratio for ratio in ratios if ratio is not None]
    if not usable:
        return 3
    if all(0.75 <= ratio <= 2.0 for ratio in usable):
        return 6
    return 3


def spot_confirmation_score(spot_text: str) -> int:
    if "偏强" in spot_text and "偏弱" not in spot_text:
        return 4
    if "偏弱" in spot_text and "偏强" not in spot_text:
        return 0
    if "偏强" in spot_text:
        return 2
    if "中性" in spot_text:
        return 2
    return 0


def orderbook_confirmation_score(coinglass_text: str | None) -> int:
    if not coinglass_text:
        return 0
    if "下方承接偏强" in coinglass_text:
        return 6
    if "上方卖压偏强" in coinglass_text:
        return 0
    if "均衡" in coinglass_text or "相对均衡" in coinglass_text:
        return 3
    return 0


def format_main_asset_score_line(
    snapshot: MarketSnapshot,
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
    market_text: str | None = None,
) -> str:
    score = main_asset_score(snapshot, liquidation_text, coinglass_text, market_text)
    if score is None:
        return ""
    return f"主流评分: {score.total_score}/100 ({score.label}) - {score.note}"


def format_main_asset_score_detail(
    snapshot: MarketSnapshot,
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
    market_text: str | None = None,
) -> str:
    score = main_asset_score(snapshot, liquidation_text, coinglass_text, market_text)
    if score is None:
        return ""
    components = score.components
    return (
        f"主流评分: {score.total_score}/100 ({score.label})\n"
        f"趋势 {components['趋势']}/35 | 资金 {components['资金']}/25 | "
        f"衍生品 {components['衍生品']}/20 | 现货订单簿 {components['现货订单簿']}/10 | "
        f"风险扣分 {components['风险扣分']}\n"
        f"结论: {score.note}"
    )


def format_flow_summary(snapshot: MarketSnapshot) -> str:
    return " / ".join(
        f"{period} {format_usd(snapshot.net_flow_usd.get(period))}"
        for period in FLOW_PERIODS
    )



def signal_strength_score(signal: Signal) -> float:
    snapshot = signal.snapshot
    if snapshot is None:
        return float(signal.score)

    funding = max(snapshot.funding_rate_percent or 0, 0) * 20
    taker = max((snapshot.taker_buy_sell_ratio or 0) - 1, 0) * 2
    crowd = max((snapshot.global_long_short_ratio or 1) - 1.8, 0) * 2
    confirm_price = max(snapshot.confirm_price_change_percent or 0, 0)
    confirm_oi = max(snapshot.confirm_oi_change_percent or 0, 0)

    base = max(snapshot.price_change_percent, 0) + max(snapshot.oi_change_percent, 0)
    if signal.kind == "top_risk":
        return base + funding + crowd + max(1 - (snapshot.taker_buy_sell_ratio or 1), 0) * 2
    if signal.kind == "distribution":
        return max(snapshot.price_change_percent, 0) + abs(min(snapshot.oi_change_percent, 0)) + max(1 - (snapshot.taker_buy_sell_ratio or 1), 0) * 2
    if signal.kind == "hot_breakout":
        return base + funding + taker + crowd + confirm_price + confirm_oi
    return base + taker + confirm_price + confirm_oi + flow_alignment_score(signal.snapshot) * 0.5


def strength_grade(score: float) -> str:
    if score >= 60:
        return "S级"
    if score >= 30:
        return "A级"
    if score >= 15:
        return "B级"
    return "C级"


SECTOR_MAP = {
    "AI": {"FET", "TAO", "WLD", "RENDER", "AIOT", "COAI", "AIXBT", "ARKM", "NMR", "GRT", "NEAR", "ICP", "VIRTUAL", "SWARMS", "AI", "AIA", "AIGENSYN", "SKYAI", "BLUAI"},
    "MEME": {"DOGE", "SHIB", "PEPE", "BONK", "FLOKI", "PENGU", "PNUT", "WIF", "TRUMP", "NEIRO", "RATS", "FARTCOIN", "BULLA", "GIGGLE", "PIPPIN"},
    "L1/L2": {"BTC", "ETH", "SOL", "BNB", "SUI", "AVAX", "APT", "ARB", "OP", "SEI", "TIA", "TON", "DOT", "ATOM", "NEAR", "ALGO", "TRX", "XTZ", "CFX", "BERA", "LINEA", "ZK", "ZRO", "STRK"},
    "DeFi": {"AAVE", "UNI", "CRV", "PENDLE", "LDO", "ENA", "CAKE", "COMP", "DYDX", "JTO", "JUP", "KNC", "ORCA", "RUNE", "SYRUP"},
    "RWA": {"ONDO", "PENDLE", "ENA", "PLUME", "HIFI", "TOKEN", "POLYX"},
    "Game/Meta": {"GALA", "SAND", "APE", "AXS", "MANA", "ENJ", "MAGIC", "PIXEL", "YGG", "PORTAL", "BIGTIME"},
    "Privacy": {"XMR", "ZEC", "ZEN", "DASH"},
    "Storage": {"FIL", "AR", "STORJ"},
    "Payment": {"XRP", "XLM", "LTC", "BCH", "TRX"},
    "New/Hot": {"LAB", "TAG", "TAC", "JCT", "XNY", "XPIN", "BIOUS", "NAORIS", "IRYS", "PLAY", "COAI", "AIN", "UB", "B2"},
}


def sector_for_symbol(symbol: str) -> str:
    base = base_symbol(symbol)
    for sector, members in SECTOR_MAP.items():
        if base in members:
            return sector
    return "Other"


def sector_stats(snapshots: list[MarketSnapshot]) -> list[dict[str, Any]]:
    groups: dict[str, list[MarketSnapshot]] = {}
    for snapshot in snapshots:
        groups.setdefault(sector_for_symbol(snapshot.symbol), []).append(snapshot)

    rows = []
    for sector, items in groups.items():
        if sector == "Other":
            continue
        total = len(items)
        avg_price = sum(item.price_change_percent for item in items) / total
        avg_oi = sum(item.oi_change_percent for item in items) / total
        flow15_ratio = sum(1 for item in items if summary_flow_value(item, "15m") > 0) / total
        flow1h_ratio = sum(1 for item in items if summary_flow_value(item, "1h") > 0) / total
        hot_count = sum(1 for item in items if is_summary_hot(item) or is_summary_discovery(item))
        risk_count = sum(1 for item in items if is_summary_top_risk(item) or is_summary_distribution(item))
        leader = max(items, key=sector_leader_score)
        score = avg_price * 2 + avg_oi + flow15_ratio * 5 + flow1h_ratio * 4 + hot_count * 1.5 - risk_count * 1.0
        rows.append({
            "sector": sector,
            "count": total,
            "avg_price": avg_price,
            "avg_oi": avg_oi,
            "flow15_ratio": flow15_ratio,
            "flow1h_ratio": flow1h_ratio,
            "hot_count": hot_count,
            "risk_count": risk_count,
            "leader": leader,
            "score": score,
        })
    return rows


def sector_leader_score(snapshot: MarketSnapshot) -> float:
    return (
        max(snapshot.price_change_percent, 0)
        + max(snapshot.oi_change_percent, 0) * 0.7
        + max((snapshot.taker_buy_sell_ratio or 0) - 1, 0) * 2
        + flow_alignment_score(snapshot) * 0.6
        - max((snapshot.funding_rate_percent or 0) - 0.05, 0) * 20
    )


def format_sector_row(row: dict[str, Any]) -> str:
    leader = row["leader"]
    if leader.price_change_percent > 0 or leader.oi_change_percent > 0:
        leader_text = f"{leader.symbol}({leader.price_change_percent:+.2f}%, OI {leader.oi_change_percent:+.2f}%)"
    else:
        leader_text = "无明显龙头"
    return (
        f"{row['sector']}: score={row['score']:+.2f} "
        f"均涨={row['avg_price']:+.2f}% 均OI={row['avg_oi']:+.2f}% "
        f"15m流入={row['flow15_ratio'] * 100:.0f}% 1h流入={row['flow1h_ratio'] * 100:.0f}% "
        f"龙头={leader_text} "
        f"风险={row['risk_count']}"
    )


def format_sectors_for_telegram(snapshots: list[MarketSnapshot], detail: bool = False) -> str:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = sector_stats(snapshots)
    if not rows:
        return f"[SECTORS] 板块热度\n\n暂无板块数据\n时间: {now}"

    hot = sorted(rows, key=lambda row: row["score"], reverse=True)[:5]
    cold = sorted(rows, key=lambda row: row["score"])[:5]

    lines = ["[SECTORS] 热点/冷门板块", ""]
    lines.append("热点板块:")
    lines.extend(format_sector_row(row) for row in hot)
    lines.append("")
    lines.append("冷门板块:")
    lines.extend(format_sector_row(row) for row in cold)

    if detail:
        lines.append("")
        lines.append("说明: score综合均涨、OI、资金流、启动数量和风险数量；龙头按价格/OI/资金流共振排序。")

    lines.append("")
    lines.append(f"时间: {now}")
    return "\n".join(lines)


def format_sector_brief_for_summary(snapshots: list[MarketSnapshot]) -> str:
    rows = sector_stats(snapshots)
    if not rows:
        return "-"
    hot = sorted(rows, key=lambda row: row["score"], reverse=True)[:3]
    cold = sorted(rows, key=lambda row: row["score"])[:3]
    lines = ["热点板块:"]
    lines.extend(format_sector_row(row) for row in hot)
    lines.append("冷门板块:")
    lines.extend(format_sector_row(row) for row in cold)
    return "\n".join(lines)


def format_regime_for_telegram(snapshots: list[MarketSnapshot]) -> str:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not snapshots:
        return f"[REGIME] 市场大方向\n\n暂无快照数据\n时间: {now}"

    total = len(snapshots)
    core_symbols = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
    core = [item for item in snapshots if item.symbol in core_symbols]
    alts = [item for item in snapshots if item.symbol not in core_symbols]

    up_ratio = sum(1 for item in snapshots if item.price_change_percent > 0) / total
    oi_up_ratio = sum(1 for item in snapshots if item.oi_change_percent > 0) / total
    flow15_ratio = sum(1 for item in snapshots if summary_flow_value(item, "15m") > 0) / total
    flow1h_ratio = sum(1 for item in snapshots if summary_flow_value(item, "1h") > 0) / total
    taker_ratio = sum(1 for item in snapshots if (item.taker_buy_sell_ratio or 0) >= 1.1) / total
    funding_hot_ratio = sum(1 for item in snapshots if (item.funding_rate_percent or 0) >= 0.03) / total

    core_score = regime_group_score(core)
    alt_score = regime_group_score(alts)
    market_score = (
        up_ratio * 22
        + oi_up_ratio * 20
        + flow15_ratio * 20
        + flow1h_ratio * 18
        + taker_ratio * 15
        - funding_hot_ratio * 10
        + core_score * 0.25
        + alt_score * 0.15
    )
    market_score = max(0.0, min(100.0, market_score))

    core_lines = format_regime_core_lines(core)
    alt_relative = alt_score - core_score
    if alt_relative >= 8:
        alt_state = "山寨强于核心，适合重点观察启动扩散"
    elif alt_relative <= -8:
        alt_state = "山寨弱于核心，追小币要更谨慎"
    else:
        alt_state = "山寨与核心差异不大，市场仍偏轮动"

    label = regime_label(market_score)
    strategy = regime_strategy(label, alt_relative)

    lines = [
        "[REGIME] 市场大方向",
        "",
        f"总体状态: {label} ({market_score:.1f}/100)",
        f"上涨占比: {up_ratio * 100:.1f}% | OI扩张: {oi_up_ratio * 100:.1f}%",
        f"15m净流入: {flow15_ratio * 100:.1f}% | 1h净流入: {flow1h_ratio * 100:.1f}%",
        f"主动买入偏强: {taker_ratio * 100:.1f}% | Funding过热: {funding_hot_ratio * 100:.1f}%",
        "",
        "核心资产:",
        core_lines,
        "",
        f"山寨相对强弱: {alt_state}",
        f"核心评分: {core_score:.1f} | 山寨评分: {alt_score:.1f}",
        "",
        "策略建议:",
        strategy,
        "",
        f"时间: {now}",
    ]
    return "\n".join(lines)


def regime_group_score(items: list[MarketSnapshot]) -> float:
    if not items:
        return 50.0
    total = len(items)
    up = sum(1 for item in items if item.price_change_percent > 0) / total
    oi = sum(1 for item in items if item.oi_change_percent > 0) / total
    flow15 = sum(1 for item in items if summary_flow_value(item, "15m") > 0) / total
    flow1h = sum(1 for item in items if summary_flow_value(item, "1h") > 0) / total
    taker = sum(1 for item in items if (item.taker_buy_sell_ratio or 0) >= 1.1) / total
    avg_price = sum(item.price_change_percent for item in items) / total
    avg_oi = sum(item.oi_change_percent for item in items) / total
    raw = up * 25 + oi * 20 + flow15 * 20 + flow1h * 20 + taker * 10 + max(min(avg_price + avg_oi * 0.5, 5), -5)
    return max(0.0, min(100.0, raw))


def format_regime_core_lines(core: list[MarketSnapshot]) -> str:
    if not core:
        return "-"
    ordered = sorted(core, key=lambda item: item.symbol)
    lines = []
    for item in ordered:
        state = "偏强" if regime_group_score([item]) >= 60 else ("偏弱" if regime_group_score([item]) < 40 else "中性")
        lines.append(
            f"{item.symbol}: {state} price={item.price_change_percent:+.2f}% "
            f"OI={item.oi_change_percent:+.2f}% 15m={format_usd(summary_flow_value(item, '15m'))} "
            f"1h={format_usd(summary_flow_value(item, '1h'))}"
        )
    return "\n".join(lines)


def regime_label(score: float) -> str:
    if score >= 72:
        return "偏多"
    if score >= 58:
        return "震荡偏多"
    if score >= 42:
        return "中性震荡"
    if score >= 28:
        return "震荡偏空"
    return "风险偏高"


def regime_strategy(label: str, alt_relative: float) -> str:
    if label == "偏多":
        return "允许正常观察启动信号，但高位过热仍需等回踩。"
    if label == "震荡偏多":
        return "只优先看资金流共振强、现货/链上确认不弱的启动。"
    if label == "中性震荡":
        return "降低追多频率，重点看强度分高的信号和回踩确认。"
    if label == "震荡偏空":
        return "启动信号降级处理，优先看逃顶风险和减仓信号。"
    return "市场风险偏高，减少追多，重点防守和等待新一轮确认。"


def format_summary_for_telegram(snapshots: list[MarketSnapshot], top_n: int) -> str:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if not snapshots:
        return f"[SUMMARY] 市场温度摘要\n\n暂无快照数据\n时间: {now}"

    total = len(snapshots)
    up_count = sum(1 for item in snapshots if item.price_change_percent > 0)
    oi_up_count = sum(1 for item in snapshots if item.oi_change_percent > 0)
    taker_buy_count = sum(1 for item in snapshots if (item.taker_buy_sell_ratio or 0) >= 1.1)
    crowded_count = sum(1 for item in snapshots if (item.global_long_short_ratio or 0) >= 2.0)
    hot_funding_count = sum(1 for item in snapshots if (item.funding_rate_percent or 0) >= 0.03)
    flow_15m_positive = sum(1 for item in snapshots if summary_flow_value(item, "15m") > 0)
    flow_1h_positive = sum(1 for item in snapshots if summary_flow_value(item, "1h") > 0)

    discovery_candidates = [item for item in snapshots if is_summary_discovery(item)]
    hot_candidates = [item for item in snapshots if is_summary_hot(item)]
    top_risk_candidates = [item for item in snapshots if is_summary_top_risk(item)]
    distribution_candidates = [item for item in snapshots if is_summary_distribution(item)]

    avg_price = sum(item.price_change_percent for item in snapshots) / total
    avg_oi = sum(item.oi_change_percent for item in snapshots) / total
    temperature = market_temperature_score(snapshots)

    hot_set = {item.symbol for item in hot_candidates}
    risk_set = {item.symbol for item in top_risk_candidates}
    discovery_set = {item.symbol for item in discovery_candidates}
    distribution_set = {item.symbol for item in distribution_candidates}

    hot_leaders = sorted(hot_candidates, key=lambda item: discovery_score(item) + top_risk_score(item), reverse=True)
    ordered = sorted(
        [item for item in snapshots if item.symbol not in hot_set and item.symbol not in risk_set and item.symbol not in distribution_set],
        key=discovery_score,
        reverse=True,
    )
    flow_leaders = sorted(snapshots, key=lambda item: summary_flow_value(item, "15m"), reverse=True)
    oi_leaders = sorted(snapshots, key=lambda item: item.oi_change_percent, reverse=True)
    risk_leaders = sorted(
        [item for item in top_risk_candidates if item.symbol not in hot_set and item.symbol not in discovery_set],
        key=top_risk_score,
        reverse=True,
    )
    distribution_leaders = sorted(distribution_candidates, key=lambda item: abs(item.oi_change_percent) + abs(min(summary_flow_value(item, "15m"), 0)) / 1_000_000, reverse=True)

    sections = [
        "[SUMMARY] 市场温度摘要 v2",
        "",
        f"市场温度: {temperature:.1f}/100 ({market_temperature_label(temperature)})",
        f"监控币数: {total}",
        f"上涨占比: {up_count}/{total} ({up_count / total * 100:.1f}%)",
        f"OI扩张: {oi_up_count}/{total} ({oi_up_count / total * 100:.1f}%)",
        f"主动买入偏强: {taker_buy_count}/{total} ({taker_buy_count / total * 100:.1f}%)",
        f"15m净流入: {flow_15m_positive}/{total} ({flow_15m_positive / total * 100:.1f}%)",
        f"1h净流入: {flow_1h_positive}/{total} ({flow_1h_positive / total * 100:.1f}%)",
        f"多头拥挤: {crowded_count}/{total} | Funding过热: {hot_funding_count}/{total}",
        f"平均涨跌: {avg_price:+.2f}% | 平均OI: {avg_oi:+.2f}%",
        "",
        market_方向_summary(snapshots),
        "",
        f"信号候选: 启动 {len(discovery_candidates)} / 强势过热 {len(hot_candidates)} / 逃顶风险 {len(top_risk_candidates)} / 派发 {len(distribution_candidates)}",
        "",
        format_sector_brief_for_summary(snapshots),
        "",
        "🔥 强势过热榜:",
        format_snapshot_lines(hot_leaders[:top_n], include_score=True, include_risk=True),
        "",
        "🟢 最接近启动:",
        format_snapshot_lines(ordered[:top_n], include_score=True),
        "",
        "🟢 15m资金净流入榜:",
        format_snapshot_lines(flow_leaders[:top_n], include_flow=True),
        "",
        "🟢 OI 增长榜:",
        format_snapshot_lines(oi_leaders[:top_n]),
        "",
        "🔴 逃顶风险榜:",
        format_snapshot_lines(risk_leaders[:top_n], include_risk=True),
        "",
        "🟡 疑似派发榜:",
        format_snapshot_lines(distribution_leaders[:top_n], include_flow=True),
        "",
        f"时间: {now}",
    ]
    return "\n".join(sections)


def market_方向_summary(snapshots: list[MarketSnapshot]) -> str:
    by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
    majors = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    lines = []
    strength = 0

    for symbol in majors:
        snapshot = by_symbol.get(symbol)
        if snapshot is None:
            lines.append(f"{symbol}: 无数据")
            continue

        看空 = short_term_score(snapshot)
        mid = mid_term_score(snapshot)
        if 看空 >= 6:
            strength += 1
        if mid >= 6:
            strength += 1
        if summary_flow_value(snapshot, "15m") > 0:
            strength += 1
        if summary_flow_value(snapshot, "1h") > 0:
            strength += 1

        lines.append(
            f"{symbol}: 短线{score_label(看空)} {看空}/10, "
            f"中线{score_label(mid)} {mid}/10, "
            f"15m {format_usd(summary_flow_value(snapshot, '15m'))}, "
            f"1h {format_usd(summary_flow_value(snapshot, '1h'))}, "
            f"位置{format_optional_value(snapshot.price_position_24h)}%"
        )

    if strength >= 9:
        env = "strong"
    elif strength >= 6:
        env = "bullish"
    elif strength >= 3:
        env = "neutral"
    else:
        env = "weak"

    return "大盘风向: " + market_regime_display_label(env) + "\n" + "\n".join(lines)


def market_regime_display_label(value: str) -> str:
    labels = {
        "neutral": "中性",
        "bullish": "偏强",
        "strong": "偏强",
        "bearish": "偏弱",
        "weak": "偏弱",
    }
    return labels.get(value, value)


def market_temperature_score(snapshots: list[MarketSnapshot]) -> float:
    if not snapshots:
        return 0.0
    total = len(snapshots)
    up_ratio = sum(1 for item in snapshots if item.price_change_percent > 0) / total
    oi_ratio = sum(1 for item in snapshots if item.oi_change_percent > 0) / total
    taker_ratio = sum(1 for item in snapshots if (item.taker_buy_sell_ratio or 0) >= 1.1) / total
    flow_ratio = sum(1 for item in snapshots if summary_flow_value(item, "15m") > 0) / total
    avg_price = sum(item.price_change_percent for item in snapshots) / total
    avg_oi = sum(item.oi_change_percent for item in snapshots) / total
    base = up_ratio * 25 + oi_ratio * 25 + taker_ratio * 20 + flow_ratio * 20
    momentum = max(min(avg_price * 2 + avg_oi, 10), -10)
    return max(0.0, min(100.0, base + momentum))


def market_temperature_label(score: float) -> str:
    if score >= 75:
        return "过热"
    if score >= 60:
        return "偏热"
    if score >= 40:
        return "中性"
    if score >= 25:
        return "偏冷"
    return "冰点"


def summary_flow_value(snapshot: MarketSnapshot, period: str) -> float:
    return float((snapshot.net_flow_usd or {}).get(period) or 0)


def is_summary_discovery(snapshot: MarketSnapshot) -> bool:
    return (
        snapshot.price_change_percent >= 1.2
        and snapshot.oi_change_percent >= 4
        and (snapshot.taker_buy_sell_ratio or 0) >= 1.15
        and summary_flow_value(snapshot, "15m") > 0
    )


def is_summary_top_risk(snapshot: MarketSnapshot) -> bool:
    crowd_hot = (snapshot.global_long_short_ratio or 0) >= 2.0
    funding_hot = (snapshot.funding_rate_percent or 0) >= 0.03
    return snapshot.price_change_percent >= 3 and snapshot.oi_change_percent >= 8 and (crowd_hot or funding_hot)


def is_summary_hot(snapshot: MarketSnapshot) -> bool:
    return is_summary_discovery(snapshot) and is_summary_top_risk(snapshot)


def is_summary_distribution(snapshot: MarketSnapshot) -> bool:
    return (
        snapshot.price_change_percent >= 2
        and snapshot.oi_change_percent <= -3
        and (snapshot.taker_buy_sell_ratio or 99) <= 0.9
        and summary_flow_value(snapshot, "15m") < 0
    )


def discovery_score(snapshot: MarketSnapshot) -> float:
    taker = min(snapshot.taker_buy_sell_ratio or 0, 3.0)
    crowd_penalty = max((snapshot.global_long_short_ratio or 1) - 1.8, 0) * 2
    flow_bonus = 0
    if summary_flow_value(snapshot, "15m") > 0:
        flow_bonus += 2
    if summary_flow_value(snapshot, "1h") > 0:
        flow_bonus += 2
    return snapshot.price_change_percent + snapshot.oi_change_percent + max(taker - 1, 0) * 2 + flow_bonus - crowd_penalty


def top_risk_score(snapshot: MarketSnapshot) -> float:
    crowd = max((snapshot.global_long_short_ratio or 1) - 1.5, 0) * 4
    funding = max((snapshot.funding_rate_percent or 0) - 0.02, 0) * 100
    taker_weak = max(1 - (snapshot.taker_buy_sell_ratio or 1), 0) * 3
    return snapshot.price_change_percent + snapshot.oi_change_percent + crowd + funding + taker_weak


def snapshot_direction_marker(snapshot: MarketSnapshot) -> str:
    if is_summary_hot(snapshot):
        return "🔥"
    if is_summary_distribution(snapshot):
        return "🟡"
    if is_summary_top_risk(snapshot):
        return "🔴"
    if is_summary_discovery(snapshot):
        return "🟢"
    if summary_flow_value(snapshot, "15m") > 0 and snapshot.oi_change_percent > 0:
        return "🟢"
    if summary_flow_value(snapshot, "15m") < 0 and snapshot.price_change_percent > 0:
        return "🟡"
    return "⚪"


def format_snapshot_lines(
    snapshots: list[MarketSnapshot],
    include_score: bool = False,
    include_flow: bool = False,
    include_risk: bool = False,
) -> str:
    if not snapshots:
        return "-"
    lines = []
    for snapshot in snapshots:
        score = f" score={discovery_score(snapshot):+.2f}" if include_score else ""
        risk = f" risk={top_risk_score(snapshot):+.2f}" if include_risk else ""
        flow = ""
        if include_flow:
            flow = f" flow15m={format_usd(summary_flow_value(snapshot, '15m'))} flow1h={format_usd(summary_flow_value(snapshot, '1h'))}"
        marker = snapshot_direction_marker(snapshot)
        lines.append(
            f"{marker} {snapshot.symbol}: 价格={snapshot.price_change_percent:+.2f}% "
            f"OI={snapshot.oi_change_percent:+.2f}% "
            f"LS={format_optional_value(snapshot.global_long_short_ratio)} "
            f"taker={format_optional_value(snapshot.taker_buy_sell_ratio)} "
            f"资金费率={format_optional_value(snapshot.funding_rate_percent)}%{flow}{score}{risk}"
        )
    return "\n".join(lines)



def split_chat_ids(chat_ids: str) -> list[str]:
    return [chat_id.strip() for chat_id in str(chat_ids).split(",") if chat_id.strip()]


def parse_priority_list(value: Any, default: tuple[str, ...]) -> list[str]:
    if not isinstance(value, (list, tuple, set)):
        return list(default)
    priorities = []
    for item in value:
        priority = str(item).strip().upper()
        if priority and priority not in priorities:
            priorities.append(priority)
    return priorities or list(default)


def parse_positive_int(value: Any, default: int) -> int:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def truncate_text(text: str, limit: int = 3500) -> str:
    if len(text) <= limit:
        return text
    suffix = "\n...已截断"
    return text[: max(0, limit - len(suffix))] + suffix


def run_dev_command(args: list[str], timeout: int = 20) -> tuple[bool, str]:
    try:
        completed = subprocess.run(
            args,
            cwd="/opt/crypto-monitor",
            shell=False,
            text=True,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return False, f"命令超时（{timeout}s）"
    except FileNotFoundError:
        return False, "命令不存在"
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"

    output = "\n".join(part.strip() for part in (completed.stdout, completed.stderr) if part and part.strip())
    output = output.strip()
    if completed.returncode != 0:
        message = output or f"退出码 {completed.returncode}"
        return False, truncate_text(message, 1200)
    return True, output


def run_dev_compile_checks() -> tuple[bool, str]:
    files = ["derivatives_monitor.py", "backtest_signals.py"]
    lines = []
    all_ok = True
    for filename in files:
        ok, output = run_dev_command(["/opt/crypto-monitor/.venv/bin/python", "-m", "py_compile", filename], timeout=30)
        if ok:
            lines.append(f"{filename}: OK")
        else:
            all_ok = False
            lines.append(f"{filename}: FAILED\n{output}")
    return all_ok, "\n".join(lines)


def format_systemctl_status(output: str) -> str:
    active = "-"
    main_pid = "-"
    memory = "-"
    started_at = "-"

    for raw_line in output.splitlines():
        line = raw_line.strip()
        if line.startswith("Active:"):
            active = line.removeprefix("Active:").strip()
            marker = " since "
            if marker in active:
                before_since, after_since = active.split(marker, 1)
                active = before_since.strip()
                started_at = after_since.split(";", 1)[0].strip()
        elif line.startswith("Main PID:"):
            main_pid = line.removeprefix("Main PID:").strip()
        elif line.startswith("Memory:"):
            memory = line.removeprefix("Memory:").strip()

    status = "运行中" if "active (running)" in active else active
    return "\n".join(
        [
            "crypto-monitor 状态:",
            f"Active: {status}",
            f"PID: {main_pid}",
            f"Memory: {memory}",
            f"Started: {started_at}",
        ]
    )


def format_dev_deploy_summary(pull_output: str, compile_summary: str, service_status: str) -> str:
    return truncate_text(
        "\n\n".join(
            [
                "部署完成。",
                f"Pull 输出:\n{pull_output.strip() or '(无输出)'}",
                f"编译结果:\n{compile_summary}",
                f"服务状态:\n{service_status}",
            ]
        ),
        3500,
    )


def dev_help_text() -> str:
    return (
        "DevOps 命令:\n"
        "/dev status - 查看服务状态摘要\n"
        "/dev logs - 查看最近 30 行日志\n"
        "/dev git - 查看工作区和最近提交\n"
        "/dev backtest - 执行最近 80 条信号回测\n"
        "/dev restart - 生成重启确认码\n"
        "/dev confirm restart <code> - 确认重启服务\n"
        "/dev deploy - 生成部署确认码\n"
        "/dev confirm deploy <code> - 确认部署最新 main 并重启服务\n"
        "/dev help - 查看帮助"
    )


def telegram_help_text() -> str:
    return (
        "单币:\n"
        " /check SYMBOL - 单币诊断\n"
        " /ask SYMBOL - AI简洁复核\n"
        " /ask SYMBOL full - AI完整上下文\n"
        " /liq [SYMBOL] - 强平流状态/单币强平\n\n"
        "市场:\n"
        " /summary - 市场温度摘要\n"
        " /regime - 市场大方向\n"
        " /sectors - 热点/冷门板块\n\n"
        "信号:\n"
        " /hot - 强势过热候选\n"
        " /signals - 最近信号\n"
        " /quality - 信号质量统计\n"
        " /digest now - 查看当前静默摘要\n"
        " /topq - 高质量信号排行\n"
        " /quiet status|normal|strict|ultra - 临时调整实时推送等级\n"
        " /why SYMBOL - 快速解释最近信号质量\n"
        " /top - 强度最高信号\n"
        " /review - 最近10条信号\n"
        " /perf - 最近信号表现\n\n"
        "运维:\n"
        " /dev help - 运维命令入口"
    )


def backtest_long_flow_sample_warning(output: str) -> bool:
    if "[LONG FLOW]" not in output:
        return False
    if "样本=0" in output:
        return True
    return bool(re.search(r"longFlow[^\n]*(?:样本|samples?)\D{0,8}[0-5]\b", output, re.IGNORECASE))


def open_binance_force_order_socket() -> ssl.SSLSocket:
    raw_sock = socket.create_connection((BINANCE_FORCE_ORDER_WS_HOST, 443), timeout=10)
    sock = ssl.create_default_context().wrap_socket(raw_sock, server_hostname=BINANCE_FORCE_ORDER_WS_HOST)
    key = base64.b64encode(secrets.token_bytes(16)).decode("ascii")
    request = (
        f"GET {BINANCE_FORCE_ORDER_WS_PATH} HTTP/1.1\r\n"
        f"Host: {BINANCE_FORCE_ORDER_WS_HOST}\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Key: {key}\r\n"
        "Sec-WebSocket-Version: 13\r\n"
        "User-Agent: crypto-monitor/1.0\r\n"
        "\r\n"
    )
    sock.sendall(request.encode("ascii"))
    response = read_http_response(sock)
    header_text = response.decode("iso-8859-1", errors="replace")
    if " 101 " not in header_text.split("\r\n", 1)[0]:
        raise ConnectionError(f"Unexpected WebSocket handshake response: {header_text.splitlines()[0]}")

    accept = ""
    for line in header_text.split("\r\n")[1:]:
        if line.lower().startswith("sec-websocket-accept:"):
            accept = line.split(":", 1)[1].strip()
            break
    expected_accept = base64.b64encode(
        hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")).digest()
    ).decode("ascii")
    if accept != expected_accept:
        raise ConnectionError("Invalid WebSocket accept header from Binance")
    sock.settimeout(None)
    return sock


def read_http_response(sock: ssl.SSLSocket) -> bytes:
    chunks = []
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = sock.recv(4096)
        if not chunk:
            break
        chunks.append(chunk)
        data = b"".join(chunks)
        if len(data) > 65536:
            raise ConnectionError("WebSocket handshake response too large")
    return data


def recv_exact(sock: ssl.SSLSocket, size: int) -> bytes:
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("WebSocket connection closed")
        data += chunk
    return data


def websocket_read_frame(sock: ssl.SSLSocket) -> tuple[int, bytes] | None:
    header = recv_exact(sock, 2)
    first, second = header
    opcode = first & 0x0F
    masked = bool(second & 0x80)
    length = second & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(sock, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(sock, 8))[0]

    mask = recv_exact(sock, 4) if masked else b""
    payload = recv_exact(sock, length) if length else b""
    if masked:
        payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    if opcode == 0x8:
        return None
    return opcode, payload


def websocket_send_frame(sock: ssl.SSLSocket, opcode: int, payload: bytes = b"") -> None:
    first = 0x80 | opcode
    length = len(payload)
    if length < 126:
        header = struct.pack("!BB", first, 0x80 | length)
    elif length <= 0xFFFF:
        header = struct.pack("!BBH", first, 0x80 | 126, length)
    else:
        header = struct.pack("!BBQ", first, 0x80 | 127, length)
    mask = secrets.token_bytes(4)
    masked_payload = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
    sock.sendall(header + mask + masked_payload)



def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as file:
        config = yaml.safe_load(file) or {}
    if not config.get("symbols"):
        raise ValueError("Config must include at least one symbol under 'symbols'.")
    return config


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor futures OI, 看多/看空 ratio, and taker flow signals.")
    parser.add_argument("-c", "--config", default="derivatives_config.yaml", help="Path to YAML config file.")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging.")
    parser.add_argument("--once", action="store_true", help="Run one scan and print the current signal table.")
    parser.add_argument("--no-refresh", action="store_true", help="Use config symbols without refreshing the screener.")
    parser.add_argument("--test-telegram", action="store_true", help="Send a Telegram test message and exit.")
    parser.add_argument("--symbol", help="Diagnose one futures symbol, for example SIGNUSDT.")
    args = parser.parse_args()

    configure_logging(args.verbose)
    config = load_config(args.config)
    monitor = DerivativesMonitor(config)
    if args.test_telegram:
        monitor.send_telegram_test()
        return 0
    if args.symbol:
        snapshot = monitor.fetch_snapshot(args.symbol.upper())
        signals = monitor.evaluate_snapshot(snapshot, {"mode": "both"})
        combined_signal = monitor.combined_signal(snapshot, signals)
        if combined_signal:
            signals.append(combined_signal)
        liquidation_text = monitor.format_liquidation_stats(args.symbol.upper())
        coinglass_text = monitor.format_coinglass_market_context(args.symbol.upper())
        print_symbol_diagnosis(snapshot, signals, liquidation_text, coinglass_text)
        return 0
    if args.once:
        results = monitor.run_once(refresh_symbols=not args.no_refresh)
        print_scan_results(results)
        return 0

    monitor.run_forever()
    return 0


def print_scan_results(results: list[tuple[MarketSnapshot, list[Signal]]]) -> None:
    print("symbol,price_change_pct,oi_change_pct,global_ls,top_position_ls,top_account_ls,taker_buy_sell,signals")
    for snapshot, signals in results:
        signal_names = "|".join(signal.kind for signal in signals) or "-"
        print(
            f"{snapshot.symbol},"
            f"{snapshot.price_change_percent:+.2f},"
            f"{snapshot.oi_change_percent:+.2f},"
            f"{DerivativesMonitor.format_optional(snapshot.global_long_short_ratio)},"
            f"{DerivativesMonitor.format_optional(snapshot.top_position_ratio)},"
            f"{DerivativesMonitor.format_optional(snapshot.top_account_ratio)},"
            f"{DerivativesMonitor.format_optional(snapshot.taker_buy_sell_ratio)},"
            f"{signal_names}"
        )
        for signal in signals:
            print(f"  {signal.title}: {signal.message}")


def format_symbol_diagnosis(
    snapshot: MarketSnapshot,
    signals: list[Signal],
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
) -> str:
    signal_names = ", ".join(signal.kind for signal in signals) or "-"
    major_long_text = format_major_long_cycle_context(snapshot, coinglass_text)
    major_long_part = f"{major_long_text}\n" if major_long_text else ""
    main_score_text = format_main_asset_score_detail(snapshot, liquidation_text, coinglass_text)
    main_score_part = f"{main_score_text}\n" if main_score_text else ""
    return (
        f"{snapshot.symbol}\n"
        f"价格: {snapshot.close_price:.8g}\n"
        f"价格变化: {snapshot.price_change_percent:+.2f}%\n"
        f"OI变化: {snapshot.oi_change_percent:+.2f}%\n"
        f"全局多空比: {format_optional_value(snapshot.global_long_short_ratio)}\n"
        f"大户持仓多空比: {format_optional_value(snapshot.top_position_ratio)}\n"
        f"大户账户多空比: {format_optional_value(snapshot.top_account_ratio)}\n"
        f"主动买卖比: {format_optional_value(snapshot.taker_buy_sell_ratio)}\n"
        f"Funding: {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})\n"
        f"资金流: {format_flow_summary(snapshot)}\n"
        f"资金流共振: {flow_alignment_score(snapshot)}/10 ({flow_alignment_note(flow_alignment_score(snapshot))})\n"
        f"长周期资金共振: {long_flow_alignment_score(snapshot)}/9 ({long_flow_alignment_note(long_flow_alignment_score(snapshot))})\n"
        f"现货/链上确认: {spot_alpha_confirmation(snapshot.symbol)}\n"
        f"短线评分: {short_term_score(snapshot)}/10 ({score_note(short_term_score(snapshot))})\n"
        f"中线评分: {mid_term_score(snapshot)}/10 ({score_note(mid_term_score(snapshot))})\n"
        f"{main_score_part}"
        f"信号: {signal_names}\n"
        f"结构判断: {market_structure_label(snapshot)}\n"
        f"清算风险: {liquidation_risk_label(snapshot)}\n"
        f"{liquidation_text or '真实强平: n/a'}\n"
        f"{major_long_part}"
        f"{coinglass_text or 'CoinGlass聚合: n/a'}\n"
        f"判断: {diagnose_snapshot(snapshot, signals)}"
    )


def format_ask_context(
    snapshot: MarketSnapshot,
    signals: list[Signal],
    market_snapshots: list[MarketSnapshot],
    recent_signal_rows: list[dict[str, str]],
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
) -> str:
    now = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    signal_text = format_ask_signal_list(signals)
    trade_plan = signal_trade_plan(signals[0]) if signals else "暂无交易计划"
    recent_text = format_recent_symbol_signals(recent_signal_rows)
    market_text = market_方向_summary(market_snapshots) if market_snapshots else "大盘风向: 暂无 BTC/ETH/SOL 快照"
    flow_score = flow_alignment_score(snapshot)
    long_flow_score = long_flow_alignment_score(snapshot)
    spot_text = spot_alpha_confirmation(snapshot.symbol)
    trap_score, trap_label, trap_reason = trap_risk_score(snapshot, signals[0] if signals else None)
    system_direction = diagnose_snapshot(snapshot, signals)
    triggered_signal_state = "有触发信号" if signals else "无触发信号"
    available_levels = (
        f"当前价格 {snapshot.close_price:.8g}；"
        f"24h高 {format_optional_value(snapshot.high_24h)}；"
        f"24h低 {format_optional_value(snapshot.low_24h)}；"
        f"交易计划 {trade_plan}；"
        f"结构判断 {market_structure_label(snapshot)}；"
        f"清算风险 {liquidation_risk_label(snapshot)}；"
        f"{liquidation_text or '真实强平: n/a'}；"
        f"{coinglass_text or 'CoinGlass聚合: n/a'}"
    )
    major_long_text = format_major_long_cycle_context(snapshot, coinglass_text)
    major_long_part = f"{major_long_text}\n" if major_long_text else ""
    main_score_text = format_main_asset_score_detail(snapshot, liquidation_text, coinglass_text, market_text)
    main_score_part = f"{main_score_text}\n" if main_score_text else ""

    text = (
        f"[ASK] {snapshot.symbol} 结构化上下文\n"
        f"时间: {now}\n\n"
        "系统优先结论:\n"
        f"综合判断: {system_direction}\n"
        f"信号触发状态: {triggered_signal_state}\n"
        f"短线评分: {short_term_score(snapshot)}/10 ({score_note(short_term_score(snapshot))})\n"
        f"中线评分: {mid_term_score(snapshot)}/10 ({score_note(mid_term_score(snapshot))})\n"
        f"诱多/诱空风险: {trap_score}/10 {trap_label} - {trap_reason}\n"
        f"{main_score_part}"
        f"大盘风向: {market_text}\n\n"
        "基础数据:\n"
        f"当前价格: {snapshot.close_price:.8g}\n"
        f"价格变化: {snapshot.price_change_percent:+.2f}%\n"
        f"OI变化: {snapshot.oi_change_percent:+.2f}%\n"
        f"24h位置: {format_optional_value(snapshot.price_position_24h)}% ({price_position_label(snapshot)})\n"
        f"24h高低: {format_optional_value(snapshot.high_24h)} / {format_optional_value(snapshot.low_24h)}\n\n"
        "衍生品情绪:\n"
        f"全局多空比: {format_optional_value(snapshot.global_long_short_ratio)}\n"
        f"大户持仓多空比: {format_optional_value(snapshot.top_position_ratio)}\n"
        f"大户账户多空比: {format_optional_value(snapshot.top_account_ratio)}\n"
        f"主动买卖比: {format_optional_value(snapshot.taker_buy_sell_ratio)}\n"
        f"Funding: {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})\n\n"
        "资金流:\n"
        f"5m: {format_usd(snapshot.net_flow_usd.get('5m'))} / ratio {format_optional_value(snapshot.net_flow_ratio.get('5m'))}\n"
        f"15m: {format_usd(snapshot.net_flow_usd.get('15m'))} / ratio {format_optional_value(snapshot.net_flow_ratio.get('15m'))}\n"
        f"1h: {format_usd(snapshot.net_flow_usd.get('1h'))} / ratio {format_optional_value(snapshot.net_flow_ratio.get('1h'))}\n"
        f"4h: {format_usd(snapshot.net_flow_usd.get('4h'))} / ratio {format_optional_value(snapshot.net_flow_ratio.get('4h'))}\n"
        f"12h: {format_usd(snapshot.net_flow_usd.get('12h'))} / ratio {format_optional_value(snapshot.net_flow_ratio.get('12h'))}\n"
        f"24h: {format_usd(snapshot.net_flow_usd.get('24h'))} / ratio {format_optional_value(snapshot.net_flow_ratio.get('24h'))}\n"
        f"资金流共振: {flow_score}/10 ({flow_alignment_note(flow_score)})\n"
        f"长周期资金共振: {long_flow_score}/9 ({long_flow_alignment_note(long_flow_score)})\n\n"
        f"现货/链上确认: {spot_text}\n"
        f"诱多/诱空风险: {trap_score}/10 {trap_label} - {trap_reason}\n"
        f"结构判断: {market_structure_label(snapshot)}\n"
        f"清算风险: {liquidation_risk_label(snapshot)}\n"
        f"{liquidation_text or '真实强平: n/a'}\n"
        f"{major_long_part}"
        f"{coinglass_text or 'CoinGlass聚合: n/a'}\n"
        f"短线评分: {short_term_score(snapshot)}/10 ({score_note(short_term_score(snapshot))})\n"
        f"中线评分: {mid_term_score(snapshot)}/10 ({score_note(mid_term_score(snapshot))})\n"
        f"综合判断: {system_direction}\n\n"
        "信号列表:\n"
        f"{signal_text}\n\n"
        "交易计划:\n"
        f"{trade_plan}\n\n"
        "确认/失效可用价位来源:\n"
        f"{available_levels}\n\n"
        "最近该币信号:\n"
        f"{recent_text}"
    )
    return truncate_text(text, 3500)


def ask_ai_review(context_text: str) -> str | None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    model = os.getenv("OPENAI_MODEL", "gpt-4.1-mini")
    prompt = (
        "你是中文加密货币衍生品交易系统的严格复核员，不是自由分析师。"
        "你只基于用户提供的结构化上下文复核，不读取、不要求、不推断任何 API key、token 或系统环境变量。"
        "这不是投资建议，只是交易复盘和风险检查。"
        "最高优先级: 必须优先服从系统上下文里的综合判断、信号列表、交易计划、结构判断、清算风险、短线评分、中线评分、大盘风向。"
        "如果系统上下文包含'信号触发状态: 无触发信号'，置信度最高只能是'中'，不得写'高'。"
        "如果系统上下文包含'交易计划: 暂无交易计划'或'交易计划: 暂无交易计划参考'，置信度最高只能是'中'，不得写'高'。"
        "如果'长周期资金共振'小于等于3/9，置信度最高只能是'中'，不得写'高'。"
        "如果短线评分大于等于7但长周期资金共振小于等于3/9，必须明确写: '短线强但长周期不支持，可能是假反弹/诱多观察，不属于启动确认。'"
        "如果大盘风向偏弱且单币短线偏强，必须写成风险: '大盘偏弱会压制单币反弹持续性。'"
        "如果真实强平为'多头强平主导'，必须解释: '多头被清较多，说明短线下跌压力/止损释放；除非资金回流和结构止跌，否则不能直接抄底。'"
        "如果真实强平为'空头强平主导'，必须解释: '空头被清较多，说明短线逼空/上冲压力释放；除非资金继续承接，否则不能直接追多。'"
        "如果真实强平为'双向强平/剧烈洗盘'，必须解释: '上下波动都剧烈，适合观望等待结构确认。'"
        "如果真实强平为'强平分散'、'强平活跃但方向分散'或'近1h暂无明显强平数据'，不得把清算作为方向确认依据。"
        "如果信号列表为'暂无触发信号'或信号触发状态为'无触发信号'，不得强行给看多/看空，只能写观望、偏观望或等待确认。"
        "解释规则: OI下降+价格下跌，多为仓位退出/风险释放，不等于新空进场；OI上升+价格上涨，多为空头/多头博弈加剧，需结合主动买卖比和资金流；极端负Funding表示空头成本高、空头拥挤，可能反抽/插针，但不能直接作为做多依据，绝不能写成多头成本高；极端正Funding表示多头成本高、多头拥挤，追多风险高，不能直接作为做空依据；资金流多周期分歧时，必须降置信度；现货/链上与合约背离时，必须写成风险。"
        "禁止编造系统上下文没有的支撑、阻力、清算带、目标价。确认条件和失效条件里的价格位只能来自当前价格、24h高低、交易计划、结构判断、清算风险；没有可用价位就写'上下文无明确价位'。"
        "如果交易计划为'暂无交易计划'或'暂无交易计划参考'，操作倾向必须包含'暂无交易计划，不建议按 AI 文本直接开仓'。"
        "固定输出格式，且只输出这些字段: [AI复核]\n系统方向:\nAI复核结论:\n置信度:\n核心理由:\n- 最多3条\n主要风险:\n- 最多3条\n操作倾向:"
        "置信度只能用低/中/高。全文控制在1200字以内，中文直接，偏交易复盘风格。"
    )
    payload = {
        "model": model,
        "input": [
            {
                "role": "system",
                "content": [{"type": "input_text", "text": prompt}],
            },
            {
                "role": "user",
                "content": [{"type": "input_text", "text": context_text}],
            },
        ],
        "max_output_tokens": 900,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            "https://api.openai.com/v1/responses",
            headers=headers,
            json=payload,
            timeout=20,
        )
        response.raise_for_status()
        data = response.json()
    except Exception:
        logging.warning("OpenAI ask review failed; falling back to structured context", exc_info=True)
        return None

    text = extract_openai_response_text(data)
    if not text:
        logging.warning("OpenAI ask review returned no text; falling back to structured context")
        return None
    return truncate_text(post_process_ask_ai_review(text.strip(), context_text), 1200)


def extract_openai_response_text(data: dict[str, Any]) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text

    parts: list[str] = []
    for item in data.get("output", []) if isinstance(data.get("output"), list) else []:
        if not isinstance(item, dict):
            continue
        content = item.get("content", [])
        if not isinstance(content, list):
            continue
        for content_item in content:
            if not isinstance(content_item, dict):
                continue
            text = content_item.get("text") or content_item.get("output_text")
            if isinstance(text, str) and text.strip():
                parts.append(text.strip())
    return "\n".join(parts)


def format_ask_response(
    context_text: str,
    ai_review: str | None,
    snapshot: MarketSnapshot,
    signals: list[Signal],
    market_snapshots: list[MarketSnapshot],
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
    full: bool = False,
) -> str:
    review_text = (ai_review or fallback_ask_ai_review(context_text, snapshot, signals, market_snapshots, liquidation_text)).strip()
    review_text = post_process_ask_ai_review(review_text, context_text)
    review_text = normalize_ai_review_text(review_text)
    if not full:
        entry_advice = format_ask_entry_advice(snapshot, signals)
        return fix_ask_orderbook_parenthesis(truncate_text(
            f"{review_text}\n开仓建议: {entry_advice}\n\n{format_ask_core_data(snapshot, liquidation_text, coinglass_text)}",
            1500,
        ))

    prefix = f"{review_text}\n\n[系统上下文]\n" if review_text.startswith("[AI复核]") else f"[AI复核]\n{review_text}\n\n[系统上下文]\n"
    remaining = max(0, 3500 - len(prefix))
    return prefix + truncate_text(context_text, remaining)


def fallback_ask_ai_review(
    context_text: str,
    snapshot: MarketSnapshot,
    signals: list[Signal],
    market_snapshots: list[MarketSnapshot],
    liquidation_text: str | None = None,
) -> str:
    system_direction = diagnose_snapshot(snapshot, signals)
    trade_plan = signal_trade_plan(signals[0]) if signals else "暂无交易计划"
    short_score = short_term_score(snapshot)
    long_flow_score = long_flow_alignment_score(snapshot)
    market_text = market_方向_summary(market_snapshots) if market_snapshots else ""
    confidence = "中" if ask_confidence_capped_at_medium(context_text) else "高"
    if not signals or long_flow_score <= 3 or "暂无交易计划" in trade_plan:
        confidence = "中"

    reasons = [
        f"短线评分 {short_score}/10，中线评分 {mid_term_score(snapshot)}/10。",
        f"资金流共振 {flow_alignment_score(snapshot)}/10，长周期资金共振 {long_flow_score}/9。",
        f"系统信号触发状态: {'有触发信号' if signals else '无触发信号'}。",
    ]
    risks = []
    if short_score >= 7 and long_flow_score <= 3:
        risks.append("短线强但长周期不支持，可能是假反弹/诱多观察，不属于启动确认。")
    if "大盘风向: 偏弱" in market_text and short_score >= 7:
        risks.append("大盘偏弱会压制单币反弹持续性。")
    liquidation_explanation = ask_liquidation_explanation(liquidation_text or "")
    if liquidation_explanation:
        risks.append(liquidation_explanation)
    if not risks:
        risks.append("资金、结构或清算未形成足够一致的启动确认。")

    conclusion = "偏观望，等待系统信号和交易计划确认。" if not signals else "按系统方向复核，通过前仍需控制仓位。"
    operation = trade_plan
    if "暂无交易计划" in trade_plan:
        operation = "暂无交易计划，不建议按 AI 文本直接开仓。"

    return (
        "[AI复核]\n"
        f"系统方向: {system_direction}\n"
        f"AI复核结论: {conclusion}\n"
        f"置信度: {confidence}\n"
        "核心理由:\n"
        + "\n".join(f"- {item}" for item in reasons[:3])
        + "\n主要风险:\n"
        + "\n".join(f"- {item}" for item in risks[:3])
        + f"\n操作倾向: {operation}"
    )


def format_ask_core_data(
    snapshot: MarketSnapshot,
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
) -> str:
    flow_items = []
    for period in ["5m", "15m", "1h", "4h", "12h"]:
        flow_items.append(
            f"{period} {format_usd(snapshot.net_flow_usd.get(period))}/r{format_optional_value(snapshot.net_flow_ratio.get(period))}"
        )
    liq_text = compact_liquidation_text(liquidation_text or "真实强平: n/a")
    major_long_text = format_major_long_cycle_one_line(snapshot, coinglass_text)
    major_long_part = f"{major_long_text}\n" if major_long_text else ""
    orderbook_text = compact_coinglass_orderbook_context(snapshot, coinglass_text)
    orderbook_part = f"订单簿: {orderbook_text}\n" if orderbook_text else ""
    main_score_text = format_main_asset_score_line(snapshot, liquidation_text, coinglass_text)
    main_score_part = f"{main_score_text}\n" if main_score_text else ""
    spot_text = spot_alpha_confirmation(snapshot.symbol)
    trap_score, trap_label, trap_reason = trap_risk_score(snapshot, None)
    coinglass_part = ""
    if not is_major_asset_tier(snapshot.symbol):
        coinglass_part = f"CoinGlass: {compact_coinglass_market_context(coinglass_text or 'CoinGlass聚合: n/a')}"
    return (
        "[核心数据]\n"
        f"价格/OI/Funding: {snapshot.close_price:.8g}; 价格 {snapshot.price_change_percent:+.2f}%; "
        f"OI {snapshot.oi_change_percent:+.2f}%; Funding {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})\n"
        f"评分: 短线 {short_term_score(snapshot)}/10; 中线 {mid_term_score(snapshot)}/10; "
        f"资金流共振 {flow_alignment_score(snapshot)}/10; 长周期资金共振 {long_flow_alignment_score(snapshot)}/9\n"
        f"诱多/诱空风险: {trap_score}/10 {trap_label} - {trap_reason}\n"
        f"{main_score_part}"
        f"资金: {'; '.join(flow_items)}\n"
        f"长周期: {long_flow_alignment_note(long_flow_alignment_score(snapshot))}\n"
        f"现货/链上: {spot_text}\n"
        f"清算推断: {liquidation_risk_label(snapshot)}\n"
        f"真实强平: {liq_text}\n"
        f"{major_long_part}"
        f"{orderbook_part}"
        f"{coinglass_part}"
    )


def compact_liquidation_text(text: str) -> str:
    compact = " ".join(str(text).split())
    compact = compact.replace("真实强平: ", "")
    return truncate_text(compact, 260)


def format_ask_entry_advice(snapshot: MarketSnapshot, signals: list[Signal]) -> str:
    trade_plan = signal_trade_plan(signals[0]) if signals else "暂无交易计划"
    if signals and "暂无交易计划" not in trade_plan:
        advice = "按交易计划观察"
    else:
        advice = "不开仓，只观察"
    if long_flow_alignment_score(snapshot) <= 3 and "长周期不支持" not in advice:
        advice = f"{advice}，长周期不支持"
    return advice


def format_ask_data_source_text(data_source_text: str, full: bool = False) -> str:
    mode_text = "完整" if full else "简洁"
    return f"{data_source_text} | 模式: {mode_text}"


def ask_stale_cache_note(data_source_text: str) -> str | None:
    match = re.search(r"数据来源:\s*缓存\s*(\d+)秒前", data_source_text)
    if match and int(match.group(1)) > TELEGRAM_SNAPSHOT_CACHE_TTL_SECONDS:
        return "注意: 缓存偏旧"
    return None


def ask_confidence_capped_at_medium(context_text: str) -> bool:
    if "信号触发状态: 无触发信号" in context_text:
        return True
    if "交易计划:\n暂无交易计划" in context_text or "交易计划: 暂无交易计划" in context_text:
        return True
    match = re.search(r"长周期资金共振:\s*(\d+)/9", context_text)
    return bool(match and int(match.group(1)) <= 3)


def post_process_ask_ai_review(review_text: str, context_text: str) -> str:
    text = review_text.strip()
    text = fix_negative_funding_cost_explanation(text)
    text = fix_liquidation_dominance_explanation(text, context_text)
    if ask_confidence_capped_at_medium(context_text):
        text = re.sub(r"(置信度:\s*)高", r"\1中", text)
    short_score = ask_context_score(context_text, "短线评分")
    long_flow_score = ask_context_score(context_text, "长周期资金共振")
    if short_score is not None and short_score >= 7 and long_flow_score is not None and long_flow_score <= 3:
        text = append_ask_bullet(
            text,
            "主要风险:",
            "短线强但长周期不支持，可能是假反弹/诱多观察，不属于启动确认。",
        )
    if "大盘风向: 偏弱" in context_text and short_score is not None and short_score >= 7:
        text = append_ask_bullet(text, "主要风险:", "大盘偏弱会压制单币反弹持续性。")
    liquidation_explanation = ask_liquidation_explanation(context_text)
    if liquidation_explanation:
        text = append_ask_bullet(text, "主要风险:", liquidation_explanation)
    if "交易计划:\n暂无交易计划" in context_text or "交易计划: 暂无交易计划" in context_text:
        required = "暂无交易计划，不建议按 AI 文本直接开仓。"
        if required not in text:
            text = append_to_ask_field(text, "操作倾向:", required)
    return text


def fix_negative_funding_cost_explanation(text: str) -> str:
    pattern = (
        r"(?:极端负\s*Funding|负\s*Funding|极端负费率|负费率)"
        r"[^。；\n]*(?:多头成本高|多头成本较高|多头持仓成本高|多头持仓成本较高)"
        r"[^。；\n]*"
    )
    return re.sub(pattern, "极端负Funding显示空头成本高/空头拥挤", text, flags=re.IGNORECASE)


def fix_liquidation_dominance_explanation(text: str, context_text: str) -> str:
    if "空头强平主导" not in context_text:
        return text
    correct = "空头被清较多，说明短线逼空/上冲压力释放；除非资金继续承接，否则不能直接追多。"
    bad_keywords = ("回踩压力", "下跌压力", "卖压", "抛压")
    fixed_lines = []
    for line in text.splitlines():
        if "空头强平主导" in line and any(keyword in line for keyword in bad_keywords):
            prefix = "- " if line.startswith("- ") else ""
            fixed_lines.append(f"{prefix}{correct}")
        else:
            fixed_lines.append(line)
    return "\n".join(fixed_lines)


def fix_ask_orderbook_parenthesis(text: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith("订单簿:") and line.count("（") > line.count("）"):
            lines[index] = f"{line}）"
    return "\n".join(lines)


def compact_coinglass_market_context(text: str) -> str:
    first_line = str(text).splitlines()[0] if str(text).splitlines() else str(text)
    return first_line.removeprefix("CoinGlass聚合: ").strip()


def compact_coinglass_orderbook_context(snapshot: MarketSnapshot, coinglass_text: str | None) -> str:
    if not is_major_asset_tier(snapshot.symbol):
        return ""
    orderbook_text = extract_labeled_segment(coinglass_text, "CoinGlass订单簿: ", "\n")
    if not orderbook_text:
        return "n/a"
    orderbook_text = orderbook_text.removeprefix("CoinGlass订单簿: ").strip()
    match = re.search(r"近1h\s*买盘([^/；]+)\s*/\s*卖盘([^；]+).*判断:\s*([^；\n]+)", orderbook_text)
    if not match:
        judgement_match = re.search(r"判断:\s*([^；\n]+)", orderbook_text)
        return compact_orderbook_judgement(judgement_match.group(1).strip()) if judgement_match else "n/a"
    bids = match.group(1).strip()
    asks = match.group(2).strip()
    judgement = compact_orderbook_judgement(match.group(3).strip())
    return f"{judgement}（1h买{bids}/卖{asks}）"


def compact_orderbook_judgement(judgement: str) -> str:
    if "下方承接偏强" in judgement:
        return "下方承接偏强"
    if "上方卖压偏强" in judgement:
        return "上方卖压偏强"
    if "均衡" in judgement or "相对均衡" in judgement:
        return "均衡"
    return judgement


def normalize_ai_review_text(text: str) -> str:
    text = limit_short_ai_review_bullets(text, "核心理由:", 3)
    text = limit_short_ai_review_bullets(text, "主要风险:", 3)
    return keep_first_short_ai_review_warning(text)


def limit_short_ai_review_bullets(text: str, field: str, max_items: int) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if not line.startswith(field):
            continue
        end = len(lines)
        for next_index in range(index + 1, len(lines)):
            if short_ai_review_field_header(lines[next_index]):
                end = next_index
                break
        kept = []
        bullet_count = 0
        for section_line in lines[index + 1 : end]:
            if section_line.startswith("- "):
                bullet_count += 1
                if bullet_count > max_items:
                    continue
            kept.append(section_line)
        return "\n".join(lines[: index + 1] + kept + lines[end:])
    return text


def short_ai_review_field_header(line: str) -> bool:
    return bool(re.match(r"^[^\s\-][^:：]{0,20}[:：]", line))


def keep_first_short_ai_review_warning(text: str) -> str:
    warning = "暂无交易计划，不建议按 AI 文本直接开仓"
    matches = list(re.finditer(re.escape(warning) + "。?", text))
    if len(matches) <= 1:
        return text
    parts = []
    cursor = 0
    for index, match in enumerate(matches):
        if index == 0:
            continue
        parts.append(text[cursor : match.start()])
        cursor = match.end()
    parts.append(text[cursor:])
    return "".join(parts)


def ask_context_score(context_text: str, label: str) -> int | None:
    match = re.search(rf"{re.escape(label)}:\s*(\d+)/", context_text)
    return int(match.group(1)) if match else None


def append_to_ask_field(text: str, field: str, addition: str) -> str:
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith(field):
            if addition not in line:
                lines[index] = f"{line} {addition}"
            return "\n".join(lines)
    return f"{text}\n{field} {addition}"


def append_ask_bullet(text: str, field: str, addition: str) -> str:
    if addition in text:
        return text
    lines = text.splitlines()
    for index, line in enumerate(lines):
        if line.startswith(field):
            lines.insert(index + 1, f"- {addition}")
            return "\n".join(lines)
    return f"{text}\n{field}\n- {addition}"


def ask_liquidation_explanation(liquidation_text: str) -> str | None:
    if "多头强平主导" in liquidation_text:
        return "多头被清较多，说明短线下跌压力/止损释放；除非资金回流和结构止跌，否则不能直接抄底。"
    if "空头强平主导" in liquidation_text:
        return "空头被清较多，说明短线逼空/上冲压力释放；除非资金继续承接，否则不能直接追多。"
    if "双向强平/剧烈洗盘" in liquidation_text:
        return "上下波动都剧烈，适合观望等待结构确认。"
    if "强平分散" in liquidation_text or "近1h暂无明显强平数据" in liquidation_text:
        return "清算不能作为方向确认依据。"
    return None


def format_ask_signal_list(signals: list[Signal]) -> str:
    if not signals:
        return "暂无触发信号"

    lines = []
    for signal in signals:
        lines.append(
            f"- {signal.kind}: score={signal.score} 强度={signal_strength_score(signal):.2f} "
            f"({strength_grade(signal_strength_score(signal))}) - {signal.message}"
        )
    return "\n".join(lines)


def format_recent_symbol_signals(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "最近暂无该币信号记录"

    lines = []
    for row in rows:
        time_text = row.get("time", "-").replace("T", " ")[:19]
        lines.append(
            f"- {time_text} {row.get('kind', '-')} "
            f"score={row.get('score', '-')} 强度={format_csv_strength(row.get('strength_score'))} "
            f"价格={format_csv_number(row.get('price_change_percent'))}% "
            f"OI={format_csv_number(row.get('oi_change_percent'))}%"
        )
    return "\n".join(lines)


def print_symbol_diagnosis(
    snapshot: MarketSnapshot,
    signals: list[Signal],
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
) -> None:
    signal_names = ", ".join(signal.kind for signal in signals) or "-"
    print(snapshot.symbol)
    print(f"价格: {snapshot.close_price:.8g}")
    print(f"价格变化: {snapshot.price_change_percent:+.2f}%")
    print(f"OI变化: {snapshot.oi_change_percent:+.2f}%")
    print(f"全局多空比: {format_optional_value(snapshot.global_long_short_ratio)}")
    print(f"大户持仓多空比: {format_optional_value(snapshot.top_position_ratio)}")
    print(f"大户账户多空比: {format_optional_value(snapshot.top_account_ratio)}")
    print(f"主动买卖比: {format_optional_value(snapshot.taker_buy_sell_ratio)}")
    print(f"Funding: {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})")
    print(f"24h位置: {format_optional_value(snapshot.price_position_24h)}% ({position_note(snapshot)}) / 高 {format_optional_value(snapshot.high_24h)} / 低 {format_optional_value(snapshot.low_24h)}")
    print(f"24h成交额: {format_usd(snapshot.quote_volume_24h or 0)} / 成交额OI比: {format_optional_value(snapshot.volume_ratio_24h)} ({volume_note(snapshot)})")
    print(f"资金流: {format_flow_summary(snapshot)}")
    print(f"资金流共振: {flow_alignment_score(snapshot)}/10 ({flow_alignment_note(flow_alignment_score(snapshot))})")
    print(f"长周期资金共振: {long_flow_alignment_score(snapshot)}/9 ({long_flow_alignment_note(long_flow_alignment_score(snapshot))})")
    print(f"现货/链上确认: {spot_alpha_confirmation(snapshot.symbol)}")
    print(f"短线评分: {short_term_score(snapshot)}/10 ({score_note(short_term_score(snapshot))})")
    print(f"中线评分: {mid_term_score(snapshot)}/10 ({score_note(mid_term_score(snapshot))})")
    main_score_text = format_main_asset_score_detail(snapshot, liquidation_text, coinglass_text)
    if main_score_text:
        print(main_score_text)
    print(f"信号: {signal_names}")
    print(f"结构判断: {market_structure_label(snapshot)}")
    print(f"清算风险: {liquidation_risk_label(snapshot)}")
    print(liquidation_text or "真实强平: n/a")
    major_long_text = format_major_long_cycle_context(snapshot, coinglass_text)
    if major_long_text:
        print(major_long_text)
    print(coinglass_text or "CoinGlass聚合: n/a")
    print(f"判断: {diagnose_snapshot(snapshot, signals)}")


def diagnose_snapshot(snapshot: MarketSnapshot, signals: list[Signal]) -> str:
    kinds = {signal.kind for signal in signals}
    if "hot_breakout" in kinds:
        return "强势启动但已经过热，适合重点观察，谨慎追高。"
    if "discovery" in kinds:
        return "价格、OI 和主动买盘同步增强，有启动迹象。"
    if "top_risk" in kinds:
        return "价格和 OI 已明显拉升，且存在拥挤或 funding 过热，追多风险较高。"
    if "distribution" in kinds:
        return "价格仍在高位但 OI 和主动买盘走弱，疑似派发。"
    if snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio < 1:
        return "暂无做多信号，主动买盘偏弱，等待价格和 OI 同步放量。"
    if snapshot.oi_change_percent < 1:
        return "暂无明确信号，OI 未明显进场。"
    return "暂无触发信号，继续观察。"


if __name__ == "__main__":
    sys.exit(main())
