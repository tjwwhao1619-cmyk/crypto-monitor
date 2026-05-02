import argparse
import concurrent.futures
import csv
import datetime as dt
import json
import logging
import os
import secrets
import shutil
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
import yaml


BINANCE_FAPI_BASE = "https://fapi.binance.com"
BINANCE_FUTURES_DATA_BASE = "https://fapi.binance.com/futures/data"
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"


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
    "net_flow_5m_ratio",
    "net_flow_15m_ratio",
    "net_flow_1h_ratio",
    "net_flow_4h_ratio",
    "price_position_24h",
    "high_24h",
    "low_24h",
    "quote_volume_24h",
    "volume_ratio_24h",
    "short_term_score",
    "mid_term_score",
    "flow_alignment_score",
    "structure_label",
    "trade_plan",
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
        self.pending_dev_confirmations: dict[str, tuple[str, float]] = {}

    def run_forever(self) -> None:
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
        periods = self.flow_config.get("periods", ["5m", "15m", "1h", "4h"])

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
            f"4h资金流={format_usd(snapshot.net_flow_usd.get('4h'))}."
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
        if not self.should_push_signal(signal):
            logging.info("Signal logged but not pushed: %s 强度=%.2f", signal.title, signal_strength_score(signal))
            return
        self.notify(signal)

    def log_signal(self, signal: Signal) -> None:
        snapshot = signal.snapshot
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
            "net_flow_5m_ratio": snapshot.net_flow_ratio.get("5m", "") if snapshot else "",
            "net_flow_15m_ratio": snapshot.net_flow_ratio.get("15m", "") if snapshot else "",
            "net_flow_1h_ratio": snapshot.net_flow_ratio.get("1h", "") if snapshot else "",
            "net_flow_4h_ratio": snapshot.net_flow_ratio.get("4h", "") if snapshot else "",
            "price_position_24h": snapshot.price_position_24h if snapshot else "",
            "high_24h": snapshot.high_24h if snapshot else "",
            "low_24h": snapshot.low_24h if snapshot else "",
            "quote_volume_24h": snapshot.quote_volume_24h if snapshot else "",
            "volume_ratio_24h": snapshot.volume_ratio_24h if snapshot else "",
            "short_term_score": short_term_score(snapshot) if snapshot else "",
            "mid_term_score": mid_term_score(snapshot) if snapshot else "",
            "flow_alignment_score": flow_alignment_score(snapshot) if snapshot else "",
            "structure_label": market_structure_label(snapshot) if snapshot else "",
            "trade_plan": signal_trade_plan(signal) if snapshot else "",
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
            for chat_id in split_chat_ids(chat_ids):
                self.send_telegram(bot_token, chat_id, signal)

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

    def send_telegram(self, bot_token: str, chat_id: str, signal: Signal) -> None:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": format_signal_for_telegram(signal)}
        self.post_json(url, payload)

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
                "可用命令:\n/symbol SIGNUSDT - 单币诊断\n/check SIGN - 单币诊断，自动补 USDT\n/summary - 立即查看市场摘要\n/hot - 查看强势过热候选\n/signals - 查看最近信号\n/top - 查看强度最高信号\n/review - 查看最近10条信号\n/perf - 查看最近信号表现\n/regime - 查看市场大方向\n/sectors - 查看热点/冷门板块\n/dev help - DevOps 命令",
            )
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
            snapshot = self.fetch_snapshot(symbol)
            signals = self.evaluate_snapshot(snapshot, {"mode": "both"})
            combined_signal = self.combined_signal(snapshot, signals)
            if combined_signal:
                signals.append(combined_signal)
            self.send_telegram_text(bot_token, chat_id, format_symbol_diagnosis(snapshot, signals))
        except Exception as exc:
            logging.exception("Failed to diagnose symbol from Telegram command")
            self.send_telegram_text(bot_token, chat_id, f"{symbol} 查询失败: {type(exc).__name__}: {exc}")

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
                self.send_telegram_text(bot_token, chat_id, truncate_text(f"回测摘要:\n{output}", 3500))
                return

            if subcommand == "restart":
                code = f"{secrets.randbelow(1_000_000):06d}"
                self.pending_dev_confirmations[chat_id] = (code, time.time() + 120)
                self.send_telegram_text(bot_token, chat_id, f"确认重启请在 2 分钟内发送:\n/dev confirm restart {code}")
                return

            if subcommand == "confirm" and len(args) >= 3 and args[1].lower() == "restart":
                self.handle_dev_restart_confirmation(bot_token, chat_id, args[2])
                return

            self.send_telegram_text(bot_token, chat_id, "未知 /dev 命令。发送 /dev help 查看用法。")
        except Exception:
            logging.exception("Failed to handle Telegram dev command")
            self.send_telegram_text(bot_token, chat_id, "DevOps 命令执行失败，请查看服务日志。")

    def handle_dev_restart_confirmation(self, bot_token: str, chat_id: str, code: str) -> None:
        pending = self.pending_dev_confirmations.get(chat_id)
        now = time.time()
        if not pending:
            self.send_telegram_text(bot_token, chat_id, "没有待确认的重启请求。")
            return

        expected_code, expires_at = pending
        if now > expires_at:
            self.pending_dev_confirmations.pop(chat_id, None)
            self.send_telegram_text(bot_token, chat_id, "确认码已过期，请重新发送 /dev restart。")
            return

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

    def dev_restart_notification_path(self) -> Path:
        return Path("/opt/crypto-monitor/dev_restart_notification.json")

    def save_pending_dev_restart_notification(self, chat_id: str) -> None:
        payload = {"chat_id": chat_id, "created_at": time.time()}
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
            self.send_telegram_text(bot_token, chat_id, f"重启后状态查询失败: {output}")
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


def format_signal_for_telegram(signal: Signal) -> str:
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

    if signal.snapshot is None:
        return f"{prefix} {label}\n\n{signal.title}\n{signal.message}\n\n时间: {now}"

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
        f"价格: {snapshot.close_price:.8g}\n"
        f"价格变化: {snapshot.price_change_percent:+.2f}%\n"
        f"OI变化: {snapshot.oi_change_percent:+.2f}%\n"
        f"全局多空比: {format_optional_value(snapshot.global_long_short_ratio)}\n"
        f"大户持仓多空比: {format_optional_value(snapshot.top_position_ratio)}\n"
        f"大户账户多空比: {format_optional_value(snapshot.top_account_ratio)}\n"
        f"主动买卖比: {format_optional_value(snapshot.taker_buy_sell_ratio)}\n"
        f"Funding: {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})\n"
        f"24h位置: {format_optional_value(snapshot.price_position_24h)}% ({price_position_label(snapshot)}) / 高 {format_optional_value(snapshot.high_24h)} / 低 {format_optional_value(snapshot.low_24h)}\n"
        f"资金流: 5m {format_usd(snapshot.net_flow_usd.get('5m'))} / 15m {format_usd(snapshot.net_flow_usd.get('15m'))} / 1h {format_usd(snapshot.net_flow_usd.get('1h'))} / 4h {format_usd(snapshot.net_flow_usd.get('4h'))}\n"
        f"资金流共振: {flow_alignment_score(snapshot)}/10 ({flow_alignment_note(flow_alignment_score(snapshot))})\n"
        f"现货/链上确认: {spot_alpha_confirmation(snapshot.symbol)}\n"
        f"短线评分: {short_term_score(snapshot)}/10 ({score_label(short_term_score(snapshot))})\n"
        f"中线评分: {mid_term_score(snapshot)}/10 ({score_label(mid_term_score(snapshot))})\n"
        f"AI共振复核: {ai_signal_review(signal)}\n"
        f"结构判断: {market_structure_label(snapshot)}\n"
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
        env = "强势"
    elif strength >= 6:
        env = "偏强"
    elif strength >= 3:
        env = "neutral"
    else:
        env = "偏弱"

    return "大盘风向: " + env + "\n" + "\n".join(lines)


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


def dev_help_text() -> str:
    return (
        "DevOps 命令:\n"
        "/dev status - 查看服务状态摘要\n"
        "/dev logs - 查看最近 30 行日志\n"
        "/dev git - 查看工作区和最近提交\n"
        "/dev backtest - 执行最近 80 条信号回测\n"
        "/dev restart - 生成重启确认码\n"
        "/dev confirm restart <code> - 确认重启服务\n"
        "/dev help - 查看帮助"
    )



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
        print_symbol_diagnosis(snapshot, signals)
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


def format_symbol_diagnosis(snapshot: MarketSnapshot, signals: list[Signal]) -> str:
    signal_names = ", ".join(signal.kind for signal in signals) or "-"
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
        f"资金流: 5m {format_usd(snapshot.net_flow_usd.get('5m'))} / 15m {format_usd(snapshot.net_flow_usd.get('15m'))} / 1h {format_usd(snapshot.net_flow_usd.get('1h'))} / 4h {format_usd(snapshot.net_flow_usd.get('4h'))}\n"
        f"资金流共振: {flow_alignment_score(snapshot)}/10 ({flow_alignment_note(flow_alignment_score(snapshot))})\n"
        f"现货/链上确认: {spot_alpha_confirmation(snapshot.symbol)}\n"
        f"短线评分: {short_term_score(snapshot)}/10 ({score_note(short_term_score(snapshot))})\n"
        f"中线评分: {mid_term_score(snapshot)}/10 ({score_note(mid_term_score(snapshot))})\n"
        f"信号: {signal_names}\n"
        f"结构判断: {market_structure_label(snapshot)}\n"
        f"判断: {diagnose_snapshot(snapshot, signals)}"
    )


def print_symbol_diagnosis(snapshot: MarketSnapshot, signals: list[Signal]) -> None:
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
    print(f"资金流: 5m {format_usd(snapshot.net_flow_usd.get('5m'))} / 15m {format_usd(snapshot.net_flow_usd.get('15m'))} / 1h {format_usd(snapshot.net_flow_usd.get('1h'))} / 4h {format_usd(snapshot.net_flow_usd.get('4h'))}")
    print(f"资金流共振: {flow_alignment_score(snapshot)}/10 ({flow_alignment_note(flow_alignment_score(snapshot))})")
    print(f"现货/链上确认: {spot_alpha_confirmation(snapshot.symbol)}")
    print(f"短线评分: {short_term_score(snapshot)}/10 ({score_note(short_term_score(snapshot))})")
    print(f"中线评分: {mid_term_score(snapshot)}/10 ({score_note(mid_term_score(snapshot))})")
    print(f"信号: {signal_names}")
    print(f"结构判断: {market_structure_label(snapshot)}")
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
