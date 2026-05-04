import argparse
import asyncio
import base64
import concurrent.futures
import csv
import datetime as dt
import hashlib
import json
import logging
import os
import queue
import re
import secrets
import shutil
import socket
import ssl
import sqlite3
import struct
import subprocess
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

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
COINGLASS_MARKET_CONTEXT_CACHE_TTL_SECONDS = 2700
TELEGRAM_SNAPSHOT_CACHE_TTL_SECONDS = 600
COINGECKO_MARKETS_URL = "https://api.coingecko.com/api/v3/coins/markets"
DEFILLAMA_STABLECOINS_URL = "https://stablecoins.llama.fi/stablecoins"
DEFILLAMA_CHAINS_URL = "https://api.llama.fi/v2/chains"
DEFILLAMA_DEX_OVERVIEW_URL = "https://api.llama.fi/overview/dexs"
DEFILLAMA_FEES_OVERVIEW_URL = "https://api.llama.fi/overview/fees"
DEFAULT_EXTERNAL_DATA_DB_PATH = "external_data.sqlite"
DEFAULT_ONCHAIN_ADDRESS_CONFIG_PATH = "onchain_addresses.yaml"
DEFILLAMA_STABLECOIN_TTL_SECONDS = 2700
DEFILLAMA_EXTENDED_TTL_SECONDS = 3600
DEXSCREENER_MARKET_TTL_SECONDS = 900
DEXSCREENER_COLLECT_LIMIT = 30
ONCHAIN_SCAN_INTERVAL_SECONDS = 900
ONCHAIN_SCAN_ADDRESS_TTL_SECONDS = 900
ONCHAIN_SCAN_ADDRESS_LIMIT = 20
ONCHAIN_SCAN_TRANSFER_LIMIT = 50
DISCORD_SUPPRESSED_DIGEST_INTERVAL_SECONDS = 900
FLOW_SHORT_PERIODS = ["5m", "15m", "1h"]
FLOW_MID_PERIODS = ["4h", "12h", "24h"]
FLOW_LONG_PERIODS = ["48h", "72h", "96h", "120h", "144h"]
FLOW_PANEL_PERIODS = ["5m", "15m", "1h", "4h", "12h", "24h", "72h", "144h"]
FLOW_PERIODS = FLOW_SHORT_PERIODS + FLOW_MID_PERIODS + FLOW_LONG_PERIODS
FLOW_SHORT_CACHE_TTL_SECONDS = 120
FLOW_MID_CACHE_TTL_SECONDS = 900
FLOW_LONG_CACHE_TTL_SECONDS = 2700
COINGLASS_TAKER_LONG_RANGES = ("24h", "7d")
COINGLASS_BALANCE_LONG_RANGES = ("24h", "7d", "30d")
COINGLASS_FUNDING_ACCUMULATED_RANGES = ("24h", "7d")
DEFAULT_TELEGRAM_REALTIME_PRIORITIES = ("S", "A", "B")
DEFAULT_TELEGRAM_DIGEST_PRIORITIES = ("C", "D")
DEFAULT_TELEGRAM_DIGEST_INTERVAL_MINUTES = 30
DEFAULT_TELEGRAM_DIGEST_MAX_PER_PRIORITY = 8
DEFAULT_TELEGRAM_MERGE_WINDOW_SECONDS = 120
DEFAULT_CONVICTION_REALTIME_THRESHOLD = 75
DEFAULT_CONVICTION_WATCH_THRESHOLD = 55
DEFAULT_RISK_REALTIME_THRESHOLD = 70
VALID_BINANCE_USDT_SYMBOL_RE = re.compile(r"^[A-Z0-9]{2,20}USDT$")
CORE_MOMENTUM_SYMBOLS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"}
MAINSTREAM_WATCH_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "XRPUSDT",
    "TONUSDT",
    "OPUSDT",
    "ARBUSDT",
}
ONCHAIN_SUMMARY_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "DOGEUSDT"]
DISCORD_CHANNEL_ENV_KEYS = {
    "main": "DISCORD_MAIN_CHANNEL_ID",
    "alerts": "DISCORD_ALERTS_CHANNEL_ID",
    "risk": "DISCORD_RISK_CHANNEL_ID",
    "summary": "DISCORD_SUMMARY_CHANNEL_ID",
    "digest": "DISCORD_DIGEST_CHANNEL_ID",
    "debug": "DISCORD_DEBUG_CHANNEL_ID",
    "alt_watch": "DISCORD_ALT_WATCH_CHANNEL_ID",
    "onchain": "DISCORD_ONCHAIN_CHANNEL_ID",
}
DISCORD_COLOR_BULLISH = 0x2ECC71
DISCORD_COLOR_RISK = 0xE74C3C
DISCORD_COLOR_WATCH = 0xF1C40F
DISCORD_COLOR_SUMMARY = 0x95A5A6


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
    spot_price: float | None = None
    price_change_periods: dict[str, float] | None = None


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
class EvidenceItem:
    label: str
    points: int
    polarity: str
    horizon: str
    source: str


@dataclass(frozen=True)
class LeadingSignalScore:
    leading_score: int
    leading_direction: str
    leading_label: str
    leading_items: list[str]
    leading_bull_score: int
    leading_bear_score: int


@dataclass(frozen=True)
class MultiTimeframePriceAction:
    score: int
    label: str
    direction: str
    short_score: int
    mid_score: int
    long_score: int
    short_label: str
    mid_label: str
    long_label: str
    items: list[str]
    risk_items: list[str]
    patterns: list[str]
    recommendation: str


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
    price_change_percent: float | None
    oi_change_percent: float | None
    reason: str


@dataclass(frozen=True)
class DiscordSuppressedDigestItem:
    timestamp: float
    symbol: str
    kind: str
    priority: str
    quality: int
    conviction: int
    reason: str
    price_change: float | None
    oi_change: float | None
    flow_label: str
    evidence_summary: str


@dataclass
class PendingTelegramSignalMerge:
    created_at: float
    updated_at: float
    symbol: str
    direction: str
    signals: list[Signal]
    priorities: list[str]
    quality_scores: list[int]
    quality_reasons: list[str]


@dataclass(frozen=True)
class DiscordConfig:
    enabled: bool
    bot_token: str
    channel_ids: dict[str, str]


@dataclass(frozen=True)
class DiscordOutboundMessage:
    channel_key: str
    content: str | None = None
    title: str | None = None
    color: int | None = None
    fields: list[tuple[str, str, bool]] | None = None
    symbol: str | None = None
    kind: str | None = None


@dataclass(frozen=True)
class DiscordAltWatchItem:
    created_at: float
    symbol: str
    kind: str
    conviction_score: int
    quality_score: int
    leading_score: int
    evidence_score: int
    trap_score: int
    price_change_percent: float | None
    oi_change_percent: float | None
    flow_label: str
    reason: str
    sort_score: int


@dataclass
class DataSourceSpec:
    name: str
    category: str
    is_real_onchain: bool
    requires_api_key: bool
    ttl_seconds: int
    priority: int
    enabled: bool
    last_success: float | None = None
    last_error: str = ""
    confidence: str = "medium"
    scanned_addresses: int = 0
    fetched_events: int = 0
    written_events: int = 0
    fetched_count: int = 0
    written_count: int = 0
    last_scan_at: float | None = None


@dataclass(frozen=True)
class ExternalDataPoint:
    source: str
    symbol: str | None
    asset: str | None
    metric: str
    value: float | str | None
    timestamp: float
    raw_json: dict[str, Any] | list[Any] | str | None
    confidence: str
    is_real_onchain: bool


@dataclass(frozen=True)
class OnchainAddressLabel:
    chain: str
    label: str
    entity: str
    type: str
    address: str
    assets: list[str]
    source: str
    confidence: str


@dataclass(frozen=True)
class OnchainAddressCandidate:
    chain: str
    label: str
    entity: str
    type: str
    address: str
    assets: list[str]
    candidate_source: str
    verification_status: str
    notes: str


@dataclass(frozen=True)
class OnchainTransferEvent:
    chain: str
    tx_hash: str
    timestamp: float
    asset: str
    amount: float | None
    amount_usd: float | None
    from_address: str
    to_address: str
    from_label: str
    to_label: str
    direction: str
    source: str
    raw_json: dict[str, Any] | list[Any] | str | None


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
    "net_flow_48h_usd",
    "net_flow_72h_usd",
    "net_flow_96h_usd",
    "net_flow_120h_usd",
    "net_flow_144h_usd",
    "net_flow_5m_ratio",
    "net_flow_15m_ratio",
    "net_flow_1h_ratio",
    "net_flow_4h_ratio",
    "net_flow_12h_ratio",
    "net_flow_24h_ratio",
    "net_flow_48h_ratio",
    "net_flow_72h_ratio",
    "net_flow_96h_ratio",
    "net_flow_120h_ratio",
    "net_flow_144h_ratio",
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
    "entry_timing_score",
    "entry_timing_label",
    "entry_timing_reason",
    "spot_onchain_score",
    "spot_onchain_label",
    "spot_onchain_reason",
    "contract_spot_divergence_label",
    "contract_spot_divergence_score",
    "contract_spot_divergence_reason",
    "major_flow_score",
    "major_flow_label",
    "major_flow_reason",
    "basis_pct",
    "basis_state",
    "basis_reason",
    "short_flow_score",
    "mid_flow_score",
    "long_flow_score",
    "flow_trend_label",
    "flow_trend_reason",
    "position_behavior_label",
    "position_behavior_score",
    "position_behavior_reason",
    "squeeze_state_label",
    "squeeze_state_score",
    "squeeze_state_reason",
    "spot_absorption_label",
    "spot_absorption_score",
    "spot_absorption_reason",
    "market_intent_label",
    "market_intent_score",
    "market_intent_reason",
    "leading_score",
    "leading_direction",
    "leading_label",
    "leading_items",
    "leading_bull_score",
    "leading_bear_score",
    "evidence_score",
    "evidence_direction",
    "evidence_summary",
    "evidence_items",
    "conviction_score",
    "conviction_label",
    "conviction_reason",
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
        self.last_hourly_summary_key: int | None = None
        self.last_onchain_summary_hour_key: int | None = None
        self.last_stablecoin_liquidity_hour_key: int | None = None
        self.last_coinglass_summary_hour_key: int | None = None
        self.last_external_funds_overview_hour_key: int | None = None
        self.last_market_summary_text = ""
        self.last_market_summary_ts = 0.0
        self.last_market_summary_source = ""
        self.market_summary_cache_lock = threading.Lock()
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
        self.discord_config = resolve_discord_config()
        self.discord_worker_started = False
        self.discord_outbound_queue: queue.Queue[DiscordOutboundMessage] = queue.Queue(maxsize=500)
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
        self.external_data_db_path = str(config.get("external_data_db_path", DEFAULT_EXTERNAL_DATA_DB_PATH))
        self.onchain_address_config_path = str(config.get("onchain_address_config_path", DEFAULT_ONCHAIN_ADDRESS_CONFIG_PATH))
        self.external_data_lock = threading.Lock()
        self.data_sources = self.build_data_source_specs()
        self.onchain_address_labels: list[OnchainAddressLabel] = []
        self.onchain_address_candidates: list[OnchainAddressCandidate] = []
        self.last_external_stablecoin_collect_at = 0.0
        self.last_defillama_extended_collect_at = 0.0
        self.last_dexscreener_collect_at = 0.0
        self.dexscreener_symbol_collect_cache: dict[str, float] = {}
        self.last_onchain_scan_collect_at = 0.0
        self.onchain_scan_address_cache: dict[tuple[str, str], float] = {}
        self.init_external_data_store()
        self.load_and_store_onchain_address_labels()
        self.flow_metrics_cache: dict[tuple[str, str], tuple[float, float | None, float | None]] = {}
        self.flow_metrics_cache_lock = threading.Lock()
        self.telegram_signal_cooldowns: dict[str, tuple[float, int]] = {}
        self.pending_telegram_signal_merges: dict[str, PendingTelegramSignalMerge] = {}
        self.pending_telegram_signal_merge_lock = threading.Lock()
        self.telegram_signal_digest_queue: list[TelegramSignalDigestItem] = []
        self.telegram_signal_digest_lock = threading.Lock()
        self.last_telegram_signal_digest_at = time.time()
        self.discord_alt_watch_queue: list[DiscordAltWatchItem] = []
        self.discord_alt_watch_lock = threading.Lock()
        self.last_discord_alt_watch_digest_at = time.time()
        self.discord_alt_watch_symbol_sent_at: dict[str, float] = {}
        self.discord_suppressed_digest_queue: list[DiscordSuppressedDigestItem] = []
        self.discord_suppressed_digest_lock = threading.Lock()
        self.discord_suppressed_digest_recent: deque[float] = deque(maxlen=1000)
        self.last_discord_suppressed_digest_flush_at = 0.0
        self.last_discord_suppressed_digest_flush_attempt_at = 0.0
        self.last_discord_suppressed_digest_flush_status = "not attempted"
        self.last_discord_suppressed_digest_sent_at = 0.0
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

    def build_data_source_specs(self) -> dict[str, DataSourceSpec]:
        coinglass_enabled = bool(os.getenv("COINGLASS_API_KEY", "").strip())
        etherscan_enabled = bool(os.getenv("ETHERSCAN_API_KEY", "").strip())
        tronscan_enabled = bool(os.getenv("TRONSCAN_API_KEY", "").strip() or os.getenv("TRONGRID_API_KEY", "").strip())
        solscan_enabled = bool(os.getenv("SOLSCAN_API_KEY", "").strip())
        return {
            "Binance futures": DataSourceSpec(
                name="Binance futures",
                category="derivatives",
                is_real_onchain=False,
                requires_api_key=bool(os.getenv(str(self.config.get("binance_api_key_env", "BINANCE_API_KEY")), "").strip()),
                ttl_seconds=120,
                priority=10,
                enabled=True,
                confidence="high",
            ),
            "Binance spot": DataSourceSpec(
                name="Binance spot",
                category="spot",
                is_real_onchain=False,
                requires_api_key=False,
                ttl_seconds=180,
                priority=20,
                enabled=True,
                confidence="high",
            ),
            "DexScreener": DataSourceSpec(
                name="DexScreener",
                category="dex",
                is_real_onchain=False,
                requires_api_key=False,
                ttl_seconds=300,
                priority=30,
                enabled=True,
                confidence="medium",
            ),
            "CoinGlass aggregation": DataSourceSpec(
                name="CoinGlass aggregation",
                category="external",
                is_real_onchain=False,
                requires_api_key=True,
                ttl_seconds=COINGLASS_MARKET_CONTEXT_CACHE_TTL_SECONDS,
                priority=40,
                enabled=coinglass_enabled,
                last_error="" if coinglass_enabled else "COINGLASS_API_KEY not configured",
                confidence="medium",
            ),
            "DefiLlama stablecoin supply": DataSourceSpec(
                name="DefiLlama stablecoin supply",
                category="stablecoin",
                is_real_onchain=False,
                requires_api_key=False,
                ttl_seconds=DEFILLAMA_STABLECOIN_TTL_SECONDS,
                priority=50,
                enabled=True,
                last_error="稳定币供应聚合，不等同钱包流/交易所净流",
                confidence="medium",
            ),
            "DefiLlama TVL/DEX metrics": DataSourceSpec(
                name="DefiLlama TVL/DEX metrics",
                category="external",
                is_real_onchain=False,
                requires_api_key=False,
                ttl_seconds=DEFILLAMA_EXTENDED_TTL_SECONDS,
                priority=51,
                enabled=True,
                confidence="medium",
            ),
            "Derived spot/DEX confirmation": DataSourceSpec(
                name="Derived spot/DEX confirmation",
                category="external",
                is_real_onchain=False,
                requires_api_key=False,
                ttl_seconds=180,
                priority=60,
                enabled=True,
                confidence="low",
            ),
            "Address labels": DataSourceSpec(
                name="Address labels",
                category="onchain",
                is_real_onchain=False,
                requires_api_key=False,
                ttl_seconds=3600,
                priority=70,
                enabled=True,
                last_error="自建地址标签覆盖有限，不能等同 Arkham/Nansen 实体标签",
                confidence="low",
            ),
            "Whale Alert": DataSourceSpec("Whale Alert", "onchain", True, True, 3600, 80, False, last_error="adapter reserved", confidence="low"),
            "Etherscan": DataSourceSpec("Etherscan", "onchain", True, True, 3600, 81, etherscan_enabled, last_error="" if etherscan_enabled else "ETHERSCAN_API_KEY not configured", confidence="low"),
            "Tronscan": DataSourceSpec("Tronscan", "onchain", True, True, 3600, 82, tronscan_enabled, last_error="" if tronscan_enabled else "TRONSCAN_API_KEY/TRONGRID_API_KEY not configured", confidence="low"),
            "Solscan": DataSourceSpec("Solscan", "onchain", True, True, 3600, 83, solscan_enabled, last_error="" if solscan_enabled else "SOLSCAN_API_KEY not configured", confidence="low"),
            "CryptoQuant": DataSourceSpec("CryptoQuant", "onchain", True, True, 3600, 84, False, last_error="adapter reserved", confidence="low"),
            "Glassnode": DataSourceSpec("Glassnode", "onchain", True, True, 3600, 85, False, last_error="adapter reserved", confidence="low"),
            "Arkham": DataSourceSpec("Arkham", "onchain", True, True, 3600, 86, False, last_error="adapter reserved", confidence="low"),
            "Nansen": DataSourceSpec("Nansen", "onchain", True, True, 3600, 87, False, last_error="adapter reserved", confidence="low"),
        }

    def external_db_connection(self) -> sqlite3.Connection:
        path = Path(self.external_data_db_path)
        if path.parent != Path("."):
            path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def init_external_data_store(self) -> None:
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS external_data_points (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        symbol TEXT,
                        asset TEXT,
                        metric TEXT NOT NULL,
                        value TEXT,
                        timestamp REAL NOT NULL,
                        raw_json TEXT,
                        confidence TEXT,
                        is_real_onchain INTEGER NOT NULL DEFAULT 0,
                        created_at REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_external_data_source_time ON external_data_points(source, timestamp);
                    CREATE INDEX IF NOT EXISTS idx_external_data_symbol_metric ON external_data_points(symbol, metric, timestamp);
                    CREATE TABLE IF NOT EXISTS source_health (
                        source TEXT PRIMARY KEY,
                        category TEXT,
                        enabled INTEGER NOT NULL,
                        is_real_onchain INTEGER NOT NULL DEFAULT 0,
                        requires_api_key INTEGER NOT NULL DEFAULT 0,
                        ttl_seconds INTEGER,
                        priority INTEGER,
                        confidence TEXT,
                        last_success REAL,
                        last_error TEXT,
                        scanned_addresses INTEGER NOT NULL DEFAULT 0,
                        fetched_events INTEGER NOT NULL DEFAULT 0,
                        written_events INTEGER NOT NULL DEFAULT 0,
                        fetched_count INTEGER NOT NULL DEFAULT 0,
                        written_count INTEGER NOT NULL DEFAULT 0,
                        last_scan_at REAL,
                        updated_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS stablecoin_supply (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        asset TEXT NOT NULL,
                        supply_usd REAL,
                        supply_native REAL,
                        timestamp REAL NOT NULL,
                        raw_json TEXT,
                        created_at REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_stablecoin_supply_asset_time ON stablecoin_supply(asset, timestamp);
                    CREATE TABLE IF NOT EXISTS exchange_balances (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        asset TEXT NOT NULL,
                        range_label TEXT,
                        balance REAL,
                        balance_usd REAL,
                        change_value REAL,
                        change_percent REAL,
                        timestamp REAL NOT NULL,
                        raw_json TEXT,
                        created_at REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_exchange_balances_asset_time ON exchange_balances(asset, timestamp);
                    CREATE TABLE IF NOT EXISTS whale_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        asset TEXT,
                        value_usd REAL,
                        from_label TEXT,
                        to_label TEXT,
                        timestamp REAL NOT NULL,
                        raw_json TEXT,
                        created_at REAL NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS dex_market_snapshots (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        source TEXT NOT NULL,
                        symbol TEXT,
                        asset TEXT,
                        chain TEXT,
                        pair_address TEXT,
                        dex TEXT,
                        quote_symbol TEXT,
                        price_change_5m REAL,
                        price_change_1h REAL,
                        price_change_24h REAL,
                        volume_5m_usd REAL,
                        volume_1h_usd REAL,
                        volume_24h_usd REAL,
                        liquidity_usd REAL,
                        fdv REAL,
                        market_cap REAL,
                        timestamp REAL NOT NULL,
                        raw_json TEXT,
                        created_at REAL NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_dex_market_symbol_time ON dex_market_snapshots(symbol, timestamp);
                    CREATE TABLE IF NOT EXISTS onchain_address_labels (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chain TEXT NOT NULL,
                        label TEXT NOT NULL,
                        entity TEXT,
                        type TEXT,
                        address TEXT NOT NULL,
                        assets TEXT,
                        source TEXT,
                        confidence TEXT,
                        updated_at REAL NOT NULL,
                        UNIQUE(chain, address)
                    );
                    CREATE INDEX IF NOT EXISTS idx_onchain_address_labels_asset ON onchain_address_labels(assets);
                    CREATE INDEX IF NOT EXISTS idx_onchain_address_labels_entity ON onchain_address_labels(entity);
                    CREATE TABLE IF NOT EXISTS onchain_transfer_events (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        chain TEXT NOT NULL,
                        tx_hash TEXT NOT NULL,
                        timestamp REAL NOT NULL,
                        asset TEXT,
                        amount REAL,
                        amount_usd REAL,
                        from_address TEXT,
                        to_address TEXT,
                        from_label TEXT,
                        to_label TEXT,
                        direction TEXT,
                        source TEXT,
                        raw_json TEXT,
                        created_at REAL NOT NULL,
                        UNIQUE(chain, tx_hash, asset, amount, from_address, to_address)
                    );
                    CREATE INDEX IF NOT EXISTS idx_onchain_transfer_events_asset_time ON onchain_transfer_events(asset, timestamp);
                    CREATE INDEX IF NOT EXISTS idx_onchain_transfer_events_direction_time ON onchain_transfer_events(direction, timestamp);
                    CREATE INDEX IF NOT EXISTS idx_onchain_transfer_events_source_time ON onchain_transfer_events(source, timestamp);
                    CREATE UNIQUE INDEX IF NOT EXISTS idx_onchain_transfer_events_unique_basic
                        ON onchain_transfer_events(chain, tx_hash, asset, from_address, to_address);
                    """
                )
                self.ensure_source_health_scan_columns(conn)
                self.ensure_dex_market_snapshot_columns(conn)
        for spec in self.data_sources.values():
            self.update_source_health(spec.name)

    def ensure_source_health_scan_columns(self, conn: sqlite3.Connection) -> None:
        existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(source_health)").fetchall()}
        int_columns = ("scanned_addresses", "fetched_events", "written_events", "fetched_count", "written_count")
        for column in int_columns:
            if column not in existing:
                conn.execute(f"ALTER TABLE source_health ADD COLUMN {column} INTEGER NOT NULL DEFAULT 0")
        if "last_scan_at" not in existing:
            conn.execute("ALTER TABLE source_health ADD COLUMN last_scan_at REAL")

    def ensure_dex_market_snapshot_columns(self, conn: sqlite3.Connection) -> None:
        existing = {str(row[1]) for row in conn.execute("PRAGMA table_info(dex_market_snapshots)").fetchall()}
        columns = {
            "pair_address": "TEXT",
            "price_change_5m": "REAL",
            "volume_5m_usd": "REAL",
            "fdv": "REAL",
            "market_cap": "REAL",
        }
        for column, column_type in columns.items():
            if column not in existing:
                conn.execute(f"ALTER TABLE dex_market_snapshots ADD COLUMN {column} {column_type}")

    def update_source_health(
        self,
        source_name: str,
        success: bool | None = None,
        error: str | None = None,
        scanned_addresses: int | None = None,
        fetched_events: int | None = None,
        written_events: int | None = None,
        fetched_count: int | None = None,
        written_count: int | None = None,
        last_scan_at: float | None = None,
    ) -> None:
        spec = self.data_sources.get(source_name)
        if not spec:
            return
        now = time.time()
        if success is True:
            spec.last_success = now
            spec.last_error = ""
        elif success is False:
            spec.last_error = truncate_text(str(error or "unknown error"), 300)
        if scanned_addresses is not None:
            spec.scanned_addresses = max(0, int(scanned_addresses))
        if fetched_events is not None:
            spec.fetched_events = max(0, int(fetched_events))
        if written_events is not None:
            spec.written_events = max(0, int(written_events))
        if fetched_count is not None:
            spec.fetched_count = max(0, int(fetched_count))
        if written_count is not None:
            spec.written_count = max(0, int(written_count))
        if last_scan_at is not None:
            spec.last_scan_at = float(last_scan_at)
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO source_health (
                        source, category, enabled, is_real_onchain, requires_api_key, ttl_seconds,
                        priority, confidence, last_success, last_error, scanned_addresses,
                        fetched_events, written_events, fetched_count, written_count,
                        last_scan_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(source) DO UPDATE SET
                        category=excluded.category,
                        enabled=excluded.enabled,
                        is_real_onchain=excluded.is_real_onchain,
                        requires_api_key=excluded.requires_api_key,
                        ttl_seconds=excluded.ttl_seconds,
                        priority=excluded.priority,
                        confidence=excluded.confidence,
                        last_success=excluded.last_success,
                        last_error=excluded.last_error,
                        scanned_addresses=excluded.scanned_addresses,
                        fetched_events=excluded.fetched_events,
                        written_events=excluded.written_events,
                        fetched_count=excluded.fetched_count,
                        written_count=excluded.written_count,
                        last_scan_at=excluded.last_scan_at,
                        updated_at=excluded.updated_at
                    """,
                    (
                        spec.name,
                        spec.category,
                        int(spec.enabled),
                        int(spec.is_real_onchain),
                        int(spec.requires_api_key),
                        spec.ttl_seconds,
                        spec.priority,
                        spec.confidence,
                        spec.last_success,
                        spec.last_error,
                        spec.scanned_addresses,
                        spec.fetched_events,
                        spec.written_events,
                        spec.fetched_count,
                        spec.written_count,
                        spec.last_scan_at,
                        now,
                    ),
                )

    def load_onchain_address_labels(self) -> list[OnchainAddressLabel]:
        path = Path(self.onchain_address_config_path)
        if not path.exists():
            logging.info("onchain address labels config missing: %s", path)
            return []
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception as exc:
            logging.warning("Failed to read onchain address labels config: %s", path, exc_info=True)
            self.update_source_health("Address labels", False, f"config read failed: {type(exc).__name__}")
            return []
        if not isinstance(payload, dict):
            self.update_source_health("Address labels", False, "config root must be mapping")
            return []
        labels: list[OnchainAddressLabel] = []
        for chain, rows in payload.items():
            chain_name = str(chain or "").strip().lower()
            if chain_name not in {"ethereum", "tron", "solana"} or not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                address = str(row.get("address") or "").strip()
                if is_placeholder_onchain_address(address):
                    continue
                label = str(row.get("label") or "").strip()
                entity = str(row.get("entity") or "").strip()
                label_type = str(row.get("type") or "unknown").strip().lower()
                if label_type not in {"exchange", "treasury", "fund", "project", "whale", "unknown"}:
                    label_type = "unknown"
                assets = row.get("assets") if isinstance(row.get("assets"), list) else []
                normalized_assets = [str(asset).strip().upper() for asset in assets if str(asset).strip()]
                if not label or not entity:
                    continue
                labels.append(
                    OnchainAddressLabel(
                        chain=chain_name,
                        label=label,
                        entity=entity,
                        type=label_type,
                        address=address,
                        assets=normalized_assets,
                        source=str(row.get("source") or "local config"),
                        confidence=str(row.get("confidence") or "low"),
                    )
                )
        logging.info("onchain address labels loaded: count=%s path=%s", len(labels), path)
        return labels

    def load_onchain_address_candidates(self) -> list[OnchainAddressCandidate]:
        path = Path(self.onchain_address_config_path)
        if not path.exists():
            return []
        try:
            payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            logging.warning("Failed to read onchain address candidates config: %s", path, exc_info=True)
            return []
        candidates_root = payload.get("candidates") if isinstance(payload, dict) else None
        if not isinstance(candidates_root, dict):
            return []
        candidates: list[OnchainAddressCandidate] = []
        for chain, rows in candidates_root.items():
            chain_name = str(chain or "").strip().lower()
            if chain_name not in {"ethereum", "tron", "solana"} or not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                status = str(row.get("verification_status") or "pending").strip().lower()
                if status not in {"pending", "verified", "rejected"}:
                    status = "pending"
                assets = row.get("assets") if isinstance(row.get("assets"), list) else []
                candidates.append(
                    OnchainAddressCandidate(
                        chain=chain_name,
                        label=str(row.get("label") or "").strip(),
                        entity=str(row.get("entity") or "").strip(),
                        type=str(row.get("type") or "unknown").strip().lower(),
                        address=str(row.get("address") or "").strip(),
                        assets=[str(asset).strip().upper() for asset in assets if str(asset).strip()],
                        candidate_source=str(row.get("candidate_source") or "").strip(),
                        verification_status=status,
                        notes=str(row.get("notes") or "").strip(),
                    )
                )
        logging.info("onchain address candidates loaded: count=%s path=%s", len(candidates), path)
        return candidates

    def load_and_store_onchain_address_labels(self) -> None:
        labels = self.load_onchain_address_labels()
        self.onchain_address_labels = labels
        self.onchain_address_candidates = self.load_onchain_address_candidates()
        self.store_onchain_address_labels(labels)
        if labels:
            spec = self.data_sources.get("Address labels")
            if spec:
                spec.confidence = "medium" if len(labels) >= 10 else "low"
            self.update_source_health("Address labels", True, f"loaded {len(labels)} labels")
        else:
            self.update_source_health("Address labels", False, "no valid labels loaded; 自建地址标签覆盖有限")

    def store_onchain_address_labels(self, labels: list[OnchainAddressLabel]) -> None:
        now = time.time()
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                conn.execute("DELETE FROM onchain_address_labels")
                if labels:
                    conn.executemany(
                        """
                        INSERT OR REPLACE INTO onchain_address_labels (
                            chain, label, entity, type, address, assets, source, confidence, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            (
                                item.chain,
                                item.label,
                                item.entity,
                                item.type,
                                item.address,
                                ",".join(item.assets),
                                item.source,
                                item.confidence,
                                now,
                            )
                            for item in labels
                        ],
                    )

    def onchain_transfer_event_count(self, since_seconds: int = 86400) -> int:
        cutoff = time.time() - since_seconds
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                row = conn.execute(
                    "SELECT COUNT(*) FROM onchain_transfer_events WHERE timestamp >= ?",
                    (cutoff,),
                ).fetchone()
        return int(row[0] if row else 0)

    def onchain_transfer_event_query_count(self, query: str, since_seconds: int = 86400) -> int:
        cutoff = time.time() - since_seconds
        normalized = str(query or "").strip().lower()
        if not normalized:
            return self.onchain_transfer_event_count(since_seconds)
        like = f"%{normalized}%"
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                row = conn.execute(
                    """
                    SELECT COUNT(*)
                    FROM onchain_transfer_events
                    WHERE timestamp >= ?
                      AND (lower(asset) = ? OR lower(from_label) LIKE ? OR lower(to_label) LIKE ?
                           OR lower(from_address) LIKE ? OR lower(to_address) LIKE ?)
                    """,
                    (cutoff, normalized, like, like, like, like),
                ).fetchone()
        return int(row[0] if row else 0)

    def onchain_transfer_direction_counts(self, since_seconds: int = 86400) -> dict[str, int]:
        cutoff = time.time() - since_seconds
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT COALESCE(direction, 'unknown'), COUNT(*)
                    FROM onchain_transfer_events
                    WHERE timestamp >= ?
                    GROUP BY direction
                    """,
                    (cutoff,),
                ).fetchall()
        return {str(direction): int(count) for direction, count in rows}

    def scan_adapter_health_rows(self) -> dict[str, dict[str, Any]]:
        names = ["Etherscan", "Tronscan", "Solscan"]
        placeholders = {
            name: {
                "source": name,
                "enabled": int(bool(self.data_sources.get(name) and self.data_sources[name].enabled)),
                "last_success": self.data_sources.get(name).last_success if self.data_sources.get(name) else None,
                "last_error": self.data_sources.get(name).last_error if self.data_sources.get(name) else "",
                "scanned_addresses": self.data_sources.get(name).scanned_addresses if self.data_sources.get(name) else 0,
                "fetched_events": self.data_sources.get(name).fetched_events if self.data_sources.get(name) else 0,
                "written_events": self.data_sources.get(name).written_events if self.data_sources.get(name) else 0,
                "last_scan_at": self.data_sources.get(name).last_scan_at if self.data_sources.get(name) else None,
            }
            for name in names
        }
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT source, enabled, last_success, last_error, scanned_addresses, fetched_events, written_events, last_scan_at
                    FROM source_health
                    WHERE source IN ('Etherscan', 'Tronscan', 'Solscan')
                    """
                ).fetchall()
        for row in rows:
            source = str(row[0])
            placeholders[source] = {
                "source": source,
                "enabled": int(row[1] or 0),
                "last_success": parse_float(row[2]),
                "last_error": str(row[3] or ""),
                "scanned_addresses": int(row[4] or 0),
                "fetched_events": int(row[5] or 0),
                "written_events": int(row[6] or 0),
                "last_scan_at": parse_float(row[7]),
            }
        return placeholders

    def format_scan_adapter_status_lines(self) -> list[str]:
        rows = self.scan_adapter_health_rows()
        lines = ["Scan adapters:"]
        for name in ("Etherscan", "Tronscan", "Solscan"):
            row = rows.get(name, {})
            enabled = bool(row.get("enabled"))
            error_text = truncate_text(str(row.get("last_error") or "-"), 80)
            if not enabled and "not configured" in error_text:
                status_text = "disabled/not configured"
            elif not enabled and name == "Solscan":
                status_text = "disabled/not configured / reserved"
            elif name == "Solscan" and "reserved" in error_text:
                status_text = "reserved"
            else:
                status_text = (
                    f"scanned={int(row.get('scanned_addresses') or 0)} "
                    f"fetched={int(row.get('fetched_events') or 0)} "
                    f"written={int(row.get('written_events') or 0)}"
                )
            lines.append(
                f"{name}: {status_text} "
                f"last={format_ts_short(parse_float(row.get('last_scan_at')) or parse_float(row.get('last_success')))} "
                f"error={error_text}"
            )
        return lines

    def onchain_event_empty_status_text(self, query: str | None = None) -> str:
        rows = self.scan_adapter_health_rows()
        relevant = rows
        normalized = str(query or "").strip().upper()
        if normalized in {"USDT", "USDC", "ETH"}:
            relevant = {name: row for name, row in rows.items() if name in {"Etherscan", "Tronscan"}}
        configured = [row for row in relevant.values() if bool(row.get("enabled"))]
        scan_rows = [
            row for row in relevant.values()
            if parse_float(row.get("last_scan_at")) or parse_float(row.get("last_success"))
        ]
        if not configured and not scan_rows:
            return "数据源未配置。"
        status_rows = scan_rows or configured
        if any("no configured onchain address labels" in str(row.get("last_error") or "") for row in status_rows):
            return "尚未扫描或没有有效地址标签。"
        any_scan = bool(scan_rows)
        if not any_scan:
            return "尚未扫描或没有有效地址标签。"
        if all(str(row.get("last_error") or "").strip() for row in status_rows):
            return "最近扫描失败，请查看 Scan 状态。"
        fetched_total = sum(int(row.get("fetched_events") or 0) for row in status_rows)
        if fetched_total == 0:
            return "最近扫描正常，但暂无匹配转账事件。"
        return "最近扫描有数据返回，但暂无符合当前查询的已入库事件。"

    def format_onchain_address_sources(self) -> str:
        labels = list(self.onchain_address_labels)
        candidates = list(self.onchain_address_candidates)
        by_chain: dict[str, int] = {}
        by_entity: dict[str, int] = {}
        by_type: dict[str, int] = {}
        for item in labels:
            by_chain[item.chain] = by_chain.get(item.chain, 0) + 1
            by_entity[item.entity] = by_entity.get(item.entity, 0) + 1
            by_type[item.type] = by_type.get(item.type, 0) + 1
        candidate_counts = self.onchain_address_candidate_counts(candidates)
        lines = [
            "链上地址标签源",
            f"已加载地址标签数量: {len(labels)}",
            (
                f"候选地址数量: {len(candidates)} | "
                f"verified={candidate_counts.get('verified', 0)} / "
                f"pending={candidate_counts.get('pending', 0)} / "
                f"rejected={candidate_counts.get('rejected', 0)}"
            ),
            "说明: 自建地址标签覆盖有限，不能等同 Arkham/Nansen 实体标签。",
            "规则: candidates 只作审计记录，不会被 Scan API 采集。",
            f"按 chain: {format_count_mapping(by_chain)}",
            f"按 entity: {format_count_mapping(by_entity)}",
            f"按 type: {format_count_mapping(by_type)}",
            f"Etherscan key: {'configured' if os.getenv('ETHERSCAN_API_KEY', '').strip() else 'disabled/not configured'}",
            f"Tronscan key: {'configured' if (os.getenv('TRONSCAN_API_KEY', '').strip() or os.getenv('TRONGRID_API_KEY', '').strip()) else 'disabled/not configured'}",
            f"Solscan key: {'configured' if os.getenv('SOLSCAN_API_KEY', '').strip() else 'disabled/not configured'}",
            f"最近24h onchain_transfer_events: {self.onchain_transfer_event_count()}",
            f"最近24h direction: {format_count_mapping(self.onchain_transfer_direction_counts())}",
        ]
        lines.extend(self.format_scan_adapter_status_lines())
        return "\n".join(lines)

    def onchain_address_candidate_counts(self, candidates: list[OnchainAddressCandidate] | None = None) -> dict[str, int]:
        counts = {"verified": 0, "pending": 0, "rejected": 0}
        for item in (candidates if candidates is not None else self.onchain_address_candidates):
            status = item.verification_status if item.verification_status in counts else "pending"
            counts[status] += 1
        return counts

    def format_onchain_address_candidates(self, limit: int = 20) -> str:
        candidates = list(self.onchain_address_candidates)
        counts = self.onchain_address_candidate_counts(candidates)
        lines = [
            "地址候选审计",
            "说明: 候选地址不会被 Scan API 采集；只有明确来源验证后才可迁入 active 地址。",
            f"candidate={len(candidates)} | verified={counts.get('verified', 0)} | pending={counts.get('pending', 0)} | rejected={counts.get('rejected', 0)}",
        ]
        if not candidates:
            lines.append("暂无候选地址。")
            return "\n".join(lines)
        for item in candidates[:limit]:
            source = truncate_text(item.candidate_source or "-", 90)
            label = item.label or "-"
            entity = item.entity or "-"
            lines.append(f"{entity} | {item.chain} | {label} | {item.verification_status} | {source}")
        if len(candidates) > limit:
            lines.append(f"共 {len(candidates)} 条，展示前 {limit} 条。")
        return "\n".join(lines)

    def format_onchain_address_query(self, query: str) -> str:
        target = str(query or "").strip()
        if not target:
            return "用法: !地址 USDT 或 !地址 Binance"
        upper_target = target.upper()
        lower_target = target.lower()
        matches = [
            item for item in self.onchain_address_labels
            if upper_target in item.assets
            or lower_target == item.entity.lower()
            or lower_target in item.label.lower()
            or lower_target == item.type.lower()
        ]
        lines = [
            f"地址标签查询: {target}",
            "说明: 自建地址标签覆盖有限，不能等同 Arkham/Nansen 实体标签。",
        ]
        if not matches:
            lines.append("暂无匹配的有效地址标签。")
            return "\n".join(lines)
        for item in matches[:20]:
            assets = "/".join(item.assets) if item.assets else "-"
            lines.append(
                f"- {item.chain} | {item.entity} | {item.type} | {item.label} | "
                f"assets={assets} | confidence={item.confidence} | {short_address(item.address)}"
            )
        if len(matches) > 20:
            lines.append(f"共 {len(matches)} 条，展示前 20 条。")
        return "\n".join(lines)

    def label_for_onchain_address(self, chain: str, address: str) -> OnchainAddressLabel | None:
        normalized_chain = str(chain or "").strip().lower()
        normalized_address = normalize_onchain_address(normalized_chain, address)
        for item in self.onchain_address_labels:
            if item.chain == normalized_chain and normalize_onchain_address(item.chain, item.address) == normalized_address:
                return item
        return None

    def classify_onchain_transfer_event(self, chain: str, from_address: str, to_address: str) -> tuple[str, str, str]:
        from_label = self.label_for_onchain_address(chain, from_address)
        to_label = self.label_for_onchain_address(chain, to_address)
        from_label_text = from_label.label if from_label else short_address(from_address)
        to_label_text = to_label.label if to_label else short_address(to_address)
        if is_zero_onchain_address(from_address):
            return "mint", from_label_text, to_label_text
        if is_zero_onchain_address(to_address):
            return "burn", from_label_text, to_label_text
        if to_label and to_label.type == "exchange":
            if from_label and from_label.type == "treasury":
                return "treasury_to_exchange", from_label_text, to_label_text
            return "exchange_inflow", from_label_text, to_label_text
        if from_label and from_label.type == "exchange":
            return "exchange_outflow", from_label_text, to_label_text
        if from_label and from_label.type == "treasury":
            return "treasury_outflow", from_label_text, to_label_text
        if to_label and to_label.type == "treasury":
            return "treasury_inflow", from_label_text, to_label_text
        return "unknown", from_label_text, to_label_text

    def write_onchain_transfer_events(self, events: list[OnchainTransferEvent]) -> int:
        if not events:
            return 0
        now = time.time()
        rows = [
            (
                event.chain,
                event.tx_hash,
                event.timestamp,
                event.asset,
                event.amount,
                event.amount_usd,
                event.from_address,
                event.to_address,
                event.from_label,
                event.to_label,
                event.direction,
                event.source,
                json.dumps(event.raw_json, ensure_ascii=False) if event.raw_json is not None else None,
                now,
            )
            for event in events
            if event.tx_hash
        ]
        if not rows:
            return 0
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                before = conn.total_changes
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO onchain_transfer_events (
                        chain, tx_hash, timestamp, asset, amount, amount_usd,
                        from_address, to_address, from_label, to_label,
                        direction, source, raw_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )
                return int(conn.total_changes - before)

    def recent_onchain_transfer_events(
        self,
        query: str | None = None,
        since_seconds: int = 86400,
        limit: int = 15,
    ) -> list[dict[str, Any]]:
        cutoff = time.time() - since_seconds
        normalized = str(query or "").strip()
        params: list[Any] = [cutoff]
        where = "timestamp >= ?"
        if normalized:
            like = f"%{normalized.lower()}%"
            where += (
                " AND (lower(asset) = ? OR lower(from_label) LIKE ? OR lower(to_label) LIKE ? "
                "OR lower(from_address) LIKE ? OR lower(to_address) LIKE ?)"
            )
            params.extend([normalized.lower(), like, like, like, like])
        params.append(limit)
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    f"""
                    SELECT chain, tx_hash, timestamp, asset, amount, amount_usd,
                           from_address, to_address, from_label, to_label, direction, source
                    FROM onchain_transfer_events
                    WHERE {where}
                    ORDER BY timestamp DESC
                    LIMIT ?
                    """,
                    params,
                ).fetchall()
        keys = [
            "chain", "tx_hash", "timestamp", "asset", "amount", "amount_usd",
            "from_address", "to_address", "from_label", "to_label", "direction", "source",
        ]
        return [dict(zip(keys, row)) for row in rows]

    def format_onchain_transfer_events(self, query: str | None = None) -> str:
        events = self.recent_onchain_transfer_events(query, limit=15)
        suffix = f" {query}" if query else ""
        lines = [
            f"最近24h链上转账事件{suffix}",
            "说明: 仅来自 onchain_addresses.yaml 已配置地址标签；覆盖有限。",
        ]
        if not events:
            lines.append(self.onchain_event_empty_status_text(query))
            lines.extend(self.format_scan_adapter_status_lines())
            return "\n".join(lines)
        for event in events:
            amount = format_optional_value(parse_float(event.get("amount")))
            asset = str(event.get("asset") or "-")
            lines.append(
                f"{format_ts_short(parse_float(event.get('timestamp')))} | {asset} {amount} | "
                f"{event.get('from_label') or short_address(str(event.get('from_address') or ''))} -> "
                f"{event.get('to_label') or short_address(str(event.get('to_address') or ''))} | "
                f"{event.get('direction') or 'unknown'} | {event.get('source') or '-'}"
            )
        total = self.onchain_transfer_event_query_count(str(query or ""))
        if total > len(events):
            lines.append(f"共 {total} 条，展示前 {len(events)} 条。")
        return "\n".join(lines)

    def write_external_data_points(self, points: list[ExternalDataPoint]) -> None:
        if not points:
            return
        now = time.time()
        rows = [
            (
                point.source,
                point.symbol,
                point.asset,
                point.metric,
                "" if point.value is None else str(point.value),
                point.timestamp,
                json.dumps(point.raw_json, ensure_ascii=False) if point.raw_json is not None else None,
                point.confidence,
                int(point.is_real_onchain),
                now,
            )
            for point in points
        ]
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                conn.executemany(
                    """
                    INSERT INTO external_data_points (
                        source, symbol, asset, metric, value, timestamp, raw_json,
                        confidence, is_real_onchain, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    rows,
                )

    def write_exchange_balance_rows(self, asset: str, balance_ranges: dict[str, Any], source: str = "CoinGlass aggregation") -> None:
        if not balance_ranges:
            return
        now = time.time()
        db_rows = []
        points: list[ExternalDataPoint] = []
        for range_label, balance in balance_ranges.items():
            if not isinstance(balance, dict):
                continue
            raw_json = json.dumps(balance, ensure_ascii=False)
            db_rows.append(
                (
                    source,
                    asset,
                    str(range_label),
                    parse_float(balance.get("balance")),
                    parse_float(balance.get("balance_usd")),
                    parse_float(balance.get("change")),
                    parse_float(balance.get("change_percent")),
                    now,
                    raw_json,
                    now,
                )
            )
            points.append(
                ExternalDataPoint(
                    source=source,
                    symbol=None,
                    asset=asset,
                    metric=f"exchange_balance_{range_label}",
                    value=parse_float(balance.get("change_percent")),
                    timestamp=now,
                    raw_json=balance,
                    confidence="medium",
                    is_real_onchain=False,
                )
            )
            display_range = {"24h": "1d", "7d": "7d", "30d": "30d"}.get(str(range_label), str(range_label))
            points.append(
                ExternalDataPoint(
                    source=source,
                    symbol=f"{asset}USDT",
                    asset=asset,
                    metric=f"exchange_balance_{display_range}",
                    value=parse_float(balance.get("change")),
                    timestamp=now,
                    raw_json=balance,
                    confidence="medium",
                    is_real_onchain=False,
                )
            )
        if not db_rows:
            return
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                conn.executemany(
                    """
                    INSERT INTO exchange_balances (
                        source, asset, range_label, balance, balance_usd, change_value,
                        change_percent, timestamp, raw_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    db_rows,
                )
        self.write_external_data_points(points)

    def persist_coinglass_market_context(self, symbol: str, context: dict[str, Any] | None) -> None:
        if not context:
            self.update_source_health("CoinGlass aggregation", False, "no usable context")
            return
        now = time.time()
        base = str(context.get("base") or (symbol[:-4] if symbol.endswith("USDT") else symbol))
        points: list[ExternalDataPoint] = []

        def add(metric: str, value: Any, raw: Any) -> None:
            points.append(
                ExternalDataPoint(
                    source="CoinGlass aggregation",
                    symbol=symbol,
                    asset=base,
                    metric=metric,
                    value=value,
                    timestamp=now,
                    raw_json=raw,
                    confidence="medium",
                    is_real_onchain=False,
                )
            )

        oi = context.get("open_interest") if isinstance(context.get("open_interest"), dict) else {}
        for key, value in oi.items():
            add(f"open_interest_{key}", value, oi)
        add("oi_1h", oi.get("change_1h"), oi)
        add("oi_4h", oi.get("change_4h"), oi)
        add("oi_24h", oi.get("change_24h"), oi)
        if "funding_oi_weight" in context:
            add("funding_oi_weight", context.get("funding_oi_weight"), {"value": context.get("funding_oi_weight")})
            add("funding_current", context.get("funding_oi_weight"), {"value": context.get("funding_oi_weight")})
        taker = context.get("taker_flow") if isinstance(context.get("taker_flow"), dict) else {}
        for key, value in taker.items():
            add(f"taker_flow_{key}", value, taker)
        add("taker_buy_24h", taker.get("buy_ratio"), taker)
        add("taker_sell_24h", taker.get("sell_ratio"), taker)
        orderbook = context.get("orderbook") if isinstance(context.get("orderbook"), dict) else {}
        for key, value in orderbook.items():
            add(f"orderbook_{key}", value, orderbook)
        add("orderbook_bid_1h", orderbook.get("bids_usd_1h"), orderbook)
        add("orderbook_ask_1h", orderbook.get("asks_usd_1h"), orderbook)
        add("orderbook_bid_4h", orderbook.get("bids_usd_avg_4h"), orderbook)
        add("orderbook_ask_4h", orderbook.get("asks_usd_avg_4h"), orderbook)
        major_long = context.get("major_long") if isinstance(context.get("major_long"), dict) else {}
        taker_ranges = major_long.get("taker_ranges") if isinstance(major_long.get("taker_ranges"), dict) else {}
        for range_label, values in taker_ranges.items():
            if isinstance(values, dict):
                for key, value in values.items():
                    add(f"taker_flow_{range_label}_{key}", value, values)
        funding_ranges = (
            major_long.get("funding_accumulated_ranges")
            if isinstance(major_long.get("funding_accumulated_ranges"), dict)
            else {}
        )
        for range_label, values in funding_ranges.items():
            if isinstance(values, dict):
                add(f"funding_accumulated_{range_label}", values.get("rate"), values)
                display_range = {"24h": "1d", "7d": "7d"}.get(str(range_label), str(range_label))
                add(f"funding_{display_range}", values.get("rate"), values)
        balance_ranges = major_long.get("balance_ranges") if isinstance(major_long.get("balance_ranges"), dict) else {}
        self.write_exchange_balance_rows(base, balance_ranges)
        self.write_external_data_points(points)
        self.update_source_health("CoinGlass aggregation", True)

    def persist_dexscreener_cached_snapshot(self, symbol: str) -> None:
        pair = _DEXSCREENER_PAIR_CACHE.get(str(symbol or "").upper())
        if not isinstance(pair, dict):
            return
        self.persist_dexscreener_pair_snapshot(symbol, pair)

    def persist_dexscreener_pair_snapshot(self, symbol: str, pair: dict[str, Any]) -> None:
        now = time.time()
        price_change = pair.get("priceChange") or {}
        volume = pair.get("volume") or {}
        liquidity = pair.get("liquidity") or {}
        base_token = pair.get("baseToken") or {}
        quote_token = pair.get("quoteToken") or {}
        row = (
            "DexScreener",
            str(symbol or "").upper(),
            str(base_token.get("symbol") or normalize_dex_symbol(symbol) or ""),
            str(pair.get("chainId") or ""),
            str(pair.get("pairAddress") or ""),
            str(pair.get("dexId") or ""),
            str(quote_token.get("symbol") or ""),
            safe_float(price_change.get("m5")),
            safe_float(price_change.get("h1")),
            safe_float(price_change.get("h24")),
            safe_float(volume.get("m5")),
            safe_float(volume.get("h1")),
            safe_float(volume.get("h24")),
            safe_float(liquidity.get("usd")),
            safe_float(pair.get("fdv")),
            safe_float(pair.get("marketCap")),
            now,
            json.dumps(pair, ensure_ascii=False),
            now,
        )
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                conn.execute(
                    """
                    INSERT INTO dex_market_snapshots (
                        source, symbol, asset, chain, pair_address, dex, quote_symbol,
                        price_change_5m, price_change_1h, price_change_24h,
                        volume_5m_usd, volume_1h_usd, volume_24h_usd,
                        liquidity_usd, fdv, market_cap, timestamp, raw_json, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    row,
                )
        self.write_external_data_points(
            [
                ExternalDataPoint("DexScreener", str(symbol or "").upper(), row[2], "dex_price_change_1h", row[8], now, pair, "medium", False),
                ExternalDataPoint("DexScreener", str(symbol or "").upper(), row[2], "dex_volume_24h", row[12], now, pair, "medium", False),
                ExternalDataPoint("DexScreener", str(symbol or "").upper(), row[2], "dex_volume_24h_usd", row[12], now, pair, "medium", False),
                ExternalDataPoint("DexScreener", str(symbol or "").upper(), row[2], "dex_liquidity_usd", row[13], now, pair, "medium", False),
            ]
        )
        self.update_source_health("DexScreener", True, fetched_count=1, written_count=4)

    def fetch_dexscreener_pair_for_symbol(self, symbol: str) -> dict[str, Any] | None:
        normalized = normalize_usdt_symbol(symbol)
        base = normalize_dex_symbol(normalized)
        if not base:
            return None
        response = self.session.get(
            "https://api.dexscreener.com/latest/dex/search",
            params={"q": base},
            timeout=8,
        )
        response.raise_for_status()
        payload = response.json()
        pairs = payload.get("pairs") if isinstance(payload, dict) else None
        if not isinstance(pairs, list):
            return None
        pair = best_dex_pair(base, pairs)
        if pair:
            _DEXSCREENER_PAIR_CACHE[normalized] = pair
        return pair

    def dexscreener_collection_symbols(self) -> list[str]:
        symbols: list[str] = list(ONCHAIN_SUMMARY_SYMBOLS)
        with self.discord_alt_watch_lock:
            symbols.extend(item.symbol for item in self.discord_alt_watch_queue[:20])
        symbols.extend(self.latest_snapshots.keys())
        try:
            for row in self.load_recent_signal_rows(120):
                symbol = str(row.get("symbol") or "").strip().upper()
                if symbol:
                    symbols.append(symbol)
        except Exception:
            logging.debug("Failed to read recent signals for DexScreener collection", exc_info=True)
        deduped = []
        seen = set()
        for symbol in symbols:
            normalized = normalize_usdt_symbol(symbol)
            if normalized in seen or not is_valid_binance_usdt_symbol(normalized):
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped[:DEXSCREENER_COLLECT_LIMIT]

    def collect_dexscreener_market_snapshots_if_due(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_dexscreener_collect_at < DEXSCREENER_MARKET_TTL_SECONDS:
            return
        self.last_dexscreener_collect_at = now
        fetched = 0
        written = 0
        no_pair = 0
        for symbol in self.dexscreener_collection_symbols():
            if not force and now - self.dexscreener_symbol_collect_cache.get(symbol, 0.0) < DEXSCREENER_MARKET_TTL_SECONDS:
                continue
            self.dexscreener_symbol_collect_cache[symbol] = now
            try:
                pair = self.fetch_dexscreener_pair_for_symbol(symbol)
                fetched += 1
                if not pair:
                    no_pair += 1
                    continue
                self.persist_dexscreener_pair_snapshot(symbol, pair)
                written += 1
            except Exception as exc:
                logging.debug("DexScreener market snapshot failed: symbol=%s", symbol, exc_info=True)
                self.update_source_health("DexScreener", False, f"{type(exc).__name__}: {exc}", fetched_count=fetched, written_count=written)
        if fetched:
            error = f"no pair for {no_pair} symbols" if no_pair else None
            self.update_source_health("DexScreener", True if written else False, error, fetched_count=fetched, written_count=written)
            logging.info("DexScreener market snapshots collected: fetched=%s written=%s no_pair=%s", fetched, written, no_pair)

    def collect_defillama_stablecoin_supply_if_due(self, force: bool = False) -> None:
        now = time.time()
        spec = self.data_sources.get("DefiLlama stablecoin supply")
        if not spec or not spec.enabled:
            return
        if not force and now - self.last_external_stablecoin_collect_at < spec.ttl_seconds:
            return
        self.last_external_stablecoin_collect_at = now
        try:
            response = self.session.get(DEFILLAMA_STABLECOINS_URL, params={"includePrices": "true"}, timeout=12)
            response.raise_for_status()
            payload = response.json()
            assets = payload.get("peggedAssets") if isinstance(payload, dict) else None
            if not isinstance(assets, list):
                self.update_source_health(spec.name, False, "unexpected DefiLlama response")
                return
            rows = []
            points: list[ExternalDataPoint] = []
            for asset in assets:
                if not isinstance(asset, dict):
                    continue
                symbol = str(asset.get("symbol") or "").upper()
                if symbol not in {"USDT", "USDC"}:
                    continue
                circulating = asset.get("circulating") if isinstance(asset.get("circulating"), dict) else {}
                supply_usd = parse_float(circulating.get("peggedUSD")) or parse_float(asset.get("circulatingUSD"))
                supply_native = parse_float(circulating.get("peggedUSD"))
                raw_json = json.dumps(asset, ensure_ascii=False)
                rows.append((spec.name, symbol, supply_usd, supply_native, now, raw_json, now))
                points.append(
                    ExternalDataPoint(
                        source=spec.name,
                        symbol=None,
                        asset=symbol,
                        metric="stablecoin_supply_usd",
                        value=supply_usd,
                        timestamp=now,
                        raw_json=asset,
                        confidence=spec.confidence,
                        is_real_onchain=False,
                    )
                )
                for chain_name, chain_value in stablecoin_chain_distribution(asset):
                    metric_suffix = defillama_chain_metric_suffix(chain_name)
                    if metric_suffix not in {"tron", "ethereum", "solana", "bsc", "arbitrum", "base"}:
                        continue
                    points.append(
                        ExternalDataPoint(
                            source=spec.name,
                            symbol=None,
                            asset=symbol,
                            metric=f"stablecoin_supply_{metric_suffix}",
                            value=chain_value,
                            timestamp=now,
                            raw_json={"chain": chain_name, "asset": symbol, "source": "DefiLlama stablecoin chain distribution"},
                            confidence=spec.confidence,
                            is_real_onchain=False,
                        )
                    )
            if not rows:
                self.update_source_health(spec.name, False, "USDT/USDC not found")
                return
            with self.external_data_lock:
                with self.external_db_connection() as conn:
                    conn.executemany(
                        """
                        INSERT INTO stablecoin_supply (
                            source, asset, supply_usd, supply_native, timestamp, raw_json, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        rows,
                    )
            self.write_external_data_points(points)
            self.update_source_health(spec.name, True, fetched_count=len(assets), written_count=len(points))
        except Exception as exc:
            logging.debug("DefiLlama stablecoin supply fetch failed", exc_info=True)
            self.update_source_health(spec.name, False, f"{type(exc).__name__}: {exc}")

    def collect_defillama_extended_metrics_if_due(self, force: bool = False) -> None:
        now = time.time()
        spec = self.data_sources.get("DefiLlama TVL/DEX metrics")
        if not spec or not spec.enabled:
            return
        if not force and now - self.last_defillama_extended_collect_at < spec.ttl_seconds:
            return
        self.last_defillama_extended_collect_at = now
        points: list[ExternalDataPoint] = []
        fetched_count = 0
        errors: list[str] = []
        try:
            chain_response = self.session.get(DEFILLAMA_CHAINS_URL, timeout=12)
            chain_response.raise_for_status()
            chain_rows = chain_response.json()
            if isinstance(chain_rows, list):
                fetched_count += len(chain_rows)
                wanted = {"ethereum", "tron", "solana", "bsc", "arbitrum", "base"}
                for row in chain_rows:
                    if not isinstance(row, dict):
                        continue
                    chain_name = str(row.get("name") or row.get("chain") or "").strip()
                    suffix = defillama_chain_metric_suffix(chain_name)
                    if suffix not in wanted:
                        continue
                    points.append(
                        ExternalDataPoint(
                            source=spec.name,
                            symbol=None,
                            asset=chain_name,
                            metric="chain_tvl_usd",
                            value=parse_float(row.get("tvl")),
                            timestamp=now,
                            raw_json=row,
                            confidence=spec.confidence,
                            is_real_onchain=False,
                        )
                    )
        except Exception as exc:
            errors.append(f"chain TVL {type(exc).__name__}: {exc}")

        try:
            dex_response = self.session.get(
                DEFILLAMA_DEX_OVERVIEW_URL,
                params={"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true"},
                timeout=12,
            )
            dex_response.raise_for_status()
            dex_payload = dex_response.json()
            dex_rows = dex_payload.get("protocols") if isinstance(dex_payload, dict) else None
            if isinstance(dex_rows, list):
                fetched_count += len(dex_rows)
                global_24h = parse_float(dex_payload.get("total24h")) if isinstance(dex_payload, dict) else None
                global_7d = parse_float(dex_payload.get("total7d")) if isinstance(dex_payload, dict) else None
                points.append(ExternalDataPoint(spec.name, None, "global", "dex_volume_24h", global_24h, now, dex_payload, spec.confidence, False))
                points.append(ExternalDataPoint(spec.name, None, "global", "dex_volume_7d", global_7d, now, dex_payload, spec.confidence, False))
                chain_totals: dict[str, dict[str, float]] = {}
                for row in dex_rows:
                    if not isinstance(row, dict):
                        continue
                    chain_name = str(row.get("chain") or "").strip()
                    suffix = defillama_chain_metric_suffix(chain_name)
                    if suffix not in {"ethereum", "tron", "solana", "bsc", "arbitrum", "base"}:
                        continue
                    bucket = chain_totals.setdefault(chain_name, {"24h": 0.0, "7d": 0.0})
                    bucket["24h"] += parse_float(row.get("total24h")) or 0.0
                    bucket["7d"] += parse_float(row.get("total7d")) or 0.0
                for chain_name, values in chain_totals.items():
                    points.append(ExternalDataPoint(spec.name, None, chain_name, "dex_volume_24h", values["24h"], now, {"chain": chain_name}, spec.confidence, False))
                    points.append(ExternalDataPoint(spec.name, None, chain_name, "dex_volume_7d", values["7d"], now, {"chain": chain_name}, spec.confidence, False))
        except Exception as exc:
            errors.append(f"DEX volume {type(exc).__name__}: {exc}")

        try:
            fees_response = self.session.get(
                DEFILLAMA_FEES_OVERVIEW_URL,
                params={"excludeTotalDataChart": "true", "excludeTotalDataChartBreakdown": "true", "dataType": "dailyFees"},
                timeout=12,
            )
            fees_response.raise_for_status()
            fees_payload = fees_response.json()
            fee_rows = fees_payload.get("protocols") if isinstance(fees_payload, dict) else None
            if isinstance(fee_rows, list):
                fetched_count += len(fee_rows)
                points.append(ExternalDataPoint(spec.name, None, "global", "protocol_fees_24h", parse_float(fees_payload.get("total24h")), now, fees_payload, spec.confidence, False))
                for row in sorted(
                    (item for item in fee_rows if isinstance(item, dict)),
                    key=lambda item: parse_float(item.get("total24h")) or 0.0,
                    reverse=True,
                )[:20]:
                    protocol = str(row.get("name") or row.get("displayName") or row.get("slug") or "").strip()
                    if not protocol:
                        continue
                    points.append(ExternalDataPoint(spec.name, None, protocol, "protocol_fees_24h", parse_float(row.get("total24h")), now, row, spec.confidence, False))
                    revenue = parse_float(row.get("revenue24h") or row.get("dailyRevenue"))
                    if revenue is not None:
                        points.append(ExternalDataPoint(spec.name, None, protocol, "protocol_revenue_24h", revenue, now, row, spec.confidence, False))
        except Exception as exc:
            errors.append(f"fees {type(exc).__name__}: {exc}")

        if points:
            self.write_external_data_points(points)
            self.update_source_health(spec.name, True, fetched_count=fetched_count, written_count=len(points))
            logging.info("DefiLlama extended metrics collected: fetched=%s written=%s", fetched_count, len(points))
        else:
            self.update_source_health(spec.name, False, "; ".join(errors) or "no DefiLlama extended metrics", fetched_count=fetched_count, written_count=0)

    def collect_onchain_scan_transfers_if_due(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_onchain_scan_collect_at < ONCHAIN_SCAN_INTERVAL_SECONDS:
            return
        self.last_onchain_scan_collect_at = now
        labels = [
            item for item in self.onchain_address_labels
            if item.chain in {"ethereum", "tron"} and any(asset in {"USDT", "USDC", "ETH"} for asset in item.assets)
        ]
        if not labels:
            for source_name in ("Etherscan", "Tronscan"):
                spec = self.data_sources.get(source_name)
                if spec and spec.enabled:
                    self.update_source_health(
                        source_name,
                        False,
                        "no configured onchain address labels",
                        scanned_addresses=0,
                        fetched_events=0,
                        written_events=0,
                    )
            solscan = self.data_sources.get("Solscan")
            if solscan and not solscan.enabled:
                self.update_source_health(
                    "Solscan",
                    False,
                    "SOLSCAN_API_KEY not configured; adapter reserved",
                    scanned_addresses=0,
                    fetched_events=0,
                    written_events=0,
                )
            return
        attempted = 0
        scan_totals = {"Etherscan": [0, 0, 0], "Tronscan": [0, 0, 0]}
        for label in labels:
            if attempted >= ONCHAIN_SCAN_ADDRESS_LIMIT:
                break
            cache_key = (label.chain, normalize_onchain_address(label.chain, label.address))
            if not force and now - self.onchain_scan_address_cache.get(cache_key, 0.0) < ONCHAIN_SCAN_ADDRESS_TTL_SECONDS:
                continue
            self.onchain_scan_address_cache[cache_key] = now
            attempted += 1
            if label.chain == "ethereum":
                result = self.collect_etherscan_transfers_for_label(label)
                if result is not None:
                    fetched, written = result
                    scan_totals["Etherscan"][0] += 1
                    scan_totals["Etherscan"][1] += fetched
                    scan_totals["Etherscan"][2] += written
            elif label.chain == "tron":
                result = self.collect_tron_transfers_for_label(label)
                if result is not None:
                    fetched, written = result
                    scan_totals["Tronscan"][0] += 1
                    scan_totals["Tronscan"][1] += fetched
                    scan_totals["Tronscan"][2] += written
        for source_name, values in scan_totals.items():
            if values[0] > 0:
                self.update_source_health(
                    source_name,
                    True,
                    scanned_addresses=values[0],
                    fetched_events=values[1],
                    written_events=values[2],
                    last_scan_at=now,
                )
                logging.info(
                    "onchain transfer scan %s: scanned=%s fetched=%s written=%s",
                    source_name,
                    values[0],
                    values[1],
                    values[2],
                )
        solscan = self.data_sources.get("Solscan")
        if solscan:
            if solscan.enabled:
                self.update_source_health(
                    "Solscan",
                    False,
                    "adapter reserved; collection not implemented",
                    scanned_addresses=0,
                    fetched_events=0,
                    written_events=0,
                )
            else:
                self.update_source_health(
                    "Solscan",
                    False,
                    "SOLSCAN_API_KEY not configured; adapter reserved",
                    scanned_addresses=0,
                    fetched_events=0,
                    written_events=0,
                )

    def collect_etherscan_transfers_for_label(self, label: OnchainAddressLabel) -> tuple[int, int] | None:
        api_key = os.getenv("ETHERSCAN_API_KEY", "").strip()
        source_name = "Etherscan"
        if not api_key:
            self.update_source_health(source_name, False, "ETHERSCAN_API_KEY not configured", scanned_addresses=0, fetched_events=0, written_events=0)
            return None
        if not any(asset in {"USDT", "USDC", "ETH"} for asset in label.assets):
            return None
        try:
            events: list[OnchainTransferEvent] = []
            token_params = {
                "module": "account",
                "action": "tokentx",
                "address": label.address,
                "page": 1,
                "offset": ONCHAIN_SCAN_TRANSFER_LIMIT,
                "sort": "desc",
                "apikey": api_key,
            }
            response = self.session.get("https://api.etherscan.io/api", params=token_params, timeout=12)
            response.raise_for_status()
            payload = response.json()
            result = payload.get("result") if isinstance(payload, dict) else None
            if isinstance(result, list):
                for row in result:
                    if not isinstance(row, dict):
                        continue
                    asset = str(row.get("tokenSymbol") or "").upper()
                    if asset not in {"USDT", "USDC"} or asset not in label.assets:
                        continue
                    events.append(self.etherscan_token_row_to_event(row, asset))
            if "ETH" in label.assets:
                eth_params = dict(token_params)
                eth_params["action"] = "txlist"
                response = self.session.get("https://api.etherscan.io/api", params=eth_params, timeout=12)
                response.raise_for_status()
                payload = response.json()
                result = payload.get("result") if isinstance(payload, dict) else None
                if isinstance(result, list):
                    for row in result:
                        if isinstance(row, dict):
                            events.append(self.etherscan_eth_row_to_event(row))
            written = self.write_onchain_transfer_events([event for event in events if event])
            logging.info(
                "onchain transfer scan Etherscan: address=%s scanned_addresses=1 fetched_events=%s written_events=%s",
                short_address(label.address),
                len(events),
                written,
            )
            self.update_source_health(source_name, True, scanned_addresses=1, fetched_events=len(events), written_events=written, last_scan_at=time.time())
            return len(events), written
        except Exception as exc:
            logging.debug("Etherscan onchain transfer scan failed: address=%s", short_address(label.address), exc_info=True)
            self.update_source_health(source_name, False, f"{type(exc).__name__}: {exc}", scanned_addresses=1, fetched_events=0, written_events=0, last_scan_at=time.time())
            return None

    def etherscan_token_row_to_event(self, row: dict[str, Any], asset: str) -> OnchainTransferEvent:
        decimals = parse_float(row.get("tokenDecimal")) or 0
        raw_value = parse_float(row.get("value"))
        amount = raw_value / (10 ** int(decimals)) if raw_value is not None and decimals >= 0 else raw_value
        from_address = str(row.get("from") or "")
        to_address = str(row.get("to") or "")
        direction, from_label, to_label = self.classify_onchain_transfer_event("ethereum", from_address, to_address)
        return OnchainTransferEvent(
            chain="ethereum",
            tx_hash=str(row.get("hash") or ""),
            timestamp=parse_float(row.get("timeStamp")) or time.time(),
            asset=asset,
            amount=amount,
            amount_usd=None,
            from_address=from_address,
            to_address=to_address,
            from_label=from_label,
            to_label=to_label,
            direction=direction,
            source="Etherscan",
            raw_json=row,
        )

    def etherscan_eth_row_to_event(self, row: dict[str, Any]) -> OnchainTransferEvent:
        raw_value = parse_float(row.get("value"))
        amount = raw_value / 1_000_000_000_000_000_000 if raw_value is not None else None
        from_address = str(row.get("from") or "")
        to_address = str(row.get("to") or "")
        direction, from_label, to_label = self.classify_onchain_transfer_event("ethereum", from_address, to_address)
        return OnchainTransferEvent(
            chain="ethereum",
            tx_hash=str(row.get("hash") or ""),
            timestamp=parse_float(row.get("timeStamp")) or time.time(),
            asset="ETH",
            amount=amount,
            amount_usd=None,
            from_address=from_address,
            to_address=to_address,
            from_label=from_label,
            to_label=to_label,
            direction=direction,
            source="Etherscan",
            raw_json=row,
        )

    def collect_tron_transfers_for_label(self, label: OnchainAddressLabel) -> tuple[int, int] | None:
        trongrid_key = os.getenv("TRONGRID_API_KEY", "").strip()
        tronscan_key = os.getenv("TRONSCAN_API_KEY", "").strip()
        source_name = "Tronscan"
        if not (trongrid_key or tronscan_key):
            self.update_source_health(source_name, False, "TRONSCAN_API_KEY/TRONGRID_API_KEY not configured", scanned_addresses=0, fetched_events=0, written_events=0)
            return None
        if "USDT" not in label.assets:
            return None
        try:
            if trongrid_key:
                events = self.fetch_trongrid_trc20_transfers(label, trongrid_key)
                source_label = "TronGrid"
            else:
                events = self.fetch_tronscan_trc20_transfers(label, tronscan_key)
                source_label = "Tronscan"
            written = self.write_onchain_transfer_events(events)
            logging.info(
                "onchain transfer scan %s: address=%s scanned_addresses=1 fetched_events=%s written_events=%s",
                source_label,
                short_address(label.address),
                len(events),
                written,
            )
            self.update_source_health(source_name, True, scanned_addresses=1, fetched_events=len(events), written_events=written, last_scan_at=time.time())
            return len(events), written
        except Exception as exc:
            logging.debug("Tron onchain transfer scan failed: address=%s", short_address(label.address), exc_info=True)
            self.update_source_health(source_name, False, f"{type(exc).__name__}: {exc}", scanned_addresses=1, fetched_events=0, written_events=0, last_scan_at=time.time())
            return None

    def fetch_trongrid_trc20_transfers(self, label: OnchainAddressLabel, api_key: str) -> list[OnchainTransferEvent]:
        headers = {"TRON-PRO-API-KEY": api_key} if api_key else {}
        url = f"https://api.trongrid.io/v1/accounts/{label.address}/transactions/trc20"
        params = {"limit": ONCHAIN_SCAN_TRANSFER_LIMIT, "only_confirmed": "true", "order_by": "block_timestamp,desc"}
        response = self.session.get(url, headers=headers, params=params, timeout=12)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data") if isinstance(payload, dict) else None
        events: list[OnchainTransferEvent] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    event = self.trongrid_row_to_event(row)
                    if event.asset == "USDT":
                        events.append(event)
        return events

    def fetch_tronscan_trc20_transfers(self, label: OnchainAddressLabel, api_key: str) -> list[OnchainTransferEvent]:
        headers = {"TRON-PRO-API-KEY": api_key} if api_key else {}
        params = {
            "limit": ONCHAIN_SCAN_TRANSFER_LIMIT,
            "start": 0,
            "sort": "-timestamp",
            "relatedAddress": label.address,
        }
        response = self.session.get("https://apilist.tronscanapi.com/api/token_trc20/transfers", headers=headers, params=params, timeout=12)
        response.raise_for_status()
        payload = response.json()
        rows = (payload.get("token_transfers") or payload.get("data")) if isinstance(payload, dict) else None
        events: list[OnchainTransferEvent] = []
        if isinstance(rows, list):
            for row in rows:
                if isinstance(row, dict):
                    event = self.tronscan_row_to_event(row)
                    if event.asset == "USDT":
                        events.append(event)
        return events

    def trongrid_row_to_event(self, row: dict[str, Any]) -> OnchainTransferEvent:
        token_info = row.get("token_info") if isinstance(row.get("token_info"), dict) else {}
        asset = str(token_info.get("symbol") or "").upper()
        decimals = parse_float(token_info.get("decimals")) or 6
        raw_value = parse_float(row.get("value"))
        amount = raw_value / (10 ** int(decimals)) if raw_value is not None else None
        from_address = str(row.get("from") or "")
        to_address = str(row.get("to") or "")
        direction, from_label, to_label = self.classify_onchain_transfer_event("tron", from_address, to_address)
        timestamp_ms = parse_float(row.get("block_timestamp"))
        return OnchainTransferEvent(
            chain="tron",
            tx_hash=str(row.get("transaction_id") or row.get("txID") or ""),
            timestamp=(timestamp_ms / 1000) if timestamp_ms and timestamp_ms > 10_000_000_000 else (timestamp_ms or time.time()),
            asset=asset,
            amount=amount,
            amount_usd=None,
            from_address=from_address,
            to_address=to_address,
            from_label=from_label,
            to_label=to_label,
            direction=direction,
            source="TronGrid",
            raw_json=row,
        )

    def tronscan_row_to_event(self, row: dict[str, Any]) -> OnchainTransferEvent:
        token_info = row.get("tokenInfo") if isinstance(row.get("tokenInfo"), dict) else {}
        asset = str(token_info.get("tokenAbbr") or token_info.get("symbol") or row.get("symbol") or "").upper()
        decimals = parse_float(token_info.get("tokenDecimal") or token_info.get("decimals")) or 6
        raw_value = parse_float(row.get("quant") or row.get("amount") or row.get("value"))
        amount = raw_value / (10 ** int(decimals)) if raw_value is not None else None
        from_address = str(row.get("from_address") or row.get("fromAddress") or row.get("from") or "")
        to_address = str(row.get("to_address") or row.get("toAddress") or row.get("to") or "")
        direction, from_label, to_label = self.classify_onchain_transfer_event("tron", from_address, to_address)
        timestamp_ms = parse_float(row.get("block_ts") or row.get("timestamp") or row.get("block_timestamp"))
        return OnchainTransferEvent(
            chain="tron",
            tx_hash=str(row.get("transaction_id") or row.get("transactionHash") or row.get("hash") or ""),
            timestamp=(timestamp_ms / 1000) if timestamp_ms and timestamp_ms > 10_000_000_000 else (timestamp_ms or time.time()),
            asset=asset,
            amount=amount,
            amount_usd=None,
            from_address=from_address,
            to_address=to_address,
            from_label=from_label,
            to_label=to_label,
            direction=direction,
            source="Tronscan",
            raw_json=row,
        )

    def collect_external_data_if_due(self) -> None:
        self.collect_defillama_stablecoin_supply_if_due()
        self.collect_defillama_extended_metrics_if_due()
        self.collect_dexscreener_market_snapshots_if_due()
        self.collect_onchain_scan_transfers_if_due()

    def persist_external_confirmation_metrics(
        self,
        snapshot: MarketSnapshot,
        signal: Signal | None = None,
        spot_text: str | None = None,
        coinglass_text: str | None = None,
    ) -> None:
        resolved_spot_text = spot_text if spot_text is not None else cached_spot_alpha_confirmation(snapshot.symbol)
        spot_score, spot_label, spot_reason = spot_onchain_score_from_text(snapshot, signal, resolved_spot_text)
        div_label, div_score, div_reason = contract_spot_divergence_from_text(snapshot, signal, resolved_spot_text)
        major_score, major_label, major_reason = major_flow_score_from_text(snapshot, signal, coinglass_text)
        now = time.time()
        self.write_external_data_points(
            [
                ExternalDataPoint(
                    "Derived spot/DEX confirmation",
                    snapshot.symbol,
                    snapshot.symbol[:-4] if snapshot.symbol.endswith("USDT") else snapshot.symbol,
                    "spot_external_score",
                    spot_score,
                    now,
                    {"label": spot_label, "reason": spot_reason, "source_text": resolved_spot_text},
                    "low",
                    False,
                ),
                ExternalDataPoint(
                    "Derived spot/DEX confirmation",
                    snapshot.symbol,
                    snapshot.symbol[:-4] if snapshot.symbol.endswith("USDT") else snapshot.symbol,
                    "contract_spot_divergence_score",
                    div_score,
                    now,
                    {"label": div_label, "reason": div_reason},
                    "low",
                    False,
                ),
                ExternalDataPoint(
                    "Derived spot/DEX confirmation",
                    snapshot.symbol,
                    snapshot.symbol[:-4] if snapshot.symbol.endswith("USDT") else snapshot.symbol,
                    "major_external_flow_score",
                    major_score,
                    now,
                    {"label": major_label, "reason": major_reason},
                    "low",
                    False,
                ),
            ]
        )
        self.update_source_health("Derived spot/DEX confirmation", True)

    def external_source_recent_counts(self, since_seconds: int = 86400) -> dict[str, int]:
        cutoff = time.time() - since_seconds
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    "SELECT source, COUNT(*) FROM external_data_points WHERE timestamp >= ? GROUP BY source",
                    (cutoff,),
                ).fetchall()
                event_rows = conn.execute(
                    "SELECT source, COUNT(*) FROM onchain_transfer_events WHERE timestamp >= ? GROUP BY source",
                    (cutoff,),
                ).fetchall()
        counts = {str(source): int(count) for source, count in rows}
        for source, count in event_rows:
            key = str(source)
            if key == "TronGrid":
                key = "Tronscan"
            counts[key] = counts.get(key, 0) + int(count)
        return counts

    def external_collection_table_counts(self, since_seconds: int = 86400) -> dict[str, int]:
        cutoff = time.time() - since_seconds
        tables = (
            ("external_data_points", "timestamp"),
            ("stablecoin_supply", "timestamp"),
            ("exchange_balances", "timestamp"),
            ("dex_market_snapshots", "timestamp"),
            ("onchain_transfer_events", "timestamp"),
        )
        counts: dict[str, int] = {}
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                for table, column in tables:
                    row = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE {column} >= ?", (cutoff,)).fetchone()
                    counts[table] = int(row[0] if row else 0)
        return counts

    def external_collection_source_top(self, since_seconds: int = 86400, limit: int = 10) -> list[tuple[str, int]]:
        cutoff = time.time() - since_seconds
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT source, COUNT(*) AS count
                    FROM (
                        SELECT source, timestamp FROM external_data_points
                        UNION ALL
                        SELECT source, timestamp FROM stablecoin_supply
                        UNION ALL
                        SELECT source, timestamp FROM exchange_balances
                        UNION ALL
                        SELECT source, timestamp FROM dex_market_snapshots
                        UNION ALL
                        SELECT source, timestamp FROM onchain_transfer_events
                    )
                    WHERE timestamp >= ?
                    GROUP BY source
                    ORDER BY count DESC
                    LIMIT ?
                    """,
                    (cutoff, limit),
                ).fetchall()
        return [(str(source), int(count)) for source, count in rows]

    def metric_recent_count(self, source: str, metric_prefixes: tuple[str, ...], since_seconds: int = 86400) -> int:
        cutoff = time.time() - since_seconds
        clauses = " OR ".join("metric LIKE ?" for _prefix in metric_prefixes)
        params: list[Any] = [source, cutoff]
        params.extend(f"{prefix}%" for prefix in metric_prefixes)
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                row = conn.execute(
                    f"""
                    SELECT COUNT(*)
                    FROM external_data_points
                    WHERE source = ? AND timestamp >= ? AND ({clauses})
                    """,
                    params,
                ).fetchone()
        return int(row[0] if row else 0)

    def format_external_collection_stats(self) -> str:
        self.collect_external_data_if_due()
        table_counts = self.external_collection_table_counts()
        source_top = self.external_collection_source_top()
        lines = ["外部数据采集统计（最近24h）"]
        for table in (
            "external_data_points",
            "stablecoin_supply",
            "exchange_balances",
            "dex_market_snapshots",
            "onchain_transfer_events",
        ):
            lines.append(f"- {table}: {table_counts.get(table, 0)}")
        lines.append("")
        lines.append("按 source Top10:")
        if source_top:
            lines.extend(f"- {source}: {count}" for source, count in source_top)
        else:
            lines.append("- 暂无最近24h采集数据")
        return "\n".join(lines)

    def format_external_source_health(self, only_available: bool = False) -> str:
        self.collect_defillama_stablecoin_supply_if_due()
        self.collect_defillama_extended_metrics_if_due()
        self.collect_dexscreener_market_snapshots_if_due()
        self.collect_onchain_scan_transfers_if_due()
        counts = self.external_source_recent_counts()
        table_counts = self.external_collection_table_counts()
        defillama_extended_count = counts.get("DefiLlama TVL/DEX metrics", 0)
        dex_snapshot_count = table_counts.get("dex_market_snapshots", 0)
        coinglass_structured_count = self.metric_recent_count(
            "CoinGlass aggregation",
            (
                "exchange_balance_",
                "oi_",
                "funding_",
                "taker_buy_",
                "taker_sell_",
                "orderbook_",
            ),
        )
        lines = [
            "数据源健康检查",
            "说明: 现阶段不把外部数据接入交易信号加减分；真实链上=是仅代表钱包/交易级链上事件。",
            (
                "扩展采集24h: "
                f"DefiLlama={defillama_extended_count} / "
                f"DexScreener snapshots={dex_snapshot_count} / "
                f"CoinGlass structured={coinglass_structured_count}"
            ),
        ]
        for spec in sorted(self.data_sources.values(), key=lambda item: item.priority):
            count = counts.get(spec.name, 0)
            if only_available and not (spec.enabled and (spec.last_success or count > 0)):
                continue
            success_text = format_ts_short(spec.last_success) if spec.last_success else "-"
            error_text = spec.last_error or "-"
            lines.append(
                f"- {spec.name} | {spec.category} | enabled={yes_no(spec.enabled)} | "
                f"真实链上={yes_no(spec.is_real_onchain)} | key={yes_no(spec.requires_api_key)} | "
                f"confidence={spec.confidence} | last_success={success_text} | "
                f"last_error={truncate_text(error_text, 80)} | 24h数据={count} | "
                f"fetched={spec.fetched_count} written={spec.written_count}"
            )
            if spec.name in {"Etherscan", "Tronscan", "Solscan"}:
                lines.append(
                    f"  scan: scanned_addresses={spec.scanned_addresses} / "
                    f"fetched_events={spec.fetched_events} / written_events={spec.written_events}"
                )
            if spec.name == "DefiLlama stablecoin supply":
                lines.append("  说明: 稳定币供应聚合，不等同钱包流/交易所净流")
            if spec.name == "Address labels":
                lines.append("  说明: 自建地址标签覆盖有限，不能等同 Arkham/Nansen 实体标签")
        if len(lines) <= 2:
            lines.append("当前没有确认可用的外部资金数据源。")
        return "\n".join(lines)

    def format_stablecoin_external_funds(self, asset: str) -> str:
        normalized = str(asset or "").strip().upper()
        if normalized not in {"USDT", "USDC"}:
            return "暂无该资产外部资金数据"
        self.collect_defillama_stablecoin_supply_if_due()
        self.collect_onchain_scan_transfers_if_due()
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT supply_usd, supply_native, timestamp, raw_json
                    FROM stablecoin_supply
                    WHERE asset = ?
                    ORDER BY timestamp DESC
                    LIMIT 200
                    """,
                    (normalized,),
                ).fetchall()
        if not rows:
            event_text = self.format_stablecoin_onchain_event_summary(normalized)
            return (
                f"资产: {normalized}\n"
                "数据源: DefiLlama 稳定币供应聚合，不代表交易所买盘或钱包净流\n"
                "稳定币供应数据暂不可用\n"
                f"{event_text}\n"
                "结论: 稳定币供应变化仅代表潜在流动性，不等同交易所买盘"
            )
        latest_supply, latest_native, latest_ts, _raw = rows[0]
        changes = stablecoin_supply_changes(rows)
        lines = [
            f"资产: {normalized}",
            "数据源: DefiLlama 稳定币供应聚合，不代表交易所买盘或钱包净流",
            f"当前供应/市值: {format_usd(parse_float(latest_supply))}",
        ]
        native_value = parse_float(latest_native)
        if native_value is not None and native_value != parse_float(latest_supply):
            lines.append(f"当前供应: {format_optional_value(native_value)}")
        lines.append(f"更新时间: {format_ts_short(parse_float(latest_ts))}")
        lines.append(
            "变化: "
            f"24h {format_percent_optional(changes.get('24h'))} / "
            f"7d {format_percent_optional(changes.get('7d'))} / "
            f"30d {format_percent_optional(changes.get('30d'))}"
        )
        lines.append(self.format_stablecoin_onchain_event_summary(normalized))
        lines.append("结论: 稳定币供应变化仅代表潜在流动性，不等同交易所买盘")
        return "\n".join(lines)

    def stablecoin_supply_rows(self, asset: str) -> list[Any]:
        normalized = str(asset or "").strip().upper()
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                return conn.execute(
                    """
                    SELECT supply_usd, supply_native, timestamp, raw_json
                    FROM stablecoin_supply
                    WHERE asset = ?
                    ORDER BY timestamp DESC
                    LIMIT 200
                    """,
                    (normalized,),
                ).fetchall()

    def latest_stablecoin_supply_rows(self, assets: tuple[str, ...] = ("USDT", "USDC")) -> dict[str, list[Any]]:
        self.collect_defillama_stablecoin_supply_if_due()
        rows_by_asset: dict[str, list[Any]] = {asset: self.stablecoin_supply_rows(asset) for asset in assets}
        if any(not rows for rows in rows_by_asset.values()):
            self.collect_defillama_stablecoin_supply_if_due(force=True)
            rows_by_asset = {asset: self.stablecoin_supply_rows(asset) for asset in assets}
        return rows_by_asset

    def stablecoin_snapshot_from_rows(self, asset: str, rows: list[Any]) -> dict[str, Any]:
        if not rows:
            return {
                "asset": asset,
                "supply": None,
                "timestamp": None,
                "changes": {"24h": None, "7d": None, "30d": None},
                "chain_top": [],
                "conclusion": "稳定币供应数据不足，等待下次采集",
            }
        latest_supply, _latest_native, latest_ts, raw_json = rows[0]
        raw = parse_json_object(raw_json)
        changes = stablecoin_supply_changes_from_raw(raw, rows)
        chain_top = stablecoin_chain_distribution(raw)
        return {
            "asset": asset,
            "supply": parse_float(latest_supply),
            "timestamp": parse_float(latest_ts),
            "changes": changes,
            "chain_top": chain_top,
            "conclusion": stablecoin_liquidity_conclusion(changes),
        }

    def format_stablecoin_liquidity_radar(self, target: str | None = None) -> str:
        normalized = str(target or "").strip().upper()
        assets = (normalized,) if normalized in {"USDT", "USDC"} else ("USDT", "USDC")
        rows_by_asset = self.latest_stablecoin_supply_rows(assets)
        lines = [
            "数据源: DefiLlama 稳定币供应聚合",
            "说明: 仅代表稳定币供应聚合变化，不代表交易所买盘或钱包净流。",
        ]
        for asset in assets:
            snapshot = self.stablecoin_snapshot_from_rows(asset, rows_by_asset.get(asset, []))
            lines.extend(self.format_stablecoin_liquidity_asset_lines(snapshot, detail=len(assets) == 1))
        return "\n".join(lines)

    def format_stablecoin_liquidity_asset_lines(self, snapshot: dict[str, Any], detail: bool = False) -> list[str]:
        asset = str(snapshot.get("asset") or "-")
        supply = parse_float(snapshot.get("supply"))
        changes = snapshot.get("changes") if isinstance(snapshot.get("changes"), dict) else {}
        chain_top = snapshot.get("chain_top") if isinstance(snapshot.get("chain_top"), list) else []
        chain_limit = 5 if detail else 3
        chains_text = stablecoin_chain_distribution_text(chain_top, chain_limit)
        lines = [
            "",
            f"{asset}",
            f"当前供应/市值: {format_usd_plain(supply)}",
            (
                "变化: "
                f"24h {format_percent_optional(changes.get('24h'))} / "
                f"7d {format_percent_optional(changes.get('7d'))} / "
                f"30d {format_percent_optional(changes.get('30d'))}"
            ),
            f"链分布 Top{chain_limit}: {chains_text}",
        ]
        if detail:
            lines.append(f"最近更新时间: {format_ts_short(parse_float(snapshot.get('timestamp')))}")
        lines.append(f"结论: {snapshot.get('conclusion') or '稳定币供应数据不足，等待下次采集'}")
        return lines

    def send_stablecoin_liquidity_summary_if_due(self) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        now = time.time()
        hour_key = int(now // 3600)
        if self.last_stablecoin_liquidity_hour_key == hour_key:
            return
        message = self.format_stablecoin_liquidity_radar()
        self.last_stablecoin_liquidity_hour_key = hour_key
        self.save_state()
        channel_key = "onchain" if self.discord_config.channel_ids.get("onchain") else "summary"
        self.enqueue_discord_message(discord_onchain_embed_v2("🟠 稳定币流动性雷达", message, channel_key))
        logging.info("Discord stablecoin liquidity radar enqueued: channel=%s hour_key=%s", channel_key, hour_key)

    def recent_coinglass_metric_values(self, symbol: str, since_seconds: int = 86400) -> dict[str, tuple[Any, float]]:
        normalized = normalize_usdt_symbol(symbol)
        cutoff = time.time() - since_seconds
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT metric, value, timestamp
                    FROM external_data_points
                    WHERE source = 'CoinGlass aggregation'
                      AND symbol = ?
                      AND timestamp >= ?
                    ORDER BY timestamp ASC
                    """,
                    (normalized, cutoff),
                ).fetchall()
        values: dict[str, tuple[Any, float]] = {}
        for metric, value, timestamp in rows:
            values[str(metric)] = (parse_float(value) if parse_float(value) is not None else value, float(timestamp or 0))
        return values

    def recent_coinglass_exchange_balances(self, symbol: str, since_seconds: int = 86400) -> dict[str, dict[str, Any]]:
        normalized = normalize_usdt_symbol(symbol)
        asset = normalized[:-4] if normalized.endswith("USDT") else normalized
        cutoff = time.time() - since_seconds
        with self.external_data_lock:
            with self.external_db_connection() as conn:
                rows = conn.execute(
                    """
                    SELECT range_label, balance, balance_usd, change_value, change_percent, timestamp
                    FROM exchange_balances
                    WHERE source = 'CoinGlass aggregation'
                      AND asset = ?
                      AND timestamp >= ?
                    ORDER BY timestamp ASC
                    """,
                    (asset, cutoff),
                ).fetchall()
        ranges: dict[str, dict[str, Any]] = {}
        for range_label, balance, balance_usd, change_value, change_percent, timestamp in rows:
            ranges[str(range_label)] = {
                "balance": parse_float(balance),
                "balance_usd": parse_float(balance_usd),
                "change": parse_float(change_value),
                "change_percent": parse_float(change_percent),
                "timestamp": parse_float(timestamp),
            }
        return ranges

    def coinglass_panel_snapshot(self, symbol: str) -> dict[str, Any]:
        normalized = normalize_usdt_symbol(symbol)
        metrics = self.recent_coinglass_metric_values(normalized)
        balances = self.recent_coinglass_exchange_balances(normalized)
        timestamps = [timestamp for _value, timestamp in metrics.values() if timestamp]
        timestamps.extend(parse_float(row.get("timestamp")) for row in balances.values() if parse_float(row.get("timestamp")))
        return {
            "symbol": normalized,
            "metrics": metrics,
            "balances": balances,
            "updated_at": max(timestamps) if timestamps else None,
            "cached_text": cached_coinglass_market_context_text(normalized),
        }

    def format_coinglass_panel(self, target: str | None = None) -> str:
        symbols = [normalize_usdt_symbol(target)] if target else ONCHAIN_SUMMARY_SYMBOLS
        lines = [
            "说明: CoinGlass 是外部聚合数据，不是钱包级链上流；仅用于外部资金确认。",
        ]
        for symbol in symbols:
            if not is_valid_binance_usdt_symbol(symbol):
                return "用法: !coinglass 或 !coinglass BTCUSDT"
            snapshot = self.coinglass_panel_snapshot(symbol)
            lines.extend(self.format_coinglass_panel_symbol_lines(snapshot, detail=bool(target)))
        return "\n".join(lines)

    def format_coinglass_panel_symbol_lines(self, snapshot: dict[str, Any], detail: bool = False) -> list[str]:
        symbol = str(snapshot.get("symbol") or "-")
        metrics = snapshot.get("metrics") if isinstance(snapshot.get("metrics"), dict) else {}
        balances = snapshot.get("balances") if isinstance(snapshot.get("balances"), dict) else {}
        balance_text = (
            f"余额 1d {format_coinglass_balance_snapshot(balances.get('24h'))} / "
            f"7d {format_coinglass_balance_snapshot(balances.get('7d'))} / "
            f"30d {format_coinglass_balance_snapshot(balances.get('30d'))}"
        )
        oi_text = (
            f"OI 1h {format_percent_optional(metric_value(metrics, 'open_interest_change_1h'))} / "
            f"4h {format_percent_optional(metric_value(metrics, 'open_interest_change_4h'))} / "
            f"24h {format_percent_optional(metric_value(metrics, 'open_interest_change_24h'))}"
        )
        funding_text = (
            f"Funding 当前 {format_percent_optional(metric_value(metrics, 'funding_oi_weight'))} / "
            f"1d {format_percent_optional(metric_value(metrics, 'funding_accumulated_24h'))} / "
            f"7d {format_percent_optional(metric_value(metrics, 'funding_accumulated_7d'))}"
        )
        taker_text = (
            f"主动买卖24h 买{format_ratio_percent(metric_value(metrics, 'taker_flow_buy_ratio'))} / "
            f"卖{format_ratio_percent(metric_value(metrics, 'taker_flow_sell_ratio'))}"
        )
        orderbook_text = (
            f"订单簿 1h 买{format_usd(metric_value(metrics, 'orderbook_bids_usd_1h'))} / "
            f"卖{format_usd(metric_value(metrics, 'orderbook_asks_usd_1h'))}；"
            f"4h 买{format_usd(metric_value(metrics, 'orderbook_bids_usd_avg_4h'))} / "
            f"卖{format_usd(metric_value(metrics, 'orderbook_asks_usd_avg_4h'))}"
        )
        judgement = coinglass_panel_judgement(metrics, balances)
        cached_text = str(snapshot.get("cached_text") or "")
        cached_line = f"缓存摘要: {compact_coinglass_market_context(cached_text)}" if cached_text and not metrics and not balances else ""
        updated = format_ts_short(parse_float(snapshot.get("updated_at")))
        if detail:
            lines = [
                "",
                symbol,
                balance_text,
                oi_text,
                funding_text,
                taker_text,
                orderbook_text,
                f"数据更新时间: {updated}",
                f"综合判断: {judgement}",
            ]
            if cached_line:
                lines.insert(-1, cached_line)
            return lines
        lines = [
            "",
            f"{symbol}: {balance_text}",
            f"{oi_text} | {funding_text}",
            f"{taker_text} | {orderbook_text}",
            f"更新时间 {updated} | 判断: {judgement}",
        ]
        if cached_line:
            lines.insert(-1, cached_line)
        return lines

    def send_coinglass_summary_if_due(self) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        now = time.time()
        hour_key = int(now // 3600)
        if self.last_coinglass_summary_hour_key == hour_key:
            return
        message = self.format_coinglass_panel()
        self.last_coinglass_summary_hour_key = hour_key
        self.save_state()
        channel_key = "onchain" if self.discord_config.channel_ids.get("onchain") else "summary"
        self.enqueue_discord_message(discord_onchain_embed_v2("🔷 CoinGlass 聚合资金摘要", message, channel_key))
        logging.info("Discord CoinGlass summary enqueued: channel=%s hour_key=%s", channel_key, hour_key)

    def stablecoin_overview_lines_and_bias(self) -> tuple[list[str], str]:
        rows_by_asset = self.latest_stablecoin_supply_rows(("USDT", "USDC"))
        lines = ["数据源: DefiLlama 稳定币供应聚合；不代表交易所买盘。"]
        conclusions: list[str] = []
        for asset in ("USDT", "USDC"):
            snapshot = self.stablecoin_snapshot_from_rows(asset, rows_by_asset.get(asset, []))
            changes = snapshot.get("changes") if isinstance(snapshot.get("changes"), dict) else {}
            conclusion = str(snapshot.get("conclusion") or "")
            conclusions.append(conclusion)
            lines.append(
                f"{asset}: {format_usd_plain(parse_float(snapshot.get('supply')))} | "
                f"24h {format_percent_optional(changes.get('24h'))} / 7d {format_percent_optional(changes.get('7d'))} | "
                f"{stablecoin_conclusion_short(conclusion)}"
            )
        if any("扩张" in item for item in conclusions):
            bias = "support"
        elif any("收缩" in item for item in conclusions):
            bias = "risk"
        elif any("数据不足" in item for item in conclusions):
            bias = "insufficient"
        else:
            bias = "neutral"
        return lines, bias

    def coinglass_overview_lines_and_bias(self) -> tuple[list[str], str]:
        lines = ["CoinGlass 是外部聚合数据，不是钱包级链上流。"]
        support = risk = conflict = insufficient = 0
        for symbol in ONCHAIN_SUMMARY_SYMBOLS:
            snapshot = self.coinglass_panel_snapshot(symbol)
            metrics = snapshot.get("metrics") if isinstance(snapshot.get("metrics"), dict) else {}
            balances = snapshot.get("balances") if isinstance(snapshot.get("balances"), dict) else {}
            balance_text = (
                f"1d {format_coinglass_balance_snapshot(balances.get('24h'))} / "
                f"7d {format_coinglass_balance_snapshot(balances.get('7d'))} / "
                f"30d {format_coinglass_balance_snapshot(balances.get('30d'))}"
            )
            taker_text = (
                f"买{format_ratio_percent(metric_value(metrics, 'taker_flow_buy_ratio'))}/"
                f"卖{format_ratio_percent(metric_value(metrics, 'taker_flow_sell_ratio'))}"
            )
            orderbook_bias = coinglass_orderbook_judgement(metric_value(metrics, "orderbook_bid_ask_ratio"))
            judgement = coinglass_panel_judgement(metrics, balances)
            short_bias = coinglass_judgement_short(judgement)
            if short_bias == "支撑":
                support += 1
            elif short_bias == "抛压" or "拥挤" in judgement:
                risk += 1
            elif short_bias == "分歧":
                conflict += 1
            else:
                insufficient += 1
            lines.append(
                f"{symbol}: 余额 {balance_text} | 主动{taker_text} | 订单簿 {orderbook_bias} | {short_bias}"
            )
        if support and not risk:
            bias = "support"
        elif risk and not support:
            bias = "risk"
        elif support or risk or conflict:
            bias = "conflict"
        elif insufficient >= len(ONCHAIN_SUMMARY_SYMBOLS):
            bias = "insufficient"
        else:
            bias = "neutral"
        return lines, bias

    def onchain_event_overview_lines_and_bias(self) -> tuple[list[str], str]:
        active_count = len(self.onchain_address_labels)
        event_count = self.onchain_transfer_event_count()
        empty_status = self.onchain_event_empty_status_text()
        lines = [
            f"active 地址标签数: {active_count}",
            f"最近24h事件数: {event_count}",
            f"状态: {empty_status if event_count == 0 else '最近24h有已标记钱包级转账事件'}",
        ]
        lines.extend(self.format_scan_adapter_status_lines())
        if event_count > 0:
            bias = "support"
        elif active_count <= 0:
            bias = "insufficient"
        elif "扫描正常" in empty_status:
            bias = "neutral"
        else:
            bias = "insufficient"
        return lines, bias

    def external_source_health_overview_lines(self) -> list[str]:
        counts = self.external_source_recent_counts()
        specs = self.data_sources
        coinglass = specs.get("CoinGlass aggregation")
        defillama = specs.get("DefiLlama stablecoin supply")
        labels_count = len(self.onchain_address_labels)
        scan_rows = self.scan_adapter_health_rows()
        scan_status = " / ".join(
            f"{name}:{'on' if row.get('enabled') else 'off'}"
            for name, row in scan_rows.items()
        )
        return [
            f"CoinGlass last_success={format_ts_short(coinglass.last_success if coinglass else None)} | 24h数据={counts.get('CoinGlass aggregation', 0)}",
            f"DefiLlama last_success={format_ts_short(defillama.last_success if defillama else None)} | 24h数据={counts.get('DefiLlama stablecoin supply', 0)}",
            f"Address labels count={labels_count}",
            f"Scan adapters configured: {scan_status}",
        ]

    def external_funds_overview_conclusion(self, stablecoin_bias: str, coinglass_bias: str, onchain_bias: str) -> str:
        biases = [stablecoin_bias, coinglass_bias, onchain_bias]
        if biases.count("support") >= 2 and "risk" not in biases:
            return "外部资金偏支撑：多项外部资金背景偏正，但不构成买卖建议。"
        if biases.count("risk") >= 2:
            return "外部资金偏风险：供应/聚合资金或链上状态偏弱，追多需谨慎。"
        if "support" in biases and "risk" in biases:
            return "外部资金分歧：部分资金背景有支撑，但风险项仍在，追多仍需谨慎。"
        if biases.count("insufficient") >= 2:
            return "外部资金数据不足：等待稳定币、CoinGlass 和地址事件缓存补齐。"
        return "外部资金中性：当前外部资金背景未形成明确方向。"

    def external_funds_overview_embed(self, channel_key: str = "onchain") -> DiscordOutboundMessage:
        stable_lines, stable_bias = self.stablecoin_overview_lines_and_bias()
        coinglass_lines, coinglass_bias = self.coinglass_overview_lines_and_bias()
        onchain_lines, onchain_bias = self.onchain_event_overview_lines_and_bias()
        health_lines = self.external_source_health_overview_lines()
        conclusion = self.external_funds_overview_conclusion(stable_bias, coinglass_bias, onchain_bias)
        fields = [
            ("稳定币流动性", discord_field_value("\n".join(stable_lines)), False),
            ("CoinGlass 主流聚合", discord_field_value("\n".join(coinglass_lines)), False),
            ("链上事件状态", discord_field_value("\n".join(onchain_lines)), False),
            ("数据源健康", discord_field_value("\n".join(health_lines)), False),
            ("总结论", discord_field_value(conclusion), False),
        ]
        return DiscordOutboundMessage(
            channel_key=channel_key,
            title="🧭 外部资金总览",
            color=DISCORD_COLOR_SUMMARY,
            fields=fields,
            kind="external_funds_overview",
        )

    def send_external_funds_overview_if_due(self) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        now = time.time()
        hour_key = int(now // 3600)
        if self.last_external_funds_overview_hour_key == hour_key:
            return
        channel_key = "onchain" if self.discord_config.channel_ids.get("onchain") else "summary"
        self.last_external_funds_overview_hour_key = hour_key
        self.save_state()
        self.enqueue_discord_message(self.external_funds_overview_embed(channel_key))
        logging.info("Discord external funds overview enqueued: channel=%s hour_key=%s", channel_key, hour_key)

    def format_stablecoin_onchain_event_summary(self, asset: str) -> str:
        events = self.recent_onchain_transfer_events(asset, limit=5)
        if not events:
            return "最近24h链上事件: 暂无钱包级转账事件，仅显示 DefiLlama 供应聚合"
        direction_counts: dict[str, int] = {}
        for event in events:
            direction = str(event.get("direction") or "unknown")
            direction_counts[direction] = direction_counts.get(direction, 0) + 1
        lines = [f"最近24h链上事件: {self.onchain_transfer_event_query_count(asset)} 条 | {format_count_mapping(direction_counts)}"]
        for event in events[:3]:
            lines.append(
                f"- {format_ts_short(parse_float(event.get('timestamp')))} "
                f"{event.get('asset') or asset} {format_optional_value(parse_float(event.get('amount')))} "
                f"{event.get('from_label')} -> {event.get('to_label')} | {event.get('direction')}"
            )
        return "\n".join(lines)

    def discord_external_funds_command_response(self, target: str) -> DiscordOutboundMessage:
        normalized = str(target or "").strip().upper()
        try:
            if normalized in {"USDT", "USDC"}:
                return discord_onchain_embed_v2(
                    f"{normalized} 外部资金确认",
                    self.format_stablecoin_external_funds(normalized),
                    "onchain",
                )
            symbol = normalize_usdt_symbol(normalized)
            if symbol in {"USDT", "USDC"} or not is_valid_binance_usdt_symbol(symbol):
                return discord_onchain_embed_v2("外部资金确认", "暂无该资产外部资金数据", "onchain")
            snapshot, data_source_text, degradation_text = self.telegram_command_snapshot(symbol)
            coinglass_text = self.format_coinglass_market_context(symbol)
            spot_text = cached_spot_alpha_confirmation(symbol) or spot_alpha_confirmation(symbol)
            self.persist_dexscreener_cached_snapshot(symbol)
            self.persist_external_confirmation_metrics(snapshot, None, spot_text, coinglass_text)
            response_parts = [data_source_text]
            if degradation_text:
                response_parts.append(degradation_text)
            response_parts.append(format_onchain_brief(snapshot, coinglass_text, spot_text))
            return discord_onchain_embed_v2(f"{symbol} 外部资金确认", "\n".join(response_parts), "onchain")
        except Exception:
            logging.exception("Discord external funds command failed: target=%s", normalized)
            return discord_onchain_embed_v2("外部资金确认", "暂无该资产外部资金数据", "onchain")


    def run_forever(self) -> None:
        self.start_liquidation_stream_worker()
        self.start_telegram_command_worker()
        self.start_discord_worker()
        self.send_pending_dev_restart_status()
        self.refresh_symbols_if_due(force=True)
        logging.info("Monitoring %s derivatives symbols", len(self.symbol_configs))
        while True:
            started = time.monotonic()
            try:
                self.refresh_symbols_if_due()
                self.run_cycle()
                self.collect_external_data_if_due()
                self.flush_pending_telegram_signals()
                self.send_summary_if_due()
                self.send_onchain_summary_if_due()
                self.send_stablecoin_liquidity_summary_if_due()
                self.send_coinglass_summary_if_due()
                self.send_external_funds_overview_if_due()
                self.flush_telegram_signal_digest_if_due()
                self.flush_discord_alt_watch_digest_if_due()
                self.flush_discord_suppressed_digest_if_due()
            except Exception:
                logging.exception("Derivatives monitor cycle failed")

            elapsed = time.monotonic() - started
            sleep_until = time.monotonic() + max(5, self.poll_interval - elapsed)
            while True:
                remaining = sleep_until - time.monotonic()
                if remaining <= 0:
                    break
                time.sleep(min(5, remaining))
                try:
                    self.flush_pending_telegram_signals()
                    self.flush_discord_alt_watch_digest_if_due()
                    self.flush_discord_suppressed_digest_if_due()
                except Exception:
                    logging.exception("Failed to flush pending Telegram signals")

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
            self.update_source_health("CoinGlass aggregation", False, "context n/a")
        else:
            self.persist_coinglass_market_context(symbol, context)
            text = format_coinglass_market_context_text(context)
        self.coinglass_market_context_cache[cache_key] = (now, text)
        _COINGLASS_TEXT_CACHE[symbol] = (now, text)
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
            logging.info(
                "Crypto monitor pool updated. Monitoring %s symbols. Added %s: %s; Removed %s: %s.",
                len(self.symbol_configs),
                len(added_symbols),
                compact_symbol_list_for_log(added_symbols),
                len(removed_symbols),
                compact_symbol_list_for_log(sorted(removed_symbols)),
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
            self.refresh_market_summary_cache_from_latest("scan")
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
        self.refresh_market_summary_cache_from_latest("scan")
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
        price_change_periods = price_change_periods_from_klines(klines, self.period)
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
            spot_price=safe_float(funding.get("indexPrice")),
            price_change_periods=price_change_periods,
        )

    def fetch_flow_metrics(self, symbol: str, price: float) -> tuple[dict[str, float], dict[str, float]]:
        if not self.flow_config.get("enabled", False):
            return {}, {}

        net_flow_usd: dict[str, float] = {}
        net_flow_ratio: dict[str, float] = {}
        configured_periods = [str(period) for period in self.flow_config.get("periods", FLOW_PERIODS)]
        periods = list(dict.fromkeys(configured_periods + FLOW_PERIODS))

        for period in periods:
            metric = self.fetch_flow_metric_cached(symbol, str(period), price)
            if metric is None:
                continue
            flow, ratio = metric
            if flow is not None:
                net_flow_usd[str(period)] = flow
            if ratio is not None:
                net_flow_ratio[str(period)] = ratio

        return net_flow_usd, net_flow_ratio

    def fetch_flow_metric_cached(self, symbol: str, period: str, price: float) -> tuple[float | None, float | None] | None:
        now = time.time()
        key = (symbol.upper(), period)
        ttl = flow_cache_ttl_seconds(period)
        with self.flow_metrics_cache_lock:
            cached = self.flow_metrics_cache.get(key)
            if cached and now - cached[0] < ttl:
                return cached[1], cached[2]

        try:
            metric = self.fetch_flow_metric_uncached(symbol, period, price)
        except Exception:
            logging.debug("Failed to fetch flow metrics for %s %s", symbol, period, exc_info=True)
            metric = None

        with self.flow_metrics_cache_lock:
            if metric is None:
                self.flow_metrics_cache[key] = (now, None, None)
            else:
                self.flow_metrics_cache[key] = (now, metric[0], metric[1])
        return metric

    def fetch_flow_metric_uncached(self, symbol: str, period: str, price: float) -> tuple[float | None, float | None] | None:
        binance_period, limit, required_rows = flow_binance_request(period)
        rows = self.get_data("takerlongshortRatio", {"symbol": symbol, "period": binance_period, "limit": limit})
        if not rows or len(rows) < required_rows:
            return None

        selected_rows = rows[-required_rows:]
        buy_vol = 0.0
        sell_vol = 0.0
        for row in selected_rows:
            buy_vol += float(row.get("buyVol", 0) or 0)
            sell_vol += float(row.get("sellVol", 0) or 0)
        if buy_vol == 0 and sell_vol == 0:
            return None
        ratio = buy_vol / sell_vol if sell_vol > 0 else None
        return (buy_vol - sell_vol) * price, ratio

    def evaluate_snapshot(self, snapshot: MarketSnapshot, symbol_config: dict[str, Any]) -> list[Signal]:
        mode = symbol_config.get("mode", "both")
        signals = []
        main_asset_radar = self.main_asset_radar_signal(snapshot)
        if main_asset_radar:
            signals.append(main_asset_radar)
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

    def main_asset_radar_signal(self, snapshot: MarketSnapshot) -> Signal | None:
        if not is_major_asset_tier(snapshot.symbol):
            return None

        leading = leading_signal_score(snapshot, None)
        ev_score, ev_direction, ev_summary, _ev_items = evidence_score(snapshot, None)
        ev_display = abs(ev_score)
        short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
        basis_pct, basis_label, _basis_reason = basis_state(snapshot)

        price_up = snapshot.price_change_percent > 0.3
        price_resilient = snapshot.price_change_percent >= -0.5
        oi_up = snapshot.oi_change_percent >= 2.0
        price_oi_sync = price_up and oi_up
        oi_up_price_resilient = snapshot.oi_change_percent >= 3.0 and price_resilient
        flow_support_count = sum(score >= 6 for score in (short_flow, mid_flow, long_flow))
        flow_resonance = flow_support_count >= 2 or (short_flow >= 7 and mid_flow >= 6)
        long_distribution = flow_label == "中长线派发"
        weak_cycle = flow_label in ("短强中弱", "中长线派发") or mid_flow <= 4 or long_flow <= 3
        high_position = snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75
        funding_hot = snapshot.funding_rate_percent is not None and snapshot.funding_rate_percent >= 0.03
        taker_weak = snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio <= 1.02
        premium = basis_label in PREMIUM_BASIS_STATES or (basis_pct is not None and basis_pct >= 0.15)
        risk_text = text_has_any(ev_summary, ("高位拥挤", "出货", "派发", "追多风险"))
        momentum_signal = main_momentum_watch_signal(snapshot)
        extreme_high_risk = (
            snapshot.price_change_percent >= 8.0
            and (snapshot.funding_rate_percent is not None and snapshot.funding_rate_percent >= 0.08)
            and (basis_label in PREMIUM_BASIS_STATES or (basis_pct is not None and basis_pct >= 0.25))
            and (high_position or snapshot.oi_change_percent >= 8.0)
        )

        crowded_risk = (
            high_position
            and snapshot.oi_change_percent >= 4.0
            and (premium or funding_hot or taker_weak or weak_cycle)
        )
        leading_risk = (
            leading.leading_score >= 5
            and leading.leading_direction == "short"
            and (ev_direction == "看空/风险" or risk_text or ev_score <= -3)
        )
        weak_cycle_risk = price_oi_sync and weak_cycle and (premium or funding_hot or taker_weak or long_distribution)
        if (crowded_risk or leading_risk or weak_cycle_risk) and not (momentum_signal and not extreme_high_risk):
            score = min(
                10,
                5
                + int(crowded_risk)
                + int(leading_risk)
                + int(weak_cycle_risk)
                + int(funding_hot)
                + int(premium)
                + int(taker_weak),
            )
            return Signal(
                symbol=snapshot.symbol,
                kind="main_risk_watch",
                score=score,
                title=f"{snapshot.symbol} 主流风险雷达",
                message=self.describe(snapshot, "主流风险雷达：高位增仓、溢价/费率或资金周期转弱，优先按风险观察。"),
                key=f"{snapshot.symbol}:main_risk_watch",
                snapshot=snapshot,
            )
        if momentum_signal:
            return momentum_signal

        trend_trigger = (
            leading.leading_score >= 5
            and leading.leading_direction == "long"
            and (ev_direction == "看多" or ev_score >= 5)
            and not long_distribution
            and flow_resonance
            and (price_oi_sync or oi_up_price_resilient)
        )
        if trend_trigger:
            score = min(
                10,
                5
                + int(flow_support_count >= 2)
                + int(short_flow >= 7 and mid_flow >= 6)
                + int(price_oi_sync)
                + int(oi_up_price_resilient)
                + int(ev_display >= 6),
            )
            return Signal(
                symbol=snapshot.symbol,
                kind="main_trend_watch",
                score=score,
                title=f"{snapshot.symbol} 主流趋势雷达",
                message=self.describe(snapshot, "主流趋势雷达：领先信号偏多，多周期资金流入，价格/OI 配合，趋势观察。"),
                key=f"{snapshot.symbol}:main_trend_watch",
                snapshot=snapshot,
            )

        return None

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
            "main_trend_watch": 1800,
            "main_risk_watch": 1800,
            "main_momentum_watch": 1200,
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
        if signal.kind in ("hot_breakout", "distribution", "bottom_reversal", "top_exhaustion", "main_trend_watch", "main_risk_watch"):
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
        entry_score, entry_label, entry_reason = entry_timing_score(snapshot, signal) if snapshot else ("", "", "")
        spot_score, spot_label, spot_reason = spot_onchain_score(snapshot, signal) if snapshot else ("", "", "")
        div_label, div_score, div_reason = contract_spot_divergence(snapshot, signal) if snapshot else ("", "", "")
        major_score, major_label, major_reason = major_flow_score(snapshot, signal) if snapshot else ("", "", "")
        basis_pct, basis_label, basis_reason = basis_state(snapshot) if snapshot else ("", "", "")
        short_flow_score, mid_flow_score, long_flow_score, flow_trend_label, flow_trend_reason = (
            flow_horizon_scores(snapshot) if snapshot else ("", "", "", "", "")
        )
        position_label, position_score, position_reason = position_behavior(snapshot, signal) if snapshot else ("", "", "")
        squeeze_label, squeeze_score, squeeze_reason = squeeze_state(snapshot) if snapshot else ("", "", "")
        absorption_label, absorption_score, absorption_reason = spot_absorption_state(snapshot, signal) if snapshot else ("", "", "")
        intent_label, intent_score, intent_reason = market_intent(snapshot, signal) if snapshot else ("", "", "")
        leading = leading_signal_score(snapshot, signal) if snapshot else LeadingSignalScore(0, "neutral", "无", [], 0, 0)
        ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal) if snapshot else ("", "", "", [])
        conv_score, conv_label, conv_reason = conviction_score(snapshot, signal) if snapshot else ("", "", "")
        priority, quality_score, quality_reason = signal_priority(signal, snapshot)
        suppressed_from_telegram, _suppressed_reason = self.telegram_signal_suppression(signal, priority, quality_score)
        if suppressed_from_telegram:
            self.enqueue_discord_alt_watch_signal(signal, priority, quality_score, quality_reason)
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
            "net_flow_48h_usd": snapshot.net_flow_usd.get("48h", "") if snapshot else "",
            "net_flow_72h_usd": snapshot.net_flow_usd.get("72h", "") if snapshot else "",
            "net_flow_96h_usd": snapshot.net_flow_usd.get("96h", "") if snapshot else "",
            "net_flow_120h_usd": snapshot.net_flow_usd.get("120h", "") if snapshot else "",
            "net_flow_144h_usd": snapshot.net_flow_usd.get("144h", "") if snapshot else "",
            "net_flow_5m_ratio": snapshot.net_flow_ratio.get("5m", "") if snapshot else "",
            "net_flow_15m_ratio": snapshot.net_flow_ratio.get("15m", "") if snapshot else "",
            "net_flow_1h_ratio": snapshot.net_flow_ratio.get("1h", "") if snapshot else "",
            "net_flow_4h_ratio": snapshot.net_flow_ratio.get("4h", "") if snapshot else "",
            "net_flow_12h_ratio": snapshot.net_flow_ratio.get("12h", "") if snapshot else "",
            "net_flow_24h_ratio": snapshot.net_flow_ratio.get("24h", "") if snapshot else "",
            "net_flow_48h_ratio": snapshot.net_flow_ratio.get("48h", "") if snapshot else "",
            "net_flow_72h_ratio": snapshot.net_flow_ratio.get("72h", "") if snapshot else "",
            "net_flow_96h_ratio": snapshot.net_flow_ratio.get("96h", "") if snapshot else "",
            "net_flow_120h_ratio": snapshot.net_flow_ratio.get("120h", "") if snapshot else "",
            "net_flow_144h_ratio": snapshot.net_flow_ratio.get("144h", "") if snapshot else "",
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
            "entry_timing_score": entry_score,
            "entry_timing_label": entry_label,
            "entry_timing_reason": entry_reason,
            "spot_onchain_score": spot_score,
            "spot_onchain_label": spot_label,
            "spot_onchain_reason": spot_reason,
            "contract_spot_divergence_label": div_label,
            "contract_spot_divergence_score": div_score,
            "contract_spot_divergence_reason": div_reason,
            "major_flow_score": major_score,
            "major_flow_label": major_label,
            "major_flow_reason": major_reason,
            "basis_pct": basis_pct,
            "basis_state": basis_label,
            "basis_reason": basis_reason,
            "short_flow_score": short_flow_score,
            "mid_flow_score": mid_flow_score,
            "long_flow_score": long_flow_score,
            "flow_trend_label": flow_trend_label,
            "flow_trend_reason": flow_trend_reason,
            "position_behavior_label": position_label,
            "position_behavior_score": position_score,
            "position_behavior_reason": position_reason,
            "squeeze_state_label": squeeze_label,
            "squeeze_state_score": squeeze_score,
            "squeeze_state_reason": squeeze_reason,
            "spot_absorption_label": absorption_label,
            "spot_absorption_score": absorption_score,
            "spot_absorption_reason": absorption_reason,
            "market_intent_label": intent_label,
            "market_intent_score": intent_score,
            "market_intent_reason": intent_reason,
            "leading_score": leading.leading_score,
            "leading_direction": leading.leading_direction,
            "leading_label": leading.leading_label,
            "leading_items": "; ".join(leading.leading_items[:20]),
            "leading_bull_score": leading.leading_bull_score,
            "leading_bear_score": leading.leading_bear_score,
            "evidence_score": ev_score,
            "evidence_direction": ev_direction,
            "evidence_summary": ev_summary,
            "evidence_items": evidence_items_compact(ev_items, 20),
            "conviction_score": conv_score,
            "conviction_label": conv_label,
            "conviction_reason": conv_reason,
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
            loaded_summary_key = payload.get("last_hourly_summary_key")
            self.last_hourly_summary_key = int(loaded_summary_key) if loaded_summary_key is not None else None
            loaded_onchain_key = payload.get("last_onchain_summary_hour_key")
            self.last_onchain_summary_hour_key = int(loaded_onchain_key) if loaded_onchain_key is not None else None
            loaded_stablecoin_key = payload.get("last_stablecoin_liquidity_hour_key")
            self.last_stablecoin_liquidity_hour_key = int(loaded_stablecoin_key) if loaded_stablecoin_key is not None else None
            loaded_coinglass_key = payload.get("last_coinglass_summary_hour_key")
            self.last_coinglass_summary_hour_key = int(loaded_coinglass_key) if loaded_coinglass_key is not None else None
            loaded_overview_key = payload.get("last_external_funds_overview_hour_key")
            self.last_external_funds_overview_hour_key = int(loaded_overview_key) if loaded_overview_key is not None else None
        except Exception:
            logging.warning("Failed to load monitor state", exc_info=True)

    def save_state(self) -> None:
        path = Path(self.state_path)
        payload = {
            "last_alerted_at": self.last_alerted_at,
            "last_hourly_summary_key": self.last_hourly_summary_key,
            "last_onchain_summary_hour_key": self.last_onchain_summary_hour_key,
            "last_stablecoin_liquidity_hour_key": self.last_stablecoin_liquidity_hour_key,
            "last_coinglass_summary_hour_key": self.last_coinglass_summary_hour_key,
            "last_external_funds_overview_hour_key": self.last_external_funds_overview_hour_key,
        }
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
        discord_available = self.discord_config.enabled and bool(self.discord_config.bot_token)
        if (bot_token and chat_ids) or discord_available:
            priority, quality_score, quality_reason = signal_priority(signal, signal.snapshot)
            suppressed, suppressed_reason = self.telegram_signal_suppression(signal, priority, quality_score)
            self.update_signal_quality_stats(signal, priority, suppressed)
            if suppressed:
                conviction, _action_text, reason_context = self.telegram_realtime_filter_inputs(signal, signal.snapshot)
                logging.info(
                    "Realtime signal suppressed: %s %s priority=%s quality=%s reason=%s",
                    signal.symbol,
                    signal.kind,
                    priority,
                    quality_score,
                    suppressed_reason or quality_reason,
                )
                self.enqueue_discord_suppressed_digest(
                    signal,
                    priority,
                    quality_score,
                    int(conviction),
                    suppressed_reason or quality_reason,
                    reason_context,
                )
                delivery, _delivery_reason = self.telegram_signal_delivery(signal)
                if (
                    delivery == "digest"
                    or (
                        str(priority).upper() == "C"
                        and quality_score >= 40
                        and is_valid_binance_usdt_symbol(signal.symbol)
                    )
                ):
                    self.enqueue_telegram_signal_digest(signal, priority, quality_score, quality_reason, force=True)
                return
            self.enqueue_pending_telegram_signal(signal, priority, quality_score, quality_reason)

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

    def pending_telegram_merge_count(self) -> int:
        with self.pending_telegram_signal_merge_lock:
            return len(self.pending_telegram_signal_merges)

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
            logging.exception("Notification request timed out")
        except Exception:
            logging.exception("Failed to send notification")

    def telegram_signal_key(self, signal: Signal) -> str:
        return f"{signal.symbol}/{signal.kind}"

    def telegram_signal_merge_key(self, signal: Signal) -> str:
        return f"{signal.symbol}/{signal_direction_label(signal.kind)}"

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

    def telegram_signal_merge_window_seconds(self) -> int:
        config = self.config.get("telegram_signal_filter", {})
        if not isinstance(config, dict):
            config = {}
        return parse_positive_int(config.get("merge_window_seconds"), DEFAULT_TELEGRAM_MERGE_WINDOW_SECONDS)

    def telegram_conviction_thresholds(self) -> tuple[int, int, int]:
        config = self.config.get("telegram_signal_filter", {})
        if not isinstance(config, dict):
            config = {}
        realtime_threshold = parse_positive_int(
            config.get("conviction_realtime_threshold"),
            DEFAULT_CONVICTION_REALTIME_THRESHOLD,
        )
        watch_threshold = parse_positive_int(
            config.get("conviction_watch_threshold"),
            DEFAULT_CONVICTION_WATCH_THRESHOLD,
        )
        risk_realtime_threshold = parse_positive_int(
            config.get("risk_realtime_threshold"),
            DEFAULT_RISK_REALTIME_THRESHOLD,
        )
        return (
            max(realtime_threshold, DEFAULT_CONVICTION_REALTIME_THRESHOLD),
            DEFAULT_CONVICTION_WATCH_THRESHOLD,
            max(risk_realtime_threshold, DEFAULT_RISK_REALTIME_THRESHOLD),
        )

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

    def telegram_signal_delivery(self, signal: Signal) -> tuple[str, str]:
        if not is_valid_binance_usdt_symbol(signal.symbol):
            return "log", f"invalid symbol {signal.symbol}"
        snapshot = signal.snapshot
        if snapshot is None:
            return "log", "no snapshot"
        conviction, _conviction_label, _conviction_reason = conviction_score(snapshot, signal)
        intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
        realtime_threshold, watch_threshold, risk_realtime_threshold = self.telegram_conviction_thresholds()
        realtime_threshold = max(realtime_threshold, DEFAULT_CONVICTION_REALTIME_THRESHOLD)
        watch_threshold = DEFAULT_CONVICTION_WATCH_THRESHOLD
        risk_realtime_threshold = max(risk_realtime_threshold, DEFAULT_RISK_REALTIME_THRESHOLD)
        risk_signal = is_risk_structure_kind(signal.kind)
        direction = signal_direction_label(signal.kind)
        action, _action_reason = action_label(snapshot, signal)
        prohibited_long_action = any(text in action for text in ("禁止", "不追", "先等稳定"))
        risk_action = any(text in action for text in ("减仓", "避险", "关注顶部"))

        if risk_signal and conviction >= risk_realtime_threshold and risk_action:
            return "realtime", f"risk {signal.kind} conviction {conviction} action {action}"
        if not risk_signal and direction == "看多" and conviction >= realtime_threshold and not prohibited_long_action:
            return "realtime", f"long conviction {conviction}>={realtime_threshold}"
        if conviction >= watch_threshold:
            return "digest", f"conviction {conviction} digest only"
        return "log", f"conviction {conviction}<{watch_threshold}"

    def telegram_realtime_filter_inputs(
        self,
        signal: Signal,
        snapshot: MarketSnapshot | None,
    ) -> tuple[int, str, dict[str, Any]]:
        if snapshot is None:
            return 0, "", {}
        conviction, _conviction_label, _conviction_reason = conviction_score(snapshot, signal)
        action, _action_reason = action_label(snapshot, signal)
        leading = leading_signal_score(snapshot, signal)
        _ev_score, _ev_direction, ev_summary, _ev_items = evidence_score(snapshot, signal)
        intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
        summary = " ".join(part for part in (ev_summary, intent_label, signal.message) if part)
        return conviction, action, {
            "leading_score": leading.leading_score,
            "evidence_summary": ev_summary,
            "summary": summary,
        }

    def should_send_realtime_telegram(
        self,
        signal: Signal,
        snapshot: MarketSnapshot | None,
        conviction: int,
        quality: int,
        priority: str,
        action_text: str,
        reason_context: dict[str, Any],
    ) -> tuple[bool, str]:
        normalized_priority = str(priority or "").strip().upper()
        if not is_valid_binance_usdt_symbol(signal.symbol):
            return False, "invalid symbol"
        if snapshot is None:
            return False, "no snapshot"

        realtime_threshold, _watch_threshold, risk_realtime_threshold = self.telegram_conviction_thresholds()
        risk_signal = is_risk_structure_kind(signal.kind)
        direction = signal_direction_label(signal.kind)
        if signal.kind == "main_momentum_watch":
            if not is_core_momentum_asset(signal.symbol):
                return False, "main momentum only core symbols"
            if normalized_priority == "D":
                return False, "weak momentum"
            if quality < 40:
                return False, "weak momentum"
            _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
            price_15m = snapshot_price_change(snapshot, "15m")
            price_1h = snapshot_price_change(snapshot, "1h")
            oi_15m = snapshot.confirm_oi_change_percent
            oi_1h = snapshot.oi_change_percent
            price_supported = (price_15m or 0) > 0 or (price_1h or 0) > 0
            oi_supported = (oi_15m or 0) > 0 or (oi_1h or 0) > 0
            if flow_label in ("中长线派发", "短弱中强"):
                return False, "weak momentum"
            if not (price_supported and oi_supported):
                return False, "weak momentum"
            if summary_flow_value(snapshot, "15m") <= 0 and summary_flow_value(snapshot, "1h") <= 0:
                ev_score, _ev_direction, _ev_summary, _ev_items = evidence_score(snapshot, signal)
                spot_score, _spot_label, _spot_reason = spot_onchain_score(snapshot, signal)
                if not (ev_score >= 10 and spot_score >= 8):
                    return False, "weak momentum"
            if conviction < 55:
                return False, "weak momentum"
            if signal.score >= 5:
                return True, f"main momentum realtime conviction={conviction} score={signal.score}"
            return False, "weak momentum"
        if normalized_priority == "D":
            return False, "priority D"
        if quality < 40:
            return False, f"quality {quality}<40"
        if normalized_priority == "C":
            return False, "priority C digest only"
        summary_text = " ".join(
            str(reason_context.get(key) or "")
            for key in ("summary", "evidence_summary")
        )
        action_summary_text = f"{action_text} {summary_text}"
        leading_score = parse_float(reason_context.get("leading_score")) or 0

        risk_keywords = ("减仓", "避险", "出货", "顶部风险", "追多风险")
        if risk_signal:
            if conviction < risk_realtime_threshold:
                return False, f"risk conviction {conviction}<{risk_realtime_threshold}"
            if quality < 55:
                return False, f"risk quality {quality}<55"
            if not any(keyword in action_summary_text for keyword in risk_keywords):
                return False, "risk text missing"
            return True, f"risk realtime conviction {conviction}>={risk_realtime_threshold}"

        if conviction < realtime_threshold:
            return False, f"conviction {conviction}<{realtime_threshold}"
        if direction != "看多":
            return False, f"direction {direction} not realtime"
        if quality < 55:
            return False, f"quality {quality}<55"
        if normalized_priority not in {"S", "A", "B"}:
            return False, f"priority {normalized_priority} digest only"
        if leading_score < 3:
            return False, f"leading_score {leading_score:g}<3"
        bad_long_keywords = ("高位拥挤", "出货", "派发", "不追")
        if any(keyword in summary_text for keyword in bad_long_keywords):
            return False, "bad long evidence"
        return True, f"long realtime conviction {conviction}>={realtime_threshold}"

    def telegram_signal_suppression(self, signal: Signal, priority: str, quality_score: int) -> tuple[bool, str]:
        conviction, action_text, reason_context = self.telegram_realtime_filter_inputs(signal, signal.snapshot)
        should_send, reason = self.should_send_realtime_telegram(
            signal,
            signal.snapshot,
            conviction,
            quality_score,
            priority,
            action_text,
            reason_context,
        )
        if not should_send:
            if signal.kind == "main_momentum_watch" and reason == "weak momentum":
                logging.info("Telegram signal suppressed: %s main_momentum_watch reason=weak momentum", signal.symbol)
            return True, reason

        key = self.telegram_signal_key(signal)
        last = self.telegram_signal_cooldowns.get(key)
        if last:
            last_pushed_at, last_quality_score = last
            age = time.time() - last_pushed_at
            if signal.kind == "main_momentum_watch" and age < 1200:
                return True, f"duplicate {key} within 20m"
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
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage" if bot_token else ""
        liquidation_text = self.format_liquidation_stats(signal.symbol)
        if priority is None or quality_score is None or quality_reason is None:
            priority, quality_score, quality_reason = signal_priority(signal, signal.snapshot)
        if signal.kind != "test":
            conviction, action_text, reason_context = self.telegram_realtime_filter_inputs(signal, signal.snapshot)
            should_send, reason = self.should_send_realtime_telegram(
                signal,
                signal.snapshot,
                conviction,
                quality_score,
                priority,
                action_text,
                reason_context,
            )
            if not should_send:
                logging.info(
                    "Telegram signal suppressed: %s %s priority=%s quality=%s reason=%s",
                    signal.symbol,
                    signal.kind,
                    priority,
                    quality_score,
                    reason,
                )
                return
        payload = {
            "chat_id": chat_id,
            "text": telegram_text(format_signal_for_telegram(signal, liquidation_text, priority, quality_score, quality_reason)),
        }
        self.post_json(url, payload)

    def enqueue_pending_telegram_signal(
        self,
        signal: Signal,
        priority: str,
        quality_score: int,
        quality_reason: str,
    ) -> None:
        now = time.time()
        key = self.telegram_signal_merge_key(signal)
        direction = signal_direction_label(signal.kind)
        with self.pending_telegram_signal_merge_lock:
            pending = self.pending_telegram_signal_merges.get(key)
            if pending is None:
                self.pending_telegram_signal_merges[key] = PendingTelegramSignalMerge(
                    created_at=now,
                    updated_at=now,
                    symbol=signal.symbol,
                    direction=direction,
                    signals=[signal],
                    priorities=[priority],
                    quality_scores=[quality_score],
                    quality_reasons=[quality_reason],
                )
                return
            pending.updated_at = now
            pending.signals.append(signal)
            pending.priorities.append(priority)
            pending.quality_scores.append(quality_score)
            pending.quality_reasons.append(quality_reason)

    def flush_pending_telegram_signals(self, now: float | None = None) -> None:
        current_time = time.time() if now is None else now
        merge_window_seconds = self.telegram_signal_merge_window_seconds()
        with self.pending_telegram_signal_merge_lock:
            due_keys = [
                key
                for key, pending in self.pending_telegram_signal_merges.items()
                if current_time - pending.created_at >= merge_window_seconds
            ]
            due_items = [self.pending_telegram_signal_merges.pop(key) for key in due_keys]

        if not due_items:
            return

        notifications = self.config.get("notifications", {})
        telegram = notifications.get("telegram", {})
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        discord_available = self.discord_config.enabled and bool(self.discord_config.bot_token)
        if (not bot_token or not chat_ids) and not discord_available:
            return

        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        chat_id_list = split_chat_ids(chat_ids) if bot_token and chat_ids else []
        sent_at = time.time()
        for pending in due_items:
            allowed_signals: list[Signal] = []
            allowed_priorities: list[str] = []
            allowed_quality_scores: list[int] = []
            allowed_quality_reasons: list[str] = []
            suppressed_rows: list[tuple[Signal, str, int, str]] = []
            for index, signal in enumerate(pending.signals):
                priority = pending.priorities[index] if index < len(pending.priorities) else "-"
                quality_score = pending.quality_scores[index] if index < len(pending.quality_scores) else 0
                quality_reason = pending.quality_reasons[index] if index < len(pending.quality_reasons) else ""
                conviction, action_text, reason_context = self.telegram_realtime_filter_inputs(signal, signal.snapshot)
                should_send, reason = self.should_send_realtime_telegram(
                    signal,
                    signal.snapshot,
                    conviction,
                    quality_score,
                    priority,
                    action_text,
                    reason_context,
                )
                if should_send:
                    allowed_signals.append(signal)
                    allowed_priorities.append(priority)
                    allowed_quality_scores.append(quality_score)
                    allowed_quality_reasons.append(quality_reason)
                else:
                    if signal.kind == "main_momentum_watch" and reason == "weak momentum":
                        logging.info(
                            "Telegram signal suppressed: %s main_momentum_watch reason=weak momentum",
                            signal.symbol,
                        )
                    conviction, _action_text, reason_context = self.telegram_realtime_filter_inputs(signal, signal.snapshot)
                    self.enqueue_discord_suppressed_digest(
                        signal,
                        priority,
                        quality_score,
                        int(conviction),
                        reason,
                        reason_context,
                    )
                    self.enqueue_discord_alt_watch_signal(signal, priority, quality_score, quality_reason)
                    suppressed_rows.append((signal, priority, quality_score, reason))

            for signal, priority, quality_score, reason in suppressed_rows:
                logging.info(
                    "Telegram merge suppressed: %s %s priority=%s quality=%s reason=%s",
                    signal.symbol,
                    signal.kind,
                    priority,
                    quality_score,
                    reason,
                )
            if not allowed_signals:
                continue

            pending_to_send = PendingTelegramSignalMerge(
                created_at=pending.created_at,
                updated_at=pending.updated_at,
                symbol=pending.symbol,
                direction=pending.direction,
                signals=allowed_signals,
                priorities=allowed_priorities,
                quality_scores=allowed_quality_scores,
                quality_reasons=allowed_quality_reasons,
            )
            liquidation_text = self.format_liquidation_stats(pending.symbol)
            coinglass_text = self.format_coinglass_market_context(pending.symbol)
            message = telegram_text(format_merged_signal_for_telegram(pending_to_send, liquidation_text, coinglass_text))
            for chat_id in chat_id_list:
                self.post_json(url, {"chat_id": chat_id, "text": message})
            if discord_available:
                for index, signal in enumerate(pending_to_send.signals):
                    priority = pending_to_send.priorities[index] if index < len(pending_to_send.priorities) else "-"
                    quality_score = pending_to_send.quality_scores[index] if index < len(pending_to_send.quality_scores) else 0
                    quality_reason = pending_to_send.quality_reasons[index] if index < len(pending_to_send.quality_reasons) else ""
                    self.enqueue_discord_signal(
                        signal,
                        priority,
                        quality_score,
                        quality_reason,
                    )

            best_quality = max(pending_to_send.quality_scores) if pending_to_send.quality_scores else 0
            best_priority = best_priority_label(pending_to_send.priorities)
            for signal, quality_score in zip(pending_to_send.signals, pending_to_send.quality_scores):
                self.telegram_signal_cooldowns[self.telegram_signal_key(signal)] = (sent_at, quality_score)
            logging.info(
                "Telegram merge send: %s %s count=%s priority=%s quality=%s",
                pending_to_send.symbol,
                pending_to_send.direction,
                len(pending_to_send.signals),
                best_priority,
                best_quality,
            )

    def signal_digest_item(
        self,
        signal: Signal,
        priority: str,
        quality_score: int,
        quality_reason: str,
    ) -> TelegramSignalDigestItem:
        snapshot = signal.snapshot
        trap_score: int | str = ""
        main_score_value: int | None = None
        if snapshot:
            trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)
            main_score = main_asset_score(snapshot)
            main_score_value = main_score.total_score if main_score else None

        return TelegramSignalDigestItem(
            created_at=time.time(),
            symbol=signal.symbol,
            kind=signal.kind,
            priority=priority,
            quality_score=quality_score,
            trap_score=trap_score,
            main_asset_score=main_score_value,
            signal_score=signal.score,
            strength_score=signal_strength_score(signal),
            price_change_percent=snapshot.price_change_percent if snapshot else None,
            oi_change_percent=snapshot.oi_change_percent if snapshot else None,
            reason=format_quality_reason_short(quality_reason or signal.message),
        )

    def enqueue_telegram_signal_digest(
        self,
        signal: Signal,
        priority: str,
        quality_score: int,
        quality_reason: str = "",
        force: bool = False,
    ) -> None:
        _realtime_priorities, digest_priorities, _digest_interval_minutes, _digest_max_per_priority = (
            self.telegram_signal_filter_settings()
        )
        if not force and priority not in set(digest_priorities):
            return

        item = self.signal_digest_item(signal, priority, quality_score, quality_reason)
        with self.telegram_signal_digest_lock:
            self.telegram_signal_digest_queue.append(item)

    def enqueue_discord_suppressed_digest(
        self,
        signal: Signal,
        priority: str,
        quality_score: int,
        conviction: int,
        reason: str,
        reason_context: dict[str, Any] | None = None,
    ) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        snapshot = signal.snapshot
        symbol = str(signal.symbol or "").strip().upper()
        if not snapshot or not is_valid_binance_usdt_symbol(symbol):
            return
        _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
        evidence_summary = ""
        if reason_context:
            evidence_summary = str(reason_context.get("evidence_summary") or reason_context.get("summary") or "")
        if not evidence_summary:
            _ev_score, _ev_direction, evidence_summary, _ev_items = evidence_score(snapshot, signal)
        item = DiscordSuppressedDigestItem(
            timestamp=time.time(),
            symbol=symbol,
            kind=signal.kind,
            priority=str(priority or "-").upper(),
            quality=int(quality_score or 0),
            conviction=int(conviction or 0),
            reason=str(reason or "-"),
            price_change=snapshot.price_change_percent,
            oi_change=snapshot.oi_change_percent,
            flow_label=flow_label,
            evidence_summary=truncate_text(evidence_summary, 120),
        )
        with self.discord_suppressed_digest_lock:
            self.discord_suppressed_digest_queue.append(item)
            self.discord_suppressed_digest_recent.append(item.timestamp)
        logging.info(
            "Discord suppressed digest queued: %s kind=%s reason=%s",
            item.symbol,
            item.kind,
            item.reason,
        )

    def enqueue_discord_alt_watch_signal(
        self,
        signal: Signal,
        priority: str,
        quality_score: int,
        quality_reason: str = "",
    ) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        snapshot = signal.snapshot
        symbol = str(signal.symbol or "").strip().upper()
        if not snapshot or not is_valid_binance_usdt_symbol(symbol):
            return
        if symbol in MAINSTREAM_WATCH_SYMBOLS:
            return

        conviction, _conv_label, _conv_reason = conviction_score(snapshot, signal)
        leading = leading_signal_score(snapshot, signal)
        ev_score, _ev_direction, ev_summary, _ev_items = evidence_score(snapshot, signal)
        trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)
        if quality_score < 25 and conviction < 45:
            return
        if trap_score >= 8:
            return
        if not (
            conviction >= 50
            or quality_score >= 45
            or ev_score >= 6
            or leading.leading_score >= 5
        ):
            return

        now = time.time()
        with self.discord_alt_watch_lock:
            last_sent_at = self.discord_alt_watch_symbol_sent_at.get(symbol, 0)
            if now - last_sent_at < 1800:
                return
            if any(item.symbol == symbol for item in self.discord_alt_watch_queue):
                return
            _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
            sort_score = int(conviction + quality_score + ev_score * 3 + leading.leading_score * 3)
            item = DiscordAltWatchItem(
                created_at=now,
                symbol=symbol,
                kind=signal.kind,
                conviction_score=int(conviction),
                quality_score=int(quality_score),
                leading_score=int(leading.leading_score),
                evidence_score=int(ev_score),
                trap_score=int(trap_score),
                price_change_percent=snapshot.price_change_percent,
                oi_change_percent=snapshot.oi_change_percent,
                flow_label=flow_label,
                reason=format_alt_watch_reason(ev_summary or quality_reason or signal.message),
                sort_score=sort_score,
            )
            self.discord_alt_watch_queue.append(item)
        logging.info("Discord alt watch queued: %s %s score=%s", symbol, signal.kind, sort_score)

    def discord_alt_watch_channel_key(self) -> str:
        return "alt_watch" if self.discord_config.channel_ids.get("alt_watch") else "digest"

    def format_discord_alt_watch_digest(self, items: list[DiscordAltWatchItem]) -> str:
        lines: list[str] = []
        for item in items:
            price_action = cached_multi_timeframe_price_action(item.symbol)
            lines.append(
                (
                    f"{item.symbol} {signal_kind_label(item.kind)} | 把握{item.conviction_score} 质量{item.quality_score} | "
                    f"领先{item.leading_score} 证据{item.evidence_score} 风险{item.trap_score}"
                )
            )
            lines.append(
                (
                    f"价格 {format_percent_optional(item.price_change_percent)} | "
                    f"OI {format_percent_optional(item.oi_change_percent)} | {item.flow_label} | "
                    f"{alt_watch_price_action_line(price_action)} | 结论：{item.reason}"
                )
            )
        return "\n".join(lines) or "暂无山寨观察候选。"

    def current_discord_alt_watch_top(self, limit: int = 10) -> list[DiscordAltWatchItem]:
        with self.discord_alt_watch_lock:
            return sorted(
                self.discord_alt_watch_queue,
                key=lambda item: (item.sort_score, item.created_at),
                reverse=True,
            )[:limit]

    def discord_alt_watch_items_from_csv(self, lookback_minutes: int = 30) -> list[DiscordAltWatchItem]:
        path = Path(self.signal_log_path)
        if not path.exists():
            return []
        since = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=lookback_minutes)
        best_by_symbol: dict[str, DiscordAltWatchItem] = {}
        try:
            with path.open(newline="", encoding="utf-8") as file:
                rows = csv.DictReader(file)
                for row in rows:
                    timestamp = str(row.get("timestamp") or row.get("time") or "")
                    try:
                        row_time = dt.datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
                        if row_time.tzinfo is None:
                            row_time = row_time.replace(tzinfo=dt.UTC)
                    except Exception:
                        continue
                    if row_time < since:
                        continue
                    item = discord_alt_watch_item_from_row(row, row_time)
                    if not item:
                        continue
                    current = best_by_symbol.get(item.symbol)
                    if current is None or item.sort_score > current.sort_score:
                        best_by_symbol[item.symbol] = item
        except Exception:
            logging.exception("Failed to read Discord alt watch rows from %s", path)
        return sorted(
            best_by_symbol.values(),
            key=lambda item: (item.sort_score, item.created_at),
            reverse=True,
        )

    def merged_discord_alt_watch_top(self, limit: int = 10) -> list[DiscordAltWatchItem]:
        best_by_symbol: dict[str, DiscordAltWatchItem] = {}
        for item in self.current_discord_alt_watch_top(100) + self.discord_alt_watch_items_from_csv(30):
            current = best_by_symbol.get(item.symbol)
            if current is None or item.sort_score > current.sort_score:
                best_by_symbol[item.symbol] = item
        return sorted(
            best_by_symbol.values(),
            key=lambda item: (item.sort_score, item.created_at),
            reverse=True,
        )[:limit]

    def format_discord_alt_watch_command_response(self) -> str:
        items = self.merged_discord_alt_watch_top(10)
        if not items:
            return "当前暂无山寨观察候选。"
        return "🟡 山寨观察（最近30分钟，非开仓信号）\n" + self.format_discord_alt_watch_digest(items)

    def discord_alt_watch_command_response(self) -> str | DiscordOutboundMessage:
        items = self.merged_discord_alt_watch_top(10)
        if not items:
            return "当前暂无山寨观察候选。"
        return discord_alt_watch_embed_v2(items, self.discord_alt_watch_channel_key(), "山寨观察", self.latest_snapshots)

    def flush_discord_alt_watch_digest_if_due(self, now: float | None = None) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        current_time = time.time() if now is None else now
        if current_time - self.last_discord_alt_watch_digest_at < 600:
            return
        self.last_discord_alt_watch_digest_at = current_time
        with self.discord_alt_watch_lock:
            if not self.discord_alt_watch_queue:
                return
            sorted_items = sorted(
                self.discord_alt_watch_queue,
                key=lambda item: (item.sort_score, item.created_at),
                reverse=True,
            )
            selected = sorted_items[:8]
            selected_symbols = {item.symbol for item in selected}
            self.discord_alt_watch_queue = [
                item
                for item in sorted_items[8:]
                if current_time - item.created_at < 3600 and item.symbol not in selected_symbols
            ]
            for item in selected:
                self.discord_alt_watch_symbol_sent_at[item.symbol] = current_time
        self.enqueue_discord_message(
            discord_alt_watch_embed_v2(selected, self.discord_alt_watch_channel_key(), "山寨观察摘要", self.latest_snapshots)
        )
        logging.info("Discord alt watch digest sent: count=%s", len(selected))

    def flush_discord_suppressed_digest_if_due(self, now: float | None = None, force: bool = False) -> tuple[bool, str, int]:
        current_time = time.time() if now is None else now
        self.last_discord_suppressed_digest_flush_attempt_at = current_time
        next_allowed_at = self.last_discord_suppressed_digest_sent_at + DISCORD_SUPPRESSED_DIGEST_INTERVAL_SECONDS
        if not force and self.last_discord_suppressed_digest_sent_at and current_time < next_allowed_at:
            remaining = int(max(0, next_allowed_at - current_time))
            self.last_discord_suppressed_digest_flush_status = f"waiting interval {remaining}s"
            return False, self.last_discord_suppressed_digest_flush_status, 0
        self.last_discord_suppressed_digest_flush_at = current_time
        with self.discord_suppressed_digest_lock:
            items = self.discord_suppressed_digest_queue
            self.discord_suppressed_digest_queue = []
        if not items:
            self.last_discord_suppressed_digest_flush_status = "empty"
            logging.info("Discord suppressed digest flush skipped: empty")
            return False, "静默摘要队列为空", 0
        channel_key = "digest" if self.discord_config.channel_ids.get("digest") else "main"
        try:
            enqueued = self.enqueue_discord_message(discord_suppressed_digest_embed_v2(items, channel_key))
            if not enqueued:
                with self.discord_suppressed_digest_lock:
                    self.discord_suppressed_digest_queue = items + self.discord_suppressed_digest_queue
                self.last_discord_suppressed_digest_flush_status = "enqueue failed"
                logging.error("Discord suppressed digest send failed: count=%s channel=%s reason=enqueue_failed", len(items), channel_key)
                return False, "静默摘要发送失败：Discord 队列已满或未配置", len(items)
            self.last_discord_suppressed_digest_sent_at = current_time
            self.last_discord_suppressed_digest_flush_status = f"enqueued count={len(items)} channel={channel_key}"
            logging.info("Discord suppressed digest enqueued: count=%s channel=%s", len(items), channel_key)
            logging.info("Discord suppressed digest sent: count=%s channel=%s", len(items), channel_key)
            return True, f"已发送静默摘要 {len(items)} 条", len(items)
        except Exception:
            with self.discord_suppressed_digest_lock:
                self.discord_suppressed_digest_queue = items + self.discord_suppressed_digest_queue
            self.last_discord_suppressed_digest_flush_status = "exception"
            logging.exception("Discord suppressed digest send failed: count=%s channel=%s", len(items), channel_key)
            return False, "静默摘要发送失败，请查看服务日志", len(items)

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
        digest_items = list(items)
        if not digest_items:
            return

        notifications = self.config.get("notifications", {})
        telegram = notifications.get("telegram", {})
        bot_token, chat_ids = resolve_telegram_credentials(telegram)
        discord_available = self.discord_config.enabled and bool(self.discord_config.bot_token)
        if (not bot_token or not chat_ids) and not discord_available:
            return

        if bot_token and chat_ids and digest_items:
            telegram_digest_priorities = extend_digest_priorities(digest_priorities, digest_items)
            message = format_telegram_signal_digest(
                digest_items,
                telegram_digest_priorities,
                digest_interval_minutes,
                digest_max_per_priority,
            )
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            for chat_id in split_chat_ids(chat_ids):
                try:
                    response = self.session.post(url, json={"chat_id": chat_id, "text": telegram_text(message)}, timeout=5)
                    response.raise_for_status()
                except Exception:
                    logging.exception("Failed to send Telegram signal digest to chat_id=%s", chat_id)
        if discord_available and digest_items:
            discord_digest_priorities = extend_digest_priorities(digest_priorities, digest_items)
            message = format_telegram_signal_digest(
                digest_items,
                discord_digest_priorities,
                digest_interval_minutes,
                digest_max_per_priority,
            )
            self.enqueue_discord_message(
                DiscordOutboundMessage(
                    channel_key="digest",
                    content=message,
                    title="静默信号摘要",
                    color=DISCORD_COLOR_SUMMARY,
                )
            )

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
                text = message if message.startswith(title) else f"{title}\n{message}"
                self.post_json(url, {"chat_id": chat_id, "text": telegram_text(text)})
        self.enqueue_discord_status(title, message)

    def send_summary_if_due(self) -> None:
        if not self.summary_config.get("enabled", False):
            return
        interval = int(self.summary_config.get("interval_seconds", 3600))
        now = time.time()
        hourly_summary_key = int(now // 3600)
        if self.last_hourly_summary_key == hourly_summary_key:
            return
        if now - self.last_summary_at < interval and self.last_hourly_summary_key is not None:
            return
        if not self.latest_snapshots:
            return

        message = self.refresh_market_summary_cache_from_latest("hourly") or format_summary_for_telegram(
            list(self.latest_snapshots.values()),
            int(self.summary_config.get("top_n", 5)),
        )
        self.last_summary_at = now
        self.last_hourly_summary_key = hourly_summary_key
        self.save_state()
        try:
            self.notify_status("📊 每小时市场简报", message)
        except Exception:
            logging.warning("Failed to send hourly summary for hour_key=%s; will not retry this hour", hourly_summary_key, exc_info=True)

    def send_onchain_summary_if_due(self) -> None:
        if not self.discord_config.enabled or not self.discord_config.bot_token:
            return
        if not self.discord_config.channel_ids.get("onchain"):
            return
        now = time.time()
        hour_key = int(now // 3600)
        if self.last_onchain_summary_hour_key == hour_key:
            return
        if not self.latest_snapshots:
            return

        message = self.format_onchain_summary_from_cache()
        self.last_onchain_summary_hour_key = hour_key
        self.save_state()
        self.enqueue_discord_message(
            discord_onchain_embed_v2("外部资金确认摘要", message, "onchain")
        )

    def format_onchain_summary_from_cache(self) -> str:
        lines: list[str] = []
        for symbol in ONCHAIN_SUMMARY_SYMBOLS:
            snapshot = self.latest_snapshots.get(symbol)
            if snapshot is None:
                continue
            coinglass_text = cached_coinglass_market_context_text(symbol)
            spot_text = cached_spot_alpha_confirmation(symbol)
            line = format_onchain_brief(snapshot, coinglass_text, spot_text, compact=True)
            if onchain_brief_has_confirmation_data(line):
                lines.append(line)
        if not lines:
            return "数据不足，等待外部资金缓存"
        return "外部资金确认摘要\n" + "\n".join(lines)

    def refresh_market_summary_cache_from_latest(self, source: str = "scan") -> str:
        if not self.latest_snapshots:
            return ""
        try:
            message = format_summary_for_telegram(
                list(self.latest_snapshots.values()),
                int(self.summary_config.get("top_n", 5)),
            )
        except Exception:
            logging.warning("Failed to refresh market summary cache from %s", source, exc_info=True)
            return ""
        self.update_market_summary_cache(message, source)
        return message

    def update_market_summary_cache(self, text: str, source: str = "") -> None:
        if not text:
            return
        with self.market_summary_cache_lock:
            self.last_market_summary_text = text
            self.last_market_summary_ts = time.time()
            self.last_market_summary_source = source

    def cached_market_summary(self) -> tuple[str, float, str]:
        with self.market_summary_cache_lock:
            return (
                self.last_market_summary_text,
                self.last_market_summary_ts,
                self.last_market_summary_source,
            )

    def format_cached_market_summary_response(self, text: str, cached_at: float, stale: bool = False) -> str:
        age_minutes = max(0, int((time.time() - cached_at) // 60))
        if stale:
            prefix = f"数据来源: 缓存 {age_minutes}分钟前，正在等待下轮刷新"
        else:
            prefix = f"数据来源: 缓存 {age_minutes}分钟前"
        return f"{prefix}\n{text}"

    def start_discord_worker(self) -> None:
        if self.discord_worker_started:
            return
        config = self.discord_config
        if not config.enabled:
            return
        if not config.bot_token:
            logging.warning("Discord enabled but DISCORD_BOT_TOKEN is not configured; Discord bot skipped")
            return
        self.discord_worker_started = True
        thread = threading.Thread(target=self.discord_worker_loop, args=(config,), name="discord-bot", daemon=True)
        thread.start()
        logging.info("Discord bot worker started")

    def discord_worker_loop(self, config: DiscordConfig) -> None:
        try:
            import discord
        except ImportError:
            logging.exception("discord.py is not installed; install it with: pip install discord.py")
            return

        intents = discord.Intents.default()
        intents.message_content = True
        client = discord.Client(intents=intents)

        async def resolve_channel(channel_key: str) -> Any:
            channel_id = config.channel_ids.get(channel_key)
            if not channel_id:
                logging.warning("Discord channel missing: %s", channel_key)
                return None
            try:
                numeric_channel_id = int(channel_id)
            except ValueError:
                logging.warning("Discord channel %s has invalid id=%s; skipping message", channel_key, channel_id)
                return None
            channel = client.get_channel(numeric_channel_id)
            if channel is None:
                channel = await client.fetch_channel(numeric_channel_id)
            return channel

        def build_embed(item: DiscordOutboundMessage) -> Any:
            description = discord_external_data_text(discord_embed_description_text(item))
            embed = discord.Embed(
                title=discord_external_data_text(item.title or "Crypto Monitor"),
                description=truncate_text(description, 900) if description else None,
                color=item.color if item.color is not None else DISCORD_COLOR_WATCH,
            )
            for name, value, inline in item.fields or []:
                embed.add_field(
                    name=discord_external_data_text(name),
                    value=truncate_text(discord_external_data_text(str(value)), 900) or "-",
                    inline=inline,
                )
            return embed

        async def send_discord_payload(channel: Any, payload: str | DiscordOutboundMessage) -> None:
            if isinstance(payload, DiscordOutboundMessage):
                await channel.send(embed=build_embed(payload))
                logging.info("Discord sent: channel=command title=%s", payload.title or payload.kind or payload.symbol or "-")
                return
            await channel.send(truncate_text(discord_external_data_text(str(payload or "")), 1900))

        async def send_outbound(item: DiscordOutboundMessage) -> None:
            try:
                channel = await resolve_channel(item.channel_key)
                if channel is None:
                    return
                if item.title or item.fields:
                    await channel.send(embed=build_embed(item))
                    logging.info(
                        "Discord sent: channel=%s title=%s",
                        item.channel_key,
                        item.title or item.kind or item.symbol or "-",
                    )
                    return
                await channel.send(truncate_text(discord_external_data_text(item.content or ""), 1900))
                logging.info("Discord sent: channel=%s title=%s", item.channel_key, item.title or item.kind or item.symbol or "-")
            except Exception:
                logging.exception(
                    "Failed to send Discord message: %s %s channel=%s",
                    item.symbol or "-",
                    item.kind or "-",
                    item.channel_key,
                )

        async def send_discord_test_pushes() -> str:
            test_targets = [
                ("main", "主流雷达频道", "测试 主流雷达"),
                ("alerts", "高把握信号频道", "测试 高把握信号"),
                ("risk", "风险预警频道", "测试 风险预警"),
                ("summary", "市场摘要频道", "测试 市场摘要"),
                ("digest", "静默摘要频道", "测试 静默摘要"),
                ("debug", "机器人调试频道", "测试 调试"),
                ("onchain", "外部资金频道", "测试 外部资金确认"),
            ]
            failures: list[str] = []
            for channel_key, label, test_text in test_targets:
                channel_id = config.channel_ids.get(channel_key)
                if not channel_id:
                    failures.append(f"{label}: 缺少频道ID")
                    continue
                try:
                    channel = await resolve_channel(channel_key)
                    if channel is None:
                        failures.append(f"{label}: 频道未配置或无法解析")
                        continue
                    await channel.send(test_text)
                    logging.info("Discord sent: channel=%s title=测试推送 embed=0", channel_key)
                except Exception as exc:
                    logging.exception("Failed to send Discord test message to channel=%s", channel_key)
                    failures.append(f"{label}: 发送失败 {type(exc).__name__}: {exc}")

            if failures:
                return "测试推送完成，但以下频道失败：\n" + "\n".join(f"- {item}" for item in failures)
            return "测试推送完成。"

        async def outbound_loop() -> None:
            await client.wait_until_ready()
            while not client.is_closed():
                item = await asyncio.to_thread(self.discord_outbound_queue.get)
                await send_outbound(item)

        @client.event
        async def on_ready() -> None:
            logging.info("Discord bot logged in as %s", client.user)
            await send_outbound(
                DiscordOutboundMessage(
                    channel_key="debug",
                    content="Discord 机器人已启动。",
                    title="启动通知",
                    color=DISCORD_COLOR_SUMMARY,
                )
            )

        @client.event
        async def on_message(message: Any) -> None:
            if getattr(message.author, "bot", False):
                return
            text = str(getattr(message, "content", "") or "").strip()
            if not text.startswith("!"):
                return
            try:
                if text == "!测试推送":
                    response = await send_discord_test_pushes()
                    await message.channel.send(truncate_text(response, 1900))
                    return
                response = await asyncio.to_thread(self.discord_command_response, text)
                if response:
                    await send_discord_payload(message.channel, response)
            except Exception:
                logging.exception("Failed to handle Discord command")
                await message.channel.send("命令处理失败，请查看服务日志。")

        async def runner() -> None:
            asyncio.create_task(outbound_loop())
            await client.start(config.bot_token)

        try:
            asyncio.run(runner())
        except Exception:
            logging.exception("Discord bot worker stopped unexpectedly")

    def enqueue_discord_message(self, item: DiscordOutboundMessage) -> bool:
        if not self.discord_config.enabled:
            return False
        if not self.discord_config.bot_token:
            return False
        try:
            self.discord_outbound_queue.put_nowait(item)
            logging.info(
                "Discord enqueue: channel=%s title=%s",
                item.channel_key,
                item.title or item.kind or item.symbol or "-",
            )
            return True
        except queue.Full:
            logging.warning("Discord outbound queue is full; dropping message for channel=%s", item.channel_key)
            return False

    def enqueue_discord_signal(
        self,
        signal: Signal,
        priority: str,
        quality_score: int,
        quality_reason: str,
        content: str | None = None,
        channel_key: str | None = None,
    ) -> None:
        channel = channel_key or discord_channel_for_signal(signal, priority)
        self.enqueue_discord_message(
            discord_signal_embed_v2(signal, priority, quality_score, quality_reason, channel)
        )

    def enqueue_discord_status(self, title: str, message: str) -> None:
        channel_key = "summary" if "摘要" in title or "简报" in title else "debug"
        self.enqueue_discord_message(discord_summary_embed_v2(title, message, channel_key))

    def discord_command_response(self, text: str) -> str | DiscordOutboundMessage:
        parts = text.split()
        command = parts[0].lower()
        if command == "!帮助":
            return discord_help_text()
        if command == "!摘要":
            cached_text, cached_at, _cache_source = self.cached_market_summary()
            if cached_text:
                summary_text = discord_clean_summary_text(
                    self.format_cached_market_summary_response(cached_text, cached_at),
                    title="📊 每小时市场简报",
                )
                return discord_summary_embed_v2("市场摘要", summary_text, "summary")
            return "当前暂无市场摘要缓存，请等待下一轮扫描完成后重试。"
        if command == "!候选":
            return discord_topq_embed_v2(self.format_topq_response(discord_view=True), "digest")
        if command == "!山寨":
            return self.discord_alt_watch_command_response()
        if command == "!数据源":
            return discord_summary_embed_v2("数据源健康检查", self.format_external_source_health(), "debug")
        if command == "!采集统计":
            return discord_summary_embed_v2("外部数据采集统计", self.format_external_collection_stats(), "debug")
        if command == "!外部资金来源":
            return discord_onchain_embed_v2("外部资金来源", self.format_external_source_health(only_available=False), "onchain")
        if command in ("!外部资金总览", "!资金面", "!资金驾驶舱"):
            return self.external_funds_overview_embed("onchain")
        if command == "!地址源":
            return discord_onchain_embed_v2("地址标签源", self.format_onchain_address_sources(), "onchain")
        if command == "!地址候选":
            return discord_onchain_embed_v2("地址候选", self.format_onchain_address_candidates(), "onchain")
        if command == "!地址":
            if len(parts) < 2:
                return "用法: !地址 USDT 或 !地址 Binance"
            return discord_onchain_embed_v2(f"地址标签 {parts[1]}", self.format_onchain_address_query(parts[1]), "onchain")
        if command == "!链上事件":
            query = parts[1] if len(parts) >= 2 else None
            title = f"链上事件 {query}" if query else "链上事件"
            return discord_onchain_embed_v2(title, self.format_onchain_transfer_events(query), "onchain")
        if command == "!稳定币":
            target = parts[1] if len(parts) >= 2 else None
            if target and str(target).strip().upper() not in {"USDT", "USDC"}:
                return discord_onchain_embed_v2("稳定币流动性雷达", "用法: !稳定币 或 !稳定币 USDT / !稳定币 USDC", "onchain")
            title = f"🟠 稳定币流动性雷达 {str(target).upper()}" if target else "🟠 稳定币流动性雷达"
            return discord_onchain_embed_v2(title, self.format_stablecoin_liquidity_radar(target), "onchain")
        if command == "!coinglass":
            target = parts[1] if len(parts) >= 2 else None
            title = f"🔷 CoinGlass 聚合资金 {normalize_usdt_symbol(target)}" if target else "🔷 CoinGlass 聚合资金摘要"
            return discord_onchain_embed_v2(title, self.format_coinglass_panel(target), "onchain")
        if command in ("!链上摘要", "!外部资金摘要"):
            return discord_onchain_embed_v2("外部资金确认摘要", self.format_onchain_summary_from_cache(), "onchain")
        if command in ("!链上", "!链上资金", "!外部资金"):
            if len(parts) < 2:
                return discord_onchain_embed_v2("外部资金确认摘要", self.format_onchain_summary_from_cache(), "onchain")
            return self.discord_external_funds_command_response(parts[1])
        if command == "!质量":
            return discord_summary_embed_v2("信号质量统计", self.format_signal_quality_stats(), "debug")
        if command == "!静默":
            return discord_summary_embed_v2("静默状态", self.format_quiet_status(), "debug")
        if command == "!静默发送":
            success, message, _count = self.flush_discord_suppressed_digest_if_due(force=True)
            title = "静默摘要发送" if success else "静默摘要状态"
            return discord_summary_embed_v2(title, message, "debug")
        if command == "!诊断":
            if len(parts) < 2:
                return "用法: !诊断 BTCUSDT"
            symbol = normalize_usdt_symbol(parts[1])
            snapshot, data_source_text, degradation_text = self.telegram_command_snapshot(symbol)
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
            return discord_diagnosis_embed_v2(symbol, snapshot, signals, liquidation_text, coinglass_text, response_parts)
        return ""

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

        if command == "/flow":
            self.handle_flow_command(bot_token, chat_id, parts[1:])
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

        digest_items = list(items)
        digest_priorities = extend_digest_priorities(digest_priorities, digest_items)
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
        realtime_threshold, watch_threshold, risk_realtime_threshold = self.telegram_conviction_thresholds()
        with self.signal_quality_stats_lock:
            stats = {
                "total": self.signal_quality_stats["total"],
                "realtime_sent": self.signal_quality_stats["realtime_sent"],
                "suppressed": self.signal_quality_stats["suppressed"],
                "by_priority": dict(self.signal_quality_stats["by_priority"]),
                "by_kind": dict(self.signal_quality_stats["by_kind"]),
                "by_symbol": dict(self.signal_quality_stats["by_symbol"]),
                "suppressed_by_priority": dict(self.signal_quality_stats["suppressed_by_priority"]),
                "pending_merge": self.pending_telegram_merge_count(),
            }

        priorities = ("S", "A", "B", "C", "D")
        priority_text = " ".join(f"{priority}:{stats['by_priority'].get(priority, 0)}" for priority in priorities)
        suppressed_text = " ".join(
            f"{priority}:{stats['suppressed_by_priority'].get(priority, 0)}" for priority in priorities
        )
        top_kind_counts: dict[str, int] = {}
        for kind, count in stats["by_kind"].items():
            label = signal_kind_label(kind)
            top_kind_counts[label] = top_kind_counts.get(label, 0) + count
        top_kinds = format_top_quality_counts(top_kind_counts)
        top_symbols = format_top_quality_counts(stats["by_symbol"])
        return "\n".join(
            [
                "信号质量统计（本次服务启动后）",
                f"总信号: {stats['total']}",
                f"实时推送: {stats['realtime_sent']}",
                f"静默: {stats['suppressed']}",
                f"pending_merge: {stats['pending_merge']}",
                f"实时把握阈值: {realtime_threshold}",
                f"摘要把握阈值: {watch_threshold}",
                f"风控实时阈值: {risk_realtime_threshold}",
                f"等级分布: {priority_text}",
                f"静默分布: {suppressed_text}",
                f"信号类型: {top_kinds}",
                f"币种排行: {top_symbols}",
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
        realtime_threshold, watch_threshold, risk_realtime_threshold = self.telegram_conviction_thresholds()
        with self.discord_suppressed_digest_lock:
            suppressed_pending = len(self.discord_suppressed_digest_queue)
            cutoff = time.time() - DISCORD_SUPPRESSED_DIGEST_INTERVAL_SECONDS
            recent_suppressed = sum(1 for timestamp in self.discord_suppressed_digest_recent if timestamp >= cutoff)
        with self.discord_alt_watch_lock:
            alt_watch_pending = len(self.discord_alt_watch_queue)
        digest_channel_configured = bool(self.discord_config.channel_ids.get("digest"))
        now = time.time()
        if suppressed_pending and not self.last_discord_suppressed_digest_sent_at:
            next_digest_in = 0
        else:
            next_digest_in = int(
                max(
                    0,
                    self.last_discord_suppressed_digest_sent_at
                    + DISCORD_SUPPRESSED_DIGEST_INTERVAL_SECONDS
                    - now,
                )
            )
        flush_callable_status = (
            "ready"
            if self.discord_config.enabled and self.discord_config.bot_token and (digest_channel_configured or self.discord_config.channel_ids.get("main"))
            else "disabled"
        )
        return "\n".join(
            [
                "Telegram 临时静音等级",
                f"实时等级: {format_priority_set(realtime_priorities)}",
                f"摘要等级: {format_priority_set(set(digest_priorities))}",
                f"实时把握阈值: {realtime_threshold}",
                f"摘要把握阈值: {watch_threshold}",
                f"风控实时阈值: {risk_realtime_threshold}",
                f"override: {'on' if override_enabled else 'off'}",
                "",
                "Discord 静默摘要",
                f"digest channel id configured: {'yes' if digest_channel_configured else 'no'}",
                f"suppressed digest pending: {suppressed_pending}",
                f"last suppressed digest sent: {format_ts_short(self.last_discord_suppressed_digest_sent_at)}",
                f"interval seconds: {DISCORD_SUPPRESSED_DIGEST_INTERVAL_SECONDS}",
                f"next digest in seconds: {next_digest_in}",
                f"last flush attempt: {format_ts_short(self.last_discord_suppressed_digest_flush_attempt_at)}",
                f"flush callable status: {flush_callable_status}",
                f"last flush status: {self.last_discord_suppressed_digest_flush_status}",
                f"recent suppressed count: {recent_suppressed}",
                f"alt_watch pending: {alt_watch_pending}",
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
            symbol = normalize_usdt_symbol(args[0])
        self.send_telegram_text(bot_token, chat_id, self.liquidation_health_report(symbol))

    def handle_flow_command(self, bot_token: str, chat_id: str, args: list[str]) -> None:
        if not args:
            self.send_telegram_text(bot_token, chat_id, "用法: /flow KNC 或 /flow KNCUSDT")
            return

        symbol = normalize_usdt_symbol(args[0])
        try:
            snapshot, data_source_text, degradation_text = self.telegram_command_snapshot(symbol)
        except Exception:
            logging.exception("Failed to fetch Telegram flow snapshot")
            self.send_telegram_text(bot_token, chat_id, "Binance接口限流或异常，请稍后再试。")
            return

        try:
            signals = self.evaluate_snapshot(snapshot, {"mode": "both"})
            combined_signal = self.combined_signal(snapshot, signals)
            signal = combined_signal or (signals[0] if signals else None)
            parts = [data_source_text]
            if degradation_text:
                parts.append(degradation_text)
            parts.append(format_flow_trader_view(snapshot, signal))
            self.send_telegram_text(bot_token, chat_id, "\n".join(parts))
        except Exception as exc:
            logging.exception("Failed to build flow trader view")
            self.send_telegram_text(bot_token, chat_id, f"{symbol} /flow 查询失败: {type(exc).__name__}: {exc}")

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
        cached_text, cached_at, _cache_source = self.cached_market_summary()
        if cached_text:
            self.send_telegram_text(
                bot_token,
                chat_id,
                self.format_cached_market_summary_response(cached_text, cached_at),
            )
            return

        logging.info("Manual /summary cache miss")
        self.send_telegram_text(bot_token, chat_id, "当前暂无市场摘要缓存，请等待下一轮扫描完成后重试。")

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
        self.send_telegram_text(bot_token, chat_id, self.format_topq_response())

    def format_topq_response(self, discord_view: bool = False) -> str:
        try:
            rows = self.load_recent_signal_rows(200)
        except Exception:
            logging.exception("Failed to read quality signal rows")
            return "读取信号记录失败，请查看服务日志。"

        scored_rows = []
        for recent_index, row in enumerate(rows):
            symbol = str(row.get("symbol") or "").strip().upper()
            if not is_valid_binance_usdt_symbol(symbol):
                continue
            conviction_score_value = parse_float(row.get("conviction_score"))
            kind = topq_kind_normalized(row.get("kind"))
            score_value = parse_float(row.get("score")) or 0
            min_conviction = 55 if kind == "main_momentum_watch" and score_value >= 5 else 65
            if conviction_score_value is None or conviction_score_value < min_conviction:
                continue
            intent = row.get("market_intent_label") or ""
            risk_row = topq_is_risk_candidate(kind)
            bullish_row = topq_is_bullish_candidate(kind)
            leading_score_value = parse_float(row.get("leading_score")) or 0
            leading_direction = str(row.get("leading_direction") or "")
            direction = signal_direction_label(kind)
            basis_label = row.get("basis_state") or ""
            evidence_summary = str(row.get("evidence_summary") or "").strip()
            if leading_score_value <= 0 and conviction_score_value > 64:
                continue
            if bullish_row and kind != "main_momentum_watch" and leading_score_value < 3:
                continue
            if (direction == "看多" or bullish_row) and text_has_any(evidence_summary, TOPQ_BAD_LONG_EVIDENCE_KEYWORDS):
                continue
            if risk_row and not (
                text_has_any(evidence_summary, TOPQ_RISK_EVIDENCE_KEYWORDS)
                or basis_label in PREMIUM_BASIS_STATES
            ):
                continue
            if direction == "看空" and leading_direction == "long" and leading_score_value >= 6:
                conviction_score_value = min(conviction_score_value, 79)
            if kind == "main_momentum_watch" and topq_main_momentum_hard_downgrade(row):
                conviction_score_value = min(conviction_score_value, 69)
            price_change_value = parse_float(row.get("price_change_percent"))
            oi_change_value = parse_float(row.get("oi_change_percent"))
            if kind == "main_risk_watch" and (price_change_value or 0) < 1.5 and (oi_change_value or 0) < 3:
                conviction_score_value = min(conviction_score_value, 79)
            if intent == "震荡分歧" and not (risk_row and conviction_score_value >= 65):
                continue
            priority = str(row.get("signal_priority") or "").upper()
            priority_order = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 0}
            kind_rank = topq_kind_tiebreak_rank(kind, direction)
            scored_rows.append(
                (
                    conviction_score_value,
                    priority_order.get(priority, -1),
                    kind_rank,
                    -recent_index,
                    direction,
                    row,
                )
            )

        if not scored_rows:
            return "暂无高把握/观察候选。"

        scored_rows.sort(key=lambda item: (item[0], item[1], item[2], item[3]), reverse=True)
        best_by_symbol_direction: dict[tuple[str, str], tuple[tuple[int, float, int, int], float, dict[str, str]]] = {}
        for quality_score, priority_rank, kind_rank, recent_rank, direction, row in scored_rows:
            symbol = str(row.get("symbol") or "-").upper()
            key = (symbol, direction)
            selection_key = (kind_rank, quality_score, priority_rank, recent_rank)
            current = best_by_symbol_direction.get(key)
            if current is None or selection_key > current[0]:
                best_by_symbol_direction[key] = (selection_key, quality_score, row)
        priority_order = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 0}
        deduped_rows = sorted(
            ((quality_score, row) for _selection_key, quality_score, row in best_by_symbol_direction.values()),
            key=lambda item: (
                item[0],
                priority_order.get(str(item[1].get("signal_priority") or "").upper(), -1),
                topq_kind_tiebreak_rank(item[1].get("kind"), signal_direction_label(item[1].get("kind"))),
            ),
            reverse=True,
        )[:10]

        lines = ["把握候选TOP10"]
        for index, (conviction_score_value, row) in enumerate(deduped_rows, start=1):
            kind = row.get("kind", "-")
            price_change = format_csv_compact_number(row.get("price_change_percent"), signed=True)
            oi_change = format_csv_compact_number(row.get("oi_change_percent"), signed=True)
            intent = row.get("market_intent_label") or "-"
            direction = signal_direction_label(kind)
            leading_score_value = parse_float(row.get("leading_score")) or 0
            evidence_score_value = parse_float(row.get("evidence_score")) or 0
            long_flow_score_value = parse_float(row.get("long_flow_score")) or 0
            flow_label = str(row.get("flow_trend_label") or "")
            evidence_summary = str(row.get("evidence_summary") or "").strip()
            risk_row = topq_is_risk_candidate(kind)
            momentum_downgraded = topq_kind_normalized(kind) == "main_momentum_watch" and topq_main_momentum_hard_downgrade(row)
            discord_downgraded = discord_view and topq_discord_bullish_display_downgrade(row)
            price_action = safe_multi_timeframe_price_action(row.get("symbol") or "", log_result=True) if discord_view else None
            price_action_downgraded = discord_view and price_action_discord_downgrade(price_action)
            price_action_blocks_high_buy = (
                discord_view
                and direction == "看多"
                and not price_action_allows_discord_high_buy(price_action)
            )
            discord_downgraded = discord_downgraded or price_action_downgraded or price_action_blocks_high_buy
            position_label = row.get("position_behavior_label") or ""
            action, _action_reason = structural_action_override(
                kind,
                row.get("basis_state") or "",
                row.get("squeeze_state_label") or "",
                parse_float(row.get("price_change_percent")),
                parse_float(row.get("oi_change_percent")),
                parse_float(row.get("price_position_24h")),
            )
            if not action:
                action, _action_reason = action_from_trade_context(
                    intent,
                    int(conviction_score_value or 0),
                    direction,
                    position_label,
                )
            if risk_row:
                action = topq_risk_action(evidence_summary, row.get("basis_state") or "")
            elif topq_kind_normalized(kind) == "main_momentum_watch":
                action = MAIN_MOMENTUM_DOWNGRADE_TEXT if momentum_downgraded else "短线拉盘观察，等待确认，不追高"
            elif direction == "看多":
                if (
                    action.startswith("强烈建议关注买入")
                    and not topq_strong_buy_allowed(row, conviction_score_value, leading_score_value, evidence_score_value)
                ):
                    action = "建议观察，等确认入场"
                if long_flow_score_value < 3 and action.startswith("强烈建议关注买入"):
                    action = "建议观察，等确认入场"
                action = topq_action_short(action)
            if discord_downgraded:
                action = DISCORD_BULLISH_DOWNGRADE_TEXT
            short_flow = format_csv_compact_number(row.get("short_flow_score"), signed=False)
            mid_flow = format_csv_compact_number(row.get("mid_flow_score"), signed=False)
            long_flow = format_csv_compact_number(row.get("long_flow_score"), signed=False)
            direction_icon_text = "🔴" if direction == "看空" else "🟢" if direction == "看多" else "⚪"
            quality_score_value = parse_float(row.get("signal_quality_score")) or 0
            status = "重点" if conviction_score_value >= 80 else "观察"
            if flow_label in TOPQ_WEAK_FLOW_LABELS or momentum_downgraded or discord_downgraded:
                status = "观察"
            if risk_row and intent == "震荡分歧" and not (conviction_score_value >= 90 and quality_score_value >= 70):
                status = "观察"
            if direction == "看空" and str(row.get("leading_direction") or "") == "long" and leading_score_value >= 6:
                status = "观察"
            if topq_kind_normalized(kind) == "main_risk_watch":
                price_change_value = parse_float(row.get("price_change_percent"))
                oi_change_value = parse_float(row.get("oi_change_percent"))
                if (price_change_value or 0) < 1.5 and (oi_change_value or 0) < 3:
                    status = "观察"
            status_icon = "🔴" if status == "重点" and direction == "看空" else "🟢" if status == "重点" else "🟡"
            symbol = display_usdt_symbol(row.get("symbol"))
            leading_part = leading_topq_brief(row)
            kind_label = topq_signal_kind_label(row, action)
            main_momentum_observation = (
                topq_kind_normalized(kind) == "main_momentum_watch"
                or kind_label == "主流异动观察"
            )
            evidence_direction = discord_infer_evidence_direction(
                evidence_summary,
                str(row.get("evidence_direction") or direction),
            )
            conflict_summary = ""
            if discord_view:
                conflict_summary = discord_conflict_aware_summary(
                    kind,
                    direction,
                    evidence_direction,
                    int(evidence_score_value or 0),
                    evidence_summary,
                    str(row.get("leading_direction") or ""),
                    flow_label,
                    int(parse_float(row.get("trap_risk_score")) or 0),
                    price_action,
                )
            if main_momentum_observation:
                conclusion = (
                    MAIN_MOMENTUM_TOPQ_TEXT
                    if momentum_downgraded
                    else "短线拉盘，等确认"
                )
                if discord_view and discord_summary_is_conflict_override(conflict_summary, evidence_summary):
                    conclusion = conflict_summary
            elif discord_downgraded:
                conclusion = "等待确认，不追高"
                if price_action:
                    conclusion = f"K线{price_action.score}/10 {price_action.label}"
                if discord_view and discord_summary_is_conflict_override(conflict_summary, evidence_summary):
                    conclusion = conflict_summary
            else:
                conclusion = topq_conclusion_text(
                    direction,
                    evidence_summary,
                    action,
                    intent,
                    flow_label,
                )
                if discord_view and discord_summary_is_conflict_override(conflict_summary, evidence_summary):
                    conclusion = conflict_summary
            lines.append(
                f"{index}. {status_icon}{status} {direction_icon_text} {symbol} {kind_label} | "
                f"把握{conviction_score_value:.0f} {leading_part}"
            )
            kline_part = ""
            if discord_view and price_action:
                kline_part = f" | K线 短{price_action.short_score} 中{price_action.mid_score} 长{price_action.long_score} {price_action.label}"
            lines.append(
                f"   {price_change}% OI{oi_change}% | 短{short_flow} 中{mid_flow} 长{long_flow}{kline_part} | {conclusion}"
            )
        return "\n".join(lines)

    def send_telegram_text(self, bot_token: str, chat_id: str, text: str) -> None:
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self.post_json(url, {"chat_id": chat_id, "text": telegram_text(text)})

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


def summary_symbol_label(symbol: str) -> str:
    normalized = str(symbol or "").upper()
    label = base_symbol(normalized)
    return normalized if len(label) < 2 else label


def is_summary_display_symbol(symbol: str | None) -> bool:
    normalized = str(symbol or "").strip().upper()
    if not normalized.endswith("USDT"):
        return False
    base = normalized[:-4]
    return bool(re.fullmatch(r"[A-Z0-9]{1,20}", base))


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


def is_core_momentum_asset(symbol: str) -> bool:
    return str(symbol or "").strip().upper() in CORE_MOMENTUM_SYMBOLS or market_tier(symbol) in ("core", "major")


def snapshot_price_change(snapshot: MarketSnapshot, period: str) -> float | None:
    if snapshot.price_change_periods:
        value = snapshot.price_change_periods.get(period)
        if value is not None:
            return value
    if period in ("5m", "15m"):
        return snapshot.confirm_price_change_percent
    if period == "1h":
        return snapshot.price_change_percent
    return None


def main_momentum_watch_signal(snapshot: MarketSnapshot) -> Signal | None:
    if not is_core_momentum_asset(snapshot.symbol):
        return None

    price_15m = snapshot_price_change(snapshot, "15m")
    price_1h = snapshot_price_change(snapshot, "1h")
    flow_15m = summary_flow_value(snapshot, "15m")
    flow_1h = summary_flow_value(snapshot, "1h")
    short_flow, mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    threshold_15m = 0.25 if snapshot.symbol in {"BTCUSDT", "ETHUSDT"} else 0.35
    oi_15m = snapshot.confirm_oi_change_percent
    oi_1h = snapshot.oi_change_percent
    oi_15m_up = (oi_15m or 0) > 0
    oi_1h_up = oi_1h > 0
    ev_score, ev_direction, _ev_summary, _ev_items = evidence_score(snapshot, None)
    spot_score, _spot_label, _spot_reason = spot_onchain_score(snapshot, None)
    squeeze_label, _squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    absorption_label, _absorption_score, _absorption_reason = spot_absorption_state(snapshot, None)
    strong_spot_confirm = spot_score >= 7
    price_supported = (price_15m or 0) > 0 or (price_1h or 0) > 0
    oi_supported = (oi_15m or 0) > 0 or (oi_1h or 0) > 0

    if not (price_supported and oi_supported):
        return None
    if (oi_15m or 0) <= 0 and (oi_1h or 0) <= 0 and not (ev_score >= 8 and strong_spot_confirm):
        return None
    if flow_label == "中长线派发":
        return None
    if short_flow <= 2 and mid_flow <= 3:
        return None

    reasons: list[str] = []
    if price_15m is not None and price_15m >= threshold_15m and flow_15m > 0:
        reasons.append(f"15m涨{price_15m:+.2f}%且资金流入")
    if (
        price_15m is not None
        and price_1h is not None
        and price_15m > 0
        and price_1h > 0
        and (oi_15m_up or oi_1h_up)
    ):
        reasons.append("15m/1h价格与OI同步上升")
    if ev_direction == "看多" and ev_score >= 8 and strong_spot_confirm:
        reasons.append("证据强多且现货/链上承接强")
    if squeeze_label == "空头挤压" and absorption_label in ("现货承接", "链上承接") and strong_spot_confirm:
        reasons.append("空头挤压+现货承接")

    if not reasons:
        return None

    downgraded = main_momentum_hard_downgrade(snapshot)
    mid_unconfirmed = flow_1h < 0 or mid_flow <= 4 or flow_label in ("短强中弱", "中长线派发", "资金分歧")
    conclusion = MAIN_MOMENTUM_DOWNGRADE_TEXT if downgraded else "短线强，中线未确认" if mid_unconfirmed else "短线拉盘观察"
    score = min(
        10,
        5
        + int(price_15m is not None and price_15m >= threshold_15m)
        + int(flow_15m > 0)
        + int(oi_15m_up or oi_1h_up)
        + int(short_flow >= 6)
        + int(ev_direction == "看多" and ev_score >= 8 and spot_score >= 7)
        + int(squeeze_label == "空头挤压" and absorption_label in ("现货承接", "链上承接")),
    )
    return Signal(
        symbol=snapshot.symbol,
        kind="main_momentum_watch",
        score=score,
        title=f"{snapshot.symbol} 主流异动雷达",
        message=f"主流异动雷达：{conclusion}；{'；'.join(reasons[:2])}。",
        key=f"{snapshot.symbol}:main_momentum_watch",
        snapshot=snapshot,
    )


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


def interval_seconds(value: str) -> int | None:
    match = re.fullmatch(r"(\d+)([mhd])", str(value).strip().lower())
    if not match:
        return None
    amount = int(match.group(1))
    return amount * {"m": 60, "h": 3600, "d": 86400}[match.group(2)]


def price_change_periods_from_klines(klines: list[Any], interval: str) -> dict[str, float]:
    seconds = interval_seconds(interval)
    if not seconds or not klines:
        return {}
    result: dict[str, float] = {}
    last_close = float(klines[-1][4])
    for label in ("5m", "15m", "1h", "4h"):
        target_seconds = interval_seconds(label)
        if not target_seconds:
            continue
        bars = max(1, int(target_seconds / seconds))
        if len(klines) <= bars:
            continue
        start_close = float(klines[-bars - 1][4])
        result[label] = percent_change(start_close, last_close)
    return result


def price_action_cache_ttl(interval: str) -> int:
    if interval in {"5m", "15m"}:
        return 180
    if interval in {"1h", "4h"}:
        return 600
    return 3600


def fetch_price_action_klines(symbol: str, interval: str, limit: int = 120) -> list[Any]:
    normalized = str(symbol or "").strip().upper()
    key = (normalized, interval)
    now = time.time()
    cached = _PRICE_ACTION_KLINE_CACHE.get(key)
    if cached and now - cached[0] < price_action_cache_ttl(interval):
        return cached[1]
    response = requests.get(
        f"{BINANCE_FAPI_BASE}/fapi/v1/klines",
        params={"symbol": normalized, "interval": interval, "limit": limit},
        timeout=8,
    )
    response.raise_for_status()
    rows = response.json()
    if not isinstance(rows, list):
        rows = []
    _PRICE_ACTION_KLINE_CACHE[key] = (now, rows)
    return rows


def cached_price_action_klines(symbol: str, interval: str) -> list[Any]:
    normalized = str(symbol or "").strip().upper()
    cached = _PRICE_ACTION_KLINE_CACHE.get((normalized, interval))
    if not cached or time.time() - cached[0] >= price_action_cache_ttl(interval):
        return []
    return cached[1]


def kline_float(row: Any, index: int) -> float | None:
    try:
        return float(row[index])
    except Exception:
        return None


def kline_values(rows: list[Any], index: int) -> list[float]:
    return [value for value in (kline_float(row, index) for row in rows) if value is not None]


def ema_value(values: list[float], period: int) -> float | None:
    if len(values) < period:
        return None
    alpha = 2 / (period + 1)
    ema = sum(values[:period]) / period
    for value in values[period:]:
        ema = value * alpha + ema * (1 - alpha)
    return ema


def price_action_trend_label(score: int) -> str:
    if score >= 7:
        return "偏多确认"
    if score <= 3:
        return "偏空/未确认"
    return "震荡分歧"


def analyze_short_price_action(rows_5m: list[Any], rows_15m: list[Any]) -> tuple[int, str, list[str], list[str], list[str]]:
    rows = rows_15m or rows_5m
    closes = kline_values(rows, 4)
    highs = kline_values(rows, 2)
    lows = kline_values(rows, 3)
    volumes = kline_values(rows, 5)
    if len(closes) < 25:
        return 5, "短线数据不足", [], ["短线K线数据不足"], []
    score = 5
    items: list[str] = []
    risks: list[str] = []
    patterns: list[str] = []
    last_close = closes[-1]
    prev_high = max(highs[-21:-1]) if len(highs) >= 21 else max(highs[:-1])
    avg_volume = sum(volumes[-21:-1]) / max(1, len(volumes[-21:-1]))
    last_volume = volumes[-1] if volumes else 0
    if last_close > prev_high and last_volume >= avg_volume * 1.2:
        score += 2
        items.append("5m/15m 放量突破近20根高点")
        patterns.append("15m 突破近20根高点")
    if len(closes) >= 4 and closes[-1] > closes[-2] > closes[-3]:
        score += 1
        items.append("短线连续收盘抬高")
    ema20 = ema_value(closes, 20)
    range_mid = (max(highs[-20:]) + min(lows[-20:])) / 2 if len(highs) >= 20 and len(lows) >= 20 else None
    if ema20 is not None and last_close >= ema20 and (range_mid is None or last_close >= range_mid):
        score += 1
        items.append("回踩不破短均线/区间中位")
    last_open = kline_float(rows[-1], 1) or last_close
    last_high = highs[-1]
    last_low = lows[-1]
    body = abs(last_close - last_open)
    upper_shadow = last_high - max(last_close, last_open)
    lower_shadow = min(last_close, last_open) - last_low
    if upper_shadow > max(body * 1.8, last_close * 0.002):
        score -= 2
        risks.append("短线长上影，追高风险")
        patterns.append("15m 长上影")
    if lower_shadow > max(body * 1.8, last_close * 0.002):
        score += 1
        items.append("短线长下影承接")
    if last_close > prev_high * 1.015 and last_volume < avg_volume:
        score -= 1
        risks.append("突破量能不足，追高风险")
    if len(rows) >= 2:
        prev_open = kline_float(rows[-2], 1) or closes[-2]
        prev_close = closes[-2]
        if last_close > last_open and prev_close < prev_open and last_close >= prev_open and last_open <= prev_close:
            score += 1
            patterns.append("15m 阳包阴")
        if last_close < last_open and prev_close > prev_open and last_open >= prev_close and last_close <= prev_open:
            score -= 1
            patterns.append("15m 阴包阳")
    if last_close > last_open and last_volume >= avg_volume * 1.5:
        patterns.append("15m 放量阳线")
    score = clamp_int(score, 0, 10)
    return score, price_action_trend_label(score), items, risks, list(dict.fromkeys(patterns))


def analyze_mid_price_action(rows_1h: list[Any], rows_4h: list[Any]) -> tuple[int, str, list[str], list[str], list[str]]:
    score = 5
    items: list[str] = []
    risks: list[str] = []
    patterns: list[str] = []
    directions: list[str] = []
    for label, rows in (("1h", rows_1h), ("4h", rows_4h)):
        closes = kline_values(rows, 4)
        highs = kline_values(rows, 2)
        lows = kline_values(rows, 3)
        volumes = kline_values(rows, 5)
        if len(closes) < 65:
            risks.append(f"{label} K线数据不足")
            continue
        last_close = closes[-1]
        ema20 = ema_value(closes, 20)
        ema60 = ema_value(closes, 60)
        if ema20 is not None and ema60 is not None and last_close > ema20 > ema60:
            score += 2
            directions.append("bullish")
            items.append(f"{label} 位于 EMA20/EMA60 上方")
        elif ema20 is not None and ema60 is not None and last_close < ema20 < ema60:
            score -= 2
            directions.append("bearish")
            risks.append(f"{label} 位于 EMA20/EMA60 下方")
        box_high = max(highs[-51:-1])
        box_low = min(lows[-51:-1])
        if last_close > box_high:
            score += 1
            items.append(f"{label} 突破近50根箱体")
        elif last_close < box_low:
            score -= 1
            risks.append(f"{label} 跌破近50根箱体")
        if label == "1h" and len(lows) >= 21 and last_close < min(lows[-21:-1]):
            patterns.append("1h 跌破近20根低点")
        avg_volume = sum(volumes[-21:-1]) / max(1, len(volumes[-21:-1]))
        if last_close >= box_high * 0.985 and volumes[-1] >= avg_volume * 1.3 and closes[-1] <= closes[-2] * 1.003:
            score -= 2
            risks.append(f"{label} 高位放量滞涨")
            if label == "1h":
                patterns.append("1h 放量滞涨")
        if label == "1h" and len(closes) >= 4 and closes[-1] > closes[-2] > closes[-3]:
            patterns.append("1h 连续收盘抬高")
        if label == "4h" and ema20 is not None and lows[-1] <= ema20 <= last_close:
            patterns.append("4h 回踩不破短均线")
        if label == "4h" and last_close <= box_high and last_close >= box_high * 0.96:
            patterns.append("4h 箱体未突破")
    if "bullish" in directions and "bearish" in directions:
        score -= 1
        risks.append("1h/4h 趋势不同向")
    if "bullish" not in directions:
        risks.append("4h结构还没确认")
        patterns.append("4h 箱体未突破")
    score = clamp_int(score, 0, 10)
    return score, price_action_trend_label(score), items, risks, list(dict.fromkeys(patterns))


def analyze_long_price_action(rows_1d: list[Any], rows_3d: list[Any], rows_1w: list[Any]) -> tuple[int, str, list[str], list[str], list[str]]:
    score = 5
    items: list[str] = []
    risks: list[str] = []
    patterns: list[str] = []
    long_downtrend = False
    long_uptrend = False
    for label, rows in (("1d", rows_1d), ("3d", rows_3d), ("1w", rows_1w)):
        closes = kline_values(rows, 4)
        highs = kline_values(rows, 2)
        lows = kline_values(rows, 3)
        if len(closes) < 65:
            risks.append(f"{label} K线数据不足")
            continue
        last_close = closes[-1]
        ema20 = ema_value(closes, 20)
        ema60 = ema_value(closes, 60)
        if ema20 is not None and ema60 is not None and last_close > ema20 > ema60:
            score += 1
            long_uptrend = True
            items.append(f"{label} 大周期位于 EMA20/EMA60 上方")
        elif ema20 is not None and ema60 is not None and last_close < ema20 < ema60:
            score -= 1
            long_downtrend = True
            risks.append(f"{label} 大周期下跌趋势反弹")
        high_90 = max(highs[-91:-1]) if len(highs) >= 91 else max(highs[:-1])
        low_90 = min(lows[-91:-1]) if len(lows) >= 91 else min(lows[:-1])
        if high_90 and last_close >= high_90 * 0.97:
            score -= 1
            risks.append("大周期压力位附近")
            if label == "1w":
                patterns.append("1w 接近周线压力")
            elif label == "1d":
                patterns.append("1d 接近日线压力")
        if low_90 and last_close <= low_90 * 1.05:
            score += 1
            items.append("大周期支撑承接观察")
            if label == "1d":
                patterns.append("1d 大周期支撑承接")
    if long_downtrend and not long_uptrend:
        risks.append("处在大周期下跌趋势中的反弹")
    if long_uptrend and score <= 6:
        items.append("大周期上升趋势中的回踩")
    score = clamp_int(score, 0, 10)
    return score, price_action_trend_label(score), items, risks, list(dict.fromkeys(patterns))


def multi_timeframe_price_action_confirmation(symbol: str, log_result: bool = True) -> MultiTimeframePriceAction:
    klines = {interval: fetch_price_action_klines(symbol, interval) for interval in PRICE_ACTION_INTERVALS}
    short_score, short_label, short_items, short_risks, short_patterns = analyze_short_price_action(klines["5m"], klines["15m"])
    mid_score, mid_label, mid_items, mid_risks, mid_patterns = analyze_mid_price_action(klines["1h"], klines["4h"])
    long_score, long_label, long_items, long_risks, long_patterns = analyze_long_price_action(klines["1d"], klines["3d"], klines["1w"])
    score = clamp_int(short_score * 0.3 + mid_score * 0.4 + long_score * 0.3, 0, 10)
    risk_items = list(dict.fromkeys(short_risks + mid_risks + long_risks))
    items = list(dict.fromkeys(short_items + mid_items + long_items))
    patterns = list(dict.fromkeys(short_patterns + mid_patterns + long_patterns))
    if mid_score <= 3 and long_score <= 4:
        direction = "bearish"
    elif short_score >= 7 and mid_score >= 6 and long_score >= 5:
        direction = "bullish"
    elif abs(short_score - long_score) >= 4 or risk_items:
        direction = "mixed"
    else:
        direction = "neutral"
    if short_score >= 7 and long_score <= 3:
        label = "短线强，大周期未确认"
    elif short_score >= 7 and mid_score >= 6 and long_score >= 5:
        label = "多周期偏多确认"
    elif mid_score <= 3 or long_score <= 3:
        label = "中长周期偏弱"
    elif risk_items and any("压力位附近" in item or "追高风险" in item for item in risk_items):
        label = "压力位附近/追高风险"
    else:
        label = "多周期震荡分歧" if direction in ("mixed", "neutral") else "多周期偏空"
    if long_score <= 3 or "大周期未确认" in label:
        recommendation = "短线异动增强，等待中长周期确认，不追高"
    elif any("压力位附近" in item or "追高风险" in item for item in risk_items):
        recommendation = "压力位附近，等待回踩/放量确认"
    elif score >= 6 and mid_score >= 6 and long_score >= 5:
        recommendation = "K线结构支持，仍需结合资金确认"
    else:
        recommendation = "结构分歧，观察为主"
    result = MultiTimeframePriceAction(
        score=score,
        label=label,
        direction=direction,
        short_score=short_score,
        mid_score=mid_score,
        long_score=long_score,
        short_label=short_label,
        mid_label=mid_label,
        long_label=long_label,
        items=items[:10],
        risk_items=risk_items[:10],
        patterns=patterns[:12],
        recommendation=recommendation,
    )
    if log_result:
        logging.info(
            "Price action confirmation: %s score=%s short=%s mid=%s long=%s label=%s",
            symbol,
            result.score,
            result.short_score,
            result.mid_score,
            result.long_score,
            result.label,
        )
    return result


def safe_multi_timeframe_price_action(symbol: str, log_result: bool = True) -> MultiTimeframePriceAction | None:
    try:
        return multi_timeframe_price_action_confirmation(symbol, log_result=log_result)
    except Exception:
        logging.debug("Failed to build multi-timeframe price action for %s", symbol, exc_info=True)
        return MultiTimeframePriceAction(
            score=5,
            label="K线数据不足",
            direction="neutral",
            short_score=5,
            mid_score=5,
            long_score=5,
            short_label="短线数据不足",
            mid_label="中线数据不足",
            long_label="大周期数据不足",
            items=[],
            risk_items=["K线数据不足"],
            patterns=[],
            recommendation="K线数据不足，观察为主",
        )


def insufficient_price_action() -> MultiTimeframePriceAction:
    return MultiTimeframePriceAction(
        score=5,
        label="K线数据不足",
        direction="neutral",
        short_score=5,
        mid_score=5,
        long_score=5,
        short_label="短线数据不足",
        mid_label="中线数据不足",
        long_label="大周期数据不足",
        items=[],
        risk_items=["K线数据不足"],
        patterns=[],
        recommendation="K线数据不足，观察为主",
    )


def cached_multi_timeframe_price_action(symbol: str) -> MultiTimeframePriceAction:
    normalized = str(symbol or "").strip().upper()
    now = time.time()
    klines: dict[str, list[Any]] = {}
    for interval in PRICE_ACTION_INTERVALS:
        cached = _PRICE_ACTION_KLINE_CACHE.get((normalized, interval))
        if interval in {"3d", "1w"} and (not cached or now - cached[0] >= price_action_cache_ttl(interval)):
            klines[interval] = []
            continue
        if not cached or now - cached[0] >= price_action_cache_ttl(interval):
            return insufficient_price_action()
        klines[interval] = cached[1]
    try:
        short_score, short_label, short_items, short_risks, short_patterns = analyze_short_price_action(klines["5m"], klines["15m"])
        mid_score, mid_label, mid_items, mid_risks, mid_patterns = analyze_mid_price_action(klines["1h"], klines["4h"])
        long_score, long_label, long_items, long_risks, long_patterns = analyze_long_price_action(klines["1d"], klines["3d"], klines["1w"])
    except Exception:
        return insufficient_price_action()
    score = clamp_int(short_score * 0.3 + mid_score * 0.4 + long_score * 0.3, 0, 10)
    items = list(dict.fromkeys(short_items + mid_items + long_items))
    risk_items = list(dict.fromkeys(short_risks + mid_risks + long_risks))
    patterns = list(dict.fromkeys(short_patterns + mid_patterns + long_patterns))
    if short_score >= 7 and long_score <= 3:
        label = "短线强，大周期未确认"
    elif short_score >= 7 and mid_score >= 6 and long_score >= 5:
        label = "多周期偏多确认"
    elif risk_items and any("压力位附近" in item or "追高风险" in item for item in risk_items):
        label = "压力位附近/追高风险"
    elif mid_score <= 3 or long_score <= 3:
        label = "中长周期偏弱"
    else:
        label = "多周期震荡分歧"
    direction = "bullish" if score >= 7 else "bearish" if score <= 3 else "mixed"
    if long_score <= 3 or "大周期未确认" in label:
        recommendation = "不追高，等待中长周期确认"
    elif any("压力位附近" in item or "追高风险" in item for item in risk_items):
        recommendation = "等回踩/放量确认，不追高"
    elif score >= 6:
        recommendation = "观察"
    else:
        recommendation = "风险观察"
    return MultiTimeframePriceAction(
        score=score,
        label=label,
        direction=direction,
        short_score=short_score,
        mid_score=mid_score,
        long_score=long_score,
        short_label=short_label,
        mid_label=mid_label,
        long_label=long_label,
        items=items[:10],
        risk_items=risk_items[:10],
        patterns=patterns[:12],
        recommendation=recommendation,
    )


def light_multi_timeframe_price_action(symbol: str) -> MultiTimeframePriceAction:
    normalized = str(symbol or "").strip().upper()
    if not normalized:
        return insufficient_price_action()
    try:
        klines = {
            "5m": fetch_price_action_klines(normalized, "5m"),
            "15m": fetch_price_action_klines(normalized, "15m"),
            "1h": fetch_price_action_klines(normalized, "1h"),
            "4h": fetch_price_action_klines(normalized, "4h"),
            "1d": fetch_price_action_klines(normalized, "1d"),
            "3d": cached_price_action_klines(normalized, "3d"),
            "1w": cached_price_action_klines(normalized, "1w"),
        }
        short_score, short_label, short_items, short_risks, short_patterns = analyze_short_price_action(klines["5m"], klines["15m"])
        mid_score, mid_label, mid_items, mid_risks, mid_patterns = analyze_mid_price_action(klines["1h"], klines["4h"])
        long_score, long_label, long_items, long_risks, long_patterns = analyze_long_price_action(klines["1d"], klines["3d"], klines["1w"])
    except Exception:
        logging.debug("Failed to build light multi-timeframe price action for %s", normalized, exc_info=True)
        return cached_multi_timeframe_price_action(normalized)
    score = clamp_int(short_score * 0.3 + mid_score * 0.4 + long_score * 0.3, 0, 10)
    items = list(dict.fromkeys(short_items + mid_items + long_items))
    risk_items = list(dict.fromkeys(short_risks + mid_risks + long_risks))
    patterns = list(dict.fromkeys(short_patterns + mid_patterns + long_patterns))
    if short_score >= 7 and long_score <= 3:
        label = "短线强，大周期未确认"
    elif short_score >= 7 and mid_score >= 6 and long_score >= 5:
        label = "多周期偏多确认"
    elif risk_items and any("压力位附近" in item or "追高风险" in item for item in risk_items):
        label = "压力位附近/追高风险"
    elif mid_score <= 3 or long_score <= 3:
        label = "中长周期偏弱"
    else:
        label = "多周期震荡分歧"
    direction = "bullish" if score >= 7 else "bearish" if score <= 3 else "mixed"
    recommendation = (
        DISCORD_BULLISH_DOWNGRADE_TEXT
        if long_score <= 3 or text_has_any(label, ("大周期未确认", "压力位附近", "追高风险"))
        else "结构分歧，观察为主"
    )
    return MultiTimeframePriceAction(
        score=score,
        label=label,
        direction=direction,
        short_score=short_score,
        mid_score=mid_score,
        long_score=long_score,
        short_label=short_label,
        mid_label=mid_label,
        long_label=long_label,
        items=items[:10],
        risk_items=risk_items[:10],
        patterns=patterns[:12],
        recommendation=recommendation,
    )


def flow_cache_ttl_seconds(period: str) -> int:
    if period in FLOW_SHORT_PERIODS:
        return FLOW_SHORT_CACHE_TTL_SECONDS
    if period in FLOW_MID_PERIODS:
        return FLOW_MID_CACHE_TTL_SECONDS
    return FLOW_LONG_CACHE_TTL_SECONDS


def flow_binance_request(period: str) -> tuple[str, int, int]:
    daily_periods = {
        "24h": 1,
        "48h": 2,
        "72h": 3,
        "96h": 4,
        "120h": 5,
        "144h": 6,
    }
    if period in daily_periods:
        rows = daily_periods[period]
        return "1d", rows, rows
    return period, 1, 1


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


def env_flag_enabled(name: str) -> bool:
    return str(discord_env_value(name)).strip().lower().strip("'\"") in {"1", "true", "yes", "on"}


def parse_env_file_value(path: str, name: str) -> str:
    def parse_lines(lines: list[str]) -> str:
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            if key.strip() == name:
                return value.strip().strip("'\"")
        return ""

    try:
        with open(path, encoding="utf-8") as file:
            return parse_lines(file.readlines())
    except FileNotFoundError:
        return ""
    except PermissionError:
        try:
            result = subprocess.run(
                ["sudo", "-n", "cat", path],
                check=False,
                capture_output=True,
                text=True,
                timeout=2,
            )
            if result.returncode == 0:
                return parse_lines(result.stdout.splitlines())
        except Exception:
            logging.debug("Failed to read env file %s via sudo", path, exc_info=True)
    except Exception:
        logging.debug("Failed to read env file %s", path, exc_info=True)
    return ""


def discord_env_value(name: str) -> str:
    value = str(os.getenv(name, "")).strip()
    if value:
        return value.strip("'\"")
    return parse_env_file_value("/etc/crypto-monitor.env", name)


def resolve_discord_config() -> DiscordConfig:
    channel_ids = {
        key: discord_env_value(env_name)
        for key, env_name in DISCORD_CHANNEL_ENV_KEYS.items()
        if discord_env_value(env_name)
    }
    return DiscordConfig(
        enabled=env_flag_enabled("DISCORD_ENABLED"),
        bot_token=discord_env_value("DISCORD_BOT_TOKEN"),
        channel_ids=channel_ids,
    )


def discord_env_diagnostics() -> str:
    config = resolve_discord_config()
    lines = [
        f"DISCORD_ENABLED={int(config.enabled)}",
        f"DISCORD_BOT_TOKEN={'set' if config.bot_token else 'missing'}",
    ]
    for _key, env_name in DISCORD_CHANNEL_ENV_KEYS.items():
        value = discord_env_value(env_name)
        lines.append(f"{env_name}={value or 'missing'}")
    return "\n".join(lines)


def discord_channel_for_signal(signal: Signal, priority: str) -> str:
    normalized_kind = str(signal.kind or "").strip().lower().replace("-", "_").replace(" ", "_")
    if normalized_kind in {"main_momentum_watch", "main_trend_watch"}:
        return "main"
    if normalized_kind in {"main_risk_watch", "top_risk", "top_exhaustion", "distribution"}:
        return "risk"
    if str(priority or "").strip().upper() in {"S", "A", "B"}:
        return "alerts"
    return "digest"


def discord_channel_for_pending_signal(pending: PendingTelegramSignalMerge) -> str:
    priorities = [str(priority or "").strip().upper() for priority in pending.priorities]
    best_priority = best_priority_label(priorities)
    for signal in pending.signals:
        channel = discord_channel_for_signal(signal, best_priority)
        if channel in {"main", "risk"}:
            return channel
    if any(priority in {"S", "A", "B"} for priority in priorities):
        return "alerts"
    return "digest"


def discord_signal_title(signal: Signal, priority: str) -> str:
    normalized_kind = str(signal.kind or "").strip().lower().replace("-", "_").replace(" ", "_")
    price_action = safe_multi_timeframe_price_action(signal.symbol, log_result=False) if signal.snapshot else None
    pair = discord_symbol_pair(signal.symbol)
    if normalized_kind == "main_momentum_watch":
        if signal.snapshot and (main_momentum_hard_downgrade(signal.snapshot) or price_action_discord_downgrade(price_action)):
            return f"🟡 主流异动观察 {pair}"
        return f"🟡 主流异动雷达 {pair}"
    if normalized_kind == "main_trend_watch":
        return f"🟢 主流趋势雷达 {pair}"
    if normalized_kind in {"main_risk_watch", "top_risk", "top_exhaustion", "distribution"} or is_risk_structure_kind(signal.kind):
        if price_action_structure_risk_confirmed(price_action):
            return f"🔴 结构风险确认 {pair}"
        return f"🔴 风险雷达 {pair}"
    if signal.snapshot and discord_bullish_display_downgrade(signal, price_action=price_action):
        return f"🟡 观察信号 {pair}"
    if signal.snapshot and signal_direction_label(signal.kind) == "看多" and not price_action_allows_discord_high_buy(price_action):
        return f"🟡 观察信号 {pair}"
    if str(priority or "").strip().upper() in {"S", "A", "B"}:
        return f"🟢 高把握信号 {pair}"
    return f"{signal_kind_label(signal.kind)} {pair}"


def discord_symbol_pair(symbol: str | None) -> str:
    normalized = str(symbol or "").strip().upper()
    if normalized.endswith("USDT"):
        return f"{normalized[:-4]}/USDT"
    return normalized or "-"


def discord_signal_content_title(signal: Signal, priority: str) -> str:
    return discord_signal_title(signal, priority)


def discord_signal_color(signal: Signal) -> int:
    if is_risk_structure_kind(signal.kind):
        return DISCORD_COLOR_RISK
    price_action = safe_multi_timeframe_price_action(signal.symbol, log_result=False) if signal.snapshot else None
    if signal.snapshot and discord_bullish_display_downgrade(signal, price_action=price_action):
        return DISCORD_COLOR_WATCH
    if signal_direction_label(signal.kind) == "看多":
        return DISCORD_COLOR_BULLISH
    return DISCORD_COLOR_WATCH


def discord_clean_summary_text(text: str, title: str = "", remove_title: bool = False) -> str:
    normalized_title = str(title or "").strip()
    seen_title = False
    cleaned_lines: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = raw_line.strip()
        if line.startswith("[SUMMARY]"):
            continue
        if normalized_title and line == normalized_title:
            if remove_title or seen_title:
                continue
            seen_title = True
        cleaned_lines.append(raw_line)
    return "\n".join(cleaned_lines).strip()


def discord_embed_description_text(item: DiscordOutboundMessage) -> str:
    content = str(item.content or "")
    if not content:
        return ""
    if item.title and not item.fields:
        return discord_clean_summary_text(content, title=item.title, remove_title=True)
    return content


def discord_evidence_field(
    signal: Signal,
    ev_direction: str,
    ev_display: int,
    ev_summary: str,
    fallback: str,
) -> tuple[str, str, bool]:
    if signal.kind == "main_momentum_watch" and signal.snapshot:
        if main_momentum_hard_downgrade(signal.snapshot):
            return ("证据", MAIN_MOMENTUM_DOWNGRADE_TEXT, False)
        short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(signal.snapshot)
        if flow_label == "短强中弱":
            return ("证据", "短线异动，中线未确认", False)
        if short_flow <= 4:
            return ("证据", ev_summary or fallback or "短线异动观察，资金仍需确认", False)
    if not is_risk_structure_kind(signal.kind):
        return ("证据", f"{ev_direction} {ev_display}分 - {ev_summary or fallback}", False)

    summary = ev_summary or fallback
    if ev_direction == "看多" and text_has_any(summary, TOPQ_RISK_EVIDENCE_KEYWORDS):
        return ("风险结论", f"风险结论：{summary}", False)
    return ("风险证据", summary or "风险结构观察", False)


def discord_risk_tip_field(
    signal: Signal,
    trap_score: int,
    trap_label: str,
    trap_reason: str,
) -> tuple[str, str, bool] | None:
    if is_risk_structure_kind(signal.kind):
        if trap_score >= 6:
            return ("风险提示", f"波动/诱捕风险较高 - {trap_reason}", False)
        return None
    if trap_score >= 3:
        return ("风险提示", f"{trap_label} {trap_score}/10 - {trap_reason}", False)
    return None


def discord_flow_field_value(snapshot: MarketSnapshot) -> str:
    short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)

    def flow_part(period: str) -> str:
        return f"{period} {format_usd(summary_flow_value(snapshot, period))}"

    short_line = " / ".join(flow_part(period) for period in ("5m", "15m", "1h"))
    mid_line = " / ".join(flow_part(period) for period in ("4h", "12h", "24h", "72h"))
    structure = f"短{short_flow}/中{mid_flow}/长{long_flow} | {flow_label}"
    return f"{short_line}\n{mid_line}\n{structure}"


def discord_field_value(text: str) -> str:
    return truncate_text(str(text or ""), 900)


def discord_price_oi_field_value(snapshot: MarketSnapshot) -> str:
    confirm_parts = []
    if snapshot.confirm_price_change_percent is not None:
        confirm_parts.append(f"确认价 {snapshot.confirm_price_change_percent:+.2f}%")
    if snapshot.confirm_oi_change_percent is not None:
        confirm_parts.append(f"确认OI {snapshot.confirm_oi_change_percent:+.2f}%")
    confirm_text = " | " + " / ".join(confirm_parts) if confirm_parts else ""
    return discord_field_value(
        f"价格 {snapshot.close_price:.8g} | 涨跌 {snapshot.price_change_percent:+.2f}% | "
        f"OI {snapshot.oi_change_percent:+.2f}%{confirm_text}"
    )


def discord_derivatives_field_value(snapshot: MarketSnapshot) -> str:
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    basis_text = f"{basis_pct:+.2f}% {basis_label}" if basis_pct is not None else basis_label
    return discord_field_value(
        f"Funding {format_optional_value(snapshot.funding_rate_percent)}% | 基差 {basis_text}\n"
        f"全局多空 {format_optional_value(snapshot.global_long_short_ratio)} | "
        f"大户持仓 {format_optional_value(snapshot.top_position_ratio)} | "
        f"大户账户 {format_optional_value(snapshot.top_account_ratio)}\n"
        f"主动买卖 {format_optional_value(snapshot.taker_buy_sell_ratio)}"
    )


def discord_leading_field_value(snapshot: MarketSnapshot, signal: Signal | None) -> str:
    leading = leading_signal_score(snapshot, signal)
    if leading.leading_score <= 0 and not leading.leading_items:
        return "暂无明显领先信号"
    items = [f"- {item}" for item in leading.leading_items[:5] if item]
    if not items:
        items = ["- 暂无明显领先信号"]
    return discord_field_value(
        f"{leading.leading_score}/10 | {leading.leading_direction} | {leading.leading_label}\n"
        + "\n".join(items)
    )


def discord_evidence_field_value(snapshot: MarketSnapshot, signal: Signal | None) -> str:
    ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    positive_items = [
        item for item in ev_items
        if item.polarity == "positive" and item.source in {"OI", "FLOW", "FUNDING", "BASIS", "LSR", "WHALE", "SPOT", "ONCHAIN", "LIQ"}
    ][:6]
    prefix = "资金证据" if signal and topq_kind_normalized(signal.kind) in DISCORD_RISK_KINDS else "证据"
    lines = [f"{ev_score}/10 | {ev_direction} | {prefix}: {ev_summary or '暂无明确证据'}"]
    lines.extend(f"- {item.label} +{item.points}" for item in positive_items)
    return discord_field_value("\n".join(lines))


def discord_conflict_aware_summary(
    kind: str | None,
    direction: str,
    evidence_direction: str,
    evidence_score: int,
    evidence_summary: str | None,
    leading_direction: str,
    flow_label: str,
    risk_score: int,
    price_action: MultiTimeframePriceAction | None,
) -> str:
    normalized_kind = topq_kind_normalized(kind)
    summary = str(evidence_summary or "")
    bullish_evidence = (
        evidence_direction == "看多"
        or text_has_any(summary, ("主力建仓", "现货确认", "资金回流", "承接", "空头拥挤"))
        or leading_direction == "long"
    )
    risk_evidence = text_has_any(summary, ("高位拥挤", "注意出货", "不宜追", "谨慎追", "中长线资金不支持", "出货", "派发"))
    pa_direction = price_action.direction if price_action else "neutral"
    pa_bearish_score = 10 - price_action.score if price_action else 0

    if normalized_kind in DISCORD_RISK_KINDS and bullish_evidence:
        if pa_direction == "mixed":
            return "多空证据冲突，风险观察"
        if pa_bearish_score >= 6 or risk_score >= 5:
            return "顶部风险增强，等待跌破确认"
        return "风险触发，但资金/现货仍有支撑，等待K线确认"
    if normalized_kind in DISCORD_BULLISH_KINDS and risk_evidence:
        return "看多触发，但风险证据冲突，等待回踩确认"
    if normalized_kind in DISCORD_RISK_KINDS:
        return "风险观察" if abs(risk_score) < 5 else "顶部风险增强，等待跌破确认"
    if direction == "看多" and flow_label in TOPQ_WEAK_FLOW_LABELS:
        return "看多触发，但资金周期未共振，等待回踩确认"
    return summary or "观察，等待确认"


def discord_summary_is_conflict_override(summary: str | None, evidence_summary: str | None) -> bool:
    value = str(summary or "")
    raw = str(evidence_summary or "")
    return bool(value) and value != raw and text_has_any(
        value,
        (
            "风险触发",
            "顶部风险增强",
            "多空证据冲突",
            "看多触发",
        ),
    )


def discord_infer_evidence_direction(summary: str | None, fallback: str = "") -> str:
    value = str(summary or "")
    if text_has_any(value, ("主力建仓", "现货确认", "资金回流", "承接", "空头拥挤", "多周期共振流入")):
        return "看多"
    if text_has_any(value, ("高位拥挤", "注意出货", "顶部风险", "出货", "派发", "多头过热")):
        return "看空"
    return fallback or "中性"


def discord_risk_field_value(
    snapshot: MarketSnapshot,
    signal: Signal | None,
    price_action: MultiTimeframePriceAction | None,
) -> str:
    trap_score, trap_label, trap_reason = trap_risk_score(snapshot, signal)
    squeeze_label, squeeze_score, squeeze_reason = squeeze_state(snapshot)
    _basis_pct, basis_label, basis_reason = basis_state(snapshot)
    _ev_score, _ev_direction, _ev_summary, ev_items = evidence_score(snapshot, signal)
    risk_items = [item.label for item in ev_items if item.polarity == "risk"]
    if price_action:
        risk_items.extend(price_action.risk_items)
    risk_items = list(dict.fromkeys(risk_items))[:6]
    risk_header = f"风险 {trap_score}/10 {trap_label} | 挤压 {squeeze_label} {squeeze_score}/10 | 基差 {basis_label}"
    if is_risk_structure_kind(signal.kind if signal else None):
        bearish_score = (10 - (price_action.score if price_action else 5)) if price_action else 0
        if bearish_score >= 6 or len(risk_items) >= 4 or price_action_structure_risk_confirmed(price_action):
            risk_header = f"结构风险确认 | {risk_header}"
        elif abs(snapshot.price_change_percent) < 1 and abs(snapshot.oi_change_percent) < 2:
            risk_header = f"风险观察 | {risk_header}"
    if not risk_items:
        risk_items = ["暂无明显结构风险"]
    lines = [risk_header, f"{trap_reason}; {squeeze_reason}; {basis_reason}"]
    lines.extend(f"- {item}" for item in risk_items)
    return discord_field_value("\n".join(lines))


def discord_signal_action(
    snapshot: MarketSnapshot,
    signal: Signal | None,
    ev_summary: str,
    price_action: MultiTimeframePriceAction | None,
) -> tuple[str, str, bool]:
    action, action_reason = action_label(snapshot, signal)
    risk_signal = is_risk_structure_kind(signal.kind if signal else None)
    if price_action and not risk_signal and (
        price_action.long_score <= 3
        or text_has_any(price_action.label, ("大周期未确认", "压力位附近", "追高风险"))
        or any(text_has_any(item, ("压力位附近", "追高风险")) for item in price_action.risk_items)
    ):
        return MAIN_MOMENTUM_DOWNGRADE_TEXT, "展示降级：K线结构未确认，观察为主", True
    if signal and discord_bullish_display_downgrade(signal, ev_summary, price_action):
        return DISCORD_BULLISH_DOWNGRADE_TEXT, "展示降级：资金/结构未共振，观察为主", True
    if (
        signal
        and signal_direction_label(signal.kind) == "看多"
        and action.startswith("强烈建议关注买入")
        and not price_action_allows_discord_high_buy(price_action)
    ):
        return DISCORD_BULLISH_DOWNGRADE_TEXT, "展示降级：资金/结构未共振，观察为主", True
    return action, action_reason, False


def discord_price_action_field_value(price_action: MultiTimeframePriceAction | None) -> str:
    if price_action is None:
        price_action = MultiTimeframePriceAction(
            score=5,
            label="K线数据不足",
            direction="neutral",
            short_score=5,
            mid_score=5,
            long_score=5,
            short_label="短线数据不足",
            mid_label="中线数据不足",
            long_label="大周期数据不足",
            items=[],
            risk_items=["K线数据不足"],
            patterns=[],
            recommendation="K线数据不足，观察为主",
        )
    if price_action.label == "K线数据不足":
        return "暂无多周期K线确认\n仅资金/OI观察，不能按K线确认交易"
    lines = [
        f"K线 {price_action.score}/10 | {price_action.label}",
        f"结构：短{price_action.short_score} 中{price_action.mid_score} 长{price_action.long_score}",
    ]
    if price_action.patterns:
        lines.append("形态：")
        lines.extend(f"- {pattern}" for pattern in price_action.patterns[:5])
    else:
        for item in price_action.items[:3] + price_action.risk_items[:2]:
            lines.append(f"- {item}")
    return truncate_text("\n".join(lines), 900)


def discord_full_price_action_fields(price_action: MultiTimeframePriceAction | None) -> list[tuple[str, str, bool]]:
    if price_action is None:
        price_action = MultiTimeframePriceAction(
            score=5,
            label="K线数据不足",
            direction="neutral",
            short_score=5,
            mid_score=5,
            long_score=5,
            short_label="短线数据不足",
            mid_label="中线数据不足",
            long_label="大周期数据不足",
            items=[],
            risk_items=["K线数据不足"],
            patterns=[],
            recommendation="K线数据不足，观察为主",
        )
    summary = discord_price_action_field_value(price_action)
    pattern_text = "\n".join(f"- {pattern}" for pattern in price_action.patterns[:8]) if price_action.patterns else "暂无多周期K线确认"
    return [
        ("K线结构", summary, False),
        ("K线形态", truncate_text(pattern_text, 900), False),
        ("短线层", truncate_text("\n".join(price_action.items[:4] or [price_action.short_label]), 900), False),
        ("中线层", truncate_text("\n".join((price_action.items + price_action.risk_items)[4:8] or [price_action.mid_label]), 900), False),
        ("大周期层", truncate_text("\n".join(price_action.risk_items[:3] or [price_action.long_label]), 900), False),
    ]


def discord_signal_fields(
    signal: Signal,
    priority: str,
    quality_score: int,
    quality_reason: str,
) -> list[tuple[str, str, bool]]:
    snapshot = signal.snapshot
    direction = "风险/减仓" if is_risk_structure_kind(signal.kind) else signal_direction_label(signal.kind)
    if snapshot is None:
        evidence_name = "风险证据" if is_risk_structure_kind(signal.kind) else "证据"
        return [
            ("币种", signal.symbol, True),
            ("方向", direction, True),
            ("等级/把握", f"{priority} / 质量 {quality_score} / 信号 {signal.score}", True),
            (evidence_name, truncate_text(quality_reason or signal.message, 1024), False),
        ]

    conviction, conviction_label, _conviction_reason = conviction_score(snapshot, signal)
    ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    ev_display = evidence_display_score(ev_direction, ev_items, ev_score)
    price_action = safe_multi_timeframe_price_action(signal.symbol, log_result=True)
    action, action_reason, display_downgraded = discord_signal_action(snapshot, signal, ev_summary, price_action)
    leading = leading_signal_score(snapshot, signal)
    _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)
    conflict_summary = discord_conflict_aware_summary(
        signal.kind,
        direction,
        ev_direction,
        ev_score,
        ev_summary,
        leading.leading_direction,
        flow_label,
        trap_score,
        price_action,
    )
    conflict_override = discord_summary_is_conflict_override(conflict_summary, ev_summary)
    if conflict_override:
        normalized_kind = topq_kind_normalized(signal.kind)
        if normalized_kind in DISCORD_RISK_KINDS or normalized_kind in DISCORD_BULLISH_KINDS:
            action = conflict_summary
            action_reason = "Discord冲突口径"
            if normalized_kind in DISCORD_BULLISH_KINDS:
                display_downgraded = True
    if topq_kind_normalized(signal.kind) in DISCORD_RISK_KINDS and text_has_any(action, ("强烈建议关注买入", "启动把握高", "买入")):
        action = conflict_summary if conflict_summary else "风险观察，不追多"
        action_reason = "Discord风险口径"
    level_text = f"{priority} / 质量 {quality_score} / 把握 {conviction} {conviction_label}"
    if display_downgraded:
        level_text = f"{level_text}\n展示降级：资金/结构未共振，观察为主"
    fields = [
        ("币种/方向", discord_field_value(f"{signal.symbol} / {direction}"), True),
        ("等级/把握", discord_field_value(level_text), True),
        ("价格/OI", discord_price_oi_field_value(snapshot), False),
        ("衍生品状态", discord_derivatives_field_value(snapshot), False),
        ("资金流", discord_field_value(discord_flow_field_value(snapshot)), False),
        ("领先信号", discord_leading_field_value(snapshot, signal), False),
        ("证据", discord_evidence_field_value(snapshot, signal), False),
        ("风险提示", discord_risk_field_value(snapshot, signal, price_action), False),
        ("K线结构", discord_price_action_field_value(price_action), False),
        ("结论", discord_field_value(conflict_summary), False),
        ("操作建议", discord_field_value(f"{action}。{action_reason}"), False),
    ]
    return fields[:12]


def discord_signal_embed_v2(
    signal: Signal,
    priority: str,
    quality_score: int,
    quality_reason: str,
    channel_key: str,
) -> DiscordOutboundMessage:
    normalized_kind = topq_kind_normalized(signal.kind)
    if normalized_kind == "main_momentum_watch":
        return discord_main_momentum_watch_embed_v2(signal, priority, quality_score, quality_reason, channel_key)
    if normalized_kind in {"main_risk_watch", "top_risk", "top_exhaustion", "distribution"} or is_risk_structure_kind(signal.kind):
        return discord_main_risk_watch_embed_v2(signal, priority, quality_score, quality_reason, channel_key)
    return discord_realtime_signal_embed_v2(signal, priority, quality_score, quality_reason, channel_key)


def discord_main_momentum_watch_embed_v2(
    signal: Signal,
    priority: str,
    quality_score: int,
    quality_reason: str,
    channel_key: str,
) -> DiscordOutboundMessage:
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title=discord_signal_title(signal, priority),
        color=DISCORD_COLOR_WATCH,
        fields=discord_signal_fields(signal, priority, quality_score, quality_reason),
        symbol=signal.symbol,
        kind=signal.kind,
    )


def discord_main_risk_watch_embed_v2(
    signal: Signal,
    priority: str,
    quality_score: int,
    quality_reason: str,
    channel_key: str,
) -> DiscordOutboundMessage:
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title=discord_signal_title(signal, priority),
        color=DISCORD_COLOR_RISK,
        fields=discord_signal_fields(signal, priority, quality_score, quality_reason),
        symbol=signal.symbol,
        kind=signal.kind,
    )


def discord_realtime_signal_embed_v2(
    signal: Signal,
    priority: str,
    quality_score: int,
    quality_reason: str,
    channel_key: str,
) -> DiscordOutboundMessage:
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title=discord_signal_title(signal, priority),
        color=discord_signal_color(signal),
        fields=discord_signal_fields(signal, priority, quality_score, quality_reason),
        symbol=signal.symbol,
        kind=signal.kind,
    )


def alt_watch_price_action_line(price_action: MultiTimeframePriceAction | None) -> str:
    if price_action is None or price_action.label == "K线数据不足":
        return "K线：暂无多周期确认"
    patterns = [compact_price_action_pattern(pattern) for pattern in price_action.patterns[:2] if pattern]
    if patterns:
        return "K线：" + "；".join(patterns)
    if price_action.short_score >= 7 and price_action.long_score <= 3:
        return "K线：短线强，4h/日线未确认"
    if price_action.short_score >= 7 and price_action.mid_score < 6:
        return "K线：短线强，中线未确认"
    if price_action.mid_score <= 4 or price_action.long_score <= 4:
        return "K线：短线异动，中长周期未确认"
    if text_has_any(price_action.label, ("震荡", "分歧")):
        return "K线：箱体震荡，方向未确认"
    return f"K线：{price_action.label}"


def compact_price_action_pattern(pattern: str) -> str:
    value = str(pattern or "").strip()
    replacements = {
        "连续收盘抬高": "连续抬高",
        "突破近20根高点": "突破20根高点",
        "跌破近20根低点": "跌破20根低点",
        "接近日线压力": "日线压力",
        "接近周线压力": "周线压力",
    }
    for old, new in replacements.items():
        value = value.replace(old, new)
    return value.replace(" ", "")


def discord_alt_watch_embed_v2(
    items: list[DiscordAltWatchItem],
    channel_key: str,
    title: str = "山寨观察",
    snapshots: dict[str, MarketSnapshot] | None = None,
) -> DiscordOutboundMessage:
    fields: list[tuple[str, str, bool]] = []
    snapshots = snapshots or {}
    for item in items[:10]:
        snapshot = snapshots.get(item.symbol)
        price_action = light_multi_timeframe_price_action(item.symbol)
        if snapshot:
            basis_pct, basis_label, _basis_reason = basis_state(snapshot)
            basis_text = f"{basis_pct:+.2f}% {basis_label}" if basis_pct is not None else basis_label
            funding_text = format_realtime_funding(snapshot.funding_rate_percent)
            flow_15m = format_usd(summary_flow_value(snapshot, "15m"))
            flow_1h = format_usd(summary_flow_value(snapshot, "1h"))
            flow_4h = format_usd(summary_flow_value(snapshot, "4h"))
            short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
        else:
            basis_text = "n/a"
            funding_text = "n/a"
            flow_15m = flow_1h = flow_4h = "n/a"
            short_flow = mid_flow = long_flow = "n/a"
            flow_label = item.flow_label
        direction = signal_direction_label(item.kind)
        evidence_direction = discord_infer_evidence_direction(item.reason, direction)
        leading_direction = "long" if evidence_direction == "看多" and item.leading_score > 0 else ""
        conflict_summary = discord_conflict_aware_summary(
            item.kind,
            direction,
            evidence_direction,
            item.evidence_score,
            item.reason,
            leading_direction,
            flow_label,
            item.trap_score,
            price_action,
        )
        conclusion = conflict_summary
        if price_action.long_score <= 3 or text_has_any(price_action.label, ("大周期未确认", "追高风险", "压力位附近")):
            if not discord_summary_is_conflict_override(conflict_summary, item.reason):
                conclusion = "不追高，等待回踩/放量确认"
        elif item.trap_score >= 6 or text_has_any(item.reason, ("风险", "出货", "顶部", "派发")):
            if not discord_summary_is_conflict_override(conflict_summary, item.reason):
                conclusion = "风险观察"
        elif item.flow_label in TOPQ_WEAK_FLOW_LABELS:
            if not discord_summary_is_conflict_override(conflict_summary, item.reason):
                conclusion = "等回踩，观察"
        kline_line = alt_watch_price_action_line(price_action)
        fields.append(
            (
                f"🟡 山寨观察 {discord_symbol_pair(item.symbol)}",
                discord_field_value(
                    f"{item.symbol} {signal_kind_label(item.kind)} | 把握{item.conviction_score} 质量{item.quality_score} | "
                    f"领先{item.leading_score} 证据{item.evidence_score} 风险{item.trap_score}\n"
                    f"价格 {format_percent_optional(item.price_change_percent)} | OI {format_percent_optional(item.oi_change_percent)} | 费率 {funding_text} | 基差 {basis_text}\n"
                    f"资金 15m {flow_15m} / 1h {flow_1h} / 4h {flow_4h} | 短{short_flow}/中{mid_flow}/长{long_flow} | {flow_label}\n"
                    f"{kline_line}\n"
                    f"结论：{conclusion}"
                ),
                False,
            )
        )
    fields.append(
        (
            "说明",
            "山寨观察仅作预警，需等待资金、K线和风控共振。",
            False,
        )
    )
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title=f"🟡 {title}",
        color=DISCORD_COLOR_WATCH,
        fields=fields or [("状态", "当前暂无山寨观察候选。", False)],
        kind="alt_watch",
    )


def discord_chunk_fields(text: str, field_prefix: str = "内容", chunk_size: int = 800) -> list[tuple[str, str, bool]]:
    lines = [line.strip() for line in str(text or "").splitlines() if line.strip()]
    chunks: list[str] = []
    current = ""
    for line in lines:
        candidate = f"{current}\n{line}".strip() if current else line
        if len(candidate) > chunk_size and current:
            chunks.append(current)
            current = line
        else:
            current = candidate
    if current:
        chunks.append(current)
    return [(f"{field_prefix}{index}", chunk, False) for index, chunk in enumerate(chunks[:6], start=1)]


def discord_external_data_text(text: str | None) -> str:
    value = str(text or "")
    replacements = (
        ("链上资金摘要", "外部资金确认摘要"),
        ("链上资金", "外部资金确认"),
        ("链上雷达", "外部资金雷达"),
        ("现货/链上确认", "现货/DEX/外部确认"),
        ("现货/链上同步确认", "现货/DEX/外部同步确认"),
        ("现货/链上出货", "现货/DEX/外部转弱"),
        ("现货/链上转弱", "现货/DEX/外部转弱"),
        ("现货/链上承接", "现货/DEX/外部承接"),
        ("现货/链上未充分确认", "现货/DEX/外部确认不足"),
        ("现货/链上与合约", "现货/DEX与合约"),
        ("现货/链上", "现货/DEX/外部确认"),
        ("链上承接", "现货/DEX承接"),
        ("链上出货", "现货/DEX出货"),
        ("链上数据不足", "外部资金数据不足"),
        ("链上有支撑", "现货/DEX有支撑"),
        ("链上流动性", "DEX流动性"),
        ("链上DEX", "DEX确认"),
    )
    for old, new in replacements:
        value = value.replace(old, new)
    return value


def yes_no(value: bool) -> str:
    return "是" if value else "否"


def format_count_mapping(values: dict[str, int], limit: int = 8) -> str:
    if not values:
        return "-"
    parts = [f"{key}={count}" for key, count in sorted(values.items(), key=lambda item: (-item[1], item[0]))[:limit]]
    return " / ".join(parts)


def short_address(address: str) -> str:
    value = str(address or "").strip()
    if len(value) <= 16:
        return value
    return f"{value[:8]}...{value[-6:]}"


def normalize_onchain_address(chain: str, address: str) -> str:
    value = str(address or "").strip()
    if str(chain or "").lower() in {"ethereum", "tron"}:
        return value.lower()
    return value


def is_zero_onchain_address(address: str) -> bool:
    value = str(address or "").strip().lower()
    return value in {
        "",
        "0x0000000000000000000000000000000000000000",
        "0000000000000000000000000000000000000000",
        "11111111111111111111111111111111",
    }


def is_placeholder_onchain_address(address: str) -> bool:
    value = str(address or "").strip()
    lower = value.lower()
    if not value or "..." in value or "placeholder" in lower or lower in {"todo", "tbd", "0x"}:
        return True
    if lower.startswith("0x") and len(value) < 20:
        return True
    if value.startswith("T") and len(value) < 20:
        return True
    return False


def format_ts_short(timestamp: float | None) -> str:
    if not timestamp:
        return "-"
    return dt.datetime.fromtimestamp(float(timestamp)).strftime("%m-%d %H:%M")


def stablecoin_supply_changes(rows: list[Any]) -> dict[str, float | None]:
    if not rows:
        return {"24h": None, "7d": None, "30d": None}
    latest_supply = parse_float(rows[0][0])
    latest_ts = parse_float(rows[0][2])
    changes: dict[str, float | None] = {}
    for label, seconds in (("24h", 86400), ("7d", 7 * 86400), ("30d", 30 * 86400)):
        changes[label] = None
        if latest_supply is None or latest_ts is None:
            continue
        target_ts = latest_ts - seconds
        older = None
        for row in rows:
            row_supply = parse_float(row[0])
            row_ts = parse_float(row[2])
            if row_supply is None or row_ts is None:
                continue
            if row_ts <= target_ts:
                older = row_supply
                break
        if older is not None:
            changes[label] = percent_change(older, latest_supply) if older else None
    return changes


def parse_json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        payload = json.loads(str(value))
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def format_usd_plain(value: float | None) -> str:
    if value is None:
        return "n/a"
    abs_value = abs(float(value))
    if abs_value >= 1_000_000_000:
        return f"{abs_value / 1_000_000_000:.2f}B"
    if abs_value >= 1_000_000:
        return f"{abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{abs_value / 1_000:.1f}K"
    return f"{abs_value:.0f}"


def stablecoin_supply_changes_from_raw(raw: dict[str, Any], rows: list[Any]) -> dict[str, float | None]:
    current = parse_float((raw.get("circulating") or {}).get("peggedUSD")) if isinstance(raw.get("circulating"), dict) else None
    if current is None and rows:
        current = parse_float(rows[0][0])
    raw_keys = {
        "24h": "circulatingPrevDay",
        "7d": "circulatingPrevWeek",
        "30d": "circulatingPrevMonth",
    }
    changes: dict[str, float | None] = {"24h": None, "7d": None, "30d": None}
    for label, key in raw_keys.items():
        prev_obj = raw.get(key)
        prev = parse_float(prev_obj.get("peggedUSD")) if isinstance(prev_obj, dict) else parse_float(prev_obj)
        if current is not None and prev:
            changes[label] = percent_change(prev, current)
    historical = stablecoin_supply_changes(rows)
    for label, value in historical.items():
        if changes.get(label) is None:
            changes[label] = value
    return changes


def stablecoin_chain_distribution(raw: dict[str, Any]) -> list[tuple[str, float]]:
    chain_values = raw.get("chainCirculating")
    if not isinstance(chain_values, dict):
        return []
    rows: list[tuple[str, float]] = []
    for chain, payload in chain_values.items():
        if not isinstance(payload, dict):
            continue
        current = payload.get("current")
        value = parse_float(current.get("peggedUSD")) if isinstance(current, dict) else parse_float(payload.get("peggedUSD"))
        if value is not None and value > 0:
            rows.append((str(chain), value))
    return sorted(rows, key=lambda item: item[1], reverse=True)


def stablecoin_chain_distribution_text(chain_top: list[tuple[str, float]], limit: int = 5) -> str:
    if not chain_top:
        return "n/a"
    total = sum(value for _chain, value in chain_top)
    parts = []
    for chain, value in chain_top[:limit]:
        share = (value / total * 100) if total else None
        share_text = f" {share:.1f}%" if share is not None else ""
        parts.append(f"{chain} {format_usd_plain(value)}{share_text}")
    return " / ".join(parts)


def defillama_chain_metric_suffix(chain_name: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "", str(chain_name or "").strip().lower())
    aliases = {
        "ethereum": "ethereum",
        "eth": "ethereum",
        "tron": "tron",
        "solana": "solana",
        "bsc": "bsc",
        "binancesmartchain": "bsc",
        "bnbchain": "bsc",
        "arbitrum": "arbitrum",
        "arbitrumone": "arbitrum",
        "base": "base",
    }
    return aliases.get(normalized, normalized)


def stablecoin_liquidity_conclusion(changes: dict[str, float | None]) -> str:
    day = parse_float(changes.get("24h"))
    week = parse_float(changes.get("7d"))
    values = [value for value in (day, week) if value is not None]
    if not values:
        return "稳定币供应数据不足，等待下次采集"
    if any(value >= 0.15 for value in values):
        return "稳定币供应扩张，潜在流动性增加，但需观察是否进入交易所"
    if any(value <= -0.15 for value in values):
        return "稳定币供应收缩，场外流动性边际转弱"
    return "稳定币供应变化不大，流动性中性"


def stablecoin_conclusion_short(conclusion: str) -> str:
    if "扩张" in conclusion:
        return "扩张"
    if "收缩" in conclusion:
        return "收缩"
    if "数据不足" in conclusion:
        return "数据不足"
    return "中性"


def discord_summary_embed_v2(title: str, message: str, channel_key: str = "summary") -> DiscordOutboundMessage:
    text = discord_clean_summary_text(message, title=title, remove_title=True)
    fields = discord_chunk_fields(text, "摘要")
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title=title,
        color=DISCORD_COLOR_SUMMARY,
        fields=fields or [("状态", "当前暂无摘要缓存。", False)],
        kind="summary",
    )


def discord_topq_embed_v2(message: str, channel_key: str = "digest") -> DiscordOutboundMessage:
    fields = discord_chunk_fields(message, "候选")
    kline_lines = [line for line in str(message or "").splitlines() if "K线" in line][:6]
    kline_text = "\n".join(kline_lines) if kline_lines else "候选已接入多周期K线结构确认；K线弱或大周期未确认时只显示观察。"
    fields.append(("K线结构", truncate_text(kline_text, 900), False))
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title="把握候选TOP10",
        color=DISCORD_COLOR_SUMMARY,
        fields=fields[:10],
        kind="topq",
    )


def discord_suppressed_digest_embed_v2(
    items: list[DiscordSuppressedDigestItem],
    channel_key: str = "digest",
) -> DiscordOutboundMessage:
    display_items = items[:12]
    lines = ["过去15分钟被实时过滤的信号"]
    for item in display_items:
        lines.append(
            f"{item.symbol} {signal_kind_label(item.kind)} | q={item.priority}/{item.quality} | "
            f"把握{item.conviction} | reason={item.reason}"
        )
        lines.append(
            f"{format_percent_optional(item.price_change)} / OI{format_percent_optional(item.oi_change)} | "
            f"{item.flow_label} | {item.evidence_summary or '-'}"
        )
    lines.append(f"共 {len(items)} 条，展示前 {len(display_items)} 条。")
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title="🧾 静默信号摘要",
        color=DISCORD_COLOR_SUMMARY,
        fields=discord_chunk_fields("\n".join(lines), "静默"),
        kind="suppressed_digest",
    )


def discord_onchain_embed_v2(title: str, message: str, channel_key: str = "onchain") -> DiscordOutboundMessage:
    text = discord_clean_summary_text(discord_external_data_text(message), title=title, remove_title=True)
    fields = discord_chunk_fields(text, "外部资金")
    return DiscordOutboundMessage(
        channel_key=channel_key,
        title=discord_external_data_text(title),
        color=DISCORD_COLOR_SUMMARY,
        fields=fields or [("状态", "数据不足，等待外部资金缓存", False)],
        kind="onchain",
    )


def discord_diagnosis_embed_v2(
    symbol: str,
    snapshot: MarketSnapshot,
    signals: list[Signal],
    liquidation_text: str | None,
    coinglass_text: str | None,
    response_parts: list[str],
) -> DiscordOutboundMessage:
    signal = signals[0] if signals else None
    if signal is None:
        signal = Signal(symbol=symbol, kind="diagnosis", score=0, title=f"{symbol} 单币诊断", message="", key=f"{symbol}:diagnosis", snapshot=snapshot)
    fields = discord_signal_fields(signal, "-", 0, "诊断")
    diagnosis_price_action = safe_multi_timeframe_price_action(symbol, log_result=True)
    if diagnosis_price_action:
        fields.extend(discord_full_price_action_fields(diagnosis_price_action)[1:])
    source = response_parts[0] if response_parts else ""
    if source:
        fields.append(("数据来源", source, False))
    if liquidation_text:
        fields.append(("强平", truncate_text(liquidation_text, 500), False))
    if coinglass_text:
        fields.append(("CoinGlass", truncate_text(compact_coinglass_market_context(coinglass_text), 900), False))
    return DiscordOutboundMessage(
        channel_key="debug",
        title=f"🟡 诊断 {discord_symbol_pair(symbol)}",
        color=DISCORD_COLOR_WATCH,
        fields=fields[:20],
        symbol=symbol,
        kind="diagnosis",
    )


def discord_help_text() -> str:
    return (
        "Discord 中文命令:\n"
        "!摘要 - 返回缓存市场摘要，不触发全市场实时生成\n"
        "!候选 - 等同 /topq，把握候选排行\n"
        "!山寨 - 查看最近山寨观察队列 Top10\n"
        "!数据源 - 查看采集层数据源健康状态\n"
        "!采集统计 - 查看外部数据最近24h采集数量\n"
        "!外部资金来源 - 查看当前真实可用外部资金来源\n"
        "!外部资金总览 / !资金面 - 查看外部资金驾驶舱\n"
        "!地址源 - 查看自建链上地址标签库状态\n"
        "!地址候选 - 查看交易所/treasury 地址候选审计\n"
        "!地址 USDT - 查询资产或实体相关地址标签\n"
        "!链上事件 - 查看最近24h已标记地址链上转账事件\n"
        "!稳定币 - 查看 DefiLlama 稳定币供应聚合变化\n"
        "!coinglass BTCUSDT - 查看 CoinGlass 聚合外部资金面板\n"
        "!外部资金 - 查看 BTC/ETH/SOL/BNB/DOGE 现货/DEX/CoinGlass 外部资金确认\n"
        "!外部资金 BTCUSDT - 单币现货/DEX/CoinGlass 外部资金确认\n"
        "!链上 BTCUSDT - 旧别名，当前不代表真实钱包流\n"
        "!链上摘要 - 旧别名，当前不代表真实钱包流\n"
        "!诊断 BTCUSDT - 单币诊断简版\n"
        "!质量 - 信号质量统计\n"
        "!静默 - 等同 /quiet status\n"
        "!静默发送 - 立即发送 Discord 静默摘要队列\n"
        "!测试推送 - 向各已配置频道发送测试消息\n"
        "!帮助 - 查看命令"
    )



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


def metric_value(metrics: dict[str, Any], key: str) -> float | None:
    value = metrics.get(key)
    if isinstance(value, tuple):
        return parse_float(value[0])
    return parse_float(value)


def format_coinglass_balance_snapshot(row: Any) -> str:
    if not isinstance(row, dict):
        return "n/a"
    change = parse_float(row.get("change"))
    change_percent = parse_float(row.get("change_percent"))
    if change is None and change_percent is None:
        return "n/a"
    parts = []
    if change is not None:
        parts.append(format_usd(change))
    if change_percent is not None:
        parts.append(format_percent_optional(change_percent))
    return " ".join(parts) if parts else "n/a"


def coinglass_panel_judgement(metrics: dict[str, Any], balances: dict[str, Any]) -> str:
    balance_1d = parse_float((balances.get("24h") or {}).get("change"))
    balance_7d = parse_float((balances.get("7d") or {}).get("change"))
    balance_30d = parse_float((balances.get("30d") or {}).get("change"))
    buy_ratio = metric_value(metrics, "taker_flow_buy_ratio")
    sell_ratio = metric_value(metrics, "taker_flow_sell_ratio")
    bid_ask_ratio = metric_value(metrics, "orderbook_bid_ask_ratio")
    oi_1h = metric_value(metrics, "open_interest_change_1h")
    oi_4h = metric_value(metrics, "open_interest_change_4h")
    oi_24h = metric_value(metrics, "open_interest_change_24h")
    funding_current = metric_value(metrics, "funding_oi_weight")
    funding_1d = metric_value(metrics, "funding_accumulated_24h")
    funding_7d = metric_value(metrics, "funding_accumulated_7d")

    balance_down = any(value is not None and value < 0 for value in (balance_1d, balance_7d, balance_30d))
    balance_up = any(value is not None and value > 0 for value in (balance_1d, balance_7d))
    buy_support = ratio_gte(buy_ratio, 52) or (bid_ask_ratio is not None and bid_ask_ratio >= 1.15)
    sell_pressure = ratio_gte(sell_ratio, 52) or (bid_ask_ratio is not None and bid_ask_ratio <= 0.85)
    oi_rising = any(value is not None and value > 0 for value in (oi_1h, oi_4h, oi_24h))
    funding_extreme = any(value is not None and abs(value) >= 0.08 for value in (funding_current, funding_1d, funding_7d))

    if funding_extreme and oi_rising:
        return "杠杆拥挤，需防波动放大"
    if balance_down and buy_support and not sell_pressure:
        return "外部资金偏支撑"
    if balance_up and sell_pressure:
        return "交易所余额回流且卖压增强，潜在抛压"
    if (balance_down and sell_pressure) or (balance_up and buy_support):
        return "CoinGlass 数据分歧，暂不构成强确认"
    if not metrics and not balances:
        return "CoinGlass 数据不足，等待缓存"
    return "CoinGlass 聚合数据中性/分歧"


def coinglass_judgement_short(judgement: str) -> str:
    if "数据不足" in judgement:
        return "数据不足"
    if "支撑" in judgement:
        return "支撑"
    if any(keyword in judgement for keyword in ("抛压", "风险", "拥挤")):
        return "抛压"
    if "分歧" in judgement:
        return "分歧"
    return "分歧"


def ratio_gte(value: float | None, threshold_percent: float) -> bool:
    if value is None:
        return False
    threshold = threshold_percent / 100 if abs(value) <= 1 else threshold_percent
    return value >= threshold


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
    balance_label = "CoinGlass交易所余额"
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
    balance_text = extract_labeled_segment(coinglass_text, "交易所余额", "；") or "CoinGlass交易所余额 n/a"
    if balance_text.startswith("交易所余额"):
        balance_text = f"CoinGlass{balance_text}"
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
    balance_text = extract_labeled_segment(coinglass_text, "交易所余额", "；") or "CoinGlass交易所余额 n/a"
    if balance_text.startswith("交易所余额"):
        balance_text = f"CoinGlass{balance_text}"
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


def format_onchain_brief(
    snapshot: MarketSnapshot,
    coinglass_text: str | None = None,
    spot_text: str | None = None,
    compact: bool = False,
) -> str:
    resolved_spot_text = spot_text if spot_text is not None else (
        cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    )
    spot_score, spot_label, spot_reason = spot_onchain_score_from_text(snapshot, None, resolved_spot_text)
    div_label, div_score, div_reason = contract_spot_divergence_from_text(snapshot, None, resolved_spot_text)
    cg_text = str(coinglass_text or "")
    balance_text = extract_labeled_segment(cg_text, "交易所余额", "；") or "CoinGlass交易所余额: 1d n/a / 7d n/a / 30d n/a"
    if balance_text.startswith("交易所余额"):
        balance_text = f"CoinGlass{balance_text}"
    taker_24h = extract_labeled_segment(cg_text, "主动买卖 24h", "；")
    taker_7d = extract_labeled_segment(cg_text, "CoinGlass主动买卖 7d", "；")
    orderbook_text = extract_labeled_segment(cg_text, "CoinGlass订单簿: ", "\n")
    conclusion = onchain_conclusion(snapshot, spot_score, div_score, cg_text)
    source_text = external_confirmation_source_text(resolved_spot_text, cg_text)

    if compact:
        coinglass_parts = "；".join(part for part in (balance_text, taker_24h, taker_7d, orderbook_text) if part)
        return discord_external_data_text(
            f"{snapshot.symbol}: {balance_text} | 现货/DEX确认{spot_score}/10 {spot_label} | "
            f"{div_label} | {coinglass_parts or 'CoinGlass: n/a'} | 结论: {conclusion}"
        )

    lines = [
        f"币种: {snapshot.symbol}",
        "当前无钱包级净流数据，以下为 CoinGlass聚合/现货DEX确认",
        f"{balance_text}",
        f"现货/DEX确认: {spot_score}/10 {spot_label} - {spot_reason}",
        f"合约现货背离: {div_label} {div_score}/10 - {div_reason}",
        f"CoinGlass主动买卖: {' / '.join(part for part in (taker_24h, taker_7d) if part) or 'n/a'}",
        f"CoinGlass订单簿: {orderbook_text or 'n/a'}",
        source_text,
        f"结论: {conclusion}",
    ]
    return discord_external_data_text("\n".join(lines))


def external_confirmation_source_text(spot_text: str | None, coinglass_text: str | None) -> str:
    sources: list[str] = []
    text = str(spot_text or "")
    cg_text = str(coinglass_text or "")
    if "标准现货" in text:
        sources.append("Binance现货")
    if "链上DEX" in text or "DEX确认" in text:
        sources.append("DexScreener")
    if cg_text and "CoinGlass聚合: n/a" not in cg_text:
        sources.append("CoinGlass聚合")
    return "数据源：" + " / ".join(sources or ["数据不足"])


def onchain_conclusion(snapshot: MarketSnapshot, spot_score: int, divergence_score: int, coinglass_text: str | None) -> str:
    text = str(coinglass_text or "")
    balance_1d = extract_coinglass_balance_change(coinglass_text, "1d")
    balance_7d = extract_coinglass_balance_change(coinglass_text, "7d")
    balance_30d = extract_coinglass_balance_change(coinglass_text, "30d")
    buy_ratio, sell_ratio = extract_coinglass_taker_ratios(coinglass_text)
    orderbook_text = extract_labeled_segment(text, "CoinGlass订单簿: ", "\n") or ""
    balances = [value for value in (balance_1d, balance_7d, balance_30d) if value is not None]
    has_balance = bool(balances)
    has_taker = buy_ratio is not None or sell_ratio is not None
    has_orderbook = bool(orderbook_text and "n/a" not in orderbook_text)
    has_spot = spot_score != 5
    has_divergence = divergence_score > 0
    valid_categories = sum([has_balance, has_taker, has_orderbook, has_spot, has_divergence])
    if valid_categories < 2:
        return "数据不足"

    bullish: list[str] = []
    risk: list[str] = []
    neutral: list[str] = []

    if balance_30d is not None and balance_30d < 0 and "下方承接偏强" in orderbook_text:
        bullish.append("30d交易所余额下降、订单簿下方承接偏强")
    if any(value is not None and value > 0 for value in (balance_1d, balance_7d)):
        risk.append("1d/7d交易所余额回流")
    if buy_ratio is not None and buy_ratio > 52:
        bullish.append("主动买盘略占优")
    elif sell_ratio is not None and sell_ratio > 52:
        risk.append("主动卖压略占优")
    elif has_taker:
        neutral.append("主动买卖接近中性")
    if spot_score >= 7:
        bullish.append("现货/DEX确认较强")
    elif spot_score <= 3:
        risk.append("现货/DEX确认偏弱")
    elif has_spot:
        neutral.append("现货/DEX中性")
    if divergence_score >= 5:
        risk.append("合约现货存在背离，降低确认度")

    if bullish and risk:
        if balance_30d is not None and balance_30d < 0 and "下方承接偏强" in orderbook_text:
            return "外部资金中性偏支撑；" + "、".join(bullish[:2]) + "，但" + "、".join(risk[:2]) + "，暂不构成强确认"
        return "外部资金分歧，暂不构成强确认；" + "；".join((bullish + risk)[:4])
    if risk:
        if any("余额回流" in item for item in risk):
            return "交易所余额回流，潜在抛压上升，追多需谨慎"
        return "外部资金偏风险；" + "、".join(risk[:3])
    if bullish:
        if balance_30d is not None and balance_30d < 0 and "下方承接偏强" in orderbook_text:
            return "外部资金中性偏支撑，交易所余额长期下降，短线承接较强"
        return "外部资金中性偏支撑；" + "、".join(bullish[:3])
    return "外部资金中性观察；" + "、".join(neutral[:3] or ["多类数据未形成方向确认"])


def onchain_brief_has_confirmation_data(text: str) -> bool:
    normalized = discord_external_data_text(text)
    if "CoinGlass交易所余额: 1d n/a / 7d n/a / 30d n/a" not in normalized:
        return True
    if "CoinGlass主动买卖" in normalized or "CoinGlass订单簿" in normalized or "近1h 买盘" in normalized:
        return True
    return "现货/DEX确认5/10 中性" not in normalized and "暂无明显现货/DEX/外部确认" not in normalized


def spot_onchain_score(snapshot: MarketSnapshot | None, signal: Signal | None = None) -> tuple[int, str, str]:
    if snapshot is None:
        return 5, "中性", "无快照"
    spot_text = cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    return spot_onchain_score_from_text(snapshot, signal, spot_text)


def spot_onchain_score_from_text(
    snapshot: MarketSnapshot | None,
    signal: Signal | None,
    spot_text: str | None,
) -> tuple[int, str, str]:
    text = str(spot_text or "")
    score = 5
    reasons: list[str] = []

    def add(points: int, reason: str) -> None:
        nonlocal score
        score += points
        reasons.append(reason)

    spot_15m = spot_period_state(text, "15m")
    spot_1h = spot_period_state(text, "1h")
    spot_4h = spot_period_state(text, "4h")
    dex_1h = dex_period_state(text, "1h")
    dex_24h = dex_period_state(text, "24h")
    dex_1h_change = dex_period_change(text, "1h")
    dex_24h_change = dex_period_change(text, "24h")
    dex_vol_1h = dex_volume_usd(text, "1h")
    dex_vol_24h = dex_volume_usd(text, "24h")
    liquidity = dex_liquidity_usd(text)
    volume_ratio = snapshot.volume_ratio_24h if snapshot else None
    price_not_weak = snapshot is None or snapshot.price_change_percent >= -0.3

    if spot_15m == "偏强" or spot_1h == "偏强":
        add(2, "标准现货短周期偏强")
    if spot_4h == "偏强":
        add(2, "标准现货4h偏强")
    if dex_1h == "偏强":
        add(2, "DEX 1h偏强")
    if dex_24h == "偏强" and dex_volume_expanded(dex_vol_24h):
        add(2, "DEX 24h偏强且成交放大")
    if ((volume_ratio is not None and volume_ratio >= 1.2) or dex_volume_expanded(dex_vol_1h) or dex_volume_expanded(dex_vol_24h)) and price_not_weak:
        add(1, "现货/DEX成交放大")
    if liquidity is not None and liquidity >= 100000:
        add(1, "流动性充足")

    if spot_15m == "偏弱" or spot_1h == "偏弱":
        add(-2, "标准现货短周期偏弱")
    if spot_4h == "偏弱":
        add(-2, "标准现货4h偏弱")
    if dex_1h == "偏弱":
        add(-2, "DEX 1h偏弱")
    if dex_24h_change is not None and dex_24h_change >= 15 and dex_1h_change is not None and dex_1h_change <= -1:
        add(-2, "高位DEX转弱")
    if "无标准现货/高流动性DEX数据" in text or (liquidity is not None and liquidity < 50000):
        add(-1, "流动性过低或缺失")

    score = max(0, min(10, int(round(score))))
    label = "弱" if score <= 3 else "强" if score >= 7 else "中性"
    reason = "；".join(dict.fromkeys(reasons)) if reasons else "暂无明显现货/DEX/外部确认"
    return score, label, truncate_text(reason, 120)


def contract_spot_divergence(snapshot: MarketSnapshot | None, signal: Signal | None = None) -> tuple[str, int, str]:
    if snapshot is None:
        return "无背离", 0, "无快照"
    spot_text = cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    return contract_spot_divergence_from_text(snapshot, signal, spot_text)


def contract_spot_divergence_from_text(
    snapshot: MarketSnapshot,
    signal: Signal | None,
    spot_text: str | None,
) -> tuple[str, int, str]:
    score = 0
    reasons: list[str] = []
    spot_score, _spot_label, _spot_reason = spot_onchain_score_from_text(snapshot, signal, spot_text)
    text = str(spot_text or "")
    weak_spot = spot_score <= 3
    dex_1h = dex_period_state(text, "1h")
    spot_1h = spot_period_state(text, "1h")
    spot_15m = spot_period_state(text, "15m")
    spot_turning_weak = dex_1h == "偏弱" or spot_1h == "偏弱" or spot_15m == "偏弱"

    def add(points: int, reason: str) -> None:
        nonlocal score
        score += points
        reasons.append(reason)

    if snapshot.price_change_percent > 0 and snapshot.oi_change_percent > 0 and weak_spot:
        add(3, "合约拉盘未确认")
    if signal and signal.kind in ("discovery", "hot_breakout") and weak_spot:
        add(3, "突破缺现货确认")
    if snapshot.price_position_24h is not None and snapshot.price_position_24h > 75 and spot_turning_weak:
        add(2, "高位现货转弱")
    if flow_alignment_score(snapshot) >= 7 and weak_spot:
        add(2, "合约现货背离")
    if snapshot.oi_change_percent > 10 and snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio < 1:
        add(2, "增仓但主动买盘弱")

    score = max(0, min(10, score))
    if score >= 8:
        label = "严重背离"
    elif score >= 6:
        label = "明显背离"
    elif score >= 3:
        label = "轻微背离"
    else:
        label = "无背离"
    reason = "；".join(dict.fromkeys(reasons)) if reasons else "现货/DEX与合约暂无明显冲突"
    return label, score, truncate_text(reason, 120)


def major_flow_score(snapshot: MarketSnapshot | None, signal: Signal | None = None) -> tuple[int, str, str]:
    if snapshot is None:
        return 5, "数据不足", "无快照"
    return major_flow_score_from_text(snapshot, signal, cached_coinglass_market_context_text(snapshot.symbol))


def major_flow_score_from_text(
    snapshot: MarketSnapshot,
    signal: Signal | None,
    coinglass_text: str | None,
) -> tuple[int, str, str]:
    text = str(coinglass_text or "")
    oi_24h = extract_coinglass_oi_change(text, "24h")
    buy_ratio, sell_ratio = extract_coinglass_taker_ratios(text)
    balance_1d = extract_coinglass_balance_change(text, "1d")
    balance_7d = extract_coinglass_balance_change(text, "7d")
    funding_1d = extract_coinglass_funding_accumulated(text, "1d")
    funding_7d = extract_coinglass_funding_accumulated(text, "7d")
    key_values = [oi_24h, buy_ratio, sell_ratio, balance_1d, balance_7d, funding_1d, funding_7d]
    if not any(value is not None for value in key_values):
        return 5, "数据不足", "CoinGlass长周期数据不足"

    raw = 0
    reasons: list[str] = []

    def add(points: int, reason: str) -> None:
        nonlocal raw
        raw += points
        reasons.append(reason)

    if oi_24h is not None and oi_24h > 0 and buy_ratio is not None and buy_ratio > 51:
        add(2, "24h增仓且主动买入占优")
    if oi_24h is not None and oi_24h > 0 and sell_ratio is not None and sell_ratio > 51:
        add(-2, "增仓偏卖")
    if any(value is not None and value < 0 for value in (balance_1d, balance_7d)):
        add(2, "交易所余额下降")
    if any(value is not None and value > 0 for value in (balance_1d, balance_7d)):
        add(-2, "交易所余额上升")
    if summary_flow_value(snapshot, "12h") > 0 or summary_flow_value(snapshot, "24h") > 0:
        add(2, "12h/24h合约资金流为正")
    if summary_flow_value(snapshot, "12h") < 0 or summary_flow_value(snapshot, "24h") < 0:
        add(-2, "12h/24h合约资金流为负")
    if not funding_accumulated_extreme(funding_1d) and not funding_accumulated_extreme(funding_7d):
        add(1, "Funding累计不极端")
    if funding_accumulated_extreme(funding_1d) or funding_accumulated_extreme(funding_7d):
        add(-1, "费率拥挤")

    spot_score, _spot_label, _spot_reason = spot_onchain_score(snapshot, signal)
    if spot_score >= 7:
        add(2, "现货确认强")
    if spot_score <= 3:
        add(-2, "现货不确认")
    long_score = long_flow_alignment_score(snapshot)
    if long_score >= 6:
        add(1, "长周期资金支持")
    if long_score <= 3:
        add(-1, "长周期资金不支持")

    mapped = max(0, min(10, int(round(5 + raw * 0.8))))
    if raw >= 4:
        mapped = max(8, mapped)
        label = "主力偏多"
    elif raw <= -4:
        mapped = min(3, mapped)
        label = "主力偏空"
    else:
        mapped = max(4, min(6, mapped))
        label = "主力分歧"
    reason = "；".join(dict.fromkeys(reasons)) if reasons else "主力长周期方向分歧"
    return mapped, label, truncate_text(reason, 120)


def spot_period_state(text: str, period: str) -> str | None:
    match = re.search(rf"{re.escape(period)}(偏强|偏弱|中性|无方向)", text)
    return match.group(1) if match else None


def dex_period_state(text: str, period: str) -> str | None:
    value = dex_period_change(text, period)
    if value is None:
        return None
    return trend_state(value)


def dex_period_change(text: str, period: str) -> float | None:
    match = re.search(rf"{re.escape(period)}=([+\-]?\d+(?:\.\d+)?)%", text)
    return parse_float(match.group(1)) if match else None


def dex_volume_usd(text: str, period: str) -> float | None:
    match = re.search(rf"成交{re.escape(period)}=([+\-]?\d+(?:\.\d+)?)([KMB]?)", text, flags=re.IGNORECASE)
    if not match:
        return None
    value = parse_float(match.group(1))
    if value is None:
        return None
    unit = match.group(2).upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(unit, 1)
    return value * multiplier


def dex_liquidity_usd(text: str) -> float | None:
    match = re.search(r"流动性=([+\-]?\d+(?:\.\d+)?)([KMB]?)", text, flags=re.IGNORECASE)
    if not match:
        return None
    value = parse_float(match.group(1))
    if value is None:
        return None
    unit = match.group(2).upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(unit, 1)
    return value * multiplier


def dex_volume_expanded(value: float | None) -> bool:
    return value is not None and value >= 100000


def extract_coinglass_balance_change(text: str | None, period: str) -> float | None:
    if not text:
        return None
    balance_segment = extract_labeled_segment(text, "交易所余额", "；") or ""
    match = re.search(rf"{re.escape(period)}\s*([+\-]?\d+(?:\.\d+)?)([KMB%]?)", balance_segment, flags=re.IGNORECASE)
    if not match:
        return None
    value = parse_float(match.group(1))
    if value is None:
        return None
    unit = match.group(2).upper()
    multiplier = {"K": 1_000, "M": 1_000_000, "B": 1_000_000_000}.get(unit, 1)
    return value * multiplier


def extract_coinglass_funding_accumulated(text: str | None, period: str) -> float | None:
    if not text:
        return None
    funding_segment = extract_labeled_segment(text, "Funding累计", "；") or ""
    match = re.search(rf"{re.escape(period)}\s*([+\-]?\d+(?:\.\d+)?)%", funding_segment)
    return parse_float(match.group(1)) if match else None


def funding_accumulated_extreme(value: float | None) -> bool:
    return value is not None and abs(value) >= 0.12


def format_rule_optimization_lines(
    snapshot: MarketSnapshot,
    signal: Signal | None = None,
    spot_text: str | None = None,
    coinglass_text: str | None = None,
) -> list[str]:
    resolved_spot_text = spot_text if spot_text is not None else (
        cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    )
    spot_score, spot_label, spot_reason = spot_onchain_score_from_text(snapshot, signal, resolved_spot_text)
    div_label, div_score, div_reason = contract_spot_divergence_from_text(snapshot, signal, resolved_spot_text)
    major_score, major_label, major_reason = major_flow_score_from_text(snapshot, signal, coinglass_text)
    return [
        f"现货/DEX确认: {spot_score}/10 {spot_label} - {spot_reason}",
        f"合约现货: {div_label} {div_score}/10 - {div_reason}",
        f"主力趋势: {major_score}/10 {major_label} - {major_reason}",
    ]


def compact_rule_confirmation(
    snapshot: MarketSnapshot,
    signal: Signal | None = None,
    spot_text: str | None = None,
    coinglass_text: str | None = None,
) -> str:
    resolved_spot_text = spot_text if spot_text is not None else (
        cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    )
    _spot_score, spot_label, _spot_reason = spot_onchain_score_from_text(snapshot, signal, resolved_spot_text)
    div_label, _div_score, _div_reason = contract_spot_divergence_from_text(snapshot, signal, resolved_spot_text)
    _major_score, major_label, _major_reason = major_flow_score_from_text(snapshot, signal, coinglass_text)
    spot_part = "现货强" if spot_label == "强" else "现货弱" if spot_label == "弱" else "现货中性"
    div_part = {
        "无背离": "背离无",
        "轻微背离": "背离轻微",
        "明显背离": "背离明显",
        "严重背离": "背离严重",
    }.get(div_label, "背离无")
    major_part = "主力数据不足" if major_label == "数据不足" else major_label
    return f"{spot_part} | {div_part} | {major_part}"


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


def topq_action_short(action: str) -> str:
    if action.startswith("短线拉盘观察"):
        return "短线拉盘，等确认"
    if action.startswith("减仓/避险"):
        return "减仓/避险，等回落确认"
    if action.startswith("风险观察"):
        return "风险观察，不追多"
    if action.startswith("建议减仓/避险"):
        return "建议减仓/避险"
    if action.startswith("强烈建议关注买入"):
        return "强烈建议关注买入"
    if action.startswith("建议观察"):
        return "建议观察，等确认入场"
    if action.startswith(("不追高", "禁止追多")):
        return "短线拉盘，等确认"
    if action.startswith("禁止抄底"):
        return "禁止抄底，等待止跌"
    if action.startswith("关注反弹"):
        return "关注反弹机会"
    return "信号不足，继续盯盘"


def topq_kind_tiebreak_rank(kind: str | None, direction: str | None = None) -> int:
    normalized = topq_kind_normalized(kind)
    risk_order = {
        "main_risk_watch": 40,
        "top_exhaustion": 30,
        "top_risk": 20,
        "distribution": 10,
    }
    bullish_order = {
        "main_trend_watch": 40,
        "main_momentum_watch": 35,
        "hot_breakout": 30,
        "discovery": 20,
        "bottom_reversal": 10,
    }
    if signal_direction_label(normalized) == "看空" or direction == "看空":
        return risk_order.get(normalized, 0)
    if signal_direction_label(normalized) == "看多" or direction == "看多":
        return bullish_order.get(normalized, 0)
    return 0


def topq_short_phrase(text: str | None, fallback: str = "观察") -> str:
    value = str(text or "").strip()
    if not value:
        return fallback
    mappings = (
        ("短线强，中线不支持", "短强中弱"),
        ("中长线资金不支持", "中长线派发"),
        ("吸筹观察/中长线派发", "中长线派发"),
        ("高位拥挤，注意出货", "高位拥挤"),
        ("主力建仓，现货确认", "建仓确认"),
        ("主力建仓，等待现货确认", "建仓待确认"),
        ("现货确认，资金等待共振", "现货确认"),
        ("空头拥挤，等待逼空确认", "空头拥挤"),
        ("资金分歧，观望", "资金分歧"),
        ("风险释放，不急追单", "风险释放"),
        ("建议减仓/避险", "减仓/避险"),
        ("强烈建议关注买入", "强关注买入"),
        ("建议观察，等确认入场", "等确认入场"),
        ("短线拉盘，等确认", "短线拉盘，等确认"),
        ("禁止抄底，等待止跌", "禁止抄底"),
        ("关注反弹机会", "关注反弹"),
        ("短线拉盘观察，等待确认，不追高", "短线拉盘，等确认"),
        ("信号不足，继续盯盘", "继续盯盘"),
        ("真启动观察", "启动观察"),
        ("合约先行观察", "合约先行"),
        ("高位出货", "高位出货"),
        ("多杀多风险", "多杀多"),
        ("震荡分歧", "震荡分歧"),
    )
    for key, phrase in mappings:
        if key in value:
            return phrase
    compact = re.sub(r"\s+", "", value)
    compact = compact.replace("，", "").replace("。", "").replace("；", "")
    return compact[:18] if compact else fallback


def topq_conclusion_text(
    direction: str,
    evidence_summary: str,
    action: str,
    intent: str,
    flow_label: str,
) -> str:
    if flow_label == "短强中弱":
        return "短强中弱，谨慎追"
    if flow_label == "中长线派发":
        return "中长线派发，先观察"
    evidence = topq_short_phrase(evidence_summary, "")
    action_text = topq_short_phrase(topq_action_short(action), "")
    intent_text = topq_short_phrase(intent, "")
    if direction == "看空":
        return evidence or action_text or intent_text or "风险观察"
    if direction == "看多":
        return action_text or evidence or intent_text or "等待确认"
    return evidence or intent_text or action_text or "观察"


def format_compact_percent(value: Any) -> str:
    if value is None:
        return "-%"
    try:
        return f"{float(value):+.2f}%"
    except Exception:
        return f"{value}%"


def is_valid_binance_usdt_symbol(symbol: str | None) -> bool:
    return bool(VALID_BINANCE_USDT_SYMBOL_RE.fullmatch(str(symbol or "").strip().upper()))


def compact_symbol_list_for_log(symbols: list[str], limit: int = 10) -> str:
    if not symbols:
        return "-"
    visible = [str(symbol).upper() for symbol in symbols[:limit]]
    suffix = f"...+{len(symbols) - limit}" if len(symbols) > limit else ""
    return ", ".join([*visible, suffix] if suffix else visible)


def display_usdt_symbol(symbol: str | None) -> str:
    normalized = str(symbol or "").strip().upper()
    return normalized[:-4] if normalized.endswith("USDT") else normalized


def signal_direction_label(kind: str | None) -> str:
    raw = str(kind or "").strip().lower()
    if raw in ("long", "看多"):
        return "看多"
    if raw in ("short", "看空"):
        return "看空"
    if raw in ("neutral", "observe", "观察"):
        return "观察"
    normalized = raw.replace("_", " ").replace("-", " ")
    bullish = {
        "discovery",
        "hot breakout",
        "main trend watch",
        "main momentum watch",
        "bottom reversal",
        "early breakout",
        "possible early breakout",
    }
    bearish = {
        "top risk",
        "main risk watch",
        "top exhaustion",
        "distribution",
        "crowded top risk",
    }
    if raw in bullish or normalized in bullish:
        return "看多"
    if raw in bearish or normalized in bearish:
        return "看空"
    return "观察"


def signal_kind_label(kind: str | None) -> str:
    labels = {
        "discovery": "启动发现",
        "hot_breakout": "热点突破",
        "main_trend_watch": "主流趋势雷达",
        "main_momentum_watch": "主流异动雷达",
        "bottom_reversal": "底部反转",
        "top_risk": "顶部风险",
        "main_risk_watch": "主流风险雷达",
        "top_exhaustion": "顶部衰竭",
        "distribution": "派发风险",
        "crowded_top_risk": "拥挤顶部",
        "unknown": "观察信号",
    }
    raw = str(kind or "unknown").strip().lower()
    normalized = raw.replace("-", "_").replace(" ", "_")
    return labels.get(normalized, labels["unknown"])


def topq_signal_kind_label(row: dict[str, str], action: str) -> str:
    kind = topq_kind_normalized(row.get("kind"))
    symbol = str(row.get("symbol") or "").strip().upper()
    action_text = str(action or "")
    cautious_action = text_has_any(action_text, ("禁止追多", "不追高", "观察"))
    if kind in {"discovery", "hot_breakout"} and is_major_asset_tier(symbol) and cautious_action:
        return "主流异动观察"
    return signal_kind_label(kind)


def entry_timing_direction(kind: str | None) -> str:
    raw = str(kind or "").strip().lower()
    normalized = raw.replace("_", " ").replace("-", " ")
    long_kinds = {
        "discovery",
        "hot breakout",
        "main trend watch",
        "main momentum watch",
        "bottom reversal",
        "early breakout",
        "possible early breakout",
    }
    risk_kinds = {
        "top risk",
        "main risk watch",
        "top exhaustion",
        "distribution",
        "crowded top risk",
    }
    if raw in long_kinds or normalized in long_kinds:
        return "long"
    if raw in risk_kinds or normalized in risk_kinds:
        return "risk"
    return "neutral"


def entry_timing_score(
    snapshot: MarketSnapshot,
    signal: Signal,
    trap_score_override: int | None = None,
) -> tuple[int, str, str]:
    direction = entry_timing_direction(signal.kind)
    if direction == "neutral":
        return 5, "观察", "中性信号"

    score = 5
    reasons: list[str] = []
    position = snapshot.price_position_24h
    price_change = snapshot.price_change_percent
    oi_change = snapshot.oi_change_percent
    taker = snapshot.taker_buy_sell_ratio
    funding = snapshot.funding_rate_percent
    flow_score = flow_alignment_score(snapshot)
    long_flow_score = long_flow_alignment_score(snapshot)
    if trap_score_override is None:
        trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)
    else:
        trap_score = trap_score_override

    def add(points: int, reason: str) -> None:
        nonlocal score
        score += points
        reasons.append(reason)

    short_flows = [summary_flow_value(snapshot, period) for period in ("5m", "15m", "1h")]
    positive_short_flows = sum(value > 0 for value in short_flows)
    negative_short_flows = sum(value < 0 for value in short_flows)
    funding_normal_or_light_negative = funding is None or -0.05 <= funding < 0.03
    funding_hot_or_extreme = funding is not None and funding >= 0.03
    funding_extreme_positive = funding is not None and funding >= 0.08
    funding_extreme_negative = funding is not None and funding <= -0.08
    spot_text = cached_spot_alpha_confirmation(snapshot.symbol)
    spot_strong = "偏强" in spot_text and "偏弱" not in spot_text
    liquidation_wash = (
        "双向强平" in signal.message
        or "剧烈洗盘" in signal.message
        or "双向高波动" in liquidation_risk_label(snapshot)
    )

    if direction == "long":
        if position is not None and 35 <= position <= 70 and -2 <= price_change <= 5:
            add(2, "启动窗口")
        if 2 <= oi_change <= 8:
            add(2, "温和增仓")
        if positive_short_flows >= 2:
            add(1, "短线资金转入")
        if long_flow_score >= 6:
            add(1, "长周期支持")
        if funding_normal_or_light_negative:
            add(1, "费率健康")
        if spot_strong:
            add(1, "现货确认")
        if trap_score <= 3:
            add(1, "假信号低")

        chase_risk = False
        falling_relay = False
        if position is not None and position > 75 and price_change > 8:
            add(-4, "高位大涨")
            chase_risk = True
        if oi_change > 10 and price_change > 8:
            add(-2, "高位增仓拥挤")
        if long_flow_score <= 3:
            add(-2, "长周期不支持")
        if flow_score <= 3:
            add(-1, "短线资金不支持")
        if funding_extreme_positive:
            add(-2, "多头拥挤")
        if trap_score >= 6:
            add(-2, "假信号风险高")
        if signal.kind == "bottom_reversal" and position is not None and position < 25 and summary_flow_value(snapshot, "1h") < 0:
            add(-3, "低位但资金未回流")
            falling_relay = True
        if signal.kind == "bottom_reversal" and taker is not None and taker < 1:
            add(-2, "抄底未确认")

        score = max(0, min(10, int(round(score))))
        if chase_risk:
            label = "追高风险"
        elif falling_relay:
            label = "下跌中继"
        elif score >= 8:
            label = "启动前/启动初期"
        elif score >= 6:
            label = "启动观察"
        elif score <= 4:
            label = "不宜追"
        else:
            label = "观察"
        return score, label, "；".join(reasons) or "暂无明显阶段特征"

    if position is not None and position > 70:
        add(2, "高位风险")
    if price_change > 5 and oi_change > 8:
        add(2, "拉升增仓")
    if taker is not None and taker < 1:
        add(1, "主动买盘转弱")
    if negative_short_flows >= 2:
        add(2, "短线资金转出")
    if funding_hot_or_extreme:
        add(1, "多头成本高")
    if signal.kind in ("top_exhaustion", "top_risk") and flow_score <= 4:
        add(1, "上攻乏力")
    if liquidation_wash:
        add(1, "波动风险高")

    if position is not None and position < 40:
        add(-3, "看空但不追空")
    if price_change < -8:
        add(-2, "已大跌")
    if funding_extreme_negative:
        add(-2, "空头拥挤")
    if positive_short_flows >= 2:
        add(-1, "短线承接")

    score = max(0, min(10, int(round(score))))
    if score >= 8:
        label = "逃顶/减仓优先"
    elif score >= 6:
        label = "顶部风险观察"
    elif score <= 4:
        label = "做空风险/不宜追空"
    else:
        label = "风险观察"
    return score, label, "；".join(reasons) or "暂无明显阶段特征"


def direction_badge(direction: str | None) -> str:
    label = str(direction or "").strip()
    if label == "看多":
        return "🟢看多"
    if label == "看空":
        return "🔴看空"
    return "⚪观察"


def direction_icon(direction: str | None) -> str:
    label = str(direction or "").strip()
    if label == "看多":
        return "🟢"
    if label == "看空":
        return "🔴"
    return "⚪"


def priority_badge(priority: str | None) -> str:
    label = str(priority or "").strip().upper()
    if label == "S":
        return "🟣S级"
    if label == "A":
        return "🟢A级"
    if label == "B":
        return "🟡B级"
    if label == "C":
        return "⚪C级"
    if label == "D":
        return "⚫D级"
    return "⚫-级"


def priority_grade_label(priority: str | None) -> str:
    return priority_badge(priority)


def best_priority_label(priorities: list[str]) -> str:
    order = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 0}
    normalized = [str(priority or "").strip().upper() for priority in priorities]
    return max(normalized or ["-"], key=lambda priority: order.get(priority, -1))


def strength_badge(strength: float | int | str | None) -> str:
    score = parse_float(strength)
    if score is None:
        return "·弱"
    if score >= 50:
        return "🔥极强"
    if score >= 30:
        return "🔥强"
    if score >= 20:
        return "⚡中"
    return "·弱"


def trap_badge(score: float | int | str | None) -> str:
    value = parse_float(score)
    if value is None:
        return "🟢假信号低"
    if value >= 8:
        return "🔴假信号极高"
    if value >= 6:
        return "🟠假信号高"
    if value >= 3:
        return "🟡假信号中"
    return "🟢假信号低"


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
_COINGLASS_TEXT_CACHE: dict[str, tuple[float, str]] = {}
_DEXSCREENER_PAIR_CACHE: dict[str, dict[str, Any]] = {}


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


def cached_coinglass_market_context_text(symbol: str) -> str:
    cached = _COINGLASS_TEXT_CACHE.get(str(symbol).upper())
    if cached and time.time() - cached[0] < COINGLASS_MARKET_CONTEXT_CACHE_TTL_SECONDS:
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
        _DEXSCREENER_PAIR_CACHE[str(symbol).upper()] = pair

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


def clamp_int(value: float, low: int, high: int) -> int:
    return max(low, min(high, int(round(value))))


def basis_state(snapshot: MarketSnapshot) -> tuple[float | None, str, str]:
    spot_price = getattr(snapshot, "spot_price", None)
    if spot_price is None or spot_price <= 0:
        return None, "数据不足", "缺现货价格"

    basis_pct = (snapshot.close_price - spot_price) / spot_price * 100
    abs_basis = abs(basis_pct)
    tier = market_tier(snapshot.symbol)
    if tier in ("core", "major"):
        thresholds = (0.03, 0.08, 0.20)
    elif tier in ("large", "alt"):
        thresholds = (0.10, 0.30, 0.80)
    else:
        thresholds = (0.20, 0.50, 1.50)

    if abs_basis < thresholds[0]:
        return basis_pct, "正常", "基差正常"
    if abs_basis < thresholds[1]:
        level = "轻微"
    elif abs_basis < thresholds[2]:
        level = "明显"
    else:
        level = "极端"

    if basis_pct > 0:
        return basis_pct, f"{level}溢价", "合约溢价，追多偏激进"
    return basis_pct, f"{level}贴水", "合约贴水，空头偏激进"


def flow_horizon_scores(snapshot: MarketSnapshot) -> tuple[int, int, int, str, str]:
    def period_score(periods: list[str]) -> int:
        seen = False
        raw = 0.0
        for period in periods:
            flow = snapshot.net_flow_usd.get(period)
            ratio = snapshot.net_flow_ratio.get(period)
            if flow is None and ratio is None:
                continue
            seen = True
            weight = {"5m": 1.0, "15m": 1.2, "1h": 1.5, "4h": 1.2, "12h": 1.4, "24h": 1.6}.get(period, 1.0)
            if flow is not None:
                if flow > 0:
                    raw += 1.4 * weight
                elif flow < 0:
                    raw -= 1.4 * weight
                size_base = max(snapshot.quote_volume_24h or 1.0, 1.0)
                size = min(abs(flow) / size_base * 100, 2.0)
                raw += size * weight * (1 if flow > 0 else -1)
            if ratio is not None:
                if ratio > 1:
                    raw += min((ratio - 1) * 2.0, 1.4) * weight
                elif ratio < 1:
                    raw -= min((1 - ratio) * 2.0, 1.4) * weight
        if not seen:
            return 5
        return clamp_int(5 + raw, 0, 10)

    short_score = period_score(["5m", "15m", "1h"])
    mid_score = period_score(["4h", "12h", "24h"])

    long_raw = 0.0
    long_seen = False
    for period in ("72h", "144h"):
        flow = snapshot.net_flow_usd.get(period)
        ratio = snapshot.net_flow_ratio.get(period)
        if flow is None and ratio is None:
            continue
        long_seen = True
        if flow is not None:
            long_raw += 2 if flow > 0 else -2 if flow < 0 else 0
        if ratio is not None:
            long_raw += 1 if ratio > 1 else -1 if ratio < 1 else 0

    if not long_seen:
        coinglass_text = cached_coinglass_market_context_text(snapshot.symbol)
        oi_24h = extract_coinglass_oi_change(coinglass_text, "24h")
        buy_ratio, sell_ratio = extract_coinglass_taker_ratios(coinglass_text)
        balance_1d = extract_coinglass_balance_change(coinglass_text, "1d")
        balance_7d = extract_coinglass_balance_change(coinglass_text, "7d")
        funding_1d = extract_coinglass_funding_accumulated(coinglass_text, "1d")
        funding_7d = extract_coinglass_funding_accumulated(coinglass_text, "7d")
        helpers = [oi_24h, buy_ratio, sell_ratio, balance_1d, balance_7d, funding_1d, funding_7d]
        if any(value is not None for value in helpers):
            long_seen = True
            if oi_24h is not None and oi_24h > 0:
                long_raw += 1
            if buy_ratio is not None and buy_ratio > 51:
                long_raw += 1
            if sell_ratio is not None and sell_ratio > 51:
                long_raw -= 1
            if any(value is not None and value < 0 for value in (balance_1d, balance_7d)):
                long_raw += 1.5
            if any(value is not None and value > 0 for value in (balance_1d, balance_7d)):
                long_raw -= 1.5
            if funding_accumulated_extreme(funding_1d) or funding_accumulated_extreme(funding_7d):
                long_raw -= 0.8
            long_raw += (mid_score - 5) * 0.4

    long_score = clamp_int(5 + long_raw, 0, 10) if long_seen else 5

    if short_score >= 7 and mid_score >= 7 and long_score >= 6:
        label = "多周期共振流入"
    elif short_score >= 7 and mid_score <= 5:
        label = "短强中弱"
    elif short_score <= 4 and mid_score >= 7:
        label = "短弱中强"
    elif mid_score >= 7 and long_score >= 7:
        label = "中长线吸筹"
    elif mid_score <= 3 and long_score <= 3:
        label = "中长线派发"
    elif short_score <= 3 and mid_score <= 3 and long_score <= 4:
        label = "全周期流出"
    else:
        label = "资金分歧"
    reason = f"短{short_score}/中{mid_score}/长{long_score}"
    return short_score, mid_score, long_score, label, reason


def position_behavior(snapshot: MarketSnapshot, signal: Signal | None = None) -> tuple[str, int, str]:
    price = snapshot.price_change_percent
    oi = snapshot.oi_change_percent
    taker = snapshot.taker_buy_sell_ratio
    funding = snapshot.funding_rate_percent or 0.0
    global_ls = snapshot.global_long_short_ratio
    top_ls = snapshot.top_position_ratio or snapshot.top_account_ratio
    short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    high_pos = snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75
    low_pos = snapshot.price_position_24h is not None and snapshot.price_position_24h <= 25
    long_biased = (global_ls is not None and global_ls >= 1.4) or (top_ls is not None and top_ls >= 1.3)
    short_biased = (global_ls is not None and global_ls <= 0.8) or (top_ls is not None and top_ls <= 0.85)
    taker_strong = taker is not None and taker >= 1.08
    taker_weak = taker is not None and taker <= 0.95
    funding_hot = funding >= 0.05
    funding_extreme_negative = funding <= -0.08
    basis_positive = basis_pct is not None and basis_pct > 0 and basis_label in ("明显溢价", "极端溢价")
    basis_negative = basis_pct is not None and basis_pct < 0 and basis_label in ("明显贴水", "极端贴水")
    reasons: list[str] = []

    if abs(price) >= 5 and oi >= 8 and (abs(short_flow - mid_flow) >= 4 or taker is None):
        return "双向加杠杆", 7, "大波动/OI大增/资金分歧"
    if price > 0 and oi > 0 and taker_strong and funding <= 0.05 and mid_flow >= 5:
        score = 8 if mid_flow >= 6 else 7
        if long_flow >= 6:
            score += 1
        return "多头主动建仓", min(score, 10), "价涨/OI涨/主动买盘强/中线资金不弱"
    if price < 0 and oi > 0 and taker_weak and funding <= 0.03 and mid_flow <= 6:
        return "空头主动建仓", 8, "价跌/OI涨/主动买盘弱/中线资金不强"
    if price > 0 and oi > 0 and (funding_hot or basis_positive) and long_biased and taker_weak:
        return "高位多头拥挤", 8 if high_pos else 7, "价涨增仓/Funding或正基差偏热/多空偏多/主买转弱"
    if price < 0 and oi > 0 and (funding_extreme_negative or basis_negative) and short_biased:
        return "低位空头拥挤", 8 if low_pos else 7, "价跌增仓/负费率或负基差/多空偏空"
    if price > 0 and oi < 0:
        return "空头回补/逼空", 7, "价涨/OI下降"
    if price < 0 and oi < 0:
        label = "多头止损/踩踏" if price <= -4 else "仓位退出/风险释放"
        return label, 7 if price <= -4 else 6, "价跌/OI下降"
    if oi > 0 and price > 0:
        reasons.append("增仓上涨")
    if flow_label != "资金分歧":
        reasons.append(flow_label)
    return "资金分歧/震荡", 5, "；".join(reasons) or "方向证据不足"


def squeeze_state(snapshot: MarketSnapshot) -> tuple[str, int, str]:
    funding = snapshot.funding_rate_percent or 0.0
    oi = snapshot.oi_change_percent
    price = snapshot.price_change_percent
    taker = snapshot.taker_buy_sell_ratio
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    long_biased = snapshot.global_long_short_ratio is not None and snapshot.global_long_short_ratio >= 1.4
    short_biased = snapshot.global_long_short_ratio is not None and snapshot.global_long_short_ratio <= 0.8
    taker_turning_strong = taker is not None and taker >= 1.08
    taker_weak = taker is not None and taker <= 0.95
    liq_label = liquidation_risk_label(snapshot)
    short_liq = "空头强平" in liq_label
    long_liq = "多头强平" in liq_label
    wash = "双向" in liq_label or "洗盘" in liq_label
    negative_basis = basis_pct is not None and basis_pct < 0 and basis_label != "正常"
    positive_basis = basis_pct is not None and basis_pct > 0 and basis_label != "正常"

    if wash and (oi >= 5 or abs(price) >= 5):
        return "双向挤压", 8, "双向强平/剧烈波动/OI扩张"
    if funding <= 0 and oi > 0 and (short_biased or taker_turning_strong) and price >= -0.5:
        score = 6 + int(funding <= -0.05) + int(short_liq) + int(negative_basis)
        return "空头挤压", min(score, 10), "负费率/OI增加/空头偏拥挤或主买转强/价格抗跌"
    if funding >= 0.03 and oi >= 0 and long_biased and (taker_weak or price < 0):
        score = 6 + int(funding >= 0.08) + int(long_liq) + int(positive_basis)
        return "多头挤压", min(score, 10), "正费率/OI高位/多头偏拥挤/主买弱或价格转跌"
    return "无明显挤压", 3, "挤压条件不足"


def spot_absorption_state(snapshot: MarketSnapshot, signal: Signal | None = None) -> tuple[str, int, str]:
    spot_text = cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    if "无标准现货/高流动性DEX数据" in spot_text:
        return "数据不足", 5, "缺标准现货/高流动性DEX数据"
    text = str(spot_text or "")
    spot_score, spot_label, spot_reason = spot_onchain_score_from_text(snapshot, signal, text)
    div_label, div_score, _div_reason = contract_spot_divergence_from_text(snapshot, signal, text)
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    pos_label, _pos_score, _pos_reason = position_behavior(snapshot, signal)
    high_or_crowded = (
        (snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75)
        or pos_label in ("高位多头拥挤", "双向加杠杆")
        or (basis_pct is not None and basis_pct > 0 and basis_label in ("明显溢价", "极端溢价"))
        or (snapshot.funding_rate_percent is not None and snapshot.funding_rate_percent >= 0.05)
    )
    dex_1h_change = dex_period_change(text, "1h")
    dex_24h_change = dex_period_change(text, "24h")
    dex_1h = dex_period_state(text, "1h")
    spot_1h = spot_period_state(text, "1h")

    if dex_24h_change is not None and dex_24h_change >= 15 and dex_1h_change is not None and dex_1h_change <= -1:
        return "链上出货", 3, "DEX 24h大涨但1h转弱"
    if spot_score >= 7:
        label = "链上承接" if "链上DEX" in text else "现货承接"
        return label, min(10, spot_score), spot_reason
    if spot_score <= 3 and high_or_crowded:
        label = "链上出货" if "链上DEX" in text else "现货出货"
        return label, max(1, spot_score), f"高位/拥挤叠加现货弱；{spot_reason}"
    if snapshot.price_change_percent > 0 and snapshot.oi_change_percent > 5 and spot_score <= 5 and div_score >= 3:
        return "现货未跟", 5, f"合约强但现货确认一般；{div_label}"
    if spot_1h == "偏弱" or dex_1h == "偏弱":
        return "承接不明", 4, spot_reason
    return "承接不明", 5 if spot_label == "中性" else spot_score, spot_reason


def leading_direction_cn(direction: str) -> str:
    if direction == "long":
        return "看多"
    if direction == "short":
        return "看空/风险"
    return "中性"


def is_telegram_risk_signal(signal: Signal | None) -> bool:
    if signal is None:
        return False
    normalized = str(signal.kind or "").strip().lower().replace("-", "_").replace(" ", "_")
    return normalized in {"main_risk_watch", "top_risk", "top_exhaustion", "distribution", "crowded_top_risk"}


def risk_signal_action_label(
    conviction: int,
    intent_label: str,
    evidence_summary: str,
    ev_display: int,
    basis_label: str,
) -> tuple[str, str]:
    strong_risk = (
        ev_display >= 5
        or conviction >= 80
        or intent_label in ("高位出货", "多杀多风险", "下跌中继")
        or basis_label in PREMIUM_BASIS_STATES
        or text_has_any(evidence_summary, TOPQ_RISK_EVIDENCE_KEYWORDS)
    )
    if strong_risk:
        return "减仓/避险，等回落确认", "风险类信号禁止切换为买入口径"
    return "风险观察，不追多", "风险证据未充分确认"


def leading_signal_score(snapshot: MarketSnapshot, signal: Signal | None = None) -> LeadingSignalScore:
    bull_score = 0
    bear_score = 0
    items: list[tuple[int, str, str]] = []

    def add(direction: str, points: int, text: str, observation: bool = False) -> None:
        nonlocal bull_score, bear_score
        points = abs(int(points))
        if direction == "long":
            bull_score += points
        elif direction == "short":
            bear_score += points
        prefix = "观察：" if observation and not text.startswith("观察") else ""
        items.append((points, direction, f"{prefix}{text} +{points}"))

    def flow(period: str) -> float | None:
        return snapshot.net_flow_usd.get(period) if snapshot.net_flow_usd else None

    def price_change(period: str) -> float | None:
        if period == "1h":
            return snapshot.price_change_percent
        if snapshot.price_change_periods:
            return snapshot.price_change_periods.get(period)
        return snapshot.confirm_price_change_percent if period in ("5m", "15m") else None

    def positive(period: str) -> bool:
        value = flow(period)
        return value is not None and value > 0

    def negative(period: str) -> bool:
        value = flow(period)
        return value is not None and value < 0

    def period_avg(periods: tuple[str, ...]) -> float | None:
        values = [value for period in periods if (value := flow(period)) is not None]
        if not values:
            return None
        return sum(values) / len(values)

    short_oi = snapshot.confirm_oi_change_percent
    short_price = price_change("15m")
    if short_oi is not None and short_price is not None and abs(short_price) <= 0.5:
        if short_oi >= 4:
            add("long", 3, "OI突增但价格未动，主力悄悄建仓")
        elif short_oi <= -4:
            add("short", 3, "OI突减但价格未动，主力悄悄平仓")

    one_hour_price = snapshot.price_change_percent
    if snapshot.oi_change_percent > 2:
        text = "OI持续增仓，价格同步推升" if one_hour_price > 0.5 else "OI持续增仓，价格尚未反应"
        add("long", 2, f"主力持续建仓；{text}")
    if snapshot.oi_change_percent > 5 and (one_hour_price >= -0.5):
        text = "OI持续增仓，价格同步推升" if one_hour_price > 1 else "OI持续增仓，价格尚未反应"
        add("long", 2, f"中周期持续建仓；{text}")

    volume_hot = snapshot.volume_ratio_24h is not None and snapshot.volume_ratio_24h >= 2
    taker = snapshot.taker_buy_sell_ratio
    short_flow_score, mid_flow_score, _long_flow_score, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    short_avg = period_avg(("5m", "15m"))
    mid_avg = period_avg(("1h", "4h"))
    long_avg = period_avg(("12h", "24h", "72h"))
    flow_cycle_divergence = False
    if short_avg is not None and mid_avg is not None and long_avg is not None:
        signs = {1 if value > 0 else -1 if value < 0 else 0 for value in (short_avg, mid_avg, long_avg)}
        flow_cycle_divergence = 1 in signs and -1 in signs
    bear_flow_reversal = long_avg is not None and short_avg is not None and long_avg > 0 and short_avg < 0
    bull_flow_reversal = long_avg is not None and short_avg is not None and long_avg < 0 and short_avg > 0
    if volume_hot:
        if (
            one_hour_price >= -0.3
            and (short_flow_score >= 6 or (taker is not None and taker >= 1.05))
            and not bear_flow_reversal
            and not flow_cycle_divergence
        ):
            add("long", 2, "爆量不跌，多头承接吸筹")
        elif one_hour_price <= 0.3 and (short_flow_score <= 4 or (taker is not None and taker <= 0.95)):
            add("short", 2, "爆量不涨，顶部出货迹象")
        else:
            items.append((0, "neutral", "观察：短周期放量，方向仍不清晰 +0"))

    funding = snapshot.funding_rate_percent
    if funding is not None:
        if funding <= -0.08 and snapshot.oi_change_percent > 0:
            add("long", 3, "费率极负+OI上升，空头拥挤，关注逼空")
        elif funding >= 0.08 and snapshot.oi_change_percent < 0:
            add("short", 3, "费率极高+OI下降，多头出逃，顶部风险")
        elif funding >= 0.08 and snapshot.oi_change_percent > 0:
            add("short", 2, "费率极高+OI继续上升，多头过热，注意踩踏")

    net_flow_reversal = "none"
    if bull_flow_reversal:
        net_flow_reversal = "bull_reversal"
        add("long", 2, "长周期流出，短周期转正，资金开始回流")
        if mid_avg is not None and mid_avg > 0:
            add("long", 1, "1h/4h资金同步转正，回流确认增强")
    elif bear_flow_reversal:
        net_flow_reversal = "bear_reversal"
        add("short", 2, "长周期流入，短周期转负，主力开始派发")
        if mid_avg is not None and mid_avg < 0:
            add("short", 1, "1h/4h资金同步转负，派发确认增强")
    if flow_cycle_divergence:
        items.append((1, "neutral", "资金周期分歧，暂不确认反转 +1"))

    short_net_negative = any(negative(period) for period in ("5m", "15m", "30m") if flow(period) is not None)
    short_net_positive = any(positive(period) for period in ("5m", "15m", "30m") if flow(period) is not None)
    if snapshot.oi_change_percent > 2 and short_net_negative:
        add("long", 1, "OI建仓但短线净卖出，疑似主力吸筹", observation=True)
    if snapshot.oi_change_percent < -2 and short_net_positive:
        add("short", 2, "OI平仓但短线净买入，疑似主力出货")

    account_lsr = snapshot.top_account_ratio
    position_lsr = snapshot.top_position_ratio
    if account_lsr is not None and account_lsr >= 2.0:
        add("short", 2, "大户账户多空比极高，多头拥挤")
    elif account_lsr is not None and account_lsr <= 0.7:
        add("long", 2, "大户账户多空比极低，空头拥挤")
    if position_lsr is not None and account_lsr is not None and 1.2 <= position_lsr < 2.0 and 0.8 <= account_lsr <= 1.8:
        items.append((0, "neutral", "大户持仓偏多但账户不极端，作为辅助观察 +0"))
    if position_lsr is not None and account_lsr is not None and abs(position_lsr - account_lsr) >= 0.5:
        items.append((0, "neutral", "多空比结构快速分化，需辅助判断 +0"))

    if bull_score >= bear_score + 2:
        direction = "long"
        score = bull_score
        if flow_cycle_divergence:
            label = "资金分歧"
        else:
            label = "主力建仓" if any("建仓" in text or "吸筹" in text for _p, d, text in items if d == "long") else "资金回流"
    elif bear_score >= bull_score + 2:
        direction = "short"
        score = bear_score
        label = "资金分歧" if flow_cycle_divergence else "高位出货" if any("出货" in text or "顶部" in text for _p, d, text in items if d == "short") else "风险观察"
    else:
        direction = "neutral"
        score = max(bull_score, bear_score)
        label = "资金分歧" if flow_cycle_divergence else "分歧观察" if items else "无"

    if flow_label == "短强中弱":
        label = "短线强，中线不支持"
    elif flow_label == "中长线派发":
        label = "资金分歧"

    ev_display: int | None = None
    has_flow_confirmation = short_flow_score >= 7 or mid_flow_score >= 7
    strong_build_allowed = flow_label not in ("短强中弱", "中长线派发") and has_flow_confirmation
    if label == "主力建仓" or not strong_build_allowed:
        ev_total, ev_direction, _ev_summary, ev_items = evidence_score(snapshot, signal)
        ev_display = evidence_display_score(ev_direction, ev_items, ev_total)
        strong_build_allowed = strong_build_allowed and ev_display >= 5
    if label == "主力建仓" and not strong_build_allowed:
        label = "资金回流观察"

    sorted_rows = sorted(items, key=lambda row: row[0], reverse=True)
    if direction == "long":
        primary_rows = [row for row in sorted_rows if row[1] != "short"]
        opposite_rows = [row for row in sorted_rows if row[1] == "short"]
        sorted_items = [text for _points, _direction, text in primary_rows]
        if opposite_rows:
            sorted_items.append(sanitize_opposite_leading_item(opposite_rows[0][2]))
    elif direction == "short":
        primary_rows = [row for row in sorted_rows if row[1] != "long"]
        opposite_rows = [row for row in sorted_rows if row[1] == "long"]
        sorted_items = [text for _points, _direction, text in primary_rows]
        if opposite_rows:
            sorted_items.append(sanitize_opposite_leading_item(opposite_rows[0][2]))
    else:
        sorted_items = [text for _points, _direction, text in sorted_rows]
    if not strong_build_allowed:
        sorted_items = [
            text.replace("主力悄悄建仓", "OI异动观察")
            .replace("主力持续建仓", "OI持续增仓观察")
            .replace("中周期持续建仓", "中周期OI增仓观察")
            for text in sorted_items
        ]
    return LeadingSignalScore(score, direction, label, sorted_items, bull_score, bear_score)

def market_intent(snapshot: MarketSnapshot, signal: Signal | None = None) -> tuple[str, int, str]:
    pos_label, pos_score, _pos_reason = position_behavior(snapshot, signal)
    squeeze_label, squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    spot_label, spot_score, _spot_reason = spot_absorption_state(snapshot, signal)
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    high_pos = snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75
    low_pos = snapshot.price_position_24h is not None and snapshot.price_position_24h <= 30
    extreme_basis = "极端" in basis_label
    leading = leading_signal_score(snapshot, signal)

    if leading.leading_score >= 6 and leading.leading_direction == "long" and not high_pos:
        if flow_label == "短强中弱":
            return "震荡分歧", 6, "短线强，中线不支持，谨慎追"
        if flow_label == "中长线派发":
            return "震荡分歧", 5, "中长线资金不支持，只能观察"
        if spot_label in ("现货承接", "链上承接") or mid_flow >= 5:
            return "真启动观察", min(10, max(leading.leading_score, pos_score, 7)), "领先信号增强/主力建仓或资金回流"
        return "合约先行观察", min(8, max(leading.leading_score, pos_score)), "领先信号增强/现货确认未充分"
    if leading.leading_score >= 6 and leading.leading_direction == "short":
        return "高位出货" if high_pos else "风险观察", min(10, max(leading.leading_score, 7)), "领先信号提示出货或风险"

    if pos_label == "多头主动建仓" and mid_flow >= 5 and spot_label in ("现货承接", "链上承接"):
        return "真启动观察", min(10, max(pos_score, spot_score, 7)), "主力建仓/资金支持/现货不弱"
    if pos_label == "多头主动建仓" and spot_label in ("现货未跟", "数据不足", "承接不明"):
        return "合约先行观察", min(8, pos_score), "合约主动建仓/现货确认未充分"
    if squeeze_label == "空头挤压":
        return "短线逼空", min(10, squeeze_score), "空头挤压结构"
    if pos_label == "高位多头拥挤" or spot_label in ("现货出货", "链上出货"):
        return "高位出货", 8 if high_pos or spot_score <= 3 else 7, "高位拥挤/现货转弱"
    if squeeze_label == "多头挤压":
        return "多杀多风险", min(10, squeeze_score), "多头拥挤挤压"
    if pos_label == "仓位退出/风险释放":
        return "风险释放", 6, "价跌/OI降"
    if pos_label == "多头止损/踩踏":
        return "洗盘回踩", 6, "下跌去杠杆"
    if pos_label == "空头主动建仓" and mid_flow <= 5 and not low_pos:
        return "下跌中继", 7, "空头增仓/资金偏弱"
    if pos_label == "双向加杠杆" or squeeze_label == "双向挤压" or extreme_basis:
        return "震荡分歧", 6, "双向杠杆或极端基差"
    return "震荡分歧", max(4, min(6, (short_flow + mid_flow + long_flow) // 3)), flow_label


def leading_items_without_zero(leading: LeadingSignalScore, limit: int = 6) -> list[str]:
    return [item for item in leading.leading_items if not item.endswith("+0")][:limit]


def leading_summary_line(leading: LeadingSignalScore) -> str:
    return f"领先信号: {leading.leading_score}分 {leading_direction_cn(leading.leading_direction)} {leading.leading_label}"


def leading_check_block(leading: LeadingSignalScore) -> str:
    items = leading_items_without_zero(leading, 6)
    if not items:
        return leading_summary_line(leading)
    display_score = displayed_leading_items_score(items)
    summary = f"领先信号: {display_score}分 {leading_direction_cn(leading.leading_direction)} {leading.leading_label}"
    return "\n".join([summary] + [f"- {item}" for item in items])


def leading_ask_brief(leading: LeadingSignalScore) -> str:
    items = leading_items_without_zero(leading, 2)
    suffix = "；".join(re.sub(r"\s*[+]\d+$", "", item) for item in items)
    detail = f" - {suffix}" if suffix else ""
    display_score = displayed_leading_items_score(items) if items else leading.leading_score
    return f"领先信号: {display_score}分 {leading.leading_label}{detail}"


def leading_topq_brief(row: dict[str, str]) -> str:
    score = parse_float(row.get("leading_score"))
    if score is None or score <= 0:
        return "领先0 无"
    label = (row.get("leading_label") or "观察").strip() or "观察"
    return f"领先{int(score)} {topq_short_phrase(label)}"

def evidence_item_icon(item: EvidenceItem) -> str:
    if item.polarity == "positive":
        return "🟢"
    if item.polarity == "risk":
        return "🔴"
    return "🟡"


def evidence_item_signed_points(item: EvidenceItem) -> int:
    return -abs(item.points) if item.polarity == "risk" else abs(item.points)


def evidence_items_compact(items: list[EvidenceItem], limit: int = 8) -> str:
    parts = []
    for item in items[:limit]:
        points = evidence_item_signed_points(item)
        parts.append(f"{item.label}({points:+d})")
    return "; ".join(parts)


def evidence_display_score(direction_hint: str, items: list[EvidenceItem], total_score: int) -> int:
    positive_points = sum(abs(item.points) for item in items if item.polarity == "positive")
    risk_points = sum(abs(item.points) for item in items if item.polarity == "risk")
    if direction_hint == "看多":
        return positive_points
    if direction_hint == "看空/风险":
        return risk_points
    return abs(total_score)


def evidence_score(snapshot: MarketSnapshot, signal: Signal | None = None) -> tuple[int, str, str, list[EvidenceItem]]:
    items: list[EvidenceItem] = []
    notes: list[str] = []

    def add(label: str, points: int, polarity: str, horizon: str, source: str, note: str | None = None) -> None:
        if len(items) >= 20:
            return
        items.append(EvidenceItem(label, abs(int(points)), polarity, horizon, source))
        if note:
            notes.append(note)

    price = snapshot.price_change_percent
    oi = snapshot.oi_change_percent
    price_pos = snapshot.price_position_24h
    taker = snapshot.taker_buy_sell_ratio
    funding = snapshot.funding_rate_percent
    global_lsr = snapshot.global_long_short_ratio
    top_position_lsr = snapshot.top_position_ratio
    top_account_lsr = snapshot.top_account_ratio

    if price > 3 and oi > 5:
        add("OI增仓推涨", 2, "positive", "mid", "OI", "主力建仓")
    if price > 8 and oi > 10:
        add("高位增仓追涨", 2, "risk", "mid", "OI", "高位拥挤")
        if price_pos is not None and price_pos > 70:
            add("高位增仓", 1, "risk", "mid", "OI", "高位拥挤")
    if price > 3 and oi < -3:
        add("空头回补推涨", 1, "neutral", "short", "OI", "短线逼空")
    if price < -3 and oi > 5:
        add("跌中增仓承压", 2, "risk", "mid", "OI", "空头建仓")
        if taker is not None and taker < 1:
            add("主动卖盘配合", 1, "risk", "short", "FLOW")
    if price < -3 and oi < -5:
        add("仓位退出/风险释放", 1, "neutral", "short", "OI", "风险释放")

    if snapshot.confirm_price_change_percent is not None and snapshot.confirm_oi_change_percent is not None:
        if snapshot.confirm_price_change_percent > 0 and snapshot.confirm_oi_change_percent > 0:
            add("短线增仓推涨", 1, "positive", "short", "OI")
        if abs(snapshot.confirm_price_change_percent) <= 0.5 and snapshot.confirm_oi_change_percent > 5:
            add("增仓分歧/可能派发", 2, "risk", "short", "OI", "资金分歧")
    if price > 0 and oi >= 8:
        add("中线持续建仓", 2, "positive", "mid", "OI", "主力建仓")
    if price <= 0.5 and oi >= 10:
        add("增仓分歧/可能派发", 2, "risk", "mid", "OI", "资金分歧")

    short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    if taker is not None and taker > 1.15:
        add("主动买盘强", 2, "positive", "short", "FLOW")
    if taker is not None and taker < 0.85:
        add("主动卖盘强", 2, "risk", "short", "FLOW", "主动卖盘")
    if short_flow >= 7:
        add("短线主力流入", 2, "positive", "short", "FLOW")
    if mid_flow >= 7:
        add("中线主力流入", 2, "positive", "mid", "FLOW")
    if long_flow >= 7:
        add("长线主力流入", 2, "positive", "long", "FLOW")
    if short_flow <= 3:
        add("短线资金不支持", 1, "risk", "short", "FLOW")
    if mid_flow <= 3:
        add("中线资金不支持", 2, "risk", "mid", "FLOW")
    if long_flow <= 3:
        add("长线资金不支持", 2, "risk", "long", "FLOW")
    if short_flow >= 7 and mid_flow <= 5:
        add("短强中弱，谨慎追", 1, "risk", "general", "FLOW", "资金分歧")
    if short_flow <= 4 and mid_flow >= 7:
        add("回踩承接观察", 1, "positive", "mid", "FLOW")

    if global_lsr is not None and global_lsr > 1.8:
        add("散户多头拥挤", 1, "risk", "general", "LSR", "高位拥挤")
    if global_lsr is not None and global_lsr < 0.7:
        add("散户空头拥挤", 1, "positive", "general", "LSR", "空头拥挤")
    if top_position_lsr is not None and top_position_lsr > 1.3 and oi > 0:
        add("大户偏多建仓", 2, "positive", "mid", "WHALE", "主力建仓")
    if top_position_lsr is not None and top_position_lsr < 0.8 and oi > 0:
        add("大户偏空建仓", 2, "risk", "mid", "WHALE", "空头建仓")
    if top_account_lsr is not None and top_account_lsr > 1.3 and (top_position_lsr is None or top_position_lsr <= 1.15):
        add("散户多/大户不跟", 1, "risk", "general", "WHALE")
    if top_account_lsr is not None and top_account_lsr < 0.8 and top_position_lsr is not None and top_position_lsr > 1.3:
        add("散户空/大户偏多", 1, "positive", "general", "WHALE")

    if funding is not None:
        if funding > 0.15:
            if oi < 0:
                add("多头风险释放", 1, "neutral", "short", "FUNDING", "风险释放")
            else:
                add("费率极热", 2, "risk", "general", "FUNDING", "高位拥挤")
        elif funding > 0.08:
            if oi < 0:
                add("多头风险释放", 1, "neutral", "short", "FUNDING", "风险释放")
            else:
                add("费率偏热", 1, "risk", "general", "FUNDING", "高位拥挤")
        if funding < -0.15:
            if oi < 0:
                add("空头风险释放", 1, "neutral", "short", "FUNDING", "风险释放")
            else:
                add("空头极度拥挤", 2, "positive", "general", "FUNDING", "空头拥挤")
        elif funding < -0.08:
            if oi < 0:
                add("空头风险释放", 1, "neutral", "short", "FUNDING", "风险释放")
            else:
                add("空头拥挤", 1, "positive", "general", "FUNDING", "空头拥挤")

    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    if basis_label == "明显溢价":
        add("合约溢价偏高", 1, "risk", "general", "BASIS", "高位拥挤")
    if basis_label == "极端溢价":
        add("合约溢价极高", 2, "risk", "general", "BASIS", "高位拥挤")
    if basis_label == "明显贴水":
        add("合约贴水偏深", 1, "positive", "general", "BASIS", "空头拥挤")
    if basis_label == "极端贴水":
        add("合约贴水极深", 2, "positive", "general", "BASIS", "空头拥挤")
    if basis_label in ("明显溢价", "极端溢价") and funding is not None and funding > 0.08 and oi > 0:
        add("合约多头拥挤", 2, "risk", "general", "BASIS", "高位拥挤")
    if basis_label in ("明显贴水", "极端贴水") and funding is not None and funding < -0.08 and oi > 0:
        add("空头挤压条件", 2, "positive", "general", "BASIS", "空头拥挤")

    spot_score, _spot_label, _spot_reason = spot_onchain_score(snapshot, signal)
    absorption_label, _absorption_score, _absorption_reason = spot_absorption_state(snapshot, signal)
    if spot_score >= 7:
        add("现货/链上确认", 2, "positive", "mid", "SPOT", "现货确认")
    if absorption_label in ("现货承接", "链上承接"):
        add("现货承接", 2, "positive", "short", "SPOT", "现货确认")
    if absorption_label in ("现货出货", "链上出货"):
        add("现货/链上出货", 2, "risk", "short", "SPOT", "出货")
    if price_pos is not None and price_pos > 70 and spot_score <= 3:
        add("高位现货未确认", 2, "risk", "mid", "SPOT", "合约先行")
    spot_text = cached_spot_alpha_confirmation(snapshot.symbol) or spot_alpha_confirmation(snapshot.symbol)
    dex_1h_change = dex_period_change(spot_text, "1h")
    dex_24h_change = dex_period_change(spot_text, "24h")
    liquidity = dex_liquidity_usd(spot_text)
    if liquidity is not None and liquidity >= 100000 and price >= -0.3:
        add("流动性增加", 1, "positive", "long", "LIQ")
    if price > 3 and ((dex_1h_change is not None and dex_1h_change < -1) or (dex_24h_change is not None and dex_24h_change < -5)):
        add("流动性下降拉盘", 2, "risk", "long", "LIQ", "出货")

    liq_label = liquidation_risk_label(snapshot)
    squeeze_label, _squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    if "空头强平" in liq_label or "上方扫空" in liq_label:
        add("空头被挤压", 1, "neutral", "short", "LIQ", "短线逼空")
    if "多头强平" in liq_label or "下方扫多" in liq_label:
        add("多头止损释放", 1, "risk", "short", "LIQ")
    if "双向" in liq_label or "洗盘" in liq_label or squeeze_label == "双向挤压":
        add("双向洗盘", 2, "risk", "short", "LIQ", "资金分歧")
    if squeeze_label == "空头挤压" and absorption_label in ("现货承接", "链上承接"):
        add("空头挤压+现货承接", 2, "positive", "short", "LIQ", "现货确认")
    if squeeze_label == "多头挤压" and absorption_label in ("现货出货", "链上出货"):
        add("多头挤压+现货出货", 2, "risk", "short", "LIQ", "出货")

    positive_points = sum(item.points for item in items if item.polarity == "positive")
    risk_points = sum(item.points for item in items if item.polarity == "risk")
    total_score = positive_points - risk_points
    if total_score >= 4:
        direction_hint = "看多"
    elif risk_points - positive_points >= 4:
        direction_hint = "看空/风险"
    else:
        direction_hint = "观察"

    note_set = set(notes)
    if flow_label == "短强中弱":
        summary = "短线强，中线不支持，谨慎追"
    elif flow_label == "中长线派发":
        summary = "中长线资金不支持，只能观察"
    elif {"高位拥挤", "出货"} & note_set:
        summary = "高位拥挤，注意出货"
    elif "合约先行" in note_set:
        summary = "合约先行，现货未跟"
    elif "主力建仓" in note_set and "现货确认" in note_set:
        summary = "主力建仓，现货确认"
    elif "空头拥挤" in note_set:
        summary = "空头拥挤，等待逼空确认"
    elif flow_label == "资金分歧" or "资金分歧" in note_set:
        summary = "资金分歧，观望"
    elif "主力建仓" in note_set:
        summary = "主力建仓，等待现货确认"
    elif "现货确认" in note_set:
        summary = "现货确认，资金等待共振"
    elif "风险释放" in note_set:
        summary = "风险释放，不急追单"
    else:
        summary = "资金分歧，观望" if items else "证据不足，观察"
    return total_score, direction_hint, summary, items[:20]


def conviction_label(score: int) -> str:
    if score >= 80:
        return "高"
    if score >= 65:
        return "中高"
    if score >= 50:
        return "中低"
    return "低"


BULLISH_STRUCTURE_KINDS = {"discovery", "hot_breakout", "bottom_reversal", "main_trend_watch", "main_momentum_watch"}
BREAKOUT_STRUCTURE_KINDS = {"discovery", "hot_breakout", "main_trend_watch"}
RISK_STRUCTURE_KINDS = {"top_risk", "top_exhaustion", "distribution", "crowded_top_risk", "main_risk_watch"}
TOPQ_BULLISH_KINDS = BULLISH_STRUCTURE_KINDS | {"main_trend_watch"}
TOPQ_RISK_KINDS = RISK_STRUCTURE_KINDS | {"main_risk_watch"}
PREMIUM_BASIS_STATES = {"明显溢价", "极端溢价"}
BAD_LONG_ENTRY_LABELS = {"追高风险", "不宜追", "下跌中继"}
TOPQ_BAD_LONG_EVIDENCE_KEYWORDS = ("高位拥挤", "出货", "派发", "禁止追多", "不追")
TOPQ_RISK_EVIDENCE_KEYWORDS = ("高位拥挤", "出货", "派发", "追多风险", "多头过热", "顶部风险", "短线强，中线不支持", "中长线资金不支持")
TOPQ_WEAK_FLOW_LABELS = {"资金分歧", "短强中弱", "短弱中强", "中长线派发"}
MAIN_MOMENTUM_DOWNGRADE_TEXT = "短线异动增强，等待中长周期确认，不追高"
MAIN_MOMENTUM_TOPQ_TEXT = "短线异动，等确认"
MAIN_MOMENTUM_DOWNGRADE_FLOW_KEYWORDS = ("资金分歧", "短强中弱", "短弱中强", "中长线派发")
DISCORD_BULLISH_DOWNGRADE_TEXT = "短线信号较强，但中长周期未确认，等待回踩/放量确认，不追高"
DISCORD_BULLISH_DOWNGRADE_NOTE = "展示降级：资金分歧，观察为主"
DISCORD_BULLISH_DOWNGRADE_EVIDENCE_KEYWORDS = ("高位拥挤", "注意出货", "不宜追", "谨慎追")
DISCORD_RISK_KINDS = {"top_exhaustion", "top_risk", "distribution", "main_risk_watch"}
DISCORD_BULLISH_KINDS = {"discovery", "hot_breakout", "bottom_reversal", "main_momentum_watch", "main_trend_watch"}
DISCORD_BEARISH_PRICE_ACTION_PATTERNS = ("长上影", "阴包阳", "放量滞涨", "日线压力", "周线压力", "跌破结构", "跌破近20根低点")
PRICE_ACTION_INTERVALS = ("5m", "15m", "1h", "4h", "1d", "3d", "1w")
_PRICE_ACTION_KLINE_CACHE: dict[tuple[str, str], tuple[float, list[Any]]] = {}


def text_has_any(text: str | None, keywords: tuple[str, ...]) -> bool:
    value = str(text or "")
    return any(keyword in value for keyword in keywords)


def positive_flow_count(snapshot: MarketSnapshot, periods: tuple[str, ...]) -> int:
    return sum(1 for period in periods if summary_flow_value(snapshot, period) > 0)


def main_momentum_hard_downgrade(snapshot: MarketSnapshot) -> bool:
    _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    return (
        long_flow_alignment_score(snapshot) <= 3
        or text_has_any(flow_label, MAIN_MOMENTUM_DOWNGRADE_FLOW_KEYWORDS)
        or positive_flow_count(snapshot, ("4h", "12h", "72h")) < 2
    )


def main_momentum_strong_buy_allowed(snapshot: MarketSnapshot, signal: Signal | None = None) -> bool:
    if signal is None or signal.kind != "main_momentum_watch":
        return False
    if main_momentum_hard_downgrade(snapshot):
        return False
    _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    leading = leading_signal_score(snapshot, signal)
    ev_score, _ev_direction, _ev_summary, _ev_items = evidence_score(snapshot, signal)
    return (
        leading.leading_score >= 6
        and ev_score >= 8
        and long_flow_alignment_score(snapshot) >= 5
        and summary_flow_value(snapshot, "15m") > 0
        and summary_flow_value(snapshot, "1h") > 0
        and not text_has_any(flow_label, MAIN_MOMENTUM_DOWNGRADE_FLOW_KEYWORDS)
    )


def price_action_discord_downgrade(price_action: MultiTimeframePriceAction | None) -> bool:
    if price_action is None:
        return False
    return (
        price_action.score <= 4
        or price_action.long_score <= 3
        or any(text_has_any(pattern, DISCORD_BEARISH_PRICE_ACTION_PATTERNS) for pattern in price_action.patterns)
        or text_has_any(price_action.label, ("大周期未确认", "压力位附近", "追高风险"))
        or any(text_has_any(item, ("压力位附近", "追高风险")) for item in price_action.risk_items)
    )


def price_action_structure_risk_confirmed(price_action: MultiTimeframePriceAction | None) -> bool:
    if price_action is None:
        return False
    return (
        any(text_has_any(pattern, DISCORD_BEARISH_PRICE_ACTION_PATTERNS) for pattern in price_action.patterns)
        or (price_action.direction == "bearish" and (price_action.score >= 6 or 10 - price_action.score >= 6))
    )


def price_action_allows_discord_high_buy(price_action: MultiTimeframePriceAction | None) -> bool:
    return (
        price_action is not None
        and price_action.score >= 6
        and price_action.mid_score >= 6
        and price_action.long_score >= 5
        and not any(text_has_any(pattern, DISCORD_BEARISH_PRICE_ACTION_PATTERNS) for pattern in price_action.patterns)
    )


def discord_bullish_display_downgrade(
    signal: Signal,
    evidence_summary: str | None = None,
    price_action: MultiTimeframePriceAction | None = None,
) -> bool:
    snapshot = signal.snapshot
    if snapshot is None:
        return False
    direction = signal_direction_label(signal.kind)
    if direction != "看多" and not is_bullish_structure_kind(signal.kind):
        return False
    _short_flow, _mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    summary = evidence_summary
    if summary is None:
        _ev_score, _ev_direction, summary, _ev_items = evidence_score(snapshot, signal)
    return (
        long_flow_alignment_score(snapshot) <= 3
        or text_has_any(flow_label, MAIN_MOMENTUM_DOWNGRADE_FLOW_KEYWORDS)
        or positive_flow_count(snapshot, ("4h", "12h", "24h", "72h")) < 2
        or text_has_any(summary, DISCORD_BULLISH_DOWNGRADE_EVIDENCE_KEYWORDS)
        or price_action_discord_downgrade(price_action)
    )


def topq_discord_bullish_display_downgrade(row: dict[str, str]) -> bool:
    kind = topq_kind_normalized(row.get("kind"))
    direction = signal_direction_label(kind)
    if direction != "看多" and not topq_is_bullish_candidate(kind):
        return False
    flow_label = str(row.get("flow_trend_label") or "")
    long_flow_alignment = parse_float(row.get("long_flow_alignment_score"))
    if long_flow_alignment is None:
        long_flow_alignment = parse_float(row.get("long_flow_score"))
    positives = sum(
        1
        for key in ("net_flow_4h_usd", "net_flow_12h_usd", "net_flow_24h_usd", "net_flow_72h_usd")
        if (parse_float(row.get(key)) or 0) > 0
    )
    evidence_summary = str(row.get("evidence_summary") or "")
    return (
        (long_flow_alignment is not None and long_flow_alignment <= 3)
        or text_has_any(flow_label, MAIN_MOMENTUM_DOWNGRADE_FLOW_KEYWORDS)
        or positives < 2
        or text_has_any(evidence_summary, DISCORD_BULLISH_DOWNGRADE_EVIDENCE_KEYWORDS)
    )


def topq_main_momentum_hard_downgrade(row: dict[str, str]) -> bool:
    flow_label = str(row.get("flow_trend_label") or "")
    long_flow_alignment = parse_float(row.get("long_flow_alignment_score"))
    if long_flow_alignment is None:
        long_flow_alignment = parse_float(row.get("long_flow_score"))
    positives = sum(
        1
        for key in ("net_flow_4h_usd", "net_flow_12h_usd", "net_flow_72h_usd")
        if (parse_float(row.get(key)) or 0) > 0
    )
    return (
        (long_flow_alignment is not None and long_flow_alignment <= 3)
        or text_has_any(flow_label, MAIN_MOMENTUM_DOWNGRADE_FLOW_KEYWORDS)
        or positives < 2
    )


def topq_kind_normalized(kind: str | None) -> str:
    return str(kind or "").strip().lower().replace("-", "_").replace(" ", "_")


def topq_is_bullish_candidate(kind: str | None) -> bool:
    normalized = topq_kind_normalized(kind)
    return normalized in TOPQ_BULLISH_KINDS or signal_direction_label(normalized) == "看多"


def topq_is_risk_candidate(kind: str | None) -> bool:
    normalized = topq_kind_normalized(kind)
    return normalized in TOPQ_RISK_KINDS or is_risk_structure_kind(normalized)


def topq_risk_action(evidence_summary: str, basis_label: str) -> str:
    if (
        "高位拥挤" in evidence_summary
        or basis_label in PREMIUM_BASIS_STATES
        or any(key in evidence_summary for key in ("出货", "多头过热", "顶部风险", "派发"))
    ):
        return "减仓/避险，等回落确认"
    return "风险观察，不追多"


def topq_strong_buy_allowed(row: dict[str, str], conviction: float, leading_score: float, evidence_score: float) -> bool:
    flow_label = str(row.get("flow_trend_label") or "")
    evidence_summary = str(row.get("evidence_summary") or "")
    long_flow_alignment = parse_float(row.get("long_flow_alignment_score")) or 0
    flow_15m = parse_float(row.get("net_flow_15m_usd")) or 0
    flow_1h = parse_float(row.get("net_flow_1h_usd")) or 0
    return (
        conviction >= 80
        and leading_score >= 6
        and evidence_score >= 8
        and long_flow_alignment >= 5
        and flow_15m > 0
        and flow_1h > 0
        and flow_label not in TOPQ_WEAK_FLOW_LABELS
        and not text_has_any(evidence_summary, ("高位拥挤", "出货", "派发"))
    )


def structural_signal_kind(signal: Signal | None) -> str:
    return signal.kind if signal else ""


def is_risk_structure_kind(kind: str | None) -> bool:
    return kind in RISK_STRUCTURE_KINDS or signal_direction_label(kind) == "看空"


def is_bullish_structure_kind(kind: str | None) -> bool:
    return kind in BULLISH_STRUCTURE_KINDS


def breakout_allows_high_conviction(
    kind: str | None,
    entry_label: str,
    basis_label: str,
    squeeze_label: str,
    flow_label: str,
) -> bool:
    if kind not in BREAKOUT_STRUCTURE_KINDS:
        return True
    return (
        ("启动前" in entry_label or "启动初期" in entry_label)
        and basis_label not in PREMIUM_BASIS_STATES
        and squeeze_label != "双向挤压"
        and flow_label != "短强中弱"
    )


def structural_action_override(
    kind: str | None,
    basis_label: str,
    squeeze_label: str,
    price_change_pct: float | None,
    oi_change_pct: float | None,
    price_position_24h: float | None,
) -> tuple[str | None, str | None]:
    bullish = is_bullish_structure_kind(kind)
    risk = is_risk_structure_kind(kind)
    price_change = price_change_pct if price_change_pct is not None else 0.0
    oi_change = oi_change_pct if oi_change_pct is not None else 0.0
    if bullish and squeeze_label == "双向挤压":
        return "双向爆仓洗盘，先等稳定", "双向挤压"
    if bullish and basis_label in PREMIUM_BASIS_STATES:
        return "合约溢价偏高，禁止追多", "明显溢价"
    if bullish and price_change > 8 and oi_change > 10:
        return "已拉升，不追", "高位拉升"
    if risk and price_position_24h is not None and price_position_24h < 40:
        return "看空但不追空", "低位不追空"
    if risk and basis_label in PREMIUM_BASIS_STATES and price_change > 5 and oi_change > 8:
        return "建议减仓/避险", "溢价拉升风控"
    return None, None


def conviction_score(snapshot: MarketSnapshot, signal: Signal | None = None) -> tuple[int, str, str]:
    kind = structural_signal_kind(signal)
    pos_label, pos_score, _pos_reason = position_behavior(snapshot, signal)
    short_flow, mid_flow, long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    spot_label, spot_score, _spot_reason = spot_absorption_state(snapshot, signal)
    squeeze_label, squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    intent_label, intent_score, _intent_reason = market_intent(snapshot, signal)
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    entry_score, entry_label, _entry_reason = entry_timing_score(snapshot, signal) if signal else (5, "观察", "中性信号")
    trap_score, _trap_label, _trap_reason = trap_risk_score(snapshot, signal)

    position_points = pos_score / 10 * 25
    flow_points = (short_flow * 0.25 + mid_flow * 0.45 + long_flow * 0.30) / 10 * 20
    if spot_label in ("现货承接", "链上承接"):
        spot_points = spot_score / 10 * 15
    elif spot_label in ("现货出货", "链上出货"):
        spot_points = 3 if intent_label in ("高位出货", "多杀多风险") else 6
    elif spot_label == "现货未跟":
        spot_points = 7
    else:
        spot_points = 7.5
    squeeze_points = squeeze_score / 10 * 15
    if squeeze_label == "无明显挤压":
        squeeze_points = 6
    stage_points = entry_score / 10 * 15
    risk_points = max(0, 10 - trap_score)

    score = position_points + flow_points + spot_points + squeeze_points + stage_points + risk_points
    reasons: list[str] = []

    def adjust(points: float, reason: str) -> None:
        nonlocal score
        score += points
        reasons.append(reason)

    def cap(max_score: int, reason: str) -> None:
        nonlocal score
        score = min(score, max_score)
        reasons.append(reason)

    if pos_label == "多头主动建仓" and mid_flow >= 5 and snapshot.funding_rate_percent is not None and snapshot.funding_rate_percent <= 0.05:
        adjust(3, "主力建仓")
    if flow_label in ("多周期共振流入", "中长线吸筹"):
        adjust(5, "中线资金支持")
    if spot_label in ("现货承接", "链上承接"):
        adjust(5, "现货承接")
    if intent_label == "合约先行观察":
        score = min(score, 78)
        reasons.append("合约先行")
    if snapshot.price_change_percent > 0 and snapshot.oi_change_percent < 0:
        adjust(-12, "空头回补非启动")
    if snapshot.price_change_percent < 0 and snapshot.oi_change_percent < 0:
        adjust(-8, "风险释放不追空")
    if intent_label in ("高位出货", "多杀多风险", "下跌中继") or pos_label == "双向加杠杆":
        adjust(-10, intent_label)
    if "极端" in basis_label:
        adjust(-8, "极端基差")
    if snapshot.price_position_24h is not None and snapshot.price_position_24h >= 85 and snapshot.price_change_percent > 5:
        adjust(-8, "高位追涨")
    if intent_label == "短线逼空" and spot_label in ("现货承接", "链上承接") and basis_pct is not None and basis_pct < 0:
        adjust(5, "逼空反弹")
    if pos_label == "高位多头拥挤" and spot_label in ("现货出货", "链上出货"):
        adjust(8, "风控确认")

    evidence_total, evidence_direction, evidence_summary, evidence_items = evidence_score(snapshot, signal)
    evidence_display = evidence_display_score(evidence_direction, evidence_items, evidence_total)
    evidence_positive = sum(item.points for item in evidence_items if item.polarity == "positive")
    evidence_risk = sum(item.points for item in evidence_items if item.polarity == "risk")
    signal_direction = signal_direction_label(signal.kind if signal else None)
    risk_signal = signal_direction == "看空" or is_risk_structure_kind(kind)
    if evidence_positive >= 8 and evidence_risk <= 4:
        adjust(15, "证据强多")
    elif evidence_positive >= 5:
        adjust(8, "证据偏多")
    if evidence_risk >= 8:
        adjust(10 if risk_signal else -15, "证据风险")
    if "高位拥挤" in evidence_summary or "出货" in evidence_summary:
        adjust(10 if risk_signal else -15, "高位拥挤/出货")
    if "主力建仓" in evidence_summary or "现货确认" in evidence_summary:
        adjust(4 if not risk_signal else -8, "主力建仓/现货确认")
    if "资金分歧" in evidence_summary:
        adjust(-8, "资金分歧")
    if evidence_total <= -5 and not risk_signal:
        score = min(score, 72)

    leading = leading_signal_score(snapshot, signal)
    leading_observation = any(key in leading.leading_label for key in ("资金分歧", "短线强", "派发", "观察"))
    if leading.leading_score >= 3:
        if leading.leading_direction == "long" and not risk_signal and not leading_observation:
            adjust(min(12, leading.leading_score * 1.5), f"领先信号:{leading.leading_label}")
        elif leading.leading_direction == "short" and risk_signal:
            adjust(min(12, leading.leading_score * 1.5), f"领先信号:{leading.leading_label}")
        elif leading.leading_direction == "long" and risk_signal:
            adjust(-12, "信号方向与主力证据冲突，观察为主")
            cap(64, "信号方向与主力证据冲突，观察为主")
        elif leading.leading_direction == "short" and not risk_signal:
            adjust(-15, "信号方向与主力证据冲突，观察为主")
            cap(60, "信号方向与主力证据冲突，观察为主")
        else:
            adjust(-3, "领先信号分歧")

    bullish_context = (
        not risk_signal
        or signal_direction == "看多"
        or leading.leading_direction == "long"
        or intent_label in ("真启动观察", "合约先行观察", "短线逼空")
        or pos_label == "多头主动建仓"
    )

    bullish_signal = is_bullish_structure_kind(kind)
    if bullish_signal and basis_label in PREMIUM_BASIS_STATES:
        cap(64, "合约溢价偏高，禁止追多")
    if bullish_signal and squeeze_label == "双向挤压":
        cap(54, "双向爆仓洗盘，先等稳定")
    if bullish_signal and flow_label == "短强中弱":
        adjust(-15, "短强中弱/短线强，中线不支持")
        cap(64, "短强中弱/短线强，中线不支持")
    if flow_label == "短强中弱" and bullish_context:
        cap(64, "短线强，中线不支持，谨慎追")
    if flow_label == "中长线派发" and bullish_context:
        cap(60, "中长线资金不支持，只能观察")
    if evidence_display <= 2 and leading_observation:
        cap(64, "证据不足，观察信号不升高把握")
    if intent_label == "短线逼空" and evidence_display <= 2:
        cap(64, "短线逼空观察，不追")
    if bullish_signal and entry_label in BAD_LONG_ENTRY_LABELS:
        cap(54, entry_label)
    if bullish_signal and snapshot.price_change_percent > 8 and snapshot.oi_change_percent > 10:
        cap(60, "已拉升，不追")
    if kind in BREAKOUT_STRUCTURE_KINDS and not breakout_allows_high_conviction(kind, entry_label, basis_label, squeeze_label, flow_label):
        cap(69, "非启动前/初期")

    if kind in ("top_risk", "top_exhaustion") and basis_label in PREMIUM_BASIS_STATES and snapshot.price_change_percent > 5 and snapshot.oi_change_percent > 8:
        adjust(8, "溢价拉升风控")
    if risk_signal and snapshot.price_position_24h is not None and snapshot.price_position_24h < 40:
        cap(64, "看空但不追空")
    if intent_label == "震荡分歧":
        cap(74 if risk_signal else 64, "震荡分歧")
    if kind == "main_momentum_watch" and main_momentum_hard_downgrade(snapshot):
        cap(69, MAIN_MOMENTUM_DOWNGRADE_TEXT)

    final_score = clamp_int(score, 0, 100)
    if not reasons:
        reasons = [pos_label, flow_label, intent_label]
    priority_reason_keys = (
        "合约溢价偏高，禁止追多",
        "双向爆仓洗盘，先等稳定",
        "短强中弱",
        "短线强，中线不支持",
        "已拉升，不追",
        "看空但不追空",
        "震荡分歧",
    )
    priority_reasons = [reason for reason in reasons if any(key in reason for key in priority_reason_keys)]
    reason = "/".join(dict.fromkeys((priority_reasons + reasons)[:5]))
    return final_score, conviction_label(final_score), reason


def format_conviction_model_lines(snapshot: MarketSnapshot, signal: Signal | None = None) -> list[str]:
    basis_pct, basis_label, basis_reason = basis_state(snapshot)
    short_flow, mid_flow, long_flow, flow_label, flow_reason = flow_horizon_scores(snapshot)
    pos_label, pos_score, pos_reason = position_behavior(snapshot, signal)
    squeeze_label, squeeze_score, squeeze_reason = squeeze_state(snapshot)
    spot_label, spot_score, spot_reason = spot_absorption_state(snapshot, signal)
    intent_label, intent_score, intent_reason = market_intent(snapshot, signal)
    conv_score, conv_label, conv_reason = conviction_score(snapshot, signal)
    basis_text = "n/a" if basis_pct is None else f"{basis_pct:+.2f}%"
    flow_display_reason = flow_reason
    if flow_label == "短强中弱":
        flow_display_reason = f"{flow_reason}；短线强，中线不支持，谨慎追"
    elif flow_label == "中长线派发":
        flow_display_reason = f"{flow_reason}；中长线资金不支持，只能观察"
    return [
        f"把握性: {conv_score}/100 {conv_label} - {conv_reason}",
        f"主力行为: {pos_label} {pos_score}/10 - {pos_reason}",
        f"资金周期: 短{short_flow}/10 中{mid_flow}/10 长{long_flow}/10 - {flow_label} ({flow_display_reason})",
        f"挤压: {squeeze_label} {squeeze_score}/10 - {squeeze_reason}",
        f"现货/链上: {spot_label} {spot_score}/10 - {spot_reason}",
        f"基差: {basis_text} {basis_label} - {basis_reason}",
        f"意图: {intent_label} {intent_score}/10 - {intent_reason}",
    ]


def format_conviction_push_lines(snapshot: MarketSnapshot, signal: Signal | None = None) -> list[str]:
    _basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    short_flow, mid_flow, long_flow, _flow_label, _flow_reason = flow_horizon_scores(snapshot)
    pos_label, _pos_score, _pos_reason = position_behavior(snapshot, signal)
    intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
    conv_score, conv_label, _conv_reason = conviction_score(snapshot, signal)
    return [
        f"把握性: {conv_score}/100 {conv_label} | 意图: {intent_label}",
        f"主力: {pos_label} | 资金: 短{short_flow} 中{mid_flow} 长{long_flow} | 基差: {basis_label}",
    ]


def flow_score_icon(score: int) -> str:
    if score >= 7:
        return "🟢"
    if score >= 4:
        return "🟡"
    return "🔴"


def flow_value_icon(value: float | None) -> str:
    if value is None:
        return ""
    if value > 0:
        return " 🟢"
    if value < 0:
        return " 🔴"
    return " ⚪"


def flow_value_text(snapshot: MarketSnapshot, period: str) -> str:
    value = snapshot.net_flow_usd.get(period)
    return f"{period} {format_usd(value)}{flow_value_icon(value)}"


def trader_action_from_intent(intent_label: str, conviction: int) -> str:
    action, _reason = action_from_intent_conviction(intent_label, conviction)
    return action


def action_from_intent_conviction(intent_label: str, conviction: int) -> tuple[str, str]:
    return action_from_trade_context(intent_label, conviction)


def action_from_trade_context(
    intent_label: str,
    conviction: int,
    direction: str | None = None,
    position_label: str | None = None,
) -> tuple[str, str]:
    if conviction >= 80 and intent_label == "真启动观察":
        return "强烈建议关注买入", "启动把握高"
    if conviction >= 65 and intent_label in ("真启动观察", "合约先行观察"):
        return "建议观察，等确认入场", "启动观察但需确认"
    if conviction >= 65 and direction == "看多" and position_label == "多头主动建仓":
        return "建议观察，等确认入场", "多头主动建仓但需确认"
    if intent_label == "高位出货":
        return "建议减仓/避险", "高位出货风险"
    if intent_label == "多杀多风险":
        return "建议减仓/避险", "多头挤压风险"
    if intent_label == "短线逼空":
        return "禁止追多，等回踩", "逼空后追价风险高"
    if intent_label == "洗盘回踩":
        return "建议观察，等确认入场", "等待承接确认"
    if intent_label == "下跌中继":
        return "禁止抄底，等待止跌", "下跌延续风险"
    if intent_label == "风险释放":
        return "关注反弹机会", "去杠杆后不追空"
    if direction == "看空" and conviction >= 65:
        return "建议减仓/避险", "风险信号优先"
    if direction == "看多" and conviction >= 70:
        return "建议观察，等确认入场", "多头信号待确认"
    if conviction < 50:
        return "信号不足，继续盯盘", "把握不足"
    return "建议观察，等确认入场", "确认不足"


def action_label(snapshot: MarketSnapshot, signal: Signal | None = None) -> tuple[str, str]:
    conviction, _conv_label, _conv_reason = conviction_score(snapshot, signal)
    intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
    position_label, _position_score, _position_reason = position_behavior(snapshot, signal)
    direction = signal_direction_label(signal.kind) if signal else None
    _basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    squeeze_label, _squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    ev_total, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    ev_display = evidence_display_score(ev_direction, ev_items, ev_total)
    if is_telegram_risk_signal(signal):
        return risk_signal_action_label(conviction, intent_label, ev_summary, ev_display, basis_label)
    if signal and signal.kind == "main_momentum_watch":
        if main_momentum_hard_downgrade(snapshot):
            return MAIN_MOMENTUM_DOWNGRADE_TEXT, "主流异动雷达降级，等待中长周期确认"
        if main_momentum_strong_buy_allowed(snapshot, signal) and conviction >= 80 and intent_label == "真启动观察":
            return "强烈建议关注买入", "启动把握高"
        return "短线异动观察，等待确认，不追高", "主流异动雷达只做观察提醒"
    override, override_reason = structural_action_override(
        signal.kind if signal else None,
        basis_label,
        squeeze_label,
        snapshot.price_change_percent,
        snapshot.oi_change_percent,
        snapshot.price_position_24h,
    )
    if override:
        return override, override_reason or "结构限制"
    if intent_label == "短线逼空" and ev_display <= 2:
        return "短线逼空观察，不追", "证据不足"
    return action_from_trade_context(intent_label, conviction, direction, position_label)


def flow_trend_short_label(label: str) -> str:
    mapping = {
        "多周期共振流入": "多周期流入",
        "短强中弱": "短线强，中线不支持",
        "短弱中强": "短弱中强",
        "中长线吸筹": "中长吸筹",
        "中长线派发": "中长派发",
        "全周期流出": "全周期流出",
        "资金分歧": "资金分歧",
    }
    return mapping.get(label, label)


def flow_trader_key_reasons(
    snapshot: MarketSnapshot,
    signal: Signal | None,
    pos_label: str,
    squeeze_label: str,
    spot_label: str,
    intent_label: str,
    flow_label: str,
    basis_label: str,
) -> list[str]:
    reasons: list[str] = []
    funding = snapshot.funding_rate_percent
    if funding is not None and funding <= -0.08 and snapshot.oi_change_percent < 0:
        reasons.append("Funding极端负，但 OI下降，偏风险释放")
    elif funding is not None and funding <= -0.08:
        reasons.append("Funding极端负，若价格抗跌需防空头挤压")
    elif funding is not None and funding >= 0.08:
        reasons.append("Funding极端正，追多性价比下降")

    if spot_label in ("现货未跟", "数据不足", "承接不明"):
        reasons.append("现货/链上未充分确认，合约先行不确认")
    elif spot_label in ("现货出货", "链上出货"):
        reasons.append("现货/链上转弱，存在出货风险")
    elif spot_label in ("现货承接", "链上承接"):
        reasons.append("现货/链上承接，给合约方向加分")

    short_score, mid_score, long_score, _label, _reason = flow_horizon_scores(snapshot)
    if mid_score <= 4 and long_score <= 5:
        reasons.append("中长资金仍弱，不追多")
    elif mid_score >= 7:
        reasons.append("中线资金支持，回踩更值得跟踪")

    if pos_label == "空头回补/逼空":
        reasons.append("价涨但 OI下降，更像回补而非健康启动")
    elif pos_label == "仓位退出/风险释放":
        reasons.append("价跌且 OI下降，偏去杠杆风险释放")
    elif pos_label in ("多头主动建仓", "空头主动建仓"):
        reasons.append(pos_label)

    if "极端" in basis_label:
        reasons.append("基差极端，避免追价")
    elif squeeze_label != "无明显挤压":
        reasons.append(squeeze_label)
    if intent_label in ("高位出货", "多杀多风险", "下跌中继"):
        reasons.append(intent_label)
    if not reasons:
        reasons.append(flow_label)
    return list(dict.fromkeys(reasons))[:4]


def realtime_trader_reason_labels(snapshot: MarketSnapshot, signal: Signal | None = None) -> str:
    pos_label, _pos_score, _pos_reason = position_behavior(snapshot, signal)
    squeeze_label, _squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    spot_label, _spot_score, _spot_reason = spot_absorption_state(snapshot, signal)
    intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
    _basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    short_score, mid_score, long_score, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    labels: list[str] = []

    if snapshot.price_position_24h is not None and snapshot.price_position_24h >= 75 and snapshot.oi_change_percent > 0:
        labels.append("高位增仓")
    if snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio < 1:
        labels.append("主买转弱")
    if spot_label in ("现货出货", "链上出货"):
        labels.append("现货转弱")
    elif spot_label in ("现货未跟", "承接不明", "数据不足"):
        labels.append("现货未确认")
    elif spot_label in ("现货承接", "链上承接"):
        labels.append("现货承接")
    if pos_label in ("高位多头拥挤", "低位空头拥挤", "多头主动建仓", "空头主动建仓", "空头回补/逼空"):
        labels.append(pos_label)
    if squeeze_label != "无明显挤压":
        labels.append(squeeze_label)
    if mid_score <= 4 and long_score <= 5:
        labels.append("中长资金弱")
    elif flow_label != "资金分歧" or short_score >= 7:
        labels.append(flow_trend_short_label(flow_label))
    if flow_label == "短强中弱":
        labels.append("短线强，中线不支持")
    if "极端" in basis_label:
        labels.append("基差极端")
    if intent_label in ("高位出货", "多杀多风险", "下跌中继", "风险释放"):
        labels.append(intent_label)
    if not labels:
        labels.append(flow_label)
    return " / ".join(dict.fromkeys(labels[:4]))


def compact_realtime_trade_plan(signal: Signal) -> str:
    plan = compact_trade_plan(signal, 90)
    if "暂无交易计划" in plan:
        return ""
    if "\n" in plan or "...已精简" in plan:
        return ""
    if not any(key in plan for key in ("入场", "止损", "TP1")):
        return ""
    if len(plan) > 90:
        return ""
    return plan


def format_flow_trader_view(snapshot: MarketSnapshot, signal: Signal | None = None) -> str:
    basis_pct, basis_label, _basis_reason = basis_state(snapshot)
    short_score, mid_score, long_score, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    pos_label, pos_score, _pos_reason = position_behavior(snapshot, signal)
    squeeze_label, squeeze_score, _squeeze_reason = squeeze_state(snapshot)
    spot_label, spot_score, _spot_reason = spot_absorption_state(snapshot, signal)
    intent_label, intent_score, _intent_reason = market_intent(snapshot, signal)
    conviction, conv_label, _conv_reason = conviction_score(snapshot, signal)
    action, _action_reason = action_label(snapshot, signal)
    basis_text = "n/a" if basis_pct is None else f"{basis_pct:+.2f}%"
    reasons = flow_trader_key_reasons(
        snapshot,
        signal,
        pos_label,
        squeeze_label,
        spot_label,
        intent_label,
        flow_label,
        basis_label,
    )
    lines = [
        f"🧭 {snapshot.symbol} 交易员视图",
        f"结论: {intent_label} | 把握 {conviction}/100 {conv_label}",
        f"动作: {action}",
        "",
        f"主力行为: {pos_label} {pos_score}/10",
        f"挤压状态: {squeeze_label} {squeeze_score}/10",
        f"市场意图: {intent_label} {intent_score}/10",
        f"基差: {basis_text} {basis_label}",
        f"现货/链上: {spot_label} {spot_score}/10",
        "",
        "资金周期:",
        f"短线 5m/15m/1h: {flow_score_icon(short_score)} {short_score}/10",
        f"中线 4h/12h/24h: {flow_score_icon(mid_score)} {mid_score}/10",
        f"长线 72h/144h: {flow_score_icon(long_score)} {long_score}/10",
        f"趋势: {flow_trend_short_label(flow_label)}",
        "",
        "主力净流:",
        " | ".join(flow_value_text(snapshot, period) for period in ("5m", "15m", "1h")),
        " | ".join(flow_value_text(snapshot, period) for period in ("4h", "12h", "24h")),
        " | ".join(flow_value_text(snapshot, period) for period in ("72h", "144h")),
        "",
        "合约结构:",
        (
            f"价格 {snapshot.price_change_percent:+.2f}% | OI {snapshot.oi_change_percent:+.2f}% | "
            f"主买 {format_optional_value(snapshot.taker_buy_sell_ratio)} | "
            f"费率 {format_optional_value(snapshot.funding_rate_percent)}%"
        ),
        (
            f"多空 {format_optional_value(snapshot.global_long_short_ratio)} | "
            f"大户持仓 {format_optional_value(snapshot.top_position_ratio)} | "
            f"大户账户 {format_optional_value(snapshot.top_account_ratio)}"
        ),
        "",
        "关键依据:",
    ]
    lines.extend(f"- {reason}" for reason in reasons)
    return telegram_text("\n".join(lines))




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

    bullish = signal.kind in ("discovery", "bottom_reversal", "main_trend_watch")
    bearish = signal.kind in ("top_risk", "distribution", "top_exhaustion", "main_risk_watch")
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

    if signal.kind in ("discovery", "hot_breakout", "main_trend_watch"):
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
    elif signal.kind in ("top_risk", "distribution", "main_risk_watch"):
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
    entry_score, entry_label, _entry_reason = entry_timing_score(snapshot, signal, trap_score_override=trap_score)
    spot_score, _spot_label, _spot_reason = spot_onchain_score(snapshot, signal)
    _div_label, div_score, _div_reason = contract_spot_divergence(snapshot, signal)
    major_score, _major_label, _major_reason = major_flow_score(snapshot, signal)

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
    if spot_score >= 7:
        add(6, "现货确认强")

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
    if spot_score <= 3:
        add(-10, "现货不确认")
    if div_score >= 6:
        add(-12, "合约现货背离")
    if major_score >= 7:
        add(6, "主力趋势支持")
    if major_score <= 3:
        add(-8, "主力趋势不支持")
    if signal.kind in ("hot_breakout", "discovery", "main_trend_watch") and long_flow_score <= 3:
        add(-15, "breakout without long-flow support")
    if signal.kind in ("discovery", "hot_breakout", "early_breakout", "main_trend_watch"):
        if snapshot.price_change_percent > 8 and snapshot.oi_change_percent > 10:
            add(-25, "追高风险")
        if snapshot.price_position_24h is not None and snapshot.price_position_24h > 75:
            add(-20, "高位风险")
        if snapshot.price_change_percent > 8 and long_flow_score <= 6:
            add(-15, "拉升缺长线确认")
        if "追高风险" in entry_label:
            add(-25, "阶段追高")
    if signal.kind == "bottom_reversal" and ((snapshot.taker_buy_sell_ratio is not None and snapshot.taker_buy_sell_ratio < 1) or summary_flow_value(snapshot, "1h") < 0):
        add(-12, "bottom_reversal weak taker or 1h flow")
    if signal.kind in ("top_risk", "top_exhaustion", "main_risk_watch") and snapshot.price_position_24h is not None and snapshot.price_position_24h < 40:
        add(-10, "top signal below 40% 24h position")
    if "双向强平" in signal.message or "剧烈洗盘" in signal.message or "双向高波动" in liquidation_risk_label(snapshot):
        add(-10, "two-way liquidation/wash risk")

    quality_score = max(0, min(100, int(round(quality_score))))
    priority = priority_from_quality_score(quality_score)
    capped_priority = capped_signal_priority(
        priority,
        signal,
        snapshot,
        quality_score,
        trap_score,
        flow_score,
        long_flow_score,
        entry_score,
        entry_label,
    )
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
    entry_score: int,
    entry_label: str,
) -> str:
    order = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 0}
    reverse = {value: key for key, value in order.items()}
    max_priority = priority

    if trap_score >= 8 and not is_major_asset_tier(snapshot.symbol):
        max_priority = min_priority_cap(max_priority, "B", order, reverse)
    trade_plan = signal_trade_plan(signal)
    if signal.kind in ("discovery", "hot_breakout", "main_trend_watch") and ("暂无交易计划" in trade_plan or "入场区" not in trade_plan):
        max_priority = min_priority_cap(max_priority, "B", order, reverse)
    if long_flow_score <= 3 and flow_score <= 3 and signal.score < 8:
        max_priority = min_priority_cap(max_priority, "C", order, reverse)
    if signal.kind in ("discovery", "hot_breakout", "early_breakout", "main_trend_watch"):
        if snapshot.price_change_percent > 8 and snapshot.oi_change_percent > 10:
            max_priority = min_priority_cap(max_priority, "B", order, reverse)
        if snapshot.price_position_24h is not None and snapshot.price_position_24h > 75:
            max_priority = min_priority_cap(max_priority, "C", order, reverse)
        if snapshot.price_change_percent > 8 and long_flow_score <= 6:
            max_priority = min_priority_cap(max_priority, "B", order, reverse)
        if "追高风险" in entry_label:
            max_priority = min_priority_cap(max_priority, "C", order, reverse)
    strict_cap = strict_quality_priority_cap(signal, snapshot, quality_score, trap_score, entry_score, entry_label)
    max_priority = min_priority_cap(max_priority, strict_cap, order, reverse)

    conviction, _conviction_label, _conviction_reason = conviction_score(snapshot, signal)
    intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
    if conviction >= 80:
        conviction_cap = "S"
    elif conviction >= 65:
        conviction_cap = "B"
    elif conviction >= 50:
        conviction_cap = "C"
    else:
        conviction_cap = "D"

    if signal.kind in ("top_exhaustion", "top_risk", "main_risk_watch") and intent_label in ("高位出货", "多杀多风险") and conviction >= 65:
        conviction_cap = "A"
    if signal.kind in ("discovery", "hot_breakout", "main_trend_watch") and intent_label not in ("真启动观察", "合约先行观察"):
        conviction_cap = min_priority_cap(conviction_cap, "C", order, reverse)
    if signal.kind == "bottom_reversal" and intent_label == "下跌中继":
        conviction_cap = "D"
    max_priority = min_priority_cap(max_priority, conviction_cap, order, reverse)

    return max_priority


def strict_quality_priority_cap(
    signal: Signal,
    snapshot: MarketSnapshot,
    quality_score: int,
    trap_score: int,
    entry_score: int,
    entry_label: str,
) -> str:
    blocked_s_labels = ("追高风险", "下跌中继", "不宜追")
    blocked_a_labels = ("追高风险", "下跌中继")
    if (
        quality_score >= 85
        and trap_score <= 3
        and entry_score >= 7
        and not any(label in entry_label for label in blocked_s_labels)
        and (
            signal.kind not in ("discovery", "hot_breakout")
            or snapshot.price_change_percent <= 8
            or entry_label in ("启动前", "启动前/启动初期", "启动初期")
        )
    ):
        return "S"
    if (
        quality_score >= 70
        and trap_score <= 5
        and entry_score >= 5
        and not any(label in entry_label for label in blocked_a_labels)
    ):
        return "A"
    if quality_score >= 55:
        return "B"
    if quality_score >= 40:
        return "C"
    return "D"


def min_priority_cap(priority: str, cap: str, order: dict[str, int], reverse: dict[int, str]) -> str:
    return reverse[min(order.get(priority, 0), order.get(cap, 0))]


def compact_digest_reason(text: str) -> str:
    compact = " ".join(str(text).split())
    return truncate_text(compact, 120)


def format_alt_watch_reason(text: str) -> str:
    compact = " ".join(str(text or "").replace("；", " ").replace(";", " ").split())
    return truncate_text(compact or "观察信号，等待确认", 48)


def discord_alt_watch_item_from_row(row: dict[str, str], row_time: dt.datetime) -> DiscordAltWatchItem | None:
    symbol = str(row.get("symbol") or "").strip().upper()
    if not is_valid_binance_usdt_symbol(symbol):
        return None
    if symbol in MAINSTREAM_WATCH_SYMBOLS:
        return None
    suppressed = str(row.get("suppressed_from_telegram") or "").strip()
    if suppressed not in {"1", "true", "True"}:
        return None

    conviction = parse_float(row.get("conviction_score")) or 0
    quality = parse_float(row.get("signal_quality_score")) or 0
    evidence = parse_float(row.get("evidence_score")) or 0
    leading = parse_float(row.get("leading_score")) or 0
    trap = parse_float(row.get("trap_risk_score")) or 0
    if quality < 25 and conviction < 45:
        return None
    if trap >= 8:
        return None
    if not (conviction >= 50 or quality >= 45 or evidence >= 6 or leading >= 5):
        return None

    sort_score = int(conviction + quality + evidence * 3 + leading * 3)
    return DiscordAltWatchItem(
        created_at=row_time.timestamp(),
        symbol=symbol,
        kind=str(row.get("kind") or ""),
        conviction_score=int(conviction),
        quality_score=int(quality),
        leading_score=int(leading),
        evidence_score=int(evidence),
        trap_score=int(trap),
        price_change_percent=parse_float(row.get("price_change_percent")),
        oi_change_percent=parse_float(row.get("oi_change_percent")),
        flow_label=str(row.get("flow_trend_label") or "-"),
        reason=format_alt_watch_reason(
            row.get("evidence_summary")
            or row.get("signal_quality_reason")
            or row.get("title")
            or ""
        ),
        sort_score=sort_score,
    )


def format_quality_reason_short(reason: str, max_parts: int = 3) -> str:
    reason_labels = [
        ("trap_risk>=8", "假信号极高", True),
        ("trap_risk>=6", "假信号高", True),
        ("long_flow_alignment<=3", "长线不支持", True),
        ("flow_alignment<=3", "资金不支持", True),
        ("short_term<=3", "短线不支持", True),
        ("mid_term<=3", "中线不支持", True),
        ("spot/onchain weak", "现货不支持", True),
        ("breakout without long-flow support", "突破未确认", True),
        ("hot/discovery long_flow<=3", "突破未确认", True),
        ("bottom_reversal weak taker or 1h flow", "抄底未确认", True),
        ("bottom weak taker/flow", "抄底未确认", True),
        ("top signal below 40% 24h position", "看空但不追空", True),
        ("top signal below 40% position", "看空但不追空", True),
        ("two-way liquidation/wash risk", "波动洗盘", True),
        ("liquidation wash", "波动洗盘", True),
        ("bottom_down_continuation", "下跌中继", True),
        ("下跌中继", "下跌中继", True),
        ("追高风险", "追高风险", True),
        ("高位风险", "高位风险", True),
        ("拉升缺长线确认", "拉升未确认", True),
        ("阶段追高", "阶段追高", True),
        ("看空但不追空", "看空但不追空", True),
        ("假信号风险高", "假信号高", True),
        ("现货不确认", "现货不确认", True),
        ("合约现货背离", "合约现货背离", True),
        ("主力趋势不支持", "主力趋势不支持", True),
        ("flow_alignment>=7", "资金强", False),
        ("long_flow_alignment>=6", "中线强", False),
        ("short_term>=7", "短线强", False),
        ("mid_term>=7", "中线强", False),
        ("trap_risk<=2", "假信号低", False),
        ("假信号低", "假信号低", False),
        ("现货确认强", "现货确认强", False),
        ("主力趋势支持", "主力趋势支持", False),
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
        lines.append(f"{priority_grade_label(priority)}:")
        for item in priority_items[:max_per_priority]:
            price_text = format_compact_percent(item.price_change_percent)
            oi_text = format_compact_percent(item.oi_change_percent)
            lines.append(
                f"{item.symbol} {direction_badge(signal_direction_label(item.kind))} "
                f"{priority_badge(item.priority)} 质量{item.quality_score} "
                f"{trap_badge(item.trap_score)} 强度{item.strength_score:.1f} "
                f"{price_text} OI{oi_text} | {item.reason}"
            )

    lines.append("")
    stats = [f"{priority_badge(priority)} {counts[priority]}" for priority in digest_priorities]
    lines.append(f"总静默: {sum(counts.values())}；" + "；".join(stats))
    return telegram_text("\n".join(lines))


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


def extend_digest_priorities(
    digest_priorities: list[str],
    items: list[TelegramSignalDigestItem],
) -> list[str]:
    extended = list(digest_priorities)
    for priority in ("S", "A", "B", "C", "D"):
        if priority not in extended and any(item.priority == priority for item in items):
            extended.append(priority)
    for item in items:
        if item.priority not in extended:
            extended.append(item.priority)
    return extended


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


def format_realtime_funding(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:+.4g}%"


def compact_realtime_liquidation_line(liquidation_text: str | None) -> str:
    text = str(liquidation_text or "")
    if not text or "暂无明显强平" in text or "暂无缓存" in text or text == "真实强平: n/a":
        return ""
    if "多头强平主导" in text:
        return "强平: 多头主导"
    if "空头强平主导" in text:
        return "强平: 空头主导"
    if "双向强平" in text or "剧烈洗盘" in text:
        return "强平: 双向洗盘"
    if "强平分散" in text or "方向分散" in text:
        return "强平: 分散"
    return ""


def compact_trade_plan(signal: Signal, max_length: int = 120) -> str:
    text = " ".join(signal_trade_plan(signal).split())
    replacements = {
        "方向:": "方向 ",
        "入场区:": "入场 ",
        "回踩观察区:": "回踩 ",
        "止损:": "止损 ",
        "止盈: TP1 ": "TP1 ",
        " / TP2 ": "；TP2 ",
        "；支撑:": "；支撑 ",
        "；阻力:": "；阻力 ",
        "。": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = re.sub(r"\([0-9.]+R\)", "", text)
    return truncate_text_by_lines(text, max_length, "...已精简")


def compact_confirmation_label(
    liquidation_text: str | None,
    coinglass_text: str | None,
    symbol: str,
    snapshot: MarketSnapshot | None = None,
    signal: Signal | None = None,
) -> str:
    if snapshot is None:
        return "现货中性 | 背离无 | 主力数据不足"
    return compact_rule_confirmation(
        snapshot,
        signal,
        cached_spot_alpha_confirmation(symbol) or spot_alpha_confirmation(symbol),
        coinglass_text,
    )


def best_pending_signal_index(pending: PendingTelegramSignalMerge) -> int:
    priority_order = {"S": 4, "A": 3, "B": 2, "C": 1, "D": 0}
    best_index = 0
    best_key = (-1, -1, -1.0, -1)
    for index, signal in enumerate(pending.signals):
        priority = pending.priorities[index] if index < len(pending.priorities) else "-"
        quality = pending.quality_scores[index] if index < len(pending.quality_scores) else 0
        key = (priority_order.get(str(priority).upper(), -1), quality, signal_strength_score(signal), signal.score)
        if key > best_key:
            best_key = key
            best_index = index
    return best_index


def progress_bar(score: float, max_score: int = 10, width: int = 10) -> str:
    if max_score <= 0 or width <= 0:
        return ""
    filled = max(0, min(width, int(round(float(score) / max_score * width))))
    return "█" * filled + "░" * (width - filled)


def trader_panel_title(signal: Signal, intent_label: str, conviction: int) -> str:
    kind = str(signal.kind or "").strip().lower()
    if kind == "main_trend_watch":
        return "🟢【主流趋势雷达】"
    if kind == "main_momentum_watch":
        return "🟡【主流异动雷达】"
    if kind == "main_risk_watch":
        return "🔴【主流风险雷达】"
    if intent_label in ("高位出货", "多杀多风险") or kind in ("top_risk", "top_exhaustion", "distribution", "crowded_top_risk"):
        return "🔴【紧急告警】 🔴关注顶部"
    if intent_label in ("下跌中继",) or kind in ("long_stop_loss", "down_acceleration"):
        return "🔴【紧急告警】 🔴关注跳水"
    if intent_label in ("底部承接",) or kind == "bottom_reversal":
        return "🟢【关注抄底】 🟢关注反弹"
    if intent_label in ("真启动观察", "合约先行观察") or kind in ("discovery", "hot_breakout"):
        if conviction >= 70:
            return "🟢【关注启动】 🟢关注买入"
    if intent_label == "短线逼空":
        return "🟡【预警告警】 🟡急跌异动"
    return "🟡【预警告警】 🟡异动观察"


def format_panel_percent(value: float | None) -> str:
    if value is None:
        return "n/a"
    arrow = "▲" if value > 0 else "▼" if value < 0 else "·"
    return f"{arrow}{abs(value):.1f}%"


def panel_price_change(snapshot: MarketSnapshot, period: str) -> float | None:
    changes = snapshot.price_change_periods or {}
    if period in changes:
        return changes[period]
    return None


def panel_oi_state(snapshot: MarketSnapshot, period: str) -> str:
    if period in ("15m", "1h"):
        return format_panel_percent(snapshot.oi_change_percent if period == "1h" else snapshot.confirm_oi_change_percent)
    return "n/a"


def flow_panel_value(snapshot: MarketSnapshot, period: str) -> str:
    value = snapshot.net_flow_usd.get(period)
    if value is None:
        return f"{period} ⚪ n/a"
    threshold = max((snapshot.quote_volume_24h or 0) * 0.00001, 1000.0)
    if abs(value) < threshold:
        icon = "⚪"
    elif value > 0:
        icon = "🟢"
    else:
        icon = "🔴"
    return f"{period} {icon} {format_usd(value)}"


def flow_strength_word(score: int) -> str:
    if score >= 7:
        return "强"
    if score <= 3:
        return "弱"
    return "中"


def trader_panel_flow_trend(snapshot: MarketSnapshot) -> str:
    short_score, mid_score, long_score, _label, _reason = flow_horizon_scores(snapshot)
    return f"短{flow_strength_word(short_score)} / 中{flow_strength_word(mid_score)} / 长{flow_strength_word(long_score)}"


def panel_ratio_bias(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value > 1.8:
        return "多头拥挤"
    if value > 1.2:
        return "偏多"
    if value >= 1.0:
        return "轻微偏多"
    if value >= 0.8:
        return "轻微偏空"
    if value < 0.7:
        return "空头拥挤"
    return "偏空"


def panel_ratio_text(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f} {panel_ratio_bias(value)}"


def panel_flow_source_title(snapshot: MarketSnapshot) -> str:
    return "✅ 主力净流（Binance）" if snapshot.net_flow_usd else "✅ 主力净流（Binance）"


def panel_flow_judgement(snapshot: MarketSnapshot, risk_panel: bool = False, kind: str | None = None) -> str:
    short_values = [snapshot.net_flow_usd.get(period) for period in ("5m", "15m", "1h")]
    mid_values = [snapshot.net_flow_usd.get(period) for period in ("4h", "12h", "24h")]
    long_values = [snapshot.net_flow_usd.get(period) for period in ("72h", "144h")]
    short_sum = sum(value for value in short_values if value is not None)
    mid_sum = sum(value for value in mid_values if value is not None)
    long_sum = sum(value for value in long_values if value is not None)
    known_mid_values = [value for value in mid_values if value is not None]
    long_crowded = any(
        value is not None and value >= threshold
        for value, threshold in (
            (snapshot.global_long_short_ratio, 1.8),
            (snapshot.top_account_ratio, 1.8),
            (snapshot.top_position_ratio, 1.5),
        )
    )
    short_price_down = any(
        value is not None and value < 0
        for value in (panel_price_change(snapshot, "15m"), panel_price_change(snapshot, "1h"))
    )
    if risk_panel:
        short_flow_out = short_sum < 0 or summary_flow_value(snapshot, "15m") < 0
        if long_crowded and short_flow_out:
            return "多头拥挤 + 短线流出，追多风险高"
        if short_flow_out or short_price_down:
            return "多头拥挤 + 短线流出，追多风险高"
        return "高位拥挤，等回落确认"
    if topq_kind_normalized(kind) == "main_momentum_watch":
        return "空头拥挤 + 短线急拉，异动观察；中周期资金仍需确认"
    if short_sum > 0 and mid_sum >= 0:
        return "短周期多头净流入，主力正在入场"
    if known_mid_values and all(value < 0 for value in known_mid_values):
        return "中周期持续净流出，主力中期持续出场"
    if long_sum > 0 and mid_sum >= 0:
        return "长周期资金承接，趋势基础较好"
    if short_sum > 0 and (snapshot.funding_rate_percent or 0) < 0:
        return "空头拥挤 + 短线急拉，等待中周期确认"
    if short_sum < 0 and mid_sum < 0 and (snapshot.funding_rate_percent or 0) > 0.08:
        return "多头过热/主力出货，注意获利了结"
    return "多周期资金分歧，方向待确认"


def panel_action_icon(action: str) -> str:
    if action.startswith(("强烈建议关注买入", "关注反弹")):
        return "🟢"
    if action.startswith(("减仓/避险", "建议减仓/避险", "禁止追多", "禁止抄底")):
        return "🔴"
    return "🟡"


def normalize_evidence_label_for_panel(label: str, item: EvidenceItem | None = None) -> str:
    mapping = {
        "OI增仓推涨": "OI持续建仓",
        "高位增仓追涨": "高位追涨增仓，谨慎接力",
        "高位增仓": "高位追涨增仓，谨慎接力",
        "空头回补推涨": "空头回补推涨，非真启动",
        "跌中增仓承压": "下跌增仓，空头施压",
        "仓位退出/风险释放": "仓位退出，风险释放",
        "短线增仓推涨": "短线OI增，主力试盘",
        "中线持续建仓": "OI中线持续建仓",
        "增仓分歧/可能派发": "OI增但价格不跟，疑似派发",
        "主动买盘强": "主买增强，资金主动进攻",
        "主动卖盘强": "主卖增强，资金主动撤退",
        "主动卖盘配合": "主卖增强，资金主动撤退",
        "短线主力流入": "短周期主力净流入",
        "中线主力流入": "中周期主力净流入",
        "长线主力流入": "长周期主力净流入",
        "短线资金不支持": "短周期资金不支持",
        "中线资金不支持": "中周期资金不支持",
        "长线资金不支持": "长周期资金不支持",
        "短强中弱，谨慎追": "短周期强但中周期弱，谨慎追",
        "回踩承接观察": "回踩有承接，等待确认",
        "散户多头拥挤": "散户多头拥挤，易被洗",
        "散户空头拥挤": "散户空头拥挤，关注反弹",
        "大户偏多建仓": "大户持仓偏多，疑似建仓",
        "大户偏空建仓": "大户持仓偏空，空方主导",
        "散户多/大户不跟": "散户多但大户未跟，谨慎追多",
        "散户空/大户偏多": "散户偏空但大户偏多，关注逼空",
        "费率偏热": "费率偏热，多头成本升高",
        "费率极热": "费率极高，多头过热",
        "空头拥挤": "空头拥挤，关注反弹",
        "空头极度拥挤": "空头过度拥挤，防逼空",
        "空头风险释放": "空头回补，风险释放",
        "多头风险释放": "多头降杠杆，风险释放",
        "合约溢价偏高": "合约溢价偏高，追多拥挤",
        "合约溢价极高": "合约溢价偏高，追多拥挤",
        "合约贴水偏深": "合约贴水偏深，空头激进",
        "合约贴水极深": "合约贴水偏深，空头激进",
        "合约多头拥挤": "合约多头拥挤，防回落",
        "空头挤压条件": "空头挤压条件形成",
        "现货/链上确认": "现货/链上同步确认",
        "现货承接": "现货承接，买盘跟随",
        "现货/链上出货": "现货/链上转弱，疑似出货",
        "高位现货未确认": "高位合约拉升，现货未确认",
        "流动性增加": "链上流动性增加",
        "流动性下降拉盘": "流动性下降仍拉盘，谨慎",
        "空头被挤压": "空头被挤，短线冲高",
        "多头止损释放": "多头止损释放",
        "双向洗盘": "双向爆仓洗盘",
        "空头挤压+现货承接": "空头挤压 + 现货承接",
        "多头挤压+现货出货": "多头挤压 + 现货出货",
    }
    return mapping.get(str(label or "").strip(), str(label or "").strip())


def panel_evidence_category(item: EvidenceItem) -> tuple[int, str]:
    if item.polarity == "risk":
        return 0, "🔴【风险】"
    if item.polarity == "positive" and item.source in {"OI", "FLOW", "FUNDING", "BASIS", "LSR", "WHALE"}:
        return 1, "🟢【领先】"
    if item.polarity == "positive" and item.source in {"SPOT", "ONCHAIN", "LIQ"}:
        return 2, "🟢【确认】"
    return 3, "🟡【观察】"


def risk_panel_evidence_label(label: str, item: EvidenceItem) -> str:
    text = normalize_evidence_label_for_panel(label, item)
    replacements = {
        "大户持仓偏多，疑似建仓": "大户持仓偏多",
        "OI持续建仓": "OI增仓支撑",
        "短线OI增，主力试盘": "短线OI增仓",
        "中周期主力净流入": "中周期资金流入",
        "长周期主力净流入": "长周期资金流入",
        "短周期主力净流入": "短周期资金流入",
        "现货/链上同步确认": "现货/链上有支撑",
        "现货承接，买盘跟随": "现货承接",
        "空头拥挤，关注反弹": "空头拥挤",
        "空头过度拥挤，防逼空": "空头过度拥挤",
    }
    return replacements.get(text, text.replace("疑似建仓", "").replace("关注反弹", "").strip("， "))


def momentum_panel_evidence_label(snapshot: MarketSnapshot, item: EvidenceItem) -> str:
    raw_label = str(item.label or "").strip()
    text = normalize_evidence_label_for_panel(raw_label, item)
    if raw_label == "短线主力流入" and summary_flow_value(snapshot, "5m") > 0 and summary_flow_value(snapshot, "15m") <= 0:
        return "5m突发流入，15m仍流出"
    replacements = {
        "短周期主力净流入": "短周期资金异动流入",
        "中周期主力净流入": "中周期资金支撑",
        "长周期主力净流入": "长周期资金支撑",
        "散户空头拥挤，关注反弹": "空头拥挤可能反抽",
        "空头拥挤，关注反弹": "空头拥挤可能反抽",
        "空头过度拥挤，防逼空": "空头拥挤可能反抽",
        "大户持仓偏多，疑似建仓": "大户持仓偏多，异动观察",
        "现货承接，买盘跟随": "现货承接，观察确认",
    }
    return replacements.get(text, text)


def is_momentum_evidence_item(item: EvidenceItem) -> bool:
    label = str(item.label or "")
    if item.polarity != "positive":
        return False
    if item.source in {"OI", "FLOW"}:
        return True
    return any(keyword in label for keyword in ("空头拥挤", "空头挤压", "现货承接", "链上承接"))


def panel_evidence_category_for_signal(item: EvidenceItem, risk_panel: bool) -> tuple[int, str, bool]:
    if item.polarity == "risk":
        return 0, "🔴【风险】", True
    if risk_panel and item.polarity == "positive":
        return 2, "🟡【反向支撑】", False
    rank, category = panel_evidence_category(item)
    return rank, category, not risk_panel


def main_momentum_panel_conclusion(snapshot: MarketSnapshot) -> str:
    if main_momentum_hard_downgrade(snapshot):
        return MAIN_MOMENTUM_TOPQ_TEXT
    _short_flow, mid_flow, _long_flow, flow_label, _flow_reason = flow_horizon_scores(snapshot)
    if summary_flow_value(snapshot, "1h") < 0 or mid_flow <= 4 or flow_label in ("短强中弱", "中长线派发", "资金分歧"):
        return "短线强，中线未确认"
    return "短线拉盘观察"


def normalize_trigger_phrase(text: str) -> str:
    phrase = str(text or "").strip()
    phrase = re.sub(r"^[+\-]?\d+\s*", "", phrase)
    phrase = phrase.replace("Funding", "费率").replace("funding", "费率").replace("OI", "持仓")
    replacements = {
        "主力建仓": "主力建仓",
        "中线资金支持": "中线资金支持",
        "现货承接": "现货承接",
        "合约先行": "合约先行",
        "空头回补非启动": "回补非启动",
        "风险释放不追空": "风险释放",
        "高位出货": "高位出货",
        "多杀多风险": "多杀多风险",
        "下跌中继": "下跌中继",
        "极端基差": "基差极端",
        "高位追涨": "高位追涨",
        "风控确认": "风控确认",
        "价涨/OI涨": "价涨持仓涨",
        "价涨/OI下降": "价涨减仓",
        "价跌/OI下降": "价跌减仓",
        "主买转弱": "主买转弱",
        "费率偏热": "费率偏热",
    }
    for key, value in replacements.items():
        if key in phrase:
            return value
    phrase = re.split(r"[；,/，|]", phrase)[0].strip()
    phrase = re.sub(r"[A-Za-z_]+", "", phrase).strip()
    return truncate_text(phrase, 14) if phrase else ""


def trader_panel_triggers(snapshot: MarketSnapshot, signal: Signal, merged_kinds: list[str]) -> tuple[int, list[str]]:
    _ev_score, _ev_direction, _ev_summary, evidence_items = evidence_score(snapshot, signal)
    trigger_items: list[tuple[int, int, str]] = []
    display_score = 0
    risk_panel = is_telegram_risk_signal(signal)
    if signal.kind == "main_momentum_watch":
        trigger_items.append((0, 0, f"🟡【观察】{main_momentum_panel_conclusion(snapshot)}"))
    if len(merged_kinds) > 1:
        trigger_items.append((3, 0, f"🟡【信号】{' + '.join(merged_kinds)}"))
    for item in evidence_items:
        rank, category, count_points = panel_evidence_category_for_signal(item, risk_panel)
        points = abs(item.points)
        if count_points:
            display_score += points
        label = risk_panel_evidence_label(item.label, item) if risk_panel and item.polarity == "positive" else normalize_evidence_label_for_panel(item.label, item)
        suffix = f" +{points}" if count_points else ""
        trigger_items.append((rank, -points, f"{category}{label}{suffix}"))
    trigger_items.sort(key=lambda row: (row[0], row[1]))
    triggers = [text for _rank, _points, text in trigger_items]
    return display_score, list(dict.fromkeys(triggers))[:8]


def trader_panel_momentum_trigger_groups(snapshot: MarketSnapshot, signal: Signal) -> tuple[int, list[str], list[str]]:
    _ev_score, _ev_direction, _ev_summary, evidence_items = evidence_score(snapshot, signal)
    evidence_rows: list[tuple[int, str]] = []
    risk_rows: list[tuple[int, str]] = []
    display_score = 0
    evidence_rows.append((0, main_momentum_panel_conclusion(snapshot)))
    for item in evidence_items:
        points = abs(item.points)
        if item.polarity == "risk":
            risk_rows.append((-points, normalize_evidence_label_for_panel(item.label, item)))
            continue
        if not is_momentum_evidence_item(item):
            continue
        display_score += points
        evidence_rows.append((-points, f"{momentum_panel_evidence_label(snapshot, item)} +{points}"))
    evidence = [text for _rank, text in sorted(evidence_rows, key=lambda row: row[0])]
    risks = [text for _rank, text in sorted(risk_rows, key=lambda row: row[0])]
    return display_score, list(dict.fromkeys(evidence))[:6], list(dict.fromkeys(risks))[:5]


def classify_leading_panel_item(text: str) -> str:
    risk_keywords = ("派发", "出货", "风险", "拥挤", "转负", "净卖出", "平仓", "顶部", "踩踏", "不支持")
    long_keywords = ("建仓", "吸筹", "承接", "回流", "逼空", "转正", "推升")
    if any(keyword in text for keyword in risk_keywords):
        return "risk"
    if any(keyword in text for keyword in long_keywords):
        return "long"
    return "neutral"


def sanitize_opposite_leading_item(text: str, final_direction: str = "") -> str:
    cleaned = text.replace("主力悄悄建仓", "OI异动观察")
    cleaned = cleaned.replace("主力持续建仓", "OI持续增仓观察")
    cleaned = cleaned.replace("中周期持续建仓", "中周期OI增仓观察")
    prefix = "反向支撑：" if final_direction == "看空" else "反向风险提示："
    return f"{prefix}{cleaned}"


def displayed_leading_items_score(items: list[str]) -> int:
    total = 0
    for item in items:
        match = re.search(r"\+(\d+)\s*$", item)
        if match:
            total += int(match.group(1))
    return total


def trader_panel_leading_items(leading: LeadingSignalScore, final_direction: str, limit: int = 6) -> tuple[int, list[str]]:
    source_items = [item for item in leading.leading_items if not item.endswith("+0")]
    if final_direction == "看空":
        primary = [item for item in source_items if classify_leading_panel_item(item) != "long"]
        opposite = [item for item in source_items if classify_leading_panel_item(item) == "long"]
        result = primary[:limit]
        if opposite and len(result) < limit:
            result.append(sanitize_opposite_leading_item(opposite[0], final_direction))
        display_items = list(dict.fromkeys(result))[:limit]
        return displayed_leading_items_score(display_items), display_items
    if final_direction == "看多":
        primary = [item for item in source_items if classify_leading_panel_item(item) != "risk"]
        opposite = [item for item in source_items if classify_leading_panel_item(item) == "risk"]
        result = primary[:limit]
        if opposite and len(result) < limit:
            result.append(sanitize_opposite_leading_item(opposite[0], final_direction))
        display_items = list(dict.fromkeys(result))[:limit]
        return displayed_leading_items_score(display_items), display_items
    display_items = source_items[:limit]
    return displayed_leading_items_score(display_items), display_items


def format_trader_panel(
    pending: PendingTelegramSignalMerge,
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
) -> str:
    best_index = best_pending_signal_index(pending)
    signal = pending.signals[best_index]
    if signal.snapshot is None:
        return telegram_text(f"{pending.symbol} n/a", 1600)

    snapshot = signal.snapshot
    kinds: list[str] = []
    for item in pending.signals:
        label = signal_kind_label(item.kind)
        if label not in kinds:
            kinds.append(label)
    basis_pct, _basis_label, _basis_reason = basis_state(snapshot)
    intent_label, _intent_score, _intent_reason = market_intent(snapshot, signal)
    conviction, _conv_label, _conv_reason = conviction_score(snapshot, signal)
    short_score, mid_score, long_score, _flow_label, _flow_reason = flow_horizon_scores(snapshot)
    action, _action_reason = action_label(snapshot, signal)
    score_10 = max(0, min(10, int(round(conviction / 10))))
    title = trader_panel_title(signal, intent_label, conviction)
    symbol_text = snapshot.symbol.replace("USDT", "/USDT")
    basis_text = "n/a" if basis_pct is None else f"{basis_pct:+.2f}%"
    risk_panel = is_telegram_risk_signal(signal)
    trigger_display_score, triggers = trader_panel_triggers(snapshot, signal, kinds)
    momentum_evidence: list[str] = []
    momentum_risks: list[str] = []
    if signal.kind == "main_momentum_watch":
        trigger_display_score, momentum_evidence, momentum_risks = trader_panel_momentum_trigger_groups(snapshot, signal)
    leading = leading_signal_score(snapshot, signal)
    final_direction = "看空" if risk_panel else signal_direction_label(signal.kind)
    leading_display_score, leading_items = trader_panel_leading_items(leading, final_direction, 6)

    lines = [
        title,
        "━━━━━━━━━━━━",
        f"◆ {symbol_text}",
        f"评分 {progress_bar(score_10)} {score_10}/10",
        "━━━━━━━━━━━━",
        f"💰 价格 ${snapshot.close_price:.8g}",
        (
            f"15m {format_panel_percent(panel_price_change(snapshot, '15m'))} | "
            f"1h {format_panel_percent(panel_price_change(snapshot, '1h'))} | "
            f"4h {format_panel_percent(panel_price_change(snapshot, '4h'))}"
        ),
        (
            f"📦 OI 15m {panel_oi_state(snapshot, '15m')} | "
            f"1h {panel_oi_state(snapshot, '1h')} | "
            f"4h {panel_oi_state(snapshot, '4h')}"
        ),
        f"💸 费率 {format_realtime_funding(snapshot.funding_rate_percent)}",
        f"📊 基差 {basis_text}",
        "━━━━━━━━━━━━",
        "👥 多空",
        f"账户多空比 {panel_ratio_text(snapshot.top_account_ratio)} | 大户持仓 {panel_ratio_text(snapshot.top_position_ratio)}",
        "━━━━━━━━━━━━",
        panel_flow_source_title(snapshot),
        flow_panel_value(snapshot, "5m"),
        flow_panel_value(snapshot, "15m"),
        flow_panel_value(snapshot, "1h"),
        flow_panel_value(snapshot, "4h"),
        flow_panel_value(snapshot, "12h"),
        flow_panel_value(snapshot, "24h"),
        flow_panel_value(snapshot, "72h"),
        flow_panel_value(snapshot, "144h"),
        f"💬 {panel_flow_judgement(snapshot, risk_panel, signal.kind)}",
        "━━━━━━━━━━━━",
        f"💡 操作建议 {panel_action_icon(action)} {action} 置信度 {score_10}/10",
        "━━━━━━━━━━━━",
    ]
    if leading_items:
        leading_title = "🎯 风险线索" if risk_panel else "🎯 领先信号"
        lines.append(f"{leading_title}（共{leading_display_score}分）")
        lines.extend(f"- {item}" for item in leading_items)
        lines.append("━━━━━━━━━━━━")
    if signal.kind == "main_momentum_watch":
        lines.append(f"✅ 异动证据（共{trigger_display_score}分）")
        lines.extend(f"• {trigger}" for trigger in momentum_evidence)
        if momentum_risks:
            lines.append("⚠️ 风险提示")
            lines.extend(f"• {trigger}" for trigger in momentum_risks)
    else:
        lines.append(f"触发信号（共{trigger_display_score}分）:")
        lines.extend(f"• {trigger}" for trigger in triggers)
    lines.extend(
        [
            "━━━━━━━━━━━━",
            f"⏰ {dt.datetime.now().strftime('%H:%M:%S')}",
        ]
    )
    return telegram_text("\n".join(lines), 1600)


def format_merged_signal_for_telegram(
    pending: PendingTelegramSignalMerge,
    liquidation_text: str | None = None,
    coinglass_text: str | None = None,
) -> str:
    return format_trader_panel(pending, liquidation_text, coinglass_text)


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
    if priority is None or quality_score is None or quality_reason is None:
        priority, quality_score, quality_reason = signal_priority(signal, signal.snapshot)
    direction = signal_direction_label(signal.kind)
    pending = PendingTelegramSignalMerge(
        created_at=time.time(),
        updated_at=time.time(),
        symbol=signal.symbol,
        direction=direction,
        signals=[signal],
        priorities=[priority],
        quality_scores=[quality_score],
        quality_reasons=[quality_reason],
    )
    return format_merged_signal_for_telegram(pending, liquidation_text)


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
            f"{snapshot.symbol}: 热度分={hot_watch_score(snapshot):+.2f} "
            f"价格={snapshot.price_change_percent:+.2f}% "
            f"OI={snapshot.oi_change_percent:+.2f}% "
            f"多空比={format_optional_value(snapshot.global_long_short_ratio)} "
            f"主动买卖比={format_optional_value(snapshot.taker_buy_sell_ratio)} "
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
        reasons.append("清算/结构提示波动洗盘")

    if signal and signal.kind in ("discovery", "hot_breakout"):
        if snapshot.price_change_percent > 8 and snapshot.oi_change_percent > 10:
            score = max(score, 6)
            reasons.append("追高风险: 涨幅>8%且OI扩张>10%")
        if high_position and long_flow_score <= 6:
            score = max(score, 6)
            reasons.append("高位且长周期资金确认不足")
        entry_score, entry_label, _entry_reason = entry_timing_score(snapshot, signal, trap_score_override=score)
        if "追高风险" in entry_label:
            score = max(score, 7)
            reasons.append("阶段追高风险")

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
    if signal.kind in ("top_risk", "main_risk_watch"):
        return base + funding + crowd + max(1 - (snapshot.taker_buy_sell_ratio or 1), 0) * 2
    if signal.kind == "distribution":
        return max(snapshot.price_change_percent, 0) + abs(min(snapshot.oi_change_percent, 0)) + max(1 - (snapshot.taker_buy_sell_ratio or 1), 0) * 2
    if signal.kind in ("hot_breakout", "main_trend_watch"):
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
    cold = sorted(rows, key=lambda row: row["score"])[:2]
    lines = ["🔥 热点板块"]
    lines.extend(format_sector_summary_hot_row(index, row) for index, row in enumerate(hot, start=1))
    if cold:
        cold_text = " | ".join(f"{row['sector']} {row['score']:+.2f}" for row in cold)
        lines.extend(["", "🧊 冷门板块", cold_text])
    return "\n".join(lines)


def format_sector_summary_hot_row(index: int, row: dict[str, Any]) -> str:
    leader = row["leader"]
    return (
        f"{index}. {row['sector']} {row['score']:+.1f} | "
        f"均涨{row['avg_price']:+.2f}% | OI{row['avg_oi']:+.2f}% | "
        f"龙头 {base_symbol(leader.symbol)} {leader.price_change_percent:+.2f}%"
    )


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
        f"主动买入偏强: {taker_ratio * 100:.1f}% | 费率过热: {funding_hot_ratio * 100:.1f}%",
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
        return "📊 每小时市场简报\n[SUMMARY] 市场温度摘要 v2\n暂无快照数据"

    total = len(snapshots)
    up_count = sum(1 for item in snapshots if item.price_change_percent > 0)
    oi_up_count = sum(1 for item in snapshots if item.oi_change_percent > 0)
    crowded_count = sum(1 for item in snapshots if (item.global_long_short_ratio or 0) >= 2.0)
    hot_funding_count = sum(1 for item in snapshots if (item.funding_rate_percent or 0) >= 0.03)
    flow_1h_positive = sum(1 for item in snapshots if summary_flow_value(item, "1h") > 0)

    rankable_snapshots = [item for item in snapshots if is_summary_display_symbol(item.symbol)]
    discovery_candidates = [item for item in rankable_snapshots if is_summary_discovery(item)]
    hot_candidates = [item for item in rankable_snapshots if is_summary_hot(item)]
    top_risk_candidates = [item for item in rankable_snapshots if is_summary_top_risk(item)]
    distribution_candidates = [item for item in rankable_snapshots if is_summary_distribution(item)]

    temperature = market_temperature_score(snapshots)

    hot_set = {item.symbol for item in hot_candidates}
    risk_set = {item.symbol for item in top_risk_candidates}
    distribution_set = {item.symbol for item in distribution_candidates}

    discovery_pool = [
        item for item in rankable_snapshots
        if item.symbol not in hot_set
        and item.symbol not in risk_set
        and item.symbol not in distribution_set
        and discovery_score(item) > 0
    ]
    primary_discovery_leaders = sorted(
        [item for item in discovery_pool if item.taker_buy_sell_ratio is not None and item.taker_buy_sell_ratio >= 1.0],
        key=summary_discovery_display_score,
        reverse=True,
    )
    fallback_discovery_leaders = sorted(
        [item for item in discovery_pool if item.taker_buy_sell_ratio is None or item.taker_buy_sell_ratio < 1.0],
        key=summary_discovery_display_score,
        reverse=True,
    )
    discovery_leaders = (primary_discovery_leaders + fallback_discovery_leaders)[:3]
    flow_leaders = [item for item in sorted(rankable_snapshots, key=lambda item: summary_flow_value(item, "15m"), reverse=True) if summary_flow_value(item, "15m") > 0][:3]
    oi_leaders = [item for item in sorted(rankable_snapshots, key=lambda item: item.oi_change_percent, reverse=True) if item.oi_change_percent > 0][:3]
    risk_leaders = sorted(
        top_risk_candidates,
        key=top_risk_score,
        reverse=True,
    )[:3]

    sections = [
        "📊 每小时市场简报",
        "[SUMMARY] 市场温度摘要 v2",
        f"市场温度: {temperature:.0f}/100 {market_temperature_label(temperature)}",
        f"大盘风向: {summary_market_direction_label(snapshots)}",
        f"监控: {total}币 | 上涨{up_count / total * 100:.0f}% | OI扩张{oi_up_count / total * 100:.0f}% | 1h净流入{flow_1h_positive / total * 100:.0f}%",
        f"拥挤: 多头{crowded_count} | 费率过热{hot_funding_count}",
        f"候选: 启动{len(discovery_candidates)} | 过热{len(hot_candidates)} | 逃顶{len(top_risk_candidates)} | 派发{len(distribution_candidates)}",
        "",
        format_summary_major_lines(snapshots),
        "",
        format_sector_brief_for_summary(snapshots),
    ]

    ranking_sections = [
        format_summary_ranking("🔴 逃顶风险 Top3", risk_leaders, format_summary_risk_item),
        format_summary_ranking("🟢 接近启动 Top3", discovery_leaders, format_summary_discovery_item),
        format_summary_ranking("🟢 资金流入 Top3", flow_leaders, format_summary_flow_item),
        format_summary_ranking("🟡 OI增长 Top3", oi_leaders, format_summary_oi_item),
    ]
    ranking_sections = [section for section in ranking_sections if section]
    if ranking_sections:
        sections.extend(["", *ranking_sections])
    else:
        sections.extend(["", "暂无高价值榜单，继续观察。"])

    sections.extend([
        "",
        f"时间: {now}",
    ])
    return telegram_text("\n".join(part for part in sections if part != ""))


def summary_market_direction_label(snapshots: list[MarketSnapshot]) -> str:
    by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
    majors = [by_symbol[symbol] for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT") if symbol in by_symbol]
    if not majors:
        return "暂无核心币数据"

    strength = 0
    for snapshot in majors:
        if short_term_score(snapshot) >= 6:
            strength += 1
        if mid_term_score(snapshot) >= 6:
            strength += 1
        if summary_flow_value(snapshot, "15m") > 0:
            strength += 1
        if summary_flow_value(snapshot, "1h") > 0:
            strength += 1

    if strength >= 7:
        return "偏强"
    if strength >= 4:
        return "中性"
    return "偏弱"


def format_summary_major_lines(snapshots: list[MarketSnapshot]) -> str:
    by_symbol = {snapshot.symbol: snapshot for snapshot in snapshots}
    lines = []
    for symbol in ("BTCUSDT", "ETHUSDT", "SOLUSDT"):
        snapshot = by_symbol.get(symbol)
        if snapshot is None:
            continue
        lines.append(
            f"{base_symbol(symbol)}: 短{short_term_score(snapshot)}/10 中{mid_term_score(snapshot)}/10 | "
            f"15m {format_usd(summary_flow_value(snapshot, '15m'))} | "
            f"1h {format_usd(summary_flow_value(snapshot, '1h'))} | "
            f"位置{format_optional_value(snapshot.price_position_24h)}%"
        )
    return "\n".join(lines) if lines else "核心币: 暂无数据"


def format_summary_ranking(
    title: str,
    snapshots: list[MarketSnapshot],
    formatter: Callable[[MarketSnapshot], str],
) -> str:
    rows = [formatter(snapshot) for snapshot in snapshots[:3]]
    rows = [row for row in rows if row]
    if not rows:
        return ""
    return "\n".join([title, *rows])


def format_summary_risk_item(snapshot: MarketSnapshot) -> str:
    return (
        f"🔴 {summary_symbol_label(snapshot.symbol)} {snapshot.price_change_percent:+.1f}% | "
        f"OI{snapshot.oi_change_percent:+.1f}% | 费率{format_realtime_funding(snapshot.funding_rate_percent)} | "
        f"风险{top_risk_score(snapshot):.0f}"
    )


def summary_discovery_item_icon(snapshot: MarketSnapshot) -> str:
    taker = snapshot.taker_buy_sell_ratio
    if taker is not None and taker >= 1.05:
        return "🟢"
    return "🟡"


def format_summary_discovery_item(snapshot: MarketSnapshot) -> str:
    icon = summary_discovery_item_icon(snapshot)
    return (
        f"{icon} {summary_symbol_label(snapshot.symbol)} {snapshot.price_change_percent:+.2f}% | "
        f"OI{snapshot.oi_change_percent:+.2f}% | 主买{format_optional_value(snapshot.taker_buy_sell_ratio)} | "
        f"分{discovery_score(snapshot):.1f}"
    )


def summary_flow_item_icon(snapshot: MarketSnapshot) -> str:
    flow15 = summary_flow_value(snapshot, "15m")
    flow1h = summary_flow_value(snapshot, "1h")
    if flow15 > 0 and flow1h > 0:
        return "🟢"
    return "🟡"


def format_summary_flow_item(snapshot: MarketSnapshot) -> str:
    return (
        f"{summary_flow_item_icon(snapshot)} {summary_symbol_label(snapshot.symbol)} {snapshot.price_change_percent:+.2f}% | "
        f"15m {format_usd(summary_flow_value(snapshot, '15m'))} | "
        f"1h {format_usd(summary_flow_value(snapshot, '1h'))}"
    )


def format_summary_oi_item(snapshot: MarketSnapshot) -> str:
    return (
        f"🟡 {summary_symbol_label(snapshot.symbol)} {snapshot.price_change_percent:+.2f}% | "
        f"OI{snapshot.oi_change_percent:+.1f}% | 费率{format_realtime_funding(snapshot.funding_rate_percent)}"
    )


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


def summary_discovery_display_score(snapshot: MarketSnapshot) -> float:
    score = discovery_score(snapshot)
    taker = snapshot.taker_buy_sell_ratio
    if taker is not None and taker < 0.9:
        return score - 1_000
    if taker is not None and taker < 1.0:
        return score - 20
    if taker is not None and taker < 1.05:
        return score - 8
    return score


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
        score = f" 启动分={discovery_score(snapshot):+.2f}" if include_score else ""
        risk = f" 风险分={top_risk_score(snapshot):+.2f}" if include_risk else ""
        flow = ""
        if include_flow:
            flow = f" 15m资金={format_usd(summary_flow_value(snapshot, '15m'))} 1h资金={format_usd(summary_flow_value(snapshot, '1h'))}"
        marker = snapshot_direction_marker(snapshot)
        lines.append(
            f"{marker} {snapshot.symbol}: 价格={snapshot.price_change_percent:+.2f}% "
            f"OI={snapshot.oi_change_percent:+.2f}% "
            f"多空比={format_optional_value(snapshot.global_long_short_ratio)} "
            f"主动买卖比={format_optional_value(snapshot.taker_buy_sell_ratio)} "
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


def truncate_text_by_lines(text: str, limit: int = 1800, suffix: str = "...已精简") -> str:
    if len(text) <= limit:
        return text
    result_lines: list[str] = []
    current_len = 0
    suffix_len = len(suffix) + 1
    for line in str(text).splitlines():
        addition = len(line) + (1 if result_lines else 0)
        if current_len + addition + suffix_len > limit:
            break
        result_lines.append(line)
        current_len += addition
    if result_lines:
        return "\n".join(result_lines + [suffix])

    allowed = max(0, limit - suffix_len)
    cut = min(len(text), allowed)
    while cut > 0 and (text[cut - 1].isdigit() or text[cut - 1] in ".+-()（）"):
        cut -= 1
    return f"{text[:cut].rstrip()}\n{suffix}"


def telegram_text(text: str, limit: int = 1800) -> str:
    return truncate_text_by_lines(text, limit)


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
        " /flow SYMBOL - 交易员主力资金视图\n"
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
    parser.add_argument("--test-discord-env", action="store_true", help="Print Discord env configuration and exit.")
    parser.add_argument("--symbol", help="Diagnose one futures symbol, for example SIGNUSDT.")
    args = parser.parse_args()

    configure_logging(args.verbose)
    if args.test_discord_env:
        print(discord_env_diagnostics())
        return 0
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
    signal = signals[0] if signals else None
    spot_text = spot_alpha_confirmation(snapshot.symbol)
    conviction_part = "\n".join(format_conviction_model_lines(snapshot, signal))
    ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    ev_display_score = evidence_display_score(ev_direction, ev_items, ev_score)
    leading = leading_signal_score(snapshot, signal)
    leading_part = leading_check_block(leading)
    rule_lines = "\n".join(format_rule_optimization_lines(snapshot, signal, spot_text, coinglass_text))
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
        f"{conviction_part}\n"
        f"证据: {ev_direction} {ev_display_score}分 - {ev_summary}\n"
        f"{leading_part}\n"
        f"现货/链上确认: {spot_text}\n"
        f"{rule_lines}\n"
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
    signal = signals[0] if signals else None
    conviction_part = "\n".join(format_conviction_model_lines(snapshot, signal))
    ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    ev_display_score = evidence_display_score(ev_direction, ev_items, ev_score)
    evidence_part = f"证据: {ev_direction} {ev_display_score}分 - {ev_summary}\n"
    leading = leading_signal_score(snapshot, signal)
    leading_part = f"{leading_ask_brief(leading)}\n"
    trap_score, trap_label, trap_reason = trap_risk_score(snapshot, signal)
    entry_score, entry_label, entry_reason = entry_timing_score(snapshot, signals[0]) if signals else (5, "观察", "中性信号")
    rule_lines = "\n".join(format_rule_optimization_lines(snapshot, signal, spot_text, coinglass_text))
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
        f"{conviction_part}\n"
        f"{evidence_part}"
        f"{leading_part}"
        f"阶段: {entry_label} {entry_score}/10 - {entry_reason}\n"
        f"诱多/诱空风险: {trap_score}/10 {trap_label} - {trap_reason}\n"
        f"{rule_lines}\n"
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
        f"{conviction_part}\n\n"
        f"{evidence_part}"
        f"{leading_part}\n"
        f"现货/链上确认: {spot_text}\n"
        f"{rule_lines}\n"
        f"阶段: {entry_label} {entry_score}/10 - {entry_reason}\n"
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
            f"{review_text}\n开仓建议: {entry_advice}\n\n{format_ask_core_data(snapshot, signals, liquidation_text, coinglass_text)}",
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
    signals: list[Signal] | None = None,
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
    signal = signals[0] if signals else None
    conviction_part = "\n".join(format_conviction_model_lines(snapshot, signal))
    ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    ev_display_score = evidence_display_score(ev_direction, ev_items, ev_score)
    evidence_part = f"证据: {ev_direction} {ev_display_score}分 - {ev_summary}\n"
    leading = leading_signal_score(snapshot, signal)
    leading_part = f"{leading_ask_brief(leading)}\n"
    trap_score, trap_label, trap_reason = trap_risk_score(snapshot, signal)
    entry_score, entry_label, entry_reason = entry_timing_score(snapshot, signal) if signal else (5, "观察", "中性信号")
    rule_lines = "\n".join(format_rule_optimization_lines(snapshot, signal, spot_text, coinglass_text))
    coinglass_part = ""
    if not is_major_asset_tier(snapshot.symbol):
        coinglass_part = f"CoinGlass: {compact_coinglass_market_context(coinglass_text or 'CoinGlass聚合: n/a')}"
    return (
        "[核心数据]\n"
        f"价格/OI/Funding: {snapshot.close_price:.8g}; 价格 {snapshot.price_change_percent:+.2f}%; "
        f"OI {snapshot.oi_change_percent:+.2f}%; Funding {format_optional_value(snapshot.funding_rate_percent)}% ({funding_note(snapshot.funding_rate_percent)})\n"
        f"评分: 短线 {short_term_score(snapshot)}/10; 中线 {mid_term_score(snapshot)}/10; "
        f"资金流共振 {flow_alignment_score(snapshot)}/10; 长周期资金共振 {long_flow_alignment_score(snapshot)}/9\n"
        f"{conviction_part}\n"
        f"{evidence_part}"
        f"{leading_part}"
        f"阶段: {entry_label} {entry_score}/10 - {entry_reason}\n"
        f"诱多/诱空风险: {trap_score}/10 {trap_label} - {trap_reason}\n"
        f"{rule_lines}\n"
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
        priority, quality_score, _quality_reason = signal_priority(signal, signal.snapshot)
        trap_score: int | str = "-"
        entry_part = ""
        if signal.snapshot:
            trap_score, _trap_label, _trap_reason = trap_risk_score(signal.snapshot, signal)
            entry_score, entry_label, entry_reason = entry_timing_score(signal.snapshot, signal)
            entry_part = f" 阶段={entry_label} {entry_score}/10 - {entry_reason}"
        strength_score = signal_strength_score(signal)
        lines.append(
            f"- {priority_badge(priority)} {direction_badge(signal_direction_label(signal.kind))} "
            f"{signal.kind} q{quality_score} score{signal.score} "
            f"{strength_badge(strength_score)}{strength_score:.1f} {trap_badge(trap_score)}"
            f"{entry_part} - {signal.message}"
        )
    return "\n".join(lines)


def format_recent_symbol_signals(rows: list[dict[str, str]]) -> str:
    if not rows:
        return "最近暂无该币信号记录"

    lines = []
    for row in rows:
        time_text = row.get("time", "-").replace("T", " ")[:19]
        entry_label = row.get("entry_timing_label") or ""
        entry_score = row.get("entry_timing_score") or ""
        entry_part = f" entry={entry_label}/{entry_score}" if entry_label and entry_score else ""
        lines.append(
            f"- {time_text} {row.get('kind', '-')} "
            f"score={row.get('score', '-')} 强度={format_csv_strength(row.get('strength_score'))} "
            f"价格={format_csv_number(row.get('price_change_percent'))}% "
            f"OI={format_csv_number(row.get('oi_change_percent'))}%"
            f"{entry_part}"
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
    spot_text = spot_alpha_confirmation(snapshot.symbol)
    print(f"现货/链上确认: {spot_text}")
    signal = signals[0] if signals else None
    for line in format_conviction_model_lines(snapshot, signal):
        print(line)
    ev_score, ev_direction, ev_summary, ev_items = evidence_score(snapshot, signal)
    ev_display_score = evidence_display_score(ev_direction, ev_items, ev_score)
    print(f"证据: {ev_direction} {ev_display_score}分 - {ev_summary}")
    leading = leading_signal_score(snapshot, signal)
    print(leading_check_block(leading))
    for item in ev_items[:8]:
        print(f"{evidence_item_icon(item)} {item.label} {evidence_item_signed_points(item):+d}")
    for line in format_rule_optimization_lines(snapshot, signal, spot_text, coinglass_text):
        print(line)
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
