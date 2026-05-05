import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import derivatives_monitor as m
import time


CONFIG = {
    "min_liquidity_usd": 100000,
    "premium_alert_threshold_pct": 3.0,
    "spread_alert_threshold_pct": 1.0,
    "ttl_seconds": 3600,
}


def instrument():
    return m.StockTokenInstrument(
        token_symbol="NVDAx",
        underlying_symbol="NVDA",
        name="NVIDIA xStock",
        sector="semis",
        provider="xStocks",
        enabled=True,
    )


def underlying(**overrides):
    data = {
        "price": 100.0,
        "change_1h": 1.2,
        "change_1d": 2.5,
        "volume_ratio": 1.5,
        "ema20": 95.0,
        "ema60": 90.0,
        "open": 99.0,
    }
    data.update(overrides)
    return data


def snapshot(**quote_overrides):
    quote = {
        "token_price": 100.8,
        "token_volume_24h": 250000,
        "token_liquidity_usd": 300000,
        "spread_pct": 0.3,
        "source": "test",
    }
    quote.update(quote_overrides)
    return m.build_stock_token_snapshot(
        instrument(),
        quote,
        underlying(),
        "指数同步偏强",
        "板块同步偏强",
        CONFIG,
        1_700_000_000,
    )


def test_stock_token_premium_pct_calculation():
    assert m.stock_token_premium_pct(101, 100) == 1.0
    assert m.stock_token_premium_pct(99, 100) == -1.0
    assert m.stock_token_premium_pct(None, 100) is None


def test_high_premium_is_risk_alert_and_penalized():
    item = snapshot(token_price=106.0)

    assert item.premium_pct == 6.0
    assert m.stock_token_is_risk_alert(item, CONFIG)
    icon, label = m.stock_token_display_level(item, CONFIG)
    assert icon == "🔴"
    assert label == "溢价/流动性风险"


def test_low_liquidity_is_risk_alert():
    item = snapshot(token_liquidity_usd=25000)

    assert m.stock_token_is_risk_alert(item, CONFIG)
    assert item.risk_score >= 5
    assert m.stock_token_display_level(item, CONFIG)[1] == "溢价/流动性风险"


def test_stock_token_score_layers():
    strong = snapshot()
    weak = m.build_stock_token_snapshot(
        instrument(),
        {"token_price": None, "token_volume_24h": None, "token_liquidity_usd": None, "spread_pct": None},
        underlying(price=90, change_1h=-1, change_1d=-2, volume_ratio=0.5, ema20=95, ema60=100),
        "指数同步偏弱",
        "板块同步偏弱",
        CONFIG,
        1_700_000_000,
    )

    assert strong.final_score >= 80
    assert m.stock_token_display_level(strong, CONFIG)[1] == "美股代币强势观察"
    assert weak.final_score < 60
    assert m.stock_token_display_level(weak, CONFIG)[1] == "Token报价不足"


def test_discord_copy_has_no_trade_advice_words():
    item = snapshot()
    embed = m.stock_token_detail_embed(item, "summary")
    text = "\n".join([embed.title or ""] + [f"{name}\n{value}" for name, value, _inline in embed.fields or []])

    assert "观察" in text
    for forbidden in ("买入", "卖出", "强烈建议"):
        assert forbidden not in text


def test_stooq_us_symbol_conversion():
    assert m.stooq_us_symbol("NVDA") == "nvda.us"
    assert m.stooq_us_symbol("SPY") == "spy.us"
    assert m.stooq_us_symbol("QQQ") == "qqq.us"


def test_yahoo_chart_response_parses_latest_price_and_1h_change():
    closes = [100 + i for i in range(13)]
    payload = {
        "chart": {
            "result": [
                {
                    "meta": {"chartPreviousClose": 98},
                    "indicators": {
                        "quote": [
                            {
                                "open": closes,
                                "close": closes,
                                "volume": [1000 + i for i in range(13)],
                            }
                        ]
                    },
                }
            ]
        }
    }

    parsed = m.parse_yahoo_chart_underlying(payload)

    assert parsed["price"] == 112
    assert parsed["change_1h"] == 12.0
    assert parsed["change_1d"] == m.percent_change(98, 112)


def test_kraken_assetpairs_matching_finds_stock_tokens():
    asset_pairs = {
        "NVDAxUSD": {"altname": "NVDAxUSD", "wsname": "NVDAx/USD"},
        "AAPLxUSD": {"altname": "AAPLxUSD", "wsname": "AAPLx/USD"},
        "XXBTZUSD": {"altname": "XBTUSD", "wsname": "XBT/USD"},
    }

    assert m.kraken_pair_matches_stock_token("NVDAxUSD", asset_pairs["NVDAxUSD"], "NVDAx")
    assert m.kraken_pair_matches_stock_token("AAPLxUSD", asset_pairs["AAPLxUSD"], "AAPLx")
    assert not m.kraken_pair_matches_stock_token("XXBTZUSD", asset_pairs["XXBTZUSD"], "NVDAx")


def test_token_missing_with_underlying_does_not_say_underlying_insufficient():
    item = m.build_stock_token_snapshot(
        instrument(),
        {"token_price": None, "token_volume_24h": None, "token_liquidity_usd": None, "spread_pct": None},
        underlying(),
        "指数同步偏强",
        "板块同步偏强",
        CONFIG,
        1_700_000_000,
    )
    embed = m.stock_token_detail_embed(item, "summary")
    text = "\n".join([embed.title or ""] + [f"{name}\n{value}" for name, value, _inline in embed.fields or []])

    assert "Token报价不足" in text
    assert "先按底层美股观察" in text
    assert "底层数据不足" not in text


def test_yahoo_rate_limited_uses_stale_cache_fallback():
    class Response429:
        status_code = 429

        def json(self):
            return {}

    class Session:
        def get(self, *_args, **_kwargs):
            return Response429()

    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.session = Session()
    monitor.stock_token_config = {"yahoo_cache_ttl_seconds": 1, "yahoo_request_min_gap_seconds": 0}
    monitor.stock_token_last_yahoo_request_at = 0
    monitor.stock_token_yahoo_cache = {
        "NVDA": (
            time.time() - 3600,
            {
                "price": 100.0,
                "change_5m": 0.1,
                "change_15m": 0.2,
                "change_1h": 0.3,
                "change_1d": 1.0,
            },
        )
    }
    health = {}
    monitor.update_stock_token_source_health = lambda source, success=False, error="", fetched_count=0, enabled=True, debug=None: health.update(
        {source: {"success": success, "error": error, "fetched": fetched_count}}
    )

    data = monitor.collect_underlying_market_data(["NVDA"])

    assert data["NVDA"]["price"] == 100.0
    assert data["NVDA"]["stale_cache_used"] is True
    assert health["Yahoo underlying"]["success"] is True
    assert "yahoo_rate_limited" in health["Yahoo underlying"]["error"]
    assert "stale_cache_used=1" in health["Yahoo underlying"]["error"]


def test_stock_token_risk_embed_token_missing_explains_quote_unavailable():
    item = m.build_stock_token_snapshot(
        instrument(),
        {"token_price": None, "token_volume_24h": None, "token_liquidity_usd": None, "spread_pct": None},
        underlying(),
        "指数同步偏强",
        "板块同步偏强",
        CONFIG,
        1_700_000_000,
    )

    embed = m.stock_token_risk_embed([], "summary", all_snapshots=[item])
    text = "\n".join(f"{name}\n{value}" for name, value, _inline in embed.fields or [])

    assert "暂无 token 报价，暂不能计算 premium/spread/liquidity 风险" in text
    assert "当前暂无 premium/spread/liquidity 异常" not in text


def test_dexscreener_pair_requires_explicit_xstocks_context():
    confirmed = {
        "baseToken": {"symbol": "NVDAx", "name": "NVIDIA xStock"},
        "quoteToken": {"symbol": "USDC"},
        "url": "https://dexscreener.com/solana/test",
        "priceUsd": "101.2",
        "liquidity": {"usd": 50000},
        "volume": {"h24": 12000},
    }
    ordinary = {
        "baseToken": {"symbol": "NVDA", "name": "NVIDIA"},
        "quoteToken": {"symbol": "USD"},
        "url": "https://example.com/NVDA",
        "priceUsd": "101.2",
    }

    assert m.dexscreener_pair_matches_stock_token(confirmed, "NVDAx")
    assert not m.dexscreener_pair_matches_stock_token(ordinary, "NVDAx")


def test_short_term_missing_copy_mentions_daily_only_observation():
    item = m.build_stock_token_snapshot(
        instrument(),
        {"token_price": None, "token_volume_24h": None, "token_liquidity_usd": None, "spread_pct": None},
        underlying(change_5m=None, change_15m=None, change_1h=None, change_1d=1.5),
        "指数同步偏强",
        "板块同步偏强",
        CONFIG,
        1_700_000_000,
    )

    text = m.stock_token_field_value(item)

    assert "短线数据不足，当前仅按日线观察" in text


def test_token_quote_daily_underlying_no_intraday_label_is_daily_observation():
    item = m.build_stock_token_snapshot(
        instrument(),
        {"token_price": 102.0, "token_volume_24h": 250000, "token_liquidity_usd": 300000, "spread_pct": None},
        underlying(change_5m=None, change_15m=None, change_1h=None, change_1d=1.5),
        "指数同步偏强",
        "板块同步偏强",
        CONFIG,
        1_700_000_000,
    )

    icon, label = m.stock_token_display_level(item, CONFIG)

    assert icon == "⚪"
    assert label == "日线观察"
    assert label != "数据不足"


def test_yahoo_rate_limited_without_fetch_or_cache_is_not_success():
    class Response429:
        status_code = 429

        def json(self):
            return {}

    class Session:
        def get(self, *_args, **_kwargs):
            return Response429()

    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.session = Session()
    monitor.stock_token_config = {"yahoo_cache_ttl_seconds": 900, "yahoo_request_min_gap_seconds": 0}
    monitor.stock_token_last_yahoo_request_at = 0
    monitor.stock_token_yahoo_cache = {}
    health = {}
    monitor.update_stock_token_source_health = lambda source, success=False, error="", fetched_count=0, enabled=True, debug=None: health.update(
        {source: {"success": success, "error": error, "fetched": fetched_count}}
    )
    monitor.fetch_stooq_intraday_history = lambda *_args, **_kwargs: []
    monitor.fetch_stooq_daily_history = lambda *_args, **_kwargs: [
        {"close": 99.0, "open": 99.0, "volume": 1.0},
        {"close": 100.0, "open": 100.0, "volume": 1.0},
    ]

    data = monitor.collect_underlying_market_data(["NVDA"])

    assert data["NVDA"]["price"] == 100.0
    assert health["Yahoo underlying"]["success"] is False
    assert health["Yahoo underlying"]["fetched"] == 0
    assert "yahoo_rate_limited" in health["Yahoo underlying"]["error"]


def test_premium_two_percent_enters_risk榜_with偏离_observation():
    item = m.build_stock_token_snapshot(
        instrument(),
        {"token_price": 102.0, "token_volume_24h": 250000, "token_liquidity_usd": 300000, "spread_pct": None},
        underlying(price=100.0, change_1h=0.5, change_1d=1.0),
        "指数同步偏强",
        "板块同步偏强",
        CONFIG,
        1_700_000_000,
    )

    assert item.premium_pct == 2.0
    assert m.stock_token_is_risk_alert(item, CONFIG)
    embed = m.stock_token_risk_embed([item], "summary", all_snapshots=[item], config=CONFIG)
    text = "\n".join(f"{name}\n{value}" for name, value, _inline in embed.fields or [])

    assert "溢价/折价偏离观察" in text
    assert "NVDAx 溢价 +2.00%，等待溢价回落，避免追价" in text
    assert "spread 数据不足" in text


def test_source_health_debug_candidates_are_truncated_for_discord():
    health = {
        "Kraken public": {
            "success": False,
            "fetched_count": 0,
            "last_error": "no xStocks pairs found",
            "last_success": None,
            "debug": ["c1", "c2", "c3", "c4", "c5"],
        }
    }

    text = m.format_stock_token_source_health(health)

    assert "c1, c2, c3 ... +2" in text
    assert "c4" not in text


def daily_rows_from_closes(closes, high_offset=1.0, low_offset=1.0):
    return [
        {
            "close": float(close),
            "open": float(close),
            "high": float(close) + high_offset,
            "low": float(close) - low_offset,
            "volume": 1000.0,
        }
        for close in closes
    ]


def test_daily_only_kline_structure_has_no_intraday_copy():
    structure = m.build_stock_kline_structure("NVDA", daily_rows_from_closes(range(80, 141)), None)
    text = m.stock_kline_detail_text(structure)

    assert structure.data_quality == "daily_only"
    assert "仅日线" in text
    assert "15m" not in text
    assert "1h" not in text


def test_daily_kline_bullish_when_close_above_ema20_above_ema60():
    structure = m.build_stock_kline_structure("NVDA", daily_rows_from_closes(range(80, 141)), None)

    assert "日线偏强" in structure.summary
    assert structure.trend_state == "bullish"


def test_daily_kline_bearish_when_close_below_ema20_below_ema60():
    structure = m.build_stock_kline_structure("NVDA", daily_rows_from_closes(range(140, 79, -1)), None)

    assert "日线偏弱" in structure.summary
    assert structure.trend_state == "bearish"


def test_daily_kline_near_20_day_high_marks_resistance():
    rows = daily_rows_from_closes([100] * 40 + [110, 112, 114, 116, 118, 120, 121, 122, 123, 124, 124.5, 125])
    structure = m.build_stock_kline_structure("NVDA", rows, None)

    assert "接近压力" in structure.patterns or "接近压力" in structure.summary


def test_daily_kline_near_20_day_low_marks_support():
    rows = daily_rows_from_closes([140] * 40 + [130, 128, 126, 124, 122, 120, 119, 118, 117, 116, 115.5, 115])
    structure = m.build_stock_kline_structure("NVDA", rows, None)

    assert "接近支撑" in structure.patterns or "接近支撑" in structure.summary


def test_stock_kline_detail_text_outputs_support_and_resistance():
    structure = m.build_stock_kline_structure("NVDA", daily_rows_from_closes(range(80, 141)), None)
    text = m.stock_kline_detail_text(structure)

    assert "- 支撑：" in text
    assert "- 压力：" in text
    assert structure.support_levels
    assert structure.resistance_levels


def test_stock_kline_summary_text_none_is_insufficient():
    assert m.stock_kline_summary_text(None) == "K线：数据不足"


def test_stock_kline_summary_text_prioritizes_pressure_and_support():
    pressure = m.StockKlineStructure("NVDA", 5, 0, 0, 5, "日线偏强", ["接近压力"], [100.0], [120.0], "bullish", "daily_only")
    support = m.StockKlineStructure("NVDA", 5, 0, 0, 5, "日线偏弱", ["接近支撑"], [100.0], [120.0], "bearish", "daily_only")

    assert m.stock_kline_summary_text(pressure) == "K线：接近压力"
    assert m.stock_kline_summary_text(support) == "K线：接近支撑"
