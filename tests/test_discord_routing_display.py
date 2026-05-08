import sys
import logging
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import derivatives_monitor as m
import queue
import threading


def snapshot(symbol="SKYAIUSDT", price_position=55, price_change=2, oi_change=3, funding=0.01):
    return m.MarketSnapshot(
        symbol=symbol,
        price_change_percent=price_change,
        oi_change_percent=oi_change,
        global_long_short_ratio=1.1,
        top_position_ratio=1.2,
        top_account_ratio=1.1,
        taker_buy_sell_ratio=1.08,
        funding_rate_percent=funding,
        confirm_price_change_percent=0.5,
        confirm_oi_change_percent=1.0,
        net_flow_usd={
            "5m": 1000,
            "15m": 2000,
            "1h": -500,
            "4h": 5000,
            "12h": 5000,
            "24h": 5000,
            "72h": 5000,
        },
        net_flow_ratio={},
        price_position_24h=price_position,
        high_24h=10,
        low_24h=8,
        quote_volume_24h=1_000_000,
        volume_ratio_24h=1.2,
        close_price=9.9 if price_position >= 80 else 9.0,
        spot_price=None,
        price_change_periods=None,
    )


def price_action(label="多周期结构偏强", long_score=3):
    return m.MultiTimeframePriceAction(
        score=7,
        label=label,
        direction="long",
        short_score=7,
        mid_score=6,
        long_score=long_score,
        short_label="短线强",
        mid_label="中期强",
        long_label="1w K线数据不足",
        items=["多周期结构偏强"],
        risk_items=["1w K线数据不足"],
        patterns=[],
        recommendation="等待逼空确认",
    )


def priority_observe_decision():
    return m.RouteDecision(
        m.DISCORD_ROUTE_PRIORITY_OBSERVE,
        "重点观察",
        "观察层高分组合; 先观察; 不追高",
        90,
        True,
        ["priority_observe"],
    )


def observe_decision():
    return m.RouteDecision(
        m.DISCORD_ROUTE_OBSERVE,
        "观察",
        "risk observe",
        70,
        True,
        ["risk"],
    )


def realtime_decision():
    return m.RouteDecision(
        m.DISCORD_ROUTE_REALTIME,
        "实时",
        "legacy realtime",
        100,
        False,
        ["realtime"],
    )


def section_text(text, title):
    lines = text.splitlines()
    start = lines.index(title)
    end = len(lines)
    section_titles = {
        "🟢 高确定性",
        "🟡 看多重点观察",
        "🟠 风险候选",
        "🟡 多空分歧",
        "👀 普通观察/静默",
    }
    for index in range(start + 1, len(lines)):
        if lines[index] in section_titles:
            end = index
            break
    return "\n".join(lines[start:end])


def patch_conservative_inputs(monkeypatch, pa):
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: pa)
    monkeypatch.setattr(m, "conviction_score", lambda *args, **kwargs: (88, "高", "smoke"))
    monkeypatch.setattr(
        m,
        "leading_signal_score",
        lambda *args, **kwargs: m.LeadingSignalScore(
            4,
            "risk",
            "资金分歧/方向不明确",
            ["疑似主力出货", "派发"],
            2,
            6,
        ),
    )
    monkeypatch.setattr(
        m,
        "evidence_score",
        lambda *args, **kwargs: (
            11,
            "看多",
            "主力建仓，现货确认，等待逼空确认",
            [
                m.EvidenceItem("中线持续建仓", 2, "positive", "mid", "OI"),
                m.EvidenceItem("多周期共振流入", 2, "positive", "long", "FLOW"),
            ],
        ),
    )
    monkeypatch.setattr(m, "flow_horizon_scores", lambda *args, **kwargs: (6, 10, 7, "中长线吸筹", "smoke"))
    monkeypatch.setattr(m, "trap_risk_score", lambda *args, **kwargs: (2, "低", "现货/DEX/外部确认偏弱"))
    monkeypatch.setattr(m, "squeeze_state", lambda *args, **kwargs: ("无明显挤压", 3, "等待逼空确认"))
    monkeypatch.setattr(m, "basis_state", lambda *args, **kwargs: (0.01, "正常", "smoke"))
    monkeypatch.setattr(m, "action_label", lambda *args, **kwargs: ("建议观察，等确认入场", "先观察"))


def test_priority_observe_bullish_conflict_display_is_conservative(monkeypatch):
    pa = price_action()
    patch_conservative_inputs(monkeypatch, pa)
    sig = m.Signal("SKYAIUSDT", "bottom_reversal", 1, "smoke", "", "k", snapshot())
    decision = priority_observe_decision()

    title = m.discord_signal_title_for_route(sig, "A", decision)
    fields = m.discord_signal_fields(sig, "A", 80, "smoke", decision)
    text = "\n".join([title] + [f"{name}\n{value}" for name, value, _inline in fields])

    assert "🟡 重点观察" in title
    assert "资金分歧，长线有支撑但短线未确认" in text
    assert "短线承接有迹象，但资金和外部确认分歧，等回踩确认" in text
    for forbidden in (
        "高把握信号",
        "中长线吸筹",
        "主力建仓",
        "多周期共振流入",
        "等待逼空确认",
    ):
        assert forbidden not in text


def test_priority_observe_channel_never_falls_back_to_main_without_observe():
    decision = priority_observe_decision()
    sig = m.Signal("BUSDT", "bottom_reversal", 1, "smoke", "", "k", None)
    channel_ids = {"main": "1111111111110001", "alerts": "2222222222226118"}

    channel = m.discord_channel_for_route(decision, sig, channel_ids)

    assert decision.route == m.DISCORD_ROUTE_PRIORITY_OBSERVE
    assert channel == "alerts"
    assert channel != "main"


def test_breakout_entry_title_high_risk_demoted_track_keeps_breakout(monkeypatch):
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: price_action())
    decision = m.RouteDecision(
        m.DISCORD_ROUTE_PRIORITY_OBSERVE,
        "重点观察",
        "breakout",
        70,
        True,
        ["breakout_watch"],
    )
    high_risk_sig = m.Signal(
        "HIGHUSDT",
        "discovery",
        1,
        "smoke",
        "",
        "k",
        snapshot("HIGHUSDT", price_position=92, price_change=6, oi_change=0, funding=0.1),
    )
    track_sig = m.Signal(
        "TRACKUSDT",
        "discovery",
        1,
        "smoke",
        "",
        "k",
        snapshot("TRACKUSDT", price_position=55, price_change=2, oi_change=3, funding=0.01),
    )

    high_title = m.discord_signal_title_for_route(high_risk_sig, "D", decision)
    track_title = m.discord_signal_title_for_route(track_sig, "D", decision)

    assert "高位观察" in high_title
    assert "爆发观察" not in high_title
    assert "爆发观察" in track_title


def test_evidence_score_display_is_capped_at_10(monkeypatch):
    monkeypatch.setattr(
        m,
        "evidence_score",
        lambda *args, **kwargs: (11, "看多", "资金分歧，观望", []),
    )
    value = m.discord_evidence_field_value(snapshot("BUSDT"), None)

    assert "10/10" in value
    assert "11/10" not in value


def test_discord_symbol_long_then_risk_merges_conflict_observe(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.discord_config = m.DiscordConfig(True, "token", {"alerts": "2222222222226118"})
    monitor.discord_outbound_queue = queue.Queue()
    monitor.discord_signal_cooldowns = {}
    monitor.discord_symbol_signal_memory = {}
    monitor.discord_symbol_signal_memory_lock = threading.Lock()

    decisions = [priority_observe_decision(), observe_decision()]
    monkeypatch.setattr(m, "discord_route_decision", lambda *args, **kwargs: decisions.pop(0))
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: price_action("4h箱体未突破", long_score=2))

    long_sig = m.Signal("ADAUSDT", "bottom_reversal", 1, "看多", "direction long 看多", "k1", snapshot("ADAUSDT"))
    risk_sig = m.Signal("ADAUSDT", "top_risk", 1, "风险", "风险 看空 主动卖盘强 高位拥挤", "k2", snapshot("ADAUSDT"))

    monitor.route_discord_signal(long_sig, "A", 80, "smoke")
    monitor.route_discord_signal(risk_sig, "B", 60, "smoke")

    items = list(monitor.discord_outbound_queue.queue)
    assert len(items) == 1
    item = items[0]
    assert item.kind == "conflict_observe"
    assert item.final_route == m.DISCORD_ROUTE_CONFLICT_OBSERVE
    assert item.channel_key == "alerts"
    assert "conflict_observe" in item.route_tags


def test_conflict_observe_channel_is_not_main_or_risk():
    decision = m.discord_conflict_route_decision(80)
    sig = m.Signal("ADAUSDT", "top_risk", 1, "risk", "", "k", snapshot("ADAUSDT"))

    channel = m.discord_channel_for_route(decision, sig, {"main": "1", "risk": "2", "alerts": "3"})

    assert channel == "suppress"
    assert channel not in {"main", "risk", "alerts", "high-confidence", "high_confidence"}


def test_conflict_observe_channel_prefers_observe_when_configured():
    decision = m.discord_conflict_route_decision(80)
    sig = m.Signal("TSTUSDT", "top_risk", 1, "risk", "", "k", snapshot("TSTUSDT"))

    channel = m.discord_channel_for_route(decision, sig, {"main": "1", "risk": "2", "alerts": "3", "observe": "4"})

    assert channel == "observe"
    assert channel != "main"


def test_main_momentum_watch_btc_routes_to_main_asset():
    decision = m.RouteDecision(m.DISCORD_ROUTE_REALTIME, "实时", "main", 90, False, [])
    sig = m.Signal("BTCUSDT", "main_momentum_watch", 1, "main", "", "k", snapshot("BTCUSDT"))

    channel = m.discord_channel_for_route(decision, sig, {"main_asset": "1500895602542903336", "alerts": "2"})

    assert channel == "main_asset"


def test_mainstream_observe_routes_to_main_asset():
    sig = m.Signal("ADAUSDT", "bottom_reversal", 1, "observe", "", "k", snapshot("ADAUSDT"))

    channel = m.discord_channel_for_route(observe_decision(), sig, {"main_asset": "1500895602542903336", "alerts": "2", "observe": "3"})

    assert channel == "main_asset"


def test_main_asset_missing_fallback_alerts_not_main():
    sig = m.Signal("BTCUSDT", "main_trend_watch", 1, "main", "", "k", snapshot("BTCUSDT"))
    decision = m.RouteDecision(m.DISCORD_ROUTE_REALTIME, "实时", "main", 90, False, [])

    channel = m.discord_channel_for_route(decision, sig, {"main": "1", "alerts": "2"})

    assert channel == "alerts"
    assert channel != "main"


def test_suppressed_digest_channel_only_digest():
    decision = m.RouteDecision(m.DISCORD_ROUTE_DIGEST, "静默", "quiet", 10, True, [])
    sig = m.Signal("ADAUSDT", "bottom_reversal", 1, "risk", "", "k", snapshot("ADAUSDT"))

    channel = m.discord_channel_for_payload(
        "suppressed_digest",
        route_decision=decision,
        signal=sig,
        title="🧾 静默信号摘要",
        channel_ids={"main": "1", "alerts": "2", "digest": "3"},
    )

    assert channel == "digest"


def test_market_summary_channel_only_summary():
    channel = m.discord_channel_for_payload(
        "summary",
        title="📊 每小时市场简报",
        channel_ids={"summary": "1", "onchain": "2", "digest": "3"},
    )

    assert channel == "summary"


def test_onchain_payload_channel_only_onchain():
    channel = m.discord_channel_for_payload(
        "onchain",
        title="🔷 CoinGlass 聚合资金摘要",
        channel_ids={"summary": "1", "onchain": "2"},
    )

    assert channel == "onchain"


def test_stock_token_payload_channel_only_stock_token():
    channel = m.discord_channel_for_payload(
        "stock_token",
        title="美股代币观察摘要",
        channel_ids={"summary": "1", "stock_token": "2"},
    )

    assert channel == "stock_token"


def test_main_title_priority_observe_corrected_to_alerts():
    corrected = m.discord_correct_route_channel_mismatch(
        m.DISCORD_ROUTE_REALTIME,
        "main",
        {"main": "1", "alerts": "3"},
        symbol="ADAUSDT",
        kind="bottom_reversal",
        title="🟡 重点观察 ADA/USDT",
    )

    assert corrected == "alerts"


def test_discord_channel_command_outputs_matrix_keywords():
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.discord_config = m.DiscordConfig(
        True,
        "token",
        {"main": "1", "main_asset": "6", "alerts": "2", "summary": "3", "onchain": "4", "stock_token": "5"},
    )

    text = monitor.discord_command_response("!频道")

    assert "Discord 频道治理矩阵" in text
    assert "main id configured?" in text
    assert "main_asset id configured?" in text
    assert "moonshot id configured?" in text
    assert "主流雷达" in text
    assert "妖币交易工作台" in text
    assert "priority_observe" in text
    assert "suppressed_digest" in text
    assert "美股代币" in text


def test_conflict_title_channel_does_not_fallback_to_alerts_without_observe():
    corrected = m.discord_correct_route_channel_mismatch(
        m.DISCORD_ROUTE_CONFLICT_OBSERVE,
        "main",
        {"main": "1", "alerts": "3"},
        symbol="ADAUSDT",
        kind="conflict_observe",
        title="🟡 多空分歧观察 ADA/USDT",
    )

    assert corrected == "suppress"


def test_conflict_title_channel_defense_suppresses_without_observe_or_alt(caplog):
    item = m.DiscordOutboundMessage(
        channel_key="main",
        title="🟡 多空分歧观察 TST/USDT",
        symbol="TSTUSDT",
        kind="conflict_observe",
        final_route=m.DISCORD_ROUTE_CONFLICT_OBSERVE,
    )
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.discord_config = m.DiscordConfig(True, "token", {"main": "1", "alerts": "3"})
    monitor.discord_outbound_queue = queue.Queue()

    with caplog.at_level("WARNING"):
        assert not monitor.enqueue_discord_message(item)

    assert "Discord conflict channel mismatch corrected: symbol=TSTUSDT" in caplog.text


def init_discord_monitor_for_routing(channel_ids=None):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.discord_config = m.DiscordConfig(True, "token", channel_ids or {"alerts": "2222222222226118"})
    monitor.discord_outbound_queue = queue.Queue()
    monitor.discord_signal_cooldowns = {}
    monitor.discord_symbol_signal_memory = {}
    monitor.discord_symbol_signal_memory_lock = threading.Lock()
    monitor.discord_visible_signal_recent = m.deque(maxlen=500)
    monitor.discord_visible_signal_lock = threading.Lock()
    monitor.discord_route_audit = m.deque(maxlen=m.DISCORD_ROUTE_AUDIT_LIMIT)
    monitor.discord_route_audit_lock = threading.Lock()
    return monitor


def test_duplicate_visible_risk_observe_suppresses_second(monkeypatch):
    monitor = init_discord_monitor_for_routing()
    decision = m.RouteDecision(m.DISCORD_ROUTE_OBSERVE, "观察", "risk observe", 70, True, ["risk"])
    monkeypatch.setattr(m, "discord_route_decision", lambda *args, **kwargs: decision)
    monkeypatch.setattr(m, "conviction_score", lambda *args, **kwargs: (50, "中", "smoke"))
    monkeypatch.setattr(m, "trap_risk_score", lambda *args, **kwargs: (4, "中", "smoke"))
    sig1 = m.Signal("UBUSDT", "top_risk", 1, "risk", "风险", "k1", snapshot("UBUSDT", price_change=1, oi_change=2))
    sig2 = m.Signal("UBUSDT", "top_risk", 1, "risk", "风险", "k2", snapshot("UBUSDT", price_change=1.5, oi_change=3))

    monitor.route_discord_signal(sig1, "B", 24, "old")
    monitor.route_discord_signal(sig2, "B", 32, "new")

    assert monitor.discord_outbound_queue.qsize() == 1
    audit_text = monitor.format_discord_channel_audit_response()
    assert "dedupe_suppress" in audit_text


def test_duplicate_visible_allows_route_upgrade(monkeypatch):
    monitor = init_discord_monitor_for_routing()
    decisions = [
        m.RouteDecision(m.DISCORD_ROUTE_OBSERVE, "观察", "observe", 40, True, []),
        m.RouteDecision(m.DISCORD_ROUTE_PRIORITY_OBSERVE, "重点观察", "upgrade", 80, True, []),
    ]
    monkeypatch.setattr(m, "discord_route_decision", lambda *args, **kwargs: decisions.pop(0))
    monkeypatch.setattr(m, "conviction_score", lambda *args, **kwargs: (50, "中", "smoke"))
    monkeypatch.setattr(m, "trap_risk_score", lambda *args, **kwargs: (2, "低", "smoke"))
    sig1 = m.Signal("UBUSDT", "bottom_reversal", 1, "long", "direction long 看多", "k1", snapshot("UBUSDT"))
    sig2 = m.Signal("UBUSDT", "bottom_reversal", 1, "long", "direction long 看多", "k2", snapshot("UBUSDT"))

    monitor.route_discord_signal(sig1, "C", 30, "old")
    monitor.route_discord_signal(sig2, "A", 60, "new")

    assert monitor.discord_outbound_queue.qsize() == 2


def test_onchain_balance_na_humanized_and_not_repeated():
    text = m.format_onchain_brief(
        snapshot("BTCUSDT"),
        "CoinGlass聚合: n/a\nCoinGlass订单簿: n/a",
        "暂无明显现货/DEX/外部确认",
        compact=True,
    )

    assert "交易所余额：暂无数据" in text
    assert "n/a / 7d n/a / 30d n/a" not in text
    assert text.count("CoinGlass交易所余额") <= 1


def test_stablecoin_summary_mentions_not_exchange_buying(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monkeypatch.setattr(monitor, "latest_stablecoin_supply_rows", lambda assets: {asset: [] for asset in assets})

    text = monitor.format_stablecoin_liquidity_radar()

    assert "不等于交易所买盘" in text


def test_onchain_title_corrected_from_summary_or_main():
    corrected_summary = m.discord_correct_route_channel_mismatch(
        None,
        "summary",
        {"onchain": "9", "summary": "1"},
        title="🔷 CoinGlass主流资金",
    )
    corrected_main = m.discord_correct_route_channel_mismatch(
        None,
        "main",
        {"onchain": "9", "main": "1"},
        title="🟠 稳定币流动性背景",
    )

    assert corrected_summary == "onchain"
    assert corrected_main == "onchain"


def test_conflict_observe_copy_and_long_evidence_sanitized(monkeypatch):
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: price_action("4h箱体未突破", long_score=2))
    decision = m.discord_conflict_route_decision(80)
    sig = m.Signal("ADAUSDT", "top_risk", 1, "risk", "风险 看空 主动卖盘强 高位拥挤 派发", "k", snapshot("ADAUSDT"))

    title = m.discord_signal_title_for_route(sig, "B", decision)
    fields = m.discord_signal_fields(sig, "B", 60, "smoke", decision)
    text = "\n".join([title] + [f"{name}\n{value}" for name, value, _inline in fields])
    long_value = next(value for name, value, _inline in fields if name == "看多理由")

    assert "多空分歧观察" in text
    assert "暂不站队" in text
    for forbidden in ("高位拥挤", "派发", "主动卖盘强"):
        assert forbidden not in long_value


def test_single_signal_internal_conflict_risk_with_bullish_kline_display(monkeypatch):
    pa = m.MultiTimeframePriceAction(
        score=8,
        label="15m/1h K线转强",
        direction="long",
        short_score=8,
        mid_score=8,
        long_score=3,
        short_label="15m 突破近20根高点",
        mid_label="1h转强",
        long_label="大周期下跌趋势反弹",
        items=["15m/1h K线转强", "15m 突破近20根高点", "放量阳线"],
        risk_items=["大周期下跌趋势反弹", "中线资金不支持", "长线资金不支持"],
        patterns=[],
        recommendation="短线强，中线不支持",
    )
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: pa)
    monkeypatch.setattr(m, "conviction_score", lambda *args, **kwargs: (55, "中", "smoke"))
    monkeypatch.setattr(
        m,
        "leading_signal_score",
        lambda *args, **kwargs: m.LeadingSignalScore(6, "neutral", "偏多 6/10", ["偏多"], 6, 2),
    )
    monkeypatch.setattr(
        m,
        "evidence_score",
        lambda *args, **kwargs: (
            3,
            "观察",
            "现货/DEX/外部确认，现货承接，OI增仓推涨",
            [
                m.EvidenceItem("OI增仓推涨", 2, "positive", "short", "OI"),
                m.EvidenceItem("现货承接", 2, "positive", "spot", "SPOT"),
                m.EvidenceItem("主动卖盘强", 2, "risk", "short", "TAKER"),
            ],
        ),
    )
    monkeypatch.setattr(m, "flow_horizon_scores", lambda *args, **kwargs: (7, 3, 2, "短强中弱", "short strong"))
    sig = m.Signal("SUIUSDT", "top_exhaustion", 1, "风险", "短线强，中线不支持，现货承接", "k", snapshot("SUIUSDT"))

    decision = m.discord_route_decision(sig, sig.snapshot)
    title = m.discord_signal_title_for_route(sig, "B", decision)
    fields = m.discord_signal_fields(sig, "B", 60, "smoke", decision)
    channel = m.discord_channel_for_route(decision, sig, {"main": "1", "risk": "2", "alerts": "3", "observe": "4"})
    long_value = next(value for name, value, _inline in fields if name == "看多理由")
    text = "\n".join(value for _name, value, _inline in fields)

    assert decision.route == m.DISCORD_ROUTE_CONFLICT_OBSERVE
    assert "多空分歧观察" in title
    assert channel == "observe"
    assert channel not in {"risk", "main"}
    assert "短线结构转强，但中长线资金和大周期仍不支持，暂不站队" in text
    assert "等待 15m/1h 方向和中线资金重新一致" in text
    for forbidden in ("主动卖盘强", "中线资金不支持", "长线资金不支持", "下跌趋势反弹", "压力位"):
        assert forbidden not in long_value


def test_discord_candidates_single_internal_conflict_not_risk(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": "SUIUSDT",
            "kind": "top_risk",
            "conviction_score": "55",
            "evidence_score": "2",
            "evidence_direction": "观察",
            "evidence_summary": "现货/DEX/外部确认 现货承接 OI增仓推涨",
            "leading_score": "6",
            "leading_direction": "neutral",
            "leading_label": "偏多 6/10",
            "signal_quality_score": "60",
            "flow_trend_label": "短强中弱",
            "kline_score": "8",
            "kline_short_score": "8",
            "kline_mid_score": "8",
            "kline_text": "15m/1h K线转强 15m 突破近20根高点",
        },
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)

    text = monitor.format_discord_route_candidates_response()

    assert "🟡 多空分歧观察 SUI/USDT" in text
    assert "🟠 风险候选\n暂无" in text


def test_discord_candidates_show_single_conflict_for_same_symbol(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": "ADAUSDT",
            "kind": "bottom_reversal",
            "conviction_score": "60",
            "evidence_score": "8",
            "leading_score": "6",
            "signal_quality_score": "80",
            "flow_trend_label": "多周期共振流入",
        },
        {
            "symbol": "ADAUSDT",
            "kind": "top_risk",
            "conviction_score": "58",
            "evidence_score": "7",
            "leading_score": "5",
            "signal_quality_score": "70",
            "flow_trend_label": "资金分歧",
            "evidence_summary": "高位拥挤 主动卖盘强",
        },
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        m,
        "discord_route_decision",
        lambda row, context=None: priority_observe_decision() if row["kind"] == "bottom_reversal" else observe_decision(),
    )

    text = monitor.format_discord_route_candidates_response()

    assert text.count("ADA/USDT") == 1
    assert "🟡 多空分歧观察 ADA/USDT" in text
    assert "底部反转" not in text
    assert "风险信号" not in text


def test_discord_candidates_default_is_capped_and_keeps_verdict(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": f"TST{i}USDT",
            "kind": "discovery",
            "conviction_score": "80",
            "evidence_score": "10",
            "evidence_direction": "看多",
            "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
            "leading_score": "6",
            "leading_direction": "long",
            "signal_quality_score": "80",
            "spot_onchain_label": "强",
            "spot_onchain_score": "10",
            "flow_trend_label": "资金分歧",
            "kline_text": "4h延续强",
        }
        for i in range(20)
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "is_valid_binance_usdt_symbol", lambda _symbol: True)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(m, "discord_route_decision", lambda *_args, **_kwargs: priority_observe_decision())

    text = monitor.format_discord_route_candidates_response()
    candidate_count = sum(1 for line in text.splitlines() if line.startswith(("🟢 ", "🟡 ", "🔴 ")) and "｜把握" in line)

    assert len(text) <= 3500
    assert candidate_count <= 8
    assert candidate_count == 4
    assert "Verdict: 看多/中｜等回踩确认｜L" in text
    assert "旧路由: priority_observe" in text
    assert "已截断，使用 !候选 全部 查看更多。" in text


def test_discord_candidates_numeric_limit_caps_total(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": f"LIM{i}USDT",
            "kind": "discovery",
            "conviction_score": "80",
            "evidence_score": "10",
            "evidence_direction": "看多",
            "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
            "leading_score": "6",
            "leading_direction": "long",
            "signal_quality_score": "80",
            "spot_onchain_label": "强",
            "spot_onchain_score": "10",
            "flow_trend_label": "资金分歧",
            "kline_text": "4h延续强",
        }
        for i in range(20)
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "is_valid_binance_usdt_symbol", lambda _symbol: True)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(m, "discord_route_decision", lambda *_args, **_kwargs: priority_observe_decision())

    response = monitor.discord_command_response("!候选 15")
    text = "\n".join(value for _name, value, _inline in response.fields)
    candidate_count = sum(1 for line in text.splitlines() if line.startswith(("🟢 ", "🟡 ", "🔴 ")) and "｜把握" in line)

    assert candidate_count <= 15
    assert len(text) <= 3500
    assert "Verdict:" in text


def test_discord_candidates_all_splits_safe_embeds(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": f"ALL{i}USDT",
            "kind": "discovery",
            "conviction_score": "80",
            "evidence_score": "10",
            "evidence_direction": "看多",
            "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
            "leading_score": "6",
            "leading_direction": "long",
            "signal_quality_score": "80",
            "spot_onchain_label": "强",
            "spot_onchain_score": "10",
            "flow_trend_label": "资金分歧",
            "kline_text": "4h延续强",
        }
        for i in range(40)
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "is_valid_binance_usdt_symbol", lambda _symbol: True)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(m, "discord_route_decision", lambda *_args, **_kwargs: priority_observe_decision())

    response = monitor.discord_command_response("!候选 全部")

    assert isinstance(response, list)
    assert response
    for embed in response:
        text = "\n".join(value for _name, value, _inline in embed.fields)
        assert len(text) <= 3500
    assert any("Verdict:" in value for embed in response for _name, value, _inline in embed.fields)


def test_discord_candidates_realtime_but_low_verdict_goes_to_observe(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": "MUSDT",
            "kind": "discovery",
            "conviction_score": "100",
            "signal_quality_score": "74",
            "evidence_score": "2",
            "evidence_direction": "观察",
            "leading_score": "0",
            "flow_trend_label": "多周期共振流入",
        }
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(m, "discord_route_decision", lambda *_args, **_kwargs: realtime_decision())

    text = monitor.format_discord_route_candidates_response()

    assert "M 启动发现" not in section_text(text, "🟢 高确定性")
    assert "M 启动发现" in section_text(text, "👀 普通观察/静默")
    assert "Verdict: 仅观察/低" in text
    assert "旧路由: realtime" in text


def test_discord_candidates_conflict_route_with_low_conflict_not_conflict_section(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": "CFXUSDT",
            "kind": "discovery",
            "conviction_score": "100",
            "signal_quality_score": "100",
            "evidence_score": "2",
            "evidence_direction": "观察",
            "leading_score": "0",
            "flow_trend_label": "多周期共振流入",
        }
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(m, "discord_route_decision", lambda *_args, **_kwargs: m.discord_conflict_route_decision(90))

    text = monitor.format_discord_route_candidates_response()

    assert "CFX/USDT" not in section_text(text, "🟡 多空分歧")
    assert "CFX/USDT" in section_text(text, "👀 普通观察/静默")
    assert "Verdict: 多空分歧/低｜暂不站队｜L" in text
    assert "C0" in text
    assert "旧路由: conflict_observe" in text


def test_discord_candidates_high_verdict_goes_to_high_confidence(monkeypatch):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    rows = [
        {
            "symbol": "ZECUSDT",
            "kind": "discovery",
            "conviction_score": "100",
            "signal_quality_score": "100",
            "evidence_score": "9",
            "evidence_direction": "看多",
            "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
            "leading_score": "7",
            "leading_direction": "long",
            "spot_onchain_label": "强",
            "spot_onchain_score": "10",
            "flow_trend_label": "多周期共振流入",
            "kline_score": "8",
            "kline_text": "15m/1h K线转强 15m 突破近20根高点",
            "risk_score": "2",
        }
    ]
    monkeypatch.setattr(monitor, "load_recent_signal_rows", lambda *_args, **_kwargs: rows)
    monkeypatch.setattr(m, "light_multi_timeframe_price_action", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(m, "discord_route_decision", lambda *_args, **_kwargs: realtime_decision())

    text = monitor.format_discord_route_candidates_response()

    assert "ZEC 启动发现" in section_text(text, "🟢 高确定性")
    assert "Verdict: 看多/高｜可跟踪" in text
    assert "旧路由: realtime" in text


def test_discord_verdict_shadow_counts_and_logs_candidates(monkeypatch, caplog):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    sig = m.Signal("MUSDT", "discovery", 1, "启动", "smoke", "k", None)
    verdict = m.SignalVerdict(
        final_direction="仅观察",
        confidence="低",
        entry_state="仅观察",
        verdict_score=40,
        long_score=80,
        risk_score=0,
        conflict_score=0,
        entry_score=50,
        primary_reasons=[],
        risk_reasons=[],
        display_title="普通观察",
        action_text="确认不足",
    )
    monkeypatch.setattr(monitor, "build_discord_verdict_shadow", lambda *_args, **_kwargs: verdict)

    caplog.set_level(logging.INFO)
    monitor.record_discord_verdict_shadow(sig, m.DISCORD_ROUTE_REALTIME, "alerts", realtime_decision())

    assert monitor.discord_verdict_shadow_stats["realtime_low"] == 1
    assert monitor.discord_verdict_shadow_stats["long_high"] == 0
    assert "SignalVerdict shadow: symbol=MUSDT kind=discovery old_route=realtime old_channel_key=alerts" in caplog.text
    assert "Verdict shadow downgrade candidate: symbol=MUSDT" in caplog.text


def test_discord_verdict_shadow_upgrade_candidate_count(monkeypatch, caplog):
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    sig = m.Signal("ZECUSDT", "discovery", 1, "启动", "smoke", "k", None)
    verdict = m.SignalVerdict(
        final_direction="看多",
        confidence="高",
        entry_state="可跟踪",
        verdict_score=90,
        long_score=92,
        risk_score=5,
        conflict_score=0,
        entry_score=95,
        primary_reasons=[],
        risk_reasons=[],
        display_title="高确定性看多",
        action_text="可跟踪，仍需按止损执行",
    )
    monkeypatch.setattr(monitor, "build_discord_verdict_shadow", lambda *_args, **_kwargs: verdict)

    caplog.set_level(logging.INFO)
    monitor.record_discord_verdict_shadow(sig, m.DISCORD_ROUTE_OBSERVE, "alt_watch", observe_decision())

    assert monitor.discord_verdict_shadow_stats["long_high"] == 1
    assert monitor.discord_verdict_shadow_stats["high_non_realtime"] == 1
    assert "Verdict shadow upgrade candidate: symbol=ZECUSDT" in caplog.text


def test_discord_verdict_shadow_stats_command():
    monitor = m.DerivativesMonitor.__new__(m.DerivativesMonitor)
    monitor.discord_verdict_shadow_stats = {
        "long_high": 2,
        "long_mid": 3,
        "risk_mid": 4,
        "conflict": 5,
        "realtime_low": 6,
        "high_non_realtime": 7,
    }
    monitor.discord_verdict_shadow_lock = threading.Lock()

    response = monitor.discord_command_response("!裁判统计")
    text = "\n".join(value for _name, value, _inline in response.fields)

    assert response.title == "SignalVerdict 影子统计"
    assert "看多/高: 2" in text
    assert "看多/中: 3" in text
    assert "看空风险/中: 4" in text
    assert "多空分歧: 5" in text
    assert "旧实时但裁判低确定: 6" in text
    assert "裁判高确定但旧非实时: 7" in text
