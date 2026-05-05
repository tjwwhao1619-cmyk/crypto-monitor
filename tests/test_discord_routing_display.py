import sys
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

    assert channel == "alerts"
    assert channel not in {"main", "risk", "high-confidence", "high_confidence"}


def test_conflict_observe_channel_prefers_observe_when_configured():
    decision = m.discord_conflict_route_decision(80)
    sig = m.Signal("ADAUSDT", "top_risk", 1, "risk", "", "k", snapshot("ADAUSDT"))

    channel = m.discord_channel_for_route(decision, sig, {"main": "1", "risk": "2", "alerts": "3", "observe": "4"})

    assert channel == "observe"
    assert channel != "main"


def test_conflict_title_channel_fallback_alerts_without_observe():
    corrected = m.discord_correct_route_channel_mismatch(
        m.DISCORD_ROUTE_CONFLICT_OBSERVE,
        "main",
        {"main": "1", "alerts": "3"},
        symbol="ADAUSDT",
        kind="conflict_observe",
        title="🟡 多空分歧观察 ADA/USDT",
    )

    assert corrected == "alerts"


def test_conflict_title_channel_defense_corrects_main_to_alerts(caplog):
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
        assert monitor.enqueue_discord_message(item)

    queued = monitor.discord_outbound_queue.get_nowait()
    assert queued.channel_key == "alerts"
    assert "Discord conflict channel mismatch corrected: symbol=TSTUSDT" in caplog.text


def test_conflict_observe_copy_and_long_evidence_sanitized(monkeypatch):
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: price_action("4h箱体未突破", long_score=2))
    decision = m.discord_conflict_route_decision(80)
    sig = m.Signal("ADAUSDT", "top_risk", 1, "risk", "风险 看空 主动卖盘强 高位拥挤 派发", "k", snapshot("ADAUSDT"))

    title = m.discord_signal_title_for_route(sig, "B", decision)
    fields = m.discord_signal_fields(sig, "B", 60, "smoke", decision)
    text = "\n".join([title] + [f"{name}\n{value}" for name, value, _inline in fields])
    long_value = next(value for name, value, _inline in fields if name == "看多证据")

    assert "多空分歧观察" in text
    assert "暂不站队" in text
    for forbidden in ("高位拥挤", "派发", "主动卖盘强"):
        assert forbidden not in long_value


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
