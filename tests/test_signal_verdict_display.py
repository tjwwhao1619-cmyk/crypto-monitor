import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import derivatives_monitor as m


def snapshot(symbol="ZECUSDT"):
    return m.MarketSnapshot(
        symbol=symbol,
        price_change_percent=2,
        oi_change_percent=3,
        global_long_short_ratio=1.1,
        top_position_ratio=1.2,
        top_account_ratio=1.1,
        taker_buy_sell_ratio=1.08,
        funding_rate_percent=0.01,
        confirm_price_change_percent=0.5,
        confirm_oi_change_percent=1.0,
        net_flow_usd={"5m": 1000, "15m": 2000, "1h": 3000, "4h": 5000, "12h": 5000, "24h": 5000, "72h": 5000},
        net_flow_ratio={},
        price_position_24h=55,
        high_24h=10,
        low_24h=8,
        quote_volume_24h=1_000_000,
        volume_ratio_24h=1.2,
        close_price=9.0,
        spot_price=None,
        price_change_periods=None,
    )


def price_action(label="4h延续强"):
    return m.MultiTimeframePriceAction(
        score=6,
        label=label,
        direction="long",
        short_score=6,
        mid_score=6,
        long_score=8,
        short_label="短线强",
        mid_label="中期强",
        long_label="4h延续强",
        items=[label],
        risk_items=[],
        patterns=[],
        recommendation="等待回踩确认",
    )


def test_signal_verdict_medium_long_display_in_single_embed(monkeypatch):
    pa = price_action()
    sig = m.Signal("ZECUSDT", "discovery", 1, "启动", "现货承接", "k", snapshot())
    decision = m.RouteDecision(m.DISCORD_ROUTE_OBSERVE, "观察", "display only", 70, True, ["observe"])
    monkeypatch.setattr(m, "safe_multi_timeframe_price_action", lambda *args, **kwargs: pa)
    monkeypatch.setattr(m, "conviction_score", lambda *args, **kwargs: (88, "高", "smoke"))
    monkeypatch.setattr(
        m,
        "evidence_score",
        lambda *args, **kwargs: (
            10,
            "看多",
            "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
            [],
        ),
    )
    monkeypatch.setattr(
        m,
        "leading_signal_score",
        lambda *args, **kwargs: m.LeadingSignalScore(6, "long", "偏多", ["主力建仓"], 6, 0),
    )
    monkeypatch.setattr(m, "flow_horizon_scores", lambda *args, **kwargs: (6, 6, 6, "资金分歧", "smoke"))
    monkeypatch.setattr(m, "trap_risk_score", lambda *args, **kwargs: (3, "中", "smoke"))
    monkeypatch.setattr(m, "discord_signal_action", lambda *args, **kwargs: ("强烈买入", "legacy", False))

    fields = m.discord_signal_fields(sig, "B", 70, "smoke", decision)
    text = "\n".join(value for _name, value, _inline in fields)
    action_value = next(value for name, value, _inline in fields if name == "怎么做")

    assert action_value == "看多重点观察，等回踩确认，不追高"
    assert "高确定性" not in text
    assert "强烈买入" not in text
    assert "SignalVerdict" not in text


def test_signal_verdict_risk_medium_display_in_candidate():
    row = {
        "symbol": "TSTUSDT",
        "kind": "top_risk",
        "conviction_score": "80",
        "signal_quality_score": "50",
        "evidence_score": "2",
        "evidence_direction": "看空",
        "risk_score": "8",
        "trap_risk_score": "8",
        "risk_text": "主动卖盘强 高位拥挤 长上影 压力位",
        "flow_trend_label": "中长线派发",
    }
    decision = m.RouteDecision(m.DISCORD_ROUTE_OBSERVE, "观察", "risk observe", 70, True, ["risk"])

    text = "\n".join(m.discord_candidate_brief_lines(row, decision, None))

    assert "SignalVerdict｜裁判方向：看空风险" in text
    assert "确定性：中" in text
    assert "入场状态：等跌破确认" in text
    assert "裁判建议：风险提醒候选，等跌破确认，不追空" in text
    assert "操作: 风险提醒候选，等跌破确认，不追空" in text


def test_signal_verdict_conflict_display_in_candidate():
    row = {
        "symbol": "SUIUSDT",
        "kind": "top_risk",
        "conviction_score": "55",
        "signal_quality_score": "60",
        "evidence_score": "8",
        "evidence_direction": "看多",
        "evidence_summary": "现货承接 OI增仓推涨 主力建仓",
        "leading_score": "6",
        "flow_trend_label": "短强中弱",
        "kline_text": "15m/1h K线转强 15m 突破近20根高点",
    }
    decision = m.discord_conflict_route_decision(80)

    text = "\n".join(m.discord_candidate_brief_lines(row, decision, None))

    assert "SignalVerdict｜裁判方向：多空分歧" in text
    assert "入场状态：暂不站队" in text
    assert "裁判建议：暂不站队，等待15m/1h方向重新一致" in text
    assert "操作: 暂不站队，等待15m/1h方向重新一致" in text
