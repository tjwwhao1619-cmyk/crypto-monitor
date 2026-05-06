
import derivatives_monitor as m


def test_signal_verdict_high_confluence_long():
    verdict = m.build_signal_verdict({
        "kind": "discovery",
        "evidence_score": "9",
        "leading_score": "7",
        "kline_score": "8",
        "risk_score": "2",
        "evidence_direction": "看多",
        "leading_direction": "偏多",
        "flow_trend_label": "多周期共振流入",
        "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
        "kline_text": "15m/1h K线转强 15m 突破近20根高点",
    })

    assert verdict.final_direction == "看多"
    assert verdict.confidence == "高"
    assert verdict.entry_state == "可跟踪"
    assert verdict.long_score >= 85
    assert verdict.entry_score >= 70
    assert verdict.risk_score <= 25
    assert verdict.conflict_score <= 30
    assert verdict.long_score > verdict.risk_score


def test_signal_verdict_long_high_requires_kline_and_external_confirm():
    verdict = m.build_signal_verdict({
        "kind": "discovery",
        "evidence_score": "9",
        "leading_score": "7",
        "kline_score": "8",
        "risk_score": "2",
        "evidence_direction": "看多",
        "leading_direction": "偏多",
        "flow_trend_label": "多周期共振流入",
        "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 OI增仓推涨 主力建仓",
        "kline_text": "15m/1h K线转强 15m 突破近20根高点",
    })

    assert not (verdict.final_direction == "看多" and verdict.confidence == "高")


def test_signal_verdict_long_high_blocked_by_pressure_and_weak_mid_term():
    verdict = m.build_signal_verdict({
        "kind": "discovery",
        "evidence_score": "9",
        "leading_score": "7",
        "kline_score": "8",
        "risk_score": "2",
        "evidence_direction": "看多",
        "leading_direction": "偏多",
        "flow_trend_label": "短强中弱",
        "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨 主力建仓",
        "risk_text": "压力位 长上影 中线资金不支持",
        "kline_text": "15m/1h K线转强 15m 突破近20根高点",
    })

    assert not (verdict.final_direction == "看多" and verdict.confidence == "高")


def test_signal_verdict_medium_long_allows_flow_divergence_as_pullback_watch():
    verdict = m.build_signal_verdict({
        "kind": "discovery",
        "evidence_score": "10",
        "leading_score": "6",
        "kline_score": "6",
        "risk_score": "3",
        "evidence_direction": "看多",
        "leading_direction": "偏多",
        "flow_trend_label": "资金分歧",
        "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
        "kline_text": "4h延续强",
    })

    assert verdict.final_direction == "看多"
    assert verdict.confidence == "中"
    assert verdict.entry_state == "等回踩确认"
    assert verdict.long_score >= 70
    assert verdict.risk_score <= 45
    assert verdict.conflict_score <= 55


def test_signal_verdict_medium_long_blocked_by_obvious_pressure():
    verdict = m.build_signal_verdict({
        "kind": "discovery",
        "evidence_score": "10",
        "leading_score": "6",
        "kline_score": "6",
        "risk_score": "2",
        "evidence_direction": "看多",
        "leading_direction": "偏多",
        "flow_trend_label": "多周期共振流入",
        "evidence_summary": "主动买盘强 短线主力流入 中线主力流入 现货承接 OI增仓推涨",
        "risk_text": "压力位很明显",
        "kline_text": "4h延续强",
    })

    assert verdict.final_direction == "仅观察"


def test_signal_verdict_risk_dominant():
    verdict = m.build_signal_verdict({
        "kind": "top_risk",
        "evidence_score": "3",
        "leading_score": "2",
        "kline_score": "5",
        "risk_score": "8",
        "evidence_direction": "看空",
        "flow_trend_label": "中长线派发",
        "risk_text": "主动卖盘强 高位拥挤 长上影 压力位 中线资金不支持 长线资金不支持",
    })

    assert verdict.final_direction == "看空风险"
    assert verdict.confidence in {"低", "中"}
    assert verdict.entry_state == "等跌破确认"
    assert verdict.risk_score >= 65
    assert verdict.action_text == "等跌破确认，不追空"


def test_signal_verdict_internal_conflict():
    verdict = m.build_signal_verdict({
        "kind": "top_risk",
        "evidence_score": "8",
        "leading_score": "6",
        "kline_score": "8",
        "risk_score": "4",
        "evidence_direction": "看多",
        "leading_direction": "偏多",
        "flow_trend_label": "短强中弱",
        "evidence_summary": "现货/DEX/外部确认 现货承接 OI增仓推涨 主力建仓 短线主力流入",
        "risk_text": "主动卖盘强 中线资金不支持 压力位",
        "kline_text": "15m/1h K线转强 15m 突破近20根高点",
    })

    assert verdict.final_direction == "多空分歧"
    assert verdict.confidence in {"低", "中"}
    assert verdict.entry_state == "暂不站队"
    assert "暂不站队" in verdict.action_text


def test_signal_verdict_low_certainty_observe():
    verdict = m.build_signal_verdict({
        "kind": "discovery",
        "evidence_score": "2",
        "leading_score": "2",
        "kline_score": "3",
        "risk_score": "2",
        "evidence_direction": "观察",
        "flow_trend_label": "资金分歧",
    })

    assert verdict.final_direction == "仅观察"
    assert verdict.confidence == "低"
