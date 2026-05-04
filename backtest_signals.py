import argparse
import contextlib
import csv
import datetime as dt
import io
import statistics
import sys
import time
from pathlib import Path

import requests
import yaml

BASE = "https://fapi.binance.com"
BULL = {"discovery", "hot_breakout", "bottom_reversal", "main_trend_watch", "main_momentum_watch"}
BEAR = {"top_risk", "distribution", "top_exhaustion", "main_risk_watch"}
HORIZONS = {"15m": 900, "1h": 3600, "4h": 14400, "12h": 43200, "24h": 86400}
REPORT_HORIZONS = ["15m", "1h", "4h", "12h", "24h"]
LONG_FLOW_GROUPS = (
    ("0-3", "长周期弱", 0, 3),
    ("4-6", "长周期分歧", 4, 6),
    ("7-9", "长周期支持", 7, 9),
)
MAIN_ASSET_SCORE_GROUPS = (
    ("0-39", "主流弱", 0, 39),
    ("40-59", "主流中性", 40, 59),
    ("60-79", "主流强", 60, 79),
    ("80-100", "主流极强", 80, 100),
)
TRAP_RISK_GROUPS = (
    ("0-2", "低", 0, 2),
    ("3-5", "中", 3, 5),
    ("6-7", "高", 6, 7),
    ("8-10", "极高", 8, 10),
)
SIGNAL_PRIORITY_GROUPS = ("S", "A", "B", "C", "D")
SPOT_ONCHAIN_GROUPS = ("弱", "中性", "强")
CONTRACT_SPOT_DIVERGENCE_GROUPS = ("无背离", "轻微背离", "明显背离", "严重背离")
MAJOR_FLOW_GROUPS = ("主力偏空", "主力分歧", "主力偏多", "数据不足")
CONVICTION_GROUPS = (
    ("0-49", "低", 0, 49),
    ("50-64", "中低", 50, 64),
    ("65-79", "中高", 65, 79),
    ("80-100", "高", 80, 100),
)
EVIDENCE_DIRECTION_GROUPS = ("看多", "看空/风险", "观察")
EVIDENCE_SCORE_GROUPS = (
    ("<=-5", "风险强", None, -5),
    ("-4..0", "风险/观察", -4, 0),
    ("1..4", "偏多", 1, 4),
    (">=5", "多头强", 5, None),
)
LEADING_SCORE_GROUPS = (
    ("0", "无", 0, 0),
    ("1-2", "弱", 1, 2),
    ("3-5", "中", 3, 5),
    ("6+", "强", 6, None),
)
LEADING_DIRECTION_GROUPS = ("long", "short", "neutral")


def parse_time(value):
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    t = dt.datetime.fromisoformat(value)
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.UTC)
    return t.astimezone(dt.UTC)


def pct(a, b):
    return 0.0 if not a else (b - a) / a * 100


def direction(kind):
    if kind in BULL:
        return "long"
    if kind in BEAR:
        return "short"
    return "neutral"


def get_klines(session, symbol, start, end):
    r = session.get(
        BASE + "/fapi/v1/klines",
        params={
            "symbol": symbol,
            "interval": "5m",
            "startTime": int(start * 1000),
            "endTime": int(end * 1000),
            "limit": 500,
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def close_before(rows, ts):
    target = int(ts * 1000)
    last = None
    for row in rows:
        if int(row[0]) <= target:
            last = row
        else:
            break
    return float(last[4]) if last else None


def eval_signal(session, row):
    symbol = (row.get("symbol") or "").upper()
    kind = row.get("kind") or ""
    t = parse_time(row.get("time") or "")
    try:
        entry = float(row.get("price") or 0)
    except Exception:
        return None
    if not symbol or not kind or not t or entry <= 0:
        return None

    start = t.timestamp()
    now_ts = time.time()
    rows = get_klines(session, symbol, start, min(now_ts, start + HORIZONS["24h"] + 600))
    if not rows:
        return None

    side = direction(kind)
    out = {"symbol": symbol, "kind": kind, "side": side, "time": t, "time_ts": start}
    out["long_flow_alignment_score"] = parse_int(row.get("long_flow_alignment_score"))
    out["main_asset_score"] = parse_int(row.get("main_asset_score"))
    out["trap_risk_score"] = parse_int(row.get("trap_risk_score"))
    out["entry_timing_score"] = parse_int(row.get("entry_timing_score"))
    out["entry_timing_label"] = (row.get("entry_timing_label") or "").strip() or None
    out["spot_onchain_score"] = parse_int(row.get("spot_onchain_score"))
    out["spot_onchain_label"] = (row.get("spot_onchain_label") or "").strip() or None
    out["contract_spot_divergence_score"] = parse_int(row.get("contract_spot_divergence_score"))
    out["contract_spot_divergence_label"] = (row.get("contract_spot_divergence_label") or "").strip() or None
    out["major_flow_score"] = parse_int(row.get("major_flow_score"))
    out["major_flow_label"] = (row.get("major_flow_label") or "").strip() or None
    out["signal_priority"] = (row.get("signal_priority") or "").strip().upper() or None
    out["signal_quality_score"] = parse_int(row.get("signal_quality_score"))
    out["signal_quality_reason"] = row.get("signal_quality_reason") or ""
    out["conviction_score"] = parse_int(row.get("conviction_score"))
    out["conviction_label"] = (row.get("conviction_label") or "").strip() or None
    out["position_behavior_label"] = (row.get("position_behavior_label") or "").strip() or None
    out["squeeze_state_label"] = (row.get("squeeze_state_label") or "").strip() or None
    out["market_intent_label"] = (row.get("market_intent_label") or "").strip() or None
    out["flow_trend_label"] = (row.get("flow_trend_label") or "").strip() or None
    out["basis_state"] = (row.get("basis_state") or "").strip() or None
    out["evidence_score"] = parse_int(row.get("evidence_score"))
    out["evidence_direction"] = (row.get("evidence_direction") or "").strip() or None
    out["evidence_summary"] = (row.get("evidence_summary") or "").strip() or None
    out["evidence_items"] = row.get("evidence_items") or ""
    out["leading_score"] = parse_int(row.get("leading_score"))
    out["leading_direction"] = (row.get("leading_direction") or "").strip() or None
    out["leading_label"] = (row.get("leading_label") or "").strip() or None
    out["funding_rate_percent"] = parse_float(row.get("funding_rate_percent"))
    out["suppressed_from_telegram"] = parse_bool_int(row.get("suppressed_from_telegram"))
    for name in ("12h", "24h", "48h", "72h", "96h", "120h", "144h"):
        out[f"flow_{name}"] = parse_float(row.get(f"net_flow_{name}_usd"))
        out[f"flow_{name}_ratio"] = parse_float(row.get(f"net_flow_{name}_ratio"))
    for name, sec in HORIZONS.items():
        if now_ts < start + sec:
            out[name] = None
            continue
        close = close_before(rows, start + sec)
        if close is None:
            out[name] = None
            continue
        change = pct(entry, close)
        out[name] = -change if side == "short" else change

    high = max(float(x[2]) for x in rows)
    low = min(float(x[3]) for x in rows)
    out["mfe"] = pct(entry, high)
    out["mae"] = pct(entry, low)
    return out


def fmt(v):
    return "-" if v is None else f"{v:+.2f}%"


def fmt_stat_value(v):
    return "-" if v is None else fmt(v)


def parse_float(value):
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def parse_int(value):
    if value in (None, ""):
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def parse_bool_int(value):
    if value in (None, ""):
        return None
    text = str(value).strip().lower()
    if text in ("1", "true", "yes"):
        return 1
    if text in ("0", "false", "no"):
        return 0
    return None


def fmt_usd(v):
    if v is None:
        return "-"
    abs_value = abs(v)
    sign = "+" if v >= 0 else "-"
    if abs_value >= 1_000_000:
        return f"{sign}{abs_value / 1_000_000:.2f}M"
    if abs_value >= 1_000:
        return f"{sign}{abs_value / 1_000:.1f}K"
    return f"{sign}{abs_value:.0f}"


def fmt_ratio(v):
    return "-" if v is None else f"{v:.4g}"


def horizon_values(rows, horizon):
    return [x[horizon] for x in rows if x.get(horizon) is not None]


def stats_for_values(vals):
    if not vals:
        return {
            "n": 0,
            "wins": 0,
            "win_rate": None,
            "avg": None,
            "median": None,
            "best": None,
            "worst": None,
        }
    wins = sum(v > 0 for v in vals)
    return {
        "n": len(vals),
        "wins": wins,
        "win_rate": wins / len(vals) * 100,
        "avg": statistics.mean(vals),
        "median": statistics.median(vals),
        "best": max(vals),
        "worst": min(vals),
    }


def print_horizon_stat_line(prefix, rows, horizon, include_worst=False):
    stat = stats_for_values(horizon_values(rows, horizon))
    if not stat["n"]:
        print(f"{prefix}{horizon}: 样本=0")
        return
    worst_part = f" 最差={fmt_stat_value(stat['worst'])}" if include_worst else ""
    print(
        f"{prefix}{horizon}: 样本={stat['n']} 胜率={stat['wins']}/{stat['n']} {stat['win_rate']:.1f}% "
        f"平均={fmt_stat_value(stat['avg'])}{worst_part}"
    )


def flow_detail(x):
    parts = []
    entry_label = x.get("entry_timing_label")
    entry_score = x.get("entry_timing_score")
    if entry_label and entry_score is not None:
        parts.append(f"entry={entry_label}/{entry_score}")
    long_flow = x.get("long_flow_alignment_score")
    if long_flow is not None:
        parts.append(f"longFlow={long_flow}/9")
    main_score = x.get("main_asset_score")
    if main_score is not None:
        parts.append(f"mainScore={main_score}/100")
    priority = x.get("signal_priority")
    quality = x.get("signal_quality_score")
    suppressed = x.get("suppressed_from_telegram")
    if priority and quality is not None:
        parts.append(f"q={priority}/{quality}")
    if suppressed is not None:
        parts.append(f"suppressed={suppressed}")
    trap_score = x.get("trap_risk_score")
    if trap_score is not None:
        parts.append(f"trap={trap_score}/10")
    spot_label = x.get("spot_onchain_label")
    spot_score = x.get("spot_onchain_score")
    div_label = x.get("contract_spot_divergence_label")
    div_score = x.get("contract_spot_divergence_score")
    major_label = x.get("major_flow_label")
    major_score = x.get("major_flow_score")
    if spot_label and spot_score is not None:
        parts.append(f"spot={spot_label}/{spot_score}")
    if div_label and div_score is not None:
        parts.append(f"div={div_label}/{div_score}")
    if major_label and major_score is not None:
        parts.append(f"major={major_label}/{major_score}")
    evidence_direction = x.get("evidence_direction")
    evidence_score = x.get("evidence_score")
    evidence_summary = x.get("evidence_summary")
    if evidence_direction and evidence_score is not None:
        parts.append(f"ev={evidence_direction}/{abs(evidence_score)} {evidence_summary or '-'}")
    for name in ("12h", "24h", "72h", "144h"):
        flow = x.get(f"flow_{name}")
        ratio = x.get(f"flow_{name}_ratio")
        if flow is not None or ratio is not None:
            parts.append(f"flow{name}={fmt_usd(flow)}/r={fmt_ratio(ratio)}")
    return " " + " ".join(parts) if parts else ""


def long_flow_group(score):
    if score is None:
        return None
    for label, name, low, high in LONG_FLOW_GROUPS:
        if low <= score <= high:
            return label, name
    return None


def main_asset_score_group(score):
    if score is None:
        return None
    for label, name, low, high in MAIN_ASSET_SCORE_GROUPS:
        if low <= score <= high:
            return label, name
    return None


def trap_risk_group(score):
    if score is None:
        return None
    for label, name, low, high in TRAP_RISK_GROUPS:
        if low <= score <= high:
            return label, name
    return None


def conviction_group(score):
    if score is None:
        return None
    for label, name, low, high in CONVICTION_GROUPS:
        if low <= score <= high:
            return label, name
    return None


def evidence_score_group(score):
    if score is None:
        return None
    for label, name, low, high in EVIDENCE_SCORE_GROUPS:
        if (low is None or score >= low) and (high is None or score <= high):
            return label, name
    return None


def leading_score_group(score):
    if score is None:
        return None
    for label, name, low, high in LEADING_SCORE_GROUPS:
        if score >= low and (high is None or score <= high):
            return label, name
    return None


def print_leading_backtest(results):
    print("[LEADING SIGNAL] 领先信号分组回测")
    grouped = [(x, leading_score_group(x.get("leading_score"))) for x in results]
    if not any(group for _x, group in grouped):
        print("暂无 leading_score 样本")
        print("")
    else:
        for label, name, _low, _high in LEADING_SCORE_GROUPS:
            group_rows = [x for x, group in grouped if group == (label, name)]
            print(f"{label} {name}: 样本={len(group_rows)}")
            for h in REPORT_HORIZONS:
                vals = [x[h] for x in group_rows if x[h] is not None]
                if not vals:
                    print(f"  {h}: 样本=0")
                    continue
                wins = sum(v > 0 for v in vals)
                print(
                    f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                    f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                    f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
                )
        print("")

    print("[LEADING DIRECTION] 领先方向分组回测")
    for direction in LEADING_DIRECTION_GROUPS:
        group_rows = [x for x in results if (x.get("leading_direction") or "neutral") == direction]
        print(f"{direction}: 样本={len(group_rows)}")
        for h in REPORT_HORIZONS:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_score_group_backtest(results, title, score_field, groups):
    grouped = [(x, conviction_group(x.get(score_field))) for x in results]
    if not any(group for _x, group in grouped):
        print(title)
        print(f"暂无 {score_field} 样本")
        print("")
        return

    print(title)
    for label, name, _low, _high in groups:
        group_rows = [x for x, group in grouped if group == (label, name)]
        print(f"{label} {name}: 样本={len(group_rows)}")
        for h in REPORT_HORIZONS:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_long_flow_backtest(results):
    grouped = [
        (x, long_flow_group(x.get("long_flow_alignment_score")))
        for x in results
    ]
    if not any(group for _x, group in grouped):
        return

    print("[LONG FLOW] 长周期资金共振分组回测")
    for kind in sorted(set(x["kind"] for x, group in grouped if group)):
        kind_rows = [(x, group) for x, group in grouped if x["kind"] == kind and group]

        print(f"{kind}:")
        for label, name, _low, _high in LONG_FLOW_GROUPS:
            group_rows = [
                x
                for x, group in kind_rows
                if group == (label, name)
            ]

            parts = [f"  {label} {name}: 样本={len(group_rows)}"]
            for h in ["15m", "1h", "4h", "24h"]:
                vals = [x[h] for x in group_rows if x[h] is not None]
                if not vals:
                    continue
                wins = sum(v > 0 for v in vals)
                parts.append(
                    f"{h}胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                    f"平均={fmt(statistics.mean(vals))} "
                    f"中位={fmt(statistics.median(vals))}"
                )
            print(" ".join(parts))
        print("")


def print_signal_quality_backtest(results):
    if not any(x.get("signal_priority") for x in results):
        return

    print("[SIGNAL QUALITY] 信号质量分组回测")
    for priority in SIGNAL_PRIORITY_GROUPS:
        group_rows = [x for x in results if x.get("signal_priority") == priority]
        print(f"{priority}: 样本={len(group_rows)}")
        for h in ["15m", "1h", "4h", "12h", "24h"]:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_main_asset_score_backtest(results):
    grouped = [
        (x, main_asset_score_group(x.get("main_asset_score")))
        for x in results
    ]
    if not any(group for _x, group in grouped):
        return

    print("[MAIN ASSET SCORE] 主流评分分组回测")
    for label, name, _low, _high in MAIN_ASSET_SCORE_GROUPS:
        group_rows = [
            x
            for x, group in grouped
            if group == (label, name)
        ]

        print(f"{label} {name}: 样本={len(group_rows)}")
        for h in ["15m", "1h", "4h", "12h", "24h"]:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_trap_risk_backtest(results):
    grouped = [
        (x, trap_risk_group(x.get("trap_risk_score")))
        for x in results
    ]

    print("[TRAP RISK] 诱多/诱空过滤评分分组回测")
    for label, name, _low, _high in TRAP_RISK_GROUPS:
        group_rows = [
            x
            for x, group in grouped
            if group == (label, name)
        ]

        print(f"{label} {name}: 样本={len(group_rows)}")
        for h in ["15m", "1h", "4h", "12h", "24h"]:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_entry_timing_backtest(results):
    labels = sorted({x.get("entry_timing_label") for x in results if x.get("entry_timing_label")})
    print("[ENTRY TIMING] 入场时机/阶段分组回测")
    if not labels:
        print("暂无 entry_timing_label 样本")
        print("")
        return

    for label in labels:
        group_rows = [x for x in results if x.get("entry_timing_label") == label]
        print(f"{label}: 样本={len(group_rows)}")
        for h in ["15m", "1h", "4h", "12h", "24h"]:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_label_group_backtest(results, title, field, labels):
    print(title)
    if labels is None:
        labels = sorted({x.get(field) for x in results if x.get(field)})
        if not labels:
            print(f"暂无 {field} 样本")
            print("")
            return
    for label in labels:
        group_rows = [x for x in results if x.get(field) == label]
        print(f"{label}: 样本={len(group_rows)}")
        for h in ["15m", "1h", "4h", "12h", "24h"]:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_spot_onchain_backtest(results):
    print_label_group_backtest(
        results,
        "[SPOT ONCHAIN] 现货/链上确认分组回测",
        "spot_onchain_label",
        SPOT_ONCHAIN_GROUPS,
    )


def print_contract_spot_divergence_backtest(results):
    print_label_group_backtest(
        results,
        "[CONTRACT SPOT DIVERGENCE] 合约现货背离分组回测",
        "contract_spot_divergence_label",
        CONTRACT_SPOT_DIVERGENCE_GROUPS,
    )


def print_major_flow_backtest(results):
    print_label_group_backtest(
        results,
        "[MAJOR FLOW] 主力长周期趋势分组回测",
        "major_flow_label",
        MAJOR_FLOW_GROUPS,
    )


def print_conviction_backtest(results):
    print_score_group_backtest(results, "[CONVICTION] 把握性评分分组回测", "conviction_score", CONVICTION_GROUPS)


def print_position_behavior_backtest(results):
    print_label_group_backtest(results, "[POSITION BEHAVIOR] 主力行为分组回测", "position_behavior_label", None)


def print_squeeze_state_backtest(results):
    print_label_group_backtest(results, "[SQUEEZE STATE] 挤压结构分组回测", "squeeze_state_label", None)


def print_market_intent_backtest(results):
    print_label_group_backtest(results, "[MARKET INTENT] 市场意图分组回测", "market_intent_label", None)


def print_flow_trend_backtest(results):
    print_label_group_backtest(results, "[FLOW TREND] 资金周期标签分组回测", "flow_trend_label", None)


def print_basis_state_backtest(results):
    print_label_group_backtest(results, "[BASIS STATE] 基差状态分组回测", "basis_state", None)


def print_evidence_direction_backtest(results):
    print_label_group_backtest(
        results,
        "[Evidence Direction] 证据方向分组回测",
        "evidence_direction",
        EVIDENCE_DIRECTION_GROUPS,
    )


def print_evidence_score_backtest(results):
    grouped = [
        (x, evidence_score_group(x.get("evidence_score")))
        for x in results
    ]
    if not any(group for _x, group in grouped):
        print("[Evidence Score] 证据分数分组回测")
        print("暂无 evidence_score 样本")
        print("")
        return

    print("[Evidence Score] 证据分数分组回测")
    for label, name, _low, _high in EVIDENCE_SCORE_GROUPS:
        group_rows = [
            x
            for x, group in grouped
            if group == (label, name)
        ]
        print(f"{label} {name}: 样本={len(group_rows)}")
        for h in REPORT_HORIZONS:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
    print("")


def print_evidence_summary_backtest(results):
    summaries = {}
    for row in results:
        summary = row.get("evidence_summary")
        if not summary:
            continue
        summaries[summary] = summaries.get(summary, 0) + 1

    print("[Evidence Summary] 证据总结TOP10分组回测")
    if not summaries:
        print("暂无 evidence_summary 样本")
        print("")
        return
    top_summaries = [
        summary
        for summary, _count in sorted(summaries.items(), key=lambda item: (-item[1], item[0]))[:10]
    ]
    for summary in top_summaries:
        group_rows = [x for x in results if x.get("evidence_summary") == summary]
        print(f"{summary}: 样本={len(group_rows)}")
        for h in REPORT_HORIZONS:
            vals = [x[h] for x in group_rows if x[h] is not None]
            if not vals:
                print(f"  {h}: 样本=0")
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 样本={len(vals)} 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))}"
            )
    print("")


def kind_average(rows, horizon):
    vals = horizon_values(rows, horizon)
    return statistics.mean(vals) if vals else None


def print_kind_ranking(results, title, reverse):
    ranked = []
    for kind in sorted(set(x["kind"] for x in results)):
        rows = [x for x in results if x["kind"] == kind]
        avg_1h = kind_average(rows, "1h")
        avg_4h = kind_average(rows, "4h")
        rank_value = avg_1h if avg_1h is not None else avg_4h
        if rank_value is None:
            continue
        ranked.append((rank_value, kind, rows, avg_1h, avg_4h))

    print(title)
    if not ranked:
        print("  暂无可排序样本")
        return
    ranked.sort(key=lambda item: item[0], reverse=reverse)
    for rank_value, kind, rows, avg_1h, avg_4h in ranked[:5]:
        vals_1h = horizon_values(rows, "1h")
        wins_1h = sum(v > 0 for v in vals_1h)
        win_text = f"{wins_1h}/{len(vals_1h)} {wins_1h/len(vals_1h)*100:.1f}%" if vals_1h else "-"
        print(
            f"  {kind}: 样本={len(rows)} 1h胜率={win_text} "
            f"1h平均={fmt_stat_value(avg_1h)} 4h平均={fmt_stat_value(avg_4h)} 排序值={fmt_stat_value(rank_value)}"
        )


def print_summary_report(total_signals, results):
    print("[SUMMARY] 综合总览")
    print(f"总信号数: {total_signals}")
    print(f"可评估信号数: {len(results)}")
    conviction_values = [x["conviction_score"] for x in results if x.get("conviction_score") is not None]
    if conviction_values:
        high_count = sum(value >= 65 for value in conviction_values)
        print(
            f"把握性: 平均={statistics.mean(conviction_values):.1f} "
            f"中高及以上={high_count}/{len(conviction_values)}"
        )
    for horizon in REPORT_HORIZONS:
        stat = stats_for_values(horizon_values(results, horizon))
        if not stat["n"]:
            print(f"{horizon}: 样本=0")
            continue
        print(
            f"{horizon}: 胜率={stat['wins']}/{stat['n']} {stat['win_rate']:.1f}% "
            f"平均={fmt_stat_value(stat['avg'])}"
        )
    print_kind_ranking(results, "表现最好 kind TOP5:", reverse=True)
    print_kind_ranking(results, "表现最差 kind TOP5:", reverse=False)
    print("")


def is_bad_signal(x):
    loss_1h = x.get("1h") is not None and x["1h"] <= -3
    loss_4h = x.get("4h") is not None and x["4h"] <= -3
    adverse_mae = x.get("mae") is not None and x["mae"] <= -5
    return loss_1h or loss_4h or adverse_mae


def bad_signal_causes(x):
    causes = []
    trap_score = x.get("trap_risk_score")
    if trap_score is not None and trap_score >= 6:
        causes.append("trap 高")
    long_flow = x.get("long_flow_alignment_score")
    if long_flow is not None and long_flow <= 3:
        causes.append("longFlow 弱")
    if x.get("signal_priority") in ("C", "D"):
        causes.append("quality C/D")
    conviction = x.get("conviction_score")
    if conviction is not None and conviction < 65:
        causes.append("conviction低/中低")
    if x.get("entry_timing_label") in ("追高风险", "下跌中继", "不宜追"):
        causes.append("entry label 风险")
    quality_reason = str(x.get("signal_quality_reason") or "").lower()
    if "spot/onchain weak" in quality_reason or "现货弱" in quality_reason or "现货/链上确认偏弱" in quality_reason:
        causes.append("现货弱")
    funding = x.get("funding_rate_percent")
    if funding is not None and abs(funding) >= 0.08:
        causes.append("Funding 极端")
    return causes


def print_bad_signals_report(results):
    bad_rows = [x for x in results if is_bad_signal(x)]
    cause_counts = {
        "trap 高": 0,
        "longFlow 弱": 0,
        "quality C/D": 0,
        "conviction低/中低": 0,
        "entry label 风险": 0,
        "现货弱": 0,
        "Funding 极端": 0,
    }
    multi_cause = 0
    for row in bad_rows:
        causes = bad_signal_causes(row)
        if len(causes) >= 2:
            multi_cause += 1
        for cause in causes:
            cause_counts[cause] += 1

    print("[BAD SIGNALS] 坏信号归因")
    print(f"坏信号定义: 1h或4h收益<=-3%，或MAE<=-5%")
    print(f"坏信号样本: {len(bad_rows)}/{len(results)}")
    if not bad_rows:
        print("")
        return
    print(f"多重原因样本: {multi_cause}/{len(bad_rows)}")
    for cause, count in sorted(cause_counts.items(), key=lambda item: (-item[1], item[0])):
        rate = count / len(bad_rows) * 100 if bad_rows else 0
        print(f"{cause}: {count} ({rate:.1f}%)")
    print("")


def combo_filter_keeps(x):
    return (
        x.get("signal_priority") in ("S", "A", "B")
        and x.get("trap_risk_score") is not None
        and x["trap_risk_score"] <= 5
        and x.get("entry_timing_score") is not None
        and x["entry_timing_score"] >= 5
        and x.get("long_flow_alignment_score") is not None
        and x["long_flow_alignment_score"] >= 3
    )


def print_combo_filter_report(results):
    kept = [x for x in results if combo_filter_keeps(x)]
    conviction_kept = [x for x in results if x.get("conviction_score") is not None and x["conviction_score"] >= 65]
    print("[COMBO FILTER] 组合过滤模拟")
    print("规则: quality in S/A/B; trap<=5; entry>=5; longFlow>=3")
    for label, rows in (("过滤前", results), ("过滤后", kept)):
        print(f"{label}: 样本={len(rows)}")
        for horizon in REPORT_HORIZONS:
            print_horizon_stat_line("  ", rows, horizon, include_worst=True)
    print("规则: conviction>=65")
    print(f"conviction>=65: 样本={len(conviction_kept)}")
    for horizon in REPORT_HORIZONS:
        print_horizon_stat_line("  ", conviction_kept, horizon, include_worst=True)
    print("")


def duplicate_groups(results):
    ordered = sorted([x for x in results if x.get("time_ts") is not None], key=lambda item: item["time_ts"])
    last_by_key = {}
    first_rows = []
    duplicate_rows = []
    for row in ordered:
        key = (row["symbol"], row["kind"])
        previous = last_by_key.get(key)
        if previous is not None and row["time_ts"] - previous["time_ts"] <= 1800:
            duplicate_rows.append(row)
        else:
            first_rows.append(row)
        last_by_key[key] = row
    return first_rows, duplicate_rows


def print_duplicate_performance(label, rows):
    print(f"{label}: 样本={len(rows)}")
    for horizon in ("1h", "4h"):
        print_horizon_stat_line("  ", rows, horizon, include_worst=True)


def print_duplicates_report(results):
    first_rows, duplicate_rows = duplicate_groups(results)
    print("[DUPLICATES] 重复信号分析")
    print("定义: 同 symbol+kind 30分钟内重复信号")
    print_duplicate_performance("首次", first_rows)
    print_duplicate_performance("重复", duplicate_rows)
    print("")


def run_backtest(args):
    cfg = yaml.safe_load(Path(args.config).read_text()) or {}
    path = Path(str(cfg.get("signal_log_path", "signals.csv")))
    if not path.is_absolute():
        path = Path.cwd() / path
    rows = list(csv.DictReader(path.open("r", encoding="utf-8")))[-args.limit:][::-1]

    session = requests.Session()
    results = []
    for row in rows:
        try:
            item = eval_signal(session, row)
            if item:
                results.append(item)
            time.sleep(0.08)
        except Exception as e:
            print(f"skip {row.get('symbol','-')}: {type(e).__name__}: {e}")

    if not results:
        raise SystemExit("no backtestable signals")

    print("[BACKTEST] 最近信号回测")
    print("说明: discovery/hot/main_trend_watch/main_momentum_watch 按看多计算，top_risk/distribution/main_risk_watch 按看空/风险计算")
    print("")

    for kind in sorted(set(x["kind"] for x in results)):
        group = [x for x in results if x["kind"] == kind]
        print(f"{kind} 样本={len(group)}")
        for h in ["15m", "1h", "4h", "12h", "24h"]:
            vals = [x[h] for x in group if x[h] is not None]
            if not vals:
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
        print("")

    print_signal_quality_backtest(results)
    print_long_flow_backtest(results)
    print_main_asset_score_backtest(results)
    print_trap_risk_backtest(results)
    print_entry_timing_backtest(results)
    print_spot_onchain_backtest(results)
    print_contract_spot_divergence_backtest(results)
    print_major_flow_backtest(results)
    print_conviction_backtest(results)
    print_leading_backtest(results)
    print_position_behavior_backtest(results)
    print_squeeze_state_backtest(results)
    print_market_intent_backtest(results)
    print_flow_trend_backtest(results)
    print_basis_state_backtest(results)
    print_evidence_direction_backtest(results)
    print_evidence_score_backtest(results)
    print_evidence_summary_backtest(results)

    print("最近20条:")
    for x in results[:20]:
        print(
            f"{x['symbol']} {x['kind']} {x['side']} "
            f"15m={fmt(x['15m'])} 1h={fmt(x['1h'])} 4h={fmt(x['4h'])} "
            f"12h={fmt(x['12h'])} 24h={fmt(x['24h'])}"
            f" conv={x.get('conviction_label') or '-'}/{x.get('conviction_score') if x.get('conviction_score') is not None else '-'}"
            f" intent={x.get('market_intent_label') or '-'}"
            f" pos={x.get('position_behavior_label') or '-'}"
            f" squeeze={x.get('squeeze_state_label') or '-'}"
            f" basis={x.get('basis_state') or '-'}"
            f" flow={x.get('flow_trend_label') or '-'}"
            f" lead={x.get('leading_score') if x.get('leading_score') is not None else '-'}/{x.get('leading_direction') or '-'}/{x.get('leading_label') or '-'}"
            f"{flow_detail(x)} MFE={fmt(x['mfe'])} MAE={fmt(x['mae'])}"
        )

    print("")
    print_summary_report(len(rows), results)
    print_bad_signals_report(results)
    print_combo_filter_report(results)
    print_duplicates_report(results)


class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, data):
        for file in self.files:
            file.write(data)
        return len(data)

    def flush(self):
        for file in self.files:
            file.flush()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="derivatives_config.yaml")
    ap.add_argument("--limit", type=int, default=80)
    ap.add_argument("--export-report")
    args = ap.parse_args()

    if not args.export_report:
        run_backtest(args)
        return

    buffer = io.StringIO()
    tee = Tee(sys.stdout, buffer)
    with contextlib.redirect_stdout(tee):
        run_backtest(args)

    export_path = Path(args.export_report)
    export_path.parent.mkdir(parents=True, exist_ok=True)
    export_path.write_text(buffer.getvalue(), encoding="utf-8")


if __name__ == "__main__":
    main()
