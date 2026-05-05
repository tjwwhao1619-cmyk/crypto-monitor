import argparse
import contextlib
import csv
import datetime as dt
import json
import io
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

import requests
import yaml

import derivatives_monitor as live_routes

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


class DataMissingError(Exception):
    def __init__(self, symbol, kind, reason):
        super().__init__(reason)
        self.symbol = symbol
        self.kind = kind
        self.reason = reason


class BacktestKlineContext:
    def __init__(self, cache_dir=".cache/backtest_klines", no_network=False, sleep_seconds=0.25, max_retries=5):
        self.cache_dir = Path(cache_dir)
        self.no_network = bool(no_network)
        self.sleep_seconds = max(0.0, float(sleep_seconds or 0))
        self.max_retries = max(1, int(max_retries or 1))
        self.last_request_at = 0.0
        self.cache_hit = 0
        self.cache_miss = 0
        self.stale_cache_used = 0
        self.request_429 = 0
        self.data_missing = []
        self.total_rows = 0
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def cache_path(self, symbol, interval, start_ms, end_ms):
        safe_symbol = "".join(ch for ch in str(symbol).upper() if ch.isalnum() or ch in "_-")
        return self.cache_dir / f"{safe_symbol}_{interval}_{start_ms}_{end_ms}.json"

    def read_cache_file(self, path):
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            rows = payload.get("rows") if isinstance(payload, dict) else payload
            return rows if isinstance(rows, list) else None
        except Exception:
            return None

    def read_exact_cache(self, symbol, interval, start_ms, end_ms):
        rows = self.read_cache_file(self.cache_path(symbol, interval, start_ms, end_ms))
        if rows is not None:
            self.cache_hit += 1
            return rows
        self.cache_miss += 1
        return None

    def stale_cache(self, symbol, interval, start_ms=None):
        safe_symbol = "".join(ch for ch in str(symbol).upper() if ch.isalnum() or ch in "_-")
        if start_ms is not None:
            pattern = f"{safe_symbol}_{interval}_{start_ms}_*.json"
        else:
            pattern = f"{safe_symbol}_{interval}_*.json"
        candidates = sorted(self.cache_dir.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in candidates:
            rows = self.read_cache_file(path)
            if rows:
                self.stale_cache_used += 1
                return rows
        return None

    def write_cache(self, symbol, interval, start_ms, end_ms, rows):
        path = self.cache_path(symbol, interval, start_ms, end_ms)
        payload = {
            "symbol": symbol,
            "interval": interval,
            "start_ms": start_ms,
            "end_ms": end_ms,
            "cached_at": dt.datetime.now(dt.UTC).isoformat(),
            "rows": rows,
        }
        path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

    def throttle(self):
        if self.sleep_seconds <= 0:
            return
        elapsed = time.time() - self.last_request_at
        if elapsed < self.sleep_seconds:
            time.sleep(self.sleep_seconds - elapsed)
        self.last_request_at = time.time()

    def note_missing(self, row, reason):
        item = {
            "symbol": (row.get("symbol") or "").upper(),
            "kind": row.get("kind") or "",
            "reason": reason,
        }
        self.data_missing.append(item)


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


def get_klines(session, symbol, start, end, ctx: BacktestKlineContext | None = None, interval="5m"):
    ctx = ctx or BacktestKlineContext()
    start_ms = int(start * 1000)
    end_ms = int(end * 1000)
    cached = ctx.read_exact_cache(symbol, interval, start_ms, end_ms)
    if cached is not None:
        return cached
    if ctx.no_network:
        stale = ctx.stale_cache(symbol, interval, start_ms)
        if stale is not None:
            return stale
        raise DataMissingError(symbol, "", "no cache and --no-network")

    params = {
        "symbol": symbol,
        "interval": interval,
        "startTime": start_ms,
        "endTime": end_ms,
        "limit": 500,
    }
    delays = [1, 2, 5, 10]
    last_error = "request failed"
    for attempt in range(ctx.max_retries):
        ctx.throttle()
        try:
            response = session.get(BASE + "/fapi/v1/klines", params=params, timeout=10)
            if response.status_code == 429:
                ctx.request_429 += 1
                retry_after = response.headers.get("Retry-After")
                delay = float(retry_after) if retry_after else delays[min(attempt, len(delays) - 1)]
                last_error = f"429 Too Many Requests"
                time.sleep(max(0.0, delay))
                continue
            response.raise_for_status()
            rows = response.json()
            ctx.write_cache(symbol, interval, start_ms, end_ms, rows)
            return rows
        except requests.HTTPError as exc:
            last_error = f"HTTPError: {exc}"
            if attempt + 1 < ctx.max_retries:
                time.sleep(delays[min(attempt, len(delays) - 1)])
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt + 1 < ctx.max_retries:
                time.sleep(delays[min(attempt, len(delays) - 1)])

    stale = ctx.stale_cache(symbol, interval, start_ms)
    if stale is not None:
        return stale
    raise DataMissingError(symbol, "", last_error)


def close_before(rows, ts):
    target = int(ts * 1000)
    last = None
    for row in rows:
        if int(row[0]) <= target:
            last = row
        else:
            break
    return float(last[4]) if last else None


def eval_signal(session, row, ctx: BacktestKlineContext | None = None):
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
    rows = get_klines(session, symbol, start, min(now_ts, start + HORIZONS["24h"] + 600), ctx=ctx)
    if not rows:
        return None

    side = direction(kind)
    out = {"symbol": symbol, "kind": kind, "side": side, "time": t, "time_ts": start}
    out["price_change_percent"] = parse_float(row.get("price_change_percent"))
    out["oi_change_percent"] = parse_float(row.get("oi_change_percent"))
    out["confirm_price_change_percent"] = parse_float(row.get("confirm_price_change_percent"))
    out["confirm_oi_change_percent"] = parse_float(row.get("confirm_oi_change_percent"))
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
        horizon_rows = [item for item in rows if int(item[0]) <= int((start + sec) * 1000)]
        if horizon_rows:
            high = max(float(item[2]) for item in horizon_rows)
            low = min(float(item[3]) for item in horizon_rows)
            out[f"mfe_{name}"] = pct(entry, high) if side != "short" else pct(low, entry)
            out[f"mae_{name}"] = pct(low, entry) if side != "short" else pct(entry, high)
        else:
            out[f"mfe_{name}"] = None
            out[f"mae_{name}"] = None
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
    add_kline_feature_proxy(out)
    return out


def kline_proxy_score(value):
    if value is None:
        return 5
    if value >= 6:
        return 10
    if value >= 3:
        return 8
    if value >= 1:
        return 6
    if value > -1:
        return 5
    if value > -3:
        return 3
    return 1


def add_kline_feature_proxy(out):
    short_score = kline_proxy_score(out.get("15m"))
    mid_score = kline_proxy_score(out.get("1h"))
    long_score = kline_proxy_score(out.get("4h"))
    out["kline_short_score"] = short_score
    out["kline_mid_score"] = mid_score
    out["kline_long_score"] = long_score
    if short_score >= 8 and mid_score >= 8 and long_score >= 6:
        label = "短中线突破延续"
    elif short_score >= 8 and mid_score < 6:
        label = "短线突破未延续"
    elif short_score >= 6 and mid_score >= 6 and long_score < 5:
        label = "短中线转强大周期未确认"
    elif short_score <= 3 and mid_score <= 3:
        label = "短线承压"
    elif long_score >= 8:
        label = "4h延续强"
    else:
        label = "结构中性"
    out["kline_label"] = label


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


def route_simulation_decision(row, disable_breakout_watch=False):
    # Backtest rows do not persist live multi_timeframe_price_action. Reuse the
    # live Discord route rules with price_action=None so non-Kline gates stay
    # consistent. Risk realtime uses evidence_direction/evidence_score as a
    # backtest-only proxy when live Kline bearish confirmation is unavailable.
    return live_routes.discord_route_decision(
        row,
        context={
            "load_price_action": False,
            "risk_realtime_proxy": True,
            "disable_breakout_watch": disable_breakout_watch,
        },
    )


def route_eval_score(x):
    vals = [x.get("1h"), x.get("4h")]
    vals = [v for v in vals if v is not None]
    return max(vals) if vals else None


def route_bad_score(x):
    adverse = x.get("mae")
    if x.get("side") == "short" and x.get("mfe") is not None:
        adverse = -x.get("mfe")
    vals = [x.get("1h"), x.get("4h"), adverse]
    vals = [v for v in vals if v is not None]
    return min(vals) if vals else None


def route_detail_line(x, decision, score):
    tags = ",".join(decision.route_tags) if decision.route_tags else "-"
    return (
        f"{x['symbol']} {x['kind']} {x['side']} route={decision.route} score={fmt(score)} "
        f"1h={fmt(x.get('1h'))} 4h={fmt(x.get('4h'))} 12h={fmt(x.get('12h'))} "
        f"MFE={fmt(x.get('mfe'))} MAE={fmt(x.get('mae'))} adverse={fmt(route_bad_score(x))} "
        f"conv={x.get('conviction_score') if x.get('conviction_score') is not None else '-'} "
        f"q={x.get('signal_priority') or '-'}/{x.get('signal_quality_score') if x.get('signal_quality_score') is not None else '-'} "
        f"lead={x.get('leading_score') if x.get('leading_score') is not None else '-'} "
        f"ev={x.get('evidence_direction') or '-'}/{x.get('evidence_score') if x.get('evidence_score') is not None else '-'} "
        f"flow={x.get('flow_trend_label') or '-'} kline={x.get('kline_label') or '-'} tags={tags} reason={decision.reason}"
    )


def print_breakout_entry_confirmation_report(routed):
    rows_by_state = {}
    for x, decision, _base_decision in routed:
        if "breakout_watch" not in decision.route_tags and "breakout_watch_fallback" not in decision.route_tags:
            continue
        entry = live_routes.breakout_entry_confirmation(x, None, x, decision)
        rows_by_state.setdefault(entry.state, []).append(x)

    print("[BREAKOUT ENTRY CONFIRMATION]")
    state_order = (
        "waiting_pullback",
        "waiting_pullback_high_risk",
        "waiting_pullback_track",
        "pullback_holding",
        "volume_continuation",
        "breakout_confirmed",
        "failed_invalidated",
        "unknown",
    )
    labels = getattr(live_routes, "BREAKOUT_ENTRY_STATE_LABELS", {})
    for state in state_order:
        rows = rows_by_state.get(state, [])
        adverse_values = [route_bad_score(row) for row in rows]
        adverse_values = [value for value in adverse_values if value is not None]
        adverse_avg = statistics.mean(adverse_values) if adverse_values else None
        stat_1h = stats_for_values(horizon_values(rows, "1h"))
        stat_4h = stats_for_values(horizon_values(rows, "4h"))
        if stat_1h["n"]:
            one_hour = f"{stat_1h['wins']}/{stat_1h['n']} {stat_1h['win_rate']:.1f}% avg={fmt_stat_value(stat_1h['avg'])}"
        else:
            one_hour = "样本=0"
        if stat_4h["n"]:
            four_hour = f"{stat_4h['wins']}/{stat_4h['n']} {stat_4h['win_rate']:.1f}% avg={fmt_stat_value(stat_4h['avg'])}"
        else:
            four_hour = "样本=0"
        print(
            f"{state} {labels.get(state, state)}: count={len(rows)} "
            f"1h={one_hour} 4h={four_hour} adverse_avg={fmt_stat_value(adverse_avg)}"
        )
    print("")


def print_route_simulation_report(results):
    routed = [(x, route_simulation_decision(x), route_simulation_decision(x, disable_breakout_watch=True)) for x in results]
    print("[ROUTE SIMULATION] Discord 路由模拟")
    print("说明: 路由统计只基于可评估样本；如 [DATA COVERAGE] data_missing>0，漏网/噪声榜会受缺失样本影响。")
    for route in ("realtime", "priority_observe", "risk_realtime", "observe", "digest", "suppress"):
        rows = [x for x, decision, _base_decision in routed if decision.route == route]
        print(f"{route}: 样本={len(rows)}")
        for horizon in ("1h", "4h"):
            print_horizon_stat_line("  ", rows, horizon, include_worst=True)
    breakout_rows = [x for x, decision, _base_decision in routed if "breakout_watch" in decision.route_tags]
    breakout_migrated = [
        x for x, decision, base_decision in routed
        if "breakout_watch" in decision.route_tags and base_decision.route == "digest"
    ]
    fallback_rows = [x for x, decision, _base_decision in routed if "breakout_watch_fallback" in decision.route_tags]
    fallback_migrated = [
        x for x, decision, base_decision in routed
        if "breakout_watch_fallback" in decision.route_tags and base_decision.route in {"digest", "observe"}
    ]
    print(f"breakout_watch: 样本={len(breakout_rows)}")
    for horizon in ("1h", "4h"):
        print_horizon_stat_line("  ", breakout_rows, horizon, include_worst=True)
    print(f"breakout_watch 从 digest 迁移: 样本={len(breakout_migrated)}")
    print(f"breakout_watch_fallback: 样本={len(fallback_rows)}")
    for horizon in ("1h", "4h"):
        print_horizon_stat_line("  ", fallback_rows, horizon, include_worst=True)
    print(f"breakout_watch_fallback 从 digest/observe 迁移: 样本={len(fallback_migrated)}")
    breakout_high_risk_demoted_count = 0
    breakout_track_count = 0
    for x, decision, _base_decision in routed:
        if "breakout_watch" not in decision.route_tags and "breakout_watch_fallback" not in decision.route_tags:
            continue
        entry = live_routes.breakout_entry_confirmation(x, None, x, decision)
        if entry.state == "waiting_pullback_high_risk":
            breakout_high_risk_demoted_count += 1
        elif entry.state == "waiting_pullback_track":
            breakout_track_count += 1
    print(f"breakout_high_risk_demoted_count={breakout_high_risk_demoted_count}")
    print(f"breakout_track_count={breakout_track_count}")
    realtime_priority_rows = [
        x for x, decision, _base_decision in routed if decision.route in {"realtime", "risk_realtime", "priority_observe"}
    ]
    print(f"realtime+priority_observe: 样本={len(realtime_priority_rows)}")
    for horizon in ("1h", "4h"):
        print_horizon_stat_line("  ", realtime_priority_rows, horizon, include_worst=True)
    print("")
    print_breakout_entry_confirmation_report(routed)

    realtime_routes = {"realtime", "risk_realtime"}
    visible_routes = {"realtime", "risk_realtime", "priority_observe"}
    missed = []
    missed_before = []
    bad_realtime = []
    bad_priority = []
    bad_breakout = []
    bad_breakout_fallback = []
    conflict_downgraded_realtime_count = 0
    leading_risk_downgrade_count = 0
    weak_external_confirmation_downgrade_count = 0
    focus_symbols = {"LABUSDT", "TAGUSDT", "TSTUSDT", "BSBUSDT"}
    focus_after = Counter()
    focus_before = Counter()
    for x, decision, base_decision in routed:
        best = route_eval_score(x)
        if decision.route not in visible_routes and best is not None and best >= 5:
            missed.append((best, x, decision))
            if x.get("symbol") in focus_symbols:
                focus_after[x.get("symbol")] += 1
        if base_decision.route not in visible_routes and best is not None and best >= 5:
            missed_before.append((best, x, base_decision))
            if x.get("symbol") in focus_symbols:
                focus_before[x.get("symbol")] += 1
        worst = route_bad_score(x)
        if decision.route in realtime_routes and worst is not None and worst <= -3:
            bad_realtime.append((worst, x, decision))
        if decision.route == "priority_observe" and worst is not None and worst <= -3:
            bad_priority.append((worst, x, decision))
        if "breakout_watch" in decision.route_tags and worst is not None and worst <= -3:
            bad_breakout.append((worst, x, decision))
        if "breakout_watch_fallback" in decision.route_tags and worst is not None and worst <= -3:
            bad_breakout_fallback.append((worst, x, decision))
        if "conflict_downgrade" in decision.route_tags:
            conflict_downgraded_realtime_count += 1
        if "leading_risk_downgrade" in decision.route_tags:
            leading_risk_downgrade_count += 1
        if "weak_external_confirmation_downgrade" in decision.route_tags:
            weak_external_confirmation_downgrade_count += 1
    missed.sort(key=lambda item: item[0], reverse=True)
    missed_before.sort(key=lambda item: item[0], reverse=True)
    bad_realtime.sort(key=lambda item: item[0])
    bad_priority.sort(key=lambda item: item[0])
    bad_breakout.sort(key=lambda item: item[0])
    bad_breakout_fallback.sort(key=lambda item: item[0])
    before_focus_total = sum(focus_before.values())
    after_focus_total = sum(focus_after.values())
    print(
        "LAB/TAG/TST/BSB 漏网对比: "
        f"before={before_focus_total} after={after_focus_total} reduced={before_focus_total - after_focus_total}"
    )
    for symbol in sorted(focus_symbols):
        print(f"  {symbol}: before={focus_before.get(symbol, 0)} after={focus_after.get(symbol, 0)}")
    print("")

    print("[MISSED WINNERS] Discord 路由漏网 TOP30")
    for index, (score, x, decision) in enumerate(missed[:30], start=1):
        print(f"{index:02d}. {route_detail_line(x, decision, score)}")
    print("")

    print("[BAD REALTIME] Discord 实时噪声 TOP30")
    print(f"conflict_downgraded_realtime_count={conflict_downgraded_realtime_count}")
    print(f"leading_risk_downgrade_count={leading_risk_downgrade_count}")
    print(f"weak_external_confirmation_downgrade_count={weak_external_confirmation_downgrade_count}")
    for index, (score, x, decision) in enumerate(bad_realtime[:30], start=1):
        print(f"{index:02d}. {route_detail_line(x, decision, score)}")
    print("")

    print("[BAD PRIORITY OBSERVE] Discord 重点观察噪声 TOP20")
    for index, (score, x, decision) in enumerate(bad_priority[:20], start=1):
        print(f"{index:02d}. {route_detail_line(x, decision, score)}")
    print("")

    print("[BAD BREAKOUT WATCH] Discord 爆发观察噪声 TOP20")
    for index, (score, x, decision) in enumerate(bad_breakout[:20], start=1):
        print(f"{index:02d}. {route_detail_line(x, decision, score)}")
    print("")

    print("[BAD BREAKOUT WATCH FALLBACK] Discord 爆发观察兜底噪声 TOP20")
    for index, (score, x, decision) in enumerate(bad_breakout_fallback[:20], start=1):
        print(f"{index:02d}. {route_detail_line(x, decision, score)}")
    print("")


def feature_bucket(value, buckets, default="-"):
    if value is None:
        return default
    for label, low, high in buckets:
        if (low is None or value >= low) and (high is None or value <= high):
            return label
    return default


def funding_bucket(value):
    if value is None:
        return "-"
    if value <= -0.03:
        return "极端负费率"
    if value < -0.005:
        return "负费率"
    if value < 0.01:
        return "费率中性"
    if value < 0.03:
        return "正费率"
    return "极端正费率"


def trap_bucket(value):
    return feature_bucket(value, [("0-2低", 0, 2), ("3-5中", 3, 5), ("6-7高", 6, 7), ("8-10极高", 8, 10)])


def conviction_bucket(value):
    return feature_bucket(value, [("0-49低", 0, 49), ("50-64中低", 50, 64), ("65-79中高", 65, 79), ("80+高", 80, None)])


def leading_bucket(value):
    return feature_bucket(value, [("0无", 0, 0), ("1-2弱", 1, 2), ("3-5中", 3, 5), ("6+强", 6, None)])


def evidence_bucket(value):
    return feature_bucket(value, [("<=0弱/风险", None, 0), ("1-4偏弱", 1, 4), ("5-7中", 5, 7), ("8+强", 8, None)])


def quality_bucket(value):
    return feature_bucket(value, [("0-24极低", 0, 24), ("25-39低", 25, 39), ("40-54中低", 40, 54), ("55-69中", 55, 69), ("70+高", 70, None)])


def missed_winner_mfe_score(row):
    checks = (("1h", 3), ("4h", 6), ("12h", 10))
    scores = []
    for horizon, threshold in checks:
        value = row.get(f"mfe_{horizon}")
        if row.get(horizon) is not None and value is not None:
            scores.append((value / threshold, horizon, value))
    if not scores:
        return None
    best = max(scores, key=lambda item: item[0])
    return best if best[0] >= 1 else None


def is_digest_or_suppressed(row, decision):
    return decision.route == "digest" or row.get("suppressed_from_telegram") == 1


def print_counter_block(title, rows, getter, limit=20):
    counts = Counter(getter(row) or "-" for row in rows)
    print(title)
    if not counts:
        print("  -")
        return
    for key, count in counts.most_common(limit):
        pct_text = count / len(rows) * 100 if rows else 0
        print(f"  {key}: {count} ({pct_text:.1f}%)")


def feature_lift_rows(winners, baseline, getter, min_count=3):
    winner_counts = Counter(getter(row) or "-" for row in winners)
    base_counts = Counter(getter(row) or "-" for row in baseline)
    rows = []
    for key, count in winner_counts.items():
        if count < min_count:
            continue
        winner_rate = count / len(winners) if winners else 0
        base_rate = base_counts.get(key, 0) / len(baseline) if baseline else 0
        lift = winner_rate / base_rate if base_rate > 0 else 99
        rows.append((lift, winner_rate, base_rate, key, count, base_counts.get(key, 0)))
    return sorted(rows, reverse=True)


def print_feature_lifts(title, winners, baseline, getter, limit=8):
    rows = feature_lift_rows(winners, baseline, getter)
    print(title)
    if not rows:
        print("  -")
        return
    for lift, winner_rate, base_rate, key, count, base_count in rows[:limit]:
        print(
            f"  {key}: 漏网赢家 {count}/{len(winners)} {winner_rate*100:.1f}% | "
            f"非赢家 {base_count}/{len(baseline)} {base_rate*100:.1f}% | lift {lift:.2f}x"
        )


def print_missed_winner_features_report(results):
    routed = [(x, route_simulation_decision(x)) for x in results]
    candidate_rows = [(x, decision) for x, decision in routed if is_digest_or_suppressed(x, decision)]
    winners = []
    baseline = []
    for row, decision in candidate_rows:
        score = missed_winner_mfe_score(row)
        if score is not None:
            winners.append((score, row, decision))
        else:
            baseline.append(row)
    winner_rows = [row for _score, row, _decision in winners]
    winners.sort(key=lambda item: item[0][0], reverse=True)

    print("[MISSED WINNER FEATURES] 低分漏网赢家特征")
    print("定义: route=digest 或 suppressed=1，且 1h MFE>=3% / 4h MFE>=6% / 12h MFE>=10%，只统计可评估样本。")
    print(f"漏网候选样本: {len(candidate_rows)}")
    print(f"漏网赢家样本: {len(winner_rows)}")
    print(f"非赢家 digest/suppressed 对照样本: {len(baseline)}")
    print("")

    print("漏网赢家 TOP20")
    for index, (score, row, decision) in enumerate(winners[:20], start=1):
        _ratio, horizon, value = score
        print(
            f"{index:02d}. {row['symbol']} {row['kind']} route={decision.route} "
            f"winner={horizon} MFE={fmt(value)} 1hMFE={fmt(row.get('mfe_1h'))} "
            f"4hMFE={fmt(row.get('mfe_4h'))} 12hMFE={fmt(row.get('mfe_12h'))} "
            f"conv={row.get('conviction_score') if row.get('conviction_score') is not None else '-'} "
            f"q={row.get('signal_priority') or '-'}/{row.get('signal_quality_score') if row.get('signal_quality_score') is not None else '-'} "
            f"lead={row.get('leading_score') if row.get('leading_score') is not None else '-'} "
            f"ev={row.get('evidence_score') if row.get('evidence_score') is not None else '-'} "
            f"flow={row.get('flow_trend_label') or '-'} kline={row.get('kline_label') or '-'}"
        )
    print("")

    print_counter_block("symbol TOP20", winner_rows, lambda row: row.get("symbol"), 20)
    print_counter_block("kind 分布", winner_rows, lambda row: row.get("kind"), 20)
    print_counter_block("priority 分布", winner_rows, lambda row: row.get("signal_priority"), 20)
    print_counter_block("quality 分布", winner_rows, lambda row: quality_bucket(row.get("signal_quality_score")), 20)
    print_counter_block("conviction 分布", winner_rows, lambda row: conviction_bucket(row.get("conviction_score")), 20)
    print_counter_block("leading_score 分布", winner_rows, lambda row: leading_bucket(row.get("leading_score")), 20)
    print_counter_block("evidence_score 分布", winner_rows, lambda row: evidence_bucket(row.get("evidence_score")), 20)
    print_counter_block("flow_label 分布", winner_rows, lambda row: row.get("flow_trend_label"), 20)
    print_counter_block("entry_label 分布", winner_rows, lambda row: row.get("entry_timing_label"), 20)
    print_counter_block("trap_risk 分布", winner_rows, lambda row: trap_bucket(row.get("trap_risk_score")), 20)
    print_counter_block("funding 状态", winner_rows, lambda row: funding_bucket(row.get("funding_rate_percent")), 20)
    print_counter_block("basis_state 分布", winner_rows, lambda row: row.get("basis_state"), 20)
    print_counter_block("squeeze_state 分布", winner_rows, lambda row: row.get("squeeze_state_label"), 20)
    print_counter_block("K线结构 label 分布", winner_rows, lambda row: row.get("kline_label"), 20)
    print_counter_block("K线 short 分布", winner_rows, lambda row: str(row.get("kline_short_score")), 20)
    print_counter_block("K线 mid 分布", winner_rows, lambda row: str(row.get("kline_mid_score")), 20)
    print_counter_block("K线 long 分布", winner_rows, lambda row: str(row.get("kline_long_score")), 20)
    print("")

    print("与非赢家 digest/suppressed 对比: 显著更高特征")
    print_feature_lifts("kind lift", winner_rows, baseline, lambda row: row.get("kind"))
    print_feature_lifts("priority lift", winner_rows, baseline, lambda row: row.get("signal_priority"))
    print_feature_lifts("conviction lift", winner_rows, baseline, lambda row: conviction_bucket(row.get("conviction_score")))
    print_feature_lifts("leading lift", winner_rows, baseline, lambda row: leading_bucket(row.get("leading_score")))
    print_feature_lifts("evidence lift", winner_rows, baseline, lambda row: evidence_bucket(row.get("evidence_score")))
    print_feature_lifts("flow lift", winner_rows, baseline, lambda row: row.get("flow_trend_label"))
    print_feature_lifts("entry lift", winner_rows, baseline, lambda row: row.get("entry_timing_label"))
    print_feature_lifts("trap lift", winner_rows, baseline, lambda row: trap_bucket(row.get("trap_risk_score")))
    print_feature_lifts("K线 label lift", winner_rows, baseline, lambda row: row.get("kline_label"))
    print_feature_lifts("K线 short lift", winner_rows, baseline, lambda row: str(row.get("kline_short_score")))
    print("")

    print("噪声里也很多，不能单独用")
    noisy_features = (
        ("priority", lambda row: row.get("signal_priority")),
        ("conviction", lambda row: conviction_bucket(row.get("conviction_score"))),
        ("quality", lambda row: quality_bucket(row.get("signal_quality_score"))),
        ("flow", lambda row: row.get("flow_trend_label")),
        ("trap", lambda row: trap_bucket(row.get("trap_risk_score"))),
        ("funding", lambda row: funding_bucket(row.get("funding_rate_percent"))),
    )
    for label, getter in noisy_features:
        rows = feature_lift_rows(winner_rows, baseline, getter, min_count=5)
        common_noise = [
            item for item in rows
            if item[2] >= 0.15 and item[0] < 1.5
        ][:5]
        if common_noise:
            print(f"{label}: " + "; ".join(f"{key} 非赢家占比{base_rate*100:.1f}% lift{lift:.2f}x" for lift, _wr, base_rate, key, _count, _bc in common_noise))
    print("")

    print("候选提升规则（仅建议，未改线上路由）")
    print("1. D/C 质量但 leading>=6 + K线短线分>=8 + kind in discovery/bottom_reversal，可提升到 priority_observe。")
    print("2. conviction<50 但 evidence>=8 + flow_label=短强中弱/资金分歧 + 1h OI增仓，可进入观察层，不直接实时。")
    print("3. D priority 但 entry_label=启动前/启动初期/启动观察 + trap<=5 + K线短中线同时>=6，可提升为重点观察。")
    print("4. discovery 低分但 evidence_direction=看多 + evidence_score>=8 + basis 正常/贴水 + squeeze 非强挤压，可从 digest 拉到 priority_observe。")
    print("5. bottom_reversal 低分但 flow_label 非中长线派发 + 15m/1h MFE早期突破代理强，可进观察摘要前排，仍要求等待承接确认。")
    print("")


def print_data_coverage(total_rows, results, ctx: BacktestKlineContext):
    print("[DATA COVERAGE] 回测数据覆盖")
    print(f"总样本: {total_rows}")
    print(f"可评估样本: {len(results)}")
    print(f"data_missing 样本: {len(ctx.data_missing)}")
    print(f"429 次数: {ctx.request_429}")
    print(f"cache_hit: {ctx.cache_hit}")
    print(f"cache_miss: {ctx.cache_miss}")
    print(f"stale_cache_used: {ctx.stale_cache_used}")
    if ctx.data_missing:
        print("data_missing by symbol TOP20")
        for symbol, count in Counter(item["symbol"] for item in ctx.data_missing).most_common(20):
            print(f"  {symbol}: {count}")
        print("data_missing by kind TOP20")
        for kind, count in Counter(item["kind"] for item in ctx.data_missing).most_common(20):
            print(f"  {kind}: {count}")
    print("")


def run_backtest(args):
    cfg = yaml.safe_load(Path(args.config).read_text()) or {}
    path = Path(str(cfg.get("signal_log_path", "signals.csv")))
    if not path.is_absolute():
        path = Path.cwd() / path
    rows = list(csv.DictReader(path.open("r", encoding="utf-8")))[-args.limit:][::-1]

    session = requests.Session()
    ctx = BacktestKlineContext(
        cache_dir=args.cache_dir,
        no_network=args.no_network,
        sleep_seconds=args.sleep_seconds,
        max_retries=args.max_retries,
    )
    ctx.total_rows = len(rows)
    results = []
    for row in rows:
        try:
            item = eval_signal(session, row, ctx=ctx)
            if item:
                results.append(item)
        except DataMissingError as e:
            ctx.note_missing(row, e.reason)
        except Exception as e:
            print(f"skip {row.get('symbol','-')}: {type(e).__name__}: {e}")
            ctx.note_missing(row, f"{type(e).__name__}: {e}")

    if not results:
        raise SystemExit("no backtestable signals")

    print_data_coverage(len(rows), results, ctx)
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
    print_missed_winner_features_report(results)
    print_route_simulation_report(results)


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
    ap.add_argument("--cache-dir", default=".cache/backtest_klines")
    ap.add_argument("--no-network", action="store_true")
    ap.add_argument("--sleep-seconds", type=float, default=0.25)
    ap.add_argument("--max-retries", type=int, default=5)
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
