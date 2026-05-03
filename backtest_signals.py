import argparse
import csv
import datetime as dt
import statistics
import time
from pathlib import Path

import requests
import yaml

BASE = "https://fapi.binance.com"
BULL = {"discovery", "hot_breakout", "bottom_reversal"}
BEAR = {"top_risk", "distribution", "top_exhaustion"}
HORIZONS = {"15m": 900, "1h": 3600, "4h": 14400, "12h": 43200, "24h": 86400}
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
    out = {"symbol": symbol, "kind": kind, "side": side}
    out["long_flow_alignment_score"] = parse_int(row.get("long_flow_alignment_score"))
    out["main_asset_score"] = parse_int(row.get("main_asset_score"))
    out["trap_risk_score"] = parse_int(row.get("trap_risk_score"))
    out["signal_priority"] = (row.get("signal_priority") or "").strip().upper() or None
    out["signal_quality_score"] = parse_int(row.get("signal_quality_score"))
    out["suppressed_from_telegram"] = parse_bool_int(row.get("suppressed_from_telegram"))
    for name in ("12h", "24h"):
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


def flow_detail(x):
    parts = []
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
    for name in ("12h", "24h"):
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


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="derivatives_config.yaml")
    ap.add_argument("--limit", type=int, default=80)
    args = ap.parse_args()

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
    print("说明: discovery/hot 按看多计算，top_risk/distribution 按看空计算")
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

    print("最近20条:")
    for x in results[:20]:
        print(
            f"{x['symbol']} {x['kind']} {x['side']} "
            f"15m={fmt(x['15m'])} 1h={fmt(x['1h'])} 4h={fmt(x['4h'])} "
            f"12h={fmt(x['12h'])} 24h={fmt(x['24h'])}"
            f"{flow_detail(x)} MFE={fmt(x['mfe'])} MAE={fmt(x['mae'])}"
        )


if __name__ == "__main__":
    main()
