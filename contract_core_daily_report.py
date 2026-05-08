import argparse
import csv
import datetime as dt
import io
import statistics
from collections import Counter
from pathlib import Path

import requests

import backtest_signals as bt
import contract_core_backtest as core


OUTPUT_DIR = Path("reports/contract_core")
CONTEXT_PERIODS = ("15m", "1h", "4h", "24h", "72h", "168h")
STAT_HORIZONS = ("5m", "15m", "30m", "1h", "4h", "24h")


def directional_value(row, horizon, direction):
    value = row.get(horizon)
    if value is None:
        return None
    return value if row.get("side") == direction else -value


def stat_payload(rows, direction):
    payload = {}
    for horizon in STAT_HORIZONS:
        values = [directional_value(row, horizon, direction) for row in rows]
        values = [value for value in values if value is not None]
        mfe_values = []
        mae_values = []
        for row in rows:
            mfe_key = "mfe" if row.get("side") == direction else "mae"
            mae_key = "mae" if row.get("side") == direction else "mfe"
            if row.get(f"{mfe_key}_{horizon}") is not None:
                mfe_values.append(row[f"{mfe_key}_{horizon}"])
            if row.get(f"{mae_key}_{horizon}") is not None:
                mae_values.append(row[f"{mae_key}_{horizon}"])
        wins = sum(value > 0 for value in values)
        payload[horizon] = {
            "n": len(values),
            "wins": wins,
            "win_rate": wins / len(values) * 100 if values else 0.0,
            "avg": statistics.mean(values) if values else 0.0,
            "median": statistics.median(values) if values else 0.0,
            "mfe_avg": statistics.mean(mfe_values) if mfe_values else 0.0,
            "mae_avg": statistics.mean(mae_values) if mae_values else 0.0,
        }
    return payload


def stat_line(label, rows, direction):
    stats = stat_payload(rows, direction)
    parts = [f"{label}: 提醒={len(rows)}"]
    for horizon in STAT_HORIZONS:
        parts.append(
            f"{horizon} {stats[horizon]['wins']}/{stats[horizon]['n']} {stats[horizon]['win_rate']:.1f}% "
            f"avg={stats[horizon]['avg']:+.2f}% med={stats[horizon]['median']:+.2f}% "
            f"MFE={stats[horizon]['mfe_avg']:+.2f}% MAE={stats[horizon]['mae_avg']:+.2f}%"
        )
    return " | ".join(parts)


def load_results(args):
    rows = list(csv.DictReader(Path(args.signals).open("r", encoding="utf-8")))
    if args.date_from:
        rows = [row for row in rows if bt.row_day(row) >= args.date_from]
    if args.date_to:
        rows = [row for row in rows if bt.row_day(row) <= args.date_to]
    if args.limit and args.limit > 0:
        rows = rows[-args.limit:]

    session = requests.Session()
    ctx = bt.BacktestKlineContext(cache_dir=args.cache_dir, no_network=args.no_network)
    results = []
    for row in rows:
        try:
            item = bt.eval_signal(session, row, ctx=ctx)
            if item:
                results.append(item)
        except Exception:
            pass
    return results, ctx


def gate_rows(results):
    gates = {}
    for row in results:
        for gate, direction in core.alt_opportunity_gates(row):
            item = dict(row)
            item["gate_direction"] = direction
            gates.setdefault(gate, []).append(item)
    return gates


def watch_rows(results):
    gates = {}
    for row in results:
        for gate, direction in core.alt_watch_gates(row):
            item = dict(row)
            item["gate_direction"] = direction
            gates.setdefault(gate, []).append(item)
    return gates


def context_missing_report(results):
    lines = []
    total = len(results)
    if not total:
        return ["无样本"]
    for period in CONTEXT_PERIODS:
        fields = (
            f"oi_change_{period}_percent",
            f"global_long_short_ratio_{period}",
            f"top_position_ratio_{period}",
            f"top_account_ratio_{period}",
            f"taker_buy_sell_ratio_{period}",
        )
        present = sum(1 for row in results if any(row.get(field) is not None for field in fields))
        lines.append(f"{period}: present={present}/{total} missing={(total-present)/total*100:.1f}%")
    return lines


def failure_lines(rows, direction, limit=20):
    failed = []
    for row in rows:
        value = directional_value(row, "1h", direction)
        if value is not None and value <= 0:
            failed.append((value, row))
    failed.sort(key=lambda item: item[0])
    lines = []
    for value, row in failed[:limit]:
        lines.append(
            f"{row.get('row_day') or '-'} {row['symbol']} {row['kind']} "
            f"1h={value:+.2f}% 4h={directional_value(row, '4h', direction) if directional_value(row, '4h', direction) is not None else 0:+.2f}% "
            f"p={row.get('price_change_percent') or 0:+.2f}% oi={row.get('oi_change_percent') or 0:+.2f}% "
            f"taker={row.get('taker_buy_sell_ratio') or 0:.3f} funding={row.get('funding_rate_percent') or 0:+.3f}%"
        )
    return lines or ["无"]


def build_report(args, results, ctx):
    gates = gate_rows(results)
    watches = watch_rows(results)
    fast_drop = gates.get("fast_drop_risk", [])
    fast_trade = gates.get("fast_drop_trade", [])
    fast_precise = gates.get("fast_drop_trade_precise", [])
    alt_start = gates.get("alt_start_confirmed", [])
    alt_early = gates.get("alt_start_early", [])
    alt_acceleration = gates.get("alt_acceleration", [])
    alt_oi_spike = watches.get("alt_oi_spike_watch", [])
    mainstream = []
    for row in results:
        state = core.mainstream_state(row)
        direction = core.MAINSTREAM_STATE_DIRECTIONS.get(state, "neutral")
        if state != "neutral" and direction != "neutral":
            item = dict(row)
            item["mainstream_state"] = state
            item["mainstream_direction"] = direction
            mainstream.append(item)

    out = io.StringIO()
    out.write("合约核心雷达离线日报\n")
    out.write(f"窗口: {args.date_from} -> {args.date_to}\n")
    out.write(f"样本: {len(results)} | data_missing={len(ctx.data_missing)} cache_hit={ctx.cache_hit} cache_miss={ctx.cache_miss} stale={ctx.stale_cache_used}\n")
    out.write("K线结构未参与筛选；MFE/MAE 为触发后窗口内最大顺向/反向幅度。\n\n")

    out.write("主流币市场状态\n")
    if mainstream:
        out.write(f"mainstream_state: 样本={len(mainstream)}\n")
        for state in ("trend_long", "top_risk"):
            rows = [row for row in mainstream if row["mainstream_state"] == state]
            if rows:
                out.write(stat_line(f"mainstream_{state}", rows, core.MAINSTREAM_STATE_DIRECTIONS[state]) + "\n")
    else:
        out.write("无\n")

    out.write("\n小山寨上涨机会\n")
    out.write(stat_line("alt_start_confirmed", alt_start, "long") + "\n")
    out.write(stat_line("alt_start_early", alt_early, "long") + "\n")
    out.write(stat_line("alt_acceleration", alt_acceleration, "long") + "\n")

    out.write("\n小山寨爆发候选池\n")
    out.write(stat_line("alt_oi_spike_watch", alt_oi_spike, "long") + "\n")

    out.write("\n小山寨下跌机会\n")
    out.write(stat_line("fast_drop_trade_precise", fast_precise, "short") + "\n")
    out.write(stat_line("fast_drop_trade", fast_trade, "short") + "\n")
    out.write(stat_line("fast_drop_risk", fast_drop, "short") + "\n\n")

    out.write("fast_drop_risk 按天\n")
    for day, count in sorted(Counter(row.get("row_day") for row in fast_drop).items()):
        day_rows = [row for row in fast_drop if row.get("row_day") == day]
        out.write(stat_line(str(day), day_rows, "short") + "\n")
    out.write("\nfast_drop_risk 按类型\n")
    for kind, count in Counter(row["kind"] for row in fast_drop).most_common():
        kind_rows = [row for row in fast_drop if row["kind"] == kind]
        out.write(stat_line(kind, kind_rows, "short") + "\n")

    out.write("\nfast_drop_trade 按天\n")
    for day, count in sorted(Counter(row.get("row_day") for row in fast_trade).items()):
        day_rows = [row for row in fast_trade if row.get("row_day") == day]
        out.write(stat_line(str(day), day_rows, "short") + "\n")

    out.write("\nfast_drop_trade_precise 按天\n")
    for day, count in sorted(Counter(row.get("row_day") for row in fast_precise).items()):
        day_rows = [row for row in fast_precise if row.get("row_day") == day]
        out.write(stat_line(str(day), day_rows, "short") + "\n")

    out.write("\nalt_start_confirmed 按天\n")
    for day, count in sorted(Counter(row.get("row_day") for row in alt_start).items()):
        day_rows = [row for row in alt_start if row.get("row_day") == day]
        out.write(stat_line(str(day), day_rows, "long") + "\n")

    out.write("\nalt_start_early 按天\n")
    for day, count in sorted(Counter(row.get("row_day") for row in alt_early).items()):
        day_rows = [row for row in alt_early if row.get("row_day") == day]
        out.write(stat_line(str(day), day_rows, "long") + "\n")

    out.write("\nalt_acceleration 按天\n")
    for day, count in sorted(Counter(row.get("row_day") for row in alt_acceleration).items()):
        day_rows = [row for row in alt_acceleration if row.get("row_day") == day]
        out.write(stat_line(str(day), day_rows, "long") + "\n")

    out.write("\nalt_start_confirmed 失败样本 TOP20\n")
    out.write("\n".join(failure_lines(alt_start, "long", 20)) + "\n")

    out.write("\n多周期合约字段缺失率\n")
    out.write("\n".join(context_missing_report(results)) + "\n")
    return out.getvalue()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", default="signals.csv")
    parser.add_argument("--date-from")
    parser.add_argument("--date-to")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--cache-dir", default=".cache/backtest_klines")
    parser.add_argument("--no-network", action="store_true")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR))
    args = parser.parse_args()

    today = dt.date.today().isoformat()
    args.date_to = args.date_to or today
    args.date_from = args.date_from or (dt.date.fromisoformat(args.date_to) - dt.timedelta(days=5)).isoformat()

    results, ctx = load_results(args)
    report = build_report(args, results, ctx)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"contract_core_{args.date_from}_{args.date_to}.txt"
    path.write_text(report, encoding="utf-8")
    latest = output_dir / "latest_contract_core.txt"
    latest.write_text(report, encoding="utf-8")
    print(report)
    print(f"report_path={path}")


if __name__ == "__main__":
    main()
