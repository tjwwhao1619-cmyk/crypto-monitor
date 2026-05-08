import argparse
import csv
import statistics
from collections import Counter
from pathlib import Path

import requests

import backtest_signals as bt


STAT_HORIZONS = ("5m", "15m", "30m", "1h", "4h", "24h")
BULL_KINDS = {"discovery", "bottom_reversal", "hot_breakout", "main_trend_watch", "main_momentum_watch"}
RISK_KINDS = {"top_risk", "top_exhaustion", "distribution", "main_risk_watch"}
MAINSTREAM_SYMBOLS = {
    "BTCUSDT",
    "ETHUSDT",
    "SOLUSDT",
    "BNBUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "XRPUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "TONUSDT",
    "OPUSDT",
    "ARBUSDT",
}
ALT_LONG_GATES = ("alt_start_early", "alt_start_confirmed", "alt_acceleration")
ALT_SHORT_GATES = ("fast_drop_trade_precise", "fast_drop_trade", "fast_drop_risk", "short_crowded_long_reverse")
ALT_OPPORTUNITY_GATES = ALT_LONG_GATES + ALT_SHORT_GATES
ALT_WATCH_GATES = ("alt_oi_spike_watch",)
MAINSTREAM_STATE_DIRECTIONS = {
    "trend_long": "long",
    "top_risk": "short",
}


def nz(value, default=0.0):
    return default if value is None else value


def sign(value):
    value = nz(value)
    if value > 0:
        return 1
    if value < 0:
        return -1
    return 0


def bucket_funding(value):
    value = nz(value)
    if value <= -0.05:
        return "extreme_negative"
    if value < -0.01:
        return "negative"
    if value <= 0.01:
        return "neutral"
    if value < 0.05:
        return "positive"
    return "extreme_positive"


def count_direction(row, periods, direction):
    target = 1 if direction == "long" else -1
    return sum(1 for period in periods if sign(row.get(f"net_flow_{period}_usd")) == target)


def flow_profile(row, direction):
    short_periods = ("15m", "1h")
    mid_periods = ("4h", "12h", "24h")
    long_periods = ("72h", "120h", "144h")
    return {
        "short": count_direction(row, short_periods, direction),
        "mid": count_direction(row, mid_periods, direction),
        "long": count_direction(row, long_periods, direction),
        "short_total": len(short_periods),
        "mid_total": len(mid_periods),
        "long_total": len(long_periods),
    }


def ratio_score(row, direction):
    global_ratio = nz(row.get("global_long_short_ratio"))
    top_position = nz(row.get("top_position_ratio"))
    top_account = nz(row.get("top_account_ratio"))
    score = 0
    if direction == "long":
        if top_position >= 1.15:
            score += 2
        elif top_position >= 1.05:
            score += 1
        if top_account >= 1.15:
            score += 2
        elif top_account >= 1.05:
            score += 1
        if 0.9 <= global_ratio <= 1.8:
            score += 1
        if global_ratio >= 2.2:
            score -= 1
    else:
        if 0 < top_position <= 0.85:
            score += 2
        elif 0 < top_position <= 0.95:
            score += 1
        if 0 < top_account <= 0.85:
            score += 2
        elif 0 < top_account <= 0.95:
            score += 1
        if 0 < global_ratio <= 1.0:
            score += 1
        if global_ratio >= 2.2:
            score -= 1
    return score


def crowding_score(row, direction):
    global_ratio = nz(row.get("global_long_short_ratio"))
    top_position = nz(row.get("top_position_ratio"))
    top_account = nz(row.get("top_account_ratio"))
    funding = nz(row.get("funding_rate_percent"))
    score = 0
    if direction == "long":
        if global_ratio >= 1.8:
            score += 1
        if top_position >= 1.8:
            score += 1
        if top_account >= 1.8:
            score += 1
        if funding >= 0.05:
            score += 2
        elif funding >= 0.03:
            score += 1
    else:
        if 0 < global_ratio <= 0.8:
            score += 1
        if 0 < top_position <= 0.8:
            score += 1
        if 0 < top_account <= 0.8:
            score += 1
        if funding <= -0.05:
            score += 2
        elif funding <= -0.03:
            score += 1
    return score


def funding_score(row, direction):
    funding = nz(row.get("funding_rate_percent"))
    if direction == "long":
        if -0.05 <= funding <= 0.03:
            return 2
        if funding < -0.05:
            return 1
        return -1
    if -0.03 <= funding <= 0.05:
        return 2
    if funding > 0.05:
        return 1
    return -1


def taker_score(row, direction):
    taker = nz(row.get("taker_buy_sell_ratio"))
    if direction == "long":
        if taker >= 1.15:
            return 3
        if taker >= 1.08:
            return 2
        if taker >= 1.02:
            return 1
        return 0
    if 0 < taker <= 0.87:
        return 3
    if 0 < taker <= 0.93:
        return 2
    if 0 < taker <= 0.98:
        return 1
    return 0


def oi_score(row):
    oi = nz(row.get("oi_change_percent"))
    if oi >= 12:
        return 3
    if oi >= 8:
        return 2
    if oi >= 4:
        return 1
    return 0


def contract_score(row, direction):
    flow = flow_profile(row, direction)
    score = 0
    score += oi_score(row)
    score += taker_score(row, direction)
    score += ratio_score(row, direction)
    score += funding_score(row, direction)
    score += flow["short"]
    score += flow["mid"] * 2
    score += flow["long"] * 2
    score -= crowding_score(row, direction)
    return score, flow


def volume_score(row):
    volume_ratio = nz(row.get("volume_ratio_24h"))
    quote_volume = nz(row.get("quote_volume_24h"))
    score = 0
    if volume_ratio >= 2.0:
        score += 2
    elif volume_ratio >= 1.2:
        score += 1
    elif 0 < volume_ratio < 0.7:
        score -= 1
    if quote_volume >= 1_000_000_000:
        score += 1
    return score


def range_24h_percent(row):
    high = nz(row.get("high_24h"))
    low = nz(row.get("low_24h"))
    if high <= 0 or low <= 0 or high <= low:
        return 0.0
    return (high - low) / low * 100


def volatility_score(row):
    range_pct = range_24h_percent(row)
    score = 0
    if range_pct >= 20:
        score += 3
    elif range_pct >= 12:
        score += 2
    elif range_pct >= 8:
        score += 1
    if nz(row.get("volume_ratio_24h")) >= 2:
        score += 1
    return score


def spot_flow_score(row, direction):
    spot_label = row.get("spot_absorption_label")
    spot_score = nz(row.get("spot_absorption_score"))
    onchain_label = row.get("spot_onchain_label")
    onchain_score = nz(row.get("spot_onchain_score"))
    major_label = row.get("major_flow_label")
    major_score = nz(row.get("major_flow_score"))
    divergence_label = row.get("contract_spot_divergence_label")
    divergence_score = nz(row.get("contract_spot_divergence_score"))

    score = 0
    if direction == "long":
        if spot_label in {"现货承接", "链上承接"}:
            score += 3 if spot_score >= 7 else 2
        elif spot_label in {"现货出货", "链上出货"}:
            score -= 4
        elif spot_label in {"现货未跟", "承接不明", "数据不足"}:
            score -= 1
        if onchain_label == "强":
            score += 2
        elif onchain_label == "弱":
            score -= 2
        if major_label == "主力偏多":
            score += 1 if major_score < 7 else 2
        elif major_label == "主力偏空":
            score -= 2
    else:
        if spot_label in {"现货出货", "链上出货"}:
            score += 3 if spot_score >= 7 else 2
        elif spot_label in {"现货承接", "链上承接"}:
            score -= 3
        if onchain_label == "弱":
            score += 2
        elif onchain_label == "强":
            score -= 2
        if major_label == "主力偏空":
            score += 1 if major_score < 7 else 2
        elif major_label == "主力偏多":
            score -= 2
    if divergence_label == "明显背离" or divergence_score >= 7:
        score -= 2
    elif divergence_label == "轻微背离" or divergence_score >= 4:
        score -= 1
    return score


def long_spot_or_onchain_confirmed(row):
    return (
        row.get("spot_absorption_label") in {"现货承接", "链上承接"}
        and row.get("spot_onchain_label") == "强"
        and spot_flow_score(row, "long") >= 2
        and nz(row.get("contract_spot_divergence_score")) < 4
    )


def is_mainstream(row):
    return row.get("symbol") in MAINSTREAM_SYMBOLS


def is_altcoin(row):
    return row.get("symbol") not in MAINSTREAM_SYMBOLS


def model_signal(row):
    long_score, long_flow = contract_score(row, "long")
    short_score, short_flow = contract_score(row, "short")
    margin = abs(long_score - short_score)
    if long_score >= 12 and margin >= 3 and long_flow["mid"] >= 2 and long_flow["long"] >= 2:
        return "long", long_score, short_score, "strong_long"
    if short_score >= 12 and margin >= 3 and short_flow["mid"] >= 2 and short_flow["long"] >= 2:
        return "short", long_score, short_score, "strong_short"
    if long_score >= 10 and margin >= 4 and long_flow["mid"] >= 2:
        return "long", long_score, short_score, "watch_long"
    if short_score >= 10 and margin >= 4 and short_flow["mid"] >= 2:
        return "short", long_score, short_score, "watch_short"
    return "neutral", long_score, short_score, "no_alert"


def directional_value(row, horizon, direction):
    value = row.get(horizon)
    if value is None or direction == "neutral":
        return None
    if row.get("side") == direction:
        return value
    return -value


def stats(rows, direction_getter):
    output = {}
    for horizon in STAT_HORIZONS:
        values = []
        mfe_values = []
        mae_values = []
        for row in rows:
            direction = direction_getter(row)
            value = directional_value(row, horizon, direction)
            if value is not None:
                values.append(value)
            if direction != "neutral":
                mfe_key = "mfe" if row.get("side") == direction else "mae"
                mae_key = "mae" if row.get("side") == direction else "mfe"
                if row.get(f"{mfe_key}_{horizon}") is not None:
                    mfe_values.append(row[f"{mfe_key}_{horizon}"])
                if row.get(f"{mae_key}_{horizon}") is not None:
                    mae_values.append(row[f"{mae_key}_{horizon}"])
        wins = sum(value > 0 for value in values)
        output[horizon] = {
            "n": len(values),
            "wins": wins,
            "win_rate": wins / len(values) * 100 if values else 0.0,
            "avg": statistics.mean(values) if values else 0.0,
            "median": statistics.median(values) if values else 0.0,
            "mfe_avg": statistics.mean(mfe_values) if mfe_values else 0.0,
            "mfe_median": statistics.median(mfe_values) if mfe_values else 0.0,
            "mae_avg": statistics.mean(mae_values) if mae_values else 0.0,
            "mae_median": statistics.median(mae_values) if mae_values else 0.0,
        }
    return output


def print_stat(label, rows, direction_getter):
    s = stats(rows, direction_getter)
    parts = [f"{label}: 提醒={len(rows)}"]
    for horizon in STAT_HORIZONS:
        parts.append(
            f"{horizon}={s[horizon]['wins']}/{s[horizon]['n']} {s[horizon]['win_rate']:.1f}% "
            f"avg={s[horizon]['avg']:+.2f}% med={s[horizon]['median']:+.2f}% "
            f"mfe={s[horizon]['mfe_avg']:+.2f}% mae={s[horizon]['mae_avg']:+.2f}%"
        )
    print(" ".join(parts))


def candidate_gates(row):
    long_score, long_flow = contract_score(row, "long")
    short_score, short_flow = contract_score(row, "short")
    row = {
        **row,
        "long_score": long_score,
        "short_score": short_score,
        **{f"long_flow_{key}": value for key, value in long_flow.items()},
        **{f"short_flow_{key}": value for key, value in short_flow.items()},
    }

    def v(key):
        return nz(row.get(key))

    gates = []
    if (
        oi_score(row) >= 1
        and v("funding_rate_percent") >= 0.05
        and taker_score(row, "short") >= 1
        and row["short_flow_mid"] >= 2
        and row["short_flow_long"] >= 1
    ):
        gates.append(("short_crowded_long_reverse", "short"))
    if (
        oi_score(row) >= 1
        and taker_score(row, "short") >= 1
        and row["short_flow_mid"] >= 2
        and row["short_flow_long"] >= 2
        and v("net_flow_24h_usd") < 0
    ):
        gates.append(("short_distribution", "short"))
    if (
        row.get("kind") in BULL_KINDS
        and oi_score(row) >= 1
        and taker_score(row, "long") >= 2
        and row["long_flow_short"] >= 1
        and row["long_flow_mid"] >= 2
        and row["long_flow_long"] >= 2
        and funding_score(row, "long") >= 1
        and crowding_score(row, "long") <= 2
    ):
        gates.append(("long_strict_trend", "long"))
    if (
        row.get("kind") in BULL_KINDS
        and -2 <= v("price_change_percent") <= 3
        and oi_score(row) >= 1
        and taker_score(row, "long") >= 2
        and row["long_flow_short"] >= 1
        and row["long_flow_mid"] >= 1
        and row["long_flow_long"] >= 1
        and v("funding_rate_percent") < 0.03
        and crowding_score(row, "long") <= 2
    ):
        gates.append(("fast_start_long", "long"))
    if (
        row.get("kind") in {"discovery", "bottom_reversal", "main_trend_watch"}
        and row.get("symbol") not in MAINSTREAM_SYMBOLS
        and -2 <= v("price_change_percent") <= 3
        and oi_score(row) >= 1
        and taker_score(row, "long") >= 2
        and volume_score(row) >= 1
        and row["long_flow_short"] >= 1
        and row["long_flow_mid"] >= 1
        and row["long_flow_long"] >= 1
        and -0.03 <= v("funding_rate_percent") < 0.03
        and crowding_score(row, "long") <= 2
        and long_spot_or_onchain_confirmed(row)
    ):
        gates.append(("alt_start_confirmed", "long"))
    if (
        row.get("kind") in {"discovery", "bottom_reversal", "main_trend_watch"}
        and row.get("symbol") not in MAINSTREAM_SYMBOLS
        and -2 <= v("price_change_percent") <= 2
        and oi_score(row) >= 1
        and taker_score(row, "long") >= 2
        and volume_score(row) >= 1
        and v("net_flow_15m_usd") > 0
        and row["long_flow_short"] >= 1
        and row["long_flow_mid"] >= 1
        and row["long_flow_long"] >= 1
        and -0.03 <= v("funding_rate_percent") < 0.03
        and crowding_score(row, "long") <= 2
        and long_spot_or_onchain_confirmed(row)
    ):
        gates.append(("alt_start_early", "long"))
    if (
        row.get("kind") in {"discovery", "bottom_reversal", "main_trend_watch"}
        and row.get("symbol") not in MAINSTREAM_SYMBOLS
        and -2 <= v("price_change_percent") <= 3
        and oi_score(row) >= 1
        and taker_score(row, "long") >= 2
        and volume_score(row) >= 1
        and row["long_flow_short"] >= 1
        and row["long_flow_mid"] >= 2
        and row["long_flow_long"] >= 1
        and -0.03 <= v("funding_rate_percent") < 0.03
        and crowding_score(row, "long") <= 2
        and long_spot_or_onchain_confirmed(row)
    ):
        gates.append(("alt_acceleration", "long"))
    if (
        row.get("kind") in RISK_KINDS
        and (v("price_change_percent") >= 4 or v("price_position_24h") >= 80)
        and oi_score(row) >= 2
        and v("funding_rate_percent") >= 0.05
        and taker_score(row, "short") >= 1
        and row["short_flow_short"] >= 1
        and row["short_flow_mid"] >= 1
        and row["short_flow_long"] >= 2
        and crowding_score(row, "long") >= 2
    ):
        gates.append(("fast_drop_risk", "short"))
    if (
        row.get("kind") in {"top_risk", "top_exhaustion"}
        and row.get("symbol") not in MAINSTREAM_SYMBOLS
        and (v("price_change_percent") >= 6 or v("price_position_24h") >= 90)
        and oi_score(row) >= 2
        and v("funding_rate_percent") >= 0.05
        and taker_score(row, "short") >= 2
        and row["short_flow_short"] >= 1
        and row["short_flow_mid"] >= 2
        and row["short_flow_long"] >= 2
        and crowding_score(row, "long") >= 2
    ):
        gates.append(("fast_drop_trade", "short"))
    if (
        row.get("kind") in {"top_risk", "top_exhaustion"}
        and row.get("symbol") not in MAINSTREAM_SYMBOLS
        and v("price_change_percent") >= 6
        and oi_score(row) >= 2
        and v("funding_rate_percent") >= 0.05
        and taker_score(row, "short") >= 2
        and row["short_flow_short"] >= 1
        and row["short_flow_mid"] >= 2
        and row["short_flow_long"] >= 2
        and crowding_score(row, "long") >= 3
    ):
        gates.append(("fast_drop_trade_precise", "short"))
    return gates


def alt_opportunity_gates(row):
    return [(gate, direction) for gate, direction in candidate_gates(row) if gate in ALT_OPPORTUNITY_GATES]


def alt_watch_gates(row):
    if not is_altcoin(row) or row.get("kind") not in {"discovery", "hot_breakout", "bottom_reversal"}:
        return []
    if (
        oi_score(row) >= 2
        and taker_score(row, "long") >= 2
        and nz(row.get("price_change_percent")) <= 12
        and nz(row.get("funding_rate_percent")) <= 0.05
    ):
        return [("alt_oi_spike_watch", "long")]
    return []


def mainstream_state(row):
    if not is_mainstream(row):
        return "neutral"
    _long_score, long_flow = contract_score(row, "long")
    active_volume = volume_score(row) >= 1
    if (
        active_volume
        and oi_score(row) >= 2
        and taker_score(row, "long") >= 2
        and long_flow["mid"] >= 1
        and row.get("spot_onchain_label") == "强"
        and row.get("spot_absorption_label") in {"现货承接", "链上承接"}
        and spot_flow_score(row, "long") >= 2
    ):
        return "trend_long"
    return "neutral"


def mainstream_direction(row):
    return MAINSTREAM_STATE_DIRECTIONS.get(mainstream_state(row), "neutral")


def load_results(args):
    cfg_rows = list(csv.DictReader(Path(args.signals).open("r", encoding="utf-8")))
    if args.date_from:
        cfg_rows = [row for row in cfg_rows if bt.row_day(row) >= args.date_from]
    if args.date_to:
        cfg_rows = [row for row in cfg_rows if bt.row_day(row) <= args.date_to]
    if args.limit and args.limit > 0:
        cfg_rows = cfg_rows[-args.limit:]

    session = requests.Session()
    ctx = bt.BacktestKlineContext(cache_dir=args.cache_dir, no_network=args.no_network)
    results = []
    for row in cfg_rows:
        try:
            item = bt.eval_signal(session, row, ctx=ctx)
            if item:
                results.append(item)
        except Exception:
            pass
    return results, ctx


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--signals", default="signals.csv")
    parser.add_argument("--date-from", default="2026-05-05")
    parser.add_argument("--date-to", default="2026-05-07")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--cache-dir", default=".cache/backtest_klines")
    parser.add_argument("--no-network", action="store_true")
    args = parser.parse_args()

    results, ctx = load_results(args)
    modeled = []
    for row in results:
        direction, long_score, short_score, label = model_signal(row)
        item = dict(row)
        item["model_direction"] = direction
        item["model_long_score"] = long_score
        item["model_short_score"] = short_score
        item["model_label"] = label
        modeled.append(item)

    alerts = [row for row in modeled if row["model_direction"] != "neutral"]
    strong = [row for row in alerts if row["model_label"].startswith("strong")]
    watch = [row for row in alerts if row["model_label"].startswith("watch")]

    print("[CONTRACT CORE BACKTEST]")
    print(f"样本={len(results)} data_missing={len(ctx.data_missing)} cache_hit={ctx.cache_hit} cache_miss={ctx.cache_miss} stale={ctx.stale_cache_used}")
    print("硬条件字段: OI / global-top多空比 / taker buy-sell / funding / 15m-1h-4h-1d-3d-6d资金流")
    print("K线结构: 未参与筛选")
    print("")
    print_stat("模型提醒综合", alerts, lambda row: row["model_direction"])
    print_stat("强提醒综合", strong, lambda row: row["model_direction"])
    print_stat("观察提醒综合", watch, lambda row: row["model_direction"])
    print_stat("多头模型提醒", [row for row in alerts if row["model_direction"] == "long"], lambda row: row["model_direction"])
    print_stat("空头模型提醒", [row for row in alerts if row["model_direction"] == "short"], lambda row: row["model_direction"])
    print("")
    print("按模型标签:")
    for label, _count in Counter(row["model_label"] for row in alerts).most_common():
        rows = [row for row in alerts if row["model_label"] == label]
        print_stat(label, rows, lambda row: row["model_direction"])
    print("")
    print("按原始 kind:")
    for kind, _count in Counter(row["kind"] for row in alerts).most_common():
        rows = [row for row in alerts if row["kind"] == kind]
        print_stat(kind, rows, lambda row: row["model_direction"])
    print("")
    print("按天提醒数:")
    for day, count in sorted(Counter(row.get("row_day") for row in alerts).items()):
        print(f"{day}: {count}")
    print("")
    print("小山寨上涨机会:")
    gate_rows: dict[str, list[dict]] = {}
    for row in modeled:
        for gate, direction in alt_opportunity_gates(row):
            item = dict(row)
            item["gate_direction"] = direction
            gate_rows.setdefault(gate, []).append(item)
    for gate in ALT_LONG_GATES:
        rows = gate_rows.get(gate, [])
        if rows:
            print_stat(gate, rows, lambda row: row["gate_direction"])
    watch_rows: dict[str, list[dict]] = {}
    for row in modeled:
        for gate, direction in alt_watch_gates(row):
            item = dict(row)
            item["gate_direction"] = direction
            watch_rows.setdefault(gate, []).append(item)
    print("")
    print("小山寨爆发候选池:")
    for gate in ALT_WATCH_GATES:
        rows = watch_rows.get(gate, [])
        if rows:
            print_stat(gate, rows, lambda row: row["gate_direction"])
    print("")
    print("小山寨下跌机会:")
    for gate in ALT_SHORT_GATES:
        rows = gate_rows.get(gate, [])
        if rows:
            print_stat(gate, rows, lambda row: row["gate_direction"])
    mainstream_rows = []
    for row in modeled:
        state = mainstream_state(row)
        direction = MAINSTREAM_STATE_DIRECTIONS.get(state, "neutral")
        if state != "neutral" and direction != "neutral":
            item = dict(row)
            item["mainstream_state"] = state
            item["mainstream_direction"] = direction
            mainstream_rows.append(item)
    print("")
    print("主流币市场状态:")
    print_stat("mainstream_direction", mainstream_rows, lambda row: row["mainstream_direction"])
    for state in ("trend_long", "top_risk"):
        rows = [row for row in mainstream_rows if row["mainstream_state"] == state]
        if rows:
            print_stat(f"mainstream_{state}", rows, lambda row: row["mainstream_direction"])


if __name__ == "__main__":
    main()
