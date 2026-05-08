import argparse
import csv
import datetime as dt
import statistics
from pathlib import Path

import alt_moonshot_coinglass as cg
import alt_moonshot_history as hist
import alt_moonshot_startup_analysis as startup


MAJOR_SYMBOLS = ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT")
PRE_WINDOWS_HOURS = (2, 4, 6, 12, 24)


def parse_time(value):
    parsed = dt.datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.UTC)
    return parsed.astimezone(dt.UTC)


def fetch_klines_5m(client, symbol, start, end):
    rows = []
    cursor = start
    while cursor < end:
        payload = client.get(
            hist.FAPI_BASE,
            "/fapi/v1/klines",
            {
                "symbol": symbol,
                "interval": "5m",
                "startTime": hist.ms(cursor),
                "endTime": hist.ms(end),
                "limit": 1500,
            },
        )
        if not isinstance(payload, list) or not payload:
            break
        rows.extend(payload)
        next_cursor = hist.parse_ms(payload[-1][0]) + dt.timedelta(minutes=5)
        if next_cursor <= cursor:
            break
        cursor = next_cursor
    seen = {}
    for row in rows:
        seen[int(row[0])] = row
    return [seen[key] for key in sorted(seen)]


def row_time(row):
    return hist.parse_ms(row[0])


def slice_rows(klines, start, end):
    return [row for row in klines if start <= row_time(row) < end]


def window_features(klines, start, end):
    rows = slice_rows(klines, start, end)
    if len(rows) < 2:
        return {}
    open_price = hist.safe_float(rows[0][1])
    close_price = hist.safe_float(rows[-1][4])
    highs = [hist.safe_float(row[2]) for row in rows]
    lows = [hist.safe_float(row[3]) for row in rows]
    high_price = max(highs)
    low_price = min(lows)
    quote_volume = sum(hist.safe_float(row[7], 0.0) or 0.0 for row in rows)
    taker_quote = sum(hist.safe_float(row[10], 0.0) or 0.0 for row in rows)
    sell_quote = max(0.0, quote_volume - taker_quote)
    amp_values = []
    for row in rows:
        high = hist.safe_float(row[2])
        low = hist.safe_float(row[3])
        if high and low:
            amp_values.append(hist.pct(low, high))
    return {
        "range": hist.pct(low_price, high_price),
        "close": hist.pct(open_price, close_price),
        "low_from_open": hist.pct(open_price, low_price),
        "close_from_low": hist.pct(low_price, close_price),
        "close_from_high": hist.pct(high_price, close_price),
        "avg_5m_amp": hist.avg(amp_values),
        "quote_volume": quote_volume,
        "taker_buy_sell": taker_quote / sell_quote if sell_quote > 0 else None,
    }


def fetch_oi_5m(client, symbol, start, end):
    try:
        payload = client.get(
            hist.FUTURES_DATA_BASE,
            "/openInterestHist",
            {
                "symbol": symbol,
                "period": "5m",
                "startTime": hist.ms(start),
                "endTime": hist.ms(end),
                "limit": 500,
            },
        )
    except Exception:
        return []
    if not isinstance(payload, list):
        return []
    rows = []
    for item in payload:
        timestamp = item.get("timestamp")
        value = hist.safe_float(item.get("sumOpenInterestValue"))
        if timestamp is None or value is None:
            value = hist.safe_float(item.get("sumOpenInterest"))
        if timestamp is None or value is None:
            continue
        rows.append({"time": hist.parse_ms(timestamp), "value": value})
    return sorted(rows, key=lambda row: row["time"])


def oi_change(oi_rows, start, end):
    rows = [row for row in oi_rows if start <= row["time"] < end and row.get("value")]
    if len(rows) < 2:
        return None
    return hist.pct(rows[0]["value"], rows[-1]["value"])


def post_high_return(klines, start, hours):
    rows = slice_rows(klines, start, start + dt.timedelta(hours=hours))
    if len(rows) < 2:
        return None
    entry = hist.safe_float(rows[0][1])
    high = max(hist.safe_float(row[2], 0.0) or 0.0 for row in rows)
    return hist.pct(entry, high)


def market_context(client, event_time):
    start = event_time - dt.timedelta(hours=30)
    end = event_time + dt.timedelta(hours=1)
    out = {}
    closes_24h = []
    for symbol in MAJOR_SYMBOLS:
        try:
            klines = fetch_klines_5m(client, symbol, start, end)
        except Exception:
            klines = []
        base = symbol.replace("USDT", "")
        for hours in (6, 12, 24):
            features = window_features(klines, event_time - dt.timedelta(hours=hours), event_time)
            value = features.get("close")
            out[f"{base.lower()}_close_{hours}h"] = value
            if hours == 24 and value is not None:
                closes_24h.append(value)
        out[f"{base.lower()}_range_24h"] = window_features(
            klines,
            event_time - dt.timedelta(hours=24),
            event_time,
        ).get("range")
    out["major_avg_close_24h"] = hist.avg(closes_24h)
    btc = hist.safe_float(out.get("btc_close_24h"), 0.0) or 0.0
    major = hist.safe_float(out.get("major_avg_close_24h"), 0.0) or 0.0
    if btc > 1.0 and major > 1.0:
        env = "risk_on"
    elif btc < -1.0 and major < -1.0:
        env = "risk_off"
    elif abs(btc) <= 1.0 and abs(major) <= 1.0:
        env = "flat_mixed"
    else:
        env = "mixed_rotation"
    out["market_env"] = env
    return out


def classify(row):
    pre2_range = hist.safe_float(row.get("pre2h_range"))
    pre6_range = hist.safe_float(row.get("pre6h_range"))
    pre12_range = hist.safe_float(row.get("pre12h_range"))
    pre6_close = hist.safe_float(row.get("pre6h_close"))
    pre12_close = hist.safe_float(row.get("pre12h_close"))
    pre24_close = hist.safe_float(row.get("pre24h_close"))
    pre6_taker = hist.safe_float(row.get("pre6h_taker_buy_sell"))
    market = row.get("market_env")
    major24 = hist.safe_float(row.get("major_avg_close_24h"))
    pre2_oi = hist.safe_float(row.get("pre2h_oi_change"))
    pre4_oi = hist.safe_float(row.get("pre4h_oi_change"))
    pre2_low = hist.safe_float(row.get("pre2h_low_from_open"))
    pre4_low = hist.safe_float(row.get("pre4h_low_from_open"))
    pre2_reclaim = hist.safe_float(row.get("pre2h_close_from_low"))
    pre4_reclaim = hist.safe_float(row.get("pre4h_close_from_low"))
    pre2_close = hist.safe_float(row.get("pre2h_close"))
    pre4_close = hist.safe_float(row.get("pre4h_close"))
    cg_long_liq_1h = hist.safe_float(row.get("cg_long_liq_1h_usd"), 0.0) or 0.0
    cg_short_liq_1h = hist.safe_float(row.get("cg_short_liq_1h_usd"), 0.0) or 0.0
    cg_long_liq_4h = hist.safe_float(row.get("cg_long_liq_4h_usd"), 0.0) or 0.0
    cg_short_liq_4h = hist.safe_float(row.get("cg_short_liq_4h_usd"), 0.0) or 0.0

    quiet = (
        pre2_range is not None
        and pre6_range is not None
        and pre12_range is not None
        and pre2_range <= 3.5
        and pre6_range <= 6.0
        and pre12_range <= 10.0
        and pre12_close is not None
        and -4.0 <= pre12_close <= 4.0
    )
    washout = (
        pre2_range is not None
        and pre2_range <= 8.0
        and (
            (pre6_close is not None and pre6_close <= -6.0)
            or (pre12_close is not None and pre12_close <= -8.0)
            or (pre24_close is not None and pre24_close <= -10.0)
        )
    )
    high_vol_reversal = (
        pre2_range is not None
        and pre6_range is not None
        and pre2_range > 8.0
        and pre2_range <= 18.0
        and pre6_range >= 12.0
        and (
            (pre6_close is not None and pre6_close <= -10.0)
            or (pre12_close is not None and pre12_close <= -12.0)
            or (pre24_close is not None and pre24_close <= -15.0)
        )
    )
    absorption = (
        pre6_taker is not None
        and pre6_taker >= 1.10
        and pre6_range is not None
        and pre6_range <= 8.0
        and pre6_close is not None
        and -4.0 <= pre6_close <= 4.0
    )
    relative_strength = (
        major24 is not None
        and major24 <= -1.0
        and pre12_close is not None
        and pre12_close >= -2.0
    )
    price_magnet_sweep = (
        (
            pre2_oi is not None
            and pre2_oi <= -1.5
            and pre2_low is not None
            and pre2_low <= -3.0
            and pre2_reclaim is not None
            and pre2_reclaim >= 2.0
        )
        or (
            pre4_oi is not None
            and pre4_oi <= -2.0
            and pre4_low is not None
            and pre4_low <= -5.0
            and pre4_reclaim is not None
            and pre4_reclaim >= 3.0
        )
    )
    short_cover_reclaim = (
        (
            pre2_oi is not None
            and pre2_oi <= -1.0
            and pre2_close is not None
            and pre2_close >= 2.0
        )
        or (
            pre4_oi is not None
            and pre4_oi <= -1.5
            and pre4_close is not None
            and pre4_close >= 3.0
        )
    )
    confirmed_long_liq_sweep = (
        price_magnet_sweep
        and (cg_long_liq_1h >= 1_000 or cg_long_liq_4h >= 3_000)
        and (cg_long_liq_1h >= cg_short_liq_1h * 1.5 or cg_long_liq_4h >= cg_short_liq_4h * 1.5)
    )
    confirmed_short_squeeze = (
        short_cover_reclaim
        and (cg_short_liq_1h >= 1_000 or cg_short_liq_4h >= 3_000)
        and (cg_short_liq_1h >= cg_long_liq_1h * 1.5 or cg_short_liq_4h >= cg_long_liq_4h * 1.5)
    )

    labels = []
    if quiet:
        labels.append("quiet_compression_base_v2")
    if washout:
        labels.append("washout_micro_base")
    if high_vol_reversal:
        labels.append("high_volatility_reversal_base")
    if absorption:
        labels.append("buyer_absorption_then_sweep")
    if relative_strength:
        labels.append("relative_strength_compression")
    if price_magnet_sweep:
        labels.append("price_magnet_liquidity_sweep")
    if short_cover_reclaim:
        labels.append("short_cover_reclaim")
    if confirmed_long_liq_sweep:
        labels.append("confirmed_long_liquidation_sweep")
    if confirmed_short_squeeze:
        labels.append("confirmed_short_squeeze_reclaim")
    if not labels:
        labels.append("unclassified")

    score = 0
    if quiet:
        score += 35
    if washout:
        score += 28
    if high_vol_reversal:
        score += 22
    if absorption:
        score += 18
    if relative_strength:
        score += 16
    if price_magnet_sweep:
        score += 12
    if short_cover_reclaim:
        score += 10
    if confirmed_long_liq_sweep:
        score += 12
    if confirmed_short_squeeze:
        score += 10
    if market == "risk_on":
        score += 6
    if pre2_range is not None and pre2_range <= 2.5:
        score += 5
    if pre6_range is not None and pre6_range <= 5.0:
        score += 5
    row["factor_labels"] = "|".join(labels)
    row["factor_score"] = score


def build_sample(client, item, days, coinglass_client=None):
    symbol = item["symbol"]
    low_time = parse_time(item["low_time"])
    end = hist.utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=days)
    klines_1h = hist.fetch_klines_1h(client, symbol, start, end)
    sample, _idx = startup.find_startup_row(klines_1h, item["low_time"], item["low"])
    if not sample:
        return None
    event_time = parse_time(sample["startup_time"])
    klines_5m = fetch_klines_5m(client, symbol, event_time - dt.timedelta(hours=30), event_time + dt.timedelta(hours=48))
    oi_5m = fetch_oi_5m(client, symbol, event_time - dt.timedelta(hours=30), event_time + dt.timedelta(hours=1))
    row = {
        "symbol": symbol,
        "startup_time": event_time.isoformat(),
        "cycle_low_time": low_time.isoformat(),
        "cycle_high_time": item.get("high_time"),
        "total_gain": item.get("gain"),
        "startup_price": sample.get("startup_price"),
        "gain_from_low": sample.get("gain_from_low"),
        "hours_from_low": sample.get("hours_from_low"),
        "price_change_24h_1h": sample.get("price_change_24h"),
        "from_24h_low_1h": sample.get("from_24h_low"),
        "volume_ratio_24h_1h": sample.get("volume_ratio_24h"),
    }
    for hours in PRE_WINDOWS_HOURS:
        window_start = event_time - dt.timedelta(hours=hours)
        features = window_features(klines_5m, event_time - dt.timedelta(hours=hours), event_time)
        for key, value in features.items():
            row[f"pre{hours}h_{key}"] = value
        row[f"pre{hours}h_oi_change"] = oi_change(oi_5m, window_start, event_time)
    for hours in (6, 24, 48):
        row[f"post{hours}h_high_ret"] = post_high_return(klines_5m, event_time, hours)
    row.update(market_context(client, event_time))
    if coinglass_client and coinglass_client.api_key:
        row.update(cg.context(coinglass_client, symbol, event_time, hours=4))
    classify(row)
    return row


def fmt(value, digits=2):
    value = hist.safe_float(value)
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def median(values):
    clean = [hist.safe_float(value) for value in values if hist.safe_float(value) is not None]
    return statistics.median(clean) if clean else None


def build_report(rows, args, client):
    lines = []
    lines.append("[MOONSHOT FACTOR EXTRACTOR]")
    lines.append(f"time={hist.utc_now().isoformat()} source={args.over100} rows={len(rows)}")
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append("definition: 100%+ cycle movers; startup is first 1h close +3%..+12% from cycle low; factor windows use 5m klines.")
    lines.append("")
    label_counts = {}
    market_counts = {}
    for row in rows:
        for label in (row.get("factor_labels") or "").split("|"):
            if label:
                label_counts[label] = label_counts.get(label, 0) + 1
        market = row.get("market_env") or "unknown"
        market_counts[market] = market_counts.get(market, 0) + 1
    lines.append("Factor label counts:")
    for label, count in sorted(label_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {label}: {count}")
    lines.append("")
    lines.append("Market environment counts:")
    for label, count in sorted(market_counts.items(), key=lambda item: (-item[1], item[0])):
        lines.append(f"- {label}: {count}")
    lines.append("")
    lines.append("Key medians:")
    for key in (
        "pre2h_range",
        "pre6h_range",
        "pre12h_range",
        "pre12h_close",
        "pre6h_taker_buy_sell",
        "pre2h_oi_change",
        "pre4h_oi_change",
        "cg_long_liq_1h_usd",
        "cg_short_liq_1h_usd",
        "cg_oi_change_1h",
        "cg_taker_buy_sell_1h",
        "major_avg_close_24h",
        "post24h_high_ret",
    ):
        lines.append(f"- {key}: median={fmt(median(row.get(key) for row in rows))}")
    lines.append("")
    lines.append("Top samples:")
    for row in sorted(rows, key=lambda item: hist.safe_float(item.get("total_gain"), 0.0) or 0.0, reverse=True)[:50]:
        lines.append(
            f"- {row['symbol']} total={fmt(row.get('total_gain'))}% startup={row['startup_time']} "
            f"labels={row.get('factor_labels')} score={row.get('factor_score')} "
            f"mkt={row.get('market_env')} major24={fmt(row.get('major_avg_close_24h'))}% "
            f"pre2r={fmt(row.get('pre2h_range'))}% pre6r={fmt(row.get('pre6h_range'))}% "
            f"pre12c={fmt(row.get('pre12h_close'))}% oi2={fmt(row.get('pre2h_oi_change'))}% "
            f"liqL1h={fmt(row.get('cg_long_liq_1h_usd'), 0)} liqS1h={fmt(row.get('cg_short_liq_1h_usd'), 0)} "
            f"taker6={fmt(row.get('pre6h_taker_buy_sell'), 3)} "
            f"post24={fmt(row.get('post24h_high_ret'))}%"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Extract reusable startup factors from 100%+ moonshot samples.")
    parser.add_argument("--over100", default="reports/alt_moonshots/over_100pct_60d.txt")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--coinglass-cache-dir", default=".cache/coinglass")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--no-network", action="store_true")
    args = parser.parse_args()

    source = startup.parse_over100(args.over100)
    if args.max_symbols > 0:
        source = source[: args.max_symbols]
    client = hist.BinanceCache(args.cache_dir, sleep_seconds=args.sleep, no_network=args.no_network)
    coinglass_client = cg.CoinGlassClient(args.coinglass_cache_dir, sleep_seconds=args.sleep, no_network=args.no_network)
    rows = []
    for idx, item in enumerate(source, start=1):
        try:
            row = build_sample(client, item, args.days, coinglass_client)
        except Exception:
            row = None
        if row:
            rows.append(row)
        if idx % 20 == 0:
            print(f"factor_samples={idx}/{len(source)} rows={len(rows)} requests={client.requests}", flush=True)

    out_dir = Path(args.out_dir)
    sample_suffix = f"_sample{args.max_symbols}" if args.max_symbols > 0 else ""
    csv_path = out_dir / f"moonshot_factor_samples_{args.days}d{sample_suffix}.csv"
    txt_path = out_dir / f"moonshot_factor_samples_{args.days}d{sample_suffix}.txt"
    latest_path = out_dir / "latest_moonshot_factor_samples.txt"
    hist.write_csv(csv_path, rows)
    report = build_report(rows, args, client)
    txt_path.write_text(report, encoding="utf-8")
    if args.max_symbols <= 0:
        latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
