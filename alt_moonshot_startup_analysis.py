import argparse
import csv
import datetime as dt
import re
import statistics
from pathlib import Path

import alt_moonshot_history as hist
import alt_moonshot_elastic_report as elastic


START_GAIN_MIN = 3.0
START_GAIN_MAX = 12.0
CONTEXT_WINDOWS = {
    "1h": ("5m", 13),
    "4h": ("15m", 17),
    "24h": ("1h", 25),
}


def median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def parse_over100(path):
    rows = []
    line_re = re.compile(
        r"^(?P<symbol>[^,]+),(?P<gain>[-0-9.]+)%,low_time=(?P<low_time>[^,]+),"
        r"high_time=(?P<high_time>[^,]+),low=(?P<low>[^,]+),high=(?P<high>[^,]+)$"
    )
    for line in Path(path).read_text(encoding="utf-8").splitlines()[1:]:
        match = line_re.match(line.strip())
        if not match:
            continue
        item = match.groupdict()
        item["gain"] = hist.safe_float(item["gain"])
        item["low"] = hist.safe_float(item["low"])
        item["high"] = hist.safe_float(item["high"])
        rows.append(item)
    return rows


def find_startup_row(klines, low_time_iso, low_price):
    low_time = dt.datetime.fromisoformat(low_time_iso)
    if low_time.tzinfo is None:
        low_time = low_time.replace(tzinfo=dt.UTC)
    low_ms = hist.ms(low_time)
    low_idx = None
    for idx, row in enumerate(klines):
        if int(row[0]) >= low_ms:
            low_idx = idx
            break
    if low_idx is None:
        return None, None
    upper = min(len(klines) - 49, low_idx + 24 * 7)
    if upper <= low_idx:
        return None, None
    selected_idx = None
    for idx in range(low_idx, upper):
        close = hist.safe_float(klines[idx][4])
        if not close or not low_price:
            continue
        gain_from_low = hist.pct(low_price, close)
        if gain_from_low is not None and START_GAIN_MIN <= gain_from_low <= START_GAIN_MAX:
            selected_idx = idx
            break
    if selected_idx is None:
        for idx in range(low_idx, upper):
            close = hist.safe_float(klines[idx][4])
            gain_from_low = hist.pct(low_price, close)
            if gain_from_low is not None and gain_from_low >= START_GAIN_MIN:
                selected_idx = idx
                break
    if selected_idx is None:
        selected_idx = low_idx

    idx = selected_idx
    close = hist.safe_float(klines[idx][4])
    if not close:
        return None, None
    prev_idx = max(0, idx - 24)
    price_change_24h = hist.pct(hist.safe_float(klines[prev_idx][4]), close)
    prev_24h_low = min(hist.safe_float(row[3], close) or close for row in klines[prev_idx : idx + 1])
    from_24h_low = hist.pct(prev_24h_low, close)
    quote_now = hist.safe_float(klines[idx][7], 0.0) or 0.0
    prev_quote_avg = hist.avg([hist.safe_float(row[7]) for row in klines[prev_idx:idx]]) or 0.0
    volume_ratio_24h = quote_now / prev_quote_avg if prev_quote_avg > 0 else None
    future = klines[idx + 1 : min(len(klines), idx + 49)]
    high_24 = max(hist.safe_float(row[2], close) or close for row in klines[idx + 1 : min(len(klines), idx + 25)] or [klines[idx]])
    high_48 = max(hist.safe_float(row[2], close) or close for row in future or [klines[idx]])
    row = {
        "symbol": "",
        "startup_time": hist.parse_ms(klines[idx][0]).isoformat(),
        "startup_price": close,
        "gain_from_low": hist.pct(low_price, close),
        "hours_from_low": (int(klines[idx][0]) - low_ms) / 3_600_000,
        "price_change_24h": price_change_24h,
        "from_24h_low": from_24h_low,
        "quote_volume_1h": quote_now,
        "volume_ratio_24h": volume_ratio_24h,
        "future_mfe_24h_from_startup": hist.pct(close, high_24),
        "future_mfe_48h_from_startup": hist.pct(close, high_48),
    }
    return row, idx


def enrich_contract(client, row):
    symbol = row["symbol"]
    end_time = dt.datetime.fromisoformat(row["startup_time"])
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=dt.UTC)
    for label, (period, limit) in CONTEXT_WINDOWS.items():
        oi_rows = hist.rows_until(client, "openInterestHist", symbol, period, end_time, limit)
        row[f"oi_change_{label}"] = (
            hist.pct(oi_rows[0].get("sumOpenInterestValue"), oi_rows[-1].get("sumOpenInterestValue"))
            if len(oi_rows) >= 2
            else None
        )
        row[f"global_ls_{label}"] = hist.latest_avg_ratio(client, "globalLongShortAccountRatio", "longShortRatio", symbol, end_time, period, limit)
        row[f"top_position_ls_{label}"] = hist.latest_avg_ratio(client, "topLongShortPositionRatio", "longShortRatio", symbol, end_time, period, limit)
        row[f"top_account_ls_{label}"] = hist.latest_avg_ratio(client, "topLongShortAccountRatio", "longShortRatio", symbol, end_time, period, limit)
        taker_rows = hist.rows_until(client, "takerlongshortRatio", symbol, period, end_time, limit)
        buy = sum(hist.safe_float(item.get("buyVol"), 0.0) or 0.0 for item in taker_rows)
        sell = sum(hist.safe_float(item.get("sellVol"), 0.0) or 0.0 for item in taker_rows)
        row[f"taker_buy_sell_{label}"] = buy / sell if sell > 0 else None
    try:
        funding_rows = client.get(hist.FAPI_BASE, "/fapi/v1/fundingRate", {"symbol": symbol, "endTime": hist.ms(end_time), "limit": 8})
    except Exception:
        funding_rows = []
    funding_values = [hist.safe_float(item.get("fundingRate")) * 100 for item in funding_rows if hist.safe_float(item.get("fundingRate")) is not None]
    row["funding_latest"] = funding_values[-1] if funding_values else None
    row["funding_avg_24h"] = hist.avg(funding_values[-3:]) if funding_values else None
    return row


def fmt(value, digits=2):
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def bucket_counts(rows, key, buckets):
    counts = []
    for label, fn in buckets:
        counts.append((label, sum(1 for row in rows if fn(hist.safe_float(row.get(key))))))
    return counts


def build_report(rows, source_count, client):
    lines = []
    lines.append("[ALT MOONSHOT STARTUP ANALYSIS]")
    lines.append(f"over100_symbols={source_count} startup_samples={len(rows)}")
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append(f"startup definition: first 1h close after cycle low with +{START_GAIN_MIN:.0f}%..+{START_GAIN_MAX:.0f}% from low, fallback first +{START_GAIN_MIN:.0f}%.")
    lines.append("")
    keys = [
        "total_gain_60d",
        "gain_from_low",
        "hours_from_low",
        "price_change_24h",
        "from_24h_low",
        "volume_ratio_24h",
        "hist_max_24h_range",
        "hist_avg_24h_range",
        "hist_quote_median_24h",
        "oi_change_1h",
        "oi_change_4h",
        "oi_change_24h",
        "taker_buy_sell_1h",
        "taker_buy_sell_4h",
        "top_position_ls_4h",
        "top_account_ls_4h",
        "funding_latest",
        "future_mfe_24h_from_startup",
        "future_mfe_48h_from_startup",
    ]
    lines.append("Median / Avg:")
    for key in keys:
        values = [hist.safe_float(row.get(key)) for row in rows]
        present = len([value for value in values if value is not None])
        avg = hist.avg(values)
        med = median(values)
        if key == "hist_quote_median_24h":
            lines.append(f"- {key}: median={fmt((med or 0) / 1_000_000)}m avg={fmt((avg or 0) / 1_000_000)}m present={present}/{len(rows)}")
        else:
            lines.append(f"- {key}: median={fmt(med)} avg={fmt(avg)} present={present}/{len(rows)}")
    lines.append("")
    lines.append("Key buckets:")
    for key, buckets in (
        (
            "price_change_24h",
            [
                ("p24<-5", lambda v: v is not None and v < -5),
                ("-5..0", lambda v: v is not None and -5 <= v < 0),
                ("0..5", lambda v: v is not None and 0 <= v <= 5),
                (">5", lambda v: v is not None and v > 5),
            ],
        ),
        (
            "volume_ratio_24h",
            [
                ("<1", lambda v: v is not None and v < 1),
                ("1..2", lambda v: v is not None and 1 <= v <= 2),
                ("2..5", lambda v: v is not None and 2 < v <= 5),
                (">5", lambda v: v is not None and v > 5),
            ],
        ),
        (
            "oi_change_4h",
            [
                ("<0", lambda v: v is not None and v < 0),
                ("0..1", lambda v: v is not None and 0 <= v < 1),
                ("1..5", lambda v: v is not None and 1 <= v <= 5),
                (">5", lambda v: v is not None and v > 5),
            ],
        ),
        (
            "taker_buy_sell_4h",
            [
                ("<1", lambda v: v is not None and v < 1),
                ("1..1.1", lambda v: v is not None and 1 <= v < 1.1),
                ("1.1..1.3", lambda v: v is not None and 1.1 <= v <= 1.3),
                (">1.3", lambda v: v is not None and v > 1.3),
            ],
        ),
    ):
        counts = bucket_counts(rows, key, buckets)
        lines.append(f"- {key}: " + " | ".join(f"{label}={count}" for label, count in counts))
    lines.append("")
    lines.append("Top startup samples by total gain:")
    for row in sorted(rows, key=lambda item: hist.safe_float(item.get("total_gain_60d"), 0.0) or 0.0, reverse=True)[:40]:
        lines.append(
            f"- {row['symbol']} total={fmt(hist.safe_float(row.get('total_gain_60d')))}% "
            f"startup={row['startup_time']} low+{fmt(hist.safe_float(row.get('gain_from_low')))}% "
            f"p24={fmt(hist.safe_float(row.get('price_change_24h')))}% volr={fmt(hist.safe_float(row.get('volume_ratio_24h')))} "
            f"oi4h={fmt(hist.safe_float(row.get('oi_change_4h')))}% taker4h={fmt(hist.safe_float(row.get('taker_buy_sell_4h')), 3)} "
            f"topPos4h={fmt(hist.safe_float(row.get('top_position_ls_4h')), 3)} fund={fmt(hist.safe_float(row.get('funding_latest')), 4)}% "
            f"MFE24={fmt(hist.safe_float(row.get('future_mfe_24h_from_startup')))}% MFE48={fmt(hist.safe_float(row.get('future_mfe_48h_from_startup')))}%"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Analyze startup features for 100%+ alt movers.")
    parser.add_argument("--over100", default="reports/alt_moonshots/over_100pct_60d.txt")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--sleep", type=float, default=0.05)
    parser.add_argument("--max-symbols", type=int, default=0)
    args = parser.parse_args()

    source = parse_over100(args.over100)
    if args.max_symbols > 0:
        source = source[: args.max_symbols]
    client = hist.BinanceCache(args.cache_dir, sleep_seconds=args.sleep)
    end = hist.utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=args.days)
    rows = []
    for idx, item in enumerate(source, start=1):
        symbol = item["symbol"]
        try:
            klines = hist.fetch_klines_1h(client, symbol, start, end)
        except Exception:
            continue
        startup, _startup_idx = find_startup_row(klines, item["low_time"], item["low"])
        features = elastic.symbol_features(klines)
        if not startup or not features:
            continue
        startup["symbol"] = symbol
        startup["total_gain_60d"] = item["gain"]
        startup["cycle_low_time"] = item["low_time"]
        startup["cycle_high_time"] = item["high_time"]
        startup["cycle_low"] = item["low"]
        startup["cycle_high"] = item["high"]
        startup.update(features)
        enrich_contract(client, startup)
        rows.append(startup)
        if idx % 20 == 0:
            print(f"analyzed={idx}/{len(source)} rows={len(rows)} requests={client.requests}", flush=True)

    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"over100_startup_features_{args.days}d.csv"
    txt_path = out_dir / f"over100_startup_features_{args.days}d.txt"
    latest_path = out_dir / "latest_over100_startup_features.txt"
    hist.write_csv(csv_path, rows)
    report = build_report(rows, len(source), client)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
