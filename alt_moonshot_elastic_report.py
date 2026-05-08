import argparse
import csv
import datetime as dt
import statistics
from pathlib import Path

import alt_moonshot_history as hist
from alt_moonshot_validate import build_prefilter_rows, stat_line


def median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def symbol_features(klines):
    if len(klines) < 72:
        return None
    day_ranges = []
    day_quotes = []
    lows = []
    highs = []
    for row in klines:
        low = hist.safe_float(row[3])
        high = hist.safe_float(row[2])
        if low is not None:
            lows.append(low)
        if high is not None:
            highs.append(high)
    for idx in range(24, len(klines), 24):
        chunk = klines[max(0, idx - 24) : idx]
        if len(chunk) < 12:
            continue
        low = min(hist.safe_float(row[3]) for row in chunk if hist.safe_float(row[3]) is not None)
        high = max(hist.safe_float(row[2]) for row in chunk if hist.safe_float(row[2]) is not None)
        day_ranges.append(hist.pct(low, high))
        day_quotes.append(sum(hist.safe_float(row[7], 0.0) or 0.0 for row in chunk))
    max_range = max([value for value in day_ranges if value is not None] or [0.0])
    avg_range = hist.avg(day_ranges) or 0.0
    return {
        "hist_max_24h_range": max_range,
        "hist_avg_24h_range": avg_range,
        "hist_boom_days": sum(1 for value in day_ranges if value is not None and value >= 30),
        "hist_quote_median_24h": median(day_quotes) or 0.0,
        "hist_total_range_60d": hist.pct(min(lows), max(highs)) if lows and highs else 0.0,
        "listed_hours": len(klines),
    }


def lowbase_shape(row):
    price_change = hist.safe_float(row.get("price_change_24h"))
    from_low = hist.safe_float(row.get("from_24h_low"))
    volume_ratio = hist.safe_float(row.get("volume_ratio_24h"), 0.0) or 0.0
    return (
        price_change is not None
        and from_low is not None
        and -8 <= price_change <= 5
        and from_low < 10
        and 1 <= volume_ratio <= 3
    )


def elastic_filters(row):
    max_range = hist.safe_float(row.get("hist_max_24h_range"), 0.0) or 0.0
    avg_range = hist.safe_float(row.get("hist_avg_24h_range"), 0.0) or 0.0
    quote = hist.safe_float(row.get("hist_quote_median_24h"), 0.0) or 0.0
    boom_days = hist.safe_float(row.get("hist_boom_days"), 0.0) or 0.0
    total_range = hist.safe_float(row.get("hist_total_range_60d"), 0.0) or 0.0
    filters = []
    if max_range >= 30 and quote < 50_000_000:
        filters.append("elastic_basic")
    if max_range >= 30 and 2_000_000 <= quote <= 50_000_000:
        filters.append("elastic_liquid")
    if max_range >= 30 and avg_range >= 8 and 2_000_000 <= quote <= 50_000_000:
        filters.append("elastic_active")
    if max_range >= 50 and avg_range >= 8 and 2_000_000 <= quote <= 50_000_000:
        filters.append("elastic_very")
    if boom_days >= 2 and 2_000_000 <= quote <= 50_000_000:
        filters.append("boom_days_2")
    if total_range >= 100 and 2_000_000 <= quote <= 50_000_000:
        filters.append("range60_100")
    return filters


def build_report(rows, args, client):
    lines = []
    lines.append("[ALT MOONSHOT ELASTIC REPORT]")
    lines.append(f"range_days={args.days} symbols={args.symbols_scanned} lowbase_rows={len(rows)}")
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append("lowbase: 24h price -8%..+5%, from 24h low <10%, 1h volume ratio 1..3.")
    lines.append("")
    lines.append(stat_line("lowbase_all", rows))
    groups = {}
    for row in rows:
        for label in row.get("elastic_filters", "").split("|"):
            if label:
                groups.setdefault(label, []).append(row)
    for label in ("elastic_basic", "elastic_liquid", "elastic_active", "elastic_very", "boom_days_2", "range60_100"):
        lines.append(stat_line(label, groups.get(label, [])))
    lines.append("")
    lines.append("elastic_active Top MFE48:")
    top_rows = sorted(groups.get("elastic_active", []), key=lambda row: hist.safe_float(row.get("future_mfe_48h"), 0.0) or 0.0, reverse=True)[:30]
    for row in top_rows:
        lines.append(
            f"- {row['symbol']} {row['event_time']} "
            f"p24={hist.safe_float(row.get('price_change_24h')):+.2f}% volr={hist.safe_float(row.get('volume_ratio_24h')):.2f} "
            f"ret24={hist.safe_float(row.get('future_return_24h')):+.2f}% "
            f"MFE24={hist.safe_float(row.get('future_mfe_24h')):+.2f}% MFE48={hist.safe_float(row.get('future_mfe_48h')):+.2f}% "
            f"histMax24={hist.safe_float(row.get('hist_max_24h_range')):.1f}% histAvg24={hist.safe_float(row.get('hist_avg_24h_range')):.1f}% "
            f"qMed24={hist.safe_float(row.get('hist_quote_median_24h')) / 1_000_000:.1f}m"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Analyze elastic alt filters over lowbase candidates.")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--stride-hours", type=int, default=4)
    parser.add_argument("--cooldown-hours", type=int, default=12)
    parser.add_argument("--no-network", action="store_true")
    args = parser.parse_args()

    client = hist.BinanceCache(args.cache_dir, no_network=args.no_network)
    end = hist.utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=args.days)
    rows = []
    symbols = hist.futures_symbols(client)
    args.symbols_scanned = len(symbols)
    for idx, symbol in enumerate(symbols, start=1):
        try:
            klines = hist.fetch_klines_1h(client, symbol, start, end)
        except Exception:
            continue
        features = symbol_features(klines)
        if not features:
            continue
        for row in build_prefilter_rows(symbol, klines, args.stride_hours, args.cooldown_hours):
            if not lowbase_shape(row):
                continue
            row.update(features)
            row["elastic_filters"] = "|".join(elastic_filters(row))
            rows.append(row)
        if idx % 100 == 0:
            print(f"scanned={idx}/{len(symbols)} lowbase_rows={len(rows)} requests={client.requests}", flush=True)

    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"alt_moonshot_elastic_{args.days}d.csv"
    txt_path = out_dir / f"alt_moonshot_elastic_{args.days}d.txt"
    latest_path = out_dir / "latest_alt_moonshot_elastic.txt"
    hist.write_csv(csv_path, rows)
    report = build_report(rows, args, client)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
