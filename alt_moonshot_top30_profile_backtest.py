import argparse
import csv
import datetime as dt
import statistics
from pathlib import Path

import alt_moonshot_history as hist
import alt_moonshot_elastic_report as elastic
from alt_moonshot_validate import build_prefilter_rows, stat_line


def median(values):
    values = [value for value in values if value is not None]
    return statistics.median(values) if values else None


def parse_float(row, key):
    return hist.safe_float(row.get(key))


def load_startup_rows(path):
    rows = list(csv.DictReader(Path(path).open(encoding="utf-8")))
    rows.sort(key=lambda row: parse_float(row, "total_gain_60d") or 0.0, reverse=True)
    return rows


def top30_profile(row):
    price_change = parse_float(row, "price_change_24h")
    from_low = parse_float(row, "from_24h_low")
    volume_ratio = parse_float(row, "volume_ratio_24h")
    hist_max = parse_float(row, "hist_max_24h_range")
    hist_avg = parse_float(row, "hist_avg_24h_range")
    quote = parse_float(row, "hist_quote_median_24h")
    if None in (price_change, from_low, volume_ratio, hist_max, hist_avg, quote):
        return []
    labels = []
    if (
        -25 <= price_change <= 5
        and 3 <= from_low <= 12
        and 1 <= volume_ratio <= 5
        and hist_max >= 50
        and hist_avg >= 10
        and 2_000_000 <= quote <= 50_000_000
    ):
        labels.append("top30_profile_core")
    if (
        -25 <= price_change <= 5
        and 3 <= from_low <= 15
        and 0.8 <= volume_ratio <= 8
        and hist_max >= 50
        and hist_avg >= 10
        and 1_000_000 <= quote <= 80_000_000
    ):
        labels.append("top30_profile_wide")
    if (
        -25 <= price_change <= 3
        and 3 <= from_low <= 10
        and 1 <= volume_ratio <= 5
        and hist_max >= 100
        and hist_avg >= 15
        and 2_000_000 <= quote <= 50_000_000
    ):
        labels.append("top30_profile_strict")
    return labels


def startup_coverage_report(rows, top30_symbols):
    other = [row for row in rows if row["symbol"] not in top30_symbols]
    groups = {"remaining_over100": other}
    for row in other:
        for label in top30_profile(row):
            groups.setdefault(label, []).append(row)
    lines = []
    lines.append("Coverage On Remaining 100%+ Coins:")
    for label, group in groups.items():
        gains = [parse_float(row, "total_gain_60d") for row in group]
        mfe24 = [parse_float(row, "future_mfe_24h_from_startup") for row in group]
        mfe48 = [parse_float(row, "future_mfe_48h_from_startup") for row in group]
        lines.append(
            f"- {label}: n={len(group)} symbols={len(set(row['symbol'] for row in group))} "
            f"total_gain_med={hist.safe_float(median(gains), 0):.2f}% "
            f"MFE24_med={hist.safe_float(median(mfe24), 0):.2f}% MFE48_med={hist.safe_float(median(mfe48), 0):.2f}%"
        )
    return lines


def build_market_rows(client, days, stride_hours, cooldown_hours):
    end = hist.utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=days)
    rows = []
    symbols = hist.futures_symbols(client)
    for idx, symbol in enumerate(symbols, start=1):
        try:
            klines = hist.fetch_klines_1h(client, symbol, start, end)
        except Exception:
            continue
        features = elastic.symbol_features(klines)
        if not features:
            continue
        for row in build_prefilter_rows(symbol, klines, stride_hours, cooldown_hours):
            row.update(features)
            row["profile_labels"] = "|".join(top30_profile(row))
            rows.append(row)
        if idx % 100 == 0:
            print(f"scanned={idx}/{len(symbols)} market_rows={len(rows)} requests={client.requests}", flush=True)
    return rows, len(symbols)


def build_report(startup_rows, market_rows, top30_symbols, symbols_scanned, client, args):
    lines = []
    lines.append("[TOP30 MOONSHOT PROFILE BACKTEST]")
    lines.append(f"days={args.days} top30_symbols={len(top30_symbols)} symbols_scanned={symbols_scanned} market_rows={len(market_rows)}")
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append("")
    lines.append("Top30 Profile:")
    lines.append("- price_change_24h: -25%..+5%")
    lines.append("- from_24h_low: core 3%..12%, wide 3%..15%")
    lines.append("- volume_ratio_24h: core 1x..5x, wide 0.8x..8x")
    lines.append("- hist_max_24h_range >=50%, hist_avg_24h_range >=10%")
    lines.append("- hist_quote_median_24h: core $2m..$50m")
    lines.append("")
    lines.extend(startup_coverage_report(startup_rows, top30_symbols))
    lines.append("")
    remaining_market = [row for row in market_rows if row["symbol"] not in top30_symbols]
    lines.append("Backtest On Market Rows Excluding Top30 Symbols:")
    lines.append(stat_line("remaining_market_base", remaining_market))
    for label in ("top30_profile_core", "top30_profile_wide", "top30_profile_strict"):
        group = [row for row in remaining_market if label in row.get("profile_labels", "").split("|")]
        lines.append(stat_line(label, group))
    lines.append("")
    lines.append("Top Hits Excluding Top30 Symbols By MFE48:")
    hits = [row for row in remaining_market if row.get("profile_labels")]
    for row in sorted(hits, key=lambda item: hist.safe_float(item.get("future_mfe_48h"), 0.0) or 0.0, reverse=True)[:40]:
        quote = hist.safe_float(row.get("hist_quote_median_24h"), 0.0) or 0.0
        lines.append(
            f"- {row['symbol']} {row['event_time']} labels={row.get('profile_labels')} "
            f"p24={hist.safe_float(row.get('price_change_24h')):+.2f}% "
            f"fromLow={hist.safe_float(row.get('from_24h_low')):.2f}% "
            f"volr={hist.safe_float(row.get('volume_ratio_24h')):.2f} "
            f"histMax={hist.safe_float(row.get('hist_max_24h_range')):.1f}% "
            f"histAvg={hist.safe_float(row.get('hist_avg_24h_range')):.1f}% "
            f"qMed={quote / 1_000_000:.1f}m "
            f"ret24={hist.safe_float(row.get('future_return_24h')):+.2f}% "
            f"MFE24={hist.safe_float(row.get('future_mfe_24h')):+.2f}% "
            f"MFE48={hist.safe_float(row.get('future_mfe_48h')):+.2f}%"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Backtest the top-30 moonshot startup profile after excluding top-30 symbols.")
    parser.add_argument("--startup-csv", default="reports/alt_moonshots/over100_startup_features_60d.csv")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--stride-hours", type=int, default=4)
    parser.add_argument("--cooldown-hours", type=int, default=12)
    parser.add_argument("--sleep", type=float, default=0.03)
    args = parser.parse_args()

    startup_rows = load_startup_rows(args.startup_csv)
    top30 = startup_rows[:30]
    top30_symbols = {row["symbol"] for row in top30}
    client = hist.BinanceCache(args.cache_dir, sleep_seconds=args.sleep)
    market_rows, symbols_scanned = build_market_rows(client, args.days, args.stride_hours, args.cooldown_hours)
    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"top30_profile_market_backtest_{args.days}d.csv"
    txt_path = out_dir / f"top30_profile_market_backtest_{args.days}d.txt"
    latest_path = out_dir / "latest_top30_profile_market_backtest.txt"
    hist.write_csv(csv_path, market_rows)
    report = build_report(startup_rows, market_rows, top30_symbols, symbols_scanned, client, args)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
