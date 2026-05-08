import argparse
import datetime as dt
import statistics
from pathlib import Path

import alt_moonshot_history as hist


VALIDATION_WINDOWS = {
    "1h": ("5m", 13),
    "4h": ("15m", 17),
}


def quantile(values, q):
    values = sorted(value for value in values if value is not None)
    if not values:
        return None
    idx = min(len(values) - 1, max(0, int((len(values) - 1) * q)))
    return values[idx]


def fmt(value, digits=2):
    if value is None:
        return "NA"
    return f"{value:.{digits}f}"


def outcome_from_klines(klines, index):
    close = hist.safe_float(klines[index][4])
    if not close or close <= 0:
        return None
    future_24 = klines[index + 1 : min(len(klines), index + 25)]
    future_48 = klines[index + 1 : min(len(klines), index + 49)]
    if not future_24:
        return None
    high_24 = max(hist.safe_float(row[2], close) or close for row in future_24)
    low_24 = min(hist.safe_float(row[3], close) or close for row in future_24)
    high_48 = max(hist.safe_float(row[2], close) or close for row in future_48) if future_48 else high_24
    low_48 = min(hist.safe_float(row[3], close) or close for row in future_48) if future_48 else low_24
    close_24 = hist.safe_float(future_24[-1][4], close) or close
    close_48 = hist.safe_float(future_48[-1][4], close) or close if future_48 else close_24
    return {
        "future_return_24h": hist.pct(close, close_24),
        "future_return_48h": hist.pct(close, close_48),
        "future_mfe_24h": hist.pct(close, high_24),
        "future_mfe_48h": hist.pct(close, high_48),
        "future_mae_24h": hist.pct(close, low_24),
        "future_mae_48h": hist.pct(close, low_48),
    }


def build_prefilter_rows(symbol, klines, stride_hours, cooldown_hours):
    rows = []
    last_selected = -10_000
    step = max(1, int(stride_hours))
    cooldown = max(1, int(cooldown_hours))
    for i in range(24, max(24, len(klines) - 49), step):
        if i - last_selected < cooldown:
            continue
        close = hist.safe_float(klines[i][4])
        if not close or close <= 0:
            continue
        price_change_24h = hist.pct(hist.safe_float(klines[i - 24][4]), close)
        prev_24h_low = min(hist.safe_float(row[3], close) or close for row in klines[i - 24 : i + 1])
        from_24h_low = hist.pct(prev_24h_low, close)
        quote_now = hist.safe_float(klines[i][7], 0.0) or 0.0
        prev_quote_avg = hist.avg([hist.safe_float(row[7]) for row in klines[i - 24 : i]]) or 0.0
        volume_ratio_24h = quote_now / prev_quote_avg if prev_quote_avg > 0 else None
        if price_change_24h is None or from_24h_low is None:
            continue
        quiet = -15 <= price_change_24h <= 20 and from_24h_low <= 25
        active = volume_ratio_24h is not None and volume_ratio_24h >= 0.8
        if not quiet and not active:
            continue
        outcome = outcome_from_klines(klines, i)
        if not outcome:
            continue
        row = {
            "symbol": symbol,
            "event_time": hist.parse_ms(klines[i][0]).isoformat(),
            "entry_price": close,
            "price_change_24h": price_change_24h,
            "from_24h_low": from_24h_low,
            "quote_volume_1h": quote_now,
            "volume_ratio_24h": volume_ratio_24h,
            **outcome,
        }
        rows.append(row)
        last_selected = i
    return rows


def enrich_validation_context(client, item):
    symbol = item["symbol"]
    end_time = dt.datetime.fromisoformat(item["event_time"])
    if end_time.tzinfo is None:
        end_time = end_time.replace(tzinfo=dt.UTC)
    for label, (period, limit) in VALIDATION_WINDOWS.items():
        oi_rows = hist.rows_until(client, "openInterestHist", symbol, period, end_time, limit)
        if len(oi_rows) >= 2:
            item[f"oi_change_{label}"] = hist.pct(oi_rows[0].get("sumOpenInterestValue"), oi_rows[-1].get("sumOpenInterestValue"))
        else:
            item[f"oi_change_{label}"] = None
        item[f"global_ls_{label}"] = hist.latest_avg_ratio(client, "globalLongShortAccountRatio", "longShortRatio", symbol, end_time, period, limit)
        item[f"top_position_ls_{label}"] = hist.latest_avg_ratio(client, "topLongShortPositionRatio", "longShortRatio", symbol, end_time, period, limit)
        item[f"top_account_ls_{label}"] = hist.latest_avg_ratio(client, "topLongShortAccountRatio", "longShortRatio", symbol, end_time, period, limit)
        taker_rows = hist.rows_until(client, "takerlongshortRatio", symbol, period, end_time, limit)
        buy = sum(hist.safe_float(row.get("buyVol"), 0.0) or 0.0 for row in taker_rows)
        sell = sum(hist.safe_float(row.get("sellVol"), 0.0) or 0.0 for row in taker_rows)
        item[f"taker_buy_sell_{label}"] = buy / sell if sell > 0 else None
    try:
        funding_rows = client.get(
            hist.FAPI_BASE,
            "/fapi/v1/fundingRate",
            {"symbol": symbol, "endTime": hist.ms(end_time), "limit": 8},
        )
    except Exception:
        funding_rows = []
    funding_values = [hist.safe_float(row.get("fundingRate")) * 100 for row in funding_rows if hist.safe_float(row.get("fundingRate")) is not None]
    item["funding_latest"] = funding_values[-1] if funding_values else None
    item["funding_avg_24h"] = hist.avg(funding_values[-3:]) if funding_values else None
    return item


def funding_clean(row):
    funding = hist.safe_float(row.get("funding_latest"))
    return funding is not None and -0.08 <= funding <= 0.05


def rule_hits(row):
    price_change = hist.safe_float(row.get("price_change_24h"))
    from_low = hist.safe_float(row.get("from_24h_low"))
    volume_ratio = hist.safe_float(row.get("volume_ratio_24h"), 0.0) or 0.0
    oi_1h = hist.safe_float(row.get("oi_change_1h"))
    oi_4h = hist.safe_float(row.get("oi_change_4h"))
    taker_1h = hist.safe_float(row.get("taker_buy_sell_1h"))
    taker_4h = hist.safe_float(row.get("taker_buy_sell_4h"))
    top_position_1h = hist.safe_float(row.get("top_position_ls_1h"))
    top_account_1h = hist.safe_float(row.get("top_account_ls_1h"))
    top_position_4h = hist.safe_float(row.get("top_position_ls_4h"))
    rules = []
    if oi_1h is not None and taker_1h is not None and oi_1h > 0 and taker_1h >= 1.05 and funding_clean(row):
        rules.append("contract_long_a")
    if (
        oi_1h is not None
        and taker_1h is not None
        and price_change is not None
        and from_low is not None
        and -10 <= price_change <= 10
        and from_low < 15
        and oi_1h > 0
        and taker_1h >= 1.05
        and funding_clean(row)
    ):
        rules.append("quiet_contract_a")
    if oi_4h is not None and taker_4h is not None and oi_4h > 0 and taker_4h >= 1.05 and funding_clean(row):
        rules.append("contract_long_4h")
        if (
            from_low is not None
            and top_position_4h is not None
            and from_low >= 2
            and oi_4h >= 1
            and top_position_4h >= 1.1
        ):
            rules.append("contract_long_4h_precise")
    if volume_ratio >= 1 and oi_1h is not None and taker_1h is not None and oi_1h > 0 and taker_1h >= 1.05 and funding_clean(row):
        rules.append("volume_contract_a")
    if (
        "volume_contract_a" in rules
        and top_position_1h is not None
        and top_account_1h is not None
        and top_position_1h >= 1.05
        and top_account_1h >= 1.05
    ):
        rules.append("volume_contract_major_confirmed")
    return rules


def stats(rows):
    if not rows:
        return {}
    def values(key):
        return [hist.safe_float(row.get(key)) for row in rows if hist.safe_float(row.get(key)) is not None]
    mfe24 = values("future_mfe_24h")
    mfe48 = values("future_mfe_48h")
    ret24 = values("future_return_24h")
    ret48 = values("future_return_48h")
    mae24 = values("future_mae_24h")
    return {
        "n": len(rows),
        "symbols": len(set(row["symbol"] for row in rows)),
        "ret24_win": sum(1 for value in ret24 if value > 0),
        "ret24_n": len(ret24),
        "ret24_avg": hist.avg(ret24),
        "ret48_win": sum(1 for value in ret48 if value > 0),
        "ret48_n": len(ret48),
        "ret48_avg": hist.avg(ret48),
        "mfe24_avg": hist.avg(mfe24),
        "mfe24_med": hist.median(mfe24),
        "mfe48_avg": hist.avg(mfe48),
        "mfe48_med": hist.median(mfe48),
        "mae24_avg": hist.avg(mae24),
        "tp10_24": sum(1 for value in mfe24 if value >= 10),
        "tp20_24": sum(1 for value in mfe24 if value >= 20),
        "tp30_24": sum(1 for value in mfe24 if value >= 30),
        "tp50_48": sum(1 for value in mfe48 if value >= 50),
    }


def stat_line(name, rows):
    s = stats(rows)
    if not s:
        return f"- {name}: n=0"
    ret24_wr = s["ret24_win"] / s["ret24_n"] * 100 if s["ret24_n"] else 0
    ret48_wr = s["ret48_win"] / s["ret48_n"] * 100 if s["ret48_n"] else 0
    tp10 = s["tp10_24"] / s["n"] * 100
    tp20 = s["tp20_24"] / s["n"] * 100
    tp30 = s["tp30_24"] / s["n"] * 100
    tp50 = s["tp50_48"] / s["n"] * 100
    return (
        f"- {name}: n={s['n']} symbols={s['symbols']} "
        f"24h胜={s['ret24_win']}/{s['ret24_n']} {ret24_wr:.1f}% avg={fmt(s['ret24_avg'])}% "
        f"48h胜={s['ret48_win']}/{s['ret48_n']} {ret48_wr:.1f}% avg={fmt(s['ret48_avg'])}% "
        f"MFE24 avg={fmt(s['mfe24_avg'])}% med={fmt(s['mfe24_med'])}% "
        f"MFE48 avg={fmt(s['mfe48_avg'])}% med={fmt(s['mfe48_med'])}% "
        f"MAE24 avg={fmt(s['mae24_avg'])}% "
        f"TP10/20/30_24h={tp10:.1f}/{tp20:.1f}/{tp30:.1f}% TP50_48h={tp50:.1f}%"
    )


def build_report(rows, prefilter_count, args, client):
    lines = []
    lines.append("[ALT MOONSHOT REVERSE VALIDATION]")
    lines.append(
        f"range_days={args.days} stride_hours={args.stride_hours} cooldown_hours={args.cooldown_hours} "
        f"selection={args.selection_mode} per_symbol_cap={args.per_symbol_cap} "
        f"symbols_scanned={args.symbols_scanned} prefilter={prefilter_count} enriched={len(rows)}"
    )
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append("prefilter: quiet price state or active 1h volume; then validate contract rules with 1h/4h OI+taker+funding.")
    lines.append("")
    lines.append(stat_line("prefilter_enriched_base", rows))
    by_rule = {}
    for row in rows:
        for rule in row.get("rules", "").split("|"):
            if rule:
                by_rule.setdefault(rule, []).append(row)
    for rule in sorted(by_rule):
        lines.append(stat_line(rule, by_rule[rule]))
    lines.append("")
    lines.append("规则命中样本 Top MFE48:")
    hit_rows = [row for row in rows if row.get("rules")]
    for row in sorted(hit_rows, key=lambda item: hist.safe_float(item.get("future_mfe_48h"), 0.0) or 0.0, reverse=True)[:30]:
        lines.append(
            f"- {row['symbol']} {row['event_time']} rules={row.get('rules')} "
            f"p24={fmt(hist.safe_float(row.get('price_change_24h')))}% volr={fmt(hist.safe_float(row.get('volume_ratio_24h')))} "
            f"oi1h={fmt(hist.safe_float(row.get('oi_change_1h')))}% taker1h={fmt(hist.safe_float(row.get('taker_buy_sell_1h')), 3)} "
            f"fund={fmt(hist.safe_float(row.get('funding_latest')), 4)}% "
            f"ret24={fmt(hist.safe_float(row.get('future_return_24h')))}% mfe24={fmt(hist.safe_float(row.get('future_mfe_24h')))}% "
            f"mfe48={fmt(hist.safe_float(row.get('future_mfe_48h')))}%"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Reverse-validate alt moonshot contract filters over ordinary market hours.")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--sleep", type=float, default=0.08)
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--max-enrich", type=int, default=360)
    parser.add_argument("--per-symbol-cap", type=int, default=0)
    parser.add_argument("--stride-hours", type=int, default=4)
    parser.add_argument("--cooldown-hours", type=int, default=12)
    parser.add_argument("--selection-mode", choices=("volume", "lowbase", "elastic"), default="volume")
    parser.add_argument("--no-network", action="store_true")
    args = parser.parse_args()

    client = hist.BinanceCache(args.cache_dir, sleep_seconds=args.sleep, no_network=args.no_network)
    end = hist.utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=args.days)
    symbols = hist.futures_symbols(client)
    if args.max_symbols > 0:
        symbols = symbols[: args.max_symbols]
    args.symbols_scanned = len(symbols)

    prefilter = []
    for idx, symbol in enumerate(symbols, start=1):
        try:
            klines = hist.fetch_klines_1h(client, symbol, start, end)
        except Exception:
            continue
        prefilter.extend(build_prefilter_rows(symbol, klines, args.stride_hours, args.cooldown_hours))
        if idx % 50 == 0:
            print(f"scanned={idx}/{len(symbols)} prefilter={len(prefilter)} requests={client.requests}", flush=True)

    prefilter_count = len(prefilter)
    if args.selection_mode in {"lowbase", "elastic"}:
        def lowbase_rank(row):
            price_change = hist.safe_float(row.get("price_change_24h"), 999.0) or 999.0
            from_low = hist.safe_float(row.get("from_24h_low"), 999.0) or 999.0
            volume_ratio = hist.safe_float(row.get("volume_ratio_24h"), 0.0) or 0.0
            in_shape = -8 <= price_change <= 5 and from_low < 10 and 1 <= volume_ratio <= 3
            return (
                1 if in_shape else 0,
                -abs(volume_ratio - 1.5),
                -abs(price_change - 2.0),
                -abs(from_low - 4.0),
            )

        if args.selection_mode == "elastic":
            try:
                import alt_moonshot_elastic_report as elastic
            except Exception:
                elastic = None
            feature_by_symbol = {}
            if elastic is not None:
                for symbol in symbols:
                    try:
                        feature_klines = hist.fetch_klines_1h(client, symbol, start, end)
                    except Exception:
                        continue
                    feature = elastic.symbol_features(feature_klines)
                    if feature:
                        feature_by_symbol[symbol] = feature

            def elastic_rank(row):
                feature = feature_by_symbol.get(row.get("symbol"), {})
                price_change = hist.safe_float(row.get("price_change_24h"), 999.0) or 999.0
                from_low = hist.safe_float(row.get("from_24h_low"), 999.0) or 999.0
                volume_ratio = hist.safe_float(row.get("volume_ratio_24h"), 0.0) or 0.0
                max_range = hist.safe_float(feature.get("hist_max_24h_range"), 0.0) or 0.0
                avg_range = hist.safe_float(feature.get("hist_avg_24h_range"), 0.0) or 0.0
                quote = hist.safe_float(feature.get("hist_quote_median_24h"), 0.0) or 0.0
                boom_days = hist.safe_float(feature.get("hist_boom_days"), 0.0) or 0.0
                in_shape = -8 <= price_change <= 5 and from_low < 10 and 1 <= volume_ratio <= 3
                elastic_active = max_range >= 30 and avg_range >= 8 and 2_000_000 <= quote <= 50_000_000
                return (
                    1 if in_shape else 0,
                    1 if elastic_active else 0,
                    min(max_range, 150.0),
                    min(boom_days, 5.0),
                    -abs(volume_ratio - 1.5),
                    -abs(price_change - 2.0),
                )

            prefilter = sorted(prefilter, key=elastic_rank, reverse=True)
        else:
            prefilter = sorted(prefilter, key=lowbase_rank, reverse=True)
    else:
        prefilter = sorted(
            prefilter,
            key=lambda row: (
                hist.safe_float(row.get("volume_ratio_24h"), 0.0) or 0.0,
                abs(hist.safe_float(row.get("price_change_24h"), 0.0) or 0.0) * -1,
            ),
            reverse=True,
        )
    if args.per_symbol_cap > 0:
        capped = []
        symbol_counts = {}
        for row in prefilter:
            symbol = row.get("symbol")
            count = symbol_counts.get(symbol, 0)
            if count >= args.per_symbol_cap:
                continue
            capped.append(row)
            symbol_counts[symbol] = count + 1
        prefilter = capped
    if args.max_enrich > 0:
        prefilter = prefilter[: args.max_enrich]

    enriched = []
    for idx, item in enumerate(prefilter, start=1):
        row = enrich_validation_context(client, dict(item))
        row["rules"] = "|".join(rule_hits(row))
        enriched.append(row)
        if idx % 25 == 0:
            print(f"enriched={idx}/{len(prefilter)} requests={client.requests}", flush=True)

    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"alt_moonshot_validation_{args.selection_mode}_{args.days}d.csv"
    txt_path = out_dir / f"alt_moonshot_validation_{args.selection_mode}_{args.days}d.txt"
    latest_path = out_dir / "latest_alt_moonshot_validation.txt"
    hist.write_csv(csv_path, enriched)
    report = build_report(enriched, prefilter_count, args, client)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
