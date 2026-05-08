import csv
import json
import statistics
import time
from pathlib import Path

import requests

import alt_moonshot_history as hist
from alt_moonshot_spot_chain_strategy import base_from_symbol, dex_cache_path, dex_search, load_spot_bases


CHAIN_IDS = {
    "ethereum": "1",
    "bsc": "56",
    "base": "8453",
    "arbitrum": "42161",
    "polygon": "137",
    "optimism": "10",
    "avalanche": "43114",
    "fantom": "250",
    "cronos": "25",
}

BURN_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
}

MAX_PRICE_RATIO_DISTANCE = 3.0


def load_startup_rows(path):
    rows = list(csv.DictReader(Path(path).open(encoding="utf-8")))
    rows.sort(key=lambda row: hist.safe_float(row.get("total_gain_60d"), 0.0) or 0.0, reverse=True)
    return rows


def exact_pairs(base, data):
    raw = data.get("pairs") if isinstance(data, dict) else []
    pairs = []
    for pair in raw or []:
        if not isinstance(pair, dict):
            continue
        base_token = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
        quote_token = pair.get("quoteToken") if isinstance(pair.get("quoteToken"), dict) else {}
        bt = base_token.get("symbol", "") or ""
        qt = quote_token.get("symbol", "") or ""
        if bt.upper() == base.upper() or qt.upper() == base.upper():
            pairs.append(pair)
    return pairs


def pair_liq(pair):
    liq = pair.get("liquidity") if isinstance(pair.get("liquidity"), dict) else {}
    return hist.safe_float(liq.get("usd"), 0.0) or 0.0


def binance_futures_prices():
    data = requests.get("https://fapi.binance.com/fapi/v1/ticker/price", timeout=20).json()
    prices = {}
    for item in data:
        symbol = item.get("symbol")
        price = hist.safe_float(item.get("price"))
        if symbol and price:
            prices[symbol] = price
    return prices


def select_token_pair(base, data, reference_price=None):
    pairs = exact_pairs(base, data)
    if not pairs:
        return None, None, "no_exact_pair"
    base_matches = [
        pair for pair in pairs
        if ((pair.get("baseToken") or {}).get("symbol", "") or "").upper() == base.upper()
    ]
    pool = base_matches or pairs
    priced = []
    if reference_price and reference_price > 0:
        for pair in pool:
            price = hist.safe_float(pair.get("priceUsd"))
            if not price or price <= 0:
                continue
            ratio = max(price / reference_price, reference_price / price)
            priced.append((ratio, pair))
        if priced:
            ratio, pair = min(priced, key=lambda item: item[0])
            if ratio <= MAX_PRICE_RATIO_DISTANCE:
                return pair, ratio, "price_matched"
            return None, ratio, "price_mismatch"
    return max(pool, key=pair_liq), None, "liquidity_fallback"


def token_address_for_base(base, pair):
    base_token = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
    quote_token = pair.get("quoteToken") if isinstance(pair.get("quoteToken"), dict) else {}
    if (base_token.get("symbol", "") or "").upper() == base.upper():
        return base_token.get("address") or "", base_token.get("name") or ""
    if (quote_token.get("symbol", "") or "").upper() == base.upper():
        return quote_token.get("address") or "", quote_token.get("name") or ""
    return "", ""


def holder_values(item, solana=False):
    holders = item.get("holders")
    if not isinstance(holders, list):
        holders = []
    out = []
    adjusted = []
    for holder in holders:
        if not isinstance(holder, dict):
            continue
        pct = hist.safe_float(holder.get("percent"), 0.0)
        if pct is None:
            continue
        out.append(pct * 100 if pct <= 1.0 else pct)
        address = (holder.get("address") or holder.get("account") or "").lower()
        is_locked = str(holder.get("is_locked", "0")) == "1"
        if address in BURN_ADDRESSES or is_locked:
            continue
        adjusted.append(pct * 100 if pct <= 1.0 else pct)
    return sorted(out, reverse=True), sorted(adjusted, reverse=True)


def holder_stats(vals):
    vals = vals or []
    return {
        "top1": sum(vals[:1]) if vals else None,
        "top5": sum(vals[:5]) if vals else None,
        "top10": sum(vals[:10]) if vals else None,
    }


def fetch_security(chain, address):
    if not address:
        return {}, "missing_address"
    try:
        if chain == "solana":
            url = "https://api.gopluslabs.io/api/v1/solana/token_security"
            params = {"contract_addresses": address}
        elif chain in CHAIN_IDS:
            url = f"https://api.gopluslabs.io/api/v1/token_security/{CHAIN_IDS[chain]}"
            params = {"contract_addresses": address}
        else:
            return {}, f"unsupported_chain:{chain}"
        data = requests.get(url, params=params, timeout=20).json()
        result = data.get("result") if isinstance(data, dict) else {}
        if not isinstance(result, dict):
            return {}, "empty_result"
        item = result.get(address) or result.get(address.lower()) or next(iter(result.values()), {})
        if not isinstance(item, dict):
            return {}, "empty_item"
        return item, "ok"
    except Exception as exc:
        return {}, f"error:{type(exc).__name__}"


def fmt_money(value):
    if value is None:
        return "NA"
    if value >= 1_000_000:
        return f"${value / 1_000_000:.2f}m"
    if value >= 1_000:
        return f"${value / 1_000:.1f}k"
    return f"${value:.0f}"


def fmt_pct(value):
    if value is None:
        return "NA"
    return f"{value:.2f}%"


def median(values):
    vals = [v for v in values if v is not None]
    return statistics.median(vals) if vals else None


def build_report(rows):
    lines = []
    lines.append("[TOP30 SIZE / HOLDER PROFILE]")
    lines.append("scope=top30 moonshot startup samples")
    lines.append("notes: startup market cap is estimated from current DEX marketCap/FDV scaled by startup_price/current_price; holder concentration is current GoPlus snapshot, not historical holder snapshot.")
    lines.append("")
    reliable = [row for row in rows if row.get("dex_match_status") == "price_matched"]
    liquid_reliable = [
        row for row in reliable
        if (hist.safe_float(row.get("dex_liquidity_usd")) or 0.0) >= 50_000
    ]
    start_mcaps = [hist.safe_float(row.get("estimated_start_market_cap")) for row in reliable]
    start_fdvs = [hist.safe_float(row.get("estimated_start_fdv")) for row in reliable]
    holder_top10 = [hist.safe_float(row.get("holder_top10_adjusted_pct")) for row in reliable]
    holder_top5 = [hist.safe_float(row.get("holder_top5_adjusted_pct")) for row in reliable]
    lines.append(
        f"summary: price_matched_dex={len(reliable)}/{len(rows)} "
        f"estimated_start_mcap_med={fmt_money(median(start_mcaps))} "
        f"estimated_start_fdv_med={fmt_money(median(start_fdvs))} "
        f"holder_top5_adjusted_med={median(holder_top5):.2f}% "
        f"holder_top10_adjusted_med={median(holder_top10):.2f}%"
    )
    low = sum(1 for r in reliable if (hist.safe_float(r.get("estimated_start_fdv")) or 10**18) < 20_000_000)
    mid = sum(1 for r in reliable if 20_000_000 <= (hist.safe_float(r.get("estimated_start_fdv")) or -1) < 100_000_000)
    high = sum(1 for r in reliable if (hist.safe_float(r.get("estimated_start_fdv")) or -1) >= 100_000_000)
    conc = sum(1 for r in reliable if (hist.safe_float(r.get("holder_top10_adjusted_pct")) or 0.0) >= 40.0)
    lines.append(f"start_fdv_buckets: <20m={low} 20m-100m={mid} >=100m={high}")
    lines.append(f"holder_concentration: adjusted_top10>=40% {conc}/{len(reliable)}")
    liquid_fdvs = [hist.safe_float(row.get("estimated_start_fdv")) for row in liquid_reliable]
    liquid_mcaps = [hist.safe_float(row.get("estimated_start_market_cap")) for row in liquid_reliable]
    liquid_top10 = [hist.safe_float(row.get("holder_top10_adjusted_pct")) for row in liquid_reliable]
    lines.append(
        f"liquid_price_matched(DEX_liq>=50k): n={len(liquid_reliable)} "
        f"start_mcap_med={fmt_money(median(liquid_mcaps))} "
        f"start_fdv_med={fmt_money(median(liquid_fdvs))} "
        f"holder_top10_adjusted_med={fmt_pct(median(liquid_top10))}"
    )
    lines.append("")
    lines.append("Top30 details:")
    for row in rows:
        lines.append(
            f"- {row['symbol']} gain={hist.safe_float(row.get('total_gain_60d')):.1f}% "
            f"match={row.get('dex_match_status')} ratio={row.get('dex_price_ratio_distance') or 'NA'} "
            f"chain={row.get('dex_chain') or 'NA'} liq={fmt_money(hist.safe_float(row.get('dex_liquidity_usd')))} "
            f"startMCap={fmt_money(hist.safe_float(row.get('estimated_start_market_cap')))} "
            f"startFDV={fmt_money(hist.safe_float(row.get('estimated_start_fdv')))} "
            f"holders={row.get('holder_count') or 'NA'} "
            f"top1/5/10={row.get('holder_top1_adjusted_pct') or 'NA'}/"
            f"{row.get('holder_top5_adjusted_pct') or 'NA'}/"
            f"{row.get('holder_top10_adjusted_pct') or 'NA'} "
            f"security={row.get('security_status')}"
        )
    return "\n".join(lines) + "\n"


def main():
    startup_csv = "reports/alt_moonshots/over100_startup_features_60d.csv"
    cache_dir = ".cache/alt_moonshots_dex"
    out_dir = Path("reports/alt_moonshots")
    rows = load_startup_rows(startup_csv)[:30]
    spot_bases, _ = load_spot_bases()
    futures_prices = binance_futures_prices()
    output = []
    for row in rows:
        base = base_from_symbol(row["symbol"])
        item = {
            "symbol": row["symbol"],
            "base": base,
            "startup_time": row.get("startup_time"),
            "startup_price": row.get("startup_price"),
            "total_gain_60d": row.get("total_gain_60d"),
            "has_binance_spot": base in spot_bases,
        }
        data = dex_search(base, cache_dir, sleep=0.08)
        ref_price = futures_prices.get(row["symbol"])
        item["binance_futures_price_current"] = ref_price
        pair, price_ratio_distance, match_status = select_token_pair(base, data, reference_price=ref_price)
        item["dex_match_status"] = match_status
        item["dex_price_ratio_distance"] = price_ratio_distance
        if pair:
            address, token_name = token_address_for_base(base, pair)
            price = hist.safe_float(pair.get("priceUsd"))
            fdv = hist.safe_float(pair.get("fdv"))
            market_cap = hist.safe_float(pair.get("marketCap"))
            startup_price = hist.safe_float(row.get("startup_price"))
            ratio = startup_price / price if startup_price is not None and price and price > 0 else None
            item.update(
                {
                    "dex_chain": pair.get("chainId") or "",
                    "dex_id": pair.get("dexId") or "",
                    "token_name": token_name,
                    "token_address": address,
                    "dex_price_usd_current": price,
                    "dex_fdv_current": fdv,
                    "dex_market_cap_current": market_cap,
                    "dex_liquidity_usd": pair_liq(pair),
                    "pair_created_at": pair.get("pairCreatedAt") or "",
                    "estimated_start_fdv": fdv * ratio if fdv is not None and ratio is not None else None,
                    "estimated_start_market_cap": market_cap * ratio if market_cap is not None and ratio is not None else None,
                }
            )
            security, status = fetch_security(item["dex_chain"], address)
            item["security_status"] = status
            item["holder_count"] = security.get("holder_count") or ""
            raw, adjusted = holder_values(security, solana=item["dex_chain"] == "solana")
            raw_stats = holder_stats(raw)
            adj_stats = holder_stats(adjusted)
            item.update(
                {
                    "holder_top1_raw_pct": raw_stats["top1"],
                    "holder_top5_raw_pct": raw_stats["top5"],
                    "holder_top10_raw_pct": raw_stats["top10"],
                    "holder_top1_adjusted_pct": adj_stats["top1"],
                    "holder_top5_adjusted_pct": adj_stats["top5"],
                    "holder_top10_adjusted_pct": adj_stats["top10"],
                    "owner_percent": security.get("owner_percent") or security.get("metadata_mutable", {}).get("status", ""),
                    "creator_percent": security.get("creator_percent") or "",
                    "is_mintable": security.get("is_mintable") or security.get("mintable", {}).get("status", ""),
                    "is_honeypot": security.get("is_honeypot") or "",
                }
            )
            time.sleep(0.08)
        else:
            item["security_status"] = "no_dex_pair"
        output.append(item)
    csv_path = out_dir / "top30_size_holder_profile_60d.csv"
    txt_path = out_dir / "top30_size_holder_profile_60d.txt"
    latest_path = out_dir / "latest_top30_size_holder_profile.txt"
    hist.write_csv(csv_path, output)
    report = build_report(output)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
