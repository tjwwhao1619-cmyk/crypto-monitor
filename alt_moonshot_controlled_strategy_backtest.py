import argparse
import csv
import json
import time
from pathlib import Path

import requests

import alt_moonshot_history as hist
from alt_moonshot_size_holder_profile import (
    binance_futures_prices,
    fetch_security,
    holder_stats,
    holder_values,
    select_token_pair,
    token_address_for_base,
)
from alt_moonshot_spot_chain_strategy import base_from_symbol, dex_search
from alt_moonshot_top30_profile_backtest import load_startup_rows
from alt_moonshot_validate import stat_line


def load_hl_universe():
    try:
        data = requests.post("https://api.hyperliquid.xyz/info", json={"type": "meta"}, timeout=20).json()
    except Exception:
        return set()
    return {str(item.get("name") or "").upper() for item in data.get("universe", []) if item.get("name")}


def safe_num(value):
    return hist.safe_float(value)


def load_symbol_profiles(symbols, cache_dir, sleep):
    prices = binance_futures_prices()
    hl = load_hl_universe()
    profiles = {}
    for idx, symbol in enumerate(sorted(symbols), start=1):
        base = base_from_symbol(symbol)
        item = {
            "base": base,
            "has_hl_perp": base.upper() in hl,
            "current_futures_price": prices.get(symbol),
        }
        try:
            data = dex_search(base, cache_dir, sleep=sleep)
            pair, ratio, status = select_token_pair(base, data, reference_price=prices.get(symbol))
            item["dex_match_status"] = status
            item["dex_price_ratio_distance"] = ratio
            if pair:
                address, name = token_address_for_base(base, pair)
                item.update(
                    {
                        "dex_chain": pair.get("chainId") or "",
                        "dex_id": pair.get("dexId") or "",
                        "token_name": name,
                        "token_address": address,
                        "dex_price_usd_current": safe_num(pair.get("priceUsd")),
                        "dex_fdv_current": safe_num(pair.get("fdv")),
                        "dex_market_cap_current": safe_num(pair.get("marketCap")),
                        "dex_liquidity_usd": safe_num((pair.get("liquidity") or {}).get("usd")) if isinstance(pair.get("liquidity"), dict) else None,
                    }
                )
                security, security_status = fetch_security(item["dex_chain"], address)
                item["security_status"] = security_status
                item["holder_count"] = security.get("holder_count") or ""
                _raw, adjusted = holder_values(security, solana=item["dex_chain"] == "solana")
                adj_stats = holder_stats(adjusted)
                item["holder_top1_adjusted_pct"] = adj_stats["top1"]
                item["holder_top5_adjusted_pct"] = adj_stats["top5"]
                item["holder_top10_adjusted_pct"] = adj_stats["top10"]
                item["is_mintable"] = security.get("is_mintable") or security.get("mintable", {}).get("status", "")
                item["is_honeypot"] = security.get("is_honeypot") or ""
                time.sleep(max(0.0, sleep))
        except Exception as exc:
            item["dex_match_status"] = f"error:{type(exc).__name__}"
        profiles[symbol] = item
        if idx % 100 == 0:
            print(f"profiled={idx}/{len(symbols)}", flush=True)
    return profiles


def estimate_event_size(row):
    entry = safe_num(row.get("entry_price"))
    current = safe_num(row.get("current_futures_price")) or safe_num(row.get("dex_price_usd_current"))
    ratio = entry / current if entry and current and current > 0 else None
    fdv = safe_num(row.get("dex_fdv_current"))
    mcap = safe_num(row.get("dex_market_cap_current"))
    row["estimated_event_fdv"] = fdv * ratio if fdv is not None and ratio is not None else None
    row["estimated_event_market_cap"] = mcap * ratio if mcap is not None and ratio is not None else None
    row["estimated_event_size"] = row["estimated_event_market_cap"] if row["estimated_event_market_cap"] is not None else row["estimated_event_fdv"]


def add_controlled_labels(row):
    labels = set((row.get("strategy_labels") or "").split("|"))
    labels.discard("")
    out = list(labels)
    size = safe_num(row.get("estimated_event_size"))
    fdv = safe_num(row.get("estimated_event_fdv"))
    top10 = safe_num(row.get("holder_top10_adjusted_pct"))
    liq = safe_num(row.get("dex_liquidity_usd")) or 0.0
    price_matched = row.get("dex_match_status") == "price_matched"
    no_hl = str(row.get("has_hl_perp")) in {"False", "false", "0", ""}
    no_spot = str(row.get("has_binance_spot")) in {"False", "false", "0", ""}
    alpha = "profile_alpha_chain" in labels or "profile_alpha_bsc" in labels
    core = "core_alpha_chain" in labels or "core_alpha_bsc" in labels
    strict = "strict_alpha_chain" in labels

    size_10_50 = size is not None and 10_000_000 <= size <= 50_000_000
    fdv_10_50 = fdv is not None and 10_000_000 <= fdv <= 50_000_000
    holder_60 = top10 is not None and top10 >= 60.0
    holder_80 = top10 is not None and top10 >= 80.0
    liquid = liq >= 50_000

    if price_matched and size_10_50:
        out.append("size_10_50m")
    if price_matched and fdv_10_50:
        out.append("fdv_10_50m")
    if holder_60:
        out.append("holder_top10_60")
    if holder_80:
        out.append("holder_top10_80")
    if price_matched and alpha and no_hl and size_10_50:
        out.append("alpha_nohl_size")
    if price_matched and alpha and no_hl and size_10_50 and holder_60:
        out.append("alpha_nohl_size_holder60")
    if price_matched and alpha and no_hl and size_10_50 and holder_80:
        out.append("alpha_nohl_size_holder80")
    if price_matched and core and no_hl and size_10_50 and holder_60:
        out.append("core_nohl_size_holder60")
    if price_matched and strict and no_hl and size_10_50 and holder_60:
        out.append("strict_nohl_size_holder60")
    if price_matched and alpha and no_hl and no_spot and size_10_50 and holder_60 and liquid:
        out.append("alpha_liquid_nohl_nospot_size_holder60")
    row["controlled_strategy_labels"] = "|".join(out)


def label_rows(market_rows, profiles):
    for row in market_rows:
        profile = profiles.get(row["symbol"], {})
        row.update(profile)
        estimate_event_size(row)
        add_controlled_labels(row)


def build_report(rows, startup_rows, top30_symbols, args):
    remaining = [row for row in rows if row["symbol"] not in top30_symbols]
    lines = []
    lines.append("[CONTROLLED MOONSHOT STRATEGY BACKTEST]")
    lines.append(f"days={args.days} rows={len(rows)} remaining={len(remaining)} top30_excluded={len(top30_symbols)}")
    lines.append("model: previous alpha/no-spot/chain profile + no Hyperliquid + estimated event size + current holder concentration.")
    lines.append("size proxy: estimated_event_market_cap if available else estimated_event_fdv, scaled by entry_price/current_price.")
    lines.append("holder proxy: current GoPlus top-holder snapshot, not historical holder snapshot.")
    lines.append("")
    for label in (
        "profile_alpha_chain",
        "profile_alpha_bsc",
        "size_10_50m",
        "holder_top10_60",
        "holder_top10_80",
        "alpha_nohl_size",
        "alpha_nohl_size_holder60",
        "alpha_nohl_size_holder80",
        "core_nohl_size_holder60",
        "strict_nohl_size_holder60",
        "alpha_liquid_nohl_nospot_size_holder60",
    ):
        matched = [row for row in remaining if label in (row.get("controlled_strategy_labels") or "").split("|")]
        lines.append(stat_line(label, matched))
    lines.append("")
    lines.append("Top alpha_nohl_size_holder60 hits by MFE48:")
    hits = [row for row in remaining if "alpha_nohl_size_holder60" in (row.get("controlled_strategy_labels") or "").split("|")]
    for row in sorted(hits, key=lambda item: safe_num(item.get("future_mfe_48h")) or 0.0, reverse=True)[:40]:
        lines.append(
            f"- {row['symbol']} {row['event_time']} labels={row.get('controlled_strategy_labels')} "
            f"size={safe_num(row.get('estimated_event_size'))/1_000_000 if safe_num(row.get('estimated_event_size')) else None:.2f}m "
            f"top10={safe_num(row.get('holder_top10_adjusted_pct')):.1f}% "
            f"chain={row.get('dex_chain')} liq={safe_num(row.get('dex_liquidity_usd')) or 0:.0f} "
            f"p24={safe_num(row.get('price_change_24h')):+.2f}% fromLow={safe_num(row.get('from_24h_low')):.2f}% "
            f"volr={safe_num(row.get('volume_ratio_24h')):.2f} "
            f"ret24={safe_num(row.get('future_return_24h')):+.2f}% "
            f"MFE24={safe_num(row.get('future_mfe_24h')):+.2f}% MFE48={safe_num(row.get('future_mfe_48h')):+.2f}%"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Backtest moonshot profile with size and holder concentration labels.")
    parser.add_argument("--market-csv", default="reports/alt_moonshots/spot_chain_strategy_backtest_60d.csv")
    parser.add_argument("--startup-csv", default="reports/alt_moonshots/over100_startup_features_60d.csv")
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots_dex")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--sleep", type=float, default=0.03)
    args = parser.parse_args()

    market_rows = list(csv.DictReader(Path(args.market_csv).open(encoding="utf-8")))
    startup_rows = load_startup_rows(args.startup_csv)
    top30_symbols = {row["symbol"] for row in startup_rows[:30]}
    symbols = {row["symbol"] for row in market_rows}
    profiles = load_symbol_profiles(symbols, args.cache_dir, args.sleep)
    label_rows(market_rows, profiles)

    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"controlled_moonshot_strategy_backtest_{args.days}d.csv"
    txt_path = out_dir / f"controlled_moonshot_strategy_backtest_{args.days}d.txt"
    latest_path = out_dir / "latest_controlled_moonshot_strategy_backtest.txt"
    hist.write_csv(csv_path, market_rows)
    report = build_report(market_rows, startup_rows, top30_symbols, args)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
