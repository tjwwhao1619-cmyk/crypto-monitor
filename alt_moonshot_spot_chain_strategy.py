import argparse
import csv
import json
import time
from pathlib import Path

import requests

import alt_moonshot_history as hist
from alt_moonshot_top30_profile_backtest import load_startup_rows, top30_profile
from alt_moonshot_validate import stat_line


def base_from_symbol(symbol):
    return symbol[:-4] if symbol.endswith("USDT") else symbol


def load_spot_bases():
    data = requests.get("https://api.binance.com/api/v3/exchangeInfo", timeout=20).json()
    bases = set()
    pairs_by_base = {}
    for item in data.get("symbols", []):
        if not isinstance(item, dict) or item.get("status") != "TRADING":
            continue
        base = item.get("baseAsset")
        if not base:
            continue
        bases.add(base)
        pairs_by_base.setdefault(base, []).append(item.get("symbol"))
    return bases, pairs_by_base


def dex_cache_path(cache_dir, base):
    safe = "".join(ch if ch.isalnum() or ch in "_-" else "_" for ch in base)
    return Path(cache_dir) / f"dex_{safe}.json"


def dex_search(base, cache_dir, sleep=0.08):
    Path(cache_dir).mkdir(parents=True, exist_ok=True)
    path = dex_cache_path(cache_dir, base)
    if path.exists():
        return json.loads(path.read_text(encoding="utf-8"))
    data = requests.get("https://api.dexscreener.com/latest/dex/search", params={"q": base}, timeout=15).json()
    path.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    time.sleep(max(0.0, sleep))
    return data


def dex_labels(base, data):
    raw_pairs = data.get("pairs") if isinstance(data, dict) else []
    if not isinstance(raw_pairs, list):
        raw_pairs = []
    pairs = []
    for pair in raw_pairs:
        if not isinstance(pair, dict):
            continue
        base_token = pair.get("baseToken") if isinstance(pair.get("baseToken"), dict) else {}
        quote_token = pair.get("quoteToken") if isinstance(pair.get("quoteToken"), dict) else {}
        bt = base_token.get("symbol", "") or ""
        qt = quote_token.get("symbol", "") or ""
        if bt.upper() != base.upper() and qt.upper() != base.upper():
            continue
        liq = hist.safe_float((pair.get("liquidity") or {}).get("usd"), 0.0) if isinstance(pair.get("liquidity"), dict) else 0.0
        vol = hist.safe_float((pair.get("volume") or {}).get("h24"), 0.0) if isinstance(pair.get("volume"), dict) else 0.0
        pairs.append(
            {
                "chain": pair.get("chainId") or "",
                "dex": pair.get("dexId") or "",
                "liq": liq or 0.0,
                "vol": vol or 0.0,
                "base": bt,
                "quote": qt,
            }
        )
    chain_liq = {}
    for pair in sorted(pairs, key=lambda item: item["liq"], reverse=True)[:12]:
        if pair["chain"]:
            chain_liq[pair["chain"]] = chain_liq.get(pair["chain"], 0.0) + pair["liq"]
    primary_chain = ""
    primary_liq = 0.0
    if chain_liq:
        primary_chain, primary_liq = max(chain_liq.items(), key=lambda item: item[1])
    return {
        "has_dex_pair": bool(pairs),
        "has_bsc_pair": "bsc" in chain_liq,
        "has_solana_pair": "solana" in chain_liq,
        "primary_chain": primary_chain,
        "primary_chain_liq": primary_liq,
        "bsc_liq": chain_liq.get("bsc", 0.0),
        "solana_liq": chain_liq.get("solana", 0.0),
        "dex_pair_count": len(pairs),
    }


def build_symbol_labels(symbols, cache_dir, sleep):
    spot_bases, pairs_by_base = load_spot_bases()
    labels = {}
    for idx, symbol in enumerate(symbols, start=1):
        base = base_from_symbol(symbol)
        item = {
            "base": base,
            "has_binance_spot": base in spot_bases,
            "spot_pairs": "|".join(sorted(pairs_by_base.get(base, []))),
        }
        try:
            item.update(dex_labels(base, dex_search(base, cache_dir, sleep=sleep)))
        except Exception:
            item.update(
                {
                    "has_dex_pair": False,
                    "has_bsc_pair": False,
                    "has_solana_pair": False,
                    "primary_chain": "",
                    "primary_chain_liq": 0.0,
                    "bsc_liq": 0.0,
                    "solana_liq": 0.0,
                    "dex_pair_count": 0,
                }
            )
        item["alpha_chain_candidate"] = (not item["has_binance_spot"]) and item["has_dex_pair"]
        item["alpha_bsc_candidate"] = item["alpha_chain_candidate"] and item["has_bsc_pair"]
        labels[symbol] = item
        if idx % 100 == 0:
            print(f"labeled={idx}/{len(symbols)}", flush=True)
    return labels


def profile_labels(row):
    labels = top30_profile(row)
    return labels


def strategy_labels(row):
    labels = profile_labels(row)
    out = list(labels)
    if labels and row.get("alpha_chain_candidate"):
        out.append("profile_alpha_chain")
    if labels and row.get("alpha_bsc_candidate"):
        out.append("profile_alpha_bsc")
    if "top30_profile_core" in labels and row.get("alpha_chain_candidate"):
        out.append("core_alpha_chain")
    if "top30_profile_core" in labels and row.get("alpha_bsc_candidate"):
        out.append("core_alpha_bsc")
    if "top30_profile_strict" in labels and row.get("alpha_chain_candidate"):
        out.append("strict_alpha_chain")
    return out


def build_report(market_rows, startup_rows, top30_symbols, symbol_labels, args):
    lines = []
    lines.append("[SPOT / CHAIN STRATEGY BACKTEST]")
    lines.append(f"days={args.days} rows={len(market_rows)} symbols={len(symbol_labels)} top30_excluded={len(top30_symbols)}")
    top30 = startup_rows[:30]
    no_spot_top30 = sum(1 for row in top30 if not symbol_labels.get(row["symbol"], {}).get("has_binance_spot"))
    bsc_top30 = sum(1 for row in top30 if symbol_labels.get(row["symbol"], {}).get("has_bsc_pair"))
    dex_top30 = sum(1 for row in top30 if symbol_labels.get(row["symbol"], {}).get("has_dex_pair"))
    lines.append(f"top30 labels: no_spot={no_spot_top30}/30 has_dex={dex_top30}/30 has_bsc={bsc_top30}/30")
    lines.append("")
    remaining = [row for row in market_rows if row["symbol"] not in top30_symbols]
    lines.append(stat_line("remaining_market_base", remaining))
    for label in (
        "top30_profile_core",
        "top30_profile_wide",
        "top30_profile_strict",
        "profile_alpha_chain",
        "profile_alpha_bsc",
        "core_alpha_chain",
        "core_alpha_bsc",
        "strict_alpha_chain",
    ):
        rows = [row for row in remaining if label in row.get("strategy_labels", "").split("|")]
        lines.append(stat_line(label, rows))
    lines.append("")
    lines.append("Top profile_alpha_bsc hits by MFE48:")
    hits = [row for row in remaining if "profile_alpha_bsc" in row.get("strategy_labels", "").split("|")]
    for row in sorted(hits, key=lambda item: hist.safe_float(item.get("future_mfe_48h"), 0.0) or 0.0, reverse=True)[:40]:
        lines.append(
            f"- {row['symbol']} {row['event_time']} labels={row.get('strategy_labels')} "
            f"chain={row.get('primary_chain')} spot={row.get('has_binance_spot')} "
            f"p24={hist.safe_float(row.get('price_change_24h')):+.2f}% "
            f"fromLow={hist.safe_float(row.get('from_24h_low')):.2f}% "
            f"volr={hist.safe_float(row.get('volume_ratio_24h')):.2f} "
            f"ret24={hist.safe_float(row.get('future_return_24h')):+.2f}% "
            f"MFE24={hist.safe_float(row.get('future_mfe_24h')):+.2f}% "
            f"MFE48={hist.safe_float(row.get('future_mfe_48h')):+.2f}%"
        )
    return "\n".join(lines) + "\n"


def main():
    parser = argparse.ArgumentParser(description="Backtest top30 profile with no-spot and chain labels.")
    parser.add_argument("--market-csv", default="reports/alt_moonshots/top30_profile_market_backtest_60d.csv")
    parser.add_argument("--startup-csv", default="reports/alt_moonshots/over100_startup_features_60d.csv")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots_dex")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--sleep", type=float, default=0.08)
    args = parser.parse_args()

    market_rows = list(csv.DictReader(Path(args.market_csv).open(encoding="utf-8")))
    startup_rows = load_startup_rows(args.startup_csv)
    top30_symbols = {row["symbol"] for row in startup_rows[:30]}
    symbols = sorted(set(row["symbol"] for row in market_rows) | set(row["symbol"] for row in startup_rows[:30]))
    symbol_labels = build_symbol_labels(symbols, args.cache_dir, args.sleep)
    for row in market_rows:
        row.update(symbol_labels.get(row["symbol"], {}))
        row["strategy_labels"] = "|".join(strategy_labels(row))
    out_dir = Path(args.out_dir)
    csv_path = out_dir / f"spot_chain_strategy_backtest_{args.days}d.csv"
    txt_path = out_dir / f"spot_chain_strategy_backtest_{args.days}d.txt"
    latest_path = out_dir / "latest_spot_chain_strategy_backtest.txt"
    hist.write_csv(csv_path, market_rows)
    report = build_report(market_rows, startup_rows, top30_symbols, symbol_labels, args)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
