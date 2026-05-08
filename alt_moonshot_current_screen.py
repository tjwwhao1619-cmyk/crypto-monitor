import argparse
import csv
import datetime as dt
import hashlib
import json
import os
import time
from pathlib import Path

import requests

import alt_moonshot_coinglass as cg
import alt_moonshot_elastic_report as elastic
import alt_moonshot_factor_extractor as factor
import alt_moonshot_history as hist
from alt_moonshot_controlled_strategy_backtest import load_hl_universe
from alt_moonshot_size_holder_profile import (
    binance_futures_prices,
    exact_pairs,
    fetch_security,
    holder_stats,
    holder_values,
    select_token_pair,
    token_address_for_base,
)
from alt_moonshot_spot_chain_strategy import base_from_symbol, dex_search, load_spot_bases
from alt_moonshot_top30_profile_backtest import top30_profile
from alt_moonshot_validate import enrich_validation_context, rule_hits


FACTOR_WINDOW_HOURS = (2, 4, 6, 12, 24)
DISCORD_API_BASE = "https://discord.com/api/v10"


def current_row_from_klines(symbol, klines, end_time):
    if len(klines) < 73:
        return None
    cutoff = hist.ms(end_time - dt.timedelta(hours=1))
    idx = None
    for i in range(len(klines) - 1, 23, -1):
        if int(klines[i][0]) <= cutoff:
            idx = i
            break
    if idx is None:
        return None
    close = hist.safe_float(klines[idx][4])
    if not close or close <= 0:
        return None
    price_change_24h = hist.pct(hist.safe_float(klines[idx - 24][4]), close)
    prev_24h_low = min(hist.safe_float(row[3], close) or close for row in klines[idx - 24 : idx + 1])
    from_24h_low = hist.pct(prev_24h_low, close)
    quote_now = hist.safe_float(klines[idx][7], 0.0) or 0.0
    prev_quote_avg = hist.avg([hist.safe_float(row[7]) for row in klines[idx - 24 : idx]]) or 0.0
    volume_ratio_24h = quote_now / prev_quote_avg if prev_quote_avg > 0 else None
    row = {
        "symbol": symbol,
        "event_time": hist.parse_ms(klines[idx][0]).isoformat(),
        "entry_price": close,
        "price_change_24h": price_change_24h,
        "from_24h_low": from_24h_low,
        "quote_volume_1h": quote_now,
        "volume_ratio_24h": volume_ratio_24h,
    }
    features = elastic.symbol_features(klines)
    if not features:
        return None
    row.update(features)
    row["profile_labels"] = "|".join(top30_profile(row))
    row["elastic_filters"] = "|".join(elastic.elastic_filters(row))
    return row


def add_current_factor_profile(client, row, coinglass_client=None):
    symbol = row["symbol"]
    event_time = factor.parse_time(row["event_time"])
    try:
        klines_5m = factor.fetch_klines_5m(
            client,
            symbol,
            event_time - dt.timedelta(hours=30),
            event_time + dt.timedelta(hours=1),
        )
        oi_5m = factor.fetch_oi_5m(client, symbol, event_time - dt.timedelta(hours=30), event_time + dt.timedelta(hours=1))
        for hours in FACTOR_WINDOW_HOURS:
            window_start = event_time - dt.timedelta(hours=hours)
            features = factor.window_features(
                klines_5m,
                window_start,
                event_time,
            )
            for key, value in features.items():
                row[f"pre{hours}h_{key}"] = value
            row[f"pre{hours}h_oi_change"] = factor.oi_change(oi_5m, window_start, event_time)
        row.update(factor.market_context(client, event_time))
        if coinglass_client and coinglass_client.api_key:
            row.update(cg.context(coinglass_client, symbol, event_time, hours=4))
        factor.classify(row)
    except Exception as exc:
        row["factor_labels"] = "factor_error"
        row["factor_error"] = type(exc).__name__
        row["factor_score"] = 0


def factor_human_reason(row):
    labels = set((row.get("factor_labels") or "").split("|"))
    if "confirmed_long_liquidation_sweep" in labels:
        return "真实多头强平+OI下降+价格收回，确认拿流动性"
    if "confirmed_short_squeeze_reclaim" in labels:
        return "真实空头强平+价格收回，确认逼空/回补"
    if "price_magnet_liquidity_sweep" in labels:
        return "下杀拿流动性后收回，留意反转"
    if "short_cover_reclaim" in labels:
        return "价格收回但OI下降，偏空头回补"
    if "quiet_compression_base_v2" in labels:
        return "低波动压缩，适合早期发现"
    if "relative_strength_compression" in labels:
        return "大盘弱但币横住，相对强"
    if "buyer_absorption_then_sweep" in labels:
        return "主动买入吸收，可能扫单"
    if "washout_micro_base" in labels:
        return "洗盘后微底，等放量确认"
    if "high_volatility_reversal_base" in labels:
        return "高波动反转，只能观察小仓"
    if "factor_error" in labels:
        return "因子数据暂缺"
    return "未匹配成熟妖币模板"


def exact_chain_flags(base, data):
    pairs = exact_pairs(base, data)
    chains = {pair.get("chainId") or "" for pair in pairs}
    return {
        "has_dex_pair": bool(pairs),
        "has_bsc_pair": "bsc" in chains,
        "has_solana_pair": "solana" in chains,
        "dex_exact_chains": "|".join(sorted(ch for ch in chains if ch)),
        "dex_exact_pair_count": len(pairs),
    }


def add_chain_holder_profile(row, spot_bases, hl_symbols, futures_prices, cache_dir, sleep):
    symbol = row["symbol"]
    base = base_from_symbol(symbol)
    row["base"] = base
    row["has_binance_spot"] = base in spot_bases
    row["has_hl_perp"] = base.upper() in hl_symbols
    row["current_futures_price"] = futures_prices.get(symbol)
    try:
        data = dex_search(base, cache_dir, sleep=sleep)
        row.update(exact_chain_flags(base, data))
        pair, ratio, status = select_token_pair(base, data, reference_price=futures_prices.get(symbol))
        row["dex_match_status"] = status
        row["dex_price_ratio_distance"] = ratio
        if not pair:
            return
        address, name = token_address_for_base(base, pair)
        row.update(
            {
                "dex_chain": pair.get("chainId") or "",
                "dex_id": pair.get("dexId") or "",
                "token_name": name,
                "token_address": address,
                "dex_price_usd_current": hist.safe_float(pair.get("priceUsd")),
                "dex_fdv_current": hist.safe_float(pair.get("fdv")),
                "dex_market_cap_current": hist.safe_float(pair.get("marketCap")),
                "dex_liquidity_usd": hist.safe_float((pair.get("liquidity") or {}).get("usd")) if isinstance(pair.get("liquidity"), dict) else None,
            }
        )
        entry = hist.safe_float(row.get("entry_price"))
        current = hist.safe_float(row.get("current_futures_price")) or hist.safe_float(row.get("dex_price_usd_current"))
        scale = entry / current if entry and current and current > 0 else None
        fdv = hist.safe_float(row.get("dex_fdv_current"))
        market_cap = hist.safe_float(row.get("dex_market_cap_current"))
        row["estimated_event_fdv"] = fdv * scale if fdv is not None and scale is not None else None
        row["estimated_event_market_cap"] = market_cap * scale if market_cap is not None and scale is not None else None
        row["estimated_event_size"] = row["estimated_event_market_cap"] if row["estimated_event_market_cap"] is not None else row["estimated_event_fdv"]
        security, status = fetch_security(row["dex_chain"], address)
        row["security_status"] = status
        row["holder_count"] = security.get("holder_count") or ""
        _raw, adjusted = holder_values(security, solana=row["dex_chain"] == "solana")
        stats = holder_stats(adjusted)
        row["holder_top1_adjusted_pct"] = stats["top1"]
        row["holder_top5_adjusted_pct"] = stats["top5"]
        row["holder_top10_adjusted_pct"] = stats["top10"]
        row["is_mintable"] = security.get("is_mintable") or security.get("mintable", {}).get("status", "")
        row["is_honeypot"] = security.get("is_honeypot") or ""
        time.sleep(max(0.0, sleep))
    except Exception as exc:
        row["dex_match_status"] = f"error:{type(exc).__name__}"


def controlled_labels(row):
    labels = set((row.get("profile_labels") or "").split("|"))
    labels.discard("")
    out = list(labels)
    size = hist.safe_float(row.get("estimated_event_size"))
    top10 = hist.safe_float(row.get("holder_top10_adjusted_pct"))
    price_matched = row.get("dex_match_status") == "price_matched"
    no_spot = not bool(row.get("has_binance_spot"))
    no_hl = not bool(row.get("has_hl_perp"))
    has_dex = bool(row.get("has_dex_pair"))
    has_bsc = bool(row.get("has_bsc_pair"))
    size_10_50 = size is not None and 10_000_000 <= size <= 50_000_000
    holder_60 = top10 is not None and top10 >= 60.0
    holder_80 = top10 is not None and top10 >= 80.0
    alpha = labels and no_spot and has_dex and price_matched
    if labels and alpha:
        out.append("profile_alpha_chain")
    if labels and alpha and has_bsc:
        out.append("profile_alpha_bsc")
    if "top30_profile_core" in labels and alpha:
        out.append("core_alpha_chain")
    if "top30_profile_strict" in labels and alpha:
        out.append("strict_alpha_chain")
    if price_matched and size_10_50:
        out.append("size_10_50m")
    if holder_60:
        out.append("holder_top10_60")
    if holder_80:
        out.append("holder_top10_80")
    if alpha and no_hl and size_10_50:
        out.append("alpha_nohl_size")
    if alpha and no_hl and size_10_50 and holder_60:
        out.append("alpha_nohl_size_holder60")
    if alpha and no_hl and size_10_50 and holder_80:
        out.append("alpha_nohl_size_holder80")
    if "top30_profile_core" in labels and alpha and no_hl and size_10_50 and holder_80:
        out.append("core_nohl_size_holder80")
    if "top30_profile_strict" in labels and alpha and no_hl and size_10_50 and holder_80:
        out.append("strict_nohl_size_holder80")
    row["current_screen_labels"] = "|".join(out)


def score_row(row):
    labels = set((row.get("current_screen_labels") or "").split("|"))
    factor_labels = set((row.get("factor_labels") or "").split("|"))
    score = 0
    reasons = []
    if "alpha_nohl_size_holder80" in labels:
        score += 70
        reasons.append("alpha+无HL+1000万-5000万+top10>=80")
    elif "alpha_nohl_size_holder60" in labels:
        score += 55
        reasons.append("alpha+无HL+1000万-5000万+top10>=60")
    if "strict_nohl_size_holder80" in labels:
        score += 20
        reasons.append("strict结构")
    elif "core_nohl_size_holder80" in labels:
        score += 12
        reasons.append("core结构")
    elif "top30_profile_wide" in labels:
        score += 5
        reasons.append("wide结构")
    contract_rules = set((row.get("contract_rules") or "").split("|"))
    if "contract_long_4h_precise" in contract_rules:
        score += 15
        reasons.append("4h合约确认")
    elif "contract_long_4h" in contract_rules:
        score += 8
        reasons.append("4h合约初确认")
    if row.get("has_bsc_pair"):
        score += 3
        reasons.append("BSC交易对")
    if "quiet_compression_base_v2" in factor_labels:
        score += 15
        reasons.append("低波动压缩")
    if "relative_strength_compression" in factor_labels:
        score += 10
        reasons.append("大盘弱中相对强")
    if "buyer_absorption_then_sweep" in factor_labels:
        score += 8
        reasons.append("吸筹扫单")
    if "washout_micro_base" in factor_labels:
        score += 6
        reasons.append("洗盘微底")
    if "high_volatility_reversal_base" in factor_labels:
        score += 3
        reasons.append("高波动反转")
    if "price_magnet_liquidity_sweep" in factor_labels:
        score += 8
        reasons.append("下杀拿流动性")
    if "short_cover_reclaim" in factor_labels:
        score += 6
        reasons.append("空头回补收回")
    if "confirmed_long_liquidation_sweep" in factor_labels:
        score += 10
        reasons.append("真实多头强平确认")
    if "confirmed_short_squeeze_reclaim" in factor_labels:
        score += 8
        reasons.append("真实空头强平确认")
    row["screen_score"] = score
    row["screen_reason"] = "；".join(reasons)
    row["factor_reason"] = factor_human_reason(row)
    if score >= 90:
        row["screen_level"] = "formal_watch"
    elif score >= 70:
        row["screen_level"] = "strong_candidate"
    elif score >= 55:
        row["screen_level"] = "candidate"
    else:
        row["screen_level"] = "radar"


def has_any_label(row, key, labels):
    current = set((row.get(key) or "").split("|"))
    return any(label in current for label in labels)


def add_trade_workbench_fields(row):
    score = hist.safe_float(row.get("screen_score"), 0.0) or 0.0
    p24 = hist.safe_float(row.get("price_change_24h"), 0.0) or 0.0
    from_low = hist.safe_float(row.get("from_24h_low"), 0.0) or 0.0
    pre2_range = hist.safe_float(row.get("pre2h_range"), 0.0) or 0.0
    pre2_oi = hist.safe_float(row.get("pre2h_oi_change"), 0.0) or 0.0
    cg_long_liq = hist.safe_float(row.get("cg_long_liq_1h_usd"), 0.0) or 0.0
    cg_short_liq = hist.safe_float(row.get("cg_short_liq_1h_usd"), 0.0) or 0.0
    dex_liq = hist.safe_float(row.get("dex_liquidity_usd"), 0.0) or 0.0
    factor_labels = set((row.get("factor_labels") or "").split("|"))
    contract_rules = set((row.get("contract_rules") or "").split("|"))
    clean_factor = bool(factor_labels - {"", "unclassified", "factor_error"})
    confirmed_liq = bool(
        factor_labels
        & {
            "confirmed_long_liquidation_sweep",
            "confirmed_short_squeeze_reclaim",
            "price_magnet_liquidity_sweep",
            "short_cover_reclaim",
        }
    )
    chain_ok = (
        row.get("dex_match_status") == "price_matched"
        and bool(row.get("token_address"))
        and not bool(row.get("has_binance_spot"))
        and not bool(row.get("has_hl_perp"))
    )
    already_extended = p24 >= 10 or from_low >= 18
    liquidity_warning = dex_liq > 0 and dex_liq < 50_000
    market_risk_off = row.get("market_env") == "risk_off"

    if not chain_ok:
        grade = "剔除"
        status = "不进妖币池"
        action = "只记录，不交易；链上/交易所条件不干净"
    elif "factor_error" in factor_labels:
        grade = "B级"
        status = "数据缺口"
        action = "等下一轮数据补齐，不主动下单"
    elif not clean_factor:
        grade = "B级" if score >= 85 else "剔除"
        status = "结构未确认"
        action = "只观察，等出现压缩、洗盘收回或清算确认"
    elif already_extended:
        grade = "B级"
        status = "已拉过"
        action = "不追高，只等回踩启动位不破"
    elif score >= 105 and (not market_risk_off or confirmed_liq):
        grade = "S级"
        status = "重点盯"
        action = "突破后回踩不破可试；若有清算确认可提高优先级"
    elif score >= 85:
        grade = "A级"
        status = "妖币潜力"
        action = "不追高，等放量突破或回踩不破"
    elif score >= 70:
        grade = "B级"
        status = "观察池"
        action = "等补一个确认信号，没确认不做"
    else:
        grade = "剔除"
        status = "弱观察"
        action = "暂不处理"

    confirmations = []
    if pre2_range <= 3:
        confirmations.append("仍在低波动压缩")
    else:
        confirmations.append("等重新压缩或回踩不破")
    if pre2_oi > 0:
        confirmations.append("OI温和增加")
    else:
        confirmations.append("等OI重新转正")
    if cg_long_liq >= 1_000 or cg_short_liq >= 1_000:
        confirmations.append("清算数据已有痕迹")
    else:
        confirmations.append("等真实清算/扫流动性确认")
    if "contract_long_4h_precise" in contract_rules:
        confirmations.append("4h合约已确认")

    risks = []
    if market_risk_off:
        risks.append("大盘偏弱，仓位降级")
    if liquidity_warning:
        risks.append("DEX流动性偏薄，地址/现货承接要复核")
    if already_extended:
        risks.append("24h已拉升，禁止追")
    if not clean_factor:
        risks.append("未匹配成熟妖币模板")
    if not risks:
        risks.append("若BTC/ETH转弱，降级观察")

    row["trade_grade"] = grade
    row["trade_status"] = status
    row["trade_action"] = action
    row["trade_confirmation"] = "；".join(dict.fromkeys(confirmations[:4]))
    row["trade_risk"] = "；".join(dict.fromkeys(risks[:4]))
    add_exit_short_fields(row)


def add_exit_short_fields(row):
    p24 = hist.safe_float(row.get("price_change_24h"), 0.0) or 0.0
    from_low = hist.safe_float(row.get("from_24h_low"), 0.0) or 0.0
    pre2_oi = hist.safe_float(row.get("pre2h_oi_change"), 0.0) or 0.0
    pre4_oi = hist.safe_float(row.get("pre4h_oi_change"), 0.0) or 0.0
    pre2_taker = hist.safe_float(row.get("pre2h_taker_buy_sell"))
    pre4_taker = hist.safe_float(row.get("pre4h_taker_buy_sell"))
    funding = hist.safe_float(row.get("cg_funding_latest"))
    cg_long_liq = hist.safe_float(row.get("cg_long_liq_1h_usd"), 0.0) or 0.0
    cg_short_liq = hist.safe_float(row.get("cg_short_liq_1h_usd"), 0.0) or 0.0
    cg_oi_1h = hist.safe_float(row.get("cg_oi_change_1h"))
    cg_taker = hist.safe_float(row.get("cg_taker_buy_sell_1h"))
    factor_labels = set((row.get("factor_labels") or "").split("|"))

    score = 0
    reasons = []
    if p24 >= 18:
        score += 25
        reasons.append("24h涨幅过大")
    elif p24 >= 10:
        score += 15
        reasons.append("24h涨幅偏大")
    if from_low >= 35:
        score += 20
        reasons.append("距24h低点太远")
    elif from_low >= 20:
        score += 12
        reasons.append("离低位已远")
    if pre2_oi >= 10 or pre4_oi >= 14:
        score += 20
        reasons.append("高位OI继续堆")
    elif pre2_oi >= 6 or pre4_oi >= 8:
        score += 12
        reasons.append("OI升温")
    if pre2_taker is not None and pre2_taker < 0.9:
        score += 15
        reasons.append("主动买盘转弱")
    elif pre4_taker is not None and pre4_taker < 0.9:
        score += 10
        reasons.append("4h主动买盘偏弱")
    if funding is not None and funding >= 0.08:
        score += 15
        reasons.append("资金费率过热")
    elif funding is not None and funding >= 0.03:
        score += 8
        reasons.append("资金费率偏热")
    if cg_long_liq >= 20_000 and cg_short_liq < cg_long_liq * 0.5:
        score += 18
        reasons.append("真实多头强平增多")
    if cg_oi_1h is not None and cg_oi_1h < -5 and p24 > 0:
        score += 12
        reasons.append("上涨中OI回落，偏止盈/回补")
    if cg_taker is not None and cg_taker < 0.95:
        score += 10
        reasons.append("CoinGlass主卖偏强")
    if "confirmed_short_squeeze_reclaim" in factor_labels and p24 >= 10:
        score += 10
        reasons.append("逼空后进入冲高段")

    if score >= 70:
        level = "逃顶"
        action = "已有顶部/派发风险，持仓优先减，反抽不过高点再看空"
        short_bias = "可观察做空"
    elif score >= 45:
        level = "减仓"
        action = "停止追多，持仓降仓，等回踩是否守住启动位"
        short_bias = "等反抽确认"
    elif score >= 25:
        level = "过热观察"
        action = "不加仓，等下一轮OI和主动买盘确认"
        short_bias = "暂不做空"
    else:
        level = "未见顶部"
        action = "按原妖币计划观察，仍需等多头确认"
        short_bias = "不做空"

    row["exit_risk_score"] = score
    row["exit_risk_level"] = level
    row["exit_action"] = action
    row["short_bias"] = short_bias
    row["exit_reason"] = "；".join(dict.fromkeys(reasons[:5])) if reasons else "暂无明显顶部证据"


def fmt_pct(value):
    value = hist.safe_float(value)
    return "NA" if value is None else f"{value:.2f}%"


def fmt_m(value):
    value = hist.safe_float(value)
    return "NA" if value is None else f"{value / 1_000_000:.2f}m"


def format_workbench_row(row):
    return (
        f"{row['symbol'].replace('USDT', '')}｜{row.get('trade_grade')}｜{row.get('trade_status')}\n"
        f"结构: {row.get('factor_reason') or '暂无'}；评分 {row.get('screen_score')}；市值 {fmt_m(row.get('estimated_event_size'))}；Top10 {fmt_pct(row.get('holder_top10_adjusted_pct'))}\n"
        f"确认: {row.get('trade_confirmation')}\n"
        f"操作: {row.get('trade_action')}\n"
        f"风险: {row.get('trade_risk')}\n"
        f"逃顶/做空: {row.get('exit_risk_level')}｜{row.get('short_bias')}｜{row.get('exit_reason')}\n"
        f"数据: 24h {fmt_pct(row.get('price_change_24h'))}，距低点 {fmt_pct(row.get('from_24h_low'))}，OI2h {fmt_pct(row.get('pre2h_oi_change'))}，"
        f"清算L/S {fmt_m(row.get('cg_long_liq_1h_usd'))}/{fmt_m(row.get('cg_short_liq_1h_usd'))}"
    )


def format_exit_radar_row(row):
    return (
        f"{row['symbol'].replace('USDT', '')}｜{row.get('exit_risk_level')}｜{row.get('short_bias')}\n"
        f"原因: {row.get('exit_reason')}\n"
        f"动作: {row.get('exit_action')}\n"
        f"数据: 24h {fmt_pct(row.get('price_change_24h'))}，距低点 {fmt_pct(row.get('from_24h_low'))}，"
        f"OI2h {fmt_pct(row.get('pre2h_oi_change'))}，OI4h {fmt_pct(row.get('pre4h_oi_change'))}，"
        f"主买2h {hist.safe_float(row.get('pre2h_taker_buy_sell'), 0):.2f}"
    )


def build_report(rows, uncertain, args, client):
    selected = [row for row in rows if row.get("screen_score", 0) >= args.min_score]
    selected.sort(key=lambda row: (hist.safe_float(row.get("screen_score"), 0.0) or 0.0), reverse=True)
    workbench = [row for row in rows if row.get("trade_grade") in {"S级", "A级", "B级"}]
    grade_order = {"S级": 3, "A级": 2, "B级": 1}
    workbench.sort(key=lambda row: (grade_order.get(row.get("trade_grade"), 0), hist.safe_float(row.get("screen_score"), 0.0) or 0.0), reverse=True)
    lines = []
    lines.append("[MOONSHOT TRADING WORKBENCH]")
    lines.append(f"time={hist.utc_now().isoformat()} rows={len(rows)} selected={len(selected)} min_score={args.min_score}")
    lines.append(f"cache hit={client.hits} miss={client.misses} requests={client.requests} errors={client.errors}")
    lines.append("mode: only trade mainstream direction + moonshot-potential names. Grades are S/A/B/reject; use confirmations, not blind chasing.")
    lines.append("")
    lines.append("Workbench:")
    for grade in ("S级", "A级", "B级"):
        grade_rows = [row for row in workbench if row.get("trade_grade") == grade]
        if not grade_rows:
            continue
        lines.append(f"[{grade}]")
        for row in grade_rows[:12]:
            lines.append(format_workbench_row(row))
            lines.append("")
    if not workbench:
        lines.append("暂无 S/A/B 妖币候选。")
        lines.append("")
    exit_rows = [
        row for row in rows
        if hist.safe_float(row.get("exit_risk_score"), 0.0) >= 25
    ]
    exit_rows.sort(key=lambda row: hist.safe_float(row.get("exit_risk_score"), 0.0) or 0.0, reverse=True)
    lines.append("Exit / Short Radar:")
    if exit_rows:
        for row in exit_rows[:12]:
            lines.append(format_exit_radar_row(row))
            lines.append("")
    else:
        lines.append("暂无明确逃顶/做空候选。")
        lines.append("")
    lines.append("Raw Selected:")
    for row in selected[:50]:
        lines.append(
            f"- {row['symbol']} level={row.get('screen_level')} score={row.get('screen_score')} "
            f"grade={row.get('trade_grade')} status={row.get('trade_status')} "
            f"exit={row.get('exit_risk_level')} short={row.get('short_bias')} "
            f"size={fmt_m(row.get('estimated_event_size'))} top10={fmt_pct(row.get('holder_top10_adjusted_pct'))} "
            f"chain={row.get('dex_chain') or 'NA'} bsc={row.get('has_bsc_pair')} liq={fmt_m(row.get('dex_liquidity_usd'))} "
            f"p24={fmt_pct(row.get('price_change_24h'))} fromLow={fmt_pct(row.get('from_24h_low'))} "
            f"volr={hist.safe_float(row.get('volume_ratio_24h'), 0):.2f} "
            f"factor={row.get('factor_labels')} mkt={row.get('market_env') or 'NA'} "
            f"pre2r={fmt_pct(row.get('pre2h_range'))} oi2={fmt_pct(row.get('pre2h_oi_change'))} "
            f"liq1h L/S={fmt_m(row.get('cg_long_liq_1h_usd'))}/{fmt_m(row.get('cg_short_liq_1h_usd'))} "
            f"pre6r={fmt_pct(row.get('pre6h_range'))} "
            f"labels={row.get('current_screen_labels')} contract={row.get('contract_rules')} "
            f"reason={row.get('screen_reason')} factor_reason={row.get('factor_reason')} "
            f"address={row.get('token_address') or 'NA'}"
        )
    lines.append("")
    lines.append("Needs Address Confirmation:")
    for row in uncertain[:30]:
        lines.append(
            f"- {row['symbol']} base={row.get('base')} dex_status={row.get('dex_match_status')} "
            f"chains={row.get('dex_exact_chains') or 'NA'} selected_chain={row.get('dex_chain') or 'NA'} "
            f"price_ratio={row.get('dex_price_ratio_distance') or 'NA'} token={row.get('token_name') or 'NA'} "
            f"address={row.get('token_address') or 'NA'} p24={fmt_pct(row.get('price_change_24h'))} "
            f"fromLow={fmt_pct(row.get('from_24h_low'))} volr={hist.safe_float(row.get('volume_ratio_24h'), 0):.2f} "
            f"factor={row.get('factor_labels')} liq1h L/S={fmt_m(row.get('cg_long_liq_1h_usd'))}/{fmt_m(row.get('cg_short_liq_1h_usd'))} "
            f"factor_reason={row.get('factor_reason')}"
        )
    return "\n".join(lines) + "\n"


def discord_env_value(name):
    value = os.environ.get(name, "")
    if value:
        return value.strip().strip("'\"")
    env_file = Path("/etc/crypto-monitor.env")
    try:
        for line in env_file.read_text(encoding="utf-8").splitlines():
            raw = line.strip()
            if not raw or raw.startswith("#") or "=" not in raw:
                continue
            key, raw_value = raw.split("=", 1)
            if key.strip() == name:
                return raw_value.strip().strip("'\"")
    except Exception:
        return ""
    return ""


def discord_moonshot_channel_id():
    for name in (
        "DISCORD_MOONSHOT_CHANNEL_ID",
        "DISCORD_ALT_WATCH_CHANNEL_ID",
        "DISCORD_OBSERVE_CHANNEL_ID",
        "DISCORD_ALERTS_CHANNEL_ID",
    ):
        value = discord_env_value(name)
        if value:
            return value
    return ""


def report_publish_signature(rows):
    signature_version = "discord_summary_v2"
    workbench = [row for row in rows if row.get("trade_grade") in {"S级", "A级", "B级"}]
    grade_order = {"S级": 3, "A级": 2, "B级": 1}
    workbench.sort(key=lambda row: (grade_order.get(row.get("trade_grade"), 0), hist.safe_float(row.get("screen_score"), 0.0) or 0.0), reverse=True)
    payload = {
        "version": signature_version,
        "items": [
            {
                "symbol": row.get("symbol"),
                "grade": row.get("trade_grade"),
                "status": row.get("trade_status"),
                "score": round(hist.safe_float(row.get("screen_score"), 0.0) or 0.0, 1),
                "exit": row.get("exit_risk_level"),
                "short": row.get("short_bias"),
            }
            for row in workbench[:12]
        ],
    }
    return hashlib.sha256(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()


def discord_workbench_line(row):
    symbol = str(row.get("symbol") or "-").replace("USDT", "")
    parts = [
        f"**{symbol}** {row.get('trade_grade')} {row.get('trade_status')}",
        f"分{row.get('screen_score')}",
        f"市值{fmt_m(row.get('estimated_event_size'))}",
        f"Top10 {fmt_pct(row.get('holder_top10_adjusted_pct'))}",
        f"24h {fmt_pct(row.get('price_change_24h'))}",
        f"OI2h {fmt_pct(row.get('pre2h_oi_change'))}",
    ]
    return " | ".join(parts) + f"\n确认：{row.get('trade_confirmation')}\n操作：{row.get('trade_action')}\n风险：{row.get('trade_risk')}"


def discord_exit_line(row):
    symbol = str(row.get("symbol") or "-").replace("USDT", "")
    return (
        f"**{symbol}** {row.get('exit_risk_level')} / {row.get('short_bias')} | "
        f"24h {fmt_pct(row.get('price_change_24h'))} | OI2h {fmt_pct(row.get('pre2h_oi_change'))} | "
        f"主买2h {hist.safe_float(row.get('pre2h_taker_buy_sell'), 0):.2f}\n"
        f"{row.get('exit_reason')}；{row.get('exit_action')}"
    )


def discord_join_lines(lines, limit=950):
    kept = []
    size = 0
    for line in lines:
        candidate_size = size + len(line) + (2 if kept else 0)
        if candidate_size > limit:
            break
        kept.append(line)
        size = candidate_size
    return "\n\n".join(kept)


def discord_report_fields(rows):
    workbench = [row for row in rows if row.get("trade_grade") in {"S级", "A级", "B级"}]
    grade_order = {"S级": 3, "A级": 2, "B级": 1}
    workbench.sort(key=lambda row: (grade_order.get(row.get("trade_grade"), 0), hist.safe_float(row.get("screen_score"), 0.0) or 0.0), reverse=True)
    exit_rows = [row for row in rows if hist.safe_float(row.get("exit_risk_score"), 0.0) >= 25]
    exit_rows.sort(key=lambda row: hist.safe_float(row.get("exit_risk_score"), 0.0) or 0.0, reverse=True)

    fields = []
    for grade in ("S级", "A级", "B级"):
        grade_rows = [row for row in workbench if row.get("trade_grade") == grade]
        if not grade_rows:
            continue
        value = discord_join_lines([discord_workbench_line(row) for row in grade_rows[:4]])
        if value:
            fields.append({"name": f"{grade}候选", "value": value, "inline": False})
    exit_value = discord_join_lines([discord_exit_line(row) for row in exit_rows[:5]])
    if exit_value:
        fields.append({"name": "过热 / 暂不做空", "value": exit_value, "inline": False})
    fields.append({"name": "完整报告", "value": "`!妖币` 查看完整工作台；`!山寨` 查看实时观察队列。", "inline": False})
    return fields or [{"name": "状态", "value": "当前暂无妖币候选。", "inline": False}]


def publish_report_to_discord(report, rows, out_dir):
    if discord_env_value("DISCORD_ENABLED").lower() not in {"1", "true", "yes", "on"}:
        return
    bot_token = discord_env_value("DISCORD_BOT_TOKEN")
    channel_id = discord_moonshot_channel_id()
    if not bot_token or not channel_id:
        print("Discord moonshot publish skipped: token/channel missing", flush=True)
        return

    state_path = Path(out_dir) / "current_moonshot_screen_discord_state.json"
    signature = report_publish_signature(rows)
    try:
        state = json.loads(state_path.read_text(encoding="utf-8")) if state_path.exists() else {}
    except Exception:
        state = {}
    if state.get("signature") == signature:
        print("Discord moonshot publish skipped: unchanged", flush=True)
        return

    payload = {
        "embeds": [
            {
                "title": "🟣 妖币交易工作台",
                "description": "S/A/B 妖币候选；只做观察和确认，不盲追。",
                "color": 0x9B59B6,
                "fields": discord_report_fields(rows),
                "timestamp": hist.utc_now().isoformat(),
            }
        ]
    }
    url = f"{DISCORD_API_BASE}/channels/{channel_id}/messages"
    response = requests.post(
        url,
        headers={"Authorization": f"Bot {bot_token}", "Content-Type": "application/json"},
        json=payload,
        timeout=15,
    )
    if response.status_code >= 300:
        print(f"Discord moonshot publish failed: {response.status_code} {response.text[:300]}", flush=True)
        return
    state_path.write_text(
        json.dumps({"signature": signature, "published_at": hist.utc_now().isoformat()}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"Discord moonshot report sent: channel={channel_id[-4:]}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Current offline screen for controlled moonshot candidates.")
    parser.add_argument("--days", type=int, default=60)
    parser.add_argument("--cache-dir", default=".cache/alt_moonshots")
    parser.add_argument("--dex-cache-dir", default=".cache/alt_moonshots_dex")
    parser.add_argument("--coinglass-cache-dir", default=".cache/coinglass")
    parser.add_argument("--out-dir", default="reports/alt_moonshots")
    parser.add_argument("--sleep", type=float, default=0.03)
    parser.add_argument("--min-score", type=int, default=55)
    parser.add_argument("--no-network", action="store_true")
    args = parser.parse_args()

    client = hist.BinanceCache(args.cache_dir, sleep_seconds=0.05, no_network=args.no_network)
    coinglass_client = cg.CoinGlassClient(args.coinglass_cache_dir, sleep_seconds=0.05, no_network=args.no_network)
    end = hist.utc_now().replace(minute=0, second=0, microsecond=0)
    start = end - dt.timedelta(days=args.days)
    spot_bases, _ = load_spot_bases()
    hl_symbols = load_hl_universe()
    futures_prices = binance_futures_prices()
    rows = []
    symbols = hist.futures_symbols(client)
    for idx, symbol in enumerate(symbols, start=1):
        try:
            klines = hist.fetch_klines_1h(client, symbol, start, end)
            row = current_row_from_klines(symbol, klines, end)
            if not row:
                continue
            if not row.get("profile_labels"):
                continue
            add_chain_holder_profile(row, spot_bases, hl_symbols, futures_prices, args.dex_cache_dir, args.sleep)
            controlled_labels(row)
            add_current_factor_profile(client, row, coinglass_client)
            if any(label in (row.get("current_screen_labels") or "").split("|") for label in ("alpha_nohl_size_holder60", "alpha_nohl_size_holder80")):
                enrich_validation_context(client, row)
                row["contract_rules"] = "|".join(rule_hits(row))
            score_row(row)
            add_trade_workbench_fields(row)
            rows.append(row)
        except Exception:
            continue
        if idx % 100 == 0:
            print(f"scanned={idx}/{len(symbols)} candidate_rows={len(rows)} requests={client.requests}", flush=True)

    uncertain = [
        row for row in rows
        if row.get("has_binance_spot") is False
        and row.get("has_hl_perp") is False
        and row.get("profile_labels")
        and (
            row.get("dex_match_status") != "price_matched"
            or not row.get("token_address")
            or hist.safe_float(row.get("dex_liquidity_usd"), 0.0) < 50_000
        )
    ]
    uncertain.sort(key=lambda row: hist.safe_float(row.get("hist_total_range_60d"), 0.0) or 0.0, reverse=True)

    out_dir = Path(args.out_dir)
    csv_path = out_dir / "current_moonshot_screen.csv"
    txt_path = out_dir / "current_moonshot_screen.txt"
    latest_path = out_dir / "latest_current_moonshot_screen.txt"
    hist.write_csv(csv_path, rows)
    report = build_report(rows, uncertain, args, client)
    txt_path.write_text(report, encoding="utf-8")
    latest_path.write_text(report, encoding="utf-8")
    publish_report_to_discord(report, rows, out_dir)
    print(report)
    print(f"wrote {csv_path}")
    print(f"wrote {txt_path}")


if __name__ == "__main__":
    main()
