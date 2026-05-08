#!/usr/bin/env python3
import csv
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


WORKDIR = Path("/opt/crypto-monitor")
STATE_PATH = WORKDIR / "trade_ideas_state.json"
REPORT_PATH = WORKDIR / "reports" / "trade_ideas_latest.txt"
SCAN_LEDGER_PATH = WORKDIR / "reports" / "trade_ideas_scan_ledger.csv"
IDEA_LEDGER_PATH = WORKDIR / "reports" / "trade_ideas_idea_ledger.csv"
BINANCE_FAPI = "https://fapi.binance.com"

MIN_QUOTE_VOLUME = float(os.getenv("TRADE_IDEAS_MIN_QUOTE_VOLUME", "20000000"))
MIN_RR = float(os.getenv("TRADE_IDEAS_MIN_RR", "2.0"))
MIN_REWARD_PCT = float(os.getenv("TRADE_IDEAS_MIN_REWARD_PCT", "10.0"))
MAX_IDEAS_PER_SIDE = int(os.getenv("TRADE_IDEAS_MAX_PER_SIDE", "3"))
COOLDOWN_SECONDS = int(os.getenv("TRADE_IDEAS_COOLDOWN_SECONDS", "1800"))
SCAN_TOP_N = int(os.getenv("TRADE_IDEAS_SCAN_TOP_N", "90"))
CHANNEL_ENV = os.getenv("TRADE_IDEAS_CHANNEL_ENV", "DISCORD_OBSERVE_CHANNEL_ID")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-trade-ideas/1.0"})


def load_env_file(path="/etc/crypto-monitor.env"):
    p = Path(path)
    if not p.exists():
        return
    try:
        lines = p.read_text(encoding="utf-8", errors="ignore").splitlines()
    except PermissionError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def fnum(x, digits=6):
    try:
        x = float(x)
    except Exception:
        return "n/a"
    if abs(x) >= 100:
        return f"{x:.2f}".rstrip("0").rstrip(".")
    if abs(x) >= 1:
        return f"{x:.4f}".rstrip("0").rstrip(".")
    return f"{x:.{digits}f}".rstrip("0").rstrip(".")


def pct(x):
    try:
        return f"{float(x):+.2f}%"
    except Exception:
        return "n/a"


def get_json(path, params=None, timeout=12):
    r = SESSION.get(BINANCE_FAPI + path, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def discord_send(title, description="", fields=None, color=0xF1C40F):
    token = os.environ.get("DISCORD_BOT_TOKEN")
    channel_id = os.environ.get(CHANNEL_ENV) or os.environ.get("DISCORD_OBSERVE_CHANNEL_ID")
    if not token or not channel_id:
        print("Discord token/channel missing")
        return False
    embed = {
        "title": title[:256],
        "description": description[:3800],
        "color": color,
        "timestamp": now_iso(),
        "fields": [],
    }
    for name, value, inline in fields or []:
        embed["fields"].append({"name": str(name)[:256], "value": str(value)[:1024] or "-", "inline": bool(inline)})
    r = SESSION.post(
        f"https://discord.com/api/v10/channels/{channel_id}/messages",
        headers={"Authorization": f"Bot {token}"},
        json={"embeds": [embed], "allowed_mentions": {"parse": []}},
        timeout=20,
    )
    if r.status_code not in (200, 201, 204):
        print("Discord send failed", r.status_code, r.text[:300])
        return False
    return True


def load_state():
    try:
        return json.loads(STATE_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {"sent": {}}


def save_state(state):
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def append_csv_row(path, fieldnames, row):
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in fieldnames})


def append_scan_ledger(scan_ts, stance, guard_line, core_lines, longs_count, shorts_count, selected_count, sent):
    append_csv_row(
        SCAN_LEDGER_PATH,
        [
            "scan_time_ts",
            "scan_time",
            "stance",
            "guard_line",
            "core_lines",
            "longs_count",
            "shorts_count",
            "selected_count",
            "sent",
        ],
        {
            "scan_time_ts": scan_ts,
            "scan_time": datetime.fromtimestamp(scan_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "stance": stance,
            "guard_line": guard_line,
            "core_lines": core_lines,
            "longs_count": longs_count,
            "shorts_count": shorts_count,
            "selected_count": selected_count,
            "sent": int(bool(sent)),
        },
    )


def append_idea_ledger(scan_ts, stance, guard_line, item, sent):
    entry_low, entry_high, stop, target1, target2, rr = item["plan"]
    append_csv_row(
        IDEA_LEDGER_PATH,
        [
            "scan_time_ts",
            "scan_time",
            "symbol",
            "side",
            "sent",
            "last",
            "entry_low",
            "entry_high",
            "stop",
            "target1",
            "target2",
            "rr",
            "reward_pct",
            "score",
            "ch24",
            "pos",
            "trend",
            "oi_chg",
            "funding",
            "buy_ratio",
            "ls_ratio",
            "quote_volume",
            "support",
            "resistance",
            "stance",
            "guard_line",
        ],
        {
            "scan_time_ts": scan_ts,
            "scan_time": datetime.fromtimestamp(scan_ts).strftime("%Y-%m-%d %H:%M:%S"),
            "symbol": item.get("symbol"),
            "side": item.get("side"),
            "sent": int(bool(sent)),
            "last": item.get("last"),
            "entry_low": entry_low,
            "entry_high": entry_high,
            "stop": stop,
            "target1": target1,
            "target2": target2,
            "rr": rr,
            "reward_pct": item.get("reward_pct", 0),
            "score": item.get("score"),
            "ch24": item.get("ch24"),
            "pos": item.get("pos"),
            "trend": item.get("trend"),
            "oi_chg": item.get("oi_chg"),
            "funding": item.get("funding"),
            "buy_ratio": item.get("buy_ratio"),
            "ls_ratio": item.get("ls_ratio"),
            "quote_volume": item.get("quote_volume"),
            "support": item.get("support"),
            "resistance": item.get("resistance"),
            "stance": stance,
            "guard_line": guard_line,
        },
    )


def market_view():
    rows = []
    weak = 0
    strong = 0
    btc_last = None
    btc_guard = None
    btc_ma20 = None
    for sym in ("BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT"):
        try:
            t = get_json("/fapi/v1/ticker/24hr", {"symbol": sym})
            kl = get_json("/fapi/v1/klines", {"symbol": sym, "interval": "15m", "limit": 24})
            closes = [float(x[4]) for x in kl]
            lows = [float(x[3]) for x in kl]
            last = float(t["lastPrice"])
            ma20 = sum(closes[-20:]) / 20
            ch = float(t["priceChangePercent"])
            if sym == "BTCUSDT":
                btc_last = last
                btc_ma20 = ma20
                recent_low = min(lows[-12:])
                btc_guard = round(recent_low / 100) * 100
            if last >= ma20 and ch >= -1:
                strong += 1
            if last < ma20 and ch < 0:
                weak += 1
            rows.append(f"{sym.replace('USDT','')}: {fnum(last)} | 24h {pct(ch)} | 15mMA {'上方' if last >= ma20 else '下方'}")
        except Exception:
            continue
    if weak >= 3:
        stance = "偏弱，只做高盈亏比，小仓，追高不做"
    elif strong >= 3:
        stance = "偏强，允许顺势多，但仍要等回踩/突破确认"
    else:
        stance = "震荡，优先条件单，低盈亏比不做"

    guard_line = "注意大盘: BTC 短线方向不清，山寨按条件区执行，不追单。"
    if btc_last and btc_guard:
        distance = (btc_last / btc_guard - 1) * 100 if btc_guard else 0
        if weak >= 3 or (btc_ma20 and btc_last < btc_ma20):
            guard_line = (
                f"注意大盘: BTC 现在偏弱，{fnum(btc_guard, 0)} 附近不能跌破太多。"
                "如果 BTC 跌破并持续收不回，山寨多单都要降仓或不做。"
            )
        elif strong >= 3:
            guard_line = (
                f"注意大盘: BTC 站在15m均线上方，短线防守看 {fnum(btc_guard, 0)} 附近。"
                "只做回踩不破或突破确认，跌回去就降低多单仓位。"
            )
        elif distance <= 0.6:
            guard_line = (
                f"注意大盘: BTC 离 {fnum(btc_guard, 0)} 防守位很近，山寨多单要等 BTC 收回后再做。"
            )
        else:
            guard_line = (
                f"注意大盘: BTC 防守位看 {fnum(btc_guard, 0)} 附近；"
                "跌破并持续收不回，山寨多单降仓或不做。"
            )
    return stance, "\n".join(rows), guard_line


def symbol_universe():
    info = get_json("/fapi/v1/exchangeInfo", timeout=20)
    valid = {
        x["symbol"]
        for x in info.get("symbols", [])
        if x.get("contractType") == "PERPETUAL"
        and x.get("quoteAsset") == "USDT"
        and x.get("status") == "TRADING"
    }
    tickers = get_json("/fapi/v1/ticker/24hr", timeout=20)
    rows = []
    for item in tickers:
        sym = item.get("symbol")
        if sym not in valid:
            continue
        quote = float(item.get("quoteVolume") or 0)
        ch = float(item.get("priceChangePercent") or 0)
        if quote >= MIN_QUOTE_VOLUME and -12 <= ch <= 18:
            rows.append((quote, sym, ch))
    rows.sort(reverse=True)
    return rows[:SCAN_TOP_N]


def rr_plan_long(last, support, resistance):
    entry_low = support + (last - support) * 0.35
    entry_high = last
    stop = support * 0.997
    target1 = resistance
    target2 = resistance + (resistance - support) * 0.35
    risk = max(last - stop, 0)
    reward = max(target1 - last, 0)
    rr = reward / risk if risk > 0 else 0
    return entry_low, entry_high, stop, target1, target2, rr


def rr_plan_short(last, resistance, support):
    entry_low = last
    entry_high = resistance - (resistance - last) * 0.35
    stop = resistance * 1.003
    target1 = support
    target2 = support - (resistance - support) * 0.35
    risk = max(stop - last, 0)
    reward = max(last - target1, 0)
    rr = reward / risk if risk > 0 else 0
    return entry_low, entry_high, stop, target1, target2, rr


def plan_reward_pct(item):
    entry_low, entry_high, _stop, _target1, target2, _rr = item["plan"]
    last = float(item["last"] or 0)
    if last <= 0:
        return 0.0
    if item["side"] == "LONG":
        return max((target2 - last) / last * 100, 0.0)
    return max((last - target2) / last * 100, 0.0)


def analyze_symbol(sym, ch24, quote_volume):
    kl15 = get_json("/fapi/v1/klines", {"symbol": sym, "interval": "15m", "limit": 96})
    kl1h = get_json("/fapi/v1/klines", {"symbol": sym, "interval": "1h", "limit": 72})
    prem = get_json("/fapi/v1/premiumIndex", {"symbol": sym})
    oih = get_json("/futures/data/openInterestHist", {"symbol": sym, "period": "15m", "limit": 20})
    taker = get_json("/futures/data/takerlongshortRatio", {"symbol": sym, "period": "15m", "limit": 12})
    global_ls = get_json("/futures/data/globalLongShortAccountRatio", {"symbol": sym, "period": "15m", "limit": 12})

    closes = [float(x[4]) for x in kl15]
    highs = [float(x[2]) for x in kl15]
    lows = [float(x[3]) for x in kl15]
    vols = [float(x[7]) for x in kl15]
    h1c = [float(x[4]) for x in kl1h]
    h1h = [float(x[2]) for x in kl1h]
    h1l = [float(x[3]) for x in kl1h]

    last = closes[-1]
    ma15_20 = sum(closes[-20:]) / 20
    ma15_50 = sum(closes[-50:]) / 50
    ma1h20 = sum(h1c[-20:]) / 20
    ma1h50 = sum(h1c[-50:]) / 50
    hi24 = max(h1h[-24:])
    lo24 = min(h1l[-24:])
    pos = (last - lo24) / (hi24 - lo24) * 100 if hi24 > lo24 else 50
    trend = (ma1h20 / ma1h50 - 1) * 100 if ma1h50 else 0
    support = min(lows[-12:])
    resistance = max(highs[-24:])
    vol_ratio = vols[-1] / (sum(vols[-20:]) / 20) if sum(vols[-20:]) else 0
    funding = float(prem.get("lastFundingRate") or 0) * 100
    oi_chg = 0.0
    if isinstance(oih, list) and len(oih) >= 8:
        oi_chg = (float(oih[-1]["sumOpenInterest"]) / float(oih[-8]["sumOpenInterest"]) - 1) * 100
    buy_ratio = 1.0
    if isinstance(taker, list) and taker:
        buy = sum(float(x.get("buyVol") or 0) for x in taker[-4:])
        sell = sum(float(x.get("sellVol") or 0) for x in taker[-4:])
        buy_ratio = buy / sell if sell else 9
    ls_ratio = float(global_ls[-1]["longShortRatio"]) if isinstance(global_ls, list) and global_ls else 1.0

    long_plan = rr_plan_long(last, support, resistance)
    short_plan = rr_plan_short(last, resistance, support)

    long_score = 0
    if trend > 0.2:
        long_score += 2
    if last >= ma1h20:
        long_score += 1
    if ma15_20 >= ma15_50:
        long_score += 1
    if 35 <= pos <= 78:
        long_score += 2
    elif pos > 88:
        long_score -= 2
    if -1 <= oi_chg <= 8:
        long_score += 1
    if funding < 0.03:
        long_score += 1
    if buy_ratio >= 1.05:
        long_score += 1
    if ch24 > 0:
        long_score += 1
    if long_plan[-1] >= MIN_RR:
        long_score += 2

    short_score = 0
    if trend < -0.2:
        short_score += 2
    if last <= ma1h20:
        short_score += 1
    if ma15_20 <= ma15_50:
        short_score += 1
    if 25 <= pos <= 70:
        short_score += 1
    elif pos >= 78:
        short_score += 2
    if -1 <= oi_chg <= 8:
        short_score += 1
    if funding > -0.02:
        short_score += 1
    if buy_ratio <= 0.95:
        short_score += 1
    if ls_ratio >= 1.8:
        short_score += 1
    if ch24 < 0:
        short_score += 1
    if short_plan[-1] >= MIN_RR:
        short_score += 2

    common = {
        "symbol": sym,
        "last": last,
        "ch24": ch24,
        "quote_volume": quote_volume,
        "pos": pos,
        "trend": trend,
        "oi_chg": oi_chg,
        "funding": funding,
        "buy_ratio": buy_ratio,
        "ls_ratio": ls_ratio,
        "vol_ratio": vol_ratio,
        "support": support,
        "resistance": resistance,
    }
    long_item = dict(common, side="LONG", score=long_score, plan=long_plan, rr=long_plan[-1])
    short_item = dict(common, side="SHORT", score=short_score, plan=short_plan, rr=short_plan[-1])
    long_item["reward_pct"] = plan_reward_pct(long_item)
    short_item["reward_pct"] = plan_reward_pct(short_item)
    return long_item, short_item


def idea_field(item):
    entry_low, entry_high, stop, target1, target2, rr = item["plan"]
    side = "做多" if item["side"] == "LONG" else "做空"
    reason = (
        f"24h {pct(item['ch24'])} | 位置 {item['pos']:.0f}% | "
        f"OI15m {pct(item['oi_chg'])} | 费率 {item['funding']:+.4f}% | "
        f"主动买卖 {item['buy_ratio']:.2f} | 多空账户 {item['ls_ratio']:.2f}"
    )
    value = (
        f"{side}区: {fnum(entry_low)} - {fnum(entry_high)}\n"
        f"止损: {fnum(stop)}\n"
        f"目标: {fnum(target1)} / {fnum(target2)}\n"
        f"盈亏比: 1:{rr:.2f} | 目标空间: {item.get('reward_pct', 0):.2f}%\n"
        f"{reason}"
    )
    return item["symbol"], value, False


def dedupe_key(items):
    return "|".join(f"{x['side']}:{x['symbol']}" for x in items)


def main():
    load_env_file()
    state = load_state()
    stance, core_lines, guard_line = market_view()
    longs = []
    shorts = []
    for _quote, sym, ch in symbol_universe():
        try:
            long_item, short_item = analyze_symbol(sym, ch, _quote)
            if long_item["rr"] >= MIN_RR and long_item.get("reward_pct", 0) >= MIN_REWARD_PCT and long_item["score"] >= 9:
                longs.append(long_item)
            if short_item["rr"] >= MIN_RR and short_item.get("reward_pct", 0) >= MIN_REWARD_PCT and short_item["score"] >= 9:
                shorts.append(short_item)
        except Exception:
            continue
        time.sleep(0.05)
    longs.sort(key=lambda x: (x["score"], x["rr"], x["quote_volume"]), reverse=True)
    shorts.sort(key=lambda x: (x["score"], x["rr"], x["quote_volume"]), reverse=True)
    selected = longs[:MAX_IDEAS_PER_SIDE] + shorts[:MAX_IDEAS_PER_SIDE]

    lines = [
        f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"大盘观点: {stance}",
        guard_line,
        "",
        core_lines or "-",
        "",
        f"筛选规则: R/R >= 1:{MIN_RR:.1f}，目标空间 >= {MIN_REWARD_PCT:.1f}%；低利润不推；入场以条件区为准，不追单。",
    ]
    fields = []
    if selected:
        if longs[:MAX_IDEAS_PER_SIDE]:
            fields.extend([idea_field(x) for x in longs[:MAX_IDEAS_PER_SIDE]])
        if shorts[:MAX_IDEAS_PER_SIDE]:
            fields.extend([idea_field(x) for x in shorts[:MAX_IDEAS_PER_SIDE]])
    else:
        fields.append(("当前结论", "没有达到盈亏比和确认条件的交易候选，静默等待。", False))

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines) + "\n\n" + "\n\n".join(f"{n}\n{v}" for n, v, _ in fields), encoding="utf-8")

    key = dedupe_key(selected)
    now = time.time()
    last_key = state.get("last_key")
    last_sent = float(state.get("last_sent", 0) or 0)
    should_send = bool(selected) and (key != last_key or now - last_sent >= COOLDOWN_SECONDS)
    sent_ok = False
    if should_send:
        sent_ok = discord_send(
            "🎯 合约交易候选：高盈亏比多空计划",
            "\n".join(lines),
            fields,
            color=0x2ECC71 if longs and not shorts else 0xE67E22 if longs and shorts else 0xE74C3C,
        )
        if sent_ok:
            state["last_key"] = key
            state["last_sent"] = now
    append_scan_ledger(now, stance, guard_line, core_lines, len(longs), len(shorts), len(selected), sent_ok)
    for item in selected:
        append_idea_ledger(now, stance, guard_line, item, sent_ok)
    state["last_scan"] = now
    state["last_candidates"] = [
        {"symbol": x["symbol"], "side": x["side"], "rr": x["rr"], "reward_pct": x.get("reward_pct", 0), "score": x["score"]}
        for x in selected
    ]
    save_state(state)
    print(REPORT_PATH)
    print(f"longs={len(longs)} shorts={len(shorts)} selected={len(selected)} sent={sent_ok}")


if __name__ == "__main__":
    main()
