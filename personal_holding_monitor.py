#!/usr/bin/env python3
import json
import logging
import os
import time
import math
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

TOKEN_NAME = "Sentio"
TOKEN_SYMBOL = "ST"
TOKEN_ADDRESS = "0x70be40667385500c5da7f108a022e21b606045dd"
ST_PRICE_MOVE_ALERT_PCT = float(os.getenv("ST_PRICE_MOVE_ALERT_PCT", "3.0"))
ST_LIQUIDITY_DROP_ALERT_PCT = float(os.getenv("ST_LIQUIDITY_DROP_ALERT_PCT", "15.0"))
ST_CEX_DEX_SPREAD_ALERT_PCT = float(os.getenv("ST_CEX_DEX_SPREAD_ALERT_PCT", "2.0"))
ST_LARGE_TRANSFER_USD = float(os.getenv("ST_LARGE_TRANSFER_USD", "10000"))
ST_WHALE_CHECK_INTERVAL_SECONDS = int(os.getenv("ST_WHALE_CHECK_INTERVAL_SECONDS", "60"))
ST_WHALE_TRANSFER_USD = float(os.getenv("ST_WHALE_TRANSFER_USD", "3000"))
ST_WHALE_FLOW_IMBALANCE_USD = float(os.getenv("ST_WHALE_FLOW_IMBALANCE_USD", "5000"))
ST_WHALE_WINDOW_SECONDS = int(os.getenv("ST_WHALE_WINDOW_SECONDS", "900"))
ST_WHALE_REPEAT_USD = float(os.getenv("ST_WHALE_REPEAT_USD", "5000"))
ST_WHALE_ALERT_COOLDOWN_SECONDS = int(os.getenv("ST_WHALE_ALERT_COOLDOWN_SECONDS", "180"))
ST_PRIORITY_EVENT_USD = float(os.getenv("ST_PRIORITY_EVENT_USD", "4000"))
ST_PRIORITY_FLOW_IMBALANCE_USD = float(os.getenv("ST_PRIORITY_FLOW_IMBALANCE_USD", "8000"))
ST_CEX_WALLETS = {a.strip().lower() for a in os.getenv("ST_CEX_WALLETS", "").split(",") if a.strip()}
FORCE_SUMMARY_INTERVAL_SECONDS = int(os.getenv("ST_FORCE_SUMMARY_INTERVAL_SECONDS", "300"))
ST_WATCH_ADDRESSES_PATH = Path(os.getenv("ST_WATCH_ADDRESSES_PATH", "/opt/crypto-monitor/st_watch_addresses.json"))
ST_MANUAL_LABELS_PATH = Path(os.getenv("ST_MANUAL_LABELS_PATH", "/opt/crypto-monitor/st_manual_labels.json"))
CEX_WALLET_CANDIDATES_BSC_PATH = Path("/opt/crypto-monitor/cex_wallet_candidates_bsc.json")
ST_GENERIC_CEX_EXCHANGES = {"binance", "bitmart", "xt", "phemex", "mexc"}
ST_CEX_STRONG_CONFIDENCE = {"official", "confirmed"}
ST_CEX_STRONG_ROLES = {"deposit_sweeper", "aggregator", "hot_wallet"}
ST_CEX_WEAK_CONFIDENCE = {"bscscan_tag", "observed"}
ST_CEX_WEAK_ROLES = {"deposit_funder", "deposit_address"}
ST_ONE_HOP_TTL_SECONDS = int(os.getenv("ST_ONE_HOP_TTL_SECONDS", "86400"))
ST_ONE_HOP_MAX_WALLETS = int(os.getenv("ST_ONE_HOP_MAX_WALLETS", "200"))
ST_WATCH_PRIORITY_ADDRESSES = {
    "0x73d8bd54f7cf5fab43fe4ef40a62d390644946db": "重点观察地址 A",
    "0x238a358808379702088667322f80ac48bad5e6c4": "重点观察地址 B",
}
STATE_PATH = Path("/opt/crypto-monitor/personal_holding_st_state.json")

DEX_URL = f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN_ADDRESS}"
COINGECKO_TICKERS_URL = "https://api.coingecko.com/api/v3/coins/sentio-token/tickers?include_exchange_logo=false"

POLL_SECONDS = int(os.environ.get("ST_HOLDING_POLL_SECONDS", "30"))
SUMMARY_SECONDS = int(os.environ.get("ST_HOLDING_SUMMARY_SECONDS", "1800"))
PRICE_ALERT_PCT = float(os.environ.get("ST_HOLDING_PRICE_ALERT_PCT", "3.0"))
LIQ_ALERT_PCT = float(os.environ.get("ST_HOLDING_LIQ_ALERT_PCT", "8.0"))
LIQ_ALERT_USD = float(os.environ.get("ST_HOLDING_LIQ_ALERT_USD", "10000"))
VOLUME_M5_ALERT_USD = float(os.environ.get("ST_HOLDING_VOLUME_M5_ALERT_USD", "20000"))
TRANSFER_ALERT_USD = float(os.environ.get("ST_HOLDING_TRANSFER_ALERT_USD", "20000"))
ST_VERBOSE_RAW_ALERTS = os.getenv("ST_VERBOSE_RAW_ALERTS", "0").lower() in {"1", "true", "on", "yes"}
ST_DEX_OBSERVE_COOLDOWN_SECONDS = int(os.getenv("ST_DEX_OBSERVE_COOLDOWN_SECONDS", "300"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-st-holding/1.0"})

ST_MANUAL_LABEL_TITLES = {
    "project_treasury": "人工标签 项目方仓/金库",
    "core_hub": "人工标签 核心Hub大户",
    "hidden_hub": "人工标签 隐性Hub大户",
    "accumulator_priority": "人工标签 优先吸筹地址",
    "binance_alpha_distribution": "人工标签 Binance Alpha分发/活动钱包",
    "static_treasury": "人工标签 静态仓",
    "standard_whale": "人工标签 标准大户",
}


def load_env_file(path="/etc/crypto-monitor.env"):
    p = Path(path)
    if not p.exists():
        return
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip("'").strip('"'))


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def short_addr(addr):
    if not addr:
        return "-"
    return addr[:8] + "..." + addr[-6:]


def fnum(x, digits=4):
    try:
        x = float(x)
    except Exception:
        return "n/a"
    if abs(x) >= 1000:
        return f"{x:,.0f}"
    if abs(x) >= 1:
        return f"{x:,.{digits}f}".rstrip("0").rstrip(".")
    return f"{x:.8f}".rstrip("0").rstrip(".")


def money(x):
    try:
        x = float(x)
    except Exception:
        return "n/a"
    if abs(x) >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if abs(x) >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


def pct(x):
    try:
        return f"{float(x):+.2f}%"
    except Exception:
        return "n/a"


def load_state():
    if STATE_PATH.exists():
        try:
            state = json.loads(STATE_PATH.read_text(encoding="utf-8"))
            if isinstance(state, dict):
                state.setdefault("st_one_hop_watch", {})
                state.setdefault("st_front_row_summary", {})
                return state
        except Exception:
            pass
    return {
        "prices": {},
        "liquidity": {},
        "seen_pairs": {},
        "alerts": {},
        "last_summary": 0,
        "last_coingecko": 0,
        "seen_txs": [],
        "st_one_hop_watch": {},
        "st_front_row_summary": {},
    }


def save_state(state):
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


def fetch_json(url, params=None, timeout=15):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def discord_send(title, description="", fields=None, color=0xF1C40F):
    token = os.environ.get("DISCORD_BOT_TOKEN")
    channel_id = os.environ.get("DISCORD_HOLDING_CHANNEL_ID")
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

    payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    r = SESSION.post(url, headers={"Authorization": f"Bot {token}"}, json=payload, timeout=20)
    if r.status_code not in (200, 201, 204):
        print("Discord send failed", r.status_code, r.text[:300])
        return False
    return True


def alert_once(state, key, title, description="", fields=None, color=0xF1C40F, cooldown=300):
    ts = time.time()
    last = float(state.get("alerts", {}).get(key, 0) or 0)
    if ts - last < cooldown:
        return False
    ok = discord_send(title, description, fields, color)
    if ok:
        state.setdefault("alerts", {})[key] = ts
    return ok


def fetch_dex():
    data = fetch_json(DEX_URL)
    pairs = data.get("pairs") or []
    clean = []
    for p in pairs:
        try:
            price = float(p.get("priceUsd") or 0)
        except Exception:
            price = 0.0
        liq = float((p.get("liquidity") or {}).get("usd") or 0)
        vol = p.get("volume") or {}
        txns = p.get("txns") or {}
        clean.append({
            "chain": p.get("chainId") or "-",
            "dex": p.get("dexId") or "-",
            "pair": p.get("pairAddress") or "-",
            "url": p.get("url") or "",
            "price": price,
            "liq": liq,
            "fdv": float(p.get("fdv") or 0),
            "vol_m5": float(vol.get("m5") or 0),
            "vol_h1": float(vol.get("h1") or 0),
            "vol_h24": float(vol.get("h24") or 0),
            "chg_m5": float((p.get("priceChange") or {}).get("m5") or 0),
            "chg_h1": float((p.get("priceChange") or {}).get("h1") or 0),
            "chg_h24": float((p.get("priceChange") or {}).get("h24") or 0),
            "buys_m5": int((txns.get("m5") or {}).get("buys") or 0),
            "sells_m5": int((txns.get("m5") or {}).get("sells") or 0),
            "base": ((p.get("baseToken") or {}).get("symbol") or "").upper(),
            "quote": ((p.get("quoteToken") or {}).get("symbol") or "").upper(),
        })
    clean.sort(key=lambda x: (x["liq"], x["vol_h1"]), reverse=True)
    return clean


def fetch_bitmart():
    urls = [
        ("BitMart", "https://api-cloud.bitmart.com/spot/quotation/v3/ticker", {"symbol": "ST_USDT"}),
        ("BitMart", "https://api-cloud.bitmart.com/spot/v1/ticker", {"symbol": "ST_USDT"}),
    ]
    for name, url, params in urls:
        try:
            data = fetch_json(url, params=params)
            d = data.get("data")
            if isinstance(d, dict):
                if "tickers" in d and d["tickers"]:
                    d = d["tickers"][0]
                price = d.get("last") or d.get("last_price") or d.get("close")
                vol = d.get("quote_volume_24h") or d.get("v_24h") or d.get("volume_24h") or d.get("quoteVolume")
                chg = d.get("fluctuation") or d.get("price_change_percent_24h") or d.get("change_24h")
                return {"exchange": name, "price": float(price), "vol24": float(vol or 0), "change24": chg, "raw": d}
        except Exception:
            continue
    return None


def fetch_xt():
    try:
        data = fetch_json("https://sapi.xt.com/v4/public/ticker/24h", params={"symbol": "st_usdt"})
        d = data.get("result") or data.get("data") or {}
        if isinstance(d, list):
            d = d[0] if d else {}
        price = d.get("c") or d.get("last") or d.get("close")
        if price:
            return {"exchange": "XT", "price": float(price), "vol24": float(d.get("q") or d.get("quoteVolume") or 0), "change24": d.get("r") or d.get("change")}
    except Exception:
        return None


def fetch_mexc():
    try:
        data = fetch_json(
            "https://api.mexc.com/api/v3/ticker/24hr",
            params={"symbol": "STUSDT"},
        )
        price = data.get("lastPrice") or data.get("price")
        if price:
            return {
                "exchange": "MEXC",
                "price": float(price),
                "vol24": float(data.get("quoteVolume") or 0),
                "change24": data.get("priceChangePercent"),
            }
        logging.info("MEXC ST ticker missing price: %s", str(data)[:300])
    except Exception:
        logging.exception("MEXC ST ticker fetch failed")
    return None



def fetch_phemex():
    for symbol in ("STUSDT", "sSTUSDT"):
        try:
            data = fetch_json("https://api.phemex.com/md/spot/ticker/24hr", params={"symbol": symbol})
            d = data.get("result") or data.get("data") or data
            if isinstance(d, list):
                d = d[0] if d else {}
            price = d.get("lastEp") or d.get("last") or d.get("close")
            if price:
                price = float(price)
                if price > 1000:
                    price = price / 1e8
                return {"exchange": "Phemex", "price": price, "vol24": float(d.get("turnoverEv") or d.get("quoteVolume") or 0), "change24": d.get("priceChangePercent")}
        except Exception:
            continue
    return None


def fetch_cex_prices():
    return [x for x in (fetch_bitmart(), fetch_xt(), fetch_mexc(), fetch_phemex()) if x]


def fetch_coingecko_markets(state):
    ts = time.time()
    if ts - float(state.get("last_coingecko", 0) or 0) < 300:
        return state.get("coingecko_tickers", [])
    try:
        data = fetch_json(COINGECKO_TICKERS_URL, timeout=20)
        tickers = []
        for t in data.get("tickers", [])[:20]:
            market = (t.get("market") or {}).get("name") or "-"
            base = t.get("base") or ""
            target = t.get("target") or ""
            last = t.get("last")
            volume = t.get("converted_volume", {}).get("usd")
            tickers.append({"market": market, "pair": f"{base}/{target}", "last": last, "volume_usd": volume})
        state["last_coingecko"] = ts
        state["coingecko_tickers"] = tickers
        return tickers
    except Exception as e:
        state["coingecko_error"] = str(e)[:160]
        return state.get("coingecko_tickers", [])


def fetch_large_transfers(price_usd):
    key = os.environ.get("BSCSCAN_API_KEY") or os.environ.get("ETHERSCAN_API_KEY")
    if not key:
        return []
    urls = [
        ("EtherscanV2-BSC", "https://api.etherscan.io/v2/api", {
            "chainid": "56", "module": "account", "action": "tokentx",
            "contractaddress": TOKEN_ADDRESS, "page": "1", "offset": "20", "sort": "desc", "apikey": key,
        }),
        ("BscScan", "https://api.bscscan.com/api", {
            "module": "account", "action": "tokentx", "contractaddress": TOKEN_ADDRESS,
            "page": "1", "offset": "20", "sort": "desc", "apikey": key,
        }),
    ]
    for source, url, params in urls:
        try:
            data = fetch_json(url, params=params, timeout=20)
            rows = data.get("result") or []
            out = []
            for r in rows:
                decimals = int(r.get("tokenDecimal") or 18)
                amount = float(r.get("value") or 0) / (10 ** decimals)
                usd = amount * price_usd if price_usd else 0
                if usd >= TRANSFER_ALERT_USD:
                    out.append({
                        "source": source,
                        "hash": r.get("hash"),
                        "from": r.get("from"),
                        "to": r.get("to"),
                        "amount": amount,
                        "usd": usd,
                        "time": r.get("timeStamp"),
                    })
            return out
        except Exception:
            continue
    return []


def dex_summary(pairs):
    if not pairs:
        return "DEX暂无数据"
    total_liq = sum(p["liq"] for p in pairs)
    total_h1 = sum(p["vol_h1"] for p in pairs)
    top = pairs[0]
    return (
        f"主池 {top['chain']}/{top['dex']} {top['base']}/{top['quote']} "
        f"价格 {fnum(top['price'])} | 总流动性 {money(total_liq)} | 1h量 {money(total_h1)} | "
        f"主池5m {pct(top['chg_m5'])} / 1h {pct(top['chg_h1'])}"
    )


def check_events(state):
    pairs = fetch_dex()
    if not pairs:
        alert_once(state, "dex_no_data", "⚠️ ST 持仓监控：DEX暂无数据", f"{TOKEN_ADDRESS}", color=0xE67E22, cooldown=900)
        return

    best = pairs[0]
    price = best["price"]
    total_liq = sum(p["liq"] for p in pairs)
    total_m5 = sum(p["vol_m5"] for p in pairs)
    total_h1 = sum(p["vol_h1"] for p in pairs)
    buys_m5 = sum(p["buys_m5"] for p in pairs)
    sells_m5 = sum(p["sells_m5"] for p in pairs)

    fields = [
        ("DEX概览", dex_summary(pairs), False),
        ("5m交易", f"成交 {money(total_m5)} | buys/sells {buys_m5}/{sells_m5}", True),
        ("1h成交", money(total_h1), True),
    ]
    observe_notes = []
    observe_bias = "neutral"
    observe_color = 0xF1C40F

    def mark_observe(note, bias="neutral"):
        nonlocal observe_bias, observe_color
        observe_notes.append(note)
        if bias == "risk":
            observe_bias = "risk"
            observe_color = 0xE67E22
        elif bias == "strong_risk":
            observe_bias = "risk"
            observe_color = 0xE74C3C
        elif bias == "bull" and observe_bias == "neutral":
            observe_bias = "bull"
            observe_color = 0x2ECC71

    # 新池提醒
    seen_pairs = state.setdefault("seen_pairs", {})
    for p in pairs[:5]:
        if p["pair"] not in seen_pairs:
            seen_pairs[p["pair"]] = now_iso()
            alert_once(
                state,
                f"new_pair:{p['pair']}",
                f"🆕 ST 新DEX池发现 {p['chain']}/{p['dex']}",
                f"{p['base']}/{p['quote']} | 流动性 {money(p['liq'])} | 价格 {fnum(p['price'])}\n{p['url']}",
                color=0x3498DB,
                cooldown=86400,
            )

    # 价格变化
    last_price = float(state.get("prices", {}).get("dex_best", 0) or 0)
    if last_price:
        move = (price - last_price) / last_price * 100
        if abs(move) >= PRICE_ALERT_PCT:
            mark_observe(
                f"DEX价格 {move:+.2f}%：当前 {fnum(price)}，上次 {fnum(last_price)}",
                "bull" if move > 0 else "risk",
            )
            if ST_VERBOSE_RAW_ALERTS:
                color = 0x2ECC71 if move > 0 else 0xE74C3C
                title = f"{'🟢' if move > 0 else '🔴'} ST 价格异动 {move:+.2f}%"
                alert_once(state, f"price_move:{int(time.time()//300)}", title, f"当前 {fnum(price)}，上次 {fnum(last_price)}", fields, color=color, cooldown=240)
    state.setdefault("prices", {})["dex_best"] = price

    # 流动性变化
    last_liq = float(state.get("liquidity", {}).get("total", 0) or 0)
    if last_liq:
        diff = total_liq - last_liq
        diff_pct = diff / last_liq * 100 if last_liq else 0
        if abs(diff) >= LIQ_ALERT_USD or abs(diff_pct) >= LIQ_ALERT_PCT:
            mark_observe(
                f"DEX流动性{'增加' if diff > 0 else '减少'} {money(diff)} ({diff_pct:+.2f}%)，当前 {money(total_liq)}",
                "bull" if diff > 0 else "strong_risk",
            )
            if ST_VERBOSE_RAW_ALERTS:
                color = 0x2ECC71 if diff > 0 else 0xE74C3C
                title = f"{'🟢' if diff > 0 else '🔴'} ST DEX流动性{'增加' if diff > 0 else '减少'}"
                desc = f"变化 {money(diff)} ({diff_pct:+.2f}%) | 当前总流动性 {money(total_liq)}"
                alert_once(state, f"liq:{int(time.time()//300)}", title, desc, fields, color=color, cooldown=300)
    state.setdefault("liquidity", {})["total"] = total_liq

    # 放量提醒
    if total_m5 >= VOLUME_M5_ALERT_USD or buys_m5 + sells_m5 >= 80:
        side = "买盘偏强" if buys_m5 > sells_m5 * 1.3 else "卖盘偏强" if sells_m5 > buys_m5 * 1.3 else "多空活跃"
        mark_observe(
            f"DEX短线放量：{side}，5m成交 {money(total_m5)}，buys/sells {buys_m5}/{sells_m5}",
            "bull" if side == "买盘偏强" else "risk" if side == "卖盘偏强" else "neutral",
        )
        if ST_VERBOSE_RAW_ALERTS:
            alert_once(
                state,
                f"dex_volume:{int(time.time()//300)}",
                f"🟡 ST DEX短线放量：{side}",
                f"5m成交 {money(total_m5)} | buys/sells {buys_m5}/{sells_m5} | 价格 {fnum(price)}",
                fields,
                color=0xF1C40F,
                cooldown=300,
            )

    # CEX spot
    cex = fetch_cex_prices()
    if cex:
        cex_lines = []
        for x in cex:
            cex_lines.append(f"{x['exchange']}: {fnum(x['price'])} | 24h量 {money(x.get('vol24') or 0)}")
            key = "cex:" + x["exchange"]
            last = float(state.get("prices", {}).get(key, 0) or 0)
            if last:
                move = (x["price"] - last) / last * 100
                if abs(move) >= PRICE_ALERT_PCT:
                    mark_observe(
                        f"{x['exchange']}现货 {move:+.2f}%：当前 {fnum(x['price'])}，上次 {fnum(last)}",
                        "bull" if move > 0 else "risk",
                    )
                    if ST_VERBOSE_RAW_ALERTS:
                        alert_once(
                            state,
                            f"cex_move:{x['exchange']}:{int(time.time()//300)}",
                            f"{'🟢' if move > 0 else '🔴'} ST {x['exchange']}现货异动 {move:+.2f}%",
                            f"当前 {fnum(x['price'])}，上次 {fnum(last)}",
                            fields + [("CEX", "\n".join(cex_lines), False)],
                            color=0x2ECC71 if move > 0 else 0xE74C3C,
                            cooldown=300,
                        )
            state.setdefault("prices", {})[key] = x["price"]

        prices = [x["price"] for x in cex] + [price]
        if min(prices) > 0:
            spread = (max(prices) - min(prices)) / min(prices) * 100
            if spread >= 3:
                mark_observe(f"现货/DEX价差 {spread:.2f}%，需要留意搬砖或短线错价", "risk")
                if ST_VERBOSE_RAW_ALERTS:
                    alert_once(
                        state,
                        f"arb_spread:{int(time.time()//300)}",
                        f"🟠 ST 现货/DEX价差 {spread:.2f}%",
                        "\n".join(cex_lines) + f"\nDEX: {fnum(price)}",
                        fields,
                        color=0xE67E22,
                        cooldown=300,
                    )

    if observe_notes:
        title = "🟠 ST 持仓风险观察" if observe_bias == "risk" else "🟢 ST 持仓承接观察" if observe_bias == "bull" else "🟡 ST 持仓异动观察"
        action = (
            "先不追，重点看卖盘是否连续、流动性是否继续下降。"
            if observe_bias == "risk"
            else "只当承接增强观察，等链上大户或交易所路径确认。"
            if observe_bias == "bull"
            else "暂时只记录异动，不单独作为买卖依据。"
        )
        desc = (
            f"结论：无合约币，当前只能按现货/DEX/链上流动性判断。\n"
            f"操作：{action}\n"
            f"关键数据：\n" + "\n".join(f"- {x}" for x in observe_notes[:5])
        )
        observe_fields = list(fields)
        if cex:
            observe_fields.append(("现货交易所", "\n".join(cex_lines), False))
        alert_once(
            state,
            f"st_market_observe:{int(time.time()//300)}",
            title,
            desc,
            observe_fields,
            color=observe_color,
            cooldown=ST_DEX_OBSERVE_COOLDOWN_SECONDS,
        )

    # CoinGecko market discovery summary every 5m cached
    tickers = fetch_coingecko_markets(state)
    if tickers:
        state["last_markets_summary"] = "\n".join(
            f"{t['market']} {t['pair']} {fnum(t['last'])} 量{money(t.get('volume_usd') or 0)}"
            for t in tickers[:5]
        )

    # Large transfers
    seen = set(state.get("seen_txs", [])[-200:])
    transfers = fetch_large_transfers(price)
    for tr in transfers:
        h = tr.get("hash")
        if not h or h in seen:
            continue
        seen.add(h)
        if ST_VERBOSE_RAW_ALERTS:
            alert_once(
                state,
                f"transfer:{h}",
                f"🐳 ST 链上大额转账 {money(tr['usd'])}",
                f"{fnum(tr['amount'])} ST | {short_addr(tr['from'])} -> {short_addr(tr['to'])}\n{tr['source']} tx {h}",
                fields,
                color=0x9B59B6,
                cooldown=60,
            )
    state["seen_txs"] = list(seen)[-200:]

    # Periodic holding summary
    ts = time.time()
    if ts - float(state.get("last_summary", 0) or 0) >= SUMMARY_SECONDS:
        extra_fields = list(fields)
        if cex:
            extra_fields.append(("现货交易所", "\n".join(cex_lines), False))
        if state.get("last_markets_summary"):
            extra_fields.append(("市场来源", state["last_markets_summary"], False))
        front_row_field = _st_front_row_summary_field(state)
        if front_row_field:
            extra_fields.append(front_row_field)
        discord_send(
            "📌 ST 个人持仓监控",
            f"{TOKEN_NAME} ({TOKEN_SYMBOL})\n合约: `{TOKEN_ADDRESS}`\n{dex_summary(pairs)}",
            extra_fields,
            color=0x5865F2,
        )
        state["last_summary"] = ts

def pct_change(new, old):
    try:
        new = float(new)
        old = float(old)
        if not old:
            return None
        return (new - old) / old * 100.0
    except Exception:
        return None

def load_st_watch_addresses():
    try:
        if ST_WATCH_ADDRESSES_PATH.exists():
            data = json.loads(ST_WATCH_ADDRESSES_PATH.read_text(encoding="utf-8"))
        else:
            data = {}
    except Exception:
        logging.exception("ST watch address file load failed")
        data = {}

    def lower_keys(name):
        value = data.get(name) or {}
        if isinstance(value, dict):
            return {str(k).lower(): v for k, v in value.items()}
        if isinstance(value, list):
            return {str(k).lower(): {} for k in value}
        return {}

    return {
        "dex_pairs": lower_keys("dex_pairs"),
        "cex_wallets": {
            str(ex).lower(): {str(a).lower() for a in (addrs or [])}
            for ex, addrs in (data.get("cex_wallets") or {}).items()
        },
        "suspected_hubs": lower_keys("suspected_hubs"),
        "candidate_accumulators": lower_keys("candidate_accumulators"),
        "candidate_distributors": lower_keys("candidate_distributors"),
        "top_holders_estimated": lower_keys("top_holders_estimated"),
        "core_top_holders": lower_keys("core_top_holders"),
        "manual_labels": load_st_manual_labels(),
    }


def load_st_manual_labels():
    try:
        if not ST_MANUAL_LABELS_PATH.exists():
            return {}
        data = json.loads(ST_MANUAL_LABELS_PATH.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("ST manual label file load failed")
        return {}

    if isinstance(data, dict) and isinstance(data.get("labels"), list):
        rows = data.get("labels") or []
    elif isinstance(data, list):
        rows = data
    else:
        rows = []

    labels = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        addr = str(row.get("address") or "").lower().strip()
        manual_type = str(row.get("type") or "").strip()
        if not addr or not manual_type:
            continue
        labels[addr] = {
            "type": manual_type,
            "label": str(row.get("label") or ST_MANUAL_LABEL_TITLES.get(manual_type) or manual_type),
            "note": str(row.get("note") or ""),
            "priority": str(row.get("priority") or "high"),
            "source": "manual",
            "manual_tag": True,
        }
    return labels


def _st_cex_strength_meta(role, confidence):
    role = str(role or "").strip().lower()
    confidence = str(confidence or "").strip().lower()

    if confidence == "bscscan_tag" and role == "hot_wallet":
        return {
            "exchange_tier": "confirmed_exchange",
            "strength": "strong_confirmed",
            "tier_label": "confirmed_exchange",
        }
    if confidence == "bscscan_tag" and role in {"aggregator", "deposit_funder"}:
        return {
            "exchange_tier": "structural_exchange",
            "strength": "strong_structural",
            "tier_label": "structural_exchange",
        }
    if confidence == "observed" and role in {"hot_wallet", "aggregator"}:
        return {
            "exchange_tier": "structural_exchange",
            "strength": "strong_structural",
            "tier_label": "structural_exchange",
        }
    if confidence in {"observed", "bscscan_tag"} and role == "deposit_address":
        return {
            "exchange_tier": "weak_exchange_candidate",
            "strength": "weak",
            "tier_label": "weak_exchange_candidate",
        }
    if confidence in {"bscscan_tag", "observed"}:
        return {
            "exchange_tier": "weak_exchange_candidate",
            "strength": "weak",
            "tier_label": "weak_exchange_candidate",
        }
    return {
        "exchange_tier": "weak_exchange_candidate",
        "strength": "weak",
        "tier_label": "weak_exchange_candidate",
    }


def load_cex_wallet_candidates_bsc():
    try:
        if not CEX_WALLET_CANDIDATES_BSC_PATH.exists():
            return {}
        rows = json.loads(CEX_WALLET_CANDIDATES_BSC_PATH.read_text(encoding="utf-8"))
    except Exception:
        logging.exception("ST generic CEX candidate file load failed")
        return {}

    wallet_map = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        if str(row.get("chain") or "").lower() != "bsc":
            continue
        exchange = str(row.get("exchange") or "").strip()
        if exchange.lower() not in ST_GENERIC_CEX_EXCHANGES:
            continue
        addr = str(row.get("address") or "").lower()
        if not addr:
            continue
        role = str(row.get("role") or "").strip()
        confidence = str(row.get("confidence") or "").strip()
        source_type = str(row.get("source_type") or "").strip()
        tier_meta = _st_cex_strength_meta(role, confidence)
        wallet_map[addr] = {
            "exchange": exchange,
            "role": role,
            "confidence": confidence,
            "source_type": source_type,
            "source_url": str(row.get("source_url") or ""),
            "notes": str(row.get("notes") or ""),
            "strength": tier_meta["strength"],
            "exchange_tier": tier_meta["exchange_tier"],
            "tier_label": tier_meta["tier_label"],
        }
    logging.info("ST generic CEX candidate library loaded: %s addresses across %s exchanges", len(wallet_map), len({v.get('exchange') for v in wallet_map.values()}))
    return wallet_map


def lookup_cex_wallet_candidate(addr, candidates=None):
    addr = (addr or "").lower()
    candidates = candidates if candidates is not None else GENERIC_CEX_WALLET_CANDIDATES_BSC
    meta = candidates.get(addr)
    if not meta:
        return None
    return {
        "exchange": meta.get("exchange") or "",
        "role": meta.get("role") or "",
        "confidence": meta.get("confidence") or "",
        "source_type": meta.get("source_type") or "",
        "strength": meta.get("strength") or "weak",
        "exchange_tier": meta.get("exchange_tier") or "weak_exchange_candidate",
        "tier_label": meta.get("tier_label") or "weak_exchange_candidate",
    }


GENERIC_CEX_WALLET_CANDIDATES_BSC = load_cex_wallet_candidates_bsc()


def _cleanup_st_one_hop_watch(state, now_ts=None):
    now_ts = int(now_ts or time.time())
    watch = state.setdefault("st_one_hop_watch", {})
    fresh = {
        str(addr).lower(): _normalize_st_one_hop_meta(meta, now_ts)
        for addr, meta in watch.items()
        if isinstance(meta, dict) and int((meta.get("expires_at") if isinstance(meta, dict) else 0) or 0) > now_ts
    }
    if len(fresh) > ST_ONE_HOP_MAX_WALLETS:
        ranked = sorted(
            fresh.items(),
            key=lambda item: (
                int((item[1] or {}).get("last_seen_ts", (item[1] or {}).get("last_seen", 0)) or 0),
                int((item[1] or {}).get("hit_count", (item[1] or {}).get("hits", 0)) or 0),
            ),
            reverse=True,
        )
        fresh = dict(ranked[:ST_ONE_HOP_MAX_WALLETS])
    state["st_one_hop_watch"] = fresh
    return fresh


def _normalize_st_one_hop_meta(meta, now_ts=None):
    now_ts = int(now_ts or time.time())
    meta = dict(meta or {})
    first_seen_ts = int(meta.get("first_seen_ts", meta.get("first_seen", now_ts)) or now_ts)
    last_seen_ts = int(meta.get("last_seen_ts", meta.get("last_seen", first_seen_ts)) or first_seen_ts)
    return {
        "first_seen_ts": first_seen_ts,
        "last_seen_ts": last_seen_ts,
        "expires_at": int(meta.get("expires_at", last_seen_ts + ST_ONE_HOP_TTL_SECONDS) or (last_seen_ts + ST_ONE_HOP_TTL_SECONDS)),
        "origin_type": str(meta.get("origin_type") or ""),
        "origin_label": str(meta.get("origin_label") or ""),
        "origin_address": str(meta.get("origin_address") or "").lower(),
        "hit_count": int(meta.get("hit_count", meta.get("hits", 0)) or 0),
        "last_side": str(meta.get("last_side") or ""),
        "escalated_once": bool(meta.get("escalated_once", False)),
        "source_confidence": str(meta.get("source_confidence") or ""),
        "origin_manual_tag": bool(meta.get("origin_manual_tag", False)),
        "origin_manual_type": str(meta.get("origin_manual_type") or ""),
    }


def _infer_st_one_hop_source_confidence(meta):
    meta = dict(meta or {})
    current = str(meta.get("source_confidence") or "").strip().lower()
    if current in {"confirmed", "structural", "weak"}:
        return current

    if meta.get("origin_manual_tag"):
        return "confirmed"

    origin_type = str(meta.get("origin_type") or "").strip().lower()
    origin_address = str(meta.get("origin_address") or "").strip().lower()

    if origin_type in {
        "core_top_holder",
        "top_holder",
        "accumulator",
        "distributor",
        "project_treasury",
        "core_hub",
        "hidden_hub",
        "accumulator_priority",
        "static_treasury",
        "standard_whale",
    }:
        return "confirmed"

    if origin_type != "cex" or not origin_address:
        return ""

    cex_meta = lookup_cex_wallet_candidate(origin_address, GENERIC_CEX_WALLET_CANDIDATES_BSC)
    if not cex_meta:
        return ""

    strength = str(cex_meta.get("strength") or "").strip().lower()
    if strength == "strong_confirmed":
        return "confirmed"
    if strength == "strong_structural":
        return "structural"
    if strength == "weak":
        return "weak"
    return ""


def _track_st_one_hop_wallet(
    state,
    addr,
    origin_type,
    origin_label,
    origin_address,
    now_ts=None,
    source_confidence="",
    origin_manual_tag=False,
    origin_manual_type="",
):
    addr = (addr or "").lower()
    if not addr:
        return None
    watch = _cleanup_st_one_hop_watch(state, now_ts)
    now_ts = int(now_ts or time.time())
    meta = _normalize_st_one_hop_meta(watch.get(addr) or {}, now_ts)
    meta.update(
        {
            "last_seen_ts": now_ts,
            "expires_at": now_ts + ST_ONE_HOP_TTL_SECONDS,
            "origin_type": origin_type,
            "origin_label": origin_label,
            "origin_address": (origin_address or "").lower(),
            "source_confidence": str(source_confidence or meta.get("source_confidence") or ""),
            "origin_manual_tag": bool(origin_manual_tag),
            "origin_manual_type": str(origin_manual_type or ""),
        }
    )
    meta["source_confidence"] = _infer_st_one_hop_source_confidence(meta)
    watch[addr] = meta
    state["st_one_hop_watch"] = watch
    return meta


def _st_one_hop_meta(state, addr):
    addr = (addr or "").lower()
    watch = _cleanup_st_one_hop_watch(state)
    meta = watch.get(addr)
    if not isinstance(meta, dict):
        return None
    meta = _normalize_st_one_hop_meta(meta)
    inferred = _infer_st_one_hop_source_confidence(meta)
    if inferred and inferred != meta.get("source_confidence"):
        meta["source_confidence"] = inferred
        watch[addr] = meta
        state["st_one_hop_watch"] = watch
    return meta


def _st_update_one_hop_event(state, actor, side, escalated=False):
    actor = (actor or "").lower()
    if not actor:
        return None
    meta = _st_one_hop_meta(state, actor)
    if not meta:
        return None
    meta = _normalize_st_one_hop_meta(meta)
    meta["hit_count"] = int(meta.get("hit_count", 0) or 0) + 1
    meta["last_seen_ts"] = int(time.time())
    meta["last_side"] = side
    if escalated:
        meta["escalated_once"] = True
    meta["source_confidence"] = _infer_st_one_hop_source_confidence(meta)
    state.setdefault("st_one_hop_watch", {})[actor] = meta
    return meta


def _st_front_row_summary(window, now_ts=None):
    now_ts = int(now_ts or time.time())
    cutoff = now_ts - ST_WHALE_WINDOW_SECONDS
    scoped = [x for x in (window or []) if int(x.get("ts", 0) or 0) >= cutoff]
    summary = {
        "window_minutes": int(ST_WHALE_WINDOW_SECONDS / 60),
        "front_row_buy_usd": 0.0,
        "front_row_sell_usd": 0.0,
        "front_row_cex_deposit_usd": 0.0,
        "front_row_cex_withdraw_usd": 0.0,
        "active_front_row_wallets": 0,
        "active_one_hop_wallets": 0,
        "front_row_related_hits": 0,
        "confirmed_cex_hits": 0,
        "structural_cex_hits": 0,
        "active_wallet_list": [],
    }
    active_wallets = set()
    active_one_hop_wallets = set()
    for item in scoped:
        side = item.get("side")
        usd = float(item.get("usd", 0) or 0)
        actor = (item.get("actor") or "").lower()
        if item.get("front_row") and actor:
            active_wallets.add(actor)
        if item.get("one_hop") and actor:
            active_one_hop_wallets.add(actor)
        if item.get("front_row") or item.get("one_hop"):
            summary["front_row_related_hits"] += 1
        if item.get("cex_strength") == "strong_confirmed":
            summary["confirmed_cex_hits"] += 1
        elif item.get("cex_strength") == "strong_structural":
            summary["structural_cex_hits"] += 1
        if not item.get("front_row"):
            continue
        if side == "buy":
            summary["front_row_buy_usd"] += usd
        elif side == "sell":
            summary["front_row_sell_usd"] += usd
        elif side == "cex_deposit":
            summary["front_row_cex_deposit_usd"] += usd
        elif side == "cex_withdraw":
            summary["front_row_cex_withdraw_usd"] += usd
    summary["active_front_row_wallets"] = len(active_wallets)
    summary["active_one_hop_wallets"] = len(active_one_hop_wallets)
    summary["active_wallet_list"] = [_st_whale_short(x) for x in sorted(active_wallets)[:5]]
    return summary


def _st_front_row_summary_field(state):
    summary = state.get("st_front_row_summary") or {}
    if not summary:
        return None
    return (
        "前排15m摘要",
        (
            f"净扫货 {money(summary.get('front_row_buy_usd', 0))} | 净卖压 {money(summary.get('front_row_sell_usd', 0))}\n"
            f"进交易所 {money(summary.get('front_row_cex_deposit_usd', 0))} | 交易所提币 {money(summary.get('front_row_cex_withdraw_usd', 0))}\n"
            f"活跃前排地址 {int(summary.get('active_front_row_wallets', 0) or 0)} | 一跳地址 {int(summary.get('active_one_hop_wallets', 0) or 0)}\n"
            f"关联命中 {int(summary.get('front_row_related_hits', 0) or 0)} | 确认CEX {int(summary.get('confirmed_cex_hits', 0) or 0)} | 结构CEX {int(summary.get('structural_cex_hits', 0) or 0)}"
            + (f" | {', '.join(summary.get('active_wallet_list') or [])}" if summary.get("active_wallet_list") else "")
        ),
        False,
    )


def st_address_label(addr, labels=None, state=None):
    addr = (addr or "").lower()
    labels = labels or load_st_watch_addresses()
    if addr in labels.get("dex_pairs", {}):
        meta = labels["dex_pairs"][addr] or {}
        return "dex_pair", f"DEX池 {meta.get('dex') or ''}".strip(), {}
    for exchange, addrs in labels.get("cex_wallets", {}).items():
        if addr in addrs:
            return "cex", f"{exchange}交易所钱包", {"exchange": exchange, "source_type": "token_specific", "confidence": "confirmed", "strength": "strong_confirmed"}
    if addr in ST_CEX_WALLETS:
        return "cex", "交易所钱包", {"exchange": "manual", "source_type": "env", "confidence": "confirmed", "strength": "strong_confirmed"}
    manual_meta = labels.get("manual_labels", {}).get(addr)
    if manual_meta:
        manual_type = str(manual_meta.get("type") or "").strip()
        manual_label = str(manual_meta.get("label") or ST_MANUAL_LABEL_TITLES.get(manual_type) or manual_type)
        note = str(manual_meta.get("note") or "").strip()
        label = manual_label if not note else f"{manual_label} | {note}"
        meta = dict(manual_meta)
        if addr in ST_WATCH_PRIORITY_ADDRESSES:
            meta["watch_priority"] = True
            meta["watch_priority_label"] = ST_WATCH_PRIORITY_ADDRESSES[addr]
        return manual_type, label, meta
    if addr in ST_WATCH_PRIORITY_ADDRESSES:
        return "watch_priority", ST_WATCH_PRIORITY_ADDRESSES[addr], {"watch_priority": True}
    if addr in labels.get("suspected_hubs", {}):
        return "hub", "高周转/做市/归集候选", {}
    if addr in labels.get("candidate_accumulators", {}):
        return "accumulator", "净流入吸筹候选", {}
    if addr in labels.get("candidate_distributors", {}):
        return "distributor", "净流出派发候选", {}
    if addr in labels.get("core_top_holders", {}):
        meta = labels["core_top_holders"][addr] or {}
        return "core_top_holder", f"核心前排钱包 Rank {meta.get('rank', '?')} / {float(meta.get('pct_supply') or 0):.2f}%", meta
    if addr in labels.get("top_holders_estimated", {}):
        meta = labels["top_holders_estimated"][addr] or {}
        return "top_holder", f"前排持仓地址 Rank {meta.get('rank', '?')}", meta

    if state is not None:
        one_hop_meta = _st_one_hop_meta(state, addr)
        if one_hop_meta:
            return "one_hop_candidate", f"一跳观察地址 <- {one_hop_meta.get('origin_label') or one_hop_meta.get('origin_type') or '重点地址'}", one_hop_meta

    cex_meta = lookup_cex_wallet_candidate(addr, GENERIC_CEX_WALLET_CANDIDATES_BSC)
    if cex_meta:
        label = f"{cex_meta['exchange']} 交易所相关地址"
        if cex_meta.get("role"):
            label = f"{label} ({cex_meta['role']})"
        label_type = "cex" if cex_meta.get("strength") in {"strong_confirmed", "strong_structural"} else "cex_candidate"
        return label_type, label, cex_meta
    return "unknown", "未知钱包", {}



def _st_whale_short(addr):
    addr = (addr or "").lower()
    return addr[:6] + "..." + addr[-4:] if len(addr) > 12 else addr


def _st_whale_amount(row):
    try:
        return float(row.get("value") or 0) / (10 ** int(row.get("tokenDecimal") or 18))
    except Exception:
        return 0.0


def _st_whale_fetch_pairs(state):
    now = time.time()
    if now - float(state.get("st_pair_cache_ts", 0) or 0) < 300 and state.get("st_dex_pair_addresses"):
        return {str(x).lower() for x in state.get("st_dex_pair_addresses", [])}
    try:
        r = requests.get(f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN_ADDRESS}", timeout=20)
        data = r.json()
        pairs = data.get("pairs") or []
        addrs = []
        labels = []
        for p in pairs:
            pair_addr = (p.get("pairAddress") or "").lower()
            if pair_addr:
                addrs.append(pair_addr)
                labels.append(f"{p.get('dexId','dex')}:{_st_whale_short(pair_addr)}")
        state["st_dex_pair_addresses"] = sorted(set(addrs))
        state["st_pair_cache_ts"] = now
        state["st_dex_pair_labels"] = labels[:8]
        return set(state["st_dex_pair_addresses"])
    except Exception:
        logging.exception("ST whale pair fetch failed")
        return {str(x).lower() for x in state.get("st_dex_pair_addresses", [])}


def _st_whale_fetch_transfers(limit=50):
    key = os.getenv("ETHERSCAN_API_KEY") or os.getenv("BSCSCAN_API_KEY")
    if not key:
        return []
    try:
        r = requests.get(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": "56",
                "module": "account",
                "action": "tokentx",
                "contractaddress": TOKEN_ADDRESS,
                "page": 1,
                "offset": limit,
                "sort": "desc",
                "apikey": key,
            },
            timeout=20,
        )
        data = r.json()
        if data.get("status") != "1" or not isinstance(data.get("result"), list):
            logging.info("ST whale transfer fetch skipped: %s", str(data)[:300])
            return []
        return data["result"]
    except Exception:
        logging.exception("ST whale transfer fetch failed")
        return []


def _st_whale_price(state):
    prices = state.get("prices", {}) if isinstance(state, dict) else {}
    for key in ("dex_best", "cex:BitMart", "cex:XT", "cex:MEXC", "cex:Phemex"):
        value = prices.get(key)
        if value:
            try:
                return float(value)
            except Exception:
                pass
    return 0.0


def _st_whale_side(row, pair_addrs):
    labels = load_st_watch_addresses()
    state = row.get("_state") or {}
    now_ts = int(row.get("timeStamp") or time.time())
    src = (row.get("from") or "").lower()
    dst = (row.get("to") or "").lower()

    src_type, src_label, src_meta = st_address_label(src, labels, state)
    dst_type, dst_label, dst_meta = st_address_label(dst, labels, state)

    tracked_origin_types = {
        "core_top_holder",
        "accumulator",
        "distributor",
        "cex",
        "project_treasury",
        "core_hub",
        "hidden_hub",
        "accumulator_priority",
        "static_treasury",
        "standard_whale",
    }

    if src_type in tracked_origin_types and dst_type == "unknown":
        tracked = _track_st_one_hop_wallet(
            state,
            dst,
            src_type,
            src_label,
            src,
            now_ts,
            source_confidence=str(src_meta.get("strength") or src_type),
            origin_manual_tag=bool(src_meta.get("manual_tag")),
            origin_manual_type=str(src_meta.get("type") or src_type),
        )
        if tracked:
            logging.info("ST one-hop watch add: addr=%s origin=%s source=%s confidence=%s ttl=%ss", dst, src_type, short_addr(src), tracked.get("source_confidence"), ST_ONE_HOP_TTL_SECONDS)

    if dst_type in tracked_origin_types and src_type == "unknown":
        tracked = _track_st_one_hop_wallet(
            state,
            src,
            dst_type,
            dst_label,
            dst,
            now_ts,
            source_confidence=str(dst_meta.get("strength") or dst_type),
            origin_manual_tag=bool(dst_meta.get("manual_tag")),
            origin_manual_type=str(dst_meta.get("type") or dst_type),
        )
        if tracked:
            logging.info("ST one-hop watch add: addr=%s origin=%s source=%s confidence=%s ttl=%ss", src, dst_type, short_addr(dst), tracked.get("source_confidence"), ST_ONE_HOP_TTL_SECONDS)

    def cex_label(base_label, meta):
        if not meta:
            return base_label
        exchange = meta.get("exchange") or "交易所"
        role = meta.get("role") or "unknown_role"
        confidence = meta.get("confidence") or "unknown_confidence"
        tier = meta.get("strength") or "weak"
        exchange_tier = meta.get("exchange_tier") or meta.get("tier_label") or "weak_exchange_candidate"
        return f"{base_label} [{exchange}/{role}/{confidence}/{exchange_tier}]"

    one_hop_src = src_type == "one_hop_candidate"
    one_hop_dst = dst_type == "one_hop_candidate"
    manual_src = bool(src_meta.get("manual_tag"))
    manual_dst = bool(dst_meta.get("manual_tag"))
    manual_one_hop_src = one_hop_src and bool(src_meta.get("origin_manual_tag"))
    manual_one_hop_dst = one_hop_dst and bool(dst_meta.get("origin_manual_tag"))
    manual_priority = manual_src or manual_dst or manual_one_hop_src or manual_one_hop_dst
    front_row = src_type in {"core_top_holder", "top_holder"} or dst_type in {"core_top_holder", "top_holder"} or manual_src or manual_dst
    watch_priority = src_type == "watch_priority" or dst_type == "watch_priority" or bool(src_meta.get("watch_priority")) or bool(dst_meta.get("watch_priority"))
    treasury_src = src_type in {"project_treasury", "static_treasury"}
    treasury_dst = dst_type in {"project_treasury", "static_treasury"}

    def tags(one_hop=False, cex_strength="", one_hop_escalated=False, watch=False):
        return {
            "front_row": front_row,
            "one_hop": one_hop,
            "cex_strength": cex_strength,
            "one_hop_escalated": one_hop_escalated,
            "watch_priority": watch,
            "manual_priority": manual_priority,
            "manual_label_type": str(
                src_meta.get("type")
                or dst_meta.get("type")
                or src_meta.get("origin_manual_type")
                or dst_meta.get("origin_manual_type")
                or ""
            ),
            "manual_label_name": str(
                src_meta.get("label")
                or dst_meta.get("label")
                or src_meta.get("origin_label")
                or dst_meta.get("origin_label")
                or ""
            ),
            "manual_static_source": treasury_src,
            "manual_static_target": treasury_dst,
        }

    if one_hop_src and (dst_type == "dex_pair" or dst in pair_addrs):
        logging.info(
            "ST one-hop escalation: path=dex side=sell source=%s source_origin=%s target_exchange=%s target_role=%s target_strength=%s",
            src,
            src_meta.get("origin_type"),
            "dex",
            "pair",
            "n/a",
        )
        return "sell", src, f"{src_label} 转入{dst_label or 'DEX池'}，一跳后进入 DEX，卖压权重上调", tags(one_hop=True, one_hop_escalated=True, watch=watch_priority)
    if one_hop_dst and (src_type == "dex_pair" or src in pair_addrs):
        logging.info(
            "ST one-hop escalation: path=dex side=buy source=%s source_origin=%s target_exchange=%s target_role=%s target_strength=%s",
            dst,
            dst_meta.get("origin_type"),
            "dex",
            "pair",
            "n/a",
        )
        return "buy", dst, f"从{src_label or 'DEX池'}转入{dst_label}，一跳后承接 DEX，吸筹权重上调", tags(one_hop=True, one_hop_escalated=True, watch=watch_priority)

    if dst_type == "dex_pair" or dst in pair_addrs:
        return "sell", src, f"转入{dst_label or 'DEX池'}，疑似出货/卖压", tags(one_hop=one_hop_src, watch=watch_priority)
    if src_type == "dex_pair" or src in pair_addrs:
        return "buy", dst, f"从{src_label or 'DEX池'}转出，疑似扫货/吸筹", tags(one_hop=one_hop_dst, watch=watch_priority)

    if dst_type == "cex":
        if one_hop_src:
            logging.info(
                "ST one-hop escalation: path=cex_deposit side=sell source=%s source_origin=%s target_exchange=%s target_role=%s target_strength=%s",
                src,
                src_meta.get("origin_type"),
                dst_meta.get("exchange"),
                dst_meta.get("role"),
                dst_meta.get("strength"),
            )
            return "cex_deposit", src, f"{src_label} 转入{cex_label(dst_label, dst_meta)}，一跳后进交易所，卖压权重上调", tags(one_hop=True, cex_strength=dst_meta.get("strength") or "", one_hop_escalated=True, watch=watch_priority)
        return "cex_deposit", src, f"转入{cex_label(dst_label, dst_meta)}，潜在卖压", tags(cex_strength=dst_meta.get("strength") or "", watch=watch_priority)
    if src_type == "cex":
        if one_hop_dst:
            logging.info(
                "ST one-hop escalation: path=cex_withdraw side=buy source=%s source_origin=%s target_exchange=%s target_role=%s target_strength=%s",
                dst,
                dst_meta.get("origin_type"),
                src_meta.get("exchange"),
                src_meta.get("role"),
                src_meta.get("strength"),
            )
            return "cex_withdraw", dst, f"从{cex_label(src_label, src_meta)}提币到{dst_label}，一跳承接，吸筹权重上调", tags(one_hop=True, cex_strength=src_meta.get("strength") or "", one_hop_escalated=True, watch=watch_priority)
        return "cex_withdraw", dst, f"从{cex_label(src_label, src_meta)}提币，偏吸筹/囤币观察", tags(cex_strength=src_meta.get("strength") or "", watch=watch_priority)

    if src_type == "watch_priority" or dst_type == "watch_priority":
        actor = src if src_type == "watch_priority" else dst
        label = src_label if src_type == "watch_priority" else dst_label
        return "watch_priority", actor, f"{label} 出现链上动作，最高优先级盯盘", tags(one_hop=one_hop_src or one_hop_dst, watch=True)

    if src_type in ST_MANUAL_LABEL_TITLES or dst_type in ST_MANUAL_LABEL_TITLES:
        actor = src if src_type in ST_MANUAL_LABEL_TITLES else dst
        label = src_label if src_type in ST_MANUAL_LABEL_TITLES else dst_label
        manual_type = src_type if src_type in ST_MANUAL_LABEL_TITLES else dst_type
        title = ST_MANUAL_LABEL_TITLES.get(manual_type, manual_type)
        return manual_type, actor, f"{title} 命中：{label} 出现链上动作，优先级高于普通前排地址", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)

    if src_type == "core_top_holder" or dst_type == "core_top_holder":
        actor = src if src_type == "core_top_holder" else dst
        label = src_label if src_type == "core_top_holder" else dst_label
        return "core_top_holder", actor, f"{label} 出现链上动作，最高优先级观察", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)
    if src_type == "top_holder" or dst_type == "top_holder":
        actor = src if src_type == "top_holder" else dst
        label = src_label if src_type == "top_holder" else dst_label
        return "top_holder", actor, f"{label} 出现链上动作，重点观察", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)
    if src_type == "accumulator" or dst_type == "accumulator":
        actor = src if src_type == "accumulator" else dst
        return "accumulator", actor, "净流入吸筹候选地址出现动作，重点观察", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)
    if src_type == "distributor" or dst_type == "distributor":
        actor = src if src_type == "distributor" else dst
        return "distributor", actor, "净流出派发候选地址出现动作，风险观察", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)
    if src_type == "hub" or dst_type == "hub":
        actor = src if src_type == "hub" else dst
        return "hub", actor, "高周转/做市/归集候选地址动作，暂不判断方向", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)

    if src_type == "cex_candidate":
        return "transfer", dst, f"来自{cex_label(src_label, src_meta)}，交易所候选相关地址动作，暂不直接判定提币", tags(one_hop=one_hop_dst, cex_strength=src_meta.get("strength") or "", watch=watch_priority)
    if dst_type == "cex_candidate":
        return "transfer", src, f"转入{cex_label(dst_label, dst_meta)}，交易所候选相关地址动作，暂不直接判定充值", tags(one_hop=one_hop_src, cex_strength=dst_meta.get("strength") or "", watch=watch_priority)

    return "transfer", dst, "普通大额链上转账，暂不判断方向", tags(one_hop=one_hop_src or one_hop_dst, watch=watch_priority)


def _st_priority_bucket(event):
    side = event.get("side")
    usd = float(event.get("usd", 0) or 0)
    if usd < ST_PRIORITY_EVENT_USD:
        return None
    if event.get("manual_static_first_outflow"):
        return "manual_static_first_outflow"
    label = str(event.get("label") or "")
    front_row = "核心前排钱包" in label or "前排持仓地址" in label or bool(event.get("manual_priority"))
    if event.get("manual_priority") and side in {"sell", "cex_deposit", "distributor"}:
        return "manual_risk"
    if event.get("manual_priority") and side in {"buy", "cex_withdraw", "accumulator", "accumulator_priority"}:
        return "manual_accum"
    if event.get("manual_priority") and side in {
        "project_treasury",
        "static_treasury",
        "core_hub",
        "hidden_hub",
        "accumulator_priority",
        "transfer",
    }:
        return "manual_active"
    if event.get("watch_priority") and side in {"sell", "cex_deposit", "distributor"}:
        return "watch_priority_risk"
    if event.get("watch_priority") and side in {"buy", "cex_withdraw", "accumulator"}:
        return "watch_priority_accum"
    if event.get("watch_priority") and side in {"watch_priority", "transfer"}:
        return "watch_priority_active"
    if event.get("one_hop") and side in {"sell", "cex_deposit"}:
        return "front_row_risk" if front_row else "one_hop_risk"
    if event.get("one_hop") and side in {"buy", "cex_withdraw"}:
        return "front_row_accum" if front_row else "one_hop_accum"
    if side in {"core_top_holder", "top_holder"}:
        return "front_row_active"
    if front_row and side in {"sell", "cex_deposit", "distributor"}:
        return "front_row_risk"
    if front_row and side in {"buy", "cex_withdraw", "accumulator"}:
        return "front_row_accum"
    return None


def _st_priority_flow_score(event):
    side = event.get("side")
    usd = float(event.get("usd", 0) or 0)
    label = str(event.get("label") or "")
    front_row = "核心前排钱包" in label or "前排持仓地址" in label or bool(event.get("manual_priority"))
    one_hop_factor = 1.25 if event.get("one_hop") else 1.0
    watch_factor = 1.6 if event.get("watch_priority") else 1.0
    manual_factor = 1.8 if event.get("manual_priority") else 1.0
    if side == "buy":
        return usd * (1.5 if front_row else 1.0) * one_hop_factor * watch_factor * manual_factor
    if side == "cex_withdraw":
        return usd * (1.3 if front_row else 0.8) * one_hop_factor * watch_factor * manual_factor
    if side in {"accumulator", "accumulator_priority"}:
        return usd * 0.8 * manual_factor
    if side == "sell":
        return -usd * (1.5 if front_row else 1.0) * one_hop_factor * watch_factor * manual_factor
    if side == "cex_deposit":
        return -usd * (1.3 if front_row else 0.8) * one_hop_factor * watch_factor * manual_factor
    if side == "distributor":
        return -usd * 0.8 * manual_factor
    if side in {"project_treasury", "static_treasury", "core_hub", "hidden_hub", "standard_whale"}:
        direction = -1.0 if event.get("manual_static_source") else 0.6
        return direction * usd * manual_factor
    return 0.0


def _st_human_block(what, meaning, action):
    return f"发生了什么：{what}\n说明什么：{meaning}\n建议怎么看：{action}"


def _st_action_line(state_payload):
    state_name = str((state_payload or {}).get("state") or "")
    if state_name == "系统性风险升高":
        return "先控仓位，不加仓；如果继续流向 DEX 或确认交易所，按派发风险处理。"
    if state_name == "主力派发中":
        return "不追高，持仓先降风险；等卖压停止、链上不再外流后再看。"
    if state_name == "主力吸筹中":
        return "继续跟踪承接，回踩有承接才考虑；单根拉升不追。"
    return "先不动作，等它从调仓变成连续吸筹或连续派发。"


def _st_state_compact_field(state_payload):
    state_payload = state_payload or {}
    return (
        "结论/操作",
        f"{state_payload.get('state', '主力试盘/调仓')}\n{_st_action_line(state_payload)}",
        False,
    )


def _st_event_brief(event):
    side = str(event.get("side") or "")
    amount_text = f"{money(event.get('usd', 0))} / {fnum(event.get('amount', 0))} ST"
    manual_name = str(event.get("manual_label_name") or "")
    label = str(event.get("label") or "")
    base = manual_name or label or "重点地址出现动作"

    if event.get("manual_static_first_outflow"):
        return _st_human_block(
            f"{base} 出现首次明显转出，规模约 {amount_text}。",
            "静态仓或项目方仓开始往外流，通常比普通大户活跃更敏感，容易抬高系统性风险。",
            "先看去向是不是 DEX 或交易所；如果后续连续出现同方向外流，优先按风险处理。",
        )
    if side == "buy":
        return _st_human_block(
            f"{base} 从 DEX 承接，规模约 {amount_text}。",
            "链上表现偏向主动拿货；如果是前排、手工重点地址或 one-hop 地址，吸筹意义更强。",
            "结合后续是否继续承接、是否伴随交易所提币，一起判断是不是持续吸筹。",
        )
    if side == "sell":
        return _st_human_block(
            f"{base} 向 DEX 打入，规模约 {amount_text}。",
            "链上表现偏向抛压释放；如果来自前排、核心 hub 或静态仓，需要提高警惕。",
            "先看是不是一次性换手，还是连续流向 DEX；连续出现时更接近派发。",
        )
    if side == "cex_withdraw":
        return _st_human_block(
            f"{base} 从交易所提币，规模约 {amount_text}。",
            "通常偏向筹码回收、囤币或场外转移；若和 DEX 承接同时出现，吸筹信号更强。",
            "继续观察这批币是否沉淀在静态仓、吸筹地址或扩散到 one-hop 钱包。",
        )
    if side == "cex_deposit":
        return _st_human_block(
            f"{base} 向交易所充值，规模约 {amount_text}。",
            "通常偏向可卖筹码增加；若来自前排地址、核心 hub 或静态仓，风险级别更高。",
            "重点看后续是否继续充值、是否同步出现 DEX 卖压，确认是不是派发升级。",
        )
    if side in {"accumulator", "accumulator_priority"}:
        return _st_human_block(
            f"{base} 再次出现净流入吸筹动作，规模约 {amount_text}。",
            "说明资金仍在回收筹码，这类地址更适合用来观察主力是否持续拿货。",
            "看它后面是一跳扩散，还是继续沉淀；持续沉淀通常比来回换手更偏多。",
        )
    if side in {"core_hub", "hidden_hub", "hub"}:
        return _st_human_block(
            f"{base} 出现大额流转，规模约 {amount_text}。",
            "这更像主力资金在调度仓位或分发通道，还不能单独下结论说一定吸筹或派发。",
            "一定要结合去向看：去 DEX 偏风险，去静态仓或吸筹地址偏调仓/吸筹。",
        )
    if side in {"project_treasury", "static_treasury", "standard_whale", "core_top_holder", "top_holder", "watch_priority"}:
        return _st_human_block(
            f"{base} 出现链上动作，规模约 {amount_text}。",
            "重点地址开始动了，说明原本沉淀或前排筹码出现变化，市场参考价值高于普通大额转账。",
            "先看动作方向，再看是否连续；同方向连续出现时，实盘意义会明显增强。",
        )
    return _st_human_block(
        f"{base} 出现大额转账，规模约 {amount_text}。",
        "这是链上异动，但单看一笔还不足以直接判断涨跌方向。",
        "优先结合后续去向、是否连续发生、是否命中重点地址，再做交易判断。",
    )


def _st_master_state(window, front_row_summary=None):
    front_row_summary = front_row_summary or {}
    scoped = list(window or [])
    if not scoped:
        return {
            "state": "主力试盘/调仓",
            "title": "🟠 主力试盘/调仓",
            "what": "当前重点地址有零散动作，但还没有形成明显同向趋势。",
            "meaning": "更像在换手、调仓、测试流动性，而不是明确单边吸筹或派发。",
            "action": "先盯后续是否转成连续买入、连续卖出，或出现交易所/DEX同向确认。",
        }

    buy_usd = sum(float(x.get("usd", 0) or 0) for x in scoped if x.get("side") in {"buy", "cex_withdraw", "accumulator", "accumulator_priority"})
    sell_usd = sum(float(x.get("usd", 0) or 0) for x in scoped if x.get("side") in {"sell", "cex_deposit", "distributor"})
    net = buy_usd - sell_usd
    manual_hits = sum(1 for x in scoped if x.get("manual_priority"))
    one_hop_hits = sum(1 for x in scoped if x.get("one_hop"))
    strong_cex_hits = sum(1 for x in scoped if x.get("cex_strength") == "strong_confirmed")
    static_outflows = sum(1 for x in scoped if x.get("manual_static_first_outflow"))
    front_buy = float(front_row_summary.get("front_row_buy_usd", 0) or 0)
    front_sell = float(front_row_summary.get("front_row_sell_usd", 0) or 0)

    if static_outflows or (strong_cex_hits >= 2 and sell_usd >= buy_usd) or net <= -ST_PRIORITY_FLOW_IMBALANCE_USD * 1.2:
        return {
            "state": "系统性风险升高",
            "title": "🚨 系统性风险升高",
            "what": f"重点地址的外流、交易所流向或前排卖压正在抬升，当前净偏空约 {money(-net if net < 0 else 0)}。",
            "meaning": "这不一定等于立刻下跌，但说明可卖筹码正在增加，且风险不再只是单点异动。",
            "action": "优先看是否连续流向 DEX/confirmed_exchange，若持续发生，实盘上应先降杠杆、控回撤。",
        }
    if net >= ST_PRIORITY_FLOW_IMBALANCE_USD and (manual_hits >= 2 or one_hop_hits >= 2 or front_buy > front_sell):
        return {
            "state": "主力吸筹中",
            "title": "🟢 主力吸筹中",
            "what": f"重点地址、前排或 one-hop 路径的承接/提币更强，当前净偏多约 {money(net)}。",
            "meaning": "这更接近主力在回收筹码，而不是单纯散户换手；人工重点地址参与越多，可信度越高。",
            "action": "继续看吸筹是否连续、是否伴随交易所提币和静态沉淀；连续出现时更适合顺势跟踪。",
        }
    if net <= -ST_PRIORITY_FLOW_IMBALANCE_USD and (manual_hits >= 2 or one_hop_hits >= 2 or front_sell > front_buy):
        return {
            "state": "主力派发中",
            "title": "🔴 主力派发中",
            "what": f"重点地址更偏向卖压、充值交易所或向 DEX 释放筹码，当前净偏空约 {money(-net)}。",
            "meaning": "这更接近主力把筹码往外分发，而不是普通噪音换手。",
            "action": "重点看 confirmed_exchange 充值和连续 DEX 卖压是否同步出现；同步时风险最高。",
        }
    return {
        "state": "主力试盘/调仓",
        "title": "🟠 主力试盘/调仓",
        "what": f"重点地址活跃，但买卖两边都在出手，当前净差约 {money(abs(net))}。",
        "meaning": "更像主力在测流动性、换手或挪仓，还没有形成明确吸筹或派发方向。",
        "action": "先观察后续去向：往静态仓/吸筹地址走偏多，往 DEX/交易所走偏空。",
    }


def _st_state_field(state_payload):
    return (
        "主力状态",
        _st_human_block(
            state_payload["what"],
            state_payload["meaning"],
            state_payload["action"],
        ),
        False,
    )


def _st_actor_window_stats(window):
    stats = {}
    for item in window or []:
        actor = str(item.get("actor") or "").lower()
        if not actor:
            continue
        row = stats.setdefault(
            actor,
            {
                "actor": actor,
                "buy_usd": 0.0,
                "sell_usd": 0.0,
                "events": 0,
                "hashes": [],
                "front_row": False,
                "one_hop": False,
                "watch_priority": False,
                "manual_priority": False,
                "manual_static_first_outflow": False,
                "confirmed_cex": 0,
                "structural_cex": 0,
                "sides": {},
            },
        )
        side = str(item.get("side") or "")
        usd = float(item.get("usd", 0) or 0)
        row["events"] += 1
        row["hashes"].append(str(item.get("hash") or ""))
        row["front_row"] = row["front_row"] or bool(item.get("front_row"))
        row["one_hop"] = row["one_hop"] or bool(item.get("one_hop"))
        row["watch_priority"] = row["watch_priority"] or bool(item.get("watch_priority"))
        row["manual_priority"] = row["manual_priority"] or bool(item.get("manual_priority"))
        row["manual_static_first_outflow"] = row["manual_static_first_outflow"] or bool(item.get("manual_static_first_outflow"))
        if item.get("cex_strength") == "strong_confirmed":
            row["confirmed_cex"] += 1
        elif item.get("cex_strength") == "strong_structural":
            row["structural_cex"] += 1
        row["sides"][side] = row["sides"].get(side, 0) + 1
        if side in {"buy", "cex_withdraw", "accumulator", "accumulator_priority"}:
            row["buy_usd"] += usd
        elif side in {"sell", "cex_deposit", "distributor"}:
            row["sell_usd"] += usd
    return stats


def _st_actor_main_alert(window, now_ts):
    stats = _st_actor_window_stats(window)
    candidates = []
    for actor, row in stats.items():
        touched = row["manual_priority"] or row["watch_priority"] or row["front_row"] or row["one_hop"]
        gross = row["buy_usd"] + row["sell_usd"]
        if not touched or gross < ST_WHALE_TRANSFER_USD:
            continue
        net = row["buy_usd"] - row["sell_usd"]
        label = _st_whale_side({"from": actor, "to": actor, "value": 0, "_state": {}}, set())[2] if False else _st_whale_short(actor)
        summary = {
            "actor": actor,
            "display": _st_whale_short(actor),
            "gross": gross,
            "net": net,
            "events": row["events"],
            "buy_usd": row["buy_usd"],
            "sell_usd": row["sell_usd"],
            "confirmed_cex": row["confirmed_cex"],
            "structural_cex": row["structural_cex"],
            "manual_priority": row["manual_priority"],
            "watch_priority": row["watch_priority"],
            "front_row": row["front_row"],
            "one_hop": row["one_hop"],
            "manual_static_first_outflow": row["manual_static_first_outflow"],
            "hashes": [h for h in row["hashes"] if h][:4],
        }
        if row["manual_static_first_outflow"] or row["confirmed_cex"] >= 1:
            summary["kind"] = "high_risk"
            summary["title"] = "🚨 ST 高风险异动"
            summary["what"] = f"{summary['display']} 在 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟内集中出现高风险路径，累计约 {money(gross)}。"
            summary["meaning"] = "这更像关键筹码正在往可卖路径移动，风险高于普通前排活跃。"
            summary["action"] = "先看是不是继续流向 DEX 或 confirmed_exchange；如果连续出现，优先按风险处理。"
        elif net <= -max(ST_PRIORITY_EVENT_USD, ST_WHALE_REPEAT_USD):
            summary["kind"] = "distribute"
            summary["title"] = "🔴 ST 主力偏派发"
            summary["what"] = f"{summary['display']} 在 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟内净偏空约 {money(-net)}，累计 {row['events']} 笔。"
            summary["meaning"] = "同一核心地址连续往卖压方向动作，更像在派发或外流，而不是普通换手。"
            summary["action"] = "继续看是否同步出现 DEX 卖压、交易所充值或一跳扩散；连续同向时风险更高。"
        elif net >= max(ST_PRIORITY_EVENT_USD, ST_WHALE_REPEAT_USD):
            summary["kind"] = "accum"
            summary["title"] = "🟢 ST 主力偏吸筹"
            summary["what"] = f"{summary['display']} 在 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟内净偏多约 {money(net)}，累计 {row['events']} 笔。"
            summary["meaning"] = "同一核心地址连续承接、提币或回收筹码，更像主力在吸筹，而不是零散试盘。"
            summary["action"] = "继续看是否沉淀到静态仓、吸筹地址或扩散到 one-hop；连续承接时更偏多。"
        else:
            continue
        priority_score = (
            gross
            + (row["confirmed_cex"] * 100000)
            + (50000 if row["manual_static_first_outflow"] else 0)
            + (20000 if row["manual_priority"] else 0)
            + (10000 if row["watch_priority"] else 0)
        )
        summary["score"] = priority_score
        candidates.append(summary)
    if not candidates:
        return None
    candidates.sort(key=lambda x: x["score"], reverse=True)
    chosen = candidates[0]
    chosen["window_id"] = int(now_ts // ST_WHALE_WINDOW_SECONDS)
    return chosen


def _st_actor_main_alert_field(alert_payload):
    detail = (
        f"{alert_payload['display']} | {alert_payload['events']} 笔 | "
        f"净偏多 {money(alert_payload['buy_usd'])} / 净偏空 {money(alert_payload['sell_usd'])}"
    )
    tx_line = ""
    if alert_payload.get("hashes"):
        tx_line = "\n" + "\n".join(f"Tx: `{h}`" for h in alert_payload["hashes"][:2])
    return ("核心地址", detail + tx_line, False)


def _st_whale_cooldown_ok(state, key):
    now = time.time()
    cooldowns = state.setdefault("st_whale_alert_cooldowns", {})
    last = float(cooldowns.get(key, 0) or 0)
    if now - last < ST_WHALE_ALERT_COOLDOWN_SECONDS:
        return False
    cooldowns[key] = now
    return True


def check_whale_activity(state):
    now = time.time()
    if now - float(state.get("last_whale_check_ts", 0) or 0) < ST_WHALE_CHECK_INTERVAL_SECONDS:
        return
    state["last_whale_check_ts"] = now

    price = _st_whale_price(state)
    if not price:
        logging.info("ST whale check skipped: no price")
        return

    pair_addrs = _st_whale_fetch_pairs(state)
    rows = _st_whale_fetch_transfers(limit=50)
    if not rows:
        return

    latest_hashes = [r.get("hash") for r in rows if r.get("hash")]
    if not state.get("st_whale_monitor_initialized"):
        state["seen_whale_transfer_hashes"] = latest_hashes[-200:]
        state["st_whale_monitor_initialized"] = True
        discord_send(
            "👁️ ST 大户行为监控已启动",
            f"已识别 DEX 池子 {len(pair_addrs)} 个；后续只提醒新发生的大额扫货/出货。\n阈值：单笔 {money(ST_WHALE_TRANSFER_USD)}，窗口 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟。",
            [("DEX池", "\n".join(state.get("st_dex_pair_labels", [])[:6]) or "-", False)],
            color=0x5865F2,
        )
        return

    seen = set(state.get("seen_whale_transfer_hashes") or [])
    manual_static_alerted = set(state.get("st_manual_static_first_outflow_alerted") or [])
    new_events = []
    for row in reversed(rows):
        txh = row.get("hash")
        if not txh or txh in seen:
            continue
        amount = _st_whale_amount(row)
        usd = amount * price
        row["_state"] = state
        side, actor, label, tags = _st_whale_side(row, pair_addrs)
        event = {
            "ts": int(row.get("timeStamp") or now),
            "hash": txh,
            "side": side,
            "actor": actor,
            "amount": amount,
            "usd": usd,
            "label": label,
            "from": row.get("from"),
            "to": row.get("to"),
            "front_row": bool((tags or {}).get("front_row")),
            "one_hop": bool((tags or {}).get("one_hop")),
            "cex_strength": str((tags or {}).get("cex_strength") or ""),
            "one_hop_escalated": bool((tags or {}).get("one_hop_escalated")),
            "watch_priority": bool((tags or {}).get("watch_priority")),
            "manual_priority": bool((tags or {}).get("manual_priority")),
            "manual_label_type": str((tags or {}).get("manual_label_type") or ""),
            "manual_label_name": str((tags or {}).get("manual_label_name") or ""),
            "manual_static_source": bool((tags or {}).get("manual_static_source")),
            "manual_static_target": bool((tags or {}).get("manual_static_target")),
        }
        if (
            event["manual_static_source"]
            and event["usd"] >= ST_PRIORITY_EVENT_USD
            and actor
            and actor not in manual_static_alerted
            and side in {"sell", "cex_deposit", "transfer", "static_treasury", "project_treasury"}
        ):
            event["manual_static_first_outflow"] = True
            manual_static_alerted.add(actor)
        else:
            event["manual_static_first_outflow"] = False
        new_events.append(event)
        seen.add(txh)
        if event["one_hop"]:
            _st_update_one_hop_event(state, actor, side, escalated=event["one_hop_escalated"])
        if event["watch_priority"]:
            logging.info("ST watch_priority hit: side=%s actor=%s label=%s tx=%s", side, actor, label, txh)
        if event["manual_priority"]:
            logging.info(
                "ST manual label hit: side=%s actor=%s manual_type=%s manual_label=%s one_hop=%s tx=%s",
                side,
                actor,
                event["manual_label_type"] or "-",
                event["manual_label_name"] or label,
                event["one_hop"],
                txh,
            )
        if side in {"cex_deposit", "cex_withdraw"} or "交易所相关地址" in label:
            logging.info("ST generic CEX tag hit: side=%s tier=%s label=%s tx=%s", side, event["cex_strength"] or "-", label, txh)

    logging.info(
        "ST whale radar heartbeat: price=%s pairs=%s rows=%s new_events=%s initialized=%s",
        price,
        len(pair_addrs),
        len(rows),
        len(new_events),
        state.get("st_whale_monitor_initialized"),
    )

    if not new_events:
        state["seen_whale_transfer_hashes"] = latest_hashes[-200:]
        return

    preview_window = [x for x in (state.get("st_whale_flow_window") or []) if int(x.get("ts", 0)) >= int(now - ST_WHALE_WINDOW_SECONDS)]
    for e in new_events:
        if e["side"] in {
            "buy",
            "sell",
            "hub",
            "core_top_holder",
            "top_holder",
            "accumulator",
            "accumulator_priority",
            "distributor",
            "cex_deposit",
            "cex_withdraw",
            "project_treasury",
            "static_treasury",
            "core_hub",
            "hidden_hub",
            "standard_whale",
        }:
            preview_window.append({
                k: e[k]
                for k in (
                    "ts",
                    "hash",
                    "side",
                    "actor",
                    "amount",
                    "usd",
                    "front_row",
                    "one_hop",
                    "cex_strength",
                    "watch_priority",
                    "manual_priority",
                    "manual_static_first_outflow",
                )
                if k in e
            })
    preview_front_row_summary = _st_front_row_summary(preview_window, now)
    master_state = _st_master_state(preview_window, preview_front_row_summary)

    fields = []
    for e in new_events:
        if e["usd"] >= ST_WHALE_TRANSFER_USD:
            fields.append((
                f"{money(e['usd'])} / {fnum(e['amount'])} ST",
                f"{_st_event_brief(e)}\n{_st_whale_short(e['from'])} -> {_st_whale_short(e['to'])}\nTx: `{e['hash']}`",
                False,
            ))

    if fields and ST_VERBOSE_RAW_ALERTS and _st_whale_cooldown_ok(state, "large_tx"):
        discord_send(
            "🟠 ST 大户链上动作",
            _st_human_block(
                f"刚出现 {len(fields)} 笔超过 {money(ST_WHALE_TRANSFER_USD)} 的新链上动作。",
                "这是链上异动总览，里面既可能有吸筹，也可能有派发或调仓。",
                "不要单看金额，优先看是否命中前排、一跳、人工重点地址，以及最终流向 DEX 还是交易所。",
            ),
            fields[:6],
            color=0xFEE75C,
        )

    priority_fields = []
    priority_buckets = set()
    for e in new_events:
        bucket = _st_priority_bucket(e)
        if not bucket:
            continue
        priority_buckets.add(bucket)
        priority_fields.append((
            f"{money(e['usd'])} / {fnum(e['amount'])} ST",
            f"{_st_event_brief(e)}\n{_st_whale_short(e['from'])} -> {_st_whale_short(e['to'])}\nTx: `{e['hash']}`",
            False,
        ))

    # Priority / front-row / one-hop details are now folded into one actor-level
    # decision alert per 15m window. These raw fields are kept for summary only.

    one_hop_dex_fields = []
    one_hop_exchange_confirmed_fields = []
    one_hop_exchange_structural_fields = []
    for e in new_events:
        if not e.get("one_hop") or e["usd"] < ST_WHALE_TRANSFER_USD:
            continue
        detail = (
            f"{e['label']}\n{_st_whale_short(e['from'])} -> {_st_whale_short(e['to'])}\n"
            f"Tx: `{e['hash']}`"
        )
        if e["side"] in {"buy", "sell"}:
            one_hop_dex_fields.append((f"{money(e['usd'])} / {fnum(e['amount'])} ST", detail, False))
        elif e["side"] in {"cex_deposit", "cex_withdraw"}:
            field = (f"{money(e['usd'])} / {fnum(e['amount'])} ST", detail, False)
            if e.get("cex_strength") == "strong_confirmed":
                one_hop_exchange_confirmed_fields.append(field)
            elif e.get("cex_strength") == "strong_structural":
                one_hop_exchange_structural_fields.append(field)

    if one_hop_dex_fields and ST_VERBOSE_RAW_ALERTS and _st_whale_cooldown_ok(state, "one_hop_dex_flow"):
        title = "🔴 ST one-hop DEX卖压" if any(e.get("side") == "sell" and e.get("one_hop") for e in new_events) else "🟢 ST one-hop DEX承接"
        desc = _st_human_block(
            "一跳观察地址直接和 DEX 发生了交互。",
            "这说明主地址影响正在向外扩散，且已经进入可交易路径，参考价值高于普通转账。",
            "如果是一跳去 DEX 偏风险；如果是一跳从 DEX 承接偏吸筹，关键是看是否连续。",
        )
        discord_send(title, desc, one_hop_dex_fields[:6], color=0xED4245 if "卖压" in title else 0x57F287)

    if one_hop_exchange_confirmed_fields and ST_VERBOSE_RAW_ALERTS and _st_whale_cooldown_ok(state, "one_hop_exchange_flow_confirmed"):
        discord_send(
            "🔴 ST one-hop 已确认交易所流向",
            _st_human_block(
                "一跳观察地址已经走到 confirmed_exchange 级别的交易所地址。",
                "这类路径通常比结构性线索更接近真实可卖/可提路径，交易意义更强。",
                "优先把它当高置信交易所流向看，再观察是否连续充值或连续提币。",
            ),
            one_hop_exchange_confirmed_fields[:6],
            color=0xED4245,
        )
    if one_hop_exchange_structural_fields and ST_VERBOSE_RAW_ALERTS and _st_whale_cooldown_ok(state, "one_hop_exchange_flow_structural"):
        discord_send(
            "🟠 ST one-hop 结构性交换所流向",
            _st_human_block(
                "一跳观察地址碰到了 structural_exchange 级别的交易所线索。",
                "这说明路径可疑度不低，但还不到最高置信级别，更像结构性证据。",
                "先结合后续是否继续流向 confirmed_exchange 或 DEX，再决定是否上调风险判断。",
            ),
            one_hop_exchange_structural_fields[:6],
            color=0xFEE75C,
        )

    window = state.get("st_whale_flow_window") or []
    cutoff = int(now - ST_WHALE_WINDOW_SECONDS)
    window = [x for x in window if int(x.get("ts", 0)) >= cutoff]
    for e in new_events:
        if e["side"] in {"buy", "sell", "hub", "core_top_holder", "top_holder", "accumulator", "accumulator_priority", "distributor", "cex_deposit", "cex_withdraw", "project_treasury", "static_treasury", "core_hub", "hidden_hub", "standard_whale"}:
            window.append({k: e[k] for k in ("ts", "hash", "side", "actor", "amount", "usd", "front_row", "one_hop", "cex_strength", "watch_priority", "manual_priority", "manual_static_first_outflow") if k in e})
    state["st_whale_flow_window"] = window[-200:]
    state["st_front_row_summary"] = _st_front_row_summary(state["st_whale_flow_window"], now)
    front_row_summary = state.get("st_front_row_summary") or {}
    current_master_state = _st_master_state(state["st_whale_flow_window"], front_row_summary)
    state["st_master_state"] = current_master_state
    logging.info(
        "ST front_row summary: state=%s buy=%s sell=%s cex_deposit=%s cex_withdraw=%s active_wallets=%s active_one_hop=%s related_hits=%s confirmed_cex=%s structural_cex=%s one-hop=%s",
        current_master_state.get("state"),
        money(front_row_summary.get("front_row_buy_usd", 0)),
        money(front_row_summary.get("front_row_sell_usd", 0)),
        money(front_row_summary.get("front_row_cex_deposit_usd", 0)),
        money(front_row_summary.get("front_row_cex_withdraw_usd", 0)),
        front_row_summary.get("active_front_row_wallets", 0),
        front_row_summary.get("active_one_hop_wallets", 0),
        front_row_summary.get("front_row_related_hits", 0),
        front_row_summary.get("confirmed_cex_hits", 0),
        front_row_summary.get("structural_cex_hits", 0),
        len(state.get("st_one_hop_watch") or {}),
    )

    buy_usd = sum(float(x.get("usd", 0) or 0) for x in window if x.get("side") == "buy")
    sell_usd = sum(float(x.get("usd", 0) or 0) for x in window if x.get("side") == "sell")
    net = buy_usd - sell_usd

    if net >= ST_WHALE_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "flow_buy"):
        discord_send(
            "🟢 ST 大户扫货占优",
            _st_human_block(
                f"最近 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟，链上净扫货约 {money(net)}。",
                "说明 DEX 池子里的 ST 正在被拿走，短线更偏吸筹而不是抛压。",
                "继续看这批承接是否来自前排、one-hop 或人工重点地址，来源越强，参考价值越高。",
            ),
            [_st_state_compact_field(current_master_state), _st_state_field(current_master_state)],
            color=0x57F287,
        )
    elif -net >= ST_WHALE_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "flow_sell"):
        discord_send(
            "🔴 ST 大户出货占优",
            _st_human_block(
                f"最近 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟，链上净出货约 {money(-net)}。",
                "说明 ST 正在被持续打回 DEX，短线卖压在抬升。",
                "先看是不是一次性释放，还是连续卖出；若叠加交易所充值和重点地址外流，风险更高。",
            ),
            [_st_state_compact_field(current_master_state), _st_state_field(current_master_state)],
            color=0xED4245,
        )

    wallet_stats = {}
    for x in window:
        actor = x.get("actor")
        if not actor:
            continue
        key = (x.get("side"), actor)
        stat = wallet_stats.setdefault(key, {"usd": 0.0, "count": 0})
        stat["usd"] += float(x.get("usd", 0) or 0)
        stat["count"] += 1

    repeat_fields = []
    one_hop_repeat_fields = []
    watch_priority_repeat_fields = []
    for (side, actor), stat in wallet_stats.items():
        if stat["count"] >= 2 and stat["usd"] >= ST_WHALE_REPEAT_USD:
            label = "连续扫货" if side == "buy" else "连续出货"
            repeat_fields.append((label, f"{_st_whale_short(actor)} | {stat['count']} 笔 | {money(stat['usd'])}", False))

    watch_priority_wallet_stats = {}
    for x in window:
        if not x.get("watch_priority"):
            continue
        actor = x.get("actor")
        if not actor:
            continue
        key = (x.get("side"), actor)
        stat = watch_priority_wallet_stats.setdefault(key, {"usd": 0.0, "count": 0})
        stat["usd"] += float(x.get("usd", 0) or 0)
        stat["count"] += 1
    for (side, actor), stat in watch_priority_wallet_stats.items():
        if stat["count"] >= 2 and stat["usd"] >= ST_WHALE_REPEAT_USD:
            label = "watch_priority 连续扫货" if side in {"buy", "cex_withdraw", "accumulator"} else "watch_priority 连续出货"
            watch_priority_repeat_fields.append((label, f"{_st_whale_short(actor)} | {stat['count']} 笔 | {money(stat['usd'])}", False))

    one_hop_wallet_stats = {}
    for x in window:
        if not x.get("one_hop"):
            continue
        actor = x.get("actor")
        if not actor:
            continue
        key = (x.get("side"), actor)
        stat = one_hop_wallet_stats.setdefault(key, {"usd": 0.0, "count": 0})
        stat["usd"] += float(x.get("usd", 0) or 0)
        stat["count"] += 1
    for (side, actor), stat in one_hop_wallet_stats.items():
        if stat["count"] >= 2 and stat["usd"] >= ST_WHALE_REPEAT_USD:
            one_hop_repeat_fields.append((f"one-hop {side}", f"{_st_whale_short(actor)} | {stat['count']} 笔 | {money(stat['usd'])}", False))

    # Repeat / watch_priority / one-hop activity is summarized into the single
    # actor-level main alert below instead of sending separate notifications.

    actor_main_alert = _st_actor_main_alert(window, now)
    if actor_main_alert:
        sent_map = state.setdefault("st_actor_main_alerts", {})
        actor_key = actor_main_alert["actor"]
        window_key = str(actor_main_alert["window_id"])
        already = str((sent_map.get(actor_key) or {}).get("window_id") or "")
        cooldown_key = f"actor_main_{actor_key}_{actor_main_alert['kind']}"
        if already != window_key and _st_whale_cooldown_ok(state, cooldown_key):
            discord_send(
                actor_main_alert["title"],
                _st_human_block(
                    actor_main_alert["what"],
                    actor_main_alert["meaning"],
                    actor_main_alert["action"],
                ),
                [_st_state_compact_field(current_master_state), _st_actor_main_alert_field(actor_main_alert), _st_state_field(current_master_state)],
                color=0x57F287 if actor_main_alert["kind"] == "accum" else 0xED4245,
            )
            sent_map[actor_key] = {"window_id": actor_main_alert["window_id"], "kind": actor_main_alert["kind"], "ts": int(now)}

    priority_window = state.get("st_priority_flow_window") or []
    priority_window = [x for x in priority_window if int(x.get("ts", 0)) >= cutoff]
    for e in new_events:
        score = _st_priority_flow_score(e)
        if score:
            priority_window.append({"ts": e["ts"], "score": score})
    state["st_priority_flow_window"] = priority_window[-200:]
    priority_net = sum(float(x.get("score", 0) or 0) for x in priority_window)
    if priority_net >= ST_PRIORITY_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "priority_flow_buy"):
        discord_send(
            "🟢 ST 重点钱包净吸筹占优",
            _st_human_block(
                f"最近 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟，重点钱包加权净吸筹约 {money(priority_net)}。",
                "这是把前排、one-hop、人工重点地址、提币和 DEX 承接一起算后的综合偏多结果。",
                "适合用来判断主力是不是在持续回收筹码，但仍需防止单次假动作。",
            ),
            [_st_state_compact_field(current_master_state), _st_state_field(current_master_state)],
            color=0x57F287,
        )
    elif -priority_net >= ST_PRIORITY_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "priority_flow_sell"):
        discord_send(
            "🔴 ST 重点钱包净卖压占优",
            _st_human_block(
                f"最近 {int(ST_WHALE_WINDOW_SECONDS/60)} 分钟，重点钱包加权净卖压约 {money(-priority_net)}。",
                "这是把前排、one-hop、人工重点地址、交易所流向和 DEX 卖压综合后的偏空结果。",
                "如果后续继续走弱并叠加静态仓外流，就更接近系统性风险升高。",
            ),
            [_st_state_compact_field(current_master_state), _st_state_field(current_master_state)],
            color=0xED4245,
        )

    state["seen_whale_transfer_hashes"] = list(seen)[-300:]
    state["st_manual_static_first_outflow_alerted"] = sorted(manual_static_alerted)


def send_summary(state, reason="定时持仓快照"):
    prices = state.get("prices", {}) if isinstance(state, dict) else {}
    liquidity = state.get("liquidity", {}) if isinstance(state, dict) else {}

    dex_price = prices.get("dex_best")
    total_liq = liquidity.get("total")
    seen_pairs = state.get("seen_pairs", {}) if isinstance(state, dict) else {}

    amount = float(os.getenv("ST_HOLDING_AMOUNT", "0") or 0)
    entry_price = float(os.getenv("ST_ENTRY_PRICE", "0") or 0)
    mark_price = float(dex_price or prices.get("cex:BitMart") or prices.get("cex:XT") or prices.get("cex:MEXC") or prices.get("cex:Phemex") or 0)

    fields = []

    if amount and entry_price and mark_price:
        cost = amount * entry_price
        value = amount * mark_price
        pnl = value - cost
        pnl_pct = (mark_price - entry_price) / entry_price * 100 if entry_price else 0
        fields.append((
            "个人持仓",
            (
                f"数量 {amount:,.2f} ST\n"
                f"成本 {fnum(entry_price)} | 当前 {fnum(mark_price)}\n"
                f"成本金额 {money(cost)} | 当前市值 {money(value)}\n"
                f"浮盈亏 {money(pnl)} ({pnl_pct:+.2f}%)"
            ),
            False,
        ))

    fields.append((
        "DEX",
        f"价格 {fnum(dex_price)} | 总流动性 {money(total_liq)} | 池数量 {len(seen_pairs)}",
        False,
    ))

    cex_lines = []
    for key, value in sorted(prices.items()):
        if str(key).startswith("cex:"):
            cex_lines.append(f"{str(key).split(':', 1)[1]}: {fnum(value)}")
    if cex_lines:
        fields.append(("CEX", "\n".join(cex_lines), False))

    if state.get("last_markets_summary"):
        fields.append(("市场来源", state["last_markets_summary"], False))
    front_row_field = _st_front_row_summary_field(state)
    if front_row_field:
        fields.append(front_row_field)
    master_state = state.get("st_master_state") or _st_master_state(state.get("st_whale_flow_window") or [], state.get("st_front_row_summary") or {})
    fields.append(_st_state_compact_field(master_state))
    fields.append(_st_state_field(master_state))

    return discord_send(
        f"📌 ST {reason}",
        (
            f"{TOKEN_NAME} ({TOKEN_SYMBOL})\n"
            f"合约: `{TOKEN_ADDRESS}`\n"
            f"说明：ST 没有合约，持仓提醒只按 DEX、CEX现货、链上路径和重点钱包判断。\n"
            f"当前归因：{master_state.get('state')}\n"
            f"操作：{_st_action_line(master_state)}"
        ),
        fields,
        color=0x5865F2,
    )


def main():
    load_env_file()
    if os.environ.get("ST_HOLDING_ENABLED", "1").lower() in {"0", "false", "off", "no"}:
        print("ST holding monitor disabled")
        return

    state = load_state()
    discord_send(
        "✅ ST 个人持仓监控已启动",
        (
            f"{TOKEN_NAME} ({TOKEN_SYMBOL})\n`{TOKEN_ADDRESS}`\n"
            "无合约币模式：不看 OI/资金费率，只看 DEX、CEX现货、链上路径和重点钱包。\n"
            f"原始明细提醒：{'开启' if ST_VERBOSE_RAW_ALERTS else '关闭'}"
        ),
        color=0x2ECC71,
    )

    while True:
        try:
            check_events(state)
            check_whale_activity(state)
            now_ts = time.time()
            if now_ts - float(state.get("last_forced_summary_ts", 0) or 0) >= FORCE_SUMMARY_INTERVAL_SECONDS:
                prices = state.get("prices", {})
                liquidity = state.get("liquidity", {})
                logging.info(
                    "ST holding heartbeat: dex_best=%s cex=%s liquidity=%s pairs=%s front_row=%s one-hop=%s",
                    prices.get("dex_best"),
                    {k: v for k, v in prices.items() if str(k).startswith("cex:")},
                    liquidity.get("total"),
                    len(state.get("seen_pairs", {})),
                    {
                        "buy_usd": money((state.get("st_front_row_summary") or {}).get("front_row_buy_usd", 0)),
                        "sell_usd": money((state.get("st_front_row_summary") or {}).get("front_row_sell_usd", 0)),
                        "cex_deposit_usd": money((state.get("st_front_row_summary") or {}).get("front_row_cex_deposit_usd", 0)),
                        "cex_withdraw_usd": money((state.get("st_front_row_summary") or {}).get("front_row_cex_withdraw_usd", 0)),
                        "active_wallets": int((state.get("st_front_row_summary") or {}).get("active_front_row_wallets", 0) or 0),
                        "active_one_hop_wallets": int((state.get("st_front_row_summary") or {}).get("active_one_hop_wallets", 0) or 0),
                        "front_row_related_hits": int((state.get("st_front_row_summary") or {}).get("front_row_related_hits", 0) or 0),
                        "confirmed_cex_hits": int((state.get("st_front_row_summary") or {}).get("confirmed_cex_hits", 0) or 0),
                        "structural_cex_hits": int((state.get("st_front_row_summary") or {}).get("structural_cex_hits", 0) or 0),
                    },
                    len(state.get("st_one_hop_watch") or {}),
                )
                send_summary(state, reason="定时持仓快照")
                state["last_forced_summary_ts"] = now_ts
            save_state(state)
        except Exception as e:
            print("loop error", repr(e))
            traceback.print_exc()
            try:
                alert_once(state, "loop_error", "⚠️ ST 持仓监控异常", str(e)[:1500], color=0xE74C3C, cooldown=600)
                save_state(state)
            except Exception:
                pass
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
