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

TOKEN_NAME = "JCT"
TOKEN_SYMBOL = "JCT"
TOKEN_ADDRESS = "0xea37a8de1de2d9d10772eeb569e28bfa5cb17707"
JCT_PRICE_MOVE_ALERT_PCT = float(os.getenv("JCT_PRICE_MOVE_ALERT_PCT", "3.0"))
JCT_LIQUIDITY_DROP_ALERT_PCT = float(os.getenv("JCT_LIQUIDITY_DROP_ALERT_PCT", "15.0"))
JCT_CEX_DEX_SPREAD_ALERT_PCT = float(os.getenv("JCT_CEX_DEX_SPREAD_ALERT_PCT", "2.0"))
JCT_LARGE_TRANSFER_USD = float(os.getenv("JCT_LARGE_TRANSFER_USD", "10000"))
JCT_WHALE_CHECK_INTERVAL_SECONDS = int(os.getenv("JCT_WHALE_CHECK_INTERVAL_SECONDS", "60"))
JCT_WHALE_TRANSFER_USD = float(os.getenv("JCT_WHALE_TRANSFER_USD", "3000"))
JCT_WHALE_FLOW_IMBALANCE_USD = float(os.getenv("JCT_WHALE_FLOW_IMBALANCE_USD", "5000"))
JCT_WHALE_WINDOW_SECONDS = int(os.getenv("JCT_WHALE_WINDOW_SECONDS", "900"))
JCT_WHALE_REPEAT_USD = float(os.getenv("JCT_WHALE_REPEAT_USD", "5000"))
JCT_WHALE_ALERT_COOLDOWN_SECONDS = int(os.getenv("JCT_WHALE_ALERT_COOLDOWN_SECONDS", "180"))
JCT_PRIORITY_EVENT_USD = float(os.getenv("JCT_PRIORITY_EVENT_USD", "3000"))
JCT_PRIORITY_FLOW_IMBALANCE_USD = float(os.getenv("JCT_PRIORITY_FLOW_IMBALANCE_USD", "6000"))
JCT_CEX_WALLETS = {a.strip().lower() for a in os.getenv("JCT_CEX_WALLETS", "").split(",") if a.strip()}
FORCE_SUMMARY_INTERVAL_SECONDS = int(os.getenv("JCT_FORCE_SUMMARY_INTERVAL_SECONDS", "300"))
JCTATE_PATH = Path("/opt/crypto-monitor/personal_watch_jct_state.json")

DEX_URL = f"https://api.dexscreener.com/latest/dex/tokens/{TOKEN_ADDRESS}"
POLL_SECONDS = int(os.environ.get("JCT_WATCH_POLL_SECONDS", "30"))
SUMMARY_SECONDS = int(os.environ.get("JCT_WATCH_SUMMARY_SECONDS", "1800"))
PRICE_ALERT_PCT = float(os.environ.get("JCT_WATCH_PRICE_ALERT_PCT", "3.0"))
LIQ_ALERT_PCT = float(os.environ.get("JCT_WATCH_LIQ_ALERT_PCT", "8.0"))
LIQ_ALERT_USD = float(os.environ.get("JCT_WATCH_LIQ_ALERT_USD", "10000"))
VOLUME_M5_ALERT_USD = float(os.environ.get("JCT_WATCH_VOLUME_M5_ALERT_USD", "20000"))
TRANSFER_ALERT_USD = float(os.environ.get("JCT_WATCH_TRANSFER_ALERT_USD", "20000"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-st-holding/1.0"})
ALLOWED_CEX_KEYS = ("cex:MEXC", "cex:BingX")


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
    if JCTATE_PATH.exists():
        try:
            return json.loads(JCTATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {"prices": {}, "liquidity": {}, "seen_pairs": {}, "alerts": {}, "last_summary": 0, "seen_txs": []}


def save_state(state):
    tmp = JCTATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(JCTATE_PATH)


def prune_cex_prices(state):
    prices = state.get("prices", {}) if isinstance(state, dict) else {}
    if not isinstance(prices, dict):
        return
    for key in list(prices.keys()):
        if str(key).startswith("cex:") and key not in ALLOWED_CEX_KEYS:
            prices.pop(key, None)


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



def fetch_cex_prices(symbol):
    out = []
    pair = f"{symbol.upper()}USDT"

    # MEXC spot ticker
    try:
        r = requests.get(
            "https://api.mexc.com/api/v3/ticker/24hr",
            params={"symbol": pair},
            timeout=15,
        )
        j = r.json()
        price = j.get("lastPrice") or j.get("price")
        if price:
            out.append({
                "exchange": "MEXC",
                "price": float(price),
                "vol24": float(j.get("quoteVolume") or 0),
                "change24": j.get("priceChangePercent"),
            })
        else:
            logging.info("%s MEXC ticker skipped: missing price in %s", symbol, str(j)[:300])
    except Exception as e:
        logging.info("%s MEXC ticker skipped: %s", symbol, e)

    # BingX spot ticker
    try:
        r = requests.get(
            "https://open-api.bingx.com/openApi/spot/v1/ticker/24hr",
            params={"symbol": f"{symbol.upper()}-USDT"},
            timeout=15,
        )
        j = r.json()
        data = j.get("data")
        if isinstance(data, list) and data:
            px = data[0].get("lastPrice") or data[0].get("last")
            if px:
                out.append({
                    "exchange": "BingX",
                    "price": float(px),
                    "vol24": float(data[0].get("quoteVolume") or data[0].get("quoteAmt") or 0),
                    "change24": data[0].get("priceChangePercent") or data[0].get("priceChange"),
                })
            else:
                logging.info("%s BingX ticker skipped: missing price in %s", symbol, str(data[0])[:300])
        elif isinstance(data, dict):
            px = data.get("lastPrice") or data.get("last")
            if px:
                out.append({
                    "exchange": "BingX",
                    "price": float(px),
                    "vol24": float(data.get("quoteVolume") or data.get("quoteAmt") or 0),
                    "change24": data.get("priceChangePercent") or data.get("priceChange"),
                })
            else:
                logging.info("%s BingX ticker skipped: missing price in %s", symbol, str(data)[:300])
        else:
            logging.info("%s BingX ticker skipped: unexpected payload %s", symbol, str(j)[:300])
    except Exception as e:
        logging.info("%s BingX ticker skipped: %s", symbol, e)

    return out


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
        alert_once(state, "dex_no_data", "⚠️ JCT 持仓监控：DEX暂无数据", f"{TOKEN_ADDRESS}", color=0xE67E22, cooldown=900)
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

    # 新池提醒
    seen_pairs = state.setdefault("seen_pairs", {})
    for p in pairs[:5]:
        if p["pair"] not in seen_pairs:
            seen_pairs[p["pair"]] = now_iso()
            alert_once(
                state,
                f"new_pair:{p['pair']}",
                f"🆕 JCT 新DEX池发现 {p['chain']}/{p['dex']}",
                f"{p['base']}/{p['quote']} | 流动性 {money(p['liq'])} | 价格 {fnum(p['price'])}\n{p['url']}",
                color=0x3498DB,
                cooldown=86400,
            )

    # 价格变化
    last_price = float(state.get("prices", {}).get("dex_best", 0) or 0)
    if last_price:
        move = (price - last_price) / last_price * 100
        if abs(move) >= PRICE_ALERT_PCT:
            color = 0x2ECC71 if move > 0 else 0xE74C3C
            title = f"{'🟢' if move > 0 else '🔴'} JCT 价格异动 {move:+.2f}%"
            alert_once(state, f"price_move:{int(time.time()//300)}", title, f"当前 {fnum(price)}，上次 {fnum(last_price)}", fields, color=color, cooldown=240)
    state.setdefault("prices", {})["dex_best"] = price

    # 流动性变化
    last_liq = float(state.get("liquidity", {}).get("total", 0) or 0)
    if last_liq:
        diff = total_liq - last_liq
        diff_pct = diff / last_liq * 100 if last_liq else 0
        if abs(diff) >= LIQ_ALERT_USD or abs(diff_pct) >= LIQ_ALERT_PCT:
            color = 0x2ECC71 if diff > 0 else 0xE74C3C
            title = f"{'🟢' if diff > 0 else '🔴'} JCT DEX流动性{'增加' if diff > 0 else '减少'}"
            desc = f"变化 {money(diff)} ({diff_pct:+.2f}%) | 当前总流动性 {money(total_liq)}"
            alert_once(state, f"liq:{int(time.time()//300)}", title, desc, fields, color=color, cooldown=300)
    state.setdefault("liquidity", {})["total"] = total_liq

    # 放量提醒
    if total_m5 >= VOLUME_M5_ALERT_USD or buys_m5 + sells_m5 >= 80:
        side = "买盘偏强" if buys_m5 > sells_m5 * 1.3 else "卖盘偏强" if sells_m5 > buys_m5 * 1.3 else "多空活跃"
        alert_once(
            state,
            f"dex_volume:{int(time.time()//300)}",
            f"🟡 JCT DEX短线放量：{side}",
            f"5m成交 {money(total_m5)} | buys/sells {buys_m5}/{sells_m5} | 价格 {fnum(price)}",
            fields,
            color=0xF1C40F,
            cooldown=300,
        )

    # CEX spot
    cex = fetch_cex_prices(TOKEN_SYMBOL)
    if cex:
        cex_lines = []
        for x in cex:
            cex_lines.append(f"{x['exchange']}: {fnum(x['price'])} | 24h量 {money(x.get('vol24') or 0)}")
            key = "cex:" + x["exchange"]
            last = float(state.get("prices", {}).get(key, 0) or 0)
            if last:
                move = (x["price"] - last) / last * 100
                if abs(move) >= PRICE_ALERT_PCT:
                    alert_once(
                        state,
                        f"cex_move:{x['exchange']}:{int(time.time()//300)}",
                        f"{'🟢' if move > 0 else '🔴'} JCT {x['exchange']}现货异动 {move:+.2f}%",
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
                alert_once(
                    state,
                    f"arb_spread:{int(time.time()//300)}",
                    f"🟠 JCT 现货/DEX价差 {spread:.2f}%",
                    "\n".join(cex_lines) + f"\nDEX: {fnum(price)}",
                    fields,
                    color=0xE67E22,
                    cooldown=300,
                )

    # Large transfers
    seen = set(state.get("seen_txs", [])[-200:])
    transfers = fetch_large_transfers(price)
    for tr in transfers:
        h = tr.get("hash")
        if not h or h in seen:
            continue
        seen.add(h)
        alert_once(
            state,
            f"transfer:{h}",
            f"🐳 JCT 链上大额转账 {money(tr['usd'])}",
            f"{fnum(tr['amount'])} JCT | {short_addr(tr['from'])} -> {short_addr(tr['to'])}\n{tr['source']} tx {h}",
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
        discord_send(
            "📌 JCT 个人持仓监控",
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
        logging.exception("JCT whale pair fetch failed")
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
            logging.info("JCT whale transfer fetch skipped: %s", str(data)[:300])
            return []
        return data["result"]
    except Exception:
        logging.exception("JCT whale transfer fetch failed")
        return []


def _st_whale_price(state):
    prices = state.get("prices", {}) if isinstance(state, dict) else {}
    for key in ("dex_best", "cex:MEXC", "cex:BingX"):
        value = prices.get(key)
        if value:
            try:
                return float(value)
            except Exception:
                pass
    return 0.0


def _st_whale_side(row, pair_addrs):
    src = (row.get("from") or "").lower()
    dst = (row.get("to") or "").lower()
    if dst in pair_addrs:
        return "sell", src, "转入DEX池，疑似出货/卖压"
    if src in pair_addrs:
        return "buy", dst, "从DEX池转出，疑似扫货/吸筹"

    return "transfer", dst, "普通大额链上转账，暂不判断方向"


def _jct_priority_bucket(event):
    side = event.get("side")
    usd = float(event.get("usd", 0) or 0)
    if usd < JCT_PRIORITY_EVENT_USD:
        return None
    if side == "sell":
        return "sell_pressure"
    if side == "buy":
        return "buy_pressure"
    return None


def _jct_priority_flow_score(event):
    side = event.get("side")
    usd = float(event.get("usd", 0) or 0)
    if side == "buy":
        return usd
    if side == "sell":
        return -usd
    return 0.0



def _st_whale_cooldown_ok(state, key):
    now = time.time()
    cooldowns = state.setdefault("st_whale_alert_cooldowns", {})
    last = float(cooldowns.get(key, 0) or 0)
    if now - last < JCT_WHALE_ALERT_COOLDOWN_SECONDS:
        return False
    cooldowns[key] = now
    return True


def check_whale_activity(state):
    now = time.time()
    if now - float(state.get("last_whale_check_ts", 0) or 0) < JCT_WHALE_CHECK_INTERVAL_SECONDS:
        return
    state["last_whale_check_ts"] = now

    price = _st_whale_price(state)
    if not price:
        logging.info("JCT whale check skipped: no price")
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
            "👁️ JCT 大户行为监控已启动",
            f"已识别 DEX 池子 {len(pair_addrs)} 个；后续只提醒新发生的大额扫货/出货。\n阈值：单笔 {money(JCT_WHALE_TRANSFER_USD)}，窗口 {int(JCT_WHALE_WINDOW_SECONDS/60)} 分钟。",
            [("DEX池", "\n".join(state.get("st_dex_pair_labels", [])[:6]) or "-", False)],
            color=0x5865F2,
        )
        return

    seen = set(state.get("seen_whale_transfer_hashes") or [])
    new_events = []
    for row in reversed(rows):
        txh = row.get("hash")
        if not txh or txh in seen:
            continue
        amount = _st_whale_amount(row)
        usd = amount * price
        side, actor, label = _st_whale_side(row, pair_addrs)
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
        }
        new_events.append(event)
        seen.add(txh)

    logging.info(
        "JCT whale radar heartbeat: price=%s pairs=%s rows=%s new_events=%s initialized=%s",
        price,
        len(pair_addrs),
        len(rows),
        len(new_events),
        state.get("st_whale_monitor_initialized"),
    )

    if not new_events:
        state["seen_whale_transfer_hashes"] = latest_hashes[-200:]
        return

    fields = []
    for e in new_events:
        if e["usd"] >= JCT_WHALE_TRANSFER_USD:
            fields.append((
                f"{money(e['usd'])} / {fnum(e['amount'])} JCT",
                f"{e['label']}\n{_st_whale_short(e['from'])} -> {_st_whale_short(e['to'])}\nTx: `{e['hash']}`",
                False,
            ))

    if fields and _st_whale_cooldown_ok(state, "large_tx"):
        discord_send(
            "🟠 JCT 大户链上动作",
            f"发现 {len(fields)} 笔超过 {money(JCT_WHALE_TRANSFER_USD)} 的新链上动作。",
            fields[:6],
            color=0xFEE75C,
        )

    priority_fields = []
    priority_buckets = set()
    for e in new_events:
        bucket = _jct_priority_bucket(e)
        if not bucket:
            continue
        priority_buckets.add(bucket)
        priority_fields.append((
            f"{money(e['usd'])} / {fnum(e['amount'])} JCT",
            f"{e['label']}\n{_st_whale_short(e['from'])} -> {_st_whale_short(e['to'])}\nTx: `{e['hash']}`",
            False,
        ))

    if priority_fields:
        if "sell_pressure" in priority_buckets and _st_whale_cooldown_ok(state, "priority_sell"):
            discord_send(
                "🔴 JCT DEX卖压动作",
                "检测到大额 JCT 持续流入 DEX 池，优先关注卖压。",
                priority_fields[:6],
                color=0xED4245,
            )
        elif "buy_pressure" in priority_buckets and _st_whale_cooldown_ok(state, "priority_buy"):
            discord_send(
                "🟢 JCT DEX扫货动作",
                "检测到大额 JCT 从 DEX 池流出，优先关注扫货。",
                priority_fields[:6],
                color=0x57F287,
            )

    window = state.get("st_whale_flow_window") or []
    cutoff = int(now - JCT_WHALE_WINDOW_SECONDS)
    window = [x for x in window if int(x.get("ts", 0)) >= cutoff]
    for e in new_events:
        if e["side"] in {"buy", "sell"}:
            window.append({k: e[k] for k in ("ts", "hash", "side", "actor", "amount", "usd")})
    state["st_whale_flow_window"] = window[-200:]

    buy_usd = sum(float(x.get("usd", 0) or 0) for x in window if x.get("side") == "buy")
    sell_usd = sum(float(x.get("usd", 0) or 0) for x in window if x.get("side") == "sell")
    net = buy_usd - sell_usd

    if net >= JCT_WHALE_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "flow_buy"):
        discord_send(
            "🟢 JCT 大户扫货占优",
            f"最近 {int(JCT_WHALE_WINDOW_SECONDS/60)} 分钟：扫货 {money(buy_usd)} / 出货 {money(sell_usd)}，净扫货 {money(net)}。",
            [("含义", "DEX 池子里的 JCT 被大额拿走，偏吸筹/扫货观察。", False)],
            color=0x57F287,
        )
    elif -net >= JCT_WHALE_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "flow_sell"):
        discord_send(
            "🔴 JCT 大户出货占优",
            f"最近 {int(JCT_WHALE_WINDOW_SECONDS/60)} 分钟：出货 {money(sell_usd)} / 扫货 {money(buy_usd)}，净出货 {money(-net)}。",
            [("含义", "JCT 被持续打入 DEX 池子，卖压上升。", False)],
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
    for (side, actor), stat in wallet_stats.items():
        if stat["count"] >= 2 and stat["usd"] >= JCT_WHALE_REPEAT_USD:
            label = "连续扫货" if side == "buy" else "连续出货"
            repeat_fields.append((label, f"{_st_whale_short(actor)} | {stat['count']} 笔 | {money(stat['usd'])}", False))

    if repeat_fields and _st_whale_cooldown_ok(state, "repeat_wallet"):
        discord_send(
            "👀 JCT 同地址连续动作",
            "同一地址在短窗口内连续买/卖，值得盯。",
            repeat_fields[:6],
            color=0xFEE75C,
        )

    priority_window = state.get("st_priority_flow_window") or []
    priority_window = [x for x in priority_window if int(x.get("ts", 0)) >= cutoff]
    for e in new_events:
        score = _jct_priority_flow_score(e)
        if score:
            priority_window.append({"ts": e["ts"], "score": score})
    state["st_priority_flow_window"] = priority_window[-200:]
    priority_net = sum(float(x.get("score", 0) or 0) for x in priority_window)
    if priority_net >= JCT_PRIORITY_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "priority_flow_buy"):
        discord_send(
            "🟢 JCT 短窗净扫货占优",
            f"最近 {int(JCT_WHALE_WINDOW_SECONDS/60)} 分钟，大额地址净扫货 {money(priority_net)}。",
            [("判断", "更多 JCT 被从 DEX 池拿走，偏吸筹。", False)],
            color=0x57F287,
        )
    elif -priority_net >= JCT_PRIORITY_FLOW_IMBALANCE_USD and _st_whale_cooldown_ok(state, "priority_flow_sell"):
        discord_send(
            "🔴 JCT 短窗净卖压占优",
            f"最近 {int(JCT_WHALE_WINDOW_SECONDS/60)} 分钟，大额地址净卖压 {money(-priority_net)}。",
            [("判断", "更多 JCT 被持续打入 DEX 池，偏卖压。", False)],
            color=0xED4245,
        )

    state["seen_whale_transfer_hashes"] = list(seen)[-300:]


def check_holding_alerts(state):
    prices = state.get("prices", {})
    liquidity = state.get("liquidity", {})
    dex_price = prices.get("dex_best")
    total_liq = liquidity.get("total")

    previous_price = state.get("alert_ref_dex_price")
    previous_liq = state.get("alert_ref_total_liquidity")

    if dex_price and previous_price:
        chg = pct_change(dex_price, previous_price)
        if chg is not None and abs(chg) >= JCT_PRICE_MOVE_ALERT_PCT:
            direction = "上涨" if chg > 0 else "下跌"
            discord_send(
                f"⚠️ JCT 价格{direction}提醒",
                f"DEX价格 {fnum(previous_price)} -> {fnum(dex_price)}，变化 {chg:+.2f}%\n合约: `{TOKEN_ADDRESS}`",
                [("操作", "先看 DEX 流动性和 CEX 是否同步，不要只看单池价格。", False)],
                color=0xFEE75C if chg > 0 else 0xED4245,
            )
            state["alert_ref_dex_price"] = dex_price

    if total_liq and previous_liq:
        chg = pct_change(total_liq, previous_liq)
        if chg is not None and chg <= -JCT_LIQUIDITY_DROP_ALERT_PCT:
            discord_send(
                "🔴 JCT 流动性下降提醒",
                f"DEX总流动性 {money(previous_liq)} -> {money(total_liq)}，变化 {chg:+.2f}%\n合约: `{TOKEN_ADDRESS}`",
                [("风险", "流动性快速下降，滑点和砸盘风险上升。", False)],
                color=0xED4245,
            )
            state["alert_ref_total_liquidity"] = total_liq

    cex_prices = {k: v for k, v in prices.items() if str(k).startswith("cex:") and v}
    if dex_price and cex_prices:
        lines = []
        for name, price in sorted(cex_prices.items()):
            spread = pct_change(price, dex_price)
            if spread is not None and abs(spread) >= JCT_CEX_DEX_SPREAD_ALERT_PCT:
                lines.append(f"{name.split(':', 1)[1]} {fnum(price)} vs DEX {fnum(dex_price)}，价差 {spread:+.2f}%")
        if lines:
            key = "|".join(lines)
            if state.get("last_spread_alert_key") != key:
                discord_send(
                    "🟠 JCT CEX/DEX 价差提醒",
                    "\n".join(lines),
                    [("含义", "价差扩大时，可能出现搬砖、补跌或拉盘后的回归。", False)],
                    color=0xFEE75C,
                )
                state["last_spread_alert_key"] = key

    if dex_price and not previous_price:
        state["alert_ref_dex_price"] = dex_price
    if total_liq and not previous_liq:
        state["alert_ref_total_liquidity"] = total_liq


def send_summary(state, reason="定时持仓快照"):
    prices = state.get("prices", {}) if isinstance(state, dict) else {}
    liquidity = state.get("liquidity", {}) if isinstance(state, dict) else {}

    dex_price = prices.get("dex_best")
    total_liq = liquidity.get("total")
    seen_pairs = state.get("seen_pairs", {}) if isinstance(state, dict) else {}

    amount = float(os.getenv("JCT_WATCH_AMOUNT", "0") or 0)
    entry_price = float(os.getenv("JCT_ENTRY_PRICE", "0") or 0)
    mark_price = float(dex_price or prices.get("cex:MEXC") or prices.get("cex:BingX") or 0)

    fields = []

    if amount and entry_price and mark_price:
        cost = amount * entry_price
        value = amount * mark_price
        pnl = value - cost
        pnl_pct = (mark_price - entry_price) / entry_price * 100 if entry_price else 0
        fields.append((
            "个人持仓",
            (
                f"数量 {amount:,.2f} JCT\n"
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

    return discord_send(
        f"📌 JCT {reason}",
        f"{TOKEN_NAME} ({TOKEN_SYMBOL})\n合约: `{TOKEN_ADDRESS}`",
        fields,
        color=0x5865F2,
    )


def main():
    load_env_file()
    if os.environ.get("JCT_WATCH_ENABLED", "1").lower() in {"0", "false", "off", "no"}:
        print("JCT holding monitor disabled")
        return

    state = load_state()
    prune_cex_prices(state)
    discord_send("✅ JCT 个人持仓监控已启动", f"{TOKEN_NAME} ({TOKEN_SYMBOL})\n`{TOKEN_ADDRESS}`\n频道: <#{os.environ.get('DISCORD_HOLDING_CHANNEL_ID','')}>", color=0x2ECC71)

    while True:
        try:
            check_events(state)
            check_whale_activity(state)
            now_ts = time.time()
            if now_ts - float(state.get("last_forced_summary_ts", 0) or 0) >= FORCE_SUMMARY_INTERVAL_SECONDS:
                prices = state.get("prices", {})
                liquidity = state.get("liquidity", {})
                logging.info(
                    "JCT holding heartbeat: dex_best=%s cex=%s liquidity=%s pairs=%s",
                    prices.get("dex_best"),
                    {k: v for k, v in prices.items() if str(k).startswith("cex:")},
                    liquidity.get("total"),
                    len(state.get("seen_pairs", {})),
                )
                send_summary(state, reason="定时持仓快照")
                state["last_forced_summary_ts"] = now_ts
            save_state(state)
        except Exception as e:
            print("loop error", repr(e))
            traceback.print_exc()
            try:
                alert_once(state, "loop_error", "⚠️ JCT 持仓监控异常", str(e)[:1500], color=0xE74C3C, cooldown=600)
                save_state(state)
            except Exception:
                pass
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
