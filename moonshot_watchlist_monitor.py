#!/usr/bin/env python3
import json
import logging
import os
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

BASE_DIR = Path("/opt/crypto-monitor")
CONFIG_PATH = Path(os.getenv("MOONSHOT_WATCHLIST_PATH", str(BASE_DIR / "moonshot_watchlist_tokens.json")))
STATE_PATH = Path(os.getenv("MOONSHOT_WATCHLIST_STATE_PATH", str(BASE_DIR / "moonshot_watchlist_state.json")))
POLL_SECONDS = int(os.getenv("MOONSHOT_WATCH_POLL_SECONDS", "60"))
SUMMARY_SECONDS = int(os.getenv("MOONSHOT_WATCH_SUMMARY_SECONDS", "1800"))
PRICE_ALERT_PCT = float(os.getenv("MOONSHOT_WATCH_PRICE_ALERT_PCT", "3.0"))
LIQ_ALERT_PCT = float(os.getenv("MOONSHOT_WATCH_LIQ_ALERT_PCT", "12.0"))
LIQ_ALERT_USD = float(os.getenv("MOONSHOT_WATCH_LIQ_ALERT_USD", "10000"))
VOLUME_M5_ALERT_USD = float(os.getenv("MOONSHOT_WATCH_VOLUME_M5_ALERT_USD", "20000"))
TRANSFER_ALERT_USD = float(os.getenv("MOONSHOT_WATCH_TRANSFER_ALERT_USD", "10000"))
ALERT_COOLDOWN_SECONDS = int(os.getenv("MOONSHOT_WATCH_ALERT_COOLDOWN_SECONDS", "300"))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-moonshot-watchlist/1.0"})


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
        return f"${x / 1_000_000:.2f}M"
    if abs(x) >= 1_000:
        return f"${x / 1_000:.1f}K"
    return f"${x:.0f}"


def pct(x):
    try:
        return f"{float(x):+.2f}%"
    except Exception:
        return "n/a"


def short_addr(addr):
    addr = str(addr or "")
    return addr[:8] + "..." + addr[-6:] if len(addr) > 16 else addr or "-"


def fetch_json(url, params=None, timeout=20):
    r = SESSION.get(url, params=params, timeout=timeout)
    r.raise_for_status()
    return r.json()


def load_tokens():
    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    tokens = data.get("tokens") if isinstance(data, dict) else data
    out = []
    for row in tokens or []:
        symbol = str(row.get("symbol") or "").upper().strip()
        address = str(row.get("address") or "").lower().strip()
        if not symbol or not address:
            continue
        item = dict(row)
        item["symbol"] = symbol
        item["address"] = address
        item["chainid"] = str(item.get("chainid") or "56")
        out.append(item)
    return out


def load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except Exception:
            logging.exception("moonshot watchlist state load failed")
    return {"tokens": {}, "alerts": {}}


def save_state(state):
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(STATE_PATH)


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
    url = f"https://discord.com/api/v10/channels/{channel_id}/messages"
    payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
    r = SESSION.post(url, headers={"Authorization": f"Bot {token}"}, json=payload, timeout=20)
    if r.status_code not in (200, 201, 204):
        print("Discord send failed", r.status_code, r.text[:300])
        return False
    return True


def alert_once(state, key, title, description="", fields=None, color=0xF1C40F, cooldown=ALERT_COOLDOWN_SECONDS):
    now = time.time()
    last = float(state.setdefault("alerts", {}).get(key, 0) or 0)
    if now - last < cooldown:
        return False
    ok = discord_send(title, description, fields, color)
    if ok:
        state["alerts"][key] = now
    return ok


def fetch_dex_pairs(token):
    data = fetch_json(f"https://api.dexscreener.com/latest/dex/tokens/{token['address']}")
    pairs = []
    for p in data.get("pairs") or []:
        if str(p.get("chainId") or "").lower() != "bsc":
            continue
        vol = p.get("volume") or {}
        txns = p.get("txns") or {}
        pairs.append(
            {
                "chain": p.get("chainId") or "-",
                "dex": p.get("dexId") or "-",
                "pair": p.get("pairAddress") or "-",
                "url": p.get("url") or "",
                "price": float(p.get("priceUsd") or 0),
                "liq": float((p.get("liquidity") or {}).get("usd") or 0),
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
            }
        )
    pairs.sort(key=lambda x: (x["liq"], x["vol_h1"]), reverse=True)
    return pairs


def dex_summary(pairs):
    if not pairs:
        return "DEX暂无BSC池数据"
    total_liq = sum(p["liq"] for p in pairs)
    total_h1 = sum(p["vol_h1"] for p in pairs)
    top = pairs[0]
    return (
        f"主池 {top['dex']} {top['base']}/{top['quote']} 价 {fnum(top['price'])} | "
        f"总流动性 {money(total_liq)} | 1h量 {money(total_h1)} | "
        f"5m {pct(top['chg_m5'])} / 1h {pct(top['chg_h1'])}"
    )


def fetch_large_transfers(token, price_usd):
    key = os.getenv("ETHERSCAN_API_KEY") or os.getenv("BSCSCAN_API_KEY")
    if not key or not price_usd:
        return []
    try:
        data = fetch_json(
            "https://api.etherscan.io/v2/api",
            params={
                "chainid": token.get("chainid") or "56",
                "module": "account",
                "action": "tokentx",
                "contractaddress": token["address"],
                "page": "1",
                "offset": "25",
                "sort": "desc",
                "apikey": key,
            },
        )
    except Exception:
        logging.exception("%s transfer fetch failed", token["symbol"])
        return []
    rows = data.get("result") if isinstance(data, dict) else []
    out = []
    for row in rows if isinstance(rows, list) else []:
        try:
            amount = float(row.get("value") or 0) / (10 ** int(row.get("tokenDecimal") or 18))
        except Exception:
            continue
        usd = amount * price_usd
        if usd >= TRANSFER_ALERT_USD:
            out.append(
                {
                    "hash": row.get("hash"),
                    "from": row.get("from"),
                    "to": row.get("to"),
                    "amount": amount,
                    "usd": usd,
                }
            )
    return out


def check_token(token, state):
    symbol = token["symbol"]
    token_state = state.setdefault("tokens", {}).setdefault(symbol, {})
    pairs = fetch_dex_pairs(token)
    if not pairs:
        alert_once(state, f"{symbol}:dex_no_data", f"{symbol} DEX暂无BSC数据", f"`{token['address']}`", color=0xE67E22, cooldown=900)
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
        ("合约", f"`{token['address']}`", False),
    ]

    seen_pairs = token_state.setdefault("seen_pairs", {})
    for p in pairs[:5]:
        pair = str(p["pair"]).lower()
        if pair and pair not in seen_pairs:
            seen_pairs[pair] = now_iso()
            alert_once(
                state,
                f"{symbol}:new_pair:{pair}",
                f"{symbol} 新DEX池发现 {p['dex']}",
                f"{p['base']}/{p['quote']} | 流动性 {money(p['liq'])} | 价格 {fnum(p['price'])}\n{p['url']}",
                color=0x3498DB,
                cooldown=86400,
            )

    last_price = float(token_state.get("price", 0) or 0)
    if last_price:
        move = (price - last_price) / last_price * 100
        if abs(move) >= PRICE_ALERT_PCT:
            alert_once(
                state,
                f"{symbol}:price:{int(time.time() // 300)}",
                f"{symbol} 价格异动 {move:+.2f}%",
                f"当前 {fnum(price)}，上次 {fnum(last_price)}",
                fields,
                color=0x2ECC71 if move > 0 else 0xE74C3C,
            )
    token_state["price"] = price

    last_liq = float(token_state.get("liquidity", 0) or 0)
    if last_liq:
        diff = total_liq - last_liq
        diff_pct = diff / last_liq * 100 if last_liq else 0.0
        if abs(diff) >= LIQ_ALERT_USD or abs(diff_pct) >= LIQ_ALERT_PCT:
            alert_once(
                state,
                f"{symbol}:liq:{int(time.time() // 300)}",
                f"{symbol} DEX流动性{'增加' if diff > 0 else '减少'}",
                f"变化 {money(diff)} ({diff_pct:+.2f}%) | 当前 {money(total_liq)}",
                fields,
                color=0x2ECC71 if diff > 0 else 0xE74C3C,
            )
    token_state["liquidity"] = total_liq

    if total_m5 >= VOLUME_M5_ALERT_USD or buys_m5 + sells_m5 >= 80:
        side = "买盘偏强" if buys_m5 > sells_m5 * 1.3 else "卖盘偏强" if sells_m5 > buys_m5 * 1.3 else "多空活跃"
        alert_once(
            state,
            f"{symbol}:volume:{int(time.time() // 300)}",
            f"{symbol} DEX短线放量：{side}",
            f"5m成交 {money(total_m5)} | buys/sells {buys_m5}/{sells_m5} | 价格 {fnum(price)}",
            fields,
            color=0xF1C40F,
        )

    seen_txs = set(token_state.get("seen_txs", [])[-300:])
    for tr in fetch_large_transfers(token, price):
        h = tr.get("hash")
        if not h or h in seen_txs:
            continue
        seen_txs.add(h)
        alert_once(
            state,
            f"{symbol}:transfer:{h}",
            f"{symbol} 链上大额转账 {money(tr['usd'])}",
            f"{fnum(tr['amount'])} {symbol} | {short_addr(tr['from'])} -> {short_addr(tr['to'])}\ntx {h}",
            fields,
            color=0x9B59B6,
            cooldown=60,
        )
    token_state["seen_txs"] = list(seen_txs)[-300:]
    token_state["last_seen"] = now_iso()
    token_state["last_summary"] = token_state.get("last_summary", 0)


def send_summary(tokens, state):
    fields = []
    for token in tokens:
        symbol = token["symbol"]
        token_state = state.setdefault("tokens", {}).setdefault(symbol, {})
        price = token_state.get("price")
        liq = token_state.get("liquidity")
        pair_count = len(token_state.get("seen_pairs", {}) or {})
        fields.append((symbol, f"价 {fnum(price)} | 流动性 {money(liq)} | 池 {pair_count} | {token.get('level', '-')}", True))
    discord_send(
        "Moonshot 5币监控快照",
        f"配置: `{CONFIG_PATH}`\n状态: `{STATE_PATH}`",
        fields,
        color=0x5865F2,
    )


def main():
    load_env_file()
    if os.getenv("MOONSHOT_WATCH_ENABLED", "1").lower() in {"0", "false", "off", "no"}:
        print("moonshot watchlist disabled")
        return
    tokens = load_tokens()
    state = load_state()
    discord_send(
        "Moonshot 5币监控已启动",
        "\n".join(f"{x['symbol']} `{x['address']}`" for x in tokens),
        color=0x2ECC71,
    )
    while True:
        try:
            for token in tokens:
                check_token(token, state)
                time.sleep(1)
            now = time.time()
            if now - float(state.get("last_summary", 0) or 0) >= SUMMARY_SECONDS:
                send_summary(tokens, state)
                state["last_summary"] = now
            save_state(state)
        except Exception as e:
            print("loop error", repr(e))
            traceback.print_exc()
            try:
                alert_once(state, "loop_error", "Moonshot 5币监控异常", str(e)[:1500], color=0xE74C3C, cooldown=600)
                save_state(state)
            except Exception:
                pass
        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
