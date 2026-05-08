#!/usr/bin/env python3
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests


WORKDIR = Path("/opt/crypto-monitor")
IDEA_LEDGER_PATH = WORKDIR / "reports" / "trade_ideas_idea_ledger.csv"
OUTPUT_DIR = WORKDIR / "reports" / "trade_ideas_backtest"
BINANCE_FAPI = "https://fapi.binance.com"
HORIZONS = {
    "1h": 3600,
    "4h": 4 * 3600,
    "24h": 24 * 3600,
}

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "crypto-monitor-trade-ideas-backtest/1.0"})
DISCORD_API = "https://discord.com/api/v10"


def load_env_file(path="/etc/crypto-monitor.env"):
    env_path = Path(path)
    if not env_path.exists():
        return
    try:
        lines = env_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except PermissionError:
        return
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip().strip("'").strip('"'))


def discord_channel_id() -> str:
    return (
        os.getenv("TRADE_IDEAS_BACKTEST_DISCORD_CHANNEL_ID", "").strip()
        or os.getenv("DISCORD_DEBUG_CHANNEL_ID", "").strip()
        or os.getenv("DISCORD_SUMMARY_CHANNEL_ID", "").strip()
    )


def discord_send(title: str, description: str, color: int = 0x9B59B6) -> bool:
    if os.getenv("TRADE_IDEAS_BACKTEST_DISCORD_ENABLED", "1").lower() not in {"1", "true", "yes", "on"}:
        return False
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    channel_id = discord_channel_id()
    if not token or not channel_id:
        print("Discord token/channel missing for trade ideas backtest", file=sys.stderr)
        return False
    text = str(description or "")
    if len(text) > 3800:
        text = text[:3720].rstrip() + "\n...\n[内容过长，完整报告看本地文件]"
    response = SESSION.post(
        f"{DISCORD_API}/channels/{channel_id}/messages",
        headers={"Authorization": f"Bot {token}"},
        json={
            "embeds": [
                {
                    "title": title[:256],
                    "description": text,
                    "color": color,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "allowed_mentions": {"parse": []},
        },
        timeout=20,
    )
    if response.status_code not in (200, 201, 204):
        print(f"Discord trade ideas backtest send failed {response.status_code}: {response.text[:300]}", file=sys.stderr)
        return False
    return True


def fnum(value, digits=2):
    try:
        value = float(value)
    except Exception:
        return "n/a"
    return f"{value:.{digits}f}".rstrip("0").rstrip(".")


def parse_float(value):
    try:
        if value in ("", None):
            return None
        return float(value)
    except Exception:
        return None


def load_rows(path: Path):
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def fetch_klines(symbol: str, start_ts: float, end_ts: float):
    params = {
        "symbol": symbol,
        "interval": "5m",
        "startTime": int(start_ts * 1000),
        "endTime": int(end_ts * 1000),
        "limit": 500,
    }
    r = SESSION.get(f"{BINANCE_FAPI}/fapi/v1/klines", params=params, timeout=15)
    r.raise_for_status()
    return r.json()


def evaluate_row(row: dict, horizon_seconds: int, now_ts: float):
    start_ts = parse_float(row.get("scan_time_ts"))
    if start_ts is None or now_ts < start_ts + horizon_seconds:
        return None
    symbol = str(row.get("symbol") or "").upper()
    side = str(row.get("side") or "").upper()
    if not symbol or side not in {"LONG", "SHORT"}:
        return None

    last = parse_float(row.get("last"))
    entry_low = parse_float(row.get("entry_low"))
    entry_high = parse_float(row.get("entry_high"))
    stop = parse_float(row.get("stop"))
    target1 = parse_float(row.get("target1"))
    target2 = parse_float(row.get("target2"))
    if None in (last, entry_low, entry_high, stop, target1, target2):
        return None

    end_ts = start_ts + horizon_seconds
    klines = fetch_klines(symbol, start_ts, end_ts)
    if not klines:
        return None

    filled = False
    hit_stop = False
    hit_target1 = False
    hit_target2 = False
    first_hit = "none"
    close = float(klines[-1][4])
    max_fav = 0.0
    max_adv = 0.0

    for k in klines:
        high = float(k[2])
        low = float(k[3])
        if not filled and low <= entry_high and high >= entry_low:
            filled = True
        if not filled:
            continue
        if side == "LONG":
            fav = (high / last - 1) * 100
            adv = (low / last - 1) * 100
            stop_now = low <= stop
            target1_now = high >= target1
            target2_now = high >= target2
        else:
            fav = (last / low - 1) * 100 if low > 0 else 0.0
            adv = (last / high - 1) * 100 if high > 0 else 0.0
            stop_now = high >= stop
            target1_now = low <= target1
            target2_now = low <= target2
        max_fav = max(max_fav, fav)
        max_adv = min(max_adv, adv)
        if first_hit == "none":
            if stop_now and (target1_now or target2_now):
                first_hit = "same_bar"
            elif stop_now:
                first_hit = "stop"
            elif target2_now:
                first_hit = "target2"
            elif target1_now:
                first_hit = "target1"
        hit_stop = hit_stop or stop_now
        hit_target1 = hit_target1 or target1_now
        hit_target2 = hit_target2 or target2_now

    if side == "LONG":
        close_return = (close / last - 1) * 100
    else:
        close_return = (last / close - 1) * 100 if close > 0 else 0.0

    if not filled:
        outcome = "not_filled"
    elif first_hit == "stop":
        outcome = "stop_first"
    elif first_hit in {"target1", "target2"}:
        outcome = "target_first"
    elif first_hit == "same_bar":
        outcome = "same_bar"
    elif close_return > 0:
        outcome = "open_profit"
    else:
        outcome = "open_loss"

    return {
        "symbol": symbol,
        "side": side,
        "sent": int(float(row.get("sent") or 0)),
        "scan_time": row.get("scan_time"),
        "horizon": "",
        "filled": filled,
        "outcome": outcome,
        "first_hit": first_hit,
        "hit_stop": hit_stop,
        "hit_target1": hit_target1,
        "hit_target2": hit_target2,
        "close_return_pct": close_return,
        "mfe_pct": max_fav,
        "mae_pct": max_adv,
        "rr": parse_float(row.get("rr")) or 0.0,
        "reward_pct": parse_float(row.get("reward_pct")) or 0.0,
        "stance": row.get("stance") or "",
        "guard_line": row.get("guard_line") or "",
    }


def summarize(results):
    filled = [r for r in results if r["filled"]]
    sent = [r for r in results if r["sent"]]
    target_first = [r for r in results if r["outcome"] == "target_first"]
    stop_first = [r for r in results if r["outcome"] == "stop_first"]
    wins = [r for r in filled if r["close_return_pct"] > 0]
    avg_close = sum(r["close_return_pct"] for r in filled) / len(filled) if filled else 0.0
    avg_mfe = sum(r["mfe_pct"] for r in filled) / len(filled) if filled else 0.0
    avg_mae = sum(r["mae_pct"] for r in filled) / len(filled) if filled else 0.0
    return {
        "n": len(results),
        "sent": len(sent),
        "filled": len(filled),
        "win_rate": (len(wins) / len(filled) * 100) if filled else 0.0,
        "target_first": len(target_first),
        "stop_first": len(stop_first),
        "avg_close_return_pct": avg_close,
        "avg_mfe_pct": avg_mfe,
        "avg_mae_pct": avg_mae,
    }


def write_csv(path: Path, rows: list[dict]):
    path.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "scan_time",
        "symbol",
        "side",
        "sent",
        "horizon",
        "filled",
        "outcome",
        "first_hit",
        "hit_stop",
        "hit_target1",
        "hit_target2",
        "close_return_pct",
        "mfe_pct",
        "mae_pct",
        "rr",
        "reward_pct",
        "stance",
        "guard_line",
    ]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main():
    load_env_file()
    parser = argparse.ArgumentParser(description="Backtest high R/R trade idea ledger")
    parser.add_argument("--lookback-days", type=int, default=3)
    parser.add_argument("--print-summary", action="store_true")
    parser.add_argument("--no-discord", action="store_true")
    args = parser.parse_args()

    now_ts = time.time()
    cutoff = now_ts - args.lookback_days * 86400
    rows = [
        row for row in load_rows(IDEA_LEDGER_PATH)
        if (parse_float(row.get("scan_time_ts")) or 0) >= cutoff
    ]

    all_results = []
    grouped = {}
    for horizon, seconds in HORIZONS.items():
        horizon_results = []
        for row in rows:
            result = evaluate_row(row, seconds, now_ts)
            if result:
                result["horizon"] = horizon
                horizon_results.append(result)
                all_results.append(result)
                time.sleep(0.05)
        grouped[horizon] = summarize(horizon_results)

    day = datetime.now().strftime("%Y-%m-%d")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / f"trade_ideas_backtest_{day}.csv"
    json_path = OUTPUT_DIR / f"trade_ideas_backtest_{day}.json"
    txt_path = OUTPUT_DIR / f"trade_ideas_backtest_{day}.txt"
    latest_txt = OUTPUT_DIR / "latest_trade_ideas_backtest.txt"
    latest_json = OUTPUT_DIR / "latest_trade_ideas_backtest.json"
    latest_csv = OUTPUT_DIR / "latest_trade_ideas_backtest.csv"

    write_csv(csv_path, all_results)
    write_csv(latest_csv, all_results)
    payload = {
        "date": day,
        "ledger": str(IDEA_LEDGER_PATH),
        "lookback_days": args.lookback_days,
        "summary": grouped,
        "rows": len(all_results),
    }
    json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_json.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    lines = [f"高盈亏比多空计划每日回测 {day}", f"样本来源: {IDEA_LEDGER_PATH}", ""]
    for horizon in HORIZONS:
        s = grouped[horizon]
        lines.append(
            f"{horizon}: 样本{s['n']} 推送{s['sent']} 成交{s['filled']} "
            f"胜率{s['win_rate']:.1f}% 先到目标{s['target_first']} 先止损{s['stop_first']} "
            f"平均收盘收益{s['avg_close_return_pct']:+.2f}% MFE{s['avg_mfe_pct']:+.2f}% MAE{s['avg_mae_pct']:+.2f}%"
        )
    lines.append("")
    lines.append(f"明细: {csv_path}")
    text = "\n".join(lines) + "\n"
    txt_path.write_text(text, encoding="utf-8")
    latest_txt.write_text(text, encoding="utf-8")
    if not args.no_discord:
        discord_send(f"🎯 高盈亏比计划每日回测 {day}", text, color=0x9B59B6)
    if args.print_summary:
        print(text, end="")
    else:
        print(json.dumps(payload, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
