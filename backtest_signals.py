import argparse
import csv
import datetime as dt
import statistics
import time
from pathlib import Path

import requests
import yaml

BASE = "https://fapi.binance.com"
BULL = {"discovery", "hot_breakout", "bottom_reversal"}
BEAR = {"top_risk", "distribution", "top_exhaustion"}
HORIZONS = {"15m": 900, "1h": 3600, "4h": 14400, "24h": 86400}


def parse_time(value):
    if not value:
        return None
    value = value.replace("Z", "+00:00")
    t = dt.datetime.fromisoformat(value)
    if t.tzinfo is None:
        t = t.replace(tzinfo=dt.UTC)
    return t.astimezone(dt.UTC)


def pct(a, b):
    return 0.0 if not a else (b - a) / a * 100


def direction(kind):
    if kind in BULL:
        return "long"
    if kind in BEAR:
        return "short"
    return "neutral"


def get_klines(session, symbol, start, end):
    r = session.get(
        BASE + "/fapi/v1/klines",
        params={
            "symbol": symbol,
            "interval": "5m",
            "startTime": int(start * 1000),
            "endTime": int(end * 1000),
            "limit": 500,
        },
        timeout=10,
    )
    r.raise_for_status()
    return r.json()


def close_before(rows, ts):
    target = int(ts * 1000)
    last = None
    for row in rows:
        if int(row[0]) <= target:
            last = row
        else:
            break
    return float(last[4]) if last else None


def eval_signal(session, row):
    symbol = (row.get("symbol") or "").upper()
    kind = row.get("kind") or ""
    t = parse_time(row.get("time") or "")
    try:
        entry = float(row.get("price") or 0)
    except Exception:
        return None
    if not symbol or not kind or not t or entry <= 0:
        return None

    start = t.timestamp()
    now_ts = time.time()
    rows = get_klines(session, symbol, start, min(now_ts, start + HORIZONS["24h"] + 600))
    if not rows:
        return None

    side = direction(kind)
    out = {"symbol": symbol, "kind": kind, "side": side}
    for name, sec in HORIZONS.items():
        if now_ts < start + sec:
            out[name] = None
            continue
        close = close_before(rows, start + sec)
        if close is None:
            out[name] = None
            continue
        change = pct(entry, close)
        out[name] = -change if side == "short" else change

    high = max(float(x[2]) for x in rows)
    low = min(float(x[3]) for x in rows)
    out["mfe"] = pct(entry, high)
    out["mae"] = pct(entry, low)
    return out


def fmt(v):
    return "-" if v is None else f"{v:+.2f}%"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-c", "--config", default="derivatives_config.yaml")
    ap.add_argument("--limit", type=int, default=80)
    args = ap.parse_args()

    cfg = yaml.safe_load(Path(args.config).read_text()) or {}
    path = Path(str(cfg.get("signal_log_path", "signals.csv")))
    if not path.is_absolute():
        path = Path.cwd() / path
    rows = list(csv.DictReader(path.open("r", encoding="utf-8")))[-args.limit:][::-1]

    session = requests.Session()
    results = []
    for row in rows:
        try:
            item = eval_signal(session, row)
            if item:
                results.append(item)
            time.sleep(0.08)
        except Exception as e:
            print(f"skip {row.get('symbol','-')}: {type(e).__name__}: {e}")

    if not results:
        raise SystemExit("no backtestable signals")

    print("[BACKTEST] 最近信号回测")
    print("说明: discovery/hot 按看多计算，top_risk/distribution 按看空计算")
    print("")

    for kind in sorted(set(x["kind"] for x in results)):
        group = [x for x in results if x["kind"] == kind]
        print(f"{kind} 样本={len(group)}")
        for h in ["15m", "1h", "4h", "24h"]:
            vals = [x[h] for x in group if x[h] is not None]
            if not vals:
                continue
            wins = sum(v > 0 for v in vals)
            print(
                f"  {h}: 胜率={wins}/{len(vals)} {wins/len(vals)*100:.1f}% "
                f"平均={fmt(statistics.mean(vals))} 中位={fmt(statistics.median(vals))} "
                f"最好={fmt(max(vals))} 最差={fmt(min(vals))}"
            )
        print("")

    print("最近20条:")
    for x in results[:20]:
        print(
            f"{x['symbol']} {x['kind']} {x['side']} "
            f"15m={fmt(x['15m'])} 1h={fmt(x['1h'])} 4h={fmt(x['4h'])} 24h={fmt(x['24h'])} "
            f"MFE={fmt(x['mfe'])} MAE={fmt(x['mae'])}"
        )


if __name__ == "__main__":
    main()
