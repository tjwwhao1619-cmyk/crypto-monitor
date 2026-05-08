#!/opt/crypto-monitor/.venv/bin/python
import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from pathlib import Path

import requests


WORKDIR = Path("/opt/crypto-monitor")
BACKTEST = WORKDIR / "backtest_signals.py"
CONFIG = WORKDIR / "derivatives_config.yaml"
PYTHON_BIN = WORKDIR / ".venv/bin/python"
OUTPUT_DIR = WORKDIR / "reports" / "daily_signal_backtest"
LOCAL_TZ = dt.datetime.now().astimezone().tzinfo
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
        os.getenv("DAILY_BACKTEST_DISCORD_CHANNEL_ID", "").strip()
        or os.getenv("DISCORD_DEBUG_CHANNEL_ID", "").strip()
        or os.getenv("DISCORD_SUMMARY_CHANNEL_ID", "").strip()
    )


def truncate(value: str, limit: int = 3800) -> str:
    text = str(value or "")
    if len(text) <= limit:
        return text
    return text[: limit - 80].rstrip() + "\n...\n[内容过长，完整报告看本地文件]"


def discord_send(title: str, description: str, color: int = 0x3498DB) -> bool:
    if os.getenv("DAILY_BACKTEST_DISCORD_ENABLED", "1").lower() not in {"1", "true", "yes", "on"}:
        return False
    token = os.getenv("DISCORD_BOT_TOKEN", "").strip()
    channel_id = discord_channel_id()
    if not token or not channel_id:
        print("Discord token/channel missing for daily backtest report", file=sys.stderr)
        return False
    payload = {
        "embeds": [
            {
                "title": title[:256],
                "description": truncate(description),
                "color": color,
                "timestamp": dt.datetime.now(dt.UTC).isoformat(),
            }
        ],
        "allowed_mentions": {"parse": []},
    }
    try:
        response = requests.post(
            f"{DISCORD_API}/channels/{channel_id}/messages",
            headers={"Authorization": f"Bot {token}"},
            json=payload,
            timeout=20,
        )
        if response.status_code not in (200, 201, 204):
            print(f"Discord daily report send failed {response.status_code}: {response.text[:300]}", file=sys.stderr)
            return False
        return True
    except Exception as exc:
        print(f"Discord daily report send failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return False


def today_local():
    return dt.datetime.now(LOCAL_TZ).date().isoformat()


def run_backtest(report_path: Path, limit: int) -> None:
    cmd = [
        str(PYTHON_BIN),
        str(BACKTEST),
        "-c",
        str(CONFIG),
        "--limit",
        str(limit),
        "--export-report",
        str(report_path),
    ]
    completed = subprocess.run(cmd, cwd=WORKDIR, check=True, capture_output=True, text=True)
    if completed.stderr.strip():
        sys.stderr.write(completed.stderr)


def parse_report(text: str) -> dict:
    summary = {
        "sample_total": None,
        "sample_evaluable": None,
        "sample_missing": None,
        "kind_counts": {},
        "kind_stats_1h": {},
        "kind_stats_15m": {},
        "best_kind_1h": None,
        "noisiest_kind_1h": None,
        "summary_horizons": {},
        "bad_signal_count": None,
        "bad_signal_ratio": None,
        "combo_filter_1h_after": None,
        "report_supports_drawdown": False,
        "actual_routes_1h": {},
        "actual_routes_4h": {},
        "sim_routes_1h": {},
        "sim_routes_4h": {},
    }

    if m := re.search(r"总样本:\s*(\d+)", text):
        summary["sample_total"] = int(m.group(1))
    if m := re.search(r"可评估样本:\s*(\d+)", text):
        summary["sample_evaluable"] = int(m.group(1))
    if m := re.search(r"data_missing 样本:\s*(\d+)", text):
        summary["sample_missing"] = int(m.group(1))

    kind_pattern = re.compile(r"^([a-z_]+) 样本=(\d+)$", re.M)
    kind_matches = list(kind_pattern.finditer(text))
    section_end = text.find("[SIGNAL QUALITY]")
    if section_end == -1:
        section_end = len(text)
    for idx, match in enumerate(kind_matches):
        kind = match.group(1)
        if match.start() > section_end:
            continue
        summary["kind_counts"][kind] = int(match.group(2))
        next_start = kind_matches[idx + 1].start() if idx + 1 < len(kind_matches) else section_end
        block = text[match.end(): min(next_start, section_end)]
        for horizon in ("15m", "1h"):
            hm = re.search(
                rf"{re.escape(horizon)}:\s*胜率=(\d+)/(\d+)\s+([0-9.]+)%\s+平均=([+-]?[0-9.]+)%",
                block,
            )
            if hm:
                payload = {
                    "wins": int(hm.group(1)),
                    "n": int(hm.group(2)),
                    "win_rate": float(hm.group(3)),
                    "avg": float(hm.group(4)),
                }
                if horizon == "1h":
                    summary["kind_stats_1h"][kind] = payload
                else:
                    summary["kind_stats_15m"][kind] = payload

    eligible_1h = {
        kind: stat
        for kind, stat in summary["kind_stats_1h"].items()
        if stat["n"] >= 3
    }
    if eligible_1h:
        best_kind = max(eligible_1h.items(), key=lambda item: (item[1]["avg"], item[1]["win_rate"], item[1]["n"]))
        worst_kind = min(eligible_1h.items(), key=lambda item: (item[1]["avg"], item[1]["win_rate"], -item[1]["n"]))
        summary["best_kind_1h"] = {"kind": best_kind[0], **best_kind[1]}
        summary["noisiest_kind_1h"] = {"kind": worst_kind[0], **worst_kind[1]}

    summary_block = text[text.find("[SUMMARY] 综合总览"):] if "[SUMMARY] 综合总览" in text else ""
    for horizon in ("15m", "1h", "4h", "12h", "24h"):
        hm = re.search(
            rf"{re.escape(horizon)}:\s*胜率=(\d+)/(\d+)\s+([0-9.]+)%\s+平均=([+-]?[0-9.]+)%",
            summary_block,
        )
        if hm:
            summary["summary_horizons"][horizon] = {
                "wins": int(hm.group(1)),
                "n": int(hm.group(2)),
                "win_rate": float(hm.group(3)),
                "avg": float(hm.group(4)),
            }

    if m := re.search(r"坏信号样本:\s*(\d+)/(\d+)", text):
        bad = int(m.group(1))
        total = int(m.group(2))
        summary["bad_signal_count"] = bad
        summary["bad_signal_ratio"] = (bad / total * 100) if total else None

    for marker, key_1h, key_4h in (
        ("[ACTUAL ROUTES] 实际已记录路由表现", "actual_routes_1h", "actual_routes_4h"),
        ("[ROUTE SIMULATION] Discord 路由模拟", "sim_routes_1h", "sim_routes_4h"),
    ):
        idx = text.find(marker)
        if idx == -1:
            continue
        block = text[idx: idx + 3000]
        route_pattern = re.compile(r"^(realtime|risk_realtime|priority_observe|observe|conflict_observe|digest|suppress|none): 样本=(\d+)$", re.M)
        matches = list(route_pattern.finditer(block))
        for i, match in enumerate(matches):
            route = match.group(1)
            next_start = matches[i + 1].start() if i + 1 < len(matches) else len(block)
            route_block = block[match.end():next_start]
            for horizon, target in (("1h", summary[key_1h]), ("4h", summary[key_4h])):
                hm = re.search(
                    rf"{re.escape(horizon)}:\s*样本=(\d+)\s+胜率=(\d+)/(\d+)\s+([0-9.]+)%\s+平均=([+-]?[0-9.]+)%",
                    route_block,
                )
                if hm:
                    target[route] = {
                        "n": int(hm.group(1)),
                        "wins": int(hm.group(2)),
                        "win_rate": float(hm.group(4)),
                        "avg": float(hm.group(5)),
                    }

    combo_marker = "过滤后:"
    idx = text.find(combo_marker)
    if idx != -1:
        combo_block = text[idx: idx + 400]
        if m := re.search(r"1h:\s*样本=(\d+)\s*胜率=(\d+)/(\d+)\s*([0-9.]+)%\s*平均=([+-]?[0-9.]+)%", combo_block):
            summary["combo_filter_1h_after"] = {
                "n": int(m.group(1)),
                "wins": int(m.group(2)),
                "win_rate": float(m.group(4)),
                "avg": float(m.group(5)),
            }

    return summary


def build_human_summary(parsed: dict, report_path: Path) -> str:
    lines = []
    lines.append(f"每日合约监控信号回测日报 {today_local()}")
    lines.append(f"回测脚本: {BACKTEST}")
    lines.append(f"报告文件: {report_path}")
    lines.append("")
    lines.append("样本概览")
    lines.append(
        f"- 总样本: {parsed.get('sample_total', '-')}, 可评估: {parsed.get('sample_evaluable', '-')}, 缺失: {parsed.get('sample_missing', '-')}"
    )
    if parsed["summary_horizons"]:
        for horizon, stat in parsed["summary_horizons"].items():
            lines.append(
                f"- {horizon}: 胜率 {stat['wins']}/{stat['n']} {stat['win_rate']:.1f}%, 平均收益 {stat['avg']:+.2f}%"
            )
    lines.append("")
    lines.append("信号分布")
    for kind, count in sorted(parsed["kind_counts"].items(), key=lambda item: (-item[1], item[0])):
        detail = parsed["kind_stats_1h"].get(kind) or parsed["kind_stats_15m"].get(kind)
        if detail:
            horizon = "1h" if kind in parsed["kind_stats_1h"] else "15m"
            lines.append(
                f"- {kind}: 样本 {count}, {horizon} 胜率 {detail['wins']}/{detail['n']} {detail['win_rate']:.1f}%, 平均 {detail['avg']:+.2f}%"
            )
        else:
            lines.append(f"- {kind}: 样本 {count}")
    lines.append("")

    best = parsed.get("best_kind_1h")
    if best:
        lines.append(
            f"表现最好: {best['kind']} (按1h平均收益), 胜率 {best['wins']}/{best['n']} {best['win_rate']:.1f}%, 平均收益 {best['avg']:+.2f}%"
        )
    worst = parsed.get("noisiest_kind_1h")
    if worst:
        lines.append(
            f"噪音最大: {worst['kind']} (按1h平均收益最差), 胜率 {worst['wins']}/{worst['n']} {worst['win_rate']:.1f}%, 平均收益 {worst['avg']:+.2f}%"
        )
    if parsed.get("bad_signal_count") is not None:
        lines.append(
            f"坏信号占比: {parsed['bad_signal_count']}/{parsed.get('sample_evaluable') or '-'}"
            + (f" ({parsed['bad_signal_ratio']:.1f}%)" if parsed.get("bad_signal_ratio") is not None else "")
        )
    if parsed.get("combo_filter_1h_after"):
        combo = parsed["combo_filter_1h_after"]
        lines.append(
            f"过滤后参考(1h): 样本 {combo['n']}, 胜率 {combo['wins']}/{combo['n']} {combo['win_rate']:.1f}%, 平均收益 {combo['avg']:+.2f}%"
        )
    lines.append("")
    lines.append("绩效口径说明")
    lines.append("- 当前脚本支持: 胜率、平均/中位收益、最好/最差、MFE、MAE、坏信号归因、路由模拟。")
    lines.append("- 当前脚本不提供: 组合净值曲线、组合最大回撤、Sharpe、资金管理后的实盘权益回测。")
    return "\n".join(lines) + "\n"


def route_text(stats: dict | None) -> str:
    if not stats:
        return "-"
    return f"样本{stats['n']} 胜率{stats['wins']}/{stats['n']} {stats['win_rate']:.1f}% 平均{stats['avg']:+.2f}%"


def build_tuning_summary(parsed: dict, report_path: Path) -> str:
    lines = []
    lines.append(f"每日合约监控信号调参日报 {today_local()}")
    lines.append(f"回测报告: {report_path}")
    lines.append("")
    lines.append("先看结论")

    kind_counts = parsed.get("kind_counts") or {}
    total = sum(kind_counts.values()) or 0
    mmw = kind_counts.get("main_momentum_watch", 0)
    mrw = kind_counts.get("main_risk_watch", 0)
    neutral_share = ((mmw + mrw) / total * 100) if total else 0.0
    lines.append(f"- 主流观察类占比: {mmw + mrw}/{total} ({neutral_share:.1f}%)")

    best = parsed.get("best_kind_1h")
    worst = parsed.get("noisiest_kind_1h")
    lines.append(
        f"- 最好信号: {best['kind']} {best['avg']:+.2f}% / {best['win_rate']:.1f}%"
        if best else "- 最好信号: -"
    )
    lines.append(
        f"- 最噪信号: {worst['kind']} {worst['avg']:+.2f}% / {worst['win_rate']:.1f}%"
        if worst else "- 最噪信号: -"
    )

    lines.append("")
    lines.append("重点盯的信号")
    for kind in ("main_momentum_watch", "main_risk_watch", "discovery", "top_exhaustion", "top_risk"):
        stat = (parsed.get("kind_stats_1h") or {}).get(kind)
        count = kind_counts.get(kind, 0)
        if stat:
            lines.append(f"- {kind}: 样本{count} 1h胜率{stat['wins']}/{stat['n']} {stat['win_rate']:.1f}% 平均{stat['avg']:+.2f}%")
        elif count:
            lines.append(f"- {kind}: 样本{count}")

    lines.append("")
    lines.append("低胜率维护清单")
    weak_items = []
    for kind, stat in (parsed.get("kind_stats_1h") or {}).items():
        if stat.get("n", 0) >= 5 and (stat.get("win_rate", 0) < 45 or stat.get("avg", 0) < 0):
            weak_items.append((kind, stat))
    weak_items.sort(key=lambda item: (item[1].get("avg", 0), item[1].get("win_rate", 0), -item[1].get("n", 0)))
    if weak_items:
        for kind, stat in weak_items[:6]:
            action = "降级到摘要/观察，收紧实时推送"
            if kind in {"top_exhaustion", "top_risk"}:
                action = "保留风险观察，但要求更强反向确认"
            elif kind in {"discovery", "main_momentum_watch", "main_risk_watch"}:
                action = "降低优先级，要求大盘/资金/入场三项共振"
            lines.append(
                f"- {kind}: 1h胜率{stat['wins']}/{stat['n']} {stat['win_rate']:.1f}% "
                f"平均{stat['avg']:+.2f}% -> {action}"
            )
    else:
        lines.append("- 暂无达到样本门槛的低胜率维护项。")

    lines.append("")
    lines.append("路由观察")
    actual_1h = parsed.get("actual_routes_1h") or {}
    sim_1h = parsed.get("sim_routes_1h") or {}
    lines.append(f"- 实际 observe: {route_text(actual_1h.get('observe'))}")
    lines.append(f"- 实际 priority_observe: {route_text(actual_1h.get('priority_observe'))}")
    lines.append(f"- 实际 digest: {route_text(actual_1h.get('digest'))}")
    lines.append(f"- 模拟 observe: {route_text(sim_1h.get('observe'))}")
    lines.append(f"- 模拟 priority_observe: {route_text(sim_1h.get('priority_observe'))}")
    lines.append(f"- 模拟 digest: {route_text(sim_1h.get('digest'))}")

    lines.append("")
    lines.append("今日调参判断")
    recommendations = []
    if neutral_share >= 70:
        recommendations.append("主流观察类占比仍高，先继续收紧 main_momentum_watch / main_risk_watch。")
    if worst and worst["kind"] in {"main_momentum_watch", "main_risk_watch", "discovery"}:
        recommendations.append(f"{worst['kind']} 仍是主要噪音源，继续优先压回 digest。")
    sim_priority = sim_1h.get("priority_observe")
    if sim_priority and sim_priority["avg"] <= 0.05:
        recommendations.append("priority_observe 方向性还不够强，暂时不要放宽阈值。")
    sim_digest = sim_1h.get("digest")
    if sim_digest and sim_digest["avg"] > 0.2 and sim_digest["win_rate"] >= 55:
        recommendations.append("digest 里开始积累正收益样本，后面要检查是否漏掉该提级的信号。")
    if not recommendations:
        recommendations.append("今天没有明显的新问题，先继续观察，不急着改评分逻辑。")
    if weak_items:
        primary = weak_items[0][0]
        recommendations.insert(0, f"今天优先维护 {primary}: 先压低路由级别，再看明天 1h/4h 是否改善。")
    for item in recommendations[:4]:
        lines.append(f"- {item}")

    lines.append("")
    lines.append("什么时候该重做评分")
    lines.append("- 连续几天 priority_observe / realtime 的 1h、4h 平均都接近 0 或为负。")
    lines.append("- digest 持续漏掉明显逃顶或抄底样本。")
    lines.append("- 高分和低分信号表现没有层次差。")
    lines.append("- main_momentum_watch / main_risk_watch 继续大量占比，但没有方向增益。")
    return "\n".join(lines) + "\n"


def write_outputs(limit: int) -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    day = today_local()
    report_path = OUTPUT_DIR / f"report_{day}.txt"
    summary_path = OUTPUT_DIR / f"summary_{day}.txt"
    tuning_path = OUTPUT_DIR / f"tuning_{day}.txt"
    json_path = OUTPUT_DIR / f"summary_{day}.json"
    latest_report = OUTPUT_DIR / "latest_report.txt"
    latest_summary = OUTPUT_DIR / "latest_summary.txt"
    latest_tuning = OUTPUT_DIR / "latest_tuning.txt"
    latest_json = OUTPUT_DIR / "latest_summary.json"

    run_backtest(report_path, limit)
    report_text = report_path.read_text(encoding="utf-8")
    parsed = parse_report(report_text)
    human = build_human_summary(parsed, report_path)
    tuning = build_tuning_summary(parsed, report_path)

    summary_path.write_text(human, encoding="utf-8")
    tuning_path.write_text(tuning, encoding="utf-8")
    json_path.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")
    latest_report.write_text(report_text, encoding="utf-8")
    latest_summary.write_text(human, encoding="utf-8")
    latest_tuning.write_text(tuning, encoding="utf-8")
    latest_json.write_text(json.dumps(parsed, ensure_ascii=False, indent=2), encoding="utf-8")

    return {
        "date": day,
        "report_path": str(report_path),
        "summary_path": str(summary_path),
        "tuning_path": str(tuning_path),
        "json_path": str(json_path),
        "latest_report": str(latest_report),
        "latest_summary": str(latest_summary),
        "latest_tuning": str(latest_tuning),
        "latest_json": str(latest_json),
        "parsed": parsed,
    }


def main():
    load_env_file()
    parser = argparse.ArgumentParser(description="Daily signal backtest wrapper")
    parser.add_argument("--limit", type=int, default=80)
    parser.add_argument("--print-summary", action="store_true")
    parser.add_argument("--no-discord", action="store_true")
    args = parser.parse_args()

    result = write_outputs(args.limit)
    if not args.no_discord:
        summary_text = Path(result["summary_path"]).read_text(encoding="utf-8")
        tuning_text = Path(result["tuning_path"]).read_text(encoding="utf-8")
        discord_send(f"📊 每日信号回测日报 {result['date']}", summary_text, color=0x3498DB)
        discord_send(f"🛠 每日低胜率维护计划 {result['date']}", tuning_text, color=0xE67E22)
    if args.print_summary:
        sys.stdout.write(Path(result["summary_path"]).read_text(encoding="utf-8"))
    else:
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
