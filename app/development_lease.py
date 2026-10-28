"""整轮租约判定；调用方须在仓库本机锁内读取正式事件并执行写入。"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path
import re

from app.development_policy import require, timestamp


def run_directory(root: Path, run_id: str) -> Path:
    require(bool(re.fullmatch(r"[A-Za-z0-9_-]{8,100}", run_id or "")), "必须提供有效的本次run-id")
    return root / ".local" / "robtaxi-development" / "runs" / run_id


def unfinished_runs(events: list[dict]) -> list[dict]:
    ended = {e.get("run_id") for e in events if e.get("event") in {"run_finished", "run_expired"}}
    starts = {}
    for event in events:
        if event.get("event") == "run_started":
            run_id = event.get("run_id")
            require(bool(run_id), "正式运行缺少run-id")
            require(run_id not in starts or starts[run_id]["deadline"] == event["deadline"], "正式运行截止时间冲突")
            starts[run_id] = event
    return [e for key, e in starts.items() if key not in ended]


def require_exclusive_run(events: list[dict], run_id: str, now: datetime) -> None:
    require(not any(e.get("run_id") == run_id and e.get("event") in {"run_finished", "run_expired"}
                    for e in events), "本次运行已结束，禁止旧上下文继续写入")
    active = [e for e in unfinished_runs(events) if timestamp(e["deadline"]) > now]
    require(len(active) <= 1, "多个有效整轮租约，需要先对账")
    require(not active or active[0]["run_id"] == run_id, "并发跳过：整轮租约由另一运行持有")


def isolated_output(root: Path, run_id: str, supplied: str | None, name: str) -> Path:
    directory = run_directory(root, run_id).resolve()
    path = Path(supplied).resolve() if supplied else directory / name
    require(path.is_relative_to(directory), "运行产物必须位于本次run-id的独立目录")
    return path


def validate_health_receipt(health: dict, run_id: str) -> None:
    require(health.get("development_run_id") == run_id, "健康回执不属于本次运行，不得复用旧回执刷新心跳")
    require(health.get("complete") is True and health.get("mode") == "apply"
            and health.get("official_state", {}).get("complete") is True,
            "健康对账未完成，不得写成功心跳")
