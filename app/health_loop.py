"""WorkBuddy 每日健康盯梢的确定性闭环状态机。

本模块不访问 GitHub，也不执行修复。它只把可信的运行产物与上一次
正式状态归一为可审计的行动清单，由 WorkBuddy 按项目治理规则执行。
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from .common import read_json, write_json
from .health_issue import build_incidents


SCHEMA_VERSION = "robtaxi-health-loop-v1"
EXTERNAL_REASON_CODES = {
    "access_forbidden",
    "captcha_required",
    "dns_error",
    "rate_limited",
    "ssl_error",
    "timeout",
}


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[dict[str, Any]]:
    return [item for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def incident_key(*, source_id: str, reason_code: str, check_id: str) -> str:
    """返回可读且稳定的闭环事件主键。"""
    source = source_id.strip().lower()
    reason = reason_code.strip().lower() or "unknown"
    if source:
        return f"source:{source}:{reason}"
    return f"check:{check_id.strip().lower() or 'health'}:{reason}"


def _risk_for(source_id: str, reason_code: str) -> str:
    if not source_id:
        return "High"
    if reason_code in EXTERNAL_REASON_CODES:
        return "Low"
    return "Medium"


def _source_participated(run_report: dict[str, Any], source_id: str) -> bool:
    if not source_id:
        return False
    for row in _as_list(run_report.get("source_stats")):
        if str(row.get("source_id", "")) != source_id:
            continue
        # source_stats 中出现该来源代表本轮已尝试；空白状态不能作为证据。
        return bool(str(row.get("status", "")).strip())
    return False


def _valid_recovery_run(health: dict[str, Any], run_report: dict[str, Any], record: dict[str, Any]) -> bool:
    run = _as_dict(health.get("run"))
    source_report = _as_dict(health.get("source_report"))
    if str(run.get("event_name", "")) != "schedule":
        return False
    if not bool(source_report.get("available", False)):
        return False
    if not bool(record.get("merged_commit_reachable", False)):
        return False
    return _source_participated(run_report, str(record.get("source_id", "")))


def _review_events(review: dict[str, Any]) -> list[dict[str, Any]]:
    """仅把明确业务异常转为 High 风险事件，避免未达转正门槛制造噪声。"""
    daily = _as_dict(review.get("daily"))
    gate = _as_dict(review.get("gate"))
    metrics = _as_dict(gate.get("metrics"))
    checks = _as_dict(gate.get("automatic_checks"))
    events: list[dict[str, Any]] = []
    if str(daily.get("business_status", "")) == "empty_uncovered":
        events.append(
            {
                "incident_key": "review:business_empty_uncovered",
                "category": "recall",
                "subject": "Agent-first",
                "reason_code": "business_empty_uncovered",
                "severity": "critical",
                "risk": "High",
                "summary": "Agent 技术运行成功但业务覆盖审计未通过",
            }
        )
    if int(metrics.get("truth_important", 0) or 0) > 0 and not bool(checks.get("important_recall", True)):
        events.append(
            {
                "incident_key": "review:important_recall_below_threshold",
                "category": "recall",
                "subject": "multi-route",
                "reason_code": "important_recall_below_threshold",
                "severity": "critical",
                "risk": "High",
                "summary": "独立复盘显示重要事件召回率未达门槛",
            }
        )
    return events


def _action_signature(row: dict[str, Any]) -> tuple[Any, ...]:
    """只比较需要通知或人工动作的状态，忽略每次运行都会变化的计数。"""
    return (
        row.get("incident_key"),
        row.get("action"),
        row.get("severity"),
        row.get("engineering_issue"),
        row.get("reopen", False),
        row.get("recovery_count", 0),
        row.get("risk"),
    )


def evaluate_health_loop(
    health: dict[str, Any],
    *,
    previous_state: dict[str, Any] | None = None,
    run_report: dict[str, Any] | None = None,
    review: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """把一次运行及历史状态转换为无副作用的行动清单。"""
    previous = _as_dict(previous_state or {})
    previous_incidents = _as_dict(previous.get("incidents"))
    previous_actions = {
        str(row.get("incident_key", "")): row
        for row in _as_list(previous.get("actions"))
        if row.get("incident_key")
    }
    report = _as_dict(run_report or {})
    run = _as_dict(health.get("run"))
    run_id = str(run.get("github_run_id", ""))
    active: dict[str, dict[str, Any]] = {}
    actions: list[dict[str, Any]] = []

    for incident in build_incidents(health):
        source_id = str(incident.get("source_id", ""))
        reason_code = str(incident.get("reason_code", ""))
        check_id = str(incident.get("check_id", ""))
        key = incident_key(source_id=source_id, reason_code=reason_code, check_id=check_id)
        old = _as_dict(previous_incidents.get(key))
        occurrence_count = int(old.get("occurrence_count", 0) or 0)
        if str(old.get("last_run_id", "")) != run_id:
            occurrence_count += 1
        severity = str(incident.get("severity", "warning"))
        external = reason_code in EXTERNAL_REASON_CODES
        engineering_issue = old.get("engineering_issue")
        needs_task = severity in {"critical", "error"} or occurrence_count >= 2
        reopen = bool(engineering_issue) and str(old.get("lifecycle", "")) in {"observing", "closed"}
        if external:
            action = "no_code_change"
        elif needs_task and engineering_issue and reopen:
            # 合并后的观察期再次出现同一异常：复用原工程任务并回到开发。
            action = "create_task"
        elif needs_task and engineering_issue:
            # 工程任务已经在处理，继续保存证据但不要每天重复创建/通知。
            action = "observe"
        elif needs_task:
            action = "create_task"
        else:
            action = "observe"
        active[key] = {
            "incident_key": key,
            "category": "source" if source_id else "delivery",
            "source_id": source_id,
            "subject": source_id or check_id,
            "reason_code": reason_code,
            "check_id": check_id,
            "severity": severity,
            "risk": _risk_for(source_id, reason_code),
            "occurrence_count": occurrence_count,
            "last_run_id": run_id,
            "engineering_issue": engineering_issue,
            "merged_commit": old.get("merged_commit", ""),
            "merged_commit_reachable": bool(old.get("merged_commit_reachable", False)),
            # 只要本轮仍有异常，原来的观察/关闭状态立即失效。
            "lifecycle": "open",
            "recovery_count": 0,
            "autofix_eligible": False,
            "autofix_block_reason": "阶段A只登记和验证，不自动修复代码",
        }
        actions.append(
            {
                "action": action,
                **active[key],
                "reopen": reopen,
                "summary": str(incident.get("summary", "")),
            }
        )

    # 只有没有同一事件、且运行证据完整时，才计算合并后的恢复次数。
    for key, value in previous_incidents.items():
        if key in active:
            continue
        old = _as_dict(value)
        if not old.get("engineering_issue") or not _valid_recovery_run(health, report, old):
            continue
        recovery_count = int(old.get("recovery_count", 0) or 0) + 1
        next_record = {
            **old,
            "recovery_count": recovery_count,
            "last_run_id": run_id,
            "lifecycle": "observing" if recovery_count < 2 else "closed",
        }
        active[str(key)] = next_record
        actions.append(
            {
                "action": "close" if recovery_count >= 2 else "verify",
                **next_record,
                "summary": "合并后正常定时运行未再发现该事件",
            }
        )

    for event in _review_events(_as_dict(review or {})):
        key = str(event["incident_key"])
        old = _as_dict(previous_incidents.get(key))
        occurrence_count = int(old.get("occurrence_count", 0) or 0)
        if str(old.get("last_run_id", "")) != run_id:
            occurrence_count += 1
        record = {
            **event,
            "occurrence_count": occurrence_count,
            "last_run_id": run_id,
            "engineering_issue": old.get("engineering_issue"),
            "recovery_count": 0,
            "autofix_eligible": False,
            "autofix_block_reason": "业务质量问题必须人工批准",
        }
        active[key] = record
        actions.append({"action": "needs_approval", **record})

    changed_actions = [
        row
        for row in actions
        if _action_signature(row) != _action_signature(_as_dict(previous_actions.get(str(row.get("incident_key", "")))))
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "date_bj": str(health.get("date_bj", "")),
        "incidents": active,
        "actions": actions,
        "changed_actions": changed_actions,
        "changed": bool(changed_actions),
    }


def render_daily_report(state: dict[str, Any]) -> str:
    action_rows = _as_list(state.get("changed_actions"))
    groups = {
        "新发现": [row for row in action_rows if row["action"] == "create_task"],
        "跟踪中": [row for row in action_rows if row["action"] == "observe"],
        "已上线但等待验证": [row for row in action_rows if row["action"] == "verify"],
        "已恢复": [row for row in action_rows if row["action"] == "close"],
        "需要你批准": [row for row in action_rows if row["action"] == "needs_approval"],
        "无需代码修复": [row for row in action_rows if row["action"] == "no_code_change"],
    }
    lines = ["# Robotaxi 每日盯梢", "", f"- 统计日期：{state['date_bj']}"]
    lines.append("- 今日结论：" + ("存在需要处理的状态变化" if state["changed"] else "无状态变化"))
    for heading, rows in groups.items():
        if not rows:
            continue
        lines.extend(["", f"## {heading}", ""])
        for row in rows:
            lines.append(f"- {row.get('subject', '')} · {row.get('reason_code', '')}：{row.get('summary', '')}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="生成 WorkBuddy 健康闭环行动清单")
    parser.add_argument("--health-report", required=True)
    parser.add_argument("--state", default="")
    parser.add_argument("--run-report", default="")
    parser.add_argument("--review-report", default="")
    parser.add_argument("--state-out", required=True)
    parser.add_argument("--report-out", required=True)
    args = parser.parse_args()
    health = read_json(Path(args.health_report))
    previous = read_json(Path(args.state)) if args.state and Path(args.state).exists() else {}
    run_report = read_json(Path(args.run_report)) if args.run_report and Path(args.run_report).exists() else {}
    review = read_json(Path(args.review_report)) if args.review_report and Path(args.review_report).exists() else {}
    state = evaluate_health_loop(health, previous_state=previous, run_report=run_report, review=review)
    write_json(Path(args.state_out), state)
    Path(args.report_out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.report_out).write_text(render_daily_report(state), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
