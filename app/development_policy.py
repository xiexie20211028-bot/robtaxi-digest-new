"""无人值守研发的纯函数边界；模型输出不是授权，也不是验收证据。"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import re
from datetime import datetime, timedelta
from pathlib import PurePosixPath
from zoneinfo import ZoneInfo

BJ = ZoneInfo("Asia/Shanghai")
SCHEMA = "robtaxi-execution-v1"


class DevelopmentError(RuntimeError):
    pass


def require(condition: bool, message: str) -> None:
    if not condition:
        raise DevelopmentError(message)


def digest(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode()).hexdigest()


def timestamp(value: str) -> datetime:
    result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    require(result.tzinfo is not None, "时间必须包含时区")
    return result


def periods(now: datetime) -> tuple[str, str]:
    require(now.tzinfo is not None, "当前时间必须包含时区")
    local = now.astimezone(BJ)
    return local.date().isoformat(), local.strftime("%Y-%m")


def validate_policy(policy: dict, legacy: dict | None = None) -> None:
    require(policy.get("schema_version") == "robtaxi-autonomy-policy-v1", "策略版本不支持")
    require(policy.get("mode") in {"off", "shadow", "pilot", "active"}, "运行模式无效")
    require(not (legacy or {}).get("enabled") or policy["mode"] == "off", "旧单信源自动修复与新调度不能同时启用")
    for key, ceiling in {"monthly_extra_fen": 10000, "codex_daily_batches": 1, "batch_items": 3,
                         "codex_timeout_seconds": 1200, "execution_timeout_seconds": 3600,
                         "daily_merges": 1, "max_repair_attempts": 2}.items():
        require(type(policy.get(key)) is int and 0 < policy[key] <= ceiling, f"{key} 超过已授权上限")
    # 首版没有额外付费通道的计量/预留提供者，不能仅改开关启用。
    require(policy.get("paid_channels_enabled") is False, "额外付费入口尚无可验证计量，必须关闭")
    if policy["mode"] in {"pilot", "active"}:
        evidence = policy.get("activation_evidence", {})
        evidence_prefix = f"https://github.com/{policy.get('repository', '')}/"
        for key in ("bridge", "normal_shadow", "high_shadow", "recovery_shadow", "worker_timeout", "billing_disabled"):
            value = evidence.get(key)
            require(isinstance(value, str) and value.startswith(evidence_prefix)
                    and ("/issues/" in value or "/actions/" in value), f"缺少正式启用证据：{key}")
        require(bool(policy.get("worker_argv")), "尚未配置可监督的 WorkBuddy 执行入口")
        require(type(policy.get("pilot_issue")) is int, "缺少唯一试点 Issue")
        if policy["mode"] == "active":
            value = evidence.get("pilot_production")
            require(isinstance(value, str) and value.startswith(evidence_prefix)
                    and ("/issues/" in value or "/actions/" in value), "试点尚未通过真实生产验收")


def safe_path(path: str) -> bool:
    return isinstance(path, str) and bool(path) and not PurePosixPath(path).is_absolute() and ".." not in PurePosixPath(path).parts and "\\" not in path and not path.startswith(".git/")


def matches(path: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns)


def validate_contract(contract: dict, issue: int | None = None) -> None:
    require(isinstance(contract, dict) and contract.get("schema_version") == SCHEMA, "执行说明版本不支持")
    require(type(contract.get("issue")) is int and contract["issue"] > 0, "必须关联正式 Issue")
    require(issue is None or contract["issue"] == issue, "执行说明主任务不匹配")
    require(type(contract.get("version")) is int and contract["version"] > 0, "方案版本无效")
    require(bool(re.fullmatch(r"[0-9a-f]{40}", str(contract.get("base_sha", "")))), "缺少完整代码版本")
    for key in ("goal", "risk_reason"):
        require(isinstance(contract.get(key), str) and bool(contract[key].strip()), f"缺少 {key}")
    for key in ("acceptance", "allowed_paths", "relevant_paths", "tests", "rollback_conditions"):
        require(isinstance(contract.get(key), list) and bool(contract[key]) and all(isinstance(x, str) and x.strip() for x in contract[key]), f"缺少 {key}")
    for path in contract["allowed_paths"] + contract["relevant_paths"]:
        require(safe_path(path), "执行说明包含越界路径")
    require(contract.get("risk") in {"Low", "Medium", "High"}, "风险等级无效")
    require(isinstance(contract.get("dependencies"), list) and all(type(x) is int and x > 0 for x in contract["dependencies"]), "依赖格式无效")
    require(isinstance(contract.get("reserved_decisions"), list), "缺少保留事项声明")
    production = contract.get("production", {})
    require(production.get("kind") in {"none", "source", "task"}, "缺少生产验收方式")
    require(isinstance(production.get("checks"), list) and bool(production["checks"]), "缺少生产验证/免观察理由")
    if production["kind"] == "source":
        require(bool(production.get("source_id")), "源恢复必须指定 source_id")


def check_fresh(contract: dict, changed_paths: list[str]) -> None:
    require(not any(matches(path, contract["relevant_paths"] + contract["allowed_paths"]) for path in changed_paths), "相关代码已变化，需要重新规划")


def classify_change(contract: dict, paths: list[str], policy: dict, *, priority: str = "P2", route: str = "") -> str:
    require(bool(paths) and all(safe_path(path) for path in paths), "修改路径为空或越界")
    require(all(matches(path, contract["allowed_paths"]) for path in paths), "超出执行说明允许范围")
    require(not contract["reserved_decisions"], "涉及人类保留决策")
    require(not any(matches(path, policy["protected_paths"]) for path in paths), "修改授权、门禁或基线需要人工决策")
    if contract["risk"] == "High" or priority == "P0" or route == "共同" or any(matches(path, policy["high_impact_paths"]) for path in paths):
        return "High"
    return contract["risk"]


def select_tasks(tasks: list[dict], policy: dict) -> dict:
    candidates, planning, reviews, deliveries = [], [], [], []
    if policy.get("mode") == "pilot":
        # 试点期间连规划批次也只允许唯一 Issue，避免高优先级普通队列挤占或越界执行。
        tasks = [task for task in tasks if task.get("number") == policy.get("pilot_issue")]
    target_order = {"本周": 0, "下周": 1, "本月": 2, "未来": 3, "持续": 4}
    for task in tasks:
        labels = set(task.get("labels", []))
        if task.get("state") != "OPEN" or task.get("type") == "Epic" or task.get("status") in {"观察中", "已完成", "已取消"} or labels.intersection({policy["labels"]["paused"], policy["labels"]["human"], "robtaxi-health", "health-alert"}):
            continue
        if task.get("review_needed"):
            reviews.append(task)
            continue
        if task.get("open_pr"):
            deliveries.append(task)
            continue
        if task.get("awaiting_production"):
            continue
        if task.get("blockers"):
            # 阻塞项可以规划，但不能领取执行。
            if not task.get("contract"):
                planning.append(task)
            continue
        if not task.get("contract") or task.get("stale") or task.get("repair_attempts", 0) >= policy["max_repair_attempts"]:
            planning.append(task)
        elif task.get("status") in {"Inbox", "待办", "开发中", "待验证"}:
            candidates.append(task)
    order = lambda row: (row.get("priority", "P3"), target_order.get(row.get("target"), 5), row["number"])
    candidates.sort(key=lambda row: (row.get("status") != "开发中", *order(row)))
    # 存在多个未完成领取时，必须先对账，不能悄悄并行执行。
    running = [t for t in candidates if t.get("status") == "开发中"]
    deliveries.sort(key=order)
    return {"batch": (sorted(reviews, key=order) + sorted(planning, key=lambda t: (not bool(t.get("blockers")), *order(t))))[:policy["batch_items"]],
            "task": candidates[0] if candidates and len(running) <= 1 else None,
            "delivery": deliveries[0] if deliveries else None,
            "conflicting_running": [t["number"] for t in running] if len(running) > 1 else []}


def reserve(events: list[dict], policy: dict, now: datetime, kind: str, key: str, *, extra_fen: int | None = 0, channel: str = "subscription") -> dict:
    day, month = periods(now)
    require(kind in {"codex", "execute", "merge"}, "无效预留类型")
    require(not any(e.get("key") == key and e.get("event") == "reserve" for e in events), "该动作已预留；不得重复调用，先恢复正式状态")
    require(channel == "subscription" and extra_fen == 0, "首版只允许无新增扣费的现有套餐入口")
    monthly = sum(e.get("extra_fen", 0) for e in events if e.get("event") == "reserve" and e.get("month") == month)
    require(monthly + extra_fen <= policy["monthly_extra_fen"], "月度新增费用预算不足")
    limit = {"codex": policy["codex_daily_batches"], "execute": 1, "merge": policy["daily_merges"]}[kind]
    count = sum(e.get("event") == "reserve" and e.get("day") == day and e.get("kind") == kind for e in events)
    require(count < limit, f"今天的 {kind} 额度已预留或消耗")
    return {"event": "reserve", "kind": kind, "key": key, "at": now.isoformat(), "day": day, "month": month, "extra_fen": extra_fen, "channel": channel}


def verify_review(review: dict, contract: dict, head_sha: str, base_sha: str) -> None:
    require(review.get("verdict") == "approve" and review.get("producer") == "codex-exec", "缺少 Codex 独立复核通过证据")
    require(review.get("head_sha") == head_sha and review.get("base_sha") == base_sha, "PR 或主分支变化，原复核失效")
    require(review.get("contract_digest") == digest(contract), "复核针对另一版执行说明")
    require(bool(review.get("evidence")), "复核缺少证据")


def production_status(contract: dict, merge_sha: str, runs: list[dict]) -> str:
    """输入须由 GitHub/正常生产产物适配器验证；不能接受模型自行勾选。"""
    if not merge_sha:
        return "awaiting_merge"
    if contract["production"]["kind"] == "none":
        return "complete"
    seen, successes = set(), 0
    for run in sorted(runs, key=lambda row: timestamp(row["created_at"])):
        if run["id"] in seen:
            continue
        seen.add(run["id"])
        if run.get("event") != "schedule" or not run.get("contains_merge"):
            continue
        source = contract["production"].get("source_id")
        if not run.get("artifacts_complete") or (source and source not in run.get("sources_executed", [])):
            successes = 0
            continue
        if not run.get("acceptance_passed"):
            return "requeue"
        successes += 1
    return "complete" if successes >= (2 if contract["production"]["kind"] == "source" else 1) else "observing"


def heartbeat_transition(events: list[dict], now: datetime, hours: int = 36) -> dict | None:
    beats = [e for e in events if e.get("event") == "heartbeat" and e.get("success") is True]
    last = max((timestamp(e["at"]) for e in beats), default=None)
    require(last is None or last <= now + timedelta(minutes=5), "心跳时间在未来，证据无效")
    stale = last is None or now - last > timedelta(hours=hours)
    notifications = [e for e in events if e.get("event") == "heartbeat_status"]
    previous = notifications[-1].get("status") if notifications else "healthy"
    current = "stale" if stale else "healthy"
    return {"event": "heartbeat_status", "status": current, "at": now.isoformat(), "last_success": last.isoformat() if last else None} if current != previous else None


def rollback_decision(contract: dict, evidence: dict) -> dict:
    """回退前证据判定；不允许把模型的建议直接当作触发条件。"""
    require(evidence.get("verified") is True, "回退证据未验证")
    triggered = set(evidence.get("triggered_conditions", []))
    require(bool(triggered) and triggered.issubset(set(contract["rollback_conditions"])), "回退条件未触发或不在原方案内")
    safe = (evidence.get("accepted_previous_sha") and evidence.get("merge_sha")
            and evidence.get("conflict_free") is True and evidence.get("dependent_changes") == []
            and evidence.get("revert_tests_passed") is True)
    return {"action": "revert_pr" if safe else "freeze_release", "conditions": sorted(triggered),
            "merge_sha": evidence.get("merge_sha"), "accepted_previous_sha": evidence.get("accepted_previous_sha")}
