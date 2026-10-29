"""Codex 定时研发的确定性状态入口；模型负责开发，脚本负责授权与证据。"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.development_policy import (DevelopmentError, check_fresh, digest, heartbeat_transition, matches,
                                    periods, require, reserve, select_tasks, timestamp, validate_contract,
                                    validate_policy, verify_review)
from app.development_runtime import GitHub, repository_lock, run
from app.development_recovery import RecoveryBudget
from app.development_lease import isolated_output, require_exclusive_run, run_directory, unfinished_runs, validate_health_receipt
from app.development_budget import budgeted, remaining_seconds
from scripts.validate_project_task import primary_task_reference_from_pr_body

ROOT = Path(__file__).resolve().parent.parent
POLICY = ROOT / ".github/robtaxi-autonomy.json"
CHECKPOINT_STAGES = {"planned", "code_changed", "tests_passed", "pr_created", "waiting_ci", "blocked"}


def load_policy() -> dict:
    policy = json.loads(POLICY.read_text())
    legacy_path = ROOT / ".github/robtaxi-health-autofix.json"
    validate_policy(policy, json.loads(legacy_path.read_text()) if legacy_path.exists() else {})
    return policy


def snapshot(client: GitHub, policy: dict, issue: int | None = None) -> dict:
    events = client.events()
    health_queues = [e for e in events if e.get("event") == "health_queue_updated"]
    health_queue = max(health_queues, key=lambda e: (int(e.get("production_run_id", 0)), int(e.get("attempt", 1)), e.get("at", "")), default={})
    health_tasks = {r["issue"]: r for r in health_queue.get("entries", []) if r.get("issue")}
    if issue is None:
        tasks, pulls = client.tasks(), client.pulls()
        merged_pulls = client.merged_pulls()
    else:
        tasks = [client.task(issue)]
        related = client.issue_pulls(issue)
        require(len(related) < 100, "关联PR列表不完整")
        pulls = [p for p in related if p["state"] == "OPEN" and p.get("baseRefName") == "main"]
        merged_pulls = [p for p in related if p["state"] == "MERGED" and p.get("baseRefName") == "main"]
    main = client.main_sha()
    tasks = [task for task in tasks if task["number"] != policy["control_issue"]]
    # 一次拉取开放任务的正式交接，完整分页；本地缓存丢失不会重新消费调用/合并配额。
    eligible = [task for task in tasks if task["state"] == "OPEN" and task["type"] != "Epic"]
    if policy.get("mode") == "pilot":
        eligible = [t for t in eligible if t["number"] == policy["pilot_issue"]]
    records = {t["number"]: client.events(t["number"]) for t in eligible}
    by_issue, merged_by_issue = {}, {}
    for pr in pulls:
        try:
            issue, _ = primary_task_reference_from_pr_body(pr["body"])
        except Exception:
            continue
        by_issue.setdefault(issue, []).append(pr)
    for pr in merged_pulls:
        if not pr["headRefName"].startswith(policy["branch_prefix"]):
            continue
        try:
            issue, _ = primary_task_reference_from_pr_body(pr["body"])
        except Exception:
            continue
        merged_by_issue.setdefault(issue, []).append(pr)
    for task in tasks:
        if "body" in task:
            section = re.search(r"(?ms)^## 验收标准\s*\n(.*?)(?=^## |\Z)", task["body"])
            task["acceptance_clear"] = bool(section and section[1].strip())
        history = records.get(task["number"], [])
        plans = [e for e in history if e.get("event") == "contract" and
                 e.get("producer") in {"codex-exec", "codex-scheduled"}]
        if plans:
            contract = plans[-1]["contract"]
            validate_contract(contract, task["number"])
            task["contract"] = contract
            task["contract_digest"] = digest(contract)
            task["contract_url"] = plans[-1].get("_comment_url")
            changed = client.changed_since(contract["base_sha"], main)
            try:
                check_fresh(contract, changed)
            except DevelopmentError:
                task["stale"] = True
            task["repair_attempts"] = sum(e.get("event") in {"attempt", "run_failed"}
                                          and e.get("changed") is True
                                          and e.get("passed") is not True
                                          and e.get("contract_digest", digest(contract)) == digest(contract)
                                          for e in history)
            for dependency in contract["dependencies"]:
                require(dependency != task["number"], "任务不能依赖自己")
                actual = next((t for t in tasks if t["number"] == dependency), None)
                dependency_state = actual["state"] if actual else client.api(f"repos/{policy['repository']}/issues/{dependency}")["state"].upper()
                if dependency_state == "OPEN" and dependency not in task["blockers"]:
                    task["blockers"].append(dependency)
        prs = by_issue.get(task["number"], [])
        require(len(prs) <= 1, f"Issue #{task['number']} 有多个开放主 PR，请先对账")
        if prs:
            task["open_pr"] = prs[0]
            reviews = [e for e in history if e.get("event") == "review"
                       and e.get("producer") == "codex-scheduled"
                       and e.get("head_sha") == prs[0]["headRefOid"] and e.get("base_sha") == main
                       and e.get("contract_digest") == task.get("contract_digest")]
            contract = task.get("contract") or {}
            planned_high = (contract.get("risk") == "High" or task.get("priority") == "P0"
                            or task.get("route") == "共同"
                            or any(matches(path, policy["high_impact_paths"])
                                   for path in contract.get("allowed_paths", [])))
            task["changes_requested"] = bool(reviews) and reviews[-1].get("verdict") == "changes_requested"
            task["ci_failed"] = any(e.get("event") == "run_failed" and e.get("reason") == "ci_failed"
                                    and e.get("head_sha") == prs[0]["headRefOid"] for e in history)
            task["review_needed"] = bool(contract) and planned_high and not reviews
        elif merged_by_issue.get(task["number"]):
            # 即使进程在合并成功、落回执之前崩溃，也不重新创建 PR。
            task["awaiting_production"] = sorted(merged_by_issue[task["number"]], key=lambda p: p["mergedAt"])[-1]
            merged_pr = task["awaiting_production"]
            deliveries = [event for event in history if event.get("event") == "delivery"
                          and event.get("pr") == merged_pr["number"]
                          and event.get("head_sha") == merged_pr["headRefOid"]]
            verified = [event for event in history if event.get("event") == "production_verified"
                        and event.get("merge_sha") == (merged_pr.get("mergeCommit") or {}).get("oid")]
            if not deliveries or (task.get("contract", {}).get("production", {}).get("kind") == "none" and not verified):
                task["delivery_recovery"] = True
            failures = [e for e in history if e.get("event") == "production_requeue"
                        and e.get("merge_sha") == (merged_pr.get("mergeCommit") or {}).get("oid")]
            if failures:
                task.pop("awaiting_production", None)
                task.pop("delivery_recovery", None)
                task["stale"] = not plans or plans[-1].get("at", "") <= failures[-1]["at"]
        task["events"] = history
        if budgeted(policy):
            health = health_tasks.get(task["number"], {})
            task.update(health_severity=health.get("severity"), health_actionable=health.get("actionable", False),
                        health_evidence_digest=health.get("evidence_digest"))
            if health and not health.get("actionable"):
                task["blocked_reason"] = health.get("blocked_reason") or "来源异常缺少可代码修复证据"
            passed_at = max((e.get("at", "") for e in history if e.get("event") == "checkpoint" and e.get("stage") == "tests_passed"), default="")
            task["repair_attempts"] = sum(e.get("event") == "run_failed" and e.get("changed") is True
                                          and e.get("passed") is not True and (not e.get("at") or e["at"] > passed_at) for e in history)
            if not task.get("risk"):
                task["blocked_reason"] = "风险尚未评估"
            if task.get("review_needed") and periods(datetime.now(timezone.utc))[0] <= periods(timestamp(task["open_pr"]["createdAt"]))[0]:
                task["blocked_reason"] = "等待下一天独立复核"
            releases = [e for e in history if e.get("event") == "task_released" and e.get("stage") == "blocked"]
            if releases and releases[-1].get("evidence_digest") == (task.get("health_evidence_digest") or task.get("contract_digest")):
                task["blocked_reason"] = "同一证据下任务仍阻塞，等待新证据或明确处理"
            production = (task.get("contract") or {}).get("production", {})
            if production.get("kind") in {"source", "task"}:
                registration = policy.get("production_verifiers", {}).get(str(task["number"]), {})
                if registration.get("kind") != production["kind"] or (production["kind"] == "source" and registration.get("source_id") != production.get("source_id")):
                    task["blocked_reason"] = "缺少任务绑定的可信生产验收器"
    selection = select_tasks(tasks, policy)
    # 当天已领取的Issue优先恢复，不受新的P0或阶段变化挤占；其他运行不能接管租约。
    day, _ = periods(datetime.now(timezone.utc))
    claimed = [e for e in events if e.get("event") == "task_claimed" and e.get("day") == day]
    active_numbers = {e["issue"] for e in events if e.get("event") == "task_claimed"
                      and active_claim(events, e["issue"], datetime.now(timezone.utc))}
    require(len(active_numbers) <= 1, "多个有效任务租约，需要先对账")
    if active_numbers:
        selection = select_tasks([t for t in tasks if t["number"] in active_numbers], policy)
    elif claimed and not budgeted(policy):
        numbers = {e["issue"] for e in claimed}
        require(len(numbers) == 1, "当天存在多个任务领取，需要对账")
        selection = select_tasks([t for t in tasks if t["number"] in numbers], policy)
    result = {"schema_version": "robtaxi-development-snapshot-v2", "main_sha": main,
              "events": events, "tasks": tasks, **selection}
    if budgeted(policy):
        result["budget_seconds_remaining"] = remaining_seconds(events, policy, datetime.now(timezone.utc), active_run_id=getattr(client, "run_id", None))
    return result


def assert_trusted_checkout(client: GitHub) -> str:
    head = run(["git", "rev-parse", "HEAD"], cwd=ROOT).strip()
    require(head == client.main_sha(), "调度器必须从最新已合并主分支运行；不能从执行分支启用新授权")
    require(not run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT).strip(), "调度器主分支工作区存在修改")
    return head


def set_status(policy: dict | GitHub, issue: int, status: str) -> None:
    client = GitHub(policy) if isinstance(policy, dict) else policy
    client.set_project_status(issue, status)


def active_claim(events: list[dict], issue: int, now: datetime) -> dict | None:
    claims = [event for event in events if event.get("event") == "task_claimed" and event.get("issue") == issue]
    if not claims:
        return None
    claim_event = claims[-1]
    released = {e.get("claim_key") for e in events if e.get("event") == "task_released"}
    ended = {e.get("run_id") for e in events if e.get("event") in {"run_finished", "run_expired"}}
    if claim_event.get("key") in released or (claim_event.get("run_id") and claim_event["run_id"] in ended):
        return None
    owners = [e for e in events if e.get("event") == "lease_adopted" and e.get("claim_key") == claim_event.get("key")]
    if owners:
        claim_event = {**claim_event, "run_id": owners[0]["run_id"]}
    return claim_event if timestamp(claim_event["lease_until"]) >= now else None


def initialize_run(client: GitHub, policy: dict, run_id: str, *, start: bool = False) -> dict:
    require(bool(re.fullmatch(r"[A-Za-z0-9_-]{8,100}", run_id or "")), "必须提供本次Codex任务ID --run-id")
    events = client.events()
    key = f"run:{run_id}"
    now = datetime.now(timezone.utc)
    require_exclusive_run(events, run_id, now)
    previous = next((e for e in events if e.get("event") == "run_started" and e.get("key") == key), None)
    created = previous is None
    if previous is None:
        require(start, "本次运行尚未登记；运行开始先inspect --start-run")
        assert_trusted_checkout(client)
        available = remaining_seconds(events, policy, now) if budgeted(policy) else policy["task_lease_seconds"]
        require(available > 120, "每日累计预算耗尽或仅剩收尾时间")
        previous = {"event": "run_started", "key": key, "run_id": run_id, "at": now.isoformat(),
                    "deadline": (now + timedelta(seconds=min(available, policy["task_lease_seconds"]))).isoformat(),
                    "owner": run_id, "expired_predecessors": [e["run_id"] for e in unfinished_runs(events)
                        if timestamp(e["deadline"]) <= now]}
        client.append(previous)
    require(timestamp(previous["deadline"]) <= timestamp(previous["at"]) + timedelta(seconds=policy["task_lease_seconds"]), "正式运行预算越界")
    client.run_id, client.run_started = run_id, previous
    client.diagnostics = run_directory(ROOT, run_id) / "diagnostics"
    client.budget = RecoveryBudget(ROOT / ".local/robtaxi-development", run_id, previous["deadline"], policy)
    if not created and not client.budget.path.exists():
        # 正式运行已存在但缓存丢失：不可重新获得额外恢复等待额度。
        client.budget.state = {"spent": client.budget.limit, "rounds": len(client.budget.delays)}
    client.budget.save()
    client.budget.check()
    return previous


def finish_run(client: GitHub, outcome: str) -> dict:
    """结束当前整轮；不把正常退出推断为业务闭环完成。"""
    require(client.run_id and client.budget is not None, "结束运行需要有效整轮租约")
    client.budget.check()
    if budgeted(client.policy):
        events = client.events()
        for issue in {e["issue"] for e in events if e.get("event") == "task_claimed"}:
            claim_event = active_claim(events, issue, datetime.now(timezone.utc))
            if claim_event and claim_event.get("run_id") == client.run_id:
                released = {"event": "task_released", "key": f"release:{claim_event['key']}",
                            "claim_key": claim_event["key"], "issue": issue, "run_id": client.run_id,
                            "stage": "run_ended", "outcome": outcome, "evidence_key": claim_event["key"],
                            "at": datetime.now(timezone.utc).isoformat()}
                _deduplicated_append(client, released, events, None)
                _deduplicated_append(client, released, client.events(issue), issue)
    event = {"event": "run_finished", "key": f"run-finished:{client.run_id}",
             "run_id": client.run_id, "outcome": outcome, "at": datetime.now(timezone.utc).isoformat()}
    client.append(event)
    return event


def require_owner(client: GitHub, claim_event: dict) -> None:
    owner = claim_event.get("run_id")
    run_id = getattr(client, "run_id", None)
    require((not run_id and not owner) or owner == run_id, "有效租约由另一运行持有或尚未认领，禁止同时接管")
    if getattr(client, "budget", None):
        client.budget.deadline = min(client.budget.deadline, timestamp(claim_event["lease_until"]))
        client.budget.check()


def claim(client: GitHub, policy: dict, state: dict, *, issue: int | None = None) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "当前模式禁止领取代码任务")
    assert_trusted_checkout(client)
    require(not state["conflicting_running"], "多个开发中任务需要先对账")
    requested = issue or (state.get("next_action") or {}).get("issue")
    task = next((row for row in state["tasks"] if row["number"] == requested), None)
    require(bool(task), "没有可领取的正式任务")
    require(policy["mode"] != "pilot" or requested == policy["pilot_issue"], "试点阶段不能领取其他任务")
    action = (state.get("next_action") or {}).get("kind")
    require((state.get("next_action") or {}).get("issue") == requested, "任务不是当前确定性队列首项")
    frozen = any(e.get("event") == "release_freeze" and not e.get("resolved") for e in state["events"])
    require(not frozen or action == "review", "存在发布冻结；只能继续复核，不能开发或交付")
    contract = task.get("contract")
    if action != "plan":
        require(bool(contract), "任务尚无有效执行说明；先记录 contract")
        validate_contract(contract, requested)
        require(not task.get("stale"), "执行说明涉及的代码已变化，需要重新规划")
        require(not contract["reserved_decisions"], "任务含人类保留事项")
    require(task["assignees"] > 0 and not task["blockers"], "负责人或依赖不满足领取条件")
    now = datetime.now(timezone.utc)
    day, _ = periods(now)
    existing = active_claim(state["events"], requested, now)
    if existing:
        if not existing.get("run_id") and getattr(client, "run_id", None):
            adoption = {"event": "lease_adopted", "key": f"lease-owner:{existing['key']}",
                        "claim_key": existing["key"], "run_id": client.run_id, "at": now.isoformat()}
            client.append(adoption)
            client.append(adoption, requested)
            existing = {**existing, "run_id": client.run_id}
        require_owner(client, existing)
        # 控制Issue已写、任务Issue未写时补齐相同事件，不能再扣额度。
        _deduplicated_append(client, existing, task.get("events", []), requested)
        set_status(client, requested, "待验证" if action in {"review", "delivery"} else "开发中")
        return {"schema_version": "robtaxi-codex-task-v1", "main_sha": state["main_sha"], "task": task, "claim": existing, "resumed": True}
    require(not any(active_claim(state["events"], e["issue"], now) for e in state["events"]
                    if e.get("event") == "task_claimed"), "另一任务仍有有效租约")
    if budgeted(policy):
        require(client.budget.remaining() > 120, "已进入收尾时间")
        started = any(e.get("event") == "task_claimed" and e.get("issue") == requested and e.get("day") == day for e in state["events"])
        require(started or client.budget.remaining() >= 900, "剩余不足15分钟，不领取新任务")
        sequence = sum(e.get("event") == "task_claimed" and e.get("run_id") == client.run_id for e in state["events"]) + 1
        key = f"task:{client.run_id}:{requested}:{sequence}"
    else:
        key = f"task:{day}:{requested}"
    deadline = now + timedelta(seconds=policy["task_lease_seconds"])
    if getattr(client, "run_started", None):
        deadline = min(deadline, timestamp(client.run_started["deadline"]))
    event = {**reserve(state["events"], policy, now, "task", key), "event": "task_claimed",
             "issue": requested, "action": action, "contract_digest": digest(contract) if contract and action != "plan" else None,
             "run_id": getattr(client, "run_id", None), "lease_until": deadline.isoformat()}
    set_status(client, requested, "待验证" if action in {"review", "delivery"} else "开发中")
    client.append(event)
    client.append(event, requested)
    return {"schema_version": "robtaxi-codex-task-v1", "main_sha": state["main_sha"],
            "task": task, "claim": event}


def _deduplicated_append(client: GitHub, event: dict, history: list[dict], issue: int) -> dict:
    if any(row.get("key") == event["key"] and row.get("event") == event["event"] for row in history):
        return {"skipped": "相同检查点已存在", **event}
    client.append(event, issue)
    return event


def release_task(client: GitHub, policy: dict, state: dict, issue: int, stage: str) -> dict:
    """阶段证据先持久化，再释放具体认领；释放不表示整个任务完成。"""
    require(budgeted(policy), "v2仍沿用原单任务规则")
    client.budget.check()
    events = client.events()
    claims = [e for e in events if e.get("event") == "task_claimed" and e.get("issue") == issue
              and e.get("run_id") == client.run_id]
    require(bool(claims), "本轮没有该任务认领记录")
    claimed = claims[-1]; key = f"release:{claimed['key']}"
    previous = next((e for e in events if e.get("key") == key), None)
    if previous:
        require(previous.get("stage") == stage, "同一认领释放阶段冲突")
        _deduplicated_append(client, previous, client.events(issue), issue)
        return previous
    lease = active_claim(events, issue, datetime.now(timezone.utc))
    require(bool(lease) and lease["key"] == claimed["key"], "租约已释放或过期")
    require_owner(client, lease)
    task = next(t for t in state["tasks"] if t["number"] == issue)
    history = client.events(issue)
    if stage == "planned":
        proofs = [e for e in history if e.get("event") == "contract" and e.get("claim_key") == claimed["key"]]
    elif stage == "waiting_production":
        proofs = [e for e in history if e.get("event") == "delivery" and e.get("at", "") >= claimed["at"]]
    else:
        expected = "pr_created" if stage == "waiting_review" else stage
        proofs = [e for e in history if e.get("event") in {"checkpoint", "run_failed"}
                  and e.get("stage") == expected and e.get("claim_key") == claimed["key"]]
    require(bool(proofs), "缺少与本次认领绑定的正式阶段证据")
    event = {"event": "task_released", "key": key, "claim_key": claimed["key"], "issue": issue,
             "run_id": client.run_id, "stage": stage, "evidence_key": proofs[-1]["key"],
             "evidence_digest": task.get("health_evidence_digest") or task.get("contract_digest"),
             "at": datetime.now(timezone.utc).isoformat()}
    _deduplicated_append(client, event, events, None)
    _deduplicated_append(client, event, history, issue)
    return event


def checkpoint_contract(client: GitHub, state: dict, task: dict, contract: dict) -> dict:
    issue = task["number"]
    assert_trusted_checkout(client)
    validate_contract(contract, issue)
    require(contract["base_sha"] == state["main_sha"], "执行说明代码基线错误")
    previous = task.get("contract") or {}
    require(set(task["blockers"]).issubset(contract["dependencies"]), "执行说明漏掉实际依赖")
    now = datetime.now(timezone.utc)
    lease = active_claim(state.get("events", []) if budgeted(getattr(client, "policy", {})) else task.get("events", []), issue, now)
    require(bool(lease), "规划必须先领取当天唯一任务")
    require_owner(client, lease)
    if previous and digest(previous) == digest(contract):
        client.label(issue, "human" if contract["reserved_decisions"] else "ready")
        return {"event": "contract", "skipped": "执行说明已保存，恢复标签", "contract": contract}
    require(contract["version"] == previous.get("version", 0) + 1, "执行说明版本必须递增一次")
    event = {"event": "contract", "producer": "codex-scheduled", "contract": contract,
             "claim_key": lease["key"], "run_id": getattr(client, "run_id", None),
             "key": f"contract:{issue}:{digest(contract)}", "at": now.isoformat()}
    result = _deduplicated_append(client, event, task.get("events", []), issue)
    client.label(issue, "human" if contract["reserved_decisions"] else "ready")
    return result


def checkpoint_review(client: GitHub, state: dict, task: dict, claim_event: dict, review: dict) -> dict:
    issue = task["number"]
    require(isinstance(review, dict) and review.get("verdict") in {"approve", "changes_requested"}
            and bool(review.get("evidence")), "复核缺少结构化证据")
    pr = task["open_pr"]
    expected = {"head_sha": pr["headRefOid"], "base_sha": state["main_sha"], "contract_digest": task["contract_digest"]}
    require(all(review.get(k) == v for k, v in expected.items()), "本地复核证据已过期或未绑定版本；必须重新复核")
    existing = next((e for e in reversed(task.get("events", [])) if e.get("event") == "review"
                     and e.get("producer") == "codex-scheduled" and e.get("verdict") == review["verdict"]
                     and all(e.get(k) == v for k, v in expected.items())), None)
    if existing:
        client.label(issue, "review" if review["verdict"] == "approve" else "planning")
        return {**existing, "skipped": "复核已正式保存，恢复标签"}
    require(task.get("review_needed"), "当前任务不是待复核PR")
    review = {**review, "producer": "codex-scheduled", "head_sha": pr["headRefOid"],
              "base_sha": state["main_sha"], "contract_digest": task["contract_digest"]}
    if review["verdict"] == "approve":
        verify_review(review, task["contract"], pr["headRefOid"], state["main_sha"])
    now = datetime.now(timezone.utc)
    require(periods(now)[0] > periods(timestamp(pr["createdAt"]))[0], "高影响PR必须由下一天的独立运行复核")
    authors = {e.get("run_id") for e in task.get("events", []) if e.get("stage") == "tests_passed"
               and e.get("head_sha") == pr["headRefOid"] and e.get("run_id")}
    require(not getattr(client, "run_id", None) or client.run_id not in authors, "执行运行不能复核自己的提交")
    event = {**review, "event": "review", "run_id": getattr(client, "run_id", None),
             "key": f"review:{issue}:{digest(expected)}:{review['verdict']}", "at": now.isoformat()}
    result = _deduplicated_append(client, event, task.get("events", []), issue)
    client.label(issue, "review" if review["verdict"] == "approve" else "planning")
    return result


def checkpoint(client: GitHub, policy: dict, state: dict, issue: int, payload: dict) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "当前模式只允许只读模拟")
    require(policy["mode"] != "pilot" or issue == policy["pilot_issue"], "试点阶段不能写入其他任务")
    task = next((row for row in state["tasks"] if row["number"] == issue), None)
    require(bool(task) and task["state"] == "OPEN", "检查点必须关联开放的正式任务")
    kind = payload.get("event")
    if kind == "contract":
        return checkpoint_contract(client, state, task, payload.get("contract"))
    now = datetime.now(timezone.utc)
    claim_event = active_claim(state.get("events", []) if budgeted(policy) else task.get("events", []), issue, now)
    require(bool(claim_event), "没有当前有效的90分钟任务租约")
    require_owner(client, claim_event)
    require(claim_event.get("contract_digest") == task.get("contract_digest") or
            any(e.get("event") == "contract" and e.get("claim_key") == claim_event.get("key")
                and digest(e["contract"]) == task.get("contract_digest") for e in task.get("events", [])), "任务租约对应另一版执行说明")
    if kind == "review":
        return checkpoint_review(client, state, task, claim_event, payload.get("review"))
    require(kind in {"checkpoint", "run_failed"}, "不允许的检查点事件")
    stage = payload.get("stage")
    require(stage in CHECKPOINT_STAGES, "检查点阶段无效")
    summary = payload.get("summary")
    require(isinstance(summary, str) and 0 < len(summary.strip()) <= 500, "检查点摘要缺失或过长")
    head_sha = payload.get("head_sha")
    require(head_sha is None or bool(re.fullmatch(r"[0-9a-f]{40}", str(head_sha))), "检查点提交版本无效")
    changed = payload.get("changed") if kind == "run_failed" else None
    require(kind != "run_failed" or type(changed) is bool, "失败检查点必须声明是否有实际代码修改")
    event = {"event": kind, "key": f"{kind}:{claim_event['key']}:{stage}:{head_sha or 'none'}",
             "run_id": getattr(client, "run_id", None),
             "issue": issue, "stage": stage, "summary": summary.strip(), "head_sha": head_sha,
             "claim_key": claim_event["key"], "contract_digest": task["contract_digest"],
             "changed": changed, "at": now.isoformat()}
    return _deduplicated_append(client, event, task.get("events", []), issue)


def cloud_heartbeat(client: GitHub, policy: dict, output: Path) -> dict:
    if not policy.get("heartbeat_enabled"):
        return {"skipped": "研发调度尚未启用，不制造离线告警"}
    event = heartbeat_transition(client.events(), datetime.now(timezone.utc), policy["heartbeat_hours"])
    if event is None:
        return {"skipped": "状态无变化"}
    # 状态变化是通知来源；发送器独立按逐渠道回执恢复，不能用此事件代表送达。
    client.append(event)
    message = "研发巡检超过 36 小时未成功；云端新闻生产不受影响。请检查本机网络、Shadowrocket、Codex 登录及桌面应用。" if event["status"] == "stale" else "研发巡检已恢复，继续从 GitHub 正式状态接续。"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(message, encoding="utf-8")
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "claim", "checkpoint", "release", "heartbeat", "cloud-heartbeat", "verify-production", "finish"])
    parser.add_argument("--stage", choices=["planned", "tests_passed", "pr_created", "waiting_ci", "waiting_review", "waiting_production", "blocked"])
    parser.add_argument("--out", default=None)
    parser.add_argument("--outcome", choices=["progress", "waiting_production", "needs_approval", "no_eligible_task", "failed"], default="no_eligible_task")
    parser.add_argument("--issue", type=int)
    parser.add_argument("--run-id", default=os.environ.get("CODEX_THREAD_ID", ""), help="本次Codex任务ID，所有阶段复用")
    parser.add_argument("--start-run", action="store_true", help="运行开始时登记90分钟截止时间；不领取研发任务")
    parser.add_argument("--apply", action="store_true", help="生产验收写回正式状态；省略则只读")
    parser.add_argument("--event", help="contract/review/checkpoint/run_failed 的结构化 JSON 文件")
    parser.add_argument("--health-sync", help="本次 health_loop_sync apply 的完整回执，只有成功后才写成功心跳")
    args = parser.parse_args()
    try:
        policy = load_policy()
        client = GitHub(policy)
        with repository_lock(policy["repository"]):
            output = (isolated_output(ROOT, args.run_id, args.out, f"{args.action}.json") if args.run_id and args.action != "cloud-heartbeat"
                      else Path(args.out or ".local/robtaxi-development/snapshot.json"))
            if args.start_run:
                require(args.action == "inspect" and policy["mode"] in {"pilot", "active"}, "只能在正式运行起点登记预算")
                result = initialize_run(client, policy, args.run_id, start=True)
                print(json.dumps(result, ensure_ascii=False))
                return 0
            if args.run_id and args.action != "cloud-heartbeat":
                initialize_run(client, policy, args.run_id)
            if args.action in {"claim", "checkpoint", "release", "heartbeat", "finish"} or (args.action == "verify-production" and args.apply):
                require(client.budget is not None, "写操作必须关联已登记的run-id")
                assert_trusted_checkout(client)
            if args.action == "finish":
                result = finish_run(client, args.outcome)
            elif args.action == "verify-production":
                from app.development_production import reconcile_production
                result = reconcile_production(client, policy, args.issue, apply=args.apply)
            elif args.action == "cloud-heartbeat":
                result = cloud_heartbeat(client, policy, output)
            elif args.action == "heartbeat":
                assert_trusted_checkout(client)
                require(bool(args.health_sync), "缺少健康对账回执")
                health = json.loads(Path(args.health_sync).read_text())
                validate_health_receipt(health, client.run_id)
                # 再次访问正式状态验证认证；回执仅表明巡检完成，不是生产已恢复。
                client.task(policy["control_issue"])
                result = {"event": "heartbeat", "key": f"heartbeat:{digest(health)}", "success": True, "at": datetime.now(timezone.utc).isoformat(), "health_sync_digest": digest(health)}
                client.append(result)
            else:
                # 首次claim必须完整选取；只读单任务inspect/检查点不重复拉整盘。
                state = snapshot(client, policy, args.issue if args.action in {"inspect", "checkpoint", "release"} else None)
                if args.action == "inspect":
                    result = state
                elif args.action == "claim":
                    result = claim(client, policy, state, issue=args.issue)
                elif args.action == "release":
                    require(bool(args.issue) and bool(args.stage), "release需要--issue与--stage")
                    result = release_task(client, policy, state, args.issue, args.stage)
                else:
                    require(bool(args.issue) and bool(args.event), "checkpoint 需要 --issue 和 --event")
                    result = checkpoint(client, policy, state, args.issue, json.loads(Path(args.event).read_text()))
            if client.failures and client.run_id:
                event = {"event": "diagnostic_summary", "key": f"diagnostic:{client.run_id}:{digest(client.failures)}",
                         "run_id": client.run_id, "at": datetime.now(timezone.utc).isoformat(),
                         "failures": client.failures}
                client.append(event)
            if args.action != "cloud-heartbeat":
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps({"action": args.action, "mode": policy["mode"], "ok": True, "skipped": result.get("skipped")}, ensure_ascii=False))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"[development-cycle] STOP: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
