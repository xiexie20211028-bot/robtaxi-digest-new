"""Codex 定时研发的确定性状态入口；模型负责开发，脚本负责授权与证据。"""
from __future__ import annotations

import argparse
import json
import re
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.development_policy import (DevelopmentError, check_fresh, digest, heartbeat_transition, matches,
                                    periods, require, reserve, select_tasks, timestamp, validate_contract,
                                    validate_policy, verify_review)
from app.development_runtime import GitHub, repository_lock, run
from app.health_loop_sync import GhMetadataClient
from scripts.validate_project_task import primary_task_reference_from_pr_body

ROOT = Path(__file__).resolve().parent.parent
POLICY = ROOT / ".github/robtaxi-autonomy.json"
CHECKPOINT_STAGES = {"planned", "code_changed", "tests_passed", "pr_created", "waiting_ci", "blocked"}


def load_policy() -> dict:
    policy = json.loads(POLICY.read_text())
    legacy_path = ROOT / ".github/robtaxi-health-autofix.json"
    validate_policy(policy, json.loads(legacy_path.read_text()) if legacy_path.exists() else {})
    return policy


def snapshot(client: GitHub, policy: dict) -> dict:
    events, tasks, pulls, main = client.events(), client.tasks(), client.pulls(), client.main_sha()
    tasks = [task for task in tasks if task["number"] != policy["control_issue"]]
    # 一次拉取开放任务的正式交接，完整分页；本地缓存丢失不会重新消费调用/合并配额。
    eligible = [task for task in tasks if task["state"] == "OPEN" and task["type"] != "Epic"]
    with ThreadPoolExecutor(max_workers=4) as pool:
        records = dict(zip([t["number"] for t in eligible], pool.map(lambda task: client.events(task["number"]), eligible)))
    by_issue, merged_by_issue = {}, {}
    for pr in pulls:
        try:
            issue, _ = primary_task_reference_from_pr_body(pr["body"])
        except Exception:
            continue
        by_issue.setdefault(issue, []).append(pr)
    for pr in client.merged_pulls():
        if not pr["headRefName"].startswith(policy["branch_prefix"]):
            continue
        try:
            issue, _ = primary_task_reference_from_pr_body(pr["body"])
        except Exception:
            continue
        merged_by_issue.setdefault(issue, []).append(pr)
    for task in tasks:
        history = records.get(task["number"], [])
        plans = [e for e in history if e.get("event") == "contract" and
                 e.get("producer") in {"codex-exec", "codex-scheduled"}]
        if plans:
            contract = plans[-1]["contract"]
            validate_contract(contract, task["number"])
            task["contract"] = contract
            task["contract_digest"] = digest(contract)
            task["contract_url"] = plans[-1].get("_comment_url")
            try:
                check_fresh(contract, client.changed_since(contract["base_sha"], main))
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
                       and e.get("producer") == "codex-scheduled" and e.get("verdict") == "approve"
                       and e.get("head_sha") == prs[0]["headRefOid"] and e.get("base_sha") == main]
            contract = task.get("contract") or {}
            planned_high = (contract.get("risk") == "High" or task.get("priority") == "P0"
                            or task.get("route") == "共同"
                            or any(matches(path, policy["high_impact_paths"])
                                   for path in contract.get("allowed_paths", [])))
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
        task["events"] = history
    return {"schema_version": "robtaxi-development-snapshot-v2", "main_sha": main,
            "events": events, "tasks": tasks, **select_tasks(tasks, policy)}


def assert_trusted_checkout(client: GitHub) -> str:
    head = run(["git", "rev-parse", "HEAD"], cwd=ROOT).strip()
    require(head == client.main_sha(), "调度器必须从最新已合并主分支运行；不能从执行分支启用新授权")
    require(not run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT).strip(), "调度器主分支工作区存在修改")
    return head


def set_status(policy: dict, issue: int, status: str) -> None:
    metadata = GhMetadataClient(repository=policy["repository"], owner=policy["project_owner"], project=policy["project_number"])
    metadata.preflight()
    item = metadata._project_item(issue)
    require(bool(item), "正式任务不在总盘")
    metadata._set_field(item["id"], "Status", status)


def active_claim(events: list[dict], issue: int, now: datetime) -> dict | None:
    claims = [event for event in events if event.get("event") == "task_claimed" and event.get("issue") == issue]
    if not claims:
        return None
    claim_event = claims[-1]
    return claim_event if timestamp(claim_event["lease_until"]) >= now else None


def claim(client: GitHub, policy: dict, state: dict, *, issue: int | None = None) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "当前模式禁止领取代码任务")
    assert_trusted_checkout(client)
    require(not state["conflicting_running"], "多个开发中任务需要先对账")
    requested = issue or (state.get("next_action") or {}).get("issue")
    task = next((row for row in state["tasks"] if row["number"] == requested), None)
    require(bool(task), "没有可领取的正式任务")
    require(policy["mode"] != "pilot" or requested == policy["pilot_issue"], "试点阶段不能领取其他任务")
    action = "review" if task.get("review_needed") else "delivery" if task.get("open_pr") or task.get("delivery_recovery") else "execute"
    require((state.get("next_action") or {}).get("issue") == requested, "任务不是当前确定性队列首项")
    frozen = any(e.get("event") == "release_freeze" and not e.get("resolved") for e in state["events"])
    require(not frozen or action == "review", "存在发布冻结；只能继续复核，不能开发或交付")
    contract = task.get("contract")
    require(bool(contract), "任务尚无有效执行说明；先记录 contract")
    validate_contract(contract, requested)
    require(not task.get("stale"), "执行说明涉及的代码已变化，需要重新规划")
    require(not contract["reserved_decisions"], "任务含人类保留事项")
    require(task["assignees"] > 0 and not task["blockers"], "负责人或依赖不满足领取条件")
    now = datetime.now(timezone.utc)
    day, _ = periods(now)
    key = f"task:{day}:{requested}:{action}"
    event = {**reserve(state["events"], policy, now, "task", key), "event": "task_claimed",
             "issue": requested, "action": action, "contract_digest": digest(contract),
             "lease_until": (now + timedelta(seconds=policy["task_lease_seconds"])).isoformat()}
    set_status(policy, requested, "开发中" if action == "execute" else "待验证")
    client.append(event)
    client.append(event, requested)
    return {"schema_version": "robtaxi-codex-task-v1", "main_sha": state["main_sha"],
            "task": task, "claim": event}


def _deduplicated_append(client: GitHub, event: dict, history: list[dict], issue: int) -> dict:
    if any(row.get("key") == event["key"] and row.get("event") == event["event"] for row in history):
        return {"skipped": "相同检查点已存在", **event}
    client.append(event, issue)
    return event


def checkpoint_contract(client: GitHub, state: dict, task: dict, contract: dict) -> dict:
    issue = task["number"]
    assert_trusted_checkout(client)
    validate_contract(contract, issue)
    require(contract["base_sha"] == state["main_sha"], "执行说明代码基线错误")
    previous = task.get("contract") or {}
    require(contract["version"] == previous.get("version", 0) + 1, "执行说明版本必须递增一次")
    require(set(task["blockers"]).issubset(contract["dependencies"]), "执行说明漏掉实际依赖")
    now = datetime.now(timezone.utc)
    event = {"event": "contract", "producer": "codex-scheduled", "contract": contract,
             "key": f"contract:{issue}:{digest(contract)}", "at": now.isoformat()}
    result = _deduplicated_append(client, event, task.get("events", []), issue)
    client.label(issue, "human" if contract["reserved_decisions"] else "ready")
    return result


def checkpoint_review(client: GitHub, state: dict, task: dict, claim_event: dict, review: dict) -> dict:
    issue = task["number"]
    require(claim_event["action"] == "review" and task.get("review_needed"), "当前任务不是待复核PR")
    require(isinstance(review, dict) and review.get("verdict") in {"approve", "changes_requested"}
            and bool(review.get("evidence")), "复核缺少结构化证据")
    pr = task["open_pr"]
    review = {**review, "producer": "codex-scheduled", "head_sha": pr["headRefOid"],
              "base_sha": state["main_sha"], "contract_digest": task["contract_digest"]}
    if review["verdict"] == "approve":
        verify_review(review, task["contract"], pr["headRefOid"], state["main_sha"])
    now = datetime.now(timezone.utc)
    require(periods(now)[0] > periods(timestamp(pr["createdAt"]))[0], "高影响PR必须由下一天的独立运行复核")
    event = {**review, "event": "review", "key": f"review:{issue}:{pr['headRefOid']}", "at": now.isoformat()}
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
    claim_event = active_claim(task.get("events", []), issue, now)
    require(bool(claim_event), "没有当前有效的90分钟任务租约")
    require(claim_event["contract_digest"] == task.get("contract_digest"), "任务租约对应另一版执行说明")
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
    # 先写去重事件，再通知；通知失败会在 Actions 留痕，不重复轰炸。
    client.append(event)
    message = "研发巡检超过 36 小时未成功；云端新闻生产不受影响。请检查本机网络、Shadowrocket、Codex 登录及桌面应用。" if event["status"] == "stale" else "研发巡检已恢复，继续从 GitHub 正式状态接续。"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(message, encoding="utf-8")
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "claim", "checkpoint", "heartbeat", "cloud-heartbeat"])
    parser.add_argument("--out", default=".local/robtaxi-development/snapshot.json")
    parser.add_argument("--issue", type=int)
    parser.add_argument("--event", help="contract/review/checkpoint/run_failed 的结构化 JSON 文件")
    parser.add_argument("--health-sync", help="本次 health_loop_sync apply 的完整回执，只有成功后才写成功心跳")
    args = parser.parse_args()
    try:
        policy = load_policy()
        client = GitHub(policy)
        with repository_lock(policy["repository"]):
            if args.action == "cloud-heartbeat":
                result = cloud_heartbeat(client, policy, Path(args.out))
            elif args.action == "heartbeat":
                assert_trusted_checkout(client)
                require(bool(args.health_sync), "缺少健康对账回执")
                health = json.loads(Path(args.health_sync).read_text())
                require(health.get("complete") is True and health.get("mode") == "apply" and health.get("official_state", {}).get("complete") is True, "健康对账未完成，不得写成功心跳")
                # 再次访问正式状态验证认证；回执仅表明巡检完成，不是生产已恢复。
                client.tasks()
                result = {"event": "heartbeat", "success": True, "at": datetime.now(timezone.utc).isoformat(), "health_sync_digest": digest(health)}
                client.append(result)
            else:
                state = snapshot(client, policy)
                if args.action == "inspect":
                    result = state
                elif args.action == "claim":
                    result = claim(client, policy, state, issue=args.issue)
                else:
                    require(bool(args.issue) and bool(args.event), "checkpoint 需要 --issue 和 --event")
                    result = checkpoint(client, policy, state, args.issue, json.loads(Path(args.event).read_text()))
            if args.action != "cloud-heartbeat":
                output = Path(args.out)
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
            print(json.dumps({"action": args.action, "mode": policy["mode"], "ok": True, "skipped": result.get("skipped")}, ensure_ascii=False))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"[development-cycle] STOP: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
