"""WorkBuddy 的确定性交接入口。默认 shadow；不代替宿主提供执行终止能力。"""
from __future__ import annotations

import argparse
import json
import os
import sys
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path

from app.development_policy import (DevelopmentError, check_fresh, digest, heartbeat_transition,
                                    periods, require, reserve, select_tasks, validate_contract, validate_policy)
from app.development_runtime import GitHub, codex_batch, repository_lock, run
from scripts.validate_project_task import primary_task_reference_from_pr_body

ROOT = Path(__file__).resolve().parent.parent
POLICY = ROOT / ".github/robtaxi-autonomy.json"


def load_policy() -> dict:
    policy = json.loads(POLICY.read_text())
    legacy_path = ROOT / ".github/robtaxi-health-autofix.json"
    validate_policy(policy, json.loads(legacy_path.read_text()) if legacy_path.exists() else {})
    return policy


def response_schema() -> dict:
    # 大段执行说明作为 JSON 对象字符串传递，再经独立语义校验；并非直接执行模型返回的命令。
    item = {"type": "object", "additionalProperties": False,
            "properties": {"issue": {"type": "integer"}, "decision": {"type": "string", "enum": ["plan", "review", "needs_human"]},
                           "reason": {"type": "string"}, "contract_json": {"type": "string"}, "review_json": {"type": "string"}},
            "required": ["issue", "decision", "reason", "contract_json", "review_json"]}
    return {"type": "object", "additionalProperties": False, "properties": {"results": {"type": "array", "items": item}}, "required": ["results"]}


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
        if not pr["headRefName"].startswith("workbuddy/development-"):
            continue
        try:
            issue, _ = primary_task_reference_from_pr_body(pr["body"])
        except Exception:
            continue
        merged_by_issue.setdefault(issue, []).append(pr)
    for task in tasks:
        history = records.get(task["number"], [])
        plans = [e for e in history if e.get("event") == "contract" and e.get("producer") == "codex-exec"]
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
            task["repair_attempts"] = sum(e.get("event") == "attempt" and e.get("changed") is True and e.get("passed") is False and e.get("contract_digest") == digest(contract) for e in history)
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
            reviews = [e for e in history if e.get("event") == "review" and e.get("head_sha") == prs[0]["headRefOid"] and e.get("base_sha") == main]
            task["review_needed"] = bool(task.get("contract")) and not reviews
        elif merged_by_issue.get(task["number"]):
            # 即使进程在合并成功、落回执之前崩溃，也不重新创建 PR。
            task["awaiting_production"] = sorted(merged_by_issue[task["number"]], key=lambda p: p["mergedAt"])[-1]
    return {"schema_version": "robtaxi-development-snapshot-v1", "main_sha": main,
            "events": events, "tasks": tasks, **select_tasks(tasks, policy)}


def assert_trusted_checkout(client: GitHub) -> str:
    head = run(["git", "rev-parse", "HEAD"], cwd=ROOT).strip()
    require(head == client.main_sha(), "调度器必须从最新已合并主分支运行；不能从执行分支启用新授权")
    require(not run(["git", "status", "--porcelain", "--untracked-files=no"], cwd=ROOT).strip(), "调度器主分支工作区存在修改")
    return head


def plan(client: GitHub, policy: dict, state: dict, binary: str, *, issue: int | None = None) -> dict:
    batch = state["batch"]
    if issue is not None:
        batch = [task for task in batch if task["number"] == issue]
    if not batch:
        return {"skipped": "没有待规划/复核事项"}
    require(policy["mode"] != "off", "研发自动化已关闭")
    # shadow 仍严格预留模型日额度；调用失败不返还，避免后台无限重试。
    now = datetime.now(timezone.utc)
    day, _ = periods(now)
    key = f"codex:{day}"
    receipt = reserve(state["events"], policy, now, "codex", key)
    client.append(receipt)
    packet = {"schema_version": "robtaxi-planning-batch-v1", "main_sha": state["main_sha"], "tasks": batch}
    try:
        for task in batch:
            if task.get("open_pr"):
                head = task["open_pr"]["headRefOid"]
                run(["git", "fetch", "--no-tags", "origin", head], cwd=ROOT)
                diff = client.gh("pr", "diff", str(task["open_pr"]["number"]), "--repo", policy["repository"])
                require(len(diff) <= 150000, "PR 超过单批可复核范围，应拆分方案")
                task["review_evidence"] = {"diff": diff, "checks": client.api(f"repos/{policy['repository']}/commits/{head}/check-runs?per_page=100")}
        response, evidence = codex_batch(binary, ROOT, packet, response_schema(), policy["codex_timeout_seconds"])
        results = response.get("results")
        require(isinstance(results, list) and len(results) == len(batch), "规划返回事项数量不匹配")
        expected = {t["number"]: t for t in batch}
        require({r["issue"] for r in results} == set(expected) and len({r["issue"] for r in results}) == len(results), "规划结果 Issue 不匹配或重复")
        validated = []
        for result in results:
            task = expected[result["issue"]]
            decision = result["decision"]
            if decision == "plan":
                require(not task.get("open_pr"), "有 PR 的任务应复核而非另建执行方案")
                contract = json.loads(result["contract_json"])
                validate_contract(contract, task["number"])
                require(contract["base_sha"] == state["main_sha"], "规划代码基线错误")
                require(contract["version"] == task.get("contract", {}).get("version", 0) + 1, "方案版本必须递增一次")
                require(set(task["blockers"]).issubset(contract["dependencies"]), "规划漏掉真实依赖")
                event = {"event": "contract", "contract": contract, **evidence}
                label = "human" if contract["reserved_decisions"] else "ready"
            elif decision == "review":
                require(bool(task.get("open_pr")), "复核任务没有开放 PR")
                review = json.loads(result["review_json"])
                require(review.get("head_sha") == task["open_pr"]["headRefOid"] and review.get("base_sha") == state["main_sha"], "复核提交绑定错误")
                require(review.get("contract_digest") == digest(task["contract"]), "复核方案摘要错误")
                require(review.get("verdict") in {"approve", "changes_requested"} and bool(review.get("evidence")), "复核结果无效")
                event = {**review, **evidence, "event": "review"}
                label = "review" if review["verdict"] == "approve" else "planning"
            else:
                require(decision == "needs_human" and bool(result["reason"]), "无法解释的规划结果")
                event = {"event": "needs_human", "reason": result["reason"], **evidence}
                label = "human"
            validated.append((task["number"], event, label))
        # 整批先验证再写；任何部分写失败，次日从已写 Issue 恢复，不重复创建工程任务。
        for number, event, label in validated:
            client.append(event, number)
            client.label(number, label)
        completion = {"event": "codex_complete", "key": key, "issues": list(expected), **evidence}
        client.append(completion)
        return completion
    except Exception:
        client.append({"event": "codex_failed", "key": key, "at": datetime.now(timezone.utc).isoformat(), "retry": "next_day_only"})
        raise


def execute(client: GitHub, policy: dict, state: dict) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "当前仅模拟，未开放 WorkBuddy 代码执行")
    assert_trusted_checkout(client)
    require(not state["conflicting_running"], "多个执行中任务需要先对账")
    task = state.get("task")
    if not task:
        return {"skipped": "没有可执行任务"}
    require(policy["mode"] != "pilot" or task["number"] == policy["pilot_issue"], "试点阶段不能执行其他任务")
    require(not any(e.get("event") == "release_freeze" for e in state["events"] if not e.get("resolved")), "存在发布冻结；需完成事故诊断")
    contract = task["contract"]
    require(not contract["reserved_decisions"], "任务含人工保留事项")
    require(task["assignees"] > 0 and not task["blockers"], "负责人或依赖不满足开工条件")
    # WorkBuddy 子进程读取主任务并按手册建立独立 worktree，不能在调度器目录编辑。
    now = datetime.now(timezone.utc)
    day, _ = periods(now)
    key = f"execute:{day}:{task['number']}"
    receipt = reserve(state["events"], policy, now, "execute", key)
    client.append({**receipt, "issue": task["number"], "contract_digest": digest(contract)})
    packet = {"issue": task["number"], "contract": contract, "main_sha": state["main_sha"], "key": key,
              "manual": ".github/codex/robtaxi-workbuddy-execution.md"}
    try:
        run(policy["worker_argv"], cwd=ROOT, stdin=json.dumps(packet, ensure_ascii=False), timeout=policy["execution_timeout_seconds"])
        result = {"event": "worker_exit", "key": key, "success": True}
    except DevelopmentError:
        result = {"event": "worker_exit", "key": key, "success": False, "resume": "github_checkpoint_next_day"}
    client.append({**result, "at": datetime.now(timezone.utc).isoformat()})
    return result


def cloud_heartbeat(client: GitHub, policy: dict, output: Path) -> dict:
    if not policy.get("heartbeat_enabled"):
        return {"skipped": "研发调度尚未启用，不制造离线告警"}
    event = heartbeat_transition(client.events(), datetime.now(timezone.utc), policy["heartbeat_hours"])
    if event is None:
        return {"skipped": "状态无变化"}
    # 先写去重事件，再通知；通知失败会在 Actions 留痕，不重复轰炸。
    client.append(event)
    message = "研发巡检超过 36 小时未成功；云端新闻生产不受影响。请检查本机网络、登录及 WorkBuddy。" if event["status"] == "stale" else "研发巡检已恢复，继续从 GitHub 正式状态接续。"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(message, encoding="utf-8")
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["inspect", "plan", "execute", "heartbeat", "cloud-heartbeat"])
    parser.add_argument("--out", default=".workbuddy/development/snapshot.json")
    parser.add_argument("--codex", default="codex")
    parser.add_argument("--issue", type=int)
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
                result = state if args.action == "inspect" else plan(client, policy, state, args.codex, issue=args.issue) if args.action == "plan" else execute(client, policy, state)
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
