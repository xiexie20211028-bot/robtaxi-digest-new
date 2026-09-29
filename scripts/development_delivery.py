#!/usr/bin/env python3
"""自动交付最后一步：从最新可信主分支重查门禁、预留每日配额并锁定提交合并。"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.development_cycle import active_claim, assert_trusted_checkout, initialize_run, load_policy, require_owner, set_status
from app.development_policy import DevelopmentError, digest, periods, require, reserve, validate_contract
from app.development_runtime import GitHub, repository_lock, run
from app.development_recovery import RequestFailure, latest_checks
from scripts.validate_development_delivery import github_gate
from scripts.validate_project_task import primary_task_reference_from_pr_body


def _append_once(client: GitHub, event: dict, events: list[dict], issue: int | None = None) -> None:
    if not any(row.get("event") == event["event"] and row.get("key") == event["key"] for row in events):
        client.append(event, issue)


def record_delivery(client: GitHub, policy: dict, merged: dict, issue: int, production: dict,
                    contract_digest: str, key: str, control_events: list[dict], issue_events: list[dict]) -> dict:
    receipt = {"event": "delivery", "issue": issue, "pr": merged["number"], "key": key,
               "day": periods(datetime.now(timezone.utc))[0], "head_sha": merged["head"]["sha"],
               "merge_sha": merged["merge_commit_sha"], "at": merged["merged_at"],
               "production": production, "contract_digest": contract_digest}
    _append_once(client, receipt, issue_events, issue)
    _append_once(client, receipt, control_events)
    if production["kind"] == "none":
        verified = {"event": "production_verified", "issue": issue,
                    "key": f"production:{merged['merge_commit_sha']}", "merge_sha": merged["merge_commit_sha"],
                    "result": "no_observation_required", "at": merged["merged_at"],
                    "contract_digest": contract_digest}
        _append_once(client, verified, issue_events, issue)
        _append_once(client, verified, control_events)
        client.gh("issue", "close", str(issue), "--repo", policy["repository"], "--reason", "completed")
        set_status(policy, issue, "已完成")
    else:
        set_status(policy, issue, "观察中")
    return receipt


def recover_merged(client: GitHub, policy: dict, pr: dict, current_main: str) -> dict:
    require(pr["head"]["ref"].startswith(policy["branch_prefix"]), "不是 Codex 自动研发分支")
    require(pr.get("base", {}).get("ref") == "main", "恢复交付只接受主分支 PR")
    require(pr.get("head", {}).get("repo", {}).get("full_name") == policy["repository"],
            "恢复交付不接受 fork")
    issue, _ = primary_task_reference_from_pr_body(pr.get("body", ""))
    require(policy["mode"] != "pilot" or issue == policy["pilot_issue"], "不在唯一试点范围")
    control_events = client.events()
    claim_event = active_claim(control_events, issue, datetime.now(timezone.utc))
    require(bool(claim_event), "恢复交付需要当前有效租约")
    require_owner(client, claim_event)
    pending = next((event for event in reversed(control_events)
                    if event.get("event") == "reserve" and event.get("kind") == "merge"
                    and event.get("pr") == pr["number"] and event.get("head_sha") == pr["head"]["sha"]), None)
    require(bool(pending), "没有可信合并预留，不能把手工合并登记为自动交付")
    issue_events = client.events(issue)
    contracts = [event for event in issue_events if event.get("event") == "contract"
                 and event.get("producer") in {"codex-exec", "codex-scheduled"}]
    require(bool(contracts), "已合并任务缺少执行说明")
    contract = contracts[-1]["contract"]
    validate_contract(contract, issue)
    comparison = client.api(f"repos/{policy['repository']}/compare/{pr['merge_commit_sha']}...{current_main}")
    require(comparison.get("status") in {"ahead", "identical"}, "已合并版本不在当前主分支历史中")
    return record_delivery(client, policy, pr, issue, contract["production"], digest(contract),
                           pending["key"], control_events, issue_events)


def delivery_lease(client: GitHub, events: list[dict], issue: int) -> dict:
    lease = active_claim(events, issue, datetime.now(timezone.utc))
    require(bool(lease), "没有当前有效的90分钟任务租约")
    require_owner(client, lease)
    require(not any(e.get("event") == "release_freeze" and not e.get("resolved") for e in events), "生产严重异常尚未解除，发布冻结")
    return lease


def wait_for_ci(client: GitHub, policy: dict, pr: dict, issue: int, lease: dict, *, ready_at: str | None = None) -> bool:
    deadline = min(time.monotonic() + policy.get("ci_wait_seconds", 900),
                   time.monotonic() + max(0, (datetime.fromisoformat(lease["lease_until"]) - datetime.now(timezone.utc)).total_seconds()))
    while True:
        if getattr(client, "budget", None):
            client.budget.check()
        current = client.api(f"repos/{policy['repository']}/pulls/{pr['number']}")
        require(current["head"]["sha"] == pr["head"]["sha"], "等待CI期间PR提交变化，旧复核失效")
        checks = client.api(f"repos/{policy['repository']}/commits/{pr['head']['sha']}/check-runs?per_page=100")
        require(checks.get("total_count", 0) <= 100, "检查列表不完整")
        rows = [c for c in checks.get("check_runs", []) if c.get("head_sha") == pr["head"]["sha"]]
        if ready_at:
            rows = [c for c in rows if c.get("name") != "project-task-gate" or (c.get("started_at") or "") >= ready_at]
        status = latest_checks(rows, policy["required_checks"])
        require(status != "failed", "必要检查最新运行失败；保留PR并进入修复，不用旧成功覆盖")
        if status == "success":
            return True
        remaining = deadline - time.monotonic()
        if remaining <= policy.get("ci_poll_seconds", 30):
            event = {"event": "checkpoint", "stage": "waiting_ci", "issue": issue,
                     "key": f"waiting-ci:{lease['key']}:{pr['head']['sha']}", "head_sha": pr["head"]["sha"],
                     "at": datetime.now(timezone.utc).isoformat(), "summary": "必要检查尚未完成；下次从同一PR继续"}
            _append_once(client, event, client.events(issue), issue)
            return False
        time.sleep(min(30, policy.get("ci_poll_seconds", 30), remaining))


def merge(client: GitHub, policy: dict, number: int) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "模拟阶段禁止自动合并")
    base = assert_trusted_checkout(client)
    pr = client.api(f"repos/{policy['repository']}/pulls/{number}")
    if pr.get("merged") is True:
        return recover_merged(client, policy, pr, base)
    require(pr["state"] == "open" and not pr.get("merged"), "PR 已关闭或已经合并")
    require(pr["head"]["ref"].startswith(policy["branch_prefix"]), "不是 Codex 自动研发分支")
    head = pr["head"]["sha"]
    run(["git", "fetch", "--no-tags", "origin", head], cwd=ROOT)
    result = github_gate(client, policy, pr, base, head)
    require(policy["mode"] != "pilot" or result["issue"] == policy["pilot_issue"], "不在唯一试点范围")
    events = client.events()
    claim_event = delivery_lease(client, events, result["issue"])
    ready_at = None
    if pr["draft"]:
        # 只有范围、独立复核、定向测试证据与当前有效租约均通过才能Ready。
        history = client.events(result["issue"])
        require(any(e.get("event") == "checkpoint" and e.get("stage") == "tests_passed" and e.get("head_sha") == head
                    for e in history), "草稿缺少绑定当前提交的测试证据")
        ready_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        try:
            client.gh("pr", "ready", str(number), "--repo", policy["repository"])
        except RequestFailure:
            require(client.api(f"repos/{policy['repository']}/pulls/{number}").get("draft") is False,
                    "Ready结果不确定；先对账，不重复写入")
    if not wait_for_ci(client, policy, pr, result["issue"], claim_event, ready_at=ready_at):
        return {"event": "checkpoint", "stage": "waiting_ci", "issue": result["issue"], "pr": number}
    # 等待后重新核对正式授权和复核，不能用等待前的快照合并。
    pr = client.api(f"repos/{policy['repository']}/pulls/{number}")
    require(pr["head"]["sha"] == head and not pr["draft"], "PR已变化或恢复草稿")
    require(client.main_sha() == base, "主分支变化；重新复核后交付")
    result = github_gate(client, policy, pr, base, head)
    events = client.events()
    delivery_lease(client, events, result["issue"])
    now = datetime.now(timezone.utc)
    # 不使用 --admin / --auto。严格分支保护仍需由启用验收确认，不能绕过未解决讨论/最新基线要求。
    readiness = json.loads(client.gh_read("pr", "view", str(number), "--repo", policy["repository"], "--json", "mergeStateStatus,headRefOid"))
    require(readiness["headRefOid"] == head and readiness["mergeStateStatus"] == "CLEAN" and client.main_sha() == base, "PR/主分支变化或合并门禁尚未满足")
    day, _ = periods(now)
    key = f"merge:{day}:{number}:{head}"
    pending = next((event for event in events if event.get("event") == "reserve" and event.get("key") == key), None)
    if pending is None:
        client.append({**reserve(events, policy, now, "merge", key), "issue": result["issue"], "pr": number,
                       "head_sha": head, "base_sha": base})
    else:
        require(pending.get("head_sha") == head and pending.get("base_sha") == base,
                "已有合并预留与当前提交不一致")
    attempted = [e for e in events if e.get("event") == "merge_attempt" and e.get("pr") == number and e.get("head_sha") == head]
    require(not attempted, "该提交已有合并尝试但结果未确认；禁止重复合并，需对账")
    client.append({"event": "merge_attempt", "key": f"merge-attempt:{number}:{head}", "pr": number,
                   "head_sha": head, "at": now.isoformat()})
    delivery_lease(client, client.events(), result["issue"])
    try:
        client.gh("pr", "merge", str(number), "--repo", policy["repository"], "--squash", "--match-head-commit", head)
    except RequestFailure:
        require(client.api(f"repos/{policy['repository']}/pulls/{number}").get("merged") is True,
                "合并结果不确定；保留尝试记录，不重复合并")
    merged = client.api(f"repos/{policy['repository']}/pulls/{number}")
    require(merged.get("merged") is True and merged["head"]["sha"] == head, "合并结果不确定；下一轮查询 PR，不重试合并")
    return record_delivery(client, policy, merged, result["issue"], result["production"],
                           result["contract_digest"], key, events, client.events(result["issue"]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument("--run-id", default=os.environ.get("CODEX_THREAD_ID", ""))
    args = parser.parse_args()
    try:
        policy = load_policy()
        with repository_lock(policy["repository"]):
            client = GitHub(policy)
            initialize_run(client, policy, args.run_id)
            print(json.dumps(merge(client, policy, args.pr), ensure_ascii=False))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError) as exc:
        print(f"[development-delivery] STOP: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
