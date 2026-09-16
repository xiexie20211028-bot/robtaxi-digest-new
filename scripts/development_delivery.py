#!/usr/bin/env python3
"""自动交付最后一步：从最新可信主分支重查门禁、预留每日配额并锁定提交合并。"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.development_cycle import active_claim, assert_trusted_checkout, load_policy, set_status
from app.development_policy import DevelopmentError, digest, periods, require, reserve, validate_contract
from app.development_runtime import GitHub, repository_lock, run
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
    require(bool(claim_event) and claim_event["action"] == "delivery", "恢复交付需要当前 delivery 租约")
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


def merge(client: GitHub, policy: dict, number: int) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "模拟阶段禁止自动合并")
    base = assert_trusted_checkout(client)
    pr = client.api(f"repos/{policy['repository']}/pulls/{number}")
    if pr.get("merged") is True:
        return recover_merged(client, policy, pr, base)
    require(pr["state"] == "open" and not pr["draft"] and not pr.get("merged"), "PR 尚未就绪或已经合并")
    require(pr["head"]["ref"].startswith(policy["branch_prefix"]), "不是 Codex 自动研发分支")
    head = pr["head"]["sha"]
    run(["git", "fetch", "--no-tags", "origin", head], cwd=ROOT)
    result = github_gate(client, policy, pr, base, head)
    require(policy["mode"] != "pilot" or result["issue"] == policy["pilot_issue"], "不在唯一试点范围")
    events = client.events()
    now = datetime.now(timezone.utc)
    claim_event = active_claim(events, result["issue"], now)
    require(bool(claim_event), "没有当前有效的90分钟任务租约")
    require(claim_event["action"] in {"execute", "review", "delivery"}, "当前租约不允许交付")
    require(not any(e.get("event") == "release_freeze" and not e.get("resolved") for e in events), "生产严重异常尚未解除，发布冻结")
    checks = client.api(f"repos/{policy['repository']}/commits/{head}/check-runs?per_page=100")
    require(checks.get("total_count", 0) <= 100, "检查列表不完整")
    for name in policy["required_checks"]:
        matching = [c for c in checks.get("check_runs", []) if c["name"] == name and c.get("app", {}).get("slug") == "github-actions"]
        require(bool(matching) and all(c.get("conclusion") == "success" for c in matching), f"必要检查未全部成功：{name}")
    # 不使用 --admin / --auto。严格分支保护仍需由启用验收确认，不能绕过未解决讨论/最新基线要求。
    readiness = json.loads(client.gh("pr", "view", str(number), "--repo", policy["repository"], "--json", "mergeStateStatus,headRefOid"))
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
    client.gh("pr", "merge", str(number), "--repo", policy["repository"], "--squash", "--match-head-commit", head)
    merged = client.api(f"repos/{policy['repository']}/pulls/{number}")
    require(merged.get("merged") is True and merged["head"]["sha"] == head, "合并结果不确定；下一轮查询 PR，不重试合并")
    return record_delivery(client, policy, merged, result["issue"], result["production"],
                           result["contract_digest"], key, events, client.events(result["issue"]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pr", required=True, type=int)
    args = parser.parse_args()
    try:
        policy = load_policy()
        with repository_lock(policy["repository"]):
            print(json.dumps(merge(GitHub(policy), policy, args.pr), ensure_ascii=False))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError) as exc:
        print(f"[development-delivery] STOP: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
