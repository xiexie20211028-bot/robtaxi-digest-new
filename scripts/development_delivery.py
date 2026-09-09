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
from app.development_cycle import assert_trusted_checkout, load_policy
from app.development_policy import DevelopmentError, periods, require, reserve
from app.development_runtime import GitHub, repository_lock, run
from app.health_loop_sync import GhMetadataClient
from scripts.validate_development_delivery import github_gate


def set_status(policy: dict, issue: int, status: str) -> None:
    metadata = GhMetadataClient(repository=policy["repository"], owner=policy["project_owner"], project=policy["project_number"])
    metadata.preflight()
    item = metadata._project_item(issue)
    require(bool(item), "正式任务不在总盘")
    metadata._set_field(item["id"], "Status", status)


def merge(client: GitHub, policy: dict, number: int) -> dict:
    require(policy["mode"] in {"pilot", "active"}, "模拟阶段禁止自动合并")
    base = assert_trusted_checkout(client)
    pr = client.api(f"repos/{policy['repository']}/pulls/{number}")
    require(pr["state"] == "open" and not pr["draft"] and not pr.get("merged"), "PR 尚未就绪或已经合并")
    require(pr["head"]["ref"].startswith("workbuddy/development-"), "不是通用研发执行分支")
    head = pr["head"]["sha"]
    run(["git", "fetch", "--no-tags", "origin", head], cwd=ROOT)
    result = github_gate(client, policy, pr, base, head)
    require(policy["mode"] != "pilot" or result["issue"] == policy["pilot_issue"], "不在唯一试点范围")
    events = client.events()
    require(not any(e.get("event") == "release_freeze" and not e.get("resolved") for e in events), "生产严重异常尚未解除，发布冻结")
    checks = client.api(f"repos/{policy['repository']}/commits/{head}/check-runs?per_page=100")
    require(checks.get("total_count", 0) <= 100, "检查列表不完整")
    for name in policy["required_checks"]:
        matching = [c for c in checks.get("check_runs", []) if c["name"] == name and c.get("app", {}).get("slug") == "github-actions"]
        require(bool(matching) and all(c.get("conclusion") == "success" for c in matching), f"必要检查未全部成功：{name}")
    # 不使用 --admin / --auto。严格分支保护仍需由启用验收确认，不能绕过未解决讨论/最新基线要求。
    readiness = json.loads(client.gh("pr", "view", str(number), "--repo", policy["repository"], "--json", "mergeStateStatus,headRefOid"))
    require(readiness["headRefOid"] == head and readiness["mergeStateStatus"] == "CLEAN" and client.main_sha() == base, "PR/主分支变化或合并门禁尚未满足")
    now = datetime.now(timezone.utc)
    day, _ = periods(now)
    key = f"merge:{number}:{head}"
    client.append({**reserve(events, policy, now, "merge", key), "issue": result["issue"], "pr": number, "head_sha": head, "base_sha": base})
    client.gh("pr", "merge", str(number), "--repo", policy["repository"], "--squash", "--match-head-commit", head)
    merged = client.api(f"repos/{policy['repository']}/pulls/{number}")
    require(merged.get("merged") is True and merged["head"]["sha"] == head, "合并结果不确定；下一轮查询 PR，不重试合并")
    receipt = {"event": "merged", "issue": result["issue"], "pr": number, "key": key, "day": day,
               "head_sha": head, "merge_sha": merged["merge_commit_sha"], "at": merged["merged_at"],
               "production": result["production"], "contract_digest": result["contract_digest"]}
    client.append(receipt, result["issue"])
    client.append(receipt)
    if result["production"]["kind"] == "none":
        # 无生产观察任务只有合并成功后才能关闭，Status 不反向触发 Issue 关闭。
        client.gh("issue", "close", str(result["issue"]), "--repo", policy["repository"], "--reason", "completed")
        set_status(policy, result["issue"], "已完成")
    else:
        set_status(policy, result["issue"], "观察中")
    return receipt


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
