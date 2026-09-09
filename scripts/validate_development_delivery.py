#!/usr/bin/env python3
"""从受信任 base 运行的通用研发门禁；不读取候选分支的策略/验收规则。"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.development_policy import (DevelopmentError, check_fresh, classify_change, digest,
                                    require, validate_contract, validate_policy, verify_review)
from app.development_runtime import GitHub, run
from scripts.validate_project_task import primary_task_reference_from_pr_body


def changed_paths(base: str, head: str) -> list[str]:
    # 使用 NUL 分隔；改名同时检查旧、新路径，不允许通过改名移走受保护样本。
    raw = run(["git", "diff", "--no-renames", "--name-only", "-z", base, head], cwd=ROOT)
    return [path for path in raw.split("\0") if path]


def source_guard(base: dict, head: dict, policy: dict) -> None:
    require(set(base) == set(head), "sources.json 顶层结构修改需要人工决策")
    require({k: v for k, v in base.items() if k != "sources"} == {k: v for k, v in head.items() if k != "sources"}, "生产路线/全局阈值配置不可自动修改")
    before = {s["id"]: s for s in base.get("sources", [])}
    after = {s["id"]: s for s in head.get("sources", [])}
    require(len(before) == len(base.get("sources", [])) and len(after) == len(head.get("sources", [])), "来源 ID 重复")
    require(before.keys() == after.keys(), "不能删除/增加信源来通过验收")
    for key in before:
        fields = {f for f in before[key].keys() | after[key].keys() if before[key].get(f) != after[key].get(f)}
        require(fields.issubset(policy["source_allowed_fields"]), "只能自动修改来源适配字段；停用信源、阈值、路线需人工决定")


def local_gate(contract: dict, policy: dict, base: str, head: str, *, priority: str, route: str) -> dict:
    validate_contract(contract)
    paths = changed_paths(base, head)
    risk = classify_change(contract, paths, policy, priority=priority, route=route)
    freshness = changed_paths(contract["base_sha"], base)
    check_fresh(contract, freshness)
    if "sources.json" in paths:
        source_guard(json.loads(run(["git", "show", f"{base}:sources.json"], cwd=ROOT)),
                     json.loads(run(["git", "show", f"{head}:sources.json"], cwd=ROOT)), policy)
    stats = run(["git", "diff", "--no-renames", "--numstat", base, head], cwd=ROOT)
    for line in stats.splitlines():
        additions, deletions, path = line.split("\t", 2)
        require(additions != "-" and deletions != "-", "自动交付不允许不可检查的二进制变更")
        require(not (path.startswith("tests/") and int(deletions) > 0), "不能删除/改写既有验收样本获得通过；新增测试允许")
    modes = run(["git", "diff", "--raw", base, head], cwd=ROOT)
    require("120000" not in modes and "160000" not in modes, "自动交付不允许符号链接或子模块变更")
    return {"risk": risk, "paths": paths, "contract_digest": digest(contract), "base_sha": base, "head_sha": head}


def github_gate(client: GitHub, policy: dict, pr: dict, base: str, head: str) -> dict:
    require(pr.get("base", {}).get("ref") == "main", "自动交付只允许主分支")
    require(pr.get("head", {}).get("repo", {}).get("full_name") == policy["repository"], "自动交付不接收 fork")
    require(pr.get("head", {}).get("sha") == head, "PR 当前提交与待验收版本不一致")
    issue, keyword = primary_task_reference_from_pr_body(pr.get("body", ""))
    tasks = client.tasks()
    task = next((t for t in tasks if t["number"] == issue), None)
    require(task is not None and task["state"] == "OPEN" and task["status"] == "待验证", "非 Draft 自动 PR 必须关联待验证的开放工程任务")
    require(task["type"] != "Epic" and task["assignees"] > 0 and not task["blockers"], "任务类型/负责人/实际依赖不允许交付")
    require(not set(task["labels"]).intersection({"robtaxi-health", "health-alert", policy["labels"]["human"], policy["labels"]["paused"]}), "健康证据或人工保留/暂停任务不能自动交付")
    events = client.events(issue)
    contracts = [e for e in events if e.get("event") == "contract" and e.get("producer") == "codex-exec"]
    require(bool(contracts), "Issue 没有 Codex 生成的执行说明")
    contract = contracts[-1]["contract"]
    validate_contract(contract, issue)
    require(not set(contract["dependencies"]).intersection({t["number"] for t in tasks if t["state"] == "OPEN"}), "执行说明依赖尚未关闭")
    if contract["production"]["kind"] != "none":
        require(keyword.lower() == "refs", "需要生产观察的任务必须使用 Refs，不能合并即关闭")
        require(not re.search(r"(?i)\b(?:fix(?:e[sd])?|close[sd]?|resolve[sd]?)\s+#" + str(issue) + r"\b", pr.get("body", "")), "PR 正文另有提前关闭任务的关键词")
    result = local_gate(contract, policy, base, head, priority=task["priority"], route=task["route"])
    if result["risk"] == "High":
        reviews = [e for e in events if e.get("event") == "review"]
        require(bool(reviews), "高影响改动需 Codex 独立复核")
        verify_review(reviews[-1], contract, head, base)
    result.update({"issue": issue, "production": contract["production"]})
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    args = parser.parse_args()
    try:
        policy = json.loads((ROOT / ".github/robtaxi-autonomy.json").read_text())
        legacy = ROOT / ".github/robtaxi-health-autofix.json"
        validate_policy(policy, json.loads(legacy.read_text()) if legacy.exists() else {})
        event = json.loads(Path(args.event).read_text())
        pr = event["pull_request"]
        # 手工工程继续走原治理；自动分支或标记任一个命中都不能绕过新门禁。
        automated = pr["head"]["ref"].startswith("workbuddy/development-") or "workbuddy-development" in {l["name"] for l in pr.get("labels", [])}
        if not automated:
            print("[development-gate] SKIP: 非自动研发 PR")
            return 0
        require(policy["mode"] in {"pilot", "active"}, "尚未开放自动交付")
        require(not pr.get("draft"), "Draft 保留在执行阶段，不作为自动交付结果")
        client = GitHub(policy)
        current = client.api(f"repos/{policy['repository']}/pulls/{pr['number']}")
        result = github_gate(client, policy, current, args.base, args.head)
        if policy["mode"] == "pilot":
            require(result["issue"] == policy["pilot_issue"], "不在当前试点范围")
        print(json.dumps(result, ensure_ascii=False))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError, TypeError) as exc:
        print(f"[development-gate] STOP: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
