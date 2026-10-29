"""以当前日期登记已验证的历史交付；不倒填合同、批准或生产恢复次数。"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path

from .development_policy import DevelopmentError, digest, require
from scripts.validate_project_task import primary_task_reference_from_pr_body


def inspect_delivery(client, issue: int, number: int) -> dict:
    pr = client.api(f"repos/{client.repo}/pulls/{number}")
    require(pr.get("merged") and pr.get("base", {}).get("ref") == "main", "历史PR未合并到main")
    require(pr["head"]["repo"]["full_name"] == client.repo, "历史PR来自其他仓库")
    primary, _ = primary_task_reference_from_pr_body(pr.get("body", ""))
    require(primary == issue, "历史PR主任务不匹配，普通引用不能作为交付")
    merge_sha, head_sha = pr["merge_commit_sha"], pr["head"]["sha"]
    main = client.main_sha()
    comparison = client.api(f"repos/{client.repo}/compare/{merge_sha}...{main}")
    require(comparison.get("status") in {"ahead", "identical"}, "历史交付不在当前主分支")
    checks = client.api(f"repos/{client.repo}/commits/{head_sha}/check-runs?per_page=100")
    require(checks.get("total_count", 0) <= 100, "历史检查被截断")
    latest = {}
    for check in sorted(checks.get("check_runs", []), key=lambda row: row.get("id", 0)):
        if check.get("head_sha", head_sha) == head_sha:
            latest[check["name"]] = check
    required = client.policy["required_checks"]
    tests_verified = all(latest.get(name, {}).get("conclusion") == "success" for name in required)
    files = client.api(f"repos/{client.repo}/pulls/{number}/files?per_page=100")
    require(isinstance(files, list) and len(files) < 100, "历史PR改动范围不完整")
    return {"issue": issue, "pr": number, "head_sha": head_sha, "merge_sha": merge_sha,
            "merged_at": pr["merged_at"], "main_sha": main, "pr_url": pr["html_url"],
            "tests_verified": tests_verified, "tests_status": "passed" if tests_verified else "unknown",
            "files": [row["filename"] for row in files], "original_pr_body": pr.get("body", "")}


def adoption(client, request: dict) -> dict:
    evidence = inspect_delivery(client, int(request["issue"]), int(request["pr"]))
    require(request.get("merge_sha") == evidence["merge_sha"], "历史登记绑定的合并SHA不匹配")
    for field in ("delivered_scope", "acceptance_evidence"):
        require(isinstance(request.get(field), list) and bool(request[field])
                and all(isinstance(v, str) and v.strip() for v in request[field]), f"历史登记缺少{field}")
    require(isinstance(request.get("remaining_scope"), list), "历史登记必须明确剩余范围")
    remaining_issue = request.get("remaining_issue")
    if remaining_issue:
        require(type(remaining_issue) is int and remaining_issue != evidence["issue"], "剩余实现必须关联独立正式任务")
        require(client.task(remaining_issue)["state"] == "OPEN", "剩余任务不可执行")
    production = request.get("production", {})
    require(production.get("kind") in {"none", "source", "task"}, "历史交付缺少验收方式")
    require(isinstance(production.get("checks"), list) and bool(production["checks"]), "历史交付缺少原验收条件")
    if production["kind"] == "none":
        require(not request["remaining_scope"] and evidence["files"] and evidence["tests_verified"] and all(p.startswith(("docs/", "tests/")) or p.endswith(".md")
                for p in evidence["files"]), "历史业务交付不能以测试通过免除生产验收")
    if production["kind"] == "source":
        require(bool(production.get("source_id")), "来源验收缺少来源绑定")
    facts = {**evidence, **{k: request[k] for k in ("delivered_scope", "remaining_scope", "acceptance_evidence")},
             "remaining_issue": remaining_issue, "production": production}
    return {"event": "historical_delivery_adopted", "issue": evidence["issue"], "facts": facts,
            "key": f"historical-delivery:{digest(facts)}", "at": datetime.now(timezone.utc).isoformat(),
            "approval_status": "not_reconstructed", "contract_status": "not_reconstructed"}


def record(client, request: dict) -> dict:
    event = adoption(client, request)
    for issue in (event["issue"], None):
        if not any(e.get("key") == event["key"] for e in client.events(issue)):
            client.append(event, issue)
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--issue", required=True, type=int)
    parser.add_argument("--pr", required=True, type=int)
    parser.add_argument("--request")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--run-id", default=os.environ.get("CODEX_THREAD_ID", ""))
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    from .development_cycle import ROOT, assert_trusted_checkout, initialize_run, load_policy
    from .development_lease import isolated_output
    from .development_runtime import GitHub, repository_lock
    try:
        client = GitHub(load_policy())
        with repository_lock(client.repo):
            if args.apply:
                initialize_run(client, client.policy, args.run_id)
                assert_trusted_checkout(client)
                isolated_output(ROOT, args.run_id, args.out, "historical-delivery.json")
                require(bool(args.request), "缺少结构化登记请求")
                request = json.loads(Path(args.request).read_text())
                require(request.get("issue") == args.issue and request.get("pr") == args.pr, "请求身份不匹配")
                result = record(client, request)
            else:
                result = inspect_delivery(client, args.issue, args.pr)
        output = Path(args.out); output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError) as exc:
        print(f"[historical-delivery] STOP: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
