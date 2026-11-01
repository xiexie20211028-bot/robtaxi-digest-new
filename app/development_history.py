"""以当前日期登记已验证的历史交付；不倒填合同、批准或生产恢复次数。"""
from __future__ import annotations

import argparse
import base64
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
    from .development_budget import impact_from_diff
    paths = sorted({p for row in files for p in (row["filename"], row.get("previous_filename")) if p})
    before = after = None
    operational = [p for p in paths if not p.startswith(("docs/", "tests/")) and not p.endswith(".md")]
    if operational == ["sources.json"]:
        base_sha = pr.get("base", {}).get("sha")
        require(bool(base_sha), "来源交付缺少原始base SHA")
        snapshots = []
        for ref in (base_sha, head_sha):
            blob = client.api(f"repos/{client.repo}/contents/sources.json?ref={ref}")
            require(blob.get("encoding") == "base64" and isinstance(blob.get("content"), str), "来源快照不可验证")
            snapshots.append(json.loads(base64.b64decode(blob["content"])))
        before, after = snapshots
    impact = impact_from_diff(paths, before=before, after=after)
    return {"issue": issue, "pr": number, "head_sha": head_sha, "merge_sha": merge_sha,
            "merged_at": pr["merged_at"], "main_sha": main, "pr_url": pr["html_url"],
            "tests_verified": tests_verified, "tests_status": "passed" if tests_verified else "unknown",
            "files": paths, "original_pr_body": pr.get("body", ""), "impact_keys": impact,
            "impact_evidence": {"method": "actual_pr_diff_v1", "head_sha": head_sha,
                                "base_sha": pr.get("base", {}).get("sha"), "files_digest": digest(paths),
                                "source_snapshots_digest": digest([before, after]) if before is not None else None}}


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
    if production.get("mode") == "offline_replay":
        from .development_acceptance import binding_for
        require(production == {**production, **binding_for(evidence["issue"])}, "离线验收绑定与任务不匹配")
        require(evidence["tests_verified"], "离线业务验收缺少原PR检查")
    else:
        require(production.get("mode") in {None, "normal_production"}, "未知验收模式")
    facts = {**evidence, **{k: request[k] for k in ("delivered_scope", "remaining_scope", "acceptance_evidence")},
             "remaining_issue": remaining_issue, "production": production}
    identity = digest({k: evidence[k] for k in ("issue", "pr", "merge_sha")})
    revision = digest({k: facts[k] for k in ("delivered_scope", "remaining_scope", "remaining_issue", "production", "impact_keys")})
    return {"event": "historical_delivery_adopted", "schema_version": "robtaxi-historical-delivery-v2",
            "issue": evidence["issue"], "facts": facts, "delivery_id": identity, "revision": revision,
            "key": f"historical-delivery:{identity}:{revision}", "at": datetime.now(timezone.utc).isoformat(),
            "approval_status": "not_reconstructed", "contract_status": "not_reconstructed"}


def record(client, request: dict) -> dict:
    event = adoption(client, request)
    event["run_id"] = getattr(client, "run_id", None)
    history = client.events(event["issue"])
    # 相同修订重跑沿用原事件；双边补写不会创建不同时间的副本。
    existing = next((e for e in history if e.get("key") == event["key"]), None)
    if existing:
        siblings = [e for e in history if e.get("event") == event["event"]
                    and e.get("facts", {}).get("pr") == event["facts"]["pr"]
                    and e.get("facts", {}).get("merge_sha") == event["facts"]["merge_sha"]]
        require(siblings[-1]["key"] == existing["key"], "登记修订已被替代，不能恢复旧验收范围")
        event = {k: v for k, v in existing.items() if not k.startswith("_")}
    else:
        previous = [e for e in history if e.get("event") == event["event"]
                    and e.get("facts", {}).get("pr") == event["facts"]["pr"]
                    and e.get("facts", {}).get("merge_sha") == event["facts"]["merge_sha"]]
        event["supersedes_adoptions"] = [e["key"] for e in previous]
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
