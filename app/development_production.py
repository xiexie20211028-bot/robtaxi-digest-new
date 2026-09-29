"""从正常Actions运行及原始产物验证交付；不接受模型勾选的通过结果。"""
from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import datetime, timezone
from pathlib import Path

from app.development_policy import digest, production_status, require, timestamp, validate_contract
from app.health_issue import build_incidents
from app.source_health import normalize_health_status

WORKFLOW = "robtaxi-digest-pages.yml"
LEGACY_FIELDS = {"daily_pool_size", "baseline_count", "baseline_matched_count", "baseline_unmatched_count",
                 "baseline_unmatched_samples", "recall_at_20", "recall_guard_alert", "recall_guard_message"}


def pages(client, endpoint: str, field: str) -> list[dict]:
    result = []
    for page in range(1, 101):
        separator = "&" if "?" in endpoint else "?"
        data = client.api(f"{endpoint}{separator}per_page=100&page={page}")
        rows = data.get(field)
        require(isinstance(rows, list), "生产证据分页结构无效")
        result.extend(rows)
        if len(rows) < 100:
            return result
    require(False, "生产证据超出完整分页上限")


def artifact_json(client, run: dict, artifacts: list[dict], prefix: str, filename: str) -> tuple[dict | None, str | None]:
    name = f"{prefix}-{run['id']}-{run['run_attempt']}"
    matches = [a for a in artifacts if a.get("name") == name and not a.get("expired")]
    if len(matches) != 1:
        return None, None
    artifact = matches[0]
    require(0 < artifact.get("size_in_bytes", 0) <= 64 * 1024 * 1024, "生产产物大小异常")
    with tempfile.TemporaryDirectory(prefix="robtaxi-evidence-") as directory:
        client.gh_read("run", "download", str(run["id"]), "--repo", client.repo, "--name", name, "--dir", directory)
        root = Path(directory).resolve()
        files = [p for p in root.rglob(filename) if p.is_file()]
        # health产物里可能也带run_report，但指定的health_report必须唯一。
        if len(files) != 1:
            return None, None
        path = files[0]
        require(not path.is_symlink() and path.resolve().is_relative_to(root), "产物路径越界")
        require(path.stat().st_size <= 8 * 1024 * 1024, "报告过大")
        raw = path.read_bytes()
        try:
            value = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            return None, None
        return (value, hashlib.sha256(raw).hexdigest()) if isinstance(value, dict) else (None, None)


def evaluate_report(name: str, contract: dict, report: dict, jobs: list[dict], health: dict | None = None) -> bool | None:
    if name == "report_compat_v1":
        # 不以overall workflow结论代替任务验收；self_check可以因无关来源告警失败。
        required = {"build", "deploy", "notify"}
        latest = {}
        for job in jobs:
            key = job.get("name")
            if key not in latest or job.get("id", 0) > latest[key].get("id", 0):
                latest[key] = job
        if not required.issubset(latest) or any(latest[k].get("status") != "completed" for k in required):
            return None
        if any(latest[k].get("conclusion") != "success" for k in required):
            return False
        if not isinstance(report.get("stage_status"), dict) or not report.get("html_output"):
            return None
        return (not LEGACY_FIELDS.intersection(report)
                and report["stage_status"].get("render") == "success"
                and report["stage_status"].get("notify") == "success"
                and all(isinstance(report.get(k), dict) and report[k].get("final_status") in {"success", "already_sent"}
                        for k in ("feishu_push_status", "wecom_push_status")))
    if name == "source_health_v1":
        source = contract["production"].get("source_id")
        rows = [r for r in report.get("source_stats", []) if r.get("source_id") == source]
        if len(rows) != 1 or not rows[0].get("status") or not health:
            return None
        return (normalize_health_status(rows[0]["status"]) == "healthy"
                and not any(i.get("source_id") == source for i in build_incidents(health)))
    return None


def collect_evidence(client, policy: dict, pr: dict, contract: dict, verifier: str) -> list[dict]:
    prefix = f"repos/{policy['repository']}"
    workflow = client.api(f"{prefix}/actions/workflows/{WORKFLOW}")
    require(workflow.get("path") == f".github/workflows/{WORKFLOW}", "生产工作流身份不一致")
    runs = pages(client, f"{prefix}/actions/workflows/{workflow['id']}/runs?branch=main&event=schedule", "workflow_runs")
    evidence = []
    for run in sorted(runs, key=lambda r: r["created_at"]):
        if (run.get("event") != "schedule" or run.get("head_branch") != "main"
                or run.get("workflow_id") != workflow["id"] or run.get("status") != "completed"
                or timestamp(run["created_at"]) < timestamp(pr["merged_at"])):
            continue
        comparison = client.api(f"{prefix}/compare/{pr['merge_commit_sha']}...{run['head_sha']}")
        if comparison.get("status") not in {"ahead", "identical"}:
            continue
        row = {"id": run["id"], "attempt": run["run_attempt"], "created_at": run["created_at"],
               "event": "schedule", "contains_merge": True, "commit_sha": run["head_sha"],
               "artifacts_complete": False, "acceptance_passed": None, "sources_executed": []}
        artifacts = pages(client, f"{prefix}/actions/runs/{run['id']}/artifacts", "artifacts")
        report, report_digest = artifact_json(client, run, artifacts, "robtaxi-notify", "run_report.json")
        if report is not None:
            jobs = pages(client, f"{prefix}/actions/runs/{run['id']}/attempts/{run['run_attempt']}/jobs", "jobs")
            health = None
            if verifier == "source_health_v1":
                health, _ = artifact_json(client, run, artifacts, "robtaxi-health", "health_report.json")
                provenance = (health or {}).get("run", {})
                if (str(provenance.get("github_run_id")) != str(run["id"])
                        or str(provenance.get("github_run_attempt")) != str(run["run_attempt"])
                        or provenance.get("commit_sha") != run["head_sha"]
                        or provenance.get("repository") != policy["repository"]
                        or provenance.get("event_name") != "schedule"
                        or (health or {}).get("source_report", {}).get("sha256") != report_digest):
                    health = None
            passed = evaluate_report(verifier, contract, report, jobs, health)
            row.update({"artifacts_complete": passed is not None, "acceptance_passed": passed,
                        "report_sha256": report_digest,
                        "sources_executed": [r["source_id"] for r in report.get("source_stats", [])
                                             if r.get("source_id") and r.get("status")]})
        evidence.append(row)
    return evidence


def reconcile_production(client, policy: dict, issue: int | None = None, *, apply: bool = False) -> dict:
    # 延迟导入避免cycle入口与生产适配器循环依赖。
    from app.development_cycle import assert_trusted_checkout, set_status
    if apply:
        require(policy["mode"] in {"pilot", "active"}, "模拟模式不写生产验收")
        require(getattr(client, "budget", None) is not None, "验收写入需要正式运行预算")
        assert_trusted_checkout(client)
    registry = policy.get("production_verifiers", {})
    tasks = [client.task(issue)] if issue is not None else client.tasks()
    results = []
    for task in tasks:
        number = task["number"]
        if number == policy["control_issue"] or (policy["mode"] == "pilot" and number != policy["pilot_issue"]):
            continue
        if task.get("type") == "Epic" or task.get("status") == "已取消" or set(task.get("labels", [])).intersection(
                {policy["labels"]["human"], policy["labels"]["paused"], "robtaxi-health", "health-alert"}):
            continue
        if task.get("status") not in {"观察中", "待验证", "已完成"}:
            continue
        history = client.events(number)
        deliveries = [e for e in history if e.get("event") == "delivery"]
        if not deliveries:
            continue
        delivery = deliveries[-1]
        contracts = [e["contract"] for e in history if e.get("event") == "contract"
                     and e.get("producer") in {"codex-scheduled", "codex-exec"}
                     and digest(e["contract"]) == delivery.get("contract_digest")]
        require(bool(contracts), "交付找不到绑定的正式合同")
        contract = contracts[-1]
        validate_contract(contract, number)
        registration = registry.get(str(number), {})
        name = registration.get("name")
        no_observation = contract["production"]["kind"] == "none"
        if not no_observation and (registration.get("kind") != contract["production"]["kind"]
                or name not in {"report_compat_v1", "source_health_v1"}
                or (name == "report_compat_v1" and (number != 69 or contract["production"]["kind"] != "task"))
                or (name == "source_health_v1" and registration.get("source_id") != contract["production"].get("source_id"))):
            results.append({"issue": number, "status": "no_trusted_verifier"})
            continue
        pr = client.api(f"repos/{policy['repository']}/pulls/{delivery['pr']}")
        require(pr.get("merged") and pr.get("merge_commit_sha") == delivery["merge_sha"]
                and pr["head"]["sha"] == delivery["head_sha"] and pr["base"]["ref"] == "main"
                and pr["head"]["repo"]["full_name"] == policy["repository"], "生产验收交付身份不一致")
        verified = next((e for e in history if e.get("event") == "production_verified"
                         and e.get("merge_sha") == delivery["merge_sha"]
                         and e.get("contract_digest") == delivery["contract_digest"]), None)
        # 文档/纯测试任务合并后可完成，仍需核对正式交付身份并恢复中断的关闭/总盘更新。
        evidence = [] if verified or no_observation else collect_evidence(client, policy, pr, contract, name)
        status = "complete" if verified else production_status(contract, delivery["merge_sha"], evidence)
        result = {"issue": number, "status": status, "runs": [r["id"] for r in evidence]}
        results.append(result)
        if not apply or status == "observing":
            continue
        client.budget.check()
        event_type = "production_verified" if status == "complete" else "production_requeue"
        event = verified or {"event": event_type, "key": f"{event_type}:{number}:{delivery['merge_sha']}:{delivery['contract_digest']}",
                             "issue": number, "merge_sha": delivery["merge_sha"], "contract_digest": delivery["contract_digest"],
                             "verifier": name, "evidence": evidence, "at": datetime.now(timezone.utc).isoformat()}
        if not any(e.get("event") == event["event"] and e.get("key") == event["key"] for e in history):
            client.append(event, number)
        if status == "complete":
            if task["state"] != "CLOSED":
                # 关闭响应丢失由下一次读取Issue状态恢复，不能盲目重发。
                client.gh("issue", "close", str(number), "--repo", policy["repository"], "--reason", "completed")
            if task.get("status") != "已完成":
                set_status(client, number, "已完成")
        else:
            if task["state"] == "CLOSED":
                client.gh("issue", "reopen", str(number), "--repo", policy["repository"])
            client.label(number, "planning")
            set_status(client, number, "待办")
    return {"action": "verify-production", "apply": apply, "results": results}
