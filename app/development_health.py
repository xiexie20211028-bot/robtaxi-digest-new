"""确定性读取正常生产证据；来源同步不等待无关业务复盘。"""
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

from .development_policy import require, digest
from .development_production import pages, artifact_json, WORKFLOW
from .health_evidence import matches_production
from .health_loop import evaluate_health_loop, _source_participated, action_fingerprint

REVIEW_WORKFLOW = "robtaxi-agent-review.yml"


def normal_runs(client, workflow: str) -> list[dict]:
    prefix = f"repos/{client.repo}"
    definition = client.api(f"{prefix}/actions/workflows/{workflow}")
    require(definition.get("path") == f".github/workflows/{workflow}", "证据工作流身份不符")
    runs = pages(client, f"{prefix}/actions/workflows/{definition['id']}/runs?branch=main&event=schedule", "workflow_runs")
    return sorted((r for r in runs if r.get("event") == "schedule" and r.get("head_branch") == "main"
        and r.get("workflow_id") == definition["id"] and r.get("status") == "completed"),
        key=lambda r: (r["created_at"], int(r["id"])))


def source_inputs(client, run: dict) -> tuple[dict | None, dict | None]:
    artifacts = pages(client, f"repos/{client.repo}/actions/runs/{run['id']}/artifacts", "artifacts")
    report, report_hash = artifact_json(client, run, artifacts, "robtaxi-notify", "run_report.json")
    health, _ = artifact_json(client, run, artifacts, "robtaxi-health", "health_report.json")
    if health is None or report is None:
        return None, None
    require(matches_production(health, run, client.repo, report_hash),
            "健康报告自身、生产身份或摘要不可信，停止正式同步")
    return health, report


def review_binding(client, review_rows: list[tuple[dict, dict]], run: dict, day: str,
                   source_runs: list[dict]) -> tuple[dict, dict]:
    matches = [(r, v) for r, v in review_rows if v.get("daily", {}).get("date") == day]
    # 同一日期出现多次独立生产或多份评审时，不能仅靠日期任选一份。
    same_day = [r for r in source_runs if r.get("_date") == day or (
        not r.get("_date") and datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
        .astimezone(timezone(timedelta(hours=8))).date().isoformat() == day)]
    if not matches:
        return {}, {"status": "pending", "date": day, "reason": "对应业务复盘尚未到达；来源继续处理"}
    if len(matches) != 1 or len(same_day) != 1:
        return {}, {"status": "unmatched", "date": day, "reason": "同日生产或评审不唯一，关联待核对"}
    rr, value = matches[0]
    comparison = client.api(f"repos/{client.repo}/compare/{run['head_sha']}...{rr['head_sha']}")
    if rr["created_at"] < run["created_at"] or comparison.get("status") not in {"ahead", "identical"}:
        return {}, {"status": "unmatched", "date": day, "reason": "业务复盘版本或生产先后关系不符"}
    evidence = {"status": "matched", "date": day, "production_run_id": str(run["id"]),
                "review_run_id": str(rr["id"]), "attempt": rr["run_attempt"], "sha256": digest(value)}
    if value.get("daily", {}).get("valid_statistical_day") is not True:
        evidence.update(status="invalid", reason="复盘未构成有效统计日，不能作为业务恢复或验收")
    return value, evidence


def sync_health(client, policy: dict, directory: Path, *, apply: bool = False) -> dict:
    from .health_loop_sync import GhMetadataClient, sync_main
    if apply:
        from .development_cycle import assert_trusted_checkout
        require(client.budget is not None and client.run_id, "正式健康同步需要已登记的整轮租约")
        assert_trusted_checkout(client)
    directory.mkdir(parents=True, exist_ok=True)
    events = client.events()
    sources = normal_runs(client, WORKFLOW)
    reviews = normal_runs(client, REVIEW_WORKFLOW)
    inputs = {}
    result = {"schema_version": "robtaxi-source-sync-v1", "mode": "apply" if apply else "shadow",
              "complete": False, "source_sync": {"status": "pending"}, "reports": [], "stopped": None}

    def stopping():
        if client.budget and client.budget.remaining() <= 180:
            result["stopped"] = "budget_finalization"
            return True
        return False

    def process(run, health, report, review, review_status):
        out = directory / str(run["id"]); out.mkdir(exist_ok=True)
        metadata = GhMetadataClient(); metadata.transport = client
        metadata.run_guard = client.budget.check if client.budget else None
        evidence = evaluate_health_loop(health, run_report=report)["run_evidence"]
        official = metadata.preflight(run_evidence=evidence)
        while True:
            decision = evaluate_health_loop(health, run_report=report, review=review,
                previous_state=official, state_origin="github_reconstructed")
            decision["business_review"] = review_status
            for row in decision["observations"]:
                if row.get("source_id") and not _source_participated(report, row["source_id"]):
                    row.update(valid=False, source_participated=False)
                    for action in decision["actions"]:
                        if action["incident_key"] == row["incident_key"]:
                            action.update(action="needs_approval", recovery_count=0,
                                          summary="来源未实际参与，证据不足；暂停该问题相关任务")
                            action["action_fingerprint"] = action_fingerprint(action)
            if review:
                for row in decision["observations"]:
                    if not row.get("source_id"):
                        row.update(review_run_id=review_status["review_run_id"], review_sha256=review_status["sha256"],
                                   review_attempt=review_status["attempt"])
                        if review.get("daily", {}).get("valid_statistical_day") is not True:
                            row["valid"] = False
            path = out / "decision.json"; path.write_text(json.dumps(decision, ensure_ascii=False, indent=2))
            args = SimpleNamespace(decision=str(path), mode=result["mode"], out=str(out / "health-sync.json"),
                                   state_out=str(out / "official-state.json"), run_id=client.run_id or "")
            sync_main(args, client)
            receipt = json.loads(Path(args.out).read_text()); official = receipt["official_state"]
            if not apply or not receipt.get("deferred_operations") or not receipt.get("applied") or stopping():
                break
        business_items = [r for r in receipt.get("item_results", []) if not r.get("source_id")]
        review_saved = (bool(review) and review.get("daily", {}).get("valid_statistical_day") is True
            and all(r.get("status") == "synced" for r in business_items)
            and not any(not r.get("source_id") for r in receipt.get("deferred_operations", [])))
        source_complete = receipt.get("source_complete", receipt["complete"])
        return {"production_run_id": str(run["id"]), "attempt": run["run_attempt"],
            "source_status": "synced" if apply and source_complete else "historical" if receipt.get("historical_only") else "planned" if not apply else "partial",
            "source_complete": source_complete, "business_review": review_status, "review_saved": review_saved,
            "decision": str(path), "receipt": args.out}

    # 第一阶段先处理来源；没有任何评审产物也必须能完成来源观察保存。
    for run in sources:
        if stopping(): break
        health, report = source_inputs(client, run)
        if health is None:
            result["reports"].append({"production_run_id": str(run["id"]), "source_status": "missing_artifacts"})
            continue
        run["_date"] = health.get("date_bj"); inputs[run["id"]] = (health, report)
        consumed = any(e.get("event") == "health_queue_updated" and e.get("complete") is True
            and str(e.get("production_run_id")) == str(run["id"])
            and e.get("attempt", 1) == run["run_attempt"] for e in events)
        pending = {"status": "pending", "date": health["date_bj"], "reason": "业务评审独立核验；来源先处理"}
        if consumed and run["id"] != sources[-1]["id"]:
            result["reports"].append({"production_run_id": str(run["id"]), "source_status": "previously_synced",
                                      "source_complete": True, "business_review": pending})
        else:
            result["reports"].append(process(run, health, report, {}, pending))

    # 第二阶段只读取已产生的评审，不轮询等待，不触发生产或模型。
    review_rows = []
    for run in reviews:
        if stopping(): break
        artifacts = pages(client, f"repos/{client.repo}/actions/runs/{run['id']}/artifacts", "artifacts")
        value, _ = artifact_json(client, run, artifacts, "robtaxi-agent-review", "daily_review.json")
        if value: review_rows.append((run, value))
    for run in sources:
        if run["id"] not in inputs or stopping(): continue
        health, report = inputs[run["id"]]
        review, status = review_binding(client, review_rows, run, health["date_bj"], sources)
        row = next(r for r in result["reports"] if r["production_run_id"] == str(run["id"]))
        row["business_review"] = status
        key = "health-review:" + digest(status)
        saved = any(e.get("event") == "health_review_synced" and e.get("key") == key for e in events)
        row["review_saved"] = saved
        if review and not saved:
            updated = process(run, health, report, review, status)
            row.update(updated)
            if apply and updated["review_saved"]:
                # 与逐次观察同一控制记录；仅记评审回执，不赋予任务批准或策略权限。
                client.append({"event": "health_review_synced", "key": key, "run_id": client.run_id,
                    "production_run_id": str(run["id"]), "evidence": status,
                    "at": datetime.now(timezone.utc).isoformat()})
    missing = any(r.get("source_status") in {"missing_artifacts", "partial"} for r in result["reports"])
    result["source_sync"] = {"status": "partial" if missing or result["stopped"] else "complete" if inputs else "pending"}
    result["complete"] = bool(inputs) and not missing and not result["stopped"] and all(
        r.get("business_review", {}).get("status") == "matched" and (not apply or r.get("review_saved"))
        for r in result["reports"])
    return result
