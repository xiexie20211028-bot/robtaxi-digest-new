"""确定性读取正常生产证据；来源同步不等待无关业务复盘。"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

from .development_policy import require, digest
from .development_production import pages, artifact_json, WORKFLOW
from .health_loop import evaluate_health_loop, _source_participated, action_fingerprint

REVIEW_WORKFLOW = "robtaxi-agent-review.yml"


def normal_runs(client, workflow: str) -> list[dict]:
    prefix = f"repos/{client.repo}"
    definition = client.api(f"{prefix}/actions/workflows/{workflow}")
    require(definition.get("path") == f".github/workflows/{workflow}", "证据工作流身份不符")
    runs = pages(client, f"{prefix}/actions/workflows/{definition['id']}/runs?branch=main&event=schedule", "workflow_runs")
    # 一次获取归档目录；过期的历史运行无需逐份请求或下载。
    artifacts = pages(client, f"{prefix}/actions/artifacts", "artifacts")
    by_run = {}
    for artifact in artifacts:
        owner = artifact.get("workflow_run") or {}
        by_run.setdefault(owner.get("id"), []).append(artifact)
    for run in runs:
        run["_artifacts"] = by_run.get(run["id"], [])
    return sorted((r for r in runs if r.get("event") == "schedule" and r.get("head_branch") == "main"
        and r.get("workflow_id") == definition["id"] and r.get("status") == "completed"),
        key=lambda r: (r["created_at"], int(r["id"])))


def source_inputs(client, run: dict) -> tuple[dict | None, dict | None]:
    artifacts = run["_artifacts"] if "_artifacts" in run else pages(client, f"repos/{client.repo}/actions/runs/{run['id']}/artifacts", "artifacts")
    report, report_hash = artifact_json(client, run, artifacts, "robtaxi-notify", "run_report.json")
    health, _ = artifact_json(client, run, artifacts, "robtaxi-health", "health_report.json")
    if health is None or report is None:
        return None, None
    origin = health.get("run", {})
    require(str(origin.get("github_run_id")) == str(run["id"])
            and str(origin.get("github_run_attempt")) == str(run["run_attempt"])
            and origin.get("commit_sha") == run["head_sha"] and origin.get("repository") == client.repo
            and origin.get("event_name") == "schedule"
            and health.get("source_report", {}).get("available") is True
            and health["source_report"].get("sha256") == report_hash,
            "健康报告的生产身份或摘要不可信，停止正式同步")
    require(not any(f.get("check_id") == "self_check_internal_failure" for f in health.get("findings", [])
                    if isinstance(f, dict)), "健康报告自身失败，停止正式同步")
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
    inputs = {}
    result = {"schema_version": "robtaxi-source-sync-v1", "mode": "apply" if apply else "shadow",
              "complete": False, "source_sync": {"status": "pending"}, "reports": [], "stopped": None}

    def checkpoint(stage, run=None):
        # 本地进度仅用于诊断，消费决定始终来自 GitHub 正式证据。
        snapshot = {**result, "stage": stage, "current_run": run and str(run["id"]),
                    "at": datetime.now(timezone.utc).isoformat()}
        temp = directory / "progress.pending"
        temp.write_text(json.dumps(snapshot, ensure_ascii=False))
        temp.replace(directory / "progress.json")
        print(f"[sync-health] {stage} production={snapshot['current_run'] or '—'}", file=sys.stderr, flush=True)

    checkpoint("读取正式消费记录")
    events = client.events()
    consumed = {(str(e.get("production_run_id")), int(e.get("attempt", 1))): e for e in events
                if (e.get("event") == "source_report_consumed" or
                    (e.get("event") == "health_queue_updated" and e.get("complete") is True))
                and e.get("production_date") and e.get("global_state_valid") is True}
    checkpoint("读取生产运行目录")
    sources = normal_runs(client, WORKFLOW)

    def stopping():
        if client.budget and client.budget.remaining() <= 180:
            result["stopped"] = "budget_finalization"
            return True
        return False

    def process(run, health, report, review, review_status):
        out = directory / str(run["id"]) / str(run["run_attempt"]) / ("review" if review else "source")
        out.mkdir(parents=True, exist_ok=True)
        metadata = GhMetadataClient(); metadata.transport = client
        metadata.run_guard = client.budget.check if client.budget else None
        evidence = evaluate_health_loop(health, run_report=report)["run_evidence"]
        checkpoint("核验正式事故记录", run)
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
            checkpoint("保存逐项观察", run)
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
        proof = consumed.get((str(run['id']), run['run_attempt']))
        row = {"production_run_id": str(run['id']), "attempt": run['run_attempt']}
        result['reports'].append(row)
        if proof and run is not sources[-1]:
            run['_date'] = proof['production_date']
            row.update(source_status='previously_synced', source_complete=True)
            checkpoint('已核对来源回执', run)
            continue
        checkpoint('下载来源产物', run)
        health, report = source_inputs(client, run)
        if health is None:
            names = {f"{prefix}-{run['id']}-{run['run_attempt']}" for prefix in ("robtaxi-notify", "robtaxi-health")}
            expired = any(a.get("expired") and a.get("name") in names for a in run.get("_artifacts", []))
            row.update(source_status="expired_artifacts" if expired else "missing_artifacts", source_complete=False)
            checkpoint('来源产物缺失或过期', run)
            continue
        run['_date'] = health['date_bj']
        inputs[run['id']] = (health, report)
        pending = {'status': 'pending', 'date': health['date_bj'], 'reason': '来源先处理，业务复盘独立核验'}
        row.update(process(run, health, report, {}, pending))
        if apply and row['source_complete'] and not proof:
            receipt = json.loads(Path(row['receipt']).read_text())
            if receipt.get('global_state_valid') is True:
                client.append({'event': 'source_report_consumed', 'key': f"source-consumed:{run['id']}:{run['run_attempt']}",
                    'production_run_id': str(run['id']), 'attempt': run['run_attempt'], 'run_id': client.run_id,
                    'production_date': health['date_bj'], 'global_state_valid': True,
                    'decision_digest': receipt['decision_digest'], 'at': datetime.now(timezone.utc).isoformat()})
        checkpoint('已处理来源', run)

    checkpoint('读取业务复盘目录')
    reviews = [] if stopping() else normal_runs(client, REVIEW_WORKFLOW)
    review_rows = []
    for run in reviews:
        if stopping(): break
        artifacts = run["_artifacts"] if "_artifacts" in run else pages(client, f"repos/{client.repo}/actions/runs/{run['id']}/artifacts", "artifacts")
        value, _ = artifact_json(client, run, artifacts, "robtaxi-agent-review", "daily_review.json")
        if value: review_rows.append((run, value))
    for run in sources:
        if not run.get("_date") or stopping(): continue
        review, status = review_binding(client, review_rows, run, run["_date"], sources)
        row = next(r for r in result["reports"] if r["production_run_id"] == str(run["id"]))
        row["business_review"] = status
        key = "health-review:" + digest(status)
        saved = any(e.get("event") == "health_review_synced" and e.get("key") == key for e in events)
        row["review_saved"] = saved
        if review and not saved:
            checkpoint("关联业务复盘", run)
            health, report = inputs[run["id"]] if run["id"] in inputs else source_inputs(client, run)
            if health is None:
                row["business_review"] = {"status": "unmatched", "reason": "原生产产物已不可用"}
                continue
            updated = process(run, health, report, review, status)
            row.update(updated)
            if apply and updated["review_saved"]:
                # 与逐次观察同一控制记录；仅记评审回执，不赋予任务批准或策略权限。
                client.append({"event": "health_review_synced", "key": key, "run_id": client.run_id,
                    "production_run_id": str(run["id"]), "evidence": status,
                    "at": datetime.now(timezone.utc).isoformat()})
    missing = any(r.get("source_status") in {"missing_artifacts", "expired_artifacts", "partial"} for r in result["reports"])
    result["source_sync"] = {"status": "partial" if missing or result["stopped"] else "complete" if inputs else "pending"}
    result["complete"] = bool(inputs) and not missing and not result["stopped"] and all(
        r.get("business_review", {}).get("status") == "matched" and (not apply or r.get("review_saved"))
        for r in result["reports"])
    checkpoint("完成")
    return result
