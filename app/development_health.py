"""确定性读取正常生产证据；来源同步不等待无关业务复盘。"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from types import SimpleNamespace

from .development_policy import require, digest
from .development_production import pages, artifact_json, WORKFLOW
from .health_evidence import matches_production
from .health_loop import evaluate_health_loop

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
                "production_attempt": run["run_attempt"], "production_sha": run["head_sha"],
                "review_run_id": str(rr["id"]), "attempt": rr["run_attempt"],
                "review_sha": rr["head_sha"], "sha256": digest(value)}
    if value.get("daily", {}).get("valid_statistical_day") is not True:
        evidence.update(status="invalid", reason="复盘未构成有效统计日，不能作为业务恢复或验收")
    return value, evidence


def sync_health(client, policy: dict, directory: Path, *, apply: bool = False, phase: str = "all") -> dict:
    from .health_loop_sync import GhMetadataClient, sync_main
    require(phase in {"all", "current", "maintenance"}, "无效来源同步阶段")
    if apply:
        from .development_cycle import assert_trusted_checkout
        require(client.budget is not None and client.run_id, "正式健康同步需要已登记的整轮租约")
        assert_trusted_checkout(client)
    directory.mkdir(parents=True, exist_ok=True)
    inputs = {}
    result = {"schema_version": "robtaxi-source-sync-v1", "mode": "apply" if apply else "shadow",
              "phase": phase, "complete": False, "source_sync": {"status": "pending"}, "reports": [], "stopped": None}

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
    maintenance_deadline = time.monotonic() + 300
    sources = normal_runs(client, WORKFLOW)

    def stopping():
        if client.budget and client.budget.remaining() <= 180:
            result["stopped"] = "budget_finalization"
            return True
        if phase == "maintenance" and (time.monotonic() >= maintenance_deadline or
                (client.budget and client.budget.remaining() < 900)):
            result["stopped"] = "maintenance_yield"
            return True
        return False

    metadata = GhMetadataClient()
    metadata.transport = client

    def process(run, health, report, review, review_status):
        metadata.run_guard = client.budget.check if client.budget else None
        out = directory / str(run["id"]) / str(run["run_attempt"]) / ("review" if review else "source")
        out.mkdir(parents=True, exist_ok=True)
        evidence = evaluate_health_loop(health, run_report=report)["run_evidence"]
        checkpoint("核验正式事故记录", run)
        official = metadata.preflight(run_evidence=evidence)
        while True:
            decision = evaluate_health_loop(health, run_report=report, review=review,
                review_binding=review_status if review else None, previous_state=official, state_origin="github_reconstructed")
            decision["business_review"] = review_status
            path = out / "decision.json"; path.write_text(json.dumps(decision, ensure_ascii=False, indent=2))
            args = SimpleNamespace(decision=str(path), mode=result["mode"], out=str(out / "health-sync.json"),
                                   state_out=str(out / "official-state.json"), run_id=client.run_id or "")
            checkpoint("保存逐项观察", run)
            sync_main(args, client, metadata=metadata)
            receipt = json.loads(Path(args.out).read_text()); official = receipt["official_state"]
            if not apply or not receipt.get("deferred_operations") or not receipt.get("applied") or stopping():
                break
        business_items = [r for r in receipt.get("item_results", []) if r["incident_key"].startswith("review:")]
        expected_business = {r["incident_key"] for r in decision["observations"] if r["incident_key"].startswith("review:")}
        synced_business = {r["incident_key"] for r in business_items if r.get("status") == "synced"}
        business_written = (apply and receipt.get("global_state_valid") is True
            and isinstance(receipt.get("item_results"), list) and expected_business <= synced_business
            and all(r.get("status") == "synced" for r in business_items)
            and not any(r["incident_key"].startswith("review:") for r in receipt.get("deferred_operations", [])))
        valid_day = review.get("daily", {}).get("valid_statistical_day")
        source_complete = receipt.get("source_complete", receipt["complete"])
        return {"production_run_id": str(run["id"]), "attempt": run["run_attempt"],
            "source_status": "synced" if apply and source_complete else "historical" if receipt.get("historical_only") else "planned" if not apply else "partial",
            "source_complete": source_complete, "business_review": review_status,
            "review_saved": business_written and valid_day is True,
            "review_invalid_checked": business_written and valid_day is False,
            "decision": str(path), "receipt": args.out}

    # 第一阶段先处理来源；没有任何评审产物也必须能完成来源观察保存。
    for run in sources[-1:] if phase == "current" else sources:
        if stopping(): break
        proof = consumed.get((str(run['id']), run['run_attempt']))
        row = {"production_run_id": str(run['id']), "attempt": run['run_attempt']}
        result['reports'].append(row)
        if phase == "current" and apply:
            # 缓存丢失即可重建。相同运行的正式队列、生产版本和内容摘要必须同时匹配。
            previous = next((e for e in reversed(events) if e.get("event") == "health_queue_updated"
                and e.get("run_id") == client.run_id and str(e.get("production_run_id")) == str(run["id"])
                and e.get("attempt", 1) == run["run_attempt"] and e.get("global_state_valid") is True), None)
            out = directory / str(run["id"]) / str(run["run_attempt"]) / "source"
            try:
                decision = json.loads((out / "decision.json").read_text())
                saved = json.loads((out / "health-sync.json").read_text())
            except (OSError, ValueError):
                decision, saved = {}, {}
            if (previous and saved.get("source_complete") is True and saved.get("development_run_id") == client.run_id
                    and decision.get("run_evidence", {}).get("commit_sha") == run["head_sha"]
                    and saved.get("decision_digest") == digest(decision) == previous["decision_digest"]):
                row.update(source_status="synced", source_complete=True, decision=str(out / "decision.json"),
                           receipt=str(out / "health-sync.json"))
                continue
        if proof and run is not sources[-1]:
            run['_date'] = proof['production_date']
            row.update(source_status='previously_synced', source_complete=True)
            checkpoint('已核对来源回执', run)
            continue
        if phase == "maintenance":
            if proof:
                run["_date"] = proof["production_date"]
                row.update(source_status="previously_synced", source_complete=True)
                continue
            names = {f"{prefix}-{run['id']}-{run['run_attempt']}" for prefix in ("robtaxi-notify", "robtaxi-health")}
            available = {a["name"] for a in run.get("_artifacts", []) if not a.get("expired")}
            if not names <= available or inputs:
                row.update(source_status="deferred", source_complete=False)
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

    if phase == "current":
        latest = result["reports"][-1] if result["reports"] else {}
        result["source_sync"] = {"status": latest.get("source_status", "pending")}
        result["business_review"] = {"status": "deferred", "reason": "当前来源已交给研发队列，历史复盘分批处理"}
        checkpoint("当前来源交接")
        return result

    checkpoint('读取业务复盘目录')
    reviews = [] if stopping() else normal_runs(client, REVIEW_WORKFLOW)
    review_rows = []
    index = {e["key"]: e for e in events if e.get("event") == "review_artifact_indexed"}
    indexed_count = 0
    incomplete_index = False
    for run in reviews:
        if stopping(): break
        artifacts = run["_artifacts"] if "_artifacts" in run else pages(client, f"repos/{client.repo}/actions/runs/{run['id']}/artifacts", "artifacts")
        if phase == "maintenance":
            name = f"robtaxi-agent-review-{run['id']}-{run['run_attempt']}"
            available = [a for a in artifacts if not a.get("expired") and a["name"] == name]
            if not available:
                continue
            if len(available) != 1:
                result["stopped"] = "ambiguous_review_archive"
                checkpoint("同一复盘存在多份有效归档，业务关联待核对", run)
                return result
            # GitHub 产物 ID 不可变，替换归档或重跑会得到新的索引键。
            key = f"review-archive:{run['id']}:{run['run_attempt']}:{run['head_sha']}:{available[0]['id']}"
            entry = index.get(key)
            if not entry:
                if indexed_count >= 3:
                    incomplete_index = True
                    continue
                value, _ = artifact_json(client, run, artifacts, "robtaxi-agent-review", "daily_review.json")
                if not isinstance(value, dict) or not value.get("daily", {}).get("date"):
                    result["stopped"] = "invalid_review_archive"
                    checkpoint("复盘归档日期待核对", run)
                    return result
                entry = {"event": "review_artifact_indexed", "key": key, "date": value["daily"]["date"],
                         "review_digest": digest(value), "run_id": client.run_id,
                         "at": datetime.now(timezone.utc).isoformat()}
                if apply:
                    client.append(entry)
                indexed_count += 1
            review_rows.append((run, {"daily": {"date": entry["date"]}, "_index_digest": entry["review_digest"]}))
            continue
        value, _ = artifact_json(client, run, artifacts, "robtaxi-agent-review", "daily_review.json")
        if value: review_rows.append((run, value))
    if phase == "maintenance" and (incomplete_index or stopping()):
        result["stopped"] = result["stopped"] or "review_index_in_progress"
        checkpoint("复盘索引接续")
        return result
    review_sources = [*sources[-1:], *sources[:-1]] if phase == "maintenance" else sources
    for run in review_sources:
        if not run.get("_date") or stopping(): continue
        if phase == "maintenance":
            required = {f"{prefix}-{run['id']}-{run['run_attempt']}" for prefix in ("robtaxi-notify", "robtaxi-health")}
            if not required <= {a["name"] for a in run.get("_artifacts", []) if not a.get("expired")}:
                continue
            matching = [(rr, v) for rr, v in review_rows if v["daily"]["date"] == run["_date"]]
            if len(matching) != 1:
                continue
            rr, indexed = matching[0]
            matching_sources = sorted((r["id"], r["run_attempt"], r["head_sha"]) for r in sources
                if (r.get("_date") or datetime.fromisoformat(r["created_at"].replace("Z", "+00:00"))
                    .astimezone(timezone(timedelta(hours=8))).date().isoformat()) == run["_date"])
            skip_key = "review-unmatched:" + digest([run["_date"], matching_sources, rr["id"],
                rr["run_attempt"], rr["head_sha"], indexed["_index_digest"]])
            if any(e.get("event") == "review_archive_unmatched" and e.get("key") == skip_key for e in events):
                continue
            # 正式回执覆盖同一生产 attempt、复盘内容和版本后，跳过归档下载。
            checked = any(e.get("event") in {"health_review_synced", "health_review_invalid_checked"}
                and (proof := e.get("evidence", {})).get("production_run_id") == str(run["id"])
                and proof.get("production_attempt") == run["run_attempt"] and proof.get("production_sha") == run["head_sha"]
                and proof.get("review_run_id") == str(rr["id"]) and proof.get("attempt") == rr["run_attempt"]
                and proof.get("review_sha") == rr["head_sha"] and proof.get("sha256") == indexed["_index_digest"] for e in events)
            if checked:
                continue
            value, _ = artifact_json(client, rr, rr["_artifacts"], "robtaxi-agent-review", "daily_review.json")
            require(value is not None and digest(value) == indexed["_index_digest"], "复盘与正式索引摘要不符")
            review_rows = [(rr, value)]
        review, status = review_binding(client, review_rows, run, run["_date"], sources)
        row = next(r for r in result["reports"] if r["production_run_id"] == str(run["id"]))
        row["business_review"] = status
        if not review:
            row["review_saved"] = False
            if phase == "maintenance":
                if apply:
                    client.append({"event": "review_archive_unmatched", "key": skip_key, "run_id": client.run_id,
                        "production_run_id": str(run["id"]), "reason": status["reason"],
                        "at": datetime.now(timezone.utc).isoformat()})
                break
            continue
        invalid = status["status"] == "invalid"
        event_name = "health_review_invalid_checked" if invalid else "health_review_synced"
        key = ("invalid-review:" if invalid else "health-review:") + digest(status)
        keys = {key}
        # 旧成功回执缺生产 attempt，仅当当前仍是首次生产时能证明对应关系。
        if not invalid and run["run_attempt"] == 1:
            legacy = {k: v for k, v in status.items() if k not in {"production_attempt", "production_sha", "review_sha"}}
            keys.add("health-review:" + digest(legacy))
        saved = any(e.get("event") == event_name and e.get("key") in keys for e in events)
        row["review_saved"] = saved and not invalid
        row["review_invalid_checked"] = saved and invalid
        if not saved:
            checkpoint("关联业务复盘", run)
            health, report = inputs[run["id"]] if run["id"] in inputs else source_inputs(client, run)
            if health is None:
                row["business_review"] = {"status": "unmatched", "reason": "原生产产物已不可用"}
                if phase == "maintenance":
                    break
                continue
            updated = process(run, health, report, review, status)
            row.update(updated)
            if apply and (updated["review_saved"] or updated["review_invalid_checked"]):
                # 无效统计日只记录已核对这个指纹，不能恢复事故或满足业务验收。
                client.append({"event": event_name, "key": key, "run_id": client.run_id,
                    "production_run_id": str(run["id"]), "evidence": status,
                    "at": datetime.now(timezone.utc).isoformat()})
        if phase == "maintenance":
            break
    missing = any(r.get("source_status") in {"missing_artifacts", "expired_artifacts", "partial"} for r in result["reports"])
    result["source_sync"] = {"status": "partial" if missing or result["stopped"] else "complete" if inputs else "pending"}
    result["complete"] = phase == "all" and bool(inputs) and not missing and not result["stopped"] and all(
        r.get("business_review", {}).get("status") == "matched" and (not apply or r.get("review_saved"))
        for r in result["reports"])
    checkpoint("完成")
    return result
