"""由正式状态生成研发结论；巡检完成、事故恢复与工程验收分别报告。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re

from .development_budget import pending_deliveries
from .development_policy import DevelopmentError, digest, require, timestamp

BEIJING = timezone(timedelta(hours=8))
SCHEMA = "robtaxi-development-outcome-v1"
PROGRESS_STAGES = {"planned", "code_changed", "tests_passed", "pr_created"}


def evidence_order(origin: dict, previous: dict) -> int:
    """生产日期优先；旧格式缺日期时按生产运行和attempt比较，不能按补录时间。"""
    day, old_day = str(origin.get("date") or "")[:10], str(previous.get("date") or "")[:10]
    if day and old_day and day != old_day:
        return 1 if day > old_day else -1
    def version(row):
        run = str(row.get("run_id") or "")
        return (int(run) if run.isdigit() else 0, int(row.get("attempt") or 1))
    current, old = version(origin), version(previous)
    return (current > old) - (current < old)


def recover_incidents(events: list[dict], official: dict | None = None) -> dict:
    """正式观察优先；缺输入时只保留可信异常，不能从空快照推断零事故。"""
    from .development_budget import queue_entry_verified
    from .health_history import canonical_history
    incidents, origins = {}, {}
    for queue in sorted((e for e in events if e.get("event") == "health_queue_updated"),
                        key=lambda e: (int(e.get("production_run_id", 0)), int(e.get("attempt", 1)))):
        for row in queue.get("entries", []):
            key = row.get("incident_key")
            if not key or not queue_entry_verified(queue, row):
                continue
            # 旧队列没有有效恢复证明；healthy/unknown不能清除已知critical。
            if row.get("severity") in {"warning", "error", "critical"}:
                origin = {"kind": "health_queue", "run_id": queue.get("production_run_id"),
                          "date": queue.get("production_date"), "attempt": queue.get("attempt", 1),
                          "url": queue.get("_comment_url")}
                if key in origins and evidence_order(origin, origins[key]) < 0:
                    continue
                incidents[key] = {**row, "lifecycle": "open"}
                origins[key] = origin
    for key, raw in (official or {}).get("incidents", {}).items():
        rows = canonical_history(raw.get("observations", []))
        effective = [r for r in rows if r.get("valid") and r.get("result") in {"failed", "healthy"}]
        if effective:
            latest = effective[-1]
            origin = {"kind": "official_observation", "run_id": latest["run_id"],
                      "date": latest.get("run_at"), "attempt": latest["attempt"]}
            order = evidence_order(origin, origins.get(key, {}))
            if key in incidents and (order < 0 or (order == 0 and latest["result"] == "healthy")):
                continue
            incidents[key] = {**raw, "incident_key": key, "severity": latest["severity"],
                              "lifecycle": "open" if latest["result"] == "failed" else "observing"}
            origins[key] = origin
        elif raw.get("severity") in {"warning", "error", "critical"} and key not in incidents:
            incidents[key] = {**raw, "incident_key": key, "lifecycle": "open"}
            origins[key] = {"kind": "official_state", "run_id": raw.get("last_run_id")}
    return {"incidents": incidents, "origins": origins, "complete": bool((official or {}).get("complete"))}


def progress_evidence(tasks: list[dict], events: list[dict], run_started: dict, now: datetime) -> list[dict]:
    """只统计本轮绑定动作；GitHub实际人工合并单列，快照差异不是开发证据。"""
    start, run_id = timestamp(run_started["at"]), run_started["run_id"]
    sha = lambda value: bool(re.fullmatch(r"[0-9a-f]{40}", str(value or "")))
    records, seen = [], set()
    prior = [e["result"] for e in events if e.get("event") == "run_outcome" and e.get("result", {}).get("run_id") != run_id]
    for task in tasks:
        number = task["number"]
        history = task.get("events", [])
        def identity(e):
            return digest([number, e.get("event"), e.get("stage"), e.get("head_sha"), e.get("merge_sha"),
                           e.get("contract_digest") or (digest(e["contract"]) if e.get("contract") else None),
                           e.get("adoption_key")])
        old = {identity(e) for e in history if e.get("at") and timestamp(e["at"]) < start}
        for e in history:
            kind = e.get("event"); at = e.get("at")
            if not at or not start <= timestamp(at) <= now:
                continue
            mode, valid = "automatic", False
            bound = e.get("run_id") == run_id
            contract_digest = e.get("contract_digest")
            if kind == "contract":
                valid = bound and bool(e.get("claim_key")) and isinstance(e.get("contract"), dict)
                valid = valid and digest(e["contract"]) == task.get("contract_digest")
            elif kind == "checkpoint" and e.get("stage") in PROGRESS_STAGES:
                valid = bound and sha(e.get("head_sha")) and bool(e.get("claim_key")) and bool(contract_digest)
                valid = valid and contract_digest == task.get("contract_digest")
                if any((e.get("stage"), e.get("head_sha")) in [tuple(v) for v in r.get("task_versions", {}).get(str(number), {}).get("checkpoint", [])] for r in prior):
                    valid = False
            elif kind == "review":
                valid = bound and sha(e.get("head_sha")) and sha(e.get("base_sha")) and bool(e.get("evidence")) and contract_digest == task.get("contract_digest")
            elif kind == "delivery":
                reserved = any(r.get("event") == "reserve" and r.get("kind") == "merge" and r.get("key") == e.get("key")
                               and r.get("at") and timestamp(r["at"]) >= start for r in events)
                valid = (bound or reserved) and sha(e.get("merge_sha")) and sha(e.get("head_sha")) and bool(contract_digest)
            elif kind in {"production_verified", "historical_production_verified"}:
                mode = "acceptance"
                if kind == "historical_production_verified":
                    valid = any(a.get("event") == "historical_delivery_adopted" and a.get("key") == e.get("adoption_key")
                                and a.get("issue") == number and a.get("facts", {}).get("merge_sha") == e.get("merge_sha") for a in history)
                else:
                    valid = any(d.get("event") == "delivery" and d.get("merge_sha") == e.get("merge_sha")
                                and d.get("contract_digest") == contract_digest for d in history)
                valid = valid and sha(e.get("merge_sha"))
            token = identity(e)
            if valid and token not in seen and token not in old:
                seen.add(token)
                records.append({"issue": number, "type": mode, "stage": e.get("stage") or kind,
                    "key": e.get("key"), "head_sha": e.get("head_sha"), "merge_sha": e.get("merge_sha"),
                    "contract_digest": contract_digest, "at": at, "url": e.get("_comment_url")})
        pr = task.get("awaiting_production") or {}
        merged_at = pr.get("mergedAt")
        merge = (pr.get("mergeCommit") or {}).get("oid")
        day = now.astimezone(BEIJING).date()
        if merged_at and sha(merge) and timestamp(merged_at).astimezone(BEIJING).date() == day and timestamp(merged_at) <= now:
            if not any(r["issue"] == number and r.get("merge_sha") == merge and r["type"] == "automatic" for r in records):
                records.append({"issue": number, "type": "manual" if task.get("manual_merge_verified") else "external", "stage": "merged", "pr": pr.get("number"),
                    "merge_sha": merge, "at": merged_at, "url": pr.get("url")})
    return records


def production_deadline(workflow: str, now: datetime) -> datetime:
    schedules = re.findall(r"cron:\s*[\"'](\d+)\s+(\d+)\s+\*\s+\*\s+\*[\"']", workflow)
    require(len(schedules) == 1, "生产计划不是单一每日时间，不能猜测证据截止时间")
    minute, hour = map(int, schedules[0])
    require(0 <= minute < 60 and 0 <= hour < 24, "生产计划时间无效")
    local = now.astimezone(BEIJING)
    scheduled = datetime.combine(local.date(), datetime.min.time(), BEIJING)
    utc_clock = datetime(2000, 1, 1, hour, minute, tzinfo=timezone.utc).astimezone(BEIJING)
    return scheduled.replace(hour=utc_clock.hour, minute=utc_clock.minute) + timedelta(hours=2)


def task_versions(tasks: list[dict]) -> dict:
    result = {}
    for task in tasks:
        history = task.get("events", [])
        checkpoints = [e for e in history if e.get("event") == "checkpoint" and e.get("stage") in PROGRESS_STAGES
                       and ((e.get("stage") == "planned" and task.get("contract_digest"))
                            or re.fullmatch(r"[0-9a-f]{40}", str(e.get("head_sha", ""))))]
        result[str(task["number"])] = {
            "contract": task.get("contract_digest"),
            "head": (task.get("open_pr") or {}).get("headRefOid"),
            "merge": (task.get("awaiting_production") or {}).get("mergeCommit", {}).get("oid"),
            "verified": sorted({e.get("key") for e in history if e.get("event") in {"production_verified", "historical_production_verified"} and e.get("key")}),
            "reviews": sorted({digest({k: e.get(k) for k in ("head_sha", "base_sha", "contract_digest", "verdict")})
                               for e in history if e.get("event") == "review"}),
            "checkpoint": [(e.get("stage"), e.get("head_sha")) for e in checkpoints[-1:]],
        }
    return result


def decision_state(events: list[dict], requests: list[dict], now: datetime) -> list[dict]:
    pending = {e["decision_id"]: dict(e) for e in events if e.get("event") == "decision_required"}
    resolved = {e.get("decision_id") for e in events if e.get("event") == "decision_resolved"
                and e.get("resolution") in {"approved", "rejected", "cancelled", "implemented"}}
    for row in requests:
        pending.setdefault(row["decision_id"], {"event": "decision_required", "at": now.isoformat(), **row})
    result = []
    for key, row in pending.items():
        if key in resolved:
            continue
        age = max(0, (now - timestamp(row["at"])).total_seconds())
        index = 0 if age < 86400 else 1 + int((age - 86400) // (72 * 3600))
        previous = max((e.get("reminder_index", 0) for e in events if e.get("event") == "decision_reminder"
                        and e.get("decision_id") == key), default=0)
        result.append({**row, "reminder_index": index, "reminder_due": index > previous})
    return result


def delivery_status(tasks: list[dict], events: list[dict], *, inventory_complete: bool) -> dict:
    """控制记录按交付身份核验。合并快照仅用于指出还需对账的 PR。"""
    registered = set()
    review = set()
    unregistered_prs = []
    usable = []
    for event in events:
        kind = event.get("event")
        if kind in {"historical_delivery_adopted", "delivery"}:
            facts = event.get("facts", {}) if kind == "historical_delivery_adopted" else event
            issue = event.get("issue")
            if (not isinstance(facts, dict) or not issue or not facts.get("merge_sha")
                    or not isinstance(facts.get("production"), dict) or not facts["production"].get("kind")
                    or not (event.get("key") if kind == "historical_delivery_adopted" else event.get("contract_digest"))):
                if issue:
                    review.add(issue)
                inventory_complete = False
                continue
            registered.add((issue, facts["merge_sha"], facts.get("pr")))
        usable.append(event)
    for task in tasks:
        pr = task.get("awaiting_production") or {}
        if pr or task.get("delivery_adoption_required"):
            identity = (task["number"], (pr.get("mergeCommit") or {}).get("oid"), pr.get("number"))
            if identity not in registered:
                review.add(task["number"])
                unregistered_prs.append({"issue": identity[0], "merge_sha": identity[1], "pr": identity[2]})
    pending = pending_deliveries(usable)
    return {"waiting_production": sorted({d["issue"] for d in pending}),
            "delivery_review_needed": sorted(review), "unregistered_delivery_prs": unregistered_prs,
            "delivery_inventory_complete": inventory_complete,
            "pending_delivery_evidence": [{k: d.get(k) for k in ("issue", "merge_sha", "contract_digest", "adoption_key")}
                                          for d in pending]}


def build_outcome(*, policy: dict, state: dict, decision: dict, receipt: dict, events: list[dict],
                  run_started: dict, now: datetime, workflow: str, trigger: str = "manual", pilot_task: dict | None = None) -> dict:
    run_id = run_started["run_id"]
    day = now.astimezone(BEIJING).date().isoformat()
    complete = (receipt.get("complete") is True and receipt.get("mode") == "apply"
                and receipt.get("development_run_id") == run_id and receipt.get("official_state", {}).get("complete") is True
                and receipt.get("decision_digest") == digest(decision)
                and decision.get("run_evidence", {}).get("health_report_available") is True
                and decision.get("run_evidence", {}).get("health_report_valid") is not False)
    partial = (not complete and receipt.get("global_state_valid") is True and receipt.get("mode") == "apply"
               and receipt.get("development_run_id") == run_id and receipt.get("decision_digest") == digest(decision)
               and any(i.get("status") == "synced" for i in receipt.get("item_results", [])))
    source_complete = complete or (receipt.get("source_complete") is True and receipt.get("mode") == "apply"
        and receipt.get("development_run_id") == run_id and receipt.get("decision_digest") == digest(decision)
        and receipt.get("global_state_valid") is True)
    review_status = receipt.get("business_review", decision.get("business_review", {}))
    if complete and review_status.get("status") in {"pending", "unmatched", "invalid"}:
        complete, partial = False, True
    partial = partial or (source_complete and not complete)
    deadline = production_deadline(workflow, now)
    current_data = decision.get("date_bj") == day and decision.get("run_evidence", {}).get("event_name") == "schedule"
    evidence_status = "current" if current_data else "waiting" if now < deadline else "overdue"
    recovered = state.get("health_context") or recover_incidents(events, receipt.get("official_state"))
    known = dict(recovered.get("incidents", {}))
    # 本轮未经保存的healthy声明不能清除先前已知critical。
    for key, row in receipt.get("official_state", {}).get("incidents", {}).items():
        if key not in known:
            known[key] = row
    rank = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3, "critical": 4}
    for key, row in decision.get("incidents", {}).items():
        # 未保存的判断只补充更严重异常；恢复只能来自按生产顺序核实的正式观察。
        if key not in known or rank.get(row.get("severity"), 0) > rank.get(known[key].get("severity"), 0):
            known[key] = row
    incidents = list(known.values())
    # 历史已恢复记录不能继续充当critical；观察期工程验收单独呈现。
    active = [r for r in incidents if r.get("lifecycle") not in {"closed", "observing"} and r.get("severity") in {"warning", "error", "critical"}]
    rank = {"unknown": 0, "healthy": 1, "warning": 2, "error": 3, "critical": 4}
    severity = max((r.get("severity", "unknown") for r in active), key=lambda s: rank.get(s, 0),
                   default="healthy" if current_data and complete else "unknown")
    requests = [{"decision_id": r["incident_key"], "owner": policy["project_owner"],
                 "reason": r.get("summary", r.get("reason_code", "")), "severity": r.get("severity", "critical")}
                for r in decision.get("actions", []) if r.get("action") == "needs_approval"]
    pilot_complete = policy.get("mode") == "pilot" and pilot_task and pilot_task.get("state") == "CLOSED"
    if pilot_complete:
        requests.append({"decision_id": "pilot_complete_awaiting_activation", "owner": policy["project_owner"],
                         "reason": "试点已完成，合格任务全队列开放待处理", "severity": "warning"})
    pending = decision_state(events, requests, now)
    tasks = []
    for task in state.get("tasks", []):
        history = [*task.get("events", []), *(e for e in events if e.get("issue") == task["number"])]
        merged = {e.get("key") or digest(e): e for e in history}
        tasks.append({**task, "events": list(merged.values())})
    versions = task_versions(tasks)
    prior = [e["result"] for e in events if e.get("event") == "run_outcome" and e.get("result", {}).get("trigger") == "schedule"
             and e["result"].get("day", "") < day]
    previous = max(prior, key=lambda r: (r["day"], r["finished_at"]), default={})
    evidence = progress_evidence(tasks, events, run_started, now)
    progress = sorted({e["issue"] for e in evidence if e["type"] in {"automatic", "acceptance"}})
    merged_issues = sorted({e["issue"] for e in evidence if e["type"] == "automatic" and e["stage"] == "delivery"})
    verified_issues = sorted({e["issue"] for e in evidence if e["type"] == "acceptance"})
    developed_issues = sorted({e["issue"] for e in evidence if e["type"] == "automatic" and e["stage"] == "code_changed"})
    eligible = bool(state.get("next_action")) and not pilot_complete
    deliveries = delivery_status(tasks, events, inventory_complete=state.get("delivery_inventory_complete", True))
    waiting = deliveries["waiting_production"]
    stalled_count = 0 if progress or not eligible or not (complete or partial) else int(previous.get("stalled_count", 0)) + 1
    if not complete and not partial:
        conclusion = "inspection_incomplete"
    elif evidence_status == "overdue":
        conclusion = "evidence_overdue"
    elif progress:
        conclusion = "progress"
    elif evidence_status == "waiting":
        conclusion = "waiting_production"
    elif pilot_complete or pending:
        conclusion = "needs_approval"
    elif stalled_count >= 2:
        conclusion = "stalled"
    elif partial:
        conclusion = "inspection_partial"
    elif not eligible and waiting:
        conclusion = "waiting_production"
    elif eligible:
        conclusion = "no_progress"
    else:
        conclusion = "no_eligible_task"
    receipts = {}
    for event in events:
        if event.get("event") == "notification_receipt" and event.get("notification_key") == f"notification:{digest(f'daily:{day}')}":
            receipts[event["channel"]] = event
    reason = ("已有可验证阶段进展" if progress else "巡检证据或回执不完整" if not complete else
              "本轮已进入预算收尾" if state.get("budget_seconds_remaining", 99999) is not None and state.get("budget_seconds_remaining", 99999) <= 120 else
              "试点完成，合格队列开放待处理" if pilot_complete else "存在合格任务但没有新阶段证据" if eligible else
              "已有交付等待原验收" if waiting else "；".join(dict.fromkeys(e["reason"] for e in state.get("excluded", [])))[:250] or "当前没有通过门禁的任务")
    return {"schema_version": SCHEMA, "result_revision": 3, "run_id": run_id, "trigger": trigger, "day": day,
            "recovered_runs": [{k: e.get(k) for k in ("run_id", "reason", "last_stage", "last_evidence", "production_run_id", "next_action")}
                               for e in events if e.get("event") == "run_expired" and e.get("recovered_by") == run_id],
            "started_at": run_started["at"], "finished_at": now.isoformat(), "conclusion": conclusion,
            "inspection": "complete" if complete else "partial" if partial else "incomplete", "evidence_status": evidence_status,
            "blocked_incidents": receipt.get("blocked_incidents", []), "review_available": decision.get("run_evidence", {}).get("review_available"),
            "production_run_id": decision.get("run_id"), "production_date": decision.get("date_bj"),
            "production_commit": decision.get("run_evidence", {}).get("commit_sha"), "production_deadline": deadline.isoformat(),
            "health_severity": severity, "active_incidents": [{"key": r.get("incident_key"), "severity": r.get("severity")} for r in active],
            "health_evidence_status": "current" if current_data and source_complete else "last_known" if active else "unknown",
            "incident_count_known": bool(complete or recovered.get("complete")),
            "health_evidence_origins": recovered.get("origins", {}),
            "source_inspection": "complete" if source_complete else "partial" if partial else "incomplete",
            "business_review": review_status or {"status": "unknown"},
            "progress_evidence": evidence,
            "manual_deliveries": [e for e in evidence if e["type"] in {"manual", "external"}],
            "requires_attention": bool(active or pending or stalled_count >= 2 or not complete or evidence_status == "overdue"),
            "budget_seconds_remaining": state.get("budget_seconds_remaining"),
            "production_opportunity": {"expected_by": deadline.isoformat(), "status": evidence_status},
            "progress_issues": progress, **deliveries, "eligible": eligible,
            "merged_issues": merged_issues, "verified_issues": verified_issues, "developed_issues": developed_issues,
            "advancement_reason": reason,
            "next_action": state.get("next_action"), "excluded": state.get("excluded", []),
            "pending_decisions": pending, "stalled_count": stalled_count, "task_versions": versions,
            "notification_receipts": list(receipts.values())}


LABELS = {"execution_failed": "研发执行失败", "inspection_incomplete": "巡检未完成", "inspection_partial": "巡检部分完成", "evidence_overdue": "生产证据过期", "progress": "研发有进展",
          "needs_approval": "等待决策", "stalled": "研发停滞", "waiting_production": "等待生产验收",
          "no_progress": "有合格任务但未推进", "no_eligible_task": "当前无合格任务"}


def render_outcome(result: dict) -> str:
    pending = result["pending_decisions"]
    health_label = {"healthy": "正常", "warning": "警告", "error": "异常", "critical": "严重异常", "unknown": "未知"}
    evidence_label = {"current": "本期已取得", "waiting": "等待本期产物", "overdue": "已过期"}
    channels = {e["channel"]: e.get("status", "unknown") for e in result.get("notification_receipts", [])}
    status_label = {"success": "成功", "failed": "失败", "unknown": "结果未知", "sending": "等待回执"}
    budget = result.get("budget_seconds_remaining")
    known_waiting = "、".join(f"#{n}" for n in result["waiting_production"])
    if "delivery_inventory_complete" not in result:
        known_waiting = ""  # 旧版清单只记录合并快照。
    if result.get("delivery_inventory_complete") is not True:
        known_waiting = (known_waiting + "；" if known_waiting else "") + "清单待核对"
    lines = [f"{result['day']} 每日研发：{LABELS[result['conclusion']]}" + ("（仍有异常或待处理）" if result.get("requires_attention") else ""),
             f"巡检：{'完成' if result['inspection'] == 'complete' else '部分完成' if result['inspection'] == 'partial' else '未完成'}；健康：{health_label[result['health_severity']]}；生产证据：{evidence_label[result['evidence_status']]}",
             "通知：" + "；".join(f"{name}{status_label.get(channels.get(channel), '待回执')}" for channel, name in (("feishu", "飞书"), ("wecom", "企微"))),
             "健康依据：" + ("最近已知异常，仍需核验新产物" if result.get("health_evidence_status") == "last_known" else "本期正式证据" if result.get("health_evidence_status") == "current" else "未知"),
             "推进任务：" + ("、".join(f"#{n}" for n in result["progress_issues"]) or "无"),
             "人工/外部协助交付：" + ("、".join(f"#{r['issue']}" for r in result.get("manual_deliveries", [])) or "无"),
             "业务复盘：" + {"matched": "已关联", "pending": "未到达，来源独立处理", "unmatched": "关联待核对", "unknown": "未核验"}.get(result.get("business_review", {}).get("status"), "未核验"),
             "合并/验收：" + ("、".join(f"#{n}" for n in result.get("merged_issues", [])) or "无") + " / " + ("、".join(f"#{n}" for n in result.get("verified_issues", [])) or "无"),
             "已登记待验收：" + (known_waiting or "无"),
             "阻塞项：" + ("；".join(f"{i['incident_key']}：{i.get('reason', '待核对')}" for i in result.get("blocked_incidents", [])[:2]) or "无"),
             "待决策：" + ("；".join(p["reason"][:45] for p in pending[:2]) or "无"),
             f"连续无进展：{result['stalled_count']}次；" + (f"当前事故：{len(result['active_incidents'])}项" if result.get("incident_count_known", result["inspection"] == "complete") else f"已知事故至少{len(result['active_incidents'])}项，总数未知" if result['active_incidents'] else "异常数量未知，未完成核验"),
             "推进说明：" + result.get("advancement_reason", "未知")[:100],
             "预算剩余：" + (f"{max(0, int(budget)) // 60}分钟" if budget is not None else "未知"),
             "通知：" + "；".join(f"{name}{status_label.get(channels.get(channel), '待回执')}" for channel, name in (("feishu", "飞书"), ("wecom", "企微"))),
             "证据与详情：https://github.com/xiexie20211028-bot/robtaxi-digest-new/issues/100"]
    if result.get("delivery_review_needed"):
        numbers = result["delivery_review_needed"]
        prs = {p["issue"]: p["pr"] for p in result.get("unregistered_delivery_prs", [])}
        display = [f"#{n}（PR #{prs[n]}）" if prs.get(n) else f"#{n}" for n in numbers[:4]]
        lines.append(f"交付待核对共{len(numbers)}项：" + "、".join(display))
    if result.get("recovered_runs"):
        lines.insert(1, f"中断补报：{len(result['recovered_runs'])} 次原运行到期未结束，详情见正式结果。")
    text = "\n".join(lines)
    if len(text.encode()) > 1500:
        text = text.encode()[:1400].decode("utf-8", errors="ignore") + "…\n完整证据见控制任务#100。"
    return text


def finalize_run(client, outcome: str, *, decision_path: str | None = None, health_sync_path: str | None = None,
                 trigger: str | None = None, error: str | None = None, stop_reason: str | None = None) -> dict:
    """所有正式结束共用收尾：先本地现场，再日报待发送，最后释放整轮。"""
    from .development_cycle import ROOT, assert_trusted_checkout, finish_run, snapshot
    from .development_notifications import notification
    from .development_lease import run_directory
    directory = run_directory(ROOT, client.run_id)
    directory.mkdir(parents=True, exist_ok=True)
    context = {"run_id": client.run_id, "outcome": outcome, "error": error, "stop_reason": stop_reason,
               "at": datetime.now(timezone.utc).isoformat(), "formal_report": False, "finished": False}
    def save_context():
        (directory / "completion.json").write_text(json.dumps(context, ensure_ascii=False, indent=2))
    def optional_json(path, default):
        target = Path(path) if path else directory / default
        require(target.resolve().is_relative_to(directory.resolve()), "收尾输入必须位于本轮独立目录")
        try:
            value = json.loads(target.read_text())
            return value if isinstance(value, dict) else {}
        except (OSError, ValueError):
            return {}
    save_context()
    trusted = False
    finished = {}
    try:
        # 过期、结束或不可信checkout只能保存本地现场，不能补造正式事件。
        client.budget.check()
        assert_trusted_checkout(client)
        trusted = True
        trigger = trigger or client.run_started.get("trigger", "manual")
        decision = optional_json(decision_path, "decision.json")
        receipt = optional_json(health_sync_path, "health-sync.json")
        state_error = None
        pilot = None
        events = []
        try:
            state = snapshot(client, client.policy)
        except (DevelopmentError, OSError, ValueError, KeyError) as exc:
            state_error = type(exc).__name__
            state = {"tasks": [], "next_action": None, "delivery_inventory_complete": False}
        try:
            events = state["events"] if "events" in state else client.events()
            if client.policy.get("mode") == "pilot":
                pilot = client.task(client.policy["pilot_issue"])
        except (DevelopmentError, OSError, ValueError, KeyError) as exc:
            state_error = type(exc).__name__
            state["delivery_inventory_complete"] = False
        official = receipt.get("official_state", {})
        if hasattr(client, "api") and not official.get("complete"):
            try:
                from .health_loop_sync import GhMetadataClient, SyncError
                metadata = GhMetadataClient(); metadata.transport = client
                metadata.run_guard = client.budget.check
                official = metadata.preflight(run_evidence=decision.get("run_evidence", {}))
            except (DevelopmentError, OSError, ValueError, KeyError, SyncError):
                # 仍保留正式队列中的已知异常，总数标未知；不把读取失败变成零事故。
                official = {}
        state["health_context"] = recover_incidents(events, official)
        if hasattr(client, "paginate"):
            for task in state.get("tasks", []):
                pr = task.get("awaiting_production") or {}
                if not pr.get("mergedAt") or timestamp(pr["mergedAt"]).astimezone(BEIJING).date() != datetime.now(BEIJING).date():
                    continue
                try:
                    comments = client.paginate(f"repos/{client.repo}/issues/{pr['number']}/comments?per_page=100")
                    task["manual_merge_verified"] = any(r.get("user", {}).get("login") in client.policy["trusted_actors"]
                        and "<!-- release-approval-" in r.get("body", "") and str(pr.get("headRefOid", "missing")) in r["body"] for r in comments)
                except (DevelopmentError, OSError, ValueError, KeyError):
                    task["manual_merge_verified"] = False
        state["budget_seconds_remaining"] = max(0, client.budget.remaining())
        result = build_outcome(policy=client.policy, state=state, decision=decision, receipt=receipt,
            events=events, run_started=client.run_started, now=datetime.now(timezone.utc),
            workflow=(ROOT / ".github/workflows/robtaxi-digest-pages.yml").read_text(), trigger=trigger, pilot_task=pilot)
        if error or state_error:
            outcome = "failed"
            result.update(conclusion="inspection_incomplete", inspection="incomplete", requires_attention=True,
                          advancement_reason=error or "正式状态读取失败，未继续研发", global_error=state_error or error)
        if outcome == "failed":
            result["conclusion"] = "execution_failed"
            result["requires_attention"] = True
            result["advancement_reason"] = error or state_error or stop_reason or "运行以失败结束，未提供已核实的中断原因"
        result["execution_outcome"] = outcome
        (directory / "outcome.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
        (directory / "daily.md").write_text(render_outcome(result))
        try:
            publish_outcome(client, result, notification)
            context["formal_report"] = True
            if trigger == "schedule":
                client.gh("workflow", "run", "robtaxi-agent-review.yml", "--repo", client.policy["repository"],
                          "--ref", "main", "-f", "notification_only=true")
                context["notification_dispatch"] = "requested"
        except (DevelopmentError, OSError, ValueError, KeyError) as exc:
            # 待发送记录若已写入，既有云端复盘会补偿；响应未知不能重发dispatch。
            context["reporting_error"] = type(exc).__name__
            outcome = "failed"
    except (DevelopmentError, OSError, ValueError, KeyError) as exc:
        context["finalization_error"] = type(exc).__name__
        outcome = "failed"
    finally:
        context["outcome"] = outcome
        if trusted:
            try:
                finished = finish_run(client, outcome)
                context["finished"] = True
            except (DevelopmentError, OSError, ValueError, KeyError) as exc:
                context["finish_error"] = type(exc).__name__
        save_context()
    return {**finished, "run_id": client.run_id, "outcome": outcome, "finished": context["finished"],
            "local_only": not context["formal_report"], "reporting_complete": context["formal_report"],
            "reporting_error": context.get("reporting_error") or context.get("finalization_error") or context.get("finish_error")}


def publish_outcome(client, result: dict, notification_factory) -> dict:
    """先落正式结论，再生成稳定的每日请求；失败时可由下一次恢复。"""
    events = client.events()
    key = f"run-outcome:{result['run_id']}"
    prior = next((e for e in events if e.get("key") == key and e.get("event") == "run_outcome"), None)
    if prior:
        result = prior["result"]
    else:
        client.append({"event": "run_outcome", "key": key, "run_id": result["run_id"], "at": result["finished_at"], "result": result})
    reminders_to_record = []
    for pending in result["pending_decisions"]:
        decision_key = f"decision:{pending['decision_id']}"
        if not any(e.get("key") == decision_key for e in events):
            client.append({**pending, "event": "decision_required", "key": decision_key})
        if pending["reminder_due"]:
            reminder_key = f"decision-reminder:{pending['decision_id']}:{pending['reminder_index']}"
            if not any(e.get("key") == reminder_key for e in events):
                reminders_to_record.append({"event": "decision_reminder", "key": reminder_key, "decision_id": pending["decision_id"],
                                            "reminder_index": pending["reminder_index"], "at": result["finished_at"]})
    if result.get("trigger") != "schedule":
        # 手动协助不抢占正式日报名额，也不冒充计划提醒已入队。
        return result
    # 同日只生成第一条日报；当日后续严重变化使用独立异常事件。
    daily = notification_factory(f"daily:{result['day']}", render_outcome(result))
    if not any(e.get("key") == daily["key"] for e in events):
        client.append(daily)
    alert = {"incidents": sorted((r["key"], r["severity"]) for r in result["active_incidents"] if r["severity"] in {"error", "critical"}),
             "failure": "stalled" if result["stalled_count"] >= 2 else result["conclusion"] if result["conclusion"] in {"inspection_incomplete", "execution_failed", "evidence_overdue"} else None,
             "reminders": sorted((p["decision_id"], p["reminder_index"]) for p in result["pending_decisions"] if p["reminder_due"])}
    if any(alert.values()):
        event = notification_factory(f"development-alert:{digest(alert)}", "研发异常提醒\n" + render_outcome(result))
        if not any(e.get("key") == event["key"] for e in events):
            client.append(event)
    # 提醒待发送项已正式保存后才登记提醒序号；中断不会吃掉一次提醒。
    for reminder in reminders_to_record:
        client.append(reminder)
    return result


def resolve_decision(client, decision_id: str, resolution: str, evidence: str) -> dict:
    """记录明确处理结果，只改变提醒状态，不授予任何开发或策略权限。"""
    require(resolution in {"approved", "rejected", "cancelled", "implemented"}, "必须明确处理结论，已收到不等于批准")
    require(isinstance(evidence, str) and 0 < len(evidence.strip()) <= 500, "缺少用户处理依据或已实施证据")
    events = client.events()
    require(any(e.get("event") == "decision_required" and e.get("decision_id") == decision_id for e in events), "待决策记录不存在")
    previous = next((e for e in events if e.get("event") == "decision_resolved" and e.get("decision_id") == decision_id
                     and e.get("resolution") in {"approved", "rejected", "cancelled", "implemented"}), None)
    if previous:
        require(previous["resolution"] == resolution, "已有不同处理结论，须先核对正式状态")
        return previous
    event = {"event": "decision_resolved", "key": f"decision-resolved:{digest([decision_id, resolution, evidence.strip()])}", "decision_id": decision_id,
             "resolution": resolution, "evidence": evidence.strip(), "at": datetime.now(timezone.utc).isoformat(),
             "authorization_changed": False}
    client.append(event)
    return event


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["report", "resolve"], nargs="?", default="report")
    parser.add_argument("--run-id", default=os.environ.get("CODEX_THREAD_ID", ""))
    parser.add_argument("--decision")
    parser.add_argument("--health-sync")
    parser.add_argument("--decision-id")
    parser.add_argument("--resolution", choices=["approved", "rejected", "cancelled", "implemented"])
    parser.add_argument("--evidence")
    parser.add_argument("--trigger", choices=["manual", "schedule"], default="manual")
    args = parser.parse_args()
    from .development_cycle import ROOT, assert_trusted_checkout, initialize_run, load_policy, snapshot
    from .development_runtime import GitHub, repository_lock
    try:
        # 启用依赖#126；缺少通知批次时在写正式状态前停止，不能悄悄降级为静默。
        from .development_notifications import notification
        policy = load_policy();client = GitHub(policy)
        with repository_lock(policy["repository"]):
            initialize_run(client, policy, args.run_id)
            assert_trusted_checkout(client)
            if args.action == "resolve":
                require(bool(args.decision_id) and bool(args.resolution), "缺少待决策ID或处理结论")
                resolve_decision(client, args.decision_id, args.resolution, args.evidence)
                print("处理结果已登记；开发授权、策略及权限未改变。")
                return 0
            require(bool(args.decision) and bool(args.health_sync), "日报需要原decision和apply回执路径")
            state = snapshot(client, policy)
            state.setdefault("budget_seconds_remaining", max(0, client.budget.remaining()))
            pilot = client.task(policy["pilot_issue"]) if policy.get("mode") == "pilot" else None
            def optional_json(path):
                try:
                    value = json.loads(Path(path).read_text())
                    return value if isinstance(value, dict) else {}
                except (OSError, ValueError):
                    return {}
            result = build_outcome(policy=policy, state=state,
                decision=optional_json(args.decision), receipt=optional_json(args.health_sync),
                events=client.events(), run_started=client.run_started, now=datetime.now(timezone.utc),
                workflow=(ROOT / ".github/workflows/robtaxi-digest-pages.yml").read_text(), trigger=args.trigger, pilot_task=pilot)
            output = ROOT / ".local/robtaxi-development/runs" / args.run_id
            output.mkdir(parents=True, exist_ok=True)
            (output / "outcome.json").write_text(json.dumps(result, ensure_ascii=False, indent=2))
            (output / "daily.md").write_text(render_outcome(result))
            publish_outcome(client, result, notification)
            if args.trigger == "schedule":
                client.gh("workflow", "run", "robtaxi-agent-review.yml", "--repo", policy["repository"], "--ref", "main", "-f", "notification_only=true")
            print(render_outcome(result))
        return 0
    except (DevelopmentError, OSError, ValueError, KeyError, ImportError) as exc:
        print(f"[development-outcomes] STOP: {type(exc).__name__}: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
