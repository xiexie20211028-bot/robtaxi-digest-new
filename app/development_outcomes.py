"""由正式状态生成研发结论；巡检完成、事故恢复与工程验收分别报告。"""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re

from .development_policy import DevelopmentError, digest, require, timestamp

BEIJING = timezone(timedelta(hours=8))
SCHEMA = "robtaxi-development-outcome-v1"
PROGRESS_STAGES = {"planned", "code_changed", "tests_passed", "pr_created"}


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


def build_outcome(*, policy: dict, state: dict, decision: dict, receipt: dict, events: list[dict],
                  run_started: dict, now: datetime, workflow: str, trigger: str = "manual", pilot_task: dict | None = None) -> dict:
    run_id = run_started["run_id"]
    day = now.astimezone(BEIJING).date().isoformat()
    complete = (receipt.get("complete") is True and receipt.get("mode") == "apply"
                and receipt.get("development_run_id") == run_id and receipt.get("official_state", {}).get("complete") is True
                and receipt.get("decision_digest") == digest(decision)
                and decision.get("run_evidence", {}).get("health_report_available") is True)
    partial = (not complete and receipt.get("global_state_valid") is True and receipt.get("mode") == "apply"
               and receipt.get("development_run_id") == run_id and receipt.get("decision_digest") == digest(decision)
               and any(i.get("status") == "synced" for i in receipt.get("item_results", [])))
    deadline = production_deadline(workflow, now)
    current_data = decision.get("date_bj") == day and decision.get("run_evidence", {}).get("event_name") == "schedule"
    evidence_status = "current" if current_data else "waiting" if now < deadline else "overdue"
    known = dict(receipt.get("official_state", {}).get("incidents", {}))
    known.update(decision.get("incidents", {}))
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
    baselines = [e["result"] for e in events if e.get("event") == "run_outcome" and e.get("result", {}).get("run_id") != run_id
                 and e.get("result", {}).get("finished_at", "") <= now.isoformat()]
    baseline = max(baselines, key=lambda r: r["finished_at"], default=previous)
    progress, merged_issues, verified_issues, developed_issues = [], [], [], []
    for task in tasks:
        relevant = [e for e in task.get("events", []) if
                    (e.get("event") in {"contract", "delivery", "production_verified", "historical_production_verified", "review"}
                     or (e.get("event") == "checkpoint" and e.get("stage") in PROGRESS_STAGES))
                    and (e.get("run_id") == run_id or (e.get("at") and timestamp(run_started["at"]) <= timestamp(e["at"]) <= now))]
        number = str(task["number"])
        if any(e.get("event") == "delivery" for e in relevant): merged_issues.append(task["number"])
        if any(e.get("event") in {"production_verified", "historical_production_verified"} for e in relevant): verified_issues.append(task["number"])
        if any(e.get("stage") == "code_changed" and e.get("head_sha") for e in relevant): developed_issues.append(task["number"])
        old = baseline.get("task_versions", {}).get(number)
        def signals(value):
            return {digest([key, item]) for key, item in value.items() if item}
        added = signals(versions[number]) - signals(old or {})
        if added and (old is not None or relevant):
            progress.append(task["number"])
    eligible = bool(state.get("next_action")) and not pilot_complete
    waiting = [t["number"] for t in state.get("tasks", []) if t.get("awaiting_production") or t.get("delivery_adoption_required")]
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
    return {"schema_version": SCHEMA, "run_id": run_id, "trigger": trigger, "day": day,
            "started_at": run_started["at"], "finished_at": now.isoformat(), "conclusion": conclusion,
            "inspection": "complete" if complete else "partial" if partial else "incomplete", "evidence_status": evidence_status,
            "blocked_incidents": receipt.get("blocked_incidents", []), "review_available": decision.get("run_evidence", {}).get("review_available"),
            "production_run_id": decision.get("run_id"), "production_date": decision.get("date_bj"),
            "production_commit": decision.get("run_evidence", {}).get("commit_sha"), "production_deadline": deadline.isoformat(),
            "health_severity": severity, "active_incidents": [{"key": r.get("incident_key"), "severity": r.get("severity")} for r in active],
            "health_evidence_status": "current" if current_data and complete else "last_known" if active else "unknown",
            "requires_attention": bool(active or pending or stalled_count >= 2 or not complete or evidence_status == "overdue"),
            "budget_seconds_remaining": state.get("budget_seconds_remaining"),
            "production_opportunity": {"expected_by": deadline.isoformat(), "status": evidence_status},
            "progress_issues": progress, "waiting_production": waiting, "eligible": eligible,
            "merged_issues": merged_issues, "verified_issues": verified_issues, "developed_issues": developed_issues,
            "advancement_reason": reason,
            "next_action": state.get("next_action"), "excluded": state.get("excluded", []),
            "pending_decisions": pending, "stalled_count": stalled_count, "task_versions": versions,
            "notification_receipts": list(receipts.values())}


LABELS = {"inspection_incomplete": "巡检未完成", "inspection_partial": "巡检部分完成", "evidence_overdue": "生产证据过期", "progress": "研发有进展",
          "needs_approval": "等待决策", "stalled": "研发停滞", "waiting_production": "等待生产验收",
          "no_progress": "有合格任务但未推进", "no_eligible_task": "当前无合格任务"}


def render_outcome(result: dict) -> str:
    pending = result["pending_decisions"]
    health_label = {"healthy": "正常", "warning": "警告", "error": "异常", "critical": "严重异常", "unknown": "未知"}
    evidence_label = {"current": "本期已取得", "waiting": "等待本期产物", "overdue": "已过期"}
    channels = {e["channel"]: e.get("status", "unknown") for e in result.get("notification_receipts", [])}
    status_label = {"success": "成功", "failed": "失败", "unknown": "结果未知", "sending": "等待回执"}
    budget = result.get("budget_seconds_remaining")
    lines = [f"{result['day']} 每日研发：{LABELS[result['conclusion']]}" + ("（仍有异常或待处理）" if result.get("requires_attention") else ""),
             f"巡检：{'完成' if result['inspection'] == 'complete' else '部分完成' if result['inspection'] == 'partial' else '未完成'}；健康：{health_label[result['health_severity']]}；生产证据：{evidence_label[result['evidence_status']]}",
             "通知：" + "；".join(f"{name}{status_label.get(channels.get(channel), '待回执')}" for channel, name in (("feishu", "飞书"), ("wecom", "企微"))),
             "健康依据：" + ("最近已知异常，仍需核验新产物" if result.get("health_evidence_status") == "last_known" else "本期正式证据" if result.get("health_evidence_status") == "current" else "未知"),
             "推进任务：" + ("、".join(f"#{n}" for n in result["progress_issues"]) or "无"),
             "合并/验收：" + ("、".join(f"#{n}" for n in result.get("merged_issues", [])) or "无") + " / " + ("、".join(f"#{n}" for n in result.get("verified_issues", [])) or "无"),
             "待生产验收：" + ("、".join(f"#{n}" for n in result["waiting_production"][:6]) or "无"),
             "阻塞项：" + ("；".join(f"{i['incident_key']}：{i.get('reason', '待核对')}" for i in result.get("blocked_incidents", [])[:2]) or "无"),
             "待决策：" + ("；".join(p["reason"][:45] for p in pending[:2]) or "无"),
             f"连续无进展：{result['stalled_count']}次；当前事故：{len(result['active_incidents'])}项",
             "推进说明：" + result.get("advancement_reason", "未知")[:100],
             "预算剩余：" + (f"{max(0, int(budget)) // 60}分钟" if budget is not None else "未知"),
             "通知：" + "；".join(f"{name}{status_label.get(channels.get(channel), '待回执')}" for channel, name in (("feishu", "飞书"), ("wecom", "企微"))),
             "证据与详情：https://github.com/xiexie20211028-bot/robtaxi-digest-new/issues/100"]
    text = "\n".join(lines)
    if len(text.encode()) > 1500:
        text = text.encode()[:1400].decode("utf-8", errors="ignore") + "…\n完整证据见控制任务#100。"
    return text


def finalize_run(client, outcome: str, *, decision_path: str | None = None, health_sync_path: str | None = None,
                 trigger: str | None = None, error: str | None = None) -> dict:
    """所有正式结束共用收尾：先本地现场，再日报待发送，最后释放整轮。"""
    from .development_cycle import ROOT, assert_trusted_checkout, finish_run, snapshot
    from .development_notifications import notification
    from .development_lease import run_directory
    directory = run_directory(ROOT, client.run_id)
    directory.mkdir(parents=True, exist_ok=True)
    context = {"run_id": client.run_id, "outcome": outcome, "error": error,
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
        try:
            state = snapshot(client, client.policy)
            events = client.events()
            pilot = client.task(client.policy["pilot_issue"]) if client.policy.get("mode") == "pilot" else None
        except (DevelopmentError, OSError, ValueError, KeyError) as exc:
            state_error = type(exc).__name__
            state, events, pilot = {"tasks": [], "next_action": None}, [], None
        state["budget_seconds_remaining"] = max(0, client.budget.remaining())
        result = build_outcome(policy=client.policy, state=state, decision=decision, receipt=receipt,
            events=events, run_started=client.run_started, now=datetime.now(timezone.utc),
            workflow=(ROOT / ".github/workflows/robtaxi-digest-pages.yml").read_text(), trigger=trigger, pilot_task=pilot)
        if error or state_error:
            outcome = "failed"
            result.update(conclusion="inspection_incomplete", inspection="incomplete", requires_attention=True,
                          advancement_reason=error or "正式状态读取失败，未继续研发", global_error=state_error or error)
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
             "failure": "stalled" if result["stalled_count"] >= 2 else result["conclusion"] if result["conclusion"] in {"inspection_incomplete", "evidence_overdue"} else None,
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
