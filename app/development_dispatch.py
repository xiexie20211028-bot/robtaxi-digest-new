"""用已登记的整轮租约恢复阶段，返回交给现有定时会话的唯一任务。"""
from copy import copy
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path

from .development_budget import budgeted, pending_deliveries
from .development_health import sync_health
from .development_policy import DevelopmentError, digest, require, periods
from .development_production import reconcile_production
from .development_recovery import RequestFailure


class StageTimeUp(Exception):
    """工作阶段到达时间片边界，返回维护或收尾阶段。"""


def bounded_client(client, end):
    # 两个传输视图共用同一正式租约、网络恢复账本及诊断列表。只有无副作用的
    # 读取缩短截止。写入及响应未知后的正式查询仍由原客户端完整处理。
    reader = copy(client)
    reader.budget = copy(client.budget)
    reader.budget.deadline = end

    def check_slice():
        client.budget.check()
        if (end - datetime.now(timezone.utc)).total_seconds() <= 30:
            raise StageTimeUp

    def read(*args, **kwargs):
        check_slice()
        try:
            return reader.gh_read(*args, **kwargs)
        except DevelopmentError as exc:
            cause = exc
            while cause is not None:
                if isinstance(cause, RequestFailure) and (not cause.transient or cause.category == "rate_limit"):
                    raise exc
                cause = cause.__cause__
            check_slice()
            raise

    def write(*args, **kwargs):
        check_slice()
        return client.gh(*args, **kwargs)

    def append(*args, **kwargs):
        check_slice()
        return client.append(*args, **kwargs)

    bounded = copy(client)
    bounded.gh_read, bounded.gh, bounded.append = read, write, append
    return bounded


def maintenance_slice(client, policy, directory):
    end = min(datetime.now(timezone.utc) + timedelta(minutes=5), client.budget.deadline - timedelta(minutes=15))
    try:
        return sync_health(bounded_client(client, end), policy, directory, apply=True, phase="maintenance")
    except StageTimeUp:
        return {"reports": [], "stopped": "maintenance_yield"}


def current_evidence(directory):
    """错误或预算收尾时也携带本轮当前来源的现场；正式有效性由收尾入口校验。"""
    try:
        progress = json.loads((directory / "health" / "progress.json").read_text())
        if progress.get("phase") == "current" and len(progress["reports"]) == 1:
            return progress["reports"][0]
    except (OSError, ValueError, KeyError, TypeError):
        pass
    return {}


def dispatch(owner, policy, directory: Path):
    from .development_outcomes import finalize_run
    require(budgeted(policy) and policy["mode"] == "active", "阶段派工仅适用于已启用的 v3 队列")
    require(owner.run_id and owner.budget, "阶段派工需要已登记的整轮租约")
    try:
        if owner.budget.remaining() > 300:
            client = bounded_client(owner, owner.budget.deadline - timedelta(minutes=3))
            return dispatch_work(client, owner, policy, directory)
        reason = "剩余预算仅够正式收尾，未启动来源读取、验收或任务认领"
    except StageTimeUp:
        reason = "工作阶段已到收尾预留时间，已保存的证据保留，剩余项目待核验"
    current = current_evidence(directory)
    completion = finalize_run(owner, "budget_exhausted", decision_path=current.get("decision"),
        health_sync_path=current.get("receipt"), stop_reason=reason, quick=True)
    return {"run_id": owner.run_id, "action": "dispatch", "stage": "finished", "reason": reason,
            "completion": completion}


def dispatch_work(client, owner, policy, directory):
    from .development_cycle import active_claim, assert_trusted_checkout, claim, health_heartbeat, snapshot
    from .development_outcomes import finalize_run

    main = assert_trusted_checkout(client)
    result = {"run_id": client.run_id, "main_sha": main, "action": "dispatch"}
    current = current_evidence(directory)
    state = None

    def finish(outcome, reason):
        result.update(stage="finished", reason=reason)
        result["completion"] = finalize_run(owner, outcome, decision_path=current.get("decision"),
            health_sync_path=current.get("receipt"), stop_reason=reason, cached_state=state, quick=state is None)
        return result

    sources = sync_health(client, policy, directory / "health", apply=True, phase="current")
    current = next(iter(sources["reports"]), {})
    if sources.get("stopped") or client.budget.remaining() <= 210:
        raise StageTimeUp
    if not current.get("receipt"):
        return finish("failed", "最新正常生产缺少本轮来源回执：" + current.get("source_status", "未取得报告"))
    receipt = json.loads(Path(current["receipt"]).read_text())
    decision = json.loads(Path(current["decision"]).read_text())
    require(receipt.get("development_run_id") == client.run_id and receipt.get("global_state_valid") is True
            and receipt.get("decision_digest") == digest(decision), "本轮来源回执或决定摘要不可信")
    result["current_source"] = {k: current[k] for k in ("production_run_id", "attempt", "decision", "receipt")}

    state = snapshot(client, policy)
    proof = next((e for e in reversed(state["events"]) if e.get("event") == "health_queue_updated"
        and e.get("run_id") == client.run_id and e.get("global_state_valid") is True
        and str(e.get("production_run_id")) == current["production_run_id"] and e.get("attempt", 1) == current["attempt"]
        and e.get("decision_digest") == receipt["decision_digest"]), None)
    require(bool(proof), "当前来源缺少对应正式队列回执，停止派工")
    result["source_queue_key"] = proof["key"]
    action = state.get("next_action")
    existing = action and active_claim(state["events"], action["issue"], datetime.now(timezone.utc))
    if client.budget.remaining() <= 210:
        raise StageTimeUp
    if existing:
        # 正式认领优先恢复。无关交付的再次验收只能排在交接之后。
        result.update(stage="work", next_action=action, excluded=state.get("excluded", []),
                      handoff=claim(client, policy, state, issue=action["issue"]))
        return result
    # 只检查已登记的待验收交付和总盘观察任务。去重后按最久未核验的优先，
    # 每次派工至多检查一个；仍待验收的交付始终保留原影响门禁。
    pending = {row["issue"] for row in pending_deliveries(state["events"])}
    pending.update(t["number"] for t in state["tasks"] if t.get("status") == "观察中")
    checks = [e for e in state["events"] if e.get("event") == "dispatch_verification_checked"]
    checked = {e["issue"] for e in checks if e.get("run_id") == client.run_id and
               e.get("source_queue_key") == proof["key"] and e.get("main_sha") == main}
    remaining = pending - checked
    if remaining and client.budget.remaining() >= 900:
        last_at = {e["issue"]: e["at"] for e in checks}
        issue = min(remaining, key=lambda n: (last_at.get(n, ""), n))
        maintenance = None
        if issue == 73 and any(e.get("issue") == 73 and e.get("event") == "historical_delivery_adopted"
                               for e in state["events"]):
            from .development_history import resume_law02_revision
            maintenance = resume_law02_revision(client, state["events"])
        if maintenance is False:
            verified = {"issue": issue, "status": "waiting", "reason": "已批准的历史修订仍缺有效修复证据"}
        elif maintenance:
            verified = reconcile_production(client, policy, issue, apply=True, historical_maintenance=True)
        else:
            verified = reconcile_production(client, policy, issue, apply=True)
        result["verification"] = verified
        client.append({"event": "dispatch_verification_checked", "key": f"dispatch-verify:{client.run_id}:{proof['key']}:{main}:{issue}",
            "run_id": client.run_id, "issue": issue, "source_queue_key": proof["key"], "main_sha": main,
            "result_digest": digest(verified), "at": datetime.now(timezone.utc).isoformat()})
        state = snapshot(client, policy)

    releases = any(e.get("event") == "task_released" and e.get("run_id") == client.run_id for e in state["events"])
    maintenance_key = f"dispatch-maintenance:{client.run_id}"
    maintained = any(e.get("key") == maintenance_key for e in state["events"])
    action = state.get("next_action")
    active = action and active_claim(state["events"], action["issue"], datetime.now(timezone.utc))
    if (not action or releases) and not active and not maintained and client.budget.remaining() > 1200:
        maintenance = maintenance_slice(owner, policy, directory / "maintenance")
        result["maintenance"] = {"stopped": maintenance["stopped"], "reports": len(maintenance["reports"])}
        latest = next((r for r in maintenance["reports"] if r.get("production_run_id") == current["production_run_id"]
            and r.get("attempt") == current["attempt"] and r.get("review_saved") and r.get("receipt")), {})
        if latest:
            current = latest
            result["current_source"] = {k: current[k] for k in ("production_run_id", "attempt", "decision", "receipt")}
            complete_receipt = json.loads(Path(current["receipt"]).read_text())
            if complete_receipt.get("complete"):
                health_heartbeat(client, complete_receipt)
        client.append({"event": "dispatch_maintenance_checked", "key": maintenance_key, "run_id": client.run_id,
            "result": result["maintenance"], "at": datetime.now(timezone.utc).isoformat()})
        state = snapshot(client, policy)
        action = state.get("next_action")
    result["excluded"] = state.get("excluded", [])
    if not action:
        waiting = any(t.get("pending_delivery_conflicts") or t.get("awaiting_production") for t in state["tasks"])
        return finish("waiting_production" if waiting else "no_eligible_task",
                      "当前没有可领取任务，候选排除原因已写入日报")
    day, _ = periods(datetime.now(timezone.utc))
    previously_claimed = any(e.get("event") == "task_claimed" and e.get("issue") == action["issue"]
        and e.get("day") == day for e in state["events"])
    if client.budget.remaining() <= 210:
        raise StageTimeUp
    if not previously_claimed and client.budget.remaining() < 900:
        return finish("budget_exhausted", f"已选出 #{action['issue']}，剩余预算不足以启动该阶段")
    handoff = claim(client, policy, state, issue=action["issue"])
    result.update(stage="work", next_action=action, handoff=handoff)
    return result
