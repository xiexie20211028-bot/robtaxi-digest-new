"""v3每日累计预算与交付影响；所有权限仍从可信策略及实际差异推导。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .development_policy import require, timestamp, digest

BEIJING = timezone(timedelta(hours=8))
ONE_OFF_TEST_START = datetime(2026, 11, 11, 16, 0, tzinfo=BEIJING)


def budgeted(policy: dict) -> bool:
    return policy.get("schema_version") == "robtaxi-autonomy-policy-v3"


def queue_entry_verified(queue: dict, entry: dict) -> bool:
    if queue.get("complete") is True:
        return queue.get("global_state_valid") is not False  # 兼容旧的全部成功记录。
    return (queue.get("schema_version") == "robtaxi-health-queue-v2" and queue.get("global_state_valid") is True
            and entry.get("observation_saved") is True
            and any(i.get("incident_key") == entry.get("incident_key") and i.get("status") == "synced"
                    and str(i.get("production_run_id")) == str(queue.get("production_run_id"))
                    for i in queue.get("item_results", [])))


def require_health_independent(impact: list[str], events: list[dict]) -> None:
    queues = [e for e in events if e.get("event") == "health_queue_updated"]
    latest = max(queues, key=lambda e: (int(e.get("production_run_id", 0)), int(e.get("attempt", 1)), e.get("at", "")), default={})
    blocked = {i.get("source_id") for i in latest.get("blocked_incidents", []) if i.get("source_id")}
    require(not blocked or not impact or ("shared:production" not in impact
            and not {f"source:{s}" for s in blocked}.intersection(impact)),
            "实际交付影响到记录阻塞的来源或共享链路，等待相关证据修复")


def verifier_for_task(policy: dict, issue: int, events: list[dict]) -> dict:
    """自动来源绑定必须能回溯正式健康队列；不能只相信合同声明。"""
    explicit = policy.get("production_verifiers", {}).get(str(issue))
    if explicit:
        return explicit
    if not budgeted(policy) or policy.get("automatic_source_verifiers") is not True:
        return {}
    for event in reversed(events):
        binding = event.get("binding", {})
        if event.get("event") != "source_verifier_bound" or binding.get("issue") != issue:
            continue
        if (event.get("key") != f"source-verifier:{digest(binding)}" or binding.get("kind") != "source"
                or binding.get("name") != "source_health_v1" or not binding.get("source_id")
                or type(binding.get("required_runs")) is not int or binding["required_runs"] < 2):
            continue
        queue = next((e for e in events if e.get("event") == "health_queue_updated" and e.get("key") == event.get("health_queue_key")), {})
        linked = any(r.get("issue") == issue and r.get("source_id") == binding["source_id"]
                     and r.get("incident_key") == binding.get("incident_key") and r.get("actionable")
                     and queue_entry_verified(queue, r) for r in queue.get("entries", []))
        if linked and str(queue.get("production_run_id")) == str(event.get("production_run_id")):
            return {k: binding[k] for k in ("name", "kind", "source_id", "required_runs")}
    return {}


def daily_seconds(events: list[dict], now: datetime, *, active_run_id: str | None = None) -> float:
    day = now.astimezone(BEIJING).replace(hour=0, minute=0, second=0, microsecond=0)
    finish = day + timedelta(days=1)
    starts, ends = {}, {}
    for event in events:
        run = event.get("run_id")
        if event.get("event") == "run_started":
            require(bool(run), "预算记录缺少run-id")
            require(run not in starts or starts[run] == event, "重复运行预算记录冲突")
            starts[run] = event
        elif event.get("event") == "run_finished":
            at = timestamp(event["at"])
            require(run not in ends or ends[run] == at, "运行结束时间冲突")
            ends[run] = at
    used = 0.0
    for run, event in starts.items():
        start, deadline = timestamp(event["at"]), timestamp(event["deadline"])
        require(start < deadline <= start + timedelta(seconds=5400), "运行预算记录越界")
        stop = min(ends.get(run, deadline), deadline)
        if run == active_run_id and run not in ends:
            stop = min(stop, now)
        require(stop >= start, "结束事件早于开始")
        # 未结束的中断按已预留截止计费，不能换ID收回未知消耗。
        used += max(0.0, (min(stop, finish) - max(start, day)).total_seconds())
    return used


def remaining_seconds(events: list[dict], policy: dict, now: datetime, *, active_run_id: str | None = None) -> float:
    return max(0.0, policy.get("daily_runtime_seconds", 5400) - daily_seconds(events, now, active_run_id=active_run_id))


def scheduled_start_delay(now: datetime) -> float:
    local = now.astimezone(BEIJING)
    opening = local.replace(hour=10, minute=30, second=0, microsecond=0)
    return max(0.0, (opening - local).total_seconds())


def window_seconds(now: datetime) -> float:
    local = now.astimezone(BEIJING)
    start = local.replace(hour=10, minute=30, second=0, microsecond=0)
    end = start.replace(hour=12, minute=0)
    return (end - local).total_seconds() if start <= local < end else 0.0


def one_off_test_seconds(now: datetime, events: list[dict]) -> float:
    """#169 单次批准。正式启动留存后不能换 run-id 再使用。"""
    require(not any(e.get("event") == "run_started" and e.get("one_off_test_at") == ONE_OFF_TEST_START.isoformat()
                    for e in events), "此次临时测试已登记过")
    end = ONE_OFF_TEST_START + timedelta(minutes=30)
    require(ONE_OFF_TEST_START <= now < end, "临时测试仅在北京时间 2026-11-11 16:00 至 16:30 前开放")
    return (end - now).total_seconds()


def impact_from_diff(paths: list[str], *, before: dict | None = None, after: dict | None = None) -> list[str]:
    """只有检查过实际source差异才可判定两个来源交付独立。"""
    require(bool(paths), "交付缺少实际改动")
    operational = [p for p in paths if not p.startswith(("tests/", "docs/")) and not p.endswith(".md")]
    if not operational:
        return []
    if operational == ["sources.json"] and before is not None and after is not None:
        common_before = {k: v for k, v in before.items() if k != "sources"}
        common_after = {k: v for k, v in after.items() if k != "sources"}
        if common_before == common_after:
            old = {s["id"]: s for s in before.get("sources", [])}
            new = {s["id"]: s for s in after.get("sources", [])}
            changed = [s for s in old.keys() | new.keys() if old.get(s) != new.get(s)]
            if len(changed) == 1 and old.keys() == new.keys():
                return [f"source:{changed[0]}"]
    return ["shared:production"]


def conflicts(first: list[str] | None, second: list[str] | None) -> bool:
    if first is None or second is None:
        return True
    if not first or not second:
        return False
    return "shared:production" in first or "shared:production" in second or bool(set(first) & set(second))


def pending_deliveries(events: list[dict]) -> list[dict]:
    # 历史登记也占用影响门禁；未重建实际差异时按未知范围保守处理。
    events = [*events, *({"event": "delivery", "issue": e["issue"],
                         "merge_sha": e["facts"]["merge_sha"], "production": e["facts"]["production"],
                         "impact_keys": e["facts"].get("impact_keys"), "adoption_key": e["key"]}
                        for e in current_adoptions(events))]
    deliveries = [e for e in events if e.get("event") == "delivery"]
    verified = {delivery_token(d) for d in deliveries if any(verification_matches(d, e) for e in events)}
    while True:
        prior = set(verified)
        for event in events:
            if event.get("event") == "delivery" and delivery_token(event) in verified:
                # 只沿同一任务的正式回归链退休；不能按SHA解除其他任务或新登记。
                verified.update(delivery_token(d) for d in deliveries if d.get("issue") == event.get("issue")
                                and d.get("merge_sha") in event.get("supersedes", [])
                                and (not d.get("adoption_key") or any(
                                    r.get("event") == "historical_production_requeue" and r.get("issue") == d.get("issue")
                                    and r.get("adoption_key") == d["adoption_key"] and r.get("merge_sha") == d.get("merge_sha")
                                    for r in events)))
        if verified == prior:
            break
    return [e for e in events if e.get("event") == "delivery" and delivery_token(e) not in verified
            and e.get("production", {}).get("kind") != "none"]


def current_adoptions(events: list[dict]) -> list[dict]:
    """兼容旧登记；同一交付仅保留最后一个正式修订参与门禁。"""
    result = {}
    for event in events:
        if event.get("event") == "historical_delivery_adopted":
            facts = event.get("facts", {})
            result[(event.get("issue"), facts.get("pr"), facts.get("merge_sha"))] = event
    return list(result.values())


def delivery_token(event: dict) -> str:
    return digest({k: event.get(k) for k in ("issue", "merge_sha", "contract_digest", "adoption_key")})


def verification_matches(delivery: dict, event: dict) -> bool:
    if event.get("issue") != delivery.get("issue") or not delivery.get("issue") or event.get("merge_sha") != delivery.get("merge_sha"):
        return False
    if delivery.get("adoption_key"):
        return (event.get("event") == "historical_production_verified"
                and event.get("adoption_key") == delivery["adoption_key"])
    return (event.get("event") == "production_verified"
            and event.get("contract_digest") == delivery.get("contract_digest"))


def delivery_preview(client, task: dict, pending: list[dict], events: list[dict]) -> list[dict]:
    """计划时提示待验收交付，合并前仍按最新实际差异执行门禁。"""
    if not pending or task.get("awaiting_production") or task.get("state") != "OPEN":
        return []
    impact = None
    basis = "尚无代码差异，独立性待核实"
    pr = task.get("open_pr")
    if pr:
        # 固定提交比较，避免分页期间分支推进导致混用两个版本。
        base = pr.get("baseRefOid") or client.main_sha()
        head = pr["headRefOid"]
        comparison = client.api(f"repos/{client.repo}/compare/{base}...{head}")
        files = comparison.get("files")
        if isinstance(files, list) and 0 < len(files) < 300:
            paths = sorted({path for row in files for path in (row["filename"], row.get("previous_filename")) if path})
            before = after = None
            if "sources.json" in paths:
                import base64
                import json
                revisions = []
                for ref in (comparison["merge_base_commit"]["sha"], head):
                    blob = client.api(f"repos/{client.repo}/contents/sources.json?ref={ref}")
                    require(blob.get("encoding") == "base64", "来源快照不可核验")
                    revisions.append(json.loads(base64.b64decode(blob["content"])))
                before, after = revisions
            impact = impact_from_diff(paths, before=before, after=after)
            basis = f"PR #{pr['number']} 实际差异 {head}"
    else:
        production = (task.get("contract") or {}).get("production", {})
        source = production.get("source_id") or task.get("health_source")
        if source:
            impact = [f"source:{source}"]
            basis = "来源任务的最低影响范围，合并前核验实际差异"
    rows = []
    for prior in pending:
        if prior.get("issue") == task["number"] and any(
            e.get("event") in {"production_requeue", "historical_production_requeue"}
            and e.get("issue") == task["number"] and e.get("merge_sha") == prior.get("merge_sha")
            and (not prior.get("adoption_key") or e.get("adoption_key") == prior["adoption_key"]) for e in events):
            continue
        if conflicts(impact, prior.get("impact_keys")):
            rows.append({"issue": prior["issue"], "merge_sha": prior.get("merge_sha"),
                         "adoption_key": prior.get("adoption_key"), "evidence_key": prior.get("adoption_key") or prior.get("key"),
                         "basis": basis,
                         "blocking": impact is not None,
                         "resolution": "原交付通过绑定验收，或实际差异证明互不影响"})
    return rows


def require_independent(impact: list[str], events: list[dict], issue: int, *, remediation: int | None = None) -> None:
    require_health_independent(impact, events)
    for prior in pending_deliveries(events):
        if prior.get("issue") == issue:
            require(any(e.get("event") in {"production_requeue", "historical_production_requeue"}
                        and e.get("merge_sha") == prior.get("merge_sha")
                        and (not prior.get("adoption_key") or e.get("adoption_key") == prior["adoption_key"])
                        for e in events),
                    "同任务仍未验收，须有原交付失败证据才能继续回归修复")
            continue
        if conflicts(impact, prior.get("impact_keys")):
            # 回归修复只能通过可信原任务依赖显式关联，不以自由文字声明绕过。
            require(remediation == prior.get("issue"), f"交付与#{prior.get('issue')}待生产验收改动相关，先验收或关联回归修复")
