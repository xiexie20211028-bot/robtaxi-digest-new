"""v3每日累计预算与交付影响；所有权限仍从可信策略及实际差异推导。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from .development_policy import require, timestamp, digest

BEIJING = timezone(timedelta(hours=8))


def budgeted(policy: dict) -> bool:
    return policy.get("schema_version") == "robtaxi-autonomy-policy-v3"


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
        queue = next((e for e in events if e.get("event") == "health_queue_updated" and e.get("key") == event.get("health_queue_key")
                      and e.get("complete") is True), {})
        linked = any(r.get("issue") == issue and r.get("source_id") == binding["source_id"]
                     and r.get("incident_key") == binding.get("incident_key") and r.get("actionable") for r in queue.get("entries", []))
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
    return max(0.0, policy["daily_runtime_seconds"] - daily_seconds(events, now, active_run_id=active_run_id))


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
    verified = {e.get("merge_sha") for e in events if e.get("event") in {"production_verified", "historical_production_verified"}}
    while True:
        prior = set(verified)
        for event in events:
            if event.get("event") == "delivery" and event.get("merge_sha") in verified:
                verified.update(event.get("supersedes", []))
        if verified == prior:
            break
    return [e for e in events if e.get("event") == "delivery" and e.get("merge_sha") not in verified
            and e.get("production", {}).get("kind") != "none"]


def require_independent(impact: list[str], events: list[dict], issue: int, *, remediation: int | None = None) -> None:
    for prior in pending_deliveries(events):
        if prior.get("issue") == issue:
            require(any(e.get("event") == "production_requeue" and e.get("merge_sha") == prior.get("merge_sha") for e in events),
                    "同任务仍未验收，须有原交付失败证据才能继续回归修复")
            continue
        if conflicts(impact, prior.get("impact_keys")):
            # 回归修复只能通过可信原任务依赖显式关联，不以自由文字声明绕过。
            require(remediation == prior.get("issue"), f"交付与#{prior.get('issue')}待生产验收改动相关，先验收或关联回归修复")
