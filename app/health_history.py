"""每个正常生产运行的事故观察记录；计数从正式历史重算。"""
from __future__ import annotations

import json
import re

from .health_evidence import usable_health_report

SCHEMA = "robtaxi-health-observation-v1"
PREFIX = "<!-- robtaxi-health-observation:"


def required_recovery_runs(issue: int | None, body: str = "", minimum: int = 2) -> int:
    """原验收更严格时取原值；缺少旧正文也保留已核实任务的三次标准。"""
    required = max(minimum, 3 if issue in {49, 52, 57, 58} else 2)
    section = re.search(r"(?ms)^## 验收标准\s*\n(.*?)(?=^## |\Z)", body)
    if section:
        for value in re.findall(r"连续\s*([0-9一二三四五六七八九十]+)\s*次", section[1]):
            parsed = int(value) if value.isdigit() else {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
                                                               "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}.get(value)
            if parsed is None:
                raise ValueError("原验收连续次数不能可靠解析，停止自动验收")
            required = max(required, parsed)
    return required


def validate_observation(row: dict) -> None:
    if row.get("schema_version") != SCHEMA or not re.fullmatch(r"[a-z0-9_.:-]{3,160}", str(row.get("incident_key", ""))):
        raise ValueError("观察记录结构无效")
    if not str(row.get("run_id", "")).isdigit() or type(row.get("attempt")) is not int or row["attempt"] < 1:
        raise ValueError("观察记录必须绑定生产run/attempt")
    if row.get("result") not in {"failed", "healthy", "unknown"} or type(row.get("valid")) is not bool:
        raise ValueError("观察结果无效")
    if row.get("severity") not in {"healthy", "warning", "error", "critical"}:
        raise ValueError("观察严重级别无效")
    if row["valid"] and (row.get("event_name") != "schedule" or not row.get("health_report_available")):
        raise ValueError("有效观察缺少正常生产证据")


def observation_marker(rows: list[dict]) -> str:
    for row in rows:
        validate_observation(row)
    body = PREFIX + json.dumps(rows, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + " -->"
    if len(body) > 55000:
        raise ValueError("观察记录过大，停止写入")
    return body


def parse_observations(comments: list[dict], owner: str) -> list[dict]:
    records = []
    for comment in comments:
        if comment.get("user", {}).get("login") != owner:
            continue
        for line in str(comment.get("body", "")).splitlines():
            if line.startswith(PREFIX) and line.endswith(" -->"):
                value = json.loads(line[len(PREFIX):-4])
                if not isinstance(value, list):
                    raise ValueError("正式观察历史损坏")
                for row in value:
                    validate_observation(row)
                    records.append(row)
    return records


_REVIEW_FIELDS = ("review_run_id", "review_attempt", "review_sha256")


def _review_revision(row: dict) -> tuple:
    run, attempt, fingerprint = (row.get(k) for k in _REVIEW_FIELDS)
    for value, ok in ((run, str(run).isdigit()), (attempt, type(attempt) is int and attempt >= 1),
                      (fingerprint, re.fullmatch(r"[0-9a-f]{64}", str(fingerprint)))):
        if value is not None and not ok:
            raise ValueError("业务复盘观察身份损坏")
    return (str(run), attempt, fingerprint) if all(v is not None for v in (run, attempt, fingerprint)) else ()


def _one_attempt(rows: list[dict]) -> dict:
    key = rows[0]["incident_key"]
    if key.startswith("review:"):
        bound = [(row, _review_revision(row)) for row in rows]
        revisions = {}
        run_ids = set()
        for row, revision in bound:
            if not revision:
                continue
            run, attempt, _ = revision
            run_ids.add(run)
            old = revisions.get(attempt)
            if old and any(old.get(k) != row.get(k) for k in ("result", "valid", "severity", "review_sha256", "commit_sha")):
                raise ValueError("同一复盘run/attempt观察相互冲突")
            revisions[attempt] = row
        if len(run_ids) > 1:
            raise ValueError("同一生产run/attempt绑定不同业务复盘，观察相互冲突")
        if revisions:
            return revisions[max(revisions)]
        # 老记录缺复盘身份，保留问题线索，但不能证明这一天属于有效统计日。
        rows = [{**row, "valid": False} for row in rows]
    elif key.startswith("check:"):
        # 旧控制器曾把独立生产检查错误附上复盘字段并可能降低 valid。
        # 只在存在同一生产证据的原始检查时采用原始值；其余历史不推断为有效。
        clean = [row for row in rows if not any(k in row for k in _REVIEW_FIELDS)]
        if clean:
            for row in rows:
                if any(row.get(k) != clean[0].get(k) for k in ("result", "severity", "commit_sha", "event_name", "health_report_available")):
                    raise ValueError("同一生产run/attempt检查相互冲突")
            rows = clean
        else:
            rows = [{**row, "valid": False} for row in rows]
    first = rows[0]
    if any(any(row[k] != first[k] for k in ("result", "severity", "valid")) for row in rows[1:]):
        raise ValueError("同一生产run/attempt观察相互冲突")
    return first


def canonical_history(rows: list[dict], *, as_of_run: str = "") -> list[dict]:
    """先核对每次生产及复盘修订的证据，再按生产次数计数。"""
    groups = {}
    for row in rows:
        validate_observation(row)
        if not as_of_run or int(row["run_id"]) <= int(as_of_run):
            attempts = groups.setdefault((row["incident_key"], row["run_id"]), {})
            attempts.setdefault(row["attempt"], []).append(row)
    chosen = []
    for attempts in groups.values():
        revisions = {n: _one_attempt(items) for n, items in attempts.items()}
        chosen.append(revisions[max(revisions)])
    return sorted(chosen, key=lambda r: (r.get("run_at", ""), int(r["run_id"])))


def pending_observations(existing: list[dict], incoming: list[dict]) -> list[dict]:
    merged = canonical_history([*existing, *incoming])
    prior = {(r["incident_key"], r["run_id"]): r for r in canonical_history(existing)}
    updated = {(r["incident_key"], r["run_id"]): r for r in merged}
    pending = []
    for row in incoming:
        key = (row["incident_key"], row["run_id"])
        if row["incident_key"].startswith(("review:", "check:")):
            selected = updated[key]
            if selected != prior.get(key) and selected not in pending:
                pending.append(selected)
        elif row not in existing and row not in pending:
            # 来源观察可能补回独立的合并版本可达证据，仍完整保存。
            pending.append(row)
    return pending


def counts(rows: list[dict]) -> dict:
    total = consecutive = recovery = 0
    for row in canonical_history(rows):
        if not row["valid"] or row["result"] == "unknown":
            continue
        if row["result"] == "failed":
            total += 1
            consecutive += 1
            recovery = 0
        else:
            consecutive = 0
            recovery += 1
    return {"occurrence_count": total, "consecutive_failures": consecutive, "recovery_count": recovery}


def make_observation(health: dict, *, key: str, source: str, reason: str, result: str, severity: str,
                     participated: bool = False) -> dict:
    run = health.get("run", {})
    available = bool(health.get("source_report", {}).get("available", False))
    valid = run.get("event_name") == "schedule" and usable_health_report(health) and (not source or participated)
    row = {"schema_version": SCHEMA, "incident_key": key, "source_id": source, "reason_code": reason,
           "run_id": str(run.get("github_run_id", "")), "attempt": int(run.get("github_run_attempt", 1)),
           "run_at": str(run.get("run_started_at") or health.get("date_bj", "")),
           "event_name": run.get("event_name", ""), "commit_sha": run.get("commit_sha", ""),
           "health_report_available": available, "source_participated": participated,
           "result": result, "severity": severity, "valid": valid}
    validate_observation(row)
    return row
