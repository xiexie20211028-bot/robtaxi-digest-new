"""每个正常生产运行的事故观察记录；计数从正式历史重算。"""
from __future__ import annotations

import json
import re

SCHEMA = "robtaxi-health-observation-v1"
PREFIX = "<!-- robtaxi-health-observation:"


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


def canonical_history(rows: list[dict], *, as_of_run: str = "") -> list[dict]:
    """较新attempt修正同一run；旧报告回放不读取未来run的状态。"""
    chosen = {}
    for row in rows:
        validate_observation(row)
        if as_of_run and int(row["run_id"]) > int(as_of_run):
            continue
        key = (row["incident_key"], row["run_id"])
        old = chosen.get(key)
        if old is None or row["attempt"] > old["attempt"]:
            chosen[key] = row
        elif row["attempt"] == old["attempt"]:
            fields = ("result", "severity", "valid")
            if any(row[field] != old[field] for field in fields):
                raise ValueError("同一生产run/attempt观察相互冲突")
    return sorted(chosen.values(), key=lambda r: (r.get("run_at", ""), int(r["run_id"])))


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
    valid = run.get("event_name") == "schedule" and available and (not source or participated)
    row = {"schema_version": SCHEMA, "incident_key": key, "source_id": source, "reason_code": reason,
           "run_id": str(run.get("github_run_id", "")), "attempt": int(run.get("github_run_attempt", 1)),
           "run_at": str(run.get("run_started_at") or health.get("date_bj", "")),
           "event_name": run.get("event_name", ""), "commit_sha": run.get("commit_sha", ""),
           "health_report_available": available, "source_participated": participated,
           "result": result, "severity": severity, "valid": valid}
    validate_observation(row)
    return row
