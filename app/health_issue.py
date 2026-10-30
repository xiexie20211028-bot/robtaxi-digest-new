from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import quote, urlencode
from urllib.request import Request, urlopen

from .common import read_json, write_json

API_ROOT = "https://api.github.com"
MANAGED_LABELS = {
    "robtaxi-health": ("0366d6", "Robtaxi 自动运行健康事件"),
    "health-warning": ("d4c5f9", "Robtaxi 健康等级：warning"),
    "health-error": ("fbca04", "Robtaxi 健康等级：error"),
    "health-critical": ("b60205", "Robtaxi 健康等级：critical"),
    "proposal-pending": ("fef2c0", "等待 Codex 自动诊断"),
    "proposal-ready": ("0e8a16", "修复方案已生成，等待审批"),
    "no-fix-required": ("cfd3d7", "诊断认为无需代码修改"),
    "health-recovered": ("1d76db", "对应运行重试后已恢复健康"),
    "health-superseded": ("6f42c1", "已被新的健康事件跟踪方式替代"),
}


def run_marker(run_id: str) -> str:
    return f"<!-- robtaxi-health-run:{run_id} -->"


def incident_marker(incident_id: str) -> str:
    return f"<!-- robtaxi-health-incident:{incident_id} -->"


def _metadata_marker(name: str, value: Any) -> str:
    return f"<!-- robtaxi-health-{name}:{value} -->"


def _marker_value(body: str, name: str) -> str:
    prefix = f"<!-- robtaxi-health-{name}:"
    for line in body.splitlines():
        line = line.strip()
        if line.startswith(prefix) and line.endswith(" -->"):
            return line[len(prefix) : -4]
    return ""


def _stable_incident_id(check_id: str, source_id: str, reason_code: str) -> str:
    """生成可跨严重级别复用的事故身份。

    来源级事件以来源和原因作为身份；检查项只保留为证据。这样同一个
    来源从 warning 升级为 critical 时会更新原 Issue，而不会制造副本。
    没有来源信息的全局事件仍保留检查项，避免把无关系统故障合并在一起。
    """
    normalized_source = source_id.strip().lower()
    normalized_reason = reason_code.strip().lower()
    if normalized_source:
        identity_parts = ("source", normalized_source, normalized_reason or "unknown")
    else:
        identity_parts = ("check", check_id.strip().lower(), normalized_reason or "unknown")
    identity = "|".join(identity_parts)
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()[:12]
    return f"ri_{digest}"


def build_incidents(health: dict[str, Any]) -> list[dict[str, Any]]:
    incidents: list[dict[str, Any]] = []
    for finding in health.get("findings", []):
        if not isinstance(finding, dict):
            raise ValueError("finding 结构无效，不能当作健康结果")
        check_id = str(finding.get("check_id", "health_status"))
        severity = str(finding.get("severity", health.get("overall_status", "warning")))
        summary = str(finding.get("summary", "运行状态异常"))
        evidence = finding.get("evidence", {})
        if not isinstance(evidence, dict):
            raise ValueError("finding evidence 必须为对象")

        failed_sources = evidence.get("failed_sources")
        if "failed_sources" in evidence and not isinstance(failed_sources, list):
            raise ValueError("failed_sources 必须为列表，不能当作健康结果")
        # 来源失败可能由聚合门禁或单来源升级门禁报告；两者必须指向同一来源事故。
        if isinstance(failed_sources, list):
            for source in failed_sources:
                if not isinstance(source, dict):
                    raise ValueError("失败来源记录必须为对象")
                if not source.get("source_id"):
                    raise ValueError("失败来源记录缺少source_id")
                source_id = str(source.get("source_id", "unknown_source"))
                reason_code = str(source.get("reason_code", "unknown_error")) or "unknown_error"
                incidents.append(
                    {
                        "incident_id": _stable_incident_id(check_id, source_id, reason_code),
                        "check_id": check_id,
                        "source_id": source_id,
                        "reason_code": reason_code,
                        "severity": severity,
                        "summary": summary,
                        "evidence": source,
                    }
                )
            continue

        reason_code = str(evidence.get("reason_code", evidence.get("reason", "")))
        source_id = str(evidence.get("source_id", ""))
        incidents.append(
            {
                "incident_id": _stable_incident_id(check_id, source_id, reason_code),
                "check_id": check_id,
                "source_id": source_id,
                "reason_code": reason_code,
                "severity": severity,
                "summary": summary,
                "evidence": evidence,
            }
        )

    if not incidents and str(health.get("overall_status", "healthy")) != "healthy":
        check_id = "health_status"
        incidents.append(
            {
                "incident_id": _stable_incident_id(check_id, "", ""),
                "check_id": check_id,
                "source_id": "",
                "reason_code": "",
                "severity": str(health.get("overall_status", "warning")),
                "summary": "运行状态异常",
                "evidence": {},
            }
        )
    unique: dict[str, dict[str, Any]] = {}
    severity_rank = {"warning": 1, "error": 2, "critical": 3}
    for incident in incidents:
        key = incident["incident_id"]
        evidence_item = {"check_id": incident["check_id"], "evidence": incident["evidence"], "severity": incident["severity"]}
        existing = unique.get(key)
        if existing is None:
            unique[key] = {**incident, "evidence_items": [evidence_item]}
        else:
            trail = existing["evidence_items"] + [evidence_item]
            if severity_rank.get(incident["severity"], 0) > severity_rank.get(existing["severity"], 0):
                unique[key] = {**incident, "evidence_items": trail}
            else:
                existing["evidence_items"] = trail
    return list(unique.values())


def find_issue_for_run(issues: list[dict[str, Any]], run_id: str) -> dict[str, Any] | None:
    marker = run_marker(run_id)
    for issue in issues:
        if "pull_request" in issue:
            continue
        if marker in str(issue.get("body", "")):
            return issue
    return None


def find_issue_for_incident(
    issues: list[dict[str, Any]], incident_id: str, *, source_id: str = "", reason_code: str = ""
) -> dict[str, Any] | None:
    return resolve_issue_for_incident(issues, incident_id, source_id=source_id, reason_code=reason_code)["issue"]


def resolve_issue_for_incident(issues: list[dict[str, Any]], incident_id: str, *,
                               source_id: str = "", reason_code: str = "") -> dict[str, Any]:
    """唯一新标记优先；旧记录必须有正式Incident正文身份，标题不是关联依据。"""
    marker = incident_marker(incident_id)
    exact, legacy = [], []
    for issue in issues:
        if "pull_request" in issue:
            continue
        if "labels" in issue:
            labels = {v if isinstance(v, str) else v.get("name", "") for v in issue.get("labels") or []}
            if not labels.intersection({"robtaxi-health", "health-alert"}):
                continue
        body = str(issue.get("body", ""))
        section = re.search(r"(?ms)^## Incident\s*\n(.*?)(?=^## |<details>|\Z)", body)
        identity = {}
        if section:
            for field, value in re.findall(r"(?m)^- (来源|原因码)：`([^`]+)`\s*$", section[1]):
                if field in identity and identity[field] != value:
                    raise ValueError("Health记录正文身份冲突，记录待核对")
                identity[field] = value
        if marker in body:
            if source_id and identity and (identity.get("来源"), identity.get("原因码")) != (source_id, reason_code):
                raise ValueError("Health新标记与正文身份不一致，记录待核对")
            exact.append(issue)
        elif (source_id and reason_code and _marker_value(body, "incident")
              and (identity.get("来源"), identity.get("原因码")) == (source_id, reason_code)):
            legacy.append(issue)
    if len(exact) > 1:
        raise ValueError("同一事故有多个新标记，记录待核对")
    candidates = exact or legacy
    chosen = min(candidates, key=lambda i: int(i["number"])) if candidates else None
    return {"issue": chosen, "related_issues": sorted({int(i["number"]) for i in exact + legacy}),
            "match": "stable" if exact else "legacy" if legacy else "missing"}


def build_issue_body(
    health: dict[str, Any],
    repair: dict[str, Any] | None,
    artifact_name: str,
    *,
    incident: dict[str, Any] | None = None,
    first_seen: str = "",
    occurrence_count: int = 1,
) -> str:
    run = health.get("run", {})
    findings = health.get("findings", [])
    lines = [
        run_marker(str(run.get("github_run_id", ""))),
        f"<!-- robtaxi-health-request:{health.get('request_id', '')} -->",
    ]
    if incident is not None:
        lines.extend(
            [
                incident_marker(str(incident.get("incident_id", ""))),
                _metadata_marker("first-seen", first_seen or str(health.get("date_bj", ""))),
                _metadata_marker("occurrence-count", occurrence_count),
            ]
        )
    lines.extend([
        "## Robtaxi 自动运行自检事件",
        "",
        f"- 状态：`{(incident or {}).get('severity', health.get('overall_status', ''))}`",
        f"- 统计日期：`{health.get('date_bj', '')}`",
        f"- request_id：`{health.get('request_id', '')}`",
        f"- commit：`{run.get('commit_sha', '')}`",
        f"- GitHub Run：{run.get('run_url', '')}",
        f"- 健康 artifact：`{artifact_name}`",
    ])
    if incident is not None:
        lines.extend(
            [
                f"- incident_id：`{incident.get('incident_id', '')}`",
                f"- 首次发现：`{first_seen or health.get('date_bj', '')}`",
                f"- 累计发生：`{occurrence_count}` 次",
                "",
                "## Incident",
                "",
                f"- 检查项：`{incident.get('check_id', '')}`",
                f"- 来源：`{incident.get('source_id', '') or '不适用'}`",
                f"- 原因码：`{incident.get('reason_code', '') or '不适用'}`",
                f"- 摘要：{incident.get('summary', '')}",
                f"- 证据：`{json.dumps(incident.get('evidence', {}), ensure_ascii=False)}`",
            ]
        )
    else:
        lines.extend(["", "## Findings", ""])
    if incident is None and not findings:
        lines.append("- 无异常。")
    elif incident is None:
        for item in findings:
            lines.append(
                f"- **{str(item.get('severity', '')).upper()} · {item.get('check_id', '')}**："
                f"{item.get('summary', '')}"
            )

    if repair is not None:
        payload = json.dumps(repair, ensure_ascii=False, indent=2)
        if len(payload) > 45000:
            payload = payload[:45000] + "\n... [truncated]"
        lines.extend(
            [
                "",
                "<details>",
                "<summary>repair_request.json</summary>",
                "",
                "```json",
                payload,
                "```",
                "",
                "</details>",
            ]
        )
    return "\n".join(lines)


def build_alert_text(
    health: dict[str, Any],
    issue_urls: list[str],
    stale_issues: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    if str(health.get("overall_status", "healthy")) != "healthy":
        lines.extend(
            [
                f"Robtaxi 运行自检：{str(health.get('overall_status', '')).upper()}",
                f"统计日期：{health.get('date_bj', '')}",
                f"request_id：{health.get('request_id', '')}",
            ]
        )
        for item in health.get("findings", [])[:5]:
            lines.append(f"- {item.get('summary', '')}")
        if issue_urls:
            lines.append("待诊断 Issue：")
            for issue_url in issue_urls[:5]:
                lines.append(f"- {issue_url}")

    if stale_issues:
        if lines:
            lines.append("")
        lines.append(f"另有 {len(stale_issues)} 个健康 Issue 等待诊断已超过 24 小时：")
        for issue in stale_issues[:5]:
            lines.append(f"- {issue.get('html_url', '')}")
    return "\n".join(lines)


class GitHubClient:
    def __init__(self, token: str, repository: str) -> None:
        self.token = token
        self.repository = repository

    def request(
        self,
        method: str,
        path: str,
        payload: dict[str, Any] | None = None,
        *,
        allow_statuses: set[int] | None = None,
    ) -> Any:
        url = f"{API_ROOT}{path}"
        data = None if payload is None else json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = Request(
            url,
            data=data,
            method=method,
            headers={
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {self.token}",
                "X-GitHub-Api-Version": "2022-11-28",
                "User-Agent": "RobtaxiHealth/1.0",
                "Content-Type": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read()
                return json.loads(raw.decode("utf-8")) if raw else {}
        except HTTPError as exc:
            if allow_statuses and exc.code in allow_statuses:
                return {}
            raw = exc.read().decode("utf-8", errors="ignore")[:500]
            raise RuntimeError(f"GitHub API {method} {path} failed: status={exc.code} body={raw}") from exc

    def list_health_issues(self) -> list[dict[str, Any]]:
        query = urlencode({"state": "all", "labels": "robtaxi-health", "per_page": 100})
        result = []
        for page in range(1, 101):
            rows = self.request("GET", f"/repos/{self.repository}/issues?{query}&page={page}")
            if not isinstance(rows, list):
                raise ValueError("Health Issue列表不完整")
            result.extend(row for row in rows if isinstance(row, dict) and "pull_request" not in row)
            if len(rows) < 100:
                return result
        raise ValueError("Health Issue列表超过完整分页上限")

    def preserve_identity_snapshot(self, number: int, incident_id: str, body: str, related: list[int]) -> None:
        """旧正文转新摘要前先保存原始证据；响应丢失后先读正式标记。"""
        marker = f"<!-- robtaxi-health-identity:{hashlib.sha256(body.encode()).hexdigest()} -->"
        for page in range(1, 101):
            comments = self.request("GET", f"/repos/{self.repository}/issues/{number}/comments?per_page=100&page={page}")
            if not isinstance(comments, list):
                raise ValueError("旧记录评论不完整，不能迁移")
            if any(marker in str(row.get("body", "")) for row in comments):
                return
            if len(comments) < 100:
                break
        else:
            raise ValueError("旧记录评论超过完整分页上限")
        snapshot = (f"{marker}\n事故身份兼容：主记录 #{number}，新身份 {incident_id}；"
                    f"关联旧记录 {related}，不追认过去合同或恢复次数。\n\n迁移前原正文：\n\n{body}")
        if len(snapshot) > 60000:
            raise ValueError("旧正文过大，保留原记录并等待人工核对")
        self.add_comment(number, snapshot)

    def ensure_labels(self) -> None:
        for name, (color, description) in MANAGED_LABELS.items():
            self.request(
                "POST",
                f"/repos/{self.repository}/labels",
                {"name": name, "color": color, "description": description},
                allow_statuses={422},
            )

    def create_issue(self, title: str, body: str, labels: list[str]) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/repos/{self.repository}/issues",
            {"title": title, "body": body, "labels": labels},
        )

    def update_issue(self, number: int, **payload: Any) -> dict[str, Any]:
        return self.request("PATCH", f"/repos/{self.repository}/issues/{number}", payload)

    def add_comment(self, number: int, body: str) -> dict[str, Any]:
        return self.request(
            "POST",
            f"/repos/{self.repository}/issues/{number}/comments",
            {"body": body},
        )


def _parse_github_time(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return None


def stale_pending_issues(
    issues: list[dict[str, Any]],
    *,
    now_utc: datetime | None = None,
    minimum_age_hours: int = 24,
) -> list[dict[str, Any]]:
    now = now_utc or datetime.now(timezone.utc)
    cutoff = now - timedelta(hours=minimum_age_hours)
    out: list[dict[str, Any]] = []
    for issue in issues:
        if str(issue.get("state", "")) != "open":
            continue
        labels = {
            str(label.get("name", ""))
            for label in issue.get("labels", [])
            if isinstance(label, dict)
        }
        if "proposal-pending" not in labels:
            continue
        created = _parse_github_time(str(issue.get("created_at", "")))
        if created is not None and created < cutoff:
            out.append(issue)
    return out


def _write_github_output(path: str, values: dict[str, str]) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def _can_reconcile_incidents(health: dict[str, Any]) -> bool:
    source_report = health.get("source_report", {})
    if not isinstance(source_report, dict) or not bool(source_report.get("available", False)):
        return False
    return not any(
        str(finding.get("check_id", "")) == "self_check_internal_failure"
        for finding in health.get("findings", [])
        if isinstance(finding, dict)
    )


def _incident_title(incident: dict[str, Any], date_text: str) -> str:
    severity = str(incident.get("severity", "warning")).upper()
    subject = str(incident.get("source_id", "")) or str(incident.get("check_id", "health"))
    reason = str(incident.get("reason_code", ""))
    suffix = f" · {reason}" if reason else ""
    return f"[Robtaxi Health][{severity}][{subject}{suffix}][{date_text}]"


def sync_health_issue(
    *,
    client: GitHubClient,
    health: dict[str, Any],
    repair: dict[str, Any] | None,
    artifact_name: str,
) -> dict[str, Any]:
    client.ensure_labels()
    issues = client.list_health_issues()
    request_id = str(health.get("request_id", ""))
    date_text = str(health.get("date_bj", ""))
    incidents = build_incidents(health)
    active_ids = {str(incident["incident_id"]) for incident in incidents}
    issue_urls: list[str] = []
    issue_numbers: list[int] = []
    superseded: dict[int, int] = {}

    for incident in incidents:
        incident_id = str(incident["incident_id"])
        resolution = resolve_issue_for_incident(issues, incident_id, source_id=incident.get("source_id", ""),
                                               reason_code=incident.get("reason_code", ""))
        existing = resolution["issue"]
        existing_body = str(existing.get("body", "")) if existing is not None else ""
        first_seen = _marker_value(existing_body, "first-seen") or date_text
        previous_run = _marker_value(existing_body, "run")
        current_run = str(health.get("run", {}).get("github_run_id", ""))
        previous_count_text = _marker_value(existing_body, "occurrence-count")
        try:
            previous_count = max(int(previous_count_text), 1)
        except ValueError:
            previous_count = 1
        occurrence_count = previous_count + 1 if existing is not None and previous_run != current_run else previous_count
        body = build_issue_body(
            health,
            repair,
            artifact_name,
            incident=incident,
            first_seen=first_seen,
            occurrence_count=occurrence_count,
        )
        severity = str(incident.get("severity", health.get("overall_status", "warning")))
        labels = ["robtaxi-health", f"health-{severity}", "proposal-pending"]
        title = _incident_title(incident, date_text)
        if existing is None:
            updated = client.create_issue(title, body, labels)
            if not any(int(item.get("number", 0)) == int(updated.get("number", -1)) for item in issues):
                issues.append(updated)
        else:
            if resolution["match"] == "legacy":
                client.preserve_identity_snapshot(int(existing["number"]), incident_id, existing_body,
                                                  resolution["related_issues"])
            updated = client.update_issue(
                int(existing["number"]),
                title=title,
                body=body,
                state="open",
                labels=labels,
            )
            # 后面的恢复扫描使用当前正文，不能用旧标记把刚恢复跟踪的主记录再次关闭。
            existing.update(updated)
        issue_number = int(updated.get("number", 0))
        superseded.update({number: issue_number for number in resolution["related_issues"] if number != issue_number})
        issue_url = str(updated.get("html_url", ""))
        if issue_number:
            issue_numbers.append(issue_number)
        if issue_url:
            issue_urls.append(issue_url)

    if _can_reconcile_incidents(health):
        for issue in issues:
            body = str(issue.get("body", ""))
            incident_id = _marker_value(body, "incident")
            if not incident_id or incident_id in active_ids or str(issue.get("state", "")) != "open":
                continue
            number = int(issue["number"])
            if number in superseded:
                client.add_comment(number, f"相同来源与原因继续由 #{superseded[number]} 跟踪；本记录仅被归并，不代表事故恢复，原证据保留。")
                client.update_issue(number, state="closed", labels=["robtaxi-health", "health-superseded"])
                continue
            client.add_comment(
                number,
                f"最新有效自检未再发现该事件，按来源级健康规则标记恢复：`{request_id}`。",
            )
            client.update_issue(
                number,
                state="closed",
                labels=["robtaxi-health", "health-recovered"],
            )

    refreshed = client.list_health_issues()
    stale = stale_pending_issues(refreshed)
    alert_text = build_alert_text(health, issue_urls, stale)
    return {
        "issue_url": issue_urls[0] if issue_urls else "",
        "issue_number": issue_numbers[0] if issue_numbers else 0,
        "issue_urls": issue_urls,
        "issue_numbers": issue_numbers,
        "should_alert": bool(alert_text),
        "alert_text": alert_text,
        "stale_count": len(stale),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Synchronize Robtaxi health findings to GitHub Issues")
    parser.add_argument("--health-report", required=True)
    parser.add_argument("--repair-request", default="")
    parser.add_argument("--artifact-name", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument("--out", required=True)
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args()

    if not args.token:
        raise SystemExit("[health_issue] missing GITHUB_TOKEN")

    health = read_json(Path(args.health_report).expanduser().resolve())
    repair_path = Path(args.repair_request).expanduser().resolve() if args.repair_request else None
    repair = read_json(repair_path) if repair_path is not None and repair_path.exists() else None
    client = GitHubClient(args.token, args.repository)
    result = sync_health_issue(
        client=client,
        health=health,
        repair=repair,
        artifact_name=args.artifact_name,
    )

    out_file = Path(args.out).expanduser().resolve()
    write_json(out_file, result)
    alert_file = out_file.with_name("health_alert.txt")
    alert_file.write_text(str(result["alert_text"]), encoding="utf-8")
    _write_github_output(
        args.github_output,
        {
            "issue_url": str(result["issue_url"]),
            "issue_number": str(result["issue_number"]),
            "issue_urls_json": json.dumps(result["issue_urls"], ensure_ascii=False),
            "issue_numbers_json": json.dumps(result["issue_numbers"]),
            "should_alert": str(bool(result["should_alert"])).lower(),
            "stale_count": str(result["stale_count"]),
            "alert_file": str(alert_file),
        },
    )
    print(
        f"[health_issue] issue={result['issue_number'] or '-'} "
        f"alert={result['should_alert']} stale={result['stale_count']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
