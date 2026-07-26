from __future__ import annotations

import argparse
import json
import os
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
}


def run_marker(run_id: str) -> str:
    return f"<!-- robtaxi-health-run:{run_id} -->"


def find_issue_for_run(issues: list[dict[str, Any]], run_id: str) -> dict[str, Any] | None:
    marker = run_marker(run_id)
    for issue in issues:
        if "pull_request" in issue:
            continue
        if marker in str(issue.get("body", "")):
            return issue
    return None


def build_issue_body(health: dict[str, Any], repair: dict[str, Any] | None, artifact_name: str) -> str:
    run = health.get("run", {})
    findings = health.get("findings", [])
    lines = [
        run_marker(str(run.get("github_run_id", ""))),
        f"<!-- robtaxi-health-request:{health.get('request_id', '')} -->",
        "## Robtaxi 自动运行自检",
        "",
        f"- 状态：`{health.get('overall_status', '')}`",
        f"- 统计日期：`{health.get('date_bj', '')}`",
        f"- request_id：`{health.get('request_id', '')}`",
        f"- commit：`{run.get('commit_sha', '')}`",
        f"- GitHub Run：{run.get('run_url', '')}",
        f"- 健康 artifact：`{artifact_name}`",
        "",
        "## Findings",
        "",
    ]
    if not findings:
        lines.append("- 无异常。")
    else:
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
    issue_url: str,
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
        if issue_url:
            lines.append(f"待诊断 Issue：{issue_url}")

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
        rows = self.request("GET", f"/repos/{self.repository}/issues?{query}")
        return [row for row in rows if isinstance(row, dict) and "pull_request" not in row]

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


def sync_health_issue(
    *,
    client: GitHubClient,
    health: dict[str, Any],
    repair: dict[str, Any] | None,
    artifact_name: str,
) -> dict[str, Any]:
    client.ensure_labels()
    issues = client.list_health_issues()
    run_id = str(health.get("run", {}).get("github_run_id", ""))
    existing = find_issue_for_run(issues, run_id)
    status = str(health.get("overall_status", "healthy"))
    issue_url = ""
    issue_number = 0

    if status == "healthy":
        if existing is not None and str(existing.get("state", "")) == "open":
            issue_number = int(existing["number"])
            client.add_comment(
                issue_number,
                f"对应 GitHub Run 的最新重试已恢复健康：`{health.get('request_id', '')}`。",
            )
            updated = client.update_issue(
                issue_number,
                state="closed",
                labels=["robtaxi-health", "health-recovered"],
            )
            issue_url = str(updated.get("html_url", existing.get("html_url", "")))
    else:
        title = f"[Robtaxi Health][{status.upper()}][{health.get('date_bj', '')}]"
        body = build_issue_body(health, repair, artifact_name)
        labels = ["robtaxi-health", f"health-{status}", "proposal-pending"]
        if existing is None:
            updated = client.create_issue(title, body, labels)
        else:
            issue_number = int(existing["number"])
            updated = client.update_issue(
                issue_number,
                title=title,
                body=body,
                state="open",
                labels=labels,
            )
        issue_number = int(updated.get("number", issue_number))
        issue_url = str(updated.get("html_url", ""))

    refreshed = client.list_health_issues()
    stale = stale_pending_issues(refreshed)
    alert_text = build_alert_text(health, issue_url, stale)
    return {
        "issue_url": issue_url,
        "issue_number": issue_number,
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
