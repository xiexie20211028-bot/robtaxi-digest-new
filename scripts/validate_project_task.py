#!/usr/bin/env python3
"""只读校验研发任务是否符合 Robotaxi Digest 总盘治理规则。"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from http.client import IncompleteRead
from pathlib import Path
from subprocess import TimeoutExpired, run
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / ".github" / "robtaxi-project-governance.json"
GRAPHQL_URL = "https://api.github.com/graphql"
PROJECT_PAGE_SIZE = 20


class GovernanceError(RuntimeError):
    """任务不满足治理规则或无法安全验证时抛出。"""


def load_config(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GovernanceError(f"无法读取治理配置：{exc}") from exc
    if not isinstance(payload, dict):
        raise GovernanceError("治理配置必须是 JSON object")
    return payload


def issue_number_from_ref(value: str) -> int:
    text = str(value).strip()
    if re.fullmatch(r"#?\d+", text):
        return int(text.lstrip("#"))
    match = re.fullmatch(r"https://github\.com/[^/]+/[^/]+/issues/(\d+)/?", text)
    if match is None:
        raise GovernanceError("--issue 必须是 Issue 编号或完整 Issue URL")
    return int(match.group(1))


def primary_issue_from_pr_body(body: str) -> int:
    match = re.search(r"(?im)^\s*Primary task:\s*(?:Fixes|Closes)\s+#(\d+)\s*$", body or "")
    if not match:
        raise GovernanceError("PR 正文必须包含精确的 `Primary task: Fixes #<number>`")
    return int(match.group(1))


def _graphql_query() -> str:
    return """
query($owner: String!, $number: Int!, $after: String) {
  user(login: $owner) {
    projectV2(number: $number) {
      title
      items(first: __PROJECT_PAGE_SIZE__, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          fieldValues(first: 30) {
            nodes {
              ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2FieldCommon { name } } }
              ... on ProjectV2ItemFieldNumberValue { number field { ... on ProjectV2FieldCommon { name } } }
              ... on ProjectV2ItemFieldTextValue { text field { ... on ProjectV2FieldCommon { name } } }
            }
          }
          content {
            ... on Issue {
              number url title body state
              repository { nameWithOwner }
              assignees(first: 10) { totalCount }
              labels(first: 100) { nodes { name } }
              blockedBy(first: 100) { nodes { number state url } }
            }
          }
        }
      }
    }
  }
}
""".replace("__PROJECT_PAGE_SIZE__", str(PROJECT_PAGE_SIZE))


def request_graphql(token: str, variables: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps({"query": _graphql_query(), "variables": variables}).encode("utf-8")
    request = Request(
        GRAPHQL_URL,
        data=body,
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "robtaxi-project-task-gate",
        },
    )
    try:
        with urlopen(request, timeout=15) as response:  # nosec B310: 固定 GitHub API 地址
            payload = json.loads(response.read().decode("utf-8"))
    except IncompleteRead:
        return request_graphql_via_gh(token, body)
    except (HTTPError, URLError, TimeoutError, OSError, json.JSONDecodeError) as exc:
        raise GovernanceError("无法读取 GitHub Project；为避免绕过门禁，已停止校验") from exc
    if not isinstance(payload, dict) or payload.get("errors"):
        raise GovernanceError("GitHub Project 查询失败；为避免绕过门禁，已停止校验")
    return payload


def request_graphql_via_gh(token: str, body: bytes) -> dict[str, Any]:
    """在 urllib 响应被截断时，通过 gh 安全重试同一只读查询。"""
    try:
        result = run(
            ["gh", "api", "graphql", "--input", "-"],
            input=body.decode("utf-8"),
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
            env={"PATH": os.environ.get("PATH", ""), "GH_TOKEN": token, "NO_COLOR": "1"},
        )
        payload = json.loads(result.stdout) if result.returncode == 0 else None
    except (OSError, TimeoutExpired, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise GovernanceError("无法读取 GitHub Project；为避免绕过门禁，已停止校验") from exc
    if not isinstance(payload, dict) or payload.get("errors"):
        raise GovernanceError("GitHub Project 查询失败；为避免绕过门禁，已停止校验")
    return payload


def fetch_issue_item(config: dict[str, Any], issue_number: int, token: str) -> dict[str, Any]:
    after: str | None = None
    while True:
        response = request_graphql(
            token,
            {
                "owner": str(config["project_owner"]),
                "number": int(config["project_number"]),
                "after": after,
            },
        )
        project = (((response.get("data") or {}).get("user") or {}).get("projectV2") or {})
        items = project.get("items") if isinstance(project, dict) else None
        if not isinstance(items, dict):
            raise GovernanceError("目标 Project 不存在或无权读取")
        nodes = items.get("nodes")
        if not isinstance(nodes, list):
            raise GovernanceError("GitHub Project 返回了无效项目列表")
        for item in nodes:
            content = item.get("content") if isinstance(item, dict) else None
            if isinstance(content, dict) and int(content.get("number", -1)) == issue_number:
                return item
        page = items.get("pageInfo")
        if not isinstance(page, dict) or not page.get("hasNextPage"):
            break
        after = str(page.get("endCursor") or "")
        if not after:
            raise GovernanceError("Project 分页游标缺失；为避免漏检，已停止校验")
    raise GovernanceError(f"Issue #{issue_number} 未加入 Robotaxi Digest 产品研发总盘")


def item_fields(item: dict[str, Any]) -> dict[str, Any]:
    values = (item.get("fieldValues") or {}).get("nodes", [])
    if not isinstance(values, list):
        raise GovernanceError("Project 字段返回格式无效")
    result: dict[str, Any] = {}
    for value in values:
        if not isinstance(value, dict):
            continue
        field = value.get("field")
        name = field.get("name") if isinstance(field, dict) else None
        if not name:
            continue
        if "name" in value:
            result[str(name)] = value["name"]
        elif "number" in value:
            result[str(name)] = value["number"]
        elif "text" in value:
            result[str(name)] = value["text"]
    return result


def _section_has_content(body: str, title: str) -> bool:
    pattern = rf"(?ms)^## {re.escape(title)}\s*$\n(.*?)(?=^## |\Z)"
    match = re.search(pattern, body)
    return bool(match and match.group(1).strip())


def expected_score(fields: dict[str, Any], effort_penalty: dict[str, Any]) -> float:
    try:
        effort = str(fields["Effort"])
        return (
            float(fields["Impact"]) * 3
            + float(fields["Urgency"]) * 2
            + float(fields["Reach"]) * 2
            + float(fields["Recurrence"])
            - float(effort_penalty[effort])
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise GovernanceError("无法计算 Priority Score") from exc


def validate_item(
    item: dict[str, Any],
    config: dict[str, Any],
    phase: str,
    *,
    is_draft: bool = False,
    expected_issue_number: int | None = None,
) -> dict[str, Any]:
    content = item.get("content")
    if not isinstance(content, dict) or "number" not in content:
        raise GovernanceError("主任务必须是正式 GitHub Issue，不能是 Project Draft 或 Pull Request")
    issue_number = int(content["number"])
    if expected_issue_number is not None and issue_number != expected_issue_number:
        raise GovernanceError("PR 主任务与请求校验的 Issue 不一致")
    if content.get("repository", {}).get("nameWithOwner") != config.get("repository"):
        raise GovernanceError("Issue 不属于配置的仓库")
    if str(content.get("state", "")).upper() != "OPEN":
        raise GovernanceError("已关闭 Issue 不能作为正在执行的主任务")
    if int((content.get("assignees") or {}).get("totalCount", 0)) < 1:
        raise GovernanceError("主任务必须分配负责人")
    labels = (content.get("labels") or {}).get("nodes", [])
    label_names = {
        str(label.get("name"))
        for label in labels
        if isinstance(label, dict) and label.get("name")
    }
    if label_names.intersection(set(config.get("health_issue_labels", []))):
        raise GovernanceError("自动 Health Issue 不能直接作为工程 PR 的主任务")
    blockers = (content.get("blockedBy") or {}).get("nodes", [])
    if any(isinstance(blocker, dict) and str(blocker.get("state", "")).upper() == "OPEN" for blocker in blockers):
        raise GovernanceError("主任务存在开放的 Blocked by 依赖")

    fields = item_fields(item)
    missing = [name for name in config.get("required_fields", []) if fields.get(name) in (None, "")]
    if missing:
        raise GovernanceError(f"总盘字段未填写：{', '.join(missing)}")
    if fields.get("Task Type") == "Epic":
        raise GovernanceError("Epic 不能直接作为工程 PR 的主任务")
    if fields.get("Task Type") not in set(config.get("executable_task_types", [])):
        raise GovernanceError(f"Task Type 不允许执行：{fields.get('Task Type')}")
    if fields.get("Priority") not in set(config.get("allowed_priorities", [])):
        raise GovernanceError(f"Priority 不合法：{fields.get('Priority')}")
    if fields.get("Status") in {"Inbox", "已评估", "待开发", "观察中", "已取消"}:
        raise GovernanceError(f"当前 Status 不允许执行：{fields.get('Status')}")
    for name in ("Impact", "Urgency", "Reach", "Recurrence"):
        try:
            value = float(fields[name])
        except (TypeError, ValueError) as exc:
            raise GovernanceError(f"{name} 必须为 1 到 5 的数字") from exc
        if value != int(value) or not 1 <= value <= 5:
            raise GovernanceError(f"{name} 必须为 1 到 5 的整数")
    score = expected_score(fields, config.get("effort_penalty", {}))
    try:
        actual_score = float(fields["Priority Score"])
    except (TypeError, ValueError) as exc:
        raise GovernanceError("Priority Score 必须是数字") from exc
    if actual_score != score:
        raise GovernanceError(f"Priority Score 应为 {score:g}，当前为 {actual_score:g}")

    body = str(content.get("body") or "")
    missing_sections = [title for title in config.get("required_sections", []) if not _section_has_content(body, title)]
    if missing_sections:
        raise GovernanceError(f"Issue 正文缺少有效章节：{', '.join(missing_sections)}")
    if not re.search(r"(?m)^- \[[ xX]\] .+", body):
        raise GovernanceError("验收标准必须包含 checklist")

    status = str(fields["Status"])
    if phase == "preflight" and status != config.get("preflight_status"):
        raise GovernanceError("preflight 要求任务状态为“开发中”")
    if phase == "pr":
        allowed = config.get("draft_pr_statuses") if is_draft else [config.get("ready_pr_status")]
        if status not in allowed:
            raise GovernanceError(f"PR 当前状态不合法：{status}")
    if phase == "postflight" and status not in config.get("postflight_statuses", []):
        raise GovernanceError("postflight 要求任务状态为“待验证”或“已完成”")
    if phase == "postflight" and status == "已完成" and "验收完成" not in body:
        raise GovernanceError("无 PR 完成任务必须在 Issue 正文写明“验收完成”")
    return {"issue": issue_number, "status": status, "priority_score": actual_score, "phase": phase}


def event_pr_context(path: Path) -> tuple[int, bool]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
        pull_request = payload["pull_request"]
        return primary_issue_from_pr_body(str(pull_request.get("body") or "")), bool(pull_request.get("draft"))
    except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
        raise GovernanceError("无法从 GitHub event 读取 PR 主任务") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description="只读校验 Robotaxi Digest 研发任务治理规则")
    parser.add_argument("--issue", default="", help="Issue 编号或 URL；PR 阶段可从 --event 推导")
    parser.add_argument("--phase", choices=("preflight", "pr", "postflight"), required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--event", default="", help="GitHub pull_request event JSON")
    parser.add_argument("--draft", action="store_true", help="本地模拟 Draft PR")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        config = load_config(Path(args.config))
        issue_number = issue_number_from_ref(args.issue) if args.issue else 0
        is_draft = args.draft
        if args.phase == "pr" and args.event:
            event_number, is_draft = event_pr_context(Path(args.event))
            if issue_number and issue_number != event_number:
                raise GovernanceError("--issue 与 PR Primary task 不一致")
            issue_number = event_number
        if not issue_number:
            raise GovernanceError("必须提供 --issue，或为 PR 阶段提供 --event")
        token = os.environ.get("ROBTAXI_PROJECT_READ_TOKEN") or os.environ.get("GH_TOKEN")
        if not token:
            raise GovernanceError("缺少 ROBTAXI_PROJECT_READ_TOKEN；为避免绕过门禁，已停止校验")
        item = fetch_issue_item(config, issue_number, token)
        result = validate_item(item, config, args.phase, is_draft=is_draft, expected_issue_number=issue_number)
    except GovernanceError as exc:
        print(f"[project-task-gate] FAIL: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(f"[project-task-gate] PASS: Issue #{result['issue']} ({result['phase']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
