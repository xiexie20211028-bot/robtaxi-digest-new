#!/usr/bin/env python3
"""受控迁移 GitHub Project 任务状态，并在返回前读回确认。"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from subprocess import run
from pathlib import Path
from typing import Any

from validate_project_task import DEFAULT_CONFIG, GovernanceError, issue_number_from_ref, load_config


QUERY = """
query($owner: String!, $number: Int!, $after: String) {
  user(login: $owner) {
    projectV2(number: $number) {
      id
      fields(first: 100) { nodes { ... on ProjectV2SingleSelectField { id name options { id name } } } }
      items(first: 20, after: $after) {
        pageInfo { hasNextPage endCursor }
        nodes {
          id
          fieldValues(first: 30) { nodes { ... on ProjectV2ItemFieldSingleSelectValue { name field { ... on ProjectV2FieldCommon { name } } } } }
          content { ... on Issue { number } }
        }
      }
    }
  }
}
"""
MUTATION = """
mutation($projectId: ID!, $itemId: ID!, $fieldId: ID!, $optionId: String!) {
  updateProjectV2ItemFieldValue(input: {projectId: $projectId, itemId: $itemId, fieldId: $fieldId, value: {singleSelectOptionId: $optionId}}) {
    projectV2Item { id }
  }
}
"""


def graphql(token: str, query: str, variables: dict[str, Any]) -> dict[str, Any]:
    payload = json.dumps({"query": query, "variables": variables})
    result = run(
        ["gh", "api", "graphql", "--input", "-"], input=payload, text=True, capture_output=True,
        timeout=20, check=False, env={"PATH": os.environ.get("PATH", ""), "GH_TOKEN": token, "NO_COLOR": "1"},
    )
    try:
        data = json.loads(result.stdout) if result.returncode == 0 else None
    except json.JSONDecodeError:
        data = None
    if not isinstance(data, dict) or data.get("errors"):
        raise GovernanceError("GitHub Project 状态迁移查询失败；已停止")
    return data


def find_issue_item(config: dict[str, Any], issue: int, token: str) -> tuple[dict[str, Any], dict[str, Any]]:
    after: str | None = None
    while True:
        data = graphql(token, QUERY, {"owner": config["project_owner"], "number": int(config["project_number"]), "after": after})
        project = (((data.get("data") or {}).get("user") or {}).get("projectV2") or {})
        items = project.get("items") if isinstance(project, dict) else None
        if not isinstance(items, dict):
            raise GovernanceError("目标 Project 不存在或无权读取")
        for item in items.get("nodes", []):
            content = item.get("content") if isinstance(item, dict) else None
            if isinstance(content, dict) and int(content.get("number", -1)) == issue:
                return project, item
        page = items.get("pageInfo")
        if not isinstance(page, dict) or not page.get("hasNextPage"):
            break
        after = str(page.get("endCursor") or "")
        if not after:
            break
    raise GovernanceError(f"Issue #{issue} 未加入 Robotaxi Digest 产品研发总盘")


def current_status(item: dict[str, Any]) -> str:
    for value in (item.get("fieldValues") or {}).get("nodes", []):
        field = value.get("field") if isinstance(value, dict) else None
        if isinstance(field, dict) and field.get("name") == "Status":
            return str(value.get("name", ""))
    return ""


def transition(config: dict[str, Any], issue: int, status: str, token: str, retries: int, delay: float) -> dict[str, Any]:
    project, item = find_issue_item(config, issue, token)
    status_field = next((field for field in project.get("fields", {}).get("nodes", []) if field.get("name") == "Status"), None)
    option = next((opt for opt in (status_field or {}).get("options", []) if opt.get("name") == status), None)
    if not status_field or not option:
        raise GovernanceError(f"Project 不存在 Status 选项：{status}")
    graphql(token, MUTATION, {"projectId": project["id"], "itemId": item["id"], "fieldId": status_field["id"], "optionId": option["id"]})
    for attempt in range(1, retries + 1):
        _, verified = find_issue_item(config, issue, token)
        if current_status(verified) == status:
            return {"issue": issue, "status": status, "attempts": attempt, "item_id": verified["id"]}
        if attempt < retries:
            time.sleep(delay)
    raise GovernanceError(f"Issue #{issue} 状态写入后未读回 {status}；已停止后续 PR 操作")


def main() -> int:
    parser = argparse.ArgumentParser(description="受控迁移 Robotaxi Project 任务状态")
    parser.add_argument("--issue", required=True)
    parser.add_argument("--status", required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--retry-delay", type=float, default=1.0)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        token = os.environ.get("ROBTAXI_PROJECT_WRITE_TOKEN") or os.environ.get("GH_TOKEN")
        if not token:
            raise GovernanceError("缺少 ROBTAXI_PROJECT_WRITE_TOKEN；已停止状态迁移")
        result = transition(load_config(Path(args.config)), issue_number_from_ref(args.issue), args.status, token, max(1, args.retries), max(0.0, args.retry_delay))
    except GovernanceError as exc:
        print(f"[project-status-transition] FAIL: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False) if args.json else f"[project-status-transition] PASS: Issue #{result['issue']} → {result['status']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
