"""将健康闭环决策安全同步到 GitHub 元数据。

本模块不执行代码修改、分支、PR、合并或生产操作。所有 GitHub 命令均以
参数数组调用，Issue、网页和日志文本不会进入 shell。
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .common import read_json, write_json
from .health_loop import action_fingerprint


SCHEMA_VERSION = "robtaxi-health-loop-sync-v1"
DECISION_SCHEMA = "robtaxi-health-loop-v1"
STATE_SCHEMA = "robtaxi-health-loop-official-state-v1"
REPOSITORY = "xiexie20211028-bot/robtaxi-digest-new"
PROJECT_OWNER = "xiexie20211028-bot"
PROJECT_NUMBER = 3
MAX_BATCHES = 3
REOPEN_WINDOW_DAYS = 30
ALLOWED_ACTIONS = {"observe", "create_task", "verify", "close", "needs_approval", "no_code_change"}
KNOWN_ENGINEERING_ISSUES = {
    "miit_news_structured": 49,
    "pony_news_structured": 52,
    "singapore_lta_news_structured": 57,
    "zoox_news_structured": 58,
}
KEY_RE = re.compile(r"^[a-z0-9_.:-]{3,160}$")
SOURCE_RE = re.compile(r"^[a-z0-9_]{2,100}$")
MARKER_PREFIX = "<!-- robtaxi-health-loop-incident:"
SYNC_PREFIX = "<!-- robtaxi-health-loop-sync:"


class SyncError(RuntimeError):
    """输入、正式状态或 GitHub 操作不满足 fail-closed 条件。"""


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[dict[str, Any]]:
    return [row for row in value if isinstance(row, dict)] if isinstance(value, list) else []


def _safe_key(value: Any, *, field: str) -> str:
    text = str(value or "").strip().lower()
    if not KEY_RE.fullmatch(text):
        raise SyncError(f"{field} 格式不安全或缺失")
    return text


def _safe_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text and not SOURCE_RE.fullmatch(text):
        raise SyncError("source_id 格式不安全")
    return text


def _incident_marker(key: str) -> str:
    return f"{MARKER_PREFIX}{key} -->"


def _sync_marker(fingerprint: str) -> str:
    return f"{SYNC_PREFIX}{fingerprint} -->"


def validate_decision(decision: dict[str, Any]) -> list[dict[str, Any]]:
    if decision.get("schema_version") != DECISION_SCHEMA:
        raise SyncError("decision schema_version 不受支持")
    if not str(decision.get("run_id", "")).strip():
        raise SyncError("decision 缺少 run_id")
    if decision.get("state_origin") not in {"local_cache", "github_reconstructed"}:
        raise SyncError("decision 缺少可信 state_origin")
    actions = _list(decision.get("changed_actions"))
    if len(actions) > MAX_BATCHES:
        raise SyncError(f"单日异常批次超过 {MAX_BATCHES}，已停止同步")
    result: list[dict[str, Any]] = []
    seen_keys: set[str] = set()
    for raw in actions:
        row = dict(raw)
        action = str(row.get("action", ""))
        if action not in ALLOWED_ACTIONS:
            raise SyncError(f"不允许的 action：{action}")
        row["incident_key"] = _safe_key(row.get("incident_key"), field="incident_key")
        if row["incident_key"] in seen_keys:
            raise SyncError("同一次 decision 包含重复 incident_key")
        seen_keys.add(row["incident_key"])
        row["source_id"] = _safe_source(row.get("source_id"))
        fingerprint = str(row.get("action_fingerprint", ""))
        if not re.fullmatch(r"[0-9a-f]{20}", fingerprint):
            raise SyncError("action_fingerprint 缺失或不合法")
        if fingerprint != action_fingerprint(row):
            raise SyncError("action_fingerprint 与结构化动作不一致")
        row["action_fingerprint"] = fingerprint
        if action in {"verify", "close"}:
            evidence = _dict(row.get("source_run_evidence"))
            if decision.get("state_origin") != "github_reconstructed":
                raise SyncError("记录恢复前必须从 GitHub 正式状态重建")
            if row.get("category") != "source" or row.get("risk") == "High":
                raise SyncError("只有非 High 的单来源工程任务可以自动验证或关闭")
            minimum = 2 if action == "close" else 1
            if int(row.get("recovery_count", 0) or 0) < minimum:
                raise SyncError("恢复次数不足")
            if str(evidence.get("run_id", "")) != str(decision.get("run_id", "")):
                raise SyncError("来源运行证据与 decision 运行不一致")
            if str(evidence.get("event_name", "")) != "schedule":
                raise SyncError("手动运行不能作为关闭证据")
            if not evidence.get("health_report_available") or not evidence.get("source_participated"):
                raise SyncError("关闭任务缺少完整来源运行证据")
            if not row.get("merged_commit_reachable"):
                raise SyncError("关闭任务缺少生产已包含合并版本的证据")
        result.append(row)
    return result


def build_sync_plan(decision: dict[str, Any], official_state: dict[str, Any]) -> list[dict[str, Any]]:
    actions = validate_decision(decision)
    if official_state.get("schema_version") != STATE_SCHEMA or not official_state.get("complete"):
        raise SyncError("GitHub/Project 正式状态不完整")
    incidents = _dict(official_state.get("incidents"))
    operations: list[dict[str, Any]] = []
    for row in actions:
        action = str(row["action"])
        if action in {"observe", "needs_approval", "no_code_change"}:
            continue
        key = str(row["incident_key"])
        current = _dict(incidents.get(key))
        fingerprint = str(row["action_fingerprint"])
        if fingerprint in set(current.get("applied_fingerprints", [])):
            continue
        # 本地 decision 中的 Issue 编号不是正式状态，不能覆盖 GitHub 重建结果。
        issue_number = int(
            current.get("engineering_issue")
            or KNOWN_ENGINEERING_ISSUES.get(str(row.get("source_id", "")), 0)
            or 0
        )
        if action in {"verify", "close"} and not issue_number:
            raise SyncError(f"{action} 缺少正式工程 Issue")
        if action == "close":
            previous = _dict(current.get("latest_sync"))
            if previous.get("action") != "verify" or int(previous.get("recovery_count", 0) or 0) != 1:
                raise SyncError("GitHub 正式状态没有第一次恢复记录，禁止关闭")
            if str(previous.get("run_id", "")) == str(row.get("last_run_id", "")):
                raise SyncError("两次恢复不能来自同一个运行")
        if action in {"verify", "close"}:
            if not current.get("merged_commit_reachable"):
                raise SyncError("GitHub 正式状态无法证明生产已包含合并版本")
            if str(current.get("merged_commit", "")) != str(row.get("merged_commit", "")):
                raise SyncError("decision 的合并版本与 GitHub 正式状态不一致")
        source_evidence = _dict(row.get("source_run_evidence"))
        operations.append(
            {
                "operation": "ensure_task" if action == "create_task" else action,
                "incident_key": key,
                "category": str(row.get("category", "source" if row.get("source_id") else "delivery")),
                "source_id": str(row.get("source_id", "")),
                "reason_code": str(row.get("reason_code", "")),
                "severity": str(row.get("severity", "warning")),
                "risk": str(row.get("risk", "Medium")),
                "occurrence_count": int(row.get("occurrence_count", 0) or 0),
                "issue_number": issue_number or None,
                "reopen": bool(row.get("reopen", False) or current.get("issue_state") == "CLOSED"),
                "action_fingerprint": fingerprint,
                "run_id": str(row.get("last_run_id", decision.get("run_id", ""))),
                "recovery_count": int(row.get("recovery_count", 0) or 0),
                "merged_commit": str(current.get("merged_commit", row.get("merged_commit", ""))),
                "merged_commit_reachable": bool(current.get("merged_commit_reachable", False)),
                "source_run_evidence": {
                    "run_id": str(source_evidence.get("run_id", "")),
                    "event_name": str(source_evidence.get("event_name", "")),
                    "commit_sha": str(source_evidence.get("commit_sha", "")),
                    "health_report_available": bool(source_evidence.get("health_report_available", False)),
                    "source_participated": bool(source_evidence.get("source_participated", False)),
                },
            }
        )
    return operations


@dataclass
class CommandResult:
    stdout: str


class GhMetadataClient:
    """只允许固定参数形式的 GitHub 元数据操作。"""

    def __init__(self, *, repository: str = REPOSITORY, owner: str = PROJECT_OWNER, project: int = PROJECT_NUMBER):
        self.repository = repository
        self.owner = owner
        self.project = project
        self._issues: list[dict[str, Any]] = []
        self._items: list[dict[str, Any]] = []
        self._fields: dict[str, dict[str, Any]] = {}
        self._project_id = ""

    def _run(self, args: list[str]) -> CommandResult:
        env = dict(os.environ)
        env["NO_COLOR"] = "1"
        try:
            result = subprocess.run(
                ["gh", *args],
                text=True,
                capture_output=True,
                check=False,
                timeout=30,
                env=env,
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            raise SyncError("GitHub CLI 不可用或超时，已停止同步") from exc
        if result.returncode != 0:
            raise SyncError("GitHub 元数据操作失败，已停止同步")
        return CommandResult(result.stdout)

    def _json(self, args: list[str]) -> Any:
        try:
            return json.loads(self._run(args).stdout)
        except json.JSONDecodeError as exc:
            raise SyncError("GitHub 返回格式不完整，已停止同步") from exc

    def preflight(self, *, run_evidence: dict[str, Any] | None = None) -> dict[str, Any]:
        repo = self._json(["repo", "view", self.repository, "--json", "nameWithOwner"])
        if _dict(repo).get("nameWithOwner") != self.repository:
            raise SyncError("GitHub 仓库身份不匹配")
        issues = self._json(
            [
                "issue",
                "list",
                "--repo",
                self.repository,
                "--state",
                "all",
                "--limit",
                "1000",
                "--json",
                "number,title,state,body,url,assignees,author,closedAt",
            ]
        )
        project_payload = self._json(
            ["project", "item-list", str(self.project), "--owner", self.owner, "--limit", "1000", "--format", "json"]
        )
        fields_payload = self._json(
            ["project", "field-list", str(self.project), "--owner", self.owner, "--format", "json"]
        )
        self._issues = _list(issues)
        self._items = _list(_dict(project_payload).get("items"))
        if len(self._issues) >= 1000 or len(self._items) >= 1000:
            raise SyncError("GitHub 列表达到分页上限，无法证明正式状态完整")
        self._fields = {
            str(row.get("name", "")): row for row in _list(_dict(fields_payload).get("fields")) if row.get("name")
        }
        required = {"Status", "Priority", "Task Type", "Change Risk", "Target", "Route"}
        if not required.issubset(self._fields):
            raise SyncError("Project 必填字段无法完整识别")
        projects = _list(
            _dict(
                self._json(
                    ["project", "list", "--owner", self.owner, "--limit", "1000", "--format", "json"]
                )
            ).get("projects")
        )
        project = next((row for row in projects if int(row.get("number", 0) or 0) == self.project), {})
        self._project_id = str(project.get("id", ""))
        if not self._project_id:
            raise SyncError("目标 Project 无法识别")
        return self.official_state(run_evidence=run_evidence)

    def validate_write_access(self) -> None:
        """在任何 apply 写入前确认仓库和个人 Project 都可更新。"""
        owner, name = self.repository.split("/", 1)
        query = """
query($owner:String!,$name:String!,$login:String!,$number:Int!) {
  repository(owner:$owner,name:$name) { viewerPermission }
  user(login:$login) { projectV2(number:$number) { viewerCanUpdate } }
}
""".strip()
        payload = _dict(
            self._json(
                [
                    "api",
                    "graphql",
                    "-f",
                    f"query={query}",
                    "-F",
                    f"owner={owner}",
                    "-F",
                    f"name={name}",
                    "-F",
                    f"login={self.owner}",
                    "-F",
                    f"number={self.project}",
                ]
            )
        )
        data = _dict(payload.get("data"))
        permission = str(_dict(data.get("repository")).get("viewerPermission", ""))
        project = _dict(_dict(data.get("user")).get("projectV2"))
        if permission not in {"ADMIN", "MAINTAIN", "WRITE"} or project.get("viewerCanUpdate") is not True:
            raise SyncError("当前 GitHub 凭据没有仓库和 Project 写权限，apply 已停止")

    def _issue_for_key(self, key: str) -> dict[str, Any]:
        marker = _incident_marker(key)
        recent_closed: dict[str, Any] = {}
        cutoff = datetime.now(timezone.utc) - timedelta(days=REOPEN_WINDOW_DAYS)
        for issue in self._issues:
            if marker not in str(issue.get("body", "")):
                continue
            if _dict(issue.get("author")).get("login") != self.owner:
                continue
            if str(issue.get("state", "")).upper() != "CLOSED":
                return issue
            closed_at = str(issue.get("closedAt", ""))
            try:
                closed_time = datetime.fromisoformat(closed_at.replace("Z", "+00:00"))
            except ValueError:
                continue
            if closed_time >= cutoff:
                recent_closed = issue
        return recent_closed

    def _issue_by_number(self, number: int) -> dict[str, Any]:
        return next((row for row in self._issues if int(row.get("number", 0) or 0) == number), {})

    def _project_item(self, number: int) -> dict[str, Any]:
        for item in self._items:
            content = _dict(item.get("content"))
            if int(content.get("number", 0) or 0) == number:
                return item
        return {}

    def _comments(self, number: int) -> list[dict[str, Any]]:
        payload = self._json(
            [
                "api",
                "--paginate",
                "--slurp",
                f"repos/{self.repository}/issues/{number}/comments?per_page=100",
            ]
        )
        pages = payload if isinstance(payload, list) else []
        return [row for page in pages if isinstance(page, list) for row in page if isinstance(row, dict)]

    def _merged_pr_evidence(self, number: int) -> dict[str, Any]:
        query = """
query($owner:String!,$name:String!,$number:Int!,$cursor:String) {
  repository(owner:$owner,name:$name) {
    issue(number:$number) {
      timelineItems(first:100,after:$cursor,itemTypes:[CROSS_REFERENCED_EVENT]) {
        pageInfo { hasNextPage endCursor }
        nodes {
          ... on CrossReferencedEvent {
            source {
              ... on PullRequest {
                number url mergedAt mergeCommit { oid }
                repository { nameWithOwner }
              }
            }
          }
        }
      }
    }
  }
}
""".strip()
        owner, name = self.repository.split("/", 1)
        cursor = ""
        merged: list[dict[str, Any]] = []
        while True:
            args = [
                "api",
                "graphql",
                "-f",
                f"query={query}",
                "-F",
                f"owner={owner}",
                "-F",
                f"name={name}",
                "-F",
                f"number={number}",
            ]
            if cursor:
                args.extend(["-F", f"cursor={cursor}"])
            payload = _dict(self._json(args))
            timeline = _dict(
                _dict(_dict(_dict(payload.get("data")).get("repository")).get("issue")).get("timelineItems")
            )
            if not timeline:
                raise SyncError(f"工程 Issue #{number} 的 PR 关联证据不完整")
            for node in _list(timeline.get("nodes")):
                source = _dict(node.get("source"))
                if (
                    source.get("mergedAt")
                    and _dict(source.get("mergeCommit")).get("oid")
                    and _dict(source.get("repository")).get("nameWithOwner") == self.repository
                ):
                    merged.append(source)
            page = _dict(timeline.get("pageInfo"))
            if not page.get("hasNextPage"):
                break
            cursor = str(page.get("endCursor", ""))
            if not cursor:
                raise SyncError(f"工程 Issue #{number} 的 PR 关联分页不完整")
        if not merged:
            return {}
        latest = max(merged, key=lambda row: str(row.get("mergedAt", "")))
        return {
            "pr_number": int(latest.get("number", 0) or 0),
            "pr_url": str(latest.get("url", "")),
            "merged_at": str(latest.get("mergedAt", "")),
            "merged_commit": str(_dict(latest.get("mergeCommit")).get("oid", "")),
        }

    def _commit_reachable(self, base_commit: str, run_commit: str) -> bool:
        if not re.fullmatch(r"[0-9a-fA-F]{7,40}", base_commit) or not re.fullmatch(
            r"[0-9a-fA-F]{7,40}", run_commit
        ):
            return False
        payload = _dict(
            self._json(["api", f"repos/{self.repository}/compare/{base_commit}...{run_commit}"])
        )
        return str(payload.get("status", "")) in {"ahead", "identical"}

    def _latest_sync(self, comments: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
        latest: dict[str, Any] = {}
        fingerprints: list[str] = []
        for comment in comments:
            if _dict(comment.get("user")).get("login") != self.owner:
                continue
            body = str(comment.get("body", ""))
            for line in body.splitlines():
                line = line.strip()
                if line.startswith(SYNC_PREFIX) and line.endswith(" -->"):
                    fingerprints.append(line[len(SYNC_PREFIX) : -4])
                if line.startswith("<!-- robtaxi-health-loop-state:") and line.endswith(" -->"):
                    raw = line[len("<!-- robtaxi-health-loop-state:") : -4]
                    try:
                        parsed = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(parsed, dict):
                        latest = parsed
        return latest, fingerprints

    def official_state(self, *, run_evidence: dict[str, Any] | None = None) -> dict[str, Any]:
        current_run = _dict(run_evidence)
        run_commit = str(current_run.get("commit_sha", ""))
        incidents: dict[str, Any] = {}
        candidates: dict[int, str] = {number: f"known:{source}" for source, number in KNOWN_ENGINEERING_ISSUES.items()}
        for issue in self._issues:
            if _dict(issue.get("author")).get("login") != self.owner:
                continue
            body = str(issue.get("body", ""))
            for line in body.splitlines():
                line = line.strip()
                if line.startswith(MARKER_PREFIX) and line.endswith(" -->"):
                    candidates[int(issue["number"])] = line[len(MARKER_PREFIX) : -4]
        for number, fallback in candidates.items():
            issue = self._issue_by_number(number)
            if not issue:
                continue
            comments = self._comments(number)
            latest, fingerprints = self._latest_sync(comments)
            key = str(latest.get("incident_key", ""))
            if not key and fallback.startswith("known:"):
                source = fallback.split(":", 1)[1]
                key = next(
                    (
                        str(row.get("incident_key", ""))
                        for row in _list(latest.get("known_incidents"))
                        if row.get("source_id") == source
                    ),
                    "",
                )
            if not key and not fallback.startswith("known:"):
                key = fallback
            if not key:
                continue
            item = self._project_item(number)
            merge_evidence = _dict(latest.get("merge_evidence"))
            if str(item.get("status", "")) == "观察中":
                merge_evidence = self._merged_pr_evidence(number)
            merged_commit = str(merge_evidence.get("merged_commit", latest.get("merged_commit", "")))
            merged_commit_reachable = self._commit_reachable(merged_commit, run_commit)
            incidents[key] = {
                "incident_key": key,
                "source_id": key.split(":", 2)[1] if key.startswith("source:") else "",
                "reason_code": key.split(":", 2)[2] if key.startswith("source:") and key.count(":") >= 2 else "",
                "engineering_issue": number,
                "issue_state": str(issue.get("state", "")),
                "project_status": str(item.get("status", "")),
                "lifecycle": (
                    "closed"
                    if str(issue.get("state", "")).upper() == "CLOSED"
                    else "observing"
                    if str(item.get("status", "")) == "观察中"
                    else "open"
                ),
                "last_run_id": str(latest.get("run_id", "")),
                "severity": str(latest.get("severity", "warning")),
                "risk": str(latest.get("risk", "Medium")),
                "occurrence_count": int(latest.get("occurrence_count", 0) or 0),
                "recovery_count": int(latest.get("recovery_count", 0) or 0),
                "merged_commit": merged_commit,
                "merged_commit_reachable": merged_commit_reachable,
                "merge_evidence": merge_evidence,
                "source_run_evidence": _dict(latest.get("source_run_evidence")),
                "latest_sync": latest,
                "applied_fingerprints": fingerprints,
            }
        return {
            "schema_version": STATE_SCHEMA,
            "complete": True,
            "repository": self.repository,
            "project_number": self.project,
            "incidents": incidents,
            "actions": [
                {
                    "incident_key": key,
                    "action": (
                        "close"
                        if record.get("lifecycle") == "closed"
                        else "verify"
                        if record.get("lifecycle") == "observing"
                        else "observe"
                    ),
                    "severity": record.get("severity", "warning"),
                    "engineering_issue": record.get("engineering_issue"),
                    "recovery_count": record.get("recovery_count", 0),
                    "risk": record.get("risk", "Medium"),
                }
                for key, record in incidents.items()
            ],
            "state_origin": "github_reconstructed",
            "run_evidence": current_run,
        }

    def _field_option(self, field_name: str, option_name: str) -> tuple[str, str]:
        field = _dict(self._fields.get(field_name))
        for option in _list(field.get("options")):
            if option.get("name") == option_name:
                return str(field.get("id", "")), str(option.get("id", ""))
        raise SyncError(f"Project 字段 {field_name} 缺少选项 {option_name}")

    def _set_field(self, item_id: str, field_name: str, option_name: str) -> None:
        field_id, option_id = self._field_option(field_name, option_name)
        if not self._project_id:
            raise SyncError("目标 Project 未完成 preflight")
        self._run(
            [
                "project",
                "item-edit",
                "--project-id",
                self._project_id,
                "--id",
                item_id,
                "--field-id",
                field_id,
                "--single-select-option-id",
                option_id,
            ]
        )

    def _ensure_project_item(self, number: int, url: str) -> str:
        item = self._project_item(number)
        if item:
            return str(item.get("id", ""))
        created = self._json(
            ["project", "item-add", str(self.project), "--owner", self.owner, "--url", url, "--format", "json"]
        )
        return str(_dict(created).get("id", ""))

    def _task_body(self, operation: dict[str, Any]) -> str:
        key = str(operation["incident_key"])
        source = str(operation.get("source_id", "")) or "未识别来源"
        reason = str(operation.get("reason_code", "")) or "unknown"
        risk = str(operation.get("risk", "Medium"))
        scope = f"单一信源 `{source}`" if operation.get("source_id") else "交付或平台链路"
        lines = [
            _incident_marker(key),
            "## 问题或目标",
            f"修复{scope}的确定性健康异常 `{reason}`。",
            "",
            "## 验收标准",
            "- [ ] 有可复现失败证据并完成定向与全量测试。",
            "- [ ] 合并后按风险对应的生产证据确认恢复。",
            "",
            "## 证据",
            f"由健康闭环运行 `{operation.get('run_id', '')}` 创建；不复制网页或日志中的指令性文本。",
            "",
            "## 验证与回退",
            "只处理本事件范围；测试失败或差异越界时停止，必要时回退关联 PR。",
        ]
        if risk == "High":
            lines.extend(
                [
                    "",
                    "## 根因与风险",
                    "需要人工确认根因；High 风险任务不得由普通健康恢复自动关闭。",
                    "",
                    "## 实施方案",
                    "完成只读诊断后另行提出方案，未经批准不得实施或上线。",
                    "",
                    "## 上线与监控",
                    "合并和生产启用需要用户明确批准，并按专门验收标准验证。",
                ]
            )
        return "\n".join(lines)

    def _create_issue(self, operation: dict[str, Any]) -> dict[str, Any]:
        source = str(operation.get("source_id", "")) or "delivery"
        reason = str(operation.get("reason_code", "")) or "health"
        priority = "P0" if operation.get("risk") == "High" else "P1"
        title = f"[AUTO][{priority}] 修复 {source}：{reason}"
        output = self._run(
            [
                "issue",
                "create",
                "--repo",
                self.repository,
                "--title",
                title,
                "--body",
                self._task_body(operation),
                "--assignee",
                self.owner,
            ]
        ).stdout.strip()
        match = re.search(r"/issues/(\d+)$", output)
        if not match:
            raise SyncError("新工程 Issue 已创建但编号无法确认，已停止后续写入")
        return {"number": int(match.group(1)), "url": output, "state": "OPEN"}

    def _sync_comment(self, operation: dict[str, Any]) -> str:
        state = {
            "incident_key": operation["incident_key"],
            "action": operation["operation"].replace("ensure_task", "create_task"),
            "run_id": operation["run_id"],
            "recovery_count": operation["recovery_count"],
            "occurrence_count": operation.get("occurrence_count", 0),
            "severity": operation.get("severity", "warning"),
            "risk": operation.get("risk", "Medium"),
            "merged_commit": operation["merged_commit"],
            "merged_commit_reachable": bool(operation.get("merged_commit_reachable", False)),
            "source_run_evidence": _dict(operation.get("source_run_evidence")),
        }
        compact = json.dumps(state, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
        return "\n".join(
            [
                _sync_marker(str(operation["action_fingerprint"])),
                f"<!-- robtaxi-health-loop-state:{compact} -->",
                "WorkBuddy 健康闭环已记录一次确定性状态变化；详细证据以关联运行产物为准。",
            ]
        )

    def apply(self, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        # 所有只读检查在第一笔写入前完成。
        for operation in operations:
            number = int(operation.get("issue_number") or 0)
            if number and not self._issue_by_number(number):
                raise SyncError(f"工程 Issue #{number} 不存在，已停止同步")
            if operation["operation"] == "ensure_task":
                for field, option in (
                    ("Status", "待办"),
                    ("Priority", "P0" if operation.get("risk") == "High" else "P1"),
                    ("Task Type", "Bug"),
                    ("Change Risk", str(operation.get("risk", "Medium"))),
                    ("Target", "本周"),
                    ("Route", "共同"),
                ):
                    self._field_option(field, option)
            elif operation["operation"] == "verify":
                self._field_option("Status", "观察中")
            elif operation["operation"] == "close":
                self._field_option("Status", "已完成")
        for operation in operations:
            number = int(operation.get("issue_number") or 0)
            issue = self._issue_by_number(number) if number else self._issue_for_key(str(operation["incident_key"]))
            if not issue:
                if operation["operation"] != "ensure_task":
                    raise SyncError("正式工程 Issue 缺失，已停止同步")
                issue = self._create_issue(operation)
                self._issues.append(issue)
            number = int(issue["number"])
            url = str(issue.get("url", f"https://github.com/{self.repository}/issues/{number}"))
            if operation["operation"] == "ensure_task" and str(issue.get("state", "")).upper() == "CLOSED":
                self._run(["issue", "reopen", str(number), "--repo", self.repository])
            item_id = self._ensure_project_item(number, url)
            if not item_id:
                raise SyncError("工程 Issue 无法加入 Project，已停止同步")
            if operation["operation"] == "ensure_task":
                for field, option in (
                    ("Status", "待办"),
                    ("Priority", "P0" if operation.get("risk") == "High" else "P1"),
                    ("Task Type", "Bug"),
                    ("Change Risk", str(operation.get("risk", "Medium"))),
                    ("Target", "本周"),
                    ("Route", "共同"),
                ):
                    self._set_field(item_id, field, option)
                self._run(["issue", "edit", str(number), "--repo", self.repository, "--add-assignee", self.owner])
            elif operation["operation"] == "verify":
                self._set_field(item_id, "Status", "观察中")
            elif operation["operation"] == "close":
                self._set_field(item_id, "Status", "已完成")
                self._run(["issue", "close", str(number), "--repo", self.repository, "--reason", "completed"])
            self._run(
                ["issue", "comment", str(number), "--repo", self.repository, "--body", self._sync_comment(operation)]
            )
            results.append({"operation": operation["operation"], "issue_number": number, "status": "applied"})
        return results


def main() -> int:
    parser = argparse.ArgumentParser(description="安全同步 WorkBuddy 健康闭环元数据")
    parser.add_argument("--decision", required=True)
    parser.add_argument("--mode", choices=("shadow", "apply"), required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--state-out", default="")
    args = parser.parse_args()
    try:
        decision = read_json(Path(args.decision))
        client = GhMetadataClient()
        official = client.preflight(run_evidence=_dict(decision.get("run_evidence")))
        # 缓存丢失时也先落下只读重建结果；调用方据此重新计算后才能 apply。
        if args.state_out:
            write_json(Path(args.state_out), official)
        operations = build_sync_plan(decision, official)
        if args.mode == "apply" and operations:
            client.validate_write_access()
        applied = client.apply(operations) if args.mode == "apply" else []
        if args.mode == "apply" and operations:
            official = client.preflight(run_evidence=_dict(decision.get("run_evidence")))
        result = {
            "schema_version": SCHEMA_VERSION,
            "mode": args.mode,
            "complete": True,
            "operations": operations,
            "applied": applied,
            "official_state": official,
        }
        write_json(Path(args.out), result)
        if args.state_out:
            write_json(Path(args.state_out), official)
    except SyncError as exc:
        print(f"[health-loop-sync] FAIL: {exc}", file=sys.stderr)
        return 1
    print(f"[health-loop-sync] PASS: mode={args.mode} operations={len(operations)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
