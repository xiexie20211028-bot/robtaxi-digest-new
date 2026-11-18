"""将健康闭环决策安全同步到 GitHub 元数据。

本模块不执行代码修改、分支、PR、合并或生产操作。所有 GitHub 命令均以
参数数组调用，Issue、网页和日志文本不会进入 shell。
"""

from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .common import read_json, write_json
from .health_loop import action_fingerprint
from .health_history import canonical_history, counts, observation_marker, pending_observations, parse_observations, validate_observation, required_recovery_runs
from .development_policy import DevelopmentError, digest
from .development_recovery import RequestFailure


SCHEMA_VERSION = "robtaxi-health-loop-sync-v1"
DECISION_SCHEMA = "robtaxi-health-loop-v1"
STATE_SCHEMA = "robtaxi-health-loop-official-state-v1"
REPOSITORY = "xiexie20211028-bot/robtaxi-digest-new"
PROJECT_OWNER = "xiexie20211028-bot"
PROJECT_NUMBER = 3
MAX_BATCHES = 3
MAX_DECISION_ACTIONS = 100
REOPEN_WINDOW_DAYS = 30
ALLOWED_ACTIONS = {"observe", "create_task", "verify", "close", "needs_approval", "no_code_change"}
KNOWN_ENGINEERING_ISSUES = {
    ("miit_news_structured", "low_date_parse_rate"): 49,
    ("pony_news_structured", "high_whitelist_reject_rate"): 52,
    ("singapore_lta_news_structured", "low_date_parse_rate"): 57,
    ("zoox_news_structured", "consecutive_empty_listing"): 58,
}
KEY_RE = re.compile(r"^[a-z0-9_.:-]{3,160}$")
SOURCE_RE = re.compile(r"^[a-z0-9_]{2,100}$")
MARKER_PREFIX = "<!-- robtaxi-health-loop-incident:"
SYNC_PREFIX = "<!-- robtaxi-health-loop-sync:"


class SyncError(RuntimeError):
    """输入、正式状态或 GitHub 操作不满足 fail-closed 条件。"""


class ItemWriteBlocked(SyncError):
    """单个已识别的正式任务需要人工核对。"""


class ItemWriteUnknown(ItemWriteBlocked):
    """目标操作已登记，但正式状态尚不能确认。"""


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
    if len(actions) > MAX_DECISION_ACTIONS:
        raise SyncError(f"异常动作超过安全输入上限 {MAX_DECISION_ACTIONS}，已停止同步")
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


def _build_all_sync_operations(decision: dict[str, Any], official_state: dict[str, Any]) -> list[dict[str, Any]]:
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
            or KNOWN_ENGINEERING_ISSUES.get((str(row.get("source_id", "")), str(row.get("reason_code", ""))), 0)
            or 0
        )
        if action in {"verify", "close"} and not issue_number:
            raise SyncError(f"{action} 缺少正式工程 Issue")
        if action == "close":
            previous = _dict(current.get("latest_sync"))
            required = required_recovery_runs(issue_number, minimum=int(current.get("required_recovery_runs", 2)))
            if int(row.get("recovery_count", 0)) < required:
                raise SyncError("未满足原任务要求的恢复次数")
            if previous.get("action") != "verify" or int(previous.get("recovery_count", 0) or 0) < required - 1:
                raise SyncError("GitHub 正式状态没有第一次恢复记录，禁止关闭")
            if str(previous.get("run_id", "")) == str(row.get("last_run_id", "")):
                raise SyncError("两次恢复不能来自同一个运行")
        if action in {"verify", "close"}:
            if not current.get("merged_commit_reachable"):
                raise SyncError("GitHub 正式状态无法证明生产已包含合并版本")
            if str(current.get("merged_commit", "")) != str(row.get("merged_commit", "")):
                raise SyncError("decision 的合并版本与 GitHub 正式状态不一致")
        plan = _dict(_dict(decision.get("_health_plans")).get(fingerprint))
        if plan and plan["issue_number"] and plan["issue_number"] != issue_number:
            raise SyncError("工程任务身份已变化，需重新核对")
        if plan and action == "create_task" and current.get("issue_state") == "CLOSED" and not plan["reopen"]:
            raise SyncError("原建单之后工程任务已关闭，需人工核对")
        if plan and action in {"close", "verify"}:
            target = "已完成" if action == "close" else "观察中"
            if current.get("project_status", "") not in {plan["project_status"], target}:
                raise SyncError("工程任务阶段已变化，需重新核对")
            if action == "verify" and current.get("issue_state", "") != plan["issue_state"]:
                raise SyncError("工程任务开关状态已变化，需重新核对")
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
                "reopen": plan["reopen"] if plan else bool(row.get("reopen") or current.get("issue_state") == "CLOSED"),
                "issue_state": current.get("issue_state", ""),
                "project_status": current.get("project_status", ""),
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


def build_sync_batches(
    decision: dict[str, Any], official_state: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """返回本次可执行批次与需要下次续做的批次。

    所有 changed_actions 会先完整校验；纯观察、待人工决策和无代码变化不产生
    GitHub 写操作，也不占用每日写入上限。实际写操作按确定性输入顺序每次最多
    执行 MAX_BATCHES 笔，其余操作保留在同步回执中，下一次从 GitHub 正式状态
    重建后会自动跳过已应用指纹并继续处理。
    """

    operations = _build_all_sync_operations(decision, official_state)
    return operations[:MAX_BATCHES], operations[MAX_BATCHES:]


def build_sync_plan(decision: dict[str, Any], official_state: dict[str, Any]) -> list[dict[str, Any]]:
    """兼容旧调用方：只返回本次允许执行的写入批次。"""

    operations, _deferred = build_sync_batches(decision, official_state)
    return operations


def isolated_sync_batches(decision: dict, official: dict, items: list[dict]) -> tuple[list[dict], list[dict]]:
    """结构/公共状态先整体校验，再隔离单项身份或验收条件；不吞掉传输失败。"""
    actions = validate_decision(decision)
    if official.get("schema_version") != STATE_SCHEMA or official.get("complete") is not True:
        raise SyncError("GitHub/Project公共状态不完整")
    operations = []
    for row in actions:
        item = next((i for i in items if i["incident_key"] == row["incident_key"]), None)
        if item and item["status"] == "blocked":
            continue
        try:
            operations.extend(_build_all_sync_operations({**decision, "changed_actions": [row]}, official))
        except SyncError as exc:
            if item is None:
                item = {"incident_key": row["incident_key"], "source_id": row.get("source_id", ""),
                        "production_run_id": decision["run_id"]}
                items.append(item)
            item.update(status="blocked", reason=str(exc))
    return operations[:MAX_BATCHES], operations[MAX_BATCHES:]


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
        self._project_updated_at = ""
        self.run_guard = None
        self.transport = None
        self._journal: dict[str, dict] | None = None

    def _run(self, args: list[str]) -> CommandResult:
        if self.run_guard:
            self.run_guard()
        if self.transport:
            # 仅固定查询命令可恢复；mutation、创建及评论写入绝不在传输层重试。
            readonly = (args[0] in {"repo", "issue", "project"} and len(args) > 1
                        and args[1] in {"view", "list", "field-list", "item-list"})
            if args[0] == "api":
                queries = [v[6:] for v in args if v.startswith("query=")]
                readonly = bool(queries and all(q.lstrip().startswith("query") for q in queries)) or (
                    "graphql" not in args and not any(v in args for v in ("--method", "-X", "--input", "-f", "-F")))
            try:
                execute = self.transport.gh_read if readonly else self.transport.gh
                return CommandResult(execute(*args))
            except DevelopmentError as exc:
                raise SyncError(f"正式元数据请求停止：{exc}") from exc
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

    def _records(self) -> dict[str, dict]:
        if self._journal is not None:
            return self._journal
        journal = {}
        for event in self.transport.events():
            if event.get("event") == "health_write_intent":
                operation = event.get("operation")
                if not isinstance(operation, dict) or event.get("key") != "health-write:" + digest(operation):
                    raise SyncError("正式健康写入意图损坏")
            elif event.get("event") == "health_operation_planned":
                parts = [event.get(key) for key in ("scope", "row", "binding")]
                if not all(isinstance(p, dict) for p in parts) or event.get("key") != "health-plan:" + digest(parts):
                    raise SyncError("正式工程操作计划损坏")
            else:
                continue
            journal[event["key"]] = event
        self._journal = journal
        return journal

    def _record(self, event: dict):
        if self.run_guard:
            self.run_guard()
        self.transport.append({**event, "run_id": self.transport.run_id, "at": datetime.now(timezone.utc).isoformat()})
        self._records()[event["key"]] = event

    def resume_decision(self, decision: dict, *, authorize: list[dict] | None = None) -> dict:
        """部分建单会改变重新生成的决策，仅接续同一生产证据下已授权的工程动作。"""
        if self.transport is None:
            return decision
        plans = self._records()
        evidence = decision.get("run_evidence", {})
        production = {"production": decision["run_id"], "attempt": evidence.get("attempt", 1),
                      "sha": evidence.get("commit_sha")}
        review = decision.get("business_review", {})

        def scope_for(row):
            if not row["incident_key"].startswith("review:"):
                return production
            binding = {k: review.get(k) for k in ("review_run_id", "attempt", "review_sha", "sha256", "status")}
            return {**production, "business_review": binding}

        current = {r["incident_key"]: r for r in decision.get("changed_actions", [])}
        expected = {}
        for plan in list(plans.values()):
            if plan.get("event") != "health_operation_planned":
                continue
            row, binding = plan["row"], plan["binding"]
            if plan["scope"] != scope_for(row):
                continue
            current[row["incident_key"]] = row
            expected[row["action_fingerprint"]] = binding
        for operation in authorize or []:
            fingerprint = operation["action_fingerprint"]
            if fingerprint in expected:
                continue
            row = current[operation["incident_key"]]
            # 正式计划仅保存重建所需字段；不把自由文本写到公共账本。
            fields = ("action", "incident_key", "source_id", "reason_code", "category", "severity",
                      "risk", "engineering_issue", "reopen", "recovery_count", "occurrence_count",
                      "last_run_id", "merged_commit", "merged_commit_reachable", "source_run_evidence",
                      "action_fingerprint")
            compact = {field: row[field] for field in fields if field in row}
            binding = {"issue_number": operation["issue_number"], "reopen": operation["reopen"],
                       "project_status": operation["project_status"], "issue_state": operation["issue_state"]}
            scope = scope_for(row)
            key = "health-plan:" + digest([scope, compact, binding])
            self._record({"event": "health_operation_planned", "key": key, "scope": scope,
                          "row": compact, "binding": binding})
            expected[fingerprint] = binding
        return {**decision, "changed_actions": list(current.values()), "_health_plans": expected}

    def _confirmed_write(self, identity: dict, write: Callable, verify: Callable):
        """正式意图先于副作用；中断或响应丢失后只核对同一操作。"""
        if self.transport is None:
            return write()
        if self.run_guard:
            self.run_guard()
        key = "health-write:" + digest(identity)
        if key not in self._records():
            self._record({"event": "health_write_intent", "key": key, "operation": identity})
            try:
                return write()
            except SyncError as exc:
                failure = exc.__cause__
                if not isinstance(failure, RequestFailure) or failure.category not in {"connection", "timeout", "service", "unknown"}:
                    raise
        try:
            confirmed = verify()
        except SyncError as exc:
            failure = exc.__cause__
            if not isinstance(failure, RequestFailure) or failure.category not in {"connection", "timeout", "service"}:
                raise
            confirmed = None
        if confirmed:
            return confirmed
        raise ItemWriteUnknown(f"写入结果待核对，已禁止重发：{key}")

    def _write_comment(self, number: int, body: str, identity: dict) -> None:
        def confirm():
            return any(c.get("user", {}).get("login") == self.owner and c.get("body") == body
                       for c in self._comments(number))
        self._confirmed_write({**identity, "issue": number},
                              lambda: self._run(["issue", "comment", str(number), "--repo", self.repository, "--body", body]),
                              confirm)

    def _json(self, args: list[str]) -> Any:
        try:
            return json.loads(self._run(args).stdout)
        except json.JSONDecodeError as exc:
            raise SyncError("GitHub 返回格式不完整，已停止同步") from exc

    def preflight(self, *, run_evidence: dict[str, Any] | None = None) -> dict[str, Any]:
        owner, name = self.repository.split("/", 1)
        query = """query($owner:String!, $name:String!, $login:String!, $number:Int!) {
          repository(owner:$owner, name:$name) { nameWithOwner }
          user(login:$login) { projectV2(number:$number) { id updatedAt } }
        }"""
        payload = self._json(["api", "graphql", "-f", f"query={query}", "-F", f"owner={owner}",
                              "-F", f"name={name}", "-F", f"login={self.owner}", "-F", f"number={self.project}"])
        data = _dict(_dict(payload).get("data"))
        project = _dict(_dict(data.get("user")).get("projectV2"))
        if _dict(payload).get("errors") or _dict(data.get("repository")).get("nameWithOwner") != self.repository:
            raise SyncError("GitHub 仓库身份或 Project 查询不完整")
        project_id = project.get("id")
        updated_at = project.get("updatedAt")
        if not project_id or not updated_at or (self._project_id and project_id != self._project_id):
            raise SyncError("目标 Project 身份或更新时间无法确认")
        if not self._fields or updated_at != self._project_updated_at:
            self._refresh_fields()
        self._project_id, self._project_updated_at = project_id, updated_at
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
                "number,title,state,body,url,assignees,author,closedAt,labels",
            ]
        )
        project_payload = self._json(
            ["project", "item-list", str(self.project), "--owner", self.owner, "--limit", "1000", "--format", "json"]
        )
        if not isinstance(issues, list) or not isinstance(_dict(project_payload).get("items"), list):
            raise SyncError("GitHub Issue 或 Project 事项列表不完整")
        self._issues = _list(issues)
        self._items = _list(_dict(project_payload).get("items"))
        if len(self._issues) >= 1000 or len(self._items) >= 1000:
            raise SyncError("GitHub 列表达到分页上限，无法证明正式状态完整")
        return self.official_state(run_evidence=run_evidence)

    def _refresh_fields(self) -> None:
        payload = self._json(["project", "field-list", str(self.project), "--owner", self.owner,
                              "--limit", "100", "--format", "json"])
        rows = _dict(payload).get("fields")
        if not isinstance(rows, list) or _dict(payload).get("totalCount") != len(rows):
            raise SyncError("Project 字段列表不完整")
        fields = {row["name"]: row for row in rows if isinstance(row, dict) and row.get("name") and row.get("id")}
        required = {"Status", "Priority", "Task Type", "Change Risk", "Target", "Route"}
        if len(fields) != len(rows) or not required.issubset(fields):
            raise SyncError("Project 必填字段无法完整识别")
        self._fields = fields

    def validate_write_access(self) -> None:
        """在任何 apply 写入前确认仓库和个人 Project 都可更新。"""
        owner, name = self.repository.split("/", 1)
        query = """
query($owner:String!,$name:String!,$login:String!,$number:Int!) {
  repository(owner:$owner,name:$name) { viewerPermission }
  user(login:$login) { projectV2(number:$number) { id viewerCanUpdate } }
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
        if payload.get("errors") or (self._project_id and project.get("id") != self._project_id):
            raise SyncError("写入权限或目标 Project 身份核验不完整")
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

    def _project_item(self, number: int, rows: list[dict] | None = None) -> dict[str, Any]:
        url = f"https://github.com/{self.repository}/issues/{number}"
        matches = [item for item in (self._items if rows is None else rows)
                   if _dict(item.get("content")).get("url") == url]
        if len(matches) > 1:
            raise SyncError("总盘事项身份不唯一")
        return matches[0] if matches else {}

    def _comments(self, number: int) -> list[dict[str, Any]]:
        payload = self._json(
            [
                "api",
                "--paginate",
                "--slurp",
                f"repos/{self.repository}/issues/{number}/comments?per_page=100",
            ]
        )
        if not isinstance(payload, list) or not all(isinstance(page, list) for page in payload):
            raise SyncError("正式评论分页不完整，停止状态重建")
        return [row for page in payload for row in page if isinstance(row, dict)]

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
        candidates: dict[int, str] = {number: f"source:{source}:{reason}" for (source, reason), number in KNOWN_ENGINEERING_ISSUES.items()}
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
                "category": "source" if key.startswith("source:") else "delivery",
                "source_id": key.split(":", 2)[1] if key.startswith("source:") else "",
                "reason_code": key.split(":", 2)[2] if key.startswith("source:") and key.count(":") >= 2 else "",
                "engineering_issue": number,
                "required_recovery_runs": required_recovery_runs(number, str(issue.get("body", ""))),
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
        history_issues = [i for i in self._issues if "<!-- robtaxi-health-incident:" in str(i.get("body", ""))
                          or int(i.get("number", 0)) == 100]
        histories: dict[str, list[dict]] = {}
        for issue in history_issues:
            for row in parse_observations(self._comments(int(issue["number"])), self.owner):
                histories.setdefault(row["incident_key"], []).append(row)
        latest_observed_run, latest_attempt = max(
            ((int(r["run_id"]), r["attempt"]) for rows in histories.values() for r in rows), default=(0, 0))
        for key, rows in histories.items():
            history = canonical_history(rows, as_of_run=str(current_run.get("run_id", "")))
            if not history:
                continue
            latest = history[-1]
            record = incidents.setdefault(key, {"incident_key": key, "engineering_issue": None, "lifecycle": "open", "applied_fingerprints": []})
            record.update({**counts(history), "observations": history,
                           "source_id": latest.get("source_id", ""), "reason_code": latest.get("reason_code", ""),
                           "last_run_id": latest["run_id"], "severity": latest["severity"],
                           "risk": record.get("risk", "Medium" if latest.get("source_id") else "High")})
            if record.get("merged_commit"):
                record["recovery_count"] = counts([r for r in history if r.get("recovery_merge_sha") == record["merged_commit"] or r["result"] == "failed"])["recovery_count"]
        return {
            "schema_version": STATE_SCHEMA,
            "complete": True,
            "repository": self.repository,
            "project_number": self.project,
            "incidents": incidents,
            "latest_observed_run": latest_observed_run,
            "latest_observed_attempt": latest_attempt,
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

    def prepare_observations(self, decision: dict, *, apply: bool = False) -> list[dict]:
        return self.persist_observations(decision, isolate=True, apply=apply)

    def persist_observations(self, decision: dict[str, Any], *, isolate: bool = False, apply: bool = True) -> list[dict]:
        """观察不占工程状态批次；每个Health Issue每run一条幂等机器记录。"""
        from .health_issue import _stable_incident_id, resolve_issue_for_incident
        groups: dict[int, list[dict]] = {}
        related: dict[int, set[int]] = {}
        self.item_results = []
        for row in _list(decision.get("observations")):
            # 非法输入属于整份报告不可信，不能伪装成一项普通阻塞。
            validate_observation(row)
            if row["run_id"] != str(decision.get("run_id", "")):
                raise SyncError("观察记录和当前生产运行不一致")
            target = 100
            item = {"incident_key": row["incident_key"], "source_id": row.get("source_id", ""),
                    "production_run_id": row["run_id"], "evidence_digest": digest(row),
                    "status": "planned", "reason": ""}
            self.item_results.append(item)
            if row.get("source_id"):
                try:
                    resolution = resolve_issue_for_incident(self._issues,
                        _stable_incident_id("", row["source_id"], row["reason_code"]),
                        source_id=row["source_id"], reason_code=row["reason_code"])
                except ValueError as exc:
                    if not isolate:
                        raise SyncError(str(exc)) from exc
                    item.update(status="blocked", reason=str(exc)); continue
                if resolution["issue"] is None:
                    if not isolate:
                        raise SyncError("来源Health证据Issue缺失或重复，停止观察写入")
                    item.update(status="blocked", reason="来源Health证据记录待核对"); continue
                target = int(resolution["issue"]["number"])
                related.setdefault(target, set()).update(resolution["related_issues"])
            item["issue_number"] = target
            groups.setdefault(target, []).append(row)
        planned = []
        for number, rows in groups.items():
            comments = self._comments(number)  # 读取/认证失败为公共故障，直接停止整轮。
            try:
                existing = parse_observations(comments, self.owner)
                pending = pending_observations(existing, rows)
            except ValueError as exc:
                if not isolate or number == 100:
                    raise SyncError(f"正式观察历史损坏：{exc}") from exc
                for item in self.item_results:
                    if item.get("issue_number") == number:
                        item.update(status="blocked", reason=f"该事故观察历史冲突：{exc}")
                continue
            if pending:
                proof = (f"事故身份核验：主记录 #{number}；同来源同原因的历史记录 "
                         + ", ".join(f"#{n}" for n in sorted(related.get(number, []))) + "。旧记录和历史证据保留。\n") if related.get(number) else ""
                planned.append((number, proof + observation_marker(pending), pending))
        result = []
        for number, body, pending in planned:
            if apply:
                try:
                    self._write_comment(number, body, {"kind": "observation", "run": decision["run_id"],
                                                        "evidence": digest(pending)})
                except ItemWriteUnknown as exc:
                    if not isolate:
                        raise
                    keys = {row["incident_key"] for row in pending}
                    for item in self.item_results:
                        if item["incident_key"] in keys:
                            item.update(status="blocked", write_status="unknown", reason=str(exc))
                    continue
            result.append({"issue_number": number, "status": "recorded" if apply else "planned"})
        for item in self.item_results:
            if item["status"] != "blocked":
                item["status"] = "synced" if apply else "planned"
        return result

    def _field_option(self, field_name: str, option_name: str) -> tuple[str, str]:
        field = _dict(self._fields.get(field_name))
        for option in _list(field.get("options")):
            if option.get("name") == option_name:
                return str(field.get("id", "")), str(option.get("id", ""))
        raise SyncError(f"Project 字段 {field_name} 缺少选项 {option_name}")

    def _set_field(self, item_id: str, field_name: str, option_name: str | int) -> None:
        if type(option_name) is int:
            field_id = str(_dict(self._fields.get(field_name)).get("id", ""))
            if not field_id:
                raise SyncError(f"Project缺少数值字段{field_name}")
            value_args = ["--number", str(option_name)]
        else:
            field_id, option_id = self._field_option(field_name, option_name)
            value_args = ["--single-select-option-id", option_id]
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
                *value_args,
            ]
        )

    def _task_fields(self, operation: dict) -> dict:
        values = {"Status": "待办", "Priority": "P0" if operation.get("risk") == "High" else "P1",
                  "Task Type": "Bug", "Change Risk": str(operation.get("risk", "Medium")),
                  "Target": "本周", "Route": "共同"}
        from .development_budget import budgeted
        if budgeted(getattr(self.transport, "policy", {})):
            source = bool(operation.get("source_id"))
            values.update({"Area": "信源" if source else "自检", "Effort": "S" if source else "M", "Remedy": "根治",
                           "Impact": 3 if source else 4, "Urgency": 5, "Reach": 2 if source else 5,
                           "Recurrence": 5, "Priority Score": 26 if source else 32})
        return values

    def _ensure_project_item(self, number: int, url: str) -> str:
        item = self._project_item(number)
        if item:
            return str(item.get("id", ""))
        try:
            created = self._json(
                ["project", "item-add", str(self.project), "--owner", self.owner, "--url", url, "--format", "json"])
        except SyncError as exc:
            if not isinstance(exc.__cause__, json.JSONDecodeError):
                raise
            created = {}
        item_id = _dict(created).get("id") or self._live_item(number).get("id")
        if not item_id:
            raise ItemWriteUnknown("加入总盘的响应缺少事项身份，需核对")
        return str(item_id)

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
        from .development_budget import budgeted
        if budgeted(getattr(self.transport, "policy", {})):
            lines += ["", "## 自动初评与依赖", "字段按本次健康证据初评，开发前仍须核对实际范围、依赖、原验收并通过preflight。",
                      "数值初评：" + json.dumps({k: v for k, v in self._task_fields(operation).items() if type(v) is int}, ensure_ascii=False),
                      "未从事故证据发现代码依赖；如诊断发现依赖，先登记，不能自行假定完成。"]
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
        if operation.get("create_marker"):
            lines.append(operation["create_marker"])
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
            confirmed = self._find_created(operation["create_marker"])
            if confirmed:
                return confirmed
            raise ItemWriteUnknown("建单响应缺少编号，正式 Issue 仍待核对")
        return {"number": int(match.group(1)), "url": output, "state": "OPEN", "assignees": [{"login": self.owner}]}

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

    def _live_item(self, number: int) -> dict:
        result = self._json(["project", "item-list", str(self.project), "--owner", self.owner,
                             "--limit", "1000", "--format", "json"])
        rows = result.get("items")
        if not isinstance(rows, list) or len(rows) >= 1000:
            raise SyncError("无法完整确认总盘成员")
        return self._project_item(number, rows)

    def _find_created(self, marker: str) -> dict | None:
        rows = self._json(["issue", "list", "--repo", self.repository, "--state", "all", "--limit", "1000",
                           "--json", "number,state,body,url,author,assignees"])
        if not isinstance(rows, list) or len(rows) >= 1000:
            raise SyncError("工程 Issue 列表不完整")
        matches = [r for r in rows if marker in r.get("body", "").splitlines()
                   and r.get("author", {}).get("login") == self.owner]
        if len(matches) > 1:
            raise ItemWriteBlocked("同一建单意图匹配多个工程 Issue，需核对")
        return matches[0] if matches else None

    def _apply_task(self, operation: dict) -> int:
        key = operation["incident_key"]
        number = int(operation.get("issue_number") or 0)
        issue = self._issue_by_number(number) if number else self._issue_for_key(key)
        action = operation["operation"]
        active_matches = [i["number"] for i in self._issues if i.get("state") == "OPEN"
                          and _incident_marker(key) in i.get("body", "").splitlines()
                          and i.get("author", {}).get("login") == self.owner]
        if len(set(active_matches + ([number] if number else []))) > 1:
            raise ItemWriteBlocked("正式事故匹配多张工程任务，需核对后再写入")
        if not issue:
            if action != "ensure_task":
                raise SyncError("正式工程 Issue 缺失，已停止同步")
            creation = {"kind": "create", "incident": key,
                        "predecessor": max((int(i["number"]) for i in self._issues
                                            if _incident_marker(key) in i.get("body", "").splitlines()
                                            and i.get("author", {}).get("login") == self.owner), default=0)}
            marker = f"<!-- robtaxi-health-create:{digest(creation)} -->"
            issue = self._confirmed_write(creation, lambda: self._create_issue({**operation, "create_marker": marker}),
                                          lambda: self._find_created(marker))
            self._issues.append(issue)
        number = int(issue["number"])
        url = f"https://github.com/{self.repository}/issues/{number}"
        prefix = {"incident": key, "issue": number, "action": operation["action_fingerprint"]}

        def write(kind, execute, confirm):
            if self.transport is not None:
                previous = {op["action"] for row in self._records().values()
                            if (op := row.get("operation", {})).get("kind") == kind
                            and op.get("issue") == number and op.get("action") != prefix["action"]
                            and op.get("action")}
                if previous:
                    _, finished = self._latest_sync(self._comments(number))
                    if previous.difference(finished):
                        raise ItemWriteUnknown(f"工程 Issue #{number} 的旧 {kind} 写入尚未取得工程回执")
            return self._confirmed_write({**prefix, "kind": kind}, execute, confirm)

        def live_issue():
            return self._json(["issue", "view", str(number), "--repo", self.repository, "--json", "state,assignees"])

        state = str(issue.get("state", "")).upper()
        if action == "ensure_task" and state == "CLOSED":
            if not operation.get("reopen"):
                raise ItemWriteBlocked("工程任务已关闭，需重新评估是否重开")
            write("reopen", lambda: self._run(["issue", "reopen", str(number), "--repo", self.repository]),
                  lambda: live_issue().get("state") == "OPEN")
        current = self._project_item(number)
        item_id = current.get("id") or write("add_to_project", lambda: self._ensure_project_item(number, url),
                                             lambda: self._live_item(number).get("id"))
        if not item_id:
            raise ItemWriteUnknown("未能确认工程 Issue 对应的总盘事项")
        if not current and self.transport is not None:
            current = self._live_item(number)
            if current.get("id") != item_id:
                raise ItemWriteUnknown("无法核对加入总盘后的事项身份")
        values = self._task_fields(operation) if action == "ensure_task" else {
            "Status": "观察中" if action == "verify" else "已完成"}
        for field, value in values.items():
            slug = field[:1].lower() + field[1:]
            old = current.get(slug)
            if field == "Change Risk" and old and old != value:
                levels = ["Low", "Medium", "High"]
                if old not in levels or levels.index(old) < levels.index(value):
                    raise ItemWriteBlocked("总盘风险低于本次工程动作要求，需核对")
            # 仅在原动作明确允许重新打开时，将观察中或已完成退回待办。
            reset_after_regression = (field == "Status" and operation.get("reopen") and
                                      old in {"已完成", "观察中"})
            if old == value or (action == "ensure_task" and old is not None and not reset_after_regression):
                continue
            write("field:" + field, lambda: self._set_field(item_id, field, value),
                  lambda: self._live_item(number).get(slug) == value)
        if action == "ensure_task" and not issue.get("assignees"):
            write("assignee", lambda: self._run(["issue", "edit", str(number), "--repo", self.repository,
                                                "--add-assignee", self.owner]),
                  lambda: bool(live_issue().get("assignees")))
        if action == "close" and state != "CLOSED":
            write("close", lambda: self._run(["issue", "close", str(number), "--repo", self.repository,
                                              "--reason", "completed"]),
                  lambda: live_issue().get("state") == "CLOSED")
        self._write_comment(number, self._sync_comment(operation), {**prefix, "kind": "sync_receipt"})
        return number

    def apply(self, operations: list[dict[str, Any]]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        if operations and self._project_id:
            # 真实写入 Project 前重新解析选项 ID，不能仅凭更新时间授权写入。
            self._refresh_fields()
        # 所有只读检查在第一笔写入前完成。
        for operation in operations:
            number = int(operation.get("issue_number") or 0)
            if number and not self._issue_by_number(number):
                raise SyncError(f"工程 Issue #{number} 不存在，已停止同步")
            if operation["operation"] == "ensure_task":
                for field, option in self._task_fields(operation).items():
                    if type(option) is int:
                        if not _dict(self._fields.get(field)).get("id"):
                            raise SyncError(f"Project缺少数值字段{field}")
                    else:
                        self._field_option(field, option)
            elif operation["operation"] == "verify":
                self._field_option("Status", "观察中")
            elif operation["operation"] == "close":
                self._field_option("Status", "已完成")
        for operation in operations:
            try:
                number = self._apply_task(operation)
            except ItemWriteBlocked as exc:
                if not hasattr(self, "item_results"):
                    raise
                item = next((i for i in self.item_results if i["incident_key"] == operation["incident_key"]), None)
                if item is None:
                    item = {"incident_key": operation["incident_key"], "production_run_id": operation["run_id"],
                            "source_id": operation["source_id"]}
                    self.item_results.append(item)
                item.update(status="blocked", write_status="unknown" if isinstance(exc, ItemWriteUnknown) else "conflict",
                            reason=str(exc))
                continue
            results.append({"operation": operation["operation"], "issue_number": number, "status": "applied"})
        return results


@contextmanager
def sync_run_scope(mode: str, run_id: str, outputs: list[str]):
    """apply 全程占有同一本机锁；每个请求前检查正式租约的剩余预算。"""
    from .development_cycle import ROOT, initialize_run, load_policy
    from .development_lease import isolated_output
    from .development_runtime import GitHub, repository_lock
    if mode != "apply" and not run_id:
        yield None
        return
    policy = load_policy()
    for path in outputs:
        if path:
            isolated_output(ROOT, run_id, path, "health-sync.json")
    with repository_lock(policy["repository"]):
        client = GitHub(policy)
        initialize_run(client, policy, run_id)
        yield client


def main() -> int:
    parser = argparse.ArgumentParser(description="安全同步 Codex 健康闭环元数据")
    parser.add_argument("--decision", required=True)
    parser.add_argument("--mode", choices=("shadow", "apply"), required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--state-out", default="")
    parser.add_argument("--run-id", default="", help="正式执行必须显式绑定整轮ID；独立shadow默认只读")
    args = parser.parse_args()
    from .development_policy import DevelopmentError
    guard = None
    try:
        with sync_run_scope(args.mode, args.run_id, [args.out, args.state_out]) as guard:
            return sync_main(args, guard)
    except (SyncError, DevelopmentError, OSError, ValueError, KeyError) as exc:
        print(f"[health-loop-sync] FAIL: {exc}", file=sys.stderr)
        if args.mode == "apply" and guard is not None and getattr(guard, "budget", None) is not None:
            from .development_outcomes import finalize_run
            from .development_runtime import repository_lock
            with repository_lock(guard.policy["repository"]):
                finalize_run(guard, "failed", decision_path=args.decision, health_sync_path=args.out,
                             error=str(exc)[:300])
        return 1


def sync_main(args, guard=None, *, metadata=None) -> int:
    decision = read_json(Path(args.decision))
    client = metadata if metadata is not None else GhMetadataClient()
    budget = getattr(guard, "budget", None)
    client.run_guard = budget.check if budget else guard if callable(guard) else None
    client.transport = guard if hasattr(guard, "gh_read") else None
    official = client.preflight(run_evidence=_dict(decision.get("run_evidence")))
    # 缓存丢失时也先落下只读重建结果；调用方据此重新计算后才能 apply。
    if args.state_out:
        write_json(Path(args.state_out), official)
    validate_decision(decision)
    native_isolation = callable(getattr(client, "prepare_observations", None))
    if native_isolation:
        client.prepare_observations(decision, apply=False)
        plan_decision = client.resume_decision(decision) if hasattr(client, "resume_decision") else decision
        operations, deferred_operations = isolated_sync_batches(plan_decision, official, client.item_results)
    else:
        operations, deferred_operations = build_sync_batches(decision, official)
    if args.mode == "apply" and (operations or decision.get("observations")):
        client.validate_write_access()
    latest = (int(official.get("latest_observed_run", 0)), int(official.get("latest_observed_attempt", 0)))
    history_only = latest > (int(decision["run_id"]), int(decision["run_evidence"].get("attempt", 1)))
    if args.mode == "apply" and (decision.get("run_evidence", {}).get("health_report_valid") is False
            or not decision.get("run_evidence", {}).get("health_report_available")):
        raise SyncError("整份健康报告不可信，停止整轮")
    if native_isolation and args.mode == "apply":
        observed = client.prepare_observations(decision, apply=True)
        operations, deferred_operations = isolated_sync_batches(plan_decision, official, client.item_results)
        if operations and not history_only and hasattr(client, "resume_decision"):
            client.resume_decision(decision, authorize=operations)
    else:
        observed = client.persist_observations(decision) if args.mode == "apply" else []
    if history_only:
        # 旧报告只补逐次证据；绝不执行旧任务动作、回退摘要或生成新候选。
        if native_isolation and client.transport and hasattr(client, "_records"):
            for op in operations + deferred_operations:
                if any(p.get("event") == "health_operation_planned" and
                       p.get("row", {}).get("action_fingerprint") == op["action_fingerprint"]
                       for p in client._records().values()):
                    item = next((i for i in client.item_results if i["incident_key"] == op["incident_key"]), None)
                    if item is None:
                        item = {"incident_key": op["incident_key"], "source_id": op["source_id"],
                                "production_run_id": decision["run_id"]}
                        client.item_results.append(item)
                    item.update(status="blocked", reason="较新生产状态已到达，未完成的旧工程操作需重新核对")
        operations, deferred_operations = [], []
    applied = client.apply(operations) if args.mode == "apply" else []
    if args.mode == "apply" and (operations or observed):
        official = client.preflight(run_evidence=_dict(decision.get("run_evidence")))
    items = getattr(client, "item_results", [])
    for operation in deferred_operations:
        for item in items:
            if item["incident_key"] == operation["incident_key"]:
                item.update(status="blocked", reason="工程元数据批次待续做，已保存观察")
    blocked = [i for i in items if i["status"] == "blocked"]
    source_complete = not any(i.get("source_id") for i in blocked) and not any(
        i.get("source_id") for i in deferred_operations)
    review_status = decision.get("business_review", {})
    result = {
        "schema_version": SCHEMA_VERSION,
        "mode": args.mode,
        "complete": not deferred_operations and not blocked and not history_only
                    and review_status.get("status") not in {"pending", "unmatched", "invalid"},
        "source_complete": source_complete,
        "business_review": review_status,
        "global_state_valid": bool(decision.get("run_evidence", {}).get("health_report_available"))
                              and decision.get("run_evidence", {}).get("health_report_valid") is not False,
        "historical_only": history_only,
        "item_results": items,
        "blocked_incidents": blocked,
        "operations": operations,
        "deferred_operations": deferred_operations,
        "applied": applied,
        "observations_recorded": observed,
        "official_state": official,
        "development_run_id": args.run_id if args.mode == "apply" else None,
        "decision_digest": digest(decision),
    }
    if args.mode == "apply" and client.transport and not history_only and decision.get("run_evidence", {}).get("event_name") == "schedule":
        entries = []
        actions = {r["incident_key"]: r for r in decision.get("actions", [])}
        source_rows = {r["incident_key"]: r for r in decision.get("observations", [])}
        source_rows.update(actions)
        for key, observation in source_rows.items():
            if not observation.get("source_id"):
                continue
            row = actions.get(key, observation)
            item = next((i for i in items if i["incident_key"] == key), {})
            known = official.get("incidents", {}).get(key, {})
            issue = known.get("engineering_issue") or KNOWN_ENGINEERING_ISSUES.get((row["source_id"], row["reason_code"]))
            saved = item.get("status") == "synced"
            actionable = saved and row.get("action") in {"create_task", "observe"} and row.get("lifecycle") == "open" and bool(issue)
            entries.append({"issue": issue, "incident_key": row["incident_key"], "source_id": row["source_id"],
                            "severity": row["severity"], "actionable": actionable, "observation_saved": saved,
                            "blocked_reason": None if actionable else item.get("reason") or "外部故障、证据不足、等待验收或尚未有正式工程任务",
                            "evidence_digest": digest({k: row.get(k) for k in ("source_id", "reason_code", "severity", "summary")})})
        proof = {"decision": decision, "entries": entries, "complete": result["complete"], "items": items,
                 "run_id": args.run_id}
        event = {"event": "health_queue_updated", "schema_version": "robtaxi-health-queue-v2", "key": f"health-queue:{digest(proof)}",
                 "run_id": args.run_id,
                 "production_run_id": decision["run_id"], "attempt": decision.get("run_evidence", {}).get("attempt", 1),
                 "complete": result["complete"], "global_state_valid": True, "decision_digest": digest(decision),
                 "production_date": decision.get("date_bj"), "business_review": decision.get("business_review", {}),
                 "entries": entries, "item_results": items, "blocked_incidents": blocked,
                 "at": datetime.now(timezone.utc).isoformat()}
        if not any(e.get("key") == event["key"] for e in client.transport.events()):
            client.transport.append(event)
        from .development_budget import budgeted
        if (budgeted(client.transport.policy)
                and client.transport.policy.get("automatic_source_verifiers") is True):
            records = client.transport.events()
            for row in entries:
                if not row["actionable"]:
                    continue
                known = official.get("incidents", {}).get(row["incident_key"], {})
                binding = {"issue": row["issue"], "source_id": row["source_id"], "incident_key": row["incident_key"],
                           "name": "source_health_v1", "kind": "source",
                           "required_runs": max(3 if row["issue"] in {49, 52, 57, 58} else 2, int(known.get("required_recovery_runs", 2)))}
                key = f"source-verifier:{digest(binding)}"
                if not any(e.get("key") == key for e in records):
                    client.transport.append({"event": "source_verifier_bound", "key": key, "binding": binding,
                                             "health_queue_key": event["key"], "production_run_id": decision["run_id"],
                                             "at": datetime.now(timezone.utc).isoformat()})
    write_json(Path(args.out), result)
    if args.state_out:
        write_json(Path(args.state_out), official)
    label = "PASS" if result["complete"] else "PARTIAL"
    print(f"[health-loop-sync] {label}: mode={args.mode} operations={len(operations)} "
          f"deferred={len(deferred_operations)} blocked={len(blocked)} complete={result['complete']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
