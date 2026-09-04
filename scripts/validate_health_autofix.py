#!/usr/bin/env python3
"""从可信 main 校验 WorkBuddy 单信源自动修复 PR 的语义差异。"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG = ROOT / ".github" / "robtaxi-health-autofix.json"
BRANCH_RE = re.compile(r"^workbuddy/health-[a-z0-9][a-z0-9-]{2,100}$")
SOURCE_RE = re.compile(r"^[a-z0-9_]{2,100}$")
INCIDENT_RE = re.compile(r"^source:([a-z0-9_]{2,100}):([a-z0-9_.-]{2,100})$")


class AutofixPolicyError(RuntimeError):
    """自动修复 PR 超出机器白名单。"""


def _git(*args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args], text=True, capture_output=True, check=False, timeout=20
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise AutofixPolicyError("无法读取 Git 差异") from exc
    if result.returncode != 0:
        raise AutofixPolicyError("Git 差异不完整")
    return result.stdout


def _load_json_text(text: str, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as exc:
        raise AutofixPolicyError(f"{name} 不是有效 JSON") from exc
    if not isinstance(value, dict):
        raise AutofixPolicyError(f"{name} 必须是 JSON object")
    return value


def load_config(path: Path) -> dict[str, Any]:
    try:
        config = _load_json_text(path.read_text(encoding="utf-8"), name="自动修复白名单")
    except OSError as exc:
        raise AutofixPolicyError("自动修复白名单不存在，功能保持禁用") from exc
    if (
        config.get("schema_version") != "robtaxi-health-autofix-v1"
        or config.get("enabled") is not True
        or config.get("shadow_mode") is not False
    ):
        raise AutofixPolicyError("自动修复白名单未启用")
    if not isinstance(config.get("sources"), dict):
        raise AutofixPolicyError("自动修复白名单缺少来源策略")
    if int(config.get("max_auto_merges_per_day", 0) or 0) != 1:
        raise AutofixPolicyError("自动修复白名单必须限制每天最多合并 1 个 PR")
    return config


def _required_line(body: str, label: str) -> str:
    match = re.search(rf"(?im)^\s*{re.escape(label)}\s*:\s*(\S.*?)\s*$", body or "")
    if not match:
        raise AutofixPolicyError(f"PR 正文缺少 {label}")
    return match.group(1).strip()


def parse_event(path: Path) -> dict[str, Any]:
    try:
        event = _load_json_text(path.read_text(encoding="utf-8"), name="PR event")
        pr = event["pull_request"]
        body = str(pr.get("body") or "")
        head = pr["head"]
        base = pr["base"]
    except (OSError, KeyError, TypeError) as exc:
        raise AutofixPolicyError("PR event 不完整") from exc
    branch = str(head.get("ref", ""))
    if not BRANCH_RE.fullmatch(branch):
        raise AutofixPolicyError("WorkBuddy 自动修复分支名不合法")
    if _dict(head.get("repo")).get("full_name") != _dict(base.get("repo")).get("full_name"):
        raise AutofixPolicyError("WorkBuddy 自动修复不接受 fork PR")
    labels = {str(row.get("name", "")) for row in pr.get("labels", []) if isinstance(row, dict)}
    if "workbuddy-autofix" not in labels:
        raise AutofixPolicyError("PR 缺少 workbuddy-autofix 标记")
    incident = _required_line(body, "Autofix incident")
    match = INCIDENT_RE.fullmatch(incident)
    if not match:
        raise AutofixPolicyError("Autofix incident 只允许单来源事件")
    source_id = _required_line(body, "Autofix source")
    if not SOURCE_RE.fullmatch(source_id) or source_id != match.group(1):
        raise AutofixPolicyError("Autofix source 与事件不一致")
    _required_line(body, "Autofix evidence")
    _required_line(body, "Autofix rollback")
    if not re.search(r"(?im)^\s*Primary task:\s*(Fixes|Closes|Refs)\s+#\d+\s*$", body):
        raise AutofixPolicyError("PR 缺少正式 Primary task")
    return {"branch": branch, "source_id": source_id, "reason_code": match.group(2)}


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _sources(config: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows = config.get("sources")
    if not isinstance(rows, list):
        raise AutofixPolicyError("sources.json 缺少 sources 列表")
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict) or not row.get("id"):
            raise AutofixPolicyError("sources.json 包含无效来源")
        source_id = str(row["id"])
        if source_id in result:
            raise AutofixPolicyError("sources.json 包含重复 source_id")
        result[source_id] = row
    return result


def validate_sources_semantic_diff(
    base: dict[str, Any], head: dict[str, Any], *, source_id: str, allowed_fields: set[str]
) -> set[str]:
    base_sources = _sources(base)
    head_sources = _sources(head)
    if set(base_sources) != set(head_sources):
        raise AutofixPolicyError("自动修复不能增加或删除来源")
    base_without = {key: value for key, value in base.items() if key != "sources"}
    head_without = {key: value for key, value in head.items() if key != "sources"}
    if base_without != head_without:
        raise AutofixPolicyError("自动修复不能修改 sources.json 顶层共享配置")
    changed_sources = {key for key in base_sources if base_sources[key] != head_sources[key]}
    if changed_sources != {source_id}:
        raise AutofixPolicyError("sources.json 必须且只能修改目标 source_id")
    before = base_sources[source_id]
    after = head_sources[source_id]
    changed_fields = {key for key in set(before) | set(after) if before.get(key) != after.get(key)}
    if not changed_fields or not changed_fields.issubset(allowed_fields):
        raise AutofixPolicyError("目标来源修改字段超出白名单")
    return changed_fields


def validate_patch(
    *,
    config: dict[str, Any],
    event: dict[str, Any],
    base_sha: str,
    head_sha: str,
    changed_files: list[str],
    base_sources: dict[str, Any] | None,
    head_sources: dict[str, Any] | None,
    changed_lines: int,
) -> dict[str, Any]:
    source_id = str(event["source_id"])
    reason_code = str(event["reason_code"])
    sources_config = _dict(config.get("sources"))
    source_policy = _dict(sources_config.get(source_id))
    if not source_policy:
        raise AutofixPolicyError("目标来源未列入白名单")
    if reason_code not in set(source_policy.get("reason_codes", [])):
        raise AutofixPolicyError("reason_code 未列入目标来源白名单")
    if len(changed_files) > int(config.get("max_changed_files", 4)):
        raise AutofixPolicyError("修改文件数量超过白名单")
    if changed_lines > int(config.get("max_changed_lines", 200)):
        raise AutofixPolicyError("修改行数超过白名单")
    allowed_files = {
        "sources.json",
        str(source_policy.get("fixture_path", "")),
        *[str(path) for path in source_policy.get("test_files", [])],
    }
    allowed_files.discard("")
    forbidden = {
        ".github/robtaxi-health-autofix.json",
        "scripts/validate_health_autofix.py",
        ".github/workflows/robtaxi-project-governance.yml",
    }
    if forbidden.intersection(changed_files) or not set(changed_files).issubset(allowed_files):
        raise AutofixPolicyError("PR 包含白名单外或自修改文件")
    if "sources.json" not in changed_files or base_sources is None or head_sources is None:
        raise AutofixPolicyError("自动修复必须包含单一来源配置变化")
    changed_fields = validate_sources_semantic_diff(
        base_sources,
        head_sources,
        source_id=source_id,
        allowed_fields={str(field) for field in source_policy.get("allowed_fields", [])},
    )
    return {
        "source_id": source_id,
        "reason_code": reason_code,
        "base_sha": base_sha,
        "head_sha": head_sha,
        "changed_files": changed_files,
        "changed_fields": sorted(changed_fields),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="校验 WorkBuddy 单信源自动修复差异")
    parser.add_argument("--event", required=True)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    try:
        config = load_config(Path(args.config))
        event = parse_event(Path(args.event))
        names = [line.strip() for line in _git("diff", "--name-only", f"{args.base}...{args.head}").splitlines() if line.strip()]
        numstat = _git("diff", "--numstat", f"{args.base}...{args.head}")
        changed_lines = 0
        for line in numstat.splitlines():
            parts = line.split("\t", 2)
            if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                changed_lines += int(parts[0]) + int(parts[1])
        base_sources = head_sources = None
        if "sources.json" in names:
            base_sources = _load_json_text(_git("show", f"{args.base}:sources.json"), name="base sources.json")
            head_sources = _load_json_text(_git("show", f"{args.head}:sources.json"), name="head sources.json")
        result = validate_patch(
            config=config,
            event=event,
            base_sha=args.base,
            head_sha=args.head,
            changed_files=names,
            base_sources=base_sources,
            head_sources=head_sources,
            changed_lines=changed_lines,
        )
    except AutofixPolicyError as exc:
        print(f"[workbuddy-autofix-gate] FAIL: {exc}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps(result, ensure_ascii=False))
    else:
        print(f"[workbuddy-autofix-gate] PASS: {result['source_id']} ({result['reason_code']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
