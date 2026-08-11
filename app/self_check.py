from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .common import ensure_dir, read_json, write_json
from .report import METHOD_ORDER

SCHEMA_VERSION = "robtaxi-health-v1"
REPAIR_SCHEMA_VERSION = "robtaxi-repair-request-v1"
SEVERITY_ORDER = {"healthy": 0, "warning": 1, "error": 2, "critical": 3}
CORE_STAGES = ("fetch", "parse", "filter", "enrich", "summarize", "editorial_digest", "render")
SAFE_NOTIFY_STATUSES = {"success", "already_sent"}


@dataclass(frozen=True)
class Finding:
    check_id: str
    severity: str
    summary: str
    evidence: dict[str, Any]


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sanitize_text(value: Any, limit: int = 300) -> str:
    """对进入健康报告的自由文本做基础脱敏，避免泄漏密钥和 Webhook。"""
    text = str(value or "")
    text = re.sub(r"(?i)(api[_-]?key|token|secret|authorization)\s*[:=]\s*\S+", r"\1=[REDACTED]", text)
    text = re.sub(r"https://(?:open\.feishu\.cn|qyapi\.weixin\.qq\.com)/\S+", "[REDACTED_WEBHOOK]", text)
    return text[:limit]


def count_jsonl(path: Path) -> int:
    count = 0
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"line {line_number}: {exc.msg}") from exc
            if not isinstance(row, dict):
                raise ValueError(f"line {line_number}: row must be an object")
            count += 1
    return count


def classify_source_failure_rate(rate: float) -> str:
    if rate >= 0.60:
        return "critical"
    if rate >= 0.30:
        return "error"
    if rate > 0:
        return "warning"
    return "healthy"


def classify_summary_fallback_rate(rate: float) -> str:
    if rate >= 0.20:
        return "error"
    if rate > 0:
        return "warning"
    return "healthy"


def highest_severity(findings: list[Finding]) -> str:
    if not findings:
        return "healthy"
    return max((item.severity for item in findings), key=lambda item: SEVERITY_ORDER[item])


def _add_finding(
    findings: list[Finding],
    check_id: str,
    severity: str,
    summary: str,
    **evidence: Any,
) -> None:
    if severity == "healthy":
        return
    findings.append(
        Finding(
            check_id=check_id,
            severity=severity,
            summary=_sanitize_text(summary),
            evidence={key: _sanitize_evidence(value) for key, value in evidence.items()},
        )
    )


def _sanitize_evidence(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _sanitize_evidence(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_sanitize_evidence(item) for item in value[:30]]
    if isinstance(value, str):
        return _sanitize_text(value)
    if isinstance(value, (int, float, bool)) or value is None:
        return value
    return _sanitize_text(value)


def _expected_window(date_text: str) -> tuple[str, str]:
    run_date = date.fromisoformat(date_text)
    start = run_date - timedelta(days=1)
    return f"{start.isoformat()} 00:00:00", f"{run_date.isoformat()} 00:00:00"


def _load_thresholds(config: dict[str, Any]) -> dict[str, float]:
    defaults = config.get("defaults", {}) if isinstance(config, dict) else {}
    raw = defaults.get("self_check", {}) if isinstance(defaults, dict) else {}
    if not isinstance(raw, dict):
        raw = {}
    return {
        "source_error": _safe_float(raw.get("source_failure_error_rate", 0.30), 0.30),
        "source_critical": _safe_float(raw.get("source_failure_critical_rate", 0.60), 0.60),
        "summary_error": _safe_float(raw.get("summary_fallback_error_rate", 0.20), 0.20),
    }


def _source_severity(rate: float, thresholds: dict[str, float]) -> str:
    if rate >= thresholds["source_critical"]:
        return "critical"
    if rate >= thresholds["source_error"]:
        return "error"
    if rate > 0:
        return "warning"
    return "healthy"


def _summary_severity(rate: float, thresholds: dict[str, float]) -> str:
    if rate >= thresholds["summary_error"]:
        return "error"
    if rate > 0:
        return "warning"
    return "healthy"


def evaluate_health(
    *,
    date_text: str,
    workspace: Path,
    config: dict[str, Any],
    report: dict[str, Any] | None,
    report_error: str,
    build_status: str,
    test_status: str,
    deploy_status: str,
    notify_job_status: str,
    notify_expected: bool,
    feishu_status: str,
    wecom_status: str,
) -> tuple[list[Finding], dict[str, Any]]:
    findings: list[Finding] = []
    thresholds = _load_thresholds(config)

    build_ok = build_status == "success"
    if not build_ok:
        _add_finding(
            findings,
            "github_build",
            "critical",
            "GitHub build 未成功，流水线核心结果不可用",
            observed=build_status or "unknown",
            test_status=test_status or "unknown",
        )
    elif test_status != "success":
        _add_finding(
            findings,
            "github_tests",
            "critical",
            "测试未成功完成",
            observed=test_status or "unknown",
        )

    if build_ok and deploy_status != "success":
        _add_finding(
            findings,
            "github_deploy",
            "critical",
            "GitHub Pages 部署未成功",
            observed=deploy_status or "unknown",
        )

    report_path = workspace / "artifacts" / "reports" / date_text / "run_report.json"
    if report is None:
        _add_finding(
            findings,
            "run_report",
            "critical",
            "run_report.json 缺失或无法解析",
            path=str(report_path.relative_to(workspace)),
            error=report_error or "missing",
        )
        return findings, {
            "source_required_total": 0,
            "source_failed_total": 0,
            "source_failure_rate": 0.0,
            "summary_total": 0,
            "summary_fallback_total": 0,
            "summary_fallback_rate": 0.0,
        }

    stage_status = report.get("stage_status", {})
    stage_status = stage_status if isinstance(stage_status, dict) else {}
    for stage in CORE_STAGES:
        status = str(stage_status.get(stage, "pending"))
        if status in {"failed", "pending", ""}:
            _add_finding(
                findings,
                f"stage_{stage}",
                "critical",
                f"核心阶段 {stage} 未正常完成",
                observed=status or "missing",
            )
        elif stage == "enrich" and status == "partial":
            _add_finding(
                findings,
                "stage_enrich_partial",
                "warning",
                "正文补全阶段部分失败，但简报仍可继续生成",
                observed=status,
            )
        elif stage not in {"fetch", "filter", "summarize", "enrich"} and status != "success":
            _add_finding(
                findings,
                f"stage_{stage}_unexpected",
                "error",
                f"核心阶段 {stage} 返回非预期状态",
                observed=status,
            )

    expected_start, expected_end = _expected_window(date_text)
    observed_start = str(report.get("window_start_bj", ""))
    observed_end = str(report.get("window_end_bj", ""))
    if observed_start != expected_start or observed_end != expected_end:
        _add_finding(
            findings,
            "beijing_window",
            "critical",
            "北京时间统计窗口不是前一自然日",
            expected_start=expected_start,
            expected_end=expected_end,
            observed_start=observed_start,
            observed_end=observed_end,
        )

    source_stats = report.get("source_stats", [])
    source_stats = source_stats if isinstance(source_stats, list) else []
    required_stats: list[dict[str, Any]] = []
    failed_required: list[dict[str, Any]] = []
    optional_missing_count = 0
    for raw in source_stats:
        if not isinstance(raw, dict):
            continue
        source_type = str(raw.get("source_type", ""))
        reason_code = str(raw.get("error_reason_code", ""))
        if source_type == "search_api" and reason_code == "search_api_missing_key":
            optional_missing_count += 1
            continue
        required_stats.append(raw)
        if str(raw.get("status", "")) != "ok":
            failed_required.append(
                {
                    "source_id": str(raw.get("source_id", "")),
                    "status": str(raw.get("status", "")),
                    "fetched_items": _safe_int(raw.get("fetched_items", 0)),
                    "reason_code": reason_code,
                    "reason": str(raw.get("error_reason_zh", "")),
                    "error_detail": _sanitize_text(raw.get("error_raw", ""), 300),
                }
            )

    source_rate = len(failed_required) / len(required_stats) if required_stats else 0.0
    source_severity = _source_severity(source_rate, thresholds)
    _add_finding(
        findings,
        "required_source_failure_rate",
        source_severity,
        "必需数据源存在抓取失败",
        failed=len(failed_required),
        total=len(required_stats),
        rate=round(source_rate, 4),
        failed_sources=failed_required[:20],
    )

    summary_total = _safe_int(report.get("summary_structured_count", report.get("brief_count", 0)))
    summary_fallback = _safe_int(report.get("summarize_fail_count", 0))
    summary_rate = summary_fallback / summary_total if summary_total else 0.0
    summary_severity = _summary_severity(summary_rate, thresholds)
    _add_finding(
        findings,
        "summary_fallback_rate",
        summary_severity,
        "DeepSeek 摘要发生规则降级",
        fallback=summary_fallback,
        total=summary_total,
        rate=round(summary_rate, 4),
    )

    editorial_fallback_reason = str(report.get("editorial_digest_fallback_reason", ""))
    expected_empty_digest = (
        editorial_fallback_reason == "no_items"
        and _safe_int(report.get("relevance_kept", 0)) == 0
        and _safe_int(report.get("brief_count", 0)) == 0
    )
    if bool(report.get("editorial_digest_fallback_used", False)) and not expected_empty_digest:
        _add_finding(
            findings,
            "editorial_digest_fallback",
            "warning",
            "最终编辑摘要使用了规则兜底版本",
            reason=editorial_fallback_reason,
        )

    total_in = _safe_int(report.get("relevance_total_in", 0))
    kept = _safe_int(report.get("relevance_kept", 0))
    dropped = _safe_int(report.get("relevance_dropped", 0))
    if total_in != kept + dropped:
        _add_finding(
            findings,
            "relevance_conservation",
            "error",
            "相关性过滤漏斗不守恒",
            total_in=total_in,
            kept=kept,
            dropped=dropped,
        )

    funnel = report.get("stage_funnel", {})
    funnel = funnel if isinstance(funnel, dict) else {}
    for method in METHOD_ORDER:
        counts = funnel.get(method, {})
        counts = counts if isinstance(counts, dict) else {}
        candidate = _safe_int(counts.get("candidate", 0))
        filtered = _safe_int(counts.get("filtered", 0))
        method_kept = _safe_int(counts.get("kept", 0))
        if candidate != filtered + method_kept:
            _add_finding(
                findings,
                f"funnel_{method}",
                "error",
                f"{method} 漏斗不守恒",
                candidate=candidate,
                filtered=filtered,
                kept=method_kept,
            )

    brief_count = _safe_int(report.get("brief_count", 0))
    if brief_count > kept:
        _add_finding(
            findings,
            "brief_exceeds_kept",
            "error",
            "简报条目数大于相关性过滤保留数",
            brief_count=brief_count,
            relevance_kept=kept,
        )
    if kept > 0 and brief_count == 0:
        _add_finding(
            findings,
            "brief_empty_after_kept",
            "error",
            "过滤阶段保留了新闻，但最终简报为空",
            relevance_kept=kept,
        )

    if build_ok:
        jsonl_expectations = (
            ("raw_items", workspace / "artifacts" / "raw" / date_text / "raw_items.jsonl", "total_items_raw"),
            (
                "canonical_items",
                workspace / "artifacts" / "canonical" / date_text / "canonical_items.jsonl",
                "total_items_canonical",
            ),
            (
                "filtered_items",
                workspace / "artifacts" / "filtered" / date_text / "filtered_items.jsonl",
                "relevance_kept",
            ),
            (
                "dropped_items",
                workspace / "artifacts" / "filtered" / date_text / "dropped_items.jsonl",
                "relevance_dropped",
            ),
            (
                "enriched_items",
                workspace / "artifacts" / "enriched" / date_text / "enriched_items.jsonl",
                "enrich_total",
            ),
            ("brief_items", workspace / "artifacts" / "brief" / date_text / "brief_items.jsonl", "brief_count"),
        )
        for check_id, path, report_key in jsonl_expectations:
            if not path.exists():
                _add_finding(
                    findings,
                    f"artifact_{check_id}",
                    "critical",
                    f"必要产物 {check_id} 缺失",
                    path=str(path.relative_to(workspace)),
                )
                continue
            try:
                actual = count_jsonl(path)
            except (OSError, ValueError) as exc:
                _add_finding(
                    findings,
                    f"artifact_{check_id}_invalid",
                    "critical",
                    f"必要产物 {check_id} 无法解析",
                    path=str(path.relative_to(workspace)),
                    error=str(exc),
                )
                continue
            expected = _safe_int(report.get(report_key, 0))
            if actual != expected:
                _add_finding(
                    findings,
                    f"artifact_{check_id}_count",
                    "error",
                    f"{check_id} 行数与运行报告不一致",
                    actual=actual,
                    expected=expected,
                    report_key=report_key,
                )

        digest_path = workspace / "artifacts" / "digest" / date_text / "daily_digest.json"
        try:
            digest = read_json(digest_path)
            if not isinstance(digest, dict):
                raise ValueError("digest must be an object")
        except Exception as exc:
            _add_finding(
                findings,
                "artifact_daily_digest",
                "critical",
                "最终编辑摘要缺失或无法解析",
                path=str(digest_path.relative_to(workspace)),
                error=str(exc),
            )

        site_path = workspace / "site" / "index.html"
        if not site_path.exists() or site_path.stat().st_size == 0:
            _add_finding(
                findings,
                "artifact_site",
                "critical",
                "最终页面缺失或为空",
                path=str(site_path.relative_to(workspace)),
            )

    if notify_expected and build_ok and deploy_status == "success":
        statuses = [feishu_status, wecom_status]
        failures = [status for status in statuses if status not in SAFE_NOTIFY_STATUSES]
        if len(failures) == 2:
            _add_finding(
                findings,
                "notify_all_failed",
                "critical",
                "飞书和企业微信均未成功推送",
                feishu=feishu_status or "unknown",
                wecom=wecom_status or "unknown",
            )
        elif notify_job_status != "success":
            _add_finding(
                findings,
                "github_notify_job",
                "error",
                "通知渠道已报告成功，但 GitHub notify job 未成功收尾",
                observed=notify_job_status or "unknown",
            )
        elif len(failures) == 1:
            _add_finding(
                findings,
                "notify_partial_failed",
                "error",
                "一个通知渠道推送失败",
                feishu=feishu_status or "unknown",
                wecom=wecom_status or "unknown",
            )

    metrics = {
        "source_required_total": len(required_stats),
        "source_failed_total": len(failed_required),
        "source_failure_rate": round(source_rate, 4),
        "optional_search_api_missing_count": optional_missing_count,
        "summary_total": summary_total,
        "summary_fallback_total": summary_fallback,
        "summary_fallback_rate": round(summary_rate, 4),
        "relevance_total_in": total_in,
        "relevance_kept": kept,
        "relevance_dropped": dropped,
        "brief_count": brief_count,
    }
    return findings, metrics


def build_health_report(
    *,
    date_text: str,
    workspace: Path,
    config: dict[str, Any],
    build_status: str,
    test_status: str,
    deploy_status: str,
    notify_job_status: str,
    notify_expected: bool,
    feishu_status: str,
    wecom_status: str,
    github_run_id: str,
    github_run_attempt: str,
    commit_sha: str,
    repository: str,
    run_url: str,
    event_name: str,
    fixture_severity: str = "none",
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    report_path = workspace / "artifacts" / "reports" / date_text / "run_report.json"
    report: dict[str, Any] | None = None
    report_error = ""
    try:
        report = read_json(report_path)
        if not isinstance(report, dict):
            raise ValueError("run report must be an object")
    except Exception as exc:
        report_error = _sanitize_text(exc)

    findings, metrics = evaluate_health(
        date_text=date_text,
        workspace=workspace,
        config=config,
        report=report,
        report_error=report_error,
        build_status=build_status,
        test_status=test_status,
        deploy_status=deploy_status,
        notify_job_status=notify_job_status,
        notify_expected=notify_expected,
        feishu_status=feishu_status,
        wecom_status=wecom_status,
    )
    if fixture_severity in {"warning", "error", "critical"}:
        _add_finding(
            findings,
            "workflow_dispatch_fixture",
            fixture_severity,
            "workflow_dispatch 注入的自检验收事件",
            fixture_severity=fixture_severity,
        )
    overall_status = highest_severity(findings)
    run_attempt = github_run_attempt or "1"
    request_id = f"rh_{github_run_id or 'local'}_{run_attempt}"
    generated_at = datetime.now(timezone.utc).isoformat()
    report_sha256 = _sha256_file(report_path) if report_path.exists() else ""
    health_report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "generated_at_utc": generated_at,
        "date_bj": date_text,
        "overall_status": overall_status,
        "codex_required": overall_status != "healthy",
        "run": {
            "github_run_id": github_run_id,
            "github_run_attempt": run_attempt,
            "commit_sha": commit_sha,
            "repository": repository,
            "run_url": run_url,
            "event_name": event_name,
            "build_status": build_status,
            "test_status": test_status,
            "deploy_status": deploy_status,
            "notify_job_status": notify_job_status,
            "notify_expected": notify_expected,
            "feishu_status": feishu_status,
            "wecom_status": wecom_status,
        },
        "metrics": metrics,
        "findings": [asdict(item) for item in findings],
        "source_report": {
            "path": str(report_path.relative_to(workspace)),
            "sha256": report_sha256,
            "available": report is not None,
        },
    }

    repair_request: dict[str, Any] | None = None
    if overall_status != "healthy":
        repair_request = {
            "schema_version": REPAIR_SCHEMA_VERSION,
            "request_id": request_id,
            "created_at_utc": generated_at,
            "date_bj": date_text,
            "overall_status": overall_status,
            "base_commit_sha": commit_sha,
            "github_run_id": github_run_id,
            "github_run_attempt": run_attempt,
            "repository": repository,
            "run_url": run_url,
            "health_report_sha256": "",
            "run_report_sha256": report_sha256,
            "findings": [asdict(item) for item in findings],
            "constraints": {
                "diagnosis_only": True,
                "do_not_modify_code": True,
                "separate_facts_and_hypotheses": True,
                "allow_no_code_change": True,
                "approval_required_before_fix": True,
            },
        }
    return health_report, repair_request


def render_health_markdown(report: dict[str, Any]) -> str:
    status_labels = {
        "healthy": "健康",
        "warning": "警告",
        "error": "错误",
        "critical": "严重故障",
    }
    lines = [
        f"# Robtaxi 运行自检：{report['date_bj']}",
        "",
        f"- 总体状态：**{status_labels.get(str(report['overall_status']), report['overall_status'])}**",
        f"- request_id：`{report['request_id']}`",
        f"- GitHub Run：{report.get('run', {}).get('run_url', '') or '本地运行'}",
        "",
        "## 检查结果",
        "",
    ]
    findings = report.get("findings", [])
    if not findings:
        lines.append("- 所有检查通过。")
    else:
        for item in findings:
            lines.append(
                f"- **{str(item.get('severity', '')).upper()} · {item.get('check_id', '')}**："
                f"{item.get('summary', '')}"
            )
    lines.extend(["", "## 核心指标", ""])
    for key, value in report.get("metrics", {}).items():
        lines.append(f"- `{key}`：{value}")
    return "\n".join(lines) + "\n"


def _write_github_output(path: str, values: dict[str, str]) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def write_internal_failure_report(args: argparse.Namespace, exc: Exception) -> Path:
    """自检程序异常时仍尽力留下可追踪的 critical 报告和修复请求。"""
    generated_at = datetime.now(timezone.utc).isoformat()
    run_attempt = str(args.github_run_attempt or "1")
    request_id = f"rh_{args.github_run_id or 'local'}_{run_attempt}"
    finding = Finding(
        check_id="self_check_internal_failure",
        severity="critical",
        summary="自检程序自身无法正常完成",
        evidence={"error": _sanitize_text(exc, 500)},
    )
    health_report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request_id": request_id,
        "generated_at_utc": generated_at,
        "date_bj": str(args.date),
        "overall_status": "critical",
        "codex_required": True,
        "run": {
            "github_run_id": str(args.github_run_id),
            "github_run_attempt": run_attempt,
            "commit_sha": str(args.commit_sha),
            "repository": str(args.repository),
            "run_url": str(args.run_url),
            "event_name": str(args.event_name),
            "build_status": str(args.build_status),
            "test_status": str(args.test_status),
            "deploy_status": str(args.deploy_status),
            "notify_job_status": str(args.notify_job_status),
            "notify_expected": bool(args.notify_expected),
            "feishu_status": str(args.feishu_status),
            "wecom_status": str(args.wecom_status),
        },
        "metrics": {},
        "findings": [asdict(finding)],
        "source_report": {"path": "", "sha256": "", "available": False},
    }
    out_dir = Path(args.out).expanduser().resolve() / str(args.date)
    ensure_dir(out_dir)
    health_json = out_dir / "health_report.json"
    write_json(health_json, health_report)
    (out_dir / "health_report.md").write_text(
        render_health_markdown(health_report),
        encoding="utf-8",
    )
    repair_request = {
        "schema_version": REPAIR_SCHEMA_VERSION,
        "request_id": request_id,
        "created_at_utc": generated_at,
        "date_bj": str(args.date),
        "overall_status": "critical",
        "base_commit_sha": str(args.commit_sha),
        "github_run_id": str(args.github_run_id),
        "github_run_attempt": run_attempt,
        "repository": str(args.repository),
        "run_url": str(args.run_url),
        "health_report_sha256": _sha256_file(health_json),
        "run_report_sha256": "",
        "findings": [asdict(finding)],
        "constraints": {
            "diagnosis_only": True,
            "do_not_modify_code": True,
            "separate_facts_and_hypotheses": True,
            "allow_no_code_change": True,
            "approval_required_before_fix": True,
        },
    }
    write_json(out_dir / "repair_request.json", repair_request)
    _write_github_output(
        args.github_output,
        {
            "overall_status": "critical",
            "codex_required": "true",
            "request_id": request_id,
            "health_dir": str(out_dir),
        },
    )
    return out_dir


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run final Robtaxi pipeline health checks")
    parser.add_argument("--date", required=True, help="Beijing run date YYYY-MM-DD")
    parser.add_argument("--workspace", default=".", help="Downloaded workflow workspace root")
    parser.add_argument("--sources", default="./sources.json", help="sources.json path")
    parser.add_argument("--out", default="./artifacts/health", help="Health artifact root")
    parser.add_argument("--build-status", default="unknown")
    parser.add_argument("--test-status", default="unknown")
    parser.add_argument("--deploy-status", default="unknown")
    parser.add_argument("--notify-job-status", default="unknown")
    parser.add_argument("--notify-expected", action="store_true")
    parser.add_argument("--feishu-status", default="skipped")
    parser.add_argument("--wecom-status", default="skipped")
    parser.add_argument("--github-run-id", default="")
    parser.add_argument("--github-run-attempt", default="1")
    parser.add_argument("--commit-sha", default="")
    parser.add_argument("--repository", default="")
    parser.add_argument("--run-url", default="")
    parser.add_argument("--event-name", default="")
    parser.add_argument("--fixture-severity", choices=("none", "warning", "error", "critical"), default="none")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    return parser


def main() -> int:
    args = build_arg_parser().parse_args()
    try:
        workspace = Path(args.workspace).expanduser().resolve()
        config = read_json(Path(args.sources).expanduser().resolve())
        health_report, repair_request = build_health_report(
            date_text=args.date,
            workspace=workspace,
            config=config,
            build_status=args.build_status,
            test_status=args.test_status,
            deploy_status=args.deploy_status,
            notify_job_status=args.notify_job_status,
            notify_expected=bool(args.notify_expected),
            feishu_status=args.feishu_status,
            wecom_status=args.wecom_status,
            github_run_id=args.github_run_id,
            github_run_attempt=args.github_run_attempt,
            commit_sha=args.commit_sha,
            repository=args.repository,
            run_url=args.run_url,
            event_name=args.event_name,
            fixture_severity=args.fixture_severity,
        )
        out_dir = Path(args.out).expanduser().resolve() / args.date
        ensure_dir(out_dir)
        health_json = out_dir / "health_report.json"
        health_md = out_dir / "health_report.md"
        write_json(health_json, health_report)
        health_md.write_text(render_health_markdown(health_report), encoding="utf-8")
        if repair_request is not None:
            repair_request["health_report_sha256"] = _sha256_file(health_json)
            write_json(out_dir / "repair_request.json", repair_request)

        _write_github_output(
            args.github_output,
            {
                "overall_status": str(health_report["overall_status"]),
                "codex_required": str(bool(health_report["codex_required"])).lower(),
                "request_id": str(health_report["request_id"]),
                "health_dir": str(out_dir),
            },
        )
        print(
            f"[self_check] date={args.date} status={health_report['overall_status']} "
            f"findings={len(health_report['findings'])} request_id={health_report['request_id']}"
        )
        return 0
    except Exception as exc:
        try:
            out_dir = write_internal_failure_report(args, exc)
            print(
                f"[self_check] internal failure: {_sanitize_text(exc, 500)} "
                f"critical_report={out_dir}"
            )
        except Exception as report_exc:
            print(
                f"[self_check] internal failure: {_sanitize_text(exc, 500)}; "
                f"could not write fallback report: {_sanitize_text(report_exc, 500)}"
            )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
