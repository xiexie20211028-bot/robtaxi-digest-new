from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import subprocess
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Optional
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen

try:
    from zoneinfo import ZoneInfo
except ImportError:  # pragma: no cover
    ZoneInfo = None


USER_AGENT = "Mozilla/5.0 (RobtaxiDigest2/1.0)"
UNPARSABLE_DT_FALLBACK = datetime(1970, 1, 1, tzinfo=timezone.utc)
_DOMAIN_RATE_LOCK = threading.Lock()
_DOMAIN_NEXT_REQUEST: dict[str, float] = {}


@dataclass
class RawItem:
    source_id: str
    source_name: str
    source_type: str
    region: str
    company_hint: str
    fetched_at: str
    url: str
    payload: dict[str, Any]
    source_role: str = "secondary"
    evidence_type: str = "general_media"
    criticality: str = "important"
    coverage_domains: list[str] = field(default_factory=list)
    official_accounts: dict[str, Any] = field(default_factory=dict)


@dataclass
class CanonicalItem:
    id: str
    source_id: str
    source_name: str
    region: str
    company_hint: str
    title: str
    content: str
    link: str
    published_at_utc: str
    published_missing: bool
    published_parse_status: str
    discovery_query_group: str
    language: str
    fingerprint: str
    published_source: str = "feed"
    resolved_ok: bool = True
    resolved_url: str = ""
    query_rss_verify_error_code: str = ""
    query_rss_verify_error_zh: str = ""
    coverage_domains: list[str] = field(default_factory=list)
    automation_level: str = "unknown"
    event_type: str = "other"
    deployment_stage: str = "unknown"
    source_role: str = "secondary"
    evidence_type: str = "general_media"
    criticality: str = "important"
    canonical_url: str = ""
    first_seen_at_utc: str = ""
    late_arrival: bool = False
    social_platform: str = ""
    official_account_verified: bool = False
    outbound_urls: list[str] = field(default_factory=list)
    discovery_method: str = "direct_source"
    evidence: list[dict[str, Any]] = field(default_factory=list)
    agent_run_id: str = ""
    agent_verification_status: str = ""
    agent_importance_score: int = 0
    source_type: str = ""


@dataclass
class BriefItem:
    id: str
    source_id: str
    source_name: str
    region: str
    company_id: str
    title_zh: str
    summary_zh: str
    summary_what: str
    summary_why: str
    summary_so_what: str
    impact_targets: list[str]
    summary_format_version: str
    link: str
    published_at_utc: str
    tags: list[str]
    confidence: float
    importance: int = 3
    coverage_domains: list[str] = field(default_factory=list)
    automation_level: str = "unknown"
    event_type: str = "other"
    deployment_stage: str = "unknown"
    source_role: str = "secondary"
    evidence_type: str = "general_media"
    canonical_url: str = ""
    first_seen_at_utc: str = ""
    late_arrival: bool = False
    social_platform: str = ""
    official_account_verified: bool = False
    outbound_urls: list[str] = field(default_factory=list)
    fingerprint: str = ""
    resolved_url: str = ""
    relevance_score: int = 0
    discovery_method: str = "direct_source"
    evidence: list[dict[str, Any]] = field(default_factory=list)
    agent_run_id: str = ""
    agent_verification_status: str = ""
    agent_importance_score: int = 0


@dataclass
class SourceStat:
    source_id: str
    source_name: str
    source_type: str
    status: str
    fetched_items: int
    error: str = ""
    error_reason_code: str = ""
    error_reason_zh: str = ""
    error_raw: str = ""
    source_role: str = "secondary"
    evidence_type: str = "general_media"
    criticality: str = "important"
    coverage_domains: list[str] = field(default_factory=list)
    health_policy: dict[str, Any] = field(default_factory=dict)
    request_count: int = 0
    request_success_count: int = 0
    listed_items: int = 0
    valid_items: int = 0
    date_parsed_items: int = 0
    body_parsed_items: int = 0
    fresh_items: int = 0
    whitelist_rejected_items: int = 0
    date_parse_rate: float = 0.0
    body_parse_rate: float = 0.0
    whitelist_reject_rate: float = 0.0
    newest_published_at: str = ""
    last_success_at: str = ""


def detect_xml_encoding(data: bytes) -> str:
    """Detect encoding from XML declaration or BOM, normalizing CJK codecs."""
    # BOM detection
    if data[:3] == b"\xef\xbb\xbf":
        return "utf-8"
    if data[:2] in (b"\xff\xfe", b"\xfe\xff"):
        return "utf-16"

    # Parse <?xml encoding="xxx"?> declaration (ASCII-safe prefix)
    header = data[:200]
    m = re.search(rb'<\?xml\b[^?]*\bencoding=["\']([^"\']+)["\']', header, re.IGNORECASE)
    if m:
        declared = m.group(1).decode("ascii", errors="ignore").strip().lower()
        # Python's gb18030 codec is a superset that handles gb2312 and gbk
        if declared in ("gb2312", "gbk", "gb18030"):
            return "gb18030"
        return declared

    return "utf-8"


def now_beijing() -> datetime:
    if ZoneInfo is None:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    return datetime.now(ZoneInfo("Asia/Shanghai"))


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            out.append(json.loads(line))
    return out


def _parse_relative_datetime(text: str, now_utc: datetime) -> Optional[datetime]:
    compact = text.strip().lower()
    if not compact:
        return None

    if compact in {"just now", "刚刚", "刚才"}:
        return now_utc

    # 英文相对时间，例如 "2 hours ago"。
    m = re.search(r"\b(\d+)\s*(minute|minutes|min|hour|hours|hr|hrs|day|days)\s+ago\b", compact)
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        if unit.startswith(("minute", "min")):
            return now_utc - timedelta(minutes=value)
        if unit.startswith(("hour", "hr")):
            return now_utc - timedelta(hours=value)
        if unit.startswith("day"):
            return now_utc - timedelta(days=value)

    # 中文相对时间，例如 "2小时前"、"3天前"。
    m = re.search(r"(\d+)\s*(分钟|小时|天)前", compact)
    if m:
        value = int(m.group(1))
        unit = m.group(2)
        if unit == "分钟":
            return now_utc - timedelta(minutes=value)
        if unit == "小时":
            return now_utc - timedelta(hours=value)
        if unit == "天":
            return now_utc - timedelta(days=value)

    # yesterday / 昨天 这种轻量表达。
    if compact.startswith("yesterday") or compact.startswith("昨天"):
        return now_utc - timedelta(days=1)

    return None


def parse_datetime_with_status(value: str) -> tuple[datetime, str]:
    now_utc = datetime.utcnow().replace(tzinfo=timezone.utc)
    if not value or not value.strip():
        return now_utc, "missing"

    # 一些 RSS 源会在时区前插入多个空格，先统一空白字符，避免标准格式匹配失败。
    text = " ".join(value.strip().split())
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S %z",
        "%Y-%m-%d %H:%M %z",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d %H:%M",
        "%Y/%m/%d",
        "%B %d, %Y",
        "%b %d, %Y",
        "%Y年%m月%d日 %H:%M:%S",
        "%Y年%m月%d日 %H:%M",
        "%Y-%m-%d",
        "%Y年%m月%d日",
    ):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc), "ok"
            return dt.astimezone(timezone.utc), "ok"
        except ValueError:
            continue

    try:
        dt = parsedate_to_datetime(text)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc), "ok"
    except (TypeError, ValueError):
        pass

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc), "ok"
    except ValueError:
        rel_dt = _parse_relative_datetime(text, now_utc)
        if rel_dt is not None:
            return rel_dt, "ok"

        # 解析失败时回落到旧时间，避免把旧闻误判为“刚发布”。
        low = text.lower()
        if any(k in low for k in ("ago", "小时前", "分钟前", "天前", "yesterday", "昨天", "刚刚", "刚才")):
            return UNPARSABLE_DT_FALLBACK, "unparseable_relative"
        return UNPARSABLE_DT_FALLBACK, "unparseable_other"


def parse_datetime(value: str) -> datetime:
    dt, _ = parse_datetime_with_status(value)
    return dt


def utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def is_recent(ts_iso: str, days: int) -> bool:
    dt = parse_datetime(ts_iso)
    cutoff = datetime.utcnow().replace(tzinfo=timezone.utc) - timedelta(days=days)
    return dt >= cutoff


def http_get_bytes(
    url: str,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 20,
    retries: int = 3,
    backoff: float = 1.5,
    min_domain_interval: float = 0.0,
    cache_dir: Optional[Path] = None,
) -> bytes:
    redirect_codes = {301, 302, 303, 307, 308}
    max_redirects = 5
    last_err: Optional[Exception] = None
    req_headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    if headers:
        req_headers.update(headers)

    cache_body_path: Optional[Path] = None
    cache_meta_path: Optional[Path] = None
    if cache_dir is not None:
        cache_key = sha1_text(url)
        cache_body_path = cache_dir / f"{cache_key}.body"
        cache_meta_path = cache_dir / f"{cache_key}.json"
        if cache_meta_path.exists() and cache_body_path.exists():
            try:
                cache_meta = read_json(cache_meta_path)
                etag = str(cache_meta.get("etag", "")).strip()
                last_modified = str(cache_meta.get("last_modified", "")).strip()
                if etag:
                    req_headers["If-None-Match"] = etag
                if last_modified:
                    req_headers["If-Modified-Since"] = last_modified
            except Exception:
                pass

    for i in range(retries):
        try:
            current_url = url
            for _ in range(max_redirects + 1):
                _wait_for_domain(current_url, min_domain_interval)
                req = Request(current_url, headers=req_headers)
                with urlopen(req, timeout=timeout) as resp:
                    code = int(getattr(resp, "status", 200) or 200)
                    if code in redirect_codes:
                        location = (resp.headers.get("Location") or "").strip()
                        if not location:
                            return resp.read()
                        current_url = urljoin(current_url, location)
                        continue
                    body = resp.read()
                    if cache_body_path is not None and cache_meta_path is not None:
                        ensure_dir(cache_body_path.parent)
                        cache_body_path.write_bytes(body)
                        write_json(
                            cache_meta_path,
                            {
                                "url": url,
                                "etag": str(resp.headers.get("ETag", "")).strip(),
                                "last_modified": str(resp.headers.get("Last-Modified", "")).strip(),
                                "updated_at": datetime.now(timezone.utc).isoformat(),
                            },
                        )
                    return body
            raise RuntimeError(f"too many redirects: {url}")
        except HTTPError as err:
            if err.code == 304 and cache_body_path is not None and cache_body_path.exists():
                return cache_body_path.read_bytes()
            if err.code in redirect_codes:
                location = (err.headers.get("Location") or "").strip() if err.headers else ""
                if location:
                    try:
                        current_url = urljoin(url, location)
                        for _ in range(max_redirects):
                            req = Request(current_url, headers=req_headers)
                            with urlopen(req, timeout=timeout) as resp:
                                code = int(getattr(resp, "status", 200) or 200)
                                if code in redirect_codes:
                                    next_location = (resp.headers.get("Location") or "").strip()
                                    if not next_location:
                                        return resp.read()
                                    current_url = urljoin(current_url, next_location)
                                    continue
                                return resp.read()
                        raise RuntimeError(f"too many redirects: {url}")
                    except Exception as redirect_err:  # pragma: no cover
                        last_err = redirect_err
                        time.sleep(backoff * (i + 1))
                        continue
            if err.code in {403, 429, 500, 502, 503, 504, 520, 521, 522, 523, 524}:
                if err.code == 429:
                    retry_after = _retry_after_seconds(err.headers.get("Retry-After", "") if err.headers else "")
                    if retry_after > 0:
                        time.sleep(min(retry_after, 60.0))
                try:
                    return _curl_http_get(url, req_headers, timeout, retries)
                except Exception as curl_err:
                    last_err = curl_err
                    time.sleep(backoff * (2**i))
                    continue
            last_err = err
            time.sleep(backoff * (2**i))
        except (URLError, TimeoutError) as err:
            last_err = err
            time.sleep(backoff * (2**i))
        except Exception as err:  # pragma: no cover
            last_err = err
            time.sleep(backoff * (2**i))

    # 某些站点在 urllib TLS 栈下不稳定，最后一次使用 curl 兜底。
    if last_err is not None:
        err_text = str(last_err).lower()
        should_fallback = any(
            key in err_text
            for key in (
                "ssl",
                "wrong version number",
                "handshake",
                "tls",
                "eof occurred in violation of protocol",
                "timed out",
                "timeout",
                "remote end closed connection without response",
                "http/2 stream",
                "not closed cleanly",
                "internal_error",
                "incompleteread",
            )
        )
        if should_fallback:
            try:
                return _curl_http_get(url, req_headers, timeout, retries)
            except Exception as curl_err:
                last_err = curl_err

    raise RuntimeError(f"http_get_bytes failed for {url}: {last_err}")


def _wait_for_domain(url: str, min_interval: float) -> None:
    interval = max(0.0, float(min_interval or 0.0))
    if interval <= 0:
        return
    host = (urlparse(url).netloc or "").lower()
    if not host:
        return
    with _DOMAIN_RATE_LOCK:
        now = time.monotonic()
        target = max(now, _DOMAIN_NEXT_REQUEST.get(host, now))
        _DOMAIN_NEXT_REQUEST[host] = target + interval
    wait_seconds = target - time.monotonic()
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def _retry_after_seconds(raw: str) -> float:
    text = str(raw or "").strip()
    if not text:
        return 0.0
    try:
        return max(0.0, float(text))
    except ValueError:
        try:
            dt = parsedate_to_datetime(text)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return max(0.0, (dt.astimezone(timezone.utc) - datetime.now(timezone.utc)).total_seconds())
        except Exception:
            return 0.0


def http_get_last_modified(
    url: str,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 15,
) -> str:
    req_headers = {"User-Agent": USER_AGENT, "Accept-Encoding": "identity"}
    if headers:
        req_headers.update(headers)

    try:
        req = Request(url, headers=req_headers, method="HEAD")
        with urlopen(req, timeout=timeout) as resp:
            return (resp.headers.get("Last-Modified") or "").strip()
    except Exception:
        pass

    curl_bin = shutil.which("curl")
    if not curl_bin:
        return ""

    cmd = [
        curl_bin,
        "--http1.1",
        "--location",
        "--silent",
        "--show-error",
        "--head",
        "--max-time",
        str(timeout),
        "--user-agent",
        req_headers.get("User-Agent", USER_AGENT),
    ]
    for key, val in req_headers.items():
        if key.lower() == "user-agent":
            continue
        cmd.extend(["-H", f"{key}: {val}"])
    cmd.append(url)

    proc = subprocess.run(cmd, capture_output=True, check=False)
    if proc.returncode != 0:
        return ""

    header_text = proc.stdout.decode("utf-8", errors="ignore")
    for line in header_text.splitlines():
        if ":" not in line:
            continue
        key, val = line.split(":", 1)
        if key.strip().lower() == "last-modified":
            return val.strip()
    return ""


def _curl_http_get(
    url: str,
    headers: dict[str, str],
    timeout: int,
    retries: int,
) -> bytes:
    curl_bin = shutil.which("curl")
    if not curl_bin:
        raise RuntimeError("curl_not_found")

    cmd = [
        curl_bin,
        "--http1.1",
        "--location",
        "--silent",
        "--show-error",
        "--fail",
        "--max-time",
        str(timeout),
        "--retry",
        str(max(retries - 1, 0)),
        "--retry-delay",
        "1",
        "--user-agent",
        headers.get("User-Agent", USER_AGENT),
    ]
    for key, val in headers.items():
        if key.lower() == "user-agent":
            continue
        cmd.extend(["-H", f"{key}: {val}"])
    cmd.extend(["--output", "-", url])

    proc = subprocess.run(cmd, capture_output=True, check=False)
    if proc.returncode != 0:
        err = proc.stderr.decode("utf-8", errors="ignore").strip() or f"curl_exit_{proc.returncode}"
        # 个别政务站点会在 OpenSSL 3 的默认椭圆曲线协商中返回 bad ecpoint。
        # 仅在精确命中该握手错误后，使用仍启用证书校验的 TLS 1.2/P-256
        # 兼容配置重试一次，避免影响其他请求或全局降低 TLS 安全设置。
        if "bad ecpoint" in err.lower():
            compat_cmd = list(cmd)
            output_index = compat_cmd.index("--output")
            compat_cmd[output_index:output_index] = [
                "--tlsv1.2",
                "--tls-max",
                "1.2",
                "--curves",
                "P-256",
            ]
            compat_proc = subprocess.run(compat_cmd, capture_output=True, check=False)
            if compat_proc.returncode == 0:
                return compat_proc.stdout
            compat_err = (
                compat_proc.stderr.decode("utf-8", errors="ignore").strip()
                or f"curl_exit_{compat_proc.returncode}"
            )
            raise RuntimeError(f"{err}; tls12_p256_retry_failed: {compat_err}")
        raise RuntimeError(err)
    return proc.stdout



def http_get_json(
    url: str,
    headers: Optional[dict[str, str]] = None,
    timeout: int = 25,
    retries: int = 3,
) -> dict[str, Any]:
    payload = http_get_bytes(url, headers=headers, timeout=timeout, retries=retries)
    return json.loads(payload.decode("utf-8", errors="ignore"))


def _safe_http_error_details(err: HTTPError) -> str:
    """提取可诊断且不包含请求正文、密钥的 HTTP 错误信息。"""
    status = int(getattr(err, "code", 0) or 0)
    reason = str(getattr(err, "reason", "") or "").strip()
    details: list[str] = []

    try:
        raw = err.read(4096).decode("utf-8", errors="ignore")
        payload = json.loads(raw)
    except Exception:
        payload = {}

    error_obj = payload.get("error", {}) if isinstance(payload, dict) else {}
    if isinstance(error_obj, dict):
        for key in ("type", "code", "message"):
            value = str(error_obj.get(key, "") or "").strip()
            if not value:
                continue
            value = re.sub(r"(?i)bearer\s+\S+", "Bearer [REDACTED]", value)
            value = re.sub(r"\bsk-[A-Za-z0-9_-]{8,}\b", "[REDACTED]", value)
            details.append(f"{key}={value[:300]}")

    summary = f"HTTP {status}" if status else "HTTP error"
    if reason:
        summary += f" {reason[:120]}"
    if details:
        summary += "; api_error=" + ", ".join(details)
    return summary


def http_post_json(
    url: str,
    body: dict[str, Any],
    headers: Optional[dict[str, str]] = None,
    timeout: int = 25,
    retries: int = 3,
) -> dict[str, Any]:
    req_headers = {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
    if headers:
        req_headers.update(headers)
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")

    last_err: Optional[Exception] = None
    for i in range(retries):
        req = Request(url, data=data, headers=req_headers, method="POST")
        try:
            with urlopen(req, timeout=timeout) as resp:
                text = resp.read().decode("utf-8", errors="ignore")
            return json.loads(text)
        except HTTPError as err:
            last_err = RuntimeError(_safe_http_error_details(err))
            time.sleep(1.2 * (i + 1))
        except Exception as err:
            last_err = err
            time.sleep(1.2 * (i + 1))
    raise RuntimeError(f"http_post_json failed for {url}: {last_err}")


def clean_text(text: str) -> str:
    s = re.sub(r"<[^>]+>", " ", text or "")
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def normalize_title(title: str) -> str:
    s = (title or "").lower().strip()
    s = re.sub(r"\(.*?\)", "", s)
    s = re.sub(r"[^a-z0-9\u4e00-\u9fff]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def normalize_url(url: str) -> str:
    try:
        p = urlparse((url or "").strip())
        if p.scheme not in {"http", "https"}:
            return ""
        query = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if not k.startswith("utm_")]
        query.sort()
        clean = p._replace(fragment="", query=urlencode(query, doseq=True))
        return urlunparse(clean)
    except Exception:
        return ""


def sha1_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8", errors="ignore")).hexdigest()


def detect_language(text: str) -> str:
    if re.search(r"[\u4e00-\u9fff]", text or ""):
        return "zh"
    return "en"


def to_dict_list(rows: list[Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows:
        if hasattr(row, "__dataclass_fields__"):
            out.append(asdict(row))
        elif isinstance(row, dict):
            out.append(row)
    return out


def tokenize(text: str) -> list[str]:
    if not text:
        return []
    low = text.lower()
    tokens = re.findall(r"[a-z0-9]+|[\u4e00-\u9fff]", low)
    return [t for t in tokens if len(t.strip()) > 0]


def cosine_similarity(vec_a: dict[str, float], vec_b: dict[str, float]) -> float:
    if not vec_a or not vec_b:
        return 0.0
    dot = sum(vec_a.get(k, 0.0) * vec_b.get(k, 0.0) for k in vec_a.keys())
    norm_a = math.sqrt(sum(v * v for v in vec_a.values()))
    norm_b = math.sqrt(sum(v * v for v in vec_b.values()))
    if norm_a == 0.0 or norm_b == 0.0:
        return 0.0
    return dot / (norm_a * norm_b)
