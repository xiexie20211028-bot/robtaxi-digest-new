from __future__ import annotations

import hashlib
import json
import math
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
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
    language: str
    fingerprint: str


@dataclass
class BriefItem:
    id: str
    source_id: str
    source_name: str
    region: str
    company_id: str
    title_zh: str
    summary_zh: str
    link: str
    published_at_utc: str
    tags: list[str]
    confidence: float


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


def parse_datetime(value: str) -> datetime:
    if not value:
        return datetime.utcnow().replace(tzinfo=timezone.utc)
    text = value.strip()
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %z",
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            dt = datetime.strptime(text, fmt)
            if dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
        except ValueError:
            continue

    try:
        dt = parsedate_to_datetime(text)
        if dt is not None:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc)
    except (TypeError, ValueError):
        pass

    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except ValueError:
        # 解析失败时回落到旧时间，避免把旧闻误判为“刚发布”。
        return UNPARSABLE_DT_FALLBACK


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
) -> bytes:
    redirect_codes = {301, 302, 303, 307, 308}
    max_redirects = 5
    last_err: Optional[Exception] = None
    req_headers = {"User-Agent": USER_AGENT}
    if headers:
        req_headers.update(headers)

    for i in range(retries):
        try:
            current_url = url
            for _ in range(max_redirects + 1):
                req = Request(current_url, headers=req_headers)
                with urlopen(req, timeout=timeout) as resp:
                    code = int(getattr(resp, "status", 200) or 200)
                    if code in redirect_codes:
                        location = (resp.headers.get("Location") or "").strip()
                        if not location:
                            return resp.read()
                        current_url = urljoin(current_url, location)
                        continue
                    return resp.read()
            raise RuntimeError(f"too many redirects: {url}")
        except HTTPError as err:
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
            last_err = err
            time.sleep(backoff * (i + 1))
        except (URLError, TimeoutError) as err:
            last_err = err
            time.sleep(backoff * (i + 1))
        except Exception as err:  # pragma: no cover
            last_err = err
            time.sleep(backoff * (i + 1))

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
            )
        )
        if should_fallback:
            try:
                return _curl_http_get(url, req_headers, timeout, retries)
            except Exception as curl_err:
                last_err = curl_err

    raise RuntimeError(f"http_get_bytes failed for {url}: {last_err}")


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
