"""有限恢复与脱敏诊断；缓存不授予权限，正式租约仍来自 GitHub。"""
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.development_policy import DevelopmentError, digest, require, timestamp


class RequestFailure(DevelopmentError):
    def __init__(self, argv: list[str], stderr: str, code: int | None, elapsed: float,
                 *, timed_out: bool = False):
        text = stderr.lower()
        status = re.search(r"(?:http[/\d.]*\s+|http\s*[:=]?\s*)([45]\d\d)", text)
        self.status = int(status[1]) if status else None
        if timed_out or "timeout" in text or "timed out" in text:
            self.category = "timeout"
        elif "rate limit" in text or self.status == 429:
            self.category = "rate_limit"
        elif self.status == 401 or "bad credentials" in text or "not logged" in text:
            self.category = "authentication"
        elif self.status == 403 or "permission" in text or "operation not permitted" in text:
            self.category = "permission"
        elif self.status and self.status >= 500:
            self.category = "service"
        elif any(s in text for s in ("eof", "connection", "connecting", "network", "no such host", "tls handshake", "resolve host")):
            self.category = "connection"
        else:
            self.category = "request_rejected" if self.status else "unknown"
        retry = re.search(r"retry-after:\s*(\d+)", text)
        reset = re.search(r"x-ratelimit-reset:\s*(\d+)", text)
        self.retry_after = float(retry[1]) if retry else max(0, int(reset[1]) - time.time()) if reset else None
        request_id = re.search(r"x-github-request-id:\s*([a-zA-Z0-9:.-]{1,100})", stderr, re.I)
        # 不保存自由文本：stderr可能包含请求正文、Token、代理密码和签名URL。
        endpoint = "graphql" if "graphql" in argv else "rest" if "api" in argv else "/".join(argv[1:3])
        for arg in argv:
            match = re.fullmatch(r"repos/[\w.-]+/[\w.-]+/([\w./?-]+(?:=[\w.-]+)?)", arg)
            if match:
                endpoint = "repos/:repo/" + match[1].split("?")[0]
                break
        self.details = {"category": self.category, "endpoint": endpoint,
                        "exit_code": code, "http_status": self.status,
                        "request_id": request_id[1] if request_id else None,
                        "elapsed_seconds": round(elapsed, 3), "error_fingerprint": digest(stderr)}
        self.transient = self.category in {"connection", "timeout", "service", "rate_limit"}
        super().__init__(f"GitHub请求失败：{self.category}；接口={endpoint}；HTTP={self.status or '未知'}")


def diagnostic(directory: Path, failure: RequestFailure, attempt: int, step: str) -> None:
    directory.mkdir(parents=True, exist_ok=True)
    now = datetime.now(timezone.utc)
    # 仅清理本模块固定命名的过期诊断，不接触其他文件。
    for path in directory.glob("requests-????-??-??.jsonl"):
        if path.is_symlink():
            continue
        try:
            day = datetime.strptime(path.stem[9:], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        except ValueError:
            continue
        if day < now - timedelta(days=14):
            path.unlink()
    path = directory / f"requests-{now:%Y-%m-%d}.jsonl"
    require(not path.is_symlink(), "诊断文件不能是符号链接")
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({**failure.details, "at": now.isoformat(), "attempt": attempt,
                                 "step": step}, ensure_ascii=False) + "\n")


class RecoveryBudget:
    """每次运行跨命令共享等待预算，正式截止时间由run_started/租约约束。"""
    def __init__(self, directory: Path, run_id: str, deadline: str, policy: dict):
        self.directory = directory
        self.path = directory / f"recovery-{digest(run_id)[:20]}.json"
        self.deadline = timestamp(deadline)
        self.limit = policy.get("network_recovery_seconds", 600)
        self.delays = policy.get("network_recovery_delays", [30, 90, 180])
        self.state = {"spent": 0, "rounds": 0}
        if self.path.exists():
            self.state = json.loads(self.path.read_text())
        require(0 <= self.state["spent"] <= self.limit and 0 <= self.state["rounds"] <= len(self.delays),
                "恢复预算缓存无效")

    def remaining(self) -> float:
        return (self.deadline - datetime.now(timezone.utc)).total_seconds()

    def check(self) -> None:
        require(self.remaining() > 0, "90分钟运行预算已到期；保存本地检查点，下次恢复")

    def save(self) -> None:
        self.directory.mkdir(parents=True, exist_ok=True)
        require(not self.path.is_symlink(), "恢复预算缓存不能是符号链接")
        self.path.write_text(json.dumps(self.state), encoding="utf-8")

    def wait(self, failure: RequestFailure) -> bool:
        index = self.state["rounds"]
        if not failure.transient or index >= len(self.delays):
            return False
        delay = failure.retry_after if failure.category == "rate_limit" else self.delays[index]
        if delay is None or delay + 30 > min(self.remaining(), self.limit - self.state["spent"]):
            return False
        self.state["rounds"] += 1
        # 先预扣，进程中断不能无成本重置等待预算。
        self.state["spent"] += delay
        self.save()
        end = time.monotonic() + delay
        while time.monotonic() < end:
            self.check()
            time.sleep(min(30, end - time.monotonic()))
        return True

    def charge(self, elapsed: float) -> None:
        self.state["spent"] = min(self.limit, self.state["spent"] + elapsed)
        self.save()


def latest_checks(checks: list[dict], required: list[str]) -> str:
    """每个必要名称只看可信Actions最新结果，旧绿不能遮住新pending。"""
    pending = False
    for name in required:
        rows = [c for c in checks if c.get("name") == name and c.get("app", {}).get("slug") == "github-actions"]
        if not rows:
            pending = True
            continue
        # 新排队检查尚无started_at；不能因此落在旧成功之后。
        current = max(rows, key=lambda c: (int(c.get("id", 0)), c.get("started_at") or ""))
        if current.get("status") != "completed":
            pending = True
        elif current.get("conclusion") != "success":
            return "failed"
    return "pending" if pending else "success"
