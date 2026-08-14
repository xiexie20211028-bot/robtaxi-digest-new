from __future__ import annotations

import argparse
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import quote

from .common import http_get_bytes


CHANNELS = ("feishu", "wecom")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_REPOSITORY_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")


def artifact_name(date_text: str, channel: str) -> str:
    if not _DATE_PATTERN.fullmatch(date_text):
        raise ValueError(f"invalid date: {date_text}")
    if channel not in CHANNELS:
        raise ValueError(f"unsupported channel: {channel}")
    return f"robtaxi-notify-lock-{date_text}-{channel}"


def has_active_artifact(payload: dict[str, Any], expected_name: str) -> bool:
    artifacts = payload.get("artifacts", [])
    if not isinstance(artifacts, list):
        raise ValueError("GitHub artifacts response has no artifacts list")
    return any(
        isinstance(item, dict)
        and str(item.get("name", "")) == expected_name
        and not bool(item.get("expired", False))
        for item in artifacts
    )


def fetch_lock(repository: str, token: str, date_text: str, channel: str) -> bool:
    if not _REPOSITORY_PATTERN.fullmatch(repository):
        raise ValueError(f"invalid repository: {repository}")
    if not token.strip():
        raise ValueError("GITHUB_TOKEN is empty")
    expected_name = artifact_name(date_text, channel)
    endpoint = (
        f"https://api.github.com/repos/{repository}/actions/artifacts"
        f"?name={quote(expected_name)}&per_page=100"
    )
    body = http_get_bytes(
        endpoint,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token.strip()}",
            "X-GitHub-Api-Version": "2026-03-10",
        },
        timeout=20,
        retries=3,
    )
    payload = json.loads(body.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("GitHub artifacts response is not an object")
    return has_active_artifact(payload, expected_name)


def _write_github_output(path: str, values: dict[str, str]) -> None:
    if not path:
        return
    with Path(path).open("a", encoding="utf-8") as handle:
        for key, value in values.items():
            handle.write(f"{key}={value}\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Check durable per-channel notification lock artifacts")
    parser.add_argument("--date", required=True, help="Beijing run date YYYY-MM-DD")
    parser.add_argument("--repository", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--token", default=os.environ.get("GITHUB_TOKEN", ""))
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args()

    values: dict[str, str] = {}
    for channel in CHANNELS:
        locked = fetch_lock(args.repository, args.token, args.date, channel)
        values[f"{channel}_locked"] = str(locked).lower()
        values[f"{channel}_artifact"] = artifact_name(args.date, channel)
    _write_github_output(args.github_output, values)
    print(
        "[notification_lock] "
        + " ".join(f"{channel}={values[f'{channel}_locked']}" for channel in CHANNELS)
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
