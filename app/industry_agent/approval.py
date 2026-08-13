from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any


COMMAND_RE = re.compile(r"^/agent-review\s+approve\s+(?P<review_id>[a-zA-Z0-9_-]+)(?:\s+reject=(?P<rejects>[a-zA-Z0-9_,.-]+))?\s*$")
MARKER_RE = re.compile(r"<!--\s*agent-review\s+(\{.*?\})\s*-->", re.S)


def validate_approval(comment: str, issue_body: str, max_overturn_rate: float = 0.10) -> dict[str, Any]:
    command = COMMAND_RE.match(str(comment or "").strip())
    marker = MARKER_RE.search(str(issue_body or ""))
    if not command:
        return {"approved": False, "reason": "invalid_command"}
    if not marker:
        return {"approved": False, "reason": "missing_review_marker"}
    try:
        metadata = json.loads(marker.group(1))
    except Exception:
        return {"approved": False, "reason": "invalid_review_marker"}
    if str(command.group("review_id")) != str(metadata.get("review_id", "")):
        return {"approved": False, "reason": "review_id_mismatch"}
    if not bool(metadata.get("ready_for_manual_approval", False)):
        return {"approved": False, "reason": "automatic_gate_not_ready"}
    rejected = [value for value in str(command.group("rejects") or "").split(",") if value]
    sample_count = int(metadata.get("manual_sample_count", 0) or 0)
    overturn_rate = len(rejected) / sample_count if sample_count else 0.0
    if overturn_rate > max_overturn_rate:
        return {
            "approved": False,
            "reason": "manual_overturn_rate_too_high",
            "overturn_rate": round(overturn_rate, 4),
        }
    return {
        "approved": True,
        "reason": "approved",
        "review_id": str(metadata.get("review_id", "")),
        "rejected_event_ids": rejected,
        "overturn_rate": round(overturn_rate, 4),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="校验 /agent-review approve 审批命令")
    parser.add_argument("--comment", required=True)
    parser.add_argument("--issue-body-file", required=True)
    parser.add_argument("--github-output", default="")
    args = parser.parse_args()
    issue_body = Path(args.issue_body_file).read_text(encoding="utf-8")
    result = validate_approval(args.comment, issue_body)
    if args.github_output:
        with Path(args.github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"approved={str(bool(result.get('approved'))).lower()}\n")
            handle.write(f"reason={result.get('reason', '')}\n")
            handle.write(f"review_id={result.get('review_id', '')}\n")
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result.get("approved") else 2


if __name__ == "__main__":
    raise SystemExit(main())
