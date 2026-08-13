from __future__ import annotations

import argparse
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

from app.common import now_beijing, read_json, write_json
from app.source_config import PROFILE_NAMES


GOOD_AGENT_STATUSES = {"success", "success_empty", "partial_budget", "degraded"}


def resolve_runtime_profile(
    run_date: str,
    requested_profile: str,
    agent_report_file: Path,
    state_file: Path,
    rollback_days: int = 30,
) -> dict[str, Any]:
    state: dict[str, Any] = {
        "version": 1,
        "activated_at": "",
        "consecutive_failures": 0,
        "last_date": "",
        "fallback_active": False,
    }
    if state_file.exists():
        try:
            loaded = read_json(state_file)
            if isinstance(loaded, dict):
                state.update(loaded)
        except Exception:
            pass

    effective = requested_profile
    agent_status = "not_required"
    within_window = True
    if requested_profile == "agent_domestic":
        if not state.get("activated_at"):
            state["activated_at"] = run_date
        activated = date.fromisoformat(str(state["activated_at"]))
        current = date.fromisoformat(run_date)
        within_window = (current - activated).days < rollback_days
        if agent_report_file.exists():
            try:
                agent_status = str(read_json(agent_report_file).get("status", "failed"))
            except Exception:
                agent_status = "failed"
        else:
            agent_status = "missing"
        if agent_status in GOOD_AGENT_STATUSES:
            state["consecutive_failures"] = 0
            state["fallback_active"] = False
        else:
            state["consecutive_failures"] = int(state.get("consecutive_failures", 0)) + 1
            if int(state["consecutive_failures"]) >= 2 and within_window:
                effective = "legacy"
                state["fallback_active"] = True
    else:
        state["consecutive_failures"] = 0
        state["fallback_active"] = False

    state["last_date"] = run_date
    state["last_agent_status"] = agent_status
    state["effective_profile"] = effective
    state["rollback_window_open"] = within_window
    write_json(state_file, state)
    return dict(state)


def main() -> int:
    parser = argparse.ArgumentParser(description="根据 Agent 连续失败状态决定当日实际 profile")
    parser.add_argument("--date", default="")
    parser.add_argument("--requested-profile", choices=sorted(PROFILE_NAMES), default="")
    parser.add_argument("--agent-report", default="./.agent-handoff/{date}/agent_run_report.json")
    parser.add_argument("--state", default="./.state/agent_runtime.json")
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT", ""))
    args = parser.parse_args()
    run_date = args.date.strip() or now_beijing().strftime("%Y-%m-%d")
    requested = args.requested_profile.strip() or os.environ.get("ROBTAXI_PROFILE", "").strip() or "legacy"
    report_path = Path(args.agent_report.format(date=run_date)).expanduser().resolve()
    result = resolve_runtime_profile(
        run_date,
        requested,
        report_path,
        Path(args.state).expanduser().resolve(),
    )
    if args.github_output:
        with Path(args.github_output).open("a", encoding="utf-8") as handle:
            handle.write(f"effective_profile={result['effective_profile']}\n")
            handle.write(f"agent_status={result['last_agent_status']}\n")
            handle.write(f"fallback_active={str(bool(result['fallback_active'])).lower()}\n")
    print(
        f"[runtime_profile] requested={requested} effective={result['effective_profile']} "
        f"agent_status={result['last_agent_status']} failures={result['consecutive_failures']}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
