from __future__ import annotations

import sys
import re
from pathlib import Path

from scripts import robtaxi_digest


EXPECTED_BUILD_STAGES = [
    "app.fetch",
    "app.industry_agent.import_events",
    "app.parse",
    "app.filter_relevance",
    "app.enrich",
    "app.summarize",
    "app.editorial_digest",
    "app.render",
]


def _stage_names(commands: list[list[str]]) -> list[str]:
    return [command[2] for command in commands]


def _production_build_stages() -> list[str]:
    workflow = (Path(__file__).parents[1] / ".github/workflows/robtaxi-digest-pages.yml").read_text(encoding="utf-8")
    block = workflow.split("- name: Run pipeline stages", 1)[1].split("- name: Save 35-day pipeline state", 1)[0]
    return re.findall(r"python -m (app(?:\.[a-z_]+)+)", block)


def test_wrapper_runs_editorial_digest_before_render(tmp_path, monkeypatch) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(robtaxi_digest, "run", lambda command: commands.append(command))
    monkeypatch.setattr(robtaxi_digest, "_update_seen_history", lambda *_args: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "robtaxi_digest.py",
            "--date",
            "2026-08-14",
            "--sources",
            str(tmp_path / "sources.json"),
            "--brief",
            str(tmp_path / "brief"),
            "--digest",
            str(tmp_path / "digest"),
            "--output",
            str(tmp_path / "site" / "index.html"),
        ],
    )

    assert robtaxi_digest.main() == 0

    stages = _stage_names(commands)
    assert stages[-3:] == ["app.summarize", "app.editorial_digest", "app.render"]
    editorial_command = commands[stages.index("app.editorial_digest")]
    assert editorial_command[editorial_command.index("--in") + 1] == str(tmp_path / "brief")
    assert editorial_command[editorial_command.index("--out") + 1] == str(tmp_path / "digest")


def test_wrapper_dry_run_still_builds_editorial_digest(tmp_path, monkeypatch) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(robtaxi_digest, "run", lambda command: commands.append(command))
    monkeypatch.setattr(robtaxi_digest, "_update_seen_history", lambda *_args: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "robtaxi_digest.py",
            "--date",
            "2026-08-14",
            "--sources",
            str(tmp_path / "sources.json"),
            "--dry-run",
        ],
    )

    assert robtaxi_digest.main() == 0

    stages = _stage_names(commands)
    assert stages[-1] == "app.editorial_digest"
    assert "app.render" not in stages


def test_local_wrapper_and_ci_use_the_same_build_stages(tmp_path, monkeypatch) -> None:
    commands: list[list[str]] = []
    monkeypatch.setattr(robtaxi_digest, "run", lambda command: commands.append(command))
    monkeypatch.setattr(robtaxi_digest, "_update_seen_history", lambda *_args: None)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "robtaxi_digest.py",
            "--date",
            "2026-08-14",
            "--sources",
            str(tmp_path / "sources.json"),
        ],
    )

    assert robtaxi_digest.main() == 0
    assert _stage_names(commands) == EXPECTED_BUILD_STAGES
    assert _production_build_stages() == EXPECTED_BUILD_STAGES
