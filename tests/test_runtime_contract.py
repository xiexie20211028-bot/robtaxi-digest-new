from __future__ import annotations

import re
from datetime import datetime, timedelta
from pathlib import Path

from app.report import default_report


ROOT = Path(__file__).parents[1]


def test_repository_declares_python_311_only() -> None:
    assert (ROOT / ".python-version").read_text(encoding="utf-8").strip() == "3.11"
    pyproject = (ROOT / "pyproject.toml").read_text(encoding="utf-8")
    assert 'requires-python = ">=3.11,<3.12"' in pyproject


def test_all_github_workflows_use_python_311() -> None:
    versions: list[str] = []
    for workflow in (ROOT / ".github/workflows").glob("*.yml"):
        versions.extend(re.findall(r'python-version:\s*["\']?([^"\'\s]+)', workflow.read_text(encoding="utf-8")))
    assert versions
    assert set(versions) == {"3.11"}


def test_application_does_not_use_naive_utcnow() -> None:
    offenders: list[str] = []
    for root_name in ("app", "scripts"):
        for path in (ROOT / root_name).rglob("*.py"):
            if "datetime.utcnow(" in path.read_text(encoding="utf-8"):
                offenders.append(str(path.relative_to(ROOT)))
    assert offenders == []


def test_report_timestamp_is_timezone_aware_utc() -> None:
    generated_at = datetime.fromisoformat(default_report()["generated_at_utc"])
    assert generated_at.tzinfo is not None
    assert generated_at.utcoffset() == timedelta(0)


def test_local_scheduled_scripts_use_the_python_311_virtualenv() -> None:
    installer = (ROOT / "scripts/install_launchd.sh").read_text(encoding="utf-8")
    runner = (ROOT / "scripts/run_if_due.sh").read_text(encoding="utf-8")
    health = (ROOT / "scripts/test_sources_health.sh").read_text(encoding="utf-8")
    assert 'VENV_DIR="$APP_DIR/.venv"' in installer
    assert 'ROBTAXI_PYTHON_BIN="${ROBTAXI_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"' in runner
    assert 'ROBTAXI_PYTHON_BIN="${ROBTAXI_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"' in health
    assert 'sys.version_info[:2] == (3, 11)' in installer
    assert 'sys.version_info[:2] == (3, 11)' in runner
    assert 'sys.version_info[:2] == (3, 11)' in health
