#!/usr/bin/env python3
"""使用同一份可信黄金输入、回放器及测试检查基线与候选，不访问付费模型。"""
from __future__ import annotations

import argparse
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from app.development_policy import DevelopmentError, digest, require
from app.development_runtime import run


def export_revision(revision: str, target: Path) -> None:
    data = subprocess.run(["git", "archive", revision], cwd=ROOT, capture_output=True, check=True).stdout
    with tarfile.open(fileobj=io.BytesIO(data)) as archive:
        # 不允许归档链接走出临时目录；这里从不执行归档内的 hooks。
        require(all(not m.issym() and not m.islnk() for m in archive.getmembers()), "回放归档含链接")
        archive.extractall(target, filter="data")


def compare_reports(baseline: dict, candidate: dict) -> None:
    require(baseline.get("acceptance_met") is True, "主分支黄金基线本身未通过，不能把失败当新标准")
    require(candidate.get("event_id") == baseline.get("event_id"), "回放输入不一致")
    require(candidate.get("acceptance_met") is True and candidate.get("negative_controls_passed") is True, "候选存在重要事件遗漏或错误纳入")
    require(candidate.get("independent_discoveries", 0) >= baseline.get("independent_discoveries", 0), "独立发现数量下降")
    for route, result in baseline["results"].items():
        require(not result["kept"] or candidate["results"][route]["kept"], f"{route} 黄金事件漏报增加")
    require(candidate.get("negative_controls") == baseline.get("negative_controls"), "固定负例结果发生回归")


def replay(base: str, head: str) -> dict:
    with tempfile.TemporaryDirectory(prefix="robtaxi-business-replay-") as directory:
        baseline, candidate = Path(directory) / "base", Path(directory) / "head"
        export_revision(base, baseline)
        export_revision(head, candidate)
        harness = "scripts/replay_golden_event.py"
        fixtures = sorted((baseline / "tests/fixtures/golden_events").glob("*.json"))
        require(bool(fixtures), "可信主分支没有黄金事件输入")
        # 覆盖候选回放器及关键验收文件，候选不能通过改测试/阈值自证。
        trusted = [baseline / harness, *fixtures,
                   baseline / "tests/fixtures/golden_scope.json",
                   baseline / "tests/test_taxonomy_golden.py", baseline / "tests/test_datetime_parsing.py",
                   baseline / "tests/test_golden_event_replay.py"]
        for source in trusted:
            require(source.is_file(), f"缺少可信测试输入：{source.name}")
            dest = candidate / source.relative_to(baseline)
            dest.parent.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(source.read_bytes())
        results = []
        # 清除所有外部凭据；固定回放器自带离线 PageReader，不把 LLM 不确定输出当回归真值。
        env = {"PATH": os.environ.get("PATH", ""), "PYTHONDONTWRITEBYTECODE": "1", "PYTEST_DISABLE_PLUGIN_AUTOLOAD": "1"}
        for source in fixtures:
            relative = str(source.relative_to(baseline))
            reports = []
            for root in (baseline, candidate):
                result = run([sys.executable, harness, "--event", relative], cwd=root, env=env, timeout=120)
                reports.append(json.loads(result))
            compare_reports(*reports)
            results.append({"fixture": relative, "input_sha256": digest(json.loads(source.read_text())), "base": reports[0], "head": reports[1]})
        for root in (baseline, candidate):
            run([sys.executable, "-m", "pytest", "-q", "--noconftest", "-c", "/dev/null",
                 "tests/test_taxonomy_golden.py", "tests/test_datetime_parsing.py", "tests/test_golden_event_replay.py"],
                cwd=root, env={**env, "PYTHONPATH": str(root)}, timeout=300)
        return {"schema_version": "robtaxi-development-replay-v1", "base_sha": base, "head_sha": head,
                "passed": True, "external_model_outputs": "not_used", "events": results,
                "coverage_limit": "固定黄金事件、分类负例、日期与证据路径；不能替代每项任务专属验收及真实生产验证"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True)
    parser.add_argument("--head", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    try:
        result = replay(args.base, args.head)
        path = Path(args.out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
        print("[development-replay] PASS: 可信固定输入的基线/候选均通过")
        return 0
    except (DevelopmentError, OSError, ValueError, subprocess.SubprocessError) as exc:
        print(f"[development-replay] STOP: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
