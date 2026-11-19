"""用 PR86 的固定基线与原交付或剩余修复重放相同的已归档输入。"""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from app.development_policy import digest
from app.development_shadow_noise import read_filter_partition
from app.law02_replay_evidence import REPLAY_PATHS

BASE = "ea1f338910cb16f69c13eae6248c0c29ba499387"
MERGE = "dad99a497e1e6376f1ef7315a60f996cec685fd0"
EVIDENCE = ROOT / ".github/codex/evidence"


def historical_checkout(sha, target):
    # 修复候选只能由本地操作者明确指定，归档和登记记录不能提供可执行版本。
    payload = subprocess.check_output(
        ["git", "archive", sha, "--", *REPLAY_PATHS], cwd=ROOT
    )
    target.mkdir()
    with tarfile.open(fileobj=io.BytesIO(payload)) as archive:
        archive.extractall(target, filter="data")
    return {name: hashlib.sha256((target / name).read_bytes()).hexdigest()
            for name in ("sources.json", "requirements.txt")}


def replay(checkout, work, source_bytes, date):
    canonical = work / "artifacts-shadow/canonical"
    filtered = work / "artifacts-shadow/filtered"
    (canonical / date).mkdir(parents=True)
    (canonical / date / "canonical_items.jsonl").write_bytes(source_bytes)
    command = [sys.executable, "-B", "-m", "app.filter_relevance", "--date", date,
               "--profile", "optimized", "--sources", str(checkout / "sources.json"),
               "--in", str(canonical), "--out", str(filtered), "--report", str(work / "reports"),
               "--health-history", str(work / "history.json")]
    env = {k: os.environ[k] for k in ("PATH", "LANG", "LC_ALL", "SYSTEMROOT") if k in os.environ}
    subprocess.run(command, cwd=checkout, env=env, check=True, timeout=90, stdout=subprocess.DEVNULL)
    parsed = read_filter_partition(work)
    if parsed is None:
        raise ValueError(f"{date} 保留和淘汰记录不能完整还原样本")
    return parsed


def repair_tree(ref):
    output = subprocess.check_output(["git", "ls-tree", ref, "--", *REPLAY_PATHS], cwd=ROOT, text=True)
    tree = {}
    for entry in output.splitlines():
        metadata, path = entry.split("\t", 1)
        mode, _, oid = metadata.split()
        tree[path] = {"mode": mode, "sha": oid}
    return tree


def comparisons(input_root, repair_ref=None):
    inputs = json.loads((EVIDENCE / "law-02-inputs.json").read_text())
    candidate = MERGE
    if repair_ref is not None:
        candidate = subprocess.check_output(
            ["git", "rev-parse", "--verify", "--end-of-options", f"{repair_ref}^{{commit}}"], cwd=ROOT, text=True
        ).strip()
        subprocess.run(["git", "merge-base", "--is-ancestor", MERGE, candidate], cwd=ROOT, check=True)
    batches = []
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        configs = {sha: historical_checkout(sha, root / sha) for sha in (BASE, candidate)}
        for meta in inputs:
            run, day = str(meta["run"]), meta["date"]
            source = input_root / run / "artifacts-shadow/canonical" / day / "canonical_items.jsonl"
            data = source.read_bytes()
            if hashlib.sha256(data).hexdigest() != meta["input_sha256"]:
                raise ValueError(f"{day} 输入与已审核的真实归档不一致")
            sides = [replay(root / sha, root / run / sha, data, day) for sha in (BASE, candidate)]
            old, new = [{r["candidate_id"]: r for r in rows} for rows, _ in sides]
            if (len(old) != meta["candidates"] or old.keys() != new.keys()
                    or any(old[k]["sample_digest"] != new[k]["sample_digest"] for k in old)):
                raise ValueError(f"{day} 两个版本的样本身份不同")
            retained = [{"key": f"{run}:{meta['attempt']}:optimized:{k}",
                         "sample_digest": old[k]["sample_digest"],
                         "before": old[k]["kept"], "after": new[k]["kept"]}
                        for k in sorted(old) if old[k]["kept"] or new[k]["kept"]]
            # 决策摘要覆盖全量候选，人工只需评审至少一侧保留的项。
            batches.append({"run": meta["run"], "input_sha256": meta["input_sha256"],
                            "count": len(old), "decision_digests": [digest(rows) for rows, _ in sides],
                            "retained": retained})
            if repair_ref is not None:
                batches[-1]["candidate_retained_digest"] = digest({
                    key: row["sample_digest"] for key, row in new.items() if row["kept"]
                })
    environment = {"python": sys.version.split()[0], "packages": {
        p: importlib.metadata.version(p) for p in ("beautifulsoup4", "pypdf")}}
    return {"schema": "law-02-paired-replay-v3" if repair_ref is not None else "law-02-paired-replay-v2",
            "issue": 73, "pr": 86, "baseline": BASE,
            "merge_sha": MERGE, "profile": "optimized", "inputs_digest": digest(inputs),
            "configs": configs, "environment": environment, "batches": batches,
            **({"repair_sha": candidate, "repair_tree": repair_tree(candidate)} if repair_ref is not None else {})}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-root", type=Path, required=True, help="每个子目录为按 run ID 命名的已解压影子归档")
    parser.add_argument("--repair-ref", help="已提交的 #73 剩余修复候选，省略则验证 PR86 原交付")
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    args.out.write_text(json.dumps(comparisons(args.input_root, args.repair_ref), ensure_ascii=False, indent=2) + "\n")
