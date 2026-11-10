"""LAW-02 在已归档的影子决策上核对同一组带标注样本。"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import tempfile

from .development_policy import digest, require
from .development_production import pages

WORKFLOW = "robtaxi-digest-shadow.yml"


def compare_decisions(before: list, after: list, labels: dict) -> dict:
    """在相同负例样本上比较误收条数；缺少分母时保留未知。"""
    def index(rows):
        decisions = {}
        for row in rows:
            key = row.get("candidate_id")
            if not isinstance(key, str) or not key or key in decisions or type(row.get("kept")) is not bool:
                return {}
            decisions[key] = (row.get("title"), row.get("source_id"), row.get("route"), row.get("sample_digest"), row["kept"])
        return decisions

    old, new = index(before), index(after)
    if (not old or not labels or old.keys() != new.keys() or labels.keys() != old.keys()
            or not all(type(v) is bool for v in labels.values())
            or any(old[k][:-1] != new[k][:-1] for k in old)):
        return {"passed": None, "reason": "样本、路线或独立标注不可比"}
    baseline = sum(old[k][-1] for k in labels if labels[k])
    candidate = sum(new[k][-1] for k in labels if labels[k])
    return {"passed": (candidate * 100 <= baseline * 110) if baseline else None,
            "reason": "原始噪声为零，增长率无分母" if not baseline else "同组标注样本误收条数",
            "baseline_noise": baseline, "candidate_noise": candidate, "samples": len(labels)}


def read_archive_rows(root, path):
    if (not path.is_file() or path.is_symlink() or not path.resolve().is_relative_to(root.resolve())
            or path.stat().st_size > 16 * 1024 * 1024):
        return None
    raw = path.read_bytes()
    try:
        rows = [json.loads(line) for line in raw.splitlines() if line.strip()]
    except (ValueError, UnicodeDecodeError):
        return None
    if not all(isinstance(row, dict) for row in rows):
        return None
    return rows, hashlib.sha256(raw).hexdigest()


def read_shadow_decisions(root):
    files = list(root.rglob("candidate_decisions.jsonl"))
    if files:
        parsed = read_archive_rows(root, files[0]) if len(files) == 1 else None
        if parsed:
            return parsed[0], {"file_sha256": parsed[1]}
        return None

    # PR86 的影子流程保存了全部候选、保留项和淘汰项，尚无逐项决策文件。
    candidates = list(root.glob("artifacts-shadow/canonical/*/canonical_items.jsonl"))
    if len(candidates) != 1:
        return None
    path = candidates[0]
    filtered = root / "artifacts-shadow" / "filtered" / path.parent.name
    originals, kept, dropped = [read_archive_rows(root, p) for p in (
        path, filtered / "filtered_items.jsonl", filtered / "dropped_items.jsonl")]
    if not originals or not kept or not dropped:
        return None
    decisions = {}
    for selected, batch in ((True, kept), (False, dropped)):
        for row in batch[0]:
            key = row.get("id")
            if not isinstance(key, str) or not key or key in decisions:
                return None
            decisions[key] = (row.get("title"), row.get("source_id"), selected)
    rows = []
    for sample in originals[0]:
        key = sample.get("id")
        if not isinstance(key, str):
            return None
        decision = decisions.pop(key, None)
        if not decision or decision[:2] != (sample.get("title"), sample.get("source_id")):
            return None
        rows.append({"candidate_id": key, "title": decision[0], "source_id": decision[1],
                     "route": "optimized", "kept": decision[2], "sample_digest": digest(sample)})
    if decisions or not rows:
        return None
    return rows, {"format": "legacy_filter", "component_sha256": {
        "canonical": originals[1], "filtered": kept[1], "dropped": dropped[1]}}


def archived_decisions(client, number, expected_sha, workflow_id):
    prefix = f"repos/{client.repo}"
    run = client.api(f"{prefix}/actions/runs/{number}")
    if (run.get("id") != number or run.get("workflow_id") != workflow_id or run.get("event") != "schedule"
            or run.get("head_branch") != "main" or run.get("head_sha") != expected_sha
            or run.get("conclusion") != "success"):
        return None
    name = f"robotaxi-l3-l4-shadow-{number}-{run['run_attempt']}"
    artifacts = pages(client, f"{prefix}/actions/runs/{number}/artifacts", "artifacts")
    matches = [a for a in artifacts if a.get("name") == name and not a.get("expired")]
    if len(matches) != 1 or not 0 < matches[0].get("size_in_bytes", 0) <= 64 * 1024 * 1024:
        return None
    with tempfile.TemporaryDirectory() as directory:
        client.gh_read("run", "download", str(number), "--repo", client.repo, "--name", name, "--dir", directory)
        result = read_shadow_decisions(Path(directory))
        if not result:
            return None
        rows, evidence = result
        return rows, {"run": number, "attempt": run["run_attempt"], "sha": expected_sha,
                      "artifact_id": matches[0]["id"], **evidence}


def shadow_evidence(client, facts):
    pair = facts["production"].get("shadow_comparison") or {}
    labels_path = Path(__file__).resolve().parents[1] / ".github/codex/evidence/law-02-shadow-labels.json"
    if not pair or not labels_path.is_file():
        return {"passed": None, "reason": "缺修改前后影子运行或经复核的同组样本标注"}
    labels = json.loads(labels_path.read_text())
    if not isinstance(labels, dict) or digest(labels) != pair.get("labels_digest"):
        return {"passed": None, "reason": "样本标注与交付登记版本不符"}
    workflow = client.api(f"repos/{client.repo}/actions/workflows/{WORKFLOW}")
    require(workflow.get("path") == f".github/workflows/{WORKFLOW}", "影子工作流身份不符")
    merge = client.api(f"repos/{client.repo}/commits/{facts['merge_sha']}")
    sides = []
    for field, sha in (("baseline_run", merge["parents"][0]["sha"]), ("candidate_run", facts["merge_sha"])):
        run = pair.get(field)
        side = archived_decisions(client, run, sha, workflow["id"]) if type(run) is int and run > 0 else None
        if side is None:
            return {"passed": None, "reason": "指定影子版本或决策归档不可用", "side": field, "run": run}
        sides.append(side)
    return {**compare_decisions(sides[0][0], sides[1][0], labels), "labels_digest": digest(labels),
            "evidence": [side[1] for side in sides]}
