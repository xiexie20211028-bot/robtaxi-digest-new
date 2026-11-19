import copy
import json
from pathlib import Path

import pytest

from app.development_acceptance import binding_for, evaluate_offline
from app.development_policy import DevelopmentError, digest
from app.development_shadow_noise import shadow_evidence
from app.law02_replay_evidence import MERGE, REPLAY_PATHS, score_labels, verify_comparison


def label(row, verdict):
    # 仅供故障注入测试；正式标注必须从经评审的主分支证据文件读取。
    return {"sample_digest": row["sample_digest"], "source_url": "https://example.org/article",
            "source_content_sha256": "a" * 64, "reason": "测试样本原文的独立判断", "verdict": verdict}


def test_paired_adoption_requires_bound_delivery_and_review(tmp_path):
    root = Path(__file__).resolve().parents[1] / ".github/codex/evidence"
    for name in ("law-02-inputs.json", "law-02-replay.json"):
        (tmp_path / name).write_bytes((root / name).read_bytes())
    replay = json.loads((root / "law-02-replay.json").read_text())
    pair = {"mode": "paired_offline", "replay_digest": digest(replay)}
    facts = {"issue": 73, "pr": 86, "merge_sha": MERGE}
    assert verify_comparison(tmp_path, facts, pair)["passed"] is None
    rows = {r["key"]: r for batch in replay["batches"] for r in batch["retained"]}
    assert len(rows) == 17
    review = {"schema": "law-02-independent-labels-v1", "replay_digest": digest(replay),
              "reviewer": "fixture", "labels": {k: label(v, "relevant") for k, v in rows.items()}}
    path = tmp_path / "law-02-paired-labels.json"
    path.write_text(json.dumps(review))
    pair["labels_digest"] = digest(review)
    result = verify_comparison(tmp_path, facts, pair)
    assert result["passed"] is True and result["total_samples"] == 2193 and result["growth_percent"] is None
    assert verify_comparison(tmp_path, {**facts, "pr": 163}, pair)["passed"] is None
    assert verify_comparison(tmp_path, {**facts, "issue": 83}, pair)["passed"] is None
    added = next(k for k, v in rows.items() if not v["before"])
    review["labels"][added]["verdict"] = "noise"
    path.write_text(json.dumps(review))
    assert verify_comparison(tmp_path, facts, pair)["passed"] is None
    result = verify_comparison(tmp_path, facts, {**pair, "labels_digest": digest(review)})
    assert result["passed"] is False and (result["baseline_noise"], result["candidate_noise"]) == (0, 1)
    assert evaluate_offline(73, version=2, shadow=result)["acceptance_passed"] is False


def test_union_only_requires_every_retained_sample_and_rejects_incomplete_source_evidence():
    row = {"sample_digest": "a" * 64, "before": True, "after": True}
    rows = {str(i): dict(row) for i in range(11)}
    rows["10"]["before"] = False
    labels = {k: label(r, "noise") for k, r in rows.items()}
    assert score_labels(rows, labels)["passed"] is True  # 10 -> 11
    rows["9"]["before"] = False
    assert score_labels(rows, labels)["passed"] is False  # 9 -> 11
    incomplete = copy.deepcopy(labels)
    incomplete["9"].pop("source_content_sha256")
    assert score_labels(rows, incomplete)["unresolved"] == ["9"]
    del incomplete["9"]
    assert score_labels(rows, incomplete)["passed"] is None


def test_versions_are_task_specific_and_legacy_compatible():
    assert binding_for(73)["verifier_version"] == 1
    assert binding_for(73, 2)["verifier_version"] == 2
    with pytest.raises(DevelopmentError):
        binding_for(83, 2)


def test_archived_independent_review_reproduces_the_historical_regression():
    root = Path(__file__).resolve().parents[1] / ".github/codex/evidence"
    pair = {
        "mode": "paired_offline",
        "replay_digest": "79b9f57d57886cc963dd3475fbec0764a69c137bb8afae269cf15f58f1fe84c2",
        "labels_digest": "bcdc13857f0a778b15d1ada09adc6891821e0ec6162b4f5d9c8ed3b9ab87ef28",
    }
    comparison = verify_comparison(root, {"issue": 73, "pr": 86, "merge_sha": MERGE}, pair)
    assert comparison["total_samples"] == 2193 and comparison["reviewed_samples"] == 17
    assert comparison["baseline_noise"] == 0 and comparison["candidate_noise"] == 1
    assert comparison["growth_percent"] is None
    verification = evaluate_offline(73, version=2, shadow=comparison)
    assert verification["checks"] == {
        "independent_routes": True,
        "negative_controls": True,
        "shadow_noise_growth_at_most_10_percent": False,
    }
    assert verification["acceptance_passed"] is False


def test_repair_requires_same_baseline_and_exact_merged_runtime_tree(tmp_path):
    root = Path(__file__).resolve().parents[1] / ".github/codex/evidence"
    for name in ("law-02-inputs.json", "law-02-replay.json", "law-02-paired-labels.json"):
        (tmp_path / name).write_bytes((root / name).read_bytes())
    replay = json.loads((root / "law-02-replay.json").read_text())
    labels = json.loads((root / "law-02-paired-labels.json").read_text())
    tree = {name: {"mode": "040000" if name == "app" else "100644", "sha": "a" * 40} for name in REPLAY_PATHS}
    replay.update(schema="law-02-paired-replay-v3", repair_sha="b" * 40, repair_tree=tree)
    for batch in replay["batches"]:
        batch["retained"] = [r for r in batch["retained"] if r["before"]]
        batch["candidate_retained_digest"] = digest({
            r["key"].rsplit(":", 1)[-1]: r["sample_digest"] for r in batch["retained"] if r["after"]
        })
    path = tmp_path / "law-02-repair-replay.json"
    path.write_text(json.dumps(replay))
    pair = {"mode": "paired_offline", "replay_digest": digest(replay), "labels_digest": digest(labels),
            "repair_pr": 188, "repair_sha": replay["repair_sha"]}
    facts = {"issue": 73, "pr": 86, "merge_sha": MERGE}
    result = verify_comparison(tmp_path, facts, pair, repair_tree=tree)
    assert (result["baseline_noise"], result["candidate_noise"], result["reviewed_samples"]) == (0, 0, 16)
    assert evaluate_offline(73, version=3, shadow=result)["acceptance_passed"] is True
    wrong_tree = {**tree, "app": {"mode": "040000", "sha": "c" * 40}}
    assert verify_comparison(tmp_path, facts, pair, repair_tree=wrong_tree)["passed"] is None
    batch = replay["batches"][0]
    extra_key = batch["retained"][0]["key"].rsplit(":", 1)[0] + ":new"
    extra_row = {"key": extra_key, "sample_digest": "f" * 64, "before": False, "after": True}
    batch["retained"].append(extra_row)
    path.write_text(json.dumps(replay))
    extra_replay = {**pair, "replay_digest": digest(replay)}
    assert verify_comparison(tmp_path, facts, extra_replay, repair_tree=tree)["passed"] is None
    batch["candidate_retained_digest"] = digest({
        r["key"].rsplit(":", 1)[-1]: r["sample_digest"] for r in batch["retained"] if r["after"]
    })
    path.write_text(json.dumps(replay))
    extra_replay["replay_digest"] = digest(replay)
    assert verify_comparison(tmp_path, facts, extra_replay, repair_tree=tree)["unresolved"] == [extra_key]
    extra_review = {**labels, "replay_digest": digest(replay), "labels": {extra_key: label(extra_row, "relevant")}}
    (tmp_path / "law-02-repair-labels.json").write_text(json.dumps(extra_review))
    extra_replay["repair_labels_digest"] = digest(extra_review)
    verified = verify_comparison(tmp_path, facts, extra_replay, repair_tree=tree)
    assert verified["passed"] is True and verified["reviewed_samples"] == 17
    replay["batches"][0]["decision_digests"][0] = "d" * 64
    path.write_text(json.dumps(replay))
    updated = {**pair, "replay_digest": digest(replay)}
    assert verify_comparison(tmp_path, facts, updated, repair_tree=tree)["passed"] is None


@pytest.mark.parametrize("group", ["draft", "other_issue", "diverged"])
def test_unbound_repair_pr_keeps_historical_acceptance_unknown(group):
    class Client:
        repo = "example/project"

        def main_sha(self):
            return "main-sha"

        def api(self, path):
            if group == "diverged" and path == "repos/example/project/compare/repair-sha...main-sha":
                return {"status": "diverged"}
            assert path == "repos/example/project/pulls/188"
            return {
                "merged": group != "draft", "base": {"ref": "main"},
                "merge_commit_sha": "repair-sha",
                "head": {"repo": {"full_name": self.repo}, "sha": "head-sha"},
                "body": "Primary task: Refs #83" if group == "other_issue" else "Primary task: Refs #73",
            }

    facts = {"issue": 73, "pr": 86, "merge_sha": MERGE, "production": {
        "verifier_version": 3, "shadow_comparison": {"repair_pr": 188},
    }}
    assert shadow_evidence(Client(), facts)["passed"] is None
