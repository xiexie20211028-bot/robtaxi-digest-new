"""#73 专属的同样本回放证据，只读取主分支中经评审的固定文件。"""
from __future__ import annotations

import json
from pathlib import Path
import re

from .development_policy import digest

MERGE = "dad99a497e1e6376f1ef7315a60f996cec685fd0"
PARENT = "ea1f338910cb16f69c13eae6248c0c29ba499387"
REPLAY_PATHS = ("app", "sources.json", "requirements.txt")


def _unknown(reason, **details):
    return {"passed": None, "reason": reason, **details}


def _retained(replay, inputs):
    batches = replay["batches"]
    if (not inputs or len(batches) != len(inputs) or replay["inputs_digest"] != digest(inputs)
            or len({i["run"] for i in inputs}) != len(inputs)):
        raise ValueError("回放输入清单不匹配")
    retained = {}
    for batch, original in zip(batches, inputs):
        if (batch["run"] != original["run"] or batch["input_sha256"] != original["input_sha256"]
                or batch["count"] != original["candidates"] or len(batch["decision_digests"]) != 2
                or not all(re.fullmatch(r"[0-9a-f]{64}", d) for d in batch["decision_digests"])):
            raise ValueError("回放批次不匹配")
        if len(batch["retained"]) > batch["count"]:
            raise ValueError("保留项超过本批样本数")
        prefix = f"{original['run']}:{original['attempt']}:optimized:"
        for row in batch["retained"]:
            key = row["key"]
            if (not key.startswith(prefix) or key in retained or not key[len(prefix):]
                    or not re.fullmatch(r"[0-9a-f]{64}", row["sample_digest"])
                    or any(type(row[k]) is not bool for k in ("before", "after"))
                    or not (row["before"] or row["after"])):
                raise ValueError("回放样本身份或去留异常")
            retained[key] = row
    return retained


def score_labels(retained, labels):
    """仅标注两版入选并集。两版均淘汰的样本对两侧误收数均无贡献。"""
    if not isinstance(labels, dict) or labels.keys() != retained.keys():
        return _unknown("缺少完整保留并集的独立标注", expected_labels=len(retained))
    missing = []
    noise = []
    for key, row in retained.items():
        label = labels[key]
        if (not isinstance(label, dict) or label.get("verdict") not in {"relevant", "noise"}
                or label.get("sample_digest") != row["sample_digest"]
                or not isinstance(label.get("reason"), str) or not label["reason"].strip()
                or not isinstance(label.get("source_url"), str) or not label["source_url"].startswith("https://")
                or not re.fullmatch(r"[0-9a-f]{64}", str(label.get("source_content_sha256", "")))):
            missing.append(key)
        elif label["verdict"] == "noise":
            noise.append(row)
    if missing:
        return _unknown("独立业务标注或可追溯原文仍不齐全", unresolved=missing)
    before = sum(row["before"] for row in noise)
    after = sum(row["after"] for row in noise)
    return {"passed": 100 * after <= 110 * before, "baseline_noise": before, "candidate_noise": after,
            "growth_percent": 100 * (after - before) / before if before else None,
            "reviewed_samples": len(labels)}


def verify_comparison(root: Path, facts: dict, pair: dict, *, repair_tree: dict | None = None) -> dict:
    if (facts.get("issue") != 73 or facts.get("pr") != 86 or facts.get("merge_sha") != MERGE
            or not isinstance(pair, dict) or pair.get("mode") != "paired_offline"):
        return _unknown("缺少匹配 #73 / PR86 的新版交付登记")
    try:
        inputs = json.loads((root / "law-02-inputs.json").read_text())
        original = json.loads((root / "law-02-replay.json").read_text())
        repaired = repair_tree is not None
        replay = json.loads((root / "law-02-repair-replay.json").read_text()) if repaired else original
        replay_id = digest(replay)
        schema = "law-02-paired-replay-v3" if repaired else "law-02-paired-replay-v2"
        if (replay_id != pair.get("replay_digest") or replay["schema"] != schema
                or replay["issue"] != 73 or replay["pr"] != 86 or replay["baseline"] != PARENT
                or replay["merge_sha"] != MERGE or replay["profile"] != "optimized"):
            return _unknown("成对回放未绑定本次验收修订")
        retained = _retained(replay, inputs)
        if repaired:
            if (set(repair_tree) != set(REPLAY_PATHS) or replay["repair_tree"] != repair_tree
                    or replay["repair_sha"] != pair.get("repair_sha")
                    or not re.fullmatch(r"[0-9a-f]{40}", replay["repair_sha"])):
                return _unknown("剩余修复的回放版本与已合并代码不一致")
            original_retained = _retained(original, inputs)
            baseline = {k: r["sample_digest"] for k, r in original_retained.items() if r["before"]}
            if (baseline != {k: r["sample_digest"] for k, r in retained.items() if r["before"]}
                    or any(a["decision_digests"][0] != b["decision_digests"][0]
                           for a, b in zip(original["batches"], replay["batches"]))):
                return _unknown("剩余修复回放与 PR86 使用了不同的固定基线")
        path = root / "law-02-paired-labels.json"
        if not path.is_file():
            return _unknown("独立原文标注待核验", replay_digest=replay_id, expected_labels=len(retained))
        reviewed = json.loads(path.read_text())
        labels_id = digest(reviewed)
        if (labels_id != pair.get("labels_digest") or reviewed["replay_digest"] != digest(original)
                or reviewed.get("schema") != "law-02-independent-labels-v1"
                or not isinstance(reviewed.get("reviewer"), str) or not reviewed["reviewer"].strip()):
            return _unknown("独立标注未绑定本次验收修订或缺少评审人")
        labels = reviewed["labels"]
        if repaired:
            # 既有盲审只覆盖原并集，对新入选候选仍要求独立标注。
            new_keys = retained.keys() - labels.keys()
            labels = {key: labels[key] for key in retained if key in labels}
            if new_keys:
                extra_path = root / "law-02-repair-labels.json"
                if not extra_path.is_file() or not pair.get("repair_labels_digest"):
                    return _unknown("新增入选项缺少本次独立原文标注", unresolved=sorted(new_keys))
                extra = json.loads(extra_path.read_text())
                if (digest(extra) != pair["repair_labels_digest"] or extra["replay_digest"] != replay_id
                        or extra.get("schema") != "law-02-independent-labels-v1"
                        or not isinstance(extra.get("reviewer"), str) or not extra["reviewer"].strip()
                        or extra["labels"].keys() != new_keys):
                    return _unknown("新增独立标注与修复回放的候选集合或绑定不符")
                labels.update(extra["labels"])
        return {**score_labels(retained, labels), "mode": "paired_offline",
                "replay_digest": replay_id, "labels_digest": labels_id,
                **({"repair_sha": replay["repair_sha"], "repair_pr": pair["repair_pr"],
                    "repair_labels_digest": pair.get("repair_labels_digest")} if repaired else {}),
                "total_samples": sum(i["candidates"] for i in inputs)}
    except (OSError, KeyError, TypeError, ValueError, AttributeError) as exc:
        return _unknown(f"回放证据读取失败：{type(exc).__name__}")


def verify_repair(client, root: Path, facts: dict, pair: dict) -> dict:
    from .development_history import inspect_delivery

    number = pair.get("repair_pr") if isinstance(pair, dict) else None
    if type(number) is not int or number <= 0 or number == 86:
        return _unknown("剩余修复未绑定独立合并 PR")
    delivery = inspect_delivery(client, 73, number)
    if not delivery["tests_verified"]:
        return _unknown("剩余修复的必要合并检查仍不可验证")
    if not {"app/taxonomy.py", "app/filter_scoring.py"}.issubset(delivery["files"]):
        return _unknown("合并 PR 不含本次分类和评分修复")
    prefix = f"repos/{client.repo}"
    comparison = client.api(f"{prefix}/compare/{MERGE}...{delivery['merge_sha']}")
    if comparison.get("status") != "ahead":
        return _unknown("剩余修复合并版本不继承 PR86")
    commit = client.api(f"{prefix}/git/commits/{delivery['merge_sha']}")
    tree = client.api(f"{prefix}/git/trees/{commit['tree']['sha']}")
    if tree.get("truncated") or not isinstance(tree.get("tree"), list):
        return _unknown("剩余修复的合并树不完整")
    repair_tree = {entry["path"]: {k: entry[k] for k in ("mode", "sha")}
                   for entry in tree["tree"] if entry["path"] in REPLAY_PATHS}
    return {**verify_comparison(root, facts, pair, repair_tree=repair_tree),
            "repair_merge_sha": delivery["merge_sha"]}
