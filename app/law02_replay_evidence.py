"""#73 专属的同样本回放证据，只读取主分支中经评审的固定文件。"""
from __future__ import annotations

import json
from pathlib import Path
import re

from .development_policy import digest

MERGE = "dad99a497e1e6376f1ef7315a60f996cec685fd0"
PARENT = "ea1f338910cb16f69c13eae6248c0c29ba499387"


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


def verify_comparison(root: Path, facts: dict, pair: dict) -> dict:
    if (facts.get("issue") != 73 or facts.get("pr") != 86 or facts.get("merge_sha") != MERGE
            or not isinstance(pair, dict) or pair.get("mode") != "paired_offline"):
        return _unknown("缺少匹配 #73 / PR86 的新版交付登记")
    try:
        inputs = json.loads((root / "law-02-inputs.json").read_text())
        replay = json.loads((root / "law-02-replay.json").read_text())
        replay_id = digest(replay)
        if (replay_id != pair.get("replay_digest") or replay["schema"] != "law-02-paired-replay-v2"
                or replay["issue"] != 73 or replay["pr"] != 86 or replay["baseline"] != PARENT
                or replay["merge_sha"] != MERGE or replay["profile"] != "optimized"):
            return _unknown("成对回放未绑定本次验收修订")
        retained = _retained(replay, inputs)
        path = root / "law-02-paired-labels.json"
        if not path.is_file():
            return _unknown("独立原文标注待核验", replay_digest=replay_id, expected_labels=len(retained))
        reviewed = json.loads(path.read_text())
        labels_id = digest(reviewed)
        if (labels_id != pair.get("labels_digest") or reviewed["replay_digest"] != replay_id
                or reviewed.get("schema") != "law-02-independent-labels-v1"
                or not isinstance(reviewed.get("reviewer"), str) or not reviewed["reviewer"].strip()):
            return _unknown("独立标注未绑定本次验收修订或缺少评审人")
        return {**score_labels(retained, reviewed["labels"]), "mode": "paired_offline",
                "replay_digest": replay_id, "labels_digest": labels_id,
                "total_samples": sum(i["candidates"] for i in inputs)}
    except (OSError, KeyError, TypeError, ValueError, AttributeError) as exc:
        return _unknown(f"回放证据读取失败：{type(exc).__name__}")
