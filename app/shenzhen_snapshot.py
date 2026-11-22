"""深圳交通官网的单源本机采集交接；使用同一采集器和原始请求统计。"""
import base64
import json
import os
import re
import subprocess
import zlib
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .fetch_structured import fetch_structured_source

SOURCE = "shenzhen_transport_structured"
VARIABLE = "ROBTAXI_SHENZHEN_SNAPSHOT_V1"
REPO = "xiexie20211028-bot/robtaxi-digest-new"
BEIJING = timezone(timedelta(hours=8))
ARTICLE = re.compile(r"https://jtys\.sz\.gov\.cn/ydmh/jtzx/tzgg_1508/content/post_\d+\.html")


def read_snapshot(source, now, value=None):
    if value is None:
        value = os.environ.get(VARIABLE)
    if source.get("id") != SOURCE or not value or len(value) > 48000:
        return None
    try:
        stream = zlib.decompressobj()
        raw = stream.decompress(base64.b64decode(value, validate=True), 1024 * 1024)
        if not stream.eof or stream.unused_data:
            return None
        data = json.loads(raw)
        stamp = datetime.fromisoformat(data["collected_at"])
        rows, stats = data["rows"], data["stats"]
        if (stamp.tzinfo is None or stamp > now or stamp.astimezone(BEIJING).date() != now.astimezone(BEIJING).date()
                or data["source_id"] != SOURCE or data["entry_urls"] != source["entry_urls"]
                or not isinstance(rows, list) or not 0 < len(rows) <= source.get("max_items_per_run", 8)
                or not all(isinstance(row, dict) and ARTICLE.fullmatch(row["link"])
                           and row.get("canonical_url", row["link"]) == row["link"]
                           and all(isinstance(row[k], str) and row[k].strip() for k in ("title", "content", "published")) for row in rows)
                or not all(type(stats[k]) is int for k in ("listed_items", "request_count", "request_success_count"))
                or stats["listed_items"] < len(rows) or stats["request_count"] <= len(rows)
                or stats["request_success_count"] != stats["request_count"]):
            return None
        return rows, stats, stamp.isoformat()
    except (ValueError, TypeError, KeyError, AttributeError, zlib.error, OverflowError):
        return None


def publish():
    config = json.loads((Path(__file__).resolve().parents[1] / "sources.json").read_text())
    source = next(s for s in config["sources"] if s["id"] == SOURCE)
    rows, error, stats = fetch_structured_source(source, include_stats=True)
    if error or not rows:
        raise RuntimeError(f"深圳官网采集不完整，保留上次快照：{error}")
    now = datetime.now(timezone.utc)
    data = {"source_id": SOURCE, "entry_urls": source["entry_urls"], "collected_at": now.isoformat(),
            "rows": rows, "stats": stats}
    raw = json.dumps(data, ensure_ascii=False, separators=(",", ":")).encode()
    value = base64.b64encode(zlib.compress(raw)).decode()
    if read_snapshot(source, now, value) is None:
        raise ValueError("深圳交通采集结果未通过交接校验")
    subprocess.run(["gh", "variable", "set", VARIABLE, "--repo", REPO], input=value, text=True, check=True)
    print(f"深圳交通上传 {len(rows)} 篇，采集时间 {now.isoformat()}")


if __name__ == "__main__":
    publish()
