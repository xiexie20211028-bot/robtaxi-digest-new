"""两路同时失败时保留上一期页面；无法取回上一期则阻止覆盖。"""
from pathlib import Path
import argparse

from app.common import http_get_bytes


def preserve_previous(path: Path, previous_url: str) -> None:
    if path.exists():
        previous = path.read_text(encoding="utf-8")
    else:
        previous = http_get_bytes(previous_url, timeout=12, retries=2).decode("utf-8")
    if "<body" not in previous.lower():
        raise RuntimeError("上一期页面不可读取，保留已有部署并停止本次覆盖")
    import re
    banner = '<aside role="alert">本次日报更新失败：Agent 与 Legacy 均未完成。以下内容为上一期日报。</aside>'
    previous = re.sub(r"(<body\b[^>]*>)", lambda match: match.group(1) + banner, previous, count=1, flags=re.I)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(previous, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--previous-url", required=True)
    parser.add_argument("--page", default="site/index.html")
    args = parser.parse_args()
    preserve_previous(Path(args.page), args.previous_url)


if __name__ == "__main__":
    main()
