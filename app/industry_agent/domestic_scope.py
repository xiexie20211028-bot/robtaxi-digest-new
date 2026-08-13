from __future__ import annotations

import re
from typing import Any


DOMESTIC_MARKERS = {
    "中国",
    "国内",
    "北京",
    "上海",
    "广州",
    "深圳",
    "武汉",
    "重庆",
    "杭州",
    "苏州",
    "成都",
    "天津",
    "南京",
    "合肥",
    "海南",
    "香港",
    "澳门",
    "粤港澳",
    "工信部",
    "交通运输部",
    "公安部",
    "市场监管总局",
    "发改委",
    "china",
    "chinese",
    "beijing",
    "shanghai",
    "guangzhou",
    "shenzhen",
    "wuhan",
    "hong kong",
}

FOREIGN_MARKERS = {
    "东京",
    "日本",
    "美国",
    "英国",
    "德国",
    "法国",
    "欧洲",
    "欧盟",
    "新加坡",
    "韩国",
    "首尔",
    "拉斯维加斯",
    "亚利桑那",
    "加州",
    "旧金山",
    "洛杉矶",
    "纽约",
    "阿联酋",
    "迪拜",
    "阿布扎比",
    "tokyo",
    "japan",
    "united states",
    "u.s.",
    "united kingdom",
    "germany",
    "france",
    "europe",
    "singapore",
    "seoul",
    "las vegas",
    "arizona",
    "california",
    "san francisco",
    "los angeles",
    "new york",
    "uae",
    "dubai",
    "abu dhabi",
}


def _compact(value: str) -> str:
    return re.sub(r"[^a-z0-9\u4e00-\u9fff]+", "", str(value or "").lower())


def _cn_company_aliases(config: dict[str, Any]) -> set[str]:
    aliases: set[str] = set()
    for company in config.get("companies", []):
        if not isinstance(company, dict) or str(company.get("region", "")).lower() != "cn":
            continue
        for value in (company.get("id", ""), company.get("name", ""), *company.get("aliases", [])):
            compact = _compact(str(value))
            if len(compact) >= 2:
                aliases.add(compact)
    return aliases


def has_domestic_relevance(
    text: str,
    config: dict[str, Any],
    companies: list[Any] | None = None,
) -> bool:
    """判断事件是否与中国市场或中国企业直接相关。

    国内媒体会报道纯海外事件，因此不能把信源所在地区直接当成事件地区。
    为避免误伤没有显式地名的国内新闻，只在“明确命中海外地点，同时没有
    中国地点、监管机构或中国企业”时排除。
    """
    raw = str(text or "").lower()
    compact = _compact(raw)
    company_text = " ".join(str(value) for value in companies or [])
    combined = f"{raw} {company_text.lower()}"
    combined_compact = _compact(combined)
    has_domestic_marker = any(marker in combined for marker in DOMESTIC_MARKERS)
    has_cn_company = any(alias in combined_compact for alias in _cn_company_aliases(config))
    has_foreign_marker = any(marker in combined for marker in FOREIGN_MARKERS)
    return bool(has_domestic_marker or has_cn_company or not has_foreign_marker)
