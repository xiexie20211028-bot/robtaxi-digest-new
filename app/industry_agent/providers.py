from __future__ import annotations

import json
import os
import re
from typing import Any

from app.common import http_post_json

from .contracts import ProviderUsage, SearchResearchResult


DEFAULT_CONTEXT_TOKENS = 128_000


class ProviderCallError(RuntimeError):
    """携带已发生用量的 Provider 异常，避免失败重试造成费用漏计。"""

    def __init__(self, message: str, usage: ProviderUsage) -> None:
        super().__init__(message)
        self.usage = usage


def _validated_deepseek_api_key(raw: str) -> str:
    """在联网前验证 Secret，避免 urllib 在编码请求头时才报模糊错误。"""
    value = str(raw or "").strip()
    if not value:
        raise RuntimeError("DEEPSEEK_API_KEY missing")
    if any(char.isspace() or ord(char) < 33 or ord(char) > 126 for char in value):
        raise RuntimeError("DEEPSEEK_API_KEY invalid: expected printable ASCII API key")
    return value


def _bounded_output_tokens(
    prices: dict[str, float],
    requested_output_tokens: int,
    max_cost_cny: float | None,
    context_tokens: int = DEFAULT_CONTEXT_TOKENS,
) -> int:
    """用模型上下文上限预留最坏情况输入费用，再换算可用输出 token。

    DeepSeek 接口不接受人民币金额上限，因此必须在发起请求前将成本上限
    转换为 token 上限。输入按未命中缓存的最高单价预留，不依赖缓存优惠。
    """
    if max_cost_cny is None:
        return max(1, int(requested_output_tokens))
    input_price = float(prices.get("input_cache_miss_cny_per_million", 1.0))
    output_price = float(prices.get("output_cny_per_million", 2.0))
    worst_input_cost = max(0, int(context_tokens)) * input_price / 1_000_000
    remaining = float(max_cost_cny) - worst_input_cost
    if remaining <= 0 or output_price <= 0:
        raise RuntimeError("budget_preflight_rejected")
    affordable = int(remaining * 1_000_000 / output_price)
    if affordable < 128:
        raise RuntimeError("budget_preflight_rejected")
    return max(128, min(int(requested_output_tokens), affordable))


def extract_json_object(text: str) -> dict[str, Any]:
    """从模型文本中提取首个 JSON 对象，兼容 Markdown 代码块。"""
    raw = str(text or "").strip()
    if raw.startswith("```"):
        raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.I)
        raw = re.sub(r"\s*```$", "", raw)
    try:
        payload = json.loads(raw)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        pass

    start = raw.find("{")
    if start < 0:
        return {}
    depth = 0
    in_string = False
    escaped = False
    for index in range(start, len(raw)):
        char = raw[index]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                try:
                    payload = json.loads(raw[start : index + 1])
                    return payload if isinstance(payload, dict) else {}
                except Exception:
                    return {}
    return {}


def _usage_from_response(payload: dict[str, Any], prices: dict[str, float], web_searches: int = 0) -> ProviderUsage:
    usage = payload.get("usage", {}) if isinstance(payload.get("usage", {}), dict) else {}
    input_tokens = int(usage.get("input_tokens", usage.get("prompt_tokens", 0)) or 0)
    output_tokens = int(usage.get("output_tokens", usage.get("completion_tokens", 0)) or 0)
    cache_read = int(
        usage.get("cache_read_input_tokens", usage.get("prompt_cache_hit_tokens", 0)) or 0
    )
    miss_tokens = max(0, input_tokens - cache_read)
    cost = (
        miss_tokens * float(prices.get("input_cache_miss_cny_per_million", 1.0))
        + cache_read * float(prices.get("input_cache_hit_cny_per_million", 0.02))
        + output_tokens * float(prices.get("output_cny_per_million", 2.0))
    ) / 1_000_000
    return ProviderUsage(
        input_tokens=input_tokens,
        output_tokens=output_tokens,
        cache_read_tokens=cache_read,
        web_searches=web_searches,
        estimated_cost_cny=round(cost, 6),
    )


class DeepSeekModelProvider:
    name = "deepseek"

    def __init__(self, model: str, prices: dict[str, float], timeout: int = 120) -> None:
        self.model = model
        self.prices = prices
        self.timeout = timeout
        self.api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
        base = os.environ.get("DEEPSEEK_BASE_URL", "https://api.deepseek.com").rstrip("/")
        self.endpoint = f"{base}/chat/completions"

    def complete_json(
        self,
        system_prompt: str,
        user_prompt: str,
        max_cost_cny: float | None = None,
    ) -> tuple[dict[str, Any], ProviderUsage]:
        api_key = _validated_deepseek_api_key(self.api_key)
        total_usage = ProviderUsage()
        for attempt in range(2):
            remaining_cost = None
            if max_cost_cny is not None:
                remaining_cost = max(0.0, float(max_cost_cny) - total_usage.estimated_cost_cny)
            retry_suffix = "\n上一次输出不是有效 JSON。本次只输出 JSON 对象，不要附加解释。" if attempt else ""
            try:
                output_tokens = _bounded_output_tokens(self.prices, 4096, remaining_cost)
            except Exception as exc:
                if total_usage.input_tokens or total_usage.output_tokens:
                    raise ProviderCallError(str(exc), total_usage) from exc
                raise
            body = {
                "model": self.model,
                "messages": [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": f"{user_prompt}{retry_suffix}"},
                ],
                "thinking": {"type": "disabled"},
                "response_format": {"type": "json_object"},
                "temperature": 0.0,
                "stream": False,
                "max_tokens": output_tokens,
            }
            try:
                payload = http_post_json(
                    self.endpoint,
                    body,
                    headers={"Authorization": f"Bearer {api_key}"},
                    timeout=self.timeout,
                    retries=2,
                )
            except Exception as exc:
                if total_usage.input_tokens or total_usage.output_tokens:
                    raise ProviderCallError(str(exc), total_usage) from exc
                raise
            total_usage.add(_usage_from_response(payload, self.prices))
            choices = payload.get("choices", []) if isinstance(payload, dict) else []
            content = ""
            if choices and isinstance(choices[0], dict):
                content = str(choices[0].get("message", {}).get("content", ""))
            parsed = extract_json_object(content)
            if parsed:
                return parsed, total_usage
        raise ProviderCallError("DeepSeek model returned invalid JSON after retry", total_usage)


class DeepSeekWebSearchProvider:
    """使用 DeepSeek Anthropic 兼容接口的服务器端 Web Search。"""

    name = "deepseek_web"

    def __init__(self, model: str, prices: dict[str, float], timeout: int = 300) -> None:
        self.model = model
        self.prices = prices
        self.timeout = timeout
        self.api_key = os.environ.get("DEEPSEEK_API_KEY", "").strip()
        base = os.environ.get("DEEPSEEK_ANTHROPIC_BASE_URL", "https://api.deepseek.com/anthropic").rstrip("/")
        self.endpoint = f"{base}/v1/messages"

    @staticmethod
    def _parse_response(payload: dict[str, Any]) -> SearchResearchResult:
        text_parts: list[str] = []
        trace: list[dict[str, Any]] = []
        searches = 0
        capability = False
        blocks = payload.get("content", []) if isinstance(payload.get("content", []), list) else []
        for block in blocks:
            if not isinstance(block, dict):
                continue
            kind = str(block.get("type", ""))
            if kind == "text":
                text_parts.append(str(block.get("text", "")))
            elif kind == "server_tool_use":
                capability = True
                if str(block.get("name", "")) == "web_search":
                    searches += 1
                    trace.append(
                        {
                            "type": "web_search",
                            "query": str(block.get("input", {}).get("query", ""))[:500],
                        }
                    )
            elif kind == "web_search_tool_result":
                capability = True
                urls: list[str] = []
                content = block.get("content", [])
                if isinstance(content, list):
                    for result in content:
                        if isinstance(result, dict) and str(result.get("url", "")).strip():
                            urls.append(str(result.get("url", "")).strip())
                trace.append({"type": "web_search_result", "urls": urls[:20]})
        return SearchResearchResult(
            text="\n".join(part for part in text_parts if part).strip(),
            usage=ProviderUsage(web_searches=searches),
            trace=trace,
            capability_confirmed=capability,
        )

    def _request(
        self,
        system_prompt: str,
        user_prompt: str,
        max_searches: int,
        max_tokens: int,
        max_cost_cny: float | None = None,
    ) -> SearchResearchResult:
        api_key = _validated_deepseek_api_key(self.api_key)
        body = {
            "model": self.model,
            "max_tokens": _bounded_output_tokens(self.prices, max_tokens, max_cost_cny),
            "temperature": 0.0,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
            "tools": [
                {
                    "type": "web_search_20250305",
                    "name": "web_search",
                    "max_uses": max(1, int(max_searches)),
                }
            ],
            "tool_choice": {"type": "auto"},
        }
        payload = http_post_json(
            self.endpoint,
            body,
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
            },
            timeout=self.timeout,
            retries=2,
        )
        result = self._parse_response(payload)
        result.usage = _usage_from_response(payload, self.prices, result.usage.web_searches)
        return result

    def probe(self, max_cost_cny: float | None = None) -> tuple[bool, ProviderUsage, list[dict[str, Any]]]:
        result = self._request(
            "你只负责验证联网搜索工具是否可用。",
            "必须使用 Web Search 搜索今天的中国日期，只回答一句话。",
            max_searches=1,
            max_tokens=128,
            max_cost_cny=max_cost_cny,
        )
        return result.capability_confirmed, result.usage, result.trace

    def research(
        self,
        system_prompt: str,
        user_prompt: str,
        max_searches: int,
        max_cost_cny: float | None = None,
    ) -> SearchResearchResult:
        return self._request(
            system_prompt,
            user_prompt,
            max_searches=max_searches,
            max_tokens=12000,
            max_cost_cny=max_cost_cny,
        )


def build_model_provider(config: dict[str, Any]) -> DeepSeekModelProvider:
    provider = str(config.get("model_provider", "deepseek"))
    if provider != "deepseek":
        raise ValueError(f"unsupported model provider: {provider}")
    return DeepSeekModelProvider(str(config.get("model", "deepseek-v4-flash")), dict(config.get("pricing", {})))


def build_search_provider(config: dict[str, Any]) -> DeepSeekWebSearchProvider:
    provider = str(config.get("search_provider", "deepseek_web"))
    if provider != "deepseek_web":
        raise ValueError(f"unsupported search provider: {provider}")
    return DeepSeekWebSearchProvider(str(config.get("model", "deepseek-v4-flash")), dict(config.get("pricing", {})))
