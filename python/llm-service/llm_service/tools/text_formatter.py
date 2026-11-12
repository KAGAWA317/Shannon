"""
Tool text formatters for the /tools/execute API.
用于 /tools/execute API 的 tool 文本格式化器。

Generates LLM-friendly text representations of tool output.
Used by the direct tool execution endpoint (CLI, external clients).
Does NOT affect orchestrated workflows (agent loop has its own formatting).
生成便于 LLM 理解的 tool 输出文本表示。
供直接 tool 执行端点（CLI、外部客户端）使用。
不影响编排 workflow（agent loop 有自己的格式化逻辑）。

Design: A generic fallback handles most tools automatically by stripping
metadata and formatting common output shapes (dicts, lists, scalars).
Per-tool overrides exist only for tools with unusual structures (e.g.
web_search with nested results + relevance filtering).
设计：通用回退逻辑通过剥离 metadata 并格式化常见输出结构（字典、列表、标量）
自动处理大多数 tool。仅对结构特殊的 tool 提供按 tool 覆写（例如
web_search 的嵌套结果与相关性过滤）。
"""

import html
import json
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Blocklist: internal metadata keys stripped from output before presenting to LLMs.
# Evolve this set as new tools are added — err on the side of stripping.
# If a key should never reach an LLM (internal routing, debug info, cost fields),
# add it here rather than per-tool.
# 黑名单：呈现给 LLM 前从输出中剥离的内部 metadata 键。
# 新增 tool 时持续演进此集合——宁可多剥离。
# 若某个键绝不应到达 LLM（内部路由、调试信息、成本字段），
# 在此添加，而不是在每个 tool 里处理。
_STRIP_KEYS = frozenset({
    "tool_source", "method", "fetch_method", "status_code", "blocked_reason",
    "provider", "provider_used", "strategy", "attempts", "partial_success",
    "failure_summary", "urls_attempted", "urls_succeeded", "urls_failed",
    "pages_fetched", "truncated", "char_count", "word_count",
    "trade_count", "vwap", "adjusted_close",
    "extracted", "extraction_model", "extraction_tokens", "extraction_cost_usd",
})

# Max total text length
# 文本总长度上限
_MAX_TEXT_LEN = 6000


def format_tool_text(
    tool_name: str, output: Any, metadata: Optional[Dict[str, Any]] = None
) -> Optional[str]:
    """
    Generate an LLM-friendly text representation of tool output.
    生成便于 LLM 理解的 tool 输出文本表示。

    Uses per-tool formatter if registered, otherwise falls back to
    generic formatting. Never raises — returns None on failure.
    若注册了按 tool 格式化器则使用，否则回退到通用格式化。
    从不抛 exception——失败时返回 None。
    """
    formatter = _FORMATTERS.get(tool_name, _format_generic)
    try:
        result = formatter(output, metadata)
        if result and len(result) > _MAX_TEXT_LEN:
            result = result[:_MAX_TEXT_LEN] + "..."
        return result
    except Exception:
        logger.debug("text_formatter failed for %s, falling back", tool_name, exc_info=True)
        return None


# ---------------------------------------------------------------------------
# Generic fallback — handles any tool without specific registration
# 通用回退——处理任何未单独注册的 tool
# ---------------------------------------------------------------------------


def _format_generic(output: Any, metadata: Optional[Dict[str, Any]]) -> Optional[str]:
    """Format any tool output by stripping metadata keys and presenting content.
    格式化任意 tool 输出：剥离 metadata 键并呈现内容。"""
    if output is None:
        return None

    # Scalar — just stringify
    # 标量——直接转为字符串
    if isinstance(output, (str, int, float, bool)):
        return str(output)

    # List of items — format each
    # 条目列表——逐项格式化
    if isinstance(output, list):
        if not output:
            return "No results."
        parts: List[str] = []
        for i, item in enumerate(output[:20], 1):
            if isinstance(item, dict):
                parts.append(f"{i}. {_format_dict_compact(item)}")
            else:
                parts.append(f"{i}. {str(item)}")
        return "\n".join(parts)

    # Dict — the most common case
    # 字典——最常见的情况
    if isinstance(output, dict):
        return _format_dict_smart(output)

    return str(output)


def _format_items(items: List[Any], empty: str) -> str:
    """Format a list of items as numbered lines (max 20 items).
    把条目列表格式化为带序号的文本（最多 20 条）。"""
    if not items:
        return empty
    return "\n".join(
        f"{i}. {_format_dict_compact(item) if isinstance(item, dict) else item}"
        for i, item in enumerate(items[:20], 1)
    )


def _format_dict_smart(d: Dict[str, Any]) -> str:
    """Intelligently format a dict based on its shape.
    根据字典结构智能格式化。"""
    # If it has a 'content' key, it's likely a fetched page / document
    # 若含 'content' 键，很可能是抓取的页面/文档
    if "content" in d and isinstance(d["content"], str):
        parts: List[str] = []
        if d.get("title"):
            parts.append(f"Title: {d['title']}")
        if d.get("url"):
            parts.append(f"URL: {d['url']}")
        content = d["content"]
        if len(content) > 3000:
            content = content[:3000] + "..."
        if parts:
            parts.append("")
        parts.append(content)
        return "\n".join(parts)

    # If it has a 'data' key with a list, format as entries
    # 若含列表类型的 'data' 键，按条目格式化
    if "data" in d and isinstance(d["data"], list):
        return _format_items(d["data"], "No data.")

    # If it has a 'results' key with a list, format as entries
    # 若含列表类型的 'results' 键，按条目格式化
    if "results" in d and isinstance(d["results"], list):
        return _format_items(d["results"], "No results.")

    # Default: strip metadata keys and compact-print remaining
    # 默认：剥离 metadata 键后紧凑打印剩余内容
    return _format_dict_compact(d)


def _format_dict_compact(d: Dict[str, Any]) -> str:
    """Format a dict by stripping metadata keys and presenting key: value pairs.
    格式化字典：剥离 metadata 键并呈现 key: value 对。"""
    clean = {k: v for k, v in d.items() if k not in _STRIP_KEYS and v is not None}
    if not clean:
        return "{}"
    # If only one key, just show the value
    # 只有一个键时，直接显示值
    if len(clean) == 1:
        val = next(iter(clean.values()))
        return str(val) if not isinstance(val, dict) else json.dumps(val, ensure_ascii=False)
    # Format as key: value lines, truncating long values
    # 按 key: value 行格式化，截断过长的值
    lines: List[str] = []
    for k, v in clean.items():
        if isinstance(v, str) and len(v) > 500:
            v = v[:500] + "..."
        elif isinstance(v, (dict, list)):
            s = json.dumps(v, ensure_ascii=False)
            if len(s) > 500:
                s = s[:500] + "..."
            v = s
        lines.append(f"{k}: {v}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Per-tool overrides — only for tools with non-obvious output structures
# 按 tool 覆写——仅用于输出结构不直观的 tool
# ---------------------------------------------------------------------------


def _format_web_search(output: Any, metadata: Optional[Dict[str, Any]]) -> Optional[str]:
    """Format web_search output — needs custom handling for nested results + html unescape.
    格式化 web_search 输出——需自定义处理嵌套结果与 HTML 反转义。"""
    if not isinstance(output, dict):
        if isinstance(output, list):
            results = output  # fallback scrape path returns bare list
            # 回退抓取路径返回裸列表
        else:
            return None
    else:
        results = output.get("results")
        if not isinstance(results, list):
            return None

    if not results:
        return "No search results found."

    query = output.get("query", "") if isinstance(output, dict) else ""
    lines: List[str] = []
    if query:
        lines.append(f"Search results for: {query}\n")

    for i, item in enumerate(results[:7], 1):
        if not isinstance(item, dict):
            continue
        title = html.unescape(item.get("title", ""))
        url = item.get("url", "")
        snippet = item.get("snippet", "")
        date = item.get("published_date", "")

        raw = item.get("markdown") if isinstance(item.get("markdown"), str) else None
        if not raw:
            raw = item.get("content", "")
        content = html.unescape(raw) if raw else html.unescape(snippet)
        if len(content) > 1500:
            content = content[:1500] + "..."

        entry = f"{i}. {title}"
        if date:
            entry += f" ({date[:10]})"
        entry += f"\n   {url}"
        if content:
            entry += f"\n   {content}"
        lines.append(entry)

    return "\n\n".join(lines)


def _format_web_fetch(output: Any, metadata: Optional[Dict[str, Any]]) -> Optional[str]:
    """Format web_fetch — batch scaling + explicit single-URL handling.
    格式化 web_fetch——批量缩放与显式单 URL 处理。

    Content cleaning is firecrawl's job (onlyMainContent + excludeTags).
    This formatter focuses on truncation budget and batch scaling only.
    内容清洗由 firecrawl 负责（onlyMainContent + excludeTags）。
    此格式化器只关注截断 budget 与批量缩放。
    """
    if not isinstance(output, dict):
        return None

    # Multi-URL response has 'pages' key (batch fetch)
    # 多 URL response 含 'pages' 键（批量抓取）
    if "pages" in output and isinstance(output["pages"], list):
        pages = output["pages"]
        num_pages = sum(1 for p in pages if isinstance(p, dict) and p.get("success", False))
        # Scale per-page cap: more pages = less per page to stay within total budget
        # 缩放单页上限：页面越多，单页越少，以控制在总 budget 内
        per_page_cap = min(3000, _MAX_TEXT_LEN // max(num_pages, 1) - 200)

        parts: List[str] = []
        for item in pages:
            if not isinstance(item, dict):
                continue
            if not item.get("success", False):
                parts.append(f"[Failed] {item.get('url', '?')}: {item.get('error', 'unknown')}")
                continue
            title = item.get("title", "")
            url = item.get("url", "")
            # Prefer snippet for batch (already clean, 500 chars) when cap is tight
            # 上限紧张时批量优先使用 snippet（已清洗，500 字符）
            if per_page_cap <= 1500 and item.get("snippet"):
                content = item["snippet"]
            else:
                content = item.get("content", "")
                if len(content) > per_page_cap:
                    content = content[:per_page_cap] + "..."
            entry = f"Title: {title}\nURL: {url}\n\n{content}" if title else f"URL: {url}\n\n{content}"
            parts.append(entry)
        return "\n\n---\n\n".join(parts) if parts else None

    # Single-URL: delegate to generic dict handler (has content/title/url)
    # 单 URL：交给通用字典处理器（含 content/title/url）
    return _format_generic(output, metadata)


# ---------------------------------------------------------------------------
# Registry — per-tool overrides only; all other tools use _format_generic
# 注册表——仅按 tool 覆写；其他 tool 均使用 _format_generic
# ---------------------------------------------------------------------------

_FORMATTERS: Dict[str, Any] = {
    "web_search": _format_web_search,
    "web_fetch": _format_web_fetch,
    "web_crawl": _format_web_fetch,
    "web_subpage_fetch": _format_web_fetch,
}
