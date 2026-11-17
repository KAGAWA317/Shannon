"""
Web Search Tool supporting multiple providers: Exa, Firecrawl, Google, Serper, and Bing
Web Search tool，支持多个 provider：Exa、Firecrawl、Google、Serper 与 Bing
"""

import logging
import os
import re
from enum import Enum
from typing import Any
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

from ...config import Settings
from ..base import Tool, ToolMetadata, ToolParameter, ToolParameterType, ToolResult
from ..openapi_parser import _is_private_ip
from .web_fetch import WebFetchTool

logger = logging.getLogger(__name__)


def _ensure_snippet(result: dict[str, Any], min_length: int = 30) -> str:
    """
    Ensure result has a meaningful snippet, generate from available fields if missing.
    确保结果包含有意义的 snippet，缺失时从可用字段生成。

    Priority: snippet -> text -> content -> description -> title + url
    优先级：snippet -> text -> content -> description -> title + url
    """
    snippet = result.get("snippet", "")

    # Already has sufficient snippet
    # 已有足够长的 snippet
    if snippet and len(snippet) >= min_length:
        return snippet

    # Try alternative fields
    # 尝试其他备选字段
    alternatives = [
        result.get("text", ""),
        result.get("content", ""),
        result.get("description", ""),
        result.get("og_description", ""),
        result.get("meta_description", ""),
        result.get("markdown", ""),
    ]

    for alt in alternatives:
        if alt and len(alt) >= min_length:
            return alt[:500]  # Cap at 500 chars 注：截断到 500 字符

    # Last resort: combine title + url for minimal context
    # 最后兜底：用 title + url 组合出最小上下文
    title = result.get("title", "")
    url = result.get("url", "")
    if title:
        return f"{title} - {url}"

    return snippet  # Return whatever we have 注：有多少返回多少


class SearchProvider(Enum):
    EXA = "exa"
    FIRECRAWL = "firecrawl"
    GOOGLE = "google"
    SERPER = "serper"
    SERPAPI = "serpapi"
    SEARCHAPI = "searchapi"
    BING = "bing"


# External API cost per search call (USD).
# Recorded as 7500 synthetic tokens in token_usage ($2/1M billing rate).
# 每次搜索调用产生的第三方 API 成本（美元）。
# 在 token_usage 中记为 7500 个合成 token（按 $2/1M 计费）。
SEARCH_PROVIDER_COSTS: dict[str, float] = {
    SearchProvider.SERPAPI.value: 0.015,
    SearchProvider.SEARCHAPI.value: 0.004,
    SearchProvider.SERPER.value: 0.002,
    SearchProvider.FIRECRAWL.value: 0.001,
    SearchProvider.EXA.value: 0.003,
    SearchProvider.GOOGLE.value: 0.005,
    SearchProvider.BING.value: 0.005,
}

SEARCH_PROVIDER_MODELS: dict[str, str] = {
    SearchProvider.SERPAPI.value: "shannon_web_search",
    SearchProvider.SEARCHAPI.value: "shannon_web_search",
    SearchProvider.SERPER.value: "shannon_web_search",
    SearchProvider.FIRECRAWL.value: "shannon_firecrawl",
    SearchProvider.EXA.value: "shannon_web_search",
    SearchProvider.GOOGLE.value: "shannon_web_search",
    SearchProvider.BING.value: "shannon_web_search",
}


class WebSearchProvider:
    """
    Base class for web search providers
    web 搜索 provider 的基类
    """

    # Standardized timeout for all providers (in seconds)
    # 所有 provider 统一使用的 timeout（秒）
    DEFAULT_TIMEOUT = 20

    @staticmethod
    def validate_api_key(api_key: str) -> bool:
        """
        Validate API key format and presence
        校验 API key 的格式与是否存在
        """
        if not api_key or not isinstance(api_key, str):
            return False
        # Basic validation: should be at least 10 chars and not contain obvious test values
        # 基础校验：至少 10 个字符，且不含明显的测试值
        if len(api_key.strip()) < 10:
            return False
        if api_key.lower() in ["test", "demo", "example", "your_api_key_here", "xxx"]:
            return False
        # Check for reasonable API key pattern (alphanumeric with some special chars)
        # 检查是否符合合理的 API key 形态（字母数字加少量特殊字符）
        if not re.match(r"^[A-Za-z0-9\-_\.]+$", api_key.strip()):
            return False
        return True

    @staticmethod
    def sanitize_error_message(error: str) -> str:
        """
        Sanitize error messages to prevent information disclosure
        清洗错误信息，防止信息泄露
        """
        # Remove URLs that might contain API keys or sensitive endpoints
        # 移除可能包含 API key 或敏感端点的 URL
        sanitized = re.sub(r"https?://[^\s]+", "[URL_REDACTED]", str(error))
        # Remove potential API keys (common patterns)
        # 移除潜在的 API key（常见 pattern）
        sanitized = re.sub(r"\b[A-Za-z0-9]{32,}\b", "[KEY_REDACTED]", sanitized)
        sanitized = re.sub(
            r"api[_\-]?key[\s=:]+[\w\-]+",
            "api_key=[REDACTED]",
            sanitized,
            flags=re.IGNORECASE,
        )
        sanitized = re.sub(
            r"bearer\s+[\w\-\.]+", "Bearer [REDACTED]", sanitized, flags=re.IGNORECASE
        )
        # Remove potential file paths
        # 移除潜在的文件路径
        sanitized = re.sub(
            r"/[\w/\-\.]+\.(py|json|yml|yaml|env)", "[PATH_REDACTED]", sanitized
        )
        # Limit length to prevent excessive logging
        # 限制长度，避免日志过长
        if len(sanitized) > 200:
            sanitized = sanitized[:200] + "..."
        return sanitized

    @staticmethod
    def validate_max_results(max_results: int) -> int:
        """
        Validate and sanitize max_results parameter
        校验并修正 max_results 参数
        """
        if not isinstance(max_results, int):
            raise ValueError("max_results must be an integer")
        if max_results < 1:
            raise ValueError("max_results must be at least 1")
        if max_results > 100:
            logger.warning(
                f"max_results {max_results} exceeds limit of 100, capping to 100"
            )
            return 100
        return max_results

    async def search(self, query: str, max_results: int = 5) -> list[dict[str, Any]]:
        raise NotImplementedError


class ExaSearchProvider(WebSearchProvider):
    """
    Exa AI search provider - Semantic search optimized for AI applications
    Exa AI 搜索 provider——面向 AI 应用优化的语义搜索
    """

    def __init__(self, api_key: str):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.base_url = "https://api.exa.ai/search"

    async def search(
        self,
        query: str,
        max_results: int = 5,
        search_type: str = "auto",
        category: str = None
    ) -> list[dict[str, Any]]:
        """
        Search using Exa AI.
        使用 Exa AI 进行搜索。

        Args:
            query: Search query
            max_results: Number of results (1-100)
            search_type: "neural" (semantic), "keyword" (exact), or "auto" (intelligent blend)
            category: Optional filter - "company", "research paper", "news", "pdf",
                     "github", "tweet", "personal site", "linkedin", "financial report"

        参数：
            query: 搜索查询
            max_results: 结果数量（1-100）
            search_type: "neural"（语义）、"keyword"（精确）或 "auto"（智能混合）
            category: 可选过滤项——"company"、"research paper"、"news"、"pdf"、
                     "github"、"tweet"、"personal site"、"linkedin"、"financial report"
        """
        headers = {"x-api-key": self.api_key, "Content-Type": "application/json"}

        # Guard against invalid search_type values (LLM/tooling may send non-enum)
        # 防御非法 search_type 取值（LLM/tooling 可能传入非枚举值）
        if not isinstance(search_type, str) or search_type.strip().lower() not in (
            "neural",
            "keyword",
            "auto",
        ):
            logger.warning(
                f"Exa: invalid search_type '{search_type}', normalizing to 'auto'"
            )
            search_type = "auto"

        # Use Exa's latest API parameters with proper text content extraction
        # 使用 Exa 最新的 API 参数，并正确提取文本内容
        payload = {
            "query": query,
            "numResults": max_results,
            "type": search_type,  # neural, keyword, or auto 注：neural、keyword 或 auto
            "useAutoprompt": True,  # Enhance query for better results 注：优化 query 以提升结果质量
            "contents": {
                "text": {
                    "maxCharacters": 2000,  # Get substantial text content for each result 注：为每条结果取足够长的正文
                    "includeHtmlTags": False,  # Clean text without HTML 注：纯文本，不含 HTML
                },
                "highlights": {
                    "numSentences": 3,  # Extract key highlights 注：提取关键 highlights
                    "highlightsPerUrl": 2,
                },
            },
            "liveCrawl": "fallback",  # Use live crawl if cached results are stale 注：缓存结果过旧时改走实时抓取
        }

        # Add category filter if specified
        # 若指定了 category，则加入分类过滤
        if category:
            payload["category"] = category

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.post(
                self.base_url, json=payload, headers=headers, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"Exa API error: status={response.status}, details logged"
                    )
                    logger.debug(f"Exa API raw error: {sanitized_error}")
                    raise Exception(
                        f"Search service temporarily unavailable (Error {response.status})"
                    )

                data = await response.json()
                results = []

                # Log the first result to see what fields are available
                # 记录第一条结果，以便查看有哪些可用字段
                if data.get("results"):
                    logger.debug(
                        f"Exa response sample - First result keys: {list(data['results'][0].keys())}"
                    )

                for result in data.get("results", []):
                    # Extract text content and highlights
                    # 提取正文内容与 highlights
                    text_content = result.get("text", "")
                    highlights = result.get("highlights", [])

                    # Use highlights if available, otherwise use text content
                    # 有 highlights 就用 highlights，否则用正文内容
                    snippet = ""
                    if highlights:
                        snippet = " ... ".join(
                            highlights[:2]
                        )  # Join first 2 highlights 注：拼接前 2 条 highlight
                    elif text_content:
                        snippet = text_content[:500]  # Use first 500 chars of text 注：取正文前 500 字符

                    results.append(
                        {
                            "title": result.get("title", ""),
                            "snippet": snippet,
                            "content": text_content[:2000]
                            if text_content
                            else "",  # Include fuller content 注：附带更完整的内容
                            "url": result.get("url", ""),
                            "source": "exa",
                            "score": result.get("score", 0),
                            "published_date": result.get("publishedDate"),
                            "author": result.get("author"),
                            "highlights": highlights,
                        }
                    )

                return results


class FirecrawlSearchProvider(WebSearchProvider):
    """
    Firecrawl search provider - V2 API with search + scrape capabilities
    Firecrawl 搜索 provider——V2 API，具备搜索 + 抓取能力
    """

    def __init__(self, api_key: str):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.base_url = "https://api.firecrawl.dev/v2/search"

    async def search(self, query: str, max_results: int = 5) -> list[dict[str, Any]]:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Use Firecrawl's latest V2 search API
        # 使用 Firecrawl 最新的 V2 search API
        payload = {
            "query": query,
            "limit": min(max_results, 20),  # Firecrawl alpha caps at lower limits 注：Firecrawl alpha 上限更低
            "sources": ["web"],  # Search the web 注：搜索网页
            "scrapeOptions": {
                "formats": ["markdown"],  # Get markdown content 注：返回 markdown 内容
                "onlyMainContent": True,  # Skip navigation, ads, etc. 注：跳过导航、广告等
            },
        }

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.post(
                self.base_url, json=payload, headers=headers, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"Firecrawl API error: status={response.status}, details logged"
                    )
                    logger.debug(f"Firecrawl API raw error: {sanitized_error}")
                    raise Exception(
                        f"Search service temporarily unavailable (Error {response.status})"
                    )

                data = await response.json()
                results = []

                # Firecrawl returns data in 'data' field
                # Firecrawl 在 'data' 字段中返回数据
                for result in data.get("data", []):
                    # Extract content from markdown if available
                    # 若有 markdown 则从中提取内容
                    content = ""
                    if result.get("markdown"):
                        content = result["markdown"][:300]  # First 300 chars 注：前 300 字符
                    elif result.get("description"):
                        content = result["description"]

                    results.append(
                        {
                            "title": result.get("title", ""),
                            "snippet": content,
                            "url": result.get("url", ""),
                            "source": "firecrawl",
                            "markdown": result.get(
                                "markdown", ""
                            ),  # Full markdown available 注：可用完整 markdown
                        }
                    )

                return results


class GoogleSearchProvider(WebSearchProvider):
    """
    Google Custom Search JSON API provider
    谷歌自定义搜索（Custom Search）JSON API provider
    """

    def __init__(self, api_key: str, search_engine_id: str = None):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.search_engine_id = search_engine_id or os.getenv(
            "GOOGLE_SEARCH_ENGINE_ID", ""
        )
        if not self.search_engine_id:
            raise ValueError("Google Search Engine ID is required")
        self.base_url = "https://customsearch.googleapis.com/customsearch/v1"

    async def search(self, query: str, max_results: int = 5) -> list[dict[str, Any]]:
        params = {
            "key": self.api_key,
            "cx": self.search_engine_id,
            "q": query,
            "num": min(max_results, 10),  # Google limits to 10 per request 注：Google 每次请求上限 10 条
        }

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.get(
                self.base_url, params=params, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"Google Search API error: status={response.status}, details logged"
                    )
                    logger.debug(f"Google Search API raw error: {sanitized_error}")
                    if response.status == 403:
                        raise Exception(
                            "Search service access denied. Please check your API credentials."
                        )
                    elif response.status == 429:
                        raise Exception(
                            "Search rate limit exceeded. Please try again later."
                        )
                    else:
                        raise Exception(
                            f"Search service temporarily unavailable (Error {response.status})"
                        )

                data = await response.json()
                results = []

                for item in data.get("items", []):
                    # Extract content from various fields
                    # 从多个字段中提取内容
                    snippet = item.get("snippet", "")
                    page_map = item.get("pagemap", {})
                    metatags = page_map.get("metatags", [{}])[0]

                    # Try to get more detailed content from metatags
                    # 尝试从 metatags 获取更详细的内容
                    content = snippet
                    if metatags.get("og:description"):
                        content = metatags.get("og:description", "")[:500]
                    elif metatags.get("description"):
                        content = metatags.get("description", "")[:500]

                    results.append(
                        {
                            "title": item.get("title", ""),
                            "snippet": snippet,
                            "content": content,
                            "url": item.get("link", ""),
                            "source": "google",
                            "display_link": item.get("displayLink", ""),
                        }
                    )

                return results


class SerperSearchProvider(WebSearchProvider):
    """
    Serper API provider - Fast and affordable Google search results
    Serper API provider——快速且低价的 Google 搜索结果
    """

    def __init__(self, api_key: str):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.base_url = "https://google.serper.dev/search"

    async def search(self, query: str, max_results: int = 5) -> list[dict[str, Any]]:
        headers = {"X-API-KEY": self.api_key, "Content-Type": "application/json"}

        payload = {
            "q": query,
            "num": max_results,
        }

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.post(
                self.base_url, json=payload, headers=headers, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"Serper API error: status={response.status}, details logged"
                    )
                    logger.debug(f"Serper API raw error: {sanitized_error}")
                    if response.status == 401:
                        raise Exception(
                            "Search service authentication failed. Please check your API credentials."
                        )
                    elif response.status == 429:
                        raise Exception(
                            "Search rate limit exceeded. Please try again later."
                        )
                    else:
                        raise Exception(
                            f"Search service temporarily unavailable (Error {response.status})"
                        )

                data = await response.json()
                results = []

                # Process organic search results
                # 处理 organic 搜索结果
                for result in data.get("organic", []):
                    results.append(
                        {
                            "title": result.get("title", ""),
                            "snippet": result.get("snippet", ""),
                            "url": result.get("link", ""),
                            "source": "serper",
                            "position": result.get("position", 0),
                            "date": result.get("date"),
                        }
                    )

                # Include knowledge graph if available
                # 若有 knowledge graph 则一并纳入
                if data.get("knowledgeGraph"):
                    kg = data["knowledgeGraph"]
                    results.insert(
                        0,
                        {
                            "title": kg.get("title", ""),
                            "snippet": kg.get("description", ""),
                            "content": kg.get("description", ""),
                            "url": kg.get("website", ""),
                            "source": "serper_knowledge_graph",
                            "type": kg.get("type", ""),
                        },
                    )

                return results


class SerpAPISearchProvider(WebSearchProvider):
    """
    SerpAPI provider - Multi-engine search API supporting Google, Bing, Yahoo, Baidu, etc.
    SerpAPI provider——多引擎搜索 API，支持 Google、Bing、Yahoo、Baidu 等
    """

    def __init__(self, api_key: str, engine: str = "google"):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.base_url = "https://serpapi.com/search"
        # Supported engines: google, bing, yahoo, baidu, yandex, duckduckgo, etc.
        # 支持的引擎：google、bing、yahoo、baidu、yandex、duckduckgo 等
        # See: https://serpapi.com/search-engine-apis
        self.engine = engine.lower() if engine else "google"

    async def search(
        self,
        query: str,
        max_results: int = 5,
        engine: str | None = None,
        **extra_params: Any,
    ) -> list[dict[str, Any]]:
        # Validate max_results
        # 校验 max_results
        max_results = self.validate_max_results(max_results)

        # Use passed engine or fall back to instance default (avoids race condition)
        # 使用传入的 engine，否则回退到实例默认值（避免竞态条件）
        effective_engine = engine.lower() if engine else self.engine

        # SerpAPI uses GET requests with query parameters
        # SerpAPI 使用带查询参数的 GET request
        params = {
            "q": query,
            "api_key": self.api_key,
            "engine": effective_engine,
            "num": max_results,
        }

        # Merge extra params (gl, hl, location, tbs, etc.)
        # 合并额外参数（gl、hl、location、tbs 等）
        params.update(extra_params)

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.get(
                self.base_url, params=params, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"SerpAPI error: status={response.status}, details logged"
                    )
                    logger.debug(f"SerpAPI raw error: {sanitized_error}")
                    if response.status == 401:
                        raise Exception(
                            "Search service authentication failed. Please check your API credentials."
                        )
                    elif response.status == 429:
                        raise Exception(
                            "Search rate limit exceeded. Please try again later."
                        )
                    else:
                        raise Exception(
                            f"Search service temporarily unavailable (Error {response.status})"
                        )

                data = await response.json()
                results = []

                # Handle Google Finance special response format
                # 处理 Google Finance 的特殊 response 格式
                if effective_engine == "google_finance":
                    # Extract summary (current price, change, etc.)
                    # 提取 summary（当前价格、涨跌幅等）
                    summary = data.get("summary", {})
                    if summary:
                        price_info = {
                            "title": f"{summary.get('title', 'Stock')} ({summary.get('stock', '')})",
                            "snippet": f"Price: {summary.get('price', 'N/A')} {summary.get('currency', '')} | Change: {summary.get('price_movement', {}).get('percentage', 'N/A')}% ({summary.get('price_movement', {}).get('movement', '')})",
                            "content": f"Exchange: {summary.get('exchange', '')} | Previous close: {summary.get('previous_close', 'N/A')}",
                            "url": f"https://www.google.com/finance/quote/{summary.get('stock', '')}:{summary.get('exchange', '')}",
                            "source": "google_finance",
                            "type": "stock_quote",
                            "raw_data": summary,
                        }
                        results.append(price_info)

                    # Extract key stats if available
                    # 若有 key_stats 则提取关键统计
                    key_stats = data.get("key_stats", {})
                    if key_stats:
                        stats_info = {
                            "title": "Key Statistics",
                            "snippet": str(key_stats),
                            "source": "google_finance",
                            "type": "key_stats",
                        }
                        results.append(stats_info)

                    return results[:max_results]

                # Handle Google Finance Markets special response format
                # 处理 Google Finance Markets 的特殊 response 格式
                if effective_engine == "google_finance_markets":
                    # Extract market trends
                    # 提取市场趋势
                    market_trends = data.get("market_trends", [])
                    for trend in market_trends:
                        trend_title = trend.get("title", "Market Trend")
                        for item in trend.get("results", [])[:5]:  # Limit items per trend 注：每个 trend 限制条数
                            results.append({
                                "title": f"{item.get('name', '')} ({item.get('stock', '')})",
                                "snippet": f"Price: {item.get('price', 'N/A')} | Change: {item.get('price_movement', {}).get('percentage', 'N/A')}%",
                                "url": item.get("link", ""),
                                "source": "google_finance_markets",
                                "type": trend_title,
                            })

                    # Also include markets overview
                    # 同时纳入 markets 总览
                    markets = data.get("markets", {})
                    for region, items in markets.items():
                        if isinstance(items, list):
                            for item in items[:3]:
                                results.append({
                                    "title": f"{item.get('name', '')} ({region})",
                                    "snippet": f"Price: {item.get('price', 'N/A')} | Change: {item.get('price_movement', {}).get('percentage', 'N/A')}%",
                                    "url": item.get("link", ""),
                                    "source": "google_finance_markets",
                                    "type": f"market_{region}",
                                })

                    return results[:max_results]

                # Handle Google News special response format
                # 处理 Google News 的特殊 response 格式
                if effective_engine == "google_news":
                    # Google News returns results in news_results, not organic_results
                    # 处理 Google News 的特殊 response 格式
                    # Google News 的结果在 news_results 中，而非 organic_results
                    news_results = data.get("news_results", [])

                    # Fallback to top_stories if news_results is empty
                    # news_results 为空时降级用 top_stories
                    if not news_results:
                        news_results = data.get("top_stories", [])

                    for result in news_results:
                        results.append(
                            {
                                "title": result.get("title", ""),
                                "snippet": result.get("snippet", result.get("date", "")),
                                "url": result.get("link", ""),
                                "source": "serpapi_news",
                                "position": result.get("position", 0),
                                "date": result.get("date"),
                                "news_source": result.get("source", {}).get("name", "") if isinstance(result.get("source"), dict) else result.get("source", ""),
                                "thumbnail": result.get("thumbnail"),
                            }
                        )

                    # Ensure we don't exceed max_results
                    # 确保不超过 max_results
                    results = results[:max_results]
                    return results

                # Process organic search results (note: organic_results not organic!)
                # 处理自然搜索结果（注意：字段是 organic_results，不是 organic！）
                for result in data.get("organic_results", []):
                    results.append(
                        {
                            "title": result.get("title", ""),
                            "snippet": result.get("snippet", ""),
                            "url": result.get("link", ""),
                            "source": "serpapi",
                            "position": result.get("position", 0),
                            "date": result.get("date"),
                        }
                    )

                # Include knowledge graph if available
                # 若有 knowledge graph 则一并纳入
                if data.get("knowledge_graph"):
                    kg = data["knowledge_graph"]
                    # Safely extract URL from source field with type checking
                    # 对 source 字段做类型检查，安全地提取 URL
                    kg_url = ""
                    kg_source = kg.get("source")
                    if isinstance(kg_source, dict):
                        kg_url = kg_source.get("link", "")
                    elif isinstance(kg_source, str):
                        kg_url = kg_source
                    
                    results.insert(
                        0,
                        {
                            "title": kg.get("title", ""),
                            "snippet": kg.get("description", ""),
                            "content": kg.get("description", ""),
                            "url": kg_url,
                            "source": "serpapi_knowledge_graph",
                            "type": kg.get("type", ""),
                        },
                    )

                # Ensure we don't exceed max_results after adding knowledge graph
                # 加入 knowledge graph 后确保不超过 max_results
                results = results[:max_results]

                return results


class SearchAPISearchProvider(WebSearchProvider):
    """
    SearchAPI.io provider - SerpAPI-compatible alternative with Google, Bing, Yahoo, Baidu, etc.
    SearchAPI.io provider——兼容 SerpAPI 的替代方案，支持 Google、Bing、Yahoo、Baidu 等
    """

    def __init__(self, api_key: str, engine: str = "google"):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.base_url = "https://www.searchapi.io/api/v1/search"
        self.engine = engine.lower() if engine else "google"

    async def search(
        self,
        query: str,
        max_results: int = 5,
        engine: str | None = None,
        **extra_params: Any,
    ) -> list[dict[str, Any]]:
        max_results = self.validate_max_results(max_results)
        effective_engine = engine.lower() if engine else self.engine

        params = {
            "q": query,
            "api_key": self.api_key,
            "engine": effective_engine,
            "num": max_results,
        }
        params.update(extra_params)

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.get(
                self.base_url, params=params, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"SearchAPI.io error: status={response.status}, details logged"
                    )
                    logger.debug(f"SearchAPI.io raw error: {sanitized_error}")
                    if response.status == 401:
                        raise Exception(
                            "Search service authentication failed. Please check your API credentials."
                        )
                    elif response.status == 429:
                        raise Exception(
                            "Search rate limit exceeded. Please try again later."
                        )
                    else:
                        raise Exception(
                            f"Search service temporarily unavailable (Error {response.status})"
                        )

                data = await response.json()
                results = []

                # SearchAPI.io uses the same response structure as SerpAPI for Google
                # 对 Google 而言，SearchAPI.io 使用与 SerpAPI 相同的 response 结构

                # Handle Google News
                # 处理 Google News
                if effective_engine == "google_news":
                    news_results = data.get("news_results", [])
                    if not news_results:
                        news_results = data.get("top_stories", [])

                    for result in news_results:
                        results.append({
                            "title": result.get("title", ""),
                            "snippet": result.get("snippet", result.get("date", "")),
                            "url": result.get("link", ""),
                            "source": "searchapi_news",
                            "position": result.get("position", 0),
                            "date": result.get("date"),
                            "news_source": result.get("source", {}).get("name", "") if isinstance(result.get("source"), dict) else result.get("source", ""),
                            "thumbnail": result.get("thumbnail"),
                        })
                    return results[:max_results]

                # Organic results
                # organic 结果
                for result in data.get("organic_results", []):
                    results.append({
                        "title": result.get("title", ""),
                        "snippet": result.get("snippet", ""),
                        "url": result.get("link", ""),
                        "source": "searchapi",
                        "position": result.get("position", 0),
                        "date": result.get("date"),
                    })

                # Knowledge graph
                # 知识图谱
                if data.get("knowledge_graph"):
                    kg = data["knowledge_graph"]
                    kg_url = ""
                    kg_source = kg.get("source")
                    if isinstance(kg_source, dict):
                        kg_url = kg_source.get("link", "")
                    elif isinstance(kg_source, str):
                        kg_url = kg_source

                    results.insert(0, {
                        "title": kg.get("title", ""),
                        "snippet": kg.get("description", ""),
                        "content": kg.get("description", ""),
                        "url": kg_url,
                        "source": "searchapi_knowledge_graph",
                        "type": kg.get("type", ""),
                    })

                return results[:max_results]


class BingSearchProvider(WebSearchProvider):
    """
    Bing Search API v7 provider (Azure Cognitive Services)
    必应搜索 API v7 provider（Azure 认知服务）
    """

    def __init__(self, api_key: str):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing API key")
        self.api_key = api_key
        self.base_url = "https://api.cognitive.microsoft.com/bing/v7.0/search"

    async def search(self, query: str, max_results: int = 5) -> list[dict[str, Any]]:
        headers = {
            "Ocp-Apim-Subscription-Key": self.api_key,
        }

        params = {
            "q": query,
            "count": max_results,
            "textDecorations": True,
            "textFormat": "HTML",
        }

        async with aiohttp.ClientSession() as session:
            timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
            async with session.get(
                self.base_url, params=params, headers=headers, timeout=timeout
            ) as response:
                if response.status != 200:
                    error_text = await response.text()
                    sanitized_error = self.sanitize_error_message(error_text)
                    logger.error(
                        f"Bing Search API error: status={response.status}, details logged"
                    )
                    logger.debug(f"Bing Search API raw error: {sanitized_error}")
                    if response.status == 401:
                        raise Exception(
                            "Search service authentication failed. Please check your API credentials."
                        )
                    elif response.status == 429:
                        raise Exception(
                            "Search rate limit exceeded. Please try again later."
                        )
                    else:
                        raise Exception(
                            f"Search service temporarily unavailable (Error {response.status})"
                        )

                data = await response.json()
                results = []

                # Process web pages results
                # 处理 web pages 结果
                web_pages = data.get("webPages", {})
                for result in web_pages.get("value", []):
                    results.append(
                        {
                            "title": result.get("name", ""),
                            "snippet": result.get("snippet", ""),
                            "url": result.get("url", ""),
                            "source": "bing",
                            "display_url": result.get("displayUrl", ""),
                            "date_published": result.get("dateLastCrawled"),
                        }
                    )

                return results


class WebSearchTool(Tool):
    """
    Web search tool supporting multiple providers:
    - Google Custom Search (default)
    - Serper API
    - Bing Search API
    - Exa AI (semantic search)
    - Firecrawl (with content extraction)
    """

    def __init__(self):
        self._provider_enum_value: str | None = None
        self.provider = self._initialize_provider()
        self.settings = Settings()
        super().__init__()

    def _extract_site_url(self, query: str) -> str | None:
        """Extract a single site URL from the query for providerless fallback."""
        if not query:
            return None
        urls = re.findall(r"https?://[^\s]+", query)
        if urls:
            return urls[0]
        m = re.search(r"site:([A-Za-z0-9\.\-]+(/[^\s]+)?)", query)
        if m:
            return f"https://{m.group(1).lstrip('/')}"
        m = re.search(
            r"\b([A-Za-z0-9][A-Za-z0-9\-\._]*\.[A-Za-z]{2,}(?:/[^\s]*)?)\b", query
        )
        if m:
            return f"https://{m.group(1).lstrip('/')}"
        return None

    async def _fallback_site_scrape(self, site_url: str, max_results: int) -> ToolResult:
        """If no provider is configured, fetch the target site and extract same-domain links."""
        try:
            parsed = urlparse(site_url)
            if not parsed.scheme or not parsed.netloc:
                return ToolResult(success=False, output=None, error="Invalid URL in query")
            if parsed.scheme not in ("http", "https"):
                return ToolResult(success=False, output=None, error="Only HTTP/HTTPS URLs are supported")
            if _is_private_ip(parsed.hostname or ""):
                return ToolResult(
                    success=False,
                    output=None,
                    error="Access to private/internal IP addresses is not allowed.",
                )

            timeout = aiohttp.ClientTimeout(total=WebSearchProvider.DEFAULT_TIMEOUT)
            headers = {"User-Agent": "Shannon-Search/1.0 (+https://docs.shannon.run)"}
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(site_url, headers=headers, allow_redirects=True) as resp:
                    if resp.status != 200:
                        return ToolResult(
                            success=False,
                            output=None,
                            error=f"Fallback fetch failed: HTTP {resp.status}",
                        )
                    html = await resp.text(errors="ignore")

            soup = BeautifulSoup(html, "lxml")
            links = []
            seen = set()
            for a in soup.find_all("a", href=True):
                href = a["href"].strip()
                full = urljoin(site_url, href)
                parsed_href = urlparse(full)
                if parsed_href.netloc != parsed.netloc:
                    continue
                if full in seen:
                    continue
                seen.add(full)
                title = (a.get_text() or "").strip()
                result_data = {
                    "title": title or full,
                    "snippet": "",
                    "content": "",
                    "url": full,
                    "source": "fallback_scrape",
                }
                result_data["snippet"] = _ensure_snippet(result_data)
                links.append(result_data)
                if len(links) >= max_results:
                    break

            if not links:
                return ToolResult(
                    success=False,
                    output=None,
                    error="Fallback scrape found no same-domain links.",
                )

            return ToolResult(success=True, output=links)
        except Exception as e:
            return ToolResult(
                success=False,
                output=None,
                error=f"Fallback scrape failed: {str(e)}",
            )

    def _initialize_provider(self) -> WebSearchProvider | None:
        """Initialize the search provider based on environment configuration"""

        # Default to Google, then Serper, then others
        # 默认 Google，其次 Serper，再次其他
        default_provider = SearchProvider.GOOGLE.value

        # Check which provider is configured via environment
        # 检查环境变量配置的是哪个 provider
        provider_name = os.getenv("WEB_SEARCH_PROVIDER", default_provider).lower()

        # Provider configuration
        # provider 配置表
        providers_config = {
            SearchProvider.GOOGLE.value: {
                "class": GoogleSearchProvider,
                "api_key_env": "GOOGLE_SEARCH_API_KEY",
                "requires_extra": "GOOGLE_SEARCH_ENGINE_ID",
            },
            SearchProvider.SERPER.value: {
                "class": SerperSearchProvider,
                "api_key_env": "SERPER_API_KEY",
            },
            SearchProvider.SERPAPI.value: {
                "class": SerpAPISearchProvider,
                "api_key_env": "SERPAPI_API_KEY",
            },
            SearchProvider.SEARCHAPI.value: {
                "class": SearchAPISearchProvider,
                "api_key_env": "SEARCHAPI_API_KEY",
            },
            SearchProvider.BING.value: {
                "class": BingSearchProvider,
                "api_key_env": "BING_API_KEY",
            },
            SearchProvider.EXA.value: {
                "class": ExaSearchProvider,
                "api_key_env": "EXA_API_KEY",
            },
            SearchProvider.FIRECRAWL.value: {
                "class": FirecrawlSearchProvider,
                "api_key_env": "FIRECRAWL_API_KEY",
            },
        }

        # Try configured provider first
        # 优先尝试已配置的 provider
        if provider_name in providers_config:
            config = providers_config[provider_name]
            api_key = os.getenv(config["api_key_env"])
            if api_key and WebSearchProvider.validate_api_key(api_key):
                # Special handling for Google which needs search engine ID
                # Google 需要额外的 search engine ID，单独处理
                if provider_name == SearchProvider.GOOGLE.value:
                    search_engine_id = os.getenv("GOOGLE_SEARCH_ENGINE_ID")
                    if not search_engine_id:
                        logger.warning(
                            "Google Search Engine ID not found. Please set GOOGLE_SEARCH_ENGINE_ID"
                        )
                    else:
                        logger.info(f"Initializing {provider_name} search provider")
                        self._provider_enum_value = provider_name
                        return config["class"](api_key, search_engine_id)
                else:
                    logger.info(f"Initializing {provider_name} search provider")
                    self._provider_enum_value = provider_name
                    return config["class"](api_key)
            else:
                logger.warning(
                    f"{provider_name} API key not found in environment variable {config['api_key_env']}"
                )

        # Fallback: try other providers in priority order
        # 兜底：按优先级依次尝试其他 provider
        priority_order = [
            SearchProvider.GOOGLE.value,
            SearchProvider.SERPER.value,
            SearchProvider.SERPAPI.value,
            SearchProvider.SEARCHAPI.value,
            SearchProvider.BING.value,
            SearchProvider.EXA.value,
            SearchProvider.FIRECRAWL.value,
        ]

        for name in priority_order:
            if (
                name != provider_name and name in providers_config
            ):  # Skip already tried provider 注：跳过已尝试过的 provider
                config = providers_config[name]
                api_key = os.getenv(config["api_key_env"])
                if api_key and WebSearchProvider.validate_api_key(api_key):
                    # Special handling for Google
                    # Google 单独处理
                    if name == SearchProvider.GOOGLE.value:
                        search_engine_id = os.getenv("GOOGLE_SEARCH_ENGINE_ID")
                        if search_engine_id:
                            logger.info(f"Falling back to {name} search provider")
                            self._provider_enum_value = name
                            return config["class"](api_key, search_engine_id)
                    else:
                        logger.info(f"Falling back to {name} search provider")
                        self._provider_enum_value = name
                        return config["class"](api_key)

        logger.error(
            "No web search provider configured. Please set one of:\n"
            "- SEARCHAPI_API_KEY for SearchAPI.io search\n"
            "- GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_ENGINE_ID for Google Custom Search\n"
            "- SERPER_API_KEY for Serper search\n"
            "- SERPAPI_API_KEY for SerpAPI search\n"
            "- BING_API_KEY for Bing search\n"
            "- EXA_API_KEY for Exa search\n"
            "- FIRECRAWL_API_KEY for Firecrawl search\n"
            "And optionally set WEB_SEARCH_PROVIDER=searchapi|google|serper|serpapi|bing|exa|firecrawl"
        )
        return None

    def _get_metadata(self) -> ToolMetadata:
        provider_name = "none"
        if self.provider:
            provider_name = self.provider.__class__.__name__.replace(
                "SearchProvider", ""
            )

        # Generate description based on provider type
        # 按 provider 类型生成描述
        if isinstance(self.provider, (SerpAPISearchProvider, SearchAPISearchProvider)):
            desc = (
                "Search the web with multiple engine support. "
                "Discovers URLs and snippets for a given query. "
                "Default engine is Google."
                "\n\n"
                "USE WHEN:\n"
                "- Finding information, companies, people, or topics\n"
                "- Need recent news, academic papers, or videos\n"
                "- Want results from specific regions or languages\n"
                "\n"
                "NOT FOR:\n"
                "- Reading full page content (use web_fetch after getting URLs)\n"
                "- Crawling entire websites (use web_crawl)\n"
                "\n"
                "Returns: {provider, query, results: [{title, snippet, url, source, position, date}], result_count}."
                "\n\n"
                "Available engines and their strengths:\n"
                "- google (default): Best global coverage, most comprehensive\n"
                "- bing: Good alternative, sometimes different results\n"
                "- baidu: Chinese language search, good for China-hosted content\n"
                "- google_scholar: Academic papers and citations\n"
                "- youtube: Video content search\n"
                "- google_news: Recent news articles\n"
                "- google_finance: Stock/currency/crypto quotes. IMPORTANT: query MUST be ticker symbol format like 'GOOGL:NASDAQ', 'AAPL:NASDAQ', 'BTC-USD', 'EUR-USD' - NOT natural language\n"
                "- google_finance_markets: Market overview. Use trend param (indexes/gainers/losers/most-active/cryptocurrencies/currencies), query can be empty\n"
                "\n"
                "Localization options:\n"
                "- gl: Country code (e.g., 'jp', 'cn', 'de') - affects result ranking\n"
                "- hl: Language (e.g., 'ja', 'zh-CN') - affects UI and some results\n"
                "- location: City-level targeting (e.g., 'Tokyo, Japan')\n"
                "- time_filter: Recency ('day', 'week', 'month', 'year')\n"
                "\n"
                "QUERY LANGUAGE BEST PRACTICE:\n"
                "- For google/google_scholar/google_news/youtube: Prefer ENGLISH queries for best global coverage\n"
                "- For baidu: Use CHINESE queries\n"
                "- Exception: Use local language when specifically searching local markets/news "
                "(e.g., Chinese for China-specific companies, Japanese for Japan market)\n"
                "- If user query is non-English, translate to English before searching (unless exception applies)"
            )
        elif isinstance(self.provider, ExaSearchProvider):
            desc = (
                "Search the web using Exa neural semantic search. "
                "Discovers URLs and snippets with AI-powered relevance ranking."
                "\n\n"
                "USE WHEN:\n"
                "- Need semantic/conceptual search (not just keyword matching)\n"
                "- Searching for companies, research papers, news, GitHub repos\n"
                "\n"
                "NOT FOR:\n"
                "- Reading full page content (use web_fetch after getting URLs)\n"
                "\n"
                "Returns: {provider, query, results: [{title, snippet, url, score, published_date}], result_count}."
                "\n\n"
                "Search modes:\n"
                "- neural: Semantic similarity search\n"
                "- keyword: Exact match search\n"
                "- auto (default): Automatic selection\n"
                "\n"
                "Categories: company, research paper, news, pdf, github, tweet, linkedin, financial report"
            )
        else:
            desc = (
                f"Search the web using {provider_name}. "
                "Discovers URLs and snippets for a given query."
                "\n\n"
                "USE WHEN:\n"
                "- Finding information on any topic\n"
                "\n"
                "NOT FOR:\n"
                "- Reading full page content (use web_fetch after getting URLs)\n"
                "\n"
                "Returns: {provider, query, results: [{title, snippet, url}], result_count}."
            )

        return ToolMetadata(
            name="web_search",
            version="3.1.0",
            description=desc,
            category="search",
            author="Shannon",
            requires_auth=True,
            rate_limit=self.settings.web_search_rate_limit,  # Configurable via WEB_SEARCH_RATE_LIMIT env var 注：可通过 WEB_SEARCH_RATE_LIMIT 环境变量配置
            timeout_seconds=20,
            memory_limit_mb=256,
            sandboxed=True,
            dangerous=False,
            cost_per_use=0.001,  # Approximate cost per search 注：每次搜索的近似成本
            session_aware=True,  # Enable session context for official_domains auto-fetch 注：启用 session context，供 official_domains 自动抓取使用
        )

    def _get_parameters(self) -> list[ToolParameter]:
        # Base parameters (common to all providers)
        # 基础参数（所有 provider 通用）
        params = [
            ToolParameter(
                name="query",
                type=ToolParameterType.STRING,
                description=(
                    "Search query. Supports operators: site:, inurl:, intitle:. "
                    "Examples: 'OpenAI GPT-4', 'site:github.com transformer'"
                ),
                required=True,
            ),
            ToolParameter(
                name="max_results",
                type=ToolParameterType.INTEGER,
                description="Maximum results to return (1-100). Default: 10",
                required=False,
                default=10,
                min_value=1,
                max_value=100,
            ),
        ]

        # SerpAPI / SearchAPI.io parameters (compatible APIs)
        # SerpAPI / SearchAPI.io 参数（两者 API 兼容）
        if isinstance(self.provider, (SerpAPISearchProvider, SearchAPISearchProvider)):
            params.extend([
                ToolParameter(
                    name="engine",
                    type=ToolParameterType.STRING,
                    description=(
                        "Search engine: google (default), bing, baidu, "
                        "google_scholar, youtube, google_news, google_finance, google_finance_markets."
                    ),
                    required=False,
                    default="google",
                ),
                ToolParameter(
                    name="gl",
                    type=ToolParameterType.STRING,
                    description="Country code for geo-targeted results (e.g., 'jp', 'cn', 'us', 'de')",
                    required=False,
                ),
                ToolParameter(
                    name="hl",
                    type=ToolParameterType.STRING,
                    description="Language code for results (e.g., 'ja', 'zh-CN', 'en')",
                    required=False,
                ),
                ToolParameter(
                    name="location",
                    type=ToolParameterType.STRING,
                    description="City-level location (e.g., 'Tokyo, Japan', 'Shanghai, China')",
                    required=False,
                ),
                ToolParameter(
                    name="time_filter",
                    type=ToolParameterType.STRING,
                    description="Recency filter: 'day', 'week', 'month', 'year'",
                    required=False,
                ),
                ToolParameter(
                    name="window",
                    type=ToolParameterType.STRING,
                    description="Time window for google_finance: '1D', '5D', '1M', '6M', 'YTD', '1Y', '5Y', 'MAX'",
                    required=False,
                ),
                ToolParameter(
                    name="trend",
                    type=ToolParameterType.STRING,
                    description="Market trend type for google_finance_markets: 'indexes', 'most-active', 'gainers', 'losers', 'cryptocurrencies', 'currencies'",
                    required=False,
                ),
            ])

        # Exa-specific parameters
        # Exa 专属参数
        elif isinstance(self.provider, ExaSearchProvider):
            params.extend([
                ToolParameter(
                    name="search_type",
                    type=ToolParameterType.STRING,
                    description="Search mode: 'neural' (semantic), 'keyword' (exact), 'auto' (default)",
                    required=False,
                    default="auto",
                ),
                ToolParameter(
                    name="category",
                    type=ToolParameterType.STRING,
                    description=(
                        "Content category: company, research paper, news, pdf, "
                        "github, tweet, linkedin, financial report"
                    ),
                    required=False,
                ),
            ])

        # Common advanced parameters (all providers)
        # 通用高级参数（所有 provider 适用）
        params.extend([
            ToolParameter(
                name="source_type",
                type=ToolParameterType.STRING,
                description=(
                    "Source routing for Deep Research: official, aggregator, news, "
                    "academic, github, financial, local_cn, local_jp"
                ),
                required=False,
            ),
            ToolParameter(
                name="site_filter",
                type=ToolParameterType.STRING,
                description="Restrict to domains (e.g., 'crunchbase.com,linkedin.com')",
                required=False,
            ),
        ])

        return params

    async def _execute_impl(self, session_context: dict | None = None, **kwargs) -> ToolResult:
        """
        Execute web search using configured provider
        """
        query = kwargs["query"]
        max_results = kwargs.get("max_results", 10)
        search_type = kwargs.get("search_type", "auto")
        if not self.provider:
            # If no provider is configured but the query targets a single site, attempt a lightweight scrape
            # 未配置任何 provider、但 query 指向单一站点时，尝试轻量抓取
            site_url = self._extract_site_url(query)
            if site_url:
                return await self._fallback_site_scrape(site_url, max_results)
            return ToolResult(
                success=False,
                output=None,
                error=(
                    "No web search provider configured. Please set one of:\n"
                    "- SEARCHAPI_API_KEY for SearchAPI.io search\n"
                    "- GOOGLE_SEARCH_API_KEY and GOOGLE_SEARCH_ENGINE_ID for Google Custom Search\n"
                    "- SERPER_API_KEY for Serper search\n"
                    "- SERPAPI_API_KEY for SerpAPI search\n"
                    "- BING_API_KEY for Bing search\n"
                    "- EXA_API_KEY for Exa search\n"
                    "- FIRECRAWL_API_KEY for Firecrawl search"
                ),
            )
        # Sanitize/normalize search_type to avoid hard failures from invalid values
        # 清洗并归一化 search_type，避免非法取值直接报错
        if not isinstance(search_type, str) or search_type.strip() == "":
            logger.warning("Missing or empty search_type, falling back to 'auto'")
            search_type = "auto"
        else:
            search_type = search_type.strip().lower()
            if search_type not in ("neural", "keyword", "auto"):
                logger.warning(
                    f"Invalid search_type '{search_type}', falling back to 'auto'"
                )
                search_type = "auto"
        category_raw = kwargs.get("category")
        source_type = kwargs.get("source_type")
        site_filter = kwargs.get("site_filter")

        # Source type to Exa category and site mapping (Deep Research 2.0)
        # source_type 到 Exa category 与站点列表的映射（Deep Research 2.0）
        source_type_config = {
            "official": {
                "category": None,  # No category filter - rely on site: queries 注：不做 category 过滤，依赖 site: 查询
                "sites": [],  # Sites come from official_domains in context 注：站点来自 context 中的 official_domains
                "query_suffix": "official site",
            },
            "aggregator": {
                "category": "company",
                "sites": ["crunchbase.com", "pitchbook.com", "wikipedia.org", "linkedin.com", "bloomberg.com", "owler.com"],
            },
            "news": {
                "category": "news",
                "sites": ["techcrunch.com", "reuters.com", "bloomberg.com", "wsj.com", "ft.com", "venturebeat.com"],
            },
            "academic": {
                "category": "research paper",
                "sites": ["arxiv.org", "scholar.google.com", "pubmed.ncbi.nlm.nih.gov", "semanticscholar.org"],
            },
            "github": {
                "category": "github",
                "sites": ["github.com", "gitlab.com"],
            },
            "financial": {
                "category": "financial report",
                "sites": ["sec.gov", "nasdaq.com", "yahoo.com"],
            },
            "local_cn": {
                "category": None,
                "sites": [
                    # China funding/company data sources (prioritized)
                    # 中国融资/公司数据源（按优先级）
                    "tianyancha.com",   # Tianyancha - company registry + funding 注：天眼查——工商信息 + 融资
                    "qichacha.com",     # Qichacha - company registry + funding 注：企查查——工商信息 + 融资
                    "it-juzi.com",      # IT Juzi - startup funding database 注：IT 桔子——创业融资数据库
                    "36kr.com",         # 36Kr - tech news + funding 注：36氪——科技新闻 + 融资
                    "iyiou.com",        # EqualOcean - industry news + funding 注：亿欧——行业新闻 + 融资
                    "pedaily.cn",       # PEdaily - PE/VC news 注：投资界——PE/VC 新闻
                ],
            },
            "local_jp": {
                "category": None,
                "sites": [
                    # Japan funding/company data sources (prioritized)
                    # 日本融资/公司数据源（按优先级）
                    "initial.inc",      # INITIAL - Japan's largest startup database 注：INITIAL——日本最大的创业公司数据库
                    "entrepedia.jp",    # Entrepedia - Japan startup encyclopedia 注：Entrepedia——日本创业百科
                    "thebridge.jp",     # The Bridge - Japan tech startup news 注：The Bridge——日本科技创业新闻
                    "jp.techcrunch.com",  # TechCrunch Japan 注：TechCrunch 日本版
                    "nikkei.com",       # Nikkei - business news 注：日经——商业新闻
                    "prtimes.jp",       # PR Times - press releases 注：PR Times——新闻稿
                ],
            },
        }

        # Apply source_type configuration if provided
        # 若提供了 source_type，则应用对应配置
        if source_type and source_type in source_type_config:
            config = source_type_config[source_type]
            logger.info(f"Applying source_type '{source_type}' configuration")

            # Override category if source_type specifies one
            # source_type 指定了 category 时覆盖之
            if config.get("category") and not category_raw:
                category_raw = config["category"]

            # Build site filter from source_type sites
            # 用 source_type 的 sites 构建 site filter
            if config.get("sites") and not site_filter:
                site_filter = ",".join(config["sites"])

            # Add query suffix for official source type
            # 为 official 类型追加 query 后缀
            if config.get("query_suffix") and source_type == "official":
                # Check if query doesn't already have site: prefix
                # 检查 query 是否已带 site: 前缀
                if not re.search(r"site:", query, re.IGNORECASE):
                    query = f"{query} {config['query_suffix']}"

        # Apply explicit site_filter to query
        # 把显式传入的 site_filter 应用到 query
        if site_filter:
            sites = [s.strip() for s in site_filter.split(",") if s.strip()]
            if sites and not re.search(r"site:", query, re.IGNORECASE):
                # For multiple sites, use OR syntax: site:a.com OR site:b.com
                # 多站点时使用 OR 语法：site:a.com OR site:b.com
                if len(sites) == 1:
                    query = f"site:{sites[0]} {query}"
                elif len(sites) <= 3:
                    # Exa works better with fewer site filters
                    # site 过滤越少，Exa 效果越好
                    site_query = " OR ".join([f"site:{s}" for s in sites[:3]])
                    query = f"({site_query}) {query}"
                else:
                    # For many sites, just use the first 3 to avoid query pollution
                    # 站点过多时只取前 3 个，避免污染 query
                    logger.warning(f"Too many site filters ({len(sites)}), using first 3")
                    site_query = " OR ".join([f"site:{s}" for s in sites[:3]])
                    query = f"({site_query}) {query}"

        # Normalize/validate category; default to personal site for domain-scoped queries
        # 归一化并校验 category；domain 限定的 query 默认用 personal site
        category_aliases = {
            "company": "company",
            "companies": "company",
            "research paper": "research paper",
            "research papers": "research paper",
            "paper": "research paper",
            "papers": "research paper",
            "news": "news",
            "pdf": "pdf",
            "github": "github",
            "tweet": "tweet",
            "tweets": "tweet",
            "personal site": "personal site",
            "personal sites": "personal site",
            "personal-site": "personal site",
            "personal_sites": "personal site",
            "personalsite": "personal site",
            "linkedin": "linkedin",
            "financial report": "financial report",
            "financial reports": "financial report",
        }
        category = None
        if isinstance(category_raw, str):
            normalized = re.sub(r"[\s_\-]+", " ", category_raw.strip().lower())
            if normalized:
                category = category_aliases.get(normalized)
        elif category_raw is not None:
            return ToolResult(
                success=False,
                output=None,
                error="Invalid parameter: category must be a string matching one of the allowed values.",
            )

        if category is None:
            # If the query is domain-scoped, default to personal site; otherwise default to news
            # query 限定域名时默认 personal site，否则默认 news
            domains = set(re.findall(r"site:([A-Za-z0-9\.\-]+)", query or ""))
            for url in re.findall(r"https?://[^\s]+", query or ""):
                parsed = urlparse(url)
                if parsed.netloc:
                    domains.add(parsed.netloc)
            for token in re.findall(r"\b([A-Za-z0-9][A-Za-z0-9\-\._]*\.[A-Za-z]{2,})\b", query or ""):
                domains.add(token.lower())

            if len(domains) == 1:
                category = "personal site"
            else:
                category = "news"

        # Validate max_results parameter
        # 校验 max_results 参数
        try:
            max_results = self.provider.validate_max_results(max_results)
        except ValueError as e:
            return ToolResult(
                success=False, output=None, error=f"Invalid parameter: {str(e)}"
            )

        try:
            logger.info(
                f"Executing web search with {self.provider.__class__.__name__}: {query}"
            )

            # Emit progress via observer if available
            # 若有 observer，则上报进度
            observer = kwargs.get("observer")
            if observer:
                try:
                    observer("progress", {"message": f"Searching with {self.provider.__class__.__name__}..."})
                except Exception:
                    pass

            # Pass provider-specific parameters
            # 传各 provider 专属参数
            if isinstance(self.provider, ExaSearchProvider):
                results = await self.provider.search(
                    query, max_results, search_type=search_type, category=category
                )
            elif isinstance(self.provider, (SerpAPISearchProvider, SearchAPISearchProvider)):
                # Extract and validate SerpAPI/SearchAPI.io-specific parameters
                # 提取并校验 SerpAPI/SearchAPI.io 专属参数
                engine = kwargs.get("engine", "google")
                gl = kwargs.get("gl")
                hl = kwargs.get("hl")
                location = kwargs.get("location")
                time_filter = kwargs.get("time_filter")
                window = kwargs.get("window")
                trend = kwargs.get("trend")

                # Validate engine
                # 校验 engine
                valid_engines = {
                    "google", "bing", "baidu", "google_scholar",
                    "youtube", "google_news", "google_finance", "google_finance_markets"
                }
                if engine not in valid_engines:
                    return ToolResult(
                        success=False,
                        output=None,
                        error=f"Invalid engine '{engine}'. Valid options: {', '.join(sorted(valid_engines))}",
                    )

                # Validate time_filter
                # 校验 time_filter
                valid_time_filters = {"day", "week", "month", "year"}
                if time_filter and time_filter not in valid_time_filters:
                    return ToolResult(
                        success=False,
                        output=None,
                        error=f"Invalid time_filter '{time_filter}'. Valid options: {', '.join(sorted(valid_time_filters))}",
                    )

                # Validate window (for google_finance)
                # 校验 window（google_finance 用）
                valid_windows = {"1D", "5D", "1M", "6M", "YTD", "1Y", "5Y", "MAX"}
                if window and window not in valid_windows:
                    return ToolResult(
                        success=False,
                        output=None,
                        error=f"Invalid window '{window}'. Valid options: {', '.join(sorted(valid_windows))}",
                    )

                # Validate trend (for google_finance_markets)
                # 校验 trend（google_finance_markets 用）
                valid_trends = {"indexes", "most-active", "gainers", "losers", "climate-leaders", "crypto", "currencies"}
                if trend and trend not in valid_trends:
                    return ToolResult(
                        success=False,
                        output=None,
                        error=f"Invalid trend '{trend}'. Valid options: {', '.join(sorted(valid_trends))}",
                    )

                # Sanitize location (basic alphanumeric + spaces/commas only)
                # 清洗 location（仅允许字母数字、空格与逗号）
                if location:
                    if not re.match(r"^[\w\s,.-]+$", location):
                        return ToolResult(
                            success=False,
                            output=None,
                            error="Invalid location format. Use alphanumeric characters, spaces, commas, periods, and hyphens only.",
                        )

                # Build extra params dict
                # 构建 extra params 字典
                extra_params: dict[str, Any] = {}
                if gl:
                    # Basic validation: 2-letter country code
                    # 基础校验：两位国家代码
                    if len(gl) == 2 and gl.isalpha():
                        extra_params["gl"] = gl.lower()
                if hl:
                    # Basic validation: language code format (e.g., "en", "zh-CN")
                    # 基础校验：语言代码格式（如 "en"、"zh-CN"）
                    if re.match(r"^[a-z]{2}(-[A-Z]{2})?$", hl):
                        extra_params["hl"] = hl
                if location:
                    extra_params["location"] = location
                if time_filter:
                    tbs_map = {"day": "qdr:d", "week": "qdr:w", "month": "qdr:m", "year": "qdr:y"}
                    extra_params["tbs"] = tbs_map[time_filter]

                # Google Finance specific parameters
                # Google Finance 专属参数
                if engine == "google_finance" and window:
                    extra_params["window"] = window
                if engine == "google_finance_markets" and trend:
                    extra_params["trend"] = trend

                # Pass engine as parameter instead of mutating provider state
                # engine 作为参数传入，而不是改写 provider 状态
                results = await self.provider.search(query, max_results, engine=engine, **extra_params)
            else:
                results = await self.provider.search(query, max_results)

            if not results:
                # Provide specific empty result reason instead of generic "knowledge cutoff" narrative
                # 给出具体的空结果原因，而不是笼统的 "knowledge cutoff" 说法
                provider_name = self.provider.__class__.__name__
                engine_info = kwargs.get("engine", "google") if isinstance(self.provider, (SerpAPISearchProvider, SearchAPISearchProvider)) else "default"

                empty_reason = f"Search returned no results. Provider: {provider_name}, Engine: {engine_info}, Query: '{query[:100]}...'"

                # Suggest troubleshooting steps
                # 给出排查建议
                suggestions = []
                if "google_news" in engine_info:
                    suggestions.append("Try broader search terms or different time filters")
                    suggestions.append("News results may be limited for this topic/region")
                elif "google_finance" in engine_info:
                    suggestions.append("Ensure ticker format is correct (e.g., 'GOOGL:NASDAQ')")
                else:
                    suggestions.append("Try rephrasing the query or using different keywords")
                    suggestions.append("Try a different search engine (bing, baidu)")

                logger.warning(f"Empty search results: {empty_reason}")

                return ToolResult(
                    success=True,  # Search executed successfully, just no results 注：搜索已成功执行，只是无结果
                    output=[],
                    metadata={
                        "query": query,
                        "provider": provider_name,
                        "result_count": 0,
                        "empty_reason": empty_reason,
                        "suggestions": suggestions,
                        "note": "Search returned empty results. This is NOT a knowledge cutoff issue - the search was executed but found no matching content.",
                    },
                )

            logger.info(f"Web search returned {len(results)} results")

            # Optional: auto-fetch top results when using Exa to ensure deep reads
            # 可选：使用 Exa 时自动抓取前几条结果，保证深读
            auto_fetch_meta = None
            auto_fetch_enabled = int(os.getenv("WEB_SEARCH_EXA_AUTO_FETCH_ENABLED", "1")) > 0
            if isinstance(self.provider, ExaSearchProvider) and auto_fetch_enabled:
                ctx = session_context if isinstance(session_context, dict) else {}
                is_research = bool(ctx.get("research_mode"))

                # Base defaults (safer): disabled by default, small caps
                # 基础默认值（更保守）：默认关闭，上限较小
                auto_fetch_top_k_env = int(os.getenv("WEB_SEARCH_EXA_AUTO_FETCH_TOP_K", "0"))
                fetch_max_length_env = int(os.getenv("WEB_SEARCH_EXA_AUTO_FETCH_MAX_LENGTH", "8000"))

                # Allow per-request overrides via session_context (passed through safe_keys)
                # 允许经 session_context 按请求覆盖（通过 safe_keys 透传）
                auto_fetch_top_k = int(ctx.get("auto_fetch_k", auto_fetch_top_k_env))
                fetch_max_length = int(ctx.get("auto_fetch_max_length", fetch_max_length_env))

                # Gate: only fetch if research OR explicit top_k > 0
                # 触发条件：research 模式，或显式 top_k > 0
                should_auto_fetch = False

                official_domains = ctx.get("official_domains", [])
                if not isinstance(official_domains, list):
                    official_domains = []
                # Skip auto-fetch for inferred domains (not verified by domain_analysis)
                # 推断出的 domain（未经 domain_analysis 验证）跳过自动抓取
                official_domains_source = ctx.get("official_domains_source", "")
                is_domains_verified = official_domains_source not in ("", "refiner_inferred")
                has_official = len(official_domains) > 0 and is_domains_verified

                if (is_research and (auto_fetch_top_k > 0 or has_official)) or auto_fetch_top_k > 0:
                    should_auto_fetch = True

                if should_auto_fetch:
                    fetcher = WebFetchTool()
                    total_chars_cap = int(os.getenv("WEB_SEARCH_EXA_AUTO_FETCH_TOTAL_CHARS_CAP", "30000"))
                    consumed_chars = 0
                    auto_fetch_results: list[dict[str, Any]] = []

                    # Step 1: Official domains (deep)
                    # 第 1 步：官方域名（深度抓取）
                    for domain in official_domains[:3]:
                        if consumed_chars >= total_chars_cap:
                            break
                        if not domain or not isinstance(domain, str):
                            continue
                        official_url = f"https://{domain}" if not domain.startswith("http") else domain
                        try:
                            # Call web_fetch for single-page content
                            # NOTE: For multi-page, caller should use web_subpage_fetch instead
                            # 调用 web_fetch 抓取单页内容
                            # 注意：多页抓取请调用方改用 web_subpage_fetch
                            fetch_res = await fetcher.execute(
                                session_context=session_context,
                                url=official_url,
                                max_length=fetch_max_length,
                            )
                            fetched_content = fetch_res.output if fetch_res.success else None
                            if isinstance(fetched_content, str):
                                consumed_chars += len(fetched_content)
                            auto_fetch_results.append(
                                {
                                    "url": official_url,
                                    "title": f"Official: {domain}",
                                    "is_official": True,
                                    "fetch_success": fetch_res.success,
                                    "fetch_error": fetch_res.error,
                                    "fetched_content": fetched_content,
                                }
                            )
                        except Exception as e:
                            logger.warning(f"Auto-fetch failed for official domain {official_url}: {e}")
                            auto_fetch_results.append(
                                {
                                    "url": official_url,
                                    "title": f"Official: {domain}",
                                    "is_official": True,
                                    "fetch_success": False,
                                    "fetch_error": str(e),
                                    "fetched_content": None,
                                }
                            )

                    # Step 2: Top-K third-party results (shallow by default)
                    # 第 2 步：Top-K 第三方结果（默认浅抓）
                    for result in results[:auto_fetch_top_k]:
                        if consumed_chars >= total_chars_cap:
                            break
                        url = result.get("url")
                        if not url or not isinstance(url, str):
                            auto_fetch_results.append(
                                {
                                    "url": url,
                                    "title": result.get("title"),
                                    "is_official": False,
                                    "fetch_success": False,
                                    "fetch_error": "Missing URL for auto-fetch",
                                    "fetched_content": None,
                                }
                            )
                            continue
                        try:
                            # Call web_fetch for single-page content
                            # 调用 web_fetch 抓取单页内容
                            fetch_res = await fetcher.execute(
                                session_context=session_context,
                                url=url,
                                max_length=fetch_max_length,
                            )
                            fetched_content = fetch_res.output if fetch_res.success else None
                            if isinstance(fetched_content, str):
                                consumed_chars += len(fetched_content)
                            auto_fetch_results.append(
                                {
                                    "url": url,
                                    "title": result.get("title"),
                                    "is_official": False,
                                    "fetch_success": fetch_res.success,
                                    "fetch_error": fetch_res.error,
                                    "fetched_content": fetched_content,
                                }
                            )
                        except Exception as e:
                            logger.warning(f"Auto-fetch failed for {url}: {e}")
                            auto_fetch_results.append(
                                {
                                    "url": url,
                                    "title": result.get("title"),
                                    "is_official": False,
                                    "fetch_success": False,
                                    "fetch_error": str(e),
                                    "fetched_content": None,
                                }
                            )

                    auto_fetch_meta = {
                        "auto_fetch_results": auto_fetch_results,
                        "auto_fetch_top_k": auto_fetch_top_k,
                        "auto_fetch_max_length": fetch_max_length,
                        "auto_fetch_total_chars": consumed_chars,
                    }

            provider_value = self._provider_enum_value or "serpapi"
            search_cost = SEARCH_PROVIDER_COSTS.get(provider_value, 0.001)
            cost_model = SEARCH_PROVIDER_MODELS.get(provider_value, "shannon_web_search")

            return ToolResult(
                success=True,
                output={
                    "provider": self.provider.__class__.__name__,
                    "query": query,
                    "results": results,
                    "result_count": len(results),
                    "tool_source": "search",  # Citation V2: mark as search-origin 注：Citation V2：标记来源为 search
                },
                metadata={
                    "query": query,
                    "provider": self.provider.__class__.__name__,
                    "result_count": len(results),
                    "auto_fetch": auto_fetch_meta,
                },
                cost_usd=search_cost,
                cost_model=cost_model,
            )

        except ValueError as e:
            # Configuration errors - these are safe to show
            # 配置类错误——可以安全展示
            logger.error(f"Search configuration error: {e}")
            return ToolResult(
                success=False,
                output=None,
                error=f"Search configuration error: {str(e)}",
            )
        except Exception as e:
            # Runtime errors - sanitize these
            # 运行时错误——需先清洗
            sanitized_error = WebSearchProvider.sanitize_error_message(str(e))
            logger.error(
                f"Search failed with {self.provider.__class__.__name__}: {sanitized_error}"
            )

            # Return user-friendly error message
            # 返回对用户友好的错误信息
            error_message = str(e)
            if (
                "temporarily unavailable" in error_message
                or "rate limit" in error_message
                or "authentication failed" in error_message
            ):
                # These are already sanitized messages from our providers
                # 这些已是各 provider 清洗过的消息
                return ToolResult(success=False, output=None, error=error_message)
            else:
                # Generic error for unexpected failures
                # 意外失败的通用错误
                return ToolResult(
                    success=False,
                    output=None,
                    error="Search service encountered an error. Please try again later.",
                )
