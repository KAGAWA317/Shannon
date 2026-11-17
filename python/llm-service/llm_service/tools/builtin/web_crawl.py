"""
Web Crawl Tool - Exploratory multi-page crawling
Web Crawl tool——探索式多页爬取

Uses Firecrawl Crawl API for automatic link discovery and content extraction.
This is an async operation that may take 30-60 seconds.
使用 Firecrawl Crawl API 自动发现链接并提取内容。
这是异步操作，可能需要 30-60 秒。

Use Cases:
- Unknown website structure
- Discovering what content exists
- Sites with dynamic/nested navigation
使用场景：
- 网站结构未知
- 需要发现存在哪些内容
- 具有动态/嵌套导航的站点

For targeted page extraction where you know the paths, use web_subpage_fetch instead.
若已知具体路径、需要定向提取页面，请改用 web_subpage_fetch。
"""

import asyncio
import logging
import os
from typing import Any
from urllib.parse import urlparse

import aiohttp

from ..base import Tool, ToolMetadata, ToolParameter, ToolParameterType, ToolResult
from ..openapi_parser import _is_private_ip
from .web_fetch import (  # P0-A: Reuse blocked detection and noise cleaning logic 注：P0-A：复用拦截检测与噪声清理逻辑
    EXTRACTION_INTERNAL_MAX,
    apply_extraction,
    apply_research_truncation,
    clean_markdown_noise,
    detect_blocked_reason,
)

logger = logging.getLogger(__name__)

# Constants
# 常量
# MAX_LIMIT is the hard cap on crawled pages per call. 20 was sized for 200K-
# context defaults; raising to 100 to support "crawl this small docs site"
# workloads on 1M-context models. DEFAULT_LIMIT stays 10 (still a sane default
# for typical use) — callers must opt in to higher values via the explicit
# `limit` parameter.
# MAX_LIMIT 是单次调用可爬取页面的硬上限。20 是按 200K context 默认值定的；
# 提升到 100 以支持 1M context 模型上的“爬取这个小文档站”类工作负载。
# DEFAULT_LIMIT 仍为 10（对典型使用仍是合理默认值）——调用方需通过显式
# `limit` 参数主动选择更高值。
MAX_LIMIT = 100
DEFAULT_LIMIT = 10
DEFAULT_MAX_LENGTH = 8000
CRAWL_TIMEOUT = int(os.getenv("WEB_FETCH_CRAWL_TIMEOUT", "120"))
POLL_INTERVAL = 2  # seconds between status checks 注：状态检查间隔（秒）
MAX_POLL_ATTEMPTS = 60  # 60 * 2s = 2 minutes max 注：60 * 2s = 最多 2 分钟


class WebCrawlTool(Tool):
    """
    Crawl a website to discover and extract content from multiple pages.
    爬取网站以发现并提取多个页面的内容。

    Uses Firecrawl Crawl API for automatic link discovery.
    This is async and may take 30-60 seconds.
    使用 Firecrawl Crawl API 自动发现链接。
    这是异步操作，可能需要 30-60 秒。
    """

    def __init__(self):
        self.firecrawl_api_key = os.getenv("FIRECRAWL_API_KEY")

        # Validate Firecrawl key
        # 校验 Firecrawl key
        self.firecrawl_available = bool(
            self.firecrawl_api_key and
            len(self.firecrawl_api_key.strip()) >= 10 and
            self.firecrawl_api_key.lower() not in ["test", "demo", "xxx"]
        )

        if self.firecrawl_available:
            logger.info("WebCrawlTool initialized with Firecrawl provider")
        else:
            logger.warning("WebCrawlTool: Firecrawl not available")

        super().__init__()

    def _get_metadata(self) -> ToolMetadata:
        return ToolMetadata(
            name="web_crawl",
            version="1.0.0",
            description=(
                "Widely crawl a website to map its structure and retrieve content "
                "Use this for broad information gathering where the tool automatically navigates through links (async operation)."
                "\n\n"
                "USE WHEN:\n"
                "• You want to capture a wide range of content from a domain (e.g. 'audit this company', 'read all blog posts')\n"
                "• Navigation paths are complex or unknown (e.g. nested documentation, paginated articles)\n"
                "• You prioritize content coverage over speed\n"
                "\n"
                "NOT FOR:\n"
                "• Looking for specific content (use web_subpage_fetch)\n"
                "• Single page retrieval (use web_fetch instead)\n"
                "\n"
                "Returns: {url, title, content, method, pages_fetched, word_count, char_count, metadata}. "
                "Metadata includes total_crawled, unique_pages, and urls array. Content is merged markdown."
                "\n\n"
                "Example usage:\n"
                "• Explore unknown startup: url='https://unknown-startup.com', limit=15\n"
                "• Discover research lab structure: url='https://research-lab.edu', limit=20\n"
                "• Crawl personal homepage: url='https://johndoe.com', limit=10"
                "\n\n"
                "NOTE: This is async and may take 30-60 seconds."
            ),
            category="retrieval",
            author="Shannon",
            requires_auth=False,
            rate_limit=10,  # Lower rate limit due to resource intensive 注：资源消耗较大，因此 rate limit 更低
            timeout_seconds=180,  # 3 minutes max 注：最长 3 分钟
            memory_limit_mb=512,
            sandboxed=False,
            dangerous=False,
            cost_per_use=0.01,  # Higher cost for crawl 注：crawl 成本更高
        )

    def _get_parameters(self) -> list[ToolParameter]:
        return [
            ToolParameter(
                name="url",
                type=ToolParameterType.STRING,
                description="Starting URL to crawl (e.g., https://example.com)",
                required=True,
            ),
            ToolParameter(
                name="limit",
                type=ToolParameterType.INTEGER,
                description=f"Maximum number of pages to crawl (1-{MAX_LIMIT}). Default: {DEFAULT_LIMIT}",
                required=False,
                default=DEFAULT_LIMIT,
                min_value=1,
                max_value=MAX_LIMIT,
            ),
            ToolParameter(
                name="max_length",
                type=ToolParameterType.INTEGER,
                description="Maximum content length per page in characters",
                required=False,
                default=DEFAULT_MAX_LENGTH,
                min_value=1000,
                max_value=30000,
            ),
            ToolParameter(
                name="extract_prompt",
                type=ToolParameterType.STRING,
                description=(
                    "When set, uses a small model to extract relevant information "
                    "instead of blind truncation. Provide what you need from the page. "
                    "Example: 'Extract all blog post titles, dates, and summaries'"
                ),
                required=False,
            ),
        ]

    async def _execute_impl(
        self, session_context: dict | None = None, **kwargs
    ) -> ToolResult:
        """Execute exploratory crawl using Firecrawl Crawl API.
        使用 Firecrawl Crawl API 执行探索式爬取。"""
        url = kwargs.get("url")
        limit = kwargs.get("limit", DEFAULT_LIMIT)
        max_length = kwargs.get("max_length", DEFAULT_MAX_LENGTH)
        extract_prompt = kwargs.get("extract_prompt")  # Optional: targeted extraction query 注：可选：定向提取查询

        # Skip auto-extraction in research mode (issue #43): OODA loop does many
        # fast fetches; LLM extraction adds ~40-60s latency per call, causing timeout.
        # Explicit extract_prompt always triggers extraction regardless of mode.
        # research 模式下跳过自动提取（issue #43）：OODA 循环会做大量快速
        # fetch；LLM 提取每次调用增加约 40-60s 延迟，导致 timeout。
        # 显式传入 extract_prompt 时，无论何种模式都会触发提取。
        _research_mode = (
            isinstance(session_context, dict)
            and session_context.get("research_mode")
            and not extract_prompt
        )

        if _research_mode:
            internal_max_length = max_length
        else:
            internal_max_length = EXTRACTION_INTERNAL_MAX

        if not url:
            return ToolResult(success=False, output=None, error="URL parameter required")

        # Validate URL
        # 校验 URL
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return ToolResult(success=False, output=None, error=f"Invalid URL: {url}")
            if parsed.scheme not in ["http", "https"]:
                return ToolResult(success=False, output=None, error="Only HTTP/HTTPS allowed")
            # SSRF protection: Block private/internal IPs
            # SSRF 防护：拦截私有/内部 IP
            host = parsed.hostname
            if host and _is_private_ip(host):
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Access to private/internal IP addresses is not allowed: {host}",
                )
        except Exception as e:
            return ToolResult(success=False, output=None, error=f"Invalid URL: {e}")

        if not self.firecrawl_available:
            return ToolResult(
                success=False,
                output=None,
                error="web_crawl requires Firecrawl API. Configure FIRECRAWL_API_KEY.",
                metadata={
                    "provider": "firecrawl",
                    "strategy": "crawl",
                    "partial_success": False,
                    "urls_attempted": [url],
                    "urls_succeeded": [],
                    "urls_failed": [{"url": url, "reason": "firecrawl_not_configured"}],
                    "failure_summary": {"failed_count": 1, "total_count": 1},
                },
            )

        try:
            result = await self._crawl(url, limit, internal_max_length)
            tool_result = ToolResult(
                success=True,
                output=result,
                metadata={
                    "provider": "firecrawl",
                    "strategy": "crawl",
                    "partial_success": False,
                    "urls_attempted": [url],
                    "urls_succeeded": [url],
                    "urls_failed": [],
                    "failure_summary": {"failed_count": 0, "total_count": 1},
                },
            )
            if _research_mode:
                return apply_research_truncation(tool_result, max_length)
            return await apply_extraction(tool_result, extract_prompt, max_length)
        except Exception as e:
            logger.error(f"Crawl failed: {e}")
            return ToolResult(
                success=False,
                output=None,
                error=f"Crawl failed: {str(e)[:200]}",
                metadata={
                    "provider": "firecrawl",
                    "strategy": "crawl",
                    "partial_success": False,
                    "urls_attempted": [url],
                    "urls_succeeded": [],
                    "urls_failed": [{"url": url, "reason": str(e)[:200]}],
                    "failure_summary": {"failed_count": 1, "total_count": 1},
                },
            )

    async def _crawl(self, url: str, limit: int, max_length: int) -> dict[str, Any]:
        """
        Execute async crawl using Firecrawl Crawl API.
        使用 Firecrawl Crawl API 执行异步爬取。

        Steps:
        1. Start crawl job
        2. Poll for completion
        3. Collect and merge results
        步骤：
        1. 启动爬取任务
        2. 轮询直到完成
        3. 收集并合并结果
        """
        timeout = aiohttp.ClientTimeout(total=CRAWL_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Step 1: Start crawl
            # 步骤 1：启动爬取
            crawl_id = await self._start_crawl(session, url, limit)
            logger.info(f"Started crawl job {crawl_id} for {url}")

            # Step 2: Poll for completion
            # 步骤 2：轮询直到完成
            results = await self._poll_crawl(session, crawl_id)
            if not results:
                raise Exception("Crawl returned no results")

            logger.info(f"Crawl completed with {len(results)} pages")

            # Step 3: Merge results (enforce limit)
            # 步骤 3：合并结果（强制 limit）
            return self._merge_results(results, url, max_length, limit)

    async def _start_crawl(self, session: aiohttp.ClientSession, url: str, limit: int) -> str:
        """Start a crawl job and return the crawl ID.
        启动爬取任务并返回 crawl ID。"""
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}",
            "Content-Type": "application/json"
        }
        payload = {
            "url": url,
            "limit": limit,
            "scrapeOptions": {
                "formats": ["markdown"],
                "parsers": ["pdf"],
                "onlyMainContent": True,
                # Note: 'form' removed - some sites wrap main content in form tags
                # 注意：已移除 'form'——部分站点会用 form 标签包裹主要内容
                "excludeTags": ["nav", "footer", "aside", "svg", "script", "style", "noscript"],
                "removeBase64Images": True,
                "blockAds": True,
            },
        }

        async with session.post(
            "https://api.firecrawl.dev/v2/crawl",
            json=payload,
            headers=headers
        ) as response:
            if response.status != 200:
                error = await response.text()
                raise Exception(f"Failed to start crawl ({response.status}): {error[:200]}")

            data = await response.json()
            crawl_id = data.get("id")
            if not crawl_id:
                raise Exception("Crawl API did not return job ID")

            return crawl_id

    async def _poll_crawl(self, session: aiohttp.ClientSession, crawl_id: str) -> list[dict]:
        """Poll crawl status until completion.
        轮询 crawl 状态直到完成。"""
        headers = {
            "Authorization": f"Bearer {self.firecrawl_api_key}"
        }

        all_results = []

        for attempt in range(MAX_POLL_ATTEMPTS):
            async with session.get(
                f"https://api.firecrawl.dev/v2/crawl/{crawl_id}",
                headers=headers
            ) as response:
                if response.status != 200:
                    error = await response.text()
                    logger.warning(f"Poll error ({response.status}): {error[:100]}")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                data = await response.json()
                status = data.get("status", "unknown")

                # Collect results
                # 收集结果
                page_data = data.get("data", [])
                if page_data:
                    all_results.extend(page_data)

                if status in ["completed", "failed"]:
                    if status == "failed":
                        logger.warning(f"Crawl {crawl_id} failed")
                    break

                # Continue polling
                # 继续轮询
                await asyncio.sleep(POLL_INTERVAL)

        return all_results

    def _merge_results(self, results: list[dict], original_url: str, max_length: int, limit: int) -> dict:
        """Merge crawl results into a single output.
        将 crawl 结果合并为单一输出。"""
        # Filter and deduplicate
        # 过滤并去重
        seen_urls = set()
        unique_results = []
        for r in results:
            url = r.get("metadata", {}).get("url", r.get("url", ""))
            if url and url not in seen_urls:
                seen_urls.add(url)
                unique_results.append(r)

        if not unique_results:
            raise Exception("No valid pages in crawl results")

        # Enforce limit to prevent excessive data processing
        # 强制 limit，避免处理过多数据
        if len(unique_results) > limit:
            logger.info(f"Truncating {len(unique_results)} pages to limit={limit}")
            unique_results = unique_results[:limit]

        if len(unique_results) == 1:
            r = unique_results[0]
            content = r.get("markdown", "")
            content = clean_markdown_noise(content)  # Clean noise before truncation 注：截断前先清理噪声
            if len(content) > max_length:
                content = content[:max_length]
            # P0-A: Detect blocked content for Citation V2 filtering
            # P0-A：为 Citation V2 过滤检测被拦截的内容
            blocked_reason = detect_blocked_reason(content, 200)
            return {
                "url": r.get("metadata", {}).get("url", original_url),
                "title": r.get("metadata", {}).get("title", ""),
                "content": content,
                "method": "firecrawl_crawl",
                "pages_fetched": 1,
                "word_count": len(content.split()),
                "char_count": len(content),
                "tool_source": "fetch",  # Citation V2: mark as fetch-origin 注：Citation V2：标记为 fetch 来源
                "status_code": 200,  # P0-A: Firecrawl crawl success = 200 注：P0-A：Firecrawl crawl 成功即 200
                "blocked_reason": blocked_reason,  # P0-A: Content-based detection 注：P0-A：基于内容检测
            }

        # Multiple pages - merge with markdown separators
        # 多页——用 markdown 分隔符合并
        merged_content = []
        total_chars = 0
        char_budget = max_length * len(unique_results)  # Total budget across pages 注：所有页面的总 budget

        for i, r in enumerate(unique_results):
            metadata = r.get("metadata", {})
            page_url = metadata.get("url", "")
            page_title = metadata.get("title", "")
            page_content = r.get("markdown", "")
            page_content = clean_markdown_noise(page_content)  # Clean noise 注：清理噪声

            # Per-page truncation
            # 单页截断
            if len(page_content) > max_length:
                page_content = page_content[:max_length]

            if i == 0:
                merged_content.append(f"# Main Page: {page_url}\n")
                if page_title:
                    merged_content.append(f"**{page_title}**\n")
            else:
                merged_content.append(f"\n---\n\n## Page {i+1}: {page_url}\n")
                if page_title:
                    merged_content.append(f"**{page_title}**\n")

            merged_content.append(f"\n{page_content}\n")
            total_chars += len(page_content)

            # Stop if we've hit the char budget
            # 达到字符 budget 即停止
            if total_chars >= char_budget:
                break

        final_content = "".join(merged_content)
        # P0-A: Detect blocked content in merged result
        # P0-A：在合并结果中检测被拦截的内容
        blocked_reason = detect_blocked_reason(final_content, 200)

        return {
            "url": original_url,
            "title": unique_results[0].get("metadata", {}).get("title", ""),
            "content": final_content,
            "method": "firecrawl_crawl",
            "pages_fetched": len(unique_results),
            "word_count": len(final_content.split()),
            "char_count": total_chars,
            "tool_source": "fetch",  # Citation V2: mark as fetch-origin 注：Citation V2：标记为 fetch 来源
            "status_code": 200,  # P0-A: Firecrawl crawl success = 200 注：P0-A：Firecrawl crawl 成功即 200
            "blocked_reason": blocked_reason,  # P0-A: Content-based detection 注：P0-A：基于内容检测
            "metadata": {
                "total_crawled": len(results),
                "unique_pages": len(unique_results),
                "urls": list(seen_urls)
            }
        }
