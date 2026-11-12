"""
Web Subpage Fetch Tool - Intelligent multi-page extraction from a website
Web Subpage Fetch tool——从网站智能提取多个页面

Uses Firecrawl Map + Scrape strategy:
1. Map API: Get all URLs on the website (fast, up to 200 URLs)
2. Score URLs by relevance (path matching, depth, keywords)
3. Batch scrape top N most relevant pages
采用 Firecrawl Map + Scrape 策略：
1. Map API：获取网站上的所有 URL（快速，最多 200 个 URL）
2. 按相关性给 URL 打分（路径匹配、深度、关键词）
3. 批量 scrape 相关性最高的前 N 个页面

Use Cases:
- Company research: /about, /team, /ir, /products
- Documentation: /docs, /api, /guides
- Known domain with specific target pages
使用场景：
- 公司调研：/about、/team、/ir、/products
- 文档：/docs、/api、/guides
- 已知域名且目标页面明确

For exploratory crawling where structure is unknown, use web_crawl instead.
若结构未知、需要探索式爬取，请改用 web_crawl。
"""

import aiohttp
import asyncio
import os
import logging
import socket
from typing import Dict, Optional, List, Any, Tuple
from urllib.parse import urlparse

from ..base import Tool, ToolMetadata, ToolParameter, ToolParameterType, ToolResult
from ..openapi_parser import _is_private_ip
from .web_fetch import detect_blocked_reason, clean_markdown_noise, apply_extraction, apply_research_truncation, EXTRACTION_INTERNAL_MAX  # P0-A: Reuse blocked detection and noise cleaning logic
# P0-A：复用拦截检测与噪声清理逻辑

logger = logging.getLogger(__name__)

# Constants
# 常量
# MAX_LIMIT cap on pages per web_subpage_fetch call. Parallel sibling to
# web_crawl's MAX_LIMIT — keep them aligned. See web_crawl.py for rationale.
# MAX_LIMIT 是单次 web_subpage_fetch 调用可获取页面的上限。与
# web_crawl 的 MAX_LIMIT 是平行对应关系——请保持两者一致。理由见 web_crawl.py。
MAX_LIMIT = 100
MAP_URL_LIMIT = 200  # Max URLs from map API
# map API 返回的最大 URL 数
DEFAULT_LIMIT = 5
DEFAULT_MAX_LENGTH = 10000
BATCH_CONCURRENCY = int(os.getenv("WEB_FETCH_BATCH_CONCURRENCY", "3"))  
SCRAPE_TIMEOUT = int(os.getenv("WEB_FETCH_SCRAPE_TIMEOUT", "30"))
MAP_TIMEOUT = int(os.getenv("WEB_FETCH_MAP_TIMEOUT", "45"))

# Retry configuration
# retry 配置
RETRY_CONFIG = {
    408: {"max_retries": 3, "delays": [2, 4, 8]},      # Timeout: exponential backoff
    # timeout：指数退避
    429: {"max_retries": 2, "delays": [5, 10]},       # Rate limit: reduced retries
    # rate limit：减少 retry 次数
    500: {"max_retries": 2, "delays": [1, 2]},         # Server error: quick retry
    # 服务端错误：快速 retry
    502: {"max_retries": 2, "delays": [2, 4]},         # Bad gateway
    # 网关错误
    503: {"max_retries": 2, "delays": [3, 6]},         # Service unavailable
    # 服务不可用
}

# Base keywords for relevance scoring (always applied)
# 相关性打分的基础关键词（始终生效）
BASE_KEYWORDS = [
    "about", "team", "company", "product", "pricing", "docs", "api",
    "contact", "careers", "leadership", "services", "features", "solutions", "overview"
]

# High-value content paths - these often contain important announcements/updates
# Boosted because they may have deeper depth but high information value
# 高价值内容路径——这些路径常包含重要公告/更新
# 因其深度可能更大，但信息价值高，故给予加权
HIGH_VALUE_PATHS = [
    "blog", "news", "press", "ir", "investors", "investor-relations",
    "announcements", "updates", "releases", "articles", "insights"
]

# Keyword to path mappings for semantic expansion
# When user provides a keyword, we also match related path patterns
# 关键词到路径的映射，用于语义扩展
# 用户提供关键词时，我们也会匹配相关的路径 pattern
KEYWORD_SYNONYMS = {
    "api": ["api", "api-reference", "reference", "api-docs", "developer"],
    "doc": ["docs", "documentation", "guide", "guides", "manual", "help"],
    "about": ["about", "about-us", "company", "who-we-are", "overview"],
    "team": ["team", "people", "leadership", "management", "our-team", "founders", "executives"],
    "product": ["products", "product", "features", "solutions", "offerings", "platform"],
    "pricing": ["pricing", "plans", "price", "packages", "cost"],
    "contact": ["contact", "contact-us", "reach-us", "get-in-touch"],
    "career": ["careers", "jobs", "join-us", "opportunities", "hiring"],
    "news": ["blog", "news", "articles", "insights", "posts", "press", "announcements"],
    "investor": ["ir", "investors", "investor-relations", "stockholders", "financials"],
    "funding": ["funding", "investment", "series", "raise", "investors"],
}


class WebSubpageFetchTool(Tool):
    """
    Fetch multiple pages from a website using intelligent selection.
    从网站智能选择并获取多个页面。

    Uses Map + Scrape strategy with relevance scoring to select
    the most important pages from a website.
    使用 Map + Scrape 策略，结合相关性打分选出网站中最重要的页面。
    """

    def __init__(self):
        self.firecrawl_api_key = os.getenv("FIRECRAWL_API_KEY")
        self.exa_api_key = os.getenv("EXA_API_KEY")
        self.preferred_provider = os.getenv("WEB_FETCH_PROVIDER", "firecrawl").lower()

        # Validate Firecrawl key
        # 校验 Firecrawl key
        self.firecrawl_available = bool(
            self.firecrawl_api_key and
            len(self.firecrawl_api_key.strip()) >= 10 and
            self.firecrawl_api_key.lower() not in ["test", "demo", "xxx"]
        )

        if self.firecrawl_available:
            logger.info("WebSubpageFetchTool initialized with Firecrawl provider")
        else:
            logger.warning("WebSubpageFetchTool: Firecrawl not available, will use fallback")

        super().__init__()

    def _get_metadata(self) -> ToolMetadata:
        return ToolMetadata(
            name="web_subpage_fetch",
            version="1.0.0",
            description=(
                "Fetch multiple pages from a known website using intelligent selection via Map API (Firecrawl). "
                "Implements Firecrawl Map (discover URLs) + Scrape (fetch content) strategy. "
                "Discovers all URLs, then scores and selects the most relevant pages to fetch based on your targets."
                "\n\n"
                "USE WHEN:\n"
                "• You have a specific domain and need specific sections (e.g., 'check company team', 'find API docs')\n"
                "• You want to efficiently grab the most important pages without crawling everything\n"
                "\n"
                "NOT FOR:\n"
                "• Blind exploration of unknown sites (use web_crawl instead)\n"
                "• Single page fetching (use web_fetch instead)\n"
                "\n"
                "Returns: {url, title, content, method, pages_fetched, word_count, char_count}. "
                "Content is merged markdown when multiple pages are fetched."
                "\n\n"
                "Example usage:\n"
                "• Research OpenAI: url='https://openai.com', limit=15, "
                "target_paths=['/about', '/our-team', '/board', '/careers', '/research', '/blog', '/product', '/company', '/leadership', '/pricing']\n"
                "• Find Stripe API docs: url='https://stripe.com', limit=10, target_keywords='API documentation developer reference'\n"
                "• Tesla investor relations: url='https://ir.tesla.com', limit=8, "
                "target_paths=['/news', '/press', '/financials', '/events', '/governance', '/stock']"
            ),
            category="retrieval",
            author="Shannon",
            requires_auth=False,
            rate_limit=20,
            timeout_seconds=120,
            memory_limit_mb=256,
            sandboxed=False,
            dangerous=False,
            cost_per_use=0.005,  # Multiple pages
            # 多个页面
        )

    def _get_parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="url",
                type=ToolParameterType.STRING,
                description="Base URL of the website (e.g., https://example.com)",
                required=True,
            ),
            ToolParameter(
                name="limit",
                type=ToolParameterType.INTEGER,
                description=f"Maximum number of pages to fetch (1-{MAX_LIMIT}). Default: {DEFAULT_LIMIT}",
                required=False,
                default=DEFAULT_LIMIT,
                min_value=1,
                max_value=MAX_LIMIT,
            ),
            ToolParameter(
                name="target_keywords",
                type=ToolParameterType.STRING,
                description=(
                    "Space-separated keywords describing what content to prioritize. "
                    "Examples: 'about team funding news products'. "
                    "URLs containing these keywords (or synonyms) get higher scores. "
                    "High-value paths like /blog, /news, /press are automatically boosted."
                ),
                required=False,
            ),
            ToolParameter(
                name="target_paths",
                type=ToolParameterType.ARRAY,
                description=(
                    "List of URL paths to prioritize. Examples: "
                    "[\"/about\", \"/team\", \"/investors\", \"/docs\"]. "
                    "Matches exact paths or subpaths; can include full URLs (path will be extracted)."
                ),
                required=False,
            ),
            ToolParameter(
                name="max_length",
                type=ToolParameterType.INTEGER,
                description="Maximum content length per page in characters",
                required=False,
                default=DEFAULT_MAX_LENGTH,
                min_value=1000,
                max_value=50000,
            ),
            ToolParameter(
                name="extract_prompt",
                type=ToolParameterType.STRING,
                description=(
                    "When set, uses a small model to extract relevant information "
                    "instead of blind truncation. Provide what you need from the page. "
                    "Example: 'Extract team members, roles, and company history'"
                ),
                required=False,
            ),
        ]

    async def _execute_impl(
        self, session_context: Optional[Dict] = None, **kwargs
    ) -> ToolResult:
        """Execute multi-page fetch using Map + Scrape strategy.
        使用 Map + Scrape 策略执行多页获取。"""
        url = kwargs.get("url")
        limit = kwargs.get("limit", DEFAULT_LIMIT)
        target_keywords = kwargs.get("target_keywords", "")
        raw_target_paths = kwargs.get("target_paths") or []
        if isinstance(raw_target_paths, str):
            target_paths = [raw_target_paths]
        elif isinstance(raw_target_paths, list):
            target_paths = raw_target_paths
        else:
            target_paths = []
        target_paths = [p for p in target_paths if isinstance(p, str)]
        max_length = kwargs.get("max_length", DEFAULT_MAX_LENGTH)
        extract_prompt = kwargs.get("extract_prompt")  # Optional: targeted extraction query
        # 可选：定向提取查询

        # Skip ALL LLM extraction in research mode (issue #43): OODA loop does many
        # fast fetches; extraction adds ~40-60s latency per call and 40%+ of total cost.
        # OODA's Observe/Orient steps handle raw content analysis — small-model
        # pre-filtering undermines the agent's own judgment loop.
        # extract_prompt from LLM is also ignored in research mode.
        # research mode 下跳过所有 LLM 提取（issue #43）：OODA 循环会做大量快速
        # fetch；提取每次调用增加约 40-60s 延迟，并占总成本的 40%+。
        # OODA 的 Observe/Orient 步骤会自行分析原始内容——小模型预过滤
        # 会削弱 agent 自身的判断循环。
        # research mode 下也会忽略 LLM 传入的 extract_prompt。
        _research_mode = (
            isinstance(session_context, dict)
            and bool(session_context.get("research_mode"))
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
            # DNS resolution guard to avoid wasted provider calls
            # DNS 解析预检，避免浪费 provider 调用
            host = parsed.hostname
            if not host:
                return ToolResult(success=False, output=None, error="Invalid host in URL")
            # SSRF protection: Block private/internal IPs
            # SSRF 防护：拦截私有/内部 IP
            if _is_private_ip(host):
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Access to private/internal IP addresses is not allowed: {host}",
                )
            try:
                socket.getaddrinfo(host, 443)
            except Exception as e:
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"DNS resolution failed for host {host}: {e}",
                )
        except Exception as e:
            return ToolResult(success=False, output=None, error=f"Invalid URL: {e}")

        last_error = ""

        # Execute with Firecrawl (primary) or fallback
        # 优先使用 Firecrawl 执行，否则走回退
        if self.firecrawl_available:
            try:
                result, scrape_meta = await self._map_and_scrape(
                    url, limit, target_keywords, target_paths, internal_max_length
                )
                tool_result = ToolResult(
                    success=True,
                    output=result,
                    metadata={
                        "provider": "firecrawl",
                        "strategy": "map_and_scrape",
                        **scrape_meta,
                    }
                )
                if _research_mode:
                    return apply_research_truncation(tool_result, max_length)
                return await apply_extraction(tool_result, extract_prompt, max_length)
            except Exception as e:
                last_error = f"Firecrawl map+scrape failed: {e}"
                logger.error(last_error)
                # Fall through to Exa or error
                # 继续落到 Exa 或返回错误

        # Fallback: Exa with subpages (if available)
        # 回退：使用 Exa 的 subpages 能力（若可用）
        if self.exa_api_key:
            try:
                result = await self._fetch_with_exa(
                    url, limit, target_keywords, target_paths, internal_max_length
                )
                tool_result = ToolResult(
                    success=True,
                    output=result,
                    metadata={
                        "provider": "exa",
                        "strategy": "subpage_search",
                        "urls_requested": [url],
                        "urls_attempted": [url],
                        "urls_succeeded": [url],
                        "urls_failed": [],
                        "partial_success": False,
                        "failure_summary": {"failed_count": 0, "total_count": 1},
                    }
                )
                if _research_mode:
                    return apply_research_truncation(tool_result, max_length)
                return await apply_extraction(tool_result, extract_prompt, max_length)
            except Exception as e:
                error_msg = f"Exa fallback failed: {e}"
                last_error = f"{last_error}; {error_msg}" if last_error else error_msg
                logger.error(error_msg)

        if not last_error:
            if not self.firecrawl_available and not self.exa_api_key:
                last_error = "Firecrawl/Exa not configured"
            else:
                last_error = "No provider returned content"

        return ToolResult(
            success=False,
            output=None,
            error=f"web_subpage_fetch failed: {last_error}",
            metadata={
                "provider": "firecrawl" if self.firecrawl_available else "exa",
                "strategy": "map_and_scrape",
                "urls_requested": [],
                "urls_attempted": [],
                "urls_succeeded": [],
                "urls_failed": [],
                "partial_success": False,
                "failure_summary": {"failed_count": 0, "total_count": 0},
            },
        )

    def _expand_keywords(self, target_keywords: str) -> set:
        """Expand target keywords using synonym mappings.
        使用同义词映射扩展目标关键词。"""
        if not target_keywords:
            return set()

        expanded = set()
        words = target_keywords.lower().split()

        for word in words:
            expanded.add(word)
            # Add synonyms if this word is a key in KEYWORD_SYNONYMS
            # 若该词是 KEYWORD_SYNONYMS 的键，则加入其同义词
            if word in KEYWORD_SYNONYMS:
                expanded.update(KEYWORD_SYNONYMS[word])
            # Also check if word matches any synonym values
            # 同时检查该词是否匹配某个同义词值
            for key, synonyms in KEYWORD_SYNONYMS.items():
                if word in synonyms:
                    expanded.add(key)
                    expanded.update(synonyms)

        return expanded

    def _normalize_target_paths(self, target_paths: List[str]) -> List[str]:
        """Normalize target paths for matching.
        规范化用于匹配的目标路径。"""
        if not target_paths:
            return []

        normalized: List[str] = []
        seen: set = set()

        for raw in target_paths:
            if not raw:
                continue
            path = raw.strip()
            if not path:
                continue
            if "://" in path:
                parsed = urlparse(path)
                path = parsed.path or ""
            else:
                path = path.split("?", 1)[0].split("#", 1)[0]

            if not path.startswith("/"):
                path = "/" + path
            if path != "/":
                path = path.rstrip("/")
            path = path.lower()

            if path and path not in seen:
                seen.add(path)
                normalized.append(path)

        return normalized

    def _matches_target_paths(self, path: str, target_paths: List[str]) -> bool:
        if not target_paths:
            return False
        if not path:
            path = "/"
        for target in target_paths:
            if target == "/":
                if path in ("", "/"):
                    return True
                continue
            if path == target or path.startswith(target + "/"):
                return True
        return False

    def _calculate_relevance_score(
        self,
        url: str,
        target_keywords: str,
        total_pages: int,
        target_paths: Optional[List[str]] = None
    ) -> float:
        """
        Calculate relevance score for a URL (0.0-1.0).
        计算 URL 的相关性分数（0.0-1.0）。

        Scoring factors:
        - Target paths match: 0.5 weight (explicit path priority)
        - Target keywords match: 0.4 weight (user-specified priority)
        - High-value paths (blog/news/press): 0.25 weight (content-rich pages)
        - Base keywords match: 0.15 weight (common important pages)
        - URL depth: 0.15 weight (shallow = better, but less important now)
        - URL length: 0.05 weight (shorter = slightly better)
        打分因素：
        - 命中目标路径：权重 0.5（显式路径优先）
        - 命中目标关键词：权重 0.4（用户指定优先）
        - 高价值路径（blog/news/press）：权重 0.25（内容丰富的页面）
        - 命中基础关键词：权重 0.15（常见的重要页面）
        - URL 深度：权重 0.15（越浅越好，但如今重要性已降低）
        - URL 长度：权重 0.05（越短略优）
        """
        score = 0.0
        parsed = urlparse(url)
        path = parsed.path.lower().rstrip("/")
        if not path:
            path = "/"
        path_segments = path.split("/")
        target_paths = target_paths or []

        if target_paths and self._matches_target_paths(path, target_paths):
            score += 0.5

        # 1. Target keywords match (0.4 weight) - highest priority
        # 1. 命中目标关键词（权重 0.4）——最高优先级
        if target_keywords:
            expanded_keywords = self._expand_keywords(target_keywords)
            matched_keywords = []
            for keyword in expanded_keywords:
                if keyword in path:
                    matched_keywords.append(keyword)
            if matched_keywords:
                # More matches = higher score, up to 0.4
                # 命中越多分数越高，最多 0.4
                score += min(0.4, 0.15 * len(matched_keywords))

        # 2. High-value paths (0.25 weight) - blog/news/press are content-rich
        # 2. 高价值路径（权重 0.25）——blog/news/press 内容丰富
        for hv_path in HIGH_VALUE_PATHS:
            if hv_path in path:
                score += 0.25
                break

        # 3. Base keywords match (0.15 weight)
        # 3. 命中基础关键词（权重 0.15）
        for keyword in BASE_KEYWORDS:
            if keyword in path:
                score += 0.05
                if score >= 1.0:
                    break

        # 4. URL depth (0.15 weight) - shallow pages often important
        # 4. URL 深度（权重 0.15）——浅层页面往往重要
        depth = path.count("/") if path else 0
        if depth <= 1:
            score += 0.15
        elif depth == 2:
            score += 0.08
        elif depth == 3:
            score += 0.03
        # depth > 3: no bonus, but not penalized (may have good content)
        # 深度 > 3：不加分，但也不惩罚（可能有优质内容）

        # 5. URL length (0.05 weight) - minor factor
        # 5. URL 长度（权重 0.05）——次要因素
        if len(url) < 60:
            score += 0.05
        elif len(url) < 100:
            score += 0.02

        # Boost for large sites: top-level pages are more likely important
        # 大型站点加权：顶层页面更可能重要
        if total_pages > 50 and depth <= 1:
            score *= 1.1

        return min(score, 1.0)

    def _is_error_page(self, content: str) -> Tuple[bool, str]:
        """Detect if page content indicates an error page.
        检测页面内容是否表明这是一个错误页。

        Returns:
            (is_error, reason): True if error page, with reason string
        返回：
            (is_error, reason)：若为错误页则为 True，并附带原因字符串
        """
        if not content:
            return True, "empty content"

        content_lower = content.lower()
        content_len = len(content)

        # Pattern 1: Common error page indicators
        # Note: Removed "coming soon" and "under construction" - too common in normal websites
        # (product announcements, feature previews, etc.) causing false positives
        error_indicators = [
            ("whitelabel error", "spring/java error page"),
            ("404 not found", "404 error"),
            ("page not found", "404 error"),
            ("500 internal server error", "500 error"),
            ("502 bad gateway", "502 error"),
            ("503 service unavailable", "503 error"),
            ("403 forbidden", "403 error"),
            ("site not available", "site down"),
            ("website is under maintenance", "maintenance"),
        ]

        for pattern, reason in error_indicators:
            if pattern in content_lower:
                # For short pages, any error indicator is enough
                if content_len < 3000:
                    return True, reason
                # For longer pages, be more strict (might be docs about errors)

        # Pattern 2: Very short content with error keywords
        if content_len < 1000:
            error_keywords = ["error", "404", "not found", "forbidden", "unavailable"]
            if any(kw in content_lower for kw in error_keywords):
                return True, f"short page ({content_len} chars) with error keywords"

        return False, ""

    async def _map_and_scrape(
        self,
        url: str,
        limit: int,
        target_keywords: str,
        target_paths: List[str],
        max_length: int
    ) -> Tuple[Dict[str, Any], Dict]:
        """Map website URLs, score by relevance, and scrape top pages.

        Hybrid selection strategy:
        1. Keyword-matched URLs get priority (up to 60% of limit)
        2. Remaining quota filled by score-ranked URLs
        """
        # Step 1: Map to get all URLs
        all_urls = await self._map(url)
        if not all_urls:
            raise Exception("Map API returned no URLs")

        logger.info(f"Map returned {len(all_urls)} URLs for {url}")

        # Step 1.5: Filter out technical/low-value URLs
        skip_patterns = [
            'sitemap.xml', 'sitemap-', 'robots.txt', 'feed.xml', 'rss.xml',
            '.xml', '.json', '.css', '.js', '.png', '.jpg', '.gif', '.svg',
            '/wp-admin/', '/wp-includes/', '/wp-content/uploads/',
            '/cart', '/checkout', '/login', '/register', '/search',
            '/tag/', '/category/', '/page/', '/attachment/',
            '/thank-you', '/thanks', '/thankyou',  # Thank you pages
            '/unsubscribe', '/confirm', '/subscribe',  # Action confirmation pages
        ]
        filtered_urls = []
        skipped_count = 0
        for u in all_urls:
            path_lower = urlparse(u).path.lower()
            if any(skip in path_lower for skip in skip_patterns):
                skipped_count += 1
                continue
            filtered_urls.append(u)

        if skipped_count > 0:
            logger.info(f"Filtered out {skipped_count} technical/low-value URLs")
        all_urls = filtered_urls

        # Step 1.6: Expand keywords and normalize target paths for matching
        expanded_keywords = self._expand_keywords(target_keywords) if target_keywords else set()
        if expanded_keywords:
            logger.info(f"target_keywords: '{target_keywords}' -> expanded to {len(expanded_keywords)} terms: {list(expanded_keywords)[:10]}...")

        normalized_target_paths = self._normalize_target_paths(target_paths) if target_paths else []
        if normalized_target_paths:
            logger.info(f"target_paths normalized: {normalized_target_paths}")

        # Step 2: Hybrid selection - separate path matches, keyword matches, and others
        path_matched = []     # URLs matching explicit target paths
        keyword_matched = []  # URLs directly matching keywords
        other_urls = []       # All other URLs

        for u in all_urls:
            path_lower = urlparse(u).path.lower().rstrip("/")
            if not path_lower:
                path_lower = "/"

            score = self._calculate_relevance_score(
                u, target_keywords, len(all_urls), normalized_target_paths
            )

            if score < 0.05:
                continue

            is_path_match = self._matches_target_paths(path_lower, normalized_target_paths)
            is_keyword_match = any(kw in path_lower for kw in expanded_keywords)

            if is_path_match:
                path_matched.append((u, score))
            elif is_keyword_match:
                keyword_matched.append((u, score))
            else:
                other_urls.append((u, score))

        # Sort groups by score
        path_matched.sort(key=lambda x: x[1], reverse=True)
        keyword_matched.sort(key=lambda x: x[1], reverse=True)
        other_urls.sort(key=lambda x: x[1], reverse=True)

        # Step 3: Hybrid quota allocation (paths first; then 60/40 keyword/other)
        selected_from_paths = [u for u, _ in path_matched]
        selected_from_keywords = []
        selected_from_others = []

        if len(selected_from_paths) >= limit:
            selected_urls = selected_from_paths[:limit]
        else:
            remaining_quota = limit - len(selected_from_paths)
            keyword_quota = int(remaining_quota * 0.6)
            selected_from_keywords = [u for u, _ in keyword_matched[:keyword_quota]]
            remaining_quota = limit - len(selected_from_paths) - len(selected_from_keywords)
            selected_from_others = [u for u, _ in other_urls[:remaining_quota]]
            selected_urls = selected_from_paths + selected_from_keywords + selected_from_others

        if normalized_target_paths:
            logger.info(
                "Path selection: %d path-matched + %d keyword-matched + %d others = %d total",
                len(selected_from_paths),
                len(selected_from_keywords),
                len(selected_from_others),
                len(selected_urls),
            )
        else:
            logger.info(
                "Hybrid selection: %d keyword-matched + %d others = %d total",
                len(selected_from_keywords),
                len(selected_from_others),
                len(selected_urls),
            )

        # Always include the base URL if not already present
        base_url = url.rstrip("/")
        normalized_selected = [u.rstrip("/") for u in selected_urls]
        if base_url not in normalized_selected:
            selected_urls.insert(0, base_url)
            if len(selected_urls) > limit:
                selected_urls = selected_urls[:limit]

        logger.info(f"Selected {len(selected_urls)} URLs for scraping: {selected_urls}")

        # Step 3.5: Pre-check main page - fast fail if site is error/down
        main_result = await self._scrape_with_retry(base_url, max_length)
        if main_result:
            main_content = main_result.get("content", "")
            is_error, error_reason = self._is_error_page(main_content)

            if is_error:
                logger.warning(f"Main page is error page ({error_reason}), skipping subpage fetch: {base_url}")
                # Return early with error info - don't waste quota on subpages
                return {
                    "url": base_url,
                    "title": main_result.get("title", ""),
                    "content": f"# Main Page: {base_url}\n\n**Site Error Detected**: {error_reason}\n\nThe main page returned an error. Skipped fetching subpages to save quota.\n\n---\n\n{main_content[:2000]}",
                    "method": "firecrawl",
                    "pages_fetched": 1,
                    "word_count": len(main_content.split()),
                    "char_count": len(main_content),
                }, {
                    "urls_attempted": [base_url],
                    "urls_succeeded": [base_url],
                    "urls_failed": [],
                    "early_exit": True,
                    "early_exit_reason": f"main page error: {error_reason}",
                }

            # Main page OK, remove from selected_urls (already fetched)
            selected_urls = [u for u in selected_urls if u.rstrip("/") != base_url]
            if not selected_urls:
                # Only main page needed
                return {
                    "url": base_url,
                    "title": main_result.get("title", ""),
                    "content": f"# Main Page: {base_url}\n\n{main_content}",
                    "method": "firecrawl",
                    "pages_fetched": 1,
                    "word_count": len(main_content.split()),
                    "char_count": len(main_content),
                }, {
                    "urls_attempted": [base_url],
                    "urls_succeeded": [base_url],
                    "urls_failed": [],
                }
        else:
            logger.warning(f"Main page fetch failed: {base_url}")

        # Step 4: Batch scrape remaining subpages
        results, scrape_meta = await self._batch_scrape(selected_urls, max_length)

        # Prepend main page result if we have it
        if main_result:
            results = [main_result] + (results if results else [])
            scrape_meta["urls_succeeded"] = [base_url] + scrape_meta.get("urls_succeeded", [])
            scrape_meta["urls_attempted"] = [base_url] + scrape_meta.get("urls_attempted", [])

        if not results:
            raise Exception("Batch scrape returned no results")

        merged = self._merge_results(results, url)
        scrape_meta["urls_requested"] = selected_urls
        scrape_meta["partial_success"] = len(scrape_meta.get("urls_failed", [])) > 0
        scrape_meta["failure_summary"] = {
            "failed_count": len(scrape_meta.get("urls_failed", [])),
            "total_count": len(selected_urls),
        }

        return merged, scrape_meta

    async def _map(self, url: str) -> List[str]:
        """Use Firecrawl Map API to get all URLs on a website."""
        timeout = aiohttp.ClientTimeout(total=MAP_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {
                "Authorization": f"Bearer {self.firecrawl_api_key}",
                "Content-Type": "application/json"
            }
            payload = {"url": url, "limit": MAP_URL_LIMIT}

            async with session.post(
                "https://api.firecrawl.dev/v2/map",
                json=payload,
                headers=headers
            ) as response:
                if response.status != 200:
                    error = await response.text()
                    raise Exception(f"Map API failed ({response.status}): {error[:200]}")

                data = await response.json()
                links = data.get("links", [])
                # V2 Map API returns list of dicts with 'url' and 'title' keys
                # Extract URL strings for compatibility
                return [
                    link.get("url") if isinstance(link, dict) else link
                    for link in links
                    if link
                ]

    async def _batch_scrape(self, urls: List[str], max_length: int) -> Tuple[List[Dict], Dict]:
        """
        Batch scrape multiple URLs with concurrency control and retry.

        Returns:
            (results, meta):
                results: successful scrape dicts
                meta: {"urls_attempted", "urls_succeeded", "urls_failed"}
        """
        semaphore = asyncio.Semaphore(BATCH_CONCURRENCY)

        async def limited_scrape(url: str) -> Optional[Dict]:
            async with semaphore:
                return await self._scrape_with_retry(url, max_length)

        tasks = [limited_scrape(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Filter out failures and deduplicate by returned URL
        # This prevents duplicate content when pages redirect to the same destination
        valid_results = []
        urls_succeeded: List[str] = []
        urls_failed: List[Dict] = []
        seen_returned_urls: set = set()  # Track returned URLs to deduplicate

        for url, r in zip(urls, results):
            # Detailed logging for each URL's scrape outcome (P0: Debug leadership content missing)
            if isinstance(r, Exception):
                reason = str(r)
                logger.warning(f"Scrape exception for {url}: {type(r).__name__}: {reason[:100]}")
                urls_failed.append({"url": url, "reason": reason[:200]})
                continue

            if not isinstance(r, dict):
                reason = f"unexpected result type: {type(r)}"
                logger.warning(f"Scrape unexpected result for {url}: {reason}")
                urls_failed.append({"url": url, "reason": reason})
                continue

            content = r.get("content", "")
            content_len = len(content) if content else 0
            returned_url = r.get("url", url)

            if not content:
                reason = "empty content"
                logger.warning(f"Scrape empty content for {url} (returned_url={returned_url})")
                urls_failed.append({"url": url, "reason": reason})
                continue

            # Log successful scrape with content length (P0: Visibility into what was fetched)
            logger.info(f"Scrape success for {url}: content_len={content_len}, returned_url={returned_url}")

            # Deduplicate by the URL Firecrawl actually returned (after redirects)
            normalized_returned_url = returned_url.rstrip("/").lower()
            if normalized_returned_url in seen_returned_urls:
                logger.info(f"Skipping duplicate result for {url} (redirected to {normalized_returned_url})")
                urls_failed.append({"url": url, "reason": f"redirected to duplicate: {normalized_returned_url}"})
                continue

            seen_returned_urls.add(normalized_returned_url)
            valid_results.append(r)
            urls_succeeded.append(url)

        meta = {
            "urls_attempted": urls,
            "urls_succeeded": urls_succeeded,
            "urls_failed": urls_failed,
        }

        # P0: Batch completion summary for debugging content pipeline
        logger.info(f"Batch scrape summary: attempted={len(urls)}, succeeded={len(urls_succeeded)}, failed={len(urls_failed)}")
        if urls_succeeded:
            logger.info(f"Succeeded URLs: {urls_succeeded}")
        if urls_failed:
            failed_summary = [(f.get("url", "?"), f.get("reason", "?")[:50]) for f in urls_failed]
            logger.info(f"Failed URLs: {failed_summary}")

        return valid_results, meta

    async def _scrape_with_retry(self, url: str, max_length: int) -> Optional[Dict]:
        """Scrape a single URL with retry logic."""
        last_error = None

        for attempt in range(3):  # Max 3 attempts
            try:
                return await self._scrape(url, max_length)
            except Exception as e:
                last_error = e
                error_str = str(e)

                # Check if retryable
                status_code = None
                for code in RETRY_CONFIG.keys():
                    if str(code) in error_str:
                        status_code = code
                        break

                if status_code and attempt < RETRY_CONFIG[status_code]["max_retries"]:
                    delay = RETRY_CONFIG[status_code]["delays"][attempt]
                    logger.warning(f"Retry {attempt+1} for {url} after {delay}s (status {status_code})")
                    await asyncio.sleep(delay)
                else:
                    break

        logger.warning(f"Failed to scrape {url} after retries: {last_error}")
        return None

    async def _scrape(self, url: str, max_length: int) -> Dict:
        """Scrape a single URL using Firecrawl."""
        timeout = aiohttp.ClientTimeout(total=SCRAPE_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {
                "Authorization": f"Bearer {self.firecrawl_api_key}",
                "Content-Type": "application/json"
            }
            payload = {
                "url": url,
                "formats": ["markdown"],
                "parsers": ["pdf"],
                "onlyMainContent": True,
                # Note: 'form' removed - some sites wrap main content in form tags
                "excludeTags": ["nav", "footer", "aside", "svg", "script", "style", "noscript"],
                "removeBase64Images": True,
                "blockAds": True,
                "timeout": SCRAPE_TIMEOUT * 1000,
            }

            async with session.post(
                "https://api.firecrawl.dev/v2/scrape",
                json=payload,
                headers=headers
            ) as response:
                if response.status != 200:
                    error = await response.text()
                    raise Exception(f"Scrape failed ({response.status}): {error[:100]}")

                data = await response.json()
                result_data = data.get("data", {})

                content = result_data.get("markdown", "")
                content = clean_markdown_noise(content)  # Clean noise before truncation
                if len(content) > max_length:
                    content = content[:max_length]

                return {
                    "url": result_data.get("metadata", {}).get("url", url),
                    "title": result_data.get("metadata", {}).get("title", ""),
                    "content": content,
                }

    def _merge_results(self, results: List[Dict], original_url: str) -> Dict:
        """Merge multiple page results into a single output."""
        if len(results) == 1:
            r = results[0]
            content = r.get("content", "")
            # P0-A: Detect blocked content for Citation V2 filtering
            blocked_reason = detect_blocked_reason(content, 200)
            return {
                "url": r.get("url", original_url),
                "title": r.get("title", ""),
                "content": content,
                "method": "firecrawl",
                "pages_fetched": 1,
                "word_count": len(content.split()),
                "char_count": len(content),
                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                "status_code": 200,  # P0-A: Firecrawl scrape success = 200
                "blocked_reason": blocked_reason,  # P0-A: Content-based detection
            }

        # Multiple pages - merge with markdown separators
        merged_content = []
        total_chars = 0

        for i, r in enumerate(results):
            page_url = r.get("url", "")
            page_title = r.get("title", "")
            page_content = r.get("content", "")

            if i == 0:
                merged_content.append(f"# Main Page: {page_url}\n")
                if page_title:
                    merged_content.append(f"**{page_title}**\n")
            else:
                merged_content.append(f"\n---\n\n## Subpage {i}: {page_url}\n")
                if page_title:
                    merged_content.append(f"**{page_title}**\n")

            merged_content.append(f"\n{page_content}\n")
            total_chars += len(page_content)

        final_content = "".join(merged_content)
        # P0-A: Detect blocked content in merged result
        blocked_reason = detect_blocked_reason(final_content, 200)

        return {
            "url": original_url,
            "title": results[0].get("title", ""),
            "content": final_content,
            "method": "firecrawl",
            "pages_fetched": len(results),
            "word_count": len(final_content.split()),
            "char_count": total_chars,
            "tool_source": "fetch",  # Citation V2: mark as fetch-origin
            "status_code": 200,  # P0-A: Firecrawl scrape success = 200
            "blocked_reason": blocked_reason,  # P0-A: Content-based detection
            "metadata": {
                "urls": [r.get("url") for r in results]
            }
        }

    async def _fetch_with_exa(
        self,
        url: str,
        limit: int,
        target_keywords: str,
        target_paths: List[str],
        max_length: int
    ) -> Dict[str, Any]:
        """Fallback: Use Exa with subpages feature."""
        timeout = aiohttp.ClientTimeout(total=60)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            headers = {
                "x-api-key": self.exa_api_key,
                "Content-Type": "application/json"
            }

            # Use target keywords and target paths as subpage_target
            subpage_target = target_keywords.strip() if target_keywords else ""
            if target_paths:
                normalized_paths = self._normalize_target_paths(target_paths)
                path_tokens = []
                for p in normalized_paths:
                    token = p.strip("/").replace("-", " ").replace("_", " ")
                    if token:
                        path_tokens.append(token)
                if path_tokens:
                    path_hint = " ".join(path_tokens)
                    if subpage_target:
                        subpage_target = f"{subpage_target} {path_hint}"
                    else:
                        subpage_target = path_hint
            if not subpage_target:
                subpage_target = None

            search_payload = {
                "query": url,
                "numResults": 1,
                "includeDomains": [urlparse(url).netloc],
                "subpages": limit,
                "livecrawl": "preferred"
            }
            if subpage_target:
                search_payload["subpageTarget"] = subpage_target

            async with session.post(
                "https://api.exa.ai/search",
                json=search_payload,
                headers=headers
            ) as response:
                if response.status != 200:
                    raise Exception(f"Exa search failed: {response.status}")

                data = await response.json()
                results = data.get("results", [])
                if not results:
                    raise Exception("Exa returned no results")

                result_ids = [r.get("id") for r in results if r.get("id")]

            # Get full content
            content_payload = {
                "ids": result_ids,
                "text": {"maxCharacters": max_length, "includeHtmlTags": False}
            }

            async with session.post(
                "https://api.exa.ai/contents",
                json=content_payload,
                headers=headers
            ) as response:
                if response.status != 200:
                    raise Exception(f"Exa contents failed: {response.status}")

                content_data = await response.json()
                content_results = content_data.get("results", [])

                if len(content_results) == 1:
                    r = content_results[0]
                    content = r.get("text", "")
                    # P0-A: Detect blocked content for Citation V2 filtering
                    blocked_reason = detect_blocked_reason(content, 200)
                    return {
                        "url": r.get("url", url),
                        "title": r.get("title", ""),
                        "content": content,
                        "method": "exa",
                        "pages_fetched": 1,
                        "word_count": len(content.split()),
                        "char_count": len(content),
                        "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                        "status_code": 200,  # P0-A: Exa success = 200
                        "blocked_reason": blocked_reason,  # P0-A: Content-based detection
                    }

                return self._merge_exa_results(content_results, url)

    def _merge_exa_results(self, results: List[Dict], original_url: str) -> Dict:
        """Merge Exa results into single output."""
        merged_content = []
        total_chars = 0

        for i, r in enumerate(results):
            page_url = r.get("url", "")
            page_title = r.get("title", "")
            page_content = r.get("text", "")

            if i == 0:
                merged_content.append(f"# Main Page: {page_url}\n")
                if page_title:
                    merged_content.append(f"**{page_title}**\n")
            else:
                merged_content.append(f"\n---\n\n## Subpage {i}: {page_url}\n")
                if page_title:
                    merged_content.append(f"**{page_title}**\n")

            merged_content.append(page_content)
            total_chars += len(page_content)

        final_content = "\n".join(merged_content)
        # P0-A: Detect blocked content in merged result
        blocked_reason = detect_blocked_reason(final_content, 200)

        return {
            "url": original_url,
            "title": results[0].get("title", ""),
            "content": final_content,
            "method": "exa",
            "pages_fetched": len(results),
            "word_count": len(final_content.split()),
            "char_count": total_chars,
            "tool_source": "fetch",  # Citation V2: mark as fetch-origin
            "status_code": 200,  # P0-A: Exa success = 200
            "blocked_reason": blocked_reason,  # P0-A: Content-based detection
        }
