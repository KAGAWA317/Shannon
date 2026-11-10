"""
Web Fetch Tool - Extract full page content for deep analysis
Web Fetch tool——提取完整页面内容以进行深入分析

Multi-provider architecture:
多 provider 架构：
- Exa: Semantic search + content extraction (handles JS-heavy sites)
- Exa：语义搜索 + 内容提取（可处理 JS 重度站点）
- Firecrawl: Smart crawling + structured extraction + actions
- Firecrawl：智能爬取 + 结构化提取 + actions
- Pure Python: Free, fast, works for most pages
- 纯 Python：免费、快速，适用于大多数页面

Security features:
安全特性：
- SSRF protection (blocks private IPs, cloud metadata endpoints)
- SSRF 防护（拦截私有 IP、云 metadata 端点）
- Memory exhaustion prevention (50MB response limit)
- 防止内存耗尽（50MB response 上限）
- Redirect loop protection (max 10 redirects)
- 重定向循环防护（最多 10 次重定向）
"""

import aiohttp
import asyncio
import os
import re
import logging
from typing import Dict, Optional, List, Any
from urllib.parse import urlparse, urljoin, parse_qsl, urlencode
from bs4 import BeautifulSoup
import html2text
from enum import Enum

from ..base import Tool, ToolMetadata, ToolParameter, ToolParameterType, ToolResult
from ..openapi_parser import _is_private_ip

logger = logging.getLogger(__name__)

# --- Prompt-guided extraction constants ---
# --- Prompt 引导的提取相关常量 ---
EXTRACTION_INTERNAL_MAX = 100000  # Internal max_length when extraction is enabled
EXTRACTION_CONTENT_CAP = 400000   # Max chars sent to extraction LLM (context safety)
EXTRACTION_TIMEOUT = 60           # Seconds before extraction call times out
# 启用提取时的内部 max_length；发送给提取 LLM 的最大字符数（context 安全）；
# 提取调用 timeout 秒数。


async def extract_with_llm(
    content: str,
    extract_prompt: Optional[str] = None,
    max_output: int = 4000,
    timeout: int = EXTRACTION_TIMEOUT,
) -> Optional[tuple]:
    """
    Call a small model to extract/compress page content.
    调用小模型对页面内容进行提取/压缩。

    When extract_prompt is provided, extracts info relevant to that query.
    When extract_prompt is None, performs generic content extraction — strips
    boilerplate and returns the main informational content.
    提供 extract_prompt 时，提取与该查询相关的信息。
    未提供 extract_prompt 时，执行通用内容提取——剥离样板内容并返回主要信息内容。

    Returns (extracted_text, total_tokens, cost_usd, model_name) on success, None on failure.
    Falls back gracefully — any exception returns None so callers can truncate instead.
    成功时返回 (extracted_text, total_tokens, cost_usd, model_name)，失败返回 None。
    优雅降级——任何 exception 都返回 None，调用方可改为截断处理。
    """
    try:
        from llm_provider.manager import get_llm_manager
        from llm_provider.base import ModelTier

        # Cap input to avoid exceeding model context window
        # 限制输入长度，避免超出模型 context window
        if len(content) > EXTRACTION_CONTENT_CAP:
            content = content[:EXTRACTION_CONTENT_CAP]

        if extract_prompt:
            system_msg = (
                "Extract information relevant to the user's query from the web page content below. "
                "Return only the relevant parts as clean, well-structured text. "
                "Be comprehensive — include all relevant details — but omit navigation, ads, and boilerplate. "
                "If the page contains no relevant information, say so briefly."
            )
            user_msg = f"Query: {extract_prompt}\n\nPage content:\n{content}"
        else:
            system_msg = (
                "You are a web page content extractor. Given raw page content, extract and return "
                "the main informational content in clean, well-structured text. "
                "Remove: navigation menus, footers, sidebars, ads, cookie banners, social media widgets, "
                "and repetitive boilerplate. "
                "Keep: all substantive content — articles, data, tables, lists, descriptions, specifications, "
                "pricing, team info, and any other informational text. "
                "Preserve the original structure with headings where appropriate."
            )
            user_msg = f"Page content:\n{content}"

        manager = get_llm_manager()
        response = await asyncio.wait_for(
            manager.complete(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                model_tier=ModelTier.SMALL,
                max_tokens=max_output,
                temperature=0.0,
                cache_source="web_fetch_summary",
            ),
            timeout=timeout,
        )
        return (
            response.content,
            getattr(response.usage, "total_tokens", 0),
            getattr(response.usage, "estimated_cost", 0.0),
            response.model,
        )
    except asyncio.TimeoutError:
        logger.warning("LLM extraction timed out after %ds, falling back to truncation", timeout)
        return None
    except Exception as e:
        logger.warning("LLM extraction failed, falling back to truncation: %s", e)
        return None


async def apply_extraction(
    result: ToolResult,
    extract_prompt: Optional[str],
    original_max_length: int,
) -> ToolResult:
    """
    Post-process a ToolResult: replace over-length content with LLM extraction.

    Always runs when content exceeds original_max_length — uses a small model to
    extract/compress the main content instead of blind truncation.
    When extract_prompt is provided, extraction is targeted to that query.
    When extract_prompt is None, generic content extraction is used.
    On extraction failure, falls back to hard truncation at original_max_length.
    """
    if not result.success or not result.output:
        return result

    content = result.output.get("content", "")
    if not content or len(content) <= original_max_length:
        return result

    # Cap LLM output tokens to respect user's max_length (chars ≈ tokens * 4)
    # 限制 LLM 输出 token 数，以符合用户传入的 max_length（字符数 ≈ token 数 * 4）
    max_output = min(original_max_length, 4000)
    ext = await extract_with_llm(content, extract_prompt, max_output=max_output)
    if ext:
        extracted_text, tokens, cost, model = ext
        # Hard clamp to original_max_length as safety net
        # 作为兜底，强制裁剪到 original_max_length
        if len(extracted_text) > original_max_length:
            extracted_text = extracted_text[:original_max_length]
        result.output["content"] = extracted_text
        result.output["extracted"] = True
        result.output["truncated"] = True
        result.output["char_count"] = len(extracted_text)
        result.output["word_count"] = len(extracted_text.split())
        result.metadata = result.metadata or {}
        result.metadata["extraction_model"] = model
        result.metadata["extraction_tokens"] = tokens
        result.metadata["extraction_cost_usd"] = cost
        result.tokens_used = (result.tokens_used or 0) + tokens
        result.cost_usd = (result.cost_usd or 0) + cost
    else:
        # Fallback: hard truncation at original limit
        # 降级方案：按原始上限硬截断
        result.output["content"] = content[:original_max_length]
        result.output["extracted"] = False
        result.output["truncated"] = True
        result.output["char_count"] = len(result.output["content"])
        result.output["word_count"] = len(result.output["content"].split())

    return result


# --- Batch extraction (N pages → 1 LLM call) ---
# --- 批量提取（N 个页面 → 1 次 LLM 调用）---

_BATCH_RESULT_RE = re.compile(
    r"<<<PAGE_RESULT\s+(\d+)\s*>>>\s*(.*?)\s*<<<END_PAGE_RESULT\s+\1\s*>>>",
    re.DOTALL,
)


async def extract_batch_with_llm(
    pages: List[Dict],
    per_page_budget: int = 2000,
    extract_prompt: Optional[str] = None,
    timeout: int = EXTRACTION_TIMEOUT,
) -> Optional[tuple]:
    """
    Combine N pages into a single LLM call for extraction.
    将 N 个页面合并为一次 LLM 调用进行提取。

    Args:
        pages: List of dicts with keys: page_id, url, title, content
        per_page_budget: Target chars per page in output
        extract_prompt: Optional query to focus extraction
        timeout: Seconds before extraction call times out

    Returns:
        (dict[int, str], total_tokens, cost_usd, model) on success, None on failure.
        dict maps page_id → extracted text. Missing pages get empty string.

    参数：
        pages: 字典列表，键为 page_id、url、title、content
        per_page_budget: 每个页面在输出中的目标字符数
        extract_prompt: 可选的查询，用于聚焦提取内容
        timeout: 提取调用 timeout 秒数

    返回：
        成功时返回 (dict[int, str], total_tokens, cost_usd, model)，失败返回 None。
        dict 将 page_id 映射到提取文本；缺失的页面返回空字符串。
    """
    if not pages:
        return None

    try:
        from llm_provider.manager import get_llm_manager
        from llm_provider.base import ModelTier

        n = len(pages)

        # Cap per-page content so combined doesn't exceed EXTRACTION_CONTENT_CAP
        # Reserve some budget for prompt overhead (delimiters, instructions)
        # 限制单页内容，使合并后不超过 EXTRACTION_CONTENT_CAP
        # 为 prompt 开销（分隔符、指令）预留一部分 budget
        overhead_budget = 1000  # for system/user prompt text + delimiters
        available = EXTRACTION_CONTENT_CAP - overhead_budget
        per_page_cap = max(available // n, 1000)

        # Build page blocks
        page_blocks = []
        for p in pages:
            content = p.get("content", "")
            if len(content) > per_page_cap:
                content = content[:per_page_cap]
            block = (
                f"<<<PAGE {p['page_id']}>>>\n"
                f"URL: {p.get('url', '')}\n"
                f"Title: {p.get('title', '')}\n"
                f"{content}\n"
                f"<<<END_PAGE {p['page_id']}>>>"
            )
            page_blocks.append(block)

        system_msg = (
            "You are a batch web page content extractor. You will receive multiple web pages. "
            "Extract the main informational content from EACH page independently. "
            "Do NOT mix content between pages. "
            "For each page, output the result wrapped in delimiters:\n"
            "<<<PAGE_RESULT N>>>\n(extracted content)\n<<<END_PAGE_RESULT N>>>\n"
            "where N is the page number."
        )

        user_parts = []
        if extract_prompt:
            user_parts.append(f"Query: {extract_prompt}\n")
        user_parts.append(
            "Extract the main content from each page below. "
            "Output each result in the delimiter format shown in the instructions.\n\n"
        )
        user_parts.append("\n\n".join(page_blocks))
        user_msg = "".join(user_parts)
        if len(user_msg) > EXTRACTION_CONTENT_CAP:
            user_msg = user_msg[:EXTRACTION_CONTENT_CAP]

        max_output = max(min(per_page_budget * n // 4, 4000), 1000)

        manager = get_llm_manager()
        response = await asyncio.wait_for(
            manager.complete(
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                model_tier=ModelTier.SMALL,
                max_tokens=max_output,
                temperature=0.0,
                cache_source="web_fetch_extract",
            ),
            timeout=timeout,
        )

        # Parse delimited output
        # 解析带分隔符的输出
        raw = response.content or ""
        extracted = {}
        for match in _BATCH_RESULT_RE.finditer(raw):
            page_id = int(match.group(1))
            text = match.group(2).strip()
            if len(text) > per_page_budget:
                text = text[:per_page_budget]
            extracted[page_id] = text

        # Fill missing pages with empty string
        # 缺失的页面用空字符串补齐
        for p in pages:
            pid = p["page_id"]
            if pid not in extracted:
                extracted[pid] = ""

        return (
            extracted,
            getattr(response.usage, "total_tokens", 0),
            getattr(response.usage, "estimated_cost", 0.0),
            response.model,
        )
    except asyncio.TimeoutError:
        logger.warning("Batch LLM extraction timed out after %ds", timeout)
        return None
    except Exception as e:
        logger.warning("Batch LLM extraction failed: %s", e)
        return None


# Constants
# MAX_SUBPAGES is the cap on how many subpages a single web_fetch call expands
# to. 15 was the old default sized for 200K-context models; with 1M-context
# defaults across Kocoro/Claude families, "summarize this 30-page docs site"
# is a routine task that benefits from a higher cap. 50 fits comfortably within
# context budget (each subpage ≈ 4-8 K tokens after truncation = ~250-400 K
# tokens worst-case, well under 1M).
# MAX_SUBPAGES 是单次 web_fetch 调用可展开的子页面上限。
# 旧默认值 15 是按 200K context 模型设定的；在 Kocoro/Claude 系列
# 普遍采用 1M context 默认值的情况下，"总结这个 30 页文档站" 已是常规 task，
# 更高的上限能带来收益。50 页仍在 context budget 内（截断后每个子页约 4-8 K token，
# 最坏情况约 250-400 K token，远低于 1M）。
MAX_SUBPAGES = 50

# P0-A: Blocked content detection patterns for Citation V2
# P0-A：用于 Citation V2 的拦截内容检测 pattern
BLOCKED_PATTERNS = [
    "Access Denied",
    "403 Forbidden",
    "Robot Check",
    "Please verify you are human",
    "Login Required",
    "Sign in to continue",
    "Captcha required",
    "Enable JavaScript",
    "This page isn't available",
    "Page not found",
]


def detect_blocked_reason(content: str, status_code: int = 200) -> Optional[str]:
    """
    Detect if fetched content indicates a blocked/failed fetch.
    检测抓取到的内容是否表示抓取被拦截或失败。

    Returns blocked_reason string if blocked, None otherwise.
    Used by Citation V2 to filter invalid sources before verification.
    被拦截时返回 blocked_reason 字符串，否则返回 None。
    供 Citation V2 在验证前过滤无效来源。
    """
    if status_code >= 400:
        return f"http_{status_code}"

    if not content or len(content.strip()) < 50:
        return "empty_content"

    content_lower = content.lower()
    for pattern in BLOCKED_PATTERNS:
        if pattern.lower() in content_lower:
            return pattern.lower().replace(" ", "_")

    return None


# Blacklist for meaningless alt text patterns
# 无意义 alt 文本 pattern 的黑名单
ALT_BLACKLIST = {
    'preload', 'loading', 'placeholder', 'lazy',
    'customer logo', 'logo', 'icon', 'image', 'img',
    'thumbnail', 'avatar', 'banner', 'hero',
    'background', 'bg', 'spacer', 'pixel',
    'video thumbnail', 'play button',
    'arrow', 'chevron', 'caret',
    'close', 'menu', 'hamburger',
    'facebook', 'twitter', 'linkedin', 'instagram', 'youtube',
    'social', 'share', 'like', 'follow',
}


def _is_meaningless_alt(alt: str) -> bool:
    """Check if alt text is meaningless and should be removed.
    检查 alt 文本是否无意义、应被移除。"""
    alt_lower = alt.lower().strip()

    # Check blacklist
    if alt_lower in ALT_BLACKLIST:
        return True

    # Check if it's a filename (e.g., image.png, photo-123.jpg)
    # 判断是否为文件名（如 image.png、photo-123.jpg）
    if re.match(r'^[\w.-]+\.(png|jpg|jpeg|gif|svg|webp|ico|bmp)$', alt_lower):
        return True

    # Check if it's just a number or ID-like string
    # 判断是否只是数字或类 ID 字符串
    if re.match(r'^[\d_-]+$', alt_lower):
        return True

    # Check for UUID-like patterns
    # 检查类 UUID pattern
    if re.match(r'^[a-f0-9-]{20,}$', alt_lower):
        return True

    return False


def clean_markdown_noise(content: str) -> str:
    """
    Clean markdown noise from fetched content.
    清理抓取内容中的 markdown 噪声。

    Features:
    功能：
    - Protect code blocks
    - 保护代码块
    - Remove images with meaningless alt text (blacklist + filename patterns)
    - 移除 alt 文本无意义的图片（黑名单 + 文件名 pattern）
    - Remove base64 data
    - 移除 base64 数据
    - Keep link text, remove URLs
    - 保留链接文本，移除 URL
    - Clean HTML img tags
    - 清理 HTML img 标签
    - Remove consecutive repetitions
    - 移除连续重复内容
    """
    if not content:
        return content

    from typing import List, Tuple

    # Step 0: Protect code blocks (fenced and inline)
    # 步骤 0：保护代码块（围栏式与行内）
    code_blocks: List[Tuple[str, str]] = []

    def save_code(m):
        placeholder = f'__CODE_BLOCK_{len(code_blocks)}__'
        code_blocks.append((placeholder, m.group(0)))
        return placeholder

    # Fenced code blocks ```...```
    content = re.sub(r'```[\s\S]*?```', save_code, content)
    # Inline code `...`
    content = re.sub(r'`[^`]+`', save_code, content)

    # Step 1: Remove base64 early (performance optimization)
    # 步骤 1：尽早移除 base64（性能优化）
    content = re.sub(r'data:image/[^;]+;base64,[a-zA-Z0-9+/=\s]+', '', content, flags=re.MULTILINE)

    # Step 2: Nested image links [![alt](img)](href) -> alt or ''
    # 步骤 2：嵌套图片链接 [![alt](img)](href) -> alt 或 ''
    def extract_meaningful_alt(m, group=1):
        alt = m.group(group).strip()
        # Filter meaningless alt text
        if _is_meaningless_alt(alt):
            return ''
        # Keep meaningful alt text (>5 chars, contains Chinese or English)
        # 保留有意义的 alt 文本（长度 >5，且包含中文或英文）
        if len(alt) > 5 and re.search(r'[\u4e00-\u9fa5a-zA-Z]{3,}', alt):
            return alt
        return ''

    content = re.sub(r'\[!\[([^\]]*)\]\([^)]*\)\]\([^)]*\)',
                     lambda m: extract_meaningful_alt(m, 1), content)

    # Step 3: Standard images ![alt](url) and ![alt](url "title")
    # 步骤 3：标准图片 ![alt](url) 与 ![alt](url "title")
    content = re.sub(r'!\[([^\]]*)\]\([^)]*(?:\s+"[^"]*")?\)',
                     lambda m: extract_meaningful_alt(m, 1), content)

    # Step 4: Reference-style images ![alt][ref]
    # 步骤 4：引用式图片 ![alt][ref]
    content = re.sub(r'!\[([^\]]*)\]\[[^\]]*\]',
                     lambda m: extract_meaningful_alt(m, 1), content)

    # Step 5: HTML img tags <img ... alt="...">
    # 步骤 5：HTML img 标签 <img ... alt="...">
    def replace_html_img(m):
        alt_match = re.search(r'alt=["\']([^"\']*)["\']', m.group(0))
        if alt_match:
            alt = alt_match.group(1).strip()
            if _is_meaningless_alt(alt):
                return ''
            if len(alt) > 5 and re.search(r'[\u4e00-\u9fa5a-zA-Z]{3,}', alt):
                return alt
        return ''

    content = re.sub(r'<img[^>]*>', replace_html_img, content, flags=re.IGNORECASE)

    # Step 6: Clean up orphaned reference definitions [ref]: url
    # 步骤 6：清理孤立的引用定义 [ref]: url
    content = re.sub(r'^\[[^\]]+\]:\s*\S+.*$', '', content, flags=re.MULTILINE)

    # Step 7: tel/mailto links - keep text, remove URL
    # 步骤 7：tel/mailto 链接——保留文本，移除 URL
    content = re.sub(r'\[([^\]]+)\]\((tel:|mailto:)[^)]+\)', r'\1', content)

    # Step 8: Anchor links - keep text, remove URL
    # 步骤 8：锚点链接——保留文本，移除 URL
    content = re.sub(r'\[([^\]]+)\]\(#[^)]*\)', r'\1', content)

    # Step 9: Autolinks - keep address, remove scheme
    # 步骤 9：自动链接——保留地址，移除 scheme
    content = re.sub(r'<mailto:([^>]+)>', r'\1', content)
    content = re.sub(r'<tel:([^>]+)>', r'\1', content)
    content = re.sub(r'<https?://([^>]+)>', r'\1', content)

    # Step 10: Empty links []()
    # 步骤 10：空链接 []()
    content = re.sub(r'\[\s*\]\([^)]*\)', '', content)

    # Step 11: Remove consecutive repetitions (e.g., "logo logo logo" -> "logo")
    # Match 2+ consecutive occurrences of the same word/phrase
    # 步骤 11：移除连续重复（如 "logo logo logo" -> "logo"）
    # 匹配同一词/短语连续出现 2 次以上
    content = re.sub(r'\b(\w+(?:\s+\w+)?)\s*(?:\1\s*){2,}', r'\1', content)

    # Step 12: Clean up excessive blank lines
    # 步骤 12：清理多余空行
    content = re.sub(r'\n{3,}', '\n\n', content)

    # Step 13: Restore code blocks
    # 步骤 13：还原代码块
    for placeholder, original in code_blocks:
        content = content.replace(placeholder, original)

    return content.strip()


CRAWL_DELAY_SECONDS = float(os.getenv("WEB_FETCH_CRAWL_DELAY", "0.5"))
MAX_TOTAL_CRAWL_CHARS = int(os.getenv("WEB_FETCH_MAX_CRAWL_CHARS", "150000"))  # 150KB total
CRAWL_TIMEOUT_SECONDS = int(os.getenv("WEB_FETCH_CRAWL_TIMEOUT", "90"))  # 90s total crawl timeout


def sanitize_snippet(content: str, title: str, max_len: int = 500) -> str:
    """
    Sanitize and truncate content for use as a citation snippet.
    清洗并截断内容，用作引用摘要（snippet）。

    Avoids noise from navigation menus, UI elements, etc.
    避免导航菜单、UI 元素等噪声。

    Args:
        content: The raw content to create snippet from
        title: Fallback title if content is unusable
        max_len: Maximum snippet length (default 500)

    Returns:
        A clean snippet string

    参数：
        content: 用于生成 snippet 的原始内容
        title: 内容不可用时的兜底标题
        max_len: snippet 最大长度（默认 500）

    返回：
        干净的 snippet 字符串
    """
    # Fallback to title if content is too short or empty
    # 内容过短或为空时回退到标题
    if not content or len(content.strip()) < 50:
        return title or ""

    snippet = content[:max_len]

    # Try to truncate at sentence boundary for cleaner snippets
    # Look for sentence endings: '. ', '! ', '? '
    # 尽量在句子边界截断，使 snippet 更整洁
    # 查找句末标记：'. '、'! '、'? '
    for end_marker in ['. ', '! ', '? ', '。', '！', '？']:
        last_idx = snippet.rfind(end_marker)
        if last_idx > 100:  # Keep at least 100 chars
            snippet = snippet[:last_idx + len(end_marker)]
            break

    # Check for navigation-heavy content (many list items)
    # 检查导航内容过多的情形（大量列表项）
    lines = snippet.split('\n')
    nav_count = sum(1 for line in lines if line.strip().startswith(('-', '*', '•')))
    if len(lines) > 3 and nav_count / len(lines) > 0.4:
        # Too navigation-heavy, use title instead
        # 导航内容过多，改用标题
        return title or snippet[:200]

    return snippet.strip()


class FetchProvider(Enum):
    """Available fetch providers
    可用的抓取 provider"""
    EXA = "exa"
    FIRECRAWL = "firecrawl"
    PYTHON = "python"
    AUTO = "auto"


class WebFetchProvider:
    """Base class for web fetch providers
    web 抓取 provider 的基类"""

    # Standardized timeout for all providers (in seconds)
    # Increased from 30s to 60s for slow websites (e.g., Japanese domains)
    # 所有 provider 统一使用的 timeout（秒）
    # 因慢速站点（如日本域名）从 30s 提升到 60s
    DEFAULT_TIMEOUT = 60
    # Timeout for aiohttp client (should be > Firecrawl timeout to allow retry)
    # aiohttp 客户端 timeout（应大于 Firecrawl timeout，以便允许 retry）
    AIOHTTP_TIMEOUT = 90

    @staticmethod
    def validate_api_key(api_key: str) -> bool:
        """Validate API key format and presence
        校验 API key 的格式与是否存在"""
        if not api_key or not isinstance(api_key, str):
            return False
        if len(api_key.strip()) < 10:
            return False
        if api_key.lower() in ["test", "demo", "example", "your_api_key_here", "xxx"]:
            return False
        return True

    @staticmethod
    def sanitize_error_message(error: str) -> str:
        """Sanitize error messages to prevent information disclosure
        清洗错误信息，防止信息泄露"""
        sanitized = re.sub(r"https?://[^\s]+", "[URL_REDACTED]", str(error))
        sanitized = re.sub(r"\b[A-Za-z0-9]{32,}\b", "[KEY_REDACTED]", sanitized)
        sanitized = re.sub(
            r"api[_\-]?key[\s=:]+[\w\-]+",
            "api_key=[REDACTED]",
            sanitized,
            flags=re.IGNORECASE,
        )
        if len(sanitized) > 200:
            sanitized = sanitized[:200] + "..."
        return sanitized

    async def fetch(
        self,
        url: str,
        max_length: int = 10000,
        subpages: int = 0,
        subpage_target: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch content from a URL.
        从 URL 抓取内容。

        Args:
            url: The URL to fetch
            max_length: Maximum content length in characters
            subpages: Number of subpages to crawl (0 = main page only)
            subpage_target: Keywords to prioritize certain subpages

        Returns:
            Dict with: url, title, content, method, pages_fetched, etc.

        参数：
            url: 要抓取的 URL
            max_length: 内容最大长度（字符数）
            subpages: 要爬取的子页面数量（0 = 仅主页面）
            subpage_target: 用于优先选择特定子页面的关键词

        返回：
            包含 url、title、content、method、pages_fetched 等的字典。
        """
        raise NotImplementedError


class FirecrawlFetchProvider(WebFetchProvider):
    """
    Firecrawl fetch provider - Smart crawling with JS rendering and structured extraction.
    Firecrawl 抓取 provider——带 JS 渲染与结构化提取的智能爬取。

    Features:
    功能：
    - Scrape: Single page extraction with JS rendering
    - Scrape：单页提取，支持 JS 渲染
    - Crawl: Multi-page crawling with automatic link discovery
    - Crawl：多页爬取，自动发现链接
    - Markdown output with clean formatting
    - 输出格式整洁的 markdown
    - Supports actions (click, scroll, wait) for complex pages
    - 支持复杂页面上的 actions（点击、滚动、等待）
    """

    def __init__(self, api_key: str):
        if not self.validate_api_key(api_key):
            raise ValueError("Invalid or missing Firecrawl API key")
        self.api_key = api_key
        self.scrape_url = "https://api.firecrawl.dev/v2/scrape"
        self.crawl_url = "https://api.firecrawl.dev/v2/crawl"

    def _infer_paths_from_target(self, subpage_target: str) -> List[str]:
        """
        Infer URL paths from subpage_target keywords.
        根据 subpage_target 关键词推断 URL 路径。
        
        This enables intelligent page selection even when LLM doesn't provide
        explicit required_paths. Maps common keywords to likely URL paths.
        这样即使 LLM 未提供显式 required_paths，也能进行智能页面选择。
        将常见关键词映射到可能的 URL 路径。
        
        Args:
            subpage_target: Keywords like "API documentation", "team information"
        
        Returns:
            List of inferred URL paths like ["/api", "/docs"]

        参数：
            subpage_target: 关键词，如 "API documentation"、"team information"

        返回：
            推断出的 URL 路径列表，如 ["/api", "/docs"]
        
        Example:
            _infer_paths_from_target("API documentation")
            -> ["/api", "/api-reference", "/reference", "/docs", "/documentation", "/guide"]
        """
        if not subpage_target:
            return []
        
        target_lower = subpage_target.lower()
        inferred = []
        
        # Keyword to path mapping
        # 关键词到路径的映射
        keyword_map = {
            "api": ["/api", "/api-reference", "/reference"],
            "doc": ["/docs", "/documentation", "/guide"],
            "tutorial": ["/tutorial", "/tutorials", "/learn", "/getting-started"],
            "about": ["/about", "/about-us", "/company"],
            "team": ["/team", "/people", "/leadership", "/management"],
            "product": ["/products", "/product", "/features"],
            "pricing": ["/pricing", "/plans", "/price"],
            "blog": ["/blog", "/news", "/articles", "/posts"],
            "contact": ["/contact", "/contact-us"],
            "career": ["/careers", "/jobs", "/join"],
        }
        
        for keyword, paths in keyword_map.items():
            if keyword in target_lower:
                inferred.extend(paths)
        
        return inferred

    async def fetch(
        self,
        url: str,
        max_length: int = 10000,
        subpages: int = 0,
        subpage_target: Optional[str] = None,
        required_paths: Optional[List[str]] = None,
        query_type: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Fetch using Firecrawl API with unified intelligent strategy.
        使用 Firecrawl API 抓取，采用统一的智能策略。

        Strategy:
        策略：
        - Single page (subpages=0): Use scrape
        - 单页（subpages=0）：使用 scrape
        - Multi-page (subpages>0): Use map+scrape with intelligent selection
        - 多页（subpages>0）：使用 map+scrape 并进行智能选择
            * With required_paths: Use explicit paths (most accurate, Deep Research)
              * 提供 required_paths 时：使用显式路径（最准确，用于 Deep Research）
            * With subpage_target: Auto-infer paths from keywords (accurate)
              * 提供 subpage_target 时：从关键词自动推断路径（较准确）
            * Neither: Use depth/length/keywords scoring (basic intelligence, still better than random)
              * 两者都没有时：使用深度/长度/关键词评分（基础智能，仍优于随机选择）
        - Fallback: If map+scrape fails, use crawl
        - 兜底：map+scrape 失败时改用 crawl
        
        All methods raise exception on failure to trigger provider fallback (Exa/Python).
        所有方法在失败时都会抛 exception，以触发 provider 回退（Exa/Python）。
        """
        # Single page mode
        # 单页 mode
        if subpages == 0:
            return await self._scrape(url, max_length)

        # === UNIFIED MAP+SCRAPE STRATEGY FOR ALL MULTI-PAGE ===
        # === 面向所有多页场景的统一 MAP+SCRAPE 策略 ===
        
        # Step 1: Prepare effective_paths
        effective_paths = required_paths  # Explicit paths from Deep Research
        
        # Step 2: Auto-infer from subpage_target if no explicit paths
        # 步骤 2：没有显式路径时，从 subpage_target 自动推断
        if not effective_paths and subpage_target:
            inferred = self._infer_paths_from_target(subpage_target)
            if inferred:
                effective_paths = inferred
                logger.info(f"Auto-inferred paths from '{subpage_target}': {inferred}")
        
        # Step 3: Log strategy
        if effective_paths:
            logger.info(f"Using map+scrape with {len(effective_paths)} path hints for {url}")
        else:
            logger.info(f"Using map+scrape with depth/keywords scoring for {url}")
        
        # Level 1: Map+scrape (UNIVERSAL INTELLIGENT SELECTION)
        # 第 1 层：Map+scrape（通用智能选择）
        is_rate_limited = False
        try:
            result = await self._map_and_scrape(url, max_length, subpages, effective_paths)
            if result.get("pages_fetched", 0) > 0:
                return result
            logger.warning(f"Map+scrape returned 0 pages for {url}")
        except Exception as e:
            error_msg = str(e)
            if "408" in error_msg or "timeout" in error_msg.lower():
                logger.warning(f"Map+scrape timeout for {url}: {e}")
            elif "429" in error_msg or "rate limit" in error_msg.lower():
                logger.warning(f"Map+scrape rate limited for {url}: {e}")
                # Mark as rate limited - crawl fallback will also fail with 429
                # 标记为 rate limited——crawl 兜底同样会因 429 失败
                is_rate_limited = True
            else:
                logger.warning(f"Map+scrape failed for {url}: {e}")

        # Level 2: Crawl (UNIVERSAL FALLBACK)
        # Skip crawl if rate limited - API-wide limit means crawl will also fail
        # 第 2 层：Crawl（通用兜底）
        # 若已触发 rate limit 则跳过 crawl——API 级别的 rate limit 意味着 crawl 也会失败
        if is_rate_limited:
            logger.warning(f"Skipping crawl fallback for {url} due to rate limit (429)")
            raise Exception(f"Firecrawl rate limit exceeded (429) for {url}")

        try:
            logger.info(f"Fallback to crawl for {url}")
            result = await self._crawl(url, max_length, subpages)
            if result.get("pages_fetched", 0) > 0:
                return result
            logger.warning(f"Crawl returned 0 pages for {url}")
        except Exception as e:
            logger.warning(f"Crawl also failed for {url}: {e}")

        # All Firecrawl methods failed - raise to trigger provider fallback
        # 所有 Firecrawl 方法均失败——抛 exception 以触发 provider 回退
        logger.error(f"All Firecrawl methods failed for {url}, raising to trigger fallback")
        raise Exception(f"Firecrawl: all methods failed for {url}")


    async def _scrape(self, url: str, max_length: int) -> Dict[str, Any]:
        """Scrape a single page using Firecrawl scrape API
        使用 Firecrawl scrape API 抓取单个页面"""
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "url": url,
            "formats": ["markdown"],
            "parsers": ["pdf"],
            "onlyMainContent": True,
            # Note: 'form' removed - some sites wrap main content in form tags
            # 注意：已移除 'form'——有些站点会把主要内容包在 form 标签里
            "excludeTags": ["nav", "footer", "aside", "svg", "script", "style", "noscript"],
            "removeBase64Images": True,
            "blockAds": True,
            "timeout": self.DEFAULT_TIMEOUT * 1000,
        }

        timeout = aiohttp.ClientTimeout(total=self.AIOHTTP_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                self.scrape_url, json=payload, headers=headers
            ) as response:
                if response.status == 401:
                    raise Exception("Firecrawl authentication failed (401)")
                elif response.status == 429:
                    raise Exception("Firecrawl rate limit exceeded (429)")
                elif response.status != 200:
                    error_text = await response.text()
                    sanitized = self.sanitize_error_message(error_text)
                    logger.error(f"Firecrawl scrape error: {response.status}")
                    raise Exception(f"Firecrawl error: {response.status}")

                data = await response.json()

                if not data.get("success"):
                    raise Exception("Firecrawl scrape returned unsuccessful result")

                result_data = data.get("data", {})
                content = result_data.get("markdown", "")

                # Clean markdown noise before truncation
                # 截断前先清理 markdown 噪声
                content = clean_markdown_noise(content)

                # Truncate if needed
                if len(content) > max_length:
                    content = content[:max_length]

                title = result_data.get("metadata", {}).get("title", "")
                # P0-A: Detect blocked content
                blocked_reason = detect_blocked_reason(content, 200)
                # P0-A：检测被拦截的内容

                return {
                    "url": result_data.get("url", url),
                    "title": title,
                    "content": content,
                    "snippet": sanitize_snippet(content, title),  # For citation extraction
                    "author": result_data.get("metadata", {}).get("author"),
                    "published_date": result_data.get("metadata", {}).get("publishedDate"),
                    "word_count": len(content.split()),
                    "char_count": len(content),
                    "truncated": len(content) >= max_length,
                    "method": "firecrawl",
                    "pages_fetched": 1,
                    "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                    "status_code": 200,  # P0-A: Firecrawl success = 200
                    "blocked_reason": blocked_reason,  # P0-A: Content-based detection
                }

    async def _crawl(self, url: str, max_length: int, limit: int) -> Dict[str, Any]:
        """
        Crawl multiple pages using Firecrawl crawl API.
        使用 Firecrawl crawl API 爬取多个页面。

        This is an async operation - we start the crawl, then poll for results.
        这是异步操作——先启动 crawl，再轮询结果。
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        # Start crawl job
        payload = {
            "url": url,
            "limit": min(limit + 1, MAX_SUBPAGES + 1),  # +1 for main page
            "scrapeOptions": {
                "formats": ["markdown"],
                "parsers": ["pdf"],
                "onlyMainContent": True,
                # Note: 'form' removed - some sites wrap main content in form tags
            "excludeTags": ["nav", "footer", "aside", "svg", "script", "style", "noscript"],
                "removeBase64Images": True,
                "blockAds": True,
            },
        }

        timeout = aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            # Start crawl
            logger.info(f"Firecrawl: Starting crawl job for {url} with limit={limit}")
            async with session.post(
                self.crawl_url, json=payload, headers=headers
            ) as response:
                if response.status == 401:
                    raise Exception("Firecrawl authentication failed (401)")
                elif response.status == 429:
                    raise Exception("Firecrawl rate limit exceeded (429)")
                elif response.status != 200:
                    error_text = await response.text()
                    logger.error(f"Firecrawl crawl start error {response.status}: {error_text[:200]}")
                    raise Exception(f"Firecrawl crawl start error: {response.status}")

                data = await response.json()
                if not data.get("success"):
                    logger.error(f"Firecrawl crawl start failed: {data}")
                    raise Exception("Firecrawl crawl start failed")

                crawl_id = data.get("id")
                if not crawl_id:
                    logger.error(f"Firecrawl did not return crawl ID: {data}")
                    raise Exception("Firecrawl did not return crawl ID")

                logger.info(f"Firecrawl: Crawl job started, ID={crawl_id}")

            # Poll for crawl completion
            # 轮询爬取是否完成
            status_url = f"{self.crawl_url}/{crawl_id}"
            max_polls = 60  # Max 60 polls (with 2s delay = 2 minutes max)
            # 最多轮询 60 次（间隔 2s，即最长 2 分钟）
            poll_delay = 2.0

            for poll_count in range(max_polls):
                await asyncio.sleep(poll_delay)

                async with session.get(status_url, headers=headers) as status_response:
                    if status_response.status != 200:
                        logger.warning(f"Firecrawl: Poll {poll_count+1} status check failed: {status_response.status}")
                        continue

                    status_data = await status_response.json()
                    status = status_data.get("status", "")
                    total = status_data.get("total", 0)
                    completed = status_data.get("completed", 0)

                    logger.info(f"Firecrawl: Poll {poll_count+1}/{max_polls} - status={status}, completed={completed}/{total}")

                    if status == "completed":
                        # Crawl finished, process results
                        # 爬取结束，处理结果
                        results = status_data.get("data", [])
                        logger.info(f"Firecrawl: Crawl completed, got {len(results)} pages")
                        
                        # Debug: Log URLs from Firecrawl response
                        # 调试：记录 Firecrawl response 中的 URL
                        for i, page_data in enumerate(results[:5]):  # Log first 5 URLs
                            page_url = page_data.get("url", "")
                            logger.info(f"Firecrawl page {i+1} URL: {page_url}")
                        
                        return self._merge_crawl_results(results, url, max_length)

                    elif status == "failed":
                        error = status_data.get("error", "Unknown error")
                        logger.error(f"Firecrawl: Crawl job failed - {error}")
                        raise Exception(f"Firecrawl crawl job failed: {error}")

                    # Still in progress (scraping, partial, etc.), continue polling
                    # 仍在进行中（scraping、partial 等），继续轮询

            logger.error(f"Firecrawl: Crawl timeout after {max_polls} polls")
            raise Exception("Firecrawl crawl timeout - job did not complete in time")


    def _sanitize_url(self, url: str) -> str:
        """Sanitize URL by removing common error patterns
        清洗 URL，移除常见的错误 pattern"""
        if not url:
            return ""

        url = url.strip()
        lower = url.lower()

        # Occasionally Firecrawl/LLM tool-call markup leaks into a URL string.
        # Strip anything starting from a parameter tag (raw or URL-encoded).
        # Firecrawl/LLM 的 tool-call 标记偶尔会泄漏到 URL 字符串中。
        # 从 parameter 标签处（原始或 URL 编码形式）截断其后所有内容。
        cut_markers = ("</parameter", "<parameter", "%3c/parameter", "%3cparameter")
        cut_idx = -1
        for marker in cut_markers:
            idx = lower.find(marker)
            if idx != -1 and (cut_idx == -1 or idx < cut_idx):
                cut_idx = idx

        if cut_idx != -1:
            url = url[:cut_idx].strip()

        return url
    
    def _merge_crawl_results(
        self, results: List[Dict], original_url: str, max_length: int
    ) -> Dict[str, Any]:
        """Merge multiple crawl results with improved filtering and deduplication
        合并多个爬取结果，并改进过滤与去重"""
        
        if not results:
            return {
                "url": original_url,
                "title": "",
                "content": "",
                "method": "firecrawl",
                "pages_fetched": 0,
                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
            }
        
        # Step 1: Filter failed pages (statusCode != 200)
        # 步骤 1：过滤失败的页面（statusCode != 200）
        valid_pages = []
        failed_count = 0
        for page_data in results:
            metadata = page_data.get("metadata", {})
            status_code = metadata.get("statusCode", 200)
            
            if status_code != 200:
                logger.warning(
                    f"Skipping failed page: {page_data.get('url')} "
                    f"(status={status_code})"
                )
                failed_count += 1
                continue
            
            valid_pages.append(page_data)
        
        # Step 2: Deduplicate by URL
        # 步骤 2：按 URL 去重
        seen_urls = set()
        unique_pages = []
        duplicate_count = 0
        
        for page_data in valid_pages:
            page_url = page_data.get("url", "")
            
            # Sanitize URL to remove error patterns
            # 清洗 URL，移除错误 pattern
            page_url = self._sanitize_url(page_url)
            
            # Skip empty URLs after sanitization
            # 清洗后 URL 为空则跳过
            if not page_url:
                logger.warning("Skipping page with empty URL after sanitization")
                duplicate_count += 1
                continue
            
            if page_url in seen_urls:
                logger.info(f"Skipping duplicate URL: {page_url}")
                duplicate_count += 1
                continue
            
            seen_urls.add(page_url)
            # Update page_data with sanitized URL
            page_data["url"] = page_url
            unique_pages.append(page_data)
        
        if not unique_pages:
            return {
                "url": original_url,
                "title": "",
                "content": "",
                "method": "firecrawl",
                "pages_fetched": 0,
                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                "metadata": {
                    "total_crawled": len(results),
                    "failed_pages": failed_count,
                    "duplicate_pages": duplicate_count,
                }
            }
        
        # Step 3: Single page optimization
        # 步骤 3：单页优化
        if len(unique_pages) == 1:
            result = unique_pages[0]
            content = result.get("markdown", "")
            content = clean_markdown_noise(content)  # Clean noise before truncation
            if len(content) > max_length:
                content = content[:max_length]
            
            return {
                "url": result.get("url", original_url),
                "title": result.get("metadata", {}).get("title", ""),
                "content": content,
                "author": result.get("metadata", {}).get("author"),
                "published_date": result.get("metadata", {}).get("publishedDate"),
                "word_count": len(content.split()),
                "char_count": len(content),
                "truncated": len(content) >= max_length,
                "method": "firecrawl",
                "pages_fetched": 1,
                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
            }
        
        # Step 4: Multiple pages - smart truncation
        # Allocate space evenly across pages
        # 步骤 4：多页——智能截断
        # 在各页面之间平均分配空间
        per_page_limit = max_length // len(unique_pages)
        
        merged_content = []
        total_chars = 0
        pages_included = 0
        
        for i, page_data in enumerate(unique_pages):
            page_url = page_data.get("url", "")
            page_title = page_data.get("metadata", {}).get("title", "")
            page_content = page_data.get("markdown", "")
            page_content = clean_markdown_noise(page_content)  # Clean noise

            # Truncate individual page
            if len(page_content) > per_page_limit:
                page_content = page_content[:per_page_limit] + "\n\n[Content truncated...]"
            
            # Check total length limit
            if total_chars + len(page_content) > max_length:
                logger.info(
                    f"Reached max_length limit at page {i+1}/{len(unique_pages)}, "
                    f"stopping merge"
                )
                break
            
            # Add page with index (don't expose URLs in content to avoid citation extraction)
            # 带序号添加页面（不在正文中暴露 URL，避免被引用提取）
            if i == 0:
                page_header = f"# Main Page\n"
                if page_title:
                    page_header += f"**{page_title}**\n\n"
            else:
                page_header = f"\n---\n\n## Subpage {i+1}\n"
                if page_title:
                    page_header += f"**{page_title}**\n\n"
            
            merged_content.append(page_header + page_content)
            total_chars += len(page_content)
            pages_included += 1
        
        # Step 5: Combine all pages
        # 步骤 5：合并所有页面
        final_content = "".join(merged_content)
        
        return {
            "url": original_url,
            "title": unique_pages[0].get("metadata", {}).get("title", ""),
            "content": final_content,
            "word_count": len(final_content.split()),
            "char_count": len(final_content),
            "truncated": pages_included < len(unique_pages),
            "method": "firecrawl",
            "pages_fetched": pages_included,
            "tool_source": "fetch",  # Citation V2: mark as fetch-origin
            "metadata": {
                "total_crawled": len(results),
                "valid_pages": len(valid_pages),
                "unique_pages": len(unique_pages),
                "pages_included": pages_included,
                "failed_pages": failed_count,
                "duplicate_pages": duplicate_count,
                "urls": [p.get("url") for p in unique_pages[:pages_included]],
            }
        }

    async def _map(self, url: str, limit: int = 100) -> List[str]:
        """
        Use Firecrawl Map API to quickly get all URLs on a website.
        使用 Firecrawl Map API 快速获取网站上的所有 URL。

        Returns:
            List[str]: URL list from the website

        返回：
            List[str]：该网站上的 URL 列表
        """
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "url": url,
            "limit": limit,
        }

        timeout = aiohttp.ClientTimeout(total=self.AIOHTTP_TIMEOUT)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(
                "https://api.firecrawl.dev/v2/map",
                json=payload,
                headers=headers
            ) as response:
                if response.status == 401:
                    raise Exception("Firecrawl authentication failed (401)")
                elif response.status == 429:
                    raise Exception("Firecrawl rate limit exceeded (429)")
                elif response.status != 200:
                    raise Exception(f"Firecrawl map error: {response.status}")

                data = await response.json()
                if not data.get("success"):
                    raise Exception("Firecrawl map failed")

                urls = data.get("links", [])
                logger.info(f"Firecrawl map returned {len(urls)} URLs for {url}")
                return urls

    async def _batch_scrape(
        self, urls: List[str], max_length: int
    ) -> Dict[str, Any]:
        """Batch scrape multiple URLs and merge results with retry for 408/429.
        批量抓取多个 URL 并合并结果，对 408/429 进行 retry。"""
        if not urls:
            return {
                "url": "",
                "title": "",
                "content": "",
                "method": "firecrawl",
                "pages_fetched": 0,
                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
            }

        max_retries = 2
        retry_delay = 5  # seconds

        # Scrape each URL with retry logic
        # 逐个抓取 URL，并带 retry 逻辑
        results = []
        for url in urls:
            success = False
            for retry in range(max_retries + 1):
                try:
                    result = await self._scrape(url, max_length // len(urls))
                    if result and result.get("content"):
                        results.append({
                            "url": result.get("url", url),
                            "markdown": result.get("content", ""),
                            "metadata": {
                                "title": result.get("title", ""),
                                "author": result.get("author"),
                                "publishedDate": result.get("published_date"),
                            },
                        })
                        success = True
                        break
                except Exception as e:
                    error_msg = str(e)
                    
                    # Handle 408 timeout - retry after delay
                    # 处理 408 timeout——延迟后 retry
                    if "408" in error_msg:
                        if retry < max_retries:
                            logger.warning(f"408 timeout for {url}, retry {retry+1}/{max_retries}")
                            await asyncio.sleep(retry_delay)
                            continue
                        else:
                            logger.error(f"408 timeout for {url} after {max_retries} retries")
                    
                    # Handle 429 rate limit - wait longer and retry
                    # 处理 429 rate limit——等待更久后 retry
                    elif "429" in error_msg:
                        wait_time = retry_delay * (retry + 2)  # 10s, 15s, 20s
                        if retry < max_retries:
                            logger.warning(f"429 rate limit for {url}, waiting {wait_time}s, retry {retry+1}/{max_retries}")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            logger.error(f"429 rate limit for {url} after {max_retries} retries")
                    
                    else:
                        logger.warning(f"Failed to scrape {url}: {e}")

                    break  # Don't retry for other errors

            # Rate limit: add delay between URLs to avoid triggering 429s
            # rate limit 保护：在 URL 之间加入延迟，避免触发 429
            if success and url != urls[-1]:  # Skip delay after last URL
                await asyncio.sleep(0.3)

        return self._merge_crawl_results(results, urls[0] if urls else "", max_length)

    def _calculate_relevance_score(
        self,
        url: str,
        required_paths: Optional[List[str]] = None,
        total_pages: int = 0
    ) -> float:
        """
        Calculate relevance score for a URL (0.0-1.0).
        计算 URL 的相关性得分（0.0-1.0）。
        
        Args:
            url: URL to score
            required_paths: Optional list of required paths (e.g., ['/about', '/team'])
            total_pages: Total number of pages on the website (for context)
        
        Returns:
            Relevance score between 0.0 and 1.0

        参数：
            url: 待打分的 URL
            required_paths: 可选的必需路径列表（如 ['/about', '/team']）
            total_pages: 网站页面总数（用于提供 context）

        返回：
            0.0 到 1.0 之间的相关性得分
        """
        score = 0.0
        url_lower = url.lower()
        
        # Factor 1: Required paths matching (weight: 0.5)
        # 因子 1：必需路径匹配（权重：0.5）
        if required_paths:
            for path in required_paths:
                path_lower = path.lower()
                if url_lower.endswith(path_lower) or f"{path_lower}/" in url_lower:
                    score += 0.5  # Exact match
                    break
                elif path_lower.strip('/') in url_lower:
                    score += 0.3  # Partial match
                    break
        
        # Factor 2: URL depth (weight: 0.2)
        # Shallower URLs are generally more important
        # 因子 2：URL 深度（权重：0.2）
        # 层级更浅的 URL 通常更重要
        depth = url.count('/') - 2  # -2 for https://
        if depth <= 1:
            score += 0.2  # Top-level page
        elif depth == 2:
            score += 0.1  # Second-level page
        
        # Factor 3: URL length (weight: 0.1)
        # Shorter URLs tend to be more important
        # 因子 3：URL 长度（权重：0.1）
        # 较短的 URL 往往更重要
        if len(url) < 50:
            score += 0.1
        elif len(url) < 80:
            score += 0.05
        
        # Factor 4: Important keywords (weight: 0.2)
        # 因子 4：重要关键词（权重：0.2）
        important_keywords = [
            'about', 'team', 'company', 'leadership', 'management',
            'product', 'service', 'pricing', 'features',
            'investor', 'ir', 'investors', 'investor-relations',
            'contact', 'careers', 'jobs',
            'docs', 'documentation', 'guide', 'tutorial', 'api',
            'blog', 'news', 'press',
        ]
        
        for keyword in important_keywords:
            if keyword in url_lower:
                score += 0.05
                break  # Only count once
        
        # Factor 5: Website size adjustment (dynamic weight)
        # 因子 5：站点规模调整（动态权重）
        if total_pages > 0:
            if total_pages > 100:
                # Large website: boost top-level pages
                # 大型站点：提升顶层页面的得分
                if depth <= 1:
                    score *= 1.2
            elif total_pages < 20:
                # Small website: all pages are relatively important
                # 小型站点：所有页面都相对重要
                score = max(score, 0.3)
        
        return min(score, 1.0)

    async def _map_and_scrape(
        self,
        url: str,
        max_length: int,
        limit: int,
        required_paths: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Map website URLs, score by relevance, and scrape top pages.
        映射网站 URL，按相关性打分，并抓取得分最高的页面。
        
        Uses Firecrawl's map API to get all URLs, then:
        1. Filters by required_paths (if provided)
        2. Scores all URLs by relevance
        3. Selects top N pages
        4. Batch scrapes selected pages
        使用 Firecrawl 的 map API 获取全部 URL，然后：
        1. 按 required_paths 过滤（若提供）
        2. 对所有 URL 按相关性打分
        3. 选出得分最高的 N 个页面
        4. 批量抓取选中的页面
        
        Args:
            url: Base URL to map
            max_length: Max content length
            limit: Number of pages to scrape
            required_paths: Optional paths to prioritize (e.g. ["/about", "/ir"])

        参数：
            url: 要映射的基础 URL
            max_length: 内容最大长度
            limit: 要抓取的页面数量
            required_paths: 可选，需要优先的路径（如 ["/about", "/ir"]）
        """
        # Step 1: Map to get all URLs
        all_urls = await self._map(url, limit=200)

        if not all_urls:
            raise Exception("Firecrawl map returned no URLs")

        logger.info(f"Map found {len(all_urls)} URLs for {url}")

        # Step 2: Score all URLs by relevance
        urls_with_scores = []
        for page_url in all_urls:
            score = self._calculate_relevance_score(
                page_url,
                required_paths=required_paths,
                total_pages=len(all_urls)
            )
            urls_with_scores.append((page_url, score))

        # Step 3: Sort by score (descending)
        urls_with_scores.sort(key=lambda x: x[1], reverse=True)

        # Step 4: Select top N URLs
        # Filter out very low scores (< 0.2)
        MIN_SCORE_THRESHOLD = 0.2
        selected_urls = [
            url_item for url_item, score in urls_with_scores[:limit]
            if score >= MIN_SCORE_THRESHOLD
        ]

        if not selected_urls:
            # Fallback: if all scores are too low, take top N anyway
            # 兜底：若所有得分都过低，仍取前 N 个
            logger.warning(
                f"All URLs scored below {MIN_SCORE_THRESHOLD}, "
                f"taking top {limit} anyway"
            )
            selected_urls = [url_item for url_item, _ in urls_with_scores[:limit]]

        # Log selection summary
        avg_score = sum(s for _, s in urls_with_scores[:len(selected_urls)]) / len(selected_urls)
        logger.info(
            f"Selected {len(selected_urls)}/{len(all_urls)} URLs "
            f"(avg_score={avg_score:.2f}, limit={limit})"
        )

        # Step 5: Batch scrape selected URLs
        return await self._batch_scrape(selected_urls, max_length)




class WebFetchTool(Tool):
    """
    Fetch full content from a web page for detailed analysis.
    从网页抓取完整内容以进行详细分析。

    Multi-provider architecture:
    多 provider 架构：
    1. Exa API: Semantic search + content extraction (handles JS-heavy sites)
    1. Exa API：语义搜索 + 内容提取（可处理 JS 重度站点）
    2. Firecrawl: Smart crawling + structured extraction + actions
    2. Firecrawl：智能爬取 + 结构化提取 + actions
    3. Pure Python: Free, fast, works for most pages
    3. 纯 Python：免费、快速，适用于大多数页面

    Provider selection:
    provider 选择：
    - Set WEB_FETCH_PROVIDER env var for default (auto|exa|firecrawl|python)
    - 通过 WEB_FETCH_PROVIDER 环境变量设置默认值（auto|exa|firecrawl|python）
    - Or specify 'provider' parameter per request for runtime override
    - 或在单次 request 中指定 'provider' 参数以在运行时覆盖
    """

    def __init__(self):
        # API keys
        self.exa_api_key = os.getenv("EXA_API_KEY")
        self.firecrawl_api_key = os.getenv("FIRECRAWL_API_KEY")

        # Default provider from env (auto = select based on available keys)
        # 来自环境变量的默认 provider（auto = 根据可用 key 选择）
        self.default_provider = os.getenv("WEB_FETCH_PROVIDER", "auto").lower()

        # Initialize Firecrawl provider if configured
        # 若已配置则初始化 Firecrawl provider
        self.firecrawl_provider: Optional[FirecrawlFetchProvider] = None
        if self.firecrawl_api_key and WebFetchProvider.validate_api_key(self.firecrawl_api_key):
            try:
                self.firecrawl_provider = FirecrawlFetchProvider(self.firecrawl_api_key)
                logger.info("Initializing firecrawl fetch provider")
            except ValueError as e:
                logger.warning(f"Failed to initialize Firecrawl provider: {e}")


        # Other settings
        self.user_agent = os.getenv(
            "WEB_FETCH_USER_AGENT", "Shannon-Research/1.0 (+https://docs.shannon.run)"
        )
        self.timeout = int(os.getenv("WEB_FETCH_TIMEOUT", "30"))
        # Security limits
        # 安全限制
        self.max_response_bytes = int(
            os.getenv("WEB_FETCH_MAX_RESPONSE_BYTES", str(50 * 1024 * 1024))
        )  # 50MB
        self.max_redirects = int(os.getenv("WEB_FETCH_MAX_REDIRECTS", "10"))
        super().__init__()

    def _get_metadata(self) -> ToolMetadata:
        # Determine active provider for description
        # 确定用于描述的可用 provider
        active_providers = []
        if self.exa_api_key:
            active_providers.append("Exa")
        if self.firecrawl_provider:
            active_providers.append("Firecrawl")
        active_providers.append("Python")  # Always available
        provider_str = ", ".join(active_providers)

        return ToolMetadata(
            name="web_fetch",
            version="3.1.0",  # Bumped for batch URL support
            description=(
                "Fetch content from web pages. Supports single URL or batch mode.\n\n"
                "SINGLE PAGE: web_fetch(url=\"https://...\")\n"
                "BATCH MODE: web_fetch(urls=[\"https://...\", \"https://...\"])\n\n"
                "BEST PRACTICE: After web_search returns multiple URLs, use urls=[] "
                "to fetch them all at once instead of calling web_fetch multiple times.\n\n"
                "Returns:\n"
                "- Single mode: {url, title, content, method, word_count, char_count}\n"
                "- Batch mode: {pages[], succeeded, failed, total_chars, partial_success}"
            ),
            category="retrieval",
            author="Shannon",
            requires_auth=False,
            rate_limit=30,
            timeout_seconds=30,
            memory_limit_mb=256,
            sandboxed=False,
            dangerous=False,
            cost_per_use=0.001,
        )

    def _get_parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="url",
                type=ToolParameterType.STRING,
                description="Single URL to fetch. Use this OR urls[], not both.",
                required=False,  # Now optional - either url or urls required
            ),
            ToolParameter(
                name="urls",
                type=ToolParameterType.ARRAY,
                description=(
                    "Multiple URLs to fetch in parallel. "
                    "Use this when you have 2+ URLs from web_search results. "
                    "Example: [\"https://example.com/page1\", \"https://example.com/page2\"]. "
                    "Use urls=[] OR url=, not both."
                ),
                required=False,
            ),
            ToolParameter(
                name="max_length",
                type=ToolParameterType.INTEGER,
                description="Max chars per page (batch mode: total budget divided among pages)",
                required=False,
                default=10000,
                min_value=1000,
                max_value=50000,
            ),
            ToolParameter(
                name="concurrency",
                type=ToolParameterType.INTEGER,
                description="Max concurrent fetches for batch mode (default: 3)",
                required=False,
                default=3,
                min_value=1,
                max_value=10,
            ),
            ToolParameter(
                name="total_chars_cap",
                type=ToolParameterType.INTEGER,
                description="Max total chars for batch mode (default: 40000)",
                required=False,
                default=40000,
                min_value=5000,
                max_value=100000,
            ),
            ToolParameter(
                name="extract_prompt",
                type=ToolParameterType.STRING,
                description=(
                    "When set, uses a small model to extract relevant information "
                    "instead of blind truncation. Provide what you need from the page. "
                    "Example: 'Extract pricing tiers, features, and free trial details'"
                ),
                required=False,
            ),
            # DEPRECATED: Keep for backward compatibility with Go orchestrator
            # These will be removed in a future version
            # 已弃用：保留以兼容 Go orchestrator
            # 这些参数将在后续版本移除
            ToolParameter(
                name="subpages",
                type=ToolParameterType.INTEGER,
                description=(
                    "[DEPRECATED] Use web_subpage_fetch or web_crawl for multi-page. "
                    "This parameter is ignored. Kept for backward compatibility."
                ),
                required=False,
                default=0,
                min_value=0,
                max_value=MAX_SUBPAGES,
            ),
            ToolParameter(
                name="subpage_target",
                type=ToolParameterType.STRING,
                description="[DEPRECATED] Use web_subpage_fetch target_keywords instead.",
                required=False,
            ),
            ToolParameter(
                name="required_paths",
                type=ToolParameterType.ARRAY,
                description="[DEPRECATED] Use web_subpage_fetch target_paths instead.",
                required=False,
            ),
            ToolParameter(
                name="query_type",
                type=ToolParameterType.STRING,
                description="[DEPRECATED] No longer used in simplified web_fetch.",
                required=False,
            ),
        ]

    async def _execute_impl(
        self, session_context: Optional[Dict] = None, **kwargs
    ) -> ToolResult:
        """Execute web content fetch using selected provider.
        使用选定的 provider 执行 web 内容抓取。"""

        url = kwargs.get("url")
        urls = kwargs.get("urls")

        # Parameter validation: require url OR urls, not both, not neither
        # 参数校验：url 与 urls 二选一，不能同时提供，也不能都不提供
        if not url and not urls:
            return ToolResult(
                success=False,
                output=None,
                error="Must provide url or urls parameter"
            )
        if url and urls:
            return ToolResult(
                success=False,
                output=None,
                error="Provide url OR urls, not both"
            )

        # Batch mode: delegate to _fetch_batch
        # 批量 mode：委托给 _fetch_batch
        if urls:
            logger.info(f"Batch fetch mode: {len(urls)} URLs")
            # Remove urls from kwargs to avoid duplicate argument error
            # 从 kwargs 中移除 urls，避免重复传参报错
            batch_kwargs = {k: v for k, v in kwargs.items() if k not in ('url', 'urls')}
            return await self._fetch_batch(urls, session_context=session_context, **batch_kwargs)

        # Single URL mode (existing logic below)
        # Provider is now controlled by WEB_FETCH_PROVIDER env var only
        # 单 URL mode（沿用下方既有逻辑）
        # provider 现在仅由 WEB_FETCH_PROVIDER 环境变量控制
        provider_name = self.default_provider
        max_length = kwargs.get("max_length", 10000)
        extract_prompt = kwargs.get("extract_prompt")  # Optional: targeted extraction query

        # Skip ALL LLM extraction in research mode (issue #43): OODA loop does many
        # fast fetches; extraction adds ~40-60s latency per call and 40%+ of total cost.
        # OODA's Observe/Orient steps handle raw content analysis — small-model
        # pre-filtering undermines the agent's own judgment loop.
        # extract_prompt from LLM is also ignored in research mode.
        # research mode 下跳过所有 LLM 提取（issue #43）：OODA 循环会进行大量
        # 快速抓取；提取每次调用增加约 40-60s 延迟，并占总体成本的 40% 以上。
        # OODA 的 Observe/Orient 步骤会处理原始内容分析——小模型预过滤
        # 会削弱 agent 自身的判断循环。
        # research mode 下同样忽略来自 LLM 的 extract_prompt。
        _research_mode = (
            isinstance(session_context, dict)
            and bool(session_context.get("research_mode"))
        )

        if _research_mode:
            internal_max_length = max_length
        else:
            internal_max_length = EXTRACTION_INTERNAL_MAX

        async def _maybe_extract(result: ToolResult) -> ToolResult:
            """Research mode: hard-truncate. Otherwise: LLM extraction.
            research mode：硬截断。其他情况：LLM 提取。"""
            if _research_mode:
                if result.success and result.output:
                    content = result.output.get("content", "")
                    if len(content) > max_length:
                        result.output["content"] = content[:max_length]
                        result.output["truncated"] = True
                        result.output["char_count"] = len(result.output["content"])
                return result
            return await apply_extraction(result, extract_prompt, max_length)

        # DEPRECATED: These parameters are ignored in v3.0.0
        # Log warning if caller uses deprecated params
        # 已弃用：这些参数在 v3.0.0 中被忽略
        # 调用方使用弃用参数时记录告警
        deprecated_subpages = kwargs.get("subpages", 0)
        deprecated_subpage_target = kwargs.get("subpage_target")
        deprecated_required_paths = kwargs.get("required_paths")
        deprecated_query_type = kwargs.get("query_type")

        if deprecated_subpages > 0 or deprecated_subpage_target or deprecated_required_paths:
            logger.warning(
                "web_fetch deprecated params used: subpages=%d, subpage_target=%s, required_paths=%s. "
                "Use web_subpage_fetch or web_crawl instead. Params will be ignored.",
                deprecated_subpages, deprecated_subpage_target, deprecated_required_paths
            )

        # Force single-page mode (deprecated params are ignored)
        # 强制单页 mode（弃用参数会被忽略）
        subpages = 0
        subpage_target = None
        required_paths = None
        query_type = None

        if not url:
            return ToolResult(success=False, output=None, error="URL parameter required")

        # Validate URL
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return ToolResult(
                    success=False, output=None, error=f"Invalid URL: {url}"
                )

            # Only allow HTTP/HTTPS
            if parsed.scheme not in ["http", "https"]:
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Only HTTP/HTTPS protocols allowed, got: {parsed.scheme}",
                )

            # SSRF protection: Block private/internal IPs
            # SSRF 防护：拦截私有/内部 IP
            if _is_private_ip(parsed.netloc.split(":")[0]):
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Access to private/internal IP addresses is not allowed: {parsed.netloc}",
                )

            # Check if this URL/domain was previously marked as failed
            # 检查该 URL/域名此前是否已被标记为失败
            if session_context:
                failed_domains = session_context.get("failed_domains", [])
                for failed_url in failed_domains:
                    if url == failed_url or parsed.netloc in failed_url:
                        logger.info(f"Skipping previously failed domain: {url}")
                        return ToolResult(
                            success=False,
                            output=None,
                            error=f"Domain previously failed, skipping: {parsed.netloc}",
                        )

        except Exception as e:
            return ToolResult(
                success=False, output=None, error=f"Invalid URL format: {str(e)}"
            )

        attempts = []

        def _attach_metadata(result: ToolResult, provider_label: str) -> ToolResult:
            meta = result.metadata or {}
            meta.setdefault("fetch_method", provider_label)
            meta["provider_used"] = provider_label
            meta["attempts"] = attempts
            meta.setdefault("urls_requested", [url])
            meta.setdefault("urls_attempted", [url])
            if result.success:
                meta.setdefault("urls_succeeded", [url])
                meta.setdefault("urls_failed", [])
                meta["partial_success"] = len(meta.get("urls_failed", [])) > 0
                meta["failure_summary"] = {
                    "failed_count": len(meta.get("urls_failed", [])),
                    "total_count": len(meta.get("urls_attempted", [url])),
                }
            else:
                meta.setdefault("urls_succeeded", [])
                meta.setdefault(
                    "urls_failed", [{"url": url, "reason": result.error or "unknown"}]
                )
                meta["partial_success"] = False
                meta["failure_summary"] = {
                    "failed_count": len(meta.get("urls_failed", [])),
                    "total_count": len(meta.get("urls_attempted", [url])),
                }
            result.metadata = meta
            return result

        try:
            # Emit progress
            observer = kwargs.get("observer")
            if observer:
                try:
                    observer("progress", {"message": f"Fetching content from {url}..."})
                except Exception:
                    pass

            # Resolve provider: auto-select or use specified
            # 解析 provider：自动选择或使用指定值
            selected_provider = self._resolve_provider(provider_name, subpages)
            logger.info(f"Fetching with {selected_provider}: {url} (subpages={subpages})")

            # Convert required_paths to subpage_target for non-firecrawl providers
            # 对非 firecrawl provider，将 required_paths 转换为 subpage_target
            effective_subpage_target = subpage_target
            if required_paths and selected_provider != "firecrawl":
                # Exa supports subpage_target for keyword filtering
                # Exa 支持用 subpage_target 进行关键词过滤
                effective_subpage_target = " ".join([p.strip("/") for p in required_paths])
                logger.info(f"Converted required_paths to subpage_target: {effective_subpage_target}")

            # Execute with selected provider (use internal_max_length for providers)
            # 使用选定的 provider 执行（provider 侧使用 internal_max_length）
            if selected_provider == "firecrawl":
                if not self.firecrawl_provider:
                    logger.warning("Firecrawl requested but not configured, falling back")
                    attempts.append({"provider": "firecrawl", "status": "skipped", "reason": "not configured"})
                    selected_provider = "exa" if self.exa_api_key else "python"
                else:
                    try:
                        attempts.append({"provider": "firecrawl", "status": "attempted"})
                        result_data = await self.firecrawl_provider.fetch(
                            url, internal_max_length, subpages, subpage_target,
                            required_paths=required_paths,
                            query_type=query_type
                        )
                        # Auto-fallback: if firecrawl returned sparse content (<500 chars),
                        # try pure_python — the page may have anti-scraping that firecrawl can't handle.
                        # 自动回退：若 firecrawl 返回内容过少（<500 字符），
                        # 尝试 pure_python——该页面可能有 firecrawl 无法处理的反爬措施。
                        fc_content = result_data.get("content", "")
                        if len(fc_content) < 500 and not fc_content.startswith("%PDF"):
                            logger.warning(
                                "web_fetch: firecrawl returned sparse content (%d chars) for %s, trying pure_python fallback",
                                len(fc_content), url,
                            )
                            attempts[-1]["status"] = "sparse_fallback"
                            selected_provider = "python"
                            # Fall through to python path below
                        else:
                            attempts[-1]["status"] = "success"
                            firecrawl_result = ToolResult(success=True, output=result_data, metadata={"fetch_method": "firecrawl"})
                            return await _maybe_extract(
                                _attach_metadata(firecrawl_result, "firecrawl"),
                            )
                    except Exception as e:
                        attempts[-1]["status"] = "failed"
                        attempts[-1]["error"] = str(e)
                        logger.error(f"Firecrawl fetch failed: {e}, falling back")
                        selected_provider = "exa" if self.exa_api_key else "python"

            if selected_provider == "exa":
                if self.exa_api_key:
                    attempts.append({"provider": "exa", "status": "attempted"})
                    result = await self._fetch_with_exa(url, internal_max_length, subpages, effective_subpage_target)
                    attempts[-1]["status"] = "success" if result.success else "failed"
                    if not result.success:
                        attempts[-1]["error"] = result.error
                    return await _maybe_extract(
                        _attach_metadata(result, "exa"),
                    )
                else:
                    logger.warning("Exa requested but not configured, falling back to Python")
                    attempts.append({"provider": "exa", "status": "skipped", "reason": "not configured"})
                    selected_provider = "python"

            # Fallback to pure Python
            attempts.append({"provider": "python", "status": "attempted"})
            result = await self._fetch_pure_python(url, internal_max_length, subpages)
            attempts[-1]["status"] = "success" if result.success else "failed"
            if not result.success:
                attempts[-1]["error"] = result.error
            return await _maybe_extract(
                _attach_metadata(result, "python"),
            )

        except Exception as e:
            logger.error(f"Failed to fetch {url}: {str(e)}")
            return _attach_metadata(
                ToolResult(success=False, output=None, error=f"Failed to fetch page: {str(e)}"),
                "error"
            )

    def _resolve_provider(self, provider_name: str, subpages: int) -> str:
        """
        Resolve which provider to use based on request and availability.
        根据 request 与可用性决定使用哪个 provider。

        Auto-selection logic:
        自动选择逻辑：
        - Always prefer Firecrawl > Exa > Python (Firecrawl has better PDF parsing and content extraction)
        - 始终优先 Firecrawl > Exa > Python（Firecrawl 的 PDF 解析与内容提取更好）
        """
        # Handle explicit provider request
        if provider_name in ["exa", "firecrawl", "python"]:
            return provider_name

        # Auto-select: always prefer Firecrawl for better PDF parsing and content extraction
        # 自动选择：始终优先 Firecrawl，以获得更好的 PDF 解析与内容提取
        if provider_name == "auto" or provider_name == self.default_provider:
            if self.firecrawl_provider:
                return "firecrawl"
            elif self.exa_api_key:
                return "exa"
            else:
                return "python"

        # Default fallback
        return self.default_provider if self.default_provider != "auto" else "python"

    async def _fetch_batch(
        self,
        urls: List[str],
        session_context: Optional[Dict] = None,
        **kwargs
    ) -> ToolResult:
        """
        Fetch multiple URLs in parallel with concurrency control.
        在并发控制下并行抓取多个 URL。

        Args:
            urls: List of URLs to fetch
            session_context: Optional session context
            **kwargs: Additional parameters (max_length, concurrency, total_chars_cap)

        Returns:
            ToolResult with aggregated results:
            - pages[]: Array of {url, success, title, content, error}
            - succeeded: Count of successful fetches
            - failed: Count of failed fetches
            - total_chars: Total characters fetched
            - partial_success: True if some succeeded and some failed

        参数：
            urls: 要抓取的 URL 列表
            session_context: 可选的 session context
            **kwargs: 其他参数（max_length、concurrency、total_chars_cap）

        返回：
            带聚合结果的 ToolResult：
            - pages[]：{url, success, title, content, error} 数组
            - succeeded：成功抓取的数量
            - failed：失败的数量
            - total_chars：抓取到的总字符数
            - partial_success：部分成功、部分失败时为 True
        """
        max_length = kwargs.get("max_length", 10000)
        concurrency = kwargs.get("concurrency", 3)
        total_chars_cap = kwargs.get("total_chars_cap", 40000)
        extract_prompt = kwargs.get("extract_prompt")

        # Skip ALL LLM extraction in research mode — same rationale as single-URL path.
        # research mode 下跳过所有 LLM 提取——理由同单 URL 路径。
        _research_mode = (
            isinstance(session_context, dict)
            and bool(session_context.get("research_mode"))
        )

        # Validate URLs list
        if not urls or not isinstance(urls, list):
            return ToolResult(
                success=False,
                output=None,
                error="urls must be a non-empty list"
            )

        # Filter and validate individual URLs
        valid_urls = []
        for u in urls:
            if not isinstance(u, str) or not u.strip():
                logger.warning(f"Skipping invalid URL in batch: {u}")
                continue
            # Basic URL validation
            try:
                parsed = urlparse(u)
                if parsed.scheme in ("http", "https") and parsed.netloc:
                    valid_urls.append(u.strip())
                else:
                    logger.warning(f"Skipping non-HTTP URL: {u}")
            except Exception:
                logger.warning(f"Skipping malformed URL: {u}")

        if not valid_urls:
            return ToolResult(
                success=False,
                output=None,
                error="No valid URLs in batch"
            )

        logger.info(f"Batch fetching {len(valid_urls)} URLs (concurrency={concurrency})")

        # Inflate per-URL budget so providers return full content for extraction
        # 抬高单个 URL 的 budget，让 provider 返回完整内容以供提取
        original_per_url_budget = max(max_length // len(valid_urls), 2000)
        if _research_mode:
            per_url_budget = original_per_url_budget
        else:
            per_url_budget = EXTRACTION_INTERNAL_MAX

        # Concurrency control
        semaphore = asyncio.Semaphore(concurrency)

        async def fetch_one(url: str) -> Dict[str, Any]:
            """Fetch a single URL with semaphore control. Prefer Firecrawl > pure_python.
            在 semaphore 控制下抓取单个 URL。优先 Firecrawl > pure_python。"""
            async with semaphore:
                page_result = None

                # Try Firecrawl first if available
                if self.firecrawl_provider:
                    try:
                        result_data = await self.firecrawl_provider._scrape(url, per_url_budget)
                        page_result = {
                            "url": result_data.get("url", url),
                            "success": True,
                            "title": result_data.get("title", ""),
                            "content": result_data.get("content", ""),
                            "char_count": result_data.get("char_count", 0),
                            "method": "firecrawl",
                            "status_code": result_data.get("status_code", 200),
                            "blocked_reason": result_data.get("blocked_reason"),
                        }
                        # Auto-fallback: sparse firecrawl result → try pure_python
                        # 自动回退：firecrawl 结果过少 → 尝试 pure_python
                        fc_content = result_data.get("content", "")
                        if page_result.get("char_count", 0) < 500 and not fc_content.startswith("%PDF"):
                            logger.warning(
                                "web_fetch batch: firecrawl sparse (%d chars) for %s, falling back to pure_python",
                                page_result.get("char_count", 0), url,
                            )
                            page_result = None  # Triggers existing pure_python fallback below
                    except Exception as e:
                        logger.warning(f"Firecrawl batch fetch failed for {url}: {e}, falling back to pure_python")

                # Fallback to pure_python
                if page_result is None:
                    try:
                        result = await self._fetch_pure_python(url, per_url_budget, subpages=0)
                        if result.success and result.output:
                            page_result = {
                                "url": result.output.get("url", url),
                                "success": True,
                                "title": result.output.get("title", ""),
                                "content": result.output.get("content", ""),
                                "char_count": len(result.output.get("content", "")),
                                "method": result.output.get("method", "pure_python"),
                                "status_code": result.output.get("status_code", 200),
                                "blocked_reason": result.output.get("blocked_reason"),
                            }
                        else:
                            page_result = {
                                "url": url,
                                "success": False,
                                "error": result.error or "Unknown error",
                                "method": "pure_python",
                                "status_code": 0,
                                "blocked_reason": "fetch_failed",
                            }
                    except Exception as e:
                        logger.error(f"Batch fetch error for {url}: {e}")
                        page_result = {
                            "url": url,
                            "success": False,
                            "error": str(e),
                            "method": "pure_python",
                            "status_code": 0,
                            "blocked_reason": "exception",
                        }

                return page_result

        # Execute all fetches concurrently
        results = await asyncio.gather(
            *[fetch_one(u) for u in valid_urls],
            return_exceptions=True
        )

        # Aggregate raw results into pages list
        pages = []
        succeeded = 0
        failed = 0
        total_chars = 0
        urls_succeeded = []
        urls_failed = []

        for i, result in enumerate(results):
            url = valid_urls[i]

            # Handle exceptions from gather
            # 处理 gather 抛出的 exception
            if isinstance(result, Exception):
                pages.append({
                    "url": url,
                    "success": False,
                    "error": str(result),
                })
                failed += 1
                urls_failed.append({"url": url, "reason": str(result)})
                continue

            pages.append(result)

            if result.get("success"):
                succeeded += 1
                urls_succeeded.append(url)
                content_len = result.get("char_count", 0)
                total_chars += content_len

                # Check total chars cap
                if total_chars >= total_chars_cap:
                    logger.info(
                        f"Reached total_chars_cap ({total_chars_cap}), "
                        f"stopping at {i + 1}/{len(valid_urls)} URLs"
                    )
                    # Mark remaining URLs as skipped
                    for remaining_url in valid_urls[i + 1:]:
                        pages.append({
                            "url": remaining_url,
                            "success": False,
                            "error": "Skipped: total_chars_cap reached",
                        })
                        failed += 1
                        urls_failed.append({
                            "url": remaining_url,
                            "reason": "total_chars_cap reached"
                        })
                    break
            else:
                failed += 1
                urls_failed.append({
                    "url": url,
                    "reason": result.get("error", "Unknown")
                })

        # --- Consolidated batch extraction ---
        # Identify pages whose raw content exceeds per-URL budget
        # --- 合并后的批量提取 ---
        # 找出原始内容超过单 URL budget 的页面
        batch_extraction_tokens = 0
        batch_extraction_cost = 0.0
        batch_extraction_model = None

        # Research mode: hard-truncate instead of LLM extraction (same as single-URL path)
        # research mode：用硬截断代替 LLM 提取（与单 URL 路径一致）
        if _research_mode:
            for page in pages:
                if page.get("success"):
                    content = page.get("content", "")
                    if len(content) > original_per_url_budget:
                        page["content"] = content[:original_per_url_budget]
                        page["truncated"] = True
                        page["char_count"] = len(page["content"])

        pages_needing_extraction = []
        if not _research_mode:
            for idx, page in enumerate(pages):
                if page.get("success") and len(page.get("content", "")) > original_per_url_budget:
                    pages_needing_extraction.append((idx, page))

        if len(pages_needing_extraction) >= 2:
            # Batch extraction: single LLM call for all over-budget pages
            # 批量提取：对所有超 budget 页面只发一次 LLM 调用
            batch_pages = [
                {
                    "page_id": idx,
                    "url": page.get("url", ""),
                    "title": page.get("title", ""),
                    "content": page.get("content", ""),
                }
                for idx, page in pages_needing_extraction
            ]
            batch_result = await extract_batch_with_llm(
                batch_pages,
                per_page_budget=original_per_url_budget,
                extract_prompt=extract_prompt,
            )
            if batch_result:
                extracted_map, b_tokens, b_cost, b_model = batch_result
                batch_extraction_tokens = b_tokens
                batch_extraction_cost = b_cost
                batch_extraction_model = b_model
                for idx, page in pages_needing_extraction:
                    extracted_text = extracted_map.get(idx, "")
                    if extracted_text:
                        if len(extracted_text) > original_per_url_budget:
                            extracted_text = extracted_text[:original_per_url_budget]
                        page["content"] = extracted_text
                        page["extracted"] = True
                        page["truncated"] = True
                        page["char_count"] = len(extracted_text)
                    else:
                        # Batch returned empty for this page — hard truncate
                        # 该页在批量结果中为空——硬截断
                        page["content"] = page["content"][:original_per_url_budget]
                        page["extracted"] = False
                        page["truncated"] = True
                        page["char_count"] = len(page["content"])
            else:
                # Batch extraction failed — hard truncate all
                # 批量提取失败——全部硬截断
                for idx, page in pages_needing_extraction:
                    page["content"] = page["content"][:original_per_url_budget]
                    page["extracted"] = False
                    page["truncated"] = True
                    page["char_count"] = len(page["content"])

        elif len(pages_needing_extraction) == 1:
            # Single page: use existing single-page extraction
            # 单页：复用既有的单页提取逻辑
            idx, page = pages_needing_extraction[0]
            content = page.get("content", "")
            batch_max_output = min(original_per_url_budget, 4000)
            ext = await extract_with_llm(content, extract_prompt, max_output=batch_max_output)
            if ext:
                extracted_text, ext_tokens, ext_cost, ext_model = ext
                if len(extracted_text) > original_per_url_budget:
                    extracted_text = extracted_text[:original_per_url_budget]
                page["content"] = extracted_text
                page["extracted"] = True
                page["truncated"] = True
                page["char_count"] = len(extracted_text)
                batch_extraction_tokens = ext_tokens
                batch_extraction_cost = ext_cost
                batch_extraction_model = ext_model
            else:
                page["content"] = content[:original_per_url_budget]
                page["extracted"] = False
                page["truncated"] = True
                page["char_count"] = len(page["content"])

        # Recalculate total_chars after extraction/truncation
        # 提取/截断后重新计算 total_chars
        total_chars = sum(
            page.get("char_count", 0)
            for page in pages
            if page.get("success")
        )

        # Build output
        output = {
            "pages": pages,
            "succeeded": succeeded,
            "failed": failed,
            "total_chars": total_chars,
            "partial_success": succeeded > 0 and failed > 0,
            "tool_source": "fetch",  # Citation V2: mark as fetch-origin
        }

        # Build metadata
        metadata = {
            "fetch_method": "batch",
            "urls_requested": urls,
            "urls_attempted": valid_urls,
            "urls_succeeded": urls_succeeded,
            "urls_failed": urls_failed,
            "concurrency": concurrency,
            "per_url_budget": per_url_budget,
            "total_chars_cap": total_chars_cap,
        }

        if batch_extraction_tokens > 0:
            metadata["extraction_tokens"] = batch_extraction_tokens
            metadata["extraction_cost_usd"] = batch_extraction_cost
            if batch_extraction_model:
                metadata["extraction_model"] = batch_extraction_model

        result = ToolResult(
            success=succeeded > 0,
            output=output,
            error=None if succeeded > 0 else "All URLs failed to fetch",
            metadata=metadata,
        )
        if batch_extraction_tokens > 0:
            result.tokens_used = batch_extraction_tokens
            result.cost_usd = batch_extraction_cost
        return result

    def _normalize_url(self, url: str) -> str:
        """
        Normalize URL for deduplication:
        - Remove trailing slash (except for root path)
        - Sort query parameters properly (handles duplicates and encoded chars)
        - Remove fragments
        归一化 URL 以便去重：
        - 移除末尾斜杠（根路径除外）
        - 正确排序查询参数（可处理重复参数与编码字符）
        - 移除 fragment
        """
        parsed = urlparse(url)
        path = parsed.path or "/"
        # Remove trailing slash (except for root)
        if path != "/" and path.endswith("/"):
            path = path[:-1]
        # Build normalized URL
        normalized = f"{parsed.scheme}://{parsed.netloc}{path}"
        # Sort query parameters properly using parse_qsl/urlencode
        # This handles duplicate params (?a=1&a=2) and encoded chars correctly
        # 用 parse_qsl/urlencode 正确排序查询参数
        # 可正确处理重复参数（?a=1&a=2）与编码字符
        if parsed.query:
            params = parse_qsl(parsed.query, keep_blank_values=True)
            sorted_params = urlencode(sorted(params))
            normalized += f"?{sorted_params}"
        return normalized

    def _is_safe_url(self, url: str) -> bool:
        """
        SSRF protection: check if URL is safe to fetch.
        Returns False for private IPs, internal networks, cloud metadata endpoints.
        SSRF 防护：检查 URL 是否可安全抓取。
        对私有 IP、内部网络、云 metadata 端点返回 False。
        """
        try:
            parsed = urlparse(url)
            host = parsed.netloc.split(":")[0]  # Remove port if present
            return not _is_private_ip(host)
        except Exception:
            return False

    def _extract_same_domain_links(self, soup: BeautifulSoup, base_url: str, root_domain: str) -> List[str]:
        """Extract all same-domain links from parsed HTML with SSRF protection.
        从已解析的 HTML 中提取所有同域链接，并做 SSRF 防护。"""
        links = []
        seen = set()
        for a_tag in soup.find_all("a", href=True):
            href = a_tag["href"].strip()
            # Skip empty hrefs, anchors, and non-HTTP schemes
            # 跳过空 href、锚点以及非 HTTP scheme
            if not href or href.startswith(("#", "javascript:", "mailto:", "tel:", "data:", "ftp:", "file:")):
                continue
            # Resolve relative URLs
            try:
                full_url = urljoin(base_url, href)
                parsed = urlparse(full_url)
            except Exception:
                continue  # Skip malformed URLs
            # Only same-domain links with HTTP/HTTPS
            if parsed.netloc == root_domain and parsed.scheme in ("http", "https"):
                # Normalize URL for deduplication
                clean_url = self._normalize_url(full_url)
                # SSRF protection for each extracted link
                # 对每个提取出的链接做 SSRF 防护
                if clean_url not in seen and self._is_safe_url(clean_url):
                    seen.add(clean_url)
                    links.append(clean_url)
        return links

    async def _fetch_single_page(
        self, session: aiohttp.ClientSession, url: str, max_length: int, root_domain: str
    ) -> Optional[Dict]:
        """
        Fetch a single page and return structured data.
        Returns None on failure (silent fail for subpages).

        Security: Validates redirect destination stays on same domain.
        """
        try:
            headers = {"User-Agent": self.user_agent}
            async with session.get(
                url,
                headers=headers,
                allow_redirects=True,
                max_redirects=self.max_redirects,
            ) as response:
                if response.status != 200:
                    return None

                # SSRF protection: Check redirect destination is same domain
                final_host = response.url.host
                if final_host != root_domain:
                    logger.debug(f"Redirect to different domain blocked: {url} -> {response.url}")
                    return None

                # SSRF protection: Check final URL is not private IP
                if not self._is_safe_url(str(response.url)):
                    logger.debug(f"Redirect to unsafe URL blocked: {url} -> {response.url}")
                    return None

                # Content-Type validation: skip explicitly non-HTML responses
                # Allow missing Content-Type (attempt to parse anyway)
                content_type = response.headers.get("Content-Type", "")
                if content_type and "text/html" not in content_type.lower() and "text/plain" not in content_type.lower():
                    logger.debug(f"Skipping non-HTML content: {url} ({content_type})")
                    return None

                html_content = await response.text(errors="ignore")
                if len(html_content) > self.max_response_bytes:
                    return None

                soup = BeautifulSoup(html_content, "lxml")

                # Extract metadata
                title = soup.find("title")
                title = title.get_text().strip() if title else ""

                # Extract links before removing elements
                links = self._extract_same_domain_links(soup, str(response.url), root_domain)

                # Remove unwanted elements
                for element in soup(
                    ["script", "style", "nav", "header", "footer", "aside", "iframe", "noscript"]
                ):
                    element.decompose()

                # Extract main content
                main_content = (
                    soup.find("article")
                    or soup.find("main")
                    or soup.find("div", class_="content")
                    or soup.find("div", class_="post")
                    or soup.find("body")
                    or soup
                )

                # Convert to markdown
                h = html2text.HTML2Text()
                h.ignore_links = False
                h.ignore_images = False
                h.ignore_emphasis = False
                h.body_width = 0
                h.unicode_snob = True
                h.skip_internal_links = True

                markdown = h.handle(str(main_content))

                # Clean up whitespace
                lines = [line.rstrip() for line in markdown.split("\n")]
                markdown = "\n".join(lines)
                while "\n\n\n" in markdown:
                    markdown = markdown.replace("\n\n\n", "\n\n")

                # Truncate if needed
                if len(markdown) > max_length:
                    markdown = markdown[:max_length]

                return {
                    "url": str(response.url),
                    "title": title,
                    "content": markdown,
                    "links": links,
                }
        except Exception as e:
            logger.debug(f"Failed to fetch subpage {url}: {str(e)}")
            return None

    async def _crawl_with_subpages(
        self, session: aiohttp.ClientSession, url: str, max_length: int, subpages: int
    ) -> ToolResult:
        """
        Crawl main page plus subpages using BFS with page count limit.
        Returns merged markdown content from all fetched pages.

        Security features:
        - SSRF protection on all crawled URLs
        - Redirect domain validation
        - Rate limiting between requests (configurable delay)
        - URL normalization for deduplication
        - Total crawl timeout
        - Cumulative content size limit
        """
        root_domain = urlparse(url).netloc
        visited = set()
        normalized_start = self._normalize_url(url)
        queue = [normalized_start]
        visited.add(normalized_start)
        pages = []
        max_pages = subpages + 1  # Main page + N subpages
        total_chars = 0

        logger.info(f"Starting pure-Python crawl: {url} (max_pages={max_pages})")

        crawl_start = asyncio.get_event_loop().time()
        is_first_request = True

        while queue and len(pages) < max_pages:
            # Check total crawl timeout
            elapsed = asyncio.get_event_loop().time() - crawl_start
            if elapsed > CRAWL_TIMEOUT_SECONDS:
                logger.info(f"Crawl timeout reached ({CRAWL_TIMEOUT_SECONDS}s), stopping")
                break

            # Check cumulative content size
            if total_chars >= MAX_TOTAL_CRAWL_CHARS:
                logger.info(f"Content size limit reached ({MAX_TOTAL_CRAWL_CHARS} chars), stopping")
                break

            current_url = queue.pop(0)

            # Rate limiting: delay between requests (skip first request)
            if not is_first_request:
                await asyncio.sleep(CRAWL_DELAY_SECONDS)
            is_first_request = False

            # SSRF check before fetching
            if not self._is_safe_url(current_url):
                logger.debug(f"Skipping unsafe URL (SSRF protection): {current_url}")
                continue

            # Fetch the page (includes redirect domain validation)
            page_data = await self._fetch_single_page(session, current_url, max_length, root_domain)
            if page_data:
                pages.append(page_data)
                total_chars += len(page_data.get("content", ""))
                logger.debug(f"Crawled page {len(pages)}/{max_pages}: {current_url}")

                # Add new links to queue (only what we need)
                if len(pages) < max_pages:
                    remaining_pages = max_pages - len(pages)
                    new_links = page_data.get("links", [])[:remaining_pages * 3]  # Buffer for failed fetches
                    for link in new_links:
                        normalized_link = self._normalize_url(link)
                        if normalized_link not in visited:
                            visited.add(normalized_link)
                            queue.append(normalized_link)

        if not pages:
            return ToolResult(
                success=False,
                output=None,
                error="Failed to fetch any pages",
            )

        # Merge pages into markdown format
        if len(pages) == 1:
            # Single page result
            page = pages[0]
            return ToolResult(
                success=True,
                output={
                    "url": page["url"],
                    "title": page["title"],
                    "content": page["content"],
                    "author": None,
                    "published_date": None,
                    "word_count": len(page["content"].split()),
                    "char_count": len(page["content"]),
                    "truncated": False,
                    "method": "pure_python",
                    "pages_fetched": 1,
                    "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                },
                metadata={
                    "fetch_method": "pure_python_crawl",
                    "pages_requested": max_pages,
                },
            )
        else:
            # Multiple pages - merge with markdown separators
            merged_content = []
            total_words = 0
            total_chars = 0

            for i, page in enumerate(pages):
                page_url = page.get("url", "")
                page_title = page.get("title", "")
                page_content = page.get("content", "")

                if i == 0:
                    # Main page
                    merged_content.append(f"# Main Page: {page_url}\n")
                    if page_title:
                        merged_content.append(f"**{page_title}**\n")
                else:
                    # Subpage
                    merged_content.append(f"\n---\n\n## Subpage {i}: {page_url}\n")
                    if page_title:
                        merged_content.append(f"**{page_title}**\n")

                merged_content.append(f"\n{page_content}\n")
                total_words += len(page_content.split())
                total_chars += len(page_content)

            final_content = "".join(merged_content)
            logger.info(f"Crawl complete: {len(pages)} pages, {total_chars} chars")

            return ToolResult(
                success=True,
                output={
                    "url": pages[0]["url"],
                    "title": pages[0]["title"],
                    "content": final_content,
                    "author": None,
                    "published_date": None,
                    "word_count": total_words,
                    "char_count": total_chars,
                    "truncated": False,
                    "method": "pure_python",
                    "pages_fetched": len(pages),
                    "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                },
                metadata={
                    "fetch_method": "pure_python_crawl",
                    "pages_requested": max_pages,
                    "pages_fetched": len(pages),
                    "urls_crawled": [p["url"] for p in pages],
                },
            )

    async def _fetch_pure_python(self, url: str, max_length: int, subpages: int = 0) -> ToolResult:
        """
        Fetch using pure Python: requests + BeautifulSoup + html2text.
        Fast, free, works for most pages (80%).

        When subpages > 0, crawls same-domain links up to the specified count.

        Security features:
        - Max response size limit (prevents memory exhaustion)
        - Redirect loop protection
        - Resource leak prevention with proper session cleanup
        """
        # SSRF protection (batch mode calls this directly, so enforce here too).
        try:
            parsed = urlparse(url)
            if not parsed.scheme or not parsed.netloc:
                return ToolResult(success=False, output=None, error=f"Invalid URL: {url}")
            if parsed.scheme not in ("http", "https"):
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Only HTTP/HTTPS protocols allowed, got: {parsed.scheme}",
                )
            host = parsed.hostname or parsed.netloc.split(":")[0]
            if host and _is_private_ip(host):
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Access to private/internal IP addresses is not allowed: {host}",
                )
        except Exception as e:
            return ToolResult(success=False, output=None, error=f"Invalid URL format: {str(e)}")

        # Configure session with security limits
        timeout = aiohttp.ClientTimeout(total=self.timeout)
        connector = aiohttp.TCPConnector(limit=10)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            try:
                # Crawl mode: fetch main page + subpages
                if subpages > 0:
                    return await self._crawl_with_subpages(session, url, max_length, subpages)

                # Single page mode (original behavior)
                headers = {"User-Agent": self.user_agent}

                async with session.get(
                    url,
                    headers=headers,
                    allow_redirects=True,
                    max_redirects=self.max_redirects,
                ) as response:
                    if response.status != 200:
                        return ToolResult(
                            success=False,
                            output=None,
                            error=f"HTTP {response.status}: Failed to fetch page",
                        )

                    # SSRF protection: block redirects to private/internal hosts.
                    final_host = getattr(response.url, "host", None)
                    if final_host and _is_private_ip(final_host):
                        return ToolResult(
                            success=False,
                            output=None,
                            error=f"Access to private/internal IP addresses is not allowed: {final_host}",
                        )

                    # Check content length before reading (if provided)
                    content_length = response.headers.get("Content-Length")
                    if content_length and int(content_length) > self.max_response_bytes:
                        return ToolResult(
                            success=False,
                            output=None,
                            error=f"Response too large: {content_length} bytes (max: {self.max_response_bytes})",
                        )

                    # Read with size limit
                    html_content = await response.text(
                        errors="ignore"
                    )  # Ignore encoding errors

                    # Check actual size after reading
                    if len(html_content) > self.max_response_bytes:
                        return ToolResult(
                            success=False,
                            output=None,
                            error=f"Response too large: {len(html_content)} bytes (max: {self.max_response_bytes})",
                        )

                    # Parse HTML (outside the response context manager to avoid blocking)
                    soup = BeautifulSoup(html_content, "lxml")

                # Extract metadata
                title = soup.find("title")
                title = title.get_text().strip() if title else ""

                # Author
                meta_author = soup.find("meta", attrs={"name": "author"})
                if not meta_author:
                    meta_author = soup.find("meta", property="article:author")
                author = meta_author.get("content") if meta_author else None

                # Published date
                meta_date = soup.find(
                    "meta", attrs={"property": "article:published_time"}
                )
                if not meta_date:
                    meta_date = soup.find("meta", attrs={"name": "date"})
                if not meta_date:
                    meta_date = soup.find("meta", attrs={"name": "publish-date"})
                published_date = meta_date.get("content") if meta_date else None

                # Remove unwanted elements
                for element in soup(
                    [
                        "script",
                        "style",
                        "nav",
                        "header",
                        "footer",
                        "aside",
                        "iframe",
                        "noscript",
                    ]
                ):
                    element.decompose()

                # Extract main content (prioritize article > main > body)
                main_content = (
                    soup.find("article")
                    or soup.find("main")
                    or soup.find("div", class_="content")
                    or soup.find("div", class_="post")
                    or soup.find("body")
                    or soup
                )

                # Convert to markdown
                h = html2text.HTML2Text()
                h.ignore_links = False
                h.ignore_images = False
                h.ignore_emphasis = False
                h.body_width = 0  # No line wrapping
                h.unicode_snob = True  # Better character handling
                h.skip_internal_links = True

                markdown = h.handle(str(main_content))

                # Clean up excessive whitespace
                lines = [line.rstrip() for line in markdown.split("\n")]
                markdown = "\n".join(lines)

                # Remove excessive blank lines
                while "\n\n\n" in markdown:
                    markdown = markdown.replace("\n\n\n", "\n\n")

                # Truncate if needed
                truncated = False
                if len(markdown) > max_length:
                    markdown = markdown[:max_length]
                    truncated = True

                # P0-A: Detect blocked content for Citation V2 filtering
                blocked_reason = detect_blocked_reason(markdown, response.status)

                return ToolResult(
                    success=True,
                    output={
                        "url": str(response.url),  # Final URL after redirects
                        "title": title,
                        "content": markdown,
                        "snippet": sanitize_snippet(markdown, title),  # For citation extraction
                        "author": author,
                        "published_date": published_date,
                        "word_count": len(markdown.split()),
                        "char_count": len(markdown),
                        "truncated": truncated,
                        "method": "pure_python",
                        "pages_fetched": 1,
                        "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                        "status_code": response.status,  # P0-A: HTTP status for Go filtering
                        "blocked_reason": blocked_reason,  # P0-A: None if valid, reason string if blocked
                    },
                    metadata={
                        "fetch_method": "pure_python",
                        "status_code": response.status,
                        "content_type": response.headers.get("Content-Type", ""),
                    },
                )

            except aiohttp.TooManyRedirects:
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Too many redirects (max: {self.max_redirects})",
                )

            except aiohttp.ClientError as e:
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Network error: {str(e)}",
                )
            except Exception as e:
                logger.error(f"Unexpected error fetching {url}: {str(e)}")
                return ToolResult(
                    success=False,
                    output=None,
                    error=f"Parsing error: {str(e)}",
                )

    async def _fetch_with_exa(
        self, url: str, max_length: int, subpages: int = 0, subpage_target: str = None
    ) -> ToolResult:
        """
        Fetch using Exa content API.
        Premium: handles JS-heavy sites, costs $0.001/page.

        Supports fetching subpages when subpages > 0.

        Improved error handling with detailed logging for auth/rate limit failures.
        """
        # Exa requires searching first to get IDs, then fetching content
        # For direct URL fetch, we use a workaround: search for exact URL
        timeout = aiohttp.ClientTimeout(total=30)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                headers = {
                    "x-api-key": self.exa_api_key,
                    "Content-Type": "application/json",
                }

                # Step 1: Search for the exact URL
                search_payload = {
                    "query": url,
                    "numResults": 1,
                    "includeDomains": [urlparse(url).netloc],
                }

                # Add subpages parameters if requested
                if subpages > 0:
                    search_payload["subpages"] = subpages
                    search_payload["livecrawl"] = "preferred"  # Force fresh crawling for subpages
                    if subpage_target:
                        search_payload["subpageTarget"] = subpage_target
                    logger.info(f"Exa subpages enabled: count={subpages}, target={subpage_target}")
                else:
                    search_payload["livecrawl"] = "fallback"  # Use cached when possible for single pages

                async with session.post(
                    "https://api.exa.ai/search",
                    json=search_payload,
                    headers=headers,
                ) as search_response:
                    # Detailed error logging for auth/rate limit failures
                    if search_response.status == 401:
                        logger.error("Exa API authentication failed (401 Unauthorized)")
                        logger.info("Falling back to pure Python mode")
                        return await self._fetch_pure_python(url, max_length)
                    elif search_response.status == 429:
                        logger.warning("Exa API rate limit exceeded (429 Too Many Requests)")
                        logger.info("Falling back to pure Python mode")
                        return await self._fetch_pure_python(url, max_length)
                    elif search_response.status != 200:
                        logger.warning(
                            f"Exa search failed with status {search_response.status}"
                        )
                        logger.info("Falling back to pure Python mode")
                        return await self._fetch_pure_python(url, max_length)

                    search_data = await search_response.json()
                    results = search_data.get("results", [])

                    if not results:
                        logger.info("Exa found no results, falling back to pure Python")
                        return await self._fetch_pure_python(url, max_length)

                    # Collect all result IDs (main page + subpages if applicable)
                    result_ids = [r.get("id") for r in results if r.get("id")]
                    logger.info(f"Exa search returned {len(result_ids)} result(s)")

                # Step 2: Get full content using ID(s)
                content_payload = {
                    "ids": result_ids,
                    "text": {
                        "maxCharacters": max_length,
                        "includeHtmlTags": False,
                    },
                }

                async with session.post(
                    "https://api.exa.ai/contents",
                    json=content_payload,
                    headers=headers,
                ) as content_response:
                    if content_response.status == 401:
                        logger.error("Exa API authentication failed (401 Unauthorized)")
                        logger.info("Falling back to pure Python mode")
                        return await self._fetch_pure_python(url, max_length)
                    elif content_response.status == 429:
                        logger.warning("Exa API rate limit exceeded (429 Too Many Requests)")
                        logger.info("Falling back to pure Python mode")
                        return await self._fetch_pure_python(url, max_length)
                    elif content_response.status != 200:
                        logger.warning(
                            f"Exa contents failed with status {content_response.status}"
                        )
                        logger.info("Falling back to pure Python mode")
                        return await self._fetch_pure_python(url, max_length)

                    content_data = await content_response.json()
                    results = content_data.get("results", [])

                    if not results:
                        logger.warning("Exa API returned no content")
                        return ToolResult(
                            success=False,
                            output=None,
                            error="Exa API returned no content",
                        )

                    # Handle single page vs multiple pages (subpages)
                    if len(results) == 1:
                        # Single page - return as before
                        result = results[0]
                        content = result.get("text", "")

                        # P0-A: Detect blocked content
                        blocked_reason = detect_blocked_reason(content, 200)
                        return ToolResult(
                            success=True,
                            output={
                                "url": result.get("url", url),
                                "title": result.get("title", ""),
                                "content": content,
                                "snippet": sanitize_snippet(content, result.get("title", "")),  # For citation extraction
                                "author": result.get("author"),
                                "published_date": result.get("publishedDate"),
                                "word_count": len(content.split()),
                                "char_count": len(content),
                                "truncated": len(content) >= max_length,
                                "method": "exa",
                                "pages_fetched": 1,
                                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                                "status_code": 200,  # P0-A: Exa success = 200
                                "blocked_reason": blocked_reason,  # P0-A: Content-based detection
                            },
                            metadata={
                                "fetch_method": "exa",
                                "exa_id": result_ids[0],
                                "exa_score": result.get("score"),
                            },
                        )
                    else:
                        # Multiple pages - merge with markdown separators
                        merged_content = []
                        total_words = 0
                        total_chars = 0

                        for i, result in enumerate(results):
                            page_url = result.get("url", "")
                            page_title = result.get("title", "")
                            page_content = result.get("text", "")

                            if i == 0:
                                # Main page
                                merged_content.append(f"# Main Page: {page_url}\n")
                                if page_title:
                                    merged_content.append(f"**{page_title}**\n")
                            else:
                                # Subpage
                                merged_content.append(f"\n---\n\n## Subpage {i}: {page_url}\n")
                                if page_title:
                                    merged_content.append(f"**{page_title}**\n")

                            merged_content.append(page_content)
                            total_words += len(page_content.split())
                            total_chars += len(page_content)

                        final_content = "\n".join(merged_content)
                        # P0-A: Detect blocked content in merged result
                        blocked_reason = detect_blocked_reason(final_content, 200)

                        return ToolResult(
                            success=True,
                            output={
                                "url": url,
                                "title": results[0].get("title", ""),
                                "content": final_content,
                                "word_count": total_words,
                                "char_count": total_chars,
                                "truncated": False,
                                "method": "exa",
                                "pages_fetched": len(results),
                                "tool_source": "fetch",  # Citation V2: mark as fetch-origin
                                "status_code": 200,  # P0-A: Exa success = 200
                                "blocked_reason": blocked_reason,  # P0-A: Content-based detection
                            },
                            metadata={
                                "fetch_method": "exa",
                                "exa_ids": result_ids,
                                "num_pages": len(results),
                            },
                        )

            except aiohttp.ClientError as e:
                logger.error(f"Exa network error: {str(e)}, falling back to pure Python")
                return await self._fetch_pure_python(url, max_length)
            except Exception as e:
                logger.error(f"Exa unexpected error: {str(e)}, falling back to pure Python")
                return await self._fetch_pure_python(url, max_length)
