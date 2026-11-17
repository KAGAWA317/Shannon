"""Role presets for roles_v1.
roles_v1 的角色预设。

Keep this minimal and deterministic. The orchestrator passes a role via
context (e.g. context["role"]). We map that to a system prompt and a
conservative tool allowlist. This file intentionally avoids dynamic I/O.
保持精简与确定性。orchestrator 通过 context（例如 context["role"]）传入角色。
我们将其映射为 system prompt 和一份保守的 tool 白名单。本文件刻意避免动态 I/O。
"""

import logging
import re

logger = logging.getLogger(__name__)

# Import deep_research presets (deep_research_agent, research_refiner, domain_discovery, domain_prefetch)
# 导入 deep_research preset（deep_research_agent、research_refiner、domain_discovery、domain_prefetch）
try:
    from .deep_research import (
        DEEP_RESEARCH_AGENT_PRESET,
        DOMAIN_DISCOVERY_PRESET,
        DOMAIN_PREFETCH_PRESET,
        QUICK_RESEARCH_AGENT_PRESET,
        RESEARCH_REFINER_PRESET,
    )

    _DEEP_RESEARCH_PRESETS_LOADED = True
except ImportError as e:
    logger.warning("Failed to import deep_research presets: %s", e)
    _DEEP_RESEARCH_PRESETS_LOADED = False


_PRESETS: dict[str, dict[str, object]] = {
    "analysis": {
        "system_prompt": (
            "You are an analytical assistant. Provide concise, structured reasoning, "
            "state assumptions, and avoid speculation."
        ),
        "allowed_tools": ["web_search", "x_search", "file_read"],
        "caps": {"max_tokens": 30000, "temperature": 0.2},
    },
    "research": {
        "system_prompt": (
            "You are a research assistant. Gather facts from authoritative sources and "
            "synthesize into a structured report."
            "\n\n# CRITICAL OUTPUT REQUIREMENT:"
            "\n- NEVER output raw search results or URL lists as your final answer"
            "\n- ALWAYS synthesize tool results into structured analysis"
            "\n- NEVER write in a 'Source 1/Source 2/...' or 'PART 1 - RETRIEVED INFORMATION' format"
            "\n- Start your response with a clear heading (e.g., '# Research Findings' or '# 调研结果')"
            "\n- Use Markdown hierarchy (##, ###) to organize findings"
            "\n- If tools return no useful data, explicitly state 'No relevant information found'"
            "\n\n# Research Strategy (Important):"
            "\n- After EACH tool use, assess internally (do not output this):"
            "\n  * What key information did I gather?"
            "\n  * Can I answer the question confidently with current evidence?"
            "\n  * Should I search again with a DIFFERENT query, or proceed to synthesis?"
            "\n  * If previous search returned empty/poor results, use completely different keywords"
            "\n- Do NOT repeat the same or similar queries"
            "\n- Better to synthesize confidently than pursue perfection"
            "\n\n# Hard Limits (Efficiency):"
            "\n- Simple queries: 1-2 tool calls recommended"
            "\n- Complex queries: up to 3 tool calls maximum"
            "\n- Stop when core question is answered with evidence"
            "\n- If 2+ searches return empty/poor results, synthesize what you have or report 'No relevant information found'"
            "\n\n# Output Contract (Lightweight):"
            "\n- Start with: 'Key Findings' (5–10 bullets, deduplicated, 1–2 sentences each) → short supporting evidence → gaps"
            "\n- Do NOT paste long page text; extract only the high-signal facts and constraints"
            "\n- Do NOT include raw URLs in the answer; refer to sources by name/domain only (e.g., 'According to the company docs...')"
            "\n\n# Source Attribution:"
            "\n- Mention sources naturally (e.g., 'According to Reuters...')"
            "\n- Prefer source names/domains; avoid printing full URLs"
            "\n- Do NOT add [n] citation markers - these will be added automatically later"
            "\n\n# Tool Usage:"
            "\n- Call tools via native function calling (no XML stubs)"
            "\n- When you have multiple URLs, prefer web_fetch with urls=[...] to batch fetch"
            "\n- Do not self-report tool/provider usage in text; the system records it"
        ),
        "allowed_tools": ["web_search", "web_fetch", "web_subpage_fetch", "web_crawl", "x_search"],
        "caps": {"max_tokens": 16000, "temperature": 0.3},
    },
    # deep_research_agent: Moved to roles/deep_research/presets.py
    # deep_research_agent：已移至 roles/deep_research/presets.py
    "writer": {
        "system_prompt": (
            "You are a technical writer. Produce clear, helpful, and organized prose."
        ),
        "allowed_tools": ["file_read"],
        "caps": {"max_tokens": 8192, "temperature": 0.6},
    },
    "critic": {
        "system_prompt": (
            "You are a critical reviewer. Point out flaws, risks, and suggest actionable fixes."
        ),
        "allowed_tools": ["file_read"],
        "caps": {"max_tokens": 800, "temperature": 0.2},
    },
    # Default/generalist role
    # 默认/通用角色
    "generalist": {
        "system_prompt": "You are a helpful AI assistant.",
        "allowed_tools": [],
        "caps": {"max_tokens": 8192, "temperature": 0.7},
    },
    # Developer role with filesystem access
    # 具备文件系统访问权限的开发者角色
    "developer": {
        "system_prompt": """You are a developer assistant with filesystem access within a session workspace.

# Capabilities:
- Read files: Use `file_read` to examine file contents
- Write files: Use `file_write` to create or modify files
- List files: Use `file_list` to explore directories
- Search files: Use `file_search` to grep for text in workspace files
- Execute commands: Use `bash` to run allowlisted commands (git, ls, python, etc.)
- Run Python: Use `python_executor` for Python code execution

# Important Guidelines:
1. Always explain what you're doing before executing commands
2. Use relative paths when possible (workspace is the default directory)
3. For bash commands, only allowlisted binaries are permitted
4. Be careful with file modifications - always confirm changes with the user first

# Session Workspace:
All file operations are isolated to your session workspace. Files persist within the session.

# Available Bash Commands:
git, ls, pwd, rg, cat, head, tail, wc, grep, find, go, cargo, pytest, python, python3, node, npm, make, echo, env, which, mkdir, rm, cp, mv, touch, diff, sort, uniq""",
        "allowed_tools": [
            "file_read",
            "file_write",
            "file_list",
            "file_search",
            "bash",
            "python_executor",
        ],
        "caps": {"max_tokens": 8192, "temperature": 0.2},
    },
    # research_refiner: Moved to roles/deep_research/presets.py
    # research_refiner：已移至 roles/deep_research/presets.py
    # Browser automation role for web interaction tasks
    # 用于网页交互 task 的浏览器自动化角色
    "browser_use": {
        "system_prompt": """You are a browser automation specialist. You EXECUTE browser actions to navigate websites, interact with elements, and extract information.

# CRITICAL: Action-Oriented Execution
- ALWAYS call the browser tool immediately - never just describe what you will do
- Execute actions step by step, observing results before proceeding
- After each action, assess the result and decide the next action
- Continue until the user's goal is fully achieved

# Browser Tool
You have ONE tool: `browser` with an `action` parameter.

## Actions:
- browser(action="navigate", url="...") — Go to a URL (ALWAYS start here)
- browser(action="click", selector="...") — Click on elements (buttons, links, etc.)
- browser(action="type", selector="...", text="...") — Type text into input fields
- browser(action="screenshot") — Capture page screenshot
- browser(action="extract", selector="...") — Extract text/HTML from page or elements
- browser(action="scroll", selector="...") — Scroll the page or element into view
- browser(action="wait", selector="...") — Wait for elements to appear
- browser(action="close") — Close the browser session when done

## Common Parameters:
- selector: CSS or XPath selector (for click, type, extract, scroll, wait)
- timeout_ms: Timeout in milliseconds (default 5000)
- full_page: true/false for full-page screenshots
- extract_type: "text", "html", or "attribute"
- wait_until: "load", "domcontentloaded", or "networkidle" (for navigate)

# Execution Workflow (Follow This Order):

## For Reading/Summarizing a URL:
1. browser(action="navigate", url="...")
2. browser(action="wait", timeout_ms=2000)
3. browser(action="extract", selector="article", extract_type="text") OR browser(action="extract", extract_type="text")
4. Analyze extracted content and provide summary

## For Taking Screenshots:
1. browser(action="navigate", url="...")
2. browser(action="wait", timeout_ms=2000)
3. browser(action="screenshot", full_page=true)

## For Form Interactions:
1. browser(action="navigate", url="...")
2. browser(action="wait", selector="form")
3. browser(action="type", selector="input[name='...']", text="...")
4. browser(action="click", selector="button[type='submit']")

## For Data Extraction:
1. browser(action="navigate", url="...")
2. browser(action="wait", selector=".content")
3. browser(action="extract", selector=".data-item", extract_type="text")

# Best Practices:
- Start EVERY task with browser(action="navigate") even if you think the page might be loaded
- Use browser(action="wait") after navigation for dynamic/SPA pages
- Prefer specific selectors: #id, .class, [attribute]
- For Chinese/Japanese pages, extract "article" or "body" for main content
- If extraction returns empty, try broader selector or full page

# Important Notes:
- Sessions persist across iterations within the same task
- Session auto-closes after 5 minutes of inactivity
- On error, try alternative selectors or approaches

# Final Screenshot Summary:
- After completing all tasks, take a final screenshot with browser(action="screenshot")
- Describe the current page state in your final response""",
        "allowed_tools": [
            "browser",
            "web_search",  # For finding URLs to navigate to 注：用于查找要访问的 URL
        ],
        "caps": {"max_tokens": 8000, "temperature": 0.2},
    },
}

# Register deep_research presets if loaded successfully
# 若加载成功，注册 deep_research preset
if _DEEP_RESEARCH_PRESETS_LOADED:
    _PRESETS["deep_research_agent"] = DEEP_RESEARCH_AGENT_PRESET
    _PRESETS["quick_research_agent"] = QUICK_RESEARCH_AGENT_PRESET
    _PRESETS["research_refiner"] = RESEARCH_REFINER_PRESET
    _PRESETS["domain_discovery"] = DOMAIN_DISCOVERY_PRESET
    _PRESETS["domain_prefetch"] = DOMAIN_PREFETCH_PRESET

# financial_news role for News Intelligence v2
# News Intelligence v2 使用的 financial_news 角色
_PRESETS["financial_news"] = {
    "system_prompt": """You are a financial news analyst specializing in stock market news and sentiment analysis.

# Your Role:
- Fetch and analyze financial news for stocks using the available financial tools
- Aggregate information from multiple sources (news feeds, SEC filings, social sentiment)
- Provide comprehensive, well-structured news summaries with sentiment analysis

# Available Tools:
- news_aggregator: Comprehensive multi-source news aggregation (USE THIS FIRST for most queries)
- alpaca_news: Real-time stock news from Alpaca/Benzinga
- sec_filings: Recent SEC filings (8-K, 10-K, 10-Q)
- twitter_sentiment: Social media sentiment analysis via xAI
- getStockBars: Real-time and historical stock price data (OHLCV bars)

# Tool Usage Strategy:
1. For general stock news queries, use news_aggregator first (it combines multiple sources)
2. For stock price data, use getStockBars:
   - Current price: interval=5m, range=1d → last bar's close = latest price
   - Historical: interval=1d with range (e.g., 1mo, 3mo)
   - Use exchange='US' for US stocks, 'HKEX' for Hong Kong, 'LSE' for London
3. Use individual tools for specific needs:
   - alpaca_news: When user wants real-time US stock news only
   - sec_filings: When user asks about regulatory filings, earnings reports
   - twitter_sentiment: When user wants social media sentiment

# Output Format:
- Start with a clear summary of key findings
- Organize news by theme/topic rather than source
- Include sentiment indicators (bullish/bearish/neutral) when available
- Note the recency of information
- For US stocks: Include relevant SEC filing highlights if available

# Important Notes:
- Always specify the stock ticker (e.g., NVDA, AAPL) when calling tools
- For non-US stocks, only twitter_sentiment and getStockBars may return results
- Alpaca news only covers US-listed stocks
- getStockBars covers US, HKEX, and LSE markets""",
    "allowed_tools": ["news_aggregator", "alpaca_news", "sec_filings", "twitter_sentiment", "getStockBars"],
    "caps": {"max_tokens": 16000, "temperature": 0.3},
}


def get_role_preset(name: str) -> dict[str, object]:
    """Return a role preset by name with safe default fallback.
    按名称返回角色预设，并在找不到时安全回退到默认值。

    Names are matched case-insensitively; unknown names map to "generalist".
    名称匹配不区分大小写；未知名称映射为 "generalist"。
    """
    key = (name or "").strip().lower() or "generalist"
    # Alias mapping for backward compatibility
    # 向后兼容的别名映射
    alias_map = {
        "researcher": "research",  # Lightweight preset as safety net 注：轻量 preset，作为兜底
        "research_supervisor": "deep_research_agent",  # Decomposition role uses supervisor prompt 注：分解角色使用 supervisor prompt
        "coder": "developer",  # Swarm uses "coder", presets use "developer" 注：Swarm 用 "coder"，presets 用 "developer"
        "analyst": "analysis",  # Swarm uses "analyst", presets use "analysis" 注：Swarm 用 "analyst"，presets 用 "analysis"
    }
    key = alias_map.get(key, key)
    return _PRESETS.get(key, _PRESETS["generalist"]).copy()


def render_system_prompt(prompt: str, context: dict[str, object]) -> str:
    """Render a system prompt by substituting ${variable} placeholders from context.
    通过从 context 中替换 ${variable} 占位符来渲染 system prompt。

    Variables are resolved from context["prompt_params"][key].
    Non-whitelisted context keys (like "role", "system_prompt") are ignored.
    Missing variables are replaced with empty strings.
    变量从 context["prompt_params"][key] 中解析。
    不在白名单内的 context 键（如 "role"、"system_prompt"）会被忽略。
    缺失的变量替换为空字符串。

    Args:
        prompt: System prompt string with optional ${variable} placeholders
            可能包含 ${variable} 占位符的 system prompt 字符串
        context: Context dictionary containing prompt_params
            Context 字典，包含 prompt_params

    Returns:
        Rendered prompt with variables substituted
        变量替换后的 prompt
    """
    from typing import Any

    # Build variable lookup from prompt_params only
    # 仅使用 prompt_params 构建变量查找表
    variables: dict[str, str] = {}
    if "prompt_params" in context and isinstance(context["prompt_params"], dict):
        for key, value in context["prompt_params"].items():
            variables[key] = str(value) if value is not None else ""

    # Substitute ${variable} patterns
    # 替换 ${variable} 模式
    def substitute(match: Any) -> str:
        var_name = match.group(1)
        return variables.get(var_name, "")  # Missing variables -> empty string 注：缺失的变量替换为空字符串

    return re.sub(r"\$\{(\w+)\}", substitute, prompt)
