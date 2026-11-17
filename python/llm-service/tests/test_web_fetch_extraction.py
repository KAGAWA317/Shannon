"""
Tests for web_fetch prompt-guided extraction feature (issue #38).

Tests the extract_prompt parameter across all three web tools:
- web_fetch (single URL, batch mode)
- web_subpage_fetch
- web_crawl

Verifies: disabled path unchanged, extraction happy path, fallback on failure,
timeout handling, cost/token tracking, and batch mode.

web_fetch 的 prompt 引导抽取功能测试（issue #38）。

覆盖三个 web tool 中的 extract_prompt 参数：
- web_fetch（单 URL、批量模式）
- web_subpage_fetch
- web_crawl

验证点：功能关闭时行为不变、抽取成功路径、失败回退、timeout 处理、
成本/token 统计，以及批量模式。
"""

import asyncio
from dataclasses import dataclass
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from llm_service.tools.base import ToolResult
from llm_service.tools.builtin.web_fetch import (
    EXTRACTION_CONTENT_CAP,
    EXTRACTION_INTERNAL_MAX,
    WebFetchTool,
    apply_extraction,
    extract_batch_with_llm,
    extract_with_llm,
)

# --- Helpers ---
# --- 辅助函数 ---

def _make_tool_result(content: str, max_length: int = 10000) -> ToolResult:
    """Create a ToolResult simulating a fetched page.
    构造一个模拟已抓取页面的 ToolResult。
    """
    return ToolResult(
        success=True,
        output={
            "url": "https://example.com",
            "title": "Example",
            "content": content,
            "char_count": len(content),
            "word_count": len(content.split()),
            "truncated": len(content) >= max_length,
            "method": "pure_python",
            "pages_fetched": 1,
            "tool_source": "fetch",
            "status_code": 200,
            "blocked_reason": None,
        },
        metadata={"fetch_method": "pure_python"},
    )


@dataclass
class FakeUsage:
    total_tokens: int = 500
    estimated_cost: float = 0.001


@dataclass
class FakeResponse:
    content: str = "Extracted: pricing is $10/mo"
    model: str = "claude-haiku-4-5-20251001"
    usage: FakeUsage = None

    def __post_init__(self):
        if self.usage is None:
            self.usage = FakeUsage()


def _mock_llm_manager():
    """Create a mock LLMManager that returns a FakeResponse.
    构造一个返回 FakeResponse 的 mock LLMManager。
    """
    manager = MagicMock()
    manager.complete = AsyncMock(return_value=FakeResponse())
    return manager


# --- Tests for extract_with_llm ---
# --- extract_with_llm 的测试 ---

class TestExtractWithLlm:
    """Tests for the extract_with_llm helper function.
    extract_with_llm 辅助函数的测试。
    """

    @pytest.mark.asyncio
    async def test_happy_path(self):
        """Extraction returns (text, tokens, cost, model) on success.
        抽取成功时返回 (text, tokens, cost, model)。
        """
        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_with_llm("long content here", "extract pricing")

        assert result is not None
        text, tokens, cost, model = result
        assert text == "Extracted: pricing is $10/mo"
        assert tokens == 500
        assert cost == 0.001
        assert "haiku" in model

    @pytest.mark.asyncio
    async def test_returns_none_on_exception(self):
        """Extraction returns None when LLM call raises.
        LLM 调用抛 exception 时，抽取返回 None。
        """
        manager = MagicMock()
        manager.complete = AsyncMock(side_effect=RuntimeError("API down"))
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_with_llm("content", "extract something")

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_none_on_timeout(self):
        """Extraction returns None when LLM call times out.
        LLM 调用 timeout 时，抽取返回 None。
        """
        async def slow_complete(*args, **kwargs):
            await asyncio.sleep(100)
            return FakeResponse()

        manager = MagicMock()
        manager.complete = slow_complete
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_with_llm("content", "extract something", timeout=0.01)

        assert result is None

    @pytest.mark.asyncio
    async def test_caps_input_content(self):
        """Content exceeding EXTRACTION_CONTENT_CAP is truncated before sending to LLM.
        超过 EXTRACTION_CONTENT_CAP 的内容在发给 LLM 之前会被截断。
        """
        manager = _mock_llm_manager()
        huge_content = "x" * (EXTRACTION_CONTENT_CAP + 10000)

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_with_llm(huge_content, "extract something")

        assert result is not None
        # Verify the content sent to LLM was capped
        # 校验发给 LLM 的内容确实被截断过
        call_args = manager.complete.call_args
        user_msg = call_args.kwargs["messages"][1]["content"]
        # The page content portion should be capped
        # 页面正文部分应被截断到上限
        assert len(user_msg) <= EXTRACTION_CONTENT_CAP + 200  # +200 for prompt prefix 注：+200 为 prompt 前缀预留


# --- Tests for apply_extraction ---
# --- apply_extraction 的测试 ---

class TestApplyExtraction:
    """Tests for the apply_extraction post-processing function.
    apply_extraction 后处理函数的测试。
    """

    @pytest.mark.asyncio
    async def test_generic_extraction_when_prompt_is_none(self):
        """When extract_prompt is None, generic extraction still runs on over-length content.
        extract_prompt 为 None 时，超长内容仍会走通用抽取。
        """
        original_content = "x" * 20000
        result = _make_tool_result(original_content)

        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed = await apply_extraction(result, None, 10000)

        # Extraction runs even without extract_prompt
        # 即使没有 extract_prompt，抽取也会执行
        assert processed.output["extracted"] is True
        manager.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_extraction_when_content_fits(self):
        """When content fits within max_length, no extraction occurs.
        内容未超过 max_length 时不做抽取。
        """
        short_content = "short page content"
        result = _make_tool_result(short_content)

        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed = await apply_extraction(result, "extract pricing", 10000)

        assert processed.output["content"] == short_content
        manager.complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_extraction_replaces_content(self):
        """When extraction succeeds, content is replaced with extracted text.
        抽取成功时，content 被替换为抽取出的文本。
        """
        long_content = "x" * 20000
        result = _make_tool_result(long_content)

        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed = await apply_extraction(result, "extract pricing", 10000)

        assert processed.output["content"] == "Extracted: pricing is $10/mo"
        assert processed.output["extracted"] is True
        assert processed.output["truncated"] is True
        assert processed.metadata["extraction_model"] == "claude-haiku-4-5-20251001"
        assert processed.metadata["extraction_tokens"] == 500
        assert processed.metadata["extraction_cost_usd"] == 0.001
        assert processed.tokens_used == 500
        assert processed.cost_usd == 0.001

    @pytest.mark.asyncio
    async def test_fallback_to_truncation_on_failure(self):
        """When extraction fails, content is hard-truncated to original max_length.
        抽取失败时，内容按原始 max_length 硬截断。
        """
        long_content = "a" * 5000 + "b" * 5000 + "c" * 10000
        result = _make_tool_result(long_content)
        original_max = 10000

        manager = MagicMock()
        manager.complete = AsyncMock(side_effect=RuntimeError("API down"))
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed = await apply_extraction(result, "extract pricing", original_max)

        assert len(processed.output["content"]) == original_max
        assert processed.output["content"] == long_content[:original_max]
        assert processed.output["extracted"] is False
        assert processed.output["truncated"] is True

    @pytest.mark.asyncio
    async def test_no_extraction_on_failed_result(self):
        """Failed ToolResults are returned unchanged.
        失败的 ToolResult 原样返回。
        """
        result = ToolResult(success=False, output=None, error="fetch failed")
        processed = await apply_extraction(result, "extract pricing", 10000)

        assert processed.success is False
        assert processed.error == "fetch failed"

    @pytest.mark.asyncio
    async def test_cost_fields_accumulate(self):
        """Extraction cost adds to existing ToolResult cost fields.
        抽取成本会累加到 ToolResult 已有的 cost 字段上。
        """
        long_content = "x" * 20000
        result = _make_tool_result(long_content)
        result.tokens_used = 100  # Pre-existing tokens from tool 注：tool 原有的 tokens
        result.cost_usd = 0.005  # Pre-existing cost 注：原有的 cost

        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed = await apply_extraction(result, "extract pricing", 10000)

        assert processed.tokens_used == 600  # 100 + 500 注：100 + 500
        assert processed.cost_usd == pytest.approx(0.006)  # 0.005 + 0.001 注：0.005 + 0.001


# --- Tests for WebFetchTool integration ---
# --- WebFetchTool 集成测试 ---

class TestWebFetchExtractionIntegration:
    """Integration tests for extract_prompt in WebFetchTool._execute_impl.
    WebFetchTool._execute_impl 中 extract_prompt 的集成测试。
    """

    @pytest.fixture
    def tool(self):
        tool = WebFetchTool()
        tool.firecrawl_provider = None
        tool.exa_api_key = None
        return tool

    @pytest.mark.asyncio
    async def test_short_content_no_extraction(self):
        """Short content (fits within max_length) is returned as-is, no extraction.
        短内容（未超过 max_length）原样返回，不做抽取。
        """
        tool = WebFetchTool()
        tool.firecrawl_provider = None
        tool.exa_api_key = None

        short_content = "short page"

        async def mock_fetch(url, max_length, subpages=0):
            return ToolResult(
                success=True,
                output={
                    "url": url,
                    "title": "Test",
                    "content": short_content,
                    "char_count": len(short_content),
                    "word_count": 2,
                    "truncated": False,
                    "method": "pure_python",
                    "pages_fetched": 1,
                    "tool_source": "fetch",
                    "status_code": 200,
                    "blocked_reason": None,
                },
                metadata={"fetch_method": "pure_python"},
            )

        with patch.object(tool, "_fetch_pure_python", side_effect=mock_fetch):
            result = await tool.execute(url="https://example.com", max_length=10000)

        assert result.success
        assert result.output["content"] == short_content
        assert "extracted" not in result.output

    @pytest.mark.asyncio
    async def test_always_inflates_max_length_and_extracts(self):
        """Internal max_length is always inflated; extraction runs on over-length content.
        内部 max_length 总会被放大；超长内容会走抽取。
        """
        tool = WebFetchTool()
        tool.firecrawl_provider = None
        tool.exa_api_key = None

        captured_max_length = None

        async def mock_fetch(url, max_length, subpages=0):
            nonlocal captured_max_length
            captured_max_length = max_length
            content = "x" * min(50000, max_length)
            return ToolResult(
                success=True,
                output={
                    "url": url,
                    "title": "Test",
                    "content": content,
                    "char_count": len(content),
                    "word_count": 1,
                    "truncated": False,
                    "method": "pure_python",
                    "pages_fetched": 1,
                    "tool_source": "fetch",
                    "status_code": 200,
                    "blocked_reason": None,
                },
                metadata={"fetch_method": "pure_python"},
            )

        manager = _mock_llm_manager()
        with patch.object(tool, "_fetch_pure_python", side_effect=mock_fetch), \
             patch("llm_provider.manager.get_llm_manager", return_value=manager):
            # No extract_prompt — generic extraction should still run
            # 没有 extract_prompt —— 通用抽取仍应执行
            result = await tool.execute(
                url="https://example.com",
                max_length=10000,
            )

        # Provider should have received the inflated max_length
        # provider 应收到被放大后的 max_length
        assert captured_max_length == EXTRACTION_INTERNAL_MAX
        # Result should have extracted content (generic extraction)
        # 结果应带有抽取后的内容（通用抽取）
        assert result.success
        assert result.output["extracted"] is True
        assert result.output["truncated"] is True


# --- Edge case tests for length clamping and truncated consistency ---
# --- 长度钳制与 truncated 一致性的边界测试 ---

class TestExtractionLengthClamping:
    """Verify extracted content respects max_length and truncated is always set.
    校验抽取结果遵守 max_length，且 truncated 一定被设置。
    """

    @pytest.mark.asyncio
    async def test_low_max_length_clamps_extraction(self):
        """When max_length=1000, extracted content must not exceed 1000 chars.
        max_length=1000 时，抽取结果不得超过 1000 字符。
        """
        long_content = "x" * 20000
        result = _make_tool_result(long_content)

        # LLM returns 2000 chars — exceeds max_length of 1000
        # LLM 返回 2000 字符 —— 超过 max_length 的 1000
        long_extraction = "y" * 2000
        manager = MagicMock()
        manager.complete = AsyncMock(return_value=FakeResponse(content=long_extraction))

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed = await apply_extraction(result, None, 1000)

        assert len(processed.output["content"]) <= 1000
        assert processed.output["extracted"] is True
        assert processed.output["truncated"] is True

    @pytest.mark.asyncio
    async def test_max_output_tokens_capped_to_max_length(self):
        """extract_with_llm max_tokens should be min(max_length, 4000).
        extract_with_llm 的 max_tokens 应为 min(max_length, 4000)。
        """
        long_content = "x" * 20000
        result = _make_tool_result(long_content)

        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            await apply_extraction(result, None, 800)

        # Verify max_tokens passed to LLM is capped at 800 (not 4000)
        # 校验传给 LLM 的 max_tokens 被钳制到 800（而不是 4000）
        call_kwargs = manager.complete.call_args.kwargs
        assert call_kwargs["max_tokens"] == 800

    @pytest.mark.asyncio
    async def test_success_and_fallback_both_set_truncated(self):
        """Both extraction success and fallback paths set truncated=True.
        抽取成功与回退两条路径都应设置 truncated=True。
        """
        long_content = "x" * 20000

        # Success path
        # 成功路径
        result_ok = _make_tool_result(long_content)
        manager = _mock_llm_manager()
        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            processed_ok = await apply_extraction(result_ok, None, 10000)
        assert processed_ok.output["truncated"] is True
        assert processed_ok.output["extracted"] is True

        # Fallback path
        # 回退路径
        result_fail = _make_tool_result(long_content)
        manager_fail = MagicMock()
        manager_fail.complete = AsyncMock(side_effect=RuntimeError("fail"))
        with patch("llm_provider.manager.get_llm_manager", return_value=manager_fail):
            processed_fail = await apply_extraction(result_fail, None, 10000)
        assert processed_fail.output["truncated"] is True
        assert processed_fail.output["extracted"] is False


# --- Tests for research_mode no longer skipping extraction ---
# --- research_mode 不再跳过抽取的测试 ---

class TestResearchModeSkipsExtraction:
    """research_mode bypasses LLM extraction — OODA loop handles raw content analysis.
    Explicit extract_prompt still triggers extraction regardless of mode.
    显式的 extract_prompt 仍然会触发抽取，与模式无关。
    """

    @pytest.fixture
    def tool(self):
        tool = WebFetchTool()
        tool.firecrawl_provider = None
        tool.exa_api_key = None
        return tool

    def _mock_fetch(self, content: str):
        """Return an async mock for _fetch_pure_python that returns given content.
        返回一个按给定内容响应 _fetch_pure_python 的 async mock。
        """
        async def mock_fetch(url, max_length, subpages=0):
            return ToolResult(
                success=True,
                output={
                    "url": url,
                    "title": "Test",
                    "content": content,
                    "char_count": len(content),
                    "word_count": len(content.split()),
                    "truncated": False,
                    "method": "pure_python",
                    "pages_fetched": 1,
                    "tool_source": "fetch",
                    "status_code": 200,
                    "blocked_reason": None,
                },
                metadata={"fetch_method": "pure_python"},
            )
        return mock_fetch

    @pytest.mark.asyncio
    async def test_single_url_skips_extraction_in_research_mode(self, tool):
        """research_mode=True => extraction skipped, hard-truncated instead.
        research_mode=True ⇒ 跳过抽取，改为硬截断。
        """
        long_content = "x" * 20000

        manager = _mock_llm_manager()
        with patch.object(tool, "_fetch_pure_python", side_effect=self._mock_fetch(long_content)), \
             patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await tool.execute(
                session_context={"research_mode": True},
                url="https://example.com",
                max_length=10000,
            )

        # Extraction should NOT run — research mode hard-truncates
        # 不应执行抽取 —— research mode 直接硬截断
        assert result.success
        assert result.output.get("extracted") is not True
        manager.complete.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_url_extracts_without_research_mode(self, tool):
        """No research_mode => apply_extraction IS called (existing behavior unchanged).
        没有 research_mode ⇒ 会调用 apply_extraction（原有行为不变）。
        """
        long_content = "x" * 20000

        manager = _mock_llm_manager()
        with patch.object(tool, "_fetch_pure_python", side_effect=self._mock_fetch(long_content)), \
             patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await tool.execute(
                url="https://example.com",
                max_length=10000,
            )

        # Extraction should have run (generic extraction on over-length content)
        # 应已执行抽取（对超长内容做通用抽取）
        assert result.success
        assert result.output.get("extracted") is True
        manager.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_skips_extraction_in_research_mode(self):
        """Batch mode: research_mode=True => batch extraction skipped, hard-truncated.
        批量模式：research_mode=True ⇒ 跳过批量抽取，改为硬截断。
        """
        tool = WebFetchTool()
        long_content = "x" * 20000

        with patch.object(tool, "_fetch_pure_python", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = ToolResult(
                success=True,
                output={
                    "url": "https://example.com",
                    "title": "Test",
                    "content": long_content,
                    "char_count": len(long_content),
                    "method": "pure_python",
                    "status_code": 200,
                    "blocked_reason": None,
                },
            )

            with patch(
                "llm_service.tools.builtin.web_fetch.extract_batch_with_llm",
                new_callable=AsyncMock,
            ) as mock_batch_extract:
                result = await tool._fetch_batch(
                    ["https://example.com/1", "https://example.com/2"],
                    session_context={"research_mode": True},
                    max_length=10000,
                    total_chars_cap=200000,
                )

                # Batch extraction should NOT be called — research mode hard-truncates
                # 不应调用批量抽取 —— research mode 直接硬截断
                mock_batch_extract.assert_not_called()
                assert result.success

    @pytest.mark.asyncio
    async def test_extract_prompt_ignored_in_research_mode(self, tool):
        """extract_prompt + research_mode=True => extraction still skipped (research mode wins).
        extract_prompt + research_mode=True ⇒ 仍然跳过抽取（research mode 优先）。
        """
        long_content = "x" * 20000

        manager = _mock_llm_manager()
        with patch.object(tool, "_fetch_pure_python", side_effect=self._mock_fetch(long_content)), \
             patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await tool.execute(
                session_context={"research_mode": True},
                url="https://example.com",
                max_length=10000,
                extract_prompt="extract pricing info",
            )

        # Research mode wins — extraction skipped even with extract_prompt
        # research mode 优先 —— 即使给了 extract_prompt 也跳过抽取
        assert result.success
        assert result.output.get("extracted") is not True
        manager.complete.assert_not_called()


# --- Tests for extract_batch_with_llm ---
# --- extract_batch_with_llm 的测试 ---

class TestExtractBatchWithLlm:
    """Tests for the batch extraction function that combines N pages into 1 LLM call.
    批量抽取函数的测试：把 N 个页面合并进 1 次 LLM 调用。
    """

    @pytest.mark.asyncio
    async def test_batch_happy_path(self):
        """2 pages, LLM returns both with delimiters, verify dict[page_id->text], single LLM call.
        2 个页面，LLM 用分隔符返回两段结果；校验 dict[page_id->text] 且只调用一次 LLM。
        """
        pages = [
            {"page_id": 0, "url": "https://a.com", "title": "Page A", "content": "Content of page A " * 100},
            {"page_id": 1, "url": "https://b.com", "title": "Page B", "content": "Content of page B " * 100},
        ]
        llm_output = (
            "<<<PAGE_RESULT 0>>>\nExtracted from page A\n<<<END_PAGE_RESULT 0>>>\n"
            "<<<PAGE_RESULT 1>>>\nExtracted from page B\n<<<END_PAGE_RESULT 1>>>"
        )
        manager = MagicMock()
        manager.complete = AsyncMock(return_value=FakeResponse(content=llm_output))

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_batch_with_llm(pages, per_page_budget=2000)

        assert result is not None
        extracted, tokens, cost, model = result
        assert isinstance(extracted, dict)
        assert extracted[0] == "Extracted from page A"
        assert extracted[1] == "Extracted from page B"
        assert tokens == 500
        assert cost == 0.001
        # Single LLM call for both pages
        # 两个页面共用一次 LLM 调用
        manager.complete.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_with_query(self):
        """Verify extract_prompt appears in the LLM prompt message.
        校验 extract_prompt 出现在 LLM 的 prompt message 中。
        """
        pages = [
            {"page_id": 0, "url": "https://a.com", "title": "Page A", "content": "Some content here"},
        ]
        llm_output = "<<<PAGE_RESULT 0>>>\nPricing info\n<<<END_PAGE_RESULT 0>>>"
        manager = MagicMock()
        manager.complete = AsyncMock(return_value=FakeResponse(content=llm_output))

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_batch_with_llm(pages, extract_prompt="extract pricing")

        assert result is not None
        # Verify extract_prompt was included in the user message
        # 校验 extract_prompt 被包含进 user message
        call_args = manager.complete.call_args
        user_msg = call_args.kwargs["messages"][1]["content"]
        assert "extract pricing" in user_msg

    @pytest.mark.asyncio
    async def test_batch_returns_none_on_failure(self):
        """LLM raises exception -> returns None.
        LLM 抛 exception -> 返回 None。
        """
        pages = [
            {"page_id": 0, "url": "https://a.com", "title": "Page A", "content": "Content"},
        ]
        manager = MagicMock()
        manager.complete = AsyncMock(side_effect=RuntimeError("API down"))

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_batch_with_llm(pages)

        assert result is None

    @pytest.mark.asyncio
    async def test_batch_missing_page_returns_empty(self):
        """LLM only returns page 0, page 1 gets empty string.
        LLM 只返回 page 0，page 1 得到空字符串。
        """
        pages = [
            {"page_id": 0, "url": "https://a.com", "title": "Page A", "content": "Content A"},
            {"page_id": 1, "url": "https://b.com", "title": "Page B", "content": "Content B"},
        ]
        # LLM only returns result for page 0
        # LLM 只返回了 page 0 的结果
        llm_output = "<<<PAGE_RESULT 0>>>\nExtracted A\n<<<END_PAGE_RESULT 0>>>"
        manager = MagicMock()
        manager.complete = AsyncMock(return_value=FakeResponse(content=llm_output))

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_batch_with_llm(pages)

        assert result is not None
        extracted, _, _, _ = result
        assert extracted[0] == "Extracted A"
        assert extracted[1] == ""  # Missing page gets empty string 注：缺失的页面返回空字符串

    @pytest.mark.asyncio
    async def test_batch_caps_combined_content(self):
        """Huge content (300K chars per page) still works, content truncated per-page to fit EXTRACTION_CONTENT_CAP.
        超大内容（每页 300K 字符）仍可工作，内容按页截断以适配 EXTRACTION_CONTENT_CAP。
        """
        pages = [
            {"page_id": 0, "url": "https://a.com", "title": "Page A", "content": "x" * 300_000},
            {"page_id": 1, "url": "https://b.com", "title": "Page B", "content": "y" * 300_000},
        ]
        llm_output = (
            "<<<PAGE_RESULT 0>>>\nSummary A\n<<<END_PAGE_RESULT 0>>>\n"
            "<<<PAGE_RESULT 1>>>\nSummary B\n<<<END_PAGE_RESULT 1>>>"
        )
        manager = MagicMock()
        manager.complete = AsyncMock(return_value=FakeResponse(content=llm_output))

        with patch("llm_provider.manager.get_llm_manager", return_value=manager):
            result = await extract_batch_with_llm(pages, per_page_budget=2000)

        assert result is not None
        # Verify combined content sent to LLM does not exceed EXTRACTION_CONTENT_CAP
        # 校验发给 LLM 的合并内容不超过 EXTRACTION_CONTENT_CAP
        call_args = manager.complete.call_args
        user_msg = call_args.kwargs["messages"][1]["content"]
        assert len(user_msg) <= EXTRACTION_CONTENT_CAP + 1000  # +1000 for prompt overhead 注：+1000 为 prompt 额外开销预留


# --- Tests for _fetch_batch consolidated extraction ---
# --- _fetch_batch 合并抽取的测试 ---

class TestBatchFetchUsesConsolidatedExtraction:
    """Verify that _fetch_batch uses a single consolidated LLM call (extract_batch_with_llm)
    instead of per-URL extract_with_llm calls.
    校验 _fetch_batch 使用单次合并的 LLM 调用（extract_batch_with_llm），
    而不是对每个 URL 各调一次 extract_with_llm。
    """

    @pytest.mark.asyncio
    async def test_batch_calls_single_extraction(self):
        """Batch fetch 3 URLs with over-budget content -> extract_batch_with_llm called once,
        extract_with_llm NOT called.
        批量抓取 3 个内容超出 budget 的 URL -> extract_batch_with_llm 只被调用一次，
        extract_with_llm 不被调用。
        """
        tool = WebFetchTool()
        long_content = "x" * 20000  # Over per-URL budget of ~3333 chars (10000/3) 注：超过每 URL budget ~3333 字符（10000/3）

        with patch.object(tool, "_fetch_pure_python", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = ToolResult(
                success=True,
                output={
                    "url": "https://example.com",
                    "title": "Test",
                    "content": long_content,
                    "char_count": len(long_content),
                    "method": "pure_python",
                    "status_code": 200,
                    "blocked_reason": None,
                },
            )

            with patch(
                "llm_service.tools.builtin.web_fetch.extract_batch_with_llm",
                new_callable=AsyncMock,
            ) as mock_batch_extract, \
                 patch(
                "llm_service.tools.builtin.web_fetch.extract_with_llm",
                new_callable=AsyncMock,
            ) as mock_single_extract:
                # Simulate successful batch extraction
                # 模拟批量抽取成功
                mock_batch_extract.return_value = (
                    {0: "Extracted A", 1: "Extracted B", 2: "Extracted C"},
                    1500,
                    0.003,
                    "claude-haiku-4-5-20251001",
                )

                result = await tool._fetch_batch(
                    ["https://a.com", "https://b.com", "https://c.com"],
                    max_length=10000,
                    total_chars_cap=200000,  # High cap to avoid skipping 注：上限设高以避免被跳过
                )

                # Batch extraction called once (not per-URL)
                # 批量抽取只调用一次（不是每个 URL 一次）
                mock_batch_extract.assert_called_once()
                # Single extraction NOT called
                # 单条抽取未被调用
                mock_single_extract.assert_not_called()

                assert result.success
                assert result.tokens_used == 1500
                assert result.cost_usd == 0.003
                # All successful pages should have extracted content
                # 所有成功的页面都应带抽取后的内容
                for page in result.output["pages"]:
                    if page.get("success"):
                        assert page.get("extracted") is True

    @pytest.mark.asyncio
    async def test_batch_single_page_uses_extract_with_llm(self):
        """When only 1 page needs extraction, use extract_with_llm (not batch).
        只有 1 个页面需要抽取时，使用 extract_with_llm（而非批量）。
        """
        tool = WebFetchTool()
        long_content = "x" * 20000
        short_content = "short"

        call_count = [0]

        async def mock_fetch(url, max_length, subpages=0):
            call_count[0] += 1
            content = long_content if call_count[0] == 1 else short_content
            return ToolResult(
                success=True,
                output={
                    "url": url,
                    "title": "Test",
                    "content": content,
                    "char_count": len(content),
                    "method": "pure_python",
                    "status_code": 200,
                    "blocked_reason": None,
                },
            )

        with patch.object(tool, "_fetch_pure_python", side_effect=mock_fetch), \
             patch(
                "llm_service.tools.builtin.web_fetch.extract_batch_with_llm",
                new_callable=AsyncMock,
             ) as mock_batch_extract, \
             patch(
                "llm_service.tools.builtin.web_fetch.extract_with_llm",
                new_callable=AsyncMock,
             ) as mock_single_extract:
            mock_single_extract.return_value = (
                "Extracted content", 500, 0.001, "claude-haiku-4-5-20251001"
            )

            result = await tool._fetch_batch(
                ["https://a.com", "https://b.com"],
                max_length=10000,
            )

            # Single extraction used (only 1 page over budget)
            # 使用了单条抽取（只有 1 个页面超出 budget）
            mock_single_extract.assert_called_once()
            # Batch extraction NOT used
            # 批量抽取未被使用
            mock_batch_extract.assert_not_called()

            assert result.success
            assert result.tokens_used == 500

    @pytest.mark.asyncio
    async def test_batch_fallback_on_extraction_failure(self):
        """Batch extraction returns None -> content hard-truncated for all pages.
        批量抽取返回 None -> 所有页面内容硬截断。
        """
        tool = WebFetchTool()
        long_content = "x" * 20000

        with patch.object(tool, "_fetch_pure_python", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = ToolResult(
                success=True,
                output={
                    "url": "https://example.com",
                    "title": "Test",
                    "content": long_content,
                    "char_count": len(long_content),
                    "method": "pure_python",
                    "status_code": 200,
                    "blocked_reason": None,
                },
            )

            with patch(
                "llm_service.tools.builtin.web_fetch.extract_batch_with_llm",
                new_callable=AsyncMock,
            ) as mock_batch_extract:
                # Batch extraction fails
                # 批量抽取失败
                mock_batch_extract.return_value = None

                result = await tool._fetch_batch(
                    ["https://a.com", "https://b.com", "https://c.com"],
                    max_length=10000,
                    total_chars_cap=200000,  # High cap to avoid skipping 注：上限设高以避免被跳过
                )

                assert result.success
                per_url_budget = max(10000 // 3, 2000)
                # All successful pages should be hard-truncated (not extracted)
                # 所有成功的页面都应被硬截断（而非抽取）
                for page in result.output["pages"]:
                    if page.get("success"):
                        assert page.get("extracted") is False
                        assert page.get("truncated") is True
                        assert len(page["content"]) == per_url_budget

    @pytest.mark.asyncio
    async def test_batch_extraction_metadata_tracking(self):
        """Verify extraction tokens/cost are tracked at batch level in metadata.
        校验抽取的 token/cost 在批量层面记录到 metadata 中。
        """
        tool = WebFetchTool()
        long_content = "x" * 20000

        with patch.object(tool, "_fetch_pure_python", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = ToolResult(
                success=True,
                output={
                    "url": "https://example.com",
                    "title": "Test",
                    "content": long_content,
                    "char_count": len(long_content),
                    "method": "pure_python",
                    "status_code": 200,
                    "blocked_reason": None,
                },
            )

            with patch(
                "llm_service.tools.builtin.web_fetch.extract_batch_with_llm",
                new_callable=AsyncMock,
            ) as mock_batch_extract:
                mock_batch_extract.return_value = (
                    {0: "Extracted A", 1: "Extracted B"},
                    1200,
                    0.0025,
                    "claude-haiku-4-5-20251001",
                )

                result = await tool._fetch_batch(
                    ["https://a.com", "https://b.com"],
                    max_length=10000,
                )

                assert result.metadata["extraction_tokens"] == 1200
                assert result.metadata["extraction_cost_usd"] == 0.0025
                assert result.metadata["extraction_model"] == "claude-haiku-4-5-20251001"
                assert result.tokens_used == 1200
                assert result.cost_usd == 0.0025


class TestFirecrawlSparseFallback:
    """When firecrawl returns <500 chars, auto-fallback to pure_python.
    firecrawl 返回不足 500 字符时，自动回退到 pure_python。
    """

    @pytest.mark.asyncio
    async def test_single_url_sparse_fallback(self):
        """Single-URL path: firecrawl returns sparse content → falls back to pure_python.
        单 URL 路径：firecrawl 返回稀疏内容 → 回退到 pure_python。
        """
        tool = WebFetchTool()
        tool.exa_api_key = None

        # Mock firecrawl returning sparse content (< 500 chars)
        # mock 出返回稀疏内容（< 500 字符）的 firecrawl
        mock_firecrawl = MagicMock()
        mock_firecrawl.fetch = AsyncMock(return_value={
            "url": "https://example.com",
            "title": "Blocked",
            "content": "Please enable JavaScript.",  # Only 26 chars 注：仅 26 个字符
            "char_count": 26,
        })
        tool.firecrawl_provider = mock_firecrawl

        # Mock pure_python returning real content
        # mock 出返回真实内容的 pure_python
        good_content = "A" * 2000  # Substantive content 注：实质性内容
        python_result = ToolResult(
            success=True,
            output={
                "url": "https://example.com",
                "title": "Real Page",
                "content": good_content,
                "char_count": len(good_content),
                "word_count": 1,
                "truncated": False,
                "method": "pure_python",
                "pages_fetched": 1,
                "tool_source": "fetch",
                "status_code": 200,
                "blocked_reason": None,
            },
            metadata={"fetch_method": "pure_python"},
        )

        with patch.object(tool, "_fetch_pure_python", return_value=python_result):
            result = await tool.execute(url="https://example.com", max_length=10000)

        assert result.success
        assert result.output["content"] == good_content
        assert result.output["char_count"] == len(good_content)
        # Firecrawl was called, then fell back
        # firecrawl 被调用过，随后发生回退
        mock_firecrawl.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_single_url_no_fallback_when_enough_content(self):
        """Single-URL path: firecrawl returns >=500 chars → no fallback.
        单 URL 路径：firecrawl 返回 >=500 字符 → 不回退。
        """
        tool = WebFetchTool()
        tool.exa_api_key = None

        good_content = "B" * 600  # Above threshold 注：高于阈值
        mock_firecrawl = MagicMock()
        mock_firecrawl.fetch = AsyncMock(return_value={
            "url": "https://example.com",
            "title": "Good Page",
            "content": good_content,
            "char_count": len(good_content),
        })
        tool.firecrawl_provider = mock_firecrawl

        with patch.object(tool, "_fetch_pure_python") as mock_python:
            result = await tool.execute(url="https://example.com", max_length=10000)

        assert result.success
        assert result.output["content"] == good_content
        # pure_python should NOT have been called
        # 不应调用 pure_python
        mock_python.assert_not_called()

    @pytest.mark.asyncio
    async def test_single_url_no_fallback_for_pdf(self):
        """Single-URL path: PDF content (starts with %PDF) should NOT trigger sparse fallback.
        单 URL 路径：PDF 内容（以 %PDF 开头）不应触发稀疏回退。
        """
        tool = WebFetchTool()
        tool.exa_api_key = None

        pdf_content = "%PDF-1.4 short"  # Only 14 chars but it's a PDF 注：仅 14 个字符，但它是 PDF
        mock_firecrawl = MagicMock()
        mock_firecrawl.fetch = AsyncMock(return_value={
            "url": "https://example.com/doc.pdf",
            "title": "PDF",
            "content": pdf_content,
            "char_count": len(pdf_content),
        })
        tool.firecrawl_provider = mock_firecrawl

        with patch.object(tool, "_fetch_pure_python") as mock_python:
            result = await tool.execute(url="https://example.com/doc.pdf", max_length=10000)

        assert result.success
        # pure_python should NOT have been called for PDFs
        # PDF 不应调用 pure_python
        mock_python.assert_not_called()

    @pytest.mark.asyncio
    async def test_batch_sparse_fallback(self):
        """Batch path: firecrawl returns sparse content → falls back to pure_python.
        批量路径：firecrawl 返回稀疏内容 → 回退到 pure_python。
        """
        tool = WebFetchTool()
        tool.exa_api_key = None

        # Mock firecrawl._scrape returning sparse content
        # mock 出返回稀疏内容的 firecrawl._scrape
        mock_firecrawl = MagicMock()
        mock_firecrawl._scrape = AsyncMock(return_value={
            "url": "https://example.com",
            "title": "Blocked",
            "content": "Enable JS",  # Only 9 chars 注：仅 9 个字符
            "char_count": 9,
            "status_code": 200,
        })
        tool.firecrawl_provider = mock_firecrawl

        # Mock pure_python returning real content
        # mock 出返回真实内容的 pure_python
        good_content = "C" * 2000
        python_result = ToolResult(
            success=True,
            output={
                "url": "https://example.com",
                "title": "Real Page",
                "content": good_content,
                "char_count": len(good_content),
                "method": "pure_python",
                "status_code": 200,
                "blocked_reason": None,
            },
        )

        with patch.object(tool, "_fetch_pure_python", return_value=python_result):
            result = await tool.execute(
                urls=["https://example.com"],
                max_length=10000,
            )

        assert result.success
        pages = result.output["pages"]
        assert len(pages) == 1
        assert pages[0]["content"] == good_content
        # Firecrawl was called, then fell back
        # firecrawl 被调用过，随后发生回退
        mock_firecrawl._scrape.assert_called_once()

    @pytest.mark.asyncio
    async def test_batch_no_fallback_when_enough_content(self):
        """Batch path: firecrawl returns >=500 chars → no fallback.
        批量路径：firecrawl 返回 >=500 字符 → 不回退。
        """
        tool = WebFetchTool()
        tool.exa_api_key = None

        good_content = "D" * 600
        mock_firecrawl = MagicMock()
        mock_firecrawl._scrape = AsyncMock(return_value={
            "url": "https://example.com",
            "title": "Good Page",
            "content": good_content,
            "char_count": len(good_content),
            "status_code": 200,
        })
        tool.firecrawl_provider = mock_firecrawl

        with patch.object(tool, "_fetch_pure_python") as mock_python:
            result = await tool.execute(
                urls=["https://example.com"],
                max_length=10000,
            )

        assert result.success
        pages = result.output["pages"]
        assert len(pages) == 1
        assert pages[0]["content"] == good_content
        assert pages[0]["method"] == "firecrawl"
        # pure_python should NOT have been called
        # 不应调用 pure_python
        mock_python.assert_not_called()

    def test_sparse_fallback_logic_exists_in_source(self):
        """Verify sparse content fallback logic exists in both code paths.
        确认两条代码路径都包含稀疏内容回退逻辑。
        """
        import inspect

        from llm_service.tools.builtin import web_fetch as wf_module
        source = inspect.getsource(wf_module)
        # Both single-URL and batch paths should have sparse fallback
        # 单 URL 与批量两条路径都应有稀疏回退
        assert source.count("sparse") >= 2, \
            "Both single-URL and batch paths should have sparse fallback logic"
