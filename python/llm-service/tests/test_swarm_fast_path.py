"""Test swarm fast path: forced_tool_calls with DATA_ONLY_TOOLS skip LLM call.
swarm fast path 测试：带 DATA_ONLY_TOOLS 的 forced_tool_calls 会跳过 LLM 调用。
"""
from llm_service.api.agent import DATA_ONLY_TOOLS, generate_tool_digest


class TestDataOnlyTools:
    """Verify DATA_ONLY_TOOLS set contents.
    验证 DATA_ONLY_TOOLS 集合的内容。
    """

    def test_contains_search_tools(self):
        assert "web_search" in DATA_ONLY_TOOLS

    def test_excludes_fetch_tools(self):
        """web_fetch/web_subpage_fetch removed: agent needs full content inline.
        web_fetch/web_subpage_fetch 已移除：agent 需要内联的完整内容。
        """
        assert "web_fetch" not in DATA_ONLY_TOOLS
        assert "web_subpage_fetch" not in DATA_ONLY_TOOLS

    def test_contains_file_tools(self):
        assert "file_read" in DATA_ONLY_TOOLS
        assert "file_write" in DATA_ONLY_TOOLS
        assert "file_list" in DATA_ONLY_TOOLS
        assert "file_search" in DATA_ONLY_TOOLS

    def test_contains_utility_tools(self):
        assert "publish_data" in DATA_ONLY_TOOLS
        assert "calculator" in DATA_ONLY_TOOLS

    def test_excludes_non_data_tools(self):
        """Tools that need LLM interpretation should NOT be in DATA_ONLY_TOOLS.
        需要 LLM 解释的 tool 不应出现在 DATA_ONLY_TOOLS 中。
        """
        assert "python_executor" not in DATA_ONLY_TOOLS
        assert "web_crawl" not in DATA_ONLY_TOOLS
        assert "file_edit" not in DATA_ONLY_TOOLS

    def test_is_frozenset(self):
        """DATA_ONLY_TOOLS must be immutable.
        DATA_ONLY_TOOLS 必须是不可变的。
        """
        assert isinstance(DATA_ONLY_TOOLS, frozenset)


class TestGenerateToolDigest:
    """Verify generate_tool_digest produces usable output.
    验证 generate_tool_digest 产出可用的输出。
    """

    def test_single_successful_web_search(self):
        records = [{
            "tool": "web_search",
            "success": True,
            "output": {
                "results": [
                    {"title": "React", "snippet": "A JS library for building UIs.", "url": "https://react.dev"}
                ]
            },
        }]
        digest = generate_tool_digest("", records, max_chars=3000)
        assert digest
        assert isinstance(digest, str)
        assert "Search Results" in digest
        assert "React" in digest

    def test_empty_records_and_empty_results_returns_empty(self):
        digest = generate_tool_digest("", [], max_chars=3000)
        assert digest == ""

    def test_fallback_to_raw_tool_results(self):
        """When no records but tool_results is provided, digest uses the raw text.
        当没有 records 但提供了 tool_results 时，digest 使用原始文本。
        """
        digest = generate_tool_digest("some raw tool output", [], max_chars=3000)
        assert "some raw tool output" in digest
        assert "Tool Output Summary" in digest

    def test_respects_max_chars(self):
        long_output = "x" * 50000
        records = [{"tool": "web_search", "success": True, "output": {"results": []}}]
        digest = generate_tool_digest(long_output, records, max_chars=1000)
        assert len(digest) <= 1000

    def test_failed_tool_recorded(self):
        records = [{
            "tool": "web_search",
            "success": False,
            "error": "timeout",
            "tool_input": {"query": "test"},
        }]
        digest = generate_tool_digest("", records, max_chars=3000)
        assert "FAILED" in digest
        assert "timeout" in digest

    def test_web_fetch_digest(self):
        records = [{
            "tool": "web_fetch",
            "success": True,
            "output": {
                "pages": [{
                    "success": True,
                    "title": "Example Page",
                    "content": "A" * 200,
                    "url": "https://example.com",
                }]
            },
        }]
        digest = generate_tool_digest("", records, max_chars=3000)
        assert "Fetched Content" in digest
        assert "Example Page" in digest

    def test_multiple_records(self):
        records = [
            {
                "tool": "web_search",
                "success": True,
                "output": {"results": [{"title": "Result 1", "snippet": "First result", "url": "https://a.com"}]},
            },
            {
                "tool": "file_read",
                "success": True,
                "output": "file content here",
            },
        ]
        digest = generate_tool_digest("", records, max_chars=5000)
        assert "Search Results" in digest
        assert "Result 1" in digest
