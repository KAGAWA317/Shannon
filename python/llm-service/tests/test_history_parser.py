"""Tests for _parse_history_entries — session history deserialization.

_parse_history_entries —— session history 反序列化的测试。
"""

import pytest

from llm_service.api.agent import _parse_history_entries


class TestParseHistoryEntries:
    """Test the shared history parser that handles list, dict, and legacy string formats.

    测试统一的 history parser，兼容 list、dict 与旧版 string 格式。
    """

    # --- New format: list of strings (from Rust JSON array) ---
    # --- 新格式：字符串列表（来自 Rust JSON array） ---

    def test_list_of_strings_basic(self):
        """Standard new-format: list of 'role: content' strings.

        标准新格式：'role: content' 字符串列表。
        """
        raw = ["user: hello", "assistant: hi there"]
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert result[0] == {"role": "user", "content": "hello"}
        assert result[1] == {"role": "assistant", "content": "hi there"}

    def test_list_of_strings_with_escaped_newlines(self):
        """Content with escaped newlines should be unescaped.

        含转义换行的 content 应被还原。
        """
        raw = [r"assistant: Line one\nLine two\nLine three"]
        result = _parse_history_entries(raw)
        assert len(result) == 1
        assert result[0]["content"] == "Line one\nLine two\nLine three"

    def test_list_of_strings_real_newline_escaped_by_go(self):
        """Go escapes real newlines as backslash+n. Python should restore them.

        Go 会把真实换行转义为反斜杠+n，Python 应还原它们。
        """
        # Go: strings.ReplaceAll(content, "\n", "\\n") produces literal \n (2 chars)
        # JSON round-trip preserves this. Python receives backslash+n.
        # Go：strings.ReplaceAll 把 content 中的换行替换为字面量 反斜杠+n（2 个字符）
        # JSON 往返会保留它，Python 收到的是反斜杠+n
        raw = ["assistant: Line one\\nLine two\\nLine three"]
        result = _parse_history_entries(raw)
        assert len(result) == 1
        assert result[0]["content"] == "Line one\nLine two\nLine three"

    def test_list_of_strings_empty_entries_skipped(self):
        raw = ["user: hello", "", "  ", "assistant: world"]
        result = _parse_history_entries(raw)
        assert len(result) == 2

    def test_list_of_strings_invalid_role_skipped(self):
        raw = ["user: hello", "system: you are helpful", "assistant: hi"]
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"

    # --- New format: list of dicts (future-proof) ---
    # --- 新格式：dict 列表（面向未来扩展） ---

    def test_list_of_dicts(self):
        raw = [
            {"role": "user", "content": "hello"},
            {"role": "assistant", "content": "hi there"},
        ]
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert result[0] == {"role": "user", "content": "hello"}
        assert result[1] == {"role": "assistant", "content": "hi there"}

    def test_list_of_dicts_with_newlines_in_content(self):
        raw = [
            {"role": "assistant", "content": "Line one\nLine two"},
        ]
        result = _parse_history_entries(raw)
        assert len(result) == 1
        # Dict content should preserve newlines as-is (no double escaping)
        # dict content 中的换行应原样保留（不做二次转义）
        assert result[0]["content"] == "Line one\nLine two"

    def test_list_of_dicts_invalid_role_skipped(self):
        raw = [
            {"role": "system", "content": "ignored"},
            {"role": "user", "content": "hello"},
        ]
        result = _parse_history_entries(raw)
        assert len(result) == 1
        assert result[0]["role"] == "user"

    def test_mixed_list_strings_and_dicts(self):
        raw = [
            "user: hello",
            {"role": "assistant", "content": "hi"},
        ]
        result = _parse_history_entries(raw)
        assert len(result) == 2

    # --- Legacy format: newline-delimited string ---
    # --- 旧格式：换行分隔的字符串 ---

    def test_legacy_string_basic(self):
        raw = "user: hello\nassistant: hi there"
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert result[0] == {"role": "user", "content": "hello"}
        assert result[1] == {"role": "assistant", "content": "hi there"}

    def test_legacy_string_multiline_content_accumulated(self):
        """Multi-line assistant content should be accumulated, not truncated.

        多行 assistant content 应被累积，而不是被截断。
        """
        raw = "user: help\nassistant: Line one\nLine two\nLine three\nuser: thanks"
        result = _parse_history_entries(raw)
        assert len(result) == 3
        assert result[0] == {"role": "user", "content": "help"}
        assert result[1] == {"role": "assistant", "content": "Line one\nLine two\nLine three"}
        assert result[2] == {"role": "user", "content": "thanks"}

    def test_legacy_string_with_escaped_newlines(self):
        """Legacy format uses real newlines as delimiters. Escaped \\n within entries is unescaped.

        旧格式用真实换行作为分隔符；条目内的转义 \\n 会被还原。
        """
        # In legacy format, real \n separates turns. Escaped \\n is within content.
        # 旧格式中，真实 \n 分隔各轮次，转义 \\n 位于 content 内部。
        raw = "user: hello\nassistant: Line one\\nLine two"
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert result[0] == {"role": "user", "content": "hello"}
        assert result[1]["content"] == "Line one\nLine two"

    def test_legacy_string_empty(self):
        result = _parse_history_entries("")
        assert result == []

    def test_legacy_string_whitespace(self):
        result = _parse_history_entries("   \n\n  ")
        assert result == []

    # --- Edge cases ---
    # --- 边界情况 ---

    def test_none_input(self):
        result = _parse_history_entries(None)
        assert result == []

    def test_numeric_input(self):
        result = _parse_history_entries(12345)
        assert result == []

    def test_empty_list(self):
        result = _parse_history_entries([])
        assert result == []

    def test_japanese_content(self):
        raw = [
            "user: 平日な朝6時にUPWARDの競合の広告を確認したい",
            r"assistant: ご質問ありがとうございます。\n\nキーワードを教えてください。",
        ]
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert "UPWARD" in result[0]["content"]
        assert "キーワード" in result[1]["content"]
        # Verify newline was unescaped
        # 验证换行已还原
        assert "\n" in result[1]["content"]

    def test_colon_in_content_preserved(self):
        """Content containing ': ' should not break parsing.

        包含 ': ' 的 content 不应破坏解析。
        """
        raw = ["user: time is 10:30", "assistant: URL: https://example.com"]
        result = _parse_history_entries(raw)
        assert len(result) == 2
        assert result[0]["content"] == "time is 10:30"
        assert result[1]["content"] == "URL: https://example.com"

    def test_full_multi_turn_conversation_round_trip(self):
        """Simulate a real Sagasu multi-turn conversation.

        模拟一次真实的 Sagasu 多轮对话。
        """
        raw = [
            "user: 平日な朝6時にUPWARDの競合になりうるサイトの広告のクリエティブを確認したい",
            r'assistant: {"type":"clarification","message":"キーワードを教えてください"}',
            "user: 営業支援やCRM、SFA",
            r'assistant: {"type":"clarification","message":"朝6時と朝9時のどちらですか？"}',
            "user: 朝6時",
        ]
        result = _parse_history_entries(raw)
        assert len(result) == 5
        # Verify all messages are present with correct roles
        # 验证所有消息都存在且 role 正确
        assert result[0]["role"] == "user"
        assert result[1]["role"] == "assistant"
        assert result[2]["role"] == "user"
        assert result[3]["role"] == "assistant"
        assert result[4]["role"] == "user"
        # Verify content is complete
        # 验证 content 完整
        assert "UPWARD" in result[0]["content"]
        assert "clarification" in result[1]["content"]
        assert "朝6時" in result[4]["content"]
