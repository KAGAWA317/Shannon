"""Tests for tool-type-aware history truncation in build_agent_messages.

Root cause: HISTORY_RESULT_MAX = 2000 truncates ALL tool results uniformly.
file_read results (agent's working memory) get destroyed between iterations,
forcing agents to waste 9 of 15 iterations re-reading the same file.

Fix: file_read/file_edit results should preserve much more content (15K),
while web_search/web_fetch keep moderate truncation (3000).

按 tool 类型区分历史截断的测试：旧实现用 HISTORY_RESULT_MAX = 2000 一刀切，
把 file_read 结果（agent 的工作记忆）在迭代之间截断，迫使 agent 反复重读同一文件。
修复后 file_read/file_edit 保留约 15K 字符，web_search/web_fetch 仍截断到约 3000。
"""


from llm_service.api.agent import (
    AgentLoopStepRequest,
    AgentLoopTurn,
    build_agent_messages,
)


# 默认构造一个带历史与 tool 建议的 swarm agent request，用例只覆盖关心的字段
def _make_request(**kwargs) -> AgentLoopStepRequest:
    defaults = dict(
        agent_id="Akiba",
        task="Analyze React vs Vue",
        iteration=3,
        is_swarm=True,
        role="researcher",
        suggested_tools=["web_search", "file_read", "file_write"],
    )
    defaults.update(kwargs)
    return AgentLoopStepRequest(**defaults)


class TestHistoryTruncation:
    """History truncation must be tool-type-aware.

    历史截断必须按 tool 类型区分，不能对所有 tool result 套同一个上限。
    """

    def test_file_read_result_not_truncated_at_2000(self):
        """file_read results should NOT be truncated to 2000 chars.

        A 5000-char file_read result is the agent's working memory — truncating
        it forces the agent to re-read the file on the next iteration.

        file_read 结果不应被截到 2000 字符：5000 字符的文件内容是 agent 的工作记忆，
        截断会迫使它在下一轮迭代重新读取同一文件。
        """
        file_content = "# Report\n" + "x" * 5000  # 5010 chars total 注：共 5010 字符
        history = [
            AgentLoopTurn(
                iteration=1,
                action="tool_call:file_read",
                result=file_content,
            ),
        ]
        msgs = build_agent_messages(_make_request(history=history))
        user_msg = msgs[1]["content"]
        # The full file_read result (5010 chars) must be preserved, not cut to 2000
        # 完整的 file_read 结果 (5010 字符) 必须保留，不能被截到 2000
        assert "x" * 4000 in user_msg, \
            "file_read result was truncated — agent loses working memory"

    def test_web_search_result_still_truncated(self):
        """web_search results should still be truncated to moderate size.

        web_search 结果仍应截断到中等长度，避免历史撑爆 prompt。
        """
        search_content = "Search result: " + "y" * 10000  # 10015 chars 注：10015 字符
        history = [
            AgentLoopTurn(
                iteration=1,
                action="tool_call:web_search",
                result=search_content,
            ),
        ]
        msgs = build_agent_messages(_make_request(history=history))
        user_msg = msgs[1]["content"]
        # web_search result should NOT have all 10000 chars
        # web_search 结果不应保留全部 10000 字符
        assert "y" * 8000 not in user_msg, \
            "web_search result should be truncated, not kept in full"

    def test_file_edit_result_preserved(self):
        """file_edit results should also be preserved like file_read.

        file_edit 结果与 file_read 同等对待，同样需要保留。
        """
        edit_result = "Edited file: " + "z" * 5000
        history = [
            AgentLoopTurn(
                iteration=1,
                action="tool_call:file_edit",
                result=edit_result,
            ),
        ]
        msgs = build_agent_messages(_make_request(history=history))
        user_msg = msgs[1]["content"]
        assert "z" * 4000 in user_msg, \
            "file_edit result was truncated — agent loses context of its own edits"

    def test_mixed_history_selective_truncation(self):
        """In mixed history, file ops keep full content while web ops truncate.

        混合历史下，file 类操作保留完整内容，web 类操作照常截断。
        """
        history = [
            AgentLoopTurn(
                iteration=1,
                action="tool_call:web_search",
                result="W" * 6000,
            ),
            AgentLoopTurn(
                iteration=2,
                action="tool_call:file_read",
                result="F" * 6000,
            ),
        ]
        msgs = build_agent_messages(_make_request(history=history, iteration=3))
        user_msg = msgs[1]["content"]
        # file_read: 6000 chars should be preserved
        # file_read：6000 字符应被保留
        assert "F" * 5000 in user_msg, "file_read result truncated in mixed history"
        # web_search: 6000 chars should be truncated
        # web_search：6000 字符应被截断
        assert "W" * 5000 not in user_msg, "web_search result not truncated in mixed history"


class TestPromptCharBudget:
    """Module-level invariants and trim correctness on list-form content.

    模块级常量不变量，以及列表形式 content 上的裁剪正确性。
    """

    def test_max_text_total_under_max_prompt_chars(self):
        """Alignment invariant: the text-attachment budget must leave room
        for the user's actual query. If MAX_TEXT_TOTAL ever creeps past
        MAX_PROMPT_CHARS, the (MAX_PROMPT_CHARS - attachment_text_chars)
        computation in query/history trim paths goes negative and clamps to
        the 1_000-char floor — silently truncating every prompt to 1 KB.

        对齐不变量：文本附件 budget 必须给用户的实际 query 留出余量。一旦
        MAX_TEXT_TOTAL 超过 MAX_PROMPT_CHARS，query/history 裁剪路径里的
        (MAX_PROMPT_CHARS - attachment_text_chars) 会变成负数并钳制到
        1_000 字符下限 —— 等于把每个 prompt 都静默截到 1 KB。
        """
        from llm_service.api.agent import (
            MAX_PROMPT_CHARS,
            MAX_TEXT_PER_FILE,
            MAX_TEXT_TOTAL,
        )

        assert MAX_TEXT_TOTAL < MAX_PROMPT_CHARS, (
            f"MAX_TEXT_TOTAL ({MAX_TEXT_TOTAL}) must stay under "
            f"MAX_PROMPT_CHARS ({MAX_PROMPT_CHARS})"
        )
        # Per-file cap should never exceed the total budget — otherwise a
        # single file can monopolize the entire text-attachment quota.
        # 单文件上限不能超过总 budget，否则一个文件就能吃光全部文本附件配额。
        assert MAX_TEXT_PER_FILE <= MAX_TEXT_TOTAL, (
            f"MAX_TEXT_PER_FILE ({MAX_TEXT_PER_FILE}) cannot exceed "
            f"MAX_TEXT_TOTAL ({MAX_TEXT_TOTAL})"
        )

    def test_multi_turn_trim_sees_list_form_observations(self):
        """`_build_multi_turn_messages` puts historical observations into
        list-form content blocks (`{"role":"user", "content":[{type:"text", text:...}]}`).
        Before the fix, the trim loop's `total_chars` sum only counted string
        content, so 10 turns × 150K chars of list-form observations stayed
        invisible — the prompt could balloon past MAX_PROMPT_CHARS with
        zero trim. After the fix, list-form text blocks are counted and
        oldest turn pairs evict until total falls under cap.

        `_build_multi_turn_messages` 把历史 observation 放进列表形式的 content block。
        修复前裁剪循环的 `total_chars` 只统计字符串 content，10 轮 × 150K 字符的
        列表形式 observation 完全不可见 —— prompt 可以在完全不裁剪的情况下
        涨过 MAX_PROMPT_CHARS。修复后列表形式的 text block 也计入统计，
        从最旧的 turn 对开始淘汰直到总量低于上限。
        """
        from llm_service.api.agent import (
            MAX_PROMPT_CHARS,
            _build_multi_turn_messages,
        )

        big_obs = "x" * 150_000  # ~150K chars per frozen observation 注：每条冻结 observation 约 150K 字符
        history = [
            AgentLoopTurn(
                iteration=i,
                action=f"tool_call:web_search_{i}",
                result="",
                assistant_replay='{"action":"step"}',
                observation_text=big_obs,
            )
            for i in range(10)
        ]
        body = _make_request(history=history, iteration=11)
        msgs = _build_multi_turn_messages(body, system_prompt="sys", cache_source="agent_loop")

        # Count chars the same way the new measurement helper does so the
        # assertion exercises both the trim AND its accounting.
        # 用与被测 measurement helper 相同的口径统计字符，让断言同时覆盖裁剪与计量。
        def measure(content):
            if isinstance(content, str):
                return len(content)
            if isinstance(content, list):
                return sum(
                    len(b.get("text", ""))
                    for b in content
                    if isinstance(b, dict) and b.get("type") == "text"
                )
            return 0

        total = sum(measure(m["content"]) for m in msgs)
        # With trim working, 10 × 150K = 1.5M raw drops near MAX_PROMPT_CHARS
        # after eviction. The loop also keeps a floor (len(messages) > 5), so
        # not every turn is guaranteed to evict — but total must come in
        # well under the pre-fix 1.5M figure.
        # 裁剪生效时，10 × 150K = 1.5M 的原始量在淘汰后应降到 MAX_PROMPT_CHARS 附近。
        # 循环保留 len(messages) > 5 的下限，因此不保证每轮都被淘汰 ——
        # 但总量必须明显低于修复前的 1.5M。
        assert total <= MAX_PROMPT_CHARS + 200_000, (
            f"After trim, total_chars={total:,} should be near "
            f"MAX_PROMPT_CHARS={MAX_PROMPT_CHARS:,} (pre-fix would stay near 1.5M)"
        )

    def test_multi_turn_delivers_raw_attachments(self):
        """`build_agent_messages` previously returned from the multi-turn
        branch via `return _build_multi_turn_messages(...)` without passing
        raw_attachments — so on every Sonnet+ agent loop step, user
        attachments (text PDFs, images) were silently dropped. They only made
        it through the legacy single-message path. The fix threads
        raw_attachments through and parks them in the immutable bootstrap
        message (list-of-blocks form) so they cache cleanly across iterations.

        `build_agent_messages` 过去在多轮分支用 `return _build_multi_turn_messages(...)`
        返回时没有传 raw_attachments —— 于是 Sonnet+ 的每个 agent loop step 上，
        用户的附件（文本 PDF、图片）都被静默丢弃，只有 legacy 单条消息路径能带上。
        修复后 raw_attachments 被透传，并放进不可变的 bootstrap message
        （列表 block 形式），从而跨迭代稳定命中 cache。
        """
        from llm_service.api.agent import build_agent_messages

        raw_attachments = [
            {
                "source": "text",
                "filename": "report.pdf",
                "text_content": "Q3 revenue: $12M\n" + ("body text " * 50),
            },
            {
                "source": "base64",
                "filename": "chart.png",
                "media_type": "image/png",
                "data": "iVBORw0KGgo=",
            },
        ]
        history = [
            AgentLoopTurn(
                iteration=1,
                action="tool_call:web_search",
                result="result text",
                assistant_replay='{"action":"step"}',
                observation_text="search result digest",
            ),
        ]
        body = _make_request(history=history, iteration=2, model_tier="medium")
        msgs = build_agent_messages(body, raw_attachments=raw_attachments)

        # Bootstrap user message (index 1) should be list-form with our
        # attachment blocks. Pre-fix: bootstrap was a plain string with no
        # attachments anywhere in msgs.
        # bootstrap user message（下标 1）应是带附件 block 的列表形式。
        # 修复前它是纯字符串，msgs 里任何位置都找不到附件。
        bootstrap = msgs[1]["content"]
        assert isinstance(bootstrap, list), (
            f"bootstrap should be list-of-blocks when attachments present, got {type(bootstrap).__name__}"
        )

        # Look for the PDF text and the shannon_attachment marker
        # (provider-neutral binary handle).
        # 查找 PDF 文本与 shannon_attachment 标记（provider 无关的二进制句柄）。
        all_text = "".join(
            b.get("text", "") for b in bootstrap if isinstance(b, dict) and b.get("type") == "text"
        )
        has_pdf_text = "Q3 revenue" in all_text
        has_image_block = any(
            isinstance(b, dict) and b.get("type") == "shannon_attachment" and b.get("filename") == "chart.png"
            for b in bootstrap
        )
        assert has_pdf_text, "text attachment content missing from bootstrap"
        assert has_image_block, "binary attachment block missing from bootstrap"

        # And the original bootstrap text (Task / OriginalQuery) should still
        # be there alongside the attachments.
        # 原有 bootstrap 文本（Task / OriginalQuery）应仍在附件之后保留。
        assert "## Task" in all_text, "bootstrap text payload should follow attachment blocks"
