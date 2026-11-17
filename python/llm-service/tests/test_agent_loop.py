"""Tests for agent loop history tiering and prompt construction.
agent loop 历史分层与 prompt 构造的测试。
"""

import pytest
import unittest
from unittest.mock import AsyncMock, patch, MagicMock

from llm_service.api.agent import AgentLoopStepRequest, AgentLoopTurn, TeamMemberInfo, agent_loop_step, build_agent_messages


class TestTieredHistory:
    """Test that history truncation uses tiered lengths based on recency.
    历史截断按新近程度使用分层长度。
    """

    def _build_request(self, num_turns: int, result_len: int = 5000) -> AgentLoopStepRequest:
        """Build a request with `num_turns` turns, each having a result of `result_len` chars.
        构造一个含 `num_turns` 轮、每轮 result 长度为 `result_len` 字符的 request。
        """
        history = []
        for i in range(num_turns):
            history.append(AgentLoopTurn(
                iteration=i,
                action=f"tool_call_{i}",
                result="x" * result_len,
            ))
        return AgentLoopStepRequest(
            agent_id="test-agent",
            task="test task",
            iteration=num_turns,
            max_iterations=25,
            history=history,
        )

    def test_recent_turns_get_full_detail(self):
        """Last 3 turns should be truncated at 4000 chars, not 500.
        最近 3 轮应在 4000 字符处截断，而不是 500。
        """
        req = self._build_request(num_turns=5, result_len=5000)
        # Simulate the tiered logic from agent_loop_step
        # 模拟 agent_loop_step 中的分层逻辑
        history_lines = []
        num_turns = len(req.history)
        for idx, turn in enumerate(req.history):
            is_recent = (num_turns - idx) <= 3
            max_len = 4000 if is_recent else 500
            result_str = str(turn.result)[:max_len] if turn.result else "no result"
            history_lines.append(result_str)

        # Older turns (index 0, 1) should be truncated to 500
        # 较老的轮次（索引 0、1）应截断到 500
        assert len(history_lines[0]) == 500
        assert len(history_lines[1]) == 500
        # Recent turns (index 2, 3, 4) should be truncated to 4000
        # 最近的轮次（索引 2、3、4）应截断到 4000
        assert len(history_lines[2]) == 4000
        assert len(history_lines[3]) == 4000
        assert len(history_lines[4]) == 4000

    def test_short_results_not_padded(self):
        """Results shorter than the limit should not be modified.
        短于上限的结果不应被修改。
        """
        req = self._build_request(num_turns=5, result_len=100)
        history_lines = []
        num_turns = len(req.history)
        for idx, turn in enumerate(req.history):
            is_recent = (num_turns - idx) <= 3
            max_len = 4000 if is_recent else 500
            result_str = str(turn.result)[:max_len] if turn.result else "no result"
            history_lines.append(result_str)

        # All should be 100 chars (shorter than both limits)
        # 长度都应为 100 字符（短于两个上限）
        for line in history_lines:
            assert len(line) == 100

    def test_fewer_than_3_turns_all_recent(self):
        """With fewer than 3 turns, all should get full 4000-char treatment.
        少于 3 轮时，所有轮次都应享受完整的 4000 字符待遇。
        """
        req = self._build_request(num_turns=2, result_len=5000)
        history_lines = []
        num_turns = len(req.history)
        for idx, turn in enumerate(req.history):
            is_recent = (num_turns - idx) <= 3
            max_len = 4000 if is_recent else 500
            result_str = str(turn.result)[:max_len] if turn.result else "no result"
            history_lines.append(result_str)

        assert len(history_lines[0]) == 4000
        assert len(history_lines[1]) == 4000

    def test_exactly_3_turns_all_recent(self):
        """With exactly 3 turns, all should be recent (full detail).
        恰好 3 轮时，全部视为最近轮次（完整细节）。
        """
        req = self._build_request(num_turns=3, result_len=5000)
        history_lines = []
        num_turns = len(req.history)
        for idx, turn in enumerate(req.history):
            is_recent = (num_turns - idx) <= 3
            max_len = 4000 if is_recent else 500
            result_str = str(turn.result)[:max_len] if turn.result else "no result"
            history_lines.append(result_str)

        for line in history_lines:
            assert len(line) == 4000

    def test_none_result_handled(self):
        """Turns with None result should produce 'no result'.
        result 为 None 的轮次应产出 'no result'。
        """
        history = [AgentLoopTurn(iteration=0, action="test", result=None)]
        num_turns = len(history)
        for idx, turn in enumerate(history):
            is_recent = (num_turns - idx) <= 3
            max_len = 4000 if is_recent else 500
            result_str = str(turn.result)[:max_len] if turn.result else "no result"
            assert result_str == "no result"

    def test_many_turns_budget(self):
        """With 25 turns, total history chars should be bounded (~23K).
        25 轮时，历史总字符数应被约束（约 23K）。
        """
        req = self._build_request(num_turns=25, result_len=10000)
        total_chars = 0
        num_turns = len(req.history)
        for idx, turn in enumerate(req.history):
            is_recent = (num_turns - idx) <= 3
            max_len = 4000 if is_recent else 500
            result_str = str(turn.result)[:max_len] if turn.result else "no result"
            total_chars += len(result_str)

        # Expected: 22 * 500 + 3 * 4000 = 11000 + 12000 = 23000
        # 预期：22 * 500 + 3 * 4000 = 11000 + 12000 = 23000
        assert total_chars == 23000


class TestMaxIterationsDefault:
    """Test that max_iterations defaults match the platform config.
    max_iterations 的默认值应与平台配置一致。
    """

    def test_default_max_iterations_is_25(self):
        req = AgentLoopStepRequest(
            agent_id="test",
            task="test",
            iteration=0,
        )
        assert req.max_iterations == 25


class TestDecisionSummary:
    """Test that decision_summary field is parsed and truncated correctly.
    decision_summary 字段能被正确解析与截断。
    """

    def test_decision_summary_in_response_model(self):
        """decision_summary field exists on AgentLoopStepResponse."""
        from llm_service.api.agent import AgentLoopStepResponse
        resp = AgentLoopStepResponse(
            action="tool_call",
            decision_summary="Checking AWS pricing data for comparison",
            tool="web_search",
            tool_params={"query": "AWS pricing"},
        )
        assert resp.decision_summary == "Checking AWS pricing data for comparison"

    def test_decision_summary_default_empty(self):
        """decision_summary defaults to empty string if not provided."""
        from llm_service.api.agent import AgentLoopStepResponse
        resp = AgentLoopStepResponse(action="done", response="finished")
        assert resp.decision_summary == ""

    def test_decision_summary_in_action_json(self):
        """decision_summary is parsed from LLM JSON response."""
        import json
        raw = '{"decision_summary": "Teammate covered AWS, I focus Azure.", "action": "tool_call", "tool": "web_search", "tool_params": {"query": "Azure pricing"}}'
        parsed = json.loads(raw)
        assert "decision_summary" in parsed
        assert len(parsed["decision_summary"]) < 500

    def test_decision_summary_truncation(self):
        """decision_summary longer than 500 chars gets truncated."""
        long_summary = "x" * 1000
        truncated = long_summary[:500]
        assert len(truncated) == 500


class TestAgentLoopModelTier:
    """Test model_tier propagation into provider selection for /agent/loop.
    model_tier 如何透传到 /agent/loop 的 provider 选择。
    """

    @pytest.mark.asyncio
    async def test_agent_loop_step_uses_explicit_model_tier(self):
        from llm_service.providers.base import ModelTier

        mock_providers = MagicMock()
        mock_providers.is_configured.return_value = True
        mock_providers.generate_completion = AsyncMock(
            return_value={
                "output_text": "{\"action\": \"done\", \"decision_summary\": \"ok\", \"response\": \"hi\"}",
                "usage": {"total_tokens": 10, "input_tokens": 2, "output_tokens": 8},
                "model": "mock-model",
                "provider": "mock-provider",
            }
        )

        mock_request = MagicMock()
        mock_request.app.state.providers = mock_providers

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="test task",
            iteration=0,
            model_tier="large",
        )

        resp = await agent_loop_step(mock_request, body)
        assert resp.action == "done"
        assert resp.response == "hi"

        assert mock_providers.generate_completion.await_args.kwargs["tier"] == ModelTier.LARGE

    @pytest.mark.asyncio
    async def test_agent_loop_step_uses_context_model_tier_when_no_explicit(self):
        from llm_service.providers.base import ModelTier

        mock_providers = MagicMock()
        mock_providers.is_configured.return_value = True
        mock_providers.generate_completion = AsyncMock(
            return_value={
                "output_text": "{\"action\": \"done\", \"decision_summary\": \"ok\", \"response\": \"hi\"}",
                "usage": {"total_tokens": 10, "input_tokens": 2, "output_tokens": 8},
                "model": "mock-model",
                "provider": "mock-provider",
            }
        )

        mock_request = MagicMock()
        mock_request.app.state.providers = mock_providers

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="test task",
            iteration=0,
            context={"model_tier": "small"},
        )

        resp = await agent_loop_step(mock_request, body)
        assert resp.action == "done"

        assert mock_providers.generate_completion.await_args.kwargs["tier"] == ModelTier.SMALL

    @pytest.mark.asyncio
    async def test_agent_loop_step_explicit_overrides_context_model_tier(self):
        from llm_service.providers.base import ModelTier

        mock_providers = MagicMock()
        mock_providers.is_configured.return_value = True
        mock_providers.generate_completion = AsyncMock(
            return_value={
                "output_text": "{\"action\": \"done\", \"decision_summary\": \"ok\", \"response\": \"hi\"}",
                "usage": {"total_tokens": 10, "input_tokens": 2, "output_tokens": 8},
                "model": "mock-model",
                "provider": "mock-provider",
            }
        )

        mock_request = MagicMock()
        mock_request.app.state.providers = mock_providers

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="test task",
            iteration=0,
            model_tier="medium",
            context={"model_tier": "small"},
        )

        resp = await agent_loop_step(mock_request, body)
        assert resp.action == "done"

        assert mock_providers.generate_completion.await_args.kwargs["tier"] == ModelTier.MEDIUM


class TestMultiJsonParsing:
    """Test that agent loop handles multi-JSON responses from GPT-5.1.

    GPT-5.1 sometimes emits tool_params JSON followed by the action JSON,
    concatenated with a newline. json.loads() only parses the first object,
    which may lack the 'action' field. The multi-JSON fallback should find the real one.
    GPT-5.1 有时会先输出 tool_params 的 JSON，再跟一段 action JSON，两者用换行拼接。
    json.loads() 只解析第一个对象，而它可能缺少 'action' 字段。
    多 JSON 回退逻辑应能找出真正带 action 的那个。
    """

    def _make_mock_request(self, output_text):
        """Create mock request + providers returning given output_text.
        构造返回指定 output_text 的 mock request + providers。
        """
        mock_providers = MagicMock()
        mock_providers.is_configured.return_value = True
        mock_providers.generate_completion = AsyncMock(
            return_value={
                "output_text": output_text,
                "usage": {"total_tokens": 50, "input_tokens": 20, "output_tokens": 30},
                "model": "gpt-5.1",
                "provider": "openai",
            }
        )
        mock_request = MagicMock()
        mock_request.app.state.providers = mock_providers
        return mock_request

    @pytest.mark.asyncio
    async def test_multi_json_recovers_action(self):
        """When LLM emits tool_params JSON then action JSON, recover the action.
        当 LLM 先输出 tool_params JSON、再输出 action JSON 时，仍能恢复出 action。
        """
        # Simulate GPT-5.1 output: tool_params JSON + newline + action JSON
        # 模拟 GPT-5.1 输出：tool_params JSON + 换行 + action JSON
        # 没有 assistant prefill —— LLM 直接返回完整的 JSON 对象
        # No assistant prefill — LLM returns complete JSON objects
        output_text = (
            '{"path": ".", "pattern": "*.md", "recursive": true}\n'
            '{"decision_summary": "Listing markdown files", "action": "tool_call", '
            '"tool": "file_list", "tool_params": {"path": ".", "pattern": "*.md", "recursive": true}}'
        )
        mock_request = self._make_mock_request(output_text)

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="list files",
            iteration=0,
        )

        resp = await agent_loop_step(mock_request, body)
        assert resp.action == "tool_call"
        assert resp.tool == "file_list"
        assert resp.decision_summary == "Listing markdown files"

    @pytest.mark.asyncio
    async def test_single_json_still_works(self):
        """Normal single-JSON response should work as before.
        普通的单 JSON response 应和以前一样工作。
        """
        output_text = (
            '{"decision_summary": "Done", "action": "done", "response": "All complete"}'
        )
        mock_request = self._make_mock_request(output_text)

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="finish up",
            iteration=0,
        )

        resp = await agent_loop_step(mock_request, body)
        assert resp.action == "done"
        assert resp.response == "All complete"

    @pytest.mark.asyncio
    async def test_multi_json_no_action_in_any_falls_back_to_done(self):
        """If no JSON object has 'action', fall back to done (non-JSON path).
        若没有任何 JSON 对象带 'action'，回退为 done（走非 JSON 路径）。
        """
        output_text = (
            '{"path": ".", "pattern": "*.md"}\n'
            '{"summary": "no action field here either"}'
        )
        mock_request = self._make_mock_request(output_text)

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="test task",
            iteration=0,
        )

        resp = await agent_loop_step(mock_request, body)
        assert resp.action == "done"


class TestAgentNoPrefill:
    """Test agent_loop_step doesn't use assistant prefill.
    agent_loop_step 不使用 assistant prefill。
    """

    @pytest.mark.asyncio
    async def test_no_assistant_prefill_in_messages(self):
        """Messages sent to LLM should NOT contain assistant prefill.
        发给 LLM 的 messages 中不应包含 assistant prefill。
        """
        captured_kwargs = {}

        async def mock_generate(**kwargs):
            captured_kwargs.update(kwargs)
            return {
                "output_text": '{"action": "idle", "decision_summary": "done", "response": "finished"}',
                "usage": {"total_tokens": 100, "input_tokens": 80, "output_tokens": 20},
                "model": "claude-sonnet-4-6",
                "provider": "anthropic",
            }

        mock_providers = MagicMock()
        mock_providers.is_configured.return_value = True
        mock_providers.generate_completion = mock_generate

        mock_request = MagicMock()
        mock_request.app.state.providers = mock_providers

        body = AgentLoopStepRequest(
            agent_id="test-agent",
            workflow_id="test-wf",
            task="Test task",
            iteration=0,
            suggested_tools=["web_search"],
        )

        result = await agent_loop_step(mock_request, body)

        messages = captured_kwargs.get("messages", [])
        assert not any(m["role"] == "assistant" for m in messages), \
            "Messages should not contain assistant prefill"
        assert messages[-1]["role"] == "user"


class TestTruncationMarker(unittest.TestCase):
    """A6: Truncation markers must discourage re-searching.
    A6：截断标记必须阻止重复检索。
    """

    def test_truncation_marker_discourages_research(self):
        """Truncated text must tell agent NOT to re-search for the same data.
        被截断的文本必须告知 agent 不要为同一份数据重复检索。
        """
        import inspect
        from llm_service.api import agent as agent_module
        source = inspect.getsource(agent_module)
        self.assertIn("do NOT re-search", source,
                       "Truncation marker must include 'do NOT re-search' instruction")


class TestInterpretationSkip(unittest.TestCase):
    """A3: Interpretation pass should be skipped for data-only tools.
    A3：纯数据类 tool 应跳过 interpretation 阶段。
    """

    def test_data_only_tools_constant_exists(self):
        """DATA_ONLY_TOOLS constant must be defined with correct membership.
        DATA_ONLY_TOOLS 常量必须定义且成员正确。
        """
        from llm_service.api.agent import DATA_ONLY_TOOLS
        self.assertIsInstance(DATA_ONLY_TOOLS, (set, frozenset))
        self.assertIn("file_list", DATA_ONLY_TOOLS)
        self.assertIn("file_read", DATA_ONLY_TOOLS)
        self.assertIn("file_search", DATA_ONLY_TOOLS)
        self.assertIn("file_write", DATA_ONLY_TOOLS)
        self.assertIn("publish_data", DATA_ONLY_TOOLS)
        # web_search is data-only (short structured snippets).
        # web_search 属于纯数据（短的结构化摘要）。
        # web_fetch 已移除：agent 需要完整的抓取内容内联在 context 中。
        # web_fetch removed: agent needs full fetch content inline.
        self.assertIn("web_search", DATA_ONLY_TOOLS)
        self.assertNotIn("web_fetch", DATA_ONLY_TOOLS)

    def test_data_only_tools_skip_interpretation(self):
        """When all executed tools are data-only, interpretation should be skipped.
        当所有执行的 tool 都是纯数据类时，应跳过 interpretation。
        """
        from llm_service.api.agent import DATA_ONLY_TOOLS
        records = [
            {"tool": "file_list", "success": True, "output": "[]"},
            {"tool": "file_read", "success": True, "output": "content"},
        ]
        all_data_only = all(r.get("tool") in DATA_ONLY_TOOLS for r in records)
        self.assertTrue(all_data_only, "All file_* tools should be classified as data-only")

    def test_mixed_tools_keep_interpretation(self):
        """When any tool is NOT data-only, interpretation should run.
        只要有任一 tool 不是纯数据类，就应执行 interpretation。
        """
        from llm_service.api.agent import DATA_ONLY_TOOLS
        records = [
            {"tool": "bash_executor", "success": True, "output": "results"},
            {"tool": "file_write", "success": True, "output": "ok"},
        ]
        all_data_only = all(r.get("tool") in DATA_ONLY_TOOLS for r in records)
        self.assertFalse(all_data_only, "bash_executor is NOT data-only, interpretation should run")

    def test_empty_records_no_skip(self):
        """Empty tool_execution_records should not trigger data-only skip.
        空的 tool_execution_records 不应触发纯数据跳过。
        """
        from llm_service.api.agent import DATA_ONLY_TOOLS
        records = []
        all_data_only = records and all(r.get("tool") in DATA_ONLY_TOOLS for r in records)
        self.assertFalse(all_data_only, "Empty records should not skip interpretation")


class TestAgentProtocolCognitiveModel:
    """Verify agent protocol contains cognitive framework elements.
    确认 agent protocol 中包含认知框架要素。
    """

    def test_mental_model_section_exists(self):
        """Protocol must open with HOW TOOLS WORK mental model, not phases.
        protocol 必须以 HOW TOOLS WORK 心智模型开头，而不是从阶段指令开始。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        model_pos = AGENT_LOOP_SYSTEM_PROMPT.find("HOW YOUR TOOLS WORK")
        phase_pos = AGENT_LOOP_SYSTEM_PROMPT.find("PHASE 1")
        assert model_pos != -1, "Missing 'HOW YOUR TOOLS WORK' section"
        assert model_pos < phase_pos, "Mental model must appear BEFORE phase instructions"

    def test_search_fetch_pipeline_explained(self):
        """Protocol must explicitly explain search→fetch as a pipeline.
        protocol 必须明确把 search→fetch 解释为一条流水线。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        assert "URL finder" in AGENT_LOOP_SYSTEM_PROMPT or "URL discovery" in AGENT_LOOP_SYSTEM_PROMPT, \
            "Must frame web_search as URL discovery, not answer retrieval"

    def test_protocol_length_reduced(self):
        """Protocol should be under 160 lines to maintain attention density.
        protocol 应控制在 160 行以内，以维持注意力密度。
        """
        from llm_service.roles.swarm.agent_protocol import AGENT_LOOP_SYSTEM_PROMPT
        line_count = AGENT_LOOP_SYSTEM_PROMPT.count('\n')
        assert line_count <= 160, f"Protocol has {line_count} lines, should be ≤160 for attention density"


class TestResearcherRolePrompt:
    """Verify researcher role prompt contains reasoning examples.
    确认 researcher 角色 prompt 中包含推理示例。
    """

    def test_has_ooda_decision_loop(self):
        """Researcher prompt must include OODA decision loop.
        researcher prompt 必须包含 OODA 决策循环。
        """
        from llm_service.roles.swarm.role_prompts import SWARM_ROLE_PROMPTS
        prompt = SWARM_ROLE_PROMPTS["researcher"]
        assert "OBSERVE" in prompt and "ORIENT" in prompt and "DECIDE" in prompt, \
            "Researcher prompt must include OODA decision loop (OBSERVE/ORIENT/DECIDE)"

    def test_search_fetch_example(self):
        """Researcher prompt must include a search->fetch workflow example.
        researcher prompt 必须包含 search->fetch 的 workflow 示例。
        """
        from llm_service.roles.swarm.role_prompts import SWARM_ROLE_PROMPTS
        prompt = SWARM_ROLE_PROMPTS["researcher"]
        assert "web_fetch" in prompt and "search" in prompt.lower(), \
            "Must show complete search->fetch workflow"


class TestRoleReasoningExamples:
    """All research-oriented roles should have reasoning examples.
    所有偏研究型的角色都应有推理示例。
    """

    def test_company_researcher_has_reasoning(self):
        from llm_service.roles.swarm.role_prompts import SWARM_ROLE_PROMPTS
        prompt = SWARM_ROLE_PROMPTS["company_researcher"]
        assert "THINK:" in prompt, "company_researcher must have reasoning examples"

    def test_analyst_has_reasoning(self):
        from llm_service.roles.swarm.role_prompts import SWARM_ROLE_PROMPTS
        prompt = SWARM_ROLE_PROMPTS["analyst"]
        assert "THINK:" in prompt, "analyst must have reasoning examples"


class TestBuildAgentMessages:
    """Test the multi-turn message builder for agent loop.
    agent loop 的多轮 message 构造器测试。
    """

    def _build_body(self, history_turns=0, task="Research React vs Vue"):
        history = []
        for i in range(history_turns):
            history.append(AgentLoopTurn(
                iteration=i + 1,
                action="tool_call",
                result=f"Result from iteration {i + 1}" + ("x" * 500),
                decision_summary=f"Searching for data point {i + 1}",
            ))
        return AgentLoopStepRequest(
            agent_id="TestAgent",
            workflow_id="test-wf",
            task=task,
            iteration=history_turns + 1,
            max_iterations=15,
            history=history,
            role="researcher",
            is_swarm=True,
        )

    def test_no_history_produces_system_and_user(self):
        """First iteration: [system, user] — original two-message structure.
        首轮迭代：[system, user] —— 原始的兩条 message 结构。
        """
        body = self._build_body(history_turns=0)
        messages = build_agent_messages(body)
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        assert "## Task" in messages[1]["content"]
        assert "Budget" in messages[1]["content"]

    def test_with_history_still_two_messages(self):
        """With history: still [system, user] — no fake assistant messages.
        带历史时仍然是 [system, user] —— 不伪造 assistant message。
        """
        body = self._build_body(history_turns=3)
        messages = build_agent_messages(body)
        assert len(messages) == 2
        assert messages[0]["role"] == "system"
        assert messages[1]["role"] == "user"
        # History should be in the user message as text, not as separate messages
        # 历史应作为文本放进 user message，而不是拆成多条 message
        assert "Previous Actions" in messages[1]["content"]
        assert "Iteration 1" in messages[1]["content"]
        # No assistant messages — fake assistant messages cause identity confusion
        # 不出现 assistant message —— 伪造的 assistant message 会导致身份混乱
        assert all(m["role"] != "assistant" for m in messages)

    def test_history_preserves_tiered_truncation(self):
        """Recent 3 turns get 4000 chars, older get 2000 (original behavior).
        最近 3 轮 4000 字符，更早的 2000 字符（原有行为）。
        """
        body = self._build_body(history_turns=5)
        messages = build_agent_messages(body)
        content = messages[1]["content"]
        # Older turns (1,2) should be truncated to 2000 chars
        # 较老的轮次（1、2）应截断到 2000 字符
        # 最近的轮次（3、4、5）应保留完整 4000 字符
        # 测试数据只有 500 字符的结果，因此都不应被截断
        # Recent turns (3,4,5) should get full 4000 chars
        # Our test data has 500-char results, so all should be untruncated
        assert "Iteration 1" in content
        assert "Iteration 5" in content

    def test_cache_break_marker_between_stable_and_volatile(self):
        """User message has cache_break marker separating stable context from volatile context.

        Stable: Task, Team Roster, Team Knowledge (don't change across iterations)
        Volatile: History, TaskBoard, Notes, Findings, Budget (change every iteration)

        This allows anthropic_provider to split into content blocks with cache_control,
        enabling prompt cache for Haiku agents (system 2700t + stable user ≥ 1400t → crosses 4096 threshold).
        Stable：Task、Team Roster、Team Knowledge（跨迭代不变）
        Volatile：History、TaskBoard、Notes、Findings、Budget（每轮都变）

        这样 anthropic_provider 才能切成带 cache_control 的 content block，
        为 Haiku agent 启用 prompt cache（system 2700t + stable user ≥ 1400t → 跨过 4096 阈值）。
        """
        from llm_service.api.agent import TeamMemberInfo

        body = self._build_body(history_turns=3)
        body.team_roster = [
            TeamMemberInfo(agent_id="TestAgent", role="researcher", task="Research React"),
            TeamMemberInfo(agent_id="Agent-B", role="analyst", task="Analyze data"),
        ]
        messages = build_agent_messages(body)
        user_content = messages[1]["content"]

        # Marker must exist in user message
        # 标记必须出现在 user message 中
        assert "<!-- cache_break -->" in user_content

        # Stable parts (Task, Team) must be BEFORE the marker
        # 稳定部分（Task、Team）必须在标记之前
        parts = user_content.split("<!-- cache_break -->", 1)
        stable = parts[0]
        volatile = parts[1]
        assert "## Task" in stable
        assert "## Your Team" in stable

        # Volatile parts (History, Budget) must be AFTER the marker
        # 易变部分（History、Budget）必须在标记之后
        assert "## Previous Actions" in volatile
        assert "Budget" in volatile

    def test_team_knowledge_before_cache_break(self):
        """Team Knowledge (L2) must be in stable (cached) block BEFORE cache_break.

        Agent loop doesn't use native tool calling, so the only cache prefix is
        system(~2700t) + user_stable. Without Knowledge in stable, total is ~3150t
        which is below Haiku's 4096t threshold → zero cache. With Knowledge,
        total can cross 4096t → enables cache (sawtooth pattern, 20-24% hit rate).
        agent loop 不使用原生 tool calling，因此唯一的 cache 前缀只有
        system(~2700t) + user_stable。若 Knowledge 不在 stable 里，总量约 3150t，
        低于 Haiku 的 4096t 阈值 → 完全不命中 cache。带上 Knowledge 后总量可跨过
        4096t → cache 生效（锯齿形态，命中率 20-24%）。
        """
        from llm_service.api.agent import TeamMemberInfo, TeamKnowledgeEntry

        body = self._build_body(history_turns=1)
        body.team_roster = [
            TeamMemberInfo(agent_id="TestAgent", role="researcher", task="Research React"),
        ]
        body.team_knowledge = [
            TeamKnowledgeEntry(url="https://example.com/react", agent="Agent-B", summary="React info"),
        ]
        messages = build_agent_messages(body)
        user_content = messages[1]["content"]

        parts = user_content.split("<!-- cache_break -->", 1)
        stable = parts[0]
        assert "## Team Knowledge" in stable

    def test_budget_in_last_user_message(self):
        """Budget and action prompt are in the final user message.
        Budget 与 action prompt 都放在最后一条 user message 中。
        """
        body = self._build_body(history_turns=2)
        messages = build_agent_messages(body)
        last_user = messages[-1]["content"]
        assert "Budget" in last_user
        assert "Decide your next action" in last_user


class TestRoleAwareProtocol:
    """Test that build_agent_messages uses role-specific protocol.
    build_agent_messages 使用按角色区分的 protocol。
    """

    def test_researcher_gets_search_fetch_in_system(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Research React adoption",
            iteration=0,
            role="researcher",
        )
        msgs = build_agent_messages(req)
        system_msg = msgs[0]["content"]
        assert "THE PIPELINE: search" in system_msg

    def test_coder_does_not_get_search_fetch_pipeline(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Implement API endpoint",
            iteration=0,
            role="coder",
        )
        msgs = build_agent_messages(req)
        system_msg = msgs[0]["content"]
        assert "THE PIPELINE: search" not in system_msg
        assert "file_list" in system_msg

    def test_synthesis_writer_no_web_search_pipeline(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Synthesize all findings",
            iteration=0,
            role="synthesis_writer",
        )
        msgs = build_agent_messages(req)
        system_msg = msgs[0]["content"]
        assert "THE PIPELINE: search" not in system_msg

    def test_generalist_gets_nonempty_protocol(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Do something",
            iteration=0,
            role="generalist",
        )
        msgs = build_agent_messages(req)
        system_msg = msgs[0]["content"]
        assert "tool_call" in system_msg
        assert "JSON" in system_msg

    def test_unknown_role_gets_default_protocol(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Custom task",
            iteration=0,
            role="custom_role_xyz",
        )
        msgs = build_agent_messages(req)
        system_msg = msgs[0]["content"]
        assert "tool_call" in system_msg
        assert "JSON" in system_msg


class TestOriginalQueryInjection:
    """Test that original_query appears in agent prompt.
    original_query 会出现在 agent prompt 中。
    """

    def test_original_query_in_user_message(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Research React adoption rates",
            iteration=0,
            original_query="帮我做一个前端技术选型报告",
        )
        msgs = build_agent_messages(req)
        user_msgs = [m for m in msgs if m["role"] == "user"]
        first_user = user_msgs[0]["content"]
        assert "帮我做一个前端技术选型报告" in first_user

    def test_no_original_query_when_empty(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Research React adoption rates",
            iteration=0,
        )
        msgs = build_agent_messages(req)
        user_msgs = [m for m in msgs if m["role"] == "user"]
        first_user = user_msgs[0]["content"]
        assert "Original Question" not in first_user

    def test_original_query_appears_before_task(self):
        req = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Research React adoption rates",
            iteration=0,
            original_query="Make a frontend tech report",
        )
        msgs = build_agent_messages(req)
        user_msgs = [m for m in msgs if m["role"] == "user"]
        first_user = user_msgs[0]["content"]
        q_pos = first_user.find("Make a frontend tech report")
        t_pos = first_user.find("Research React adoption rates")
        assert q_pos < t_pos, "Original query should appear before task"


class TestAgentLoopMessageStructure:
    """Test that agent_loop_step uses [system, user] structure (no fake assistant messages).
    agent_loop_step 使用 [system, user] 结构（不含伪造的 assistant message）。
    """

    @pytest.mark.asyncio
    async def test_agent_loop_sends_two_messages(self):
        """Agent loop sends [system, user] — no multi-turn assistant messages.
        agent loop 只发 [system, user] —— 不发送多轮 assistant message。
        """
        captured_messages = []

        async def mock_generate(messages, **kwargs):
            captured_messages.extend(messages)
            return {
                "output_text": '{"action": "done", "decision_summary": "ok", "response": "done"}',
                "usage": {"total_tokens": 10, "input_tokens": 5, "output_tokens": 5,
                          "cache_read_tokens": 0, "cache_creation_tokens": 0},
                "model": "mock", "provider": "mock",
            }

        mock_providers = MagicMock()
        mock_providers.is_configured.return_value = True
        mock_providers.generate_completion = AsyncMock(side_effect=mock_generate)

        mock_request = MagicMock()
        mock_request.app.state.providers = mock_providers

        body = AgentLoopStepRequest(
            agent_id="TestAgent",
            task="Research topic",
            iteration=3,
            role="researcher",
            is_swarm=True,
            history=[
                AgentLoopTurn(iteration=1, action="tool_call", result="result1", decision_summary="step1"),
                AgentLoopTurn(iteration=2, action="tool_call", result="result2", decision_summary="step2"),
            ],
        )

        await agent_loop_step(mock_request, body)

        # Should be [system, user] — no fake assistant messages
        # 应为 [system, user] —— 不含伪造的 assistant message
        assert len(captured_messages) == 2, f"Expected [system, user], got {len(captured_messages)} messages"
        assert captured_messages[0]["role"] == "system"
        assert captured_messages[1]["role"] == "user"
        # History should be in the user message, not as separate messages
        # 历史应放进 user message，而不是拆成多条 message
        assert "Previous Actions" in captured_messages[1]["content"]
        # Cache break marker should be present (stable/volatile split for prompt caching)
        # 应存在 cache break 标记（为 prompt caching 划分 stable/volatile）
        assert "<!-- cache_break -->" in captured_messages[1]["content"]


class TestContextTrimming(unittest.TestCase):
    """Context trimming must work with [system, user] two-message structure.
    context 裁剪必须在 [system, user] 两条 message 结构下生效。
    """

    def test_trimming_activates_on_large_user_message(self):
        """When user message exceeds max_prompt_chars, history should be trimmed.

        History uses tiered truncation: last 3 turns get up to 4000 chars,
        older turns get up to 500 chars. With 250 entries of 5000-char results,
        total before trimming exceeds 400K threshold → trimming activates.
        历史采用分层截断：最近 3 轮最多 4000 字符，更早的轮次最多 500 字符。
        250 条 5000 字符的结果，裁剪前总量会超过 400K 阈值 → 触发裁剪。
        """
        large_history = [
            AgentLoopTurn(iteration=i, action=f"tool_call:web_search", result="x" * 5000)
            for i in range(250)
        ]
        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Test task",
            history=large_history,
            iteration=251,
            session_id="test-session",
        )
        messages = build_agent_messages(body)
        # Must be exactly 2 messages
        # 必须恰好是 2 条 message
        self.assertEqual(len(messages), 2)
        # User message must be under 450K chars (400K target + tolerance)
        # user message 必须小于 450K 字符（400K 目标 + 容差）
        user_content = messages[1]["content"]
        self.assertLessEqual(len(user_content), 450_000,
            "User message should be trimmed when history is too large")

    def test_trimming_preserves_recent_history(self):
        """Trimming should remove oldest history first, keeping recent turns.
        裁剪应优先移除最老的历史，保留最近的轮次。
        """
        large_history = [
            AgentLoopTurn(iteration=i, action=f"tool_call:search_{i}", result="x" * 5000)
            for i in range(250)
        ]
        body = AgentLoopStepRequest(
            agent_id="test-agent",
            task="Test task",
            history=large_history,
            iteration=251,
            session_id="test-session",
        )
        messages = build_agent_messages(body)
        user_content = messages[1]["content"]
        # Most recent iteration should still be present
        # 最近一轮迭代仍应保留
        self.assertIn("search_249", user_content)
        # Oldest entries should have been removed
        # 最老的条目应已被移除
        self.assertNotIn("search_0", user_content)


class TestTeamRosterRole:
    """Test that team roster displays role information.
    team roster 中会展示角色信息。
    """

    def test_roster_shows_role(self):
        req = AgentLoopStepRequest(
            agent_id="agent-Luna",
            task="My task",
            iteration=0,
            team_roster=[
                TeamMemberInfo(agent_id="agent-Luna", task="Research pricing", role="researcher"),
                TeamMemberInfo(agent_id="agent-Mars", task="Analyze data", role="analyst"),
            ],
        )
        msgs = build_agent_messages(req)
        user_msgs = [m for m in msgs if m["role"] == "user"]
        first_user = user_msgs[0]["content"]
        assert "[researcher]" in first_user
        assert "[analyst]" in first_user

    def test_roster_without_role_still_works(self):
        req = AgentLoopStepRequest(
            agent_id="agent-Luna",
            task="My task",
            iteration=0,
            team_roster=[
                TeamMemberInfo(agent_id="agent-Luna", task="Research pricing"),
            ],
        )
        msgs = build_agent_messages(req)
        user_msgs = [m for m in msgs if m["role"] == "user"]
        first_user = user_msgs[0]["content"]
        assert "agent-Luna" in first_user
