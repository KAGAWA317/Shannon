"""TTL routing tests for _ttl_block.

Precedence: SHANNON_FORCE_TTL env > cache_source map > short default.
cache_ttl field is intentionally NOT consulted for prompt-cache routing
(that field drives manager.py's response-cache layer, a different concern).

_ttl_block 的 TTL 路由测试。

优先级：SHANNON_FORCE_TTL 环境变量 > cache_source 映射 > 短 TTL 默认值。
prompt-cache 路由有意不读取 cache_ttl 字段
（该字段驱动 manager.py 的 response-cache 层，属于另一个关注点）。
"""

import os

import pytest

# Set dummy key before import
# 在 import 之前设置一个假 key
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from llm_provider.anthropic_provider import (
    _ttl_block,
    resolve_prompt_cache_ttl_block,
    CACHE_TTL_LONG,
    CACHE_TTL_SHORT,
)
from llm_provider.base import CompletionRequest


def _mk(source=None):
    return CompletionRequest(
        messages=[{"role": "user", "content": "hi"}],
        model="claude-sonnet-4-6",
        cache_source=source,
    )


class TestSourceMapping:
    """Source -> TTL lookup without env overrides.
    不涉及环境变量覆盖时的 source -> TTL 查询。
    """

    def test_slack_channel_uses_long_ttl(self):
        assert _ttl_block(_mk("slack")) == CACHE_TTL_LONG

    def test_line_channel_uses_long_ttl(self):
        assert _ttl_block(_mk("line")) == CACHE_TTL_LONG

    def test_feishu_channel_uses_long_ttl(self):
        assert _ttl_block(_mk("feishu")) == CACHE_TTL_LONG

    def test_tui_uses_long_ttl(self):
        assert _ttl_block(_mk("tui")) == CACHE_TTL_LONG

    def test_webhook_uses_short_ttl(self):
        assert _ttl_block(_mk("webhook")) == CACHE_TTL_SHORT

    def test_cron_uses_short_ttl(self):
        assert _ttl_block(_mk("cron")) == CACHE_TTL_SHORT

    def test_mcp_uses_short_ttl(self):
        assert _ttl_block(_mk("mcp")) == CACHE_TTL_SHORT

    def test_swarm_subagent_uses_short_ttl(self):
        assert _ttl_block(_mk("swarm_subagent")) == CACHE_TTL_SHORT

    def test_tool_select_uses_short_ttl(self):
        """Internal swarm decision prompts — different each call, no resume.
        内部 swarm 决策 prompt —— 每次调用都不同，不会续用 cache。
        """
        assert _ttl_block(_mk("tool_select")) == CACHE_TTL_SHORT

    def test_lead_decide_uses_short_ttl(self):
        assert _ttl_block(_mk("lead_decide")) == CACHE_TTL_SHORT

    def test_interpretation_uses_short_ttl(self):
        assert _ttl_block(_mk("interpretation")) == CACHE_TTL_SHORT

    def test_unknown_source_defaults_short_fail_cheap(self):
        """Fail-cheap: unrecognized source pays 1.25x premium, not 2x.
        低成本兜底：未识别的 source 只付 1.25 倍写入溢价，而不是 2 倍。
        """
        assert _ttl_block(_mk("some_new_path_we_havent_classified_yet")) == CACHE_TTL_SHORT

    def test_none_source_defaults_short(self):
        assert _ttl_block(_mk(None)) == CACHE_TTL_SHORT

    def test_empty_string_source_defaults_short(self):
        assert _ttl_block(_mk("")) == CACHE_TTL_SHORT

    def test_source_is_case_insensitive(self):
        assert _ttl_block(_mk("SLACK")) == CACHE_TTL_LONG
        assert _ttl_block(_mk("Slack")) == CACHE_TTL_LONG


class TestEnvEscapeHatch:
    """SHANNON_FORCE_TTL overrides source map — operator debug/A-B.
    SHANNON_FORCE_TTL 会覆盖 source 映射 —— 供运维调试/A-B 对比。
    """

    def test_force_off_returns_none_suppresses_cache(self, monkeypatch):
        monkeypatch.setenv("SHANNON_FORCE_TTL", "off")
        assert _ttl_block(_mk("slack")) is None
        assert _ttl_block(_mk("webhook")) is None
        assert _ttl_block(_mk(None)) is None

    def test_force_5m_overrides_long_sources(self, monkeypatch):
        monkeypatch.setenv("SHANNON_FORCE_TTL", "5m")
        assert _ttl_block(_mk("slack")) == CACHE_TTL_SHORT
        assert _ttl_block(_mk("tui")) == CACHE_TTL_SHORT

    def test_force_1h_overrides_short_sources(self, monkeypatch):
        """For comparison benches — force everyone to 1h to recreate legacy behavior.
        用于对比基准 —— 把所有 source 强制成 1h，以复现旧行为。
        """
        monkeypatch.setenv("SHANNON_FORCE_TTL", "1h")
        assert _ttl_block(_mk("webhook")) == CACHE_TTL_LONG
        assert _ttl_block(_mk("cron")) == CACHE_TTL_LONG

    def test_force_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("SHANNON_FORCE_TTL", "OFF")
        assert _ttl_block(_mk("slack")) is None

    def test_force_invalid_value_falls_through_to_source_map(self, monkeypatch):
        """Typos don't silently break routing — invalid env value is ignored.
        拼写错误不会悄悄破坏路由 —— 无效的 env 值会被忽略。
        """
        monkeypatch.setenv("SHANNON_FORCE_TTL", "bogus")
        assert _ttl_block(_mk("slack")) == CACHE_TTL_LONG
        assert _ttl_block(_mk("webhook")) == CACHE_TTL_SHORT


class TestCacheTtlFieldIgnored:
    """request.cache_ttl is the response-cache TTL (manager.py), not prompt-cache.
    Prompt-cache routing must ignore it to avoid conflating the two layers.
    request.cache_ttl 是 response-cache 的 TTL（manager.py），不是 prompt-cache。
    prompt-cache 路由必须忽略它，避免把两个层次混为一谈。
    """

    def test_explicit_cache_ttl_does_not_override_source(self):
        req = CompletionRequest(
            messages=[{"role": "user", "content": "hi"}],
            model="claude-sonnet-4-6",
            cache_source="webhook",
            cache_ttl=3600,  # would imply 1h under naive interpretation
            # 按朴素理解会推出 1h
        )
        # webhook source still routes to short regardless of cache_ttl=3600
        # 无论 cache_ttl=3600，webhook source 仍路由到短 TTL
        assert _ttl_block(req) == CACHE_TTL_SHORT


class TestResolveLoopCacheSource:
    """Single source-of-truth helper the /agent/loop handler uses for BOTH
    message-level breakpoint tagging AND the provider call. If these drift,
    _force_uniform_cache_ttl silently rewrites the TTL and any upstream
    identity (e.g. shanclaw) becomes ineffective.
    /agent/loop handler 同时用于 message 级断点标记和 provider 调用的
    单一事实来源辅助函数。若两者发生漂移，_force_uniform_cache_ttl 会悄悄
    改写 TTL，任何上游身份（如 shanclaw）都会失效。
    """

    def _body(self, context=None):
        from llm_service.api.agent import AgentLoopStepRequest
        return AgentLoopStepRequest(
            agent_id="a", workflow_id="w", task="t",
            context=context if context is not None else {},
        )

    def test_context_cache_source_wins(self):
        from llm_service.api.agent import _resolve_loop_cache_source
        assert _resolve_loop_cache_source(self._body({"cache_source": "shanclaw"})) == "shanclaw"

    def test_fallback_to_agent_loop(self):
        from llm_service.api.agent import _resolve_loop_cache_source
        assert _resolve_loop_cache_source(self._body()) == "agent_loop"
        assert _resolve_loop_cache_source(self._body({})) == "agent_loop"

    def test_empty_or_whitespace_falls_back(self):
        from llm_service.api.agent import _resolve_loop_cache_source
        assert _resolve_loop_cache_source(self._body({"cache_source": ""})) == "agent_loop"
        assert _resolve_loop_cache_source(self._body({"cache_source": "   "})) == "agent_loop"
        assert _resolve_loop_cache_source(self._body({"cache_source": None})) == "agent_loop"

    def test_strips_surrounding_whitespace(self):
        from llm_service.api.agent import _resolve_loop_cache_source
        assert _resolve_loop_cache_source(self._body({"cache_source": "  slack  "})) == "slack"


class TestResolvePromptCacheTtlBlock:
    """Public pure-function API: same semantics as _ttl_block but takes a
    cache_source string so upstream callers (llm_service.api.agent) don't
    need a CompletionRequest object to resolve the TTL for message-level
    cache_control writes.
    公开的纯函数 API：语义与 _ttl_block 相同，但入参是 cache_source 字符串，
    这样上游调用方（llm_service.api.agent）在写 message 级 cache_control 时
    无需构造 CompletionRequest 对象即可解析 TTL。
    """

    def test_long_source_returns_long(self):
        assert resolve_prompt_cache_ttl_block("shanclaw") == CACHE_TTL_LONG
        assert resolve_prompt_cache_ttl_block("slack") == CACHE_TTL_LONG

    def test_short_source_returns_short(self):
        assert resolve_prompt_cache_ttl_block("agent_execute") == CACHE_TTL_SHORT
        assert resolve_prompt_cache_ttl_block("agent_loop") == CACHE_TTL_SHORT
        assert resolve_prompt_cache_ttl_block("webhook") == CACHE_TTL_SHORT

    def test_none_and_empty_return_short(self):
        assert resolve_prompt_cache_ttl_block(None) == CACHE_TTL_SHORT
        assert resolve_prompt_cache_ttl_block("") == CACHE_TTL_SHORT

    def test_case_insensitive(self):
        assert resolve_prompt_cache_ttl_block("SHANCLAW") == CACHE_TTL_LONG
        assert resolve_prompt_cache_ttl_block("ShanClaw") == CACHE_TTL_LONG

    def test_force_off_returns_none(self, monkeypatch):
        monkeypatch.setenv("SHANNON_FORCE_TTL", "off")
        assert resolve_prompt_cache_ttl_block("shanclaw") is None
        assert resolve_prompt_cache_ttl_block("agent_execute") is None

    def test_force_1h_overrides_short_source(self, monkeypatch):
        monkeypatch.setenv("SHANNON_FORCE_TTL", "1h")
        assert resolve_prompt_cache_ttl_block("agent_execute") == CACHE_TTL_LONG

    def test_force_5m_overrides_long_source(self, monkeypatch):
        monkeypatch.setenv("SHANNON_FORCE_TTL", "5m")
        assert resolve_prompt_cache_ttl_block("shanclaw") == CACHE_TTL_SHORT


class TestTtlBlockPublicWrapperParity:
    """_ttl_block(request) must stay byte-equivalent to
    resolve_prompt_cache_ttl_block(request.cache_source).

    This pins the root-cause fix invariant for agent.py: when agent.py
    writes message-level cache_control using the public helper, the
    provider's own writes (via _ttl_block) land on the exact same dict.
    If this parity ever drifts, the uniform-TTL guard is the only thing
    stopping another Anthropic 400, which is a regression.

    _ttl_block(request) 必须与 resolve_prompt_cache_ttl_block(request.cache_source)
    保持逐字节等价。

    这固定了 agent.py 根因修复的不变式：当 agent.py 用公开辅助函数写
    message 级 cache_control 时，provider 自身（通过 _ttl_block）的写入
    会落到完全相同的 dict 上。一旦这种等价性漂移，uniform-TTL 保护逻辑
    就是阻止再次出现 Anthropic 400 的唯一防线，那就是回归。
    """

    @pytest.mark.parametrize("source", [
        None, "", "shanclaw", "slack", "feishu", "tui", "cache_bench",
        "agent_execute", "agent_loop", "agent_execute_stream",
        "webhook", "cron", "mcp", "swarm_subagent",
    ])
    def test_wrapper_matches_public(self, source):
        req = CompletionRequest(
            messages=[{"role": "user", "content": "hi"}],
            model="claude-sonnet-4-6",
            cache_source=source,
        )
        assert _ttl_block(req) == resolve_prompt_cache_ttl_block(source)

    @pytest.mark.parametrize("force", ["off", "5m", "1h", "bogus"])
    def test_parity_holds_under_env_overrides(self, monkeypatch, force):
        monkeypatch.setenv("SHANNON_FORCE_TTL", force)
        req = CompletionRequest(
            messages=[{"role": "user", "content": "hi"}],
            model="claude-sonnet-4-6",
            cache_source="shanclaw",
        )
        assert _ttl_block(req) == resolve_prompt_cache_ttl_block("shanclaw")
