"""Verify the Anthropic provider relays the full ordered content_blocks list
end-to-end, including interleaved thinking blocks and their signatures.

This is the wire-shape change that lets ShanClaw (and any other client) echo
the assistant's trajectory back to Anthropic on the next turn — without it
the model loses the native thinking context, which empirically caused empty
`think({})` tool emissions and 14-minute agent-loop hangs.

See plan 2026-05-14-thinking-blocks-cc-alignment.md Phase A.

验证 Anthropic provider 端到端透传完整且有序的 content_blocks 列表，
包括交错的 thinking 块及其 signature。

这是改变 wire 格式的一步，让 ShanClaw（以及任何其他客户端）能在下一轮
把 assistant 的轨迹原样回传给 Anthropic —— 缺少这一步，模型会丢失原生
thinking context，实测会导致空的 think({}) tool 调用和 14 分钟的 agent loop 卡死。

参见 plan 2026-05-14-thinking-blocks-cc-alignment.md Phase A。
"""

import os
from types import SimpleNamespace

import pytest

# Set dummy key before import.
# 导入前先设置 dummy key。
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from llm_provider.anthropic_provider import _block_to_dict
from llm_provider.base import CompletionResponse, TokenUsage


# ---------- CompletionResponse dataclass field ----------
# ---------- CompletionResponse dataclass 字段 ----------

def test_completion_response_carries_content_blocks():
    resp = CompletionResponse(
        content="visible reply",
        model="claude-sonnet-4-6",
        provider="anthropic",
        usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15, estimated_cost=0.0),
        finish_reason="tool_use",
        content_blocks=[
            {"type": "thinking", "thinking": "plan", "signature": "sig1"},
            {"type": "text", "text": "visible reply"},
            {"type": "tool_use", "id": "toolu_X", "name": "file_read", "input": {"path": "/x"}},
        ],
    )
    assert resp.content_blocks is not None
    assert len(resp.content_blocks) == 3
    assert [b["type"] for b in resp.content_blocks] == ["thinking", "text", "tool_use"]


def test_completion_response_content_blocks_defaults_none():
    resp = CompletionResponse(
        content="hi",
        model="claude-sonnet-4-6",
        provider="anthropic",
        usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2, estimated_cost=0.0),
        finish_reason="stop",
    )
    assert resp.content_blocks is None


# ---------- _block_to_dict helper ----------
# ---------- _block_to_dict 辅助函数 ----------

def test_block_to_dict_pydantic_shape():
    """SDK Pydantic objects (most common case) — use model_dump.
    SDK 的 Pydantic 对象（最常见情况）—— 使用 model_dump。
    """
    class FakePydantic:
        def model_dump(self, exclude_none=True):
            return {"type": "thinking", "thinking": "x", "signature": "s"}

    out = _block_to_dict(FakePydantic(), "thinking")
    assert out == {"type": "thinking", "thinking": "x", "signature": "s"}


def test_block_to_dict_to_dict_shape():
    """Older SDKs / compat providers expose .to_dict().
    较老的 SDK / 兼容 provider 暴露 .to_dict()。
    """
    class FakeOldSdk:
        def to_dict(self):
            return {"type": "text", "text": "hello"}

    out = _block_to_dict(FakeOldSdk(), "text")
    assert out == {"type": "text", "text": "hello"}


def test_block_to_dict_plain_dict_input():
    """MiniMax compat: input is already a plain dict.
    MiniMax 兼容场景：输入本身已经是普通 dict。
    """
    inp = {"type": "tool_use", "id": "t1", "name": "x", "input": {"a": 1}}
    out = _block_to_dict(inp, "tool_use")
    # Output is a copy, not the same reference.
    # 输出是副本，不是同一个引用。
    assert out == inp
    assert out is not inp
    # Mutating output must not affect input.
    # 修改输出不能影响输入。
    out["mutated"] = True
    assert "mutated" not in inp


def test_block_to_dict_per_type_fallback_thinking():
    """SDK shape with neither model_dump nor to_dict — falls back to per-type.
    既没有 model_dump 也没有 to_dict 的 SDK 形态 —— 回退到按类型取值。
    """
    block = SimpleNamespace(type="thinking", thinking="fallback-text", signature="sig-fallback")
    # Strip the model_dump shape so the helper exercises the fallback path.
    # 去掉 model_dump 形态，让辅助函数走回退分支。
    out = _block_to_dict(block, "thinking")
    # SimpleNamespace has no model_dump/to_dict, so we hit the per-type branch.
    # SimpleNamespace 没有 model_dump/to_dict，因此会进入按类型分支。
    assert out is not None
    assert out["type"] == "thinking"
    assert out["thinking"] == "fallback-text"
    assert out["signature"] == "sig-fallback"


def test_block_to_dict_per_type_fallback_redacted_thinking():
    block = SimpleNamespace(type="redacted_thinking", data="opaque-blob")
    out = _block_to_dict(block, "redacted_thinking")
    assert out == {"type": "redacted_thinking", "data": "opaque-blob"}


def test_block_to_dict_unknown_type_returns_none():
    """Unknown / unsupported block types: emit None so caller skips them.
    未知 / 不支持的块类型：返回 None，由调用方跳过。
    """
    block = SimpleNamespace(type="future_block_type")
    out = _block_to_dict(block, "future_block_type")
    # No model_dump, no to_dict, no per-type match → None.
    # 没有 model_dump、没有 to_dict、也没有按类型匹配 → 返回 None。
    assert out is None


# ---------- _serialize_completion wire shape ----------
# ---------- _serialize_completion 的 wire 结构 ----------

def test_serialize_completion_includes_content_blocks():
    # Lazy import to avoid loading the whole providers package at module top.
    # 延迟导入，避免在模块顶层加载整个 providers 包。
    from llm_service.providers import ProviderManager

    resp = CompletionResponse(
        content="visible",
        model="claude-sonnet-4-6",
        provider="anthropic",
        usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2, estimated_cost=0.0),
        finish_reason="stop",
        content_blocks=[
            {"type": "thinking", "thinking": "x", "signature": "s"},
            {"type": "text", "text": "visible"},
        ],
    )

    mgr = ProviderManager.__new__(ProviderManager)
    out = mgr._serialize_completion(resp)

    assert "content_blocks" in out
    assert out["content_blocks"][0] == {"type": "thinking", "thinking": "x", "signature": "s"}
    # Legacy fields still populated for older clients.
    # 为老客户端保留 legacy 字段。
    assert out["output_text"] == "visible"


def test_serialize_completion_omits_content_blocks_when_none():
    """Backward compat: when provider didn't populate the new field, the wire
    response must NOT carry an empty content_blocks key — that would force
    older clients to handle a None / [] value they don't expect.
    向后兼容：当 provider 没有填充这个新字段时，wire response 绝不能带上空的
    content_blocks key —— 否则老客户端不得不处理它们并不预期的 None / [] 值。
    """
    from llm_service.providers import ProviderManager

    resp = CompletionResponse(
        content="hi",
        model="claude-sonnet-4-6",
        provider="anthropic",
        usage=TokenUsage(input_tokens=1, output_tokens=1, total_tokens=2, estimated_cost=0.0),
        finish_reason="stop",
        content_blocks=None,
    )
    mgr = ProviderManager.__new__(ProviderManager)
    out = mgr._serialize_completion(resp)
    assert "content_blocks" not in out


# ---------- Redis cache serialize/deserialize roundtrip ----------
# ---------- Redis cache 序列化/反序列化往返 ----------

def test_redis_cache_roundtrip_preserves_content_blocks():
    """A Redis cache hit must return content_blocks intact. Without this fix,
    every cached response silently reverted to {output_text, tool_calls} only
    and the thinking trajectory broke for any cached upstream call.
    Redis cache 命中时必须原样返回 content_blocks。没有这个修复，每个 cache response
    都会悄悄退化成只剩 {output_text, tool_calls}，任何走 cache 的上游调用其 thinking
    轨迹都会断掉。
    """
    from llm_provider.manager import _serialize_response, _deserialize_response

    resp = CompletionResponse(
        content="visible",
        model="claude-sonnet-4-6",
        provider="anthropic",
        usage=TokenUsage(input_tokens=10, output_tokens=5, total_tokens=15, estimated_cost=0.0),
        finish_reason="tool_use",
        tool_calls=[{"id": "t1", "name": "f", "arguments": {}}],
        content_blocks=[
            {"type": "thinking", "thinking": "private reasoning", "signature": "sigA"},
            {"type": "text", "text": "visible"},
            {"type": "tool_use", "id": "t1", "name": "f", "input": {}},
            {"type": "thinking", "thinking": "interleaved reasoning", "signature": "sigB"},
        ],
    )

    serialized = _serialize_response(resp)
    assert "content_blocks" in serialized, "serializer dropped content_blocks"
    assert len(serialized["content_blocks"]) == 4
    assert [b["type"] for b in serialized["content_blocks"]] == ["thinking", "text", "tool_use", "thinking"]
    assert serialized["content_blocks"][0]["signature"] == "sigA"
    assert serialized["content_blocks"][3]["signature"] == "sigB"

    deserialized = _deserialize_response(serialized)
    assert deserialized.content_blocks is not None
    assert len(deserialized.content_blocks) == 4
    assert deserialized.content_blocks[0]["thinking"] == "private reasoning"
    assert deserialized.content_blocks[0]["signature"] == "sigA"
    assert deserialized.cached is True  # cache marker still flips


def test_redis_cache_legacy_response_no_content_blocks():
    """Cache entries written before the new field exists must still deserialize
    cleanly with content_blocks=None — no exception, no fabricated empty list.
    新字段出现之前写入的 cache 条目，仍须能以 content_blocks=None 正常反序列化
    —— 不抛 exception，也不伪造空列表。
    """
    from llm_provider.manager import _deserialize_response

    legacy_payload = {
        "content": "hi",
        "model": "claude-sonnet-4-6",
        "provider": "anthropic",
        "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2, "estimated_cost": 0.0},
        "finish_reason": "stop",
        # No content_blocks key — pre-2026-05 cache entry.
        # 没有 content_blocks key —— 2026-05 之前的 cache 条目。
    }
    resp = _deserialize_response(legacy_payload)
    assert resp.content_blocks is None
    assert resp.content == "hi"


# ---------- Streaming path parity ----------
# ---------- streaming 路径行为对齐 ----------

@pytest.mark.asyncio
async def test_stream_complete_yields_content_blocks():
    """stream_complete's final result dict must carry the ordered content_blocks
    list (mirror of non-stream complete()). Without this, streaming clients
    (TUI, future SSE consumers) lose thinking blocks while non-stream clients
    keep them — same asymmetric bug the Cloud-side reviewer flagged. The
    streaming SSE done event in completions.py then forwards this key to the
    wire.
    stream_complete 的最终结果 dict 必须携带有序的 content_blocks 列表
    （与非 stream complete() 保持一致）。否则 stream 客户端（TUI、未来的 SSE 消费方）
    会丢失 thinking 块，而非 stream 客户端仍能拿到 —— 正是 Cloud 侧 reviewer 指出的
    那种不对称 bug。随后 completions.py 的 streaming SSE done event 会把这个 key
    转发到 wire 上。
    """
    from unittest.mock import MagicMock
    from llm_provider.anthropic_provider import AnthropicProvider

    config = {
        "name": "anthropic",
        "api_key": "test-key",
        "models": {
            "claude-sonnet-4-6": {
                "model_id": "claude-sonnet-4-6",
                "tier": "medium",
                "context_window": 200000,
                "max_tokens": 8192,
                "input_price_per_1k": 0.003,
                "output_price_per_1k": 0.015,
            },
        },
    }

    # Build a final message with interleaved thinking + tool_use blocks.
    # 构造一条包含交错 thinking + tool_use 块的最终 message。
    final_msg = SimpleNamespace(
        content=[
            SimpleNamespace(type="thinking", thinking="reasoning step 1", signature="sigA"),
            SimpleNamespace(type="text", text="visible reply"),
            SimpleNamespace(type="tool_use", id="t1", name="f", input={"x": 1}),
            SimpleNamespace(type="thinking", thinking="reasoning step 2 (interleaved)", signature="sigB"),
        ],
        usage=SimpleNamespace(
            input_tokens=100,
            output_tokens=50,
            cache_read_input_tokens=0,
            cache_creation_input_tokens=0,
            cache_creation=None,
        ),
        model="claude-sonnet-4-6",
        stop_reason="tool_use",
        id="msg_test",
    )

    class FakeStream:
        text_stream = None

        def __init__(self):
            self.text_stream = self._text_iter()

        async def _text_iter(self):
            yield "visible reply"

        async def get_final_message(self):
            return final_msg

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            return False

    provider = AnthropicProvider(config)
    provider.client = MagicMock()
    provider.client.messages.stream = MagicMock(return_value=FakeStream())

    from llm_provider.base import CompletionRequest

    request = CompletionRequest(
        messages=[{"role": "user", "content": "hi"}],
        model="claude-sonnet-4-6",
    )

    chunks = []
    async for chunk in provider.stream_complete(request):
        chunks.append(chunk)

    # The final dict carries content_blocks in original order.
    # 最终 dict 按原始顺序携带 content_blocks。
    final = next(c for c in reversed(chunks) if isinstance(c, dict))
    assert "content_blocks" in final, f"streaming final dict missing content_blocks; keys={list(final)}"
    blocks = final["content_blocks"]
    assert len(blocks) == 4, f"expected 4 blocks, got {len(blocks)}: {blocks}"
    types_in_order = [b["type"] for b in blocks]
    assert types_in_order == ["thinking", "text", "tool_use", "thinking"], (
        f"order broken: {types_in_order} (Anthropic requires verbatim order)"
    )
    # Each thinking block keeps its own signature.
    # 每个 thinking 块保留自己的 signature。
    assert blocks[0]["signature"] == "sigA"
    assert blocks[3]["signature"] == "sigB"
    # Legacy fields still populated for backward compat.
    # 为向后兼容保留 legacy 字段。
    assert final.get("function_call") == {"id": "t1", "name": "f", "arguments": {"x": 1}}
