"""Byte-stability regression tests — pin down cross-turn hash invariants so
prev_marker preservation stays reliable.

These tests protect against the pathology observed in session
2026-04-15-69f601dc1c98 (same system_len across 61 requests but 17 distinct
system_h hashes → −13pp CHR vs session-peer median). The stable-hash function
must be invariant under content shape that clients may legitimately vary.

字节稳定性回归测试 —— 固定跨轮次的 hash 不变量，保证 prev_marker 保留可靠。

针对 session 2026-04-15-69f601dc1c98 观察到的病态现象：61 次 request 的
system_len 相同，却出现 17 个不同的 system_h hash（CHR 比同 session 中位数低 13pp）。
稳定 hash 函数必须对客户端可能合法变化的内容形态保持不变。
"""

import os

# 占位 API key：AnthropicProvider.__init__ 缺 key 会直接 raise，import 前先兜底
os.environ.setdefault("ANTHROPIC_API_KEY", "test-key-for-unit-tests")

from llm_provider.anthropic_provider import (
    _msg_stable_hash,
    _strip_cache_control_for_hash,
)


class TestStableHashInvariants:
    """Logical equivalents MUST hash identically. Each test case represents a
    serialization shape that can legitimately vary across requests without a
    semantic change.

    逻辑等价的消息必须得到相同 hash。每个用例对应一种客户端可能合法变化的
    序列化形态 —— 形态不同但语义不变。
    """

    def test_string_content_shape_is_distinguished(self):
        """DOCUMENTS intentional behavior: a string-form message and a
        text-block-form message hash differently. This is correct because the
        two produce different bytes on the Anthropic wire, so treating them as
        equivalent for prev_marker matching would point at a position whose
        actual bytes differ — guaranteed cache miss. Clients MUST be consistent
        about which form they produce (ShanClaw gateway.go normalizes before
        send).

        记录有意为之的行为：字符串形式与 text-block 形式的同一条消息 hash 不同。
        这是正确的，因为两者在 Anthropic 线路上产生不同字节；若把它们视为等价，
        prev_marker 会指向字节不同的位置 —— 必然 cache miss。
        客户端必须固定使用其中一种形式（ShanClaw gateway.go 在发送前会归一化）。
        """
        msg_str = {"role": "assistant", "content": "Hello world"}
        msg_block = {"role": "assistant", "content": [{"type": "text", "text": "Hello world"}]}
        # Different hashes on purpose — see docstring.
        # 故意不同 —— 见 docstring。
        assert _msg_stable_hash(msg_str) != _msg_stable_hash(msg_block)

    def test_tool_use_input_key_ordering(self):
        """Go map iteration order is randomized; the same logical tool_use call
        can serialize with different key orders per goroutine. Hash must absorb.

        Go map 的迭代顺序是随机的，同一个逻辑 tool_use 调用在不同 goroutine 上
        可能序列化成不同 key 顺序，hash 必须吸收这种差异。
        """
        msg1 = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "file_read",
             "input": {"path": "/a", "line": 5}}
        ]}
        msg2 = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "file_read",
             "input": {"line": 5, "path": "/a"}}
        ]}
        assert _msg_stable_hash(msg1) == _msg_stable_hash(msg2)

    def test_tool_use_nested_input_key_ordering(self):
        """Nested map keys too — covers tool inputs with structured parameters.

        嵌套 map 的 key 同理 —— 覆盖带结构化参数的 tool input。
        """
        msg1 = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "bash",
             "input": {"cmd": "ls", "opts": {"recursive": True, "color": "auto"}}}
        ]}
        msg2 = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "bash",
             "input": {"opts": {"color": "auto", "recursive": True}, "cmd": "ls"}}
        ]}
        assert _msg_stable_hash(msg1) == _msg_stable_hash(msg2)

    def test_tool_result_content_shape_is_distinguished(self):
        """Same as test_string_content_shape_is_distinguished: clients must
        pick one form and stick with it. Shannon's gateway sends tool_result as
        plain string, ShanClaw is the source of truth.

        与 test_string_content_shape_is_distinguished 同理：客户端必须选定一种
        形式并保持一致。Shannon 的 gateway 以纯字符串发送 tool_result，
        ShanClaw 是事实来源。
        """
        msg_str = {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1", "content": "result text"}
        ]}
        msg_blk = {"role": "user", "content": [
            {"type": "tool_result", "tool_use_id": "t1",
             "content": [{"type": "text", "text": "result text"}]}
        ]}
        # Intentionally different — see docstring.
        # 有意不同 —— 见 docstring。
        assert _msg_stable_hash(msg_str) != _msg_stable_hash(msg_blk)

    def test_cache_control_stripped_before_hashing(self):
        """Mutated cache_control markers MUST be transparent to the hash;
        otherwise applying a rolling marker would invalidate its own lookup.

        被改写的 cache_control 标记必须对 hash 透明，否则打上滚动标记
        本身就会让下一次查找失效。
        """
        msg_clean = {"role": "user", "content": [{"type": "text", "text": "hello"}]}
        msg_marked = {"role": "user", "content": [
            {"type": "text", "text": "hello", "cache_control": {"type": "ephemeral", "ttl": "1h"}}
        ]}
        assert _msg_stable_hash(msg_clean) == _msg_stable_hash(msg_marked)

    def test_cache_control_stripped_nested(self):
        """Nested cache_control at any block depth is stripped.

        任意 block 深度的嵌套 cache_control 都会被剥离。
        """
        obj = {
            "role": "user",
            "content": [
                {"type": "text", "text": "hi", "cache_control": {"type": "ephemeral"}},
                {"type": "text", "text": "bye"},
            ]
        }
        stripped = _strip_cache_control_for_hash(obj)
        for block in stripped["content"]:
            assert "cache_control" not in block

    def test_tool_use_id_participates_in_hash(self):
        """tool_use IDs are cross-request stable identifiers and MUST affect
        the hash — different IDs signify different prior tool invocations.

        tool_use ID 是跨 request 稳定的标识符，必须参与 hash ——
        不同 ID 代表不同的历史 tool 调用。
        """
        msg1 = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "x", "input": {}}
        ]}
        msg2 = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t2", "name": "x", "input": {}}
        ]}
        assert _msg_stable_hash(msg1) != _msg_stable_hash(msg2)

    def test_role_difference_breaks_hash(self):
        """A user message and an assistant message with identical content must
        hash differently — role is a semantic distinction.

        内容完全相同的 user 消息与 assistant 消息必须 hash 不同 —— role 是语义区分。
        """
        msg_user = {"role": "user", "content": "hello"}
        msg_asst = {"role": "assistant", "content": "hello"}
        assert _msg_stable_hash(msg_user) != _msg_stable_hash(msg_asst)

    def test_repeated_hash_is_deterministic(self):
        """Sanity: hashing the same message twice must yield the same digest
        within a process. Covers accidental dependency on iteration order.

        基本检查：同一进程内对同一条消息 hash 两次必须得到相同 digest，
        覆盖对迭代顺序的意外依赖。
        """
        msg = {"role": "assistant", "content": [
            {"type": "tool_use", "id": "t1", "name": "file_read",
             "input": {"path": "/a", "line": 5, "offset": 10}}
        ]}
        h1 = _msg_stable_hash(msg)
        h2 = _msg_stable_hash(msg)
        assert h1 == h2 and h1 != ""

    def test_non_ascii_content_stable(self):
        """Unicode in content must not trip ensure_ascii or encoding drift.
        Agents on this project do speak Chinese/Japanese frequently.

        内容中的 Unicode 不能触发 ensure_ascii 或编码漂移。
        本项目的 agent 经常使用中文/日文。
        """
        msg1 = {"role": "user", "content": "你好，世界"}
        msg2 = {"role": "user", "content": "你好，世界"}
        assert _msg_stable_hash(msg1) == _msg_stable_hash(msg2) != ""


class TestStripCacheControl:
    """Unit coverage of the hash-input preprocessor.

    hash 输入预处理器（剥离 cache_control）的单元覆盖。
    """

    def test_strip_leaves_non_dict_values_unchanged(self):
        # 非 dict 值原样返回，不参与剥离
        assert _strip_cache_control_for_hash("just a string") == "just a string"
        assert _strip_cache_control_for_hash(42) == 42
        assert _strip_cache_control_for_hash(None) is None

    def test_strip_handles_list_of_blocks(self):
        # 列表逐项剥离，保持顺序与其余 key
        blocks = [
            {"type": "text", "text": "a", "cache_control": {"type": "ephemeral"}},
            {"type": "text", "text": "b"},
        ]
        out = _strip_cache_control_for_hash(blocks)
        assert out == [{"type": "text", "text": "a"}, {"type": "text", "text": "b"}]

    def test_strip_preserves_non_cache_control_keys(self):
        # 只删 cache_control，citations 等其他 key 必须保留
        blk = {"type": "text", "text": "hi", "citations": [1, 2], "cache_control": {}}
        out = _strip_cache_control_for_hash(blk)
        assert out == {"type": "text", "text": "hi", "citations": [1, 2]}
