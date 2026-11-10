"""
Anthropic Claude Provider Implementation

Anthropic Claude provider 实现。
"""

import hashlib
import json
import os
import logging
import time
from collections import OrderedDict
from typing import Dict, List, Any, AsyncIterator, Optional
import anthropic
from anthropic import AsyncAnthropic

from .base import (
    LLMProvider,
    CompletionRequest,
    CompletionResponse,
    TokenUsage,
    TokenCounter,
    extract_text_from_content,
    _ensure_tool_id,
)


def _block_to_dict(content_block: Any, block_type: Optional[str]) -> Optional[Dict[str, Any]]:
    """Serialize a single Anthropic content block into a verbatim dict.

    Strategy (most → least preferred):
      1. SDK Pydantic shape: ``model_dump(exclude_none=True)`` — preserves any
         forward-compat fields like new ``redacted_thinking`` subtypes without
         this serializer needing to know about them.
      2. Generic ``.to_dict()`` — older Anthropic SDK / some compat providers.
      3. Plain dict input (e.g., MiniMax compat path) — copy.
      4. Per-type fallback — manual field extraction for the four block types
         we know exist (``text`` / ``thinking`` / ``redacted_thinking`` /
         ``tool_use``). Unknown types return ``None`` (silently — see below)
         rather than emitting a partial shape Anthropic would reject on the
         round-trip.

    Returns ``None`` if all strategies fail. The caller skips ``None`` entries
    (legacy ``content`` / ``tool_calls`` still carry enough info for older
    clients); a ``logger.debug`` line is emitted so operators can grep for new
    Anthropic block types we haven't taught the per-type fallback yet.

    将单个 Anthropic content block 序列化为逐字 dict。

    策略（优先级从高到低）：
      1. SDK 的 Pydantic 形状：``model_dump(exclude_none=True)`` —— 可保留诸如新的
         ``redacted_thinking`` 子类型等向前兼容字段，本序列化器无需了解它们。
      2. 通用的 ``.to_dict()`` —— 老旧 Anthropic SDK / 部分兼容 provider。
      3. 纯 dict 输入（如 MiniMax 兼容路径）—— 复制。
      4. 按类型兜底 —— 对我们已知的四种 block 类型（``text`` / ``thinking`` /
         ``redacted_thinking`` / ``tool_use``）手工提取字段。未知类型返回 ``None``
         （静默处理，见下），避免产生 Anthropic 在回传时会拒绝的半成品形状。

    所有策略都失败时返回 ``None``。调用方会跳过 ``None`` 项（遗留的 ``content`` /
    ``tool_calls`` 对老客户端仍包含足够信息）；同时会输出一条 ``logger.debug``，
    便于运维排查我们尚未支持的 Anthropic 新 block 类型。
    """
    # 1. Pydantic SDK shape.
    model_dump = getattr(content_block, "model_dump", None)
    if callable(model_dump):
        try:
            d = model_dump(exclude_none=True)
            if isinstance(d, dict):
                return d
        except Exception as e:  # noqa: BLE001
            logger.debug("model_dump failed on Anthropic block (type=%s): %s", block_type, e)

    # 2. Generic .to_dict() on older SDK shapes.
    to_dict = getattr(content_block, "to_dict", None)
    if callable(to_dict):
        try:
            d = to_dict()
            if isinstance(d, dict):
                return d
        except Exception as e:  # noqa: BLE001
            logger.debug("to_dict failed on Anthropic block (type=%s): %s", block_type, e)

    # 3. Plain dict input (MiniMax, etc.). Make a shallow copy so callers
    # can't mutate the upstream object via our returned dict.
    if isinstance(content_block, dict):
        return dict(content_block)

    # 4. Per-type fallback for the four known shapes.
    if block_type == "text":
        return {"type": "text", "text": getattr(content_block, "text", "") or ""}
    if block_type == "thinking":
        return {
            "type": "thinking",
            "thinking": getattr(content_block, "thinking", "") or "",
            "signature": getattr(content_block, "signature", "") or "",
        }
    if block_type == "redacted_thinking":
        return {"type": "redacted_thinking", "data": getattr(content_block, "data", "") or ""}
    if block_type == "tool_use":
        return {
            "type": "tool_use",
            "id": getattr(content_block, "id", "") or "",
            "name": getattr(content_block, "name", "") or "",
            "input": getattr(content_block, "input", {}) or {},
        }

    # Unknown / unhandled type: don't emit a partial / malformed shape. Log so
    # the day Anthropic ships a new block type we have a breadcrumb pointing at
    # this branch (vs. silent drop forever).
    logger.debug("Unhandled Anthropic content block type: %s", block_type)
    return None

logger = logging.getLogger(__name__)

# Optional prompt-cache metrics (per-source observability).
# Labels: provider, model, source. `source` comes from CompletionRequest.cache_source
# so we can see which call sites (agent_loop/decompose/tool_select/synthesis) are
# paying the 2.0x 1h write premium without enough reads to break even.
# 可选的 prompt cache 指标（按来源观测）。
# 标签：provider、model、source。`source` 来自 CompletionRequest.cache_source，
# 用于观察哪些调用点（agent_loop/decompose/tool_select/synthesis）支付了 2.0 倍的
# 1 小时写入溢价，却读取不足、无法回本。
try:
    from prometheus_client import Counter as _PromCounter

    _CACHE_METRICS_ENABLED = True
    ANTHROPIC_CACHE_READ_TOKENS = _PromCounter(
        "anthropic_cache_read_tokens_total",
        "Anthropic prompt cache read tokens (billed at 0.1x input)",
        labelnames=("provider", "model", "source"),
    )
    ANTHROPIC_CACHE_WRITE_5M_TOKENS = _PromCounter(
        "anthropic_cache_write_5m_tokens_total",
        "Anthropic prompt cache write tokens at 5min TTL (billed at 1.25x input)",
        labelnames=("provider", "model", "source"),
    )
    ANTHROPIC_CACHE_WRITE_1H_TOKENS = _PromCounter(
        "anthropic_cache_write_1h_tokens_total",
        "Anthropic prompt cache write tokens at 1h TTL (billed at 2.0x input)",
        labelnames=("provider", "model", "source"),
    )
except Exception:
    _CACHE_METRICS_ENABLED = False


def _record_cache_metrics(
    provider: str,
    model: str,
    source: Optional[str],
    cache_read: int,
    cache_creation: int,
    cache_creation_1h: int,
) -> None:
    """Emit per-source cache metrics. Cheap no-op if prometheus is unavailable.

    上报按来源划分的 cache 指标。prometheus 不可用时为低开销的空操作。
    """
    if not _CACHE_METRICS_ENABLED:
        return
    src = source or "unknown"
    try:
        if cache_read:
            ANTHROPIC_CACHE_READ_TOKENS.labels(provider, model, src).inc(cache_read)
        cache_5m = max(0, cache_creation - cache_creation_1h)
        if cache_5m:
            ANTHROPIC_CACHE_WRITE_5M_TOKENS.labels(provider, model, src).inc(cache_5m)
        if cache_creation_1h:
            ANTHROPIC_CACHE_WRITE_1H_TOKENS.labels(provider, model, src).inc(cache_creation_1h)
    except Exception:
        # Metrics must never break the request path
        pass


CACHE_BREAK_MARKER = "<!-- cache_break -->"
VOLATILE_MARKER = "<!-- volatile -->"

# Anthropic prompt cache TTL blocks.
# 1h: write premium 2x, break-even when cache_read amortizes across >=3 calls
# 5m: write premium 1.25x, break-even from 1 read — correct for one-shot paths
# Ordering across breakpoints: system >= tools >= messages — monotonic non-increasing.
# Which TTL applies per call is resolved by `_ttl_block(request)` based on cache_source.
# Anthropic prompt cache 的 TTL block。
# 1h：写入溢价 2 倍，当 cache_read 在 >=3 次调用中摊薄时才回本
# 5m：写入溢价 1.25 倍，1 次读取即可回本 —— 适合一次性调用路径
# 断点顺序：system >= tools >= messages —— 单调不增。
# 每次调用适用哪个 TTL 由 `_ttl_block(request)` 依据 cache_source 决定。
CACHE_TTL_LONG = {"type": "ephemeral", "ttl": "1h"}
CACHE_TTL_SHORT = {"type": "ephemeral"}

# Sources that amortize across many turns → worth the 1h write premium.
# Human-conversation channels where idle > 5m is common. Keep in sync with
# docs/cache-strategy.md. Unknown/unset → treated as short (fail cheap).
# 可跨多轮摊薄的来源 → 值得支付 1 小时写入溢价。
# 这些是人工 session 渠道，空闲超过 5 分钟很常见。需与 docs/cache-strategy.md 保持同步。
# 未知/未设置 → 按短 TTL 处理（以低成本失败兜底）。
_LONG_CACHE_SOURCES = frozenset({
    "slack", "line", "feishu", "lark", "telegram",
    "tui", "shanclaw", "oneshot_interactive", "cache_bench",
})


def resolve_prompt_cache_ttl_block(cache_source: Optional[str]) -> Optional[Dict[str, str]]:
    """Pure function: map a cache_source string to the cache_control block.

    Public API so upstream callers (e.g. llm_service.api.agent) can resolve
    the same TTL as the provider without depending on a CompletionRequest
    object. Returns None when SHANNON_FORCE_TTL=off to suppress prompt-cache
    writes entirely.

    Precedence (matches _ttl_block):
      1. SHANNON_FORCE_TTL env (off / 5m / 1h) — operator escape hatch
      2. cache_source → _LONG_CACHE_SOURCES map → 1h
      3. Fallback → 5m (short is the safe default for unknown / internal
         subagent paths; 1.25x write premium vs 2x for 1h)

    纯函数：把 cache_source 字符串映射为 cache_control block。

    作为公开 API，上游调用方（如 llm_service.api.agent）无需依赖 CompletionRequest
    对象即可解析出与 provider 相同的 TTL。当 SHANNON_FORCE_TTL=off 时返回 None，
    以完全禁止 prompt cache 写入。

    优先级（与 _ttl_block 一致）：
      1. SHANNON_FORCE_TTL 环境变量（off / 5m / 1h）—— 运维应急开关
      2. cache_source → _LONG_CACHE_SOURCES 映射 → 1h
      3. 兜底 → 5m（对未知/内部子 agent 路径而言短 TTL 是安全默认值；
         写入溢价 1.25 倍，而 1 小时为 2 倍）
    """
    force = os.environ.get("SHANNON_FORCE_TTL", "").strip().lower()
    if force == "off":
        return None
    if force == "5m":
        return CACHE_TTL_SHORT
    if force == "1h":
        return CACHE_TTL_LONG

    src = (cache_source or "").strip().lower()
    if src in _LONG_CACHE_SOURCES:
        return CACHE_TTL_LONG
    return CACHE_TTL_SHORT


def _ttl_block(request) -> Optional[Dict[str, str]]:
    """Resolve the cache_control block for a CompletionRequest.

    Thin wrapper over resolve_prompt_cache_ttl_block so provider and upstream
    callers share one source of truth. Note: request.cache_ttl is NOT consulted
    here — that field drives manager.py's response-cache TTL, a different layer.

    为 CompletionRequest 解析 cache_control block。

    对 resolve_prompt_cache_ttl_block 的薄封装，使 provider 与上游调用方共享同一
    事实来源。注意：此处不会读取 request.cache_ttl —— 该字段驱动的是 manager.py
    的 response cache TTL，属于另一个层次。
    """
    return resolve_prompt_cache_ttl_block(getattr(request, "cache_source", None))


# Per-session memo of the last applied rolling marker hash.
# Why: Anthropic prefix cache only matches at positions with explicit
# cache_control. Without remembering last turn's marker, the new turn's
# rolling marker writes a fresh block while the previous block becomes
# unreachable — long-session CER drops from 16.85x (3-turn) to 2.35x
# (30-turn). Preserving prev marker keeps both writes readable.
#
# Concurrency: _apply_rolling_cache_marker is currently NOT wired into
# _build_api_request (disabled by the 2026-04-15 bench regression — see
# docs/cache-strategy.md). That means this dict never sees concurrent writes
# in production today. If the preservation path is ever re-enabled, wrap
# _remember_marker / _recall_marker with an asyncio.Lock (or move the memo
# to a per-session object) before the first request hits them — FastAPI
# dispatches async handlers concurrently and OrderedDict.popitem is not
# coroutine-safe.
# 按 session 记忆最近一次应用的滚动标记哈希。
# 原因：Anthropic 前缀 cache 只在显式设置 cache_control 的位置匹配。若不记住上一轮的
# 标记，新一轮的滚动标记会写入新 block，而旧 block 变得不可达 —— 长 session CER 会从
# 16.85 倍（3 轮）降到 2.35 倍（30 轮）。保留上一轮标记可让两次写入都可读。
#
# 并发性：_apply_rolling_cache_marker 目前并未接入 _build_api_request（因
# 2026-04-15 基准回归而禁用 —— 见 docs/cache-strategy.md）。因此当前生产环境中该
# dict 不会出现并发写入。若将来重新启用保留路径，请在首个 request 到达前用
# asyncio.Lock 包裹 _remember_marker / _recall_marker（或将 memo 改为按 session 对象
# 存放）—— FastAPI 会并发派发 async handler，而 OrderedDict.popitem 不是协程安全的。
_PREV_ROLLING_MAX = 1000
_prev_rolling = OrderedDict()  # session_id → marker hash; LRU evicted


def _msg_stable_hash(msg: Dict[str, Any]) -> str:
    """Semantic hash of a claude_message for cross-turn marker matching.

    Why semantic instead of full JSON: clients re-serialize messages between
    turns with structural variations (content as str vs [{"type":"text",...}],
    optional fields present/absent, dict ordering). A pure JSON hash drifts
    even when the underlying message is "the same one." This hash extracts
    just (role, semantic content) — invariant to those representation shifts.

    Includes tool_use IDs and tool_result tool_use_ids since those are stable
    Anthropic-generated identifiers that uniquely tag a turn's content.

    用于跨轮标记匹配的 claude_message 语义哈希。

    为什么用语义哈希而非完整 JSON：客户端在轮次之间重新序列化消息时会出现结构差异
    （content 为 str 还是 [{"type":"text",...}]、可选字段有无、dict 顺序不同）。
    纯 JSON 哈希即使底层消息是"同一条"也会漂移。本哈希只提取 (role, 语义内容)，
    对这些表示差异保持不变。

    包含 tool_use ID 与 tool_result 的 tool_use_id，因为它们是 Anthropic 生成的
    稳定标识符，能唯一标记某一轮的内容。
    """
    try:
        role = msg.get("role", "")
        sig = _semantic_signature(msg.get("content", ""))
        return hashlib.sha1((role + "|" + sig).encode("utf-8")).hexdigest()[:16]
    except Exception:
        return ""


def _semantic_signature(content: Any) -> str:
    """Build a normalized string signature of a message's content.
    String content -> raw text. List content -> "T:text" / "U:tool_use_id" /
    "R:tool_result_id:text" tokens joined by newline. Excludes cache_control,
    type field shape, and other structural noise.

    构建消息内容的归一化字符串签名。
    字符串内容 -> 原始文本。列表内容 -> 由换行连接的 "T:text" / "U:tool_use_id" /
    "R:tool_result_id:text" token。排除 cache_control、type 字段形状及其他结构噪声。
    """
    if isinstance(content, str):
        return "S:" + content
    if isinstance(content, list):
        parts = []
        for b in content:
            if not isinstance(b, dict):
                continue
            btype = b.get("type", "")
            if btype == "text":
                parts.append("T:" + str(b.get("text", "")))
            elif btype == "tool_use":
                # Tool use ID is stable across turns (Anthropic-assigned)
                parts.append("U:" + str(b.get("id", "")))
            elif btype == "tool_result":
                tid = str(b.get("tool_use_id", ""))
                tc = b.get("content", "")
                tc_str = tc if isinstance(tc, str) else json.dumps(tc, sort_keys=True, default=str)
                parts.append("R:" + tid + ":" + tc_str)
            elif btype in ("image", "document"):
                # Hash by source URL/data presence; ignore noise like media_type ordering
                src = b.get("source", {}) if isinstance(b.get("source"), dict) else {}
                parts.append(btype[0].upper() + ":" + str(src.get("url") or src.get("data", ""))[:64])
            else:
                # Unknown block: hash whole thing (rare; keep stable as best we can)
                parts.append("X:" + json.dumps(b, sort_keys=True, default=str))
        return "\n".join(parts)
    # Fallback for unexpected shapes
    return "F:" + json.dumps(content, sort_keys=True, default=str)


def _strip_cache_control_for_hash(obj: Any) -> Any:
    """Return a deep copy of obj with all `cache_control` keys removed.
    Kept for backward-compat / debugging only — _msg_stable_hash no longer uses it.

    返回移除了所有 `cache_control` key 的 obj 深拷贝。
    仅用于向后兼容/调试 —— _msg_stable_hash 已不再使用。
    """
    if isinstance(obj, dict):
        return {k: _strip_cache_control_for_hash(v) for k, v in obj.items() if k != "cache_control"}
    if isinstance(obj, list):
        return [_strip_cache_control_for_hash(item) for item in obj]
    return obj


def _remember_marker(session_id: str, msg_hash: str) -> None:
    if not session_id or not msg_hash:
        return
    if session_id in _prev_rolling:
        _prev_rolling.move_to_end(session_id)
    _prev_rolling[session_id] = msg_hash
    while len(_prev_rolling) > _PREV_ROLLING_MAX:
        _prev_rolling.popitem(last=False)


def _recall_marker(session_id: str) -> Optional[str]:
    if not session_id:
        return None
    return _prev_rolling.get(session_id)


def _build_beta_header(
    *,
    thinking: bool,
    any_deferred: bool,
) -> Optional[str]:
    """Build comma-separated anthropic-beta header value. Returns None when no beta is needed.

    Combines interleaved-thinking-2025-05-14 (when thinking enabled) and
    advanced-tool-use-2025-11-20 (when any deferred tool is present, unless
    SHANNON_NO_ADVANCED_TOOL_USE_BETA=1 is set as an endpoint escape hatch for
    GA paths that reject the beta token).

    构造逗号分隔的 anthropic-beta header 值。无需 beta 时返回 None。

    组合 interleaved-thinking-2025-05-14（启用 thinking 时）与
    advanced-tool-use-2025-11-20（存在延迟加载 tool 时），除非设置了
    SHANNON_NO_ADVANCED_TOOL_USE_BETA=1 —— 用于拒绝该 beta token 的 GA 端点应急开关。
    """
    tokens: list[str] = []
    if thinking:
        tokens.append("interleaved-thinking-2025-05-14")
    if any_deferred and os.environ.get("SHANNON_NO_ADVANCED_TOOL_USE_BETA") != "1":
        tokens.append("advanced-tool-use-2025-11-20")
    if not tokens:
        return None
    return ",".join(tokens)


class CacheBreakDetector:
    """Tracks API request state across calls to detect cache-breaking changes.

    Compares system prompt hash, tool name set, and model ID between sequential
    calls. When a change is detected, returns a dict describing what changed.
    Designed for per-provider-instance use (one detector per AnthropicProvider).

    跨调用跟踪 API request 状态，以检测破坏 cache 的变更。

    比较相邻调用之间的 system prompt 哈希、tool 名称集合与 model ID。检测到变更时，
    返回描述变更内容的 dict。设计为每个 provider 实例一份（每个 AnthropicProvider 一个检测器）。
    """

    def __init__(self):
        self._prev_system_hash: str = ""
        self._prev_tool_names: list[str] = []
        self._prev_model: str = ""
        self.call_count: int = 0

    def check(
        self,
        system_text: str,
        tool_names: list[str],
        model: str,
    ) -> Optional[dict]:
        """Compare current state against previous. Returns None if no break.

        Returns dict with keys: changed (list[str]), tools_added, tools_removed,
        prev_model, new_model, system_char_delta.

        对比当前状态与上一次状态。无破坏时返回 None。

        返回的 dict 键包括：changed（list[str]）、tools_added、tools_removed、
        prev_model、new_model、system_char_delta。
        """
        self.call_count += 1

        sys_hash = hashlib.md5(system_text.encode()).hexdigest()[:12]
        sorted_names = sorted(tool_names)

        if not self._prev_system_hash:
            # First call — record state, no comparison possible
            self._prev_system_hash = sys_hash
            self._prev_tool_names = sorted_names
            self._prev_model = model
            return None

        changed = []
        result = {}

        if sys_hash != self._prev_system_hash:
            changed.append("system")
            result["system_char_delta"] = len(system_text)

        if sorted_names != self._prev_tool_names:
            changed.append("tools")
            prev_set = set(self._prev_tool_names)
            curr_set = set(sorted_names)
            result["tools_added"] = sorted(curr_set - prev_set)
            result["tools_removed"] = sorted(prev_set - curr_set)

        if model != self._prev_model:
            changed.append("model")
            result["prev_model"] = self._prev_model
            result["new_model"] = model

        # Update state
        self._prev_system_hash = sys_hash
        self._prev_tool_names = sorted_names
        self._prev_model = model

        if changed:
            result["changed"] = changed
            result["call_count"] = self.call_count
            return result
        return None


class AnthropicProvider(LLMProvider):
    """Anthropic Claude API provider implementation

    Anthropic Claude API 的 provider 实现。
    """

    def __init__(self, config: Dict[str, Any]):
        # Initialize Anthropic client
        # 初始化 Anthropic 客户端
        api_key = config.get("api_key") or os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise ValueError("Anthropic API key not provided")

        # Support custom base_url for Anthropic-compatible providers (e.g. MiniMax)
        # 支持为 Anthropic 兼容 provider（如 MiniMax）自定义 base_url
        base_url = config.get("base_url")
        self.client = AsyncAnthropic(api_key=api_key, base_url=base_url)

        # Tool schema freeze: cache converted tool schemas by name set to prevent
        # description-drift cache breaks. Legitimate schema changes take effect on
        # process restart (acceptable for Docker deployment model).
        # tool schema 冻结：按名称集合 cache 已转换的 tool schema，避免描述漂移破坏 cache。
        # 合法的 schema 变更会在进程重启后生效（对 Docker 部署模型可以接受）。
        self._frozen_tools: list = []
        self._frozen_tools_key: str = ""
        self._cache_break_detector = CacheBreakDetector()

        super().__init__(config)

    def _initialize_models(self):
        """Load Anthropic model configurations from YAML-driven config.

        从 YAML 驱动的配置加载 Anthropic 模型配置。
        """

        self._load_models_from_config()

    def count_tokens(self, messages: List[Dict[str, Any]], model: str) -> int:
        """
        Count tokens for Claude models.
        Note: Anthropic doesn't provide a public tokenizer, so we estimate.

        统计 Claude 模型的 token 数。
        注意：Anthropic 未提供公开分词器，因此这里采用估算。
        """
        # Use the base token counter for estimation
        return TokenCounter.count_messages_tokens(messages, model)

    def _convert_messages_to_claude_format(
        self,
        messages: List[Dict[str, Any]],
        ttl_block: Optional[Dict[str, str]] = CACHE_TTL_LONG,
    ) -> tuple[str, List[Dict]]:
        """Convert OpenAI-style messages to Claude format.

        ttl_block: the cache_control dict to apply at cache_break positions and
        the rolling [-2] marker. Explicit ``None`` suppresses prompt-cache writes
        entirely (used when ``SHANNON_FORCE_TTL=off``). Default ``CACHE_TTL_LONG``
        preserves legacy behavior for callers that invoke this helper directly
        (e.g. unit tests) without going through ``_build_api_request``.

        将 OpenAI 风格消息转换为 Claude 格式。

        ttl_block：在 cache_break 位置与滚动 [-2] 标记上应用的 cache_control dict。
        显式传入 ``None`` 会完全禁止 prompt cache 写入（``SHANNON_FORCE_TTL=off`` 时
        使用）。默认 ``CACHE_TTL_LONG``，为直接调用此辅助函数（如单元测试）而不经过
        ``_build_api_request`` 的调用方保留旧行为。
        """
        system_message = ""
        claude_messages = []

        for message in messages:
            role = message["role"]
            content = message["content"]

            if role == "system":
                # Claude uses a separate system parameter (must be a string)
                # Claude 使用独立的 system 参数（必须是字符串）
                system_message = extract_text_from_content(content)
            elif role == "user":
                if isinstance(content, str) and CACHE_BREAK_MARKER in content:
                    parts = content.split(CACHE_BREAK_MARKER, 1)
                    raw_stable = parts[0]
                    volatile = parts[1]
                    if raw_stable.strip():
                        stable_block: Dict[str, Any] = {"type": "text", "text": raw_stable}
                        if ttl_block is not None:
                            stable_block["cache_control"] = ttl_block
                        claude_messages.append({
                            "role": "user",
                            "content": [
                                stable_block,
                                {"type": "text", "text": volatile},
                            ]
                        })
                    else:
                        # No stable prefix — skip cache_control to avoid Anthropic 400
                        # on empty text blocks (happens when ShanClaw has no sticky context)
                        # 无稳定前缀 —— 跳过 cache_control，避免 Anthropic 对空 text block
                        # 返回 400（ShanClaw 没有粘性 context 时会出现）
                        claude_messages.append({"role": "user", "content": volatile})
                else:
                    claude_messages.append({"role": "user", "content": content})
            elif role == "assistant":
                # Anthropic rejects assistant messages ending with whitespace
                # Anthropic 拒绝以空白结尾的 assistant 消息
                if isinstance(content, str):
                    content = content.rstrip()
                # Convert OpenAI-style tool_calls field to Anthropic tool_use content blocks
                # 将 OpenAI 风格的 tool_calls 字段转换为 Anthropic tool_use content block
                tool_calls = message.get("tool_calls")
                if tool_calls and isinstance(tool_calls, list):
                    blocks = []
                    if content:
                        text = content if isinstance(content, str) else str(content)
                        if text.strip():
                            blocks.append({"type": "text", "text": text})
                    for tc in tool_calls:
                        if not isinstance(tc, dict):
                            continue
                        # Handle OpenAI format: {"id":"...", "type":"function", "function":{"name":"...", "arguments":"..."}}
                        # and Shannon format: {"id":"...", "name":"...", "arguments":...}
                        # 处理 OpenAI 格式：{"id":"...", "type":"function", "function":{"name":"...", "arguments":"..."}}
                        # 以及 Shannon 格式：{"id":"...", "name":"...", "arguments":...}
                        fn = tc.get("function", {}) if tc.get("type") == "function" else tc
                        tc_id = _ensure_tool_id(tc.get("id", ""))
                        name = fn.get("name", "") if isinstance(fn, dict) else tc.get("name", "")
                        arguments = fn.get("arguments", {}) if isinstance(fn, dict) else tc.get("arguments", {})
                        if isinstance(arguments, str):
                            try:
                                arguments = json.loads(arguments)
                            except (json.JSONDecodeError, TypeError):
                                arguments = {}
                        blocks.append({
                            "type": "tool_use",
                            "id": tc_id,
                            "name": name,
                            "input": arguments,
                        })
                    claude_messages.append({"role": "assistant", "content": blocks if blocks else content})
                else:
                    claude_messages.append({"role": "assistant", "content": content})
            elif role == "tool":
                # Convert OpenAI-style tool result to Anthropic tool_result content block.
                # Content may be a string (legacy plain-text tool results) or a list of
                # content blocks (e.g. tool_reference blocks from a tool_search call).
                # Preserve list shape so Anthropic sees the structured blocks; stringify
                # only when something truly non-list/non-string slips through (defensive).
                # 将 OpenAI 风格的 tool 结果转换为 Anthropic tool_result content block。
                # content 可能是字符串（遗留的纯文本 tool 结果），也可能是内容 block 列表
                # （如 tool_search 调用产生的 tool_reference block）。保留列表形状，让
                # Anthropic 看到结构化 block；仅在真正非列表/非字符串的内容溜进来时才
                # 转为字符串（防御性处理）。
                if isinstance(content, list):
                    inner_content = content
                elif isinstance(content, str):
                    inner_content = content
                else:
                    inner_content = str(content or "")
                tool_result_block = {
                    "type": "tool_result",
                    "tool_use_id": _ensure_tool_id(message.get("tool_call_id", "")),
                    "content": inner_content,
                }
                # Merge into previous user message to maintain Anthropic's alternating role requirement
                # 合并进上一条 user 消息，以满足 Anthropic 的角色交替要求
                if claude_messages and claude_messages[-1]["role"] == "user":
                    prev_content = claude_messages[-1]["content"]
                    if isinstance(prev_content, list):
                        prev_content.append(tool_result_block)
                    elif isinstance(prev_content, str):
                        claude_messages[-1]["content"] = [
                            {"type": "text", "text": prev_content},
                            tool_result_block,
                        ]
                    else:
                        claude_messages[-1]["content"] = [tool_result_block]
                else:
                    claude_messages.append({
                        "role": "user",
                        "content": [tool_result_block],
                    })
            elif role == "function":
                # Convert function results to user messages
                # 将 function 结果转换为 user 消息
                claude_messages.append(
                    {"role": "user", "content": f"Function result: {content}"}
                )

        # Basic rolling marker on [-2] (no prev-turn preservation here because
        # session_id isn't available at this layer). _build_api_request calls
        # _apply_rolling_cache_marker afterwards to add prev preservation when
        # a session_id is available; that call is idempotent on this marker.
        # 在 [-2] 上做基础滚动标记（此处不做上一轮保留，因为这一层拿不到 session_id）。
        # _build_api_request 之后会调用 _apply_rolling_cache_marker，在有 session_id 时
        # 增加上一轮保留；该调用对本标记是幂等的。
        if len(claude_messages) >= 2 and ttl_block is not None:
            self._mark_last_block(claude_messages[-2], ttl_block)
        return system_message, claude_messages

    @staticmethod
    def _mark_last_block(msg: Dict[str, Any], ttl_block: Dict[str, str]) -> bool:
        """Add cache_control to the last block of msg. Returns True if applied.
        No-op if any block already has cache_control (de-dup).

        给 msg 的最后一个 block 加上 cache_control。成功应用返回 True。
        若已有任一 block 带 cache_control 则为空操作（去重）。
        """
        content = msg.get("content")
        if isinstance(content, str):
            msg["content"] = [{"type": "text", "text": content, "cache_control": ttl_block}]
            return True
        if isinstance(content, list) and content:
            already = any(isinstance(b, dict) and b.get("cache_control") for b in content)
            if already:
                return False
            last_block = content[-1]
            if isinstance(last_block, dict):
                last_block["cache_control"] = ttl_block
                return True
        return False

    @staticmethod
    def _strip_message_cache_control(msg: Dict[str, Any]) -> None:
        """Remove cache_control from all blocks in msg (used to free a breakpoint
        slot when promoting prev_rolling marker — user_1's bytes remain readable
        as part of prev_rolling's larger cached prefix, no info loss).

        移除 msg 所有 block 上的 cache_control（用于在提升 prev_rolling 标记时
        腾出断点槽位 —— user_1 的字节仍可作为 prev_rolling 更大 cache 前缀的一部分读取，
        无信息损失）。
        """
        content = msg.get("content")
        if isinstance(content, list):
            for block in content:
                if isinstance(block, dict):
                    block.pop("cache_control", None)

    def _apply_rolling_cache_marker(
        self,
        claude_messages: List[Dict[str, Any]],
        session_id: Optional[str],
        ttl_block: Optional[Dict[str, str]] = CACHE_TTL_LONG,
    ) -> None:
        """Apply rolling cache_control marker on penultimate message; when a
        previous turn's marker is still present in messages, also preserve it.

        Why preserve prev marker: Anthropic prefix cache only matches at positions
        with explicit cache_control. Without preserving prev marker, every turn
        rewrites a fresh rolling block while the previous rolling cache becomes
        unreachable. Long-session bench observed CHR 86%->68%, CER 16.85x->2.35x.

        Breakpoint accounting (Anthropic cap = 4):
        - Always: system (1) + tools (1) = 2 reserved
        - Without prev_rolling: user_1 (cache_break) + rolling@-2 = 4 ✓
        - With prev_rolling preserved: prev_rolling + rolling@-2 = 4
          (user_1's cache_control stripped — its bytes are inside prev_rolling's
          cached prefix, readable for free without an extra breakpoint)

        在倒数第二条消息上应用滚动 cache_control 标记；若上一轮的标记仍在消息中，也一并保留。

        为何保留上一轮标记：Anthropic 前缀 cache 只在显式设置 cache_control 的位置匹配。
        若不保留，每一轮都会重写新的滚动 block，而上一轮的滚动 cache 变得不可达。长 session
        基准观测到 CHR 从 86% 降到 68%，CER 从 16.85 倍降到 2.35 倍。

        断点核算（Anthropic 上限 = 4）：
        - 固定：system（1）+ tools（1）= 预留 2 个
        - 无 prev_rolling：user_1（cache_break）+ rolling@-2 = 4 ✓
        - 保留 prev_rolling：prev_rolling + rolling@-2 = 4
          （user_1 的 cache_control 被移除 —— 其字节已包含在 prev_rolling 的 cache 前缀内，
          无需额外断点即可免费读取）
        """
        if ttl_block is None:
            return
        if len(claude_messages) < 2:
            return

        target_idx = len(claude_messages) - 2
        target = claude_messages[target_idx]

        # Locate prev marker by hash (None if first turn or compaction event).
        # 按哈希定位上一轮标记（第一轮或发生压缩时为 None）。
        prev_idx: Optional[int] = None
        if session_id:
            prev_hash = _recall_marker(session_id)
            if prev_hash:
                for i, msg in enumerate(claude_messages[:target_idx]):
                    if _msg_stable_hash(msg) == prev_hash:
                        prev_idx = i
                        break

        # Apply current rolling marker on [-2] (existing behavior).
        # 在 [-2] 上应用当前滚动标记（既有行为）。
        self._mark_last_block(target, ttl_block)

        # Preserve prev marker: free a breakpoint by stripping earlier
        # cache_control (user_1), then mark the prev message.
        # 保留上一轮标记：先移除更早的 cache_control（user_1）腾出断点，再标记上一轮消息。
        if prev_idx is not None and prev_idx < target_idx:
            for i in range(prev_idx):
                self._strip_message_cache_control(claude_messages[i])
            self._mark_last_block(claude_messages[prev_idx], ttl_block)
            logger.debug(
                "rolling cache: preserved prev marker at idx=%d (target=%d, session=%s)",
                prev_idx, target_idx, (session_id or "")[:12],
            )

        # Memo current target hash for next turn's lookup.
        # 记住当前目标的哈希，供下一轮查找。
        if session_id:
            _remember_marker(session_id, _msg_stable_hash(target))

    def _split_system_message(
        self,
        system_message: str,
        ttl_block: Optional[Dict[str, str]] = CACHE_TTL_LONG,
    ) -> list[dict]:
        """Split system message at <!-- volatile --> marker.

        Returns a list of content blocks for the Anthropic API 'system' parameter.
        The stable prefix (before marker) gets cache_control if ttl_block is
        provided; volatile suffix never does. If no marker is present, returns
        a single (cached iff ttl_block is non-None) block.

        Monotonic TTL ordering across breakpoints is preserved because all
        cache_control blocks in a single request share the same ttl_block.

        在 <!-- volatile --> 标记处切分 system 消息。

        返回用于 Anthropic API 'system' 参数的内容 block 列表。若提供了 ttl_block，
        稳定前缀（标记之前）会带 cache_control；易变后缀永远不带。若无标记，则返回
        单个 block（ttl_block 非 None 时才带 cache）。

        由于单个 request 内所有 cache_control block 共用同一个 ttl_block，跨断点的 TTL
        单调顺序得以保持。
        """
        if VOLATILE_MARKER in system_message:
            stable, volatile = system_message.split(VOLATILE_MARKER, 1)
            blocks: list[dict] = []
            if stable.strip():
                stable_block: Dict[str, Any] = {"type": "text", "text": stable.strip()}
                if ttl_block is not None:
                    stable_block["cache_control"] = ttl_block
                blocks.append(stable_block)
            if volatile.strip():
                blocks.append({"type": "text", "text": volatile.strip()})
            if blocks:
                return blocks
        full_block: Dict[str, Any] = {"type": "text", "text": system_message}
        if ttl_block is not None:
            full_block["cache_control"] = ttl_block
        return [full_block]

    def _convert_functions_to_tools(self, functions: List[Dict]) -> List[Dict]:
        """Convert OpenAI function format to Claude tools format.

        Frozen by tool name set: same names → return cached schemas (prevents
        description-drift cache breaks). Different name set → rebuild.

        将 OpenAI function 格式转换为 Claude tools 格式。

        按 tool 名集合冻结：名称集合相同 → 返回已 cache 的 schema（避免描述漂移破坏 cache）。
        名称集合不同 → 重建。
        """
        # Cache key: sorted (name, defer_loading) tuples. Including defer flag in the
        # key ensures the frozen cache rebuilds when a tool toggles between deferred
        # and non-deferred modes across sessions.
        # cache key：排序后的 (name, defer_loading) 元组。把延迟加载标志纳入 key，
        # 可在 tool 跨 session 切换延迟/非延迟模式时触发冻结 cache 重建。
        key = str(sorted(
            (
                (f.get("name") or (f.get("function") or {}).get("name", "") or ""),
                bool(
                    f.get("defer_loading")
                    or (f.get("function") or {}).get("defer_loading")
                ),
            )
            for f in functions
        ))

        if self._frozen_tools and self._frozen_tools_key == key:
            return [dict(t) for t in self._frozen_tools]

        tools = []
        for func in functions:
            # Handle both OpenAI format ({"type": "function", "function": {...}})
            # and direct function schema format ({"name": "...", ...})
            # 同时处理 OpenAI 格式（{"type": "function", "function": {...}}）
            # 与直接的 function schema 格式（{"name": "...", ...}）
            if func.get("type") == "function" and "function" in func:
                func = func["function"]

            # Skip if function schema doesn't have required 'name' field
            # 若 function schema 缺少必需的 'name' 字段则跳过
            if "name" not in func:
                continue

            tool = {
                "name": func["name"],
                "description": func.get("description", ""),
                "input_schema": {
                    "type": "object",
                    "properties": func.get("parameters", {}).get("properties", {}),
                    "required": func.get("parameters", {}).get("required", []),
                },
            }
            # Task 3.1: Anthropic strips defer_loading:true tools from the prefix-hash
            # before caching, so they don't invalidate the tools cache_control breakpoint
            # even as the deferred set grows across sessions. Omit the field entirely
            # when False so absence == disabled (avoids wire-format drift).
            # Task 3.1：Anthropic 在 cache 前会把 defer_loading:true 的 tool 从前缀哈希中剔除，
            # 因此即使延迟加载集合跨 session 增长，也不会使 tools 的 cache_control 断点失效。
            # 为 False 时完全省略该字段，使"字段缺失"等同于"未启用"（避免线上格式漂移）。
            if func.get("defer_loading") is True:
                tool["defer_loading"] = True
            tools.append(tool)
        # Sort by name for cache prefix stability across requests
        # 按名称排序，保证跨 request 的 cache 前缀稳定
        tools.sort(key=lambda t: t["name"])

        self._frozen_tools = tools
        self._frozen_tools_key = key
        return [dict(t) for t in tools]

    def reset_tool_cache(self):
        """Clear frozen tool schemas. Call on explicit tool set change.

        清空冻结的 tool schema。在 tool 集合显式变更时调用。
        """
        self._frozen_tools = []
        self._frozen_tools_key = ""



    def _convert_attachments_for_anthropic(self, messages: List[Dict]) -> List[Dict]:
        """Convert shannon_attachment blocks in user messages to Anthropic-native format.

        shannon_attachment is a Shannon-internal marker that must never be sent raw
        to any LLM API. This converts them to Anthropic image/document blocks.

        将 user 消息中的 shannon_attachment block 转换为 Anthropic 原生格式。

        shannon_attachment 是 Shannon 内部标记，绝不能原样发送给任何 LLM API。
        此处将其转换为 Anthropic 的 image/document block。
        """
        for msg in messages:
            content = msg.get("content")
            if isinstance(content, list):
                msg["content"] = [
                    self._shannon_att_to_anthropic(b)
                    if isinstance(b, dict) and b.get("type") == "shannon_attachment"
                    else b
                    for b in content
                ]
        return messages

    @staticmethod
    def _shannon_att_to_anthropic(block: Dict) -> Dict:
        """Convert a single shannon_attachment block to Anthropic format.

        将单个 shannon_attachment block 转换为 Anthropic 格式。
        """
        media_type = block["media_type"]
        # URL-based: route by media type
        # 基于 URL：按媒体类型分流
        if block.get("source") == "url":
            if media_type == "application/pdf":
                # Anthropic doesn't support URL-based PDF documents; degrade to text
                # Anthropic 不支持基于 URL 的 PDF 文档；降级为文本
                return {"type": "text", "text": f"[PDF document: {block['url']}]"}
            return {
                "type": "image",
                "source": {"type": "url", "url": block["url"]},
            }
        if media_type == "application/pdf":
            return {
                "type": "document",
                "source": {
                    "type": "base64",
                    "media_type": media_type,
                    "data": block["data"],
                },
            }
        if not media_type.startswith("image/"):
            # Unsupported binary: degrade to text
            # 不支持的二进制类型：降级为文本
            fname = block.get("filename", "file")
            return {"type": "text", "text": f"[Unsupported attachment: {fname} ({media_type})]"}
        return {
            "type": "image",
            "source": {
                "type": "base64",
                "media_type": media_type,
                "data": block["data"],
            },
        }

    def _build_api_request(self, request: CompletionRequest, model_config) -> Dict[str, Any]:
        """Build the Anthropic API request dict shared by complete() and stream_complete().

        Handles: message conversion, context window headroom, temperature/top_p
        mutual exclusivity, system message, stop sequences, tools, and output_config.

        构造 complete() 与 stream_complete() 共用的 Anthropic API request dict。

        处理：消息转换、context 窗口余量、temperature/top_p 互斥、system 消息、
        停止序列、tools 以及 output_config。
        """
        model = model_config.model_id

        # Resolve prompt-cache TTL once for the whole request. All cache_control
        # blocks below share this single value to keep Anthropic's monotonic
        # TTL-ordering invariant (system >= tools >= messages) trivially satisfied.
        # 整个 request 只解析一次 prompt cache TTL。下方所有 cache_control block 共用该值，
        # 从而轻松满足 Anthropic 的 TTL 单调顺序不变量（system >= tools >= messages）。
        ttl_block = _ttl_block(request)

        # Convert messages to Claude format
        # 将消息转换为 Claude 格式
        system_message, claude_messages = self._convert_messages_to_claude_format(
            request.messages, ttl_block
        )

        # Convert shannon_attachment blocks to Anthropic-native format
        # 将 shannon_attachment block 转换为 Anthropic 原生格式
        claude_messages = self._convert_attachments_for_anthropic(claude_messages)

        # Rolling cache_control marker on [-2] is applied inside
        # _convert_messages_to_claude_format. Cross-turn prev_marker
        # preservation (_apply_rolling_cache_marker) is intentionally NOT
        # called here. Empirically (bench 2026-04-15 Phase 3 attempt):
        # re-enabling it regressed 30-turn CHR from 93% → 61% and CER
        # 15.6x → 4.0x, tripling uncached input tokens. Root cause: the
        # preservation path calls _strip_message_cache_control on user_1 to
        # free a breakpoint slot, but stripping cache_control mutates the
        # block's byte representation on the wire. Even though the
        # non-cache_control content is identical, Anthropic's prefix match
        # appears to break at that boundary — so the "free" cached prefix
        # up to msg[prev_idx] no longer matches, and every turn falls back
        # to writing fresh cache. Single rolling marker on [-2] (applied by
        # _convert_messages_to_claude_format already) gives us the observed
        # 93% CHR / 15.6x CER on 30-turn sessions, which is optimal under
        # the public API's 4-breakpoint cap. To revive prev_marker safely,
        # we'd need either an Anthropic API change (treat cache_control as
        # cache-key-insensitive) or a byte-rewriting scheme that preserves
        # exact bytes while freeing a slot — neither is available today.

        # Compute safe max_tokens based on context window headroom (OpenAI-style)
        # 基于 context window 余量计算安全的 max_tokens（OpenAI 风格）
        prompt_tokens_est = self.count_tokens(request.messages, model)
        safety_margin = 256
        model_context = getattr(model_config, "context_window", 200000)
        model_max_output = getattr(model_config, "max_tokens", 8192)
        requested_max = int(request.max_tokens) if request.max_tokens else model_max_output
        headroom = model_context - prompt_tokens_est - safety_margin

        # Check if there's sufficient context window headroom
        # 检查 context window 余量是否足够
        if headroom <= 0:
            raise ValueError(
                f"Insufficient context window: prompt uses ~{prompt_tokens_est} tokens, "
                f"max context is {model_context}, leaving no room for output. "
                f"Please reduce prompt length."
            )

        adjusted_max = min(requested_max, model_max_output, headroom)

        # SDK 0.64.0+ rejects non-streaming requests that may take >10 minutes.
        # Cap max_tokens to avoid this for non-streaming complete() calls.
        # Haiku ~800 tok/s → 8192 tokens ≈ 10s. 16384 is safe for all models.
        # SDK 0.64.0+ 会拒绝可能耗时超过 10 分钟的非流式 request。
        # 对非流式 complete() 调用限制 max_tokens 以避免触发。
        # Haiku 约 800 tok/s → 8192 token 约 10 秒。16384 对所有模型都安全。
        MAX_NON_STREAMING = 16384
        if adjusted_max > MAX_NON_STREAMING and not request.stream:
            logger.info(f"Capping max_tokens {adjusted_max} → {MAX_NON_STREAMING} for non-streaming Anthropic call")
            adjusted_max = MAX_NON_STREAMING

        api_request: Dict[str, Any] = {
            "model": model,
            "messages": claude_messages,
            "max_tokens": adjusted_max,
        }

        # Anthropic API requires temperature and top_p to be mutually exclusive.
        # Note: `0.0` is a valid temperature; do not use truthiness checks here.
        # Anthropic API 要求 temperature 与 top_p 互斥。
        # 注意：`0.0` 是合法 temperature；此处不能用真值判断。
        if request.temperature is not None and request.top_p is not None:
            # Prefer temperature when both are present.
            # 两者都存在时优先使用 temperature。
            api_request["temperature"] = request.temperature
            logger.warning(
                "Anthropic API: both temperature and top_p were set; "
                "using temperature and ignoring top_p"
            )
        elif request.temperature is not None:
            api_request["temperature"] = request.temperature
        elif request.top_p is not None:
            api_request["top_p"] = request.top_p
        # If neither is set, omit both and let the API defaults apply.
        # 两者都未设置时全部省略，使用 API 默认值。

        if system_message:
            api_request["system"] = self._split_system_message(system_message, ttl_block)

        if request.stop:
            api_request["stop_sequences"] = request.stop

        # Handle functions/tools
        # 处理 functions/tools
        if request.functions and model_config.supports_functions:
            tools = self._convert_functions_to_tools(request.functions)
            if tools and ttl_block is not None:
                tools[-1]["cache_control"] = ttl_block
            api_request["tools"] = tools

            # Handle function calling / tool_choice
            # 处理 function calling / tool_choice
            if request.function_call:
                if isinstance(request.function_call, str):
                    if request.function_call == "auto":
                        api_request["tool_choice"] = {"type": "auto"}
                    elif request.function_call == "any":
                        # Force model to use at least one tool
                        # 强制模型至少使用一个 tool
                        api_request["tool_choice"] = {"type": "any"}
                    elif request.function_call == "none":
                        api_request["tool_choice"] = {"type": "none"}
                elif isinstance(request.function_call, dict):
                    api_request["tool_choice"] = {
                        "type": "tool",
                        "name": request.function_call.get("name"),
                    }

        # NOTE: Automatic caching (top-level cache_control) tested extensively but
        # NET NEGATIVE for swarm: 25% write premium on every call, but parallel agents
        # constantly change shared state (team roster, task board, workspace) → prefix
        # breaks between nearly every call → ~7% hit rate → net -17% cost increase.
        # Explicit system message cache_control (line 190) is sufficient: Sonnet/Lead
        # (>1024 threshold, ~8K stable system prompt) benefits; Haiku agents (<4096
        # threshold) silently skip without paying write premium.
        # 注意：自动 cache（顶层 cache_control）经过大量测试，但对 swarm 为净负收益：
        # 每次调用都有 25% 写入溢价，而并行 agent 不断改变共享状态（团队名册、task board、
        # 工作区）→ 几乎每次调用之间前缀都会失效 → 命中率约 7% → 净成本增加 17%。
        # 显式的 system 消息 cache_control（第 190 行）已足够：Sonnet/Lead
        # （阈值 >1024，约 8K 稳定 system prompt）可获益；Haiku agent（阈值 <4096）
        # 会静默跳过，无需支付写入溢价。

        # Structured outputs: inject output_config for constrained JSON decoding.
        # SDK <0.42 doesn't have native output_config param; pass via extra_body.
        # 结构化输出：注入 output_config 以进行受限 JSON 解码。
        # SDK <0.42 没有原生 output_config 参数；通过 extra_body 传递。
        if request.output_config:
            api_request["extra_body"] = {"output_config": request.output_config}
            schema_keys = list(request.output_config.get("format", {}).get("schema", {}).get("properties", {}).keys())
            logger.info(f"Anthropic structured output enabled: schema keys={schema_keys}")

        # Extended thinking: force temperature=1 and pass config via extra_body
        # (SDK 0.40.0 doesn't accept 'thinking' as a named kwarg in stream())
        # 扩展思考：强制 temperature=1，并通过 extra_body 传递配置
        # （SDK 0.40.0 的 stream() 不接受 'thinking' 作为具名 kwarg）
        if request.thinking:
            thinking_config = dict(request.thinking)
            # Adaptive thinking only supported on Opus models.
            # For other models, convert adaptive → enabled with a budget.
            # 自适应思考仅 Opus 模型支持。
            # 对其他模型，将 adaptive 转为带 budget 的 enabled。
            if thinking_config.get("type") == "adaptive" and "opus" not in model.lower():
                thinking_config = {"type": "enabled", "budget_tokens": thinking_config.get("budget_tokens", 10000)}
                logger.info(f"Converted adaptive thinking to enabled (budget=10000) for model {model}")
            api_request["temperature"] = 1
            api_request.pop("top_p", None)
            extra = api_request.get("extra_body", {})
            extra["thinking"] = thinking_config
            api_request["extra_body"] = extra

        # Cache break detection
        # cache 破坏检测
        system_text = system_message or ""
        tool_names = [t["name"] for t in api_request.get("tools", [])]
        break_info = self._cache_break_detector.check(
            system_text=system_text,
            tool_names=tool_names,
            model=model,
        )
        if break_info:
            parts = [f"Cache break detected (call #{break_info['call_count']}): changed={break_info['changed']}"]
            if "tools_added" in break_info:
                parts.append(f"tools_added={break_info['tools_added']}")
            if "tools_removed" in break_info:
                parts.append(f"tools_removed={break_info['tools_removed']}")
            if "model" in break_info.get("changed", []):
                parts.append(f"model={break_info.get('prev_model', '')}→{break_info.get('new_model', '')}")
            logger.warning(" ".join(parts))

        # Defensive: upstream callers (agent.py agent loop, history replay)
        # inject cache_control with a hardcoded TTL that may not match the
        # one resolved for this request's cache_source. Anthropic requires
        # TTLs to be monotonic non-increasing across tools → system → messages.
        # Force every existing cache_control to the single resolved ttl_block
        # (or strip them entirely when SHANNON_FORCE_TTL=off).
        # 防御性处理：上游调用方（agent.py 的 agent loop、历史回放）注入的 cache_control
        # 使用硬编码 TTL，可能与本次 request cache_source 解析出的 TTL 不一致。Anthropic 要求
        # TTL 在 tools → system → messages 之间单调不增。此处强制把所有已有 cache_control
        # 统一为解析出的 ttl_block（SHANNON_FORCE_TTL=off 时全部移除）。
        self._force_uniform_cache_ttl(api_request, ttl_block)

        return api_request

    @staticmethod
    def _force_uniform_cache_ttl(api_request: Dict[str, Any], ttl_block: Optional[Dict[str, str]]) -> None:
        """Boundary invariant: enforce a single uniform TTL on every cache_control
        block before the request leaves the provider.

        Behavior — note this IS a semantic override, not a merge:
          - ttl_block is a dict: every existing cache_control value is REPLACED
            with ttl_block, including any values an upstream caller explicitly
            set. This is intentional: Anthropic requires TTL monotonic-non-
            increasing across tools → system → messages, and the current design
            contract is "single resolved TTL per request" so that the invariant
            is trivially satisfied at every breakpoint. Heterogeneous TTL
            layouts (e.g. 1h prefix + 5m tail) are not a supported path today;
            if a future design requires them, this guard must be revisited.
          - ttl_block is None (SHANNON_FORCE_TTL=off): every cache_control is
            stripped so no prompt-cache writes happen.

        Emits a debug log of how many blocks were modified. Persistent non-zero
        counts for internal cache_sources signal an upstream caller (e.g. the
        hardcoded CACHE_TTL_LONG injections in agent.py) whose TTL disagrees
        with this request's resolved ttl_block — file a follow-up.

        边界不变量：在 request 离开 provider 之前，强制所有 cache_control block 使用统一 TTL。

        行为 —— 注意这是语义覆盖，而非合并：
          - ttl_block 为 dict：所有已有 cache_control 值都会被 REPLACE 为 ttl_block，
            包括上游调用方显式设置的值。这是有意为之：Anthropic 要求 TTL 在
            tools → system → messages 之间单调不增，而当前设计契约是"每个 request 单一
            解析 TTL"，从而在每个断点上轻松满足该不变量。当前不支持异构 TTL 布局
            （如 1h 前缀 + 5m 尾部）；若未来设计需要，必须重新审视此保护逻辑。
          - ttl_block 为 None（SHANNON_FORCE_TTL=off）：移除所有 cache_control，
            不发生任何 prompt cache 写入。

        会输出一条 debug 日志说明修改了多少个 block。若内部 cache_source 的计数
        持续非零，说明上游调用方（如 agent.py 中硬编码的 CACHE_TTL_LONG 注入）的
        TTL 与本 request 解析出的 ttl_block 不一致 —— 需要跟进处理。
        """
        modified = 0

        def _visit(container: Any) -> None:
            nonlocal modified
            if isinstance(container, dict):
                # cache_control always sits directly on content-block dicts
                # (tools, system blocks, message content blocks); there's no
                # need to recurse into other dict values for Anthropic's schema.
                if "cache_control" in container:
                    if ttl_block is None:
                        container.pop("cache_control", None)
                        modified += 1
                    elif container["cache_control"] != ttl_block:
                        container["cache_control"] = ttl_block
                        modified += 1
            elif isinstance(container, list):
                for item in container:
                    _visit(item)

        for t in api_request.get("tools", []) or []:
            _visit(t)
        _visit(api_request.get("system"))
        for m in api_request.get("messages", []) or []:
            _visit(m.get("content"))

        if modified:
            logger.debug(
                "cache_control uniform-TTL guard modified %d block(s) (ttl_block=%r)",
                modified, ttl_block,
            )

    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Generate a completion using Anthropic API

        使用 Anthropic API 生成 completion。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择模型
        model_config = self.resolve_model_config(request)
        model = model_config.model_id

        api_request = self._build_api_request(request, model_config)

        # Make API call
        # 发起 API 调用
        start_time = time.time()

        try:
            create_kwargs = dict(api_request)
            beta = _build_beta_header(
                thinking=bool(request.thinking),
                any_deferred=any(t.get("defer_loading") for t in api_request.get("tools", [])),
            )
            if beta:
                create_kwargs["extra_headers"] = {"anthropic-beta": beta}
            response = await self.client.messages.create(**create_kwargs)
        except anthropic.APIError as e:
            raise Exception(f"Anthropic API error: {e}")

        latency_ms = int((time.time() - start_time) * 1000)

        # Extract response content
        # 提取 response 内容
        content = ""
        function_calls = []
        # Verbatim ordered copy of the assistant message's content blocks.
        # Preserved in original index order so Anthropic's "thinking blocks
        # must be unmodified across the assistant trajectory" requirement
        # is satisfied — including interleaved thinking that appears between
        # consecutive tool_use blocks.
        # assistant 消息 content block 的逐字有序副本。
        # 按原始索引顺序保留，以满足 Anthropic "thinking block 必须在 assistant
        # 轨迹中原样保留"的要求 —— 包括出现在相邻 tool_use block 之间的交错思考。
        content_blocks_out: List[Dict[str, Any]] = []

        for content_block in response.content:
            # Handle both Anthropic SDK objects (.type attr) and plain dicts (MiniMax compat)
            # 同时处理 Anthropic SDK 对象（.type 属性）与纯 dict（MiniMax 兼容）
            block_type = getattr(content_block, "type", None) or (content_block.get("type") if isinstance(content_block, dict) else None)

            # Verbatim copy: try the SDK's own serializer first (preserves
            # forward-compat fields like new "redacted_thinking" subtypes
            # without us needing to know about them); fall back to per-type
            # extraction only when no native serializer is available.
            # 逐字复制：优先尝试 SDK 自带序列化器（可保留新的 "redacted_thinking"
            # 子类型等向前兼容字段，无需我们了解它们）；仅在无原生序列化器时
            # 才回退到按类型提取。
            block_dict = _block_to_dict(content_block, block_type)
            if block_dict is not None:
                content_blocks_out.append(block_dict)

            # Populate legacy flat fields alongside for older clients.
            # 同时填充遗留的扁平字段，供老客户端使用。
            if block_type == "text":
                content = getattr(content_block, "text", None) or (content_block.get("text", "") if isinstance(content_block, dict) else "")
            elif block_type == "tool_use":
                block_id = getattr(content_block, "id", None) or (content_block.get("id") if isinstance(content_block, dict) else None)
                block_name = getattr(content_block, "name", None) or (content_block.get("name") if isinstance(content_block, dict) else None)
                block_input = getattr(content_block, "input", None) or (content_block.get("input") if isinstance(content_block, dict) else None)
                function_calls.append({
                    "id": block_id,
                    "name": block_name,
                    "arguments": block_input,
                })

        function_call = function_calls[0] if function_calls else None

        # Get token usage — handle both SDK objects and dicts (MiniMax compat)
        # 获取 token 用量 —— 同时处理 SDK 对象与 dict（MiniMax 兼容）
        usage = response.usage
        if isinstance(usage, dict):
            output_tokens = usage.get("output_tokens", 0)
            input_tokens = usage.get("input_tokens", 0)
            cache_read = usage.get("cache_read_input_tokens", 0) or 0
            cache_creation = usage.get("cache_creation_input_tokens", 0) or 0
        else:
            output_tokens = usage.output_tokens
            input_tokens = usage.input_tokens
            cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
            cache_creation = getattr(usage, "cache_creation_input_tokens", 0) or 0
        # Extract per-TTL cache creation breakdown (1h vs 5min)
        # 提取按 TTL 划分的 cache 创建明细（1 小时 vs 5 分钟）
        cache_creation_1h = 0
        cache_creation_5m = 0
        if isinstance(usage, dict):
            cc = usage.get("cache_creation")
            if isinstance(cc, dict):
                cache_creation_1h = cc.get("ephemeral_1h_input_tokens", 0) or 0
                cache_creation_5m = cc.get("ephemeral_5m_input_tokens", 0) or 0
        else:
            cc = getattr(usage, "cache_creation", None)
            if cc is not None:
                cache_creation_1h = getattr(cc, "ephemeral_1h_input_tokens", 0) or 0
                cache_creation_5m = getattr(cc, "ephemeral_5m_input_tokens", 0) or 0

        total_tokens = input_tokens + output_tokens
        if cache_read > 0 or cache_creation > 0:
            logger.info(f"Anthropic prompt cache: read={cache_read}, creation={cache_creation} (5m={cache_creation_5m}, 1h={cache_creation_1h}), input={input_tokens}")
        logger.info(f"Anthropic complete: model={model}, structured_output={bool(request.output_config)}, input={input_tokens}, output={output_tokens}")

        # Calculate cost (including prompt cache pricing)
        # 计算成本（包含 prompt cache 计价）
        cost = self.estimate_cost(
            input_tokens, output_tokens, model,
            cache_read_tokens=cache_read, cache_creation_tokens=cache_creation,
            cache_creation_1h_tokens=cache_creation_1h,
        )

        _record_cache_metrics(
            self.config.get("name", "anthropic"),
            model,
            request.cache_source,
            cache_read,
            cache_creation,
            cache_creation_1h,
        )

        # Build response
        # 构造 response
        return CompletionResponse(
            content=content,
            model=model,
            provider=self.config.get("name", "anthropic"),
            usage=TokenUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                estimated_cost=cost,
                cache_read_tokens=cache_read,
                cache_creation_tokens=cache_creation,
                cache_creation_5m_tokens=cache_creation_5m,
                cache_creation_1h_tokens=cache_creation_1h,
                call_sequence=self._cache_break_detector.call_count,
            ),
            finish_reason=response.stop_reason or "stop",
            function_call=function_call,
            tool_calls=function_calls if function_calls else None,
            content_blocks=content_blocks_out or None,
            request_id=response.id,
            latency_ms=latency_ms,
        )

    async def stream_complete(self, request: CompletionRequest) -> AsyncIterator[str]:
        """Stream a completion using Anthropic API

        使用 Anthropic API 流式生成 completion。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择模型
        model_config = self.resolve_model_config(request)
        model = model_config.model_id

        api_request = self._build_api_request(request, model_config)

        # Make streaming API call
        # 发起流式 API 调用
        try:
            stream_kwargs = dict(api_request)
            beta = _build_beta_header(
                thinking=bool(request.thinking),
                any_deferred=any(t.get("defer_loading") for t in api_request.get("tools", [])),
            )
            if beta:
                stream_kwargs["extra_headers"] = {"anthropic-beta": beta}
            async with self.client.messages.stream(**stream_kwargs) as stream:
                async for text in stream.text_stream:
                    yield text

                # After streaming completes, get the final message with usage and tool calls
                # 流结束后获取包含用量与 tool 调用的最终消息
                final_message = await stream.get_final_message()

                # Extract tool_use AND content_blocks (parity with non-stream
                # complete(): both are required for the round-trip — function
                # calls drive tool execution, content_blocks drive the verbatim
                # assistant trajectory including thinking blocks the client
                # echoes back on the next turn).
                # 同时提取 tool_use 与 content_blocks（与非流式 complete() 保持一致：
                # 两者都是往返所必需的 —— function calls 驱动 tool 执行，content_blocks
                # 驱动逐字的 assistant 轨迹，包括客户端在下一轮回传的 thinking block）。
                function_calls = []
                content_blocks_out: List[Dict[str, Any]] = []
                if final_message and hasattr(final_message, "content"):
                    for content_block in final_message.content:
                        block_type = getattr(content_block, "type", None) or (
                            content_block.get("type") if isinstance(content_block, dict) else None
                        )
                        # Verbatim block copy — preserves order including
                        # interleaved thinking. Unknown types return None and
                        # are skipped (don't emit a malformed shape).
                        # 逐字复制 block —— 保留顺序，包括交错思考。未知类型返回 None
                        # 并被跳过（不输出畸形形状）。
                        bdict = _block_to_dict(content_block, block_type)
                        if bdict is not None:
                            content_blocks_out.append(bdict)
                        # Legacy tool_calls list for backward-compat clients.
                        # 为向后兼容的客户端保留遗留 tool_calls 列表。
                        if block_type != "tool_use":
                            continue
                        block_id = getattr(content_block, "id", None) or (
                            content_block.get("id") if isinstance(content_block, dict) else None
                        )
                        block_name = getattr(content_block, "name", None) or (
                            content_block.get("name") if isinstance(content_block, dict) else None
                        )
                        block_input = getattr(content_block, "input", None) or (
                            content_block.get("input") if isinstance(content_block, dict) else None
                        )
                        function_calls.append({
                            "id": block_id,
                            "name": block_name,
                            "arguments": block_input,
                        })

                if final_message and hasattr(final_message, "usage"):
                    # Handle both SDK objects and dicts (MiniMax / Anthropic-compat
                    # provider parity with the non-stream complete() path above).
                    # 同时处理 SDK 对象与 dict（与上方非流式 complete() 路径的
                    # MiniMax / Anthropic 兼容 provider 保持一致）。
                    usage = final_message.usage
                    if isinstance(usage, dict):
                        input_tokens = usage.get("input_tokens", 0) or 0
                        output_tokens = usage.get("output_tokens", 0) or 0
                        cache_read = usage.get("cache_read_input_tokens", 0) or 0
                        cache_creation = usage.get("cache_creation_input_tokens", 0) or 0
                        cc = usage.get("cache_creation")
                        cache_creation_1h = (
                            cc.get("ephemeral_1h_input_tokens", 0) or 0
                            if isinstance(cc, dict) else 0
                        )
                        cache_creation_5m = (
                            cc.get("ephemeral_5m_input_tokens", 0) or 0
                            if isinstance(cc, dict) else 0
                        )
                    else:
                        input_tokens = usage.input_tokens
                        output_tokens = usage.output_tokens
                        cache_read = getattr(usage, "cache_read_input_tokens", 0) or 0
                        cache_creation = getattr(usage, "cache_creation_input_tokens", 0) or 0
                        # Per-TTL cache creation breakdown (1h vs 5min). Without
                        # this the 1h TTL slice silently drops from cost accounting.
                        # 按 TTL 划分的 cache 创建明细（1 小时 vs 5 分钟）。缺少它，
                        # 1 小时 TTL 部分会从成本核算中静默丢失。
                        cache_creation_1h = 0
                        cache_creation_5m = 0
                        cc = getattr(usage, "cache_creation", None)
                        if cc is not None:
                            cache_creation_1h = getattr(cc, "ephemeral_1h_input_tokens", 0) or 0
                            cache_creation_5m = getattr(cc, "ephemeral_5m_input_tokens", 0) or 0
                    cost = self.estimate_cost(
                        input_tokens,
                        output_tokens,
                        model,
                        cache_read_tokens=cache_read,
                        cache_creation_tokens=cache_creation,
                        cache_creation_1h_tokens=cache_creation_1h,
                    )
                    _record_cache_metrics(
                        self.config.get("name", "anthropic"),
                        model,
                        request.cache_source,
                        cache_read,
                        cache_creation,
                        cache_creation_1h,
                    )
                    result = {
                        "usage": {
                            "total_tokens": input_tokens + output_tokens,
                            "input_tokens": input_tokens,
                            "output_tokens": output_tokens,
                            "cache_read_tokens": cache_read,
                            "cache_creation_tokens": cache_creation,
                            "cache_creation_5m_tokens": cache_creation_5m,
                            "cache_creation_1h_tokens": cache_creation_1h,
                            "cost_usd": cost,
                            "call_sequence": self._cache_break_detector.call_count,
                        },
                        "model": final_message.model,
                        "provider": self.config.get("name", "anthropic"),
                        "finish_reason": final_message.stop_reason or "stop",
                    }
                    if function_calls:
                        result["function_call"] = function_calls[0]
                        result["function_calls"] = function_calls
                    if content_blocks_out:
                        result["content_blocks"] = content_blocks_out
                    yield result

        except anthropic.APIError as e:
            raise Exception(f"Anthropic API error: {e}")
