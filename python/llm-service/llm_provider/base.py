"""
Base LLM Provider Abstraction Layer
Provides a unified interface for multiple LLM providers with token management,
caching, and model tiering support.

LLM Provider 抽象基类层
为多个 LLM provider 提供统一接口，支持 token 管理、cache 与 model tier 分层。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Union, AsyncIterator
from enum import Enum
import asyncio
import hashlib
import json
import re
import time
import uuid
from datetime import datetime, timedelta


class ModelTier(Enum):
    """Model tier classification for cost optimization

    model tier 分类，用于成本优化。
    """

    SMALL = "small"  # Fast, cheap models for simple tasks
    # 用于 simple task 的快速、低成本模型
    MEDIUM = "medium"  # Balanced performance/cost
    # 性能/成本均衡
    LARGE = "large"  # High-capability models for complex tasks
    # 用于 complex task 的高能力模型


@dataclass
class ModelCapabilities:
    """Capabilities for a specific model (for dynamic API selection).

    某个具体模型的能力集合（用于动态选择 API）。
    """

    supports_tools: bool = True
    supports_json_mode: bool = True
    supports_reasoning: bool = False
    supports_vision: bool = False
    supports_streaming: bool = True
    max_parallel_tools: int = 1


@dataclass
class ModelConfig:
    """Configuration for a specific model

    单个模型的配置。
    """

    provider: str
    model_id: str
    tier: ModelTier
    max_tokens: int
    context_window: int
    input_price_per_1k: float
    output_price_per_1k: float
    supports_functions: bool = True
    supports_streaming: bool = True
    supports_vision: bool = False
    timeout: int = 60
    capabilities: ModelCapabilities = None

    @property
    def full_name(self) -> str:
        return f"{self.provider}:{self.model_id}"


def compute_token_cost(
    input_per_1k: float,
    output_per_1k: float,
    input_tokens: int,
    output_tokens: int,
    cache_read_tokens: int = 0,
    cache_creation_tokens: int = 0,
    cache_creation_1h_tokens: int = 0,
    provider: str = "openai",
) -> float:
    """Pure cache-aware token cost (USD).

    Single source of truth for provider cache semantics. Mirrors Go's
    pricing.CostForSplitWithCache. Used by LLMProvider.estimate_cost and by
    tools (e.g. x_search) that compute cost without a full provider instance.

    纯函数式的、感知 cache 的 token 成本计算（美元）。

    它是各 provider cache 语义的唯一事实来源，与 Go 的
    pricing.CostForSplitWithCache 保持一致。既供 LLMProvider.estimate_cost 使用，
    也供那些没有完整 provider 实例却要计算成本的 tool（如 x_search）使用。
    """
    input_cost = (input_tokens / 1000) * input_per_1k
    output_cost = (output_tokens / 1000) * output_per_1k
    if provider in ("anthropic", "minimax"):
        # Cache tokens are separate from input_tokens.
        # cache token 与 input_tokens 分开统计。
        input_cost += (cache_read_tokens / 1000) * input_per_1k * 0.1
        cache_5m = max(0, cache_creation_tokens - cache_creation_1h_tokens)
        input_cost += (cache_5m / 1000) * input_per_1k * 1.25
        input_cost += (cache_creation_1h_tokens / 1000) * input_per_1k * 2.0
    elif provider in ("kimi", "xai") and cache_read_tokens > 0:
        # Cached tokens included in input at full price; bill at 25% (75% discount).
        # cache token 已按全价计入 input；此处按 25% 计费（即 75% 折扣）。
        input_cost -= (cache_read_tokens / 1000) * input_per_1k * 0.75
    elif cache_read_tokens > 0:
        # OpenAI default: cached tokens in input at full price, bill at 50%.
        # OpenAI 默认：cache token 已按全价计入 input，按 50% 计费。
        input_cost -= (cache_read_tokens / 1000) * input_per_1k * 0.5
    return input_cost + output_cost


@dataclass
class TokenUsage:
    """Token usage tracking

    token 用量统计。
    """

    input_tokens: int
    output_tokens: int
    total_tokens: int
    estimated_cost: float
    cache_read_tokens: int = 0
    cache_creation_tokens: int = 0
    cache_creation_5m_tokens: int = 0
    cache_creation_1h_tokens: int = 0
    call_sequence: int = 0  # Monotonic counter: which LLM call within this provider session
    # 单调递增计数器：标记本 provider session 内的第几次 LLM 调用

    def __add__(self, other: "TokenUsage") -> "TokenUsage":
        return TokenUsage(
            input_tokens=self.input_tokens + other.input_tokens,
            output_tokens=self.output_tokens + other.output_tokens,
            total_tokens=self.total_tokens + other.total_tokens,
            estimated_cost=self.estimated_cost + other.estimated_cost,
            cache_read_tokens=self.cache_read_tokens + other.cache_read_tokens,
            cache_creation_tokens=self.cache_creation_tokens + other.cache_creation_tokens,
            cache_creation_5m_tokens=self.cache_creation_5m_tokens + other.cache_creation_5m_tokens,
            cache_creation_1h_tokens=self.cache_creation_1h_tokens + other.cache_creation_1h_tokens,
            call_sequence=max(self.call_sequence, other.call_sequence),
        )


@dataclass
class CompletionRequest:
    """Unified completion request format

    统一的 completion request 格式。
    """

    messages: List[Dict[str, Any]]
    model_tier: ModelTier = ModelTier.SMALL
    model: Optional[str] = None
    temperature: float = 0.7
    max_tokens: Optional[int] = None
    top_p: Optional[float] = None  # None means API default; explicit 1.0 if needed
    # None 表示使用 API 默认值；需要时显式设为 1.0
    frequency_penalty: float = 0.0
    presence_penalty: float = 0.0
    stop: Optional[List[str]] = None
    functions: Optional[List[Dict]] = None
    function_call: Optional[Union[str, Dict]] = None
    stream: bool = False
    user: Optional[str] = None
    seed: Optional[int] = None
    response_format: Optional[Dict] = None

    # Shannon-specific parameters
    # Shannon 特有参数
    session_id: Optional[str] = None
    task_id: Optional[str] = None
    agent_id: Optional[str] = None
    cache_key: Optional[str] = None
    cache_ttl: int = 3600  # 1 hour default
    # 默认 1 小时
    max_tokens_budget: Optional[int] = None
    complexity_score: Optional[float] = (
        None  # Optional signal for dynamic API selection
        # 可选信号，用于动态选择 API
    )
    # Provider override (e.g., "openai", "anthropic"). Optional.
    # provider 覆盖（如 "openai"、"anthropic"）。可选。
    provider_override: Optional[str] = None
    # OpenAI Responses API: chain from previous response for cache reuse
    # OpenAI Responses API：基于上一个 response 串联，以复用 cache
    previous_response_id: Optional[str] = None
    # Anthropic structured outputs: output_config for constrained JSON decoding
    # Anthropic 结构化输出：output_config 用于受限 JSON 解码
    output_config: Optional[Dict] = None
    # Anthropic extended thinking config (e.g. {"type": "enabled", "budget_tokens": 5000})
    # Anthropic 扩展思考配置（例如 {"type": "enabled", "budget_tokens": 5000}）
    thinking: Optional[Dict] = None
    # OpenAI reasoning effort for o-models (minimal/low/medium/high)
    # OpenAI o 系列模型的推理强度（minimal/low/medium/high）
    reasoning_effort: Optional[str] = None
    # Prompt-cache observability label — distinguishes call sites (e.g.
    # "agent_loop", "decompose", "tool_select", "synthesis"). Unset → "unknown".
    # Prompt cache 观测标签 —— 区分不同调用点（如 "agent_loop"、"decompose"、"tool_select"、"synthesis"）。未设置时为 "unknown"。
    cache_source: Optional[str] = None

    def generate_cache_key(self) -> str:
        """Generate a cache key for this request

        为本次 request 生成 cache key。
        """
        if self.cache_key:
            return self.cache_key

        # Create deterministic hash of request parameters
        # 为 request 参数生成确定性哈希
        key_data = {
            "messages": self.messages,
            "model_tier": self.model_tier.value,
            "model": self.model,
            "temperature": self.temperature,
            "max_tokens": self.max_tokens,
            "functions": self.functions,
            "seed": self.seed,
        }
        if self.thinking:
            key_data["thinking"] = self.thinking
        if self.reasoning_effort:
            key_data["reasoning_effort"] = self.reasoning_effort

        key_json = json.dumps(key_data, sort_keys=True)
        return hashlib.sha256(key_json.encode()).hexdigest()


@dataclass
class CompletionResponse:
    """Unified completion response format

    统一的 completion response 格式。
    """

    content: str
    model: str
    provider: str
    usage: TokenUsage
    finish_reason: str
    function_call: Optional[Dict] = None
    tool_calls: Optional[List[Dict]] = None

    # Verbatim ordered copy of the assistant message's content blocks. Each
    # entry is the raw dict shape Anthropic returned (or an Anthropic-shape
    # equivalent), preserving the original block order:
    #   {"type": "thinking", "thinking": "<text>", "signature": "<base64>"}
    #   {"type": "redacted_thinking", "data": "<opaque>"}
    #   {"type": "text", "text": "<text>"}
    #   {"type": "tool_use", "id": "<id>", "name": "<name>", "input": {...}}
    #
    # Required for Anthropic's contract: thinking blocks must be preserved
    # unmodified across the assistant trajectory (assistant → tool_result →
    # next assistant). Interleaved thinking can emit thinking blocks between
    # consecutive tool_use blocks, so callers must not reorder / partition.
    # `content`, `function_call`, `tool_calls` remain populated for backward
    # compatibility with clients that pre-date this wire field.
    # assistant 消息 content blocks 的逐字有序副本。每项为 Anthropic 返回的原始 dict 形状（或等价的 Anthropic 形状），保持原始 block 顺序。
    # 这是 Anthropic 契约的要求：thinking block 必须在 assistant 轨迹（assistant → tool_result → 下一条 assistant）中原样保留。交错思考可能在相邻 tool_use block 之间产生 thinking block，因此调用方不得重排或切分。
    # `content`、`function_call`、`tool_calls` 仍会填充，以兼容早于该线上字段的客户端。
    content_blocks: Optional[List[Dict[str, Any]]] = None

    # Metadata
    # metadata
    request_id: Optional[str] = None  # Also used as response_id for OpenAI Responses API chaining
    # 同时用作 OpenAI Responses API 串联的 response_id
    created_at: datetime = None
    latency_ms: Optional[int] = None
    cached: bool = False
    effective_max_completion: Optional[int] = None  # Actual max completion after provider headroom clamp
    # 按 provider 预留空间裁剪后的实际最大 completion

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class LLMProvider(ABC):
    """Abstract base class for LLM providers

    LLM provider 的抽象基类。
    """

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.models: Dict[str, ModelConfig] = {}
        self._initialize_models()

    @abstractmethod
    def _initialize_models(self):
        """Initialize available models for this provider"""
        pass

    @abstractmethod
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Generate a completion for the given request"""
        pass

    @abstractmethod
    async def stream_complete(self, request: CompletionRequest) -> AsyncIterator[str]:
        """Stream a completion for the given request"""
        pass

    @abstractmethod
    def count_tokens(self, messages: List[Dict[str, Any]], model: str) -> int:
        """Count tokens for the given messages"""
        pass

    def select_model_for_tier(
        self, tier: ModelTier, max_tokens: Optional[int] = None
    ) -> ModelConfig:
        """Select the best model for the given tier and constraints

        在给定层级与约束下选择最合适的模型。
        """
        tier_models = [m for m in self.models.values() if m.tier == tier]

        if not tier_models:
            raise ValueError(f"No models available for tier {tier}")

        # Filter by max_tokens if specified
        # 若指定了 max_tokens 则按 context window 过滤
        if max_tokens:
            tier_models = [m for m in tier_models if m.context_window >= max_tokens]
            if not tier_models:
                raise ValueError(
                    f"No models in tier {tier} support {max_tokens} tokens"
                )

        # Return the first suitable model (could be enhanced with more logic)
        # 返回第一个合适的模型（后续可增加更复杂的逻辑）
        return tier_models[0]

    def estimate_cost(
        self,
        input_tokens: int,
        output_tokens: int,
        model: str,
        cache_read_tokens: int = 0,
        cache_creation_tokens: int = 0,
        cache_creation_1h_tokens: int = 0,
    ) -> float:
        """Estimate cost for the given token usage, including prompt cache pricing.

        Anthropic: input_tokens excludes cached tokens; cache_read at 10%, cache_creation at 125% (5min) or 200% (1h).
        OpenAI: input_tokens includes cached tokens; cache_read at 50% (discount applied here).

        估算给定 token 用量的成本，包含 prompt cache 计价。

        Anthropic：input_tokens 不含 cache token；cache_read 按 10% 计，cache_creation 按 125%（5 分钟）或 200%（1 小时）计。
        OpenAI：input_tokens 已含 cache token；cache_read 按 50% 计（折扣在此处应用）。
        """
        if model not in self.models:
            return 0.0

        model_config = self.models[model]
        return compute_token_cost(
            input_per_1k=model_config.input_price_per_1k,
            output_per_1k=model_config.output_price_per_1k,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            cache_read_tokens=cache_read_tokens,
            cache_creation_tokens=cache_creation_tokens,
            cache_creation_1h_tokens=cache_creation_1h_tokens,
            provider=model_config.provider,
        )

    def _load_models_from_config(self, allow_empty: bool = False) -> None:
        """Populate provider models from configuration dictionary.

        从配置字典填充该 provider 的模型列表。
        """

        models_cfg = self.config.get("models") or {}

        if not models_cfg:
            if allow_empty:
                return
            raise ValueError(
                f"{self.__class__.__name__} requires model definitions in configuration"
            )

        provider_name = (
            self.config.get("provider_name")
            or self.config.get("name")
            or self.config.get("type")
            or self.__class__.__name__.replace("Provider", "").lower()
        )

        for alias, raw_meta in models_cfg.items():
            meta = raw_meta or {}
            self.models[alias] = self._make_model_config(provider_name, alias, meta)

    def _make_model_config(
        self, provider_name: str, alias: str, meta: Dict[str, Any]
    ) -> ModelConfig:
        """Create a ModelConfig object from raw metadata.

        根据原始 metadata 构造 ModelConfig 对象。
        """

        tier_value = meta.get("tier", "medium")
        if isinstance(tier_value, ModelTier):
            tier_enum = tier_value
        else:
            tier_enum = ModelTier(str(tier_value).lower())

        model_id = meta.get("model_id") or alias
        context_window = int(meta.get("context_window", meta.get("max_context", 8192)))
        max_tokens = int(
            meta.get("max_tokens", meta.get("max_output_tokens", context_window))
        )

        input_price = meta.get("input_price_per_1k")
        output_price = meta.get("output_price_per_1k")

        input_price = float(input_price) if input_price is not None else 0.0
        output_price = float(output_price) if output_price is not None else 0.0

        # Build capabilities
        capabilities = ModelCapabilities(
            supports_tools=bool(meta.get("supports_functions", True)),
            supports_json_mode=bool(meta.get("supports_json_mode", True)),
            supports_reasoning=bool(meta.get("supports_reasoning", False)),
            supports_vision=bool(meta.get("supports_vision", False)),
            supports_streaming=bool(meta.get("supports_streaming", True)),
            max_parallel_tools=int(meta.get("max_parallel_tools", 1)),
        )

        return ModelConfig(
            provider=str(meta.get("provider") or provider_name),
            model_id=model_id,
            tier=tier_enum,
            max_tokens=max_tokens,
            context_window=context_window,
            input_price_per_1k=input_price,
            output_price_per_1k=output_price,
            supports_functions=bool(meta.get("supports_functions", True)),
            supports_streaming=bool(meta.get("supports_streaming", True)),
            supports_vision=bool(meta.get("supports_vision", False)),
            timeout=int(meta.get("timeout", 60)),
            capabilities=capabilities,
        )

    def resolve_model_config(self, request: CompletionRequest) -> ModelConfig:
        """Resolve the model configuration for a request, honoring overrides.

        解析 request 对应的模型配置，并尊重显式覆盖。
        """

        if request.model:
            requested = request.model

            # Allow provider:model syntax
            # 支持 provider:model 语法
            if ":" in requested:
                _, requested = requested.split(":", 1)

            # Direct key lookup (alias)
            # 直接按 key 查找（别名）
            if requested in self.models:
                return self.models[requested]

            # Match by vendor model_id
            # 按厂商 model_id 匹配
            for config in self.models.values():
                if config.model_id == request.model or config.model_id == requested:
                    return config

            raise ValueError(
                f"Model '{request.model}' not available for provider {self.__class__.__name__}"
            )

        return self.select_model_for_tier(request.model_tier, request.max_tokens)

    def _resolve_alias(self, model_id: str) -> str:
        """Return the configured alias for a given vendor model_id, if any.

        返回给定厂商 model_id 对应的已配置别名（若有）。
        """
        for alias, cfg in self.models.items():
            if cfg.model_id == model_id:
                return alias
        return model_id


class LLMProviderRegistry:
    """Registry for managing multiple LLM providers

    管理多个 LLM provider 的 registry。
    """

    def __init__(self):
        self.providers: Dict[str, LLMProvider] = {}
        self.default_provider = None
        self.tier_routing: Dict[ModelTier, List[str]] = {
            ModelTier.SMALL: [],
            ModelTier.MEDIUM: [],
            ModelTier.LARGE: [],
        }

    def register_provider(
        self, name: str, provider: LLMProvider, is_default: bool = False
    ):
        """Register a new provider

        注册一个新的 provider。
        """
        self.providers[name] = provider

        if is_default:
            self.default_provider = name

        # Update tier routing
        # 更新层级路由
        for model in provider.models.values():
            provider_model = f"{name}:{model.model_id}"
            if provider_model not in self.tier_routing[model.tier]:
                self.tier_routing[model.tier].append(provider_model)

    def get_provider(self, name: str) -> LLMProvider:
        """Get a specific provider"""
        if name not in self.providers:
            raise ValueError(f"Provider {name} not registered")
        return self.providers[name]

    def select_provider_for_request(
        self, request: CompletionRequest
    ) -> tuple[str, LLMProvider]:
        """Select the best provider for a given request

        为给定 request 选择最合适的 provider。
        """
        # Get available providers for the tier
        # 获取该层级可用的 provider
        tier_providers = self.tier_routing.get(request.model_tier, [])

        if not tier_providers:
            # Fall back to default provider
            # 回退到默认 provider
            if self.default_provider:
                return self.default_provider, self.providers[self.default_provider]
            raise ValueError(f"No providers available for tier {request.model_tier}")

        # Simple round-robin or could implement more sophisticated routing
        # For now, return the first available
        # 简单轮询，或可实现更复杂的路由；当前直接返回第一个可用项
        provider_model = tier_providers[0]
        provider_name = provider_model.split(":")[0]

        return provider_name, self.providers[provider_name]


class CacheManager:
    """Simple in-memory cache for LLM responses

    LLM response 的简单内存 cache。
    """

    def __init__(self, max_size: int = 1000):
        self.cache: Dict[str, tuple[CompletionResponse, datetime]] = {}
        self.max_size = max_size
        self.hits = 0
        self.misses = 0

    def get(self, key: str) -> Optional[CompletionResponse]:
        """Get a cached response"""
        if key in self.cache:
            response, expiry = self.cache[key]
            if datetime.utcnow() < expiry:
                self.hits += 1
                response.cached = True
                return response
            else:
                # Expired, remove from cache
                del self.cache[key]

        self.misses += 1
        return None

    def set(self, key: str, response: CompletionResponse, ttl: int = 3600):
        """Cache a response"""
        # Simple LRU: remove oldest if at capacity
        # 简单 LRU：达到容量上限时移除最旧项
        if len(self.cache) >= self.max_size:
            oldest_key = min(self.cache.keys(), key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]

        expiry = datetime.utcnow() + timedelta(seconds=ttl)
        self.cache[key] = (response, expiry)

    def delete(self, key: str) -> None:
        """Delete a cache entry if present"""
        try:
            if key in self.cache:
                del self.cache[key]
        except Exception:
            # Best-effort; ignore
            # 尽力而为；忽略失败
            pass

    @property
    def hit_rate(self) -> float:
        """Calculate cache hit rate"""
        total = self.hits + self.misses
        return self.hits / total if total > 0 else 0.0


def extract_text_from_content(content) -> str:
    """Extract text from content that may be a string or list of content blocks.

    从可能是字符串或 content block 列表的内容中提取文本。
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join(
            part.get("text", "") if isinstance(part, dict) and part.get("type") == "text"
            else part if isinstance(part, str)
            else ""
            for part in content
        )
    return str(content) if content else ""


def translate_content_for_openai(content):
    """Translate Anthropic-style content blocks to OpenAI format.

    Handles: text, image (base64 + url), image_url passthrough,
    shannon_attachment (internal marker), plain strings in lists,
    and unknown block types (passed through).

    将 Anthropic 风格的 content block 转换为 OpenAI 格式。

    处理：text、image（base64 + url）、image_url 透传、shannon_attachment（内部标记）、
    列表中的普通字符串，以及未知 block 类型（原样透传）。
    """
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content) if content else ""
    blocks = []
    for block in content:
        if isinstance(block, str):
            blocks.append({"type": "text", "text": block})
        elif not isinstance(block, dict):
            continue
        elif block.get("type") == "shannon_attachment":
            media_type = block["media_type"]
            if block.get("source") == "url":
                # URL-based: route by media type
                # 基于 URL：按媒体类型分流
                if media_type == "application/pdf":
                    # OpenAI doesn't support URL-based PDF; degrade to text
                    # OpenAI 不支持基于 URL 的 PDF；降级为文本
                    blocks.append({"type": "text", "text": f"[PDF document: {block['url']}]"})
                else:
                    blocks.append({"type": "image_url", "image_url": {"url": block["url"]}})
            elif media_type == "application/pdf":
                data_uri = f"data:{media_type};base64,{block['data']}"
                blocks.append({
                    "type": "file",
                    "file": {
                        "filename": block.get("filename", "document.pdf"),
                        "file_data": data_uri,
                    },
                })
            elif media_type.startswith("image/"):
                data_uri = f"data:{media_type};base64,{block['data']}"
                blocks.append({"type": "image_url", "image_url": {"url": data_uri}})
            else:
                # Unsupported binary type: degrade to text description
                # 不支持的二进制类型：降级为文本描述
                fname = block.get("filename", "file")
                blocks.append({"type": "text", "text": f"[Unsupported attachment: {fname} ({media_type})]"})
        elif block.get("type") == "image":
            source = block.get("source", {})
            if source.get("type") == "url":
                url = source.get("url", "")
            else:
                url = f"data:{source.get('media_type', 'image/png')};base64,{source.get('data', '')}"
            blocks.append({"type": "image_url", "image_url": {"url": url}})
        else:
            # text, image_url, and any future block types — pass through
            # text、image_url 及未来新增的 block 类型 —— 原样透传
            blocks.append(block)
    return blocks


_TOOL_CALL_PLACEHOLDER = re.compile(r'^\[tool_call:\s*\w+\]$')
_ERROR_MARKER = re.compile(r'^\[error:\s*.*\]$')
_FRIENDLY_ERROR_PREFIXES = (
    "I apologize", "I'm sorry", "Sorry, I encountered",
    "The request timed out", "Rate limit", "An error occurred",
)


def sanitize_completion_messages(messages: list) -> list:
    """Defensive sanitizer for /v1/completions message history.

    Strips malformed entries that cause provider API 400/500 errors:
    - Tool call placeholder strings ([tool_call: web_search])
    - Error marker strings ([error: agent failed to respond])
    - FriendlyAgentError boilerplate messages
    - Orphaned role:'tool' messages without preceding tool_calls/tool_use
    - Consecutive same-role messages (keeps last)

    /v1/completions 消息历史的防御性清理器。

    剔除会导致 provider API 返回 400/500 的畸形条目：
    - tool call 占位字符串（[tool_call: web_search]）
    - 错误标记字符串（[error: agent failed to respond]）
    - FriendlyAgentError 的套话消息
    - 前面没有 tool_calls/tool_use 的孤立 role:'tool' 消息
    - 连续的同角色消息（保留最后一条）
    """
    if not messages:
        return messages

    cleaned = []
    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content")

        # Skip messages with tool-call placeholder text
        # 跳过包含 tool call 占位文本的消息
        if isinstance(content, str):
            stripped = content.strip()
            if _TOOL_CALL_PLACEHOLDER.match(stripped):
                continue
            if _ERROR_MARKER.match(stripped):
                continue
            if any(stripped.startswith(p) for p in _FRIENDLY_ERROR_PREFIXES) and len(stripped) < 500:
                continue

        cleaned.append(msg)

    # Drop orphaned role:'tool' messages (no preceding tool_calls/tool_use)
    # 丢弃孤立的 role:'tool' 消息（前面没有 tool_calls/tool_use）
    result = []
    for i, msg in enumerate(cleaned):
        if msg.get("role") == "tool":
            # Look back for a valid tool-calling assistant message
            # 向前查找合法的、发起 tool call 的 assistant 消息
            has_preceding_tool_call = False
            for j in range(len(result) - 1, -1, -1):
                prev = result[j]
                if prev.get("role") == "tool":
                    continue  # skip other tool results in this batch
                    # 跳过本批次中的其他 tool result
                if prev.get("role") == "assistant":
                    if prev.get("tool_calls") or prev.get("function_call"):
                        has_preceding_tool_call = True
                    elif isinstance(prev.get("content"), list):
                        has_preceding_tool_call = any(
                            isinstance(b, dict) and b.get("type") == "tool_use"
                            for b in prev["content"]
                        )
                break
            if not has_preceding_tool_call:
                continue  # drop orphaned tool result
                # 丢弃孤立的 tool result
        result.append(msg)

    # Merge consecutive same-role messages (keep last, except tool)
    # 合并连续同角色消息（保留最后一条，tool 角色除外）
    if len(result) > 1:
        merged = [result[0]]
        for msg in result[1:]:
            if msg.get("role") == merged[-1].get("role") and msg.get("role") != "tool":
                merged[-1] = msg  # keep the later message
                # 保留较后的一条消息
            else:
                merged.append(msg)
    else:
        merged = list(result)

    # Strip orphaned tool_calls/tool_use from assistant messages whose
    # tool_result was dropped by earlier sanitization phases.
    # Both Anthropic and OpenAI reject tool_use without a matching tool_result.
    # 清除 assistant 消息中孤立的 tool_calls/tool_use：其 tool_result 已被前面的清理阶段丢弃。Anthropic 与 OpenAI 都会拒绝没有匹配 tool_result 的 tool_use。
    final = []
    for i, msg in enumerate(merged):
        if msg.get("role") != "assistant":
            final.append(msg)
            continue

        # Collect all tool call IDs from this assistant message
        # 收集本 assistant 消息中所有 tool call ID
        tool_call_ids = set()
        if msg.get("tool_calls"):
            for tc in msg["tool_calls"]:
                tc_id = tc.get("id", "")
                if tc_id:
                    tool_call_ids.add(tc_id)
        if msg.get("function_call"):
            fc_name = msg["function_call"].get("name", "")
            tool_call_ids.add(f"fc_{fc_name}" if fc_name else "__fc__")
        if isinstance(msg.get("content"), list):
            for b in msg["content"]:
                if isinstance(b, dict) and b.get("type") == "tool_use":
                    tu_id = b.get("id", "")
                    if tu_id:
                        tool_call_ids.add(tu_id)

        if not tool_call_ids:
            final.append(msg)
            continue

        # Collect all tool result IDs from the immediately following messages
        # 收集紧随其后消息中所有 tool result ID
        result_ids = set()
        for j in range(i + 1, len(merged)):
            next_msg = merged[j]
            if next_msg.get("role") == "tool":
                tc_id = next_msg.get("tool_call_id", "")
                if tc_id:
                    result_ids.add(tc_id)
                continue  # keep scanning for more tool results in this batch
                # 继续在本批次中扫描更多 tool result
            if next_msg.get("role") == "user" and isinstance(next_msg.get("content"), list):
                for b in next_msg["content"]:
                    if isinstance(b, dict) and b.get("type") == "tool_result":
                        tu_id = b.get("tool_use_id", "")
                        if tu_id:
                            result_ids.add(tu_id)
                break
            break  # next message is something else entirely
            # 下一条消息完全是其他内容

        if tool_call_ids <= result_ids:
            # All tool calls have matching results
            # 所有 tool call 都有匹配结果
            final.append(msg)
        elif result_ids:
            # Partial match: strip only the unsatisfied tool calls
            # 部分匹配：仅清除没有对应结果的 tool call
            satisfied = result_ids
            new_msg = dict(msg)
            if new_msg.get("tool_calls"):
                new_msg["tool_calls"] = [
                    tc for tc in new_msg["tool_calls"]
                    if tc.get("id", "") in satisfied
                ]
                if not new_msg["tool_calls"]:
                    del new_msg["tool_calls"]
            if new_msg.get("function_call") and f"fc_{new_msg['function_call'].get('name', '')}" not in satisfied:
                del new_msg["function_call"]
            if isinstance(new_msg.get("content"), list):
                new_msg["content"] = [
                    b for b in new_msg["content"]
                    if not (isinstance(b, dict) and b.get("type") == "tool_use"
                            and b.get("id", "") not in satisfied)
                ]
                if not new_msg["content"]:
                    new_msg["content"] = ""
            final.append(new_msg)
        else:
            # No matching results at all — strip all tool invocations, keep text
            # 完全没有匹配结果 —— 清除所有 tool call，保留文本
            stripped = {k: v for k, v in msg.items()
                        if k not in ("tool_calls", "function_call")}
            if isinstance(stripped.get("content"), list):
                stripped["content"] = [
                    b for b in stripped["content"]
                    if not (isinstance(b, dict) and b.get("type") == "tool_use")
                ]
                if all(isinstance(b, dict) and b.get("type") == "text" for b in stripped["content"]):
                    text = " ".join(b.get("text", "") for b in stripped["content"]).strip()
                    stripped["content"] = text if text else ""
                elif not stripped["content"]:
                    stripped["content"] = ""
            if not stripped.get("content") and stripped.get("content") != 0:
                continue
            final.append(stripped)

    return final


_TOOL_ID_PATTERN = re.compile(r'^[a-zA-Z0-9_-]+$')


def _ensure_tool_id(raw_id: str) -> str:
    """Ensure a tool ID is non-empty and contains only safe characters.

    Anthropic requires ^[a-zA-Z0-9_-]+$. OpenAI accepts any non-empty string.
    Returns raw_id if valid, otherwise generates a fallback.

    确保 tool ID 非空且只包含安全字符。

    Anthropic 要求满足 ^[a-zA-Z0-9_-]+$；OpenAI 接受任意非空字符串。
    合法时原样返回 raw_id，否则生成一个兜底 ID。
    """
    if raw_id and _TOOL_ID_PATTERN.match(raw_id):
        return raw_id
    if raw_id:
        # Deterministic replacement for malformed IDs (e.g. dots, colons)
        # 为畸形 ID（如含点号、冒号）生成确定性替换值
        suffix = hashlib.md5(raw_id.encode()).hexdigest()[:16]
    else:
        # Random fallback for missing/empty IDs
        # ID 缺失/为空时使用随机兜底值
        suffix = uuid.uuid4().hex[:16]
    return f"tool_{suffix}"


def _has_function_call_tool_mismatch(messages):
    """Check if messages have legacy function_call + role:'tool' that OpenAI rejects.

    OpenAI requires role:'tool' messages to follow assistant messages with
    'tool_calls' (modern format), not 'function_call' (legacy format).

    检查消息中是否同时存在旧式 function_call 与 role:'tool'（OpenAI 会拒绝这种组合）。

    OpenAI 要求 role:'tool' 消息跟在带 'tool_calls'（现代格式）的 assistant 消息之后，
    而不是带 'function_call'（旧格式）的消息之后。
    """
    has_tool_role = any(m.get("role") == "tool" for m in messages)
    has_function_call = any(
        m.get("role") == "assistant" and m.get("function_call") and not m.get("tool_calls")
        for m in messages
    )
    return has_tool_role and has_function_call


def _upgrade_function_call_to_tool_calls(msg):
    """Convert assistant message from legacy function_call to modern tool_calls format.

    Input:  {"role": "assistant", "content": "...", "function_call": {"name": "...", "arguments": "..."}}
    Output: {"role": "assistant", "content": "...", "tool_calls": [{"id": "...", "type": "function", "function": {...}}]}

    将 assistant 消息从旧式 function_call 转换为现代 tool_calls 格式。

    输入： {"role": "assistant", "content": "...", "function_call": {"name": "...", "arguments": "..."}}
    输出： {"role": "assistant", "content": "...", "tool_calls": [{"id": "...", "type": "function", "function": {...}}]}
    """
    import json as _json

    fc = msg.get("function_call", {})
    if not fc:
        return msg

    name = fc.get("name", "")
    arguments = fc.get("arguments", "{}")
    if isinstance(arguments, dict):
        arguments = _json.dumps(arguments)

    tool_call_id = _ensure_tool_id(f"fc_{name}")
    upgraded = {k: v for k, v in msg.items() if k != "function_call"}
    upgraded["tool_calls"] = [{
        "id": tool_call_id,
        "type": "function",
        "function": {"name": name, "arguments": arguments},
    }]
    return upgraded


def prepare_openai_messages(messages):
    """Translate messages for OpenAI API, handling cross-provider format differences.

    Handles:
    - Anthropic-native content blocks (tool_use → tool_calls, tool_result → role:"tool")
    - Legacy function_call → tool_calls upgrade when role:"tool" messages present
    - role:"tool" messages pass through (already OpenAI-native)

    为 OpenAI API 转换消息，处理跨 provider 的格式差异。

    处理：
    - Anthropic 原生 content block（tool_use → tool_calls，tool_result → role:"tool"）
    - 存在 role:"tool" 消息时，将旧式 function_call 升级为 tool_calls
    - role:"tool" 消息原样透传（本就是 OpenAI 原生格式）
    """
    import json as _json

    # Check if we need to upgrade function_call → tool_calls
    # 检查是否需要将 function_call 升级为 tool_calls
    needs_fc_upgrade = _has_function_call_tool_mismatch(messages)

    # Fast path: all string content, no cross-provider blocks to convert
    # 快速路径：内容全为字符串，没有需要转换的跨 provider block
    if all(isinstance(msg.get("content"), (str, type(None))) for msg in messages
           if msg.get("role") != "tool"):
        if not needs_fc_upgrade:
            return messages
        # Only need function_call → tool_calls upgrade
        # 只需做 function_call → tool_calls 升级
        return [
            _upgrade_function_call_to_tool_calls(m) if m.get("role") == "assistant" and m.get("function_call") else m
            for m in messages
        ]

    result = []
    for msg in messages:
        role = msg.get("role", "")
        content = msg.get("content")

        # role:"tool" is already OpenAI-native, pass through
        # role:"tool" 本就是 OpenAI 原生格式，直接透传
        if role == "tool":
            result.append(msg)
            continue

        # Assistant messages with legacy function_call → upgrade to tool_calls
        # 带旧式 function_call 的 assistant 消息 → 升级为 tool_calls
        if needs_fc_upgrade and role == "assistant" and msg.get("function_call") and not msg.get("tool_calls"):
            result.append(_upgrade_function_call_to_tool_calls(msg))
            continue

        # Assistant messages with Anthropic tool_use content blocks
        # 带 Anthropic tool_use content block 的 assistant 消息
        if role == "assistant" and isinstance(content, list):
            tool_uses = [b for b in content if isinstance(b, dict) and b.get("type") == "tool_use"]
            if tool_uses:
                text_parts = []
                for b in content:
                    if isinstance(b, dict) and b.get("type") == "text":
                        text_parts.append(b.get("text", ""))
                    elif isinstance(b, str):
                        text_parts.append(b)
                text_content = "".join(text_parts) or ""
                openai_tool_calls = []
                for tu in tool_uses:
                    args = tu.get("input", {})
                    openai_tool_calls.append({
                        "id": _ensure_tool_id(tu.get("id", "")),
                        "type": "function",
                        "function": {
                            "name": tu.get("name", ""),
                            "arguments": _json.dumps(args) if isinstance(args, dict) else str(args),
                        },
                    })
                result.append({"role": "assistant", "content": text_content, "tool_calls": openai_tool_calls})
                continue

        # User messages with Anthropic tool_result content blocks
        # 带 Anthropic tool_result content block 的 user 消息
        if role == "user" and isinstance(content, list):
            tool_results = [b for b in content if isinstance(b, dict) and b.get("type") == "tool_result"]
            if tool_results:
                non_tool = [b for b in content if not (isinstance(b, dict) and b.get("type") == "tool_result")]
                for tr in tool_results:
                    tr_content = tr.get("content", "")
                    result.append({
                        "role": "tool",
                        "tool_call_id": _ensure_tool_id(tr.get("tool_use_id", "")),
                        "content": tr_content if isinstance(tr_content, str) else str(tr_content),
                    })
                if non_tool:
                    result.append({**msg, "content": translate_content_for_openai(non_tool)})
                continue

        # Default: translate content (handles images, etc.)
        # Guard: normalize None content to "" for assistant+tool_calls messages
        # (strict OpenAI-compatible providers reject null content in that shape)
        # 防护：对 assistant+tool_calls 消息把 None content 归一为 ""（严格的 OpenAI 兼容 provider 会拒绝这种形状下的 null content）
        # 默认：转换内容（处理图片等）
        if isinstance(content, (str, type(None))):
            if content is None and role == "assistant" and msg.get("tool_calls"):
                result.append({**msg, "content": ""})
            else:
                result.append(msg)
        else:
            result.append({**msg, "content": translate_content_for_openai(content)})

    return result


class TokenCounter:
    """Token counting utilities for different models

    面向不同模型的 token 计数工具。
    """

    @staticmethod
    def count_messages_tokens(messages: List[Dict[str, Any]], model: str) -> int:
        """
        Estimate token count for messages.
        This is a simplified version - real implementation would use
        provider-specific tokenizers.

        Improved estimation based on empirical data:
        - Average English word is ~4.7 characters
        - Average token is ~0.75 words (due to BPE splitting)
        - Therefore: 1 token ≈ 3.5 characters (not 4)

        估算消息的 token 数。
        这是简化版本 —— 真实实现会使用各 provider 专属的分词器。

        基于经验数据改进的估算：
        - 英文单词平均约 4.7 个字符
        - 每个 token 平均约 0.75 个单词（BPE 切分所致）
        - 因此：1 个 token ≈ 3.5 个字符（而非 4 个）
        """
        total_chars = 0
        image_tokens = 0
        for message in messages:
            content = message.get("content", "")
            if isinstance(content, str):
                total_chars += len(content)
            elif isinstance(content, list):
                for item in content:
                    if isinstance(item, dict):
                        if item.get("type") in ("image", "image_url"):
                            image_tokens += 256  # midpoint: ~85 low-detail, ~765 high-detail
                            # 中间值：low-detail 约 85，high-detail 约 765
                        elif "text" in item:
                            total_chars += len(item["text"])

        # More accurate: 1 token per 3.5 characters
        # 更准确：每 3.5 个字符约 1 个 token
        base_tokens = int(total_chars / 3.5) + image_tokens

        # Add overhead for message structure
        # 为消息结构增加开销
        message_overhead = len(messages) * 4  # ~4 tokens per message for role/structure
        # 每条消息的角色/结构约 4 个 token

        return base_tokens + message_overhead

    @staticmethod
    def count_functions_tokens(functions: List[Dict]) -> int:
        """Estimate token count for function definitions"""
        if not functions:
            return 0

        # Serialize functions to estimate size
        # 序列化 functions 以估算大小
        functions_str = json.dumps(functions)
        # Use improved estimation: 1 token per 3.5 chars
        # 使用改进后的估算：每 3.5 个字符 1 个 token
        return int(len(functions_str) / 3.5)


class RateLimiter:
    """Rate limiting for API calls with RPM + concurrency control

    API 调用的 rate limit，支持 RPM 与并发控制。
    """

    def __init__(self, requests_per_minute: int = 60, max_concurrent: int = 0):
        self.requests_per_minute = requests_per_minute
        self.requests = []
        # Concurrency semaphore: 0 = unlimited, >0 = limit concurrent API calls
        # 并发信号量：0 = 不限制，>0 = 限制并发 API 调用数
        self._semaphore = asyncio.Semaphore(max_concurrent) if max_concurrent > 0 else None

    async def acquire(self):
        """Acquire permission to make a request (concurrency + RPM check).

        When max_concurrent > 0, this blocks until a concurrency slot opens,
        then checks the RPM window. The semaphore is released automatically
        after a brief hold period (simulating request duration awareness)
        via a background task, so callers don't need to explicitly release.

        获取发起 request 的许可（并发 + RPM 检查）。

        当 max_concurrent > 0 时，会先阻塞直到有并发槽位空出，再检查 RPM 窗口。
        信号量会在短暂持有后由后台 task 自动释放（模拟对 request 时长的感知），
        因此调用方无需显式释放。
        """
        while True:
            # Concurrency gate: block until a slot is available
            # 并发闸门：阻塞直到获得槽位
            if self._semaphore:
                await self._semaphore.acquire()

            now = time.time()

            # rpm=0 means unlimited — skip RPM tracking entirely
            # rpm=0 表示不限制 —— 完全跳过 RPM 统计
            if self.requests_per_minute <= 0:
                if self._semaphore:
                    asyncio.get_running_loop().call_later(15.0, self._safe_release)
                return

            # Remove old requests outside the window
            # 移除窗口外的旧 request
            self.requests = [r for r in self.requests if now - r < 60]

            # Check if we're at the limit
            # 检查是否已达上限
            if len(self.requests) >= self.requests_per_minute:
                # Release concurrency slot before retry to avoid deadlock
                # retry 前释放并发槽位，避免死锁
                if self._semaphore:
                    self._semaphore.release()
                # Calculate wait time
                # 计算等待时间
                oldest_request = min(self.requests)
                wait_time = 60 - (now - oldest_request) + 0.1
                if wait_time > 0:
                    await asyncio.sleep(wait_time)
                continue

            # Add current request
            # 记录当前 request
            self.requests.append(now)

            # Auto-release concurrency slot after 15 seconds (LLM calls typically 5-20s)
            # This prevents deadlocks without requiring callers to release
            # 自动在 15 秒后释放并发槽位（LLM 调用通常耗时 5-20 秒）。这样调用方无需显式释放，也不会死锁。
            if self._semaphore:
                asyncio.get_running_loop().call_later(15.0, self._safe_release)
            return

    def _safe_release(self):
        """Release concurrency slot (called by timer)"""
        if self._semaphore:
            try:
                self._semaphore.release()
            except ValueError:
                pass  # Already released
                # 已释放
