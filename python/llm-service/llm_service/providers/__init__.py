from typing import Dict, List, Optional, Any
import logging
from enum import Enum

from llm_provider.manager import get_llm_manager, LLMManager
from llm_provider.base import (
    ModelTier,
    CompletionResponse,
    TokenUsage as CoreTokenUsage,
)

from .base import ModelInfo

logger = logging.getLogger(__name__)


class ProviderType(Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    GOOGLE = "google"
    DEEPSEEK = "deepseek"
    QWEN = "qwen"
    BEDROCK = "bedrock"
    OLLAMA = "ollama"
    GROQ = "groq"
    XAI = "xai"
    KIMI = "kimi"
    MINIMAX = "minimax"


_PROVIDER_NAME_MAP: Dict[str, ProviderType] = {
    "openai": ProviderType.OPENAI,
    "anthropic": ProviderType.ANTHROPIC,
    "google": ProviderType.GOOGLE,
    "deepseek": ProviderType.DEEPSEEK,
    "qwen": ProviderType.QWEN,
    "bedrock": ProviderType.BEDROCK,
    "ollama": ProviderType.OLLAMA,
    "groq": ProviderType.GROQ,
    "xai": ProviderType.XAI,
    "kimi": ProviderType.KIMI,
    "minimax": ProviderType.MINIMAX,
}


class _ProviderAdapter:
    """Thin adapter that exposes list_models while delegating other attributes.

    薄适配器：对外暴露 list_models，其余属性透传。
    """

    def __init__(
        self, provider_type: ProviderType, provider: Any, models: List[ModelInfo]
    ):
        self._provider_type = provider_type
        self._provider = provider
        self._models = models

    def list_models(self) -> List[ModelInfo]:
        return list(self._models)

    def __getattr__(self, item: str) -> Any:
        return getattr(self._provider, item)


class ProviderManager:
    """Facade that keeps legacy API surface while delegating to LLMManager.

    门面类：保留旧 API 接口，同时委托给 LLMManager。
    """

    def __init__(self, settings):
        self.settings = settings
        self._manager: LLMManager = get_llm_manager()
        self.providers: Dict[ProviderType, _ProviderAdapter] = {}
        self.model_registry: Dict[str, ModelInfo] = {}
        self.tier_models: Dict[ModelTier, List[str]] = {
            ModelTier.SMALL: [],
            ModelTier.MEDIUM: [],
            ModelTier.LARGE: [],
        }
        self.session_tokens: Dict[str, int] = {}
        self.max_tokens_per_session = 100000
        self._emitter = None

    async def initialize(self) -> None:
        """Load provider metadata from the unified manager.

        从统一的 manager 加载 provider metadata。
        """
        self._refresh_registry()

    async def reload(self) -> None:
        """Hot-reload provider configuration from the unified manager.

        从统一的 manager 热重载 provider 配置。
        """

        await self._manager.reload()
        self._refresh_registry()

    def _refresh_registry(self) -> None:
        self.providers.clear()
        self.model_registry.clear()
        for tier in self.tier_models:
            self.tier_models[tier] = []

        for name, provider in self._manager.registry.providers.items():
            provider_type = _PROVIDER_NAME_MAP.get(name)
            models = self._collect_models(provider_type, provider)
            if provider_type:
                self.providers[provider_type] = _ProviderAdapter(
                    provider_type, provider, models
                )

    def _collect_models(
        self, provider_type: Optional[ProviderType], provider: Any
    ) -> List[ModelInfo]:
        models: List[ModelInfo] = []

        for alias, config in provider.models.items():
            info = self._build_model_info(provider_type, alias, config)
            models.append(info)
            self.model_registry[alias] = info
            if config.model_id != alias:
                self.model_registry[config.model_id] = info
            if info.tier not in self.tier_models:
                self.tier_models[info.tier] = []
            if alias not in self.tier_models[info.tier]:
                self.tier_models[info.tier].append(alias)

        return models

    @staticmethod
    def _build_model_info(
        provider_type: Optional[ProviderType], alias: str, config: Any
    ) -> ModelInfo:
        # config.tier is already ModelTier from llm_provider.base (same enum now)
        # config.tier 已是 llm_provider.base 的 ModelTier（现在共用同一 enum）
        provider_value: Any = (
            provider_type if provider_type else (config.provider or "unknown")
        )

        return ModelInfo(
            id=alias,
            name=alias,
            provider=provider_value,
            tier=config.tier,
            context_window=config.context_window,
            cost_per_1k_prompt_tokens=config.input_price_per_1k,
            cost_per_1k_completion_tokens=config.output_price_per_1k,
            supports_tools=getattr(config, "supports_functions", True),
            supports_streaming=getattr(config, "supports_streaming", True),
            available=True,
        )

    async def close(self) -> None:
        """Close adapters if underlying providers expose close hooks.

        若底层 provider 暴露 close 钩子，则关闭各适配器。
        """
        for adapter in self.providers.values():
            close = getattr(adapter, "close", None)
            if close:
                await close()

    def set_emitter(self, emitter) -> None:
        self._emitter = emitter

    def select_model(
        self, tier: ModelTier = None, specific_model: str = None
    ) -> Optional[str]:
        if specific_model and specific_model in self.model_registry:
            return specific_model

        tier = tier or ModelTier.SMALL

        preferred = self.tier_models.get(tier, [])
        if preferred:
            return preferred[0]

        # Fallback to any available tier in order
        # 按顺序回退到任意可用 tier
        for fallback_tier in (
            ModelTier.SMALL,
            ModelTier.MEDIUM,
            ModelTier.LARGE,
        ):
            candidates = self.tier_models.get(fallback_tier, [])
            if candidates:
                return candidates[0]

        return None

    async def generate_completion(
        self,
        messages: List[dict],
        tier: ModelTier = None,
        specific_model: str = None,
        provider_override: Optional[str] = None,
        **kwargs,
    ) -> dict:
        params = dict(kwargs)

        session_id = params.get("session_id")
        workflow_id = (
            params.pop("workflow_id", None)
            or params.pop("workflowId", None)
            or params.pop("WORKFLOW_ID", None)
        )
        agent_id = (
            params.get("agent_id")
            or params.pop("agentId", None)
            or params.pop("AGENT_ID", None)
        )

        # ModelTier is now unified - no conversion needed
        # ModelTier 现已统一 —— 无需转换
        tier = tier or ModelTier.SMALL

        manager_kwargs: Dict[str, Any] = {}

        # Recognized request fields passed through to CompletionRequest
        # 透传给 CompletionRequest 的已识别 request 字段
        passthrough_fields = {
            "temperature",
            "max_tokens",
            "top_p",
            "frequency_penalty",
            "presence_penalty",
            "stop",
            "response_format",
            "seed",
            "user",
            "function_call",
            "stream",
            "cache_key",
            "cache_ttl",
            "session_id",
            "task_id",
            "agent_id",
            "max_tokens_budget",
            "previous_response_id",
            "output_config",
            "thinking",
            "reasoning_effort",
            "cache_source",
        }

        for field in list(params.keys()):
            if field in passthrough_fields and params[field] is not None:
                manager_kwargs[field] = params.pop(field)

        if "temperature" not in manager_kwargs or manager_kwargs["temperature"] is None:
            manager_kwargs["temperature"] = self.settings.temperature

        # Anthropic models don't support both temperature and top_p.
        # Since we may not know the final provider at this point (auto-routing),
        # always drop top_p when temperature is set to ensure compatibility.
        # Anthropic 模型不支持同时设置 temperature 与 top_p。
        # 此处可能还不知道最终 provider（自动路由），因此在设置 temperature 时
        # 一律丢弃 top_p 以确保兼容。
        if "temperature" in manager_kwargs and "top_p" in manager_kwargs:
            manager_kwargs.pop("top_p", None)

        if agent_id and "agent_id" not in manager_kwargs:
            manager_kwargs["agent_id"] = agent_id

        tools = params.pop("tools", None)
        if tools:
            # Normalize OpenAI tools schema → legacy functions schema for providers
            # that expect functions. Each tool is {"type":"function","function":{...}}
            # 将 OpenAI tools schema 归一化为旧式 functions schema，供期望 functions 的
            # provider 使用。每个 tool 形如 {"type":"function","function":{...}}
            functions: List[Dict[str, Any]] = []
            try:
                for t in tools:
                    if isinstance(t, dict) and t.get("type") == "function" and isinstance(t.get("function"), dict):
                        functions.append(t["function"])
                    elif isinstance(t, dict):
                        # Already a function schema
                        # 已经是 function schema
                        functions.append(t)
            except Exception:
                # Best-effort; fall back to passing through
                # 尽力而为；回退为原样透传
                functions = tools  # type: ignore[assignment]
            if functions:
                manager_kwargs["functions"] = functions

        if specific_model:
            manager_kwargs["model"] = specific_model
        if provider_override:
            manager_kwargs["provider_override"] = provider_override

        response: CompletionResponse = await self._manager.complete(
            messages=messages,
            model_tier=tier,
            **manager_kwargs,
        )

        result = self._serialize_completion(response)

        if session_id and result.get("usage"):
            total_tokens = result["usage"].get("total_tokens")
            if total_tokens is not None:
                self.session_tokens[session_id] = (
                    self.session_tokens.get(session_id, 0) + total_tokens
                )
                logger.info(
                    "Session %s token usage: %s",
                    session_id,
                    self.session_tokens[session_id],
                )

        if self.settings.enable_llm_events and self._emitter and workflow_id:
            self._emit_events(
                workflow_id=workflow_id,
                agent_id=agent_id,
                messages=messages,
                response=result,
            )

        return result

    async def stream_completion(
        self,
        messages: List[dict],
        tier: ModelTier = None,
        specific_model: str = None,
        provider_override: Optional[str] = None,
        **kwargs,
    ):
        params = dict(kwargs)

        # ModelTier is now unified - no conversion needed
        # ModelTier 现已统一，无需转换
        tier = tier or ModelTier.SMALL

        manager_kwargs: Dict[str, Any] = {}

        passthrough_fields = {
            "temperature",
            "max_tokens",
            "top_p",
            "frequency_penalty",
            "presence_penalty",
            "stop",
            "response_format",
            "seed",
            "user",
            "function_call",
            "stream",
            "cache_key",
            "cache_ttl",
            "session_id",
            "task_id",
            "agent_id",
            "max_tokens_budget",
            "output_config",
            "thinking",
            "reasoning_effort",
            "cache_source",
        }

        for field in list(params.keys()):
            if field in passthrough_fields and params[field] is not None:
                manager_kwargs[field] = params.pop(field)

        if "temperature" not in manager_kwargs or manager_kwargs["temperature"] is None:
            manager_kwargs["temperature"] = self.settings.temperature

        # Anthropic models don't support both temperature and top_p.
        # Since we may not know the final provider at this point (auto-routing),
        # always drop top_p when temperature is set to ensure compatibility.
        # Anthropic 模型不支持同时设置 temperature 与 top_p。
        # 此处可能还不知道最终 provider（自动路由），因此在设置 temperature 时
        # 一律丢弃 top_p 以确保兼容。
        if "temperature" in manager_kwargs and "top_p" in manager_kwargs:
            manager_kwargs.pop("top_p", None)

        tools = params.pop("tools", None)
        if tools:
            functions: List[Dict[str, Any]] = []
            try:
                for t in tools:
                    if isinstance(t, dict) and t.get("type") == "function" and isinstance(t.get("function"), dict):
                        functions.append(t["function"])
                    elif isinstance(t, dict):
                        functions.append(t)
            except Exception:
                functions = tools  # type: ignore[assignment]
            if functions:
                manager_kwargs["functions"] = functions

        if specific_model:
            manager_kwargs["model"] = specific_model
        if provider_override:
            manager_kwargs["provider_override"] = provider_override

        async for chunk in self._manager.stream_complete(
            messages=messages,
            model_tier=tier,
            **manager_kwargs,
        ):
            if chunk:
                yield chunk

    def _serialize_completion(self, response: CompletionResponse) -> Dict[str, Any]:
        usage = self._serialize_usage(response.usage)

        # Normalize provider to a non-null string to avoid None → null propagation
        # 将 provider 归一化为非空字符串，避免 None 传播成 null
        provider = response.provider if isinstance(response.provider, str) and response.provider else "unknown"

        result = {
            "provider": provider,
            "model": response.model,
            "output_text": response.content,
            "usage": usage,
            "finish_reason": response.finish_reason,
            "function_call": response.function_call,
            "request_id": response.request_id,
            "latency_ms": response.latency_ms,
            "cached": response.cached,
        }

        if response.tool_calls:
            result["tool_calls"] = response.tool_calls

        # Ordered list of the assistant's content blocks,
        # preserving Anthropic's original block order including thinking /
        # redacted_thinking. Clients that understand this field MUST prefer
        # it over `output_text` + `tool_calls` when constructing the next
        # assistant turn — only `content_blocks` carries the thinking blocks
        # required for the trajectory roundtrip. Omitted for providers /
        # paths that don't emit it (e.g. when the SDK shape isn't supported)
        # so older clients keep working untouched.
        # assistant content block 的有序列表，保留 Anthropic 的原始 block 顺序，
        # 包括 thinking / redacted_thinking。理解该字段的客户端在构造下一轮 assistant
        # 消息时必须优先使用它（而非 `output_text` + `tool_calls`）—— 只有
        # `content_blocks` 携带轨迹往返所需的 thinking block。不产生该字段的
        # provider / 路径会省略它（如 SDK 形状不受支持时），使老客户端不受影响。
        if response.content_blocks:
            result["content_blocks"] = response.content_blocks

        # Add effective_max_completion if available (for continuation trigger logic)
        # 若可用则加入 effective_max_completion（供续写触发逻辑使用）
        if response.effective_max_completion is not None:
            result["effective_max_completion"] = response.effective_max_completion

        return result

    @staticmethod
    def _serialize_usage(usage: Optional[CoreTokenUsage]) -> Dict[str, Any]:
        if not usage:
            return {}
        return {
            "input_tokens": usage.input_tokens,
            "output_tokens": usage.output_tokens,
            "total_tokens": usage.total_tokens,
            "cost_usd": usage.estimated_cost,
            "cache_read_tokens": usage.cache_read_tokens,
            "cache_creation_tokens": usage.cache_creation_tokens,
            "cache_creation_5m_tokens": usage.cache_creation_5m_tokens,
            "cache_creation_1h_tokens": usage.cache_creation_1h_tokens,
            "call_sequence": usage.call_sequence,
        }

    def _extract_user_query(self, content: str) -> str:
        """Extract user query from message content, omitting tools.

        Industry best practice: Don't stream input prompts or tool definitions.
        Only extract the actual user query for display purposes.

        Args:
            content: Message content (plain string or JSON-serialized dict)

        Returns:
            User query text, or empty string to skip emission

        从消息 content 中提取用户 query，并省略 tools。
        业界最佳实践：不流式输出 input prompt 或 tool 定义，仅提取真实的用户 query 用于展示。
        """
        import json

        # Try to parse as JSON (agent execution request)
        # 尝试按 JSON 解析（agent 执行 request）
        try:
            data = json.loads(content) if isinstance(content, str) and content.strip().startswith("{") else None
            if isinstance(data, dict):
                # Skip agent execution requests with tools entirely
                # 带 tools 的 agent 执行 request 完全跳过
                if "tools" in data:
                    return ""
                # Extract task field if present
                # 若存在 task 字段则提取
                if "task" in data:
                    return str(data["task"])
                # Otherwise skip
                # 其他情况跳过
                return ""
        except (json.JSONDecodeError, TypeError):
            pass

        # Plain string content - return as-is
        # 普通字符串内容 —— 原样返回
        return content if isinstance(content, str) else ""

    def _emit_events(
        self,
        workflow_id: str,
        agent_id: Optional[str],
        messages: List[dict],
        response: Dict[str, Any],
    ) -> None:
        try:
            last_user = next(
                (
                    m.get("content", "")
                    for m in reversed(messages)
                    if m.get("role") == "user"
                ),
                "",
            )
        except Exception:
            last_user = ""

        if last_user:
            # Extract user query only, omit tools (industry best practice)
            # 仅提取用户 query，省略 tools（业界最佳实践）
            prompt_text = self._extract_user_query(last_user)

            # Only emit LLM_PROMPT if we have valid query text
            # 仅在有有效 query 文本时上报 LLM_PROMPT
            if prompt_text:
                payload = {
                    "provider": response.get("provider"),
                    "model": response.get("model"),
                }
                try:
                    self._emitter.emit(
                        workflow_id,
                        "LLM_PROMPT",
                        agent_id=agent_id,
                        message=prompt_text[:500],  # Industry standard: 500 chars max 注：业界惯例上限 500 字符
                        payload=payload,
                    )
                except Exception:
                    logger.debug("Failed to emit LLM_PROMPT", exc_info=True)

        output_text = response.get("output_text") or ""
        if not output_text:
            return

        if self.settings.enable_llm_partials:
            chunk = max(int(self.settings.partial_chunk_chars), 1)
            total = (len(output_text) + chunk - 1) // chunk
            for idx, start in enumerate(range(0, len(output_text), chunk)):
                try:
                    self._emitter.emit(
                        workflow_id,
                        "LLM_PARTIAL",
                        agent_id=agent_id,
                        message=output_text[start : start + chunk],
                        payload={"chunk_index": idx, "total_chunks": total},
                    )
                except Exception:
                    logger.debug("Failed to emit LLM_PARTIAL", exc_info=True)

        usage_payload = response.get("usage") or {}
        try:
            self._emitter.emit(
                workflow_id,
                "LLM_OUTPUT",
                agent_id=agent_id,
                message=output_text[:4000],
                payload={
                    "provider": response.get("provider"),
                    "model": response.get("model"),
                    "usage": usage_payload,
                },
            )
        except Exception:
            logger.debug("Failed to emit LLM_OUTPUT", exc_info=True)

    async def generate_embedding(self, text: str, model: str = None) -> List[float]:
        return await self._manager.generate_embedding(text, model)

    def is_configured(self) -> bool:
        return bool(self._manager.registry.providers)

    def get_provider(self, tier: str = "small") -> Any:
        if self.providers:
            return next(iter(self.providers.values()))
        return None

    def get_model_info(self, model_id: str) -> Optional[ModelInfo]:
        return self.model_registry.get(model_id)

    def list_available_models(self, tier: ModelTier = None) -> List[ModelInfo]:
        if tier:
            ids = self.tier_models.get(tier, [])
            return [self.model_registry[mid] for mid in ids]
        return list(self.model_registry.values())
