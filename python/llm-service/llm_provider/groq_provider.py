"""
Groq Provider Implementation
High-performance LLM inference using Groq's LPU (Language Processing Unit).
Aligned with OpenAI, xAI, and Google provider patterns for consistency.

Groq provider 实现。
使用 Groq 的 LPU（Language Processing Unit）进行高性能 LLM 推理。
与 OpenAI、xAI、Google 的 provider 模式保持一致。
"""

import os
import time
from typing import Dict, List, Any, AsyncIterator
from tenacity import retry, stop_after_attempt, wait_exponential
import logging
from openai import AsyncOpenAI

from .base import (
    LLMProvider,
    CompletionRequest,
    CompletionResponse,
    TokenUsage,
    TokenCounter,
)


class GroqProvider(LLMProvider):
    """Provider for Groq's high-performance LLM inference

    Groq 高性能 LLM 推理的 provider。
    """

    def __init__(self, config: Dict[str, Any]):
        self.logger = logging.getLogger(__name__)
        # Get API key from config or environment
        # 从 config 或环境变量获取 API key
        self.api_key = config.get("api_key") or os.getenv("GROQ_API_KEY")
        if not self.api_key:
            raise ValueError("Groq API key not provided")

        # Validate API key format (Groq keys typically start with 'gsk_' and are 56+ chars)
        # 校验 API key 格式（Groq 的 key 通常以 'gsk_' 开头且为 56+ 个字符）
        if len(self.api_key) < 40:
            raise ValueError("Invalid Groq API key format - too short")
        if not self.api_key.startswith(
            ("gsk_", "sk-", "test-")
        ):  # gsk_ for Groq, sk-/test- for testing 注：gsk_ 为 Groq，sk-/test- 用于测试
            self.logger.warning("Groq API key does not match expected format")

        # Initialize OpenAI-compatible client with Groq's base URL
        # 使用 Groq 的 base URL 初始化 OpenAI 兼容客户端
        self.client = AsyncOpenAI(
            api_key=self.api_key, base_url="https://api.groq.com/openai/v1"
        )

        super().__init__(config)

    def _initialize_models(self):
        """Initialize available Groq models from configuration.

        从配置初始化可用的 Groq 模型。
        """
        self._load_models_from_config()

    def count_tokens(self, messages: List[Dict[str, Any]], model: str) -> int:
        """Count tokens for messages using TokenCounter.

        使用 TokenCounter 统计消息的 token 数。
        """
        return TokenCounter.count_messages_tokens(messages, model)

    def _sanitize_messages(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Flatten list content to text for Groq (no multimodal support).

        Degrades shannon_attachment blocks to text descriptions; extracts text
        from structured content blocks.

        为 Groq 把列表内容压平成文本（不支持多模态）。

        将 shannon_attachment block 降级为文本描述；从结构化 content block 中提取文本。
        """
        sanitized = []
        for msg in messages:
            content = msg.get("content")
            if isinstance(content, list):
                text_parts = []
                for part in content:
                    if isinstance(part, dict) and part.get("type") == "shannon_attachment":
                        fname = part.get("filename", "file")
                        mtype = part.get("media_type", "unknown")
                        text_parts.append(f"[Attached file: {fname} ({mtype})]")
                    elif isinstance(part, dict) and part.get("type") == "text":
                        text_parts.append(part.get("text", ""))
                    elif isinstance(part, str):
                        text_parts.append(part)
                sanitized.append({**msg, "content": " ".join(text_parts)})
            else:
                sanitized.append(msg)
        return sanitized

    def _build_params(
        self, request: CompletionRequest, model_config, *, stream: bool
    ) -> Dict[str, Any]:
        # Prepare OpenAI-compatible request
        # 准备 OpenAI 兼容 request
        params: Dict[str, Any] = {
            "model": model_config.model_id,
            "messages": self._sanitize_messages(request.messages),
            "temperature": request.temperature,
            "max_tokens": request.max_tokens or model_config.max_tokens,
            "top_p": request.top_p,
            "frequency_penalty": request.frequency_penalty,
            "presence_penalty": request.presence_penalty,
            "stream": stream,
        }

        if request.stop:
            params["stop"] = request.stop

        if request.seed is not None:
            params["seed"] = request.seed

        # Functions are supported by some models
        # 部分 model 支持 functions
        if request.functions and model_config.supports_functions:
            from llm_provider.openai_provider import OpenAIProvider
            if OpenAIProvider._has_tool_role_messages(params.get("messages", [])):
                params["tools"] = OpenAIProvider._functions_to_tools_param(request.functions)
            else:
                params["functions"] = request.functions
            if request.function_call:
                if "tools" in params:
                    if request.function_call == "any":
                        params["tool_choice"] = "required"
                    elif request.function_call == "auto":
                        params["tool_choice"] = "auto"
                    elif request.function_call == "none":
                        params["tool_choice"] = "none"
                    else:
                        params["tool_choice"] = {"type": "function", "function": {"name": request.function_call}}
                else:
                    params["function_call"] = request.function_call

        return params

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=8)
    )
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Execute a completion request using Groq with native async support.

        使用 Groq 以原生 async 方式执行 completion request。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择 model
        model_config = self.resolve_model_config(request)
        model_id = model_config.model_id
        model_alias = self._resolve_alias(model_id)

        # Prepare OpenAI-compatible request
        # 准备 OpenAI 兼容 request
        completion_params = self._build_params(request, model_config, stream=False)

        if request.response_format:
            completion_params["response_format"] = request.response_format

        start_time = time.time()
        try:
            # Execute completion
            # 执行 completion
            response = await self.client.chat.completions.create(**completion_params)
            
            latency_ms = int((time.time() - start_time) * 1000)

            # Extract response
            # 提取 response
            choice = response.choices[0]
            message = choice.message
            content = message.content or ""

            # Handle function calls if present
            # 若存在 function call 则处理
            function_call = None
            if hasattr(message, "function_call") and message.function_call:
                function_call = {
                    "name": message.function_call.name,
                    "arguments": message.function_call.arguments,
                }

            # Extract token usage from response
            # 从 response 提取 token 用量
            try:
                prompt_tokens = int(getattr(response.usage, "prompt_tokens", 0))
                completion_tokens = int(getattr(response.usage, "completion_tokens", 0))
                total_tokens = int(
                    getattr(response.usage, "total_tokens", prompt_tokens + completion_tokens)
                )
            except Exception:
                # Fallback to estimation if usage not provided
                # 未提供 usage 时回退到估算
                prompt_tokens = self.count_tokens(request.messages, model_id)
                completion_tokens = self.count_tokens(
                    [{"role": "assistant", "content": content}], model_id
                )
                total_tokens = prompt_tokens + completion_tokens

            # Use base class estimate_cost with alias for proper lookup
            # 使用基类 estimate_cost，并传入别名以正确查找价格
            cost = self.estimate_cost(prompt_tokens, completion_tokens, model_alias)

            return CompletionResponse(
                content=content,
                model=model_id,
                provider="groq",
                usage=TokenUsage(
                    input_tokens=prompt_tokens,
                    output_tokens=completion_tokens,
                    total_tokens=total_tokens,
                    estimated_cost=cost,
                ),
                finish_reason=choice.finish_reason or "stop",
                function_call=function_call,
                request_id=getattr(response, "id", None),
                latency_ms=latency_ms,
            )

        except Exception as e:
            self.logger.error(f"Groq completion failed: {e}")
            raise Exception(f"Groq API error: {e}")

    async def complete_stream(
        self, request: CompletionRequest
    ) -> AsyncIterator[CompletionResponse]:
        """Stream a completion response from Groq (provider-specific chunks).

        流式返回 Groq 的 completion response（provider 特定的 chunk）。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择 model
        model_config = self.resolve_model_config(request)
        model_id = model_config.model_id
        model_alias = self._resolve_alias(model_id)

        # Prepare request
        # 准备 request
        completion_params = self._build_params(request, model_config, stream=True)

        try:
            # Stream response
            # 流式获取 response
            stream = await self.client.chat.completions.create(**completion_params)

            input_tokens = 0
            output_tokens = 0

            async for chunk in stream:
                if chunk.choices and chunk.choices[0].delta.content:
                    yield CompletionResponse(
                        content=chunk.choices[0].delta.content,
                        model=model_id,
                        provider="groq",
                        usage=TokenUsage(
                            input_tokens=0,
                            output_tokens=0,
                            total_tokens=0,
                            estimated_cost=0.0,
                        ),
                        finish_reason=None,
                        function_call=None,
                        request_id=None,
                        latency_ms=None,
                    )

                # Track token usage from chunks if available
                # 若 chunk 中带有 token 用量则累计
                if hasattr(chunk, "usage") and chunk.usage:
                    input_tokens = chunk.usage.prompt_tokens or input_tokens
                    output_tokens = chunk.usage.completion_tokens or output_tokens

            # Final response with usage
            # 带用量的最终 response
            if input_tokens > 0 or output_tokens > 0:
                total_tokens = input_tokens + output_tokens
                cost = self.estimate_cost(input_tokens, output_tokens, model_alias)

                yield CompletionResponse(
                    content="",
                    model=model_id,
                    provider="groq",
                    usage=TokenUsage(
                        input_tokens=input_tokens,
                        output_tokens=output_tokens,
                        total_tokens=total_tokens,
                        estimated_cost=cost,
                    ),
                    finish_reason="stop",
                    function_call=None,
                    request_id=None,
                    latency_ms=None,
                )

        except Exception as e:
            self.logger.error(f"Groq streaming failed: {e}")
            raise Exception(f"Groq streaming error: {e}")

    # NOTE: Tier-to-model fallback removed. Selection now relies on resolve_model_config
    # backed by models.yaml via the manager's configuration.
    # 注意：已移除 tier 到 model 的回退。现由 resolve_model_config 依靠 manager 的配置
    # （底层是 models.yaml）完成选择。

    async def stream_complete(self, request: CompletionRequest) -> AsyncIterator[str]:
        """Normalized streaming: yield text chunks and usage metadata.

        归一化 streaming：产出文本 chunk 与 usage metadata。
        """
        async for chunk in self.complete_stream(request):
            if chunk and isinstance(chunk.content, str) and chunk.content:
                yield chunk.content
            # Yield usage metadata when available (final chunk with empty content)
            # 有 usage metadata 时产出（content 为空的最后一个 chunk）
            elif chunk and chunk.usage and chunk.usage.total_tokens > 0:
                yield {
                    "usage": {
                        "total_tokens": chunk.usage.total_tokens,
                        "input_tokens": chunk.usage.input_tokens,
                        "output_tokens": chunk.usage.output_tokens,
                    },
                    "model": chunk.model,
                    "provider": chunk.provider,
                }
