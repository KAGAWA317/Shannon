"""
Google Gemini Provider Implementation
Provides access to Google's Gemini models via the Google AI Python SDK.
Aligned with OpenAI and xAI provider patterns for consistency.

Google Gemini provider 实现。
通过 Google AI Python SDK 访问 Google 的 Gemini 模型。
与 OpenAI、xAI 的 provider 模式保持一致。
"""

import logging
import os
import time
from typing import Dict, List, Any, AsyncIterator
from tenacity import retry, stop_after_attempt, wait_exponential
import google.generativeai as genai
from google.generativeai.types import HarmCategory, HarmBlockThreshold

from .base import (
    LLMProvider,
    CompletionRequest,
    CompletionResponse,
    TokenUsage,
    TokenCounter,
)


class GoogleProvider(LLMProvider):
    """Provider for Google Gemini models

    Google Gemini 模型的 provider。
    """

    def __init__(self, config: Dict[str, Any]):
        self.logger = logging.getLogger(__name__)
        
        # Get API key from config or environment
        # 从 config 或环境变量获取 API key
        api_key = config.get("api_key") or os.getenv("GOOGLE_API_KEY")
        if not api_key:
            raise ValueError("Google API key not provided")

        # Validate API key format (Google AI keys typically start with 'AIza' and are 39 chars)
        # 校验 API key 格式（Google AI 的 key 通常以 'AIza' 开头且为 39 个字符）
        if len(api_key) < 30:
            raise ValueError("Invalid Google API key format - too short")
        if not api_key.startswith(("AIza", "sk-", "test-")):
            self.logger.warning("Google API key does not match expected format")

        # Configure the Google AI SDK
        # 配置 Google AI SDK
        genai.configure(api_key=api_key)
        self.api_key = api_key

        # Store model instances (will be populated during initialization)
        # 存放 model 实例（初始化阶段填充）
        self.model_instances = {}

        # Safety settings - permissive by default to avoid blocking legitimate queries
        # Can be customized via config for production use
        # 安全设置 —— 默认宽松，避免拦截正常查询
        # 生产环境可通过 config 自定义
        self.safety_settings = config.get(
            "safety_settings",
            {
                HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_NONE,
                HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_NONE,
            },
        )

        super().__init__(config)

    def _initialize_models(self):
        """Initialize available Google Gemini models from configuration.

        从配置初始化可用的 Google Gemini 模型。
        """
        self._load_models_from_config()

        # Initialize model instances
        # 初始化 model 实例
        for alias, model_config in self.models.items():
            try:
                self.model_instances[alias] = genai.GenerativeModel(
                    model_config.model_id
                )
            except Exception as e:
                self.logger.warning(
                    f"Failed to initialize model {model_config.model_id}: {e}"
                )

    def _resolve_alias(self, model_id: str) -> str:
        """Return the configured alias for a given vendor model_id, if any.

        返回给定厂商 model_id 对应的已配置别名（若有）。
        """
        for alias, cfg in self.models.items():
            if cfg.model_id == model_id:
                return alias
        return model_id

    @staticmethod
    def _content_to_gemini_parts(content) -> list:
        """Convert message content (str or list of blocks) to Gemini parts.

        Handles text blocks, shannon_attachment blocks (internal marker),
        Anthropic-style image blocks, and plain strings.

        将消息内容（字符串或 block 列表）转换为 Gemini parts。

        处理 text block、shannon_attachment block（内部标记）、
        Anthropic 风格的 image block，以及普通字符串。
        """
        if isinstance(content, str):
            return [{"text": content}] if content else []
        if not isinstance(content, list):
            return [{"text": str(content)}] if content else []

        parts = []
        for part in content:
            if isinstance(part, str):
                parts.append({"text": part})
            elif isinstance(part, dict):
                ptype = part.get("type", "")
                if ptype == "text":
                    parts.append({"text": part.get("text", "")})
                elif ptype == "shannon_attachment":
                    if part.get("source") == "url":
                        url = part.get("url", "")
                        media_type = part.get("media_type", "application/octet-stream")
                        # Gemini file_data.file_uri is only safe for Gemini-hosted
                        # file URIs returned by the Files API. Arbitrary external
                        # URLs degrade to text instead of being sent as broken input.
                        # Gemini 的 file_data.file_uri 仅对 Files API 返回的 Gemini 托管
                        # 文件 URI 安全。任意外部 URL 会降级为文本，而不是作为坏输入发送。
                        if (
                            url.startswith("gs://")
                            or (
                                url.startswith("https://generativelanguage.googleapis.com/")
                                and "/files/" in url
                            )
                        ):
                            parts.append({
                                "file_data": {
                                    "mime_type": media_type,
                                    "file_uri": url,
                                }
                            })
                        else:
                            parts.append({"text": f"[External attachment: {url} ({media_type})]"})
                    elif part.get("media_type", "").startswith("image/") or part.get("media_type") == "application/pdf":
                        parts.append({
                            "inline_data": {
                                "mime_type": part["media_type"],
                                "data": part["data"],
                            }
                        })
                    else:
                        # Unsupported binary: degrade to text
                        # 不支持的二进制类型：降级为文本
                        fname = part.get("filename", "file")
                        parts.append({"text": f"[Unsupported attachment: {fname} ({part.get('media_type', '')})]"})
                elif ptype == "image" and part.get("source", {}).get("type") == "base64":
                    parts.append({
                        "inline_data": {
                            "mime_type": part["source"]["media_type"],
                            "data": part["source"]["data"],
                        }
                    })
        return parts

    def _convert_messages_to_gemini_format(
        self, messages: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Convert OpenAI-style messages to Gemini format with proper system message handling.

        Gemini API:
        - Uses "user" and "model" roles (not "assistant")
        - Doesn't support "system" role natively
        - Requires content to be a list of parts

        将 OpenAI 风格消息转换为 Gemini 格式，并正确处理 system 消息。

        Gemini API：
        - 使用 "user" 与 "model" 角色（而非 "assistant"）
        - 不原生支持 "system" 角色
        - 要求 content 为 parts 列表
        """
        gemini_messages = []
        system_parts = []

        # First pass: collect system messages (text only — system is always text)
        # 第一遍：收集 system 消息（仅文本 —— system 始终是文本）
        for msg in messages:
            role = msg.get("role")
            content = msg.get("content", "")

            if role == "system":
                # System messages: extract text only
                # system 消息：仅提取文本
                if isinstance(content, list):
                    text_parts = []
                    for part in content:
                        if isinstance(part, dict) and part.get("type") == "text":
                            text_parts.append(part.get("text", ""))
                        elif isinstance(part, str):
                            text_parts.append(part)
                    text = " ".join(text_parts)
                elif isinstance(content, str):
                    text = content
                else:
                    text = str(content) if content else ""
                if text:
                    system_parts.append(f"System: {text}")

        # Second pass: build Gemini messages with multimodal support
        # 第二遍：构建支持多模态的 Gemini 消息
        first_user_msg = True
        for msg in messages:
            role = msg.get("role")
            content = msg.get("content", "")

            # Skip system messages (already collected)
            # 跳过 system 消息（已收集）
            if role == "system":
                continue

            # Convert content to Gemini parts (handles attachments, images, text)
            # 将 content 转换为 Gemini parts（处理附件、图片、文本）
            parts = self._content_to_gemini_parts(content)

            # Skip empty messages
            # 跳过空消息
            if not parts:
                continue

            if role == "user":
                # Prepend system messages to first user message
                # 把 system 消息前置到第一条 user 消息
                if first_user_msg and system_parts:
                    system_prefix = "\n\n".join(system_parts)
                    parts = [{"text": system_prefix + "\n\n"}] + parts
                    first_user_msg = False
                gemini_messages.append({"role": "user", "parts": parts})
            elif role == "assistant":
                gemini_messages.append({"role": "model", "parts": parts})

        return gemini_messages

    def _create_generation_config(self, request: CompletionRequest) -> Dict[str, Any]:
        """Create Gemini generation configuration from request

        根据 request 创建 Gemini 生成配置。
        """
        config = {
            "temperature": request.temperature,
            "top_p": request.top_p,
            "max_output_tokens": request.max_tokens or 2048,
        }

        if request.stop:
            config["stop_sequences"] = request.stop

        return config

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=8)
    )
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Execute a completion request with native async support.

        以原生 async 方式执行 completion request。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择 model
        model_config = self.resolve_model_config(request)
        model_id = model_config.model_id
        model_alias = self._resolve_alias(model_id)

        if model_alias not in self.model_instances:
            raise ValueError(f"Model {model_alias} not initialized")

        model = self.model_instances[model_alias]

        # Convert messages to Gemini format
        # 将消息转换为 Gemini 格式
        gemini_messages = self._convert_messages_to_gemini_format(request.messages)

        # Create generation config
        # 创建生成配置
        generation_config = self._create_generation_config(request)

        start_time = time.time()
        try:
            # Use native async method if available
            # 若有原生 async 方法则使用
            if hasattr(model, "generate_content_async"):
                response = await model.generate_content_async(
                    gemini_messages,
                    generation_config=generation_config,
                    safety_settings=self.safety_settings,
                )
            else:
                # Fallback to sync method in executor
                # 回退到在 executor 中执行同步方法
                import asyncio
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: model.generate_content(
                        gemini_messages,
                        generation_config=generation_config,
                        safety_settings=self.safety_settings,
                    ),
                )
            
            latency_ms = int((time.time() - start_time) * 1000)

            # Extract text from response
            # 从 response 中提取文本
            if hasattr(response, "text") and response.text:
                content = response.text
            else:
                # Handle blocked or empty responses
                # 处理被拦截或为空的 response
                content = "Response was blocked or empty"
                self.logger.warning(f"Empty or blocked response from Gemini: {response}")

            # Extract tokens from usage metadata (Gemini provides accurate counts)
            # 从 usage metadata 提取 token 数（Gemini 提供精确计数）
            try:
                usage_metadata = getattr(response, "usage_metadata", None)
                if usage_metadata:
                    input_tokens = int(getattr(usage_metadata, "prompt_token_count", 0))
                    output_tokens = int(getattr(usage_metadata, "candidates_token_count", 0))
                    total_tokens = int(getattr(usage_metadata, "total_token_count", input_tokens + output_tokens))
                else:
                    # Fallback to estimation
                    # 回退到估算
                    input_tokens = self._estimate_tokens(str(request.messages))
                    output_tokens = self._estimate_tokens(content)
                    total_tokens = input_tokens + output_tokens
            except Exception:
                # Fallback to estimation
                # 回退到估算
                input_tokens = self._estimate_tokens(str(request.messages))
                output_tokens = self._estimate_tokens(content)
                total_tokens = input_tokens + output_tokens

            # Use base class estimate_cost with alias for proper lookup
            # 使用基类 estimate_cost，并传入别名以正确查找价格
            cost = self.estimate_cost(input_tokens, output_tokens, model_alias)

            # Extract request ID if available
            # 提取 request ID（若有）
            request_id = None
            if hasattr(response, "model_version"):
                request_id = f"gemini-{response.model_version}"
            elif hasattr(response, "_result"):
                request_id = str(getattr(response._result, "id", None))

            # Determine finish reason
            # 确定 finish reason
            finish_reason = "stop"
            if hasattr(response, "candidates") and response.candidates:
                first_candidate = response.candidates[0]
                if hasattr(first_candidate, "finish_reason"):
                    finish_reason = str(first_candidate.finish_reason).lower()

            # Create response
            # 构造 response
            return CompletionResponse(
                content=content,
                model=model_id,
                provider="google",
                usage=TokenUsage(
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    total_tokens=total_tokens,
                    estimated_cost=cost,
                ),
                finish_reason=finish_reason,
                function_call=None,  # Gemini doesn't use OpenAI-style function calls
                # Gemini 不使用 OpenAI 风格的 function call
                request_id=request_id,
                latency_ms=latency_ms,
            )

        except Exception as e:
            self.logger.error(f"Google completion failed: {e}")
            raise Exception(f"Google Gemini API error: {e}")

    async def complete_stream(
        self, request: CompletionRequest
    ) -> AsyncIterator[CompletionResponse]:
        """Stream a completion response (returns CompletionResponse chunks).

        流式返回 completion response（返回 CompletionResponse chunk）。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择 model
        model_config = self.resolve_model_config(request)
        model_id = model_config.model_id
        model_alias = self._resolve_alias(model_id)

        if model_alias not in self.model_instances:
            raise ValueError(f"Model {model_alias} not initialized")

        model = self.model_instances[model_alias]

        # Convert messages to Gemini format
        # 将消息转换为 Gemini 格式
        gemini_messages = self._convert_messages_to_gemini_format(request.messages)

        # Create generation config
        # 创建生成配置
        generation_config = self._create_generation_config(request)

        try:
            # Use native async streaming if available
            # 若有原生 async streaming 则使用
            if hasattr(model, "generate_content_async"):
                response_stream = await model.generate_content_async(
                    gemini_messages,
                    generation_config=generation_config,
                    safety_settings=self.safety_settings,
                    stream=True,
                )
            else:
                # Fallback to sync method in executor
                # 回退到在 executor 中执行同步方法
                import asyncio
                loop = asyncio.get_event_loop()
                response_stream = await loop.run_in_executor(
                    None,
                    lambda: model.generate_content(
                        gemini_messages,
                        generation_config=generation_config,
                        safety_settings=self.safety_settings,
                        stream=True,
                    ),
                )

            # Stream chunks
            # 逐块流式输出
            accumulated_text = ""
            async for chunk in response_stream:
                if hasattr(chunk, "text") and chunk.text:
                    accumulated_text += chunk.text

                    yield CompletionResponse(
                        content=chunk.text,
                        model=model_id,
                        provider="google",
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

            # Final response with usage
            # 带用量的最终 response
            if hasattr(response_stream, "_done") and response_stream._done:
                final_response = response_stream._done
                try:
                    usage_metadata = getattr(final_response, "usage_metadata", None)
                    if usage_metadata:
                        input_tokens = int(getattr(usage_metadata, "prompt_token_count", 0))
                        output_tokens = int(getattr(usage_metadata, "candidates_token_count", 0))
                    else:
                        input_tokens = self._estimate_tokens(str(request.messages))
                        output_tokens = self._estimate_tokens(accumulated_text)
                except Exception:
                    input_tokens = self._estimate_tokens(str(request.messages))
                    output_tokens = self._estimate_tokens(accumulated_text)

                total_tokens = input_tokens + output_tokens
                cost = self.estimate_cost(input_tokens, output_tokens, model_alias)

                yield CompletionResponse(
                    content="",
                    model=model_id,
                    provider="google",
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
            self.logger.error(f"Google streaming failed: {e}")
            raise Exception(f"Google Gemini streaming error: {e}")

    # NOTE: Tier-to-model fallback removed. Selection now relies on resolve_model_config
    # backed by models.yaml via the manager's configuration.
    # 注意：已移除 tier 到 model 的回退。现由 resolve_model_config 依靠 manager 的配置
    # （底层是 models.yaml）完成选择。

    def _estimate_tokens(self, text: str) -> int:
        """Estimate token count for text (rough heuristic).

        估算文本的 token 数（粗略启发式）。
        """
        # Rough estimation: 1 token per 4 characters
        # 粗略估算：每 4 个字符 1 个 token
        return len(text) // 4

    def count_tokens(self, messages: List[Dict[str, Any]], model: str) -> int:
        """Count tokens for messages using Gemini's native counter or TokenCounter fallback.

        使用 Gemini 原生计数器或 TokenCounter 兜底来统计消息的 token 数。
        """
        # Try to use Gemini's native token counting if available
        # 若可用，优先使用 Gemini 原生 token 计数
        model_alias = self._resolve_alias(model)
        if model_alias in self.model_instances:
            try:
                model_instance = self.model_instances[model_alias]
                # Convert messages to text for counting
                # 将消息拼接为文本以便计数
                text_parts = []
                for msg in messages:
                    content = msg.get("content", "")
                    if isinstance(content, list):
                        for part in content:
                            if isinstance(part, dict) and part.get("type") == "text":
                                text_parts.append(part.get("text", ""))
                            elif isinstance(part, str):
                                text_parts.append(part)
                    else:
                        text_parts.append(str(content))
                combined_text = " ".join(text_parts)
                return model_instance.count_tokens(combined_text).total_tokens
            except Exception as e:
                self.logger.warning(f"Failed to count tokens with Gemini model: {e}")

        # Fallback to TokenCounter
        # 回退到 TokenCounter
        return TokenCounter.count_messages_tokens(messages, model)

    async def stream_complete(self, request: CompletionRequest) -> AsyncIterator[str]:
        """Stream text chunks and usage metadata (normalized streaming).

        流式输出文本 chunk 与 usage metadata（归一化 streaming）。
        """
        # Use complete_stream and yield text chunks and usage metadata
        # 使用 complete_stream 并产出文本 chunk 与 usage metadata
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
