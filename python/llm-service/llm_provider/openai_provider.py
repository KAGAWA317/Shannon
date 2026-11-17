"""
OpenAI Provider Implementation
Adds support for OpenAI Responses API with fallback to Chat Completions.
Prefers provider-reported token usage; falls back to estimation only if needed.

OpenAI provider 实现
支持 OpenAI Responses API，并可回退到 Chat Completions。
优先使用 provider 上报的 token 用量；仅在必要时回退到估算。
"""

import os
from collections.abc import AsyncIterator
from typing import Any

import openai
import tiktoken
from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import (
    CompletionRequest,
    CompletionResponse,
    LLMProvider,
    TokenUsage,
    extract_text_from_content,
    prepare_openai_messages,
)


class OpenAIProvider(LLMProvider):
    """OpenAI API provider implementation

    OpenAI API 的 provider 实现。
    """

    def __init__(self, config: dict[str, Any]):
        # Initialize OpenAI client
        # 初始化 OpenAI 客户端
        api_key = config.get("api_key") or os.getenv("OPENAI_API_KEY")
        if not api_key:
            raise ValueError("OpenAI API key not provided")

        self.organization = config.get("organization")
        timeout = int(config.get("timeout", 60) or 60)

        # Pass organization and timeout at construction time
        # 在构造时传入 organization 与 timeout
        self.client = AsyncOpenAI(
            api_key=api_key,
            organization=self.organization,
            timeout=timeout,
        )

        # Token encoders for different models
        # 各 model 的 token encoder
        self.encoders = {}

        super().__init__(config)

    def _resolve_alias(self, model_id: str) -> str:
        """Return the configured alias for a given vendor model_id, if any.

        返回给定厂商 model_id 对应的已配置别名（若有）。
        """
        for alias, cfg in self.models.items():
            if getattr(cfg, "model_id", None) == model_id:
                return alias
        return model_id

    def _initialize_models(self):
        """Load OpenAI model configurations from YAML-driven config.

        从 YAML 驱动的配置加载 OpenAI model 配置。
        """

        self._load_models_from_config()

    def _get_encoder(self, model: str):
        """Get or create token encoder for a model

        获取或创建某个 model 的 token encoder。
        """
        if model not in self.encoders:
            try:
                self.encoders[model] = tiktoken.encoding_for_model(model)
            except KeyError:
                # Fall back to cl100k_base encoding
                # 回退到 cl100k_base 编码
                self.encoders[model] = tiktoken.get_encoding("cl100k_base")
        return self.encoders[model]

    def count_tokens(self, messages: list[dict[str, Any]], model: str) -> int:
        """Count tokens using tiktoken

        使用 tiktoken 统计 token 数。
        """
        encoder = self._get_encoder(model)

        # Token counting logic based on OpenAI's guidelines
        # 基于 OpenAI 指南的 token 计数逻辑
        tokens_per_message = (
            3  # Every message follows <im_start>{role/name}\n{content}<im_end>\n 注：每条消息都遵循 <im_start>{role/name}\n{content}<im_end>\n
        )
        tokens_per_name = 1  # If there's a name, the role is omitted 注：若存在 name 字段，则省略 role

        num_tokens = 0
        for message in messages:
            num_tokens += tokens_per_message
            for key, value in message.items():
                if isinstance(value, str):
                    num_tokens += len(encoder.encode(value))
                    if key == "name":
                        num_tokens += tokens_per_name
                elif key == "content" and isinstance(value, list):
                    for block in value:
                        if isinstance(block, dict):
                            if block.get("type") == "text":
                                num_tokens += len(encoder.encode(block.get("text", "")))
                            elif block.get("type") in ("image", "image_url"):
                                # ~85 tokens low-detail, ~765 high-detail; use 256 as safe midpoint
                                # low-detail 约 85 个 token，high-detail 约 765 个；取 256 作为安全中间值
                                num_tokens += 256
                        elif isinstance(block, str):
                            num_tokens += len(encoder.encode(block))

        num_tokens += 3  # Every reply is primed with <im_start>assistant<im_sep> 注：每个回复都以 <im_start>assistant<im_sep> 作为起始
        return num_tokens

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=8)
    )
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        """Generate a completion using OpenAI API (Responses API preferred).

        使用 OpenAI API 生成 completion（优先 Responses API）。
        """
        import logging
        logger = logging.getLogger(__name__)

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择 model
        model_config = self.resolve_model_config(request)
        model = model_config.model_id

        # Choose API route based on model capabilities and request intent
        # 依据 model 能力与 request 意图选择 API 路由
        route = self._choose_api_route(request, model_config)
        if route == "responses":
            if hasattr(self.client, "responses") and hasattr(
                self.client.responses, "create"
            ):
                try:
                    return await self._complete_responses_api(request, model)
                except Exception as e:
                    # Log the actual error from Responses API
                    # 记录来自 Responses API 的实际错误
                    logger.warning(
                        f"Responses API failed for model {model}, falling back to Chat Completions API. "
                        f"Error: {type(e).__name__}: {str(e)}"
                    )
                    # Fall through to Chat Completions API
                    # 转为走 Chat Completions API
            # If Responses not available, fall through to chat
            # 若 Responses 不可用，则转为走 chat

        # Fallback: Chat Completions API
        # 兜底：Chat Completions API
        import time

        api_request = {
            "model": model,
            "messages": prepare_openai_messages(request.messages),
        }

        # GPT-5 family (excluding gpt-5-pro) has restricted parameter support
        # GPT-5 系列（不含 gpt-5-pro）支持的参数受限
        is_gpt5_chat = model.startswith("gpt-5") and not model.startswith("gpt-5-pro")
        # OpenAI reasoning models (o1, o3, o4) don't support sampling params
        # OpenAI reasoning model（o1、o3、o4）不支持采样参数
        is_reasoning_model = any(model.startswith(p) for p in ("o1", "o3", "o4"))

        # Only include sampling parameters if NOT GPT-5 chat or reasoning models
        # 仅当不是 GPT-5 chat 或 reasoning model 时才带上采样参数
        if not is_gpt5_chat and not is_reasoning_model:
            api_request["temperature"] = request.temperature
            api_request["top_p"] = request.top_p
            api_request["frequency_penalty"] = request.frequency_penalty
            api_request["presence_penalty"] = request.presence_penalty

        # Compute a safe max completion tokens based on context window headroom
        # This prevents requesting more tokens than the model can return given the prompt size
        # 基于 context window 余量计算安全的 max completion token
        # 避免 request 的 token 超过 model 在给定 prompt 大小下能返回的数量
        prompt_tokens_est = self.count_tokens(request.messages, model)
        # Reserve a small safety margin for tool metadata or post-processing
        # 为 tool metadata 或后处理预留少量安全余量
        safety_margin = 256
        # Model-configured maxima
        # model 配置的上限
        model_context = getattr(model_config, "context_window", 8192)
        model_max_output = getattr(model_config, "max_tokens", model_context)
        # Requested maximum, if provided
        # request 的上限（若提供）
        requested_max = int(request.max_tokens) if request.max_tokens else model_max_output
        # Available headroom for completion
        # completion 可用的余量
        headroom = max(0, model_context - prompt_tokens_est - safety_margin)
        adjusted_max = max(1, min(requested_max, model_max_output, headroom))

        # Debug: Log the calculation for GPT-5
        # 调试：记录 GPT-5 的 token 上限计算
        if model.startswith("gpt-5"):
            logger.info(
                f"Token limit calculation for {model}: "
                f"request.max_tokens={request.max_tokens}, model_max_output={model_max_output}, "
                f"requested_max={requested_max}, headroom={headroom}, adjusted_max={adjusted_max}"
            )

        # Debug logging for token limits
        # token 上限的调试日志
        if adjusted_max < 100:
            logger.warning(
                f"Very low max_completion_tokens for model {model}: adjusted_max={adjusted_max}, "
                f"prompt_tokens={prompt_tokens_est}, model_context={model_context}, "
                f"requested_max={requested_max}, headroom={headroom}"
            )

        # GPT-5 and reasoning models use max_completion_tokens instead of max_tokens
        # GPT-5 与推理模型使用 max_completion_tokens 而非 max_tokens
        if adjusted_max:
            if is_gpt5_chat or is_reasoning_model:
                api_request["max_completion_tokens"] = adjusted_max
                if is_gpt5_chat:
                    logger.info(
                        f"GPT-5 Chat API request for {model}: max_completion_tokens={adjusted_max}, "
                        f"prompt_tokens_est={prompt_tokens_est}, context_window={model_context}"
                    )
            else:
                api_request["max_tokens"] = adjusted_max

        if request.reasoning_effort and is_reasoning_model:
            api_request["reasoning_effort"] = request.reasoning_effort

        if request.stop and not is_reasoning_model:
            api_request["stop"] = request.stop
        if request.functions:
            # Modern OpenAI API requires 'tools' param (not legacy 'functions')
            # when messages contain role:"tool" entries
            # 当消息中包含 role:"tool" 条目时，现代 OpenAI API 要求使用 'tools'
            # 参数（而非旧式 'functions'）
            if self._has_tool_role_messages(api_request.get("messages", [])):
                api_request["tools"] = self._functions_to_tools_param(request.functions)
            else:
                api_request["functions"] = request.functions
            # Only set tool_choice when tools/functions are actually present.
            # Fallback from Anthropic may carry function_call without clearing it.
            # 仅在确实存在 tools/functions 时设置 tool_choice。
            # 从 Anthropic 回退时可能带着 function_call 而未清除。
            if request.function_call:
                if request.function_call == "any":
                    api_request["tool_choice"] = "required"
                elif request.function_call == "auto":
                    api_request["tool_choice"] = "auto"
                elif request.function_call == "none":
                    api_request["tool_choice"] = "none"
                else:
                    api_request["tool_choice"] = {"type": "function", "function": {"name": request.function_call}}
        if request.seed is not None:
            api_request["seed"] = request.seed
        if request.response_format:
            api_request["response_format"] = request.response_format
        if request.user:
            api_request["user"] = request.user

        start_time = time.time()
        try:
            response = await self.client.chat.completions.create(**api_request)
        except openai.APIError as e:
            logger.error(
                f"OpenAI Chat Completions API error for model {model}: "
                f"Status={getattr(e, 'status_code', 'unknown')}, "
                f"Type={type(e).__name__}, "
                f"Message={str(e)}"
            )
            raise Exception(f"OpenAI Chat Completions API error: {e}") from e
        except Exception as e:
            logger.error(
                f"Unexpected error calling Chat Completions API for model {model}: "
                f"Type={type(e).__name__}, "
                f"Message={str(e)}"
            )
            raise

        latency_ms = int((time.time() - start_time) * 1000)

        choice = response.choices[0]
        message = choice.message

        # Debug: Log raw message structure for GPT-5 models
        # 调试：记录 GPT-5 模型的原始消息结构
        if model.startswith("gpt-5"):
            raw_content = getattr(message, "content", None)
            # Get all message attributes to see what's available
            # 获取全部消息属性，以便了解可用字段
            msg_attrs = [attr for attr in dir(message) if not attr.startswith('_')]
            logger.info(
                f"GPT-5 message attributes: {msg_attrs}"
            )
            logger.info(
                f"GPT-5 raw message.content: type={type(raw_content)}, "
                f"len={len(raw_content) if isinstance(raw_content, str) else 'N/A'}, "
                f"is_empty_string={raw_content == ''}, "
                f"value_preview={raw_content[:200] if raw_content else '(empty or None)'}"
            )
            # Check for alternative content fields
            # 检查可选的 content 字段
            for alt_field in ['reasoning_content', 'thinking', 'internal_thoughts', 'output', 'text']:
                if hasattr(message, alt_field):
                    alt_value = getattr(message, alt_field, None)
                    logger.info(f"GPT-5 message.{alt_field}: {type(alt_value)}, preview: {str(alt_value)[:100]}")

        # Normalize message content: some models (e.g., GPT‑5/4.1) may return
        # content as a list of parts instead of a plain string. Extract the
        # text segments to avoid returning an empty string with non‑zero tokens.
        # 归一化消息 content：部分 model（如 GPT‑5/4.1）可能返回 content part 列表
        # 而非普通字符串。此处提取文本片段，避免在 token 数非零时返回空字符串。
        def _extract_text_from_message(msg) -> str:
            try:
                content = getattr(msg, "content", None)

                # 1. Plain string content
                # 1. 普通字符串 content
                if isinstance(content, str) and content.strip():
                    return content

                # 2. List of content parts (each may have a .text attribute or be a dict)
                # 2. content 分片列表（每片可能带 .text 属性，或是 dict）
                if isinstance(content, list):
                    parts: list[str] = []
                    for part in content:
                        try:
                            # Try part.text attribute
                            # 尝试 part.text 属性
                            text = getattr(part, "text", None)
                            # Try part["text"] dict key
                            # 尝试 part["text"] 字典键
                            if not text and isinstance(part, dict):
                                text = part.get("text")
                            # Try part["output_text"] for GPT-5
                            # GPT-5 情形下尝试 part["output_text"]
                            if not text and isinstance(part, dict):
                                text = part.get("output_text")
                            if isinstance(text, str) and text.strip():
                                parts.append(text.strip())
                        except Exception:
                            # Be permissive; ignore malformed parts
                            # 保持宽松，忽略格式错误的分片
                            pass
                    if parts:
                        return "\n\n".join(parts).strip()

                # 3. Some SDK variants expose a single object with .text
                # 3. 部分 SDK 变体暴露一个带 .text 的单一对象
                if hasattr(content, "text"):
                    txt = getattr(content, "text", "")
                    if txt:
                        return txt

                # 4. GPT-5 fallback: try model_dump() to inspect structured content
                # 4. GPT-5 兜底：尝试 model_dump() 检查结构化 content
                if hasattr(msg, "model_dump"):
                    try:
                        dump = msg.model_dump()
                        # Check if content is a list in the dump
                        # 检查 dump 中的 content 是否为列表
                        if isinstance(dump.get("content"), list):
                            parts: list[str] = []
                            for item in dump["content"]:
                                if isinstance(item, dict):
                                    # Try output_text, text, or any text field
                                    # 尝试 output_text、text 或任意文本字段
                                    for field in ["output_text", "text", "content"]:
                                        if field in item and isinstance(item[field], str) and item[field].strip():
                                            parts.append(item[field].strip())
                                            break
                            if parts:
                                logger.info(f"Extracted content from model_dump() list parts: {len(parts)} parts")
                                return "\n\n".join(parts).strip()
                    except Exception as e:
                        logger.warning(f"Failed to extract from model_dump(): {e}")

                # 5. GPT-5 reasoning fields
                # 5. GPT-5 推理字段
                for alt_field in ["reasoning_content", "output", "thinking"]:
                    if hasattr(msg, alt_field):
                        alt_value = getattr(msg, alt_field, None)
                        if isinstance(alt_value, str) and alt_value.strip():
                            logger.info(f"Extracted content from message.{alt_field}")
                            return alt_value
                        elif isinstance(alt_value, list):
                            # Handle list of reasoning parts
                            # 处理推理分片列表
                            parts: list[str] = []
                            for part in alt_value:
                                if isinstance(part, dict) and "text" in part:
                                    parts.append(str(part["text"]))
                                elif isinstance(part, str):
                                    parts.append(part)
                            if parts:
                                logger.info(f"Extracted content from message.{alt_field} list")
                                return "\n\n".join(parts).strip()

            except Exception as e:
                logger.warning(f"Content extraction failed: {e}")

            return ""

        content_text = _extract_text_from_message(message)

        # Special case: finish_reason == "function_call" or "tool_calls"
        # Content is in message.tool_calls array (new format) or message.function_call (old format)
        # 特殊情况：finish_reason 为 "function_call" 或 "tool_calls"
        # 内容位于 message.tool_calls 数组（新格式）或 message.function_call（旧格式）
        if not content_text or not content_text.strip():
            if choice.finish_reason in ["function_call", "tool_calls"]:
                # Try new format first (tool_calls array)
                # 先尝试新格式（tool_calls 数组）
                tool_calls = getattr(message, "tool_calls", None)
                if tool_calls and len(tool_calls) > 0:
                    tool_descriptions = []
                    for tc in tool_calls:
                        try:
                            func_name = getattr(tc.function, "name", "unknown")
                            func_args = getattr(tc.function, "arguments", "{}")
                            tool_descriptions.append(f"Tool: {func_name}, Args: {func_args}")
                        except Exception:
                            pass
                    if tool_descriptions:
                        content_text = "Tool calls:\n" + "\n".join(tool_descriptions)
                        logger.info(f"Extracted {len(tool_calls)} tool calls from message.tool_calls for finish_reason={choice.finish_reason}")

                # Try old format if new format didn't work (function_call object)
                # 新格式无效时再尝试旧格式（function_call 对象）
                if not content_text or not content_text.strip():
                    function_call = getattr(message, "function_call", None)
                    if function_call:
                        try:
                            func_name = getattr(function_call, "name", "unknown")
                            func_args = getattr(function_call, "arguments", "{}")
                            content_text = f"Tool call: {func_name}, Args: {func_args}"
                            logger.info(f"Extracted function call from message.function_call (old format) for finish_reason={choice.finish_reason}")
                        except Exception:
                            pass

        # Guard: If content is still empty but completion_tokens > 0, handle gracefully
        # 防护：若 content 仍为空但 completion_tokens > 0，则优雅处理
        completion_tokens_actual = int(getattr(response.usage, "completion_tokens", 0))
        if (not content_text or not content_text.strip()) and completion_tokens_actual > 0:
            # Special case: finish_reason == "length" means model hit token limit
            # GPT-5 reasoning models may consume all tokens for internal reasoning
            # 特殊情况：finish_reason == "length" 表示 model 触及 token 上限
            # GPT-5 reasoning model 可能把所有 token 都用于内部推理
            if choice.finish_reason == "length":
                logger.warning(
                    f"Model {model} hit token limit with empty content. "
                    f"finish_reason: length, "
                    f"prompt_tokens: {getattr(response.usage, 'prompt_tokens', 'N/A')}, "
                    f"completion_tokens: {completion_tokens_actual}. "
                    f"Returning partial content message."
                )
                # Return a partial content message instead of raising an exception
                # 返回部分内容的提示消息，而不是抛出 exception
                content_text = (
                    f"[Incomplete response: Model hit token limit ({completion_tokens_actual} tokens used). "
                    f"The response was truncated. Consider increasing max_tokens or simplifying the prompt.]"
                )
            else:
                # For other finish_reasons with empty content, this is an error
                # 其他 finish_reason 却内容为空，属于错误
                logger.error(
                    f"Empty content after all extraction attempts for model {model}. "
                    f"finish_reason: {choice.finish_reason}, "
                    f"prompt_tokens: {getattr(response.usage, 'prompt_tokens', 'N/A')}, "
                    f"completion_tokens: {completion_tokens_actual}"
                )
                # Dump message structure for debugging
                # 转储消息结构以便调试
                if hasattr(message, "model_dump"):
                    try:
                        dump = message.model_dump()
                        logger.error(f"Message dump: {dump}")
                    except Exception as e:
                        logger.error(f"Failed to dump message: {e}")
                # This is an error condition - content should exist with non-zero tokens
                # 属于错误状态——token 数非零时 content 本应存在
                raise Exception(
                    f"GPT-5 model {model} returned {completion_tokens_actual} completion tokens "
                    f"but content extraction failed. finish_reason={choice.finish_reason}"
                )

        # Debug logging for empty responses (only when completion_tokens == 0 or None content in function_call)
        # 空响应的调试日志（仅在 completion_tokens == 0，或 function_call 中 content 为 None 时）
        if not content_text or not content_text.strip():
            logger.warning(
                f"Empty response from Chat Completions API for model {model}. "
                f"message.content type: {type(message.content)}, "
                f"message.content value: {message.content}, "
                f"finish_reason: {choice.finish_reason}, "
                f"prompt_tokens: {getattr(response.usage, 'prompt_tokens', 'N/A')}, "
                f"completion_tokens: {completion_tokens_actual}"
            )

        # Prefer provider usage for tokens
        # 优先使用 provider 返回的 token 用量
        try:
            prompt_tokens = int(getattr(response.usage, "prompt_tokens", 0))
            completion_tokens = int(getattr(response.usage, "completion_tokens", 0))
            total_tokens = int(
                getattr(
                    response.usage, "total_tokens", prompt_tokens + completion_tokens
                )
            )
        except Exception:
            # Fallback to estimation only if needed
            # 仅在必要时回退到估算
            prompt_tokens = self.count_tokens(request.messages, model)
            completion_tokens = self.count_tokens(
                [{"role": "assistant", "content": content_text}], model
            )
            total_tokens = prompt_tokens + completion_tokens

        # Extract cache metrics (OpenAI automatic caching)
        # 提取 cache 指标（OpenAI 自动 cache）
        cache_read = 0
        try:
            details = getattr(response.usage, "prompt_tokens_details", None)
            if details:
                cache_read = int(getattr(details, "cached_tokens", 0) or 0)
        except Exception:
            pass

        # Use configured alias for cost lookup when available
        # 有已配置别名时用它查找成本
        cost = self.estimate_cost(
            prompt_tokens, completion_tokens, self._resolve_alias(model),
            cache_read_tokens=cache_read,
        )

        # Normalize function/tool call information to a plain dict for JSON safety
        # 将 function/tool call 信息归一化为普通 dict，确保 JSON 安全
        normalized_fc = None
        all_tool_calls = []
        try:
            # Newer SDKs expose structured tool calls; prefer those when present
            # 较新的 SDK 暴露结构化 tool call；存在时优先使用
            if hasattr(message, "tool_calls") and message.tool_calls:
                for tc in message.tool_calls:
                    tc_id = getattr(tc, "id", None)
                    fn = getattr(tc, "function", None)
                    if fn is not None:
                        if hasattr(fn, "model_dump"):
                            data = fn.model_dump()
                            entry = {
                                "id": tc_id,
                                "name": data.get("name"),
                                "arguments": data.get("arguments"),
                            }
                        else:
                            entry = {
                                "id": tc_id,
                                "name": getattr(fn, "name", None),
                                "arguments": getattr(fn, "arguments", None),
                            }
                        all_tool_calls.append(entry)
                if all_tool_calls:
                    normalized_fc = all_tool_calls[0]
            elif hasattr(message, "function_call") and message.function_call:
                fc = message.function_call
                if hasattr(fc, "model_dump"):
                    data = fc.model_dump()
                    normalized_fc = {
                        "name": data.get("name"),
                        "arguments": data.get("arguments"),
                    }
                else:
                    normalized_fc = {
                        "name": getattr(fc, "name", None),
                        "arguments": getattr(fc, "arguments", None),
                    }
        except Exception:
            # Be permissive – missing/invalid function call info should not fail the request
            # 保持宽容 —— 缺失/无效的 function call 信息不应让 request 失败
            normalized_fc = None
            all_tool_calls = []

        return CompletionResponse(
            content=content_text,
            model=model,
            provider="openai",
            usage=TokenUsage(
                input_tokens=prompt_tokens,
                output_tokens=completion_tokens,
                total_tokens=total_tokens,
                estimated_cost=cost,
                cache_read_tokens=cache_read,
            ),
            finish_reason=choice.finish_reason,
            function_call=normalized_fc,
            tool_calls=all_tool_calls if all_tool_calls else None,
            request_id=response.id,
            latency_ms=latency_ms,
            effective_max_completion=adjusted_max,
        )

    async def stream_complete(self, request: CompletionRequest) -> AsyncIterator[str]:
        """Stream a completion using OpenAI API

        使用 OpenAI API 流式生成 completion。
        """

        # Select model based on tier or explicit override
        # 按层级或显式覆盖选择 model
        model_config = self.resolve_model_config(request)
        model = model_config.model_id

        # Prepare API request (align parameters with non-streaming variant)
        # 准备 API request（参数与非流式版本保持一致）
        api_request = {
            "model": model,
            "messages": prepare_openai_messages(request.messages),
            "stream": True,
        }

        # GPT-5 family (excluding gpt-5-pro) has restricted parameter support
        # GPT-5 系列（不含 gpt-5-pro）支持的参数受限
        is_gpt5_chat = model.startswith("gpt-5") and not model.startswith("gpt-5-pro")
        # OpenAI reasoning models (o1, o3, o4) don't support sampling params
        # OpenAI reasoning model（o1、o3、o4）不支持采样参数
        is_reasoning_model = any(model.startswith(p) for p in ("o1", "o3", "o4"))

        # Only include sampling parameters if NOT GPT-5 chat or reasoning models
        # 仅当不是 GPT-5 chat 或 reasoning model 时才带上采样参数
        if not is_gpt5_chat and not is_reasoning_model:
            api_request["temperature"] = request.temperature
            api_request["top_p"] = request.top_p
            api_request["frequency_penalty"] = request.frequency_penalty
            api_request["presence_penalty"] = request.presence_penalty

        # GPT-5 and reasoning models use max_completion_tokens instead of max_tokens
        # GPT-5 与推理模型使用 max_completion_tokens 而非 max_tokens
        if request.max_tokens:
            if is_gpt5_chat or is_reasoning_model:
                api_request["max_completion_tokens"] = request.max_tokens
            else:
                api_request["max_tokens"] = request.max_tokens

        if request.reasoning_effort and is_reasoning_model:
            api_request["reasoning_effort"] = request.reasoning_effort

        if request.stop and not is_reasoning_model:
            api_request["stop"] = request.stop
        if request.functions:
            if self._has_tool_role_messages(api_request.get("messages", [])):
                api_request["tools"] = self._functions_to_tools_param(request.functions)
            else:
                api_request["functions"] = request.functions
            if request.function_call:
                if request.function_call == "any":
                    api_request["tool_choice"] = "required"
                elif request.function_call == "auto":
                    api_request["tool_choice"] = "auto"
                elif request.function_call == "none":
                    api_request["tool_choice"] = "none"
                else:
                    api_request["tool_choice"] = {"type": "function", "function": {"name": request.function_call}}
        if request.seed is not None:
            api_request["seed"] = request.seed
        if request.response_format:
            api_request["response_format"] = request.response_format
        if request.user:
            api_request["user"] = request.user

        # Make streaming API call
        # Request usage statistics for streaming
        # Add stream options for usage
        # 发起 streaming API 调用
        # 为 streaming request 统计 usage
        # 添加 usage 相关的 stream options
        api_request["stream_options"] = {"include_usage": True}

        # Helper to extract text from streaming delta (handles GPT-5 formats)
        # 从 streaming delta 中提取文本的辅助函数（处理 GPT-5 格式）
        def _extract_delta_content(delta) -> str:
            """Extract text content from streaming delta, handling GPT-5 formats.

            从 streaming delta 中提取文本内容，处理 GPT-5 格式。
            """
            # 1. Standard delta.content (string)
            # 1. 标准 delta.content（字符串）
            content = getattr(delta, "content", None)
            if isinstance(content, str) and content:
                return content

            # 2. delta.content as list (GPT-5 may return structured content)
            # 2. delta.content 为列表（GPT-5 可能返回结构化 content）
            if isinstance(content, list):
                parts = []
                for part in content:
                    text = None
                    if hasattr(part, "text"):
                        text = getattr(part, "text", None)
                    elif isinstance(part, dict):
                        text = part.get("text") or part.get("output_text")
                    if text:
                        parts.append(str(text))
                if parts:
                    return "".join(parts)

            # 3. GPT-5 reasoning/output fields
            # 3. GPT-5 推理/output 字段
            for field in ["reasoning_content", "output", "thinking"]:
                value = getattr(delta, field, None)
                if isinstance(value, str) and value:
                    return value
                elif isinstance(value, list):
                    parts = []
                    for part in value:
                        if isinstance(part, dict) and "text" in part:
                            parts.append(str(part["text"]))
                        elif isinstance(part, str):
                            parts.append(part)
                    if parts:
                        return "".join(parts)

            return ""

        try:
            stream = await self.client.chat.completions.create(**api_request)

            # Accumulate tool/function calls across streaming deltas.
            # OpenAI streams tool calls incrementally (name/arguments chunks).
            # 跨 streaming delta 累计 tool/function call。
            # OpenAI 会增量流式输出 tool call（name/arguments 分块）
            tool_calls_by_index: dict[int, dict[str, Any]] = {}
            last_model = model
            last_finish_reason = None
            yielded_final_meta = False

            def _accumulate_tool_call(index: int, tc_id: Any, name: Any, arguments_part: Any) -> None:
                entry = tool_calls_by_index.get(index, {"id": None, "name": None, "arguments": ""})
                if isinstance(tc_id, str) and tc_id:
                    entry["id"] = tc_id
                if isinstance(name, str) and name:
                    entry["name"] = name
                if isinstance(arguments_part, str) and arguments_part:
                    entry["arguments"] = (entry.get("arguments") or "") + arguments_part
                tool_calls_by_index[index] = entry

            async for chunk in stream:
                # Track actual model from chunks when available
                # 尽可能从 chunk 中跟踪实际使用的 model
                if getattr(chunk, "model", None):
                    last_model = chunk.model

                if chunk.choices:
                    choice = chunk.choices[0]
                    delta = choice.delta

                    # Track finish_reason (set on the final chunk)
                    # 跟踪 finish_reason（在最后一个 chunk 上设置）
                    if choice.finish_reason:
                        last_finish_reason = choice.finish_reason

                    # Newer SDK: tool_calls streaming (array of calls)
                    # 较新 SDK：tool_calls 流式输出（call 数组）
                    for tc in getattr(delta, "tool_calls", None) or []:
                        try:
                            idx = getattr(tc, "index", None)
                            if idx is None:
                                continue
                            fn = getattr(tc, "function", None)
                            if fn is None:
                                continue
                            _accumulate_tool_call(
                                int(idx),
                                getattr(tc, "id", None),
                                getattr(fn, "name", None),
                                getattr(fn, "arguments", None),
                            )
                        except Exception:
                            continue

                    # Older SDK: function_call streaming (single call)
                    # 较旧 SDK：function_call 流式输出（单次调用）
                    fc = getattr(delta, "function_call", None)
                    if fc is not None:
                        try:
                            _accumulate_tool_call(
                                0, None, getattr(fc, "name", None), getattr(fc, "arguments", None)
                            )
                        except Exception:
                            pass

                    text = _extract_delta_content(delta)
                    if text:
                        yield text

                # Check for usage in the chunk (usually the last one)
                # 检查 chunk 中的 usage（通常是最后一个）
                if chunk.usage:
                    stream_cache_read = 0
                    try:
                        stream_details = getattr(chunk.usage, "prompt_tokens_details", None)
                        if stream_details:
                            stream_cache_read = int(getattr(stream_details, "cached_tokens", 0) or 0)
                    except Exception:
                        pass
                    cost = self.estimate_cost(
                        chunk.usage.prompt_tokens,
                        chunk.usage.completion_tokens,
                        last_model,
                        cache_read_tokens=stream_cache_read,
                    )
                    meta = {
                        "usage": {
                            "total_tokens": chunk.usage.total_tokens,
                            "input_tokens": chunk.usage.prompt_tokens,
                            "output_tokens": chunk.usage.completion_tokens,
                            "cache_read_tokens": stream_cache_read,
                            "cost_usd": cost,
                        },
                        "model": last_model,
                        "provider": "openai",
                        "finish_reason": last_finish_reason or "stop",
                    }
                    # Include (possibly multiple) tool calls when present.
                    # 存在 tool call（可能多个）时一并附带。
                    if tool_calls_by_index:
                        ordered = [
                            tool_calls_by_index[i]
                            for i in sorted(tool_calls_by_index.keys())
                            if tool_calls_by_index[i].get("name")
                        ]
                        if ordered:
                            meta["function_call"] = {
                                "name": ordered[0].get("name"),
                                "arguments": ordered[0].get("arguments"),
                            }
                            meta["function_calls"] = ordered
                    yielded_final_meta = True
                    yield meta

            # If usage wasn't emitted but we did see tool calls, emit tool call metadata anyway.
            # 若未输出 usage 但确实看到了 tool call，仍然输出 tool call metadata。
            if not yielded_final_meta and tool_calls_by_index:
                ordered = [
                    tool_calls_by_index[i]
                    for i in sorted(tool_calls_by_index.keys())
                    if tool_calls_by_index[i].get("name")
                ]
                if ordered:
                    yield {
                        "model": last_model,
                        "provider": "openai",
                        "function_call": {
                            "name": ordered[0].get("name"),
                            "arguments": ordered[0].get("arguments"),
                        },
                        "function_calls": ordered,
                    }

        except openai.APIError as e:
            raise Exception(f"OpenAI API error: {e}")

    async def generate_embedding(
        self, text: str, model: str = "text-embedding-3-small"
    ) -> list[float]:
        """Generate embeddings using OpenAI API.

        使用 OpenAI API 生成 embedding。
        """

        try:
            response = await self.client.embeddings.create(model=model, input=text)
            return response.data[0].embedding
        except openai.APIError as e:
            raise Exception(f"OpenAI embedding error: {e}")

    def _choose_api_route(self, request: CompletionRequest, model_config) -> str:
        """Heuristic selection between Responses vs Chat APIs.

        - GPT-5-pro requires Responses API (only available there)
        - Prefer Chat for tool calling and strict JSON mode (more mature behavior).
        - Prefer Chat for GPT-5 and GPT-4.1 families (standard chat completions).
        - Prefer Responses for reasoning‑heavy tasks when supported and signaled by complexity.

        在 Responses 与 Chat API 之间做启发式选择。

        - GPT-5-pro 必须使用 Responses API（仅该处可用）
        - tool 调用与严格 JSON 模式优先用 Chat（行为更成熟）
        - GPT-5 与 GPT-4.1 系列优先用 Chat（标准 chat completions）
        - reasoning 繁重的 task，在支持且 complexity 有信号时优先用 Responses
        """
        # Multimodal content must use Chat Completions — Responses API doesn't support it
        # 多模态内容必须使用 Chat Completions —— Responses API 不支持
        has_image_content = any(
            isinstance(m.get("content"), list) for m in (request.messages or [])
        )
        if has_image_content:
            return "chat"

        # Reasoning models (o1, o3, o4) stay on Chat Completions unless chaining
        # 除非需要串联，reasoning model（o1、o3、o4）保持在 Chat Completions
        model_name_route = getattr(model_config, "model_id", "")
        is_reasoning = any(model_name_route.startswith(p) for p in ("o1", "o3", "o4"))
        if is_reasoning and not getattr(request, "previous_response_id", None):
            return "chat"

        # Prefer Responses API when chaining from a previous response
        # 从上一个 response 串联时优先 Responses API
        if getattr(request, "previous_response_id", None):
            return "responses"

        caps = getattr(model_config, "capabilities", None)
        model_name = getattr(model_config, "model_id", "")

        has_tools = bool(request.functions)
        wants_json = bool(
            request.response_format
            and request.response_format.get("type") == "json_object"
        )
        high_complexity = (request.complexity_score or 0.0) >= 0.7

        # Check requirements that need Chat Completions API BEFORE model family checks
        # 在判断 model 系列之前，先检查必须使用 Chat Completions API 的需求
        if has_tools and getattr(caps, "supports_tools", True):
            return "chat"
        if wants_json and getattr(caps, "supports_json_mode", True):
            return "chat"

        # GPT-5 family: Use Chat API by default (more reliable than Responses API)
        # Responses API has strict content type validation and empty output issues with reasoning models
        # GPT-5 系列：默认使用 Chat API（比 Responses API 更可靠）
        # Responses API 对 content type 校验严格，且对 reasoning model 存在空输出问题
        if model_name.startswith("gpt-5"):
            return "chat"

        # GPT-4.1 family uses standard chat completions API
        # GPT-4.1 系列使用标准 chat completions API
        if model_name.startswith("gpt-4."):
            return "chat"

        if high_complexity and getattr(caps, "supports_reasoning", False):
            return "responses"
        # Default preference: Chat Completions API
        # 默认偏好：Chat Completions API
        return "chat"

    @staticmethod
    def _has_tool_role_messages(messages: list) -> bool:
        """Check if messages contain role:'tool' entries (modern OpenAI format).

        检查消息中是否包含 role:'tool' 条目（现代 OpenAI 格式）。
        """
        return any(
            isinstance(m, dict) and m.get("role") == "tool"
            for m in messages
        )

    @staticmethod
    def _functions_to_tools_param(functions: list) -> list:
        """Convert legacy functions list to modern tools parameter format.

        Input:  [{"name": "...", "description": "...", "parameters": {...}}]
        Output: [{"type": "function", "function": {"name": "...", ...}}]

        将旧式 functions 列表转换为现代 tools 参数格式。

        输入： [{"name": "...", "description": "...", "parameters": {...}}]
        输出： [{"type": "function", "function": {"name": "...", ...}}]
        """
        tools = []
        for fn in functions:
            if not isinstance(fn, dict):
                continue
            if fn.get("type") == "function" and "function" in fn:
                tools.append(fn)
            else:
                tools.append({"type": "function", "function": fn})
        return tools

    async def _complete_responses_api(
        self, request: CompletionRequest, model: str
    ) -> CompletionResponse:
        """Call OpenAI Responses API and normalize to CompletionResponse.

        调用 OpenAI Responses API 并归一化为 CompletionResponse。
        """
        import time

        # Get model config for token limits
        # 获取 model 配置以确定 token 上限
        model_config = self.resolve_model_config(request)

        # Map OpenAI chat-style messages to Responses input blocks
        # 将 OpenAI chat 风格消息映射为 Responses 的 input block
        inputs: list[dict[str, Any]] = []
        for msg in request.messages:
            role = msg.get("role", "user")
            text = extract_text_from_content(msg.get("content", ""))
            content_block = {"type": "input_text", "text": text}
            inputs.append({"role": role, "content": [content_block]})

        # Clamp max_output_tokens to model limits (same as Chat API path)
        # 将 max_output_tokens 限制在 model 上限内（与 Chat API 路径一致）
        prompt_tokens_est = self.count_tokens(request.messages, model)
        safety_margin = 256
        model_context = getattr(model_config, "context_window", 8192)
        model_max_output = getattr(model_config, "max_tokens", model_context)
        requested_max = int(request.max_tokens) if request.max_tokens else model_max_output
        headroom = max(0, model_context - prompt_tokens_est - safety_margin)
        adjusted_max = max(1, min(requested_max, model_max_output, headroom))

        params: dict[str, Any] = {
            "model": model,
            "max_output_tokens": adjusted_max,
        }

        # Chain from previous response for cache reuse (OpenAI keeps
        # the full conversation server-side, so we only send the new message).
        # 基于上一个 response 串联以复用 cache（OpenAI 在服务端保留完整对话，
        # 因此只发送新消息）。
        if request.previous_response_id:
            params["previous_response_id"] = request.previous_response_id
            # Only send the last user message as incremental input
            # 仅把最后一条 user 消息作为增量输入
            last_user = [m for m in request.messages if m.get("role") == "user"]
            if last_user:
                params["input"] = [{"role": "user", "content": [{"type": "input_text", "text": extract_text_from_content(last_user[-1].get("content", ""))}]}]
            else:
                params["input"] = inputs
        else:
            params["input"] = inputs
            params["store"] = True  # Enable server-side storage for future chaining 注：启用服务端存储，供后续串联使用

        if request.reasoning_effort and any(model.startswith(p) for p in ("o1", "o3", "o4")):
            params["reasoning"] = {"effort": request.reasoning_effort}

        # Note: Responses API doesn't support response_format parameter
        # If needed, the fallback to Chat Completions API will handle it
        # 注意：Responses API 不支持 response_format 参数
        # 如需要，回退到 Chat Completions API 时会处理
        if request.functions:
            # Minimal pass-through using function blocks
            # 使用 function block 做最小化透传
            tools: list[dict[str, Any]] = []
            for fn in request.functions:
                if not isinstance(fn, dict):
                    continue
                name = fn.get("name")
                if not name:
                    continue
                tools.append(
                    {
                        "type": "function",
                        "name": name,
                        "description": fn.get("description"),
                        "parameters": fn.get("parameters", {}),
                    }
                )
            if tools:
                params["tools"] = tools

        start_time = time.time()
        try:
            response = await self.client.responses.create(**params)
        except openai.APIError as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f"OpenAI Responses API error for model {model}: "
                f"Status={getattr(e, 'status_code', 'unknown')}, "
                f"Type={type(e).__name__}, "
                f"Message={str(e)}"
            )
            raise Exception(f"OpenAI Responses API error: {e}") from e
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(
                f"Unexpected error calling Responses API for model {model}: "
                f"Type={type(e).__name__}, "
                f"Message={str(e)}"
            )
            raise

        # Prefer output_text when Responses API provides it directly
        # Responses API 直接提供 output_text 时优先使用
        direct_text = getattr(response, "output_text", None)
        if isinstance(direct_text, str) and direct_text.strip():
            content = direct_text.strip()
            try:
                raw = response.model_dump()
            except Exception:
                raw = {
                    "output": getattr(response, "output", None),
                    "usage": getattr(response, "usage", None),
                    "id": getattr(response, "id", None),
                    "model": getattr(response, "model", model),
                }
        else:
            # Extract text blocks; usage may be a dict-like
            # 提取文本 block；usage 可能是类 dict 对象
            text_parts: list[str] = []
            try:
                raw = response.model_dump()
            except Exception:
                raw = {
                    "output": getattr(response, "output", None),
                    "usage": getattr(response, "usage", None),
                    "id": getattr(response, "id", None),
                    "model": getattr(response, "model", model),
                }

            out = raw.get("output") or []
            if isinstance(out, list):
                for item in out:
                    if isinstance(item, dict):
                        if item.get("type") in ("output_text", "text"):
                            val = item.get("content") or item.get("text")
                            if isinstance(val, str) and val.strip():
                                text_parts.append(val.strip())
                        elif item.get("type") == "message":
                            for block in item.get("content", []) or []:
                                if isinstance(block, dict) and block.get("type") in (
                                    "output_text",
                                    "text",
                                ):
                                    val = block.get("text")
                                    if isinstance(val, str) and val.strip():
                                        text_parts.append(val.strip())

            content = "\n\n".join(text_parts).strip()

        usage = raw.get("usage") or {}
        try:
            input_tokens = int(usage.get("input_tokens", 0))
            output_tokens = int(usage.get("output_tokens", 0))
            total_tokens = int(usage.get("total_tokens", input_tokens + output_tokens))
        except Exception:
            # Fallback to estimation
            # 回退到估算
            input_tokens = self.count_tokens(request.messages, model)
            output_tokens = self.count_tokens(
                [{"role": "assistant", "content": content}], model
            )
            total_tokens = input_tokens + output_tokens

        cache_read = 0
        try:
            # Responses API uses input_tokens_details (not prompt_tokens_details)
            # Responses API 使用 input_tokens_details（而非 prompt_tokens_details）
            details = getattr(response.usage, "input_tokens_details", None) or \
                      getattr(response.usage, "prompt_tokens_details", None)
            if details:
                cache_read = int(getattr(details, "cached_tokens", 0) or 0)
        except Exception:
            pass

        latency_ms = int((time.time() - start_time) * 1000)
        cost = self.estimate_cost(
            input_tokens, output_tokens, self._resolve_alias(model),
            cache_read_tokens=cache_read,
        )

        # For Responses API, max_output_tokens is the effective limit
        # 对 Responses API 而言，max_output_tokens 即实际生效的上限
        effective_max = params.get("max_output_tokens", 2048)

        return CompletionResponse(
            content=content,
            model=raw.get("model", model),
            provider="openai",
            usage=TokenUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                estimated_cost=cost,
                cache_read_tokens=cache_read,
            ),
            finish_reason="stop",
            function_call=None,
            request_id=raw.get("id"),
            latency_ms=latency_ms,
            effective_max_completion=effective_max,
        )
