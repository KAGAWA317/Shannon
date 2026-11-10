"""xAI Provider implementation.

Provides a thin wrapper around the xAI (Grok) REST API which is intentionally
OpenAI-compatible. We keep the logic simple and reuse the OpenAI chat
completions surface while accounting for a few xAI-specific quirks (reasoning
models ignoring certain parameters, etc.).

xAI provider 实现。

对有意保持 OpenAI 兼容的 xAI（Grok）REST API 做一层薄封装。逻辑保持简单，
复用 OpenAI chat completions 接口，同时处理若干 xAI 特有的怪癖
（reasoning model 忽略某些参数等）。
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

from openai import AsyncOpenAI
from tenacity import retry, stop_after_attempt, wait_exponential

from .base import (
    CompletionRequest,
    CompletionResponse,
    LLMProvider,
    TokenCounter,
    TokenUsage,
)


class XAIProvider(LLMProvider):
    """xAI provider using the OpenAI-compatible chat completions API.

    使用 OpenAI 兼容 chat completions API 的 xAI provider。
    """

    def __init__(self, config: Dict[str, Any]):
        api_key = config.get("api_key") or os.getenv("XAI_API_KEY")
        if not api_key:
            raise ValueError("xAI API key not provided")

        base_url = config.get("base_url", "https://api.x.ai/v1").rstrip("/")
        timeout = int(config.get("timeout", 60) or 60)

        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=timeout)
        self.base_url = base_url
        # Prefer Responses API when set in config or env (XAI_PREFER_RESPONSES=true)
        # 当 config 或环境变量设置时优先 Responses API（XAI_PREFER_RESPONSES=true）
        try:
            prefer = config.get("prefer_responses")
        except Exception:
            prefer = None
        prefer_env = os.getenv("XAI_PREFER_RESPONSES", "").strip().lower() in ("1", "true", "yes")
        self.prefer_responses = bool(prefer) if isinstance(prefer, bool) else prefer_env

        # Preserve original config but ensure a provider name for downstream logs
        # 保留原始 config，但确保 provider 名称存在，供下游日志使用
        effective_config = dict(config)
        effective_config.setdefault("name", "xai")

        super().__init__(effective_config)

    def _initialize_models(self) -> None:
        self._load_models_from_config(allow_empty=True)
        if not self.models:
            self._add_default_models()

    def _add_default_models(self) -> None:
        """Populate a minimal catalog if models were not provided via config.

        未通过 config 提供 model 时，填充一个最小目录。
        """

        defaults: Dict[str, Dict[str, Any]] = {
            "grok-4-1-fast-non-reasoning": {
                "model_id": "grok-4-1-fast-non-reasoning",
                "tier": "small",
                "context_window": 2000000,
                "max_tokens": 128000,
                "supports_functions": True,
                "supports_streaming": True,
                "supports_reasoning": False,
            },
            "grok-4-1-fast-reasoning": {
                "model_id": "grok-4-1-fast-reasoning",
                "tier": "large",
                "context_window": 2000000,
                "max_tokens": 128000,
                "supports_functions": True,
                "supports_streaming": True,
                "supports_reasoning": True,
            },
        }

        for alias, meta in defaults.items():
            self.models[alias] = self._make_model_config("xai", alias, meta)

    def count_tokens(self, messages: List[Dict[str, Any]], model: str) -> int:
        return TokenCounter.count_messages_tokens(messages, model)

    def _resolve_alias(self, model_id: str) -> str:
        for alias, cfg in self.models.items():
            if cfg.model_id == model_id:
                return alias
        return model_id

    def _supports_reasoning(self, model_alias: str) -> bool:
        config = self.models.get(model_alias)
        if not config:
            return False
        caps = getattr(config, "capabilities", None)
        return bool(getattr(caps, "supports_reasoning", False))

    def _sanitize_messages(self, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Remove tool_calls and non-string content that xAI API rejects.

        移除 xAI API 会拒绝的 tool_calls 与非字符串 content。
        """
        sanitized = []
        allowed_roles = {"system", "user", "assistant"}
        for msg in messages:
            role = str(msg.get("role", "user")).strip()
            if role not in allowed_roles:
                continue
            # xAI Chat API tolerates system, but to maximize compatibility, map system→user with prefix
            # xAI Chat API 可以容忍 system，但为最大化兼容性，将 system 映射为 user 并加前缀
            mapped_role = "user" if role == "system" else role
            clean_msg = {"role": mapped_role}

            # Ensure content is a string - xAI rejects non-string content
            # 确保 content 为字符串 —— xAI 拒绝非字符串 content
            content = msg.get("content")
            if content is None:
                continue  # Skip messages with no content
                # 跳过没有 content 的消息
            elif isinstance(content, str):
                clean_msg["content"] = content
            elif isinstance(content, list):
                # Flatten multi-part content to text; degrade attachments to descriptions
                # 把多段 content 压平为文本；附件降级为描述
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
                clean_msg["content"] = " ".join(text_parts)
            else:
                clean_msg["content"] = str(content)

            # xAI rejects messages with tool_calls, function_call, name fields
            # Don't copy these fields over
            # xAI 拒绝带 tool_calls、function_call、name 字段的消息
            # 不要复制这些字段

            # Drop empty assistant turns that some providers reject
            # 丢弃部分 provider 会拒绝的空 assistant 轮次
            if mapped_role == "assistant" and not clean_msg.get("content"):
                continue
            if role == "system":
                # Prefix system content for clarity after role mapping
                # 角色映射后给 system 内容加前缀，便于辨识
                clean_msg["content"] = f"System: {clean_msg['content']}" if clean_msg.get("content") else "System:"
            sanitized.append(clean_msg)
        return sanitized

    @retry(
        stop=stop_after_attempt(3), wait=wait_exponential(multiplier=0.5, min=1, max=8)
    )
    async def complete(self, request: CompletionRequest) -> CompletionResponse:
        model_config = self.resolve_model_config(request)
        model_id = model_config.model_id
        model_alias = self._resolve_alias(model_id)
        supports_reasoning = self._supports_reasoning(model_alias)

        # Prefer Responses API for reasoning models or when forced via config/env
        # reasoning model 或 config/env 强制时优先 Responses API
        if supports_reasoning or self.prefer_responses:
            import logging
            logging.getLogger(__name__).info(
                f"xAI using Responses API (supports_reasoning={supports_reasoning}, prefer_responses={self.prefer_responses})"
            )
            return await self._complete_responses_api(request, model_id, model_alias)

        # Sanitize messages to remove tool calls and non-string content
        # 清理消息，移除 tool call 与非字符串 content
        clean_messages = self._sanitize_messages(request.messages)

        payload: Dict[str, Any] = {
            "model": model_id,
            "messages": clean_messages,
            "temperature": request.temperature,
            "top_p": request.top_p,
        }

        if request.max_tokens:
            payload["max_tokens"] = request.max_tokens

        # xAI uses tools format (OpenAI-compatible), not functions
        # xAI 使用 tools 格式（OpenAI 兼容），而非 functions
        if request.functions and model_config.supports_functions:
            # Convert functions to tools format (idempotent if already tools)
            tools: List[Dict[str, Any]] = []
            for func in request.functions:
                if isinstance(func, dict) and func.get("type") == "function" and isinstance(func.get("function"), dict):
                    tools.append(func)
                else:
                    tools.append({"type": "function", "function": func})
            if tools:
                payload["tools"] = tools

            # Convert function_call to tool_choice if provided
            # 若提供 function_call 则转为 tool_choice
            if request.function_call:
                if isinstance(request.function_call, str):
                    if request.function_call == "auto":
                        payload["tool_choice"] = "auto"
                    elif request.function_call == "none":
                        payload["tool_choice"] = "none"
                    else:
                        payload["tool_choice"] = {"type": "function", "function": {"name": request.function_call}}
                elif isinstance(request.function_call, dict) and "name" in request.function_call:
                    payload["tool_choice"] = {"type": "function", "function": request.function_call}

        # xAI does not support response_format, user, or seed - omit entirely
        # These fields cause 400 Bad Request
        # xAI 不支持 response_format、user、seed —— 全部省略
        # 这些字段会触发 400 Bad Request

        # Only send penalty/stop params for non-reasoning models AND only if non-default
        # 仅对非 reasoning model 发送 penalty/stop 参数，且仅当非默认值
        if not supports_reasoning:
            if request.frequency_penalty != 0.0:
                payload["frequency_penalty"] = request.frequency_penalty
            if request.presence_penalty != 0.0:
                payload["presence_penalty"] = request.presence_penalty
            if request.stop:
                payload["stop"] = (
                    [request.stop] if isinstance(request.stop, str) else request.stop
                )

        start = time.time()
        try:
            response = await self.client.chat.completions.create(**payload)
        except Exception as exc:
            # Attempt Responses API fallback before failing
            # 失败前先尝试回退到 Responses API
            try:
                return await self._complete_responses_api(request, model_id, model_alias)
            except Exception as resp_exc:
                # Log the full payload and error details for debugging, then raise original
                # 记录完整 payload 与错误细节以便调试，然后抛出原始错误
                import json
                import logging
                logger = logging.getLogger(__name__)
                try:
                    logger.error(
                        f"xAI Chat API error. Payload: {json.dumps(payload, indent=2)}"
                    )
                except Exception:
                    logger.error("xAI Chat API error; failed to serialize payload for logs")
                # Try to extract response body if it's an HTTP error
                # 若是 HTTP 错误，尝试提取 response body
                error_detail = str(exc)
                if hasattr(exc, 'response'):
                    try:
                        error_detail = exc.response.text if hasattr(exc.response, 'text') else str(exc)
                    except Exception:
                        pass
                logger.error(f"xAI error detail: {error_detail}")
                logger.error(f"Responses API fallback also failed: {resp_exc}")
                raise Exception(f"xAI API error ({self.base_url}): {exc}")
        latency_ms = int((time.time() - start) * 1000)

        choice = response.choices[0]
        message = choice.message
        content = (message.content or "") if message else ""

        prompt_tokens = 0
        completion_tokens = 0
        total_tokens = 0
        cached_tokens = 0

        usage = getattr(response, "usage", None)
        if usage:
            try:
                prompt_tokens = int(getattr(usage, "prompt_tokens", 0))
                completion_tokens = int(getattr(usage, "completion_tokens", 0))
                total_tokens = int(
                    getattr(usage, "total_tokens", prompt_tokens + completion_tokens)
                )
                # xAI prompt caching: cached_tokens nested under prompt_tokens_details
                # xAI prompt cache：cached_tokens 嵌在 prompt_tokens_details 下
                details = getattr(usage, "prompt_tokens_details", None)
                if details is not None:
                    cached_tokens = int(getattr(details, "cached_tokens", 0) or 0)
            except Exception:
                prompt_tokens = completion_tokens = total_tokens = cached_tokens = 0

        if total_tokens == 0:
            prompt_tokens = self.count_tokens(request.messages, model_id)
            completion_tokens = self.count_tokens(
                [{"role": "assistant", "content": content}], model_id
            )
            total_tokens = prompt_tokens + completion_tokens

        estimated_cost = self.estimate_cost(
            prompt_tokens, completion_tokens, model_alias, cache_read_tokens=cached_tokens
        )

        finish_reason = getattr(choice, "finish_reason", None) or "stop"
        function_call: Optional[Dict[str, Any]] = None
        if message and hasattr(message, "function_call"):
            function_call = message.function_call  # type: ignore[assignment]
        elif message and hasattr(message, "tool_calls"):
            tool_calls = getattr(message, "tool_calls", None)
            if tool_calls:
                first_tool = tool_calls[0]
                if isinstance(first_tool, dict):
                    function_call = first_tool

        created_ts = getattr(response, "created", None)
        created_at = (
            datetime.utcfromtimestamp(created_ts)
            if isinstance(created_ts, (int, float))
            else None
        )

        usage_payload = TokenUsage(
            input_tokens=prompt_tokens,
            output_tokens=completion_tokens,
            total_tokens=total_tokens,
            estimated_cost=estimated_cost,
            cache_read_tokens=cached_tokens,
        )

        response_obj = CompletionResponse(
            content=content,
            model=model_id,
            provider="xai",
            usage=usage_payload,
            finish_reason=finish_reason,
            function_call=function_call,
            request_id=getattr(response, "id", None),
            latency_ms=latency_ms,
        )

        if created_at:
            response_obj.created_at = created_at

        return response_obj

    async def _complete_responses_api(
        self, request: CompletionRequest, model_id: str, model_alias: str
    ) -> CompletionResponse:
        """Call xAI Responses API and normalize to CompletionResponse.

        调用 xAI Responses API 并归一化为 CompletionResponse。
        """
        import time

        start = time.time()

        # Map sanitized messages to Responses input blocks
        # 将清理后的消息映射为 Responses 的 input block
        inputs: List[Dict[str, Any]] = []
        for msg in self._sanitize_messages(request.messages):
            role = msg.get("role", "user")
            text = msg.get("content", "")
            content_block = {"type": "input_text", "text": text}
            inputs.append({"role": role, "content": [content_block]})

        params: Dict[str, Any] = {
            "model": model_id,
            "input": inputs,
            "max_output_tokens": request.max_tokens or 2048,
        }
        if request.temperature is not None:
            params["temperature"] = request.temperature

        # Convert functions to Responses API tools shape
        # 将 functions 转换为 Responses API 的 tools 形状
        if request.functions and self.models.get(model_alias, None) and self.models[model_alias].supports_functions:
            tools: List[Dict[str, Any]] = []
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

        try:
            response = await self.client.responses.create(**params)
        except Exception as exc:
            import json
            import logging
            logger = logging.getLogger(__name__)
            try:
                logger.error(
                    f"xAI Responses API error. Params: {json.dumps(params, indent=2)}"
                )
            except Exception:
                logger.error("xAI Responses API error; failed to serialize params for logs")
            raise Exception(f"xAI Responses API error ({self.base_url}): {exc}")

        # Extract content and usage
        # 提取内容与用量
        try:
            raw = response.model_dump()
        except Exception:
            raw = {
                "output": getattr(response, "output", None),
                "usage": getattr(response, "usage", None),
                "id": getattr(response, "id", None),
                "model": getattr(response, "model", model_id),
            }

        text_parts: List[str] = []
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
                            if isinstance(block, dict) and block.get("type") in ("output_text", "text"):
                                val = block.get("text")
                                if isinstance(val, str) and val.strip():
                                    text_parts.append(val.strip())

        content = "\n\n".join(text_parts).strip()

        usage = raw.get("usage") or {}
        cached_tokens = 0
        try:
            input_tokens = int(usage.get("input_tokens", 0))
            output_tokens = int(usage.get("output_tokens", 0))
            total_tokens = int(usage.get("total_tokens", input_tokens + output_tokens))
            # xAI Responses API: cached_tokens nested under input_tokens_details
            # xAI Responses API：cached_tokens 嵌在 input_tokens_details 下
            details = usage.get("input_tokens_details") or {}
            if isinstance(details, dict):
                cached_tokens = int(details.get("cached_tokens", 0) or 0)
        except Exception:
            input_tokens = self.count_tokens(request.messages, model_id)
            output_tokens = self.count_tokens(
                [{"role": "assistant", "content": content}], model_id
            )
            total_tokens = input_tokens + output_tokens
            cached_tokens = 0

        latency_ms = int((time.time() - start) * 1000)
        cost = self.estimate_cost(
            input_tokens, output_tokens, model_alias, cache_read_tokens=cached_tokens
        )

        return CompletionResponse(
            content=content,
            model=raw.get("model", model_id),
            provider="xai",
            usage=TokenUsage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
                estimated_cost=cost,
                cache_read_tokens=cached_tokens,
            ),
            finish_reason="stop",
            function_call=None,
            request_id=raw.get("id"),
            latency_ms=latency_ms,
        )

    async def stream_complete(self, request: CompletionRequest) -> AsyncIterator[str]:
        model_config = self.resolve_model_config(request)
        model_id = model_config.model_id
        model_alias = self._resolve_alias(model_id)
        supports_reasoning = self._supports_reasoning(model_alias)

        # Prefer Responses API streaming for reasoning models or when forced
        # reasoning model 或被强制时优先 Responses API streaming
        if supports_reasoning or getattr(self, "prefer_responses", False):
            # Map sanitized messages to Responses input blocks
            # 将清理后的消息映射为 Responses 的 input block
            inputs: List[Dict[str, Any]] = []
            for msg in self._sanitize_messages(request.messages):
                role = msg.get("role", "user")
                text = msg.get("content", "")
                content_block = {"type": "input_text", "text": text}
                inputs.append({"role": role, "content": [content_block]})

            params: Dict[str, Any] = {
                "model": model_id,
                "input": inputs,
                "max_output_tokens": request.max_tokens or 2048,
            }
            if request.temperature is not None:
                params["temperature"] = request.temperature

            # Convert functions → tools for Responses
            # 为 Responses 将 functions 转换为 tools
            if request.functions and model_config.supports_functions:
                tools: List[Dict[str, Any]] = []
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

            try:
                async with self.client.responses.stream(**params) as stream:
                    async for event in stream:
                        etype = getattr(event, "type", "")
                        if "output_text.delta" in etype:
                            delta = getattr(event, "delta", None)
                            if isinstance(delta, str) and delta:
                                yield delta
                            else:
                                text_val = getattr(event, "text", None)
                                if isinstance(text_val, str) and text_val:
                                    yield text_val
                return
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(
                    f"xAI Responses streaming failed, falling back to Chat: {e}"
                )

        # Sanitize messages to remove tool calls and non-string content
        # 清理消息，移除 tool call 与非字符串 content
        clean_messages = self._sanitize_messages(request.messages)

        payload: Dict[str, Any] = {
            "model": model_id,
            "messages": clean_messages,
            "temperature": request.temperature,
            "stream": True,
            # Note: xAI does NOT support stream_options parameter (returns 400 error)
            # xAI includes usage data in streaming chunks by default
            # 注意：xAI 不支持 stream_options 参数（会返回 400 错误）
            # xAI 默认在 streaming chunk 中包含 usage 数据
        }

        if request.max_tokens:
            payload["max_tokens"] = request.max_tokens

        # xAI uses tools format (OpenAI-compatible), not functions
        # xAI 使用 tools 格式（OpenAI 兼容），而非 functions
        if request.functions and model_config.supports_functions:
            # Convert functions to tools format (idempotent if already tools)
            tools: List[Dict[str, Any]] = []
            for func in request.functions:
                if isinstance(func, dict) and func.get("type") == "function" and isinstance(func.get("function"), dict):
                    tools.append(func)
                else:
                    tools.append({"type": "function", "function": func})
            if tools:
                payload["tools"] = tools

            # Convert function_call to tool_choice if provided
            # 若提供 function_call 则转为 tool_choice
            if request.function_call:
                if isinstance(request.function_call, str):
                    if request.function_call == "auto":
                        payload["tool_choice"] = "auto"
                    elif request.function_call == "none":
                        payload["tool_choice"] = "none"
                    else:
                        payload["tool_choice"] = {"type": "function", "function": {"name": request.function_call}}
                elif isinstance(request.function_call, dict) and "name" in request.function_call:
                    payload["tool_choice"] = {"type": "function", "function": request.function_call}

        # xAI does not support response_format, user, or seed - omit entirely
        # xAI 不支持 response_format、user、seed —— 全部省略

        # Only send stop for non-reasoning models if provided
        # 仅对非 reasoning model 在提供时发送 stop
        if not supports_reasoning and request.stop:
            payload["stop"] = (
                [request.stop] if isinstance(request.stop, str) else request.stop
            )

        try:
            stream = await self.client.chat.completions.create(**payload)
            async for chunk in stream:
                # Yield text content if present
                # 有文本内容时产出
                if chunk.choices and len(chunk.choices) > 0:
                    delta = chunk.choices[0].delta
                    if delta and getattr(delta, "content", None):
                        yield delta.content

                # Check for usage in the chunk (usually the last one)
                # 检查 chunk 中的 usage（通常是最后一个）
                if chunk.usage:
                    yield {
                        "usage": {
                            "total_tokens": chunk.usage.total_tokens,
                            "input_tokens": chunk.usage.prompt_tokens,
                            "output_tokens": chunk.usage.completion_tokens,
                        },
                        "model": chunk.model,
                        "provider": "xai",
                    }
        except Exception as exc:
            raise Exception(f"xAI streaming error ({self.base_url}): {exc}")
