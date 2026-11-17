import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import StreamingResponse
from llm_provider.base import extract_text_from_content, sanitize_completion_messages
from pydantic import BaseModel, Field

from ..metrics import TimedOperation, metrics
from ..providers.base import ModelTier

logger = logging.getLogger(__name__)

router = APIRouter()


class CompletionRequest(BaseModel):
    messages: list[dict[str, Any]]
    model_tier: str | None = Field(
        default="small", description="Model tier: small, medium, large"
    )
    specific_model: str | None = Field(
        default=None, description="Specific model to use"
    )
    temperature: float | None = Field(default=0.7, ge=0.0, le=2.0)
    max_tokens: int | None = Field(default=8192, ge=1, le=32000)
    tools: list[dict] | None = None
    cache_key: str | None = None
    stream: bool | None = Field(default=False, description="Enable SSE streaming")
    thinking: dict[str, Any] | None = Field(default=None, description="Anthropic extended thinking config")
    reasoning_effort: str | None = Field(default=None, description="OpenAI reasoning effort (minimal/low/medium/high)")
    session_id: str | None = Field(default=None, description="Session id; enables cross-turn rolling cache_control marker preservation")
    cache_source: str | None = Field(default=None, description="Call-site origin for prompt-cache TTL routing (e.g. tui, shanclaw, oneshot_cli)")


@router.post("/")
async def generate_completion(request: Request, body: CompletionRequest):
    """Generate a completion using the LLM service

    使用 LLM 服务生成 completion。
    """
    providers = request.app.state.providers

    # If no providers are configured, return a simple mock response to keep local dev flows working
    # 若未配置任何 provider，则返回简单的 mock response，保证本地开发流程可用
    try:
        if not providers or not providers.is_configured():
            # Simple deterministic mock based on last user message
            # 基于最后一条用户消息生成简单的确定性 mock
            user_text = ""
            for m in reversed(body.messages or []):
                if m.get("role") == "user":
                    user_text = extract_text_from_content(m.get("content", ""))
                    break
            reply = (
                "(mock) I received your request and would normally ask the configured LLM. "
                "No providers are configured, so this is a placeholder response."
            )
            # Minimal usage estimate
            # 最小化的 usage 估算
            prompt_tokens = max(len(user_text.split()), 1)
            completion_tokens = max(len(reply.split()), 1)
            total_tokens = prompt_tokens + completion_tokens
            result = {
                "provider": "mock",
                "model": "mock-model-v1",
                "output_text": reply,
                "usage": {
                    "input_tokens": prompt_tokens,
                    "output_tokens": completion_tokens,
                    "total_tokens": total_tokens,
                    "cost_usd": 0.0,
                },
                "cache_hit": False,
            }
            return result
    except Exception:
        # If provider check fails unexpectedly, fall through to normal path which will error gracefully
        # 若 provider 检查意外失败，则落回正常路径，由后者优雅报错
        pass

    # Convert tier string to enum
    # 把 tier 字符串转为枚举
    tier = None
    if body.model_tier:
        try:
            tier = ModelTier(body.model_tier.lower())
        except ValueError:
            raise HTTPException(
                status_code=400, detail=f"Invalid model tier: {body.model_tier}"
            )

    # Defensive sanitization: strip malformed history entries that cause provider 400/500s
    # 防御性清理：剔除会导致 provider 返回 400/500 的畸形历史条目
    original_count = len(body.messages)
    body.messages = sanitize_completion_messages(body.messages)
    if len(body.messages) != original_count:
        logger.warning(
            f"Sanitized completion messages: {original_count} → {len(body.messages)} "
            f"(dropped {original_count - len(body.messages)} malformed entries)"
        )

    if body.stream:
        return StreamingResponse(
            _stream_completion(request, body, providers, tier),
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "Connection": "keep-alive",
                "X-Accel-Buffering": "no",
            },
        )

    with TimedOperation("llm_completion", "llm") as timer:
        try:
            wf_id = (
                request.headers.get("X-Parent-Workflow-ID")
                or request.headers.get("X-Workflow-ID")
                or request.headers.get("x-workflow-id")
            )
            ag_id = request.headers.get("X-Agent-ID") or request.headers.get(
                "x-agent-id"
            )
            # Generate completion
            # 生成 completion
            result = await providers.generate_completion(
                messages=body.messages,
                tier=tier,
                specific_model=body.specific_model,
                temperature=body.temperature,
                max_tokens=body.max_tokens,
                tools=body.tools,
                workflow_id=wf_id,
                agent_id=ag_id,
                thinking=body.thinking,
                reasoning_effort=body.reasoning_effort,
                cache_source=body.cache_source or "completions_proxy",
                session_id=body.session_id,
            )
        except Exception as e:
            metrics.record_error("CompletionError", "llm")
            raise HTTPException(status_code=500, detail=str(e))

    # After timing context exits, record metrics with final duration
    # 在计时 context 退出后，用最终 duration 记录 metrics
    usage = result.get("usage", {}) or {}
    prompt_tokens = usage.get("input_tokens", usage.get("prompt_tokens", 0))
    completion_tokens = usage.get("output_tokens", usage.get("completion_tokens", 0))
    cost = usage.get("cost_usd", usage.get("cost", 0.0))

    # Check if response was cached (manager sets this flag)
    # 检查 response 是否命中 cache（由 manager 设置该标记）
    cache_hit = result.get("cached", False)

    metrics.record_llm_request(
        provider=result.get("provider", "unknown"),
        model=result.get("model", "unknown"),
        tier=body.model_tier or "unknown",
        cache_hit=cache_hit,
        duration=timer.duration or 0.0,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cost=cost,
    )

    return result


async def _stream_completion(request, body, providers, tier):
    """SSE generator for streaming completions.

    用于 streaming completion 的 SSE 生成器。
    """
    import time

    start_time = time.time()
    full_text = ""
    final_meta = None

    try:
        async for chunk in providers.stream_completion(
            messages=body.messages,
            tier=tier,
            specific_model=body.specific_model,
            temperature=body.temperature,
            max_tokens=body.max_tokens,
            tools=body.tools,
            thinking=body.thinking,
            reasoning_effort=body.reasoning_effort,
            cache_source=body.cache_source or "completions_proxy_stream",
            session_id=body.session_id,
        ):
            if isinstance(chunk, str):
                full_text += chunk
                event = {"type": "content_delta", "text": chunk}
                yield f"data: {json.dumps(event)}\n\n"
            elif isinstance(chunk, dict):
                # Final metadata from provider (usage, model, function_calls)
                # provider 返回的最终 metadata（usage、model、function_calls）
                final_meta = chunk

        # Build the done event with full response
        # 构造携带完整响应的 done 事件
        done_event = {
            "type": "done",
            "output_text": full_text,
        }

        if final_meta:
            done_event["provider"] = final_meta.get("provider", "unknown")
            done_event["model"] = final_meta.get("model", "unknown")
            done_event["usage"] = final_meta.get("usage")
            if final_meta.get("finish_reason"):
                done_event["finish_reason"] = final_meta["finish_reason"]
            if final_meta.get("function_call"):
                done_event["function_call"] = final_meta["function_call"]
            if final_meta.get("function_calls"):
                done_event["tool_calls"] = final_meta["function_calls"]
            # Forward the verbatim ordered content_blocks list so
            # streaming clients (TUI, future SSE consumers) round-trip thinking
            # blocks the same way non-stream complete() callers do. ShanClaw's
            # CompleteStream decodes the done event into CompletionResponse via
            # the existing json.Unmarshal — the new field auto-populates.
            # 原样转发有序的 content_blocks 列表，
            # 让 streaming 客户端（TUI、未来的 SSE 消费方）与非流式 complete() 调用方
            # 一样地往返 thinking block。ShanClaw 的
            # CompleteStream 通过现有 json.Unmarshal 把 done 事件解码为 CompletionResponse——
            # 新字段会自动填充。
            if final_meta.get("content_blocks"):
                done_event["content_blocks"] = final_meta["content_blocks"]

        yield f"data: {json.dumps(done_event)}\n\n"
        yield "data: [DONE]\n\n"

        # Record metrics
        # 记录 metrics
        duration = time.time() - start_time
        usage = (final_meta or {}).get("usage", {})
        metrics.record_llm_request(
            provider=(final_meta or {}).get("provider", "unknown"),
            model=(final_meta or {}).get("model", "unknown"),
            tier=body.model_tier or "unknown",
            cache_hit=False,
            duration=duration,
            prompt_tokens=usage.get("input_tokens", 0),
            completion_tokens=usage.get("output_tokens", 0),
            cost=usage.get("cost_usd", 0.0),
        )

    except Exception as e:
        logger.error(f"Streaming completion error: {e}", exc_info=True)
        error_event = {"type": "error", "message": str(e)}
        yield f"data: {json.dumps(error_event)}\n\n"
        yield "data: [DONE]\n\n"
