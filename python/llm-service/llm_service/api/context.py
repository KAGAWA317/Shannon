from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

router = APIRouter()


class Message(BaseModel):
    role: str
    content: str


class CompressRequest(BaseModel):
    messages: list[Message]
    target_tokens: int | None = Field(default=400, ge=64, le=2000)


class CompressResponse(BaseModel):
    summary: str
    tokens_saved: int = 0
    model_used: str = "unknown"
    usage: dict[str, Any] = Field(default_factory=dict)


@router.post("/context/compress", response_model=CompressResponse)
async def compress_context(request: Request, body: CompressRequest) -> CompressResponse:
    providers = getattr(request.app.state, "providers", None)
    if not body.messages:
        raise HTTPException(status_code=400, detail="messages required")

    # If no providers, fallback to heuristic: join and truncate
    # 没有 providers 时退化为启发式：拼接后截断
    if not providers or not providers.is_configured():
        joined = "\n".join([m.content for m in body.messages[-10:]])
        # Very simple heuristic compression
        # 极简的启发式压缩
        summary = joined[: min(len(joined), 1000)]
        if len(joined) > 1000:
            summary += "..."
        return CompressResponse(
            summary=summary, tokens_saved=max(0, len(joined) - len(summary))
        )

    # Build prompt for compact summary
    # 构造用于生成精简摘要的 prompt
    sys = (
        "You compress long conversations into a concise, factual summary. "
        "Capture key decisions, entities, intents, and unresolved items."
    )
    # Use only the last N messages for cost, but summarize older context implicitly
    # 为控制成本只取最后 N 条消息，更早的 context 靠摘要隐含覆盖
    recent = body.messages[-20:]
    user = "\n".join([f"{m.role}: {m.content}" for m in recent])

    try:
        from ..providers.base import ModelTier

        wf_id = (
            request.headers.get("X-Parent-Workflow-ID")
            or request.headers.get("X-Workflow-ID")
            or request.headers.get("x-workflow-id")
        )
        ag_id = request.headers.get("X-Agent-ID") or request.headers.get("x-agent-id")

        result = await providers.generate_completion(
            messages=[
                {"role": "system", "content": sys},
                {"role": "user", "content": user},
            ],
            tier=ModelTier.SMALL,
            max_tokens=int(body.target_tokens or 400),
            temperature=0.2,
            workflow_id=wf_id,
            agent_id=ag_id,
            cache_source="context_summary",
        )
        completion = (result.get("output_text") or "").strip()
        usage = result.get("usage", {})
        tokens = usage.get("total_tokens", 0)
        est_original = max(tokens * 2, 1000)  # loose heuristic 注：粗略估算
        saved = max(0, est_original - tokens)
        return CompressResponse(
            summary=completion or "",
            tokens_saved=saved,
            model_used=result.get("model", "unknown"),
            usage=usage,
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
