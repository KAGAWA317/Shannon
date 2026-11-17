from typing import Any

from fastapi import APIRouter, HTTPException, Request

from ..providers.base import ModelTier

router = APIRouter(prefix="/providers", tags=["providers"])


def _model_info_to_dict(mi) -> dict[str, Any]:
    return {
        "id": mi.id,
        "name": mi.name,
        "tier": mi.tier.value if isinstance(mi.tier, ModelTier) else str(mi.tier),
        "context_window": mi.context_window,
        "cost_per_1k_prompt_tokens": mi.cost_per_1k_prompt_tokens,
        "cost_per_1k_completion_tokens": mi.cost_per_1k_completion_tokens,
        "supports_tools": mi.supports_tools,
        "supports_streaming": mi.supports_streaming,
        "available": mi.available,
    }


@router.get("/models")
async def list_provider_models(
    request: Request, tier: str | None = None
) -> dict[str, list[dict[str, Any]]]:
    """Return live model registries per provider.

    返回各 provider 的实时 model registry。

    - Aggregates per configured provider using provider.list_models().
      使用 provider.list_models() 按已配置的 provider 聚合。
    - Optional `tier` query filters to small|medium|large.
      可选的 `tier` 查询参数可过滤 small|medium|large。
    """
    pm = getattr(request.app.state, "providers", None)
    if pm is None:
        raise HTTPException(status_code=503, detail="Provider manager not initialized")

    # Optional tier filter
    # 可选的 tier 过滤条件
    tier_enum: ModelTier | None = None
    if tier:
        v = tier.lower()
        if v == "small":
            tier_enum = ModelTier.SMALL
        elif v == "medium":
            tier_enum = ModelTier.MEDIUM
        elif v == "large":
            tier_enum = ModelTier.LARGE

    out: dict[str, list[dict[str, Any]]] = {}
    for ptype, provider in pm.providers.items():
        try:
            models = provider.list_models()
            if tier_enum is not None:
                models = [m for m in models if m.tier == tier_enum]
            out[ptype.value] = [_model_info_to_dict(m) for m in models]
        except Exception as e:
            out[ptype.value] = [{"error": str(e)}]
    return out
