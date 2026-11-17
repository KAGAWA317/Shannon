from datetime import datetime

from fastapi import APIRouter, Request

router = APIRouter()


@router.get("/")
async def health_check(request: Request):
    """Basic health check endpoint

    基础健康检查 endpoint。
    """
    return {
        "status": "healthy",
        "service": "shannon-llm-service",
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/ready")
async def readiness_check(request: Request):
    """Readiness check - verifies all dependencies are accessible

    就绪检查 —— 验证所有依赖是否可访问。
    """
    providers = request.app.state.providers

    checks = {
        "providers": len(providers.providers) > 0,
        "models": len(providers.model_registry) > 0,
    }

    # In debug/dev mode, allow readiness without providers/models
    # 在 debug/dev 模式下，允许缺少 provider/model 时仍判定为就绪
    debug_mode = getattr(request.app.state.settings, "debug", False)
    all_ready = all(checks.values()) or debug_mode

    return {
        "ready": all_ready,
        "checks": checks,
        "debug_mode": debug_mode,
        "timestamp": datetime.utcnow().isoformat(),
    }


@router.get("/live")
async def liveness_check():
    """Liveness check - simple ping

    存活检查 —— 简单 ping。
    """
    return {"alive": True}
