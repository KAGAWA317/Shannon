from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

router = APIRouter(prefix="/mcp", tags=["mcp-mock"])


class MCPInvokeRequest(BaseModel):
    function: str
    args: dict[str, Any] | None = None


@router.post("/mock")
async def mcp_mock(req: MCPInvokeRequest) -> dict[str, Any]:
    """Simple MCP mock endpoint used for smoke testing.

    - function: name of the function (supports only "echo")
    - args: payload object

    用于冒烟测试的简单 MCP mock endpoint。
    """
    if req.function.lower() == "echo":
        return {"ok": True, "function": req.function, "echo": req.args or {}}
    raise HTTPException(status_code=400, detail="Unknown function")
