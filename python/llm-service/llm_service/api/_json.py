import json
import re
from typing import Any, Dict, Optional


def extract_json_block(text: str) -> Optional[Dict[str, Any]]:
    """Extract JSON from an LLM response.

    从模型 response 中提取 JSON.

    Order: whole text → fenced JSON block → first {...} block.
    顺序：整体文本 → 围栏 JSON 块 → 首个 {...} 块.
    """
    try:
        return json.loads(text)
    except Exception:
        pass
    # Try fenced JSON or first {...} block
    # 尝试 fenced JSON 或第一个 {...} 块
    fence = re.search(r"```(?:json)?\s*(\{[\s\S]*?\})\s*```", text, re.IGNORECASE)
    blob = fence.group(1) if fence else None
    if not blob:
        brace = re.search(r"\{[\s\S]*\}", text)
        blob = brace.group(0) if brace else None
    if not blob:
        return None
    try:
        return json.loads(blob)
    except Exception:
        return None
