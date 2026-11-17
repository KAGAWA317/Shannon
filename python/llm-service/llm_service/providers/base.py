"""Legacy provider base definitions - re-exports from core for backward compatibility.

旧版 provider 基础定义 —— 为向后兼容从核心模块重新导出。
"""

from dataclasses import dataclass
from typing import Any

# Re-export ModelTier from canonical source to eliminate duplicate enum
# 从权威来源重新导出 ModelTier，消除重复 enum
from llm_provider.base import ModelTier

__all__ = ["ModelTier", "ModelInfo"]


@dataclass
class ModelInfo:
    """Model information for legacy API.

    旧版 API 使用的模型信息。
    """

    id: str
    name: str
    provider: Any  # Can be ProviderType enum or string 注：可以是 ProviderType 枚举或字符串
    # 可以是 ProviderType enum 或字符串
    tier: ModelTier
    context_window: int
    cost_per_1k_prompt_tokens: float
    cost_per_1k_completion_tokens: float
    supports_tools: bool = True
    supports_streaming: bool = True
    available: bool = True
