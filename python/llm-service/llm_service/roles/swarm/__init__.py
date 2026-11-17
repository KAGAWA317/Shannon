"""Swarm V2 prompt definitions.
Swarm V2 prompt 定义。

All swarm-related prompts live here, separated from API endpoint logic.
所有 swarm 相关 prompt 都集中在此，与 API 端点逻辑分离。
- agent_protocol.py: Agent's core protocol (actions, phases, rules)
- agent_protocol.py：Agent 的核心协议（动作、阶段、规则）
- lead_protocol.py: Lead's orchestration and evaluation prompts
- lead_protocol.py：Lead 的编排与评估 prompt
- role_prompts.py: Role-specific prompt snippets for swarm agents
- role_prompts.py：swarm agent 的角色专属 prompt 片段
"""

from .agent_protocol import (
    AGENT_LOOP_SYSTEM_PROMPT,
    COMMON_PROTOCOL_BASE,
    get_work_protocol,
)
from .lead_protocol import LEAD_SYSTEM_PROMPT
from .role_prompts import SWARM_ROLE_PROMPTS, get_swarm_role_catalog

__all__ = [
    "AGENT_LOOP_SYSTEM_PROMPT",
    "COMMON_PROTOCOL_BASE",
    "get_work_protocol",
    "LEAD_SYSTEM_PROMPT",
    "SWARM_ROLE_PROMPTS",
    "get_swarm_role_catalog",
]
