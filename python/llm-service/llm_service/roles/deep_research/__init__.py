"""Deep Research role presets for ResearchWorkflow.
Deep Research 角色预设，供 ResearchWorkflow 使用。

This module contains specialized roles for:
本模块包含以下专用角色：
- deep_research_agent: Main subtask agent for deep research
- deep_research_agent：深度研究的主要 subtask agent
- research_refiner: Query expansion and research planning
- research_refiner：查询扩展与研究规划
- domain_discovery: Company domain identification
- domain_discovery：公司域名识别
- domain_prefetch: Website content pre-fetching
- domain_prefetch：网站内容预抓取
"""

from .deep_research_agent import DEEP_RESEARCH_AGENT_PRESET
from .quick_research_agent import QUICK_RESEARCH_AGENT_PRESET
from .research_refiner import RESEARCH_REFINER_PRESET
from .research_supervisor import RESEARCH_SUPERVISOR_IDENTITY, DOMAIN_ANALYSIS_HINT
from .domain_discovery import DOMAIN_DISCOVERY_PRESET
from .domain_prefetch import DOMAIN_PREFETCH_PRESET

__all__ = [
    "DEEP_RESEARCH_AGENT_PRESET",
    "QUICK_RESEARCH_AGENT_PRESET",
    "RESEARCH_REFINER_PRESET",
    "RESEARCH_SUPERVISOR_IDENTITY",
    "DOMAIN_ANALYSIS_HINT",
    "DOMAIN_DISCOVERY_PRESET",
    "DOMAIN_PREFETCH_PRESET",
]
