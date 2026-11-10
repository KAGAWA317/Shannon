package shannon.task

import future.keywords.if
import future.keywords.in

# Data Science team policy rules
# 数据科学团队策略规则

# Allow high-tier models for data science team
# 允许数据科学团队使用高档位模型
allow_model(model) if {
    input.context.team == "data-science"
    model in ["gpt-5-2025-08-07", "claude-sonnet-4-5-20250929", "claude-opus-4-1-20250805"]
}

# Higher token budget for data science team
# 数据科学团队拥有更高的 Token budget
max_tokens := 50000 if {
    input.context.team == "data-science"
}

# Allow all tools for data science team
# 允许数据科学团队使用所有 tool
allow_tool(_) if {
    input.context.team == "data-science"
}

# Decision for data science team
# 数据科学团队的决策
decision := {
    "allow": true,
    "reason": "Data science team has full access",
    "obligations": {
        "max_tokens": 50000,
        "allowed_models": ["gpt-5-2025-08-07", "claude-sonnet-4-5-20250929", "claude-opus-4-1-20250805"],
        "tool_restrictions": []
    }
} if {
    input.context.team == "data-science"
    input.mode in ["simple", "standard", "complex"]
}
