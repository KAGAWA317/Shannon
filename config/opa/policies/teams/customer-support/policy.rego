package shannon.task

import future.keywords.if
import future.keywords.in

# Customer Support team policy rules
# 客户支持团队策略规则

# Only allow mini models for support team
# 支持团队仅允许使用 mini 模型
allow_model(model) if {
    input.context.team == "support"
    model in ["gpt-5-mini-2025-08-07", "claude-haiku-4-5-20251001"]
}

# Limited token budget for support team
# 支持团队的 Token budget 有限
max_tokens := 5000 if {
    input.context.team == "support"
}

# Deny dangerous tools for support team
# 禁止支持团队使用危险 tool
deny_tool(tool) if {
    input.context.team == "support"
    tool in ["database_write", "code_execution", "system_command"]
}

# Allow safe tools only
# 仅允许安全 tool
allow_tool(tool) if {
    input.context.team == "support"
    not deny_tool(tool)
    tool in ["web_search", "database_read", "knowledge_base"]
}

# Decision for support team
# 支持团队的决策
decision := {
    "allow": true,
    "reason": "Support team has limited access",
    "obligations": {
        "max_tokens": 5000,
        "allowed_models": ["gpt-5-mini-2025-08-07", "claude-haiku-4-5-20251001"],
        "tool_restrictions": ["database_write", "code_execution", "system_command"]
    }
} if {
    input.context.team == "support"
    input.mode in ["simple", "standard"]
}
