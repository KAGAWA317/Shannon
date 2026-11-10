package shannon.task

# Shannon Task Execution Policy - Phase 0.5 Basic Allowlist
# Shannon task 执行策略 - 阶段 0.5 基础白名单
# This implements basic security controls for Shannon AI agent task execution
# 为 Shannon AI Agent task 执行实现基础安全控制
# Mode: allowlist-first approach with environment-specific rules
# 模式：白名单优先，并配合按环境区分的规则

import future.keywords.in

# Default decision when no specific allow rules match
# 无匹配的允许规则时的默认决策
default decision := {
    "allow": false,
    "reason": "default deny - no matching allow rule",
    "require_approval": false
}

# === DENY PRECEDENCE RULES ===
# === 拒绝优先规则 ===
# Deny rules always take precedence over allow rules
# 拒绝规则始终优先于允许规则
# If any deny rule matches, the request is denied regardless of allows
# 若任一拒绝规则匹配，则无论允许规则如何，request 都会被拒绝

decision := {
    "allow": false,
    "reason": reason,
    "require_approval": false
} {
    some reason
    deny[reason]
}

# === DEVELOPMENT ENVIRONMENT RULES ===
# === 开发环境规则 ===
# Allow all operations in development environment (for testing and development)
# 在开发环境中允许所有操作（用于测试与开发）
decision := {
    "allow": true,
    "reason": "development environment - all operations allowed",
    "require_approval": false
} {
    input.environment == "dev"
    input.token_budget <= 10000  # Still enforce reasonable budget limits
}

# === PRODUCTION SAFETY RULES ===
# === 生产环境安全规则 ===

# Simple mode operations - generally safe, minimal resources
# 简单模式操作 - 通常安全，资源占用极少
decision := {
    "allow": true,
    "reason": "simple mode operation - low risk",
    "require_approval": false
} {
    input.mode == "simple"
    input.token_budget <= 1000
    safe_query_check
}

# Standard operations for authenticated users
# 已通过 authentication 的用户的标准操作
decision := {
    "allow": true,
    "reason": "standard operation for authorized user",
    "require_approval": false
} {
    input.mode == "standard"
    input.user_id != ""
    allowed_users[input.user_id]
    input.token_budget <= 5000
    not suspicious_query
}

# Complex operations - require approval in production
# 复杂操作 - 生产环境需审批
decision := {
    "allow": true,
    "reason": "complex operation approved for privileged user",
    "require_approval": input.environment == "prod"
} {
    input.mode == "complex"
    privileged_users[input.user_id]
    input.token_budget <= 15000
    not dangerous_query
}

# === USER ALLOWLISTS ===
# === 用户白名单 ===

# Basic approved users for standard operations
# 可执行标准操作的基础授权用户
# TODO: Customize these user IDs for your organization
# TODO：请按你的组织自定义这些用户 ID
allowed_users := {
    "admin",
    "test_user", 
    "orchestrator",
    "shannon_system",
    "api_user",
    "dev_team_lead",
    "data_scientist",
    "product_manager"
}

# Privileged users who can perform complex operations
# 可执行复杂操作的特权用户
# TODO: Restrict to actual senior staff in production
# TODO：生产环境中应仅限实际的高级人员
privileged_users := {
    "admin",
    "shannon_system", 
    "senior_engineer",
    "team_lead",
    "security_admin"
}

# === QUERY PATTERN CONTROLS ===
# === 查询模式控制 ===

# Safe query check - contains safe patterns
# 安全查询检查 - 命中安全模式
safe_query_check {
    count([pattern | 
        safe_patterns[pattern]
        contains(lower(input.query), pattern)
    ]) > 0
}

# Check for suspicious queries
# 检查可疑查询
suspicious_query {
    count([pattern | 
        suspicious_patterns[pattern]
        contains(lower(input.query), pattern)
    ]) > 0
}

# Check for dangerous queries
# 检查危险查询
dangerous_query {
    count([pattern | 
        dangerous_patterns[pattern]
        contains(lower(input.query), pattern)
    ]) > 0
}

# Safe query patterns that are always allowed
# 始终允许的安全查询模式
safe_patterns := {
    "what is",
    "how to",
    "explain",
    "help me understand",
    "summarize",
    "translate",
    "calculate",
    "convert"
}

# Suspicious patterns that require extra scrutiny
# 需额外审查的可疑模式
suspicious_patterns := {
    "delete",
    "remove",
    "hack",
    "bypass",
    "override",
    "admin",
    "root",
    "sudo"
}

# Dangerous patterns that are blocked in production
# 生产环境中会被阻止的危险模式
# TODO: Add your organization's sensitive patterns
# TODO：补充你所在组织的敏感模式
dangerous_patterns := {
    # System commands
    # 系统命令
    "rm -rf",
    "format disk", 
    "drop table",
    "truncate table",
    "delete from",
    "shutdown",
    "reboot",
    "kill -9",
    
    # File system access
    # 文件系统访问
    "/etc/passwd",
    "/etc/shadow", 
    "~/.ssh",
    "id_rsa",
    "private key",
    
    # Sensitive data patterns
    # 敏感数据模式
    "credit card",
    "social security",
    "ssn",
    "password",
    "api key",
    "secret token",
    "database connection",
    
    # Company-specific (customize for your org)
    # 公司特有（按你的组织自定义）
    "company database",
    "production server",
    "admin panel"
}

# === BUDGET ENFORCEMENT ===
# === budget 约束 ===

# Block requests that exceed maximum token budgets per mode
# 阻止超过各模式最大 Token budget 的 request
decision := {
    "allow": false,
    "reason": sprintf("token budget %d exceeds maximum %d for mode %s", [input.token_budget, max_budgets[input.mode], input.mode])
} {
    max_budgets[input.mode] < input.token_budget
}

# Maximum token budgets per execution mode
# 各执行模式的最大 Token budget
max_budgets := {
    "simple": 1000,
    "standard": 5000,
    "complex": 15000
}

# === AGENT ALLOWLIST ===
# === Agent 白名单 ===

# Allow specific agent IDs
# 允许特定的 Agent ID
decision := {
    "allow": true,
    "reason": "approved agent ID",
    "require_approval": false
} {
    allowed_agent_ids[input.agent_id]
    input.token_budget <= max_budgets[input.mode]
}

# Approved agent IDs
# 已批准的 Agent ID
allowed_agent_ids := {
    "agent-core",
    "llm-agent",
    "synthesis-agent", 
    "complexity-agent",
    "orchestrator-agent"
}

# === DENY RULES (OVERRIDE ALL ALLOWS) ===
# === 拒绝规则（覆盖所有允许规则） ===

# Deny dangerous query patterns regardless of other rules
# 无论其他规则如何，拒绝危险查询模式
deny[sprintf("dangerous pattern detected: %s", [pattern])] {
    some pattern
    dangerous_patterns[pattern]
    contains(lower(input.query), pattern)
}

# Deny excessive token budgets
# 拒绝超额的 Token budget
deny[sprintf("token budget %d exceeds system maximum %d", [input.token_budget, system_limits.max_tokens])] {
    input.token_budget > system_limits.max_tokens
}

# Deny blocked users
# 拒绝被封禁用户的 request
deny[sprintf("user %s is blocked", [input.user_id])] {
    blocked_users[input.user_id]
}

# Deny production operations from unknown users
# 拒绝来自未知用户的生产环境操作
deny["unknown user in production environment"] {
    input.environment == "prod"
    input.user_id != ""
    not allowed_users[input.user_id]
    not privileged_users[input.user_id]
}

# === SYSTEM LIMITS ===
# === 系统限制 ===

system_limits := {
    "max_tokens": 50000,
    "max_concurrent_requests": 20,
    "max_session_duration": 7200  # 2 hours
}

# Blocked users (security enforcement)
# 被封禁的用户（安全约束）
blocked_users := {
    "blocked_user",
    "suspended_account",
    "test_blocked"
}# Test comment to trigger .rego reload

# Another test change Mon Sep  8 23:49:03 JST 2025
