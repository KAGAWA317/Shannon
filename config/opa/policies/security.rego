package shannon.task

# Security policies - these can override base allow rules
# 安全策略 - 可覆盖基础的允许规则

# Production restrictions
# 生产环境限制
deny[{"reason": "production environment requires explicit approval for complex tasks"}] {
	input.environment == "prod"
	input.mode == "complex"
	not approved_complex_task
}

deny[{"reason": "high complexity score requires approval"}] {
	input.complexity_score > 0.9
	not approved_high_complexity
}

deny[{"reason": "suspicious session pattern detected"}] {
	suspicious_session
}

# Security checks
# 安全检查
approved_complex_task {
	# This would be set by human intervention workflow
	# 该值由人工介入 workflow 设置
	input.context.approved_by_human == true
}

approved_high_complexity {
	# This would be set by human intervention workflow  
	# 该值由人工介入 workflow 设置
	input.context.complexity_approved == true
}

suspicious_session {
	# Example: too many requests in short time
	# 示例：短时间内 request 过多
	input.context.request_count > 100
	input.context.time_window_minutes < 5
}

suspicious_session {
	# Example: requests from blocked IP ranges
	# 示例：来自被封锁 IP 段的 request
	startswith(input.ip_address, "192.168.100.") # blocked internal range
}

# Token budget restrictions
# Token budget 限制
deny[{"reason": "token budget exceeded"}] {
	input.token_budget > 10000
	input.environment == "prod"
	not budget_approved
}

budget_approved {
	input.context.budget_approved == true
}