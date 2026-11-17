package patterns

// Options provides common configuration for pattern execution
// 各 pattern 共用的入参；BudgetAgentMax <= 0 表示不做 per-agent 限额。
type Options struct {
	BudgetAgentMax int            // Per-agent token budget 注：单个 agent 的 token 预算
	SessionID      string         // Session identifier 注：session 标识
	UserID         string         // User identifier for budget/recording 注：用于预算与记录的 user 标识
	EmitEvents     bool           // Whether to emit streaming events 注：是否发出 streaming 事件
	ModelTier      string         // Model tier (small/medium/large) 注：model tier（small/medium/large）
	Context        map[string]any // Additional context 注：附加上下文
}

// ReflectionConfig controls reflection behavior
// MaxRetries 与 ConfidenceThreshold 共同决定何时停止反思：次数用完或分数达标即退出。
type ReflectionConfig struct {
	Enabled             bool     // Whether reflection is enabled 注：是否启用反思
	MaxRetries          int      // Maximum reflection iterations 注：反思的最大迭代次数
	ConfidenceThreshold float64  // Minimum acceptable quality score 注：可接受的最低质量分
	Criteria            []string // Evaluation criteria 注：评估标准
	TimeoutMs           int      // Timeout per reflection attempt 注：每次反思尝试的超时时间
}
