package activities

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
	"go.uber.org/zap"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/budget"
	cfg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
)

// BudgetActivities handles token budget operations
// BudgetActivities 处理 token 预算操作。
type BudgetActivities struct {
	budgetManager *budget.BudgetManager
	logger        *zap.Logger
}

// NewBudgetActivities creates a new budget activities handler
// NewBudgetActivities 创建 budget activities handler。
func NewBudgetActivities(db *sql.DB, logger *zap.Logger) *BudgetActivities {
	var features *cfg.Features
	if f, err := cfg.Load(); err == nil {
		features = f
	}
	bcfg := cfg.BudgetFromEnvOrDefaults(features)
	opts := budget.Options{
		BackpressureThreshold:  bcfg.Backpressure.Threshold,
		MaxBackpressureDelayMs: bcfg.Backpressure.MaxDelayMs,
		// Circuit breaker and rate limit are configured per-user at runtime
		// circuit breaker 与 rate limit 按用户在使用时配置。
	}
	return &BudgetActivities{
		budgetManager: budget.NewBudgetManagerWithOptions(db, logger, opts),
		logger:        logger,
	}
}

// NewBudgetActivitiesWithDefaults allows setting default task/session budgets from typed config.
// NewBudgetActivitiesWithDefaults 允许从 typed config 设置默认 task/session 预算。
func NewBudgetActivitiesWithDefaults(db *sql.DB, logger *zap.Logger, defaultTaskBudget, defaultSessionBudget int) *BudgetActivities {
	var features *cfg.Features
	if f, err := cfg.Load(); err == nil {
		features = f
	}
	bcfg := cfg.BudgetFromEnvOrDefaults(features)
	opts := budget.Options{
		BackpressureThreshold:  bcfg.Backpressure.Threshold,
		MaxBackpressureDelayMs: bcfg.Backpressure.MaxDelayMs,
		DefaultTaskBudget:      defaultTaskBudget,
		DefaultSessionBudget:   defaultSessionBudget,
	}
	return &BudgetActivities{
		budgetManager: budget.NewBudgetManagerWithOptions(db, logger, opts),
		logger:        logger,
	}
}

// NewBudgetActivitiesWithManager allows injecting a custom BudgetManager (useful for tests)
// NewBudgetActivitiesWithManager 允许注入自定义 BudgetManager（便于测试）。
func NewBudgetActivitiesWithManager(mgr *budget.BudgetManager, logger *zap.Logger) *BudgetActivities {
	return &BudgetActivities{
		budgetManager: mgr,
		logger:        logger,
	}
}

// BudgetCheckInput represents input for budget checking
// BudgetCheckInput 表示预算检查的 input。
type BudgetCheckInput struct {
	UserID          string `json:"user_id"`
	SessionID       string `json:"session_id"`
	TaskID          string `json:"task_id"`
	EstimatedTokens int    `json:"estimated_tokens"`
}

// CheckTokenBudget checks if an operation can proceed within budget
// CheckTokenBudget 检查某次操作是否在预算内可继续。
func (b *BudgetActivities) CheckTokenBudget(ctx context.Context, input BudgetCheckInput) (*budget.BudgetCheckResult, error) {
	b.logger.Info("Checking token budget",
		zap.String("user_id", input.UserID),
		zap.String("session_id", input.SessionID),
		zap.Int("estimated_tokens", input.EstimatedTokens),
	)

	result, err := b.budgetManager.CheckBudget(
		ctx,
		input.UserID,
		input.SessionID,
		input.TaskID,
		input.EstimatedTokens,
	)

	if err != nil {
		b.logger.Error("Budget check failed", zap.Error(err))
		return nil, fmt.Errorf("budget check failed: %w", err)
	}

	if !result.CanProceed {
		b.logger.Warn("Budget constraint exceeded",
			zap.String("reason", result.Reason),
			zap.Int("remaining", result.RemainingTaskBudget),
		)
	}

	return result, nil
}

// CheckTokenBudgetWithBackpressure checks budget and applies backpressure if needed
// CheckTokenBudgetWithBackpressure 检查预算，必要时施加 backpressure。
func (b *BudgetActivities) CheckTokenBudgetWithBackpressure(ctx context.Context, input BudgetCheckInput) (*budget.BackpressureResult, error) {
	b.logger.Info("Checking token budget with backpressure",
		zap.String("user_id", input.UserID),
		zap.String("session_id", input.SessionID),
		zap.Int("estimated_tokens", input.EstimatedTokens),
	)

	result, err := b.budgetManager.CheckBudgetWithBackpressure(
		ctx,
		input.UserID,
		input.SessionID,
		input.TaskID,
		input.EstimatedTokens,
	)

	if err != nil {
		b.logger.Error("Budget check with backpressure failed", zap.Error(err))
		return nil, fmt.Errorf("budget check failed: %w", err)
	}

	if result.BackpressureActive {
		b.logger.Warn("Backpressure activated",
			zap.String("pressure_level", result.BudgetPressure),
			zap.Int("delay_ms", result.BackpressureDelay),
		)

		// Don't apply delay here - let workflow handle it with workflow.Sleep()
		// 这里不施加 delay——交给 workflow 用 workflow.Sleep() 处理。
		// This prevents blocking Temporal workers
		// 这样可以避免阻塞 Temporal worker。
	}

	if !result.CanProceed {
		b.logger.Warn("Budget constraint exceeded with backpressure",
			zap.String("reason", result.Reason),
			zap.Int("remaining", result.RemainingTaskBudget),
		)
	}

	return result, nil
}

// CheckTokenBudgetWithCircuitBreaker checks budget with circuit breaker behavior
// CheckTokenBudgetWithCircuitBreaker 以 circuit breaker 行为检查预算。
func (b *BudgetActivities) CheckTokenBudgetWithCircuitBreaker(ctx context.Context, input BudgetCheckInput) (*budget.BackpressureResult, error) {
	b.logger.Info("Checking token budget with circuit breaker",
		zap.String("user_id", input.UserID),
		zap.String("session_id", input.SessionID),
		zap.Int("estimated_tokens", input.EstimatedTokens),
	)

	result, err := b.budgetManager.CheckBudgetWithCircuitBreaker(
		ctx,
		input.UserID,
		input.SessionID,
		input.TaskID,
		input.EstimatedTokens,
	)
	if err != nil {
		b.logger.Error("Budget check with circuit breaker failed", zap.Error(err))
		return nil, fmt.Errorf("budget check failed: %w", err)
	}

	// If circuit is open, return immediately with reason
	// circuit 已 open 时，立即带原因返回。
	if result.CircuitBreakerOpen || !result.CanProceed {
		b.logger.Warn("Request blocked by circuit breaker or budget",
			zap.Bool("breaker_open", result.CircuitBreakerOpen),
			zap.String("reason", result.Reason),
		)
		return result, nil
	}

	// Log backpressure but don't apply delay - let workflow handle it
	// 记录 backpressure 日志但不施加 delay——交给 workflow 处理。
	if result.BackpressureActive && result.BackpressureDelay > 0 {
		b.logger.Warn("Backpressure activated (circuit check)",
			zap.String("pressure_level", result.BudgetPressure),
			zap.Int("delay_ms", result.BackpressureDelay),
		)
		// Don't apply delay here - let workflow handle it with workflow.Sleep()
		// 这里不施加 delay——交给 workflow 用 workflow.Sleep() 处理。
		// This prevents blocking Temporal workers
		// 这样可以避免阻塞 Temporal worker。
	}

	return result, nil
}

// TokenUsageInput represents token usage to record
// TokenUsageInput 表示待记录的 token 用量。
type TokenUsageInput struct {
	UserID              string                 `json:"user_id"`
	SessionID           string                 `json:"session_id"`
	TaskID              string                 `json:"task_id"`
	AgentID             string                 `json:"agent_id"`
	Model               string                 `json:"model"`
	Provider            string                 `json:"provider"`
	InputTokens         int                    `json:"input_tokens"`
	OutputTokens        int                    `json:"output_tokens"`
	CacheReadTokens     int                    `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens   int                    `json:"cache_creation_tokens,omitempty"`
	CacheCreation1hTokens int                    `json:"cache_creation_1h_tokens,omitempty"`
	CallSequence        int                    `json:"call_sequence,omitempty"`
	CostOverride        float64                `json:"cost_override,omitempty"` // When > 0, use instead of pricing calculation 注：大于 0 时用它替代按定价计算的结果
	Metadata            map[string]any `json:"metadata"`
}

// RecordTokenUsage records actual token usage
// RecordTokenUsage 记录实际的 token 用量。
func (b *BudgetActivities) RecordTokenUsage(ctx context.Context, input TokenUsageInput) error {
	// Get activity info for idempotency key
	// 获取 activity info 以生成幂等 key。
	info := activity.GetInfo(ctx)

	b.logger.Info("Recording token usage",
		zap.String("user_id", input.UserID),
		zap.String("agent_id", input.AgentID),
		zap.Int("total_tokens", input.InputTokens+input.OutputTokens),
		zap.String("activity_id", info.ActivityID),
		zap.Int32("attempt", info.Attempt),
	)

	// Generate idempotency key using workflow ID, activity ID, and attempt number
	// 用 workflow ID、activity ID 与 attempt 号生成幂等 key，
	// This ensures retries of the same activity won't double-count tokens
	// 确保同一 activity 的重试不会重复计入 token。
	idempotencyKey := fmt.Sprintf("%s-%s-%d", info.WorkflowExecution.ID, info.ActivityID, info.Attempt)

	usage := &budget.BudgetTokenUsage{
		UserID:              input.UserID,
		SessionID:           input.SessionID,
		TaskID:              input.TaskID,
		AgentID:             input.AgentID,
		Model:               input.Model,
		Provider:            input.Provider,
		InputTokens:         input.InputTokens,
		OutputTokens:        input.OutputTokens,
		CacheReadTokens:     input.CacheReadTokens,
		CacheCreationTokens:   input.CacheCreationTokens,
		CacheCreation1hTokens: input.CacheCreation1hTokens,
		CallSequence:        input.CallSequence,
		CostOverride:        input.CostOverride,
		Metadata:            input.Metadata,
		IdempotencyKey:      idempotencyKey,
	}

	// Backfill missing provider from model if available
	// provider 缺失且 model 可用时，回填 provider。
	if usage.Provider == "" && usage.Model != "" {
		usage.Provider = detectProviderFromModel(usage.Model)
		b.logger.Debug("Backfilled provider from model",
			zap.String("model", usage.Model),
			zap.String("provider", usage.Provider))
	}

	err := b.budgetManager.RecordUsage(ctx, usage)
	if err != nil {
		b.logger.Error("Failed to record token usage", zap.Error(err))
		return fmt.Errorf("failed to record usage: %w", err)
	}

	return nil
}

// BudgetedAgentInput combines agent input with budget constraints
// BudgetedAgentInput 组合 agent input 与预算约束。
type BudgetedAgentInput struct {
	AgentInput AgentExecutionInput `json:"agent_input"`
	MaxTokens  int                 `json:"max_tokens"`
	UserID     string              `json:"user_id"`
	TaskID     string              `json:"task_id"`
	ModelTier  string              `json:"model_tier"` // small/medium/large 注：small/medium/large
}

// detectProviderFromModel determines the provider based on the model name
// detectProviderFromModel 根据 model 名判断 provider，
// Delegates to shared models.DetectProvider for consistent provider detection
// 委托给共享的 models.DetectProvider，保证判断一致。
func detectProviderFromModel(model string) string {
	return models.DetectProvider(model)
}

// shouldRecordUsage decides whether to persist a token_usage row given the
// breakdown observed from an agent execution. Returns true when at least one
// of these holds:
//  1. the call consumed input/output tokens,
//  2. the caller asked for zero-token audit (record_zero_token flag),
//  3. there are tool costs to attribute, or
//  4. the call read or wrote prompt cache (whose cost must still be billed
//     even if input+output happen to be zero — the failure mode we're
//     guarding against here is the cache leak).
// 判断是否落一条 token_usage 记录，满足任一条件即记录：
// 1) 消耗了 input/output token；2) 调用方要求零 token 审计（record_zero_token）；
// 3) 有 tool 成本需要归集；4) 读写过 prompt cache——即使 input+output 为 0 也必须计费，
// 这里防的失败模式正是 cache 成本漏记。
func shouldRecordUsage(inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens int, recordZeroToken, hasToolCosts bool) bool {
	if (inputTokens + outputTokens) > 0 {
		return true
	}
	if recordZeroToken || hasToolCosts {
		return true
	}
	return cacheReadTokens > 0 || cacheCreationTokens > 0
}

// ExecuteAgentWithBudget executes an agent with token budget constraints
// ExecuteAgentWithBudget 在 token 预算约束下执行 agent。
func (b *BudgetActivities) ExecuteAgentWithBudget(ctx context.Context, input BudgetedAgentInput) (*AgentExecutionResult, error) {
	b.logger.Info("Executing agent with budget constraints",
		zap.String("agent_id", input.AgentInput.AgentID),
		zap.Int("max_tokens", input.MaxTokens),
		zap.String("model_tier", input.ModelTier),
	)

	// Check budget before execution
	// 执行前先检查预算。
	budgetCheck, err := b.budgetManager.CheckBudget(
		ctx,
		input.UserID,
		input.AgentInput.SessionID,
		input.TaskID,
		input.MaxTokens,
	)

	if err != nil {
		return nil, fmt.Errorf("budget check failed: %w", err)
	}

	if !budgetCheck.CanProceed {
		return &AgentExecutionResult{
			AgentID: input.AgentInput.AgentID,
			Success: false,
			Error:   fmt.Sprintf("Budget exceeded: %s", budgetCheck.Reason),
		}, nil
	}

	// Add budget constraints to context (do not hardcode model/provider)
	// 把预算约束写入 context（不硬编码 model/provider）。
	input.AgentInput.Context["max_tokens"] = input.MaxTokens
	input.AgentInput.Context["model_tier"] = input.ModelTier

	// Execute the actual agent using shared helper (not calling activity directly)
	// 通过共享 helper 执行真正的 agent（不直接调用 activity）。
	activity.GetLogger(ctx).Info("Executing agent with budget",
		"agent_id", input.AgentInput.AgentID,
		"max_tokens", input.MaxTokens,
	)
	logger := zap.L()
	var result AgentExecutionResult

	// If we have pre-computed tool parameters and tools, use the forced-tools path (emits SSE events).
	// 已有预计算的 tool 参数与 tools 时走 forced-tools 路径（会发出 SSE 事件）。
	if len(input.AgentInput.ToolParameters) > 0 && len(input.AgentInput.SuggestedTools) > 0 {
		result, err = ExecuteAgentWithForcedTools(ctx, input.AgentInput)
	} else {
		result, err = executeAgentCore(ctx, input.AgentInput, logger)
	}
	if err != nil {
		return nil, fmt.Errorf("agent execution failed: %w", err)
	}

	// Don't override model/provider — rely on downstream selection based on tier
	// 不覆盖 model/provider——交由下游按 tier 选择。

	// Ensure tokens don't exceed budget
	// 确保 token 不超出预算。
	if result.TokensUsed > input.MaxTokens {
		b.logger.Warn("Agent used more tokens than budgeted",
			zap.Int("used", result.TokensUsed),
			zap.Int("max", input.MaxTokens),
		)
		result.TokensUsed = input.MaxTokens // Cap at max budget 注：截断到最大预算
	}

	// Record the actual usage with the model that was actually used
	// 按实际使用的 model 记录真实用量。
	// Use the model from result if available, otherwise fall back to tier's priority-one model
	// 优先用 result 中的 model，缺失时回退到该 tier 的 priority-one model。
	actualModel := result.ModelUsed
	if strings.TrimSpace(actualModel) == "" {
		// Fallback to tier's priority-one model for accurate cost tracking
		// 回退到该 tier 的 priority-one model，以保证成本统计准确。
		if m := pricing.GetPriorityOneModel(input.ModelTier); m != "" {
			actualModel = m
		}
	}
	// Determine actual provider. Prefer result.Provider when available; fallback to detection from model.
	// 确定实际 provider：优先 result.Provider，缺失时按 model 推断。
	actualProvider := result.Provider
	if strings.TrimSpace(actualProvider) == "" {
		actualProvider = detectProviderFromModel(actualModel)
	}

	// Get activity info for idempotency key
	// 获取 activity info 以生成幂等 key。
	info := activity.GetInfo(ctx)
	idempotencyKey := fmt.Sprintf("%s-%s-%d", info.WorkflowExecution.ID, info.ActivityID, info.Attempt)

	// Populate input/output tokens in result if not already provided by agent
	// agent 未提供时，补齐 result 中的 input/output token。
	// Prefer agent-reported splits, fallback to estimation
	// 优先用 agent 上报的拆分，缺失时用估算。
	if result.InputTokens == 0 && result.OutputTokens == 0 && result.TokensUsed > 0 {
		result.InputTokens = result.TokensUsed * 6 / 10
		result.OutputTokens = result.TokensUsed * 4 / 10
	}

	// Use result's populated splits for budget recording
	// 用 result 中已补齐的拆分记录预算。
	inputTokens := result.InputTokens
	outputTokens := result.OutputTokens
	if inputTokens == 0 && outputTokens == 0 {
		// Fallback if result still doesn't have splits
		// result 仍无拆分时回退。
		inputTokens = result.TokensUsed * 6 / 10
		outputTokens = result.TokensUsed * 4 / 10
	}

	// Respect optional zero-token audit flag from context
	// 遵循 context 中可选的零 token 审计标记。
	recordZeroToken := false
	if ctxMap := input.AgentInput.Context; ctxMap != nil {
		if v, ok := ctxMap["record_zero_token"]; ok {
			switch t := v.(type) {
			case bool:
				recordZeroToken = t
			case string:
				if strings.EqualFold(t, "true") {
					recordZeroToken = true
				}
			}
		}
	}

	// Treat empty output with no tokens/tools as a failure and skip recording unless explicitly requested
	// Check for tool cost entries before early exits — these must be recorded
	// even if the LLM call produced zero tokens (e.g., provider error).
	// 无输出且无 token、无 tool 调用时视为失败并跳过记录，除非显式要求；
	// 但先检查 tool_cost_entries——即使 LLM 调用产生零 token（如 provider 报错），tool 成本也必须记录。
	hasToolCosts := result.Metadata != nil &&
		func() bool { _, ok := result.Metadata["tool_cost_entries"].([]any); return ok }()

	noOutput := strings.TrimSpace(result.Response) == "" &&
		len(result.ToolExecutions) == 0 &&
		result.TokensUsed == 0 &&
		inputTokens == 0 &&
		outputTokens == 0
	if noOutput {
		result.Success = false
		if result.Error == "" {
			result.Error = "agent produced no output or token usage"
		}
		if !recordZeroToken && !hasToolCosts {
			return &result, nil
		}
	}

	// Skip recording only when nothing of value happened: zero input/output, no
	// audit flag, no tool costs, AND no prompt-cache activity. Pure cache hits
	// (cache_read>0 or cache_creation>0 with zero input/output) must still be
	// recorded for accurate quota accounting.
	// 仅在确实无事发生时跳过记录：input/output 为零、无审计标记、无 tool 成本，且无 prompt cache activity；
	// 纯 cache 命中（cache_read 或 cache_creation > 0 而 input/output 为 0）仍需记录，以保证配额计量准确。
	if !shouldRecordUsage(inputTokens, outputTokens, result.CacheReadTokens, result.CacheCreationTokens, recordZeroToken, hasToolCosts) {
		b.logger.Warn("Skipping token usage record: zero tokens, no cache, no tool costs, no audit flag",
			zap.String("agent_id", input.AgentInput.AgentID),
			zap.String("task_id", input.TaskID),
		)
		return &result, nil
	}

	err = b.budgetManager.RecordUsage(ctx, &budget.BudgetTokenUsage{
		UserID:         input.UserID,
		SessionID:      input.AgentInput.SessionID,
		TaskID:         input.TaskID,
		AgentID:        input.AgentInput.AgentID,
		Model:          actualModel,
		Provider:       actualProvider,
		InputTokens:         inputTokens,
		OutputTokens:        outputTokens,
		CacheReadTokens:       result.CacheReadTokens,
		CacheCreationTokens:   result.CacheCreationTokens,
		CacheCreation1hTokens: result.CacheCreation1hTokens,
		IdempotencyKey:         idempotencyKey,
	})

	if err != nil {
		b.logger.Error("Failed to record usage after agent execution", zap.Error(err))
	}

	// Record external tool costs if present in agent metadata.
	// Metadata is populated by both paths:
	//   - HTTP path (ExecuteAgentWithForcedTools): from Python agent response metadata
	//   - gRPC path (executeAgentCore): from ExecuteTaskResponse.metadata proto field
	// Note: streaming path (runStreaming) does not carry metadata, but agents with
	// tools always use the unary path (useStreaming=false when tools are present).
	// 记录 metadata 中的外部 tool 成本。两条路径都会填充 metadata：
	// HTTP 路径（ExecuteAgentWithForcedTools）来自 Python agent response，gRPC 路径来自 ExecuteTaskResponse.metadata；
	// streaming 路径不携带 metadata，但有 tool 时总走 unary（useStreaming=false）。
	if result.Metadata != nil {
		if rawEntries, ok := result.Metadata["tool_cost_entries"].([]any); ok {
			for i, raw := range rawEntries {
				em, ok := raw.(map[string]any)
				if !ok {
					continue
				}
				costModel, _ := em["cost_model"].(string)
				provider, _ := em["provider"].(string)
				toolName, _ := em["tool"].(string)
				syntheticTokens := 7500
				if st, ok := em["synthetic_tokens"].(float64); ok && int(st) > 0 {
					syntheticTokens = int(st)
				}
				// Read upstream-reported cost (e.g. web_fetch LLM extraction cost)
				// 读取上游上报的成本（如 web_fetch 的 LLM 提取成本）。
				var costOverride float64
				if c, ok := em["cost_usd"].(float64); ok && c > 0 {
					costOverride = c
				}

				toolUsage := &budget.BudgetTokenUsage{
					UserID:         input.UserID,
					SessionID:      input.AgentInput.SessionID,
					TaskID:         input.TaskID,
					AgentID:        fmt.Sprintf("tool_%s", toolName),
					Model:          costModel,
					Provider:       provider,
					InputTokens:    0,
					OutputTokens:   syntheticTokens,
					CostOverride:   costOverride,
					IdempotencyKey: fmt.Sprintf("%s-%s-tool-%s-%d",
						input.TaskID, input.AgentInput.AgentID, toolName, i),
				}
				if err := b.budgetManager.RecordUsage(ctx, toolUsage); err != nil {
					b.logger.Warn("Failed to record tool cost",
						zap.String("tool", toolName),
						zap.String("model", costModel),
						zap.Error(err))
				}
			}
		}
	}

	return &result, nil
}

// UsageReportInput represents input for generating usage reports
// UsageReportInput 表示生成用量报告的 input。
type UsageReportInput struct {
	UserID    string    `json:"user_id"`
	SessionID string    `json:"session_id"`
	TaskID    string    `json:"task_id"`
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
}

// GenerateUsageReport generates a token usage report
// GenerateUsageReport 生成 token 用量报告。
func (b *BudgetActivities) GenerateUsageReport(ctx context.Context, input UsageReportInput) (*budget.UsageReport, error) {
	b.logger.Info("Generating usage report",
		zap.String("user_id", input.UserID),
		zap.String("session_id", input.SessionID),
	)

	// Set default time range if not provided
	// 未提供时设置默认时间范围。
	if input.EndTime.IsZero() {
		input.EndTime = time.Now()
	}
	if input.StartTime.IsZero() {
		input.StartTime = input.EndTime.Add(-24 * time.Hour)
	}

	report, err := b.budgetManager.GetUsageReport(ctx, budget.UsageFilters{
		UserID:    input.UserID,
		SessionID: input.SessionID,
		TaskID:    input.TaskID,
		StartTime: input.StartTime,
		EndTime:   input.EndTime,
	})

	if err != nil {
		b.logger.Error("Failed to generate usage report", zap.Error(err))
		return nil, fmt.Errorf("failed to generate report: %w", err)
	}

	return report, nil
}

// UpdateBudgetInput represents input for updating budget policies
// UpdateBudgetInput 表示更新预算策略的 input。
type UpdateBudgetInput struct {
	UserID           string   `json:"user_id"`
	SessionID        string   `json:"session_id"`
	TaskBudget       *int     `json:"task_budget,omitempty"`
	SessionBudget    *int     `json:"session_budget,omitempty"`
	HardLimit        *bool    `json:"hard_limit,omitempty"`
	WarningThreshold *float64 `json:"warning_threshold,omitempty"`
	RequireApproval  *bool    `json:"require_approval,omitempty"`
}

// UpdateBudgetPolicy updates budget policies for a user/session
// UpdateBudgetPolicy 更新用户/session 的预算策略。
func (b *BudgetActivities) UpdateBudgetPolicy(ctx context.Context, input UpdateBudgetInput) error {
	b.logger.Info("Updating budget policy",
		zap.String("user_id", input.UserID),
		zap.String("session_id", input.SessionID),
	)

	// This would update the budget policies in the database
	// 这里本应更新数据库中的预算策略。
	// For now, we'll just log the update
	// 目前仅记录日志。

	return nil
}

// Model selection is delegated downstream; avoid hardcoding here.
// model 选择已委托给下游，这里不再硬编码。

// SessionUpdateInput is defined in types.go
// SessionUpdateInput 定义在 types.go 中。
