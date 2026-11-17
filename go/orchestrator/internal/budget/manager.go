package budget

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// TokenBudget represents budget constraints at different levels
// TokenBudget 表示不同层级的 budget 约束
type TokenBudget struct {
	// Task-level budgets
	// Task 级 budget
	TaskBudget     int `json:"task_budget"`
	TaskTokensUsed int `json:"task_tokens_used"`

	// Session-level budgets
	// Session 级 budget
	SessionBudget     int `json:"session_budget"`
	SessionTokensUsed int `json:"session_tokens_used"`

	// User-level budgets (daily/monthly)
	// User 级 budget（按日/按月）
	// Daily/Monthly budgets removed - task and session budgets are sufficient
	// 已移除日/月 budget——task 与 session budget 已足够

	// Cost tracking
	// 成本追踪
	EstimatedCostUSD float64 `json:"estimated_cost_usd"`
	ActualCostUSD    float64 `json:"actual_cost_usd"`

	// Enforcement settings
	// 强制限制设置
	HardLimit        bool    `json:"hard_limit"`        // Stop execution if exceeded 注：超出时停止执行
	WarningThreshold float64 `json:"warning_threshold"` // Warn at X% of budget (0.8 = 80%) 注：达到 budget 的 X% 时告警（0.8 = 80%）
	RequireApproval  bool    `json:"require_approval"`  // Require approval if exceeded 注：超出时需要审批
}

// BudgetTokenUsage tracks token consumption for budget management (renamed to avoid conflict with models.TokenUsage)
// BudgetTokenUsage 记录 budget 管理所需的 token 消耗（重命名以避免与 models.TokenUsage 冲突）
type BudgetTokenUsage struct {
	ID                    string `json:"id"`
	UserID                string `json:"user_id"`
	SessionID             string `json:"session_id"`
	TaskID                string `json:"task_id"`
	AgentID               string `json:"agent_id"`
	Model                 string `json:"model"`
	Provider              string `json:"provider"`
	InputTokens           int    `json:"input_tokens"`
	OutputTokens          int    `json:"output_tokens"`
	TotalTokens           int    `json:"total_tokens"`
	CacheReadTokens       int    `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens   int    `json:"cache_creation_tokens,omitempty"`
	CacheCreation1hTokens int    `json:"cache_creation_1h_tokens,omitempty"`
	// CacheAwareTotalTokens = InputTokens + OutputTokens + CacheReadTokens + CacheCreationTokens.
	// Parallel to TotalTokens (= input + output), used for quota accounting that
	// must include prompt-cache cost while keeping TotalTokens OpenAI-compatible.
	// CacheAwareTotalTokens = InputTokens + OutputTokens + CacheReadTokens + CacheCreationTokens，与 TotalTokens（= input + output）并列，用于配额核算，须包含 prompt-cache 成本，同时保持 TotalTokens 与 OpenAI 兼容。
	CacheAwareTotalTokens int            `json:"cache_aware_total_tokens,omitempty"`
	CallSequence          int            `json:"call_sequence,omitempty"`
	CostUSD               float64        `json:"cost_usd"`
	CostOverride          float64        `json:"cost_override,omitempty"` // When > 0, use this instead of pricing calculation (e.g. Python-reported cost_usd) 注：大于 0 时使用该值而非定价计算（例如 Python 上报的 cost_usd）
	Timestamp             time.Time      `json:"timestamp"`
	Metadata              map[string]any `json:"metadata"`
	IdempotencyKey        string         `json:"idempotency_key,omitempty"` // Optional key for retry safety 注：可选键，用于重试安全
}

// BudgetManager manages token budgets and usage tracking
//
// Mutex Lock Ordering (IMPORTANT - to prevent deadlocks):
// When acquiring multiple locks, always follow this order:
//  1. mu (main budget mutex) - protects sessionBudgets, userBudgets
//  2. rateLimitsmu - protects rateLimiters map
//  3. cbMu - protects circuitBreakers map
//  4. priorityMu - protects priorityTiers map
//  5. allocationMu - protects sessionAllocations map
//  6. idempotencyMu - protects processedUsage map
//
// Never acquire a lower-numbered lock while holding a higher-numbered lock.
// Each mutex protects independent data structures, minimizing lock contention.
// 加锁顺序（重要，用于避免死锁）：获取多个锁时始终按 mu → rateLimitsmu → cbMu → priorityMu → allocationMu → idempotencyMu 的顺序；绝不在持有更高编号锁时获取更低编号锁；每个 mutex 保护独立的数据结构，以最小化锁竞争。
type BudgetManager struct {
	db     *sql.DB
	logger *zap.Logger

	// In-memory cache for active sessions
	// 活跃 session 的内存缓存
	sessionBudgets map[string]*TokenBudget
	userBudgets    map[string]*TokenBudget
	mu             sync.RWMutex // Lock order: 1 注：锁顺序：1

	// Budget policies
	// Budget 策略
	defaultTaskBudget    int
	defaultSessionBudget int
	// Daily/monthly budgets removed
	// 已移除日/月 budget

	// Enhanced features - Backpressure control
	// 增强特性——背压控制
	backpressureThreshold float64 // Activate backpressure at X% of budget (default 0.8) 注：在 budget 的 X% 处启用背压（默认 0.8）
	maxBackpressureDelay  int     // Maximum delay in milliseconds 注：最大延迟（毫秒）

	// Rate limiting
	// 限流
	rateLimiters map[string]*rate.Limiter
	rateLimitsmu sync.RWMutex // Lock order: 2 注：锁顺序：2

	// Circuit breaker per user
	// 按 user 维度的熔断器
	circuitBreakers map[string]*CircuitBreaker
	cbMu            sync.RWMutex // Lock order: 3 注：锁顺序：3

	// Priority tiers for allocation
	// 用于分配的优先级层级
	priorityTiers map[string]PriorityTier
	priorityMu    sync.RWMutex // Lock order: 4 注：锁顺序：4

	// Session allocations for dynamic reallocation
	// session 分配额，用于动态再分配
	sessionAllocations map[string]int
	allocationMu       sync.RWMutex // Lock order: 5 注：锁顺序：5

	// Idempotency tracking for retry safety
	// 幂等追踪，用于重试安全
	processedUsage map[string]time.Time // Maps idempotency key to timestamp when processed 注：将幂等键映射到处理时刻的时间戳
	idempotencyMu  sync.RWMutex         // Lock order: 6 - Separate mutex for idempotency tracking 注：锁顺序：6——幂等追踪专用的独立 mutex
	idempotencyTTL time.Duration        // How long to keep idempotency records (default 1 hour) 注：幂等记录的保留时长（默认 1 小时）

	// Cleanup control
	// 清理控制
	cleanupStop chan struct{} // Channel to stop the cleanup goroutine 注：用于停止清理 goroutine 的 channel
	cleanupOnce sync.Once     // Ensures cleanup goroutine starts only once 注：确保清理 goroutine 只启动一次
}

// ErrTokenOverflow indicates a token counter would overflow the int range.
// ErrTokenOverflow 表示 token 计数器将超出 int 范围
var ErrTokenOverflow = fmt.Errorf("token count would overflow")

// NewBudgetManager creates a new budget manager
// NewBudgetManager 创建新的 budget manager
func NewBudgetManager(db *sql.DB, logger *zap.Logger) *BudgetManager {
	bm := &BudgetManager{
		db:             db,
		logger:         logger,
		sessionBudgets: make(map[string]*TokenBudget),
		userBudgets:    make(map[string]*TokenBudget),

		// Default budgets (configurable via shannon.yaml session.token_budget_per_task)
		// 默认 budget（可通过 shannon.yaml 的 session.token_budget_per_task 配置）
		defaultTaskBudget:    10000, // 10K tokens per task (fallback if not configured) 注：每个 task 10K token（未配置时的兜底值）
		defaultSessionBudget: 50000, // 50K tokens per session (fallback) 注：每个 session 50K token（兜底值）

		// Enhanced features initialization
		// 增强特性初始化
		backpressureThreshold: 0.8,
		maxBackpressureDelay:  5000,
		rateLimiters:          make(map[string]*rate.Limiter),
		circuitBreakers:       make(map[string]*CircuitBreaker),
		priorityTiers:         make(map[string]PriorityTier),
		sessionAllocations:    make(map[string]int),

		// Idempotency tracking
		// 幂等追踪
		processedUsage: make(map[string]time.Time),
		idempotencyTTL: 1 * time.Hour, // Keep idempotency records for 1 hour 注：幂等记录保留 1 小时
		cleanupStop:    make(chan struct{}),
	}

	// Initialize database tables if needed
	// 按需初始化数据库表
	if db != nil {
		bm.initializeTables()
	}

	return bm
}

// Options allow configuring budget manager behavior from config/env
// Options 允许通过 config/env 配置 budget manager 的行为
type Options struct {
	BackpressureThreshold  float64
	MaxBackpressureDelayMs int
	// Default task/session budgets (tokens); 0 = use built-in defaults
	// 默认 task/session budget（token 数）；为 0 时使用内置默认值
	DefaultTaskBudget    int
	DefaultSessionBudget int
}

// NewBudgetManagerWithOptions creates a budget manager and applies options
// NewBudgetManagerWithOptions 创建 budget manager 并应用 options
func NewBudgetManagerWithOptions(db *sql.DB, logger *zap.Logger, opts Options) *BudgetManager {
	bm := NewBudgetManager(db, logger)
	if opts.BackpressureThreshold > 0 {
		bm.backpressureThreshold = opts.BackpressureThreshold
	}
	if opts.MaxBackpressureDelayMs > 0 {
		bm.maxBackpressureDelay = opts.MaxBackpressureDelayMs
	}
	if opts.DefaultTaskBudget > 0 {
		bm.defaultTaskBudget = opts.DefaultTaskBudget
	}
	if opts.DefaultSessionBudget > 0 {
		bm.defaultSessionBudget = opts.DefaultSessionBudget
	}
	return bm
}

// CheckBudget verifies if an operation can proceed within budget constraints
// CheckBudget 校验操作是否可在 budget 约束内继续
func (bm *BudgetManager) CheckBudget(ctx context.Context, userID, sessionID, taskID string, estimatedTokens int) (*BudgetCheckResult, error) {
	// Phase 1: Try read lock first for existing budgets (optimization for common case)
	// 阶段 1：先尝试读锁读取已有 budget（常见路径的优化）
	bm.mu.RLock()
	userBudget, userExists := bm.userBudgets[userID]
	sessionBudget, sessionExists := bm.sessionBudgets[sessionID]
	bm.mu.RUnlock()

	// Phase 2: If budgets don't exist, acquire write lock to create them
	// 阶段 2：若 budget 不存在，获取写锁创建
	if !userExists || !sessionExists {
		bm.mu.Lock()
		// Double-check pattern: budgets might have been created by another goroutine
		// 双重检查：budget 可能已被其他 goroutine 创建
		if !userExists {
			if ub, exists := bm.userBudgets[userID]; exists {
				userBudget = ub
			} else {
				userBudget = &TokenBudget{
					HardLimit:        true,
					WarningThreshold: 0.8,
				}
				bm.userBudgets[userID] = userBudget
			}
		}
		if !sessionExists {
			if sb, exists := bm.sessionBudgets[sessionID]; exists {
				sessionBudget = sb
			} else {
				sessionBudget = &TokenBudget{
					TaskBudget:       bm.defaultTaskBudget,
					SessionBudget:    bm.defaultSessionBudget,
					HardLimit:        false,
					WarningThreshold: 0.8,
					RequireApproval:  false,
				}
				bm.sessionBudgets[sessionID] = sessionBudget
			}
		}
		bm.mu.Unlock()
	}

	result := &BudgetCheckResult{
		CanProceed:      true,
		RequireApproval: false,
		Warnings:        []string{},
	}

	// Acquire read lock to safely read budget values
	// 获取读锁以安全读取 budget 值
	bm.mu.RLock()
	// Create local copies of the values we need to check
	// 为需要校验的值创建本地副本
	taskTokensUsed := sessionBudget.TaskTokensUsed
	taskBudget := sessionBudget.TaskBudget
	sessionTokensUsed := sessionBudget.SessionTokensUsed
	sessionBudgetLimit := sessionBudget.SessionBudget
	// Daily budget tracking removed - using task/session budgets only
	// 已移除日 budget 追踪——仅使用 task/session budget
	hardLimit := sessionBudget.HardLimit
	requireApproval := sessionBudget.RequireApproval
	warningThreshold := sessionBudget.WarningThreshold
	bm.mu.RUnlock()

	// Check task-level budget
	// 校验 task 级 budget
	if taskTokensUsed+estimatedTokens > taskBudget {
		if hardLimit {
			result.CanProceed = false
			result.Reason = fmt.Sprintf("Task budget exceeded: %d/%d tokens",
				taskTokensUsed+estimatedTokens, taskBudget)
		} else {
			result.RequireApproval = requireApproval
			result.Warnings = append(result.Warnings, "Task budget will be exceeded")
		}
	}

	// Check session-level budget
	// 校验 session 级 budget
	if sessionTokensUsed+estimatedTokens > sessionBudgetLimit {
		if hardLimit {
			result.CanProceed = false
			result.Reason = fmt.Sprintf("Session budget exceeded: %d/%d tokens",
				sessionTokensUsed+estimatedTokens, sessionBudgetLimit)
		} else {
			result.Warnings = append(result.Warnings, "Session budget will be exceeded")
		}
	}

	// Daily budget check removed - task and session budgets provide sufficient control
	// 已移除日 budget 校验——task 与 session budget 已提供足够的控制

	// Check warning threshold and emit streaming event
	// 校验告警阈值并发出 streaming 事件
	taskUsagePercent := float64(taskTokensUsed) / float64(taskBudget)
	if taskUsagePercent > warningThreshold {
		warningMsg := fmt.Sprintf("Task budget at %.1f%% (threshold: %.1f%%)",
			taskUsagePercent*100, warningThreshold*100)
		result.Warnings = append(result.Warnings, warningMsg)

		// Emit BUDGET_THRESHOLD event for streaming observability
		// 发出 BUDGET_THRESHOLD 事件，用于 streaming 可观测性
		bm.emitBudgetThresholdEvent(taskID, sessionID, warningMsg, map[string]any{
			"usage_percent":     taskUsagePercent * 100,
			"threshold_percent": warningThreshold * 100,
			"tokens_used":       taskTokensUsed,
			"tokens_budget":     taskBudget,
			"level":             "warning",
			"budget_type":       "task",
		})
	}

	// Estimate cost using priority-1 small tier model from config
	// 使用配置中 priority-1 small 层级的模型估算成本
	defaultModel := pricing.GetPriorityOneModel("small")
	if defaultModel == "" {
		defaultModel = "gpt-5-nano-2025-08-07" // Fallback if config unavailable 注：配置不可用时的兜底值
	}
	result.EstimatedCost = bm.estimateCost(estimatedTokens, defaultModel)
	result.RemainingTaskBudget = taskBudget - taskTokensUsed
	result.RemainingSessionBudget = sessionBudgetLimit - sessionTokensUsed
	// RemainingDailyBudget removed
	// 已移除 RemainingDailyBudget

	return result, nil
}

// RecordUsage records actual token usage after an operation
// RecordUsage 在操作结束后记录实际 token 用量
func (bm *BudgetManager) RecordUsage(ctx context.Context, usage *BudgetTokenUsage) error {
	// Generate ID if not provided
	// 未提供 ID 时生成
	if usage.ID == "" {
		usage.ID = uuid.New().String()
	}

	// Check idempotency if key is provided
	// 若提供了幂等键则检查幂等性
	if usage.IdempotencyKey != "" {
		bm.idempotencyMu.Lock()
		if processedAt, exists := bm.processedUsage[usage.IdempotencyKey]; exists {
			// Check if the key is still within TTL
			// 检查该键是否仍在 TTL 内
			if time.Since(processedAt) < bm.idempotencyTTL {
				bm.idempotencyMu.Unlock()
				bm.logger.Debug("Skipping duplicate usage record",
					zap.String("idempotency_key", usage.IdempotencyKey),
					zap.String("usage_id", usage.ID))
				return nil // Already processed, skip to prevent double-counting 注：已处理过，跳过以避免重复计数
			}
			// Key has expired, will be re-processed
			// 键已过期，将被重新处理
		}
		// Mark as processed immediately to prevent TOCTOU races
		// 立即标记为已处理，避免 TOCTOU 竞态
		bm.processedUsage[usage.IdempotencyKey] = time.Now()
		bm.idempotencyMu.Unlock()
	}

	usage.Timestamp = time.Now()
	usage.TotalTokens = usage.InputTokens + usage.OutputTokens
	usage.CacheAwareTotalTokens = usage.InputTokens + usage.OutputTokens +
		usage.CacheReadTokens + usage.CacheCreationTokens

	// Calculate cost: prefer upstream-reported cost (e.g. Python LLM call) over pricing lookup
	// 计算成本：优先采用上游上报的成本（如 Python LLM 调用），其次才查定价表
	if usage.CostOverride > 0 {
		usage.CostUSD = usage.CostOverride
	} else {
		usage.CostUSD = pricing.CostForSplitWithCache(
			usage.Model, usage.InputTokens, usage.OutputTokens,
			usage.CacheReadTokens, usage.CacheCreationTokens, usage.CacheCreation1hTokens, usage.Provider,
		)
	}

	// Update in-memory budgets with overflow checks. Use CacheAwareTotalTokens
	// (= input + output + cache_read + cache_creation) so subsequent
	// CheckBudget / backpressure / circuit-breaker see the true cost,
	// including prompt cache. Without this, cache-heavy sessions silently
	// blow past their quota.
	// 使用 CacheAwareTotalTokens（= input + output + cache_read + cache_creation）更新内存 budget，使后续 CheckBudget / backpressure / circuit-breaker 能看到真实成本（含 prompt cache）；否则 cache 密集的 sessions 会悄悄超出配额。
	const maxInt = int(^uint(0) >> 1)
	bm.mu.Lock()
	if sessionBudget, ok := bm.sessionBudgets[usage.SessionID]; ok {
		if sessionBudget.TaskTokensUsed > maxInt-usage.CacheAwareTotalTokens ||
			sessionBudget.SessionTokensUsed > maxInt-usage.CacheAwareTotalTokens {
			bm.mu.Unlock()
			return ErrTokenOverflow
		}
		sessionBudget.TaskTokensUsed += usage.CacheAwareTotalTokens
		sessionBudget.SessionTokensUsed += usage.CacheAwareTotalTokens
		sessionBudget.ActualCostUSD += usage.CostUSD
	}
	// Daily/monthly user budget tracking removed
	// 已移除日/月 user budget 追踪
	bm.mu.Unlock()

	// Store in database
	// 存入数据库
	err := bm.storeUsage(ctx, usage)
	if err != nil {
		return err
	}

	// Record Prometheus cache metrics (best-effort, after successful DB store)
	// 记录 Prometheus cache 指标（尽力而为，在 DB 写入成功后执行）
	if usage.CacheReadTokens > 0 || usage.CacheCreationTokens > 0 {
		savingsUSD := 0.0
		if usage.Model != "" {
			costWithoutCache := pricing.CostForSplit(
				usage.Model,
				usage.InputTokens+usage.CacheReadTokens+usage.CacheCreationTokens,
				usage.OutputTokens,
			)
			if costWithoutCache > usage.CostUSD {
				savingsUSD = costWithoutCache - usage.CostUSD
			}
		}
		metrics.RecordPromptCacheMetrics(usage.Provider, usage.Model,
			usage.CacheReadTokens, usage.CacheCreationTokens, savingsUSD)
	}

	// Start cleanup goroutine if not already running (idempotency key already marked above)
	// 若尚未启动则启动清理 goroutine（幂等键已在上文标记）
	if usage.IdempotencyKey != "" {
		bm.startIdempotencyCleanup()
	}

	return nil
}

// GetUsageReport generates a usage report for a user/session/task
// GetUsageReport 生成 user/session/task 的用量报告
func (bm *BudgetManager) GetUsageReport(ctx context.Context, filters UsageFilters) (*UsageReport, error) {
	report := &UsageReport{
		StartTime: filters.StartTime,
		EndTime:   filters.EndTime,
	}

	// Query database for usage records using migration schema
	// 按 migration 的 schema 查询数据库中的用量记录
	// Join with task_executions to support filtering by workflow_id (common case) or task_id
	// 关联 task_executions，以支持按 workflow_id（常见场景）或 task_id 过滤
	rows, err := bm.db.QueryContext(ctx, `
		SELECT tu.user_id, tu.task_id, tu.model, tu.provider,
		       SUM(tu.prompt_tokens) as input_total,
		       SUM(tu.completion_tokens) as output_total,
		       SUM(tu.total_tokens) as total_tokens,
		       SUM(tu.cache_aware_total_tokens) as cache_aware_total_tokens,
		       SUM(tu.cost_usd) as total_cost,
		       COUNT(*) as request_count
		FROM token_usage tu
		LEFT JOIN task_executions te ON tu.task_id = te.id
		WHERE tu.created_at BETWEEN $1 AND $2
		  AND ($3 = '' OR tu.user_id::text = $3)
		  AND ($4 = '' OR tu.task_id::text = $4 OR te.workflow_id = $4)
		GROUP BY tu.user_id, tu.task_id, tu.model, tu.provider
		ORDER BY total_tokens DESC
	`, filters.StartTime, filters.EndTime, filters.UserID, filters.TaskID)

	if err != nil {
		return nil, fmt.Errorf("failed to query usage: %w", err)
	}
	defer rows.Close()

	var totalTokens int
	var cacheAwareTotalTokens int
	var totalCost float64

	for rows.Next() {
		var detail UsageDetail
		err := rows.Scan(
			&detail.UserID, &detail.TaskID,
			&detail.Model, &detail.Provider,
			&detail.InputTokens, &detail.OutputTokens, &detail.TotalTokens,
			&detail.CacheAwareTotalTokens,
			&detail.CostUSD, &detail.RequestCount,
		)
		if err != nil {
			continue
		}

		report.Details = append(report.Details, detail)
		totalTokens += detail.TotalTokens
		cacheAwareTotalTokens += detail.CacheAwareTotalTokens
		totalCost += detail.CostUSD

		// Update model breakdown
		// 更新按 model 的汇总
		if report.ModelBreakdown == nil {
			report.ModelBreakdown = make(map[string]ModelUsage)
		}
		modelKey := fmt.Sprintf("%s:%s", detail.Provider, detail.Model)
		mb := report.ModelBreakdown[modelKey]
		mb.Tokens += detail.TotalTokens
		mb.CacheAwareTokens += detail.CacheAwareTotalTokens
		mb.Cost += detail.CostUSD
		mb.Requests += detail.RequestCount
		report.ModelBreakdown[modelKey] = mb
	}

	report.TotalTokens = totalTokens
	report.CacheAwareTotalTokens = cacheAwareTotalTokens
	report.TotalCostUSD = totalCost

	return report, nil
}

// Helper methods
// 辅助方法

func (bm *BudgetManager) getUserBudget(userID string) *TokenBudget {
	if budget, ok := bm.userBudgets[userID]; ok {
		return budget
	}
	// Return a transient default without mutating shared maps
	// 返回临时默认值，不修改共享 map
	return &TokenBudget{
		HardLimit:        true,
		WarningThreshold: 0.8,
	}
}

func (bm *BudgetManager) getSessionBudget(sessionID string) *TokenBudget {
	if budget, ok := bm.sessionBudgets[sessionID]; ok {
		return budget
	}
	// Return a transient default without mutating shared maps
	// 返回临时默认值，不修改共享 map
	return &TokenBudget{
		TaskBudget:       bm.defaultTaskBudget,
		SessionBudget:    bm.defaultSessionBudget,
		HardLimit:        false,
		WarningThreshold: 0.8,
		RequireApproval:  false,
	}
}

func (bm *BudgetManager) estimateCost(tokens int, model string) float64 {
	// Use centralized pricing for estimation; if model unknown, defaults are applied
	// 使用集中式定价做估算；model 未知时套用默认值
	return pricing.CostForTokens(model, tokens)
}

func (bm *BudgetManager) storeUsage(ctx context.Context, usage *BudgetTokenUsage) error {
	// Skip database operations if no database is configured (e.g., in tests)
	// 未配置数据库时跳过数据库操作（例如测试中）
	if bm.db == nil {
		return nil
	}

	// Handle user_id - convert to UUID or lookup/create user
	// 处理 user_id——转换为 UUID，或查找/创建 user
	var userUUID *uuid.UUID
	if usage.UserID != "" {
		parsed, err := uuid.Parse(usage.UserID)
		if err == nil {
			// Valid UUID
			// 合法 UUID
			userUUID = &parsed
		} else {
			// Not a UUID, lookup or create user by external_id
			// 不是 UUID，按 external_id 查找或创建 user
			var uid uuid.UUID
			err := bm.db.QueryRowContext(ctx,
				"SELECT id FROM users WHERE external_id = $1",
				usage.UserID,
			).Scan(&uid)

			if err != nil {
				// User doesn't exist, create it
				// user 不存在，创建之
				uid = uuid.New()
				// Use QueryRowContext with RETURNING to properly get the id on conflict
				// 使用 QueryRowContext + RETURNING，在冲突时也能正确取回 id
				err = bm.db.QueryRowContext(ctx,
					"INSERT INTO users (id, external_id, created_at, updated_at) VALUES ($1, $2, NOW(), NOW()) ON CONFLICT (external_id) DO UPDATE SET updated_at = NOW() RETURNING id",
					uid, usage.UserID,
				).Scan(&uid)
				if err != nil {
					bm.logger.Error("Failed to create or retrieve user",
						zap.String("user_id", usage.UserID),
						zap.Error(err))
					// Return error instead of silently continuing
					// 返回错误，而不是静默继续
					return fmt.Errorf("failed to resolve user_id %s: %w", usage.UserID, err)
				}
				userUUID = &uid
			} else {
				userUUID = &uid
			}
		}
	}

	// Handle task_id - convert to UUID, verify existence, or resolve by workflow_id; otherwise store NULL
	// 处理 task_id——转换为 UUID、校验是否存在，或按 workflow_id 解析；否则存 NULL
	var taskUUID *uuid.UUID
	if usage.TaskID != "" {
		if parsed, err := uuid.Parse(usage.TaskID); err == nil {
			// Parsed as UUID; verify it exists in task_executions
			// 已解析为 UUID；校验其在 task_executions 中存在
			exists := false
			if bm.db != nil {
				if err := bm.db.QueryRowContext(ctx,
					"SELECT EXISTS(SELECT 1 FROM task_executions WHERE id = $1)",
					parsed,
				).Scan(&exists); err != nil {
					bm.logger.Warn("Failed to verify task_id UUID existence",
						zap.String("task_id", usage.TaskID),
						zap.Error(err))
				}
			}
			if exists {
				taskUUID = &parsed
				bm.logger.Debug("Successfully verified task_id UUID",
					zap.String("task_id", parsed.String()))
			} else if bm.db != nil {
				// Might actually be a workflow_id that looks like a UUID; try resolving by workflow_id
				// 可能是形似 UUID 的 workflow_id，改为按 workflow_id 解析
				bm.logger.Debug("task_id UUID not found in task_executions; attempting workflow_id resolution",
					zap.String("task_id", usage.TaskID))
				var resolved uuid.UUID
				if err2 := bm.db.QueryRowContext(ctx,
					"SELECT id FROM task_executions WHERE workflow_id = $1 LIMIT 1",
					usage.TaskID,
				).Scan(&resolved); err2 == nil {
					taskUUID = &resolved
					bm.logger.Info("Resolved task_id by workflow_id after UUID check failed",
						zap.String("workflow_id", usage.TaskID),
						zap.String("resolved_task_id", resolved.String()))
				} else {
					bm.logger.Warn("task_id UUID not found and no matching workflow_id; storing NULL",
						zap.String("task_id", usage.TaskID),
						zap.String("user_id", usage.UserID),
						zap.String("model", usage.Model),
						zap.Error(err2))
				}
			} else {
				bm.logger.Warn("task_id UUID provided but DB unavailable; storing NULL",
					zap.String("task_id", usage.TaskID))
			}
		} else if bm.db != nil {
			// Not a UUID: attempt to resolve by workflow_id -> task_executions.id
			// 不是 UUID：尝试按 workflow_id 解析为 task_executions.id
			bm.logger.Debug("task_id is not a UUID; attempting workflow_id resolution",
				zap.String("task_id", usage.TaskID),
				zap.Error(err))
			var resolved uuid.UUID
			if err2 := bm.db.QueryRowContext(ctx,
				"SELECT id FROM task_executions WHERE workflow_id = $1 LIMIT 1",
				usage.TaskID,
			).Scan(&resolved); err2 == nil {
				taskUUID = &resolved
				bm.logger.Info("Successfully resolved task_id by workflow_id",
					zap.String("workflow_id", usage.TaskID),
					zap.String("resolved_task_id", resolved.String()))
			} else {
				bm.logger.Warn("Invalid task_id format and no matching workflow_id; storing NULL",
					zap.String("task_id", usage.TaskID),
					zap.String("user_id", usage.UserID),
					zap.String("model", usage.Model),
					zap.String("parse_error", err.Error()),
					zap.Error(err2))
			}
		} else {
			bm.logger.Warn("Invalid task_id format and DB unavailable; storing NULL",
				zap.String("task_id", usage.TaskID),
				zap.Error(err))
		}
	}

	// Store using schema that matches migration: prompt_tokens, completion_tokens, created_at
	// 按与 migration 一致的 schema 写入：prompt_tokens、completion_tokens、created_at
	_, err := bm.db.ExecContext(ctx, `
		INSERT INTO token_usage (
			user_id, task_id, agent_id, provider, model,
			prompt_tokens, completion_tokens, total_tokens, cost_usd,
			cache_read_tokens, cache_creation_tokens, cache_aware_total_tokens, call_sequence
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`, userUUID, taskUUID, usage.AgentID, usage.Provider, usage.Model,
		usage.InputTokens, usage.OutputTokens, usage.TotalTokens, usage.CostUSD,
		usage.CacheReadTokens, usage.CacheCreationTokens, usage.CacheAwareTotalTokens, usage.CallSequence)

	if err != nil {
		bm.logger.Error("Failed to store token usage", zap.Error(err))
		return fmt.Errorf("failed to store usage: %w", err)
	}

	return nil
}

func (bm *BudgetManager) initializeTables() {
	// Note: Tables are now created via migrations, specifically:
	// - token_usage table in 001_initial_schema.sql
	// - budget_policies table can be added in a future migration
	// This method is kept for backward compatibility but does minimal work
	// 注意：相关表目前由 migration 创建，具体为：token_usage 表见 001_initial_schema.sql；
	// budget_policies 表可在后续 migration 中添加。本方法仅为向后兼容保留，只做少量工作。

	bm.logger.Info("Budget manager initialized - using migration-managed schema")
}

// Types for results and filters
// 结果与过滤器的类型

type BudgetCheckResult struct {
	CanProceed             bool     `json:"can_proceed"`
	RequireApproval        bool     `json:"require_approval"`
	Reason                 string   `json:"reason"`
	Warnings               []string `json:"warnings"`
	EstimatedCost          float64  `json:"estimated_cost"`
	RemainingTaskBudget    int      `json:"remaining_task_budget"`
	RemainingSessionBudget int      `json:"remaining_session_budget"`
	// RemainingDailyBudget removed
	// 已移除 RemainingDailyBudget
}

type UsageFilters struct {
	UserID    string
	SessionID string
	TaskID    string
	StartTime time.Time
	EndTime   time.Time
}

type UsageReport struct {
	StartTime time.Time `json:"start_time"`
	EndTime   time.Time `json:"end_time"`
	// TotalTokens is OpenAI-compatible (= prompt + completion). For
	// quota/billing use CacheAwareTotalTokens, which adds cache classes.
	// TotalTokens 与 OpenAI 兼容（= prompt + completion）；配额/计费请使用 CacheAwareTotalTokens，它额外包含 cache 类别。
	TotalTokens           int                   `json:"total_tokens"`
	CacheAwareTotalTokens int                   `json:"cache_aware_total_tokens"`
	TotalCostUSD          float64               `json:"total_cost_usd"`
	Details               []UsageDetail         `json:"details"`
	ModelBreakdown        map[string]ModelUsage `json:"model_breakdown"`
}

type UsageDetail struct {
	UserID                string  `json:"user_id"`
	SessionID             string  `json:"session_id"`
	TaskID                string  `json:"task_id"`
	Model                 string  `json:"model"`
	Provider              string  `json:"provider"`
	InputTokens           int     `json:"input_tokens"`
	OutputTokens          int     `json:"output_tokens"`
	TotalTokens           int     `json:"total_tokens"`
	CacheAwareTotalTokens int     `json:"cache_aware_total_tokens"`
	CostUSD               float64 `json:"cost_usd"`
	RequestCount          int     `json:"request_count"`
}

type ModelUsage struct {
	Tokens              int     `json:"tokens"`
	CacheAwareTokens    int     `json:"cache_aware_tokens,omitempty"`
	Cost                float64 `json:"cost"`
	Requests            int     `json:"requests"`
	CacheReadTokens     int     `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens int     `json:"cache_creation_tokens,omitempty"`
}

// Enhanced Budget Manager Features - Backpressure and Circuit Breaker
// 增强的 Budget Manager 特性——背压与熔断器

// CircuitBreaker tracks failure patterns
// CircuitBreaker 追踪失败模式
type CircuitBreaker struct {
	failureCount    int32
	lastFailureTime time.Time
	state           string // "closed", "open", "half-open" 注：状态取值：closed、open、half-open
	config          CircuitBreakerConfig
	successCount    int32
	mu              sync.RWMutex
}

// CircuitBreakerConfig defines circuit breaker parameters
// CircuitBreakerConfig 定义熔断器参数
type CircuitBreakerConfig struct {
	FailureThreshold int
	ResetTimeout     time.Duration
	HalfOpenRequests int
}

// PriorityTier defines budget allocation priorities
// PriorityTier 定义 budget 分配优先级
type PriorityTier struct {
	Priority         int
	BudgetMultiplier float64
}

// BackpressureResult extends BudgetCheckResult with backpressure info
// BackpressureResult 在 BudgetCheckResult 之上扩展了背压信息
type BackpressureResult struct {
	*BudgetCheckResult
	BackpressureActive bool   `json:"backpressure_active"`
	BackpressureDelay  int    `json:"backpressure_delay_ms"`
	CircuitBreakerOpen bool   `json:"circuit_breaker_open"`
	BudgetPressure     string `json:"budget_pressure"` // low, medium, high, critical 注：取值：low、medium、high、critical
}

// Enhanced Budget Manager Methods
// 增强的 Budget Manager 方法

// CheckBudgetWithBackpressure checks budget and applies backpressure if needed
// CheckBudgetWithBackpressure 校验 budget，并在需要时施加背压
func (bm *BudgetManager) CheckBudgetWithBackpressure(
	ctx context.Context, userID, sessionID, taskID string, estimatedTokens int,
) (*BackpressureResult, error) {

	// Regular budget check
	// 常规 budget 校验
	baseResult, err := bm.CheckBudget(ctx, userID, sessionID, taskID, estimatedTokens)
	if err != nil {
		return nil, err
	}

	result := &BackpressureResult{
		BudgetCheckResult: baseResult,
	}

	// Calculate usage percentage INCLUDING the new tokens (ensure budget exists)
	// 计算包含新增 token 在内的使用率（确保 budget 已存在）
	// Use session budget for backpressure calculation (daily budget removed)
	// 背压计算使用 session budget（已移除日 budget）
	// Copy required fields under lock to avoid data races on concurrent updates
	// 在锁内复制所需字段，避免并发更新导致数据竞争
	var (
		sbExists      bool
		sbTokensUsed  int
		sbBudgetLimit int
	)
	bm.mu.RLock()
	if sb, ok := bm.sessionBudgets[sessionID]; ok && sb != nil {
		sbExists = true
		sbTokensUsed = sb.SessionTokensUsed
		sbBudgetLimit = sb.SessionBudget
	}
	bm.mu.RUnlock()

	var usagePercent float64
	if sbExists && sbBudgetLimit > 0 {
		projectedUsage := sbTokensUsed + estimatedTokens
		usagePercent = float64(projectedUsage) / float64(sbBudgetLimit)
	} else {
		usagePercent = 0 // No budget defined, no backpressure 注：未定义 budget，不施加背压
	}

	// Apply backpressure if threshold exceeded
	// 超过阈值时施加背压
	if usagePercent >= bm.backpressureThreshold {
		result.BackpressureActive = true
		result.BackpressureDelay = bm.calculateBackpressureDelay(usagePercent)
	}

	// Determine budget pressure level
	// 判定 budget 压力等级
	result.BudgetPressure = bm.calculatePressureLevel(usagePercent)

	return result, nil
}

// calculateBackpressureDelay calculates delay based on usage
// calculateBackpressureDelay 依据使用率计算延迟
func (bm *BudgetManager) calculateBackpressureDelay(usagePercent float64) int {
	if usagePercent < bm.backpressureThreshold {
		return 0
	}

	// Map usage percentage to delay ranges
	// 将使用率映射到延迟区间
	if usagePercent >= 1.0 {
		return bm.maxBackpressureDelay // At or over limit 注：达到或超出限额
	} else if usagePercent >= 0.95 {
		return 1500 // 95-100%: 1000-2000ms range (returning midpoint) 95-100%：1000-2000ms 注：区间（返回中值）
	} else if usagePercent >= 0.9 {
		return 750 // 90-95%: 500-1000ms range (returning midpoint) 90-95%：500-1000ms 注：区间（返回中值）
	} else if usagePercent >= 0.85 {
		return 300 // 85-90%: medium delay 85-90%：注：中等延迟
	} else if usagePercent >= 0.8 {
		return 50 // 80-85%: 10-100ms range (returning midpoint) 80-85%：10-100ms 注：区间（返回中值）
	}

	return 0
}

// calculatePressureLevel determines the budget pressure level
// calculatePressureLevel 判定 budget 压力等级
func (bm *BudgetManager) calculatePressureLevel(usagePercent float64) string {
	switch {
	case usagePercent < 0.5:
		return "low"
	case usagePercent < 0.75:
		return "medium"
	case usagePercent < 0.9:
		return "high"
	default:
		return "critical"
	}
}

// SetUserBudget sets budget for a user
// SetUserBudget 设置 user 的 budget
func (bm *BudgetManager) SetUserBudget(userID string, budget *TokenBudget) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.userBudgets[userID] = budget
}

// SetSessionBudget sets budget for a session
// SetSessionBudget 设置 session 的 budget
func (bm *BudgetManager) SetSessionBudget(sessionID string, budget *TokenBudget) {
	bm.mu.Lock()
	defer bm.mu.Unlock()
	bm.sessionBudgets[sessionID] = budget
}

// SetRateLimit sets rate limit for a user
// SetRateLimit 设置 user 的限流
func (bm *BudgetManager) SetRateLimit(userID string, requestsPerInterval int, interval time.Duration) {
	bm.rateLimitsmu.Lock()
	defer bm.rateLimitsmu.Unlock()

	// Calculate rate per second
	// 计算每秒速率
	ratePerSecond := float64(requestsPerInterval) / interval.Seconds()
	bm.rateLimiters[userID] = rate.NewLimiter(rate.Limit(ratePerSecond), requestsPerInterval)
}

// CheckRateLimit checks if request is allowed under rate limit
// CheckRateLimit 校验请求在限流下是否放行
func (bm *BudgetManager) CheckRateLimit(userID string) bool {
	bm.rateLimitsmu.RLock()
	limiter, exists := bm.rateLimiters[userID]
	bm.rateLimitsmu.RUnlock()

	if !exists {
		return true // No rate limit configured 注：未配置限流
	}

	return limiter.Allow()
}

// GetBudgetPressure returns the current budget pressure level
// GetBudgetPressure 返回当前 budget 压力等级
// NOTE: Daily budget removed - pressure tracking now done per-session via CheckTokenBudgetWithBackpressure
// 注意：已移除日 budget——压力追踪现由 CheckTokenBudgetWithBackpressure 按 session 进行
func (bm *BudgetManager) GetBudgetPressure(userID string) string {
	// Daily budget tracking removed, always return low pressure
	// 已移除日 budget 追踪，始终返回低压力
	// Use CheckTokenBudgetWithBackpressure for per-session pressure monitoring
	// 按 session 的压力监控请使用 CheckTokenBudgetWithBackpressure
	return "low"
}

// ResetUserUsage resets usage counters for a user
// ResetUserUsage 重置 user 的用量计数
func (bm *BudgetManager) ResetUserUsage(userID string) {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	if budget, ok := bm.userBudgets[userID]; ok {
		// Daily/monthly fields removed
		// 已移除日/月字段
		budget.TaskTokensUsed = 0
		budget.SessionTokensUsed = 0
	}
}

// Circuit Breaker Methods
// 熔断器方法

// ConfigureCircuitBreaker sets up circuit breaker for a user
// ConfigureCircuitBreaker 为 user 配置熔断器
func (bm *BudgetManager) ConfigureCircuitBreaker(userID string, config CircuitBreakerConfig) {
	bm.cbMu.Lock()
	defer bm.cbMu.Unlock()

	bm.circuitBreakers[userID] = &CircuitBreaker{
		state:  "closed",
		config: config,
	}
}

// RecordFailure records a failure for circuit breaker
// RecordFailure 为熔断器记录一次失败
func (bm *BudgetManager) RecordFailure(userID string) {
	bm.cbMu.RLock()
	cb, exists := bm.circuitBreakers[userID]
	bm.cbMu.RUnlock()

	if !exists {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	atomic.AddInt32(&cb.failureCount, 1)
	cb.lastFailureTime = time.Now()

	if int(cb.failureCount) >= cb.config.FailureThreshold {
		cb.state = "open"
	}
}

// RecordSuccess records a success for circuit breaker
// RecordSuccess 为熔断器记录一次成功
func (bm *BudgetManager) RecordSuccess(userID string) {
	bm.cbMu.RLock()
	cb, exists := bm.circuitBreakers[userID]
	bm.cbMu.RUnlock()

	if !exists {
		return
	}

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if cb.state == "half-open" {
		atomic.AddInt32(&cb.successCount, 1)
		if int(cb.successCount) >= cb.config.HalfOpenRequests {
			cb.state = "closed"
			atomic.StoreInt32(&cb.failureCount, 0)
			atomic.StoreInt32(&cb.successCount, 0)
		}
	}
}

// GetCircuitState returns current circuit breaker state
// GetCircuitState 返回当前熔断器状态
func (bm *BudgetManager) GetCircuitState(userID string) string {
	bm.cbMu.RLock()
	cb, exists := bm.circuitBreakers[userID]
	bm.cbMu.RUnlock()

	if !exists {
		return "closed"
	}
	// Lock for possible state transition without mixed unlock/lock
	// 加锁以处理可能的状态转换，避免 unlock/lock 混用
	cb.mu.Lock()
	if cb.state == "open" && time.Since(cb.lastFailureTime) > cb.config.ResetTimeout {
		cb.state = "half-open"
		atomic.StoreInt32(&cb.successCount, 0)
	}
	state := cb.state
	cb.mu.Unlock()
	return state
}

// CheckBudgetWithCircuitBreaker includes circuit breaker check
// CheckBudgetWithCircuitBreaker 包含熔断器检查
func (bm *BudgetManager) CheckBudgetWithCircuitBreaker(
	ctx context.Context, userID, sessionID, taskID string, estimatedTokens int,
) (*BackpressureResult, error) {

	// Check circuit breaker first
	// 先检查熔断器
	state := bm.GetCircuitState(userID)
	if state == "open" {
		return &BackpressureResult{
			BudgetCheckResult: &BudgetCheckResult{
				CanProceed: false,
				Reason:     "Circuit breaker is open due to repeated failures",
			},
			CircuitBreakerOpen: true,
		}, nil
	}

	// Allow limited requests in half-open state
	// half-open 状态下放行有限请求
	if state == "half-open" {
		bm.cbMu.RLock()
		cb := bm.circuitBreakers[userID]
		bm.cbMu.RUnlock()

		if cb != nil && int(atomic.LoadInt32(&cb.successCount)) >= cb.config.HalfOpenRequests {
			return &BackpressureResult{
				BudgetCheckResult: &BudgetCheckResult{
					CanProceed: false,
					Reason:     "Circuit breaker in half-open state, test quota exceeded",
				},
				CircuitBreakerOpen: true,
			}, nil
		}
	}

	return bm.CheckBudgetWithBackpressure(ctx, userID, sessionID, taskID, estimatedTokens)
}

// Priority-based allocation methods
// 基于优先级的分配方法

// SetPriorityTiers configures priority tiers
// SetPriorityTiers 配置优先级层级
func (bm *BudgetManager) SetPriorityTiers(tiers map[string]PriorityTier) {
	bm.priorityMu.Lock()
	defer bm.priorityMu.Unlock()
	bm.priorityTiers = tiers
}

// AllocateBudgetByPriority allocates budget based on priority
// AllocateBudgetByPriority 按优先级分配 budget
func (bm *BudgetManager) AllocateBudgetByPriority(ctx context.Context, baseBudget int, priority string) int {
	bm.priorityMu.RLock()
	defer bm.priorityMu.RUnlock()

	if tier, ok := bm.priorityTiers[priority]; ok {
		return int(float64(baseBudget) * tier.BudgetMultiplier)
	}

	return baseBudget
}

// AllocateBudgetAcrossSessions distributes budget across sessions
// AllocateBudgetAcrossSessions 在多个 session 间分配 budget
func (bm *BudgetManager) AllocateBudgetAcrossSessions(ctx context.Context, sessions []string, totalBudget int) {
	bm.allocationMu.Lock()
	defer bm.allocationMu.Unlock()

	if len(sessions) == 0 {
		return
	}

	perSession := totalBudget / len(sessions)
	for _, session := range sessions {
		bm.sessionAllocations[session] = perSession
	}
}

// GetSessionAllocation returns allocated budget for a session
// GetSessionAllocation 返回 session 的已分配 budget
func (bm *BudgetManager) GetSessionAllocation(sessionID string) int {
	bm.allocationMu.RLock()
	defer bm.allocationMu.RUnlock()
	return bm.sessionAllocations[sessionID]
}

// ReallocateBudgetsByUsage redistributes budget based on usage patterns
// ReallocateBudgetsByUsage 依据使用模式重新分配 budget
func (bm *BudgetManager) ReallocateBudgetsByUsage(ctx context.Context, sessions []string) {
	// Gather usage under read lock
	// 在读锁下收集用量
	type sessionUsage struct {
		id    string
		usage int
	}
	usages := make([]sessionUsage, 0, len(sessions))
	totalUsage := 0
	bm.mu.RLock()
	for _, session := range sessions {
		if budget, ok := bm.sessionBudgets[session]; ok {
			u := sessionUsage{id: session, usage: budget.SessionTokensUsed}
			usages = append(usages, u)
			totalUsage += u.usage
		}
	}
	bm.mu.RUnlock()

	if totalUsage == 0 {
		return
	}

	// Update allocations under allocation lock only
	// 仅在 allocation 锁下更新分配额
	bm.allocationMu.Lock()
	defer bm.allocationMu.Unlock()

	totalBudget := 0
	for _, session := range sessions {
		if allocation, ok := bm.sessionAllocations[session]; ok {
			totalBudget += allocation
		}
	}
	for _, u := range usages {
		proportion := float64(u.usage) / float64(totalUsage)
		smoothed := proportion*0.7 + 0.3/float64(len(sessions))
		bm.sessionAllocations[u.id] = int(float64(totalBudget) * smoothed)
	}
}

// NewEnhancedBudgetManager creates an enhanced budget manager (compatibility wrapper)
// NewEnhancedBudgetManager 创建增强的 budget manager（兼容性包装）
func NewEnhancedBudgetManager(db *sql.DB, logger *zap.Logger) *BudgetManager {
	return NewBudgetManager(db, logger)
}

// emitBudgetThresholdEvent emits a streaming event when budget thresholds are crossed
// emitBudgetThresholdEvent 在跨越 budget 阈值时发出 streaming 事件
func (bm *BudgetManager) emitBudgetThresholdEvent(taskID, sessionID, message string, payload map[string]any) {
	// This is best-effort, non-blocking - we don't want budget monitoring to fail the task
	// 这是尽力而为、非阻塞的操作——budget 监控不应导致 task 失败
	// The streaming manager will publish to Redis for SSE/WebSocket delivery
	// streaming manager 会发布到 Redis，经 SSE/WebSocket 投递
	if taskID == "" {
		return // No workflow ID to emit event to 注：没有可投递事件的 workflow ID
	}

	event := streaming.Event{
		WorkflowID: taskID,
		Type:       "BUDGET_THRESHOLD",
		AgentID:    "budget-manager",
		Message:    message,
		Payload:    payload,
		Timestamp:  time.Now(),
	}

	streaming.Get().Publish(taskID, event)
	bm.logger.Debug("Emitted BUDGET_THRESHOLD event",
		zap.String("task_id", taskID),
		zap.String("session_id", sessionID),
		zap.String("message", message),
	)
}

// Idempotency Key TTL Cleanup
// 幂等键 TTL 清理

// startIdempotencyCleanup starts a background goroutine that periodically cleans up
// expired idempotency keys. Uses sync.Once to ensure only one cleanup goroutine runs.
// startIdempotencyCleanup 启动后台 goroutine 定期清理过期的幂等键；使用 sync.Once 保证只运行一个清理 goroutine。
func (bm *BudgetManager) startIdempotencyCleanup() {
	bm.cleanupOnce.Do(func() {
		go bm.idempotencyCleanupLoop()
		bm.logger.Info("Started idempotency key cleanup goroutine",
			zap.Duration("ttl", bm.idempotencyTTL))
	})
}

// idempotencyCleanupLoop runs periodically to remove expired idempotency keys.
// Cleanup interval is set to half the TTL for timely removal.
// 清理间隔设为 TTL 的一半，以便及时移除
func (bm *BudgetManager) idempotencyCleanupLoop() {
	// Run cleanup at half the TTL interval for timely expiration
	// 以 TTL 一半的间隔运行清理，保证及时过期
	cleanupInterval := max(bm.idempotencyTTL/2,
		// Minimum 1 minute interval
		// 最小间隔 1 分钟
		time.Minute)

	ticker := time.NewTicker(cleanupInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			bm.cleanupExpiredIdempotencyKeys()
		case <-bm.cleanupStop:
			bm.logger.Info("Stopping idempotency key cleanup goroutine")
			return
		}
	}
}

// cleanupExpiredIdempotencyKeys removes idempotency keys that have exceeded their TTL.
// This prevents unbounded memory growth from accumulated idempotency records.
// 防止幂等记录累积导致内存无限增长
func (bm *BudgetManager) cleanupExpiredIdempotencyKeys() {
	now := time.Now()
	expiredKeys := make([]string, 0)

	// First pass: identify expired keys under read lock
	// 第一遍：在读锁下找出过期键
	bm.idempotencyMu.RLock()
	for key, processedAt := range bm.processedUsage {
		if now.Sub(processedAt) > bm.idempotencyTTL {
			expiredKeys = append(expiredKeys, key)
		}
	}
	totalKeys := len(bm.processedUsage)
	bm.idempotencyMu.RUnlock()

	// No expired keys, skip write lock
	// 无过期键，跳过写锁
	if len(expiredKeys) == 0 {
		return
	}

	// Second pass: remove expired keys under write lock
	// 第二遍：在写锁下移除过期键
	bm.idempotencyMu.Lock()
	for _, key := range expiredKeys {
		// Double-check the key still exists and is still expired
		// 再次确认该键仍存在且仍已过期
		if processedAt, exists := bm.processedUsage[key]; exists {
			if now.Sub(processedAt) > bm.idempotencyTTL {
				delete(bm.processedUsage, key)
			}
		}
	}
	remainingKeys := len(bm.processedUsage)
	bm.idempotencyMu.Unlock()

	bm.logger.Debug("Cleaned up expired idempotency keys",
		zap.Int("expired_count", len(expiredKeys)),
		zap.Int("total_before", totalKeys),
		zap.Int("remaining", remainingKeys))
}

// StopIdempotencyCleanup stops the background cleanup goroutine.
// StopIdempotencyCleanup 停止后台清理 goroutine。
// Call this during graceful shutdown.
// 在优雅关闭时调用。
func (bm *BudgetManager) StopIdempotencyCleanup() {
	select {
	case <-bm.cleanupStop:
		// Already closed
		// 已关闭
	default:
		close(bm.cleanupStop)
	}
}

// GetIdempotencyKeyCount returns the current number of tracked idempotency keys.
// GetIdempotencyKeyCount 返回当前追踪的幂等键数量。
// Useful for monitoring and debugging.
// 便于监控与调试。
func (bm *BudgetManager) GetIdempotencyKeyCount() int {
	bm.idempotencyMu.RLock()
	defer bm.idempotencyMu.RUnlock()
	return len(bm.processedUsage)
}
