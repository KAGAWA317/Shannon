package workflows

import (
	"strings"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/budget"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	ometrics "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/ratecontrol"
)

// BudgetPreflight performs a token budget check with optional backpressure delay.
// It returns the full BackpressureResult so callers can decide how to proceed.
// BudgetPreflight 执行 token 预算检查，可选地带上 backpressure 延迟。
// 它返回完整的 BackpressureResult，以便调用方决定后续处理。
func BudgetPreflight(ctx workflow.Context, input TaskInput, estimatedTokens int) (*budget.BackpressureResult, error) {
	logger := workflow.GetLogger(ctx)

	// Use short activity timeout; this should be fast
	// 使用较短的 activity 超时；该调用应当很快
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})

	var res budget.BackpressureResult
	err := workflow.ExecuteActivity(actx, constants.CheckTokenBudgetWithBackpressureActivity, activities.BudgetCheckInput{
		UserID:          input.UserID,
		SessionID:       input.SessionID,
		TaskID:          workflow.GetInfo(ctx).WorkflowExecution.ID,
		EstimatedTokens: estimatedTokens,
	}).Get(ctx, &res)
	if err != nil {
		return nil, err
	}

	if res.BackpressureActive && res.BackpressureDelay > 0 {
		logger.Info("Applying budget backpressure delay",
			"delay_ms", res.BackpressureDelay,
			"pressure_level", res.BudgetPressure,
		)
		if err := workflow.Sleep(ctx, time.Duration(res.BackpressureDelay)*time.Millisecond); err != nil {
			return nil, err // Return error on cancellation 注：取消时返回错误
		}
	}

	rateControlVersion := workflow.GetVersion(ctx, "provider_rate_control_v1", workflow.DefaultVersion, 1)
	if rateControlVersion >= 1 {
		// Resolve runtime setting from features.yaml + env override
		// 从 features.yaml + 环境变量覆盖中解析运行时设置
		var f *config.Features
		if feats, err := config.Load(); err == nil {
			f = feats
		}
		er := config.ResolveEnforcementRuntime(f)
		if !er.ProviderRateControlEnabled {
			logger.Info("Provider rate control disabled by config/env")
		} else {
			tier := deriveModelTier(input.Context)
			provider := resolveProviderFromContext(input.Context)

			// If provider is unknown, infer from tier's primary provider (models.yaml priority 1)
			// 若 provider 未知，则从该档位的首选 provider 推断（models.yaml priority 1）
			if provider == "unknown" {
				provider = inferProviderFromTier(tier)
			}

			delay := ratecontrol.DelayForRequest(provider, tier, estimatedTokens)
			if delay > 0 {
				logger.Info("Applying provider rate control delay",
					"provider", provider,
					"tier", tier,
					"delay_ms", delay.Milliseconds(),
				)
				// Record metric for rate limit delay
				// 记录限流延迟的 metric
				ometrics.RateLimitDelay.WithLabelValues(provider, tier).Observe(delay.Seconds())
				if err := workflow.Sleep(ctx, delay); err != nil {
					return nil, err // Return error on cancellation 注：取消时返回错误
				}
			}
		}
	}
	return &res, nil
}

// WithAgentBudget returns a child context annotated with a per-agent budget.
// Strategies can pass this context to budgeted activities.
// WithAgentBudget 返回带每 agent 预算注解的子 context。
// 各 strategy 可将该 context 传给带预算的 activity。
func WithAgentBudget(ctx workflow.Context, maxTokens int) workflow.Context {
	return workflow.WithValue(ctx, "agent_budget", maxTokens)
}

// EstimateTokens provides a coarse estimate of tokens needed for executing the plan.
// It mirrors the logic used by the previous budgeted workflow and keeps it central.
// EstimateTokens 粗略估算执行该计划所需的 token 数。
// 它与此前带预算的 workflow 逻辑一致，并集中到此处。
func EstimateTokens(decomp activities.DecompositionResult) int {
	return EstimateTokensWithConfig(decomp, nil)
}

// EstimateTokensWithConfig provides a coarse estimate with configurable thresholds
// EstimateTokensWithConfig 提供可配置阈值的粗略估算
func EstimateTokensWithConfig(decomp activities.DecompositionResult, cfg *activities.WorkflowConfig) int {
	base := 2000
	mul := 1.0

	// Use configurable thresholds with defaults
	// 使用可配置阈值，未配置时取默认值
	mediumThreshold := 0.5
	if cfg != nil && cfg.ComplexityMediumThreshold > 0 {
		mediumThreshold = cfg.ComplexityMediumThreshold
	}

	if decomp.ComplexityScore > mediumThreshold {
		mul = 2.5
	} else if decomp.ComplexityScore > 0.4 {
		mul = 1.5
	}
	n := len(decomp.Subtasks)
	if n == 0 {
		n = 1
	}
	return int(float64(base*n) * mul)
}

func resolveProviderFromContext(ctx map[string]any) string {
	if ctx == nil {
		return "unknown"
	}
	if v, ok := ctx["provider"].(string); ok {
		if provider := strings.ToLower(strings.TrimSpace(v)); provider != "" {
			return provider
		}
	}
	if v, ok := ctx["llm_provider"].(string); ok {
		if provider := strings.ToLower(strings.TrimSpace(v)); provider != "" {
			return provider
		}
	}
	if v, ok := ctx["model"].(string); ok {
		if model := strings.TrimSpace(v); model != "" {
			return strings.ToLower(strings.TrimSpace(detectProviderFromModel(model)))
		}
	}
	return "unknown"
}

// CapBudgetAtQuotaRemaining is a no-op in the open source version.
// Enterprise version caps agentMax for free-tier users based on remaining quota.
// Returns agentMax unchanged.
// CapBudgetAtQuotaRemaining 在开源版中为空操作。
// 企业版会按免费用户剩余配额限制 agentMax。
// 原样返回 agentMax。
// 开源版为空操作：企业版会按免费用户剩余配额限制 agentMax，此处原样返回。
func CapBudgetAtQuotaRemaining(ctx workflow.Context, tenantID string, agentMax int) int {
	return agentMax
}

// inferProviderFromTier returns the most likely provider for a given tier.
// Reads priority-1 provider from config/models.yaml to avoid config drift.
// Falls back to hardcoded defaults if config unavailable.
// inferProviderFromTier 返回给定档位最可能的 provider。
// 从 config/models.yaml 读取 priority-1 的 provider，避免配置漂移。
// 配置不可用时退回硬编码默认值。
// 从 config/models.yaml 读取该档位 priority-1 的 provider，避免配置漂移；配置不可用时退回硬编码默认值。
func inferProviderFromTier(tier string) string {
	// Try to get from config first (data-driven)
	// 优先从配置获取（数据驱动）
	if provider := pricing.GetPriorityOneProvider(tier); provider != "" {
		return provider
	}

	// Fallback to hardcoded defaults if config not available
	// 配置不可用时退回硬编码默认值
	switch tier {
	case "small":
		return "openai" // Default: gpt-5-nano-2025-08-07 is priority 1 注：默认 gpt-5-nano-2025-08-07 为 priority 1
	case "medium":
		return "openai" // Default: gpt-5-2025-08-07 is priority 1 注：默认 gpt-5-2025-08-07 为 priority 1
	case "large":
		return "openai" // Default: gpt-4.1-2025-04-14 is priority 1 注：默认 gpt-4.1-2025-04-14 为 priority 1
	default:
		return "unknown"
	}
}
