package activities

import (
	"context"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/metrics"
)

// PatternMetricsInput contains pattern execution metrics
// PatternMetricsInput 保存 pattern 执行指标，由 workflow 侧统计后传入。
type PatternMetricsInput struct {
	Pattern      string
	Version      string
	AgentCount   int
	TokensUsed   int
	Duration     time.Duration
	Improved     bool // For reflection pattern
	WorkflowType string
}

// RecordPatternMetrics records pattern execution metrics
// RecordPatternMetrics 记录 pattern 执行指标；各字段均为可选，为零值时跳过对应的 metric。
func RecordPatternMetrics(ctx context.Context, input PatternMetricsInput) error {
	// Record pattern execution
	// 记录 pattern 执行次数。
	metrics.RecordPatternExecution(input.Pattern, input.Version)

	// Record duration if provided
	// 仅在提供了 duration 时记录耗时。
	if input.Duration > 0 {
		metrics.RecordPatternDuration(input.Pattern, input.Version, input.Duration.Seconds())
	}

	// Record agent executions
	// 记录 agent 执行数。
	if input.AgentCount > 0 {
		metrics.RecordAgentExecution(input.Pattern, input.Version, input.AgentCount)
	}

	// Record token usage
	// 记录 token 消耗。
	if input.TokensUsed > 0 {
		metrics.RecordTokenUsage(input.Pattern, input.Version, input.TokensUsed)
	}

	// Record reflection improvement
	// 只有 reflection pattern 且判定为 improved 时才记录。
	if input.Pattern == "reflection" && input.Improved {
		metrics.RecordReflectionImprovement(input.Version)
	}

	// Record workflow version
	// workflow type 非空时记录 workflow 版本。
	if input.WorkflowType != "" {
		metrics.RecordWorkflowVersion(input.WorkflowType, input.Version)
	}

	return nil
}
