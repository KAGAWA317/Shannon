package opts

import (
	"fmt"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TokenRecordActivityOptions returns standardized activity options for token recording
// TokenRecordActivityOptions 返回 token 记录的标准 activity options
func TokenRecordActivityOptions() workflow.ActivityOptions {
	return workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	}
}

// WithTokenRecordOptions applies standardized token record activity options to a context
// WithTokenRecordOptions 把标准 token 记录 activity options 应用到 context
func WithTokenRecordOptions(ctx workflow.Context) workflow.Context {
	return workflow.WithActivityOptions(ctx, TokenRecordActivityOptions())
}

// RecordToolCostEntries records synthetic tool cost usage rows from agent metadata.
// For non-budgeted ExecuteAgent paths, tool_cost_entries in the result metadata are
// not persisted automatically. Call this after RecordTokenUsageActivity to capture them.
// 非 budget 的 ExecuteAgent 路径不会自动持久化 metadata 中的 tool_cost_entries，
// 需在 RecordTokenUsageActivity 之后调用本函数补记。
func RecordToolCostEntries(
	ctx workflow.Context,
	result activities.AgentExecutionResult,
	userID, sessionID, taskID string,
) {
	if result.Metadata == nil {
		return
	}
	rawEntries, ok := result.Metadata["tool_cost_entries"]
	if !ok {
		return
	}
	entries, ok := rawEntries.([]any)
	if !ok || len(entries) == 0 {
		return
	}

	recCtx := WithTokenRecordOptions(ctx)
	for i, raw := range entries {
		em, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		toolName, _ := em["tool"].(string)
		if strings.TrimSpace(toolName) == "" {
			continue
		}
		costModel, _ := em["cost_model"].(string)
		provider, _ := em["provider"].(string)
		syntheticTokens := 7500
		switch v := em["synthetic_tokens"].(type) {
		case int:
			if v > 0 {
				syntheticTokens = v
			}
		case float64:
			if v > 0 {
				syntheticTokens = int(v)
			}
		case int64:
			if v > 0 {
				syntheticTokens = int(v)
			}
		}
		// Read upstream-reported cost (e.g. web_fetch LLM extraction cost)
		// 读取上游上报的 cost（例如 web_fetch 的 LLM 抽取成本）
		var costOverride float64
		if c, ok := em["cost_usd"].(float64); ok && c > 0 {
			costOverride = c
		}

		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       userID,
			SessionID:    sessionID,
			TaskID:       taskID,
			AgentID:      fmt.Sprintf("tool_%s", toolName),
			Model:        costModel,
			Provider:     provider,
			InputTokens:  0,
			OutputTokens: syntheticTokens,
			CostOverride: costOverride,
			Metadata:     map[string]any{"tool_cost_index": i},
		}).Get(recCtx, nil)
	}
}
