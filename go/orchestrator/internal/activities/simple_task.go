package activities

import (
	"context"
	"maps"

	"go.temporal.io/sdk/activity"
	"go.uber.org/zap"
)

// ExecuteSimpleTaskInput contains everything needed for a simple task
// ExecuteSimpleTaskInput 包含简单任务执行所需的全部字段
type ExecuteSimpleTaskInput struct {
	Query          string         `json:"query"`
	UserID         string         `json:"user_id"`
	SessionID      string         `json:"session_id"`
	Context        map[string]any `json:"context"`
	SessionCtx     map[string]any `json:"session_ctx"`
	History        []string       `json:"history"`
	PersonaID      string         `json:"persona_id"`
	SuggestedTools []string       `json:"suggested_tools,omitempty"`
	ToolParameters map[string]any `json:"tool_parameters,omitempty"`
	// Parent workflow ID for unified event streaming
	// 父 workflow ID，用于统一 event streaming。
	ParentWorkflowID string `json:"parent_workflow_id,omitempty"`
}

// ExecuteSimpleTaskResult contains the complete result
// ExecuteSimpleTaskResult 包含完整结果
type ExecuteSimpleTaskResult struct {
	Response        string          `json:"response"`
	TokensUsed      int             `json:"tokens_used"`
	Success         bool            `json:"success"`
	Error           string          `json:"error,omitempty"`
	ModelUsed       string          `json:"model_used,omitempty"`
	Provider        string          `json:"provider,omitempty"`
	DurationMs      int64           `json:"duration_ms,omitempty"`
	ToolExecutions  []ToolExecution `json:"tool_executions,omitempty"`
	ScreenshotPaths []string        `json:"screenshot_paths,omitempty"`
}

// ExecuteSimpleTask executes a simple query with minimal overhead
// This consolidated activity merges context and executes the agent in one step.
// Session updates and vector persistence are handled by the workflow after this activity completes.
// ExecuteSimpleTask 以最小开销执行一次简单 query；
// 这个合并后的 activity 把 context 合并与 agent 执行并到一步完成。
// session 更新与向量持久化由 workflow 在本 activity 结束后处理。
func ExecuteSimpleTask(ctx context.Context, input ExecuteSimpleTaskInput) (ExecuteSimpleTaskResult, error) {
	// Use activity logger for proper Temporal correlation
	// 用 activity logger，保证 Temporal 关联正确。
	activity.GetLogger(ctx).Info("ExecuteSimpleTask activity started",
		"query", input.Query,
		"session_id", input.SessionID,
	)

	// Use zap logger for the core logic which needs *zap.Logger
	// 核心逻辑需要 *zap.Logger，因此改用 zap logger。
	logger := zap.L()
	if logger == nil {
		// Fallback to creating a new logger if global logger is not initialized
		// 全局 logger 未初始化时兜底新建一个。
		logger, _ = zap.NewProduction()
	}

	// Step 1: Merge context (no separate activity)
	// Step 1：合并 context，不单独开一个 activity。
	mergedContext := make(map[string]any)
	maps.Copy(mergedContext, input.Context)
	maps.Copy(mergedContext, input.SessionCtx)

	// Add user_id to context for audit logging in Python tools (e.g., session_file)
	// 把 user_id 放进 context，供 Python tool 做审计日志（如 session_file）。
	if input.UserID != "" {
		if _, exists := mergedContext["user_id"]; !exists {
			mergedContext["user_id"] = input.UserID
		}
	}

	// Step 2: Execute agent using shared helper (not calling activity directly)
	// Step 2：走共享 helper 执行 agent，而不是直接调 activity。
	agentInput := AgentExecutionInput{
		Query:            input.Query,
		AgentID:          "simple-agent",
		Context:          mergedContext,
		Mode:             "simple",
		SessionID:        input.SessionID,
		UserID:           input.UserID,
		History:          input.History,
		PersonaID:        input.PersonaID,
		SuggestedTools:   input.SuggestedTools,
		ToolParameters:   input.ToolParameters,
		ParentWorkflowID: input.ParentWorkflowID,
	}

	var agentResult AgentExecutionResult
	var err error

	// If we have pre-computed tool parameters and tools, use the forced-tools path so events are emitted.
	// 若已预计算好 tool parameters 与 tools，走 forced-tools 路径，以便正常发出 events。
	if len(agentInput.ToolParameters) > 0 && len(agentInput.SuggestedTools) > 0 {
		agentResult, err = ExecuteAgentWithForcedTools(ctx, agentInput)
	} else {
		agentResult, err = executeAgentCore(ctx, agentInput, logger)
	}

	if err != nil {
		logger.Error("Agent execution failed", zap.Error(err))
		return ExecuteSimpleTaskResult{
			Success: false,
			Error:   err.Error(),
		}, err
	}

	// Return the complete result including details needed for persistence
	// The workflow will handle persistence via Temporal activities for better resilience
	// 返回完整结果，包含后续持久化所需的明细；
	// 持久化交给 workflow 通过 Temporal activities 完成，以获得更好的容错。
	return ExecuteSimpleTaskResult{
		Response:        agentResult.Response,
		TokensUsed:      agentResult.TokensUsed,
		Success:         true,
		ModelUsed:       agentResult.ModelUsed,
		Provider:        agentResult.Provider,
		DurationMs:      agentResult.DurationMs,
		ToolExecutions:  agentResult.ToolExecutions,
		ScreenshotPaths: agentResult.ScreenshotPaths,
	}, nil
}
