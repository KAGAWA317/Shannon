package workflows

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
)

// persistAgentExecution is a helper to persist agent execution results
// This is a fire-and-forget operation that won't fail the workflow
// persistAgentExecution 是持久化 agent 执行结果的辅助函数
// 这是 fire-and-forget 操作，不会让 workflow 失败。
func persistAgentExecution(ctx workflow.Context, workflowID string, agentID string, input string, result activities.AgentExecutionResult) {
	// Create a new context for persistence with no retries
	// 为持久化创建新的 context，且不重试。
	persistCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Pre-generate agent execution ID using SideEffect for replay safety
	// This allows correlating tool executions with their parent agent execution
	// 用 SideEffect 预生成执行 ID，便于把 tool 执行关联到其所属的 agent 执行记录。
	var agentExecutionID string
	workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New().String()
	}).Get(&agentExecutionID)

	// Determine state based on success
	// 根据成功与否确定 state。
	state := "COMPLETED"
	if !result.Success {
		state = "FAILED"
	}

	// Fire and forget - don't wait for result
	// Fire and forget —— 不等待结果。
	workflow.ExecuteActivity(
		persistCtx,
		activities.PersistAgentExecutionStandalone,
		activities.PersistAgentExecutionInput{
			ID:         agentExecutionID, // Use pre-generated ID 注：使用预生成的 ID
			WorkflowID: workflowID,
			AgentID:    agentID,
			Input:      input,
			Output:     result.Response,
			State:      state,
			TokensUsed: result.TokensUsed,
			ModelUsed:  result.ModelUsed,
			DurationMs: result.DurationMs,
			Error:      result.Error,
		},
	)

	// Persist tool executions if any
	// 若有 tool 执行记录则一并持久化。
	if len(result.ToolExecutions) > 0 {
		for _, tool := range result.ToolExecutions {
			// Convert tool output to string
			// 把 tool 输出转换为字符串。
			outputStr := ""
			if tool.Output != nil {
				// Handle different output types
				// 处理不同的输出类型。
				switch v := tool.Output.(type) {
				case string:
					outputStr = v
				default:
					// Properly serialize complex outputs to JSON
					// 将复杂输出正确序列化为 JSON。
					if jsonBytes, err := json.Marshal(v); err == nil {
						outputStr = string(jsonBytes)
					} else {
						outputStr = "complex output"
					}
				}
			}

			// Extract input params from tool execution (from HTTP path)
			// 从 tool 执行记录中提取 input params（来自 HTTP 路径）。
			inputParamsMap, _ := tool.InputParams.(map[string]any)

			workflow.ExecuteActivity(
				persistCtx,
				activities.PersistToolExecutionStandalone,
				activities.PersistToolExecutionInput{
					WorkflowID:       workflowID,
					AgentID:          agentID,
					AgentExecutionID: agentExecutionID, // Link to parent agent execution 注：关联到父 agent 执行
					ToolName:         tool.Tool,
					InputParams:      inputParamsMap,
					Output:           outputStr,
					Success:          tool.Success,
					TokensConsumed:   0,               // Not provided by agent 注：agent 未提供
					DurationMs:       tool.DurationMs, // From agent-core proto 注：来自 agent-core proto
					Error:            tool.Error,
				},
			)
		}
	}
}
