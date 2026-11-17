package execution

import (
	"encoding/json"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// SequentialConfig controls sequential execution behavior
// SequentialConfig 控制顺序执行的行为
type SequentialConfig struct {
	EmitEvents               bool           // Whether to emit streaming events 注：是否发出流式事件
	Context                  map[string]any // Base context for all agents 注：所有 agent 的基础 context
	PassPreviousResults      bool           // Whether to pass previous results to next agent 注：是否把前序结果传给下一个 agent
	ExtractNumericValues     bool           // Whether to extract numeric values from responses 注：是否从响应中提取数值
	ClearDependentToolParams bool           // Clear tool params for dependent tasks 注：为依赖任务清空 tool 参数
}

// SequentialTask represents a task to execute sequentially
// SequentialTask 表示一个按顺序执行的任务
type SequentialTask struct {
	ID             string
	Description    string
	SuggestedTools []string
	ToolParameters map[string]any
	PersonaID      string
	Role           string
	Dependencies   []string // Tasks this depends on 注：本任务依赖的任务
}

// SequentialResult contains results from sequential execution
// SequentialResult 包含顺序执行的结果
type SequentialResult struct {
	Results     []activities.AgentExecutionResult
	TotalTokens int
	Metadata    map[string]any
}

// ExecuteSequential runs tasks one after another, optionally passing results between them.
// ExecuteSequential 依次运行各任务，可选地在任务之间传递结果。
// Each task can access results from all previous tasks in the sequence.
// 每个任务都可以访问序列中所有前序任务的结果。
func ExecuteSequential(
	ctx workflow.Context,
	tasks []SequentialTask,
	sessionID string,
	history []string,
	config SequentialConfig,
	budgetPerAgent int,
	userID string,
	modelTier string,
) (*SequentialResult, error) {

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting sequential execution",
		"task_count", len(tasks),
		"pass_results", config.PassPreviousResults,
	)

	// Activity options
	// Activity 选项
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: agentStartToCloseTimeout(config.Context, 10*time.Minute),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Execute tasks sequentially
	// 依次执行任务
	var results []activities.AgentExecutionResult
	totalTokens := 0
	successCount := 0
	errorCount := 0

	for i, task := range tasks {
		// Prepare task context
		// 准备任务 context
		taskContext := make(map[string]any)
		maps.Copy(taskContext, config.Context)
		taskContext["role"] = task.Role
		taskContext["task_id"] = task.ID

		// Compute agent name using station names for deterministic, human-readable IDs
		// 使用站点名计算 agent 名称，以获得确定且可读的 ID
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if config.Context != nil {
			if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		agentName := agents.GetAgentName(wid, i)

		// Fetch agent-specific memory if session exists
		// 若 session 存在，则拉取该 agent 专属的 memory
		if sessionID != "" {
			var am activities.FetchAgentMemoryResult
			_ = workflow.ExecuteActivity(ctx,
				activities.FetchAgentMemory,
				activities.FetchAgentMemoryInput{
					SessionID: sessionID,
					AgentID:   agentName,
					TopK:      5,
				}).Get(ctx, &am)
			if len(am.Items) > 0 {
				taskContext["agent_memory"] = am.Items
			}
		}

		// Add previous results to context if configured
		// 若已配置，则把前序结果加入 context
		if config.PassPreviousResults && len(results) > 0 {
			previousResults := make(map[string]any)
			for j, prevResult := range results {
				if j < i && j < len(tasks) {
					resultMap := map[string]any{
						"response": prevResult.Response,
						"tokens":   prevResult.TokensUsed,
						"success":  prevResult.Success,
					}

					// Extract numeric value if configured
					// 若已配置，则提取数值
					if config.ExtractNumericValues {
						if numVal, ok := util.ParseNumericValue(prevResult.Response); ok {
							resultMap["numeric_value"] = numVal
						}
					}

					// Extract tool results if available
					// 若可用，则提取 tool 结果
					if len(prevResult.ToolExecutions) > 0 {
						for _, te := range prevResult.ToolExecutions {
							switch te.Tool {
							case "calculator":
								if te.Output != nil {
									if calcResult, ok := te.Output.(map[string]any); ok {
										if res, ok := calcResult["result"]; ok {
											resultMap["calculator"] = map[string]any{"result": res}
										}
									}
								}
							case "code_executor":
								if te.Output != nil {
									resultMap["code_executor"] = map[string]any{"output": te.Output}
								}
							default:
								if te.Output != nil {
									resultMap[te.Tool] = te.Output
								}
							}
						}
					}

					previousResults[tasks[j].ID] = resultMap
				}
			}
			taskContext["previous_results"] = previousResults
		}

		// Clear tool parameters for dependent tasks if configured
		// 若已配置，则为依赖任务清空 tool 参数
		if config.ClearDependentToolParams && len(task.Dependencies) > 0 && task.ToolParameters != nil {
			task.ToolParameters = nil
		}

		// Emit agent started event
		// 发出 agent 已启动事件
		if config.EmitEvents {
			_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
				activities.EmitTaskUpdateInput{
					WorkflowID: wid,
					EventType:  activities.StreamEventAgentStarted,
					AgentID:    agentName,
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
		}

		logger.Debug("Executing agent for sequential task",
			"task_index", i,
			"task_id", task.ID,
			"suggested_tools", task.SuggestedTools,
		)

		// Execute agent
		// 执行 agent
		var result activities.AgentExecutionResult
		var err error

		if budgetPerAgent > 0 {
			// Execute with budget
			// 带 budget 执行
			// Use parent workflow ID when available, otherwise fallback to child ID (trim suffix if present)
			// 有父 workflow ID 时优先使用，否则回退到子 ID（若存在后缀则去掉）
			taskID := wid
			if idx := strings.LastIndex(wid, "_"); idx > 0 {
				taskID = wid[:idx]
			}
			err = workflow.ExecuteActivity(ctx,
				constants.ExecuteAgentWithBudgetActivity,
				activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            task.Description,
						AgentID:          agentName,
						Context:          taskContext,
						Mode:             "standard",
						SessionID:        sessionID,
						UserID:           userID,
						History:          history,
						SuggestedTools:   task.SuggestedTools,
						ToolParameters:   task.ToolParameters,
						PersonaID:        task.PersonaID,
						ParentWorkflowID: wid,
					},
					MaxTokens: budgetPerAgent,
					UserID:    userID,
					TaskID:    taskID,
					ModelTier: modelTier,
				}).Get(ctx, &result)
		} else {
			// Execute without budget
			// 不带 budget 执行
			// Inject model_tier to taskContext to ensure consistent tier selection
			// 向 taskContext 注入 model_tier，以保证 tier 选择一致
			// (budget path injects via BudgetedAgentInput.ModelTier -> budget.go:298)
			// （budget 路径通过 BudgetedAgentInput.ModelTier 注入 -> budget.go:298）
			if modelTier != "" {
				taskContext["model_tier"] = modelTier
			}

			err = workflow.ExecuteActivity(ctx,
				activities.ExecuteAgent,
				activities.AgentExecutionInput{
					Query:            task.Description,
					AgentID:          agentName,
					Context:          taskContext,
					Mode:             "standard",
					SessionID:        sessionID,
					UserID:           userID,
					History:          history,
					SuggestedTools:   task.SuggestedTools,
					ToolParameters:   task.ToolParameters,
					PersonaID:        task.PersonaID,
					ParentWorkflowID: wid,
				}).Get(ctx, &result)
		}

		if err != nil {
			logger.Error("Agent execution failed",
				"task_id", task.ID,
				"error", err,
			)
			errorCount++

			// Emit error event (parent workflow when available)
			// 发出错误事件（有父 workflow 时使用父 workflow）
			if config.EmitEvents {
				_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
					activities.EmitTaskUpdateInput{
						WorkflowID: wid,
						EventType:  activities.StreamEventErrorOccurred,
						AgentID:    agentName,
						Message:    err.Error(),
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
			}

			// Continue to next task even on failure
			// 即使失败也继续下一个任务
			continue
		}

		// Persist agent execution (fire-and-forget). Use parent workflow ID when available.
		// 持久化 agent 执行记录（fire-and-forget）。有父 workflow ID 时优先使用。
		persistAgentExecution(ctx, wid, agentName, task.Description, result)

		// Success
		// 成功
		results = append(results, result)
		totalTokens += result.TokensUsed
		successCount++

		// Record token usage for this sequential task when not budgeted
		// 非 budget 路径时，记录该顺序任务的 token 用量
		// Avoid double-recording: budgeted path already records inside the activity
		// 避免重复记录：budget 路径已在 activity 内部记录用量。
		if budgetPerAgent <= 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if config.Context != nil {
				if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
			}
			inTok := result.InputTokens
			outTok := result.OutputTokens
			if inTok == 0 && outTok == 0 && result.TokensUsed > 0 {
				inTok = result.TokensUsed * 6 / 10
				outTok = result.TokensUsed - inTok
			}
			model := result.ModelUsed
			if strings.TrimSpace(model) == "" {
				if m := pricing.GetPriorityOneModel(modelTier); m != "" {
					model = m
				}
			}
			provider := result.Provider
			if strings.TrimSpace(provider) == "" {
				provider = imodels.DetectProvider(model)
			}
			recCtx := opts.WithTokenRecordOptions(ctx)
			// Zero-token observability flag via workflow context: record_zero_token=true
			// 通过 workflow context 传入的零 token 可观测性开关：record_zero_token=true
			recordZero := false
			if config.Context != nil {
				if v, ok := config.Context["record_zero_token"]; ok {
					switch t := v.(type) {
					case bool:
						recordZero = t
					case string:
						if strings.EqualFold(t, "true") {
							recordZero = true
						}
					}
				}
			}

			meta := map[string]any{"phase": "sequential"}
			if (inTok + outTok) == 0 {
				if recordZero {
					meta["zero_tokens"] = true
					_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
						UserID:       userID,
						SessionID:    sessionID,
						TaskID:       wid,
						AgentID:      result.AgentID,
						Model:        model,
						Provider:     provider,
						InputTokens:  inTok,
						OutputTokens: outTok,
						Metadata:     meta,
					}).Get(recCtx, nil)
				} else {
					logger.Warn("Skipping token usage record: zero tokens",
						"agent_id", result.AgentID,
						"task_id", task.ID,
					)
				}
			} else {
				_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
					UserID:       userID,
					SessionID:    sessionID,
					TaskID:       wid,
					AgentID:      result.AgentID,
					Model:        model,
					Provider:     provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     meta,
				}).Get(recCtx, nil)
			}
			opts.RecordToolCostEntries(ctx, result, userID, sessionID, wid)
		}

		// Emit completion event (parent workflow when available)
		// 发出完成事件（有父 workflow 时使用父 workflow）
		if config.EmitEvents {
			_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
				activities.EmitTaskUpdateInput{
					WorkflowID: wid,
					EventType:  activities.StreamEventAgentCompleted,
					AgentID:    agentName,
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
		}

		// Record agent memory if session exists
		// 若 session 存在，则记录 agent memory
		if sessionID != "" {
			detachedCtx, _ := workflow.NewDisconnectedContext(ctx)
			workflow.ExecuteActivity(detachedCtx,
				activities.RecordAgentMemory,
				activities.RecordAgentMemoryInput{
					SessionID: sessionID,
					UserID:    userID,
					AgentID:   result.AgentID,
					Role:      task.Role,
					Query:     task.Description,
					Answer:    result.Response,
					Model:     result.ModelUsed,
					RedactPII: true,
					Extra: map[string]any{
						"task_id": task.ID,
					},
				})
		}
	}

	logger.Info("Sequential execution completed",
		"total_tasks", len(tasks),
		"successful", successCount,
		"failed", errorCount,
		"total_tokens", totalTokens,
	)

	return &SequentialResult{
		Results:     results,
		TotalTokens: totalTokens,
		Metadata: map[string]any{
			"total_tasks": len(tasks),
			"successful":  successCount,
			"failed":      errorCount,
		},
	}, nil
}

// persistAgentExecution persists agent execution results (fire-and-forget)
// persistAgentExecution 持久化 agent 执行结果（fire-and-forget）
func persistAgentExecution(ctx workflow.Context, workflowID string, agentID string, input string, result activities.AgentExecutionResult) {
	// Create a new context for persistence with no retries
	// 为持久化创建新 context，不重试
	persistCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Pre-generate agent execution ID using SideEffect for replay safety
	// 使用 SideEffect 预生成 agent 执行 ID，以保证 replay 安全
	var agentExecutionID string
	workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New().String()
	}).Get(&agentExecutionID)

	// Determine state based on success
	// 依据是否成功确定状态
	state := "COMPLETED"
	if !result.Success {
		state = "FAILED"
	}

	// Fire and forget - don't wait for result
	// fire-and-forget：不等待结果
	workflow.ExecuteActivity(
		persistCtx,
		activities.PersistAgentExecutionStandalone,
		activities.PersistAgentExecutionInput{
			ID:         agentExecutionID,
			WorkflowID: workflowID,
			AgentID:    agentID,
			Input:      input,
			Output:     result.Response,
			State:      state,
			TokensUsed: result.TokensUsed,
			ModelUsed:  result.ModelUsed,
			DurationMs: result.DurationMs,
			Error:      result.Error,
			Metadata: map[string]any{
				"workflow": "sequential",
				"strategy": "sequential",
			},
		},
	)

	// Persist tool executions if any
	// 若有 tool 执行记录，则持久化
	if len(result.ToolExecutions) > 0 {
		for _, tool := range result.ToolExecutions {
			// Convert tool output to string
			// 把 tool 输出转为字符串
			outputStr := ""
			if tool.Output != nil {
				switch v := tool.Output.(type) {
				case string:
					outputStr = v
				default:
					// Properly serialize complex outputs to JSON
					// 把复杂输出正确序列化为 JSON
					if jsonBytes, err := json.Marshal(v); err == nil {
						outputStr = string(jsonBytes)
					} else {
						outputStr = "complex output"
					}
				}
			}

			// Extract input params from tool execution (from HTTP path)
			// 从 tool 执行记录中提取输入参数（来自 HTTP 路径）
			inputParamsMap, _ := tool.InputParams.(map[string]any)

			workflow.ExecuteActivity(
				persistCtx,
				activities.PersistToolExecutionStandalone,
				activities.PersistToolExecutionInput{
					WorkflowID:       workflowID,
					AgentID:          agentID,
					AgentExecutionID: agentExecutionID,
					ToolName:         tool.Tool,
					InputParams:      inputParamsMap,
					Output:           outputStr,
					Success:          tool.Success,
					TokensConsumed:   0,
					DurationMs:       tool.DurationMs,
					Error:            tool.Error,
				},
			)
		}
	}
}

// Helper function to parse numeric values from responses
// 辅助函数：从响应中解析数值
// parseNumericValue wrapper removed in favor of util.ParseNumericValue at call sites
// parseNumericValue 包装已移除，调用处改用 util.ParseNumericValue
