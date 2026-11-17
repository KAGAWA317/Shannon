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
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// ParallelConfig controls parallel execution behavior
// ParallelConfig 控制并行执行行为
type ParallelConfig struct {
	MaxConcurrency   int                // Maximum concurrent agents 注：最大并发 agent 数
	Semaphore        workflow.Semaphore // Concurrency control (interface, not pointer) 注：并发控制（接口，不是指针）
	EmitEvents       bool               // Whether to emit streaming events 注：是否发送流式事件
	Context          map[string]any     // Base context for all agents 注：所有 agent 的公共上下文
	AgentIndexOffset int                // Offset added to loop index for GetAgentName (used by hybrid executor) 注：传给 GetAgentName 时叠加到循环下标上的偏移（供 hybrid executor 使用）
}

// ParallelTask represents a task to execute in parallel
// ParallelTask 表示一个待并行执行的任务
type ParallelTask struct {
	ID             string
	Description    string
	SuggestedTools []string
	ToolParameters map[string]any
	PersonaID      string
	Role           string
	ParentArea     string
	Dependencies   []string // For hybrid parallel/sequential execution 注：用于混合并行/串行执行
	// Optional per-task context overrides (e.g., task contracts)
	// 可选的单任务上下文覆盖（例如 task contracts）
	ContextOverrides map[string]any
}

// ParallelResult contains results from parallel execution
// ParallelResult 保存并行执行的结果
type ParallelResult struct {
	Results     []activities.AgentExecutionResult
	TotalTokens int
	Metadata    map[string]any
}

// ExecuteParallel runs multiple tasks in parallel with concurrency control.
// ExecuteParallel 在并发控制下并行运行多个任务。
// It supports optional budget enforcement and streaming events.
// 支持可选的预算约束与流式事件。
func ExecuteParallel(
	ctx workflow.Context,
	tasks []ParallelTask,
	sessionID string,
	history []string,
	config ParallelConfig,
	budgetPerAgent int,
	userID string,
	modelTier string,
) (*ParallelResult, error) {

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting parallel execution",
		"task_count", len(tasks),
		"max_concurrency", config.MaxConcurrency,
	)

	// Create semaphore if not provided
	// 未提供信号量时创建
	if config.Semaphore == nil {
		config.Semaphore = workflow.NewSemaphore(ctx, int64(config.MaxConcurrency))
	}

	// Channel for collecting in-flight futures with a release handshake
	// 收集进行中 future 的 channel 带释放握手：信号量要等收集器处理完结果后才释放。
	futuresChan := workflow.NewChannel(ctx)

	// Track futures with their original index
	// 按原始下标跟踪 future
	type futureWithIndex struct {
		Index   int
		Future  workflow.Future
		Release workflow.Channel // send a signal when it's safe to release the semaphore 注：可安全释放信号量时发送信号
	}

	// Activity options
	// Activity 选项
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: agentStartToCloseTimeout(config.Context, 10*time.Minute),
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOpts)

	// Launch parallel executions
	// 启动并行执行
	for i, task := range tasks {

		workflow.Go(ctx, func(ctx workflow.Context) {
			// Acquire semaphore
			// 获取信号量
			if err := config.Semaphore.Acquire(ctx, 1); err != nil {
				logger.Error("Failed to acquire semaphore",
					"task_id", task.ID,
					"error", err,
				)
				futuresChan.Send(ctx, futureWithIndex{Index: i, Future: nil, Release: nil})
				return
			}
			// Create a release channel so the collector can signal when to release
			// 创建释放 channel，供收集器通知何时释放
			rel := workflow.NewChannel(ctx)

			// Prepare task context
			// 准备任务上下文
			taskContext := make(map[string]any)
			maps.Copy(taskContext, config.Context)
			if task.ContextOverrides != nil {
				maps.Copy(taskContext, task.ContextOverrides)
			}
			taskContext["role"] = task.Role
			taskContext["task_id"] = task.ID
			if task.ParentArea != "" {
				taskContext["parent_area"] = task.ParentArea
			}

			// Compute agent name using station names for deterministic, human-readable IDs
			// 用 station 名称生成 agent 名称，得到确定且可读的 ID
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if config.Context != nil {
				if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
			}
			agentName := agents.GetAgentName(wid, i+config.AgentIndexOffset)

			// Emit agent started event (publish under parent workflow when available)
			// 发送 agent started 事件（有父 workflow 时挂在父 workflow 下发布）
			if config.EmitEvents {
				_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
					activities.EmitTaskUpdateInput{
						WorkflowID: wid,
						EventType:  activities.StreamEventAgentStarted,
						AgentID:    agentName,
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
			}

			// Execute agent
			// 执行 agent
			var future workflow.Future

			if budgetPerAgent > 0 {
				// Execute with budget
				// 带预算执行
				// Prefer parent workflow ID for budget tracking and persistence
				// 预算记账与持久化优先使用父 workflow ID
				parentWid := ""
				if config.Context != nil {
					if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
						parentWid = p
					}
				}
				// Use parent workflow ID when available, otherwise fallback to child ID (trim suffix if present)
				// 有父 workflow ID 时使用之，否则回退到子 ID（存在后缀则裁掉）
				taskID := wid
				if parentWid != "" {
					taskID = parentWid
				} else if idx := strings.LastIndex(wid, "_"); idx > 0 {
					taskID = wid[:idx]
				}
				future = workflow.ExecuteActivity(ctx,
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
							ParentWorkflowID: parentWid,
						},
						MaxTokens: budgetPerAgent,
						UserID:    userID,
						TaskID:    taskID,
						ModelTier: modelTier,
					})
			} else {
				// Execute without budget
				// 不带预算执行
				// Inject model_tier to taskContext to ensure consistent tier selection
				// 向 taskContext 注入 model_tier，保证 tier 选择一致
				// (budget path injects via BudgetedAgentInput.ModelTier -> budget.go:298)
				// （budget 路径通过 BudgetedAgentInput.ModelTier 注入 -> budget.go:298）
				if modelTier != "" {
					taskContext["model_tier"] = modelTier
				}

				// Determine parent workflow if available for streaming correlation
				// 若存在父 workflow 则取出，用于流式事件关联
				parentWid := ""
				if config.Context != nil {
					if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
						parentWid = p
					}
				}
				future = workflow.ExecuteActivity(ctx,
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
						ParentWorkflowID: parentWid,
					})
			}

			futuresChan.Send(ctx, futureWithIndex{Index: i, Future: future, Release: rel})

			// Hold the permit until the collector signals that it has finished processing the result
			// 在收集器确认处理完结果之前一直持有许可
			var _sig struct{}
			rel.Receive(ctx, &_sig)
			// Now safe to release the semaphore
			// 在收集器确认已处理完结果之前一直持有许可，之后才可安全释放信号量。
			config.Semaphore.Release(1)
		})
	}

	// Collect results
	// 收集结果
	results := make([]activities.AgentExecutionResult, len(tasks))
	totalTokens := 0
	successCount := 0
	errorCount := 0

	// Use a selector to receive futures and process completions in completion order
	// 用 selector 接收 future，按完成顺序处理
	sel := workflow.NewSelector(ctx)
	received := 0
	skippedNil := 0
	processed := 0

	var registerReceive func()
	registerReceive = func() {
		sel.AddReceive(futuresChan, func(c workflow.ReceiveChannel, more bool) {
			var fwi futureWithIndex
			c.Receive(ctx, &fwi)
			received++
			if fwi.Future == nil {
				// Failed to acquire or schedule; count as error and skip
				// 获取信号量或调度失败；按错误计数并跳过
				errorCount++
				skippedNil++
			} else {
				fwi := fwi // capture for closure 注：为闭包捕获变量
				sel.AddFuture(fwi.Future, func(f workflow.Future) {
					var result activities.AgentExecutionResult
					err := f.Get(ctx, &result)
					if err != nil {
						logger.Error("Agent execution failed",
							"task_id", tasks[fwi.Index].ID,
							"error", err,
						)
						errorCount++
						// Emit error event (parent workflow when available)
						// 发送错误事件（有父 workflow 时挂父 workflow）
						if config.EmitEvents {
							wid := workflow.GetInfo(ctx).WorkflowExecution.ID
							if config.Context != nil {
								if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
									wid = p
								}
							}
							agentName := agents.GetAgentName(wid, fwi.Index+config.AgentIndexOffset)
							_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
								activities.EmitTaskUpdateInput{
									WorkflowID: wid,
									EventType:  activities.StreamEventErrorOccurred,
									AgentID:    agentName,
									Message:    err.Error(),
									Timestamp:  workflow.Now(ctx),
								}).Get(ctx, nil)
						}
					} else {
						results[fwi.Index] = result
						totalTokens += result.TokensUsed
						successCount++

						// Record token usage for this parallel task
						// 为该并行任务记录 token 用量
						// Important: Avoid double-recording when running budgeted agents.
						// 注意：budget agent 运行时避免重复记录。
						// ExecuteAgentWithBudget already records usage inside the activity.
						// 注意：budget 路径已在 activity 内部记录用量，此处勿重复记录。
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
							// Fallbacks for missing model/provider
							// model/provider 缺失时的兜底
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
							// Standardized activity options
							// 统一化的 activity 选项
							recCtx := opts.WithTokenRecordOptions(ctx)

							// Zero-token observability flag via workflow context: record_zero_token=true
							// 通过 workflow context 传递零 token 可观测开关：record_zero_token=true
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

							meta := map[string]any{"phase": "parallel"}

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
										"task_id", tasks[fwi.Index].ID,
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

						// Persist agent execution (fire-and-forget). Use parent workflow ID when available.
						// 持久化 agent 执行记录（fire-and-forget）。有父 workflow ID 时使用之。
						workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
						if config.Context != nil {
							if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
								workflowID = p
							}
						}
						agentName := agents.GetAgentName(workflowID, fwi.Index+config.AgentIndexOffset)
						persistAgentExecutionLocal(ctx, workflowID, agentName, tasks[fwi.Index].Description, result)

						// Emit completion event (parent workflow when available)
						// 发送完成事件（有父 workflow 时挂父 workflow）
						if config.EmitEvents {
							_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
								activities.EmitTaskUpdateInput{
									WorkflowID: workflowID,
									EventType:  activities.StreamEventAgentCompleted,
									AgentID:    agentName,
									Timestamp:  workflow.Now(ctx),
								}).Get(ctx, nil)
						}

						// Record agent memory if session exists
						// 存在 session 时记录 agent 记忆
						if sessionID != "" {
							detachedCtx, _ := workflow.NewDisconnectedContext(ctx)
							workflow.ExecuteActivity(detachedCtx,
								activities.RecordAgentMemory,
								activities.RecordAgentMemoryInput{
									SessionID: sessionID,
									UserID:    userID,
									AgentID:   result.AgentID,
									Role:      tasks[fwi.Index].Role,
									Query:     tasks[fwi.Index].Description,
									Answer:    result.Response,
									Model:     result.ModelUsed,
									RedactPII: true,
									Extra: map[string]any{
										"task_id": tasks[fwi.Index].ID,
									},
								})
						}
					}

					// Signal producer that we're done with this future (release semaphore)
					// 通知生产者该 future 已处理完（释放信号量）
					if fwi.Release != nil {
						var sig struct{}
						fwi.Release.Send(ctx, sig)
					}
					processed++
				})
			}

			// Continue receiving until we've seen all producer messages
			// 持续接收，直到收齐全部生产者消息
			if received < len(tasks) {
				registerReceive()
			}
		})
	}

	// Prime the selector to start receiving
	// 预热 selector 以开始接收
	if len(tasks) > 0 {
		registerReceive()
	}

	// Event loop: select until all non-nil futures are processed
	// 事件循环：select 直到所有非 nil future 处理完毕
	for processed < (len(tasks) - skippedNil) {
		sel.Select(ctx)
	}

	logger.Info("Parallel execution completed",
		"total_tasks", len(tasks),
		"successful", successCount,
		"failed", errorCount,
		"total_tokens", totalTokens,
	)

	// Build metadata summary
	// 构建 metadata 摘要
	md := map[string]any{
		"total_tasks": len(tasks),
		"successful":  successCount,
		"failed":      errorCount,
	}

	return &ParallelResult{
		Results:     results,
		TotalTokens: totalTokens,
		Metadata:    md,
	}, nil
}

// persistAgentExecutionLocal is a local helper to avoid circular imports
// persistAgentExecutionLocal 是本地辅助函数，用于规避包循环导入
// It mirrors the logic from supervisor_workflow.go and sequential.go
// 本地副本，用于规避包循环导入；逻辑与 supervisor_workflow.go、sequential.go 保持一致。
func persistAgentExecutionLocal(ctx workflow.Context, workflowID, agentID, input string, result activities.AgentExecutionResult) {
	logger := workflow.GetLogger(ctx)

	// Use detached context for fire-and-forget persistence
	// 使用 detached context 做 fire-and-forget 持久化
	detachedCtx, _ := workflow.NewDisconnectedContext(ctx)
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	detachedCtx = workflow.WithActivityOptions(detachedCtx, activityOpts)

	// Pre-generate agent execution ID using SideEffect for replay safety
	// 用 SideEffect 预生成执行 ID，保证重放时 ID 一致。
	var agentExecutionID string
	workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New().String()
	}).Get(&agentExecutionID)

	// Persist agent execution asynchronously
	// 异步持久化 agent 执行记录
	workflow.ExecuteActivity(detachedCtx,
		activities.PersistAgentExecutionStandalone,
		activities.PersistAgentExecutionInput{
			ID:         agentExecutionID,
			WorkflowID: workflowID,
			AgentID:    agentID,
			Input:      input,
			Output:     result.Response,
			State:      "COMPLETED",
			TokensUsed: result.TokensUsed,
			ModelUsed:  result.ModelUsed,
			DurationMs: result.DurationMs,
			Error:      result.Error,
			Metadata: map[string]any{
				"workflow": "parallel",
				"strategy": "parallel",
			},
		})

	// Persist tool executions if any
	// 若有工具执行则持久化
	for _, tool := range result.ToolExecutions {
		outputStr := ""
		if tool.Output != nil {
			switch v := tool.Output.(type) {
			case string:
				outputStr = v
			default:
				// Properly serialize complex outputs to JSON
				// 将复杂输出正确序列化为 JSON
				if jsonBytes, err := json.Marshal(v); err == nil {
					outputStr = string(jsonBytes)
				} else {
					outputStr = "complex output"
				}
			}
		}

		// Extract input params from tool execution
		// 从工具执行记录中提取输入参数
		inputParamsMap, _ := tool.InputParams.(map[string]any)

		workflow.ExecuteActivity(detachedCtx,
			activities.PersistToolExecutionStandalone,
			activities.PersistToolExecutionInput{
				WorkflowID:       workflowID,
				AgentID:          agentID,
				AgentExecutionID: agentExecutionID,
				ToolName:         tool.Tool,
				InputParams:      inputParamsMap,
				Output:           outputStr,
				Success:          tool.Success,
				DurationMs:       tool.DurationMs,
				Error:            tool.Error,
			})
	}

	logger.Debug("Scheduled persistence for agent execution",
		"workflow_id", workflowID,
		"agent_id", agentID,
	)
}
