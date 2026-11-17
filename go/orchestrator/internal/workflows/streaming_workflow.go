package workflows

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metadata"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/state"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// StreamingWorkflow executes tasks with streaming output and typed state management
// StreamingWorkflow 以流式输出与类型化状态管理执行任务
func StreamingWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting StreamingWorkflow",
		"query", input.Query,
		"user_id", input.UserID,
		"session_id", input.SessionID,
	)

	// Determine workflow ID for event streaming
	// 确定用于事件流式传输的 workflow ID
	workflowID := input.ParentWorkflowID
	if workflowID == "" {
		workflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	agentName := agents.GetAgentName(workflowID, 0)

	// Initialize control signal handler for pause/resume/cancel
	// 初始化用于 pause/resume/cancel 的控制信号处理器
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	controlHandler := &ControlSignalHandler{
		WorkflowID: workflowID,
		AgentID:    "streaming",
		Logger:     logger,
		EmitCtx:    emitCtx,
	}
	controlHandler.Setup(ctx)

	// Initialize typed state channel
	// 初始化类型化状态通道
	stateChannel := state.NewStateChannel("streaming-workflow")

	// Set initial state
	// 设置初始状态
	agentState := &state.AgentState{
		Query:   input.Query,
		Context: input.Context,
		PlanningState: state.PlanningState{
			CurrentStep: 0,
			TotalSteps:  1,
			Plan:        []string{"Analyze and respond to query"},
			Completed:   []bool{false},
		},
		ExecutionState: state.ExecutionState{
			Status:    "pending",
			StartTime: workflow.Now(ctx),
		},
		BeliefState: state.BeliefState{
			Confidence: 1.0,
		},
	}

	// Add state validation
	// 添加状态校验
	stateChannel.AddValidator(func(data any) error {
		s, ok := data.(*state.AgentState)
		if !ok {
			return fmt.Errorf("invalid state type")
		}
		return s.Validate()
	})

	if err := stateChannel.Set(agentState); err != nil {
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Invalid initial state: %v", err),
		}, err
	}

	// Create checkpoint before execution
	// 执行前创建 checkpoint
	checkpointID, _ := stateChannel.Checkpoint(map[string]any{
		"phase": "pre-execution",
	})
	logger.Info("State checkpoint created", "checkpoint_id", checkpointID)

	// Configure activity options with longer timeout for streaming
	// 配置 activity 选项，为流式输出延长超时
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,  // Longer timeout for streaming 注：流式输出需要更长的超时
		HeartbeatTimeout:    30 * time.Second, // Heartbeat to track progress 注：用心跳跟踪进度
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Update state to running
	// 把状态更新为 running
	agentState.ExecutionState.Status = "running"
	if err := stateChannel.Set(agentState); err != nil {
		logger.Error("Failed to update state", "error", err)
	}

	// Check pause/cancel before execution
	// 执行前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Execute with streaming
	// 以流式方式执行
	streamingActivities := activities.NewStreamingActivities()
	streamInput := activities.StreamExecuteInput{
		Query:     input.Query,
		Context:   input.Context,
		SessionID: input.SessionID,
		UserID:    input.UserID,
		AgentID:   agentName,
		Mode:      input.Mode,
	}

	// Start streaming execution
	// 启动流式执行
	logger.Info("Starting streaming execution")

	var streamRes activities.AgentExecutionResult
	err := workflow.ExecuteActivity(ctx, streamingActivities.StreamExecute, streamInput).Get(ctx, &streamRes)

	if err != nil {
		logger.Error("Streaming execution failed", "error", err)

		// Update state with error
		// 用错误更新状态
		agentState.ExecutionState.Status = "failed"
		agentState.AddError(state.ErrorRecord{
			Timestamp:    workflow.Now(ctx),
			ErrorType:    "streaming_error",
			ErrorMessage: err.Error(),
			Recoverable:  false,
		})
		stateChannel.Set(agentState)

		return TaskResult{
			Success:      false,
			ErrorMessage: err.Error(),
		}, err
	}

	// Update state with result
	// 用结果更新状态
	agentState.IntermediateResults = append(agentState.IntermediateResults, streamRes.Response)
	agentState.ExecutionState.Status = "completed"
	agentState.PlanningState.CurrentStep = 1
	agentState.PlanningState.Completed[0] = true

	// Add tool result
	// 添加 tool 结果
	agentState.AddToolResult(state.ToolResult{
		ToolName:      "streaming_llm",
		Input:         input.Query,
		Output:        streamRes.Response,
		Success:       true,
		ExecutionTime: int64(agentState.GetExecutionDuration().Milliseconds()),
		TokensUsed:    streamRes.TokensUsed,
		Timestamp:     workflow.Now(ctx),
	})

	if err := stateChannel.Set(agentState); err != nil {
		logger.Warn("Failed to update final state", "error", err)
	}

	// Create final checkpoint
	// 创建最终 checkpoint
	finalCheckpointID, _ := stateChannel.Checkpoint(map[string]any{
		"phase":  "completed",
		"result": streamRes.Response,
	})

	logger.Info("StreamingWorkflow completed successfully",
		"result_length", len(streamRes.Response),
		"tokens_used", streamRes.TokensUsed,
		"duration_ms", agentState.GetExecutionDuration().Milliseconds(),
		"final_checkpoint", finalCheckpointID,
	)

	// Record token usage for streaming agent (non-budgeted path).
	// 记录 streaming agent 的 token 用量（非预算路径）。
	if input.UserID != "" && streamRes.TokensUsed > 0 {
		recCtx := opts.WithTokenRecordOptions(ctx)
		inTok := streamRes.InputTokens
		outTok := streamRes.OutputTokens
		if inTok == 0 && outTok == 0 {
			// Token breakdown not provided by LLM provider - estimate 60/40 split
			// This may cause billing inaccuracy; fix provider to return actual breakdown
			// LLM provider 未返回 token 拆分，按 60/40 估算；这会造成计费不准，需修 provider 返回真实拆分。
			inTok = streamRes.TokensUsed * 6 / 10
			outTok = streamRes.TokensUsed - inTok
			workflow.GetLogger(ctx).Warn("Token breakdown estimated (60/40 split)",
				"total_tokens", streamRes.TokensUsed,
				"estimated_input", inTok,
				"estimated_output", outTok,
				"model", streamRes.ModelUsed,
				"provider", streamRes.Provider)
		}
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       input.UserID,
			SessionID:    input.SessionID,
			TaskID:       workflowID,
			AgentID:      agentName,
			Model:        streamRes.ModelUsed,
			Provider:     streamRes.Provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata: map[string]any{
				"phase":    "stream",
				"workflow": "streaming",
			},
		}).Get(recCtx, nil)
	}

	// Record tool-cost rows if streaming returned tool usage metadata.
	// 若流式执行返回了 tool 用量 metadata，则记录 tool 成本行。
	if input.UserID != "" {
		opts.RecordToolCostEntries(ctx, streamRes, input.UserID, input.SessionID, workflowID)
	}

	// Update session with token usage
	// 用 token 用量更新 session
	if input.SessionID != "" {
		var sessionUpdateResult activities.SessionUpdateResult
		err := workflow.ExecuteActivity(ctx,
			constants.UpdateSessionResultActivity,
			activities.SessionUpdateInput{
				SessionID:  input.SessionID,
				Result:     streamRes.Response,
				TokensUsed: streamRes.TokensUsed,
				AgentsUsed: 1,
				ModelUsed:  streamRes.ModelUsed,
			},
		).Get(ctx, &sessionUpdateResult)
		if err != nil {
			logger.Warn("Failed to update session with tokens",
				"session_id", input.SessionID,
				"error", err,
			)
		}
	}

	// Build metadata and include model/provider + token breakdown (estimated 60/40 if missing)
	// 构建 metadata，并包含 model/provider 与 token 拆分（缺失时按 60/40 估算）
	meta := map[string]any{
		"execution_time_ms": agentState.GetExecutionDuration().Milliseconds(),
		"checkpoints":       stateChannel.ListCheckpoints(),
		"final_state":       agentState.ExecutionState.Status,
	}

	// Prepare a single-agent result for aggregation
	// 准备单 agent 结果用于聚合
	ar := activities.AgentExecutionResult{
		AgentID:      agentName,
		ModelUsed:    streamRes.ModelUsed,
		TokensUsed:   streamRes.TokensUsed,
		InputTokens:  streamRes.InputTokens,
		OutputTokens: streamRes.OutputTokens,
		Success:      true,
	}
	if ar.InputTokens == 0 && ar.OutputTokens == 0 && ar.TokensUsed > 0 {
		ar.InputTokens = ar.TokensUsed * 6 / 10
		ar.OutputTokens = ar.TokensUsed - ar.InputTokens
		workflow.GetLogger(ctx).Warn("Token breakdown estimated for metadata (60/40 split)",
			"total_tokens", ar.TokensUsed,
			"estimated_input", ar.InputTokens,
			"estimated_output", ar.OutputTokens)
	}
	agentMeta := metadata.AggregateAgentMetadata([]activities.AgentExecutionResult{ar}, 0)
	maps.Copy(meta, agentMeta)

	// Add cost estimate if tokens available
	// 有 token 数据时加入成本估算
	if streamRes.TokensUsed > 0 {
		model := ""
		if m, ok := meta["model"].(string); ok && m != "" {
			model = m
		}
		meta["cost_usd"] = pricing.CostForTokens(model, streamRes.TokensUsed)
	}

	// Check pause/cancel before completion
	// 完成前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Emit final clean LLM_OUTPUT for OpenAI-compatible streaming.
	// 为 OpenAI 兼容的流式输出发出最终干净的 LLM_OUTPUT。
	// Agent ID "final_output" signals the streamer to always show this content.
	// Agent ID "final_output" 告知 streamer 始终展示该内容。
	if streamRes.Response != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    streamRes.Response,
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"tokens_used": streamRes.TokensUsed,
				"model_used":  streamRes.ModelUsed,
			},
		}).Get(ctx, nil)
	}

	return TaskResult{
		Result:     streamRes.Response,
		Success:    true,
		TokensUsed: streamRes.TokensUsed,
		Metadata:   meta,
	}, nil
}

// ParallelStreamingWorkflow executes multiple streaming tasks in parallel
// ParallelStreamingWorkflow 并行执行多个流式任务
func ParallelStreamingWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ParallelStreamingWorkflow",
		"query", input.Query,
		"user_id", input.UserID,
	)

	// Determine workflow ID for event streaming
	// 确定用于事件流式传输的 workflow ID
	// Use parent workflow ID if this is a child workflow, otherwise use own ID
	// 若为子 workflow 则使用父 workflow ID，否则使用自身 ID
	workflowID := input.ParentWorkflowID
	if workflowID == "" {
		workflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	// Initialize control signal handler for pause/resume/cancel
	// 初始化用于 pause/resume/cancel 的控制信号处理器
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	controlHandler := &ControlSignalHandler{
		WorkflowID: workflowID,
		AgentID:    "parallel-streaming",
		Logger:     logger,
		EmitCtx:    emitCtx,
	}
	controlHandler.Setup(ctx)

	// Configure activity options
	// 配置 activity 选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Create multiple streaming inputs for parallel execution
	// 为并行执行创建多个流式输入
	streamingActivities := activities.NewStreamingActivities()
	agent1 := agents.GetAgentName(workflowID, 0)
	agent2 := agents.GetAgentName(workflowID, 1)
	agent3 := agents.GetAgentName(workflowID, 2)
	inputs := []activities.StreamExecuteInput{
		{
			Query:     input.Query + " (perspective 1)",
			Context:   input.Context,
			SessionID: input.SessionID,
			UserID:    input.UserID,
			AgentID:   agent1,
			Mode:      input.Mode,
		},
		{
			Query:     input.Query + " (perspective 2)",
			Context:   input.Context,
			SessionID: input.SessionID,
			UserID:    input.UserID,
			AgentID:   agent2,
			Mode:      input.Mode,
		},
		{
			Query:     input.Query + " (perspective 3)",
			Context:   input.Context,
			SessionID: input.SessionID,
			UserID:    input.UserID,
			AgentID:   agent3,
			Mode:      input.Mode,
		},
	}

	// Check pause/cancel before execution
	// 执行前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Execute streams in parallel
	// 并行执行多个流
	var futures []workflow.Future
	for _, streamInput := range inputs {
		future := workflow.ExecuteActivity(ctx, streamingActivities.StreamExecute, streamInput)
		futures = append(futures, future)
	}

	// Collect results
	// 收集结果
	var results []activities.AgentExecutionResult
	totalTokens := 0

	for i, future := range futures {
		var result activities.AgentExecutionResult
		err := future.Get(ctx, &result)
		if err != nil {
			logger.Error("Stream execution failed",
				"agent_id", inputs[i].AgentID,
				"error", err,
			)
			// Continue with other streams
			// 继续处理其它流
			continue
		}
		results = append(results, result)
		totalTokens += result.TokensUsed
	}

	// Record token usage for each streaming agent (non-budgeted path).
	// This ensures parallel streaming tokens are captured in token_usage so
	// orchestrator aggregation and quota/overage accounting see full costs.
	// 把并行 stream 的 token 写入 token_usage，orchestrator 聚合与配额/超额核算才能看到完整成本。
	if input.UserID != "" {
		recCtx := opts.WithTokenRecordOptions(ctx)
		for _, res := range results {
			opts.RecordToolCostEntries(ctx, res, input.UserID, input.SessionID, workflowID)
			// Skip zero-token runs to avoid noisy rows.
			// 跳过零 token 的运行，避免产生噪声行。
			if res.TokensUsed <= 0 && res.InputTokens <= 0 && res.OutputTokens <= 0 {
				continue
			}

			inTok := res.InputTokens
			outTok := res.OutputTokens
			if inTok == 0 && outTok == 0 && res.TokensUsed > 0 {
				inTok = res.TokensUsed * 6 / 10
				outTok = res.TokensUsed - inTok
				workflow.GetLogger(ctx).Warn("Token breakdown estimated for parallel stream (60/40 split)",
					"total_tokens", res.TokensUsed,
					"estimated_input", inTok,
					"estimated_output", outTok,
					"model", res.ModelUsed,
					"provider", res.Provider)
			}

			// Fallbacks for missing model/provider
			// model/provider 缺失时的兜底
			model := res.ModelUsed
			provider := res.Provider
			if provider == "" {
				provider = ""
			}

			_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
				UserID:       input.UserID,
				SessionID:    input.SessionID,
				TaskID:       workflowID,
				AgentID:      res.AgentID,
				Model:        model,
				Provider:     provider,
				InputTokens:  inTok,
				OutputTokens: outTok,
				Metadata: map[string]any{
					"phase":    "stream",
					"workflow": "streaming",
				},
			}).Get(recCtx, nil)
		}
	}

	// Collect citations from streaming agent results (best-effort)
	// 从各 streaming agent 结果中收集引用（尽力而为）
	var collectedCitations []metadata.Citation
	if len(results) > 0 {
		var resultsForCitations []any
		for _, ar := range results {
			var toolExecs []any
			if len(ar.ToolExecutions) > 0 {
				for _, te := range ar.ToolExecutions {
					toolExecs = append(toolExecs, map[string]any{
						"tool":    te.Tool,
						"success": te.Success,
						"output":  te.Output,
						"error":   te.Error,
					})
				}
			}
			resultsForCitations = append(resultsForCitations, map[string]any{
				"agent_id":        ar.AgentID,
				"tool_executions": toolExecs,
				"response":        ar.Response,
			})
		}
		collectedCitations, _ = metadata.CollectCitations(resultsForCitations, workflow.Now(ctx), 0)
	}

	// Prepare synthesis context (copy input context, set defaults, inject citations)
	// 准备合成 context（复制输入 context、设置默认值、注入引用）
	buildContext := func() map[string]any {
		ctxMap := map[string]any{}
		if input.Context != nil {
			maps.Copy(ctxMap, input.Context)
		}
		if _, ok := ctxMap["synthesis_style"]; !ok {
			if _, hasAreas := ctxMap["research_areas"]; hasAreas {
				ctxMap["synthesis_style"] = "comprehensive"
			}
		}
		addCitationsToContext(ctxMap, collectedCitations)
		return ctxMap
	}

	// Synthesize results (LLM-first)
	// 合成结果（优先用 LLM）
	var synthesis activities.SynthesisResult
	didSynthesisLLM := false

	// Already have AgentExecutionResult from streaming
	// 流式执行已经产出 AgentExecutionResult
	agentResults := make([]activities.AgentExecutionResult, len(results))
	copy(agentResults, results)

	// 先把 bypass 条件算完（含 tools 与 JSON 两项否决），再只调用一次合成。
	bypass := input.BypassSingleResult && len(agentResults) == 1 && agentResults[0].Success
	if bypass {
		// Avoid bypass if the single result looks like raw JSON or comes from web_search
		// 若唯一结果是裸 JSON 或来自 web_search，则不要跳过合成。
		for _, t := range agentResults[0].ToolsUsed {
			if strings.EqualFold(t, "web_search") {
				bypass = false
				break
			}
		}
	}
	if bypass {
		trimmed := strings.TrimSpace(agentResults[0].Response)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			bypass = false
		}
	}

	if bypass {
		synthesis = activities.SynthesisResult{FinalResult: agentResults[0].Response, TokensUsed: agentResults[0].TokensUsed}
	} else {
		if err := workflow.ExecuteActivity(ctx, activities.SynthesizeResultsLLM, activities.SynthesisInput{
			Query:              input.Query,
			AgentResults:       agentResults,
			Context:            buildContext(),
			CollectedCitations: collectedCitations,
			ParentWorkflowID:   workflowID,
		}).Get(ctx, &synthesis); err != nil {
			logger.Error("Result synthesis failed", "error", err)
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}
		didSynthesisLLM = true
	}

	// Record synthesis token usage when LLM synthesis ran
	// LLM 合成实际执行时记录其 token 用量
	if didSynthesisLLM && synthesis.TokensUsed > 0 {
		inTok := synthesis.InputTokens
		outTok := synthesis.CompletionTokens
		if inTok == 0 && outTok > 0 {
			est := synthesis.TokensUsed - outTok
			if est > 0 {
				inTok = est
			}
		}
		recCtx := opts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       input.UserID,
			SessionID:    input.SessionID,
			TaskID:       workflowID,
			AgentID:      "streaming_synthesis",
			Model:        synthesis.ModelUsed,
			Provider:     synthesis.Provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata: map[string]any{
				"phase":    "synthesis",
				"workflow": "streaming",
			},
		}).Get(recCtx, nil)
	}

	logger.Info("ParallelStreamingWorkflow completed",
		"num_streams", len(results),
		"total_tokens", totalTokens,
	)

	// Update session with token usage
	// 用 token 用量更新 session
	if input.SessionID != "" {
		var sessionUpdateResult activities.SessionUpdateResult
		err := workflow.ExecuteActivity(ctx,
			constants.UpdateSessionResultActivity,
			activities.SessionUpdateInput{
				SessionID:  input.SessionID,
				Result:     synthesis.FinalResult,
				TokensUsed: totalTokens,
				AgentsUsed: len(results),
				AgentUsage: func() []activities.AgentUsage {
					u := make([]activities.AgentUsage, 0, len(agentResults))
					for _, r := range agentResults {
						u = append(u, activities.AgentUsage{Model: r.ModelUsed, Tokens: r.TokensUsed, InputTokens: r.InputTokens, OutputTokens: r.OutputTokens})
					}
					return u
				}(),
			},
		).Get(ctx, &sessionUpdateResult)
		if err != nil {
			logger.Warn("Failed to update session with tokens",
				"session_id", input.SessionID,
				"error", err,
			)
		}
	}

	// Build metadata and include aggregate model/provider + token breakdown across streams
	// 构建 metadata，并包含跨各流聚合的 model/provider 与 token 拆分
	meta := map[string]any{
		"num_streams": len(results),
		"parallel":    true,
	}
	// Ensure each agent has input/output tokens (estimate 60/40 if missing)
	// 确保每个 agent 都有 input/output token（缺失时按 60/40 估算）
	agentResultsForMeta := make([]activities.AgentExecutionResult, 0, len(agentResults))
	for _, r := range agentResults {
		ar := r
		if (ar.InputTokens == 0 && ar.OutputTokens == 0) && ar.TokensUsed > 0 {
			ar.InputTokens = ar.TokensUsed * 6 / 10
			ar.OutputTokens = ar.TokensUsed - ar.InputTokens
			logger.Warn("Token breakdown estimated for agent metadata (60/40 split)",
				"total_tokens", ar.TokensUsed,
				"estimated_input", ar.InputTokens,
				"estimated_output", ar.OutputTokens,
				"model", ar.ModelUsed)
		}
		agentResultsForMeta = append(agentResultsForMeta, ar)
	}
	agentMeta := metadata.AggregateAgentMetadata(agentResultsForMeta, 0)
	maps.Copy(meta, agentMeta)

	// Add cost estimate if tokens available
	// 有 token 数据时加入成本估算
	if totalTokens > 0 {
		model := ""
		if m, ok := meta["model"].(string); ok && m != "" {
			model = m
		}
		meta["cost_usd"] = pricing.CostForTokens(model, totalTokens)
	}

	// Check pause/cancel before completion
	// 完成前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Emit final clean LLM_OUTPUT for OpenAI-compatible streaming.
	// 为 OpenAI 兼容的流式输出发出最终干净的 LLM_OUTPUT。
	// Agent ID "final_output" signals the streamer to always show this content.
	// Agent ID "final_output" 告知 streamer 始终展示该内容。
	if synthesis.FinalResult != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    synthesis.FinalResult,
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"tokens_used": totalTokens,
			},
		}).Get(ctx, nil)
	}

	// Emit WORKFLOW_COMPLETED before returning
	// 返回前发出 WORKFLOW_COMPLETED
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowCompleted,
		AgentID:    "parallel-streaming",
		Message:    activities.MsgWorkflowCompleted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	return TaskResult{
		Result:     synthesis.FinalResult,
		Success:    true,
		TokensUsed: totalTokens,
		Metadata:   meta,
	}, nil
}
