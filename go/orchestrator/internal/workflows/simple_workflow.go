package workflows

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metadata"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// SimpleTaskWorkflow handles simple, single-agent tasks efficiently
// This workflow minimizes events by using a single consolidated activity
// SimpleTaskWorkflow 高效处理简单的单 agent 任务
// 该 workflow 通过单个合并 activity 来最小化事件数量
func SimpleTaskWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting SimpleTaskWorkflow",
		"query", input.Query,
		"user_id", input.UserID,
		"session_id", input.SessionID,
	)

	// Determine workflow ID for event streaming
	// Use parent workflow ID if this is a child workflow, otherwise use own ID
	// 确定用于事件流的 workflow ID
	// 若是子 workflow 则用父 workflow ID，否则用自己的 ID
	workflowID := input.ParentWorkflowID
	if workflowID == "" {
		workflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	agentName := agents.GetAgentName(workflowID, 0)

	// Emit workflow started event
	// 发出 workflow 开始事件
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Initialize control signal handler for pause/resume/cancel
	// 初始化 pause/resume/cancel 的控制信号处理器
	controlHandler := &ControlSignalHandler{
		WorkflowID: workflowID,
		AgentID:    agentName,
		Logger:     logger,
		EmitCtx:    emitCtx,
	}
	controlHandler.Setup(ctx)

	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowStarted,
		AgentID:    agentName,
		Message:    activities.MsgWorkflowStarted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Emit thinking event
	// 发出思考事件
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventAgentThinking,
		AgentID:    agentName,
		Message:    activities.MsgThinking(input.Query),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Configure activity options
	// 配置 activity 选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Minute, // Simple tasks should be fast 注：简单任务应当很快
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2, // Fewer retries for simple tasks 注：简单任务的重试次数更少
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Emit agent started event
	// 发出 agent 开始事件
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventAgentStarted,
		AgentID:    agentName,
		Message:    activities.MsgProcessing(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Check pause/cancel before memory retrieval
	// 记忆检索前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_memory"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Memory retrieval with gate precedence (hierarchical > simple session)
	// 记忆检索遵循闸门优先级（hierarchical > simple session）
	hierarchicalVersion := workflow.GetVersion(ctx, "memory_retrieval_v1", workflow.DefaultVersion, 1)
	sessionVersion := workflow.GetVersion(ctx, "session_memory_v1", workflow.DefaultVersion, 1)

	if hierarchicalVersion >= 1 && input.SessionID != "" {
		// Use hierarchical memory (combines recent + semantic)
		// 使用分层记忆（合并 recent + semantic）
		var hierMemory activities.FetchHierarchicalMemoryResult
		_ = workflow.ExecuteActivity(ctx, activities.FetchHierarchicalMemory,
			activities.FetchHierarchicalMemoryInput{
				Query:        input.Query,
				SessionID:    input.SessionID,
				TenantID:     input.TenantID,
				RecentTopK:   5,    // Fixed for determinism 注：为确定性而固定取值
				SemanticTopK: 5,    // Fixed for determinism 注：为确定性而固定取值
				Threshold:    0.75, // Fixed semantic threshold 注：固定的语义阈值
			}).Get(ctx, &hierMemory)

		if len(hierMemory.Items) > 0 {
			if input.Context == nil {
				input.Context = make(map[string]any)
			}
			input.Context["agent_memory"] = hierMemory.Items
			// Emit memory recall metadata (no content)
			// 发出记忆召回元数据（不含内容）
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventDataProcessing,
				AgentID:    agentName,
				Message:    activities.MsgMemoryRecalled(len(hierMemory.Items)),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
			logger.Info("Injected hierarchical memory into context",
				"session_id", input.SessionID,
				"memory_items", len(hierMemory.Items),
				"sources", hierMemory.Sources,
			)
		}
	} else if sessionVersion >= 1 && input.SessionID != "" {
		// Fallback to simple session memory if hierarchical not enabled
		// 未启用分层记忆时回退到简单 session 记忆
		var sessionMemory activities.FetchSessionMemoryResult
		_ = workflow.ExecuteActivity(ctx, activities.FetchSessionMemory,
			activities.FetchSessionMemoryInput{
				SessionID: input.SessionID,
				TenantID:  input.TenantID,
				TopK:      20, // Fixed for determinism 注：为确定性而固定取值
			}).Get(ctx, &sessionMemory)

		if len(sessionMemory.Items) > 0 {
			if input.Context == nil {
				input.Context = make(map[string]any)
			}
			input.Context["agent_memory"] = sessionMemory.Items
			// Emit memory recall metadata (no content)
			// 发出记忆召回元数据（不含内容）
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventDataProcessing,
				AgentID:    agentName,
				Message:    activities.MsgMemoryRecalled(len(sessionMemory.Items)),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
			logger.Info("Injected session memory into context",
				"session_id", input.SessionID,
				"memory_items", len(sessionMemory.Items),
			)
		}
	}

	// User persistent memory: prompt injection and extraction are swarm-only.
	// Simple tasks generate too many low-value extractions that bloat /memory/.
	// 用户长期记忆的注入与抽取仅限 swarm；simple task 会产生大量低价值抽取，撑大 /memory/。

	// Context compression (version-gated for determinism)
	// 上下文压缩（用版本闸门保证确定性）
	compressionVersion := workflow.GetVersion(ctx, "context_compress_v1", workflow.DefaultVersion, 1)
	if compressionVersion >= 1 && input.SessionID != "" && len(input.History) > 20 {
		// Check if compression is needed with rate limiting
		// 在限流条件下检查是否需要压缩
		estimatedTokens := activities.EstimateTokens(convertHistoryForAgent(input.History))
		// Determine model tier from context or per-agent budget
		// 从 context 或单 agent 预算判定 model tier
		modelTier := deriveModelTier(input.Context)
		if tier, ok := input.Context["model_tier"].(string); ok {
			modelTier = tier
		}

		var checkResult activities.CheckCompressionNeededResult
		err := workflow.ExecuteActivity(ctx, "CheckCompressionNeeded",
			activities.CheckCompressionNeededInput{
				SessionID:       input.SessionID,
				MessageCount:    len(input.History),
				EstimatedTokens: estimatedTokens,
				ModelTier:       modelTier,
			}).Get(ctx, &checkResult)

		if err == nil && checkResult.ShouldCompress {
			logger.Info("Triggering context compression",
				"session_id", input.SessionID,
				"reason", checkResult.Reason,
				"message_count", len(input.History),
			)

			// Compress context via activity
			// 通过 activity 压缩上下文
			var compressResult activities.CompressContextResult
			err = workflow.ExecuteActivity(ctx, activities.CompressAndStoreContext,
				activities.CompressContextInput{
					SessionID:        input.SessionID,
					History:          convertHistoryMapForCompression(input.History),
					TargetTokens:     int(float64(activities.GetModelWindowSize(modelTier)) * 0.375), // Compress to half of 75% 注：压缩到 75% 的一半
					ParentWorkflowID: workflowID,
				}).Get(ctx, &compressResult)

			if err == nil && compressResult.Summary != "" && compressResult.Stored {
				logger.Info("Context compressed and stored",
					"session_id", input.SessionID,
					"summary_length", len(compressResult.Summary),
				)

				// Emit compression applied (metadata only)
				// 发出已应用压缩（仅元数据）
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: workflowID,
					EventType:  activities.StreamEventDataProcessing,
					AgentID:    agentName,
					Message:    activities.MsgCompressionApplied(),
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)

				// Update compression state in session
				// 更新 session 中的压缩状态
				var updateResult activities.UpdateCompressionStateResult
				_ = workflow.ExecuteActivity(ctx, "UpdateCompressionStateActivity",
					activities.UpdateCompressionStateInput{
						SessionID:    input.SessionID,
						MessageCount: len(input.History),
					}).Get(ctx, &updateResult)

				if updateResult.Updated {
					logger.Info("Compression state updated in session",
						"session_id", input.SessionID,
					)
				}
			}
		} else if err == nil {
			logger.Debug("Compression not needed",
				"session_id", input.SessionID,
				"reason", checkResult.Reason,
			)
		}
	}

	// Prepare history for agent; optionally inject summary and use sliding window when compressed
	// 为 agent 准备 history；可选注入摘要，压缩后改用滑动窗口
	historyForAgent := convertHistoryForAgent(input.History)

	// If compression was performed earlier in this workflow and produced a summary,
	// add it to context and shape history to primers+recents
	// Note: This piggybacks on the compression block above; we also re-check here in case
	// no prior compression happened but history is still large.
	// 若本次 workflow 早前做过压缩并产出摘要，则把它加入 context 并裁成 primers+recents；
	// 这里再检查一次，以覆盖此前未压缩但历史仍然很大的情况。
	if input.SessionID != "" && compressionVersion >= 1 {
		// Estimate tokens and, if needed, perform on-the-fly compression for the middle section
		// 估算 token，必要时对中段做即时压缩
		estimatedTokens := activities.EstimateTokens(historyForAgent)
		// Use the same model tier determination for consistency
		// 沿用同一套 model tier 判定以保持一致
		modelTier := deriveModelTier(input.Context)
		if tier, ok := input.Context["model_tier"].(string); ok {
			modelTier = tier
		}
		window := activities.GetModelWindowSize(modelTier)
		trig, tgt := getCompressionRatios(input.Context, 0.75, 0.375)
		if estimatedTokens >= int(float64(window)*trig) {
			// Compress and store context to get a summary
			// 压缩并存储上下文以取得摘要
			var compressResult activities.CompressContextResult
			_ = workflow.ExecuteActivity(ctx, activities.CompressAndStoreContext,
				activities.CompressContextInput{
					SessionID:        input.SessionID,
					History:          convertHistoryMapForCompression(input.History),
					TargetTokens:     int(float64(window) * tgt),
					ParentWorkflowID: workflowID,
				},
			).Get(ctx, &compressResult)
			if compressResult.Summary != "" {
				if input.Context == nil {
					input.Context = make(map[string]any)
				}
				input.Context["context_summary"] = fmt.Sprintf("Previous context summary: %s", compressResult.Summary)
				prim, rec := getPrimersRecents(input.Context, 3, 20)
				shaped := shapeHistory(input.History, prim, rec)
				historyForAgent = convertHistoryForAgent(shaped)
				// Emit summary injected (metadata only)
				// 发出已注入摘要（仅元数据）
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: workflowID,
					EventType:  activities.StreamEventDataProcessing,
					AgentID:    agentName,
					Message:    activities.MsgSummaryAdded(),
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
			}
		}
	}

	// Check pause/cancel before main execution
	// 主执行前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Execute the consolidated simple task activity
	// This single activity handles everything: agent execution, session update, etc.
	// 单个 activity 完成全部工作：agent 执行、session 更新等。
	var result activities.ExecuteSimpleTaskResult
	err := workflow.ExecuteActivity(ctx, activities.ExecuteSimpleTask, activities.ExecuteSimpleTaskInput{
		Query:            input.Query,
		UserID:           input.UserID,
		SessionID:        input.SessionID,
		Context:          input.Context,
		SessionCtx:       input.SessionCtx,
		History:          historyForAgent,
		SuggestedTools:   input.SuggestedTools,
		ToolParameters:   input.ToolParameters,
		ParentWorkflowID: workflowID,
	}).Get(ctx, &result)

	if err != nil {
		logger.Error("Simple task execution failed", "error", err)
		// Emit error event
		// 发出错误事件
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventErrorOccurred,
			AgentID:    agentName,
			Message:    activities.MsgTaskFailed(err.Error()),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
		return TaskResult{
			Success:      false,
			ErrorMessage: err.Error(),
		}, err
	}

	// Persist agent execution (await result to ensure completion)
	// 持久化 agent 执行记录（等待结果以确保完成）
	if result.Success {
		persistOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 30 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 3,
			},
		}
		persistCtx := workflow.WithActivityOptions(ctx, persistOpts)

		// Pre-generate agent execution ID using SideEffect for replay safety
		// 用 SideEffect 预生成 agent 执行 ID，保证重放安全
		var agentExecutionID string
		workflow.SideEffect(ctx, func(ctx workflow.Context) any {
			return uuid.New().String()
		}).Get(&agentExecutionID)

		// Persist agent execution
		// 持久化 agent 执行记录
		_ = workflow.ExecuteActivity(persistCtx,
			activities.PersistAgentExecutionStandalone,
			activities.PersistAgentExecutionInput{
				ID:         agentExecutionID,
				WorkflowID: workflowID,
				AgentID:    agentName,
				Input:      input.Query,
				Output:     result.Response,
				State:      "COMPLETED",
				TokensUsed: result.TokensUsed,
				ModelUsed:  result.ModelUsed,
				DurationMs: result.DurationMs,
				Error:      result.Error,
				Metadata: map[string]any{
					"workflow": "simple",
					"strategy": "simple",
				},
			}).Get(ctx, nil)

		// Persist tool executions if any
		// 如有工具执行则一并持久化
		for _, tool := range result.ToolExecutions {
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

			// Extract input params from tool execution
			// 从工具执行记录中取出输入参数
			inputParamsMap, _ := tool.InputParams.(map[string]any)

			_ = workflow.ExecuteActivity(persistCtx,
				activities.PersistToolExecutionStandalone,
				activities.PersistToolExecutionInput{
					WorkflowID:       workflowID,
					AgentID:          agentName,
					AgentExecutionID: agentExecutionID,
					ToolName:         tool.Tool,
					InputParams:      inputParamsMap,
					Output:           outputStr,
					Success:          tool.Success,
					DurationMs:       tool.DurationMs,
					Error:            tool.Error,
				}).Get(ctx, nil)
		}
	}

	// Persist to vector store for future context retrieval (await result)
	// 写入向量库供后续上下文检索（等待结果）
	if input.SessionID != "" {
		ro := workflow.ActivityOptions{StartToCloseTimeout: 30 * time.Second, RetryPolicy: &temporal.RetryPolicy{MaximumAttempts: 3}}
		// Use regular context to ensure we wait for completion
		// 用普通 context 以确保等待完成
		dctx := workflow.WithActivityOptions(ctx, ro)
		// Schedule and wait for result
		// 调度并等待结果
		_ = workflow.ExecuteActivity(dctx, activities.RecordQuery, activities.RecordQueryInput{
			SessionID: input.SessionID,
			UserID:    input.UserID,
			Query:     input.Query,
			Answer:    result.Response,
			Model:     "simple-agent",
			Metadata:  map[string]any{"workflow": "simple", "mode": "simple", "tenant_id": input.TenantID},
			RedactPII: true,
		}).Get(ctx, nil)
	}

	// Update session with token usage
	// 用 token 用量更新 session
	if input.SessionID != "" {
		var sessionUpdateResult activities.SessionUpdateResult
		err = workflow.ExecuteActivity(ctx,
			constants.UpdateSessionResultActivity,
			activities.SessionUpdateInput{
				SessionID:  input.SessionID,
				Result:     result.Response,
				TokensUsed: result.TokensUsed,
				AgentsUsed: 1,
				ModelUsed:  result.ModelUsed,
			},
		).Get(ctx, &sessionUpdateResult)
		if err != nil {
			logger.Warn("Failed to update session with tokens",
				"session_id", input.SessionID,
				"error", err,
			)
		}

		// Session title generation is now handled centrally in OrchestratorWorkflow
		// Keep version gate for replay determinism (no-op for new executions)
		// session 标题生成已统一由 OrchestratorWorkflow 负责；保留版本闸门以维持重放确定性（对新执行是空操作）。
		_ = workflow.GetVersion(ctx, "session_title_v1", workflow.DefaultVersion, 1)
	}

	// Check if we need synthesis for web_search or JSON results
	// 判断 web_search 或 JSON 结果是否需要综合
	finalResult := result.Response
	totalTokens := result.TokensUsed

	// Determine if synthesis is needed
	// 判定是否需要综合
	skipSynthesis := GetContextBool(input.Context, "skip_synthesis")
	needsSynthesis := false
	if skipSynthesis {
		logger.Info("Synthesis skipped via context flag")
	}
	if !skipSynthesis && input.SuggestedTools != nil {
		// Check if web_search was used
		// 检查是否用到了 web_search
		for _, tool := range input.SuggestedTools {
			if strings.EqualFold(tool, "web_search") {
				needsSynthesis = true
				break
			}
		}
	}

	// Check if response looks like JSON
	// 检查响应是否像 JSON
	if !needsSynthesis && !skipSynthesis {
		trimmed := strings.TrimSpace(result.Response)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			needsSynthesis = true
		}
	}

	// Check pause/cancel before synthesis
	// 综合前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_synthesis"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Perform synthesis if needed
	// 需要时执行综合
	if needsSynthesis && result.Success {
		logger.Info("Response appears to be web_search results or JSON, performing synthesis")

		// Collect citations from tool executions (best-effort)
		// 从工具执行记录中收集引用（尽力而为）
		var collectedCitations []metadata.Citation
		if len(result.ToolExecutions) > 0 {
			var resultsForCitations []any
			for _, te := range result.ToolExecutions {
				resultsForCitations = append(resultsForCitations, map[string]any{
					"agent_id": agentName,
					"tool_executions": []any{
						map[string]any{
							"tool":    te.Tool,
							"success": te.Success,
							"output":  te.Output,
							"error":   te.Error,
						},
					},
					"response": result.Response,
				})
			}
			collectedCitations, _ = metadata.CollectCitations(resultsForCitations, workflow.Now(ctx), 0)
		}

		// Build context for synthesis and inject citations for inline formatting
		// 构建综合用的 context，并注入引用以支持行内标注
		ctxForSynth := make(map[string]any)
		if input.Context != nil {
			maps.Copy(ctxForSynth, input.Context)
		}
		addCitationsToContext(ctxForSynth, collectedCitations)

		// Convert to agent results format for synthesis
		// 转成综合所需的 agent 结果格式
		agentResults := []activities.AgentExecutionResult{
			{
				AgentID:    agentName,
				Response:   result.Response,
				Success:    true,
				TokensUsed: result.TokensUsed,
			},
		}

		var synthesis activities.SynthesisResult
		err = workflow.ExecuteActivity(ctx,
			activities.SynthesizeResultsLLM,
			activities.SynthesisInput{
				Query:              input.Query,
				AgentResults:       agentResults,
				Context:            ctxForSynth,
				CollectedCitations: collectedCitations,
				ParentWorkflowID:   workflowID,
			},
		).Get(ctx, &synthesis)

		if err != nil {
			logger.Warn("Synthesis failed, using raw result", "error", err)
		} else {
			finalResult = synthesis.FinalResult
			totalTokens += synthesis.TokensUsed
			logger.Info("Synthesis completed", "additional_tokens", synthesis.TokensUsed)
		}
	}

	// Aggregate tool errors for user-facing metadata
	// 汇总工具错误，用于面向用户的 metadata
	var toolErrors []map[string]string
	if len(result.ToolExecutions) > 0 {
		for _, te := range result.ToolExecutions {
			if !te.Success || (te.Error != "") {
				toolErrors = append(toolErrors, map[string]string{
					"agent_id": agentName,
					"tool":     te.Tool,
					"error":    te.Error,
				})
			}
		}
	}

	// Record token usage for SimpleTaskWorkflow (best-effort)
	// Use a simple 60/40 split when detailed counts are unavailable
	// 缺少细粒度计数时，按 60/40 估算 prompt/completion 拆分。
	if input.UserID != "" && totalTokens > 0 {
		recCtx := opts.WithTokenRecordOptions(ctx)
		inTok := totalTokens * 6 / 10
		outTok := totalTokens - inTok
		// Prefer provider from activity result; fallback to detection from model name
		// 优先取 activity 结果里的 provider；否则从模型名推断
		provider := result.Provider
		if provider == "" {
			provider = detectProviderFromModel(result.ModelUsed)
		}
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       input.UserID,
			SessionID:    input.SessionID,
			TaskID:       workflowID, // may not be UUID; DB layer resolves via workflow_id when possible 注：可能不是 UUID；DB 层在可能时通过 workflow_id 解析
			AgentID:      agentName,
			Model:        result.ModelUsed,
			Provider:     provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata:     map[string]any{"workflow": "simple"},
		}).Get(ctx, nil)
	}

	// Memory extraction is swarm-only — removed from SimpleTaskWorkflow to avoid
	// low-value extractions bloating /memory/ on every simple query.
	// 记忆抽取仅限 swarm——已从 SimpleTaskWorkflow 移除，避免每次简单查询都产生低价值抽取。

	// Check pause/cancel before completion
	// 完成前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	logger.Info("SimpleTaskWorkflow completed successfully",
		"tokens_used", totalTokens,
	)

	// Emit final clean LLM_OUTPUT for OpenAI-compatible streaming.
	// Agent ID "final_output" signals the streamer to always show this content.
	// Agent ID "final_output" 告诉 stream 层必须展示这段内容。
	if finalResult != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    finalResult,
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"tokens_used": totalTokens,
				"model_used":  result.ModelUsed,
			},
		}).Get(ctx, nil)
	}

	// Emit completion event
	// 发出完成事件
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventAgentCompleted,
		AgentID:    agentName,
		Message:    activities.MsgTaskDone(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Emit workflow completed event for dashboards
	// 发出 workflow 完成事件供看板使用
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowCompleted,
		AgentID:    agentName,
		Message:    activities.MsgWorkflowCompleted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	meta := map[string]any{
		"mode":       "simple",
		"num_agents": 1,
	}
	if len(toolErrors) > 0 {
		meta["tool_errors"] = toolErrors
	}

	// Persist screenshot paths in task metadata (version-gated)
	// 把截图路径写进任务 metadata（受版本闸门控制）
	screenshotVersion := workflow.GetVersion(ctx, "screenshot_persistence_v1", workflow.DefaultVersion, 1)
	if screenshotVersion >= 1 && len(result.ScreenshotPaths) > 0 {
		meta["screenshots"] = result.ScreenshotPaths
	}

	// Add model and provider information for task persistence
	// 为任务持久化补上 model 与 provider 信息
	if result.ModelUsed != "" {
		meta["model"] = result.ModelUsed
		meta["model_used"] = result.ModelUsed

		// Prefer provider from activity result; fallback to detection from model name
		// 优先取 activity 结果里的 provider；否则从模型名推断
		providerForMeta := result.Provider
		if providerForMeta == "" {
			providerForMeta = detectProviderFromModel(result.ModelUsed)
		}
		meta["provider"] = providerForMeta
	}

	// Add token breakdown (60/40 split for prompt/completion)
	// 补上 token 拆分（prompt/completion 按 60/40 估算）
	if totalTokens > 0 {
		inputTokens := totalTokens * 6 / 10
		outputTokens := totalTokens - inputTokens
		meta["input_tokens"] = inputTokens
		meta["output_tokens"] = outputTokens
		meta["total_tokens"] = totalTokens

		// Calculate cost (rough estimate, actual cost calculated by service layer)
		// 计算成本（粗略估算，实际成本由 service 层计算）
		if result.ModelUsed != "" {
			cost := float64(totalTokens) * 0.0000005 // Default fallback rate 注：默认兜底费率
			meta["cost_usd"] = cost
		}
	}

	return TaskResult{
		Result:     finalResult,
		Success:    true,
		TokensUsed: totalTokens,
		Metadata:   meta,
	}, nil
}

// detectProviderFromModel determines the provider based on the model name
// deriveModelTier intelligently determines the model tier based on context
// Priority: explicit model_tier > per-agent budget > default "medium"
// 优先级：显式 model_tier > 单 agent budget > 默认 "medium"。
func deriveModelTier(ctx map[string]any) string {
	if ctx == nil {
		return "medium"
	}

	// First check for explicit model_tier
	// 先看是否显式指定了 model_tier
	if tier, ok := ctx["model_tier"].(string); ok && tier != "" {
		return tier
	}

	// Derive from per-agent budget if available
	// 有单 agent 预算时据此推导
	if budget, ok := ctx["token_budget_per_agent"].(int); ok {
		return modelTierFromBudget(budget)
	}
	if budget, ok := ctx["token_budget_per_agent"].(float64); ok {
		return modelTierFromBudget(int(budget))
	}

	// Check budget_agent_max (set by orchestrator router)
	// 检查 budget_agent_max（由 orchestrator router 设置）
	if budget, ok := ctx["budget_agent_max"].(int); ok {
		return modelTierFromBudget(budget)
	}
	if budget, ok := ctx["budget_agent_max"].(float64); ok {
		return modelTierFromBudget(int(budget))
	}

	// Default to medium for simple tasks
	// 简单任务默认用 medium
	return "medium"
}

// modelTierFromBudget maps token budget to appropriate model tier
// modelTierFromBudget 把 token 预算映射到合适的 model tier
func modelTierFromBudget(budget int) string {
	switch {
	case budget <= 8000:
		return "small" // 8k window models 注：8k 窗口模型
	case budget <= 32000:
		return "medium" // 32k window models 注：32k 窗口模型
	case budget <= 128000:
		return "large" // 128k window models 注：128k 窗口模型
	default:
		return "xlarge" // 200k+ window models 注：200k+ 窗口模型
	}
}

// addCitationsToContext 把引用格式化为可注入 prompt 的文本，并附带结构化引用列表。
func addCitationsToContext(ctxMap map[string]any, citations []metadata.Citation) {
	if len(citations) == 0 {
		return
	}
	var b strings.Builder
	for i, c := range citations {
		idx := i + 1
		title := c.Title
		if title == "" {
			title = c.Source
		}
		if c.PublishedDate != nil {
			fmt.Fprintf(&b, "[%d] %s (%s) - %s, %s\n", idx, title, c.URL, c.Source, c.PublishedDate.Format("2006-01-02"))
		} else {
			fmt.Fprintf(&b, "[%d] %s (%s) - %s\n", idx, title, c.URL, c.Source)
		}
	}
	ctxMap["available_citations"] = strings.TrimRight(b.String(), "\n")
	ctxMap["citation_count"] = len(citations)

	out := make([]map[string]any, 0, len(citations))
	for _, c := range citations {
		out = append(out, map[string]any{
			"url":               c.URL,
			"title":             c.Title,
			"source":            c.Source,
			"credibility_score": c.CredibilityScore,
			"quality_score":     c.QualityScore,
		})
	}
	ctxMap["citations"] = out
}
