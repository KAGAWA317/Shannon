package strategies

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/formatting"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metadata"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/control"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/patterns"
)

// ReactWorkflow uses the extracted React pattern for step-by-step problem solving.
// ReactWorkflow 使用抽出的 React 模式，逐步拆解问题。
// It leverages the Reason-Act-Observe loop with optional reflection.
// 借助 Reason-Act-Observe 循环，并可选地做 reflection。
func ReactWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ReactWorkflow with pattern",
		"query", input.Query,
		"session_id", input.SessionID,
		"version", "v2",
	)

	// Determine workflow ID for event streaming
	// 确定用于事件流式推送的 workflow ID
	workflowID := input.ParentWorkflowID
	if workflowID == "" {
		workflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	// Configure activity options
	// 配置 activity 选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 3 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Initialize control signal handler for pause/resume/cancel
	// 初始化用于 pause/resume/cancel 的控制信号处理器
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	// Skip SSE emissions when running as child workflow (parent already emits)
	// 作为子 workflow 运行时跳过 SSE 推送（父 workflow 已经在推送）
	controlHandler := &control.SignalHandler{
		WorkflowID:  workflowID,
		AgentID:     "react",
		Logger:      logger,
		EmitCtx:     emitCtx,
		SkipSSEEmit: input.ParentWorkflowID != "",
	}
	controlHandler.Setup(ctx)

	// Load configuration
	// 加载配置
	var config activities.WorkflowConfig
	configActivity := workflow.ExecuteActivity(ctx,
		activities.GetWorkflowConfig,
	)
	if err := configActivity.Get(ctx, &config); err != nil {
		logger.Warn("Failed to load config, using defaults", "error", err)
		config = activities.WorkflowConfig{
			ReactMaxIterations:     10,
			ReactObservationWindow: 3,
		}
	}

	// Prepare base context
	// 准备基础 context
	baseContext := make(map[string]any)
	maps.Copy(baseContext, input.Context)
	maps.Copy(baseContext, input.SessionCtx)
	if input.ParentWorkflowID != "" {
		baseContext["parent_workflow_id"] = input.ParentWorkflowID
	}

	// Memory retrieval with gate precedence (hierarchical > simple session)
	// 记忆检索的开关优先级（hierarchical > simple session）
	hierarchicalVersion := workflow.GetVersion(ctx, "memory_retrieval_v1", workflow.DefaultVersion, 1)
	sessionVersion := workflow.GetVersion(ctx, "session_memory_v1", workflow.DefaultVersion, 1)

	if hierarchicalVersion >= 1 && input.SessionID != "" {
		// Use hierarchical memory (combines recent + semantic)
		// 使用分层记忆（组合 recent + semantic）
		var hierMemory activities.FetchHierarchicalMemoryResult
		_ = workflow.ExecuteActivity(ctx, activities.FetchHierarchicalMemory,
			activities.FetchHierarchicalMemoryInput{
				Query:        input.Query,
				SessionID:    input.SessionID,
				TenantID:     input.TenantID,
				RecentTopK:   5,    // Fixed for determinism 注：为保确定性固定取值
				SemanticTopK: 5,    // Fixed for determinism 注：为保确定性固定取值
				Threshold:    0.75, // Fixed semantic threshold 注：固定的语义阈值
			}).Get(ctx, &hierMemory)

		if len(hierMemory.Items) > 0 {
			baseContext["agent_memory"] = hierMemory.Items
			logger.Info("Injected hierarchical memory into React context",
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
				TopK:      20, // Fixed for determinism 注：为保确定性固定取值
			}).Get(ctx, &sessionMemory)

		if len(sessionMemory.Items) > 0 {
			baseContext["agent_memory"] = sessionMemory.Items
			logger.Info("Injected session memory into React context",
				"session_id", input.SessionID,
				"memory_items", len(sessionMemory.Items),
			)
		}
	}

	// Context compression (version-gated for determinism)
	// context 压缩（按版本开关控制，保证确定性）
	compressionVersion := workflow.GetVersion(ctx, "context_compress_v1", workflow.DefaultVersion, 1)
	if compressionVersion >= 1 && input.SessionID != "" && len(input.History) > 20 {
		// Check if compression is needed with rate limiting
		// 带限流地检查是否需要压缩
		estimatedTokens := activities.EstimateTokens(convertHistoryForAgent(input.History))
		tierForCompression := "medium"

		var checkResult activities.CheckCompressionNeededResult
		err := workflow.ExecuteActivity(ctx, "CheckCompressionNeeded",
			activities.CheckCompressionNeededInput{
				SessionID:       input.SessionID,
				MessageCount:    len(input.History),
				EstimatedTokens: estimatedTokens,
				ModelTier:       tierForCompression,
			}).Get(ctx, &checkResult)

		if err == nil && checkResult.ShouldCompress {
			logger.Info("Triggering context compression in React workflow",
				"session_id", input.SessionID,
				"reason", checkResult.Reason,
				"message_count", len(input.History),
			)

			// Compress context via activity
			// 通过 activity 压缩 context
			var compressResult activities.CompressContextResult
			err = workflow.ExecuteActivity(ctx, activities.CompressAndStoreContext,
				activities.CompressContextInput{
					SessionID:        input.SessionID,
					History:          convertHistoryMapForCompression(input.History),
					TargetTokens:     int(float64(activities.GetModelWindowSize(tierForCompression)) * 0.375),
					ParentWorkflowID: input.ParentWorkflowID,
				}).Get(ctx, &compressResult)

			if err == nil && compressResult.Summary != "" && compressResult.Stored {
				logger.Info("Context compressed and stored",
					"session_id", input.SessionID,
					"summary_length", len(compressResult.Summary),
				)

				// Update compression state in session
				// 更新 session 中的压缩状态
				var updateResult activities.UpdateCompressionStateResult
				_ = workflow.ExecuteActivity(ctx, "UpdateCompressionStateActivity",
					activities.UpdateCompressionStateInput{
						SessionID:    input.SessionID,
						MessageCount: len(input.History),
					}).Get(ctx, &updateResult)
			}
		}
	}

	// Check for budget configuration
	// 检查 budget 配置
	agentMaxTokens := getBudgetMax(baseContext)

	// Determine model tier based on query complexity
	// 根据 query 复杂度确定 model tier
	modelTier := "medium" // Default for React tasks 注：React 任务的默认值
	// Resolve an approximate concrete model for pricing/metadata; prefer provider override from context
	// 为计费/metadata 解析一个近似具体的 model；优先采用 context 中的 provider override
	providerOverride := ""
	if v, ok := baseContext["provider_override"].(string); ok && strings.TrimSpace(v) != "" {
		providerOverride = strings.ToLower(strings.TrimSpace(v))
	} else if v, ok := baseContext["provider"].(string); ok && strings.TrimSpace(v) != "" {
		providerOverride = strings.ToLower(strings.TrimSpace(v))
	} else if v, ok := baseContext["llm_provider"].(string); ok && strings.TrimSpace(v) != "" {
		providerOverride = strings.ToLower(strings.TrimSpace(v))
	}
	approxModel := ""
	if providerOverride != "" {
		approxModel = pricing.GetPriorityModelForProvider(modelTier, providerOverride)
	}
	if approxModel == "" {
		approxModel = pricing.GetPriorityOneModel(modelTier)
	}
	if approxModel == "" {
		approxModel = modelTier
	}

	// Configure React pattern
	// 配置 React 模式
	reactConfig := patterns.ReactConfig{
		MaxIterations:     config.ReactMaxIterations,
		MinIterations:     1,
		ObservationWindow: config.ReactObservationWindow,
		MaxObservations:   100, // Safety limit 注：安全上限
		MaxThoughts:       50,  // Safety limit 注：安全上限
		MaxActions:        50,  // Safety limit 注：安全上限
	}

	reactOpts := patterns.Options{
		BudgetAgentMax: agentMaxTokens,
		SessionID:      input.SessionID,
		UserID:         input.UserID,
		EmitEvents:     true,
		ModelTier:      modelTier,
		Context:        baseContext,
	}

	// Check pause/cancel before React loop execution
	// 执行 React 循环前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Execute React loop
	// 执行 React 循环
	logger.Info("Executing React loop pattern",
		"max_iterations", reactConfig.MaxIterations,
		"observation_window", reactConfig.ObservationWindow,
	)

	reactResult, err := patterns.ReactLoop(
		ctx,
		input.Query,
		baseContext,
		input.SessionID,
		convertHistoryForAgent(input.History),
		reactConfig,
		reactOpts,
	)

	if err != nil {
		logger.Error("React loop failed", "error", err)
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("React loop failed: %v", err),
		}, err
	}

	// Check pause/cancel before reflection
	// reflection 前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_reflection"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Optional: Apply reflection for quality improvement on complex results
	// 可选：对复杂结果应用 reflection 以提升质量
	finalResult := reactResult.FinalResult
	qualityScore := 0.5
	totalTokens := reactResult.TotalTokens
	reflectionTokens := 0

	if reactResult.Iterations > 5 { // Complex task that needed many iterations 注：需要多轮迭代的复杂任务
		logger.Info("Applying reflection for quality improvement",
			"iterations", reactResult.Iterations,
		)

		reflectionConfig := patterns.ReflectionConfig{
			Enabled:             true,
			MaxRetries:          1, // Single reflection pass for React 注：React 只做一次 reflection
			ConfidenceThreshold: 0.7,
			Criteria:            []string{"completeness", "correctness", "clarity"},
			TimeoutMs:           30000,
		}

		reflectionOpts := patterns.Options{
			BudgetAgentMax: agentMaxTokens,
			SessionID:      input.SessionID,
			UserID:         input.UserID,
			ModelTier:      modelTier,
		}

		// Convert React result to agent result for reflection
		// 把 React 结果转成 agent 结果，供 reflection 使用
		approxModelRefl := pricing.GetPriorityOneModel(modelTier)
		if approxModelRefl == "" {
			approxModelRefl = modelTier
		}
		agentResults := []activities.AgentExecutionResult{
			{
				AgentID:    "react-agent",
				Response:   reactResult.FinalResult,
				TokensUsed: reactResult.TotalTokens,
				Success:    true,
				ModelUsed:  approxModelRefl,
			},
		}

		improvedResult, score, reflTokens, err := patterns.ReflectOnResult(
			ctx,
			input.Query,
			reactResult.FinalResult,
			agentResults,
			baseContext,
			reflectionConfig,
			reflectionOpts,
		)

		if err == nil {
			finalResult = improvedResult
			qualityScore = score
			reflectionTokens = reflTokens
			totalTokens += reflectionTokens
			logger.Info("Reflection improved quality",
				"score", qualityScore,
				"tokens", reflectionTokens,
			)
		} else {
			logger.Warn("Reflection failed, using original result", "error", err)
		}
	}

	// Optional: collect citations and append Sources section when enabled
	// 可选：启用时收集引用并追加 Sources 段落
	var collectedCitations []metadata.Citation
	if v, ok := baseContext["enable_citations"].(bool); ok && v {
		var resultsForCitations []any
		for _, ar := range reactResult.AgentResults {
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
		now := workflow.Now(ctx)
		citations, _ := metadata.CollectCitations(resultsForCitations, now, 0)
		if len(citations) > 0 {
			collectedCitations = citations
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
			citationList := strings.TrimRight(b.String(), "\n")
			finalResult = formatting.FormatReportWithCitations(finalResult, citationList)
		}
	}

	// Update session with results (include per-agent usage for accurate cost)
	// 用结果更新 session（带上每个 agent 的用量，以便准确计算成本）
	if input.SessionID != "" {
		var updRes activities.SessionUpdateResult
		// Build per-agent usage from pattern loop results
		// 从 pattern 循环结果构造每个 agent 的用量
		usages := make([]activities.AgentUsage, 0, len(reactResult.AgentResults))
		for _, ar := range reactResult.AgentResults {
			usages = append(usages, activities.AgentUsage{Model: ar.ModelUsed, Tokens: ar.TokensUsed, InputTokens: ar.InputTokens, OutputTokens: ar.OutputTokens})
		}
		err = workflow.ExecuteActivity(ctx,
			constants.UpdateSessionResultActivity,
			activities.SessionUpdateInput{
				SessionID:  input.SessionID,
				Result:     finalResult,
				TokensUsed: totalTokens,
				AgentsUsed: reactResult.Iterations * 2, // Reasoner + Actor per iteration 注：每轮 Reasoner + Actor 两个 agent
				AgentUsage: usages,
			}).Get(ctx, &updRes)

		if err != nil {
			logger.Error("Failed to update session", "error", err)
		}

		// Persist to vector store (await result to prevent race condition)
		// 持久化到向量库（等待结果以避免竞态）
		_ = workflow.ExecuteActivity(ctx,
			activities.RecordQuery,
			activities.RecordQueryInput{
				SessionID: input.SessionID,
				UserID:    input.UserID,
				Query:     input.Query,
				Answer:    finalResult,
				Model:     approxModel,
				Metadata: map[string]any{
					"workflow":      "react_v2",
					"iterations":    reactResult.Iterations,
					"quality_score": qualityScore,
					"thoughts":      len(reactResult.Thoughts),
					"actions":       len(reactResult.Actions),
					"observations":  len(reactResult.Observations),
					"tenant_id":     input.TenantID,
				},
				RedactPII: true,
			}).Get(ctx, nil)
	}

	logger.Info("ReactWorkflow completed successfully",
		"total_tokens", totalTokens,
		"quality_score", qualityScore,
		"iterations", reactResult.Iterations,
	)

	// Record pattern metrics (fire-and-forget)
	// 记录 pattern 指标（fire-and-forget）
	metricsCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
	})
	_ = workflow.ExecuteActivity(metricsCtx, "RecordPatternMetrics", activities.PatternMetricsInput{
		Pattern:      "react",
		Version:      "v2",
		AgentCount:   reactResult.Iterations * 2, // Reasoner + Actor per iteration 注：每轮 Reasoner + Actor 两个 agent
		TokensUsed:   totalTokens,
		WorkflowType: "react",
	}).Get(ctx, nil)

	// Aggregate tool errors from React agent results
	// 汇总 React agent 结果中的工具错误
	var toolErrors []map[string]string
	for _, ar := range reactResult.AgentResults {
		if len(ar.ToolExecutions) == 0 {
			continue
		}
		for _, te := range ar.ToolExecutions {
			if !te.Success || (te.Error != "") {
				toolErrors = append(toolErrors, map[string]string{
					"agent_id": ar.AgentID,
					"tool":     te.Tool,
					"error":    te.Error,
				})
			}
		}
	}

	meta := map[string]any{
		"version":       "v2",
		"iterations":    reactResult.Iterations,
		"quality_score": qualityScore,
		"thoughts":      len(reactResult.Thoughts),
		"actions":       len(reactResult.Actions),
		"observations":  len(reactResult.Observations),
	}
	if len(collectedCitations) > 0 {
		out := make([]map[string]any, 0, len(collectedCitations))
		for _, c := range collectedCitations {
			out = append(out, map[string]any{
				"url":               c.URL,
				"title":             c.Title,
				"source":            c.Source,
				"credibility_score": c.CredibilityScore,
				"quality_score":     c.QualityScore,
			})
		}
		meta["citations"] = out
	}
	if len(toolErrors) > 0 {
		meta["tool_errors"] = toolErrors
	}

	// Aggregate agent metadata (model, provider, tokens, cost)
	// 汇总 agent metadata（model、provider、tokens、cost）
	// Note: ReactResult doesn't track per-iteration models; use tier's priority-1 model
	// ReactResult 不跟踪每轮使用的 model，这里取该档位 priority-1 的 model。
	agentResults := []activities.AgentExecutionResult{
		{
			AgentID:      "react-agent",
			ModelUsed:    approxModel,
			TokensUsed:   reactResult.TotalTokens,
			InputTokens:  reactResult.TotalTokens * 6 / 10, // Estimate 60/40 split 注：按 60/40 估算拆分
			OutputTokens: reactResult.TotalTokens * 4 / 10,
			Success:      true,
		},
	}
	agentMeta := metadata.AggregateAgentMetadata(agentResults, reflectionTokens)
	maps.Copy(meta, agentMeta)

	// Align: compute and include estimated cost using centralized pricing
	// 对齐：使用集中式 pricing 计算并写入预估成本
	if totalTokens > 0 {
		meta["cost_usd"] = pricing.CostForTokens(approxModel, totalTokens)
	}

	// Check pause/cancel before completion
	// 完成前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Emit final clean LLM_OUTPUT for OpenAI-compatible streaming.
	// 为 OpenAI 兼容流式输出推送最终干净的 LLM_OUTPUT。
	// Agent ID "final_output" signals the streamer to always show this content.
	// Agent ID "final_output" 会提示 streamer 始终展示这段内容。
	if finalResult != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    finalResult,
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"tokens_used": totalTokens,
				"model_used":  approxModel,
			},
		}).Get(ctx, nil)
	}

	// Emit WORKFLOW_COMPLETED before returning
	// 返回前推送 WORKFLOW_COMPLETED
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowCompleted,
		AgentID:    "react",
		Message:    activities.MsgWorkflowCompleted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	return TaskResult{
		Result:     finalResult,
		Success:    true,
		TokensUsed: totalTokens,
		Metadata:   meta,
	}, nil
}
