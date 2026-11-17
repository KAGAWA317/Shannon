package patterns

import (
	"fmt"
	"maps"
	"strings"
	"time" // Used for activity timeout durations 注：用于 activity 超时时长

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	wopts "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// ReactConfig controls the Reason-Act-Observe loop behavior
// ReactConfig 控制 Reason-Act-Observe 循环的行为
type ReactConfig struct {
	MaxIterations     int // Maximum number of ReAct loops 注：ReAct 循环的最大次数
	MinIterations     int // Minimum iterations before allowing completion 注：允许完成前的最小迭代次数
	ObservationWindow int // How many recent observations to consider 注：参与推理的最近观测条数
	MaxObservations   int // Maximum observations to keep 注：保留的观测上限
	MaxThoughts       int // Maximum thoughts to track 注：记录的 thoughts 上限
	MaxActions        int // Maximum actions to track 注：记录的 actions 上限
}

// ReactLoopResult contains the results of a ReAct execution
// ReactLoopResult 保存一次 ReAct 执行的结果
type ReactLoopResult struct {
	Thoughts     []string
	Actions      []string
	Observations []string
	FinalResult  string
	TotalTokens  int
	Iterations   int
	AgentResults []activities.AgentExecutionResult
}

// ReactLoop executes a Reason-Act-Observe loop for step-by-step problem solving.
// It alternates between reasoning about what to do next, taking actions, and observing results.
// ReactLoop 执行 Reason-Act-Observe 循环，逐步解决问题。
// 它在推理下一步做什么、采取行动、观测结果之间交替。
func ReactLoop(
	ctx workflow.Context,
	query string,
	baseContext map[string]any,
	sessionID string,
	history []string,
	config ReactConfig,
	opts Options,
) (*ReactLoopResult, error) {

	logger := workflow.GetLogger(ctx)

	// Set activity options
	// 设置 activity 选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Initialize state
	// 初始化状态
	var observations []string
	var thoughts []string
	var actions []string
	totalTokens := 0
	iteration := 0
	toolExecuted := false

	// Default minimum iterations to 1 if unset to keep backwards compatibility
	// 未设置时把最小迭代次数默认为 1，以保持向后兼容
	if config.MinIterations <= 0 {
		config.MinIterations = 1
	}

	isResearch := false
	if baseContext != nil {
		if util.GetContextBool(baseContext, "force_research") {
			isResearch = true
		} else if rs, ok := baseContext["research_strategy"].(string); ok && strings.TrimSpace(rs) != "" {
			isResearch = true
		}
	}

	// Main Reason-Act-Observe loop
	// 主 Reason-Act-Observe 循环
	var agentResults []activities.AgentExecutionResult

	// Get workflow ID for SSE events
	// 获取 workflow ID，用于 SSE 事件
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	if baseContext != nil {
		if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
			wfID = p
		}
	}

	for iteration < config.MaxIterations {
		logger.Info("ReAct iteration",
			"iteration", iteration+1,
			"observations_count", len(observations),
		)

		reasonerID := agents.GetAgentName(wfID, iteration*2)
		actorID := agents.GetAgentName(wfID, iteration*2+1)

		// Emit iteration progress
		// 发出迭代进度
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "PROGRESS",
			AgentID:    "react",
			Message:    activities.MsgReactIteration(iteration+1, config.MaxIterations),
			Timestamp:  workflow.Now(ctx),
		})

		// Phase 1: REASON - Think about what to do next
		// 阶段 1：REASON——思考下一步做什么
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "AGENT_STARTED",
			AgentID:    reasonerID,
			Message:    activities.MsgReactReasoning(),
			Timestamp:  workflow.Now(ctx),
		})

		reasonContext := make(map[string]any)
		maps.Copy(reasonContext, baseContext)
		reasonContext["query"] = query
		reasonContext["observations"] = getRecentObservations(observations, config.ObservationWindow)
		reasonContext["thoughts"] = thoughts
		reasonContext["actions"] = actions
		reasonContext["iteration"] = iteration

		reasonQuery := fmt.Sprintf(
			"REASON (1–2 sentences) about the single next action for: %s\nConstraints:\n- Keep response in the same language as the user's query.\n- State exactly what to do, why it's needed, and expected outcome.\n- If external information is required, say 'search'.\nContext: Previous observations: %v",
			query,
			getRecentObservations(observations, config.ObservationWindow),
		)

		var reasonResult activities.AgentExecutionResult
		var err error

		// Execute reasoning with optional budget
		// 在可选 budget 限制下执行推理
		if opts.BudgetAgentMax > 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if p, ok := reasonContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
			err = workflow.ExecuteActivity(ctx,
				constants.ExecuteAgentWithBudgetActivity,
				activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            reasonQuery,
						AgentID:          reasonerID,
						Context:          reasonContext,
						Mode:             "standard",
						SessionID:        sessionID,
						UserID:           opts.UserID,
						History:          history,
						ParentWorkflowID: wid,
					},
					MaxTokens: opts.BudgetAgentMax,
					UserID:    opts.UserID,
					TaskID:    wid,
					ModelTier: opts.ModelTier,
				}).Get(ctx, &reasonResult)
		} else {
			// Determine parent workflow for streaming correlation
			// 确定用于流式关联的父 workflow
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if p, ok := reasonContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
			err = workflow.ExecuteActivity(ctx,
				"ExecuteAgent",
				activities.AgentExecutionInput{
					Query:            reasonQuery,
					AgentID:          reasonerID,
					Context:          reasonContext,
					Mode:             "standard",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					ParentWorkflowID: wid,
				}).Get(ctx, &reasonResult)
		}

		if err != nil {
			logger.Error("Reasoning failed", "error", err)
			break
		}

		// Emit reasoning complete
		// 发出推理完成事件
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "AGENT_COMPLETED",
			AgentID:    reasonerID,
			Message:    activities.MsgReactReasoningDone(),
			Timestamp:  workflow.Now(ctx),
		})

		thoughts = append(thoughts, reasonResult.Response)
		// Trim thoughts if exceeding limit
		// thoughts 超过上限时裁剪
		if len(thoughts) > config.MaxThoughts {
			thoughts = thoughts[len(thoughts)-config.MaxThoughts:]
		}
		totalTokens += reasonResult.TokensUsed
		// Record token usage for the reasoner step
		// Avoid double-recording when budgeted execution already recorded usage inside the activity
		// 记录 reasoner 步骤的 token 用量
		// budgeted 执行已在 activity 内部记录过用量，避免重复记录
		if opts.BudgetAgentMax <= 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if baseContext != nil {
				if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
			}
			inTok := reasonResult.InputTokens
			outTok := reasonResult.OutputTokens
			if inTok == 0 && outTok == 0 && reasonResult.TokensUsed > 0 {
				inTok = reasonResult.TokensUsed * 6 / 10
				outTok = reasonResult.TokensUsed - inTok
			}
			// Fallbacks for missing model/provider
			// model/provider 缺失时的兜底
			model := reasonResult.ModelUsed
			if strings.TrimSpace(model) == "" {
				if m := pricing.GetPriorityOneModel(opts.ModelTier); m != "" {
					model = m
				}
			}
			provider := reasonResult.Provider
			if strings.TrimSpace(provider) == "" {
				provider = imodels.DetectProvider(model)
			}
			recCtx := wopts.WithTokenRecordOptions(ctx)
			_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
				UserID:       opts.UserID,
				SessionID:    sessionID,
				TaskID:       wid,
				AgentID:      reasonerID,
				Model:        model,
				Provider:     provider,
				InputTokens:  inTok,
				OutputTokens: outTok,
				Metadata:     map[string]any{"phase": "react_reason"},
			}).Get(recCtx, nil)
			wopts.RecordToolCostEntries(ctx, reasonResult, opts.UserID, sessionID, wid)
		}

		// Check if reasoning indicates completion
		// 检查推理是否表示已完成
		canComplete := isTaskComplete(reasonResult.Response)
		if canComplete {
			// Enforce minimum iterations
			// 强制满足最小迭代次数
			if iteration+1 < config.MinIterations {
				logger.Info("Completion deferred due to MinIterations",
					"iteration", iteration+1,
					"min_iterations", config.MinIterations,
				)
				canComplete = false
			}
			// Research flows must produce evidence before stopping
			// 研究流程必须产出证据后才能停止
			if isResearch && (!toolExecuted || len(observations) == 0) {
				logger.Info("Completion deferred to collect research evidence",
					"iteration", iteration+1,
					"tool_executed", toolExecuted,
					"observations", len(observations),
				)
				canComplete = false
			}
			// Avoid stopping with zero observations in any flow
			// 任何流程都避免在零观测的情况下停止
			if len(observations) == 0 && len(actions) == 0 {
				logger.Info("Completion deferred because no observations/actions recorded",
					"iteration", iteration+1,
				)
				canComplete = false
			}
		}

		if canComplete {
			logger.Info("Task marked complete by reasoning",
				"iteration", iteration+1,
			)
			break
		}

		// Phase 2: ACT - Execute the planned action
		// 阶段 2：ACT——执行计划中的动作
		actionContext := make(map[string]any)
		maps.Copy(actionContext, baseContext)
		actionContext["query"] = query
		actionContext["current_thought"] = reasonResult.Response
		actionContext["observations"] = getRecentObservations(observations, config.ObservationWindow)

		// Let LLM decide tools - no pattern matching
		// The agent will select appropriate tools based on the action query
		// 由 LLM 决定 tools——不做 pattern matching
		// agent 会依据 action query 选择合适的 tools
		var suggestedTools []string
		// Bias toward web_search + web_fetch when running in research mode to ensure citations
		// The LLM can use web_search to find sources, then web_fetch to read full content
		// For non-research tasks, enable python_executor for computation tasks
		// tool 由 LLM 自行选择，不做 pattern matching；研究 mode 下偏向 web_search + web_fetch 以保证引用，
		// 非研究场景则启用 python_executor 以支持计算。
		if baseContext != nil {
			if util.GetContextBool(baseContext, "force_research") {
				suggestedTools = []string{"web_search", "web_fetch", "python_executor"}
			} else if rs, ok := baseContext["research_strategy"].(string); ok && strings.TrimSpace(rs) != "" {
				suggestedTools = []string{"web_search", "web_fetch", "python_executor"}
			} else {
				// Enable python_executor for general tasks to allow real code execution
				// This prevents hallucination where LLM writes <tool_call> as text
				// Note: Firecracker VM has no network; only pure computation works
				// 启用 python_executor 让 LLM 真正执行代码，避免它把 <tool_call> 当文本写出（幻觉）；
				// 注意 Firecracker VM 无网络，只能做纯计算。
				suggestedTools = []string{"python_executor"}
			}
		} else {
			// Fallback: enable python_executor even if baseContext is nil
			// 兜底：即使 baseContext 为 nil 也启用 python_executor
			suggestedTools = []string{"python_executor"}
		}

		actionQuery := ""
		if isResearch {
			// Research mode: be explicit about tool usage and reporting
			// Build quoted queries and site filters from context when available
			// 研究模式：明确要求 tool 的使用与汇报
			// 有上下文时据此构造带引号的 query 与 site 过滤
			quoted := ""
			if eq, ok := actionContext["exact_queries"]; ok {
				switch t := eq.(type) {
				case []string:
					if len(t) > 0 {
						quoted = strings.Join(t, " OR ")
					}
				case []any:
					parts := make([]string, 0, len(t))
					for _, it := range t {
						if s, ok := it.(string); ok && s != "" {
							parts = append(parts, s)
						}
					}
					if len(parts) > 0 {
						quoted = strings.Join(parts, " OR ")
					}
				}
			}
			domains := ""
			if od, ok := actionContext["official_domains"]; ok {
				switch t := od.(type) {
				case []string:
					if len(t) > 0 {
						// Create new slice to avoid mutating the original context
						// 新建 slice，避免修改原始 context
						prefixed := make([]string, len(t))
						for i, d := range t {
							prefixed[i] = fmt.Sprintf("site:%s", d)
						}
						domains = strings.Join(prefixed, " OR ")
					}
				case []any:
					parts := []string{}
					for _, it := range t {
						if s, ok := it.(string); ok && s != "" {
							parts = append(parts, fmt.Sprintf("site:%s", s))
						}
					}
					if len(parts) > 0 {
						domains = strings.Join(parts, " OR ")
					}
				}
			}
			disamb := ""
			if dt, ok := actionContext["disambiguation_terms"]; ok {
				switch t := dt.(type) {
				case []string:
					if len(t) > 0 {
						disamb = strings.Join(t, ", ")
					}
				case []any:
					parts := []string{}
					for _, it := range t {
						if s, ok := it.(string); ok && s != "" {
							parts = append(parts, s)
						}
					}
					if len(parts) > 0 {
						disamb = strings.Join(parts, ", ")
					}
				}
			}

			searchLine := fmt.Sprintf("Use web_search with queries: %s", quoted)
			if quoted == "" {
				searchLine = fmt.Sprintf("Use web_search to find authoritative information about: %s", query)
			}
			if domains != "" {
				searchLine += fmt.Sprintf("; prefer domains (%s)", domains)
			}
			if disamb != "" {
				searchLine += fmt.Sprintf("; add disambiguation: %s", disamb)
			}

			actionQuery = fmt.Sprintf(
				"ACT on this plan: %s\n\nIMPORTANT:\n- %s\n- For web_search parameters: search_type ∈ {neural|keyword|auto} (default 'auto' if unsure); category ∈ {company|research paper|news|pdf|github|tweet|personal site|linkedin|financial report}; NEVER leave category blank. If the query targets a specific domain (site:example.com or explicit host), default category=personal site; otherwise pick the closest match.\n- After web_search, run web_fetch on the selected URLs to read content before summarizing; do not summarize from snippets alone.\n- Execute NOW and return findings in the SAME language as the user's query.\n- For each source used, include a 1–2 sentence summary plus Title and URL inline.\n- Keep the action atomic.\n- Do NOT include a '## Sources' section (the system will append Sources).",
				reasonResult.Response,
				searchLine,
			)
		} else {
			actionQuery = fmt.Sprintf(
				"ACT on this plan: %s\nConstraints:\n- Execute the next step with available tools.\n- Keep response in the SAME language as the user's query.\n- Keep actions atomic. If you used a tool that fetched information, add a brief 1–2 sentence summary of the key finding.",
				reasonResult.Response,
			)
		}

		// Emit acting started
		// 发出 action 开始事件
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "AGENT_STARTED",
			AgentID:    actorID,
			Message:    activities.MsgReactActing(),
			Timestamp:  workflow.Now(ctx),
		})

		var actionResult activities.AgentExecutionResult

		// Execute action with optional budget
		// 在可选 budget 限制下执行 action
		if opts.BudgetAgentMax > 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if p, ok := actionContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
			err = workflow.ExecuteActivity(ctx,
				constants.ExecuteAgentWithBudgetActivity,
				activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            actionQuery,
						AgentID:          actorID,
						Context:          actionContext,
						Mode:             "standard",
						SessionID:        sessionID,
						UserID:           opts.UserID,
						History:          history,
						SuggestedTools:   suggestedTools,
						ParentWorkflowID: wid,
					},
					MaxTokens: opts.BudgetAgentMax,
					UserID:    opts.UserID,
					TaskID:    wid,
					ModelTier: opts.ModelTier,
				}).Get(ctx, &actionResult)
		} else {
			// Determine parent workflow for streaming correlation
			// 确定用于流式关联的父 workflow
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if p, ok := actionContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
			err = workflow.ExecuteActivity(ctx,
				"ExecuteAgent",
				activities.AgentExecutionInput{
					Query:            actionQuery,
					AgentID:          actorID,
					Context:          actionContext,
					Mode:             "standard",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					SuggestedTools:   suggestedTools,
					ParentWorkflowID: wid,
				}).Get(ctx, &actionResult)
		}

		if err != nil {
			logger.Error("Action execution failed", "error", err)
			observations = append(observations, fmt.Sprintf("Error: %v", err))
			// Continue to next iteration to try recovery
			// 进入下一轮迭代尝试恢复
		} else {
			// Emit acting complete
			// 发出 action 完成事件
			streaming.Get().Publish(wfID, streaming.Event{
				WorkflowID: wfID,
				Type:       "AGENT_COMPLETED",
				AgentID:    actorID,
				Message:    activities.MsgReactActingDone(),
				Timestamp:  workflow.Now(ctx),
			})

			// Track whether any tool actually executed successfully
			// 记录是否有 tool 真正执行成功
			if hasSuccessfulToolExecution(actionResult.ToolExecutions) {
				toolExecuted = true
			}

			// Always track tokens and tool executions for citations
			// 始终记录 tokens 与 tool 执行情况，供引用使用
			totalTokens += actionResult.TokensUsed
			agentResults = append(agentResults, actionResult)

			// Skip recording empty action responses (treat as no-op)
			// 空 action 响应不记录（视为 no-op）
			if strings.TrimSpace(actionResult.Response) != "" {
				// Remove base64 screenshot data from actions (already emitted to UI)
				// 从 actions 中移除 base64 截图数据（已发给 UI）
				truncatedAction := truncateBase64Images(actionResult.Response)
				actions = append(actions, truncatedAction)
				// Trim actions if exceeding limit
				// actions 超过上限时裁剪
				if len(actions) > config.MaxActions {
					actions = actions[len(actions)-config.MaxActions:]
				}
				// Phase 3: OBSERVE - Record and analyze the result
				// Remove base64 screenshot data to prevent context overflow
				// Screenshots are already emitted to UI via TOOL_OBSERVATION events
				// 阶段 3：OBSERVE——记录并分析结果
				// 移除 base64 截图数据，防止 context 溢出
				// 截图已通过 TOOL_OBSERVATION event 发给 UI
				observation := fmt.Sprintf("Action result: %s", truncatedAction)
				observations = append(observations, observation)
			} else {
				logger.Warn("Empty action response; skipping record",
					"iteration", iteration+1,
					"agent_id", actorID,
				)
			}

			// Keep only recent observations to prevent memory growth
			// 只保留最近的观测，防止内存增长
			if len(observations) > config.MaxObservations {
				// Create a summary of oldest observations
				// 为最旧的观测生成摘要
				oldCount := len(observations) - config.MaxObservations + 1
				summary := fmt.Sprintf("[%d older observations truncated]", oldCount)
				observations = append([]string{summary}, observations[len(observations)-config.MaxObservations+1:]...)
			}

			// Record token usage for the action step
			// Avoid double-recording when budgeted execution already recorded usage inside the activity
			// 记录 action 步骤的 token 用量
			// budgeted 执行已在 activity 内部记录过用量，避免重复记录
			if opts.BudgetAgentMax <= 0 {
				wid := workflow.GetInfo(ctx).WorkflowExecution.ID
				if p, ok := actionContext["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
				inTok := actionResult.InputTokens
				outTok := actionResult.OutputTokens
				if inTok == 0 && outTok == 0 && actionResult.TokensUsed > 0 {
					inTok = actionResult.TokensUsed * 6 / 10
					outTok = actionResult.TokensUsed - inTok
				}
				// Fallbacks for missing model/provider
				// model/provider 缺失时的兜底
				model := actionResult.ModelUsed
				if strings.TrimSpace(model) == "" {
					if m := pricing.GetPriorityOneModel(opts.ModelTier); m != "" {
						model = m
					}
				}
				provider := actionResult.Provider
				if strings.TrimSpace(provider) == "" {
					provider = imodels.DetectProvider(model)
				}
				recCtx := wopts.WithTokenRecordOptions(ctx)
				_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
					UserID:       opts.UserID,
					SessionID:    sessionID,
					TaskID:       wid,
					AgentID:      actorID,
					Model:        model,
					Provider:     provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     map[string]any{"phase": "react_action"},
				}).Get(recCtx, nil)
				wopts.RecordToolCostEntries(ctx, actionResult, opts.UserID, sessionID, wid)
			}

			logger.Info("Observation recorded",
				"iteration", iteration+1,
				"total_observations", len(observations),
			)
		}

		iteration++

		// Check for early termination based on multiple criteria
		// 按多个判据检查是否提前终止
		if shouldStopReactLoop(observations, thoughts, agentResults, iteration) {
			logger.Info("Early termination criteria met",
				"iteration", iteration,
				"total_agent_results", len(agentResults),
			)
			break
		}
	}

	// Emit loop completed
	// 发出循环完成事件
	streaming.Get().Publish(wfID, streaming.Event{
		WorkflowID: wfID,
		Type:       "PROGRESS",
		AgentID:    "react",
		Message:    activities.MsgReactLoopDone(),
		Timestamp:  workflow.Now(ctx),
	})

	// Final synthesis of all observations and actions
	// 对全部观测与动作做最终综合
	logger.Info("Synthesizing final result from ReAct loops")

	synthesisQuery := fmt.Sprintf(
		"Synthesize the final answer for: %s\nThoughts: %v\nActions: %v\nObservations: %v",
		query,
		thoughts,
		actions,
		observations,
	)

	var finalResult activities.AgentExecutionResult

	// Execute final synthesis with optional budget
	// 在可选 budget 限制下执行最终综合
	if opts.BudgetAgentMax > 0 {
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if baseContext != nil {
			if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		err := workflow.ExecuteActivity(ctx,
			constants.ExecuteAgentWithBudgetActivity,
			activities.BudgetedAgentInput{
				AgentInput: activities.AgentExecutionInput{
					Query:            synthesisQuery,
					AgentID:          "react-synthesizer",
					Context:          baseContext,
					Mode:             "standard",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					ParentWorkflowID: wid,
				},
				MaxTokens: opts.BudgetAgentMax,
				UserID:    opts.UserID,
				TaskID:    wid,
				ModelTier: opts.ModelTier,
			}).Get(ctx, &finalResult)

		if err != nil {
			return nil, fmt.Errorf("final synthesis failed: %w", err)
		}
	} else {
		// Determine parent workflow for streaming correlation
		// 确定用于流式关联的父 workflow
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if baseContext != nil {
			if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		err := workflow.ExecuteActivity(ctx,
			"ExecuteAgent",
			activities.AgentExecutionInput{
				Query:            synthesisQuery,
				AgentID:          "react-synthesizer",
				Context:          baseContext,
				Mode:             "standard",
				SessionID:        sessionID,
				UserID:           opts.UserID,
				History:          history,
				ParentWorkflowID: wid,
			}).Get(ctx, &finalResult)

		if err != nil {
			return nil, fmt.Errorf("final synthesis failed: %w", err)
		}
	}

	totalTokens += finalResult.TokensUsed
	agentResults = append(agentResults, finalResult)

	// Record token usage for the react-synthesizer step
	// Avoid double-recording when budgeted execution already recorded usage inside the activity
	// 记录 react-synthesizer 步骤的 token 用量
	// budgeted 执行已在 activity 内部记录过用量，避免重复记录
	if opts.BudgetAgentMax <= 0 {
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if baseContext != nil {
			if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		inTok := finalResult.InputTokens
		outTok := finalResult.OutputTokens
		if inTok == 0 && outTok == 0 && finalResult.TokensUsed > 0 {
			inTok = finalResult.TokensUsed * 6 / 10
			outTok = finalResult.TokensUsed - inTok
		}
		// Fallbacks for missing model/provider
		// model/provider 缺失时的兜底
		model := finalResult.ModelUsed
		if strings.TrimSpace(model) == "" {
			if m := pricing.GetPriorityOneModel(opts.ModelTier); m != "" {
				model = m
			}
		}
		provider := finalResult.Provider
		if strings.TrimSpace(provider) == "" {
			provider = imodels.DetectProvider(model)
		}
		recCtx := wopts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       opts.UserID,
			SessionID:    sessionID,
			TaskID:       wid,
			AgentID:      "react-synthesizer",
			Model:        model,
			Provider:     provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata:     map[string]any{"phase": "react_synth"},
		}).Get(recCtx, nil)
		wopts.RecordToolCostEntries(ctx, finalResult, opts.UserID, sessionID, wid)
	}

	return &ReactLoopResult{
		Thoughts:     thoughts,
		Actions:      actions,
		Observations: observations,
		FinalResult:  finalResult.Response,
		TotalTokens:  totalTokens,
		Iterations:   iteration,
		AgentResults: agentResults,
	}, nil
}

// Helper functions
// 辅助函数

// truncateBase64Images removes only base64 image data from observations/actions
// to prevent context overflow. Screenshots are already emitted to UI via TOOL_OBSERVATION events.
// This is safe for all workflows - only removes binary image data, not text content.
// 只移除观测/动作中的 base64 图片数据以防 context 溢出；截图已通过 TOOL_OBSERVATION event 发给 UI。
// 对所有 workflow 都安全：只删二进制图像数据，不动文本内容。
func truncateBase64Images(content string) string {
	// Check for base64 image data patterns (screenshots, images)
	// Common patterns: "screenshot": "iVBOR...", "image": "data:image/png;base64,..."
	// Only targets actual base64 data, not text descriptions
	// 检查 base64 图片数据特征（截图、图片）
	// 常见形式："screenshot": "iVBOR..."、"image": "data:image/png;base64,..."
	// 只针对真正的 base64 数据，不动文本描述
	base64Patterns := []string{
		`"screenshot": "`,
		`"screenshot":"`,
	}

	result := content
	for _, pattern := range base64Patterns {
		for {
			idx := strings.Index(result, pattern)
			if idx == -1 {
				break
			}
			// Find the start of the base64 data
			// 定位 base64 数据的起始位置
			startIdx := idx + len(pattern)
			// Find the end (closing quote)
			// 定位结束位置（右引号）
			endIdx := strings.Index(result[startIdx:], `"`)
			if endIdx > 500 { // Only truncate if base64 is large (>500 chars = likely actual image) 注：仅当 base64 很大时才截断（>500 字符基本就是真图片）
				// Replace with placeholder, keeping JSON structure valid
				// 用占位符替换，保持 JSON 结构合法
				truncated := result[:startIdx] + "[SCREENSHOT_STORED_SEPARATELY]" + result[startIdx+endIdx:]
				result = truncated
			} else {
				break // Not a base64 image, stop processing this pattern 注：不是 base64 图片，停止处理该 pattern
			}
		}
	}

	return result
}

func getRecentObservations(observations []string, window int) []string {
	if len(observations) <= window {
		return observations
	}
	return observations[len(observations)-window:]
}

func hasSuccessfulToolExecution(exec []activities.ToolExecution) bool {
	for _, te := range exec {
		if te.Success {
			return true
		}
	}
	return false
}

func isTaskComplete(reasoning string) bool {
	// Simple heuristic - in production would use structured output or LLM
	// 简单启发式——生产环境应改用结构化输出或 LLM
	lowerReasoning := strings.ToLower(reasoning)
	completionPhrases := []string{
		"task complete",
		"problem solved",
		"found the answer",
		"successfully completed",
		"objective achieved",
		"goal reached",
		"finished",
		"done",
	}

	for _, phrase := range completionPhrases {
		if strings.Contains(lowerReasoning, phrase) {
			return true
		}
	}
	return false
}

// Removed extractToolsFromReasoning - all tool selection is now LLM-driven
// to maintain consistency with the LLM-native architecture
// 已移除 extractToolsFromReasoning：tool 选择全部交给 LLM，以保持 LLM 原生架构的一致性。

// shouldStopReactLoop checks multiple criteria for early termination
// 提前终止判据：① 找到高置信解法；② 最近两轮结果相似（收敛）；③ 最近两轮没有新增引用/信息。
func shouldStopReactLoop(observations []string, thoughts []string, agentResults []activities.AgentExecutionResult, iteration int) bool {
	// Need at least 2 iterations to compare
	// 至少需要 2 轮迭代才能比较
	if iteration < 2 {
		return false
	}

	// Criterion 1: High confidence solution found
	// 判据 1：找到高置信解
	if hasHighConfidenceSolution(observations, thoughts) {
		return true
	}

	// Criterion 2: Last two iterations returned similar results (convergence)
	// 判据 2：最近两轮结果相似（收敛）
	var lastObs, prevObs string
	if len(observations) >= 2 {
		lastObs = observations[len(observations)-1]
		prevObs = observations[len(observations)-2]
		if areSimilar(lastObs, prevObs) {
			return true
		}
	}

	// Criterion 3: No new citations/information in last 2 iterations
	// 判据 3：最近两轮没有新增引用/信息
	if len(agentResults) >= 2 && len(observations) >= 2 {
		lastCitationCount := countCitations(agentResults[len(agentResults)-1])
		prevCitationCount := countCitations(agentResults[len(agentResults)-2])

		// If both iterations have citations and counts are the same, likely no new info
		// 两轮都有引用且数量相同时，很可能没有新增信息
		if lastCitationCount > 0 && prevCitationCount > 0 && lastCitationCount == prevCitationCount {
			// Also check if observation length is similar (not significantly more content)
			// 再检查观测长度是否相近（内容没有明显增多）
			if len(lastObs) > 0 && len(prevObs) > 0 {
				lengthRatio := float64(len(lastObs)) / float64(len(prevObs))
				if lengthRatio > 0.8 && lengthRatio < 1.2 {
					return true
				}
			}
		}
	}

	return false
}

func hasHighConfidenceSolution(observations []string, thoughts []string) bool {
	// Check if we have strong indicators of a solution
	// 检查是否存在明确的解法迹象
	if len(observations) == 0 || len(thoughts) == 0 {
		return false
	}

	// Look for success indicators in recent observations
	// 在最近的观测中寻找成功迹象
	recentObs := getRecentObservations(observations, 3)
	for _, obs := range recentObs {
		lowerObs := strings.ToLower(obs)
		if strings.Contains(lowerObs, "success") ||
			strings.Contains(lowerObs, "correct") ||
			strings.Contains(lowerObs, "solved") ||
			strings.Contains(lowerObs, "answer is") ||
			strings.Contains(lowerObs, "found") ||
			strings.Contains(lowerObs, "comprehensive") {
			return true
		}
	}

	return false
}

// areSimilar checks if two strings are similar (simple overlap check)
// areSimilar 判断两个字符串是否相似（简单重叠检查）
func areSimilar(s1, s2 string) bool {
	if s1 == s2 {
		return true
	}

	// Check length similarity first
	// 先检查长度是否相近
	len1, len2 := len(s1), len(s2)
	if len1 == 0 || len2 == 0 {
		return false
	}

	lengthRatio := float64(len1) / float64(len2)
	if lengthRatio < 0.5 || lengthRatio > 2.0 {
		return false
	}

	// Check for high word overlap
	// 检查词的重复度是否够高
	words1 := strings.Fields(strings.ToLower(s1))
	words2 := strings.Fields(strings.ToLower(s2))

	if len(words1) == 0 || len(words2) == 0 {
		return false
	}

	wordSet := make(map[string]bool)
	for _, w := range words1 {
		if len(w) > 3 { // Only consider words longer than 3 chars 注：只统计长度超过 3 的单词
			wordSet[w] = true
		}
	}

	overlap := 0
	for _, w := range words2 {
		if len(w) > 3 && wordSet[w] {
			overlap++
		}
	}

	// If >70% of significant words overlap, consider similar
	// 有效词重叠超过 70% 即视为相似
	overlapRatio := float64(overlap) / float64(len(words2))
	return overlapRatio > 0.7
}

// countCitations counts how many citations are in an agent result
// countCitations 统计 agent 结果中包含多少个引用
func countCitations(result activities.AgentExecutionResult) int {
	count := 0
	for _, toolExec := range result.ToolExecutions {
		if toolExec.Tool == "web_search" && toolExec.Success {
			// Rough estimate: count URLs in output
			// 粗略估计：统计输出中的 URL 数量
			if output, ok := toolExec.Output.(string); ok {
				count += strings.Count(output, "http")
			}
		}
	}
	return count
}
