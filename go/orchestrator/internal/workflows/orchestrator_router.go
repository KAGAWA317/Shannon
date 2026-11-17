package workflows

import (
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	ometrics "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/roles"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/templates"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/strategies"
)

const (
	// DefaultReviewTimeout is how long the workflow waits for human review before timing out.
	// DefaultReviewTimeout 指定 workflow 等待人工审核的时长，超时即放弃。
	DefaultReviewTimeout         = 15 * time.Minute
	ResearchChildWorkflowTimeout = 45 * time.Minute
)

// OrchestratorWorkflow is a thin entrypoint that routes to specialized workflows.
// It performs a single decomposition step, decides the strategy, then delegates
// to an appropriate child workflow. It does not execute agents directly.
// 这是薄入口：只做一次分解、决定策略，然后委派给相应的子 workflow，本身不直接执行 agent。
func OrchestratorWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID

	logger.Info("Starting OrchestratorWorkflow",
		"query", input.Query,
		"user_id", input.UserID,
		"session_id", input.SessionID,
	)

	// Emit workflow started event with task context
	// 携带 task context 发送 workflow 已开始事件
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Initialize control signal handler for pause/resume/cancel
	// 初始化用于 pause/resume/cancel 的 control 信号处理器
	controlHandler := &ControlSignalHandler{
		WorkflowID: workflowID,
		AgentID:    "orchestrator",
		Logger:     logger,
		EmitCtx:    emitCtx,
	}
	controlHandler.Setup(ctx)

	if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowStarted,
		AgentID:    "orchestrator",
		Message:    activities.MsgWorkflowStarted(),
		Timestamp:  workflow.Now(ctx),
		Payload: map[string]any{
			"task_context": input.Context, // Include context for frontend 注：把 context 带给前端
		},
	}).Get(ctx, nil); err != nil {
		logger.Warn("Failed to emit workflow started event", "error", err)
	}

	// Start async title generation only if session doesn't have a title yet (non-blocking)
	// Title is generated from query, doesn't need task result, so start early for better UX
	// v2: Check SessionCtx for existing title - more reliable than history length
	// 仅在 session 尚无标题时启动异步标题生成（非阻塞）
	// 标题由 query 生成、不依赖 task 结果，因此尽早启动以改善体验
	// v2：改为检查 SessionCtx 中是否已有标题，比按 history 长度判断更可靠
	titleGateVersion := workflow.GetVersion(ctx, "title_gate_v2", workflow.DefaultVersion, 1)
	needsTitle := true
	if titleGateVersion >= 1 {
		// New behavior: check SessionCtx for existing title
		// 新行为：检查 SessionCtx 中是否已有标题
		if input.SessionCtx != nil {
			if existingTitle, ok := input.SessionCtx["title"].(string); ok && existingTitle != "" {
				needsTitle = false
			}
		}
	} else {
		// Old behavior: check history length (kept for replay compatibility)
		// 旧行为：按 history 长度判断（为兼容重放而保留）
		if len(input.History) > 0 {
			needsTitle = false
		}
	}
	if needsTitle {
		startAsyncTitleGeneration(ctx, input.SessionID, input.Query)
	}

	// Conservative activity options for fast planning
	// 快速规划用的保守 activity 选项
	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 60 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	})

	// (Optional) Load router/approval config
	// （可选）加载 router/approval 配置
	var cfg activities.WorkflowConfig
	if err := workflow.ExecuteActivity(actx, activities.GetWorkflowConfig).Get(ctx, &cfg); err != nil {
		// Continue with defaults on failure
		// 失败时继续使用默认值
	}
	simpleThreshold := cfg.SimpleThreshold
	if simpleThreshold == 0 {
		simpleThreshold = 0.3
	}

	templateVersionGate := workflow.GetVersion(ctx, "template_router_v1", workflow.DefaultVersion, 1)
	var templateEntry templates.Entry
	templateFound := false
	templateRequested := false
	var requestedTemplateName, requestedTemplateVersion string
	if templateVersionGate >= 1 {
		requestedTemplateName, requestedTemplateVersion = extractTemplateRequest(input)
		if requestedTemplateName != "" {
			templateRequested = true
			if entry, ok := TemplateRegistry().Find(requestedTemplateName, requestedTemplateVersion); ok {
				templateEntry = entry
				templateFound = true
				if input.Context == nil {
					input.Context = map[string]any{}
				}
				input.Context["template_resolved"] = entry.Key
				input.Context["template_content_hash"] = entry.ContentHash
			}
		}
		if input.DisableAI && !templateFound {
			msg := fmt.Sprintf("requested template '%s' not found", requestedTemplateName)
			if requestedTemplateName == "" {
				msg = "template execution required but no template specified"
			}
			logger.Error("Template requirement cannot be satisfied",
				"template", requestedTemplateName,
				"version", requestedTemplateVersion,
			)
			return TaskResult{
				Success:      false,
				ErrorMessage: msg,
				Metadata: map[string]any{
					"template_requested": requestedTemplateName,
					"template_version":   requestedTemplateVersion,
				},
			}, nil
		}
		if templateRequested && !templateFound {
			logger.Warn("Requested template not found; continuing with heuristic routing",
				"template", requestedTemplateName,
				"version", requestedTemplateVersion,
			)
		}
	}

	learningVersionGate := workflow.GetVersion(ctx, "learning_router_v1", workflow.DefaultVersion, 1)
	if learningVersionGate >= 1 && !templateFound && cfg.ContinuousLearningEnabled {
		if rec, err := recommendStrategy(ctx, input); err == nil && rec != nil && rec.Strategy != "" {
			if input.Context == nil {
				input.Context = map[string]any{}
			}
			input.Context["learning_strategy"] = rec.Strategy
			input.Context["learning_confidence"] = rec.Confidence
			if rec.Source != "" {
				input.Context["learning_source"] = rec.Source
			}
			if result, handled, err := routeStrategyWorkflow(ctx, input, rec.Strategy, "learning", emitCtx, controlHandler); handled {
				return result, err
			}
			logger.Warn("Learning router returned unknown strategy", "strategy", rec.Strategy)
		}
	}

	// Check pause/cancel before routing to child workflow
	// 路由到子 workflow 之前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_routing"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// 1) Decompose the task (planning + complexity)
	// 1) 分解 task（规划 + 复杂度）
	if templateFound {
		input.TemplateName = templateEntry.Template.Name
		input.TemplateVersion = templateEntry.Template.Version

		templateInput := TemplateWorkflowInput{
			Task:         input,
			TemplateKey:  templateEntry.Key,
			TemplateHash: templateEntry.ContentHash,
		}

		ometrics.WorkflowsStarted.WithLabelValues("TemplateWorkflow", "template").Inc()
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgHandoffTemplate(templateEntry.Template.Name),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)

		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		var result TaskResult
		templateFuture := workflow.ExecuteChildWorkflow(childCtx, TemplateWorkflow, templateInput)
		var templateExec workflow.Execution
		if err := templateFuture.GetChildWorkflowExecution().Get(childCtx, &templateExec); err != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, err
		}
		controlHandler.RegisterChildWorkflow(templateExec.ID)
		if err := templateFuture.Get(childCtx, &result); err != nil {
			controlHandler.UnregisterChildWorkflow(templateExec.ID)
			if cfg.TemplateFallbackEnabled {
				logger.Warn("Template workflow failed; falling back to AI decomposition", "error", err)
				ometrics.TemplateFallbackTriggered.WithLabelValues("error").Inc()
				ometrics.TemplateFallbackSuccess.WithLabelValues("error").Inc()
				// Allow router to proceed to decomposition path below
				// 允许 router 继续走下方的分解路径
				templateFound = false
			} else {
				result = AddTaskContextToMetadata(result, input.Context)
				return result, err
			}
		} else if !result.Success {
			controlHandler.UnregisterChildWorkflow(templateExec.ID)
			if cfg.TemplateFallbackEnabled {
				logger.Warn("Template workflow returned unsuccessful result; falling back to AI decomposition")
				ometrics.TemplateFallbackTriggered.WithLabelValues("unsuccessful").Inc()
				ometrics.TemplateFallbackSuccess.WithLabelValues("unsuccessful").Inc()
				templateFound = false
			} else {
				scheduleStreamEnd(ctx)
				result = AddTaskContextToMetadata(result, input.Context)
				return result, nil
			}
		} else {
			controlHandler.UnregisterChildWorkflow(templateExec.ID)
			scheduleStreamEnd(ctx)
			result = AddTaskContextToMetadata(result, input.Context)
			return result, nil
		}
	}

	// Early route: skip_synthesis forces SimpleTaskWorkflow, bypassing decomposition.
	// Used by Sagasu NL workflow setup and other flows that produce structured JSON
	// and must not be rewritten by a synthesis LLM call.
	// skip_synthesis 强制走 SimpleTaskWorkflow 并跳过分解，
	// 用于 Sagasu NL 等已产出结构化 JSON、不能被合成 LLM 改写的流程。
	skipSynthesisVersion := workflow.GetVersion(ctx, "skip_synthesis_early_route_v1", workflow.DefaultVersion, 1)
	if skipSynthesisVersion >= 1 && GetContextBool(input.Context, "skip_synthesis") {
		logger.Info("Early route: skip_synthesis forces SimpleTaskWorkflow")

		wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
		input.ParentWorkflowID = wfID

		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: wfID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgHandoffSimple(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)

		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		var result TaskResult
		childFuture := workflow.ExecuteChildWorkflow(childCtx, SimpleTaskWorkflow, input)
		if err := childFuture.Get(ctx, &result); err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}
		scheduleStreamEnd(ctx)
		result = AddTaskContextToMetadata(result, input.Context)
		return result, nil
	}

	// Early route: Force SwarmWorkflow bypasses decomposition entirely
	// SwarmWorkflow has its own Lead Agent that does initial planning, so orchestrator-level
	// decomposition is redundant and wastes LLM tokens.
	// SwarmWorkflow 有自己的 Lead Agent 做初始规划，再做一层 orchestrator 分解既冗余又浪费 token。
	forceSwarmVersion := workflow.GetVersion(ctx, "force_swarm_early_route_v1", workflow.DefaultVersion, 1)
	if forceSwarmVersion >= 1 && GetContextBool(input.Context, "force_swarm") {
		logger.Info("Force swarm detected - bypassing orchestrator decomposition, Lead Agent will plan")
		swarmWfID := workflow.GetInfo(ctx).WorkflowExecution.ID
		input.ParentWorkflowID = swarmWfID

		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		var result TaskResult
		ometrics.WorkflowsStarted.WithLabelValues("SwarmWorkflow", "swarm").Inc()
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: swarmWfID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgSwarmStarted(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
		childFuture := workflow.ExecuteChildWorkflow(childCtx, SwarmWorkflow, input)
		var childExec workflow.Execution
		if err := childFuture.GetChildWorkflowExecution().Get(childCtx, &childExec); err != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to start SwarmWorkflow: %v", err)}, err
		}
		controlHandler.RegisterChildWorkflow(childExec.ID)

		// HITL: forward human-input signals from parent to SwarmWorkflow child.
		// Uses blocking Receive — goroutine sleeps until a signal arrives (zero polling).
		// Goroutine exits naturally when the workflow context is cancelled (child completes).
		// 用阻塞式 Receive 转发人类输入信号，无轮询；workflow context 取消（子 workflow 结束）时 goroutine 自然退出。
		humanInputCh := workflow.GetSignalChannel(ctx, "human-input")
		workflow.Go(ctx, func(gCtx workflow.Context) {
			for {
				var humanMsg map[string]string
				humanInputCh.Receive(gCtx, &humanMsg)
				_ = workflow.SignalExternalWorkflow(gCtx, childExec.ID, "", "human-input", humanMsg).Get(gCtx, nil)
			}
		})

		execErr := childFuture.Get(childCtx, &result)
		controlHandler.UnregisterChildWorkflow(childExec.ID)
		if execErr != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("SwarmWorkflow failed: %v", execErr)}, execErr
		}
		scheduleStreamEnd(ctx)
		result = AddTaskContextToMetadata(result, input.Context)
		return result, nil
	}

	// Early route: Force ResearchWorkflow bypasses decomposition entirely
	// ResearchWorkflow has its own query refinement + decompose pipeline, so orchestrator-level
	// decomposition is redundant and wastes LLM tokens.
	// ResearchWorkflow 自带查询精炼 + 分解流水线，再做 orchestrator 分解既冗余又浪费 token。
	if GetContextBool(input.Context, "force_research") {
		logger.Info("Force research detected - bypassing orchestrator decomposition")

		// Inject current date for time awareness (use workflow.Now for Temporal determinism).
		// Version-gated to preserve replay for workflows started before this behavior existed.
		// 用 workflow.Now 保证 Temporal 确定性；加版本闸门，使该行为出现之前启动的 workflow 仍可重放。
		forceResearchDateVersion := workflow.GetVersion(ctx, "force_research_current_date_v1", workflow.DefaultVersion, 1)
		if forceResearchDateVersion >= 1 {
			if _, hasDate := input.Context["current_date"]; !hasDate {
				workflowTime := workflow.Now(ctx)
				input.Context["current_date"] = workflowTime.UTC().Format("2006-01-02")
				input.Context["current_date_human"] = workflowTime.UTC().Format("January 2, 2006")
			}
		}

		// ── HITL: Research Plan Review (optional) ──
		// Check both "require_review: true" (legacy) and "review_plan: manual" (frontend)
		// 同时兼容 "require_review: true"（旧版）与 "review_plan: manual"（前端）两种开关。
		requireReview := GetContextBool(input.Context, "require_review") ||
			GetContextString(input.Context, "review_plan") == "manual"
		if requireReview {
			logger.Info("HITL review enabled - generating research plan")

			reviewTimeout := DefaultReviewTimeout
			if t, ok := input.Context["review_timeout"]; ok {
				var seconds int
				switch v := t.(type) {
				case float64:
					seconds = int(v)
				case int:
					seconds = v
				case int32:
					seconds = int(v)
				case int64:
					seconds = int(v)
				case string:
					if parsed, err := strconv.Atoi(strings.TrimSpace(v)); err == nil {
						seconds = parsed
					}
				}
				if seconds > 0 {
					reviewTimeout = time.Duration(seconds) * time.Second
				}
			}
			// Safety clamp: keep review wait bounded (docs specify max 15 minutes).
			// 安全兜底：限制审核等待时长上限（文档规定最多 15 分钟）。
			if reviewTimeout > DefaultReviewTimeout {
				reviewTimeout = DefaultReviewTimeout
			}

			// 1) Generate initial research plan via LLM + initialize Redis state
			// 1) 通过 LLM 生成初始 research plan，并初始化 Redis 状态
			var plan activities.ResearchPlanResult
			planInput := activities.ResearchPlanInput{
				Query:      input.Query,
				Context:    input.Context,
				WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				SessionID:  input.SessionID,
				UserID:     input.UserID,
				TenantID:   input.TenantID,
				TTL:        reviewTimeout,
			}
			planCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
				StartToCloseTimeout: 60 * time.Second,
				RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
			})
			if err := workflow.ExecuteActivity(planCtx, constants.GenerateResearchPlanActivity, planInput).Get(ctx, &plan); err != nil {
				return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to generate research plan: %v", err)}, err
			}

			// 2) Emit SSE: plan ready (message already stripped of [RESEARCH_BRIEF] and [INTENT:...])
			// 2) 发送 SSE：plan ready（message 已剥离 [RESEARCH_BRIEF] 与 [INTENT:...]）
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				EventType:  activities.StreamEventResearchPlanReady,
				AgentID:    "planner",
				Message:    plan.Message,
				Timestamp:  workflow.Now(ctx),
				Payload:    map[string]any{"round": plan.Round, "intent": plan.Intent},
			}).Get(ctx, nil)

			// 3) Wait for user approval Signal or timeout
			// 3) 等待用户批准的 Signal 或超时
			sigName := "research-plan-approved-" + workflow.GetInfo(ctx).WorkflowExecution.ID
			ch := workflow.GetSignalChannel(ctx, sigName)
			timerCtx, cancelTimer := workflow.WithCancel(ctx)
			timer := workflow.NewTimer(timerCtx, reviewTimeout)

			var reviewResult activities.ResearchReviewResult
			var timedOut bool

			sel := workflow.NewSelector(ctx)
			sel.AddReceive(ch, func(c workflow.ReceiveChannel, more bool) {
				c.Receive(ctx, &reviewResult)
				cancelTimer() // Cancel the timer so it doesn't linger in Temporal UI 注：取消 timer，避免它一直留在 Temporal UI 上
			})
			sel.AddFuture(timer, func(f workflow.Future) {
				timedOut = true
			})
			sel.Select(ctx)

			if timedOut {
				wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
				logger.Warn("HITL review timed out", "workflow_id", wfID)
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: wfID,
					EventType:  activities.StreamEventErrorOccurred,
					AgentID:    "planner",
					Message:    activities.MsgResearchTimedOut(),
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
				// Emit WORKFLOW_COMPLETED so frontend/SSE can transition out of "running"
				// 发送 WORKFLOW_COMPLETED，使前端/SSE 能从 "running" 状态切换出去
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: wfID,
					EventType:  activities.StreamEventWorkflowCompleted,
					AgentID:    "orchestrator",
					Message:    activities.MsgWorkflowCompleted(),
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
				return TaskResult{Success: false, ErrorMessage: "research plan review timed out"}, nil
			}

			// 4) Inject confirmed plan and research brief into context
			// 4) 把确认后的 plan 与 research brief 注入 context
			input.Context["confirmed_plan"] = reviewResult.FinalPlan
			input.Context["review_conversation"] = reviewResult.Conversation
			if reviewResult.ResearchBrief != "" {
				input.Context["research_brief"] = reviewResult.ResearchBrief
			}

			// 5) Emit SSE: plan approved
			// 5) 发送 SSE：plan approved
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				EventType:  activities.StreamEventResearchPlanApproved,
				AgentID:    "planner",
				Message:    activities.MsgResearchConfirmed(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)

			logger.Info("HITL review approved, continuing with research",
				"conversation_rounds", len(reviewResult.Conversation),
			)
		}
		// ── End HITL review ──

		// Force research still needs token budget preflight so downstream
		// pattern calls use budgeted execution paths and record tool costs.
		// Version-gated: in-flight workflows started before this code must not
		// encounter the new BudgetPreflight activity during replay.
		// 强制研究路径仍需 budget 预检，使下游 pattern 调用走 budget 执行路径并记录 tool 成本；
		// 版本闸门：新代码上线前启动的 workflow 重放时不得遇到新增的 BudgetPreflight activity。
		forceResearchBudgetVersion := workflow.GetVersion(ctx, "force_research_budget_v1", workflow.DefaultVersion, 1)
		if forceResearchBudgetVersion >= 1 && input.UserID != "" {
			est := EstimateTokensWithConfig(activities.DecompositionResult{
				ComplexityScore: 0.5,
				Subtasks:        []activities.Subtask{{ID: "force_research-1"}},
			}, &cfg)
			if res, err := BudgetPreflight(ctx, input, est); err == nil && res != nil {
				if !res.CanProceed {
					scheduleStreamEnd(ctx)
					out := TaskResult{Success: false, ErrorMessage: res.Reason, Metadata: map[string]any{"budget_blocked": true}}
					out = AddTaskContextToMetadata(out, input.Context)
					return out, nil
				}
				if input.Context == nil {
					input.Context = map[string]any{}
				}
				input.Context["budget_remaining"] = res.RemainingTaskBudget
				// ResearchWorkflow distributes budget internally; pass full remaining.
				// ResearchWorkflow 内部自行分配 budget，这里传入全部剩余额度。
				agentMax := res.RemainingTaskBudget
				if v := os.Getenv("TOKEN_BUDGET_PER_AGENT"); v != "" {
					if n, err := strconv.Atoi(v); err == nil && n > 0 && n < agentMax {
						agentMax = n
					}
				}
				if capv, ok := input.Context["token_budget_per_agent"].(int); ok && capv > 0 && capv < agentMax {
					agentMax = capv
				}
				if capv, ok := input.Context["token_budget_per_agent"].(float64); ok && capv > 0 && int(capv) < agentMax {
					agentMax = int(capv)
				}
				// Cap at remaining quota for free-tier users
				// 对免费额度用户按剩余配额封顶
				agentMax = CapBudgetAtQuotaRemaining(ctx, input.TenantID, agentMax)
				input.Context["budget_agent_max"] = agentMax
			}
		}

		// Set parent workflow ID for unified event streaming (must be set before routing)
		// 设置 parent workflow ID 以便统一事件流（必须在路由之前设置）
		parentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
		input.ParentWorkflowID = parentWorkflowID

		// Emit delegation event
		// 发送 delegation 事件
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: parentWorkflowID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgWorkflowRouting(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)

		ometrics.WorkflowsStarted.WithLabelValues("ResearchWorkflow", "force_research").Inc()

		strategiesInput := convertToStrategiesInput(input)
		var strategiesResult strategies.TaskResult
		childOpts := workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		}
		v := workflow.GetVersion(ctx, "research_child_timeout_v1", workflow.DefaultVersion, 1)
		if v >= 1 {
			childOpts.WorkflowExecutionTimeout = ResearchChildWorkflowTimeout
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		researchFuture := workflow.ExecuteChildWorkflow(childCtx, strategies.ResearchWorkflow, strategiesInput)
		var researchExec workflow.Execution
		if err := researchFuture.GetChildWorkflowExecution().Get(childCtx, &researchExec); err != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, err
		}
		controlHandler.RegisterChildWorkflow(researchExec.ID)
		execErr := researchFuture.Get(childCtx, &strategiesResult)
		controlHandler.UnregisterChildWorkflow(researchExec.ID)

		scheduleStreamEnd(ctx)

		if execErr != nil {
			controlHandler.EmitCancelledIfNeeded(ctx, execErr.Error())
			// Preserve child's failure metadata (e.g. partial_result, phase, token counts)
			// 保留子 workflow 的失败元数据（如 partial_result、phase、token 计数）
			result := convertFromStrategiesResult(strategiesResult)
			result = AddTaskContextToMetadata(result, input.Context)
			return result, execErr
		}
		result := convertFromStrategiesResult(strategiesResult)
		result = AddTaskContextToMetadata(result, input.Context)
		return result, nil
	}

	// Early route: research_strategy context values.
	// Generic research depth hints (quick/standard/deep/academic) are
	// handled by the existing force_research + post-decomposition paths.
	// Enterprise strategies removed in OSS
	// are not available in the OSS version.
	// 早路由：research_strategy 的 context 取值。
	// 通用 research 深度提示（quick/standard/deep/academic）由既有的
	// force_research 与分解后路径处理。
	// 企业版策略在 OSS 中已被移除，
	// 在 OSS 版本中不提供。
	earlyStrategyRouteVersion := workflow.GetVersion(ctx, "research_strategy_early_route_v1", workflow.DefaultVersion, 1)
	_ = earlyStrategyRouteVersion // Preserved for Temporal replay compatibility 注：为兼容 Temporal 重放而保留

	// 1) Decompose the task (planning + complexity)
	// Add history to context for decomposition to be context-aware
	// 1) 分解 task（规划 + 复杂度）
	// 把 history 加入 context，使分解具备上下文感知
	decompContext := make(map[string]any)
	if input.Context != nil {
		maps.Copy(decompContext, input.Context)
	}
	// Inject current date for time awareness (use workflow.Now for Temporal determinism)
	// Only inject if not already provided (allow user override)
	// 注入当前日期以提供时间感知（用 workflow.Now 保证 Temporal 确定性）
	// 仅在尚未提供时才注入（允许用户覆盖）
	if _, hasDate := decompContext["current_date"]; !hasDate {
		workflowTime := workflow.Now(ctx)
		decompContext["current_date"] = workflowTime.UTC().Format("2006-01-02")
		decompContext["current_date_human"] = workflowTime.UTC().Format("January 2, 2006")
	}
	// Inject P2P flag so decomposition prompt can include produces/consumes fields
	// 注入 P2P 开关，使分解 prompt 能带上 produces/consumes 字段
	if cfg.P2PCoordinationEnabled {
		decompContext["p2p_enabled"] = true
	}
	// Add history for context awareness in decomposition
	// 为分解的上下文感知加入 history
	if len(input.History) > 0 {
		// Convert history to a single string for the decompose endpoint
		// 把 history 拼成单个字符串供 decompose 接口使用
		historyLines := convertHistoryForAgent(input.History)
		decompContext["history"] = strings.Join(historyLines, "\n")
	}

	var decomp activities.DecompositionResult

	// If a single-purpose agent is specified, bypass LLM decomposition entirely.
	// AgentWorkflow is already fully deterministic and self-contained.
	// 若指定了单一用途的 agent，则完全跳过 LLM 分解。
	// AgentWorkflow 本身已完全确定且自包含。
	agentPresent := false
	if input.Context != nil {
		if agentID, ok := input.Context["agent"].(string); ok && agentID != "" {
			agentPresent = true
			logger.Info("Agent specified - bypassing LLM decomposition", "agent_id", agentID)

			// Pass through suggested_tools from API context (e.g., Sagasu sends ["web_search"]
			// for setup conversations so the agent can resolve company names to URLs).
			// 透传 API context 中的 suggested_tools（例如 Sagasu 会传 ["web_search"]
			// 用于 setup 会话，让 agent 能把公司名解析成 URL）。
			var agentTools []string
			if toolsVal, ok := input.Context["suggested_tools"]; ok {
				if arr, ok := toolsVal.([]any); ok {
					for _, v := range arr {
						if s, ok := v.(string); ok {
							agentTools = append(agentTools, s)
						}
					}
				}
			}

			decomp = activities.DecompositionResult{
				Mode:              "simple",
				ComplexityScore:   0.0,
				ExecutionStrategy: "sequential",
				ConcurrencyLimit:  1,
				Subtasks: []activities.Subtask{
					{
						ID:              "task-1",
						Description:     fmt.Sprintf("Execute agent %s", agentID),
						Dependencies:    []string{},
						EstimatedTokens: 0,
						SuggestedTools:  agentTools,
						ToolParameters:  map[string]any{},
					},
				},
				TotalEstimatedTokens: 0,
				TokensUsed:           0,
				InputTokens:          0,
				OutputTokens:         0,
			}
		}
	}

	// Check if a role is specified - if so, bypass LLM decomposition and create simple plan.
	// Role-specific agents have their own internal multi-step logic, so orchestrator-level
	// decomposition is unnecessary and can conflict with role-specific output contracts.
	// 检查是否指定了 role：若有则跳过 LLM 分解，直接生成简单的 plan。
	// role 专属 agent 自带内部多步逻辑，因此 orchestrator 这一层的
	// 分解是多余的，还可能与 role 专属的输出契约冲突。
	rolePresent := false
	if !agentPresent && input.Context != nil {
		if role, ok := input.Context["role"].(string); ok && role != "" {
			rolePresent = true
			roleTools := roles.AllowedTools(role)
			logger.Info("Role specified - bypassing LLM decomposition", "role", role, "tool_count", len(roleTools))

			// Emit ROLE_ASSIGNED event
			// 发送 ROLE_ASSIGNED 事件
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				EventType:  activities.StreamEventRoleAssigned,
				AgentID:    role,
				Message:    activities.MsgRoleAssigned(role),
				Timestamp:  workflow.Now(ctx),
				Payload: map[string]any{
					"role":       role,
					"tools":      roleTools,
					"tool_count": len(roleTools),
				},
			}).Get(ctx, nil)

			// Create a simple single-subtask plan
			// 生成只含单个 subtask 的简单 plan
			decomp = activities.DecompositionResult{
				Mode:              "simple",
				ComplexityScore:   0.5,
				ExecutionStrategy: "sequential",
				ConcurrencyLimit:  1,
				Subtasks: []activities.Subtask{
					{
						ID:              "task-1",
						Description:     input.Query,
						Dependencies:    []string{},
						EstimatedTokens: 5000,
						SuggestedTools:  append([]string(nil), roleTools...),
						ToolParameters:  map[string]any{}, // Agent constructs from context 注：由 agent 从 context 构造
					},
				},
				TotalEstimatedTokens: 5000,
				TokensUsed:           0, // No LLM call for decomposition 注：分解不调用 LLM
				InputTokens:          0,
				OutputTokens:         0,
			}

			// Check pause/cancel after role assignment - signal may have arrived during setup
			// role 分配后检查 pause/cancel —— 信号可能在 setup 期间就已到达
			if err := controlHandler.CheckPausePoint(ctx, "post_role_assignment"); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error()}, err
			}
		}
	}

	// If no role, proceed with normal LLM decomposition
	// 若无 role，则走正常的 LLM 分解
	if !rolePresent && !agentPresent {
		// Emit "Understanding your request" before decomposition
		// 在分解前发送 "Understanding your request"
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			EventType:  activities.StreamEventProgress,
			AgentID:    "planner",
			Message:    activities.MsgUnderstandingRequest(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)

		if err := workflow.ExecuteActivity(actx, constants.DecomposeTaskActivity, activities.DecompositionInput{
			Query:          input.Query,
			Context:        decompContext,
			AvailableTools: nil, // Let llm-service derive tools from registry + role preset 注：由 llm-service 根据 registry 与 role 预设推导 tool
		}).Get(ctx, &decomp); err != nil {
			logger.Warn("Task decomposition failed, falling back to SimpleTaskWorkflow", "error", err)
			// Emit warning event
			// 发送 warning 事件
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
				EventType:  activities.StreamEventProgress,
				AgentID:    "planner",
				Message:    activities.MsgDecompositionFailed(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)

			// Create fallback decomposition for SimpleTaskWorkflow
			// 为 SimpleTaskWorkflow 构造兜底分解结果
			decomp = activities.DecompositionResult{
				Mode:              "simple",
				ComplexityScore:   0.1, // Low complexity to trigger SimpleTaskWorkflow 注：低复杂度以触发 SimpleTaskWorkflow
				ExecutionStrategy: "sequential",
				CognitiveStrategy: "",
				Subtasks: []activities.Subtask{
					{
						ID:           "1",
						Description:  input.Query,
						TaskType:     "generic",
						Dependencies: []string{},
					},
				},
				TotalEstimatedTokens: 5000,
				TokensUsed:           0, // No LLM call for fallback decomposition 注：兜底分解不调用 LLM
				InputTokens:          0,
				OutputTokens:         0,
			}
			logger.Info("Created fallback decomposition for simple execution", "query", input.Query)
		}

		// Check pause/cancel after decomposition - signal may have arrived during the activity
		// 分解后检查 pause/cancel —— 信号可能在 activity 执行期间到达
		if err := controlHandler.CheckPausePoint(ctx, "post_decomposition"); err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}
	}

	// Record decomposition usage if provided
	// 若分解结果带用量信息则记录之
	if decomp.TokensUsed > 0 || decomp.InputTokens > 0 || decomp.OutputTokens > 0 {
		inTok := decomp.InputTokens
		outTok := decomp.OutputTokens
		if inTok == 0 && outTok == 0 && decomp.TokensUsed > 0 {
			inTok = int(float64(decomp.TokensUsed) * 0.6)
			outTok = decomp.TokensUsed - inTok
		}
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		recCtx := opts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:              input.UserID,
			SessionID:           input.SessionID,
			TaskID:              wid,
			AgentID:             "decompose",
			Model:               decomp.ModelUsed,
			Provider:            decomp.Provider,
			InputTokens:         inTok,
			OutputTokens:        outTok,
			CacheReadTokens:     decomp.CacheReadTokens,
			CacheCreationTokens: decomp.CacheCreationTokens,
			Metadata:            map[string]any{"phase": "decompose"},
		}).Get(ctx, nil)
	}

	logger.Info("Routing decision",
		"complexity", decomp.ComplexityScore,
		"mode", decomp.Mode,
		"num_subtasks", len(decomp.Subtasks),
		"cognitive_strategy", decomp.CognitiveStrategy,
	)

	// Emit a human-friendly plan summary with payload (steps + deps)
	// 发送便于人阅读的 plan 摘要，payload 含步骤与依赖
	{
		steps := make([]map[string]any, 0, len(decomp.Subtasks))
		deps := make([]map[string]string, 0, 4)
		for _, st := range decomp.Subtasks {
			steps = append(steps, map[string]any{
				"id":   st.ID,
				"name": st.Description,
				"type": st.TaskType,
			})
			for _, d := range st.Dependencies {
				deps = append(deps, map[string]string{"from": d, "to": st.ID})
			}
		}
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			EventType:  activities.StreamEventProgress,
			AgentID:    "planner",
			Message:    activities.MsgPlanCreated(len(steps)),
			Timestamp:  workflow.Now(ctx),
			Payload:    map[string]any{"plan": steps, "deps": deps},
		}).Get(ctx, nil)
	}

	// Propagate the plan to child workflows to avoid a second decompose
	// 把 plan 传给子 workflow，避免二次分解
	input.PreplannedDecomposition = &decomp

	// 1.5) Budget preflight (estimate based on plan)
	// 1.5) budget 预检（按 plan 估算）
	if input.UserID != "" { // Only check when we have a user scope 注：仅在存在 user 作用域时才检查
		est := EstimateTokensWithConfig(decomp, &cfg)
		if res, err := BudgetPreflight(ctx, input, est); err == nil && res != nil {
			if !res.CanProceed {
				// Best-effort title generation even when budget preflight blocks execution
				// 即使 budget 预检拦截执行，也尽力生成标题
				scheduleStreamEnd(ctx)
				out := TaskResult{Success: false, ErrorMessage: res.Reason, Metadata: map[string]any{"budget_blocked": true}}
				out = AddTaskContextToMetadata(out, input.Context)
				return out, nil
			}
			// Pass budget info to child workflows via context
			// 通过 context 把 budget 信息传给子 workflow
			if input.Context == nil {
				input.Context = map[string]any{}
			}
			// Propagate current_date to all child workflows (if not already set)
			// 把 current_date 传给所有子 workflow（若尚未设置）
			if _, hasDate := input.Context["current_date"]; !hasDate {
				workflowTime := workflow.Now(ctx)
				input.Context["current_date"] = workflowTime.UTC().Format("2006-01-02")
				input.Context["current_date_human"] = workflowTime.UTC().Format("January 2, 2006")
			}
			input.Context["budget_remaining"] = res.RemainingTaskBudget
			n := len(decomp.Subtasks)
			if n == 0 {
				n = 1
			}
			agentMax := res.RemainingTaskBudget / n
			// Optional clamp: environment or request context can cap per-agent budget
			// 可选封顶：环境变量或请求 context 可限制单 agent 的 budget
			if v := os.Getenv("TOKEN_BUDGET_PER_AGENT"); v != "" {
				if n, err := strconv.Atoi(v); err == nil && n > 0 && n < agentMax {
					agentMax = n
				}
			}
			if capv, ok := input.Context["token_budget_per_agent"].(int); ok && capv > 0 && capv < agentMax {
				agentMax = capv
			}
			if capv, ok := input.Context["token_budget_per_agent"].(float64); ok && capv > 0 && int(capv) < agentMax {
				agentMax = int(capv)
			}
			// Cap at remaining quota for free-tier users
			// 对免费额度用户按剩余配额封顶
			agentMax = CapBudgetAtQuotaRemaining(ctx, input.TenantID, agentMax)
			input.Context["budget_agent_max"] = agentMax
		}
	}

	// 1.6) Approval gate (optional, config-driven or explicit request)
	// 1.6) 审批闸门（可选，由配置或显式请求触发）
	if cfg.ApprovalEnabled {
		// Override policy thresholds via config if provided
		// Note: current CheckApprovalPolicy uses default thresholds; we gate invocation here
		// 若提供了配置，则用它覆盖策略阈值
		// 注意：当前 CheckApprovalPolicy 使用默认阈值，这里由我们决定是否调用
	}
	if cfg.ApprovalEnabled || input.RequireApproval {
		// Build policy from config
		// 按配置构造 policy
		pol := activities.ApprovalPolicy{
			ComplexityThreshold: cfg.ApprovalComplexityThreshold,
			TokenBudgetExceeded: false,
			RequireForTools:     cfg.ApprovalDangerousTools,
		}
		if need, reason := CheckApprovalPolicyWith(pol, input, decomp); need {
			if ar, err := RequestAndWaitApproval(ctx, input, reason); err != nil {
				// Best-effort title generation even on approval flow errors
				// 即使审批流程出错也尽力生成标题
				scheduleStreamEnd(ctx)
				out := TaskResult{Success: false, ErrorMessage: fmt.Sprintf("approval request failed: %v", err)}
				out = AddTaskContextToMetadata(out, input.Context)
				return out, err
			} else if ar == nil || !ar.Approved {
				msg := reason
				if ar != nil && ar.Feedback != "" {
					msg = ar.Feedback
				}
				// Best-effort title generation even when approval is denied
				// 即使审批被拒也尽力生成标题
				scheduleStreamEnd(ctx)
				out := TaskResult{Success: false, ErrorMessage: fmt.Sprintf("approval denied: %s", msg)}
				out = AddTaskContextToMetadata(out, input.Context)
				return out, nil
			}
		}
	}

	// 2) Routing rules (simple, cognitive, supervisor, dag)
	// Treat as simple ONLY when truly one-shot (no tools, no deps) AND below threshold
	// 仅当真正是一次性 task（无 tool、无依赖）且低于阈值时，才按 simple 处理。
	needsTools := false
	for _, st := range decomp.Subtasks {
		if len(st.SuggestedTools) > 0 || len(st.Dependencies) > 0 || len(st.Consumes) > 0 || len(st.Produces) > 0 {
			needsTools = true
			break
		}
		if len(st.ToolParameters) > 0 {
			needsTools = true
			break
		}
	}
	if rolePresent {
		needsTools = false
	}
	simpleByShape := len(decomp.Subtasks) == 0 || (len(decomp.Subtasks) == 1 && !needsTools)
	isSimple := decomp.ComplexityScore < simpleThreshold && simpleByShape

	// Set parent workflow ID for child workflows to use for unified event streaming
	// MUST be set BEFORE any strategy workflow routing to ensure events go to parent
	// 设置 parent workflow ID，供子 workflow 统一事件流使用
	// 必须在任何 strategy workflow 路由之前设置，确保事件发往 parent
	parentWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	input.ParentWorkflowID = parentWorkflowID

	// Route to AgentWorkflow if context.agent is present (single-purpose deterministic agents)
	// context.agent 存在时路由到 AgentWorkflow（单一用途的确定性 agent）
	if input.Context != nil {
		if agentID, ok := input.Context["agent"].(string); ok && agentID != "" {
			logger.Info("Routing to AgentWorkflow based on context.agent", "agent_id", agentID)

			// Convert to agent workflow input
			// 转换为 agent workflow 的输入
			agentInput, err := ConvertAgentInputFromTask(input)
			if err != nil {
				logger.Error("Failed to convert agent input", "error", err)
				return TaskResult{Success: false, ErrorMessage: err.Error()}, err
			}

			// Execute AgentWorkflow as child
			// 以子 workflow 形式执行 AgentWorkflow
			childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
				ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
			})
			var agentOutput AgentWorkflowOutput
			err = workflow.ExecuteChildWorkflow(childCtx, AgentWorkflow, *agentInput).Get(ctx, &agentOutput)
			if err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error()}, err
			}

			return AgentWorkflowOutputToTaskResult(&agentOutput), nil
		}
	}

	// NOTE: force_research is handled in early routing (before decomposition) to avoid
	// redundant LLM calls. See "Early route: Force ResearchWorkflow" block above.
	// 注意：force_research 已在早路由（分解之前）处理，以避免
	// 多余的 LLM 调用；见上方 "Early route: Force ResearchWorkflow" 代码块。

	// Route to BrowserUseWorkflow if browser_use role is present (unified agent loop)
	// 若存在 browser_use role，则路由到 BrowserUseWorkflow（统一的 agent 循环）
	browserUseVersion := workflow.GetVersion(ctx, "browser_use_routing_v1", workflow.DefaultVersion, 2)
	if browserUseVersion >= 2 && rolePresent && input.Context != nil {
		if role, ok := input.Context["role"].(string); ok && role == "browser_use" {
			logger.Info("Routing to BrowserUseWorkflow based on browser_use role")
			// Force LLM to use browser tools (prevents hallucinating completion without tool calls)
			// 强制 LLM 使用浏览器工具（防止未调用工具就谎称完成）
			input.Context["force_tools"] = true
			if result, handled, err := routeStrategyWorkflow(ctx, input, "browser_use", decomp.Mode, emitCtx, controlHandler); handled {
				return result, err
			}
		}
	} else if browserUseVersion == 1 && rolePresent && input.Context != nil {
		// Legacy: v1 used ReactWorkflow
		// 兼容旧版：v1 使用 ReactWorkflow
		if role, ok := input.Context["role"].(string); ok && role == "browser_use" {
			logger.Info("Routing to ReactWorkflow based on browser_use role (legacy v1)")
			input.Context["force_tools"] = true
			if result, handled, err := routeStrategyWorkflow(ctx, input, "react", decomp.Mode, emitCtx, controlHandler); handled {
				return result, err
			}
		}
	}

	// Auto-detect browser intent and assign browser_use role if not already set
	// IMPORTANT: detectBrowserIntent behavior changed significantly (simplified to JS-required domains only)
	// Use a separate changeID to track this behavior change independently from workflow routing
	// detectBrowserIntent 的行为已大幅简化（仅 JS 必需域名），
	// 故用独立 changeID 跟踪，与 workflow 路由解耦。
	autoDetectVersion := workflow.GetVersion(ctx, "browser_auto_detect_v2", workflow.DefaultVersion, 1)
	if autoDetectVersion >= 1 && !rolePresent && detectBrowserIntent(input.Query) {
		logger.Info("Auto-detected browser intent, assigning browser_use role")
		if input.Context == nil {
			input.Context = map[string]any{}
		}
		input.Context["role"] = "browser_use"
		input.Context["role_auto_detected"] = true
		// Force LLM to use browser tools (prevents hallucinating completion without tool calls)
		// 强制 LLM 使用浏览器工具（防止未调用工具就谎称完成）
		input.Context["force_tools"] = true

		// Emit role assignment event
		// 发送 role 分配事件
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			EventType:  activities.StreamEventRoleAssigned,
			AgentID:    "browser_use",
			Message:    activities.MsgRoleAssigned("browser_use (auto-detected)"),
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"role":          "browser_use",
				"auto_detected": true,
			},
		}).Get(ctx, nil)

		// Use BrowserUseWorkflow for v2+, ReactWorkflow for legacy v1
		// v2+ 使用 BrowserUseWorkflow，旧版 v1 使用 ReactWorkflow
		strategy := "browser_use"
		if browserUseVersion == 1 {
			strategy = "react"
		}
		if result, handled, err := routeStrategyWorkflow(ctx, input, strategy, decomp.Mode, emitCtx, controlHandler); handled {
			return result, err
		}
	}

	// Cognitive program takes precedence if specified
	// 若指定了 cognitive program，则优先使用
	if decomp.CognitiveStrategy != "" && decomp.CognitiveStrategy != "direct" && decomp.CognitiveStrategy != "decompose" {
		if result, handled, err := routeStrategyWorkflow(ctx, input, decomp.CognitiveStrategy, decomp.Mode, emitCtx, controlHandler); handled {
			return result, err
		}
		logger.Warn("Unknown cognitive strategy; continuing routing", "strategy", decomp.CognitiveStrategy)
	}

	// Force ResearchWorkflow via context flag (user-facing via CLI and scheduled tasks)
	// Uses GetContextBool to handle both bool and string "true" (proto map<string,string> converts to string)
	// 通过 context 开关强制走 ResearchWorkflow（CLI 与定时任务对用户可见）
	// 用 GetContextBool 兼容 bool 与字符串 "true"（proto 的 map<string,string> 会转成字符串）
	if GetContextBool(input.Context, "force_research") {
		logger.Info("Forcing ResearchWorkflow via context flag")
		if result, handled, err := routeStrategyWorkflow(ctx, input, "research", decomp.Mode, emitCtx, controlHandler); handled {
			return result, err
		}
	}

	// Check if P2P is forced via context
	// 检查是否通过 context 强制启用 P2P
	forceP2P := GetContextBool(input.Context, "force_p2p")
	if forceP2P {
		logger.Info("P2P coordination forced via context flag")
	}

	// NOTE: force_swarm is handled in early routing (before decomposition) to avoid
	// redundant LLM calls. See "Early route: Force SwarmWorkflow" block above.
	// 注意：force_swarm 已在早路由（分解之前）处理，以避免
	// 多余的 LLM 调用；见上方 "Early route: Force SwarmWorkflow" 代码块。

	switch {
	case isSimple && !forceP2P:
		// Check pause/cancel before starting child workflow
		// 启动子 workflow 前检查 pause/cancel
		if err := controlHandler.CheckPausePoint(ctx, "pre_simple_workflow"); err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}
		// Keep simple path lightweight as a child for isolation (unless P2P is forced)
		// simple 路径仍以子 workflow 形式保持轻量以便隔离（除非强制启用 P2P）
		var result TaskResult
		ometrics.WorkflowsStarted.WithLabelValues("SimpleTaskWorkflow", "simple").Inc()
		// Emit delegation event
		// 发送 delegation 事件
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: parentWorkflowID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgHandoffSimple(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)

		// Pass suggested tools from decomposition to SimpleTaskWorkflow
		// 把分解结果中的 suggested tools 传给 SimpleTaskWorkflow
		if len(decomp.Subtasks) > 0 && len(decomp.Subtasks[0].SuggestedTools) > 0 {
			input.SuggestedTools = decomp.Subtasks[0].SuggestedTools
			input.ToolParameters = decomp.Subtasks[0].ToolParameters
		}

		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		childFuture := workflow.ExecuteChildWorkflow(childCtx, SimpleTaskWorkflow, input)
		var childExec workflow.Execution
		if err := childFuture.GetChildWorkflowExecution().Get(childCtx, &childExec); err != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, err
		}
		controlHandler.RegisterChildWorkflow(childExec.ID)
		execErr := childFuture.Get(childCtx, &result)
		controlHandler.UnregisterChildWorkflow(childExec.ID)

		// Generate title regardless of success/failure (best-effort)
		// 无论成功与否都生成标题（尽力而为）
		scheduleStreamEnd(ctx)

		if execErr != nil {
			// Emit workflow.cancelled if this was a cancellation (child skipped emission due to SkipSSEEmit)
			// 若本次是取消，则发送 workflow.cancelled（子 workflow 因 SkipSSEEmit 跳过了发送）
			controlHandler.EmitCancelledIfNeeded(ctx, execErr.Error())
			result = AddTaskContextToMetadata(result, input.Context)
			return result, execErr
		}
		// Add task context to metadata for API exposure
		// 把 task context 写入 metadata 以便 API 暴露
		result = AddTaskContextToMetadata(result, input.Context)
		return result, nil

	default:
		// Check pause/cancel before starting child workflow
		// 启动子 workflow 前检查 pause/cancel
		if err := controlHandler.CheckPausePoint(ctx, "pre_dag_workflow"); err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}
		// Standard DAG strategy (fan-out/fan-in)
		// 标准 DAG 策略（fan-out/fan-in）
		ometrics.WorkflowsStarted.WithLabelValues("DAGWorkflow", "standard").Inc()
		// Emit delegation event
		// 发送 delegation 事件
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: parentWorkflowID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgHandoffTeamPlan(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
		strategiesInput := convertToStrategiesInput(input)
		var strategiesResult strategies.TaskResult
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		dagFuture := workflow.ExecuteChildWorkflow(childCtx, strategies.DAGWorkflow, strategiesInput)
		var dagExec workflow.Execution
		if err := dagFuture.GetChildWorkflowExecution().Get(childCtx, &dagExec); err != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, err
		}
		controlHandler.RegisterChildWorkflow(dagExec.ID)
		execErr := dagFuture.Get(childCtx, &strategiesResult)
		controlHandler.UnregisterChildWorkflow(dagExec.ID)

		// Generate title regardless of success/failure (best-effort)
		// 无论成功与否都生成标题（尽力而为）
		scheduleStreamEnd(ctx)

		if execErr != nil {
			// Emit workflow.cancelled if this was a cancellation (child skipped emission due to SkipSSEEmit)
			// 若本次是取消，则发送 workflow.cancelled（子 workflow 因 SkipSSEEmit 跳过了发送）
			controlHandler.EmitCancelledIfNeeded(ctx, execErr.Error())
			out := AddTaskContextToMetadata(TaskResult{Success: false, ErrorMessage: execErr.Error()}, input.Context)
			return out, execErr
		}
		// Add task context to metadata for API exposure
		// 把 task context 写入 metadata 以便 API 暴露
		result := convertFromStrategiesResult(strategiesResult)
		result = AddTaskContextToMetadata(result, input.Context)
		return result, nil
	}
}

// startAsyncTitleGeneration fires off title generation at workflow start (non-blocking).
// This runs in parallel with the main workflow so users see titles immediately.
// The activity is best-effort with a short timeout and no retries.
// workflow 启动时并行触发标题生成（非阻塞），让用户尽快看到标题；该 activity 尽力而为，timeout 短、无 retry。
func startAsyncTitleGeneration(ctx workflow.Context, sessionID, query string) {
	// Version gate for deterministic replay - new version for async behavior
	// 为确定性重放设版本闸门 —— 异步行为使用新的版本号
	titleVersion := workflow.GetVersion(ctx, "session_title_async_v1", workflow.DefaultVersion, 1)
	if titleVersion < 1 {
		return
	}
	// Skip when sessionID is empty
	// sessionID 为空时跳过
	if sessionID == "" {
		return
	}

	// Fire-and-forget: run title generation in background goroutine
	// fire-and-forget：在后台 goroutine 中执行标题生成
	workflow.Go(ctx, func(gCtx workflow.Context) {
		titleOpts := workflow.ActivityOptions{
			StartToCloseTimeout: 15 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				MaximumAttempts: 1, // Best-effort, don't retry on failure 注：尽力而为，失败不重试
			},
		}
		titleCtx := workflow.WithActivityOptions(gCtx, titleOpts)

		// Execute title generation - errors are ignored (best-effort)
		// 执行标题生成 —— 错误直接忽略（尽力而为）
		_ = workflow.ExecuteActivity(titleCtx, "GenerateSessionTitle", activities.GenerateSessionTitleInput{
			SessionID: sessionID,
			Query:     query,
		}).Get(titleCtx, nil)
	})
}

// scheduleStreamEnd emits the STREAM_END event to signal end of workflow processing.
// This should be called at the end of each workflow path.
// scheduleStreamEnd 发送 STREAM_END 事件，标志 workflow 处理结束。
// 应在每条 workflow 路径结束时调用。
func scheduleStreamEnd(ctx workflow.Context) {
	// Version gate for deterministic replay
	// 为确定性重放设版本闸门
	streamEndVersion := workflow.GetVersion(ctx, "stream_end_v1", workflow.DefaultVersion, 1)
	if streamEndVersion < 1 {
		return
	}

	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		EventType:  activities.StreamEventStreamEnd,
		AgentID:    "orchestrator",
		Message:    activities.MsgStreamEnd(),
		Timestamp:  workflow.Now(ctx),
	}).Get(emitCtx, nil)
}

// convertToStrategiesInput converts workflows.TaskInput to strategies.TaskInput
// convertToStrategiesInput 把 workflows.TaskInput 转换为 strategies.TaskInput
func convertToStrategiesInput(input TaskInput) strategies.TaskInput {
	// Convert History messages
	// 转换 History 消息
	history := make([]strategies.Message, len(input.History))
	for i, msg := range input.History {
		history[i] = strategies.Message{
			Role:      msg.Role,
			Content:   msg.Content,
			Timestamp: msg.Timestamp,
		}
	}

	return strategies.TaskInput{
		Query:                   input.Query,
		UserID:                  input.UserID,
		TenantID:                input.TenantID,
		SessionID:               input.SessionID,
		Context:                 input.Context,
		Mode:                    input.Mode,
		TemplateName:            input.TemplateName,
		TemplateVersion:         input.TemplateVersion,
		DisableAI:               input.DisableAI,
		History:                 history,
		SessionCtx:              input.SessionCtx,
		RequireApproval:         input.RequireApproval,
		ApprovalTimeout:         input.ApprovalTimeout,
		BypassSingleResult:      input.BypassSingleResult,
		ParentWorkflowID:        input.ParentWorkflowID,
		PreplannedDecomposition: input.PreplannedDecomposition,
	}
}

// convertFromStrategiesResult converts strategies.TaskResult to workflows.TaskResult
// convertFromStrategiesResult 把 strategies.TaskResult 转换为 workflows.TaskResult
func convertFromStrategiesResult(result strategies.TaskResult) TaskResult {
	return TaskResult{
		Result:       result.Result,
		Success:      result.Success,
		TokensUsed:   result.TokensUsed,
		ErrorMessage: result.ErrorMessage,
		Metadata:     result.Metadata,
	}
}

func extractTemplateRequest(input TaskInput) (string, string) {
	name := strings.TrimSpace(input.TemplateName)
	version := strings.TrimSpace(input.TemplateVersion)

	if name == "" && input.Context != nil {
		if v, ok := input.Context["template"].(string); ok {
			name = strings.TrimSpace(v)
		}
		// Accept legacy/alias key: template_name
		// 兼容旧版/别名 key：template_name
		if name == "" {
			if v2, ok2 := input.Context["template_name"].(string); ok2 {
				name = strings.TrimSpace(v2)
			}
		}
	}
	if version == "" && input.Context != nil {
		if v, ok := input.Context["template_version"].(string); ok {
			version = strings.TrimSpace(v)
		}
	}
	return name, version
}

func routeStrategyWorkflow(ctx workflow.Context, input TaskInput, strategy string, mode string, emitCtx workflow.Context, controlHandler *ControlSignalHandler) (TaskResult, bool, error) {
	strategyLower := strings.ToLower(strings.TrimSpace(strategy))
	if strategyLower == "" {
		return TaskResult{}, false, nil
	}

	switch strategyLower {
	case "simple":
		// Check pause/cancel before starting child workflow
		// 启动子 workflow 前检查 pause/cancel
		if controlHandler != nil {
			if err := controlHandler.CheckPausePoint(ctx, "pre_simple_strategy"); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error()}, true, err
			}
		}
		var result TaskResult
		ometrics.WorkflowsStarted.WithLabelValues("SimpleTaskWorkflow", mode).Inc()
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgWorkflowRouting(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		childFuture := workflow.ExecuteChildWorkflow(childCtx, SimpleTaskWorkflow, input)
		var childExecID string
		if controlHandler != nil {
			var childExec workflow.Execution
			if err := childFuture.GetChildWorkflowExecution().Get(childCtx, &childExec); err != nil {
				return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, true, err
			}
			childExecID = childExec.ID
			controlHandler.RegisterChildWorkflow(childExecID)
		}
		execErr := childFuture.Get(childCtx, &result)
		if controlHandler != nil && childExecID != "" {
			controlHandler.UnregisterChildWorkflow(childExecID)
		}

		// Generate title regardless of success/failure (best-effort)
		// 无论成功与否都生成标题（尽力而为）
		scheduleStreamEnd(ctx)

		if execErr != nil {
			// Emit workflow.cancelled if this was a cancellation (child skipped emission due to SkipSSEEmit)
			// 若本次是取消，则发送 workflow.cancelled（子 workflow 因 SkipSSEEmit 跳过了发送）
			if controlHandler != nil {
				controlHandler.EmitCancelledIfNeeded(ctx, execErr.Error())
			}
			result = AddTaskContextToMetadata(result, input.Context)
			return result, true, execErr
		}
		// Add task context to metadata for API exposure
		// 把 task context 写入 metadata 以便 API 暴露
		result = AddTaskContextToMetadata(result, input.Context)
		return result, true, nil
	case "react", "exploratory", "research", "scientific", "browser_use":
		// Check pause/cancel before starting child workflow
		// 启动子 workflow 前检查 pause/cancel
		if controlHandler != nil {
			if err := controlHandler.CheckPausePoint(ctx, "pre_"+strategyLower+"_workflow"); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error()}, true, err
			}
		}
		var wfName string
		var wfFunc any
		switch strategyLower {
		case "react":
			wfName = "ReactWorkflow"
			wfFunc = strategies.ReactWorkflow
		case "exploratory":
			wfName = "ExploratoryWorkflow"
			wfFunc = strategies.ExploratoryWorkflow
		case "research":
			wfName = "ResearchWorkflow"
			wfFunc = strategies.ResearchWorkflow
		case "scientific":
			wfName = "ScientificWorkflow"
			wfFunc = strategies.ScientificWorkflow
		case "browser_use":
			wfName = "BrowserUseWorkflow"
			wfFunc = strategies.BrowserUseWorkflow
		}

		strategiesInput := convertToStrategiesInput(input)
		var strategiesResult strategies.TaskResult
		ometrics.WorkflowsStarted.WithLabelValues(wfName, mode).Inc()
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
			EventType:  activities.StreamEventDelegation,
			AgentID:    "orchestrator",
			Message:    activities.MsgWorkflowRouting(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
		childOpts := workflow.ChildWorkflowOptions{
			ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		}
		if wfName == "ResearchWorkflow" {
			v := workflow.GetVersion(ctx, "research_child_timeout_v1", workflow.DefaultVersion, 1)
			if v >= 1 {
				childOpts.WorkflowExecutionTimeout = ResearchChildWorkflowTimeout
			}
		}
		childCtx := workflow.WithChildOptions(ctx, childOpts)
		strategyFuture := workflow.ExecuteChildWorkflow(childCtx, wfFunc, strategiesInput)
		var strategyExecID string
		if controlHandler != nil {
			var strategyExec workflow.Execution
			if err := strategyFuture.GetChildWorkflowExecution().Get(childCtx, &strategyExec); err != nil {
				return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, true, err
			}
			strategyExecID = strategyExec.ID
			controlHandler.RegisterChildWorkflow(strategyExecID)
		}
		execErr := strategyFuture.Get(childCtx, &strategiesResult)
		if controlHandler != nil && strategyExecID != "" {
			controlHandler.UnregisterChildWorkflow(strategyExecID)
		}

		// Generate title regardless of success/failure (best-effort)
		// 无论成功与否都生成标题（尽力而为）
		scheduleStreamEnd(ctx)

		if execErr != nil {
			// Emit workflow.cancelled if this was a cancellation (child skipped emission due to SkipSSEEmit)
			// 若本次是取消，则发送 workflow.cancelled（子 workflow 因 SkipSSEEmit 跳过了发送）
			if controlHandler != nil {
				controlHandler.EmitCancelledIfNeeded(ctx, execErr.Error())
			}
			// Preserve child's failure metadata (e.g. partial_result, phase, token counts)
			// 保留子 workflow 的失败元数据（如 partial_result、phase、token 计数）
			res := convertFromStrategiesResult(strategiesResult)
			res = AddTaskContextToMetadata(res, input.Context)
			return res, true, execErr
		}
		// Add task context to metadata for API exposure
		// 把 task context 写入 metadata 以便 API 暴露
		result := convertFromStrategiesResult(strategiesResult)
		result = AddTaskContextToMetadata(result, input.Context)
		return result, true, nil
	default:
		return TaskResult{}, false, nil
	}
}

func recommendStrategy(ctx workflow.Context, input TaskInput) (*activities.RecommendStrategyOutput, error) {
	startTime := workflow.Now(ctx)

	actx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})

	var rec activities.RecommendStrategyOutput
	err := workflow.ExecuteActivity(actx, activities.RecommendWorkflowStrategy, activities.RecommendStrategyInput{
		SessionID: input.SessionID,
		UserID:    input.UserID,
		TenantID:  input.TenantID,
		Query:     input.Query,
	}).Get(ctx, &rec)

	// Record metrics (fire-and-forget)
	// 记录 metrics（fire-and-forget）
	metricsCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 2 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	latency := workflow.Now(ctx).Sub(startTime).Seconds()
	strategy := "none"
	source := "none"
	confidence := 0.0
	success := false

	if err == nil && rec.Strategy != "" {
		strategy = rec.Strategy
		source = rec.Source
		confidence = rec.Confidence
		success = true
	}

	workflow.ExecuteActivity(
		metricsCtx,
		"RecordLearningRouterMetrics",
		map[string]any{
			"latency_seconds": latency,
			"strategy":        strategy,
			"source":          source,
			"confidence":      confidence,
			"success":         success,
		},
	)

	if err != nil {
		return nil, err
	}
	return &rec, nil
}

// detectBrowserIntent checks if the query requires browser automation.
// Only returns true for sites that REQUIRE JavaScript rendering.
// Normal URLs go through decomposition which chooses web_fetch/web_search.
// Users can explicitly set role=browser_use for interactive tasks.
// 仅对必须 JS 渲染的站点返回 true；普通 URL 交给分解流程选择 web_fetch/web_search；
// 用户也可显式设置 role=browser_use 做交互式 task。
func detectBrowserIntent(query string) bool {
	q := strings.ToLower(query)

	// Sites that REQUIRE browser automation (JavaScript rendering)
	// These sites cannot be fetched with simple HTTP - they need a real browser
	// 必须使用浏览器自动化（JavaScript 渲染）的站点
	// 这些站点无法用普通 HTTP 抓取 —— 需要真实浏览器
	jsRequiredDomains := []string{
		"weixin.qq.com",    // WeChat articles (heavy JS) 注：微信文章（JS 很重）
		"mp.weixin.qq.com", // WeChat public accounts 注：微信公众号
		// Add other JS-required domains as discovered
		// 后续发现其他必需 JS 的域名可在此追加
	}
	for _, domain := range jsRequiredDomains {
		if strings.Contains(q, domain) {
			return true
		}
	}

	// All other URLs (including http://, https://, .com, .io, etc.)
	// go through normal decomposition which will choose appropriate tools
	// (web_fetch, web_search, research workflow, etc.)
	// 其余所有 URL（包括 http://、https://、.com、.io 等）
	// 都走正常分解流程，由它挑选合适的 tool
	// （web_fetch、web_search、research workflow 等）
	return false
}
