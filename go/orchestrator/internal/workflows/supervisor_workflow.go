package workflows

import (
	"encoding/json"
	"fmt"
	"maps"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/formatting"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metadata"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/strategies"
)

// Note: parseNumericValue is defined in dag_workflow.go and shared across workflows
// 说明：parseNumericValue 定义在 dag_workflow.go 中，由各 workflow 共用

// MailboxMessage is a minimal deterministic message record used by SupervisorWorkflow.
// MailboxMessage 是 SupervisorWorkflow 使用的最小化确定性消息记录。
type MailboxMessage struct {
	From    string
	To      string
	Role    string
	Content string
}

// SendMailboxMessage helper to signal another workflow's mailbox.
// SendMailboxMessage 辅助函数，用于向另一个 workflow 的 mailbox 发送 signal。
func SendMailboxMessage(ctx workflow.Context, targetWorkflowID string, msg MailboxMessage) error {
	return workflow.SignalExternalWorkflow(ctx, targetWorkflowID, "", "mailbox_v1", msg).Get(ctx, nil)
}

// SupervisorWorkflow orchestrates sub-teams using child workflows.
// SupervisorWorkflow 使用子 workflow 编排子团队。
// v1: decompose → delegate subtasks to SimpleTaskWorkflow children → synthesize.
// v1：decompose → 把 subtask 委派给 SimpleTaskWorkflow 子 workflow → synthesize。
func SupervisorWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting SupervisorWorkflow", "query", input.Query, "user_id", input.UserID)

	// Capture workflow start time for duration tracking
	// 记录 workflow 启动时间，用于统计耗时
	workflowStartTime := workflow.Now(ctx)

	// ENTERPRISE TIMEOUT STRATEGY:
	// - No overall workflow timeout (complex tasks may take hours/days)
	// - Per-task retry limits (3 max) prevent infinite loops
	// - Failure threshold (50%+1) provides intelligent abort criteria
	// - See docs/timeout-retry-strategy.md for full details
	// timeout 策略：不设整体 workflow timeout（复杂 task 可能跑数小时甚至数天）；
	// 用单个 task retry 上限（3 次）防止死循环，用失败阈值（50%+1）作为中止判据。

	// Determine workflow ID for event streaming
	// 确定用于 event streaming 的 workflow ID
	// Use parent workflow ID if this is a child workflow, otherwise use own ID
	// 若为子 workflow 则用父 workflow ID，否则用自身 ID
	workflowID := input.ParentWorkflowID
	if workflowID == "" {
		workflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	}

	// Emit WORKFLOW_STARTED event
	// 发出 WORKFLOW_STARTED 事件
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Initialize control signal handler for pause/resume/cancel
	// 初始化 pause/resume/cancel 控制 signal 处理器
	controlHandler := &ControlSignalHandler{
		WorkflowID: workflowID,
		AgentID:    "supervisor",
		Logger:     logger,
		EmitCtx:    emitCtx,
	}
	controlHandler.Setup(ctx)

	if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowStarted,
		AgentID:    "supervisor",
		Message:    activities.MsgSupervisorStarted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil); err != nil {
		logger.Warn("Failed to emit workflow started event", "error", err)
	}

	// Mailbox v1 (optional): accept messages via signal and expose via query handler
	// Mailbox v1（可选）：通过 signal 接收消息，并通过 query handler 对外暴露
	var messages []MailboxMessage
	var messagesMu sync.RWMutex // Protects messages slice from query handler races 注：保护 messages 切片免受 query handler 竞态影响
	// Agent directory (role metadata)
	// Agent 目录（role 元数据）
	type AgentInfo struct {
		AgentID string
		Role    string
	}
	var teamAgents []AgentInfo
	var teamAgentsMu sync.RWMutex // Protects teamAgents slice from query handler races 注：保护 teamAgents 切片免受 query handler 竞态影响
	// Dependency sync (selectors) — topic notifications
	// 依赖同步（selector）—— topic 通知
	topicChans := make(map[string]workflow.Channel)
	var msgChan workflow.Channel // Declare at function scope for use across version checks 注：在函数作用域声明，便于跨版本检查复用
	if workflow.GetVersion(ctx, "mailbox_v1", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
		sig := workflow.GetSignalChannel(ctx, "mailbox_v1")
		msgChan = workflow.NewChannel(ctx)
		workflow.Go(ctx, func(ctx workflow.Context) {
			for {
				var msg MailboxMessage
				sig.Receive(ctx, &msg)
				// Non-blocking send to prevent goroutine deadlock
				// 非阻塞发送，避免 goroutine 死锁
				sel := workflow.NewSelector(ctx)
				sel.AddSend(msgChan, msg, func() {})
				sel.AddDefault(func() {
					logger.Debug("Mailbox channel send would block, skipping message", "from", msg.From, "to", msg.To)
				})
				sel.Select(ctx)
			}
		})
		workflow.Go(ctx, func(ctx workflow.Context) {
			for {
				var msg MailboxMessage
				msgChan.Receive(ctx, &msg)
				// Protect slice modification from concurrent query handler reads
				// 保护切片修改，避免与并发 query handler 读取冲突
				messagesMu.Lock()
				messages = append(messages, msg)
				messagesMu.Unlock()
			}
		})
		_ = workflow.SetQueryHandler(ctx, "getMailbox", func() ([]MailboxMessage, error) {
			// Return a copy to avoid race conditions
			// 返回副本以避免竞态
			messagesMu.RLock()
			result := make([]MailboxMessage, len(messages))
			copy(result, messages)
			messagesMu.RUnlock()
			return result, nil
		})
	}
	_ = workflow.SetQueryHandler(ctx, "listTeamAgents", func() ([]AgentInfo, error) {
		// Return a copy to avoid race conditions
		// 返回副本以避免竞态
		teamAgentsMu.RLock()
		result := make([]AgentInfo, len(teamAgents))
		copy(result, teamAgents)
		teamAgentsMu.RUnlock()
		return result, nil
	})
	_ = workflow.SetQueryHandler(ctx, "findTeamAgentsByRole", func(role string) ([]AgentInfo, error) {
		teamAgentsMu.RLock()
		out := make([]AgentInfo, 0)
		for _, a := range teamAgents {
			if a.Role == role {
				out = append(out, a)
			}
		}
		teamAgentsMu.RUnlock()
		return out, nil
	})

	// Configure activities
	// 配置 activities
	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, actOpts)

	// Version gate for enhanced supervisor memory
	// enhanced supervisor memory 的版本门禁
	supervisorMemoryVersion := workflow.GetVersion(ctx, "supervisor_memory_v2", workflow.DefaultVersion, 2)

	var decompositionAdvisor *activities.DecompositionAdvisor
	var decompositionSuggestion activities.DecompositionSuggestion

	if supervisorMemoryVersion >= 2 && input.SessionID != "" {
		// Fetch enhanced supervisor memory with strategic insights
		// 获取带战略洞察的 enhanced supervisor memory
		var supervisorMemory *activities.SupervisorMemoryContext
		supervisorMemoryInput := activities.FetchSupervisorMemoryInput{
			SessionID: input.SessionID,
			UserID:    input.UserID,
			TenantID:  input.TenantID,
			Query:     input.Query,
		}

		// Execute enhanced memory fetch
		// 执行 enhanced memory 获取
		if err := workflow.ExecuteActivity(ctx, "FetchSupervisorMemory", supervisorMemoryInput).Get(ctx, &supervisorMemory); err == nil {
			// Store conversation history in context
			// 将对话历史写入 context
			if len(supervisorMemory.ConversationHistory) > 0 {
				if input.Context == nil {
					input.Context = make(map[string]any)
				}
				input.Context["agent_memory"] = supervisorMemory.ConversationHistory
			}

			// Create decomposition advisor for intelligent task breakdown
			// 创建 decomposition advisor，用于智能任务拆解
			decompositionAdvisor = activities.NewDecompositionAdvisor(supervisorMemory)
			decompositionSuggestion = decompositionAdvisor.SuggestDecomposition(input.Query)

			// Log strategic memory insights
			// 记录 strategic memory 洞察
			logger.Info("Enhanced supervisor memory loaded",
				"decomposition_patterns", len(supervisorMemory.DecompositionHistory),
				"strategies_tracked", len(supervisorMemory.StrategyPerformance),
				"failure_patterns", len(supervisorMemory.FailurePatterns),
				"user_expertise", supervisorMemory.UserPreferences.ExpertiseLevel)
		} else {
			logger.Warn("Failed to fetch enhanced supervisor memory, falling back to basic", "error", err)
			// Fall back to basic hierarchical memory
			// 回退到基础分层 memory
			fallbackToBasicMemory(ctx, &input, logger)
		}
	} else if supervisorMemoryVersion >= 1 && input.SessionID != "" {
		// Use basic memory for older versions
		// 旧版本使用基础 memory
		fallbackToBasicMemory(ctx, &input, logger)
	}

	// User persistent memory: prompt injection and extraction are swarm-only.
	// 用户持久 memory：prompt 注入与抽取仅 swarm 使用。

	// Dynamic team v1: handle recruit/retire signals
	// 动态团队 v1：处理 recruit/retire signal
	type RecruitRequest struct {
		Description string
		Role        string
	}
	type RetireRequest struct{ AgentID string }
	recruitCh := workflow.GetSignalChannel(ctx, "recruit_v1")
	retireCh := workflow.GetSignalChannel(ctx, "retire_v1")
	var childResults []activities.AgentExecutionResult
	if workflow.GetVersion(ctx, "dynamic_team_v1", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
		workflow.Go(ctx, func(ctx workflow.Context) {
			for {
				sel := workflow.NewSelector(ctx)
				sel.AddReceive(recruitCh, func(c workflow.ReceiveChannel, more bool) {
					var req RecruitRequest
					c.Receive(ctx, &req)
					role := req.Role
					if role == "" {
						role = "generalist"
					}
					// Policy authorization
					// 策略授权
					var dec activities.TeamActionDecision
					if err := workflow.ExecuteActivity(ctx, activities.AuthorizeTeamAction, activities.TeamActionInput{
						Action: "recruit", SessionID: input.SessionID, UserID: input.UserID, AgentID: "supervisor", Role: role,
						Metadata: map[string]any{"reason": "dynamic recruit", "description": req.Description},
					}).Get(ctx, &dec); err != nil {
						logger.Error("Team action authorization failed", "error", err)
						return
					}
					if !dec.Allow {
						logger.Warn("Recruit denied by policy", "reason", dec.Reason)
						return
					}
					// Stream event
					// 流式推送事件
					emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
						RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
					})
					if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventTeamRecruited,
						AgentID:    role,
						Message:    req.Description,
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil); err != nil {
						logger.Warn("Failed to emit team recruited event", "error", err)
					}
					// Start child simple task with graceful cancellation
					// 启动子简单 task，支持优雅取消
					childOpts := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
						ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
					})
					var res TaskResult
					simpleTaskFuture := workflow.ExecuteChildWorkflow(childOpts, SimpleTaskWorkflow, TaskInput{
						Query: req.Description, UserID: input.UserID, SessionID: input.SessionID,
						Context: map[string]any{"role": role}, Mode: input.Mode, History: input.History, SessionCtx: input.SessionCtx,
						ParentWorkflowID: workflowID, // Preserve parent workflow ID for event streaming 注：保留父 workflow ID 用于 event streaming
					})
					var dynamicChildExec workflow.Execution
					if err := simpleTaskFuture.GetChildWorkflowExecution().Get(childOpts, &dynamicChildExec); err != nil {
						logger.Error("Dynamic child workflow failed to get execution", "error", err)
						return
					}
					controlHandler.RegisterChildWorkflow(dynamicChildExec.ID)
					if err := simpleTaskFuture.Get(childOpts, &res); err != nil {
						controlHandler.UnregisterChildWorkflow(dynamicChildExec.ID)
						logger.Error("Dynamic child workflow failed", "error", err)
						return
					}
					controlHandler.UnregisterChildWorkflow(dynamicChildExec.ID)
					childResults = append(childResults, activities.AgentExecutionResult{AgentID: "dynamic", Response: res.Result, TokensUsed: res.TokensUsed, Success: res.Success})
				})
				sel.AddReceive(retireCh, func(c workflow.ReceiveChannel, more bool) {
					var req RetireRequest
					c.Receive(ctx, &req)
					var dec activities.TeamActionDecision
					if err := workflow.ExecuteActivity(ctx, activities.AuthorizeTeamAction, activities.TeamActionInput{
						Action: "retire", SessionID: input.SessionID, UserID: input.UserID, AgentID: req.AgentID,
						Metadata: map[string]any{"reason": "dynamic retire"},
					}).Get(ctx, &dec); err != nil {
						logger.Error("Team action authorization failed", "error", err)
						return
					}
					if !dec.Allow {
						logger.Warn("Retire denied by policy", "reason", dec.Reason)
						return
					}
					emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
						StartToCloseTimeout: 30 * time.Second,
						RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
					})
					if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventTeamRetired,
						AgentID:    req.AgentID,
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil); err != nil {
						logger.Warn("Failed to emit team retired event", "error", err)
					}
				})
				sel.Select(ctx)
			}
		})
	}

	// Check pause/cancel before decomposition
	// 分解前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_decomposition"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Prepare decomposition input with advisor suggestions
	// 准备分解输入，带上 advisor 建议
	decomposeInput := activities.DecompositionInput{
		Query:          input.Query,
		Context:        input.Context,
		AvailableTools: []string{},
	}

	// Apply decomposition advisor suggestions if available
	// 若有 decomposition advisor 建议则应用
	if decompositionAdvisor != nil {
		if decompositionSuggestion.UsesPreviousSuccess {
			// Add suggested subtasks to context for LLM to consider
			// 将建议的 subtask 加入 context 供 LLM 参考
			if decomposeInput.Context == nil {
				decomposeInput.Context = make(map[string]any)
			}
			decomposeInput.Context["suggested_subtasks"] = decompositionSuggestion.SuggestedSubtasks
			decomposeInput.Context["suggested_strategy"] = decompositionSuggestion.Strategy
			decomposeInput.Context["confidence"] = decompositionSuggestion.Confidence
		}

		if len(decompositionSuggestion.Warnings) > 0 {
			decomposeInput.Context["decomposition_warnings"] = decompositionSuggestion.Warnings
		}

		logger.Info("Using decomposition advisor suggestions",
			"strategy", decompositionSuggestion.Strategy,
			"confidence", decompositionSuggestion.Confidence,
			"uses_previous", decompositionSuggestion.UsesPreviousSuccess)
	}

	// Decompose the task to get subtasks and agent types (use preplanned if provided)
	// 分解 task，得到 subtask 与 agent 类型（已提供预规划则直接使用）
	var decomp activities.DecompositionResult
	if input.PreplannedDecomposition != nil {
		decomp = *input.PreplannedDecomposition
	} else {
		if err := workflow.ExecuteActivity(ctx, constants.DecomposeTaskActivity, decomposeInput).Get(ctx, &decomp); err != nil {
			logger.Error("Task decomposition failed", "error", err)
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("decomposition failed: %v", err)}, err
		}

		// Record decomposition usage if provided
		// 若提供则记录 decomposition 用量
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
				UserID:       input.UserID,
				SessionID:    input.SessionID,
				TaskID:       wid,
				AgentID:      "decompose",
				Model:        decomp.ModelUsed,
				Provider:     decomp.Provider,
				InputTokens:  inTok,
				OutputTokens: outTok,
				Metadata:     map[string]any{"phase": "decompose"},
			}).Get(recCtx, nil)
		}
	}

	// Override strategy if advisor has high confidence
	// advisor 置信度高时覆盖策略
	if decompositionAdvisor != nil && decompositionSuggestion.Confidence > 0.8 {
		decomp.ExecutionStrategy = decompositionSuggestion.Strategy
		logger.Info("Overriding execution strategy based on advisor", "strategy", decomp.ExecutionStrategy)
	}

	// Emit team status event after decomposition
	// 分解后发出团队状态事件
	if len(decomp.Subtasks) > 1 {
		message := fmt.Sprintf("Coordinating %d agents to handle subtasks", len(decomp.Subtasks))
		if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventTeamStatus,
			AgentID:    "supervisor",
			Message:    message,
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to emit team status event", "error", err)
		}
	}

	// Check if task needs tools or has dependencies
	// 检查 task 是否需要工具或存在依赖
	needsTools := false
	for _, subtask := range decomp.Subtasks {
		if len(subtask.SuggestedTools) > 0 || len(subtask.Dependencies) > 0 || len(subtask.Produces) > 0 || len(subtask.Consumes) > 0 {
			needsTools = true
			break
		}
		if len(subtask.ToolParameters) > 0 {
			needsTools = true
			break
		}
	}

	// If simple task (no tools, trivial plan) OR zero-subtask fallback, delegate to DAGWorkflow
	// 若为简单 task（无工具、计划平凡）或零 subtask 回退，则委派给 DAGWorkflow
	// A single tool-based subtask should NOT be treated as simple
	// 单个基于工具的 subtask 不应视为简单 task
	simpleByShape := len(decomp.Subtasks) == 0 || (len(decomp.Subtasks) == 1 && !needsTools)
	isSimpleTask := len(decomp.Subtasks) == 0 || ((decomp.ComplexityScore < 0.3) && simpleByShape)

	if isSimpleTask {
		// Convert to strategies.TaskInput
		// 转换为 strategies.TaskInput
		strategiesInput := convertToStrategiesInput(input)
		var strategiesResult strategies.TaskResult

		// Start child workflow and register for signal propagation
		// 启动子 workflow 并注册 signal 传播
		dagFuture := workflow.ExecuteChildWorkflow(ctx, strategies.DAGWorkflow, strategiesInput)
		var dagChildExec workflow.Execution
		if err := dagFuture.GetChildWorkflowExecution().Get(ctx, &dagChildExec); err != nil {
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Failed to get child execution: %v", err)}, err
		}
		controlHandler.RegisterChildWorkflow(dagChildExec.ID)
		err := dagFuture.Get(ctx, &strategiesResult)
		controlHandler.UnregisterChildWorkflow(dagChildExec.ID)
		if err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}

		// Ensure WORKFLOW_COMPLETED is emitted even on the simple path
		// 即使在简单路径上也确保发出 WORKFLOW_COMPLETED
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventWorkflowCompleted,
			AgentID:    "supervisor",
			Message:    activities.MsgWorkflowCompleted(),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)

		return convertFromStrategiesResult(strategiesResult), nil
	}

	// Execute each subtask as a child SimpleTaskWorkflow sequentially (deterministic)
	// 依次把每个 subtask 作为子 SimpleTaskWorkflow 执行（确定性）
	var lastWSSeq uint64

	// Track running budget usage across agents if a task-level budget is present
	// 若存在 task 级预算，则跨 agent 跟踪累计预算用量
	totalUsed := 0
	taskBudget := 0
	if v, ok := input.Context["budget_remaining"].(int); ok && v > 0 {
		taskBudget = v
	}
	if v, ok := input.Context["budget_remaining"].(float64); ok && v > 0 {
		taskBudget = int(v)
	}

	// INTELLIGENT RETRY STRATEGY: Prevents infinite loops while supporting complex tasks
	// 智能 retry 策略：既支持复杂 task，又防止死循环
	failedTasks := 0
	maxFailures := len(decomp.Subtasks)/2 + 1 // Allow up to 50%+1 tasks to fail before aborting 注：允许最多 50%+1 的 task 失败后才中止
	taskRetries := make(map[string]int)       // Track retry count per task ID (prevents infinite retries) 注：按 task ID 跟踪重试次数（防止无限重试）
	maxRetriesPerTask := 3                    // Max 3 retries per individual task (handles transient failures) 注：单个 task 最多重试 3 次（应对瞬时故障）

	// Build a set of topics actually produced by this plan to avoid waiting
	// on dependencies that will never be satisfied.
	// 先算出本计划实际会产出的 topic 集合，避免等待永远不会被满足的依赖。
	producesSet := make(map[string]struct{})
	for _, s := range decomp.Subtasks {
		for _, t := range s.Produces {
			if t == "" {
				continue
			}
			producesSet[t] = struct{}{}
		}
	}

	// Version gate for context compression determinism
	// context 压缩确定性的版本门禁
	compressionVersion := workflow.GetVersion(ctx, "context_compress_v1", workflow.DefaultVersion, 1)

	// Lightweight sanitizer to keep tool_execution payloads small and serializable
	// 轻量清洗器，让 tool_execution payload 小而可序列化
	truncate := func(s string, max int) string {
		if len(s) <= max {
			return s
		}
		return s[:max] + "...(truncated)"
	}
	sanitizeToolExecutions := func(exec []activities.ToolExecution) []map[string]any {
		const maxLen = 512
		out := make([]map[string]any, 0, len(exec))
		for _, te := range exec {
			entry := map[string]any{
				"tool":    te.Tool,
				"success": te.Success,
			}
			if te.Error != "" {
				entry["error"] = te.Error
			}
			if te.Output != nil {
				switch v := te.Output.(type) {
				case string:
					entry["output"] = truncate(v, maxLen)
				default:
					if b, err := json.Marshal(v); err == nil {
						entry["output"] = truncate(string(b), maxLen)
					}
				}
			}
			out = append(out, entry)
		}
		return out
	}

	for i, st := range decomp.Subtasks {
		agentName := agents.GetAgentName(workflowID, i)
		// Check pause/cancel before each subtask
		// 每个 subtask 执行前检查 pause/cancel
		if err := controlHandler.CheckPausePoint(ctx, fmt.Sprintf("pre_subtask_%d", i)); err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}

		// Emit progress event for this subtask
		// 为该 subtask 发出进度事件
		progressMessage := fmt.Sprintf("Working on step %d of %d", i+1, len(decomp.Subtasks))
		if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventProgress,
			AgentID:    agentName,
			Message:    progressMessage,
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to emit progress event", "error", err)
		}

		// Build context, injecting role when enabled
		// 构建 context，启用时注入 role
		childCtx := make(map[string]any)
		maps.Copy(childCtx, input.Context)
		// Multimodal: all subtask agents receive attachments by default.
		// Go bool can't distinguish "unset" from "false", so we can't safely opt-out
		// based on NeedsAttachments. Cost optimization happens at the Swarm layer
		// (Lead's skip_attachments) where the LLM explicitly opts out per-agent.
		// 所有 subtask agent 默认收到附件：Go 的 bool 无法区分"未设置"与 false，
		// 因此不能安全地按 NeedsAttachments 关闭；成本优化放在 Swarm 层（Lead 的 skip_attachments）由 LLM 显式选择。
		if workflow.GetVersion(ctx, "roles_v1", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
			// Preserve incoming role by default; allow LLM-specified agent_types to override
			// 默认保留传入的 role；允许 LLM 指定的 agent_types 覆盖
			baseRole := "generalist"
			if v, ok := input.Context["role"].(string); ok && v != "" {
				baseRole = v
			}
			role := baseRole
			if i < len(decomp.AgentTypes) && decomp.AgentTypes[i] != "" {
				role = decomp.AgentTypes[i]
			}
			childCtx["role"] = role
			// Protect slice modification from concurrent query handler reads
			// 保护切片修改，避免与并发 query handler 读取冲突
			teamAgentsMu.Lock()
			teamAgents = append(teamAgents, AgentInfo{AgentID: agentName, Role: role})
			teamAgentsMu.Unlock()
			// Optional: record role assignment in mailbox
			// 可选：在 mailbox 中记录 role 分配
			if workflow.GetVersion(ctx, "mailbox_v1", workflow.DefaultVersion, 1) != workflow.DefaultVersion {
				msg := MailboxMessage{From: "supervisor", To: agentName, Role: role, Content: "role_assigned"}
				// Send to channel instead of direct append to avoid race condition
				// 发往 channel 而非直接 append，避免竞态
				msgChan.Send(ctx, msg)
			}
			// Stream role assignment
			// 流式推送 role 分配
			if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventRoleAssigned,
				AgentID:    agentName,
				Message:    role,
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil); err != nil {
				logger.Warn("Failed to emit role assigned event", "error", err)
			}
		}

		// Budget hinting: set token_budget for policy + agent if per-agent budget is present
		// 预算提示：存在按 agent 预算时，为 policy 与 agent 设置 token_budget
		agentMax := 0
		if v, ok := childCtx["budget_agent_max"].(int); ok {
			agentMax = v
		}
		if v, ok := childCtx["budget_agent_max"].(float64); ok && v > 0 {
			agentMax = int(v)
		}
		if agentMax > 0 && compressionVersion >= 1 {
			childCtx["token_budget"] = agentMax
		}

		// Sliding-window shaping with optional middle summary when nearing per-agent budget
		// 接近按 agent 预算时，用滑动窗口整形，必要时插入中段摘要
		historyForAgent := convertHistoryForAgent(input.History)
		if agentMax > 0 {
			est := activities.EstimateTokens(historyForAgent)
			trig, tgt := getCompressionRatios(childCtx, 0.75, 0.375)
			if est >= int(float64(agentMax)*trig) {
				var compressResult activities.CompressContextResult
				_ = workflow.ExecuteActivity(ctx, activities.CompressAndStoreContext, activities.CompressContextInput{
					SessionID:        input.SessionID,
					History:          convertHistoryMapForCompression(input.History),
					TargetTokens:     int(float64(agentMax) * tgt),
					ParentWorkflowID: workflowID,
				}).Get(ctx, &compressResult)
				if compressResult.Summary != "" {
					childCtx["context_summary"] = fmt.Sprintf("Previous context summary: %s", compressResult.Summary)
					prim, rec := getPrimersRecents(childCtx, 3, 20)
					shaped := shapeHistory(input.History, prim, rec)
					historyForAgent = convertHistoryForAgent(shaped)
					// Emit compression applied event
					// 发出压缩已应用事件
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventDataProcessing,
						AgentID:    agentName,
						Message:    activities.MsgCompressionApplied(),
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
					// Emit summary injected event
					// 发出摘要已注入事件
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

		// P2P Coordination: wait on declared Consumes topics before starting this subtask
		// Only enabled if P2PCoordinationEnabled is true in config and decomposition has valid Produces/Consumes
		// P2P 协调：启动 subtask 前先等待其声明的 Consumes topic；
		// 仅当配置开启 P2PCoordinationEnabled 且分解结果含合法 Produces/Consumes 时生效。
		var p2pConfig activities.WorkflowConfig
		if err := workflow.ExecuteActivity(ctx, activities.GetWorkflowConfig).Get(ctx, &p2pConfig); err != nil {
			logger.Warn("Failed to load P2P config, skipping coordination", "error", err)
			p2pConfig.P2PCoordinationEnabled = false
		}

		// Check version gates first for determinism, but only execute P2P if enabled
		// 先检查版本门禁以保证确定性，仅在启用时才执行 P2P
		p2pSyncVersion := workflow.GetVersion(ctx, "p2p_sync_v1", workflow.DefaultVersion, 1)
		teamWorkspaceVersion := workflow.GetVersion(ctx, "team_workspace_v1", workflow.DefaultVersion, 1)

		// Only proceed with P2P coordination if:
		// 仅在以下条件满足时才进行 P2P 协调：
		// 1. P2P is enabled in config AND
		// 1. 配置中已启用 P2P，且
		// 2. Version gates indicate P2P code exists
		// 2. 版本门禁表明 P2P 代码存在
		if p2pConfig.P2PCoordinationEnabled &&
			p2pSyncVersion != workflow.DefaultVersion &&
			teamWorkspaceVersion != workflow.DefaultVersion &&
			i < len(decomp.Subtasks) && len(decomp.Subtasks[i].Consumes) > 0 {
			logger.Debug("P2P coordination enabled, checking dependencies",
				"subtask_id", decomp.Subtasks[i].ID,
				"consumes", decomp.Subtasks[i].Consumes)
			for _, topic := range decomp.Subtasks[i].Consumes {
				// Skip waiting if no subtask produces this topic
				// 若无 subtask 产出该 topic 则跳过等待
				if _, ok := producesSet[topic]; !ok {
					logger.Info("Skipping P2P wait: no producer in plan", "topic", topic, "subtask_id", st.ID)
					continue
				}
				// Use configured timeout or default
				// 使用配置的 timeout 或默认值
				maxWaitTime := time.Duration(p2pConfig.P2PTimeoutSeconds) * time.Second
				if maxWaitTime == 0 {
					maxWaitTime = 6 * time.Minute
				}
				startTime := workflow.Now(ctx)
				backoff := 1 * time.Second
				maxBackoff := 30 * time.Second
				attempts := 0

				for workflow.Now(ctx).Sub(startTime) < maxWaitTime {
					// Emit waiting event on first attempt
					// 首次尝试时发出等待事件
					if attempts == 0 {
						waitMessage := "Waiting on a previous step"
						if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
							WorkflowID: workflowID,
							EventType:  activities.StreamEventWaiting,
							AgentID:    agentName,
							Message:    waitMessage,
							Timestamp:  workflow.Now(ctx),
						}).Get(ctx, nil); err != nil {
							logger.Warn("Failed to emit waiting event", "error", err)
						}
					}

					// Check if entries already exist
					// 检查条目是否已存在
					var entries []activities.WorkspaceEntry
					if err := workflow.ExecuteActivity(ctx, constants.WorkspaceListActivity, activities.WorkspaceListInput{
						WorkflowID: workflowID,
						Topic:      topic,
						SinceSeq:   0,
						Limit:      1,
					}).Get(ctx, &entries); err != nil {
						logger.Warn("Failed to check workspace", "topic", topic, "error", err)
						break
					}
					if len(entries) > 0 {
						break
					}

					// Check if we've exceeded the time limit before waiting
					// 等待前检查是否已超时
					if workflow.Now(ctx).Sub(startTime) >= maxWaitTime {
						break
					}

					// Setup selector wait using a topic channel + exponential backoff timer
					// 用 topic channel 加指数退避 timer 设置 selector 等待
					ch, ok := topicChans[topic]
					if !ok {
						ch = workflow.NewChannel(ctx)
						topicChans[topic] = ch
					}
					sel := workflow.NewSelector(ctx)
					sel.AddReceive(ch, func(c workflow.ReceiveChannel, more bool) {})
					// Exponential backoff to reduce polling frequency
					// 用 topic channel + 指数退避定时器做 selector 等待，降低轮询频率。
					timer := workflow.NewTimer(ctx, backoff)
					sel.AddFuture(timer, func(f workflow.Future) {})
					sel.Select(ctx)
					attempts++

					// Increase backoff up to max
					// 退避时间增大到上限
					backoff = min(backoff*2, maxBackoff)
				}
				if workflow.Now(ctx).Sub(startTime) >= maxWaitTime {
					logger.Warn("Dependency wait timeout", "topic", topic, "wait_time", maxWaitTime, "attempts", attempts)
				}
				// Stream dependency satisfied
				// 流式推送依赖已满足
				if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: workflowID,
					EventType:  activities.StreamEventDependencySatisfied,
					AgentID:    agentName,
					Message:    "Previous step completed",
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil); err != nil {
					logger.Warn("Failed to emit dependency satisfied event", "error", err)
				}
			}
		} else if i < len(decomp.Subtasks) && len(decomp.Subtasks[i].Consumes) > 0 {
			// Log when P2P dependencies exist but P2P is disabled
			// 存在 P2P 依赖但 P2P 未启用时记日志
			logger.Debug("Skipping P2P dependency wait (P2P disabled)",
				"p2p_enabled", p2pConfig.P2PCoordinationEnabled,
				"subtask_id", decomp.Subtasks[i].ID,
				"would_consume", decomp.Subtasks[i].Consumes)
		}

		// P2P demo code removed - use P2PCoordinationEnabled config instead
		// P2P 演示代码已删除 - 改用 P2PCoordinationEnabled 配置

		// Add previous results to context for sequential dependencies
		// 将先前结果加入 context，支持顺序依赖
		if len(childResults) > 0 {
			previousResults := make(map[string]any)
			for j, prevResult := range childResults {
				if j < i && j < len(decomp.Subtasks) {
					resultMap := map[string]any{
						"response":        prevResult.Response,
						"tokens":          prevResult.TokensUsed,
						"success":         prevResult.Success,
						"tools_used":      prevResult.ToolsUsed,
						"tool_executions": sanitizeToolExecutions(prevResult.ToolExecutions),
					}
					// Try to extract numeric value from response (standardize key name)
					// 尝试从 response 中提取数值（统一 key 名）
					if numVal, ok := util.ParseNumericValue(prevResult.Response); ok {
						resultMap["numeric_value"] = numVal
					}
					previousResults[decomp.Subtasks[j].ID] = resultMap
				}
			}
			childCtx["previous_results"] = previousResults
		}

		// Clear tool_parameters for dependent tasks to avoid placeholder issues
		// 对依赖 task 清空 tool_parameters，避免占位符问题
		if len(st.Dependencies) > 0 && st.ToolParameters != nil {
			st.ToolParameters = nil
		}

		// If tool parameters imply a tool but suggested_tools is empty, add it so the agent can use the tool
		// 若 tool 参数暗示某工具但 suggested_tools 为空，则补上该工具，使 agent 可用
		if len(st.SuggestedTools) == 0 && st.ToolParameters != nil {
			if t, ok := st.ToolParameters["tool"].(string); ok && strings.TrimSpace(t) != "" {
				st.SuggestedTools = []string{t}
			}
		}

		// Performance-based agent selection (epsilon-greedy)
		// 基于性能的 agent 选择（epsilon-greedy）
		defaultAgentID := agentName
		availableAgents := []string{defaultAgentID} // TODO: populate from registry 注：待从 registry 填充
		selectedAgent, err := SelectAgentForTask(ctx, st.ID, availableAgents, defaultAgentID)
		if err != nil {
			logger.Warn("Agent selection failed, using default",
				"task_id", st.ID,
				"default_agent", defaultAgentID,
				"error", err)
			selectedAgent = defaultAgentID
		}

		var res activities.AgentExecutionResult
		// Retry loop within the same iteration to avoid relying on range index mutation
		// 在同一轮迭代内重试，避免依赖 range 索引变更
		var execErr error
		execStartTime := workflow.Now(ctx)
		// Prepare fire-and-forget context for persistence activities
		// 为持久化 activities 准备 fire-and-forget context
		persistCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		for {
			// Use budgeted agent when a per-agent budget hint is present
			// 存在按 agent 预算提示时使用带预算的 agent
			agentMax := 0
			if v, ok := childCtx["budget_agent_max"].(int); ok {
				agentMax = v
			}
			if v, ok := childCtx["budget_agent_max"].(float64); ok && v > 0 {
				agentMax = int(v)
			}
			if agentMax > 0 {
				wid := workflowID
				execErr = workflow.ExecuteActivity(ctx, constants.ExecuteAgentWithBudgetActivity, activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            st.Description,
						AgentID:          selectedAgent,
						Context:          childCtx,
						Mode:             input.Mode,
						SessionID:        input.SessionID,
						UserID:           input.UserID,
						History:          historyForAgent,
						SuggestedTools:   st.SuggestedTools,
						ToolParameters:   st.ToolParameters,
						ParentWorkflowID: workflowID,
					},
					MaxTokens: agentMax,
					UserID:    input.UserID,
					TaskID:    wid,
					ModelTier: "medium",
				}).Get(ctx, &res)
			} else {
				execErr = workflow.ExecuteActivity(ctx, activities.ExecuteAgent, activities.AgentExecutionInput{
					Query:            st.Description,
					AgentID:          selectedAgent,
					Context:          childCtx,
					Mode:             input.Mode,
					SessionID:        input.SessionID,
					UserID:           input.UserID,
					History:          historyForAgent,
					SuggestedTools:   st.SuggestedTools,
					ToolParameters:   st.ToolParameters,
					ParentWorkflowID: workflowID,
				}).Get(ctx, &res)
			}
			if execErr == nil {
				// Emit budget usage progress if available
				// 若有预算用量则发出进度
				if agentMax > 0 {
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventProgress,
						AgentID:    agentName,
						Message:    activities.MsgBudget(res.TokensUsed, agentMax),
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
				}
				// Update and emit running total if task budget is known
				// 已知 task 预算时更新并发出累计总量
				totalUsed += res.TokensUsed
				if taskBudget > 0 {
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventProgress,
						AgentID:    "supervisor",
						Message:    activities.MsgBudget(totalUsed, taskBudget),
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
				}
				// Persist agent execution (fire-and-forget)
				// 持久化 agent 执行记录（fire-and-forget）
				// Pre-generate agent execution ID using SideEffect for replay safety
				// 用 SideEffect 预生成 agent 执行 ID，保证 replay 安全
				var agentExecutionID string
				workflow.SideEffect(ctx, func(ctx workflow.Context) any {
					return uuid.New().String()
				}).Get(&agentExecutionID)

				state := "COMPLETED"
				if !res.Success {
					state = "FAILED"
				}

				workflow.ExecuteActivity(
					persistCtx,
					activities.PersistAgentExecutionStandalone,
					activities.PersistAgentExecutionInput{
						ID:         agentExecutionID,
						WorkflowID: workflowID,
						AgentID:    agentName,
						Input:      st.Description,
						Output:     res.Response,
						State:      state,
						TokensUsed: res.TokensUsed,
						ModelUsed:  res.ModelUsed,
						DurationMs: res.DurationMs,
						Error:      res.Error,
						Metadata: map[string]any{
							"workflow": "supervisor",
							"strategy": "supervisor",
							"task_id":  st.ID,
						},
					},
				)

				// Persist tool executions (fire-and-forget)
				// 持久化 tool 执行记录（fire-and-forget）
				if len(res.ToolExecutions) > 0 {
					for _, texec := range res.ToolExecutions {
						outputStr := ""
						switch v := texec.Output.(type) {
						case string:
							outputStr = v
						default:
							if b, err := json.Marshal(v); err == nil {
								outputStr = string(b)
							}
						}
						// Extract input params from tool execution
						// 从 tool 执行记录中提取输入参数
						inputParamsMap, _ := texec.InputParams.(map[string]any)

						workflow.ExecuteActivity(
							persistCtx,
							activities.PersistToolExecutionStandalone,
							activities.PersistToolExecutionInput{
								WorkflowID:       workflowID,
								AgentID:          agentName,
								AgentExecutionID: agentExecutionID,
								ToolName:         texec.Tool,
								InputParams:      inputParamsMap,
								Output:           outputStr,
								Success:          texec.Success,
								TokensConsumed:   0,
								DurationMs:       texec.DurationMs,
								Error:            texec.Error,
								Metadata: map[string]any{
									"workflow": "supervisor",
									"task_id":  st.ID,
								},
							},
						)
					}
				}

				// Record agent performance (fire-and-forget)
				// 记录 agent 性能（fire-and-forget）
				execDuration := workflow.Now(ctx).Sub(execStartTime).Milliseconds()
				workflow.ExecuteActivity(
					persistCtx,
					activities.RecordAgentPerformance,
					activities.RecordAgentPerformanceInput{
						AgentID:    selectedAgent,
						SessionID:  input.SessionID,
						Success:    res.Success,
						TokensUsed: res.TokensUsed,
						DurationMs: execDuration,
						Mode:       input.Mode,
					},
				)
				break
			}
			taskRetries[st.ID]++
			logger.Error("Child SimpleTaskWorkflow failed", "subtask_id", st.ID, "error", execErr, "retry_count", taskRetries[st.ID])

			if taskRetries[st.ID] >= maxRetriesPerTask {
				logger.Error("Task exceeded retry limit, marking as failed", "subtask_id", st.ID, "retries", taskRetries[st.ID])
				failedTasks++
				if failedTasks >= maxFailures {
					logger.Error("Too many subtask failures, aborting workflow", "failed_tasks", failedTasks, "max_failures", maxFailures)
					return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Too many subtask failures (%d/%d)", failedTasks, len(decomp.Subtasks))}, fmt.Errorf("workflow aborted due to excessive failures")
				}
				// Give up on this task and move to the next one
				// 放弃该 task，转入下一个
				execErr = fmt.Errorf("max retries reached")

				// Record failed performance (fire-and-forget)
				// 记录失败的性能结果（fire-and-forget）
				execDuration := workflow.Now(ctx).Sub(execStartTime).Milliseconds()
				workflow.ExecuteActivity(
					persistCtx,
					activities.RecordAgentPerformance,
					activities.RecordAgentPerformanceInput{
						AgentID:    selectedAgent,
						SessionID:  input.SessionID,
						Success:    false,
						TokensUsed: 0, // Failed execution 注：执行失败
						DurationMs: execDuration,
						Mode:       input.Mode,
					},
				)
				break
			}
			// Retry immediately (deterministic). Optionally sleep if desired.
			// 立即重试（确定性）。需要时可自行 sleep。
			logger.Info("Retrying failed task", "subtask_id", st.ID, "retry_count", taskRetries[st.ID])
		}
		if execErr != nil {
			continue
		}
		// Capture agent result for synthesis directly
		// 直接捕获 agent 结果供合成使用
		childResults = append(childResults, res)

		// Produce outputs to workspace per plan
		// 按计划将输出写入 workspace
		if teamWorkspaceVersion != workflow.DefaultVersion &&
			i < len(decomp.Subtasks) && len(decomp.Subtasks[i].Produces) > 0 {
			for _, topic := range decomp.Subtasks[i].Produces {
				var wr activities.WorkspaceAppendResult
				if err := workflow.ExecuteActivity(ctx, constants.WorkspaceAppendActivity, activities.WorkspaceAppendInput{
					WorkflowID: workflowID,
					Topic:      topic,
					Entry:      map[string]any{"subtask_id": st.ID, "summary": res.Response},
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, &wr); err != nil {
					logger.Warn("Failed to append to workspace", "topic", topic, "error", err)
					continue
				}
				lastWSSeq = wr.Seq
				_ = lastWSSeq
				// Notify any selector waiting on this topic (non-blocking)
				// 通知等待该 topic 的所有 selector（非阻塞）
				if ch, ok := topicChans[topic]; ok {
					sel := workflow.NewSelector(ctx)
					sel.AddSend(ch, true, func() {})
					sel.AddDefault(func() {
						logger.Debug("Channel send would block, skipping notification", "topic", topic)
					})
					sel.Select(ctx)
				}
			}
		}
	}

	// Check pause/cancel before synthesis
	// 合成前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_synthesis"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Emit data processing event for synthesis
	// 为合成发出数据处理事件
	if len(childResults) > 1 {
		synthMessage := "Combining results"
		if err := workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventDataProcessing,
			AgentID:    "supervisor",
			Message:    synthMessage,
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil); err != nil {
			logger.Warn("Failed to emit data processing event", "error", err)
		}
	}

	// Synthesize results using configured mode
	// 按配置的模式合成结果
	var synth activities.SynthesisResult

	// Prepare synthesis context with collected citations from child results
	// 准备合成 context，带上从子结果收集的引用
	ctxForSynth := make(map[string]any)
	if input.Context != nil {
		maps.Copy(ctxForSynth, input.Context)
	}
	var collectedCitations []metadata.Citation
	{
		var resultsForCitations []any
		for _, ar := range childResults {
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

		// Apply entity-based citation filtering if canonical name is present
		// 存在规范化名称时，应用基于实体的引用过滤
		if len(citations) > 0 {
			canonicalName, _ := ctxForSynth["canonical_name"].(string)
			if canonicalName != "" {
				// Extract entity hints for filtering
				// 提取用于过滤的实体提示
				var domains []string
				if d, ok := ctxForSynth["official_domains"].([]string); ok {
					domains = d
				}
				var aliases []string
				if eq, ok := ctxForSynth["exact_queries"].([]string); ok {
					aliases = eq
				}

				beforeCount := len(citations)
				logger.Info("Applying citation entity filter (supervisor)",
					"pre_filter_count", beforeCount,
					"canonical_name", canonicalName,
					"official_domains", domains,
					"alias_count", len(aliases),
				)
				citations = strategies.FilterCitationsByEntity(citations, canonicalName, aliases, domains)
				logger.Info("Citation filter completed (supervisor)",
					"before", beforeCount,
					"after", len(citations),
					"removed", beforeCount-len(citations),
					"retention_rate", float64(len(citations))/float64(beforeCount),
				)
			}
		}

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
			ctxForSynth["available_citations"] = strings.TrimRight(b.String(), "\n")
			ctxForSynth["citation_count"] = len(citations)

			// Also store structured citations for SSE emission
			// 同时保存结构化引用，供 SSE 推送
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
			ctxForSynth["citations"] = out
		}
	}

	// Check if the decomposition included a synthesis/summarization subtask
	// This commonly happens when users request specific output formats (e.g., "summarize in Chinese")
	// Following SOTA patterns: if decomposition includes synthesis, use that instead of duplicating
	// 分解结果中常含"总结/合成"subtask（用户要求特定输出格式时尤甚）；
	// 按 SOTA 做法：已有合成 subtask 就直接用其结果，不再重复合成。
	hasSynthesisSubtask := false
	var synthesisTaskIdx int

	for i, subtask := range decomp.Subtasks {
		taskLower := strings.ToLower(subtask.Description)
		// Check if this subtask is a synthesis/summary task
		// 检查该 subtask 是否为合成/总结型 task
		if strings.Contains(taskLower, "synthesize") ||
			strings.Contains(taskLower, "synthesis") ||
			strings.Contains(taskLower, "summarize") ||
			strings.Contains(taskLower, "summary") ||
			strings.Contains(taskLower, "combine") ||
			strings.Contains(taskLower, "aggregate") {
			hasSynthesisSubtask = true
			synthesisTaskIdx = i
			logger.Info("Detected synthesis subtask in decomposition",
				"task_id", subtask.ID,
				"description", subtask.Description,
				"index", i,
			)
		}
	}

	didSynthesisLLM := false

	if input.BypassSingleResult && len(childResults) == 1 && childResults[0].Success {
		// Only bypass if the single result is not raw JSON and role doesn't require formatting
		// 仅当单个结果不是原始 JSON 且 role 不要求格式化时才走捷径
		shouldBypass := true
		trimmed := strings.TrimSpace(childResults[0].Response)
		if strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[") {
			shouldBypass = false
		}

		// Avoid bypass when citations are available; they require synthesis for inline formatting
		// 有引用时不要走捷径；引用需经合成才能生成行内标注
		if shouldBypass && len(collectedCitations) > 0 {
			shouldBypass = false
		}

		if shouldBypass {
			synth = activities.SynthesisResult{FinalResult: childResults[0].Response, TokensUsed: childResults[0].TokensUsed}
		} else {
			// Perform synthesis for JSON-like results or when role requires formatting
			// 类 JSON 结果或 role 要求格式化时执行合成
			logger.Info("Single result requires synthesis (JSON/role formatting)")
			if err := workflow.ExecuteActivity(ctx, activities.SynthesizeResultsLLM, activities.SynthesisInput{
				Query:              input.Query,
				AgentResults:       childResults,
				Context:            ctxForSynth,
				CollectedCitations: collectedCitations,
				ParentWorkflowID:   workflowID,
			}).Get(ctx, &synth); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error()}, err
			}
			didSynthesisLLM = true
		}
	} else if hasSynthesisSubtask && synthesisTaskIdx < len(childResults) && childResults[synthesisTaskIdx].Success && len(collectedCitations) == 0 {
		// Use the synthesis subtask's result as the final result ONLY if:
		// - No citations exist (citations require re-synthesis for inline formatting)
		// - The response is substantial (not a placeholder)
		// - Tokens were consumed (LLM actually ran)
		// 仅当无引用（引用需重新合成才能生成行内标注）、response 足够充实（非占位符）、
		// 且确实消耗了 token（LLM 真的跑过）时，才直接采用合成 subtask 的结果。
		synthesisResult := childResults[synthesisTaskIdx]
		const minSynthesisResponseLen = 100

		if len(strings.TrimSpace(synthesisResult.Response)) >= minSynthesisResponseLen && synthesisResult.TokensUsed > 0 {
			synth = activities.SynthesisResult{
				FinalResult: synthesisResult.Response,
				TokensUsed:  0, // Don't double-count tokens as they're already counted in agent execution 注：不要重复计 token，这些已在 agent 执行中计入
			}
			logger.Info("Using synthesis subtask result as final output",
				"agent_id", synthesisResult.AgentID,
				"response_length", len(synthesisResult.Response),
				"tokens_used", synthesisResult.TokensUsed,
			)
		} else {
			logger.Warn("Synthesis subtask result too short or zero tokens; falling back to LLM synthesis",
				"agent_id", synthesisResult.AgentID,
				"response_length", len(synthesisResult.Response),
				"tokens_used", synthesisResult.TokensUsed,
				"min_required_length", minSynthesisResponseLen,
			)
			if err := workflow.ExecuteActivity(ctx, activities.SynthesizeResultsLLM, activities.SynthesisInput{
				Query:              input.Query,
				AgentResults:       childResults,
				Context:            ctxForSynth,
				CollectedCitations: collectedCitations,
				ParentWorkflowID:   workflowID,
			}).Get(ctx, &synth); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error()}, err
			}
			didSynthesisLLM = true
		}
	} else {
		// Perform synthesis: either no synthesis subtask, or we have citations that need formatting
		// 执行合成：要么没有合成 subtask，要么有引用需要格式化
		if len(collectedCitations) > 0 {
			logger.Info("Re-running synthesis to inject inline citations",
				"citation_count", len(collectedCitations),
			)
		} else {
			logger.Info("Performing standard synthesis of agent results")
		}
		if err := workflow.ExecuteActivity(ctx, activities.SynthesizeResultsLLM, activities.SynthesisInput{
			Query:              input.Query,
			AgentResults:       childResults,
			Context:            ctxForSynth, // Pass role/prompt_params for role-aware synthesis 注：传入 role/prompt_params，支持角色感知的合成
			CollectedCitations: collectedCitations,
			ParentWorkflowID:   workflowID, // For observability correlation 注：用于可观测性关联
		}).Get(ctx, &synth); err != nil {
			return TaskResult{Success: false, ErrorMessage: err.Error()}, err
		}
		didSynthesisLLM = true
	}

	// Citation Agent: add citations to synthesis result (if we have citations)
	// Citation Agent：为合成结果添加引用（若存在引用）
	if len(collectedCitations) > 0 && synth.FinalResult != "" {
		// Step 1: Remove ## Sources section before passing to Citation Agent
		// 步骤 1：交给 Citation Agent 前移除 ## Sources 小节
		// (FormatReportWithCitations in synthesis.go may have added it already)
		// （synthesis.go 的 FormatReportWithCitations 可能已添加过）
		reportForCitation := synth.FinalResult
		var extractedSources string
		if idx := strings.LastIndex(strings.ToLower(reportForCitation), "## sources"); idx != -1 {
			extractedSources = strings.TrimSpace(reportForCitation[idx:])
			reportForCitation = strings.TrimSpace(reportForCitation[:idx])
			logger.Info("CitationAgent: stripped Sources section before processing",
				"sources_length", len(extractedSources),
				"report_length", len(reportForCitation),
			)
		}

		// Convert metadata.Citation to CitationForAgent
		// 将 metadata.Citation 转换为 CitationForAgent
		citationsForAgent := make([]activities.CitationForAgent, 0, len(collectedCitations))
		for _, c := range collectedCitations {
			citationsForAgent = append(citationsForAgent, activities.CitationForAgent{
				URL:              c.URL,
				Title:            c.Title,
				Source:           c.Source,
				Snippet:          c.Snippet,
				CredibilityScore: c.CredibilityScore,
				QualityScore:     c.QualityScore,
			})
		}

		var citationResult activities.CitationAgentResult
		citationCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 180 * time.Second,
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumAttempts:    2,
			},
		})

		// Dynamic model tier: use medium for longer reports (better instruction following)
		// 动态模型档位：较长报告用 medium（指令遵循更好）
		citationModelTier := "small"
		if len(reportForCitation) > 8000 {
			citationModelTier = "medium"
		}

		cerr := workflow.ExecuteActivity(citationCtx, "AddCitations", activities.CitationAgentInput{
			Report:           reportForCitation,
			Citations:        citationsForAgent,
			ParentWorkflowID: workflowID,
			ModelTier:        citationModelTier,
		}).Get(citationCtx, &citationResult)

		if cerr != nil {
			logger.Warn("CitationAgent failed, using original synthesis", "error", cerr)
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventProgress,
				AgentID:    "citation_agent",
				Message:    activities.MsgCitationSkipped(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
		} else if citationResult.ValidationPassed {
			// Use cited report and rebuild Sources with correct Used inline/Additional labels
			// The new report has [n] markers, so FormatReportWithCitations will correctly
			// identify which citations were actually used inline
			// 用带引用的报告重建 Sources 段：新报告含 [n] 标注，
			// FormatReportWithCitations 才能正确区分"行内引用"与"附加来源"。
			citationsList := ""
			if v, ok := ctxForSynth["available_citations"].(string); ok {
				citationsList = v
			}
			if citationsList != "" {
				synth.FinalResult = formatting.FormatReportWithCitations(citationResult.CitedReport, citationsList)
			} else {
				// Fallback: just append the extracted sources
				// 回退：直接追加已提取的 sources
				synth.FinalResult = citationResult.CitedReport
				if extractedSources != "" {
					synth.FinalResult = strings.TrimSpace(synth.FinalResult) + "\n\n" + extractedSources
				}
			}
			synth.TokensUsed += citationResult.TokensUsed
			logger.Info("CitationAgent: citations added and Sources rebuilt",
				"citations_used", len(citationResult.CitationsUsed),
			)
		} else {
			logger.Warn("CitationAgent validation failed, keeping original synthesis",
				"error", citationResult.ValidationError,
			)
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventProgress,
				AgentID:    "citation_agent",
				Message:    activities.MsgCitationSkipped(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
			// Keep original synth.FinalResult (which already has Sources)
			// 保留原始 synth.FinalResult（已含 Sources）
		}
	}

	// Emit final clean LLM_OUTPUT for OpenAI-compatible streaming.
	// 发出最终干净的 LLM_OUTPUT，用于 OpenAI 兼容流式输出。
	// This is the canonical answer after all processing (synthesis + citation).
	// 这是全部处理（合成 + 引用）完成的规范答案。
	// Agent ID "final_output" signals the streamer to always show this content.
	// Agent ID "final_output" 通知 streamer 始终展示该内容。
	if synth.FinalResult != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    synth.FinalResult,
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"tokens_used": synth.TokensUsed,
				"model_used":  synth.ModelUsed,
			},
		}).Get(ctx, nil)
	}

	// Compute total tokens across child results + synthesis
	// 统计子结果加合成的 token 总量
	totalChildTokens := 0
	for _, cr := range childResults {
		totalChildTokens += cr.TokensUsed
	}
	totalTokens := totalChildTokens + synth.TokensUsed

	// Record synthesis token usage (only when LLM synthesis actually ran)
	// 记录合成 token 用量（仅当 LLM 合成真的跑过）
	if didSynthesisLLM && synth.TokensUsed > 0 {
		inTok := synth.InputTokens
		outTok := synth.CompletionTokens
		if inTok == 0 && outTok > 0 {
			est := synth.TokensUsed - outTok
			if est > 0 {
				inTok = est
			}
		}
		recCtx := opts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       input.UserID,
			SessionID:    input.SessionID,
			TaskID:       workflowID,
			AgentID:      "supervisor_synthesis",
			Model:        synth.ModelUsed,
			Provider:     synth.Provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata: map[string]any{
				"phase":    "synthesis",
				"workflow": "supervisor",
			},
		}).Get(recCtx, nil)
	}

	// Update session with token usage (include per-agent usage for accurate cost)
	// 更新 session 的 token 用量（含按 agent 用量，便于准确计费）
	if input.SessionID != "" {
		var sessionUpdateResult activities.SessionUpdateResult
		// Build per-agent usage list (model + tokens)
		// 构建按 agent 的用量列表（model + token）
		usages := make([]activities.AgentUsage, 0, len(childResults))
		for _, cr := range childResults {
			usages = append(usages, activities.AgentUsage{Model: cr.ModelUsed, Tokens: cr.TokensUsed, InputTokens: cr.InputTokens, OutputTokens: cr.OutputTokens})
		}
		err := workflow.ExecuteActivity(ctx,
			constants.UpdateSessionResultActivity,
			activities.SessionUpdateInput{
				SessionID:  input.SessionID,
				Result:     synth.FinalResult,
				TokensUsed: totalTokens,
				AgentsUsed: len(childResults),
				AgentUsage: usages,
			},
		).Get(ctx, &sessionUpdateResult)
		if err != nil {
			logger.Warn("Failed to update session with tokens",
				"session_id", input.SessionID,
				"error", err,
			)
		}
	}

	// Record decomposition results for future learning (fire-and-forget)
	// 记录分解结果，供后续学习（fire-and-forget）
	if supervisorMemoryVersion >= 2 && input.SessionID != "" && len(decomp.Subtasks) > 0 {
		recordCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})

		// Calculate workflow duration
		// 计算 workflow 耗时
		workflowDuration := workflow.Now(ctx).Sub(workflowStartTime).Milliseconds()

		// Extract subtask descriptions
		// 提取 subtask 描述
		subtaskDescriptions := make([]string, len(decomp.Subtasks))
		for i, st := range decomp.Subtasks {
			subtaskDescriptions[i] = st.Description
		}

		// Fire and forget - don't wait for result
		// fire and forget - 不等结果
		workflow.ExecuteActivity(recordCtx, "RecordDecomposition", activities.RecordDecompositionInput{
			SessionID:  input.SessionID,
			Query:      input.Query,
			Subtasks:   subtaskDescriptions,
			Strategy:   decomp.ExecutionStrategy,
			Success:    true,
			DurationMs: workflowDuration,
			TokensUsed: totalTokens,
		})

		logger.Info("Recorded decomposition outcome",
			"strategy", decomp.ExecutionStrategy,
			"subtasks", len(decomp.Subtasks),
			"duration_ms", workflowDuration)
	}

	// Memory extraction is swarm-only — removed from SupervisorWorkflow.
	// 用户记忆抽取仅在 swarm 中执行，SupervisorWorkflow 已移除该步骤。

	// Check pause/cancel before completion
	// 完成前检查 pause/cancel
	if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error()}, err
	}

	// Emit workflow completed event for dashboards
	// 为看板发出 workflow 完成事件
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowCompleted,
		AgentID:    "supervisor",
		Message:    activities.MsgWorkflowCompleted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Aggregate tool errors across child results
	// 汇总各子结果中的 tool 错误
	var toolErrors []map[string]string
	for _, cr := range childResults {
		if len(cr.ToolExecutions) == 0 {
			continue
		}
		for _, te := range cr.ToolExecutions {
			if !te.Success || (te.Error != "") {
				toolErrors = append(toolErrors, map[string]string{
					"agent_id": cr.AgentID,
					"tool":     te.Tool,
					"error":    te.Error,
				})
			}
		}
	}

	// Optional: verify claims if enabled and we have citations
	// 可选：已启用且有引用时校验论断
	var verification activities.VerificationResult
	verifyEnabled := false
	if input.Context != nil {
		if v, ok := input.Context["enable_verification"].(bool); ok {
			verifyEnabled = v
		}
	}
	if verifyEnabled && len(collectedCitations) > 0 {
		var verCitations []any
		for _, c := range collectedCitations {
			m := map[string]any{
				"url":               c.URL,
				"title":             c.Title,
				"source":            c.Source,
				"credibility_score": c.CredibilityScore,
				"quality_score":     c.QualityScore,
			}
			verCitations = append(verCitations, m)
		}
		_ = workflow.ExecuteActivity(ctx, "VerifyClaimsActivity", activities.VerifyClaimsInput{
			Answer:    synth.FinalResult,
			Citations: verCitations,
		}).Get(ctx, &verification)
	}
	meta := map[string]any{
		"num_children": len(childResults),
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
	if verification.TotalClaims > 0 || verification.OverallConfidence > 0 {
		meta["verification"] = verification
	}

	// Aggregate agent metadata (model, provider, tokens, cost)
	// 汇总 agent 元数据（model、provider、token、费用）
	agentMeta := metadata.AggregateAgentMetadata(childResults, synth.TokensUsed)
	maps.Copy(meta, agentMeta)

	// Ensure total_tokens present (fallback to computed workflow total)
	// 确保有 total_tokens（回退为计算出的 workflow 总量）
	if tv, ok := meta["total_tokens"]; !ok || (ok && ((func() int {
		switch x := tv.(type) {
		case int:
			return x
		case float64:
			return int(x)
		default:
			return 0
		}
	})() == 0)) {
		if totalTokens > 0 {
			meta["total_tokens"] = totalTokens
		}
	}

	// Fallbacks for model/provider/cost using tier defaults, but prefer provider override from input.Context
	// model/provider/费用的回退用档位默认值，但优先用 input.Context 里的 provider 覆盖
	tier := deriveModelTier(input.Context)
	providerOverride := ""
	if input.Context != nil {
		if v, ok := input.Context["provider_override"].(string); ok && strings.TrimSpace(v) != "" {
			providerOverride = strings.ToLower(strings.TrimSpace(v))
		} else if v, ok := input.Context["provider"].(string); ok && strings.TrimSpace(v) != "" {
			providerOverride = strings.ToLower(strings.TrimSpace(v))
		} else if v, ok := input.Context["llm_provider"].(string); ok && strings.TrimSpace(v) != "" {
			providerOverride = strings.ToLower(strings.TrimSpace(v))
		}
	}
	if _, ok := meta["model"]; !ok {
		if _, ok2 := meta["model_used"]; !ok2 {
			chosen := ""
			if providerOverride != "" {
				chosen = pricing.GetPriorityModelForProvider(tier, providerOverride)
			}
			if chosen == "" {
				chosen = pricing.GetPriorityOneModel(tier)
			}
			if chosen != "" {
				meta["model"] = chosen
				meta["model_used"] = chosen
			}
		}
	}
	if _, ok := meta["provider"]; !ok || meta["provider"] == "" {
		prov := providerOverride
		if prov == "" {
			if m, okm := meta["model"].(string); okm && m != "" {
				prov = detectProviderFromModel(m)
			}
		}
		if prov == "" || prov == "unknown" {
			prov = pricing.GetPriorityOneProvider(tier)
		}
		if prov != "" {
			meta["provider"] = prov
		}
	}
	if _, ok := meta["cost_usd"]; !ok {
		if m, okm := meta["model"].(string); okm && m != "" {
			tokens := 0
			if tv, ok := meta["total_tokens"]; ok {
				switch x := tv.(type) {
				case int:
					tokens = x
				case float64:
					tokens = int(x)
				}
			}
			if tokens == 0 {
				tokens = totalTokens
			}
			if tokens > 0 {
				meta["cost_usd"] = pricing.CostForTokens(m, tokens)
			}
		}
	}

	return TaskResult{Result: synth.FinalResult, Success: true, TokensUsed: totalTokens, Metadata: meta}, nil
}

// Note: convertToStrategiesInput and convertFromStrategiesResult are defined in orchestrator_router.go
// 说明：convertToStrategiesInput 与 convertFromStrategiesResult 定义在 orchestrator_router.go
