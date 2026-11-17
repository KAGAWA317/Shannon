package patterns

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
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	wopts "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// BrowserConfig controls the browser automation loop behavior
// BrowserConfig 控制浏览器自动化循环的行为
type BrowserConfig struct {
	MaxIterations int // Maximum number of browser action loops 注：浏览器动作循环的最大次数
	ActionTimeout int // Timeout for each action in milliseconds 注：每个动作的超时时间，单位毫秒
}

// CheckPauseFunc is a callback for checking pause/cancel signals during iteration
// CheckPauseFunc 是迭代过程中检查暂停/取消信号的回调
type CheckPauseFunc func(ctx workflow.Context, checkpoint string) error

// BrowserLoopResult contains the results of browser automation
// BrowserLoopResult 包含浏览器自动化的结果
type BrowserLoopResult struct {
	Actions      []string
	Observations []string
	FinalResult  string
	TotalTokens  int
	Iterations   int
	AgentResults []activities.AgentExecutionResult
}

// BrowserLoop executes a unified agent loop for browser automation.
// BrowserLoop 为浏览器自动化执行统一的 agent 循环。
// Unlike ReactLoop which splits REASON and ACT into separate agent calls,
// 与把 REASON 和 ACT 拆成两次独立 agent 调用的 ReactLoop 不同，
// BrowserLoop uses a single agent that reasons and acts together.
// BrowserLoop 使用单个 agent，边推理边执行动作。
//
// This matches modern browser automation approaches (Claude Code, Manus):
// 这与现代浏览器自动化方案（Claude Code、Manus）一致：
// 1. Agent receives: task + current page state + action history
// 1. Agent 收到：task + 当前页面状态 + 动作历史
// 2. Agent reasons about what to do AND executes tools in one call
// 2. Agent 在一次调用中推理该做什么，并执行 tool
// 3. Tool results become observations for next iteration
// 3. tool 的结果成为下一轮的观测
// 4. Repeat until task complete or max iterations
// 4. 重复直到 task 完成或达到最大迭代次数
// 与把 REASON/ACT 拆成两次调用的 ReactLoop 不同，这里用单个 agent 边推理边行动：
// 一次调用里既决定又执行 tool，tool 结果作为下一轮观测，直到 task 完成或达到迭代上限。
func BrowserLoop(
	ctx workflow.Context,
	query string,
	baseContext map[string]any,
	sessionID string,
	history []string,
	config BrowserConfig,
	opts Options,
	checkPause CheckPauseFunc,
) (*BrowserLoopResult, error) {
	logger := workflow.GetLogger(ctx)

	// Set activity options with longer timeout for browser actions
	// 设置 activity options，为浏览器动作留出更长超时
	activityTimeout := 3 * time.Minute
	if config.ActionTimeout > 0 {
		activityTimeout = time.Duration(config.ActionTimeout) * time.Millisecond
	}

	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: activityTimeout,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Initialize state
	// 初始化状态
	var observations []string
	var actions []string
	totalTokens := 0
	iteration := 0

	var agentResults []activities.AgentExecutionResult
	promptVersion := workflow.GetVersion(ctx, "browser_agent_prompt_v2", workflow.DefaultVersion, 1)

	// Get workflow ID for SSE events
	// 获取 workflow ID，用于 SSE 事件
	wfID := workflow.GetInfo(ctx).WorkflowExecution.ID
	if baseContext != nil {
		if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
			wfID = p
		}
	}

	// Emit browser automation started
	// 发出浏览器自动化开始事件
	streaming.Get().Publish(wfID, streaming.Event{
		WorkflowID: wfID,
		Type:       "PROGRESS",
		AgentID:    "browser",
		Message:    activities.MsgBrowserStarted(),
		Timestamp:  workflow.Now(ctx),
	})

	// Main unified agent loop
	// 主循环：统一的 agent 循环
	for iteration < config.MaxIterations {
		// Check for pause/cancel at start of each iteration
		// 每轮迭代开始时检查暂停/取消
		if checkPause != nil {
			if err := checkPause(ctx, fmt.Sprintf("pre_iteration_%d", iteration)); err != nil {
				return &BrowserLoopResult{
					Actions:      actions,
					Observations: observations,
					FinalResult:  "Browser automation paused/cancelled",
					TotalTokens:  totalTokens,
					Iterations:   iteration,
					AgentResults: agentResults,
				}, err
			}
		}

		logger.Info("Browser loop iteration",
			"iteration", iteration+1,
			"observations_count", len(observations),
		)

		agentID := agents.GetAgentName(wfID, iteration)

		// Emit iteration progress
		// 发出迭代进度事件
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "PROGRESS",
			AgentID:    "browser",
			Message:    activities.MsgBrowserAction(iteration+1, config.MaxIterations),
			Timestamp:  workflow.Now(ctx),
		})

		// Emit agent started
		// 发出 agent 开始事件
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "AGENT_STARTED",
			AgentID:    agentID,
			Message:    activities.MsgBrowserAnalyzing(),
			Timestamp:  workflow.Now(ctx),
		})

		// Build unified context for single agent call
		// 为单次 agent 调用构建统一上下文
		agentContext := make(map[string]any)
		maps.Copy(agentContext, baseContext)
		agentContext["query"] = query
		agentContext["iteration"] = iteration
		agentContext["previous_actions"] = actions

		// Include recent observations (truncated to prevent context overflow)
		// 带上最近的观测（已截断，避免上下文溢出）
		recentObs := observations
		if len(recentObs) > 5 {
			recentObs = recentObs[len(recentObs)-5:]
		}
		agentContext["observations"] = recentObs

		// Build the unified prompt that combines reasoning and action
		// 构建把推理与动作合并到一起的统一 prompt
		var agentQuery string
		if promptVersion < 1 {
			agentQuery = buildBrowserAgentPromptV1(query, actions, recentObs, iteration)
		} else {
			agentQuery = buildBrowserAgentPromptV2(query, actions, recentObs, iteration)
		}

		var agentResult activities.AgentExecutionResult
		var err error

		// Browser tools that should be available for this agent
		// 该 agent 可用的浏览器工具
		browserTools := []string{
			"browser",
		}

		// Execute unified agent (reason + act together)
		// 执行统一 agent（推理 + 动作一体）
		if opts.BudgetAgentMax > 0 {
			err = workflow.ExecuteActivity(ctx,
				constants.ExecuteAgentWithBudgetActivity,
				activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            agentQuery,
						AgentID:          agentID,
						Context:          agentContext,
						Mode:             "standard",
						SessionID:        sessionID,
						UserID:           opts.UserID,
						History:          history,
						ParentWorkflowID: wfID,
						SuggestedTools:   browserTools,
					},
					MaxTokens: opts.BudgetAgentMax,
					UserID:    opts.UserID,
					TaskID:    wfID,
					ModelTier: opts.ModelTier,
				}).Get(ctx, &agentResult)
		} else {
			err = workflow.ExecuteActivity(ctx,
				"ExecuteAgent",
				activities.AgentExecutionInput{
					Query:            agentQuery,
					AgentID:          agentID,
					Context:          agentContext,
					Mode:             "standard",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					ParentWorkflowID: wfID,
					SuggestedTools:   browserTools,
				}).Get(ctx, &agentResult)
		}

		if err != nil {
			logger.Error("Browser agent execution failed", "error", err)
			observations = append(observations, fmt.Sprintf("Error: %v", err))
			iteration++
			continue
		}

		// Emit agent completed
		// 发出 agent 完成事件
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       "AGENT_COMPLETED",
			AgentID:    agentID,
			Message:    activities.MsgBrowserCompleted(),
			Timestamp:  workflow.Now(ctx),
		})

		totalTokens += agentResult.TokensUsed
		agentResults = append(agentResults, agentResult)

		// Process response - remove base64 screenshot data to prevent context overflow
		// 处理响应 —— 去掉 base64 截图数据，避免上下文溢出
		if strings.TrimSpace(agentResult.Response) != "" {
			truncatedAction := truncateBase64Images(agentResult.Response)
			actions = append(actions, truncatedAction)

			// Build observation from tool executions (screenshots already emitted via TOOL_OBSERVATION)
			// 从 tool 执行结果构建观测（截图已通过 TOOL_OBSERVATION 发出）
			observation := buildObservationFromTools(agentResult.ToolExecutions)
			if observation != "" {
				observations = append(observations, observation)
			}
		}

		// Limit action/observation history to prevent memory growth
		// 限制动作/观测历史长度，避免内存持续增长
		if len(actions) > 20 {
			actions = actions[len(actions)-20:]
		}
		if len(observations) > 20 {
			observations = observations[len(observations)-20:]
		}

		// Record token usage
		// 记录 token 用量
		if opts.BudgetAgentMax <= 0 {
			recordBrowserTokenUsage(ctx, wfID, sessionID, opts, agentID, agentResult)
		}

		iteration++

		// Check for task completion
		// 检查 task 是否完成
		if isBrowserTaskComplete(agentResult) {
			logger.Info("Browser task completed",
				"iteration", iteration,
				"tool_count", len(agentResult.ToolExecutions),
			)
			break
		}

		// Check for no-progress (agent didn't take any action)
		// 检查是否无进展（agent 没有执行任何动作）
		// Use response pattern detection since tools execute in Python
		// tool 在 Python 侧执行，因此用 response 文本 pattern 判断
		// tool 在 Python 侧执行，因此用 response 文本 pattern 判断"是否毫无动作"。
		if !responseIndicatesToolUse(agentResult.Response) {
			logger.Warn("No tool execution detected in response, may be stuck",
				"iteration", iteration,
			)
			// Give agent 2 more chances before breaking
			// 跳出前再给 agent 两次机会
			if iteration > 2 && !hasRecentToolExecutions(agentResults, 2) {
				logger.Info("No recent tool executions, stopping loop")
				break
			}
		}
	}

	// Emit loop completed
	// 发出循环完成事件
	streaming.Get().Publish(wfID, streaming.Event{
		WorkflowID: wfID,
		Type:       "PROGRESS",
		AgentID:    "browser",
		Message:    activities.MsgBrowserCompleted(),
		Timestamp:  workflow.Now(ctx),
	})

	// Build final result from last agent response
	// 从最后一次 agent 响应构建最终结果
	finalResult := ""
	if len(agentResults) > 0 {
		lastResult := agentResults[len(agentResults)-1]
		finalResult = truncateBase64Images(lastResult.Response)
	}

	// If no useful final result, synthesize from observations
	// 若没有可用的最终结果，则由观测合成
	if strings.TrimSpace(finalResult) == "" && len(observations) > 0 {
		finalResult = fmt.Sprintf("Browser automation completed after %d iterations. Final observations:\n%s",
			iteration, strings.Join(observations[max(0, len(observations)-3):], "\n"))
	}

	return &BrowserLoopResult{
		Actions:      actions,
		Observations: observations,
		FinalResult:  finalResult,
		TotalTokens:  totalTokens,
		Iterations:   iteration,
		AgentResults: agentResults,
	}, nil
}

func buildBrowserAgentPromptV1(query string, actions []string, observations []string, iteration int) string {
	var sb strings.Builder

	sb.WriteString("You are a browser automation agent. Analyze the current page state and take the next action to complete the task.\n\n")
	fmt.Fprintf(&sb, "TASK: %s\n\n", query)

	if len(actions) > 0 {
		sb.WriteString("PREVIOUS ACTIONS:\n")
		for i, a := range actions {
			// Only show last 5 actions to save context
			// 只展示最近 5 个动作，节省上下文
			if i >= len(actions)-5 {
				fmt.Fprintf(&sb, "- %s\n", truncateString(a, 200))
			}
		}
		sb.WriteString("\n")
	}

	if len(observations) > 0 {
		sb.WriteString("CURRENT STATE (from previous tool results):\n")
		for _, obs := range observations {
			fmt.Fprintf(&sb, "- %s\n", truncateString(obs, 300))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("INSTRUCTIONS:\n")
	sb.WriteString("1. Decide what action to take next to progress toward the goal\n")
	sb.WriteString("2. Use the browser tool with the appropriate action parameter (e.g. browser(action=\"navigate\", url=\"...\"), browser(action=\"click\", selector=\"...\"), browser(action=\"screenshot\"))\n")
	sb.WriteString("3. If the task is complete, summarize what was accomplished\n")
	sb.WriteString("4. If you need to see the current page, use browser(action=\"screenshot\")\n")
	sb.WriteString("5. Keep your response concise - focus on the action, not lengthy explanations\n\n")

	return sb.String()
}

func buildBrowserAgentPromptV2(query string, actions []string, observations []string, iteration int) string {
	var sb strings.Builder

	sb.WriteString("You are a browser automation agent. Analyze the current page state and take the next action to complete the task.\n\n")
	fmt.Fprintf(&sb, "TASK: %s\n\n", query)

	if len(actions) > 0 {
		sb.WriteString("PREVIOUS ACTIONS:\n")
		for i, a := range actions {
			// Only show last 5 actions to save context
			// 只展示最近 5 个动作，节省上下文
			if i >= len(actions)-5 {
				fmt.Fprintf(&sb, "- %s\n", truncateString(a, 200))
			}
		}
		sb.WriteString("\n")
	}

	if len(observations) > 0 {
		sb.WriteString("CURRENT STATE (from previous tool results):\n")
		for _, obs := range observations {
			fmt.Fprintf(&sb, "- %s\n", truncateString(obs, 300))
		}
		sb.WriteString("\n")
	}

	sb.WriteString("INSTRUCTIONS:\n")
	sb.WriteString("1. Decide what action to take next to progress toward the goal\n")
	sb.WriteString("2. Use the browser tool with the appropriate action (e.g. browser(action=\"navigate\", url=\"...\"), browser(action=\"extract\", selector=\"...\"), browser(action=\"click\", selector=\"...\"))\n")
	sb.WriteString("3. If you need page content (for reading/summarizing), use browser(action=\"extract\") — browser(action=\"navigate\") only returns url/title\n")
	sb.WriteString("4. If the task is complete, summarize what was accomplished and say \"Task completed\"\n")
	sb.WriteString("5. Keep your response concise - focus on the action, not lengthy explanations\n\n")

	if iteration == 0 {
		sb.WriteString("This is the first iteration. Start by navigating to the target page.\n")
	} else if iteration == 1 {
		sb.WriteString("If the page is loaded, use browser(action=\"extract\") to get the content before summarizing.\n")
	}

	return sb.String()
}

// buildObservationFromTools creates a compact observation string from tool executions
// buildObservationFromTools 从 tool 执行结果构建紧凑的观测字符串
func buildObservationFromTools(toolExecs []activities.ToolExecution) string {
	if len(toolExecs) == 0 {
		return ""
	}

	var parts []string
	for _, te := range toolExecs {
		if te.Success {
			output, _ := te.Output.(map[string]any)
			action := ""
			if output != nil {
				if a, ok := output["action"].(string); ok {
					action = a
				}
			}

			switch action {
			case "navigate":
				if output != nil {
					title := output["title"]
					url := output["url"]
					parts = append(parts, fmt.Sprintf("Navigated to: %s (%s)", title, url))
				} else {
					parts = append(parts, "Navigation completed")
				}
			case "click":
				parts = append(parts, "Click action completed")
			case "type":
				parts = append(parts, "Text input completed")
			case "screenshot":
				parts = append(parts, "Screenshot captured (see UI)")
			case "extract":
				if output != nil {
					if content, ok := output["content"].(string); ok {
						parts = append(parts, fmt.Sprintf("Extracted: %s", truncateString(content, 500)))
					} else {
						parts = append(parts, "Content extracted")
					}
				}
			case "scroll":
				parts = append(parts, "Scrolled")
			case "wait":
				parts = append(parts, "Wait completed")
			case "close":
				parts = append(parts, "Browser session closed")
			default:
				parts = append(parts, fmt.Sprintf("%s: success", te.Tool))
			}
		} else {
			parts = append(parts, fmt.Sprintf("%s failed: %s", te.Tool, truncateString(te.Error, 100)))
		}
	}

	return strings.Join(parts, "; ")
}

// isBrowserTaskComplete checks if the browser task should be considered complete
// isBrowserTaskComplete 判断浏览器 task 是否应视为已完成
func isBrowserTaskComplete(result activities.AgentExecutionResult) bool {
	response := strings.ToLower(result.Response)

	// Check for completion phrases - agent explicitly stating task is done
	// 检查完成短语 —— agent 明确表示 task 已完成
	completionPhrases := []string{
		"task complete",
		"task completed",
		"successfully completed",
		"all steps completed",
		"i have completed",
		"have been completed",
	}

	for _, phrase := range completionPhrases {
		if strings.Contains(response, phrase) {
			return true
		}
	}

	// NOTE: We do NOT check for hasToolExecution here because:
	// 注意：这里刻意不检查 hasToolExecution，原因：
	// 1. Tools are executed in Python LLM service, not always tracked in Go struct
	// 1. tool 在 Python LLM 服务中执行，未必记入 Go 结构体
	// 2. The agent might just be reporting tool results, not signaling completion
	// 2. agent 可能只是在汇报 tool 结果，并非宣告完成
	// 这里刻意不检查 hasToolExecution：① tool 在 Python LLM 服务中执行，未必记入 Go 结构体；
	// ② agent 可能只是在汇报 tool 结果，并非宣告完成。
	// 3. Stuck detection is handled separately via hasRecentToolExecutions
	// 3. 卡死检测由 hasRecentToolExecutions 单独处理

	return false
}

// hasToolExecution checks if any tools were executed
// hasToolExecution 检查是否有 tool 被执行
// Note: This checks the Go struct, but tools executed in Python may not be tracked here.
// 注意：这里只检查 Go 结构体，Python 侧执行的 tool 可能未被记录。
// Use responseIndicatesToolUse for more reliable detection.
// 更可靠的判断请用 responseIndicatesToolUse。
// 只检查 Go 结构体，Python 侧执行的 tool 可能未被记录；更可靠的判断请用 responseIndicatesToolUse。
func hasToolExecution(toolExecs []activities.ToolExecution) bool {
	return len(toolExecs) > 0
}

// responseIndicatesToolUse checks if the response contains patterns indicating a browser tool was used
// responseIndicatesToolUse 检查 response 中是否包含表示使用了浏览器 tool 的 pattern
func responseIndicatesToolUse(response string) bool {
	toolPatterns := []string{
		"browser result",
		"action=\"navigate\"",
		"action=\"click\"",
		"action=\"extract\"",
		"action=\"screenshot\"",
		"navigated to",
		"clicked on",
		"screenshot captured",
		"url\":",
		"title\":",
	}

	responseLower := strings.ToLower(response)
	for _, pattern := range toolPatterns {
		if strings.Contains(responseLower, strings.ToLower(pattern)) {
			return true
		}
	}
	return false
}

// hasRecentToolExecutions checks if tools were executed in recent iterations
// hasRecentToolExecutions 检查最近几轮迭代中是否有 tool 被执行
func hasRecentToolExecutions(results []activities.AgentExecutionResult, lookback int) bool {
	start := max(len(results)-lookback, 0)
	for i := start; i < len(results); i++ {
		// Check Go struct first
		// 先检查 Go 结构体
		if hasToolExecution(results[i].ToolExecutions) {
			return true
		}
		// Also check response for tool execution patterns (for Python-executed tools)
		// 再检查 response 中的 tool 执行 pattern（针对 Python 侧执行的 tool）
		if responseIndicatesToolUse(results[i].Response) {
			return true
		}
	}
	return false
}

// truncateString truncates a string to maxLen chars with ellipsis
// truncateString 把字符串截断到 maxLen 个字符并加省略号
func truncateString(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-3] + "..."
}

// recordBrowserTokenUsage records token usage for browser automation
// recordBrowserTokenUsage 记录浏览器自动化的 token 用量
func recordBrowserTokenUsage(ctx workflow.Context, wfID, sessionID string, opts Options, agentID string, result activities.AgentExecutionResult) {
	inTok := result.InputTokens
	outTok := result.OutputTokens
	if inTok == 0 && outTok == 0 && result.TokensUsed > 0 {
		inTok = result.TokensUsed * 6 / 10
		outTok = result.TokensUsed - inTok
	}

	model := result.ModelUsed
	if strings.TrimSpace(model) == "" {
		if m := pricing.GetPriorityOneModel(opts.ModelTier); m != "" {
			model = m
		}
	}
	provider := result.Provider
	if strings.TrimSpace(provider) == "" {
		provider = imodels.DetectProvider(model)
	}

	recCtx := wopts.WithTokenRecordOptions(ctx)
	_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
		UserID:       opts.UserID,
		SessionID:    sessionID,
		TaskID:       wfID,
		AgentID:      agentID,
		Model:        model,
		Provider:     provider,
		InputTokens:  inTok,
		OutputTokens: outTok,
		Metadata:     map[string]any{"phase": "browser_action"},
	}).Get(recCtx, nil)
	wopts.RecordToolCostEntries(ctx, result, opts.UserID, sessionID, wfID)
}
