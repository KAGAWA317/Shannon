package activities

import (
	"fmt"
	"strings"
)

// compactTokens formats a token count into a compact human form, e.g. 14800 -> "14.8k".
// compactTokens 把 token 数格式化成紧凑的可读形式，如 14800 -> "14.8k"。
func compactTokens(tokens int) string {
	if tokens < 1000 {
		return fmt.Sprintf("%d", tokens)
	}
	// one decimal precision for thousands
	// 千位保留一位小数
	k := float64(tokens) / 1000.0
	// Trim trailing .0
	// 去掉末尾的 .0
	s := fmt.Sprintf("%.1fk", k)
	s = strings.ReplaceAll(s, ".0k", "k")
	return s
}

// msgTruncate shortens a string for display, adding ellipsis if needed.
// msgTruncate 把字符串截短用于展示，必要时加省略号。
func msgTruncate(s string, maxLen int) string {
	return truncateQuery(s, maxLen)
}

// -----------------------------------------------------------------------------
// Workflow Lifecycle Messages
// 工作流生命周期消息
// -----------------------------------------------------------------------------

// MsgWorkflowStarted announces workflow initialization.
// MsgWorkflowStarted 宣告 workflow 开始初始化。
func MsgWorkflowStarted() string { return "Starting up" }

// MsgWorkflowRouting indicates routing to a specific workflow type.
// MsgWorkflowRouting 表示正在路由到某个具体的 workflow 类型。
func MsgWorkflowRouting() string {
	return "Planning the approach"
}

// MsgWorkflowCompleted announces successful workflow completion.
// MsgWorkflowCompleted 宣告 workflow 成功完成。
func MsgWorkflowCompleted() string { return "All done" }

// MsgStreamEnd marks the end of streaming.
// MsgStreamEnd 标记流式输出结束。
func MsgStreamEnd() string { return "Done" }

// -----------------------------------------------------------------------------
// Task Processing Messages
// 任务处理消息
// -----------------------------------------------------------------------------

// MsgThinking shows the system is analyzing the query.
// MsgThinking 表示系统正在分析 query。
func MsgThinking(query string) string {
	truncated := msgTruncate(query, 50)
	return fmt.Sprintf("Thinking about: %s", truncated)
}

// MsgProcessing indicates active processing.
// MsgProcessing 表示正在处理中。
func MsgProcessing() string { return "Working on it" }

// MsgTaskDone indicates task completion.
// MsgTaskDone 表示任务已完成。
func MsgTaskDone() string { return "Done" }

// MsgTaskFailed reports task failure with reason.
// MsgTaskFailed 上报任务失败及原因。
func MsgTaskFailed(reason string) string {
	if reason == "" {
		return "Something went wrong"
	}
	// Truncate long error messages
	// 截断过长的错误消息
	if len(reason) > 100 {
		reason = reason[:100] + "..."
	}
	return fmt.Sprintf("Hit an issue: %s", reason)
}

// -----------------------------------------------------------------------------
// Planning & Decomposition Messages
// 规划与拆解消息
// -----------------------------------------------------------------------------

// MsgPlanCreated announces plan creation with step count.
// MsgPlanCreated 宣告计划已创建，并带上步骤数。
func MsgPlanCreated(steps int) string {
	if steps == 1 {
		return "Got a plan — 1 step"
	}
	return fmt.Sprintf("Got a plan — %d steps", steps)
}

// MsgUnderstandingRequest indicates decomposition is in progress.
// MsgUnderstandingRequest 表示正在拆解请求。
func MsgUnderstandingRequest() string { return "Understanding your request" }

// MsgDecompositionFailed indicates fallback to simple execution.
// MsgDecompositionFailed 表示已回退到简单执行。
func MsgDecompositionFailed() string { return "Handling this directly" }

// MsgRoleAssigned announces role assignment with available tools.
// MsgRoleAssigned 宣告角色分配及可用工具。
func MsgRoleAssigned(role string) string {
	return fmt.Sprintf("Ready as %s", role)
}

// -----------------------------------------------------------------------------
// Agent Messages
// Agent 消息
// -----------------------------------------------------------------------------

// MsgContextPreparing returns a short status for context preparation.
// MsgContextPreparing 返回上下文准备阶段的简短状态。
func MsgContextPreparing(_ int, _ int) string { return "Gathering context" }

// MsgAgentRunning announces an agent role starting work.
// MsgAgentRunning 宣告某个 agent 角色开始工作。
func MsgAgentRunning(role string) string {
	if role == "" {
		return "Agent working"
	}
	return fmt.Sprintf("%s is on it", role)
}

// MsgAgentThinking indicates agent is analyzing.
// MsgAgentThinking 表示 agent 正在分析。
func MsgAgentThinking() string { return "Thinking" }

// MsgAgentWorking indicates agent is actively working.
// MsgAgentWorking 表示 agent 正在实际干活。
func MsgAgentWorking(agentName string) string {
	if agentName == "" {
		return "Working"
	}
	return fmt.Sprintf("%s working", agentName)
}

// -----------------------------------------------------------------------------
// Progress Messages
// -----------------------------------------------------------------------------

// MsgProgress reports step progress.
// MsgProgress 上报步骤进度，step/total 越界时会先夹到合法区间。
func MsgProgress(step, total int) string {
	if total <= 0 {
		total = 1
	}
	if step < 0 {
		step = 0
	}
	if step > total {
		step = total
	}
	return fmt.Sprintf("Step %d of %d done", step, total)
}

// MsgBudget reports a simple budget usage.
// MsgBudget 上报简单的预算使用情况。
func MsgBudget(used, limit int) string {
	if limit <= 0 {
		return "Processing"
	}
	pct := (used * 100) / limit
	return fmt.Sprintf("Progress: %d%% of budget used", pct)
}

// MsgTokensUsed reports token consumption.
// MsgTokensUsed 上报 token 消耗。
func MsgTokensUsed() string {
	return "Processing complete"
}

// -----------------------------------------------------------------------------
// Handoff Messages
// Handoff 消息
// -----------------------------------------------------------------------------

// MsgHandoffTemplate announces handoff to a template workflow.
// MsgHandoffTemplate 宣告交接给 template workflow。
func MsgHandoffTemplate(templateName string) string {
	return fmt.Sprintf("Running workflow: %s", templateName)
}

// MsgHandoffSimple announces handoff to simple task workflow.
// MsgHandoffSimple 宣告交接给简单任务 workflow。
func MsgHandoffSimple() string { return "Processing" }

// MsgHandoffSupervisor announces handoff to supervisor workflow.
// MsgHandoffSupervisor 宣告交接给 supervisor workflow。
func MsgHandoffSupervisor() string { return "Coordinating agents" }

// MsgHandoffTeamPlan announces handoff to team plan workflow.
// MsgHandoffTeamPlan 宣告交接给 team plan workflow。
func MsgHandoffTeamPlan() string { return "Running team plan" }

// -----------------------------------------------------------------------------
// Synthesis Messages
// 综合（synthesis）消息
// -----------------------------------------------------------------------------

// MsgCompressionApplied notes that trimming was applied.
// MsgCompressionApplied 说明已执行裁剪。
func MsgCompressionApplied() string { return "Condensing the conversation" }

// MsgCombiningResults announces synthesis.
// MsgCombiningResults 宣告开始综合结果。
func MsgCombiningResults() string { return "Pulling it all together" }

// MsgSynthesizing indicates synthesis in progress.
// MsgSynthesizing 表示正在综合。
func MsgSynthesizing() string { return "Combining results" }

// MsgFinalAnswer announces the final answer is ready.
// MsgFinalAnswer 宣告最终答案已就绪。
func MsgFinalAnswer() string { return "Answer ready" }

// MsgSynthesisSummary reports synthesis completion with token count.
// MsgSynthesisSummary 上报综合完成，并带上 token 数。
func MsgSynthesisSummary() string {
	return "Wrapping up"
}

// MsgSynthesisFallback warns that synthesis used a simpler approach.
// MsgSynthesisFallback 提示综合走了更简单的兜底方式。
func MsgSynthesisFallback(_ string) string {
	return "Summarizing results"
}

// -----------------------------------------------------------------------------
// Memory Messages
// 记忆消息
// -----------------------------------------------------------------------------

// MsgWaiting reports a generic waiting status.
// MsgWaiting 上报通用的等待状态。
func MsgWaiting(_ string) string { return "Waiting on previous step" }

// MsgMemoryRecalled reports number of memory items recalled.
// MsgMemoryRecalled 上报召回的记忆条数。
func MsgMemoryRecalled(items int) string {
	if items == 1 {
		return "Recalled 1 past note"
	}
	return fmt.Sprintf("Recalled %d past notes", items)
}

// MsgSummaryAdded indicates a previous context summary was injected.
// MsgSummaryAdded 表示已注入先前的上下文摘要。
func MsgSummaryAdded() string { return "Loaded prior context" }

// -----------------------------------------------------------------------------
// Approval Messages
// 审批消息
// -----------------------------------------------------------------------------

// MsgApprovalRequested indicates approval is needed.
// MsgApprovalRequested 表示需要审批。
func MsgApprovalRequested(reason, approvalID string) string {
	if reason != "" {
		return fmt.Sprintf("Needs approval: %s", reason)
	}
	return "Needs your approval"
}

// MsgApprovalProcessed reports approval decision.
// MsgApprovalProcessed 上报审批决定。
func MsgApprovalProcessed(decision string) string {
	switch strings.ToLower(decision) {
	case "approved":
		return "Approved — continuing"
	case "denied", "rejected":
		return "Denied — stopping"
	default:
		return fmt.Sprintf("Approval %s", decision)
	}
}

// -----------------------------------------------------------------------------
// Supervisor Messages
// Supervisor 消息
// -----------------------------------------------------------------------------

// MsgSupervisorStarted announces supervisor workflow start.
// MsgSupervisorStarted 宣告 supervisor workflow 启动。
func MsgSupervisorStarted() string { return "Coordinating agents" }

// MsgSubtaskProgress reports subtask completion progress.
// MsgSubtaskProgress 上报子任务完成进度。
func MsgSubtaskProgress(completed, total int) string {
	return fmt.Sprintf("%d of %d tasks done", completed, total)
}

// MsgSubtasksFailed reports too many subtask failures.
// MsgSubtasksFailed 上报子任务失败过多。
func MsgSubtasksFailed(failed, total int) string {
	return fmt.Sprintf("Too many failures (%d/%d) — stopping", failed, total)
}

// -----------------------------------------------------------------------------
// React Pattern Messages
// React 模式消息
// -----------------------------------------------------------------------------

// MsgReactIteration reports React loop progress.
// MsgReactIteration 上报 React 循环进度。
func MsgReactIteration(current, max int) string {
	return fmt.Sprintf("Thinking step %d of %d", current, max)
}

// MsgReactReasoning indicates reasoning phase started.
// MsgReactReasoning 表示推理阶段开始。
func MsgReactReasoning() string { return "Reflecting on progress" }

// MsgReactReasoningDone indicates reasoning phase completed.
// MsgReactReasoningDone 表示推理阶段已完成。
func MsgReactReasoningDone() string { return "Decided next step" }

// MsgReactActing indicates action phase started.
// MsgReactActing 表示行动阶段开始。
func MsgReactActing() string { return "Taking action" }

// MsgReactActingDone indicates action phase completed.
// MsgReactActingDone 表示行动阶段已完成。
func MsgReactActingDone() string { return "Action done" }

// MsgReactLoopDone indicates the React loop finished.
// MsgReactLoopDone 表示 React 循环已结束。
func MsgReactLoopDone() string { return "Finished thinking" }

// -----------------------------------------------------------------------------
// Tool Observation Messages
// 工具观测消息
// -----------------------------------------------------------------------------

// MsgToolCompleted generates a human-readable message for tool completion.
// MsgToolCompleted 为工具完成生成一条可读消息。
func MsgToolCompleted(toolName string, response string) string {
	if response == "" {
		return fmt.Sprintf("%s done", humanizeToolName(toolName))
	}

	// Extract first meaningful line or sentence
	// 取第一行有意义的内容作为摘要。
	summary := extractSummary(response, 80)
	if summary == "" {
		return fmt.Sprintf("%s done", humanizeToolName(toolName))
	}

	return fmt.Sprintf("%s: %s", humanizeToolName(toolName), summary)
}

// MsgToolFailed generates a human-readable message for tool failure.
// MsgToolFailed 为工具失败生成一条可读消息。
func MsgToolFailed(toolName string) string {
	return fmt.Sprintf("%s failed", humanizeToolName(toolName))
}

// humanizeToolName converts tool_name to "Tool Name" format.
// humanizeToolName 把 tool_name 转成 "Tool Name" 这种可读形式。
func humanizeToolName(toolName string) string {
	switch toolName {
	case "web_search":
		return "Search"
	case "web_fetch":
		return "Fetch"
	case "calculator":
		return "Calculator"
	case "python_code", "python_executor", "code_executor":
		return "Code"
	case "read_file", "file_reader", "file_read":
		return "File read"
	case "file_write":
		return "File write"
	case "file_list":
		return "File list"
	case "code_reader":
		return "Code review"
	case "ga4_run_report":
		return "GA4 report"
	case "yahoo_jp_ads":
		return "Yahoo ads lookup"
	case "meta_ad_library":
		return "Meta ads lookup"
	case "ads_competitor_discover":
		return "Competitor ads search"
	case "ads_transparency_search":
		return "Ads transparency search"
	case "page_screenshot":
		return "Taking screenshot"
	case "browser":
		return "Browsing"
	default:
		return strings.Title(strings.ReplaceAll(toolName, "_", " "))
	}
}

// HumanizeToolName is the exported version of humanizeToolName.
// HumanizeToolName 是 humanizeToolName 的导出版本。
func HumanizeToolName(toolName string) string {
	return humanizeToolName(toolName)
}

// -----------------------------------------------------------------------------
// Swarm Workflow Messages
// Swarm workflow 消息
// -----------------------------------------------------------------------------

// MsgSwarmStarted announces swarm workflow initialization.
// MsgSwarmStarted 宣告 swarm workflow 初始化。
func MsgSwarmStarted() string { return "Assembling the team" }

// MsgSwarmPlanning announces task decomposition.
// MsgSwarmPlanning 宣告任务拆解。
func MsgSwarmPlanning() string { return "Thinking about the approach..." }

// MsgSwarmSpawning announces agents being assigned.
// MsgSwarmSpawning 宣告正在分派 agent。
func MsgSwarmSpawning(count int) string {
	if count == 1 {
		return "1 agent is on it"
	}
	return fmt.Sprintf("%d agents are on it", count)
}

// MsgSwarmMonitoring announces agent coordination phase.
// MsgSwarmMonitoring 宣告进入 agent 协同阶段。
func MsgSwarmMonitoring() string { return "Agents working in parallel" }

// MsgSwarmSynthesizing announces result combination.
// MsgSwarmSynthesizing 宣告开始合并结果。
func MsgSwarmSynthesizing(count int) string {
	return fmt.Sprintf("Combining findings from %d agents", count)
}

// MsgSwarmCompleted announces swarm completion.
// MsgSwarmCompleted 宣告 swarm 完成。
func MsgSwarmCompleted() string { return "All done" }

// MsgAgentStarted announces an agent beginning work.
// MsgAgentStarted 宣告某个 agent 开始工作。
func MsgAgentStarted(agentName string) string {
	return fmt.Sprintf("%s is on it", agentName)
}

// MsgAgentCompleted announces an agent finishing work.
// MsgAgentCompleted 宣告某个 agent 完成工作。
func MsgAgentCompleted(agentName string) string {
	return fmt.Sprintf("%s is done", agentName)
}

// humanizeAction converts a raw action name to user-friendly text.
// humanizeAction 把原始 action 名转成对用户友好的文案。
func humanizeAction(action string) string {
	switch action {
	case "tool_call":
		return "researching"
	case "done":
		return "wrapping up"
	case "send_message":
		return "coordinating with team"
	case "publish_data":
		return "reporting back"
	case "idle":
		return "waiting for next task"
	case "request_help":
		return "requesting help"
	default:
		// For tool-specific actions like "web_search", "file_write"
		// tool 类动作（如 "web_search"、"file_write"）交给 humanizeToolName 处理。
		return humanizeToolName(action)
	}
}

// MsgAgentProgress reports agent iteration progress in UX-friendly terms.
// MsgAgentProgress 用体验友好的措辞上报 agent 迭代进度。
func MsgAgentProgress(agentName string, step, total int, action string) string {
	friendly := humanizeAction(action)
	return fmt.Sprintf("%s — %s (%d/%d)", agentName, friendly, step, total)
}

// -----------------------------------------------------------------------------
// Citation Messages
// 引用消息
// -----------------------------------------------------------------------------

// MsgCitationSkipped reports citation enrichment was skipped.
// MsgCitationSkipped 上报跳过了引用补充。
func MsgCitationSkipped() string { return "Skipping source attribution" }

// -----------------------------------------------------------------------------
// Browser Pattern Messages
// Browser 模式消息
// -----------------------------------------------------------------------------

// MsgBrowserStarted announces browser automation start.
// MsgBrowserStarted 宣告浏览器自动化开始。
func MsgBrowserStarted() string { return "Opening browser" }

// MsgBrowserAction reports browser iteration progress.
// MsgBrowserAction 上报浏览器操作迭代进度。
func MsgBrowserAction(step, total int) string {
	return fmt.Sprintf("Browser action %d of %d", step, total)
}

// MsgBrowserAnalyzing indicates page analysis.
// MsgBrowserAnalyzing 表示正在分析页面。
func MsgBrowserAnalyzing() string { return "Analyzing page" }

// MsgBrowserCompleted announces browser automation completion.
// MsgBrowserCompleted 宣告浏览器自动化完成。
func MsgBrowserCompleted() string { return "Browser task done" }

// -----------------------------------------------------------------------------
// Research Messages
// 调研消息
// -----------------------------------------------------------------------------

// MsgResearchConfirmed announces research direction confirmed by user.
// MsgResearchConfirmed 宣告用户已确认调研方向。
func MsgResearchConfirmed() string { return "Got it — starting research" }

// MsgResearchTimedOut announces research plan review timed out.
// MsgResearchTimedOut 宣告调研方案审阅已超时。
func MsgResearchTimedOut() string { return "Review timed out" }

// extractSummary extracts a brief summary from response text.
// extractSummary 从 response 文本中提取一句简短摘要。
func extractSummary(text string, maxLen int) string {
	// Clean up the text
	// 先清理文本。
	text = strings.TrimSpace(text)
	if text == "" {
		return ""
	}

	// Try to get first line that's not empty or a header
	// 找第一行非空、且不是标题的内容。
	lines := strings.SplitSeq(text, "\n")
	for line := range lines {
		line = strings.TrimSpace(line)
		// Skip empty lines, headers, and JSON-like content
		// 跳过空行、标题行与类 JSON 内容。
		if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "{") {
			continue
		}
		// Found a good line
		// 找到可用的一行。
		return truncateQuery(line, maxLen)
	}

	// Fallback: truncate the whole text
	// 兜底：直接截断整段文本。
	return truncateQuery(text, maxLen)
}

// -----------------------------------------------------------------------------
// Workflow Control Messages (pause/resume/cancel)
// Workflow 控制消息（pause/resume/cancel）
// -----------------------------------------------------------------------------

// MsgWorkflowPausing formats a message when pause signal is received.
// MsgWorkflowPausing 在收到 pause 信号时拼装消息。
func MsgWorkflowPausing(reason string) string {
	if reason == "" {
		return "Pausing..."
	}
	return fmt.Sprintf("Pausing: %s", reason)
}

// MsgWorkflowPaused formats a message when workflow is blocked at checkpoint.
// MsgWorkflowPaused 在 workflow 于 checkpoint 被阻塞时拼装消息。
func MsgWorkflowPaused() string {
	return "Paused"
}

// MsgWorkflowResumed formats a message when workflow resumes.
// MsgWorkflowResumed 在 workflow 恢复时拼装消息。
func MsgWorkflowResumed(reason string) string {
	if reason == "" {
		return "Resuming"
	}
	return fmt.Sprintf("Resuming: %s", reason)
}

// MsgWorkflowCancelling formats a message when cancel signal is received.
// MsgWorkflowCancelling 在收到 cancel 信号时拼装消息。
func MsgWorkflowCancelling(reason string) string {
	if reason == "" {
		return "Cancelling..."
	}
	return fmt.Sprintf("Cancelling: %s", reason)
}

// MsgWorkflowCancelled formats a message when workflow is cancelled at checkpoint.
// MsgWorkflowCancelled 在 workflow 于 checkpoint 被取消时拼装消息。
func MsgWorkflowCancelled(reason string) string {
	if reason == "" {
		return "Cancelled"
	}
	return fmt.Sprintf("Cancelled: %s", reason)
}
