package activities

import (
	"context"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"go.temporal.io/sdk/activity"
)

// StreamEventType is a minimal set of event types for streaming_v1
// StreamEventType 是 streaming_v1 的最小 event 类型集合
type StreamEventType string

const (
	StreamEventWorkflowStarted   StreamEventType = "WORKFLOW_STARTED"
	StreamEventWorkflowCompleted StreamEventType = "WORKFLOW_COMPLETED"
	StreamEventAgentStarted      StreamEventType = "AGENT_STARTED"
	StreamEventAgentCompleted    StreamEventType = "AGENT_COMPLETED"
	StreamEventErrorOccurred     StreamEventType = "ERROR_OCCURRED"
	StreamEventMessageSent       StreamEventType = "MESSAGE_SENT"
	StreamEventMessageReceived   StreamEventType = "MESSAGE_RECEIVED"
	StreamEventWorkspaceUpdated  StreamEventType = "WORKSPACE_UPDATED"
	StreamEventTaskListUpdated   StreamEventType = "TASKLIST_UPDATED"
	// Extended types (emitted when corresponding gates are enabled)
	// 扩展 event 类型（对应 gate 打开时才会发出）。
	StreamEventTeamRecruited       StreamEventType = "TEAM_RECRUITED"
	StreamEventTeamRetired         StreamEventType = "TEAM_RETIRED"
	StreamEventRoleAssigned        StreamEventType = "ROLE_ASSIGNED"
	StreamEventDelegation          StreamEventType = "DELEGATION"
	StreamEventDependencySatisfied StreamEventType = "DEPENDENCY_SATISFIED"
	StreamEventBudgetThreshold     StreamEventType = "BUDGET_THRESHOLD"

	// Human-readable UX events
	// 人类可读的 UX event
	StreamEventToolInvoked    StreamEventType = "TOOL_INVOKED"    // Tool usage with details in message 注：tool 调用，细节放在 message 中
	StreamEventAgentThinking  StreamEventType = "AGENT_THINKING"  // Planning/reasoning phases 注：规划/推理阶段
	StreamEventTeamStatus     StreamEventType = "TEAM_STATUS"     // Multi-agent coordination updates 注：多 agent 协作更新
	StreamEventProgress       StreamEventType = "PROGRESS"        // Step completion updates 注：步骤完成更新
	StreamEventDataProcessing StreamEventType = "DATA_PROCESSING" // Processing/analyzing data 注：处理/分析数据
	StreamEventWaiting        StreamEventType = "WAITING"         // Waiting for resources/responses 注：等待资源/响应
	StreamEventErrorRecovery  StreamEventType = "ERROR_RECOVERY"  // Handling and recovering from errors 注：处理并从错误中恢复
	StreamEventWarning        StreamEventType = "WARNING"         // Non-fatal warnings that user should be aware of 注：用户需要知晓的非致命警告
	StreamEventHITLResponse   StreamEventType = "HITL_RESPONSE"   // Lead's response to human input during swarm execution 注：swarm 执行期间 lead 对人类输入的响应
	StreamEventLeadDecision   StreamEventType = "LEAD_DECISION"   // Lead agent made a planning/coordination decision 注：lead agent 做出规划/协调决策
	StreamEventLeadToolCall   StreamEventType = "LEAD_TOOL_CALL"  // Lead executing a tool directly 注：lead 直接执行 tool

	// LLM events (uniform across workflows)
	// LLM event（各 workflow 统一）
	StreamEventLLMPrompt  StreamEventType = "LLM_PROMPT"  // Sanitized prompt 注：已清洗的 prompt
	StreamEventLLMPartial StreamEventType = "LLM_PARTIAL" // Incremental output chunk 注：增量输出片段
	StreamEventLLMOutput  StreamEventType = "LLM_OUTPUT"  // Final output for a step 注：单步的最终输出
	StreamEventToolObs    StreamEventType = "TOOL_OBSERVATION"

	// Browser screenshot persistence
	// 浏览器截图持久化
	StreamEventScreenshotSaved StreamEventType = "SCREENSHOT_SAVED"

	// Stream lifecycle
	// stream 生命周期
	StreamEventStreamEnd StreamEventType = "STREAM_END" // Explicit end-of-stream signal 注：显式的流结束信号。

	// Human approval
	// 人工审批
	StreamEventApprovalRequested StreamEventType = "APPROVAL_REQUESTED"
	StreamEventApprovalDecision  StreamEventType = "APPROVAL_DECISION"

	// HITL Research Review
	// HITL 研究评审
	StreamEventResearchPlanReady    StreamEventType = "RESEARCH_PLAN_READY"
	StreamEventResearchPlanUpdated  StreamEventType = "RESEARCH_PLAN_UPDATED"
	StreamEventResearchPlanApproved StreamEventType = "RESEARCH_PLAN_APPROVED"
	StreamEventReviewUserFeedback   StreamEventType = "REVIEW_USER_FEEDBACK"

	// Workflow control (pause/resume/cancel)
	// workflow 控制：PAUSING/CANCELLING 只表示收到信号，PAUSED/CANCELLED 才表示在 checkpoint 处真正生效。
	StreamEventWorkflowPausing    StreamEventType = "WORKFLOW_PAUSING"    // Pause signal received 注：收到暂停信号
	StreamEventWorkflowPaused     StreamEventType = "WORKFLOW_PAUSED"     // Actually blocked at checkpoint 注：已在 checkpoint 处真正阻塞
	StreamEventWorkflowResumed    StreamEventType = "WORKFLOW_RESUMED"    // Unblocked after pause 注：暂停后解除阻塞
	StreamEventWorkflowCancelling StreamEventType = "WORKFLOW_CANCELLING" // Cancel signal received 注：收到取消信号
	StreamEventWorkflowCancelled  StreamEventType = "WORKFLOW_CANCELLED"  // Cancelled at checkpoint 注：已在 checkpoint 处取消
)

// EmitTaskUpdateInput carries minimal event data for streaming_v1
// EmitTaskUpdateInput 承载 streaming_v1 所需的最小 event 数据
type EmitTaskUpdateInput struct {
	WorkflowID string          `json:"workflow_id"`
	EventType  StreamEventType `json:"event_type"`
	AgentID    string          `json:"agent_id,omitempty"`
	Message    string          `json:"message,omitempty"`
	Timestamp  time.Time       `json:"timestamp"`
	Payload    map[string]any  `json:"payload,omitempty"`
}

// EmitTaskUpdate logs a minimal deterministic event. In future it can publish to a stream.
// EmitTaskUpdate 记录一个最小的确定性 event；将来可以改为发布到 stream。
func EmitTaskUpdate(ctx context.Context, in EmitTaskUpdateInput) error {
	logger := activity.GetLogger(ctx)
	logger.Info("streaming_v1 event",
		"workflow_id", in.WorkflowID,
		"type", string(in.EventType),
		"agent_id", in.AgentID,
		"message", in.Message,
		"ts", in.Timestamp,
	)
	// Publish to in-process stream manager (best-effort)
	// 发布到进程内 stream manager（best-effort）。
	streaming.Get().Publish(in.WorkflowID, streaming.Event{
		WorkflowID: in.WorkflowID,
		Type:       string(in.EventType),
		AgentID:    in.AgentID,
		Message:    in.Message,
		Payload:    in.Payload,
		Timestamp:  in.Timestamp,
	})
	return nil
}
