package scheduled

import (
	"context"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/daemon"
	"go.temporal.io/sdk/activity"
	"go.uber.org/zap"
)

// SignalDaemonReply is the Temporal signal name used to deliver daemon replies
// back to the waiting ScheduledTaskWorkflow.
// SignalDaemonReply 是 Temporal signal 名，用于把 daemon 的回复投递给等待中的 ScheduledTaskWorkflow。
const SignalDaemonReply = "daemon-reply-v1"

// DaemonReplyTimeout is how long the workflow waits for a daemon reply before
// recording failure if no daemon responds.
// DaemonReplyTimeout 是 workflow 等待 daemon 回复的时长，超过且没有 daemon 响应就记录失败。
const DaemonReplyTimeout = 10 * time.Minute

// DaemonDispatchInput is the input for the DaemonDispatchActivity.
// DaemonDispatchInput 是 DaemonDispatchActivity 的入参。
type DaemonDispatchInput struct {
	TenantID      string
	UserID        string
	TaskQuery     string
	AgentName     string
	WorkflowID    string // Temporal workflow ID for signal routing 注：用于 signal 路由的 Temporal workflow ID
	WorkflowRunID string // Temporal run ID for signal routing 注：用于 signal 路由的 Temporal run ID
}

// DaemonDispatchResult is the output of the DaemonDispatchActivity.
// DaemonDispatchResult 是 DaemonDispatchActivity 的出参。
type DaemonDispatchResult struct {
	Dispatched bool
	Error      string
}

// DaemonActivities holds shared state (the Hub) for daemon-related activities.
// DaemonActivities 持有 daemon 相关 activity 的共享状态（即 Hub）。
type DaemonActivities struct {
	hub *daemon.Hub
}

// NewDaemonActivities creates a DaemonActivities with access to the Hub.
// NewDaemonActivities 创建一个可访问 Hub 的 DaemonActivities。
func NewDaemonActivities(hub *daemon.Hub) *DaemonActivities {
	return &DaemonActivities{hub: hub}
}

// DaemonDispatchActivity publishes a task to the Daemon Hub.
// This is a fast activity (<1s) — it does not wait for the daemon response.
// The workflow uses a Temporal signal (SignalDaemonReply) to receive the reply.
// 这是快速 activity（<1s），不等待 daemon response；workflow 通过 SignalDaemonReply 信号接收回复。
func (da *DaemonActivities) DaemonDispatchActivity(ctx context.Context, input DaemonDispatchInput) (DaemonDispatchResult, error) {
	logger := activity.GetLogger(ctx)

	if da.hub == nil {
		logger.Info("daemon hub not configured, cannot dispatch")
		return DaemonDispatchResult{Dispatched: false, Error: "daemon hub not configured"}, nil
	}

	payload := daemon.MessagePayload{
		Channel:   daemon.ChannelSchedule,
		ThreadID:  input.WorkflowID, // Use workflow ID as thread for sticky routing 注：用 workflow ID 作为 thread，便于粘性路由
		Sender:    "scheduler",
		Text:      input.TaskQuery,
		AgentName: input.AgentName,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}

	meta := daemon.ClaimMetadata{
		ChannelType:   daemon.ChannelSchedule,
		ThreadID:      input.WorkflowID,
		WorkflowID:    input.WorkflowID,
		WorkflowRunID: input.WorkflowRunID,
		Timestamp:     time.Now().UTC().Format(time.RFC3339),
	}

	// Publish to Redis stream — the gateway Hub subscribes and dispatches to local WS connections.
	// This bridges the orchestrator process (no WS connections) and gateway process (has connections).
	// 发布到 Redis stream：gateway 的 Hub 订阅后分发给本地 WS 连接，
	// 从而打通 orchestrator 进程（无 WS 连接）与 gateway 进程（有连接）。
	req := daemon.DispatchRequest{
		TenantID: input.TenantID,
		UserID:   input.UserID,
		Payload:  payload,
		Meta:     meta,
	}
	err := da.hub.PublishDispatch(ctx, req)
	if err != nil {
		logger.Info("daemon dispatch publish failed",
			zap.String("tenant_id", input.TenantID),
			zap.String("error", err.Error()),
		)
		return DaemonDispatchResult{
			Dispatched: false,
			Error:      err.Error(),
		}, nil
	}

	logger.Info("daemon dispatch published to stream",
		zap.String("tenant_id", input.TenantID),
		zap.String("workflow_id", input.WorkflowID),
	)

	return DaemonDispatchResult{
		Dispatched: true,
	}, nil
}
