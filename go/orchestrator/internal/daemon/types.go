package daemon

import "encoding/json"

// Server -> Daemon message types
// Server -> Daemon 消息类型
const (
	MsgTypeConnected = "connected"
	MsgTypeMessage   = "message"
	MsgTypeClaimAck  = "claim_ack"
	MsgTypeSystem    = "system"
)

// Daemon -> Server message types
// Daemon -> Server 消息类型
const (
	MsgTypeClaim      = "claim"
	MsgTypeReply      = "reply"
	MsgTypeProgress   = "progress"
	MsgTypeDisconnect = "disconnect"

	// Unified messaging: daemon -> server
	// 统一消息：daemon -> server
	MsgTypeDaemonApprovalRequest  = "approval_request"
	MsgTypeDaemonApprovalResolved = "approval_resolved"

	// Agent loop event forwarding: daemon -> server
	// agent 循环 event 转发：daemon -> server
	MsgTypeEvent = "event"

	// Proactive messaging: daemon -> server (unsolicited, no prior claim)
	// 主动消息：daemon -> server（无 request、无先前 claim）
	MsgTypeProactive = "proactive"
)

// Server -> Daemon message types (unified messaging)
// Server -> Daemon 消息类型（统一消息）
const (
	MsgTypeApprovalResponse = "approval_response"
)

// Channel types for optional integrations (messaging platforms, schedulers, etc.)
// 可选集成（消息平台、调度器等）使用的渠道类型
const (
	ChannelSlack    = "slack"
	ChannelLINE     = "line"
	ChannelTeams    = "teams"
	ChannelWeChat   = "wechat"
	ChannelWeb      = "web"
	ChannelSchedule = "schedule"
	ChannelSystem   = "system"
	ChannelFeishu   = "feishu"
)

// Reply format types
// 回复格式类型
const (
	FormatText     = "text"
	FormatMarkdown = "markdown"
)

// ServerMessage is the envelope for all server-to-daemon messages.
// ServerMessage 是所有 server-to-daemon 消息的信封。
type ServerMessage struct {
	Type      string          `json:"type"`
	MessageID string          `json:"message_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// DaemonMessage is the envelope for all daemon-to-server messages.
// DaemonMessage 是所有 daemon-to-server 消息的信封。
type DaemonMessage struct {
	Type      string          `json:"type"`
	MessageID string          `json:"message_id,omitempty"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// MessagePayload is what the daemon's agent loop processes.
// MessagePayload 是 daemon 的 agent 循环要处理的内容。
type MessagePayload struct {
	Channel   string `json:"channel"`
	ThreadID  string `json:"thread_id"`
	Sender    string `json:"sender"`
	Text      string `json:"text"`
	AgentName string `json:"agent_name,omitempty"`
	Source    string `json:"source,omitempty"`
	Timestamp string `json:"timestamp"`
}

// DaemonApprovalRequest is sent by daemon when it needs tool approval.
// daemon 需要 tool 审批时发送 DaemonApprovalRequest。
type DaemonApprovalRequest struct {
	RequestID string `json:"request_id"`
	Agent     string `json:"agent"`
	Tool      string `json:"tool"`
	Args      string `json:"args"`
	ThreadID  string `json:"thread_id,omitempty"`  // populated by Cloud from active claim 注：由 Cloud 依据当前 claim 填充
	ChannelID string `json:"channel_id,omitempty"` // populated by Cloud from active claim 注：由 Cloud 依据当前 claim 填充
}

// DaemonApprovalResolved is sent by daemon when ShanClaw resolves an approval first.
// ShanClaw 先处理掉审批时，daemon 发送 DaemonApprovalResolved。
type DaemonApprovalResolved struct {
	RequestID  string `json:"request_id"`
	Decision   string `json:"decision"`
	ResolvedBy string `json:"resolved_by"`
}

// ApprovalResponsePayload is sent from server to daemon with the approval decision.
// ApprovalResponsePayload 由 server 发往 daemon，携带审批结论。
type ApprovalResponsePayload struct {
	RequestID  string `json:"request_id"`
	Decision   string `json:"decision"`
	ResolvedBy string `json:"resolved_by,omitempty"`
}

// ReplyPayload is sent back after agent completes.
// agent 完成后回传 ReplyPayload。
type ReplyPayload struct {
	Channel  string `json:"channel"`
	ThreadID string `json:"thread_id"`
	Text     string `json:"text"`
	Format   string `json:"format,omitempty"`
}

// ClaimAckPayload is sent to confirm or deny a claim.
// ClaimAckPayload 用于确认或拒绝 claim。
type ClaimAckPayload struct {
	Granted bool `json:"granted"`
}

// ProgressPayload is an optional payload for progress heartbeats.
// When workflow_id is set, Cloud starts streaming for the associated channel.
// ProgressPayload 是进度心跳的可选载荷。
// 设置 workflow_id 时，Cloud 会为对应渠道启动 streaming 输出。
type ProgressPayload struct {
	WorkflowID string `json:"workflow_id,omitempty"`
}

// DaemonEventPayload carries a single agent loop event from daemon to server.
// Published to streaming.Manager for channel card updates.
// DaemonEventPayload 携带 daemon 到 server 的单条 agent 循环 event。
// 发布到 streaming.Manager 以更新渠道卡片。
type DaemonEventPayload struct {
	EventType string         `json:"event_type"` // TOOL_INVOKED, TOOL_COMPLETED, LLM_OUTPUT, LLM_PARTIAL 注：event 类型取值
	Message   string         `json:"message"`
	Data      map[string]any `json:"data,omitempty"`
	Seq       int64          `json:"seq"`
	Timestamp string         `json:"ts"`
}

// ProactivePayload is sent by the daemon to push an unsolicited message
// to all channels mapped to the named agent.
// daemon 用 ProactivePayload 把非 request 消息推送给该 agent 映射的所有渠道。
type ProactivePayload struct {
	AgentName string `json:"agent_name"`
	Text      string `json:"text"`
	Format    string `json:"format,omitempty"`
	SessionID string `json:"session_id,omitempty"`
}

// IsSystemChannel returns true for channels that don't expect agent processing.
// 对不期待 agent 处理的渠道，IsSystemChannel 返回 true。
func IsSystemChannel(channel string) bool {
	return channel == ChannelSystem
}
