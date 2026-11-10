package control

import "time"

// Signal names for workflow control
// 名字带 _v1 后缀：signal 名是对外契约，改名会让在途 workflow 收不到信号。
const (
	SignalPause       = "pause_v1"
	SignalResume      = "resume_v1"
	SignalCancel      = "cancel_v1"
	QueryControlState = "control_state_v1"
)

// PauseRequest is sent when pausing a workflow
type PauseRequest struct {
	Reason      string `json:"reason"`
	RequestedBy string `json:"requested_by"`
}

// ResumeRequest is sent when resuming a paused workflow
type ResumeRequest struct {
	Reason      string `json:"reason"`
	RequestedBy string `json:"requested_by"`
}

// CancelRequest is sent when gracefully cancelling a workflow
type CancelRequest struct {
	Reason      string `json:"reason"`
	RequestedBy string `json:"requested_by"`
}

// WorkflowControlState tracks pause/cancel state for query handlers
// query handler 用它回报当前控制状态；signal 只改 workflow 内存状态，不落库。
type WorkflowControlState struct {
	IsPaused     bool      `json:"is_paused"`
	IsCancelled  bool      `json:"is_cancelled"`
	PausedAt     time.Time `json:"paused_at,omitempty"`
	PauseReason  string    `json:"pause_reason,omitempty"`
	PausedBy     string    `json:"paused_by,omitempty"`
	CancelReason string    `json:"cancel_reason,omitempty"`
	CancelledBy  string    `json:"cancelled_by,omitempty"`
}
