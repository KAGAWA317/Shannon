package workflows

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/control"
)

// Re-export signal names for backward compatibility
// 保留旧的包级名字，调用方无需改 import；定义已迁到 control 包。
const (
	SignalPause       = control.SignalPause
	SignalResume      = control.SignalResume
	SignalCancel      = control.SignalCancel
	QueryControlState = control.QueryControlState
)

// Re-export types for backward compatibility
// 同样用 alias，保证新旧两个名字是同一类型，可直接互传。
type (
	PauseRequest         = control.PauseRequest
	ResumeRequest        = control.ResumeRequest
	CancelRequest        = control.CancelRequest
	WorkflowControlState = control.WorkflowControlState
)
