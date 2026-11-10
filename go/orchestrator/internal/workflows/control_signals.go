package workflows

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/control"
)

// ControlSignalHandler is an alias for control.SignalHandler
// 用 type alias（=）而非新类型，两边完全等价、可直接互传。
type ControlSignalHandler = control.SignalHandler
