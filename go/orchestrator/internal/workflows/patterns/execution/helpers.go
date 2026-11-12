package execution

import (
	"strings"
	"time"
)

// agentStartToCloseTimeout extracts timeout from context with fallback to default
// human_in_loop 场景放宽到 48h 等人工审批，其余用调用方给的默认值。
func agentStartToCloseTimeout(ctx map[string]any, defaultTimeout time.Duration) time.Duration {
	if ctx == nil {
		return defaultTimeout
	}
	if v, ok := ctx["human_in_loop"]; ok {
		switch t := v.(type) {
		case bool:
			if t {
				return 48 * time.Hour
			}
		case string:
			if strings.EqualFold(t, "true") {
				return 48 * time.Hour
			}
		}
	}
	return defaultTimeout
}
