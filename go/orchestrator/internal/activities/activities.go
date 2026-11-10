package activities

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"go.uber.org/zap"
)

// Activities struct holds dependencies for activities
// Activities 结构体持有各 activity 所需的依赖。
type Activities struct {
	sessionManager *session.Manager
	logger         *zap.Logger
}

// NewActivities creates a new activities instance with dependencies
// NewActivities 用给定依赖创建 activities 实例。
func NewActivities(sessionManager *session.Manager, logger *zap.Logger) *Activities {
	return &Activities{
		sessionManager: sessionManager,
		logger:         logger,
	}
}
