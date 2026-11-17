package health

import (
	"context"
	"time"
)

// CheckStatus represents the result of a health check
// CheckStatus 表示健康检查的结果
type CheckStatus int

const (
	StatusHealthy CheckStatus = iota
	StatusDegraded
	StatusUnhealthy
	StatusUnknown
)

func (s CheckStatus) String() string {
	switch s {
	case StatusHealthy:
		return "healthy"
	case StatusDegraded:
		return "degraded"
	case StatusUnhealthy:
		return "unhealthy"
	case StatusUnknown:
		return "unknown"
	default:
		return "unknown"
	}
}

// CheckResult contains the result of a health check
// CheckResult 包含健康检查的结果
type CheckResult struct {
	Status    CheckStatus    `json:"status"`
	Message   string         `json:"message,omitempty"`
	Error     string         `json:"error,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
	Duration  time.Duration  `json:"duration"`
	Timestamp time.Time      `json:"timestamp"`
	Component string         `json:"component"`
	Critical  bool           `json:"critical"` // Whether failure affects service availability 注：失败是否影响服务可用性
}

// Checker defines the interface for health checks
// Checker 定义健康检查的接口
type Checker interface {
	// Name returns the unique name of this health check
	// Name 返回该健康检查的唯一名称
	Name() string

	// Check performs the health check and returns the result
	// Check 执行健康检查并返回结果
	Check(ctx context.Context) CheckResult

	// IsCritical returns true if this check's failure should mark the service as unhealthy
	// 若该检查失败即应判定服务不健康，则 IsCritical 返回 true
	IsCritical() bool

	// Timeout returns the maximum duration this check should take
	// Timeout 返回该检查允许的最长耗时
	Timeout() time.Duration
}

// Registrar allows components to register health checks
// Registrar 允许各组件注册健康检查
type Registrar interface {
	// RegisterChecker registers a health check
	// RegisterChecker 注册一个健康检查
	RegisterChecker(checker Checker) error

	// UnregisterChecker removes a health check
	// UnregisterChecker 移除一个健康检查
	UnregisterChecker(name string) error

	// GetCheckers returns all registered checkers
	// GetCheckers 返回所有已注册的检查器
	GetCheckers() map[string]Checker
}

// Reporter provides health status reporting
// Reporter 提供健康状态上报
type Reporter interface {
	// GetOverallHealth returns the overall health status
	// GetOverallHealth 返回整体健康状态
	GetOverallHealth(ctx context.Context) OverallHealth

	// GetDetailedHealth returns detailed health information
	// GetDetailedHealth 返回详细的健康信息
	GetDetailedHealth(ctx context.Context) DetailedHealth

	// IsReady returns true if the service is ready to serve requests
	// 服务可接收 request 时 IsReady 返回 true
	IsReady(ctx context.Context) bool

	// IsLive returns true if the service is alive (for liveness probes)
	// 服务存活时 IsLive 返回 true（用于 liveness 探针）
	IsLive(ctx context.Context) bool
}

// OverallHealth represents the overall service health
// OverallHealth 表示服务整体的健康状况
type OverallHealth struct {
	Status    CheckStatus   `json:"status"`
	Message   string        `json:"message,omitempty"`
	Timestamp time.Time     `json:"timestamp"`
	Duration  time.Duration `json:"duration"`
	Degraded  bool          `json:"degraded"`
	Ready     bool          `json:"ready"`
	Live      bool          `json:"live"`
}

// DetailedHealth provides detailed health information
// DetailedHealth 提供详细的健康信息
type DetailedHealth struct {
	Overall    OverallHealth          `json:"overall"`
	Components map[string]CheckResult `json:"components"`
	Summary    HealthSummary          `json:"summary"`
	Timestamp  time.Time              `json:"timestamp"`
}

// HealthSummary provides summary statistics
// HealthSummary 提供汇总统计
type HealthSummary struct {
	Total       int `json:"total"`
	Healthy     int `json:"healthy"`
	Degraded    int `json:"degraded"`
	Unhealthy   int `json:"unhealthy"`
	Critical    int `json:"critical"`
	NonCritical int `json:"non_critical"`
}

// HealthManager combines registration and reporting functionality
// HealthManager 组合注册与上报功能
type HealthManager interface {
	Registrar
	Reporter

	// Start begins background health checking
	// Start 启动后台健康检查
	Start(ctx context.Context) error

	// Stop stops background health checking
	// Stop 停止后台健康检查
	Stop() error
}
