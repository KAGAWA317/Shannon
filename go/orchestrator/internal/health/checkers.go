package health

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
	agentpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/agent"
)

// RedisHealthChecker checks Redis connectivity
// RedisHealthChecker 检查 Redis 连通性
type RedisHealthChecker struct {
	client  redis.UniversalClient
	wrapper *circuitbreaker.RedisWrapper
	logger  *zap.Logger
	timeout time.Duration
}

// NewRedisHealthChecker creates a Redis health checker
// NewRedisHealthChecker 创建一个 Redis 健康检查器
func NewRedisHealthChecker(client redis.UniversalClient, wrapper *circuitbreaker.RedisWrapper, logger *zap.Logger) *RedisHealthChecker {
	return &RedisHealthChecker{
		client:  client,
		wrapper: wrapper,
		logger:  logger,
		timeout: 5 * time.Second,
	}
}

func (r *RedisHealthChecker) Name() string           { return "redis" }
func (r *RedisHealthChecker) IsCritical() bool       { return true }
func (r *RedisHealthChecker) Timeout() time.Duration { return r.timeout }

func (r *RedisHealthChecker) Check(ctx context.Context) CheckResult {
	startTime := time.Now()
	result := CheckResult{
		Component: "redis",
		Critical:  true,
		Timestamp: startTime,
	}

	// Check circuit breaker state
	// 检查熔断器状态
	if r.wrapper != nil && r.wrapper.IsCircuitBreakerOpen() {
		result.Status = StatusUnhealthy
		result.Error = "circuit breaker open"
		result.Message = "Redis circuit breaker is open"
		result.Duration = time.Since(startTime)
		return result
	}

	// Try to ping Redis
	// 尝试 ping Redis
	err := r.client.Ping(ctx).Err()
	result.Duration = time.Since(startTime)

	if err != nil {
		result.Status = StatusUnhealthy
		result.Error = err.Error()
		result.Message = "Redis ping failed"
		result.Details = map[string]any{
			"error":      err.Error(),
			"latency_ms": result.Duration.Milliseconds(),
		}
		return result
	}

	// Check if degraded (high latency)
	// 检查是否降级（高延迟）
	if result.Duration > 100*time.Millisecond {
		result.Status = StatusDegraded
		result.Message = "Redis responding but with high latency"
	} else {
		result.Status = StatusHealthy
		result.Message = "Redis healthy"
	}

	result.Details = map[string]any{
		"latency_ms":           result.Duration.Milliseconds(),
		"circuit_breaker_open": false,
	}

	return result
}

// DatabaseHealthChecker checks PostgreSQL connectivity
// DatabaseHealthChecker 检查 PostgreSQL 连通性
type DatabaseHealthChecker struct {
	db      *sql.DB
	wrapper *circuitbreaker.DatabaseWrapper
	logger  *zap.Logger
	timeout time.Duration
}

// NewDatabaseHealthChecker creates a database health checker
// NewDatabaseHealthChecker 创建一个数据库健康检查器
func NewDatabaseHealthChecker(db *sql.DB, wrapper *circuitbreaker.DatabaseWrapper, logger *zap.Logger) *DatabaseHealthChecker {
	return &DatabaseHealthChecker{
		db:      db,
		wrapper: wrapper,
		logger:  logger,
		timeout: 5 * time.Second,
	}
}

func (d *DatabaseHealthChecker) Name() string           { return "database" }
func (d *DatabaseHealthChecker) IsCritical() bool       { return true }
func (d *DatabaseHealthChecker) Timeout() time.Duration { return d.timeout }

func (d *DatabaseHealthChecker) Check(ctx context.Context) CheckResult {
	startTime := time.Now()
	result := CheckResult{
		Component: "database",
		Critical:  true,
		Timestamp: startTime,
	}

	// Check circuit breaker state
	// 检查熔断器状态
	if d.wrapper != nil && d.wrapper.IsCircuitBreakerOpen() {
		result.Status = StatusUnhealthy
		result.Error = "circuit breaker open"
		result.Message = "Database circuit breaker is open"
		result.Duration = time.Since(startTime)
		return result
	}

	// Try to ping database
	// 尝试 ping 数据库
	err := d.db.PingContext(ctx)
	result.Duration = time.Since(startTime)

	if err != nil {
		result.Status = StatusUnhealthy
		result.Error = err.Error()
		result.Message = "Database ping failed"
		result.Details = map[string]any{
			"error":      err.Error(),
			"latency_ms": result.Duration.Milliseconds(),
		}
		return result
	}

	// Get connection stats
	// 获取连接统计
	stats := d.db.Stats()

	// Check for connection pool issues
	// 检查连接池问题
	if stats.OpenConnections >= stats.MaxOpenConnections && stats.MaxOpenConnections > 0 {
		result.Status = StatusDegraded
		result.Message = "Database connection pool exhausted"
	} else if result.Duration > 100*time.Millisecond {
		result.Status = StatusDegraded
		result.Message = "Database responding but with high latency"
	} else {
		result.Status = StatusHealthy
		result.Message = "Database healthy"
	}

	result.Details = map[string]any{
		"latency_ms":           result.Duration.Milliseconds(),
		"open_connections":     stats.OpenConnections,
		"max_open_connections": stats.MaxOpenConnections,
		"idle_connections":     stats.Idle,
		"in_use_connections":   stats.InUse,
		"circuit_breaker_open": false,
	}

	return result
}

// AgentCoreHealthChecker checks Agent Core gRPC service
// AgentCoreHealthChecker 检查 Agent Core gRPC 服务
type AgentCoreHealthChecker struct {
	client  agentpb.AgentServiceClient
	conn    *grpc.ClientConn
	logger  *zap.Logger
	timeout time.Duration
}

// NewAgentCoreHealthChecker creates an Agent Core health checker
// NewAgentCoreHealthChecker 创建 Agent Core 健康检查器
func NewAgentCoreHealthChecker(client agentpb.AgentServiceClient, conn *grpc.ClientConn, logger *zap.Logger) *AgentCoreHealthChecker {
	return &AgentCoreHealthChecker{
		client:  client,
		conn:    conn,
		logger:  logger,
		timeout: 5 * time.Second,
	}
}

func (a *AgentCoreHealthChecker) Name() string           { return "agent_core" }
func (a *AgentCoreHealthChecker) IsCritical() bool       { return true }
func (a *AgentCoreHealthChecker) Timeout() time.Duration { return a.timeout }

func (a *AgentCoreHealthChecker) Check(ctx context.Context) CheckResult {
	startTime := time.Now()
	result := CheckResult{
		Component: "agent_core",
		Critical:  true,
		Timestamp: startTime,
	}

	// Check gRPC connection state
	// 检查 gRPC 连接状态
	connState := a.conn.GetState()

	// Call Agent Core health check RPC (defined in internal/pb/agent)
	// 调用 Agent Core 健康检查 RPC（定义在 internal/pb/agent）
	_, err := a.client.HealthCheck(ctx, &agentpb.HealthCheckRequest{})

	result.Duration = time.Since(startTime)

	// Analyze the error
	// 分析错误
	if err != nil {
		st, ok := status.FromError(err)
		if ok {
			switch st.Code() {
			case codes.Unavailable:
				result.Status = StatusUnhealthy
				result.Message = "Agent Core service unavailable"
			case codes.DeadlineExceeded:
				result.Status = StatusDegraded
				result.Message = "Agent Core responding slowly"
			default:
				result.Status = StatusDegraded
				result.Message = fmt.Sprintf("Agent Core responding with errors: %s", st.Code())
			}
		} else {
			result.Status = StatusUnhealthy
			result.Message = "Agent Core connection failed"
		}

		result.Error = err.Error()
	} else {
		result.Status = StatusHealthy
		result.Message = "Agent Core healthy"
	}

	result.Details = map[string]any{
		"latency_ms":       result.Duration.Milliseconds(),
		"connection_state": connState.String(),
	}

	return result
}

// LLMServiceHealthChecker checks LLM service HTTP endpoint
// LLMServiceHealthChecker 检查 LLM 服务的 HTTP endpoint
type LLMServiceHealthChecker struct {
	baseURL string
	logger  *zap.Logger
	timeout time.Duration
}

// NewLLMServiceHealthChecker creates an LLM service health checker
// NewLLMServiceHealthChecker 创建 LLM 服务健康检查器
func NewLLMServiceHealthChecker(baseURL string, logger *zap.Logger) *LLMServiceHealthChecker {
	return &LLMServiceHealthChecker{
		baseURL: baseURL,
		logger:  logger,
		timeout: 5 * time.Second,
	}
}

func (l *LLMServiceHealthChecker) Name() string           { return "llm_service" }
func (l *LLMServiceHealthChecker) IsCritical() bool       { return false } // Non-critical, can fallback 注：非关键，可降级回退
func (l *LLMServiceHealthChecker) Timeout() time.Duration { return l.timeout }

func (l *LLMServiceHealthChecker) Check(ctx context.Context) CheckResult {
	startTime := time.Now()
	result := CheckResult{
		Component: "llm_service",
		Critical:  false,
		Timestamp: startTime,
	}

	// For now, implement a simple check
	// In a real implementation, you'd make an HTTP call to the health endpoint
	// 目前实现为简单检查
	// 真实实现中应向健康端点发起 HTTP 调用
	result.Duration = time.Since(startTime)
	result.Status = StatusHealthy
	result.Message = "LLM service assumed healthy (not implemented)"

	result.Details = map[string]any{
		"base_url":   l.baseURL,
		"latency_ms": result.Duration.Milliseconds(),
		"note":       "Health check not fully implemented",
	}

	return result
}

// CustomHealthChecker allows for custom health check logic
// CustomHealthChecker 允许自定义健康检查逻辑
type CustomHealthChecker struct {
	name     string
	critical bool
	timeout  time.Duration
	checkFn  func(ctx context.Context) CheckResult
}

// NewCustomHealthChecker creates a custom health checker
// NewCustomHealthChecker 创建一个自定义健康检查器
func NewCustomHealthChecker(name string, critical bool, timeout time.Duration, checkFn func(ctx context.Context) CheckResult) *CustomHealthChecker {
	return &CustomHealthChecker{
		name:     name,
		critical: critical,
		timeout:  timeout,
		checkFn:  checkFn,
	}
}

func (c *CustomHealthChecker) Name() string           { return c.name }
func (c *CustomHealthChecker) IsCritical() bool       { return c.critical }
func (c *CustomHealthChecker) Timeout() time.Duration { return c.timeout }

func (c *CustomHealthChecker) Check(ctx context.Context) CheckResult {
	return c.checkFn(ctx)
}
