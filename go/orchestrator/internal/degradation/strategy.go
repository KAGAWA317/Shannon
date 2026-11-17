package degradation

import (
	"context"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
	"go.uber.org/zap"
)

// DegradationStrategy defines how the system should degrade when dependencies fail
// DegradationStrategy 定义系统在依赖失败时应如何降级
type DegradationStrategy interface {
	// ShouldDegrade returns true if the system should enter degraded mode
	// ShouldDegrade 在系统应进入降级模式时返回 true
	ShouldDegrade(ctx context.Context) (bool, DegradationLevel, error)

	// GetFallbackBehavior returns the fallback behavior for a specific operation
	// GetFallbackBehavior 返回指定操作对应的兜底行为
	GetFallbackBehavior(operation string) FallbackBehavior

	// RecordDegradation records a degradation event for metrics
	// RecordDegradation 记录一次降级事件，用于指标统计
	RecordDegradation(level DegradationLevel, reason string)
}

// DegradationLevel represents the severity of degradation
// DegradationLevel 表示降级的严重程度
type DegradationLevel int

const (
	LevelNone     DegradationLevel = iota
	LevelMinor                     // Single dependency issue 注：单个依赖故障
	LevelModerate                  // Multiple dependency issues 注：多个依赖故障
	LevelSevere                    // Critical dependency failure 注：关键依赖故障
)

func (d DegradationLevel) String() string {
	switch d {
	case LevelNone:
		return "none"
	case LevelMinor:
		return "minor"
	case LevelModerate:
		return "moderate"
	case LevelSevere:
		return "severe"
	default:
		return "unknown"
	}
}

// FallbackBehavior defines how to handle operations when degraded
// FallbackBehavior 定义降级时各类操作的处理方式
type FallbackBehavior int

const (
	BehaviorProceed FallbackBehavior = iota // Continue with warnings 注：继续执行并附带警告
	BehaviorDegrade                         // Downgrade mode 注：降级模式
	BehaviorCache                           // Use cached results 注：使用缓存结果
	BehaviorSkip                            // Skip non-essential operations 注：跳过非必要操作
	BehaviorFail                            // Fail fast 注：快速失败
)

func (f FallbackBehavior) String() string {
	switch f {
	case BehaviorProceed:
		return "proceed"
	case BehaviorDegrade:
		return "degrade"
	case BehaviorCache:
		return "cache"
	case BehaviorSkip:
		return "skip"
	case BehaviorFail:
		return "fail"
	default:
		return "unknown"
	}
}

// DependencyHealth represents the health status of a dependency
// DependencyHealth 表示某个依赖的健康状态
type DependencyHealth struct {
	Name           string
	IsHealthy      bool
	CircuitBreaker circuitbreaker.State
	LastError      error
	LastCheckTime  time.Time
}

// SystemHealth aggregates dependency health information
// SystemHealth 汇总各依赖的健康信息
type SystemHealth struct {
	Redis      DependencyHealth
	Database   DependencyHealth
	AgentCore  DependencyHealth
	LLMService DependencyHealth
	Overall    DegradationLevel
	Timestamp  time.Time
}

// DefaultStrategy implements a conservative degradation strategy
// DefaultStrategy 实现一种保守的降级策略
type DefaultStrategy struct {
	logger               *zap.Logger
	redisWrapper         interface{ IsCircuitBreakerOpen() bool }
	databaseWrapper      interface{ IsCircuitBreakerOpen() bool }
	agentCoreHealthy     bool
	llmServiceHealthy    bool
	degradationThreshold int // Number of failed dependencies to trigger degradation 注：触发降级所需的失败依赖数
}

// NewDefaultStrategy creates a new default degradation strategy
// NewDefaultStrategy 创建默认的降级策略
func NewDefaultStrategy(
	logger *zap.Logger,
	redisWrapper interface{ IsCircuitBreakerOpen() bool },
	databaseWrapper interface{ IsCircuitBreakerOpen() bool },
) *DefaultStrategy {
	return &DefaultStrategy{
		logger:               logger,
		redisWrapper:         redisWrapper,
		databaseWrapper:      databaseWrapper,
		agentCoreHealthy:     true,
		llmServiceHealthy:    true,
		degradationThreshold: 2, // Degrade when 2+ dependencies fail 注：2 个及以上依赖失败时降级
	}
}

// ShouldDegrade determines if system should degrade based on circuit breaker states
// ShouldDegrade 依据熔断器状态判断系统是否应降级
func (ds *DefaultStrategy) ShouldDegrade(ctx context.Context) (bool, DegradationLevel, error) {
	health := ds.checkSystemHealth()

	failedCount := 0
	if !health.Redis.IsHealthy {
		failedCount++
	}
	if !health.Database.IsHealthy {
		failedCount++
	}
	if !health.AgentCore.IsHealthy {
		failedCount++
	}
	if !health.LLMService.IsHealthy {
		failedCount++
	}

	level := health.Overall
	shouldDegrade := level != LevelNone

	if shouldDegrade {
		ds.logger.Warn("System degradation triggered",
			zap.String("level", level.String()),
			zap.Int("failed_dependencies", failedCount),
			zap.Bool("redis_healthy", health.Redis.IsHealthy),
			zap.Bool("database_healthy", health.Database.IsHealthy),
			zap.Bool("agent_core_healthy", health.AgentCore.IsHealthy),
			zap.Bool("llm_service_healthy", health.LLMService.IsHealthy),
		)
	}

	return shouldDegrade, level, nil
}

// GetFallbackBehavior returns appropriate fallback behavior for operations
// GetFallbackBehavior 返回各操作对应的兜底行为
func (ds *DefaultStrategy) GetFallbackBehavior(operation string) FallbackBehavior {
	// Check current system health to determine fallback behavior
	// 检查当前系统健康状态以确定兜底行为
	health := ds.checkSystemHealth()

	switch operation {
	case "agent_execution":
		if !health.AgentCore.IsHealthy {
			return BehaviorDegrade // Downgrade to simpler mode 注：降级到更简单的模式
		}

	case "llm_call":
		if !health.LLMService.IsHealthy {
			return BehaviorCache // Use cached responses if available 注：如有缓存响应则使用缓存
		}

	case "complex_workflow":
		if health.Overall >= LevelModerate {
			return BehaviorDegrade // Downgrade to standard/simple mode 注：降级到标准/简单模式
		}

	case "non_essential_logging":
		if health.Overall >= LevelSevere {
			return BehaviorSkip // Skip non-essential operations 注：跳过非必要操作
		}
	}

	// Conservative default - proceed with warnings
	// 保守的默认行为：继续执行并附带警告
	return BehaviorProceed
}

// RecordDegradation records degradation events for monitoring
// RecordDegradation 记录降级事件，用于监控
func (ds *DefaultStrategy) RecordDegradation(level DegradationLevel, reason string) {
	ds.logger.Info("Degradation event recorded",
		zap.String("level", level.String()),
		zap.String("reason", reason),
		zap.Time("timestamp", time.Now()),
	)

	// Update metrics
	// 更新指标
	degradationEventsTotal.WithLabelValues(level.String(), reason).Inc()
	currentDegradationLevel.Set(float64(level))
}

// checkSystemHealth checks the health of all dependencies
// checkSystemHealth 检查所有依赖的健康状态
func (ds *DefaultStrategy) checkSystemHealth() SystemHealth {
	now := time.Now()

	health := SystemHealth{
		Timestamp: now,
		Redis: DependencyHealth{
			Name:          "redis",
			IsHealthy:     !ds.redisWrapper.IsCircuitBreakerOpen(),
			LastCheckTime: now,
		},
		Database: DependencyHealth{
			Name:          "database",
			IsHealthy:     !ds.databaseWrapper.IsCircuitBreakerOpen(),
			LastCheckTime: now,
		},
		AgentCore: DependencyHealth{
			Name:          "agent-core",
			IsHealthy:     ds.agentCoreHealthy,
			LastCheckTime: now,
		},
		LLMService: DependencyHealth{
			Name:          "llm-service",
			IsHealthy:     ds.llmServiceHealthy,
			LastCheckTime: now,
		},
	}

	// Map circuit breaker states
	// 映射熔断器状态
	if ds.redisWrapper.IsCircuitBreakerOpen() {
		health.Redis.CircuitBreaker = circuitbreaker.StateOpen
	} else {
		health.Redis.CircuitBreaker = circuitbreaker.StateClosed
	}

	if ds.databaseWrapper.IsCircuitBreakerOpen() {
		health.Database.CircuitBreaker = circuitbreaker.StateOpen
	} else {
		health.Database.CircuitBreaker = circuitbreaker.StateClosed
	}

	// Determine overall health
	// 判定整体健康状况
	failedCount := 0
	if !health.Redis.IsHealthy {
		failedCount++
	}
	if !health.Database.IsHealthy {
		failedCount++
	}
	if !health.AgentCore.IsHealthy {
		failedCount++
	}
	if !health.LLMService.IsHealthy {
		failedCount++
	}

	switch failedCount {
	case 0:
		health.Overall = LevelNone
	case 1:
		health.Overall = LevelMinor
	case 2:
		health.Overall = LevelModerate
	default:
		health.Overall = LevelSevere
	}

	return health
}

// UpdateAgentCoreHealth updates the agent core health status
// UpdateAgentCoreHealth 更新 agent core 的健康状态
func (ds *DefaultStrategy) UpdateAgentCoreHealth(healthy bool) {
	ds.agentCoreHealthy = healthy
}

// UpdateLLMServiceHealth updates the LLM service health status
// UpdateLLMServiceHealth 更新 LLM service 的健康状态
func (ds *DefaultStrategy) UpdateLLMServiceHealth(healthy bool) {
	ds.llmServiceHealthy = healthy
}
