package degradation

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Manager coordinates all degradation-related functionality
// Manager 统一协调所有 degradation 相关能力
type Manager struct {
	strategy              DegradationStrategy
	modeManager           *ModeManager
	partialResultsManager *PartialResultsManager
	logger                *zap.Logger

	// Background monitoring
	healthCheckInterval time.Duration
	stopCh              chan struct{}
	started             bool
	mu                  sync.RWMutex
}

// NewManager creates a new degradation manager
func NewManager(
	redisWrapper interface{ IsCircuitBreakerOpen() bool },
	databaseWrapper interface{ IsCircuitBreakerOpen() bool },
	logger *zap.Logger,
) *Manager {
	// Create default strategy
	strategy := NewDefaultStrategy(logger, redisWrapper, databaseWrapper)

	// Create sub-managers
	modeManager := NewModeManager(strategy, logger)
	partialResultsManager := NewPartialResultsManager(strategy, logger)

	return &Manager{
		strategy:              strategy,
		modeManager:           modeManager,
		partialResultsManager: partialResultsManager,
		logger:                logger,
		healthCheckInterval:   30 * time.Second, // Check health every 30 seconds
		stopCh:                make(chan struct{}),
	}
}

// Start begins background health monitoring and degradation tracking
// Start 启动后台健康检查与 degradation 跟踪
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return nil
	}

	m.started = true

	// Start background health monitoring
	go m.healthMonitorLoop()

	m.logger.Info("Degradation manager started",
		zap.Duration("health_check_interval", m.healthCheckInterval),
	)

	return nil
}

// Stop stops background monitoring
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil
	}

	close(m.stopCh)
	m.started = false

	m.logger.Info("Degradation manager stopped")

	return nil
}

// healthMonitorLoop runs periodic health checks and updates metrics
// healthMonitorLoop 周期性执行健康检查并刷新 metrics
func (m *Manager) healthMonitorLoop() {
	ticker := time.NewTicker(m.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.updateHealthMetrics()
		}
	}
}

// updateHealthMetrics updates health-related metrics
// updateHealthMetrics 刷新健康相关 metrics
func (m *Manager) updateHealthMetrics() {
	// Get system health from strategy
	if defaultStrategy, ok := m.strategy.(*DefaultStrategy); ok {
		health := defaultStrategy.checkSystemHealth()

		// Update dependency health metrics
		RecordDependencyHealth("redis", health.Redis.IsHealthy)
		RecordDependencyHealth("database", health.Database.IsHealthy)
		RecordDependencyHealth("agent_core", health.AgentCore.IsHealthy)
		RecordDependencyHealth("llm_service", health.LLMService.IsHealthy)

		// Update circuit breaker health metrics
		RecordCircuitBreakerHealth("redis", health.Redis.CircuitBreaker == 2) // StateOpen = 2
		// StateOpen = 2，即 circuit breaker 处于 open 时记为不健康
		RecordCircuitBreakerHealth("database", health.Database.CircuitBreaker == 2)

		// Update current degradation level
		currentDegradationLevel.Set(float64(health.Overall))
	}
}

// GetModeManager returns the mode manager for workflow use
func (m *Manager) GetModeManager() *ModeManager {
	return m.modeManager
}

// GetPartialResultsManager returns the partial results manager
func (m *Manager) GetPartialResultsManager() *PartialResultsManager {
	return m.partialResultsManager
}

// GetStrategy returns the degradation strategy
func (m *Manager) GetStrategy() DegradationStrategy {
	return m.strategy
}

// CheckSystemHealth returns current system health status
// CheckSystemHealth 返回当前系统健康状态
func (m *Manager) CheckSystemHealth(ctx context.Context) (*SystemHealth, error) {
	if defaultStrategy, ok := m.strategy.(*DefaultStrategy); ok {
		health := defaultStrategy.checkSystemHealth()
		return &health, nil
	}

	// Fallback health check
	_, level, err := m.strategy.ShouldDegrade(ctx)
	if err != nil {
		return nil, err
	}

	return &SystemHealth{
		Overall:   level,
		Timestamp: time.Now(),
	}, nil
}

// UpdateExternalServiceHealth updates health status for external services
// UpdateExternalServiceHealth 更新外部服务的健康状态
func (m *Manager) UpdateExternalServiceHealth(service string, healthy bool) {
	if defaultStrategy, ok := m.strategy.(*DefaultStrategy); ok {
		switch service {
		case "agent-core":
			defaultStrategy.UpdateAgentCoreHealth(healthy)
		case "llm-service":
			defaultStrategy.UpdateLLMServiceHealth(healthy)
		}

		m.logger.Debug("Updated external service health",
			zap.String("service", service),
			zap.Bool("healthy", healthy),
		)
	}
}

// IsSystemDegraded returns true if system is currently in degraded state
func (m *Manager) IsSystemDegraded(ctx context.Context) (bool, DegradationLevel, error) {
	return m.strategy.ShouldDegrade(ctx)
}

// GetRecommendedMode returns the recommended execution mode considering degradation
// GetRecommendedMode 结合 degradation 情况返回推荐的执行 mode
func (m *Manager) GetRecommendedMode(
	ctx context.Context,
	originalMode string,
	query string,
	sessionID string,
) (string, bool, error) {
	// Convert string to ExecutionMode
	var execMode ExecutionMode
	switch originalMode {
	case "simple":
		execMode = ModeSimple
	case "standard":
		execMode = ModeStandard
	case "complex":
		execMode = ModeComplex
	default:
		execMode = ModeStandard // Default fallback
	}

	finalMode, reason, err := m.modeManager.DetermineFinalMode(ctx, execMode, query, sessionID)
	if err != nil {
		return originalMode, false, err
	}

	wasDowngraded := reason != nil

	return string(finalMode), wasDowngraded, nil
}

// ShouldReturnPartialResults determines if partial results should be returned
// ShouldReturnPartialResults 判断是否应返回 partial results
func (m *Manager) ShouldReturnPartialResults(
	ctx context.Context,
	workflowType string,
	successCount, totalCount int,
) (bool, error) {
	return m.partialResultsManager.ShouldReturnPartialResults(ctx, workflowType, successCount, totalCount)
}

// AggregatePartialResults aggregates partial results into a coherent response
// AggregatePartialResults 将 partial results 聚合为一致的 response
func (m *Manager) AggregatePartialResults(
	ctx context.Context,
	results []PartialResult,
	workflowType string,
) (*AggregatedResult, error) {
	return m.partialResultsManager.AggregateResults(ctx, results, workflowType)
}

// CreatePartialResult creates a partial result from component execution
func (m *Manager) CreatePartialResult(
	source string,
	success bool,
	result interface{},
	err error,
	confidence float64,
	degraded bool,
) PartialResult {
	return m.partialResultsManager.CreatePartialResult(source, success, result, err, confidence, degraded)
}

// CanExecuteOperation checks if an operation should proceed in current state
// CanExecuteOperation 判断当前状态下该 operation 是否可执行
func (m *Manager) CanExecuteOperation(ctx context.Context, operation string) (bool, string, error) {
	canExecute, behavior, err := m.modeManager.CanExecuteOperation(ctx, operation)
	return canExecute, behavior.String(), err
}
