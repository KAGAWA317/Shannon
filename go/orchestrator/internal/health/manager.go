package health

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"go.uber.org/zap"
)

// CheckerState represents the runtime state of a health checker
// CheckerState 表示健康检查器的运行时状态
type CheckerState struct {
	checker   Checker
	enabled   bool
	interval  time.Duration
	timeout   time.Duration
	critical  bool
	lastCheck time.Time
}

// Manager implements the HealthManager interface
// Manager 实现 HealthManager 接口
type Manager struct {
	checkers      map[string]*CheckerState
	lastResults   map[string]CheckResult
	config        *HealthConfiguration
	started       bool
	checkInterval time.Duration
	stopCh        chan struct{}
	logger        *zap.Logger
	mu            sync.RWMutex
}

// HealthConfiguration contains health check configuration
// HealthConfiguration 包含健康检查配置
type HealthConfiguration struct {
	Enabled       bool
	CheckInterval time.Duration
	GlobalTimeout time.Duration
	Checks        map[string]CheckConfig
}

// CheckConfig represents individual check configuration
// CheckConfig 表示单个检查的配置
type CheckConfig struct {
	Enabled  bool
	Critical bool
	Timeout  time.Duration
	Interval time.Duration
}

// NewManager creates a new health manager
// NewManager 创建新的健康管理器
func NewManager(logger *zap.Logger) *Manager {
	return &Manager{
		checkers:    make(map[string]*CheckerState),
		lastResults: make(map[string]CheckResult),
		config: &HealthConfiguration{
			Enabled:       true,
			CheckInterval: 30 * time.Second,
			GlobalTimeout: 5 * time.Second,
			Checks:        make(map[string]CheckConfig),
		},
		checkInterval: 30 * time.Second,
		stopCh:        make(chan struct{}),
		logger:        logger,
	}
}

// NewManagerWithConfig creates a health manager with specific configuration
// NewManagerWithConfig 使用指定配置创建健康管理器
func NewManagerWithConfig(config *HealthConfiguration, logger *zap.Logger) *Manager {
	if config == nil {
		return NewManager(logger)
	}

	return &Manager{
		checkers:      make(map[string]*CheckerState),
		lastResults:   make(map[string]CheckResult),
		config:        config,
		checkInterval: config.CheckInterval,
		stopCh:        make(chan struct{}),
		logger:        logger,
	}
}

// RegisterChecker registers a health check
// RegisterChecker 注册健康检查
func (m *Manager) RegisterChecker(checker Checker) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	name := checker.Name()
	if name == "" {
		return fmt.Errorf("checker name cannot be empty")
	}

	if _, exists := m.checkers[name]; exists {
		return fmt.Errorf("checker %s already registered", name)
	}

	// Get configuration for this checker
	// 获取该检查器的配置
	checkConfig, hasConfig := m.config.Checks[name]

	// Create checker state with config or defaults
	// 按配置或默认值创建检查器状态
	state := &CheckerState{
		checker:  checker,
		enabled:  true,
		interval: m.config.CheckInterval,
		timeout:  checker.Timeout(),
		critical: checker.IsCritical(),
	}

	// Apply configuration overrides if available
	// 若存在配置覆盖则应用
	if hasConfig {
		state.enabled = checkConfig.Enabled
		if checkConfig.Interval > 0 {
			state.interval = checkConfig.Interval
		}
		if checkConfig.Timeout > 0 {
			state.timeout = checkConfig.Timeout
		}
		state.critical = checkConfig.Critical
	}

	m.checkers[name] = state
	m.logger.Info("Health checker registered",
		zap.String("checker", name),
		zap.Bool("enabled", state.enabled),
		zap.Bool("critical", state.critical),
		zap.Duration("timeout", state.timeout),
		zap.Duration("interval", state.interval),
	)

	return nil
}

// UnregisterChecker removes a health check
// UnregisterChecker 移除健康检查
func (m *Manager) UnregisterChecker(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.checkers[name]; !exists {
		return fmt.Errorf("checker %s not found", name)
	}

	delete(m.checkers, name)
	delete(m.lastResults, name)

	m.logger.Info("Health checker unregistered", zap.String("checker", name))
	return nil
}

// GetCheckers returns all registered checkers
// GetCheckers 返回所有已注册的检查器
func (m *Manager) GetCheckers() map[string]Checker {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(map[string]Checker)
	for name, state := range m.checkers {
		result[name] = state.checker
	}
	return result
}

// GetOverallHealth returns the overall health status
// GetOverallHealth 返回整体健康状况
func (m *Manager) GetOverallHealth(ctx context.Context) OverallHealth {
	startTime := time.Now()
	detailed := m.GetDetailedHealth(ctx)

	overall := OverallHealth{
		Status:    detailed.Overall.Status,
		Message:   detailed.Overall.Message,
		Timestamp: detailed.Timestamp,
		Duration:  time.Since(startTime),
		Degraded:  detailed.Overall.Degraded,
		Ready:     detailed.Overall.Ready,
		Live:      detailed.Overall.Live,
	}

	return overall
}

// GetDetailedHealth returns detailed health information
// GetDetailedHealth 返回详细健康信息
func (m *Manager) GetDetailedHealth(ctx context.Context) DetailedHealth {
	m.mu.RLock()
	checkerStates := make(map[string]*CheckerState)
	maps.Copy(checkerStates, m.checkers)
	m.mu.RUnlock()

	timestamp := time.Now()
	components := make(map[string]CheckResult)
	enabledCount := 0

	// Count enabled checkers
	// 统计已启用的检查器数量
	for _, state := range checkerStates {
		if state.enabled {
			enabledCount++
		}
	}

	summary := HealthSummary{
		Total: enabledCount, // Only count enabled checkers 注：仅统计已启用的检查器
	}

	// Run health checks only for enabled checkers
	// 仅对已启用的检查器执行健康检查
	for name, state := range checkerStates {
		if !state.enabled {
			// Skip disabled checkers
			// 跳过已禁用的检查器
			continue
		}

		result := m.runSingleCheckWithState(ctx, state)
		components[name] = result

		// Update summary
		// 更新汇总
		switch result.Status {
		case StatusHealthy:
			summary.Healthy++
		case StatusDegraded:
			summary.Degraded++
		case StatusUnhealthy:
			summary.Unhealthy++
		}

		if result.Critical {
			summary.Critical++
		} else {
			summary.NonCritical++
		}
	}

	// Store results for future queries
	// 存储结果供后续查询
	m.mu.Lock()
	maps.Copy(m.lastResults, components)
	m.mu.Unlock()

	// Determine overall status
	// 判定整体状态
	overall := m.calculateOverallStatus(components, summary)

	return DetailedHealth{
		Overall:    overall,
		Components: components,
		Summary:    summary,
		Timestamp:  timestamp,
	}
}

// runSingleCheck executes a single health check with timeout (legacy method)
// runSingleCheck 执行单次健康检查并带 timeout（旧方法）
func (m *Manager) runSingleCheck(ctx context.Context, checker Checker) CheckResult {
	checkCtx, cancel := context.WithTimeout(ctx, checker.Timeout())
	defer cancel()

	startTime := time.Now()
	result := checker.Check(checkCtx)

	// Ensure result has required fields
	// 确保结果带有必需字段
	result.Component = checker.Name()
	result.Critical = checker.IsCritical()
	result.Duration = time.Since(startTime)
	result.Timestamp = startTime

	return result
}

// runSingleCheckWithState executes a single health check with state-based configuration
// runSingleCheckWithState 基于状态中的配置执行单次健康检查
func (m *Manager) runSingleCheckWithState(ctx context.Context, state *CheckerState) CheckResult {
	checkCtx, cancel := context.WithTimeout(ctx, state.timeout)
	defer cancel()

	startTime := time.Now()
	result := state.checker.Check(checkCtx)

	// Ensure result has required fields from state
	// 确保结果带有状态中的必需字段
	result.Component = state.checker.Name()
	result.Critical = state.critical
	result.Duration = time.Since(startTime)
	result.Timestamp = startTime

	// Update last check time
	// 更新最后检查时间
	state.lastCheck = startTime

	return result
}

// calculateOverallStatus determines overall health from component results
// calculateOverallStatus 由各组件结果判定整体健康状况
func (m *Manager) calculateOverallStatus(components map[string]CheckResult, summary HealthSummary) OverallHealth {
	if summary.Total == 0 {
		return OverallHealth{
			Status:  StatusUnknown,
			Message: "No health checks registered",
			Ready:   false,
			Live:    false,
		}
	}

	// Critical check failures make system unhealthy
	// 关键检查失败会使系统不健康
	criticalFailures := 0
	nonCriticalFailures := 0
	degradedComponents := 0

	for _, result := range components {
		if result.Status == StatusDegraded {
			degradedComponents++
		}

		if result.Status == StatusUnhealthy {
			if result.Critical {
				criticalFailures++
			} else {
				nonCriticalFailures++
			}
		}
	}

	// Determine overall status
	// 判定整体状态
	var status CheckStatus
	var message string
	var ready, live bool

	if criticalFailures > 0 {
		status = StatusUnhealthy
		message = fmt.Sprintf("%d critical component(s) failing", criticalFailures)
		ready = false
		live = true // Still alive but not ready 注：仍然存活但未就绪
	} else if degradedComponents > 0 {
		status = StatusDegraded
		message = fmt.Sprintf("%d component(s) degraded", degradedComponents)
		ready = true // Still ready but degraded 注：仍然就绪但处于降级
		live = true
	} else if nonCriticalFailures > 0 {
		status = StatusDegraded
		message = fmt.Sprintf("%d non-critical component(s) failing", nonCriticalFailures)
		ready = true
		live = true
	} else {
		status = StatusHealthy
		message = fmt.Sprintf("All %d components healthy", summary.Total)
		ready = true
		live = true
	}

	return OverallHealth{
		Status:   status,
		Message:  message,
		Degraded: status == StatusDegraded || degradedComponents > 0,
		Ready:    ready,
		Live:     live,
	}
}

// IsReady returns true if the service is ready to serve requests
// 服务可接收 request 时 IsReady 返回 true
func (m *Manager) IsReady(ctx context.Context) bool {
	overall := m.GetOverallHealth(ctx)
	return overall.Ready
}

// IsLive returns true if the service is alive (for liveness probes)
// 服务存活时 IsLive 返回 true（用于 liveness 探针）
func (m *Manager) IsLive(ctx context.Context) bool {
	overall := m.GetOverallHealth(ctx)
	return overall.Live
}

// Start begins background health checking
// Start 启动后台健康检查
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.started {
		return nil
	}

	m.started = true
	go m.backgroundChecker()

	m.logger.Info("Health manager started",
		zap.Duration("check_interval", m.checkInterval),
		zap.Int("registered_checkers", len(m.checkers)),
	)

	return nil
}

// Stop stops background health checking
// Stop 停止后台健康检查
func (m *Manager) Stop() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if !m.started {
		return nil
	}

	close(m.stopCh)
	m.started = false

	m.logger.Info("Health manager stopped")
	return nil
}

// backgroundChecker runs periodic health checks
// backgroundChecker 周期性执行健康检查
func (m *Manager) backgroundChecker() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.runBackgroundChecks()
		}
	}
}

// runBackgroundChecks executes all health checks in background with per-check intervals
// runBackgroundChecks 在后台按各自的间隔执行全部健康检查
func (m *Manager) runBackgroundChecks() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	m.mu.RLock()
	checkerStates := make(map[string]*CheckerState)
	maps.Copy(checkerStates, m.checkers)
	m.mu.RUnlock()

	now := time.Now()
	var checkResults = make(map[string]CheckResult)

	// Run checks based on individual intervals
	// 按各自的间隔执行检查
	for name, state := range checkerStates {
		if !state.enabled {
			continue // Skip disabled checkers 注：跳过已禁用的检查器
		}

		// Check if it's time to run this check
		// 检查是否到了该执行此检查的时间
		if now.Sub(state.lastCheck) >= state.interval {
			result := m.runSingleCheckWithState(ctx, state)
			checkResults[name] = result
		}
	}

	// Update last results if we have any new results
	// 有新结果时更新最后一次结果
	if len(checkResults) > 0 {
		m.mu.Lock()
		maps.Copy(m.lastResults, checkResults)
		m.mu.Unlock()

		m.logger.Debug("Background health checks completed",
			zap.Int("checks_run", len(checkResults)),
		)
	}
}

// SetCheckInterval updates the background check interval
// SetCheckInterval 更新后台检查间隔
func (m *Manager) SetCheckInterval(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.checkInterval = interval
	m.logger.Info("Health check interval updated", zap.Duration("interval", interval))
}

// GetLastResults returns the most recent health check results without running new checks
// GetLastResults 返回最近一次健康检查结果，不执行新检查
func (m *Manager) GetLastResults() map[string]CheckResult {
	m.mu.RLock()
	defer m.mu.RUnlock()

	results := make(map[string]CheckResult)
	maps.Copy(results, m.lastResults)
	return results
}

// UpdateConfiguration updates the health manager configuration
// UpdateConfiguration 更新健康管理器配置
func (m *Manager) UpdateConfiguration(config *HealthConfiguration) error {
	if config == nil {
		return fmt.Errorf("configuration cannot be nil")
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.config = config
	m.checkInterval = config.CheckInterval

	// Update existing checker states with new configuration
	// 用新配置更新已有的检查器状态
	for name, state := range m.checkers {
		if checkConfig, exists := config.Checks[name]; exists {
			state.enabled = checkConfig.Enabled
			if checkConfig.Interval > 0 {
				state.interval = checkConfig.Interval
			}
			if checkConfig.Timeout > 0 {
				state.timeout = checkConfig.Timeout
			}
			state.critical = checkConfig.Critical

			m.logger.Info("Updated health checker configuration",
				zap.String("checker", name),
				zap.Bool("enabled", state.enabled),
				zap.Bool("critical", state.critical),
				zap.Duration("timeout", state.timeout),
				zap.Duration("interval", state.interval),
			)
		}
	}

	// Log overall configuration changes
	// 记录整体配置变更
	m.logger.Info("Health manager configuration updated",
		zap.Bool("enabled", config.Enabled),
		zap.Duration("global_check_interval", config.CheckInterval),
		zap.Duration("global_timeout", config.GlobalTimeout),
		zap.Int("check_configs", len(config.Checks)),
	)

	return nil
}

// GetConfiguration returns the current health configuration
// GetConfiguration 返回当前健康配置
func (m *Manager) GetConfiguration() *HealthConfiguration {
	m.mu.RLock()
	defer m.mu.RUnlock()

	// Return a copy to prevent external modification
	// 返回副本，防止外部修改
	configCopy := &HealthConfiguration{
		Enabled:       m.config.Enabled,
		CheckInterval: m.config.CheckInterval,
		GlobalTimeout: m.config.GlobalTimeout,
		Checks:        make(map[string]CheckConfig),
	}

	for name, checkConfig := range m.config.Checks {
		configCopy.Checks[name] = CheckConfig{
			Enabled:  checkConfig.Enabled,
			Critical: checkConfig.Critical,
			Timeout:  checkConfig.Timeout,
			Interval: checkConfig.Interval,
		}
	}

	return configCopy
}

// EnableChecker enables a specific health checker
// EnableChecker 启用指定健康检查器
func (m *Manager) EnableChecker(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.checkers[name]
	if !exists {
		return fmt.Errorf("checker %s not found", name)
	}

	if state.enabled {
		return nil // Already enabled 注：已启用
	}

	state.enabled = true
	m.logger.Info("Health checker enabled", zap.String("checker", name))
	return nil
}

// DisableChecker disables a specific health checker
// DisableChecker 禁用指定健康检查器
func (m *Manager) DisableChecker(name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, exists := m.checkers[name]
	if !exists {
		return fmt.Errorf("checker %s not found", name)
	}

	if !state.enabled {
		return nil // Already disabled 注：已禁用
	}

	state.enabled = false
	m.logger.Info("Health checker disabled", zap.String("checker", name))
	return nil
}

// GetCheckerStates returns the current state of all checkers (for debugging)
// GetCheckerStates 返回所有检查器的当前状态（用于调试）
func (m *Manager) GetCheckerStates() map[string]CheckerState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	states := make(map[string]CheckerState)
	for name, state := range m.checkers {
		states[name] = CheckerState{
			checker:   state.checker,
			enabled:   state.enabled,
			interval:  state.interval,
			timeout:   state.timeout,
			critical:  state.critical,
			lastCheck: state.lastCheck,
		}
	}
	return states
}
