package circuitbreaker

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
)

// State represents the circuit breaker state
// State 表示 circuit breaker 的状态
type State int

const (
	StateClosed State = iota
	StateHalfOpen
	StateOpen
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "closed"
	case StateHalfOpen:
		return "half-open"
	case StateOpen:
		return "open"
	default:
		return "unknown"
	}
}

var (
	ErrCircuitBreakerOpen = errors.New("circuit breaker is open")
	ErrTooManyRequests    = errors.New("too many requests in half-open state")
)

// Config holds circuit breaker configuration
// Config 保存 circuit breaker 配置
type Config struct {
	MaxRequests      uint32        // Max requests in half-open state 注：half-open 状态下允许的最大 request 数
	Interval         time.Duration // Interval to clear request counter in closed state 注：closed 状态下清零 request 计数的周期
	Timeout          time.Duration // Time to wait before transitioning from open to half-open 注：从 open 转为 half-open 前的等待时长
	FailureThreshold uint32        // Failure threshold in closed state 注：closed 状态下的失败阈值
	SuccessThreshold uint32        // Success threshold in half-open state to transition to closed 注：half-open 状态下转为 closed 所需的成功阈值
	OnStateChange    func(name string, from State, to State)
}

// DefaultConfig returns sensible defaults for circuit breaker
// DefaultConfig 返回 circuit breaker 的合理默认配置
func DefaultConfig() Config {
	return Config{
		MaxRequests:      3,
		Interval:         60 * time.Second,
		Timeout:          10 * time.Second,
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OnStateChange:    nil,
	}
}

// Counts holds the circuit breaker statistics
// Counts 保存 circuit breaker 的统计信息
type Counts struct {
	Requests             uint32
	TotalSuccesses       uint32
	TotalFailures        uint32
	ConsecutiveSuccesses uint32
	ConsecutiveFailures  uint32
}

// CircuitBreaker implements the circuit breaker pattern
// CircuitBreaker 实现 circuit breaker 模式
type CircuitBreaker struct {
	name   string
	config Config
	logger *zap.Logger

	mutex      sync.RWMutex
	state      State
	generation uint64
	counts     Counts
	expiry     time.Time
}

// NewCircuitBreaker creates a new circuit breaker
// NewCircuitBreaker 创建一个新的 circuit breaker
func NewCircuitBreaker(name string, config Config, logger *zap.Logger) *CircuitBreaker {
	cb := &CircuitBreaker{
		name:   name,
		config: config,
		logger: logger,
		state:  StateClosed,
		expiry: time.Now().Add(config.Interval),
	}

	return cb
}

// Execute executes the given function if the circuit breaker is closed or half-open
// Execute 在 circuit breaker 为 closed 或 half-open 时执行给定函数
func (cb *CircuitBreaker) Execute(ctx context.Context, fn func() error) error {
	generation, err := cb.beforeRequest()
	if err != nil {
		return err
	}

	defer func() {
		if r := recover(); r != nil {
			cb.afterRequest(generation, false)
			panic(r)
		}
	}()

	err = fn()
	cb.afterRequest(generation, err == nil)
	return err
}

// State returns the current state of the circuit breaker
// State 返回 circuit breaker 的当前状态
func (cb *CircuitBreaker) State() State {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.state
}

// Counts returns the current counts
// Counts 返回当前计数
func (cb *CircuitBreaker) Counts() Counts {
	cb.mutex.RLock()
	defer cb.mutex.RUnlock()
	return cb.counts
}

// beforeRequest checks if request can proceed
// beforeRequest 检查 request 能否放行
func (cb *CircuitBreaker) beforeRequest() (uint64, error) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)

	if state == StateOpen {
		return generation, ErrCircuitBreakerOpen
	} else if state == StateHalfOpen && cb.counts.Requests >= cb.config.MaxRequests {
		return generation, ErrTooManyRequests
	}

	cb.counts.Requests++
	return generation, nil
}

// afterRequest updates the circuit breaker state after request completion
// afterRequest 在 request 完成后更新 circuit breaker 状态
func (cb *CircuitBreaker) afterRequest(before uint64, success bool) {
	cb.mutex.Lock()
	defer cb.mutex.Unlock()

	now := time.Now()
	state, generation := cb.currentState(now)
	if generation != before {
		return
	}

	if success {
		cb.onSuccess(state, now)
	} else {
		cb.onFailure(state, now)
	}
}

// currentState returns the current state, updating if necessary
// currentState 返回当前状态，必要时先推进状态：closed 计数窗口过期则开新一代，open timeout 则转 half-open
func (cb *CircuitBreaker) currentState(now time.Time) (State, uint64) {
	switch cb.state {
	case StateClosed:
		if !cb.expiry.IsZero() && cb.expiry.Before(now) {
			cb.toNewGeneration(now)
		}
	case StateOpen:
		if cb.expiry.Before(now) {
			cb.setState(StateHalfOpen, now)
		}
	}
	return cb.state, cb.generation
}

// onSuccess handles successful request
// onSuccess 处理成功的 request
func (cb *CircuitBreaker) onSuccess(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.TotalSuccesses++
		cb.counts.ConsecutiveFailures = 0
	case StateHalfOpen:
		cb.counts.TotalSuccesses++
		cb.counts.ConsecutiveSuccesses++
		if cb.counts.ConsecutiveSuccesses >= cb.config.SuccessThreshold {
			cb.setState(StateClosed, now)
		}
	}
}

// onFailure handles failed request
// onFailure 处理失败的 request
func (cb *CircuitBreaker) onFailure(state State, now time.Time) {
	switch state {
	case StateClosed:
		cb.counts.TotalFailures++
		cb.counts.ConsecutiveFailures++
		if cb.counts.ConsecutiveFailures >= cb.config.FailureThreshold {
			cb.setState(StateOpen, now)
		}
	case StateHalfOpen:
		cb.setState(StateOpen, now)
	}
}

// setState transitions to a new state
// setState 切换到新状态
func (cb *CircuitBreaker) setState(state State, now time.Time) {
	if cb.state == state {
		return
	}

	prev := cb.state
	cb.state = state

	cb.toNewGeneration(now)

	if cb.config.OnStateChange != nil {
		cb.config.OnStateChange(cb.name, prev, state)
	}

	cb.logger.Info("Circuit breaker state changed",
		zap.String("name", cb.name),
		zap.String("from", prev.String()),
		zap.String("to", state.String()),
	)
}

// toNewGeneration resets counters and updates generation
// toNewGeneration 重置计数器并推进 generation，使并发中的旧 request 结果被丢弃
func (cb *CircuitBreaker) toNewGeneration(now time.Time) {
	cb.generation++
	cb.counts = Counts{}

	var zero time.Time
	switch cb.state {
	case StateClosed:
		if cb.config.Interval == 0 {
			cb.expiry = zero
		} else {
			cb.expiry = now.Add(cb.config.Interval)
		}
	case StateOpen:
		cb.expiry = now.Add(cb.config.Timeout)
	default: // StateHalfOpen 注：即 StateHalfOpen 状态
		cb.expiry = zero
	}
}
