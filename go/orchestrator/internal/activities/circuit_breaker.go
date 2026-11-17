package activities

import (
	"context"
	"errors"
	"sync"
	"time"
)

// CircuitBreakerState represents the current state of the circuit breaker
// CircuitBreakerState 表示 circuit breaker 的当前状态。
type CircuitBreakerState int

const (
	StateClosed CircuitBreakerState = iota
	StateOpen
	StateHalfOpen
)

// CircuitBreaker implements a simple circuit breaker pattern for database queries
// CircuitBreaker 为数据库查询实现了一个简单的 circuit breaker 模式。
type CircuitBreaker struct {
	mu              sync.RWMutex
	state           CircuitBreakerState
	failureCount    int
	successCount    int
	lastFailureTime time.Time

	// Configuration
	// 配置
	maxFailures     int
	resetTimeout    time.Duration
	halfOpenSuccess int
}

// NewCircuitBreaker creates a new circuit breaker with default settings
// NewCircuitBreaker 用默认参数创建 circuit breaker。
func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		state:           StateClosed,
		maxFailures:     5,                // Open circuit after 5 consecutive failures 注：连续 5 次失败后打开熔断
		resetTimeout:    30 * time.Second, // Try to reset after 30 seconds 注：30 秒后尝试重置
		halfOpenSuccess: 2,                // Need 2 successful calls to fully close circuit 注：需要 2 次成功调用才完全关闭熔断
	}
}

// ErrCircuitOpen is returned when the circuit breaker is open
// circuit breaker 处于 open 状态时返回 ErrCircuitOpen。
var ErrCircuitOpen = errors.New("circuit breaker is open")

// Call executes the function with circuit breaker protection
// Call 在 circuit breaker 保护下执行给定函数。
func (cb *CircuitBreaker) Call(ctx context.Context, fn func(context.Context) error) error {
	cb.mu.Lock()

	// Check if we should transition from Open to Half-Open
	// 判断是否应从 Open 切换到 Half-Open。
	if cb.state == StateOpen {
		if time.Since(cb.lastFailureTime) > cb.resetTimeout {
			cb.state = StateHalfOpen
			cb.successCount = 0
		} else {
			cb.mu.Unlock()
			return ErrCircuitOpen
		}
	}

	cb.mu.Unlock()

	// Execute the function
	// 执行函数。
	err := fn(ctx)

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.onFailure()
	} else {
		cb.onSuccess()
	}

	return err
}

func (cb *CircuitBreaker) onSuccess() {
	cb.failureCount = 0

	switch cb.state {
	case StateHalfOpen:
		cb.successCount++
		if cb.successCount >= cb.halfOpenSuccess {
			cb.state = StateClosed
		}
	}
}

func (cb *CircuitBreaker) onFailure() {
	cb.failureCount++
	cb.lastFailureTime = time.Now()

	switch cb.state {
	case StateClosed:
		if cb.failureCount >= cb.maxFailures {
			cb.state = StateOpen
		}
	case StateHalfOpen:
		cb.state = StateOpen
	}
}

// GetState returns the current state of the circuit breaker
// GetState 返回 circuit breaker 当前状态。
func (cb *CircuitBreaker) GetState() CircuitBreakerState {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.state
}

// Reset manually resets the circuit breaker
// Reset 手动重置 circuit breaker。
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.state = StateClosed
	cb.failureCount = 0
	cb.successCount = 0
}

// Global circuit breaker for database operations
// 数据库操作使用的全局 circuit breaker。
var dbCircuitBreaker = NewCircuitBreaker()

// WithCircuitBreaker wraps a database operation with circuit breaker protection
// WithCircuitBreaker 用 circuit breaker 包住一次数据库操作。
func WithCircuitBreaker(ctx context.Context, operation func(context.Context) error) error {
	return dbCircuitBreaker.Call(ctx, operation)
}
