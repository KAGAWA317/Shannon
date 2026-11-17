package circuitbreaker

import (
	"context"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCWrapper provides circuit breaker functionality for gRPC calls
// GRPCWrapper 为 gRPC 调用提供 circuit breaker 能力。
type GRPCWrapper struct {
	cb      *CircuitBreaker
	logger  *zap.Logger
	name    string
	service string
}

// NewGRPCWrapper creates a gRPC wrapper with circuit breaker
// NewGRPCWrapper 创建一个带 circuit breaker 的 gRPC wrapper。
func NewGRPCWrapper(name, service string, logger *zap.Logger) *GRPCWrapper {
	config := GetGRPCConfig().ToConfig()
	cb := NewCircuitBreaker(name, config, logger)

	// Register with metrics collector
	// 注册到 metrics collector。
	GlobalMetricsCollector.RegisterCircuitBreaker(name, service, cb)

	return &GRPCWrapper{
		cb:      cb,
		logger:  logger,
		name:    name,
		service: service,
	}
}

// Execute executes a gRPC call with circuit breaker protection
// Execute 在 circuit breaker 保护下执行一次 gRPC 调用。
func (gw *GRPCWrapper) Execute(ctx context.Context, fn func() error) error {
	var originalErr error

	cbErr := gw.cb.Execute(ctx, func() error {
		originalErr = fn()
		// Only count server/transient errors toward circuit breaker state
		// 只有服务端/瞬时错误才计入 circuit breaker 状态
		if originalErr != nil && isCircuitBreakerError(originalErr) {
			return originalErr
		}
		// Client errors don't count toward circuit breaker failure
		// 客户端错误不计入 circuit breaker 失败。
		return nil
	})

	// Record metrics based on actual error, not circuit breaker decision
	// 指标按真实错误记录，而不是按 circuit breaker 的放行/拒绝结果记录
	// 只有熔断拒绝会替换调用结果，其余情况原样返回调用错误。
	err := originalErr
	if cbErr == ErrCircuitBreakerOpen || cbErr == ErrTooManyRequests {
		err = cbErr
	}

	GlobalMetricsCollector.RecordRequest(gw.name, gw.service, gw.cb.State(), err == nil)

	return err
}

// UnaryClientInterceptor returns a gRPC unary client interceptor with circuit breaker
// UnaryClientInterceptor 返回带 circuit breaker 的 gRPC unary client interceptor。
func (gw *GRPCWrapper) UnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return gw.Execute(ctx, func() error {
			return invoker(ctx, method, req, reply, cc, opts...)
		})
	}
}

// StreamClientInterceptor returns a gRPC stream client interceptor with circuit breaker
// StreamClientInterceptor 返回带 circuit breaker 的 gRPC stream client interceptor。
func (gw *GRPCWrapper) StreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		var stream grpc.ClientStream
		err := gw.Execute(ctx, func() error {
			var streamErr error
			stream, streamErr = streamer(ctx, desc, cc, method, opts...)
			return streamErr
		})
		return stream, err
	}
}

// IsCircuitBreakerOpen returns true if the circuit breaker is open
// IsCircuitBreakerOpen 在 circuit breaker 打开时返回 true。
func (gw *GRPCWrapper) IsCircuitBreakerOpen() bool {
	return gw.cb.State() == StateOpen
}

// GetState returns the current circuit breaker state
// GetState 返回 circuit breaker 的当前状态。
func (gw *GRPCWrapper) GetState() State {
	return gw.cb.State()
}

// isCircuitBreakerError determines if an error should trigger the circuit breaker
// isCircuitBreakerError 判断某个错误是否应触发 circuit breaker。
func isCircuitBreakerError(err error) bool {
	if err == nil {
		return false
	}

	// Circuit breaker specific errors
	// circuit breaker 自身的错误。
	if err == ErrCircuitBreakerOpen || err == ErrTooManyRequests {
		return true
	}

	// Check gRPC status codes
	// 检查 gRPC 状态码。
	if st, ok := status.FromError(err); ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted, codes.Internal:
			return true
		case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.PermissionDenied, codes.Unauthenticated:
			// These are client errors, don't trigger circuit breaker
			// 这些是客户端错误，不触发 circuit breaker。
			return false
		default:
			return true
		}
	}

	return true
}

// GRPCConnectionWrapper wraps gRPC dial with circuit breaker
// GRPCConnectionWrapper 为 gRPC dial 包上 circuit breaker。
type GRPCConnectionWrapper struct {
	cb      *CircuitBreaker
	logger  *zap.Logger
	target  string
	service string
}

// NewGRPCConnectionWrapper creates a connection wrapper with circuit breaker
// NewGRPCConnectionWrapper 创建一个带 circuit breaker 的连接 wrapper。
func NewGRPCConnectionWrapper(target, service string, logger *zap.Logger) *GRPCConnectionWrapper {
	config := GetGRPCConnectionConfig().ToConfig()
	cb := NewCircuitBreaker("grpc-connection", config, logger)

	// Register with metrics collector
	// 注册到 metrics collector。
	GlobalMetricsCollector.RegisterCircuitBreaker("grpc-connection", service, cb)

	return &GRPCConnectionWrapper{
		cb:      cb,
		logger:  logger,
		target:  target,
		service: service,
	}
}

// DialContext dials a gRPC connection with circuit breaker protection
// DialContext 在 circuit breaker 保护下建立 gRPC 连接。
func (gcw *GRPCConnectionWrapper) DialContext(ctx context.Context, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	var conn *grpc.ClientConn
	var err error

	cbErr := gcw.cb.Execute(ctx, func() error {
		conn, err = grpc.DialContext(ctx, gcw.target, opts...)
		return err
	})

	// Record metrics
	// 记录指标。
	state := gcw.cb.State()
	success := cbErr == nil && err == nil
	GlobalMetricsCollector.RecordRequest("grpc-connection", gcw.service, state, success)

	if cbErr != nil {
		return nil, cbErr
	}
	return conn, err
}

// IsCircuitBreakerOpen returns true if the circuit breaker is open
// IsCircuitBreakerOpen 在 circuit breaker 打开时返回 true。
func (gcw *GRPCConnectionWrapper) IsCircuitBreakerOpen() bool {
	return gcw.cb.State() == StateOpen
}
