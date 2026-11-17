package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// TracingMiddleware provides distributed tracing support
// TracingMiddleware 提供分布式 tracing 支持
type TracingMiddleware struct {
	logger *zap.Logger
}

// NewTracingMiddleware creates a new tracing middleware
// NewTracingMiddleware 创建一个新的 tracing middleware
func NewTracingMiddleware(logger *zap.Logger) *TracingMiddleware {
	return &TracingMiddleware{
		logger: logger,
	}
}

// Middleware returns the HTTP middleware function
// Middleware 返回 HTTP middleware 函数
func (tm *TracingMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Extract or generate trace ID
		// 提取或生成 trace ID
		traceID := tm.extractTraceID(r)
		if traceID == "" {
			traceID = tm.generateTraceID()
		}

		// Extract or generate span ID
		// 提取或生成 span ID
		spanID := tm.generateSpanID()

		// Add trace context to request context
		// 把 trace context 注入 request context
		ctx = context.WithValue(ctx, "trace_id", traceID)
		ctx = context.WithValue(ctx, "span_id", spanID)

		// Add trace headers to response
		// 给响应加上 trace 头
		w.Header().Set("X-Trace-ID", traceID)
		w.Header().Set("X-Span-ID", spanID)

		// Log request with trace context
		// 带 trace context 记录请求日志
		tm.logger.Debug("Request received",
			zap.String("trace_id", traceID),
			zap.String("span_id", spanID),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
			zap.String("remote_addr", r.RemoteAddr),
		)

		// Continue with traced request
		// 继续处理带 trace 的请求
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// extractTraceID extracts trace ID from request headers
// extractTraceID 从请求头中提取 trace ID
func (tm *TracingMiddleware) extractTraceID(r *http.Request) string {
	// Check traceparent header (W3C Trace Context)
	// 检查 traceparent 头（W3C Trace Context）。
	if traceparent := r.Header.Get("traceparent"); traceparent != "" {
		parts := strings.Split(traceparent, "-")
		if len(parts) >= 2 {
			return parts[1] // trace-id is the second part 注：trace-id 是第二部分
		}
	}

	// Check X-Trace-ID header (custom)
	// 检查 X-Trace-ID 头（自定义）
	if traceID := r.Header.Get("X-Trace-ID"); traceID != "" {
		return traceID
	}

	// Check X-Request-ID header (common alternative)
	// 检查 X-Request-ID 头（常见替代方案）
	if requestID := r.Header.Get("X-Request-ID"); requestID != "" {
		return requestID
	}

	return ""
}

// generateTraceID generates a new trace ID
// generateTraceID 生成新的 trace ID
func (tm *TracingMiddleware) generateTraceID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

// generateSpanID generates a new span ID
// generateSpanID 生成新的 span ID
func (tm *TracingMiddleware) generateSpanID() string {
	// Use a shorter ID for spans
	// span 使用更短的 ID。
	id := uuid.New()
	return strings.ReplaceAll(id.String()[:16], "-", "")
}

// ServeHTTP implements http.Handler interface
// ServeHTTP 实现 http.Handler 接口
func (tm *TracingMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte(`{"error":"Direct access not allowed"}`))
}
