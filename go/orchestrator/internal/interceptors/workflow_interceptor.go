package interceptors

import (
	"context"
	"net/http"

	"go.temporal.io/sdk/activity"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// WorkflowHTTPRoundTripper adds workflow metadata to outgoing HTTP requests
// WorkflowHTTPRoundTripper 为出站 HTTP request 附加 workflow metadata
type WorkflowHTTPRoundTripper struct {
	base http.RoundTripper
}

// NewWorkflowHTTPRoundTripper creates a new HTTP interceptor that adds workflow metadata
// NewWorkflowHTTPRoundTripper 创建注入 workflow metadata 的 HTTP 拦截器
func NewWorkflowHTTPRoundTripper(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &WorkflowHTTPRoundTripper{base: base}
}

// RoundTrip implements http.RoundTripper and injects workflow headers
// RoundTrip 实现 http.RoundTripper 并注入 workflow 相关头
func (w *WorkflowHTTPRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	// Check if we're running in an activity context
	// Handle panic gracefully if not in Temporal context (e.g., during tests)
	// 检查是否运行在 activity context 中
	// 不在 Temporal context 时（如测试中）优雅处理 panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				// Not in activity context, continue without headers
				// 不在 activity context，不添加头继续执行
			}
		}()

		info := activity.GetInfo(req.Context())
		if info.WorkflowExecution.ID != "" {
			// Add workflow ID and run ID headers
			// 添加 workflow ID 与 run ID 头
			req.Header.Set("X-Workflow-ID", info.WorkflowExecution.ID)
			req.Header.Set("X-Run-ID", info.WorkflowExecution.RunID)
		}
	}()

	return w.base.RoundTrip(req)
}

// WorkflowUnaryClientInterceptor adds workflow metadata to outgoing gRPC requests
// WorkflowUnaryClientInterceptor 为出站 gRPC request 附加 workflow metadata
func WorkflowUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Add workflow metadata if available
		info := activity.GetInfo(ctx)
		if info.WorkflowExecution.ID != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-workflow-id", info.WorkflowExecution.ID)
			ctx = metadata.AppendToOutgoingContext(ctx, "x-run-id", info.WorkflowExecution.RunID)
		}

		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

// WorkflowStreamClientInterceptor adds workflow metadata to outgoing gRPC streams
// WorkflowStreamClientInterceptor 为出站 gRPC stream 附加 workflow metadata
func WorkflowStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		// Add workflow metadata if available
		info := activity.GetInfo(ctx)
		if info.WorkflowExecution.ID != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "x-workflow-id", info.WorkflowExecution.ID)
			ctx = metadata.AppendToOutgoingContext(ctx, "x-run-id", info.WorkflowExecution.RunID)
		}

		return streamer(ctx, desc, cc, method, opts...)
	}
}
