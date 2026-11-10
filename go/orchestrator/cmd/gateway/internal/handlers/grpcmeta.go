package handlers

import (
	"context"
	"net/http"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"google.golang.org/grpc/metadata"
)

// withGRPCMetadata attaches authentication and tracing headers from the HTTP request
// to the outgoing gRPC context. It supports X-API-Key and Authorization (Bearer),
// as well as W3C traceparent for tracing propagation.
// It also propagates user/tenant IDs from the auth context for orchestrator ownership checks.
// withGRPCMetadata 把 HTTP request 中的 authentication 与 tracing 头附加到出站 gRPC context：
// 支持 X-API-Key 与 Authorization（Bearer），以及用于链路传播的 W3C traceparent；
// 同时透传 auth context 中的 user/tenant ID，供 orchestrator 做归属校验。
func withGRPCMetadata(ctx context.Context, r *http.Request) context.Context {
	md := metadata.MD{}
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		md.Set("x-api-key", apiKey)
	}
	if authHeader := r.Header.Get("Authorization"); authHeader != "" {
		md.Set("authorization", authHeader)
	}
	if traceParent := r.Header.Get("traceparent"); traceParent != "" {
		md.Set("traceparent", traceParent)
	}

	// Pass user/tenant IDs for ownership checks
	// First try to get from auth context (set by auth middleware)
	// IMPORTANT: Use auth.UserContextKey (typed ContextKey), not plain string "user"
	// 透传 user/tenant ID 用于归属校验：先从 auth context 取（由 auth middleware 写入）；
	// 注意：必须用 auth.UserContextKey（带类型的 ContextKey），不能用普通字符串 "user"。
	if userCtx, ok := r.Context().Value(auth.UserContextKey).(*auth.UserContext); ok {
		md.Set("x-user-id", userCtx.UserID.String())
		md.Set("x-tenant-id", userCtx.TenantID.String())
	} else {
		// Fallback to HTTP headers (dev mode support)
		if userID := r.Header.Get("x-user-id"); userID != "" {
			md.Set("x-user-id", userID)
		}
		if tenantID := r.Header.Get("x-tenant-id"); tenantID != "" {
			md.Set("x-tenant-id", tenantID)
		}
	}

	if len(md) > 0 {
		return metadata.NewOutgoingContext(ctx, md)
	}
	return ctx
}
