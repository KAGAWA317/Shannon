package middleware

import (
	"context"
	"net/http"
	"os"
	"strings"

	authpkg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// AuthMiddleware provides authentication middleware
// AuthMiddleware 提供 authentication middleware。
type AuthValidator interface {
	ValidateAPIKey(ctx context.Context, apiKey string) (*authpkg.UserContext, error)
	ValidateAccessToken(ctx context.Context, token string) (*authpkg.UserContext, error)
}

// AuthMiddleware provides authentication middleware
// AuthMiddleware 提供 authentication middleware。
type AuthMiddleware struct {
	authService AuthValidator
	logger      *zap.Logger
}

// NewAuthMiddleware creates a new authentication middleware
// NewAuthMiddleware 创建新的 authentication middleware。
func NewAuthMiddleware(authService AuthValidator, logger *zap.Logger) *AuthMiddleware {
	return &AuthMiddleware{
		authService: authService,
		logger:      logger,
	}
}

// Middleware returns the HTTP middleware function
// Middleware 返回 HTTP middleware 函数。
func (m *AuthMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check if auth should be skipped (DEVELOPMENT ONLY - NEVER USE IN PRODUCTION)
		// 检查是否应跳过 authentication（仅限开发环境——生产环境绝不可用）。
		env := strings.TrimSpace(os.Getenv("ENVIRONMENT"))
		skipAuth := strings.TrimSpace(os.Getenv("GATEWAY_SKIP_AUTH"))

		if skipAuth == "1" {
			// Only allow auth skip in development environment
			// 仅允许在开发环境跳过 authentication。
			if env != "development" && env != "dev" && env != "test" {
				m.logger.Error("SECURITY WARNING: GATEWAY_SKIP_AUTH enabled in non-development environment",
					zap.String("environment", env),
					zap.String("path", r.URL.Path),
				)
				m.sendUnauthorized(w, "Authentication required")
				return
			}

			m.logger.Warn("Authentication bypassed (DEVELOPMENT MODE ONLY)",
				zap.String("environment", env),
				zap.String("path", r.URL.Path),
			)

			// In dev mode, respect x-user-id and x-tenant-id headers if provided
			// This allows testing ownership/tenancy isolation without real auth
			// 开发 mode 下，若 request 带 x-user-id / x-tenant-id 头则予以采信，
			// 以便在没有真实 authentication 的情况下测试归属与租户隔离。
			userID := uuid.MustParse("00000000-0000-0000-0000-000000000002")   // default 注：默认值
			tenantID := uuid.MustParse("00000000-0000-0000-0000-000000000001") // default 注：默认值

			if headerUserID := r.Header.Get("x-user-id"); headerUserID != "" {
				if parsed, err := uuid.Parse(headerUserID); err == nil {
					userID = parsed
				}
			}
			if headerTenantID := r.Header.Get("x-tenant-id"); headerTenantID != "" {
				if parsed, err := uuid.Parse(headerTenantID); err == nil {
					tenantID = parsed
				}
			}

			userCtx := &authpkg.UserContext{
				UserID:       userID,
				TenantID:     tenantID,
				Username:     "admin",
				Email:        "admin@localhost",
				Role:         "admin",
				IsAPIKey:     true,
				TokenType:    "api_key",
				AuthBypassed: true,
			}
			ctx := context.WithValue(r.Context(), authpkg.UserContextKey, userCtx)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}

		// Extract token from headers (preferred) or query params (SSE endpoints only)
		// 从 request 头（优先）或 query 参数（仅 SSE 端点）中提取 token。
		token := m.extractToken(r)
		if token == "" {
			m.sendUnauthorized(w, "Authentication required")
			return
		}

		// Validate JWT access token first, then fall back to API key
		// 先按 JWT access token 校验，失败再回退到 API key。
		userCtx, err := m.validateToken(r.Context(), token)
		if err != nil {
			m.logger.Debug("Authentication validation failed",
				zap.Error(err),
				zap.String("token_prefix", m.getTokenPrefix(token)),
			)
			m.sendUnauthorized(w, "Invalid token")
			return
		}

		// Add user context to request context
		// IMPORTANT: Use authpkg.UserContextKey (typed ContextKey), not plain string "user"
		// This matches what grpcmeta.go expects when extracting user context
		// 把用户信息写入 request context。注意：必须用 authpkg.UserContextKey（带类型的 ContextKey），
		// 不能用普通字符串 "user"；这与 grpcmeta.go 提取用户信息时的约定一致。
		ctx := context.WithValue(r.Context(), authpkg.UserContextKey, userCtx)

		// Log successful authentication
		// 记录 authentication 成功的日志。
		m.logger.Debug("Request authenticated",
			zap.String("user_id", userCtx.UserID.String()),
			zap.String("tenant_id", userCtx.TenantID.String()),
			zap.String("path", r.URL.Path),
		)

		// Continue with authenticated request
		// 继续处理已完成 authentication 的 request。
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *AuthMiddleware) validateToken(ctx context.Context, token string) (*authpkg.UserContext, error) {
	if userCtx, err := m.authService.ValidateAccessToken(ctx, token); err == nil {
		return userCtx, nil
	}
	return m.authService.ValidateAPIKey(ctx, token)
}

// extractToken extracts an auth token (JWT access token or API key) from the request.
// extractToken 从 request 中提取 auth token（JWT access token 或 API key）。
func (m *AuthMiddleware) extractToken(r *http.Request) string {
	// Check X-API-Key header (preferred)
	// 检查 X-API-Key 头（优先）。
	if apiKey := r.Header.Get("X-API-Key"); apiKey != "" {
		return normalizePotentialAPIKey(apiKey)
	}

	// Check Authorization header with Bearer token (preferred)
	// 检查带 Bearer token 的 Authorization 头（优先）。
	if auth := r.Header.Get("Authorization"); auth != "" {
		parts := strings.Split(auth, " ")
		if len(parts) == 2 && strings.ToLower(parts[0]) == "bearer" {
			return normalizePotentialAPIKey(parts[1])
		}
	}

	// Check query parameter (SSE/WebSocket streaming endpoints only - for EventSource/WebSocket API compatibility)
	// SECURITY WARNING: Query parameters appear in server logs, browser history, and referrer headers.
	// This is only supported because browser EventSource API doesn't allow custom headers.
	// Production deployments should use headers when possible.
	// 仅对 SSE/WebSocket streaming 端点支持从 query 参数取 token（为兼容浏览器 EventSource/WebSocket API）。
	// 安全警告：query 参数会出现在服务端日志、浏览器历史和 referrer 头中；
	// 之所以支持，只是因为浏览器 EventSource 无法自定义 request 头。生产部署应尽量使用 request 头。
	path := r.URL.Path
	isStreamingEndpoint := r.Method == http.MethodGet && (strings.HasPrefix(path, "/api/v1/stream/") || strings.HasPrefix(path, "/v1/ws/"))

	if isStreamingEndpoint {
		// Try api_key query param first
		// 优先尝试 api_key query 参数。
		if apiKey := r.URL.Query().Get("api_key"); apiKey != "" {
			m.logger.Warn("API key provided via query parameter (SSE endpoint)",
				zap.String("path", path),
				zap.String("method", r.Method),
				zap.String("remote_addr", r.RemoteAddr),
				zap.String("token_prefix", m.getTokenPrefix(apiKey)),
				zap.String("warning", "Query params appear in access logs. Use X-API-Key header when possible."),
			)
			return normalizePotentialAPIKey(apiKey)
		}

		// Try token query param (JWT) for SSE endpoints
		// 对 SSE 端点尝试 token query 参数（JWT）。
		if token := r.URL.Query().Get("token"); token != "" {
			m.logger.Warn("JWT token provided via query parameter (SSE endpoint)",
				zap.String("path", path),
				zap.String("method", r.Method),
				zap.String("remote_addr", r.RemoteAddr),
				zap.String("token_prefix", m.getTokenPrefix(token)),
				zap.String("warning", "Query params appear in access logs. Use Authorization header when possible."),
			)
			return normalizePotentialAPIKey(token)
		}
	} else {
		// Non-streaming endpoint - reject query param auth
		// 非 streaming 端点 —— 拒绝 query 参数认证。
		if apiKey := r.URL.Query().Get("api_key"); apiKey != "" {
			m.logger.Warn("API key in query parameter rejected (non-SSE endpoint)",
				zap.String("path", path),
				zap.String("method", r.Method),
				zap.String("token_prefix", m.getTokenPrefix(apiKey)),
				zap.String("hint", "Use X-API-Key header or Authorization: Bearer header instead"),
			)
		}
		if token := r.URL.Query().Get("token"); token != "" {
			m.logger.Warn("JWT token in query parameter rejected (non-SSE endpoint)",
				zap.String("path", path),
				zap.String("method", r.Method),
				zap.String("token_prefix", m.getTokenPrefix(token)),
				zap.String("hint", "Use Authorization: Bearer header instead"),
			)
		}
	}

	return ""
}

func normalizePotentialAPIKey(token string) string {
	token = strings.TrimSpace(token)
	if after, ok := strings.CutPrefix(token, "sk-shannon-"); ok {
		token = after
		if !strings.HasPrefix(token, "sk_") {
			token = "sk_" + token
		}
	}
	return token
}

// getTokenPrefix returns the first few characters of a token for logging.
// getTokenPrefix 返回 token 的前几个字符，用于日志。
func (m *AuthMiddleware) getTokenPrefix(token string) string {
	if len(token) > 8 {
		return token[:8] + "..."
	}
	return "***"
}

// sendUnauthorized sends an unauthorized response
// sendUnauthorized 发送未授权响应。
func (m *AuthMiddleware) sendUnauthorized(w http.ResponseWriter, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("WWW-Authenticate", `Bearer realm="Shannon API"`)
	w.WriteHeader(http.StatusUnauthorized)
	w.Write([]byte(`{"error":"` + message + `"}`))
}

// ServeHTTP implements http.Handler interface for convenience
// ServeHTTP 实现 http.Handler 接口，便于直接使用。
func (m *AuthMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// This allows the middleware to be used directly as a handler
	// It will reject all requests since there's no next handler
	// 这样 middleware 可以直接当作 handler 使用；由于没有 next handler，它会拒绝所有 request。
	m.sendUnauthorized(w, "Direct access not allowed")
}
