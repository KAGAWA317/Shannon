package httpapi

import (
	"encoding/json"
	"net/http"

	auth "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"go.uber.org/zap"
)

// AuthHTTPHandler provides minimal HTTP endpoints for authentication.
// Endpoints:
//
//	POST /api/auth/register
//	POST /api/auth/login
// AuthHTTPHandler 提供最简的 authentication HTTP 端点。
// 端点：POST /api/auth/register、POST /api/auth/login
type AuthHTTPHandler struct {
	svc    *auth.Service
	logger *zap.Logger
}

// NewAuthHTTPHandler constructs a new handler.
// NewAuthHTTPHandler 构造新的 handler。
func NewAuthHTTPHandler(svc *auth.Service, logger *zap.Logger) *AuthHTTPHandler {
	return &AuthHTTPHandler{svc: svc, logger: logger}
}

// RegisterRoutes registers auth endpoints on the given mux.
// RegisterRoutes 在给定的 mux 上注册 auth 端点。
func (h *AuthHTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/auth/register", h.handleRegister)
	mux.HandleFunc("/api/auth/login", h.handleLogin)
	mux.HandleFunc("/api/auth/refresh", h.handleRefresh)
}

func (h *AuthHTTPHandler) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req auth.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	// Minimal validation
	// 最小化校验
	if req.Email == "" || req.Username == "" || req.Password == "" || req.FullName == "" {
		http.Error(w, `{"error":"missing required fields"}`, http.StatusBadRequest)
		return
	}

	user, err := h.svc.Register(r.Context(), &req)
	if err != nil {
		h.logger.Warn("Register failed", zap.Error(err))
		http.Error(w, `{"error":"`+sanitizeErr(err.Error())+`"}`, http.StatusBadRequest)
		return
	}

	// Respond with a safe user view
	// 返回脱敏后的用户视图
	resp := map[string]any{
		"user": map[string]any{
			"id":        user.ID,
			"email":     user.Email,
			"username":  user.Username,
			"tenant_id": user.TenantID,
			"role":      user.Role,
		},
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *AuthHTTPHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req auth.LoginRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	if req.Email == "" || req.Password == "" {
		http.Error(w, `{"error":"missing email or password"}`, http.StatusBadRequest)
		return
	}

	tokens, err := h.svc.Login(r.Context(), &req)
	if err != nil {
		h.logger.Warn("Login failed", zap.Error(err))
		http.Error(w, `{"error":"invalid email or password"}`, http.StatusUnauthorized)
		return
	}

	writeJSON(w, http.StatusOK, tokens)
}

func (h *AuthHTTPHandler) handleRefresh(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RefreshToken string `json:"refresh_token"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	if req.RefreshToken == "" {
		http.Error(w, `{"error":"missing refresh_token"}`, http.StatusBadRequest)
		return
	}

	tokens, err := h.svc.Refresh(r.Context(), req.RefreshToken)
	if err != nil {
		h.logger.Warn("Token refresh failed", zap.Error(err))
		http.Error(w, `{"error":"invalid refresh token"}`, http.StatusUnauthorized)
		return
	}

	writeJSON(w, http.StatusOK, tokens)
}

// writeJSON writes a JSON response with status and content-type.
// writeJSON 按指定状态码与 content-type 写出 JSON response。
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// sanitizeErr trims error messages for safe client output (UTF-8 safe).
// sanitizeErr 截断错误信息以便安全返回给客户端（UTF-8 安全）。
func sanitizeErr(s string) string {
	// Keep it simple: don't leak internals
	// 保持简单：不要泄露内部细节
	runes := []rune(s)
	if len(runes) > 200 {
		return string(runes[:200])
	}
	return s
}
