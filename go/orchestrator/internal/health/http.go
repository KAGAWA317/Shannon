package health

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"
)

// HTTPHandler provides HTTP endpoints for health checks
// HTTPHandler 提供健康检查的 HTTP 端点
type HTTPHandler struct {
	manager *Manager
	logger  *zap.Logger
}

// NewHTTPHandler creates a new HTTP handler for health checks
// NewHTTPHandler 创建健康检查的 HTTP 处理器
func NewHTTPHandler(manager *Manager, logger *zap.Logger) *HTTPHandler {
	return &HTTPHandler{
		manager: manager,
		logger:  logger,
	}
}

// RegisterRoutes registers health check endpoints with an HTTP mux
// RegisterRoutes 把健康检查端点注册到 HTTP mux
func (h *HTTPHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.handleHealth)
	mux.HandleFunc("/health/ready", h.handleReadiness)
	mux.HandleFunc("/health/live", h.handleLiveness)
	mux.HandleFunc("/health/detailed", h.handleDetailedHealth)
}

// handleHealth returns overall health status (for general monitoring)
// handleHealth 返回整体健康状态（用于常规监控）
func (h *HTTPHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()
	overall := h.manager.GetOverallHealth(ctx)

	// Set HTTP status based on health
	var statusCode int
	switch overall.Status {
	case StatusHealthy:
		statusCode = http.StatusOK
	case StatusDegraded:
		statusCode = http.StatusOK // Still OK but with warning
	case StatusUnhealthy:
		statusCode = http.StatusServiceUnavailable
	default:
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := map[string]any{
		"status":    overall.Status.String(),
		"message":   overall.Message,
		"timestamp": overall.Timestamp.Unix(),
		"duration":  overall.Duration.String(),
		"degraded":  overall.Degraded,
		"ready":     overall.Ready,
		"live":      overall.Live,
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode health response", zap.Error(err))
	}
}

// handleReadiness returns readiness status (for k8s readiness probes)
// handleReadiness 返回就绪状态（用于 k8s readiness 探针）
func (h *HTTPHandler) handleReadiness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()
	ready := h.manager.IsReady(ctx)

	var statusCode int
	var message string

	if ready {
		statusCode = http.StatusOK
		message = "ready"
	} else {
		statusCode = http.StatusServiceUnavailable
		message = "not ready"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := map[string]any{
		"status":    message,
		"ready":     ready,
		"timestamp": time.Now().Unix(),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode readiness response", zap.Error(err))
	}
}

// handleLiveness returns liveness status (for k8s liveness probes)
// handleLiveness 返回存活状态（用于 k8s liveness 探针）
func (h *HTTPHandler) handleLiveness(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()
	alive := h.manager.IsLive(ctx)

	var statusCode int
	var message string

	if alive {
		statusCode = http.StatusOK
		message = "alive"
	} else {
		statusCode = http.StatusServiceUnavailable
		message = "not alive"
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := map[string]any{
		"status":    message,
		"live":      alive,
		"timestamp": time.Now().Unix(),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode liveness response", zap.Error(err))
	}
}

// handleDetailedHealth returns detailed health information (for debugging)
// handleDetailedHealth 返回详细健康信息（用于调试）
func (h *HTTPHandler) handleDetailedHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		h.writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}

	ctx := r.Context()

	// Check for cached results parameter
	cached := r.URL.Query().Get("cached") == "true"

	var detailed DetailedHealth
	if cached {
		// Return cached results without running new checks
		// 直接返回 cache 结果，不执行新的检查
		lastResults := h.manager.GetLastResults()
		components := make(map[string]CheckResult)
		for name, result := range lastResults {
			components[name] = result
		}

		// Calculate summary from cached results
		summary := HealthSummary{Total: len(components)}
		for _, result := range components {
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

		// Use manager's calculateOverallStatus (same package, no export needed)
		// 使用 manager 的 calculateOverallStatus（同包，无需导出）
		overall := h.manager.calculateOverallStatus(components, summary)

		detailed = DetailedHealth{
			Overall:    overall,
			Components: components,
			Summary:    summary,
			Timestamp:  time.Now(),
		}
	} else {
		detailed = h.manager.GetDetailedHealth(ctx)
	}

	// Set HTTP status based on overall health
	// 依据整体健康状态设置 HTTP 状态码
	var statusCode int
	switch detailed.Overall.Status {
	case StatusHealthy:
		statusCode = http.StatusOK
	case StatusDegraded:
		statusCode = http.StatusOK
	case StatusUnhealthy:
		statusCode = http.StatusServiceUnavailable
	default:
		statusCode = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	if err := json.NewEncoder(w).Encode(detailed); err != nil {
		h.logger.Error("Failed to encode detailed health response", zap.Error(err))
	}
}

// writeError writes an error response
func (h *HTTPHandler) writeError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)

	response := map[string]any{
		"error":     message,
		"timestamp": time.Now().Unix(),
	}

	if err := json.NewEncoder(w).Encode(response); err != nil {
		h.logger.Error("Failed to encode error response", zap.Error(err))
	}
}

// StartHealthServer starts a dedicated HTTP server for health checks
// StartHealthServer 启动专用于健康检查的 HTTP 服务
func StartHealthServer(manager *Manager, port int, logger *zap.Logger) *http.Server {
	handler := NewHTTPHandler(manager, logger)
	mux := http.NewServeMux()
	handler.RegisterRoutes(mux)

	server := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      mux,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info("Starting health check server", zap.Int("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Health check server failed", zap.Error(err))
		}
	}()

	return server
}
