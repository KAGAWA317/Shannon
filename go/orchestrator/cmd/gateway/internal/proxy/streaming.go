package proxy

import (
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"

	"go.uber.org/zap"
)

// StreamingProxy handles reverse proxying to the admin server for SSE/WebSocket
// StreamingProxy 负责把 SSE/WebSocket 反向代理到 admin server
type StreamingProxy struct {
	proxy  *httputil.ReverseProxy
	target *url.URL
	logger *zap.Logger
}

// NewStreamingProxy creates a new streaming proxy
// NewStreamingProxy 创建新的 streaming proxy
func NewStreamingProxy(adminURL string, logger *zap.Logger) (*StreamingProxy, error) {
	target, err := url.Parse(adminURL)
	if err != nil {
		return nil, err
	}

	proxy := httputil.NewSingleHostReverseProxy(target)

	// Customize the proxy for SSE support
	// 为支持 SSE 定制该 proxy
	sp := &StreamingProxy{
		proxy:  proxy,
		target: target,
		logger: logger,
	}

	// Customize Director to rewrite paths and preserve headers
	// 定制 Director 以重写路径并保留头
	originalDirector := proxy.Director
	proxy.Director = func(req *http.Request) {
		originalDirector(req)

		// Rewrite paths: /api/v1/stream/sse -> /stream/sse, /api/v1/blob/* -> /blob/*
		// 重写路径：/api/v1/stream/sse -> /stream/sse，/api/v1/blob/* -> /blob/*
		req.URL.Path = strings.Replace(req.URL.Path, "/api/v1/stream", "/stream", 1)
		req.URL.Path = strings.Replace(req.URL.Path, "/api/v1/blob", "/blob", 1)

		// Don't forward auth query params upstream (they may appear in logs).
		// Gateway auth middleware validates them before proxying.
		// 不要把 auth query 参数转发到上游（它们可能进入日志）；gateway 的 auth middleware 在代理前已完成校验。
		q := req.URL.Query()
		changed := false
		if q.Has("token") {
			q.Del("token")
			changed = true
		}
		if q.Has("api_key") {
			q.Del("api_key")
			changed = true
		}
		if changed {
			req.URL.RawQuery = q.Encode()
		}

		// Preserve important headers
		// 保留重要的头
		req.Host = target.Host

		// Log proxy request
		// 记录代理 request 日志
		logger.Debug("Proxying streaming request",
			zap.String("original_path", req.URL.Path),
			zap.String("target", target.String()),
		)
	}

	// Customize ModifyResponse to ensure SSE works properly
	// 定制 ModifyResponse 以确保 SSE 正常工作
	proxy.ModifyResponse = func(resp *http.Response) error {
		// Ensure proper headers for SSE
		// 为 SSE 补齐必要的头
		if strings.Contains(resp.Header.Get("Content-Type"), "text/event-stream") {
			resp.Header.Set("Cache-Control", "no-cache")
			resp.Header.Set("Connection", "keep-alive")
			resp.Header.Set("X-Accel-Buffering", "no")
		}

		// CORS headers handled by CORS middleware, not here
		// CORS 头由 CORS 中间件处理，不在此处

		return nil
	}

	// Set FlushInterval to 0 for immediate flushing (important for SSE)
	// FlushInterval 设为 -1 表示立即 flush（对 SSE 至关重要）。
	proxy.FlushInterval = -1 // Negative value means flush immediately 注：负值表示立即 flush

	return sp, nil
}

// ServeHTTP handles the HTTP request
// ServeHTTP 处理 HTTP request
func (sp *StreamingProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Check if this is a WebSocket upgrade request
	// 判断是否为 WebSocket 升级 request
	if sp.isWebSocketRequest(r) {
		sp.handleWebSocket(w, r)
		return
	}

	// Handle as regular HTTP/SSE request
	// 按普通 HTTP/SSE request 处理
	sp.proxy.ServeHTTP(w, r)
}

// isWebSocketRequest checks if the request is for WebSocket upgrade
// isWebSocketRequest 判断该 request 是否为 WebSocket 升级请求
func (sp *StreamingProxy) isWebSocketRequest(r *http.Request) bool {
	return strings.ToLower(r.Header.Get("Upgrade")) == "websocket"
}

// handleWebSocket handles WebSocket proxy
func (sp *StreamingProxy) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// WebSocket requires special handling
	// For simplicity, we'll use the default proxy which handles WebSocket
	// WebSocket 需要特殊处理；为简单起见，直接使用默认 proxy（它已支持 WebSocket）。
	sp.proxy.ServeHTTP(w, r)
}
