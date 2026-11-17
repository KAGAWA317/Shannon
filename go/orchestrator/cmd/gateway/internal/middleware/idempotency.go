package middleware

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// IdempotencyMiddleware provides idempotency key support
// IdempotencyMiddleware 提供幂等键支持
type IdempotencyMiddleware struct {
	redis  *redis.Client
	logger *zap.Logger
	ttl    time.Duration
}

// NewIdempotencyMiddleware creates a new idempotency middleware
// NewIdempotencyMiddleware 创建新的幂等中间件
func NewIdempotencyMiddleware(redis *redis.Client, logger *zap.Logger) *IdempotencyMiddleware {
	return &IdempotencyMiddleware{
		redis:  redis,
		logger: logger,
		ttl:    24 * time.Hour, // Store idempotency results for 24 hours 注：幂等结果保存 24 小时
	}
}

// IdempotencyResult stores the cached result of an idempotent request
// IdempotencyResult 保存幂等请求的缓存结果
type IdempotencyResult struct {
	StatusCode int                 `json:"status_code"`
	Headers    map[string][]string `json:"headers"`
	Body       []byte              `json:"body"`
	Timestamp  time.Time           `json:"timestamp"`
}

// responseRecorder captures the response for caching
// responseRecorder 捕获响应以便缓存
type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
	written    bool
}

func newResponseRecorder(w http.ResponseWriter) *responseRecorder {
	return &responseRecorder{
		ResponseWriter: w,
		statusCode:     http.StatusOK,
		body:           &bytes.Buffer{},
	}
}

func (r *responseRecorder) WriteHeader(code int) {
	if !r.written {
		r.statusCode = code
		r.written = true
		r.ResponseWriter.WriteHeader(code)
	}
}

func (r *responseRecorder) Write(b []byte) (int, error) {
	if !r.written {
		r.WriteHeader(http.StatusOK)
	}
	r.body.Write(b)
	return r.ResponseWriter.Write(b)
}

// Middleware returns the HTTP middleware function
// Middleware 返回 HTTP 中间件函数
func (im *IdempotencyMiddleware) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Only process POST requests with idempotency key
		// 只处理带幂等键的 POST 请求
		if r.Method != http.MethodPost {
			next.ServeHTTP(w, r)
			return
		}

		idempotencyKey := r.Header.Get("Idempotency-Key")
		if idempotencyKey == "" {
			// No idempotency key, proceed normally
			// 没有幂等键，正常处理
			next.ServeHTTP(w, r)
			return
		}

		ctx := r.Context()

		// Generate cache key including request details
		// 生成包含请求细节的 cache key
		cacheKey := im.generateCacheKey(r, idempotencyKey)

		// Check if we have a cached result
		// 检查是否已有缓存结果
		cached, err := im.getCachedResult(ctx, cacheKey)
		if err == nil && cached != nil {
			// Return cached response
			// 返回缓存的响应
			im.logger.Debug("Returning cached idempotent response",
				zap.String("idempotency_key", idempotencyKey),
				zap.String("path", r.URL.Path),
			)

			// Set headers
			// 设置 header
			for key, values := range cached.Headers {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}

			// Add idempotency header
			// 添加幂等 header
			w.Header().Set("X-Idempotency-Cached", "true")
			w.Header().Set("X-Idempotency-Key", idempotencyKey)

			// Write response
			// 写出响应
			w.WriteHeader(cached.StatusCode)
			w.Write(cached.Body)
			return
		}

		// No cached result, process request and cache the response
		// 无缓存结果，处理请求并缓存响应
		recorder := newResponseRecorder(w)

		// Process the request
		// 处理请求
		next.ServeHTTP(recorder, r)

		// Only cache successful responses (2xx)
		// 只缓存成功的响应（2xx）
		if recorder.statusCode >= 200 && recorder.statusCode < 300 {
			result := &IdempotencyResult{
				StatusCode: recorder.statusCode,
				Headers:    recorder.Header(),
				Body:       recorder.body.Bytes(),
				Timestamp:  time.Now(),
			}

			// Store result in cache
			// 把结果存入 cache
			if err := im.cacheResult(ctx, cacheKey, result); err != nil {
				im.logger.Error("Failed to cache idempotent response",
					zap.Error(err),
					zap.String("idempotency_key", idempotencyKey),
				)
			} else {
				im.logger.Debug("Cached idempotent response",
					zap.String("idempotency_key", idempotencyKey),
					zap.String("path", r.URL.Path),
					zap.Int("status_code", recorder.statusCode),
				)
			}
		}
	})
}

// generateCacheKey generates a unique cache key for the request
// generateCacheKey 为请求生成唯一的 cache key
func (im *IdempotencyMiddleware) generateCacheKey(r *http.Request, idempotencyKey string) string {
	// Include user context in the key to prevent cross-user cache pollution
	// 把用户信息纳入 key，避免不同用户之间 cache 串用。
	userID := ""
	if userCtx := r.Context().Value(auth.UserContextKey); userCtx != nil {
		if uc, ok := userCtx.(*auth.UserContext); ok {
			userID = uc.UserID.String()
		}
	}

	// Create a hash of the request details
	// 对请求细节计算 hash
	h := sha256.New()
	h.Write([]byte(idempotencyKey))
	h.Write([]byte(userID))
	h.Write([]byte(r.URL.Path))

	// Include request body in the hash (for extra safety)
	// 把请求体也纳入 hash（更保险）
	if r.Body != nil {
		body, _ := io.ReadAll(r.Body)
		r.Body = io.NopCloser(bytes.NewReader(body))
		h.Write(body)
	}

	hash := hex.EncodeToString(h.Sum(nil))
	return fmt.Sprintf("idempotency:%s", hash[:16]) // Use first 16 chars of hash 注：取 hash 的前 16 个字符
}

// getCachedResult retrieves a cached result from Redis
// getCachedResult 从 Redis 取回缓存结果
func (im *IdempotencyMiddleware) getCachedResult(ctx context.Context, key string) (*IdempotencyResult, error) {
	data, err := im.redis.Get(ctx, key).Bytes()
	if err != nil {
		return nil, err
	}

	var result IdempotencyResult
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// cacheResult stores a result in Redis
// cacheResult 把结果存入 Redis
func (im *IdempotencyMiddleware) cacheResult(ctx context.Context, key string, result *IdempotencyResult) error {
	data, err := json.Marshal(result)
	if err != nil {
		return err
	}

	return im.redis.Set(ctx, key, data, im.ttl).Err()
}

// ServeHTTP implements http.Handler interface
// ServeHTTP 实现 http.Handler 接口
func (im *IdempotencyMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	w.Write([]byte(`{"error":"Direct access not allowed"}`))
}
