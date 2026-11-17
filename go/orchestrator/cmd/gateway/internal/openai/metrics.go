package openai

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Request metrics
	// 请求指标
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openai_compat_requests_total",
			Help: "Total number of OpenAI-compatible API requests",
		},
		[]string{"model", "endpoint", "status"}, // status: success, error, rate_limited 注：status 取值：success、error、rate_limited
	)

	RequestLatency = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openai_compat_latency_seconds",
			Help:    "OpenAI-compatible API request latency in seconds",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300},
		},
		[]string{"model", "endpoint", "stream"},
	)

	// Token metrics
	// Token 指标
	TokensTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openai_compat_tokens_total",
			Help: "Total tokens used via OpenAI-compatible API",
		},
		[]string{"model", "type"}, // type: prompt, completion 注：type 取值：prompt、completion
	)

	TokensPerRequest = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openai_compat_tokens_per_request",
			Help:    "Token usage per request",
			Buckets: []float64{10, 50, 100, 500, 1000, 2000, 5000, 10000, 20000},
		},
		[]string{"model", "type"},
	)

	// Error metrics
	// 错误指标
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openai_compat_errors_total",
			Help: "Total number of OpenAI-compatible API errors",
		},
		[]string{"model", "error_type", "error_code"},
	)

	// Session metrics
	// Session 指标
	SessionsActive = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "openai_compat_sessions_active",
			Help: "Number of active OpenAI sessions",
		},
	)

	SessionsCreated = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openai_compat_sessions_created_total",
			Help: "Total number of OpenAI sessions created",
		},
	)

	SessionCollisions = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "openai_compat_session_collisions_total",
			Help: "Total number of session ID collisions detected",
		},
	)

	// Streaming metrics
	// 流式指标
	StreamChunksTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openai_compat_stream_chunks_total",
			Help: "Total number of SSE chunks sent",
		},
		[]string{"model"},
	)

	StreamDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openai_compat_stream_duration_seconds",
			Help:    "Duration of streaming responses",
			Buckets: []float64{1, 5, 10, 30, 60, 120, 300, 600},
		},
		[]string{"model"},
	)

	StreamErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openai_compat_stream_errors_total",
			Help: "Total number of streaming errors",
		},
		[]string{"model", "error_type"},
	)

	// Model usage metrics
	// Model 使用指标
	ModelRequests = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "openai_compat_model_requests_total",
			Help: "Total requests per model",
		},
		[]string{"model"},
	)

	// Time to first token (streaming)
	// 首个 token 耗时（流式）
	TimeToFirstToken = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "openai_compat_time_to_first_token_seconds",
			Help:    "Time from request start to first token in streaming mode",
			Buckets: []float64{0.1, 0.25, 0.5, 1, 2, 5, 10, 30},
		},
		[]string{"model"},
	)
)

// MetricsRecorder provides methods to record OpenAI API metrics
// MetricsRecorder 提供记录 OpenAI API 指标的方法
type MetricsRecorder struct {
	startTime time.Time
	model     string
	endpoint  string
	stream    bool
}

// NewMetricsRecorder creates a new metrics recorder for a request
// NewMetricsRecorder 为单次请求创建一个新的指标记录器
func NewMetricsRecorder(model, endpoint string, stream bool) *MetricsRecorder {
	ModelRequests.WithLabelValues(model).Inc()
	return &MetricsRecorder{
		startTime: time.Now(),
		model:     model,
		endpoint:  endpoint,
		stream:    stream,
	}
}

// RecordSuccess records a successful request
// RecordSuccess 记录一次成功的请求
func (m *MetricsRecorder) RecordSuccess() {
	duration := time.Since(m.startTime).Seconds()
	streamLabel := "false"
	if m.stream {
		streamLabel = "true"
	}

	RequestsTotal.WithLabelValues(m.model, m.endpoint, "success").Inc()
	RequestLatency.WithLabelValues(m.model, m.endpoint, streamLabel).Observe(duration)
}

// RecordError records an error
// RecordError 记录一次错误
func (m *MetricsRecorder) RecordError(errorType, errorCode string) {
	duration := time.Since(m.startTime).Seconds()
	streamLabel := "false"
	if m.stream {
		streamLabel = "true"
	}

	RequestsTotal.WithLabelValues(m.model, m.endpoint, "error").Inc()
	RequestLatency.WithLabelValues(m.model, m.endpoint, streamLabel).Observe(duration)
	ErrorsTotal.WithLabelValues(m.model, errorType, errorCode).Inc()
}

// RecordTokens records token usage
// RecordTokens 记录 token 用量
func (m *MetricsRecorder) RecordTokens(promptTokens, completionTokens int) {
	if promptTokens > 0 {
		TokensTotal.WithLabelValues(m.model, "prompt").Add(float64(promptTokens))
		TokensPerRequest.WithLabelValues(m.model, "prompt").Observe(float64(promptTokens))
	}
	if completionTokens > 0 {
		TokensTotal.WithLabelValues(m.model, "completion").Add(float64(completionTokens))
		TokensPerRequest.WithLabelValues(m.model, "completion").Observe(float64(completionTokens))
	}
}

// RecordStreamChunk records a stream chunk
// RecordStreamChunk 记录一个流式 chunk
func (m *MetricsRecorder) RecordStreamChunk() {
	StreamChunksTotal.WithLabelValues(m.model).Inc()
}

// RecordStreamComplete records stream completion
// RecordStreamComplete 记录流式响应完成
func (m *MetricsRecorder) RecordStreamComplete() {
	duration := time.Since(m.startTime).Seconds()
	StreamDuration.WithLabelValues(m.model).Observe(duration)
}

// RecordStreamError records a stream error
// RecordStreamError 记录一次流式错误
func (m *MetricsRecorder) RecordStreamError(errorType string) {
	StreamErrors.WithLabelValues(m.model, errorType).Inc()
}

// RecordFirstToken records time to first token
// RecordFirstToken 记录首个 token 的耗时
func (m *MetricsRecorder) RecordFirstToken() {
	duration := time.Since(m.startTime).Seconds()
	TimeToFirstToken.WithLabelValues(m.model).Observe(duration)
}

// RecordSessionCreated records a new session
// RecordSessionCreated 记录新建的 session
func RecordSessionCreated() {
	SessionsCreated.Inc()
}

// RecordSessionCollision records a session collision
// RecordSessionCollision 记录一次 session ID 冲突
func RecordSessionCollision() {
	SessionCollisions.Inc()
}

// UpdateActiveSessionsGauge updates the active sessions gauge
// UpdateActiveSessionsGauge 更新活跃 session 的 gauge
func UpdateActiveSessionsGauge(count float64) {
	SessionsActive.Set(count)
}
