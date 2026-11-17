package policy

import (
	"crypto/sha1"
	"fmt"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Policy evaluation metrics
	// policy 评估指标
	policyEvaluations = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_evaluations_total",
			Help: "Total number of policy evaluations",
		},
		[]string{"decision", "mode", "reason"},
	)

	policyEvaluationDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "shannon_policy_evaluation_duration_seconds",
			Help:    "Time spent evaluating policies",
			Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s 注：1ms 到约 1s
		},
		[]string{"mode"},
	)

	policyErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_errors_total",
			Help: "Total number of policy evaluation errors",
		},
		[]string{"error_type", "mode"},
	)

	// Dry-run comparison metrics
	// dry-run 对比指标
	policyDryRunDivergence = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_dry_run_divergence_total",
			Help: "Cases where dry-run decision differs from default allow",
		},
		[]string{"divergence_type"}, // "would_deny", "would_allow" 注：取值为 "would_deny" 或 "would_allow"
	)

	// Policy load metrics
	// policy 加载指标
	policyLoadTime = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shannon_policy_load_timestamp_seconds",
			Help: "Timestamp of last successful policy load",
		},
		[]string{"policy_path"},
	)

	policyCount = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shannon_policy_files_loaded",
			Help: "Number of policy files currently loaded",
		},
		[]string{"policy_path"},
	)

	// Cache performance metrics
	// 缓存性能指标
	policyCacheHits = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_cache_hits_total",
			Help: "Total number of policy cache hits",
		},
		[]string{"effective_mode"},
	)

	policyCacheMisses = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_cache_misses_total",
			Help: "Total number of policy cache misses",
		},
		[]string{"effective_mode"},
	)

	policyCacheSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shannon_policy_cache_entries",
			Help: "Current number of entries in policy cache",
		},
		[]string{"cache_type"},
	)

	// Canary deployment metrics
	// canary 部署指标
	policyCanaryDecisions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_canary_decisions_total",
			Help: "Policy decisions broken down by canary routing",
		},
		[]string{"configured_mode", "effective_mode", "routing_reason", "decision"},
	)

	// Top deny reasons tracking (top 10 most frequent)
	// 拒绝原因 top 统计（最常见的 top 10）
	policyDenyReasons = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_deny_reasons_total",
			Help: "Count of policy denials by reason (top reasons only)",
		},
		[]string{"reason_hash", "mode", "truncated_reason"},
	)

	// Policy version tracking
	// policy 版本追踪
	policyVersionInfo = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "shannon_policy_version_info",
			Help: "Policy version information (value always 1, labels contain version data)",
		},
		[]string{"policy_path", "version_hash", "load_timestamp"},
	)

	// Enhanced dry-run analysis
	// 增强的 dry-run 分析
	policyModeComparison = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_mode_comparison_total",
			Help: "Comparison of what decisions would be made in enforce vs dry-run",
		},
		[]string{"original_decision", "effective_mode", "would_enforce_decision", "user_type"},
	)

	// SLO tracking metrics
	// SLO 追踪指标
	policyLatencyObjective = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "shannon_policy_latency_slo_seconds",
			Help:    "Policy evaluation latency for SLO tracking",
			Buckets: []float64{0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0},
		},
		[]string{"effective_mode", "cache_hit"},
	)

	// Error rate SLO tracking
	// 错误率 SLO 追踪
	policyErrorRate = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_policy_slo_errors_total",
			Help: "Policy errors for SLO error rate calculation",
		},
		[]string{"error_category", "effective_mode"},
	)
)

// RecordEvaluation records a policy evaluation result
// RecordEvaluation 记录一次 policy 评估结果
func RecordEvaluation(decision string, mode string, reason string) {
	policyEvaluations.WithLabelValues(decision, mode, reason).Inc()
}

// RecordEvaluationDuration records the time spent evaluating a policy
// RecordEvaluationDuration 记录评估 policy 所花时间
func RecordEvaluationDuration(mode string, duration float64) {
	policyEvaluationDuration.WithLabelValues(mode).Observe(duration)
}

// RecordError records a policy evaluation error
// RecordError 记录一次 policy 评估错误
func RecordError(errorType string, mode string) {
	policyErrors.WithLabelValues(errorType, mode).Inc()
}

// RecordDryRunDivergence records when dry-run differs from default behavior
// RecordDryRunDivergence 记录 dry-run 与默认行为不一致的情况
func RecordDryRunDivergence(divergenceType string) {
	policyDryRunDivergence.WithLabelValues(divergenceType).Inc()
}

// RecordPolicyLoad records successful policy loading
// RecordPolicyLoad 记录 policy 加载成功
func RecordPolicyLoad(policyPath string, count int, timestamp float64) {
	policyLoadTime.WithLabelValues(policyPath).Set(timestamp)
	policyCount.WithLabelValues(policyPath).Set(float64(count))
}

// RecordCacheHit records a policy cache hit
// RecordCacheHit 记录一次 policy 缓存命中
func RecordCacheHit(effectiveMode string) {
	policyCacheHits.WithLabelValues(effectiveMode).Inc()
}

// RecordCacheMiss records a policy cache miss
// RecordCacheMiss 记录一次 policy 缓存未命中
func RecordCacheMiss(effectiveMode string) {
	policyCacheMisses.WithLabelValues(effectiveMode).Inc()
}

// RecordCacheSize records current cache size
// RecordCacheSize 记录当前缓存大小
func RecordCacheSize(cacheType string, size int) {
	policyCacheSize.WithLabelValues(cacheType).Set(float64(size))
}

// RecordCanaryDecision records a canary routing decision
// RecordCanaryDecision 记录一次 canary 路由决策
func RecordCanaryDecision(configuredMode, effectiveMode, routingReason, decision string) {
	policyCanaryDecisions.WithLabelValues(configuredMode, effectiveMode, routingReason, decision).Inc()
}

// RecordDenyReason records a denial reason (top reasons tracking)
// RecordDenyReason 记录一次拒绝原因（top 原因追踪）
func RecordDenyReason(reason, mode string) {
	// Hash the reason for consistent labeling while limiting cardinality
	// 对 reason 做哈希，既保证标签一致，又限制指标基数
	reasonHash := hashString(reason)
	truncatedReason := util.TruncateString(reason, 50, true) // Limit label size 注：限制标签长度
	policyDenyReasons.WithLabelValues(reasonHash, mode, truncatedReason).Inc()
}

// RecordPolicyVersion records policy version information
// RecordPolicyVersion 记录 policy 版本信息
func RecordPolicyVersion(policyPath, versionHash, loadTimestamp string) {
	policyVersionInfo.WithLabelValues(policyPath, versionHash, loadTimestamp).Set(1)
}

// RecordModeComparison records comparison between dry-run and enforce decisions
// RecordModeComparison 记录 dry-run 与 enforce 决策的对比
func RecordModeComparison(originalDecision, effectiveMode, wouldEnforceDecision, userType string) {
	policyModeComparison.WithLabelValues(originalDecision, effectiveMode, wouldEnforceDecision, userType).Inc()
}

// RecordSLOLatency records latency for SLO tracking with enhanced labels
// RecordSLOLatency 记录用于 SLO 追踪的延迟，标签更细
func RecordSLOLatency(effectiveMode string, cacheHit bool, duration float64) {
	cacheLabel := "miss"
	if cacheHit {
		cacheLabel = "hit"
	}
	policyLatencyObjective.WithLabelValues(effectiveMode, cacheLabel).Observe(duration)
}

// RecordSLOError records an error for SLO error rate calculation
// RecordSLOError 记录用于 SLO 错误率计算的错误
func RecordSLOError(errorCategory, effectiveMode string) {
	policyErrorRate.WithLabelValues(errorCategory, effectiveMode).Inc()
}

// Helper functions
// 辅助函数

// hashString creates a consistent hash for high-cardinality strings
// hashString 为高基数字符串生成一致的哈希
func hashString(s string) string {
	h := sha1.Sum([]byte(s))
	return fmt.Sprintf("%x", h[:4]) // Use first 8 hex chars for reasonable uniqueness 注：取前 8 个十六进制字符即可保证足够的区分度
}

// Truncation unified in util.TruncateString
// 截断逻辑统一在 util.TruncateString
