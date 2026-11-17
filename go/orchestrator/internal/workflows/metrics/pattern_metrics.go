package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// Pattern execution counters
	// 所有 pattern 指标都带 workflow_version label，用于新旧版本灰度对比。
	PatternExecutions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_pattern_executions_total",
			Help: "Total number of pattern executions by type",
		},
		[]string{"pattern", "workflow_version"},
	)

	// Pattern execution duration
	// Pattern 执行耗时
	PatternDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "shannon_pattern_duration_seconds",
			Help:    "Duration of pattern executions in seconds",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"pattern", "workflow_version"},
	)

	// Agent executions by pattern
	// 按 pattern 统计的 agent 执行次数
	AgentExecutionsByPattern = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_agents_by_pattern_total",
			Help: "Total number of agents executed by pattern type",
		},
		[]string{"pattern", "workflow_version"},
	)

	// Token usage by pattern
	// 按 pattern 统计的 token 用量
	TokenUsageByPattern = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_tokens_by_pattern_total",
			Help: "Total tokens used by pattern type",
		},
		[]string{"pattern", "workflow_version"},
	)

	// Reflection improvements
	// 反思带来的质量改进
	ReflectionImprovements = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_reflection_improvements_total",
			Help: "Number of times reflection improved quality",
		},
		[]string{"workflow_version"},
	)

	// Workflow version distribution
	// workflow 版本分布
	WorkflowVersions = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "shannon_workflow_versions_total",
			Help: "Distribution of workflow executions by version",
		},
		[]string{"workflow", "version"},
	)
)

// RecordPatternExecution records a pattern execution
// RecordPatternExecution 记录一次 pattern 执行
func RecordPatternExecution(pattern, version string) {
	PatternExecutions.WithLabelValues(pattern, version).Inc()
}

// RecordPatternDuration records pattern execution duration
// RecordPatternDuration 记录 pattern 的执行耗时
func RecordPatternDuration(pattern, version string, seconds float64) {
	PatternDuration.WithLabelValues(pattern, version).Observe(seconds)
}

// RecordAgentExecution records agent executions for a pattern
// RecordAgentExecution 记录某个 pattern 下的 agent 执行次数
func RecordAgentExecution(pattern, version string, count int) {
	AgentExecutionsByPattern.WithLabelValues(pattern, version).Add(float64(count))
}

// RecordTokenUsage records token usage for a pattern
// RecordTokenUsage 记录某个 pattern 的 token 用量
func RecordTokenUsage(pattern, version string, tokens int) {
	TokenUsageByPattern.WithLabelValues(pattern, version).Add(float64(tokens))
}

// RecordReflectionImprovement records when reflection improves quality
// RecordReflectionImprovement 记录反思提升了质量的次数
func RecordReflectionImprovement(version string) {
	ReflectionImprovements.WithLabelValues(version).Inc()
}

// RecordWorkflowVersion records workflow version usage
// RecordWorkflowVersion 记录各 workflow 版本的使用量
func RecordWorkflowVersion(workflow, version string) {
	WorkflowVersions.WithLabelValues(workflow, version).Inc()
}
