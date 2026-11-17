package degradation

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.uber.org/zap"
)

// PartialResultsManager handles partial result aggregation and fallback behaviors
// PartialResultsManager 处理 partial result 聚合与 fallback 行为
type PartialResultsManager struct {
	logger   *zap.Logger
	strategy DegradationStrategy
}

// NewPartialResultsManager creates a new partial results manager
// NewPartialResultsManager 创建一个新的 partial results manager
func NewPartialResultsManager(strategy DegradationStrategy, logger *zap.Logger) *PartialResultsManager {
	return &PartialResultsManager{
		logger:   logger,
		strategy: strategy,
	}
}

// PartialResult represents a partial result from a failed operation
// PartialResult 表示来自失败操作的 partial result
type PartialResult struct {
	Source     string         `json:"source"`             // Component that generated result 注：生成该结果的组件
	Success    bool           `json:"success"`            // Whether this component succeeded 注：该组件是否成功
	Result     any            `json:"result"`             // Actual result data 注：实际结果数据
	Error      string         `json:"error,omitempty"`    // Error message if failed 注：失败时的错误消息
	Timestamp  time.Time      `json:"timestamp"`          // When result was generated 注：结果生成的时间
	Metadata   map[string]any `json:"metadata,omitempty"` // Additional context 注：附加上下文
	Confidence float64        `json:"confidence"`         // Confidence score (0-1) 注：置信度评分 (0-1)
	Degraded   bool           `json:"degraded"`           // Whether this was a degraded result 注：该结果是否为降级结果
}

// AggregatedResult combines multiple partial results into a coherent response
// AggregatedResult 将多个 partial result 合并为一个连贯的响应
type AggregatedResult struct {
	Success         bool             `json:"success"`                    // Overall success status 注：整体成功状态
	Result          any              `json:"result"`                     // Combined result 注：合并后的结果
	PartialResults  []PartialResult  `json:"partial_results"`            // Individual component results 注：各组件的结果
	TotalComponents int              `json:"total_components"`           // Total number of components attempted 注：尝试的组件总数
	SuccessCount    int              `json:"success_count"`              // Number of successful components 注：成功的组件数量
	FailureCount    int              `json:"failure_count"`              // Number of failed components 注：失败的组件数量
	DegradationInfo *DegradationInfo `json:"degradation_info,omitempty"` // Degradation context 注：降级上下文
	Timestamp       time.Time        `json:"timestamp"`                  // When aggregation was completed 注：聚合完成的时间
	Warning         string           `json:"warning,omitempty"`          // Warning message for partial results 注：针对 partial results 的警告消息
}

// DegradationInfo provides context about why results were degraded
// DegradationInfo 提供关于结果为何被降级的原因上下文
type DegradationInfo struct {
	Level              DegradationLevel `json:"level"`
	FailedDependencies []string         `json:"failed_dependencies"`
	Reason             string           `json:"reason"`
	RecommendedAction  string           `json:"recommended_action"`
}

// AggregateResults combines partial results into a meaningful response
// AggregateResults 将 partial result 合并为有意义的响应
func (prm *PartialResultsManager) AggregateResults(
	ctx context.Context,
	results []PartialResult,
	workflowType string,
) (*AggregatedResult, error) {
	if len(results) == 0 {
		return nil, fmt.Errorf("no results to aggregate")
	}

	successCount := 0
	var successfulResults []any
	var errors []string
	var warnings []string

	for _, result := range results {
		if result.Success {
			successCount++
			if result.Result != nil {
				successfulResults = append(successfulResults, result.Result)
			}
		} else {
			if result.Error != "" {
				errors = append(errors, fmt.Sprintf("%s: %s", result.Source, result.Error))
			}
		}

		if result.Degraded {
			warnings = append(warnings, fmt.Sprintf("degraded result from %s", result.Source))
		}
	}

	// Determine overall success based on partial success threshold
	// 根据部分成功阈值判断整体是否成功
	overallSuccess := prm.shouldConsiderSuccessful(successCount, len(results), workflowType)

	// Create aggregated result
	// 创建聚合结果
	aggregated := &AggregatedResult{
		Success:         overallSuccess,
		PartialResults:  results,
		TotalComponents: len(results),
		SuccessCount:    successCount,
		FailureCount:    len(results) - successCount,
		Timestamp:       time.Now(),
	}

	// Combine successful results
	// 合并成功的结果
	if len(successfulResults) > 0 {
		aggregated.Result = prm.combineResults(successfulResults, workflowType)
	}

	// Add degradation info if applicable
	// 在适用时添加降级信息
	shouldDegrade, degradationLevel, err := prm.strategy.ShouldDegrade(ctx)
	if err == nil && shouldDegrade {
		aggregated.DegradationInfo = &DegradationInfo{
			Level:             degradationLevel,
			Reason:            fmt.Sprintf("partial results due to %s degradation", degradationLevel.String()),
			RecommendedAction: prm.getRecommendedAction(degradationLevel),
		}

		// Collect failed dependencies
		// 收集失败的依赖
		var failedDeps []string
		for _, result := range results {
			if !result.Success {
				failedDeps = append(failedDeps, result.Source)
			}
		}
		aggregated.DegradationInfo.FailedDependencies = failedDeps
	}

	// Create warning message
	// 生成警告消息
	if len(warnings) > 0 || len(errors) > 0 {
		var warningParts []string
		if len(warnings) > 0 {
			warningParts = append(warningParts, strings.Join(warnings, "; "))
		}
		if len(errors) > 0 && !overallSuccess {
			warningParts = append(warningParts, fmt.Sprintf("errors: %s", strings.Join(errors, "; ")))
		}
		aggregated.Warning = strings.Join(warningParts, ". ")
	}

	// Record metrics
	// 记录指标
	RecordPartialResults(workflowType, fmt.Sprintf("success_ratio_%d_%d", successCount, len(results)))

	prm.logger.Info("Aggregated partial results",
		zap.String("workflow_type", workflowType),
		zap.Int("total_components", len(results)),
		zap.Int("success_count", successCount),
		zap.Int("failure_count", len(results)-successCount),
		zap.Bool("overall_success", overallSuccess),
		zap.String("warning", aggregated.Warning),
	)

	return aggregated, nil
}

// shouldConsiderSuccessful determines if partial results should be considered successful
// shouldConsiderSuccessful 判断 partial result 是否应被视为成功
func (prm *PartialResultsManager) shouldConsiderSuccessful(successCount, totalCount int, workflowType string) bool {
	if totalCount == 0 {
		return false
	}

	successRatio := float64(successCount) / float64(totalCount)

	// Define success thresholds per workflow type
	// 按 workflow type 定义成功阈值
	switch workflowType {
	case "simple":
		// Simple workflows need at least 1 success
		// simple workflow 至少需要 1 次成功
		return successCount >= 1
	case "standard":
		// Standard workflows need at least 50% success
		// standard workflow 至少需要 50% 成功
		return successRatio >= 0.5
	case "complex":
		// Complex workflows need at least 60% success
		// complex workflow 至少需要 60% 成功
		return successRatio >= 0.6
	case "agent_dag":
		// DAG workflows need at least 40% success (more lenient due to parallel execution)
		// DAG workflow 至少需要 40% 成功（因并行执行而更宽松）
		return successRatio >= 0.4
	default:
		// Default: need at least 50% success
		// 默认：成功率至少 50% 才算成功
		return successRatio >= 0.5
	}
}

// combineResults combines successful results into a single result
// combineResults 将成功的结果合并为单个结果
func (prm *PartialResultsManager) combineResults(results []any, workflowType string) any {
	if len(results) == 0 {
		return nil
	}

	if len(results) == 1 {
		return results[0]
	}

	// For multiple results, create a combined response
	// 多个结果时，创建合并后的响应
	switch workflowType {
	case "agent_dag":
		// For agent DAG, results are typically structured
		// 对于 agent DAG，结果通常是结构化的
		return map[string]any{
			"type":         "combined_agent_results",
			"results":      results,
			"result_count": len(results),
			"combined_at":  time.Now(),
		}
	default:
		// For other workflows, concatenate string results or return structured data
		// 对于其他 workflow，拼接字符串结果或返回结构化数据
		return map[string]any{
			"type":         "combined_results",
			"results":      results,
			"result_count": len(results),
			"combined_at":  time.Now(),
		}
	}
}

// getRecommendedAction returns recommended action based on degradation level
// getRecommendedAction 根据 degradation level 返回推荐操作
func (prm *PartialResultsManager) getRecommendedAction(level DegradationLevel) string {
	switch level {
	case LevelMinor:
		return "Monitor system health and retry if needed"
	case LevelModerate:
		return "Check failed dependencies and consider using cached results"
	case LevelSevere:
		return "Investigate system issues immediately and use fallback procedures"
	default:
		return "Monitor system status"
	}
}

// CreatePartialResult creates a partial result from a component execution
// CreatePartialResult 根据组件执行情况创建 partial result
func (prm *PartialResultsManager) CreatePartialResult(
	source string,
	success bool,
	result any,
	err error,
	confidence float64,
	degraded bool,
) PartialResult {
	var errorMsg string
	if err != nil {
		errorMsg = err.Error()
	}

	return PartialResult{
		Source:     source,
		Success:    success,
		Result:     result,
		Error:      errorMsg,
		Timestamp:  time.Now(),
		Confidence: confidence,
		Degraded:   degraded,
		Metadata:   make(map[string]any),
	}
}

// ShouldReturnPartialResults determines if partial results should be returned instead of failing
// ShouldReturnPartialResults 判断是否应返回 partial result 而不是直接失败
func (prm *PartialResultsManager) ShouldReturnPartialResults(
	ctx context.Context,
	workflowType string,
	successCount, totalCount int,
) (bool, error) {
	if totalCount == 0 {
		return false, nil
	}

	// Check if system is degraded
	// 检查系统是否已降级
	shouldDegrade, degradationLevel, err := prm.strategy.ShouldDegrade(ctx)
	if err != nil {
		return false, err
	}

	if !shouldDegrade {
		// No degradation, use normal failure logic
		// 未降级，使用正常的失败逻辑
		return false, nil
	}

	// In degraded state, be more lenient about partial results
	// 降级状态下对 partial result 更宽松
	successRatio := float64(successCount) / float64(totalCount)

	switch degradationLevel {
	case LevelMinor:
		// Minor degradation: return partial if we have any success
		// 轻度降级：只要有任意成功就返回 partial result
		return successCount > 0, nil
	case LevelModerate:
		// Moderate degradation: return partial if we have >= 25% success
		// 中度降级：成功率 >= 25% 时返回 partial result
		return successRatio >= 0.25, nil
	case LevelSevere:
		// Severe degradation: return partial if we have any success at all
		// 严重降级：只要有任意成功就返回 partial result
		return successCount > 0, nil
	default:
		return false, nil
	}
}
