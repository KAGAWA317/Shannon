package patterns

import (
	"maps"
	"strings"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	wopts "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// ReflectOnResult evaluates and potentially improves a result through iterative reflection.
// It uses the EvaluateResult activity to score the response and re-synthesizes with feedback if needed.
// Returns: improved result, final quality score, total tokens used, and any error.
// 用 EvaluateResult activity 打分，不达标则带反馈重新合成；返回改进结果、最终质量分、总 token 消耗与错误。
func ReflectOnResult(
	ctx workflow.Context,
	query string,
	initialResult string,
	agentResults []activities.AgentExecutionResult, // For re-synthesis 注：用于重新合成
	baseContext map[string]any,
	config ReflectionConfig,
	opts Options,
) (string, float64, int, error) {

	logger := workflow.GetLogger(ctx)
	finalResult := initialResult
	var totalTokens int
	var retryCount int
	var lastScore float64 = 0.5 // Default score 注：默认分值

	// Early exit if reflection is disabled
	// reflection 未启用时提前返回
	if !config.Enabled {
		return finalResult, lastScore, totalTokens, nil
	}

	for retryCount < config.MaxRetries {
		// Evaluate current result quality
		// 评估当前结果的质量
		var evalResult activities.EvaluateResultOutput
		evalCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: time.Duration(config.TimeoutMs) * time.Millisecond,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})

		err := workflow.ExecuteActivity(evalCtx, "EvaluateResult",
			activities.EvaluateResultInput{
				Query:    query,
				Response: finalResult,
				Criteria: config.Criteria,
			}).Get(ctx, &evalResult)

		if err != nil {
			logger.Warn("Reflection evaluation failed, using current result", "error", err)
			return finalResult, lastScore, totalTokens, nil
		}

		lastScore = evalResult.Score
		logger.Info("Reflection evaluation completed",
			"score", evalResult.Score,
			"threshold", config.ConfidenceThreshold,
			"retry_count", retryCount)

		// Check if meets quality threshold
		// 检查是否达到质量阈值
		if evalResult.Score >= config.ConfidenceThreshold {
			logger.Info("Response meets quality threshold, no retry needed")
			return finalResult, evalResult.Score, totalTokens, nil
		}

		// Result doesn't meet threshold, check if we can retry
		// 结果未达阈值，检查是否还能重试
		retryCount++
		if retryCount >= config.MaxRetries {
			logger.Info("Max reflection retries reached, using best effort result")
			return finalResult, evalResult.Score, totalTokens, nil
		}

		logger.Info("Response below threshold, retrying with reflection feedback",
			"feedback", evalResult.Feedback,
			"retry", retryCount)

		// Build reflection context with feedback
		// 构建带反馈的 reflection context
		reflectionContext := make(map[string]any)
		maps.Copy(reflectionContext, baseContext)
		reflectionContext["reflection_feedback"] = evalResult.Feedback
		reflectionContext["previous_response"] = finalResult
		reflectionContext["improvement_needed"] = true

		// Re-synthesize with feedback
		// 带上反馈重新合成
		var improvedSynthesis activities.SynthesisResult
		synthCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 2 * time.Minute, // Allow more time for synthesis 注：为合成留出更多时间
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
		})

		// Preserve top-level correlation for streaming by forwarding parent_workflow_id if present
		// 存在 parent_workflow_id 时转发它，以保留 streaming 的顶层关联
		parentID := ""
		if baseContext != nil {
			if p, ok := baseContext["parent_workflow_id"].(string); ok && p != "" {
				parentID = p
			}
		}
		err = workflow.ExecuteActivity(synthCtx, "SynthesizeResultsLLM",
			activities.SynthesisInput{
				Query:            query,
				AgentResults:     agentResults,
				Context:          reflectionContext,
				ParentWorkflowID: parentID,
			}).Get(ctx, &improvedSynthesis)

		if err != nil {
			logger.Warn("Reflection re-synthesis failed, keeping previous result", "error", err)
			return finalResult, evalResult.Score, totalTokens, nil
		}

		// Update result and track tokens
		// 更新结果并累计 token 消耗
		finalResult = improvedSynthesis.FinalResult
		totalTokens += improvedSynthesis.TokensUsed

		// Record re-synthesis token usage (always record; this path is not budgeted)
		// 记录重新合成的 token 消耗（始终记录；该路径不纳入预算）
		{
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if parentID != "" {
				wid = parentID
			}
			inTok := improvedSynthesis.InputTokens
			outTok := improvedSynthesis.CompletionTokens
			if inTok == 0 && outTok > 0 {
				est := improvedSynthesis.TokensUsed - outTok
				if est > 0 {
					inTok = est
				}
			}
			model := improvedSynthesis.ModelUsed
			if strings.TrimSpace(model) == "" {
				if m := pricing.GetPriorityOneModel(opts.ModelTier); m != "" {
					model = m
				}
			}
			provider := improvedSynthesis.Provider
			if strings.TrimSpace(provider) == "" {
				provider = imodels.DetectProvider(model)
			}
			recCtx := wopts.WithTokenRecordOptions(ctx)
			_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity,
				activities.TokenUsageInput{
					UserID:       opts.UserID,
					SessionID:    opts.SessionID,
					TaskID:       wid,
					AgentID:      "reflection-synth",
					Model:        model,
					Provider:     provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     map[string]any{"phase": "reflection_synth"},
				}).Get(recCtx, nil)
		}

		logger.Info("Reflection iteration completed",
			"retry", retryCount,
			"tokens_used", improvedSynthesis.TokensUsed)
	}

	return finalResult, lastScore, totalTokens, nil
}
