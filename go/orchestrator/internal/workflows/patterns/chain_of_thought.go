package patterns

import (
	"fmt"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	wopts "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ChainOfThoughtConfig configures the chain-of-thought pattern
// ChainOfThoughtConfig 配置 chain-of-thought 模式
type ChainOfThoughtConfig struct {
	MaxSteps              int    // Maximum reasoning steps 注：最大推理步数
	RequireExplanation    bool   // Require step-by-step explanation 注：要求逐步解释
	ShowIntermediateSteps bool   // Include intermediate reasoning in result 注：在结果中包含中间推理
	PromptTemplate        string // Custom prompt template 注：自定义 prompt 模板
	StepDelimiter         string // Delimiter between steps 注：步骤之间的分隔符
	ModelTier             string // Model tier to use 注：使用的模型档位
}

// ChainOfThoughtResult contains the result of chain-of-thought reasoning
// ChainOfThoughtResult 包含 chain-of-thought 推理的结果
type ChainOfThoughtResult struct {
	FinalAnswer    string
	ReasoningSteps []string
	TotalTokens    int
	Confidence     float64
	StepDurations  []time.Duration
}

// ChainOfThought implements step-by-step reasoning pattern
// This pattern guides an agent through explicit reasoning steps before reaching a conclusion
// ChainOfThought 实现逐步推理模式
// 该模式引导 agent 先走完显式推理步骤再得出结论
func ChainOfThought(
	ctx workflow.Context,
	query string,
	context map[string]any,
	sessionID string,
	history []string,
	config ChainOfThoughtConfig,
	opts Options,
) (*ChainOfThoughtResult, error) {

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Chain-of-Thought reasoning",
		"query", query,
		"max_steps", config.MaxSteps,
	)

	// Set activity options
	// 设置 activity 选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Set defaults
	// 设置默认值
	if config.MaxSteps == 0 {
		config.MaxSteps = 5
	}
	if config.StepDelimiter == "" {
		config.StepDelimiter = "\n→ "
	}
	if config.ModelTier == "" {
		config.ModelTier = opts.ModelTier
		if config.ModelTier == "" {
			config.ModelTier = "medium"
		}
	}

	result := &ChainOfThoughtResult{
		ReasoningSteps: make([]string, 0, config.MaxSteps),
		StepDurations:  make([]time.Duration, 0, config.MaxSteps),
	}

	// Build the chain-of-thought prompt
	// 构建 chain-of-thought prompt
	cotPrompt := buildChainOfThoughtPrompt(query, config)

	// Execute chain-of-thought reasoning
	// 执行 chain-of-thought 推理
	startTime := workflow.Now(ctx)

	var cotResult activities.AgentExecutionResult
	if opts.BudgetAgentMax > 0 {
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		// Prefer parent workflow ID from context when available for budget tracking and streaming
		// budget 记账与 event stream 都按 parent workflow 归集；用子 workflow 自己的 ID 会把统计割裂开。
		if context != nil {
			if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		err := workflow.ExecuteActivity(ctx,
			constants.ExecuteAgentWithBudgetActivity,
			activities.BudgetedAgentInput{
				AgentInput: activities.AgentExecutionInput{
					Query:            cotPrompt,
					AgentID:          "cot-reasoner",
					Context:          context,
					Mode:             "reasoning",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					ParentWorkflowID: wid,
				},
				MaxTokens: opts.BudgetAgentMax,
				UserID:    opts.UserID,
				TaskID:    wid,
				ModelTier: config.ModelTier,
			}).Get(ctx, &cotResult)
		if err != nil {
			return nil, fmt.Errorf("chain-of-thought reasoning failed: %w", err)
		}
	} else {
		// Determine parent workflow for streaming correlation
		// 确定用于 streaming 关联的 parent workflow
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if context != nil {
			if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		err := workflow.ExecuteActivity(ctx,
			activities.ExecuteAgent,
			activities.AgentExecutionInput{
				Query:            cotPrompt,
				AgentID:          "cot-reasoner",
				Context:          context,
				Mode:             "reasoning",
				SessionID:        sessionID,
				UserID:           opts.UserID,
				History:          history,
				ParentWorkflowID: wid,
			}).Get(ctx, &cotResult)
		if err != nil {
			return nil, fmt.Errorf("chain-of-thought reasoning failed: %w", err)
		}
	}

	duration := workflow.Now(ctx).Sub(startTime)
	result.StepDurations = append(result.StepDurations, duration)
	result.TotalTokens = cotResult.TokensUsed

	// Record token usage for the main CoT reasoning step when not budgeted
	// 走 budget activity 时已内部记账，这里只处理非 budget 路径，避免重复计费。
	if opts.BudgetAgentMax <= 0 {
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if context != nil {
			if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		inTok := cotResult.InputTokens
		outTok := cotResult.OutputTokens
		if inTok == 0 && outTok == 0 && cotResult.TokensUsed > 0 {
			inTok = cotResult.TokensUsed * 6 / 10
			outTok = cotResult.TokensUsed - inTok
		}
		model := cotResult.ModelUsed
		if strings.TrimSpace(model) == "" {
			if m := pricing.GetPriorityOneModel(config.ModelTier); m != "" {
				model = m
			}
		}
		provider := cotResult.Provider
		if strings.TrimSpace(provider) == "" {
			provider = imodels.DetectProvider(model)
		}
		recCtx := wopts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       opts.UserID,
			SessionID:    sessionID,
			TaskID:       wid,
			AgentID:      "cot-reasoner",
			Model:        model,
			Provider:     provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata:     map[string]any{"phase": "chain_of_thought"},
		}).Get(recCtx, nil)
		wopts.RecordToolCostEntries(ctx, cotResult, opts.UserID, sessionID, wid)
	}

	// Parse reasoning steps from response
	// 解析失败不报错：steps 为空会让 confidence 自然偏低，再由澄清分支兜底。
	steps := parseReasoningSteps(cotResult.Response, config.StepDelimiter)
	result.ReasoningSteps = steps

	// Extract final answer
	// 提取最终答案
	result.FinalAnswer = extractFinalAnswer(cotResult.Response, steps)

	// Calculate confidence based on reasoning clarity
	// 根据推理清晰度计算置信度
	result.Confidence = calculateReasoningConfidence(steps, cotResult.Response)

	// If we need to validate the reasoning, do an additional check
	// 仅当要求解释且置信度 < 0.7 时才多花一次 LLM 调用重问。
	if config.RequireExplanation && result.Confidence < 0.7 {
		logger.Info("Low confidence reasoning, requesting clarification")

		clarificationPrompt := fmt.Sprintf(
			"The previous reasoning for '%s' had unclear steps. Please provide a clearer step-by-step explanation:\n%s",
			query,
			strings.Join(steps, config.StepDelimiter),
		)

		var clarifyResult activities.AgentExecutionResult
		if opts.BudgetAgentMax > 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if context != nil {
				if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
			}
			err := workflow.ExecuteActivity(ctx,
				constants.ExecuteAgentWithBudgetActivity,
				activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            clarificationPrompt,
						AgentID:          "cot-clarifier",
						Context:          context,
						Mode:             "reasoning",
						SessionID:        sessionID,
						UserID:           opts.UserID,
						History:          append(history, fmt.Sprintf("Previous: %s", cotResult.Response)),
						ParentWorkflowID: wid,
					},
					MaxTokens: opts.BudgetAgentMax / 2, // Use less budget for clarification 注：澄清时使用更少的预算
					UserID:    opts.UserID,
					TaskID:    wid,
					ModelTier: config.ModelTier,
				}).Get(ctx, &clarifyResult)
			if err == nil {
				// Update with clarified reasoning
				// 用澄清后的推理更新结果
				clarifiedSteps := parseReasoningSteps(clarifyResult.Response, config.StepDelimiter)
				if len(clarifiedSteps) > 0 {
					result.ReasoningSteps = clarifiedSteps
					result.FinalAnswer = extractFinalAnswer(clarifyResult.Response, clarifiedSteps)
					result.Confidence = calculateReasoningConfidence(clarifiedSteps, clarifyResult.Response)
				}
				result.TotalTokens += clarifyResult.TokensUsed
				// Record clarification usage when not budgeted
				// 未走 budget 时记录澄清的用量
				if opts.BudgetAgentMax <= 0 {
					wid := workflow.GetInfo(ctx).WorkflowExecution.ID
					if context != nil {
						if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
							wid = p
						}
					}
					inTok := clarifyResult.InputTokens
					outTok := clarifyResult.OutputTokens
					if inTok == 0 && outTok == 0 && clarifyResult.TokensUsed > 0 {
						inTok = clarifyResult.TokensUsed * 6 / 10
						outTok = clarifyResult.TokensUsed - inTok
					}
					model := clarifyResult.ModelUsed
					if strings.TrimSpace(model) == "" {
						if m := pricing.GetPriorityOneModel(config.ModelTier); m != "" {
							model = m
						}
					}
					provider := clarifyResult.Provider
					if strings.TrimSpace(provider) == "" {
						provider = imodels.DetectProvider(model)
					}
					recCtx := wopts.WithTokenRecordOptions(ctx)
					_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
						UserID:       opts.UserID,
						SessionID:    sessionID,
						TaskID:       wid,
						AgentID:      "cot-clarifier",
						Model:        model,
						Provider:     provider,
						InputTokens:  inTok,
						OutputTokens: outTok,
						Metadata:     map[string]any{"phase": "chain_of_thought_clarify"},
					}).Get(recCtx, nil)
					wopts.RecordToolCostEntries(ctx, clarifyResult, opts.UserID, sessionID, wid)
				}
			}
		}
	}

	// Format the result based on configuration
	// 按配置格式化结果
	if config.ShowIntermediateSteps {
		stepsText := strings.Join(result.ReasoningSteps, config.StepDelimiter)
		result.FinalAnswer = fmt.Sprintf(
			"Reasoning:\n%s\n\nFinal Answer: %s",
			stepsText,
			result.FinalAnswer,
		)
	}

	logger.Info("Chain-of-Thought completed",
		"steps", len(result.ReasoningSteps),
		"tokens", result.TotalTokens,
		"confidence", result.Confidence,
	)

	return result, nil
}

// buildChainOfThoughtPrompt creates the prompt for chain-of-thought reasoning
// buildChainOfThoughtPrompt 构建 chain-of-thought 推理所需的 prompt
func buildChainOfThoughtPrompt(query string, config ChainOfThoughtConfig) string {
	if config.PromptTemplate != "" {
		return strings.ReplaceAll(config.PromptTemplate, "{query}", query)
	}

	// Default chain-of-thought prompt
	// 默认的 chain-of-thought prompt
	return fmt.Sprintf(`Please solve this step-by-step:

Question: %s

Think through this systematically:
1. First, identify what is being asked
2. Break down the problem into steps
3. Work through each step with clear reasoning
4. Show your work and explain your thinking
5. Arrive at the final answer

Use "→" to mark each reasoning step.
End with "Therefore:" followed by your final answer.`, query)
}

// parseReasoningSteps extracts reasoning steps from the response
// parseReasoningSteps 从 response 中提取推理步骤
func parseReasoningSteps(response, delimiter string) []string {
	// Look for step markers
	// 查找 step 标记
	lines := strings.Split(response, "\n")
	steps := []string{}

	for _, line := range lines {
		line = strings.TrimSpace(line)
		// Check for step indicators
		// 检查步骤指示符
		if strings.HasPrefix(line, "→") ||
			strings.HasPrefix(line, "Step") ||
			strings.HasPrefix(line, "1.") ||
			strings.HasPrefix(line, "2.") ||
			strings.HasPrefix(line, "3.") ||
			strings.HasPrefix(line, "•") {
			steps = append(steps, line)
		}
	}

	// If no explicit steps found, try to extract logical segments
	// 没有 step 标记时退化为按句切分，超过 20 字符才算有效片段。
	if len(steps) == 0 {
		segments := strings.SplitSeq(response, ". ")
		for segment := range segments {
			if len(strings.TrimSpace(segment)) > 20 { // Meaningful segment 注：有效片段
				steps = append(steps, segment)
				if len(steps) >= 5 { // Limit to reasonable number 注：限制在合理数量
					break
				}
			}
		}
	}

	return steps
}

// extractFinalAnswer gets the final answer from the reasoning
// extractFinalAnswer 从推理中取出最终答案
func extractFinalAnswer(response string, steps []string) string {
	// Look for explicit final answer markers
	// 查找显式的最终答案标记
	markers := []string{
		"Therefore:",
		"Final Answer:",
		"The answer is:",
		"In conclusion:",
		"Result:",
	}

	lowerResponse := strings.ToLower(response)
	for _, marker := range markers {
		markerLower := strings.ToLower(marker)
		if idx := strings.Index(lowerResponse, markerLower); idx != -1 {
			answer := response[idx+len(marker):]
			// Take first paragraph/sentence as answer
			// 取第一段/第一句作为答案
			if endIdx := strings.Index(answer, "\n\n"); endIdx > 0 {
				answer = answer[:endIdx]
			}
			return strings.TrimSpace(answer)
		}
	}

	// If no explicit marker, use last step or last paragraph
	// 找不到标记时取最后一步/最后一段，比返回全文更接近用户要的答案。
	if len(steps) > 0 {
		return steps[len(steps)-1]
	}

	// Fallback: last paragraph
	// 兜底：取最后一段
	paragraphs := strings.Split(response, "\n\n")
	if len(paragraphs) > 0 {
		return paragraphs[len(paragraphs)-1]
	}

	return response
}

// calculateReasoningConfidence estimates confidence based on reasoning quality
// calculateReasoningConfidence 依据推理质量估算置信度
func calculateReasoningConfidence(steps []string, response string) float64 {
	confidence := 0.5 // Base confidence 注：基础置信度

	// More steps indicate thorough reasoning
	// 步骤越多说明推理越充分
	if len(steps) >= 3 {
		confidence += 0.2
	}

	// Check for logical connectors
	// 检查逻辑连接词
	logicalTerms := []string{
		"therefore", "because", "since", "thus",
		"consequently", "hence", "so", "implies",
	}
	lowerResponse := strings.ToLower(response)
	logicalCount := 0
	for _, term := range logicalTerms {
		logicalCount += strings.Count(lowerResponse, term)
	}
	if logicalCount >= 3 {
		confidence += 0.15
	}

	// Check for structured reasoning
	// 检查是否为结构化推理
	if strings.Contains(response, "Step") || strings.Contains(response, "→") {
		confidence += 0.1
	}

	// Check for conclusion
	// 检查是否给出结论
	if strings.Contains(lowerResponse, "therefore") ||
		strings.Contains(lowerResponse, "final answer") {
		confidence += 0.05
	}

	// Cap at 1.0
	// 上限为 1.0
	if confidence > 1.0 {
		confidence = 1.0
	}

	return confidence
}
