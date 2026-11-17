package strategies

import (
	"fmt"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"go.temporal.io/sdk/workflow"
)

// convertHistoryForAgent converts message history to string format for agents
// convertHistoryForAgent 把消息历史转成给 agent 用的字符串格式
func convertHistoryForAgent(history []Message) []string {
	result := make([]string, len(history))
	for i, msg := range history {
		content := strings.ReplaceAll(msg.Content, "\n", "\\n")
		result[i] = fmt.Sprintf("%s: %s", msg.Role, content)
	}
	return result
}

// determineModelTier selects a model tier based on context and default
// determineModelTier 依据 context 与 default 选择 model tier
func determineModelTier(context map[string]any, defaultTier string) string {
	// Check for explicit model tier in context (highest priority)
	// 检查 context 里显式指定的 model tier（最高优先级）
	if tier, ok := context["model_tier"].(string); ok && strings.TrimSpace(tier) != "" {
		return strings.ToLower(strings.TrimSpace(tier))
	}

	// Get thresholds from config (with defaults)
	// 从配置取阈值（带默认值）
	simpleThreshold := 0.3
	mediumThreshold := 0.5 // Changed default from 0.7 to 0.5 注：默认值由 0.7 改为 0.5
	if cfg, ok := context["config"].(*activities.WorkflowConfig); ok && cfg != nil {
		if cfg.ComplexitySimpleThreshold > 0 {
			simpleThreshold = cfg.ComplexitySimpleThreshold
		}
		if cfg.ComplexityMediumThreshold > 0 {
			mediumThreshold = cfg.ComplexityMediumThreshold
		}
	}

	// Strategy-aware overrides for research workflows (applied before pure complexity routing)
	// NOTE: These tiers apply to AGENT EXECUTION only.
	// Final synthesis always uses "large" tier (hardcoded in synthesis.go:361).
	// Utility activities (coverage_eval, fact_extraction, etc.) use "small" tier.
	// 这些档位只作用于 agent 执行；最终合成固定用 "large"（见 synthesis.go）；
	// tool 型 activity（coverage_eval、fact_extraction 等）用 "small"。
	if strategyRaw, ok := context["research_strategy"].(string); ok && strings.TrimSpace(strategyRaw) != "" {
		strategy := strings.ToLower(strings.TrimSpace(strategyRaw))

		switch strategy {
		case "quick":
			// Fast, cheap research regardless of complexity.
			// 快且便宜，不看复杂度。
			return "small"
		case "standard":
			// Default research path uses small agents, large synthesis.
			// 默认研究路径用小 model 的 agent、大 model 做合成。
			return "small"
		case "deep":
			// Use medium for agent execution; final synthesis will use large.
			// Research shows agentic workflows with smaller models + iteration
			// match or outperform single large-model calls at lower cost.
			// agent 执行用 medium，最终合成用 large：研究表明小 model + 迭代的 agent workflow
			// 能以更低成本达到甚至超过单次 large model 调用。
			return "medium"
		case "academic":
			// Academic uses medium for agent execution; final synthesis uses large.
			// academic 档位 agent 执行用 medium，最终合成用 large。
			// Cost optimization: only synthesis benefits from large tier.
			// 为省成本：只有合成值得用 large 档。
			return "medium"
		}
	}

	// Fall back to complexity-based routing
	// 回退到按复杂度路由
	if complexity, ok := context["complexity"].(float64); ok {
		if complexity < simpleThreshold {
			return "small"
		} else if complexity < mediumThreshold {
			return "medium"
		}
		return "large"
	}

	// Use default if provided
	// 提供了 default 就用 default
	if defaultTier != "" {
		return defaultTier
	}

	return "medium"
}

// validateInput validates the input for a workflow
// validateInput 校验 workflow 的输入
func validateInput(input TaskInput) error {
	if input.Query == "" {
		return fmt.Errorf("query cannot be empty")
	}
	if len(input.Query) > 10000 {
		return fmt.Errorf("query exceeds maximum length of 10000 characters")
	}
	return nil
}

// getBudgetMax extracts the budget maximum from context
// getBudgetMax 从 context 取出预算上限
func getBudgetMax(context map[string]any) int {
	if v, ok := context["budget_agent_max"].(int); ok {
		return v
	}
	if v, ok := context["budget_agent_max"].(float64); ok && v > 0 {
		return int(v)
	}
	return 0
}

// getWorkflowConfig loads workflow configuration with defaults
// getWorkflowConfig 加载 workflow 配置，失败时用默认值
func getWorkflowConfig(ctx workflow.Context) activities.WorkflowConfig {
	var config activities.WorkflowConfig
	configActivity := workflow.ExecuteActivity(workflow.WithActivityOptions(ctx,
		workflow.ActivityOptions{StartToCloseTimeout: 10 * time.Second}),
		activities.GetWorkflowConfig,
	)
	if err := configActivity.Get(ctx, &config); err != nil {
		workflow.GetLogger(ctx).Warn("Failed to load config, using defaults", "error", err)
		// Return sensible defaults
		// 返回一组合理的默认值
		config = activities.WorkflowConfig{
			ExploratoryMaxIterations:         5,
			ExploratoryConfidenceThreshold:   0.85,
			ExploratoryBranchFactor:          3,
			ExploratoryMaxConcurrentAgents:   3,
			ScientificMaxHypotheses:          3,
			ScientificMaxIterations:          4,
			ScientificConfidenceThreshold:    0.85,
			ScientificContradictionThreshold: 0.2,
		}
	}
	return config
}

// extractPersonaHints extracts persona suggestions from context
// extractPersonaHints 从 context 提取 persona 建议
func extractPersonaHints(context map[string]any) []string {
	hints := []string{}

	// Check for domain keywords
	// 检查领域关键词
	if domain, ok := context["domain"].(string); ok {
		switch domain {
		case "finance", "trading", "investment":
			hints = append(hints, "financial-analyst")
		case "engineering", "technical", "code":
			hints = append(hints, "software-engineer")
		case "medical", "health", "clinical":
			hints = append(hints, "medical-expert")
		case "legal", "law", "compliance":
			hints = append(hints, "legal-advisor")
		case "research", "academic", "science":
			hints = append(hints, "researcher")
		}
	}

	// Check for task type hints
	// 检查任务类型线索
	if taskType, ok := context["task_type"].(string); ok {
		switch taskType {
		case "analysis":
			hints = append(hints, "analyst")
		case "creative":
			hints = append(hints, "creative-writer")
		case "educational":
			hints = append(hints, "educator")
		case "strategic":
			hints = append(hints, "strategist")
		}
	}

	// Check for explicit persona hint
	// 检查显式指定的 persona
	if persona, ok := context["persona"].(string); ok && persona != "" {
		hints = append(hints, persona)
	}

	return hints
}

// parseNumericValue attempts to extract a numeric value from a response string
// parseNumericValue 尝试从 response 字符串中取出数值
// parseNumericValue removed; use util.ParseNumericValue at call sites
// parseNumericValue 已移除；调用方请改用 util.ParseNumericValue

// shouldReflect determines if reflection should be applied based on complexity
// shouldReflect 依据复杂度决定是否走 reflection
func shouldReflect(complexity float64, config *activities.WorkflowConfig) bool {
	// Reflect on complex tasks to improve quality
	// 复杂任务做 reflection 以提升质量
	// Use configurable threshold from config
	// 使用配置里可调的阈值
	threshold := 0.5 // Default fallback 注：默认兜底值
	if config != nil && config.ComplexityMediumThreshold > 0 {
		threshold = config.ComplexityMediumThreshold
	}
	return complexity > threshold
}

// emitTaskUpdate sends a task update event (fire-and-forget with timeout)
// emitTaskUpdate 发送任务更新事件（带超时的 fire-and-forget）
func emitTaskUpdate(ctx workflow.Context, input TaskInput, eventType activities.StreamEventType, agentID, message string) {
	emitTaskUpdatePayload(ctx, input, eventType, agentID, message, nil)
}

// emitTaskUpdatePayload sends a task update event with an optional payload
// emitTaskUpdatePayload 发送带可选 payload 的任务更新事件
func emitTaskUpdatePayload(ctx workflow.Context, input TaskInput, eventType activities.StreamEventType, agentID, message string, payload map[string]any) {
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
	})
	// Use parent workflow ID if this is a child workflow, otherwise use own ID
	// 若是 child workflow 就用 parent 的 workflow ID，否则用自身 ID
	wid := input.ParentWorkflowID
	if wid == "" {
		wid = workflow.GetInfo(ctx).WorkflowExecution.ID
	}
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate",
		activities.EmitTaskUpdateInput{
			WorkflowID: wid,
			EventType:  eventType,
			AgentID:    agentID,
			Message:    message,
			Timestamp:  workflow.Now(ctx),
			Payload:    payload,
		}).Get(ctx, nil)
}

// detectProviderFromModel determines the provider based on the model name
// detectProviderFromModel 依据 model 名判断 provider
// Delegates to shared models.DetectProvider for consistent provider detection
// 委托给公共的 models.DetectProvider，保证 provider 判断一致
func detectProviderFromModel(model string) string {
	return models.DetectProvider(model)
}

// aggregateAgentMetadata extracts model, provider, and token information from agent results
// aggregateAgentMetadata 从 agent 结果中提取 model、provider 与 token 信息
// Returns metadata map with model_used, provider, input_tokens, output_tokens
// 返回带 model_used、provider、input_tokens、output_tokens 的 metadata
// aggregateAgentMetadata removed; use metadata.AggregateAgentMetadata at call sites
// aggregateAgentMetadata 已移除；调用方请改用 metadata.AggregateAgentMetadata

// getPriorityModelForTier resolves a tier string to an actual model name using config.
// getPriorityModelForTier 借助配置把 tier 字符串解析成实际 model 名。
// Returns the priority-1 model for the tier, or empty string if not found.
// 返回该 tier 的 priority-1 model，找不到则返回空字符串。
func getPriorityModelForTier(tier string) string {
	return pricing.GetPriorityOneModel(tier)
}
