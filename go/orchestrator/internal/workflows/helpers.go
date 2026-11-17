package workflows

import (
	"fmt"
	"strings"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	"go.temporal.io/sdk/log"
	"go.temporal.io/sdk/workflow"
)

// GetContextBool is a convenience wrapper around util.GetContextBool for workflows.
// It extracts a boolean value from context, handling both bool and string "true"/"false".
// GetContextBool 是 util.GetContextBool 在 workflow 场景下的便捷封装。
// 它从 context 中取出布尔值，同时兼容 bool 和字符串 "true"/"false"。
func GetContextBool(ctx map[string]any, key string) bool {
	return util.GetContextBool(ctx, key)
}

// GetContextString extracts a string value from a context map.
// GetContextString 从 context map 中取出字符串值。
func GetContextString(ctx map[string]any, key string) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// convertHistoryForAgent formats session history into a simple string slice for agents
// convertHistoryForAgent 把 session 历史格式化成供 agent 使用的简单字符串切片
func convertHistoryForAgent(messages []Message) []string {
	result := make([]string, len(messages))
	for i, msg := range messages {
		content := strings.ReplaceAll(msg.Content, "\n", "\\n")
		result[i] = fmt.Sprintf("%s: %s", msg.Role, content)
	}
	return result
}

// parseNumericValue attempts to extract a numeric value from a response string
// parseNumericValue wrappers removed; use util.ParseNumericValue at call sites
// parseNumericValue 尝试从响应字符串中提取数值
// parseNumericValue 的包装已移除；调用方请直接使用 util.ParseNumericValue

// convertHistoryMapForCompression converts Message history to map format for compression
// convertHistoryMapForCompression 把 Message 历史转成 map 格式以便压缩
func convertHistoryMapForCompression(messages []Message) []map[string]string {
	result := make([]map[string]string, len(messages))
	for i, msg := range messages {
		result[i] = map[string]string{
			"role":    msg.Role,
			"content": msg.Content,
		}
	}
	return result
}

// getPrimersRecents extracts primers/recents counts from context with defaults.
// getPrimersRecents 从 context 中取出 primers/recents 数量，缺失时用默认值。
func getPrimersRecents(ctx map[string]any, defPrimers, defRecents int) (int, int) {
	p, r := defPrimers, defRecents
	if ctx == nil {
		return p, r
	}
	if v, ok := ctx["primers_count"].(int); ok {
		if v >= 0 {
			p = v
		}
	}
	if v, ok := ctx["primers_count"].(float64); ok {
		if v >= 0 {
			p = int(v)
		}
	}
	if v, ok := ctx["recents_count"].(int); ok {
		if v >= 0 {
			r = v
		}
	}
	if v, ok := ctx["recents_count"].(float64); ok {
		if v >= 0 {
			r = int(v)
		}
	}
	return p, r
}

// getCompressionRatios reads compression trigger/target ratios from context
// falling back to provided defaults. Values are clamped to sane bounds.
// getCompressionRatios 从 context 中读取压缩的触发/目标比例，
// 缺失时回退到传入的默认值。取值会被收敛到合理范围内。
func getCompressionRatios(ctx map[string]any, defTrigger, defTarget float64) (float64, float64) {
	tr, tg := defTrigger, defTarget
	clamp := func(v float64, lo, hi float64) float64 {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	if ctx != nil {
		if v, ok := ctx["compression_trigger_ratio"].(float64); ok {
			tr = v
		}
		if v, ok := ctx["compression_trigger_ratio"].(int); ok {
			tr = float64(v)
		}
		if v, ok := ctx["compression_target_ratio"].(float64); ok {
			tg = v
		}
		if v, ok := ctx["compression_target_ratio"].(int); ok {
			tg = float64(v)
		}
	}
	// Sane bounds to avoid extremes
	// 合理范围，避免取值极端
	tr = clamp(tr, 0.1, 0.95)
	tg = clamp(tg, 0.1, 0.9)
	return tr, tg
}

// shapeHistory returns a sliced history keeping the first nPrimers and last nRecents messages
// when there are enough messages to have a middle section. Otherwise it returns the original.
// 保留首 nPrimers 条与末 nRecents 条，中间段落仅在消息足够多时才丢弃；否则原样返回。
func shapeHistory(messages []Message, nPrimers, nRecents int) []Message {
	m := len(messages)
	if m == 0 {
		return messages
	}
	if nPrimers < 0 {
		nPrimers = 0
	}
	if nRecents < 0 {
		nRecents = 0
	}
	if nPrimers+nRecents >= m {
		return messages
	}
	primersEnd := min(nPrimers, m)
	recentsStart := max(m-nRecents, primersEnd)
	shaped := make([]Message, 0, primersEnd+(m-recentsStart))
	shaped = append(shaped, messages[:primersEnd]...)
	shaped = append(shaped, messages[recentsStart:]...)
	return shaped
}

// fallbackToBasicMemory loads basic hierarchical memory for supervisor workflow
// fallbackToBasicMemory 为 supervisor workflow 加载基础分层记忆
func fallbackToBasicMemory(ctx workflow.Context, input *TaskInput, logger log.Logger) {
	var memoryResult activities.FetchHierarchicalMemoryResult
	memoryInput := activities.FetchHierarchicalMemoryInput{
		Query:        input.Query,
		SessionID:    input.SessionID,
		TenantID:     input.TenantID,
		RecentTopK:   5,
		SemanticTopK: 3,
		SummaryTopK:  2,
		Threshold:    0.7,
	}

	if err := workflow.ExecuteActivity(ctx, "FetchHierarchicalMemory", memoryInput).Get(ctx, &memoryResult); err == nil {
		if len(memoryResult.Items) > 0 {
			if input.Context == nil {
				input.Context = make(map[string]any)
			}
			input.Context["agent_memory"] = memoryResult.Items
			logger.Info("Injected basic memory into supervisor context",
				"session_id", input.SessionID,
				"memory_items", len(memoryResult.Items))
		}
	}
}

// extractSubtaskDescriptions extracts descriptions from subtasks
// extractSubtaskDescriptions 提取各 subtask 的描述
func extractSubtaskDescriptions(subtasks []activities.Subtask) []string {
	descriptions := make([]string, len(subtasks))
	for i, st := range subtasks {
		descriptions[i] = st.Description
	}
	return descriptions
}

// detectProviderFromModel determines the provider based on the model name
// Delegates to shared models.DetectProvider for consistent provider detection
// detectProviderFromModel 根据模型名称判断 provider
// 委托给共享的 models.DetectProvider，保证 provider 判断一致
func detectProviderFromModel(model string) string {
	return models.DetectProvider(model)
}

// aggregateAgentMetadata extracts model, provider, and token information from agent results
// Returns metadata map with model_used, provider, input_tokens, output_tokens
// aggregateAgentMetadata removed; use metadata.AggregateAgentMetadata at call sites
// aggregateAgentMetadata 从 agent 结果中提取 model、provider 与 token 信息
// 返回包含 model_used、provider、input_tokens、output_tokens 的 metadata map
// aggregateAgentMetadata 已移除；调用方请使用 metadata.AggregateAgentMetadata

// AddTaskContextToMetadata enriches TaskResult metadata with task submission context
// for API exposure (force_research, research_strategy, etc.). Call before returning
// from workflows to ensure context is persisted and available via GET /tasks/{id}.
// 在返回前调用，把 task 提交 context（force_research、research_strategy 等）写入 metadata，
// 确保其被持久化并可通过 GET /tasks/{id} 读取。
func AddTaskContextToMetadata(result TaskResult, taskContext map[string]any) TaskResult {
	if result.Metadata == nil {
		result.Metadata = make(map[string]any)
	}
	if len(taskContext) > 0 {
		result.Metadata["task_context"] = taskContext
	}
	return result
}
