package activities

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"
)

// StreamExecuteInput represents input for streaming execution
// StreamExecuteInput 是 streaming 执行的入参。
type StreamExecuteInput struct {
	Query     string         `json:"query"`
	Context   map[string]any `json:"context"`
	SessionID string         `json:"session_id"`
	UserID    string         `json:"user_id"`
	AgentID   string         `json:"agent_id"`
	Mode      string         `json:"mode"`
}

// StreamProgress represents the progress of a streaming operation
// StreamProgress 表示一次 streaming 操作的进度。
type StreamProgress struct {
	Tokens        int    `json:"tokens"`
	PartialResult string `json:"partial_result"`
	Complete      bool   `json:"complete"`
	Error         string `json:"error,omitempty"`
}

// StreamingActivities handles streaming execution of agents
// StreamingActivities 负责 agent 的 streaming 执行。
type StreamingActivities struct {
	// In production, this would connect to the actual LLM service
	// For now, we'll simulate streaming
	// 生产环境应接入真实的 LLM service；目前先模拟 streaming。
}

// NewStreamingActivities creates a new streaming activities handler
// NewStreamingActivities 创建新的 streaming activities handler。
func NewStreamingActivities() *StreamingActivities {
	return &StreamingActivities{}
}

// StreamExecute executes an agent with streaming output
// StreamExecute 以 streaming 方式执行 agent 并输出结果。
func (s *StreamingActivities) StreamExecute(ctx context.Context, input StreamExecuteInput) (AgentExecutionResult, error) {
	logger := activity.GetLogger(ctx)
	info := activity.GetInfo(ctx)

	logger.Info("Starting streaming execution",
		"agent_id", input.AgentID,
		"session_id", input.SessionID,
		"activity_id", info.ActivityID,
	)

	// Execute the agent once to obtain accurate tokens/model (non-streaming call)
	// We then stream the final response via heartbeats for UI consumption.
	// 先执行一次 agent 拿到准确的 tokens/model（这次是非流式调用），
	// 再把最终 response 通过 heartbeat 分块推给 UI。
	res, err := executeAgentCore(ctx, AgentExecutionInput{
		Query:     input.Query,
		AgentID:   input.AgentID,
		Context:   input.Context,
		Mode:      input.Mode,
		SessionID: input.SessionID,
		UserID:    input.UserID,
		History:   nil,
	}, nil)
	if err != nil {
		return AgentExecutionResult{AgentID: input.AgentID, Success: false, Error: err.Error()}, err
	}

	// Stream the final response content in chunks for UI updates
	// 把最终 response 内容分块推送，用于 UI 更新。
	tokens := strings.Fields(res.Response)
	var buffer strings.Builder
	heartbeatInterval := 10 // Send heartbeat every 10 tokens 注：每 10 个 token 发一次 heartbeat
	for i, token := range tokens {
		select {
		case <-ctx.Done():
			return AgentExecutionResult{AgentID: input.AgentID, Success: false, Error: ctx.Err().Error()}, ctx.Err()
		default:
		}
		if i > 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(token)
		if (i+1)%heartbeatInterval == 0 || i == len(tokens)-1 {
			activity.RecordHeartbeat(ctx, StreamProgress{Tokens: i + 1, PartialResult: buffer.String(), Complete: i == len(tokens)-1})
		}
	}

	logger.Info("Streaming execution completed",
		"agent_id", input.AgentID,
		"total_tokens", res.TokensUsed,
		"result_length", len(res.Response),
		"model", res.ModelUsed,
	)

	return res, nil
}

// StreamExecuteWithCallback executes with a callback for each chunk
// StreamExecuteWithCallback 每产出一个 chunk 就调用一次 callback。
func (s *StreamingActivities) StreamExecuteWithCallback(
	ctx context.Context,
	input StreamExecuteInput,
	callback func(StreamProgress) error,
) error {
	logger := activity.GetLogger(ctx)

	logger.Info("Starting streaming with callback",
		"agent_id", input.AgentID,
		"session_id", input.SessionID,
	)

	// Simulate streaming
	// 模拟 streaming。
	fullResponse := s.simulateAgentResponse(input.Query, input.Context)
	tokens := strings.Fields(fullResponse)

	var buffer strings.Builder
	tokenCount := 0

	for i, token := range tokens {
		// Check for cancellation
		// 检查是否已取消。
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Add token to buffer
		// 把 token 追加到 buffer。
		if i > 0 {
			buffer.WriteString(" ")
		}
		buffer.WriteString(token)
		tokenCount++

		// Create progress update
		// 构造进度更新。
		progress := StreamProgress{
			Tokens:        tokenCount,
			PartialResult: buffer.String(),
			Complete:      i == len(tokens)-1,
		}

		// Call the callback
		// 调用 callback。
		if err := callback(progress); err != nil {
			logger.Error("Callback error", "error", err)
			return fmt.Errorf("callback error: %w", err)
		}
	}

	return nil
}

// BatchStreamExecute executes multiple agents with streaming
// BatchStreamExecute 以 streaming 方式批量执行多个 agent。
func (s *StreamingActivities) BatchStreamExecute(
	ctx context.Context,
	inputs []StreamExecuteInput,
) ([]AgentExecutionResult, error) {
	logger := activity.GetLogger(ctx)

	logger.Info("Starting batch streaming execution",
		"num_agents", len(inputs),
	)

	results := make([]AgentExecutionResult, len(inputs))
	totalTokens := 0

	for i, input := range inputs {
		// Check for cancellation
		// 检查是否已取消。
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Execute with streaming
		// 以 streaming 方式执行。
		result, err := s.StreamExecute(ctx, input)
		if err != nil {
			logger.Error("Stream execution failed",
				"index", i,
				"agent_id", input.AgentID,
				"error", err,
			)
			return nil, fmt.Errorf("agent %s failed: %w", input.AgentID, err)
		}

		results[i] = result
		totalTokens += result.TokensUsed

		// Record progress for the batch
		// 记录该批次的进度。
		activity.RecordHeartbeat(ctx, map[string]any{
			"completed": i + 1,
			"total":     len(inputs),
			"tokens":    totalTokens,
		})
	}

	logger.Info("Batch streaming completed",
		"num_agents", len(inputs),
		"total_tokens", totalTokens,
	)

	return results, nil
}

// simulateAgentResponse simulates an agent's response
// In production, this would call the actual LLM service
// 模拟 agent 的 response；生产环境应调用真实的 LLM service。
func (s *StreamingActivities) simulateAgentResponse(query string, context map[string]any) string {
	// Generate a response based on the query
	// 基于 query 生成 response。
	response := fmt.Sprintf("Based on your query '%s', here is my analysis: ", query)

	// Add some context-aware content
	// 追加一些结合 context 的内容。
	if val, exists := context["complexity_score"]; exists {
		response += fmt.Sprintf("The task has a complexity score of %.2f. ", val)
	}

	// Add some generic response text
	// 追加一些通用 response 文本。
	response += "I have analyzed the requirements and identified several key points. "
	response += "First, we need to consider the overall architecture and design patterns. "
	response += "Second, the implementation should follow best practices for maintainability. "
	response += "Third, performance optimization is crucial for scalability. "
	response += "Finally, comprehensive testing ensures reliability and correctness."

	return response
}

// StreamingResult represents a complete streaming result
// StreamingResult 表示一次完整的 streaming 结果。
type StreamingResult struct {
	AgentID    string    `json:"agent_id"`
	Result     string    `json:"result"`
	TokensUsed int       `json:"tokens_used"`
	Duration   int64     `json:"duration_ms"`
	StreamedAt time.Time `json:"streamed_at"`
}

// GetStreamingMetrics returns metrics for streaming operations
// 返回 streaming 操作的指标。
func (s *StreamingActivities) GetStreamingMetrics(ctx context.Context) (map[string]any, error) {
	// In production, this would return actual metrics
	// 生产环境应返回真实指标。
	return map[string]any{
		"total_streams":         42,
		"active_streams":        3,
		"avg_tokens_per_stream": 150,
		"avg_duration_ms":       2500,
		"error_rate":            0.02,
	}, nil
}
