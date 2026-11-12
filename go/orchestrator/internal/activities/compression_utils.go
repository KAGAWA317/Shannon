package activities

import (
	"context"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"go.uber.org/zap"
)

// CompressionState tracks when compression was last performed
type CompressionState struct {
	LastCompressedAt  time.Time `json:"last_compressed_at"`
	LastMessageCount  int       `json:"last_message_count"`
	TotalCompressions int       `json:"total_compressions"`
}

// EstimateTokensFromHistory estimates token count from message history
// Rough approximation: ~4 characters per token (GPT-style tokenization)
func EstimateTokensFromHistory(messages []session.Message) int {
	if len(messages) == 0 {
		return 0
	}

	totalChars := 0
	for _, msg := range messages {
		totalChars += len(msg.Content)
	}

	// Conservative estimate: ~4 chars per token
	// This slightly overestimates to ensure we compress before hitting limits
	return (totalChars / 4) + (len(messages) * 5) // Add 5 tokens per message for formatting
}

// EstimateTokens estimates token count from string messages
// This is used by workflows which convert history to string format
// EstimateTokens 按字符串 message 估算 token 数，供把 history 转成字符串的 workflow 使用。
func EstimateTokens(messages []string) int {
	if len(messages) == 0 {
		return 0
	}

	totalChars := 0
	for _, msg := range messages {
		totalChars += len(msg)
	}

	// Conservative estimate: ~4 chars per token
	// This slightly overestimates to ensure we compress before hitting limits
	return (totalChars / 4) + (len(messages) * 5) // Add 5 tokens per message for formatting
}

// GetModelWindowSize returns the token window for a given model
// GetModelWindowSize 返回指定 model 的 token 窗口大小。
func GetModelWindowSize(modelTier string) int {
	// Common model windows (conservative estimates)
	// 常见 model 的 context 窗口（保守取值）。
	switch modelTier {
	case "small":
		return 8000 // 8k models
	case "medium":
		return 32000 // 32k models
	case "large":
		return 128000 // 128k models
	case "xlarge":
		return 200000 // Claude 200k
	default:
		return 8000 // Conservative default
	}
}

// ShouldCompressContext determines if compression is needed
// Uses token-based thresholds and rate limiting via session state
// ShouldCompressContext 判断是否需要压缩 context：基于 token 阈值，并结合 session state 做频率限制。
func ShouldCompressContext(
	ctx context.Context,
	sessionID string,
	messages []session.Message,
	modelTier string,
	sessionMgr *session.Manager,
) (bool, CompressionState, error) {
	logger := zap.L()

	// Get compression state from session
	state := CompressionState{}
	if sessionMgr != nil && sessionID != "" {
		sessData, err := sessionMgr.GetSession(ctx, sessionID)
		if err == nil && sessData != nil {
			// Retrieve compression state from session metadata
			if sessData.Metadata != nil {
				if compState, ok := sessData.Metadata["compression_state"].(map[string]any); ok {
					if lastTime, ok := compState["last_compressed_at"].(int64); ok {
						state.LastCompressedAt = time.Unix(lastTime, 0)
					}
					if lastCount, ok := compState["last_message_count"].(int); ok {
						state.LastMessageCount = lastCount
					}
					if totalCount, ok := compState["total_compressions"].(int); ok {
						state.TotalCompressions = totalCount
					}
				}
			}
		}
	}

	// Estimate current tokens
	// 估算当前 token 用量。
	currentTokens := EstimateTokensFromHistory(messages)
	modelWindow := GetModelWindowSize(modelTier)
	threshold := int(float64(modelWindow) * 0.75) // Compress at 75% of window

	logger.Debug("Checking compression need",
		zap.Int("current_tokens", currentTokens),
		zap.Int("threshold", threshold),
		zap.Int("message_count", len(messages)),
		zap.Time("last_compressed", state.LastCompressedAt),
	)

	// Check token threshold
	// 检查 token 阈值。
	if currentTokens < threshold {
		return false, state, nil
	}

	// Rate limiting: minimum requirements
	// 频率限制：最低触发条件。
	const minMessagesSinceLastCompression = 20
	const minTimeSinceLastCompression = 30 * time.Minute

	// Check message count rate limit
	// 检查 message 条数这一频率限制。
	messagesSinceLastCompression := len(messages) - state.LastMessageCount
	if messagesSinceLastCompression < minMessagesSinceLastCompression {
		logger.Debug("Skipping compression: insufficient new messages",
			zap.Int("new_messages", messagesSinceLastCompression),
			zap.Int("required", minMessagesSinceLastCompression),
		)
		return false, state, nil
	}

	// Check time rate limit
	// 检查时间维度的频率限制。
	if !state.LastCompressedAt.IsZero() {
		timeSinceLastCompression := time.Since(state.LastCompressedAt)
		if timeSinceLastCompression < minTimeSinceLastCompression {
			logger.Debug("Skipping compression: too soon since last compression",
				zap.Duration("time_since", timeSinceLastCompression),
				zap.Duration("required", minTimeSinceLastCompression),
			)
			return false, state, nil
		}
	}

	// All conditions met - should compress
	logger.Info("Compression recommended",
		zap.Int("tokens", currentTokens),
		zap.Int("messages", len(messages)),
		zap.Int("compressions_done", state.TotalCompressions),
	)

	return true, state, nil
}

// UpdateCompressionState updates the compression state in session
// UpdateCompressionState 更新 session 中的压缩状态。
func UpdateCompressionState(
	ctx context.Context,
	sessionID string,
	messageCount int,
	sessionMgr *session.Manager,
) error {
	if sessionMgr == nil || sessionID == "" {
		return nil
	}

	state := CompressionState{
		LastCompressedAt:  time.Now(),
		LastMessageCount:  messageCount,
		TotalCompressions: 0, // Will be incremented from existing state
	}

	// Get existing session to preserve compression count
	// 先取现有 session 以保留压缩次数。
	sessData, err := sessionMgr.GetSession(ctx, sessionID)
	if err == nil && sessData != nil && sessData.Metadata != nil {
		if compState, ok := sessData.Metadata["compression_state"].(map[string]any); ok {
			if totalCount, ok := compState["total_compressions"].(int); ok {
				state.TotalCompressions = totalCount
			}
		}
	}
	state.TotalCompressions++

	// Store updated state in session metadata
	// 把更新后的状态写入 session metadata。
	compStateMap := map[string]any{
		"last_compressed_at": state.LastCompressedAt.Unix(),
		"last_message_count": state.LastMessageCount,
		"total_compressions": state.TotalCompressions,
	}

	// Update session metadata (would need session manager update method)
	// For now, just log the update with the state map
	// In real implementation, this would call sessionMgr.UpdateMetadata or similar
	// 更新 session metadata 需要 session manager 提供更新方法；目前仅记录状态，真实实现会调用 sessionMgr.UpdateMetadata 之类的方法。

	zap.L().Info("Updated compression state",
		zap.String("session_id", sessionID),
		zap.Int("message_count", messageCount),
		zap.Int("total_compressions", state.TotalCompressions),
		zap.Any("compression_state", compStateMap),
	)

	return nil
}
