package activities

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
)

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
