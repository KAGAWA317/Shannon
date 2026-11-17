package activities

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
)

// EstimateTokensFromHistory estimates token count from message history
// Rough approximation: ~4 characters per token (GPT-style tokenization)
// EstimateTokensFromHistory 依据 message history 估算 token 数
// 粗略近似：约 4 个字符对应一个 token（GPT 风格分词）
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
	// 保守估算：约 4 个字符对应一个 token
	// 该估算略微偏高，确保在触及上限之前先做压缩
	return (totalChars / 4) + (len(messages) * 5) // Add 5 tokens per message for formatting 注：每条 message 额外加 5 个 token 作为格式开销
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
	// 保守估算：约 4 个字符对应一个 token
	// 该估算略微偏高，确保在触及上限之前先做压缩
	return (totalChars / 4) + (len(messages) * 5) // Add 5 tokens per message for formatting 注：每条 message 额外加 5 个 token 作为格式开销
}

// GetModelWindowSize returns the token window for a given model
// GetModelWindowSize 返回指定 model 的 token 窗口大小。
func GetModelWindowSize(modelTier string) int {
	// Common model windows (conservative estimates)
	// 常见 model 的 context 窗口（保守取值）。
	switch modelTier {
	case "small":
		return 8000 // 8k models 注：8k model
	case "medium":
		return 32000 // 32k models 注：32k model
	case "large":
		return 128000 // 128k models 注：128k model
	case "xlarge":
		return 200000 // Claude 200k 注：Claude 200k
	default:
		return 8000 // Conservative default 注：保守默认值
	}
}
