package activities

// Centralized truncation limits for streaming messages.
// Keep these values consistent across agent and synthesis paths.
// streaming 消息的统一截断上限；agent 与 synthesis 两条路径要保持一致。
const (
	// Max length for synthesized final content in SSE.
	// SSE 中合成后最终内容的长度上限。
	MaxSynthesisOutputChars = 10000

	// Max length for agent LLM final outputs in SSE.
	// SSE 中 agent LLM 最终输出的长度上限。
	MaxLLMOutputChars = 10000

	// Max length for prompts displayed/logged in SSE (sanitized).
	// SSE 中展示/记录的 prompt 长度上限（已脱敏）。
	MaxPromptChars = 5000

	// Max length for "thinking" status snippets.
	// "thinking" 状态片段的长度上限。
	MaxThinkingChars = 200
)
