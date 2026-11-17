package embeddings

import (
	"strings"

	"github.com/google/uuid"
)

// ChunkingConfig controls text chunking behavior
// ChunkingConfig 控制文本分块行为
type ChunkingConfig struct {
	Enabled       bool   `yaml:"Enabled"`
	MaxTokens     int    `yaml:"MaxTokens"`
	OverlapTokens int    `yaml:"OverlapTokens"`
	TokenizerMode string `yaml:"TokenizerMode"` // "simple" | "tiktoken" 注：取值 "simple" 或 "tiktoken"
}

// DefaultChunkingConfig returns sensible defaults
// DefaultChunkingConfig 返回合理的默认值
func DefaultChunkingConfig() ChunkingConfig {
	return ChunkingConfig{
		Enabled:       true,
		MaxTokens:     1800,     // Safe for most models 注：对多数模型安全
		OverlapTokens: 200,      // ~11% overlap 注：约 11% 重叠
		TokenizerMode: "simple", // Start with simple word-based 注：先用简单的基于词的分词
	}
}

// Chunk represents a text chunk with metadata
// Chunk 表示带 metadata 的文本分块
type Chunk struct {
	QAID       string // UUID for the Q&A pair 注：该 Q&A 对的 UUID
	Text       string // The chunk text 注：分块文本
	Index      int    // 0-based chunk position 注：分块位置，从 0 开始
	TotalCount int    // Total number of chunks 注：分块总数
}

// Chunker handles text chunking with overlap
// Chunker 处理带重叠的文本分块
type Chunker struct {
	maxTokens     int
	overlapTokens int
	tokenizerMode string
}

// NewChunker creates a new chunker with the given configuration
// NewChunker 按给定配置创建分块器
func NewChunker(config ChunkingConfig) *Chunker {
	if config.MaxTokens <= 0 {
		config.MaxTokens = 1800
	}
	if config.OverlapTokens <= 0 {
		config.OverlapTokens = 200
	}
	if config.TokenizerMode == "" {
		config.TokenizerMode = "simple"
	}

	return &Chunker{
		maxTokens:     config.MaxTokens,
		overlapTokens: config.OverlapTokens,
		tokenizerMode: config.TokenizerMode,
	}
}

// ChunkText splits text into overlapping chunks if needed
// Returns nil if text fits within maxTokens (no chunking needed)
// ChunkText 按需把文本切分为重叠分块
// 文本未超过 maxTokens 时返回 nil（无需分块）
func (c *Chunker) ChunkText(text string) []Chunk {
	tokens := c.tokenize(text)

	// No chunking needed if text fits
	// 文本长度在限制内时无需分块
	if len(tokens) <= c.maxTokens {
		return nil
	}

	// Generate a unique ID for this Q&A pair
	// 为该 Q&A 对生成唯一 ID
	qaID := uuid.New().String()
	chunks := []Chunk{}

	// Calculate step size (how many tokens to advance for each chunk)
	// 计算步长（每个分块前进的 token 数）
	step := c.maxTokens - c.overlapTokens
	if step <= 0 {
		step = c.maxTokens / 2 // Fallback to 50% overlap 注：回退到 50% 重叠
	}

	for i := 0; i < len(tokens); i += step {
		end := min(i+c.maxTokens, len(tokens))

		chunkTokens := tokens[i:end]
		chunks = append(chunks, Chunk{
			QAID:  qaID,
			Text:  c.detokenize(chunkTokens),
			Index: len(chunks),
		})

		// Stop if we've reached the end
		// 到达末尾即停止
		if end == len(tokens) {
			break
		}
	}

	// Set total count for all chunks
	// 为所有分块设置总数
	for i := range chunks {
		chunks[i].TotalCount = len(chunks)
	}

	return chunks
}

// CountTokens estimates the token count for a given text
// CountTokens 估算给定文本的 token 数
func (c *Chunker) CountTokens(text string) int {
	return len(c.tokenize(text))
}

// tokenize splits text into tokens based on the tokenizer mode
// tokenize 按 tokenizer 模式把文本切分为 token
func (c *Chunker) tokenize(text string) []string {
	switch c.tokenizerMode {
	case "tiktoken":
		// TODO: Implement proper tiktoken tokenization
		// For now, fall back to simple
		// TODO: 实现正规的 tiktoken 分词
		// 当前回退到简单实现
		return c.simpleTokenize(text)
	default:
		return c.simpleTokenize(text)
	}
}

// detokenize joins tokens back into text
// detokenize 把 token 重新拼接为文本
func (c *Chunker) detokenize(tokens []string) string {
	switch c.tokenizerMode {
	case "tiktoken":
		// TODO: Implement proper tiktoken detokenization
		// For now, fall back to simple
		// TODO: 实现正规的 tiktoken 反分词
		// 当前回退到简单实现
		return strings.Join(tokens, "")
	default:
		// For character-based tokens, join without spaces
		// 基于字符的 token 直接拼接，不加空格
		return strings.Join(tokens, "")
	}
}

// simpleTokenize provides character-based tokenization
// Approximation: ~1 token = 4 characters (standard GPT estimation)
// simpleTokenize 提供基于字符的分词
// 近似：约 1 token = 4 字符（GPT 的通用估算）
func (c *Chunker) simpleTokenize(text string) []string {
	// For simple tokenization, we estimate tokens based on characters
	// Standard approximation: 1 token ≈ 4 characters
	// 简单分词按字符估算 token
	// 通用近似：1 token ≈ 4 字符
	const charsPerToken = 4

	tokens := []string{}
	runes := []rune(text)

	for i := 0; i < len(runes); i += charsPerToken {
		end := min(i+charsPerToken, len(runes))
		if i < len(runes) {
			tokens = append(tokens, string(runes[i:end]))
		}
	}

	return tokens
}

// EstimateTokensForModel estimates tokens from word count; all models share the same rate today.
// EstimateTokensForModel 按词数估算 token 数；当前所有模型使用同一比例。
func EstimateTokensForModel(text string, model string) int {
	return len(strings.Fields(text)) * 13 / 10 // 1.3 tokens per word 注：每词 1.3 个 token
}
