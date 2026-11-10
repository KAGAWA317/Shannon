package embeddings

import "time"

// Config controls the embedding service behavior
type Config struct {
	// BaseURL points to the LLM service providing /embeddings
	// BaseURL 指向提供 /embeddings 的 LLM 服务
	BaseURL string
	// DefaultModel is the default embedding model (e.g., text-embedding-3-small)
	// DefaultModel 是默认 embedding 模型（如 text-embedding-3-small）
	DefaultModel string
	// Timeout for outbound HTTP calls
	Timeout time.Duration
	// EnableRedis enables Redis-backed cache (optional)
	EnableRedis bool
	// RedisAddr in host:port form when EnableRedis is true
	// EnableRedis 为 true 时，RedisAddr 形如 host:port
	RedisAddr string
	// CacheTTL sets TTL for embedding cache entries
	// CacheTTL 设置 embedding cache 条目的 TTL
	CacheTTL time.Duration
	// MaxLRU controls in-process LRU size
	MaxLRU int
	// Chunking configuration for long texts
	Chunking ChunkingConfig
}
