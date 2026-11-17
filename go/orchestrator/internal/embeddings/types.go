package embeddings

import "time"

// Config controls the embedding service behavior
// Config 控制 embedding 服务行为
type Config struct {
	// BaseURL points to the LLM service providing /embeddings
	// BaseURL 指向提供 /embeddings 的 LLM 服务
	BaseURL string
	// DefaultModel is the default embedding model (e.g., text-embedding-3-small)
	// DefaultModel 是默认 embedding 模型（如 text-embedding-3-small）
	DefaultModel string
	// Timeout for outbound HTTP calls
	// 出站 HTTP 调用的超时
	Timeout time.Duration
	// EnableRedis enables Redis-backed cache (optional)
	// EnableRedis 启用 Redis 缓存（可选）
	EnableRedis bool
	// RedisAddr in host:port form when EnableRedis is true
	// EnableRedis 为 true 时，RedisAddr 形如 host:port
	RedisAddr string
	// CacheTTL sets TTL for embedding cache entries
	// CacheTTL 设置 embedding cache 条目的 TTL
	CacheTTL time.Duration
	// MaxLRU controls in-process LRU size
	// MaxLRU 控制进程内 LRU 大小
	MaxLRU int
	// Chunking configuration for long texts
	// 长文本分块配置
	Chunking ChunkingConfig
}
