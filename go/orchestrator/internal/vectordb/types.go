package vectordb

import "time"

// Config controls Qdrant client behavior
type Config struct {
	Enabled bool
	Host    string
	Port    int
	// Collections
	TaskEmbeddings string
	ToolResults    string
	Cases          string
	DocumentChunks string
	Summaries      string
	// Search params
	TopK      int
	Threshold float64
	Timeout   time.Duration
	// Validation
	ExpectedEmbeddingDim int // Expected embedding dimension (e.g., 1536 for OpenAI ada-002)
	// 期望的 embedding 维度（如 OpenAI ada-002 为 1536）
	// MMR (diversity) re-ranking
	MMREnabled        bool
	MMRLambda         float64
	MMRPoolMultiplier int
}

// SimilarQuery represents a similar query with metadata
// SimilarQuery 表示带 metadata 的相似查询
type SimilarQuery struct {
	Query      string    `json:"query"`
	Outcome    string    `json:"outcome"`
	Confidence float64   `json:"confidence"`
	Timestamp  time.Time `json:"timestamp"`
}

// ContextItem represents context retrieved for a session
// ContextItem 表示为 session 检索到的 context
type ContextItem struct {
	SessionID string                 `json:"session_id"`
	Payload   map[string]interface{} `json:"payload"`
	Score     float64                `json:"score"`
	// Optional candidate vector when fetched with vectors (for MMR)
	// 随向量一起取回时的候选向量（用于 MMR）
	Vector []float32 `json:"-"`
}

// UpsertItem represents a single point to insert into Qdrant
// UpsertItem 表示要写入 Qdrant 的单个 point
type UpsertItem struct {
	ID      interface{}            `json:"id,omitempty"`
	Vector  []float32              `json:"vector"`
	Payload map[string]interface{} `json:"payload"`
}

// UpsertResponse captures basic Qdrant upsert response
type UpsertResponse struct {
	Status string  `json:"status"`
	Time   float64 `json:"time"`
}
