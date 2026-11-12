package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/embeddings"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
)

// PersistDebateConsensusInput stores the outcome of a debate
// PersistDebateConsensusInput 用于存储一次 debate 的结果
type PersistDebateConsensusInput struct {
	SessionID        string         `json:"session_id"`
	Topic            string         `json:"topic"`            // Original query/topic
	WinningPosition  string         `json:"winning_position"` // The consensus or winning argument
	ConsensusReached bool           `json:"consensus_reached"`
	Confidence       float64        `json:"confidence"`
	Positions        []string       `json:"positions"` // All debate positions
	Metadata         map[string]any `json:"metadata"`
}

// PersistDebateConsensusResult indicates storage success
// PersistDebateConsensusResult 表示存储是否成功
type PersistDebateConsensusResult struct {
	Stored bool   `json:"stored"`
	Error  string `json:"error,omitempty"`
}

// PersistDebateConsensus stores debate outcomes in the Summaries collection
// Uses type="consensus" to distinguish from other summaries (Phase 3)
// PersistDebateConsensus 将 debate 结果存入 Summaries collection
// 用 type="consensus" 与其他 summaries 区分（Phase 3）
func PersistDebateConsensus(ctx context.Context, in PersistDebateConsensusInput) (PersistDebateConsensusResult, error) {
	svc := embeddings.Get()
	vdb := vectordb.Get()

	if svc == nil || vdb == nil {
		return PersistDebateConsensusResult{Stored: false, Error: "services unavailable"}, nil
	}

	// Create a searchable text from the consensus
	searchableText := fmt.Sprintf("Topic: %s\nConsensus: %s", in.Topic, in.WinningPosition)

	// Generate embedding for semantic retrieval
	vec, err := svc.GenerateEmbedding(ctx, searchableText, "")
	if err != nil {
		return PersistDebateConsensusResult{Stored: false, Error: err.Error()}, nil
	}

	// Build payload with consensus metadata
	payload := map[string]any{
		"type":              "consensus", // IMPORTANT: Identifies this as consensus memory
		// IMPORTANT: 标记此条为 consensus memory
		"session_id":        in.SessionID,
		"topic":             in.Topic,
		"winning_position":  in.WinningPosition,
		"consensus_reached": in.ConsensusReached,
		"confidence":        in.Confidence,
		"timestamp":         time.Now().Unix(),
		"searchable_text":   searchableText,
	}

	// Add positions if provided
	if len(in.Positions) > 0 {
		payload["positions"] = in.Positions
		payload["num_positions"] = len(in.Positions)
	}

	// Merge additional metadata
	for k, v := range in.Metadata {
		if k != "type" && k != "session_id" { // Protect critical keys
			// 保护关键 key
			payload[k] = v
		}
	}

	// Store in Summaries collection (reusing existing infrastructure)
	// This avoids creating a new collection and maintains compatibility
	// 存入 Summaries collection（复用现有基础设施）
	// 这样无需新建 collection，并保持兼容
	if _, err := vdb.UpsertSummaryEmbedding(ctx, vec, payload); err != nil {
		return PersistDebateConsensusResult{Stored: false, Error: err.Error()}, nil
	}

	return PersistDebateConsensusResult{Stored: true}, nil
}

// FetchConsensusMemoryInput retrieves past consensus decisions
// FetchConsensusMemoryInput 用于检索历史 consensus 决策
type FetchConsensusMemoryInput struct {
	Query     string  `json:"query"`      // Current topic/query
	SessionID string  `json:"session_id"` // Optional: filter by session
	Threshold float64 `json:"threshold"`  // Similarity threshold
	TopK      int     `json:"top_k"`      // Max results
}

// FetchConsensusMemoryResult contains relevant past consensus decisions
// FetchConsensusMemoryResult 包含相关的历史 consensus 决策
type FetchConsensusMemoryResult struct {
	Items []ConsensusItem `json:"items"`
}

// ConsensusItem represents a past consensus decision
// ConsensusItem 表示一条历史 consensus 决策
type ConsensusItem struct {
	Topic            string  `json:"topic"`
	WinningPosition  string  `json:"winning_position"`
	Confidence       float64 `json:"confidence"`
	ConsensusReached bool    `json:"consensus_reached"`
	Similarity       float64 `json:"similarity"`
	Timestamp        int64   `json:"timestamp"`
}

// FetchConsensusMemory retrieves similar past consensus decisions
// This helps inform future debates with historical context (Phase 3)
// FetchConsensusMemory 检索相似的历史 consensus 决策
// 借助历史 context 为后续 debate 提供参考（Phase 3）
func FetchConsensusMemory(ctx context.Context, in FetchConsensusMemoryInput) (FetchConsensusMemoryResult, error) {
	svc := embeddings.Get()
	vdb := vectordb.Get()

	if svc == nil || vdb == nil || in.Query == "" {
		return FetchConsensusMemoryResult{Items: nil}, nil
	}

	// Generate embedding for the query
	_, err := svc.GenerateEmbedding(ctx, in.Query, "")
	if err != nil {
		return FetchConsensusMemoryResult{Items: nil}, nil
	}

	// Search with type filter for consensus items
	filter := map[string]any{
		"type": "consensus",
	}

	// Add session filter if provided
	if in.SessionID != "" {
		filter["session_id"] = in.SessionID
	}

	// Use threshold or default
	threshold := in.Threshold
	if threshold <= 0 {
		threshold = 0.7 // Lower threshold for consensus (more lenient)
		// consensus 使用更低的 threshold（更宽松）
	}

	// Determine TopK
	topK := in.TopK
	if topK <= 0 {
		topK = 3 // Default to top 3 consensus items
	}

	// Search in the summaries collection (where consensus is stored)
	// This would need a small enhancement to the search method to support
	// searching in summaries collection, or we store in task_embeddings
	// For now, we'll document this as a future enhancement
	// 在 summaries collection 中搜索（consensus 就存放在这里）
	// 这需要对 search 方法做小幅增强，以支持在 summaries collection 中搜索，
	// 或者改为存入 task_embeddings
	// 目前先记录为后续增强

	// TODO: Add search method for summaries collection or use task_embeddings
	// For MVP, we can store consensus in task_embeddings with type="consensus"

	return FetchConsensusMemoryResult{Items: nil}, nil
}
