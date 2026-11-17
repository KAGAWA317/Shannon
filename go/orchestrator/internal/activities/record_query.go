package activities

import (
	"context"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/embeddings"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
)

// RecordQueryInput carries information to store a query and its result
// RecordQueryInput 承载存储 query 及其结果所需的信息。
type RecordQueryInput struct {
	SessionID string         `json:"session_id"`
	UserID    string         `json:"user_id"`
	TenantID  string         `json:"tenant_id"`
	Query     string         `json:"query"`
	Answer    string         `json:"answer"`
	Model     string         `json:"model"`
	Metadata  map[string]any `json:"metadata"`
	RedactPII bool           `json:"redact_pii"`
}

type RecordQueryResult struct {
	Stored        bool   `json:"stored"`
	Error         string `json:"error,omitempty"`
	ChunksCreated int    `json:"chunks_created,omitempty"`
}

var (
	emailRe = regexp.MustCompile(`([a-zA-Z0-9_.%+-]+)@([a-zA-Z0-9.-]+)`)
	phoneRe = regexp.MustCompile(`\b\+?[0-9][0-9\-\s]{6,}[0-9]\b`)
)

func redact(text string) string {
	if text == "" {
		return text
	}
	out := emailRe.ReplaceAllString(text, "***@***")
	out = phoneRe.ReplaceAllString(out, "***PHONE***")
	return out
}

// containsError checks if the response is likely an error message
// containsError 判断 response 是否大概率是错误信息。
func containsError(text string) bool {
	lowerText := strings.ToLower(text)
	errorPhrases := []string{
		"error:",
		"failed to",
		"unable to",
		"could not",
		"exception",
		"invalid",
		"not found",
		"denied",
		"unauthorized",
	}
	for _, phrase := range errorPhrases {
		if strings.Contains(lowerText, phrase) {
			return true
		}
	}
	return false
}

// recordQueryCore contains the shared logic for storing queries in the vector database
// This is used by both RecordQuery and RecordAgentMemory activities to avoid
// activities calling other activities directly
// recordQueryCore 是写入 vector database 的共享逻辑。
// RecordQuery 与 RecordAgentMemory 都走这里，避免 activity 之间直接互调。
func recordQueryCore(ctx context.Context, in RecordQueryInput) (RecordQueryResult, error) {
	logger := activity.GetLogger(ctx)
	svc := embeddings.Get()
	vdb := vectordb.Get()
	if svc == nil || vdb == nil {
		logger.Warn("Vector services unavailable for RecordQuery",
			"session_id", in.SessionID,
			"user_id", in.UserID)
		return RecordQueryResult{Stored: false, Error: "vector services unavailable"}, nil
	}
	q := in.Query
	a := in.Answer
	if in.RedactPII {
		q = redact(q)
		a = redact(a)
	}

	// Skip storing very short or error responses
	// 跳过过短或错误 response。
	if len(a) < 50 || containsError(a) {
		metrics.MemoryWritesSkipped.WithLabelValues("low_value").Inc()
		return RecordQueryResult{Stored: false, Error: "response too short or error message"}, nil
	}

	// Generate embedding for the query to check for duplicates
	// 为 query 生成 embedding，用于查重。
	queryEmbedding, err := svc.GenerateEmbedding(ctx, q, "")
	if err != nil {
		logger.Warn("Failed to generate query embedding",
			"session_id", in.SessionID,
			"error", err)
		return RecordQueryResult{Stored: false, Error: err.Error()}, nil
	}

	// Check for near-duplicates with high similarity threshold (95%)
	// 用高相似度阈值（95%）检查近似重复。
	const duplicateThreshold = 0.95

	// Search for similar existing memories in this session
	// 在本 session 内搜索相似的既有 memory。
	similarItems, err := vdb.GetSessionContextSemanticByEmbedding(ctx, queryEmbedding, in.SessionID, in.TenantID, 1, duplicateThreshold)
	if err == nil && len(similarItems) > 0 {
		// Check if the similar item has the same query (near exact match)
		// 检查相似项是否就是同一条 query（近似精确匹配）。
		for _, item := range similarItems {
			if _, ok := item.Payload["query"].(string); ok {
				// If query is very similar (>95% similarity), skip storing
				// query 高度相似（>95%）时跳过写入。
				if item.Score > duplicateThreshold {
					metrics.MemoryWritesSkipped.WithLabelValues("duplicate").Inc()
					return RecordQueryResult{Stored: false, Error: "near-duplicate memory already exists"}, nil
				}
			}
		}
	}

	// Get embedding service config for chunking settings
	// 取 embedding service 配置，读取 chunking 设置。
	config := svc.GetConfig()

	// Check if chunking is needed
	// 检查是否需要 chunking。
	if config.Chunking.Enabled {
		chunker := embeddings.NewChunker(config.Chunking)

		// Try to chunk the answer (long answers are the main issue)
		// 尝试对 answer 做 chunk（长 answer 是主要问题）。
		chunks := chunker.ChunkText(a)

		if len(chunks) > 0 {
			// Batch embed all chunk texts for efficiency
			// 批量 embed 全部 chunk 文本，提高效率。
			chunkTexts := make([]string, len(chunks))
			for i, chunk := range chunks {
				chunkTexts[i] = chunk.Text
			}

			// Generate embeddings in batch
			// 批量生成 embedding。
			embeddings, err := svc.GenerateBatchEmbeddings(ctx, chunkTexts, "")
			if err != nil {
				logger.Warn("Failed to generate batch embeddings for chunks",
					"session_id", in.SessionID,
					"chunk_count", len(chunkTexts),
					"error", err)
				return RecordQueryResult{Stored: false, Error: err.Error()}, nil
			}

			// Create points for each chunk
			// 为每个 chunk 创建 point。
			points := make([]vectordb.UpsertItem, 0, len(chunks))
			timestamp := time.Now().Unix()

			for i, chunk := range chunks {
				// Build payload with chunk metadata
				// 构造带 chunk metadata 的 payload。
				payload := map[string]any{
					"query": q, // Original query 注：原始 query。
					"chunk_text": chunk.Text, // The actual chunk (no full answer stored) 注：实际 chunk 内容（不存完整 answer）。
					"qa_id": chunk.QAID, // UUID for this Q&A pair 注：该 Q&A 对的 UUID。
					"chunk_index": chunk.Index, // 0-based chunk position 注：chunk 位置，从 0 开始。
					"chunk_count": chunk.TotalCount, // Total chunks for this Q&A 注：该 Q&A 的 chunk 总数。
					"is_chunked": true, // Flag for retrieval 注：供检索使用的标记。
					"session_id": in.SessionID,
					"user_id":    in.UserID,
					"tenant_id":  in.TenantID,
					"model":      in.Model,
					"timestamp":  timestamp,
				}

				// Add any additional metadata
				// 补充额外的 metadata。
				maps.Copy(payload, in.Metadata)

				// Create point with UUID for Qdrant compatibility
				// Store qa_id and chunk_index in payload for idempotency checks
				// 用 UUID 创建 point，兼容 Qdrant。
				// 把 qa_id 与 chunk_index 写进 payload，便于做幂等检查。
				point := vectordb.UpsertItem{
					ID:      uuid.New().String(),
					Vector:  embeddings[i],
					Payload: payload,
				}
				points = append(points, point)
			}

			// Batch upsert all chunks
			// 批量 upsert 全部 chunk。
			if _, err := vdb.Upsert(ctx, vdb.GetConfig().TaskEmbeddings, points); err != nil {
				logger.Warn("Failed to batch upsert chunks to vector store",
					"session_id", in.SessionID,
					"chunk_count", len(points),
					"error", err)
				return RecordQueryResult{Stored: false, Error: err.Error()}, nil
			}

			// Record chunking metrics
			// 记录 chunking metric。
			if len(chunks) > 0 {
				totalTokens := 0
				for _, chunk := range chunks {
					// Estimate tokens (rough approximation: 1 token ≈ 4 chars)
					// 估算 token（粗略近似：1 token ≈ 4 字符）。
					totalTokens += len(chunk.Text) / 4
				}
				avgTokensPerChunk := float64(totalTokens) / float64(len(chunks))
				metrics.RecordChunkingMetrics(in.SessionID, len(chunks), avgTokensPerChunk)
			}

			return RecordQueryResult{Stored: true, ChunksCreated: len(chunks)}, nil
		}
	}

	// Single vector path (no chunking needed or chunking disabled)
	// Embed the answer for consistency with chunked path
	// 单向量路径（无需 chunking，或 chunking 已关闭）。
	// 为与 chunk 路径一致，embed 的是 answer。
	vec, err := svc.GenerateEmbedding(ctx, a, "")
	if err != nil {
		logger.Warn("Failed to generate answer embedding",
			"session_id", in.SessionID,
			"error", err)
		return RecordQueryResult{Stored: false, Error: err.Error()}, nil
	}

	// Build payload
	// 构造 payload。
	payload := map[string]any{
		"query":      q,
		"answer":     a,
		"is_chunked": false, // Flag for retrieval 注：供检索使用的标记。
		"session_id": in.SessionID,
		"user_id":    in.UserID,
		"tenant_id":  in.TenantID,
		"model":      in.Model,
		"timestamp":  time.Now().Unix(),
	}
	maps.Copy(payload, in.Metadata)

	// Upsert point
	// 写入（upsert）向量点。
	if _, err := vdb.UpsertTaskEmbedding(ctx, vec, payload); err != nil {
		logger.Warn("Failed to upsert task embedding to vector store",
			"session_id", in.SessionID,
			"error", err)
		return RecordQueryResult{Stored: false, Error: err.Error()}, nil
	}
	return RecordQueryResult{Stored: true}, nil
}

// RecordQuery generates an embedding and upserts to Qdrant (TaskEmbeddings)
// This is a Temporal activity that wraps the core logic
// RecordQuery 生成 embedding 并 upsert 到 Qdrant（TaskEmbeddings）。
// 这是一个包装核心逻辑的 Temporal activity。
func RecordQuery(ctx context.Context, in RecordQueryInput) (RecordQueryResult, error) {
	res, err := recordQueryCore(ctx, in)
	if err == nil && res.Stored {
		if info := activity.GetInfo(ctx); info.WorkflowExecution.ID != "" {
			wfID := info.WorkflowExecution.ID
			// Friendly message for memory write
			// 供 memory 写入展示的友好文案。
			msg := "Saved to task memory"
			count := res.ChunksCreated
			if count <= 0 { // single vector path 注：单向量路径。
				count = 1
			}
			streaming.Get().Publish(wfID, streaming.Event{
				WorkflowID: wfID,
				Type:       string(StreamEventProgress),
				AgentID:    "memory",
				Message:    msg,
				Payload:    map[string]any{"operation": "upsert", "count": count},
				Timestamp:  time.Now(),
			})
		}
	}
	return res, err
}
