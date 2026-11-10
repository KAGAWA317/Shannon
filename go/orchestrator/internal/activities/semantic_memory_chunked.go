package activities

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/embeddings"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
	"go.uber.org/zap"
)

// FetchSemanticMemoryChunked performs semantic search with chunk aggregation support
// This is a replacement for FetchSemanticMemory that handles chunked content
// FetchSemanticMemoryChunked 执行语义检索，并支持 chunk 聚合；
// 它替代 FetchSemanticMemory，专门处理被切块的内容。
func FetchSemanticMemoryChunked(ctx context.Context, in FetchSemanticMemoryInput) (FetchSemanticMemoryResult, error) {
	// Get services
	// 取所需服务。
	embSvc := embeddings.Get()
	vdb := vectordb.Get()

	// Graceful degradation if services unavailable
	// 服务不可用时优雅降级。
	if embSvc == nil || vdb == nil || in.SessionID == "" || in.Query == "" {
		return FetchSemanticMemoryResult{Items: nil}, nil
	}

	// Generate embedding from query
	// 由 query 生成 embedding。
	vec, err := embSvc.GenerateEmbedding(ctx, in.Query, "")
	if err != nil {
		// Log error for debugging
		// 记录错误便于排查。
		if logger := zap.L(); logger != nil {
			logger.Warn("Failed to generate embedding for query",
				zap.String("query", in.Query),
				zap.Error(err))
		}
		// Graceful degradation
		// 优雅降级。
		return FetchSemanticMemoryResult{Items: nil}, nil
	}
	// Check if embedding is valid
	// 校验 embedding 是否有效。
	if len(vec) == 0 {
		if logger := zap.L(); logger != nil {
			logger.Warn("Empty embedding generated for query",
				zap.String("query", in.Query))
		}
		return FetchSemanticMemoryResult{Items: nil}, nil
	}

	// Use configured threshold or default
	// Note: 0 means no threshold, negative means use default
	// 使用配置的 threshold 或默认值。
	// 注意：0 表示不设阈值，负数表示使用默认阈值。
	threshold := in.Threshold
	if threshold < 0 {
		threshold = 0.75
	}

	// Use provided TopK or default
	// 使用传入的 TopK，否则用默认值。
	topK := in.TopK
	if topK <= 0 {
		topK = 5
	}

	// Fetch more results to account for chunking using configured multiplier
	// 按配置的倍数多取一些结果，抵消 chunk 切分带来的影响。
	poolMultiplier := 3 // Default multiplier
	if cfg := vdb.GetConfig(); cfg.MMRPoolMultiplier > 0 {
		poolMultiplier = cfg.MMRPoolMultiplier
	}
	searchLimit := topK * poolMultiplier

	// Perform semantic search
	// 执行语义检索。
	items, err := vdb.GetSessionContextSemanticByEmbedding(ctx, vec, in.SessionID, in.TenantID, searchLimit, threshold)
	if err != nil {
		// Record metrics
		// 记录 metrics。
		metrics.MemoryFetches.WithLabelValues("semantic", "qdrant", "miss").Inc()
		metrics.MemoryItemsRetrieved.WithLabelValues("semantic", "qdrant").Observe(0)
		// Log error for debugging
		// 记录错误便于排查。
		if logger := zap.L(); logger != nil {
			logger.Warn("Failed to fetch semantic memory",
				zap.String("session_id", in.SessionID),
				zap.String("tenant_id", in.TenantID),
				zap.Error(err))
		}
		return FetchSemanticMemoryResult{Items: nil}, nil
	}

	// Optional MMR re-ranking (diversity) of candidate pool
	// 可选的 MMR 重排，提升候选池的多样性。
	if cfg := vectordb.Get().GetConfig(); cfg.MMREnabled {
		allHaveVec := true
		for i := range items {
			if len(items[i].Vector) == 0 {
				allHaveVec = false
				break
			}
		}
		if allHaveVec && len(items) > 1 {
			items = mmrReorder(vec, items, cfg.MMRLambda)
		}
	}

	// Group results by qa_id for chunked content
	// 对切块内容按 qa_id 归组。
	type QAGroup struct {
		QAID      string
		Chunks    []vectordb.ContextItem
		BestScore float64
		Query     string
		Payload   map[string]interface{}
	}

	qaGroups := make(map[string]*QAGroup)
	singleItems := make([]map[string]interface{}, 0)

	for _, item := range items {
		payload := item.Payload
		if payload == nil {
			continue
		}

		// Check if this is a chunked item
		// 判断是否为切块条目。
		isChunked := false
		if chunked, ok := payload["is_chunked"].(bool); ok {
			isChunked = chunked
		}

		if isChunked {
			// Extract QA ID
			// 取出 QA ID。
			qaID, _ := payload["qa_id"].(string)
			if qaID == "" {
				// Fallback to single item if no QA ID
				// 没有 QA ID 时按普通单条处理。
				payload["_similarity_score"] = item.Score
				singleItems = append(singleItems, payload)
				continue
			}

			// Group chunks by QA ID
			// 按 QA ID 聚合 chunk。
			if qaGroups[qaID] == nil {
				qaGroups[qaID] = &QAGroup{
					QAID:      qaID,
					Chunks:    []vectordb.ContextItem{},
					BestScore: item.Score,
					Payload:   payload,
				}

				// Extract query from first chunk
				// 从第一个 chunk 取 query。
				if q, ok := payload["query"].(string); ok {
					qaGroups[qaID].Query = q
				}
			}

			// Add chunk to group
			// 把 chunk 加入分组。
			qaGroups[qaID].Chunks = append(qaGroups[qaID].Chunks, item)

			// Track best score
			// 记录组内最高分。
			if item.Score > qaGroups[qaID].BestScore {
				qaGroups[qaID].BestScore = item.Score
			}
		} else {
			// Non-chunked item - add directly
			// 非切块条目，直接加入结果。
			payload["_similarity_score"] = item.Score
			singleItems = append(singleItems, payload)
		}
	}

	// Process chunked groups
	// 处理各个切块分组。
	aggregationStart := time.Now()
	for _, group := range qaGroups {
		// Sort chunks by chunk_index with defensive type checking
		// 按 chunk_index 排序，并对类型做防御性判断。
		sort.Slice(group.Chunks, func(i, j int) bool {
			idxI := 0
			idxJ := 0

			// Safely extract chunk_index from payload with multiple type checks
			// 从 payload 中安全取出 chunk_index，兼容多种数值类型。
			if group.Chunks[i].Payload != nil {
				if val, ok := group.Chunks[i].Payload["chunk_index"]; ok && val != nil {
					switch v := val.(type) {
					case float64:
						idxI = int(v)
					case int:
						idxI = v
					case int64:
						idxI = int(v)
					}
				}
			}

			if group.Chunks[j].Payload != nil {
				if val, ok := group.Chunks[j].Payload["chunk_index"]; ok && val != nil {
					switch v := val.(type) {
					case float64:
						idxJ = int(v)
					case int:
						idxJ = v
					case int64:
						idxJ = int(v)
					}
				}
			}

			return idxI < idxJ
		})

		// Create aggregated item
		// 构造聚合后的条目。
		aggregated := make(map[string]interface{})

		// Copy base fields from first chunk's payload
		// 从第一个 chunk 的 payload 复制基础字段。
		if len(group.Chunks) > 0 && group.Chunks[0].Payload != nil {
			for k, v := range group.Chunks[0].Payload {
				// Skip chunk-specific fields
				// 跳过 chunk 专属字段。
				if k != "chunk_text" && k != "chunk_index" && k != "chunk_count" && k != "qa_id" && k != "is_chunked" {
					aggregated[k] = v
				}
			}
		}

		// Reconstruct full answer from ordered chunk texts
		// Handle overlap by only taking non-overlapping parts after first chunk
		// 按顺序拼接 chunk 文本，还原完整 answer；
		// 通过只取非重叠部分来处理首块之后的重叠。
		var answerBuilder strings.Builder
		for i, chunk := range group.Chunks {
			if chunkText, ok := chunk.Payload["chunk_text"].(string); ok {
				if i == 0 {
					// First chunk - use entire text
					// 首个 chunk：整段使用。
					answerBuilder.WriteString(chunkText)
				} else {
					// Subsequent chunks - skip overlap (approximately 200 tokens = 800 chars)
					// This is a simplified approach; ideally we'd find exact overlap
					// 后续 chunk：跳过重叠部分（约 200 tokens，即 800 字符）。
					// 这是简化做法，理想情况应精确计算重叠区间。
					overlapChars := 800
					if len(chunkText) > overlapChars {
						answerBuilder.WriteString(chunkText[overlapChars:])
					} else {
						// If chunk is smaller than overlap, still append it
						// chunk 比重叠区还短时，仍然整体追加。
						answerBuilder.WriteString(chunkText)
					}
				}
			}
		}

		aggregated["query"] = group.Query
		aggregated["answer"] = answerBuilder.String()
		aggregated["_similarity_score"] = group.BestScore
		aggregated["_was_chunked"] = true
		aggregated["_chunk_count"] = len(group.Chunks)

		singleItems = append(singleItems, aggregated)
	}

	// Record chunk aggregation metrics if we processed any chunked groups
	// 处理过切块分组时，记录 chunk 聚合 metrics。
	if len(qaGroups) > 0 {
		metrics.RecordChunkAggregation(time.Since(aggregationStart).Seconds())
	}

	// Sort by score and limit to topK
	// 按分数排序并截断到 topK。
	sort.Slice(singleItems, func(i, j int) bool {
		scoreI := 0.0
		scoreJ := 0.0
		if s, ok := singleItems[i]["_similarity_score"].(float64); ok {
			scoreI = s
		}
		if s, ok := singleItems[j]["_similarity_score"].(float64); ok {
			scoreJ = s
		}
		return scoreI > scoreJ
	})

	// Limit to requested topK
	// 截断到请求的 topK。
	if len(singleItems) > topK {
		singleItems = singleItems[:topK]
	}

	// Record metrics
	// 记录 metrics。
	if len(singleItems) == 0 {
		metrics.MemoryFetches.WithLabelValues("semantic", "qdrant", "miss").Inc()
	} else {
		metrics.MemoryFetches.WithLabelValues("semantic", "qdrant", "hit").Inc()
	}
	metrics.MemoryItemsRetrieved.WithLabelValues("semantic", "qdrant").Observe(float64(len(singleItems)))

	return FetchSemanticMemoryResult{Items: singleItems}, nil
}

// cosineSim computes cosine similarity between two float32 vectors
// cosineSim 计算两个 float32 向量间的 cosine 相似度。
func cosineSim(a, b []float32) float64 {
	var dot, na, nb float64
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		da := float64(a[i])
		db := float64(b[i])
		dot += da * db
		na += da * da
		nb += db * db
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

// mmrReorder reorders candidates by MMR (greedy) with relevance-diversity trade-off lambda
// mmrReorder 用贪心 MMR 重排候选，lambda 控制相关性与多样性的权衡。
func mmrReorder(query []float32, items []vectordb.ContextItem, lambda float64) []vectordb.ContextItem {
	if lambda < 0 {
		lambda = 0
	}
	if lambda > 1 {
		lambda = 1
	}
	n := len(items)
	if n <= 1 {
		return items
	}
	qd := make([]float64, n)
	for i := 0; i < n; i++ {
		qd[i] = cosineSim(query, items[i].Vector)
	}
	selected := make([]int, 0, n)
	remaining := make([]bool, n)
	for i := 0; i < n; i++ {
		remaining[i] = true
	}
	for len(selected) < n {
		bestIdx := -1
		bestScore := -1e9
		for i := 0; i < n; i++ {
			if !remaining[i] {
				continue
			}
			maxDiv := 0.0
			for _, s := range selected {
				sim := cosineSim(items[i].Vector, items[s].Vector)
				if sim > maxDiv {
					maxDiv = sim
				}
			}
			score := lambda*qd[i] - (1.0-lambda)*maxDiv
			if score > bestScore {
				bestScore = score
				bestIdx = i
			}
		}
		if bestIdx == -1 {
			break
		}
		selected = append(selected, bestIdx)
		remaining[bestIdx] = false
	}
	out := make([]vectordb.ContextItem, 0, n)
	for _, idx := range selected {
		out = append(out, items[idx])
	}
	return out
}
