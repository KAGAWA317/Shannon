package activities

import (
	"context"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/embeddings"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
)

// FetchSemanticMemoryInput requests semantic search within a session
// FetchSemanticMemoryInput 用于在指定 session 内发起 semantic search
type FetchSemanticMemoryInput struct {
	Query     string  `json:"query"`      // The query to generate embedding from
	SessionID string  `json:"session_id"` // Session to filter by
	TenantID  string  `json:"tenant_id"`  // Tenant ID (for future multi-tenancy)
	Threshold float64 `json:"threshold"`  // Similarity threshold (0.0-1.0)
	TopK      int     `json:"top_k"`      // Maximum results to return
}

// FetchSemanticMemoryResult contains semantically similar items
// FetchSemanticMemoryResult 包含语义相似的条目
type FetchSemanticMemoryResult struct {
	Items []map[string]any `json:"items"`
}

// FetchSemanticMemory performs semantic search for session memories
// This activity generates the embedding from the query and calls the vectordb layer
// It now supports chunked content aggregation for better handling of long Q&A pairs
// FetchSemanticMemory 对 session memory 执行 semantic search
// 该 activity 先由 query 生成 embedding，再调用 vectordb 层
// 现已支持 chunked 内容聚合，便于处理较长的 Q&A 对
func FetchSemanticMemory(ctx context.Context, in FetchSemanticMemoryInput) (FetchSemanticMemoryResult, error) {
	// Delegate to the chunked version which handles both chunked and non-chunked content
	// 委托给 chunked 版本，它同时处理 chunked 与非 chunked 内容
	return FetchSemanticMemoryChunked(ctx, in)
}

// FetchHierarchicalMemoryInput combines recent and semantic retrieval
// FetchHierarchicalMemoryInput 组合 recent 与 semantic 两种检索
type FetchHierarchicalMemoryInput struct {
	Query        string  `json:"query"`          // For semantic search
	SessionID    string  `json:"session_id"`     // Session identifier
	TenantID     string  `json:"tenant_id"`      // Tenant ID
	RecentTopK   int     `json:"recent_top_k"`   // Recent items from session
	SemanticTopK int     `json:"semantic_top_k"` // Semantic items
	SummaryTopK  int     `json:"summary_top_k"`  // Summary items (default: 3)
	Threshold    float64 `json:"threshold"`      // Semantic similarity threshold
}

// FetchHierarchicalMemoryResult contains deduplicated memory items
// FetchHierarchicalMemoryResult 包含去重后的 memory 条目
type FetchHierarchicalMemoryResult struct {
	Items   []map[string]any `json:"items"`
	Sources map[string]int   `json:"sources"` // Count by source type
}

// FetchHierarchicalMemory combines recent session memory with semantic search
// This provides both temporal relevance (recent) and semantic relevance
// FetchHierarchicalMemory 将 recent session memory 与 semantic search 合并
// 同时提供时间相关性（recent）与语义相关性
func FetchHierarchicalMemory(ctx context.Context, in FetchHierarchicalMemoryInput) (FetchHierarchicalMemoryResult, error) {
	result := FetchHierarchicalMemoryResult{
		Items:   make([]map[string]any, 0),
		Sources: make(map[string]int),
	}

	// Fetch recent items from session
	if in.RecentTopK > 0 {
		recentResult, _ := FetchSessionMemory(ctx, FetchSessionMemoryInput{
			SessionID: in.SessionID,
			TenantID:  in.TenantID,
			TopK:      in.RecentTopK,
		})

		// Record metrics for recent fetch (from Qdrant via FetchSessionMemory)
		// 记录 recent 抓取的 metrics（数据经 FetchSessionMemory 从 Qdrant 取得）
		if len(recentResult.Items) == 0 {
			metrics.MemoryFetches.WithLabelValues("hierarchical-recent", "qdrant", "miss").Inc()
		} else {
			metrics.MemoryFetches.WithLabelValues("hierarchical-recent", "qdrant", "hit").Inc()
		}
		metrics.MemoryItemsRetrieved.WithLabelValues("hierarchical-recent", "qdrant").Observe(float64(len(recentResult.Items)))

		// Add recent items with source marker
		for _, item := range recentResult.Items {
			if item != nil {
				item["_source"] = "recent"
				result.Items = append(result.Items, item)
				result.Sources["recent"]++
			}
		}
	}

	// Build dedup keys from recent items (prefer point ID, fallback to composite key)
	// 由 recent 条目构建去重 key（优先 point ID，退化时用 composite key）
	seen := make(map[string]bool)

	// Fetch semantic items if query provided
	if in.SemanticTopK > 0 && in.Query != "" {
		semanticResult, _ := FetchSemanticMemory(ctx, FetchSemanticMemoryInput{
			Query:     in.Query,
			SessionID: in.SessionID,
			TenantID:  in.TenantID,
			Threshold: in.Threshold,
			TopK:      in.SemanticTopK,
		})
		for _, item := range result.Items {
			key := ""
			// Prefer point ID for deduplication
			// 去重优先使用 point ID
			if pid, ok := item["_point_id"].(string); ok && pid != "" {
				key = pid
			} else if id, ok := item["id"].(string); ok && id != "" {
				key = id
			} else {
				// Fallback to composite key (query + first 100 chars of answer)
				// 退化为 composite key（query + answer 的前 100 个字符）
				if q, ok := item["query"].(string); ok {
					key = q
				}
				if a, ok := item["answer"].(string); ok {
					runes := []rune(a)
					if len(runes) > 100 {
						key += "_" + string(runes[:100])
					} else {
						key += "_" + a
					}
				}
			}
			if key != "" {
				seen[key] = true
			}
		}

		// Add non-duplicate semantic items
		for _, item := range semanticResult.Items {
			if item != nil {
				key := ""
				// Build dedup key using same logic
				if pid, ok := item["_point_id"].(string); ok && pid != "" {
					key = pid
				} else if id, ok := item["id"].(string); ok && id != "" {
					key = id
				} else {
					if q, ok := item["query"].(string); ok {
						key = q
					}
					if a, ok := item["answer"].(string); ok {
						runes := []rune(a)
						if len(runes) > 100 {
							key += "_" + string(runes[:100])
						} else {
							key += "_" + a
						}
					}
				}

				isDuplicate := false
				if key != "" && seen[key] {
					isDuplicate = true
				}

				if !isDuplicate {
					item["_source"] = "semantic"
					result.Items = append(result.Items, item)
					result.Sources["semantic"]++
					if key != "" {
						seen[key] = true
					}
				} else {
					result.Sources["duplicate"]++
				}
			}
		}
	}

	// Fetch summaries if we have a query and vector db is configured
	// Summaries provide compressed historical context
	// 在有 query 且 vector db 已配置时抓取 summaries
	// summaries 提供压缩后的历史 context
	if in.Query != "" && in.SemanticTopK > 0 {
		vdb := vectordb.Get()
		embSvc := embeddings.Get()
		if vdb != nil && embSvc != nil && vdb.GetConfig().Summaries != "" {
			// Generate embedding for query
			vec, err := embSvc.GenerateEmbedding(ctx, in.Query, "")
			if err == nil {
				// Use configured summary limit or default to 3
				summaryLimit := in.SummaryTopK
				if summaryLimit <= 0 {
					summaryLimit = 3
				}
				summaries, _ := vdb.SearchSummaries(ctx, vec, in.SessionID, in.TenantID, summaryLimit, in.Threshold)

				// Add summaries with source marker and dedup
				for _, summary := range summaries {
					if summary.Payload != nil {
						// Build dedup key
						key := ""
						if id, ok := summary.Payload["summary_id"].(string); ok && id != "" {
							key = "summary_" + id
						} else if content, ok := summary.Payload["content"].(string); ok {
							runes := []rune(content)
							if len(runes) > 50 {
								key = "summary_" + string(runes[:50])
							} else {
								key = "summary_" + content
							}
						}

						// Skip if already seen
						if key != "" && seen[key] {
							continue
						}

						summary.Payload["_source"] = "summary"
						result.Items = append(result.Items, summary.Payload)
						result.Sources["summary"]++

						if key != "" {
							seen[key] = true
						}
					}
				}
			}
		}
	}

	// Limit total items to prevent context explosion
	// 限制条目总数，避免 context 爆炸
	maxTotal := 10
	if len(result.Items) > maxTotal {
		result.Items = result.Items[:maxTotal]
	}

	return result, nil
}
