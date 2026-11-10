package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
	"go.temporal.io/sdk/activity"
)

// FetchSessionMemoryInput requests session-scoped context items
// FetchSessionMemoryInput 是按 session 维度拉取 context 条目的入参。
type FetchSessionMemoryInput struct {
	SessionID string `json:"session_id"`
	TenantID  string `json:"tenant_id"`
	TopK      int    `json:"top_k"`
}

// FetchSessionMemoryResult contains retrieved items for merging
type FetchSessionMemoryResult struct {
	Items []map[string]interface{} `json:"items"`
}

// FetchSessionMemory fetches recent items for a session from Qdrant
// FetchSessionMemory 从 Qdrant 拉取该 session 最近的条目。
func FetchSessionMemory(ctx context.Context, in FetchSessionMemoryInput) (FetchSessionMemoryResult, error) {
	vdb := vectordb.Get()
	if vdb == nil || in.SessionID == "" {
		return FetchSessionMemoryResult{Items: nil}, nil
	}
	items, err := vdb.GetSessionContext(ctx, in.SessionID, in.TenantID, in.TopK)
	if err != nil {
		// Record metrics for failed fetch (miss)
		// 拉取失败（miss）时记录 metrics。
		metrics.MemoryFetches.WithLabelValues("session", "qdrant", "miss").Inc()
		metrics.MemoryItemsRetrieved.WithLabelValues("session", "qdrant").Observe(0)
		return FetchSessionMemoryResult{Items: nil}, nil
	}

	// Record metrics based on whether we found items
	// 按是否命中条目分别记录 metrics。
	if len(items) == 0 {
		metrics.MemoryFetches.WithLabelValues("session", "qdrant", "miss").Inc()
	} else {
		metrics.MemoryFetches.WithLabelValues("session", "qdrant", "hit").Inc()
	}
	metrics.MemoryItemsRetrieved.WithLabelValues("session", "qdrant").Observe(float64(len(items)))

	// Emit a friendly memory retrieval event
	// 发一条面向用户的 memory 检索 event。
	if info := activity.GetInfo(ctx); info.WorkflowExecution.ID != "" {
		wfID := info.WorkflowExecution.ID
		msg := fmt.Sprintf("Looking up past notes (%d found)", len(items))
		streaming.Get().Publish(wfID, streaming.Event{
			WorkflowID: wfID,
			Type:       string(StreamEventProgress),
			AgentID:    "memory",
			Message:    msg,
			Payload:    map[string]interface{}{"operation": "fetch", "hits": len(items)},
			Timestamp:  time.Now(),
		})
	}

	out := make([]map[string]interface{}, 0, len(items))
	for _, it := range items {
		out = append(out, it.Payload)
	}
	return FetchSessionMemoryResult{Items: out}, nil
}
