package activities

import (
	"context"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
)

// FetchAgentMemoryInput requests agent-scoped items within a session
// FetchAgentMemoryInput 用于在某个 session 内按 agent 维度取条目。
type FetchAgentMemoryInput struct {
	SessionID string `json:"session_id"`
	TenantID  string `json:"tenant_id"`
	AgentID   string `json:"agent_id"`
	TopK      int    `json:"top_k"`
}

// FetchAgentMemoryResult contains retrieved items for merging
// FetchAgentMemoryResult 保存取回、待合并的条目。
type FetchAgentMemoryResult struct {
	Items []map[string]interface{} `json:"items"`
}

// FetchAgentMemory retrieves agent-specific memory by filtering in Qdrant.
// This now uses a dedicated vectordb method that filters by both session_id and agent_id.
// FetchAgentMemory 从 Qdrant 检索该 agent 专属的 memory；
// 现已改为走专用 vectordb 方法，在 Qdrant 侧同时按 session_id 与 agent_id 过滤。
func FetchAgentMemory(ctx context.Context, in FetchAgentMemoryInput) (FetchAgentMemoryResult, error) {
	if in.SessionID == "" || in.AgentID == "" {
		return FetchAgentMemoryResult{Items: nil}, nil
	}

	// Get vectordb client
	// 取 vectordb client。
	vdb := vectordb.Get()
	if vdb == nil {
		return FetchAgentMemoryResult{Items: nil}, nil
	}

	// Use dedicated agent context method that filters in Qdrant
	// 走专用的 agent context 方法，在 Qdrant 侧完成过滤。
	items, err := vdb.GetAgentContext(ctx, in.SessionID, in.AgentID, in.TenantID, in.TopK)
	if err != nil {
		// Graceful degradation
		// 优雅降级：memory 取不到时返回空结果，不阻断主流程。
		return FetchAgentMemoryResult{Items: nil}, nil
	}

	// Convert to map format
	// 转成 map 形式。
	out := make([]map[string]interface{}, 0, len(items))
	for _, item := range items {
		if item.Payload != nil {
			out = append(out, item.Payload)
		}
	}
	return FetchAgentMemoryResult{Items: out}, nil
}

// RecordAgentMemoryInput stores an agent-scoped interaction into the vector store via RecordQuery
// RecordAgentMemoryInput 描述一次 agent 维度的交互，最终经 RecordQuery 写入向量库。
type RecordAgentMemoryInput struct {
	SessionID string                 `json:"session_id"`
	UserID    string                 `json:"user_id"`
	TenantID  string                 `json:"tenant_id"`
	AgentID   string                 `json:"agent_id"`
	Role      string                 `json:"role"`
	Query     string                 `json:"query"`
	Answer    string                 `json:"answer"`
	Model     string                 `json:"model"`
	RedactPII bool                   `json:"redact_pii"`
	Extra     map[string]interface{} `json:"extra"`
}

// RecordAgentMemory stores agent-specific memory in the vector database
// This is a Temporal activity that uses the shared vector storage logic
// RecordAgentMemory 把 agent 专属 memory 写入向量库；
// 作为 Temporal activity 复用共享的向量存储逻辑。
func RecordAgentMemory(ctx context.Context, in RecordAgentMemoryInput) (RecordQueryResult, error) {
	meta := map[string]interface{}{
		"agent_id": in.AgentID,
		"role":     in.Role,
		"source":   "agent",
	}
	for k, v := range in.Extra {
		meta[k] = v
	}
	// Use the shared helper function instead of calling RecordQuery directly
	// 走共享 helper，而不是直接调用 RecordQuery。
	return recordQueryCore(ctx, RecordQueryInput{
		SessionID: in.SessionID,
		UserID:    in.UserID,
		TenantID:  in.TenantID,
		Query:     in.Query,
		Answer:    in.Answer,
		Model:     in.Model,
		Metadata:  meta,
		RedactPII: in.RedactPII,
	})
}
