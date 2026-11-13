package vectordb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/tracing"
)

// FindSimilarQueries queries the task_embeddings (or cases) collection for similar historical queries
// FindSimilarQueries 在 task_embeddings（或 cases）集合中查询相似的历史查询
func (c *Client) FindSimilarQueries(ctx context.Context, embedding []float32, limit int) ([]SimilarQuery, error) {
	if limit <= 0 {
		limit = c.cfg.TopK
	}
	// Extract tenant_id from context for filtering if available
	// 可用时从 context 中提取 tenant_id 用于过滤
	var filter map[string]any
	if userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext); ok && userCtx.TenantID.String() != "00000000-0000-0000-0000-000000000000" {
		filter = map[string]any{
			"must": []map[string]any{
				{"key": "tenant_id", "match": map[string]any{"value": userCtx.TenantID.String()}},
			},
		}
	}
	pts, err := c.search(ctx, c.cfg.TaskEmbeddings, embedding, limit, c.cfg.Threshold, filter)
	if err != nil {
		return nil, err
	}
	out := make([]SimilarQuery, 0, len(pts))
	for _, p := range pts {
		sq := SimilarQuery{Confidence: p.Score}
		if q, ok := p.Payload["query"].(string); ok {
			sq.Query = q
		}
		if o, ok := p.Payload["outcome"].(string); ok {
			sq.Outcome = o
		}
		if ts, ok := p.Payload["timestamp"].(float64); ok {
			sq.Timestamp = time.Unix(int64(ts), 0)
		}
		out = append(out, sq)
	}
	return out, nil
}

// scrollPoint is the Qdrant scroll point shape decoded by scrollRecent
// scrollPoint 是 scrollRecent 解码的 Qdrant scroll point 结构
type scrollPoint struct {
	Payload map[string]any `json:"payload"`
	Score   float64        `json:"score,omitempty"`
}

// scrollRecent fetches the points matching must, sorts them by payload timestamp descending and truncates to topK
// scrollRecent 拉取 must 命中的 point，按 payload.timestamp 降序排序并截断到 topK
func (c *Client) scrollRecent(ctx context.Context, sessionID string, must []map[string]any, topK int) ([]ContextItem, error) {
	if topK <= 0 {
		topK = c.cfg.TopK
	}
	// Use Qdrant Scroll API for filter-only retrieval
	// 使用 Qdrant Scroll API 做纯过滤读取
	url := fmt.Sprintf("%s/collections/%s/points/scroll", c.base, c.cfg.TaskEmbeddings)

	// Start tracing span for context retrieval
	// 为 context 检索启动 tracing span
	ctx, span := tracing.StartHTTPSpan(ctx, "POST", url)
	defer span.End()

	// Fetch more than topK to allow for sorting/MMR reranking
	// 多取一些以留出排序/MMR 重排的余地
	poolMultiplier := c.cfg.MMRPoolMultiplier
	if poolMultiplier <= 0 {
		poolMultiplier = 3 // Default multiplier
	}
	limit := min(topK*poolMultiplier,
		// Cap to prevent excessive data transfer
		100)
	body := map[string]any{
		"limit":        limit,
		"with_payload": true,
		"filter":       map[string]any{"must": must},
	}
	buf, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	tracing.InjectTraceparent(ctx, req)
	resp, err := c.httpw.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("qdrant scroll status %d", resp.StatusCode)
	}
	var r struct {
		Result struct {
			Points []scrollPoint `json:"points"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	out := make([]ContextItem, 0, len(r.Result.Points))
	for _, p := range r.Result.Points {
		out = append(out, ContextItem{SessionID: sessionID, Payload: p.Payload, Score: p.Score})
	}

	// Sort by timestamp descending (most recent first)
	// This is done client-side as Qdrant Scroll doesn't support ordering
	// 按时间戳降序排序（最新的在前）
	// 这在客户端完成，因为 Qdrant Scroll 不支持排序
	sort.Slice(out, func(i, j int) bool {
		tsI, okI := out[i].Payload["timestamp"].(float64)
		tsJ, okJ := out[j].Payload["timestamp"].(float64)
		// If both have timestamps, sort descending
		if okI && okJ {
			return tsI > tsJ
		}
		// Items with timestamps come before items without
		// 有时间戳的条目排在没有时间戳的之前
		if okI && !okJ {
			return true
		}
		if !okI && okJ {
			return false
		}
		// Neither has timestamp, maintain original order
		return false
	})

	// Limit to topK after sorting
	if len(out) > topK {
		out = out[:topK]
	}

	return out, nil
}

// GetSessionContext searches for recent points with the same session_id
// GetSessionContext 查找同一 session_id 的近期 point
func (c *Client) GetSessionContext(ctx context.Context, sessionID string, tenantID string, topK int) ([]ContextItem, error) {
	must := []map[string]any{
		{"key": "session_id", "match": map[string]any{"value": sessionID}},
	}
	if tenantID != "" {
		must = append(must, map[string]any{"key": "tenant_id", "match": map[string]any{"value": tenantID}})
	}
	return c.scrollRecent(ctx, sessionID, must, topK)
}

// GetAgentContext searches for recent points with the same session_id and agent_id
// GetAgentContext 查找同一 session_id 与 agent_id 的近期 point
func (c *Client) GetAgentContext(ctx context.Context, sessionID string, agentID string, tenantID string, topK int) ([]ContextItem, error) {
	must := []map[string]any{
		{"key": "session_id", "match": map[string]any{"value": sessionID}},
		{"key": "agent_id", "match": map[string]any{"value": agentID}},
	}
	if tenantID != "" {
		must = append(must, map[string]any{"key": "tenant_id", "match": map[string]any{"value": tenantID}})
	}
	return c.scrollRecent(ctx, sessionID, must, topK)
}
