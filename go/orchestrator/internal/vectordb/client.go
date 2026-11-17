package vectordb

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	ometrics "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/tracing"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// Client is a minimal Qdrant HTTP client
// Client 是最简的 Qdrant HTTP 客户端
type Client struct {
	cfg   Config
	http  *http.Client
	base  string
	httpw *circuitbreaker.HTTPWrapper
	log   *zap.Logger
}

var global *Client

func Initialize(cfg Config) {
	c := cfg
	if c.Port == 0 {
		c.Port = 6333
	}
	if c.TopK == 0 {
		c.TopK = 5
	}
	if c.Timeout == 0 {
		c.Timeout = 5 * time.Second
	}
	if c.TaskEmbeddings == "" {
		c.TaskEmbeddings = "task_embeddings"
	}
	logger, _ := zap.NewProduction()
	httpClient := &http.Client{
		Timeout:   c.Timeout,
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}
	httpw := circuitbreaker.NewHTTPWrapper(httpClient, "qdrant", "vectordb", logger)
	client := &Client{cfg: c, http: httpClient, base: fmt.Sprintf("http://%s:%d", c.Host, c.Port), httpw: httpw, log: logger}
	global = client
}

func Get() *Client { return global }

// GetConfig returns the current configuration
// GetConfig 返回当前配置
func (c *Client) GetConfig() Config {
	if c == nil {
		return Config{
			TaskEmbeddings: "task_embeddings",
		}
	}
	return c.cfg
}

// qdrant search request/response (simplified)
// qdrant 检索请求/响应（简化）
type qdrantQueryRequest struct {
	Query          []float32      `json:"query"`
	Limit          int            `json:"limit"`
	ScoreThreshold *float64       `json:"score_threshold,omitempty"`
	WithPayload    bool           `json:"with_payload"`
	Filter         map[string]any `json:"filter,omitempty"`
	WithVector     bool           `json:"with_vector,omitempty"`
}

type qdrantPoint struct {
	ID      any            `json:"id"`
	Score   float64        `json:"score"`
	Payload map[string]any `json:"payload"`
	Vector  []float64      `json:"vector,omitempty"`
}

type qdrantSearchResponse struct {
	Result []qdrantPoint `json:"result"`
	Status string        `json:"status"`
}

// qdrantQueryResponse for the /points/query endpoint which has nested structure
// qdrantQueryResponse 对应 /points/query 端点的嵌套结构
type qdrantQueryResponse struct {
	Result struct {
		Points []qdrantPoint `json:"points"`
	} `json:"result"`
	Status string `json:"status"`
}

func (c *Client) search(ctx context.Context, collection string, vec []float32, limit int, threshold float64, filter map[string]any) ([]qdrantPoint, error) {
	if c == nil || !c.cfg.Enabled {
		return nil, fmt.Errorf("vectordb: search called while disabled")
	}
	start := time.Now()

	// Start tracing span for vector search
	// 为向量检索开启 tracing span
	ctx, span := tracing.StartHTTPSpan(ctx, "POST", fmt.Sprintf("%s/collections/%s/points/query", c.base, collection))
	defer span.End()

	// Prefer modern /points/query; on failure, fallback to /points/search for compatibility
	// 优先使用较新的 /points/query；失败时回退到 /points/search 以保持兼容
	var thr *float64
	if threshold > 0 {
		thr = &threshold
	}
	reqBody := qdrantQueryRequest{Query: vec, Limit: limit, ScoreThreshold: thr, WithPayload: true, Filter: filter, WithVector: c.cfg.MMREnabled}
	buf, _ := json.Marshal(reqBody)

	call := func(url string, body []byte) (*http.Response, error) {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")
		tracing.InjectTraceparent(ctx, req)
		return c.httpw.Do(req)
	}

	urlQuery := fmt.Sprintf("%s/collections/%s/points/query", c.base, collection)
	resp, err := call(urlQuery, buf)
	if err != nil {
		ometrics.RecordVectorSearchMetrics(collection, "error", time.Since(start).Seconds())
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// fallback to /points/search
		// 回退到 /points/search
		urlSearch := fmt.Sprintf("%s/collections/%s/points/search", c.base, collection)
		// map to search payload {vector: ...}
		// 映射为 search 请求体 {vector: ...}
		legacy := map[string]any{"vector": vec, "limit": limit, "with_payload": true, "with_vector": c.cfg.MMREnabled}
		if threshold > 0 {
			legacy["score_threshold"] = threshold
		}
		if filter != nil {
			legacy["filter"] = filter
		}
		buf2, _ := json.Marshal(legacy)
		resp2, err2 := call(urlSearch, buf2)
		if err2 != nil {
			ometrics.RecordVectorSearchMetrics(collection, "error", time.Since(start).Seconds())
			return nil, fmt.Errorf("qdrant query/search failed: %w", err2)
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusOK {
			ometrics.RecordVectorSearchMetrics(collection, "error", time.Since(start).Seconds())
			return nil, fmt.Errorf("qdrant status %d", resp2.StatusCode)
		}
		var qr qdrantSearchResponse
		if err := json.NewDecoder(resp2.Body).Decode(&qr); err != nil {
			ometrics.RecordVectorSearchMetrics(collection, "error", time.Since(start).Seconds())
			return nil, err
		}
		ometrics.RecordVectorSearchMetrics(collection, "ok", time.Since(start).Seconds())
		return qr.Result, nil
	}
	// Try to decode as query response first (nested structure)
	// 先尝试按 query response 解析（嵌套结构）
	var qr qdrantQueryResponse
	if err := json.NewDecoder(resp.Body).Decode(&qr); err != nil {
		ometrics.RecordVectorSearchMetrics(collection, "error", time.Since(start).Seconds())
		return nil, err
	}
	ometrics.RecordVectorSearchMetrics(collection, "ok", time.Since(start).Seconds())
	return qr.Result.Points, nil
}

// Upsert inserts or updates one or more points into a collection
// Upsert 向集合中插入或更新一个或多个 point
func (c *Client) Upsert(ctx context.Context, collection string, points []UpsertItem) (*UpsertResponse, error) {
	if c == nil || !c.cfg.Enabled {
		return nil, fmt.Errorf("vectordb: upsert called while disabled")
	}

	// Start tracing span for vector upsert
	// 为向量 upsert 开启 tracing span
	url := fmt.Sprintf("%s/collections/%s/points", c.base, collection)
	ctx, span := tracing.StartHTTPSpan(ctx, "PUT", url)
	defer span.End()

	body := map[string]any{
		"points": points,
	}
	buf, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, bytes.NewReader(buf))
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
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("qdrant upsert status %d", resp.StatusCode)
	}
	var r UpsertResponse
	if err := json.NewDecoder(resp.Body).Decode(&r); err != nil {
		return nil, err
	}
	return &r, nil
}

// UpsertTaskEmbedding helper for inserting a query/answer embedding into TaskEmbeddings collection
// UpsertTaskEmbedding 把查询/答案的 embedding 写入 TaskEmbeddings 集合的辅助函数
func (c *Client) UpsertTaskEmbedding(ctx context.Context, vec []float32, payload map[string]any) (*UpsertResponse, error) {
	p := UpsertItem{
		ID:      uuid.New().String(),
		Vector:  vec,
		Payload: payload,
	}
	return c.Upsert(ctx, c.cfg.TaskEmbeddings, []UpsertItem{p})
}

// UpsertSummaryEmbedding inserts a summary into the configured summaries collection
// Falls back to TaskEmbeddings if Summaries collection is not configured.
// UpsertSummaryEmbedding 把摘要写入配置的 summaries 集合
// 未配置 Summaries 集合时回退到 TaskEmbeddings。
func (c *Client) UpsertSummaryEmbedding(ctx context.Context, vec []float32, payload map[string]any) (*UpsertResponse, error) {
	collection := c.cfg.Summaries
	if collection == "" {
		collection = c.cfg.TaskEmbeddings
	}
	p := UpsertItem{
		ID:      uuid.New().String(),
		Vector:  vec,
		Payload: payload,
	}
	return c.Upsert(ctx, collection, []UpsertItem{p})
}

// Convert to ContextItem format
// 转换为 ContextItem 格式
func contextItems(points []qdrantPoint) []ContextItem {
	items := make([]ContextItem, 0, len(points))
	for _, point := range points {
		// Add point ID to payload for deduplication
		// 把 point ID 加入 payload 以便去重
		payload := point.Payload
		if payload == nil {
			payload = make(map[string]any)
		}
		// Include Qdrant point ID for strong deduplication
		// 带上 Qdrant point ID 以实现强去重
		if point.ID != nil {
			payload["_point_id"] = fmt.Sprintf("%v", point.ID)
		}

		item := ContextItem{
			Score:   point.Score,
			Payload: payload,
		}
		if len(point.Vector) > 0 {
			v := make([]float32, len(point.Vector))
			for i, f := range point.Vector {
				v[i] = float32(f)
			}
			item.Vector = v
		}
		items = append(items, item)
	}
	return items
}

// GetSessionContextSemanticByEmbedding performs semantic search filtered by session ID
// This method accepts a pre-computed embedding to keep the vectordb layer independent of embeddings service
// GetSessionContextSemanticByEmbedding 按 session ID 过滤做语义检索
// 该方法接收预先算好的 embedding，使 vectordb 层不依赖 embeddings 服务
func (c *Client) GetSessionContextSemanticByEmbedding(ctx context.Context, embedding []float32, sessionID string, tenantID string, limit int, threshold float64) ([]ContextItem, error) {
	if c == nil || !c.cfg.Enabled {
		return nil, nil
	}

	// Build Qdrant-compliant filter with "must" clauses
	// 构建符合 Qdrant 规范的 "must" 条件过滤器
	mustClauses := []map[string]any{
		{
			"key": "session_id",
			"match": map[string]any{
				"value": sessionID,
			},
		},
	}

	// Add tenant filter if provided (for future multi-tenancy)
	// 提供了 tenant 时加上租户过滤（为将来多租户预留）
	if tenantID != "" {
		mustClauses = append(mustClauses, map[string]any{
			"key": "tenant_id",
			"match": map[string]any{
				"value": tenantID,
			},
		})
	}

	// Create proper Qdrant filter structure
	// 构建规范的 Qdrant 过滤器结构
	filter := map[string]any{
		"must": mustClauses,
	}

	// Use provided limit or fall back to config
	// 使用传入的 limit，否则回退到配置值
	topK := limit
	if topK <= 0 {
		topK = c.cfg.TopK
	}

	// Search with embedding and filter
	// 带 embedding 和 filter 执行检索
	points, err := c.search(ctx, c.cfg.TaskEmbeddings, embedding, topK, threshold, filter)
	if err != nil {
		return nil, err
	}

	return contextItems(points), nil
}

// SearchSummaries performs semantic search in the summaries collection
// SearchSummaries 在 summaries 集合中做语义检索
func (c *Client) SearchSummaries(ctx context.Context, embedding []float32, sessionID string, tenantID string, limit int, threshold float64) ([]ContextItem, error) {
	if c == nil || !c.cfg.Enabled || c.cfg.Summaries == "" {
		return nil, nil
	}

	// Build Qdrant-compliant filter with "must" clauses
	// 构建符合 Qdrant 规范的 "must" 条件过滤器
	mustClauses := []map[string]any{
		{
			"key": "session_id",
			"match": map[string]any{
				"value": sessionID,
			},
		},
	}

	// Add tenant filter if provided
	// 提供了 tenant 时再加上租户过滤
	if tenantID != "" {
		mustClauses = append(mustClauses, map[string]any{
			"key": "tenant_id",
			"match": map[string]any{
				"value": tenantID,
			},
		})
	}

	// Create proper Qdrant filter structure
	// 构建规范的 Qdrant 过滤器结构
	filter := map[string]any{
		"must": mustClauses,
	}

	// Use provided limit or fall back to config
	// 使用传入的 limit，否则回退到配置值
	topK := limit
	if topK <= 0 {
		topK = 3 // Default to 3 summaries 注：默认取 3 条摘要
	}

	// Search with embedding and filter
	// 带 embedding 和 filter 执行检索
	points, err := c.search(ctx, c.cfg.Summaries, embedding, topK, threshold, filter)
	if err != nil {
		return nil, err
	}

	return contextItems(points), nil
}

// SearchDecompositionPatterns performs semantic search in the decomposition_patterns collection filtered by session
// SearchDecompositionPatterns 在 decomposition_patterns 集合中按 session 做语义检索
func (c *Client) SearchDecompositionPatterns(ctx context.Context, embedding []float32, sessionID string, tenantID string, limit int, threshold float64) ([]ContextItem, error) {
	if c == nil || !c.cfg.Enabled {
		return nil, nil
	}

	// Build Qdrant filter for session (and optional tenant)
	// 构建按 session（及可选 tenant）的 Qdrant 过滤器
	mustClauses := []map[string]any{
		{
			"key": "session_id",
			"match": map[string]any{
				"value": sessionID,
			},
		},
	}
	if tenantID != "" {
		mustClauses = append(mustClauses, map[string]any{
			"key": "tenant_id",
			"match": map[string]any{
				"value": tenantID,
			},
		})
	}
	filter := map[string]any{
		"must": mustClauses,
	}

	topK := limit
	if topK <= 0 {
		topK = c.cfg.TopK
	}

	// Hardcode collection name for now to keep API simple
	// 暂时写死集合名，以保持 API 简单
	const collection = "decomposition_patterns"
	points, err := c.search(ctx, collection, embedding, topK, threshold, filter)
	if err != nil {
		return nil, err
	}

	return contextItems(points), nil
}
