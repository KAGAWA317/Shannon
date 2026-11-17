package handlers

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// SessionContextKeyTitle is the key for session title in the context JSONB field
	// SessionContextKeyTitle 是 context JSONB 字段中 session 标题的 key
	SessionContextKeyTitle = "title"
)

// SessionHandler handles session-related HTTP requests
// SessionHandler 处理 session 相关 HTTP 请求
type SessionHandler struct {
	db               *sqlx.DB
	redis            *redis.Client
	logger           *zap.Logger
	firecrackerURL   string
	workspaceBaseDir string
	httpClient       *http.Client
}

// NewSessionHandler creates a new session handler
// NewSessionHandler 创建一个新的 session handler
func NewSessionHandler(
	db *sqlx.DB,
	redis *redis.Client,
	logger *zap.Logger,
) *SessionHandler {
	firecrackerURL := os.Getenv("FIRECRACKER_EXECUTOR_URL")
	if firecrackerURL == "" {
		firecrackerURL = "http://firecracker-executor:9001"
	}

	workspaceBaseDir := os.Getenv("SHANNON_SESSION_WORKSPACES_DIR")
	if workspaceBaseDir == "" {
		workspaceBaseDir = os.Getenv("FIRECRACKER_EFS_MOUNT")
	}
	if workspaceBaseDir == "" {
		workspaceBaseDir = "/tmp/shannon-sessions"
	}

	return &SessionHandler{
		db:               db,
		redis:            redis,
		logger:           logger,
		firecrackerURL:   strings.TrimSuffix(firecrackerURL, "/"),
		workspaceBaseDir: workspaceBaseDir,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// SessionResponse represents a session metadata response
// SessionResponse 表示 session metadata 响应
type SessionResponse struct {
	SessionID         string         `json:"session_id"`
	UserID            string         `json:"user_id"`
	Context           map[string]any `json:"context,omitempty"`
	TokenBudget       int            `json:"token_budget,omitempty"`
	TokensUsed        int            `json:"tokens_used"`
	TaskCount         int            `json:"task_count"`
	CreatedAt         time.Time      `json:"created_at"`
	UpdatedAt         *time.Time     `json:"updated_at,omitempty"`
	ExpiresAt         *time.Time     `json:"expires_at,omitempty"`
	IsResearchSession bool           `json:"is_research_session"`
	ResearchStrategy  string         `json:"research_strategy,omitempty"`
}

// SessionHistoryResponse represents session task history
// SessionHistoryResponse 表示 session 的 task 历史
type SessionHistoryResponse struct {
	SessionID string        `json:"session_id"`
	Tasks     []TaskHistory `json:"tasks"`
	Total     int           `json:"total"`
}

// ListSessionsResponse represents the list sessions response
// ListSessionsResponse 表示 session 列表响应
type ListSessionsResponse struct {
	Sessions   []SessionSummary `json:"sessions"`
	TotalCount int              `json:"total_count"`
}

// SessionSummary represents a single session in listing
// SessionSummary 表示列表中的单个 session
type SessionSummary struct {
	SessionID   string         `json:"session_id"`
	UserID      string         `json:"user_id"`
	Title       string         `json:"title,omitempty"`
	TaskCount   int            `json:"task_count"`
	TokensUsed  int            `json:"tokens_used"`
	TokenBudget int            `json:"token_budget,omitempty"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   *time.Time     `json:"updated_at,omitempty"`
	ExpiresAt   *time.Time     `json:"expires_at,omitempty"`
	Context     map[string]any `json:"context,omitempty"`

	// Activity tracking
	// activity 追踪
	LastActivityAt *time.Time `json:"last_activity_at,omitempty"`
	IsActive       bool       `json:"is_active"`

	// Task success metrics
	// task 成功指标
	SuccessfulTasks int     `json:"successful_tasks"`
	FailedTasks     int     `json:"failed_tasks"`
	SuccessRate     float64 `json:"success_rate,omitempty"`

	// Cost tracking
	// cost 追踪
	TotalCostUSD       float64 `json:"total_cost_usd"`
	AverageCostPerTask float64 `json:"average_cost_per_task,omitempty"`

	// Budget utilization (computed)
	// 预算使用率（计算得出）
	BudgetUtilization float64 `json:"budget_utilization,omitempty"`
	BudgetRemaining   int     `json:"budget_remaining,omitempty"`
	IsNearBudgetLimit bool    `json:"is_near_budget_limit"`

	// Latest task preview
	// 最近一次 task 预览
	LatestTaskQuery  string `json:"latest_task_query,omitempty"`
	LatestTaskStatus string `json:"latest_task_status,omitempty"`

	// Research detection (computed from first task)
	// 研究 task 判定（根据首个 task 计算）。
	IsResearchSession bool   `json:"is_research_session"`
	FirstTaskMode     string `json:"first_task_mode,omitempty"`
}

// TaskHistory represents a task in session history
// TaskHistory 表示 session 历史中的一个 task
type TaskHistory struct {
	TaskID       string         `json:"task_id"`
	WorkflowID   string         `json:"workflow_id"`
	Query        string         `json:"query"`
	Status       string         `json:"status"`
	Mode         string         `json:"mode,omitempty"`
	Result       string         `json:"result,omitempty"`
	ErrorMessage string         `json:"error_message,omitempty"`
	TotalTokens  int            `json:"total_tokens"`
	TotalCostUSD float64        `json:"total_cost_usd"`
	DurationMs   int            `json:"duration_ms,omitempty"`
	AgentsUsed   int            `json:"agents_used"`
	ToolsInvoked int            `json:"tools_invoked"`
	StartedAt    time.Time      `json:"started_at"`
	CompletedAt  *time.Time     `json:"completed_at,omitempty"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

// GetSession handles GET /api/v1/sessions/{sessionId}
// GetSession 处理 GET /api/v1/sessions/{sessionId}
func (h *SessionHandler) GetSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth 中间件取出用户 context
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract session ID from path
	// 从路径中取出 session ID
	sessionID := r.PathValue("sessionId")
	if sessionID == "" {
		h.sendError(w, "Session ID is required", http.StatusBadRequest)
		return
	}

	// Get session metadata from database
	// 从数据库读取 session metadata
	var session struct {
		ID          string        `db:"id"`
		UserID      string        `db:"user_id"`
		Context     []byte        `db:"context"`
		TokenBudget sql.NullInt32 `db:"token_budget"`
		TokensUsed  sql.NullInt32 `db:"tokens_used"`
		CreatedAt   time.Time     `db:"created_at"`
		UpdatedAt   sql.NullTime  `db:"updated_at"`
		ExpiresAt   sql.NullTime  `db:"expires_at"`
	}

	err := h.db.GetContext(ctx, &session, `
            SELECT id, user_id, context, token_budget, tokens_used, created_at, updated_at, expires_at
            FROM sessions
            WHERE (id::text = $1 OR context->>'external_id' = $1)
              AND user_id = $2
              AND tenant_id = $3
              AND deleted_at IS NULL
        `, sessionID, userCtx.UserID.String(), userCtx.TenantID.String())

	if err == sql.ErrNoRows {
		h.sendError(w, "Session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("Failed to query session", zap.Error(err), zap.String("session_id", sessionID))
		h.sendError(w, "Failed to retrieve session", http.StatusInternalServerError)
		return
	}

	// Note: Access control enforced in query (user_id and tenant_id WHERE clauses)
	// 注意：访问控制已写入查询条件（user_id 与 tenant_id 的 WHERE 子句）。

	// Parse context JSON first to get external_id if present
	// 先解析 context JSON，以便拿到可能存在的 external_id
	var contextData map[string]any
	if len(session.Context) > 0 {
		json.Unmarshal(session.Context, &contextData)
	}

	// Get task count for this session from task_executions (check both UUID and external_id)
	// 从 task_executions 统计该 session 的 task 数（同时按 UUID 和 external_id 查）。
	var taskCount int
	if extID, ok := contextData["external_id"].(string); ok && extID != "" {
		// Session has external_id, check both session.ID and external_id
		// session 有 external_id 时，同时按 session.ID 和 external_id 查询。
		err = h.db.GetContext(ctx, &taskCount, `
			SELECT COUNT(*) FROM task_executions
			WHERE (session_id = $1 OR session_id = $2) AND user_id = $3 AND tenant_id = $4
		`, session.ID, extID, userCtx.UserID.String(), userCtx.TenantID)
	} else {
		// No external_id, just check by UUID
		// 无 external_id，仅按 UUID 查询
		err = h.db.GetContext(ctx, &taskCount, `
			SELECT COUNT(*) FROM task_executions
			WHERE session_id = $1 AND user_id = $2 AND tenant_id = $3
		`, session.ID, userCtx.UserID.String(), userCtx.TenantID)
	}
	if err != nil {
		h.logger.Warn("Failed to get task count", zap.Error(err))
		taskCount = 0
	}

	// Prefer DB aggregate from task_executions when present: it sums
	// cache_aware_total_tokens which reflects the true per-session cost
	// (including prompt cache). Many workflows still feed pre-cache
	// session.TokensUsed into Redis, so the DB is the only path that
	// guarantees cache cost shows up here. Redis is used only as a
	// fallback while a session has no completed task rows yet.
	// 优先使用 task_executions 的聚合结果：它累加 cache_aware_total_tokens，能反映包含 prompt cache 的真实 session 成本。
	// 许多 workflow 仍把未含 cache 的 session.TokensUsed 写入 Redis，因此只有 DB 这条路径能保证 cache 成本被计入；
	// Redis 仅在 session 尚无已完成 task 记录时作为兜底。
	tokensUsed := 0
	{
		var aggregatedTokens int
		var dbErr error
		if extID, ok := contextData["external_id"].(string); ok && extID != "" {
			dbErr = h.db.GetContext(ctx, &aggregatedTokens, `
				SELECT COALESCE(SUM(cache_aware_total_tokens), 0)::int FROM task_executions
				WHERE (session_id = $1 OR session_id = $2) AND user_id = $3 AND tenant_id = $4
			`, session.ID, extID, userCtx.UserID.String(), userCtx.TenantID)
		} else {
			dbErr = h.db.GetContext(ctx, &aggregatedTokens, `
				SELECT COALESCE(SUM(cache_aware_total_tokens), 0)::int FROM task_executions
				WHERE session_id = $1 AND user_id = $2 AND tenant_id = $3
			`, session.ID, userCtx.UserID.String(), userCtx.TenantID)
		}
		if dbErr == nil && aggregatedTokens > 0 {
			tokensUsed = aggregatedTokens
		}
	}

	// Fallback to Redis real-time counter when DB has no rows yet.
	// Note: Don't use session.TokensUsed - it's not reliably updated.
	// DB 还没有记录时，退回 Redis 的实时计数器。注意：不要用 session.TokensUsed——它并不保证被更新。
	if tokensUsed == 0 && h.redis != nil {
		// Try both possible Redis keys: the input sessionID and any external_id
		// 依次尝试两种 Redis key：传入的 sessionID 以及可能的 external_id。
		keysToTry := []string{fmt.Sprintf("session:%s", sessionID)}
		if extID, ok := contextData["external_id"].(string); ok && extID != "" {
			keysToTry = append(keysToTry, fmt.Sprintf("session:%s", extID))
		}

		for _, redisKey := range keysToTry {
			if data, err := h.redis.Get(ctx, redisKey).Result(); err == nil {
				var sessionData map[string]any
				if err := json.Unmarshal([]byte(data), &sessionData); err == nil {
					if tokens, ok := sessionData["total_tokens_used"].(float64); ok {
						tokensUsed = int(tokens)
						break
					}
				}
			}
		}
	}

	// Check if this is a research session
	// Priority: session.context (set at session creation) > first task's metadata
	// 判断是否为研究 task：优先级为 session.context（创建时写入）> 首个 task 的 metadata。
	isResearchSession := false
	researchStrategy := ""
	foundResearchInContext := false
	foundStrategyInContext := false

	// Check session context first (more reliable)
	// 先看 session context（更可靠）。
	if fr, ok := contextData["force_research"].(bool); ok && fr {
		isResearchSession = true
		foundResearchInContext = true
	}
	if rs, ok := contextData["research_strategy"].(string); ok && rs != "" {
		researchStrategy = rs
		foundStrategyInContext = true
	}

	// Fall back to first task's metadata for any fields not found in session context
	// session context 中缺失的字段，回退到首个 task 的 metadata。
	if !foundResearchInContext || !foundStrategyInContext {
		var firstTaskMetadata sql.NullString
		if extID, ok := contextData["external_id"].(string); ok && extID != "" {
			err = h.db.GetContext(ctx, &firstTaskMetadata, `
				SELECT metadata FROM task_executions
				WHERE (session_id = $1 OR session_id = $2) AND user_id = $3 AND tenant_id = $4
				ORDER BY created_at ASC LIMIT 1
			`, session.ID, extID, userCtx.UserID.String(), userCtx.TenantID)
		} else {
			err = h.db.GetContext(ctx, &firstTaskMetadata, `
				SELECT metadata FROM task_executions
				WHERE session_id = $1 AND user_id = $2 AND tenant_id = $3
				ORDER BY created_at ASC LIMIT 1
			`, session.ID, userCtx.UserID.String(), userCtx.TenantID)
		}
		if err == nil && firstTaskMetadata.Valid && firstTaskMetadata.String != "" {
			var metadata map[string]any
			if err := json.Unmarshal([]byte(firstTaskMetadata.String), &metadata); err == nil {
				if taskContext, ok := metadata["task_context"].(map[string]any); ok {
					// Only set values that weren't found in session context
					// 只设置 session context 中未找到的字段
					if !foundResearchInContext {
						if fr, ok := taskContext["force_research"].(bool); ok && fr {
							isResearchSession = true
						}
					}
					if !foundStrategyInContext {
						if rs, ok := taskContext["research_strategy"].(string); ok {
							researchStrategy = rs
						}
					}
				}
			}
		}
	}

	// Build response
	// 构建响应
	resp := SessionResponse{
		SessionID:         session.ID,
		UserID:            session.UserID,
		Context:           contextData,
		TokensUsed:        tokensUsed,
		TaskCount:         taskCount,
		CreatedAt:         session.CreatedAt,
		IsResearchSession: isResearchSession,
		ResearchStrategy:  researchStrategy,
	}

	if session.TokenBudget.Valid {
		resp.TokenBudget = int(session.TokenBudget.Int32)
	}
	if session.UpdatedAt.Valid {
		resp.UpdatedAt = &session.UpdatedAt.Time
	}
	if session.ExpiresAt.Valid {
		resp.ExpiresAt = &session.ExpiresAt.Time
	}

	h.logger.Debug("Session retrieved",
		zap.String("session_id", sessionID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.Int("task_count", taskCount),
	)

	// Send response
	// 发送响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// GetSessionHistory handles GET /api/v1/sessions/{sessionId}/history
// GetSessionHistory 处理 GET /api/v1/sessions/{sessionId}/history
func (h *SessionHandler) GetSessionHistory(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth 中间件取出用户 context
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract session ID from path
	// 从路径中取出 session ID
	sessionID := r.PathValue("sessionId")
	if sessionID == "" {
		h.sendError(w, "Session ID is required", http.StatusBadRequest)
		return
	}

	// Verify session exists and user has access (also get id and external_id for task query)
	// 校验 session 存在且用户有访问权（同时取出 id 和 external_id 供 task 查询使用）。
	var sessionData struct {
		ID         string  `db:"id"`
		UserID     string  `db:"user_id"`
		ExternalID *string `db:"external_id"`
	}
	err := h.db.GetContext(ctx, &sessionData, `
        SELECT id, user_id, context->>'external_id' as external_id
        FROM sessions
        WHERE (id::text = $1 OR context->>'external_id' = $1)
          AND user_id = $2
          AND tenant_id = $3
          AND deleted_at IS NULL
    `, sessionID, userCtx.UserID.String(), userCtx.TenantID)

	if err == sql.ErrNoRows {
		h.sendError(w, "Session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("Failed to query session", zap.Error(err))
		h.sendError(w, "Failed to retrieve session", http.StatusInternalServerError)
		return
	}

	// Query all tasks for this session (check both UUID and external_id with user_id and tenant_id filter)
	// 查询该 session 的所有 task（同时按 UUID 和 external_id 匹配，并带上 user_id 与 tenant_id 过滤）。
	var rows *sqlx.Rows
	if sessionData.ExternalID != nil && *sessionData.ExternalID != "" {
		// Session has external_id, check both session.ID and external_id
		// session 有 external_id 时，同时按 session.ID 和 external_id 查询
		rows, err = h.db.QueryxContext(ctx, `
			SELECT
				id,
				workflow_id,
				query,
				status,
				COALESCE(mode, '') as mode,
				COALESCE(result, '') as result,
				COALESCE(error_message, '') as error_message,
				COALESCE(total_tokens, 0) as total_tokens,
				COALESCE(total_cost_usd, 0) as total_cost_usd,
				COALESCE(duration_ms, 0) as duration_ms,
				COALESCE(agents_used, 0) as agents_used,
				COALESCE(tools_invoked, 0) as tools_invoked,
				started_at,
				completed_at,
				metadata
			FROM task_executions
			WHERE (session_id = $1 OR session_id = $2) AND user_id = $3 AND tenant_id = $4
			ORDER BY started_at ASC
		`, sessionData.ID, *sessionData.ExternalID, sessionData.UserID, userCtx.TenantID)
	} else {
		// No external_id, just check by UUID
		// 无 external_id，仅按 UUID 查询
		rows, err = h.db.QueryxContext(ctx, `
			SELECT
				id,
				workflow_id,
				query,
				status,
				COALESCE(mode, '') as mode,
				COALESCE(result, '') as result,
				COALESCE(error_message, '') as error_message,
				COALESCE(total_tokens, 0) as total_tokens,
				COALESCE(total_cost_usd, 0) as total_cost_usd,
				COALESCE(duration_ms, 0) as duration_ms,
				COALESCE(agents_used, 0) as agents_used,
				COALESCE(tools_invoked, 0) as tools_invoked,
				started_at,
				completed_at,
				metadata
			FROM task_executions
			WHERE session_id = $1 AND user_id = $2 AND tenant_id = $3
			ORDER BY started_at ASC
		`, sessionData.ID, sessionData.UserID, userCtx.TenantID)
	}

	if err != nil {
		h.logger.Error("Failed to query task history", zap.Error(err))
		h.sendError(w, "Failed to retrieve session history", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Parse results
	// 解析结果
	tasks := make([]TaskHistory, 0)
	for rows.Next() {
		var task struct {
			ID           string       `db:"id"`
			WorkflowID   string       `db:"workflow_id"`
			Query        string       `db:"query"`
			Status       string       `db:"status"`
			Mode         string       `db:"mode"`
			Result       string       `db:"result"`
			ErrorMessage string       `db:"error_message"`
			TotalTokens  int          `db:"total_tokens"`
			TotalCostUSD float64      `db:"total_cost_usd"`
			DurationMs   int          `db:"duration_ms"`
			AgentsUsed   int          `db:"agents_used"`
			ToolsInvoked int          `db:"tools_invoked"`
			StartedAt    time.Time    `db:"started_at"`
			CompletedAt  sql.NullTime `db:"completed_at"`
			Metadata     []byte       `db:"metadata"`
		}

		if err := rows.StructScan(&task); err != nil {
			h.logger.Error("Failed to scan task", zap.Error(err))
			continue
		}

		// Parse metadata JSON
		// 解析 metadata JSON
		var metadata map[string]any
		if len(task.Metadata) > 0 {
			json.Unmarshal(task.Metadata, &metadata)
		}

		history := TaskHistory{
			TaskID:       task.ID,
			WorkflowID:   task.WorkflowID,
			Query:        task.Query,
			Status:       task.Status,
			Mode:         task.Mode,
			Result:       task.Result,
			ErrorMessage: task.ErrorMessage,
			TotalTokens:  task.TotalTokens,
			TotalCostUSD: task.TotalCostUSD,
			DurationMs:   task.DurationMs,
			AgentsUsed:   task.AgentsUsed,
			ToolsInvoked: task.ToolsInvoked,
			StartedAt:    task.StartedAt,
			Metadata:     metadata,
		}

		if task.CompletedAt.Valid {
			history.CompletedAt = &task.CompletedAt.Time
		}

		tasks = append(tasks, history)
	}

	if err := rows.Err(); err != nil {
		h.logger.Error("Failed to iterate task rows", zap.Error(err))
		h.sendError(w, "Failed to retrieve session history", http.StatusInternalServerError)
		return
	}

	h.logger.Debug("Session history retrieved",
		zap.String("session_id", sessionID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.Int("task_count", len(tasks)),
	)

	// Build response
	// 构建响应
	resp := SessionHistoryResponse{
		SessionID: sessionID,
		Tasks:     tasks,
		Total:     len(tasks),
	}

	// Send response
	// 发送响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// GetSessionEvents handles GET /api/v1/sessions/{sessionId}/events
// Returns grouped turns (one per task) including full events per turn.
// GetSessionEvents 处理 GET /api/v1/sessions/{sessionId}/events，返回按 task 分组的多轮对话，每轮包含完整 event。
func (h *SessionHandler) GetSessionEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth 中间件取出用户 context
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract session ID from path
	// 从路径中取出 session ID
	sessionID := r.PathValue("sessionId")
	if sessionID == "" {
		h.sendError(w, "Session ID is required", http.StatusBadRequest)
		return
	}

	// Verify session exists and user has access (also get id and external_id)
	// 校验 session 存在且用户有访问权（同时取出 id 和 external_id）
	var sessionData struct {
		ID         string  `db:"id"`
		UserID     string  `db:"user_id"`
		ExternalID *string `db:"external_id"`
	}
	err := h.db.GetContext(ctx, &sessionData, `
        SELECT id, user_id, context->>'external_id' as external_id
        FROM sessions
        WHERE (id::text = $1 OR context->>'external_id' = $1)
          AND user_id = $2
          AND tenant_id = $3
          AND deleted_at IS NULL
    `, sessionID, userCtx.UserID.String(), userCtx.TenantID)
	if err == sql.ErrNoRows {
		h.sendError(w, "Session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("Failed to query session", zap.Error(err))
		h.sendError(w, "Failed to retrieve session", http.StatusInternalServerError)
		return
	}

	// Pagination params (turns)
	// 分页参数（以 turn 为单位）
	q := r.URL.Query()
	limit := 10
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}
	includePayload := q.Get("include_payload") == "true"

	// Shapes
	// 响应结构定义
	type Event struct {
		WorkflowID string    `json:"workflow_id"`
		Type       string    `json:"type"`
		AgentID    string    `json:"agent_id,omitempty"`
		Message    string    `json:"message,omitempty"`
		Timestamp  time.Time `json:"timestamp"`
		Seq        uint64    `json:"seq"`
		StreamID   string    `json:"stream_id,omitempty"`
		Payload    string    `json:"payload,omitempty"`
	}
	type Turn struct {
		Turn        int       `json:"turn"`
		TaskID      string    `json:"task_id"`
		UserQuery   string    `json:"user_query"`
		FinalOutput string    `json:"final_output"`
		Timestamp   time.Time `json:"timestamp"`
		Events      []Event   `json:"events"`
		Metadata    struct {
			TokensUsed      int              `json:"tokens_used"`
			ExecutionTimeMs int              `json:"execution_time_ms"`
			AgentsInvolved  []string         `json:"agents_involved"`
			Attachments     []map[string]any `json:"attachments,omitempty"`
		} `json:"metadata"`
	}

	// Count total turns (tasks)
	// 统计 turn（task）总数
	var total int
	if sessionData.ExternalID != nil && *sessionData.ExternalID != "" {
		err = h.db.GetContext(ctx, &total, `
            SELECT COUNT(*) FROM task_executions
            WHERE (session_id = $1 OR session_id = $2) AND user_id = $3 AND tenant_id = $4
        `, sessionData.ID, *sessionData.ExternalID, sessionData.UserID, userCtx.TenantID)
	} else {
		err = h.db.GetContext(ctx, &total, `
            SELECT COUNT(*) FROM task_executions
            WHERE session_id = $1 AND user_id = $2 AND tenant_id = $3
        `, sessionData.ID, sessionData.UserID, userCtx.TenantID)
	}
	if err != nil {
		h.logger.Error("Failed to count session turns", zap.Error(err))
		h.sendError(w, "Failed to retrieve session events", http.StatusInternalServerError)
		return
	}

	// Fetch turns ordered by started_at
	// 按 started_at 顺序取出 turn
	type turnRow struct {
		TaskID      string         `db:"id"`
		WorkflowID  string         `db:"workflow_id"`
		Query       string         `db:"query"`
		Result      sql.NullString `db:"result"`
		StartedAt   time.Time      `db:"started_at"`
		CompletedAt sql.NullTime   `db:"completed_at"`
		TotalTokens sql.NullInt32  `db:"total_tokens"`
		DurationMs  sql.NullInt32  `db:"duration_ms"`
		Metadata    sql.NullString `db:"metadata"`
	}

	var turnRows []turnRow
	if sessionData.ExternalID != nil && *sessionData.ExternalID != "" {
		err = h.db.SelectContext(ctx, &turnRows, `
            SELECT id, workflow_id, query, result, started_at, completed_at,
                   COALESCE(total_tokens,0) as total_tokens,
                   COALESCE(duration_ms,0) as duration_ms,
                   COALESCE(metadata::text,'{}') as metadata
            FROM task_executions
            WHERE (session_id = $1 OR session_id = $2) AND user_id = $3 AND tenant_id = $4
            ORDER BY started_at ASC
            LIMIT $5 OFFSET $6
        `, sessionData.ID, *sessionData.ExternalID, sessionData.UserID, userCtx.TenantID, limit, offset)
	} else {
		err = h.db.SelectContext(ctx, &turnRows, `
            SELECT id, workflow_id, query, result, started_at, completed_at,
                   COALESCE(total_tokens,0) as total_tokens,
                   COALESCE(duration_ms,0) as duration_ms,
                   COALESCE(metadata::text,'{}') as metadata
            FROM task_executions
            WHERE session_id = $1 AND user_id = $2 AND tenant_id = $3
            ORDER BY started_at ASC
            LIMIT $4 OFFSET $5
        `, sessionData.ID, sessionData.UserID, userCtx.TenantID, limit, offset)
	}
	if err != nil {
		h.logger.Error("Failed to query session turns", zap.Error(err))
		h.sendError(w, "Failed to retrieve session events", http.StatusInternalServerError)
		return
	}

	// Build IN clause for workflows
	// 为 workflow 构建 IN 条件
	wfIDs := make([]string, 0, len(turnRows))
	for _, t := range turnRows {
		wfIDs = append(wfIDs, t.WorkflowID)
	}

	// Map of workflow_id -> []Event
	// workflow_id -> []Event 的映射
	eventsByWF := make(map[string][]Event, len(wfIDs))
	if len(wfIDs) > 0 {
		// Safe IN expansion using sqlx.In and rebind for Postgres
		// Conditionally include payload based on query parameter
		// 用 sqlx.In 安全展开 IN 条件并对 Postgres 重新绑定；payload 是否返回由查询参数决定。
		baseQuery := `
            SELECT workflow_id, type, COALESCE(agent_id,''), COALESCE(message,''), timestamp, COALESCE(seq,0), COALESCE(stream_id,'')`
		if includePayload {
			baseQuery += `, COALESCE(payload::text,'')`
		}
		baseQuery += `
            FROM event_logs
            WHERE workflow_id IN (?) AND type <> 'LLM_PARTIAL'
            ORDER BY timestamp ASC`
		inQuery, args, inErr := sqlx.In(baseQuery, wfIDs)
		if inErr != nil {
			h.logger.Error("Failed to build IN query for workflows", zap.Error(inErr))
			h.sendError(w, "Failed to retrieve session events", http.StatusInternalServerError)
			return
		}
		// Force PostgreSQL-style bindvars for predictable queries
		// 强制使用 PostgreSQL 风格的占位符，保证查询行为可预期。
		inQuery = sqlx.Rebind(sqlx.DOLLAR, inQuery)

		rows, qerr := h.db.QueryxContext(ctx, inQuery, args...)
		if qerr != nil {
			h.logger.Error("Failed to query events for workflows", zap.Error(qerr))
			h.sendError(w, "Failed to retrieve session events", http.StatusInternalServerError)
			return
		}
		defer rows.Close()
		// Safety: cap total events to avoid excessive memory usage per request
		// 安全上限：单次 request 的 event 总数封顶，避免占用过多内存。
		const maxEventsPerRequest = 5000
		totalEvents := 0
		for rows.Next() {
			if totalEvents >= maxEventsPerRequest {
				h.logger.Error("Session exceeds event limit",
					zap.Int("max", maxEventsPerRequest),
					zap.Strings("workflow_ids", wfIDs))
				h.sendError(w, "Session has too many events. Please use pagination with smaller turn limits.", http.StatusRequestEntityTooLarge)
				return
			}
			var e Event
			if includePayload {
				if err := rows.Scan(&e.WorkflowID, &e.Type, &e.AgentID, &e.Message, &e.Timestamp, &e.Seq, &e.StreamID, &e.Payload); err != nil {
					h.logger.Error("Failed to scan event row with payload", zap.Error(err), zap.Strings("workflow_ids", wfIDs))
					h.sendError(w, "Failed to read events", http.StatusInternalServerError)
					return
				}
			} else {
				if err := rows.Scan(&e.WorkflowID, &e.Type, &e.AgentID, &e.Message, &e.Timestamp, &e.Seq, &e.StreamID); err != nil {
					h.logger.Error("Failed to scan event row", zap.Error(err), zap.Strings("workflow_ids", wfIDs))
					h.sendError(w, "Failed to read events", http.StatusInternalServerError)
					return
				}
			}
			eventsByWF[e.WorkflowID] = append(eventsByWF[e.WorkflowID], e)
			totalEvents++
		}
		if err := rows.Err(); err != nil {
			h.sendError(w, "Failed to read events", http.StatusInternalServerError)
			return
		}
	}

	// Build turns with metadata and fallback logic
	// 构建带 metadata 的 turn，并处理兜底逻辑
	turns := make([]Turn, 0, len(turnRows))
	globalStart := offset + 1
	for i, t := range turnRows {
		evs := eventsByWF[t.WorkflowID]
		// Final output fallback: result -> first LLM_OUTPUT message -> ""
		// Exclude title_generator agent as its output is the session title, not task result
		// 最终输出兜底顺序：result → 首条 LLM_OUTPUT 消息 → 空串；排除 title_generator agent，它的输出是 session 标题而非 task 结果。
		final := strings.TrimSpace(t.Result.String)
		if final == "" {
			for _, e := range evs {
				if e.Type == "LLM_OUTPUT" && strings.TrimSpace(e.Message) != "" && e.AgentID != "title_generator" {
					final = e.Message
					break
				}
			}
		}
		// Execution time
		// 执行耗时
		execMs := 0
		if t.DurationMs.Valid && t.DurationMs.Int32 > 0 {
			execMs = int(t.DurationMs.Int32)
		} else if t.CompletedAt.Valid {
			ms := int(t.CompletedAt.Time.Sub(t.StartedAt).Milliseconds())
			if ms > 0 {
				execMs = ms
			}
		} else {
			// Still running: compute duration up to now
			// 仍在运行：计算到当前时刻的耗时
			ms := int(time.Since(t.StartedAt).Milliseconds())
			if ms > 0 {
				execMs = ms
			}
		}
		// Agents involved (distinct agent_id from events)
		// 参与的 agent（从 event 中去重的 agent_id）
		agentSet := map[string]struct{}{}
		for _, e := range evs {
			if e.AgentID != "" {
				agentSet[e.AgentID] = struct{}{}
			}
		}
		agents := make([]string, 0, len(agentSet))
		for a := range agentSet {
			agents = append(agents, a)
		}
		sort.Strings(agents) // Sort for deterministic output 注：排序以保证输出稳定

		var turn Turn
		turn.Turn = globalStart + i
		turn.TaskID = t.TaskID
		turn.UserQuery = t.Query
		turn.FinalOutput = final
		turn.Timestamp = t.StartedAt
		turn.Events = evs
		turn.Metadata.TokensUsed = int(t.TotalTokens.Int32)
		turn.Metadata.ExecutionTimeMs = execMs
		turn.Metadata.AgentsInvolved = agents
		turn.Metadata.Attachments = extractTurnAttachmentsFromTaskMetadata(t.Metadata)
		turns = append(turns, turn)
	}

	h.logger.Debug("Session turns retrieved",
		zap.String("session_id", sessionID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.Int("turns", len(turns)),
		zap.Int("total", total),
	)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"session_id": sessionID,
		"count":      total,
		"turns":      turns,
	})
}

func extractTurnAttachmentsFromTaskMetadata(raw sql.NullString) []map[string]any {
	if !raw.Valid || strings.TrimSpace(raw.String) == "" {
		return nil
	}

	var metadata map[string]any
	if err := json.Unmarshal([]byte(raw.String), &metadata); err != nil {
		return nil
	}

	taskContext, ok := metadata["task_context"].(map[string]any)
	if !ok || taskContext == nil {
		return nil
	}
	rawAttachments, ok := taskContext["attachments"].([]any)
	if !ok || len(rawAttachments) == 0 {
		return nil
	}

	attachments := make([]map[string]any, 0, len(rawAttachments))
	for _, item := range rawAttachments {
		am, ok := item.(map[string]any)
		if !ok {
			continue
		}

		out := map[string]any{}
		if id, ok := am["id"].(string); ok && id != "" {
			out["id"] = id
		}
		if mediaType, ok := am["media_type"].(string); ok && mediaType != "" {
			out["media_type"] = mediaType
		}
		if filename, ok := am["filename"].(string); ok && filename != "" {
			out["filename"] = filename
		}
		if size, ok := am["size_bytes"]; ok {
			out["size_bytes"] = size
		}
		if thumb, ok := sanitizeAttachmentThumbnail(am["thumbnail"]); ok {
			out["thumbnail"] = thumb
		}

		if len(out) > 0 {
			attachments = append(attachments, out)
		}
	}

	if len(attachments) == 0 {
		return nil
	}
	return attachments
}

// ListSessions handles GET /api/v1/sessions
// ListSessions 处理 GET /api/v1/sessions
func (h *SessionHandler) ListSessions(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth 中间件取出用户 context
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse pagination params
	// 解析分页参数
	q := r.URL.Query()
	limit := 20
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 100 {
			limit = n
		}
	}
	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			offset = n
		}
	}

	// Query sessions for this user
	// Note: task_executions.session_id is VARCHAR, sessions.id is UUID - match by id or external_id
	// Aggregate tokens_used from task_executions (more accurate than sessions.tokens_used which may not be updated).
	// cache_aware_total_tokens (migration 121) reflects true cost incl. prompt cache.
	// 查询该用户的 session。注意：task_executions.session_id 是 VARCHAR，sessions.id 是 UUID，需按 id 或 external_id 匹配；
	// token 用量从 task_executions 聚合（比可能未更新的 sessions.tokens_used 更准确）；cache_aware_total_tokens（migration 121）含 prompt cache，反映真实成本。
	rows, err := h.db.QueryxContext(ctx, `
        SELECT
            s.id,
            s.user_id,
            COALESCE(s.context->>'title', '') as title,
            s.context as context,
            COALESCE(s.token_budget, 0) as token_budget,
            COALESCE(SUM(t.cache_aware_total_tokens), 0)::int as tokens_used,
            s.created_at,
            s.updated_at,
            s.expires_at,
            COUNT(t.id) as task_count,

            -- Activity tracking (no additional cost - uses existing JOIN)
            MAX(t.completed_at) as last_activity_at,

            -- Task success metrics (conditional COUNT - efficient)
            COUNT(CASE WHEN t.status = 'COMPLETED' THEN 1 END)::int as successful_tasks,
            COUNT(CASE WHEN t.status = 'FAILED' THEN 1 END)::int as failed_tasks,

            -- Cost tracking (simple SUM - no additional JOIN)
            COALESCE(SUM(t.total_cost_usd), 0) as total_cost_usd,

            -- Latest task preview (correlated subqueries - indexed on session_id)
            (SELECT query FROM task_executions
             WHERE (session_id = s.id::text OR session_id = s.context->>'external_id')
             AND user_id = s.user_id
             AND tenant_id = s.tenant_id
             ORDER BY created_at DESC LIMIT 1) as latest_task_query,
            (SELECT status FROM task_executions
             WHERE (session_id = s.id::text OR session_id = s.context->>'external_id')
             AND user_id = s.user_id
             AND tenant_id = s.tenant_id
             ORDER BY created_at DESC LIMIT 1) as latest_task_status,

            -- First task metadata for research detection
            (SELECT mode FROM task_executions
             WHERE (session_id = s.id::text OR session_id = s.context->>'external_id')
             AND user_id = s.user_id
             AND tenant_id = s.tenant_id
             ORDER BY created_at ASC LIMIT 1) as first_task_mode,
            (SELECT metadata::text FROM task_executions
             WHERE (session_id = s.id::text OR session_id = s.context->>'external_id')
             AND user_id = s.user_id
             AND tenant_id = s.tenant_id
             ORDER BY created_at ASC LIMIT 1) as first_task_metadata
        FROM sessions s
        LEFT JOIN task_executions t ON (t.session_id = s.id::text OR t.session_id = s.context->>'external_id')
            AND t.user_id = s.user_id
            AND t.tenant_id = s.tenant_id
        WHERE s.user_id = $1 AND s.tenant_id = $2 AND s.deleted_at IS NULL
        GROUP BY s.id, s.user_id, s.context, s.token_budget, s.created_at, s.updated_at, s.expires_at
        ORDER BY s.created_at DESC
        LIMIT $3 OFFSET $4
    `, userCtx.UserID.String(), userCtx.TenantID, limit, offset)

	if err != nil {
		h.logger.Error("Failed to query sessions", zap.Error(err))
		h.sendError(w, "Failed to retrieve sessions", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// Parse results
	// 解析结果
	sessions := make([]SessionSummary, 0)
	for rows.Next() {
		var sess struct {
			ID                string         `db:"id"`
			UserID            string         `db:"user_id"`
			Title             string         `db:"title"`
			Context           sql.NullString `db:"context"`
			TokenBudget       int            `db:"token_budget"`
			TokensUsed        int            `db:"tokens_used"`
			CreatedAt         time.Time      `db:"created_at"`
			UpdatedAt         sql.NullTime   `db:"updated_at"`
			ExpiresAt         sql.NullTime   `db:"expires_at"`
			TaskCount         int            `db:"task_count"`
			LastActivityAt    sql.NullTime   `db:"last_activity_at"`
			SuccessfulTasks   int            `db:"successful_tasks"`
			FailedTasks       int            `db:"failed_tasks"`
			TotalCostUSD      float64        `db:"total_cost_usd"`
			LatestTaskQuery   sql.NullString `db:"latest_task_query"`
			LatestTaskStatus  sql.NullString `db:"latest_task_status"`
			FirstTaskMode     sql.NullString `db:"first_task_mode"`
			FirstTaskMetadata sql.NullString `db:"first_task_metadata"`
		}

		if err := rows.StructScan(&sess); err != nil {
			h.logger.Error("Failed to scan session", zap.Error(err))
			continue
		}

		summary := SessionSummary{
			SessionID:       sess.ID,
			UserID:          sess.UserID,
			Title:           sess.Title,
			TaskCount:       sess.TaskCount,
			TokensUsed:      sess.TokensUsed,
			CreatedAt:       sess.CreatedAt,
			SuccessfulTasks: sess.SuccessfulTasks,
			FailedTasks:     sess.FailedTasks,
			TotalCostUSD:    sess.TotalCostUSD,
		}

		// Parse context JSON
		// 解析 context JSON
		if sess.Context.Valid && sess.Context.String != "" {
			var ctxMap map[string]any
			if err := json.Unmarshal([]byte(sess.Context.String), &ctxMap); err == nil {
				summary.Context = ctxMap
			}
		}

		// Activity tracking
		// activity 追踪
		if sess.LastActivityAt.Valid {
			summary.LastActivityAt = &sess.LastActivityAt.Time
			// Consider active if last activity within 24 hours
			// 最近 24 小时内有 activity 即视为活跃。
			summary.IsActive = time.Since(sess.LastActivityAt.Time) < 24*time.Hour
		}

		// Task success rate
		// task 成功率
		if sess.TaskCount > 0 {
			summary.SuccessRate = float64(sess.SuccessfulTasks) / float64(sess.TaskCount)
		}

		// Average cost per task
		// 每个 task 的平均 cost
		if sess.TaskCount > 0 && sess.TotalCostUSD > 0 {
			summary.AverageCostPerTask = sess.TotalCostUSD / float64(sess.TaskCount)
		}

		// Budget utilization (computed fields)
		// 预算使用率（计算字段）
		if sess.TokenBudget > 0 {
			summary.TokenBudget = sess.TokenBudget
			summary.BudgetUtilization = float64(sess.TokensUsed) / float64(sess.TokenBudget)
			summary.BudgetRemaining = max(0, sess.TokenBudget-sess.TokensUsed)
			summary.IsNearBudgetLimit = summary.BudgetUtilization > 0.9
		}

		// Latest task preview
		// 最近一次 task 预览
		if sess.LatestTaskQuery.Valid {
			summary.LatestTaskQuery = sess.LatestTaskQuery.String
		}
		if sess.LatestTaskStatus.Valid {
			summary.LatestTaskStatus = sess.LatestTaskStatus.String
		}

		// Research detection - check session context first, then fall back to first task mode
		// Priority: session.context.first_task_mode > session.context.role > task_executions.mode
		// 研究 task 判定：先看 session context，再回退到首个 task 的 mode；优先级为 session.context.first_task_mode > session.context.role > task_executions.mode。
		if summary.Context != nil {
			// Check for first_task_mode in session context (set by backend when session created)
			// 检查 session context 中的 first_task_mode（后端创建 session 时写入）。
			if ftm, ok := summary.Context["first_task_mode"].(string); ok && ftm != "" {
				summary.FirstTaskMode = ftm
			} else if role, ok := summary.Context["role"].(string); ok && role != "" {
				// Fall back to role field (also set by backend for role-based tasks)
				// 回退到 role 字段（后端在按 role 处理 task 时也会写入）
				summary.FirstTaskMode = role
			}
		}

		// If still not set, fall back to database query result
		// 若仍未设置，回退到数据库查询结果
		if summary.FirstTaskMode == "" && sess.FirstTaskMode.Valid {
			summary.FirstTaskMode = sess.FirstTaskMode.String
		}

		isResearch := false
		// Check session context for research flags first (more reliable than task metadata)
		// 先检查 session context 中的研究标记（比 task metadata 更可靠）。
		if summary.Context != nil {
			if fr, ok := summary.Context["force_research"].(bool); ok && fr {
				isResearch = true
			}
		}

		// Fall back to first task's metadata.task_context.force_research
		// 回退到首个 task 的 metadata.task_context.force_research
		if !isResearch && sess.FirstTaskMetadata.Valid && sess.FirstTaskMetadata.String != "" {
			var metadata map[string]any
			if err := json.Unmarshal([]byte(sess.FirstTaskMetadata.String), &metadata); err == nil {
				// Check task_context.force_research (explicit research mode flag)
				// 检查 task_context.force_research（显式研究模式标记）
				if taskContext, ok := metadata["task_context"].(map[string]any); ok {
					if fr, ok := taskContext["force_research"].(bool); ok && fr {
						isResearch = true
					}
				}
			}
		}

		summary.IsResearchSession = isResearch

		if sess.UpdatedAt.Valid {
			summary.UpdatedAt = &sess.UpdatedAt.Time
		}
		if sess.ExpiresAt.Valid {
			summary.ExpiresAt = &sess.ExpiresAt.Time
		}

		sessions = append(sessions, summary)
	}

	if err := rows.Err(); err != nil {
		h.logger.Error("Failed to iterate session rows", zap.Error(err))
		h.sendError(w, "Failed to retrieve sessions", http.StatusInternalServerError)
		return
	}

	// Get total count for pagination
	// 取分页所需的总数
	var totalCount int
	err = h.db.GetContext(ctx, &totalCount, `
        SELECT COUNT(*) FROM sessions WHERE user_id = $1 AND tenant_id = $2 AND deleted_at IS NULL
    `, userCtx.UserID.String(), userCtx.TenantID)
	if err != nil {
		h.logger.Warn("Failed to get total session count", zap.Error(err))
		// Don't fail the request, just return 0 total
		// 不要让请求失败，总数返回 0 即可
		totalCount = len(sessions)
	}

	// Enrich with real-time token usage from Redis if available
	// 若 Redis 可用，用其实时 token 用量补充数据
	if h.redis != nil && len(sessions) > 0 {
		// Prepare keys for MGET using the canonical session ID.
		// For list view, we only use "session:<uuid>" to keep the call efficient.
		// External IDs are still honored for per-session views (GetSession / history / events).
		// 用规范 session ID 准备 MGET 的 key。列表视图只用 "session:<uuid>" 以保证效率；
		// 单 session 视图（GetSession / history / events）仍会识别 external_id。
		var keys []string
		for _, s := range sessions {
			keys = append(keys, fmt.Sprintf("session:%s", s.SessionID))
			// We don't have external_id in SessionSummary easily available without parsing context
			// For list view, we'll stick to session ID for now to keep it efficient.
			// If external_id is critical for token counts in list view, we'd need to fetch it.
			// 列表视图拿不到 external_id（需要解析 context 才有），因此这里只用 session ID；
			// 若列表视图的 token 统计必须覆盖 external_id，需要额外查询。
		}

		// Execute MGET
		// 执行 MGET
		if len(keys) > 0 {
			values, err := h.redis.MGet(ctx, keys...).Result()
			if err != nil {
				h.logger.Warn("Failed to fetch session data from Redis", zap.Error(err))
			} else {
				// Update sessions with Redis data
				// 用 Redis 数据更新 session
				for i, val := range values {
					if val == nil {
						continue
					}

					// Redis returns interface{}, needs type assertion
					// Redis 返回 interface{}，需要类型断言
					if strVal, ok := val.(string); ok {
						var sessionData map[string]any
						if err := json.Unmarshal([]byte(strVal), &sessionData); err == nil {
							if tokens, ok := sessionData["total_tokens_used"].(float64); ok {
								// Update if Redis has more current data
								// 若 Redis 数据更实时则覆盖
								if int(tokens) > sessions[i].TokensUsed {
									sessions[i].TokensUsed = int(tokens)
								}
							}
						}
					}
				}
			}
		}
	}

	h.logger.Debug("Sessions retrieved",
		zap.String("user_id", userCtx.UserID.String()),
		zap.Int("count", len(sessions)),
		zap.Int("total", totalCount),
	)

	// Build response
	// 构建响应
	resp := ListSessionsResponse{
		Sessions:   sessions,
		TotalCount: totalCount,
	}

	// Send response
	// 发送响应
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// sendError sends an error response
// sendError 发送错误响应
func (h *SessionHandler) sendError(w http.ResponseWriter, message string, code int) {
	writeJSON(w, code, map[string]string{"error": message})
}

// UpdateSessionTitle handles PATCH /api/v1/sessions/{sessionId}
// UpdateSessionTitle 处理 PATCH /api/v1/sessions/{sessionId}
func (h *SessionHandler) UpdateSessionTitle(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 取出用户 context
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract session ID from path
	// 从路径中取出 session ID
	sessionID := r.PathValue("sessionId")
	if sessionID == "" {
		h.sendError(w, "Session ID is required", http.StatusBadRequest)
		return
	}

	// Parse request body
	// 解析请求体
	var reqBody struct {
		Title string `json:"title"`
	}
	if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
		h.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validate title
	// 校验 title
	title := strings.TrimSpace(reqBody.Title)
	if title == "" {
		h.sendError(w, "Title cannot be empty", http.StatusBadRequest)
		return
	}

	// Sanitize title: strip control characters (newlines, tabs, zero-width chars, etc.)
	// 清洗标题：去掉控制字符（换行、制表符、零宽字符等）。
	title = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || r == '\u200B' || r == '\uFEFF' {
			return -1 // Remove control characters, zero-width space, and BOM 注：移除控制字符、零宽空格与 BOM
		}
		return r
	}, title)

	// Re-check after sanitization
	// 清洗后重新校验
	if strings.TrimSpace(title) == "" {
		h.sendError(w, "Title cannot contain only control characters", http.StatusBadRequest)
		return
	}

	// Check character count (runes) not byte count to match truncation logic
	// 按 rune 而非字节计数，与截断逻辑保持一致。
	if len([]rune(title)) > 60 {
		h.sendError(w, "Title must be 60 characters or less", http.StatusBadRequest)
		return
	}

	// Verify session exists and user has access (fetch ID for Redis cache)
	// 校验 session 存在且用户有访问权（同时取出 ID 供 Redis cache 使用）
	var session struct {
		ID      string `db:"id"`
		UserID  string `db:"user_id"`
		Context []byte `db:"context"`
	}
	err := h.db.GetContext(ctx, &session, `
		SELECT id, user_id, context FROM sessions
		WHERE (id::text = $1 OR context->>'external_id' = $1)
		  AND user_id = $2
		  AND tenant_id = $3
		  AND deleted_at IS NULL
	`, sessionID, userCtx.UserID.String(), userCtx.TenantID)
	if err == sql.ErrNoRows {
		h.sendError(w, "Session not found", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("Failed to query session", zap.Error(err))
		h.sendError(w, "Failed to update session", http.StatusInternalServerError)
		return
	}

	// Parse existing context
	// 解析已有的 context
	var contextData map[string]any
	if len(session.Context) > 0 {
		if err := json.Unmarshal(session.Context, &contextData); err != nil {
			h.logger.Error("Failed to parse session context", zap.Error(err))
			contextData = make(map[string]any)
		}
	} else {
		contextData = make(map[string]any)
	}

	// Update title in context
	// 在 context 中更新 title
	contextData[SessionContextKeyTitle] = title

	// Serialize updated context
	// 序列化更新后的 context
	updatedContext, err := json.Marshal(contextData)
	if err != nil {
		h.logger.Error("Failed to serialize context", zap.Error(err))
		h.sendError(w, "Failed to update session", http.StatusInternalServerError)
		return
	}

	// Update database (with deleted_at guard for race hardening)
	// 更新数据库（带 deleted_at 条件，缓解并发竞态）。
	_, err = h.db.ExecContext(ctx, `
		UPDATE sessions
		SET context = $1, updated_at = NOW()
		WHERE (id::text = $2 OR context->>'external_id' = $2)
		  AND user_id = $3
		  AND tenant_id = $4
		  AND deleted_at IS NULL
	`, updatedContext, sessionID, userCtx.UserID.String(), userCtx.TenantID)
	if err != nil {
		h.logger.Error("Failed to update session title", zap.Error(err))
		h.sendError(w, "Failed to update session", http.StatusInternalServerError)
		return
	}

	// Also update Redis cache if available (best-effort)
	// Session manager may store sessions under different keys depending on how they were created
	// 同时尽力更新 Redis cache；session manager 可能按创建方式使用不同的 key。
	if h.redis != nil {
		// Try multiple possible Redis keys: input sessionID, DB UUID, and external_id
		// 依次尝试可能的 Redis key：传入的 sessionID、DB 中的 UUID 以及 external_id。
		keysToTry := []string{
			fmt.Sprintf("session:%s", sessionID),  // Original input (could be UUID or external_id) 注：原始输入（可能是 UUID 或 external_id）
			fmt.Sprintf("session:%s", session.ID), // Database UUID 注：数据库中的 UUID
		}
		// Also add external_id if present
		// 若存在 external_id 也一并加入
		if extID, ok := contextData["external_id"].(string); ok && extID != "" {
			keysToTry = append(keysToTry, fmt.Sprintf("session:%s", extID))
		}

		// Try each possible key
		// 依次尝试每个可能的 key
		for _, redisKey := range keysToTry {
			sessionData, err := h.redis.Get(ctx, redisKey).Result()
			if err != nil {
				continue // Try next key 注：尝试下一个 key
			}

			var redisSession map[string]any
			if err := json.Unmarshal([]byte(sessionData), &redisSession); err != nil {
				h.logger.Warn("Failed to unmarshal Redis session data",
					zap.String("redis_key", redisKey),
					zap.Error(err),
				)
				continue
			}

			// Update context in Redis session (lowercase "context" to match Session struct)
			// 更新 Redis session 中的 context（键名用小写 "context" 以匹配 Session 结构体）。
			if redisCtx, ok := redisSession["context"].(map[string]any); ok {
				redisCtx[SessionContextKeyTitle] = title
			} else {
				redisSession["context"] = map[string]any{SessionContextKeyTitle: title}
			}

			// Write back to Redis with KeepTTL to preserve expiration
			// 用 KeepTTL 写回 Redis，保留原有过期时间。
			if updatedData, err := json.Marshal(redisSession); err != nil {
				h.logger.Warn("Failed to marshal updated Redis session",
					zap.String("redis_key", redisKey),
					zap.Error(err),
				)
			} else {
				if err := h.redis.SetArgs(ctx, redisKey, updatedData, redis.SetArgs{KeepTTL: true}).Err(); err != nil {
					h.logger.Warn("Failed to update Redis cache with new title",
						zap.String("redis_key", redisKey),
						zap.Error(err),
					)
				} else {
					h.logger.Debug("Updated Redis session title",
						zap.String("redis_key", redisKey),
						zap.String("new_title", title),
					)
				}
			}
		}
	}

	h.logger.Info("Session title updated",
		zap.String("session_id", sessionID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("new_title", title),
	)

	// Return success with updated title
	// 返回成功及更新后的 title
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{
		"session_id": sessionID,
		"title":      title,
	})
}

// DeleteSession handles DELETE /api/v1/sessions/{sessionId}
// DeleteSession 处理 DELETE /api/v1/sessions/{sessionId}
func (h *SessionHandler) DeleteSession(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 取出用户 context
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract session ID (accepts both UUID and external_id for consistency)
	// 取出 session ID（为保持一致，同时接受 UUID 和 external_id）。
	sessionID := r.PathValue("sessionId")
	if sessionID == "" {
		h.sendError(w, "Session ID is required", http.StatusBadRequest)
		return
	}

	// Ownership check and fetch canonical/id mapping (do not filter on deleted_at to keep idempotency)
	// 校验归属并取出 canonical ID / id 映射（不过滤 deleted_at，以保持幂等）。
	var sessMeta struct {
		ID         string  `db:"id"`
		UserID     string  `db:"user_id"`
		ExternalID *string `db:"external_id"`
	}
	if err := h.db.GetContext(ctx, &sessMeta, `
        SELECT id, user_id, context->>'external_id' as external_id
        FROM sessions
        WHERE (id::text = $1 OR context->>'external_id' = $1)
          AND user_id = $2
          AND tenant_id = $3
    `, sessionID, userCtx.UserID.String(), userCtx.TenantID); err != nil {
		if err == sql.ErrNoRows {
			h.sendError(w, "Session not found", http.StatusNotFound)
			return
		}
		h.logger.Error("Failed to query session for delete", zap.Error(err), zap.String("session_id", sessionID))
		h.sendError(w, "Failed to delete session", http.StatusInternalServerError)
		return
	}

	// Soft delete (idempotent)
	// 软删除（幂等）
	if _, err := h.db.ExecContext(ctx, `
        UPDATE sessions
        SET deleted_at = NOW(), deleted_by = $2, updated_at = NOW()
        WHERE (id::text = $1 OR context->>'external_id' = $1)
          AND user_id = $3
          AND tenant_id = $4
          AND deleted_at IS NULL
    `, sessionID, userCtx.UserID.String(), userCtx.UserID.String(), userCtx.TenantID); err != nil {
		h.logger.Error("Failed to soft delete session", zap.Error(err), zap.String("session_id", sessionID))
		h.sendError(w, "Failed to delete session", http.StatusInternalServerError)
		return
	}

	// Resolve workspace key using same logic as file APIs (workspace.go:279-302)
	// CRITICAL: workspace paths use external_id if present, not session UUID
	// 用与文件 API 相同的逻辑解析 workspace key（workspace.go:279-302）。关键：workspace 路径使用 external_id（若存在），而不是 session UUID。
	workspaceKey := sessMeta.ID
	if sessMeta.ExternalID != nil && strings.TrimSpace(*sessMeta.ExternalID) != "" {
		workspaceKey = strings.TrimSpace(*sessMeta.ExternalID)
	}

	// SECURITY: Validate workspaceKey and ensure path can't escape base dir
	// (external_id isn't validated at DB write time - reuse workspace.go guardrails)
	// 安全：校验 workspaceKey，确保路径无法逃出基目录（external_id 在写入 DB 时未做校验，因此复用 workspace.go 的防护）。
	workspacePath, err := buildWorkspacePath(h.workspaceBaseDir, workspaceKey)
	if err != nil {
		h.logger.Warn("Invalid workspace key, skipping cleanup",
			zap.Error(err),
			zap.String("workspace_key", workspaceKey))
		// Don't fail the delete - just skip cleanup
		// 不让删除失败——仅跳过清理
	} else {
		// Attempt workspace cleanup (best-effort)
		// 尝试清理 workspace（尽力而为）
		if cleanupErr := h.cleanupWorkspace(ctx, workspaceKey, workspacePath); cleanupErr != nil {
			h.logger.Warn("Workspace cleanup failed (will be cleaned by cron)",
				zap.Error(cleanupErr),
				zap.String("session_id", sessMeta.ID),
				zap.String("workspace_key", workspaceKey))
			// Don't return error - session is already soft-deleted
			// 不返回错误——session 已完成软删除
		}
	}

	// Clear Redis cache for this session (best-effort)
	// 清理该 session 的 Redis cache（尽力而为）
	if h.redis != nil {
		// Try all possible keys: original input, DB UUID, and external_id (if present)
		// 尝试所有可能的 key：原始输入、DB 中的 UUID 以及 external_id（若有）。
		keys := []string{fmt.Sprintf("session:%s", sessionID), fmt.Sprintf("session:%s", sessMeta.ID)}
		if sessMeta.ExternalID != nil && *sessMeta.ExternalID != "" {
			keys = append(keys, fmt.Sprintf("session:%s", *sessMeta.ExternalID))
		}
		for _, key := range keys {
			if err := h.redis.Del(ctx, key).Err(); err != nil {
				h.logger.Warn("Failed to clear session cache", zap.Error(err), zap.String("redis_key", key))
			}
		}
	}

	// No content, idempotent
	// 无内容返回，保持幂等
	w.WriteHeader(http.StatusNoContent)
}

// cleanupWorkspace calls the Firecracker executor to clean up workspace files.
// cleanupWorkspace 调用 Firecracker executor 清理 workspace 文件。
func (h *SessionHandler) cleanupWorkspace(ctx context.Context, sessionID, workspacePath string) error {
	reqBody, err := json.Marshal(map[string]string{
		"session_id":     sessionID,
		"workspace_path": workspacePath,
	})
	if err != nil {
		return fmt.Errorf("failed to marshal request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		h.firecrackerURL+"/workspace/cleanup", bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("firecracker request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("cleanup failed: %s", string(body))
	}

	h.logger.Info("Workspace cleanup succeeded",
		zap.String("session_id", sessionID),
		zap.String("workspace_path", workspacePath))

	return nil
}
