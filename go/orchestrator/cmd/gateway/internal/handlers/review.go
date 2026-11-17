package handlers

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
)

// MaxReviewRounds is the maximum number of review conversation rounds allowed.
// At the final round, the LLM is instructed to produce a definitive plan.
// Beyond this limit, further feedback is rejected (user must approve).
// MaxReviewRounds 是评审对话允许的最大轮数；到达最后一轮时要求 LLM 给出确定方案，
// 超过上限则拒绝继续反馈（用户只能批准）。
const MaxReviewRounds = 10

// ReviewHandler handles HITL research plan review requests.
// ReviewHandler 处理 HITL 研究计划评审 request。
type ReviewHandler struct {
	orchClient orchpb.OrchestratorServiceClient
	redis      *redis.Client
	logger     *zap.Logger
}

// NewReviewHandler creates a new ReviewHandler.
// NewReviewHandler 创建新的 ReviewHandler。
func NewReviewHandler(
	orchClient orchpb.OrchestratorServiceClient,
	redis *redis.Client,
	logger *zap.Logger,
) *ReviewHandler {
	return &ReviewHandler{
		orchClient: orchClient,
		redis:      redis,
		logger:     logger,
	}
}

// reviewRequest is the HTTP request body for the review endpoint.
// reviewRequest 是评审 endpoint 的 HTTP request body。
type reviewRequest struct {
	Action  string `json:"action"`  // "feedback" or "approve" 注："feedback" 或 "approve"
	Message string `json:"message"` // User's feedback message (optional for approve) 注：用户的反馈消息（approve 时可省略）
}

// reviewRound mirrors the activity type for Redis storage.
// reviewRound 对应 Redis 存储使用的 activity 类型。
type reviewRound struct {
	Role      string `json:"role"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}

// reviewState is the Redis-stored conversation state.
// reviewState 是存放在 Redis 中的对话状态。
type reviewState struct {
	WorkflowID    string         `json:"workflow_id"`
	Query         string         `json:"query"`
	Context       map[string]any `json:"context"`
	Status        string         `json:"status"`
	Round         int            `json:"round"`
	Version       int            `json:"version"`
	OwnerUserID   string         `json:"owner_user_id"`
	OwnerTenantID string         `json:"owner_tenant_id"`
	Rounds        []reviewRound  `json:"rounds"`
	CurrentPlan   string         `json:"current_plan"`
	ResearchBrief string         `json:"research_brief,omitempty"`
}

// llmResearchPlanRequest is the request to the LLM service.
// llmResearchPlanRequest 是发给 LLM 服务的 request。
type llmResearchPlanRequest struct {
	Query        string         `json:"query"`
	Context      map[string]any `json:"context"`
	Conversation []reviewRound  `json:"conversation"`
	IsFinalRound bool           `json:"is_final_round,omitempty"`
	CurrentRound int            `json:"current_round"`
	MaxRounds    int            `json:"max_rounds"`
}

// llmResearchPlanResponse is the response from the LLM service.
// llmResearchPlanResponse 是 LLM 服务返回的 response。
type llmResearchPlanResponse struct {
	Message      string `json:"message"`
	Intent       string `json:"intent"`
	Round        int    `json:"round"`
	Model        string `json:"model"`
	Provider     string `json:"provider"`
	InputTokens  int    `json:"input_tokens"`
	OutputTokens int    `json:"output_tokens"`
}

// HandleReview processes review feedback or approval.
// HandleReview 处理评审反馈或批准。
func (h *ReviewHandler) HandleReview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		h.sendError(w, "workflow_id is required", http.StatusBadRequest)
		return
	}

	// Auth check
	// 鉴权校验
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok || userCtx == nil {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse request
	// 解析 request
	var req reviewRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, "Invalid request body", http.StatusBadRequest)
		return
	}
	if req.Action != "feedback" && req.Action != "approve" {
		h.sendError(w, "action must be 'feedback' or 'approve'", http.StatusBadRequest)
		return
	}

	// Load state from Redis
	// 从 Redis 加载状态
	key := fmt.Sprintf("review:%s", workflowID)
	data, err := h.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		h.sendError(w, "Review session not found or expired", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("Failed to read review state from Redis", zap.Error(err))
		h.sendError(w, "Internal error", http.StatusInternalServerError)
		return
	}

	var state reviewState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		h.logger.Error("Failed to unmarshal review state", zap.Error(err))
		h.sendError(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Owner check (skip if owner not set — e.g., dev mode without user_id in context)
	// 归属校验（owner 未设置时跳过，例如开发 mode 下 context 中没有 user_id）。
	if state.OwnerUserID != "" && state.OwnerUserID != userCtx.UserID.String() {
		h.sendError(w, "Forbidden: not the task owner", http.StatusForbidden)
		return
	}
	if state.OwnerTenantID != "" && state.OwnerTenantID != userCtx.TenantID.String() {
		h.sendError(w, "Forbidden: not authorized for this tenant", http.StatusForbidden)
		return
	}

	switch req.Action {
	case "feedback":
		h.handleFeedback(ctx, w, r, key, &state, req, workflowID)
	case "approve":
		h.handleApprove(ctx, w, r, key, &state, workflowID, userCtx)
	}
}

func (h *ReviewHandler) handleFeedback(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
	key string, state *reviewState, req reviewRequest, workflowID string,
) {
	if state.Status == "approved" {
		h.sendError(w, "Review already approved", http.StatusConflict)
		return
	}

	// Optimistic concurrency check
	// 乐观并发校验
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if ifMatch != strconv.Itoa(state.Version) {
			h.sendError(w, "Conflict: state has been modified", http.StatusConflict)
			return
		}
	}

	// Acquire distributed lock to prevent race condition during long LLM call.
	// Without this, two concurrent feedback requests can both pass version check,
	// then clobber each other's state on write.
	// 加分布式锁，避免长时间 LLM 调用期间的竞态：否则两个并发反馈 request 可能都通过版本校验，写入时互相覆盖。
	lockKey := key + ":lock"
	lockTTL := 90 * time.Second // longer than LLM timeout (60s) + buffer 注：长于 LLM timeout（60s）加缓冲
	// Use unique lock value to prevent accidental deletion of another request's lock
	// if TTL expires and lock is re-acquired by another request.
	// 锁值保持唯一，避免 TTL 过期后锁被他人获取时误删对方的锁。
	lockBytes := make([]byte, 16)
	if _, err := rand.Read(lockBytes); err != nil {
		h.logger.Error("Failed to generate lock value", zap.Error(err))
		h.sendError(w, "Failed to process feedback", http.StatusInternalServerError)
		return
	}
	lockValue := hex.EncodeToString(lockBytes)
	acquired, err := h.redis.SetNX(ctx, lockKey, lockValue, lockTTL).Result()
	if err != nil {
		h.logger.Error("Failed to acquire feedback lock", zap.Error(err))
		h.sendError(w, "Failed to process feedback", http.StatusInternalServerError)
		return
	}
	if !acquired {
		h.sendError(w, "Another feedback request is in progress. Please wait.", http.StatusConflict)
		return
	}
	// Conditional delete: only release lock if we still own it (value matches).
	// Prevents accidentally deleting another request's lock if ours expired.
	// 条件删除：只有仍持有锁（value 匹配）才释放，防止锁过期后误删他人的锁。
	defer func() {
		unlockScript := redis.NewScript(`
			if redis.call("get", KEYS[1]) == ARGV[1] then
				return redis.call("del", KEYS[1])
			else
				return 0
			end
		`)
		unlockScript.Run(ctx, h.redis, []string{lockKey}, lockValue)
	}()

	if req.Message == "" {
		h.sendError(w, "message is required for feedback", http.StatusBadRequest)
		return
	}
	// Limit message length to prevent excessively large payloads
	// 限制消息长度，避免 payload 过大
	const maxMessageLen = 10 * 1024 // 10KB 注：10KB
	if len(req.Message) > maxMessageLen {
		h.sendError(w, "message exceeds maximum length (10KB)", http.StatusBadRequest)
		return
	}

	// Round limit check: reject feedback beyond MaxReviewRounds
	// 轮数上限校验：超过 MaxReviewRounds 的反馈直接拒绝
	nextRound := state.Round + 1
	if nextRound > MaxReviewRounds {
		h.sendError(w, "Maximum review rounds reached. Please approve the plan.", http.StatusConflict)
		return
	}
	isFinalRound := nextRound >= MaxReviewRounds

	// Append user message
	// 追加用户消息
	state.Rounds = append(state.Rounds, reviewRound{
		Role:      "user",
		Message:   req.Message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})

	// Call LLM service for updated plan.
	// Use a detached context so client disconnects don't cancel the LLM call.
	// 调用 LLM 服务获取更新后的计划；使用独立的 context，客户端断连不会取消这次 LLM 调用。
	llmCtx, llmCancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer llmCancel()
	plan, err := h.callResearchPlan(llmCtx, state.Query, state.Context, state.Rounds, isFinalRound, nextRound)
	if err != nil {
		h.logger.Error("Failed to generate research plan", zap.Error(err))
		// Remove the user message we appended (don't persist bad state)
		// 移除刚追加的用户消息（不把坏状态落盘）
		state.Rounds = state.Rounds[:len(state.Rounds)-1]
		h.sendError(w, "Failed to generate updated plan", http.StatusBadGateway)
		return
	}

	// Record token usage (best-effort, non-blocking)
	// 记录 token 用量（best-effort，不阻塞）
	grpcCtx := withGRPCMetadata(context.Background(), r)
	go func(grpcCtx context.Context) {
		rCtx, cancel := context.WithTimeout(grpcCtx, 5*time.Second)
		defer cancel()
		_, err := h.orchClient.RecordTokenUsage(rCtx, &orchpb.RecordTokenUsageRequest{
			WorkflowId:   workflowID,
			AgentId:      "research-planner",
			Model:        plan.Model,
			Provider:     plan.Provider,
			InputTokens:  int32(plan.InputTokens),
			OutputTokens: int32(plan.OutputTokens),
		})
		if err != nil {
			h.logger.Warn("Failed to record token usage (best-effort)", zap.Error(err))
		}
	}(grpcCtx)

	// Parse and strip [RESEARCH_BRIEF] block from LLM response (machine-consumed metadata).
	// Same pattern as [INTENT:...] — parsed by gateway, stripped from user-visible message.
	// 解析并剥离 [RESEARCH_BRIEF] 块（供机器消费的 metadata），与 [INTENT:...] 同一套路：由 gateway 解析，并从用户可见消息中移除。
	briefRegex := regexp.MustCompile(`(?s)\[RESEARCH_BRIEF\]\n?(.*?)\n?\[/RESEARCH_BRIEF\]`)
	if match := briefRegex.FindStringSubmatch(plan.Message); len(match) > 1 {
		state.ResearchBrief = strings.TrimSpace(match[1])
		plan.Message = strings.TrimSpace(briefRegex.ReplaceAllString(plan.Message, ""))
	}

	// Append assistant response (with [RESEARCH_BRIEF] already stripped)
	// 追加 assistant response（[RESEARCH_BRIEF] 已剥离）
	state.Rounds = append(state.Rounds, reviewRound{
		Role:      "assistant",
		Message:   plan.Message,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	})
	// Always update CurrentPlan with the latest assistant message so users can approve
	// at any point. The intent is a hint for the UI (show approve button more prominently
	// when ready), but backend allows approval of any plan.
	// 始终用最新的 assistant 消息更新 CurrentPlan，使用户随时可以批准；intent 只是给 UI 的提示
	// （ready 时更突出地展示批准按钮），后端允许批准任何计划。
	state.CurrentPlan = plan.Message
	state.Round++
	state.Version++

	// Determine intent (default to "feedback" if LLM didn't provide one)
	// 确定 intent（LLM 未给出时默认 "feedback"）
	intent := plan.Intent
	if intent == "" {
		intent = "feedback"
	}
	// Force ready on final round — LLM should output a definitive direction,
	// but we enforce it regardless. User still needs to click Approve.
	// 最后一轮强制置为 ready——LLM 本应给出确定方向，但这里无论如何都强制；用户仍需点击批准。
	if isFinalRound && intent == "feedback" {
		intent = "ready"
		// Also set CurrentPlan so Approve has a non-empty FinalPlan
		// 同时设置 CurrentPlan，让批准时 FinalPlan 非空
		state.CurrentPlan = plan.Message
	}

	// Save to Redis (keep original TTL) — AFTER intent/CurrentPlan resolution
	// 写入 Redis（保留原有 TTL）——在确定 intent/CurrentPlan 之后。
	stateBytes, _ := json.Marshal(state)
	ttl, err := h.redis.TTL(ctx, key).Result()
	if err != nil || ttl <= 0 {
		ttl = 20 * time.Minute // fallback (15min review + 5min buffer) 注：兜底值（15 分钟评审 + 5 分钟缓冲）
	}
	if err := h.redis.Set(ctx, key, stateBytes, ttl).Err(); err != nil {
		h.logger.Error("Failed to save review state to Redis", zap.Error(err), zap.String("workflow_id", workflowID))
		h.sendError(w, "Failed to save review state", http.StatusInternalServerError)
		return
	}

	// Publish review events to Redis stream so they're captured by SSE and persisted.
	// This makes the review conversation visible in session history on page reload.
	// 把评审 event 发布到 Redis stream，以便 SSE 捕获并持久化，页面刷新后评审对话仍能在 session history 中看到。
	roundPayload := map[string]any{"round": state.Round, "version": state.Version}
	h.publishStreamEvent(workflowID, "REVIEW_USER_FEEDBACK", "user", req.Message, roundPayload)
	h.publishStreamEvent(workflowID, "RESEARCH_PLAN_UPDATED", "research-planner", plan.Message,
		map[string]any{"round": state.Round, "version": state.Version, "intent": intent})

	// Response
	// 响应
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", strconv.Itoa(state.Version))
	json.NewEncoder(w).Encode(map[string]any{
		"status": "reviewing",
		"plan": map[string]any{
			"message": plan.Message,
			"round":   state.Round,
			"version": state.Version,
			"intent":  intent,
		},
	})
}

func (h *ReviewHandler) handleApprove(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
	key string, state *reviewState, workflowID string, userCtx *auth.UserContext,
) {
	if state.Status == "approved" {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"status":  "approved",
			"message": "Research started",
		})
		return
	}

	// Optimistic concurrency check (ensure user approves the plan they saw)
	// 乐观并发校验：确保用户批准的是他看到的那个版本。
	if ifMatch := r.Header.Get("If-Match"); ifMatch != "" {
		if ifMatch != strconv.Itoa(state.Version) {
			h.sendError(w, "Conflict: plan has been updated. Please review the latest version.", http.StatusConflict)
			return
		}
	}

	// Acquire lock to prevent race with in-flight feedback.
	// If feedback is mid-flight, it holds the lock and we fail fast.
	// If we acquire first, feedback will fail when it tries to acquire.
	// 加锁避免与进行中的反馈 request 竞争：若反馈正在进行，它持有锁，这里快速失败；若先拿到锁，反馈 request 稍后会获取失败。
	lockKey := key + ":lock"
	lockTTL := 30 * time.Second // shorter than feedback (approve is fast) 注：比反馈路径短（批准很快）
	lockBytes := make([]byte, 16)
	if _, err := rand.Read(lockBytes); err != nil {
		h.logger.Error("Failed to generate lock value", zap.Error(err))
		h.sendError(w, "Failed to process approval", http.StatusInternalServerError)
		return
	}
	lockValue := hex.EncodeToString(lockBytes)
	acquired, err := h.redis.SetNX(ctx, lockKey, lockValue, lockTTL).Result()
	if err != nil {
		h.logger.Error("Failed to acquire approval lock", zap.Error(err))
		h.sendError(w, "Failed to process approval", http.StatusInternalServerError)
		return
	}
	if !acquired {
		h.sendError(w, "A feedback request is in progress. Please wait and try again.", http.StatusConflict)
		return
	}
	defer func() {
		unlockScript := redis.NewScript(`
			if redis.call("get", KEYS[1]) == ARGV[1] then
				return redis.call("del", KEYS[1])
			else
				return 0
			end
		`)
		unlockScript.Run(ctx, h.redis, []string{lockKey}, lockValue)
	}()

	// Validate that a research plan exists before approval.
	// CurrentPlan is only set when LLM returns intent="ready" with an actionable direction.
	// Empty plan means LLM only asked clarifying questions (intent="feedback") or user
	// skipped directly to approve without getting a plan.
	// 批准前确认存在研究计划。仅当 LLM 返回 intent=ready 且给出可执行方向时才会设置 CurrentPlan；
	// 计划为空说明 LLM 只问了澄清问题（intent=feedback），或用户未生成计划就直接批准。
	if strings.TrimSpace(state.CurrentPlan) == "" {
		h.sendError(w, "No research plan to approve. Please provide feedback to generate a plan first.", http.StatusBadRequest)
		return
	}

	// Marshal conversation for gRPC
	// 为 gRPC 序列化对话
	convBytes, _ := json.Marshal(state.Rounds)

	// Send via dedicated gRPC (Gateway → Orchestrator → Temporal Signal)
	// 通过专用 gRPC 发送（Gateway → Orchestrator → Temporal Signal）
	grpcCtx := withGRPCMetadata(ctx, r)
	_, err = h.orchClient.SubmitReviewDecision(grpcCtx, &orchpb.SubmitReviewDecisionRequest{
		WorkflowId:    workflowID,
		Approved:      true,
		FinalPlan:     state.CurrentPlan,
		Conversation:  string(convBytes),
		ApprovedBy:    userCtx.UserID.String(),
		ResearchBrief: state.ResearchBrief,
	})
	if err != nil {
		h.logger.Error("Failed to submit review decision", zap.Error(err))
		h.sendError(w, "Failed to approve review", http.StatusBadGateway)
		return
	}

	// Mark review as approved in Redis (keep state for page reload, TTL handles cleanup)
	// 在 Redis 中标记为已批准（保留状态供页面刷新，过期由 TTL 处理）。
	state.Status = "approved"
	if approvedBytes, err := json.Marshal(state); err == nil {
		if err := h.redis.Set(ctx, key, approvedBytes, 60*time.Minute).Err(); err != nil {
			h.logger.Warn("Failed to update review state in Redis (best-effort)", zap.Error(err))
		}
	}

	h.logger.Info("HITL review approved via gateway",
		zap.String("workflow_id", workflowID),
		zap.String("approved_by", userCtx.UserID.String()),
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"status":  "approved",
		"message": "Research started",
	})
}

func (h *ReviewHandler) callResearchPlan(
	ctx context.Context,
	query string,
	taskCtx map[string]any,
	rounds []reviewRound,
	isFinalRound bool,
	currentRound int,
) (*llmResearchPlanResponse, error) {
	base := os.Getenv("LLM_SERVICE_URL")
	if base == "" {
		base = "http://llm-service:8000"
	}
	url := fmt.Sprintf("%s/agent/research-plan", base)

	reqBody := llmResearchPlanRequest{
		Query:        query,
		Context:      taskCtx,
		Conversation: rounds,
		IsFinalRound: isFinalRound,
		CurrentRound: currentRound,
		MaxRounds:    MaxReviewRounds,
	}
	bodyBytes, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(bodyBytes))
	if err != nil {
		return nil, err
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{
		// Timeout controlled by context (60s from callResearchPlan caller)
		// timeout 由 context 控制（调用方 callResearchPlan 传 60s）
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("LLM service call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Read body for logging but don't expose internal details to client
		// 读取 body 用于日志，但不向客户端暴露内部细节
		body, _ := io.ReadAll(resp.Body)
		// Log detailed error for debugging (will be logged by caller)
		// 记录详细错误便于排查（调用方也会记录）
		return nil, fmt.Errorf("LLM service error (status %d, body: %s)", resp.StatusCode, truncateForLog(string(body), 500))
	}

	var result llmResearchPlanResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode LLM response: %w", err)
	}

	if result.Message == "" {
		return nil, fmt.Errorf("LLM service returned empty plan message")
	}

	return &result, nil
}

// HandleGetReview returns the current review conversation state from Redis.
// HandleGetReview 从 Redis 返回当前评审对话状态。
func (h *ReviewHandler) HandleGetReview(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		h.sendError(w, "workflow_id is required", http.StatusBadRequest)
		return
	}

	// Auth check
	// 鉴权校验
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok || userCtx == nil {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Load state from Redis
	// 从 Redis 加载状态
	key := fmt.Sprintf("review:%s", workflowID)
	data, err := h.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		h.sendError(w, "Review session not found or expired", http.StatusNotFound)
		return
	}
	if err != nil {
		h.logger.Error("Failed to read review state from Redis", zap.Error(err))
		h.sendError(w, "Internal error", http.StatusInternalServerError)
		return
	}

	var state reviewState
	if err := json.Unmarshal([]byte(data), &state); err != nil {
		h.logger.Error("Failed to unmarshal review state", zap.Error(err))
		h.sendError(w, "Internal error", http.StatusInternalServerError)
		return
	}

	// Owner check
	// 归属校验
	if state.OwnerUserID != "" && state.OwnerUserID != userCtx.UserID.String() {
		h.sendError(w, "Forbidden: not the task owner", http.StatusForbidden)
		return
	}
	if state.OwnerTenantID != "" && state.OwnerTenantID != userCtx.TenantID.String() {
		h.sendError(w, "Forbidden: not authorized for this tenant", http.StatusForbidden)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("ETag", strconv.Itoa(state.Version))
	json.NewEncoder(w).Encode(map[string]any{
		"status":       state.Status,
		"round":        state.Round,
		"version":      state.Version,
		"current_plan": state.CurrentPlan,
		"rounds":       state.Rounds,
		"query":        state.Query,
	})
}

// publishStreamEvent publishes a review event to the Redis event stream so it's
// captured by SSE and persisted to DB — same format as orchestrator events.
// This makes review conversation messages first-class citizens in the event system.
// publishStreamEvent 把评审 event 发布到 Redis event stream，由 SSE 捕获并持久化到 DB，格式与 orchestrator event 一致，
// 使评审对话成为 event 系统中的一等公民。
func (h *ReviewHandler) publishStreamEvent(workflowID, eventType, agentID, message string, payload map[string]any) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	streamKey := fmt.Sprintf("shannon:workflow:events:%s", workflowID)
	seqKey := fmt.Sprintf("shannon:workflow:events:%s:seq", workflowID)

	seq, err := h.redis.Incr(ctx, seqKey).Result()
	if err != nil {
		h.logger.Warn("Failed to increment event seq", zap.Error(err))
		return
	}

	payloadJSON := "{}"
	if payload != nil {
		if b, err := json.Marshal(payload); err == nil {
			payloadJSON = string(b)
		}
	}

	_, err = h.redis.XAdd(ctx, &redis.XAddArgs{
		Stream: streamKey,
		MaxLen: 256,
		Approx: true,
		Values: map[string]any{
			"workflow_id": workflowID,
			"type":        eventType,
			"agent_id":    agentID,
			"message":     message,
			"payload":     payloadJSON,
			"ts_nano":     strconv.FormatInt(time.Now().UnixNano(), 10),
			"seq":         strconv.FormatUint(uint64(seq), 10),
		},
	}).Result()
	if err != nil {
		h.logger.Warn("Failed to publish review event to stream", zap.Error(err), zap.String("type", eventType))
	} else {
		h.logger.Info("Published review event to stream",
			zap.String("workflow_id", workflowID),
			zap.String("type", eventType),
			zap.Int64("seq", seq),
		)
	}
}

func (h *ReviewHandler) sendError(w http.ResponseWriter, message string, code int) {
	writeJSON(w, code, map[string]string{"error": message})
}

// truncateForLog truncates a string to maxLen for safe logging
// truncateForLog 把字符串截断到 maxLen，便于安全记录日志
func truncateForLog(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}
