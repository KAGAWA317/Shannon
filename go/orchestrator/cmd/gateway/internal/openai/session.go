package openai

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	// SessionPrefix is the Redis key prefix for OpenAI sessions
	// SessionPrefix 是 OpenAI session 在 Redis 中的 key 前缀
	SessionPrefix = "openai:session:"

	// SessionTTL is the default session time-to-live
	// SessionTTL 是 session 的默认存活时间
	SessionTTL = 24 * time.Hour

	// HeaderSessionID is the header name for session ID
	// HeaderSessionID 是传递 session ID 的 header 名
	HeaderSessionID = "X-Session-ID"
)

// SessionInfo contains metadata about a session
// SessionInfo 包含 session 的元数据
type SessionInfo struct {
	SessionID    string    `json:"session_id"`
	UserID       string    `json:"user_id"`
	TenantID     string    `json:"tenant_id"`
	ShannonSID   string    `json:"shannon_session_id"` // Mapped Shannon session 注：映射到的 Shannon session
	CreatedAt    time.Time `json:"created_at"`
	LastUsedAt   time.Time `json:"last_used_at"`
	MessageCount int       `json:"message_count"`
}

// sessionTouchRequest represents a background session touch request
// sessionTouchRequest 表示一次后台 session touch 请求
type sessionTouchRequest struct {
	sessionID string
}

// SessionManager handles OpenAI session management
// SessionManager 负责 OpenAI session 管理
type SessionManager struct {
	redis   *redis.Client
	logger  *zap.Logger
	ttl     time.Duration
	touchCh chan sessionTouchRequest // Bounded channel for async session touches 注：用于异步 session touch 的有界 channel
	stopCh  chan struct{}            // Signal to stop background workers 注：通知后台 worker 停止
}

// NewSessionManager creates a new session manager
// NewSessionManager 创建新的 session manager
func NewSessionManager(redisClient *redis.Client, logger *zap.Logger) *SessionManager {
	sm := &SessionManager{
		redis:   redisClient,
		logger:  logger,
		ttl:     SessionTTL,
		touchCh: make(chan sessionTouchRequest, 50), // Bounded buffer 注：有界缓冲区
		stopCh:  make(chan struct{}),
	}

	// Start background worker for session touches
	// 启动处理 session touch 的后台 worker
	go sm.touchWorker()

	return sm
}

// Close gracefully shuts down the session manager
// Close 优雅关闭 session manager
func (sm *SessionManager) Close() {
	close(sm.stopCh)
}

// touchWorker processes session touch requests from the channel
// touchWorker 从 channel 中取并处理 session touch 请求
func (sm *SessionManager) touchWorker() {
	for {
		select {
		case req := <-sm.touchCh:
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			sm.doTouchSession(ctx, req.sessionID)
			cancel()
		case <-sm.stopCh:
			return
		}
	}
}

// enqueueTouchSession adds a session touch to the async queue (non-blocking)
// enqueueTouchSession 把 session touch 放入异步队列（非阻塞）
func (sm *SessionManager) enqueueTouchSession(sessionID string) {
	select {
	case sm.touchCh <- sessionTouchRequest{sessionID: sessionID}:
		// Successfully queued
		// 已成功入队
	default:
		// Channel full, skip (better than unbounded goroutines)
		// channel 已满则跳过（好过无上限地起 goroutine）
		sm.logger.Debug("Session touch queue full, skipping",
			zap.String("session_id", sessionID))
	}
}

// SessionResult contains the resolved session and whether it was newly created
// SessionResult 包含解析出的 session 以及它是否为新建
type SessionResult struct {
	SessionID      string // The OpenAI session ID to use 注：要使用的 OpenAI session ID
	ShannonSession string // The mapped Shannon session ID 注：映射到的 Shannon session ID
	IsNew          bool   // True if a new session was created 注：新建了 session 时为 true
	WasCollision   bool   // True if the provided session was rejected due to ownership 注：提供的 session 因归属不符被拒绝时为 true
}

// ResolveSession resolves or creates a session for a request
// - If sessionID is empty, derives one from the request
// - If sessionID is provided but owned by different user, creates new and flags collision
// - Returns the session to use and whether headers should be echoed
// ResolveSession 为 request 解析或创建 session：
// - sessionID 为空时由 request 内容推导；
// - sessionID 已提供但属于其他用户时，新建 session 并标记 collision；
// - 返回应使用的 session 以及是否需要回写 header。
func (sm *SessionManager) ResolveSession(
	ctx context.Context,
	providedSessionID string,
	userID, tenantID string,
	req *ChatCompletionRequest,
) (*SessionResult, error) {
	result := &SessionResult{}

	// Case 1: No session provided - derive from request
	// 情况一：未提供 session——由 request 推导
	if providedSessionID == "" {
		sessionID := sm.deriveSessionID(req, userID)
		shannonSID, isNew, err := sm.getOrCreateSession(ctx, sessionID, userID, tenantID)
		if err != nil {
			return nil, err
		}
		result.SessionID = sessionID
		result.ShannonSession = shannonSID
		result.IsNew = isNew
		return result, nil
	}

	// Case 2: Session provided - check ownership
	// 情况二：已提供 session——校验归属
	info, err := sm.getSessionInfo(ctx, providedSessionID)
	if err != nil && err != redis.Nil {
		return nil, fmt.Errorf("failed to check session: %w", err)
	}

	// Session exists - verify ownership
	// session 已存在——校验归属
	if info != nil {
		if info.UserID != userID || info.TenantID != tenantID {
			// Collision: session belongs to different user/tenant
			// Generate new session and flag collision
			// 冲突：session 属于其他 user/tenant，生成新 session 并标记 collision。
			sm.logger.Info("Session collision detected, generating new session",
				zap.String("provided_session", providedSessionID),
				zap.String("user_id", userID),
			)
			newSessionID := sm.generateSessionID()
			shannonSID, _, err := sm.getOrCreateSession(ctx, newSessionID, userID, tenantID)
			if err != nil {
				return nil, err
			}
			result.SessionID = newSessionID
			result.ShannonSession = shannonSID
			result.IsNew = true
			result.WasCollision = true
			return result, nil
		}

		// Session owned by this user - use it
		// session 属于当前用户——直接使用
		result.SessionID = providedSessionID
		result.ShannonSession = info.ShannonSID
		result.IsNew = false

		// Update last used via bounded worker (prevents unbounded goroutines)
		// 通过有界 worker 更新 last used，避免 goroutine 数量失控。
		sm.enqueueTouchSession(providedSessionID)
		return result, nil
	}

	// Session doesn't exist - create with provided ID
	// session 不存在——用提供的 ID 新建
	shannonSID, _, err := sm.getOrCreateSession(ctx, providedSessionID, userID, tenantID)
	if err != nil {
		return nil, err
	}
	result.SessionID = providedSessionID
	result.ShannonSession = shannonSID
	result.IsNew = true
	return result, nil
}

// deriveSessionID creates a deterministic session ID from request content
// deriveSessionID 由 request 内容生成确定性的 session ID。
func (sm *SessionManager) deriveSessionID(req *ChatCompletionRequest, userID string) string {
	var parts []string

	// Include user ID for isolation
	// 纳入 user ID 以保证隔离
	parts = append(parts, userID)

	// Include system message if present
	// 存在 system message 时一并纳入
	for _, msg := range req.Messages {
		if msg.Role == "system" {
			parts = append(parts, msg.Content[:min(200, len(msg.Content))])
			break
		}
	}

	// Include first user message (use attachment summary for attachment-only messages)
	// 纳入第一条 user message（纯附件消息改用附件摘要）
	for _, msg := range req.Messages {
		if msg.Role == "user" {
			content := msg.Content
			if content == "" && msg.ContentWithAttachmentSummary != "" {
				content = msg.ContentWithAttachmentSummary
			}
			parts = append(parts, content[:min(100, len(content))])
			break
		}
	}

	// Hash to create session ID
	// 做哈希以生成 session ID
	h := sha256.New()
	h.Write([]byte(strings.Join(parts, "|")))
	hash := hex.EncodeToString(h.Sum(nil))[:16]
	return "oai-" + hash
}

// generateSessionID creates a new unique session ID
// generateSessionID 生成新的唯一 session ID
func (sm *SessionManager) generateSessionID() string {
	return "oai-" + uuid.New().String()[:8]
}

// getOrCreateSession gets an existing session or creates a new one
// getOrCreateSession 取已有 session，没有则新建
func (sm *SessionManager) getOrCreateSession(
	ctx context.Context,
	sessionID, userID, tenantID string,
) (shannonSID string, isNew bool, err error) {
	key := SessionPrefix + sessionID

	// Try to get existing
	// 先尝试取已有的
	info, err := sm.getSessionInfo(ctx, sessionID)
	if err != nil && err != redis.Nil {
		return "", false, err
	}

	if info != nil {
		return info.ShannonSID, false, nil
	}

	// Create new session - include full user ID for isolation even if orchestrator
	// doesn't strictly require it (defense in depth against session_id reuse after TTL)
	// 新建 session——即使 orchestrator 并不强制要求，也把完整 user ID 纳入以保持隔离
	// （纵深防御，防止 TTL 过期后 session_id 被复用）。
	shannonSID = fmt.Sprintf("shannon-%s-%s", userID, sessionID)
	info = &SessionInfo{
		SessionID:    sessionID,
		UserID:       userID,
		TenantID:     tenantID,
		ShannonSID:   shannonSID,
		CreatedAt:    time.Now(),
		LastUsedAt:   time.Now(),
		MessageCount: 0,
	}

	// Store in Redis
	// 存入 Redis
	data := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d",
		info.SessionID,
		info.UserID,
		info.TenantID,
		info.ShannonSID,
		info.CreatedAt.Unix(),
		info.LastUsedAt.Unix(),
		info.MessageCount,
	)

	if err := sm.redis.Set(ctx, key, data, sm.ttl).Err(); err != nil {
		return "", false, fmt.Errorf("failed to store session: %w", err)
	}

	sm.logger.Debug("Created new OpenAI session",
		zap.String("session_id", sessionID),
		zap.String("shannon_sid", shannonSID),
		zap.String("user_id", userID),
	)

	return shannonSID, true, nil
}

// getSessionInfo retrieves session info from Redis
// getSessionInfo 从 Redis 读取 session 信息
func (sm *SessionManager) getSessionInfo(ctx context.Context, sessionID string) (*SessionInfo, error) {
	key := SessionPrefix + sessionID
	data, err := sm.redis.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	// Parse stored data
	// 解析存储的数据
	parts := strings.Split(data, "|")
	if len(parts) < 7 {
		return nil, fmt.Errorf("invalid session data format")
	}

	var createdAt, lastUsedAt int64
	var messageCount int
	fmt.Sscanf(parts[4], "%d", &createdAt)
	fmt.Sscanf(parts[5], "%d", &lastUsedAt)
	fmt.Sscanf(parts[6], "%d", &messageCount)

	return &SessionInfo{
		SessionID:    parts[0],
		UserID:       parts[1],
		TenantID:     parts[2],
		ShannonSID:   parts[3],
		CreatedAt:    time.Unix(createdAt, 0),
		LastUsedAt:   time.Unix(lastUsedAt, 0),
		MessageCount: messageCount,
	}, nil
}

// doTouchSession updates the last used time and increments message count.
// Called by the background worker to avoid unbounded goroutines.
// doTouchSession 更新最后使用时间并累加消息数；由后台 worker 调用，避免 goroutine 数量失控。
func (sm *SessionManager) doTouchSession(ctx context.Context, sessionID string) {
	info, err := sm.getSessionInfo(ctx, sessionID)
	if err != nil {
		return
	}

	info.LastUsedAt = time.Now()
	info.MessageCount++

	key := SessionPrefix + sessionID
	data := fmt.Sprintf("%s|%s|%s|%s|%d|%d|%d",
		info.SessionID,
		info.UserID,
		info.TenantID,
		info.ShannonSID,
		info.CreatedAt.Unix(),
		info.LastUsedAt.Unix(),
		info.MessageCount,
	)

	sm.redis.Set(ctx, key, data, sm.ttl)
}

// IncrementMessageCount increments the message count for a session
// IncrementMessageCount 累加某个 session 的消息数
func (sm *SessionManager) IncrementMessageCount(ctx context.Context, sessionID string) {
	sm.enqueueTouchSession(sessionID)
}
