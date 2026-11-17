package session

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
	"go.uber.org/zap"

	auth "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/circuitbreaker"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
)

// Manager handles session management with Redis backend
// Manager 使用 Redis 后端处理 session 管理
type Manager struct {
	client      *circuitbreaker.RedisWrapper
	logger      *zap.Logger
	ttl         time.Duration
	maxHistory  int // Maximum messages to keep per session 注：每个 session 保留的最大消息数
	mu          sync.RWMutex
	localCache  map[string]*Session  // Local cache for performance 注：为提升性能的本地 cache
	cacheAccess map[string]time.Time // Track last access time for LRU 注：记录最后访问时间，用于 LRU
	maxSessions int
}

// ManagerConfig contains configuration for session manager
// ManagerConfig 包含 session 管理器的配置
type ManagerConfig struct {
	MaxHistory int           // Maximum messages to keep per session (default: 500) 注：每个 session 保留的最大消息数（默认 500）
	// 每个 session 保留的最大消息数（默认 500）
	TTL        time.Duration // Session expiry time (default: 30 days) 注：session 过期时间（默认 30 天）
	CacheSize  int           // Max sessions to keep in local cache (default: 10000) 注：本地 cache 保留的最大 session 数（默认 10000）
	// 本地 cache 保留的最大 session 数（默认 10000）
}

// NewManager creates a new session manager
// NewManager 创建新的 session 管理器
func NewManager(redisAddr string, logger *zap.Logger) (*Manager, error) {
	return NewManagerWithConfig(redisAddr, logger, nil)
}

// NewManagerWithConfig creates a new session manager with specific config
// NewManagerWithConfig 使用指定配置创建 session 管理器
func NewManagerWithConfig(redisAddr string, logger *zap.Logger, config *ManagerConfig) (*Manager, error) {
	// Get Redis password from environment variable
	// 从环境变量获取 Redis 密码
	redisPassword := os.Getenv("REDIS_PASSWORD")

	redisClient := redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		Password:     redisPassword, // Use environment variable 注：使用环境变量
		DB:           0,             // Default DB 注：默认 DB
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})

	// Create circuit breaker wrapped client
	// 创建带 circuit breaker 包装的 client
	client := circuitbreaker.NewRedisWrapper(redisClient, logger)

	// Test connection
	// 测试连接
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	// Apply defaults if no config provided
	// 未提供配置时应用默认值
	maxHistory := 500
	ttl := 720 * time.Hour // 30 days default 注：默认 30 天
	cacheSize := 10000

	if config != nil {
		if config.MaxHistory > 0 {
			maxHistory = config.MaxHistory
		}
		if config.TTL > 0 {
			ttl = config.TTL
		}
		if config.CacheSize > 0 {
			cacheSize = config.CacheSize
		}
	}

	return &Manager{
		client:      client,
		logger:      logger,
		ttl:         ttl,
		maxHistory:  maxHistory,
		localCache:  make(map[string]*Session),
		cacheAccess: make(map[string]time.Time),
		maxSessions: cacheSize,
	}, nil
}

// CreateSession creates a new session
// CreateSession 创建新 session
func (m *Manager) CreateSession(ctx context.Context, userID string, tenantID string, metadata map[string]any) (*Session, error) {
	sessionID := uuid.New().String()

	session := &Session{
		ID:        sessionID,
		UserID:    userID,
		TenantID:  tenantID,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		ExpiresAt: time.Now().Add(m.ttl),
		Metadata:  metadata,
		Context:   make(map[string]any),
		History:   make([]Message, 0),
	}

	// Store in Redis
	// 存入 Redis
	if err := m.saveSession(ctx, session); err != nil {
		return nil, fmt.Errorf("failed to save session: %w", err)
	}

	// Cache locally
	// 写入本地 cache
	m.mu.Lock()
	m.localCache[sessionID] = session
	m.cleanupLocalCache()
	metrics.SessionCacheSize.Set(float64(len(m.localCache)))
	m.mu.Unlock()

	m.logger.Info("Created new session",
		zap.String("session_id", sessionID),
		zap.String("user_id", userID),
	)
	metrics.SessionsCreated.Inc()

	return session, nil
}

// CreateSessionWithID creates a new session with a specific ID
// CreateSessionWithID 用指定 ID 创建新 session
func (m *Manager) CreateSessionWithID(ctx context.Context, sessionID string, userID string, tenantID string, metadata map[string]any) (*Session, error) {
	// IMPORTANT: Check if session already exists to prevent hijacking
	// 重要：先检查 session 是否已存在，防止 session 劫持
	existing, _ := m.GetSession(ctx, sessionID)
	if existing != nil {
		if existing.UserID != userID {
			// Session exists but belongs to different user - security violation
			// Generate a new session ID instead
			// session 已存在但属于其他用户——安全违规
			// 改为生成新的 session ID
			m.logger.Warn("Attempted to reuse session ID from different user, generating new ID",
				zap.String("requested_session_id", sessionID),
				zap.String("requesting_user", userID),
				zap.String("existing_owner", existing.UserID),
			)
			// Create new session with generated ID
			// 用生成的 ID 创建新 session
			return m.CreateSession(ctx, userID, tenantID, metadata)
		}
		// Session exists and belongs to same user - return existing
		// session 已存在且属于同一用户——返回现有 session
		return existing, nil
	}

	session := &Session{
		ID:        sessionID,
		UserID:    userID,
		TenantID:  tenantID,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
		ExpiresAt: time.Now().Add(m.ttl),
		Metadata:  metadata,
		Context:   make(map[string]any),
		History:   make([]Message, 0),
	}

	// Store in Redis
	// 存入 Redis
	if err := m.saveSession(ctx, session); err != nil {
		return nil, fmt.Errorf("failed to save session: %w", err)
	}

	// Cache locally
	// 写入本地 cache
	m.mu.Lock()
	m.localCache[sessionID] = session
	m.cleanupLocalCache()
	metrics.SessionCacheSize.Set(float64(len(m.localCache)))
	m.mu.Unlock()

	m.logger.Info("Created new session with specific ID",
		zap.String("session_id", sessionID),
		zap.String("user_id", userID),
	)
	metrics.SessionsCreated.Inc()
	return session, nil
}

// GetSession retrieves a session by ID
// GetSession 按 ID 获取 session
func (m *Manager) GetSession(ctx context.Context, sessionID string) (*Session, error) {
	// Check local cache first
	// 先查本地 cache
	m.mu.RLock()
	if session, ok := m.localCache[sessionID]; ok {
		m.mu.RUnlock()
		metrics.SessionCacheHits.Inc()
		if session.IsExpired() {
			m.DeleteSession(ctx, sessionID)
			return nil, ErrSessionExpired
		}
		// Update access time for LRU
		// 更新访问时间，用于 LRU
		m.mu.Lock()
		m.cacheAccess[sessionID] = time.Now()
		m.mu.Unlock()
		return session, nil
	}
	m.mu.RUnlock()
	metrics.SessionCacheMisses.Inc()

	// Load from Redis
	// 从 Redis 加载
	key := m.sessionKey(sessionID)
	data, err := m.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, ErrSessionNotFound
	} else if err != nil {
		return nil, fmt.Errorf("failed to get session: %w", err)
	}

	var session Session
	if err := json.Unmarshal(data, &session); err != nil {
		return nil, fmt.Errorf("failed to unmarshal session: %w", err)
	}

	// Check expiration
	// 检查是否过期
	if session.IsExpired() {
		m.DeleteSession(ctx, sessionID)
		return nil, ErrSessionExpired
	}

	// Enforce tenant isolation if auth context is present
	// 有 auth context 时强制租户隔离
	if userCtx, err := authFromContext(ctx); err == nil && userCtx.TenantID != "" {
		if session.TenantID != "" && session.TenantID != userCtx.TenantID {
			// Do not leak existence
			// 不泄露是否存在
			return nil, ErrSessionNotFound
		}
	}

	// Update local cache and track access time
	// 更新本地 cache 并记录访问时间
	m.mu.Lock()
	m.localCache[sessionID] = &session
	m.cacheAccess[sessionID] = time.Now()
	m.cleanupLocalCache()
	metrics.SessionCacheSize.Set(float64(len(m.localCache)))
	m.mu.Unlock()

	return &session, nil
}

// UpdateSession updates an existing session
// UpdateSession 更新已有 session
func (m *Manager) UpdateSession(ctx context.Context, session *Session) error {
	if session == nil {
		return fmt.Errorf("session is nil")
	}

	session.UpdatedAt = time.Now()

	// Save to Redis
	// 保存到 Redis
	if err := m.saveSession(ctx, session); err != nil {
		return fmt.Errorf("failed to save session: %w", err)
	}

	// Update local cache
	// 更新本地 cache
	m.mu.Lock()
	m.localCache[session.ID] = session
	m.mu.Unlock()

	return nil
}

// DeleteSession deletes a session
// DeleteSession 删除 session
func (m *Manager) DeleteSession(ctx context.Context, sessionID string) error {
	// Remove from Redis
	// 从 Redis 删除
	key := m.sessionKey(sessionID)
	if err := m.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete session: %w", err)
	}

	// Remove from local cache
	// 从本地 cache 删除
	m.mu.Lock()
	delete(m.localCache, sessionID)
	// Update cache size metric while holding the lock to avoid races
	// 持锁时更新 cache 大小指标，避免竞态
	metrics.SessionCacheSize.Set(float64(len(m.localCache)))
	m.mu.Unlock()

	m.logger.Info("Deleted session", zap.String("session_id", sessionID))
	return nil
}

// ExtendSession extends the TTL of a session
// ExtendSession 延长 session 的 TTL
func (m *Manager) ExtendSession(ctx context.Context, sessionID string, duration time.Duration) error {
	session, err := m.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}

	session.ExpiresAt = time.Now().Add(duration)
	return m.UpdateSession(ctx, session)
}

// AddMessage adds a message to session history
// AddMessage 把消息加入 session 历史
func (m *Manager) AddMessage(ctx context.Context, sessionID string, msg Message) error {
	session, err := m.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}

	session.History = append(session.History, msg)

	// Limit history size
	// 限制历史长度
	if len(session.History) > m.maxHistory {
		session.History = session.History[len(session.History)-m.maxHistory:]
	}

	return m.UpdateSession(ctx, session)
}

// UpdateContext updates session context
// UpdateContext 更新 session context
func (m *Manager) UpdateContext(ctx context.Context, sessionID string, key string, value any) error {
	session, err := m.GetSession(ctx, sessionID)
	if err != nil {
		return err
	}

	if session.Context == nil {
		session.Context = make(map[string]any)
	}
	session.Context[key] = value

	return m.UpdateSession(ctx, session)
}

// GetUserSessions gets all sessions for a user
// GetUserSessions 获取用户的全部 session
func (m *Manager) GetUserSessions(ctx context.Context, userID string) ([]*Session, error) {
	pattern := "session:*"
	keys, err := m.client.Keys(ctx, pattern).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to list sessions: %w", err)
	}

	var sessions []*Session
	for _, key := range keys {
		data, err := m.client.Get(ctx, key).Bytes()
		if err != nil {
			continue // Skip failed sessions 注：跳过失败的 session
		}

		var session Session
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}

		// Enforce tenant isolation if available
		// 若可用则强制租户隔离
		if userCtx, err := authFromContext(ctx); err == nil && userCtx.TenantID != "" {
			if session.TenantID != "" && session.TenantID != userCtx.TenantID {
				continue
			}
		}

		if session.UserID == userID && !session.IsExpired() {
			sessions = append(sessions, &session)
		}
	}

	return sessions, nil
}

// CleanupExpired removes expired sessions
// CleanupExpired 移除已过期的 session
func (m *Manager) CleanupExpired(ctx context.Context) (int, error) {
	pattern := "session:*"
	keys, err := m.client.Keys(ctx, pattern).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to list sessions: %w", err)
	}

	cleaned := 0
	for _, key := range keys {
		data, err := m.client.Get(ctx, key).Bytes()
		if err != nil {
			continue
		}

		var session Session
		if err := json.Unmarshal(data, &session); err != nil {
			continue
		}

		if session.IsExpired() {
			if err := m.client.Del(ctx, key).Err(); err == nil {
				cleaned++
			}
		}
	}

	m.logger.Info("Cleaned up expired sessions", zap.Int("count", cleaned))
	return cleaned, nil
}

// Private methods
// 私有方法

func (m *Manager) sessionKey(sessionID string) string {
	return fmt.Sprintf("session:%s", sessionID)
}

func (m *Manager) saveSession(ctx context.Context, session *Session) error {
	data, err := json.Marshal(session)
	if err != nil {
		return fmt.Errorf("failed to marshal session: %w", err)
	}

	key := m.sessionKey(session.ID)
	ttl := time.Until(session.ExpiresAt)
	if ttl <= 0 {
		ttl = m.ttl
	}

	return m.client.Set(ctx, key, data, ttl).Err()
}

func (m *Manager) cleanupLocalCache() {
	// Remove oldest entries if cache is too large using LRU
	// cache 过大时按 LRU 移除最旧的条目
	if len(m.localCache) > m.maxSessions {
		// Find the oldest accessed sessions
		// 找出最久未访问的 session
		type accessEntry struct {
			id   string
			time time.Time
		}

		entries := make([]accessEntry, 0, len(m.localCache))
		for id := range m.localCache {
			accessTime, exists := m.cacheAccess[id]
			if !exists {
				// If no access time tracked, consider it very old
				// 未记录访问时间的条目按很久未访问处理
				accessTime = time.Time{}
			}
			entries = append(entries, accessEntry{id: id, time: accessTime})
		}

		// Sort by access time (oldest first)
		// 按访问时间排序（最旧在前）
		for i := 0; i < len(entries)-1; i++ {
			for j := i + 1; j < len(entries); j++ {
				if entries[j].time.Before(entries[i].time) {
					entries[i], entries[j] = entries[j], entries[i]
				}
			}
		}

		// Remove the oldest half
		// 移除最旧的一半
		toRemove := m.maxSessions / 2
		for i := 0; i < toRemove && i < len(entries); i++ {
			delete(m.localCache, entries[i].id)
			delete(m.cacheAccess, entries[i].id)
			metrics.SessionCacheEvictions.Inc()
		}
	}
}

// Close closes the session manager
// Close 关闭 session 管理器
func (m *Manager) Close() error {
	return m.client.Close()
}

// RedisWrapper returns the underlying Redis circuit breaker wrapper for health checks and monitoring
// RedisWrapper 返回底层 Redis circuit breaker 包装，用于健康检查与监控
func (m *Manager) RedisWrapper() *circuitbreaker.RedisWrapper {
	return m.client
}

// authFromContext extracts a minimal subset of auth context without hard coupling
// to the entire auth package surface in this file.
// authFromContext 只提取 auth context 的最小子集，避免本文件
// 与整个 auth 包表面强耦合。
type minimalUserCtx struct {
	TenantID string
}

func authFromContext(ctx context.Context) (*minimalUserCtx, error) {
	if uc, err := auth.GetUserContext(ctx); err == nil && uc != nil {
		return &minimalUserCtx{TenantID: uc.TenantID.String()}, nil
	}
	return nil, fmt.Errorf("no auth context")
}
