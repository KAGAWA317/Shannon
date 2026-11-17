package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"

	"time"
	"unicode/utf8"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
	"github.com/go-redis/redis/v8"
	"go.uber.org/zap"
)

const globalNotificationMaxLen = 10000

// Event is a minimal streaming event used by SSE and future gRPC.
// Event 是 SSE 与将来 gRPC 使用的最小 streaming event。
type Event struct {
	WorkflowID string         `json:"workflow_id"`
	Type       string         `json:"type"`
	AgentID    string         `json:"agent_id,omitempty"`
	Message    string         `json:"message,omitempty"`
	Payload    map[string]any `json:"payload,omitempty"`
	Timestamp  time.Time      `json:"timestamp"`
	Seq        uint64         `json:"seq"`
	StreamID   string         `json:"stream_id,omitempty"` // Redis stream ID for deduplication 注：用于去重的 Redis stream ID
}

// subscription tracks a subscriber with its cancellation mechanism
// subscription 记录一个订阅者及其取消机制
type subscription struct {
	cancel context.CancelFunc
}

// Manager provides Redis Streams-based pub/sub for workflow events.
//
// Lifecycle:
//  1. Subscribe() creates a channel and starts a background reader goroutine
//  2. The reader forwards Redis stream events to the channel
//  3. Unsubscribe() stops the reader and closes the channel
//
// IMPORTANT: Callers must NOT close subscription channels themselves.
// The reader owns the channel lifetime. Always call Unsubscribe() to clean up.
//
// Thread-safety: All methods are goroutine-safe.
// Manager 基于 Redis Streams 为 workflow event 提供 pub/sub。
// 生命周期：
// 1. Subscribe() 创建 channel 并启动后台 reader goroutine
// 2. reader 把 Redis stream event 转发到 channel
// 3. Unsubscribe() 停止 reader 并关闭 channel
// 重要：调用方不得自行关闭订阅 channel。
// channel 的生命周期由 reader 持有。清理时务必调用 Unsubscribe()。
// 线程安全：所有方法都是 goroutine 安全的。
type Manager struct {
	mu            sync.RWMutex
	redis         *redis.Client
	dbClient      *db.Client
	persistCh     chan db.EventLog
	persistClosed bool
	persistMu     sync.Mutex
	batchSize     int
	flushEvery    time.Duration
	subscribers   map[string]map[chan Event]*subscription
	capacity      int
	logger        *zap.Logger
	shutdownCh    chan struct{}
	wg            sync.WaitGroup
	persistWg     sync.WaitGroup
}

var (
	defaultMgr      *Manager
	once            sync.Once
	defaultCapacity = 256
)

// Get returns the global streaming manager, initializing it lazily.
// Get 返回全局 streaming manager，惰性初始化。
func Get() *Manager {
	once.Do(func() {
		// This will be properly initialized via InitializeRedis
		// 后续会通过 InitializeRedis 正确初始化
		defaultMgr = &Manager{
			subscribers: make(map[string]map[chan Event]*subscription),
			capacity:    defaultCapacity,
			logger:      zap.L(),
			shutdownCh:  make(chan struct{}),
		}
	})
	return defaultMgr
}

// InitializeRedis initializes the manager with a Redis client
// InitializeRedis 用 Redis client 初始化管理器
func InitializeRedis(redisClient *redis.Client, logger *zap.Logger) {
	if defaultMgr == nil {
		Get()
	}
	defaultMgr.mu.Lock()
	defer defaultMgr.mu.Unlock()
	defaultMgr.redis = redisClient
	if logger != nil {
		defaultMgr.logger = logger
	}
}

// InitializeEventStore sets the persistent store for events.
// InitializeEventStore 设置 event 的持久化存储。
func InitializeEventStore(store *db.Client, logger *zap.Logger) {
	if defaultMgr == nil {
		Get()
	}
	defaultMgr.mu.Lock()
	defer defaultMgr.mu.Unlock()
	defaultMgr.dbClient = store
	if logger != nil {
		defaultMgr.logger = logger
	}
	if defaultMgr.persistCh == nil {
		// Configure batching from env
		// 从环境变量配置 batching
		bs := 100
		if v := os.Getenv("EVENTLOG_BATCH_SIZE"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				bs = n
			}
		}
		iv := 100 * time.Millisecond
		if v := os.Getenv("EVENTLOG_BATCH_INTERVAL_MS"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 {
				iv = time.Duration(n) * time.Millisecond
			}
		}
		defaultMgr.persistCh = make(chan db.EventLog, bs*4)
		defaultMgr.batchSize = bs
		defaultMgr.flushEvery = iv
		defaultMgr.persistWg.Add(1)
		go defaultMgr.persistWorker()
		defaultMgr.logger.Info("Initialized event log batcher", zap.Int("batch_size", bs), zap.Duration("interval", iv))
	}
}

// Configure sets default capacity for new/empty managers and rings.
// Configure 为新/空管理器及 ring 设置默认容量。
func Configure(capacity int) {
	if capacity <= 0 {
		return
	}
	defaultCapacity = capacity
	if defaultMgr != nil {
		defaultMgr.mu.Lock()
		defaultMgr.capacity = capacity
		defaultMgr.mu.Unlock()
	}
}

// streamKey returns the Redis stream key for a workflow
// streamKey 返回 workflow 对应的 Redis stream 键
func (m *Manager) streamKey(workflowID string) string {
	return fmt.Sprintf("shannon:workflow:events:%s", workflowID)
}

// seqKey returns the Redis key for sequence counter
// seqKey 返回序列计数器的 Redis 键
func (m *Manager) seqKey(workflowID string) string {
	return fmt.Sprintf("shannon:workflow:events:%s:seq", workflowID)
}

// Subscribe adds a subscriber channel for a workflowID; caller must drain and call Unsubscribe.
// Subscribe 为某个 workflowID 添加订阅 channel；调用方必须读空并调用 Unsubscribe。
func (m *Manager) Subscribe(workflowID string, buffer int) chan Event {
	return m.SubscribeFrom(workflowID, buffer, "0-0")
}

// SubscribeFrom adds a subscriber starting from a specific stream ID
// SubscribeFrom 从指定 stream ID 开始添加订阅者
func (m *Manager) SubscribeFrom(workflowID string, buffer int, startID string) chan Event {
	ch := make(chan Event, buffer)

	// Create context with cancellation for this subscription
	// 为该订阅创建带取消的 context
	ctx, cancel := context.WithCancel(context.Background())

	m.mu.Lock()
	subs := m.subscribers[workflowID]
	if subs == nil {
		subs = make(map[chan Event]*subscription)
		m.subscribers[workflowID] = subs
	}
	subs[ch] = &subscription{cancel: cancel}
	m.mu.Unlock()

	// Start Redis stream reader goroutine with specific start position
	// 以指定起始位置启动 Redis stream reader goroutine
	m.wg.Add(1)
	go m.streamReaderFrom(ctx, workflowID, ch, startID)

	return ch
}

// streamReaderFrom reads from Redis stream starting from specific ID with context support
// streamReaderFrom 从指定 ID 开始读取 Redis stream，支持 context
func (m *Manager) streamReaderFrom(ctx context.Context, workflowID string, ch chan Event, startID string) {
	defer m.wg.Done()
	defer close(ch) // Always close channel when reader exits 注：reader 退出时始终关闭 channel

	if m.redis == nil {
		// In-memory mode: keep channel open until cancelled
		// 内存模式：保持 channel 打开直到被取消
		select {
		case <-ctx.Done():
		case <-m.shutdownCh:
		}
		return
	}

	streamKey := m.streamKey(workflowID)
	lastID := startID
	retryDelay := time.Second
	maxRetryDelay := 30 * time.Second

	m.logger.Debug("Starting stream reader",
		zap.String("workflow_id", workflowID),
		zap.String("stream_key", streamKey),
		zap.String("start_id", lastID))

	for {
		// Check for context cancellation or shutdown
		// 检查 context 是否取消或已关闭
		select {
		case <-ctx.Done():
			m.logger.Debug("Stream reader stopping - context cancelled",
				zap.String("workflow_id", workflowID))
			return
		case <-m.shutdownCh:
			m.logger.Debug("Stream reader stopping - manager shutdown",
				zap.String("workflow_id", workflowID))
			return
		default:
		}

		// Read from stream with blocking
		// 以阻塞方式从 stream 读取
		result, err := m.redis.XRead(ctx, &redis.XReadArgs{
			Streams: []string{streamKey, lastID},
			Count:   10,
			Block:   5 * time.Second,
		}).Result()

		if err == redis.Nil {
			// Timeout, no new messages - reset retry delay
			// timeout 且无新消息——重置 retry 延迟
			retryDelay = time.Second
			continue
		}

		if err != nil {
			// Check if context was cancelled
			// 检查 context 是否已取消
			if ctx.Err() != nil {
				return
			}

			m.logger.Error("Failed to read from Redis stream",
				zap.String("workflow_id", workflowID),
				zap.String("stream_key", streamKey),
				zap.String("last_id", lastID),
				zap.Duration("retry_in", retryDelay),
				zap.Error(err))

			// Exponential backoff on errors
			// 出错时指数退避
			select {
			case <-time.After(retryDelay):
				retryDelay = min(retryDelay*2, maxRetryDelay)
			case <-ctx.Done():
				return
			case <-m.shutdownCh:
				return
			}
			continue
		}

		// Success - reset retry delay
		// 成功——重置 retry 延迟
		retryDelay = time.Second

		// Process messages
		// 处理消息
		for _, stream := range result {
			for _, message := range stream.Messages {
				lastID = message.ID
				event := eventFromMessage(workflowID, message)

				// Best-effort DB persistence for events from external publishers (e.g., gateway).
				// Events published via Publish() are already enqueued; the DB dedup index prevents duplicates.
				// 对外部发布者（如 gateway）的 event 做尽力而为的 DB 持久化。
				// 通过 Publish() 发布的 event 已入队；DB 去重索引可防止重复。
				if shouldPersistEvent(event.Type) {
					el := db.EventLog{
						WorkflowID: event.WorkflowID,
						Type:       event.Type,
						AgentID:    event.AgentID,
						Message:    sanitizeEventMessage(event.Message),
						Timestamp:  event.Timestamp,
						Seq:        event.Seq,
						StreamID:   event.StreamID,
					}
					if event.Payload != nil {
						el.Payload = db.JSONB(sanitizeEventPayload(event.Payload))
					}
					m.enqueuePersistEvent(el)
				}

				// Send to channel (non-blocking to avoid deadlock)
				// 发送到 channel（非阻塞，避免死锁）
				select {
				case ch <- event:
					m.logger.Debug("Sent event to subscriber",
						zap.String("workflow_id", workflowID),
						zap.String("type", event.Type),
						zap.Uint64("seq", event.Seq),
						zap.String("stream_id", message.ID))
				default:
					// Escalate log severity for critical events
					// 关键 event 提升日志级别
					if isCriticalEvent(event.Type) {
						m.logger.Error("CRITICAL: Dropped important event - subscriber slow",
							zap.String("workflow_id", workflowID),
							zap.String("type", event.Type),
							zap.Uint64("seq", event.Seq))
					} else {
						m.logger.Warn("Dropped event - subscriber slow",
							zap.String("workflow_id", workflowID),
							zap.String("type", event.Type),
							zap.Uint64("seq", event.Seq))
					}
				}
			}
		}
	}
}

// streamSeq 解析 seq 字段；ok=false 表示字段缺失或非字符串/不可解析
func streamSeq(msg redis.XMessage) (uint64, bool) {
	v, ok := msg.Values["seq"].(string)
	if !ok {
		return 0, false
	}
	seq, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0, false
	}
	return seq, true
}

// eventFromMessage 把 Redis stream 消息还原成 Event（字段集与 gateway review.go 的 XAdd 一致）
func eventFromMessage(workflowID string, msg redis.XMessage) Event {
	event := Event{WorkflowID: workflowID, StreamID: msg.ID}
	if v, ok := msg.Values["type"].(string); ok {
		event.Type = v
	}
	if v, ok := msg.Values["agent_id"].(string); ok {
		event.AgentID = v
	}
	if v, ok := msg.Values["message"].(string); ok {
		event.Message = v
	}
	event.Seq, _ = streamSeq(msg)
	if v, ok := msg.Values["ts_nano"].(string); ok {
		if nano, err := strconv.ParseInt(v, 10, 64); err == nil {
			event.Timestamp = time.Unix(0, nano)
		}
	}
	if v, ok := msg.Values["payload"].(string); ok && v != "" {
		var p map[string]any
		if err := json.Unmarshal([]byte(v), &p); err == nil {
			event.Payload = p
		}
	}
	return event
}

// isCriticalEvent determines if an event type is critical and should not be dropped silently
// isCriticalEvent 判断 event 类型是否关键、不应被静默丢弃
func isCriticalEvent(eventType string) bool {
	switch eventType {
	case "WORKFLOW_FAILED",
		"WORKFLOW_COMPLETED",
		"AGENT_FAILED",
		"ERROR_OCCURRED",
		"TOOL_ERROR":
		return true
	default:
		return false
	}
}

// Unsubscribe removes the subscriber channel and cancels its reader goroutine.
// The channel will be closed by the reader goroutine after cancellation.
// Unsubscribe 移除订阅 channel 并取消其 reader goroutine。
// channel 会在取消后由 reader goroutine 关闭。
func (m *Manager) Unsubscribe(workflowID string, ch chan Event) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if subs, ok := m.subscribers[workflowID]; ok {
		if sub, exists := subs[ch]; exists {
			// Cancel the context to stop the reader goroutine
			// 取消 context 以停止 reader goroutine
			sub.cancel()
			delete(subs, ch)

			if len(subs) == 0 {
				delete(m.subscribers, workflowID)
			}
		}
	}
}

// Publish sends an event to Redis stream and all local subscribers (for backward compatibility)
// Publish 把 event 发送到 Redis stream 及所有本地订阅者（向后兼容）
func (m *Manager) Publish(workflowID string, evt Event) {
	if m.redis != nil {
		ctx := context.Background()

		// Increment sequence number
		// 递增序列号
		seq, err := m.redis.Incr(ctx, m.seqKey(workflowID)).Result()
		if err != nil {
			m.logger.Error("Failed to increment sequence",
				zap.String("workflow_id", workflowID),
				zap.Error(err))
			seq = 0
		}
		evt.Seq = uint64(seq)

		// Add to Redis stream
		// 写入 Redis stream
		streamKey := m.streamKey(workflowID)
		var payloadJSON string
		if evt.Payload != nil {
			if b, err := json.Marshal(evt.Payload); err == nil {
				payloadJSON = string(b)
			}
		}
		streamID, err := m.redis.XAdd(ctx, &redis.XAddArgs{
			Stream: streamKey,
			MaxLen: int64(m.capacity),
			Approx: true,
			Values: map[string]any{
				"workflow_id": evt.WorkflowID,
				"type":        evt.Type,
				"agent_id":    evt.AgentID,
				"message":     evt.Message,
				"payload":     payloadJSON,
				"ts_nano":     strconv.FormatInt(evt.Timestamp.UnixNano(), 10),
				"seq":         strconv.FormatUint(evt.Seq, 10),
			},
		}).Result()

		if err != nil {
			m.logger.Error("Failed to publish to Redis stream",
				zap.String("workflow_id", workflowID),
				zap.Error(err))
		} else {
			evt.StreamID = streamID // Store the Redis stream ID 注：保存 Redis stream ID
			m.logger.Debug("Published event to Redis stream",
				zap.String("workflow_id", workflowID),
				zap.String("type", evt.Type),
				zap.Uint64("seq", evt.Seq),
				zap.String("stream_id", streamID))
		}

		// Set TTL on stream key (24 hours)
		// Use longer TTL for sequence counter to prevent resets
		// 为 stream 键设置 TTL（24 小时）
		// 序列计数器使用更长的 TTL，防止被重置
		m.redis.Expire(ctx, streamKey, 24*time.Hour)
		m.redis.Expire(ctx, m.seqKey(workflowID), 48*time.Hour)

		// Publish to global notification stream for webhook delivery
		// Only for terminal workflow events (completion/failure)
		// 发布到全局通知 stream 以供 webhook 投递
		// 仅限终结类 workflow event（完成/失败）
		if isNotifiableEvent(evt.Type) {
			globalKey := "shannon:notifications:global"
			_, gErr := m.redis.XAdd(ctx, &redis.XAddArgs{
				Stream: globalKey,
				MaxLen: globalNotificationMaxLen,
				Approx: true,
				Values: map[string]any{
					"workflow_id": evt.WorkflowID,
					"type":        evt.Type,
					"agent_id":    evt.AgentID,
					"message":     evt.Message,
					"ts_nano":     strconv.FormatInt(evt.Timestamp.UnixNano(), 10),
				},
			}).Result()
			if gErr != nil {
				m.logger.Error("Failed to publish to global notification stream",
					zap.String("workflow_id", workflowID),
					zap.String("type", evt.Type),
					zap.Error(gErr))
			}
			m.redis.Expire(ctx, globalKey, 48*time.Hour)
		}
	}

	// Persist to DB if configured (best-effort, non-blocking)
	// Only persist important events, not streaming deltas
	// 已配置时持久化到 DB（尽力而为、非阻塞）
	// 仅持久化重要 event，不持久化 streaming 增量
	if shouldPersistEvent(evt.Type) {
		el := db.EventLog{
			WorkflowID: evt.WorkflowID,
			Type:       evt.Type,
			AgentID:    evt.AgentID,
			Message:    sanitizeUTF8(evt.Message),
			Timestamp:  evt.Timestamp,
			Seq:        evt.Seq,
			StreamID:   evt.StreamID,
		}
		if evt.Payload != nil {
			el.Payload = db.JSONB(sanitizePayloadForPersistence(evt.Type, evt.Payload))
		}
		m.enqueuePersistEvent(el)
	}

	// Only publish to local subscribers if Redis is nil (in-memory mode)
	// When Redis is available, the streamReader will deliver events
	// 仅在 Redis 为 nil（内存模式）时发布给本地订阅者
	// 有 Redis 时由 streamReader 投递 event
	if m.redis == nil {
		m.mu.RLock()
		defer m.mu.RUnlock()
		subs := m.subscribers[workflowID]
		if len(subs) == 0 {
			return
		}
		for ch := range subs {
			select {
			case ch <- evt:
			default:
				// Drop if subscriber is slow
				// 订阅者慢则丢弃
			}
		}
	}
}

// Marshal returns JSON for event payloads in SSE or logs.
// Marshal 返回 event 载荷的 JSON，用于 SSE 或日志。
func (e Event) Marshal() []byte {
	b, _ := json.Marshal(e)
	return b
}

// shouldPersistEvent determines if an event type should be persisted to PostgreSQL.
// We only persist important events, not streaming deltas, to reduce DB write load.
// shouldPersistEvent 判断 event 类型是否应持久化到 PostgreSQL。
// 只持久化重要 event 而非 streaming 增量，以降低 DB 写入压力。
func shouldPersistEvent(eventType string) bool {
	switch eventType {
	// ✅ Persist: Important workflow events
	// ✅ 持久化：重要的 workflow event
	case "WORKFLOW_COMPLETED",
		"WORKFLOW_FAILED",
		"AGENT_COMPLETED",
		"AGENT_FAILED",
		"TOOL_INVOKED",
		"TOOL_OBSERVATION",
		"TOOL_ERROR",
		"ERROR_OCCURRED",
		"LLM_OUTPUT",
		"STREAM_END",
		// Phase 2A: Multi-agent coordination events
		// 阶段 2A：多 agent 协作 event
		"ROLE_ASSIGNED",
		"DELEGATION",
		"BUDGET_THRESHOLD",
		"SCREENSHOT_SAVED":
		return true

	// ❌ Don't persist: Streaming deltas and heartbeats
	// ❌ 不持久化：streaming 增量与心跳
	case "LLM_PARTIAL", // thread.message.delta events 注：thread.message.delta 事件
		"HEARTBEAT",
		"PING",
		"LLM_PROMPT": // Prompts are logged separately 注：prompt 单独记录日志
		return false

	// ✅ Persist AGENT_THINKING so timeline is consistent between live and history
	// ✅ 持久化 AGENT_THINKING，使实时与历史时间线保持一致
	case "AGENT_THINKING":
		return true

	// Default: persist unknown event types (safe default)
	// 默认：持久化未知 event 类型（安全默认）
	default:
		return true
	}
}

// isNotifiableEvent returns true for events that should trigger webhook notifications.
// isNotifiableEvent 对应触发 webhook 通知的 event 返回 true。
func isNotifiableEvent(eventType string) bool {
	switch eventType {
	case "WORKFLOW_COMPLETED", "WORKFLOW_FAILED":
		return true
	default:
		return false
	}
}

// SanitizeBase64Image truncates large base64 image data in strings.
// SanitizeBase64Image 截断字符串中较大的 base64 图片数据。
func SanitizeBase64Image(s string) string {
	if s == "" {
		return s
	}

	// Common base64 image patterns from browser tools
	// 浏览器 tool 产生的常见 base64 图片模式
	patterns := []string{
		`"screenshot": "data:image/`,
		`"screenshot":"data:image/`,
		`"image": "data:image/`,
		`"image":"data:image/`,
		`"base64": "`,
		`"base64":"`,
	}

	result := s
	for _, pattern := range patterns {
		for {
			idx := strings.Index(result, pattern)
			if idx == -1 {
				break
			}

			// Find the start of the base64 data (after the pattern)
			// 定位 base64 数据的起始位置（模式之后）
			startIdx := idx + len(pattern)
			if startIdx >= len(result) {
				break
			}

			// Find the closing quote
			// 查找结束引号
			endIdx := strings.Index(result[startIdx:], `"`)
			if endIdx == -1 {
				break
			}

			dataLen := endIdx
			// Only truncate if the data is larger than a reasonable threshold (1KB)
			// 仅当数据大于合理阈值（1KB）时才截断
			if dataLen > 1024 {
				// Replace the base64 data with a placeholder
				// 用占位符替换 base64 数据
				placeholder := "[BASE64_IMAGE_TRUNCATED]"
				result = result[:startIdx] + placeholder + result[startIdx+endIdx:]
			} else {
				// Skip past this occurrence to prevent infinite loop
				// 跳过本次出现的位置，防止死循环
				break
			}
		}
	}

	return result
}

func sanitizeEventMessage(s string) string {
	s = sanitizeUTF8(s)
	s = SanitizeBase64Image(s)
	return s
}

// sanitizeEventPayload sanitizes payload map for storage.
// Truncates large base64 images in string values.
// sanitizeEventPayload 清洗待存储的 payload map。
// 截断字符串值中较大的 base64 图片。
func sanitizeEventPayload(payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}

	const maxDepth = 4
	var sanitizeValue func(key string, v any, depth int) any
	sanitizeValue = func(key string, v any, depth int) any {
		if v == nil || depth > maxDepth {
			return v
		}

		switch val := v.(type) {
		case string:
			// Handle raw base64 values directly (common for browser action=screenshot payloads).
			// 直接处理裸 base64 值（浏览器 action=screenshot 的载荷中很常见）。
			if (key == "screenshot" || key == "popup_screenshot") && len(val) > 1024 {
				return "[BASE64_IMAGE_TRUNCATED]"
			}
			return SanitizeBase64Image(val)
		case map[string]any:
			return sanitizeEventPayload(val)
		case []any:
			out := make([]any, 0, len(val))
			for _, item := range val {
				out = append(out, sanitizeValue("", item, depth+1))
			}
			return out
		default:
			return v
		}
	}

	sanitized := make(map[string]any, len(payload))
	for k, v := range payload {
		sanitized[k] = sanitizeValue(k, v, 0)
	}
	return sanitized
}

// sanitizePayloadForPersistence removes large data (e.g., base64 screenshots) from payloads
// before persisting to Postgres. The full payload is still available via Redis/SSE for real-time UI.
// sanitizePayloadForPersistence 在持久化到 Postgres 之前移除 payload 中的大块数据
// （如 base64 截图）。完整 payload 仍可通过 Redis/SSE 供实时 UI 使用。
func sanitizePayloadForPersistence(eventType string, payload map[string]any) map[string]any {
	if payload == nil {
		return nil
	}

	// Only TOOL_OBSERVATION with screenshot data needs sanitization
	// 只有带截图数据的 TOOL_OBSERVATION 需要清洗
	if eventType != "TOOL_OBSERVATION" {
		return payload
	}

	// Check if this is a browser screenshot tool result
	// 判断是否为浏览器截图 tool 的结果
	tool, hasT := payload["tool"].(string)
	output, hasO := payload["output"].(map[string]any)
	if !hasT || !hasO || tool != "browser" {
		return payload
	}
	// Only sanitize if output contains screenshot data
	// 仅当输出包含截图数据时才清洗
	if _, hasScreenshot := output["screenshot"]; !hasScreenshot {
		return payload
	}

	// Deep copy payload and strip screenshot base64
	// 深拷贝 payload 并去掉截图的 base64
	sanitized := make(map[string]any)
	for k, v := range payload {
		if k == "output" {
			// Create sanitized output without screenshot base64
			// 构建去掉截图 base64 的清洗后输出
			sanitizedOutput := make(map[string]any)
			for ok, ov := range output {
				if ok == "screenshot" {
					sanitizedOutput[ok] = "[BASE64_STRIPPED_FOR_PERSISTENCE]"
				} else {
					sanitizedOutput[ok] = ov
				}
			}
			sanitized[k] = sanitizedOutput
		} else {
			sanitized[k] = v
		}
	}
	return sanitized
}

// enqueuePersistEvent enqueues an event for DB persistence without blocking and without panicking on shutdown.
// The persistMu mutex ensures the closed check and send are atomic — no race with Shutdown.
// enqueuePersistEvent 把 event 入队待持久化，不阻塞，且关闭时不会 panic。
// persistMu 互斥锁保证关闭检查与发送的原子性——不会与 Shutdown 竞态。
func (m *Manager) enqueuePersistEvent(event db.EventLog) {
	m.persistMu.Lock()
	defer m.persistMu.Unlock()

	if m.dbClient == nil || m.persistCh == nil || m.persistClosed {
		return
	}

	select {
	case m.persistCh <- event:
	default:
		if isCriticalEvent(event.Type) {
			m.logger.Error("CRITICAL: eventlog batcher full; dropping important event",
				zap.String("workflow_id", event.WorkflowID),
				zap.String("type", event.Type))
		} else {
			m.logger.Warn("eventlog batcher full; dropping event",
				zap.String("workflow_id", event.WorkflowID),
				zap.String("type", event.Type))
		}
	}
}

// sanitizeUTF8 ensures invalid UTF-8 bytes are removed before persistence.
// sanitizeUTF8 在持久化前确保移除非法的 UTF-8 字节。
func sanitizeUTF8(s string) string {
	if s == "" || utf8.ValidString(s) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for len(s) > 0 {
		r, size := utf8.DecodeRuneInString(s)
		if r == utf8.RuneError && size == 1 {
			// Skip invalid byte; Postgres rejects malformed UTF-8.
			// 跳过非法字节；Postgres 会拒绝格式错误的 UTF-8。
			s = s[size:]
			continue
		}
		b.WriteRune(r)
		s = s[size:]
	}
	return b.String()
}

// persistWorker batches event logs and writes them asynchronously.
// persistWorker 批量汇总 event 日志并异步写入。
func (m *Manager) persistWorker() {
	defer m.persistWg.Done()
	batch := make([]db.EventLog, 0, m.batchSize)
	ticker := time.NewTicker(m.flushEvery)
	defer ticker.Stop()
	flush := func() {
		if len(batch) == 0 || m.dbClient == nil {
			return
		}
		// Write sequentially (simple, safe). Could be optimized to a batch insert if needed.
		// 顺序写入（简单、安全）。确有需要可优化为批量插入。
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for i := range batch {
			if err := m.dbClient.SaveEventLog(ctx, &batch[i]); err != nil {
				m.logger.Warn("SaveEventLog failed", zap.String("workflow_id", batch[i].WorkflowID), zap.String("type", batch[i].Type), zap.Uint64("seq", batch[i].Seq), zap.Error(err))
			}
		}
		cancel()
		batch = batch[:0]
	}
	for {
		select {
		case ev, ok := <-m.persistCh:
			if !ok {
				flush()
				return
			}
			batch = append(batch, ev)
			if len(batch) >= m.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// ReplaySince returns events with Seq > since (from Redis stream)
// ReplaySince 返回 Seq > since 的 event（来自 Redis stream）
func (m *Manager) ReplaySince(workflowID string, since uint64) []Event {
	if m.redis == nil {
		return nil
	}

	ctx := context.Background()
	streamKey := m.streamKey(workflowID)

	// Read all messages from the stream
	// 读取 stream 中的所有消息
	messages, err := m.redis.XRange(ctx, streamKey, "-", "+").Result()
	if err != nil {
		m.logger.Error("Failed to read replay from Redis stream",
			zap.String("workflow_id", workflowID),
			zap.Error(err))
		return nil
	}

	var events []Event
	for _, msg := range messages {
		// Skip if not after 'since'
		// 不在 'since' 之后则跳过
		if seq, ok := streamSeq(msg); ok && seq <= since {
			continue
		}

		events = append(events, eventFromMessage(workflowID, msg))
	}

	return events
}

// ReplayFromStreamID returns events starting from a specific Redis stream ID
// ReplayFromStreamID 返回从指定 Redis stream ID 开始的 event
func (m *Manager) ReplayFromStreamID(workflowID string, streamID string) []Event {
	if m.redis == nil {
		return nil
	}

	ctx := context.Background()
	streamKey := m.streamKey(workflowID)

	// Read messages after the given stream ID
	// 读取给定 stream ID 之后的消息
	messages, err := m.redis.XRange(ctx, streamKey, "("+streamID, "+").Result()
	if err != nil {
		m.logger.Error("Failed to read replay from Redis stream",
			zap.String("workflow_id", workflowID),
			zap.String("stream_id", streamID),
			zap.Error(err))
		return nil
	}

	var events []Event
	for _, msg := range messages {
		events = append(events, eventFromMessage(workflowID, msg))
	}

	return events
}

// HasEmittedCompletion checks if WORKFLOW_COMPLETED has been emitted for a workflow.
// This is a hint for visibility races (stream may show completion slightly before Temporal does).
// HasEmittedCompletion 检查某 workflow 是否已发出 WORKFLOW_COMPLETED。
// 这是针对可见性竞态的提示（stream 可能比 Temporal 略早显示完成）。
func (m *Manager) HasEmittedCompletion(ctx context.Context, workflowID string) bool {
	if m.redis == nil {
		return false
	}
	if ctx == nil {
		ctx = context.Background()
	}

	checkCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()

	streamKey := m.streamKey(workflowID)

	// Only scan the tail of the stream to keep this cheap.
	// 只扫描 stream 尾部，以保持开销很低。
	const scanCount int64 = 10
	messages, err := m.redis.XRevRangeN(checkCtx, streamKey, "+", "-", scanCount).Result()
	if err != nil {
		if err == redis.Nil || checkCtx.Err() != nil {
			return false
		}
		m.logger.Debug("Failed to check completion status in Redis",
			zap.String("workflow_id", workflowID),
			zap.Error(err))
		return false
	}

	for _, msg := range messages {
		if eventType, ok := msg.Values["type"].(string); ok && eventType == "WORKFLOW_COMPLETED" {
			return true
		}
	}

	return false
}

// GetLastStreamID returns the ID of the last message in the stream
// GetLastStreamID 返回 stream 中最后一条消息的 ID
func (m *Manager) GetLastStreamID(workflowID string) string {
	if m.redis == nil {
		return ""
	}

	ctx := context.Background()
	streamKey := m.streamKey(workflowID)

	// Get only the last message efficiently with XRevRangeN
	// 用 XRevRangeN 高效地只取最后一条消息
	messages, err := m.redis.XRevRangeN(ctx, streamKey, "+", "-", 1).Result()
	if err != nil || len(messages) == 0 {
		return ""
	}

	return messages[0].ID
}

// Shutdown gracefully shuts down the manager, stopping all stream readers and flushing persistence.
// It waits for all goroutines to complete with the provided context timeout.
// Shutdown 优雅关闭管理器，停止所有 stream reader 并刷出持久化队列。
// 它按传入的 context timeout 等待所有 goroutine 结束。
func (m *Manager) Shutdown(ctx context.Context) error {
	m.logger.Info("Shutting down streaming manager")

	// Signal shutdown to all stream readers
	// 向所有 stream reader 发出关闭信号
	close(m.shutdownCh)

	// Cancel all subscriptions
	// 取消所有订阅
	m.mu.Lock()
	for workflowID, subs := range m.subscribers {
		for ch, sub := range subs {
			sub.cancel()
			delete(subs, ch)
		}
		delete(m.subscribers, workflowID)
	}
	m.mu.Unlock()

	// Wait for all stream readers to exit
	// 等待所有 stream reader 退出
	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		m.logger.Info("All stream readers stopped")
	case <-ctx.Done():
		m.logger.Warn("Shutdown timeout waiting for stream readers")
		return ctx.Err()
	}

	// Close persistence channel and wait for flush
	// 关闭持久化 channel 并等待刷出
	if m.persistCh != nil {
		m.persistMu.Lock()
		if !m.persistClosed {
			m.persistClosed = true
			close(m.persistCh)
		}
		m.persistMu.Unlock()

		// Wait for persist worker to exit
		// 等待 persist worker 退出
		persistDone := make(chan struct{})
		go func() {
			m.persistWg.Wait()
			close(persistDone)
		}()

		select {
		case <-persistDone:
			m.logger.Info("Event persistence flushed")
		case <-ctx.Done():
			m.logger.Warn("Shutdown timeout waiting for persistence flush")
			return ctx.Err()
		}
	}

	m.logger.Info("Streaming manager shutdown complete")
	return nil
}

// Blob storage for large payloads (screenshots, etc.) that exceed Temporal limits
// 用于超大载荷（截图等）的 blob 存储，这些载荷会超出 Temporal 限制

const (
	// blobKeyPrefix is the Redis key prefix for stored blobs
	// blobKeyPrefix 是已存 blob 的 Redis 键前缀
	blobKeyPrefix = "shannon:blob:"
	// blobTTL is how long blobs are kept in Redis (7 days)
	// blobTTL 是 blob 在 Redis 中的保留时长（7 天）
	blobTTL = 7 * 24 * time.Hour
)

// StoreBlob stores a large blob in Redis and returns a reference key.
// The blob is stored with a TTL and can be retrieved via GetBlob.
// StoreBlob 把大块 blob 存入 Redis 并返回引用键。
// blob 带 TTL 存储，可通过 GetBlob 取回。
func (m *Manager) StoreBlob(ctx context.Context, workflowID, fieldName, data string) (string, error) {
	if m.redis == nil {
		return "", fmt.Errorf("redis not configured")
	}

	// Generate a unique key for this blob
	// 为该 blob 生成唯一键
	key := fmt.Sprintf("%s%s:%s", blobKeyPrefix, workflowID, fieldName)

	err := m.redis.Set(ctx, key, data, blobTTL).Err()
	if err != nil {
		m.logger.Error("Failed to store blob in Redis",
			zap.String("key", key),
			zap.Int("size", len(data)),
			zap.Error(err))
		return "", err
	}

	m.logger.Debug("Stored blob in Redis",
		zap.String("key", key),
		zap.Int("size", len(data)),
		zap.Duration("ttl", blobTTL))

	return key, nil
}

// GetBlob retrieves a blob from Redis by its key.
// Returns empty string if not found or expired.
// GetBlob 按 key 从 Redis 取回 blob。
// 未找到或已过期时返回空字符串。
func (m *Manager) GetBlob(ctx context.Context, key string) (string, error) {
	if m.redis == nil {
		return "", fmt.Errorf("redis not configured")
	}

	data, err := m.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil // Not found or expired 注：未找到或已过期
	}
	if err != nil {
		m.logger.Error("Failed to get blob from Redis",
			zap.String("key", key),
			zap.Error(err))
		return "", err
	}

	return data, nil
}

// RefreshBlobTTL extends the TTL of a blob key.
// Useful when a blob is still being accessed.
// RefreshBlobTTL 延长 blob 键的 TTL。
// 适用于 blob 仍被访问的场景。
func (m *Manager) RefreshBlobTTL(ctx context.Context, key string) error {
	if m.redis == nil {
		return fmt.Errorf("redis not configured")
	}

	return m.redis.Expire(ctx, key, blobTTL).Err()
}
