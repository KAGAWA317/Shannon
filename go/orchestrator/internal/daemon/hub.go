package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

var ErrNoDaemonConnected = errors.New("no daemon connected")

// DaemonConn represents a single connected daemon.
// DaemonConn 表示单个已连接的 daemon。
type DaemonConn struct {
	ID          string
	TenantID    string
	UserID      string
	ConnectedAt time.Time
	LastActive  time.Time

	sendFn func(ServerMessage) error
	mu     sync.Mutex
}

func (dc *DaemonConn) Send(msg ServerMessage) error {
	dc.mu.Lock()
	defer dc.mu.Unlock()
	dc.LastActive = time.Now()
	return dc.sendFn(msg)
}

// WorkflowStartedCallback is called when a progress message carries a workflow_id.
// Used to start streaming for the associated channel.
// 进度消息携带 workflow_id 时调用 WorkflowStartedCallback。
// 用于为对应渠道启动 streaming 输出。
type WorkflowStartedCallback func(ctx context.Context, messageID string, meta ClaimMetadata)

// Hub manages all connected daemon WebSocket connections.
// Hub 管理所有已连接的 daemon WebSocket 连接。
type Hub struct {
	mu          sync.RWMutex
	conns       map[string]*DaemonConn         // conn_id -> connection 注：conn_id -> 连接对象
	index       map[string]map[string]struct{} // "tenant:user" -> set of conn_ids 注："tenant:user" -> conn_id 集合
	sticky      map[string]string              // "channel_type:thread_id" -> conn_id 注："channel_type:thread_id" -> conn_id
	pendingMeta    map[string]ClaimMetadata // messageID -> metadata template (set by Dispatch, consumed by HandleClaim) 注：messageID -> metadata 模板（由 Dispatch 设置，由 HandleClaim 消费）
	pendingTimers  map[string]*time.Timer  // messageID -> cleanup timer 注：messageID -> 清理 timer

	claims *ClaimManager
	logger *zap.Logger

	// OnWorkflowStarted is called when a daemon progress message carries a workflow_id.
	// daemon 进度消息携带 workflow_id 时调用 OnWorkflowStarted。
	OnWorkflowStarted WorkflowStartedCallback
}

func NewHub(rc redis.Cmdable, logger *zap.Logger) *Hub {
	return &Hub{
		conns:         make(map[string]*DaemonConn),
		index:         make(map[string]map[string]struct{}),
		sticky:        make(map[string]string),
		pendingMeta:   make(map[string]ClaimMetadata),
		pendingTimers: make(map[string]*time.Timer),
		claims:        NewClaimManager(rc),
		logger:        logger,
	}
}

const pendingMetaTTL = 90 * time.Second // must exceed claim timeout + processing start 注：必须大于 claim timeout 加上开始处理的时间

func indexKey(tenantID, userID string) string {
	return tenantID + ":" + userID
}

func stickyKey(channelType, threadID string) string {
	return channelType + ":" + threadID
}

// Register adds a daemon connection to the hub.
// Register 把 daemon 连接加入 hub。
func (h *Hub) Register(conn *DaemonConn) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.conns[conn.ID] = conn

	ik := indexKey(conn.TenantID, conn.UserID)
	if h.index[ik] == nil {
		h.index[ik] = make(map[string]struct{})
	}
	h.index[ik][conn.ID] = struct{}{}

	ConnectionsActive.Inc()

	h.logger.Info("daemon connected",
		zap.String("conn_id", conn.ID),
		zap.String("tenant_id", conn.TenantID),
		zap.String("user_id", conn.UserID),
	)
}

// Unregister removes a daemon connection from the hub.
// Unregister 从 hub 移除 daemon 连接。
func (h *Hub) Unregister(connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()

	conn, ok := h.conns[connID]
	if !ok {
		return
	}

	delete(h.conns, connID)

	ik := indexKey(conn.TenantID, conn.UserID)
	if set, exists := h.index[ik]; exists {
		delete(set, connID)
		if len(set) == 0 {
			delete(h.index, ik)
		}
	}

	h.clearStickyForConnLocked(connID)

	ConnectionsActive.Dec()

	h.logger.Info("daemon disconnected",
		zap.String("conn_id", connID),
		zap.String("tenant_id", conn.TenantID),
		zap.String("user_id", conn.UserID),
	)
}

// GetConnections returns all connections for a tenant:user pair.
// GetConnections 返回某个 tenant:user 组合的全部连接。
func (h *Hub) GetConnections(tenantID, userID string) []*DaemonConn {
	h.mu.RLock()
	defer h.mu.RUnlock()

	ik := indexKey(tenantID, userID)
	set := h.index[ik]
	if len(set) == 0 {
		return nil
	}

	conns := make([]*DaemonConn, 0, len(set))
	for id := range set {
		if c, ok := h.conns[id]; ok {
			conns = append(conns, c)
		}
	}
	return conns
}

// GetConn returns a single connection by ID.
// GetConn 按 ID 返回单个连接。
func (h *Hub) GetConn(connID string) (*DaemonConn, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	c, ok := h.conns[connID]
	return c, ok
}

// SetSticky sets sticky routing for a channel:thread to a specific connection.
// SetSticky 把 channel:thread 的粘性路由指向某个连接。
func (h *Hub) SetSticky(channelType, threadID, connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.sticky[stickyKey(channelType, threadID)] = connID
}

// GetSticky returns the sticky connection for a channel:thread, if any.
// GetSticky 返回 channel:thread 的粘性连接（若有）。
func (h *Hub) GetSticky(channelType, threadID string) (string, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	id, ok := h.sticky[stickyKey(channelType, threadID)]
	return id, ok
}

// ClearStickyForConn removes all sticky entries pointing to a connection.
// ClearStickyForConn 移除指向某连接的全部粘性条目。
func (h *Hub) ClearStickyForConn(connID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.clearStickyForConnLocked(connID)
}

// clearStickyForConnLocked removes sticky entries; caller must hold h.mu.
// clearStickyForConnLocked 移除粘性条目；调用方必须持有 h.mu。
func (h *Hub) clearStickyForConnLocked(connID string) {
	for k, v := range h.sticky {
		if v == connID {
			delete(h.sticky, k)
		}
	}
}

// Dispatch sends a message to the appropriate daemon connection(s).
// For system channels, it broadcasts to all connections without expecting a claim.
// For normal channels, it tries sticky routing first, then broadcasts.
// The meta parameter carries channel routing metadata that will be stored for
// claim resolution — when a daemon claims this message, the pending metadata
// is merged with the claim so replies can be routed back to the originating channel.
// Dispatch 把消息发送到合适的 daemon 连接。
// 系统渠道会广播给所有连接且不期待 claim。
// 普通渠道先尝试粘性路由，再广播。
// meta 参数携带渠道路由 metadata，会被保存用于 claim 解析——当 daemon 占用该消息时，
// 待处理 metadata 与 claim 合并，使回复能路由回原渠道。
func (h *Hub) Dispatch(ctx context.Context, tenantID, userID string, payload MessagePayload, meta ClaimMetadata) error {
	messageID := uuid.New().String()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	msg := ServerMessage{
		Type:      MsgTypeMessage,
		MessageID: messageID,
		Payload:   payloadBytes,
	}

	isSystem := IsSystemChannel(payload.Channel)
	if isSystem {
		msg.Type = MsgTypeSystem
	}

	// Collect targets under lock, then send outside to avoid holding the write lock
	// during potentially slow WebSocket writes (conn.Send has a 10s write deadline).
	// 在锁内收集目标，然后在锁外发送，避免在可能较慢的 WebSocket 写入期间持有写锁
	// （conn.Send 有 10s 写 timeout）。
	h.mu.Lock()

	// Try sticky routing for non-system channels with a thread.
	// 对带 thread 的非系统渠道尝试粘性路由。
	var stickyConn *DaemonConn
	if !isSystem && payload.ThreadID != "" {
		sk := stickyKey(payload.Channel, payload.ThreadID)
		if connID, ok := h.sticky[sk]; ok {
			if conn, exists := h.conns[connID]; exists {
				stickyConn = conn
				h.storePendingMeta(messageID, meta)
				h.logger.Debug("sticky dispatch",
					zap.String("message_id", messageID),
					zap.String("conn_id", connID),
				)
			} else {
				delete(h.sticky, sk)
				h.logger.Debug("cleared stale sticky entry",
					zap.String("old_conn_id", connID),
				)
			}
		}
	}

	// If sticky hit, release lock and send.
	// 粘性命中则释放锁后发送。
	if stickyConn != nil {
		h.mu.Unlock()
		return stickyConn.Send(msg)
	}

	// Broadcast: collect target connections.
	// Broadcast：收集目标连接。
	ik := indexKey(tenantID, userID)
	set := h.index[ik]
	if len(set) == 0 {
		h.mu.Unlock()
		ClaimsTotal.WithLabelValues(payload.Channel, "no_daemon").Inc()
		return ErrNoDaemonConnected
	}

	if !isSystem {
		h.storePendingMeta(messageID, meta)
	}

	targets := make([]*DaemonConn, 0, len(set))
	for id := range set {
		if conn, ok := h.conns[id]; ok {
			targets = append(targets, conn)
		}
	}
	h.mu.Unlock()

	// Send outside lock — WebSocket writes can be slow.
	// Return nil if at least one daemon received the message (partial success is OK for broadcast).
	// 在锁外发送——WebSocket 写入可能较慢。
	// 只要至少一个 daemon 收到消息就返回 nil（广播允许部分成功）。
	var sent int
	for _, conn := range targets {
		if err := conn.Send(msg); err != nil {
			h.logger.Warn("dispatch send failed",
				zap.String("message_id", messageID),
				zap.String("conn_id", conn.ID),
				zap.Error(err),
			)
		} else {
			sent++
		}
	}
	if sent == 0 {
		return ErrNoDaemonConnected
	}

	return nil
}

// HandleClaim processes a daemon's claim on a message. Returns true if granted.
// If pending metadata was stored by Dispatch, it is merged with the incoming meta
// so that channel routing information (channel_id, channel_type, thread_id, etc.)
// is preserved in the claim for reply routing.
// storePendingMeta stores metadata and schedules automatic cleanup.
// Caller must hold h.mu.
// HandleClaim 处理 daemon 对某消息的占用，占用成功返回 true。
// 若 Dispatch 已保存待处理 metadata，会与传入的 meta 合并，
// 从而在 claim 中保留渠道路由信息（channel_id、channel_type、thread_id 等）供回复路由使用。
// storePendingMeta 保存 metadata 并安排自动清理。
// 调用方必须持有 h.mu。
func (h *Hub) storePendingMeta(messageID string, meta ClaimMetadata) {
	h.pendingMeta[messageID] = meta
	h.pendingTimers[messageID] = time.AfterFunc(pendingMetaTTL, func() {
		h.mu.Lock()
		delete(h.pendingMeta, messageID)
		delete(h.pendingTimers, messageID)
		h.mu.Unlock()
	})
}

// consumePendingMeta retrieves and removes pending metadata. Caller must hold h.mu.
// consumePendingMeta 取出并删除待处理 metadata。调用方必须持有 h.mu。
func (h *Hub) consumePendingMeta(messageID string) (ClaimMetadata, bool) {
	meta, ok := h.pendingMeta[messageID]
	if ok {
		delete(h.pendingMeta, messageID)
		if t, exists := h.pendingTimers[messageID]; exists {
			t.Stop()
			delete(h.pendingTimers, messageID)
		}
	}
	return meta, ok
}

func (h *Hub) HandleClaim(ctx context.Context, connID, messageID string, meta ClaimMetadata) (bool, error) {
	// Merge pending metadata from Dispatch (carries channel routing info).
	// 合并 Dispatch 留下的待处理 metadata（携带渠道路由信息）。
	h.mu.Lock()
	if pending, ok := h.consumePendingMeta(messageID); ok {
		if meta.ChannelID == "" {
			meta.ChannelID = pending.ChannelID
		}
		if meta.ChannelType == "" {
			meta.ChannelType = pending.ChannelType
		}
		if meta.ThreadID == "" {
			meta.ThreadID = pending.ThreadID
		}
		if meta.ReplyToken == "" {
			meta.ReplyToken = pending.ReplyToken
		}
		if meta.AgentName == "" {
			meta.AgentName = pending.AgentName
		}
		if meta.WorkflowID == "" {
			meta.WorkflowID = pending.WorkflowID
		}
		if meta.WorkflowRunID == "" {
			meta.WorkflowRunID = pending.WorkflowRunID
		}
	}
	h.mu.Unlock()

	meta.ConnID = connID
	granted, err := h.claims.TryClaim(ctx, messageID, meta)
	if err != nil {
		return false, err
	}
	if granted {
		ClaimsTotal.WithLabelValues(meta.ChannelType, "claimed").Inc()
		if meta.ThreadID != "" && meta.ChannelType != "" {
			h.SetSticky(meta.ChannelType, meta.ThreadID, connID)
		}
	} else {
		ClaimsTotal.WithLabelValues(meta.ChannelType, "denied").Inc()
	}
	return granted, nil
}

// HandleProgress extends the claim TTL for a message being processed.
// If payload carries a workflow_id, updates the claim and notifies via OnWorkflowStarted.
// HandleProgress 延长正在处理的消息的 claim TTL。
// 若 payload 携带 workflow_id，则更新 claim 并通过 OnWorkflowStarted 通知。
func (h *Hub) HandleProgress(ctx context.Context, messageID string, payload *ProgressPayload) error {
	if err := h.claims.ExtendClaim(ctx, messageID, h.claims.claimTTL); err != nil {
		return err
	}

	if payload != nil && payload.WorkflowID != "" && len(payload.WorkflowID) <= 256 {
		meta, err := h.claims.GetClaim(ctx, messageID)
		if err != nil {
			return err
		}
		if meta.WorkflowID == "" { // Only set once 注：只设置一次
			meta.WorkflowID = payload.WorkflowID
			if err := h.claims.UpdateClaim(ctx, messageID, meta); err != nil {
				return err
			}
			if h.OnWorkflowStarted != nil {
				h.OnWorkflowStarted(ctx, messageID, meta)
			}
		}
	}
	return nil
}

// HandleReply retrieves claim metadata and releases the claim.
// HandleReply 取出 claim metadata 并释放 claim。
func (h *Hub) HandleReply(ctx context.Context, messageID string) (ClaimMetadata, error) {
	meta, err := h.claims.GetClaim(ctx, messageID)
	if err != nil {
		return ClaimMetadata{}, err
	}
	if err := h.claims.ReleaseClaim(ctx, messageID); err != nil {
		return meta, fmt.Errorf("release claim: %w", err)
	}

	if meta.Timestamp != "" {
		if t, err := time.Parse(time.RFC3339, meta.Timestamp); err == nil {
			ReplyLatency.WithLabelValues(meta.ChannelType).Observe(time.Since(t).Seconds())
		}
	}

	return meta, nil
}

// HandleDisconnect unregisters a connection and releases all its claims.
// HandleDisconnect 注销连接并释放其全部 claim。
func (h *Hub) HandleDisconnect(ctx context.Context, connID string) ([]string, error) {
	h.Unregister(connID)
	return h.claims.ReleaseAllForConn(ctx, connID)
}

// ConnectedCount returns the number of connections for a tenant:user pair.
// ConnectedCount 返回某个 tenant:user 组合的连接数。
func (h *Hub) ConnectedCount(tenantID, userID string) int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.index[indexKey(tenantID, userID)])
}

// SendToConn sends a message to a specific connection by ID.
// SendToConn 按 ID 向指定连接发送消息。
func (h *Hub) SendToConn(connID string, msg ServerMessage) error {
	conn, ok := h.GetConn(connID)
	if !ok {
		return fmt.Errorf("connection %s not found", connID)
	}
	return conn.Send(msg)
}

// Claims returns the underlying ClaimManager.
// Claims 返回底层的 ClaimManager。
func (h *Hub) Claims() *ClaimManager {
	return h.claims
}
