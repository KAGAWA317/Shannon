package daemon

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// daemonStreamTracker tracks initialization state of daemon event streams.
// daemonStreamTracker 跟踪 daemon event stream 的初始化状态。
type daemonStreamTracker struct {
	initState *sync.Map // Key: messageID, Value: bool (true=initialized, false=failed)
	// 键为 messageID，值为 bool（true=已初始化，false=失败）
}

func (t *daemonStreamTracker) isInitialized(messageID string) bool {
	v, ok := t.initState.Load(messageID)
	if !ok {
		return false
	}
	return v.(bool)
}

func (t *daemonStreamTracker) cleanup(messageID string) {
	t.initState.Delete(messageID)
}

// eventPublisher abstracts streaming.Manager.Publish for testability.
// eventPublisher 抽象 streaming.Manager.Publish，便于测试。
type eventPublisher interface {
	Publish(workflowID string, evt streaming.Event)
}

const (
	writeWait  = 10 * time.Second
	pongWait   = 60 * time.Second
	pingPeriod = 20 * time.Second
)

// ReplyCallback is called when a daemon sends a reply. The Hub calls this
// to route replies back to the originating channel.
// daemon 发送回复时调用 ReplyCallback。Hub 用它把回复路由回原渠道。
type ReplyCallback func(ctx context.Context, meta ClaimMetadata, reply ReplyPayload)

// ApprovalRequestCallback is called when a daemon sends an approval request
// that needs to be broadcast to channels (Slack buttons, LINE flex).
// daemon 发送需要广播到各渠道的审批 request 时调用 ApprovalRequestCallback
// （Slack 按钮、LINE flex）。
type ApprovalRequestCallback func(ctx context.Context, connID, tenantID, userID string, req DaemonApprovalRequest)

// ApprovalResolvedCallback is called when a daemon reports that an approval
// was resolved locally (e.g. by ShanClaw). Cloud should dismiss channel messages.
// daemon 报告审批已在本地处理时调用 ApprovalResolvedCallback（如 ShanClaw），Cloud 应撤回渠道消息。
type ApprovalResolvedCallback func(ctx context.Context, resolved DaemonApprovalResolved)

// DisconnectCallback is called when a daemon connection is closed, with the conn ID.
// daemon 连接关闭时调用 DisconnectCallback，携带连接 ID。
type DisconnectCallback func(ctx context.Context, connID string)

// ProactiveCallback is called when a daemon sends an unsolicited message
// to be delivered to all channels mapped to the named agent.
// daemon 发送非 request 消息时调用 ProactiveCallback，
// 消息将投递给该 agent 映射到的所有渠道。
type ProactiveCallback func(ctx context.Context, connID, tenantID, userID string, payload ProactivePayload) error

// ConnCallbacks groups all callbacks for daemon message handling.
// ConnCallbacks 汇总 daemon 消息处理的全部回调。
type ConnCallbacks struct {
	OnReply            ReplyCallback
	OnApprovalRequest  ApprovalRequestCallback
	OnApprovalResolved ApprovalResolvedCallback
	OnProactive        ProactiveCallback
	OnDisconnect       DisconnectCallback
	StreamTracker      *daemonStreamTracker
}

// HandleConnection runs the read/write loop for a single daemon WebSocket connection.
// Blocks until the connection is closed.
// HandleConnection 运行单个 daemon WebSocket 连接的读写循环，阻塞直到连接关闭。
func HandleConnection(ctx context.Context, hub *Hub, ws *websocket.Conn, tenantID, userID string, onReply ReplyCallback, logger *zap.Logger, opts ...func(*ConnCallbacks)) {
	cbs := &ConnCallbacks{OnReply: onReply}
	for _, opt := range opts {
		opt(cbs)
	}
	cbs.StreamTracker = &daemonStreamTracker{initState: &sync.Map{}}
	connID := uuid.New().String()

	conn := &DaemonConn{
		ID:          connID,
		TenantID:    tenantID,
		UserID:      userID,
		ConnectedAt: time.Now(),
		LastActive:  time.Now(),
		sendFn: func(msg ServerMessage) error {
			ws.SetWriteDeadline(time.Now().Add(writeWait))
			return ws.WriteJSON(msg)
		},
	}

	hub.Register(conn)
	defer func() {
		released, _ := hub.HandleDisconnect(ctx, connID)
		if len(released) > 0 {
			logger.Warn("daemon disconnected with active claims",
				zap.String("conn_id", connID),
				zap.Strings("released_messages", released),
			)
		}
		if cbs.OnDisconnect != nil {
			cbs.OnDisconnect(ctx, connID)
		}
		ws.Close()
	}()

	// Send connected confirmation.
	conn.Send(ServerMessage{Type: MsgTypeConnected})

	// Configure WebSocket.
	ws.SetReadLimit(64 * 1024)
	ws.SetReadDeadline(time.Now().Add(pongWait))
	ws.SetPongHandler(func(string) error {
		ws.SetReadDeadline(time.Now().Add(pongWait))
		return nil
	})

	// Ping ticker.
	ticker := time.NewTicker(pingPeriod)
	defer ticker.Stop()

	// Read pump in separate goroutine.
	msgCh := make(chan DaemonMessage, 16)
	go func() {
		defer close(msgCh)
		for {
			var msg DaemonMessage
			if err := ws.ReadJSON(&msg); err != nil {
				return
			}
			msgCh <- msg
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-msgCh:
			if !ok {
				return
			}
			handleDaemonMessage(ctx, hub, conn, msg, cbs, logger)
		case <-ticker.C:
			// Acquire conn mutex — gorilla/websocket requires single-writer serialization.
			// Send() also holds this lock, so pings are serialized with data writes.
			// 获取连接互斥锁——gorilla/websocket 要求单写者串行化。
			// Send() 也持有该锁，因此 ping 与数据写入是串行的。
			conn.mu.Lock()
			ws.SetWriteDeadline(time.Now().Add(writeWait))
			err := ws.WriteMessage(websocket.PingMessage, nil)
			conn.mu.Unlock()
			if err != nil {
				return
			}
		}
	}
}

func handleDaemonMessage(ctx context.Context, hub *Hub, conn *DaemonConn, msg DaemonMessage, cbs *ConnCallbacks, logger *zap.Logger) {
	if msg.Type == "" {
		return // Ignore empty-type messages (e.g. keep-alive frames)
		// 忽略无类型的消息（如 keep-alive 帧）
	}

	if msg.MessageID == "" && (msg.Type == MsgTypeClaim || msg.Type == MsgTypeReply || msg.Type == MsgTypeProgress || msg.Type == MsgTypeEvent) {
		logger.Warn("daemon message missing message_id", zap.String("type", msg.Type))
		return
	}

	switch msg.Type {
	case MsgTypeClaim:
		granted, err := hub.HandleClaim(ctx, conn.ID, msg.MessageID, ClaimMetadata{
			ConnID:    conn.ID,
			Timestamp: time.Now().UTC().Format(time.RFC3339),
		})
		if err != nil {
			logger.Error("claim failed", zap.Error(err), zap.String("message_id", msg.MessageID))
			return
		}
		ackPayload, _ := json.Marshal(ClaimAckPayload{Granted: granted})
		conn.Send(ServerMessage{
			Type:      MsgTypeClaimAck,
			MessageID: msg.MessageID,
			Payload:   ackPayload,
		})

	case MsgTypeProgress:
		var payload *ProgressPayload
		if len(msg.Payload) > 0 {
			var p ProgressPayload
			if err := json.Unmarshal(msg.Payload, &p); err == nil && p.WorkflowID != "" {
				payload = &p
			}
		}
		if err := hub.HandleProgress(ctx, msg.MessageID, payload); err != nil {
			logger.Warn("progress heartbeat failed", zap.Error(err), zap.String("message_id", msg.MessageID))
		}

	case MsgTypeEvent:
		publisher := streaming.Get()
		if cbs.StreamTracker == nil {
			cbs.StreamTracker = &daemonStreamTracker{initState: &sync.Map{}}
		}
		handleDaemonEvent(ctx, hub, msg.MessageID, msg.Payload, publisher, cbs.StreamTracker, logger)

	case MsgTypeReply:
		// If streaming was initialized for this message, publish WORKFLOW_COMPLETED
		// before HandleReply so StreamConsumer processes it before the reply callback
		// tears down the channel context.
		// 若该消息已初始化 stream，则在 HandleReply 之前发布 WORKFLOW_COMPLETED，
		// 使 StreamConsumer 在回复回调拆掉 channel context 之前处理它。
		if cbs.StreamTracker != nil {
			streamID := "daemon:" + msg.MessageID
			if cbs.StreamTracker.isInitialized(msg.MessageID) {
				var reply ReplyPayload
				if err := json.Unmarshal(msg.Payload, &reply); err == nil {
					publisher := streaming.Get()
					publisher.Publish(streamID, streaming.Event{
						WorkflowID: streamID,
						Type:       "WORKFLOW_COMPLETED",
						AgentID:    "",
						Message:    reply.Text,
						Payload:    map[string]interface{}{"response": reply.Text},
						Timestamp:  time.Now(),
					})
					// No sleep needed — RouteReply unconditionally suppresses plain-text
					// replies for daemon streams (WorkflowID prefix "daemon:").
					// 无需 sleep——RouteReply 会无条件抑制 daemon stream（WorkflowID 前缀 "daemon:"）的纯文本回复。
				}
			}
			// Always cleanup to prevent sync.Map leak.
			// 始终执行清理，避免 sync.Map 泄漏。
			cbs.StreamTracker.cleanup(msg.MessageID)
		}

		meta, err := hub.HandleReply(ctx, msg.MessageID)
		if err != nil {
			logger.Warn("reply for unknown claim", zap.Error(err), zap.String("message_id", msg.MessageID))
			return
		}
		var reply ReplyPayload
		if err := json.Unmarshal(msg.Payload, &reply); err != nil {
			logger.Error("invalid reply payload", zap.Error(err))
			return
		}
		if cbs.OnReply != nil {
			cbs.OnReply(ctx, meta, reply)
		}
		logger.Info("daemon reply received",
			zap.String("message_id", msg.MessageID),
			zap.String("channel_type", meta.ChannelType),
			zap.String("conn_id", conn.ID),
		)

	case MsgTypeDaemonApprovalRequest:
		var req DaemonApprovalRequest
		if err := json.Unmarshal(msg.Payload, &req); err != nil {
			logger.Error("invalid approval_request payload", zap.Error(err))
			return
		}
		// Resolve thread context from the active claim for this message
		// 从该消息的活跃 claim 解析 thread context
		if msg.MessageID != "" {
			if meta, err := hub.Claims().GetClaim(ctx, msg.MessageID); err == nil {
				req.ThreadID = meta.ThreadID
				req.ChannelID = meta.ChannelID
			}
		}
		if cbs.OnApprovalRequest != nil {
			cbs.OnApprovalRequest(ctx, conn.ID, conn.TenantID, conn.UserID, req)
		}
		logger.Info("daemon approval request received",
			zap.String("request_id", req.RequestID),
			zap.String("tool", req.Tool),
			zap.String("conn_id", conn.ID),
		)

	case MsgTypeDaemonApprovalResolved:
		var resolved DaemonApprovalResolved
		if err := json.Unmarshal(msg.Payload, &resolved); err != nil {
			logger.Error("invalid approval_resolved payload", zap.Error(err))
			return
		}
		if cbs.OnApprovalResolved != nil {
			cbs.OnApprovalResolved(ctx, resolved)
		}
		logger.Info("daemon approval resolved locally",
			zap.String("request_id", resolved.RequestID),
			zap.String("resolved_by", resolved.ResolvedBy),
			zap.String("conn_id", conn.ID),
		)

	case MsgTypeProactive:
		var payload ProactivePayload
		if err := json.Unmarshal(msg.Payload, &payload); err != nil {
			logger.Warn("invalid proactive payload", zap.Error(err))
			return
		}
		if payload.AgentName == "" || payload.Text == "" {
			logger.Warn("proactive message missing required fields")
			return
		}
		if cbs.OnProactive != nil {
			if err := cbs.OnProactive(ctx, conn.ID, conn.TenantID, conn.UserID, payload); err != nil {
				logger.Error("proactive delivery failed", zap.Error(err), zap.String("agent", payload.AgentName))
			}
		}
		logger.Info("daemon proactive message received",
			zap.String("agent", payload.AgentName),
			zap.String("conn_id", conn.ID),
		)

	case MsgTypeDisconnect:
		logger.Info("daemon graceful disconnect", zap.String("conn_id", conn.ID))

	default:
		logger.Warn("unknown daemon message type", zap.String("type", msg.Type))
	}
}

// handleDaemonEvent processes a single agent loop event forwarded by the daemon.
// On the first event for a given messageID, it initializes the stream via HandleProgress
// (which triggers OnWorkflowStarted for channel card creation). All events are published
// to the streaming.Manager for progressive card updates.
// handleDaemonEvent 处理 daemon 转发的单条 agent 循环 event。
// 对某个 messageID 的首个 event，通过 HandleProgress 初始化 stream
// （进而触发 OnWorkflowStarted 创建渠道卡片）。所有 event 都会发布到
// streaming.Manager 以渐进更新卡片。
func handleDaemonEvent(ctx context.Context, hub *Hub, messageID string, rawPayload json.RawMessage, publisher eventPublisher, tracker *daemonStreamTracker, logger *zap.Logger) {
	if logger == nil {
		logger = zap.NewNop()
	}
	if messageID == "" {
		return
	}

	var payload DaemonEventPayload
	if err := json.Unmarshal(rawPayload, &payload); err != nil {
		logger.Warn("invalid daemon event payload", zap.Error(err), zap.String("message_id", messageID))
		return
	}

	streamID := "daemon:" + messageID

	// Initialize stream on first event (or retry after previous failure).
	// 在首个 event 时初始化 stream（或上次失败后 retry）。
	if val, loaded := tracker.initState.Load(messageID); !loaded || !val.(bool) {
		err := hub.HandleProgress(ctx, messageID, &ProgressPayload{WorkflowID: streamID})
		if err != nil {
			tracker.initState.Store(messageID, false)
			logger.Warn("daemon event init failed (HandleProgress)",
				zap.Error(err),
				zap.String("message_id", messageID),
			)
		} else {
			tracker.initState.Store(messageID, true)
		}
	}

	// Preserve daemon timestamp; fall back to now.
	// 保留 daemon 时间戳；缺失时回退为当前时间。
	ts := time.Now()
	if payload.Timestamp != "" {
		if parsed, err := time.Parse(time.RFC3339, payload.Timestamp); err == nil {
			ts = parsed
		}
	}

	// Copy daemon data and inject daemon_seq.
	data := payload.Data
	if data == nil {
		data = make(map[string]interface{})
	}
	data["daemon_seq"] = payload.Seq

	publisher.Publish(streamID, streaming.Event{
		WorkflowID: streamID,
		Type:       payload.EventType,
		AgentID:    "", // Must be empty — StreamConsumer's isSynthesisAgent filter drops non-empty for LLM_OUTPUT/LLM_PARTIAL
		// 必须为空——StreamConsumer 的 isSynthesisAgent 过滤会对 LLM_OUTPUT/LLM_PARTIAL 丢弃非空值
		Message:    payload.Message,
		Payload:    data,
		Timestamp:  ts,
	})
}
