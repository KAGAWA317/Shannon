package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	authpkg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/daemon"
	"go.uber.org/zap"
)

var signalClient = &http.Client{Timeout: 10 * time.Second}

var wsUpgrader = websocket.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

type DaemonHandler struct {
	hub         *daemon.Hub
	approvals   *daemon.ApprovalManager
	adminURL    string // orchestrator admin server URL for Temporal signals
	eventsToken string // auth token for admin server
	logger      *zap.Logger
}

func NewDaemonHandler(hub *daemon.Hub, approvals *daemon.ApprovalManager, adminURL, eventsToken string, logger *zap.Logger) *DaemonHandler {
	dh := &DaemonHandler{hub: hub, approvals: approvals, adminURL: adminURL, eventsToken: eventsToken, logger: logger}
	return dh
}

func (dh *DaemonHandler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	userCtx, ok := r.Context().Value(authpkg.UserContextKey).(*authpkg.UserContext)
	if !ok || userCtx == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	ws, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		dh.logger.Error("websocket upgrade failed", zap.Error(err))
		return
	}

	tenantID := userCtx.TenantID
	userID := userCtx.UserID

	// onReply callback — routes replies back to the originating channel and/or Temporal workflow,
	// then broadcasts to all other channels configured for the same agent.
	// onReply 回调——把回复路由回原渠道和/或 Temporal workflow，再广播给同一 agent 的其他已配置渠道。
	onReply := func(ctx context.Context, meta daemon.ClaimMetadata, reply daemon.ReplyPayload) {
		// Signal Temporal workflow if this reply is for a scheduled task.
		// Skip daemon streams (synthetic "daemon:" prefix) — no Temporal workflow to signal.
		// 若该回复属于定时 task，则 signal 对应的 Temporal workflow；跳过 daemon stream（合成前缀 "daemon:"），它们没有可 signal 的 workflow。
		if meta.WorkflowID != "" && !strings.HasPrefix(meta.WorkflowID, "daemon:") {
			dh.signalWorkflow(ctx, meta, reply)
		}

		// Outbound channel routing removed in OSS (channels package not available)
		// OSS 版本已移除出站渠道路由（没有 channels 包）。
	}

	// Approval callbacks
	onApprovalRequest := func(ctx context.Context, connID, tID, uID string, req daemon.DaemonApprovalRequest) {
		if dh.approvals == nil {
			return
		}
		dh.handleApprovalRequest(ctx, connID, tID, uID, req)
	}
	onApprovalResolved := func(ctx context.Context, resolved daemon.DaemonApprovalResolved) {
		if dh.approvals == nil {
			return
		}
		dh.handleApprovalResolved(ctx, resolved)
	}

	onDisconnect := func(ctx context.Context, connID string) {
		if dh.approvals == nil {
			return
		}
		cancelled, err := dh.approvals.CancelByConn(ctx, connID)
		if err != nil {
			dh.logger.Warn("failed to cancel approvals on disconnect",
				zap.String("conn_id", connID),
				zap.Error(err),
			)
		}
		if len(cancelled) > 0 {
			dh.logger.Info("cancelled pending approvals on disconnect",
				zap.String("conn_id", connID),
				zap.Strings("request_ids", cancelled),
			)
		}
	}

	onProactive := func(ctx context.Context, connID, tID, uID string, payload daemon.ProactivePayload) error {
		// Outbound channel routing removed in OSS
		return nil
	}

	daemon.HandleConnection(r.Context(), dh.hub, ws, tenantID.String(), userID.String(), onReply, dh.logger,
		func(cbs *daemon.ConnCallbacks) {
			cbs.OnApprovalRequest = onApprovalRequest
			cbs.OnApprovalResolved = onApprovalResolved
			cbs.OnProactive = onProactive
			cbs.OnDisconnect = onDisconnect
		},
	)
}

// signalWorkflow sends a daemon reply signal to the Temporal workflow via the admin server.
// signalWorkflow 通过 admin server 向 Temporal workflow 发送 daemon 回复 signal。
func (dh *DaemonHandler) signalWorkflow(ctx context.Context, meta daemon.ClaimMetadata, reply daemon.ReplyPayload) {
	if dh.adminURL == "" {
		dh.logger.Warn("cannot signal workflow: adminURL not configured", zap.String("workflow_id", meta.WorkflowID))
		return
	}

	payload := map[string]interface{}{
		"workflow_id":     meta.WorkflowID,
		"workflow_run_id": meta.WorkflowRunID,
		"reply":           reply,
	}
	body, _ := json.Marshal(payload)

	signalCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	url := dh.adminURL + "/daemon/signal"
	req, err := http.NewRequestWithContext(signalCtx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		dh.logger.Error("failed to create signal request", zap.Error(err))
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if dh.eventsToken != "" {
		req.Header.Set("Authorization", "Bearer "+dh.eventsToken)
	}

	resp, err := signalClient.Do(req)
	if err != nil {
		dh.logger.Error("failed to signal workflow",
			zap.String("workflow_id", meta.WorkflowID),
			zap.Error(err),
		)
		return
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		dh.logger.Error("workflow signal returned non-200",
			zap.String("workflow_id", meta.WorkflowID),
			zap.Int("status", resp.StatusCode),
		)
	}
}

// handleApprovalRequest broadcasts an approval to all channels for the agent.
// handleApprovalRequest 把审批广播到该 agent 的所有渠道。
func (dh *DaemonHandler) handleApprovalRequest(ctx context.Context, connID, tenantID, userID string, req daemon.DaemonApprovalRequest) {
	approval := daemon.PendingApproval{
		RequestID: req.RequestID,
		ConnID:    connID,
		TenantID:  tenantID,
		UserID:    userID,
		Agent:     req.Agent,
		Tool:      req.Tool,
		Args:      req.Args,
		ThreadID:  req.ThreadID,
		CreatedAt: time.Now().UTC().Format(time.RFC3339),
	}

	if err := dh.approvals.Store(ctx, approval); err != nil {
		dh.logger.Error("failed to store approval", zap.Error(err), zap.String("request_id", req.RequestID))
		return
	}

	// Outbound channel broadcast removed in OSS
}

// handleApprovalResolved dismisses channel approval messages when ShanClaw resolved first.
// handleApprovalResolved 在 ShanClaw 已先解决时，关闭渠道上的审批消息。
func (dh *DaemonHandler) handleApprovalResolved(ctx context.Context, resolved daemon.DaemonApprovalResolved) {
	_, err := dh.approvals.Resolve(ctx, resolved.RequestID)
	if err != nil {
		if err == daemon.ErrApprovalNotFound {
			dh.logger.Debug("approval already resolved", zap.String("request_id", resolved.RequestID))
			return
		}
		dh.logger.Error("failed to resolve approval", zap.Error(err), zap.String("request_id", resolved.RequestID))
		return
	}
}

// Valid approval decisions that the daemon recognizes.
var validDecisions = map[string]bool{
	"allow":        true,
	"deny":         true,
	"always_allow": true,
}

func isValidDecision(decision string) bool {
	return validDecisions[decision]
}

// ResolveApproval resolves an approval from a channel callback (Slack/LINE), sends
// the decision to the daemon, and dismisses other channel messages.
// ResolveApproval 处理来自渠道回调（Slack/LINE）的审批：把决定发给 daemon，并关闭其他渠道消息。
func (dh *DaemonHandler) ResolveApproval(ctx context.Context, requestID, decision, resolvedBy string) error {
	if !isValidDecision(decision) {
		return fmt.Errorf("invalid approval decision: %q", decision)
	}

	approval, err := dh.approvals.Resolve(ctx, requestID)
	if err != nil {
		return err
	}

	// Send approval_response to daemon — fail closed if delivery fails.
	// If the daemon can't receive the decision, re-store the approval so
	// another callback (or retry) can attempt delivery.
	// 向 daemon 发送 approval_response——投递失败即 fail closed；若 daemon 收不到决定，就把审批重新存回，供其他回调（或 retry）再次投递。
	payload, _ := json.Marshal(daemon.ApprovalResponsePayload{
		RequestID:  requestID,
		Decision:   decision,
		ResolvedBy: resolvedBy,
	})
	if err := dh.hub.SendToConn(approval.ConnID, daemon.ServerMessage{
		Type:    daemon.MsgTypeApprovalResponse,
		Payload: payload,
	}); err != nil {
		dh.logger.Error("failed to send approval_response to daemon, re-storing approval",
			zap.String("request_id", requestID),
			zap.String("conn_id", approval.ConnID),
			zap.Error(err),
		)
		// Re-store so another responder can retry
		if storeErr := dh.approvals.Store(ctx, approval); storeErr != nil {
			dh.logger.Error("failed to re-store approval after send failure",
				zap.String("request_id", requestID),
				zap.Error(storeErr),
			)
			return fmt.Errorf("approval_response delivery failed: %w; re-store also failed: %v", err, storeErr)
		}
		return fmt.Errorf("approval_response delivery failed (approval re-queued): %w", err)
	}

	// Outbound channel dismiss removed in OSS

	return nil
}

// Approvals returns the approval manager (used by interaction handlers).
func (dh *DaemonHandler) Approvals() *daemon.ApprovalManager {
	return dh.approvals
}

func (dh *DaemonHandler) HandleStatus(w http.ResponseWriter, r *http.Request) {
	userCtx, ok := r.Context().Value(authpkg.UserContextKey).(*authpkg.UserContext)
	if !ok || userCtx == nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	count := dh.hub.ConnectedCount(userCtx.TenantID.String(), userCtx.UserID.String())
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]int{"connected_daemons": count})
}
