package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ApprovalHandler handles approval-related HTTP requests
// ApprovalHandler 处理与审批相关的 HTTP request。
type ApprovalHandler struct {
	orchClient orchpb.OrchestratorServiceClient
	logger     *zap.Logger
}

// NewApprovalHandler creates a new approval handler
// NewApprovalHandler 创建新的 approval handler。
func NewApprovalHandler(
	orchClient orchpb.OrchestratorServiceClient,
	logger *zap.Logger,
) *ApprovalHandler {
	return &ApprovalHandler{
		orchClient: orchClient,
		logger:     logger,
	}
}

// ApprovalDecisionRequest represents the approval decision request
// ApprovalDecisionRequest 表示审批决策 request。
type ApprovalDecisionRequest struct {
	WorkflowID     string `json:"workflow_id"`
	RunID          string `json:"run_id,omitempty"`
	ApprovalID     string `json:"approval_id"`
	Approved       bool   `json:"approved"`
	Feedback       string `json:"feedback,omitempty"`
	ModifiedAction string `json:"modified_action,omitempty"`
	ApprovedBy     string `json:"approved_by,omitempty"`
}

// SubmitDecision handles POST /api/v1/approvals/decision
// SubmitDecision 处理 POST /api/v1/approvals/decision。
func (h *ApprovalHandler) SubmitDecision(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 获取用户 context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse request body
	// 解析 request body。
	var req ApprovalDecisionRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate required fields
	// 校验必填字段。
	if req.WorkflowID == "" || req.ApprovalID == "" {
		h.sendError(w, "workflow_id and approval_id are required", http.StatusBadRequest)
		return
	}

	// Use authenticated user as approved_by if not provided
	// 未提供 approved_by 时，使用 authenticated 用户。
	if req.ApprovedBy == "" {
		req.ApprovedBy = userCtx.UserID.String()
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 将 auth/tracing 头透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Build gRPC request
	// 构造 gRPC request。
	grpcReq := &orchpb.ApproveTaskRequest{
		ApprovalId:     req.ApprovalID,
		WorkflowId:     req.WorkflowID,
		RunId:          req.RunID,
		Approved:       req.Approved,
		Feedback:       req.Feedback,
		ModifiedAction: req.ModifiedAction,
		ApprovedBy:     req.ApprovedBy,
	}

	// Call ApproveTask gRPC
	// 调用 ApproveTask gRPC。
	resp, err := h.orchClient.ApproveTask(ctx, grpcReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				h.sendError(w, st.Message(), http.StatusBadRequest)
			case codes.NotFound:
				h.sendError(w, "Workflow or approval not found", http.StatusNotFound)
			case codes.Unauthenticated:
				h.sendError(w, "Unauthorized", http.StatusUnauthorized)
			case codes.PermissionDenied:
				h.sendError(w, "Forbidden", http.StatusForbidden)
			default:
				h.sendError(w, fmt.Sprintf("Failed to submit approval: %v", st.Message()), http.StatusInternalServerError)
			}
		} else {
			h.sendError(w, fmt.Sprintf("Failed to submit approval: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Log approval
	// 记录审批日志。
	h.logger.Info("Approval decision submitted",
		zap.String("workflow_id", req.WorkflowID),
		zap.String("approval_id", req.ApprovalID),
		zap.Bool("approved", req.Approved),
		zap.String("user_id", userCtx.UserID.String()),
	)

	// Send response
	// 发送响应。
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]any{
		"status":      "sent",
		"success":     resp.Success,
		"message":     resp.Message,
		"workflow_id": req.WorkflowID,
		"run_id":      req.RunID,
		"approval_id": req.ApprovalID,
	})
}

// sendError sends an error response
// sendError 发送错误响应。
func (h *ApprovalHandler) sendError(w http.ResponseWriter, message string, code int) {
	writeJSON(w, code, map[string]string{"error": message})
}
