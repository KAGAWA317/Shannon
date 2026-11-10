package activities

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
)

// HumanApprovalInput represents a request for human approval
// HumanApprovalInput 表示一次人工审批 request。
type HumanApprovalInput struct {
	SessionID      string                 `json:"session_id"`
	WorkflowID     string                 `json:"workflow_id"`
	RunID          string                 `json:"run_id"`
	Query          string                 `json:"query"`
	Context        map[string]interface{} `json:"context"`
	ProposedAction string                 `json:"proposed_action"`
	Reason         string                 `json:"reason"`
	Metadata       map[string]interface{} `json:"metadata"`
}

// HumanApprovalResult represents the human's response
// HumanApprovalResult 表示人工给出的审批结果。
type HumanApprovalResult struct {
	ApprovalID     string    `json:"approval_id"`
	Approved       bool      `json:"approved"`
	Feedback       string    `json:"feedback"`
	ModifiedAction string    `json:"modified_action"`
	ApprovedBy     string    `json:"approved_by"`
	Timestamp      time.Time `json:"timestamp"`
}

// HumanInterventionActivities handles human-in-the-loop approvals
// HumanInterventionActivities 负责 human-in-the-loop 审批。
type HumanInterventionActivities struct {
	// In production, these would be real database and notification services
	// For now, we'll use in-memory storage
	// 生产环境应接真实数据库与通知服务；
	// 目前先用内存存储占位。
	approvals map[string]*HumanApprovalResult
	mu        sync.RWMutex // Protect concurrent access to approvals map
	// mu 保护 approvals map 的并发访问。
}

// NewHumanInterventionActivities creates a new human intervention activities handler
// NewHumanInterventionActivities 创建 human intervention activities 实例。
func NewHumanInterventionActivities() *HumanInterventionActivities {
	return &HumanInterventionActivities{
		approvals: make(map[string]*HumanApprovalResult),
	}
}

// RequestApproval creates an approval request and returns an approval ID
// The workflow will wait for a signal with the actual approval
// RequestApproval 创建审批 request 并返回 approval ID；
// workflow 随后等待携带审批结果的 signal。
func (h *HumanInterventionActivities) RequestApproval(ctx context.Context, input HumanApprovalInput) (HumanApprovalResult, error) {
	// Generate approval ID
	// 生成 approval ID。
	approvalID := uuid.New().String()

	// Log the approval request
	// 记录审批 request。
	fmt.Printf("Human approval requested:\n")
	fmt.Printf("  Approval ID: %s\n", approvalID)
	fmt.Printf("  Query: %s\n", input.Query)
	fmt.Printf("  Reason: %s\n", input.Reason)
	fmt.Printf("  Proposed Action: %s\n", input.ProposedAction)

	// In production, this would:
	// 1. Store the request in a database
	// 2. Send notifications (webhook, email, etc.)
	// 3. Return the approval ID
	// 生产环境应在此：
	// 1. 把 request 写入数据库
	// 2. 发送通知（webhook、email 等）
	// 3. 返回 approval ID

	// Return approval ID for the workflow to track
	// 返回 approval ID 供 workflow 跟踪。
	return HumanApprovalResult{
		ApprovalID: approvalID,
		Timestamp:  time.Now(),
	}, nil
}

// ProcessApprovalResponse processes the human's response to an approval request
// ProcessApprovalResponse 处理人工对审批 request 的回复。
func (h *HumanInterventionActivities) ProcessApprovalResponse(ctx context.Context, response HumanApprovalResult) error {
	// Store the response with mutex protection
	// 加锁保存 response。
	h.mu.Lock()
	h.approvals[response.ApprovalID] = &response
	h.mu.Unlock()

	fmt.Printf("Approval response processed:\n")
	fmt.Printf("  Approval ID: %s\n", response.ApprovalID)
	fmt.Printf("  Approved: %v\n", response.Approved)
	fmt.Printf("  Feedback: %s\n", response.Feedback)

	return nil
}

// GetApprovalStatus retrieves the current status of an approval request
// GetApprovalStatus 查询审批 request 的当前状态。
func (h *HumanInterventionActivities) GetApprovalStatus(ctx context.Context, approvalID string) (HumanApprovalResult, error) {
	h.mu.RLock()
	result, exists := h.approvals[approvalID]
	h.mu.RUnlock()

	if exists {
		return *result, nil
	}

	// Return empty result if not found (still pending)
	// 查不到时返回空结果，表示仍在等待。
	return HumanApprovalResult{
		ApprovalID: approvalID,
	}, nil
}

// ApprovalPolicy defines when human intervention is required
// ApprovalPolicy 定义何时需要人工介入。
type ApprovalPolicy struct {
	ComplexityThreshold float64  `json:"complexity_threshold"`
	TokenBudgetExceeded bool     `json:"token_budget_exceeded"`
	RequireForTools     []string `json:"require_for_tools"`
}

// EvaluateApprovalPolicy determines if human approval is required
// EvaluateApprovalPolicy 判断是否需要人工审批。
func EvaluateApprovalPolicy(policy ApprovalPolicy, context map[string]interface{}) (bool, string) {
	// Check complexity threshold
	// 检查复杂度阈值。
	if complexity, ok := context["complexity_score"].(float64); ok {
		if policy.ComplexityThreshold > 0 && complexity >= policy.ComplexityThreshold {
			return true, fmt.Sprintf("Complexity score %.2f exceeds threshold %.2f", complexity, policy.ComplexityThreshold)
		}
	}

	// Check token budget (handle both int and float64 from JSON)
	// 检查 token budget；JSON 反序列化可能是 int 也可能是 float64，两种都要处理。
	if policy.TokenBudgetExceeded {
		var tokensUsed, tokenBudget int

		// Handle tokens_used as either int or float64
		// tokens_used 可能为 int 或 float64。
		switch v := context["tokens_used"].(type) {
		case int:
			tokensUsed = v
		case float64:
			tokensUsed = int(math.Round(v))
		}

		// Handle token_budget as either int or float64
		// token_budget 可能为 int 或 float64。
		switch v := context["token_budget"].(type) {
		case int:
			tokenBudget = v
		case float64:
			tokenBudget = int(math.Round(v))
		}

		if tokensUsed > 0 && tokenBudget > 0 && tokensUsed >= tokenBudget {
			return true, fmt.Sprintf("Token budget exceeded: %d/%d", tokensUsed, tokenBudget)
		}
	}

	// Check tools
	// 检查 tools。
	if len(policy.RequireForTools) > 0 {
		// Handle tools_to_use as []string or []interface{} (from JSON)
		// tools_to_use 可能是 []string，也可能是 JSON 反序列化出的 []interface{}。
		switch tools := context["tools_to_use"].(type) {
		case []string:
			for _, tool := range tools {
				for _, requiredTool := range policy.RequireForTools {
					if tool == requiredTool {
						return true, fmt.Sprintf("Tool '%s' requires approval", tool)
					}
				}
			}
		case []interface{}:
			for _, toolInterface := range tools {
				if tool, ok := toolInterface.(string); ok {
					for _, requiredTool := range policy.RequireForTools {
						if tool == requiredTool {
							return true, fmt.Sprintf("Tool '%s' requires approval", tool)
						}
					}
				}
			}
		}
	}

	return false, ""
}
