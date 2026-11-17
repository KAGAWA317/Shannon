package workflows

import (
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
)

// CheckApprovalPolicy evaluates whether human approval is required based on
// decomposition results and simple policy defaults.
// CheckApprovalPolicy 依据分解结果与简单 policy 默认值，
// 判断是否需要人工审批。
func CheckApprovalPolicy(_ workflow.Context, input TaskInput, decomp activities.DecompositionResult) (bool, string) {
	policy := activities.ApprovalPolicy{
		ComplexityThreshold: 0.8,
		TokenBudgetExceeded: false,
		RequireForTools:     []string{"file_system", "code_execution"},
	}
	return CheckApprovalPolicyWith(policy, input, decomp)
}

// CheckApprovalPolicyWith evaluates approval against a provided policy
// CheckApprovalPolicyWith 按传入的 policy 评估审批
func CheckApprovalPolicyWith(policy activities.ApprovalPolicy, input TaskInput, decomp activities.DecompositionResult) (bool, string) {

	ctx := map[string]any{
		"complexity_score": decomp.ComplexityScore,
		"query":            input.Query,
	}
	// Aggregate suggested tools (best-effort)
	// 从各 subtask 汇总 suggested tools，仅用于策略评估，缺失不影响主流程。
	tools := []string{}
	for _, st := range decomp.Subtasks {
		if len(st.SuggestedTools) > 0 {
			tools = append(tools, st.SuggestedTools...)
		}
	}
	if len(tools) > 0 {
		ctx["tools_to_use"] = tools
	}

	return activities.EvaluateApprovalPolicy(policy, ctx)
}

// RequestAndWaitApproval requests approval and waits on a signal until the
// timeout is reached. Returns the final approval result.
// RequestAndWaitApproval 请求审批并等待 signal，直到
// 超时；返回最终审批结果。
func RequestAndWaitApproval(ctx workflow.Context, input TaskInput, reason string) (*activities.HumanApprovalResult, error) {
	logger := workflow.GetLogger(ctx)

	// Request approval via activity with timeout
	// 通过 activity 请求审批，带超时
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute, // Activity timeout for request 注：请求审批的 activity 超时
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	var approval activities.HumanApprovalResult
	err := workflow.ExecuteActivity(ctx, constants.RequestApprovalActivity, activities.HumanApprovalInput{
		SessionID:      input.SessionID,
		WorkflowID:     workflow.GetInfo(ctx).WorkflowExecution.ID,
		RunID:          workflow.GetInfo(ctx).WorkflowExecution.RunID,
		Query:          input.Query,
		Context:        input.Context,
		ProposedAction: "execute_decomposition_plan",
		Reason:         reason,
		Metadata:       map[string]any{"subtasks": len(input.History)},
	}).Get(ctx, &approval)
	if err != nil {
		return nil, err
	}

	// Emit APPROVAL_REQUESTED event via activity
	// 通过 activity 发出 APPROVAL_REQUESTED 事件
	_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		EventType:  activities.StreamEventApprovalRequested,
		AgentID:    "orchestrator",
		Message:    activities.MsgApprovalRequested(reason, approval.ApprovalID),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	logger.Info("Waiting for human approval", "approval_id", approval.ApprovalID)

	// Await either signal or timeout
	// selector 同时等审批信号与 timer，谁先到用谁；timeout 按拒绝处理。
	sigName := "human-approval-" + approval.ApprovalID
	ch := workflow.GetSignalChannel(ctx, sigName)
	sel := workflow.NewSelector(ctx)

	timeout := 60 * time.Minute // Default to 1 hour 注：默认 1 小时
	if input.ApprovalTimeout > 0 {
		timeout = time.Duration(input.ApprovalTimeout) * time.Second
	}
	timer := workflow.NewTimer(ctx, timeout)

	var result activities.HumanApprovalResult
	var timedOut bool

	sel.AddReceive(ch, func(c workflow.ReceiveChannel, more bool) {
		c.Receive(ctx, &result)
	})
	sel.AddFuture(timer, func(f workflow.Future) {
		timedOut = true
		result = activities.HumanApprovalResult{Approved: false, Feedback: "approval timeout"}
	})
	sel.Select(ctx)

	if timedOut {
		logger.Warn("Approval timed out", "approval_id", approval.ApprovalID)
	}
	// Emit APPROVAL_DECISION event
	// 发出 APPROVAL_DECISION 事件
	decision := "denied"
	if result.Approved {
		decision = "approved"
	}
	_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		EventType:  activities.StreamEventApprovalDecision,
		AgentID:    "orchestrator",
		Message:    activities.MsgApprovalProcessed(decision),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)
	return &result, nil
}
