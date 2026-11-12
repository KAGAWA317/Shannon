package activities

import (
	"context"
	policy "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/policy"
	"time"
)

// TeamActionInput captures a dynamic team action request for policy evaluation
// TeamActionInput 承载一次动态 team action request，用于 policy 评估。
type TeamActionInput struct {
	Action    string         `json:"action"` // "recruit" | "retire"
	SessionID string         `json:"session_id"`
	UserID    string         `json:"user_id"`
	AgentID   string         `json:"agent_id"`
	Role      string         `json:"role"`
	Metadata  map[string]any `json:"metadata"`
}

// TeamActionDecision is a minimal decision wrapper
// TeamActionDecision 是精简的决策包装。
type TeamActionDecision struct {
	Allow  bool   `json:"allow"`
	Reason string `json:"reason"`
}

// AuthorizeTeamAction evaluates dynamic team actions via the policy engine
// AuthorizeTeamAction 通过 policy engine 评估动态 team action。
func AuthorizeTeamAction(ctx context.Context, in TeamActionInput) (TeamActionDecision, error) {
	if policyEngine == nil || !policyEngine.IsEnabled() {
		return TeamActionDecision{Allow: true, Reason: "policy engine disabled"}, nil
	}
	// Build policy input using existing structure
	// 复用既有结构体组装 policy 输入。
	pi := policyInputForTeam(in)
	start := time.Now()
	dec, err := policyEngine.Evaluate(ctx, pi)
	_ = start // metrics can be added in future; keep function minimal
	if err != nil {
		return TeamActionDecision{Allow: false, Reason: err.Error()}, nil
	}
	return TeamActionDecision{Allow: dec.Allow, Reason: dec.Reason}, nil
}

// policyInputForTeam adapts team action to the existing PolicyInput
// policyInputForTeam 把 team action 适配成既有的 PolicyInput。
func policyInputForTeam(in TeamActionInput) *policy.PolicyInput {
	ctx := map[string]any{}
	for k, v := range in.Metadata {
		ctx[k] = v
	}
	ctx["action"] = in.Action
	if in.Role != "" {
		ctx["role"] = in.Role
	}
	return &policy.PolicyInput{
		SessionID:   in.SessionID,
		UserID:      in.UserID,
		AgentID:     in.AgentID,
		Query:       "", // not applicable
		Mode:        "", // not applicable
		Context:     ctx,
		Environment: "dev",
		Timestamp:   time.Now(),
	}
}
