package activities

import (
	"context"
	"strings"
)

// EvaluateResult provides a minimal, fast heuristic to score a response.
// It avoids external calls to keep workflows deterministic and CI friendly.
// EvaluateResult 用最小、快速的启发式给 response 打分；
// 刻意不做外部调用，以保证 workflow 确定性和 CI 可离线运行。
func (a *Activities) EvaluateResult(ctx context.Context, in EvaluateResultInput) (EvaluateResultOutput, error) { // nolint:revive
	// Very lightweight scoring:
	// - Empty or very short responses score low
	// - Otherwise return a neutral/high score with simple feedback
	// 极轻量打分：
	// - 空或过短的 response 给低分
	// - 其余给中性/高分，并附简单 feedback
	trimmed := strings.TrimSpace(in.Response)
	if trimmed == "" || len(trimmed) < 8 {
		return EvaluateResultOutput{
			Score:    0.3,
			Feedback: "Response seems too short or empty; add detail and specifics.",
		}, nil
	}

	// If criteria are provided, include them in feedback to guide improvements
	// 有 criteria 时把它写进 feedback，引导后续改进。
	var fb string
	if len(in.Criteria) > 0 {
		fb = "Check criteria: " + strings.Join(in.Criteria, ", ")
	} else {
		fb = "Looks reasonable; verify correctness and clarity."
	}

	return EvaluateResultOutput{
		Score:    0.85,
		Feedback: fb,
	}, nil
}
