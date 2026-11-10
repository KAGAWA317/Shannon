package activities

import (
	"context"
)

// AnalyzeComplexity is a legacy compatibility shim used by older workflow histories.
// It returns a DecompositionResult that includes mode/complexity and optional subtasks.
// Implementation delegates to DecomposeTask to avoid duplicating logic.
// AnalyzeComplexity 是历史遗留的兼容 shim，供旧版 workflow history 重放时调用。
// 返回的 DecompositionResult 包含 mode/complexity 及可选 subtasks；
// 实现直接委托给 DecomposeTask，避免重复一份逻辑。
func (a *Activities) AnalyzeComplexity(ctx context.Context, in DecompositionInput) (DecompositionResult, error) {
	// Delegate to DecomposeTask to keep behavior consistent.
	// 委托给 DecomposeTask，保证行为一致。
	return a.DecomposeTask(ctx, in)
}
