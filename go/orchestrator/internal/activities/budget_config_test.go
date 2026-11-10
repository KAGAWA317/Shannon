package activities

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/budget"
	"go.uber.org/zap"
)

// Test env overrides are wired into BudgetActivities/NewBudgetManager
// 验证 env override 已接入 BudgetActivities/NewBudgetManager
func TestBudgetEnvOverrides_WireIntoManager(t *testing.T) {
	// Set env overrides
	os.Setenv("BACKPRESSURE_THRESHOLD", "0.9")
	os.Setenv("MAX_BACKPRESSURE_DELAY_MS", "120")
	defer func() {
		os.Unsetenv("BACKPRESSURE_THRESHOLD")
		os.Unsetenv("MAX_BACKPRESSURE_DELAY_MS")
	}()

	acts := NewBudgetActivities(nil, zap.NewNop())

	// Case 1: 85% projected usage should NOT trigger backpressure with threshold 0.9
	// Case 1：threshold 为 0.9 时，85% 的预计使用率不应触发 backpressure
	// Prepare a session budget large enough so 85k = 85%
	// 准备足够大的 session budget，使 85k 恰好等于 85%
	acts.budgetManager.SetSessionBudget("s", &budget.TokenBudget{TaskBudget: 200000, SessionBudget: 100000})
	in := BudgetCheckInput{UserID: "u", SessionID: "s", TaskID: "t1", EstimatedTokens: 85000}
	res, err := acts.CheckTokenBudgetWithBackpressure(context.Background(), in)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.BackpressureActive {
		t.Fatalf("expected no backpressure at 85%% with threshold 0.9, got %+v", res)
	}

	// Case 2: 100% projected usage should return max delay from env (120ms)
	// Case 2：100% 的预计使用率应返回 env 配置的最大 delay（120ms）
	acts.budgetManager.SetSessionBudget("s2", &budget.TokenBudget{TaskBudget: 200000, SessionBudget: 100000})
	in2 := BudgetCheckInput{UserID: "u2", SessionID: "s2", TaskID: "t2", EstimatedTokens: 100000}
	start := time.Now()
	res2, err := acts.CheckTokenBudgetWithBackpressure(context.Background(), in2)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !res2.BackpressureActive || res2.BackpressureDelay != 120 {
		t.Fatalf("expected backpressure with 120ms delay, got %+v", res2)
	}
	// Activity should NOT block - it returns immediately with delay value for workflow to handle
	// Activity 不应阻塞——立即返回 delay 值，由 workflow 处理等待
	// This ensures Temporal workers are not blocked
	// 这样可确保 Temporal worker 不被阻塞
	if elapsed > 50*time.Millisecond {
		t.Fatalf("activity should return immediately without blocking, but took %v", elapsed)
	}
}
