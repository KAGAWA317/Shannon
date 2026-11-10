package pricing

import (
	"math"
	"testing"
)

// resetPricingGlobals clears package-level caches so the next lookup re-reads
// config/models.yaml. Safe to call from tests; registers a t.Cleanup to reset
// again on exit so ordering-dependent pollution doesn't leak.
// resetPricingGlobals 清除包级 cache，使下一次查找重新读取
// config/models.yaml。可在测试中安全调用；会注册 t.Cleanup 在退出时
// 再重置一次，避免依赖顺序的污染泄漏到其它测试。
func resetPricingGlobals(t *testing.T) {
	t.Helper()
	doReset := func() {
		mu.Lock()
		initialized = false
		loaded = nil
		mu.Unlock()
	}
	doReset()
	t.Cleanup(doReset)
}

// TestCacheSavings_xAI_NoDoubleCount verifies that when input_tokens already
// includes cached tokens (xAI, OpenAI, Kimi), the savings calculation uses
// input_tokens directly (not input+cache_read) — matching response_transformer.go
// and service.go logic after the 2026-04 fix.
// TestCacheSavings_xAI_NoDoubleCount 验证当 input_tokens 已
// 包含 cache token（xAI、OpenAI、Kimi）时，节省额计算直接使用
// input_tokens（而非 input+cache_read）——与 2026-04 修复后的
// response_transformer.go 和 service.go 逻辑一致。
func TestCacheSavings_xAI_NoDoubleCount(t *testing.T) {
	resetPricingGlobals(t)

	model := "grok-4-1-fast-non-reasoning"
	input, output, cacheRead := 15254, 2280, 1805

	withCache := CostForSplitWithCache(model, input, output, cacheRead, 0, 0, "xai")
	// Cache-inclusive providers: use input directly, no addition
	// cache 已含型 provider：直接使用 input，不做累加
	withoutCache := CostForSplit(model, input, output)
	savings := withoutCache - withCache

	// Correct savings = cache_read × input_price × 0.75 (the 75% discount)
	// 正确节省额 = cache_read × input_price × 0.75（即 75% 折扣）
	expected := float64(cacheRead) * 0.0002 / 1000.0 * 0.75
	if math.Abs(savings-expected) > 1e-6 {
		t.Errorf("xAI savings double-count: got %.6f, expected %.6f (diff %.9f)", savings, expected, savings-expected)
	}
}

// TestCacheSavings_MiniMax_AnthropicStyle verifies that MiniMax uses the
// Anthropic-style savings reconstruction: input_tokens EXCLUDES cached tokens,
// so the "without-cache" baseline must add them back. Mirrors the branches in
// server/response_transformer.go and server/service.go.
// TestCacheSavings_MiniMax_AnthropicStyle 验证 MiniMax 采用
// Anthropic 风格的节省额还原：input_tokens 不含 cache token，
// 因此“无 cache”基线必须把它们加回来。对应
// server/response_transformer.go 与 server/service.go 中的分支。
func TestCacheSavings_MiniMax_AnthropicStyle(t *testing.T) {
	resetPricingGlobals(t)

	model := "MiniMax-M2.7"
	input, output, cacheRead, cacheCreate := 5000, 1000, 3000, 0

	withCache := CostForSplitWithCache(model, input, output, cacheRead, cacheCreate, 0, "minimax")
	// Cache-separate providers: reconstruct baseline by adding cache tokens back.
	// cache 分离型 provider：通过把 cache token 加回来还原基线。
	withoutCache := CostForSplit(model, input+cacheRead+cacheCreate, output)
	savings := withoutCache - withCache

	// baseline - withCache = cache_read × input_price × (1 - 0.1) = cache_read × input_price × 0.9
	// input_per_1k for MiniMax-M2.7 is 0.00033.
	// baseline - withCache = cache_read × input_price × (1 - 0.1) = cache_read × input_price × 0.9
	// MiniMax-M2.7 的 input_per_1k 为 0.00033。
	expected := float64(cacheRead) * 0.00033 / 1000.0 * 0.9
	if math.Abs(savings-expected) > 1e-6 {
		t.Errorf("MiniMax savings: got %.6f, expected %.6f (diff %.9f)", savings, expected, savings-expected)
	}
}
