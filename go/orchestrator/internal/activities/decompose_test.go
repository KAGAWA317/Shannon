package activities

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"go.uber.org/zap"
)

func TestDecomposeTask_Success(t *testing.T) {
	// In restricted sandboxes, binding a local port may be disallowed.
	// 在受限 sandbox 中可能不允许绑定本地端口。
	// If creating an httptest server fails due to bind restrictions,
	// 若因 bind 限制导致 httptest server 创建失败，
	// skip this test rather than failing the suite.
	// 则跳过该测试而不是让整个测试套件失败。
	// Quick preflight: try binding a loopback port.
	// 先做一次预检：尝试绑定 loopback 端口。
	// If this fails, we assume the environment forbids listeners.
	// 若失败，则认为环境禁止监听端口。
	// Try IPv6 first to mirror httptest defaults; then IPv4.
	// 先试 IPv6 以匹配 httptest 默认行为，再试 IPv4。
	if ln6, err6 := net.Listen("tcp6", "[::1]:0"); err6 == nil {
		_ = ln6.Close()
	} else if ln4, err4 := net.Listen("tcp4", "127.0.0.1:0"); err4 == nil {
		_ = ln4.Close()
	} else {
		t.Skip("port binding not permitted in this environment; skipping")
	}

	// Mock Python service
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/agent/decompose" || r.Method != http.MethodPost {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		resp := DecompositionResult{
			Mode:            "standard",
			ComplexityScore: 0.55,
			Subtasks: []Subtask{
				{ID: "a", Description: "do A", Dependencies: []string{}, EstimatedTokens: 100},
				{ID: "b", Description: "do B", Dependencies: []string{"a"}, EstimatedTokens: 200},
			},
			TotalEstimatedTokens: 300,
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	defer srv.Close()

	os.Setenv("LLM_SERVICE_URL", srv.URL)
	defer os.Unsetenv("LLM_SERVICE_URL")

	// Create a test instance that can handle non-Temporal contexts
	// 创建可处理非 Temporal context 的测试实例
	a := &Activities{logger: zap.NewNop()}

	// Test the decomposition logic directly without the HTTP client interceptor issues
	// 直接测试 decomposition 逻辑，绕过 HTTP client interceptor 的问题
	input := DecompositionInput{
		Query:          "test query",
		Context:        map[string]any{"k": "v"},
		AvailableTools: []string{"tool1"},
	}

	out, err := a.DecomposeTask(context.Background(), input)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Mode != "standard" || len(out.Subtasks) != 2 || out.TotalEstimatedTokens != 300 {
		t.Fatalf("unexpected output: %+v", out)
	}
}
