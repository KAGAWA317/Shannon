package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/embeddings"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/vectordb"
	"github.com/google/uuid"
	"go.temporal.io/sdk/activity"
	"go.uber.org/zap"
)

// CompressContextInput requests summary for long conversation history
// CompressContextInput 用于为过长的对话历史请求一份 summary。
type CompressContextInput struct {
	SessionID string `json:"session_id"`
	TenantID  string `json:"tenant_id,omitempty"`
	// History messages as pairs: {role, content}
	// History 为消息列表，每条消息包含 role 与 content 字段。
	History      []map[string]string `json:"history"`
	TargetTokens int                 `json:"target_tokens"`
	// Parent workflow ID for unified event streaming
	// 父 workflow ID，用于统一 event stream 归属。
	ParentWorkflowID string `json:"parent_workflow_id,omitempty"`
}

// CompressContextResult returns the summary and persistence status
// CompressContextResult 返回 summary 及其持久化状态。
type CompressContextResult struct {
	Summary          string `json:"summary"`
	Stored           bool   `json:"stored"`
	Error            string `json:"error,omitempty"`
	OriginalTokens   int    `json:"original_tokens,omitempty"`   // Token count before compression
	CompressedTokens int    `json:"compressed_tokens,omitempty"` // Token count after compression
}

// CompressAndStoreContext summarizes history via llm-service and stores it in Qdrant
// CompressAndStoreContext 通过 llm-service 生成 history 的 summary，并存入 Qdrant。
func CompressAndStoreContext(ctx context.Context, in CompressContextInput) (CompressContextResult, error) {
	// Use activity logger for Temporal correlation
	// 用 activity logger，便于 Temporal 侧关联日志。
	activity.GetLogger(ctx).Info("Compressing context", "session_id", in.SessionID, "history_length", len(in.History))
	logger := zap.L()
	if len(in.History) == 0 {
		return CompressContextResult{Summary: "", Stored: false}, nil
	}

	// Mark compression triggered
	// 标记已触发 compression。
	metrics.CompressionEvents.WithLabelValues("triggered").Inc()

	// Estimate original tokens from history
	// 估算 history 的原始 token 数。
	originalTokens := 0
	for _, msg := range in.History {
		if content, ok := msg["content"]; ok {
			// Conservative estimate: ~4 chars per token
			// 保守估算：约 4 个字符对应 1 个 token。
			originalTokens += len(content) / 4
		}
	}
	// Add overhead for formatting
	// 追加格式化带来的额外开销。
	originalTokens += len(in.History) * 5

	// Call llm-service /context/compress
	// 调用 llm-service 的 /context/compress。
	base := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := base + "/context/compress"
	reqBody := map[string]any{
		"messages":      in.History,
		"target_tokens": in.TargetTokens,
	}
	buf, _ := json.Marshal(reqBody)
	client := &http.Client{Timeout: 8 * time.Second, Transport: interceptors.NewWorkflowHTTPRoundTripper(nil)}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(buf))
	if err != nil {
		return CompressContextResult{Summary: "", Stored: false, Error: err.Error()}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	if in.ParentWorkflowID != "" {
		req.Header.Set("X-Parent-Workflow-ID", in.ParentWorkflowID)
	}
	resp, err := client.Do(req)
	if err != nil {
		logger.Warn("Context compress HTTP error", zap.Error(err))
		return CompressContextResult{Summary: "", Stored: false, Error: err.Error()}, nil
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logger.Warn("Context compress non-2xx", zap.Int("status", resp.StatusCode))
		metrics.CompressionEvents.WithLabelValues("failed").Inc()
		return CompressContextResult{Summary: "", Stored: false, Error: fmt.Sprintf("status_%d", resp.StatusCode)}, nil
	}
	var out struct {
		Summary string `json:"summary"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		metrics.CompressionEvents.WithLabelValues("failed").Inc()
		return CompressContextResult{Summary: "", Stored: false, Error: err.Error()}, nil
	}

	summary := out.Summary
	// Basic PII redaction for safety (best-effort)
	// 出于安全做基础 PII 脱敏，尽力而为。
	summary = redactPII(summary)
	if summary == "" {
		// No summary produced; count as skipped
		// 未生成 summary，按跳过处理。
		metrics.CompressionEvents.WithLabelValues("skipped").Inc()
		return CompressContextResult{Summary: "", Stored: false, OriginalTokens: originalTokens}, nil
	}

	// Estimate compressed tokens from summary
	// 按 summary 估算压缩后的 token 数。
	compressedTokens := len(summary)/4 + 10 // Conservative estimate + overhead

	// Record compression ratio metric if we achieved compression
	// 真正压下来了就记录 compression ratio 指标。
	if originalTokens > 0 && compressedTokens > 0 {
		ratio := float64(originalTokens) / float64(compressedTokens)
		metrics.CompressionRatio.Observe(ratio)
		saved := originalTokens - compressedTokens
		if saved > 0 {
			metrics.CompressionTokensSaved.Observe(float64(saved))
		}
		logger.Info("Context compression completed",
			zap.Int("original_tokens", originalTokens),
			zap.Int("compressed_tokens", compressedTokens),
			zap.Float64("compression_ratio", ratio),
		)
	}

	// Generate embedding for summary and upsert to Qdrant
	// 为 summary 生成 embedding 并 upsert 到 Qdrant。
	if svc := embeddings.Get(); svc != nil {
		if vdb := vectordb.Get(); vdb != nil {
			vec, err := svc.GenerateEmbedding(ctx, summary, "")
			if err != nil {
				logger.Warn("Embedding generation failed for summary", zap.Error(err))
				return CompressContextResult{
					Summary:          summary,
					Stored:           false,
					Error:            "embed_failed",
					OriginalTokens:   originalTokens,
					CompressedTokens: compressedTokens,
				}, nil
			}
			// Generate a deterministic summary ID for deduplication
			// 生成确定性的 summary ID，用于去重。
			summaryID := uuid.New().String()
			payload := map[string]any{
				"session_id": in.SessionID,
				"tenant_id":  in.TenantID, // Add tenant_id for filtering
				// 带上 tenant_id 以便按租户过滤。
				"type":       "summary",
				"timestamp":  time.Now().Unix(),
				"content":    summary,   // Changed from "text" to "content" for consistency
				// 字段由 "text" 改为 "content"，与其他写入保持一致。
				"summary_id": summaryID, // Add summary_id for dedup
				// 带 summary_id 以便去重。
			}
			if _, err := vdb.UpsertSummaryEmbedding(ctx, vec, payload); err != nil {
				logger.Warn("Qdrant upsert failed for summary", zap.Error(err))
				return CompressContextResult{
					Summary:          summary,
					Stored:           false,
					Error:            "upsert_failed",
					OriginalTokens:   originalTokens,
					CompressedTokens: compressedTokens,
				}, nil
			}
			return CompressContextResult{
				Summary:          summary,
				Stored:           true,
				OriginalTokens:   originalTokens,
				CompressedTokens: compressedTokens,
			}, nil
		}
	}

	// If no vectordb/embeddings, return summary without storage
	// 没有 vectordb/embeddings 时只返回 summary，不做存储。
	return CompressContextResult{
		Summary:          summary,
		Stored:           false,
		OriginalTokens:   originalTokens,
		CompressedTokens: compressedTokens,
	}, nil
}

// redactPII performs comprehensive PII redaction on summaries
// redactPII 对 summary 做较全面的 PII 脱敏。
func redactPII(s string) string {
	if s == "" {
		return s
	}

	// Email addresses
	emailRe := regexp.MustCompile(`(?i)[a-z0-9._%+\-]+@[a-z0-9.\-]+\.[a-z]{2,}`)
	s = emailRe.ReplaceAllString(s, "[REDACTED_EMAIL]")

	// Phone numbers (various formats)
	// 电话号码（多种格式）。
	phoneRe := regexp.MustCompile(`(?i)(\+?\d[\d\s\-()]{8,}\d)`)
	s = phoneRe.ReplaceAllString(s, "[REDACTED_PHONE]")

	// SSN (US Social Security Numbers)
	// SSN（美国社会安全号）。
	ssnRe := regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	s = ssnRe.ReplaceAllString(s, "[REDACTED_SSN]")

	// Credit card numbers (basic pattern)
	// 信用卡号（基础模式）。
	ccRe := regexp.MustCompile(`\b(?:\d{4}[-\s]?){3}\d{4}\b`)
	s = ccRe.ReplaceAllString(s, "[REDACTED_CC]")

	// IP addresses (IPv4)
	ipRe := regexp.MustCompile(`\b(?:\d{1,3}\.){3}\d{1,3}\b`)
	s = ipRe.ReplaceAllString(s, "[REDACTED_IP]")

	// API keys and tokens (common patterns)
	// API key 与 token（常见模式）。
	apiKeyRe := regexp.MustCompile(`(?i)\b(api[_-]?key|apikey|access[_-]?token|bearer|token)[\s:=]+[\w-]{20,}\b`)
	s = apiKeyRe.ReplaceAllString(s, "[REDACTED_API_KEY]")

	// Passwords and secrets (when explicitly mentioned)
	// 密码与密钥（显式出现时）。
	secretRe := regexp.MustCompile(`(?i)\b(password|secret|pwd|passwd)[\s:=]+\S{8,}\b`)
	s = secretRe.ReplaceAllString(s, "[REDACTED_SECRET]")

	return s
}
