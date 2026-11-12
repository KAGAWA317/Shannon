package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"go.uber.org/zap"
)

// CheckCompressionNeededInput checks if compression should be triggered
// CheckCompressionNeededInput 是判断是否需要触发 compression 的入参。
type CheckCompressionNeededInput struct {
	SessionID       string `json:"session_id"`
	MessageCount    int    `json:"message_count"`
	EstimatedTokens int    `json:"estimated_tokens"`
	ModelTier       string `json:"model_tier"`
}

// CheckCompressionNeededResult indicates if compression should proceed
// CheckCompressionNeededResult 表示是否应该继续做 compression。
type CheckCompressionNeededResult struct {
	ShouldCompress bool   `json:"should_compress"`
	Reason         string `json:"reason"`
}

// CheckCompressionNeeded determines if context compression should be triggered
// This activity checks token thresholds and rate limits via session state
// CheckCompressionNeeded 判断是否应触发 context compression；
// 该 activity 通过 session 状态检查 token 阈值与频率限制。
// CheckCompressionNeeded 判定是否应触发 context 压缩；
// 该 activity 通过 session 状态检查 token 阈值与频率限制（rate limit）。
func (a *Activities) CheckCompressionNeeded(ctx context.Context, in CheckCompressionNeededInput) (CheckCompressionNeededResult, error) {
	logger := zap.L()

	// Get model window and threshold
	// 取 model 的 context 窗口与阈值。
	modelWindow := GetModelWindowSize(in.ModelTier)
	threshold := int(float64(modelWindow) * 0.75)

	// Check token threshold first
	// 先检查 token 阈值。
	if in.EstimatedTokens < threshold {
		metrics.CompressionEvents.WithLabelValues("skipped").Inc()
		return CheckCompressionNeededResult{
			ShouldCompress: false,
			Reason:         "Below token threshold",
		}, nil
	}

	// Check rate limiting via session state
	// 通过 session 状态检查频率限制。
	if a.sessionManager != nil && in.SessionID != "" {
		sessData, err := a.sessionManager.GetSession(ctx, in.SessionID)
		if err == nil && sessData != nil && sessData.Metadata != nil {
			// Check compression state for rate limiting
			// 读取 compression 状态以判断是否触发过于频繁。
			if compState, ok := sessData.Metadata["compression_state"].(map[string]any); ok {
				// Check message count since last compression
				// 检查距上次 compression 的消息条数。
				if lastCount, ok := compState["last_message_count"].(int); ok {
					messagesSince := in.MessageCount - lastCount
					if messagesSince < 20 { // Minimum 20 new messages
						// 至少积累 20 条新消息，避免压缩过于频繁。
						metrics.CompressionEvents.WithLabelValues("skipped").Inc()
						return CheckCompressionNeededResult{
							ShouldCompress: false,
							Reason:         "Insufficient new messages since last compression",
						}, nil
					}
				}

				// Check time since last compression
				// 检查距上次 compression 的时间间隔。
				if lastTime, ok := compState["last_compressed_at"].(int64); ok {
					timeSince := time.Since(time.Unix(lastTime, 0))
					if timeSince < 30*time.Minute { // Minimum 30 minutes
						// 距上次压缩至少 30 分钟。
						metrics.CompressionEvents.WithLabelValues("skipped").Inc()
						return CheckCompressionNeededResult{
							ShouldCompress: false,
							Reason:         "Too soon since last compression",
						}, nil
					}
				}
			}
		}
	}

	logger.Info("Compression check passed",
		zap.String("session_id", in.SessionID),
		zap.Int("tokens", in.EstimatedTokens),
		zap.Int("threshold", threshold),
		zap.Int("messages", in.MessageCount),
	)

	metrics.CompressionEvents.WithLabelValues("triggered").Inc()
	return CheckCompressionNeededResult{
		ShouldCompress: true,
		Reason:         "Token threshold exceeded and rate limits passed",
	}, nil
}

// UpdateCompressionStateInput updates session after compression
// UpdateCompressionStateInput 是 compression 之后更新 session 的入参。
type UpdateCompressionStateInput struct {
	SessionID    string `json:"session_id"`
	MessageCount int    `json:"message_count"`
}

// UpdateCompressionStateResult indicates update success
// UpdateCompressionStateResult 表示更新是否成功。
type UpdateCompressionStateResult struct {
	Updated bool   `json:"updated"`
	Error   string `json:"error,omitempty"`
}

// UpdateCompressionStateActivity updates the compression state in session after successful compression
// UpdateCompressionStateActivity 在 compression 成功后更新 session 中的 compression 状态。
func (a *Activities) UpdateCompressionStateActivity(ctx context.Context, in UpdateCompressionStateInput) (UpdateCompressionStateResult, error) {
	if a.sessionManager == nil || in.SessionID == "" {
		return UpdateCompressionStateResult{Updated: false}, nil
	}

	// Get current session
	// 取当前 session。
	sessData, err := a.sessionManager.GetSession(ctx, in.SessionID)
	if err != nil || sessData == nil {
		return UpdateCompressionStateResult{
			Updated: false,
			Error:   "Session not found",
		}, nil
	}

	// Get or create compression state
	// 取出或创建 compression 状态。
	totalCompressions := 0
	if sessData.Metadata == nil {
		sessData.Metadata = make(map[string]any)
	}

	if compState, ok := sessData.Metadata["compression_state"].(map[string]any); ok {
		if total, ok := compState["total_compressions"].(int); ok {
			totalCompressions = total
		}
	}
	totalCompressions++

	// Update compression state
	// 更新 compression 状态。
	sessData.Metadata["compression_state"] = map[string]any{
		"last_compressed_at": time.Now().Unix(),
		"last_message_count": in.MessageCount,
		"total_compressions": totalCompressions,
	}

	// Persist the updated session
	// 持久化更新后的 session。
	if err := a.sessionManager.UpdateSession(ctx, sessData); err != nil {
		zap.L().Error("Failed to persist compression state",
			zap.String("session_id", in.SessionID),
			zap.Error(err),
		)
		return UpdateCompressionStateResult{
			Updated: false,
			Error:   fmt.Sprintf("Failed to persist: %v", err),
		}, nil
	}

	zap.L().Info("Updated and persisted compression state",
		zap.String("session_id", in.SessionID),
		zap.Int("message_count", in.MessageCount),
		zap.Int("total_compressions", totalCompressions),
	)

	return UpdateCompressionStateResult{Updated: true}, nil
}
