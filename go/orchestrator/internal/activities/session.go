package activities

import (
	"context"
	"fmt"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	"go.uber.org/zap"
)

// UpdateSessionResult updates the session with final results from workflow execution
// UpdateSessionResult 把 workflow 执行的最终结果写回 session。
func (a *Activities) UpdateSessionResult(ctx context.Context, input SessionUpdateInput) (SessionUpdateResult, error) {
	a.logger.Info("Updating session with results",
		zap.String("session_id", input.SessionID),
		zap.Int("tokens_used", input.TokensUsed),
		zap.Int("agents_used", input.AgentsUsed),
		zap.Int("result_length", len(input.Result)),
	)

	// Validate input
	// 校验输入。
	if input.SessionID == "" {
		return SessionUpdateResult{
			Success: false,
			Error:   "session ID is required",
		}, fmt.Errorf("session ID is required")
	}

	// Get session from manager
	// 从 manager 取 session。
	sess, err := a.sessionManager.GetSession(ctx, input.SessionID)
	if err != nil {
		a.logger.Error("Failed to get session", zap.Error(err))
		return SessionUpdateResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to get session: %v", err),
		}, err
	}

	// Update token usage and cost (centralized pricing; prefer per-agent, then model, then default).
	// When AgentUsage carries cache_read / cache_creation, use CostForSplitWithCache so the
	// computed cost reflects prompt-cache discount/premium pricing.
	// 更新 token 用量与开销（集中计费；优先按 agent，其次按 model，最后用默认值）。
	// 当 AgentUsage 带有 cache_read / cache_creation 时，改用 CostForSplitWithCache，
	// 让计算出的开销体现 prompt cache 的折扣/溢价。
	costUSD := input.CostUSD
	if costUSD <= 0 {
		if len(input.AgentUsage) > 0 {
			var total float64
			for _, au := range input.AgentUsage {
				model := au.Model
				if model == "" {
					a.logger.Warn("Pricing fallback used (missing model)", zap.Int("tokens", au.Tokens))
				} else if _, ok := pricing.PricePerTokenForModel(model); !ok {
					a.logger.Warn("Pricing model not found; using default", zap.String("model", model), zap.Int("tokens", au.Tokens))
				}
				switch {
				case au.CacheReadTokens > 0 || au.CacheCreationTokens > 0 || au.CacheCreation1hTokens > 0:
					total += pricing.CostForSplitWithCache(
						model, au.InputTokens, au.OutputTokens,
						au.CacheReadTokens, au.CacheCreationTokens, au.CacheCreation1hTokens, au.Provider,
					)
				case au.InputTokens > 0 || au.OutputTokens > 0:
					total += pricing.CostForSplit(model, au.InputTokens, au.OutputTokens)
				default:
					total += pricing.CostForTokens(model, au.Tokens)
				}
			}
			costUSD = total
		} else if input.ModelUsed != "" {
			if _, ok := pricing.PricePerTokenForModel(input.ModelUsed); !ok {
				a.logger.Warn("Pricing model not found; using default", zap.String("model", input.ModelUsed), zap.Int("tokens", input.TokensUsed))
			}
			// When CacheAwareTokensUsed is set, price the cache-inclusive total
			// so the model-only fallback doesn't undercount prompt-cache cost.
			tokensForPricing := input.TokensUsed
			if input.CacheAwareTokensUsed > tokensForPricing {
				tokensForPricing = input.CacheAwareTokensUsed
			}
			costUSD = pricing.CostForTokens(input.ModelUsed, tokensForPricing)
		} else {
			tokensForPricing := input.TokensUsed
			if input.CacheAwareTokensUsed > tokensForPricing {
				tokensForPricing = input.CacheAwareTokensUsed
			}
			costUSD = float64(tokensForPricing) * pricing.DefaultPerToken()
		}
	}

	// Prefer cache-aware token total for the session counter so prompt-cache
	// cost shows up in session.TotalTokensUsed and downstream context fields.
	// session 计数器优先使用 cache-aware 的 token 总数，让 prompt cache 开销
	// 能反映到 session.TotalTokensUsed 和下游的 context 字段里。
	tokensForSession := input.TokensUsed
	if input.CacheAwareTokensUsed > tokensForSession {
		tokensForSession = input.CacheAwareTokensUsed
	} else if len(input.AgentUsage) > 0 {
		// Derive cache-aware total from AgentUsage when caller didn't precompute it.
		// 调用方没预先计算时，从 AgentUsage 推导 cache-aware 总量。
		var derived int
		for _, au := range input.AgentUsage {
			derived += au.InputTokens + au.OutputTokens + au.CacheReadTokens + au.CacheCreationTokens
		}
		if derived > tokensForSession {
			tokensForSession = derived
		}
	}
	sess.UpdateTokenUsage(tokensForSession, costUSD)

	// Record metrics
	// 记录 metric。
	metrics.RecordSessionTokens(input.TokensUsed)

	// Add assistant message to history directly on sess to avoid stale overwrite
	// 直接往 sess 的 history 追加 assistant 消息，避免基于旧副本覆盖。
	if input.Result != "" {
		message := session.Message{
			ID:         fmt.Sprintf("msg-%d", time.Now().UnixNano()),
			Role:       "assistant",
			Content:    input.Result,
			Timestamp:  time.Now(),
			TokensUsed: input.TokensUsed,
			CostUSD:    costUSD,
		}
		sess.History = append(sess.History, message)

		// Enforce max history limit (default 500)
		// 限制 history 最大条数（默认 500）。
		const maxHistory = 500
		if len(sess.History) > maxHistory {
			sess.History = sess.History[len(sess.History)-maxHistory:]
		}
	}

	// Maintain conversational context for follow-up tasks
	// 维护对话 context，供后续 task 使用。
	sess.SetContextValue("last_updated_at", time.Now().UTC().Format(time.RFC3339))
	sess.SetContextValue("total_tokens_used", sess.TotalTokensUsed)
	sess.SetContextValue("total_cost_usd", sess.TotalCostUSD)
	if input.TokensUsed > 0 {
		sess.SetContextValue("last_tokens_used", input.TokensUsed)
	}
	if input.AgentsUsed > 0 {
		sess.SetContextValue("last_agents_used", input.AgentsUsed)
	}
	if input.Result != "" {
		sess.SetContextValue("last_response", util.TruncateString(input.Result, 500, false))
	}

	// Update session metadata
	// 更新 session metadata。
	if sess.Metadata == nil {
		sess.Metadata = make(map[string]interface{})
	}
	sess.Metadata["last_agents_used"] = input.AgentsUsed
	sess.Metadata["last_workflow_result"] = util.TruncateString(input.Result, 200, false)

	// Save session back to Redis
	// 把 session 写回 Redis。
	if err := a.sessionManager.UpdateSession(ctx, sess); err != nil {
		a.logger.Error("Failed to update session", zap.Error(err))
		return SessionUpdateResult{
			Success: false,
			Error:   fmt.Sprintf("Failed to update session: %v", err),
		}, err
	}

	a.logger.Info("Session updated successfully with token tracking",
		zap.String("session_id", input.SessionID),
		zap.Int("tokens_added", input.TokensUsed),
		zap.Float64("cost_added", costUSD),
		zap.Int("total_tokens", sess.TotalTokensUsed),
		zap.Float64("total_cost", sess.TotalCostUSD),
	)

	return SessionUpdateResult{
		Success: true,
		Error:   "",
	}, nil
}

// Helper function to truncate strings for logging
// Truncation unified in util.TruncateString
// 用于日志截断字符串的辅助函数。
// 截断逻辑已统一到 util.TruncateString。
