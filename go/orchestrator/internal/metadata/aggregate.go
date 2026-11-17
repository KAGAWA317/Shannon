package metadata

import (
    "sort"

    "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
    "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
    "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
)

// AggregateAgentMetadata extracts model, provider, and token information from agent results.
// It is a legacy helper used to build per-task metadata.
// NOTE: agent_usages emitted here is best-effort and deprecated for external consumption.
// For complete, authoritative usage and cost breakdowns, use model_breakdown from the token_usage table.
// AggregateAgentMetadata 从 agent 结果中提取模型、provider 与 token 信息。
// 它是用于构建按 task metadata 的旧辅助函数。
// NOTE: 此处产出的 agent_usages 为尽力而为，且已不推荐对外使用。
// 完整的权威用量与成本明细请使用 token_usage 表的 model_breakdown。
func AggregateAgentMetadata(agentResults []activities.AgentExecutionResult, synthesisTokens int) map[string]any {
	meta := make(map[string]any)

	if len(agentResults) == 0 {
		return meta
	}

	// Find the primary model (from first successful agent or most used model)
	// 确定主模型（取首个成功 agent 或使用次数最多的模型）
	var primaryModel string
	// Track providers reported by agents (prefer real provider over detection)
	// 记录各 agent 上报的 provider（优先真实 provider 而非探测结果）
	providerCounts := make(map[string]int)
	totalInputTokens := 0
	totalOutputTokens := 0
	totalTokensUsed := 0
	totalCacheRead := 0
	totalCacheCreation := 0
	modelCounts := make(map[string]int)

	// Per-agent usage details for visibility
	// 每个 agent 的用量明细，便于查看
	agentUsages := make([]map[string]any, 0, len(agentResults))

	// Track tool invocations across all agents
	// 统计所有 agent 的 tool 调用
	totalToolsInvoked := 0
	toolNames := make(map[string]bool)

	for _, result := range agentResults {
		// Count tool executions
		// 统计 tool 执行次数
		totalToolsInvoked += len(result.ToolExecutions)
		for _, te := range result.ToolExecutions {
			toolNames[te.Tool] = true
		}
		if result.Success && result.ModelUsed != "" {
			modelCounts[result.ModelUsed]++
			if primaryModel == "" {
				primaryModel = result.ModelUsed
			}
		}
		// Count provider if present
		// 若存在则统计 provider
		if result.Success {
			if p := result.Provider; p != "" {
				providerCounts[p]++
			}
		}
		totalInputTokens += result.InputTokens
		totalOutputTokens += result.OutputTokens
		totalTokensUsed += result.TokensUsed
		totalCacheRead += result.CacheReadTokens
		totalCacheCreation += result.CacheCreationTokens

		// Record per-agent usage
		// 记录每个 agent 的用量
        if result.Success {
            agentUsage := map[string]any{
                "agent_id": result.AgentID,
            }
            if result.ModelUsed != "" {
                agentUsage["model"] = result.ModelUsed
            }
            // Tokens and per-agent cost
            // token 与每个 agent 的成本
            var cost float64
            if result.InputTokens > 0 || result.OutputTokens > 0 {
                agentUsage["input_tokens"] = result.InputTokens
                agentUsage["output_tokens"] = result.OutputTokens
                total := result.InputTokens + result.OutputTokens
                agentUsage["total_tokens"] = total
                cost = pricing.CostForSplit(result.ModelUsed, result.InputTokens, result.OutputTokens)
            } else if result.TokensUsed > 0 {
                agentUsage["total_tokens"] = result.TokensUsed
                cost = pricing.CostForTokens(result.ModelUsed, result.TokensUsed)
            }
            agentUsage["cost_usd"] = cost
            if result.CacheReadTokens > 0 {
                agentUsage["cache_read_tokens"] = result.CacheReadTokens
            }
            if result.CacheCreationTokens > 0 {
                agentUsage["cache_creation_tokens"] = result.CacheCreationTokens
            }
            agentUsages = append(agentUsages, agentUsage)
        }
	}

	// Use the most frequently used model if available
	// 若有则使用出现次数最多的模型
	maxCount := 0
	for model, count := range modelCounts {
		if count > maxCount {
			maxCount = count
			primaryModel = model
		}
	}

	// Populate metadata
	// 填充 metadata
	if primaryModel != "" {
		meta["model"] = primaryModel
		meta["model_used"] = primaryModel
		// Prefer the most frequent non-empty provider from agent results; fallback to detection
		// 优先采用 agent 结果中出现最多的非空 provider；否则回退到探测
		topProvider := ""
		maxProv := 0
		for prov, c := range providerCounts {
			if c > maxProv {
				maxProv = c
				topProvider = prov
			}
		}
		if topProvider != "" {
			meta["provider"] = topProvider
		} else {
			meta["provider"] = models.DetectProvider(primaryModel)
		}
	}

	// Add cache stats
	// 添加 cache 统计
	if totalCacheRead > 0 {
		meta["cache_read_tokens"] = totalCacheRead
	}
	if totalCacheCreation > 0 {
		meta["cache_creation_tokens"] = totalCacheCreation
	}

	// Add token breakdown
	// Prefer split tokens when available, fallback to TokensUsed sum
	// 添加 token 明细
	// 有拆分数据时优先使用，否则回退到 TokensUsed 之和
	if totalInputTokens > 0 || totalOutputTokens > 0 {
		meta["input_tokens"] = totalInputTokens
		meta["output_tokens"] = totalOutputTokens
		totalTokens := totalInputTokens + totalOutputTokens + synthesisTokens
		meta["total_tokens"] = totalTokens
		// cache_aware_total_tokens parallels total_tokens but adds prompt-cache
		// classes for quota accounting (see migration 121). Synthesis tokens
		// have no cache component so they only appear in the input+output sum.
		// cache_aware_total_tokens 与 total_tokens 并列，但额外计入 prompt cache
		// 类别用于配额统计（见迁移 121）。synthesis token 无 cache 部分，
		// 因此只出现在 input+output 之和中。
		meta["cache_aware_total_tokens"] = totalTokens + totalCacheRead + totalCacheCreation
	} else if totalTokensUsed > 0 {
		// Fallback: use TokensUsed when splits unavailable
		// Estimate 60/40 split for input/output
		// 回退：拆分数据不可用时使用 TokensUsed
		// 按 60/40 估算 input/output
		totalTokens := totalTokensUsed + synthesisTokens
		meta["input_tokens"] = int(float64(totalTokensUsed) * 0.6)
		meta["output_tokens"] = int(float64(totalTokensUsed) * 0.4)
		meta["total_tokens"] = totalTokens
		meta["cache_aware_total_tokens"] = totalTokens + totalCacheRead + totalCacheCreation
	}

    // Do not set cost_usd here; server or workflow computes accurately from pricing
    // 此处不设置 cost_usd；由 server 或 workflow 按定价精确计算

	// Include per-agent usage details when present.
	// NOTE: agent_usages is deprecated; consumers should prefer model_breakdown.
	// 存在时附带每个 agent 的用量明细。
	// NOTE: agent_usages 已废弃；消费方应优先使用 model_breakdown。
	if len(agentUsages) > 0 {
		meta["agent_usages"] = agentUsages
	}

	// Include tool invocation count
	// 附带 tool 调用次数
	if totalToolsInvoked > 0 {
		meta["tools_invoked"] = totalToolsInvoked
		// Also include list of unique tools used (sorted for deterministic output)
		// 同时附带所用 tool 的去重列表（排序以保证输出确定性）
		tools := make([]string, 0, len(toolNames))
		for tool := range toolNames {
			tools = append(tools, tool)
		}
		sort.Strings(tools)
		meta["tools_used"] = tools
	}

	return meta
}
