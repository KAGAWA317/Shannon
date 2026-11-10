package models

import (
	"strings"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
)

// DetectProvider determines the provider from a model name.
// It first checks models.yaml for explicit provider mapping via model_catalog,
// then falls back to pattern matching for common model naming conventions.
//
// This function consolidates provider detection logic to avoid inconsistencies
// across the codebase. All provider detection should use this function.
//
// Note: Llama models are mapped to "ollama" (local deployment convention) even
// though models.yaml may list them under "meta" provider (Together AI).
// DetectProvider 由模型名判定 provider。
// 先查 models.yaml 的 model_catalog 显式映射，
// 再回退到常见模型命名惯例的模式匹配。
// 该函数统一 provider 判定逻辑，避免代码库各处不一致。
// 所有 provider 判定都应使用该函数。
// 注意：Llama 模型映射为 "ollama"（本地部署惯例），
// 尽管 models.yaml 可能把它们列在 "meta" provider（Together AI）下。
func DetectProvider(model string) string {
	if model == "" {
		return "unknown"
	}

	// Special case: Check for Groq provider first (before llama pattern matching)
	// Groq-specific models should be identified as "groq" not "ollama"
	// 特殊情况：先检查 Groq（在 llama 模式匹配之前）
	// Groq 专属模型应判定为 "groq" 而非 "ollama"
	ml := strings.ToLower(model)
	if strings.Contains(ml, "groq") && !strings.Contains(ml, "llama") {
		return "groq"
	}
	if strings.Contains(ml, "groq-llama") {
		return "groq"
	}

	// Strategy 1: Check if model exists in models.yaml model_catalog
	// The pricing package loads models.yaml, and we can infer provider from
	// the model_catalog structure (provider.model_name)
	// 策略 1：检查模型是否存在于 models.yaml 的 model_catalog
	// pricing 包会加载 models.yaml，可从 model_catalog 结构
	// （provider.model_name）推断 provider
	provider := detectProviderFromCatalog(model)
	if provider != "" {
		// Override: map "meta" provider to "ollama" for llama models
		// This maintains the codebase convention of using "ollama" for llama models
		// 覆盖：把 llama 模型的 "meta" provider 映射为 "ollama"
		// 以维持代码库中 llama 模型使用 "ollama" 的惯例
		if provider == "meta" && strings.Contains(ml, "llama") {
			return "ollama"
		}
		return provider
	}

	// Strategy 2: Fall back to pattern matching for common model names
	// 策略 2：回退到常见模型名的模式匹配
	return detectProviderFromPattern(model)
}

// detectProviderFromCatalog checks models.yaml model_catalog for explicit provider mapping.
// The config structure has: model_catalog -> provider -> model_name
// We search through all providers to find which one has this model.
// detectProviderFromCatalog 在 models.yaml 的 model_catalog 中查找显式 provider 映射。
// 配置结构为：model_catalog -> provider -> model_name
// 遍历所有 provider，找出包含该模型的那个。
func detectProviderFromCatalog(model string) string {
	// Try to infer provider by checking which tier contains this model
	// If model exists in model_tiers, we can get its provider
	// 尝试通过检查哪个 tier 包含该模型来推断 provider
	// 若模型存在于 model_tiers，即可得到其 provider
	tiers := []string{"small", "medium", "large"}
	for _, tier := range tiers {
		provider := pricing.GetProviderForModel(tier, model)
		if provider != "" {
			return provider
		}
	}
	return ""
}

// detectProviderFromPattern uses pattern matching to detect provider from model name.
// This is a fallback when the model is not found in models.yaml.
// detectProviderFromPattern 用模式匹配从模型名判定 provider。
// 这是模型不在 models.yaml 中时的兜底。
func detectProviderFromPattern(model string) string {
	ml := strings.ToLower(model)

	// Kimi / Moonshot models (must be before OpenAI check — "turbo" substring collision)
	// Kimi / Moonshot 模型（必须在 OpenAI 检查之前——"turbo" 子串会冲突）
	if strings.Contains(ml, "kimi") || strings.Contains(ml, "moonshot") {
		return "kimi"
	}

	// MiniMax models
	if strings.Contains(ml, "minimax") {
		return "minimax"
	}

	// OpenAI models
	if strings.Contains(ml, "gpt-") || strings.Contains(ml, "davinci") ||
		strings.Contains(ml, "turbo") || strings.Contains(ml, "text-") {
		return "openai"
	}

	// Anthropic models
	if strings.Contains(ml, "claude") || strings.Contains(ml, "opus") ||
		strings.Contains(ml, "sonnet") || strings.Contains(ml, "haiku") {
		return "anthropic"
	}

	// Google models
	if strings.Contains(ml, "gemini") || strings.Contains(ml, "palm") ||
		strings.Contains(ml, "bard") {
		return "google"
	}

	// DeepSeek models
	if strings.Contains(ml, "deepseek") {
		return "deepseek"
	}

	// Qwen models
	if strings.Contains(ml, "qwen") {
		return "qwen"
	}

	// X.AI models
	if strings.Contains(ml, "grok") {
		return "xai"
	}

	// Llama/Meta models - map to "ollama" (local deployment convention)
	// This matches the majority convention in the codebase
	// Llama/Meta 模型——映射为 "ollama"（本地部署惯例）
	// 与代码库中的多数惯例一致
	if strings.Contains(ml, "llama") || strings.Contains(ml, "codellama") {
		return "ollama"
	}

	// ZhipuAI models
	if strings.Contains(ml, "glm") {
		return "zai"
	}

	// Groq models
	if strings.Contains(ml, "groq") {
		return "groq"
	}

	return "unknown"
}
