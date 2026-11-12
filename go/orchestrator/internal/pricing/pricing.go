package pricing

import (
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	pmetrics "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
)

// providerRef 对应 models.yaml model_tiers 下的一条 provider 记录。
type providerRef struct {
	Provider string `yaml:"provider"`
	Model    string `yaml:"model"`
	Priority int    `yaml:"priority"`
}

// Config structure for pricing section in config/models.yaml
// config/models.yaml 中 pricing 段的配置结构
type config struct {
	ModelTiers struct {
		Small struct {
			Providers []providerRef `yaml:"providers"`
		} `yaml:"small"`
		Medium struct {
			Providers []providerRef `yaml:"providers"`
		} `yaml:"medium"`
		Large struct {
			Providers []providerRef `yaml:"providers"`
		} `yaml:"large"`
	} `yaml:"model_tiers"`
	Pricing struct {
		Defaults struct {
			CombinedPer1K float64 `yaml:"combined_per_1k"`
		} `yaml:"defaults"`
		Models map[string]map[string]struct {
			InputPer1K    float64 `yaml:"input_per_1k"`
			OutputPer1K   float64 `yaml:"output_per_1k"`
			CombinedPer1K float64 `yaml:"combined_per_1k"`
		} `yaml:"models"`
	} `yaml:"pricing"`
	// ModelCatalog maps provider → model → metadata (used for zero-cost provider detection)
	// ModelCatalog 映射 provider → model → metadata（用于零成本 provider 判定）
	ModelCatalog map[string]map[string]any `yaml:"model_catalog"`
}

var (
	mu          sync.RWMutex
	loaded      *config
	initialized bool
)

// default locations inside containers / local dev
var defaultPaths = []string{
	os.Getenv("MODELS_CONFIG_PATH"),
	"/app/config/models.yaml",
	"./config/models.yaml",
	"../../config/models.yaml",    // from go/orchestrator
	"../../../config/models.yaml", // from go/orchestrator/internal/*
}

// findUpConfig searches parent directories for config/models.yaml starting at CWD.
// findUpConfig 从当前工作目录开始向上搜索父目录中的 config/models.yaml。
func findUpConfig() (string, bool) {
	wd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	// Walk upwards up to 6 levels to be safe in test/package paths
	// 向上最多遍历 6 层，以适配测试/包路径
	for i := 0; i < 6; i++ {
		cand := filepath.Join(wd, "config", "models.yaml")
		if _, err := os.Stat(cand); err == nil {
			return cand, true
		}
		// Also try repo root style: look for a sibling "config/models.yaml" while we traverse up
		// 同时尝试仓库根目录形式：向上遍历时查找同级的 "config/models.yaml"
		wd = filepath.Dir(wd)
	}
	return "", false
}

// loadLocked loads the configuration - must be called while holding mu.Lock()
// loadLocked 加载配置——必须在持有 mu.Lock() 时调用
func loadLocked() {
	var cfg config
	// 1) Try explicit and common defaults
	for _, p := range defaultPaths {
		if p == "" {
			continue
		}
		data, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var tmp config
		if err := yaml.Unmarshal(data, &tmp); err != nil {
			log.Printf("WARNING: Failed to unmarshal pricing config from %s: %v", p, err)
			continue
		}
		cfg = tmp
		log.Printf("Loaded pricing configuration from %s", p)
		break
	}
	// 2) If not loaded yet, search upwards from current working directory
	// 2) 若尚未加载，从当前工作目录向上搜索
	if cfg.Pricing.Defaults.CombinedPer1K == 0 && len(cfg.Pricing.Models) == 0 {
		if path, ok := findUpConfig(); ok {
			if data, err := os.ReadFile(path); err == nil {
				var tmp config
				if err := yaml.Unmarshal(data, &tmp); err == nil {
					cfg = tmp
					log.Printf("Loaded pricing configuration from %s", path)
				}
			}
		}
	}
	// No locking needed - caller must hold the lock
	// 无需加锁——调用方必须已持有锁
	loaded = &cfg
	initialized = true
}

func get() *config {
	mu.RLock()
	if initialized {
		defer mu.RUnlock()
		return loaded
	}
	mu.RUnlock()

	// Need to initialize - use write lock to prevent races
	// 需要初始化——用写锁防止竞态
	mu.Lock()
	defer mu.Unlock()
	// Double-check after acquiring write lock
	if !initialized {
		loadLocked() // Call the version that doesn't lock
	}
	return loaded
}

// ModifiedTime returns the mtime of the config file used (best-effort)
// ModifiedTime 返回所用配置文件的 mtime（尽力而为）
func ModifiedTime() time.Time {
	for _, p := range defaultPaths {
		if p == "" {
			continue
		}
		if st, err := os.Stat(p); err == nil {
			return st.ModTime()
		}
	}
	return time.Time{}
}

// Reload forces a re-read of pricing configuration.
// Thread-safe: uses mutex to prevent race conditions.
// Reload 强制重新读取定价配置。
// 线程安全：用互斥锁防止竞态。
func Reload() {
	mu.Lock()
	defer mu.Unlock()

	// Mark as uninitialized to force reload
	initialized = false
	// Load new configuration
	loadLocked()
}

// DefaultPerToken returns default combined price per token
// DefaultPerToken 返回默认的每 token 合计价格
func DefaultPerToken() float64 {
	cfg := get()
	if cfg.Pricing.Defaults.CombinedPer1K > 0 {
		return cfg.Pricing.Defaults.CombinedPer1K / 1000.0
	}
	// Fallback: $0.005 per 1K tokens (generic default)
	// 回退：每 1K token $0.005（通用默认值）
	return 0.000005
}

// PricePerTokenForModel returns combined price per token for a model if available
// PricePerTokenForModel 返回某模型的每 token 合计价格（若有）
func PricePerTokenForModel(model string) (float64, bool) {
	if model == "" {
		return 0, false
	}
	cfg := get()
	for _, models := range cfg.Pricing.Models {
		if m, ok := models[model]; ok {
			if m.CombinedPer1K > 0 {
				return m.CombinedPer1K / 1000.0, true
			}
			// If only input/output provided, approximate combined as average
			// 若只提供 input/output 价格，按平均值近似合计价
			if m.InputPer1K > 0 && m.OutputPer1K > 0 {
				return ((m.InputPer1K + m.OutputPer1K) / 2.0) / 1000.0, true
			}
		}
	}
	return 0, false
}

// CostForTokens returns cost in USD for total tokens with optional model
// CostForTokens 返回总 token 的美元成本，可选指定模型
func CostForTokens(model string, tokens int) float64 {
	// Validate token count
	if tokens < 0 {
		tokens = 0 // Treat negative as zero to avoid negative costs
		// 负数按 0 处理，避免负成本
	}

	if price, ok := PricePerTokenForModel(model); ok {
		return float64(tokens) * price
	}
	if model == "" {
		pmetrics.PricingFallbacks.WithLabelValues("missing_model").Inc()
	} else {
		pmetrics.PricingFallbacks.WithLabelValues("unknown_model").Inc()
	}
	return float64(tokens) * DefaultPerToken()
}

// CostForSplit computes cost using input/output token split when available.
// Falls back to combined pricing or default if model not found.
// CostForSplit 在可拆分 input/output token 时按拆分计价。
// 模型未找到时回退到合计价或默认价。
func CostForSplit(model string, inputTokens, outputTokens int) float64 {
	// Validate token counts
	if inputTokens < 0 {
		inputTokens = 0
	}
	if outputTokens < 0 {
		outputTokens = 0
	}

	cfg := get()
	// Find model pricing
	for _, models := range cfg.Pricing.Models {
		if m, ok := models[model]; ok {
			in := m.InputPer1K
			out := m.OutputPer1K
			// Explicit zero pricing (e.g. local Ollama models) → return 0, don't fallback
			// 显式零价（如本地 Ollama 模型）→ 返回 0，不回退
			if in == 0 && out == 0 && m.CombinedPer1K == 0 {
				return 0
			}
			if in > 0 && out > 0 {
				return (float64(inputTokens)/1000.0)*in + (float64(outputTokens)/1000.0)*out
			}
			// If only combined provided, approximate
			if m.CombinedPer1K > 0 {
				return (float64(inputTokens+outputTokens) / 1000.0) * m.CombinedPer1K
			}
			break
		}
	}
	// Check if model belongs to a zero-cost provider (e.g. Ollama local models)
	// before falling back to default pricing
	// 在回退到默认定价之前，先判断模型是否属于零成本 provider（如 Ollama 本地模型）
	if isZeroCostProvider(model) {
		return 0
	}

	// Unknown or missing model -> fallback
	if model == "" {
		pmetrics.PricingFallbacks.WithLabelValues("missing_model").Inc()
	} else {
		pmetrics.PricingFallbacks.WithLabelValues("unknown_model").Inc()
	}
	return float64(inputTokens+outputTokens) * DefaultPerToken()
}

// CostForSplitWithCache computes cost including prompt cache pricing adjustments.
// For Anthropic/MiniMax: input_tokens excludes cache; cache_read at 10%,
// 5m creation at 125%, 1h creation at 200% of input price.
// cacheCreation1hTokens is the subset of cacheCreationTokens that used 1h TTL.
// For Kimi/xAI: input_tokens includes cache; cache_read gets 75% discount (billed at 25%).
// For OpenAI (default): input_tokens includes cache; cache_read gets 50% discount.
// CostForSplitWithCache 计算成本，包含 prompt cache 的价格调整。
// Anthropic/MiniMax：input_tokens 不含 cache；cache_read 按 10%，
// 5m 创建按 125%，1h 创建按 input 价格的 200%。
// cacheCreation1hTokens 是使用 1h TTL 的那部分 cacheCreationTokens。
// Kimi/xAI：input_tokens 含 cache；cache_read 享 75% 折扣（按 25% 计费）。
// OpenAI（默认）：input_tokens 含 cache；cache_read 享 50% 折扣。
func CostForSplitWithCache(model string, inputTokens, outputTokens, cacheReadTokens, cacheCreationTokens, cacheCreation1hTokens int, provider string) float64 {
	base := CostForSplit(model, inputTokens, outputTokens)
	if cacheReadTokens <= 0 && cacheCreationTokens <= 0 {
		return base
	}

	// Find input price for this model
	cfg := get()
	var inputPer1K float64
	for _, models := range cfg.Pricing.Models {
		if m, ok := models[model]; ok {
			inputPer1K = m.InputPer1K
			break
		}
	}
	if inputPer1K <= 0 {
		return base
	}

	switch {
	case provider == "anthropic" || provider == "minimax":
		// Anthropic/MiniMax: cache tokens are separate from input_tokens,
		// add them at discounted (read) / premium (creation) rates.
		// 1h TTL creation is billed at 2.0x; 5m TTL creation at 1.25x.
		// Anthropic/MiniMax：cache token 与 input_tokens 分开，
		// 按折扣价（读）/溢价（创建）单独累加。
		// 1h TTL 创建按 2.0 倍计费；5m TTL 创建按 1.25 倍。
		base += (float64(cacheReadTokens) / 1000.0) * inputPer1K * 0.1
		cache5m := cacheCreationTokens - cacheCreation1hTokens
		if cache5m < 0 {
			cache5m = 0
		}
		base += (float64(cache5m) / 1000.0) * inputPer1K * 1.25
		base += (float64(cacheCreation1hTokens) / 1000.0) * inputPer1K * 2.0
	case provider == "kimi" || provider == "xai":
		// Kimi/xAI: cached tokens included in input_tokens at full price,
		// actual billing is 25% (75% discount) — subtract the 75% discount.
		// xAI: grok-4-1-fast cached input $0.05/1M vs base $0.20/1M.
		// Kimi/xAI：cache token 已按全价计入 input_tokens，
		// 实际按 25% 计费（75% 折扣）——减去这 75% 折扣。
		// xAI：grok-4-1-fast cache 输入 $0.05/1M，基础价 $0.20/1M。
		base -= (float64(cacheReadTokens) / 1000.0) * inputPer1K * 0.75
	default:
		// OpenAI: cached tokens already in input_tokens at full price, subtract 50% discount
		// OpenAI：cache token 已按全价计入 input_tokens，减去 50% 折扣
		base -= (float64(cacheReadTokens) / 1000.0) * inputPer1K * 0.5
	}

	if base < 0 {
		base = 0
	}
	return base
}

// ValidateMap validates the pricing section in a raw config map for the config manager.
// ValidateMap 为配置管理器校验原始配置 map 中的 pricing 段。
func ValidateMap(m map[string]any) error {
	p, ok := m["pricing"].(map[string]any)
	if !ok {
		return nil
	}
	if d, ok := p["defaults"].(map[string]any); ok {
		if v, ok := d["combined_per_1k"].(float64); ok && v < 0 {
			return errors.New("pricing.defaults.combined_per_1k must be >= 0")
		}
	}
	if provs, ok := p["models"].(map[string]any); ok {
		for provName, pm := range provs {
			models, ok := pm.(map[string]any)
			if !ok {
				continue
			}
			for modelName, mv := range models {
				entry, ok := mv.(map[string]any)
				if !ok {
					continue
				}
				if v, ok := entry["input_per_1k"].(float64); ok && v < 0 {
					return errors.New("negative input_per_1k for " + provName + ":" + modelName)
				}
				if v, ok := entry["output_per_1k"].(float64); ok && v < 0 {
					return errors.New("negative output_per_1k for " + provName + ":" + modelName)
				}
				if v, ok := entry["combined_per_1k"].(float64); ok && v < 0 {
					return errors.New("negative combined_per_1k for " + provName + ":" + modelName)
				}
			}
		}
	}
	return nil
}

// tierProviders 返回 tier 对应的 provider 列表；tier 未知时 ok=false。
func tierProviders(cfg *config, tier string) ([]providerRef, bool) {
	switch strings.ToLower(tier) {
	case "small":
		return cfg.ModelTiers.Small.Providers, true
	case "medium":
		return cfg.ModelTiers.Medium.Providers, true
	case "large":
		return cfg.ModelTiers.Large.Providers, true
	default:
		return nil, false
	}
}

// GetPriorityOneProvider returns the priority-1 provider for a given tier from config.
// Returns empty string if not found in config (caller should use fallback).
// GetPriorityOneProvider 从配置返回某 tier 的优先级 1 provider。
// 配置中不存在时返回空字符串（调用方应使用兜底）。
func GetPriorityOneProvider(tier string) string {
	cfg := get()
	if cfg == nil {
		return ""
	}

	providers, ok := tierProviders(cfg, tier)
	if !ok {
		return ""
	}

	// Find provider with priority == 1
	for _, p := range providers {
		if p.Priority == 1 {
			return p.Provider
		}
	}

	return ""
}

// GetPriorityOneModel returns the priority-1 model for a given tier from config.
// Returns empty string if not found in config.
// GetPriorityOneModel 从配置返回某 tier 的优先级 1 模型。
// 配置中不存在时返回空字符串。
func GetPriorityOneModel(tier string) string {
	cfg := get()
	if cfg == nil {
		return ""
	}

	providers, ok := tierProviders(cfg, tier)
	if !ok {
		return ""
	}

	for _, p := range providers {
		if p.Priority == 1 && p.Model != "" {
			return p.Model
		}
	}
	return ""
}

// GetPriorityModelForProvider returns the preferred model for a given tier and provider.
// It scans the tier's provider list and returns the model with the lowest priority value
// for the specified provider (case-insensitive). Returns empty string if not found.
// GetPriorityModelForProvider 返回某 tier 与 provider 下首选的模型。
// 它扫描该 tier 的 provider 列表，返回指定 provider（大小写不敏感）
// 中 priority 值最小的模型。未找到时返回空字符串。
func GetPriorityModelForProvider(tier, provider string) string {
	cfg := get()
	if cfg == nil || provider == "" {
		return ""
	}

	provider = strings.ToLower(provider)
	providers, ok := tierProviders(cfg, tier)
	if !ok {
		return ""
	}

	bestModel := ""
	bestPriority := int(^uint(0) >> 1) // max int
	for _, p := range providers {
		if strings.ToLower(p.Provider) == provider {
			if p.Priority > 0 && p.Priority < bestPriority && p.Model != "" {
				bestPriority = p.Priority
				bestModel = p.Model
			}
		}
	}
	return bestModel
}

// GetProviderForModel searches all model tiers to find which provider offers a given model.
// Returns empty string if the model is not found in any tier.
// This is useful for reverse-lookup: given a model name, determine its provider.
// GetProviderForModel 搜索所有模型 tier，找出哪个 provider 提供该模型。
// 任何 tier 中都找不到时返回空字符串。
// 这适合反向查找：给定模型名，确定其 provider。
func GetProviderForModel(tier, model string) string {
	cfg := get()
	if cfg == nil || model == "" {
		return ""
	}

	providers, ok := tierProviders(cfg, tier)
	if !ok {
		return ""
	}

	for _, p := range providers {
		if p.Model == model {
			return p.Provider
		}
	}
	return ""
}

// zeroCostProviders lists providers where all models are free (local inference).
// zeroCostProviders 列出所有模型都免费的 provider（本地推理）。
var zeroCostProviders = map[string]bool{
	"ollama": true,
}

// isZeroCostProvider checks if a model belongs to a free provider (e.g. Ollama)
// by looking it up in model_catalog.
// isZeroCostProvider 在 model_catalog 中查找，判断模型是否属于免费 provider（如 Ollama）。
func isZeroCostProvider(model string) bool {
	cfg := get()
	if cfg == nil || model == "" {
		return false
	}
	for provName, models := range cfg.ModelCatalog {
		if !zeroCostProviders[provName] {
			continue
		}
		if _, found := models[model]; found {
			return true
		}
	}
	return false
}
