package openai

import (
	"fmt"
	"os"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// ModelConfig represents a single model's configuration.
// ModelConfig 表示单个模型的配置。
type ModelConfig struct {
	WorkflowMode     string         `yaml:"workflow_mode"`      // simple, research, supervisor 注：simple、research、supervisor
	Context          map[string]any `yaml:"context"`            // Shannon context to inject 注：要注入的 Shannon context
	Description      string         `yaml:"description"`        // Human-readable description 注：人类可读的描述
	MaxTokensDefault int            `yaml:"max_tokens_default"` // Default max_tokens 注：默认 max_tokens
}

// RegistryConfig represents the full model registry configuration.
// RegistryConfig 表示完整的模型 registry 配置。
type RegistryConfig struct {
	Models       map[string]ModelConfig `yaml:"models"`
	DefaultModel string                 `yaml:"default_model"`
	Settings     struct {
		MaxTokensLimit     int              `yaml:"max_tokens_limit"`
		DefaultTemperature float64          `yaml:"default_temperature"`
		SessionTTL         int              `yaml:"session_ttl"`
	} `yaml:"settings"`
}

// Registry manages OpenAI-compatible model mappings.
// Registry 管理 OpenAI 兼容的 model 映射。
type Registry struct {
	config  *RegistryConfig
	mu      sync.RWMutex
	created time.Time
}

var (
	globalRegistry     *Registry
	globalRegistryOnce sync.Once
	globalRegistryErr  error
)

// GetRegistry returns the singleton model registry.
// GetRegistry 返回单例 model registry。
func GetRegistry() (*Registry, error) {
	globalRegistryOnce.Do(func() {
		globalRegistry, globalRegistryErr = loadRegistry()
	})
	return globalRegistry, globalRegistryErr
}

// loadRegistry loads the model registry from configuration files.
// loadRegistry 从配置文件加载模型 registry。
func loadRegistry() (*Registry, error) {
	candidates := []string{
		"config/openai_models.yaml",
		"/app/config/openai_models.yaml",
	}

	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			data, err := os.ReadFile(path)
			if err != nil {
				return nil, fmt.Errorf("failed to read %s: %w", path, err)
			}

			var config RegistryConfig
			if err := yaml.Unmarshal(data, &config); err != nil {
				return nil, fmt.Errorf("failed to parse %s: %w", path, err)
			}

			// Apply defaults
			// 应用默认值
			if config.DefaultModel == "" {
				config.DefaultModel = "shannon-chat"
			}
			if config.Settings.MaxTokensLimit == 0 {
				config.Settings.MaxTokensLimit = 16384
			}
			if config.Settings.DefaultTemperature == 0 {
				config.Settings.DefaultTemperature = 0.7
			}
			if config.Settings.SessionTTL == 0 {
				config.Settings.SessionTTL = 86400
			}

			return &Registry{
				config:  &config,
				created: time.Now(),
			}, nil
		}
	}

	// Return default registry if no config file found
	// 没找到 config 文件时返回默认 registry。
	return newDefaultRegistry(), nil
}

// newDefaultRegistry creates a registry with built-in defaults.
// newDefaultRegistry 用内置默认值创建 registry。
func newDefaultRegistry() *Registry {
	config := &RegistryConfig{
		DefaultModel: "shannon-chat",
		Models: map[string]ModelConfig{
			"shannon-deep-research": {
				WorkflowMode: "research",
				Context: map[string]any{
					"force_research":             true,
					"research_strategy":          "deep",
					"iterative_research_enabled": true,
					"iterative_max_iterations":   3,
				},
				Description:      "Deep research with iterative refinement",
				MaxTokensDefault: 8192,
			},
			"shannon-standard-research": {
				WorkflowMode: "research",
				Context: map[string]any{
					"force_research":    true,
					"research_strategy": "standard",
				},
				Description:      "Balanced research with moderate depth",
				MaxTokensDefault: 4096,
			},
			"shannon-quick-research": {
				WorkflowMode: "research",
				Context: map[string]any{
					"force_research":    true,
					"research_strategy": "quick",
				},
				Description:      "Fast research for simple queries",
				MaxTokensDefault: 4096,
			},
			"shannon-chat": {
				WorkflowMode:     "simple",
				Context:          map[string]any{},
				Description:      "General chat completion",
				MaxTokensDefault: 4096,
			},
			"shannon-complex": {
				WorkflowMode:     "supervisor",
				Context:          map[string]any{},
				Description:      "Multi-agent orchestration for complex tasks",
				MaxTokensDefault: 8192,
			},
		},
	}
	config.Settings.MaxTokensLimit = 16384
	config.Settings.DefaultTemperature = 0.7
	config.Settings.SessionTTL = 86400

	return &Registry{
		config:  config,
		created: time.Now(),
	}
}

// GetModel returns the configuration for a model, or error if not found.
// GetModel 返回指定 model 的配置，model 不存在时返回 error。
func (r *Registry) GetModel(modelName string) (*ModelConfig, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	if modelName == "" {
		modelName = r.config.DefaultModel
	}

	model, ok := r.config.Models[modelName]
	if !ok {
		return nil, fmt.Errorf("model not found: %s", modelName)
	}

	return &model, nil
}

// GetDefaultModel returns the default model name.
// GetDefaultModel 返回默认模型名。
func (r *Registry) GetDefaultModel() string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config.DefaultModel
}

// ListModels returns all available models.
// ListModels 返回所有可用模型。
func (r *Registry) ListModels() []ModelObject {
	r.mu.RLock()
	defer r.mu.RUnlock()

	models := make([]ModelObject, 0, len(r.config.Models))
	for name := range r.config.Models {
		models = append(models, ModelObject{
			ID:      name,
			Object:  "model",
			Created: r.created.Unix(),
			OwnedBy: "shannon",
		})
	}
	return models
}

// GetMaxTokensLimit returns the global max tokens limit.
// GetMaxTokensLimit 返回全局最大 token 上限。
func (r *Registry) GetMaxTokensLimit() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config.Settings.MaxTokensLimit
}

// GetDefaultTemperature returns the default temperature.
// GetDefaultTemperature 返回默认 temperature。
func (r *Registry) GetDefaultTemperature() float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.config.Settings.DefaultTemperature
}

// IsValidModel checks if a model name is valid.
// IsValidModel 校验模型名是否有效。
func (r *Registry) IsValidModel(modelName string) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	_, ok := r.config.Models[modelName]
	return ok
}


