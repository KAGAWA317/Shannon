package patterns

import (
	"fmt"
	"sync"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"go.temporal.io/sdk/workflow"
)

// PatternType represents the type of multi-agent pattern
// PatternType 表示 multi-agent pattern 的类型
type PatternType string

const (
	PatternReflection     PatternType = "reflection"
	PatternReact          PatternType = "react"
	PatternChainOfThought PatternType = "chain_of_thought"
	PatternDebate         PatternType = "debate"
	PatternTreeOfThoughts PatternType = "tree_of_thoughts"
	PatternEnsemble       PatternType = "ensemble"
)

// PatternCapability describes what a pattern can do
// PatternCapability 描述某个 pattern 能做什么
type PatternCapability string

const (
	CapabilityIterativeImprovement PatternCapability = "iterative_improvement"
	CapabilityStepByStep           PatternCapability = "step_by_step"
	CapabilityMultiPerspective     PatternCapability = "multi_perspective"
	CapabilityConflictResolution   PatternCapability = "conflict_resolution"
	CapabilityExploration          PatternCapability = "exploration"
	CapabilityConsensusBuilding    PatternCapability = "consensus_building"
)

// Pattern defines the interface for all multi-agent patterns
// Pattern 定义所有 multi-agent pattern 的接口
type Pattern interface {
	// Execute runs the pattern with given context and configuration
	// Execute 按给定的 context 与配置运行该 pattern
	Execute(ctx workflow.Context, input PatternInput) (*PatternResult, error)

	// GetType returns the pattern type
	// GetType 返回 pattern 类型
	GetType() PatternType

	// GetCapabilities returns what this pattern can do
	// GetCapabilities 返回该 pattern 具备的能力
	GetCapabilities() []PatternCapability

	// EstimateTokens estimates token usage for this pattern
	// EstimateTokens 估算该 pattern 的 token 用量
	EstimateTokens(input PatternInput) int
}

// PatternInput provides input to a pattern
// PatternInput 为 pattern 提供输入
type PatternInput struct {
	Query     string
	Context   map[string]any
	History   []string
	SessionID string
	UserID    string
	Config    any // Pattern-specific config 注：pattern 专属配置
	BudgetMax int
	ModelTier string
}

// PatternResult contains the output from a pattern
// PatternResult 承载 pattern 的输出
type PatternResult struct {
	Result       string
	TokensUsed   int
	Confidence   float64
	Metadata     map[string]any
	AgentResults []activities.AgentExecutionResult
}

// PatternRegistry manages available patterns
// PatternRegistry 管理可用的 pattern
type PatternRegistry struct {
	mu       sync.RWMutex
	patterns map[PatternType]Pattern

	// Pattern selection strategy
	// pattern 选择策略
	selector PatternSelector
}

// PatternSelector decides which pattern to use
// PatternSelector 决定使用哪个 pattern
type PatternSelector interface {
	SelectPattern(query string, context map[string]any, available []Pattern) (Pattern, error)
}

// DefaultPatternSelector selects patterns based on task characteristics
// DefaultPatternSelector 依据 task 特征选择 pattern
type DefaultPatternSelector struct{}

// SelectPattern chooses the best pattern for a task
// SelectPattern 为 task 选出最合适的 pattern
func (s *DefaultPatternSelector) SelectPattern(query string, context map[string]any, available []Pattern) (Pattern, error) {
	// Simple heuristic-based selection
	// 目前只认 context["pattern"] 显式提示，否则退回第一个注册的 pattern，没有按 task 特征真正打分。
	// Could be enhanced with ML-based selection
	// 后续可用基于 ML 的选择来增强

	// Check for explicit pattern hint
	// 检查是否显式给出了 pattern 提示
	if patternHint, ok := context["pattern"].(string); ok {
		for _, p := range available {
			if string(p.GetType()) == patternHint {
				return p, nil
			}
		}
	}

	// Default to first available pattern
	// 默认取第一个可用的 pattern
	if len(available) > 0 {
		return available[0], nil
	}

	return nil, fmt.Errorf("no suitable pattern found")
}

var (
	globalRegistry *PatternRegistry
	registryOnce   sync.Once
)

// GetRegistry returns the global pattern registry
// GetRegistry 返回全局 pattern registry
func GetRegistry() *PatternRegistry {
	registryOnce.Do(func() {
		globalRegistry = &PatternRegistry{
			patterns: make(map[PatternType]Pattern),
			selector: &DefaultPatternSelector{},
		}
		// Register default patterns
		// 注册内置 pattern
		registerDefaultPatterns()
	})
	return globalRegistry
}

// Register adds a pattern to the registry
// Register 向 registry 添加 pattern
func (r *PatternRegistry) Register(pattern Pattern) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if pattern == nil {
		return fmt.Errorf("cannot register nil pattern")
	}

	r.patterns[pattern.GetType()] = pattern
	return nil
}

// Get retrieves a pattern by type
// Get 按类型取出 pattern
func (r *PatternRegistry) Get(patternType PatternType) (Pattern, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	pattern, ok := r.patterns[patternType]
	return pattern, ok
}

// List returns all registered patterns
// List 返回所有已注册的 pattern
func (r *PatternRegistry) List() []Pattern {
	r.mu.RLock()
	defer r.mu.RUnlock()

	patterns := make([]Pattern, 0, len(r.patterns))
	for _, p := range r.patterns {
		patterns = append(patterns, p)
	}
	return patterns
}

// SelectForTask chooses the best pattern for a given task
// SelectForTask 为给定 task 选出最合适的 pattern
func (r *PatternRegistry) SelectForTask(query string, context map[string]any) (Pattern, error) {
	available := r.List()
	if len(available) == 0 {
		return nil, fmt.Errorf("no patterns registered")
	}

	return r.selector.SelectPattern(query, context, available)
}

// SetSelector updates the pattern selection strategy
// SetSelector 更新 pattern 选择策略
func (r *PatternRegistry) SetSelector(selector PatternSelector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.selector = selector
}

// registerDefaultPatterns registers built-in patterns
// registerDefaultPatterns 注册内置 pattern
func registerDefaultPatterns() {
	// Register existing patterns
	// These will be implemented as Pattern interface wrappers
	// around existing implementations
	// 内置 pattern 都是既有实现的薄适配层，注册顺序决定默认选择结果。
	_ = globalRegistry.Register(reflectionPattern{})
	_ = globalRegistry.Register(reactPattern{})
	_ = globalRegistry.Register(chainOfThoughtPattern{})
	_ = globalRegistry.Register(debatePattern{})
	_ = globalRegistry.Register(treeOfThoughtsPattern{})
}
