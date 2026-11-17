package activities

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/viper"
)

// WorkflowConfig contains configuration for cognitive workflows
// WorkflowConfig 保存各类 cognitive workflow 的配置。
type WorkflowConfig struct {
	// Exploratory workflow config
	// 探索型 workflow 配置
	ExploratoryMaxIterations       int     `json:"exploratory_max_iterations"`
	ExploratoryConfidenceThreshold float64 `json:"exploratory_confidence_threshold"`
	ExploratoryBranchFactor        int     `json:"exploratory_branch_factor"`
	ExploratoryMaxConcurrentAgents int     `json:"exploratory_max_concurrent_agents"`

	// React workflow config
	// React workflow 配置
	ReactMaxIterations     int `json:"react_max_iterations"`
	ReactObservationWindow int `json:"react_observation_window"`

	// Research workflow config
	// 研究型 workflow 配置
	ResearchDepth               int `json:"research_depth"`
	ResearchSourcesPerRound     int `json:"research_sources_per_round"`
	ResearchMinSources          int `json:"research_min_sources"`
	ResearchMaxConcurrentAgents int `json:"research_max_concurrent_agents"`

	// Scientific workflow config
	// 科学型 workflow 配置
	ScientificMaxHypotheses          int     `json:"scientific_max_hypotheses"`
	ScientificMaxIterations          int     `json:"scientific_max_iterations"`
	ScientificConfidenceThreshold    float64 `json:"scientific_confidence_threshold"`
	ScientificContradictionThreshold float64 `json:"scientific_contradiction_threshold"`

	// Reflection config
	// 反思配置
	ReflectionEnabled             bool     `json:"reflection_enabled"`
	ReflectionMaxRetries          int      `json:"reflection_max_retries"`
	ReflectionConfidenceThreshold float64  `json:"reflection_confidence_threshold"`
	ReflectionCriteria            []string `json:"reflection_criteria"`
	ReflectionTimeoutMs           int      `json:"reflection_timeout_ms"`

	// Router/DAG config
	// 路由/DAG 配置
	SimpleThreshold   float64 `json:"simple_threshold"`
	MaxParallelAgents int     `json:"max_parallel_agents"`

	// Complexity thresholds for model tier selection
	// 用于选择 model tier 的复杂度阈值：低于 Simple 用 small，低于 Medium 用 medium，再往上用 large。
	ComplexitySimpleThreshold float64 `json:"complexity_simple_threshold"` // < this = small model 注：小于该值使用 small 模型
	ComplexityMediumThreshold float64 `json:"complexity_medium_threshold"` // < this = medium model, >= this = large model 注：小于该值使用 medium 模型，大于等于该值使用 large 模型

	// Approval config
	// 审批配置
	ApprovalEnabled             bool     `json:"approval_enabled"`
	ApprovalComplexityThreshold float64  `json:"approval_complexity_threshold"`
	ApprovalDangerousTools      []string `json:"approval_dangerous_tools"`

	// Execution pattern config
	// 执行模式配置
	ParallelMaxConcurrency   int  `json:"parallel_max_concurrency"`
	HybridDependencyTimeout  int  `json:"hybrid_dependency_timeout_seconds"`
	SequentialPassResults    bool `json:"sequential_pass_results"`
	SequentialExtractNumeric bool `json:"sequential_extract_numeric"`

	// P2P Coordination config
	// P2P 协同配置
	P2PCoordinationEnabled bool `json:"p2p_coordination_enabled"`
	P2PTimeoutSeconds      int  `json:"p2p_timeout_seconds"`

	// Swarm Agent config
	// Swarm Agent 配置
	SwarmEnabled               bool `json:"swarm_enabled"`
	SwarmMaxAgents             int  `json:"swarm_max_agents"`
	SwarmMaxIterationsPerAgent int  `json:"swarm_max_iterations_per_agent"`
	SwarmAgentTimeoutSeconds   int  `json:"swarm_agent_timeout_seconds"`
	SwarmMaxMessagesPerAgent   int  `json:"swarm_max_messages_per_agent"`
	SwarmWorkspaceSnippetChars int  `json:"swarm_workspace_snippet_chars"`
	SwarmWorkspaceMaxEntries   int  `json:"swarm_workspace_max_entries"`
	SwarmMaxTotalLLMCalls      int  `json:"swarm_max_total_llm_calls"`
	SwarmMaxTotalTokens        int  `json:"swarm_max_total_tokens"`
	SwarmMaxWallClockMinutes   int  `json:"swarm_max_wall_clock_minutes"`

	// Templates
	// 模板
	TemplateFallbackEnabled bool `json:"template_fallback_enabled"`

	// Learning router toggle (continuous learning)
	// When false, skip learning-based strategy recommendation in the router
	// 学习型 router 开关（continuous learning）。
	// 为 false 时，router 跳过基于学习的策略推荐。
	ContinuousLearningEnabled bool `json:"continuous_learning_enabled"`
}

// GetWorkflowConfig is an activity that returns workflow configuration
// GetWorkflowConfig 是返回 workflow 配置的 activity。
func GetWorkflowConfig(ctx context.Context) (*WorkflowConfig, error) {
	// Create a new viper instance to load features.yaml
	// 新建一个 viper 实例来加载 features.yaml。
	v := viper.New()

	// Determine config path
	// 确定配置路径。
	cfgPath := os.Getenv("CONFIG_PATH")
	if cfgPath != "" {
		// CONFIG_PATH may be a directory; append filename if so
		// CONFIG_PATH 可能是目录，是目录时补上文件名。
		info, err := os.Stat(cfgPath)
		if err == nil && info.IsDir() {
			cfgPath = cfgPath + "/features.yaml"
		}
	} else {
		// Try local development paths first
		// 先试本地开发用的相对路径。
		if _, err := os.Stat("config/features.yaml"); err == nil {
			cfgPath = "config/features.yaml"
		} else if _, err := os.Stat("../../config/features.yaml"); err == nil {
			cfgPath = "../../config/features.yaml"
		} else {
			cfgPath = "/app/config/features.yaml" // Docker path 注：Docker 环境路径
		}
	}

	v.SetConfigFile(cfgPath)
	// Try to read config, but don't fail if it doesn't exist - use defaults
	// 尝试读取配置；不存在也不报错，全部走默认值。
	_ = v.ReadInConfig()

	config := &WorkflowConfig{
		// Exploratory defaults
		// 探索型默认值
		ExploratoryMaxIterations:       v.GetInt("cognitive_workflows.exploratory.max_iterations"),
		ExploratoryConfidenceThreshold: v.GetFloat64("cognitive_workflows.exploratory.confidence_threshold"),
		ExploratoryBranchFactor:        v.GetInt("cognitive_workflows.exploratory.branch_factor"),
		ExploratoryMaxConcurrentAgents: v.GetInt("cognitive_workflows.exploratory.max_concurrent_agents"),

		// React defaults
		// React 默认值
		ReactMaxIterations:     v.GetInt("cognitive_workflows.react.max_iterations"),
		ReactObservationWindow: v.GetInt("cognitive_workflows.react.observation_window"),

		// Research defaults
		// 研究型默认值
		ResearchDepth:               v.GetInt("cognitive_workflows.research.research_depth"),
		ResearchSourcesPerRound:     v.GetInt("cognitive_workflows.research.sources_per_round"),
		ResearchMinSources:          v.GetInt("cognitive_workflows.research.min_sources"),
		ResearchMaxConcurrentAgents: v.GetInt("cognitive_workflows.research.max_concurrent_agents"),

		// Scientific defaults
		// 科学型默认值
		ScientificMaxHypotheses:          v.GetInt("cognitive_workflows.scientific.max_hypotheses"),
		ScientificMaxIterations:          v.GetInt("cognitive_workflows.scientific.max_iterations"),
		ScientificConfidenceThreshold:    v.GetFloat64("cognitive_workflows.scientific.confidence_threshold"),
		ScientificContradictionThreshold: v.GetFloat64("cognitive_workflows.scientific.contradiction_threshold"),

		// Reflection defaults
		// 反思默认值
		ReflectionEnabled:             v.GetBool("workflows.reflection.enabled"),
		ReflectionMaxRetries:          v.GetInt("workflows.reflection.max_retries"),
		ReflectionConfidenceThreshold: v.GetFloat64("workflows.reflection.confidence_threshold"),
		ReflectionCriteria:            v.GetStringSlice("workflows.reflection.criteria"),
		ReflectionTimeoutMs:           v.GetInt("workflows.reflection.timeout_ms"),

		// Router/DAG
		// 路由/DAG
		SimpleThreshold:   v.GetFloat64("workflows.dag.simple_threshold"),
		MaxParallelAgents: v.GetInt("workflows.dag.max_parallel_agents"),

		// Complexity thresholds
		// 复杂度阈值
		ComplexitySimpleThreshold: v.GetFloat64("workflows.complexity.simple_threshold"),
		ComplexityMediumThreshold: v.GetFloat64("workflows.complexity.medium_threshold"),

		// Approval
		// 审批
		ApprovalEnabled:             v.GetBool("workflows.approval.enabled"),
		ApprovalComplexityThreshold: v.GetFloat64("workflows.approval.complexity_threshold"),
		ApprovalDangerousTools:      v.GetStringSlice("workflows.approval.dangerous_tools"),

		// Execution patterns
		// 执行模式
		ParallelMaxConcurrency:   v.GetInt("workflows.execution.parallel_max_concurrency"),
		HybridDependencyTimeout:  v.GetInt("workflows.execution.hybrid_dependency_timeout_seconds"),
		SequentialPassResults:    v.GetBool("workflows.execution.sequential_pass_results"),
		SequentialExtractNumeric: v.GetBool("workflows.execution.sequential_extract_numeric"),
	}

	// Set defaults if not configured
	// 未配置的项按零值判定，补上默认值。
	if config.ExploratoryMaxIterations == 0 {
		config.ExploratoryMaxIterations = 5
	}
	if config.ExploratoryConfidenceThreshold == 0 {
		config.ExploratoryConfidenceThreshold = 0.85
	}
	if config.ExploratoryBranchFactor == 0 {
		config.ExploratoryBranchFactor = 3
	}
	if config.ExploratoryMaxConcurrentAgents == 0 {
		config.ExploratoryMaxConcurrentAgents = 3
	}

	if config.ReactMaxIterations == 0 {
		config.ReactMaxIterations = 10
	}
	if config.ReactObservationWindow == 0 {
		config.ReactObservationWindow = 3
	}

	if config.ResearchDepth == 0 {
		config.ResearchDepth = 3
	}
	if config.ResearchSourcesPerRound == 0 {
		config.ResearchSourcesPerRound = 4
	}
	if config.ResearchMinSources == 0 {
		config.ResearchMinSources = 5
	}
	if config.ResearchMaxConcurrentAgents == 0 {
		config.ResearchMaxConcurrentAgents = 3
	}

	if config.ScientificMaxHypotheses == 0 {
		config.ScientificMaxHypotheses = 3
	}
	if config.ScientificMaxIterations == 0 {
		config.ScientificMaxIterations = 4
	}
	if config.ScientificConfidenceThreshold == 0 {
		config.ScientificConfidenceThreshold = 0.75
	}
	if config.ScientificContradictionThreshold == 0 {
		config.ScientificContradictionThreshold = 0.3
	}

	// Reflection defaults
	// 反思默认值
	if config.ReflectionMaxRetries == 0 {
		config.ReflectionMaxRetries = 2
	}
	if config.ReflectionConfidenceThreshold == 0 {
		config.ReflectionConfidenceThreshold = 0.7
	}
	if len(config.ReflectionCriteria) == 0 {
		config.ReflectionCriteria = []string{"completeness", "accuracy", "relevance"}
	}
	if config.ReflectionTimeoutMs == 0 {
		config.ReflectionTimeoutMs = 5000
	}

	// Router/DAG defaults
	// 路由/DAG 默认值
	if config.SimpleThreshold == 0 {
		config.SimpleThreshold = 0.3
	}
	if config.MaxParallelAgents == 0 {
		config.MaxParallelAgents = 5
	}

	// Complexity thresholds defaults
	// 复杂度阈值默认值
	if config.ComplexitySimpleThreshold == 0 {
		config.ComplexitySimpleThreshold = 0.3
	}
	if config.ComplexityMediumThreshold == 0 {
		config.ComplexityMediumThreshold = 0.5 // Changed from hardcoded 0.7 注：由硬编码的 0.7 调整而来
	}

	// Approval defaults - check environment variables first
	// Override with environment variables if set
	// approval 默认值：先看环境变量，
	// 环境变量有值时覆盖配置。
	if envEnabled := os.Getenv("APPROVAL_ENABLED"); envEnabled != "" {
		config.ApprovalEnabled = envEnabled == "true" || envEnabled == "1"
	}
	if envThreshold := os.Getenv("APPROVAL_COMPLEXITY_THRESHOLD"); envThreshold != "" {
		if threshold, err := strconv.ParseFloat(envThreshold, 64); err == nil {
			config.ApprovalComplexityThreshold = threshold
		}
	}
	if envTools := os.Getenv("APPROVAL_DANGEROUS_TOOLS"); envTools != "" {
		config.ApprovalDangerousTools = strings.Split(envTools, ",")
	}

	// Apply defaults if not set
	// 未设置时套用默认值
	if config.ApprovalComplexityThreshold == 0 {
		config.ApprovalComplexityThreshold = 0.8
	}
	if len(config.ApprovalDangerousTools) == 0 {
		config.ApprovalDangerousTools = []string{"file_system", "code_execution"}
	}

	// Execution pattern defaults
	// 执行模式默认值
	if config.ParallelMaxConcurrency == 0 {
		config.ParallelMaxConcurrency = 5
	}
	if config.HybridDependencyTimeout == 0 {
		config.HybridDependencyTimeout = 360 // 6 minutes 注：6 分钟
	}
	// SequentialPassResults defaults to true if not explicitly set to false
	// SequentialPassResults 未显式设为 false 时默认为 true。
	if !v.IsSet("workflows.execution.sequential_pass_results") {
		config.SequentialPassResults = true
	}
	// SequentialExtractNumeric defaults to true if not explicitly set to false
	// SequentialExtractNumeric 未显式设为 false 时默认为 true。
	if !v.IsSet("workflows.execution.sequential_extract_numeric") {
		config.SequentialExtractNumeric = true
	}

	// P2P Coordination defaults
	// P2P 协同默认值
	config.P2PCoordinationEnabled = v.GetBool("workflows.p2p.enabled")
	if config.P2PTimeoutSeconds == 0 {
		config.P2PTimeoutSeconds = v.GetInt("workflows.p2p.timeout_seconds")
		if config.P2PTimeoutSeconds == 0 {
			config.P2PTimeoutSeconds = 360 // 6 minutes default 注：默认 6 分钟
		}
	}

	// Swarm config
	// Swarm 配置
	config.SwarmEnabled = v.GetBool("workflows.swarm.enabled")
	config.SwarmMaxAgents = v.GetInt("workflows.swarm.max_agents")
	if config.SwarmMaxAgents == 0 {
		config.SwarmMaxAgents = 10
	}
	config.SwarmMaxIterationsPerAgent = v.GetInt("workflows.swarm.max_iterations_per_agent")
	if config.SwarmMaxIterationsPerAgent == 0 {
		config.SwarmMaxIterationsPerAgent = 25
	}
	config.SwarmAgentTimeoutSeconds = v.GetInt("workflows.swarm.agent_timeout_seconds")
	if config.SwarmAgentTimeoutSeconds == 0 {
		config.SwarmAgentTimeoutSeconds = 1800
	}
	config.SwarmMaxMessagesPerAgent = v.GetInt("workflows.swarm.max_messages_per_agent")
	if config.SwarmMaxMessagesPerAgent == 0 {
		config.SwarmMaxMessagesPerAgent = 20
	}
	config.SwarmWorkspaceSnippetChars = v.GetInt("workflows.swarm.workspace_snippet_chars")
	if config.SwarmWorkspaceSnippetChars == 0 {
		config.SwarmWorkspaceSnippetChars = 800
	}
	config.SwarmWorkspaceMaxEntries = v.GetInt("workflows.swarm.workspace_max_entries")
	if config.SwarmWorkspaceMaxEntries == 0 {
		config.SwarmWorkspaceMaxEntries = 5
	}
	config.SwarmMaxTotalLLMCalls = v.GetInt("workflows.swarm.max_total_llm_calls")
	if config.SwarmMaxTotalLLMCalls == 0 {
		config.SwarmMaxTotalLLMCalls = 200
	}
	config.SwarmMaxTotalTokens = v.GetInt("workflows.swarm.max_total_tokens")
	if config.SwarmMaxTotalTokens == 0 {
		config.SwarmMaxTotalTokens = 1000000
	}
	config.SwarmMaxWallClockMinutes = v.GetInt("workflows.swarm.max_wall_clock_minutes")
	if config.SwarmMaxWallClockMinutes == 0 {
		config.SwarmMaxWallClockMinutes = 30
	}

	// Template fallback (prefer env override; default false)
	// template fallback：优先读环境变量，默认关闭。
	if env := os.Getenv("TEMPLATE_FALLBACK_ENABLED"); env != "" {
		config.TemplateFallbackEnabled = env == "true" || env == "1"
	} else {
		config.TemplateFallbackEnabled = v.GetBool("workflows.templates.fallback_to_ai")
	}

	// Continuous learning toggle (default false)
	// Prefer env CONTINUOUS_LEARNING_ENABLED, else config key continuous_learning.enabled
	// continuous learning 开关（默认关闭）。
	// 优先取环境变量 CONTINUOUS_LEARNING_ENABLED，其次取配置项 continuous_learning.enabled。
	if env := os.Getenv("CONTINUOUS_LEARNING_ENABLED"); env != "" {
		config.ContinuousLearningEnabled = env == "true" || env == "1"
	} else {
		config.ContinuousLearningEnabled = v.GetBool("continuous_learning.enabled")
	}

	return config, nil
}

// LoadWorkflowConfig loads configuration at workflow start
// LoadWorkflowConfig 在 workflow 启动时加载配置。
func LoadWorkflowConfig(ctx context.Context) (map[string]any, error) {
	config, err := GetWorkflowConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to load workflow config: %w", err)
	}

	// Convert to map for easier access
	// 转成 map，便于按 key 访问。
	return map[string]any{
		"exploratory": map[string]any{
			"max_iterations":        config.ExploratoryMaxIterations,
			"confidence_threshold":  config.ExploratoryConfidenceThreshold,
			"branch_factor":         config.ExploratoryBranchFactor,
			"max_concurrent_agents": config.ExploratoryMaxConcurrentAgents,
		},
		"react": map[string]any{
			"max_iterations":     config.ReactMaxIterations,
			"observation_window": config.ReactObservationWindow,
		},
		"research": map[string]any{
			"research_depth":        config.ResearchDepth,
			"sources_per_round":     config.ResearchSourcesPerRound,
			"min_sources":           config.ResearchMinSources,
			"max_concurrent_agents": config.ResearchMaxConcurrentAgents,
		},
		"scientific": map[string]any{
			"max_hypotheses":          config.ScientificMaxHypotheses,
			"max_iterations":          config.ScientificMaxIterations,
			"confidence_threshold":    config.ScientificConfidenceThreshold,
			"contradiction_threshold": config.ScientificContradictionThreshold,
		},
		"reflection": map[string]any{
			"enabled":              config.ReflectionEnabled,
			"max_retries":          config.ReflectionMaxRetries,
			"confidence_threshold": config.ReflectionConfidenceThreshold,
			"criteria":             config.ReflectionCriteria,
			"timeout_ms":           config.ReflectionTimeoutMs,
		},
		"dag": map[string]any{
			"simple_threshold":    config.SimpleThreshold,
			"max_parallel_agents": config.MaxParallelAgents,
		},
		"approval": map[string]any{
			"enabled":              config.ApprovalEnabled,
			"complexity_threshold": config.ApprovalComplexityThreshold,
			"dangerous_tools":      config.ApprovalDangerousTools,
		},
		"execution": map[string]any{
			"parallel_max_concurrency":   config.ParallelMaxConcurrency,
			"hybrid_dependency_timeout":  config.HybridDependencyTimeout,
			"sequential_pass_results":    config.SequentialPassResults,
			"sequential_extract_numeric": config.SequentialExtractNumeric,
		},
		"p2p": map[string]any{
			"enabled":         config.P2PCoordinationEnabled,
			"timeout_seconds": config.P2PTimeoutSeconds,
		},
		"swarm": map[string]any{
			"enabled":                  config.SwarmEnabled,
			"max_agents":               config.SwarmMaxAgents,
			"max_iterations_per_agent": config.SwarmMaxIterationsPerAgent,
			"agent_timeout_seconds":    config.SwarmAgentTimeoutSeconds,
			"max_messages_per_agent":   config.SwarmMaxMessagesPerAgent,
			"workspace_snippet_chars":  config.SwarmWorkspaceSnippetChars,
			"workspace_max_entries":    config.SwarmWorkspaceMaxEntries,
			"max_total_llm_calls":      config.SwarmMaxTotalLLMCalls,
			"max_total_tokens":         config.SwarmMaxTotalTokens,
			"max_wall_clock_minutes":   config.SwarmMaxWallClockMinutes,
		},
	}, nil
}
