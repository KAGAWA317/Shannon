package policy

import (
	"os"
	"strconv"
	"strings"
)

// Mode defines the policy engine operating mode
// Mode 定义策略引擎的运行模式
type Mode string

const (
	// ModeOff disables policy evaluation entirely
	// ModeOff 完全关闭策略评估
	ModeOff Mode = "off"
	// ModeDryRun evaluates policies but doesn't enforce them (log only)
	// ModeDryRun 只评估策略但不强制执行（仅记录日志）
	ModeDryRun Mode = "dry-run"
	// ModeEnforce evaluates and enforces policies
	// ModeEnforce 评估并强制执行策略
	ModeEnforce Mode = "enforce"
)

// CanaryConfig defines canary rollout configuration for staged enforcement
// CanaryConfig 定义分阶段强制生效的 canary 灰度发布配置
type CanaryConfig struct {
	// Enabled controls whether canary rollout is active
	// Enabled 控制 canary 灰度发布是否启用
	Enabled bool

	// EnforcePercentage controls what percentage of requests get enforce mode
	// EnforcePercentage 控制多大比例的 request 使用 enforce 模式
	// Remaining percentage will use dry-run mode for safety
	// 剩余比例出于安全考虑使用 dry-run 模式
	// Range: 0-100, default: 0 (all dry-run)
	// EnforcePercentage 控制进入 enforce 模式的 request 比例，其余 request 为安全起见走 dry-run；范围 0-100，默认 0（全部 dry-run）。
	EnforcePercentage int

	// EnforceUsers list of specific users who always get enforce mode
	// EnforceUsers 指定始终使用 enforce 模式的用户列表
	EnforceUsers []string

	// EnforceAgents list of specific agents who always get enforce mode
	// EnforceAgents 指定始终使用 enforce 模式的 agent 列表
	EnforceAgents []string

	// DryRunUsers list of specific users who always get dry-run mode (override percentage)
	// DryRunUsers 指定始终使用 dry-run 模式的用户列表（优先于百分比配置）
	DryRunUsers []string

	// SLOThresholds for monitoring and automatic rollback
	// SLOThresholds 用于监控与自动回滚
	SLOThresholds SLOConfig
}

// SLOConfig defines Service Level Objective thresholds for safe rollout
// SLOConfig 定义安全发布所需的 Service Level Objective 阈值
type SLOConfig struct {
	// MaxErrorRate maximum acceptable error rate (0-100%)
	// MaxErrorRate 可接受的最大错误率（0-100%）
	MaxErrorRate float64

	// MaxLatencyP95 maximum acceptable 95th percentile latency in milliseconds
	// MaxLatencyP95 可接受的最大 P95 延迟（毫秒）
	MaxLatencyP95 float64

	// MaxLatencyP50 maximum acceptable 50th percentile latency in milliseconds
	// MaxLatencyP50 可接受的最大 P50 延迟（毫秒）
	MaxLatencyP50 float64

	// MinCacheHitRate minimum acceptable cache hit rate (0-100%)
	// MinCacheHitRate 可接受的最低缓存命中率（0-100%）
	MinCacheHitRate float64
}

// Config holds policy engine configuration
// Config 保存策略引擎配置
type Config struct {
	// Enabled controls whether the policy engine is active
	// Enabled 控制策略引擎是否启用
	Enabled bool

	// Mode controls policy enforcement behavior
	// Mode 控制策略强制执行行为
	Mode Mode

	// Path to the directory containing .rego policy files
	// Path 指向存放 .rego 策略文件的目录
	Path string

	// FailClosed determines behavior when policies can't be loaded
	// FailClosed 决定策略无法加载时的行为
	// true: deny all requests if policies fail to load
	// true：策略加载失败则拒绝所有 request
	// false: allow all requests if policies fail to load (fail-open)
	// FailClosed 决定策略加载失败时的行为：true 则拒绝所有 request，false 则放行所有 request（fail-open）。
	FailClosed bool

	// Environment context for policy evaluation
	// Environment 为策略评估的环境上下文
	Environment string

	// Canary configuration for staged rollout
	// Canary 为分阶段灰度发布配置
	Canary CanaryConfig

	// EmergencyKillSwitch forces all requests to dry-run mode regardless of other settings
	// EmergencyKillSwitch 无视其他配置，强制所有 request 走 dry-run
	EmergencyKillSwitch bool
}

// LoadConfig loads policy configuration from environment variables
// LoadConfig 从环境变量加载策略配置
func LoadConfig() *Config {
	config := &Config{
		Enabled:             getEnvBool("SHANNON_POLICY_ENABLED", false),
		Mode:                Mode(getEnvString("SHANNON_POLICY_MODE", "off")),
		Path:                getEnvString("SHANNON_POLICY_PATH", "/app/config/opa/policies"),
		FailClosed:          getEnvBool("SHANNON_POLICY_FAIL_CLOSED", false),
		Environment:         getEnvString("ENVIRONMENT", "dev"),
		EmergencyKillSwitch: getEnvBool("SHANNON_POLICY_EMERGENCY_KILL_SWITCH", false),
		Canary: CanaryConfig{
			Enabled:           getEnvBool("SHANNON_POLICY_CANARY_ENABLED", false),
			EnforcePercentage: getEnvInt("SHANNON_POLICY_CANARY_ENFORCE_PERCENTAGE", 0),
			EnforceUsers:      getEnvStringSlice("SHANNON_POLICY_CANARY_ENFORCE_USERS"),
			EnforceAgents:     getEnvStringSlice("SHANNON_POLICY_CANARY_ENFORCE_AGENTS"),
			DryRunUsers:       getEnvStringSlice("SHANNON_POLICY_CANARY_DRYRUN_USERS"),
			SLOThresholds: SLOConfig{
				MaxErrorRate:    getEnvFloat64("SHANNON_POLICY_SLO_MAX_ERROR_RATE", 5.0),
				MaxLatencyP95:   getEnvFloat64("SHANNON_POLICY_SLO_MAX_LATENCY_P95", 5.0),
				MaxLatencyP50:   getEnvFloat64("SHANNON_POLICY_SLO_MAX_LATENCY_P50", 1.0),
				MinCacheHitRate: getEnvFloat64("SHANNON_POLICY_SLO_MIN_CACHE_HIT_RATE", 80.0),
			},
		},
	}

	// Validate and normalize mode
	// 校验并归一化 mode
	switch config.Mode {
	case ModeOff, ModeDryRun, ModeEnforce:
		// Valid modes
		// 合法模式
	case "":
		config.Mode = ModeOff
	default:
		// Invalid mode, default to off
		// 非法模式，回退为 off
		config.Mode = ModeOff
	}

	// If mode is off, disable the engine
	// mode 为 off 时关闭引擎
	if config.Mode == ModeOff {
		config.Enabled = false
	}

	return config
}

// LoadConfigFromShannon creates policy configuration from Shannon config
// LoadConfigFromShannon 基于 Shannon 配置构造策略配置
func LoadConfigFromShannon(shannonPolicy any) *Config {
	// Start with environment defaults as fallback
	// 先取环境变量默认值作为兜底
	config := LoadConfig()

	// Parse Shannon policy config if provided
	// 若提供 Shannon 策略配置则解析
	if policyMap, ok := shannonPolicy.(map[string]any); ok {
		if enabled, ok := policyMap["enabled"].(bool); ok {
			config.Enabled = enabled
		}
		if mode, ok := policyMap["mode"].(string); ok {
			config.Mode = Mode(mode)
		}
		if path, ok := policyMap["path"].(string); ok {
			config.Path = path
		}
		if failClosed, ok := policyMap["fail_closed"].(bool); ok {
			config.FailClosed = failClosed
		}
		if environment, ok := policyMap["environment"].(string); ok {
			config.Environment = environment
		}
		if emergencyKillSwitch, ok := policyMap["emergency_kill_switch"].(bool); ok {
			config.EmergencyKillSwitch = emergencyKillSwitch
		}

		// Parse canary configuration
		// 解析 canary 配置
		if canaryMap, ok := policyMap["canary"].(map[string]any); ok {
			if enabled, ok := canaryMap["enabled"].(bool); ok {
				config.Canary.Enabled = enabled
			}
			if percentage, ok := canaryMap["enforce_percentage"].(int); ok {
				config.Canary.EnforcePercentage = percentage
			} else if percentageFloat, ok := canaryMap["enforce_percentage"].(float64); ok {
				config.Canary.EnforcePercentage = int(percentageFloat)
			}
			if users, ok := canaryMap["enforce_users"].([]any); ok {
				config.Canary.EnforceUsers = interfaceSliceToStringSlice(users)
			}
			if agents, ok := canaryMap["enforce_agents"].([]any); ok {
				config.Canary.EnforceAgents = interfaceSliceToStringSlice(agents)
			}
			if dryRunUsers, ok := canaryMap["dry_run_users"].([]any); ok {
				config.Canary.DryRunUsers = interfaceSliceToStringSlice(dryRunUsers)
			}

			// Parse SLO thresholds
			// 解析 SLO 阈值
			if sloMap, ok := canaryMap["slo_thresholds"].(map[string]any); ok {
				if maxErrorRate, ok := sloMap["max_error_rate"].(float64); ok {
					config.Canary.SLOThresholds.MaxErrorRate = maxErrorRate
				}
				if maxLatencyP95, ok := sloMap["max_latency_p95"].(float64); ok {
					config.Canary.SLOThresholds.MaxLatencyP95 = maxLatencyP95
				}
				if maxLatencyP50, ok := sloMap["max_latency_p50"].(float64); ok {
					config.Canary.SLOThresholds.MaxLatencyP50 = maxLatencyP50
				}
				if minCacheHitRate, ok := sloMap["min_cache_hit_rate"].(float64); ok {
					config.Canary.SLOThresholds.MinCacheHitRate = minCacheHitRate
				}
			}
		}
	}

	// Validate and normalize mode
	// 校验并归一化 mode
	switch config.Mode {
	case ModeOff, ModeDryRun, ModeEnforce:
		// Valid modes
		// 合法模式
	case "":
		config.Mode = ModeOff
	default:
		// Invalid mode, default to off
		// 非法模式，回退为 off
		config.Mode = ModeOff
	}

	// If mode is off, disable the engine
	// mode 为 off 时关闭引擎
	if config.Mode == ModeOff {
		config.Enabled = false
	}

	return config
}

// getEnvString returns environment variable value or default
// getEnvString 返回环境变量值，未设置时返回默认值
func getEnvString(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// getEnvBool returns environment variable as boolean or default
// getEnvBool 将环境变量解析为布尔值，失败时返回默认值
func getEnvBool(key string, defaultValue bool) bool {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	// Parse common boolean representations
	// 解析常见的布尔值写法
	switch strings.ToLower(value) {
	case "true", "1", "yes", "on", "enable", "enabled":
		return true
	case "false", "0", "no", "off", "disable", "disabled":
		return false
	default:
		// Try parsing as boolean
		// 尝试按布尔值解析
		if parsed, err := strconv.ParseBool(value); err == nil {
			return parsed
		}
		return defaultValue
	}
}

// getEnvInt returns environment variable as integer or default
// getEnvInt 将环境变量解析为整数，失败时返回默认值
func getEnvInt(key string, defaultValue int) int {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	if parsed, err := strconv.Atoi(value); err == nil {
		return parsed
	}
	return defaultValue
}

// getEnvFloat64 returns environment variable as float64 or default
// getEnvFloat64 将环境变量解析为 float64，失败时返回默认值
func getEnvFloat64(key string, defaultValue float64) float64 {
	value := os.Getenv(key)
	if value == "" {
		return defaultValue
	}

	if parsed, err := strconv.ParseFloat(value, 64); err == nil {
		return parsed
	}
	return defaultValue
}

// getEnvStringSlice returns environment variable as string slice (comma-separated) or empty slice
// getEnvStringSlice 将环境变量解析为字符串切片（逗号分隔），未设置时返回空切片
func getEnvStringSlice(key string) []string {
	value := os.Getenv(key)
	if value == "" {
		return []string{}
	}

	// Split by comma and trim whitespace
	// 按逗号切分并去除空白
	parts := strings.Split(value, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// interfaceSliceToStringSlice converts []interface{} to []string
// interfaceSliceToStringSlice 将 []interface{} 转换为 []string
func interfaceSliceToStringSlice(slice []any) []string {
	result := make([]string, 0, len(slice))
	for _, item := range slice {
		if str, ok := item.(string); ok && str != "" {
			result = append(result, str)
		}
	}
	return result
}
