package policy

import (
	"container/list"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/open-policy-agent/opa/rego"
	"go.uber.org/zap"
)

// Engine defines the policy evaluation interface
// Engine 定义策略评估接口
type Engine interface {
	Evaluate(ctx context.Context, input *PolicyInput) (*Decision, error)
	LoadPolicies() error
	IsEnabled() bool
	// Environment returns the configured environment (e.g., dev|staging|prod)
	// Environment 返回已配置的 environment（例如 dev|staging|prod）
	Environment() string
	// Mode returns the current enforcement mode (off|dry-run|enforce)
	// Mode 返回当前执行模式（off|dry-run|enforce）
	Mode() Mode
}

// PolicyInput represents the input context for policy evaluation
// PolicyInput 表示策略评估的输入上下文
type PolicyInput struct {
	// Core identifiers
	// 核心标识
	SessionID string `json:"session_id"`
	UserID    string `json:"user_id,omitempty"`
	AgentID   string `json:"agent_id"`

	// Request details
	// 请求详情
	Query   string         `json:"query"`
	Mode    string         `json:"mode"` // simple, standard, complex 注：simple、standard、complex
	Context map[string]any `json:"context,omitempty"`

	// Security context
	// 安全上下文
	Environment string `json:"environment"` // dev, staging, prod 注：dev、staging、prod
	IPAddress   string `json:"ip_address,omitempty"`

	// Resource constraints
	// 资源约束
	ComplexityScore float64 `json:"complexity_score,omitempty"`
	TokenBudget     int     `json:"token_budget,omitempty"`

	// Vector-enhanced fields (optional, feature-gated)
	// 向量增强字段（可选，特性开关控制）
	SimilarQueries  []SimilarQuery `json:"similar_queries,omitempty"`
	ContextScore    float64        `json:"context_score,omitempty"`
	SemanticCluster string         `json:"semantic_cluster,omitempty"`
	RiskProfile     string         `json:"risk_profile,omitempty"`

	// Timestamp
	// 时间戳
	Timestamp time.Time `json:"timestamp"`
}

// SimilarQuery represents a semantically similar historical query or decision context
// SimilarQuery 表示语义相似的历史 query 或决策上下文
type SimilarQuery struct {
	Query      string    `json:"query"`
	Outcome    string    `json:"outcome"`
	Confidence float64   `json:"confidence"`
	Timestamp  time.Time `json:"timestamp"`
}

// Decision represents the policy evaluation result
// Decision 表示策略评估结果
type Decision struct {
	// Core decision
	// 核心决策
	Allow  bool   `json:"allow"`
	Reason string `json:"reason,omitempty"`

	// Obligations (future phase)
	// 义务（后续阶段）
	RequireApproval bool           `json:"require_approval,omitempty"`
	Obligations     map[string]any `json:"obligations,omitempty"`

	// Audit
	// 审计
	PolicyVersion string            `json:"policy_version,omitempty"`
	AuditTags     map[string]string `json:"audit_tags,omitempty"`
}

// OPAEngine implements the Engine interface using OPA rego
// OPAEngine 使用 OPA rego 实现 Engine 接口
type OPAEngine struct {
	config   *Config
	logger   *zap.Logger
	compiled *rego.PreparedEvalQuery
	enabled  bool
	// simple in-memory LRU cache for decisions
	// 用于决策的简单内存 LRU cache
	cache *decisionCache
}

// NewOPAEngine creates a new OPA-based policy engine
// NewOPAEngine 创建基于 OPA 的策略引擎
func NewOPAEngine(config *Config, logger *zap.Logger) (*OPAEngine, error) {
	engine := &OPAEngine{
		config:  config,
		logger:  logger,
		enabled: config.Enabled && config.Mode != ModeOff,
		cache:   newDecisionCache(1000, 5*time.Minute), // 1K entries, 5min TTL 注：1000 条，TTL 5 分钟
	}

	if engine.enabled {
		if err := engine.LoadPolicies(); err != nil {
			if config.FailClosed {
				return nil, fmt.Errorf("failed to load policies in fail-closed mode: %w", err)
			}
			logger.Warn("Failed to load policies, running in fail-open mode", zap.Error(err))
			engine.enabled = false
		}
	}

	return engine, nil
}

// LoadPolicies loads and compiles all policy files from the configured directory
// LoadPolicies 从配置目录加载并编译所有策略文件
func (e *OPAEngine) LoadPolicies() error {
	if !e.config.Enabled {
		return nil
	}

	policies := make(map[string]string)

	// Load all .rego files from the policy directory
	// 从策略目录加载所有 .rego 文件
	err := filepath.Walk(e.config.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if !info.IsDir() && strings.HasSuffix(info.Name(), ".rego") {
			content, err := os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("failed to read policy file %s: %w", path, err)
			}

			// Use relative path as module name
			// 使用相对路径作为 module 名
			relPath, _ := filepath.Rel(e.config.Path, path)
			moduleName := strings.TrimSuffix(relPath, ".rego")
			policies[moduleName] = string(content)

			e.logger.Debug("Loaded policy file",
				zap.String("path", path),
				zap.String("module", moduleName),
			)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to walk policy directory: %w", err)
	}

	if len(policies) == 0 {
		e.logger.Warn("No policy files found", zap.String("path", e.config.Path))
		if e.config.FailClosed {
			return fmt.Errorf("no policies found in fail-closed mode")
		}
		return nil
	}

	// Compile policies
	// 编译策略
	regoOptions := []func(*rego.Rego){
		rego.Query("data.shannon.task.decision"),
	}

	for moduleName, content := range policies {
		regoOptions = append(regoOptions, rego.Module(moduleName, content))
	}

	regoBuilder := rego.New(regoOptions...)

	compiled, err := regoBuilder.PrepareForEval(context.Background())
	if err != nil {
		return fmt.Errorf("failed to compile policies: %w", err)
	}

	e.compiled = &compiled

	e.logger.Info("Policies loaded and compiled successfully",
		zap.Int("policy_count", len(policies)),
		zap.String("decision_query", "data.shannon.task.decision"),
	)

	// Record policy load metrics
	// 记录策略加载指标
	RecordPolicyLoad(e.config.Path, len(policies), float64(time.Now().Unix()))

	// Record policy version for tracking deployments
	// 记录策略版本用于追踪部署
	versionHash := e.calculatePolicyVersion(policies)
	loadTimestamp := fmt.Sprintf("%d", time.Now().Unix())
	RecordPolicyVersion(e.config.Path, versionHash, loadTimestamp)

	return nil
}

// Evaluate evaluates the policy against the given input
// Evaluate 针对给定输入评估策略
func (e *OPAEngine) Evaluate(ctx context.Context, input *PolicyInput) (*Decision, error) {
	startTime := time.Now()

	// Default decision based on configuration
	// 基于配置的默认决策
	defaultDecision := &Decision{
		Allow:  !e.config.FailClosed, // fail-open allows by default, fail-closed denies 注：fail-open 默认放行，fail-closed 默认拒绝
		Reason: "policy engine disabled or no policies loaded",
		AuditTags: map[string]string{
			"policy_enabled": fmt.Sprintf("%t", e.enabled),
			"mode":           string(e.config.Mode),
		},
	}

	if !e.enabled || e.compiled == nil {
		e.logger.Debug("Policy evaluation skipped",
			zap.Bool("enabled", e.enabled),
			zap.Bool("compiled", e.compiled != nil),
		)
		return defaultDecision, nil
	}

	// Try cache first
	// 先尝试 cache
	if d, ok := e.cache.Get(input); ok {
		// Record cache hit metrics
		// 记录 cache 命中指标
		RecordCacheHit(string(e.config.Mode))
		RecordSLOLatency(string(e.config.Mode), true, time.Since(startTime).Seconds())
		return d, nil
	}

	// Record cache miss
	// 记录 cache 未命中
	RecordCacheMiss(string(e.config.Mode))

	// Convert input to map for OPA
	// 将 input 转换为 map 供 OPA 使用
	inputMap, err := e.inputToMap(input)
	if err != nil {
		e.logger.Error("Failed to convert input to map", zap.Error(err))
		// Record conversion error
		// 记录转换错误
		RecordSLOError("input_conversion", string(e.config.Mode))
		RecordError("input_conversion", string(e.config.Mode))

		if e.config.FailClosed {
			return &Decision{Allow: false, Reason: "input conversion failed"}, err
		}
		return defaultDecision, nil
	}

	// Evaluate policy
	// 评估策略
	results, err := e.compiled.Eval(ctx, rego.EvalInput(inputMap))
	if err != nil {
		e.logger.Error("Policy evaluation failed", zap.Error(err))
		// Record evaluation error
		// 记录评估错误
		RecordSLOError("policy_evaluation", string(e.config.Mode))
		RecordError("policy_evaluation", string(e.config.Mode))

		if e.config.FailClosed {
			return &Decision{Allow: false, Reason: "policy evaluation error"}, err
		}
		return defaultDecision, nil
	}

	// Parse results
	// 解析结果
	decision := e.parseResults(results, input)
	originalDecision := *decision // Save original for comparison metrics 注：保存原始决策用于对比指标

	// Apply canary enforcement logic
	// 应用 canary 执行逻辑
	effectiveMode := e.determineEffectiveMode(input)
	decision = e.applyModeToDecision(decision, effectiveMode, input)

	// Record comprehensive metrics
	// 记录完整指标
	duration := time.Since(startTime)
	e.recordComprehensiveMetrics(input, &originalDecision, decision, effectiveMode, duration)

	e.logger.Debug("Policy evaluated",
		zap.Bool("allow", decision.Allow),
		zap.String("reason", decision.Reason),
		zap.Duration("duration", duration),
		zap.String("session_id", input.SessionID),
		zap.String("agent_id", input.AgentID),
		zap.String("effective_mode", string(effectiveMode)),
	)

	// Store in cache
	// 写入 cache
	e.cache.Set(input, decision)
	return decision, nil
}

// IsEnabled returns whether the policy engine is enabled and ready
// IsEnabled 返回策略引擎是否已启用并就绪
func (e *OPAEngine) IsEnabled() bool {
	return e.enabled && e.compiled != nil
}

// Environment returns the configured environment for the engine
// Environment 返回引擎已配置的 environment
func (e *OPAEngine) Environment() string { return e.config.Environment }

// Mode returns the configured enforcement mode for the engine
// Mode 返回引擎已配置的执行模式
func (e *OPAEngine) Mode() Mode { return e.config.Mode }

// inputToMap converts PolicyInput to a map for OPA evaluation
// inputToMap 将 PolicyInput 转换为 map 以便 OPA 评估
func (e *OPAEngine) inputToMap(input *PolicyInput) (map[string]any, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, err
	}

	return result, nil
}

// parseResults parses OPA evaluation results into a Decision
// parseResults 将 OPA 评估结果解析为 Decision
func (e *OPAEngine) parseResults(results rego.ResultSet, input *PolicyInput) *Decision {
	decision := &Decision{
		Allow:  false, // Default deny 注：默认拒绝
		Reason: "no matching policy rules",
		AuditTags: map[string]string{
			"session_id": input.SessionID,
			"agent_id":   input.AgentID,
			"mode":       input.Mode,
		},
	}

	if len(results) == 0 {
		e.logger.Debug("No policy results returned")
		return decision
	}

	// Parse the first result
	// 解析第一个结果
	result := results[0]
	if len(result.Expressions) == 0 {
		return decision
	}

	// Try to parse structured decision
	// 尝试解析结构化决策
	value := result.Expressions[0].Value
	if valueMap, ok := value.(map[string]any); ok {
		if allow, ok := valueMap["allow"].(bool); ok {
			decision.Allow = allow
		}
		if reason, ok := valueMap["reason"].(string); ok {
			decision.Reason = reason
		}
		if requireApproval, ok := valueMap["require_approval"].(bool); ok {
			decision.RequireApproval = requireApproval
		}
		if obligations, ok := valueMap["obligations"].(map[string]any); ok {
			decision.Obligations = obligations
		}
	} else if allow, ok := value.(bool); ok {
		// Simple boolean result
		// 简单的布尔结果
		decision.Allow = allow
		if allow {
			decision.Reason = "allowed by policy"
		} else {
			decision.Reason = "denied by policy"
		}
	}

	return decision
}

// --- internal decision cache (simple LRU with TTL) ---
// --- 内部决策 cache（带 TTL 的简单 LRU）---

// The cache key includes environment, mode, user, agent, token budget, rounded complexity and a hash of the query
// to avoid query-pattern related false positives.
// cache 键包含 environment、mode、user、agent、token budget、四舍五入后的 complexity 以及 query 的哈希，以避免因 query 模式差异导致误命中。

type decisionCache struct {
	cap    int
	ttl    time.Duration
	mu     sync.Mutex
	list   *list.List               // MRU at front 注：MRU 在队首
	m      map[string]*list.Element // key -> element 注：key -> element
	hits   atomic.Int64
	misses atomic.Int64
}

type cacheEntry struct {
	key       string
	expiresAt time.Time
	decision  *Decision
}

func newDecisionCache(cap int, ttl time.Duration) *decisionCache {
	if cap <= 0 {
		cap = 1024
	}
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	return &decisionCache{
		cap:  cap,
		ttl:  ttl,
		list: list.New(),
		m:    make(map[string]*list.Element),
	}
}

func (c *decisionCache) makeKey(input *PolicyInput) string {
	// Hash query to keep key small
	// 对 query 做哈希以保持 cache 键足够小
	h := fnv.New64a()
	_, _ = h.Write([]byte(strings.ToLower(input.Query)))
	qh := h.Sum64()
	// Round complexity to 2 decimals to reduce key churn
	// complexity 保留两位小数，减少 cache 键的抖动
	comp := fmt.Sprintf("%.2f", input.ComplexityScore)
	return fmt.Sprintf("%s|%s|%s|%s|%d|%s|%x",
		input.Environment, input.Mode, input.UserID, input.AgentID, input.TokenBudget, comp, qh,
	)
}

func (c *decisionCache) Get(input *PolicyInput) (*Decision, bool) {
	key := c.makeKey(input)
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[key]; ok {
		ce := el.Value.(cacheEntry)
		if ce.expiresAt.After(now) {
			c.list.MoveToFront(el)
			c.hits.Add(1)
			return ce.decision, true
		}
		// expired
		// 已过期
		c.list.Remove(el)
		delete(c.m, key)
	}
	c.misses.Add(1)
	return nil, false
}

func (c *decisionCache) Set(input *PolicyInput, d *Decision) {
	key := c.makeKey(input)
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.m[key]; ok {
		// update existing
		// 更新已有条目
		el.Value = cacheEntry{key: key, expiresAt: time.Now().Add(c.ttl), decision: d}
		c.list.MoveToFront(el)
		return
	}
	// insert new
	// 插入新条目
	el := c.list.PushFront(cacheEntry{key: key, expiresAt: time.Now().Add(c.ttl), decision: d})
	c.m[key] = el
	if c.list.Len() > c.cap {
		// evict LRU
		// 淘汰 LRU
		lru := c.list.Back()
		if lru != nil {
			ce := lru.Value.(cacheEntry)
			delete(c.m, ce.key)
			c.list.Remove(lru)
		}
	}
}

// Stats returns cumulative cache hit/miss counts
// Stats 返回累计的 cache 命中/未命中计数
func (c *decisionCache) Stats() (hits, misses int64) {
	return c.hits.Load(), c.misses.Load()
}

// --- Canary Enforcement Implementation ---
// --- Canary 执行实现 ---

// determineEffectiveMode determines the effective enforcement mode for a specific request
// determineEffectiveMode 为单个请求确定实际生效的执行模式
// based on canary configuration, emergency controls, and rollout rules
// 依据 canary 配置、紧急控制开关与灰度规则
func (e *OPAEngine) determineEffectiveMode(input *PolicyInput) Mode {
	// Emergency kill switch overrides everything - forces dry-run
	// 紧急开关优先级最高——强制走 dry-run
	if e.config.EmergencyKillSwitch {
		e.logger.Debug("Emergency kill switch active - forcing dry-run mode",
			zap.String("user_id", input.UserID),
			zap.String("agent_id", input.AgentID),
		)
		return ModeDryRun
	}

	// If canary is not enabled, use the configured global mode
	// 若未启用 canary，则使用配置的全局 mode
	if !e.config.Canary.Enabled {
		return e.config.Mode
	}

	// Check explicit dry-run user overrides (these always get dry-run)
	// 检查显式 dry-run 用户覆盖（这些用户始终走 dry-run）
	if slices.Contains(e.config.Canary.DryRunUsers, input.UserID) {
		e.logger.Debug("User in dry-run override list",
			zap.String("user_id", input.UserID),
		)
		return ModeDryRun
	}

	// Check explicit enforce user overrides (these always get enforce mode)
	// 检查显式 enforce 用户覆盖（这些用户始终走 enforce 模式）
	if slices.Contains(e.config.Canary.EnforceUsers, input.UserID) {
		e.logger.Debug("User in enforce list - using enforce mode",
			zap.String("user_id", input.UserID),
		)
		return ModeEnforce
	}

	// Check explicit enforce agent overrides (these always get enforce mode)
	// 检查显式 enforce agent 覆盖（这些始终走 enforce 模式）
	if slices.Contains(e.config.Canary.EnforceAgents, input.AgentID) {
		e.logger.Debug("Agent in enforce list - using enforce mode",
			zap.String("agent_id", input.AgentID),
		)
		return ModeEnforce
	}

	// Apply percentage-based rollout using deterministic hash
	// 使用确定性哈希按百分比灰度放量
	if e.config.Canary.EnforcePercentage > 0 {
		// Create deterministic hash based on user and agent to ensure consistent routing
		// 基于 user 与 agent 构造确定性哈希，确保路由一致
		hash := e.calculateCanaryHash(input.UserID, input.AgentID, input.SessionID)
		percentage := int(hash % 100)

		if percentage < e.config.Canary.EnforcePercentage {
			e.logger.Debug("Request selected for enforce mode via percentage rollout",
				zap.String("user_id", input.UserID),
				zap.String("agent_id", input.AgentID),
				zap.Int("percentage", percentage),
				zap.Int("enforce_threshold", e.config.Canary.EnforcePercentage),
			)
			return ModeEnforce
		}

		e.logger.Debug("Request selected for dry-run mode via percentage rollout",
			zap.String("user_id", input.UserID),
			zap.String("agent_id", input.AgentID),
			zap.Int("percentage", percentage),
			zap.Int("enforce_threshold", e.config.Canary.EnforcePercentage),
		)
	}

	// Default to dry-run for safety during canary rollout
	// canary 灰度期间默认走 dry-run 以确保安全
	return ModeDryRun
}

// applyModeToDecision applies the effective enforcement mode to the policy decision
// applyModeToDecision 将实际生效的执行模式应用到策略决策
func (e *OPAEngine) applyModeToDecision(decision *Decision, effectiveMode Mode, input *PolicyInput) *Decision {
	// Add audit tags for tracking
	// 增加审计 tag 以便追踪
	if decision.AuditTags == nil {
		decision.AuditTags = make(map[string]string)
	}
	decision.AuditTags["effective_mode"] = string(effectiveMode)
	decision.AuditTags["configured_mode"] = string(e.config.Mode)
	decision.AuditTags["canary_enabled"] = fmt.Sprintf("%t", e.config.Canary.Enabled)

	switch effectiveMode {
	case ModeEnforce:
		// In enforce mode, respect the policy decision as-is
		// enforce 模式下按策略决策原样执行
		e.logger.Debug("Enforcing policy decision",
			zap.Bool("allow", decision.Allow),
			zap.String("reason", decision.Reason),
			zap.String("user_id", input.UserID),
		)
		return decision

	case ModeDryRun:
		// In dry-run mode, always allow but log what would have happened
		// dry-run 模式下始终放行，但记录本应发生的决策
		originalDecision := *decision // Copy original decision 注：复制原始决策

		decision.Allow = true // Override to allow in dry-run 注：dry-run 下覆盖为放行
		if !originalDecision.Allow {
			// Add dry-run context to reason
			// 在 reason 中补充 dry-run 上下文
			decision.Reason = fmt.Sprintf("DRY-RUN: would have been denied - %s", originalDecision.Reason)
		} else {
			decision.Reason = fmt.Sprintf("DRY-RUN: would have been allowed - %s", originalDecision.Reason)
		}

		// Log dry-run decision for analysis
		// 记录 dry-run 决策以便分析
		e.logger.Info("Dry-run policy evaluation",
			zap.Bool("would_allow", originalDecision.Allow),
			zap.Bool("actual_allow", decision.Allow),
			zap.String("original_reason", originalDecision.Reason),
			zap.String("dry_run_reason", decision.Reason),
			zap.String("user_id", input.UserID),
			zap.String("agent_id", input.AgentID),
		)

		return decision

	case ModeOff:
		// Engine should be disabled if mode is off, but handle gracefully
		// mode 为 off 时引擎应已禁用，此处做容错处理
		decision.Allow = !e.config.FailClosed
		decision.Reason = "policy engine disabled"
		return decision

	default:
		// Unknown mode, default to safe behavior
		// 未知 mode，采用安全默认行为
		e.logger.Warn("Unknown effective mode, defaulting to dry-run",
			zap.String("mode", string(effectiveMode)),
		)
		decision.Allow = true
		decision.Reason = fmt.Sprintf("unknown mode %s, defaulting to allow", effectiveMode)
		return decision
	}
}

// calculateCanaryHash creates a deterministic hash for canary percentage calculation
// calculateCanaryHash 为 canary 百分比计算生成确定性哈希
func (e *OPAEngine) calculateCanaryHash(userID, agentID, sessionID string) uint32 {
	// Create a deterministic string for hashing
	// 构造用于哈希的确定性字符串
	// Include user and agent for consistent routing, but not session to avoid per-session variance
	// 哈希包含 user 与 agent 以保证路由一致，但不含 session，避免同一用户在不同 session 间漂移
	hashInput := fmt.Sprintf("%s|%s", userID, agentID)

	// Use MD5 for consistent percentage distribution
	// 使用 MD5 以保证百分比分布稳定
	h := md5.Sum([]byte(hashInput))

	// Convert first 4 bytes to uint32
	// 将前 4 个字节转换为 uint32
	return uint32(h[0])<<24 | uint32(h[1])<<16 | uint32(h[2])<<8 | uint32(h[3])
}

// recordComprehensiveMetrics records all detailed metrics for dashboard and SLO tracking
// recordComprehensiveMetrics 记录所有明细指标，供看板与 SLO 追踪使用
func (e *OPAEngine) recordComprehensiveMetrics(input *PolicyInput, originalDecision, finalDecision *Decision, effectiveMode Mode, duration time.Duration) {
	// Basic evaluation metrics
	// 基础评估指标
	decisionLabel := "allow"
	if !finalDecision.Allow {
		decisionLabel = "deny"
	}

	// Record basic evaluation
	// 记录基础评估
	RecordEvaluation(decisionLabel, string(effectiveMode), finalDecision.Reason)
	RecordEvaluationDuration(string(effectiveMode), duration.Seconds())
	RecordSLOLatency(string(effectiveMode), false, duration.Seconds()) // false = cache miss 注：false = cache 未命中

	// Record canary routing decision
	// 记录 canary 路由决策
	routingReason := e.determineCanaryRoutingReason(input, effectiveMode)
	RecordCanaryDecision(string(e.config.Mode), string(effectiveMode), routingReason, decisionLabel)

	// Record top deny reasons if denied
	// 若被拒绝，记录主要拒绝原因
	if !finalDecision.Allow {
		RecordDenyReason(finalDecision.Reason, string(effectiveMode))
	}

	// Record mode comparison for dry-run analysis
	// 记录 mode 对比以用于 dry-run 分析
	originalDecisionLabel := "allow"
	if !originalDecision.Allow {
		originalDecisionLabel = "deny"
	}

	userType := e.classifyUserType(input)
	RecordModeComparison(originalDecisionLabel, string(effectiveMode), originalDecisionLabel, userType)

	// Record dry-run divergence if applicable
	// 如适用，记录 dry-run 偏离情况
	if effectiveMode == ModeDryRun && originalDecisionLabel != decisionLabel {
		if originalDecisionLabel == "deny" {
			RecordDryRunDivergence("would_deny")
		} else {
			RecordDryRunDivergence("would_allow")
		}
	}

	// Record cache size periodically (every 100th request to avoid overhead)
	// 周期性记录 cache 大小（每 100 次请求一次，避免额外开销）
	if e.cache != nil {
		e.cache.mu.Lock()
		cacheSize := e.cache.list.Len()
		e.cache.mu.Unlock()
		RecordCacheSize("policy_decisions", cacheSize)
	}
}

// determineCanaryRoutingReason determines why a request was routed to enforce vs dry-run
// determineCanaryRoutingReason 判断请求被路由到 enforce 还是 dry-run 的原因
func (e *OPAEngine) determineCanaryRoutingReason(input *PolicyInput, effectiveMode Mode) string {
	if e.config.EmergencyKillSwitch {
		return "emergency_kill_switch"
	}

	if !e.config.Canary.Enabled {
		return "canary_disabled"
	}

	// Check explicit overrides
	// 检查显式覆盖
	if slices.Contains(e.config.Canary.DryRunUsers, input.UserID) {
		return "explicit_dry_run_user"
	}

	if slices.Contains(e.config.Canary.EnforceUsers, input.UserID) {
		return "explicit_enforce_user"
	}

	if slices.Contains(e.config.Canary.EnforceAgents, input.AgentID) {
		return "explicit_enforce_agent"
	}

	if e.config.Canary.EnforcePercentage > 0 {
		return "percentage_rollout"
	}

	return "default_dry_run"
}

// classifyUserType categorizes users for metrics
// classifyUserType 对用户分类以用于指标统计
func (e *OPAEngine) classifyUserType(input *PolicyInput) string {
	// Check if user is in enforce lists (privileged)
	// 检查用户是否在 enforce 名单中（特权用户）
	if slices.Contains(e.config.Canary.EnforceUsers, input.UserID) {
		return "enforce_user"
	}

	// Check if user is in dry-run override list
	// 检查用户是否在 dry-run 覆盖名单中
	if slices.Contains(e.config.Canary.DryRunUsers, input.UserID) {
		return "dry_run_user"
	}

	// Check if it's a system user
	// 检查是否为系统用户
	if input.UserID == "" || input.UserID == "system" || input.UserID == "orchestrator" {
		return "system_user"
	}

	return "regular_user"
}

// calculatePolicyVersion creates a version hash from policy content for tracking
// calculatePolicyVersion 依据策略内容生成版本哈希以便追踪
func (e *OPAEngine) calculatePolicyVersion(policies map[string]string) string {
	// Create a deterministic hash from all policy content
	// 依据所有策略内容生成确定性哈希
	h := md5.New()

	// Sort policy names for deterministic ordering
	// 对策略名排序以保证顺序确定
	for _, name := range slices.Sorted(maps.Keys(policies)) {
		h.Write([]byte(name))
		h.Write([]byte(policies[name]))
	}

	// Return first 8 hex chars for reasonable uniqueness
	// 返回前 8 个十六进制字符，保证足够的唯一性
	return fmt.Sprintf("%x", h.Sum(nil)[:4])
}
