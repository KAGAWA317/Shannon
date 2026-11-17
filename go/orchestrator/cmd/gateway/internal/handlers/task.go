package handlers

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/attachments"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	commonpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/common"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/skills"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
	"gopkg.in/yaml.v3"
)

// ErrWorkspaceQuotaExceeded is returned when tenant workspace quota is exceeded
// ErrWorkspaceQuotaExceeded 表示 tenant 的 workspace 配额已超限。
var ErrWorkspaceQuotaExceeded = errors.New("workspace quota exceeded")

// TaskHandler handles task-related HTTP requests
// TaskHandler 处理与 task 相关的 HTTP 请求。
type TaskHandler struct {
	orchClient orchpb.OrchestratorServiceClient
	db         *sqlx.DB
	redis      *redis.Client
	skills     *skills.SkillRegistry
	logger     *zap.Logger
	sessionMgr *session.Manager // For persisting HITL messages to session history 注：用于把 HITL 消息持久化到 session history
}

// ResearchStrategiesConfig represents research strategy presets loaded from YAML
// ResearchStrategiesConfig 表示从 YAML 加载的 research strategy preset。
type ResearchStrategiesConfig struct {
	Strategies map[string]struct {
		MaxIterations            int    `yaml:"max_iterations"` // deprecated 注：已废弃
		VerificationEnabled      bool   `yaml:"verification_enabled"`
		MaxConcurrentAgents      int    `yaml:"max_concurrent_agents"`
		ReactMaxIterations       int    `yaml:"react_max_iterations"`
		BudgetAgentMin           int    `yaml:"budget_agent_min"`
		GapFillingEnabled        bool   `yaml:"gap_filling_enabled"`
		GapFillingMaxGaps        int    `yaml:"gap_filling_max_gaps"`
		GapFillingMaxIterations  int    `yaml:"gap_filling_max_iterations"`
		GapFillingCheckCitations bool   `yaml:"gap_filling_check_citations"`
		AgentModelTier           string `yaml:"agent_model_tier"`           // small/medium/large for agent execution 注：agent 执行时使用的 small/medium/large 档位
		IterativeMaxIterations   int    `yaml:"iterative_max_iterations"`   // coverage evaluation iterations (1-5) 注：覆盖率评估的迭代次数（1-5）
		IterativeResearchEnabled *bool  `yaml:"iterative_research_enabled"` // DR 2.0 iterative loop (nil = use default true) 注：DR 2.0 迭代循环（nil 表示使用默认值 true）
	} `yaml:"strategies"`
}

// researchStrategiesPtr holds the latest parsed config, atomically swapped by
// a background goroutine every 30 seconds. Readers call loadResearchStrategies()
// which is a simple atomic load — no lock, no I/O on the hot path.
// researchStrategiesPtr 保存最新解析结果，由后台 goroutine 每 30 秒原子替换；
// 读取方调用 loadResearchStrategies()，热路径上只有一次原子读，无锁、无 I/O。
var researchStrategiesPtr atomic.Pointer[ResearchStrategiesConfig]

func init() {
	// Eagerly load once at startup so the first request is never empty.
	// 启动时先加载一次，保证首个 request 不会拿到空配置。
	if cfg := readResearchStrategiesFromDisk(); cfg != nil {
		researchStrategiesPtr.Store(cfg)
	} else {
		log.Printf("[warn] research_strategies.yaml not found at startup; strategy presets disabled")
	}

	// Background reloader — picks up YAML changes without Gateway restart.
	// 后台重载：无需重启 Gateway 即可感知 YAML 变更。
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if cfg := readResearchStrategiesFromDisk(); cfg != nil {
				researchStrategiesPtr.Store(cfg)
			}
		}
	}()
}

// readResearchStrategiesFromDisk tries the standard candidate paths and returns
// the parsed config, or nil on any error.
// NOTE: Uses stdlib log (not zap) because this runs from init() before zap is configured.
// readResearchStrategiesFromDisk 依次尝试标准候选路径，成功则返回解析后的配置，任何错误都返回 nil。
// 注意：这里用标准库 log 而非 zap，因为它在 init() 中执行，此时 zap 尚未配置。
func readResearchStrategiesFromDisk() *ResearchStrategiesConfig {
	candidates := []string{"config/research_strategies.yaml", "/app/config/research_strategies.yaml"}
	for _, p := range candidates {
		if _, statErr := os.Stat(p); statErr != nil {
			continue
		}
		data, err := os.ReadFile(p)
		if err != nil {
			log.Printf("[warn] failed to read %s: %v", p, err)
			continue
		}
		var cfg ResearchStrategiesConfig
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			log.Printf("[warn] failed to parse %s: %v", p, err)
			continue
		}
		return &cfg
	}
	return nil
}

// loadResearchStrategies returns the latest cached config (lock-free).
// loadResearchStrategies 返回最新 cache 的配置（无锁读取）。
func loadResearchStrategies() (*ResearchStrategiesConfig, error) {
	cfg := researchStrategiesPtr.Load()
	if cfg == nil {
		return nil, fmt.Errorf("research_strategies.yaml not loaded")
	}
	return cfg, nil
}

// applyStrategyPreset seeds ctxMap with preset defaults when absent
// applyStrategyPreset 在 ctxMap 缺少对应键时，用 preset 的默认值填充。
func applyStrategyPreset(ctxMap map[string]any, strategy string) {
	s := strings.ToLower(strings.TrimSpace(strategy))
	if s == "" {
		return
	}
	cfg, err := loadResearchStrategies()
	if err != nil || cfg == nil || cfg.Strategies == nil {
		return
	}
	preset, ok := cfg.Strategies[s]
	if !ok {
		return
	}
	// Seed react_max_iterations (independent of deprecated max_iterations)
	// 填充 react_max_iterations（与已废弃的 max_iterations 相互独立）。
	if _, ok := ctxMap["react_max_iterations"]; !ok && preset.ReactMaxIterations >= 1 && preset.ReactMaxIterations <= 10 {
		ctxMap["react_max_iterations"] = preset.ReactMaxIterations
	}
	// Seed max_concurrent_agents
	// 填充 max_concurrent_agents。
	if _, ok := ctxMap["max_concurrent_agents"]; !ok && preset.MaxConcurrentAgents >= 1 && preset.MaxConcurrentAgents <= 20 {
		ctxMap["max_concurrent_agents"] = preset.MaxConcurrentAgents
	}
	// Seed budget_agent_min (minimum per-agent token budget)
	// 填充 budget_agent_min（每个 agent 的最小 token budget）。
	if _, ok := ctxMap["budget_agent_min"]; !ok && preset.BudgetAgentMin > 0 {
		ctxMap["budget_agent_min"] = preset.BudgetAgentMin
	}
	// Seed enable_verification
	// 填充 enable_verification。
	if _, ok := ctxMap["enable_verification"]; !ok {
		ctxMap["enable_verification"] = preset.VerificationEnabled
	}
	// Seed gap filling settings (always apply, not gated by max_iterations)
	// 填充 gap filling 相关设置：始终生效，不受 max_iterations 控制。
	if _, ok := ctxMap["gap_filling_enabled"]; !ok {
		ctxMap["gap_filling_enabled"] = preset.GapFillingEnabled
	}
	if _, ok := ctxMap["gap_filling_max_gaps"]; !ok && preset.GapFillingMaxGaps > 0 {
		ctxMap["gap_filling_max_gaps"] = preset.GapFillingMaxGaps
	}
	if _, ok := ctxMap["gap_filling_max_iterations"]; !ok && preset.GapFillingMaxIterations > 0 {
		ctxMap["gap_filling_max_iterations"] = preset.GapFillingMaxIterations
	}
	if _, ok := ctxMap["gap_filling_check_citations"]; !ok {
		ctxMap["gap_filling_check_citations"] = preset.GapFillingCheckCitations
	}
	// Seed model_tier from agent_model_tier (only if not already set by user)
	// 由 agent_model_tier 填充 model_tier（仅当用户未显式设置时）。
	if _, ok := ctxMap["model_tier"]; !ok && preset.AgentModelTier != "" {
		ctxMap["model_tier"] = preset.AgentModelTier
	}
	// Seed iterative_research_enabled (DR 2.0 iterative loop; only seed when explicitly configured)
	// 填充 iterative_research_enabled（DR 2.0 的迭代循环；仅在配置显式给出时才填充）。
	if _, ok := ctxMap["iterative_research_enabled"]; !ok && preset.IterativeResearchEnabled != nil {
		ctxMap["iterative_research_enabled"] = *preset.IterativeResearchEnabled
	}
	// Seed iterative_max_iterations for coverage evaluation loop (1-5)
	// 为覆盖率评估循环填充 iterative_max_iterations（取值 1-5）。
	if _, ok := ctxMap["iterative_max_iterations"]; !ok && preset.IterativeMaxIterations >= 1 && preset.IterativeMaxIterations <= 5 {
		ctxMap["iterative_max_iterations"] = preset.IterativeMaxIterations
	}
}

// applyTaskContextAndLabels normalizes and validates task context and mode, then
// applies them to the gRPC request. Returns false if a validation error response
// has already been sent to the client.
// applyTaskContextAndLabels 先规范化并校验 task context 与 mode，再写入 gRPC request；
// 若已向客户端返回校验错误则返回 false。
func (h *TaskHandler) applyTaskContextAndLabels(req *TaskRequest, grpcReq *orchpb.SubmitTaskRequest, w http.ResponseWriter, r *http.Request) bool {
	// Ensure context map exists so we can inject optional fields safely
	// 确保 context map 存在，以便安全注入可选字段。
	ctxMap := map[string]any{}
	if len(req.Context) > 0 {
		maps.Copy(ctxMap, req.Context)
	}

	// Normalize alias: context.template_name -> context.template (if not already set)
	// 别名归一化：context.template_name → context.template（仅在尚未设置时）。
	if _, ok := ctxMap["template"]; !ok {
		if v, ok2 := ctxMap["template_name"].(string); ok2 {
			if tv := strings.TrimSpace(v); tv != "" {
				ctxMap["template"] = tv
			}
		}
	}

	// Expand skill into context if specified
	// 若指定了 skill，则把它展开到 context 中。
	if skillName := strings.TrimSpace(req.Skill); skillName != "" && h.skills != nil {
		entry, ok := h.skills.Get(skillName)
		if !ok {
			h.sendError(w, fmt.Sprintf("Unknown skill: %s", skillName), http.StatusBadRequest)
			return false
		}
		skill := entry.Skill

		// Check if skill is enabled
		// 检查该 skill 是否已启用。
		if !skill.Enabled {
			h.sendError(w, fmt.Sprintf("Skill %s is disabled", skillName), http.StatusBadRequest)
			return false
		}

		// SECURITY: Check authorization for dangerous skills
		// Dangerous skills require admin/owner role OR explicit skills:dangerous scope
		// 安全：校验危险 skill 的授权——需要 admin/owner 角色，或显式拥有 skills:dangerous scope。
		if skill.Dangerous {
			// Get userCtx from request context (already validated in SubmitTask)
			// 从 request context 中取 userCtx（在 SubmitTask 中已校验）。
			userCtx, _ := r.Context().Value(auth.UserContextKey).(*auth.UserContext)
			authorized := false
			if userCtx != nil {
				// Admin and owner roles can use dangerous skills
				// admin 与 owner 角色可以使用危险 skill。
				if userCtx.Role == auth.RoleAdmin || userCtx.Role == auth.RoleOwner {
					authorized = true
				}
				// Check for explicit skills:dangerous scope
				// 检查是否显式拥有 skills:dangerous scope。
				if slices.Contains(userCtx.Scopes, auth.ScopeSkillsDangerous) {
					authorized = true
				}
			}
			if !authorized {
				h.sendError(w, fmt.Sprintf("Skill %s is marked dangerous and requires admin/owner role or skills:dangerous scope", skillName), http.StatusForbidden)
				return false
			}
			h.logger.Warn("Dangerous skill invoked",
				zap.String("skill", skill.Name),
				zap.String("version", skill.Version),
				zap.String("user_id", userCtx.UserID.String()),
				zap.String("role", userCtx.Role),
			)
		}

		// Skill content becomes the system prompt override
		// skill 内容会作为 system prompt 的覆盖值。
		ctxMap["system_prompt"] = skill.Content

		// If skill declares a role, set it (bypasses decomposition)
		// 若 skill 声明了 role，则直接设置（跳过 task 分解）。
		if skill.RequiresRole != "" {
			ctxMap["role"] = skill.RequiresRole
		}

		// Echo skill metadata into context for observability
		// 把 skill metadata 回写进 context，便于观测。
		ctxMap["skill"] = skill.Name
		ctxMap["skill_version"] = skill.Version

		// Apply budget_max if specified and not already set
		// 若指定了 budget_max 且尚未设置，则应用它。
		if skill.BudgetMax > 0 {
			if _, exists := ctxMap["budget_max"]; !exists {
				ctxMap["budget_max"] = skill.BudgetMax
			}
		}

		h.logger.Info("Applied skill to task",
			zap.String("skill", skill.Name),
			zap.String("version", skill.Version),
			zap.String("role", skill.RequiresRole),
			zap.Bool("dangerous", skill.Dangerous),
		)
	}

	// Validate and inject model_tier from top-level (top-level wins)
	// 校验并写入顶层的 model_tier（顶层优先）。
	if mt := strings.TrimSpace(strings.ToLower(req.ModelTier)); mt != "" {
		switch mt {
		case "small", "medium", "large":
			ctxMap["model_tier"] = mt
			h.logger.Debug("Applied top-level model_tier override", zap.String("model_tier", mt))
		default:
			h.sendError(w, "Invalid model_tier (allowed: small, medium, large)", http.StatusBadRequest)
			return false
		}
	}

	// Inject top-level model_override when provided
	// 提供顶层 model_override 时写入 context。
	if mo := strings.TrimSpace(req.ModelOverride); mo != "" {
		ctxMap["model_override"] = mo
		h.logger.Debug("Applied top-level model_override", zap.String("model_override", mo))
	}

	// Inject top-level provider_override when provided
	// 提供顶层 provider_override 时写入 context。
	if po := strings.TrimSpace(strings.ToLower(req.ProviderOverride)); po != "" {
		// Validate provider exists
		// 校验该 provider 是否存在。
		validProviders := []string{"openai", "anthropic", "google", "groq", "xai", "deepseek", "qwen", "zai", "kimi", "minimax", "ollama", "mistral", "cohere"}
		isValid := slices.Contains(validProviders, po)
		if !isValid {
			h.sendError(w, fmt.Sprintf("Invalid provider_override: %s (allowed: %s)", po, strings.Join(validProviders, ", ")), http.StatusBadRequest)
			return false
		}
		ctxMap["provider_override"] = po
		h.logger.Debug("Applied top-level provider_override", zap.String("provider_override", po))
	}

	// Map research strategy controls into context
	// 把 research strategy 相关控制项映射到 context 中。
	if rs := strings.TrimSpace(strings.ToLower(req.ResearchStrategy)); rs != "" {
		switch rs {
		case "quick", "standard", "deep", "academic":
			ctxMap["research_strategy"] = rs
		default:
			h.sendError(w, "Invalid research_strategy (allowed: quick, standard, deep, academic)", http.StatusBadRequest)
			return false
		}
	}
	if req.MaxIterations != nil {
		if *req.MaxIterations <= 0 || *req.MaxIterations > 50 {
			h.sendError(w, "max_iterations out of range (1..50)", http.StatusBadRequest)
			return false
		}
		ctxMap["max_iterations"] = *req.MaxIterations
	}
	if req.MaxConcurrentAgents != nil {
		if *req.MaxConcurrentAgents <= 0 || *req.MaxConcurrentAgents > 20 {
			h.sendError(w, "max_concurrent_agents out of range (1..20)", http.StatusBadRequest)
			return false
		}
		ctxMap["max_concurrent_agents"] = *req.MaxConcurrentAgents
	}
	if req.EnableVerification != nil {
		ctxMap["enable_verification"] = *req.EnableVerification
	}

	// Apply research strategy presets (seed defaults only when absent)
	// Default to "standard" for force_research when no strategy specified
	// 应用 research strategy preset（仅补缺失的默认值）；force_research 未指定 strategy 时默认用 standard。
	rs, rsOk := ctxMap["research_strategy"].(string)
	if !rsOk || strings.TrimSpace(rs) == "" {
		if forceResearch, _ := ctxMap["force_research"].(bool); forceResearch {
			rs = "standard"
			ctxMap["research_strategy"] = rs
		}
	}
	if strings.TrimSpace(rs) != "" {
		applyStrategyPreset(ctxMap, rs)
	}

	// Conflict validation: disable_ai=true cannot be combined with model controls
	// 冲突校验：disable_ai=true 时不能同时指定 model 相关控制项。
	var disableAI bool
	if v, exists := ctxMap["disable_ai"]; exists {
		switch t := v.(type) {
		case bool:
			disableAI = t
		case string:
			s := strings.TrimSpace(strings.ToLower(t))
			disableAI = s == "true" || s == "1" || s == "yes" || s == "y"
		case float64:
			disableAI = t != 0
		case int:
			disableAI = t != 0
		}
	}
	if disableAI {
		// top-level conflicts
		// 顶层字段冲突
		if req.ModelTier != "" || req.ModelOverride != "" || req.ProviderOverride != "" {
			h.sendError(w, "disable_ai=true conflicts with model_tier/model_override", http.StatusBadRequest)
			return false
		}
		// context conflicts
		// context 字段冲突
		if vt, ok := ctxMap["model_tier"].(string); ok && strings.TrimSpace(vt) != "" {
			h.sendError(w, "disable_ai=true conflicts with model_tier/model_override", http.StatusBadRequest)
			return false
		}
		if vo, ok := ctxMap["model_override"].(string); ok && strings.TrimSpace(vo) != "" {
			h.sendError(w, "disable_ai=true conflicts with model_tier/model_override", http.StatusBadRequest)
			return false
		}
		if vp, ok := ctxMap["provider_override"].(string); ok && strings.TrimSpace(vp) != "" {
			h.sendError(w, "disable_ai=true conflicts with model_tier/model_override", http.StatusBadRequest)
			return false
		}
	}

	// Add context if present
	// 存在 context 时附加到 request 上。
	if len(ctxMap) > 0 {
		st, err := structpb.NewStruct(ctxMap)
		if err != nil {
			h.logger.Warn("Failed to convert context to struct", zap.Error(err))
		} else {
			grpcReq.Context = st
		}
	}

	// Propagate optional mode via labels for routing (e.g., supervisor)
	// 把可选的 mode 通过 labels 传给下游用于路由（如 supervisor）。
	if m := strings.TrimSpace(strings.ToLower(req.Mode)); m != "" {
		switch m {
		case "simple", "standard", "complex", "supervisor":
			if grpcReq.Metadata.Labels == nil {
				grpcReq.Metadata.Labels = map[string]string{}
			}
			grpcReq.Metadata.Labels["mode"] = m
		default:
			h.sendError(w, "Invalid mode (allowed: simple, standard, complex, supervisor)", http.StatusBadRequest)
			return false
		}
	}

	return true
}

// NewTaskHandler creates a new task handler
// NewTaskHandler 创建一个新的 task handler。
func NewTaskHandler(
	orchClient orchpb.OrchestratorServiceClient,
	db *sqlx.DB,
	redis *redis.Client,
	skillRegistry *skills.SkillRegistry,
	logger *zap.Logger,
	sessionMgr *session.Manager,
) *TaskHandler {
	return &TaskHandler{
		orchClient: orchClient,
		db:         db,
		redis:      redis,
		skills:     skillRegistry,
		logger:     logger,
		sessionMgr: sessionMgr,
	}
}

// TaskRequest represents a task submission request
// TaskRequest 表示一次 task 提交 request。
type TaskRequest struct {
	Query     string         `json:"query"`
	SessionID string         `json:"session_id,omitempty"`
	Context   map[string]any `json:"context,omitempty"`
	// Optional execution mode hint (e.g., "supervisor").
	// Routed via metadata labels to orchestrator.
	// 可选的执行 mode 提示（如 supervisor），通过 metadata labels 交给 orchestrator 路由。
	Mode string `json:"mode,omitempty"`
	// Optional skill name to apply (expands into system_prompt + role).
	// Skills are markdown-based task definitions loaded from config/skills/.
	// 可选的 skill 名称（会展开为 system_prompt + role）；skill 是 Markdown 形式的 task 定义，从 config/skills/ 加载。
	Skill string `json:"skill,omitempty"`
	// Optional model tier hint; if provided, inject into context
	// so downstream services can honor it (small|medium|large).
	// 可选的 model tier 提示；提供后写入 context，供下游服务采用（small|medium|large）。
	ModelTier string `json:"model_tier,omitempty"`
	// Optional specific model override; if provided, inject into context
	// (e.g., "gpt-5-2025-08-07", "gpt-5-pro-2025-10-06", "claude-sonnet-4-5-20250929").
	// 可选的具体模型覆盖；提供后写入 context（如 "gpt-5-2025-08-07"、"gpt-5-pro-2025-10-06"、"claude-sonnet-4-5-20250929"）。
	ModelOverride    string `json:"model_override,omitempty"`
	ProviderOverride string `json:"provider_override,omitempty"`
	// Phase 6: Strategy presets (mapped into context)
	// Phase 6：strategy preset（映射到 context 中）。
	ResearchStrategy    string `json:"research_strategy,omitempty"`     // quick|standard|deep|academic 注：取值 quick|standard|deep|academic
	MaxIterations       *int   `json:"max_iterations,omitempty"`        // Optional override 注：可选覆盖值
	MaxConcurrentAgents *int   `json:"max_concurrent_agents,omitempty"` // Optional override 注：可选覆盖值
	EnableVerification  *bool  `json:"enable_verification,omitempty"`   // Optional flag 注：可选的开关
}

// TaskResponse represents a task submission response
// TaskResponse 表示一次 task 提交的 response。
type TaskResponse struct {
	TaskID    string    `json:"task_id"`
	Status    string    `json:"status"`
	Message   string    `json:"message,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// TaskStatusResponse represents a task status response
// TaskStatusResponse 表示 task 状态查询的 response。
type TaskStatusResponse struct {
	TaskID     string         `json:"task_id"`
	WorkflowID string         `json:"workflow_id,omitempty"` // Same as task_id, for clarity 注：与 task_id 相同，仅为清晰起见
	Status     string         `json:"status"`
	Result     string         `json:"result,omitempty"`   // Raw result from LLM (plain text or JSON) 注：LLM 返回的原始结果（纯文本或 JSON）
	Response   map[string]any `json:"response,omitempty"` // Parsed JSON (backward compatibility) 注：解析后的 JSON（向后兼容）
	Error      string         `json:"error,omitempty"`
	CreatedAt  time.Time      `json:"created_at"`
	UpdatedAt  time.Time      `json:"updated_at"`
	// Extra metadata to enable "reply" UX
	// 用于支撑 "reply" 交互的额外 metadata。
	Query     string         `json:"query,omitempty"`
	SessionID string         `json:"session_id,omitempty"`
	Mode      string         `json:"mode,omitempty"`
	Context   map[string]any `json:"context,omitempty"` // Task context (force_research, research_strategy, etc.) 注：task context（force_research、research_strategy 等）
	// Usage metadata
	// 用量 metadata。
	ModelUsed string         `json:"model_used,omitempty"`
	Provider  string         `json:"provider,omitempty"`
	Usage     map[string]any `json:"usage,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`         // Task metadata (citations, etc.) 注：task metadata（citations 等）
	Unified   map[string]any `json:"unified_response,omitempty"` // Structured unified response 注：结构化的 unified response
}

// ListTasksResponse represents the list tasks response
// ListTasksResponse 表示 task 列表查询的 response。
type ListTasksResponse struct {
	Tasks      []TaskSummary `json:"tasks"`
	TotalCount int32         `json:"total_count"`
}

// TaskSummary represents a single task in listing
// TaskSummary 表示列表中的单条 task。
type TaskSummary struct {
	TaskID          string         `json:"task_id"`
	Query           string         `json:"query,omitempty"`
	Status          string         `json:"status"`
	Mode            string         `json:"mode,omitempty"`
	CreatedAt       *time.Time     `json:"created_at,omitempty"`
	CompletedAt     *time.Time     `json:"completed_at,omitempty"`
	TotalTokenUsage map[string]any `json:"total_token_usage,omitempty"`
}

// SubmitTask handles POST /api/v1/tasks
// SubmitTask 处理 POST /api/v1/tasks。
func (h *TaskHandler) SubmitTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth middleware 取 user context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Limit request body size to accommodate multimodal payloads.
	// 限制 request 体大小，同时为多模态 payload 留出空间。
	r.Body = http.MaxBytesReader(w, r.Body, attachments.MaxMultimodalBodyBytes)

	// Parse request body
	// 解析 request body。
	var req TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request — allow empty query when attachments are present
	// 校验 request——存在附件时允许 query 为空。
	hasAttachments := false
	if req.Context != nil {
		if atts, ok := req.Context["attachments"]; ok {
			if attList, ok := atts.([]any); ok && len(attList) > 0 {
				hasAttachments = true
			}
		}
	}
	if req.Query == "" && !hasAttachments {
		h.sendError(w, "Query is required", http.StatusBadRequest)
		return
	}
	if req.Query == "" && hasAttachments {
		req.Query = "[User sent file attachments]"
	}

	// Generate session ID if not provided
	// 未提供 session ID 时生成一个。
	if req.SessionID == "" {
		req.SessionID = uuid.New().String()
	}

	// Extract and store inline base64 attachments from context.attachments.
	// Replace raw data with lightweight Redis references before forwarding.
	// 从 context.attachments 中取出内联 base64 附件并存储，转发前把原始数据替换为轻量的 Redis 引用。
	if err := h.extractAndStoreContextAttachments(ctx, req.SessionID, req.Context); err != nil {
		h.sendError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// NOTE: Workspace quota check deferred to Phase 2.
	// Current count-based approach is too coarse - blocks all tasks, not just workspace-using ones.
	// 注意：workspace 配额检查推迟到 Phase 2；当前基于数量的做法过于粗糙，会拦住所有 task 而不只是使用 workspace 的 task。

	// Build gRPC request
	// 构造 gRPC request。
	grpcReq := &orchpb.SubmitTaskRequest{
		Metadata: &commonpb.TaskMetadata{
			UserId:    userCtx.UserID.String(),
			TenantId:  userCtx.TenantID.String(),
			SessionId: req.SessionID,
			Labels:    map[string]string{},
		},
		Query: req.Query,
	}

	// Apply context, model controls, and mode labels
	// 应用 context、model 控制项与 mode labels。
	if !h.applyTaskContextAndLabels(&req, grpcReq, w, r) {
		return
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Submit task to orchestrator
	// 把 task 提交给 orchestrator。
	resp, err := h.orchClient.SubmitTask(ctx, grpcReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				h.sendError(w, st.Message(), http.StatusBadRequest)
			case codes.ResourceExhausted:
				h.sendError(w, "Rate limit exceeded", http.StatusTooManyRequests)
			default:
				h.sendError(w, fmt.Sprintf("Failed to submit task: %v", st.Message()), http.StatusInternalServerError)
			}
		} else {
			h.sendError(w, fmt.Sprintf("Failed to submit task: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Log task submission
	// 记录 task 提交日志。
	h.logger.Info("Task submitted",
		zap.String("task_id", resp.TaskId),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("session_id", req.SessionID),
	)

	// Prepare response
	// 准备 response。
	taskResp := TaskResponse{
		TaskID:    resp.TaskId,
		Status:    resp.Status.String(),
		Message:   resp.Message,
		CreatedAt: time.Now(),
	}

	// Add workflow ID header for tracing
	// 添加 workflow ID header，便于链路追踪。
	w.Header().Set("X-Workflow-ID", resp.WorkflowId)
	w.Header().Set("X-Session-ID", req.SessionID)

	// Send response
	// 发送 response。
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(taskResp)
}

// SubmitTaskAndGetStreamURL handles POST /api/v1/tasks/stream
// Submits a task and returns a stream URL for SSE consumption.
// SubmitTaskAndGetStreamURL 处理 POST /api/v1/tasks/stream：提交 task 并返回供 SSE 消费的 stream URL。
func (h *TaskHandler) SubmitTaskAndGetStreamURL(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth middleware 取 user context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Limit request body size to accommodate multimodal payloads.
	// 限制 request body 大小，以容纳多模态 payload。
	r.Body = http.MaxBytesReader(w, r.Body, attachments.MaxMultimodalBodyBytes)

	// Parse request body
	// 解析 request body。
	var req TaskRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate request — allow empty query when attachments are present
	// 校验 request——存在附件时允许 query 为空。
	hasAtts := false
	if req.Context != nil {
		if atts, ok := req.Context["attachments"]; ok {
			if attList, ok := atts.([]any); ok && len(attList) > 0 {
				hasAtts = true
			}
		}
	}
	if req.Query == "" && !hasAtts {
		h.sendError(w, "Query is required", http.StatusBadRequest)
		return
	}
	if req.Query == "" && hasAtts {
		req.Query = "[User sent file attachments]"
	}

	// Generate session ID if not provided
	// 未提供 session ID 时生成一个。
	if req.SessionID == "" {
		req.SessionID = uuid.New().String()
	}

	// Extract and store inline base64 attachments from context.attachments.
	// 从 context.attachments 中取出内联 base64 附件并存储。
	if err := h.extractAndStoreContextAttachments(ctx, req.SessionID, req.Context); err != nil {
		h.sendError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// NOTE: Workspace quota check deferred to Phase 2.
	// 注意：workspace 配额检查推迟到 Phase 2。

	// Build gRPC request (reuse same shape as SubmitTask)
	// 构造 gRPC request（沿用与 SubmitTask 相同的结构）。
	grpcReq := &orchpb.SubmitTaskRequest{
		Metadata: &commonpb.TaskMetadata{
			UserId:    userCtx.UserID.String(),
			TenantId:  userCtx.TenantID.String(),
			SessionId: req.SessionID,
			Labels:    map[string]string{},
		},
		Query: req.Query,
	}

	// Apply context, model controls, and mode labels
	// 应用 context、model 控制项与 mode labels。
	if !h.applyTaskContextAndLabels(&req, grpcReq, w, r) {
		return
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Submit task to orchestrator
	// 把 task 提交给 orchestrator。
	resp, err := h.orchClient.SubmitTask(ctx, grpcReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				h.sendError(w, st.Message(), http.StatusBadRequest)
			case codes.ResourceExhausted:
				h.sendError(w, "Rate limit exceeded", http.StatusTooManyRequests)
			default:
				h.sendError(w, fmt.Sprintf("Failed to submit task: %v", st.Message()), http.StatusInternalServerError)
			}
		} else {
			h.sendError(w, fmt.Sprintf("Failed to submit task: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Log task submission
	// 记录 task 提交日志。
	h.logger.Info("Task submitted with stream URL",
		zap.String("task_id", resp.TaskId),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("session_id", req.SessionID),
	)

	// Prepare stream URL (clients will use EventSource on this URL)
	// 准备 stream URL（客户端会用 EventSource 连接该地址）。
	streamURL := fmt.Sprintf("/api/v1/stream/sse?workflow_id=%s", resp.WorkflowId)

	// Headers for discoverability
	// 便于发现该接口的 header。
	w.Header().Set("X-Workflow-ID", resp.WorkflowId)
	w.Header().Set("X-Session-ID", req.SessionID)
	w.Header().Set("Link", fmt.Sprintf("<%s>; rel=stream", streamURL))
	w.Header().Set("Content-Type", "application/json")

	// Body with stream URL
	// 包含 stream URL 的 body。
	w.WriteHeader(http.StatusCreated)
	_ = json.NewEncoder(w).Encode(map[string]string{
		"workflow_id": resp.WorkflowId,
		"task_id":     resp.TaskId,
		"stream_url":  streamURL,
	})
}

// GetTaskStatus handles GET /api/v1/tasks/{id}
// GetTaskStatus 处理 GET /api/v1/tasks/{id}。
func (h *TaskHandler) GetTaskStatus(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 取 user context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract task ID from path
	// 从 path 中取出 task ID。
	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Get task status from orchestrator
	// 从 orchestrator 查询 task 状态。
	grpcReq := &orchpb.GetTaskStatusRequest{
		TaskId: taskID,
	}

	resp, err := h.orchClient.GetTaskStatus(ctx, grpcReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			if st.Code() == codes.NotFound {
				h.sendError(w, "Task not found", http.StatusNotFound)
			} else {
				h.sendError(w, fmt.Sprintf("Failed to get task status: %v", st.Message()), http.StatusInternalServerError)
			}
		} else {
			h.sendError(w, fmt.Sprintf("Failed to get task status: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Prepare response with raw result
	// 准备带原始结果的 response。
	statusResp := TaskStatusResponse{
		TaskID:     resp.TaskId,
		WorkflowID: resp.TaskId, // Same as task_id 注：与 task_id 相同
		Status:     resp.Status.String(),
		Result:     resp.Result, // Always include raw result (plain text or JSON string) 注：始终包含原始结果（纯文本或 JSON 字符串）
		Error:      resp.ErrorMessage,
	}

	// Optionally parse as JSON for backward compatibility (response field)
	// If result is valid JSON, populate response field; otherwise leave it nil
	// 为向后兼容，尝试把 result 解析为 JSON：成功则填充 response 字段，否则保持 nil。
	if resp.Result != "" {
		var responseData map[string]any
		if err := json.Unmarshal([]byte(resp.Result), &responseData); err == nil {
			statusResp.Response = responseData
		}
		// If unmarshal fails, it's plain text - only result field will be populated
		// 反序列化失败说明是纯文本，此时只会填充 result 字段。
	}

	// Enrich with metadata from database (query, session_id, mode, model, provider, tokens, cost, metadata, response)
	// 用数据库中的 metadata 补全 response（query、session_id、mode、model、provider、tokens、cost、metadata、response）。
	var (
		q                sql.NullString
		sid              sql.NullString
		mode             sql.NullString
		modelUsed        sql.NullString
		provider         sql.NullString
		totalTokens      sql.NullInt32
		promptTokens     sql.NullInt32
		completionTokens sql.NullInt32
		totalCost        sql.NullFloat64
		responseJSON     []byte
		metadataJSON     []byte
	)
	row := h.db.QueryRowxContext(ctx, `
		SELECT
			query,
			COALESCE(session_id,''),
			COALESCE(mode,''),
			COALESCE(model_used,''),
			COALESCE(provider,''),
			total_tokens,
			prompt_tokens,
			completion_tokens,
			total_cost_usd,
			COALESCE(response::text, '{}'),
			metadata
		FROM task_executions
		WHERE workflow_id = $1
		LIMIT 1`, taskID)
	if err := row.Scan(&q, &sid, &mode, &modelUsed, &provider, &totalTokens, &promptTokens, &completionTokens, &totalCost, &responseJSON, &metadataJSON); err != nil {
		h.logger.Warn("Failed to scan task metadata", zap.Error(err), zap.String("workflow_id", taskID))
	}
	statusResp.Query = q.String
	statusResp.SessionID = sid.String
	statusResp.Mode = mode.String

	// Populate model and provider if available
	// 若存在则填充 model 与 provider。
	if modelUsed.Valid && modelUsed.String != "" {
		statusResp.ModelUsed = modelUsed.String
	}
	if provider.Valid && provider.String != "" {
		statusResp.Provider = provider.String
	}

	// Populate usage metadata if available
	// 若存在则填充用量 metadata。
	if totalTokens.Valid || totalCost.Valid {
		statusResp.Usage = map[string]any{}
		if totalTokens.Valid && totalTokens.Int32 > 0 {
			statusResp.Usage["total_tokens"] = totalTokens.Int32
		}
		if promptTokens.Valid && promptTokens.Int32 > 0 {
			statusResp.Usage["input_tokens"] = promptTokens.Int32
		}
		if completionTokens.Valid && completionTokens.Int32 > 0 {
			statusResp.Usage["output_tokens"] = completionTokens.Int32
		}
		if totalCost.Valid && totalCost.Float64 > 0 {
			statusResp.Usage["estimated_cost"] = totalCost.Float64
		}
	}

	// Parse and populate unified response if available
	// 若存在则解析并填充 unified response。
	if len(responseJSON) > 0 {
		var unified map[string]any
		if err := json.Unmarshal(responseJSON, &unified); err == nil && len(unified) > 0 {
			statusResp.Unified = unified
		}
	}

	// Parse and populate metadata (citations, etc.) if available
	// 若存在则解析并填充 metadata（citations 等）。
	if len(metadataJSON) > 0 {
		var metadata map[string]any
		if err := json.Unmarshal(metadataJSON, &metadata); err == nil {
			statusResp.Metadata = metadata

			// Extract context from metadata if available (stored as metadata.task_context)
			// 若存在则从 metadata 中取出 context（存放在 metadata.task_context）。
			if taskContext, ok := metadata["task_context"].(map[string]any); ok {
				statusResp.Context = taskContext
			}
		}
	}

	// Add model_breakdown from token_usage table for transparency
	// 从 token_usage 表补充 model_breakdown，便于看清用量构成。
	if statusResp.Metadata == nil {
		statusResp.Metadata = make(map[string]any)
	}
	if breakdown := h.buildModelBreakdown(ctx, taskID); breakdown != nil {
		statusResp.Metadata["model_breakdown"] = breakdown
	}

	// Set timestamps to current time since they're not in the proto
	// proto 中没有时间戳字段，这里设为当前时间。
	statusResp.CreatedAt = time.Now()
	statusResp.UpdatedAt = time.Now()

	h.logger.Debug("Task status retrieved",
		zap.String("task_id", taskID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("status", resp.Status.String()),
	)

	// Send response
	// 发送 response。
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Workflow-ID", taskID)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(statusResp)
}

// ListTasks handles GET /api/v1/tasks
// ListTasks 处理 GET /api/v1/tasks。
func (h *TaskHandler) ListTasks(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Parse query params
	// 解析 query 参数。
	q := r.URL.Query()
	limit := parseIntDefault(q.Get("limit"), 20)
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset := max(parseIntDefault(q.Get("offset"), 0), 0)
	sessionID := q.Get("session_id")
	statusStr := q.Get("status")

	// Map status to proto
	// 把 status 映射为 proto 取值。
	var statusFilter orchpb.TaskStatus
	switch strings.ToUpper(statusStr) {
	case "QUEUED":
		statusFilter = orchpb.TaskStatus_TASK_STATUS_QUEUED
	case "RUNNING":
		statusFilter = orchpb.TaskStatus_TASK_STATUS_RUNNING
	case "COMPLETED":
		statusFilter = orchpb.TaskStatus_TASK_STATUS_COMPLETED
	case "FAILED":
		statusFilter = orchpb.TaskStatus_TASK_STATUS_FAILED
	case "CANCELLED", "CANCELED":
		statusFilter = orchpb.TaskStatus_TASK_STATUS_CANCELLED
	case "TIMEOUT":
		statusFilter = orchpb.TaskStatus_TASK_STATUS_TIMEOUT
	default:
		statusFilter = orchpb.TaskStatus_TASK_STATUS_UNSPECIFIED
	}

	req := &orchpb.ListTasksRequest{
		UserId:       userCtx.UserID.String(),
		SessionId:    sessionID,
		Limit:        int32(limit),
		Offset:       int32(offset),
		FilterStatus: statusFilter,
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	resp, err := h.orchClient.ListTasks(ctx, req)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			h.sendError(w, st.Message(), http.StatusInternalServerError)
		} else {
			h.sendError(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// Map to HTTP response shape
	// 映射为 HTTP response 结构。
	out := ListTasksResponse{Tasks: make([]TaskSummary, 0, len(resp.Tasks)), TotalCount: resp.TotalCount}
	for _, t := range resp.Tasks {
		var createdAt, completedAt *time.Time
		if t.CreatedAt != nil {
			ct := t.CreatedAt.AsTime()
			createdAt = &ct
		}
		if t.CompletedAt != nil {
			cp := t.CompletedAt.AsTime()
			completedAt = &cp
		}
		var usage map[string]any
		if t.TotalTokenUsage != nil {
			usage = map[string]any{
				"total_tokens":      t.TotalTokenUsage.TotalTokens,
				"cost_usd":          t.TotalTokenUsage.CostUsd,
				"prompt_tokens":     t.TotalTokenUsage.PromptTokens,
				"completion_tokens": t.TotalTokenUsage.CompletionTokens,
			}
		}
		out.Tasks = append(out.Tasks, TaskSummary{
			TaskID:          t.TaskId,
			Query:           t.Query,
			Status:          t.Status.String(),
			Mode:            t.Mode.String(),
			CreatedAt:       createdAt,
			CompletedAt:     completedAt,
			TotalTokenUsage: usage,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(out)
}

// GetTaskEvents handles GET /api/v1/tasks/{id}/events
// GetTaskEvents 处理 GET /api/v1/tasks/{id}/events。
func (h *TaskHandler) GetTaskEvents(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if _, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext); !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	q := r.URL.Query()
	limit := parseIntDefault(q.Get("limit"), 50)
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := max(parseIntDefault(q.Get("offset"), 0), 0)

	rows, err := h.db.QueryxContext(ctx, `
        SELECT workflow_id, type, COALESCE(agent_id,''), COALESCE(message,''), timestamp, COALESCE(seq,0), COALESCE(stream_id,''), COALESCE(payload, '{}')
        FROM event_logs
        WHERE workflow_id = $1
        ORDER BY timestamp ASC
        LIMIT $2 OFFSET $3
    `, taskID, limit, offset)
	if err != nil {
		h.sendError(w, fmt.Sprintf("Failed to load events: %v", err), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	type Event struct {
		WorkflowID string          `json:"workflow_id"`
		Type       string          `json:"type"`
		AgentID    string          `json:"agent_id,omitempty"`
		Message    string          `json:"message,omitempty"`
		Timestamp  time.Time       `json:"timestamp"`
		Seq        uint64          `json:"seq"`
		StreamID   string          `json:"stream_id,omitempty"`
		Payload    json.RawMessage `json:"payload,omitempty"`
	}
	events := []Event{}
	for rows.Next() {
		var e Event
		var payloadBytes []byte
		if err := rows.Scan(&e.WorkflowID, &e.Type, &e.AgentID, &e.Message, &e.Timestamp, &e.Seq, &e.StreamID, &payloadBytes); err != nil {
			h.sendError(w, fmt.Sprintf("Failed to scan event: %v", err), http.StatusInternalServerError)
			return
		}
		if len(payloadBytes) > 0 && string(payloadBytes) != "{}" {
			e.Payload = payloadBytes
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		h.sendError(w, fmt.Sprintf("Failed to read events: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"events": events, "count": len(events)})
}

func parseIntDefault(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

// StreamTask handles GET /api/v1/tasks/{id}/stream
// StreamTask 处理 GET /api/v1/tasks/{id}/stream。
func (h *TaskHandler) StreamTask(w http.ResponseWriter, r *http.Request) {
	// Extract task ID from path
	// 从 path 中取出 task ID。
	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Rewrite the request to proxy to admin server
	// This will be handled by the streaming proxy
	// For now, we'll redirect to the SSE endpoint with workflow_id
	// 改为代理到 admin server 的 request，由 streaming proxy 处理；目前直接重定向到带 workflow_id 的 SSE 端点。
	redirectURL := fmt.Sprintf("/api/v1/stream/sse?workflow_id=%s", taskID)

	// Copy any additional query parameters
	// 复制其余 query 参数。
	if types := r.URL.Query().Get("types"); types != "" {
		redirectURL += "&types=" + types
	}
	if lastEventID := r.URL.Query().Get("last_event_id"); lastEventID != "" {
		redirectURL += "&last_event_id=" + lastEventID
	}

	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// CancelTask handles POST /api/v1/tasks/{id}/cancel
// CancelTask 处理 POST /api/v1/tasks/{id}/cancel。
func (h *TaskHandler) CancelTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 取 user context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract task ID from path
	// 从 path 中取出 task ID。
	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Parse optional request body for reason
	// 解析可选的 request body，取出 reason。
	type cancelRequest struct {
		Reason string `json:"reason,omitempty"`
	}
	var req cancelRequest
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Call CancelTask gRPC
	// Service layer enforces ownership and handles all auth/authorization
	// 调用 CancelTask gRPC；归属校验与 authentication/authorization 全部由 service 层负责。
	cancelReq := &orchpb.CancelTaskRequest{
		TaskId: taskID,
		Reason: req.Reason,
	}
	cancelResp, err := h.orchClient.CancelTask(ctx, cancelReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unauthenticated:
				h.sendError(w, "Unauthorized", http.StatusUnauthorized)
			case codes.PermissionDenied:
				h.sendError(w, "Forbidden", http.StatusForbidden)
			case codes.NotFound:
				h.sendError(w, "Task not found", http.StatusNotFound)
			default:
				h.sendError(w, fmt.Sprintf("Failed to cancel task: %v", st.Message()), http.StatusInternalServerError)
			}
		} else {
			h.sendError(w, fmt.Sprintf("Failed to cancel task: %v", err), http.StatusInternalServerError)
		}
		return
	}

	// Log cancellation
	// 记录取消日志。
	h.logger.Info("Task cancelled",
		zap.String("task_id", taskID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("reason", req.Reason),
	)

	// Return 202 Accepted
	// 返回 202 Accepted。
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"success": cancelResp.Success,
		"message": cancelResp.Message,
	})
}

// PauseTask handles POST /api/v1/tasks/{id}/pause
// PauseTask 处理 POST /api/v1/tasks/{id}/pause。
func (h *TaskHandler) PauseTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 取 user context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract task ID from path
	// 从 path 中取出 task ID。
	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Parse optional request body for reason
	// 解析可选的 request body，取出 reason。
	var req struct {
		Reason string `json:"reason,omitempty"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Call PauseTask gRPC (service layer enforces ownership)
	// 调用 PauseTask gRPC（归属校验由 service 层负责）。
	pauseReq := &orchpb.PauseTaskRequest{
		TaskId: taskID,
		Reason: req.Reason,
	}
	pauseResp, err := h.orchClient.PauseTask(ctx, pauseReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unauthenticated:
				h.sendError(w, "Unauthorized", http.StatusUnauthorized)
			case codes.PermissionDenied:
				h.sendError(w, "Forbidden", http.StatusForbidden)
			case codes.NotFound:
				h.sendError(w, "Task not found", http.StatusNotFound)
			default:
				h.sendError(w, st.Message(), http.StatusBadGateway)
			}
			return
		}
		h.sendError(w, "Failed to pause task", http.StatusInternalServerError)
		return
	}

	// Log for audit trail
	// 记录审计日志。
	h.logger.Info("Task pause requested",
		zap.String("task_id", taskID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("reason", req.Reason),
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"success": pauseResp.Success,
		"message": pauseResp.Message,
		"task_id": taskID,
	})
}

// ResumeTask handles POST /api/v1/tasks/{id}/resume
// ResumeTask 处理 POST /api/v1/tasks/{id}/resume。
func (h *TaskHandler) ResumeTask(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context
	// 取 user context。
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract task ID from path
	// 从 path 中取出 task ID。
	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Parse optional request body for reason
	// 解析可选的 request body，取出 reason。
	var req struct {
		Reason string `json:"reason,omitempty"`
	}
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&req)
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Call ResumeTask gRPC (service layer enforces ownership)
	// 调用 ResumeTask gRPC（归属校验由 service 层负责）。
	resumeReq := &orchpb.ResumeTaskRequest{
		TaskId: taskID,
		Reason: req.Reason,
	}
	resumeResp, err := h.orchClient.ResumeTask(ctx, resumeReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unauthenticated:
				h.sendError(w, "Unauthorized", http.StatusUnauthorized)
			case codes.PermissionDenied:
				h.sendError(w, "Forbidden", http.StatusForbidden)
			case codes.NotFound:
				h.sendError(w, "Task not found", http.StatusNotFound)
			default:
				h.sendError(w, st.Message(), http.StatusBadGateway)
			}
			return
		}
		h.sendError(w, "Failed to resume task", http.StatusInternalServerError)
		return
	}

	// Log for audit trail
	// 记录审计日志。
	h.logger.Info("Task resume requested",
		zap.String("task_id", taskID),
		zap.String("user_id", userCtx.UserID.String()),
		zap.String("reason", req.Reason),
	)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]any{
		"success": resumeResp.Success,
		"message": resumeResp.Message,
		"task_id": taskID,
	})
}

// GetControlState handles GET /api/v1/tasks/{id}/control-state
// GetControlState 处理 GET /api/v1/tasks/{id}/control-state。
func (h *TaskHandler) GetControlState(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context (auth required)
	// 取 user context（需要认证）。
	_, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract task ID from path
	// 从 path 中取出 task ID。
	taskID := r.PathValue("id")
	if taskID == "" {
		h.sendError(w, "Task ID is required", http.StatusBadRequest)
		return
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 透传到 gRPC metadata。
	ctx = withGRPCMetadata(ctx, r)

	// Call GetControlState gRPC (service layer enforces ownership)
	// 调用 GetControlState gRPC（归属校验由 service 层负责）。
	stateReq := &orchpb.GetControlStateRequest{TaskId: taskID}
	stateResp, err := h.orchClient.GetControlState(ctx, stateReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.Unauthenticated:
				h.sendError(w, "Unauthorized", http.StatusUnauthorized)
			case codes.PermissionDenied:
				h.sendError(w, "Forbidden", http.StatusForbidden)
			case codes.NotFound:
				h.sendError(w, "Task not found", http.StatusNotFound)
			default:
				h.sendError(w, st.Message(), http.StatusBadGateway)
			}
			return
		}
		h.sendError(w, "Failed to get control state", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"is_paused":     stateResp.IsPaused,
		"is_cancelled":  stateResp.IsCancelled,
		"paused_at":     stateResp.PausedAt,
		"pause_reason":  stateResp.PauseReason,
		"paused_by":     stateResp.PausedBy,
		"cancel_reason": stateResp.CancelReason,
		"cancelled_by":  stateResp.CancelledBy,
	})
}

// buildModelBreakdown queries token_usage table to build detailed model breakdown
// buildModelBreakdown 查询 token_usage 表，构建按 model 拆分的用量明细。
func (h *TaskHandler) buildModelBreakdown(ctx context.Context, workflowID string) []map[string]any {
	// Query token_usage table grouped by model
	// 按 model 分组查询 token_usage 表。
	query := `
		SELECT
			provider,
			model,
			COUNT(*) as executions,
			SUM(total_tokens) as total_tokens,
			SUM(cost_usd) as total_cost,
			COALESCE(SUM(cache_read_tokens), 0) as cache_read_tokens,
			COALESCE(SUM(cache_creation_tokens), 0) as cache_creation_tokens
		FROM token_usage
		WHERE task_id = (
			SELECT id FROM task_executions WHERE workflow_id = $1
		)
		GROUP BY provider, model
		ORDER BY total_cost DESC
	`

	rows, err := h.db.QueryxContext(ctx, query, workflowID)
	if err != nil {
		h.logger.Debug("Failed to query model breakdown", zap.Error(err), zap.String("workflow_id", workflowID))
		return nil
	}
	defer rows.Close()

	var breakdown []map[string]any
	var totalCost float64
	var totalTokens int64

	// First pass: collect data and calculate totals
	// 第一轮：收集数据并计算总量。
	type modelData struct {
		Provider            string
		Model               string
		Executions          int
		Tokens              int64
		Cost                float64
		CacheReadTokens     int64
		CacheCreationTokens int64
	}
	var models []modelData

	for rows.Next() {
		var m modelData
		if err := rows.Scan(&m.Provider, &m.Model, &m.Executions, &m.Tokens, &m.Cost,
			&m.CacheReadTokens, &m.CacheCreationTokens); err != nil {
			h.logger.Debug("Failed to scan model breakdown row", zap.Error(err))
			continue
		}
		models = append(models, m)
		totalCost += m.Cost
		totalTokens += m.Tokens
	}

	// Second pass: build breakdown with percentages
	// 第二轮：计算占比，构建明细。
	for _, m := range models {
		percentage := 0
		if totalCost > 0 {
			percentage = int((m.Cost / totalCost) * 100)
		}

		entry := map[string]any{
			"model":      m.Model,
			"provider":   m.Provider,
			"executions": m.Executions,
			"tokens":     m.Tokens,
			"cost_usd":   m.Cost,
			"percentage": percentage,
		}
		if m.CacheReadTokens > 0 {
			entry["cache_read_tokens"] = m.CacheReadTokens
		}
		if m.CacheCreationTokens > 0 {
			entry["cache_creation_tokens"] = m.CacheCreationTokens
		}
		breakdown = append(breakdown, entry)
	}

	return breakdown
}

// sendError sends an error response
// sendError 发送一个错误 response。
func (h *TaskHandler) sendError(w http.ResponseWriter, message string, code int) {
	writeJSON(w, code, map[string]string{"error": message})
}

// extractAndStoreContextAttachments inspects ctxMap["attachments"] for inline
// base64 data, stores each blob in Redis via the attachment store, and replaces
// the entry with a lightweight reference (id + media_type + filename + size_bytes).
// Attachments without "data" (already references or URL-based) are passed through.
// extractAndStoreContextAttachments 检查 ctxMap["attachments"] 中的内联 base64 数据，把每个 blob 存入 Redis，
// 并替换为轻量引用（id + media_type + filename + size_bytes）；没有 "data" 字段的条目（已是引用或 URL）原样透传。
func (h *TaskHandler) extractAndStoreContextAttachments(ctx context.Context, sessionID string, ctxMap map[string]any) error {
	if ctxMap == nil {
		return nil
	}
	raw, ok := ctxMap["attachments"]
	if !ok {
		return nil
	}
	attList, ok := raw.([]any)
	if !ok {
		return fmt.Errorf("context.attachments must be an array")
	}
	if len(attList) == 0 {
		return nil
	}

	attStore := attachments.NewStore(h.redis, 30*time.Minute)
	var refs []any
	var totalDecodedBytes int

	for _, a := range attList {
		am, ok := a.(map[string]any)
		if !ok {
			return fmt.Errorf("each attachment must be an object")
		}

		// If the entry has a "data" field, it contains base64-encoded content
		// that should be offloaded to Redis.
		// 若条目含 "data" 字段，说明内容是 base64 编码，需要转存到 Redis。
		dataStr, hasData := am["data"].(string)
		if !hasData {
			// URL-based or pre-existing ref — require MIME and reject dangerous
			// types only. Per attachment-format-parity §2 P0, unknown / archive /
			// audio-video / octet-stream MIMEs are NOT rejected here: the daemon
			// fetches via URL and uses bash / archive_extract on the other end.
			// Cloud-side extraction (Phase 3) is opportunistic, not a gate.
			// URL 形式或已有引用：只要求 MIME 存在并拒绝危险类型。按 attachment-format-parity §2 P0，
			// 未知/归档/音视频/octet-stream 等 MIME 不在此处拒绝——daemon 会按 URL 拉取，并在另一端用 bash / archive_extract 处理；
			// 云端提取（Phase 3）是机会性的，不作为门禁。
			mt, _ := am["media_type"].(string)
			if mt == "" {
				return fmt.Errorf("attachment media_type is required for URL-based attachments")
			}
			if attachments.IsDangerous(mt) {
				return fmt.Errorf("rejected attachment type: %s (executables / installers are not accepted)", mt)
			}
			ref := make(map[string]any, len(am))
			maps.Copy(ref, am)
			if thumb, ok := sanitizeAttachmentThumbnail(am["thumbnail"]); ok {
				ref["thumbnail"] = thumb
			} else {
				delete(ref, "thumbnail")
			}
			refs = append(refs, ref)
			continue
		}

		decoded, err := base64.StdEncoding.DecodeString(dataStr)
		if err != nil {
			// Try raw (no-padding) variant.
			// 尝试无 padding 的原始变体。
			decoded, err = base64.RawStdEncoding.DecodeString(dataStr)
			if err != nil {
				return fmt.Errorf("attachment base64 decode failed: %w", err)
			}
		}

		totalDecodedBytes += len(decoded)
		if totalDecodedBytes > attachments.MaxDecodedAttachmentBytes {
			return fmt.Errorf("total attachment size %d bytes exceeds %d byte limit", totalDecodedBytes, attachments.MaxDecodedAttachmentBytes)
		}

		mediaType, _ := am["media_type"].(string)
		filename, _ := am["filename"].(string)
		// Sanitize filename to prevent path traversal (e.g. "../../etc/passwd" → "passwd")
		// 清洗文件名以防路径穿越（如 "../../etc/passwd" → "passwd"）。
		if filename != "" {
			filename = filepath.Base(filename)
		}

		// Require media_type for base64 attachments (prevents silent degradation downstream)
		// base64 附件必须带 media_type，避免下游静默降级。
		if mediaType == "" {
			return fmt.Errorf("attachment media_type is required for base64 attachments")
		}
		// Base64 inline path = cloud actually holds the bytes for an inline LLM
		// content block. Only IsExtractable types make sense here (cloud will
		// forward to Anthropic or run extraction). Passthrough types belong to
		// URL-only refs (see the !hasData branch above). Dangerous types are
		// always rejected.
		// 内联 base64 路径意味着云端真正持有字节，会生成内联 LLM content block，因此只接受 IsExtractable 类型
		// （云端转发给 Anthropic 或执行提取）；passthrough 类型只用于 URL 引用（见上面的 !hasData 分支）；危险类型一律拒绝。
		if attachments.IsDangerous(mediaType) {
			return fmt.Errorf("rejected attachment type: %s (executables / installers are not accepted)", mediaType)
		}
		if !attachments.IsExtractable(mediaType) {
			return fmt.Errorf("unsupported attachment type for inline upload: %s (use URL-based file_ref instead)", mediaType)
		}

		id, err := attStore.Put(ctx, sessionID, decoded, mediaType, filename)
		if err != nil {
			return fmt.Errorf("failed to store attachment: %w", err)
		}

		ref := map[string]any{
			"id":         id,
			"media_type": mediaType,
			"filename":   filename,
			"size_bytes": len(decoded),
		}
		if thumb, ok := sanitizeAttachmentThumbnail(am["thumbnail"]); ok {
			ref["thumbnail"] = thumb
		}
		refs = append(refs, ref)
	}

	ctxMap["attachments"] = refs
	return nil
}

func sanitizeAttachmentThumbnail(raw any) (string, bool) {
	thumb, ok := raw.(string)
	if !ok {
		return "", false
	}
	if thumb == "" || len(thumb) > attachments.MaxAttachmentThumbnailBytes {
		return "", false
	}
	return thumb, true
}

// checkWorkspaceQuota checks if the tenant has exceeded their workspace quota.
// Uses count-based approach: count active sessions for tenant, compare to workspace_max_size_gb.
// This is a simple proxy since each workspace uses approximately 1GB of storage.
// checkWorkspaceQuota 检查 tenant 是否超出 workspace 配额：按活跃 session 数估算
// （假设每个 workspace 约占 1GB），与 workspace_max_size_gb 比较。
func (h *TaskHandler) checkWorkspaceQuota(ctx context.Context, tenantID uuid.UUID) error {
	// Skip quota check if db is not configured (e.g., in tests)
	// 未配置 db 时跳过配额检查（如测试环境）。
	if h.db == nil {
		return nil
	}

	// Get tenant's workspace quota
	// 获取该 tenant 的 workspace 配额。
	var tenant struct {
		WorkspaceMaxSizeGB sql.NullInt32 `db:"workspace_max_size_gb"`
	}
	err := h.db.GetContext(ctx, &tenant,
		`SELECT workspace_max_size_gb FROM auth.tenants WHERE id = $1`, tenantID)
	if err != nil {
		if err == sql.ErrNoRows {
			// Tenant not found in auth.tenants - use default quota (5GB = 5 workspaces)
			// auth.tenants 中找不到该 tenant——使用默认配额（5GB，即 5 个 workspace）。
			h.logger.Debug("Tenant not found in auth.tenants, using default quota",
				zap.String("tenant_id", tenantID.String()))
			return nil
		}
		h.logger.Warn("Failed to query tenant workspace quota", zap.Error(err))
		// Don't block on quota check failure
		// 配额检查失败时不阻塞主流程。
		return nil
	}

	// If workspace_max_size_gb is not set, use default (5GB)
	// 未设置 workspace_max_size_gb 时使用默认值（5GB）。
	maxWorkspaces := 5
	if tenant.WorkspaceMaxSizeGB.Valid && tenant.WorkspaceMaxSizeGB.Int32 > 0 {
		maxWorkspaces = int(tenant.WorkspaceMaxSizeGB.Int32)
	}

	// Count active sessions for this tenant as a proxy for workspace count
	// 用该 tenant 的活跃 session 数近似 workspace 数量。
	var sessionCount int
	err = h.db.GetContext(ctx, &sessionCount, `
		SELECT COUNT(*) FROM sessions
		WHERE tenant_id = $1 AND deleted_at IS NULL`,
		tenantID)
	if err != nil {
		h.logger.Warn("Failed to count tenant sessions", zap.Error(err))
		// Don't block on count failure
		// 统计失败时不阻塞主流程。
		return nil
	}

	if sessionCount >= maxWorkspaces {
		h.logger.Warn("Workspace quota exceeded",
			zap.String("tenant_id", tenantID.String()),
			zap.Int("session_count", sessionCount),
			zap.Int("max_workspaces", maxWorkspaces))
		return ErrWorkspaceQuotaExceeded
	}

	return nil
}

// SendSwarmMessage handles POST /api/v1/swarm/{workflowID}/message
// Sends a human message to a running SwarmWorkflow's Lead agent.
// SendSwarmMessage 处理 POST /api/v1/swarm/{workflowID}/message：向运行中的 SwarmWorkflow 的 Lead agent 发送人工消息。
func (h *TaskHandler) SendSwarmMessage(w http.ResponseWriter, r *http.Request) {
	// Verify tenant ownership
	// 校验 tenant 归属。
	userCtx, ok := r.Context().Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	workflowID := r.PathValue("workflowID")
	if workflowID == "" {
		h.sendError(w, "workflow_id is required", http.StatusBadRequest)
		return
	}

	// Verify the workflow belongs to the caller's tenant and get session_id
	// 校验该 workflow 属于调用方 tenant，并取出 session_id。
	if h.db == nil {
		h.sendError(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	var sessionID string
	var tenantID string
	err := h.db.QueryRowContext(r.Context(),
		`SELECT tenant_id, session_id FROM task_executions WHERE workflow_id = $1 LIMIT 1`,
		workflowID).Scan(&tenantID, &sessionID)
	if err != nil {
		h.sendError(w, "Workflow not found", http.StatusNotFound)
		return
	}
	if tenantID != userCtx.TenantID.String() {
		h.sendError(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Limit request body to 64KB to prevent oversized Temporal signal payloads
	// request 体限制 64KB，避免 Temporal signal payload 过大。
	r.Body = http.MaxBytesReader(w, r.Body, 64*1024)

	type swarmMessageRequest struct {
		Message string `json:"message"`
	}
	var req swarmMessageRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Message == "" {
		h.sendError(w, "message is required", http.StatusBadRequest)
		return
	}

	const maxMessageLen = 32 * 1024 // 32KB per message 注：每条消息 32KB
	if len(req.Message) > maxMessageLen {
		h.sendError(w, fmt.Sprintf("message too long (%d bytes, max %d)", len(req.Message), maxMessageLen), http.StatusBadRequest)
		return
	}

	ctx := withGRPCMetadata(r.Context(), r)

	resp, err := h.orchClient.SendSwarmMessage(ctx, &orchpb.SendSwarmMessageRequest{
		WorkflowId: workflowID,
		Message:    req.Message,
	})
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				h.sendError(w, st.Message(), http.StatusBadRequest)
			case codes.NotFound:
				h.sendError(w, "Workflow not found", http.StatusNotFound)
			default:
				h.sendError(w, "Failed to send message", http.StatusInternalServerError)
			}
			return
		}
		h.sendError(w, "Failed to send message", http.StatusInternalServerError)
		return
	}

	// Persist HITL message to session history for multi-turn context
	// 把 HITL 消息写入 session history，供多轮 context 使用。
	if h.sessionMgr != nil && sessionID != "" {
		if addErr := h.sessionMgr.AddMessage(r.Context(), sessionID, session.Message{
			ID:        fmt.Sprintf("msg-%d", time.Now().UnixNano()),
			Role:      "user",
			Content:   req.Message,
			Timestamp: time.Now(),
		}); addErr != nil {
			h.logger.Warn("Failed to persist HITL message to session history",
				zap.String("session_id", sessionID),
				zap.Error(addErr))
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"success": resp.Success,
		"status":  resp.Status,
	})
}
