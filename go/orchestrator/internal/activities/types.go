package activities

import "time"

// ComplexityAnalysisInput is the input for complexity analysis
// ComplexityAnalysisInput 是复杂度分析的输入
type ComplexityAnalysisInput struct {
	Query   string
	Context map[string]any
}

// ComplexityAnalysisResult is the result of complexity analysis
// ComplexityAnalysisResult 是复杂度分析的结果
type ComplexityAnalysisResult struct {
	ComplexityScore float64
	Mode            string
	Subtasks        []Subtask
}

// OutputFormatSpec defines expected output structure for a subtask
// OutputFormatSpec 定义 subtask 期望的输出结构
type OutputFormatSpec struct {
	Type           string   `json:"type,omitempty"`            // "structured", "narrative", "list" 注：取值 "structured"、"narrative"、"list"
	RequiredFields []string `json:"required_fields,omitempty"` // Fields that must be present 注：必须存在的字段
	OptionalFields []string `json:"optional_fields,omitempty"` // Nice-to-have fields 注：可选（有则更好）的字段
}

// SourceGuidanceSpec defines source type recommendations for a subtask
// SourceGuidanceSpec 定义 subtask 的源类型建议
type SourceGuidanceSpec struct {
	Required []string `json:"required,omitempty"` // Must use these source types (official, aggregator, news, academic) 注：必须使用这些源类型（official、aggregator、news、academic）
	Optional []string `json:"optional,omitempty"` // May use these source types 注：可以使用这些源类型
	Avoid    []string `json:"avoid,omitempty"`    // Should not use (social, forums) 注：不应使用（social、forums）
}

// SearchBudgetSpec defines search limits for a subtask
// SearchBudgetSpec 定义 subtask 的检索上限
type SearchBudgetSpec struct {
	MaxQueries int `json:"max_queries,omitempty"` // Maximum web_search calls 注：web_search 调用上限
	MaxFetches int `json:"max_fetches,omitempty"` // Maximum web_fetch calls 注：web_fetch 调用上限
}

// BoundariesSpec defines scope boundaries for a subtask
// BoundariesSpec 定义 subtask 的范围边界
type BoundariesSpec struct {
	InScope    []string `json:"in_scope,omitempty"`     // Topics explicitly within scope 注：明确在范围内的 topic
	OutOfScope []string `json:"out_of_scope,omitempty"` // Topics to avoid (prevent overlap) 注：应避开的 topic（避免重叠）
}

// Subtask represents a decomposed subtask
// Subtask 表示拆解后的一个 subtask
type Subtask struct {
	ID              string
	Description     string
	Dependencies    []string
	EstimatedTokens int
	// Structured subtask classification to avoid brittle string matching
	// 结构化的 subtask 分类，避免依赖脆弱的字符串匹配。
	TaskType string `json:"task_type,omitempty"`
	// Plan IO (optional, plan_io_v1): topics produced/consumed by this subtask
	// Plan IO（可选，plan_io_v1）：本 subtask 产出与消费的 topic。
	Produces []string `json:"produces,omitempty"`
	Consumes []string `json:"consumes,omitempty"`
	// Optional grouping for research-area-driven decomposition
	// 可选分组，供按 research area 驱动的 task 拆解使用。
	ParentArea string `json:"parent_area,omitempty"`
	// LLM-native tool selection
	// 由 LLM 自行选择 tool。
	SuggestedTools []string       `json:"suggested_tools"`
	ToolParameters map[string]any `json:"tool_parameters"`
	// Persona assignment for specialized agent behavior
	// 指定 persona，以启用专门的 agent 行为。
	SuggestedPersona string `json:"suggested_persona"`
	// Deep Research 2.0: Task Contract fields for explicit boundaries
	// Deep Research 2.0：Task Contract 字段，用于显式界定 task 边界。
	OutputFormat   *OutputFormatSpec   `json:"output_format,omitempty"`   // Expected output structure 注：期望的输出结构
	SourceGuidance *SourceGuidanceSpec `json:"source_guidance,omitempty"` // Source type recommendations 注：源类型建议
	SearchBudget   *SearchBudgetSpec   `json:"search_budget,omitempty"`   // Search limits 注：检索上限
	Boundaries     *BoundariesSpec     `json:"boundaries,omitempty"`      // Scope boundaries 注：范围边界
	// Multimodal: whether this subtask needs direct access to user-uploaded attachments.
	// Set by decomposition LLM. When false, attachments are stripped from agent context to save tokens.
	// 多模态：该 subtask 是否需要直接访问用户上传的附件。
	// 由负责拆解的 LLM 设置；为 false 时会从 agent context 中剥离附件以节省 tokens。
	NeedsAttachments bool `json:"needs_attachments,omitempty"`
}

// AgentExecutionInput is the input for agent execution
// AgentExecutionInput 是 agent 执行的输入
type AgentExecutionInput struct {
	Query     string
	AgentID   string
	Context   map[string]any
	Mode      string
	SessionID string   // Session identifier 注：session 标识
	UserID    string   // User identifier (for memory mount, audit) 注：用户标识（用于挂载 memory、审计）
	History   []string // Conversation history 注：对话历史
	// LLM-native tool selection
	// 由 LLM 自行选择 tool。
	SuggestedTools []string       `json:"suggested_tools"`
	ToolParameters map[string]any `json:"tool_parameters"`
	// Persona for specialized agent behavior
	// 用于专门 agent 行为的 persona
	PersonaID string `json:"persona_id"`
	// Parent workflow ID for unified event streaming
	// 父 workflow ID，用于统一事件流
	ParentWorkflowID string `json:"parent_workflow_id,omitempty"`
}

// AgentExecutionResult is the result of agent execution
// AgentExecutionResult 是 agent 执行的结果
type AgentExecutionResult struct {
	AgentID             string
	Role                string `json:"role,omitempty"`
	Response            string
	TokensUsed          int
	ModelUsed           string
	Provider            string
	InputTokens         int
	OutputTokens        int
	CacheReadTokens       int `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens   int `json:"cache_creation_tokens,omitempty"`
	CacheCreation1hTokens int `json:"cache_creation_1h_tokens,omitempty"`
	CallSequence          int `json:"call_sequence,omitempty"`
	DurationMs          int64
	Success             bool
	Error               string
	// Tools used and their outputs (when applicable)
	// 使用过的 tools 及其输出（适用时）。
	ToolsUsed      []string        `json:"tools_used,omitempty"`
	ToolExecutions []ToolExecution `json:"tool_executions,omitempty"`
	Metadata       map[string]any  `json:"metadata,omitempty"`
	// ScreenshotPaths holds relative paths of persisted browser screenshots (e.g. "screenshots/17093..._0.png")
	// ScreenshotPaths 保存已持久化的浏览器截图相对路径（如 "screenshots/17093..._0.png"）。
	ScreenshotPaths []string `json:"screenshot_paths,omitempty"`
}

// ToolExecution summarizes a single tool invocation result returned by Agent-Core
// ToolExecution 概括 Agent-Core 返回的单次 tool 调用结果。
type ToolExecution struct {
	Tool        string `json:"tool"`
	Success     bool   `json:"success"`
	Output      any    `json:"output,omitempty"`
	Error       string `json:"error,omitempty"`
	DurationMs  int64  `json:"duration_ms,omitempty"`
	InputParams any    `json:"tool_input,omitempty"`
}

// SynthesisInput is the input for result synthesis
// SynthesisInput 是结果综合的输入
type SynthesisInput struct {
	Query        string
	AgentResults []AgentExecutionResult
	Context      map[string]any // Optional context for synthesis 注：可选的综合上下文
	// Parent workflow ID for unified event streaming
	// 父 workflow ID，用于统一事件流
	ParentWorkflowID   string `json:"parent_workflow_id,omitempty"`
	CollectedCitations any    `json:"collected_citations,omitempty"` // []metadata.Citation to avoid import cycle 注：类型为 []metadata.Citation，以避免循环 import
	// SessionID for reading workspace files (swarm mode)
	// SessionID 用于读取 workspace 文件（swarm 模式）。
	SessionID string `json:"session_id,omitempty"`
}

// SynthesisResult is the result of synthesis
// SynthesisResult 是综合的结果
type SynthesisResult struct {
	FinalResult            string
	TokensUsed             int
	FinishReason           string // Reason model stopped: "stop", "length", "content_filter", etc. 注：model 停止的原因："stop"、"length"、"content_filter" 等
	RequestedMaxTokens     int    // Max completion tokens requested from provider for this synthesis 注：本次综合向 provider 请求的最大 completion tokens
	CompletionTokens       int    // Output tokens (excludes prompt) 注：输出 tokens（不含 prompt）
	EffectiveMaxCompletion int    // Actual max completion after provider headroom clamp 注：经 provider headroom 钳制后的实际最大 completion
	// Added for accurate cost tracking
	// 为精确成本核算新增
	InputTokens  int     // Prompt tokens (when available or inferred) 注：prompt tokens（可用或推算得出时）
	CallSequence int     // Monotonic call sequence (always 1 for synthesis) 注：单调递增的调用序号（综合场景恒为 1）
	ModelUsed    string  // Model used for synthesis 注：综合使用的 model
	Provider     string  // Provider used for synthesis 注：综合使用的 provider
	CostUsd      float64 // Reported cost from provider metadata when available 注：provider metadata 给出时上报的成本
}

// EvaluateResultInput carries data for reflection/quality checks
// EvaluateResultInput 承载反思/质量检查所需数据
type EvaluateResultInput struct {
	Query    string
	Response string
	Criteria []string
}

// EvaluateResultOutput returns a simple quality score and feedback
// EvaluateResultOutput 返回简单的质量分与反馈
type EvaluateResultOutput struct {
	Score    float64
	Feedback string
}

// VerifyClaimsInput is the input for claim verification
// VerifyClaimsInput 是 claim 校验的输入
type VerifyClaimsInput struct {
	Answer    string // Synthesis result to verify 注：待校验的综合结果
	Citations []any  // Available citations (from metadata.CollectCitations) 注：可用的引用（来自 metadata.CollectCitations）
}

// VerificationResult contains claim verification analysis (V2 format with three-category classification)
// VerificationResult 保存 claim 校验分析结果（V2 格式，按三类判定划分）。
type VerificationResult struct {
	OverallConfidence float64          `json:"overall_confidence"` // 0.0-1.0 注：0.0-1.0
	TotalClaims       int              `json:"total_claims"`       // Number of claims extracted 注：抽取出的 claim 数量
	SupportedClaims   int              `json:"supported_claims"`   // Claims with supporting citations (count) 注：有支持性引用的 claim 数（计数）
	Conflicts         []ConflictReport `json:"conflicts"`          // Conflicting information found 注：发现的冲突信息

	// V2 three-category fields
	// V2 三类判定字段
	UnsupportedClaims          int `json:"unsupported_claims"`           // Claims contradicted by sources (count) 注：被来源否证的 claim 数（计数）
	InsufficientEvidenceClaims int `json:"insufficient_evidence_claims"` // Claims without sufficient evidence (count) 注：证据不足的 claim 数（计数）

	// V2 claim text lists
	// V2 claim 文本列表
	SupportedClaimTexts    []string `json:"supported_claim_texts,omitempty"`
	UnsupportedClaimTexts  []string `json:"unsupported_claim_texts,omitempty"`
	InsufficientClaimTexts []string `json:"insufficient_claim_texts,omitempty"`

	// V2 claim details with verdict and retrieval info
	// V2 claim 明细，含判定与检索信息
	ClaimDetails []ClaimVerification `json:"claim_details"`

	// V2 quality metrics
	// V2 质量指标
	EvidenceCoverage  float64 `json:"evidence_coverage"`   // % of claims with definitive verdict 注：有确定判定的 claim 占比
	AvgRetrievalScore float64 `json:"avg_retrieval_score"` // Average top-1 BM25 relevance 注：top-1 BM25 相关性均值
}

// ClaimVerification contains verification for a single claim (V2 format)
// ClaimVerification 是单条 claim 的校验结果（V2 格式）。
type ClaimVerification struct {
	Claim                string  `json:"claim"`                 // The factual claim text 注：claim 的事实陈述文本
	SupportingCitations  []int   `json:"supporting_citations"`  // Citation numbers supporting this claim 注：支持该 claim 的引用编号
	ConflictingCitations []int   `json:"conflicting_citations"` // Citation numbers conflicting with this claim 注：与该 claim 冲突的引用编号
	Confidence           float64 `json:"confidence"`            // 0.0-1.0 (weighted by citation credibility) 注：0.0-1.0（按引用可信度加权）

	// V2 fields
	// V2 字段
	Verdict         string          `json:"verdict,omitempty"`          // "supported" | "unsupported" | "insufficient_evidence" 注：取值 "supported" | "unsupported" | "insufficient_evidence"
	RetrievalScores map[int]float64 `json:"retrieval_scores,omitempty"` // citation_id -> BM25 relevance score 注：citation_id -> BM25 相关性得分
	Reasoning       string          `json:"reasoning,omitempty"`        // Brief explanation for verdict 注：判定的简要说明
}

// ConflictReport describes conflicting information
// ConflictReport 描述冲突信息
type ConflictReport struct {
	Claim       string `json:"claim"`        // The claim in question 注：涉及的 claim
	Source1     int    `json:"source1"`      // Citation number 1 注：引用编号 1
	Source1Text string `json:"source1_text"` // What source 1 says 注：来源 1 的说法
	Source2     int    `json:"source2"`      // Citation number 2 注：引用编号 2
	Source2Text string `json:"source2_text"` // What source 2 says 注：来源 2 的说法
}

// SessionUpdateInput is the input for updating session
// SessionUpdateInput 是更新 session 的输入
type SessionUpdateInput struct {
	SessionID  string
	Result     string
	TokensUsed int
	// Optional: cache-aware token total (= input + output + cache_read +
	// cache_creation). When > 0, UpdateSessionResult prefers this for
	// session.TotalTokensUsed and pricing so cache cost is reflected in
	// the running session counter and cost. Cache-blind callers can leave
	// this zero; the activity will fall back to TokensUsed.
	// 可选：计入 cache 的 token 总量（= input + output + cache_read +
	// cache_creation）。大于 0 时 UpdateSessionResult 优先用它写入
	// session.TotalTokensUsed 并参与计价，使 cache 成本反映到
	// session 累计计数与费用中。不感知 cache 的调用方可以留 0，
	// activity 会回退到 TokensUsed。
	CacheAwareTokensUsed int
	AgentsUsed           int
	CostUSD              float64
	// Optional: model used for this update when single-model
	// 可选：单 model 场景下本次更新使用的 model
	ModelUsed string
	// Optional: per-agent usage for accurate cost across multiple models
	// 可选：按 agent 的用量，便于跨多个 model 精确核算成本
	AgentUsage []AgentUsage `json:"agent_usage,omitempty"`
}

// SessionUpdateResult is the result of session update
// SessionUpdateResult 是 session 更新的结果
type SessionUpdateResult struct {
	Success bool
	Error   string
}

// AgentUsage captures model-specific token usage for cost calculation
// AgentUsage 记录按 model 区分的 token 用量，用于成本计算。
type AgentUsage struct {
	Model                 string `json:"model"`
	Provider              string `json:"provider,omitempty"`
	Tokens                int    `json:"tokens"`
	InputTokens           int    `json:"input_tokens,omitempty"`
	OutputTokens          int    `json:"output_tokens,omitempty"`
	CacheReadTokens       int    `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens   int    `json:"cache_creation_tokens,omitempty"`
	CacheCreation1hTokens int    `json:"cache_creation_1h_tokens,omitempty"`
}

// ── HITL Research Review Types ──
// ── HITL 研究审阅相关类型 ──

// ResearchPlanInput is the input for the GenerateResearchPlan activity
// ResearchPlanInput 是 GenerateResearchPlan activity 的输入
type ResearchPlanInput struct {
	Query      string         `json:"query"`
	Context    map[string]any `json:"context"`
	WorkflowID string         `json:"workflow_id"`
	SessionID  string         `json:"session_id"`
	UserID     string         `json:"user_id"`
	TenantID   string         `json:"tenant_id"`
	TTL        time.Duration  `json:"ttl"`
}

// ResearchPlanResult is the result of the GenerateResearchPlan activity
// ResearchPlanResult 是 GenerateResearchPlan activity 的结果
type ResearchPlanResult struct {
	Message string `json:"message"`
	Intent  string `json:"intent"`
	Round   int    `json:"round"`
}

// ResearchReviewResult is the Signal payload sent when user approves the review
// ResearchReviewResult 是用户批准审阅时发送的 Signal payload
type ResearchReviewResult struct {
	Approved      bool          `json:"approved"`
	FinalPlan     string        `json:"final_plan"`
	Conversation  []ReviewRound `json:"conversation"`
	ResearchBrief string        `json:"research_brief,omitempty"`
}

// ReviewRound represents a single round in the review conversation
// ReviewRound 表示审阅对话中的单轮
type ReviewRound struct {
	Role      string `json:"role"`
	Message   string `json:"message"`
	Timestamp string `json:"timestamp"`
}
