package workflows

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"time"
)

// TaskInput represents the input to a workflow
// TaskInput 表示工作流的输入
type TaskInput struct {
	Query     string
	UserID    string
	TenantID  string
	SessionID string
	Context   map[string]any
	Mode      string

	// Template routing hints (optional, user-specified)
	// 模板路由提示（可选，由用户指定）
	TemplateName    string
	TemplateVersion string
	DisableAI       bool // When true, fall back only to templates (no AI decomposition) 注：打开后只走模板匹配，不再调用 LLM 做分解。

	// Session context for multi-turn conversations
	// 多轮对话的 session 上下文
	History    []Message      // Recent conversation history 注：近期对话历史
	SessionCtx map[string]any // Persistent session context 注：持久化的 session 上下文

	// Human intervention settings
	// 人工介入设置
	RequireApproval bool // Whether to require human approval for this task 注：本任务是否需要人工审批
	ApprovalTimeout int  // Timeout in seconds for human approval (0 = use default) 注：人工审批的超时时间（秒），0 表示使用默认值

	// Workflow behavior flags (deterministic per-run)
	// 工作流行为开关（每次运行确定）
	BypassSingleResult bool // If true, return single successful result directly 注：单 subtask 成功时跳过合成调用，省一次 LLM。

	// Parent workflow ID for event streaming (used by child workflows)
	// 用于事件流式推送的父 workflow ID（供 child workflow 使用）
	ParentWorkflowID string // Parent workflow ID for unified event streaming 注：用于统一事件流式推送的父 workflow ID

	// Tool suggestions from decomposition (for simple tasks with tools)
	// 分解产出的 tool 建议（适用于带 tool 的简单任务）
	SuggestedTools []string       // Tools suggested by decomposition 注：分解建议使用的 tool
	ToolParameters map[string]any // Pre-structured parameters for tool execution 注：tool 执行的预构造参数

	// Optional: preplanned decomposition from router to avoid re-decompose downstream
	// router 已分解过就透传，下游不再重复调用 DecomposeTask。
	PreplannedDecomposition *activities.DecompositionResult
}

// Message represents a conversation message
// Message 表示一条对话消息
type Message struct {
	Role      string
	Content   string
	Timestamp time.Time
}

// TaskResult represents the result of a workflow execution
// TaskResult 表示一次工作流执行的结果
type TaskResult struct {
	Result       string
	Success      bool
	TokensUsed   int
	ErrorMessage string
	Metadata     map[string]any
}
