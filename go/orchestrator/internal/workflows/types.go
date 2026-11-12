package workflows

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"time"
)

// TaskInput represents the input to a workflow
type TaskInput struct {
	Query     string
	UserID    string
	TenantID  string
	SessionID string
	Context   map[string]any
	Mode      string

	// Template routing hints (optional, user-specified)
	TemplateName    string
	TemplateVersion string
	DisableAI       bool // When true, fall back only to templates (no AI decomposition)
	// 打开后只走模板匹配，不再调用 LLM 做分解。

	// Session context for multi-turn conversations
	History    []Message      // Recent conversation history
	SessionCtx map[string]any // Persistent session context

	// Human intervention settings
	RequireApproval bool // Whether to require human approval for this task
	ApprovalTimeout int  // Timeout in seconds for human approval (0 = use default)

	// Workflow behavior flags (deterministic per-run)
	BypassSingleResult bool // If true, return single successful result directly
	// 单 subtask 成功时跳过合成调用，省一次 LLM。

	// Parent workflow ID for event streaming (used by child workflows)
	ParentWorkflowID string // Parent workflow ID for unified event streaming

	// Tool suggestions from decomposition (for simple tasks with tools)
	SuggestedTools []string       // Tools suggested by decomposition
	ToolParameters map[string]any // Pre-structured parameters for tool execution

	// Optional: preplanned decomposition from router to avoid re-decompose downstream
	// router 已分解过就透传，下游不再重复调用 DecomposeTask。
	PreplannedDecomposition *activities.DecompositionResult
}

// Message represents a conversation message
type Message struct {
	Role      string
	Content   string
	Timestamp time.Time
}

// TaskResult represents the result of a workflow execution
type TaskResult struct {
	Result       string
	Success      bool
	TokensUsed   int
	ErrorMessage string
	Metadata     map[string]any
}
