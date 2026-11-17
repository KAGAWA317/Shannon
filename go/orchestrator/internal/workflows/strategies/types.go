package strategies

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"time"
)

// TaskInput represents the input to a workflow
// workflow 的输入契约：字段只增不改，改名或删除会让在途 workflow 的历史重放失败。
type TaskInput struct {
	Query     string
	UserID    string
	TenantID  string
	SessionID string
	Context   map[string]any
	Mode      string

	TemplateName    string
	TemplateVersion string
	DisableAI       bool

	// Session context for multi-turn conversations
	// 多轮续接靠这两个字段：History 是近期消息，SessionCtx 是跨轮持久化 context。
	History    []Message      // Recent conversation history 注：近期对话历史
	SessionCtx map[string]any // Persistent session context 注：跨轮持久的 session context

	// Human intervention settings
	// 人工审批开关；ApprovalTimeout 为 0 时走全局默认值。
	RequireApproval bool // Whether to require human approval for this task 注：该 task 是否需要人工审批
	ApprovalTimeout int  // Timeout in seconds for human approval (0 = use default) 注：人工审批的 timeout 秒数（0 = 使用默认值）

	// Workflow behavior flags (deterministic per-run)
	// 必须 per-run 确定：重放时取值若变化，workflow 会被判定为非确定性。
	BypassSingleResult bool // If true, return single successful result directly 注：为 true 时直接返回单个成功结果

	// Parent workflow ID for event streaming (used by child workflows)
	// child workflow 用它把 event 汇到 parent 的 stream，避免前端收到重复的 stream。
	ParentWorkflowID string // Parent workflow ID for unified event streaming 注：用于统一 event streaming 的 parent workflow ID

	// Optional: preplanned decomposition to avoid re-decompose
	// 可选：复用上游已算好的 decomposition，省一次 LLM 调用。
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
// Metadata 由各 strategy 自行填充，字段不固定，读取方需按需断言。
type TaskResult struct {
	Result       string
	Success      bool
	TokensUsed   int
	ErrorMessage string
	Metadata     map[string]any
}
