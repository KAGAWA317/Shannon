package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	"go.temporal.io/sdk/activity"
)

// ── Lead Decision Types ─────────────────────────────────────────────────────
// ── Lead 决策类型 ─────────────────────────────────────────────────────

// LeadEvent describes what triggered the Lead to wake up.
// LeadEvent 描述这次唤醒 Lead 的触发源。
type LeadEvent struct {
	Type             string            `json:"type"` // "agent_completed", "agent_idle", "help_request", "checkpoint", "human_input" 注：取值："agent_completed"、"agent_idle"、"help_request"、"checkpoint"、"human_input"
	AgentID          string            `json:"agent_id"`
	ResultSummary    string            `json:"result_summary"`
	HumanMessage     string            `json:"human_message,omitempty"`
	CompletionReport map[string]any    `json:"completion_report,omitempty"`
	Error            string            `json:"error,omitempty"`
	Success          bool              `json:"success,omitempty"`
	FileContents     []FileReadResult  `json:"file_contents,omitempty"` // Lead file_read results 注：Lead 的 file_read 结果
	ToolResults      []ToolResultEntry `json:"tool_results,omitempty"`  // Lead tool_call results 注：Lead 的 tool_call 结果
}

// FileReadResult holds the content of a file read by Lead.
// FileReadResult 保存 Lead 读取某个文件得到的內容。
type FileReadResult struct {
	Path      string `json:"path"`
	Content   string `json:"content"`
	Truncated bool   `json:"truncated"`
	Error     string `json:"error,omitempty"`
}

// ToolResultEntry holds the result of a tool executed by Lead.
// ToolResultEntry 保存 Lead 执行某个工具的结果。
type ToolResultEntry struct {
	Tool   string `json:"tool"`
	Output string `json:"output"`
	Error  string `json:"error,omitempty"`
}

// LeadAgentState tracks an agent's current state for Lead decisions.
// LeadAgentState 记录 agent 当前状态，供 Lead 做决策。
type LeadAgentState struct {
	AgentID        string   `json:"agent_id"`
	Status         string   `json:"status"`          // "running", "idle", "completed" 注：取值："running"、"idle"、"completed"
	CurrentTask    string   `json:"current_task"`
	IterationsUsed int      `json:"iterations_used"`
	ElapsedSeconds int      `json:"elapsed_seconds,omitempty"` // Wall-clock time since agent started current task 注：agent 开始当前 task 以来的实际耗时
	Role           string   `json:"role,omitempty"`             // Agent's assigned role (researcher, analyst, synthesis_writer, etc.) 注：agent 被分配的角色（researcher、analyst、synthesis_writer 等）
	FilesWritten   []string `json:"files_written,omitempty"`    // Files written by agent (for Lead to pass to synthesis_writer) 注：agent 写入的文件（供 Lead 传给 synthesis_writer）
}

// LeadBudget tracks global budget for the swarm.
// LeadBudget 跟踪整个 swarm 的全局 budget。
type LeadBudget struct {
	TotalLLMCalls      int `json:"total_llm_calls"`
	RemainingLLMCalls  int `json:"remaining_llm_calls"`
	TotalTokens        int `json:"total_tokens"`
	RemainingTokens    int `json:"remaining_tokens"`
	ElapsedSeconds     int `json:"elapsed_seconds"`
	MaxWallClockSeconds int `json:"max_wall_clock_seconds"`
}

// LeadDecisionInput is the input to the LeadDecision activity.
// LeadDecisionInput 是 LeadDecision activity 的输入。
type LeadDecisionInput struct {
	WorkflowID    string                   `json:"workflow_id"`
	Event         LeadEvent                `json:"event"`
	TaskList      []SwarmTask              `json:"task_list"`
	AgentStates   []LeadAgentState         `json:"agent_states"`
	Budget        LeadBudget               `json:"budget"`
	History       []map[string]any `json:"history"`                    // Recent Lead decisions 注：最近的 Lead 决策
	Messages            []map[string]any `json:"messages,omitempty"`              // Agent→Lead mailbox messages 注：Agent→Lead 的邮箱消息
	OriginalQuery       string                   `json:"original_query,omitempty"`        // User's original query (for language context) 注：用户的原始 query（用于语言上下文）
	ConversationHistory []map[string]any `json:"conversation_history,omitempty"`   // Session history for multi-turn context 注：session 历史，用于多轮上下文
	WorkspaceFiles      []string                 `json:"workspace_files,omitempty"`        // File paths in workspace (for planning context) 注：workspace 中的文件路径（用于规划上下文）
	HitlMessages        []string                 `json:"hitl_messages,omitempty"`           // All HITL messages received during this execution 注：本次执行期间收到的全部 HITL 消息
	LeadModelOverride    string                  `json:"lead_model_override,omitempty"`     // Explicit model for Lead (e.g. "kimi-k2.5") 注：为 Lead 显式指定的 model（例如 "kimi-k2.5"）
	LeadProviderOverride string                  `json:"lead_provider_override,omitempty"`  // Explicit provider for Lead (e.g. "kimi") 注：为 Lead 显式指定的 provider（例如 "kimi"）
}

// LeadAction is a single action the Lead wants to take.
// LeadAction 是 Lead 想执行的一条动作。
type LeadAction struct {
	Type            string           `json:"type"` // assign_task, spawn_agent, send_message, broadcast, revise_plan, file_read, done 注：取值：assign_task、spawn_agent、send_message、broadcast、revise_plan、file_read、done
	TaskID          string           `json:"task_id,omitempty"`
	AgentID         string           `json:"agent_id,omitempty"`
	Role            string           `json:"role,omitempty"`
	TaskDescription string           `json:"task_description,omitempty"`
	To              string           `json:"to,omitempty"`
	Content         string           `json:"content,omitempty"`
	ModelTier       string           `json:"model_tier,omitempty"` // small, medium, large 注：取值：small、medium、large
	Create          []map[string]any `json:"create,omitempty"`
	Cancel          []string                 `json:"cancel,omitempty"`
	Update          []map[string]any `json:"update,omitempty"` // revise_plan: update existing task descriptions 注：revise_plan：更新已有 task 描述
	Path            string                   `json:"path,omitempty"`            // file_read target path 注：file_read 的目标路径
	Tool               string                   `json:"tool,omitempty"`                // tool_call: web_search, web_fetch, calculator 注：tool_call：web_search、web_fetch、calculator
	ToolParams         map[string]any   `json:"tool_params,omitempty"`         // tool_call: tool arguments 注：tool_call：工具参数
	SkipAttachments bool `json:"skip_attachments,omitempty"` // spawn_agent: set true to NOT pass user files (e.g. web research agents) 注：spawn_agent：置为 true 则不传递用户文件（例如 web 研究类 agent）
}

// LeadDecisionResult is the output from the LeadDecision activity.
// LeadDecisionResult 是 LeadDecision activity 的输出。
type LeadDecisionResult struct {
	DecisionSummary     string       `json:"decision_summary"`
	UserSummary         string       `json:"user_summary"`
	Actions             []LeadAction `json:"actions"`
	TokensUsed          int          `json:"tokens_used"`
	InputTokens         int          `json:"input_tokens"`
	OutputTokens        int          `json:"output_tokens"`
	CacheReadTokens     int          `json:"cache_read_tokens"`
	CacheCreationTokens int          `json:"cache_creation_tokens"`
	CallSequence        int          `json:"call_sequence,omitempty"`
	ModelUsed           string       `json:"model_used"`
	Provider            string       `json:"provider"`
}

// LeadDecision calls the Python LLM service's /lead/decide endpoint (D2: replay-safe).
// LeadDecision 调用 Python LLM service 的 /lead/decide 接口（D2：replay 安全）。
func LeadDecision(ctx context.Context, in LeadDecisionInput) (LeadDecisionResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("LeadDecision called", "workflow_id", in.WorkflowID, "event", in.Event.Type)

	base := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/lead/decide", base)

	body, err := json.Marshal(in)
	if err != nil {
		return LeadDecisionResult{}, fmt.Errorf("failed to marshal lead decision input: %w", err)
	}

	// Must be under the 150s Temporal StartToCloseTimeout to let Temporal
	// detect the timeout and retry, rather than the HTTP client swallowing it.
	// 必须小于 Temporal 的 150s StartToCloseTimeout，让 Temporal 能
	// 感知到 timeout 并 retry，而不是被 HTTP client 自己吞掉。
	timeoutSec := 120
	if v := os.Getenv("LEAD_DECISION_TIMEOUT_SECONDS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			timeoutSec = n
		}
	}

	client := &http.Client{
		Timeout:   time.Duration(timeoutSec) * time.Second,
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return LeadDecisionResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return LeadDecisionResult{}, fmt.Errorf("lead decision HTTP call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logger.Error("LeadDecision HTTP error", "status", resp.StatusCode, "body", string(bodyBytes))
		return LeadDecisionResult{}, fmt.Errorf("lead decision returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var out LeadDecisionResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return LeadDecisionResult{}, fmt.Errorf("failed to decode lead decision response: %w", err)
	}

	logger.Info("LeadDecision completed",
		"workflow_id", in.WorkflowID,
		"decision", out.DecisionSummary,
		"actions", len(out.Actions),
		"tokens", out.TokensUsed,
	)
	return out, nil
}

// ListWorkspaceFilesInput is the input for the ListWorkspaceFiles activity.
// ListWorkspaceFilesInput 是 ListWorkspaceFiles activity 的输入。
type ListWorkspaceFilesInput struct {
	SessionID string `json:"session_id"`
}

// ListWorkspaceFilesResult is the output of the ListWorkspaceFiles activity.
// ListWorkspaceFilesResult 是 ListWorkspaceFiles activity 的输出。
type ListWorkspaceFilesResult struct {
	Files []WorkspaceMaterial `json:"files"`
}

// ListWorkspaceFiles reads workspace file metadata for closing_checkpoint.
// ListWorkspaceFiles 读取 workspace 文件 metadata，供 closing_checkpoint 使用。
func ListWorkspaceFiles(ctx context.Context, in ListWorkspaceFilesInput) (ListWorkspaceFilesResult, error) {
	if in.SessionID == "" {
		return ListWorkspaceFilesResult{}, nil
	}
	if err := validateSessionID(in.SessionID); err != nil {
		return ListWorkspaceFilesResult{}, fmt.Errorf("invalid session_id: %w", err)
	}
	baseDir := getenv("SHANNON_SESSION_WORKSPACES_DIR", "/tmp/shannon-sessions")
	sessionDir := filepath.Join(baseDir, in.SessionID)
	files := readWorkspaceFiles(sessionDir, 10000, 2000)
	return ListWorkspaceFilesResult{Files: files}, nil
}

// ── Lead Tool Execution ───────────────────────────────────────────────────
// ── Lead 工具执行 ───────────────────────────────────────────────────

// LeadToolInput is the input for direct tool execution by Lead.
// LeadToolInput 是 Lead 直接执行工具时的输入。
type LeadToolInput struct {
	Tool       string         `json:"tool"`
	ToolParams map[string]any `json:"tool_params"`
	SessionID  string         `json:"session_id,omitempty"`
}

// LeadToolResult is the output of direct tool execution.
// LeadToolResult 是直接执行工具的输出。
type LeadToolResult struct {
	Success bool   `json:"success"`
	Output  string `json:"output"` // formatted text 注：格式化后的文本
	Error   string `json:"error,omitempty"`
}

// LeadExecuteTool calls Python LLM-service POST /tools/execute directly.
// Zero LLM cost — the tool registry executes without involving an LLM.
// LeadExecuteTool 直接调用 Python LLM-service 的 POST /tools/execute。
// 不产生 LLM 成本：完全由 tool registry 执行，不经过 LLM。
func LeadExecuteTool(ctx context.Context, in LeadToolInput) (LeadToolResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("LeadExecuteTool called", "tool", in.Tool)

	base := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/tools/execute", base)

	// Build request body matching Python ToolExecuteRequest field names
	// 构造 request body，字段名与 Python 侧 ToolExecuteRequest 对齐。
	reqBody := map[string]any{
		"tool_name":  in.Tool,
		"parameters": in.ToolParams,
	}
	body, err := json.Marshal(reqBody)
	if err != nil {
		return LeadToolResult{}, fmt.Errorf("failed to marshal tool input: %w", err)
	}

	client := &http.Client{
		Timeout:   60 * time.Second,
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return LeadToolResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return LeadToolResult{Error: err.Error()}, nil // Return error in result, don't fail activity 注：把错误放进返回值，不让 activity 失败。
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return LeadToolResult{Error: fmt.Sprintf("HTTP %d: %s", resp.StatusCode, string(bodyBytes))}, nil
	}

	var pyResp struct {
		Success bool   `json:"success"`
		Output  any    `json:"output"`
		Text    string `json:"text"`
		Error   string `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&pyResp); err != nil {
		return LeadToolResult{Error: fmt.Sprintf("decode error: %v", err)}, nil
	}

	// Prefer formatted text; fall back to raw output
	// 优先用格式化后的 text，缺失时回退到原始 output。
	output := pyResp.Text
	if output == "" {
		if pyResp.Output != nil {
			if s, ok := pyResp.Output.(string); ok {
				output = s
			} else {
				b, _ := json.Marshal(pyResp.Output)
				output = string(b)
			}
		}
	}

	logger.Info("LeadExecuteTool completed", "tool", in.Tool, "success", pyResp.Success, "output_len", len(output))
	return LeadToolResult{
		Success: pyResp.Success,
		Output:  output,
		Error:   pyResp.Error,
	}, nil
}
