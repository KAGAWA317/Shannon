package activities

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	"go.temporal.io/sdk/activity"
)

// WorkspaceSnippet is a truncated KV workspace entry for prompt injection.
// WorkspaceSnippet 是从 workspace KV 条目截断而来，用于注入 prompt。
type WorkspaceSnippet struct {
	Author string `json:"author"`
	Data   string `json:"data"`
	Seq    uint64 `json:"seq"`
}

// TeamKnowledgeEntry records a URL fetched by any swarm agent for cross-agent dedup.
// TeamKnowledgeEntry 记录任一 swarm agent 抓取过的 URL，用于跨 agent 去重。
type TeamKnowledgeEntry struct {
	URL       string `json:"url"`
	Agent     string `json:"agent"`
	Summary   string `json:"summary"`
	CharCount int    `json:"char_count"`
}

// TeamMemberInfo describes a teammate for prompt injection.
// TeamMemberInfo 描述一名队友，供 prompt 注入使用。
type TeamMemberInfo struct {
	AgentID string `json:"agent_id"`
	Task    string `json:"task"`
	Role    string `json:"role,omitempty"`
}

// AgentLoopStepInput is the input for a single reason-act iteration of an autonomous agent.
// AgentLoopStepInput 是自主 agent 单次 reason-act 迭代的输入。
type AgentLoopStepInput struct {
	AgentID         string                 `json:"agent_id"`
	WorkflowID      string                 `json:"workflow_id"`
	Task            string                 `json:"task"`
	Iteration       int                    `json:"iteration"`
	MaxIterations   int                    `json:"max_iterations,omitempty"` // Total iterations for budget display 注：用于预算展示的总迭代次数
	Messages        []AgentMailboxMsg      `json:"messages,omitempty"`       // Inbox messages from other agents 注：来自其他 agent 的收件箱消息
	History         []AgentLoopTurn        `json:"history,omitempty"`        // Previous turns in this agent's loop 注：该 agent 循环中的历史轮次
	Context         map[string]any `json:"context,omitempty"`
	SessionID       string                 `json:"session_id,omitempty"`
	TeamRoster      []TeamMemberInfo       `json:"team_roster,omitempty"`    // Teammates and their tasks 注：队友及其任务
	WorkspaceData   []WorkspaceSnippet     `json:"workspace_data,omitempty"` // Recent KV workspace entries 注：最近的 KV workspace 条目
	SuggestedTools  []string               `json:"suggested_tools,omitempty"`  // Tools available to this agent 注：该 agent 可用的工具
	RoleDescription string                 `json:"role_description,omitempty"` // e.g. "financial research specialist" 注：例如 "financial research specialist"
	Role            string                 `json:"role,omitempty"`             // Persona role: researcher, coder, analyst, generalist 注：人格角色：researcher、coder、analyst、generalist
	TaskList        []SwarmTask            `json:"task_list,omitempty"`        // Current TaskList state for prompt injection 注：当前 TaskList 状态，用于 prompt 注入
	ModelTier          string                 `json:"model_tier,omitempty"`          // "small", "medium", "large" (empty = default) 注："small"、"medium"、"large"（空 = 默认）
	PreviousResponseID string                 `json:"previous_response_id,omitempty"` // OpenAI Responses API: chain from previous response 注：OpenAI Responses API：从上一个 response 续接
	SystemMessage      string                 `json:"system_message,omitempty"`       // Urgent directive appended to user prompt end (recency bias) 注：追加到用户 prompt 末尾的紧急指令（利用近因偏差）
	RunningNotes       string                 `json:"running_notes,omitempty"`        // Agent's cumulative notes — survives history truncation 注：agent 的累积笔记 —— 历史截断后仍保留
	IsSwarm            bool                   `json:"is_swarm,omitempty"`             // True when running inside SwarmWorkflow (enables done→idle mapping) 注：在 SwarmWorkflow 内运行时为 true（启用 done→idle 映射）
	CumulativeToolCalls int                   `json:"cumulative_tool_calls,omitempty"` // Total tool_calls across all tasks (survives reassignment) 注：所有任务累计的 tool_calls 总数（重新分配后仍保留）
	TeamKnowledge      []TeamKnowledgeEntry   `json:"team_knowledge,omitempty"`       // URLs already fetched by other agents (L1 dedup) 注：其他 agent 已抓取的 URL（L1 去重）
	OriginalQuery      string                 `json:"original_query,omitempty"`       // User's original question for agent context 注：用户的原始问题，供 agent 作为 context
}

// AgentMailboxMsg is a message received from another agent's mailbox.
// AgentMailboxMsg 是从其他 agent mailbox 收到的消息。
type AgentMailboxMsg struct {
	From    string         `json:"from"`
	Type    string         `json:"type"`
	Payload map[string]any `json:"payload"`
}

// AgentLoopTurn records a previous action in the agent's loop for context.
// AgentLoopTurn 记录 agent 循环中的上一步动作，用于提供 context。
type AgentLoopTurn struct {
	Iteration       int    `json:"iteration"`
	Action          string `json:"action"`
	Result          any    `json:"result,omitempty"`
	DecisionSummary string `json:"decision_summary,omitempty"`
	AssistantReplay string `json:"assistant_replay,omitempty"` // Compact JSON for multi-turn cache replay 注：用于多轮缓存回放的紧凑 JSON
	ObservationText string `json:"observation_text,omitempty"` // Frozen tool result digest 注：冻结的工具结果摘要
}

// AgentLoopStepResult is the LLM's decision for one iteration.
// AgentLoopStepResult 是 LLM 对单次迭代给出的决策。
type AgentLoopStepResult struct {
	Action          string `json:"action"`                     // "tool_call", "send_message", "publish_data", "request_help", "done" 注："tool_call"、"send_message"、"publish_data"、"request_help"、"done"
	DecisionSummary string `json:"decision_summary,omitempty"` // 1-3 sentence reasoning (D7: controlled, not unlimited chain-of-thought) 注：1-3 句推理（D7：受控，而非无限 chain-of-thought）

	// tool_call fields
	// tool_call 字段
	Tool       string         `json:"tool,omitempty"`
	ToolParams map[string]any `json:"tool_params,omitempty"`
	ToolResult any            `json:"tool_result,omitempty"` // Populated after execution 注：执行完成后填充

	// send_message fields
	// send_message 字段
	To          string         `json:"to,omitempty"`
	MessageType string         `json:"message_type,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`

	// publish_data fields
	// publish_data 字段
	Topic string `json:"topic,omitempty"`
	Data  string `json:"data,omitempty"`

	// request_help fields
	// request_help 字段
	HelpDescription string   `json:"help_description,omitempty"`
	HelpSkills      []string `json:"help_skills,omitempty"`

	// complete_task / claim_task fields
	// complete_task / claim_task 字段
	TaskID string `json:"task_id,omitempty"` // TaskList entry to mark completed or claimed 注：要标记为已完成或已认领的 TaskList 条目

	// create_task fields
	// create_task 字段
	TaskDescription string `json:"task_description,omitempty"`

	// done fields
	// done 字段
	Response string `json:"response,omitempty"`

	// LLM usage metadata
	// LLM 用量元数据
	TokensUsed          int    `json:"tokens_used,omitempty"`
	InputTokens         int    `json:"input_tokens,omitempty"`
	OutputTokens        int    `json:"output_tokens,omitempty"`
	CacheReadTokens     int    `json:"cache_read_tokens,omitempty"`
	CacheCreationTokens int    `json:"cache_creation_tokens,omitempty"`
	CallSequence        int    `json:"call_sequence,omitempty"`
	ModelUsed           string `json:"model_used,omitempty"`
	Provider            string `json:"provider,omitempty"`
	ResponseID          string                 `json:"response_id,omitempty"` // OpenAI Responses API ID for chaining 注：用于串联的 OpenAI Responses API ID
	CompletionReport    map[string]any `json:"completion_report,omitempty"`
	Notes               string         `json:"notes,omitempty"`            // Agent's updated running notes for next iteration 注：agent 更新后的 running notes，供下一轮使用
	AssistantReplay     string         `json:"assistant_replay,omitempty"` // Compact JSON for multi-turn cache replay 注：用于多轮缓存回放的紧凑 JSON
}

// AgentLoopStep calls the Python LLM service's /agent/loop endpoint
// to get the next action for a persistent agent.
// AgentLoopStep 调用 Python LLM 服务的 /agent/loop 接口，为常驻 agent 取下一步动作。
func AgentLoopStep(ctx context.Context, in AgentLoopStepInput) (AgentLoopStepResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("AgentLoopStep called", "agent_id", in.AgentID, "iteration", in.Iteration, "task_len", len(in.Task))

	base := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/agent/loop", base)

	body, err := json.Marshal(in)
	if err != nil {
		return AgentLoopStepResult{}, fmt.Errorf("failed to marshal agent loop input: %w", err)
	}

	timeoutSec := 180 // /agent/loop includes LLM call + tool execution + interpretation pass 注：/agent/loop 同时包含 LLM 调用、工具执行与结果解释环节
	if v := os.Getenv("AGENT_LOOP_STEP_TIMEOUT_SECONDS"); v != "" {
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
		return AgentLoopStepResult{}, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return AgentLoopStepResult{}, fmt.Errorf("agent loop step HTTP call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		logger.Error("AgentLoopStep HTTP error", "agent_id", in.AgentID, "status", resp.StatusCode, "body", string(bodyBytes))
		return AgentLoopStepResult{}, fmt.Errorf("agent loop step returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var out AgentLoopStepResult
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return AgentLoopStepResult{}, fmt.Errorf("failed to decode agent loop step response: %w", err)
	}

	logger.Info("AgentLoopStep completed", "agent_id", in.AgentID, "iteration", in.Iteration, "action", out.Action, "tokens", out.TokensUsed)
	return out, nil
}
