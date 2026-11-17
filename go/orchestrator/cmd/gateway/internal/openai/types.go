// Package openai provides OpenAI-compatible API endpoints for Shannon.
// Package openai 提供面向 Shannon 的 OpenAI 兼容 API endpoint。
package openai

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// ChatCompletionRequest represents an OpenAI-compatible chat completion request.
// ChatCompletionRequest 表示一个 OpenAI 兼容的 chat completion request。
// See: https://platform.openai.com/docs/api-reference/chat/create
// 参见：https://platform.openai.com/docs/api-reference/chat/create
type ChatCompletionRequest struct {
	Model            string          `json:"model"`
	Messages         []ChatMessage   `json:"messages"`
	Stream           bool            `json:"stream,omitempty"`
	MaxTokens        int             `json:"max_tokens,omitempty"`
	Temperature      *float64        `json:"temperature,omitempty"` // Pointer to distinguish "not set" from 0 注：指针用于区分“未设置”与 0
	TopP             *float64        `json:"top_p,omitempty"`       // Pointer to distinguish "not set" from 0 注：指针用于区分“未设置”与 0
	N                int             `json:"n,omitempty"`
	Stop             []string        `json:"stop,omitempty"`
	PresencePenalty  float64         `json:"presence_penalty,omitempty"`
	FrequencyPenalty float64         `json:"frequency_penalty,omitempty"`
	User             string          `json:"user,omitempty"`
	StreamOptions    *StreamOptions  `json:"stream_options,omitempty"`
	ShannonOptions   *ShannonOptions `json:"shannon_options,omitempty"` // Shannon-specific options 注：Shannon 专有选项
}

// ShannonOptions contains Shannon-specific request options.
// These extend the OpenAI API with Shannon capabilities.
// ShannonOptions 包含 Shannon 专有的 request 选项，在 OpenAI API 之上扩展 Shannon 能力。
type ShannonOptions struct {
	// Context is passed directly to the Shannon orchestrator
	// Context 直接传入 Shannon orchestrator。
	Context map[string]any `json:"context,omitempty"`
	// Agent specifies a single-purpose agent to execute (e.g., "serp-ads", "competitor-discover")
	// Agent 指定要执行的单一用途 agent（例如 "serp-ads"、"competitor-discover"）。
	Agent string `json:"agent,omitempty"`
	// AgentInput provides parameters for the agent (alternative to context.agent_input)
	// AgentInput 为 agent 提供参数（替代 context.agent_input）。
	AgentInput map[string]any `json:"agent_input,omitempty"`
	// Role specifies a workflow role (e.g., "browser_use")
	// Role 指定 workflow 角色（例如 "browser_use"）。
	Role string `json:"role,omitempty"`
	// ResearchStrategy sets the research depth (quick, standard, deep, academic)
	// ResearchStrategy 设置 research 深度（quick、standard、deep、academic）。
	ResearchStrategy string `json:"research_strategy,omitempty"`
	// ModelTier overrides the model tier (small, medium, large)
	// ModelTier 覆盖模型档位（small、medium、large）。
	ModelTier string `json:"model_tier,omitempty"`
}

// ChatMessage represents a single message in the conversation.
// Supports both plain string content (backward compatible) and OpenAI-compatible
// content arrays containing text, image_url, and file blocks.
// ChatMessage 表示对话中的一条消息：既支持纯字符串 content（向后兼容），
// 也支持 OpenAI 兼容的 content 数组（text、image_url、file 块）。
type ChatMessage struct {
	Role                         string          `json:"-"`
	Content                      string          `json:"-"` // Extracted text (always populated) 注：提取出的文本（始终有值）
	RawContent                   json.RawMessage `json:"-"` // Original JSON content field 注：原始 JSON content 字段
	RawAttachments               []RawAttachment `json:"-"` // Extracted attachments from content array 注：从 content 数组提取的附件
	ContentWithAttachmentSummary string          `json:"-"` // Text + "[Attached: ...]" summaries 注：文本 + "[Attached: ...]" 摘要
	Name                         string          `json:"-"`
}

// MarshalJSON produces OpenAI-compatible JSON with content as a plain string.
// MarshalJSON 输出 OpenAI 兼容的 JSON，content 为纯字符串。
func (m ChatMessage) MarshalJSON() ([]byte, error) {
	type plain struct {
		Role    string `json:"role"`
		Content string `json:"content"`
		Name    string `json:"name,omitempty"`
	}
	return json.Marshal(plain{
		Role:    m.Role,
		Content: m.Content,
		Name:    m.Name,
	})
}

// UnmarshalJSON supports both string content and array-of-blocks content.
// UnmarshalJSON 同时支持字符串 content 与块数组 content。
func (m *ChatMessage) UnmarshalJSON(data []byte) error {
	// Use an intermediate struct to capture raw content
	// 使用中间 struct 捕获原始 content。
	var aux struct {
		Role       string          `json:"role"`
		RawContent json.RawMessage `json:"content"`
		Name       string          `json:"name,omitempty"`
	}
	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	m.Role = aux.Role
	m.Name = aux.Name
	m.RawContent = aux.RawContent

	if len(aux.RawContent) > 0 {
		parsed, err := ParseContent(aux.RawContent)
		if err != nil {
			return fmt.Errorf("parse content for role=%s: %w", m.Role, err)
		}
		m.Content = parsed.Text
		m.RawAttachments = parsed.Attachments

		if len(parsed.Attachments) > 0 {
			var descs []string
			for _, att := range parsed.Attachments {
				name := att.Filename
				if name == "" {
					name = att.MediaType
				}
				descs = append(descs, fmt.Sprintf("[Attached: %s]", name))
			}
			m.ContentWithAttachmentSummary = m.Content + "\n" + strings.Join(descs, " ")
		}
	}
	return nil
}

// StreamOptions controls streaming behavior.
// StreamOptions 控制 streaming 行为。
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

// ChatCompletionResponse represents a non-streaming response.
// ChatCompletionResponse 表示非 streaming 的响应。
type ChatCompletionResponse struct {
	ID                string   `json:"id"`
	Object            string   `json:"object"` // "chat.completion" 注：固定值 "chat.completion"
	Created           int64    `json:"created"`
	Model             string   `json:"model"`
	Choices           []Choice `json:"choices"`
	Usage             *Usage   `json:"usage,omitempty"`
	SystemFingerprint string   `json:"system_fingerprint,omitempty"`
}

// Choice represents a single completion choice.
// Choice 表示单个 completion choice。
type Choice struct {
	Index        int          `json:"index"`
	Message      *ChatMessage `json:"message,omitempty"`       // For non-streaming 注：用于非 streaming
	Delta        *ChatDelta   `json:"delta,omitempty"`         // For streaming 注：用于 streaming
	FinishReason string       `json:"finish_reason,omitempty"` // stop, length, tool_calls, null 注：取值 stop、length、tool_calls、null
}

// ChatDelta represents incremental content in streaming responses.
// ChatDelta 表示 streaming 响应中的增量内容。
type ChatDelta struct {
	Role    string `json:"role,omitempty"`
	Content string `json:"content,omitempty"`
}

// Usage represents token usage statistics.
// Usage 表示 token 用量统计。
type Usage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

// ChatCompletionChunk represents a single streaming chunk.
// ChatCompletionChunk 表示单个 streaming chunk。
type ChatCompletionChunk struct {
	ID                string         `json:"id"`
	Object            string         `json:"object"` // "chat.completion.chunk" 注：固定值 "chat.completion.chunk"
	Created           int64          `json:"created"`
	Model             string         `json:"model"`
	Choices           []Choice       `json:"choices"`
	Usage             *Usage         `json:"usage,omitempty"` // Only in final chunk if requested 注：仅在请求时出现于最后一个 chunk
	SystemFingerprint string         `json:"system_fingerprint,omitempty"`
	ShannonEvents     []ShannonEvent `json:"shannon_events,omitempty"` // Shannon-specific agent events 注：Shannon 专有的 agent event
}

// ShannonEvent represents an agent lifecycle or progress event.
// These are Shannon-specific extensions to the OpenAI streaming format.
// ShannonEvent 表示 agent 生命周期或进度 event，是对 OpenAI streaming 格式的 Shannon 扩展。
type ShannonEvent struct {
	Type      string         `json:"type"`                // Event type (AGENT_STARTED, TOOL_INVOKED, etc.) 注：event 类型（AGENT_STARTED、TOOL_INVOKED 等）
	AgentID   string         `json:"agent_id,omitempty"`  // Agent identifier (e.g., "Ryogoku", "synthesis") 注：agent 标识（例如 "Ryogoku"、"synthesis"）
	Message   string         `json:"message,omitempty"`   // Human-readable message 注：人类可读的消息
	Timestamp int64          `json:"timestamp,omitempty"` // Unix timestamp 注：Unix 时间戳
	Payload   map[string]any `json:"payload,omitempty"`   // Additional event data 注：附加的 event 数据
}

// ModelObject represents a model in the /v1/models response.
// ModelObject 表示 /v1/models 响应中的模型。
type ModelObject struct {
	ID      string `json:"id"`
	Object  string `json:"object"` // "model" 注：固定值 "model"
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// ModelsResponse represents the /v1/models endpoint response.
// ModelsResponse 表示 /v1/models endpoint 的响应。
type ModelsResponse struct {
	Object string        `json:"object"` // "list" 注：固定值 "list"
	Data   []ModelObject `json:"data"`
}

// ErrorResponse represents an OpenAI-compatible error response.
// ErrorResponse 表示 OpenAI 兼容的错误响应。
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains the error information.
// ErrorDetail 包含错误信息。
type ErrorDetail struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Param   string `json:"param,omitempty"`
	Code    string `json:"code,omitempty"`
}

// NewErrorResponse creates a new error response.
// NewErrorResponse 创建新的错误响应。
func NewErrorResponse(message, errType, code string) *ErrorResponse {
	return &ErrorResponse{
		Error: ErrorDetail{
			Message: message,
			Type:    errType,
			Code:    code,
		},
	}
}

// Common error types matching OpenAI's error taxonomy.
// 与 OpenAI 错误分类一致的通用错误类型。
const (
	ErrorTypeInvalidRequest = "invalid_request_error"
	ErrorTypeAuthentication = "authentication_error"
	ErrorTypePermission     = "permission_error"
	ErrorTypeNotFound       = "not_found_error"
	ErrorTypeRateLimit      = "rate_limit_error"
	ErrorTypeServer         = "server_error"
)

// Common error codes.
// 通用错误码。
const (
	ErrorCodeInvalidRequest    = "invalid_request"
	ErrorCodeInvalidAPIKey     = "invalid_api_key"
	ErrorCodeModelNotFound     = "model_not_found"
	ErrorCodeRateLimitExceeded = "rate_limit_exceeded"
	ErrorCodeInsufficientQuota = "insufficient_quota"
	ErrorCodeInternalError     = "internal_error"
)

// GenerateCompletionID generates a unique completion ID.
// GenerateCompletionID 生成唯一的 completion ID。
func GenerateCompletionID() string {
	return "chatcmpl-" + generateID()
}

// generateID creates a unique ID string.
// generateID 创建唯一的 ID 字符串。
func generateID() string {
	// Use timestamp + random suffix for uniqueness
	// 使用时间戳 + 随机后缀保证唯一性。
	return time.Now().Format("20060102150405") + randomSuffix(8)
}

// randomSuffix generates a cryptographically secure random alphanumeric suffix.
// randomSuffix 生成密码学安全的随机字母数字后缀。
func randomSuffix(length int) string {
	bytes := make([]byte, (length+1)/2)
	if _, err := rand.Read(bytes); err != nil {
		// Fallback to timestamp-based (less secure but functional)
		// 回退到基于时间戳的方案（安全性较低但可用）。
		return fmt.Sprintf("%016x", time.Now().UnixNano())[:length]
	}
	return hex.EncodeToString(bytes)[:length]
}
