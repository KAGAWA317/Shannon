package session

import (
	"errors"
	"slices"
	"time"
)

var (
	// ErrSessionNotFound is returned when a session doesn't exist
	// session 不存在时返回 ErrSessionNotFound
	ErrSessionNotFound = errors.New("session not found")

	// ErrSessionExpired is returned when a session has expired
	// session 已过期时返回 ErrSessionExpired
	ErrSessionExpired = errors.New("session expired")

	// ErrInvalidSession is returned when session data is invalid
	// session 数据非法时返回 ErrInvalidSession
	ErrInvalidSession = errors.New("invalid session")
)

// Session represents a user session with context continuity
// Session 表示具有 context 连续性的用户 session
type Session struct {
	ID        string         `json:"id"`
	UserID    string         `json:"user_id"`
	TenantID  string         `json:"tenant_id"`
	CreatedAt time.Time      `json:"created_at"`
	UpdatedAt time.Time      `json:"updated_at"`
	ExpiresAt time.Time      `json:"expires_at"`
	Metadata  map[string]any `json:"metadata"`
	Context   map[string]any `json:"context"`
	History   []Message      `json:"history"`

	// Agent-specific context
	// agent 专属 context
	AgentStates map[string]*AgentState `json:"agent_states,omitempty"`

	// Token tracking
	// token 统计
	TotalTokensUsed int     `json:"total_tokens_used"`
	TotalCostUSD    float64 `json:"total_cost_usd"`
}

// Message represents a message in the session history
// Message 表示 session 历史中的一条消息
type Message struct {
	ID        string         `json:"id"`
	Role      string         `json:"role"` // "user", "assistant", "system" 注："user"、"assistant"、"system"
	Content   string         `json:"content"`
	Timestamp time.Time      `json:"timestamp"`
	Metadata  map[string]any `json:"metadata,omitempty"`

	// Token tracking for this message
	// 本条 message 的 token 统计
	TokensUsed int     `json:"tokens_used,omitempty"`
	CostUSD    float64 `json:"cost_usd,omitempty"`
}

// AgentState represents the state of a specific agent in the session
// AgentState 表示 session 中某个 agent 的状态
type AgentState struct {
	AgentID    string         `json:"agent_id"`
	LastActive time.Time      `json:"last_active"`
	State      string         `json:"state"`
	Memory     map[string]any `json:"memory"`
	ToolsUsed  []string       `json:"tools_used"`
	TokensUsed int            `json:"tokens_used"`
}

// IsExpired checks if the session has expired
// IsExpired 判断 session 是否已过期
func (s *Session) IsExpired() bool {
	return time.Now().After(s.ExpiresAt)
}

// GetContextValue retrieves a value from the session context
// GetContextValue 从 session context 中取值
func (s *Session) GetContextValue(key string) (any, bool) {
	if s.Context == nil {
		return nil, false
	}
	val, ok := s.Context[key]
	return val, ok
}

// SetContextValue sets a value in the session context
// SetContextValue 在 session context 中设值
func (s *Session) SetContextValue(key string, value any) {
	if s.Context == nil {
		s.Context = make(map[string]any)
	}
	s.Context[key] = value
	s.UpdatedAt = time.Now()
}

// GetAgentState retrieves the state for a specific agent
// GetAgentState 获取某个 agent 的状态
func (s *Session) GetAgentState(agentID string) (*AgentState, bool) {
	if s.AgentStates == nil {
		return nil, false
	}
	state, ok := s.AgentStates[agentID]
	return state, ok
}

// SetAgentState sets the state for a specific agent
// SetAgentState 设置某个 agent 的状态
func (s *Session) SetAgentState(agentID string, state *AgentState) {
	if s.AgentStates == nil {
		s.AgentStates = make(map[string]*AgentState)
	}
	s.AgentStates[agentID] = state
	s.UpdatedAt = time.Now()
}

// GetRecentHistory returns the most recent messages from history
// GetRecentHistory 返回历史中最近的消息
func (s *Session) GetRecentHistory(count int) []Message {
	if len(s.History) <= count {
		return s.History
	}
	return s.History[len(s.History)-count:]
}

// GetHistorySummary returns a summary suitable for LLM context
// GetHistorySummary 返回适合 LLM context 的摘要
func (s *Session) GetHistorySummary(maxTokens int) string {
	// Simple implementation - in production, use proper summarization
	// 简单实现——生产环境应使用正规的摘要生成
	summary := ""
	currentTokens := 0

	// Start from most recent messages
	// 从最近的消息开始
	for _, msg := range slices.Backward(s.History) {

		// Rough token estimate: 1 token per 4 characters
		// 粗略的 token 估算：每 4 个字符 1 token
		msgTokens := len(msg.Content) / 4

		if currentTokens+msgTokens > maxTokens {
			break
		}

		// Prepend to maintain chronological order
		// 前插以保持时间顺序
		summary = formatMessage(msg) + "\n" + summary
		currentTokens += msgTokens
	}

	return summary
}

func formatMessage(msg Message) string {
	return msg.Role + ": " + msg.Content
}

// UpdateTokenUsage updates the token usage for the session
// UpdateTokenUsage 更新 session 的 token 用量
func (s *Session) UpdateTokenUsage(tokens int, cost float64) {
	s.TotalTokensUsed += tokens
	s.TotalCostUSD += cost
	s.UpdatedAt = time.Now()
}
