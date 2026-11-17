package db

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// JSONB represents a PostgreSQL jsonb column
// JSONB 表示 PostgreSQL 的 jsonb 列
type JSONB map[string]any

// ErrConcurrentAccess is returned when JSONB cloning fails due to concurrent map access
// JSONB 克隆因并发 map 访问失败时返回 ErrConcurrentAccess
var ErrConcurrentAccess = fmt.Errorf("concurrent map access during JSONB clone")

// SafeClone creates a deep copy of JSONB via JSON serialization.
// Uses recover() to handle concurrent map access panics gracefully.
//
// WARNING: Returns empty JSONB{} on concurrent access or errors.
// Use TryClone() if you need to detect and handle failures.
// SafeClone 通过 JSON 序列化深拷贝 JSONB。
// 用 recover() 优雅处理并发 map 访问 panic。
// WARNING: 并发访问或出错时返回空的 JSONB{}。
// 需要检测并处理失败时请用 TryClone()。
func (j JSONB) SafeClone() (result JSONB) {
	clone, _ := j.TryClone()
	return clone
}

// TryClone creates a deep copy of JSONB via JSON serialization.
// Returns an error if cloning fails due to concurrent access or serialization issues.
// This is the preferred method when you need to handle clone failures explicitly.
// TryClone 通过 JSON 序列化深拷贝 JSONB。
// 因并发访问或序列化问题导致克隆失败时返回错误。
// 需要显式处理克隆失败时应优先使用该方法。
func (j JSONB) TryClone() (result JSONB, err error) {
	if j == nil {
		return nil, nil
	}

	// Recover from panic caused by concurrent map iteration
	// 从并发 map 迭代引发的 panic 中恢复
	defer func() {
		if r := recover(); r != nil {
			result = JSONB{}
			err = fmt.Errorf("%w: %v", ErrConcurrentAccess, r)
		}
	}()

	// Use JSON round-trip for deep clone
	// 用 JSON 往返实现深拷贝
	data, marshalErr := json.Marshal(j)
	if marshalErr != nil {
		return JSONB{}, fmt.Errorf("JSONB marshal failed: %w", marshalErr)
	}
	var clone JSONB
	if unmarshalErr := json.Unmarshal(data, &clone); unmarshalErr != nil {
		return JSONB{}, fmt.Errorf("JSONB unmarshal failed: %w", unmarshalErr)
	}
	return clone, nil
}

// Value implements the driver.Valuer interface
// Value 实现 driver.Valuer 接口
func (j JSONB) Value() (driver.Value, error) {
	if j == nil {
		return nil, nil
	}
	return json.Marshal(j)
}

// Scan implements the sql.Scanner interface
// Scan 实现 sql.Scanner 接口
func (j *JSONB) Scan(value any) error {
	if value == nil {
		*j = nil
		return nil
	}

	bytes, ok := value.([]byte)
	if !ok {
		return fmt.Errorf("cannot scan %T into JSONB", value)
	}

	return json.Unmarshal(bytes, j)
}

// TaskExecution represents a workflow/task execution record
// TaskExecution 表示一次 workflow/task 执行记录
type TaskExecution struct {
	ID          uuid.UUID  `db:"id"`
	WorkflowID  string     `db:"workflow_id"`
	UserID      *uuid.UUID `db:"user_id"`
	SessionID   string     `db:"session_id"`
	TenantID    *uuid.UUID `db:"tenant_id"`
	Query       string     `db:"query"`
	Mode        string     `db:"mode"`
	Status      string     `db:"status"`
	StartedAt   time.Time  `db:"started_at"`
	CompletedAt *time.Time `db:"completed_at"`

	// Results
	// 结果
	Result       *string `db:"result"`
	ErrorMessage *string `db:"error_message"`

	// Model information
	// 模型信息
	ModelUsed string `db:"model_used"`
	Provider  string `db:"provider"`

	// Token metrics
	// token 指标
	TotalTokens      int     `db:"total_tokens"`
	PromptTokens     int     `db:"prompt_tokens"`
	CompletionTokens int     `db:"completion_tokens"`
	TotalCostUSD     float64 `db:"total_cost_usd"`

	// Performance metrics
	// 性能指标
	DurationMs      *int    `db:"duration_ms"`
	AgentsUsed      int     `db:"agents_used"`
	ToolsInvoked    int     `db:"tools_invoked"`
	CacheHits       int     `db:"cache_hits"`
	ComplexityScore float64 `db:"complexity_score"`

	// Prompt cache metrics (Anthropic cache read/creation tokens)
	// prompt cache 指标（Anthropic cache read/creation token）
	CacheReadTokens     int `db:"cache_read_tokens"`
	CacheCreationTokens int `db:"cache_creation_tokens"`

	// Cache-aware rollup: prompt + completion + cache_read + cache_creation.
	// Parallel to TotalTokens (= prompt + completion); used by quota tracking
	// where prompt-cache cost must count, while keeping TotalTokens
	// OpenAI-compatible. See migration 121.
	// cache 感知汇总：prompt + completion + cache_read + cache_creation。
	// 与 TotalTokens（= prompt + completion）并列；用于必须计入 prompt cache 成本的配额统计，
	// 同时保持 TotalTokens 与 OpenAI 兼容。见迁移 121。
	CacheAwareTotalTokens int `db:"cache_aware_total_tokens"`

	// Structured response (unified API format, stored in response JSONB column)
	// 结构化 response（统一 API 格式，存入 response JSONB 列）
	Response JSONB `db:"response"`

	// Metadata
	// 元数据
	Metadata  JSONB     `db:"metadata"`
	CreatedAt time.Time `db:"created_at"`

	// Trigger information (unified execution model)
	// 触发信息（统一执行模型）
	TriggerType string     `db:"trigger_type"` // 'api', 'schedule' 注：取值为 api、schedule
	ScheduleID  *uuid.UUID `db:"schedule_id"`  // FK to scheduled_tasks (NULL for API-triggered) 注：指向 scheduled_tasks 的外键（API 触发时为 NULL）
}

// AgentExecution represents an individual agent execution
// AgentExecution 表示单次 agent 执行
type AgentExecution struct {
	ID         string `db:"id"`
	WorkflowID string `db:"workflow_id"` // References task_executions.workflow_id 注：引用 task_executions.workflow_id
	TaskID     string `db:"task_id"`     // Optional reference to task_executions.id 注：可选地引用 task_executions.id
	AgentID    string `db:"agent_id"`

	// Execution details
	// 执行详情
	Input        string `db:"input"`
	Output       string `db:"output"`
	State        string `db:"state"`
	ErrorMessage string `db:"error_message"`

	// Token usage
	// token 用量
	TokensUsed int    `db:"tokens_used"`
	ModelUsed  string `db:"model_used"`

	// Performance
	// 性能
	DurationMs int64 `db:"duration_ms"`

	// Metadata
	// 元数据
	Metadata  JSONB     `db:"metadata"`
	CreatedAt time.Time `db:"created_at"`
	UpdatedAt time.Time `db:"updated_at"`
}

// ToolExecution represents a tool execution record
// ToolExecution 表示一条 tool 执行记录
type ToolExecution struct {
	ID               string  `db:"id"`
	WorkflowID       string  `db:"workflow_id"`        // References task_executions.workflow_id 注：引用 task_executions.workflow_id
	AgentID          string  `db:"agent_id"`           // Agent that executed the tool 注：执行该工具的 agent
	AgentExecutionID *string `db:"agent_execution_id"` // Optional reference to agent_executions.id 注：可选地引用 agent_executions.id

	ToolName string `db:"tool_name"`

	// Execution details
	// 执行详情
	InputParams JSONB  `db:"input_params"`
	Output      string `db:"output"`
	Success     bool   `db:"success"`
	Error       string `db:"error"`

	// Performance
	// 性能
	DurationMs     int64 `db:"duration_ms"`
	TokensConsumed int   `db:"tokens_consumed"`

	// Metadata
	// 元数据
	Metadata  JSONB     `db:"metadata"`
	CreatedAt time.Time `db:"created_at"`
}

// SessionArchive represents a snapshot of a Redis session
// SessionArchive 表示 Redis session 的快照
type SessionArchive struct {
	ID        uuid.UUID  `db:"id"`
	SessionID string     `db:"session_id"`
	UserID    *uuid.UUID `db:"user_id"`

	// Snapshot data
	// 快照数据
	SnapshotData JSONB   `db:"snapshot_data"`
	MessageCount int     `db:"message_count"`
	TotalTokens  int     `db:"total_tokens"`
	TotalCostUSD float64 `db:"total_cost_usd"`

	// Timing
	// 时间信息
	SessionStartedAt time.Time  `db:"session_started_at"`
	SnapshotTakenAt  time.Time  `db:"snapshot_taken_at"`
	TTLExpiresAt     *time.Time `db:"ttl_expires_at"`
}

// UsageDailyAggregate represents daily usage statistics
// UsageDailyAggregate 表示按天的用量统计
type UsageDailyAggregate struct {
	ID     uuid.UUID  `db:"id"`
	UserID *uuid.UUID `db:"user_id"`
	Date   time.Time  `db:"date"`

	// Aggregated metrics
	// 聚合指标
	TotalTasks      int `db:"total_tasks"`
	SuccessfulTasks int `db:"successful_tasks"`
	FailedTasks     int `db:"failed_tasks"`

	// Token usage
	// token 用量
	TotalTokens  int     `db:"total_tokens"`
	TotalCostUSD float64 `db:"total_cost_usd"`

	// Model distribution
	// 模型分布
	ModelUsage JSONB `db:"model_usage"`

	// Tool usage
	// tool 使用情况
	ToolsInvoked     int   `db:"tools_invoked"`
	ToolDistribution JSONB `db:"tool_distribution"`

	// Performance
	// 性能
	AvgDurationMs int     `db:"avg_duration_ms"`
	CacheHitRate  float64 `db:"cache_hit_rate"`

	CreatedAt time.Time `db:"created_at"`
}

// AuditLog represents an audit log entry
// AuditLog 表示一条审计日志
type AuditLog struct {
	ID         uuid.UUID  `db:"id"`
	UserID     *uuid.UUID `db:"user_id"`
	Action     string     `db:"action"`
	EntityType string     `db:"entity_type"`
	EntityID   string     `db:"entity_id"`

	// Audit details
	// 审计详情
	IPAddress string `db:"ip_address"`
	UserAgent string `db:"user_agent"`
	RequestID string `db:"request_id"`

	// Changes
	// 变更内容
	OldValue JSONB `db:"old_value"`
	NewValue JSONB `db:"new_value"`

	CreatedAt time.Time `db:"created_at"`
}

// TaskExecutionFilter provides filtering options for task queries
// TaskExecutionFilter 提供 task 查询的过滤条件
type TaskExecutionFilter struct {
	UserID    *uuid.UUID
	SessionID *string
	Status    *string
	Mode      *string
	StartTime *time.Time
	EndTime   *time.Time
	Limit     int
	Offset    int
}

// AggregateStats represents aggregated statistics
// AggregateStats 表示聚合统计数据
type AggregateStats struct {
	Period       string  `db:"period"`
	TotalTasks   int     `db:"total_tasks"`
	TotalTokens  int     `db:"total_tokens"`
	TotalCost    float64 `db:"total_cost"`
	AvgDuration  int     `db:"avg_duration"`
	SuccessRate  float64 `db:"success_rate"`
	CacheHitRate float64 `db:"cache_hit_rate"`
	TopModels    JSONB   `db:"top_models"`
	TopTools     JSONB   `db:"top_tools"`
}
