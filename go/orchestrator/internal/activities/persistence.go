package activities

import (
	"context"
	"database/sql"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
	"github.com/google/uuid"
	"go.uber.org/zap"
)

// PersistenceActivities handles database persistence for agent and tool executions
// PersistenceActivities 负责把 agent 与 tool 执行记录落到数据库。
type PersistenceActivities struct {
	dbClient *db.Client
	logger   *zap.Logger
}

// NewPersistenceActivities creates a new persistence activities instance
// NewPersistenceActivities 创建新的 persistence activities 实例
func NewPersistenceActivities(dbClient *db.Client, logger *zap.Logger) *PersistenceActivities {
	return &PersistenceActivities{
		dbClient: dbClient,
		logger:   logger,
	}
}

// PersistAgentExecutionInput contains data to persist for an agent execution
// PersistAgentExecutionInput 包含一次 agent 执行需要持久化的数据
type PersistAgentExecutionInput struct {
	ID         string         `json:"id,omitempty"` // Optional pre-generated ID for correlation with tool executions 注：可选，预生成的 ID，用于与 tool 执行关联
	WorkflowID string         `json:"workflow_id"`
	AgentID    string         `json:"agent_id"`
	Input      string         `json:"input"`
	Output     string         `json:"output"`
	State      string         `json:"state"`
	TokensUsed int            `json:"tokens_used"`
	ModelUsed  string         `json:"model_used"`
	DurationMs int64          `json:"duration_ms"`
	Error      string         `json:"error,omitempty"`
	Metadata   map[string]any `json:"metadata,omitempty"`
}

// PersistAgentExecutionStandalone is a standalone function that persists agent execution
// It uses the global dbClient if available
// PersistAgentExecutionStandalone 是独立函数，用于持久化 agent 执行记录；
// 有全局 dbClient 时就用它。
func PersistAgentExecutionStandalone(ctx context.Context, input PersistAgentExecutionInput) error {
	zap.L().Info("PersistAgentExecutionStandalone called", zap.String("workflow_id", input.WorkflowID))
	dbClient := GetGlobalDBClient()
	if dbClient == nil {
		// Log when no dbClient available
		// 没有 dbClient 时打日志
		zap.L().Warn("No global dbClient available, skipping agent execution persistence")
		return nil
	}
	zap.L().Info("Got dbClient, proceeding with persistence")

	activities := &PersistenceActivities{
		dbClient: dbClient,
		logger:   zap.L(),
	}
	return activities.PersistAgentExecution(ctx, input)
}

// PersistToolExecutionStandalone is a standalone function that persists tool execution
// It uses the global dbClient if available
// PersistToolExecutionStandalone 是独立函数，用于持久化 tool 执行记录；
// 有全局 dbClient 时就用它。
func PersistToolExecutionStandalone(ctx context.Context, input PersistToolExecutionInput) error {
	dbClient := GetGlobalDBClient()
	if dbClient == nil {
		// Log when no dbClient available
		// 没有 dbClient 时打日志
		zap.L().Warn("No global dbClient available, skipping tool execution persistence")
		return nil
	}

	activities := &PersistenceActivities{
		dbClient: dbClient,
		logger:   zap.L(),
	}
	return activities.PersistToolExecution(ctx, input)
}

// PersistAgentExecution persists an agent execution to the database
// PersistAgentExecution 把一条 agent 执行记录写入数据库。
func (p *PersistenceActivities) PersistAgentExecution(ctx context.Context, input PersistAgentExecutionInput) error {
	// Use zap.L() for logging since this might be called from goroutines
	// 这里可能从 goroutine 调用，因此用 zap.L() 记日志。
	logger := zap.L()
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	logger.Info("Persisting agent execution",
		zap.String("workflow_id", input.WorkflowID),
		zap.String("agent_id", input.AgentID),
	)

	// Ensure metadata map exists and enrich with session/user if available via DB
	// 确保 metadata map 存在；能通过数据库查到 session/user 时补充进去。
	if input.Metadata == nil {
		input.Metadata = make(map[string]any)
	}

	// Attempt to look up session_id and user_id via task_executions for this workflow
	// 尝试按 workflow 从 task_executions 反查 session_id 与 user_id。
	if p.dbClient != nil {
		if sqlDB := p.dbClient.GetDB(); sqlDB != nil {
			var sessID, userUUID sql.NullString
			_ = sqlDB.QueryRowContext(ctx,
				"SELECT session_id::text, user_id::text FROM task_executions WHERE workflow_id = $1 ORDER BY created_at DESC LIMIT 1",
				input.WorkflowID,
			).Scan(&sessID, &userUUID)
			if sessID.Valid && sessID.String != "" {
				if _, ok := input.Metadata["session_id"]; !ok {
					input.Metadata["session_id"] = sessID.String
				}
			}
			if userUUID.Valid && userUUID.String != "" {
				if _, ok := input.Metadata["user_id"]; !ok {
					input.Metadata["user_id"] = userUUID.String
				}
			}
		}
	}

	// Use pre-generated ID if provided, otherwise generate new one
	// 有预生成 ID 就用，否则新生成一个。
	agentID := input.ID
	if agentID == "" {
		agentID = uuid.New().String()
	}

	agentExec := &db.AgentExecution{
		ID:           agentID,
		WorkflowID:   input.WorkflowID,
		TaskID:       input.WorkflowID, // Use workflow ID as task ID 注：用 workflow ID 作为 task ID
// 直接用 workflow ID 作为 task ID。
		AgentID:      input.AgentID,
		Input:        input.Input,
		Output:       input.Output,
		State:        input.State,
		TokensUsed:   input.TokensUsed,
		ModelUsed:    input.ModelUsed,
		DurationMs:   input.DurationMs,
		ErrorMessage: input.Error,
		Metadata:     input.Metadata,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Queue the write
	// 把写入排入队列。
	err := p.dbClient.QueueWrite(db.WriteTypeAgentExecution, agentExec, func(writeErr error) {
		if writeErr != nil {
			p.logger.Error("Failed to persist agent execution",
				zap.String("workflow_id", input.WorkflowID),
				zap.String("agent_id", input.AgentID),
				zap.Error(writeErr),
			)
		} else {
			p.logger.Debug("Agent execution persisted",
				zap.String("workflow_id", input.WorkflowID),
				zap.String("agent_id", input.AgentID),
			)
		}
	})

	if err != nil {
		p.logger.Error("Failed to queue agent execution write",
			zap.String("workflow_id", input.WorkflowID),
			zap.String("agent_id", input.AgentID),
			zap.Error(err),
		)
		// Don't fail the activity - persistence is non-critical
		// 不让 activity 失败：持久化属于非关键路径。
	}

	return nil
}

// PersistToolExecutionInput contains data to persist for a tool execution
// PersistToolExecutionInput 包含一次 tool 执行需要持久化的数据
type PersistToolExecutionInput struct {
	WorkflowID       string         `json:"workflow_id"`
	AgentID          string         `json:"agent_id"`
	AgentExecutionID string         `json:"agent_execution_id,omitempty"` // Links to agent_executions.id 注：关联到 agent_executions.id
	ToolName         string         `json:"tool_name"`
	InputParams      map[string]any `json:"input_params"`
	Output           string         `json:"output"`
	Success          bool           `json:"success"`
	TokensConsumed   int            `json:"tokens_consumed"`
	DurationMs       int64          `json:"duration_ms"`
	Error            string         `json:"error,omitempty"`
	Metadata         map[string]any `json:"metadata,omitempty"`
}

// PersistToolExecution persists a tool execution to the database
// PersistToolExecution 把一条 tool 执行记录写入数据库。
func (p *PersistenceActivities) PersistToolExecution(ctx context.Context, input PersistToolExecutionInput) error {
	// Use zap.L() for logging since this might be called from goroutines
	// 这里可能从 goroutine 调用，因此用 zap.L() 记日志。
	logger := zap.L()
	if logger == nil {
		logger, _ = zap.NewProduction()
	}
	logger.Info("Persisting tool execution",
		zap.String("workflow_id", input.WorkflowID),
		zap.String("agent_id", input.AgentID),
		zap.String("tool_name", input.ToolName),
	)

	// Set AgentExecutionID if provided
	// 传了 AgentExecutionID 就设置上。
	var agentExecID *string
	if input.AgentExecutionID != "" {
		agentExecID = &input.AgentExecutionID
	}

	toolExec := &db.ToolExecution{
		ID:               uuid.New().String(),
		WorkflowID:       input.WorkflowID,
		AgentID:          input.AgentID,
		AgentExecutionID: agentExecID,
		ToolName:         input.ToolName,
		InputParams:      input.InputParams,
		Output:           input.Output,
		Success:          input.Success,
		TokensConsumed:   input.TokensConsumed,
		DurationMs:       input.DurationMs,
		Error:            input.Error,
		Metadata:         input.Metadata,
		CreatedAt:        time.Now(),
	}

	// Queue the write
	// 把写入排入队列。
	err := p.dbClient.QueueWrite(db.WriteTypeToolExecution, toolExec, func(writeErr error) {
		if writeErr != nil {
			p.logger.Error("Failed to persist tool execution",
				zap.String("workflow_id", input.WorkflowID),
				zap.String("agent_id", input.AgentID),
				zap.String("tool_name", input.ToolName),
				zap.Error(writeErr),
			)
		} else {
			p.logger.Debug("Tool execution persisted",
				zap.String("workflow_id", input.WorkflowID),
				zap.String("agent_id", input.AgentID),
				zap.String("tool_name", input.ToolName),
			)
		}
	})

	if err != nil {
		p.logger.Error("Failed to queue tool execution write",
			zap.String("workflow_id", input.WorkflowID),
			zap.String("agent_id", input.AgentID),
			zap.String("tool_name", input.ToolName),
			zap.Error(err),
		)
		// Don't fail the activity - persistence is non-critical
		// 不让 activity 失败：持久化属于非关键路径。
	}

	return nil
}
