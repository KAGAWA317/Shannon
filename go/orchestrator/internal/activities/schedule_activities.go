package activities

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/robfig/cron/v3"
	"go.temporal.io/sdk/client"
	"go.uber.org/zap"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
)

// ScheduleActivities holds dependencies for schedule-related activities
// ScheduleActivities 持有 schedule 相关 activity 的依赖。
type ScheduleActivities struct {
	DB             *sql.DB
	TemporalClient client.Client // optional, nil = skip Temporal schedule pause 注：可选；为 nil 时跳过 Temporal schedule 的 pause。
	Logger         *zap.Logger
}

// NewScheduleActivities creates a new ScheduleActivities instance
//NewScheduleActivities 创建一个 ScheduleActivities 实例
func NewScheduleActivities(sqlDB *sql.DB, temporalClient client.Client, logger *zap.Logger) *ScheduleActivities {
	return &ScheduleActivities{
		DB:             sqlDB,
		TemporalClient: temporalClient,
		Logger:         logger,
	}
}

// RecordScheduleExecutionInput is the input for starting execution tracking
// RecordScheduleExecutionInput 是开始记录一次 schedule 执行的入参。
type RecordScheduleExecutionInput struct {
	ScheduleID uuid.UUID
	TaskID     string // workflow_id of the child workflow 注：子 workflow 的 workflow_id
	Query      string // task query for display 注：用于展示的 task query
	UserID     string
	TenantID   string
}

// RecordScheduleExecutionStart logs the start of a scheduled execution
// and creates a task_executions record for unified task tracking
// RecordScheduleExecutionStart 记录一次定时执行的开始，
// 并创建 task_executions 记录，纳入统一 task 跟踪。
func (a *ScheduleActivities) RecordScheduleExecutionStart(ctx context.Context, input RecordScheduleExecutionInput) error {
	a.Logger.Debug("Recording schedule execution start",
		zap.String("schedule_id", input.ScheduleID.String()),
		zap.String("task_id", input.TaskID),
	)

	// Start transaction for atomic multi-table insert
	// 开启事务，保证多表写入的原子性。
	tx, err := a.DB.BeginTx(ctx, nil)
	if err != nil {
		a.Logger.Error("Failed to begin transaction", zap.Error(err))
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback() // Rollback if not committed 注：未 commit 时回滚。

	// 1. Create link record in scheduled_task_executions
	// 1. 在 scheduled_task_executions 建关联记录。
	_, err = tx.ExecContext(ctx, `
		INSERT INTO scheduled_task_executions (schedule_id, task_id, triggered_at)
		VALUES ($1, $2, NOW())
		ON CONFLICT (schedule_id, task_id) DO NOTHING
	`, input.ScheduleID, input.TaskID)
	if err != nil {
		a.Logger.Error("Failed to create scheduled_task_executions link", zap.Error(err))
		return fmt.Errorf("failed to create scheduled_task_executions link: %w", err)
	}

	// 2. Create task_executions record using shared persistence code
	// 2. 复用共享持久化逻辑创建 task_executions 记录。
	dbClient := GetGlobalDBClient()
	if dbClient == nil {
		a.Logger.Warn("Global DB client not available, skipping task_executions persistence")
		// Commit the link record even if task_executions fails
		// 即使 task_executions 写失败，也要提交这条关联记录。
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
		return nil
	}

	// Parse user/tenant IDs
	// 解析 user/tenant ID。
	var userID *uuid.UUID
	if input.UserID != "" {
		if uid, err := uuid.Parse(input.UserID); err == nil {
			userID = &uid
		}
	}

	// Use synthetic session ID for scheduled runs: "schedule:<scheduleID>:<taskID>"
	// Use full IDs to avoid collisions and panics from short IDs
	// 定时执行使用合成 session ID："schedule:<scheduleID>:<taskID>"。
	// 用完整 ID，避免短 ID 造成的冲突与 panic。
	sessionID := fmt.Sprintf("schedule:%s:%s", input.ScheduleID.String(), input.TaskID)

	task := &db.TaskExecution{
		WorkflowID:  input.TaskID,
		UserID:      userID,
		SessionID:   sessionID,
		Query:       input.Query,
		Status:      "RUNNING",
		StartedAt:   time.Now(),
		TriggerType: "schedule",
		ScheduleID:  &input.ScheduleID,
	}

	if err := dbClient.SaveTaskExecution(ctx, task); err != nil {
		a.Logger.Error("Failed to create task_executions record for scheduled run",
			zap.String("task_id", input.TaskID),
			zap.Error(err))
		// Rollback transaction on task_executions failure
		// task_executions 写失败时回滚事务。
		return fmt.Errorf("failed to create task_executions record: %w", err)
	}

	// Commit transaction
	// 提交事务。
	if err := tx.Commit(); err != nil {
		a.Logger.Error("Failed to commit transaction", zap.Error(err))
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	a.Logger.Info("Scheduled execution started and task_executions record created",
		zap.String("schedule_id", input.ScheduleID.String()),
		zap.String("task_id", input.TaskID),
		zap.String("trigger_type", "schedule"),
	)

	return nil
}

// RecordScheduleExecutionCompleteInput is the input for completing execution tracking
// RecordScheduleExecutionCompleteInput 是收尾记录一次 schedule 执行的入参。
type RecordScheduleExecutionCompleteInput struct {
	ScheduleID uuid.UUID
	TaskID     string
	Status     string // COMPLETED, FAILED, CANCELLED 注：COMPLETED、FAILED、CANCELLED
	TotalCost  float64
	ErrorMsg   string
	Result     string // optional result text 注：可选的结果文本

	// Metadata from child workflow (for unified task_executions consistency)
	// 来自子 workflow 的 metadata，用于保持 task_executions 口径一致。
	ModelUsed        string
	Provider         string
	TotalTokens      int
	PromptTokens     int
	CompletionTokens int

	// Full metadata from child workflow result (screenshot_urls, pipeline, etc.)
	// 子 workflow 结果里的完整 metadata（screenshot_urls、pipeline 等）。
	ResultMetadata map[string]any
}

// RecordScheduleExecutionComplete logs the completion of a scheduled execution
// and updates the task_executions record
// RecordScheduleExecutionComplete 记录一次定时执行的结束，
// 并更新对应的 task_executions 记录。
func (a *ScheduleActivities) RecordScheduleExecutionComplete(ctx context.Context, input RecordScheduleExecutionCompleteInput) error {
	a.Logger.Debug("Recording schedule execution completion",
		zap.String("schedule_id", input.ScheduleID.String()),
		zap.String("task_id", input.TaskID),
		zap.String("status", input.Status),
		zap.Float64("cost", input.TotalCost),
	)

	// 1. Update task_executions via shared persistence code
	// 1. 复用共享持久化逻辑更新 task_executions。
	dbClient := GetGlobalDBClient()
	if dbClient != nil {
		now := time.Now()
		var result *string
		if input.Result != "" {
			result = &input.Result
		}
		var errorMsg *string
		if input.ErrorMsg != "" {
			errorMsg = &input.ErrorMsg
		}

		// Get existing task to preserve fields, then update
		// 先取出现有 task 以保留其余字段，再更新。
		existingTask, err := dbClient.GetTaskExecution(ctx, input.TaskID)
		if err != nil {
			a.Logger.Warn("Failed to get existing task_executions record",
				zap.String("task_id", input.TaskID),
				zap.Error(err))
		}

		if existingTask != nil {
			// Update existing record with completion info
			// 用完成信息更新现有记录。
			existingTask.Status = input.Status
			existingTask.CompletedAt = &now
			existingTask.TotalCostUSD = input.TotalCost
			if result != nil {
				existingTask.Result = result
			}
			if errorMsg != nil {
				existingTask.ErrorMessage = errorMsg
			}

			// Populate metadata from child workflow result (Option A: unified model)
			// 用子 workflow 结果填充 metadata（Option A：统一模型）。
			if input.ModelUsed != "" {
				existingTask.ModelUsed = input.ModelUsed
			}
			if input.Provider != "" {
				existingTask.Provider = input.Provider
			}
			if input.TotalTokens > 0 {
				existingTask.TotalTokens = input.TotalTokens
			}
			if input.PromptTokens > 0 {
				existingTask.PromptTokens = input.PromptTokens
			}
			if input.CompletionTokens > 0 {
				existingTask.CompletionTokens = input.CompletionTokens
			}

			// Write full metadata from child workflow result (screenshot_urls, pipeline, etc.)
			// 写入子 workflow 结果里的完整 metadata（screenshot_urls、pipeline 等）。
			if len(input.ResultMetadata) > 0 {
				existingTask.Metadata = db.JSONB(input.ResultMetadata)
			}

			// Calculate duration if we have start time
			// 有开始时间就计算耗时。
			if !existingTask.StartedAt.IsZero() {
				durationMs := int(now.Sub(existingTask.StartedAt).Milliseconds())
				existingTask.DurationMs = &durationMs
			}

			if err := dbClient.SaveTaskExecution(ctx, existingTask); err != nil {
				a.Logger.Warn("Failed to update task_executions record",
					zap.String("task_id", input.TaskID),
					zap.Error(err))
			} else {
				a.Logger.Debug("Updated task_executions record with completion status and metadata",
					zap.String("task_id", input.TaskID),
					zap.String("status", input.Status),
					zap.String("model", input.ModelUsed),
					zap.String("provider", input.Provider),
					zap.Int("total_tokens", input.TotalTokens))
			}
		}
	}

	// 2. Update schedule statistics with atomic SQL (prevents race conditions)
	// 2. 用原子 SQL 更新 schedule 统计，避免竞态。
	if input.Status == "COMPLETED" {
		_, err := a.DB.ExecContext(ctx, `
			UPDATE scheduled_tasks
			SET total_runs = total_runs + 1,
				successful_runs = successful_runs + 1,
				last_run_at = NOW(),
				updated_at = NOW()
			WHERE id = $1
		`, input.ScheduleID)
		if err != nil {
			a.Logger.Error("Failed to update schedule statistics",
				zap.String("schedule_id", input.ScheduleID.String()),
				zap.Error(err))
		}
	} else if input.Status == "FAILED" {
		_, err := a.DB.ExecContext(ctx, `
			UPDATE scheduled_tasks
			SET total_runs = total_runs + 1,
				failed_runs = failed_runs + 1,
				last_run_at = NOW(),
				updated_at = NOW()
			WHERE id = $1
		`, input.ScheduleID)
		if err != nil {
			a.Logger.Error("Failed to update schedule statistics",
				zap.String("schedule_id", input.ScheduleID.String()),
				zap.Error(err))
		}
	}

	// 3. Update next_run_at
	// 3. 更新 next_run_at。
	a.updateNextRunAt(ctx, input.ScheduleID)

	a.Logger.Info("Scheduled execution completed",
		zap.String("schedule_id", input.ScheduleID.String()),
		zap.String("task_id", input.TaskID),
		zap.String("status", input.Status),
		zap.Float64("cost", input.TotalCost),
	)

	return nil
}

// PauseScheduleForQuotaInput is the input for PauseScheduleForQuota activity.
// PauseScheduleForQuotaInput 是 PauseScheduleForQuota activity 的入参。
type PauseScheduleForQuotaInput struct {
	ScheduleID uuid.UUID
	Reason     string
}

// PauseScheduleForQuota pauses a Temporal schedule and updates DB status.
// Used by ScheduledTaskWorkflow to auto-pause free-tier schedules on quota exceeded.
// PauseScheduleForQuota 暂停 Temporal schedule 并更新 DB 状态；
// 由 ScheduledTaskWorkflow 在免费额度超限时自动暂停 schedule。
func (a *ScheduleActivities) PauseScheduleForQuota(ctx context.Context, input PauseScheduleForQuotaInput) error {
	reason := input.Reason
	if reason == "" {
		reason = "Auto-paused: quota exceeded"
	}

	// 1. Get temporal_schedule_id from DB
	// 1. 从 DB 取 temporal_schedule_id。
	var temporalScheduleID, currentStatus string
	err := a.DB.QueryRowContext(ctx, `
		SELECT temporal_schedule_id, status FROM scheduled_tasks WHERE id = $1
	`, input.ScheduleID).Scan(&temporalScheduleID, &currentStatus)
	if err != nil {
		a.Logger.Error("Failed to get schedule for auto-pause",
			zap.String("schedule_id", input.ScheduleID.String()),
			zap.Error(err))
		return nil // non-fatal 注：非致命
	}

	if currentStatus == "PAUSED" {
		a.Logger.Debug("Schedule already paused, skipping",
			zap.String("schedule_id", input.ScheduleID.String()))
		return nil
	}

	// 2. Pause in Temporal (if client available)
	// Must succeed before updating DB to prevent desync (DB=PAUSED but Temporal still fires).
	// 2. 在 Temporal 侧暂停（有 client 时）。
	// 必须先成功再改 DB，否则会不同步：DB 显示 PAUSED，但 Temporal 仍在触发。
	if a.TemporalClient != nil && temporalScheduleID != "" {
		handle := a.TemporalClient.ScheduleClient().GetHandle(ctx, temporalScheduleID)
		if err := handle.Pause(ctx, client.SchedulePauseOptions{
			Note: reason,
		}); err != nil {
			a.Logger.Error("Failed to pause Temporal schedule, skipping DB update to avoid desync",
				zap.String("schedule_id", input.ScheduleID.String()),
				zap.String("temporal_id", temporalScheduleID),
				zap.Error(err))
			return nil // non-fatal, next execution will retry pause 注：非致命：下次执行会 retry pause。
		}
	}

	// 3. Update DB status (only if Temporal pause succeeded)
	// 3. 更新 DB 状态，仅在 Temporal 暂停成功后执行。
	_, err = a.DB.ExecContext(ctx, `
		UPDATE scheduled_tasks SET status = 'PAUSED', pause_reason = 'quota_exceeded', updated_at = NOW() WHERE id = $1
	`, input.ScheduleID)
	if err != nil {
		a.Logger.Error("Failed to update schedule status to PAUSED",
			zap.String("schedule_id", input.ScheduleID.String()),
			zap.Error(err))
		return nil // non-fatal — Temporal is paused, DB will catch up on next query 注：非致命：Temporal 已暂停，DB 会在下次查询时对齐。
	}

	a.Logger.Info("Auto-paused schedule due to quota exceeded",
		zap.String("schedule_id", input.ScheduleID.String()),
		zap.String("reason", reason))

	return nil
}

// updateNextRunAt calculates and updates the next run time for a schedule.
// NOTE: This is a best-effort local calculation. The authoritative next_run_at
// is maintained by Temporal. Activities cannot access ScheduleClient.Describe()
// directly. For accurate next_run_at after schedule changes (cron/timezone),
// see manager.go which reads from Temporal after updates.
// updateNextRunAt 计算并更新 schedule 的下次运行时间。
// 注意：这只是本地尽力而为的估算，权威的 next_run_at 由 Temporal 维护；
// activity 里无法直接调 ScheduleClient.Describe()。
// 若要在 schedule 变更（cron/timezone）后拿到准确的 next_run_at，
// 参见 manager.go——它在更新后从 Temporal 读取。
func (a *ScheduleActivities) updateNextRunAt(ctx context.Context, scheduleID uuid.UUID) {
	// Fetch schedule's cron expression and timezone
	// 取该 schedule 的 cron 表达式与时区。
	var cronExpr, timezone string
	err := a.DB.QueryRowContext(ctx, `
		SELECT cron_expression, timezone
		FROM scheduled_tasks
		WHERE id = $1 AND status = 'ACTIVE'
	`, scheduleID).Scan(&cronExpr, &timezone)

	if err != nil {
		a.Logger.Warn("Failed to fetch schedule for next_run_at update",
			zap.String("schedule_id", scheduleID.String()),
			zap.Error(err))
		return
	}

	// Parse cron expression
	// 解析 cron 表达式。
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	schedule, err := parser.Parse(cronExpr)
	if err != nil {
		a.Logger.Error("Failed to parse cron expression",
			zap.String("schedule_id", scheduleID.String()),
			zap.String("cron", cronExpr),
			zap.Error(err))
		return
	}

	// Load timezone
	// 加载时区。
	loc, err := time.LoadLocation(timezone)
	if err != nil {
		a.Logger.Warn("Invalid timezone, using UTC",
			zap.String("schedule_id", scheduleID.String()),
			zap.String("timezone", timezone))
		loc = time.UTC
	}

	// Calculate next run time
	// 计算下次运行时间。
	nextRun := schedule.Next(time.Now().In(loc))

	// Update database
	// 写入数据库。
	_, err = a.DB.ExecContext(ctx, `
		UPDATE scheduled_tasks
		SET next_run_at = $1,
			updated_at = NOW()
		WHERE id = $2
	`, nextRun, scheduleID)

	if err != nil {
		a.Logger.Error("Failed to update next_run_at",
			zap.String("schedule_id", scheduleID.String()),
			zap.Error(err))
	} else {
		a.Logger.Debug("Updated next_run_at",
			zap.String("schedule_id", scheduleID.String()),
			zap.Time("next_run", nextRun))
	}
}
