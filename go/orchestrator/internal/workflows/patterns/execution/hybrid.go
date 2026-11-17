package execution

import (
	"fmt"
	"maps"
	"time"

	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
)

// HybridConfig controls hybrid parallel/sequential execution with dependencies
// HybridConfig 控制带依赖的 hybrid 并行/串行执行
type HybridConfig struct {
	MaxConcurrency           int                                        // Maximum concurrent agents 注：最大并发 agent 数
	EmitEvents               bool                                       // Whether to emit streaming events 注：是否发送 streaming 事件
	Context                  map[string]any                             // Base context for all agents 注：所有 agent 共用的基础 context
	DependencyWaitTimeout    time.Duration                              // Max time to wait for dependencies 注：等待依赖完成的最长时间
	DependencyCheckInterval  time.Duration                              // Interval between dependency checks (default 30s) 注：依赖检查间隔（默认 30s）
	PassDependencyResults    bool                                       // Pass dependency results to dependent tasks 注：把依赖结果传给依赖它的 task
	ClearDependentToolParams bool                                       // Clear tool params for dependent tasks 注：清空下游 task 的 tool 参数
	PrefilledResults         map[string]activities.AgentExecutionResult // Pre-populated results from external sources (child workflows, previous phases) 注：来自外部来源的预填结果（子 workflow、之前的阶段）
}

// HybridTask represents a task with dependencies
// HybridTask 表示一个带依赖的 task
type HybridTask struct {
	ID             string
	Description    string
	SuggestedTools []string
	ToolParameters map[string]any
	PersonaID      string
	Role           string
	ParentArea     string
	Dependencies   []string // IDs of tasks this depends on 注：该 task 依赖的 task ID
	// Optional per-task context overrides (e.g., task contracts)
	// 可选的单 task context 覆盖（例如 task contract）
	ContextOverrides map[string]any
}

// HybridResult contains results from hybrid execution
// HybridResult 包含 hybrid 执行的结果
type HybridResult struct {
	Results     map[string]activities.AgentExecutionResult // Keyed by task ID 注：以 task ID 为 key
	TotalTokens int
	Metadata    map[string]any
}

// ExecuteHybrid runs tasks with dependency management.
// Tasks without dependencies run in parallel up to MaxConcurrency.
// Tasks with dependencies wait for their dependencies to complete first.
//
// ## Concurrency Model
//
// This function uses workflow.Go to launch concurrent coroutines. However, Temporal
// workflows execute in a **single-threaded, deterministic** event loop. The "concurrency"
// is cooperative, not preemptive:
//
//   - All workflow.Go coroutines run on the same thread
//   - Coroutines yield control at blocking operations (channel reads, activity calls, workflow.Sleep)
//   - Shared state (completedTasks, taskResults) is safe to access without mutexes
//   - The workflow scheduler ensures deterministic replay from event history
//
// This means:
//   - No race conditions between goroutines accessing shared maps
//   - No need for sync.Mutex on completedTasks or taskResults
//   - Execution is fully deterministic and replayable
//
// For more details, see: https://docs.temporal.io/develop/go/foundations#workflows
// 无依赖的 task 并行执行（不超过 MaxConcurrency），有依赖的 task 等依赖完成后再跑。
// 并发模型：workflow.Go 启动的协程运行在 Temporal 单线程确定性 event 循环中，"并发"是协作式的：
// 所有协程同线程运行，仅在阻塞操作（channel 读、activity 调用、workflow.Sleep）处让出控制权；
// 因此共享状态（completedTasks、taskResults）无需加锁即可安全访问，不存在竞态，执行完全确定、可重放。
func ExecuteHybrid(
	ctx workflow.Context,
	tasks []HybridTask,
	sessionID string,
	history []string,
	config HybridConfig,
	budgetPerAgent int,
	userID string,
	modelTier string,
) (*HybridResult, error) {

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting hybrid execution",
		"task_count", len(tasks),
		"max_concurrency", config.MaxConcurrency,
	)

	// Create channels for coordination
	// 创建用于协调的 channel
	semaphore := workflow.NewSemaphore(ctx, int64(config.MaxConcurrency))
	resultsChan := workflow.NewChannel(ctx)

	// Track task completion status (safe without mutex due to Temporal's single-threaded model)
	// Temporal 单线程模型下，不加互斥锁访问共享状态也是安全的。
	completedTasks := make(map[string]bool)
	taskResults := make(map[string]activities.AgentExecutionResult)
	totalTokens := 0

	// Merge prefilled results from external sources (child workflows, previous execution phases)
	// This enables cross-phase dependency resolution
	// 合并外部预填结果（子 workflow、前一阶段），从而支持跨阶段依赖解析。
	if len(config.PrefilledResults) > 0 {
		for id, result := range config.PrefilledResults {
			taskResults[id] = result
			completedTasks[id] = true
		}
		logger.Info("Prefilled dependency results from external sources",
			"prefilled_count", len(config.PrefilledResults),
		)
	}

	// Build task index for quick lookup
	// 构建 task 索引以便快速查找
	taskIndex := make(map[string]*HybridTask)
	for i := range tasks {
		taskIndex[tasks[i].ID] = &tasks[i]
	}

	// Launch task executor for each task
	// 为每个 task 启动执行协程
	for i := range tasks {
		task := tasks[i]
		idx := i // Capture index for closure 注：捕获索引供闭包使用
		workflow.Go(ctx, func(ctx workflow.Context) {
			executeHybridTask(
				ctx,
				task,
				idx,
				taskIndex,
				completedTasks,
				taskResults,
				semaphore,
				resultsChan,
				sessionID,
				history,
				config,
				budgetPerAgent,
				userID,
				modelTier,
			)
		})
	}

	// Collect results
	// 收集结果
	successCount := 0
	errorCount := 0

	for range tasks {
		var result taskExecutionResult
		resultsChan.Receive(ctx, &result)

		if result.Error != nil {
			logger.Error("Task execution failed",
				"task_id", result.TaskID,
				"error", result.Error,
			)
			completedTasks[result.TaskID] = true // Mark failed task as completed to unblock dependents 注：把失败的 task 也标记为完成，以解除对下游的阻塞
			errorCount++
		} else {
			completedTasks[result.TaskID] = true
			taskResults[result.TaskID] = result.Result
			totalTokens += result.Result.TokensUsed
			successCount++
		}
	}

	logger.Info("Hybrid execution completed",
		"total_tasks", len(tasks),
		"successful", successCount,
		"failed", errorCount,
		"total_tokens", totalTokens,
	)

	return &HybridResult{
		Results:     taskResults,
		TotalTokens: totalTokens,
		Metadata: map[string]any{
			"total_tasks": len(tasks),
			"successful":  successCount,
			"failed":      errorCount,
		},
	}, nil
}

// taskExecutionResult is sent through the results channel
// taskExecutionResult 通过结果 channel 传递
type taskExecutionResult struct {
	TaskID string
	Result activities.AgentExecutionResult
	Error  error
}

// executeHybridTask handles execution of a single task with dependency management
// executeHybridTask 处理单个 task 的执行并管理其依赖
func executeHybridTask(
	ctx workflow.Context,
	task HybridTask,
	originalIndex int,
	taskIndex map[string]*HybridTask,
	completedTasks map[string]bool,
	taskResults map[string]activities.AgentExecutionResult,
	semaphore workflow.Semaphore,
	resultsChan workflow.Channel,
	sessionID string,
	history []string,
	config HybridConfig,
	budgetPerAgent int,
	userID string,
	modelTier string,
) {
	logger := workflow.GetLogger(ctx)

	// Wait for dependencies if any
	// 有依赖时先等待依赖完成
	if len(task.Dependencies) > 0 {
		logger.Info("Waiting for dependencies",
			"task_id", task.ID,
			"dependencies", task.Dependencies,
		)

		if !waitForDependencies(ctx, task.Dependencies, completedTasks, config.DependencyWaitTimeout) {
			resultsChan.Send(ctx, taskExecutionResult{
				TaskID: task.ID,
				Error:  fmt.Errorf("timeout waiting for dependencies"),
			})
			return
		}

		logger.Info("Dependencies satisfied",
			"task_id", task.ID,
		)
	}

	// Acquire semaphore for execution
	// 获取信号量后再执行
	if err := semaphore.Acquire(ctx, 1); err != nil {
		resultsChan.Send(ctx, taskExecutionResult{
			TaskID: task.ID,
			Error:  fmt.Errorf("failed to acquire semaphore: %w", err),
		})
		return
	}
	defer semaphore.Release(1)

	// Prepare task context
	// 准备 task context
	taskContext := make(map[string]any)
	maps.Copy(taskContext, config.Context)
	if task.ContextOverrides != nil {
		maps.Copy(taskContext, task.ContextOverrides)
	}
	taskContext["role"] = task.Role
	taskContext["task_id"] = task.ID
	if task.ParentArea != "" {
		taskContext["parent_area"] = task.ParentArea
	}

	// Add dependency results if configured
	// 按配置注入依赖结果
	if config.PassDependencyResults && len(task.Dependencies) > 0 {
		depResults := make(map[string]any)
		for _, depID := range task.Dependencies {
			if result, ok := taskResults[depID]; ok {
				depResults[depID] = map[string]any{
					"response": result.Response,
					"tokens":   result.TokensUsed,
					"success":  result.Success,
				}
			}
		}
		if len(depResults) > 0 {
			taskContext["dependency_results"] = depResults
			logger.Info("Injected dependency_results into task context",
				"task_id", task.ID,
				"dependency_count", len(depResults),
			)
		}
	}

	// Clear tool parameters for dependent tasks if configured
	// 按配置清空下游 task 的 tool 参数
	if config.ClearDependentToolParams && len(task.Dependencies) > 0 && task.ToolParameters != nil {
		task.ToolParameters = nil
	}

	// Compute agent name using station names for deterministic, human-readable IDs
	// 用站点名计算 agent 名称，得到确定且可读的 ID
	wid := workflow.GetInfo(ctx).WorkflowExecution.ID
	if config.Context != nil {
		if p, ok := config.Context["parent_workflow_id"].(string); ok && p != "" {
			wid = p
		}
	}
	agentName := agents.GetAgentName(wid, originalIndex)

	// Emit agent started event (parent workflow when available)
	// 发出 agent started 事件（有 parent workflow 时发给它）
	if config.EmitEvents {
		_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
			activities.EmitTaskUpdateInput{
				WorkflowID: wid,
				EventType:  activities.StreamEventAgentStarted,
				AgentID:    agentName,
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
	}

	// Execute the task using parallel or sequential execution patterns
	// 用并行或串行执行模式执行该 task
	parallelTask := ParallelTask{
		ID:               task.ID,
		Description:      task.Description,
		SuggestedTools:   task.SuggestedTools,
		ToolParameters:   task.ToolParameters,
		PersonaID:        task.PersonaID,
		Role:             task.Role,
		ContextOverrides: task.ContextOverrides,
	}

	parallelConfig := ParallelConfig{
		MaxConcurrency:   1, // Single task execution 注：单 task 执行
		Context:          taskContext,
		EmitEvents:       false,         // Already handled 注：已在外部处理
		AgentIndexOffset: originalIndex, // Align inner agent name with hybrid's naming 注：让内层 agent 名与 hybrid 的命名对齐
	}

	result, err := ExecuteParallel(
		ctx,
		[]ParallelTask{parallelTask},
		sessionID,
		history,
		parallelConfig,
		budgetPerAgent,
		userID,
		modelTier,
	)

	if err != nil {
		// Emit error event (parent workflow when available)
		// 发出 error 事件（有 parent workflow 时发给它）
		if config.EmitEvents {
			_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
				activities.EmitTaskUpdateInput{
					WorkflowID: wid,
					EventType:  activities.StreamEventErrorOccurred,
					AgentID:    agentName,
					Message:    err.Error(),
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
		}

		resultsChan.Send(ctx, taskExecutionResult{
			TaskID: task.ID,
			Error:  err,
		})
		return
	}

	// Emit completion event (parent workflow when available)
	// 发出完成事件（有 parent workflow 时发给它）
	if config.EmitEvents && len(result.Results) > 0 {
		_ = workflow.ExecuteActivity(ctx, "EmitTaskUpdate",
			activities.EmitTaskUpdateInput{
				WorkflowID: wid,
				EventType:  activities.StreamEventAgentCompleted,
				AgentID:    agentName,
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
	}

	// Send result
	// 发送结果
	if len(result.Results) > 0 {
		resultsChan.Send(ctx, taskExecutionResult{
			TaskID: task.ID,
			Result: result.Results[0],
		})
	} else {
		resultsChan.Send(ctx, taskExecutionResult{
			TaskID: task.ID,
			Error:  fmt.Errorf("no result from execution"),
		})
	}
}

// waitForDependencies waits for all dependencies to complete
// waitForDependencies 等待所有依赖完成
func waitForDependencies(
	ctx workflow.Context,
	dependencies []string,
	completedTasks map[string]bool,
	timeout time.Duration,
) bool {
	logger := workflow.GetLogger(ctx)

	ok, err := workflow.AwaitWithTimeout(ctx, timeout, func() bool {
		for _, depID := range dependencies {
			if !completedTasks[depID] {
				return false
			}
		}
		return true
	})
	if err != nil {
		logger.Debug("Context cancelled during dependency wait", "error", err)
		return false
	}
	if !ok {
		logger.Warn("Dependency wait timeout",
			"dependencies", dependencies,
			"timeout", timeout,
		)
		return false
	}
	return true
}
