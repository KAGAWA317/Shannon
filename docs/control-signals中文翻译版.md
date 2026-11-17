# Workflow 控制信号

本文档描述 Shannon workflow 的暂停/恢复/取消控制信号系统。

## 概述

Shannon 支持三种用于 workflow 管理的控制信号：
- **Pause**：在下一个 checkpoint 处暂时中止 workflow 执行
- **Resume**：从 checkpoint 继续已暂停的 workflow
- **Cancel**：永久停止 workflow 并执行清理

## 信号语义

### Pause 信号（`pause_v1`）

**pause 何时生效：**
- pause 信号会立即入队，但只在 **checkpoint** 处生效
- checkpoint 是 workflow 执行中的策略性位置，在这些位置暂停是安全的
- OrchestratorWorkflow 的 checkpoint：`pre_routing`、`pre_simple_workflow`、`pre_supervisor_workflow`、`pre_dag_workflow`
- strategy workflow 的 checkpoint：`pre_simple_strategy`、`pre_research_workflow` 等
- 子 workflow 有各自的 checkpoint：`pre_execution`、`pre_completion`、`post_agent`

**「checkpoint」的含义：**
- checkpoint 是 workflow 开发者有意插入的暂停点
- 在 checkpoint 处，workflow 会查询自身的控制状态，若处于 pause 状态则阻塞
- checkpoint 确保 workflow 在一致且可恢复的状态下暂停
- 正在进行的工作（LLM 调用、activity）会在暂停前完成
- checkpoint 使用 `workflow.Await()` 实现高效阻塞（不轮询 timer）

**行为：**
1. 信号发送至父 workflow
2. 父 workflow 将信号传播给所有已注册的子 workflow
3. 每个 workflow 在下一个 checkpoint 处阻塞，直到收到 resume 或 cancel

### Resume 信号（`resume_v1`）

**resume 何时生效：**
- 立即解除在 checkpoint 等待的 workflow 的阻塞
- 传播至所有子 workflow

**行为：**
1. 清除 pause 状态
2. 发出 `WORKFLOW_RESUMED` SSE event
3. workflow 从 checkpoint 继续执行

### Cancel 信号（`cancel_v1`）

**cancel 何时生效：**
- 立即将 workflow 标记为已取消
- 下一个 checkpoint 返回错误，终止 workflow

**行为：**
1. 设置取消状态并记录原因
2. 传播至所有子 workflow
3. 依次发出 `WORKFLOW_CANCELLING` 与 `WORKFLOW_CANCELLED` event
4. workflow 以取消错误终止

## API 端点

### gRPC

```protobuf
// Pause a running workflow
rpc PauseTask(PauseTaskRequest) returns (PauseTaskResponse);

// Resume a paused workflow
rpc ResumeTask(ResumeTaskRequest) returns (ResumeTaskResponse);

// Cancel a workflow
rpc CancelTask(CancelTaskRequest) returns (CancelTaskResponse);

// Get current control state
rpc GetControlState(GetControlStateRequest) returns (GetControlStateResponse);
```

### 查询处理器

workflow 会为 `control_state_v1` 注册查询处理器，返回：
```go
type WorkflowControlState struct {
    IsPaused      bool
    PausedAt      time.Time
    PauseReason   string
    PausedBy      string
    IsCancelled   bool
    CancelReason  string
    CancelledBy   string
}
```

## SSE event

控制信号会发出 SSE event 供前端集成：

| 内部类型 | SSE event 名称 | Payload |
|--------------|----------------|---------|
| WORKFLOW_PAUSING | workflow.pausing | workflow_id, agent_id, message |
| WORKFLOW_PAUSED | workflow.paused | workflow_id, agent_id, checkpoint, message |
| WORKFLOW_RESUMED | workflow.resumed | workflow_id, agent_id, message |
| WORKFLOW_CANCELLING | workflow.cancelling | workflow_id, agent_id, message |
| WORKFLOW_CANCELLED | workflow.cancelled | workflow_id, agent_id, message |

## 集成指南

### 为 workflow 添加 checkpoint

```go
func MyWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
    // Initialize control handler
    controlHandler := &control.SignalHandler{
        WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
        AgentID:    "my-agent",
        Logger:     workflow.GetLogger(ctx),
        EmitCtx:    emitCtx,
    }
    controlHandler.Setup(ctx)

    // Pre-execution checkpoint
    if err := controlHandler.CheckPausePoint(ctx, "pre_execution"); err != nil {
        return TaskResult{Success: false, ErrorMessage: err.Error()}, err
    }

    // ... do work ...

    // Pre-completion checkpoint
    if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
        return TaskResult{Success: false, ErrorMessage: err.Error()}, err
    }

    return TaskResult{Success: true}, nil
}
```

### 子 workflow 注册

对于会创建子 workflow 的父 workflow：

```go
// Before starting child
childFuture := workflow.ExecuteChildWorkflow(ctx, ChildWorkflow, input)
var childExec workflow.Execution
childFuture.GetChildWorkflowExecution().Get(ctx, &childExec)
controlHandler.RegisterChildWorkflow(childExec.ID)

// After child completes
err := childFuture.Get(ctx, &result)
controlHandler.UnregisterChildWorkflow(childExec.ID)
```

## 确定性与重放

控制信号使用版本门控以保证重放安全：

```go
version := workflow.GetVersion(ctx, "pause_resume_v1", workflow.DefaultVersion, 1)
if version < 1 {
    // Legacy behavior (no control signals)
    return
}
// Control signal handling
```

这可以确保：
- 已有的 workflow 重放不受影响
- 新的 workflow 获得控制信号支持
- 运行中的 workflow 可平滑迁移

## 测试

控制信号的单元测试：
```bash
go test -v ./internal/workflows/control/...
```

测试覆盖：
- 信号处理器初始化
- pause 信号的接收与阻塞
- resume 信号的解除阻塞
- cancel 信号的终止
- 重放确定性
- 子 workflow 注册
- 多次 pause/resume 循环

## 故障排查

### 已发送 pause 信号但 workflow 继续执行
- **原因**：workflow 尚未到达 checkpoint
- **解决方案**：在 workflow 的策略性位置添加 checkpoint

### 子 workflow 未暂停
- **原因**：父 workflow 未注册子 workflow ID
- **解决方案**：在启动子 workflow 之前使用 `RegisterChildWorkflow()`

### 控制状态显示为 paused 但状态为 RUNNING
- **原因**：DB 状态更新发生在终态
- **解决方案**：调用 `GetControlState` 查询实时 pause 状态
