# Token 预算与成本追踪

## 概述

Shannon 实现了一套双路径 token 追踪系统，确保在所有 workflow pattern 下都能准确上报成本，同时在启用 budget 时避免重复记录。

**核心原则**：每一次 agent 执行都**恰好记录一次** token 使用量，要么通过带 budget 的 activity（启用 budget 时），要么通过 workflow pattern（未启用 budget 时）。

## 架构

### 记录路径

```
┌─────────────────────────────────────────────────────────────────┐
│                     Workflow Execution                          │
└────────────────┬────────────────────────────────────────────────┘
                 │
                 ├── Budget Enabled? ──────┐
                 │                          │
        ┌────────▼─────────┐     ┌─────────▼──────────┐
        │   YES (Budget)   │     │  NO (Non-Budget)   │
        └────────┬─────────┘     └──────────┬─────────┘
                 │                           │
                 ▼                           ▼
    ┌────────────────────────┐   ┌──────────────────────────┐
    │ ExecuteAgentWithBudget │   │    ExecuteAgent          │
    │    (activity)          │   │    (activity)            │
    └────────┬───────────────┘   └──────────┬───────────────┘
             │                               │
             ▼                               │
    ┌────────────────────────┐              │
    │ Records token usage    │              │
    │ in activity (budget.go)│              │
    └────────────────────────┘              │
                                             ▼
                                  ┌──────────────────────────┐
                                  │ Pattern records tokens   │
                                  │ (parallel/react/etc.)    │
                                  └──────────────────────────┘
                 │                           │
                 └───────────┬───────────────┘
                             ▼
                ┌────────────────────────────┐
                │    token_usage table       │
                │  (one record per agent)    │
                └────────────────────────────┘
```

## 核心组件

### 1. 带预算的执行 Activity

**文件**：`go/orchestrator/internal/activities/budget.go`

**函数**：`ExecuteAgentWithBudget`

**职责**：
- 按 agent 强制 token budget 上限
- 带 token 约束调用 LLM 服务
- **立即记录 token 使用量**（第 346-356 行）
- 将执行结果返回给 workflow

**记录逻辑**：
```go
// Line 346-356: Records usage inside the activity
err = b.budgetManager.RecordUsage(ctx, &budget.BudgetTokenUsage{
    UserID:         input.UserID,
    SessionID:      input.AgentInput.SessionID,
    TaskID:         input.TaskID,
    AgentID:        input.AgentInput.AgentID,
    Model:          actualModel,
    Provider:       actualProvider,
    InputTokens:    inputTokens,
    OutputTokens:   outputTokens,
    IdempotencyKey: idempotencyKey,
})
```

**使用场景**：
- `budgetPerAgent > 0`（Parallel、Sequential、Hybrid pattern）
- `opts.BudgetAgentMax > 0`（React、Chain-of-Thought、Debate、Tree-of-Thoughts）
- 带 budget 约束的 SupervisorWorkflow

---

### 2. 不带预算的执行 Activity

**文件**：`go/orchestrator/internal/activities/agent.go`

**函数**：`ExecuteAgent`

**职责**：
- 在无 budget 约束下执行 agent
- 调用 LLM 服务
- **不记录 token 使用量**（由 pattern 负责）
- 将执行结果返回给 workflow

**使用场景**：
- SimpleTaskWorkflow
- 无 budget 约束的 workflow
- Decompose/Refine/Synthesis 阶段（始终不带预算）

---

### 3. Workflow Pattern 记录

所有 workflow pattern 都实现了条件化的 token 记录，以避免重复：

#### Guard 模式

```go
// Budgeted path: Activity records, pattern skips
if budgetPerAgent <= 0 {  // or opts.BudgetAgentMax <= 0
    _ = workflow.ExecuteActivity(ctx, constants.RecordTokenUsageActivity, ...)
}
```

#### 带记录的 Pattern

| Pattern | 文件 | Guard 位置 | Agent ID |
|---------|------|----------------|-----------|
| **Parallel** | `patterns/execution/parallel.go` | 第 267 行 | Result AgentID |
| **Sequential** | `patterns/execution/sequential.go` | 第 289 行 | Result AgentID |
| **React** | `patterns/react.go` | 第 154、367、493 行 | reasoner、action、synthesizer |
| **Chain-of-Thought** | `patterns/chain_of_thought.go` | 第 145、236 行 | cot-reasoner、cot-clarifier |
| **Debate** | `patterns/debate.go` | 第 201、354 行 | debater AgentID |
| **Tree-of-Thoughts** | `patterns/tree_of_thoughts.go` | 第 272-350 行 | tot-generator-{id} |
| **Reflection** | `patterns/wrappers.go`、`patterns/reflection.go` | 第 212-245 行（initial）、第 123-159 行（synth） | reflection-initial、reflection-synth |
| **ScheduledTask** | `workflows/scheduled/scheduled_task_workflow.go` | 版本门禁 `scheduled_quota_record_v1` | 不适用（记录 tenant 配额，而非按 agent 记录） |

#### 不带记录的 Pattern（委托）

| Pattern | 文件 | 委托目标 |
|---------|------|-------------------|
| **Hybrid** | `patterns/execution/hybrid.go` | 委托给 Parallel（第 275 行） |

---

### 4. Workflow 级记录（始终记录）

部分 workflow 阶段始终记录使用量（不适用 budget 约束）：

**OrchestratorWorkflow - Decompose**
- 文件：`internal/workflows/orchestrator_router.go:218`
- AgentID：`"decompose"`
- Metadata：`{"phase": "decompose"}`

**SupervisorWorkflow - Decompose**
- 文件：`internal/workflows/supervisor_workflow.go:337`
- AgentID：`"decompose"`
- Metadata：`{"phase": "decompose"}`

**ResearchWorkflow - 各阶段**
- Refine：`strategies/research.go:204` → AgentID：`"research-refiner"`
- Decompose：`strategies/research.go:265` → AgentID：`"decompose"`
- Synthesis：`strategies/research.go:826` → AgentID：`"synthesis"`

**SimpleTaskWorkflow**
- 文件：`internal/workflows/simple_workflow.go:487`
- AgentID：`"simple-agent"`
- 始终记录（单个 agent，不拆分 budget）

---

## Token 使用量记录

### 数据库 Schema

**表**：`token_usage`

```sql
CREATE TABLE token_usage (
    id                uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id           uuid,
    provider          varchar(50)  NOT NULL,
    model             varchar(255) NOT NULL,
    prompt_tokens     integer      NOT NULL,
    completion_tokens integer      NOT NULL,
    total_tokens      integer      NOT NULL,
    cost_usd          numeric(10,6) NOT NULL,
    created_at        timestamp with time zone DEFAULT CURRENT_TIMESTAMP,
    task_id           uuid REFERENCES task_executions(id) ON DELETE CASCADE
);
```

**索引**：
- `id` 上的主键
- `task_id` 上的外键 → `task_executions(id)`
- `user_id` 上的索引，用于用户级成本查询
- `provider`、`model` 上的索引，用于 provider/model 分析
- `created_at` 上的索引，用于时间序列查询

---

### 记录流程

```
┌──────────────────────────────────────────────────────────────────┐
│ 1. LLM Provider Returns Response                                │
│    - model_used: "gpt-5-2025-08-07"                             │
│    - provider: "openai"                                         │
│    - input_tokens: 732                                          │
│    - output_tokens: 1464                                        │
└────────────────────┬─────────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ 2. Activity Captures Token Counts                               │
│    - AgentExecutionResult.InputTokens = 732                     │
│    - AgentExecutionResult.OutputTokens = 1464                   │
│    - AgentExecutionResult.ModelUsed = "gpt-5-2025-08-07"        │
│    - AgentExecutionResult.Provider = "openai"                   │
└────────────────────┬─────────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ 3. Recording Decision (Budgeted vs Non-Budgeted)                │
│                                                                  │
│  IF budgetPerAgent > 0:                                         │
│    → ExecuteAgentWithBudget records in activity (budget.go:346) │
│    → Workflow pattern SKIPS recording (guard prevents duplicate)│
│                                                                  │
│  ELSE (budgetPerAgent <= 0):                                    │
│    → ExecuteAgent does NOT record                               │
│    → Workflow pattern records via RecordTokenUsageActivity      │
└────────────────────┬─────────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ 4. RecordTokenUsageActivity (activities/session.go)             │
│    - Resolves task_id from workflow_id                          │
│    - Calculates cost: pricing.CostForSplit(model, in, out)      │
│    - Inserts into token_usage table                             │
└────────────────────┬─────────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ 5. Database Persistence (PostgreSQL)                            │
│    INSERT INTO token_usage (                                    │
│      user_id, provider, model,                                  │
│      prompt_tokens, completion_tokens, total_tokens,            │
│      cost_usd, task_id                                          │
│    ) VALUES (...)                                               │
└────────────────────┬─────────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ 6. Workflow Aggregation (metadata/aggregate.go)                 │
│    - Sums all token_usage records for task                      │
│    - Calculates total cost                                      │
│    - Populates TaskResult.Metadata:                             │
│      {                                                           │
│        "model_used": "gpt-5-2025-08-07",                        │
│        "provider": "openai",                                    │
│        "input_tokens": 4362,                                    │
│        "output_tokens": 8727,                                   │
│        "total_tokens": 13089,                                   │
│        "cost_usd": 0.183258                                     │
│      }                                                           │
└────────────────────┬─────────────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│ 7. API Response (GET /api/v1/tasks/{id})                        │
│    {                                                             │
│      "task_id": "task-...",                                     │
│      "status": "TASK_STATUS_COMPLETED",                         │
│      "usage": {                                                 │
│        "total_tokens": 13089,                                   │
│        "prompt_tokens": 4362,                                   │
│        "completion_tokens": 8727,                               │
│        "cost_usd": 0.183258                                     │
│      },                                                          │
│      "metadata": {                                              │
│        "model_used": "gpt-5-2025-08-07",                        │
│        "provider": "openai"                                     │
│      }                                                           │
│    }                                                             │
└──────────────────────────────────────────────────────────────────┘
```

---
## 零 Token 执行

### 纯 Tool 路径

有些 agent 执行在设计上不会发生任何 LLM 推理，因而返回 **0 个 LLM token**：

**常见场景**：
- **强制 tool 调用**：agent 不经过 LLM 推理直接使用 tool（`forced_tools` 模式）
- **Cache 命中**：响应完全由 cache 返回（未来功能）
- **纯 tool workflow**：只执行 tool、不做文本生成的 workflow

**示例**（来自 ResearchWorkflow）：
```go
// Tool-only web search execution
result := executeWebSearch(ctx, query)
// Returns: InputTokens=0, OutputTokens=0, ToolExecutions=[...]
```

关于 gap-filling tool 执行的细节，参见 [Deep Research Overview](deep-research-overview.md#gap-filling-and-convergence)。

---

### 记录行为

**默认**：零 token 执行**不会**写入 `token_usage` 表

**理由**：
- 没有 LLM 成本需要追踪
- 没有可计费的 token 消耗
- 减少非 LLM 操作的数据库写入

**实现**：
```go
// Pattern recording guards (parallel.go, sequential.go)
if result.TokensUsed == 0 && result.InputTokens == 0 {
    if recordZeroToken, ok := context["record_zero_token"].(bool); !ok || !recordZeroToken {
        logger.Warn("Skipping zero-token recording (no LLM inference occurred)")
        return nil  // Skip recording
    }
}
```

**位置**：
- `go/orchestrator/internal/workflows/patterns/execution/sequential.go:316-380`
- `go/orchestrator/internal/workflows/patterns/execution/parallel.go:295-320`

---

### 启用零 Token 记录

如需为**审计追踪目的**记录零 token 执行：

**通过 API Context**：
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "Task with tool-only execution",
    "context": {
      "record_zero_token": true
    }
  }'
```

**通过 Workflow Context**：
```go
baseContext := map[string]interface{}{
    "record_zero_token": true,  // Enable zero-token recording
}
```

**启用后**：
- 零 token 记录会插入 `token_usage` 表
- `cost_usd` = 0.0
- metadata 可用于区分纯 tool 执行与 LLM 执行

**使用场景**：
- 合规/审计要求（追踪所有 agent 执行）
- 调试 tool 执行 workflow
- 分析纯 tool 路径与 LLM 路径

---

### 区分零 Token 类型

在查看 token 为 0 的 `token_usage` 记录时：

```sql
-- Find zero-token executions
SELECT
    tu.model,
    tu.provider,
    tu.total_tokens,
    tu.cost_usd,
    te.metadata->>'tool_executions' as tools_used
FROM token_usage tu
JOIN task_executions te ON tu.task_id = te.id
WHERE tu.total_tokens = 0
ORDER BY tu.created_at DESC;
```

**解读**：
- `tools_used IS NOT NULL` → 强制 tool 调用（预期为零）
- `tools_used IS NULL` → 可能存在问题（LLM 本应返回 token）

---

## 防止重复记录

### 问题（修复前）

**问题**：启用 budget 的执行把用量记录了两次：
1. **Activity 记录**：`budget.go:346`（在 ExecuteAgentWithBudget 内部）
2. **Pattern 记录**：`parallel.go:289`（workflow 层）

**影响**：token 数 ×2、成本计算 ×2、数据库出现重复行

**示例**：
```
Actual usage: 2,196 tokens ($0.030744)
Recorded:     4,392 tokens ($0.061488)  ← 2× overcount
```

---

### 解决方案（方案 B - 条件守卫）

**策略**：启用 budget 时阻止 pattern 层记录

**实现**：用 budget 检查为所有 pattern 层记录加上守卫

```go
// BEFORE (duplicates)
_ = workflow.ExecuteActivity(ctx, constants.RecordTokenUsageActivity, ...)

// AFTER (no duplicates)
if budgetPerAgent <= 0 {  // Only record if NOT budgeted
    _ = workflow.ExecuteActivity(ctx, constants.RecordTokenUsageActivity, ...)
}
```

**守卫差异**：
- Parallel/Sequential/Hybrid：`if budgetPerAgent <= 0`
- React/CoT/Debate：`if opts.BudgetAgentMax <= 0`
- Tree-of-Thoughts：`if tokenBudget > 0 { budgeted } else { record }`

---

### 验证

**测试查询**：
```sql
-- Check for duplicate recordings (should return 0 rows)
SELECT
  prompt_tokens,
  completion_tokens,
  model,
  COUNT(*) as record_count
FROM token_usage tu
JOIN task_executions te ON tu.task_id = te.id
WHERE te.workflow_id = 'task-...'
GROUP BY prompt_tokens, completion_tokens, model
HAVING COUNT(*) > 1;
```

**预期结果**：0 行（无重复）

---

## Budget 配置

### 设置 Budget

**通过 API**：
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{
    "query": "Complex research task",
    "context": {
      "budget_agent_max": 50000,  # Max tokens per agent
      "budget_total_max": 200000  # Max tokens for entire workflow
    }
  }'
```

**通过模板**：
```yaml
name: research_workflow
defaults:
  budget_agent_max: 30000
  budget_total_max: 150000
```

**通过环境变量**：
```bash
# Default budgets in .env
BUDGET_AGENT_MAX=40000
BUDGET_TOTAL_MAX=200000
```

---

### Budget 强制执行

**SupervisorWorkflow 示例**：
```go
// Determine budget per agent
agentMaxTokens := 0
if v, ok := baseContext["budget_agent_max"].(int); ok {
    agentMaxTokens = v
}

// Calculate per-agent budget
budgetPerAgent := 0
if agentMaxTokens > 0 && len(tasks) > 0 {
    budgetPerAgent = agentMaxTokens / len(tasks)
}

// Execute with budget
if budgetPerAgent > 0 {
    // Budgeted path: activity records
    err := workflow.ExecuteActivity(ctx,
        constants.ExecuteAgentWithBudgetActivity,
        activities.BudgetedAgentInput{
            AgentInput: agentInput,
            MaxTokens:  budgetPerAgent,
            UserID:     input.UserID,
            TaskID:     workflowID,
            ModelTier:  modelTier,
        }).Get(ctx, &result)
} else {
    // Non-budgeted path: pattern records
    err := workflow.ExecuteActivity(ctx,
        activities.ExecuteAgent,
        agentInput).Get(ctx, &result)
}
```

---

## 成本计算

### 定价配置

**文件**：`config/models.yaml`

```yaml
pricing:
  defaults:
    combined_per_1k: 0.005
  models:
    openai:
      gpt-5-2025-08-07:
        input_per_1k: 0.0060   # $0.006 per 1K input tokens
        output_per_1k: 0.0180  # $0.018 per 1K output tokens
```

细节参见 [centralized-pricing.md](centralized-pricing.md)。

---

### 计算逻辑

**文件**：`go/orchestrator/internal/pricing/pricing.go`

**函数**：`CostForSplit`

```go
func CostForSplit(model string, inputTokens, outputTokens int) float64 {
    provider := DetectProvider(model)

    // Lookup pricing config
    inputPrice := getInputPricePerToken(provider, model)
    outputPrice := getOutputPricePerToken(provider, model)

    // Calculate cost
    inputCost := float64(inputTokens) * inputPrice
    outputCost := float64(outputTokens) * outputPrice

    return inputCost + outputCost
}
```

**示例**：
```
Model: gpt-5-2025-08-07
Input: 732 tokens × $0.000006 = $0.004392
Output: 1464 tokens × $0.000018 = $0.026352
Total: $0.030744
```

---

## Metadata 聚合

### 聚合逻辑

**文件**：`go/orchestrator/internal/metadata/aggregate.go`

**函数**：`AggregateAgentMetadata`（legacy helper）

该 helper 从内存中的 agent 执行结果构建一份轻量摘要（model_used、provider、input_tokens、output_tokens、total_tokens），并可能附加一个尽力而为的 `agent_usages` 数组用于调试。

> ⚠️ 对于外部消费者，`agent_usages` 已废弃（deprecated）。如需完整的用量与成本数据（包括 synthesis、decomposition、utilities 以及失败的尝试），请依赖 `model_breakdown`，它由 `token_usage` 表构建。

workflow 仍会调用 `AggregateAgentMetadata` 填充 task 级 metadata，但计费、报表与公共 API 应把 `model_breakdown` 作为可信数据源。

---

## 监控与调试

### 数据库查询

**查询某个 task 的 token 用量**：
```sql
SELECT
    tu.model,
    tu.provider,
    tu.prompt_tokens,
    tu.completion_tokens,
    tu.total_tokens,
    tu.cost_usd,
    tu.created_at
FROM token_usage tu
JOIN task_executions te ON tu.task_id = te.id
WHERE te.workflow_id = 'task-...'
ORDER BY tu.created_at;
```

**按 task 聚合成本**：
```sql
SELECT
    te.workflow_id,
    COUNT(tu.id) as agent_count,
    SUM(tu.prompt_tokens) as total_input,
    SUM(tu.completion_tokens) as total_output,
    SUM(tu.total_tokens) as total_tokens,
    SUM(tu.cost_usd) as total_cost
FROM task_executions te
LEFT JOIN token_usage tu ON tu.task_id = te.id
WHERE te.user_id = '...'
GROUP BY te.workflow_id
ORDER BY total_cost DESC
LIMIT 10;
```

**检测重复记录**：
```sql
SELECT
    prompt_tokens,
    completion_tokens,
    model,
    COUNT(*) as duplicates
FROM token_usage
WHERE task_id = (
    SELECT id FROM task_executions WHERE workflow_id = 'task-...'
)
GROUP BY prompt_tokens, completion_tokens, model
HAVING COUNT(*) > 1;
```

---

### Orchestrator 日志

**Budget 追踪**：
```bash
docker compose -f deploy/compose/docker-compose.yml logs orchestrator | \
  grep "Budget used"
```

**Token 记录**：
```bash
docker compose -f deploy/compose/docker-compose.yml logs orchestrator | \
  grep "RecordTokenUsage"
```

**成本计算**：
```bash
docker compose -f deploy/compose/docker-compose.yml logs orchestrator | \
  grep "cost_usd"
```

---

### Temporal Workflow 历史

**查看 workflow 事件**：
```bash
docker compose exec temporal temporal workflow describe \
  --workflow-id task-... \
  --address temporal:7233
```

**检查 activity 执行**：
```bash
docker compose exec temporal temporal workflow show \
  --workflow-id task-... \
  --address temporal:7233 | \
  grep -A 5 "RecordTokenUsageActivity"
```

---
## 最佳实践

### 1. 始终使用拆分后的 token 计数

**推荐做法**：
```go
cost := pricing.CostForSplit(model, inputTokens, outputTokens)
```

**避免**：
```go
cost := pricing.CostForTokens(model, totalTokens)  // Less accurate
```

**原因**：输入 token 与输出 token 的价格不同

---

### 2. 传播模型信息

**AgentExecutionResult 中的必填项**：
- `ModelUsed`（例如 "gpt-5-2025-08-07"）
- `Provider`（例如 "openai"）
- `InputTokens`（prompt token）
- `OutputTokens`（completion token）

**示例**：
```go
result := activities.AgentExecutionResult{
    Response:      llmResponse.Content,
    TokensUsed:    llmResponse.Usage.TotalTokens,
    InputTokens:   llmResponse.Usage.PromptTokens,
    OutputTokens:  llmResponse.Usage.CompletionTokens,
    ModelUsed:     llmResponse.Model,
    Provider:      "openai",
    AgentID:       agentID,
}
```

---

### 3. 为 pattern 级记录加守卫

**新增 pattern 时**，务必为记录加守卫：

```go
// Check if budgeted execution is active
if budgetPerAgent <= 0 {  // or opts.BudgetAgentMax <= 0
    // Record token usage
    _ = workflow.ExecuteActivity(ctx,
        constants.RecordTokenUsageActivity,
        activities.TokenUsageInput{
            UserID:       userID,
            SessionID:    sessionID,
            TaskID:       workflowID,
            AgentID:      agentID,
            Model:        result.ModelUsed,
            Provider:     result.Provider,
            InputTokens:  result.InputTokens,
            OutputTokens: result.OutputTokens,
            Metadata:     map[string]interface{}{"phase": "pattern_name"},
        }).Get(ctx, nil)
}
```

---

### 4. 附带 phase 元数据

**目的**：在分析中区分记录的来源

**示例**：
- `{"phase": "parallel"}`
- `{"phase": "react-reasoner"}`
- `{"phase": "decompose"}`
- `{"phase": "synthesis"}`

**用法**：
```sql
-- Analyze token usage by phase
SELECT
    metadata->>'phase' as phase,
    COUNT(*) as executions,
    AVG(total_tokens) as avg_tokens,
    SUM(cost_usd) as total_cost
FROM token_usage
GROUP BY metadata->>'phase'
ORDER BY total_cost DESC;
```

---

### 5. 变更后验证没有重复记录

**修改 token 记录逻辑后**：

1. 在启用 budget 的情况下运行一个测试 workflow
2. 查询是否存在重复：
```sql
SELECT COUNT(*) FROM (
    SELECT prompt_tokens, completion_tokens, model, COUNT(*)
    FROM token_usage
    WHERE task_id IN (
        SELECT id FROM task_executions
        WHERE workflow_id = 'test-task-...'
    )
    GROUP BY prompt_tokens, completion_tokens, model
    HAVING COUNT(*) > 1
) duplicates;
```
3. 预期结果：`0`

---

## 相关文档

- [集中式定价配置](centralized-pricing.md) - 定价配置与成本计算
- [速率感知的 budget 机制](rate-aware-budgeting.md) - 速率限制的管理与控制
- [多 agent workflow 架构](multi-agent-workflow-architecture.md) - workflow pattern 与执行
- [Pattern 使用指南](pattern-usage-guide.md) - 何时使用每种 workflow pattern
- [API 参考](api-reference.md) - 用于任务提交与状态查询的 REST API 端点

---

## 故障排查

### token 计数缺失

**症状**：API 响应中的 `input_tokens` 与 `output_tokens` 为 0

**原因**：
1. LLM provider 未返回 token 计数
2. agent activity 未捕获 token 计数
3. 回退到 `TokensUsed`（仅总数）

**解决方案**：
```go
// Ensure LLM response includes token counts
if result.InputTokens == 0 && result.OutputTokens == 0 {
    if result.TokensUsed > 0 {
        // Approximate split (60/40)
        result.InputTokens = result.TokensUsed * 6 / 10
        result.OutputTokens = result.TokensUsed - result.InputTokens
    }
}
```

---

### 重复记录

**症状**：token 计数是预期值的 2 倍

**原因**：
1. pattern 级记录没有加守卫
2. budget 检查缺失或不正确
3. activity 与 pattern 同时记录

**解决方案**：
1. 为 pattern 记录添加 budget 守卫
2. 确认守卫条件：`if budgetPerAgent <= 0`
3. 运行重复检测查询
4. 检查 Temporal workflow 历史中是否有重复的 activity

---

### 成本不正确

**症状**：成本与预期值不符

**原因**：
1. `models.yaml` 中的定价配置错误
2. 模型名称不匹配（例如 "gpt-5" 与 "gpt-5-2025-08-07"）
3. provider 识别失败
4. 使用合并定价而非拆分定价

**解决方案**：
1. 核对 `config/models.yaml` 中的定价
2. 确认模型名称完全匹配：`SELECT DISTINCT model FROM token_usage`
3. 强制指定 provider：`result.Provider = "openai"`
4. 使用 `CostForSplit` 而不是 `CostForTokens`

---

### budget 未生效

**症状**：agent 超出 budget 却没有报错

**原因**：
1. budget 没有传给 activity
2. budget 检查被禁用
3. 使用 `ExecuteAgent` 而不是 `ExecuteAgentWithBudget`

**解决方案**：
1. 确认 context 中的 budget：`budget_agent_max`
2. 检查 activity 调用：
```go
// Should use ExecuteAgentWithBudget
err := workflow.ExecuteActivity(ctx,
    constants.ExecuteAgentWithBudgetActivity,
    activities.BudgetedAgentInput{
        AgentInput: agentInput,
        MaxTokens:  budgetPerAgent,  // ← Ensure > 0
        ...
    })
```
3. 查看 budget manager 日志：`grep "Budget exceeded"`

---

## 感知 cache 的 token 记账（migration 121）

Shannon 追踪**两个并列的总 token 字段**，二者都持久化在 `token_usage`（每次 LLM 调用）和 `task_executions`（每个 workflow）中：

| 字段 | 公式 | 用途 |
|-------|---------|---------|
| `total_tokens` | `prompt_tokens + completion_tokens` | 兼容 OpenAI 的 API 响应；与响应形态一致的元数据 |
| `cache_aware_total_tokens` | `prompt + completion + cache_read + cache_creation` | 配额追踪、真实成本、企业计费 |

两者刻意保持区分：`total_tokens` 保留 OpenAI 语义，因此现有的 SDK 客户端（以及 `/v1/chat/completions` / `/v1/completions` 响应中的 `usage.total_tokens`）保持不变。`cache_aware_total_tokens` 才是企业配额计数器应当读取的字段。

### 不变式

对于这两张表中的每一行：

```
cache_aware_total_tokens
  == prompt_tokens + completion_tokens + cache_read_tokens + cache_creation_tokens
```

运行 `scripts/verify_cache_quota_invariant.sh` 加以确认。输出为空（逐行不匹配数 = 0，逐任务不匹配数 = 0）即表示不变式成立。

### 写入路径

写入 `token_usage` 的路径共有**四条**。它们都必须填充 `cache_aware_total_tokens`：

| 路径 | 文件 | 说明 |
|------|------|------|
| BudgetManager（编排式 workflow） | `internal/budget/manager.go` `RecordUsage` / `storeUsage` | 在 `TotalTokens` 之后立即计算 `CacheAwareTotalTokens` |
| Gateway completions 代理 | `cmd/gateway/internal/openai/handler.go` `recordCompletionUsage` | `/v1/completions` 绕过 orchestrator —— 其 INSERT 在语句内计算该字段 |
| Gateway tools 代理 | `cmd/gateway/internal/handlers/tools.go` `recordToolUsage` | `/api/v1/tools/.../execute` 直接 INSERT —— 将 `tokens` 存入 `completion_tokens`，从而在不伪造 cache 字段的前提下满足不变式 |
| TaskExecution 写入器（rollup，独立表） | `internal/db/task_writer.go` | 将每个 workflow 的 rollup 写入 `task_executions`；在每次 `INSERT` 之前防御性地重新计算，使调用方无法产生漂移 |

`internal/activities/budget.go` 中的零 token 守卫（`shouldRecordUsage`）允许纯 cache 命中通过：某次调用 `input + output == 0`，但 `cache_read > 0` 或 `cache_creation > 0` 时仍会被记录，因此 prompt cache 的成本依然会计费。

### 已更新的读取 / 配额路径

| 路径 | 文件 | 行为 |
|------|------|----------|
| Workflow rollup SUM | `internal/server/service.go`（`getTaskMetadataFromDB`） | 使用 `SUM(cache_aware_total_tokens)`，并基于各分量做回退 |
| 逐 agent 明细 | `internal/server/service.go`（`agent_usages`） | 在 `total_tokens` 之外同时输出 `cache_aware_total_tokens` |
| 使用量报告 | `internal/budget/manager.go` `GetUsageReport` | 填充 `UsageReport.CacheAwareTotalTokens`；按模型填充 `ModelUsage.CacheAwareTokens` |
| Session 配额更新 | `internal/workflows/strategies/research.go` | `session.TokensUsed` 读取 `report.CacheAwareTotalTokens`（迁移期间回退到 `TotalTokens`） |
| Session API（`tokens_used`） | `cmd/gateway/internal/handlers/session.go` | 单 session 与列表两类端点都对 `cache_aware_total_tokens` 求和 |
| 逐任务的进程内元数据 | `internal/metadata/aggregate.go` | 在 `total_tokens` 之外同时输出 `meta["cache_aware_total_tokens"]` |

### gRPC 接口面

`RecordTokenUsageRequest`（proto tag 7-9）携带 `cache_read_tokens`、`cache_creation_tokens`、`cache_creation_1h_tokens`。`OrchestratorService.RecordUsage` hook 接收一个 `RecordUsageDetails` 结构体，其中 `CacheAwareTotalTokens` 已预先算好，供企业版覆盖使用。

### shannon-cloud cherry-pick 检查清单

把 `feat/cache-aware-quota-rollup` 的提交 cherry-pick 到 `shannon-cloud` 之后：

1. 确认 `migrations/postgres/121_cache_aware_total_tokens.sql` 已在生产环境应用。
2. 在 shannon-cloud 对 `OrchestratorService.RecordUsage` 的覆盖实现中，把配额计数器从任何 `tokensUsed` 风格的参数改为 `details.CacheAwareTotalTokens`。
3. **审计所有云侧 SQL**，凡是读取 `SUM(total_tokens)` / `tokens_used`，或自行写入 `token_usage` 行的地方都要检查。每一处都可能是 cache 泄漏点。OSS 侧审计发现四条写入路径和六条读取路径（见上表）—— 可以假定云侧的差异具有相同的形态。
4. 对生产数据库运行 `scripts/verify_cache_quota_invariant.sh` —— 预期两项不匹配计数都为 0。
5. 留意配额看板：每个 tenant 的 token 使用量会跳升（这是泄漏终于被记录下来 —— 而非回归）。
