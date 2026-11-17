# Shannon Orchestrator（Go）

orchestrator 是 Shannon 的中央协调服务，通过 Temporal 以基于 pattern 的认知架构管理 AI agent workflow，执行策略，并处理 session 状态。

## 🎯 核心职责

- **基于 pattern 的编排** - 将 task 路由到认知 pattern（CoT、ToT、ReAct、Debate、Reflection）
- **Workflow 管理** - 通过 Temporal workflow 协调 multi-agent 执行
- **Budget 管理** - 跟踪并强制各 agent 的 token 用量上限
- **策略执行** - 集成 OPA 以实现安全与合规规则
- **Session 管理** - 在多次交互之间维护对话状态
- **服务协调** - 在 Rust agent core 与 Python LLM 服务之间进行路由

## 🏗️ 架构

```
User Request → gRPC Server (:50052)
    ↓
OrchestratorRouter (Pattern Selection)
    ↓
Pattern Analysis → Cognitive Pattern Selection
    ├── Chain of Thought (CoT) - Sequential reasoning
    ├── Tree of Thoughts (ToT) - Exploration with backtracking
    ├── ReAct - Reasoning + Acting loops
    ├── Debate - Multi-agent argumentation
    └── Reflection - Self-improvement iterations
    ↓
Pattern Execution → Agent Coordination
    ↓
Results → Synthesis → Session Update → Response
```

## 📁 项目结构

```
go/orchestrator/
├── main.go                      # Service entry point
├── Dockerfile                   # Container build
├── internal/
│   ├── activities/              # Temporal activity implementations
│   │   ├── agent.go            # Agent execution activities
│   │   ├── budget.go           # Budget tracking activities
│   │   ├── decompose.go        # Task decomposition
│   │   ├── synthesis.go        # Result synthesis
│   │   └── metrics.go          # Pattern metrics tracking
│   ├── workflows/               # Temporal workflow definitions
│   │   ├── orchestrator_router.go  # Main pattern router
│   │   ├── supervisor_workflow.go  # Retry & supervision
│   │   ├── simple_workflow.go      # Simple task execution
│   │   ├── patterns/               # Cognitive patterns
│   │   │   ├── chain_of_thought.go
│   │   │   ├── tree_of_thoughts.go
│   │   │   ├── react.go
│   │   │   ├── debate.go
│   │   │   └── reflection.go
│   │   ├── strategies/            # Legacy workflow strategies
│   │   │   ├── dag.go
│   │   │   ├── exploratory.go
│   │   │   ├── research.go
│   │   │   └── scientific.go
│   │   └── execution/             # Execution patterns
│   │       ├── parallel.go
│   │       ├── sequential.go
│   │       └── hybrid.go
│   ├── server/                  # gRPC service implementation
│   ├── policy/                  # OPA policy engine integration
│   ├── budget/                  # Token budget management
│   ├── auth/                    # Authentication & authorization
│   ├── db/                      # PostgreSQL operations
│   ├── health/                  # Health checks & degradation
│   ├── config/                  # Configuration management
│   ├── streaming/               # SSE/WebSocket streaming
│   └── circuitbreaker/          # Failure protection
├── histories/                   # Workflow replay test files
├── tests/                       # Integration tests
│   └── replay/                  # Determinism testing
└── tools/replay/                # Temporal replay tooling
```

## 🚀 快速开始

### 前置条件
- Go 1.21+
- Docker 与 Docker Compose
- PostgreSQL、Redis、Temporal 已运行

### 开发

```bash
# Install dependencies
go mod download

# Run tests
go test -race ./...

# Build binary
go build -o orchestrator .

# Run locally (requires services)
./orchestrator
```

### Docker 部署

```bash
# Build image
docker build -t shannon-orchestrator .

# Run with compose (recommended)
make dev  # From repository root
```

## ⚙️ 配置

配置从 `/app/config/shannon.yaml` 加载（在 Docker 中挂载）：

```yaml
# Key configuration sections
service:
  port: 50052           # gRPC port
  health_port: 8081     # Health check HTTP port

policy:
  enabled: true         # OPA policy enforcement
  mode: "dry-run"      # off | dry-run | enforce
  path: "/app/config/opa/policies"

temporal:
  host_port: "temporal:7233"
  namespace: "default"
  task_queue: "shannon-task-queue"

patterns:
  chain_of_thought:
    max_iterations: 10
    timeout: "5m"
  tree_of_thoughts:
    max_depth: 5
    branching_factor: 3
  react:
    max_steps: 15
    timeout: "10m"

budget:
  max_tokens_per_request: 10000
  max_cost_per_request: 1.0
```

### 环境变量

- `PRIORITY_QUEUES`（默认：空）
  - 当设为 `on`/`true`/`1` 时，orchestrator 会为每个 priority queue 启动一个 Temporal worker：
    - `shannon-tasks-critical`、`shannon-tasks-high`、`shannon-tasks`（normal）、`shannon-tasks-low`
  - 每个 queue 的并发度在 `main.go` 中调整

- `ENABLE_TOOL_SELECTION`（默认：`1`）
  - 启用后，orchestrator 会调用 LLM 服务的 `/tools/select` 来自动填充 `context.tool_calls`
  - 当 `TOOL_PARALLELISM > 1` 时，这会使得 Agent Core 中的 tool 并行执行成为可能

- Priority worker 并发度（可选覆盖项）：
  - `WORKER_ACT_CRITICAL` / `WORKER_WF_CRITICAL`（默认：`12` / `12`）
  - `WORKER_ACT_HIGH` / `WORKER_WF_HIGH`（默认：`10` / `10`）
  - `WORKER_ACT_NORMAL` / `WORKER_WF_NORMAL`（默认：`8` / `8`）
  - `WORKER_ACT_LOW` / `WORKER_WF_LOW`（默认：`4` / `4`）

- 单 queue 模式下的并发度（当 `PRIORITY_QUEUES` 关闭时）：
  - `WORKER_ACT` / `WORKER_WF`（默认：`10` / `10`）

### 以优先级提交

在 `SubmitTaskRequest` 中通过 `metadata.labels["priority"]` 设置优先级。

有效值：`critical`、`high`、`normal`、`low`（不区分大小写）。无效值会回退到默认 queue。

示例：
```go
req := &pb.SubmitTaskRequest{
    Metadata: &common.TaskMetadata{
        UserId: "user-123",
        Labels: map[string]string{"priority": "critical"},
    },
    Query: "Plan and execute task",
}
resp, err := client.SubmitTask(ctx, req)
```

## 🔧 核心特性

### 基于 pattern 的 workflow

**认知 pattern：**
- `ChainOfThought` - 逐步的逻辑推理
- `TreeOfThoughts` - 通过回溯探索多条解法路径
- `ReAct` - 将推理与行动结合，适用于交互式 task
- `Debate` - 用于复杂决策的 multi-agent 论证
- `Reflection` - 迭代式自我改进

**核心 workflow：**
- `OrchestratorRouter` - 选择 pattern 的主入口
- `SupervisorWorkflow` - 处理 retry 与监督
- `SimpleWorkflow` - 简单 task 的直接执行

**关键 activity：**
- `DecomposeTask` - 分析复杂度并创建子 task
- `ExecuteAgent` - 运行单个 agent task
- `SynthesizeResults` - 汇总各 agent 的输出
- `UpdateSessionResult` - 持久化 session 状态
- `RecordPatternMetrics` - 跟踪 pattern 性能

### Budget 管理

token 用量在多个层级上进行跟踪：
- 单请求 budget，带 backpressure
- 单用户配额，带 circuit breaker
- 执行前进行成本估算
- 实时用量监控

### 策略执行

OPA 策略控制：
- Task 执行权限
- Agent 访问控制
- 资源用量限制
- 数据访问边界

### 健康检查与降级

负载下的自动降级：
- Complex → Standard 模式 fallback
- 针对外部服务的 circuit breaker
- 优雅的超时处理
- 健康检查端点在 `:8081/health`

## 📊 可观测性

### 指标（Prometheus 格式）
- **端点**：`:2112/metrics`
- Workflow 执行耗时
- Pattern 选择分布
- 每个 pattern 的 token 用量
- 按 workflow 类型划分的错误率

### 日志
- 使用 zap 的结构化 JSON 日志
- 用于请求追踪的 correlation ID
- 可通过 `LOG_LEVEL=debug` 开启 debug 模式

### 流式输出
- SSE：`GET /stream/sse?workflow_id=<id>`
- WebSocket：`GET /stream/ws?workflow_id=<id>`
- 说明：
  - 所有子 workflow 与 agent 的 event 都统一归到父 `workflow_id` 下，形成单个 stream。
  - SSE/WS event 缓存在 Redis Streams 中（TTL 约 24h），配置后会持久化到 Postgres。

#### Synthesis 事件
- `LLM_OUTPUT`（AgentID：`synthesis`）：最终综合后的内容（截断至 10k 字符）。在 LLM 成功时以及所有 fallback/simple 路径上发出。
- `DATA_PROCESSING` 摘要：轻量的 token 用量消息（例如 `"Used 1.5k tokens"`），基于 synthesis 上报的 model/token。
- 顺序：`LLM_OUTPUT` → 摘要（`DATA_PROCESSING` "Used … tokens"）→ `DATA_PROCESSING` "Answer ready" → `WORKFLOW_COMPLETED`。
- 旁路行为：当 synthesis 被旁路时（只有一个可用结果），不会发出额外的 synthesis event；agent 自身的 `LLM_OUTPUT` 即为最终结果。

## 🧪 测试

### 单元测试
```bash
go test ./internal/...
```

### 集成测试
```bash
# Requires running services
go test ./tests/integration/...
```

### 重放测试
```bash
# Export workflow history
make replay-export WORKFLOW_ID=task-xxx OUT=histories/test.json

# Test determinism
make replay HISTORY=histories/test.json

# Run all replay tests
go test ./tests/replay
```

## 🚨 常见问题

### Workflow 非确定性
- 确保 activity 中没有 `time.Sleep()`
- 在 workflow 中使用 `workflow.Sleep()`
- 以一致的名称注册所有 activity

### Budget 超限
- 检查配置中的 token 上限
- 通过指标监控用量
- 调整 `max_tokens_per_request`

### Pattern 选择
- 检查分解结果
- 查看 pattern 的置信度分数
- 监控 pattern 指标

## 📚 更多文档

- [Pattern 使用指南](../../docs/pattern-usage-guide.md)
- [Multi-Agent 架构](../../docs/multi-agent-workflow-architecture.md)
- [测试指南](../../docs/testing.md)
- [主 README](../../README.md)
