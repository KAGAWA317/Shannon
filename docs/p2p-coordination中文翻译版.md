# Shannon 中的 P2P Agent 协同

## 概述

Shannon 现已支持 **Peer-to-Peer（P2P）Agent 协同**，使自主 agent 能够根据数据依赖关系协同执行 task。该特性允许 agent 在继续执行之前等待来自其他 agent 的所需数据，从而无需人工编排即可形成高效的 pipeline。

## 工作原理

### 1. 自动依赖检测
当你提交含有顺序步骤或依赖步骤的 query 时，Shannon 的分解服务（decomposition service）会自动：
- 识别每个子 task **产生**什么数据
- 确定每个子 task **消费**什么数据（需要从其他 task 获取）
- 检测到依赖时路由到 SupervisorWorkflow

### 2. 协同机制
Agent 使用基于 topic 的发布-订阅 pattern：
- **Producer agent** 将结果发布到语义 topic（例如 "analysis-results"、"metrics"）
- **Consumer agent** 在开始执行前等待所需 topic
- Workspace 存储（基于 Redis）促成 agent 之间的数据交换

### 3. Workflow 路由
系统自动选择合适的 workflow：
- **无依赖** → SimpleTaskWorkflow 或 DAGWorkflow（并行）
- **有依赖** → 带 P2P 协同的 SupervisorWorkflow
- **强制 P2P** → 始终使用 SupervisorWorkflow

## 配置

### 启用 P2P 协同

通过 `config/features.yaml` 配置：

```yaml
# config/features.yaml
workflows:
  p2p:
    enabled: true           # Master switch for P2P coordination
    timeout_seconds: 360    # Maximum wait time for dependencies
```

注意：P2P 不通过环境变量开关；orchestrator 会在运行时读取这些 YAML 设置。

## 使用示例

### 示例 1：顺序 pipeline
```python
# Query: "Analyze sales data and then create a report based on the analysis"

# Shannon automatically detects:
# - Task 1: Analyze → produces: ["sales-analysis", "insights"]
# - Task 2: Report → consumes: ["sales-analysis", "insights"]
#
# Task 2 waits for Task 1 to complete before starting
```

### 示例 2：复杂数据 pipeline
```python
# Query: "Load CSV, process the data, create visualizations, and generate PDF report"

# Dependency chain detected:
# - Load CSV → produces: ["raw-data"]
# - Process → consumes: ["raw-data"], produces: ["processed-data", "statistics"]
# - Visualize → consumes: ["statistics"], produces: ["charts"]
# - PDF → consumes: ["processed-data", "charts"]
```

### 示例 3：强制 P2P 模式
```python
# Force P2P coordination even for simple tasks
grpcurl -d '{
  "query": "What is 2+2?",
  "context": {"force_p2p": true}
}' localhost:50052 shannon.orchestrator.OrchestratorService/SubmitTask
```

## API 用法

### 通过 gRPC
```protobuf
// Normal usage - P2P activates automatically when needed
SubmitTaskRequest {
  query: "Analyze data then create report"
  context: {}
}

// Force P2P mode
SubmitTaskRequest {
  query: "Simple calculation"
  context: {
    "force_p2p": true
  }
}
```

### 通过 Python 客户端
```python
from shannon import ShannonClient

client = ShannonClient(base_url="http://localhost:8080")

# Automatic P2P for dependent tasks
response = client.submit_task(
    "Research the topic, validate findings, and write article",
    session_id="p2p-demo"
)

# Force P2P mode
response = client.submit_task(
    "Simple query",
    session_id="p2p-demo",
    context={"force_p2p": True}
)
```

## 依赖检测规则

系统基于以下方式检测依赖：

1. **顺序指示词**："then"、"after"、"based on"、"using the results"
2. **数据流分析**：每个 task 产生什么、需要什么
3. **Tool 输出**：使用 tool 的 task 会自动产生该 tool 的输出类型
4. **语义理解**：LLM 分析各个 task 的逻辑流程

## 优势

1. **自动编排**：无需手动指定 task 顺序
2. **高效执行**：依赖一旦满足，task 立即运行
3. **尽可能并行**：相互独立的 task 仍然并行运行
4. **稳健协同**：内置超时保护与错误处理
5. **透明**：日志会展示 P2P 协同决策

## 监控

在日志中检查 P2P 协同情况：

```bash
# View P2P detection in decomposition
docker compose logs llm-service | grep "P2P coordination detected"

# View workflow routing decisions
docker compose logs orchestrator | grep "SupervisorWorkflow"

# View dependency waiting
docker compose logs orchestrator | grep "Dependency wait"
```

## 架构细节

### 涉及组件

1. **LLM Service**：在分解过程中检测并填充 `produces`/`consumes` 字段
2. **Orchestrator Router**：将带依赖的 task 路由到 SupervisorWorkflow
3. **SupervisorWorkflow**：管理 P2P 协同与依赖等待
4. **Workspace Activities**：通过 Redis 处理数据交换

### 数据流

```
User Query → Decomposition (detects dependencies) → Router (selects workflow)
    ↓                                                   ↓
If dependencies exist                            SupervisorWorkflow
    ↓                                                   ↓
Agent 1 executes → Publishes to workspace → Agent 2 waits → Agent 2 executes
```

## 与其他框架对比

| 框架 | 协同方式 | Shannon 的优势 |
|-----------|-------------------|-------------------|
| LangGraph | 静态图边 | 动态依赖检测 |
| CrewAI | 基于角色的顺序 | 自动数据流分析 |
| AutoGen | 基于对话 | 带超时的结构化 P2P |
| OpenAI SDK | 无内置 P2P | 原生 P2P 基础设施 |

## 限制

- 最大超时可配置（默认 6 分钟）
- 当前不检测循环依赖（计划在未来支持）
- P2P 会为简单 task 增加开销（请谨慎使用 force 标志）

## 未来增强

计划中的改进：
- 循环依赖检测
- 跨 session 数据共享
- 基于优先级的 task 调度
- 动态超时调整
- 可视化依赖图生成

## 测试

运行 P2P 协同测试套件：

```bash
./tests/e2e/p2p_coordination_test.sh
```

该测试覆盖：
1. 依赖 task 的自动 P2P 激活
2. 强制 P2P 模式功能
3. 复杂 pipeline 协同

## 故障排查

### P2P 未激活
- 检查分解结果中是否填充了 `produces`/`consumes` 字段
- 确认配置中已启用 P2P
- 确保选中了 SupervisorWorkflow

### Task 未等待依赖
- 检查 workspace 连通性（Redis）
- 确认 producer/consumer 两端的 topic 名称一致
- 检查超时设置

### 强制 P2P 不生效
- 确保 context 中包含 `"force_p2p": true`
- 检查 orchestrator 日志中的 "forced" 消息
- 确认部署的是最新代码
