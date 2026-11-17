# CLAUDE.md

本文件为 Claude Code 在操作本仓库代码时提供指导。

## 项目概览

**Shannon** 是一个开源的企业级 multi-agent AI 平台，融合 Rust（agent 核心）、Go（基于 Temporal 的编排）和 Python（LLM 服务），构建出 token 高效、分布式的 AI 系统。

## 常用命令

```bash
# Setup & Run
make setup && vim .env && make dev         # Setup, configure API keys, start all
make smoke                                # E2E smoke tests
make proto                                # Regenerate proto files after .proto changes
make ci                                   # Run CI checks

# Local Docker Builds
# Pattern: docker build --no-cache -f <Dockerfile> -t <image> .
# Then: docker compose -f deploy/compose/docker-compose.yml up -d <service> --no-build
# Dockerfiles: go/orchestrator/Dockerfile, go/orchestrator/cmd/gateway/Dockerfile,
#              python/llm-service/Dockerfile, rust/agent-core/Dockerfile

# Unit Tests
cd rust/agent-core && cargo test
cd go/orchestrator && go test -race ./...
cd python/llm-service && python3 -m pytest
```

## 项目结构

- **`rust/agent-core/`**：执行管控 gateway、WASI sandbox、gRPC 服务端
- **`go/orchestrator/`**：Temporal workflow、budget 管理器、复杂度分析器
- **`python/llm-service/`**：LLM provider、MCP 工具、agent 循环
- **`protos/`**：共享的 protocol buffer 定义
- **`deploy/compose/`**：Docker Compose 配置
- **`config/`**：支持热重载的配置文件
- **`docs/`**：架构与 API 文档
- **`scripts/`**：自动化与辅助脚本

## API 入口（共 4 个）

| 入口 | 是否经过 Orchestrator？ | 格式 | 使用场景 |
|----------|:---:|--------|----------|
| `POST /v1/chat/completions` | 是 | OpenAI 兼容 | 使用 OpenAI SDK 的应用 —— 自动工具选择、深度研究、swarm、策略 |
| `POST /v1/completions` | 否（代理） | OpenAI 兼容 | 轻量 LLM 代理 —— 单次调用，无编排，仅使用调用方提供的工具 |
| `POST /api/v1/tasks` | 是 | Shannon 原生（同步） | 完整 orchestrator pipeline，同步响应 |
| `POST /api/v1/tasks/stream` | 是 | Shannon 原生（SSE） | 完整 orchestrator pipeline，以 SSE event 流式返回 |

`/v1/completions` **不**经过 orchestrator —— 没有工具选择、没有任务分解、没有策略。需要编排式 workflow 的应用必须使用 `/v1/chat/completions` 或 `/api/v1/tasks`。

### 工具执行 API

- `GET /api/v1/tools` —— 列出可用的 tool schema（排除危险工具）
- `GET /api/v1/tools/{name}` —— 获取工具元数据 + 参数 schema
- `POST /api/v1/tools/{name}/execute` —— 直接执行某个工具

危险工具（`bash_executor`、`file_write`）在 gateway 层被拦截 —— 它们只能在编排式 workflow 内部运行。

## 跳过认证配置

本地开发和测试时，需在两处关闭认证：

```bash
# 1. .env
GATEWAY_SKIP_AUTH=1  # 0=prod, 1=dev/test

# 2. config/shannon.yaml
auth:
  skip_auth: true    # false=prod, true=dev/test
```

修改后必须**重建**（而非重启）这两个服务 —— `restart` 不会重新读取 `env_file`：
```bash
docker compose -f deploy/compose/docker-compose.yml down && docker compose -f deploy/compose/docker-compose.yml up -d
```

Gateway 负责 HTTP 认证（读取 .env），Orchestrator 负责 gRPC 认证（读取 shannon.yaml）。两者必须保持一致。

快速验证：
```bash
curl -sS -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query":"test","session_id":"test-123"}'
```

## Workflow 类型与策略

### 核心 Workflow

| Workflow | 文件 | 触发条件 |
|----------|------|---------|
| **SimpleTaskWorkflow** | `simple_workflow.go` | 复杂度 < 0.3，单子任务 |
| **SupervisorWorkflow** | `supervisor_workflow.go` | 已禁用（router 中为 `case false:`）—— 所有多任务路由到 DAG |
| **StreamingWorkflow** | `streaming_workflow.go` | 流式模式 |
| **TemplateWorkflow** | `template_workflow.go` | 模板执行 |
| **SwarmWorkflow** | `swarm_workflow.go` | context 中 `force_swarm: true` |

### 策略 Workflow（位于 `strategies/`）

| 策略 | 文件 | 模式 |
|----------|------|---------|
| **DAGWorkflow** | `dag.go` | 所有多任务流程的默认模式（fan-out/fan-in） |
| **ReactWorkflow** | `react.go` | 推理循环 |
| **ResearchWorkflow** | `research.go` | React + 并行 |
| **ExploratoryWorkflow** | `exploratory.go` | Tree-of-Thoughts |
| **ScientificWorkflow** | `scientific.go` | 多模式组合 |
| **BrowserUseWorkflow** | `browser_use.go` | `context.role == "browser_use"` 或自动检测 |
| **DomainAnalysisWorkflow** | `domain_analysis_workflow.go` | 领域特定分析 |

## 关键实现规则

### Temporal Workflow
- 始终用 `.Get(ctx, &result)` 等待 activity 完成
- 在 workflow 中使用 `workflow.Sleep()`，在 activity 中绝不使用 `time.Sleep()`
- 保持 workflow 的确定性，以便进行 replay 测试
- 任何新增的代码路径都**必须**用 `workflow.GetVersion()` 保护

### Agent 昵称
始终使用 `agents/names.go` 中的 `agents.GetAgentName(workflowID, index)`。

### 数据库
- 任务表：`task_executions`
- 状态值：API/DB 中为全大写（用 `"COMPLETED"` 而非 `"completed"`）
- 空 UUID 字符串必须转为 NULL

### Proto/gRPC
- Rust 枚举：使用 `ExecutionMode::Simple`（而非 `ExecutionMode::ExecutionModeSimple`）
- 任何 `.proto` 变更后都要用 `make proto` 重新生成

### Proto 变更后的构建顺序

`make proto` -> `cd go/orchestrator && go mod tidy` -> `cd rust/agent-core && cargo build`

## 新增/修复 LLM Provider

唯一事实来源：`config/models.yaml`。需要同步改动的文件：
- Go 定价：`pricing/pricing.go`
- Go 检测：`models/provider.go`
- Python provider：`llm_provider/{name}_provider.py`
- Python 注册表：`providers/__init__.py`

## 综合模板

通过 `config/templates/synthesis/` 自定义最终答案格式：

| 方式 | Context 参数 | 使用场景 |
|--------|-------------------|----------|
| 具名模板 | `synthesis_template: "my_format"` | 可复用的 `.tmpl` 文件 |
| 原样覆盖 | `synthesis_template_override: "..."` | 一次性的自定义格式 |

## 环境陷阱

修改 `.env` 后，服务必须被**重建**（`docker compose down` 再 `up -d`），仅重启不够 —— `restart` 不会重新读取 `env_file`。

## 文档

- [Multi-Agent Workflow 架构](docs/multi-agent-workflow-architecture.md)
- [模式使用指南](docs/pattern-usage-guide.md)
- [流式 API](docs/streaming-api.md)
- [Token 与 Budget 跟踪](docs/token-budget-tracking.md)
- [扩展 Shannon](docs/extending-shannon.md)
- [Skills 系统](docs/skills-system.md)
- [Session 工作区](docs/session-workspaces.md)
- [Swarm Agents](docs/swarm-agents.md)
