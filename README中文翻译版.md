# Shannon — 真正能落地的生产级 AI Agent

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Documentation](https://img.shields.io/badge/docs-shannon.run-blue.svg)](https://docs.shannon.run)
[![Docker Hub](https://img.shields.io/badge/Docker%20Hub-waylandzhang%2Fshannon-blue.svg)](https://hub.docker.com/u/waylandzhang)
[![Go Version](https://img.shields.io/badge/Go-1.24%2B-blue.svg)](https://golang.org/)
[![Rust](https://img.shields.io/badge/Rust-stable-orange.svg)](https://www.rust-lang.org/)
[![PRs Welcome](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](CONTRIBUTING.md)

把可靠的 AI agent 交付到生产环境。多策略编排、swarm 协作、token 预算控制、人工审批 workflow，以及时间旅行调试 —— 全部内置。**<a href="https://shannon.run" target="_blank">在线演示</a>**

<div align="center">

![Shannon Desktop App](docs/images/desktop-demo.gif)

*实时查看 agent 执行过程与事件流*

</div>

<div align="center">

![Shannon Architecture](docs/images/architecture-oss.png)

*多 agent 编排，包含执行策略、WASI 沙箱与内置可观测性*

</div>

## 为什么选 Shannon？

| 痛点 | Shannon 的解法 |
|---|---|
| *agent 会静默失败？* | 带时间旅行调试的 Temporal workflow —— 任意一次执行都能逐步重放 |
| *成本失控？* | 按 task/agent 设置硬性 token 预算，并自动回退到更便宜的模型 |
| *对执行过程完全看不见？* | 实时事件流、Prometheus 指标、OpenTelemetry 链路追踪 |
| *担心安全？* | 代码执行跑在 WASI sandbox 中，配合 OPA 策略与多租户隔离 |
| *被厂商锁定？* | 支持 OpenAI、Anthropic、Google、DeepSeek、xAI，以及通过 Ollama 运行的本地模型 |

## 快速开始

### 前置条件

- Docker 与 Docker Compose
- 至少一个 LLM provider 的 API Key（OpenAI、Anthropic 等）

### 一条命令安装

```bash
curl -fsSL https://raw.githubusercontent.com/Kocoro-lab/Shannon/main/scripts/install.sh | bash
```

该脚本会下载配置、提示输入 API Key、拉取 Docker 镜像并启动服务。

**必需的 API Key**（任选其一）：
- OpenAI：`OPENAI_API_KEY=sk-...`
- Anthropic：`ANTHROPIC_API_KEY=sk-ant-...`
- 或任何 OpenAI 兼容端点

**可选但推荐：**
- Web 搜索：`SERPAPI_API_KEY=...`（[serpapi.com](https://serpapi.com)）
- Web 抓取：`FIRECRAWL_API_KEY=...`（[firecrawl.dev](https://firecrawl.dev)）

> **想从源码构建？** 见下方 [开发](#开发)。
>
> **各平台专属指南：** [Ubuntu](docs/ubuntu-quickstart.md) | [Rocky Linux](docs/rocky-linux-quickstart.md) | [Windows](docs/windows-setup-guide-en.md) | [Windows (中文)](docs/windows-setup-guide-cn.md)

### 你的第一个 Agent

Shannon 提供了多种与 AI agent 交互的方式：

#### REST API

```bash
# Submit a task
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query": "What is the capital of France?", "session_id": "demo"}'

# Stream events in real-time
curl -N "http://localhost:8080/api/v1/stream/sse?workflow_id=<task_id>"
```

#### Python SDK

```bash
pip install shannon-sdk
```

```python
from shannon import ShannonClient

with ShannonClient(base_url="http://localhost:8080") as client:
    handle = client.submit_task("What is the capital of France?", session_id="demo")
    result = client.wait(handle.task_id)
    print(result.result)
```

参见 [Python SDK 文档](https://pypi.org/project/shannon-sdk/)。

#### OpenAI 兼容 API

```bash
# Drop-in replacement for OpenAI API
export OPENAI_API_BASE=http://localhost:8080/v1
# Your existing OpenAI code works unchanged
```

#### 桌面应用

从 [GitHub Releases](https://github.com/Kocoro-lab/Shannon/releases/latest) 下载（macOS、Windows、Linux），或从源码构建：

```bash
cd desktop && npm install && npm run tauri:build
```

参见 [桌面应用指南](desktop/README.md)。

## 架构

```
Client --> Gateway (Go) --> Orchestrator (Go) --> Agent Core (Rust) --> LLM Service (Python) --> Providers
             |                    |                      |                      |
             | Auth/Rate limit    | Temporal workflows    | WASI sandbox         | Tool execution
             |                    | Budget management     | Token enforcement    | Agent loop
             |                    | Complexity routing    | Circuit breaker      | Context management
             v                    v                       v                      v
           PostgreSQL          Temporal                 Redis              Tool Adapters
```

### 核心服务

| 服务 | 语言 | 端口 | 职责 |
|---------|----------|------|------|
| **Gateway** | Go | 8080 | REST API、认证（JWT/API Key）、限流 |
| **Orchestrator** | Go | 50052 | Temporal workflow、task 拆解、预算管理 |
| **Agent Core** | Rust | 50051 | 执行网关（enforcement gateway）、WASI sandbox、token 计数 |
| **LLM Service** | Python | 8000 | provider 抽象、MCP tool、agent 循环 |
| **Playwright** | Python | 8002 | 用于网页抓取的浏览器自动化 |

### 执行策略

task 会根据复杂度自动路由：

| 策略 | 触发条件 | 适用场景 |
|----------|---------|----------|
| **Simple** | 复杂度 < 0.3 | 单 agent 直接响应 |
| **DAG** | 多步骤 task（默认） | 带依赖追踪的扇出/扇入 |
| **ReAct** | 迭代式推理 | 推理 + tool 调用循环 |
| **Research** | 多步骤研究 | 分层模型优化成本（降低 50-70%） |
| **Exploratory** | 思维树（Tree-of-Thoughts） | 并行假设探索 |
| **Browser Use** | Web 交互类 task | 由 Playwright 支撑的浏览 agent |
| **Domain Analysis** | 专项分析 | 领域特定的深度研究 |
| **Swarm** | 自治团队 | 由 lead 编排、带收敛检测的多 agent 协作 |

## 核心能力

### 研究工作流
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Compare renewable energy adoption in EU vs US",
    "context": {"force_research": true, "research_strategy": "deep"}
  }'
```

### Swarm 多 Agent
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Analyze this dataset from multiple perspectives",
    "context": {"force_swarm": true}
  }'
```

### Skills 系统
```bash
curl http://localhost:8080/api/v1/skills           # List skills
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query": "Review the auth module", "skill": "code-review", "session_id": "review-123"}'
```

在 `config/skills/user/` 下创建自定义 skill（目录不存在就新建 —— 该目录已被 gitignore）。参见 [Skills 系统](docs/skills-system.md)。

### 人在回路中的审批（Human-in-the-Loop）

通过 OPA 策略，或使用带 `require_approval: true` 的 workflow 模板来启用审批关卡。审批请求会经由 WebSocket 路由到已连接的 daemon 客户端。

```bash
# Submit an approval decision
curl -X POST http://localhost:8080/api/v1/approvals/decision \
  -H "Content-Type: application/json" \
  -d '{"workflow_id": "<workflow_id>", "approval_id": "<approval_id>", "approved": true}'
```

### Session 连续性
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query": "What is GDP?", "session_id": "econ-101"}'
# Follow-up remembers context
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query": "How does it relate to inflation?", "session_id": "econ-101"}'
```

### 定时任务
```bash
curl -X POST http://localhost:8080/api/v1/schedules \
  -H "Content-Type: application/json" \
  -d '{"name": "Daily Analysis", "cron_expression": "0 9 * * *", "task_query": "Analyze market trends"}'
```

### Token 预算控制
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Generate a market report",
    "context": {"budget_max": 5000}
  }'
```

### 时间旅行调试
```bash
./scripts/replay_workflow.sh task-prod-failure-123
```

### 10+ 个 LLM Provider
- **Anthropic**：Claude Opus 4.6、Sonnet 4.6、Haiku 4.5
- **OpenAI**：GPT-5.1、GPT-5 mini、GPT-5 nano
- **Google**：Gemini 2.5 Pro、Gemini 2.5 Flash、Gemini 3 Pro Preview
- **xAI**：Grok 4（推理与非推理）、Grok 3 Mini
- **DeepSeek**：DeepSeek Chat、DeepSeek Reasoner
- **MiniMax**：M3、M2.7、M2.7-highspeed
- **Groq**：Llama、Mixtral（极速推理）
- **其他**：Qwen、Meta（Llama 4）、Zhipu（GLM）、Kimi
- **本地**：Ollama、LM Studio、vLLM —— 任何 OpenAI 兼容端点
- provider 之间自动故障转移

## API 端点

| 端点 | 走 Orchestrator？ | 格式 | 适用场景 |
|----------|:---:|--------|----------|
| `POST /v1/chat/completions` | 是 | OpenAI 兼容 | 使用 OpenAI SDK 的应用 |
| `POST /v1/completions` | 否（代理） | OpenAI 兼容 | 轻量 LLM 代理 |
| `POST /api/v1/tasks` | 是 | Shannon 原生（同步） | 完整的 orchestrator 流水线 |
| `POST /api/v1/tasks/stream` | 是 | Shannon 原生（SSE） | 流式编排 |

**Tool 执行：** `GET /api/v1/tools`、`POST /api/v1/tools/{name}/execute`

**认证：** `GET /api/v1/auth/me`、`POST /api/v1/auth/refresh-key`、`GET/POST/DELETE /api/v1/auth/api-keys`

**Session：** `GET/PATCH/DELETE /api/v1/sessions/{id}`、历史记录、事件、文件

**Task 控制：** `POST /api/v1/tasks/{id}/cancel|pause|resume`、`GET .../control-state|events|timeline`

**定时任务：** `/api/v1/schedules` 下提供完整 CRUD，外加 `pause`、`resume`、`runs`

**实时流：** `GET /api/v1/stream/sse`、`GET /api/v1/stream/ws`、`WS /v1/ws/messages`（daemon）

## 项目结构

```
shannon/
├── go/orchestrator/          # Temporal workflows, budget manager, gateway
│   ├── cmd/gateway/          # REST API gateway (auth, rate limiting)
│   └── internal/             # Workflows, strategies, activities
├── rust/agent-core/          # Enforcement gateway, WASI sandbox
├── python/llm-service/       # LLM providers, MCP tools, agent loop
├── desktop/                  # Tauri + Next.js desktop application
├── clients/python/           # Python SDK
├── protos/                   # Shared protobuf definitions
├── config/                   # YAML configuration files
├── deploy/compose/           # Docker Compose for local dev + release
├── migrations/               # PostgreSQL schema migrations
├── scripts/                  # Automation and helper scripts
└── docs/                     # Architecture and API documentation
```

### 配置

| 文件 | 用途 |
|------|---------|
| `.env` | API Key、运行时设置 |
| `config/shannon.yaml` | 功能开关、认证、追踪 |
| `config/models.yaml` | LLM provider、定价、能力 |
| `config/features.yaml` | Workflow 设置、执行模式 |
| `config/openai_models.yaml` | 为 OpenAI 兼容 API 自定义的 `shannon-*` 模型名 |
| `config/research_strategies.yaml` | 研究策略的模型分层 |

### 服务端口（本地开发）

| 服务 | 端口 | 协议 |
|---------|------|----------|
| Gateway | 8080 | HTTP |
| Orchestrator | 50052 (gRPC), 8081 (health) | gRPC/HTTP |
| Agent Core | 50051 | gRPC |
| LLM Service | 8000 | HTTP |
| Temporal | 7233 (gRPC), 8088 (UI) | gRPC/HTTP |
| PostgreSQL | 5432 | TCP |
| Redis | 6379 | TCP |

## 开发

### 从源码构建

```bash
git clone https://github.com/Kocoro-lab/Shannon.git
cd Shannon
make setup                              # Create .env + generate protobuf stubs
vim .env                                # Add your API key
./scripts/setup_python_wasi.sh          # Download Python WASI interpreter
make dev                                # Start all services
make smoke                              # Run E2E smoke tests
```

### 开发命令

```bash
make dev      # Start all services
make smoke    # E2E smoke tests
make ci       # Full CI suite
make proto    # Regenerate protobuf files
make lint     # Run linters
make logs     # View service logs
make down     # Stop all services
```

### 浏览器自动化（可选）

Playwright 服务用于支撑浏览器自动化 workflow（镜像 3.4GB，内含 Chromium）。它**默认不启动**。启用方式：

```bash
# Development
docker compose -f deploy/compose/docker-compose.yml --profile browser up -d

# Pre-built images
docker compose -f deploy/compose/docker-compose.release.yml --profile browser up -d
```

测试一下：
```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query":"Go to https://waylandz.com and get the page title","session_id":"test","context":{"role":"browser_use"}}'
```

### 使用预构建镜像

```bash
cd Shannon
cp .env.example .env && vim .env
docker compose -f deploy/compose/docker-compose.release.yml up -d
```

## 故障排查

### 健康检查

```bash
docker compose -f deploy/compose/docker-compose.yml ps
curl http://localhost:8080/health
curl http://localhost:8081/health
```

### 常见问题

**服务起不来：**
- 检查 `.env` 中是否填了必需的 API Key
- 确认 8080、8081、50052 端口未被占用
- 执行 `docker compose down && docker compose up -d` 重新创建

**Task 执行失败：**
- 确认 LLM API Key 有效
- 查看 orchestrator 日志：`docker compose logs -f orchestrator`

**内存不足：**
- 调低 `WASI_MEMORY_LIMIT_MB`（默认：512）
- 调低 `HISTORY_WINDOW_MESSAGES`（默认：50）

## 常见问答

### Shannon 是什么？

Shannon 是一个用 Go、Rust、Python 构建的**面向生产环境的多 agent 编排框架**。它的重点是可靠性：用 Temporal workflow 实现容错、时间旅行调试、token 预算控制、人工审批 workflow，以及用于安全执行代码的 WASI sandbox。

### Shannon 与 LangChain 或 CrewAI 有何不同？

| 框架 | 关注点 | 关键差异 |
|-----------|-------|-------------------|
| **Shannon** | 生产可靠性 | Temporal workflow、时间旅行调试、硬性 token 预算 |
| **LangChain** | LLM 链与 RAG | 思维链组合、检索增强 |
| **CrewAI** | 角色扮演型 agent | 多 agent 角色、task 委派 |

Shannon 面向**生产部署**设计，内置可观测性、预算管理与 workflow 重放 —— 不只是做原型。

### 有哪些可用的执行策略？

Shannon 会根据复杂度自动路由 task：

- **Simple**：单 agent 直接响应（复杂度 < 0.3）
- **DAG**：带依赖追踪的扇出/扇入（多步骤场景的默认选项）
- **ReAct**：推理 + tool 调用循环
- **Research**：分层模型，成本降低 50-70%
- **Swarm**：由 lead 编排的多 agent 团队
- **Browser Use**：由 Playwright 支撑的 Web 交互

### 支持哪些 LLM provider？

Shannon 支持 10+ 个 provider，并带自动故障转移：

- **Anthropic**：Claude Opus 4.6、Sonnet 4.6、Haiku 4.5
- **OpenAI**：GPT-5.1、GPT-5 mini、GPT-5 nano
- **Google**：Gemini 2.5 Pro、Gemini 2.5 Flash
- **xAI**：Grok 4.1（推理与非推理）
- **DeepSeek**、**MiniMax**、**Qwen**、**Kimi**
- **本地**：Ollama、LM Studio、vLLM —— 任何 OpenAI 兼容端点

### 如何安装 Shannon？

**一条命令安装：**
```bash
curl -fsSL https://raw.githubusercontent.com/Kocoro-lab/Shannon/main/scripts/install.sh | bash
```

**前置条件：**
- Docker 与 Docker Compose
- 至少一个 LLM provider 的 API Key

### 什么是时间旅行调试？

时间旅行调试让你逐步重放任意一次 workflow 执行，从而弄清失败原因或异常行为：
```bash
./scripts/replay_workflow.sh task-prod-failure-123
```

### token 预算控制是怎么工作的？

在每次 task 的 context 参数里设置 `budget_max`。Shannon 会强制执行硬性 token 上限，并在预算接近耗尽时自动回退到更便宜的模型。

### 可以在现有的 OpenAI SDK 中使用 Shannon 吗？

可以！Shannon 提供了 OpenAI 兼容的 API 端点：
```bash
export OPENAI_API_BASE=http://localhost:8080/v1
# Your existing OpenAI code works unchanged
```

### 在哪里可以获得帮助？

- **文档**：[docs.shannon.run](https://docs.shannon.run)
- **GitHub Issues**：[github.com/Kocoro-lab/Shannon/issues](https://github.com/Kocoro-lab/Shannon/issues)
- **在线演示**：[shannon.run](https://shannon.run)

## 文档

| 资源 | 说明 |
|----------|-------------|
| [官方文档](https://docs.shannon.run) | 完整文档站点 |
| [架构](docs/multi-agent-workflow-architecture.md) | 系统设计深入解读 |
| [流式 API](docs/streaming-api.md) | SSE 与 WebSocket 流式传输 |
| [Skills 系统](docs/skills-system.md) | 自定义 skill 开发 |
| [Session 工作区](docs/session-workspaces.md) | WASI sandbox 指南 |
| [扩展 Shannon](docs/extending-shannon.md) | 自定义 tool 与模板 |
| [Swarm Agents](docs/swarm-agents.md) | 多 agent 协作 |

### 延伸阅读

以下偏实现细节的章节解释了 Shannon 在 MCP、skill、路由与 prompt 缓存方面的设计取舍：

- [MCP 协议深入解析](https://waylandz.com/ai-agent-book-en/chapter-04-mcp-protocol-deep-dive/) —— 客户端/服务端角色、tool 发现与安全边界。
- [Skills 系统](https://waylandz.com/ai-agent-book-en/chapter-05-skills-system/) —— 可复用的 skill、发现机制与受限的 tool 暴露。
- [分层模型策略](https://waylandz.com/ai-agent-book-en/chapter-31-tiered-model-strategy/) —— 兼顾能力与成本的模型路由。
- [Prompt 缓存稳定性](https://waylandz.com/ai-agent-book-en/chapter-39-prompt-cache-stability/) —— 稳定的请求前缀与缓存高效的 agent 执行。

## 贡献

欢迎贡献！规范见 [CONTRIBUTING.md](CONTRIBUTING.md)。

- [提交 issue](https://github.com/Kocoro-lab/Shannon/issues)
- [查看路线图](ROADMAP.md)

## 许可协议

MIT 许可 —— 随处可用、随意修改。见 [LICENSE](LICENSE)。

---

<p align="center">
  <b>别再调试 AI 故障了，开始交付可靠的 agent。</b><br><br>
  <a href="https://github.com/Kocoro-lab/Shannon">GitHub</a> ·
  <a href="https://docs.shannon.run">Docs</a>
</p>
