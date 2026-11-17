# Shannon Gateway API

Shannon 多 agent AI 平台的统一 HTTP gateway，为 orchestrator 的 gRPC 服务提供 REST API 访问。

## 功能特性

- **REST API** — 干净的 HTTP/JSON 接口，用于提交 task 和查询状态
- **身份认证** — 复用现有 auth 服务校验 API key
- **速率限制** — 按用户固定窗口限流（1 分钟窗口）
- **幂等性** — 支持带 `Idempotency-Key` header 的幂等请求
- **SSE 流式输出** — 通过 Server-Sent Events 实时推送 event
- **OpenAPI 规范** — 带 OpenAPI 3.0 规范的自描述 API
- **分布式追踪** — 传播 trace context 以便调试

## 快速开始

**Docker Compose（无需认证！）**：Docker Compose 配置默认 `GATEWAY_SKIP_AUTH=1`，方便开源用户上手。

**本地构建**：需要设置 `GATEWAY_SKIP_AUTH=1` 环境变量，或在 `config/features.yaml` 中配置 `gateway.skip_auth: true`。

### 使用 Docker Compose 运行

```bash
# Build and start all services (auth disabled by default for easy start)
docker compose -f deploy/compose/docker-compose.yml up -d

# Submit your first task - no API key needed!
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{"query":"What is 2+2?"}'

# Check gateway health
curl http://localhost:8080/health

# View OpenAPI specification
curl http://localhost:8080/openapi.json | jq
```

### API 端点

#### 公开端点（无需认证）

- `GET /health` — 健康检查
- `GET /readiness` — 就绪探针
- `GET /openapi.json` — OpenAPI 规范

#### 需认证端点

**Task 管理：**
- `POST /api/v1/tasks` — 提交新 task
- `POST /api/v1/tasks/stream` — 提交 task 并返回 stream URL（201）
- `GET /api/v1/tasks` — 列出 task（limit、offset、status、session_id）
- `GET /api/v1/tasks/{id}` — 查询 task 状态（含 query/session_id/mode）
- `POST /api/v1/tasks/{id}/cancel` — 取消运行中或排队中的 task
- `GET /api/v1/tasks/{id}/events` — 获取已持久化的 event 历史（来自 Postgres）
- `GET /api/v1/tasks/{id}/timeline` — 从 Temporal history 构建人类可读的 timeline（summary/full、persist）
- `GET /api/v1/tasks/{id}/stream` — 重定向到该 task 的 SSE stream
- `GET /api/v1/stream/sse?workflow_id={id}` — 流式推送 task event（SSE）
- `GET /api/v1/stream/ws?workflow_id={id}` — WebSocket stream

**审批管理：**
- `POST /api/v1/approvals/decision` — 为需要人工审批的 workflow 提交审批决定

**Session 管理：**
- `GET /api/v1/sessions` — 分页列出 session（limit、offset）
- `GET /api/v1/sessions/{sessionId}` — 获取 session 详情
- `GET /api/v1/sessions/{sessionId}/history` — 获取对话历史
- `GET /api/v1/sessions/{sessionId}/events` — 获取 session event（不含 LLM_PARTIAL）

##### Task 列表

```bash
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/tasks?limit=20&offset=0&status=COMPLETED"
```

##### Event 历史（持久化）

```bash
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/tasks/$TASK_ID/events?limit=200"
```

##### 确定性 Timeline（Temporal 回放）

```bash
# Persist derived timeline (async, 202)
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/tasks/$TASK_ID/timeline?mode=summary&persist=true"

# Preview timeline only (no DB writes, 200)
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/tasks/$TASK_ID/timeline?mode=full&include_payloads=false&persist=false" | jq
```

##### Session 管理

```bash
# List all sessions for a user
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/sessions?limit=20&offset=0"

# Get session details
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/sessions/$SESSION_ID" | jq

# Get conversation history for a session
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/sessions/$SESSION_ID/history" | jq

# Get session events (excludes partial LLM responses)
curl -s -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/sessions/$SESSION_ID/events?limit=100" | jq
```

说明：
- SSE event 存在 Redis Streams 中（TTL 约 24 小时），供实时查看。
- 持久化 event 历史存在 Postgres 的 `event_logs` 表中。
- Timeline API 从 Temporal 的权威 event 存储中推导出人类可读的历史，当 `persist=true` 时异步持久化。

### 身份认证

**🚀 Docker Compose 默认行为**：Docker Compose 中认证**默认关闭**（`GATEWAY_SKIP_AUTH=1`），方便快速上手。

**本地构建**：认证默认开启（`config/features.yaml` 中为 `gateway.skip_auth: false`）。设置 `GATEWAY_SKIP_AUTH=1` 可关闭。

**生产环境**：设置 `GATEWAY_SKIP_AUTH=0` 并配合 API key 启用认证：

```bash
# Enable authentication for production
export GATEWAY_SKIP_AUTH=0
docker compose up -d gateway

# Then use API keys via header (recommended)
curl -H "X-API-Key: sk_test_123456" http://localhost:8080/api/v1/tasks

# For SSE connections, also use headers (query params are not accepted)
curl -N -H "X-API-Key: sk_test_123456" \
  "http://localhost:8080/api/v1/stream/sse?workflow_id=xxx"
```

### Task 提交

```bash
# Default (no auth required in Docker Compose)
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is 2+2?",
    "session_id": "optional-session-id"
  }'

# With authentication enabled (production)
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_test_123456" \
  -d '{"query": "What is 2+2?"}'
```

### Strategy 预设

通过预设和覆盖参数控制研究行为（由 gateway 映射进 context）：

```bash
# Quick strategy
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is quantum computing?",
    "research_strategy": "quick"
  }'

# Deep strategy with override
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Compare LangChain and AutoGen frameworks",
    "research_strategy": "deep",
    "max_iterations": 12
  }'

# Academic strategy
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Latest research on transformer architectures",
    "research_strategy": "academic"
  }'
```

响应：
```json
{
  "task_id": "task-00000000-0000-0000-0000-000000000001",
  "status": "QUEUED",
  "created_at": "2025-01-20T10:00:00Z"
}
```

### 提交并获取 Stream URL（推荐的开发者体验）

这个便捷端点一次调用即可返回可直接使用的 SSE URL。

```bash
curl -s -X POST http://localhost:8080/api/v1/tasks/stream \
  -H "Content-Type: application/json" \
  -d '{"query": "What is 2+2?"}' | jq
```

响应（201 Created）：
```json
{
  "workflow_id": "task-...",
  "task_id": "task-...",
  "stream_url": "/api/v1/stream/sse?workflow_id=task-..."
}
```

然后用 EventSource 连接：
```js
const r = await fetch('/api/v1/tasks/stream', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ query: 'What is 2+2?' })});
const { stream_url } = await r.json();
const es = new EventSource(stream_url);
```

### 幂等性

用 `Idempotency-Key` header 防止重复提交：

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -H "X-API-Key: sk_test_123456" \
  -H "Idempotency-Key: unique-request-id-123" \
  -d '{"query": "Process this once"}'
```

### 速率限制

gateway 按用户执行固定窗口限流：

- 默认：每分钟 60 个请求（1 分钟固定窗口）
- 按 user ID 计数（不是按 API key）
- 返回的 header：`X-RateLimit-Limit`、`X-RateLimit-Remaining`、`X-RateLimit-Reset`
- 超限时：HTTP 429 并带 `Retry-After` header

### SSE 流式输出

流式推送某个 task 的实时 event（支持可选过滤与断点续传）：

```bash
# Unfiltered
curl -N -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/stream/sse?workflow_id={task_id}"

# Filter by event types
curl -N -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/stream/sse?workflow_id={task_id}&types=LLM_OUTPUT,WORKFLOW_COMPLETED"

# Resume using Last-Event-ID (Redis stream ID or numeric seq)
curl -N -H "X-API-Key: $API_KEY" \
  "http://localhost:8080/api/v1/stream/sse?workflow_id={task_id}&last_event_id=1700000000000-0"
```

说明：
- gateway 会透传过滤条件并接受任意 event 类型；未知类型只是不会产生 event。
- `last_event_id` 同时支持 Redis stream ID 和数字序列号。

### 环境变量

| 变量 | 默认值 | 说明 |
|----------|---------|-------------|
| `PORT` | `8080` | HTTP 服务端口 |
| `ORCHESTRATOR_GRPC` | `orchestrator:50052` | Orchestrator gRPC 地址 |
| `ADMIN_SERVER` | `http://orchestrator:8081` | 用于 SSE 代理的 admin server |
| `POSTGRES_HOST` | `postgres` | PostgreSQL 主机 |
| `POSTGRES_PORT` | `5432` | PostgreSQL 端口 |
| `POSTGRES_USER` | `shannon` | 数据库用户 |
| `POSTGRES_PASSWORD` | `shannon` | 数据库密码 |
| `POSTGRES_DB` | `shannon` | 数据库名 |
| `REDIS_URL` | `redis://redis:6379` | 用于速率限制的 Redis URL |
| `JWT_SECRET` | `your-secret-key` | JWT 签名密钥 |
| `GATEWAY_SKIP_AUTH` | `0`（仓库）、`1`（Compose） | 跳过认证（1=关闭，便于快速上手，0=开启，用于生产） |

## 开发

### 本地构建

```bash
cd go/orchestrator
go build -o gateway cmd/gateway/main.go
./gateway
```

### 运行测试

```bash
# Unit tests
cd go/orchestrator
go test ./cmd/gateway/...

# Full E2E smoke test (includes gateway)
make smoke
```

### 新增端点

1. 在 `cmd/gateway/internal/handlers/` 中添加 handler
2. 在 `cmd/gateway/main.go` 中注册路由
3. 更新 `cmd/gateway/internal/handlers/openapi.go` 中的 OpenAPI 规范
4. 在 `cmd/gateway/internal/handlers/` 中添加测试

## 架构

gateway 充当一层薄薄的 HTTP 转换层：

```
Client → Gateway (cmd/gateway/main.go)
         ↓
         ├─→ Orchestrator gRPC (tasks, workflows)
         ├─→ Postgres (sessions, events, auth)
         ├─→ Redis (rate limiting, idempotency)
         └─→ Admin Server (SSE/WebSocket proxy)
```

关键设计决策：
- 作为独立二进制放在 `go/orchestrator/cmd/gateway/`
- gateway 专属的 handler 和 middleware 放在 `cmd/gateway/internal/`
- 直接函数调用 orchestrator 内部 auth 服务（不单独走 auth RPC）
- 手写 HTTP handler 以获得精确控制（不用 grpc-gateway 自动生成）
- 对 SSE/WebSocket 使用反向代理，以复用现有 admin server 的流式基础设施

## 监控

### 健康检查

```bash
# Basic health
curl http://localhost:8080/health

# Readiness (checks orchestrator connection)
curl http://localhost:8080/readiness
```

### 指标

gateway 会记录所有请求的日志并带 trace ID。查看日志：

```bash
docker compose logs -f gateway
```

### 追踪

gateway 通过以下方式传播 trace context：
- `traceparent` header（W3C Trace Context）
- `X-Trace-ID` header（自定义）
- 响应中的 `X-Workflow-ID` header

## 故障排查

### Gateway 无法启动

检查 orchestrator 是否在运行：
```bash
docker compose ps orchestrator
curl http://localhost:50052  # Should fail but shows connectivity
```

### 认证失败

确认 API key 存在于数据库中：
```bash
docker compose exec postgres psql -U shannon -d shannon \
  -c "SELECT * FROM auth.api_keys WHERE key_hash = encode(digest('sk_test_123456', 'sha256'), 'hex');"
```

### 速率限制问题

检查 Redis 连通性：
```bash
docker compose exec redis redis-cli ping
```

清除某个用户的速率限制：
```bash
# Keys are per user and window (ratelimit:user:<USER_ID>:<window_unix>)
docker compose exec redis sh -lc 'for k in \$(redis-cli --scan --pattern "ratelimit:user:YOUR_USER_ID:*"); do redis-cli DEL "$k"; done'
```

## 安全

- API key 使用现有 auth 服务校验
- 速率限制防止滥用
- 幂等 key 防止重放攻击
- 开发环境的 CORS header（生产环境需另行配置）
- 所有数据库查询使用参数化查询
- 密钥永不写入日志

## 许可证

Copyright (c) 2025 Shannon AI Platform
