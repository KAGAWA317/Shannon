# Shannon Agent Core - 架构文档

## 概览

Shannon Agent Core 是 agent 执行层的高性能 Rust 实现，提供安全的 sandbox、高效的内存管理和智能的 tool 编排。本文档描述遵循 2025 最佳实践的现代化架构。

## 架构原则

1. **关注点分离**：智能（Python）与执行（Rust）
2. **零拷贝操作**：最小化字符串克隆和内存分配
3. **现代并发**：使用 `OnceLock` 和 `std::sync::Once` 替代 `lazy_static`
4. **全面的错误处理**：通过 `thiserror` 提供结构化错误的 `Result<T>` 类型
5. **可观测系统**：OpenTelemetry tracing 与 Prometheus metrics
6. **安全优先**：为不可信代码执行提供 WASI sandbox

## 组件架构

```
┌─────────────────────────────────────────────────────────────┐
│                     gRPC Server (port 50051)                │
├─────────────────────────────────────────────────────────────┤
│                    Enforcement Gateway                     │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────┐ │
│  │  Timeouts    │  │  Rate Limits │  │ Circuit Breakers │ │
│  └──────────────┘  └──────────────┘  └──────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Tool Execution Layer                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │   Tool   │  │   Tool   │  │   Tool   │  │   WASI   │  │
│  │ Registry │  │   Cache  │  │ Executor │  │  Sandbox │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
├─────────────────────────────────────────────────────────────┤
│                    Infrastructure Layer                     │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Memory  │  │  Config  │  │ Tracing  │  │ Metrics  │  │
│  │   Pool   │  │  Manager │  │  (OTEL)  │  │  (Prom)  │  │
│  └──────────┘  └──────────┘  └──────────┘  └──────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## 核心组件

### 1. Enforcement Gateway（`src/enforcement.rs`）

为所有代码路径提供统一的逐请求策略执行：

- Timeouts：每个请求的硬性墙上时钟上限
- Token 上限：拒绝预估 token 数超限的请求
- 限流：简单的按 key token 桶
- 熔断器：按 key 的滚动错误窗口
  - 可选的分布式限流器：设置 `ENFORCE_RATE_REDIS_URL` 即可启用基于 Redis、跨实例共享的 token 桶。

配置位于 `config/agent.yaml` 的 `enforcement` 之下，并支持环境变量覆盖（`ENFORCE_*`）。

### 2. Tool 系统

#### Tool Registry（`src/tool_registry.rs`）
- 集中化的 tool 能力管理
- 带过滤与相关性打分的发现 API
- 包含 schema、权限和 TTL 的元数据

#### Tool Cache（`src/tool_cache.rs`）
- 可配置 TTL 的 LRU 缓存
- 确定性的 cache key 生成
- 自动过期与清扫
- 全面的统计跟踪

#### Tool Executor（`src/tools.rs`）
- 统一的 tool 执行接口
- 与 Python LLM 服务集成
- 代码执行的 WASI sandbox 路由
- 自动的结果缓存

### 3. WASI Sandbox（`src/wasi_sandbox.rs`）

安全的 WebAssembly 执行环境，具备：
- 文件系统隔离（只读访问 `/tmp`）
- 内存限制（可配置，默认 256MB）
- 执行超时（默认 30s）
- 用于控制 CPU 用量的 fuel 计量

### 4. 内存管理（`src/memory.rs`）

高效的内存池，具备：
- 预分配的内存块
- 自动垃圾回收
- 基于压力的拒绝
- 线程安全的分配/释放

### 5. 配置（`src/config.rs`）

集中化的配置管理：
- 基于 YAML 的配置文件
- 环境变量覆盖（包括 enforcement 的 `ENFORCE_*`）
- 热重载支持（未来）
- 结构化的配置类型

### 6. 可观测性

#### Tracing（`src/tracing.rs`）
- OpenTelemetry 集成
- W3C trace context 传播
- 活跃 span context 注入
- 跨服务 tracing 支持

#### Metrics（`src/metrics.rs`）
- Prometheus metrics 导出
- Tool 执行指标
- 内存使用跟踪
- 缓存性能统计
- Enforcement 指标：按原因分类的丢弃数、放行结果

## API 契约

### gRPC API

agent 暴露以下 gRPC 服务：

```protobuf
service AgentService {
  rpc ExecuteTask(ExecuteTaskRequest) returns (ExecuteTaskResponse);
  rpc StreamExecuteTask(ExecuteTaskRequest) returns (stream TaskUpdate);
  rpc GetCapabilities(GetCapabilitiesRequest) returns (GetCapabilitiesResponse);
  rpc HealthCheck(HealthCheckRequest) returns (HealthCheckResponse);
  rpc DiscoverTools(DiscoverToolsRequest) returns (DiscoverToolsResponse);
  rpc GetToolCapability(GetToolCapabilityRequest) returns (GetToolCapabilityResponse);
}
```

### Python-Rust 契约

Rust agent 通过 HTTP 与 Python LLM 服务通信：

#### Tool 选择
```
POST /tools/select
{
  "task": "string",
  "context": {},
  "exclude_dangerous": boolean,
  "max_tools": number
}
```

#### Tool 执行
```
POST /tools/execute
{
  "tool_name": "string",
  "parameters": {}
}
```

#### Task 分析
```
POST /analyze_task
{
  "query": "string",
  "context": {}
}
```

## 错误处理

使用 `thiserror` 的全面错误分类：

```rust
pub enum AgentError {
    ToolNotFound { name: String },
    ToolExecutionFailed { tool: String, reason: String },
    MemoryExhausted { requested: usize, available: usize },
    SandboxViolation { operation: String },
    ConfigurationError(String),
    NetworkError(String),
    // ... 20+ error variants
}
```

## 性能优化

### 1. 零拷贝字符串
使用 `Cow<str>` 处理字符串，避免不必要的内存分配：
```rust
pub fn process_text<'a>(input: &'a str) -> Cow<'a, str>
```

### 2. 惰性初始化
metrics 的现代 `OnceLock` 模式：
```rust
static METRICS: OnceLock<HashMap<String, Counter>> = OnceLock::new();
```

### 3. 并行 Tool 执行
使用 tokio 的并发 tool 执行：
```rust
let futures = tools.iter().map(|tool| executor.execute_tool(tool));
let results = futures::future::join_all(futures).await;
```

### 4. 缓存优先架构
- 可配置 TTL 的 tool 结果缓存
- 简单查询的 LLM 响应缓存
- 发现结果缓存

## 安全模型

### WASI Sandbox 隔离
- 无网络访问
- 受限的文件系统访问（只读 `/tmp`）
- 强制执行内存限制
- 通过 fuel 计量控制 CPU 用量

### Tool 权限系统
```rust
pub struct ToolCapability {
    pub required_permissions: Vec<String>,
    pub is_dangerous: bool,
    pub requires_confirmation: bool,
}
```

### 输入校验
- 参数 schema 校验
- 输入大小限制
- 超时保护

## 测试策略

### 单元测试
- 组件级测试
- mock 依赖
- 复杂逻辑的基于属性的测试

### 集成测试
- Python-Rust 契约验证
- 端到端 tool 执行
- 缓存行为验证
- 错误处理场景

### 性能测试
- 基准测试关键路径
- 内存使用剖析
- 并发执行压力测试

## 部署

### Docker 容器
```dockerfile
FROM rust:latest as builder
WORKDIR /usr/src/app

# Copy manifests and sources
COPY rust/agent-core/Cargo.toml ./
COPY rust/agent-core/build.rs ./
COPY protos /protos
COPY rust/agent-core/src ./src

# Build
RUN apt-get update && apt-get install -y protobuf-compiler \
 && cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates netcat-openbsd \
 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/target/release/shannon-agent-core /usr/local/bin/shannon-agent-core
EXPOSE 50051
CMD ["shannon-agent-core"]
```

### 配置
环境变量：
- `RUST_LOG`：日志级别
- `OTEL_EXPORTER_OTLP_ENDPOINT`：tracing 端点
- `MEMORY_POOL_SIZE_MB`：内存池大小
- `WASI_MEMORY_LIMIT_MB`：WASI sandbox 内存限制
- `LLM_SERVICE_URL`：Python LLM 服务基础 URL

注意：tool 结果缓存 TTL 通过 agent 配置（YAML）中的 `tools.cache_ttl_secs` 配置。不存在 `TOOL_CACHE_TTL_SECONDS` 环境变量覆盖。

### 健康检查
- 端口 `50051` 上的 gRPC HealthCheck RPC
- Metrics 端点：`:2113/metrics`

## 迁移指南

### 从旧模式迁移

#### 替换 `lazy_static!`
```rust
// Old
lazy_static! {
    static ref METRICS: Mutex<HashMap<String, Counter>> = Mutex::new(HashMap::new());
}

// New
static METRICS: OnceLock<Mutex<HashMap<String, Counter>>> = OnceLock::new();
```

#### 错误处理
```rust
// Old
let result = operation().unwrap();

// New
let result = operation().context("Failed to perform operation")?;
```

#### 字符串操作
```rust
// Old
fn process(input: String) -> String

// New
fn process(input: &str) -> Cow<str>
```

## 未来增强

1. **WebAssembly Component Model**：支持 WASI Preview 2
2. **分布式缓存**：集成 Redis 以共享缓存
3. **GPU 加速**：为 ML 操作提供 CUDA/ROCm 支持
4. **多区域支持**：地理分布式 agent 部署
5. **高级监控**：自定义 metrics 与 tracing span

## 贡献

请遵循以下准则：
1. 提交前使用 `cargo fmt` 和 `cargo clippy`
2. 为新功能添加测试
3. API 变更时更新文档
4. 遵循错误处理最佳实践
5. 尽量减少不必要的内存分配

## 许可证

Copyright 2025 Shannon Project。保留所有权利。
