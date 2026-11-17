# Shannon LLM Service（Python）

LLM Service 是 Shannon 的 AI provider gateway，负责管理 LLM 交互、tool 执行与 MCP 集成，支持多个 provider 以及智能 tool 选择。

## ⚠️ 重要环境准备提示

**在构建或运行该服务之前**，必须先生成 protobuf 文件：

```bash
# From repository root
./scripts/generate_protos_local.sh
```

这会创建 `python/llm-service/llm_service/grpc_gen` 目录，其中包含 Python WASI 执行器以及其他 gRPC 通信所需的 protobuf v5 兼容文件。

## 🎯 核心职责

- **多 Provider LLM Gateway** —— 为 OpenAI 与 Anthropic 提供统一接口（更多 provider 可通过内部 LLM manager 库接入）
- **Tool 管理** —— MCP tool 的注册、校验与执行
- **智能 Tool 选择** —— 根据 task 需求自动选择 tool
- **Web 搜索集成** —— 支持多个搜索 provider（Exa、Perplexity、Brave、DuckDuckGo）
- **Embeddings 与分块** —— 为向量存储处理文档
- **成本追踪** —— 按 provider 统计 token 用量并计算成本

## 🏗️ 架构

```
HTTP API (:8000)
    ↓
FastAPI Application
    ├── LLM Router → Provider Selection
    │   ├── OpenAI (GPT‑5 family)
    │   ├── Anthropic (Claude 4)
    │   └── (Additional providers via library)
    ├── Tools Router → MCP Integration
    │   ├── Tool Registry
    │   ├── Tool Validation
    │   └── Tool Execution
    └── Search Router → Web Search
        ├── Exa Search
        ├── Perplexity
        ├── Brave Search
        └── DuckDuckGo
```

## 📁 项目结构

```
python/llm-service/
├── main.py                      # FastAPI application entry
├── Dockerfile                   # Container configuration
├── requirements.txt             # Python dependencies
├── llm_service/
│   ├── api/                    # API endpoints
│   │   ├── llm.py              # LLM completion endpoints
│   │   ├── tools.py            # Tool management endpoints
│   │   └── health.py           # Health check endpoints
│   ├── core/                   # Core functionality
│   │   ├── config.py           # Configuration management
│   │   ├── models.py           # Pydantic models
│   │   └── metrics.py          # Prometheus metrics
│   ├── tools/                  # Tool system
│   │   ├── manager.py          # Tool registry & execution
│   │   ├── selector.py         # Intelligent tool selection
│   │   ├── mcp_tools.py        # MCP tool integration
│   │   └── builtin/            # Built-in tools
│   └── web_search/             # Search providers
│       ├── manager.py          # Search orchestration
│       └── providers/          # Individual providers
├── llm_provider/               # LLM provider implementations
│   ├── base.py                # Abstract provider interface
│   ├── openai_provider.py     # OpenAI implementation
│   ├── anthropic_provider.py  # Anthropic implementation
│   ├── google_provider.py     # Google Gemini
│   └── groq_provider.py       # Groq implementation
├── integrations/               # External integrations
│   └── mcp/                   # MCP protocol implementation
└── tests/                      # Test suite
    ├── test_providers.py       # Provider tests
    ├── test_tools.py           # Tool system tests
    └── test_web_search.py      # Search tests
```

## 🚀 快速开始

### 前置条件
- Python 3.11+
- 至少一个 LLM API key（OpenAI、Anthropic 等）
- Docker（用于容器化部署）

### 开发环境搭建

```bash
# Create virtual environment
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Set environment variables
export OPENAI_API_KEY="sk-..."
export ANTHROPIC_API_KEY="..."

# Run locally
python main.py

# Service will be available at http://localhost:8000
```

### Docker 部署

```bash
# Build image
docker build -t shannon-llm-service .

# Run with compose (recommended)
make dev  # From repository root

# Or run standalone
docker run -p 8000:8000 \
  -e OPENAI_API_KEY=$OPENAI_API_KEY \
  shannon-llm-service
```

## 🔌 API 端点

### LLM Completion
```bash
# Generate completion
curl -X POST http://localhost:8000/llm/completion \
  -H "Content-Type: application/json" \
  -d '{
    "provider": "openai",
    "model": "gpt-5-2025-08-07",
    "messages": [{"role": "user", "content": "Hello"}],
    "temperature": 0.7
  }'
```

### Tool 管理
```bash
# List available tools
curl http://localhost:8000/tools/list

# Execute a tool
curl -X POST http://localhost:8000/tools/execute \
  -H "Content-Type: application/json" \
  -d '{
    "tool_name": "calculator",
    "parameters": {"expression": "2+2"}
  }'

# Auto-select tools for task
curl -X POST http://localhost:8000/tools/select \
  -H "Content-Type: application/json" \
  -d '{
    "task": "Get weather in Beijing and convert temperature",
    "max_tools": 3
  }'
```

### Web 搜索
```bash
# Search the web
curl -X POST http://localhost:8000/search \
  -H "Content-Type: application/json" \
  -d '{
    "query": "latest AI developments",
    "provider": "exa",
    "max_results": 5
  }'
```

### 健康检查与指标
```bash
# Health check
curl http://localhost:8000/health

# Prometheus metrics
curl http://localhost:8000/metrics
```

## ⚙️ 配置

### 环境变量

```bash
# LLM Providers (at least one required)
OPENAI_API_KEY=sk-...
ANTHROPIC_API_KEY=...

# Search Providers (optional)
EXA_API_KEY=...
PERPLEXITY_API_KEY=...
BRAVE_API_KEY=...

# Tool Configuration
ENABLE_TOOL_SELECTION=1      # Auto-select tools
TOOL_PARALLELISM=4           # Parallel tool execution
MCP_REGISTER_TOKEN=...       # MCP registration auth

# Service Configuration
PORT=8000
LOG_LEVEL=INFO
METRICS_ENABLED=true
```

### Provider 模型

每个 provider 支持的模型各不相同。完整列表见 [providers-models.md](../../docs/providers-models.md)。

常用模型：
- **OpenAI**：gpt-5-2025-08-07, gpt-5-pro-2025-10-06, gpt-5-nano-2025-08-07, gpt-5-mini-2025-08-07
- **Anthropic**：claude-opus-4-1-20250805, claude-sonnet-4-5-20250929, claude-haiku-4-5-20251001

## 🧪 测试

### 单元测试
```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=. --cov-report=html

# Run specific test file
pytest tests/test_providers.py
```

### 集成测试
```bash
# Test LLM providers
python -m pytest tests/test_providers.py -k "test_openai"

# Test tool system
python -m pytest tests/test_tools.py

# Test web search
python -m pytest tests/test_web_search.py
```

### 手动测试
```bash
# Test tool execution
curl -X POST http://localhost:8000/tools/execute \
  -d '{"tool_name": "calculator", "parameters": {"expression": "2+2"}}'

# Test LLM completion
curl -X POST http://localhost:8000/llm/completion \
  -d '{"provider": "openai", "model": "gpt-5-2025-08-07", "messages": [{"role": "user", "content": "Hi"}]}'
```

## 🔧 主要特性

### 多 Provider 支持

该服务屏蔽了 provider 之间的差异：
- 所有 provider 使用统一 API
- 自动 retry 与 fallback
- 针对 provider 的专门优化
- 按 provider 统计成本

### MCP Tool 集成

支持 Model Context Protocol：
- 动态注册 tool
- tool 校验与 sandbox 隔离
- 并行执行 tool
- tool 成本追踪

### 智能 Tool 选择

基于以下依据自动选择 tool：
- task 分析
- tool 能力匹配
- 成本优化
- 执行时间预估

### Web 搜索集成

多个搜索 provider，具备：
- 结果去重
- 来源可信度评分
- 内容提取
- 缓存以提升效率

## 📊 可观测性

### 指标
- **端点**：`:8000/metrics`（Prometheus 格式）
- LLM token 用量与成本
- tool 执行次数与延迟
- provider 可用性与错误
- 搜索 query 性能

### 日志
- 结构化 JSON 日志
- 请求/响应链路追踪
- 带上下文的错误追踪
- 性能剖析

### 健康检查
- `/health` —— 基础健康状态
- `/health/ready` —— 就绪探针
- `/health/live` —— 存活探针

## 🚨 常见问题

### Provider 认证
- 确认环境变量中已设置 API key
- 检查 key 的格式与有效性
- 核对 provider 特有的要求

### Tool 注册
- MCP tool 需要有效的端点
- 检查 tool 参数 schema
- 动态注册时确认 `MCP_REGISTER_TOKEN`

### 内存占用
- 过大的 context window 会消耗内存
- 长响应请使用 stream
- 关注 embedding 的批大小

## 📚 更多文档

- [MCP 集成指南](../../docs/mcp-integration.md)
- [Provider 模型参考](../../docs/providers-models.md)
- [Web 搜索配置](../../docs/web-search-configuration.md)
- [主 README](../../README.md)
