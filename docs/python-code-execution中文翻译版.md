# Shannon 中的 Python 代码执行

## 概述

Shannon 通过 WebAssembly System Interface（WASI）提供安全的 Python 代码执行，确保完整的 sandbox 与资源隔离。本文档涵盖 Python 执行系统的配置、用法与架构。

## 关键配置要求 ⚠️

### 前置条件 —— 启动前必须完成

#### 1. 下载 Python WASI 解释器（必需）

```bash
# This MUST be run BEFORE starting services
./scripts/setup_python_wasi.sh

# Verify the file was downloaded (should be ~20MB)
ls -lh wasm-interpreters/python-3.11.4.wasm
```

#### 2. 理解 WebAssembly 表限制

**重要**：「Table limits」是 WebAssembly runtime 的概念：

- **WebAssembly 表**：在 WASM 模块中存储函数引用的内存结构
- **Python 需要大表**：CPython 有数千个内部函数（5413+ 条目）
- **默认限制**：默认值应超过 5000+，而 Python 需要 10000+

#### 3. 必需的配置修改

以下内容必须在 `rust/agent-core/src/wasi_sandbox.rs` 中配置：

```rust
// In WasiSandbox::with_config()
execution_timeout: app_config.wasi_timeout(),
// Python WASM requires larger table limits (5413+ elements)
table_elements_limit: 10000,  // CRITICAL: Default 1024 is too small!
instances_limit: 10,           // Multiple WASM instances needed
tables_limit: 10,              // Multiple function tables
memories_limit: 4,             // Memory regions
```

#### 4. 构建并启动服务

```bash
# Rebuild agent-core with the configuration changes
docker compose -f deploy/compose/docker-compose.yml build --no-cache agent-core

# Rebuild llm-service to include Python executor
docker compose -f deploy/compose/docker-compose.yml build llm-service

# Start all services
make dev
```

## 快速开始（配置完成后）

### 2. 执行 Python 代码

```bash
# Simple execution
./scripts/submit_task.sh "Execute Python: print('Hello, Shannon!')"

# Mathematical computation
./scripts/submit_task.sh "Execute Python code to calculate factorial of 10"
# Or more directly:
./scripts/submit_task.sh "Run Python code to calculate factorial of 10"

# Data processing
./scripts/submit_task.sh "Use Python to generate the first 20 Fibonacci numbers"
```

## 架构

```
┌──────────────────────────────────────────────────────────┐
│                     User Request                          │
│           "Execute Python: print(2+2)"                    │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│                 Orchestrator (Go)                         │
│    Routes to LLM Service based on complexity              │
└────────────────────┬─────────────────────────────────────┘
                     ▼
┌──────────────────────────────────────────────────────────┐
│                LLM Service (Python)                       │
│  ┌──────────────────────────────────────────────────┐    │
│  │         PythonWasiExecutorTool                   │    │
│  │  • Detects Python execution need                 │    │
│  │  • Prepares execution context                    │    │
│  │  • Manages session state (optional)              │    │
│  │  • Caches interpreter for performance            │    │
│  └────────────────┬─────────────────────────────────┘    │
└──────────────────┼────────────────────────────────────────┘
                   ▼
┌──────────────────────────────────────────────────────────┐
│               Agent Core (Rust)                           │
│  ┌──────────────────────────────────────────────────┐    │
│  │          WASI Sandbox (Wasmtime)                 │    │
│  │  ┌────────────────────────────────────────────┐  │    │
│  │  │    Python.wasm (CPython 3.11.4)            │  │    │
│  │  │  • Full standard library                   │  │    │
│  │  │  • Memory limit: 256MB                     │  │    │
│  │  │  • CPU limit: Configurable                 │  │    │
│  │  │  • Timeout: 30s (max 60s)                  │  │    │
│  │  │  • No network access                       │  │    │
│  │  │  • Read-only filesystem                    │  │    │
│  │  └────────────────────────────────────────────┘  │    │
│  └──────────────────────────────────────────────────┘    │
└──────────────────────────────────────────────────────────┘
```

## 特性

### ✅ 生产特性

- **完整的 Python 标准库**：完整的 CPython 3.11.4，包含全部内置模块
- **会话持久化**：通过 session ID 在多次执行之间保留变量
- **性能优化**：缓存解释器，启动时间从 500ms 降至 50ms
- **资源限制**：可配置的内存、CPU 与超时限制
- **安全隔离**：通过 WASI 实现真正的 sandbox —— 无网络、无文件系统写入
- **输出流式返回**：为长时间运行的计算提供渐进式输出
- **错误处理**：完整的错误消息与超时保护

### 🚀 高级能力

- **持久 session**：变量与 import 在多次执行之间保持
- **自定义超时**：每次执行可在 1 到 60 秒之间调整
- **Stdin 支持**：向 Python 脚本提供输入数据
- **性能指标**：执行时间跟踪与上报

## 用法示例

### 基础 Python 执行

```python
# Request
"Execute Python: print('Hello, World!')"

# Output
Hello, World!
```

### 数学计算

```python
# Request
"Execute Python code to calculate the factorial of 10"

# Generated Code
import math
result = math.factorial(10)
print(f"10! = {result}")

# Output
10! = 3628800
```

### 数据处理

```python
# Request
"Generate the first 20 Fibonacci numbers using Python"

# Generated Code
def fibonacci(n):
    fib = [0, 1]
    for i in range(2, n):
        fib.append(fib[-1] + fib[-2])
    return fib

result = fibonacci(20)
print(f"First 20 Fibonacci numbers: {result}")

# Output
First 20 Fibonacci numbers: [0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144, 233, 377, 610, 987, 1597, 2584, 4181]
```

### 会话持久化

```python
# Request 1 (with session_id: "data-analysis")
"Execute Python: data = [1, 2, 3, 4, 5]"

# Request 2 (same session_id)
"Execute Python: import statistics; print(statistics.mean(data))"

# Output
3
```

## API 集成

### 直接 API 调用

```bash
curl -X POST http://localhost:8000/agent/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Execute Python: print(sum(range(100)))",
    "tools": ["python_executor"],
    "mode": "standard"
  }'
```

### 工具参数

```json
{
  "tool": "python_executor",
  "parameters": {
    "code": "print('Hello')",
    "session_id": "optional-session-id",
    "timeout_seconds": 30,
    "stdin": "optional input data"
  }
}
```

## 配置

### 环境变量

```bash
# Python WASI interpreter path (required)
PYTHON_WASI_WASM_PATH=/opt/wasm-interpreters/python-3.11.4.wasm

# Agent Core address (for gRPC communication)
AGENT_CORE_ADDR=agent-core:50051
```

### 资源限制配置

#### 代码中（rust/agent-core/src/wasi_sandbox.rs）

```rust
// CRITICAL: These values MUST be set for Python WASM to work!
pub fn with_config(app_config: &Config) -> Result<Self> {
    // ...
    Ok(Self {
        // ...
        // Python WASM requires larger table limits (5413+ elements)
        table_elements_limit: 10000,  // MUST be ≥ 5413 for Python
        instances_limit: 10,           // MUST be ≥ 4
        tables_limit: 10,              // MUST be ≥ 4
        memories_limit: 4,             // MUST be ≥ 2
    })
}
```

#### 环境变量（docker-compose.yml）

```yaml
agent-core:
  environment:
    - WASI_MEMORY_LIMIT_MB=512      # Memory for Python execution
    - WASI_TIMEOUT_SECONDS=60        # Max execution time
    - PYTHON_WASI_WASM_PATH=/opt/wasm-interpreters/python-3.11.4.wasm

llm-service:
  environment:
    - PYTHON_WASI_WASM_PATH=/opt/wasm-interpreters/python-3.11.4.wasm
    - AGENT_CORE_ADDR=agent-core:50051
```

## 安全模型

### sandbox 保证

| 特性             | 状态        | 说明                          |
| ---------------- | ----------- | ----------------------------- |
| **内存隔离**     | ✅ 强制     | 每次执行使用独立的线性内存空间 |
| **网络访问**     | ❌ 阻止     | 不授予任何网络能力            |
| **文件系统**     | 🔒 只读     | 仅限白名单路径                |
| **进程创建**     | ❌ 阻止     | 无法创建子进程                |
| **资源限制**     | ✅ 强制     | 内存、CPU 与时间限制          |
| **系统调用**     | ❌ 阻止     | 无法访问宿主系统调用          |

### 为什么选择 WASI？

1. **真正的隔离**：WebAssembly 提供硬件级隔离
2. **确定性**：同一份代码在各平台上产生相同结果
3. **资源控制**：对内存与 CPU 使用进行细粒度控制
4. **无法逃逸**：即使恶意代码也无法突破 sandbox
5. **行业标准**：被 Cloudflare Workers、Fastly 等采用

## 支持的 Python 特性

### ✅ 完全支持

- 所有 Python 3.11 语法与特性
- 标准库模块：
  - `math`、`statistics`、`random`
  - `json`、`csv`、`xml`
  - `datetime`、`calendar`、`time`
  - `re`、`string`、`textwrap`
  - `collections`、`itertools`、`functools`
  - `hashlib`、`base64`、`binascii`
  - `decimal`、`fractions`
  - `pathlib`、`os.path`（只读）

### ⚠️ 有限支持

- `io`：仅支持内存内操作
- `sqlite3`：仅支持内存数据库
- 文件操作：对 `/tmp` 的只读访问

### ❌ 不支持

- 网络操作（`requests`、`urllib`、`socket`）
- 包安装（`pip install`）
- 原生扩展（C 模块）
- GUI 库（`tkinter`、`pygame`）
- 多进程/多线程
- 系统操作（`subprocess`、`os.system`）

## 性能特征

| 操作                          | 时间   | 内存   |
| ----------------------------- | ------ | ------ |
| 解释器加载（首次）            | ~500ms | 20MB   |
| 解释器加载（缓存后）          | ~50ms  | 0MB    |
| 简单 print                    | ~100ms | 50MB   |
| Factorial(1000)               | ~150ms | 55MB   |
| 排序 10,000 个数字            | ~200ms | 60MB   |
| 处理 1MB JSON                 | ~300ms | 80MB   |

## 故障排查

### 常见问题与解决方案

#### 1. "Failed to instantiate WASM module: table minimum size of 5413 elements exceeds table limits"

**原因**：WebAssembly 表限制对 Python WASM 来说太小。

**解决方案**：更新 `rust/agent-core/src/wasi_sandbox.rs`：

```rust
table_elements_limit: 10000,  // Increase from default 1024
```

然后重新构建：`docker compose build agent-core`

#### 2. "Python WASI interpreter not found"

**原因**：Python WASM 文件未下载。

**解决方案**：

```bash
# Download the interpreter
./scripts/setup_python_wasi.sh

# Verify it exists (should be ~20MB)
ls -lh wasm-interpreters/python-3.11.4.wasm
```

#### 3. "can't open file '//import sys; exec(sys.stdin.read())': [Errno 44] No such file or directory"

**原因**：Python WASM 的 argv 格式不正确。

**解决方案**：确保 `python_wasi_executor.py` 使用正确的 argv：

```python
"argv": ["python", "-c", "import sys; exec(sys.stdin.read())"],  # Note: argv[0] must be "python"
```

#### 4. "Execution timeout"

**原因**：Python 代码执行时间超过超时限制。

**解决方案**：

```python
# Increase timeout in request (max 60 seconds)
{
  "code": "long_running_code()",
  "timeout_seconds": 60
}
```

#### 5. "Memory limit exceeded"

**原因**：Python 代码使用的内存超过分配上限。

**解决方案**：在环境变量中提高内存限制：

```bash
# In docker-compose.yml for agent-core
environment:
  - WASI_MEMORY_LIMIT_MB=512  # Increase from 256
```

#### 6. "Module not found"

**原因**：尝试导入外部包（numpy、pandas 等）

**解决方案**：只有 Python 标准库可用。外部包必须用 stdlib 重新实现。

#### 7. "WASI execution error"，没有任何详细信息

**原因**：多种问题 —— 请检查 agent-core 日志。

**解决方案**：

```bash
# Check detailed logs
docker compose logs agent-core --tail 100 | grep -E "WASI|Python|code_executor"

# Enable debug logging in docker-compose.yml
environment:
  - RUST_LOG=debug,shannon_agent_core::wasi_sandbox=debug
```

## 最佳实践

1. **保持代码简单**：复杂操作会增加执行时间
2. **合理使用 session**：session 会消耗内存 —— 用完后清除
3. **处理超时**：长时间计算应拆分为多个步骤
4. **减少 import**：每次 import 都会增加开销
5. **批量操作**：分块处理数据以获得更好性能

## 与其他方案的对比

| 特性             | Shannon WASI | Docker Container | AWS Lambda  | Google Colab |
| ----------------- | ------------ | ---------------- | ----------- | ------------ |
| **安全性**        | ⭐⭐⭐⭐⭐        | ⭐⭐⭐              | ⭐⭐⭐⭐        | ⭐⭐⭐          |
| **启动时间**      | 50-100ms     | 1-5s             | 100-500ms   | 5-10s        |
| **内存开销**      | 50MB         | 200MB+           | 128MB+      | 1GB+         |
| **包支持**        | 仅标准库     | 完整             | 完整        | 完整         |
| **网络访问**      | 无           | 有               | 有          | 有           |
| **成本**          | 极低         | 中等             | 按请求计费  | 免费/付费    |
| **确定性**        | 是           | 否               | 多数情况    | 否           |

## 未来路线图

### 计划中的增强

1. **包支持**：通过 Pyodide 提供预编译的 NumPy、Pandas
2. **多语言**：JavaScript（QuickJS）、Ruby（ruby.wasm）
3. **调试**：支持单步调试
4. **可视化**：通过 base64 图片输出 Matplotlib 结果
5. **分布式执行**：面向大任务的多节点执行

## 技术细节

### 解释器：CPython 3.11.4

- **来源**：[VMware WebAssembly Language Runtimes](https://github.com/vmware-labs/webassembly-language-runtimes)
- **大小**：压缩后 20MB
- **Python 版本**：3.11.4
- **构建日期**：2023-07-14
- **兼容性**：100% 兼容 CPython

### WASI runtime：Wasmtime

- **版本**：最新稳定版
- **特性**：WASI Preview 1
- **安全**：基于 capability 的安全模型
- **性能**：接近原生的执行速度

## 获取帮助

### 资源

- [WebAssembly 规范](https://webassembly.github.io/spec/)
- [WASI 文档](https://wasi.dev/)
- [Python WASM 指南](https://github.com/vmware-labs/webassembly-language-runtimes/tree/main/python)
- [Shannon 文档](https://github.com/Kocoro-lab/shannon)

### 支持

- GitHub Issues：[shannon/issues](https://github.com/Kocoro-lab/shannon/issues)
- 文档：[docs/](../docs/)

---

*最后更新：2025 年 1 月*
