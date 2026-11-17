# Shannon Session Workspace

## 概述

Session workspace 为每个用户 session 提供隔离的文件系统环境。所有文件操作都被限定在 session 的 workspace 目录内，确保不同 session 无法访问彼此的文件。

这种隔离对以下方面至关重要：
- **多租户安全**：防止用户之间的数据泄漏
- **可复现性**：每个 session 都从干净的 workspace 开始
- **资源管理**：按 session 的磁盘配额可防止用量失控

## 本地与 EKS 执行

Shannon 会根据环境使用**不同的 Python sandbox 后端**：

| | 本地（Docker Compose） | EKS（生产环境） |
|---|---|---|
| **`python_executor` 后端** | WASI sandbox（通过 wasmtime 运行 Python.wasm） | Firecracker microVM |
| **Python 包** | 仅标准库 | 完整数据科学栈（pandas、numpy、scipy、torch 等） |
| **写入 `/workspace/`** | 可以，前提是存在 `session_id`（WASI preopened dir） | 可以，通过 EFS 支撑的 ext4 块设备 |
| **文件持久化** | `file_*` tool 与 `python_executor` 写入 Docker volume 上的 session 目录 | `file_*` tool 与 `python_executor` 都写入 EFS（双向同步） |
| **隔离** | WASI 基于 capability（wasmtime preopened dir） | 硬件级 VM 隔离 |

**本文档描述的是本地 Docker Compose 路径。**

## File tool 的 role 要求

**重要**：file tool 受 role 控制。你必须在请求 context 中指定合适的 role 才能访问文件操作。

| Role | 可用的 file tool |
|------|---------------------|
| `developer` | `file_read`, `file_write`, `file_list`, `bash`, `python_executor` |
| `generalist`（默认） | `file_read`, `file_list`, `bash`（没有 `file_write`） |
| `critic` | `file_read`, `file_list`, `bash` |

**要写文件，必须使用 `role: "developer"`：**

```json
{
  "query": "Create a file called test.txt with content 'Hello'",
  "session_id": "my-session-123",
  "context": {
    "role": "developer"
  }
}
```

没有 `role: "developer"` 时，agent 无法使用 `file_write`，请求要么失败，要么 LLM 根本没有这个 tool 可用。

### 示例：跨请求的文件持久化

```bash
# First request - create file (requires developer role)
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Write '\''test data'\'' to data.txt",
    "session_id": "persist-test",
    "context": {"role": "developer"}
  }'

# Second request - read same file (same session_id)
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Read data.txt and show contents",
    "session_id": "persist-test",
    "context": {"role": "developer"}
  }'
```

文件在 session 内持久存在——使用相同 `session_id` 的后续请求可以访问先前创建的文件。

## 架构

### 目录结构

```
/tmp/shannon-sessions/              # SHANNON_SESSION_WORKSPACES_DIR (base)
├── session-abc123/                 # Session A's workspace
│   ├── code/
│   │   └── main.py
│   ├── data/
│   │   └── input.csv
│   └── output.txt
├── session-def456/                 # Session B's workspace
│   └── results.json
└── session-xyz789/                 # Session C's workspace
    └── notes.md
```

每个 session 都会得到一个以 `session_id` 命名的专属子目录。workspace 在首次文件操作时自动创建。

### 组件流程

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Tool Request                                       │
│  {"tool": "file_write", "path": "data.txt", "content": "Hello"}             │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Python llm-service (file_ops.py)                          │
│                                                                              │
│  Check: is_sandbox_enabled()?                                                │
│    ├── Yes → Proxy to Rust agent-core via gRPC                              │
│    └── No  → Execute locally with Python path validation                     │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
            ┌───────────────────────┴───────────────────────┐
            │                                               │
            ▼ (WASI mode)                                   ▼ (Python mode)
┌───────────────────────────────┐           ┌───────────────────────────────┐
│   Rust agent-core             │           │   Python Path Validation       │
│   (sandbox_service.rs)        │           │                                │
│                               │           │   - Resolve canonical path     │
│   - WorkspaceManager          │           │   - Check against allowlist    │
│   - SafeCommand execution     │           │   - Execute if permitted       │
│   - Capability-based security │           │                                │
└───────────────────────────────┘           └───────────────────────────────┘
```

## 安全模型

Shannon 为文件操作实现了两个安全层。根据配置不同，可能只有一层生效，也可能两层都生效。

### 阶段 2：Python 路径校验（fallback）

当 `SHANNON_USE_WASI_SANDBOX=0` 时，文件操作直接由 Python 处理，并做路径校验：

1. **路径规范化**：所有路径都用 `Path.resolve()` 解析，以消除符号链接和 `..` 分量
2. **allowlist 强制执行**：解析后的路径必须落在允许的目录内：
   - Session workspace：`{SHANNON_SESSION_WORKSPACES_DIR}/{session_id}/`
   - 可选的共享 workspace：`SHANNON_WORKSPACE`
   - 仅开发模式：当前工作目录（若 `SHANNON_DEV_ALLOW_CWD=1`）
3. **符号链接防护**：指向允许目录之外的符号链接会被拒绝

**被拦截的路径穿越示例：**
```python
# Request: {"path": "../../../etc/passwd"}
# Resolved: /etc/passwd
# Result: DENIED (not within session workspace)
```

### 阶段 3：WASI sandbox（默认，增强安全性）

当 `SHANNON_USE_WASI_SANDBOX=1`（docker-compose 中的默认值）时，文件操作会被代理到 Rust agent-core 服务，它提供额外的安全保证：

1. **基于 capability 的安全**：WASI runtime 只授予对 session workspace 目录的访问权限
2. **内存限制**：sandbox 执行受内存限制约束
3. **原生命令实现**：shell 命令（`ls`、`cat` 等）用纯 Rust 实现，而不是启动 shell 进程
4. **元字符拦截**：shell 元字符在解析阶段就被拒绝

**安全属性：**
- 不可能发生 shell 注入（命令是被解析的，而不是通过 shell 执行）
- sandbox 内没有网络访问
- 执行受内存限制
- 无法访问系统文件；只暴露 `/workspace/`（session 目录）

## 配置

### 环境变量

| 变量 | 默认值 | 描述 |
|----------|---------|-------------|
| `SHANNON_USE_WASI_SANDBOX` | `0` | 启用 WASI sandbox 模式。设为 `1`、`true` 或 `yes` 即启用。 |
| `SHANNON_SESSION_WORKSPACES_DIR` | `/tmp/shannon-sessions` | 所有 session workspace 的基础目录 |
| `SHANNON_MAX_WORKSPACE_SIZE_MB` | `100` | 每个 session 的磁盘配额，单位 MB |
| `SHANNON_WORKSPACE` | （未设置） | 可选的共享 workspace，所有 session 都可访问 |
| `SHANNON_DEV_ALLOW_CWD` | `0` | 仅开发用：允许访问当前工作目录 |
| `SHANNON_ALLOW_GLOBAL_TMP` | `0` | 允许访问全局 `/tmp`（出于隔离考虑默认禁用） |

### Docker Compose 配置

`agent-core` 和 `llm-service` 必须共享同一个 session workspace volume：

```yaml
volumes:
  shannon-sessions:

services:
  agent-core:
    volumes:
      - shannon-sessions:/tmp/shannon-sessions
    environment:
      - SHANNON_SESSION_WORKSPACES_DIR=/tmp/shannon-sessions

  llm-service:
    volumes:
      - shannon-sessions:/tmp/shannon-sessions
    environment:
      - SHANNON_USE_WASI_SANDBOX=${SHANNON_USE_WASI_SANDBOX:-0}
      - SHANNON_SESSION_WORKSPACES_DIR=/tmp/shannon-sessions
```

## 使用 file tool

### file_read

从 session workspace 读取文件内容。

**参数：**
| 参数 | 类型 | 必填 | 默认值 | 描述 |
|-----------|------|----------|---------|-------------|
| `path` | string | 是 | - | 文件路径（相对于 workspace） |
| `encoding` | string | 否 | `utf-8` | 文件编码（`utf-8`、`ascii`、`latin-1`） |
| `max_size_mb` | integer | 否 | `10` | 读取的最大文件大小（1-100 MB） |

**示例：**
```json
{
  "tool": "file_read",
  "path": "data/config.json"
}
```

**响应：**
```json
{
  "success": true,
  "output": {"key": "value"},
  "metadata": {
    "path": "/tmp/shannon-sessions/my-session/data/config.json",
    "size_bytes": 42,
    "encoding": "utf-8",
    "file_type": ".json"
  }
}
```

JSON 和 YAML 文件会被自动解析为结构化数据。

### file_write

向 session workspace 中的文件写入内容。

**参数：**
| 参数 | 类型 | 必填 | 默认值 | 描述 |
|-----------|------|----------|---------|-------------|
| `path` | string | 是 | - | 要写入的文件路径 |
| `content` | string | 是 | - | 要写入的内容 |
| `mode` | string | 否 | `overwrite` | `overwrite` 或 `append` |
| `encoding` | string | 否 | `utf-8` | 文件编码 |
| `create_dirs` | boolean | 否 | `false` | 按需创建父目录 |

**示例：**
```json
{
  "tool": "file_write",
  "path": "output/results.txt",
  "content": "Analysis complete.\nTotal items: 42",
  "create_dirs": true
}
```

**响应：**
```json
{
  "success": true,
  "output": "/tmp/shannon-sessions/my-session/output/results.txt",
  "metadata": {
    "path": "/tmp/shannon-sessions/my-session/output/results.txt",
    "size_bytes": 35,
    "mode": "overwrite",
    "encoding": "utf-8",
    "created_dirs": true
  }
}
```

### file_list

列出 session workspace 内某个目录中的文件。

**参数：**
| 参数 | 类型 | 必填 | 默认值 | 描述 |
|-----------|------|----------|---------|-------------|
| `path` | string | 是 | - | 要列出的目录路径 |
| `pattern` | string | 否 | `*` | glob pattern（例如 `*.txt`、`*.py`） |
| `recursive` | boolean | 否 | `false` | 是否包含子目录 |
| `include_hidden` | boolean | 否 | `false` | 是否包含隐藏文件（`.xxx`） |

**示例：**
```json
{
  "tool": "file_list",
  "path": ".",
  "pattern": "*.py",
  "recursive": true
}
```

**响应：**
```json
{
  "success": true,
  "output": [
    {"name": "main.py", "path": "main.py", "is_file": true, "size_bytes": 1024},
    {"name": "utils.py", "path": "lib/utils.py", "is_file": true, "size_bytes": 512}
  ],
  "metadata": {
    "directory": "/tmp/shannon-sessions/my-session",
    "pattern": "*.py",
    "recursive": true,
    "file_count": 2,
    "dir_count": 0
  }
}
```

## Sandbox 命令（WASI 模式）

当 `SHANNON_USE_WASI_SANDBOX=1` 时，以下安全命令可直接执行。命令在 Rust 中原生实现（不经过 shell），从而消除 shell 注入风险。

### 读操作

| 命令 | 描述 | 示例 |
|---------|-------------|---------|
| `ls` | 列出目录内容 | `ls -la data/` |
| `cat` | 打印文件内容 | `cat config.json` |
| `head` | 打印前 N 行 | `head -n 10 log.txt` |
| `tail` | 打印后 N 行 | `tail -n 20 log.txt` |
| `wc` | 统计行数/词数/字节数 | `wc data.csv` |
| `pwd` | 打印当前工作目录 | `pwd` |

### 写操作

| 命令 | 描述 | 示例 |
|---------|-------------|---------|
| `mkdir` | 创建目录 | `mkdir -p output/reports` |
| `rm` | 删除文件/目录 | `rm -r temp/` |
| `cp` | 复制文件 | `cp source.txt backup.txt` |
| `mv` | 移动/重命名文件 | `mv old.txt new.txt` |
| `touch` | 创建空文件 | `touch marker.txt` |

### 搜索操作

| 命令 | 描述 | 示例 |
|---------|-------------|---------|
| `find` | 按名称查找文件 | `find . -name "*.py"` |
| `grep` | 搜索 pattern | `grep -i error log.txt` |

### 工具命令

| 命令 | 描述 | 示例 |
|---------|-------------|---------|
| `echo` | 打印文本 | `echo "Hello World"` |

### 命令 flag

| 命令 | 支持的 flag |
|---------|-----------------|
| `ls` | `-a`（全部）、`-l`（长格式） |
| `head` | `-n NUM`（行数） |
| `tail` | `-n NUM`（行数） |
| `mkdir` | `-p`（创建父目录） |
| `rm` | `-r`（递归） |
| `grep` | `-i`（忽略大小写） |
| `find` | `-name PATTERN` |

## 被拦截的 shell 元字符

以下 shell 元字符会被**拦截**，以防命令注入：

| 字符 | 含义 | 拦截原因 |
|-----------|---------|-------------|
| `\|` | 管道 | 防止串联到其他命令 |
| `;` | 命令分隔符 | 防止执行额外命令 |
| `&&` | AND 运算符 | 防止条件执行 |
| `\|\|` | OR 运算符 | 防止条件执行 |
| `>` | 输出重定向 | 防止写入任意文件 |
| `<` | 输入重定向 | 防止读取任意文件 |
| `>>` | 追加重定向 | 防止向任意文件追加 |
| `$(` | 命令替换 | 防止内嵌命令执行 |
| `` ` `` | 反引号替换 | 防止内嵌命令执行 |
| `\n` | 换行符 | 防止多行注入 |
| `\r` | 回车符 | 防止通过 CR 注入 |

**被拦截命令的示例：**
```
cat file.txt | grep secret     # REJECTED: pipe character
ls; rm -rf /                   # REJECTED: semicolon
echo $(whoami)                 # REJECTED: command substitution
```

## 配额执行

每个 session workspace 都有可配置的磁盘配额（默认：100MB）。

### 配额如何生效

1. 每次写操作之前，都会计算当前 workspace 的大小
2. 如果新增内容会超出配额，操作就会失败
3. 不会出现部分写入——要么整个写入成功，要么什么都不写

### 检查 workspace 大小

`WorkspaceManager` 提供 `get_workspace_size()` 方法，递归计算某个 session workspace 中所有文件的总大小。

### 错误响应

配额超限时：
```json
{
  "success": false,
  "error": "Workspace quota exceeded: 102.5MB > 100MB limit"
}
```

### 建议

- 在大批量写入之前用 `file_list` 审计 workspace 内容
- 不再需要时及时清理临时文件
- 如果合理需求超出默认值，可通过配置申请提高配额

## Session ID 校验

Session ID 会经过校验，以防路径穿越攻击：

| 规则 | 有效示例 | 无效示例 |
|------|---------------|-----------------|
| 仅允许字母数字、连字符、下划线 | `session-123`, `user_abc` | `session/../etc` |
| 最多 128 个字符 | （任意不超过 128 字符的字符串） | （超过 128 字符的字符串） |
| 不能以 `.` 开头 | `session.1` | `.hidden-session` |
| 不能包含 `..` | `my-session` | `session..test` |

## 清理

### 自动清理

Session workspace 会根据 session 的 TTL（Time-To-Live，存活时间）自动清理。当 session 过期时：

1. session 记录被标记为待删除
2. workspace 目录及其全部内容被移除
3. 清理作为后台进程周期性运行

### 手动清理

管理员可以手动删除某个 session workspace：

```rust
let mgr = WorkspaceManager::from_env();
mgr.delete_workspace("session-abc123")?;
```

### 列出所有 workspace

要查看所有活跃的 session workspace：

```rust
let mgr = WorkspaceManager::from_env();
let sessions = mgr.list_workspaces()?;
// Returns: ["session-abc123", "session-def456", ...]
```

## 故障排查

### 文件未找到

```
Error: File not found: data.txt
```
- 确认该文件存在于 session workspace 中
- 用 `file_list` 查看可用的文件
- 检查路径是否相对于 workspace 根目录

### 访问被拒绝

```
Error: Reading /etc/passwd is not allowed. Use session workspace.
```
- workspace 之外的绝对路径会被拒绝
- 路径穿越尝试（`../`）会被拦截
- 使用相对于 workspace 根目录的路径

### Sandbox proto 不可用

```
Error: Sandbox proto not available
```
- WASI sandbox 已启用，但 agent-core 没有运行
- 检查 agent-core 容器是否健康
- 确认各服务之间的 gRPC 连通性

### 配额超限

```
Error: Workspace quota exceeded
```
- 用 `rm` 清理未使用的文件
- 如有需要，提高 `SHANNON_MAX_WORKSPACE_SIZE_MB`
- 检查是否存在异常大的文件

## 已知限制

### Session ID 需要 `role: developer`

File tool（`file_read`、`file_write`、`file_list`）受 role 控制。如果请求 context 中没有 `role: "developer"`，LLM 就没有 `file_write` 可用，可能改用 `python_executor`（在存在 session_id 时可以写入 `/workspace/`）。

**解决方案**：提交需要文件操作的 task 时，始终带上 `"context": {"role": "developer"}`。

### python_executor 与 file tool 的对比

当存在 `session_id` 时，`python_executor` 和 `file_*` tool 都能读写 session workspace：

- **`python_executor`**：在 sandbox 内把 workspace 视为 `/workspace/`。本地 WASI 下，session 目录通过 wasmtime 的 preopened dir 以完全读写权限（`DirPerms::all()`）挂载。EKS 下，Firecracker VM 把 ext4 挂载到 `/workspace/`。没有 `session_id` 时不存在 `/workspace` 挂载，写入会失败。
- **`file_*` tool**：通过 Rust SandboxService（gRPC）直接写入宿主机上的 session 目录。只要提供 `session_id` 就始终可写。

**在 EKS 上**，两种 tool 访问同一个 EFS 支撑的 ext4，session 目录与 ext4 镜像之间双向同步。

## 关键源文件

| 组件 | 文件 | 用途 |
|-----------|------|---------|
| Workspace Manager | `rust/agent-core/src/workspace.rs` | session 目录管理 |
| Safe Commands | `rust/agent-core/src/safe_commands.rs` | 原生命令实现 |
| Sandbox Service | `rust/agent-core/src/sandbox_service.rs` | 用于 sandbox 操作的 gRPC 服务 |
| Python File Tools | `python/llm-service/llm_service/tools/builtin/file_ops.py` | 文件读/写/列举 tool |
| Sandbox Client | `python/llm-service/llm_service/tools/builtin/sandbox_client.py` | sandbox 代理的 gRPC client |
