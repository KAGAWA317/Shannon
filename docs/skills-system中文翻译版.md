# Shannon Skills 系统

## 概述

Skill 是基于 markdown 的 workflow 定义，为常见任务提供结构化的 prompt、tool 配置与执行约束。它们兼容 Anthropic 的 Agent Skills 规范。

当任务使用某个 skill 时，该 skill 的 markdown 内容会成为 system prompt，引导 AI agent 遵循结构化的 workflow。这使得任务执行模式一致且可重复。

## 目录结构

```
config/skills/
├── core/           # Built-in skills (committed to repo)
│   ├── code-review.md
│   ├── debugging.md
│   └── test-driven-dev.md
├── user/           # User custom skills (gitignored)
└── vendor/         # Vendor-specific skills (gitignored)
```

| 目录 | 用途 | Git 状态 |
|-----------|---------|------------|
| `core/` | 随 Shannon 一起提供的内置 skill | 已提交 |
| `user/` | 个人/团队自定义 skill | 被 Git 忽略 |
| `vendor/` | 第三方或厂商特定的 skill | 被 Git 忽略 |

## Skill 文件格式

Skill 是带 YAML frontmatter 的 markdown 文件，其后是 markdown 内容：

```markdown
---
name: my-skill
version: 1.0.0
author: Your Name
category: development
description: Brief description of what this skill does
requires_tools:
  - file_read
  - file_write
  - bash
requires_role: generalist
budget_max: 5000
dangerous: false
enabled: true
metadata:
  complexity: medium
  estimated_duration: 10min
---

# Skill Title

Your skill instructions in markdown format...

## Step 1: Gather Information
- Use `file_list` to discover files
- Read relevant files with `file_read`

## Step 2: Perform Analysis
...

## Output Format
Provide findings in this structure:
- Summary
- Details
- Recommendations
```

### Frontmatter 字段

| 字段 | 必填 | 类型 | 默认值 | 说明 |
|-------|----------|------|---------|-------------|
| `name` | 是 | string | - | 唯一标识符（仅小写字母、连字符、下划线） |
| `version` | 否 | string | `1.0.0` | 语义化版本 |
| `author` | 否 | string | - | skill 作者 |
| `category` | 否 | string | - | 用于分组的类别（如 development、research） |
| `description` | 否 | string | - | 简要说明（当 `dangerous: true` 时必填） |
| `requires_tools` | 否 | list | `[]` | 该 skill 需要的 tool 列表 |
| `requires_role` | 否 | string | - | 要使用的 role 预设（跳过任务分解） |
| `budget_max` | 否 | int | - | 执行时可用的最大 token budget |
| `dangerous` | 否 | bool | `false` | skill 是否执行危险操作 |
| `enabled` | 否 | bool | `true` | skill 是否启用 |
| `metadata` | 否 | object | `{}` | 附加的键值 metadata |

### 名称校验

Skill 名称只能包含：

- 小写字母（a-z）
- 大写字母（A-Z）
- 数字（0-9）
- 连字符（-）
- 下划线（_）

## 通过 API 使用 Skill

### 用 Skill 执行任务

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Review the authentication module for security issues",
    "skill": "code-review",
    "session_id": "my-session-123"
  }'
```

指定 `skill` 时：

1. skill 的 markdown 内容成为 system prompt
2. 若设置了 `requires_role`，则应用该设置（跳过任务分解）
3. 任务在 skill 的指导下以单 agent 方式执行

### 使用带版本的 Skill

用 `name@version` 请求指定版本：

```bash
curl -X POST http://localhost:8080/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Debug the login failure",
    "skill": "debugging@1.0.0",
    "session_id": "my-session-123"
  }'
```

## API 端点

### 列出所有 Skill

```bash
GET /api/v1/skills
```

响应：

```json
{
  "skills": [
    {
      "name": "code-review",
      "version": "1.0.0",
      "category": "development",
      "description": "Systematic code review workflow",
      "requires_tools": ["file_read", "file_list", "bash"],
      "dangerous": false,
      "enabled": true
    }
  ],
  "count": 3,
  "categories": ["development"]
}
```

### 按类别筛选

```bash
GET /api/v1/skills?category=development
```

### 获取 Skill 详情

```bash
GET /api/v1/skills/{name}
```

响应：

```json
{
  "skill": {
    "name": "code-review",
    "version": "1.0.0",
    "author": "Shannon",
    "category": "development",
    "description": "Systematic code review workflow",
    "requires_tools": ["file_read", "file_list", "bash"],
    "requires_role": "critic",
    "budget_max": 5000,
    "dangerous": false,
    "enabled": true,
    "content": "# Code Review Skill\n\nYou are performing..."
  },
  "metadata": {
    "source_path": "/app/config/skills/core/code-review.md",
    "content_hash": "abc123...",
    "loaded_at": "2026-01-26T10:00:00Z"
  }
}
```

### 列出 Skill 版本

```bash
GET /api/v1/skills/{name}/versions
```

响应：

```json
{
  "name": "code-review",
  "versions": [
    {"name": "code-review", "version": "2.0.0", ...},
    {"name": "code-review", "version": "1.0.0", ...}
  ],
  "count": 2
}
```

## 创建自定义 Skill

### 步骤 1：创建 Skill 文件

在 `config/skills/user/` 下创建一个 `.md` 文件：

```bash
mkdir -p config/skills/user
cat > config/skills/user/my-analysis.md << 'EOF'
---
name: my-analysis
version: 1.0.0
author: Your Name
category: analysis
description: Custom analysis workflow
requires_tools:
  - file_read
  - file_list
requires_role: generalist
budget_max: 3000
---

# My Analysis Skill

Instructions for the AI agent...

## Step 1: Gather Data
...

## Step 2: Analyze
...

## Output Format
...
EOF
```

### 步骤 2：将目录加入 SKILLS_PATH（可选）

如果使用自定义目录：

```bash
export SKILLS_PATH="config/skills/core:config/skills/user:/custom/skills"
```

### 步骤 3：重启 Gateway

Skill 在 gateway 启动时加载：

```bash
docker compose -f deploy/compose/docker-compose.yml restart gateway
```

### 步骤 4：验证加载

```bash
curl -sS http://localhost:8080/api/v1/skills | jq '.skills[].name'
```

## 环境变量

| 变量 | 默认值 | 说明 |
|----------|---------|-------------|
| `SKILLS_PATH` | `config/skills/core`（开发环境）或 `/app/config/skills/core`（容器） | 扫描 skill 的目录列表，以冒号分隔 |

示例：

```bash
# Multiple directories
export SKILLS_PATH="/app/config/skills/core:/app/config/skills/user:/custom/vendor-skills"
```

## 安全考量

### 危险 Skill

带 `dangerous: true` 的 skill 表示其会执行可能具破坏性的操作：

```yaml
---
name: cleanup-files
dangerous: true
description: Removes temporary files (REQUIRED when dangerous=true)
---
```

危险 skill 的要求：

- 必须有非空的 `description` 字段
- 应配合用户的显式确认使用
- 生产环境建议实现审批流程

### 基于角色的访问控制

`requires_role` 字段对应 Shannon 的 role 预设：

| Role | 说明 | 典型 Tool |
|------|-------------|---------------|
| `generalist` | 通用 agent | file_read, file_list, bash, web_search |
| `critic` | 代码审查与分析 | file_read, file_list, bash |
| `developer` | 开发任务 | file_read, file_write, file_list, bash, python_executor |

设置 `requires_role` 时，orchestrator 会跳过任务分解，以指定 role 按单 agent 方式运行任务。

### Tool 限制

`requires_tools` 字段声明该 skill 预期使用哪些 tool：

```yaml
requires_tools:
  - file_read
  - file_list
  - bash
```

这起到文档作用，并可用于：

- 预先校验所需 tool 是否可用
- 访问控制策略
- 审计日志

## 最佳实践

1. **保持具体**：skill 应提供清晰的分步指引
2. **结构化输出**：在 skill 中定义预期的输出格式
3. **Tool 需求**：只列出 skill 实际使用的 tool
4. **版本管理**：skill 更新使用语义化版本
5. **测试 Skill**：部署前验证 skill 能产生预期结果
6. **记录上下文**：说明 skill 期望的输入内容
7. **错误处理**：指导 agent 处理边界情况

## 示例：内置 Skill

### code-review

系统化的代码审查 workflow：

- 安全分析（注入、XSS、鉴权缺口）
- 代码质量指标
- 性能审查
- 测试覆盖率分析

### debugging

结构化的调试方法：

- 理解问题
- 收集信息
- 提出假设并验证
- 根因分析
- 实现解决方案

### test-driven-dev

测试驱动开发 workflow：

- 先写会失败的测试
- 实现最小代码使其通过
- 有信心地重构

## 故障排查

### Skill 未加载

1. 检查目录是否存在：
   ```bash
   ls -la config/skills/core/
   ```

2. 校验 YAML frontmatter 语法：
   ```bash
   head -20 config/skills/core/my-skill.md
   ```

3. 检查 gateway 日志中的加载错误：
   ```bash
   docker compose -f deploy/compose/docker-compose.yml logs gateway | grep -i skill
   ```

### 找不到 Skill（404）

1. 确认 skill 已启用（`enabled: true` 或省略）
2. 检查名称拼写是否准确
3. 确认文件扩展名为 `.md`
4. 确认目录在 `SKILLS_PATH` 中

### 版本冲突

如果两个 skill 具有相同的 `name@version`，加载会失败。请使用唯一版本号，或将 skill 放在不同目录中。
