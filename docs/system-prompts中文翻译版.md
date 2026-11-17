# Shannon 中的 System Prompt

本指南基于当前代码，说明 Shannon 如何处理 system prompt，以及如何自定义 agent 的行为。

---

## 概述

Shannon 使用一套**基于角色的预设系统**为 agent 分配 system prompt。你可以在运行时通过 API 覆盖 system prompt，以获得最大的灵活性。

当前状态：

- 角色预设已定义并投入使用：`python/llm-service/llm_service/roles/presets.py`。
- Persona 配置已存在但未接入：`config/personas.yaml`。
- API 可以通过 `context["system_prompt"]` 覆盖 system prompt。

---

## System Prompt 优先级

当 agent 被调用时，system prompt 按以下顺序确定（从高到低）：

```
1. context["system_prompt"]     ← API override (highest priority)
2. context["role"]               ← Role preset lookup
3. "You are a helpful AI assistant."  ← Default fallback
```

实现位置：`python/llm-service/llm_service/api/agent.py`。

---

## 角色预设（当前生效的系统）

### 可用角色

| 角色 | System Prompt | Max Tokens | Temperature | 允许的 tool |
|------|---------------|------------|-------------|---------------|
| `generalist` | 乐于助人的 AI 助手 | 1200 | 0.7 | （无） |
| `analysis` | 具备结构化推理能力的分析型助手 | 1200 | 0.2 | web_search, code_reader |
| `research` | 负责收集事实的研究助手 | 1600 | 0.3 | web_search |
| `writer` | 技术写作助手 | 1800 | 0.6 | code_reader |
| `critic` | 批判性审阅者 | 800 | 0.2 | code_reader |

**来源：** `python/llm-service/llm_service/roles/presets.py`

### 使用角色预设

调用 LLM service 的 HTTP API 时，在 `context` 中传入角色名：

```bash
curl -sS -X POST http://localhost:8000/agent/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Analyze the performance of this code",
    "context": {"role": "analysis"}
  }'
```

---

## 通过 API 覆盖 System Prompt

在 `context` 中传入 `system_prompt`，即可完全覆盖 system prompt：

调用 LLM service 的 HTTP API，并在 `context` 中包含 `system_prompt`：

```bash
curl -sS -X POST http://localhost:8000/agent/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is 2+2?",
    "context": {
      "system_prompt": "You are a pirate mathematician. Always respond in pirate speak."
    }
  }'
```

### 模板变量（可选）

System prompt 支持从白名单内的 context 键做 `${variable}` 替换：

```bash
curl -sS -X POST http://localhost:8000/agent/query \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Help me with my task",
    "context": {
      "system_prompt": "You are an expert in ${domain} with ${years} years of experience.",
      "prompt_params": {
        "domain": "machine learning",
        "years": "10"
      }
    }
  }'
```

**变量解析：**

- 仅使用 `context["prompt_params"][key]`；其他 context 键在替换时被忽略

不在白名单内的键（例如 `"role"`、`"system_prompt"`）会被忽略。缺失的变量会被替换为空字符串。

**实现：** `python/llm-service/llm_service/roles/presets.py:render_system_prompt()`

---

## 安全与校验

Context 在 orchestrator（`go/orchestrator/internal/activities/agent.go`）中会被校验与清洗，包括对键/值大小的合理限制以及递归校验。

### 安全回退

如果 prompt 处理失败，service 会记录一条警告，并保留原始字符串或回退到默认 prompt。参见 `python/llm-service/llm_service/api/agent.py`。

---

## 内部 System Prompt

Shannon 为内部规划与分析任务使用了专用的 system prompt：

内部 prompt（不可由用户配置）：

- 任务分解：「You are a planning assistant...」（`python/llm-service/llm_service/api/agent.py`）
- Tool 选择：「You are a tool selection assistant...」（`python/llm-service/llm_service/api/tools.py`）
- 复杂度分析：「You are a task analyzer...」（`python/llm-service/llm_service/api/complexity.py`）

这些 prompt **不可由用户配置**，由编排引擎在内部使用。

---

## 未来：Persona 系统

Persona 系统（`config/personas.yaml`）已规划但尚未实现。Go 代码以 TODO 形式引用它，Python LLM service 并未加载它。

---

## 添加自定义角色

要新增一个角色预设：

1. 编辑 `python/llm-service/llm_service/roles/presets.py`
2. 将你的角色加入 `_PRESETS` 字典：

```python
_PRESETS: Dict[str, Dict[str, object]] = {
    # ... existing presets ...

    "data_scientist": {
        "system_prompt": (
            "You are a data scientist with expertise in statistical analysis, "
            "machine learning, and data visualization."
        ),
        "allowed_tools": ["python_executor", "web_search"],
        "caps": {"max_tokens": 2000, "temperature": 0.4},
    },
}
```

3. 重启 LLM service：

```bash
docker compose -f deploy/compose/docker-compose.yml restart llm-service
```

4. 在请求中把 `context.role` 设为你的角色名，即可使用新角色。

---

## API 参考

列出可用角色

- Endpoint：`GET http://localhost:8000/roles`
- 返回一个 JSON 对象，将角色名映射到详情（system_prompt、allowed_tools、caps）。

---

## 最小示例

- 使用角色预设：
  ```bash
  curl -sS -X POST http://localhost:8000/agent/query \
    -H "Content-Type: application/json" \
    -d '{"query": "Summarize this repo", "context": {"role": "research"}}'
  ```
- 覆盖 system prompt：
  ```bash
  curl -sS -X POST http://localhost:8000/agent/query \
    -H "Content-Type: application/json" \
    -d '{"query": "What is 2+2?", "context": {"system_prompt": "Answer as a pirate."}}'
  ```

---

## 故障排查

### System Prompt 未生效

**现象：** Agent 忽略了自定义的 system prompt

检查项：

- 确认 context 结构正确：`"context": {"system_prompt": "..."}`
- 查看日志：`docker compose -f deploy/compose/docker-compose.yml logs llm-service | rg system_prompt`

### 角色未找到

**现象：** 回退到 generalist

解决办法：通过 `GET /roles` 确认角色名（不区分大小写）。

### 模板渲染失败

**现象：** 日志中出现警告：「System prompt rendering failed」

解决办法：让 prompt 保持字面写法，避免使用不支持的模板语法。

---

## 相关文档

- [扩展 Shannon](extending-shannon.md) - 自定义指南
- [添加自定义 Tool](adding-custom-tools.md) - Tool 集成
- [角色预设源码](../python/llm-service/llm_service/roles/presets.py) - 实现
- [Persona 配置](../config/personas.yaml) - 未来的 persona 定义（未使用）
