# Swarm agent

## 概述

Swarm 模式部署的是持久化、自主的 agent，它们由 **Lead Agent** 编排，依托一个事件驱动的决策循环，循环中带有 **file_read 内循环**。Lead 把用户查询拆解为 task，孵化角色专精的 agent，通过 event（idle、completed、checkpoint）监控其进展，读取工作区文件来核验质量（零 LLM 成本），并协调多阶段执行直至全部工作完成。收尾 checkpoint 决定 Lead 是直接回复，还是触发 LLM 综合。

Agent 运行 reason-act 循环，最多 50 轮迭代，其间使用 tool、通过工作区文件系统共享发现，并在当前 task 完成后进入 **idle** 状态（而非 done）——进入 idle 之前必须做一次 **QUALITY SELF-CHECK** 并给出 `key_findings`。Lead 掌控 agent 的生命周期，可以把 idle 的 agent 重新指派到新 task。Agent 可以向 `"lead"` 发送 `send_message`，把问题上报给 Lead。

## 架构

```
┌──────────────────────────────────────────────────────────────────────┐
│                          SwarmWorkflow                                │
│  Phase 1: Lead initial_plan → Phase 2: event loop → Phase 3: close  │
└──────────┬──────────────────────┬────────────────────┬───────────────┘
           │                      │                    │
     ┌─────▼──────┐        ┌─────▼──────┐      ┌──────▼──────┐
     │ AgentLoop   │        │ AgentLoop   │      │ Lead Agent  │
     │ (Takao)     │        │ (Mitaka)    │      │ /lead/decide│
     │ role:       │        │ role:       │      │ event-driven│
     │ researcher  │        │ analyst     │      │ loop        │
     └──────┬──────┘        └──────┬──────┘      └──────┬──────┘
            │                      │                     │
            │   idle / completed   │    agent_idle /     │
            │   ─────────────────► │    agent_completed  │
            │                      │    ────────────────►│
            │                      │                     │
      ┌─────▼──────────────────────▼─────┐               │
      │     Session Workspace (Files)     │◄──── reads ───┘
      │   file_write / file_read / etc.   │
      └───────────────────────────────────┘
```

**关键组件：**

| 组件 | 文件 | 用途 |
|-----------|------|---------|
| SwarmWorkflow | `go/.../workflows/swarm_workflow.go` | 顶层 Temporal workflow（Lead 循环 + agent 协调） |
| AgentLoop | 同一文件（子 workflow） | 每个 agent 的 reason-act 循环 |
| LeadDecision | `go/.../activities/lead.go` | 调用 `/lead/decide` 的 activity + file_read 内循环 |
| Lead protocol | `python/.../roles/swarm/lead_protocol.py` | Lead 的 system prompt、event 类型、12 个 action |
| Agent protocol | `python/.../roles/swarm/agent_protocol.py` | Agent 的 system prompt、4 个 action + QUALITY SELF-CHECK |
| Role prompts | `python/.../roles/swarm/role_prompts.py` | 12 个角色各自的方法论 prompt |
| Lead endpoint | `python/.../api/lead.py` | `/lead/decide` HTTP endpoint |
| P2P + TaskList | `go/.../activities/p2p.go` | 邮箱、工作区、task CRUD（Redis） |
| Config | `config/features.yaml` + `go/.../activities/config.go` | Swarm 参数与默认值 |
| Synthesis template | `config/templates/synthesis/swarm_default.tmpl` | 最终输出的格式化 |
| Agent names | `go/.../agents/names.go` | 确定性的站点名生成 |
| Stream events | `go/.../activities/stream_events.go` | SSE event 发送 |

## 生命周期

### 阶段 1：Lead 初始规划

SwarmWorkflow 收到一个 `TaskInput`，其 context 中带有 `"force_swarm": true`。**Lead Agent** 不做静态拆解，而是自己做出第一个决策：

1. Lead 收到一个 `initial_plan` event，其中包含用户查询
2. Lead 把查询拆解为 task（ID 形如 `T1`、`T2`、`T3`）
3. Lead 决定孵化哪些 agent，为每个 agent 指定 **role** 和 **task**
4. Lead 可以在 task 之间设置 `depends_on` 关系

Lead 在初始规划中给出的 action 会被执行：task 存入 Redis，agent 作为子 workflow 孵化，并带角色专属的 prompt。

### 阶段 2：Agent 执行 + Lead event 循环

每个 agent 运行一个 **AgentLoop**（子 workflow），最多执行 `max_iterations_per_agent` 轮（默认：50）reason-act 循环。

**Agent 迭代循环：**
```
┌──────────────────────────────────────────────────────────────┐
│  1. Non-blocking shutdown check (signal from Lead)           │
│  2. Inject context: Task Board + Running Notes + Role prompt │
│  3. Call LLM (/agent/loop) with history + workspace state    │
│  4. Execute action:                                          │
│     • tool_call  → run tool, record result                   │
│     • publish_data → share findings with team workspace      │
│     • send_message → P2P to specific agent                   │
│     • idle → signal parent, wait for reassignment            │
│  5. If agent says "done" and TeamRoster exists → convert to  │
│     "idle" (Lead controls lifecycle)                         │
│  6. Check convergence / error thresholds                     │
│  7. Loop or exit                                             │
└──────────────────────────────────────────────────────────────┘
```

**Lead 事件驱动循环**（并发运行）：

Lead 通过 Temporal signal 与定时器监控 agent：

| Event 类型 | 触发时机 | Lead 的响应 |
|-----------|---------|---------------|
| `agent_idle` | Agent 完成 task，正在等待 | 分配新 task、孵化助手，或关闭 |
| `agent_completed` | Agent 子 workflow 结束 | 质量门禁（ACCEPT/RETRY），更新 task 状态 |
| `checkpoint` | 每 2 分钟 | 审视进展，必要时修订计划 |
| `human_input` | 用户发来 HITL 消息 | 吸收反馈，调整计划 |

每遇到一个 event，Lead 都会调用 `/lead/decide`，传入：当前 TaskList、agent 状态、budget、决策历史，以及任何 agent→Lead 消息。Lead 返回的 action 由 workflow 执行。若 Lead 返回 `file_read` action，workflow 会读取所请求的文件，把文件内容注入后再次调用 `/lead/decide`——这个 **file_read 内循环** 每个 event 最多跑 3 轮，LLM 成本为零。Lead 还可以返回 `interim_reply`，在不打断 workflow 的前提下发出用户可见的进度消息。

### 阶段 3：收尾与综合

一旦所有 agent 完成（或 budget 耗尽）：

1. **构建收尾摘要** —— 汇总 agent 结果 + 工作区文件清单
2. **Lead 收尾 checkpoint** —— Lead 用 `closing_checkpoint` event 审视全部工作
3. **Lead 只选一个 action**（第一个 action 生效，其余忽略）：
   - `reply`：Lead 直接给出答案。由 `isLeadReplyValid()` 对照工作区文件校验——不合法则回落到综合。合法时跳过综合阶段的 LLM 调用。
   - `synthesize`：用 `swarm_default.tmpl` 模板触发完整的 LLM 综合
   - `done`：旧版/向后兼容——触发综合
4. **返回** —— 带 metadata 的 TaskResult（每个 agent 的摘要、迭代轮数、token、model）

**回复准则**：如果 `synthesis_writer` 或 analyst 已经写出了完整报告，Lead 的 reply 可以很短（3-5 句），只需引用报告文件路径。如果没有专职的综合 agent，Lead 的 reply 应完整回答用户查询。

## 触发 Swarm 模式

### API payload

```bash
curl -X POST /api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Compare AI chip markets across US, Japan, and South Korea",
    "session_id": "session-123",
    "context": {
      "force_swarm": true
    }
  }'
```

**重要**：context 中的 `force_swarm: true` 是触发 SwarmWorkflow 的**唯一**方式。`orchestrator_router.go` 里的早期路由会在拆解之前检查 `GetContextBool(input.Context, "force_swarm")`。使用 `execution_mode: swarm` **不会**触发 SwarmWorkflow——它走的是常规的 decompose → DAG 流程。

### HITL：执行中的人工输入

用户可以通过以下方式向运行中的 swarm 发送消息：

```bash
POST /api/v1/swarm/{workflowID}/message
Content-Type: application/json

{"message": "Focus more on Samsung's foundry strategy"}
```

消息会以 `human_input` event 的形式抵达 Lead 的事件循环，Lead 据此调整计划。

## Lead Agent

### Event 类型

Lead 会在以下 event 上被唤醒：

| Event | 何时 | 典型 action |
|-------|------|-----------------|
| `initial_plan` | Workflow 启动 | `interim_reply`（必须）+ `revise_plan`（创建 task）+ `spawn_agent` |
| `agent_idle` | Agent 完成当前 task | `file_read`（核验）、`assign_task`、`shutdown_agent`，或 `noop` |
| `agent_completed` | Agent 子 workflow 结束 | 质量门禁，可选 `file_read`，必要时补一个替补 agent |
| `checkpoint` | 每 2 分钟 | 审视、`revise_plan`、`broadcast` |
| `human_input` | 用户 HITL 消息 | `interim_reply`（必须）+ 依据反馈调整计划 |
| `closing_checkpoint` | 所有 agent 完成 | `reply` 或 `synthesize`（只选一个 action） |

### 可用 action

| Action | 参数 | 效果 |
|--------|-----------|--------|
| `interim_reply` | `content`（1-3 句） | 用户可见的进度消息；非终止性，执行继续 |
| `spawn_agent` | `role`、`task_description`、`task_id`、`model_tier` | 创建新的 AgentLoop 子 workflow |
| `assign_task` | `task_id`、`agent_id`、`task_description`、`model_tier` | 把 idle 的 agent 改派到新 task |
| `send_message` | `to`、`content` | 向指定 agent 发 P2P 消息 |
| `broadcast` | `content` | 向所有 agent 广播 |
| `revise_plan` | `create`（带 `depends_on` 的新 task）、`cancel`（task ID） | 动态更新 TaskList |
| `file_read` | `path` | 读取工作区文件用于核验（零 LLM 成本，触发内循环） |
| `shutdown_agent` | `agent_id` | 优雅停止 agent |
| `noop` | — | 等待运行中的 agent（agent 还在跑时绝不要用 `done`） |
| `done` | — | 所有 agent 完成，进入综合 |
| `reply` | `content`（Markdown） | 直接作答，跳过综合（仅限 closing_checkpoint） |
| `synthesize` | — | 触发 LLM 综合（仅限 closing_checkpoint） |

### 质量门禁

在每一个 `agent_completed` 或 `agent_idle` event 上，Lead 都会跑一遍质量门禁：

**步骤 1 —— 可选的文件核验（零 LLM 成本）：**
- Agent 写了文件 → 用 `file_read` 检查主要交付物的内容
- Agent 声称了具体数字 → 在文件里核对
- 只有在 task 简单、或 `key_findings` 已明确给出具体数据时才跳过

**步骤 2 —— 决策：**

| 决策 | 判据 | 动作 |
|----------|----------|--------|
| **ACCEPT**（默认） | `key_findings` 含具体数据/数字/证据；文件内容（若已读取）证实确有实质内容 | 继续 |
| **RETRY ONCE**（少见） | 结果为空或残缺、不足 50 词，或文件为空/不符合要求 | 指派一次跟进，前缀为「CONTINUE:」 |
| **ANTI-SPIRAL** | 防止核验死循环 | 绝不创建 verify/check/diagnostic 类 task；`file_read` 不算核验 task（0 LLM 成本）；当 2 个以上 agent 处于 idle + 没有待办 task + 0 个在跑 → **立刻** `done`；有 agent 在跑 → 用 `noop` |

### Lead 的 file_read 内循环

当 Lead 需要在做质量决策前核验 agent 的输出时，它可以使用 `file_read` action：

```
Event: agent_completed (researcher)
  → Lead returns: file_read path="/research/findings.md"
  → Go reads file (max 4000 chars, truncation flag if longer)
  → Go calls /lead/decide again with file_contents injected
  → Lead sees actual content, makes ACCEPT/RETRY decision
```

**约束：**
- 每个 event 最多 3 轮文件读取（防止无限循环）
- 每轮最多 3 个文件
- 每个文件最多 4000 字符（超出会截断并加 `[TRUNCATED]` 标记）
- 零 LLM 成本 —— 纯文件 I/O，不消耗 token

### 自适应规划

审视完某个已完成 agent 的输出后，Lead 可以动态修订计划：

- Agent 发现了值得另开一个跟进 task 的东西 → 用 `revise_plan` 创建它（带 `depends_on`）
- Agent 发现原定范围不对 → 用 `revise_plan` 取消无关 task
- 发现揭示出原计划之外的缺口 → 创建补缺 task + 指派 idle 的 agent

**约束：**
- 绝不创建 verify/check/diagnostic 类 task（反螺旋）
- 绝不只因为结果不完美就修订——接受并继续
- 每次修订最多新增 2 个 task——把范围控住
- 只有 agent 的发现带来真正**新**信息时才修订

### 临时回复（用户进度更新）

Lead 可以通过 `interim_reply` 发出用户可见的进度消息，而不打断 workflow：

**何时应包含：**
- `initial_plan`：始终 —— 在孵化 agent 之前说明思路
- 阶段切换（例如 research 完成 → 综合）：要
- `human_input` 响应：始终 —— 确认并解释调整

**何时不应包含：**
- 例行的 `noop`/等待：绝不
- `checkpoint`：绝不

**规则：**
- 每个决策最多 1 条 `interim_reply`
- 使用与用户查询相同的语言回复
- 聚焦于用户视角「正在发生什么」，而非 agent 内部（不提 agent 名、task ID）

### LLM 配置

- **模型层级**：MEDIUM（始终）
- **Temperature**：0.3
- **最大 token 数**：2048（收尾 checkpoint：4096）
- **System prompt**：来自 `lead_protocol.py` 的 `LEAD_SYSTEM_PROMPT`
- **Assistant prefill**：`{`（确保输出合法 JSON）

## 角色系统

### 三层 prompt 架构

```
┌─────────────────────────────────┐
│  Layer 1: Core Protocol         │  ← AGENT_LOOP_SYSTEM_PROMPT (shared by all agents)
│  Available actions, memory mgmt │     agent_protocol.py
├─────────────────────────────────┤
│  Layer 2: Role Methodology      │  ← SWARM_ROLE_PROMPTS[role] (per-role specialization)
│  Domain expertise, approach     │     role_prompts.py
├─────────────────────────────────┤
│  Layer 3: Dynamic Context       │  ← Built per iteration (task, team, history, budget)
│  Task, workspace, inbox, notes  │     agent.py / swarm_workflow.go
└─────────────────────────────────┘
```

### 角色目录（12 个角色）

9 个核心角色在 `SWARM_ROLE_PROMPTS` 中带 swarm 方法论 prompt，另有 3 个扩展角色使用 `presets.py` 的 system prompt：

| 角色 | 专精方向 | 类型 |
|------|---------------|------|
| `researcher` | 信息收集、市场分析、事实查证 | Core |
| `company_researcher` | 公司尽职调查、企业背景、竞争格局 | Core |
| `analyst` | 数据分析、统计、对比、图表 | Core |
| `financial_analyst` | 财务分析、估值、多空论点、风险评估 | Core |
| `planner` | 战略拆解、研究设计、依赖关系梳理 | Core |
| `critic` | 批判性审阅、核验、找缺口 | Core |
| `coder` | 代码实现、脚本、调试 | Core |
| `generalist` | 灵活，承接任何简单/混合 task | Core |
| `synthesis_writer` | 把所有发现综合成结构化报告 | Core |
| `writer` | 技术写作、报告、文档 | Extended |
| `browser_use` | 浏览器自动化、页面交互 | Extended |
| `deep_research_agent` | 扩展的多步深度研究 | Extended |

### 模型层级选择

Lead 在孵化 agent 时为每个 agent 指定 `model_tier`：

| 层级 | 适用场景 |
|------|----------|
| `small` | 快、便宜 —— 简单查询、格式化 |
| `medium` | 均衡 —— 分析、research、推理（默认） |
| `large` | 能力最强 —— 复杂代码、深度分析 |

## Agent action

每一轮迭代，LLM 只返回一个 action，以 JSON 表示：

### tool_call

从可用集合中执行一个 tool：

```json
{"action": "tool_call", "tool": "web_search", "tool_params": {"query": "NVIDIA H100 market share 2025"}}
```

可用的 tool 包括：`web_search`、`python_executor`、`file_write`、`file_read`、`file_edit`、`file_list`、`file_search`、`diff_files`、`json_query`、`web_fetch`、`web_subpage_fetch`、`web_crawl`、`calculator`。

### send_message

向指定队友或 Lead（用于上报）发送点对点 P2P 消息：

```json
{"action": "send_message", "to": "mitaka", "message_type": "info", "payload": {"message": "Check Samsung foundry plans"}}
```

**Agent→Lead 上报** —— agent 可以向 `"lead"` 发消息，报告阻塞或请求指引：

```json
{"action": "send_message", "to": "lead", "message_type": "request", "payload": {"message": "Cannot find reliable enterprise adoption data for Svelte - should we adjust scope?"}}
```

Lead 会在下一次决策上下文的 `## Agent Messages` 段落里收到这些消息。

### publish_data

通过工作区与团队共享发现：

```json
{"action": "publish_data", "topic": "findings", "data": "US market dominated by NVIDIA with 80% share"}
```

### idle

宣告 task 完成并等待重新指派。进入 idle 前必须做 **QUALITY SELF-CHECK**：

1. 我是否覆盖了 task 描述的所有方面？
2. 发现是否有具体数据、数字、证据支撑？
3. 我是否漏了某个维度？→ 如果有，在 idle 前再搜一次

```json
{
  "decision_summary": "Saved report to research/takao-us-market.md, self-check passed",
  "notes": "see file at research/takao-us-market.md",
  "action": "idle",
  "key_findings": [
    "NVIDIA holds 80% US AI chip market share (2025 Q3)",
    "AMD gained 12% with MI300X, up from 8% YoY",
    "Intel Gaudi3 at 5% share, focused on inference workloads"
  ],
  "response": "Completed US market analysis. Full report in research/takao-us-market.md"
}
```

**`key_findings`**：3-5 条带具体数据的要点。Lead 依靠它们判断质量，无需读完整文件（当然也可以 `file_read` 核验）。

**注意**：如果 agent 在 swarm 中（存在 TeamRoster）返回 `done`，workflow 会自动把它转成 `idle`——agent 生命周期由 Lead 掌控。done 的响应会保留在 `savedDoneResponse` 中，供 idle signal 使用。

### 运行笔记

Agent 有一个 `notes` 字段，作为工作记忆跨所有轮次持久存在。与在历史中会被截断的 tool 结果不同，notes 能留存整个 session：

```json
{"action": "tool_call", "tool": "web_search", "tool_params": {"query": "..."}, "notes": "Key finding: NVIDIA holds 80% market share. Need to verify Samsung data next."}
```
## 任务管理

### SwarmTask 结构

```go
type SwarmTask struct {
    ID          string   `json:"id"`            // e.g. "T1", "T2"
    Description string   `json:"description"`
    Status      string   `json:"status"`        // "pending", "in_progress", "completed"
    Owner       string   `json:"owner"`         // agent ID
    CreatedBy   string   `json:"created_by"`    // "decompose" or agent ID
    DependsOn   []string `json:"depends_on"`    // task IDs that must complete first
    CreatedAt   string   `json:"created_at"`    // RFC3339
    CompletedAt string   `json:"completed_at,omitempty"`
}
```

### Redis 存储

任务存储在一个 Redis hash 中：

```
Key: wf:{workflow_id}:tasklist
Field: {task_id}    (e.g. "T1")
Value: {json}       (serialized SwarmTask)
```

### 任务 activities

| Activity | 用途 |
|----------|------|
| `InitTaskList` | 批量写入 Lead 初始计划中的任务 |
| `GetTaskList` | 获取全部任务，按 ID 排序 |
| `UpdateTaskStatus` | 校验状态流转，更新 status/owner |
| `ClaimTask` | 用于任务领取的原子 Lua 脚本 |
| `CreateTask` | 新增任务（来自 `revise_plan`） |

### 状态流转

```
pending ──► in_progress ──► completed
   │                           ▲
   └───────────────────────────┘  (Lead can directly complete/cancel)
         in_progress ──► in_progress (Lead reassign)
```

### 依赖约束

带 `depends_on` 的任务在所有依赖都变为 `completed` 之前不能被 spawn/分配。workflow 在允许 Lead 分配工作之前会通过 `taskHasUnmetDeps()` 检查。

## P2P 消息传递

agent 之间通过 Redis 支撑的 mailbox 通信。

### 消息类型

| 类型 | 常量 | 使用场景 |
|------|----------|----------|
| `request` | `MessageTypeRequest` | 任务委派或求助请求 |
| `offer` | `MessageTypeOffer` | 主动提出协助 |
| `accept` | `MessageTypeAccept` | 接受任务 |
| `delegation` | `MessageTypeDelegation` | 委派子任务 |
| `info` | `MessageTypeInfo` | 一般信息共享 |

### Redis key

```
wf:{workflow_id}:mbox:{agent_id}:seq    # Atomic counter
wf:{workflow_id}:mbox:{agent_id}:msgs   # List of JSON messages
```

每条消息：
```json
{
  "seq": 1,
  "from": "takao",
  "to": "mitaka",
  "type": "info",
  "payload": {"message": "Found relevant data"},
  "ts": 1707782400000000000
}
```

所有 key 都有 48 小时 TTL，用于自动清理。

## 共享工作区

agent 通过 Redis 中按 topic 划分的 workspace 列表，以及 session 的 workspace 文件来共享发现。

### Redis workspace key

```
wf:{workflow_id}:ws:seq          # Global sequence counter
wf:{workflow_id}:ws:{topic}      # List of entries per topic
```

### File-as-Memory 模式

agent 把大型结果外置到 workspace 文件：

1. **tool 结果 > 500 字符** → 保存到文件，记下文件名
2. **进入 idle 之前** → 把完整发现写入 `findings/{agent-id}-{topic}.md`
3. **idle 响应** → 只给简短摘要（约 1500 字符以内）
4. **开始调研之前** → 用 `file_read` + `file_list` 检查队友的文件

文件命名约定：
- 报告：`findings/{agent-id}-{topic}.md`
- 代码：相应的源码文件
- 数据：`data/` 目录

### 用户 memory（swarm 专属）

用户 memory 是 **swarm 专属**的 —— 提取与召回都只限定在 SwarmWorkflow 内，以免简单查询产生的低价值提取把 memory 目录撑大。

**召回（读）**：workflow 启动时，`user_memory_prompt` 被注入到 `input.Context`，指示 agent 读取 `/memory/MEMORY.md` 获取过往发现的索引，再按需读取具体文件了解细节。

**提取（写）**：swarm 任务成功完成后，orchestrator 会在两条成功路径上（Lead 回复 + synthesis）运行 `ExtractMemoryActivity`。该 activity 调用 Python 的 `/memory/extract` endpoint（由 LLM 判断这些发现是否值得持久化），然后写入 EFS 的 `/mnt/shannon-users/{user_id}/memory/`。它是尽力而为的（`MaximumAttempts: 1`），并且是 await 的（不是 fire-and-forget），以避免子 workflow 返回时被取消。

**存储**：`MEMORY.md` 是自动维护的索引，以 `## filename` 作为标题，每条附一行摘要。内容文件保存完整提取出的知识。每个用户 10MB 磁盘配额，由 Rust 的 `MemoryManager` 强制执行。

agent 通过文件 tool（`file_read`、`file_write`）中的 `/memory/` 路径前缀访问 memory，该路径经由 gRPC 路由到 Rust 的 SandboxService（而不是 Firecracker）。

## 收敛检测

有三种机制防止 agent 无限循环：

### 1. 无进展收敛

如果 agent 连续 3 次执行非 tool 动作（只是 idle 而没有进展），就会收敛。任何 `tool_call`、`send_message` 或 `publish_data` 动作都会重置计数（协作类动作算作进展）。

### 2. 连续错误中止

如果连续出现 3 次 **permanent** tool 错误（不是瞬时错误），agent 会中止。任何一次成功的 tool 执行都会重置计数。

### 3. 最大迭代强制完成

在最后一轮迭代时，如果 agent 还没有进入 idle，workflow 会强制其完成。距上限还有两轮迭代时，prompt 会加入一条警告。

## 智能 retry

瞬时错误会自动 retry 并带 backoff；permanent 错误会计入中止计数。

| 错误类型 | 行为 | Backoff | 是否计入中止？ |
|-----------|----------|---------|---------------------|
| Transient（瞬时） | 带 backoff 重试 | 5s x 尝试次数（上限 30s） | 否 |
| Permanent（永久） | 记录失败 | 无 | 是（3 次即中止） |

backoff 使用 `workflow.Sleep()` 以保证 Temporal 的确定性。

## idle snapshot 回退

当子 workflow 超时（例如 agent 在阶段之间处于 idle），swarm 会回退到超时前捕获的**最后一个 idle snapshot**：

1. **捕获**：每次收到 `agent_idle` 信号时，把 `AgentLoopResult` 存入 `idleSnapshots` map
2. **回退**：如果 `future.Get()` 返回 `!result.Success`，检查 `idleSnapshots[agentID]`
3. **使用 snapshot**：如果 snapshot 的响应比错误结果更长，就用它
4. **多阶段 context**：重新分配 agent 时，把上一次的 idle snapshot 作为 context 注入（截断到 1500 字符）

这对于修复多阶段 swarm 空结果 bug（agent 在阶段之间超时）至关重要。

## Budget 管理

swarm 强制三条 budget 上限：

| Budget 项 | 默认值 | 来源 |
|--------|---------|--------|
| 最大 LLM 调用数 | 200 | 硬编码在 SwarmWorkflow 中 |
| 最大 token 数 | 500,000 | 硬编码在 SwarmWorkflow 中 |
| 最大 wall-clock | 30 分钟 | `features.yaml` 的 `max_wall_clock_minutes` |

**统计**：每次 Lead 决策和 agent 迭代都会递增 `budgetTotalLLMCalls` 和 `budgetTotalTokens`。wall clock 从 `swarmStartTime` 起算。

**Budget 信息**在每次决策中通过 `LeadBudget` 传给 Lead：
```go
type LeadBudget struct {
    TotalLLMCalls       int
    RemainingLLMCalls   int
    TotalTokens         int
    RemainingTokens     int
    ElapsedSeconds      int
    MaxWallClockSeconds int
}
```

**耗尽**：任一上限被超出时，workflow 会强制退出 event loop，转入收尾/synthesis。

## 分层历史

agent 的 context 会随每次迭代增长。分层截断让它保持在可管理的范围内：

| 迭代新旧 | 最大字符数 | 理由 |
|--------------|-----------|-----------|
| 最近 3 轮 | 4,000 | 近期工作保留完整细节 |
| 更早的轮次 | 500 | 仅保留摘要 |

### Token 感知裁剪

如果完整 prompt 超过 400K 字符（约 100K token），就从最旧的历史条目开始丢弃直到装得下，但至少保留最近 3 条。

## 配置

### features.yaml

```yaml
workflows:
  swarm:
    enabled: true
    max_agents: 10                    # Max total agents (initial + dynamic)
    max_iterations_per_agent: 50      # Max reason-act loops per agent
    agent_timeout_seconds: 1800       # Per-agent timeout (must cover idle wait between phases)
    max_messages_per_agent: 20        # Max P2P messages per agent
    workspace_snippet_chars: 800      # Max chars per workspace entry in prompt
    workspace_max_entries: 5          # Max recent entries shown to agents
    # Global budget controls
    max_total_llm_calls: 200          # Safety net for dynamic spawn + idle + Lead overhead
    max_total_tokens: 500000          # Total tokens across all agents + Lead
    max_wall_clock_minutes: 30        # Hard wall-clock limit for entire swarm workflow
```

### Go 默认值（config.go）

如果 YAML 中的值缺失或为零，则应用以下默认值：

| 参数 | 默认值 | 说明 |
|-----------|---------|-------|
| `SwarmMaxAgents` | 10 | 总上限，包含动态 spawn |
| `SwarmMaxIterationsPerAgent` | 25 | 每个 agent 的迭代上限（YAML 会覆盖为 50） |
| `SwarmAgentTimeoutSeconds` | 1200 | 每个 agent 20 分钟（YAML 会覆盖为 1800） |
| `SwarmMaxMessagesPerAgent` | 20 | P2P 消息上限 |
| `SwarmWorkspaceSnippetChars` | 800 | 注入 prompt 时的截断长度 |
| `SwarmWorkspaceMaxEntries` | 5 | 每个 topic 的最近条目数 |

### Temporal 超时

| Activity | 超时 | 重试 |
|----------|---------|---------|
| Agent LLM 调用（`/agent/loop`） | 30-90s | 2 |
| Lead 决策（`/lead/decide`） | 90s | 2 |
| P2P activities（mailbox、workspace） | 10s | 1 |
| event 发送 | 5s | 1 |
| Synthesis | 10 min | 3 |
| AgentLoop 子 workflow | `agent_timeout_seconds` | — |

## 流式 event

SwarmWorkflow 为实时看板发出 SSE event：

| event 类型 | Agent ID | Payload | 触发时机 |
|-----------|----------|---------|------|
| `WORKFLOW_STARTED` | `swarm-lead` | — | workflow 开始 |
| `AGENT_STARTED` | `{agent-name}` | `{role: "researcher"}` | agent 带 role 被 spawn |
| `AGENT_COMPLETED` | `{agent-name}` | — | agent 结束（idle/收敛/中止） |
| `TASKLIST_UPDATED` | `tasklist` | `{tasks: SwarmTask[]}` | 任务列表变化（初始化、状态更新、新建） |
| `LEAD_DECISION` | `swarm-lead` | `{event_type, actions_count}` | Lead 的协调决策 |
| `INTERIM_REPLY` | `swarm-lead` | `{message: string}` | Lead 给用户的进度更新（经 LLM_OUTPUT stream） |
| `MESSAGE_SENT` | `{sender}` | — | P2P 消息已发送 |
| `MESSAGE_RECEIVED` | `{receiver}` | — | P2P 消息已投递 |
| `WORKSPACE_UPDATED` | `workspace` | — | 发布了新的 workspace 条目 |
| `WORKFLOW_COMPLETED` | `swarm-lead` | — | 最终 synthesis 完成 |

### TASKLIST_UPDATED payload

```json
{
  "type": "TASKLIST_UPDATED",
  "agent_id": "tasklist",
  "message": "task=T1 status=in_progress",
  "payload": {
    "tasks": [
      {
        "id": "T1",
        "description": "Research US AI chip market",
        "status": "in_progress",
        "owner": "takao",
        "created_by": "decompose",
        "depends_on": [],
        "created_at": "2026-02-26T10:00:00Z"
      },
      {
        "id": "T2",
        "description": "Research Japan AI chip market",
        "status": "pending",
        "owner": "",
        "depends_on": ["T1"],
        "created_at": "2026-02-26T10:00:00Z"
      }
    ]
  }
}
```

### 流式订阅示例

```bash
# Watch swarm task board and agent lifecycle
curl -N "http://localhost:8081/stream/sse?workflow_id=$WF&types=TASKLIST_UPDATED,AGENT_STARTED,AGENT_COMPLETED,LEAD_DECISION"
```

## Agent prompt 结构

每次 `/agent/loop` 调用都会构造如下 prompt：

```
[System]
  Layer 1: AGENT_LOOP_SYSTEM_PROMPT
    - Available actions (tool_call, publish_data, send_message, idle)
    - Memory management rules (file-as-memory, notes)
    - Work phases: ORIENT → EXECUTE → SAVE & COMPLETE
    - Collaboration rules (check before duplicating, share via files)

  Layer 2: ROLE METHODOLOGY (from role_prompts.py)
    - Domain-specific approach and expertise
    - e.g. "researcher" methodology, "analyst" methodology

[User]
  ## Task
  {agent's task description, with previous work context if reassigned}

  ## Your Team (shared session workspace)
  - **takao (you)** [researcher]: "Research US AI chip market"
  - mitaka [analyst]: "Analyze comparative data"
  - kichijoji [writer]: "Write final report"

  ## Task Board
  - T1 [completed] Research US AI chip market (owner: takao)
  - T2 [in_progress] Analyze data (owner: mitaka)
  - T3 [pending] Write report → depends on T1, T2

  ## Running Notes (persistent working memory)
  {accumulated notes from previous iterations}

  ## Shared Findings (workspace)
  - takao: NVIDIA dominates US with 80% market share...
  - mitaka: Japan focuses on edge AI chips...

  ## Previous Actions
  - Iteration 0: tool_call:web_search → {full 4000-char result}   ← recent
  - Iteration 1: tool_call:web_search → {full 4000-char result}   ← recent
  - Iteration 2: publish_data → {full 4000-char result}           ← recent
  - Iteration 3: tool_call:web_search → {500-char summary}        ← older

  ## Inbox Messages
  - From mitaka (info): {"message": "Check Samsung foundry plans"}

  ## Budget: Iteration 4 of 50

[Assistant prefill] {
```

`{` 的 prefill 确保 LLM 返回以 `{` 开头的合法 JSON。

## 模型 tier

agent 的 LLM 调用使用 Lead 分配的 tier（默认：**MEDIUM**），参数为：
- Temperature：0.3
- 最大输出 token：2,048

Lead 决策始终使用 **MEDIUM tier**。

最终 synthesis（如果触发）走标准 synthesis 路径，该路径强制使用 **LARGE tier**。

## 关键源码文件

| 文件 | 作用 |
|------|-------------|
| `go/orchestrator/internal/workflows/swarm_workflow.go` | SwarmWorkflow + AgentLoop 两个 Temporal workflow（约 2300 行） |
| `go/orchestrator/internal/activities/lead.go` | LeadDecision activity + file_read 内层循环，LeadEvent/LeadAction/LeadBudget 类型 |
| `go/orchestrator/internal/activities/p2p.go` | P2P mailbox、workspace、SwarmTask 增删改查、Redis 操作 |
| `go/orchestrator/internal/activities/stream_events.go` | SSE event 类型常量与发送 |
| `go/orchestrator/internal/activities/config.go` | SwarmConfig 结构体与兜底默认值 |
| `go/orchestrator/internal/agents/names.go` | agent 名称生成（日本站名） |
| `python/llm-service/llm_service/api/lead.py` | `/lead/decide` endpoint、prompt 构造、响应解析 |
| `python/llm-service/llm_service/roles/swarm/lead_protocol.py` | `LEAD_SYSTEM_PROMPT` —— Lead 的 event 处理 + 动作 |
| `python/llm-service/llm_service/roles/swarm/agent_protocol.py` | `AGENT_LOOP_SYSTEM_PROMPT` —— agent 动作 + QUALITY SELF-CHECK + key_findings |
| `python/llm-service/llm_service/roles/swarm/role_prompts.py` | `SWARM_ROLE_PROMPTS` —— 12 种 role 的方法论定义 |
| `config/features.yaml` | Swarm 配置段（L114-128） |
| `config/templates/synthesis/swarm_default.tmpl` | Swarm synthesis 输出模板 |
