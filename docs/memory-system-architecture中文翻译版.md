# Shannon Memory 系统架构

> **版本 3.0** - 具备学习能力的增强型 Supervisor Memory

## 概述

Shannon 的 memory 系统在用户 session 之间提供智能的 context 保留与检索，使 agent 能够保持对话连续性，并利用历史交互来改进回复。

## 架构组件

### 1. 存储层

#### PostgreSQL
- **Session Context**：session 级状态与元数据
- **执行持久化**：agent 与 tool 的执行历史
- **Task 追踪**：高层 task 与 workflow 元数据
- **用户管理**：认证与授权

#### Redis
- **Session Cache**：快速访问活跃 session 数据
- **Token Budget**：实时 token 用量追踪
- **压缩状态**：追踪 context 压缩状态

#### Qdrant（向量存储）
- **Semantic Memory**：高性能向量相似度检索
- **集合组织**：task_embeddings、summaries、tool_results、cases、document_chunks、decomposition_patterns
- **混合检索**：结合时效性与语义相关性
- **分块存储**：高效处理长问答对

### 2. 依赖

#### 对 Embedding Provider 的要求

**Memory 功能需要 OpenAI API 访问权限以生成文本 embedding。**

- **默认模型**：`text-embedding-3-small`（1536 维）
- **必需用于**：语义检索、分层 memory、agent memory 检索
- **降级行为**：若未配置 OpenAI key，memory 操作会静默降级：
  - workflow 继续正常执行
  - memory 检索返回空结果（不抛出错误）
  - agent 以「无状态」模式运行，没有历史 context

**配置**：
```bash
# Required for memory features
OPENAI_API_KEY=sk-...
```

**注意**：目前只有 OpenAI provider 实现了 `generate_embedding()`。仅使用 Anthropic/XAI/其他 provider 运行 Shannon 会禁用 memory 功能。这是有意设计，以便优雅降级。

### 3. Memory 类型

#### 分层 Memory
组合多种检索策略：
- **近期 Memory**：当前 session 中最近 N 次交互
- **语义 Memory**：基于 query 相似度的上下文相关内容
- **压缩摘要**：较早对话的浓缩表示

#### Session Memory
按时间顺序检索 session 内的近期交互。

#### Agent Memory
单个 agent 的执行记录，包括：
- 输入 query 与生成的回复
- Token 用量与模型信息
- Tool 执行与结果
- 用于 strategy 选择的性能指标

#### 增强型 Supervisor Memory
面向智能 task 拆解的策略性 memory：
- **Decomposition Patterns**：可复用的成功 task 拆解
- **Strategy 性能**：按 strategy 类型聚合的指标
- **团队构成**：成功的 agent 团队配置
- **失败模式**：已知失败及缓解策略
- **用户偏好**：推断出的专业水平与交互风格

### 4. 持久化层

#### Agent 执行追踪
- Workflow ID、Agent ID 与 Task ID 的关联
- 成功/失败状态及错误详情
- Token 消耗与耗时指标
- 用于性能分析的 strategy 元数据

#### Tool 执行日志
- Tool 名称、参数与输出
- 成功/失败追踪
- 关联的 agent 与 workflow context

### 5. 核心特性

#### 高级分块系统
- **智能文本分块**：切分长回答（>2000 token）
- **幂等存储**：通过 qa_id 与 chunk_index 去重
- **批量 Embedding**：单次 API 调用处理所有分块
- **智能重建**：从分块重新组装完整回答
- **重叠策略**：200 token 重叠以保留 context

#### Context 压缩
- **自动触发**：基于消息数量与 token 估算
- **限流**：防止过度压缩操作
- **模型感知阈值**：不同模型层级使用不同限额
- **发后即忘式存储**：非阻塞持久化

#### Memory 检索策略

**分层检索（默认）**
1. 从 session 获取近期消息
2. 对相关历史执行语义检索
3. 合并并去重结果
4. 以 `agent_memory` 形式注入 agent context

**Fallback 链**
1. 主要：分层 memory
2. 次要：简单 session memory
3. 三级：不注入 memory（新 session）

#### 版本门控
Memory 功能由 Temporal workflow 版本门保护：
- `memory_retrieval_v1`：分层 memory 系统
- `session_memory_v1`：基础 session memory
- `context_compress_v1`：context 压缩
- `supervisor_memory_v2`：增强型 supervisor memory
- **[未实现]** `performance_selection_v1`：基于性能的路由

## 数据库 Schema

### 核心表

#### 主表（PostgreSQL）
- `task_executions`：中央 task 追踪，含完整指标（88+ 行，活跃使用中）
- `sessions`：session 元数据与 context，支持 external_id
- `agent_executions`：agent 执行记录，含性能指标
- `tool_executions`：tool 调用历史
- `users`：用户管理

#### Supervisor Memory 表（PostgreSQL）
- `failure_patterns`：已知失败模式（3 行，**活跃使用中**）
  - 追踪：rate_limit、context_overflow、ambiguous_request 等模式
  - 包含缓解策略与严重级别
- `decomposition_patterns`：task 拆解历史（PostgreSQL 表已存在；实际持久化在 Qdrant 集合中）
- `strategy_performance`：聚合性能指标（0 行，聚合待实现）
- `team_compositions`：agent 团队配置（0 行，写入待实现）
- `user_preferences`：用户交互偏好（0 行，推断待实现）

#### 遗留表
- `tasks`：已废弃，由 task_executions 取代（仅保留历史行）

### Workflow 集成点

1. **SimpleTaskWorkflow**：agent 执行之前
2. **Strategy Workflow**：Research、React、Scientific、Exploratory
3. **SequentialExecution**：每个 agent 步骤之前
4. **ParallelExecution**：为所有 agent 共享 context
5. **SupervisorWorkflow**：带策略洞察的增强型 memory

### Memory 生命周期

1. **创建**：agent 回复连同 embedding 一起存储
2. **检索**：基于 query 使用混合检索进行选择
3. **压缩**：定期对旧对话做摘要
4. **过期**：可选的基于 TTL 的清理

## 性能优化

#### 智能分块
- **基于字符的 token 化**：使用 4 字符 ≈ 1 token 的近似值以保证分块一致
- **存储减少 50%**：只存储分块文本，不存完整 embedding
- **去重**：用 qa_id 与 chunk_index 保证幂等
- **高效重建**：按序聚合分块
- **可配置参数**：通过 config 设置 MaxTokens（2000）与 OverlapTokens（200）

#### MMR（最大边际相关性）多样性
- **多样性感知重排**：在相关性与信息多样性之间取得平衡
- **Lambda 参数**：可配置相关性（λ→1）与多样性（λ→0）之间的权衡
- **默认 λ=0.7**：为既相关又多样的 context 选择而优化
- **候选池倍数**：先取 3 倍请求量，再按多样性重排

#### 批量处理
- **快 5 倍**：多个分块一次 API 调用完成
- **智能缓存**：LRU（2048 条）+ Redis
- **降低成本**：N 个分块 → 1 次 API 调用
- **候选池扩展**：取 3 倍候选再重排
- **更好覆盖**：避免结果冗余

#### 索引策略
- **过滤快 50-90%**：在过滤字段上建立 payload 索引
- **优化 HNSW**：m=16，ef_construct=100
- **全面索引**：session_id、tenant_id、user_id、agent_id

## 隐私与数据治理

#### PII 保护
- **数据最小化**：只存储必要字段
- **匿名化**：用 UUID 代替真实身份
- **脱敏**：自动检测并移除 PII
- **访问控制**：基于角色的权限

#### 数据保留
- **对话历史**：默认保留 30 天
- **Decomposition Patterns**：保留 90 天
- **用户偏好**：基于 session，24 小时过期
- **删除权**：支持用户删除 API

## 限制

- Memory 检索会增加延迟（通过缓存缓解）
- 向量相似度可能漏掉精确关键词匹配
- 压缩是有损的（保留要点）
- **[未实现]** 跨 session memory 需要显式同意

## 尚未实现的内容

1. **跨 session memory 检索** - session 严格隔离
2. **基于内容哈希的去重** - 目前使用 embedding 相似度
3. **基于性能的 agent 选择** - 指标已采集但未用于路由
4. **用户同意机制** - 没有跨 session 访问的 API
5. **用户偏好推断准确率指标** - 已定义但未计算
6. **PostgreSQL decomposition pattern 写入** - 实际存储在 Qdrant；Postgres 表写入保留给未来的聚合
7. **Strategy 性能聚合** - 指标已采集但聚合待实现

## 迁移状态（2025-01）

### 已完成
- **task_executions**：现为唯一事实来源（88+ 行，持续增长）
- **Session 关联**：通过 external_id 映射支持非 UUID 的 session ID
- **Supervisor memory**：查询已修复为与 task_executions 关联
- **数据流**：所有新 task 都写入 task_executions 并带有正确指标

### 待实现
- **Decomposition pattern 存储（PostgreSQL）**：Qdrant 写入已生效；Postgres 表写入为可选/未来项
- **Strategy 聚合**：性能指标已采集但未聚合
- **用户偏好推断**：逻辑已定义但未启用
- **团队构成追踪**：Schema 已存在，写入未实现

## 总结

Shannon 的 memory 系统为 context 感知的 AI 交互提供了坚实基础。核心基础设施已可运行：`task_executions` 作为主数据存储，supervisor memory 检索正常工作，P2P agent 协同已启用。虽然 decomposition pattern 持久化与 strategy 聚合等部分高级特性仍待实现，但系统已成功维持 session 连续性，并从 task 执行中学习以改进后续表现。
