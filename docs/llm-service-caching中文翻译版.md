# LLM 服务响应缓存

本文说明 Python LLM 服务的精确匹配响应 cache。它不会修改也不依赖任何 provider 内部的 KV‑cache。

## 架构：状态与计算的分离

Shannon 遵循清晰的分层架构：
- **Go Orchestrator** 持有全部持久化状态（Qdrant 向量库、session memory、语义搜索）
- **Python LLM Service** 是无状态计算（provider 抽象，仅做简单的精确匹配缓存）

这种分离确保了：
- 语义 memory 的单一事实来源
- 在 Temporal 中确定性地重放 workflow
- 干净的调试与维护

## 缓存的内容
- 非 stream 的 completion 调用的最终响应。
- 通过 manager 请求 stream 时，缓存响应会作为单个 chunk 发出。
- **仅精确匹配缓存** —— 语义相似度搜索由 Go Orchestrator 处理

## 不缓存的内容
- 部分的 stream chunk。
- provider 内部的 attention KV 状态（客户端无法控制）。
- 语义相似度匹配（属于 Go Orchestrator 的职责）

## 单一 cache strategy

为简洁与一致，Shannon 在 manager 层使用**一个精确匹配 cache**：

### 位置
- **Manager 层 cache**，位于 `llm_provider/manager.py`
- 应用于**所有调用路径**（包括 streaming）
- 使用 Redis key 前缀：`llm:cache:*`

### 后端
- **内存（默认）**：LRU 行为，按条目数设上限（最多 1000）
- **Redis（推荐）**：设置 `REDIS_HOST`/`REDIS_PORT`/`REDIS_PASSWORD` 以启用跨进程共享 cache
  - Key 格式：`llm:cache:{sha256_hash}`
  - TTL 取自 `prompt_cache.ttl_seconds`（默认 3600s）

### 配置
- YAML：`config/models.yaml`
  - `prompt_cache.enabled`（bool，默认 true）
  - `prompt_cache.ttl_seconds`（int，默认 3600）
  - `prompt_cache.max_cache_size_mb` 仅作说明用途
- 环境变量：设置 `REDIS_HOST`（以及可选的 `REDIS_PORT`/`REDIS_PASSWORD`）以使用 Redis
- 单请求：通过 `CompletionRequest.cache_ttl` 覆盖 TTL

## Cache Key 的推导
默认情况下，key 是对一份稳定 JSON 求 SHA‑256 得到的 hash，该 JSON 包含：
- `messages`（仅内容）
- `model_tier`
- `model`（如显式提供则覆盖）
- `temperature`
- `max_tokens`
- `functions`
- `seed`

注意：
- 上文未列出的参数（例如 `top_p`、`presence_penalty`、`response_format`）不属于默认 key。如果你的用法依赖它们，请显式设置 `CompletionRequest.cache_key`。

## 行为概览
- 非 stream 调用：manager 先查 cache；命中则返回缓存的 `CompletionResponse`。
- 通过 `LLMManager.stream_complete(...)` 的 stream 调用：若存在缓存结果，会作为单个 text chunk 发出；否则由 provider 逐块产出 text chunk。
- 过期：条目在 `ttl_seconds`（YAML）或请求级覆盖值之后过期。
- **语义相似度匹配**由 Go Orchestrator 在调用 LLM 服务之前处理。

## 指标与可观测性
- 暴露 Prometheus 指标：requests、tokens、cost、latency
- cache 命中由 `CompletionResponse` 中的 `cached` flag 表示
- manager 会记录 cache 命中及当前命中率（仅内存后端；Redis 显示 0.0）

## 安全与一致性
- 缓存响应是既有输出的不可变快照；请确保不要缓存包含敏感/临时数据的 prompt，除非你有意为之。
- 若需按 tenant 隔离，请设置 `CompletionRequest.cache_key`，把 tenant/session 标识纳入 cache key。
