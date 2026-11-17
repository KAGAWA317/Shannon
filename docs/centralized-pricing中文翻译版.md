# 集中式定价配置

## 概述

Shannon 平台现在提供集中式定价配置系统，统一管理所有服务（Go orchestrator、Rust agent-core、Python llm-service）中的模型成本。全部定价数据维护在唯一的事实来源中：`config/models.yaml`。

## 配置结构

定价配置定义在 `config/models.yaml` 的 `pricing` 段下：

```yaml
pricing:
  defaults:
    combined_per_1k: 0.005  # Default cost per 1K tokens when model is unknown
  models:
    <provider>:
      <model_id>:
        input_per_1k: 0.0005   # Cost per 1K input tokens
        output_per_1k: 0.0015  # Cost per 1K output tokens
        combined_per_1k: 0.005 # Optional: Used when only total tokens are known
```

## 实现细节

### Go Orchestrator

**位置**：`go/orchestrator/internal/pricing/pricing.go`

- 从 `config/models.yaml`（或 `MODELS_CONFIG_PATH` 环境变量）加载定价配置
- 提供以下函数：
  - `DefaultPerToken()`：返回每个 token 的默认成本
  - `PricePerTokenForModel(model string)`：返回指定模型的每 token 成本
  - `CostForTokens(model string, tokens int)`：计算总成本
  - `CostForSplit(model string, inputTokens, outputTokens int)`：精确的输入/输出拆分定价
  - `ModifiedTime()`：返回配置文件的修改时间
- 使用位置：
  - `internal/server/service.go`：计算 workflow 执行成本
  - `internal/activities/session.go`：更新 session 成本追踪

热重载与校验：

- 配置管理器会监听 `models.yaml`，文件变更时重新加载定价
- 基础校验确保 `pricing` 段下不存在负值

Fallback 指标：

- `shannon_pricing_fallback_total{reason="missing_model|unknown_model"}`
  在每次使用默认值（模型缺失或未知）时递增

### Rust Agent-Core

**位置**：`rust/agent-core/src/llm_client.rs`

- 函数 `pricing_cost_per_1k(model)` 从同一配置文件读取定价
- 函数 `calculate_cost(model, tokens)` 使用集中式定价并带 fallback
- 依次尝试从以下位置读取：
  1. `MODELS_CONFIG_PATH` 环境变量
  2. `/app/config/models.yaml`（Docker 容器路径）
  3. `./config/models.yaml`（本地开发）
- 配置不可用时回退到硬编码的启发式规则

### Python LLM Service

**位置**：`python/llm-service/llm_provider/manager.py`

- `LLMManager` 在初始化 provider 之后加载定价覆盖项
- 函数 `_load_and_apply_pricing_overrides()`：
  - 从 `config/models.yaml`（或 `MODELS_CONFIG_PATH`）读取
  - 按 model_id 或 key 匹配 provider 模型并应用定价
- 用集中式数值覆盖 provider 自带的定价
- 保持与现有 provider 配置的兼容性

## 成本计算逻辑

1. **输入/输出 token 分别已知时**：
   - 使用 `input_per_1k` 与 `output_per_1k` 做精确计算

2. **仅已知总 token 数时**：
   - 若配置了 `combined_per_1k` 则使用它
   - 否则取平均值：`(input_per_1k + output_per_1k) / 2`

3. **模型未知时**：
   - 使用 `defaults.combined_per_1k`
   - 配置不可用时回退到通用默认值（例如每 1K token `0.002`）
   - 递增 `shannon_pricing_fallback_total`

## 环境变量

- `MODELS_CONFIG_PATH`：覆盖默认配置文件位置
  - 所有服务（Go、Rust、Python）都会使用
  - 优先级高于默认路径

## 默认路径

服务按以下顺序查找配置：

1. `$MODELS_CONFIG_PATH`（若已设置）
2. `/app/config/models.yaml`（Docker 容器）
3. `./config/models.yaml`（本地开发）

## 测试

### Go 测试

```bash
cd go/orchestrator
go test -v ./internal/pricing/...
```

### 手动验证

已验证该实现可以：

- 从 `config/models.yaml` 正确加载定价配置
- 对已知模型准确计算成本
- 对未知模型回退到默认值
- 在 Python LLM service 中应用定价覆盖项

## 迁移说明

### 变更内容

- **Go**：服务改用 `pricing.CostForTokens()` / `CostForSplit()`，不再使用内联启发式规则
- **Rust**：`calculate_cost()` 在 fallback 之前先检查集中式配置
- **Python**：Manager 在 provider 初始化之后应用定价覆盖项
- **Config**：在 `config/models.yaml` 中新增 `pricing` 段

### 向后兼容性

- 所有服务都保留了配置缺失时的 fallback 逻辑
- 现有 provider 自带的定价除非被覆盖，否则保持不变
- 无需修改 activity/proto 签名

## Workflow 覆盖情况

关于哪些 workflow 具备真实的按模型成本、哪些只是近似值，详见 [workflow-pricing-coverage.md](workflow-pricing-coverage.md)。

### 摘要

- **生产可用**（真实成本）：Simple、DAG v2、Supervisor、React、Streaming（单路与并行）
- **使用默认值**：Exploratory、Scientific 两类 pattern

## 增强项

1. **拆分 token 追踪**：workflow 现在分别追踪输入与输出 token
2. **价格校验**：已实现非负校验
3. **热重载支持**：`models.yaml` 变更时重新加载定价
4. **模型透传**：生产 workflow 会传递模型名以获得准确成本
5. **Fallback 指标**：`shannon_pricing_fallback_total` 追踪覆盖率

### 潜在改进

1. **成本告警**：成本超出阈值时发送通知
2. **用量分析**：追踪按模型的用量模式与成本
3. **pattern 扩展**：若用量确实需要，为 Exploratory/Scientific 增加按 agent 的追踪

## 配置示例

常用模型的当前定价：

```yaml
pricing:
  defaults:
    combined_per_1k: 0.005
  models:
    openai:
      gpt-5-2025-08-07:
        input_per_1k: 0.0060
        output_per_1k: 0.0180
      gpt-5-nano-2025-08-07:
        input_per_1k: 0.00010
        output_per_1k: 0.00040
    anthropic:
      claude-sonnet-4-5-20250929:
        input_per_1k: 0.0030
        output_per_1k: 0.0150
      claude-haiku-4-5-20251001:
        input_per_1k: 0.00025
        output_per_1k: 0.00125
    deepseek:
      deepseek-chat:
        input_per_1k: 0.0001
        output_per_1k: 0.0002
```

## Workflow 覆盖情况（真实成本 vs. 近似值）

Streaming 现在报告真实的按模型成本（在可用时包含 token 拆分）。

- Simple：真实成本（按模型，总 token）
- DAG v2：真实成本（按 agent，输入/输出拆分）
- Supervisor：真实成本（按 agent，输入/输出拆分）
- React：真实成本（按 agent，输入/输出拆分）
- Streaming（单路/并行）：真实成本（按 agent，总 token；提供拆分时使用拆分）
- Exploratory（ToT/Debate/Reflect）：近似值（聚合总量）
- Scientific（CoT/Debate/ToT/Reflect）：近似值（聚合总量）

注意：当这些 pattern 能够从内部 agent 调用中暴露模型 ID 与 token 拆分后，我们会将 Exploratory/Scientific 升级为真实的按 agent 成本。
