# 速率感知的 budget 与限流控制

## 概述

Shannon 的速率感知 budget 系统为不同 LLM provider 与 tier 提供智能的速率限制管理，在遵守 provider 配额的前提下确保最佳吞吐量。

## 架构

### 限流控制系统

```
┌─────────────────────────────────────────────────────────────┐
│                    Workflow Execution                        │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              Middleware Budget Controller                    │
│  - Detects provider and tier from context                   │
│  - Calculates required sleep based on RPM/TPM               │
│  - Applies deterministic delays via workflow.Sleep()        │
└────────────────┬────────────────────────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────┐
│              Rate Control Helper                             │
│  - Loads provider limits from config                        │
│  - Tracks token and request counts                          │
│  - Computes optimal sleep durations                         │
└─────────────────────────────────────────────────────────────┘
```

## 配置

### Provider 速率限制

速率限制在 `config/models.yaml` 的 `rate_limits` 段中配置。Go orchestrator 直接读取该文件（见 `go/orchestrator/internal/ratecontrol/ratecontrol.go`）。

规范的 schema：

```yaml
rate_limits:
  default_rpm: 60      # global default requests per minute
  default_tpm: 100000  # global default tokens per minute

  # Optional: per-tier overrides
  tier_overrides:
    small:  { rpm: 120, tpm: 200000 }
    medium: { rpm: 60,  tpm: 100000 }
    large:  { rpm: 30,  tpm: 50000 }

  # Optional: per-provider overrides
  provider_overrides:
    openai:    { rpm: 30,  tpm: 60000 }
    anthropic: { rpm: 20,  tpm: 40000 }
    google:    { rpm: 40,  tpm: 80000 }
```

说明：
- tier 与 provider 的覆盖值通过取最严格的 RPM/TPM 合并。
- 若某覆盖项缺失，则使用对应的默认值。

### 默认限制

若未在 `config/models.yaml` 中指定限制，系统会采用保守的内置默认值（建议覆盖）：

| Provider | 内置 RPM | 内置 TPM |
|----------|--------------|--------------|
| OpenAI   | 30           | 60,000       |
| Anthropic| 20           | 40,000       |
| Google   | 40           | 80,000       |
| 其他     | 45           | 90,000       |

## 实现

### 限流 Helper

限流工具位于 `go/orchestrator/internal/ratecontrol/ratecontrol.go`，对外暴露单一入口：

```go
// DelayForRequest combines tier + provider limits (RPM/TPM)
// and returns a deterministic sleep duration for the request.
delay := ratecontrol.DelayForRequest(provider, tier, estimatedTokens)
```

### Middleware 集成

budget middleware（`go/orchestrator/internal/workflows/middleware_budget.go`）计算延迟并以确定性方式 sleep：

```go
// Inside BudgetPreflight or a similar pre-check
version := workflow.GetVersion(ctx, "provider_rate_control_v1", workflow.DefaultVersion, 1)
if version >= 1 {
    provider := resolveProviderFromContext(input.Context)
    tier := deriveModelTier(input.Context)
    delay := ratecontrol.DelayForRequest(provider, tier, estimatedTokens)
    if delay > 0 {
        _ = workflow.Sleep(ctx, delay)
    }
}
```

## 用法

### 自动限流

启用后，限流会自动应用于所有 workflow：

```yaml
# In task context
context:
  model_tier: "large"        # Determines rate limits
  provider: "openai"         # Optional, inferred from model
  respect_rate_limits: true  # Enable rate control

#### Set tier via HTTP API

```bash
curl -sS -X POST http://localhost:8080/api/v1/tasks \
  -H 'Content-Type: application/json' \
  -d '{"query": "Process batch data", "model_tier": "medium"}'
```
```

### 基于 Template 的控制

Template 可以指定限流参数：

```yaml
name: high_volume_analysis
defaults:
  model_tier: small  # Use small tier for higher RPM
  budget_agent_max: 5000

nodes:
  - id: batch_process
    type: dag
    metadata:
      rate_control:
        burst_allowed: false  # Strict rate limiting
        buffer_factor: 0.8    # Use 80% of limit for safety
```

### 手动覆盖

针对特定 task，可覆盖速率限制：

```go
input := TaskInput{
    Query: "Process batch data",
    Context: map[string]interface{}{
        "rate_override": map[string]interface{}{
            "rpm": 100,  // Custom RPM
            "tpm": 50000, // Custom TPM
        },
    },
}
```

## 监控

### 指标

系统对外暴露限流相关的 Prometheus 指标：

```
# Rate limit delays applied
shannon_rate_limit_delay_seconds{provider="openai",tier="large"} 0.5

# Rate limit violations
shannon_rate_limit_exceeded_total{provider="anthropic",tier="medium"} 3

# Current rate usage
shannon_rate_usage_ratio{provider="openai",type="rpm"} 0.75
shannon_rate_usage_ratio{provider="openai",type="tpm"} 0.82
```

### 日志

限流 event 会被记录以便调试：

```json
{
  "level": "info",
  "msg": "Rate limit sleep applied",
  "provider": "openai",
  "tier": "large",
  "sleep_ms": 500,
  "reason": "TPM limit",
  "token_count": 3500,
  "request_count": 2
}
```

## 最佳实践

### 1. tier 选择

根据工作负载选择合适的 tier：

- **Small tier**：高并发、简单的 task
- **Medium tier**：均衡的性能
- **Large tier**：复杂推理、较低并发

### 2. 突发管理

为不同场景配置突发处理：

```yaml
# Steady processing
metadata:
  rate_control:
    burst_allowed: false
    smoothing: true

# Batch jobs
metadata:
  rate_control:
    burst_allowed: true
    burst_window: "5m"
```

### 3. Provider 分配

在 provider 之间分散负载：

```yaml
# Primary provider
- id: primary_task
  metadata:
    provider: "openai"

# Fallback provider
- id: fallback_task
  metadata:
    provider: "anthropic"
  on_fail:
    provider: "google"
```

### 4. 缓冲管理

为速率限制留出余量：

```yaml
defaults:
  metadata:
    rate_control:
      buffer_factor: 0.9  # Use 90% of limit
```

## 与 budget 系统的集成

### Token budget

限流与 token budget 协同工作：

```go
// Budget enforced first
if task.BudgetMax > 0 && tokensUsed >= task.BudgetMax {
    return ErrBudgetExceeded
}

// Then rate control
if sleepDuration := rc.CalculateSleep(); sleepDuration > 0 {
    workflow.Sleep(ctx, sleepDuration)
}
```

### 成本优化

系统同时针对成本和速率限制进行优化：

1. **Pattern 降级**：接近限制时减少 token
2. **tier 选择**：为成本/性能选择最优 tier
3. **Provider 路由**：路由到可用的 provider

## 高级特性

### 自适应限流

系统会随时间学习最优速率：

```go
type AdaptiveController struct {
    baseRPM     int
    actualRPM   float64
    errorRate   float64
    adjustments int
}

func (ac *AdaptiveController) AdjustLimits() {
    if ac.errorRate > 0.05 { // >5% errors
        ac.baseRPM = int(float64(ac.baseRPM) * 0.9)
    } else if ac.errorRate < 0.01 { // <1% errors
        ac.baseRPM = int(float64(ac.baseRPM) * 1.05)
    }
}
```

### 优先级队列

高优先级 task 可绕过限流：

```yaml
context:
  priority: "critical"
  rate_control:
    bypass: true  # Skip rate limiting
```

### 速率池

在 session 之间共享速率限制：

```yaml
context:
  rate_pool: "organization_xyz"  # Share limits
  rate_pool_weight: 0.2          # Use 20% of pool
```

## 故障排查

### 常见问题

#### 高延迟
- 检查速率限制配置
- 核实 tier 选择
- 监控实际限制与配置限制

#### 速率限制错误
```bash
# Check current usage
curl http://localhost:2112/metrics | grep rate_usage

# View rate control logs
docker logs orchestrator | grep "rate_limit"
```

#### 配置问题
```bash
# Validate models.yaml
go run cmd/validate/main.go -config config/models.yaml

# Test rate calculation
go test ./internal/ratecontrol/... -v
```

## 迁移指南

### 启用限流

1. 更新 docker-compose.yml：
```yaml
environment:
  - ENABLE_RATE_CONTROL=true
```

2. 在 models.yaml 中配置 provider 限制

3. 重启 orchestrator：
```bash
docker-compose restart orchestrator
```

### 向后兼容

限流受版本门控，不会影响已有的 workflow：

```go
// Old workflows continue without rate control
version := workflow.GetVersion(ctx, "provider_rate_control_v1",
    workflow.DefaultVersion, 1)
if version < 1 {
    return nil // Skip rate control
}
```
