# Learning Router 增强

## 概述

learning router 使用 **epsilon-greedy** 算法，结合上下文 pattern 匹配，基于历史表现与探索需求智能选择 strategy。

## 算法设计

### Epsilon-Greedy 选择

router 使用 epsilon-greedy 探索，探索率为 10%：
- 90% 的情况：基于历史成功率，利用（exploit）表现最好的 strategy
- 10% 的情况：通过尝试不同 strategy 进行探索（explore）

### Strategy 分数计算

router 根据历史表现为每个 strategy 计算分数：

```
Score = Success Rate + Contextual Boost - Performance Penalties
```

### 组件

#### 1. 成功率（0.0 - 1.0）
- 基于历史成功/失败比值的基础分量
- 按新近度加权（近期结果的权重更高）

```go
successRate := float64(successes) / float64(attempts)
```

#### 2. 探索（Epsilon-Greedy）
- 10% 的概率随机选择 strategy 进行探索
- 确保持续学习新的 pattern

```go
if rand.Float64() < 0.1 {  // 10% exploration
    return selectRandomStrategy()
}
```

#### 3. 惩罚项

**延迟惩罚**（0.0 - 0.2）：
```go
if avgLatency > targetLatency {
    penalty = min(0.2, (avgLatency - targetLatency) / targetLatency * 0.1)
}
```

**Token 效率惩罚**（0.0 - 0.15）：
```go
if avgTokens > budgetTarget {
    penalty = min(0.15, (avgTokens - budgetTarget) / budgetTarget * 0.1)
}
```

#### 4. 关键词加成（0.0 - 0.1）
- 为匹配查询 pattern 的 strategy 加成
- 基于 TF-IDF 相似度

```go
if keywordMatch > threshold {
    boost = min(0.1, similarity * 0.2)
}
```

### 最终置信度

置信度被裁剪到 [0, 1]：

```go
confidence := max(0.0, min(1.0, ucbScore))
```

## 实现

### RecommendWorkflowStrategy

```go
func RecommendWorkflowStrategy(ctx context.Context,
    input RecommendStrategyInput) (RecommendStrategyOutput, error) {

    // Fetch historical patterns from supervisor memory
    patterns := SearchDecompositionPatterns(input.SessionID, input.Query)

    // Epsilon-greedy selection
    if rand.Float64() < 0.1 { // 10% exploration
        return selectRandomStrategy()
    }

    // Calculate scores for each strategy
    scores := make(map[StrategyType]float64)
    for strategy, stats := range patterns {
        score := calculateStrategyScore(strategy, stats, input.Query)
        scores[strategy] = score
    }

    // Select highest scoring strategy
    best := selectBestStrategy(scores)

    return RecommendStrategyOutput{
        Strategy:   best.Strategy,
        Confidence: best.Confidence,
        Source:     "epsilon_greedy",
    }, nil
}
```

### Strategy 分数计算

```go
func calculateStrategyScore(strategy StrategyType,
    stats StrategyStats, query string) float64 {

    // Base success rate from historical data
    successRate := stats.SuccessRate()

    // Latency penalty
    latencyPenalty := 0.0
    if stats.AvgLatencyMs > targetLatencyMs {
        ratio := (stats.AvgLatencyMs - targetLatencyMs) / targetLatencyMs
        latencyPenalty = math.Min(0.2, ratio * 0.1)
    }

    // Token efficiency penalty
    tokenPenalty := 0.0
    if stats.AvgTokens > targetTokens {
        ratio := (stats.AvgTokens - targetTokens) / targetTokens
        tokenPenalty = math.Min(0.15, ratio * 0.1)
    }

    // Contextual boost from query similarity
    contextBoost := calculateContextualBoost(strategy, query)

    // Composite score
    score := successRate - latencyPenalty - tokenPenalty + contextBoost

    // Clamp to [0, 1]
    return math.Max(0.0, math.Min(1.0, score))
}
```

## Strategy pattern

### Pattern 识别

系统为每个 strategy 学习查询 pattern：

```go
type PatternSignature struct {
    Keywords     []string
    Complexity   float64
    ToolsNeeded  []string
    OutputFormat string
}

func matchPattern(query string,
    signature PatternSignature) float64 {
    // TF-IDF similarity
    keywordScore := tfidfSimilarity(query, signature.Keywords)

    // Complexity matching
    queryComplexity := estimateComplexity(query)
    complexityMatch := 1.0 - math.Abs(queryComplexity -
                                      signature.Complexity)

    // Tool requirement matching
    requiredTools := detectRequiredTools(query)
    toolMatch := jaccardSimilarity(requiredTools,
                                   signature.ToolsNeeded)

    // Weighted combination
    return keywordScore*0.5 + complexityMatch*0.3 + toolMatch*0.2
}
```

### Strategy 画像

每个 strategy 都有一个学习得到的画像：

| Strategy | 关键词 | 复杂度 | 典型 tool | 成功领域 |
|----------|----------|------------|---------------|-----------------|
| Tree of Thoughts | analyze, explore, compare, evaluate | 高 | web_search, calculator | 研究与分析 |
| Chain of Thought | explain, describe, summarize | 中 | web_search | 解释与摘要 |
| ReAct | find, get, retrieve, calculate | 低 | 全部 tool | 数据检索、计算 |
| Debate | pros/cons, compare, argue | 高 | web_search | 决策 |
| Reflection | improve, refine, review | 中 | 无 | 质量提升 |

## 置信度评分

### 置信度等级

| 置信度 | 含义 | 行动 |
|------------|---------|--------|
| 0.9 - 1.0 | 非常高 | 使用推荐的 strategy |
| 0.7 - 0.89 | 高 | 在允许 pattern 降级的前提下使用 |
| 0.5 - 0.69 | 中 | 考虑其他选择 |
| 0.3 - 0.49 | 低 | 优先探索 |
| 0.0 - 0.29 | 非常低 | 使用默认 strategy |

### 置信度因子

置信度受以下因素影响：

1. **样本量**：数据越多 → 置信度越高
2. **新近度**：近期成功 → 置信度越高
3. **一致性**：表现稳定 → 置信度越高
4. **Context 匹配**：查询越相似 → 置信度越高

```go
func adjustConfidence(baseConfidence float64,
    stats StrategyStats) float64 {

    // Sample size factor
    sampleFactor := math.Min(1.0, float64(stats.Attempts) / 100.0)

    // Recency factor (exponential decay)
    recencyFactor := calculateRecencyWeight(stats.LastSuccess)

    // Consistency factor (low variance = high consistency)
    consistencyFactor := 1.0 - stats.SuccessVariance()

    // Weighted adjustment
    adjusted := baseConfidence *
                (0.4 + sampleFactor*0.2 +
                 recencyFactor*0.2 +
                 consistencyFactor*0.2)

    return math.Max(0.0, math.Min(1.0, adjusted))
}
```

## 存储与检索

### 向量库集成

pattern 以 embedding 的形式存储在 Qdrant 中：

```go
type DecompositionPattern struct {
    ID           string
    Query        string
    QueryVector  []float32  // Embedding
    Strategy     string
    Success      bool
    TokensUsed   int
    LatencyMs    int64
    Timestamp    time.Time
    Metadata     map[string]interface{}
}
```

### 相似度搜索

查找相似的 pattern：

```go
func findSimilarPatterns(query string, limit int) []Pattern {
    // Generate query embedding
    embedding := generateEmbedding(query)

    // Search in Qdrant
    results := qdrant.Search(
        collection: "decomposition_patterns",
        vector: embedding,
        limit: limit,
        threshold: 0.75,  // Minimum similarity
    )

    return results
}
```

## 指标与监控

### 关键指标

```prometheus
# Strategy selection distribution
shannon_strategy_selection_total{strategy="tree_of_thoughts"} 142
shannon_strategy_selection_total{strategy="chain_of_thought"} 298
shannon_strategy_selection_total{strategy="react"} 521

# Decomposition patterns recorded
shannon_decomposition_patterns_recorded_total 961

# Learning source breakdown
shannon_strategy_selection_total{source="epsilon_greedy"} 865
shannon_strategy_selection_total{source="exploration"} 96

# Pattern cache performance
shannon_decomposition_pattern_cache_hits_total 432
shannon_decomposition_pattern_cache_misses_total 529
```

### 性能追踪

```sql
-- Strategy performance over time
SELECT
    strategy,
    DATE(timestamp) as date,
    AVG(CASE WHEN success THEN 1.0 ELSE 0.0 END) as success_rate,
    AVG(tokens_used) as avg_tokens,
    AVG(latency_ms) as avg_latency,
    COUNT(*) as attempts
FROM decomposition_patterns
WHERE timestamp > NOW() - INTERVAL '7 days'
GROUP BY strategy, date
ORDER BY date DESC, strategy;
```

## 配置

### 调优参数

```yaml
learning_router:
  # Exploration rate (epsilon-greedy)
  epsilon: 0.1

  # UCB parameters
  exploration_weight: 2.0  # C in UCB formula

  # Penalty thresholds
  target_latency_ms: 5000
  target_tokens: 3000

  # Confidence thresholds
  min_confidence: 0.3
  high_confidence: 0.7

  # History window
  pattern_retention_days: 30
  max_patterns_per_session: 1000

  # Keyword matching
  keyword_boost_weight: 0.1
  min_keyword_similarity: 0.6
```

### A/B 测试

通过受控实验对比 strategy：

```yaml
experiments:
  strategy_comparison:
    enabled: true
    control_strategy: "chain_of_thought"
    test_strategy: "ucb_router"
    traffic_split: 0.5
    metrics:
      - success_rate
      - avg_tokens
      - avg_latency
    duration_hours: 168  # 1 week
```

## 最佳实践

### 1. 冷启动处理

对于没有历史记录的新 session，使用启发式规则：

```go
if len(patterns) == 0 {
    // Exploration mode: randomly select to build initial data
    if rand.Float64() < 0.3 {  // 30% exploration during cold start
        return selectRandomStrategy()
    }
    // Otherwise use query complexity heuristic
    complexity := estimateComplexity(query)
    if complexity > 0.7 {
        return "tree_of_thoughts", 0.5
    } else if complexity > 0.4 {
        return "chain_of_thought", 0.5
    }
    return "react", 0.5
}
```

### 2. Strategy 回退

始终保留一条回退链：

```go
strategies := []StrategyType{
    recommendedStrategy,
    "chain_of_thought",  // Fallback 1
    "react",             // Fallback 2
}
```

### 3. 持续学习

每次执行后更新 pattern：

```go
defer recordPattern(PatternRecord{
    Query:      input.Query,
    Strategy:   selectedStrategy,
    Success:    result.Success,
    TokensUsed: result.TokensUsed,
    LatencyMs:  elapsed.Milliseconds(),
})
```

### 4. 隐私考量

- 存储前对 pattern 做匿名化
- 对聚合数据使用差分隐私
- 实施数据保留策略
- 允许用户选择退出

## 未来增强

1. **Thompson Sampling**：用于探索/利用的贝叶斯方法
2. **Contextual Bandits**：在选择时考虑更多上下文特征
3. **深度学习**：基于神经网络的 strategy 预测
4. **迁移学习**：在相似领域之间共享 pattern
5. **动态探索**：根据表现方差调整 epsilon 率
6. **分层 strategy**：多层级 strategy 选择

## 算法说明

当前实现使用 **epsilon-greedy**（而非 UCB），原因如下：
- 实现与理解都更简单
- 无需按 strategy 追踪访问次数
- 探索行为更可预测
- 在历史数据有限时表现良好

未来版本可能会探索 UCB 变体（UCB1、Thompson Sampling），以实现更精细的探索策略。
