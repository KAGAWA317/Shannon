# Pattern 使用指南

## 快速开始

本指南提供使用 Shannon 的 pattern 库构建复杂多 agent workflow 的实用示例。

## Pattern 目录

### 1. Chain-of-Thought（CoT）Pattern

**用途**：带有显式思考推进过程的分步推理

**使用场景**：
- 需要展示推导过程的数学问题
- 逻辑证明与推导
- 需要透明度的复杂分析
- 教学式讲解

**配置示例**：
```go
config := patterns.ChainOfThoughtConfig{
    MaxSteps:              5,        // Maximum reasoning steps
    RequireExplanation:    true,     // Force step-by-step explanation
    ShowIntermediateSteps: true,     // Include steps in output
    ModelTier:             "large",  // Use stronger model for complex reasoning
}
```

**输入/输出流程**：
```
Input: "Calculate the compound interest on $1000 at 5% for 3 years"
   ↓
Step 1: Identify the formula: A = P(1 + r)^t
Step 2: Substitute values: P=1000, r=0.05, t=3
Step 3: Calculate: 1000(1.05)^3
Step 4: Compute: 1000 × 1.157625
Step 5: Result: $1,157.63
   ↓
Output: Final amount is $1,157.63 with $157.63 in interest
```

### 2. Debate Pattern

**用途**：通过多 agent 辩论探索不同视角

**使用场景**：
- 有争议或主观的话题
- 涉及权衡的决策
- 探索利弊
- 建立共识

**配置示例**：
```go
config := patterns.DebateConfig{
    NumDebaters:      3,              // Number of debating agents
    MaxRounds:        3,              // Debate rounds
    Perspectives:     []string{       // Assigned perspectives
        "optimistic",
        "pessimistic",
        "pragmatic",
    },
    RequireConsensus: false,          // Don't force agreement
    ModeratorEnabled: true,           // Use moderator for synthesis
    VotingEnabled:    true,           // Enable position voting
}
```

**辩论流程**：
```
Query: "Should we migrate to microservices?"
   ↓
Round 1: Initial Positions
- Optimist: "Scalability and team independence"
- Pessimist: "Complexity and operational overhead"
- Pragmatist: "Depends on team size and growth plans"
   ↓
Round 2: Counter-arguments
- Each agent responds to others' points
   ↓
Round 3: Finding Common Ground
- Consensus on gradual migration approach
   ↓
Output: Synthesized recommendation with confidence scores
```

### 3. Tree-of-Thoughts（ToT）Pattern

**用途**：通过分支系统地探索解空间

**使用场景**：
- 存在多条解法路径的问题
- 创造性的问题求解
- 策略规划
- 可能需要回溯的场景

**配置示例**：
```go
config := patterns.TreeOfThoughtsConfig{
    MaxDepth:          4,             // Tree depth limit
    BranchingFactor:   3,             // Branches per node
    EvaluationMethod:  "scoring",    // How to evaluate paths
    PruningThreshold:  0.3,           // Min score to continue
    ExplorationBudget: 15,            // Max thoughts to explore
    BacktrackEnabled:  true,          // Allow returning to promising branches
}
```

**探索树**：
```
Query: "Design a fault-tolerant system"
        ↓
    [Root: System Design]
    /        |         \
[Active-     [Active-    [Passive
 Active]      Passive]    Backup]
   /  \         /  \        /  \
[Load  [Hot    [Primary [Cold  [Warm
 Balance] Standby] Secondary] Standby] Standby]
   ↓      ↓        ↓         ↓       ↓
(score) (score)  (score)  (score) (score)
        ↓
Best Path: Active-Passive → Primary-Secondary
```

### 4. React Pattern

**用途**：用于交互式问题求解的 Reason-Act-Observe 循环

**使用场景**：
- 需要使用 tool 的 task
- 调试与故障排查
- 迭代式改进
- 与环境交互

**配置示例**：
```go
config := patterns.ReactConfig{
    MaxIterations:     5,             // Maximum React loops
    ObservationWindow: 3,             // Recent observations to track
    MaxObservations:   20,            // Total observation limit
    MaxThoughts:       10,            // Reasoning step limit
    MaxActions:        10,            // Action execution limit
}
```

**React 循环**：
```
Query: "Debug why the API is returning 500 errors"
   ↓
Thought: Need to check server logs
Action: execute_command("tail -n 50 /var/log/app.log")
Observation: "NullPointerException at line 45"
   ↓
Thought: Found NPE, need to check the code
Action: read_file("api/handler.py", line=45)
Observation: "user = request.user.profile"
   ↓
Thought: User might not have profile
Action: add_null_check("if hasattr(request.user, 'profile')")
Observation: Fixed and deployed
   ↓
Output: Resolved 500 error caused by missing user profiles
```

### 5. Reflection Pattern

**用途**：通过自我评估迭代提升质量

**使用场景**：
- 最终质量打磨
- 需要验证的关键输出
- 初始结果低于阈值时
- 持续改进循环

**配置示例**：
```go
config := patterns.ReflectionConfig{
    Enabled:             true,
    MaxRetries:          3,           // Improvement iterations
    ConfidenceThreshold: 0.85,        // Target quality score
    Criteria: []string{               // Evaluation dimensions
        "accuracy",
        "completeness",
        "clarity",
    },
    TimeoutMs:           30000,       // Per-reflection timeout
}
```

**反思过程**：
```
Initial Result → Evaluate (Score: 0.6)
   ↓
Issues: Lacks specific examples, unclear conclusion
   ↓
Improved Result → Evaluate (Score: 0.8)
   ↓
Issues: Missing edge cases
   ↓
Final Result → Evaluate (Score: 0.9) ✓
```

## Pattern 组合示例

### 示例 1：科学调查

**Workflow**：ScientificWorkflow
**Composition**：CoT → Debate → ToT → Reflection

```go
// Phase 1: Generate hypotheses with Chain-of-Thought
hypotheses := ChainOfThought(
    "Generate 3 testable hypotheses for performance issue",
    ChainOfThoughtConfig{MaxSteps: 3, RequireExplanation: true},
)

// Phase 2: Test hypotheses through Debate
testResults := Debate(
    "Which hypothesis best explains the data?",
    DebateConfig{NumDebaters: 3, Perspectives: hypotheses},
)

// Phase 3: Explore implications with Tree-of-Thoughts
implications := TreeOfThoughts(
    "What are the implications of the winning hypothesis?",
    TreeOfThoughtsConfig{MaxDepth: 3, BranchingFactor: 2},
)

// Phase 4: Polish with Reflection
finalReport := Reflection(
    combineResults(hypotheses, testResults, implications),
    ReflectionConfig{ConfidenceThreshold: 0.9},
)
```

### 示例 2：复杂问题求解

**Workflow**：ExploratoryWorkflow
**Composition**：ToT（主）→ Debate（置信度不足时）→ Reflection（最终）

```go
// Primary exploration with Tree-of-Thoughts
exploration := TreeOfThoughts(
    "How to optimize database performance?",
    TreeOfThoughtsConfig{
        MaxDepth: 4,
        BranchingFactor: 3,
        BacktrackEnabled: true,
    },
)

// If confidence < threshold, apply Debate
if exploration.Confidence < 0.7 {
    debate := Debate(
        "Evaluate the top 3 optimization strategies",
        DebateConfig{
            NumDebaters: 3,
            Perspectives: exploration.TopPaths,
        },
    )
    exploration = mergeResults(exploration, debate)
}

// Final quality check
final := Reflection(
    exploration,
    ReflectionConfig{MaxRetries: 2},
)
```

### 示例 3：交互式调试

**Workflow**：ReactWorkflow 配合 Reflection
**Composition**：React（主循环）→ Reflection（结果质量）

```go
// Main React loop for debugging
debugging := React(
    "Find and fix the memory leak",
    ReactConfig{
        MaxIterations: 10,
        ObservationWindow: 5,
    },
)

// Ensure solution quality
solution := Reflection(
    debugging.Solution,
    ReflectionConfig{
        Criteria: []string{"correctness", "completeness"},
    },
)
```

## 执行 Pattern

### 并行执行

**使用场景**：各 task 相互独立
**优势**：吞吐量最大
**局限**：无共享 context

```go
// Execute multiple independent analyses
results := ParallelExecution(
    subtasks: []Task{
        "Analyze performance metrics",
        "Review security logs",
        "Check error rates",
    },
    config: ParallelConfig{
        MaxConcurrency: 3,
        Timeout: 60000,
    },
)
```

### 顺序执行

**使用场景**：task 之间层层叠加
**优势**：context 持续累积
**局限**：整体更慢

```go
// Step-by-step data pipeline
results := SequentialExecution(
    subtasks: []Task{
        "Extract data from source",
        "Transform to standard format",
        "Validate data quality",
        "Load into destination",
    },
    config: SequentialConfig{
        PassContext: true,
    },
)
```

### 混合执行

**使用场景**：task 之间存在依赖关系
**优势**：在约束下取得最优并行度
**局限**：依赖管理复杂

```go
// Execute with dependency graph
results := HybridExecution(
    tasks: map[string]Task{
        "A": {Query: "Task A"},
        "B": {Query: "Task B", Dependencies: []string{"A"}},
        "C": {Query: "Task C", Dependencies: []string{"A"}},
        "D": {Query: "Task D", Dependencies: []string{"B", "C"}},
    },
)
// Execution order: A → (B || C) → D
```

## Pattern 选择决策树

```
Start → Is the problem well-defined?
         ├─ No → Use ExploratoryWorkflow (ToT + Debate)
         └─ Yes → Does it need multiple perspectives?
                   ├─ Yes → Does it need systematic testing?
                   │         ├─ Yes → Use ScientificWorkflow (CoT + Debate + ToT)
                   │         └─ No → Use Debate pattern
                   └─ No → Does it need tool interaction?
                           ├─ Yes → Use ReactWorkflow
                           └─ No → Is it complex reasoning?
                                   ├─ Yes → Use Chain-of-Thought
                                   └─ No → Use simple execution
```

## 性能建议

### Token 优化

1. **先从简单的 pattern 入手**，再考虑复杂组合
2. **使用置信度阈值**跳过不必要的 pattern
3. **为每个 pattern 配置合适的 model tier**
4. **为 ToT pattern 设置 exploration budget**
5. **根据收敛情况限制 debate 轮次**

### 质量与速度的权衡

| 优先级 | Pattern 选择 | 配置 |
|----------|---------------|---------------|
| **速度** | 跳过 Reflection，限制迭代次数 | `MaxRetries: 1, MaxIterations: 3` |
| **质量** | 增加 Reflection，加大探索 | `ConfidenceThreshold: 0.9, MaxDepth: 5` |
| **平衡** | 按置信度条件化选择 pattern | `if confidence < 0.7: add_pattern()` |

### 排查 Pattern 问题

**Pattern 不收敛**：
- 增加最大迭代次数/轮次
- 调整置信度阈值
- 检查 query 是否形式良好

**Token 消耗过大**：
- 降低分支因子（ToT）
- 限制 debate 轮次
- 使用更小的 model tier
- 设置更紧的 exploration budget

**结果质量不佳**：
- 增加 Reflection pattern
- 提升 model tier
- 增加更多评估标准
- 组合使用多个 pattern

## 进阶技巧

### 动态 Pattern 选择

```go
func SelectPatternDynamically(query string, context map[string]interface{}) Pattern {
    complexity := analyzeComplexity(query)
    uncertainty := measureUncertainty(context)

    if complexity > 0.8 && uncertainty > 0.7 {
        return ComposePatterns(TreeOfThoughts, Debate, Reflection)
    } else if complexity > 0.5 {
        return ComposePatterns(ChainOfThought, Reflection)
    } else {
        return React
    }
}
```

### Pattern 结果缓存

```go
// Cache pattern results for similar queries
cacheKey := hash(pattern_type, query, config)
if cached := cache.Get(cacheKey); cached != nil {
    return cached
}
result := pattern.Execute(query, config)
cache.Set(cacheKey, result, ttl=3600)
```

### 跨 Pattern 的 Context 共享

```go
// Share insights across patterns
sharedContext := map[string]interface{}{
    "previous_patterns": []string{"ChainOfThought"},
    "insights": cotResult.ReasoningSteps,
    "confidence": cotResult.Confidence,
}

// Next pattern can leverage previous insights
debateResult := Debate(query, config, sharedContext)
```

## 结语

Shannon 的 pattern 库为复杂的 AI 推理提供了强大的构建模块。理解每个 pattern 的优势与最佳适用场景后，开发者可以为任何问题领域组合出有效的 workflow。先从单个 pattern 起步，衡量其效果，再按需逐步组合出更复杂的 workflow。
