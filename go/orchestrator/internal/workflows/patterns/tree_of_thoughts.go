package patterns

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/util"
	wopts "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
	"go.temporal.io/sdk/workflow"
)

// TreeOfThoughtsConfig configures the tree-of-thoughts pattern
// TreeOfThoughtsConfig 配置 tree-of-thoughts（思维树）模式
type TreeOfThoughtsConfig struct {
	MaxDepth          int     // Maximum tree depth 注：最大树深度
	BranchingFactor   int     // Number of branches per node (2-4) 注：每个节点的分支数（2-4）
	EvaluationMethod  string  // "scoring", "voting", "llm" 注：取值："scoring"、"voting"、"llm"
	PruningThreshold  float64 // Minimum score to continue branch (0-1) 注：继续展开分支的最低分数（0-1）
	ExplorationBudget int     // Max total thoughts to explore 注：最多探索的 thought 总数
	BacktrackEnabled  bool    // Allow backtracking to promising branches 注：是否允许回溯到更有希望的分支
	ModelTier         string  // Model tier for thought generation 注：thought 生成使用的 model tier
}

// ThoughtNode represents a node in the thought tree
// ThoughtNode 表示 thought 树中的一个节点
type ThoughtNode struct {
	ID          string
	Thought     string
	Score       float64
	Depth       int
	ParentID    string
	Children    []*ThoughtNode
	TokensUsed  int
	IsTerminal  bool
	Explanation string
}

// TreeOfThoughtsResult contains the exploration result
// TreeOfThoughtsResult 保存探索结果
type TreeOfThoughtsResult struct {
	BestPath        []*ThoughtNode
	BestSolution    string
	TotalThoughts   int
	TreeDepth       int
	TotalTokens     int
	ExplorationTree *ThoughtNode // Root of the tree 注：树的根节点
	Confidence      float64
}

// TreeOfThoughts implements systematic exploration of solution paths
// This pattern explores multiple reasoning branches and selects the most promising path
// TreeOfThoughts 对求解路径做系统化探索
// 该模式探索多条推理分支，并选出最有希望的一条路径
func TreeOfThoughts(
	ctx workflow.Context,
	query string,
	context map[string]any,
	sessionID string,
	history []string,
	config TreeOfThoughtsConfig,
	opts Options,
) (*TreeOfThoughtsResult, error) {

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Tree-of-Thoughts exploration",
		"query", query,
		"max_depth", config.MaxDepth,
		"branching_factor", config.BranchingFactor,
	)

	// Set defaults
	// 设置默认值
	if config.MaxDepth == 0 {
		config.MaxDepth = 3
	}
	if config.BranchingFactor == 0 {
		config.BranchingFactor = 3
	}
	if config.BranchingFactor > 4 {
		config.BranchingFactor = 4 // Cap for manageability 注：为便于管理而设上限
	}
	if config.PruningThreshold == 0 {
		config.PruningThreshold = 0.3
	}
	if config.ExplorationBudget == 0 {
		config.ExplorationBudget = 15
	}
	if config.EvaluationMethod == "" {
		config.EvaluationMethod = "scoring"
	}
	if config.ModelTier == "" {
		config.ModelTier = opts.ModelTier
		if config.ModelTier == "" {
			config.ModelTier = "medium"
		}
	}

	result := &TreeOfThoughtsResult{
		BestPath: make([]*ThoughtNode, 0),
	}

	// Initialize root node
	// 初始化根节点
	root := &ThoughtNode{
		ID:          "root",
		Thought:     query,
		Score:       1.0,
		Depth:       0,
		Children:    make([]*ThoughtNode, 0),
		IsTerminal:  false,
		Explanation: "Initial problem statement",
	}
	result.ExplorationTree = root

	// Track exploration budget
	// 跟踪探索预算
	thoughtsExplored := 0
	tokenBudgetPerThought := 0
	if opts.BudgetAgentMax > 0 {
		tokenBudgetPerThought = opts.BudgetAgentMax / config.ExplorationBudget
	}

	// Exploration queue (for BFS-like exploration with scoring)
	// 每轮按 score 排序取最高分节点展开，本质是带评分的 best-first，而非严格 BFS。
	queue := []*ThoughtNode{root}
	allNodes := []*ThoughtNode{root}

	// Main exploration loop
	// 主探索循环
	for len(queue) > 0 && thoughtsExplored < config.ExplorationBudget {
		// Get most promising node (based on score)
		// 取最有希望的节点（按 score 判断）
		sort.Slice(queue, func(i, j int) bool {
			return queue[i].Score > queue[j].Score
		})

		current := queue[0]
		queue = queue[1:]

		// Check depth limit
		// 检查深度限制
		if current.Depth >= config.MaxDepth {
			current.IsTerminal = true
			continue
		}

		logger.Info("Exploring thought node",
			"id", current.ID,
			"depth", current.Depth,
			"score", current.Score,
		)

		// Generate branches from current node
		// 从当前节点生成分支
		branches := generateBranches(
			ctx,
			current,
			query,
			config.BranchingFactor,
			context,
			sessionID,
			history,
			tokenBudgetPerThought,
			config.ModelTier,
			opts,
		)

		thoughtsExplored += len(branches)
		result.TotalThoughts = thoughtsExplored

		// Evaluate and prune branches
		// 低于 PruningThreshold 的分支直接丢弃，不进 queue，用来控制探索规模。
		for _, branch := range branches {
			// Calculate score
			// 计算分数
			branch.Score = evaluateThought(ctx, branch, query, config.EvaluationMethod, opts)

			// Prune low-scoring branches
			// 剪掉低分分支
			if branch.Score < config.PruningThreshold {
				logger.Info("Pruning low-scoring branch",
					"id", branch.ID,
					"score", branch.Score,
				)
				continue
			}

			// Add to tree
			// 加入树中
			current.Children = append(current.Children, branch)
			allNodes = append(allNodes, branch)
			result.TotalTokens += branch.TokensUsed

			// Check if terminal (solution found or dead end)
			// 判断是否为终态（已得解或走进死胡同）
			if isTerminalThought(branch.Thought) {
				branch.IsTerminal = true
			} else {
				// Add to exploration queue
				// 加入探索队列
				queue = append(queue, branch)
			}
		}

		// Update tree depth
		// 更新树深度
		if current.Depth+1 > result.TreeDepth {
			result.TreeDepth = current.Depth + 1
		}
	}

	// Find best path through tree
	// 在树中找出最佳路径
	result.BestPath = findBestPath(root)

	// Generate final solution from best path
	// 基于最佳路径生成最终解
	if len(result.BestPath) > 0 {
		result.BestSolution = synthesizeSolution(result.BestPath, query)
		result.Confidence = calculatePathConfidence(result.BestPath)
	} else {
		result.BestSolution = "No viable solution path found"
		result.Confidence = 0.0
	}

	// Backtrack if enabled and confidence is low
	// 仅在开启 backtrack 且置信度 < 0.5 时回看队列中剩余的高分分支。
	if config.BacktrackEnabled && result.Confidence < 0.5 && len(queue) > 0 {
		logger.Info("Backtracking to explore alternative paths")

		// Explore top alternatives
		// 探索靠前的备选分支
		alternatives := queue[:min(3, len(queue))]
		for _, alt := range alternatives {
			altPath := getPathToNode(alt, allNodes)
			altConfidence := calculatePathConfidence(altPath)

			if altConfidence > result.Confidence {
				result.BestPath = altPath
				result.BestSolution = synthesizeSolution(altPath, query)
				result.Confidence = altConfidence
			}
		}
	}

	logger.Info("Tree-of-Thoughts completed",
		"total_thoughts", result.TotalThoughts,
		"tree_depth", result.TreeDepth,
		"best_path_length", len(result.BestPath),
		"confidence", result.Confidence,
		"tokens", result.TotalTokens,
	)

	return result, nil
}

// generateBranches creates child thoughts from current node
// generateBranches 从当前节点生成子 thought
func generateBranches(
	ctx workflow.Context,
	parent *ThoughtNode,
	originalQuery string,
	branchingFactor int,
	context map[string]any,
	sessionID string,
	history []string,
	tokenBudget int,
	modelTier string,
	opts Options,
) []*ThoughtNode {

	logger := workflow.GetLogger(ctx)
	branches := make([]*ThoughtNode, 0, branchingFactor)

	// Build branching prompt
	// 构造分支生成的 prompt
	branchPrompt := fmt.Sprintf(`Given this problem: %s

Current reasoning path: %s

Generate %d different next steps or approaches to explore. Each should be:
1. A distinct direction or method
2. Building on the current thought
3. Moving toward a solution

Format each as a clear, concise thought.`,
		originalQuery,
		parent.Thought,
		branchingFactor,
	)

	// Generate branches
	// 生成分支
	var branchResult activities.AgentExecutionResult
	if tokenBudget > 0 {
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if context != nil {
			if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		err := workflow.ExecuteActivity(ctx,
			constants.ExecuteAgentWithBudgetActivity,
			activities.BudgetedAgentInput{
				AgentInput: activities.AgentExecutionInput{
					Query:            branchPrompt,
					AgentID:          fmt.Sprintf("tot-generator-%s", parent.ID),
					Context:          context,
					Mode:             "exploration",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					ParentWorkflowID: wid,
				},
				MaxTokens: tokenBudget,
				UserID:    opts.UserID,
				TaskID:    wid,
				ModelTier: modelTier,
			}).Get(ctx, &branchResult)
		if err != nil {
			logger.Warn("Failed to generate branches", "error", err)
			return branches
		}
	} else {
		// Determine parent workflow for streaming correlation
		// 确定用于流式关联的 parent workflow
		wid := workflow.GetInfo(ctx).WorkflowExecution.ID
		if context != nil {
			if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
		}
		err := workflow.ExecuteActivity(ctx,
			activities.ExecuteAgent,
			activities.AgentExecutionInput{
				Query:            branchPrompt,
				AgentID:          fmt.Sprintf("tot-generator-%s", parent.ID),
				Context:          context,
				Mode:             "exploration",
				SessionID:        sessionID,
				UserID:           opts.UserID,
				History:          history,
				ParentWorkflowID: wid,
			}).Get(ctx, &branchResult)
		if err != nil {
			logger.Warn("Failed to generate branches", "error", err)
			return branches
		}
		// Record branch generation usage when not budgeted
		// 未走预算路径时，补记分支生成的用量
		inTok := branchResult.InputTokens
		outTok := branchResult.OutputTokens
		if inTok == 0 && outTok == 0 && branchResult.TokensUsed > 0 {
			inTok = branchResult.TokensUsed * 6 / 10
			outTok = branchResult.TokensUsed - inTok
		}
		model := branchResult.ModelUsed
		if strings.TrimSpace(model) == "" {
			if m := pricing.GetPriorityOneModel(modelTier); m != "" {
				model = m
			}
		}
		provider := branchResult.Provider
		if strings.TrimSpace(provider) == "" {
			provider = imodels.DetectProvider(model)
		}
		recCtx := wopts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       opts.UserID,
			SessionID:    sessionID,
			TaskID:       wid,
			AgentID:      fmt.Sprintf("tot-generator-%s", parent.ID),
			Model:        model,
			Provider:     provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata:     map[string]any{"phase": "tree_of_thoughts"},
		}).Get(recCtx, nil)
		wopts.RecordToolCostEntries(ctx, branchResult, opts.UserID, sessionID, wid)
	}

	// Parse generated branches
	// 解析生成的分支
	thoughts := parseBranches(branchResult.Response, branchingFactor)

	// Create thought nodes
	// 创建 thought 节点
	for i, thought := range thoughts {
		node := &ThoughtNode{
			ID:          fmt.Sprintf("%s-%d", parent.ID, i),
			Thought:     thought,
			Depth:       parent.Depth + 1,
			ParentID:    parent.ID,
			Children:    make([]*ThoughtNode, 0),
			TokensUsed:  branchResult.TokensUsed / len(thoughts), // Distribute tokens 注：按分支均摊 token
			IsTerminal:  false,
			Explanation: fmt.Sprintf("Branch %d from %s", i+1, parent.ID),
		}
		branches = append(branches, node)
	}

	return branches
}

// evaluateThought scores a thought node
// evaluateThought 对 thought 节点打分
func evaluateThought(
	ctx workflow.Context,
	node *ThoughtNode,
	originalQuery string,
	method string,
	opts Options,
) float64 {

	// Simple heuristic scoring
	// 纯关键词启发式：正向词加分、含糊词减分，再按深度惩罚，避免长链虚高。
	score := 0.5 // Base score 注：基础分

	// Check for solution indicators
	// 检查解题标志词
	thought := strings.ToLower(node.Thought)
	if strings.Contains(thought, "therefore") ||
		strings.Contains(thought, "solution") ||
		strings.Contains(thought, "answer") {
		score += 0.2
	}

	// Check for logical progression
	// 检查逻辑推进标志词
	if strings.Contains(thought, "because") ||
		strings.Contains(thought, "since") ||
		strings.Contains(thought, "thus") {
		score += 0.1
	}

	// Check for concrete steps
	// 检查具体步骤标志词
	if strings.Contains(thought, "step") ||
		strings.Contains(thought, "first") ||
		strings.Contains(thought, "next") {
		score += 0.1
	}

	// Penalize vague thoughts
	// 对含糊的 thought 扣分
	if strings.Contains(thought, "maybe") ||
		strings.Contains(thought, "perhaps") ||
		strings.Contains(thought, "might") {
		score -= 0.1
	}

	// Depth penalty (prefer shorter paths)
	// 深度惩罚（偏向更短的路径）
	score -= float64(node.Depth) * 0.05

	// Clamp between 0 and 1
	// 限制在 0 到 1 之间
	if score < 0 {
		score = 0
	}
	if score > 1 {
		score = 1
	}

	return score
}

// isTerminalThought checks if a thought represents a solution or dead end
// isTerminalThought 判断某个 thought 是否代表解或死胡同
func isTerminalThought(thought string) bool {
	lower := strings.ToLower(thought)

	// Solution indicators
	// 解题标志词
	solutionKeywords := []string{
		"the answer is",
		"therefore",
		"in conclusion",
		"final answer",
		"solution:",
		"result:",
	}

	for _, keyword := range solutionKeywords {
		if strings.Contains(lower, keyword) {
			return true
		}
	}

	// Dead end indicators
	// 死胡同标志词
	deadEndKeywords := []string{
		"impossible",
		"cannot be solved",
		"no solution",
		"contradiction",
	}

	for _, keyword := range deadEndKeywords {
		if strings.Contains(lower, keyword) {
			return true
		}
	}

	return false
}

// findBestPath finds the highest scoring path through the tree
// findBestPath 在树中找出得分最高的路径
func findBestPath(root *ThoughtNode) []*ThoughtNode {
	if root == nil {
		return nil
	}

	bestPath := []*ThoughtNode{}
	bestScore := 0.0

	// DFS to find all paths
	// 用路径平均分而非累积分比较，否则更长的路径天然占优。
	var findPaths func(node *ThoughtNode, currentPath []*ThoughtNode, currentScore float64)
	findPaths = func(node *ThoughtNode, currentPath []*ThoughtNode, currentScore float64) {
		currentPath = append(currentPath, node)
		currentScore += node.Score

		// If terminal or leaf, check if best
		// 若已是终态或叶子节点，比较是否为最优
		if node.IsTerminal || len(node.Children) == 0 {
			avgScore := currentScore / float64(len(currentPath))
			if avgScore > bestScore {
				bestScore = avgScore
				bestPath = make([]*ThoughtNode, len(currentPath))
				copy(bestPath, currentPath)
			}
			return
		}

		// Explore children
		// 展开子节点
		for _, child := range node.Children {
			findPaths(child, currentPath, currentScore)
		}
	}

	findPaths(root, []*ThoughtNode{}, 0)
	return bestPath
}

// getPathToNode reconstructs path from root to given node
// getPathToNode 重建从根节点到目标节点的路径
func getPathToNode(target *ThoughtNode, allNodes []*ThoughtNode) []*ThoughtNode {
	path := []*ThoughtNode{}
	current := target

	// Build path backwards
	// 反向拼出路径
	for current != nil {
		path = append([]*ThoughtNode{current}, path...)

		// Find parent
		// 查找父节点
		if current.ParentID == "" {
			break
		}

		parent := findNodeByID(current.ParentID, allNodes)
		current = parent
	}

	return path
}

// findNodeByID finds a node by ID in the list
// findNodeByID 在列表中按 ID 查找节点
func findNodeByID(id string, nodes []*ThoughtNode) *ThoughtNode {
	for _, node := range nodes {
		if node.ID == id {
			return node
		}
	}
	return nil
}

// synthesizeSolution creates final answer from best path
// synthesizeSolution 根据最佳路径生成最终答案
func synthesizeSolution(path []*ThoughtNode, query string) string {
	if len(path) == 0 {
		return "No solution found"
	}

	// Build solution narrative
	// 组织解的叙述
	var solution strings.Builder
	fmt.Fprintf(&solution, "Solution to: %s\n\n", query)
	solution.WriteString("Reasoning path:\n")

	for i, node := range path {
		if i == 0 {
			continue // Skip root 注：跳过根节点
		}
		fmt.Fprintf(&solution, "%d. %s\n", i, node.Thought)
	}

	// Add final answer if present
	// 若有最终答案则追加
	lastNode := path[len(path)-1]
	if lastNode.IsTerminal {
		fmt.Fprintf(&solution, "\nFinal Answer: %s", lastNode.Thought)
	}

	return solution.String()
}

// calculatePathConfidence calculates confidence for a solution path
// calculatePathConfidence 计算某条解路径的置信度
func calculatePathConfidence(path []*ThoughtNode) float64 {
	if len(path) == 0 {
		return 0.0
	}

	totalScore := 0.0
	for _, node := range path {
		totalScore += node.Score
	}

	// Average score with depth penalty
	// 平均分并施加深度惩罚
	avgScore := totalScore / float64(len(path))
	depthPenalty := 1.0 / (1.0 + float64(len(path))*0.1)

	return avgScore * depthPenalty
}

// parseBranches extracts individual thoughts from response
// parseBranches 从响应中抽取各条 thought
func parseBranches(response string, expectedCount int) []string {
	thoughts := []string{}

	// Look for numbered items
	// 查找编号条目
	lines := strings.SplitSeq(response, "\n")
	for line := range lines {
		line = strings.TrimSpace(line)

		// Check for numbered points
		// 检查是否为编号要点
		if strings.HasPrefix(line, "1.") ||
			strings.HasPrefix(line, "2.") ||
			strings.HasPrefix(line, "3.") ||
			strings.HasPrefix(line, "4.") ||
			strings.HasPrefix(line, "-") ||
			strings.HasPrefix(line, "•") {
			// Remove prefix
			// 去掉前缀
			thought := strings.TrimLeft(line, "1234567890.-• ")
			if len(thought) > 10 {
				thoughts = append(thoughts, thought)
			}
		}
	}

	// If not enough structured thoughts, split by sentences
	// 结构化 thought 不足时，按句子切分
	if len(thoughts) < expectedCount {
		sentences := strings.SplitSeq(response, ". ")
		for sent := range sentences {
			if len(thoughts) >= expectedCount {
				break
			}
			if len(sent) > 20 && !util.ContainsString(thoughts, sent) {
				thoughts = append(thoughts, sent)
			}
		}
	}

	// Limit to expected count
	// 截断到期望条数
	if len(thoughts) > expectedCount {
		thoughts = thoughts[:expectedCount]
	}

	return thoughts
}
