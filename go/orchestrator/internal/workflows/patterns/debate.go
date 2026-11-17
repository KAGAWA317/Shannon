package patterns

import (
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	imodels "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	wopts "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// DebateConfig configures the debate pattern
// DebateConfig 配置 debate 模式
type DebateConfig struct {
	NumDebaters      int      // Number of agents in debate (2-5) 注：参与辩论的 agent 数量（2-5）
	MaxRounds        int      // Maximum debate rounds 注：最大辩论轮数
	Perspectives     []string // Different perspectives to represent 注：要代表的不同视角
	RequireConsensus bool     // Whether to require consensus 注：是否要求达成共识
	ModeratorEnabled bool     // Use a moderator agent 注：使用 moderator agent
	VotingEnabled    bool     // Enable voting mechanism 注：启用投票机制
	ModelTier        string   // Model tier for debaters 注：辩手使用的 model tier
}

// DebateResult contains the outcome of a debate
// DebateResult 包含一次辩论的结果
type DebateResult struct {
	FinalPosition    string
	Positions        []DebatePosition
	ConsensusReached bool
	TotalTokens      int
	Rounds           int
	WinningArgument  string
	Votes            map[string]int
}

// DebatePosition represents one agent's position
// DebatePosition 表示单个 agent 的立场
type DebatePosition struct {
	AgentID    string
	Position   string
	Arguments  []string
	Confidence float64
	TokensUsed int
}

// Debate implements multi-agent debate pattern for exploring different perspectives
// Debate 实现多 agent 辩论模式，用于探索不同视角
func Debate(
	ctx workflow.Context,
	query string,
	context map[string]any,
	sessionID string,
	history []string,
	config DebateConfig,
	opts Options,
) (*DebateResult, error) {

	logger := workflow.GetLogger(ctx)
	logger.Info("Starting Debate pattern",
		"query", query,
		"debaters", config.NumDebaters,
		"max_rounds", config.MaxRounds,
	)

	// Set defaults
	// 设置默认值
	if config.NumDebaters == 0 {
		config.NumDebaters = 3
	}
	if config.NumDebaters > 5 {
		config.NumDebaters = 5 // Cap at 5 for manageability 注：上限 5 以保证可管理性
	}
	if config.MaxRounds == 0 {
		config.MaxRounds = 3
	}
	if config.ModelTier == "" {
		config.ModelTier = opts.ModelTier
		if config.ModelTier == "" {
			config.ModelTier = "medium"
		}
	}

	// Initialize perspectives if not provided
	// 未提供视角时初始化默认视角
	if len(config.Perspectives) == 0 {
		config.Perspectives = generateDefaultPerspectives(config.NumDebaters)
	}

	result := &DebateResult{
		Positions: make([]DebatePosition, 0, config.NumDebaters),
		Votes:     make(map[string]int),
	}

	// Track debate history for context
	// 跟踪辩论历史以提供上下文
	debateHistory := []string{}

	// Phase 1: Initial positions
	// 三个阶段：各自立论 → 互相反驳 → 收敛（moderator / 投票 / 直接综合）。
	logger.Info("Phase 1: Gathering initial positions")

	var initialPositions []DebatePosition
	futures := make([]workflow.Future, config.NumDebaters)

	for i := 0; i < config.NumDebaters; i++ {
		perspective := ""
		if i < len(config.Perspectives) {
			perspective = config.Perspectives[i]
		}

		agentID := fmt.Sprintf("debater-%d-%s", i+1, perspective)

		// Create debate context
		// 构建辩论上下文
		debateContext := make(map[string]any)
		maps.Copy(debateContext, context)
		debateContext["perspective"] = perspective
		debateContext["role"] = "debater"
		debateContext["agent_number"] = i + 1

		initialPrompt := fmt.Sprintf(
			"As a %s perspective, provide your position on: %s\n\nBe specific and provide strong arguments.",
			perspective,
			query,
		)

		if opts.BudgetAgentMax > 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if p, ok := debateContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			} else if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
			futures[i] = workflow.ExecuteActivity(ctx,
				constants.ExecuteAgentWithBudgetActivity,
				activities.BudgetedAgentInput{
					AgentInput: activities.AgentExecutionInput{
						Query:            initialPrompt,
						AgentID:          agentID,
						Context:          debateContext,
						Mode:             "debate",
						SessionID:        sessionID,
						UserID:           opts.UserID,
						History:          history,
						ParentWorkflowID: wid,
					},
					MaxTokens: opts.BudgetAgentMax / config.NumDebaters,
					UserID:    opts.UserID,
					TaskID:    wid,
					ModelTier: config.ModelTier,
				})
		} else {
			// Determine parent workflow for streaming correlation
			// 确定父 workflow 以便流式事件关联
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if p, ok := debateContext["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			} else if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
				wid = p
			}
			futures[i] = workflow.ExecuteActivity(ctx,
				activities.ExecuteAgent,
				activities.AgentExecutionInput{
					Query:            initialPrompt,
					AgentID:          agentID,
					Context:          debateContext,
					Mode:             "debate",
					SessionID:        sessionID,
					UserID:           opts.UserID,
					History:          history,
					ParentWorkflowID: wid,
				})
		}
	}

	// Collect initial positions
	// 收集初始立场
	for i, future := range futures {
		var agentResult activities.AgentExecutionResult
		if err := future.Get(ctx, &agentResult); err != nil {
			logger.Warn("Debater failed to provide initial position",
				"agent", i,
				"error", err,
			)
			continue
		}

		position := DebatePosition{
			AgentID:    fmt.Sprintf("debater-%d", i+1),
			Position:   agentResult.Response,
			Arguments:  extractArguments(agentResult.Response),
			Confidence: 0.5, // Initial confidence 注：初始置信度
			TokensUsed: agentResult.TokensUsed,
		}
		initialPositions = append(initialPositions, position)
		result.TotalTokens += agentResult.TokensUsed
		debateHistory = append(debateHistory, fmt.Sprintf("%s: %s", position.AgentID, position.Position))

		// Record token usage for initial positions when not budgeted
		// 非 budget 路径需自行记账；6/10 拆分是缺输入/输出计数时的经验估算。
		if opts.BudgetAgentMax <= 0 {
			wid := workflow.GetInfo(ctx).WorkflowExecution.ID
			if context != nil {
				if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
			}
			inTok := agentResult.InputTokens
			outTok := agentResult.OutputTokens
			if inTok == 0 && outTok == 0 && agentResult.TokensUsed > 0 {
				inTok = agentResult.TokensUsed * 6 / 10
				outTok = agentResult.TokensUsed - inTok
			}
			model := agentResult.ModelUsed
			if strings.TrimSpace(model) == "" {
				if m := pricing.GetPriorityOneModel(config.ModelTier); m != "" {
					model = m
				}
			}
			provider := agentResult.Provider
			if strings.TrimSpace(provider) == "" {
				provider = imodels.DetectProvider(model)
			}
			recCtx := wopts.WithTokenRecordOptions(ctx)
			_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
				UserID:       opts.UserID,
				SessionID:    sessionID,
				TaskID:       wid,
				AgentID:      position.AgentID,
				Model:        model,
				Provider:     provider,
				InputTokens:  inTok,
				OutputTokens: outTok,
				Metadata:     map[string]any{"phase": "debate_initial"},
			}).Get(recCtx, nil)
			wopts.RecordToolCostEntries(ctx, agentResult, opts.UserID, sessionID, wid)
		}
	}

	result.Positions = initialPositions

	// Phase 2: Debate rounds
	// Phase 2：辩论轮次
	for round := 1; round <= config.MaxRounds; round++ {
		logger.Info("Debate round", "round", round)

		roundPositions := make([]DebatePosition, 0, len(initialPositions))
		roundFutures := make([]workflow.Future, len(initialPositions))

		// Each debater responds to others
		// 每个辩手回应其他辩手
		for i, debater := range initialPositions {
			// Build context with other positions
			// 构建包含其他立场的上下文
			othersPositions := []string{}
			for j, other := range initialPositions {
				if i != j {
					othersPositions = append(othersPositions,
						fmt.Sprintf("%s argues: %s", other.AgentID, other.Position))
				}
			}

			responsePrompt := fmt.Sprintf(
				"Round %d: Consider these other perspectives:\n%s\n\n"+
					"As %s, respond with:\n"+
					"1. Counter-arguments to opposing views\n"+
					"2. Strengthen your position\n"+
					"3. Find any common ground\n",
				round,
				strings.Join(othersPositions, "\n"),
				debater.AgentID,
			)

			debateContext := map[string]any{
				"round":           round,
				"debate_history":  debateHistory,
				"other_positions": othersPositions,
			}

			if opts.BudgetAgentMax > 0 {
				wid := workflow.GetInfo(ctx).WorkflowExecution.ID
				if p, ok := debateContext["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				} else if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
				roundFutures[i] = workflow.ExecuteActivity(ctx,
					constants.ExecuteAgentWithBudgetActivity,
					activities.BudgetedAgentInput{
						AgentInput: activities.AgentExecutionInput{
							Query:            responsePrompt,
							AgentID:          debater.AgentID,
							Context:          debateContext,
							Mode:             "debate",
							SessionID:        sessionID,
							UserID:           opts.UserID,
							History:          append(history, debateHistory...),
							ParentWorkflowID: wid,
						},
						MaxTokens: opts.BudgetAgentMax / (config.NumDebaters * config.MaxRounds),
						UserID:    opts.UserID,
						TaskID:    wid,
						ModelTier: config.ModelTier,
					})
			} else {
				// Determine parent workflow for streaming correlation
				// 确定父 workflow 以便流式事件关联
				wid := workflow.GetInfo(ctx).WorkflowExecution.ID
				if p, ok := debateContext["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				} else if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
					wid = p
				}
				roundFutures[i] = workflow.ExecuteActivity(ctx,
					activities.ExecuteAgent,
					activities.AgentExecutionInput{
						Query:            responsePrompt,
						AgentID:          debater.AgentID,
						Context:          debateContext,
						Mode:             "debate",
						SessionID:        sessionID,
						UserID:           opts.UserID,
						History:          append(history, debateHistory...),
						ParentWorkflowID: wid,
					})
			}
		}

		// Collect round responses
		// 收集本轮回应
		for i, future := range roundFutures {
			var agentResult activities.AgentExecutionResult
			if err := future.Get(ctx, &agentResult); err != nil {
				logger.Warn("Debater failed in round",
					"round", round,
					"agent", i,
					"error", err,
				)
				continue
			}

			position := DebatePosition{
				AgentID:    initialPositions[i].AgentID,
				Position:   agentResult.Response,
				Arguments:  extractArguments(agentResult.Response),
				Confidence: calculateArgumentStrength(agentResult.Response),
				TokensUsed: agentResult.TokensUsed,
			}
			roundPositions = append(roundPositions, position)
			result.TotalTokens += agentResult.TokensUsed
			debateHistory = append(debateHistory,
				fmt.Sprintf("Round %d - %s: %s", round, position.AgentID, position.Position))

			// Record token usage for each debate round when not budgeted
			// 非 budget 路径下为每轮辩论记录 token 用量
			if opts.BudgetAgentMax <= 0 {
				wid := workflow.GetInfo(ctx).WorkflowExecution.ID
				if context != nil {
					if p, ok := context["parent_workflow_id"].(string); ok && p != "" {
						wid = p
					}
				}
				inTok := agentResult.InputTokens
				outTok := agentResult.OutputTokens
				if inTok == 0 && outTok == 0 && agentResult.TokensUsed > 0 {
					inTok = agentResult.TokensUsed * 6 / 10
					outTok = agentResult.TokensUsed - inTok
				}
				model := agentResult.ModelUsed
				if strings.TrimSpace(model) == "" {
					if m := pricing.GetPriorityOneModel(config.ModelTier); m != "" {
						model = m
					}
				}
				provider := agentResult.Provider
				if strings.TrimSpace(provider) == "" {
					provider = imodels.DetectProvider(model)
				}
				recCtx := wopts.WithTokenRecordOptions(ctx)
				_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
					UserID:       opts.UserID,
					SessionID:    sessionID,
					TaskID:       wid,
					AgentID:      position.AgentID,
					Model:        model,
					Provider:     provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     map[string]any{"phase": "debate_round", "round": round},
				}).Get(recCtx, nil)
				wopts.RecordToolCostEntries(ctx, agentResult, opts.UserID, sessionID, wid)
			}
		}

		// Update positions with latest round
		// 用最新一轮更新立场
		initialPositions = roundPositions
		result.Positions = roundPositions
		result.Rounds = round

		// Check for consensus if required
		// 若要求共识则检查是否已达成
		if config.RequireConsensus && checkConsensus(roundPositions) {
			result.ConsensusReached = true
			logger.Info("Consensus reached", "round", round)
			break
		}
	}

	// Phase 3: Resolution (Moderator or Voting)
	// Phase 3：收敛（moderator 或投票）
	if config.ModeratorEnabled {
		result.FinalPosition = moderateDebate(ctx, result.Positions, query, debateHistory, opts)
	} else if config.VotingEnabled {
		result.FinalPosition, result.Votes = conductVoting(result.Positions)
	} else {
		// Synthesize positions
		// 综合各方立场
		result.FinalPosition = synthesizePositions(result.Positions, query)
	}

	// Determine winning argument
	// 确定获胜论点
	bestConfidence := 0.0
	for _, pos := range result.Positions {
		if pos.Confidence > bestConfidence {
			bestConfidence = pos.Confidence
			result.WinningArgument = pos.Position
		}
	}

	logger.Info("Debate completed",
		"rounds", result.Rounds,
		"consensus", result.ConsensusReached,
		"total_tokens", result.TotalTokens,
	)

	// Persist debate consensus for learning (fire-and-forget)
	// fire-and-forget：持久化失败不影响本次结果，version gate 保证旧运行重放不触发。
	consensusVersion := workflow.GetVersion(ctx, "persist_debate_consensus_v1", workflow.DefaultVersion, 1)
	if consensusVersion >= 1 {
		persistCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 5 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})

		// Extract all position texts for storage
		// 提取所有立场文本用于存储
		positionTexts := make([]string, len(result.Positions))
		for i, pos := range result.Positions {
			positionTexts[i] = pos.Position
		}

		// Calculate consensus confidence
		// 计算共识置信度
		consensusConfidence := bestConfidence
		if result.ConsensusReached {
			consensusConfidence = 0.9 // High confidence when consensus reached 注：达成共识时给予高置信度
		}

		workflow.ExecuteActivity(
			persistCtx,
			activities.PersistDebateConsensus,
			activities.PersistDebateConsensusInput{
				SessionID:        sessionID,
				Topic:            query,
				WinningPosition:  result.FinalPosition,
				ConsensusReached: result.ConsensusReached,
				Confidence:       consensusConfidence,
				Positions:        positionTexts,
				Metadata: map[string]any{
					"rounds":       result.Rounds,
					"total_tokens": result.TotalTokens,
					"num_debaters": config.NumDebaters,
					"voting":       config.VotingEnabled,
				},
			},
		)
	}

	return result, nil
}

// generateDefaultPerspectives creates default debate perspectives
// generateDefaultPerspectives 创建默认的辩论视角
func generateDefaultPerspectives(num int) []string {
	perspectives := []string{
		"optimistic",
		"skeptical",
		"practical",
		"innovative",
		"conservative",
	}

	if num <= len(perspectives) {
		return perspectives[:num]
	}
	return perspectives
}

// extractArguments parses key arguments from a position
// extractArguments 从立场中解析关键论点
func extractArguments(position string) []string {
	arguments := []string{}

	// Look for numbered points
	// 查找带编号的要点
	lines := strings.SplitSeq(position, "\n")
	for line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "1.") ||
			strings.HasPrefix(line, "2.") ||
			strings.HasPrefix(line, "3.") ||
			strings.HasPrefix(line, "-") ||
			strings.HasPrefix(line, "•") {
			arguments = append(arguments, line)
		}
	}

	// If no structured arguments, extract sentences
	// 若无结构化论点，则抽取句子
	if len(arguments) == 0 {
		sentences := strings.Split(position, ". ")
		for i, sent := range sentences {
			if i >= 3 { // Limit to key points 注：只取关键点
				break
			}
			if len(sent) > 20 {
				arguments = append(arguments, sent)
			}
		}
	}

	return arguments
}

// calculateArgumentStrength estimates the strength of arguments
// calculateArgumentStrength 估计论点的强度
func calculateArgumentStrength(response string) float64 {
	strength := 0.5

	// Evidence indicators
	// 证据类指示词
	if strings.Contains(strings.ToLower(response), "evidence") ||
		strings.Contains(strings.ToLower(response), "study") ||
		strings.Contains(strings.ToLower(response), "data") {
		strength += 0.15
	}

	// Logical structure
	// 逻辑结构类指示词
	if strings.Contains(response, "therefore") ||
		strings.Contains(response, "because") ||
		strings.Contains(response, "consequently") {
		strength += 0.1
	}

	// Counter-arguments addressed
	// 是否回应了反方观点
	if strings.Contains(strings.ToLower(response), "however") ||
		strings.Contains(strings.ToLower(response), "although") ||
		strings.Contains(strings.ToLower(response), "counter") {
		strength += 0.15
	}

	// Specific examples
	// 具体例子类指示词
	if strings.Contains(strings.ToLower(response), "for example") ||
		strings.Contains(strings.ToLower(response), "such as") ||
		strings.Contains(strings.ToLower(response), "instance") {
		strength += 0.1
	}

	if strength > 1.0 {
		strength = 1.0
	}

	return strength
}

// checkConsensus determines if positions have converged
// checkConsensus 判断各方立场是否已收敛
func checkConsensus(positions []DebatePosition) bool {
	if len(positions) < 2 {
		return true
	}

	// Simple consensus check: look for agreement keywords
	// 用关键词占比近似共识，不做语义比对——成本低，但会漏掉换述式的一致。
	agreementCount := 0
	for _, pos := range positions {
		lower := strings.ToLower(pos.Position)
		if strings.Contains(lower, "agree") ||
			strings.Contains(lower, "consensus") ||
			strings.Contains(lower, "common ground") ||
			strings.Contains(lower, "alignment") {
			agreementCount++
		}
	}

	// If majority show agreement, consider consensus reached
	// 多数表示赞同即视为达成共识
	return agreementCount > len(positions)/2
}

// moderateDebate uses a moderator to synthesize the debate
// moderateDebate 由 moderator 综合整场辩论
func moderateDebate(ctx workflow.Context, positions []DebatePosition, query string, history []string, opts Options) string {
	// Simplified moderation - in practice would use another agent
	// 目前 moderator 只是复用综合逻辑，并没有真的调用第三个 agent。
	return synthesizePositions(positions, query)
}

// conductVoting tallies votes from positions
// conductVoting 统计各立场的票数
func conductVoting(positions []DebatePosition) (string, map[string]int) {
	votes := make(map[string]int)

	// Simple voting based on confidence
	// 基于置信度的简易投票
	winner := positions[0]
	for _, pos := range positions {
		votes[pos.AgentID] = int(pos.Confidence * 100)
		if pos.Confidence > winner.Confidence {
			winner = pos
		}
	}

	return winner.Position, votes
}

// synthesizePositions combines multiple debate positions
// synthesizePositions 合并多个辩论立场
func synthesizePositions(positions []DebatePosition, query string) string {
	if len(positions) == 0 {
		return "No positions available"
	}

	// Find strongest arguments across all positions
	// 在所有立场中找出最强论点
	allArguments := []string{}
	for _, pos := range positions {
		allArguments = append(allArguments, pos.Arguments...)
	}

	// Build synthesis
	// 构建综合结果
	var synthesis strings.Builder
	fmt.Fprintf(&synthesis, "After debate on '%s':\n\n", query)

	// Add strongest position
	// 加入最强立场
	strongest := positions[0]
	for _, pos := range positions {
		if pos.Confidence > strongest.Confidence {
			strongest = pos
		}
	}
	fmt.Fprintf(&synthesis, "Strongest Position: %s\n\n", strongest.Position)

	// Add key arguments
	// 加入关键论点
	if len(allArguments) > 0 {
		synthesis.WriteString("Key Arguments:\n")
		for i, arg := range allArguments {
			if i >= 5 { // Limit to top arguments 注：只取最重要的论点
				break
			}
			fmt.Fprintf(&synthesis, "- %s\n", arg)
		}
	}

	return synthesis.String()
}
