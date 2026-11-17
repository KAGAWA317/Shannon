package patterns

import "maps"

// Budget thresholds (in tokens) for triggering degradation.
// Updated for modern LLMs with 128K+ context windows (2025).
// Previous thresholds were designed for GPT-3.5 era (4K-8K context).
// 降级阈值（token）已按 2025 年 128K+ context 窗口的 model 调整；
// 旧阈值是为 GPT-3.5 时代（4K-8K context）设计的。
var defaultDegradeThresholds = map[PatternType]int{
	PatternTreeOfThoughts: 8000, // Was 2000 注：原为 2000
	PatternChainOfThought: 4000, // Was 1000 注：原为 1000
	PatternDebate:         6000, // Was 1500 注：原为 1500
	PatternReflection:     3000, // Was 800 注：原为 800
}

// degradeChain defines the next strategy when degrading from a given pattern.
// degradeChain 定义从某个 pattern 降级时采用的下一档策略。
var degradeChain = map[PatternType]PatternType{
	PatternTreeOfThoughts: PatternChainOfThought,
	PatternChainOfThought: PatternReact,
	PatternDebate:         PatternReflection,
	PatternReflection:     PatternReact,
}

// DegradationThresholds returns a copy of the default thresholds map.
// DegradationThresholds 返回默认阈值表的副本。
func DegradationThresholds() map[PatternType]int {
	out := make(map[PatternType]int, len(defaultDegradeThresholds))
	maps.Copy(out, defaultDegradeThresholds)
	return out
}

// DegradeByBudget returns the most complex strategy that fits within the supplied budget.
// If no degradation is applied, the returned pattern matches the input.
// DegradeByBudget 返回在给定 budget 内可行的最复杂策略。
// 若未发生降级，返回的 pattern 与输入相同。
func DegradeByBudget(strategy PatternType, budget int, thresholds map[PatternType]int) (PatternType, bool) {
	if budget <= 0 {
		return strategy, false
	}

	table := thresholds
	if table == nil {
		table = defaultDegradeThresholds
	}

	current := strategy
	degraded := false

	for {
		threshold, ok := table[current]
		if !ok || threshold <= 0 || budget >= threshold {
			break
		}
		next, ok := degradeChain[current]
		if !ok || next == "" {
			break
		}
		current = next
		degraded = true
	}

	return current, degraded
}
