package workflows

import (
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	enums "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
)

// isTransientError classifies tool errors as transient (rate limit, timeout) vs permanent.
// Transient errors warrant backoff and retry; permanent errors count toward abort threshold.
// 瞬时错误（rate limit、timeout）退避 retry；永久错误计入中止阈值。
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	transientPatterns := []string{"rate limit", "429", "timeout", "timed out", "temporary", "unavailable", "503", "502"}
	for _, p := range transientPatterns {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

// leadDisplayMessage returns the user-facing display message for a Lead decision.
// leadDisplayMessage 返回 Lead 决策面向用户的展示消息。
// Prefers user_summary (short, user-friendly) with fallback to decision_summary.
// 优先 user_summary（简短、对用户友好），回退到 decision_summary。
func leadDisplayMessage(decision activities.LeadDecisionResult) string {
	if decision.UserSummary != "" {
		return decision.UserSummary
	}
	return truncateDecisionSummary(decision.DecisionSummary)
}

// truncateDecisionSummary caps the decision_summary to 12000 chars.
// truncateDecisionSummary 把 decision_summary 截断到 12000 字符。
func truncateDecisionSummary(s string) string {
	if len(s) > 12000 {
		return s[:12000]
	}
	return s
}

// buildObservationText creates a frozen tool result digest for multi-turn cache replay.
// buildObservationText 为多轮 cache 重放生成冻结的 tool 结果摘要。
func buildObservationText(action string, result any) string {
	resultStr := fmt.Sprintf("%v", result)
	const maxLen = 3000
	if len(resultStr) > maxLen {
		resultStr = resultStr[:maxLen]
	}
	return fmt.Sprintf("%s → %s", action, resultStr)
}

// taskHasUnmetDeps checks if a task's depends_on constraints are all satisfied.
// taskHasUnmetDeps 检查 task 的 depends_on 约束是否全部满足。
// Returns true if ANY dependency is not yet "completed" in the task list.
// 任务列表中只要有任一依赖尚未 "completed" 就返回 true。
func taskHasUnmetDeps(taskID string, taskList []activities.SwarmTask) bool {
	for _, t := range taskList {
		if t.ID == taskID && len(t.DependsOn) > 0 {
			for _, dep := range t.DependsOn {
				depComplete := false
				for _, other := range taskList {
					if other.ID == dep && other.Status == "completed" {
						depComplete = true
						break
					}
				}
				if !depComplete {
					return true
				}
			}
		}
	}
	return false
}

// hasAssignablePendingTask returns true if any pending task has all dependencies met.
// Used by checkpoint skip to avoid waking Lead when only blocked tasks remain.
// 用于 checkpoint 跳过逻辑：只剩被阻塞的 task 时不必唤醒 Lead。
func hasAssignablePendingTask(taskList []activities.SwarmTask) bool {
	for _, t := range taskList {
		if t.Status == "pending" && !taskHasUnmetDeps(t.ID, taskList) {
			return true
		}
	}
	return false
}

// Only filters specific tool output patterns (file_write blobs, raw action JSON),
// not arbitrary JSON which may contain valid structured results.
// 只过滤特定 tool 输出形态（file_write 大块内容、裸 action JSON），
// 不过滤任意 JSON，以免误删合法结构化结果。
func looksLikeToolJSON(s string) bool {
	trimmed := strings.TrimSpace(s)
	if len(trimmed) == 0 {
		return false
	}
	if trimmed[0] != '{' {
		return false
	}
	// file_write raw output: {"path": "...", "content": "..."} — large blobs
	// file_write 原始输出：{"path": "...", "content": "..."} —— 大块内容
	if strings.Contains(trimmed, `"path"`) && strings.Contains(trimmed, `"content"`) && len(trimmed) > 500 {
		return true
	}
	// Raw action JSON leaked through: {"action": "tool_call", "tool": "..."}
	// 泄漏出来的裸 action JSON：{"action": "tool_call", "tool": "..."}
	if strings.Contains(trimmed, `"action"`) && strings.Contains(trimmed, `"tool"`) && strings.Contains(trimmed, `"tool_params"`) {
		return true
	}
	return false
}

// jaccardSimilarity computes word-level Jaccard similarity between two strings.
// jaccardSimilarity 计算两个字符串的词级 Jaccard 相似度。
// Used by search saturation detection (B3).
// 供搜索饱和检测使用（B3）。
func jaccardSimilarity(a, b string) float64 {
	wordsA := toWordSet(strings.ToLower(a))
	wordsB := toWordSet(strings.ToLower(b))
	if len(wordsA) == 0 && len(wordsB) == 0 {
		return 1.0
	}
	intersection := 0
	union := make(map[string]bool)
	for w := range wordsA {
		union[w] = true
		if wordsB[w] {
			intersection++
		}
	}
	for w := range wordsB {
		union[w] = true
	}
	if len(union) == 0 {
		return 0
	}
	return float64(intersection) / float64(len(union))
}

func toWordSet(s string) map[string]bool {
	set := make(map[string]bool)
	for w := range strings.FieldsSeq(s) {
		if len(w) > 1 { // Skip single-char noise 注：跳过单字符噪声
			set[w] = true
		}
	}
	return set
}

// isSearchSaturated checks if the last N queries in a window are too similar.
// isSearchSaturated 检查窗口内最近 N 个 query 是否过于相似。
// Returns true if most pairwise comparisons within the window exceed minSimilarity.
// 窗口内多数两两比较超过 minSimilarity 时返回 true。
func isSearchSaturated(queries []string, window int, minSimilarity float64) bool {
	if len(queries) < window {
		return false
	}
	recent := queries[len(queries)-window:]
	similarCount := 0
	totalPairs := 0
	for i := range recent {
		for j := i + 1; j < len(recent); j++ {
			totalPairs++
			if jaccardSimilarity(recent[i], recent[j]) >= minSimilarity {
				similarCount++
			}
		}
	}
	// Need most pairs to be similar (allow 1 outlier)
	// 需要多数配对相似（允许 1 个离群值）
	return totalPairs > 0 && similarCount >= totalPairs-1
}

// buildToolsUsedSummary creates a compact one-line summary of tool calls from history.
// Example output: "web_search(x3), file_read(x2), file_write(x1)"
// This is included in idle signals so the parent can inject it on reassignment,
// preventing agents from repeating the same searches/reads.
// 该摘要随 idle 信号传给父 workflow，Lead 重新派活时可注入，避免 agent 重复相同的搜索/读取。
func buildToolsUsedSummary(history []activities.AgentLoopTurn) string {
	counts := make(map[string]int)
	for _, turn := range history {
		action := turn.Action
		if action == "" || action == "idle" || action == "done_converted_to_idle" {
			continue
		}
		counts[action]++
	}
	if len(counts) == 0 {
		return ""
	}
	parts := make([]string, 0, len(counts))
	for action, count := range counts {
		parts = append(parts, fmt.Sprintf("%s(x%d)", action, count))
	}
	sort.Strings(parts)
	return strings.Join(parts, ", ")
}

// toFloat64 safely converts interface{} to float64 (JSON numbers are float64).
// toFloat64 安全地把 interface{} 转成 float64（JSON 数字即 float64）。
func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

// convertHistoryForLead converts session conversation history into a concise format for Lead.
// convertHistoryForLead 把 session 对话历史转成给 Lead 的紧凑格式。
// Keeps last few exchanges, truncates long assistant responses to avoid bloating Lead's context.
// 保留最近几轮交互，截断过长的 assistant 回复，避免撑大 Lead 的 context。
func convertHistoryForLead(messages []Message) []map[string]any {
	const maxMessages = 6 // Last 3 turns (user + assistant) 注：最近 3 轮（user + assistant）
	const maxAssistantLen = 800

	start := 0
	if len(messages) > maxMessages {
		start = len(messages) - maxMessages
	}

	result := make([]map[string]any, 0, len(messages)-start)
	for _, msg := range messages[start:] {
		content := msg.Content
		if msg.Role == "assistant" && len(content) > maxAssistantLen {
			content = content[:maxAssistantLen] + "\n... [truncated — use file_read to check workspace for full details]"
		}
		result = append(result, map[string]any{
			"role":    msg.Role,
			"content": content,
		})
	}
	return result
}

// ── fetchTracker ────────────────────────────────────────────────────────────
// Consolidates L1 fetch cache, L2 team knowledge, and L3 search overlap
// into a single structure, eliminating duplicated check/write logic.
// 把 L1 抓取 cache、L2 团队知识与 L3 搜索重叠合并到同一结构，消除重复的检查/写入逻辑。

const fetchTrackerMaxEntries = 200 // cap to prevent unbounded memory growth 注：上限，防止内存无界增长

type fetchTracker struct {
	cache      map[string]string               // L1: normalized URL → full response 注：L1：规范化 URL → 完整响应
	discovered map[string]string               // L3: normalized URL → discovering agent 注：L3：规范化 URL → 发现该 URL 的 agent
	knowledge  []activities.TeamKnowledgeEntry // L2: metadata for cross-agent sharing 注：L2：跨 agent 共享的 metadata
}

func newFetchTracker(seed []activities.TeamKnowledgeEntry) *fetchTracker {
	ft := &fetchTracker{
		cache:      make(map[string]string),
		discovered: make(map[string]string),
		knowledge:  make([]activities.TeamKnowledgeEntry, 0, len(seed)),
	}
	if len(seed) > 0 {
		ft.knowledge = append(ft.knowledge, seed...)
		for _, tk := range seed {
			if norm := normalizeURL(tk.URL); norm != "" {
				ft.discovered[norm] = tk.Agent
			}
		}
	}
	return ft
}

// checkCache returns cached content if ALL requested URLs are cached (L1).
// checkCache 在所有请求的 URL 都已缓存时返回缓存内容（L1）。
// Returns ("", false) when ft is nil, tool is not a fetch, or any URL is uncached.
// ft 为 nil、tool 不是抓取类、或有任一 URL 未缓存时返回 ("", false)。
func (ft *fetchTracker) checkCache(tool string, params map[string]any) (string, bool) {
	if ft == nil || (tool != "web_fetch" && tool != "web_subpage_fetch") {
		return "", false
	}
	urls := extractURLsFromParams(params)
	if len(urls) == 0 {
		return "", false
	}
	for _, rawURL := range urls {
		if _, ok := ft.cache[normalizeURL(rawURL)]; !ok {
			return "", false
		}
	}
	var b strings.Builder
	for i, rawURL := range urls {
		if i > 0 {
			b.WriteString("\n---\n")
		}
		b.WriteString(ft.cache[normalizeURL(rawURL)])
	}
	return b.String(), true
}

// writeCache stores fetch results and updates knowledge + discovered (L1 write).
// writeCache 保存抓取结果并更新 knowledge 与 discovered（L1 写入）。
// Returns URLs that were newly added (for logging).
// 返回新增的 URL（用于日志）。
func (ft *fetchTracker) writeCache(tool string, params map[string]any, response any, agentID string) []string {
	if ft == nil || (tool != "web_fetch" && tool != "web_subpage_fetch") {
		return nil
	}
	urls := extractURLsFromParams(params)
	respStr := fmt.Sprintf("%v", response)
	var added []string
	for _, rawURL := range urls {
		norm := normalizeURL(rawURL)
		if _, exists := ft.cache[norm]; exists {
			continue
		}
		if len(ft.cache) >= fetchTrackerMaxEntries {
			break
		}
		ft.cache[norm] = respStr
		ft.discovered[norm] = agentID
		ft.knowledge = append(ft.knowledge, activities.TeamKnowledgeEntry{
			URL:       rawURL,
			Agent:     agentID,
			Summary:   truncateToSentence(respStr, 200),
			CharCount: len(respStr),
		})
		added = append(added, rawURL)
	}
	return added
}

// checkSearchOverlap registers new URLs and returns overlap stats (L3).
// checkSearchOverlap 登记新 URL 并返回重叠统计（L3）。
func (ft *fetchTracker) checkSearchOverlap(result any, agentID string) (overlapPct float64, known, total int) {
	if ft == nil || result == nil {
		return 0, 0, 0
	}
	searchURLs := extractURLsFromSearchResults(fmt.Sprintf("%v", result))
	newCount := 0
	for _, su := range searchURLs {
		norm := normalizeURL(su)
		if _, exists := ft.discovered[norm]; exists {
			known++
		} else {
			newCount++
			ft.discovered[norm] = agentID
		}
	}
	total = known + newCount
	if total > 0 && known > 0 {
		overlapPct = float64(known) / float64(total) * 100
	}
	return overlapPct, known, total
}

// Knowledge returns the accumulated team knowledge entries (nil-safe).
// Knowledge 返回累积的团队知识条目（nil 安全）。
func (ft *fetchTracker) Knowledge() []activities.TeamKnowledgeEntry {
	if ft == nil {
		return nil
	}
	return ft.knowledge
}

// mergeTeamKnowledge deduplicates and appends new entries into the global list.
// mergeTeamKnowledge 去重后把新条目追加到全局列表。
func mergeTeamKnowledge(global []activities.TeamKnowledgeEntry, incoming []activities.TeamKnowledgeEntry) []activities.TeamKnowledgeEntry {
	seen := make(map[string]bool, len(global))
	for _, e := range global {
		seen[normalizeURL(e.URL)] = true
	}
	for _, e := range incoming {
		norm := normalizeURL(e.URL)
		if norm != "" && !seen[norm] {
			seen[norm] = true
			global = append(global, e)
		}
	}
	return global
}

// normalizeURL strips fragments and trailing slashes, lowercases scheme+host,
// preserves path case and query parameters.
// NOTE: Related but distinct from metadata.NormalizeURL (which also strips utm/www).
// 注意与 metadata.NormalizeURL 不同：后者还会去掉 utm 参数与 www。
func normalizeURL(raw string) string {
	if raw == "" {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Scheme == "" {
		return raw
	}
	u.Fragment = ""
	path := strings.TrimRight(u.Path, "/")
	if path == "" {
		path = "/"
	}
	base := strings.ToLower(u.Scheme) + "://" + strings.ToLower(u.Host) + path
	if u.RawQuery != "" {
		return base + "?" + u.RawQuery
	}
	return base
}

// truncateToSentence cuts text at sentence boundary or maxLen.
// truncateToSentence 在句子边界或 maxLen 处截断文本。
func truncateToSentence(text string, maxLen int) string {
	runes := []rune(text)
	if len(runes) <= maxLen {
		return text
	}
	cut := string(runes[:maxLen])
	if idx := strings.LastIndex(cut, ". "); idx > maxLen/2 {
		return cut[:idx+1]
	}
	return cut + "..."
}

// extractURLsFromParams extracts URLs from tool_call params (handles both "url" string and "urls" array).
// extractURLsFromParams 从 tool_call 参数中提取 URL（兼容 "url" 字符串与 "urls" 数组）。
func extractURLsFromParams(params map[string]any) []string {
	var urls []string
	if u, ok := params["url"].(string); ok && u != "" {
		urls = append(urls, u)
	}
	if arr, ok := params["urls"].([]any); ok {
		for _, v := range arr {
			if s, ok := v.(string); ok && s != "" {
				urls = append(urls, s)
			}
		}
	}
	return urls
}

// extractURLsFromSearchResults extracts URLs from search result text using regex.
// extractURLsFromSearchResults 用正则从搜索结果文本中提取 URL。
func extractURLsFromSearchResults(result string) []string {
	re := regexp.MustCompile(`https?://[^\s"'\]>]+`)
	matches := re.FindAllString(result, -1)
	seen := make(map[string]bool)
	var urls []string
	for _, m := range matches {
		m = strings.TrimRight(m, ".,;:!?)")
		if !seen[m] {
			seen[m] = true
			urls = append(urls, m)
		}
	}
	return urls
}

// ── AgentLoop ──────────────────────────────────────────────────────────────────
// ── AgentLoop：持久化 agent 循环 ───────────────────────────────────────────────

// TeamMember describes a teammate visible to each agent for collaboration.
// TeamMember 描述每个 agent 可见的协作队友。
type TeamMember struct {
	AgentID string `json:"agent_id"`
	Task    string `json:"task"`
	Role    string `json:"role,omitempty"`
}

// AgentLoopInput is the input for a persistent agent loop.
// AgentLoopInput 是持久化 agent 循环的输入。
type AgentLoopInput struct {
	AgentID               string                          `json:"agent_id"`
	WorkflowID            string                          `json:"workflow_id"`
	Task                  string                          `json:"task"`
	MaxIterations         int                             `json:"max_iterations"`
	SessionID             string                          `json:"session_id,omitempty"`
	UserID                string                          `json:"user_id,omitempty"`
	Context               map[string]any                  `json:"context,omitempty"`
	TeamRoster            []TeamMember                    `json:"team_roster,omitempty"`
	WorkspaceMaxEntries   int                             `json:"workspace_max_entries,omitempty"`
	WorkspaceSnippetChars int                             `json:"workspace_snippet_chars,omitempty"`
	Role                  string                          `json:"role,omitempty"`           // Persona role: researcher, coder, analyst, generalist 注：角色定位：researcher、coder、analyst、generalist
	ReassignCount         int                             `json:"reassign_count,omitempty"` // B1: tracks reassignment count per agent 注：B1：记录每个 agent 的重新派活次数
	OriginalQuery         string                          `json:"original_query,omitempty"` // User's original query for agent context 注：用户原始 query，用于 agent context
	TeamKnowledge         []activities.TeamKnowledgeEntry `json:"team_knowledge,omitempty"` // Cross-agent fetch knowledge (L2 dedup) 注：跨 agent 抓取知识（L2 去重）
}

// AgentLoopResult is the final result from a persistent agent.
// AgentLoopResult 是持久化 agent 的最终结果。
type AgentLoopResult struct {
	AgentID               string                          `json:"agent_id"`
	Role                  string                          `json:"role,omitempty"`
	Response              string                          `json:"response"`
	Iterations            int                             `json:"iterations"`
	TokensUsed            int                             `json:"tokens_used"`
	InputTokens           int                             `json:"input_tokens"`
	OutputTokens          int                             `json:"output_tokens"`
	CacheReadTokens       int                             `json:"cache_read_tokens"`
	CacheCreationTokens   int                             `json:"cache_creation_tokens"`
	CacheCreation1hTokens int                             `json:"cache_creation_1h_tokens"`
	ModelUsed             string                          `json:"model_used"`
	Provider              string                          `json:"provider"`
	Success               bool                            `json:"success"`
	Error                 string                          `json:"error,omitempty"`
	ToolsUsed             string                          `json:"tools_used,omitempty"`     // Compact summary: "web_search(x3), file_read(x2)" 注：紧凑摘要："web_search(x3), file_read(x2)"
	TeamKnowledge         []activities.TeamKnowledgeEntry `json:"team_knowledge,omitempty"` // URLs fetched by this agent (for parent merge) 注：该 agent 抓取的 URL（供父 workflow 合并）
}

// AgentLoop is a persistent agent workflow that runs a reason-act cycle.
// AgentLoop 是运行 reason-act 循环的持久化 agent workflow。
// Each iteration: check mailbox → call LLM → execute action → loop.
// 每轮：检查 mailbox → 调用 LLM → 执行动作 → 循环。
func AgentLoop(ctx workflow.Context, input AgentLoopInput) (AgentLoopResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("AgentLoop started",
		"agent_id", input.AgentID,
		"task", input.Task,
		"max_iterations", input.MaxIterations,
	)

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 3 * time.Minute, // 90s too short for synthesis_writer generating long reports 注：90 秒对生成长报告的 synthesis_writer 太短
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	}
	ctx = workflow.WithActivityOptions(ctx, actOpts)

	// Short timeout for P2P activities
	// P2P activity 使用较短超时
	p2pCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Version gate for P2P activities (RegisterFile, SendAgentMessage, WorkspaceAppend).
	// Old workflows in replay must not execute these new activity calls.
	// P2P activity 的版本闸门：重放中的旧 workflow 不得执行这些新增 activity 调用。
	p2pV := workflow.GetVersion(ctx, "agent_loop_p2p_v1", workflow.DefaultVersion, 1)

	// Emit agent started event
	// 发出 agent 启动事件
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: input.WorkflowID,
		EventType:  activities.StreamEventAgentStarted,
		AgentID:    input.AgentID,
		Message:    activities.MsgAgentStarted(input.AgentID),
		Timestamp:  workflow.Now(ctx),
		Payload:    map[string]any{"role": input.Role},
	}).Get(ctx, nil)

	// Convert team roster to activity-level type
	// 把 team roster 转成 activity 层类型
	var teamRoster []activities.TeamMemberInfo
	for _, tm := range input.TeamRoster {
		teamRoster = append(teamRoster, activities.TeamMemberInfo{
			AgentID: tm.AgentID,
			Task:    tm.Task,
			Role:    tm.Role,
		})
	}

	// Apply guardrail defaults if not set
	// 未设置时套用 guardrail 默认值
	wsMaxEntries := input.WorkspaceMaxEntries
	if wsMaxEntries <= 0 {
		wsMaxEntries = 5
	}
	wsSnippetChars := input.WorkspaceSnippetChars
	if wsSnippetChars <= 0 {
		wsSnippetChars = 800
	}

	modelTier := deriveModelTier(input.Context)

	var history []activities.AgentLoopTurn
	var totalTokens, totalInput, totalOutput int
	var totalCacheRead, totalCacheCreation, totalCacheCreation1h int
	var lastModel, lastProvider string
	var savedDoneResponse string // Preserved when "done" is converted to "idle" 注："done" 被改写为 "idle" 时保留
	var lastWorkspaceSeq uint64
	var lastMailboxSeq uint64
	var filesWritten []string                     // Paths written by file_write during current task 注：当前 task 中 file_write 写入的路径
	var consecutiveToolErrors int                 // Track consecutive tool failures to prevent infinite loops 注：跟踪连续 tool 失败，防止死循环
	var consecutiveNonToolActions int             // Track iterations without tool calls for convergence detection 注：跟踪未调用 tool 的轮数，用于收敛检测
	var consecutiveMsgWithoutWrite int            // Track send_message/publish_data without file_write 注：跟踪未伴随 file_write 的 send_message/publish_data
	var consecutiveTransientErrors int            // Track transient errors for escalating backoff 注：跟踪瞬时错误以实现递增退避
	var stepResult activities.AgentLoopStepResult // Declared outside loop so shutdown check can reference previous iteration's result 注：在循环外声明，便于关停检查引用上一轮结果
	var lastResponseID string                     // OpenAI Responses API: chain from previous response for cache reuse 注：OpenAI Responses API：串联上一次 response 以复用 cache
	var pendingSystemMessage string               // Urgent directive for next iteration (rendered at prompt end for recency bias) 注：下一轮的紧急指令（渲染在 prompt 末尾以利用近因效应）
	var runningNotes string                       // Agent's cumulative notes — survives history truncation 注：agent 的累积笔记——在历史截断后仍保留
	var recentSearchQueries []string              // B3: Track web_search queries for saturation detection 注：B3：记录 web_search query 用于饱和检测
	var searchesSinceLastFetch int                // B4: Detect consecutive searches without fetch 注：B4：检测连续搜索未抓取
	var cumulativeToolCalls int                   // Total tool_calls across all tasks — survives reassignment 注：所有 task 的 tool_call 总数——跨重新派活保留

	// Extract model/provider overrides for tool execution calls
	// 提取 tool 执行调用的 model/provider 覆盖项
	toolModelOverride := GetContextString(input.Context, "model_override")
	toolProviderOverride := GetContextString(input.Context, "provider_override")

	// Team knowledge dedup: L1 fetch cache, L2 cross-agent knowledge, L3 search overlap.
	// 团队知识去重：L1 抓取 cache、L2 跨 agent 知识、L3 搜索重叠。
	teamKnowledgeVersion := workflow.GetVersion(ctx, "team-knowledge-dedup", workflow.DefaultVersion, 1)
	var ft *fetchTracker
	if teamKnowledgeVersion >= 1 {
		ft = newFetchTracker(input.TeamKnowledge)
	}

	for iteration := 0; iteration < input.MaxIterations; iteration++ {
		// Non-blocking shutdown check: catches signals sent while the agent
		// was blocked in a tool_call activity on the previous iteration.
		// 非阻塞关停检查：捕获上一轮在 tool_call activity 阻塞期间收到的信号。
		shutdownCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("agent:%s:shutdown", input.AgentID))
		shutdownSel := workflow.NewSelector(ctx)
		shouldShutdown := false
		shutdownSel.AddReceive(shutdownCh, func(ch workflow.ReceiveChannel, more bool) {
			var msg string
			ch.Receive(ctx, &msg)
			shouldShutdown = true
		})
		shutdownSel.AddDefault(func() {}) // no signal → continue 注：无信号 → 继续
		shutdownSel.Select(ctx)
		if shouldShutdown {
			logger.Info("Agent shutdown during iteration", "agent_id", input.AgentID, "iteration", iteration)
			shutdownResponse := savedDoneResponse
			if shutdownResponse == "" && stepResult.DecisionSummary != "" {
				shutdownResponse = stepResult.DecisionSummary
			}
			if shutdownResponse == "" {
				shutdownResponse = fmt.Sprintf("Agent %s shutdown after %d iterations", input.AgentID, iteration)
			}
			return AgentLoopResult{
				AgentID:               input.AgentID,
				Role:                  input.Role,
				Response:              shutdownResponse,
				Iterations:            iteration,
				TokensUsed:            totalTokens,
				InputTokens:           totalInput,
				OutputTokens:          totalOutput,
				CacheReadTokens:       totalCacheRead,
				CacheCreationTokens:   totalCacheCreation,
				CacheCreation1hTokens: totalCacheCreation1h,
				ModelUsed:             lastModel,
				Provider:              lastProvider,
				Success:               true,
				TeamKnowledge:         ft.Knowledge(),
			}, nil
		}

		logger.Info("AgentLoop iteration", "agent_id", input.AgentID, "iteration", iteration)

		// Step 1: Check mailbox for incoming messages
		// 步骤 1：检查 mailbox 是否有新消息
		var mailboxMsgs []activities.AgentMailboxMsg
		var fetchedMsgs []activities.AgentMessage
		fetchErr := workflow.ExecuteActivity(p2pCtx, constants.FetchAgentMessagesActivity, activities.FetchAgentMessagesInput{
			WorkflowID: input.WorkflowID,
			AgentID:    input.AgentID,
			SinceSeq:   lastMailboxSeq,
		}).Get(ctx, &fetchedMsgs)
		if fetchErr != nil {
			logger.Warn("AgentLoop mailbox fetch failed", "agent_id", input.AgentID, "error", fetchErr)
		}
		if fetchErr == nil && len(fetchedMsgs) > 0 {
			for _, m := range fetchedMsgs {
				mailboxMsgs = append(mailboxMsgs, activities.AgentMailboxMsg{
					From:    m.From,
					Type:    string(m.Type),
					Payload: m.Payload,
				})
				if m.Seq > lastMailboxSeq {
					lastMailboxSeq = m.Seq
				}
			}
			logger.Info("AgentLoop received mailbox messages",
				"agent_id", input.AgentID,
				"count", len(mailboxMsgs),
			)
		}

		// Step 1b: Fetch shared workspace entries from ALL topics (findings from other agents)
		// 步骤 1b：从所有 topic 拉取共享工作区条目（其他 agent 的发现）
		var wsSnippets []activities.WorkspaceSnippet
		var wsEntries []activities.WorkspaceEntry
		wsErr := workflow.ExecuteActivity(p2pCtx, constants.WorkspaceListAllActivity, activities.WorkspaceListAllInput{
			WorkflowID: input.WorkflowID,
			SinceSeq:   lastWorkspaceSeq,
			MaxEntries: wsMaxEntries,
		}).Get(ctx, &wsEntries)
		if wsErr != nil {
			logger.Warn("AgentLoop workspace fetch failed", "agent_id", input.AgentID, "error", wsErr)
		}
		if wsErr == nil && len(wsEntries) > 0 {
			for _, e := range wsEntries {
				author, _ := e.Entry["author"].(string)
				data, _ := e.Entry["data"].(string)
				// Truncate to control token usage (rune-safe to avoid splitting UTF-8)
				// 截断以控制 token 用量（按 rune 安全截断，避免切断 UTF-8）
				if runeData := []rune(data); len(runeData) > wsSnippetChars {
					data = string(runeData[:wsSnippetChars]) + "..."
				}
				wsSnippets = append(wsSnippets, activities.WorkspaceSnippet{
					Author: author,
					Data:   data,
					Seq:    e.Seq,
				})
				if e.Seq > lastWorkspaceSeq {
					lastWorkspaceSeq = e.Seq
				}
			}
			logger.Info("AgentLoop fetched workspace entries",
				"agent_id", input.AgentID,
				"count", len(wsSnippets),
			)
		}

		// Step 1c: Fetch TaskList for prompt injection
		// 步骤 1c：拉取 TaskList 用于注入 prompt
		var currentTaskList []activities.SwarmTask
		_ = workflow.ExecuteActivity(p2pCtx, constants.GetTaskListActivity, activities.GetTaskListInput{
			WorkflowID: input.WorkflowID,
		}).Get(ctx, &currentTaskList)

		// Step 2: Call LLM to decide next action
		// 步骤 2：调用 LLM 决定下一步动作
		stepResult = activities.AgentLoopStepResult{} // Reset for this iteration 注：本轮重置
		if err := workflow.ExecuteActivity(ctx, constants.AgentLoopStepActivity, activities.AgentLoopStepInput{
			AgentID:             input.AgentID,
			WorkflowID:          input.WorkflowID,
			Task:                input.Task,
			Iteration:           iteration,
			MaxIterations:       input.MaxIterations,
			Messages:            mailboxMsgs,
			History:             history,
			Context:             input.Context,
			SessionID:           input.SessionID,
			TeamRoster:          teamRoster,
			WorkspaceData:       wsSnippets,
			SuggestedTools:      []string{"web_search", "web_fetch", "web_subpage_fetch", "web_crawl", "python_executor", "calculator", "file_read", "file_write", "file_edit", "file_delete", "file_list", "file_search", "diff_files", "json_query"},
			RoleDescription:     input.Task,
			Role:                input.Role,
			TaskList:            currentTaskList,
			ModelTier:           modelTier,
			PreviousResponseID:  lastResponseID,
			SystemMessage:       pendingSystemMessage,
			RunningNotes:        runningNotes,
			IsSwarm:             true,
			CumulativeToolCalls: cumulativeToolCalls,
			TeamKnowledge:       ft.Knowledge(),
			OriginalQuery:       input.OriginalQuery,
		}).Get(ctx, &stepResult); err != nil {
			logger.Error("AgentLoop LLM step failed", "agent_id", input.AgentID, "error", err)
			// Build fallback response from accumulated state
			// 用已累积的状态构建兜底 response
			fallbackResponse := savedDoneResponse
			if fallbackResponse == "" && len(history) > 0 {
				for _, h := range slices.Backward(history) {
					if h.DecisionSummary != "" {
						fallbackResponse = h.DecisionSummary
						break
					}
				}
			}
			return AgentLoopResult{
				AgentID:               input.AgentID,
				Role:                  input.Role,
				Response:              fallbackResponse,
				Success:               false,
				Error:                 fmt.Sprintf("LLM step failed at iteration %d: %v", iteration, err),
				Iterations:            iteration,
				TokensUsed:            totalTokens,
				InputTokens:           totalInput,
				OutputTokens:          totalOutput,
				CacheReadTokens:       totalCacheRead,
				CacheCreationTokens:   totalCacheCreation,
				CacheCreation1hTokens: totalCacheCreation1h,
				ModelUsed:             lastModel,
				Provider:              lastProvider,
				TeamKnowledge:         ft.Knowledge(),
			}, nil
		}

		pendingSystemMessage = "" // Clear after consumption 注：消费后清空

		// Track token usage
		// 记录 token 用量
		totalTokens += stepResult.TokensUsed
		totalInput += stepResult.InputTokens
		totalOutput += stepResult.OutputTokens
		totalCacheRead += stepResult.CacheReadTokens
		totalCacheCreation += stepResult.CacheCreationTokens
		if stepResult.ModelUsed != "" {
			lastModel = stepResult.ModelUsed
		}
		if stepResult.Provider != "" {
			lastProvider = stepResult.Provider
		}

		// Record per-step LLM token usage (agent thinking)
		// 记录每步 LLM token 用量（agent 思考）
		if stepResult.InputTokens > 0 || stepResult.OutputTokens > 0 {
			recCtx := opts.WithTokenRecordOptions(ctx)
			provider := stepResult.Provider
			if provider == "" {
				provider = detectProviderFromModel(stepResult.ModelUsed)
			}
			_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
				UserID:                input.UserID,
				SessionID:             input.SessionID,
				TaskID:                input.WorkflowID,
				AgentID:               input.AgentID,
				Model:                 stepResult.ModelUsed,
				Provider:              provider,
				InputTokens:           stepResult.InputTokens,
				OutputTokens:          stepResult.OutputTokens,
				CacheReadTokens:       stepResult.CacheReadTokens,
				CacheCreationTokens:   stepResult.CacheCreationTokens,
				CacheCreation1hTokens: 0,         // AgentLoopStepResult doesn't carry per-TTL breakdown 注：AgentLoopStepResult 不携带按 TTL 的拆分
				CallSequence:          iteration, // Use Go iteration (per-agent), not Python global counter 注：用 Go 侧迭代计数（按 agent），而非 Python 全局计数
				Metadata:              map[string]any{"workflow": "swarm", "phase": "agent_step", "iteration": iteration},
			}).Get(ctx, nil)
		}
		if stepResult.ResponseID != "" {
			lastResponseID = stepResult.ResponseID
		}
		if stepResult.Notes != "" {
			runningNotes = stepResult.Notes
		}

		// Post-LLM shutdown check: catches signals that arrived during the
		// AgentLoopStep activity (which can block for 30-90s on LLM calls).
		// Only intercept if the LLM chose a non-tool action (idle/done).
		// If the LLM returned a tool_call, let the tool execute first —
		// shutting down mid-tool loses work (e.g., file_write never executes).
		// LLM 调用后的关停检查：AgentLoopStep 可能因 LLM 调用阻塞 30-90 秒，期间到达的信号在此处理。
		// 仅当 LLM 选择非 tool 动作（idle/done）时才拦截；若返回 tool_call，先让 tool 执行完，
		// 中途关停会丢失工作（例如 file_write 根本没落盘）。
		if stepResult.Action == "idle" || stepResult.Action == "done" {
			postLLMShutdownCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("agent:%s:shutdown", input.AgentID))
			postLLMSel := workflow.NewSelector(ctx)
			postLLMShutdown := false
			postLLMSel.AddReceive(postLLMShutdownCh, func(ch workflow.ReceiveChannel, more bool) {
				var msg string
				ch.Receive(ctx, &msg)
				postLLMShutdown = true
			})
			postLLMSel.AddDefault(func() {})
			postLLMSel.Select(ctx)
			if postLLMShutdown {
				logger.Info("Agent shutdown after LLM step", "agent_id", input.AgentID, "iteration", iteration)
				shutdownResponse := savedDoneResponse
				if shutdownResponse == "" && stepResult.DecisionSummary != "" {
					shutdownResponse = stepResult.DecisionSummary
				}
				if shutdownResponse == "" && stepResult.Response != "" {
					shutdownResponse = stepResult.Response
				}
				if shutdownResponse == "" {
					shutdownResponse = fmt.Sprintf("Agent %s shutdown after %d iterations", input.AgentID, iteration)
				}
				return AgentLoopResult{
					AgentID:               input.AgentID,
					Role:                  input.Role,
					Response:              shutdownResponse,
					Iterations:            iteration + 1,
					TokensUsed:            totalTokens,
					InputTokens:           totalInput,
					OutputTokens:          totalOutput,
					CacheReadTokens:       totalCacheRead,
					CacheCreationTokens:   totalCacheCreation,
					CacheCreation1hTokens: totalCacheCreation1h,
					ModelUsed:             lastModel,
					Provider:              lastProvider,
					Success:               true,
					TeamKnowledge:         ft.Knowledge(),
				}, nil
			}
		}

		// Force done on last iteration if LLM didn't choose it
		// 最后一轮若 LLM 未选择 done 则强制 done
		if iteration == input.MaxIterations-1 && stepResult.Action != "done" {
			logger.Warn("Forcing done on last iteration",
				"agent_id", input.AgentID,
				"original_action", stepResult.Action,
			)
			// Build summary from last 3 iterations only (avoid blowing synthesis context)
			// 只用最近 3 轮构建摘要（避免撑爆合成 context）
			var histSummary string
			startIdx := max(len(history)-3, 0)
			for _, h := range history[startIdx:] {
				s := fmt.Sprintf("%v", h.Result)
				if s == "" || s == "<nil>" {
					continue
				}
				if len(s) > 2000 {
					s = s[:2000] + "..."
				}
				histSummary += fmt.Sprintf("[Iteration %d - %s]: %s\n", h.Iteration, h.Action, s)
			}
			if histSummary == "" {
				histSummary = fmt.Sprintf("Agent %s reached iteration limit. Last action was: %s", input.AgentID, stepResult.Action)
			} else {
				histSummary = fmt.Sprintf("Agent %s reached iteration limit (%d iterations). Partial findings from last 3 iterations:\n%s",
					input.AgentID, input.MaxIterations, histSummary)
			}
			stepResult.Action = "done"
			stepResult.Response = histSummary
		}

		// Enforce Lead-controlled lifecycle: in swarm mode (has team roster),
		// agents cannot self-exit with "done". Convert "done" → "idle".
		// Non-swarm agents (no team roster) can exit normally.
		// 强制 Lead 掌控生命周期：swarm mode（有 team roster）下 agent 不能用 "done" 自行退出，
		// "done" 会被改写为 "idle"；无 roster 的普通 agent 可正常退出。
		if stepResult.Action == "done" && len(input.TeamRoster) > 0 {
			logger.Info("Agent tried done — converting to idle (Lead controls lifecycle)",
				"agent_id", input.AgentID,
				"iteration", iteration,
			)

			// Preserve the agent's response so it's not lost when idle times out.
			// Guard: LLMs sometimes stuff a raw tool_call JSON into the response
			// field (e.g. the entire file_write action). Detect and fall back to
			// DecisionSummary which is always a clean human-readable summary.
			// 保留 agent 的 response 以免 idle timeout 后丢失；但 LLM 有时会把裸 tool_call JSON
			// （例如整个 file_write 动作）塞进 response 字段，检测到就退回 DecisionSummary——
			// 它始终是干净的可读摘要。
			if stepResult.Response != "" {
				resp := strings.TrimSpace(stepResult.Response)
				if strings.HasPrefix(resp, "{") && strings.Contains(resp, `"action"`) && strings.Contains(resp, `"tool"`) {
					if stepResult.DecisionSummary != "" {
						savedDoneResponse = stepResult.DecisionSummary
					}
				} else {
					savedDoneResponse = stepResult.Response
				}
			}
			history = append(history, activities.AgentLoopTurn{
				Iteration:       iteration,
				Action:          "done_converted_to_idle",
				Result:          "Lead controls your lifecycle. Going idle — Lead will assign new work or shut you down.",
				DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
			})
			stepResult.Action = "idle"
		}

		// Step 3: Execute the action
		// 步骤 3：执行动作
		switch stepResult.Action {
		case "done":
			// This case is now only reachable internally (e.g., max iterations forced done).
			// 该分支现在只能在内部触达（例如达到最大轮数被强制 done）。
			// Normal agent "done" is converted to "idle" above.
			// 普通 agent 的 "done" 已在上面被改写为 "idle"。
			logger.Info("AgentLoop completed (internal)", "agent_id", input.AgentID, "iterations", iteration+1)

			// Emit agent completed event
			// 发出 agent 完成事件
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: input.WorkflowID,
				EventType:  activities.StreamEventAgentCompleted,
				AgentID:    input.AgentID,
				Message:    activities.MsgAgentCompleted(input.AgentID),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)

			return AgentLoopResult{
				AgentID:               input.AgentID,
				Role:                  input.Role,
				Response:              stepResult.Response,
				Iterations:            iteration + 1,
				TokensUsed:            totalTokens,
				InputTokens:           totalInput,
				OutputTokens:          totalOutput,
				CacheReadTokens:       totalCacheRead,
				CacheCreationTokens:   totalCacheCreation,
				CacheCreation1hTokens: totalCacheCreation1h,
				ModelUsed:             lastModel,
				Provider:              lastProvider,
				Success:               true,
				TeamKnowledge:         ft.Knowledge(),
			}, nil

		case "tool_call":
			consecutiveNonToolActions = 0 // Tool use = progress 注：使用 tool 即视为有进展
			// Reset message spam counter on file_write/file_edit (agent is producing output)
			// file_write/file_edit 时重置消息刷屏计数（agent 正在产出）
			if stepResult.Tool == "file_write" || stepResult.Tool == "file_edit" {
				consecutiveMsgWithoutWrite = 0
			}

			// Auto-inject extract_prompt for web_fetch/web_crawl so extraction
			// LLM focuses on data the agent actually needs (not generic extraction).
			// 为 web_fetch/web_crawl 自动注入 extract_prompt，让抽取用的 LLM
			// 聚焦 agent 真正需要的数据，而不是做通用抽取。
			if (stepResult.Tool == "web_fetch" || stepResult.Tool == "web_crawl") && stepResult.ToolParams != nil {
				if _, hasPrompt := stepResult.ToolParams["extract_prompt"]; !hasPrompt {
					taskSnippet := input.Task
					if len(taskSnippet) > 200 {
						taskSnippet = taskSnippet[:200]
					}
					stepResult.ToolParams["extract_prompt"] = fmt.Sprintf("Extract data relevant to: %s", taskSnippet)
				}
			}

			// L1 fetch cache check
			// L1 抓取 cache 检查
			turnResult := any(nil)
			if cached, hit := ft.checkCache(stepResult.Tool, stepResult.ToolParams); hit {
				logger.Info("L1 fetch cache hit — skipping ExecuteAgent",
					"agent_id", input.AgentID,
					"tool", stepResult.Tool,
					"urls", len(extractURLsFromParams(stepResult.ToolParams)),
				)
				turnResult = cached
				consecutiveToolErrors = 0
				consecutiveTransientErrors = 0
				cumulativeToolCalls++
			}

			if turnResult == nil {
				// Execute tool via standard agent execution (one-shot)
				// 通过标准 agent 执行一次性运行 tool
				var toolRes activities.AgentExecutionResult
				toolErr := workflow.ExecuteActivity(ctx, activities.ExecuteAgent, activities.AgentExecutionInput{
					Query:   fmt.Sprintf("Execute tool %s with params: %v", stepResult.Tool, stepResult.ToolParams),
					AgentID: input.AgentID,
					Context: func() map[string]any {
						c := map[string]any{"force_swarm": true}
						if toolModelOverride != "" {
							c["model_override"] = toolModelOverride
						}
						if toolProviderOverride != "" {
							c["provider_override"] = toolProviderOverride
						}
						return c
					}(),
					SuggestedTools:   []string{stepResult.Tool},
					ToolParameters:   stepResult.ToolParams,
					SessionID:        input.SessionID,
					UserID:           input.UserID,
					ParentWorkflowID: input.WorkflowID,
				}).Get(ctx, &toolRes)

				if toolErr == nil {
					turnResult = toolRes.Response
					consecutiveToolErrors = 0
					consecutiveTransientErrors = 0
					cumulativeToolCalls++ // Survives history truncation and reassignment 注：跨历史截断与重新派活保留

					// Record tool execution LLM tokens (typically Haiku)
					// 记录 tool 执行的 LLM token（通常是 Haiku）
					if toolRes.InputTokens > 0 || toolRes.OutputTokens > 0 {
						recCtx := opts.WithTokenRecordOptions(ctx)
						provider := toolRes.Provider
						if provider == "" {
							provider = detectProviderFromModel(toolRes.ModelUsed)
						}
						_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
							UserID:                input.UserID,
							SessionID:             input.SessionID,
							TaskID:                input.WorkflowID,
							AgentID:               input.AgentID,
							Model:                 toolRes.ModelUsed,
							Provider:              provider,
							InputTokens:           toolRes.InputTokens,
							OutputTokens:          toolRes.OutputTokens,
							CacheReadTokens:       toolRes.CacheReadTokens,
							CacheCreationTokens:   toolRes.CacheCreationTokens,
							CacheCreation1hTokens: toolRes.CacheCreation1hTokens,
							CallSequence:          iteration, // Use Go iteration (per-agent) 注：用 Go 侧迭代计数（按 agent）
							Metadata:              map[string]any{"workflow": "swarm", "phase": "tool_exec", "tool": stepResult.Tool},
						}).Get(ctx, nil)
					}

					// Record tool cost entries (e.g., web_search API costs)
					// 记录 tool 成本条目（例如 web_search API 费用）
					opts.RecordToolCostEntries(ctx, toolRes, input.UserID, input.SessionID, input.WorkflowID)

					// Auto-register files written by agents
					// 自动登记 agent 写入的文件
					if stepResult.Tool == "file_write" {
						if path, ok := stepResult.ToolParams["path"].(string); ok && path != "" {
							filesWritten = append(filesWritten, path)
							if p2pV >= 1 {
								_ = workflow.ExecuteActivity(p2pCtx, constants.RegisterFileActivity, activities.RegisterFileInput{
									WorkflowID: input.WorkflowID,
									Path:       path,
									Author:     input.AgentID,
									Size:       len(fmt.Sprintf("%v", stepResult.ToolParams["content"])),
									Summary:    truncateDecisionSummary(stepResult.DecisionSummary),
								}).Get(ctx, nil)
							}

							// Prevent double-write: inject system message reminding agent
							// it already wrote its deliverable. This survives history truncation
							// (unlike notes which may be too short after large file_write).
							// 防止重复写入：注入系统消息提醒 agent 已交付过成果；
							// 该提醒在历史被截断后仍保留（笔记类信息在大文件写入后往往太短而丢失）。
							pendingSystemMessage = fmt.Sprintf(
								"FILE ALREADY WRITTEN: You saved your deliverable to '%s' in the previous iteration. "+
									"Do NOT write another file. Proceed to publish_data and then idle.",
								path,
							)
						}
					}

					// L1 fetch cache write
					// L1 抓取 cache 写入
					if added := ft.writeCache(stepResult.Tool, stepResult.ToolParams, toolRes.Response, input.AgentID); len(added) > 0 {
						for _, u := range added {
							logger.Info("L1 fetch cache write", "agent_id", input.AgentID, "url", u, "chars", len(fmt.Sprintf("%v", toolRes.Response)))
						}
					}

					// Accumulate tool execution tokens into agent totals
					// 把 tool 执行 token 累加到 agent 总计
					totalTokens += toolRes.TokensUsed
					totalInput += toolRes.InputTokens
					totalOutput += toolRes.OutputTokens
					totalCacheRead += toolRes.CacheReadTokens
					totalCacheCreation += toolRes.CacheCreationTokens
					totalCacheCreation1h += toolRes.CacheCreation1hTokens
				} else {
					if isTransientError(toolErr) {
						consecutiveTransientErrors++
						backoff := min(time.Duration(consecutiveTransientErrors)*5*time.Second, 30*time.Second)
						logger.Warn("Transient tool error, backing off",
							"agent_id", input.AgentID,
							"tool", stepResult.Tool,
							"backoff", backoff,
							"attempt", consecutiveTransientErrors,
							"error", toolErr,
						)
						if err := workflow.Sleep(ctx, backoff); err != nil {
							return AgentLoopResult{
								AgentID:               input.AgentID,
								Role:                  input.Role,
								Response:              savedDoneResponse,
								Success:               false,
								Error:                 fmt.Sprintf("cancelled during backoff: %v", err),
								TokensUsed:            totalTokens,
								InputTokens:           totalInput,
								OutputTokens:          totalOutput,
								CacheReadTokens:       totalCacheRead,
								CacheCreationTokens:   totalCacheCreation,
								CacheCreation1hTokens: totalCacheCreation1h,
								ModelUsed:             lastModel,
								Provider:              lastProvider,
								TeamKnowledge:         ft.Knowledge(),
							}, nil
						}
						turnResult = fmt.Sprintf("transient error (will retry): %v", toolErr)
					} else {
						turnResult = fmt.Sprintf("tool error: %v", toolErr)
						consecutiveToolErrors++
					}
				}
			} // end if turnResult == nil (cache miss → execute tool) 注：turnResult == nil 分支结束（cache 未命中 → 执行 tool）
			history = append(history, activities.AgentLoopTurn{
				Iteration:       iteration,
				Action:          fmt.Sprintf("tool_call:%s", stepResult.Tool),
				Result:          turnResult,
				DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
				AssistantReplay: stepResult.AssistantReplay,
				ObservationText: buildObservationText(fmt.Sprintf("tool_call:%s", stepResult.Tool), turnResult),
			})

			// B4: Reset search-without-fetch counter on any fetch/crawl
			// B4：任何 fetch/crawl 都重置"连续搜索未抓取"计数
			if stepResult.Tool == "web_fetch" || stepResult.Tool == "web_subpage_fetch" || stepResult.Tool == "web_crawl" {
				searchesSinceLastFetch = 0
			}

			// B3: Search saturation detection — inject warning if agent repeats similar queries
			// B3：搜索饱和检测——agent 重复相似 query 时注入警告
			if stepResult.Tool == "web_search" {
				if q, ok := stepResult.ToolParams["query"].(string); ok && q != "" {
					recentSearchQueries = append(recentSearchQueries, q)
					// Cap to last 6 queries — saturation only checks window of 3
					// 最多保留最近 6 个 query——饱和检测只看 3 个窗口
					if len(recentSearchQueries) > 6 {
						recentSearchQueries = recentSearchQueries[len(recentSearchQueries)-6:]
					}
					if isSearchSaturated(recentSearchQueries, 3, 0.7) {
						if pendingSystemMessage == "" {
							pendingSystemMessage = "SEARCH SATURATED: You have searched very similar queries 3+ times with diminishing returns. STOP searching and synthesize what you have. Go to PHASE 3 immediately."
						}
						logger.Warn("Search saturation detected",
							"agent_id", input.AgentID,
							"total_searches", len(recentSearchQueries),
							"last_query", q,
						)
					}
				}

				// B4: Consecutive search without fetch — enforce search→fetch cycle
				// B4：连续搜索未抓取——强制 search→fetch 循环
				searchesSinceLastFetch++
				if searchesSinceLastFetch >= 2 {
					if pendingSystemMessage == "" {
						pendingSystemMessage = "MANDATORY: You just searched twice without fetching. Your research cycle REQUIRES: search → FETCH → think. " +
							"Pick 3-8 URLs from your search results and call web_fetch(urls=[...]) NOW. Do NOT search again until you have fetched."
					}
					logger.Warn("Consecutive search without fetch",
						"agent_id", input.AgentID,
						"searches_since_fetch", searchesSinceLastFetch,
					)
				}

				// L3: Search overlap detection
				// L3：搜索重叠检测
				if overlapPct, known, total := ft.checkSearchOverlap(turnResult, input.AgentID); total > 0 && known > 0 {
					logger.Info("L3 search overlap",
						"agent_id", input.AgentID,
						"total_urls", total,
						"known", known,
						"new", total-known,
						"overlap_pct", overlapPct,
					)
					if overlapPct >= 70 && pendingSystemMessage == "" {
						pendingSystemMessage = fmt.Sprintf(
							"SEARCH OVERLAP: %d/%d URLs from your search are already known to the team. "+
								"Focus on NEW angles or UNIQUE URLs that teammates haven't explored. "+
								"Check ## Team Knowledge for data the team already has.",
							known, total,
						)
					}
				}
			}

		case "send_message":
			// Collaborative action — but spam detection: 3+ messages without file_write is non-progress.
			// 协作动作——但需防刷屏：3 条以上消息且无 file_write 视为无进展。
			consecutiveMsgWithoutWrite++
			if consecutiveMsgWithoutWrite >= 3 {
				consecutiveNonToolActions++
				logger.Warn("Agent send_message spam detected: 3+ messages without file_write",
					"agent_id", input.AgentID, "count", consecutiveMsgWithoutWrite)
			} else {
				consecutiveNonToolActions = 0
			}
			// Send message to another agent via P2P mailbox
			// 通过 P2P mailbox 给其他 agent 发消息
			if p2pV >= 1 {
				_ = workflow.ExecuteActivity(p2pCtx, constants.SendAgentMessageActivity, activities.SendAgentMessageInput{
					WorkflowID: input.WorkflowID,
					From:       input.AgentID,
					To:         stepResult.To,
					Type:       activities.MessageType(stepResult.MessageType),
					Payload:    stepResult.Payload,
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
			}

			// Notify parent so it can forward a wake-up signal to idle recipients.
			// 通知父 workflow，以便它向空闲接收方转发唤醒信号。
			// Parent holds agentFutures mapping; we don't know target's child workflow ID.
			// 父 workflow 持有 agentFutures 映射；此处不知道目标的子 workflow ID。
			if parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution; parentExec != nil && stepResult.To != "" {
				// Include message preview so Temporal UI shows content (not just from/to)
				// 附带消息预览，让 Temporal UI 显示内容（不只是 from/to）
				preview := ""
				if msg, ok := stepResult.Payload["message"]; ok {
					preview = fmt.Sprintf("%v", msg)
					if len(preview) > 200 {
						preview = preview[:200] + "..."
					}
				}
				_ = workflow.SignalExternalWorkflow(ctx, parentExec.ID, "",
					"agent-message-sent",
					map[string]string{"from": input.AgentID, "to": stepResult.To, "preview": preview},
				).Get(ctx, nil)
			}

			history = append(history, activities.AgentLoopTurn{
				Iteration:       iteration,
				Action:          fmt.Sprintf("send_message:%s", stepResult.To),
				Result:          "sent",
				DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
				AssistantReplay: stepResult.AssistantReplay,
				ObservationText: buildObservationText(fmt.Sprintf("send_message:%s", stepResult.To), "sent"),
			})

		case "publish_data":
			// Collaborative action — but spam detection: 3+ messages without file_write is non-progress.
			// 协作动作——但需防刷屏：3 条以上消息且无 file_write 视为无进展。
			consecutiveMsgWithoutWrite++
			if consecutiveMsgWithoutWrite >= 3 {
				consecutiveNonToolActions++
				logger.Warn("Agent publish_data spam detected: 3+ without file_write",
					"agent_id", input.AgentID, "count", consecutiveMsgWithoutWrite)
			} else {
				consecutiveNonToolActions = 0
			}
			// Publish to workspace topic
			// 发布到工作区 topic
			if p2pV >= 1 {
				_ = workflow.ExecuteActivity(p2pCtx, constants.WorkspaceAppendActivity, activities.WorkspaceAppendInput{
					WorkflowID: input.WorkflowID,
					Topic:      stepResult.Topic,
					Entry: map[string]any{
						"author": input.AgentID,
						"data":   stepResult.Data,
					},
					Timestamp: workflow.Now(ctx),
				}).Get(ctx, nil)
			}

			history = append(history, activities.AgentLoopTurn{
				Iteration:       iteration,
				Action:          fmt.Sprintf("publish_data:%s", stepResult.Topic),
				Result:          "published",
				DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
				AssistantReplay: stepResult.AssistantReplay,
				ObservationText: buildObservationText(fmt.Sprintf("publish_data:%s", stepResult.Topic), "published"),
			})

		case "idle":
			// Agent goes idle — notify parent and wait for new task (Temporal Signal)
			// agent 转入空闲——通知父 workflow 并等待新 task（Temporal Signal）
			consecutiveNonToolActions = 0
			consecutiveMsgWithoutWrite = 0
			logger.Info("Agent going idle", "agent_id", input.AgentID)

			// Build result summary for Lead: prefer savedDoneResponse (from done→idle),
			// 为 Lead 构建结果摘要：优先 savedDoneResponse（来自 done→idle），
			// then stepResult.Response (from idle with response), then DecisionSummary as fallback.
			// 其次 stepResult.Response（idle 且带 response），最后回退 DecisionSummary。
			idleSummary := savedDoneResponse
			if idleSummary == "" && stepResult.Response != "" {
				idleSummary = stepResult.Response
			}
			if idleSummary == "" && stepResult.DecisionSummary != "" {
				idleSummary = stepResult.DecisionSummary
			}

			history = append(history, activities.AgentLoopTurn{
				Iteration:       iteration,
				Action:          "idle",
				Result:          "waiting for new task",
				DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
				AssistantReplay: stepResult.AssistantReplay,
				ObservationText: buildObservationText("idle", "waiting for new task"),
			})

			// Notify Lead that this agent is idle and available for reassignment.
			// 通知 Lead：该 agent 已空闲，可重新派活。
			// Signal the parent SwarmWorkflow so Lead wakes up immediately.
			// 向父 SwarmWorkflow 发信号，让 Lead 立即唤醒。
			// Carry result_summary so Lead can evaluate what the agent produced.
			// 携带 result_summary，供 Lead 评估该 agent 的产出。
			var parentWfID string
			if parentExec := workflow.GetInfo(ctx).ParentWorkflowExecution; parentExec != nil {
				parentWfID = parentExec.ID
			}
			if parentWfID != "" {
				// Build compact tools-used summary so Lead can inject it on reassignment
				// 构建紧凑的已用 tool 摘要，供 Lead 在重新派活时注入
				toolsUsed := buildToolsUsedSummary(history)

				// Build enriched completion report for Lead
				// 为 Lead 构建更丰富的完成报告
				completionReport := stepResult.CompletionReport
				if completionReport == nil {
					completionReport = map[string]any{}
				}
				if len(filesWritten) > 0 {
					completionReport["files_written"] = filesWritten
				}
				if _, ok := completionReport["summary"]; !ok {
					completionReport["summary"] = idleSummary
				}

				idlePayload := map[string]any{
					"agent_id":          input.AgentID,
					"result_summary":    idleSummary,
					"iterations":        iteration + 1,
					"tools_used":        toolsUsed,
					"completion_report": completionReport,
					"team_knowledge":    ft.Knowledge(),
				}
				_ = workflow.SignalExternalWorkflow(ctx, parentWfID, "", "agent-idle", idlePayload).Get(ctx, nil)
				logger.Info("Agent signaled parent: idle", "agent_id", input.AgentID, "parent", parentWfID)
			}

			// Wait for new task, peer message, or shutdown via Temporal Signals (zero-cost idle)
			// 通过 Temporal Signal 等待新 task、同伴消息或关停（零成本空闲）
			taskCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("agent:%s:new-task", input.AgentID))
			shutdownCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("agent:%s:shutdown", input.AgentID))
			peerMsgCh := workflow.GetSignalChannel(ctx, fmt.Sprintf("agent:%s:peer-message", input.AgentID))

			idleSel := workflow.NewSelector(ctx)
			var newTaskDesc string
			var newModelTier string
			gotShutdown := false
			gotPeerMessage := false

			idleSel.AddReceive(taskCh, func(ch workflow.ReceiveChannel, more bool) {
				var assignment map[string]string
				ch.Receive(ctx, &assignment)
				newTaskDesc = assignment["description"]
				newModelTier = assignment["model_tier"]
				logger.Info("Agent received new task", "agent_id", input.AgentID, "task", newTaskDesc, "model_tier", newModelTier)
			})
			idleSel.AddReceive(shutdownCh, func(ch workflow.ReceiveChannel, more bool) {
				var msg string
				ch.Receive(ctx, &msg)
				gotShutdown = true
				logger.Info("Agent received shutdown", "agent_id", input.AgentID)
			})
			// P2P wake-up: peer message signal from parent (forwarded from another agent).
			// P2P 唤醒：来自父 workflow 的同伴消息信号（由其他 agent 转发）。
			// Agent resumes its current task — mailbox is polled at next iteration start.
			// agent 继续当前 task——下一轮开始时才会轮询 mailbox。
			idleSel.AddReceive(peerMsgCh, func(ch workflow.ReceiveChannel, more bool) {
				var info map[string]string
				ch.Receive(ctx, &info)
				gotPeerMessage = true
				logger.Info("Agent woken by peer message", "agent_id", input.AgentID, "from", info["from"])
			})
			// Timeout: if idle for 10 minutes, auto-exit (increased from 5min)
			// 超时：空闲 10 分钟自动退出（由 5 分钟上调）
			idleSel.AddFuture(workflow.NewTimer(ctx, 10*time.Minute), func(f workflow.Future) {
				_ = f.Get(ctx, nil)
				gotShutdown = true
				logger.Info("Agent idle timeout", "agent_id", input.AgentID)
			})
			idleSel.Select(ctx)

			if gotShutdown {
				// Reuse idleSummary (computed above with fallback chain:
				// 复用 idleSummary（上面按回退链计算：
				// savedDoneResponse → stepResult.Response → DecisionSummary)
				// savedDoneResponse → stepResult.Response → DecisionSummary）
				idleResponse := idleSummary
				if idleResponse == "" {
					idleResponse = "Agent shutdown after idle"
				}
				return AgentLoopResult{
					AgentID:               input.AgentID,
					Role:                  input.Role,
					Response:              idleResponse,
					Iterations:            iteration + 1,
					TokensUsed:            totalTokens,
					InputTokens:           totalInput,
					OutputTokens:          totalOutput,
					CacheReadTokens:       totalCacheRead,
					CacheCreationTokens:   totalCacheCreation,
					CacheCreation1hTokens: totalCacheCreation1h,
					ModelUsed:             lastModel,
					Provider:              lastProvider,
					Success:               true,
					TeamKnowledge:         ft.Knowledge(),
				}, nil
			}

			if gotPeerMessage {
				// Woken by peer message — resume current task with unchanged state.
				// 被同伴消息唤醒——保持状态不变继续当前 task。
				// Next iteration will poll mailbox and see the incoming message in ## Inbox Messages.
				// 下一轮会轮询 mailbox，在 ## Inbox Messages 中看到该消息。
				// No task/history/iteration changes needed; budget constraints still apply.
				// 无需改动 task/历史/轮数；budget 约束仍然生效。
				continue
			}

			// Got new task — keep recent history so agent remembers what it searched/read.
			// 领到新 task——保留近期历史，让 agent 记得搜索/读取过什么。
			// Python-side truncation (agent.py:3042) already limits older turns to 500 chars,
			// Python 侧截断（agent.py:3042）已把更早的轮次限制到 500 字符，
			// so retaining 5 turns adds ~2-4K tokens — far less than repeating tool calls.
			// 因此保留 5 轮只增加约 2-4K token——远少于重复 tool 调用的开销。
			input.Task = newTaskDesc
			// Update model tier if Lead specified one for this reassignment
			// 若 Lead 为本次重新派活指定了 model tier 则更新
			if newModelTier != "" {
				modelTier = newModelTier
			}
			const keepLastNOnReassign = 5
			if len(history) > keepLastNOnReassign {
				history = history[len(history)-keepLastNOnReassign:]
			}
			filesWritten = nil // Reset files written for new task 注：新 task 开始时清空已写文件

			// B1: Per-agent reassignment limit to prevent runaway iteration expansion.
			// Each agent can be reassigned at most 2 times (3 total tasks including initial).
			// 每个 agent 最多被重新派活 2 次（含初始 task 共 3 个），防止迭代规模失控。
			const maxReassignments = 2
			const iterationsPerReassign = 15
			const maxTotalIterations = 80 // Lowered from 150 注：由 150 下调

			input.ReassignCount++
			if input.ReassignCount > maxReassignments {
				logger.Warn("Agent reached max reassignments, rejecting new task",
					"agent_id", input.AgentID,
					"reassign_count", input.ReassignCount,
					"max", maxReassignments,
				)
				// Don't accept the new task — agent stays idle and will be shut down
				// 不接受新 task——agent 保持空闲并会被关停
				savedDoneResponse = ""
				continue
			}

			newMax := min(input.MaxIterations+iterationsPerReassign, maxTotalIterations)
			if newMax > input.MaxIterations {
				logger.Info("Extending iteration budget on reassignment",
					"agent_id", input.AgentID,
					"old_max", input.MaxIterations,
					"new_max", newMax,
					"current_iteration", iteration,
				)
				input.MaxIterations = newMax
			}

			savedDoneResponse = "" // Clear stale response so shutdown returns fresh result 注：清空过期 response，让关停返回最新结果
			continue

		default:
			// Treat unrecognized actions (file_write, file_read, web_search, etc.)
			// 把未识别的动作（file_write、file_read、web_search 等）
			// as tool_call where the action name is the tool name. LLMs sometimes
			// 当作 tool_call 处理，动作名即 tool 名。LLM 有时
			// return the tool name directly instead of wrapping in tool_call.
			// 直接返回 tool 名而不包在 tool_call 里。
			toolName := stepResult.Action
			if toolName == "" {
				consecutiveNonToolActions++
				logger.Warn("Empty action from LLM", "agent_id", input.AgentID)
				history = append(history, activities.AgentLoopTurn{
					Iteration:       iteration,
					Action:          "unknown:empty",
					Result:          "skipped",
					DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
				})
			} else {
				consecutiveNonToolActions = 0 // Implicit tool use = progress 注：隐式使用 tool 即视为有进展
				logger.Info("Treating action as tool_call", "action", toolName, "agent_id", input.AgentID)
				params := stepResult.ToolParams
				if len(params) == 0 {
					// Some actions put params in payload or data fields
					// 有些动作把参数放在 payload 或 data 字段
					params = make(map[string]any)
					if stepResult.Data != "" {
						params["content"] = stepResult.Data
					}
					if stepResult.Topic != "" {
						params["path"] = stepResult.Topic
					}
				}
				// L1 fetch cache check (default case)
				// L1 抓取 cache 检查（default 分支）
				turnResult := any(nil)
				if cached, hit := ft.checkCache(toolName, params); hit {
					logger.Info("L1 fetch cache hit — skipping ExecuteAgent",
						"agent_id", input.AgentID,
						"tool", toolName,
						"urls", len(extractURLsFromParams(params)),
					)
					turnResult = cached
					consecutiveToolErrors = 0
					consecutiveTransientErrors = 0
					cumulativeToolCalls++
				}

				if turnResult == nil {
					var toolRes activities.AgentExecutionResult
					toolErr := workflow.ExecuteActivity(ctx, activities.ExecuteAgent, activities.AgentExecutionInput{
						Query:   fmt.Sprintf("Execute tool %s with params: %v", toolName, params),
						AgentID: input.AgentID,
						Context: func() map[string]any {
							c := map[string]any{"force_swarm": true}
							if toolModelOverride != "" {
								c["model_override"] = toolModelOverride
							}
							if toolProviderOverride != "" {
								c["provider_override"] = toolProviderOverride
							}
							return c
						}(),
						SuggestedTools:   []string{toolName},
						ToolParameters:   params,
						SessionID:        input.SessionID,
						UserID:           input.UserID,
						ParentWorkflowID: input.WorkflowID,
					}).Get(ctx, &toolRes)

					if toolErr == nil {
						turnResult = toolRes.Response
						consecutiveToolErrors = 0
						consecutiveTransientErrors = 0
						cumulativeToolCalls++ // Survives history truncation and reassignment 注：跨历史截断与重新派活保留

						// Record tool execution LLM tokens (typically Haiku)
						// 记录 tool 执行的 LLM token（通常是 Haiku）
						if toolRes.InputTokens > 0 || toolRes.OutputTokens > 0 {
							recCtx := opts.WithTokenRecordOptions(ctx)
							provider := toolRes.Provider
							if provider == "" {
								provider = detectProviderFromModel(toolRes.ModelUsed)
							}
							_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
								UserID:                input.UserID,
								SessionID:             input.SessionID,
								TaskID:                input.WorkflowID,
								AgentID:               input.AgentID,
								Model:                 toolRes.ModelUsed,
								Provider:              provider,
								InputTokens:           toolRes.InputTokens,
								OutputTokens:          toolRes.OutputTokens,
								CacheReadTokens:       toolRes.CacheReadTokens,
								CacheCreationTokens:   toolRes.CacheCreationTokens,
								CacheCreation1hTokens: toolRes.CacheCreation1hTokens,
								CallSequence:          iteration, // Use Go iteration (per-agent) 注：用 Go 侧迭代计数（按 agent）
								Metadata:              map[string]any{"workflow": "swarm", "phase": "tool_exec", "tool": toolName},
							}).Get(ctx, nil)
						}

						// Record tool cost entries (e.g., web_search API costs)
						// 记录 tool 成本条目（例如 web_search API 费用）
						opts.RecordToolCostEntries(ctx, toolRes, input.UserID, input.SessionID, input.WorkflowID)

						// Auto-register files written by agents
						// 自动登记 agent 写入的文件
						if toolName == "file_write" {
							if path, ok := params["path"].(string); ok && path != "" {
								if p2pV >= 1 {
									_ = workflow.ExecuteActivity(p2pCtx, constants.RegisterFileActivity, activities.RegisterFileInput{
										WorkflowID: input.WorkflowID,
										Path:       path,
										Author:     input.AgentID,
										Size:       len(fmt.Sprintf("%v", params["content"])),
										Summary:    truncateDecisionSummary(stepResult.DecisionSummary),
									}).Get(ctx, nil)
								}

								// Track for completion report (same as tool_call case)
								// 记入完成报告（与 tool_call 分支相同）
								filesWritten = append(filesWritten, path)
								pendingSystemMessage = fmt.Sprintf(
									"SUCCESS: File written to '%s'. Do NOT write to this file again. "+
										"If you have more findings, go idle with key_findings.", path)
							}
						}

						// L1 fetch cache write
						// L1 抓取 cache 写入
						if added := ft.writeCache(toolName, params, toolRes.Response, input.AgentID); len(added) > 0 {
							for _, u := range added {
								logger.Info("L1 fetch cache write", "agent_id", input.AgentID, "url", u, "chars", len(fmt.Sprintf("%v", toolRes.Response)))
							}
						}

						// Accumulate tool execution tokens into agent totals
						// 把 tool 执行 token 累加到 agent 总计
						totalTokens += toolRes.TokensUsed
						totalInput += toolRes.InputTokens
						totalOutput += toolRes.OutputTokens
						totalCacheRead += toolRes.CacheReadTokens
						totalCacheCreation += toolRes.CacheCreationTokens
						totalCacheCreation1h += toolRes.CacheCreation1hTokens
					} else {
						if isTransientError(toolErr) {
							consecutiveTransientErrors++
							backoff := min(time.Duration(consecutiveTransientErrors)*5*time.Second, 30*time.Second)
							logger.Warn("Transient tool error, backing off",
								"agent_id", input.AgentID,
								"tool", toolName,
								"backoff", backoff,
								"attempt", consecutiveTransientErrors,
								"error", toolErr,
							)
							if err := workflow.Sleep(ctx, backoff); err != nil {
								return AgentLoopResult{
									AgentID:               input.AgentID,
									Role:                  input.Role,
									Response:              savedDoneResponse,
									Success:               false,
									Error:                 fmt.Sprintf("cancelled during backoff: %v", err),
									TokensUsed:            totalTokens,
									InputTokens:           totalInput,
									OutputTokens:          totalOutput,
									CacheReadTokens:       totalCacheRead,
									CacheCreationTokens:   totalCacheCreation,
									CacheCreation1hTokens: totalCacheCreation1h,
									ModelUsed:             lastModel,
									Provider:              lastProvider,
									TeamKnowledge:         ft.Knowledge(),
								}, nil
							}
							turnResult = fmt.Sprintf("transient error (will retry): %v", toolErr)
						} else {
							turnResult = fmt.Sprintf("tool error: %v", toolErr)
							consecutiveToolErrors++
							logger.Warn("Tool execution failed",
								"agent_id", input.AgentID,
								"tool", toolName,
								"consecutive_errors", consecutiveToolErrors,
								"error", toolErr,
							)
						}
					}
				} // end if !defaultCacheHit 注：!defaultCacheHit 分支结束
				history = append(history, activities.AgentLoopTurn{
					Iteration:       iteration,
					Action:          fmt.Sprintf("tool_call:%s", toolName),
					Result:          turnResult,
					DecisionSummary: truncateDecisionSummary(stepResult.DecisionSummary),
					AssistantReplay: stepResult.AssistantReplay,
					ObservationText: buildObservationText(fmt.Sprintf("tool_call:%s", toolName), turnResult),
				})

				// Bail out if agent is stuck in a loop of failing tool calls (permanent errors only)
				// agent 陷入失败 tool 调用死循环时退出（仅统计永久错误）
				if consecutiveToolErrors >= 3 {
					logger.Warn("AgentLoop aborting: 3 consecutive permanent tool errors",
						"agent_id", input.AgentID,
						"last_tool", toolName,
					)
					return AgentLoopResult{
						AgentID:               input.AgentID,
						Role:                  input.Role,
						Response:              fmt.Sprintf("Agent stopped after %d consecutive tool failures. Last attempted: %s", consecutiveToolErrors, toolName),
						Iterations:            iteration + 1,
						TokensUsed:            totalTokens,
						InputTokens:           totalInput,
						OutputTokens:          totalOutput,
						CacheReadTokens:       totalCacheRead,
						CacheCreationTokens:   totalCacheCreation,
						CacheCreation1hTokens: totalCacheCreation1h,
						ModelUsed:             lastModel,
						Provider:              lastProvider,
						Success:               false,
						Error:                 "consecutive tool errors",
						TeamKnowledge:         ft.Knowledge(),
					}, nil
				}
			}
		}

		// History truncation: sliding window to prevent unbounded growth.
		// Keep first 2 turns (initial context) + last 13 (recent context) = max 15.
		// 历史滑动窗口：保留最初 2 轮（初始 context）+ 最近 13 轮，最多 15 轮。
		const maxHistoryTurns = 15
		if len(history) > maxHistoryTurns {
			history = append(history[:2], history[len(history)-(maxHistoryTurns-2):]...)
		}

		// Convergence detection: if agent hasn't used tools for 3 consecutive iterations,
		// it's likely stuck in a reasoning loop without making progress (Claude Code pattern)
		// 连续 3 轮未使用 tool，说明 agent 很可能陷入无进展的推理循环（Claude Code 的做法）。
		if consecutiveNonToolActions >= 3 {
			logger.Warn("AgentLoop converged: no tool use for 3 consecutive iterations",
				"agent_id", input.AgentID,
				"iteration", iteration,
			)
			// Build partial findings from history
			// 从历史构建部分结果
			var summary string
			startIdx := max(len(history)-3, 0)
			for _, h := range history[startIdx:] {
				s := fmt.Sprintf("%v", h.Result)
				if s == "" || s == "<nil>" {
					continue
				}
				if len(s) > 2000 {
					s = s[:2000] + "..."
				}
				summary += fmt.Sprintf("[%s]: %s\n", h.Action, s)
			}
			if summary == "" {
				summary = fmt.Sprintf("Agent %s converged after %d iterations with no further tool use.", input.AgentID, iteration+1)
			}
			return AgentLoopResult{
				AgentID:               input.AgentID,
				Role:                  input.Role,
				Response:              summary,
				Iterations:            iteration + 1,
				TokensUsed:            totalTokens,
				InputTokens:           totalInput,
				OutputTokens:          totalOutput,
				CacheReadTokens:       totalCacheRead,
				CacheCreationTokens:   totalCacheCreation,
				CacheCreation1hTokens: totalCacheCreation1h,
				ModelUsed:             lastModel,
				Provider:              lastProvider,
				Success:               true,
				TeamKnowledge:         ft.Knowledge(),
			}, nil
		}

		// Emit progress event
		// 发出进度事件
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: input.WorkflowID,
			EventType:  activities.StreamEventProgress,
			AgentID:    input.AgentID,
			Message:    activities.MsgAgentProgress(input.AgentID, iteration+1, input.MaxIterations, stepResult.Action),
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
	}

	// Max iterations reached — return partial success so synthesis can use whatever was collected
	// 达到最大轮数——返回部分成功，让合成使用已收集到的内容
	logger.Warn("AgentLoop max iterations reached", "agent_id", input.AgentID)
	var partialResponse string
	for _, h := range history {
		if s, ok := h.Result.(string); ok && s != "" {
			partialResponse += s + "\n"
		}
	}
	if partialResponse == "" {
		partialResponse = "Max iterations reached without completing task"
	}
	return AgentLoopResult{
		AgentID:               input.AgentID,
		Role:                  input.Role,
		Response:              partialResponse,
		Iterations:            input.MaxIterations,
		TokensUsed:            totalTokens,
		InputTokens:           totalInput,
		OutputTokens:          totalOutput,
		CacheReadTokens:       totalCacheRead,
		CacheCreationTokens:   totalCacheCreation,
		CacheCreation1hTokens: totalCacheCreation1h,
		ModelUsed:             lastModel,
		Provider:              lastProvider,
		Success:               true,
		TeamKnowledge:         ft.Knowledge(),
	}, nil
}

// ── SwarmWorkflow ──────────────────────────────────────────────────────────────
// ── SwarmWorkflow：swarm 编排主流程 ────────────────────────────────────────────

// SwarmWorkflow orchestrates persistent AgentLoop child workflows with
// SwarmWorkflow 编排持久化 AgentLoop 子 workflow，
// inter-agent messaging and dynamic spawn capability.
// 并提供 agent 间消息与动态 spawn 能力。
func SwarmWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	if input.ParentWorkflowID != "" {
		workflowID = input.ParentWorkflowID
	}

	logger.Info("SwarmWorkflow started", "query", input.Query, "workflow_id", workflowID)

	// Activity options
	// activity 选项
	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 3},
	}
	ctx = workflow.WithActivityOptions(ctx, actOpts)

	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	p2pCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Dedicated context for workspace file I/O (read + list) — p2pCtx (10s) is too tight for EFS.
	// 工作区文件 I/O（读取 + 列表）的专用 context——p2pCtx（10s）对 EFS 太紧。
	fileReadCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts:    3,
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2.0,
		},
	})

	// Emit workflow started
	// 发出 workflow 启动事件
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowStarted,
		AgentID:    "swarm-supervisor",
		Message:    activities.MsgSwarmStarted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Version gate for P2P activities in SwarmWorkflow (send_message, broadcast).
	// SwarmWorkflow 中 P2P activity 的版本闸门（send_message、broadcast）。
	swarmP2pV := workflow.GetVersion(ctx, "swarm_p2p_v1", workflow.DefaultVersion, 1)

	// Load swarm config
	// 加载 swarm 配置
	var cfg activities.WorkflowConfig
	if err := workflow.ExecuteActivity(ctx, activities.GetWorkflowConfig).Get(ctx, &cfg); err != nil {
		logger.Warn("Failed to load config, using defaults", "error", err)
		cfg.SwarmMaxAgents = 10
		cfg.SwarmMaxIterationsPerAgent = 25
		cfg.SwarmAgentTimeoutSeconds = 1800
		cfg.SwarmMaxTotalLLMCalls = 200
		cfg.SwarmMaxTotalTokens = 1000000
		cfg.SwarmMaxWallClockMinutes = 30
	}

	// Setup workspace directories
	// 初始化工作区目录
	_ = workflow.ExecuteActivity(p2pCtx, constants.SetupWorkspaceDirsActivity, activities.SetupWorkspaceDirsInput{
		WorkflowID: workflowID,
		SessionID:  input.SessionID,
	}).Get(ctx, nil)

	// Replay safety: keep legacy behavior for old runs, disable injected memory prompt
	// for current/new runs. The injected context key is not consumed by /agent/loop.
	// 为兼容重放，旧运行保留原行为；新运行禁用注入的记忆 prompt——该 context key 并不被 /agent/loop 消费。
	memoryPromptVersion := workflow.GetVersion(ctx, "swarm_user_memory_prompt_v1", workflow.DefaultVersion, 1)
	if memoryPromptVersion == workflow.DefaultVersion && input.UserID != "" {
		if input.Context == nil {
			input.Context = make(map[string]any)
		}
		input.Context["user_memory_prompt"] = "You have access to a persistent memory directory at /memory/. " +
			"This contains knowledge from past sessions with this user.\n\n" +
			"At the start, read /memory/MEMORY.md to see what memories exist. " +
			"Read specific files for full details as needed."
	}

	// Lead activity context — longer timeout for LLM call
	// Lead activity context——LLM 调用需要更长超时
	leadCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 150 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 2},
	})

	// Track spawns, budget, and agent states
	// 跟踪 spawn、budget 与 agent 状态
	spawnCount := 0
	var budgetTotalLLMCalls, budgetTotalTokens int
	var historyTruncated bool
	var lastLeadMailboxSeq uint64 // Track last-read seq from Lead's mailbox 注：跟踪 Lead mailbox 最后读取的 seq
	maxLLMCalls := cfg.SwarmMaxTotalLLMCalls
	maxTokens := cfg.SwarmMaxTotalTokens
	maxWallClockSeconds := cfg.SwarmMaxWallClockMinutes * 60
	swarmStartTime := workflow.Now(ctx)

	// Cache-aware budget accounting (v1+): include prompt-cache tokens in
	// budgetTotalTokens so SwarmMaxTotalTokens hard gate at L2780 enforces
	// the true token cost. Pre-version replay paths keep the old cache-blind
	// math to preserve workflow determinism.
	// cache 感知的 budget 核算（v1+）：把 prompt cache token 计入 budgetTotalTokens，
	// 使 SwarmMaxTotalTokens 硬闸门按真实 token 成本执行；
	// 旧版本的 replay 路径保留不含 cache 的算法，以维持 workflow 确定性。
	swarmCacheAwareBudgetV := workflow.GetVersion(ctx, "swarm_cache_aware_budget_v1", workflow.DefaultVersion, 1)
	cacheAwareBudget := func(tokens, cacheRead, cacheCreation int) int {
		if swarmCacheAwareBudgetV >= 1 {
			return tokens + cacheRead + cacheCreation
		}
		return tokens
	}
	var elapsed int // declared here so it can be updated per-iteration and in file_read inner loop 注：在此声明，便于每轮以及 file_read 内循环中更新

	leadBudget := func(elapsed int) activities.LeadBudget {
		return activities.LeadBudget{
			TotalLLMCalls:       budgetTotalLLMCalls,
			RemainingLLMCalls:   maxLLMCalls - budgetTotalLLMCalls,
			TotalTokens:         budgetTotalTokens,
			RemainingTokens:     maxTokens - budgetTotalTokens,
			ElapsedSeconds:      elapsed,
			MaxWallClockSeconds: maxWallClockSeconds,
		}
	}

	recordLeadTokens := func(phase string, d activities.LeadDecisionResult) {
		recCtx := opts.WithTokenRecordOptions(ctx)
		provider := d.Provider
		if provider == "" {
			provider = detectProviderFromModel(d.ModelUsed)
		}
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:                input.UserID,
			SessionID:             input.SessionID,
			TaskID:                workflowID,
			AgentID:               "swarm-lead",
			Model:                 d.ModelUsed,
			Provider:              provider,
			InputTokens:           d.InputTokens,
			OutputTokens:          d.OutputTokens,
			CacheReadTokens:       d.CacheReadTokens,
			CacheCreationTokens:   d.CacheCreationTokens,
			CacheCreation1hTokens: 0, // LeadDecisionResult doesn't carry per-TTL breakdown 注：LeadDecisionResult 不携带按 TTL 的拆分
			CallSequence:          d.CallSequence,
			Metadata:              map[string]any{"workflow": "swarm", "phase": phase},
		}).Get(ctx, nil)
	}

	runLeadFileIO := func(ctx, fileReadCtx workflow.Context, actions []activities.LeadAction, readCharLimit int) []activities.FileReadResult {
		var out []activities.FileReadResult
		for _, fr := range actions {
			if fr.Type == "file_list" {
				var listResult activities.ListWorkspaceFilesResult
				listErr := workflow.ExecuteActivity(fileReadCtx, constants.ListWorkspaceFilesActivity,
					activities.ListWorkspaceFilesInput{SessionID: input.SessionID}).Get(ctx, &listResult)
				if listErr != nil {
					out = append(out, activities.FileReadResult{Path: ".", Error: listErr.Error()})
				} else {
					var listing string
					for _, f := range listResult.Files {
						listing += f.Path + "\n"
					}
					if listing == "" {
						listing = "(empty workspace)"
					}
					out = append(out, activities.FileReadResult{Path: ".", Content: listing})
				}
			} else if fr.Path != "" {
				var readResult activities.ReadWorkspaceFileResult
				readErr := workflow.ExecuteActivity(fileReadCtx, constants.ReadWorkspaceFileActivity,
					activities.ReadWorkspaceFileInput{
						SessionID: input.SessionID, Path: fr.Path, MaxChars: readCharLimit,
					}).Get(ctx, &readResult)
				if readErr != nil {
					out = append(out, activities.FileReadResult{Path: fr.Path, Error: readErr.Error()})
				} else {
					out = append(out, activities.FileReadResult(readResult))
				}
			}
		}
		return out
	}

	type agentFuture struct {
		ID         string
		Future     workflow.ChildWorkflowFuture
		WorkflowID string // Child workflow execution ID for signaling 注：用于发信号的子 workflow execution ID
	}
	var agentFutures []agentFuture
	var roster []TeamMember
	results := make(map[string]AgentLoopResult)
	idleSnapshots := make(map[string]AgentLoopResult)              // Best-effort snapshots from idle notifications 注：来自 idle 通知的尽力快照
	swarmTeamKnowledge := make([]activities.TeamKnowledgeEntry, 0) // Cross-agent knowledge accumulator (L2) 注：跨 agent 知识累加器（L2）
	completionCh := workflow.NewChannel(ctx)
	agentStates := make(map[string]activities.LeadAgentState)
	agentTaskStartTimes := make(map[string]time.Time) // agentID → when current task started 注：agentID → 当前 task 开始时间
	agentTaskMap := make(map[string]string)           // agentID → current taskID 注：agentID → 当前 taskID
	leadHistory := make([]map[string]any, 0)          // Must be non-nil for JSON → Pydantic List 注：必须非 nil，否则 JSON → Pydantic List 失败

	// Version gate for Lead tool_call support (Temporal replay safety)
	// Lead tool_call 支持的版本闸门（保证 Temporal 重放安全）
	leadToolCallV := workflow.GetVersion(ctx, "lead-tool-call-v1", workflow.DefaultVersion, 1)

	allowedLeadTools := map[string]bool{
		"web_search": true, "web_fetch": true, "calculator": true,
		"file_list": true, "file_read": true,
	}

	const maxToolCallRounds = 5

	// Dedicated activity context for Lead tool execution (web_search/web_fetch need 30-60s).
	// Lead tool 执行的专用 activity context（web_search/web_fetch 需 30-60 秒）。
	// p2pCtx has 10s timeout which is too short — tool calls would always timeout.
	// p2pCtx 只有 10 秒超时，太短——tool 调用必定超时。
	toolCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 90 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Extract model override fields from task context for experimentation
	// 从 task context 提取 model 覆盖字段，供实验使用
	leadModelOverride := GetContextString(input.Context, "lead_model_override")
	leadProviderOverride := GetContextString(input.Context, "lead_provider_override")
	agentModelOverride := GetContextString(input.Context, "agent_model_override")
	agentProviderOverride := GetContextString(input.Context, "agent_provider_override")

	// ── Phase 1: Lead Initial Planning ──────────────────────────────────────
	// ── Phase 1：Lead 初始规划 ──────────────────────────────────────────────
	// Lead Agent decomposes the query into tasks and decides which agents to spawn.
	// Lead Agent 把 query 拆成 task，并决定 spawn 哪些 agent。
	// This replaces the old DecomposeTask → static roster approach.
	// 这取代了旧的 DecomposeTask → 静态 roster 方案。
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventProgress,
		AgentID:    "swarm-lead",
		Message:    activities.MsgSwarmPlanning(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Convert session history for Lead multi-turn context
	// 转换 session 历史为 Lead 的多轮 context
	var conversationHistory []map[string]any
	if len(input.History) > 0 {
		conversationHistory = convertHistoryForLead(input.History)
	}

	var planDecision activities.LeadDecisionResult
	planEvent := activities.LeadEvent{
		Type:          "initial_plan",
		ResultSummary: input.Query,
	}
	// Phase 1 inner loop: handles tool_call actions (same pattern as Phase 2).
	// 阶段 1 内循环：处理 tool_call 动作（与阶段 2 同一模式）。
	// Lead may return tool_call to search/fetch before deciding on plan.
	// Lead 可先返回 tool_call 搜索/抓取，再决定 plan。
	for phase1ToolRound := 0; phase1ToolRound <= maxToolCallRounds; phase1ToolRound++ {
		elapsed = int(workflow.Now(ctx).Sub(swarmStartTime).Seconds())
		planErr := workflow.ExecuteActivity(leadCtx, constants.LeadDecisionActivity, activities.LeadDecisionInput{
			WorkflowID:           workflowID,
			Event:                planEvent,
			TaskList:             make([]activities.SwarmTask, 0),
			AgentStates:          make([]activities.LeadAgentState, 0),
			Budget:               leadBudget(elapsed),
			History:              leadHistory,
			OriginalQuery:        input.Query,
			ConversationHistory:  conversationHistory,
			LeadModelOverride:    leadModelOverride,
			LeadProviderOverride: leadProviderOverride,
		}).Get(ctx, &planDecision)

		if planErr != nil {
			logger.Error("Lead initial planning failed", "error", planErr)
			return TaskResult{Success: false, ErrorMessage: fmt.Sprintf("Lead planning failed: %v", planErr)}, planErr
		}

		budgetTotalLLMCalls++
		budgetTotalTokens += cacheAwareBudget(planDecision.TokensUsed, planDecision.CacheReadTokens, planDecision.CacheCreationTokens)

		// Record Lead token usage
		// 记录 Lead token 用量
		recordLeadTokens("initial_plan", planDecision)

		leadHistory = append(leadHistory, map[string]any{
			"decision_summary": planDecision.DecisionSummary,
			"event":            "initial_plan",
			"actions":          len(planDecision.Actions),
		})

		// Separate actions into file I/O, tool_call, and others
		// 把动作分为文件 I/O、tool_call 与其他
		var fileIOActions []activities.LeadAction
		var toolCallActions []activities.LeadAction
		var otherActions []activities.LeadAction
		for _, action := range planDecision.Actions {
			if action.Type == "file_read" || action.Type == "file_list" {
				fileIOActions = append(fileIOActions, action)
			} else if leadToolCallV >= 1 && action.Type == "tool_call" {
				toolCallActions = append(toolCallActions, action)
			} else {
				otherActions = append(otherActions, action)
			}
		}

		// Handle interim_reply before I/O — only emit on first round to avoid repetitive messages
		// 在 I/O 之前处理 interim_reply——只在首轮发出，避免重复消息
		if phase1ToolRound == 0 {
			for _, action := range otherActions {
				if action.Type == "interim_reply" && action.Content != "" {
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventLLMOutput,
						AgentID:    "swarm-lead",
						Message:    action.Content,
						Payload: map[string]any{
							"role":    "lead",
							"interim": true,
						},
						Timestamp: workflow.Now(ctx),
					}).Get(ctx, nil)
				}
			}
		}

		// Execute file I/O AND tool_call in the same round (no mutual exclusion)
		// 同一轮内同时执行文件 I/O 与 tool_call（互不排斥）
		ranFileIO := false
		ranToolCalls := false

		// File I/O: file_read + file_list (zero LLM cost)
		// 文件 I/O：file_read + file_list（零 LLM 成本）
		if len(fileIOActions) > 0 && phase1ToolRound < maxToolCallRounds {
			ranFileIO = true
			planEvent.FileContents = runLeadFileIO(ctx, fileReadCtx, fileIOActions, 4000)
		}

		// Tool calls
		// tool 调用
		if len(toolCallActions) > 0 && phase1ToolRound < maxToolCallRounds {
			ranToolCalls = true
			var toolResults []activities.ToolResultEntry
			for _, tc := range toolCallActions {
				if !allowedLeadTools[tc.Tool] {
					toolResults = append(toolResults, activities.ToolResultEntry{
						Tool: tc.Tool, Error: fmt.Sprintf("tool %q not in allowlist", tc.Tool),
					})
					continue
				}
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: workflowID,
					EventType:  activities.StreamEventLeadToolCall,
					AgentID:    "swarm-lead",
					Message:    fmt.Sprintf("Lead executing %s", tc.Tool),
					Payload:    map[string]any{"tool": tc.Tool, "phase": "initial_plan"},
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
				var toolResult activities.LeadToolResult
				toolErr := workflow.ExecuteActivity(toolCtx, constants.LeadExecuteToolActivity,
					activities.LeadToolInput{
						Tool: tc.Tool, ToolParams: tc.ToolParams, SessionID: input.SessionID,
					}).Get(ctx, &toolResult)
				if toolErr != nil {
					toolResults = append(toolResults, activities.ToolResultEntry{Tool: tc.Tool, Error: toolErr.Error()})
				} else {
					output := toolResult.Output
					maxChars := 4000
					if tc.Tool == "calculator" {
						maxChars = 500
					}
					if len(output) > maxChars {
						output = output[:maxChars] + "\n... [truncated]"
					}
					toolResults = append(toolResults, activities.ToolResultEntry{
						Tool: tc.Tool, Output: output, Error: toolResult.Error,
					})
				}
				logger.Info("Lead Phase 1 tool_call executed", "tool", tc.Tool, "round", phase1ToolRound)
			}
			planEvent.ToolResults = toolResults
		}

		// Clear stale fields (M2)
		// 清理过期字段（M2）
		if ranFileIO && !ranToolCalls {
			planEvent.ToolResults = nil
		}
		if ranToolCalls && !ranFileIO {
			planEvent.FileContents = nil
		}

		// If any I/O was executed, loop back for another LeadDecision
		// 若执行过任何 I/O，回到循环再调一次 LeadDecision
		if !ranFileIO && !ranToolCalls {
			break // No more I/O — proceed to action processing 注：没有更多 I/O——进入动作处理
		}
		logger.Info("Lead Phase 1 I/O round complete, calling LeadDecision again",
			"round", phase1ToolRound, "ranFileIO", ranFileIO, "ranToolCalls", ranToolCalls)
		continue
	}

	logger.Info("Lead initial plan",
		"summary", planDecision.DecisionSummary,
		"actions", len(planDecision.Actions),
	)

	// Check if Lead wants to reply directly (no agents needed)
	// 检查 Lead 是否要直接回复（无需 agent）
	// This happens when Lead uses tool_call to gather data and then replies.
	// 这种情况发生在 Lead 用 tool_call 收集数据后直接回复。
	// Version-gated: old workflows in replay won't have this early return path.
	// 受版本闸门保护：重放中的旧 workflow 不会走这条提前返回路径。
	directReplyV := workflow.GetVersion(ctx, "swarm_direct_reply_v1", workflow.DefaultVersion, 1)
	if directReplyV >= 1 {
		var directReply string
		for _, action := range planDecision.Actions {
			if action.Type == "reply" || action.Type == "done" {
				directReply = action.Content
				break
			}
		}
		if directReply != "" {
			logger.Info("Lead answering directly without agents", "reply_len", len(directReply))
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventLLMOutput,
				AgentID:    "swarm-lead",
				Message:    directReply,
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)

			// Emit final_output LLM_OUTPUT so the OpenAI-compatible streamer
			// 发出 final_output LLM_OUTPUT，让 OpenAI 兼容的 streamer
			// picks up the canonical answer (it only forwards AgentID=="final_output").
			// 取到最终答案（它只转发 AgentID=="final_output" 的输出）。
			finalOutputV := workflow.GetVersion(ctx, "swarm_final_output_event_v1", workflow.DefaultVersion, 1)
			if finalOutputV >= 1 {
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: workflowID,
					EventType:  activities.StreamEventLLMOutput,
					AgentID:    "final_output",
					Message:    directReply,
					Timestamp:  workflow.Now(ctx),
				}).Get(ctx, nil)
			}

			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventWorkflowCompleted,
				AgentID:    "swarm-supervisor",
				Message:    activities.MsgSwarmCompleted(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
			return TaskResult{
				Success: true,
				Result:  directReply,
			}, nil
		}
	}

	// Emit LEAD_DECISION so the frontend can show Lead's pulse in Radar
	// 发出 LEAD_DECISION，让前端在 Radar 中显示 Lead 的脉冲
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventLeadDecision,
		AgentID:    "swarm-lead",
		Message:    leadDisplayMessage(planDecision),
		Payload: map[string]any{
			"event_type": "initial_plan",
			"actions":    len(planDecision.Actions),
		},
	}).Get(ctx, nil)

	// Execute Lead's initial planning actions (revise_plan + spawn_agent)
	// 执行 Lead 的初始规划动作（revise_plan + spawn_agent）
	// Helper to spawn an agent from Lead action
	// 根据 Lead 动作 spawn 一个 agent 的辅助函数
	spawnAgent := func(taskDesc string, role string, modelTier string, taskID string, skipAttachments bool) {
		if spawnCount >= cfg.SwarmMaxAgents {
			logger.Warn("Lead spawn rejected: at capacity")
			return
		}
		agentName := agents.GetAgentName(workflowID, spawnCount)
		spawnCount++

		roster = append(roster, TeamMember{AgentID: agentName, Task: taskDesc, Role: role})
		agentStates[agentName] = activities.LeadAgentState{
			AgentID:     agentName,
			Status:      "running",
			CurrentTask: taskDesc,
			Role:        role,
		}
		agentTaskStartTimes[agentName] = workflow.Now(ctx)

		// Build per-agent context with model_tier.
		// Swarm agents default to "medium" when Lead omits model_tier,
		// preventing fallthrough to budget_agent_max which incorrectly maps to "small".
		// swarm agent 在 Lead 未指定 model_tier 时默认 "medium"，
		// 避免落到 budget_agent_max 而被错误映射为 "small"。
		agentCtx := make(map[string]any, len(input.Context)+1)
		maps.Copy(agentCtx, input.Context)
		// Default: agents receive attachments. Lead can opt-out via skipAttachments
		// 默认：agent 会收到附件。Lead 可用 skipAttachments 关闭，
		// to save tokens for agents that don't need files (e.g. web research).
		// 为不需要文件的 agent（如 web 调研）省 token。
		if _, hasAtt := agentCtx["attachments"]; hasAtt && skipAttachments {
			delete(agentCtx, "attachments")
		}
		if modelTier == "" {
			modelTier = "medium"
		}
		agentCtx["model_tier"] = modelTier
		// Apply agent-level model/provider override from task context
		// 从 task context 套用 agent 级 model/provider 覆盖
		if agentModelOverride != "" {
			agentCtx["model_override"] = agentModelOverride
		}
		if agentProviderOverride != "" {
			agentCtx["provider_override"] = agentProviderOverride
		}

		childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
			WorkflowExecutionTimeout: time.Duration(cfg.SwarmAgentTimeoutSeconds) * time.Second,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
		})
		future := workflow.ExecuteChildWorkflow(childCtx, AgentLoop, AgentLoopInput{
			AgentID:               agentName,
			WorkflowID:            workflowID,
			Task:                  taskDesc,
			MaxIterations:         cfg.SwarmMaxIterationsPerAgent,
			SessionID:             input.SessionID,
			UserID:                input.UserID,
			Context:               agentCtx,
			TeamRoster:            roster,
			WorkspaceMaxEntries:   cfg.SwarmWorkspaceMaxEntries,
			WorkspaceSnippetChars: cfg.SwarmWorkspaceSnippetChars,
			Role:                  role,
			OriginalQuery:         input.Query,
			TeamKnowledge:         swarmTeamKnowledge,
		})
		af := agentFuture{ID: agentName, Future: future}
		// Resolve child workflow execution ID for signaling
		// 解析子 workflow 的 execution ID，用于发信号
		var childExec workflow.Execution
		if err := future.GetChildWorkflowExecution().Get(ctx, &childExec); err == nil {
			af.WorkflowID = childExec.ID
		}
		agentFutures = append(agentFutures, af)

		workflow.Go(ctx, func(gCtx workflow.Context) {
			var result AgentLoopResult
			if err := future.Get(gCtx, &result); err != nil {
				result = AgentLoopResult{AgentID: agentName, Success: false, Error: err.Error()}
			}
			// If child workflow timed out / was cancelled (Success=false, empty Response),
			// 若子 workflow 超时/被取消（Success=false、Response 为空），
			// check if we already captured an idle snapshot with useful data.
			// 检查是否已捕获含可用数据的 idle 快照。
			if !result.Success {
				if snapshot, ok := idleSnapshots[agentName]; ok && snapshot.Response != "" {
					if len(snapshot.Response) > len(result.Response) {
						logger.Info("Agent child workflow failed, using idle snapshot as fallback",
							"agent_id", agentName,
							"error", result.Error,
							"snapshot_len", len(snapshot.Response),
						)
						snapshot.Error = result.Error
						snapshot.Iterations = result.Iterations
						result = snapshot
					}
				}
			}
			completionCh.Send(gCtx, result)
		})

		// Link agent to TaskList entry if specified
		// 指定了 task_id 时把 agent 关联到 TaskList 条目
		if taskID != "" {
			_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
				WorkflowID: workflowID,
				TaskID:     taskID,
				Status:     "in_progress",
				AgentID:    agentName,
			}).Get(ctx, nil)
			agentTaskMap[agentName] = taskID
		}

		logger.Info("Lead spawned agent", "agent_id", agentName, "task", taskDesc, "task_id", taskID)
	}

	var planTasks []activities.SwarmTask // tracks tasks from revise_plan for dependency checking 注：记录 revise_plan 产生的 task，用于依赖检查

	// Phase 1: Process revise_plan first to ensure tasks exist in Redis before spawn_agent references them.
	// Without this ordering, spawn_agent may call UpdateTaskStatus(T1, in_progress) before T1 is created.
	// 先处理 revise_plan，确保 task 已写入 Redis 再处理 spawn_agent；
	// 否则 spawn_agent 可能在 T1 创建前就调用 UpdateTaskStatus(T1, in_progress)。
	for _, action := range planDecision.Actions {
		if action.Type == "revise_plan" {
			for _, newTask := range action.Create {
				task := activities.SwarmTask{
					ID:          fmt.Sprintf("%v", newTask["id"]),
					Description: fmt.Sprintf("%v", newTask["description"]),
					CreatedBy:   "lead",
				}
				// Parse depends_on for task dependency enforcement
				// 解析 depends_on 以执行 task 依赖约束
				if deps, ok := newTask["depends_on"].([]any); ok {
					for _, d := range deps {
						task.DependsOn = append(task.DependsOn, fmt.Sprintf("%v", d))
					}
				}
				planTasks = append(planTasks, task)
			}
		}
	}
	if len(planTasks) > 0 {
		_ = workflow.ExecuteActivity(p2pCtx, constants.InitTaskListActivity, activities.InitTaskListInput{
			WorkflowID: workflowID,
			Tasks:      planTasks,
		}).Get(ctx, nil)
	}

	// Phase 1b: Process task description updates from revise_plan
	// 阶段 1b：处理 revise_plan 带来的 task 描述更新
	// Version-gated: this activity call didn't exist in older workflows.
	// 受版本闸门保护：旧 workflow 中没有这个 activity 调用。
	phase1DescV := workflow.GetVersion(ctx, "swarm_phase1_task_desc_update_v1", workflow.DefaultVersion, 1)
	if phase1DescV >= 1 {
		for _, action := range planDecision.Actions {
			if action.Type == "revise_plan" {
				for _, upd := range action.Update {
					taskID := fmt.Sprintf("%v", upd["id"])
					desc, _ := upd["description"].(string)
					if taskID != "" && desc != "" {
						_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskDescriptionActivity, activities.UpdateTaskDescriptionInput{
							WorkflowID:  workflowID,
							TaskID:      taskID,
							Description: desc,
						}).Get(ctx, nil)
					}
				}
			}
		}
	}

	// Phase 2: Process remaining actions (interim_reply, spawn_agent, etc.)
	// 阶段 2：处理剩余动作（interim_reply、spawn_agent 等）
	for _, action := range planDecision.Actions {
		switch action.Type {
		case "revise_plan":
			continue // already handled in Phase 1 注：已在阶段 1 处理
		case "interim_reply":
			if action.Content != "" {
				_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
					WorkflowID: workflowID,
					EventType:  activities.StreamEventLLMOutput,
					AgentID:    "swarm-lead",
					Message:    action.Content,
					Payload: map[string]any{
						"role":    "lead",
						"interim": true,
					},
					Timestamp: workflow.Now(ctx),
				}).Get(ctx, nil)
			}
		case "spawn_agent":
			// Dependency guard: reject spawn for tasks with unmet depends_on
			// 依赖守卫：depends_on 未满足的 task 拒绝 spawn
			if action.TaskID != "" && taskHasUnmetDeps(action.TaskID, planTasks) {
				logger.Warn("Rejecting spawn_agent: task has unmet dependencies — will be assigned when deps complete",
					"task_id", action.TaskID,
					"role", action.Role,
				)
			} else {
				spawnAgent(action.TaskDescription, action.Role, action.ModelTier, action.TaskID, action.SkipAttachments)
			}
		}
	}

	// Auto-link orphaned tasks to unlinked agents (defensive fix for Lead omitting task_id in spawn_agent).
	// Without this, tasks stay "pending" forever and block all downstream depends_on chains.
	// 把未被认领的 pending task 自动关联到未绑定的 agent（防御 Lead 在 spawn_agent 中漏写 task_id）；
	// 否则 task 永远停在 pending，阻塞整条 depends_on 链。
	autoLinkV := workflow.GetVersion(ctx, "swarm_auto_link_orphans_v1", workflow.DefaultVersion, 1)
	if autoLinkV == 1 {
		var unlinked []string
		for _, af := range agentFutures {
			if _, ok := agentTaskMap[af.ID]; !ok {
				unlinked = append(unlinked, af.ID)
			}
		}
		if len(unlinked) > 0 {
			// Find orphaned: pending tasks not claimed by any agent, no unmet deps
			// 查找孤儿 task：未被任何 agent 认领、且依赖已满足的 pending task
			claimed := make(map[string]bool)
			for _, tid := range agentTaskMap {
				claimed[tid] = true
			}
			var orphaned []string
			for _, t := range planTasks {
				if (t.Status == "" || t.Status == "pending") && !claimed[t.ID] && !taskHasUnmetDeps(t.ID, planTasks) {
					orphaned = append(orphaned, t.ID)
				}
			}
			if len(unlinked) == len(orphaned) && len(unlinked) > 0 {
				for i, agentID := range unlinked {
					taskID := orphaned[i]
					agentTaskMap[agentID] = taskID
					_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity,
						activities.UpdateTaskStatusInput{WorkflowID: workflowID, TaskID: taskID, Status: "in_progress", AgentID: agentID}).Get(ctx, nil)
					logger.Info("Auto-linked orphaned task to agent", "agent_id", agentID, "task_id", taskID)
				}
			} else if len(unlinked) > 0 {
				logger.Warn("Unlinked agents after initial plan, cannot auto-link (count mismatch)",
					"unlinked", len(unlinked), "orphaned", len(orphaned))
			}
		}
	}

	if len(agentFutures) == 0 {
		return TaskResult{Success: false, ErrorMessage: "Lead planning produced no agents to spawn"}, nil
	}

	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventProgress,
		AgentID:    "swarm-lead",
		Message:    activities.MsgSwarmSpawning(len(agentFutures)),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// ── Phase 2: Lead Event-Driven Monitoring ───────────────────────────────
	// ── Phase 2：Lead 事件驱动监控 ──────────────────────────────────────────
	completedCount := 0
	totalExpected := len(agentFutures)
	checkpointInterval := 2 * time.Minute // Fix 2: reduced from 60s to 120s 注：修复 2：由 60s 调整为 120s

	// Signal channel for idle agents — wakes Lead immediately when an agent goes idle
	// 空闲 agent 的信号通道——agent 一空闲就立即唤醒 Lead
	agentIdleCh := workflow.GetSignalChannel(ctx, "agent-idle")

	// Signal channel for P2P message forwarding — agents notify parent when they send
	// P2P 消息转发的信号通道——agent 发送消息时通知父 workflow，
	// a message, so parent can wake idle recipients via Temporal Signal (zero LLM cost).
	// 父 workflow 再用 Temporal Signal 唤醒空闲接收方（零 LLM 成本）。
	messageSentCh := workflow.GetSignalChannel(ctx, "agent-message-sent")

	// Signal channel for HITL (Human-in-the-Loop) — user can send messages to Lead mid-execution
	// HITL（Human-in-the-Loop）信号通道——用户可在执行中给 Lead 发消息
	humanInputCh := workflow.GetSignalChannel(ctx, "human-input")

	// Throttle interim_reply emissions — suppress if <60s since last emit
	// 限流 interim_reply 发送——距上次发出不足 60s 则抑制
	const interimThrottleSeconds = 60
	var lastInterimEmitTime time.Time

	// Track whether a HITL message has been received but not yet acted on
	// (i.e., Lead hasn't created tasks or revised plan in response).
	// Prevents FORCED CLOSE from skipping deferred user requests.
	// After 2 checkpoint rounds without action, give up to prevent infinite loop.
	// 记录"已收到但尚未处理"的 HITL 消息，避免 FORCED CLOSE 跳过用户 request；
	// 连续 2 轮 checkpoint 未处理则放弃，防止死循环。
	hitlPendingAction := false
	hitlReminderCount := 0
	var hitlMessages []string // Accumulate all HITL messages for Lead context 注：累积所有 HITL 消息，供 Lead context 使用

	// Event-driven Lead loop (D1, D5: event coalescing)
	// 事件驱动的 Lead 主循环（D1、D5：事件合并）
	for completedCount < totalExpected {
		var event activities.LeadEvent
		gotCompletion := false

		// Create cancellable context for the checkpoint timer so it doesn't leak
		// 为 checkpoint 计时器创建可取消的 context，避免泄漏
		// when a non-timer handler fires first.
		// （当非计时器分支先触发时）。
		timerCtx, timerCancel := workflow.WithCancel(ctx)

		sel := workflow.NewSelector(ctx)

		// Agent idle notification — agent finished task and is waiting for reassignment
		// agent 空闲通知——agent 已完成 task，等待重新派活
		sel.AddReceive(agentIdleCh, func(ch workflow.ReceiveChannel, more bool) {
			timerCancel()
			var idleInfo map[string]any
			ch.Receive(ctx, &idleInfo)
			agentID, _ := idleInfo["agent_id"].(string)
			resultSummary, _ := idleInfo["result_summary"].(string)
			iterations, _ := idleInfo["iterations"].(float64) // JSON numbers → float64 注：JSON 数字 → float64
			toolsUsed, _ := idleInfo["tools_used"].(string)
			completionReport, _ := idleInfo["completion_report"].(map[string]any)

			// Filter raw tool JSON from result_summary (P4 fix)
			// 从 result_summary 中过滤裸 tool JSON（P4 修复）
			if resultSummary != "" && looksLikeToolJSON(resultSummary) {
				logger.Warn("Filtering tool JSON from idle result_summary — agent likely output file content as text instead of calling file_write",
					"agent_id", agentID,
					"summary_length", len(resultSummary),
				)
				resultSummary = ""
			}

			logger.Info("Lead notified: agent went idle",
				"agent_id", agentID,
				"has_result", resultSummary != "",
				"iterations", int(iterations),
				"tools_used", toolsUsed,
			)

			// Update agent state to idle — preserve CurrentTask so Lead knows what was completed
			// 把 agent 状态更新为空闲——保留 CurrentTask，让 Lead 知道完成了什么
			if s, ok := agentStates[agentID]; ok {
				s.Status = "idle"
				s.IterationsUsed = int(iterations)
				// Populate FilesWritten from completion_report so Lead can pass paths to synthesis_writer
				// 用 completion_report 填充 FilesWritten，便于 Lead 把路径传给 synthesis_writer
				if filesRaw, ok := completionReport["files_written"]; ok {
					if files, ok := filesRaw.([]string); ok {
						s.FilesWritten = files
					} else if filesArr, ok := filesRaw.([]any); ok {
						for _, f := range filesArr {
							if fs, ok := f.(string); ok {
								s.FilesWritten = append(s.FilesWritten, fs)
							}
						}
					}
				}
				agentStates[agentID] = s
			}

			// Mark the agent's current task as completed in TaskList.
			// This is critical for depends_on enforcement — downstream tasks check
			// dep status in TaskList, not agentStates.
			// 这是 depends_on 生效的关键：下游 task 检查的是 TaskList 中的依赖状态，而非 agentStates。
			if taskID, ok := agentTaskMap[agentID]; ok && taskID != "" {
				idleTaskErr := workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
					WorkflowID: workflowID,
					TaskID:     taskID,
					Status:     "completed",
					AgentID:    agentID,
				}).Get(ctx, nil)
				if idleTaskErr != nil {
					logger.Error("Failed to mark task completed on agent idle",
						"agent_id", agentID, "task_id", taskID, "error", idleTaskErr)
				} else {
					logger.Info("Marked task completed on agent idle",
						"agent_id", agentID, "task_id", taskID)
				}
				delete(agentTaskMap, agentID)
			} else {
				logger.Warn("Agent idle but no task mapping found",
					"agent_id", agentID, "agentTaskMap_size", len(agentTaskMap))
			}

			// Save idle snapshot — if child workflow later times out, we still have this result
			// 保存 idle 快照——子 workflow 之后即使超时，也还有这份结果
			if resultSummary != "" {
				idleSnapshots[agentID] = AgentLoopResult{
					AgentID:    agentID,
					Response:   resultSummary,
					Iterations: int(iterations),
					Success:    true,
					ToolsUsed:  toolsUsed,
				}
			}

			// Merge agent's team knowledge into global accumulator (L2 cross-agent dedup)
			// 把 agent 的团队知识并入全局累加器（L2 跨 agent 去重）
			if tkRaw, ok := idleInfo["team_knowledge"]; ok {
				if tkSlice, ok := tkRaw.([]any); ok {
					var agentTK []activities.TeamKnowledgeEntry
					for _, item := range tkSlice {
						if m, ok := item.(map[string]any); ok {
							agentTK = append(agentTK, activities.TeamKnowledgeEntry{
								URL:       fmt.Sprintf("%v", m["url"]),
								Agent:     fmt.Sprintf("%v", m["agent"]),
								Summary:   fmt.Sprintf("%v", m["summary"]),
								CharCount: int(toFloat64(m["char_count"])),
							})
						}
					}
					if len(agentTK) > 0 {
						swarmTeamKnowledge = mergeTeamKnowledge(swarmTeamKnowledge, agentTK)
						logger.Info("Merged team knowledge from idle agent",
							"agent_id", agentID,
							"new_entries", len(agentTK),
							"total_entries", len(swarmTeamKnowledge),
						)
					}
				}
			}

			event = activities.LeadEvent{
				Type:             "agent_idle",
				AgentID:          agentID,
				ResultSummary:    truncateDecisionSummary(resultSummary),
				CompletionReport: completionReport,
			}
		})

		// Agent completion events
		// agent 完成事件
		sel.AddReceive(completionCh, func(ch workflow.ReceiveChannel, more bool) {
			timerCancel()
			var result AgentLoopResult
			ch.Receive(ctx, &result)
			results[result.AgentID] = result
			completedCount++
			// Each AgentLoopStep iteration = exactly 1 LLM call.
			// Tool execution tokens (web_search results, etc.) are NOT counted here
			// because they're processed by agent-core, not the LLM service.
			// 每次 AgentLoopStep 迭代恰好一次 LLM 调用；tool 执行的 token（web_search 结果等）
			// 不在此计数，因为它们由 agent-core 而非 LLM 服务处理。
			budgetTotalLLMCalls += result.Iterations
			budgetTotalTokens += cacheAwareBudget(result.TokensUsed, result.CacheReadTokens, result.CacheCreationTokens)
			// Note: per-step LLM tokens and per-tool Haiku tokens are already recorded
			// 注意：每步 LLM token 与每个 tool 的 Haiku token 已在
			// inside AgentLoop (agent_step + tool_exec phases). No aggregate recording
			// AgentLoop 内（agent_step + tool_exec 阶段）记录过。此处不做汇总记录，
			// here to avoid double-counting.
			// 以免重复计数。

			// Update agent state
			// 更新 agent 状态
			agentStates[result.AgentID] = activities.LeadAgentState{
				AgentID:        result.AgentID,
				Status:         "completed",
				CurrentTask:    "",
				IterationsUsed: result.Iterations,
				Role:           result.Role,
			}

			// Merge completed agent's team knowledge into global accumulator
			// 把已完成 agent 的团队知识并入全局累加器
			if len(result.TeamKnowledge) > 0 {
				swarmTeamKnowledge = mergeTeamKnowledge(swarmTeamKnowledge, result.TeamKnowledge)
				logger.Info("Merged team knowledge from completed agent",
					"agent_id", result.AgentID,
					"new_entries", len(result.TeamKnowledge),
					"total_entries", len(swarmTeamKnowledge),
				)
			}

			resultSummary := truncateDecisionSummary(result.Response)
			if resultSummary == "" && result.Error != "" {
				resultSummary = fmt.Sprintf("Agent failed: %s", truncateDecisionSummary(result.Error))
			}
			event = activities.LeadEvent{
				Type:          "agent_completed",
				AgentID:       result.AgentID,
				ResultSummary: resultSummary,
				Error:         result.Error,
				Success:       result.Success,
			}
			gotCompletion = true

			// Sync TaskList: mark the agent's current task as completed
			// 同步 TaskList：把该 agent 当前 task 标记为 completed
			if taskID, ok := agentTaskMap[result.AgentID]; ok && taskID != "" {
				_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
					WorkflowID: workflowID,
					TaskID:     taskID,
					Status:     "completed",
					AgentID:    result.AgentID,
				}).Get(ctx, nil)
				delete(agentTaskMap, result.AgentID)
			}

			// Clear child workflow ID to prevent signaling a dead workflow
			// 清空子 workflow ID，避免向已结束的 workflow 发信号
			for i := range agentFutures {
				if agentFutures[i].ID == result.AgentID {
					agentFutures[i].WorkflowID = ""
					break
				}
			}

			logger.Info("Agent completed",
				"agent_id", result.AgentID,
				"success", result.Success,
				"completed", completedCount,
				"total", totalExpected,
			)
		})

		// Checkpoint timer (wake Lead periodically even if no events)
		// checkpoint 计时器（即使无事件也定期唤醒 Lead）
		sel.AddFuture(workflow.NewTimer(timerCtx, checkpointInterval), func(f workflow.Future) {
			_ = f.Get(ctx, nil)
			if !gotCompletion {
				event = activities.LeadEvent{Type: "checkpoint"}
			}
		})

		// HITL: human user sends a message to Lead mid-execution
		// HITL：用户在执行中给 Lead 发消息
		sel.AddReceive(humanInputCh, func(ch workflow.ReceiveChannel, more bool) {
			timerCancel()
			var humanMsg map[string]string
			ch.Receive(ctx, &humanMsg)
			message := humanMsg["message"]
			logger.Info("Lead received human input", "message", message)
			hitlPendingAction = true
			hitlMessages = append(hitlMessages, message)

			event = activities.LeadEvent{
				Type:         "human_input",
				HumanMessage: message,
			}
		})

		sel.Select(ctx)

		// P2P message forwarding: drain buffered "agent-message-sent" signals and
		// forward wake-up signals to idle recipients. No Lead LLM call — pure Go routing.
		// 排空已缓冲的 "agent-message-sent" 信号，向空闲接收方转发唤醒信号；
		// 不调用 Lead LLM，纯 Go 路由。
		for {
			var msgInfo map[string]string
			if !messageSentCh.ReceiveAsync(&msgInfo) {
				break
			}
			targetID := msgInfo["to"]
			fromID := msgInfo["from"]
			if s, ok := agentStates[targetID]; ok && s.Status == "idle" {
				for _, af := range agentFutures {
					if af.ID == targetID && af.WorkflowID != "" {
						_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "",
							fmt.Sprintf("agent:%s:peer-message", targetID),
							map[string]string{"from": fromID},
						).Get(ctx, nil)
						logger.Info("Forwarded peer message to idle agent",
							"from", fromID, "to", targetID)
						break
					}
				}
			}
		}

		// Signal coalescing (P3 fix): drain any additional idle signals that arrived
		// 信号合并（P3 修复）：排空同时到达的其他 idle 信号。
		// simultaneously. ReceiveAsync is deterministic — only drains buffered signals.
		// ReceiveAsync 是确定性的——只排空已缓冲的信号。
		if event.Type == "agent_idle" {
			for {
				var extraIdle map[string]any
				if !agentIdleCh.ReceiveAsync(&extraIdle) {
					break
				}
				extraAgentID, _ := extraIdle["agent_id"].(string)
				extraSummary, _ := extraIdle["result_summary"].(string)
				extraIter, _ := extraIdle["iterations"].(float64)
				extraToolsUsed, _ := extraIdle["tools_used"].(string)
				extraReport, _ := extraIdle["completion_report"].(map[string]any)

				if extraSummary != "" && looksLikeToolJSON(extraSummary) {
					logger.Warn("Filtering tool JSON from coalesced idle result_summary",
						"agent_id", extraAgentID,
						"summary_length", len(extraSummary),
					)
					extraSummary = ""
				}

				// Update agentStates for each drained signal (Codex finding #3)
				// 为每个排空出的信号更新 agentStates（Codex 发现 #3）
				if s, ok := agentStates[extraAgentID]; ok {
					s.Status = "idle"
					s.IterationsUsed = int(extraIter)
					// Populate FilesWritten from completion_report (mirrors primary idle handler)
					// 用 completion_report 填充 FilesWritten（与主 idle 处理一致）
					if extraReport != nil {
						if filesRaw, ok := extraReport["files_written"]; ok {
							if files, ok := filesRaw.([]string); ok {
								s.FilesWritten = files
							} else if filesArr, ok := filesRaw.([]any); ok {
								for _, f := range filesArr {
									if fs, ok := f.(string); ok {
										s.FilesWritten = append(s.FilesWritten, fs)
									}
								}
							}
						}
					}
					agentStates[extraAgentID] = s
				}
				// Merge coalesced agent's team knowledge into global accumulator (L2 dedup)
				// 把被合并 agent 的团队知识并入全局累加器（L2 去重）
				if tkRaw, ok := extraIdle["team_knowledge"]; ok {
					if tkSlice, ok := tkRaw.([]any); ok {
						var agentTK []activities.TeamKnowledgeEntry
						for _, item := range tkSlice {
							if m, ok := item.(map[string]any); ok {
								agentTK = append(agentTK, activities.TeamKnowledgeEntry{
									URL:       fmt.Sprintf("%v", m["url"]),
									Agent:     fmt.Sprintf("%v", m["agent"]),
									Summary:   fmt.Sprintf("%v", m["summary"]),
									CharCount: int(toFloat64(m["char_count"])),
								})
							}
						}
						if len(agentTK) > 0 {
							swarmTeamKnowledge = mergeTeamKnowledge(swarmTeamKnowledge, agentTK)
							logger.Info("Merged team knowledge from coalesced idle agent",
								"agent_id", extraAgentID,
								"new_entries", len(agentTK),
								"total_entries", len(swarmTeamKnowledge),
							)
						}
					}
				}
				// Mark coalesced agent's task as completed in TaskList (mirrors primary idle handler)
				// 在 TaskList 中把被合并 agent 的 task 标记为 completed（与主 idle 处理一致）
				if taskID, ok := agentTaskMap[extraAgentID]; ok && taskID != "" {
					_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
						WorkflowID: workflowID,
						TaskID:     taskID,
						Status:     "completed",
						AgentID:    extraAgentID,
					}).Get(ctx, nil)
					delete(agentTaskMap, extraAgentID)
				}
				if extraSummary != "" {
					idleSnapshots[extraAgentID] = AgentLoopResult{
						AgentID:    extraAgentID,
						Response:   extraSummary,
						Iterations: int(extraIter),
						Success:    true,
						ToolsUsed:  extraToolsUsed,
					}
				}

				// Use first agent's completion report if primary doesn't have one
				// 主 agent 没有完成报告时用第一个 agent 的报告
				if event.CompletionReport == nil && extraReport != nil {
					event.CompletionReport = extraReport
				}

				// Append per-agent detail to event summary (Codex finding #7)
				// 把逐 agent 细节追加到事件摘要（Codex 发现 #7）
				event.ResultSummary = fmt.Sprintf("%s\n---\n%s: %s",
					event.ResultSummary, extraAgentID, truncateDecisionSummary(extraSummary))
				event.AgentID = fmt.Sprintf("%s+%s", event.AgentID, extraAgentID)

				logger.Info("Coalesced idle signal", "agent_id", extraAgentID)
			}
		}

		// Budget check (D9) — force synthesis if exhausted
		// budget 检查（D9）——耗尽则强制合成
		elapsed = int(workflow.Now(ctx).Sub(swarmStartTime).Seconds())
		if budgetTotalLLMCalls >= maxLLMCalls || budgetTotalTokens >= maxTokens || elapsed >= maxWallClockSeconds {
			logger.Warn("Budget exhausted, forcing synthesis",
				"llm_calls", budgetTotalLLMCalls,
				"tokens", budgetTotalTokens,
				"elapsed_seconds", elapsed,
			)
			break
		}

		// Skip Lead LLM call if all agents are already done
		// 所有 agent 都已完成时跳过 Lead LLM 调用
		if completedCount >= totalExpected {
			break
		}

		// Skip Lead LLM call on checkpoint when there's nothing actionable
		// checkpoint 上没有可执行事项时跳过 Lead LLM 调用
		if event.Type == "checkpoint" {
			hasIdleAgent := false
			hasRunningAgent := false
			for _, s := range agentStates {
				switch s.Status {
				case "idle":
					hasIdleAgent = true
				case "running":
					hasRunningAgent = true
				}
			}
			// Quick check TaskList for pending tasks (without LLM call)
			// 快速检查 TaskList 是否有 pending task（不调 LLM）
			var checkTaskList []activities.SwarmTask
			_ = workflow.ExecuteActivity(p2pCtx, constants.GetTaskListActivity, activities.GetTaskListInput{
				WorkflowID: workflowID,
			}).Get(ctx, &checkTaskList)
			// B2: Only count unblocked pending tasks as actionable (replay-safe)
			// B2：只把未被阻塞的 pending task 视为可执行（重放安全）
			checkpointDepsV := workflow.GetVersion(ctx, "checkpoint-deps-filter", workflow.DefaultVersion, 1)
			hasPendingTask := false
			if checkpointDepsV >= 1 {
				hasPendingTask = hasAssignablePendingTask(checkTaskList)
			} else {
				for _, t := range checkTaskList {
					if t.Status == "pending" {
						hasPendingTask = true
					}
				}
			}
			// Skip 1: all agents running, no idle to process, no pending tasks
			// 跳过情形 1：所有 agent 在运行、无 idle 待处理、无 pending task
			if !hasIdleAgent && !hasPendingTask {
				logger.Info("Checkpoint: no idle agents, no pending tasks — skipping Lead LLM call")
				continue
			}
			// Terminal state: all agents idle, none running, no pending tasks.
			// Lead already had its chance during agent_idle events — force wrap-up.
			// EXCEPTION: if there's an unresolved HITL message, give Lead another chance (max 2 rounds).
			// 终态：所有 agent 空闲、无运行中 task、无待办 task——Lead 已在 agent_idle event 中有过机会，
			// 此处强制收尾；例外：存在未处理的 HITL 消息时再给 Lead 一次机会（最多 2 轮）。
			if hasIdleAgent && !hasRunningAgent && !hasPendingTask {
				if hitlPendingAction && hitlReminderCount < 2 {
					hitlReminderCount++
					logger.Info("Terminal state but HITL pending — giving Lead another chance",
						"reminder", hitlReminderCount)
					// Fall through to normal LeadDecision below
					// 落到下面正常的 LeadDecision 流程
				} else {
					logger.Info("Terminal state: all agents idle with no pending tasks — auto-completing")
					// Shutdown all non-completed agents
					// 关停所有未完成的 agent
					for _, af := range agentFutures {
						if af.WorkflowID != "" {
							if s, ok := agentStates[af.ID]; ok && s.Status == "completed" {
								continue
							}
							_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "",
								fmt.Sprintf("agent:%s:shutdown", af.ID), "Auto-complete",
							).Get(ctx, nil)
							// Emit AGENT_COMPLETED so frontend can remove radar flight
							// 发出 AGENT_COMPLETED，让前端移除雷达航班
							_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
								WorkflowID: workflowID,
								EventType:  activities.StreamEventAgentCompleted,
								AgentID:    af.ID,
								Message:    activities.MsgAgentCompleted(af.ID),
								Timestamp:  workflow.Now(ctx),
							}).Get(ctx, nil)
						}
					}
					// Populate results from idle snapshots (agents already did the work)
					// 用 idle 快照填充结果（agent 已完成工作）
					for agentID, snap := range idleSnapshots {
						if _, exists := results[agentID]; !exists {
							results[agentID] = snap
							logger.Info("Using idle snapshot for agent result", "agent_id", agentID)
						}
					}
					goto synthesis
				}
			}
		}

		// Fetch current TaskList
		// 拉取当前 TaskList
		var currentTaskList []activities.SwarmTask
		_ = workflow.ExecuteActivity(p2pCtx, constants.GetTaskListActivity, activities.GetTaskListInput{
			WorkflowID: workflowID,
		}).Get(ctx, &currentTaskList)

		// Collect agent states — inject elapsed time for running agents
		// 收集 agent 状态——为运行中的 agent 注入已耗时
		now := workflow.Now(ctx)
		var statesList []activities.LeadAgentState
		for _, s := range agentStates {
			if s.Status == "running" {
				if startTime, ok := agentTaskStartTimes[s.AgentID]; ok {
					s.ElapsedSeconds = int(now.Sub(startTime).Seconds())
				}
			}
			statesList = append(statesList, s)
		}

		// Read Lead's mailbox — agents can send_message(to="lead") to escalate issues
		// 读取 Lead 的 mailbox——agent 可用 send_message(to="lead") 上报问题
		var leadMessages []activities.AgentMessage
		leadMailboxErr := workflow.ExecuteActivity(p2pCtx, constants.FetchAgentMessagesActivity,
			activities.FetchAgentMessagesInput{
				WorkflowID: workflowID,
				AgentID:    "lead",
				SinceSeq:   lastLeadMailboxSeq,
			}).Get(ctx, &leadMessages)

		var leadMsgMaps []map[string]any
		if leadMailboxErr == nil {
			for _, m := range leadMessages {
				leadMsgMaps = append(leadMsgMaps, map[string]any{
					"from":    m.From,
					"type":    string(m.Type),
					"payload": m.Payload,
				})
				if m.Seq > lastLeadMailboxSeq {
					lastLeadMailboxSeq = m.Seq
				}
			}
		}

		// Collect workspace file paths for Lead context — scan DISK so multi-turn
		// conversations see files from previous swarm runs, not just current run.
		// 扫描磁盘收集工作区文件路径，使多轮 session 能看到此前 swarm 运行留下的文件，而不只是本轮产物。
		var wsFileList []string
		{
			var diskFiles activities.ListWorkspaceFilesResult
			_ = workflow.ExecuteActivity(fileReadCtx, constants.ListWorkspaceFilesActivity,
				activities.ListWorkspaceFilesInput{SessionID: input.SessionID},
			).Get(ctx, &diskFiles)
			seen := make(map[string]bool)
			for _, f := range diskFiles.Files {
				if !seen[f.Path] {
					wsFileList = append(wsFileList, f.Path)
					seen[f.Path] = true
				}
			}
			// Merge agent-reported files (in case disk scan missed in-flight writes)
			// 合并 agent 上报的文件（防止磁盘扫描漏掉在途写入）
			for _, s := range agentStates {
				for _, f := range s.FilesWritten {
					if !seen[f] {
						wsFileList = append(wsFileList, f)
						seen[f] = true
					}
				}
			}
		}

		// Lead decision with optional file_read / tool_call inner loop.
		// When Lead returns file_read or tool_call actions, Go executes them (zero LLM cost
		// for file_read; direct tool invocation for tool_call) and calls LeadDecision again
		// with results injected into the event.
		// Lead 决策内循环：当 Lead 返回 file_read 或 tool_call 时由 Go 直接执行
		// （file_read 零 LLM 成本，tool_call 直接调 tool），再把结果注入 event 后重新调用 LeadDecision。
		const maxFileReadRounds = 3
		var decision activities.LeadDecisionResult
		var leadDecisionOK bool
		fileReadRound := 0
		toolCallRound := 0
		for fileReadRound <= maxFileReadRounds || toolCallRound < maxToolCallRounds {
			// Refresh elapsed time each round so Lead gets accurate budget info
			// 每轮刷新已耗时，让 Lead 拿到准确的 budget 信息
			elapsed = int(workflow.Now(ctx).Sub(swarmStartTime).Seconds())
			// Call Lead LLM for decision (D2: Temporal Activity ensures replay determinism)
			// 调用 Lead LLM 做决策（D2：Temporal Activity 保证重放确定性）
			leadErr := workflow.ExecuteActivity(leadCtx, constants.LeadDecisionActivity, activities.LeadDecisionInput{
				WorkflowID:           workflowID,
				Event:                event,
				TaskList:             currentTaskList,
				AgentStates:          statesList,
				Budget:               leadBudget(elapsed),
				History:              leadHistory,
				Messages:             leadMsgMaps,
				OriginalQuery:        input.Query,
				ConversationHistory:  conversationHistory,
				WorkspaceFiles:       wsFileList,
				HitlMessages:         hitlMessages,
				LeadModelOverride:    leadModelOverride,
				LeadProviderOverride: leadProviderOverride,
			}).Get(ctx, &decision)

			if leadErr != nil {
				logger.Warn("Lead decision failed, continuing with checkpoint", "error", leadErr)
				break
			}

			// Track Lead decision in history
			// 在历史中记录 Lead 决策
			budgetTotalLLMCalls++
			budgetTotalTokens += cacheAwareBudget(decision.TokensUsed, decision.CacheReadTokens, decision.CacheCreationTokens)

			// Record Lead decision token usage
			// 记录 Lead 决策 token 用量
			recordLeadTokens("lead_decision", decision)

			leadHistory = append(leadHistory, map[string]any{
				"decision_summary": decision.DecisionSummary,
				"event":            event.Type,
				"actions":          len(decision.Actions),
			})
			// Truncate leadHistory to prevent unbounded growth in long-running swarms.
			// Keep last 20 entries (up from 5) for better Lead decision context.
			// 截断 leadHistory 防止长跑 swarm 无限增长：保留最近 20 条（原为 5 条）以给 Lead 更多决策 context。
			const maxLeadHistory = 20
			if len(leadHistory) > maxLeadHistory {
				leadHistory = leadHistory[len(leadHistory)-maxLeadHistory:]
			}

			// Classify actions into file I/O, tool_call, and others (M1: include file_list)
			// 把动作分为文件 I/O、tool_call 与其他（M1：包含 file_list）
			var fileIOActions []activities.LeadAction
			var toolCallActions []activities.LeadAction
			var otherActions []activities.LeadAction
			for _, action := range decision.Actions {
				if action.Type == "file_read" || action.Type == "file_list" {
					fileIOActions = append(fileIOActions, action)
				} else if leadToolCallV >= 1 && action.Type == "tool_call" {
					toolCallActions = append(toolCallActions, action)
				} else {
					otherActions = append(otherActions, action)
				}
			}

			// Execute file I/O AND tool_call in the same round (M3: no mutual exclusion)
			// 同一轮内同时执行文件 I/O 与 tool_call（M3：互不排斥）
			ranFileIO := false
			ranToolCalls := false

			// File I/O: file_read + file_list (zero LLM cost)
			// 文件 I/O：file_read + file_list（零 LLM 成本）
			if len(fileIOActions) > 0 && fileReadRound < maxFileReadRounds {
				fileReadRound++
				ranFileIO = true
				event.FileContents = runLeadFileIO(ctx, fileReadCtx, fileIOActions, 4000)
			}

			// Tool calls (zero LLM cost — direct tool invocation)
			// tool 调用（零 LLM 成本——直接调用 tool）
			if len(toolCallActions) > 0 && toolCallRound < maxToolCallRounds {
				toolCallRound++
				ranToolCalls = true
				var toolResults []activities.ToolResultEntry
				for _, tc := range toolCallActions {
					if !allowedLeadTools[tc.Tool] {
						logger.Warn("Lead requested disallowed tool, skipping", "tool", tc.Tool)
						toolResults = append(toolResults, activities.ToolResultEntry{
							Tool: tc.Tool, Error: fmt.Sprintf("tool %q not in allowlist", tc.Tool),
						})
						continue
					}
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventLeadToolCall,
						AgentID:    "swarm-lead",
						Message:    fmt.Sprintf("Lead executing %s", tc.Tool),
						Payload: map[string]any{
							"tool": tc.Tool, "phase": "lead_tool_call",
						},
						Timestamp: workflow.Now(ctx),
					}).Get(ctx, nil)
					var toolResult activities.LeadToolResult
					toolErr := workflow.ExecuteActivity(toolCtx, constants.LeadExecuteToolActivity,
						activities.LeadToolInput{
							Tool: tc.Tool, ToolParams: tc.ToolParams, SessionID: input.SessionID,
						}).Get(ctx, &toolResult)
					if toolErr != nil {
						toolResults = append(toolResults, activities.ToolResultEntry{Tool: tc.Tool, Error: toolErr.Error()})
					} else {
						output := toolResult.Output
						maxChars := 4000
						if tc.Tool == "calculator" {
							maxChars = 500
						}
						if len(output) > maxChars {
							output = output[:maxChars] + "\n... [truncated]"
						}
						toolResults = append(toolResults, activities.ToolResultEntry{
							Tool: tc.Tool, Output: output, Error: toolResult.Error,
						})
					}
					logger.Info("Lead tool_call executed", "tool", tc.Tool, "round", toolCallRound)
				}
				event.ToolResults = toolResults
			}

			// M2: clear stale fields — if only one type ran, nil out the other
			// M2：清理过期字段——只执行了一类动作时把另一类置 nil
			if ranFileIO && !ranToolCalls {
				event.ToolResults = nil
			}
			if ranToolCalls && !ranFileIO {
				event.FileContents = nil
			}

			// If any I/O was executed, loop back for another LeadDecision
			// 执行过任何 I/O 就回到循环再调一次 LeadDecision
			if ranFileIO || ranToolCalls {
				logger.Info("Lead I/O round complete, calling LeadDecision again",
					"fileReadRound", fileReadRound, "toolCallRound", toolCallRound)
				continue
			}

			// No file_read/file_list/tool_call actions (or max rounds reached) — replace decision.Actions
			// 无 file_read/file_list/tool_call 动作（或已达最大轮数）——把 decision.Actions
			// with otherActions (file_reads/tool_calls already handled) if we had any
			// 换成 otherActions（file_read/tool_call 已处理完），前提是存在这些动作
			if len(fileIOActions) > 0 || len(toolCallActions) > 0 {
				decision.Actions = otherActions
			}
			leadDecisionOK = true
			break // Exit file_read loop, proceed to action processing 注：退出 file_read 循环，进入动作处理
		}

		if !leadDecisionOK {
			continue // Lead decision failed or only had file_reads — wait for next event 注：Lead 决策失败或仅有 file_read——等待下一个事件
		}

		logger.Info("Lead decision",
			"summary", decision.DecisionSummary,
			"actions", len(decision.Actions),
		)

		// Emit LEAD_DECISION for Radar visualization (gold pulse)
		// 发出 LEAD_DECISION 供 Radar 可视化（金色脉冲）
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLeadDecision,
			AgentID:    "swarm-lead",
			Message:    leadDisplayMessage(decision),
			Payload: map[string]any{
				"event_type": event.Type,
				"actions":    len(decision.Actions),
			},
		}).Get(ctx, nil)

		// Emit HITL_RESPONSE when Lead processes a human_input event
		// Lead 处理 human_input 事件时发出 HITL_RESPONSE
		if event.Type == "human_input" {
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventHITLResponse,
				AgentID:    "swarm-lead",
				Message:    leadDisplayMessage(decision),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
		}

		// Execute Lead actions
		// 执行 Lead 的动作
		for _, action := range decision.Actions {
			switch action.Type {
			case "spawn_agent":
				hitlPendingAction = false // Lead acted on user request 注：Lead 已就用户请求采取动作
				if strings.TrimSpace(action.TaskDescription) == "" {
					logger.Warn("Lead spawn ignored: empty task description", "role", action.Role)
					continue
				}
				if spawnCount >= cfg.SwarmMaxAgents {
					logger.Warn("Lead spawn rejected: at capacity", "requested", action.TaskDescription)
					continue
				}
				// Dependency guard
				// 依赖守卫
				if action.TaskID != "" && taskHasUnmetDeps(action.TaskID, currentTaskList) {
					// Debug: log all task statuses for dependency diagnosis
					// 调试：打印所有 task 状态以诊断依赖
					var depDebug []string
					for _, t := range currentTaskList {
						depDebug = append(depDebug, fmt.Sprintf("%s=%s", t.ID, t.Status))
					}
					logger.Warn("Lead spawn rejected: task has unmet dependencies",
						"task_id", action.TaskID, "role", action.Role,
						"task_statuses", strings.Join(depDebug, ", "))
					continue
				}
				prevCount := spawnCount
				spawnAgent(action.TaskDescription, action.Role, action.ModelTier, action.TaskID, action.SkipAttachments)
				if spawnCount > prevCount {
					totalExpected++
					// Auto-link: if Lead omitted task_id, try to find the unique orphaned task
					// 自动关联：Lead 漏写 task_id 时，尝试找到唯一的孤儿 task
					if autoLinkV == 1 && action.TaskID == "" {
						newAgent := agentFutures[len(agentFutures)-1].ID
						if _, ok := agentTaskMap[newAgent]; !ok {
							claimed := make(map[string]bool)
							for _, tid := range agentTaskMap {
								claimed[tid] = true
							}
							var candidates []string
							for _, t := range currentTaskList {
								if t.Status == "pending" && t.Owner == "" && !claimed[t.ID] && !taskHasUnmetDeps(t.ID, currentTaskList) {
									candidates = append(candidates, t.ID)
								}
							}
							if len(candidates) == 1 {
								agentTaskMap[newAgent] = candidates[0]
								_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity,
									activities.UpdateTaskStatusInput{WorkflowID: workflowID, TaskID: candidates[0], Status: "in_progress", AgentID: newAgent}).Get(ctx, nil)
								logger.Info("Auto-linked spawned agent (main loop)", "agent_id", newAgent, "task_id", candidates[0])
							} else {
								logger.Warn("Spawned agent without task_id, cannot auto-link",
									"agent_id", newAgent, "candidates", len(candidates))
							}
						}
					}
				}

			case "assign_task":
				hitlPendingAction = false // Lead acted on user request 注：Lead 已就用户请求采取动作
				if action.AgentID != "" {
					// Dependency guard
					// 依赖守卫
					if action.TaskID != "" && taskHasUnmetDeps(action.TaskID, currentTaskList) {
						logger.Warn("Lead assign_task rejected: task has unmet dependencies",
							"task_id", action.TaskID, "agent_id", action.AgentID)
						break
					}
					taskDesc := action.TaskDescription
					if taskDesc == "" {
						for _, t := range currentTaskList {
							if t.ID == action.TaskID {
								taskDesc = t.Description
								break
							}
						}
					}

					// P1 fix: inject previous work context + tools-used summary
					// so agent skips re-orientation and doesn't repeat tool calls
					// 注入此前的工作 context 与已用 tool 摘要，让重新派活的 agent 免去重新摸索、避免重复 tool 调用。
					if snap, ok := idleSnapshots[action.AgentID]; ok && snap.Response != "" {
						prevContext := snap.Response
						if len(prevContext) > 1500 {
							prevContext = prevContext[:1500] + "..."
						}
						if snap.ToolsUsed != "" {
							taskDesc = fmt.Sprintf("PREVIOUS WORK CONTEXT:\n%s\n\nTOOLS ALREADY USED (do NOT repeat these):\n%s\n\nNEW INSTRUCTIONS:\n%s",
								prevContext, snap.ToolsUsed, taskDesc)
						} else {
							taskDesc = fmt.Sprintf("PREVIOUS WORK CONTEXT:\n%s\n\nNEW INSTRUCTIONS:\n%s",
								prevContext, taskDesc)
						}
					}

					// Inject workspace file registry so reassigned agent knows what files exist
					// 注入工作区文件清单，让重新派活的 agent 知道有哪些文件
					var fileEntries []activities.FileRegistryEntry
					fileRegErr := workflow.ExecuteActivity(p2pCtx, constants.GetFileRegistryActivity,
						activities.GetFileRegistryInput{WorkflowID: workflowID}).Get(ctx, &fileEntries)
					if fileRegErr == nil && len(fileEntries) > 0 {
						var fileList strings.Builder
						fileList.WriteString("\n\nWORKSPACE FILES (already created by team):\n")
						for _, f := range fileEntries {
							fmt.Fprintf(&fileList, "  %s (%d bytes, by %s)\n", f.Path, f.Size, f.Author)
						}
						taskDesc += fileList.String()
					}

					if action.TaskID != "" {
						_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
							WorkflowID: workflowID,
							TaskID:     action.TaskID,
							Status:     "in_progress",
							AgentID:    action.AgentID,
						}).Get(ctx, nil)
						agentTaskMap[action.AgentID] = action.TaskID
					}

					assignPayload := map[string]string{
						"description": taskDesc,
					}
					if action.ModelTier != "" {
						assignPayload["model_tier"] = action.ModelTier
					}
					for _, af := range agentFutures {
						if af.ID == action.AgentID && af.WorkflowID != "" {
							_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "", fmt.Sprintf("agent:%s:new-task", action.AgentID), assignPayload).Get(ctx, nil)
							break
						}
					}

					// Notify frontend that idle agent is active again
					// 通知前端该空闲 agent 已重新活跃
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventAgentStarted,
						AgentID:    action.AgentID,
						Message:    fmt.Sprintf("%s got a new task", action.AgentID),
						Payload: map[string]any{
							"role": action.Role,
						},
					}).Get(ctx, nil)

					// Preserve IterationsUsed from prior work (read-modify-write)
					// 保留此前工作的 IterationsUsed（读-改-写）
					if s, ok := agentStates[action.AgentID]; ok {
						s.Status = "running"
						s.CurrentTask = taskDesc
						agentStates[action.AgentID] = s
					} else {
						agentStates[action.AgentID] = activities.LeadAgentState{
							AgentID:     action.AgentID,
							Status:      "running",
							CurrentTask: taskDesc,
						}
					}
					agentTaskStartTimes[action.AgentID] = workflow.Now(ctx)
					logger.Info("Lead assigned task to idle agent",
						"agent_id", action.AgentID,
						"task_id", action.TaskID,
						"task", taskDesc,
					)
				}

			case "send_message":
				if swarmP2pV >= 1 {
					_ = workflow.ExecuteActivity(p2pCtx, constants.SendAgentMessageActivity, activities.SendAgentMessageInput{
						WorkflowID: workflowID,
						From:       "lead",
						To:         action.To,
						Type:       activities.MessageTypeInfo,
						Payload:    map[string]any{"message": action.Content},
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
				}
				// Wake idle recipient so it sees the Lead message immediately
				// 唤醒空闲接收方，让它立即看到 Lead 的消息
				if s, ok := agentStates[action.To]; ok && s.Status == "idle" {
					for _, af := range agentFutures {
						if af.ID == action.To && af.WorkflowID != "" {
							_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "",
								fmt.Sprintf("agent:%s:peer-message", action.To),
								map[string]string{"from": "lead"},
							).Get(ctx, nil)
							break
						}
					}
				}

			case "broadcast":
				broadcastIDs := slices.Sorted(maps.Keys(agentStates))
				for _, id := range broadcastIDs {
					s := agentStates[id]
					if s.Status == "running" || s.Status == "idle" {
						if swarmP2pV >= 1 {
							_ = workflow.ExecuteActivity(p2pCtx, constants.SendAgentMessageActivity, activities.SendAgentMessageInput{
								WorkflowID: workflowID,
								From:       "lead",
								To:         s.AgentID,
								Type:       activities.MessageTypeInfo,
								Payload:    map[string]any{"message": action.Content},
								Timestamp:  workflow.Now(ctx),
							}).Get(ctx, nil)
						}
						// Wake idle agents so they see the broadcast immediately
						// 唤醒空闲 agent，让它们立即看到广播
						if s.Status == "idle" {
							for _, af := range agentFutures {
								if af.ID == id && af.WorkflowID != "" {
									_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "",
										fmt.Sprintf("agent:%s:peer-message", id),
										map[string]string{"from": "lead"},
									).Get(ctx, nil)
									break
								}
							}
						}
					}
				}

			case "revise_plan":
				hitlPendingAction = false // Lead acted on user request 注：Lead 已就用户请求采取动作
				for _, newTask := range action.Create {
					task := activities.SwarmTask{
						ID:          fmt.Sprintf("%v", newTask["id"]),
						Description: fmt.Sprintf("%v", newTask["description"]),
						CreatedBy:   "lead",
					}
					// Parse depends_on for task dependency enforcement (matches initial planning)
					// 解析 depends_on 以执行 task 依赖约束（与初始规划一致）
					if deps, ok := newTask["depends_on"].([]any); ok {
						for _, d := range deps {
							task.DependsOn = append(task.DependsOn, fmt.Sprintf("%v", d))
						}
					}
					_ = workflow.ExecuteActivity(p2pCtx, constants.CreateTaskActivity, activities.CreateTaskInput{
						WorkflowID: workflowID,
						Task:       task,
					}).Get(ctx, nil)
				}
				for _, cancelID := range action.Cancel {
					_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
						WorkflowID: workflowID,
						TaskID:     cancelID,
						Status:     "completed",
						AgentID:    "lead",
					}).Get(ctx, nil)
				}
				// Update existing task descriptions
				// 更新已有 task 的描述
				// Version-gated: this activity call didn't exist in older workflows.
				// 受版本闸门保护：旧 workflow 中没有这个 activity 调用。
				phase2DescV := workflow.GetVersion(ctx, "swarm_phase2_task_desc_update_v1", workflow.DefaultVersion, 1)
				for _, upd := range action.Update {
					taskID := fmt.Sprintf("%v", upd["id"])
					desc, _ := upd["description"].(string)
					if taskID != "" && desc != "" {
						if phase2DescV >= 1 {
							_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskDescriptionActivity, activities.UpdateTaskDescriptionInput{
								WorkflowID:  workflowID,
								TaskID:      taskID,
								Description: desc,
							}).Get(ctx, nil)
						}
						// Sync in-memory currentTaskList (always, regardless of version)
						// 同步内存中的 currentTaskList（无论版本，始终执行）
						for i := range currentTaskList {
							if currentTaskList[i].ID == taskID {
								currentTaskList[i].Description = desc
								break
							}
						}
					}
				}

			case "shutdown_agent":
				if action.AgentID != "" {
					for _, af := range agentFutures {
						if af.ID == action.AgentID && af.WorkflowID != "" {
							_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "",
								fmt.Sprintf("agent:%s:shutdown", action.AgentID),
								"Lead shutdown",
							).Get(ctx, nil)
							break
						}
					}
					if s, ok := agentStates[action.AgentID]; ok {
						s.Status = "completed"
						agentStates[action.AgentID] = s
					}
					if taskID, ok := agentTaskMap[action.AgentID]; ok && taskID != "" {
						_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity,
							activities.UpdateTaskStatusInput{
								WorkflowID: workflowID,
								TaskID:     taskID,
								Status:     "completed",
								AgentID:    action.AgentID,
							}).Get(ctx, nil)
						delete(agentTaskMap, action.AgentID)
						// Sync in-memory currentTaskList so subsequent spawn_agent
						// 同步内存中的 currentTaskList，让后续 spawn_agent
						// dependency checks see the updated status (fixes stale snapshot race)
						// 的依赖检查看到最新状态（修复快照过期竞态）
						for i := range currentTaskList {
							if currentTaskList[i].ID == taskID {
								currentTaskList[i].Status = "completed"
								break
							}
						}
					}
					// Emit AGENT_COMPLETED so frontend can remove radar flight
					// 发出 AGENT_COMPLETED，让前端移除雷达航班
					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventAgentCompleted,
						AgentID:    action.AgentID,
						Message:    activities.MsgAgentCompleted(action.AgentID),
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)
					logger.Info("Lead shutdown individual agent", "agent_id", action.AgentID)
				}

			case "interim_reply":
				if action.Content != "" {
					now := workflow.Now(ctx)
					if lastInterimEmitTime.IsZero() || now.Sub(lastInterimEmitTime).Seconds() >= float64(interimThrottleSeconds) {
						lastInterimEmitTime = now
						_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
							WorkflowID: workflowID,
							EventType:  activities.StreamEventLLMOutput,
							AgentID:    "swarm-lead",
							Message:    action.Content,
							Payload: map[string]any{
								"role":    "lead",
								"interim": true,
							},
							Timestamp: now,
						}).Get(ctx, nil)
					} else {
						logger.Info("Throttled interim_reply (too frequent)",
							"seconds_since_last", int(now.Sub(lastInterimEmitTime).Seconds()))
					}
				}

			case "noop":
				// Lead explicitly chose to wait — no action needed this round.
				// Lead 明确选择等待——本轮无需动作。
				logger.Info("Lead chose noop — waiting for running agents")

			case "done", "reply":
				// Guard: reject premature "done"/"reply" if any agent is still running.
				// LLM sometimes returns done/reply action while its decision_summary says "wait".
				// 防呆：仍有 agent 在运行时拒绝过早的 "done"/"reply"——LLM 有时 decision_summary 说"等待"却返回 done。
				hasRunning := false
				for _, s := range agentStates {
					if s.Status == "running" {
						hasRunning = true
						break
					}
				}
				if hasRunning {
					logger.Warn("Lead said done but agents still running — ignoring premature done",
						"running_agents", func() []string {
							var ids []string
							for _, s := range agentStates {
								if s.Status == "running" {
									ids = append(ids, s.AgentID)
								}
							}
							return ids
						}(),
					)
					continue // Skip done, wait for agents to finish 注：跳过 done，等待 agent 执行完
				}

				logger.Info("Lead decided: all tasks complete, shutting down agents")
				// Gracefully shutdown remaining agents (skip already-completed ones)
				// 优雅关停剩余 agent（跳过已完成的）
				for _, af := range agentFutures {
					if af.WorkflowID != "" {
						if s, ok := agentStates[af.ID]; ok && s.Status == "completed" {
							continue // Already shut down via shutdown_agent 注：已通过 shutdown_agent 关停
						}
						_ = workflow.SignalExternalWorkflow(ctx, af.WorkflowID, "",
							fmt.Sprintf("agent:%s:shutdown", af.ID), "Lead completed",
						).Get(ctx, nil)
						// Emit AGENT_COMPLETED so frontend can remove radar flight
						// 发出 AGENT_COMPLETED，让前端移除雷达航班
						_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
							WorkflowID: workflowID,
							EventType:  activities.StreamEventAgentCompleted,
							AgentID:    af.ID,
							Message:    activities.MsgAgentCompleted(af.ID),
							Timestamp:  workflow.Now(ctx),
						}).Get(ctx, nil)
					}
				}
				// Drain remaining results with timeout
				// 带超时排空剩余结果
				drainTimer := workflow.NewTimer(ctx, 60*time.Second)
				for completedCount < totalExpected {
					drainSel := workflow.NewSelector(ctx)
					drained := false
					drainSel.AddReceive(completionCh, func(ch workflow.ReceiveChannel, more bool) {
						var r AgentLoopResult
						ch.Receive(ctx, &r)
						results[r.AgentID] = r
						completedCount++
						// Sync TaskList (mirror of main loop logic)
						// 同步 TaskList（与主循环逻辑一致）
						if taskID, ok := agentTaskMap[r.AgentID]; ok && taskID != "" {
							_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
								WorkflowID: workflowID,
								TaskID:     taskID,
								Status:     "completed",
								AgentID:    r.AgentID,
							}).Get(ctx, nil)
							delete(agentTaskMap, r.AgentID)
						}
					})
					drainSel.AddReceive(agentIdleCh, func(ch workflow.ReceiveChannel, more bool) {
						var m map[string]any
						ch.Receive(ctx, &m) // discard 注：丢弃
					})
					drainSel.AddFuture(drainTimer, func(f workflow.Future) {
						_ = f.Get(ctx, nil)
						drained = true
					})
					drainSel.Select(ctx)
					if drained {
						break
					}
				}
				goto synthesis
			}
		}

		// ContinueAsNew check (D6): prevent Temporal event limit
		// ContinueAsNew 检查（D6）：防止触发 Temporal event 数量上限
		if workflow.GetInfo(ctx).GetCurrentHistoryLength() > 8000 {
			logger.Warn("ContinueAsNew triggered: event count high, synthesizing partial results",
				"events", workflow.GetInfo(ctx).GetCurrentHistoryLength(),
			)
			// For now, break and synthesize — full ContinueAsNew with snapshot in Phase 3.4
			// ContinueAsNew 检查：防止触发 Temporal event 数量上限；当前先跳出并合成。
			historyTruncated = true
			break
		}
	}

synthesis:

	// Cleanup: mark any remaining pending/in_progress tasks as completed
	// 收尾：把仍为 pending/in_progress 的 task 全部标记为 completed，
	// so the TaskList doesn't have stale entries in Redis.
	// 避免 TaskList 在 Redis 中留下残留条目。
	{
		var remainingTasks []activities.SwarmTask
		_ = workflow.ExecuteActivity(p2pCtx, constants.GetTaskListActivity, activities.GetTaskListInput{
			WorkflowID: workflowID,
		}).Get(ctx, &remainingTasks)
		for _, t := range remainingTasks {
			if t.Status == "pending" || t.Status == "in_progress" {
				_ = workflow.ExecuteActivity(p2pCtx, constants.UpdateTaskStatusActivity, activities.UpdateTaskStatusInput{
					WorkflowID: workflowID,
					TaskID:     t.ID,
					Status:     "completed",
					AgentID:    "lead",
				}).Get(ctx, nil)
			}
		}
	}

	// Phase 4: Synthesize results
	// 阶段 4：合成结果
	logger.Info("SwarmWorkflow all agents completed", "result_count", len(results))

	// Log cache stats across all agents
	// 记录所有 agent 的 cache 统计
	{
		var swarmCacheRead, swarmCacheCreation, swarmTotalInput int
		for _, r := range results {
			swarmCacheRead += r.CacheReadTokens
			swarmCacheCreation += r.CacheCreationTokens
			swarmTotalInput += r.InputTokens
		}
		if swarmCacheRead > 0 || swarmCacheCreation > 0 {
			logger.Info("Prompt cache stats",
				"workflow_id", workflowID,
				"cache_read_tokens", swarmCacheRead,
				"cache_creation_tokens", swarmCacheCreation,
				"total_input_tokens", swarmTotalInput,
			)
		}
	}

	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventProgress,
		AgentID:    "swarm-supervisor",
		Message:    activities.MsgSwarmSynthesizing(len(results)),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// Convert AgentLoopResults to AgentExecutionResults for synthesis
	// 把 AgentLoopResults 转成 AgentExecutionResults 供合成使用
	var agentResults []activities.AgentExecutionResult
	var totalTokensUsed int
	synthIDs := slices.Sorted(maps.Keys(results))
	for _, id := range synthIDs {
		r := results[id]
		agentResults = append(agentResults, activities.AgentExecutionResult{
			AgentID:      r.AgentID,
			Role:         r.Role,
			Response:     r.Response,
			TokensUsed:   r.TokensUsed,
			InputTokens:  r.InputTokens,
			OutputTokens: r.OutputTokens,
			ModelUsed:    r.ModelUsed,
			Provider:     r.Provider,
			Success:      r.Success,
			Error:        r.Error,
		})
		totalTokensUsed += r.TokensUsed
	}

	// Pre-synthesis guard: check if any agents produced usable results.
	// Agents that timed out or were cancelled may still have useful output.
	// 合成前检查是否有 agent 产出可用结果；timeout 或被取消的 agent 也可能留下有用输出。
	successCount := 0
	for i, r := range agentResults {
		if r.Success {
			successCount++
		} else if r.Response != "" {
			// Promote usable-but-failed agents for synthesis
			// 把失败但结果可用的 agent 提升到合成输入
			agentResults[i].Success = true
			successCount++
		}
	}
	if successCount == 0 {
		logger.Error("SwarmWorkflow all agents failed", "total_agents", len(agentResults))
		meta := buildSwarmMetadata(results)
		if historyTruncated {
			meta["truncated"] = true
			meta["truncation_reason"] = "temporal_history_limit"
		}
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("All %d agents failed — no results to synthesize", len(agentResults)),
			TokensUsed:   totalTokensUsed,
			Metadata:     meta,
		}, nil
	}

	// Note: single-agent results also go through closing_checkpoint below.
	// 注意：单 agent 结果也会走下面的 closing_checkpoint。
	// Agent idle summaries are internal status reports, not user-facing answers.
	// agent 的 idle 摘要是内部状态报告，不是给用户的答复。

	// --- Lead Closing Checkpoint (with file I/O + tool_call loop) ---
	// --- Lead 收尾检查点（含 file I/O + tool_call 循环）---
	// Read workspace files for the closing summary
	// 读取工作区文件用于收尾摘要
	var wsResult activities.ListWorkspaceFilesResult
	_ = workflow.ExecuteActivity(ctx, constants.ListWorkspaceFilesActivity, activities.ListWorkspaceFilesInput{
		SessionID: input.SessionID,
	}).Get(ctx, &wsResult)

	// Collect files written by agents in THIS run (to prioritize in closing summary)
	// 收集本次运行中 agent 写入的文件（在收尾摘要中优先展示）
	currentRunFiles := make(map[string]bool)
	for _, s := range agentStates {
		for _, f := range s.FilesWritten {
			currentRunFiles[f] = true
		}
	}

	closingSummary := buildClosingSummary(results, wsResult.Files, currentRunFiles)

	closingCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 150 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 2,
		},
	})

	const maxClosingRounds = 5
	var closingDecision activities.LeadDecisionResult
	var closingErr error
	closingEvent := activities.LeadEvent{
		Type:          "closing_checkpoint",
		ResultSummary: closingSummary,
	}

	for closingRound := 0; closingRound <= maxClosingRounds; closingRound++ {
		closingElapsed := int(workflow.Now(ctx).Sub(swarmStartTime).Seconds())

		closingErr = workflow.ExecuteActivity(closingCtx, constants.LeadDecisionActivity, activities.LeadDecisionInput{
			WorkflowID:           workflowID,
			Event:                closingEvent,
			TaskList:             make([]activities.SwarmTask, 0),
			AgentStates:          make([]activities.LeadAgentState, 0),
			Budget:               leadBudget(closingElapsed),
			History:              leadHistory,
			OriginalQuery:        input.Query,
			ConversationHistory:  conversationHistory,
			HitlMessages:         hitlMessages,
			LeadModelOverride:    leadModelOverride,
			LeadProviderOverride: leadProviderOverride,
		}).Get(ctx, &closingDecision)

		if closingErr != nil {
			logger.Warn("Lead closing_checkpoint failed, falling back to synthesis", "error", closingErr, "round", closingRound)
			break
		}

		budgetTotalLLMCalls++
		budgetTotalTokens += cacheAwareBudget(closingDecision.TokensUsed, closingDecision.CacheReadTokens, closingDecision.CacheCreationTokens)

		// Record Lead closing decision token usage
		// 记录 Lead 收尾决策 token 用量
		recordLeadTokens("closing_decision", closingDecision)

		leadHistory = append(leadHistory, map[string]any{
			"decision_summary": closingDecision.DecisionSummary,
			"event":            "closing_checkpoint",
			"actions":          len(closingDecision.Actions),
		})

		// Separate actions into file I/O, tool_call, and terminal actions
		// 把动作分为文件 I/O、tool_call 与终态动作
		var fileIOActions []activities.LeadAction
		var toolCallActions []activities.LeadAction
		var terminalActions []activities.LeadAction
		for _, action := range closingDecision.Actions {
			switch action.Type {
			case "file_read", "file_list":
				fileIOActions = append(fileIOActions, action)
			case "tool_call":
				if leadToolCallV >= 1 {
					toolCallActions = append(toolCallActions, action)
				}
			case "reply", "synthesize", "done":
				terminalActions = append(terminalActions, action)
			}
		}

		// Execute file I/O actions — zero LLM cost
		// 执行文件 I/O 动作——零 LLM 成本
		if len(fileIOActions) > 0 && closingRound < maxClosingRounds {
			closingEvent.FileContents = runLeadFileIO(ctx, fileReadCtx, fileIOActions, 8000)
			closingEvent.ToolResults = nil
			logger.Info("Lead closing file I/O complete, calling LeadDecision again",
				"round", closingRound, "files", len(closingEvent.FileContents))
			continue
		}

		// Execute tool_call actions
		// 执行 tool_call 动作
		if len(toolCallActions) > 0 && closingRound < maxClosingRounds {
			var toolResults []activities.ToolResultEntry
			for _, tc := range toolCallActions {
				if !allowedLeadTools[tc.Tool] {
					toolResults = append(toolResults, activities.ToolResultEntry{
						Tool: tc.Tool, Error: fmt.Sprintf("tool %q not in allowlist", tc.Tool),
					})
					continue
				}
				var toolResult activities.LeadToolResult
				toolErr := workflow.ExecuteActivity(toolCtx, constants.LeadExecuteToolActivity,
					activities.LeadToolInput{
						Tool: tc.Tool, ToolParams: tc.ToolParams, SessionID: input.SessionID,
					}).Get(ctx, &toolResult)
				if toolErr != nil {
					toolResults = append(toolResults, activities.ToolResultEntry{Tool: tc.Tool, Error: toolErr.Error()})
				} else {
					output := toolResult.Output
					if len(output) > 4000 {
						output = output[:4000] + "\n... [truncated]"
					}
					toolResults = append(toolResults, activities.ToolResultEntry{
						Tool: tc.Tool, Output: output, Error: toolResult.Error,
					})
				}
				logger.Info("Lead closing tool_call executed", "tool", tc.Tool, "round", closingRound)
			}
			closingEvent.ToolResults = toolResults
			closingEvent.FileContents = nil
			logger.Info("Lead closing tool_call round complete",
				"round", closingRound, "tools_executed", len(toolResults))
			continue
		}

		// Handle terminal actions (reply / synthesize / done)
		// 处理终态动作（reply / synthesize / done）
		for _, action := range terminalActions {
			switch action.Type {
			case "reply":
				replyContent := action.Content
				if isLeadReplyValid(replyContent, wsResult.Files) {
					logger.Info("SwarmWorkflow using Lead reply",
						"reply_len", len(replyContent),
						"tokens", closingDecision.TokensUsed,
					)

					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventLLMOutput,
						AgentID:    "swarm-supervisor",
						Message:    replyContent,
						Timestamp:  workflow.Now(ctx),
						Payload: map[string]any{
							"tokens_used": closingDecision.TokensUsed,
							"model_used":  closingDecision.ModelUsed,
							"provider":    closingDecision.Provider,
						},
					}).Get(ctx, nil)

					// Emit final_output LLM_OUTPUT so the OpenAI-compatible streamer
					// 发出 final_output LLM_OUTPUT，让 OpenAI 兼容的 streamer
					// picks up the canonical answer (it only forwards AgentID=="final_output").
					// 取到最终答案（它只转发 AgentID=="final_output" 的输出）。
					closingFinalOutputV := workflow.GetVersion(ctx, "swarm_final_output_event_v1", workflow.DefaultVersion, 1)
					if closingFinalOutputV >= 1 {
						_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
							WorkflowID: workflowID,
							EventType:  activities.StreamEventLLMOutput,
							AgentID:    "final_output",
							Message:    replyContent,
							Timestamp:  workflow.Now(ctx),
						}).Get(ctx, nil)
					}

					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventDataProcessing,
						AgentID:    "swarm-supervisor",
						Message:    "Processing complete",
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)

					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventDataProcessing,
						AgentID:    "swarm-supervisor",
						Message:    "Final answer ready",
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)

					_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
						WorkflowID: workflowID,
						EventType:  activities.StreamEventWorkflowCompleted,
						AgentID:    "swarm-supervisor",
						Message:    activities.MsgSwarmCompleted(),
						Timestamp:  workflow.Now(ctx),
					}).Get(ctx, nil)

					totalTokensUsed += closingDecision.TokensUsed

					// User-level memory extraction (best-effort, awaited to prevent child workflow cancellation)
					// 用户级记忆抽取（尽力而为，需 await，避免子 workflow 被取消）
					if input.UserID != "" && len(replyContent) >= 500 {
						memCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
							StartToCloseTimeout: 60 * time.Second,
							RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
						})
						_ = workflow.ExecuteActivity(memCtx, activities.ExtractMemoryActivity, activities.MemoryExtractInput{
							UserID:           input.UserID,
							TenantID:         input.TenantID,
							SessionID:        input.SessionID,
							Query:            input.Query,
							Result:           replyContent,
							ParentWorkflowID: workflowID,
						}).Get(ctx, nil)
					}

					replyMeta := buildSwarmMetadata(results)
					if historyTruncated {
						replyMeta["truncated"] = true
						replyMeta["truncation_reason"] = "temporal_history_limit"
					}
					return TaskResult{
						Result:     replyContent,
						Success:    true,
						TokensUsed: totalTokensUsed,
						Metadata:   replyMeta,
					}, nil
				}
				logger.Warn("Lead reply failed validation, falling back to synthesis",
					"reply_len", len(replyContent),
					"files", len(wsResult.Files),
				)

			case "synthesize":
				logger.Info("Lead requested synthesis")

			case "done":
				logger.Info("Lead returned done (backward compat), using synthesis")
			}
		}
		break // terminal action processed (or no actions left), fall through to synthesis 注：终态动作已处理（或无剩余动作），进入合成
	}

	// LLM synthesis — use medium model tier for swarm (agents already did heavy lifting)
	// LLM 合成——swarm 用 medium model tier（重活已由 agent 完成）
	synthContext := make(map[string]any, len(input.Context)+1)
	maps.Copy(synthContext, input.Context)
	if _, exists := synthContext["synthesis_model_tier"]; !exists {
		synthContext["synthesis_model_tier"] = "medium"
	}

	var synth activities.SynthesisResult
	if err := workflow.ExecuteActivity(ctx, activities.SynthesizeResultsLLM, activities.SynthesisInput{
		Query:            input.Query,
		AgentResults:     agentResults,
		Context:          synthContext,
		ParentWorkflowID: workflowID,
		SessionID:        input.SessionID,
	}).Get(ctx, &synth); err != nil {
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Synthesis failed: %v", err),
			TokensUsed:   totalTokensUsed,
			Metadata:     buildSwarmMetadata(results),
		}, err
	}

	totalTokensUsed += synth.TokensUsed

	// Record synthesis token usage
	// 记录合成 token 用量
	{
		recCtx := opts.WithTokenRecordOptions(ctx)
		provider := synth.Provider
		if provider == "" {
			provider = detectProviderFromModel(synth.ModelUsed)
		}
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       input.UserID,
			SessionID:    input.SessionID,
			TaskID:       workflowID,
			AgentID:      "swarm-synthesis",
			Model:        synth.ModelUsed,
			Provider:     provider,
			InputTokens:  synth.InputTokens,
			OutputTokens: synth.CompletionTokens,
			CallSequence: synth.CallSequence,
			Metadata:     map[string]any{"workflow": "swarm", "phase": "synthesis"},
		}).Get(ctx, nil)
	}

	// Emit final_output LLM_OUTPUT so the OpenAI-compatible streamer
	// 发出 final_output LLM_OUTPUT，让 OpenAI 兼容的 streamer
	// picks up the canonical answer (it only forwards AgentID=="final_output").
	// 取到最终答案（它只转发 AgentID=="final_output" 的输出）。
	synthFinalOutputV := workflow.GetVersion(ctx, "swarm_final_output_event_v1", workflow.DefaultVersion, 1)
	if synthFinalOutputV >= 1 && synth.FinalResult != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    synth.FinalResult,
			Timestamp:  workflow.Now(ctx),
		}).Get(ctx, nil)
	}

	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowCompleted,
		AgentID:    "swarm-supervisor",
		Message:    activities.MsgSwarmCompleted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	// User-level memory extraction (best-effort, awaited to prevent child workflow cancellation)
	// 用户级记忆抽取（尽力而为，需 await，避免子 workflow 被取消）
	if input.UserID != "" && len(synth.FinalResult) >= 500 {
		memCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 60 * time.Second,
			RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
		})
		_ = workflow.ExecuteActivity(memCtx, activities.ExtractMemoryActivity, activities.MemoryExtractInput{
			UserID:           input.UserID,
			TenantID:         input.TenantID,
			SessionID:        input.SessionID,
			Query:            input.Query,
			Result:           synth.FinalResult,
			ParentWorkflowID: workflowID,
		}).Get(ctx, nil)
	}

	meta := buildSwarmMetadata(results)
	if historyTruncated {
		meta["truncated"] = true
		meta["truncation_reason"] = "temporal_history_limit"
	}

	return TaskResult{
		Result:     synth.FinalResult,
		Success:    true,
		TokensUsed: totalTokensUsed,
		Metadata:   meta,
	}, nil
}

// buildSwarmMetadata builds metadata from swarm agent results.
// buildSwarmMetadata 从 swarm agent 结果构建 metadata。
func buildSwarmMetadata(results map[string]AgentLoopResult) map[string]any {
	agentSummaries := make([]map[string]any, 0, len(results))
	metaIDs := slices.Sorted(maps.Keys(results))
	var totalInput, totalOutput, totalCacheRead, totalCacheCreation int
	var totalCost float64
	for _, id := range metaIDs {
		r := results[id]
		agentSummaries = append(agentSummaries, map[string]any{
			"agent_id":   r.AgentID,
			"iterations": r.Iterations,
			"tokens":     r.TokensUsed,
			"success":    r.Success,
			"model":      r.ModelUsed,
		})
		totalInput += r.InputTokens
		totalOutput += r.OutputTokens
		totalCacheRead += r.CacheReadTokens
		totalCacheCreation += r.CacheCreationTokens
		totalCost += pricing.CostForSplitWithCache(
			r.ModelUsed, r.InputTokens, r.OutputTokens,
			r.CacheReadTokens, r.CacheCreationTokens, r.CacheCreation1hTokens, r.Provider,
		)
	}

	// Use flat keys only — nested structures (like agentSummaries) cause
	// Temporal JSON codec to fail deserializing map[string]interface{},
	// resulting in empty metadata and zero cost/tokens in persistence.
	// 只用扁平 key：嵌套结构（如 agentSummaries）会让 Temporal JSON codec
	// 反序列化 map[string]interface{} 失败，导致 metadata 为空、持久化的成本/token 归零。
	meta := map[string]any{
		"workflow_type":         "swarm",
		"total_agents":          len(results),
		"num_agents":            len(results),
		"input_tokens":          totalInput,
		"output_tokens":         totalOutput,
		"total_tokens":          totalInput + totalOutput,
		"cache_read_tokens":     totalCacheRead,
		"cache_creation_tokens": totalCacheCreation,
		"cost_usd":              totalCost,
	}
	return meta
}
