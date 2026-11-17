package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	"go.temporal.io/sdk/activity"
)

// SubqueryGeneratorInput is the input for generating gap-filling subqueries
// SubqueryGeneratorInput 是生成补缺 subquery 的入参。
type SubqueryGeneratorInput struct {
	Query            string         `json:"query"`
	CoverageGaps     []CoverageGap  `json:"coverage_gaps"`
	CurrentSynthesis string         `json:"current_synthesis,omitempty"`
	Iteration        int            `json:"iteration"`
	MaxSubqueries    int            `json:"max_subqueries"` // Limit new queries per iteration 注：每轮新增 query 的上限
	Context          map[string]any `json:"context,omitempty"`
	ParentWorkflowID string         `json:"parent_workflow_id,omitempty"`
	// Deep Research 2.0: Source-type aware search
	// Deep Research 2.0：检索时感知 source type。
	EntityName      string   `json:"entity_name,omitempty"`      // Primary entity being researched 注：正在研究的核心实体
	QueryType       string   `json:"query_type,omitempty"`       // company, industry, scientific, etc. 注：company、industry、scientific 等
	TargetLanguages []string `json:"target_languages,omitempty"` // Languages for regional searches 注：地区检索使用的语言
}

// GeneratedSubquery represents a generated subquery for gap-filling
// GeneratedSubquery 表示一条用于填补覆盖缺口的 subquery。
type GeneratedSubquery struct {
	ID             string         `json:"id"`
	Query          string         `json:"query"`
	TargetGap      string         `json:"target_gap"`      // Which gap this addresses 注：针对哪个缺口
	Priority       string         `json:"priority"`        // "high", "medium", "low" 注："high"、"medium"、"low"
	SuggestedTools []string       `json:"suggested_tools"` // Tools to use 注：使用的 tool
	SourceTypes    []string       `json:"source_types"`    // Source types to target 注：目标 source types
	ToolParameters map[string]any `json:"tool_parameters"` // Pre-structured tool params 注：预构造的 tool 参数
	// Task contract fields
	// Task contract 字段。
	OutputFormat   *OutputFormatSpec   `json:"output_format,omitempty"`
	SourceGuidance *SourceGuidanceSpec `json:"source_guidance,omitempty"`
	SearchBudget   *SearchBudgetSpec   `json:"search_budget,omitempty"`
	Boundaries     *BoundariesSpec     `json:"boundaries,omitempty"`
}

// SubqueryGeneratorResult is the result of subquery generation
// SubqueryGeneratorResult 是 subquery 生成的结果。
type SubqueryGeneratorResult struct {
	Subqueries     []GeneratedSubquery `json:"subqueries"`
	TotalGenerated int                 `json:"total_generated"`
	Reasoning      string              `json:"reasoning"`
	TokensUsed     int                 `json:"tokens_used"`
	ModelUsed      string              `json:"model_used"`
	Provider       string              `json:"provider"`
	InputTokens    int                 `json:"input_tokens"`
	OutputTokens   int                 `json:"output_tokens"`
}

// GenerateSubqueries generates new subqueries to fill coverage gaps
// GenerateSubqueries 针对覆盖缺口生成新的 subquery。
func (a *Activities) GenerateSubqueries(ctx context.Context, input SubqueryGeneratorInput) (*SubqueryGeneratorResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("GenerateSubqueries: starting",
		"query", truncateStr(input.Query, 100),
		"gaps", len(input.CoverageGaps),
		"iteration", input.Iteration,
		"max_subqueries", input.MaxSubqueries,
	)

	// Default max subqueries
	// 默认的最大 subquery 数。
	maxSub := input.MaxSubqueries
	if maxSub == 0 {
		maxSub = 3 // Default: 3 gap-filling queries per iteration 注：默认每轮 3 条补缺 query
	}

	// Build generation prompt
	// 构造生成用的 prompt。
	systemPrompt := buildSubqueryGenerationPrompt(input, maxSub)
	userContent := buildSubqueryGenerationContent(input)

	// Call LLM service
	// 调用 LLM service。
	llmServiceURL := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/agent/query", llmServiceURL)

	reqBody := map[string]any{
		"query":       userContent,
		"max_tokens":  8192, // Extended for structured JSON output with multiple subqueries 注：为输出多条 subquery 的结构化 JSON 而调大
		"temperature": 0.3,
		"agent_id":    "subquery_generator",
		"model_tier":  "small",
		"context": map[string]any{
			"system_prompt":      systemPrompt,
			"parent_workflow_id": input.ParentWorkflowID,
		},
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	client := &http.Client{
		Timeout:   120 * time.Second, // Extended for LLM processing time 注：为 LLM 处理时间而调大
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(reqJSON)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", "subquery_generator")
	if input.ParentWorkflowID != "" {
		req.Header.Set("X-Workflow-ID", input.ParentWorkflowID)
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("LLM service call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d from LLM service", resp.StatusCode)
	}

	// Parse response
	// 解析 response
	var llmResp struct {
		Success  bool   `json:"success"`
		Response string `json:"response"`
		Metadata struct {
			InputTokens  int     `json:"input_tokens"`
			OutputTokens int     `json:"output_tokens"`
			CostUSD      float64 `json:"cost_usd"`
		} `json:"metadata"`
		TokensUsed int    `json:"tokens_used"`
		ModelUsed  string `json:"model_used"`
		Provider   string `json:"provider"`
	}

	if err := json.NewDecoder(resp.Body).Decode(&llmResp); err != nil {
		return nil, fmt.Errorf("failed to parse LLM response: %w", err)
	}

	result := &SubqueryGeneratorResult{
		TokensUsed:   llmResp.TokensUsed,
		ModelUsed:    llmResp.ModelUsed,
		Provider:     llmResp.Provider,
		InputTokens:  llmResp.Metadata.InputTokens,
		OutputTokens: llmResp.Metadata.OutputTokens,
	}

	// Parse response
	// 解析 response
	if err := parseSubqueryGenerationResponse(llmResp.Response, result, input.Iteration); err != nil {
		logger.Warn("GenerateSubqueries: failed to parse structured response",
			"error", err,
		)
		// Fallback: generate basic subqueries from gaps
		// 兜底：直接按缺口生成基础 subquery。
		result.Subqueries = generateFallbackSubqueries(input, maxSub)
		result.Reasoning = "Fallback generation due to parse error"
	}

	// Enforce max limit
	// 强制截断到上限。
	if len(result.Subqueries) > maxSub {
		result.Subqueries = result.Subqueries[:maxSub]
	}

	result.TotalGenerated = len(result.Subqueries)

	logger.Info("GenerateSubqueries: complete",
		"generated", result.TotalGenerated,
	)

	return result, nil
}

// buildSubqueryGenerationPrompt creates the system prompt
// buildSubqueryGenerationPrompt 构造 system prompt。
func buildSubqueryGenerationPrompt(input SubqueryGeneratorInput, maxSub int) string {
	var sb strings.Builder

	sb.WriteString(`You are a research query generator. Your task is to generate targeted subqueries
to fill specific coverage gaps identified in the research.

## Your Goals:
1. Generate focused queries that directly address coverage gaps
2. Prioritize CRITICAL gaps over optional ones
3. Suggest appropriate tools and source types for each query
4. Avoid generating redundant or overlapping queries

## Guidelines:
- Generate specific, searchable queries (not vague questions)
- Each query should target ONE specific gap
- Include tool parameters when web_search is suggested
- Set appropriate source_guidance for targeted searches
- Keep queries concise but descriptive

## IMPORTANT: Alternative Search Strategies

When standard searches fail to find information about a company/entity, try these strategies:

1. **Direct Domain Access**: For company research, suggest web_fetch for:
   - The company's likely domain (e.g., "companyname.com", "companyname.io")
   - Known product domains if different from company name
   - Example: If researching "ExampleCorp", try fetching "example.com"

2. **Alternative Search Terms**: Try:
   - Product names (companies often have products with different names)
   - Japanese/local language terms for Asian companies
   - LinkedIn company page searches
   - Crunchbase/AngelList searches
   - "[company] site:linkedin.com" or "[company] site:crunchbase.com"

3. **Include web_fetch** when you know/suspect a URL exists:
   - tool_parameters: {"tool": "web_fetch", "url": "https://example.com"}
   - This directly retrieves page content vs. searching

`)
	fmt.Fprintf(&sb, "Maximum subqueries to generate: %d\n", maxSub)
	fmt.Fprintf(&sb, "Current iteration: %d\n\n", input.Iteration)

	sb.WriteString(`## Response Format:
Return a JSON object:
{
  "subqueries": [
    {
      "id": "gap-fill-1",
      "query": "Specific search query text",
      "target_gap": "Name of gap being addressed",
      "priority": "high",
      "suggested_tools": ["web_search"],
      "source_types": ["news", "official"],
      "tool_parameters": {
        "tool": "web_search",
        "query": "search query with specific terms"
      },
      "source_guidance": {"required": ["official"], "optional": ["news"]},
      "boundaries": {"in_scope": ["topic1"], "out_of_scope": ["topic2"]}
    }
  ],
  "reasoning": "Brief explanation of generation strategy"
}
`)

	return sb.String()
}

// buildSubqueryGenerationContent builds user content
// buildSubqueryGenerationContent 构造 user content。
func buildSubqueryGenerationContent(input SubqueryGeneratorInput) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "## Original Query:\n%s\n\n", input.Query)

	sb.WriteString("## Coverage Gaps to Address:\n\n")
	for i, gap := range input.CoverageGaps {
		fmt.Fprintf(&sb, "### Gap %d: %s\n", i+1, gap.Area)
		fmt.Fprintf(&sb, "- Importance: %s\n", gap.Importance)
		if len(gap.Questions) > 0 {
			sb.WriteString("- Questions to answer:\n")
			for _, q := range gap.Questions {
				fmt.Fprintf(&sb, "  - %s\n", q)
			}
		}
		if len(gap.SourceTypes) > 0 {
			fmt.Fprintf(&sb, "- Recommended sources: %s\n", strings.Join(gap.SourceTypes, ", "))
		}
		sb.WriteString("\n")
	}

	if input.CurrentSynthesis != "" {
		synthesis := input.CurrentSynthesis
		if len(synthesis) > 1000 {
			synthesis = synthesis[:1000] + "...[truncated]"
		}
		fmt.Fprintf(&sb, "## Current Understanding (avoid overlap):\n%s\n", synthesis)
	}

	return sb.String()
}

// parseSubqueryGenerationResponse parses the LLM response
// parseSubqueryGenerationResponse 解析 LLM response。
func parseSubqueryGenerationResponse(response string, result *SubqueryGeneratorResult, iteration int) error {
	// Find JSON in response
	// 从 response 里截出 JSON。
	start := strings.Index(response, "{")
	end := strings.LastIndex(response, "}")

	if start == -1 || end == -1 || end <= start {
		return fmt.Errorf("no JSON object found in response")
	}

	jsonStr := response[start : end+1]

	var parsed struct {
		Subqueries []struct {
			ID             string              `json:"id"`
			Query          string              `json:"query"`
			TargetGap      string              `json:"target_gap"`
			Priority       string              `json:"priority"`
			SuggestedTools []string            `json:"suggested_tools"`
			SourceTypes    []string            `json:"source_types"`
			ToolParameters map[string]any      `json:"tool_parameters"`
			SourceGuidance map[string][]string `json:"source_guidance"`
			Boundaries     map[string][]string `json:"boundaries"`
		} `json:"subqueries"`
		Reasoning string `json:"reasoning"`
	}

	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	result.Reasoning = parsed.Reasoning
	result.Subqueries = make([]GeneratedSubquery, 0, len(parsed.Subqueries))

	for i, sq := range parsed.Subqueries {
		subquery := GeneratedSubquery{
			ID:             sq.ID,
			Query:          sq.Query,
			TargetGap:      sq.TargetGap,
			Priority:       sq.Priority,
			SuggestedTools: sq.SuggestedTools,
			SourceTypes:    sq.SourceTypes,
			ToolParameters: sq.ToolParameters,
		}

		// Set default ID if empty
		// ID 为空时补一个默认值。
		if subquery.ID == "" {
			subquery.ID = fmt.Sprintf("iter%d-gap%d", iteration, i+1)
		}

		// Parse source guidance if present
		// 有 source guidance 就解析出来。
		if sq.SourceGuidance != nil {
			subquery.SourceGuidance = &SourceGuidanceSpec{
				Required: sq.SourceGuidance["required"],
				Optional: sq.SourceGuidance["optional"],
				Avoid:    sq.SourceGuidance["avoid"],
			}
		}

		// Parse boundaries if present
		// 有 boundaries 就解析出来。
		if sq.Boundaries != nil {
			subquery.Boundaries = &BoundariesSpec{
				InScope:    sq.Boundaries["in_scope"],
				OutOfScope: sq.Boundaries["out_of_scope"],
			}
		}

		result.Subqueries = append(result.Subqueries, subquery)
	}

	return nil
}

// generateFallbackSubqueries creates basic subqueries from gaps when parsing fails
// Enhanced to use source_types.yaml config for targeted searches
// generateFallbackSubqueries 在解析失败时按缺口生成基础 subquery；
// 并会借助 source_types.yaml 配置做更有针对性的检索。
func generateFallbackSubqueries(input SubqueryGeneratorInput, maxSub int) []GeneratedSubquery {
	subqueries := make([]GeneratedSubquery, 0, maxSub)

	// Load source types config
	// 加载 source types 配置。
	stConfig, err := config.LoadSourceTypes()
	if err != nil {
		stConfig = nil // Will use basic fallback 注：将使用基础兜底
	}

	// Track which entity gap types we've already queried to avoid duplicates
	// 记录已查过的 entity gap 类型，避免重复查询。
	queriedInfoTypes := make(map[string]bool)

	for i, gap := range input.CoverageGaps {
		if len(subqueries) >= maxSub {
			break
		}

		// Build base query from the gap
		// 用缺口信息拼出基础 query。
		baseQuery := gap.Area
		if len(gap.Questions) > 0 {
			baseQuery = gap.Questions[0]
		}

		// Get source types for this gap/dimension
		// 取该缺口/dimension 对应的 source types。
		sourceTypes := gap.SourceTypes
		if len(sourceTypes) == 0 && stConfig != nil {
			// Try to get recommended source types from config
			// 尝试从配置中取推荐的 source types。
			dimName := config.NormalizeDimensionName(gap.Area)
			primary, _ := stConfig.GetSourceTypesForDimension(dimName)
			sourceTypes = primary
		}
		if len(sourceTypes) == 0 {
			sourceTypes = []string{"aggregator", "news"} // Default 注：默认值
		}

		// Deep Research 2.0: Use entity_gap_queries for specific missing info types
		// Deep Research 2.0：对特定缺失信息类型使用 entity_gap_queries。
		if input.EntityName != "" && stConfig != nil {
			infoType := detectGapInfoType(gap.Area)
			// Also check questions for info type detection
			// 同时从 questions 里判断信息类型。
			if infoType == "" && len(gap.Questions) > 0 {
				for _, q := range gap.Questions {
					if it := detectGapInfoType(q); it != "" {
						infoType = it
						break
					}
				}
			}
			// Generate entity gap queries if not already done for this type
			// 该类型还没生成过时，生成 entity gap query。
			if infoType != "" && !queriedInfoTypes[infoType] {
				entityGapQueries := generateEntityGapQueries(stConfig, input.EntityName, infoType, input.Iteration, i)
				for _, eq := range entityGapQueries {
					if len(subqueries) < maxSub {
						subqueries = append(subqueries, eq)
					}
				}
				queriedInfoTypes[infoType] = true
			}
		}

		// If we have an entity name and this is company research, add targeted queries
		// 有实体名且属于公司研究时，补充针对性 query。
		if input.EntityName != "" && (input.QueryType == "company" || input.QueryType == "") {
			// Generate site-specific query for aggregator sources
			// 为 aggregator 类来源生成限定站点的 query。
			aggregatorQuery := generateSourceTypeQuery(stConfig, input.EntityName, baseQuery, "aggregator")
			if aggregatorQuery != "" && len(subqueries) < maxSub {
				subqueries = append(subqueries, GeneratedSubquery{
					ID:             fmt.Sprintf("iter%d-gap%d-agg", input.Iteration, i+1),
					Query:          aggregatorQuery,
					TargetGap:      gap.Area,
					Priority:       gap.Importance,
					SuggestedTools: []string{"web_search"},
					SourceTypes:    []string{"aggregator"},
					ToolParameters: map[string]any{
						"tool":  "web_search",
						"query": aggregatorQuery,
					},
					SourceGuidance: &SourceGuidanceSpec{
						Required: []string{"aggregator"},
					},
				})
			}
		}

		// Add standard query
		// 追加标准 query。
		if len(subqueries) < maxSub {
			query := baseQuery
			if input.EntityName != "" && !strings.Contains(strings.ToLower(baseQuery), strings.ToLower(input.EntityName)) {
				query = fmt.Sprintf(`"%s" %s`, input.EntityName, baseQuery)
			}

			subqueries = append(subqueries, GeneratedSubquery{
				ID:             fmt.Sprintf("iter%d-gap%d", input.Iteration, i+1),
				Query:          query,
				TargetGap:      gap.Area,
				Priority:       gap.Importance,
				SuggestedTools: []string{"web_search"},
				SourceTypes:    sourceTypes,
				ToolParameters: map[string]any{
					"tool":  "web_search",
					"query": query,
				},
				SourceGuidance: &SourceGuidanceSpec{
					Required: sourceTypes[:min(2, len(sourceTypes))],
				},
			})
		}

		// Add regional language queries if needed
		// 需要时追加地区语言 query。
		if len(input.TargetLanguages) > 0 && stConfig != nil && len(subqueries) < maxSub {
			for _, lang := range input.TargetLanguages {
				if lang == "en" {
					continue // Skip English, already covered 注：跳过英文，已覆盖
				}
				regionalSources := stConfig.GetRegionalSourcesForLanguage(lang)
				if len(regionalSources) > 0 {
					srcName := regionalSources[0]
					if rs, ok := stConfig.GetRegionalSource(srcName); ok && len(rs.Sites) > 0 {
						// Use first site for targeted search
						// 用首个站点做定向检索。
						regionalQuery := fmt.Sprintf("site:%s %s", rs.Sites[0], input.EntityName)
						if len(subqueries) < maxSub {
							subqueries = append(subqueries, GeneratedSubquery{
								ID:             fmt.Sprintf("iter%d-gap%d-%s", input.Iteration, i+1, lang),
								Query:          regionalQuery,
								TargetGap:      gap.Area,
								Priority:       "medium",
								SuggestedTools: []string{"web_search"},
								SourceTypes:    []string{srcName},
								ToolParameters: map[string]any{
									"tool":  "web_search",
									"query": regionalQuery,
								},
							})
						}
					}
				}
			}
		}
	}

	return subqueries
}

// generateSourceTypeQuery creates a site-filtered query for a source type
// generateSourceTypeQuery 为某 source type 生成限定站点的 query。
func generateSourceTypeQuery(stConfig *config.SourceTypesConfig, entityName, baseQuery, sourceType string) string {
	if stConfig == nil {
		return ""
	}

	sites := stConfig.GetSitesForSourceType(sourceType)
	if len(sites) == 0 {
		return ""
	}

	// Use first 2-3 priority sites
	// 取前 2-3 个优先站点。
	maxSites := min(3, len(sites))
	siteParts := make([]string, maxSites)
	for i := range maxSites {
		siteParts[i] = fmt.Sprintf("site:%s", sites[i])
	}

	// Build query with entity name
	// 用实体名拼 query。
	query := entityName
	if baseQuery != "" && baseQuery != entityName {
		// Extract key terms from base query
		// 从基础 query 中提取关键词。
		terms := extractKeyTerms(baseQuery)
		if len(terms) > 0 {
			query = fmt.Sprintf(`"%s" %s`, entityName, strings.Join(terms, " "))
		}
	}

	// Return query with site filter OR'd together
	// 返回把站点过滤条件用 OR 连接后的 query。
	return fmt.Sprintf("(%s) %s", strings.Join(siteParts, " OR "), query)
}

// extractKeyTerms extracts key search terms from a gap description
// extractKeyTerms 从缺口描述里提取关键检索词。
func extractKeyTerms(text string) []string {
	// Simple extraction of key terms
	// 关键词的简单提取方式。
	keywords := []string{"funding", "founder", "CEO", "revenue", "products", "services",
		"employees", "headquarters", "competitors", "market", "investors", "valuation",
		"founded", "history", "team", "leadership", "customers", "pricing"}

	lower := strings.ToLower(text)
	var found []string
	for _, kw := range keywords {
		if strings.Contains(lower, kw) {
			found = append(found, kw)
		}
	}
	return found
}

// detectGapInfoType maps gap area text to entity_gap_queries keys from config
// Returns the info type (e.g., "founder_ceo", "funding_history") if detected
// detectGapInfoType 把缺口描述映射到配置里 entity_gap_queries 的 key；
// 命中时返回对应信息类型（如 "founder_ceo"、"funding_history"）。
func detectGapInfoType(gapArea string) string {
	lower := strings.ToLower(gapArea)

	// Map common gap descriptions to entity_gap_queries keys
	// 把常见缺口描述映射到 entity_gap_queries 的 key。
	patterns := map[string][]string{
		// Founder / CEO / leadership information
		// 创始人 / CEO / 管理层信息。
		"founder_ceo": {
			// English
			// 英文
			"founder", "co-founder", "founding team", "founding members",
			"ceo", "chief executive officer", "leadership", "leadership team",
			"executive", "executive team", "management", "management team",
			// Chinese
			// 中文
			"创始人", "联合创始人", "创办人", "创立者", "创始团队", "创业团队",
			"首席执行官", "领导", "领导层", "管理层", "管理团队", "高管", "核心团队",
			// Japanese
			// 日文
			"創業者", "共同創業者", "創業メンバー", "創設者", "創業チーム", "創設メンバー",
			"代表取締役", "経営陣", "経営チーム", "マネジメント", "幹部",
		},
		// Funding / investment history
		// 融资 / 投资历史。
		"funding_history": {
			// English
			// 英文
			"funding", "funding history", "investment", "investments", "raised",
			"fundraising", "financing", "funding round", "funding rounds",
			"series a", "series b", "series c", "seed round", "pre-seed",
			"venture capital", "vc", "investors",
			// Chinese
			// 中文
			"融资", "融資", "融资历史", "融资情况", "融资轮次", "融资阶段",
			"投资", "投資", "资金", "资本", "融资金额", "天使轮", "A轮", "B轮", "C轮",
			// Japanese
			// 日文
			"資金調達", "資金調達ラウンド", "調達ラウンド", "調達額",
			"投資", "出資", "出資ラウンド", "シリーズa", "シリーズb", "シードラウンド",
		},
		// Employee / headcount
		// 员工数 / 人员规模。
		"employee_count": {
			// English
			// 英文
			"employee", "employees", "employee count", "headcount",
			"team size", "staff", "staff size", "workforce", "number of employees",
			// Chinese
			// 中文
			"员工", "員工", "员工人数", "员工规模", "团队规模", "人员规模", "员工总数", "在职人数",
			// Japanese
			// 日文
			"従業員", "従業員数", "社員数", "人員", "人数", "人員規模", "従業員規模", "スタッフ数",
		},
		// Founding year / company age
		// 成立年份 / 公司年限。
		"founding_year": {
			// English
			// 英文
			"founded", "founded in", "founding", "founding year",
			"established", "established in", "started", "started in",
			"inception", "launch date",
			// Chinese
			// 中文
			"成立", "成立时间", "成立于", "创建于", "創立", "创立时间", "创办时间", "成立日期", "公司历史",
			// Japanese
			// 日文
			"設立", "設立年", "設立日", "設立年月日",
			"創業", "創業年", "創業日", "創立", "創立年", "創立日",
		},
		// Headquarters / location
		// 总部 / 所在地。
		"headquarters": {
			// English
			// 英文
			"headquarters", "hq", "head office", "headquarter",
			"based in", "based at", "located in", "location",
			"office location", "main office", "corporate headquarters", "address",
			// Chinese
			// 中文
			"总部", "總部", "总部所在地", "公司所在地", "所在城市", "所在地", "注册地", "注册地址",
			"办公地址", "办公地点", "总部地点", "位置",
			// Japanese
			// 日文
			"本社", "本社所在地", "所在地", "本社オフィス", "本社住所", "住所", "拠点", "本部",
		},
		// Revenue / financial performance
		// 营收 / 财务表现。
		"revenue": {
			// English
			// 英文
			"revenue", "revenues", "arr", "annual recurring revenue",
			"mrr", "monthly recurring revenue", "sales", "turnover",
			"income", "earnings", "financial performance", "revenue growth",
			// Chinese
			// 中文
			"收入", "营收", "營收", "营业收入", "營業收入", "营业额", "營業額",
			"营收增长", "收入规模", "营业情况", "财务表现",
			// Japanese
			// 日文
			"売上", "売上高", "売上収益", "売上額", "収益",
			"売上成長", "売上成長率", "売上規模", "売上実績",
		},
	}

	for infoType, keywords := range patterns {
		for _, kw := range keywords {
			if strings.Contains(lower, kw) {
				return infoType
			}
		}
	}
	return ""
}

// generateEntityGapQueries creates specific queries from entity_gap_queries config
// Returns subqueries for the detected info type
// generateEntityGapQueries 依据 entity_gap_queries 配置生成具体 query；
// 返回该信息类型对应的 subquery 列表。
func generateEntityGapQueries(stConfig *config.SourceTypesConfig, entityName, infoType string, iteration, gapIdx int) []GeneratedSubquery {
	if stConfig == nil || entityName == "" || infoType == "" {
		return nil
	}

	queries, sourceTypes := stConfig.GetEntityGapQueries(infoType)
	if len(queries) == 0 {
		return nil
	}

	var subqueries []GeneratedSubquery

	// Use up to 2 queries per info type to avoid query explosion
	// 每个信息类型最多用 2 条 query，避免 query 数量爆炸。
	maxQueries := min(2, len(queries))
	for i := range maxQueries {
		// Replace {entity} placeholder with actual entity name
		// 把 {entity} 占位符替换成真实实体名。
		query := strings.ReplaceAll(queries[i], "{entity}", entityName)

		subqueries = append(subqueries, GeneratedSubquery{
			ID:             fmt.Sprintf("iter%d-gap%d-%s-%d", iteration, gapIdx, infoType, i+1),
			Query:          query,
			TargetGap:      infoType,
			Priority:       "high", // Entity gap queries are high priority 注：entity gap query 属于高优先级
			SuggestedTools: []string{"web_search"},
			SourceTypes:    sourceTypes,
			ToolParameters: map[string]any{
				"tool":  "web_search",
				"query": query,
			},
			SourceGuidance: &SourceGuidanceSpec{
				Required: sourceTypes[:min(2, len(sourceTypes))],
			},
		})
	}

	return subqueries
}
