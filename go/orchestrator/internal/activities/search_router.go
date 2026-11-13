package activities

import (
	"context"
	"fmt"

	"go.temporal.io/sdk/activity"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
)

// SearchRouteInput defines input for routing a search to appropriate sources
// SearchRouteInput 是把一次 search 路由到合适来源的入参。
type SearchRouteInput struct {
	Query           string               `json:"query"`
	Dimension       string               `json:"dimension,omitempty"`        // Research dimension (e.g., "funding", "technology")
	SourceTypes     []string             `json:"source_types,omitempty"`     // Explicit source types to use
	Priority        string               `json:"priority,omitempty"`         // "high", "medium", "low"
	Context         map[string]any       `json:"context,omitempty"`          // Additional context
	LocalizationCtx *LocalizationContext `json:"localization_ctx,omitempty"` // For multi-language queries
}

// LocalizationContext provides localization info for searches
// LocalizationContext 提供检索所需的本地化信息。
type LocalizationContext struct {
	Languages      []string            `json:"languages,omitempty"`       // Target languages (e.g., ["zh", "ja"])
	LocalizedNames map[string][]string `json:"localized_names,omitempty"` // Entity name translations
	Region         string              `json:"region,omitempty"`          // Primary region focus
}

// SearchRoute defines a single search to execute
// SearchRoute 描述要执行的一次 search。
type SearchRoute struct {
	Query         string   `json:"query"`
	SourceType    string   `json:"source_type"`
	Sites         []string `json:"sites,omitempty"`
	ExaCategory   string   `json:"exa_category,omitempty"`
	RecencyDays   int      `json:"recency_days,omitempty"`
	MaxResults    int      `json:"max_results"`
	PriorityBoost float64  `json:"priority_boost"`
	QuerySuffix   string   `json:"query_suffix,omitempty"`
	Language      string   `json:"language,omitempty"` // For localized searches
	IsLocalized   bool     `json:"is_localized,omitempty"`
}

// SearchRouteResult contains the routing plan for a search
type SearchRouteResult struct {
	Routes          []SearchRoute `json:"routes"`
	TotalMaxResults int           `json:"total_max_results"`
	Strategy        string        `json:"strategy"` // "parallel", "sequential", "priority"
}

// RouteSearch generates a search routing plan based on dimension and source type config
// RouteSearch 依据 dimension 与 source type 配置生成 search 路由方案。
func (a *Activities) RouteSearch(ctx context.Context, input SearchRouteInput) (*SearchRouteResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("RouteSearch: generating routing plan",
		"query", input.Query,
		"dimension", input.Dimension,
		"source_types", input.SourceTypes,
	)

	// Load source types configuration
	// 加载 source types 配置。
	cfg, err := config.LoadSourceTypes()
	if err != nil {
		logger.Warn("RouteSearch: failed to load source types config, using defaults", "error", err)
		cfg = nil
	}

	var routes []SearchRoute
	totalMaxResults := 0

	// Determine source types to use
	// 确定要使用的 source types。
	sourceTypes := input.SourceTypes
	if len(sourceTypes) == 0 && input.Dimension != "" && cfg != nil {
		// Get source types for dimension
		// 取该 dimension 对应的 source types。
		primary, secondary := cfg.GetDimensionSources(input.Dimension)
		sourceTypes = append(primary, secondary...)
		logger.Info("RouteSearch: using dimension-based sources",
			"dimension", input.Dimension,
			"sources", sourceTypes,
		)
	}

	// Default source types if none specified
	// 未指定 source types 时使用默认组合。
	if len(sourceTypes) == 0 {
		sourceTypes = []string{"news", "aggregator", "official"}
		logger.Info("RouteSearch: using default sources", "sources", sourceTypes)
	}

	// Generate routes for each source type
	for _, sourceType := range sourceTypes {
		route := a.buildRouteForSourceType(cfg, sourceType, input.Query, input.Priority)
		if route != nil {
			routes = append(routes, *route)
			totalMaxResults += route.MaxResults
		}
	}

	// Add localized search routes if needed
	// 需要时追加本地化 search 路由。
	if input.LocalizationCtx != nil && len(input.LocalizationCtx.Languages) > 0 {
		localizedRoutes := a.buildLocalizedRoutes(cfg, input)
		routes = append(routes, localizedRoutes...)
		for _, r := range localizedRoutes {
			totalMaxResults += r.MaxResults
		}
	}

	// Determine execution strategy based on priority and route count
	// 按 priority 与路由数量决定执行策略。
	strategy := "parallel"
	if input.Priority == "high" && len(routes) > 3 {
		strategy = "priority" // Execute high-priority routes first
		// 高优先级路由先执行。
	}

	result := &SearchRouteResult{
		Routes:          routes,
		TotalMaxResults: totalMaxResults,
		Strategy:        strategy,
	}

	logger.Info("RouteSearch: generated routing plan",
		"route_count", len(routes),
		"total_max_results", totalMaxResults,
		"strategy", strategy,
	)

	return result, nil
}

// buildRouteForSourceType creates a search route for a specific source type
// buildRouteForSourceType 为指定 source type 构造一条 search 路由。
func (a *Activities) buildRouteForSourceType(cfg *config.SourceTypesConfig, sourceType, query, priority string) *SearchRoute {
	route := &SearchRoute{
		Query:         query,
		SourceType:    sourceType,
		MaxResults:    10, // Default
		PriorityBoost: 1.0,
	}

	if cfg == nil {
		// Use defaults when config not available
		// 配置不可用时走内置默认值。
		return a.buildDefaultRoute(sourceType, query)
	}

	// Try to get source type config
	// 尝试取该 source type 的配置。
	if st, ok := cfg.GetSourceType(sourceType); ok {
		route.Sites = st.Sites
		route.RecencyDays = st.RecencyDays
		route.MaxResults = st.MaxResults
		route.PriorityBoost = st.PriorityBoost
		route.QuerySuffix = st.FallbackQuerySuffix

		// Get Exa category if available
		// 有 Exa category 就带上。
		if st.ExaCategory != nil {
			route.ExaCategory = *st.ExaCategory
		}
	} else if rs, ok := cfg.GetRegionalSource(sourceType); ok {
		// Regional source
		// 地区性来源。
		route.Sites = rs.Sites
		route.MaxResults = rs.MaxResults
		route.PriorityBoost = rs.PriorityBoost
		route.Language = rs.Language
		route.IsLocalized = true
	} else {
		// Unknown source type, use defaults
		// 未知 source type，退回默认配置。
		return a.buildDefaultRoute(sourceType, query)
	}

	// Adjust max results based on priority
	// 按 priority 调整 max results：high 放大、low 缩小。
	if priority == "high" {
		route.MaxResults = int(float64(route.MaxResults) * 1.5)
	} else if priority == "low" {
		route.MaxResults = int(float64(route.MaxResults) * 0.5)
	}

	return route
}

// buildDefaultRoute creates a default route for unknown source types
// buildDefaultRoute 为未知 source type 生成默认路由。
func (a *Activities) buildDefaultRoute(sourceType, query string) *SearchRoute {
	defaults := map[string]SearchRoute{
		"official": {
			SourceType:    "official",
			QuerySuffix:   "official site",
			MaxResults:    5,
			PriorityBoost: 1.5,
		},
		"aggregator": {
			SourceType:    "aggregator",
			Sites:         []string{"crunchbase.com", "pitchbook.com", "wikipedia.org", "linkedin.com"},
			MaxResults:    8,
			PriorityBoost: 1.2,
		},
		"news": {
			SourceType:    "news",
			ExaCategory:   "news",
			RecencyDays:   365,
			MaxResults:    10,
			PriorityBoost: 1.0,
		},
		"academic": {
			SourceType:    "academic",
			ExaCategory:   "research paper",
			Sites:         []string{"arxiv.org", "scholar.google.com", "pubmed.ncbi.nlm.nih.gov"},
			MaxResults:    8,
			PriorityBoost: 1.3,
		},
		"github": {
			SourceType:    "github",
			Sites:         []string{"github.com"},
			MaxResults:    5,
			PriorityBoost: 1.1,
		},
		"financial": {
			SourceType:    "financial",
			Sites:         []string{"sec.gov", "bloomberg.com", "reuters.com", "finance.yahoo.com"},
			MaxResults:    8,
			PriorityBoost: 1.2,
		},
	}

	if def, ok := defaults[sourceType]; ok {
		def.Query = query
		return &def
	}

	// Generic fallback
	// 通用兜底。
	return &SearchRoute{
		Query:         query,
		SourceType:    sourceType,
		MaxResults:    10,
		PriorityBoost: 1.0,
	}
}

// buildLocalizedRoutes creates routes for localized searches
// buildLocalizedRoutes 为本地化检索生成路由。
func (a *Activities) buildLocalizedRoutes(cfg *config.SourceTypesConfig, input SearchRouteInput) []SearchRoute {
	var routes []SearchRoute

	if input.LocalizationCtx == nil {
		return routes
	}

	for _, lang := range input.LocalizationCtx.Languages {
		// Map language to regional source
		// 把语言映射为对应的地区性来源标识。
		regionalSource := fmt.Sprintf("local_%s", lang)

		// Get localized entity names for this language
		// 取该语言下实体的本地化名称；没有就回退到原始 query。
		var localizedQuery string
		if names, ok := input.LocalizationCtx.LocalizedNames[lang]; ok && len(names) > 0 {
			localizedQuery = names[0] // Use first localized name
		} else {
			localizedQuery = input.Query // Fallback to original query
		}

		route := &SearchRoute{
			Query:         localizedQuery,
			SourceType:    regionalSource,
			Language:      lang,
			IsLocalized:   true,
			MaxResults:    5, // Default for localized
			PriorityBoost: 1.1,
		}

		// Try to get regional config
		// 尝试取地区配置。
		if cfg != nil {
			if rs, ok := cfg.GetRegionalSource(regionalSource); ok {
				route.Sites = rs.Sites
				route.MaxResults = rs.MaxResults
				route.PriorityBoost = rs.PriorityBoost
			}
		} else {
			// Use defaults for common languages
			// 常见语言使用内置默认站点。
			switch lang {
			case "zh":
				route.Sites = []string{"36kr.com", "iyiou.com", "tianyancha.com", "pedaily.cn"}
			case "ja":
				route.Sites = []string{"nikkei.com", "prtimes.jp", "startup-db.com"}
			case "ko":
				route.Sites = []string{"platum.kr", "thevc.kr", "venturesquare.net"}
			}
		}

		routes = append(routes, *route)
	}

	return routes
}

// MergeSearchResults merges results from multiple search routes with deduplication
// MergeSearchResults 合并多条 search 路由的结果并去重。
type MergeSearchInput struct {
	Results []SearchRouteResults `json:"results"`
}

// SearchRouteResults holds results from a single search route
// SearchRouteResults 保存单条 search 路由的结果。
type SearchRouteResults struct {
	Route   SearchRoute  `json:"route"`
	Items   []SearchItem `json:"items"`
	Success bool         `json:"success"`
	Error   string       `json:"error,omitempty"`
}

// SearchItem represents a single search result
type SearchItem struct {
	URL           string  `json:"url"`
	Title         string  `json:"title"`
	Snippet       string  `json:"snippet,omitempty"`
	PublishedDate string  `json:"published_date,omitempty"`
	Score         float64 `json:"score"`
	SourceType    string  `json:"source_type"`
}

// MergeSearchResult contains the merged, deduplicated results
// MergeSearchResult 是合并去重后的结果。
type MergeSearchResult struct {
	Items             []SearchItem   `json:"items"`
	TotalResults      int            `json:"total_results"`
	DeduplicatedCount int            `json:"deduplicated_count"`
	SourceBreakdown   map[string]int `json:"source_breakdown"`
}

// MergeSearchResults merges and deduplicates results from multiple search routes
func (a *Activities) MergeSearchResults(ctx context.Context, input MergeSearchInput) (*MergeSearchResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("MergeSearchResults: merging results", "route_count", len(input.Results))

	// Load config for merge settings
	// 加载合并相关的配置。
	cfg, _ := config.LoadSourceTypes()

	// Track seen URLs for deduplication
	// 记录已见过的 URL，用于去重。
	seenURLs := make(map[string]bool)
	domainCounts := make(map[string]int)
	sourceBreakdown := make(map[string]int)

	var mergedItems []SearchItem
	totalResults := 0
	duplicates := 0

	// Merge settings
	// 合并参数。
	maxPerDomain := 3
	if cfg != nil {
		maxPerDomain = cfg.ResultMerging.MaxPerDomain
	}

	// Process each route's results
	// 逐条路由处理其结果。
	for _, routeResult := range input.Results {
		if !routeResult.Success {
			continue
		}

		for _, item := range routeResult.Items {
			totalResults++

			// Normalize URL for deduplication
			normalizedURL := normalizeURL(item.URL)
			if seenURLs[normalizedURL] {
				duplicates++
				continue
			}

			// Check domain limit
			// 检查同一 domain 的条数上限。
			domain := extractDomain(item.URL)
			if domainCounts[domain] >= maxPerDomain {
				continue
			}

			// Apply priority boost from route
			// 应用该路由的 priority boost。
			item.Score = item.Score * routeResult.Route.PriorityBoost
			item.SourceType = routeResult.Route.SourceType

			seenURLs[normalizedURL] = true
			domainCounts[domain]++
			sourceBreakdown[routeResult.Route.SourceType]++
			mergedItems = append(mergedItems, item)
		}
	}

	// Sort by score (descending)
	sortSearchItemsByScore(mergedItems)

	result := &MergeSearchResult{
		Items:             mergedItems,
		TotalResults:      totalResults,
		DeduplicatedCount: duplicates,
		SourceBreakdown:   sourceBreakdown,
	}

	logger.Info("MergeSearchResults: merge complete",
		"total_input", totalResults,
		"merged_output", len(mergedItems),
		"duplicates_removed", duplicates,
	)

	return result, nil
}

// normalizeURL normalizes a URL for deduplication
func normalizeURL(url string) string {
	// Simple normalization: remove trailing slashes and common tracking params
	// In production, use a proper URL parsing library
	// 简单归一化：去掉末尾斜杠等；
	// 生产环境应改用正规的 URL 解析库。
	if len(url) > 0 && url[len(url)-1] == '/' {
		url = url[:len(url)-1]
	}
	return url
}

// extractDomain extracts the domain from a URL
func extractDomain(url string) string {
	// Simple extraction - in production, use net/url
	// 简单截取；生产环境应改用 net/url。
	start := 0
	if len(url) > 8 && url[:8] == "https://" {
		start = 8
	} else if len(url) > 7 && url[:7] == "http://" {
		start = 7
	}

	end := start
	for end < len(url) && url[end] != '/' && url[end] != '?' {
		end++
	}

	return url[start:end]
}

// sortSearchItemsByScore sorts search items by score in descending order
func sortSearchItemsByScore(items []SearchItem) {
	// Simple bubble sort for now - replace with sort.Slice in production
	// 目前是简单冒泡排序；生产环境应换成 sort.Slice。
	for i := range items {
		for j := i + 1; j < len(items); j++ {
			if items[j].Score > items[i].Score {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
}
