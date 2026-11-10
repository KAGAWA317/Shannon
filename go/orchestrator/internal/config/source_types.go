package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

// SourceTypeConfig represents a source type definition
type SourceTypeConfig struct {
	Description         string   `yaml:"description"`
	Strategy            string   `yaml:"strategy"`
	ExaCategory         *string  `yaml:"exa_category"`
	FallbackQuerySuffix string   `yaml:"fallback_query_suffix"`
	Sites               []string `yaml:"sites"`
	RecencyDays         int      `yaml:"recency_days"`
	PriorityBoost       float64  `yaml:"priority_boost"`
	MaxResults          int      `yaml:"max_results"`
}

// RegionalSourceConfig represents a regional/local source configuration
// RegionalSourceConfig 表示某个区域/本地来源的配置
type RegionalSourceConfig struct {
	Description   string   `yaml:"description"`
	Language      string   `yaml:"language"`
	Strategy      string   `yaml:"strategy"`
	Sites         []string `yaml:"sites"`
	QueryTemplate string   `yaml:"query_template"`
	PriorityBoost float64  `yaml:"priority_boost"`
	MaxResults    int      `yaml:"max_results"`
}

// DimensionSourceMapping represents source type recommendations for a dimension
// DimensionSourceMapping 表示某个维度推荐的来源类型
type DimensionSourceMapping struct {
	Primary    []string `yaml:"primary"`
	Secondary  []string `yaml:"secondary"`
	GapQueries []string `yaml:"gap_queries"` // Queries to use when info is missing
	// 信息缺失时使用的查询
}

// EntityGapQuery represents gap-filling query patterns for missing entity info
// EntityGapQuery 表示实体信息缺失时的补漏查询模式
type EntityGapQuery struct {
	Queries     []string `yaml:"queries"`
	SourceTypes []string `yaml:"source_types"`
}

// EntityRelevanceConfig represents entity relevance filtering settings
type EntityRelevanceConfig struct {
	RequireEntityMention bool     `yaml:"require_entity_mention"`
	MinNameSimilarity    float64  `yaml:"min_name_similarity"`
	AllowPartialMatch    bool     `yaml:"allow_partial_match"`
	SkipForSourceTypes   []string `yaml:"skip_for_source_types"`
}

// PrefetchSource represents a forced prefetch source pattern
// PrefetchSource 表示强制预取的来源模式
type PrefetchSource struct {
	Pattern   string   `yaml:"pattern"`   // URL pattern with {slug} placeholder
	Priority  int      `yaml:"priority"`  // Lower = higher priority
	InfoTypes []string `yaml:"info_types"` // Types of info this source provides
	Subpages  int      `yaml:"subpages"`  // Number of subpages to fetch
}

// SearchStrategyConfig represents a search strategy configuration
type SearchStrategyConfig struct {
	QueryFormat           string `yaml:"query_format"`
	MergeStrategy         string `yaml:"merge_strategy"`
	UseExaCategory        bool   `yaml:"use_exa_category"`
	FallbackToSiteFilter  bool   `yaml:"fallback_to_site_filter"`
	SearchType            string `yaml:"search_type"`
	UseAutoprompt         bool   `yaml:"use_autoprompt"`
}

// ResultMergingConfig represents result merging configuration
type ResultMergingConfig struct {
	URLNormalization        bool    `yaml:"url_normalization"`
	SimilarityThreshold     float64 `yaml:"similarity_threshold"`
	MaxPerDomain            int     `yaml:"max_per_domain"`
	EnsureSourceDiversity   bool    `yaml:"ensure_source_diversity"`
	RelevanceWeight         float64 `yaml:"relevance_weight"`
	CredibilityWeight       float64 `yaml:"credibility_weight"`
	RecencyWeight           float64 `yaml:"recency_weight"`
	SourceTypeBoostWeight   float64 `yaml:"source_type_boost_weight"`
}

// SourceTypesConfig represents the complete source types configuration
// SourceTypesConfig 表示完整的来源类型配置
type SourceTypesConfig struct {
	SourceTypes            map[string]SourceTypeConfig       `yaml:"source_types"`
	RegionalSources        map[string]RegionalSourceConfig   `yaml:"regional_sources"`
	DimensionSourceMapping map[string]DimensionSourceMapping `yaml:"dimension_source_mapping"`
	QueryTypeDimensions    map[string][]string               `yaml:"query_type_dimensions"`
	EntityGapQueries       map[string]EntityGapQuery         `yaml:"entity_gap_queries"`
	SearchStrategies       map[string]SearchStrategyConfig   `yaml:"search_strategies"`
	ResultMerging          ResultMergingConfig               `yaml:"result_merging"`
	EntityRelevance        EntityRelevanceConfig             `yaml:"entity_relevance"`
	PrefetchSources        map[string][]PrefetchSource       `yaml:"prefetch_sources"` // query_type -> sources
}

var (
	sourceTypesConfig    *SourceTypesConfig
	sourceTypesConfigMu  sync.RWMutex
	sourceTypesConfigErr error
	sourceTypesLoaded    bool
)

// LoadSourceTypes loads the source_types.yaml configuration file
// LoadSourceTypes 加载 source_types.yaml 配置文件
func LoadSourceTypes() (*SourceTypesConfig, error) {
	sourceTypesConfigMu.RLock()
	if sourceTypesLoaded {
		cfg, err := sourceTypesConfig, sourceTypesConfigErr
		sourceTypesConfigMu.RUnlock()
		return cfg, err
	}
	sourceTypesConfigMu.RUnlock()

	// Upgrade to write lock for initialization
	sourceTypesConfigMu.Lock()
	defer sourceTypesConfigMu.Unlock()

	// Double-check after acquiring write lock
	if sourceTypesLoaded {
		return sourceTypesConfig, sourceTypesConfigErr
	}

	sourceTypesConfig, sourceTypesConfigErr = loadSourceTypesFromFile()
	sourceTypesLoaded = true
	return sourceTypesConfig, sourceTypesConfigErr
}

// loadSourceTypesFromFile loads source types from the config file
// loadSourceTypesFromFile 从配置文件加载来源类型
func loadSourceTypesFromFile() (*SourceTypesConfig, error) {
	cfgPath := os.Getenv("SOURCE_TYPES_CONFIG_PATH")
	if cfgPath == "" {
		// Try common paths
		candidates := []string{
			"/app/config/source_types.yaml",
			"config/source_types.yaml",
			"../../config/source_types.yaml",
		}
		for _, c := range candidates {
			if _, err := os.Stat(c); err == nil {
				cfgPath = c
				break
			}
		}
	}

	if cfgPath == "" {
		// Return default config if file not found
		// 文件不存在时返回默认配置
		return defaultSourceTypesConfig(), nil
	}

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read source_types.yaml: %w", err)
	}

	var cfg SourceTypesConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse source_types.yaml: %w", err)
	}

	// Apply defaults for missing values
	applySourceTypeDefaults(&cfg)

	return &cfg, nil
}

// defaultSourceTypesConfig returns a minimal default configuration
func defaultSourceTypesConfig() *SourceTypesConfig {
	return &SourceTypesConfig{
		SourceTypes: map[string]SourceTypeConfig{
			"official": {
				Description:   "Company websites, .gov, .edu domains",
				Strategy:      "site_filter",
				PriorityBoost: 1.5,
				MaxResults:    5,
			},
			"aggregator": {
				Description:   "Crunchbase, PitchBook, Wikipedia, LinkedIn",
				Strategy:      "site_filter",
				Sites:         []string{"crunchbase.com", "pitchbook.com", "wikipedia.org", "linkedin.com"},
				PriorityBoost: 1.2,
				MaxResults:    8,
			},
			"news": {
				Description:   "TechCrunch, Reuters, industry publications",
				Strategy:      "category",
				Sites:         []string{"techcrunch.com", "reuters.com", "bloomberg.com"},
				RecencyDays:   365,
				PriorityBoost: 1.0,
				MaxResults:    10,
			},
			"academic": {
				Description:   "arXiv, Google Scholar, PubMed",
				Strategy:      "category",
				Sites:         []string{"arxiv.org", "scholar.google.com", "pubmed.ncbi.nlm.nih.gov"},
				PriorityBoost: 1.3,
				MaxResults:    8,
			},
		},
		RegionalSources: map[string]RegionalSourceConfig{
			"local_cn": {
				Description:   "Chinese market intelligence sources",
				Language:      "zh",
				Strategy:      "site_filter",
				Sites:         []string{"36kr.com", "iyiou.com", "tianyancha.com"},
				QueryTemplate: "{localized_name} {dimension}",
				PriorityBoost: 1.1,
				MaxResults:    5,
			},
			"local_jp": {
				Description:   "Japanese market intelligence sources",
				Language:      "ja",
				Strategy:      "site_filter",
				Sites:         []string{"nikkei.com", "prtimes.jp"},
				QueryTemplate: "{localized_name} {dimension}",
				PriorityBoost: 1.1,
				MaxResults:    5,
			},
		},
		ResultMerging: ResultMergingConfig{
			URLNormalization:      true,
			SimilarityThreshold:   0.85,
			MaxPerDomain:          3,
			EnsureSourceDiversity: true,
			RelevanceWeight:       0.4,
			CredibilityWeight:     0.3,
			RecencyWeight:         0.2,
			SourceTypeBoostWeight: 0.1,
		},
	}
}

// applySourceTypeDefaults applies default values for missing configurations
// applySourceTypeDefaults 为缺失的配置填充默认值
func applySourceTypeDefaults(cfg *SourceTypesConfig) {
	if cfg.ResultMerging.MaxPerDomain == 0 {
		cfg.ResultMerging.MaxPerDomain = 3
	}
	if cfg.ResultMerging.SimilarityThreshold == 0 {
		cfg.ResultMerging.SimilarityThreshold = 0.85
	}

	for name, st := range cfg.SourceTypes {
		if st.MaxResults == 0 {
			st.MaxResults = 10
		}
		if st.PriorityBoost == 0 {
			st.PriorityBoost = 1.0
		}
		cfg.SourceTypes[name] = st
	}

	for name, rs := range cfg.RegionalSources {
		if rs.MaxResults == 0 {
			rs.MaxResults = 5
		}
		if rs.PriorityBoost == 0 {
			rs.PriorityBoost = 1.0
		}
		cfg.RegionalSources[name] = rs
	}
}

// GetSourceType returns the configuration for a specific source type
// GetSourceType 返回指定来源类型的配置
func (c *SourceTypesConfig) GetSourceType(name string) (SourceTypeConfig, bool) {
	st, ok := c.SourceTypes[name]
	return st, ok
}

// GetRegionalSource returns the configuration for a specific regional source
// GetRegionalSource 返回指定区域来源的配置
func (c *SourceTypesConfig) GetRegionalSource(name string) (RegionalSourceConfig, bool) {
	rs, ok := c.RegionalSources[name]
	return rs, ok
}

// GetDimensionSources returns the recommended source types for a dimension
// GetDimensionSources 返回某个维度推荐的来源类型
func (c *SourceTypesConfig) GetDimensionSources(dimension string) ([]string, []string) {
	mapping, ok := c.DimensionSourceMapping[dimension]
	if !ok {
		// Default sources if no mapping found
		return []string{"news", "aggregator"}, []string{}
	}
	return mapping.Primary, mapping.Secondary
}

// GetSitesForSourceType returns the sites list for a source type
// GetSitesForSourceType 返回某来源类型的站点列表
func (c *SourceTypesConfig) GetSitesForSourceType(sourceType string) []string {
	if st, ok := c.SourceTypes[sourceType]; ok {
		return st.Sites
	}
	if rs, ok := c.RegionalSources[sourceType]; ok {
		return rs.Sites
	}
	return nil
}

// GetExaCategoryForSourceType returns the Exa category for a source type
// GetExaCategoryForSourceType 返回某来源类型对应的 Exa 分类
func (c *SourceTypesConfig) GetExaCategoryForSourceType(sourceType string) string {
	if st, ok := c.SourceTypes[sourceType]; ok && st.ExaCategory != nil {
		return *st.ExaCategory
	}
	return ""
}

// GetGapQueriesForDimension returns gap-filling queries for a dimension
// GetGapQueriesForDimension 返回某个维度的补漏查询
func (c *SourceTypesConfig) GetGapQueriesForDimension(dimension string) []string {
	if mapping, ok := c.DimensionSourceMapping[dimension]; ok {
		return mapping.GapQueries
	}
	return nil
}

// GetDimensionsForQueryType returns the dimensions for a query type
// GetDimensionsForQueryType 返回某个查询类型对应的维度
func (c *SourceTypesConfig) GetDimensionsForQueryType(queryType string) []string {
	if dims, ok := c.QueryTypeDimensions[queryType]; ok {
		return dims
	}
	// Default to company dimensions
	return []string{"entity_identity", "business_model", "market_position"}
}

// GetEntityGapQueries returns gap-filling query patterns for a missing info type
// GetEntityGapQueries 返回某类缺失信息的补漏查询模式
func (c *SourceTypesConfig) GetEntityGapQueries(infoType string) ([]string, []string) {
	if gq, ok := c.EntityGapQueries[infoType]; ok {
		return gq.Queries, gq.SourceTypes
	}
	return nil, nil
}

// NormalizeDimensionName converts a dimension name to its normalized key form
// e.g., "Entity Identity" -> "entity_identity", "Financial Performance" -> "financial_performance"
// NormalizeDimensionName 将维度名转换为规范化键形式
// 如 "Entity Identity" -> "entity_identity"、"Financial Performance" -> "financial_performance"
func NormalizeDimensionName(dimension string) string {
	// Convert to lowercase and replace spaces with underscores
	// 转为小写并把空格替换为下划线
	normalized := strings.ToLower(dimension)
	normalized = strings.ReplaceAll(normalized, " ", "_")
	normalized = strings.ReplaceAll(normalized, "-", "_")
	// Remove special characters
	result := strings.Builder{}
	for _, r := range normalized {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '_' {
			result.WriteRune(r)
		}
	}
	return result.String()
}

// GetSourceTypesForDimension returns source types for a dimension, with normalization
// GetSourceTypesForDimension 返回某个维度的来源类型（带规范化）
func (c *SourceTypesConfig) GetSourceTypesForDimension(dimension string) (primary, secondary []string) {
	// Try exact match first
	if mapping, ok := c.DimensionSourceMapping[dimension]; ok {
		return mapping.Primary, mapping.Secondary
	}
	// Try normalized match
	normalized := NormalizeDimensionName(dimension)
	if mapping, ok := c.DimensionSourceMapping[normalized]; ok {
		return mapping.Primary, mapping.Secondary
	}
	// Default fallback
	return []string{"news", "aggregator"}, []string{}
}

// BuildSiteFilterQueries builds queries with site: prefix for a source type
// BuildSiteFilterQueries 为某来源类型构造带 site: 前缀的查询
func (c *SourceTypesConfig) BuildSiteFilterQueries(query, sourceType string) []string {
	sites := c.GetSitesForSourceType(sourceType)
	if len(sites) == 0 {
		return []string{query}
	}

	queries := make([]string, 0, len(sites))
	for _, site := range sites {
		queries = append(queries, fmt.Sprintf("site:%s %s", site, query))
	}
	return queries
}

// GetAllSourceTypes returns all available source type names (global + regional)
// GetAllSourceTypes 返回全部可用来源类型名（全局 + 区域）
func (c *SourceTypesConfig) GetAllSourceTypes() []string {
	types := make([]string, 0, len(c.SourceTypes)+len(c.RegionalSources))
	for name := range c.SourceTypes {
		types = append(types, name)
	}
	for name := range c.RegionalSources {
		types = append(types, name)
	}
	return types
}

// GetRegionalSourcesForLanguage returns regional sources matching a language
// GetRegionalSourcesForLanguage 返回与指定语言匹配的区域来源
func (c *SourceTypesConfig) GetRegionalSourcesForLanguage(lang string) []string {
	var sources []string
	for name, rs := range c.RegionalSources {
		if rs.Language == lang {
			sources = append(sources, name)
		}
	}
	return sources
}

// ReloadSourceTypes forces a reload of the source types configuration
// This can be used for hot-reload scenarios
// ReloadSourceTypes 强制重新加载来源类型配置
// 可用于热重载场景
func ReloadSourceTypes() (*SourceTypesConfig, error) {
	sourceTypesConfigMu.Lock()
	defer sourceTypesConfigMu.Unlock()

	// Force reload by resetting loaded flag and reloading
	// 通过重置已加载标志并重新加载来强制刷新
	cfg, err := loadSourceTypesFromFile()
	sourceTypesConfig = cfg
	sourceTypesConfigErr = err
	sourceTypesLoaded = true
	return cfg, err
}

// GetConfigPath returns the resolved config file path for debugging
// GetConfigPath 返回解析后的配置文件路径，便于调试
func GetSourceTypesConfigPath() string {
	cfgPath := os.Getenv("SOURCE_TYPES_CONFIG_PATH")
	if cfgPath != "" {
		return cfgPath
	}

	candidates := []string{
		"/app/config/source_types.yaml",
		"config/source_types.yaml",
		"../../config/source_types.yaml",
	}
	for _, c := range candidates {
		absPath, _ := filepath.Abs(c)
		if _, err := os.Stat(absPath); err == nil {
			return absPath
		}
	}
	return "(using defaults)"
}

// GetPrefetchURLs returns URLs to prefetch for a given entity name and query type
// The returned URLs are sorted by priority (lower priority value = earlier in list)
// GetPrefetchURLs 返回给定实体名与查询类型需要预取的 URL
// 返回的 URL 按优先级排序（priority 值越小越靠前）
func GetPrefetchURLs(entityName string, queryType string) []string {
	cfg, err := LoadSourceTypes()
	if err != nil || cfg == nil {
		return nil
	}

	sources, ok := cfg.PrefetchSources[queryType]
	if !ok || len(sources) == 0 {
		return nil
	}

	// Generate slug from entity name: lowercase, spaces to hyphens
	// 由实体名生成 slug：转小写、空格转连字符
	slug := strings.ToLower(strings.TrimSpace(entityName))
	slug = strings.ReplaceAll(slug, " ", "-")
	// Remove special characters that might break URLs
	// 移除可能破坏 URL 的特殊字符
	slug = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			return r
		}
		return -1
	}, slug)

	// Also create underscore variant
	slugUnderscore := strings.ReplaceAll(slug, "-", "_")

	var urls []string
	seen := make(map[string]bool)

	for _, src := range sources {
		// Replace placeholders in pattern
		url := src.Pattern
		url = strings.ReplaceAll(url, "{slug}", slug)
		url = strings.ReplaceAll(url, "{slug_underscore}", slugUnderscore)

		// Ensure https:// prefix
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			url = "https://" + url
		}

		if !seen[url] {
			seen[url] = true
			urls = append(urls, url)
		}
	}

	return urls
}
