package metadata

import (
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

// Snippet length constants for citation extraction
const (
	MaxSnippetLength    = 4000 // Maximum characters for snippet content (V3: increased for better citation matching)
	MinSnippetLength    = 30   // Minimum characters for a valid snippet
	MinLLMSignalMatches = 2    // Required matches for weak signals and structured fields
)

// Citation represents a single source citation with quality metrics
// Citation 表示单条来源引用及其质量指标
type Citation struct {
	URL              string     `json:"url"`
	Title            string     `json:"title"`
	Source           string     `json:"source"`      // domain name
	SourceType       string     `json:"source_type"` // web|news|academic|social
	ToolSource       string     `json:"tool_source"` // Citation V2: "search" or "fetch" (origin tool type)
	RetrievedAt      time.Time  `json:"retrieved_at"`
	PublishedDate    *time.Time `json:"published_date,omitempty"`
	RelevanceScore   float64    `json:"relevance_score"`   // from search tool
	QualityScore     float64    `json:"quality_score"`     // recency + completeness
	CredibilityScore float64    `json:"credibility_score"` // domain reputation
	AgentID          string     `json:"agent_id"`
	Snippet          string     `json:"snippet"`
	// P0-A: Fetch failure structuring for Citation V2
	// P0-A：为 Citation V2 结构化抓取失败信息
	StatusCode    int    `json:"status_code,omitempty"`    // HTTP status code (0 = unknown, 200 = success, 4xx/5xx = error)
	BlockedReason string `json:"blocked_reason,omitempty"` // Non-empty if content was blocked/invalid
	Content       string `json:"content,omitempty"`        // Full content for IsValid() check
}

// IsValid returns true if the citation has valid, usable content for verification.
// Used by Citation V2 to filter out invalid sources before VerifyBatch.
// 引用具有可用于校验的有效内容时 IsValid 返回 true。
// Citation V2 在 VerifyBatch 之前用它过滤无效来源。
func (c *Citation) IsValid() bool {
	// HTTP 4xx/5xx = invalid
	if c.StatusCode >= 400 {
		return false
	}
	// Blocked content = invalid
	if c.BlockedReason != "" {
		return false
	}
	// Content too short = invalid
	contentLen := len(c.Content)
	if contentLen == 0 {
		contentLen = len(c.Snippet)
	}
	if contentLen < MinSnippetLength {
		return false
	}
	return true
}

// CitationStats provides aggregate metrics for collected citations
// CitationStats 提供已收集引用的汇总指标
type CitationStats struct {
	TotalSources    int     `json:"total_sources"`
	UniqueDomains   int     `json:"unique_domains"`
	AvgQuality      float64 `json:"avg_quality"`
	AvgCredibility  float64 `json:"avg_credibility"`
	SourceDiversity float64 `json:"source_diversity"` // unique_domains / total_sources

	// P1-4: Extended metrics for observability
	QualityBuckets map[string]int `json:"quality_buckets,omitempty"` // "low", "medium", "high" → count
	TopDomains     []DomainCount  `json:"top_domains,omitempty"`     // Top 10 domains by count
	DuplicateURLs  int            `json:"duplicate_urls,omitempty"`  // Number of duplicate URLs removed
	PerAgentCount  map[string]int `json:"per_agent_count,omitempty"` // agent_id → citation count
}

// DomainCount represents a domain and its citation count
// DomainCount 表示某个域名及其引用数
type DomainCount struct {
	Domain string `json:"domain"`
	Count  int    `json:"count"`
}

// CredibilityConfig holds domain credibility scoring rules
type CredibilityConfig struct {
	CredibilityRules struct {
		TLDPatterns []struct {
			Suffix      string  `yaml:"suffix"`
			Score       float64 `yaml:"score"`
			Description string  `yaml:"description"`
		} `yaml:"tld_patterns"`

		DomainGroups []struct {
			Category    string   `yaml:"category"`
			Score       float64  `yaml:"score"`
			Description string   `yaml:"description"`
			Domains     []string `yaml:"domains"`
		} `yaml:"domain_groups"`

		DefaultScore float64 `yaml:"default_score"`
	} `yaml:"credibility_rules"`

	QualityGates struct {
		MinCredibilityScore float64 `yaml:"min_credibility_score"`
		PreferredScore      float64 `yaml:"preferred_score"`
		HighQualityScore    float64 `yaml:"high_quality_score"`
	} `yaml:"quality_gates"`

	DiversityRules struct {
		MaxPerDomain     int     `yaml:"max_per_domain"`
		MinUniqueDomains int     `yaml:"min_unique_domains"`
		DiversityBonus   float64 `yaml:"diversity_bonus"`
	} `yaml:"diversity_rules"`
}

var (
	credibilityConfig     *CredibilityConfig
	credibilityConfigOnce sync.Once
)

// GetCredibilityConfigPath returns the config path, checking env var first
// GetCredibilityConfigPath 返回配置路径，优先检查环境变量
func GetCredibilityConfigPath() string {
	if envPath := os.Getenv("CITATION_CREDIBILITY_CONFIG"); envPath != "" {
		return envPath
	}
	return "/app/config/citation_credibility.yaml"
}

// LoadCredibilityConfig loads credibility scoring rules from config file
// LoadCredibilityConfig 从配置文件加载可信度评分规则
func LoadCredibilityConfig() *CredibilityConfig {
	credibilityConfigOnce.Do(func() {
		configPath := GetCredibilityConfigPath()

		data, err := os.ReadFile(configPath)
		if err != nil {
			log.Printf("Warning: Failed to load citation credibility config from %s: %v. Using defaults.", configPath, err)
			credibilityConfig = getDefaultCredibilityConfig()
			return
		}

		var config CredibilityConfig
		if err := yaml.Unmarshal(data, &config); err != nil {
			log.Printf("Warning: Failed to parse citation credibility config: %v. Using defaults.", err)
			credibilityConfig = getDefaultCredibilityConfig()
			return
		}

		credibilityConfig = &config
		log.Printf("Loaded citation credibility config from %s", configPath)
	})

	return credibilityConfig
}

// isCitationsDebugEnabled returns true when verbose citation debug logging is enabled
// 启用详细的引用调试日志时 isCitationsDebugEnabled 返回 true
func isCitationsDebugEnabled() bool {
	v := os.Getenv("CITATIONS_DEBUG")
	if v == "1" || strings.EqualFold(v, "true") || strings.EqualFold(v, "on") {
		return true
	}
	return false
}

// ResetCredibilityConfigForTest resets the singleton for testing purposes
// This should only be called from test code
// ResetCredibilityConfigForTest 为测试重置单例
// 仅应在测试代码中调用
func ResetCredibilityConfigForTest() {
	credibilityConfigOnce = sync.Once{}
	credibilityConfig = nil
}

// getDefaultCredibilityConfig returns fallback config when file is unavailable
// 文件不可用时 getDefaultCredibilityConfig 返回兜底配置
func getDefaultCredibilityConfig() *CredibilityConfig {
	return &CredibilityConfig{
		CredibilityRules: struct {
			TLDPatterns []struct {
				Suffix      string  `yaml:"suffix"`
				Score       float64 `yaml:"score"`
				Description string  `yaml:"description"`
			} `yaml:"tld_patterns"`
			DomainGroups []struct {
				Category    string   `yaml:"category"`
				Score       float64  `yaml:"score"`
				Description string   `yaml:"description"`
				Domains     []string `yaml:"domains"`
			} `yaml:"domain_groups"`
			DefaultScore float64 `yaml:"default_score"`
		}{
			TLDPatterns: []struct {
				Suffix      string  `yaml:"suffix"`
				Score       float64 `yaml:"score"`
				Description string  `yaml:"description"`
			}{
				{Suffix: ".edu", Score: 0.85, Description: "Educational"},
				{Suffix: ".gov", Score: 0.80, Description: "Government"},
			},
			DomainGroups: []struct {
				Category    string   `yaml:"category"`
				Score       float64  `yaml:"score"`
				Description string   `yaml:"description"`
				Domains     []string `yaml:"domains"`
			}{},
			DefaultScore: 0.60,
		},
	}
}

// NormalizeURL cleans and normalizes a URL for deduplication
// - Converts to lowercase
// - Removes trailing slashes
// - Removes common query parameters (utm_*, fbclid, etc.)
// - Removes fragment identifiers (#)
// NormalizeURL 清洗并规范化 URL，用于去重
// - 转为小写
// - 去掉末尾斜杠
// - 去掉常见查询参数（utm_*、fbclid 等）
// - 去掉片段标识（#）
func NormalizeURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	// Validate URL has required components
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid URL: missing scheme or host")
	}

	// Only allow http/https schemes
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", fmt.Errorf("invalid URL scheme: %s", parsed.Scheme)
	}

	// Normalize scheme to lowercase
	parsed.Scheme = scheme

	// Normalize host to lowercase
	parsed.Host = strings.ToLower(parsed.Host)

	// Remove www. prefix for consistency
	parsed.Host = strings.TrimPrefix(parsed.Host, "www.")

	// Remove fragment
	parsed.Fragment = ""

	// Remove tracking query parameters
	if parsed.RawQuery != "" {
		q := parsed.Query()
		trackingParams := []string{
			"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
			"fbclid", "gclid", "msclkid",
			"ref", "source",
		}
		for _, param := range trackingParams {
			q.Del(param)
		}
		parsed.RawQuery = q.Encode()
	}

	// Remove trailing slash from path (including root path "/" -> "")
	// 去掉路径末尾的斜杠（含根路径 "/" -> ""）
	parsed.Path = strings.TrimSuffix(parsed.Path, "/")

	return parsed.String(), nil
}

// ExtractDomain returns the lowercase host from a URL, removing any port and a
// leading "www." but preserving other subdomains when present.
// Example: "https://blog.example.com/path" -> "blog.example.com"
// ExtractDomain 返回 URL 的小写主机名，去掉端口和
// 开头的 "www."，但保留其它子域名。
// 例如："https://blog.example.com/path" -> "blog.example.com"
func ExtractDomain(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}

	host := strings.ToLower(parsed.Host)

	// Remove port if present
	if colonIndex := strings.Index(host, ":"); colonIndex != -1 {
		host = host[:colonIndex]
	}

	// Remove www. prefix
	host = strings.TrimPrefix(host, "www.")

	return host, nil
}

// ScoreQuality calculates quality score based on recency and completeness
// Formula: relevance * 0.7 + recency * 0.3 + completeness bonus
//
// Recency scoring (30-day decay):
// - < 7 days: 1.0
// - 7-30 days: 0.7
// - 30-90 days: 0.4
// - > 90 days: 0.2
//
// Completeness bonus (+0.1 if has published_date, title, snippet)
// ScoreQuality 依据时效性与完整性计算质量分
// 公式：relevance * 0.7 + recency * 0.3 + 完整性加分
// 时效性评分（30 天衰减）：
// - < 7 天：1.0
// - 7-30 天：0.7
// - 30-90 天：0.4
// - > 90 天：0.2
// 完整性加分（有 published_date、title、snippet 时 +0.1）
func ScoreQuality(relevance float64, publishedDate *time.Time, hasTitle, hasSnippet bool, now time.Time) float64 {
	// Base score from relevance (70% weight)
	score := relevance * 0.7

	// Recency score (30% weight)
	recencyScore := 0.2 // default for old/unknown dates
	if publishedDate != nil {
		daysSincePublished := now.Sub(*publishedDate).Hours() / 24
		switch {
		case daysSincePublished < 7:
			recencyScore = 1.0
		case daysSincePublished < 30:
			recencyScore = 0.7
		case daysSincePublished < 90:
			recencyScore = 0.4
		default:
			recencyScore = 0.2
		}
	}
	score += recencyScore * 0.3

	// Completeness bonus
	completeness := 0.0
	if publishedDate != nil {
		completeness += 0.033
	}
	if hasTitle {
		completeness += 0.033
	}
	if hasSnippet {
		completeness += 0.034
	}
	score += completeness

	// Cap at 1.0
	if score > 1.0 {
		score = 1.0
	}

	return score
}

// ScoreCredibility calculates credibility score based on domain reputation
// Loads rules from config/citation_credibility.yaml
// Falls back to hardcoded defaults if config unavailable
// ScoreCredibility 依据域名信誉计算可信度分
// 从 config/citation_credibility.yaml 加载规则
// 配置不可用时回退到硬编码默认值
func ScoreCredibility(domain string) float64 {
	config := LoadCredibilityConfig()
	domain = strings.ToLower(domain)

	// Check TLD patterns first (highest priority)
	for _, tldPattern := range config.CredibilityRules.TLDPatterns {
		if strings.HasSuffix(domain, tldPattern.Suffix) {
			return tldPattern.Score
		}
	}

	// Helper for safe domain matching (exact match or subdomain boundary)
	// 安全域名匹配的辅助函数（精确匹配或子域名边界）
	domainMatches := func(host, pattern string) bool {
		host = strings.ToLower(host)
		pattern = strings.ToLower(pattern)
		if host == pattern {
			return true
		}
		// Allow subdomains (e.g., docs.github.com matches github.com)
		// 允许子域名（如 docs.github.com 匹配 github.com）
		if strings.HasSuffix(host, "."+pattern) {
			return true
		}
		return false
	}

	// Check domain groups
	for _, group := range config.CredibilityRules.DomainGroups {
		for _, knownDomain := range group.Domains {
			if domainMatches(domain, knownDomain) {
				return group.Score
			}
		}
	}

	// Return default score
	if config.CredibilityRules.DefaultScore > 0 {
		return config.CredibilityRules.DefaultScore
	}
	return 0.60
}

// CalculateSourceDiversity calculates diversity metric
// Formula: unique_domains / total_sources
// Higher is better (more diverse sources)
// CalculateSourceDiversity 计算多样性指标
// 公式：unique_domains / total_sources
// 越高越好（来源越多样）
func CalculateSourceDiversity(citations []Citation) float64 {
	if len(citations) == 0 {
		return 0.0
	}

	domainSet := make(map[string]bool)
	for _, c := range citations {
		domainSet[c.Source] = true
	}

	return float64(len(domainSet)) / float64(len(citations))
}

// extractCitationFromSearchResult extracts a Citation from a web_search result
// extractCitationFromSearchResult 从 web_search 结果中提取 Citation
func extractCitationFromSearchResult(result map[string]any, agentID string, now time.Time) (*Citation, error) {
	// Extract required fields
	urlStr, ok := result["url"].(string)
	if !ok || urlStr == "" {
		return nil, fmt.Errorf("missing or invalid url")
	}

	title, _ := result["title"].(string)
	snippet, _ := result["text"].(string)
	if snippet == "" {
		snippet, _ = result["snippet"].(string)
	}

	// Try to get content for snippet fallback
	// 尝试获取内容作为 snippet 兜底
	content, _ := result["content"].(string)

	// Normalize URL for deduplication
	normalizedURL, err := NormalizeURL(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize URL: %w", err)
	}

	// Extract domain
	domain, err := ExtractDomain(normalizedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract domain: %w", err)
	}

	// Ensure snippet is not empty
	snippet = ensureSnippet(snippet, content, title, normalizedURL, MinSnippetLength)

	// Extract scores and metadata
	relevanceScore := 0.5 // default
	if score, ok := result["score"].(float64); ok {
		relevanceScore = score
	}

	// Try to parse published date
	var publishedDate *time.Time
	if pubDateStr, ok := result["published_date"].(string); ok && pubDateStr != "" {
		if parsed, err := time.Parse(time.RFC3339, pubDateStr); err == nil {
			publishedDate = &parsed
		}
	}

	// Determine source type from domain or category
	// 按域名或分类判定来源类型
	sourceType := "web"
	if category, ok := result["category"].(string); ok {
		sourceType = category
	}

	// Calculate quality and credibility scores
	qualityScore := ScoreQuality(relevanceScore, publishedDate, title != "", snippet != "", now)
	credibilityScore := ScoreCredibility(domain)

	// Citation V2: extract tool_source field
	toolSource := "search" // default for search results
	if ts, ok := result["tool_source"].(string); ok && ts != "" {
		toolSource = ts
	}

	return &Citation{
		URL:              normalizedURL,
		Title:            title,
		Source:           domain,
		SourceType:       sourceType,
		ToolSource:       toolSource,
		RetrievedAt:      now,
		PublishedDate:    publishedDate,
		RelevanceScore:   relevanceScore,
		QualityScore:     qualityScore,
		CredibilityScore: credibilityScore,
		AgentID:          agentID,
		Snippet:          snippet,
	}, nil
}

// extractCitationFromFetchResult extracts a Citation from a web_fetch result
// extractCitationFromFetchResult 从 web_fetch 结果中提取 Citation
func extractCitationFromFetchResult(result map[string]any, agentID string, now time.Time) (*Citation, error) {
	// Extract required fields
	urlStr, ok := result["url"].(string)
	if !ok || urlStr == "" {
		return nil, fmt.Errorf("missing or invalid url")
	}

	title, _ := result["title"].(string)
	content, _ := result["content"].(string)

	// Try explicit snippet field first, then fall back to content
	// 优先使用显式的 snippet 字段，否则回退到 content
	snippet := ""
	if s, ok := result["snippet"].(string); ok && len(s) > 0 {
		snippet = s
	} else if len(content) > 0 {
		snippet = truncateRunes(content, MaxSnippetLength)
	}

	// Normalize URL
	normalizedURL, err := NormalizeURL(urlStr)
	if err != nil {
		return nil, fmt.Errorf("failed to normalize URL: %w", err)
	}

	// Extract domain
	domain, err := ExtractDomain(normalizedURL)
	if err != nil {
		return nil, fmt.Errorf("failed to extract domain: %w", err)
	}

	// Ensure snippet is not empty
	snippet = ensureSnippet(snippet, content, title, normalizedURL, MinSnippetLength)

	// Try to parse published date
	var publishedDate *time.Time
	if pubDateStr, ok := result["published_date"].(string); ok && pubDateStr != "" {
		if parsed, err := time.Parse(time.RFC3339, pubDateStr); err == nil {
			publishedDate = &parsed
		}
	}

	// web_fetch results are fetched for detailed analysis, give higher relevance
	// web_fetch 结果是抓取来用于详细分析的，给予更高相关度
	relevanceScore := 0.8

	// Calculate quality and credibility scores
	qualityScore := ScoreQuality(relevanceScore, publishedDate, title != "", snippet != "", now)
	credibilityScore := ScoreCredibility(domain)

	// Citation V2: extract tool_source field
	toolSource := "fetch" // default for fetch results
	if ts, ok := result["tool_source"].(string); ok && ts != "" {
		toolSource = ts
	}

	// P0-A: Extract status_code and blocked_reason for validity filtering
	// P0-A：提取 status_code 与 blocked_reason 用于有效性过滤
	statusCode := 0 // 0 = unknown/legacy (treat as valid)
	if sc, ok := result["status_code"].(float64); ok {
		statusCode = int(sc)
	} else if sc, ok := result["status_code"].(int); ok {
		statusCode = sc
	}

	blockedReason := ""
	if br, ok := result["blocked_reason"].(string); ok {
		blockedReason = br
	}

	return &Citation{
		URL:              normalizedURL,
		Title:            title,
		Source:           domain,
		SourceType:       "web",
		ToolSource:       toolSource,
		RetrievedAt:      now,
		PublishedDate:    publishedDate,
		RelevanceScore:   relevanceScore,
		QualityScore:     qualityScore,
		CredibilityScore: credibilityScore,
		AgentID:          agentID,
		Snippet:          snippet,
		// P0-A: Fetch failure fields
		StatusCode:    statusCode,
		BlockedReason: blockedReason,
		Content:       content, // Store for IsValid() check
	}, nil
}

// extractCitationsFromResponse attempts to parse citations from agent response text
// This is a fallback when tool_executions are missing but the response contains structured search results
// extractCitationsFromResponse 尝试从 agent response 文本中解析引用
// 当 tool_executions 缺失但 response 中含结构化搜索结果时作为兜底
func extractCitationsFromResponse(response string, agentID string, now time.Time) []Citation {
	var citations []Citation

	// Try to parse response as JSON array of search results
	// Common format: [{"url": "...", "title": "...", "snippet": "...", ...}, ...]
	// 尝试把 response 当作 JSON 数组的搜索结果解析
	// 常见格式：[{"url": "...", "title": "...", "snippet": "...", ...}, ...]
	var results []map[string]any
	if err := json.Unmarshal([]byte(response), &results); err != nil {
		return citations // Not JSON array, no fallback possible
	}

	// Extract citations from parsed results
	for _, result := range results {
		if citation, err := extractCitationFromSearchResult(result, agentID, now); err == nil {
			citations = append(citations, *citation)
		}
	}

	return citations
}

// containsLLMSignals detects if text contains LLM-generated content signals.
// Uses a three-tier approach:
// 1. Strong signals: immediate pollution (tool-call/XML residue)
// 2. JSON detection: requires ≥2 JSON keys to trigger
// 3. Weak signals: need ≥2 matches AND at least one extraction keyword
// containsLLMSignals 判断文本是否含有 LLM 生成内容的特征。
// 采用三层判定：
// 1. 强信号：命中即判定污染（tool-call/XML 残留）
// 2. JSON 检测：需要 ≥2 个 JSON 键才触发
// 3. 弱信号：需要 ≥2 次匹配且至少一个提取类关键词
func containsLLMSignals(text string) bool {
	if text == "" {
		return false
	}

	// Strong signals: any match = immediate pollution (high-confidence tool/XML residue only)
	// 强信号：任一命中即判定污染（仅限高置信度的 tool/XML 残留）
	strongSignals := []string{
		// XML/function call residue (generalized prefix matching)
		// XML/函数调用残留（广义前缀匹配）
		"</invoke>", "</function_calls>", "<function_calls>",
		"<parameter>", "</parameter>", "<invoke ",
		"<web_fetch>", "</web_fetch>", "<web_search>", "</web_search>",
		"<web_subpage_fetch>", "</web_subpage_fetch>",
		"<attempt_", "</attempt_", // Generalized matching for <attempt_*> series

		// Tool call structure residue (standalone words and JSON format)
		// tool call 结构残留（独立词与 JSON 形式）
		"tool_executions",
		"web_subpage_fetch", // standalone match
		`"tool": "web_fetch"`, `"tool": "web_search"`,
		`'tool': 'web_fetch'`, `'tool': 'web_search'`,

		// High-confidence Agent templates (complete action+object combinations)
		// 高置信度的 Agent 模板（完整的动作+对象组合）
		"信息提取结果", "情報抽出結果",
		"公司信息提取", "子页面分析",
		"我已成功从", "successfully extracted from",
		"深入抓取", "サブページから抽出",

		// Agent planning/meta-commentary statements (Phase 3)
		// Agent 规划/元评论式语句（阶段 3）
		"Direct fetch of",                  // Agent suggested action
		"were not included in the initial", // Agent meta-commentary
		"would be needed (these",           // Exact match with parenthesis

		// Agent structured output format
		"**URL:**",              // Agent extracted URL field
		"Key Business Products", // Agent analysis header
		"Products Identified",   // Agent analysis header variant
	}

	for _, sig := range strongSignals {
		if strings.Contains(text, sig) {
			return true
		}
	}

	// Extraction keywords: weak signals must be combined with these to trigger
	// 提取类关键词：弱信号必须与这些词同时出现才触发
	extractionKeywords := []string{
		// Chinese
		"提取", "抽取", "整理", "获取结果",
		// Japanese
		"抽出", "取得",
		// English
		"extract", "retriev",
	}

	hasExtractionKeyword := false
	for _, kw := range extractionKeywords {
		if strings.Contains(text, kw) {
			hasExtractionKeyword = true
			break
		}
	}

	// JSON-like structure detection: requires ≥2 JSON keys present
	// 类 JSON 结构检测：需要 ≥2 个 JSON 键
	jsonKeys := []string{
		`"results":`, `'results':`,
		`"title":`, `{'title':`, `'title':`,
		`"url":`, `'url':`,
		`"source":`, `'source':`,
		`"position":`, `'position':`,
	}
	jsonKeyCount := 0
	for _, jk := range jsonKeys {
		if strings.Contains(text, jk) {
			jsonKeyCount++
		}
	}
	if jsonKeyCount >= MinLLMSignalMatches {
		return true
	}

	// Structured field detection (Phase 3): requires ≥2 fields to trigger
	// 结构化字段检测（阶段 3）：需要 ≥2 个字段才触发
	structuredFields := []string{
		"- **Industry:**",
		"- **Website:**",
		"- **Founded:**",
		"- **Headquarters:**",
		"- **CEO:**",
		"- **Revenue:**",
		"- **Employees:**",
		"- **Company:**",
	}
	fieldCount := 0
	for _, f := range structuredFields {
		if strings.Contains(text, f) {
			fieldCount++
		}
	}
	if fieldCount >= MinLLMSignalMatches {
		return true
	}

	// Weak signals: require ≥2 matches + extraction keyword
	// 弱信号：需要 ≥2 次匹配 + 提取类关键词
	if !hasExtractionKeyword {
		return false // No extraction keyword, weak signals don't trigger
	}

	weakSignals := []string{
		// Chinese weak signals (excluding high-frequency words like "以下是")
		"我需要", "让我", "基于我的", "关键信息",

		// Japanese weak signals (excluding "以下の", "情報を", etc.)
		"ツールで", "抽出しました", "取得しました",

		// English weak signals
		"I found", "Let me", "Based on my", "Here is the",

		// Markdown structure weak signals
		"## 基本", "## 概要", "## Summary",
		"**公司", "**企業", "**Company",
	}

	weakCount := 0
	for _, sig := range weakSignals {
		if strings.Contains(text, sig) {
			weakCount++
			if weakCount >= MinLLMSignalMatches {
				return true
			}
		}
	}

	return false
}

// ensureSnippet ensures a citation has a meaningful snippet, generating from available fields if missing.
// It also filters out LLM-generated pollution from snippets.
// ensureSnippet 确保引用具有有意义的 snippet，缺失时由可用字段生成。
// 它还会过滤 snippet 中的 LLM 生成污染。
func ensureSnippet(snippet, content, title, urlStr string, minLen int) string {
	// Clear polluted snippets that contain LLM-generated content
	// 清除含有 LLM 生成内容的受污染 snippet
	if containsLLMSignals(snippet) {
		snippet = ""
	}

	// Already has sufficient clean snippet
	if len([]rune(snippet)) >= minLen {
		return snippet
	}

	// Try content (also check for pollution)
	if len([]rune(content)) >= minLen && !containsLLMSignals(content) {
		return truncateRunes(content, MaxSnippetLength)
	}

	// Fallback to title + url for minimal context
	// 回退到 title + url 以提供最小 context
	if title != "" {
		return fmt.Sprintf("%s - %s", title, urlStr)
	}

	// Return whatever we have (only if it's not polluted)
	// 返回现有内容（仅当未被污染）
	if !containsLLMSignals(snippet) {
		return snippet
	}
	return ""
}

// extractCitationsFromPlainTextResponse extracts citations by scanning plain-text
// agent responses for HTTP/HTTPS URLs when structured tool outputs are absent.
// This is a conservative fallback to ensure metadata contains citations even when
// agents only mention sources narratively.
// extractCitationsFromPlainTextResponse 在缺少结构化 tool 输出时，
// 通过扫描纯文本 agent response 中的 HTTP/HTTPS URL 提取引用。
// 这是保守的兜底手段，确保 agent 仅叙述性提及来源时 metadata 仍含引用。
func extractCitationsFromPlainTextResponse(response string, agentID string, now time.Time) []Citation {
	var citations []Citation
	if strings.TrimSpace(response) == "" {
		return citations
	}

	// Regex for http(s) URLs, stop at whitespace or common trailing punctuation / tag delimiters
	// 匹配 http(s) URL 的正则，遇到空白或常见尾部标点/标签分隔符即停止
	urlRe := regexp.MustCompile(`https?://[^\s\]\)\>\<\"']+`)
	matches := urlRe.FindAllStringIndex(response, -1)
	if len(matches) == 0 {
		return citations
	}

	// Deduplicate per-response by normalized URL
	seen := make(map[string]bool)

	// Some models wrap URLs with tags like <url>...</url> or include encoded variants.
	// Strip common closing-tag suffix artifacts so citations remain valid URLs.
	// 部分模型会用 <url>...</url> 之类标签包裹 URL，或给出编码变体。
	// 去掉常见的闭合标签后缀残留，使引用仍是合法 URL。
	urlTagSuffixes := []string{
		"</url>", "</url", "%3c/url%3e", "%3c/url",
		"&lt;/url&gt;", "&lt;/url&gt", "&lt;/url",
	}
	stripURLTagSuffix := func(s string) string {
		for {
			before := s
			for _, suffix := range urlTagSuffixes {
				if strings.HasSuffix(strings.ToLower(s), suffix) {
					s = s[:len(s)-len(suffix)]
				}
			}
			if s == before {
				return s
			}
		}
	}

	// trimNonASCIISuffix truncates the URL at the first non-ASCII character
	// This handles cases where Chinese text (e.g., "（个人介绍）") is attached to URLs
	trimNonASCIISuffix := func(s string) string {
		for i := 0; i < len(s); i++ {
			if s[i] > 127 {
				return s[:i]
			}
		}
		return s
	}

	// Helper to build a small context snippet around the URL (UTF-8 safe)
	// 围绕 URL 构建小段 context snippet 的辅助函数（UTF-8 安全）
	getSnippet := func(start, end int) string {
		r := []rune(response)
		// Map byte indices to rune indices conservatively
		// Fallback: slice by bytes if mapping fails
		// Simplicity: approximate by converting entire string to runes and searching substring
		// 保守地把字节索引映射为 rune 索引
		// 映射失败时回退为按字节切片
		// 简化做法：把整个字符串转为 rune 后做子串查找
		urlText := response[start:end]
		ri := strings.Index(string(r), urlText)
		if ri < 0 {
			// Fallback: just return first 200 runes of the tail starting at start
			// 回退：从 start 开始返回尾部前 200 个 rune
			if start < len(response) {
				tail := []rune(response[start:])
				if len(tail) > 200 {
					return string(tail[:200]) + "..."
				}
				return string(tail)
			}
			return ""
		}
		lo := ri - 120
		hi := ri + len([]rune(urlText)) + 120
		if lo < 0 {
			lo = 0
		}
		if hi > len(r) {
			hi = len(r)
		}
		// Ensure lo <= hi after clamping (can happen with multi-byte chars at boundaries)
		// 夹取后确保 lo <= hi（边界处的多字节字符可能造成反序）
		if lo > hi {
			lo = 0
			if hi < len(r) {
				hi = len(r)
			}
		}
		if lo >= len(r) {
			return ""
		}
		snippet := string(r[lo:hi])
		if len([]rune(snippet)) > 220 {
			snippet = string([]rune(snippet)[:220]) + "..."
		}

		// Filter out LLM-generated content pollution
		// This prevents agent response text from being stored as webpage snippets
		// 过滤 LLM 生成的内容污染
		// 避免 agent response 文本被当作网页 snippet 存储
		if containsLLMSignals(snippet) {
			return ""
		}

		return snippet
	}

	for _, m := range matches {
		start, end := m[0], m[1]
		raw := response[start:end]
		// Trim trailing punctuation that often gets attached in prose
		// 去掉散文中常被一并带上的尾部标点
		raw = strings.TrimRight(raw, ",.;:)]}")
		raw = stripURLTagSuffix(raw)
		// Truncate at first non-ASCII character (e.g., Chinese parentheses)
		// 在首个非 ASCII 字符处截断（如中文括号）
		raw = trimNonASCIISuffix(raw)
		// Normalize URL
		normalized, err := NormalizeURL(raw)
		if err != nil || normalized == "" {
			continue
		}
		if seen[normalized] {
			continue
		}
		seen[normalized] = true

		// Skip sitemap, robots.txt, static assets, etc.
		// 跳过 sitemap、robots.txt、静态资源等
		if shouldSkipURL(normalized) {
			continue
		}

		domain, err := ExtractDomain(normalized)
		if err != nil || domain == "" {
			continue
		}

		snippet := getSnippet(start, end)

		// Conservative relevance for narrative references
		relevance := 0.4
		quality := ScoreQuality(relevance, nil, false, snippet != "", now)
		credibility := ScoreCredibility(domain)

		citations = append(citations, Citation{
			URL:              normalized,
			Title:            "", // Unknown in plain text; formatter will display URL/domain
			Source:           domain,
			SourceType:       "web",
			ToolSource:       "", // Citation V2: unknown origin (URL scan fallback)
			RetrievedAt:      now,
			PublishedDate:    nil,
			RelevanceScore:   relevance,
			QualityScore:     quality,
			CredibilityScore: credibility,
			AgentID:          agentID,
			Snippet:          snippet,
		})
	}

	if isCitationsDebugEnabled() {
		log.Printf("[citations] plain_text_extracted=%d", len(citations))
	}
	return citations
}

// extractCitationsFromToolOutput extracts citations from a tool execution output
// extractCitationsFromToolOutput 从 tool 执行输出中提取引用
func extractCitationsFromToolOutput(toolName string, output any, agentID string, now time.Time) []Citation {
	var citations []Citation

	if isCitationsDebugEnabled() {
		log.Printf("[citations] tool=%s output_type=%T", toolName, output)
	}

	// 字符串载荷先统一解码，后续各工具只处理结构化形态
	if s, ok := output.(string); ok && s != "" {
		var decoded any
		if err := json.Unmarshal([]byte(s), &decoded); err == nil {
			if decoded == nil {
				// JSON null：保持原 web_fetch / multi-page 以 nil map 调用的行为
				decoded = map[string]any(nil)
			}
			output = decoded
		}
	}

	appendResults := func(items any) {
		arr, _ := items.([]any)
		for _, item := range arr {
			if resultMap, ok := item.(map[string]any); ok {
				if citation, err := extractCitationFromSearchResult(resultMap, agentID, now); err == nil {
					citations = append(citations, *citation)
				}
			}
		}
	}
	appendPage := func(pageMap map[string]any) {
		if citation, err := extractCitationFromFetchResult(pageMap, agentID, now); err == nil {
			citations = append(citations, *citation)
		}
	}

	switch toolName {
	case "web_search":
		if m, ok := output.(map[string]any); ok {
			output = m["results"]
		}
		appendResults(output)

	case "web_fetch":
		// Handle both single page and batch fetch formats
		// 同时处理单页与批量抓取两种格式
		if m, ok := output.(map[string]any); ok {
			// Check for batch format: {pages: [...], succeeded: N, failed: M}
			// 检查批量格式：{pages: [...], succeeded: N, failed: M}
			if pages, ok := m["pages"].([]any); ok {
				for _, page := range pages {
					if pageMap, ok := page.(map[string]any); ok {
						// Only extract from successful pages
						if success, _ := pageMap["success"].(bool); success {
							appendPage(pageMap)
						}
					}
				}
			} else {
				// Single page format (backward compatible)
				appendPage(m)
			}
		}

	case "web_subpage_fetch", "web_crawl":
		// Multi-page tools: try to extract citations from metadata.urls
		// 多页 tool：尝试从 metadata.urls 中提取引用
		if m, ok := output.(map[string]any); ok {
			if multi := extractCitationsFromMultiPageResult(m, agentID, now); len(multi) > 0 {
				citations = append(citations, multi...)
			} else {
				// Fallback: treat as single page fetch
				appendPage(m)
			}
		}
	}

	if isCitationsDebugEnabled() {
		log.Printf("[citations] tool=%s extracted=%d", toolName, len(citations))
	}
	return citations
}

// CollectCitations extracts, deduplicates, scores, and ranks citations from agent execution results
// Returns top N citations with aggregate statistics
// CollectCitations 从 agent 执行结果中提取、去重、评分并排序引用
// 返回前 N 条引用及汇总统计
func CollectCitations(results []any, now time.Time, maxCitations int) ([]Citation, CitationStats) {
	if maxCitations <= 0 {
		maxCitations = 200 // default
	}

	var allCitations []Citation

	// Step 1: Extract from tool executions
	for _, resultInterface := range results {
		// Try to cast to map[string]interface{} first (common case)
		// 先尝试断言为 map[string]interface{}（常见情况）
		resultMap, ok := resultInterface.(map[string]any)
		if !ok {
			continue
		}

		agentID, _ := resultMap["agent_id"].(string)
		if agentID == "" {
			agentID = "unknown"
		}

		// Extract tool_executions array if present; allow nil/missing
		// 存在时提取 tool_executions 数组；允许为 nil/缺失
		var toolExecutions []any
		if tev, ok := resultMap["tool_executions"]; ok && tev != nil {
			if arr, ok := tev.([]any); ok {
				toolExecutions = arr
			}
		}

		for _, toolExecInterface := range toolExecutions {
			toolExecMap, ok := toolExecInterface.(map[string]any)
			if !ok {
				continue
			}

			toolName, _ := toolExecMap["tool"].(string)
			success, _ := toolExecMap["success"].(bool)
			output := toolExecMap["output"]

			// Only process successful executions of web_search or web_fetch tools
			// 只处理成功的 web_search 或 web_fetch tool 执行
			if !success || (toolName != "web_search" && toolName != "web_fetch" && toolName != "web_subpage_fetch" && toolName != "web_crawl") {
				continue
			}

			citations := extractCitationsFromToolOutput(toolName, output, agentID, now)
			allCitations = append(allCitations, citations...)
		}

		// Also attempt extraction from the agent's response text (helps when tool payloads are truncated or missing)
		// 同时尝试从 agent response 文本中提取（tool 载荷被截断或缺失时有用）
		if responseStr, ok := resultMap["response"].(string); ok && strings.TrimSpace(responseStr) != "" {
			// 1) Try structured JSON array fallback
			fallbackCitations := extractCitationsFromResponse(responseStr, agentID, now)
			if len(fallbackCitations) > 0 {
				allCitations = append(allCitations, fallbackCitations...)
			} else {
				// 2) Plain-text URL scan fallback
				plain := extractCitationsFromPlainTextResponse(responseStr, agentID, now)
				allCitations = append(allCitations, plain...)
			}
		}
	}

	if isCitationsDebugEnabled() {
		log.Printf("[citations] raw_extracted=%d", len(allCitations))
	}

	// Sanitize citation titles/metadata (e.g., fix arXiv reCAPTCHA titles)
	// 清洗引用标题/metadata（如修正 arXiv 的 reCAPTCHA 标题）
	allCitations = sanitizeCitations(allCitations)

	// Step 2: Deduplicate by normalized URL
	dedupedCitations := deduplicateCitations(allCitations)

	// Step 3: Enforce diversity (max N per domain)
	// 步骤 3：限制多样性（每个域名最多 N 条）
	config := LoadCredibilityConfig()
	maxPerDomain := 3
	if config.DiversityRules.MaxPerDomain > 0 {
		maxPerDomain = config.DiversityRules.MaxPerDomain
	}
	diverseCitations := enforceDiversity(dedupedCitations, maxPerDomain)

	// Step 3.5: Filter out citations with fallback-only snippets (title + URL, no real content)
	// 步骤 3.5：过滤仅含兜底 snippet（title + URL，无真实内容）的引用
	filteredCitations := filterFallbackSnippets(diverseCitations)
	if isCitationsDebugEnabled() && len(diverseCitations) != len(filteredCitations) {
		log.Printf("[citations] fallback_filtered=%d remaining=%d",
			len(diverseCitations)-len(filteredCitations), len(filteredCitations))
	}

	// Step 4: Rank by combined score and limit to top N
	// 步骤 4：按综合分排序并截取前 N 条
	rankedCitations := rankAndLimit(filteredCitations, maxCitations)

	// Step 5: Calculate aggregate stats
	stats := calculateCitationStats(rankedCitations)

	if isCitationsDebugEnabled() {
		log.Printf("[citations] final_total=%d unique_domains=%d avg_quality=%.2f avg_cred=%.2f",
			len(rankedCitations), stats.UniqueDomains, stats.AvgQuality, stats.AvgCredibility)

		// P1-4: Extended metrics logging
		log.Printf("[citations] quality_buckets: low=%d medium=%d high=%d",
			stats.QualityBuckets["low"], stats.QualityBuckets["medium"], stats.QualityBuckets["high"])

		if len(stats.TopDomains) > 0 {
			var topDomainsStr strings.Builder
			for i, d := range stats.TopDomains {
				if i > 0 {
					topDomainsStr.WriteString(", ")
				}
				fmt.Fprintf(&topDomainsStr, "%s(%d)", d.Domain, d.Count)
				if i >= 4 { // Show top 5 in log
					break
				}
			}
			log.Printf("[citations] top_domains: %s", topDomainsStr.String())
		}

		if len(stats.PerAgentCount) > 0 {
			log.Printf("[citations] per_agent_count: %v", stats.PerAgentCount)
		}

		if stats.DuplicateURLs > 0 {
			log.Printf("[citations] duplicate_urls_removed=%d", stats.DuplicateURLs)
		}
	}

	return rankedCitations, stats
}

// deduplicateCitations removes duplicate citations by normalized URL (keeping first occurrence)
// deduplicateCitations 按规范化 URL 去除重复引用（保留首次出现）
func deduplicateCitations(citations []Citation) []Citation {
	// Keep one entry per canonical key; merge to preserve best scores/metadata
	// 每个规范键只保留一条；合并以保留最佳评分/metadata
	index := make(map[string]int)
	var deduped []Citation

	for _, citation := range citations {
		key := citation.URL
		// Prefer DOI-based key for cross-domain canonicalization
		// 优先使用基于 DOI 的键以实现跨域规范化
		if parsed, err := url.Parse(citation.URL); err == nil {
			if doi := extractDOIFromURL(parsed); doi != "" {
				key = "doi:" + strings.ToLower(doi)
			} else if norm, err2 := NormalizeURL(citation.URL); err2 == nil && norm != "" {
				key = norm
			}
		} else if norm, err2 := NormalizeURL(citation.URL); err2 == nil && norm != "" {
			key = norm
		}

		if idx, ok := index[key]; ok {
			// Merge: keep higher quality/credibility; fill missing metadata when available
			// 合并：保留更高的质量/可信度；可用时补全缺失的 metadata
			if citation.QualityScore > deduped[idx].QualityScore {
				deduped[idx].QualityScore = citation.QualityScore
			}
			if citation.CredibilityScore > deduped[idx].CredibilityScore {
				deduped[idx].CredibilityScore = citation.CredibilityScore
			}
			if deduped[idx].Title == "" && citation.Title != "" {
				deduped[idx].Title = citation.Title
			}
			if deduped[idx].Snippet == "" && citation.Snippet != "" {
				deduped[idx].Snippet = citation.Snippet
			}
			if deduped[idx].PublishedDate == nil && citation.PublishedDate != nil {
				deduped[idx].PublishedDate = citation.PublishedDate
			}
			// Relevance: prefer higher
			if citation.RelevanceScore > deduped[idx].RelevanceScore {
				deduped[idx].RelevanceScore = citation.RelevanceScore
			}
			// Citation V2: Prefer "fetch" over "search" for ToolSource
			// Fetch provides full content for verification, search only provides snippets
			// Citation V2：ToolSource 优先取 "fetch" 而非 "search"
			// fetch 提供完整内容可用于校验，search 只有片段
			if citation.ToolSource == "fetch" && deduped[idx].ToolSource != "fetch" {
				deduped[idx].ToolSource = citation.ToolSource
				deduped[idx].StatusCode = citation.StatusCode
				deduped[idx].BlockedReason = citation.BlockedReason
				deduped[idx].Content = citation.Content
			}
		} else {
			index[key] = len(deduped)
			deduped = append(deduped, citation)
		}
	}

	return deduped
}

// sanitizeCitations performs light cleanup on citation metadata
// sanitizeCitations 对引用 metadata 做轻度清洗
func sanitizeCitations(citations []Citation) []Citation {
	for i := range citations {
		citations[i].Title = sanitizeTitle(citations[i].Title, citations[i].URL, citations[i].Source)
	}
	return citations
}

func sanitizeTitle(title, rawURL, source string) string {
	t := strings.TrimSpace(title)
	lower := strings.ToLower(t)
	if strings.EqualFold(source, "arxiv.org") {
		if t == "" || strings.Contains(lower, "recaptcha") {
			if id := extractArxivID(rawURL); id != "" {
				return "arXiv:" + id
			}
			return "arXiv"
		}
	}
	return t
}

// extractArxivID returns the arXiv identifier from common arxiv.org URLs
// Examples: https://arxiv.org/abs/2408.13687 -> 2408.13687
//
//	https://arxiv.org/pdf/2408.13687.pdf -> 2408.13687
//
// extractArxivID 从常见的 arxiv.org URL 中返回 arXiv 标识
// 例如：https://arxiv.org/abs/2408.13687 -> 2408.13687
// https://arxiv.org/pdf/2408.13687.pdf -> 2408.13687
func extractArxivID(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	if !strings.Contains(strings.ToLower(u.Host), "arxiv.org") {
		return ""
	}
	p := strings.Trim(u.Path, "/")
	parts := strings.Split(p, "/")
	if len(parts) == 0 {
		return ""
	}
	last := parts[len(parts)-1]
	// Strip .pdf if present
	last = strings.TrimSuffix(last, ".pdf")
	// abs/<id> or pdf/<id>
	if len(parts) >= 2 {
		return last
	}
	return ""
}

// extractDOIFromURL attempts to extract a DOI from the URL or query
// Recognizes: host contains doi.org, query param "doi", or DOI pattern in path
// extractDOIFromURL 尝试从 URL 或查询参数中提取 DOI
// 识别：主机含 doi.org、查询参数 "doi"，或路径中的 DOI 模式
func extractDOIFromURL(u *url.URL) string {
	// doi.org host
	if strings.Contains(strings.ToLower(u.Host), "doi.org") {
		doi := strings.Trim(u.Path, "/")
		if doi != "" {
			return doi
		}
	}
	// doi param in query (e.g., crossmark)
	// 查询参数中的 doi（如 crossmark）
	if q := u.Query().Get("doi"); q != "" {
		return q
	}
	// DOI pattern in path (case-insensitive)
	// 10.XXXX/...
	// 路径中的 DOI 模式（大小写不敏感）
	// 10.XXXX/...
	re := regexp.MustCompile(`(?i)10\.[0-9]{4,9}/[-._;()/:A-Z0-9]+`)
	if m := re.FindString(u.Path); m != "" {
		return m
	}
	return ""
}

// truncateRunes returns s truncated to at most max runes, appending "..." when truncated.
// truncateRunes 返回最多 max 个 rune 的 s，截断时追加 "..."。
func truncateRunes(s string, max int) string {
	if max <= 0 || s == "" {
		return ""
	}
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "..."
}

// isFallbackSnippet detects snippets generated by ensureSnippet()'s fallback path:
// fmt.Sprintf("%s - %s", title, urlStr)
// These contain no real content — just title and URL — so the citation agent LLM
// cannot judge relevance and may incorrectly place them.
// isFallbackSnippet 识别由 ensureSnippet() 兜底路径生成的 snippet：
// fmt.Sprintf("%s - %s", title, urlStr)
// 这类内容没有真实信息——只有标题和 URL——引用判定用的 LLM
// 无法据此判断相关性，可能错误地放置引用。
func isFallbackSnippet(snippet, title, url string) bool {
	if snippet == "" {
		return true
	}
	// Match the exact format produced by ensureSnippet() fallback
	// 匹配 ensureSnippet() 兜底生成的精确格式
	if title != "" && snippet == fmt.Sprintf("%s - %s", title, url) {
		return true
	}
	return false
}

// filterFallbackSnippets removes citations whose snippets are fallback-only
// (generated by ensureSnippet when no real content was available).
// filterFallbackSnippets 移除仅含兜底 snippet 的引用
// （即 ensureSnippet 在无真实内容时生成的）。
func filterFallbackSnippets(citations []Citation) []Citation {
	var filtered []Citation
	for _, c := range citations {
		if isFallbackSnippet(c.Snippet, c.Title, c.URL) {
			continue
		}
		filtered = append(filtered, c)
	}
	return filtered
}

// enforceDiversity limits citations per domain to maintain source diversity
// enforceDiversity 限制每个域名的引用数，以保持来源多样性
func enforceDiversity(citations []Citation, maxPerDomain int) []Citation {
	domainCount := make(map[string]int)
	var diverse []Citation

	// Sort by quality * credibility first to keep best sources per domain
	// 先按 quality * credibility 排序，以保留每个域名下的最佳来源
	sorted := make([]Citation, len(citations))
	copy(sorted, citations)
	sortByScore(sorted)

	for _, citation := range sorted {
		if domainCount[citation.Source] < maxPerDomain {
			domainCount[citation.Source]++
			diverse = append(diverse, citation)
		}
	}

	return diverse
}

// rankAndLimit sorts citations by combined score and returns top N
// rankAndLimit 按综合分排序并返回前 N 条
func rankAndLimit(citations []Citation, limit int) []Citation {
	if len(citations) == 0 {
		return citations
	}

	// Sort by combined score (quality * credibility)
	sorted := make([]Citation, len(citations))
	copy(sorted, citations)
	sortByScore(sorted)

	// Limit to top N
	if len(sorted) > limit {
		sorted = sorted[:limit]
	}

	return sorted
}

// sortByScore sorts citations by combined score (quality * credibility) descending
// sortByScore 按综合分（quality * credibility）降序排序
func sortByScore(citations []Citation) {
	sort.Slice(citations, func(i, j int) bool {
		scoreI := citations[i].QualityScore * citations[i].CredibilityScore
		scoreJ := citations[j].QualityScore * citations[j].CredibilityScore
		return scoreI > scoreJ // descending order
	})
}

// calculateCitationStats computes aggregate statistics
func calculateCitationStats(citations []Citation) CitationStats {
	if len(citations) == 0 {
		return CitationStats{}
	}

	uniqueDomains := make(map[string]bool)
	domainCounts := make(map[string]int)
	perAgentCount := make(map[string]int)
	qualityBuckets := map[string]int{"low": 0, "medium": 0, "high": 0}
	urlSet := make(map[string]bool)
	duplicateURLs := 0
	totalQuality := 0.0
	totalCredibility := 0.0

	for _, c := range citations {
		uniqueDomains[c.Source] = true
		domainCounts[c.Source]++
		totalQuality += c.QualityScore
		totalCredibility += c.CredibilityScore

		// P1-4: Quality buckets
		switch {
		case c.QualityScore < 0.3:
			qualityBuckets["low"]++
		case c.QualityScore < 0.6:
			qualityBuckets["medium"]++
		default:
			qualityBuckets["high"]++
		}

		// P1-4: Duplicate URL tracking
		if urlSet[c.URL] {
			duplicateURLs++
		}
		urlSet[c.URL] = true

		// P1-4: Per-agent count
		if c.AgentID != "" {
			perAgentCount[c.AgentID]++
		}
	}

	// P1-4: Top domains (sorted by count, top 10)
	// P1-4：高频域名（按数量排序，取前 10）
	topDomains := getTopDomains(domainCounts, 10)

	return CitationStats{
		TotalSources:    len(citations),
		UniqueDomains:   len(uniqueDomains),
		AvgQuality:      totalQuality / float64(len(citations)),
		AvgCredibility:  totalCredibility / float64(len(citations)),
		SourceDiversity: CalculateSourceDiversity(citations),
		QualityBuckets:  qualityBuckets,
		TopDomains:      topDomains,
		DuplicateURLs:   duplicateURLs,
		PerAgentCount:   perAgentCount,
	}
}

// getTopDomains returns the top N domains by citation count
// getTopDomains 按引用数返回前 N 个域名
func getTopDomains(domainCounts map[string]int, n int) []DomainCount {
	// Convert to slice for sorting
	domains := make([]DomainCount, 0, len(domainCounts))
	for domain, count := range domainCounts {
		domains = append(domains, DomainCount{Domain: domain, Count: count})
	}

	// Sort by count descending
	sort.Slice(domains, func(i, j int) bool {
		return domains[i].Count > domains[j].Count
	})

	// Take top N
	if len(domains) > n {
		domains = domains[:n]
	}
	return domains
}

// pageInfo holds title and snippet extracted from merged multi-page content
// pageInfo 保存从多页合并内容中提取的标题与 snippet
type pageInfo struct {
	Title   string
	Snippet string
}

// extractCitationsFromMultiPageResult extracts citations from web_subpage_fetch or web_crawl results
// that contain multiple pages merged into a single content with metadata.urls listing all page URLs
// extractCitationsFromMultiPageResult 从 web_subpage_fetch 或 web_crawl 结果中提取引用，
// 这些结果把多页合并为单个 content，并在 metadata.urls 中列出全部页面 URL
func extractCitationsFromMultiPageResult(output map[string]any, agentID string, now time.Time) []Citation {
	var citations []Citation

	// 1. Get metadata.urls
	metadata, ok := output["metadata"].(map[string]any)
	if !ok {
		return nil
	}
	urlsRaw, ok := metadata["urls"].([]any)
	if !ok || len(urlsRaw) == 0 {
		return nil
	}

	// 2. Get merged content
	content := ""
	if c, ok := output["content"].(string); ok {
		content = c
	}

	// 3. Parse content to build url -> {title, snippet} mapping
	// Note: content may be truncated (~2000 chars), so not all pages will have snippet info
	// 3. 解析内容构建 url -> {title, snippet} 映射
	// 注意：内容可能被截断（约 2000 字符），因此并非所有页面都有 snippet 信息
	pageInfoMap := parseMultiPageContent(content)

	if isCitationsDebugEnabled() {
		log.Printf("[citations] multi-page: urls=%d, pageInfoMap=%d", len(urlsRaw), len(pageInfoMap))
	}

	// 4. Create citation for each URL
	for _, urlVal := range urlsRaw {
		urlStr, ok := urlVal.(string)
		if !ok || urlStr == "" {
			continue
		}

		// Filter URLs that should not become sources
		// 过滤不应成为来源的 URL
		if shouldSkipURL(urlStr) {
			continue
		}

		normalizedURL, err := NormalizeURL(urlStr)
		if err != nil {
			continue
		}

		domain, err := ExtractDomain(urlStr)
		if err != nil {
			domain = ""
		}

		title := ""
		snippet := ""

		// Look up page info (content may be truncated, so some pages won't have info)
		// 查找页面信息（内容可能被截断，部分页面没有信息）
		if info, found := pageInfoMap[normalizedURL]; found {
			title = info.Title
			snippet = info.Snippet
		} else if info, found := pageInfoMap[urlStr]; found {
			title = info.Title
			snippet = info.Snippet
		}

		// Ensure snippet is not empty - use title + url as fallback
		// 确保 snippet 非空——以 title + url 作为兜底
		snippet = ensureSnippet(snippet, "", title, normalizedURL, MinSnippetLength)

		// Build complete Citation with proper scoring
		// Citation V2: extract tool_source from output
		// 构建完整的 Citation 并正确评分
		// Citation V2：从输出中提取 tool_source
		toolSource := "fetch" // default for multi-page fetch
		if ts, ok := output["tool_source"].(string); ok && ts != "" {
			toolSource = ts
		}

		citation := Citation{
			URL:            normalizedURL,
			Title:          title,
			Source:         domain,
			SourceType:     "web",
			ToolSource:     toolSource,
			RetrievedAt:    now,
			RelevanceScore: 0.8, // fetch tools get fixed 0.8
			AgentID:        agentID,
			Snippet:        snippet,
		}

		// Calculate quality and credibility scores using existing functions
		// 用现有函数计算质量分与可信度分
		citation.QualityScore = ScoreQuality(citation.RelevanceScore, citation.PublishedDate, title != "", snippet != "", now)
		citation.CredibilityScore = ScoreCredibility(domain)

		citations = append(citations, citation)
	}

	return citations
}

// parseMultiPageContent parses merged markdown content to extract title and snippet for each page
// Format patterns:
//   - web_subpage_fetch: "# Main Page: {url}" and "## Subpage N: {url}"
//   - web_crawl:         "# Main Page: {url}" and "## Page N: {url}"
//
// parseMultiPageContent 解析合并后的 markdown 内容，提取每页的标题与 snippet
// 格式模式：
// - web_subpage_fetch："# Main Page: {url}" 与 "## Subpage N: {url}"
// - web_crawl：        "# Main Page: {url}" 与 "## Page N: {url}"
func parseMultiPageContent(content string) map[string]pageInfo {
	result := make(map[string]pageInfo)

	if content == "" {
		return result
	}

	// Match page headers: # Main Page: URL or ## Subpage N: URL or ## Page N: URL
	// 匹配页面标题行：# Main Page: URL 或 ## Subpage N: URL 或 ## Page N: URL
	headerPattern := regexp.MustCompile(`(?m)^#{1,2}\s+(?:Main Page|Subpage \d+|Page \d+):\s*(.+)$`)
	matches := headerPattern.FindAllStringSubmatchIndex(content, -1)

	for i, match := range matches {
		if len(match) < 4 {
			continue
		}

		// Extract URL from the header
		urlStart, urlEnd := match[2], match[3]
		pageURL := strings.TrimSpace(content[urlStart:urlEnd])

		// Determine content range for this page
		contentStart := match[1] // end of header line
		contentEnd := len(content)
		if i+1 < len(matches) {
			contentEnd = matches[i+1][0] // start of next header
		}

		pageContent := content[contentStart:contentEnd]

		// Extract title (may be on next line as **title**)
		// 提取标题（可能在下一行以 **title** 形式出现）
		title := extractTitleFromPageContent(pageContent)

		// Extract snippet (first 500 chars of actual content)
		// 提取 snippet（正文内容的前 500 字符）
		snippet := extractSnippetFromPageContent(pageContent, MaxSnippetLength)

		result[pageURL] = pageInfo{Title: title, Snippet: snippet}

		// Also store with normalized URL as key
		// 同时以规范化 URL 作为键再存一份
		if normalized, err := NormalizeURL(pageURL); err == nil && normalized != pageURL {
			result[normalized] = pageInfo{Title: title, Snippet: snippet}
		}
	}

	return result
}

// extractTitleFromPageContent extracts title from page content
// Looks for **title** pattern on the first non-empty line after the header
// extractTitleFromPageContent 从页面内容中提取标题
// 查找标题行之后首个非空行中的 **title** 模式
func extractTitleFromPageContent(content string) string {
	lines := strings.SplitSeq(content, "\n")
	for line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Look for **title** pattern
		if strings.HasPrefix(line, "**") && strings.HasSuffix(line, "**") {
			return strings.Trim(line, "*")
		}
		// If first non-empty line doesn't match pattern, stop looking
		// 首个非空行不匹配模式时停止查找
		break
	}
	return ""
}

// extractSnippetFromPageContent extracts first N characters of meaningful content
// extractSnippetFromPageContent 提取有意义内容的前 N 个字符
func extractSnippetFromPageContent(content string, maxLen int) string {
	lines := strings.Split(content, "\n")
	var sb strings.Builder
	skipFirstLines := true

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		// Skip title line if present
		if skipFirstLines && strings.HasPrefix(line, "**") && strings.HasSuffix(line, "**") {
			skipFirstLines = false
			continue
		}
		skipFirstLines = false

		// Skip markdown separators
		if line == "---" || strings.HasPrefix(line, "# ") || strings.HasPrefix(line, "## ") {
			continue
		}

		if sb.Len() > 0 {
			sb.WriteString(" ")
		}
		sb.WriteString(line)

		if sb.Len() >= maxLen {
			break
		}
	}

	result := sb.String()
	if len(result) > maxLen {
		result = result[:maxLen]
	}
	return result
}

// shouldSkipURL returns true if the URL should be skipped (not become a source)
// Filters out sitemap, robots.txt, static assets, etc.
// URL 应被跳过（不作为来源）时 shouldSkipURL 返回 true
// 过滤 sitemap、robots.txt、静态资源等
func shouldSkipURL(urlStr string) bool {
	lower := strings.ToLower(urlStr)

	// Skip sitemaps, robots, and common non-content URLs
	// 跳过 sitemap、robots 及常见的非内容 URL
	skipPatterns := []string{
		".xml",
		"sitemap",
		"robots.txt",
		".css",
		".js",
		".ico",
		".png",
		".jpg",
		".jpeg",
		".gif",
		".svg",
		".woff",
		".woff2",
		".ttf",
		".eot",
		"favicon",
		"/feed",
		"/rss",
		"/atom",
	}

	for _, pattern := range skipPatterns {
		if strings.HasSuffix(lower, pattern) || strings.Contains(lower, pattern+"/") || strings.Contains(lower, pattern+"?") {
			return true
		}
	}

	// Skip low-value page types (authentication, search, error, legal, forms)
	// 跳过低价值页面类型（authentication、搜索、错误、法务、表单）
	lowValuePatterns := []string{
		// Authentication pages
		"/login", "/signin", "/sign-in", "/auth",
		"/register", "/signup", "/sign-up",
		"/logout", "/signout", "/sign-out",
		// Search/results pages (usually not useful as citations)
		// 搜索/结果页（通常不适合作为引用）
		"/search", "/results",
		// Error pages
		"/404", "/error", "not-found", "/500",
		// Legal/policy pages (boilerplate, not useful for research)
		// 法务/政策页（样板文本，对研究无用）
		"/terms", "/tos", "/terms-of-service",
		"/privacy", "/privacy-policy",
		"/legal", "/legal-notice",
		"/cookie", "/cookies",
		// Contact/form pages (mostly forms, not content)
		// 联系/表单页（多为表单而非内容）
		"/contact", "/contact-us", "/get-in-touch",
		"/support", "/help",
	}

	for _, pattern := range lowValuePatterns {
		if strings.Contains(lower, pattern) {
			return true
		}
	}

	// Skip URLs with search query parameters
	if strings.Contains(lower, "?q=") || strings.Contains(lower, "?search=") || strings.Contains(lower, "?query=") {
		return true
	}

	return false
}

// ============================================================================
// Citation V2: Filter functions for Deep Research workflow
// ============================================================================
// Citation V2：Deep Research workflow 使用的过滤函数

// FilterValidCitations returns only citations that pass IsValid() check.
// Used by Citation V2 to remove blocked/empty/errored citations before VerifyBatch.
// Returns (valid citations, invalid count, blocked URLs for metadata).
// FilterValidCitations 只返回通过 IsValid() 检查的引用。
// Citation V2 在 VerifyBatch 之前用它移除被拦截/为空/出错的引用。
// 返回（有效引用、无效数量、用于 metadata 的被拦截 URL）。
func FilterValidCitations(citations []Citation) ([]Citation, int, []string) {
	var valid []Citation
	var blockedURLs []string
	invalidCount := 0

	for _, c := range citations {
		if c.IsValid() {
			valid = append(valid, c)
		} else {
			invalidCount++
			if c.BlockedReason != "" {
				blockedURLs = append(blockedURLs, c.URL)
			}
		}
	}

	if isCitationsDebugEnabled() {
		log.Printf("[citations] FilterValidCitations: input=%d valid=%d invalid=%d blocked_urls=%d",
			len(citations), len(valid), invalidCount, len(blockedURLs))
	}

	return valid, invalidCount, blockedURLs
}

// CitationWithID wraps Citation with a sequential ID for verification tracking
// CitationWithID 为 Citation 包装顺序 ID，便于校验时跟踪
type CitationWithID struct {
	ID       int      `json:"id"`       // Sequential ID (1, 2, 3...)
	Citation Citation `json:"citation"` // Original citation data
}

// assignSequentialIDs wraps citations with 1-indexed sequential IDs, preserving order.
// assignSequentialIDs 按顺序为引用分配从 1 开始的 ID
func assignSequentialIDs(citations []Citation) []CitationWithID {
	result := make([]CitationWithID, len(citations))
	for i, c := range citations {
		result[i] = CitationWithID{
			ID:       i + 1, // 1-indexed
			Citation: c,
		}
	}
	return result
}

// FilterFetchOnlyAndAssignIDs filters citations to fetch-only sources and assigns sequential IDs.
// This is used by Deep Research workflow to:
// 1. Only use citations from actual page content (web_fetch/web_subpage_fetch/web_crawl)
// 2. Exclude search snippets which are often incomplete/unreliable for verification
// 3. Assign stable IDs for claim-citation mapping in verification
//
// Returns citations sorted by quality*credibility descending, with IDs 1, 2, 3...
// FilterFetchOnlyAndAssignIDs 将引用过滤为仅 fetch 来源并分配顺序 ID。
// Deep Research workflow 用它来：
// 1. 只使用来自实际页面内容的引用（web_fetch/web_subpage_fetch/web_crawl）
// 2. 排除往往不完整/不可靠、难以校验的搜索片段
// 3. 为校验中的 claim-citation 映射分配稳定 ID
// 返回按 quality*credibility 降序排序、ID 从 1 开始递增的引用
func FilterFetchOnlyAndAssignIDs(citations []Citation) []CitationWithID {
	// Filter to fetch-only citations
	var fetchCitations []Citation
	for _, c := range citations {
		if c.ToolSource == "fetch" {
			fetchCitations = append(fetchCitations, c)
		}
	}

	// Sort by combined score (quality * credibility) descending
	// 按综合分（quality * credibility）降序排序
	sortByScore(fetchCitations)

	// Assign sequential IDs (1-indexed)
	result := assignSequentialIDs(fetchCitations)

	if isCitationsDebugEnabled() {
		log.Printf("[citations] FilterFetchOnlyAndAssignIDs: input=%d fetch_only=%d", len(citations), len(result))
	}

	return result
}

// AssignIDsToAllCitations assigns sequential IDs to all citations (not filtered).
// Used by P1 to output all citations in Sources section.
// Sorts by combined score (quality * credibility) descending, then assigns 1-indexed IDs.
// AssignIDsToAllCitations 为所有引用（不过滤）分配顺序 ID。
// P1 用它把全部引用输出到 Sources 段。
// 按综合分（quality * credibility）降序排序，然后分配从 1 开始的 ID。
func AssignIDsToAllCitations(citations []Citation) []CitationWithID {
	if len(citations) == 0 {
		return nil
	}

	// Make a copy to avoid modifying the original slice
	// 复制一份，避免修改原切片
	sortedCitations := make([]Citation, len(citations))
	copy(sortedCitations, citations)

	// Sort by combined score (quality * credibility) descending
	// 按综合分（quality * credibility）降序排序
	sortByScore(sortedCitations)

	// Assign sequential IDs (1-indexed)
	result := assignSequentialIDs(sortedCitations)

	if isCitationsDebugEnabled() {
		log.Printf("[citations] AssignIDsToAllCitations: input=%d output=%d", len(citations), len(result))
	}

	return result
}

// FilterByIDs returns only citations matching the given ID set.
// Used after verification to filter to only the citations that support claims.
// Preserves original order and IDs.
// FilterByIDs 只返回匹配给定 ID 集合的引用。
// 校验之后用它筛出支撑 claim 的引用。
// 保持原有顺序与 ID。
func FilterByIDs(citations []CitationWithID, ids []int) []CitationWithID {
	if len(ids) == 0 {
		return nil
	}

	// Build ID lookup set
	idSet := make(map[int]bool, len(ids))
	for _, id := range ids {
		idSet[id] = true
	}

	// Filter to matching IDs
	var result []CitationWithID
	for _, c := range citations {
		if idSet[c.ID] {
			result = append(result, c)
		}
	}

	if isCitationsDebugEnabled() {
		log.Printf("[citations] FilterByIDs: input=%d ids=%d matched=%d", len(citations), len(ids), len(result))
	}

	return result
}

// GetAllCitationIDs extracts all IDs from a slice of CitationWithID
// GetAllCitationIDs 从 CitationWithID 切片中提取全部 ID
func GetAllCitationIDs(citations []CitationWithID) []int {
	ids := make([]int, len(citations))
	for i, c := range citations {
		ids[i] = c.ID
	}
	return ids
}

// CitationWithIDToCitation extracts Citation slice from CitationWithID slice
// CitationWithIDToCitation 从 CitationWithID 切片中提取 Citation 切片
func CitationWithIDToCitation(citations []CitationWithID) []Citation {
	result := make([]Citation, len(citations))
	for i, c := range citations {
		result[i] = c.Citation
	}
	return result
}
