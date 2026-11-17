package strategies

import (
	"encoding/json"
	"fmt"
	"maps"
	"net"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/agents"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/budget"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/formatting"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metadata"
	pricing "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/control"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/opts"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/patterns"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/patterns/execution"
)

// FilterCitationsByEntity filters citations based on entity relevance when canonical name is detected.
// FilterCitationsByEntity 在检测到规范名称（canonical name）时，按实体相关性过滤引用。
//
// Scoring System (OR logic, not AND):
// 打分体系（OR 逻辑，不是 AND）：
//   - Official domain match: +0.6 points (e.g., example.com, jp.example.com)
//   - 官方域名匹配：+0.6 分（例如 example.com、jp.example.com）
//   - Alias in URL: +0.4 points (broader domain matching)
//   - URL 中含别名：+0.4 分（更宽松的域名匹配）
//   - Title/snippet/source contains alias: +0.4 points
//   - 标题/摘要/来源含别名：+0.4 分
//   - Threshold: 0.3 (passes with any single match)
//   - 阈值：0.3（任意单项匹配即可通过）
//
// Filtering Strategy:
// 过滤策略：
//  1. Always keep ALL official domain citations (bypass threshold)
//  1. 官方域名引用始终全部保留（绕过阈值）
//  2. Keep non-official citations scoring >= threshold
//  2. 非官方引用得分 >= 阈值者保留
//  3. Backfill to minKeep (10) using quality×credibility+entity_score
//  3. 用 quality×credibility+entity_score 回填至 minKeep（10）
//
// Prevents Over-Filtering:
// 避免过度过滤：
//   - Lower threshold (0.3) allows title/snippet matches (0.4) to pass
//   - 较低的阈值（0.3）可放行标题/摘要匹配（0.4）
//   - Higher minKeep (10) for deep research coverage
//   - 较高的 minKeep（10）以覆盖深度研究
//   - Official sites guaranteed inclusion
//   - 官方站点保证入选
//
// Search Engine Variance:
// 搜索引擎波动：
//   - If search doesn't return official sites, filter won't create them
//   - 搜索未返回官方站点时，过滤也无法凭空造出
//   - If official sites in results, they're always preserved
//   - 结果中若有官方站点，则始终保留
//
// Future Improvements (see long-term plan):
// 后续改进（见长期计划）：
//   - Phase 2: Soft rerank (multiply scores vs hard filter)
//   - 阶段 2：软重排（乘分而非硬过滤）
//   - Phase 3: Verification entity coherence
//   - 阶段 3：校验实体一致性
//   - Phase 4: Adaptive retry if coverage < target
//   - 阶段 4：覆盖不足（< 目标）时自适应 retry
// 当检测到规范名称（canonical name）时，按实体相关性过滤引用。
// 打分采用 OR 逻辑：官方域名匹配 +0.6；URL 含别名 +0.4；标题/摘要/来源含别名 +0.4；
// 阈值 0.3，因此任意单项匹配即可通过。
// 过滤策略：官方域名引用全部无条件保留；非官方引用保留得分 >= 阈值者；
// 最后按「质量×可信度+实体分」回填至 minKeep（10 条）。
// 这样可避免过度过滤：阈值偏低以放行标题/摘要匹配，minKeep 偏高以覆盖深度研究，
// 官方站点必定入选。搜索结果本身的波动无法被过滤补偿：搜索没返回官网，过滤也造不出来。
// 后续改进（见长期计划）：阶段 2 软重排（乘分而非硬过滤）、阶段 3 实体一致性校验、
// 阶段 4 覆盖不足时自适应 retry。

// containsAsWord checks if text contains term as a whole word (word boundary matching).
// containsAsWord 检查 text 中是否以整词形式包含 term（词边界匹配）。
// This prevents "mind" from matching "Minders.io" or "reminded".
// 整词匹配（检查词边界），避免 "mind" 误命中 "Minders.io" 或 "reminded"。
func containsAsWord(text, term string) bool {
	if term == "" {
		return false
	}
	idx := strings.Index(text, term)
	if idx < 0 {
		return false
	}
	// Check left boundary
	// 检查左边界
	if idx > 0 {
		prev := text[idx-1]
		if (prev >= 'a' && prev <= 'z') || (prev >= '0' && prev <= '9') {
			// Previous char is alphanumeric, not a word boundary
			// 前一个字符是字母数字，不是词边界
			// Try to find next occurrence
			// 尝试查找下一次出现
			rest := text[idx+len(term):]
			return containsAsWord(rest, term)
		}
	}
	// Check right boundary
	// 检查右边界
	endIdx := idx + len(term)
	if endIdx < len(text) {
		next := text[endIdx]
		if (next >= 'a' && next <= 'z') || (next >= '0' && next <= '9') {
			// Next char is alphanumeric, not a word boundary
			// 下一个字符是字母数字，不是词边界
			rest := text[idx+len(term):]
			return containsAsWord(rest, term)
		}
	}
	return true
}

func FilterCitationsByEntity(citations []metadata.Citation, canonicalName string, aliases []string, officialDomains []string) []metadata.Citation {
	if canonicalName == "" || len(citations) == 0 {
		return citations
	}

	const (
		threshold = 0.3 // Minimum relevance score to pass (title/snippet match = 0.4 can pass) 注：通过所需的最低相关性得分（标题/摘要匹配 0.4 也能通过）
		minKeep   = 10  // Safety floor: keep at least this many for deep research 注：安全下限：为深度研究至少保留这么多条
	)

	// Normalize canonical name and aliases for matching
	// 归一化规范名称与别名以便匹配
	canonical := strings.ToLower(strings.TrimSpace(canonicalName))
	aliasSet := make(map[string]bool)
	aliasSet[canonical] = true
	for _, a := range aliases {
		normalized := strings.ToLower(strings.TrimSpace(a))
		if normalized != "" {
			aliasSet[normalized] = true
		}
	}

	// Normalize official domains (extract domain from URL or use as-is)
	// 归一化官方域名（从 URL 中提取域名，或原样使用）
	domainSet := make(map[string]bool)
	for _, d := range officialDomains {
		normalized := strings.ToLower(strings.TrimSpace(d))
		// Remove protocol if present
		// 若存在协议前缀则移除
		normalized = strings.TrimPrefix(normalized, "https://")
		normalized = strings.TrimPrefix(normalized, "http://")
		normalized = strings.TrimPrefix(normalized, "www.")
		if normalized != "" {
			domainSet[normalized] = true
		}
	}

	type scoredCitation struct {
		citation      metadata.Citation
		score         float64
		isOfficial    bool
		matchedDomain string
		matchedAlias  string
	}

	var scored []scoredCitation
	var officialSites []scoredCitation

	for _, c := range citations {
		score := 0.0
		isOfficial := false
		matchedDomain := ""
		matchedAlias := ""

		// Check 1: Domain match - stronger signal for official domains
		// 检查 1：域名匹配——官方域名的信号更强
		urlLower := strings.ToLower(c.URL)
		for domain := range domainSet {
			if strings.Contains(urlLower, domain) {
				score += 0.6 // Increased weight for domain match 注：为域名匹配提高权重
				isOfficial = true
				matchedDomain = domain
				break
			}
		}

		// Also check if URL contains any alias (broader domain matching)
		// 同时检查 URL 是否含任意别名（更宽松的域名匹配）
		// For URLs, we use substring matching since domains often contain the brand
		// 对 URL 采用子串匹配，因为域名通常包含品牌名
		// e.g., "acme.com" contains "acme"
		// URL 用子串匹配，因为域名通常包含品牌名，例如 "acme.com" 含 "acme"。
		hostLabels := []string{}
		if parsedURL, err := url.Parse(c.URL); err == nil {
			host := strings.ToLower(parsedURL.Hostname())
			host = strings.TrimPrefix(host, "www.")
			if host != "" {
				hostLabels = strings.Split(host, ".")
			}
		}
		if !isOfficial {
			for alias := range aliasSet {
				// Remove quotes from aliases for URL matching
				// 匹配 URL 时去掉别名中的引号
				cleanAlias := strings.Trim(alias, "\"")
				if cleanAlias == "" {
					continue
				}
				if len(cleanAlias) >= 5 {
					// For long aliases, allow substring match (brand in domain/path).
					// 较长别名允许子串匹配（品牌出现在域名/路径中）。
					if strings.Contains(urlLower, cleanAlias) {
						score += 0.4 // Partial credit for alias in URL 注：URL 含别名给部分分
						matchedDomain = "alias-in-url:" + cleanAlias
						break
					}
					continue
				}

				// For short aliases, only match whole hostname labels to avoid false positives
				// 短别名只匹配完整主机名标签，避免误判
				// like "mind" matching "minders.io".
				// 例如 "mind" 命中 "minders.io"。
				if slices.Contains(hostLabels, cleanAlias) {
					score += 0.4
					matchedDomain = "alias-in-host:" + cleanAlias
				}
				if matchedDomain != "" {
					break
				}
			}
		}

		// Check 2: Title/snippet contains canonical name or aliases
		// 检查 2：标题/摘要含规范名称或别名
		titleLower := strings.ToLower(c.Title)
		snippetLower := strings.ToLower(c.Snippet)
		sourceLower := strings.ToLower(c.Source)
		combined := titleLower + " " + snippetLower + " " + sourceLower

		for alias := range aliasSet {
			cleanAlias := strings.Trim(alias, "\"")
			// Use word boundary matching to prevent "mind" matching "Minders.io"
			// 使用词边界匹配，避免 "mind" 命中 "Minders.io"
			// For short aliases (<5 chars), require exact word match
			// 短别名（<5 字符）要求精确整词匹配
			// For longer aliases, allow partial matches (e.g., "acme" in "acme.com")
			// 用词边界匹配避免 "mind" 命中 "Minders.io"；短别名（<5 字符）要求精确整词，
			// 较长别名允许部分匹配。
			if cleanAlias != "" {
				if len(cleanAlias) < 5 {
					// Short alias: require word boundary
					// 短别名：要求词边界
					if containsAsWord(combined, cleanAlias) {
						score += 0.4 // Title/snippet match 注：标题/摘要匹配
						matchedAlias = cleanAlias
						break
					}
				} else {
					// Longer alias: allow substring match
					// 较长别名：允许子串匹配
					if strings.Contains(combined, cleanAlias) {
						score += 0.4 // Title/snippet match 注：标题/摘要匹配
						matchedAlias = cleanAlias
						break
					}
				}
			}
		}

		sc := scoredCitation{
			citation:      c,
			score:         score,
			isOfficial:    isOfficial,
			matchedDomain: matchedDomain,
			matchedAlias:  matchedAlias,
		}
		scored = append(scored, sc)

		// Track official sites separately for backfill
		// 单独记录官方站点以便回填
		if isOfficial {
			officialSites = append(officialSites, sc)
		}
	}

	// Step 1: Always keep official domain citations (bypass threshold)
	// 步骤 1：官方域名引用始终保留（绕过阈值）
	var filtered []metadata.Citation
	officialKept := 0
	for _, sc := range officialSites {
		filtered = append(filtered, sc.citation)
		officialKept++
	}

	// Step 2: Add non-official citations that pass threshold
	// 步骤 2：加入通过阈值的非官方引用
	for _, sc := range scored {
		if !sc.isOfficial && sc.score >= threshold {
			filtered = append(filtered, sc.citation)
		}
	}

	// Step 3: Safety floor with backfill
	// 步骤 3：带安全下限的回填
	if len(filtered) < minKeep {
		// Sort all citations by combined score (quality × credibility + entity relevance)
		// 按综合得分（质量 × 可信度 + 实体相关性）对所有引用排序
		sort.Slice(scored, func(i, j int) bool {
			scoreI := (scored[i].citation.QualityScore * scored[i].citation.CredibilityScore) + scored[i].score
			scoreJ := (scored[j].citation.QualityScore * scored[j].citation.CredibilityScore) + scored[j].score
			return scoreI > scoreJ
		})

		// Backfill from top-scored citations
		// 从得分最高的引用开始回填
		existingURLs := make(map[string]bool)
		for _, c := range filtered {
			existingURLs[c.URL] = true
		}

		for i := 0; i < len(scored) && len(filtered) < minKeep; i++ {
			if !existingURLs[scored[i].citation.URL] {
				filtered = append(filtered, scored[i].citation)
			}
		}
	}

	return filtered
}

// CitationFilterResult holds the result of citation filtering with fallback logic.
// CitationFilterResult 保存引用过滤的结果及回退逻辑。
type CitationFilterResult struct {
	Citations []metadata.Citation
	Before    int
	After     int
	Retention float64
	Applied   bool // true if filter applied, false if fallback (kept original) 注：true 表示已应用过滤，false 表示回退（保留原始）
}

// Constants for citation filter fallback thresholds
// 引用过滤回退阈值的常量
const (
	citationFilterMinCount     = 20  // Minimum citations to accept filter result 注：接受过滤结果所需的最少引用数
	citationFilterMinRetention = 0.3 // Minimum retention rate (30%) 注：最低保留率（30%）
)

// Constants for domain discovery
// 域名发现相关的常量
const (
	maxProductHints       = 5 // Maximum product domains to extract from refiner hints 注：从 refiner 提示中提取的产品域名上限
	maxResearchFocusAreas = 3 // Maximum research areas to include in discovery query 注：域名发现查询中纳入的研究领域上限
	minProductNameLength  = 3 // Minimum length for valid product name extraction 注：有效产品名称提取的最短长度
)

// P0-B: Constants for V2 + V1 Supplement logic
// P0-B：V2 + V1 补充逻辑的常量
const (
	v2MinSupportRate    = 0.1 // 10% - below this, enable V1 supplement 注：10%——低于此值则启用 V1 补充
	v2MinCitationsUsed  = 3   // Minimum inline citations to consider V2 sufficient 注：判定 V2 足够的最少行内引用数
	v2MinClaimsRequired = 5   // Minimum claims to trigger supplement (avoid short answer false positives) 注：触发补充所需的最少 claim 数（避免简短回答误判）
	v1MaxExtraCitations = 10  // Maximum additional citations from V1 supplement 注：V1 补充最多额外引入的引用数
)

// ApplyCitationFilterWithFallback applies entity-based filtering with automatic
// ApplyCitationFilterWithFallback 应用基于实体的过滤，并在过滤会移除
// fallback when the filter would remove too many citations.
// 过多引用时自动回退。
// Returns a CitationFilterResult containing the filtered citations and metadata.
// 返回包含过滤后引用及元数据的 CitationFilterResult。
func ApplyCitationFilterWithFallback(
	citations []metadata.Citation,
	canonicalName string,
	aliases []string,
	domains []string,
) CitationFilterResult {
	beforeCount := len(citations)
	if beforeCount == 0 {
		return CitationFilterResult{Citations: citations, Applied: false}
	}

	filtered := FilterCitationsByEntity(citations, canonicalName, aliases, domains)
	retention := float64(len(filtered)) / float64(beforeCount)

	result := CitationFilterResult{
		Before:    beforeCount,
		After:     len(filtered),
		Retention: retention,
	}

	// Apply filter only if results are reasonable (>=20 citations AND >=30% retention)
	// 仅在结果合理（>=20 条引用且保留率 >=30%）时应用过滤
	// Using AND ensures we don't apply aggressive filtering that removes too many citations
	// 仅当结果数量合理（>=20 条且保留率 >=30%）时才应用过滤；用 AND 避免过度过滤。
	if len(filtered) >= citationFilterMinCount && retention >= citationFilterMinRetention {
		result.Citations = filtered
		result.Applied = true
	} else {
		result.Citations = citations // Keep original 注：保留原始
		result.Applied = false
	}

	return result
}

func mergeCitationsPreferFirst(primary []metadata.Citation, secondary []metadata.Citation) []metadata.Citation {
	if len(primary) == 0 {
		return secondary
	}
	if len(secondary) == 0 {
		return primary
	}

	out := make([]metadata.Citation, 0, len(primary)+len(secondary))
	seen := make(map[string]bool)

	citationKey := func(c metadata.Citation) string {
		if strings.TrimSpace(c.URL) != "" {
			if normalized, err := metadata.NormalizeURL(c.URL); err == nil && normalized != "" {
				return normalized
			}
			return strings.ToLower(strings.TrimSpace(c.URL))
		}
		if strings.TrimSpace(c.Source) != "" {
			return strings.ToLower(strings.TrimSpace(c.Source))
		}
		if strings.TrimSpace(c.Title) != "" {
			return strings.ToLower(strings.TrimSpace(c.Title))
		}
		return ""
	}

	add := func(c metadata.Citation) {
		key := citationKey(c)
		if key != "" {
			if seen[key] {
				return
			}
			seen[key] = true
		}
		out = append(out, c)
	}

	for _, c := range primary {
		add(c)
	}
	for _, c := range secondary {
		add(c)
	}

	return out
}

func hasSuccessfulToolExecutions(results []activities.AgentExecutionResult) bool {
	for _, ar := range results {
		for _, te := range ar.ToolExecutions {
			if te.Success {
				return true
			}
		}
	}
	return false
}

// languageToCode normalizes language strings to 2-letter codes.
// languageToCode 将语言字符串归一化为两字母代码。
// Handles variants like zh-cn, zh-hans, japanese, etc.
// 处理 zh-cn、zh-hans、japanese 等变体。
func languageToCode(lang string) string {
	l := strings.ToLower(strings.TrimSpace(lang))
	// Handle Chinese variants: zh-cn, zh-hans, zh-tw, chinese, etc.
	// 处理中文变体：zh-cn、zh-hans、zh-tw、chinese 等。
	if strings.HasPrefix(l, "zh") || l == "chinese" {
		return "zh"
	}
	// Handle Japanese variants: ja, ja-jp, japanese, etc.
	// 处理日文变体：ja、ja-jp、japanese 等。
	if strings.HasPrefix(l, "ja") || l == "japanese" {
		return "ja"
	}
	// Handle Korean variants: ko, ko-kr, korean, etc.
	// 处理韩文变体：ko、ko-kr、korean 等。
	if strings.HasPrefix(l, "ko") || l == "korean" {
		return "ko"
	}
	if len(l) == 2 {
		return l
	}
	return ""
}

// extractRegionCodeFromTargetLanguages extracts regional language code from LLM-determined target_languages.
// extractRegionCodeFromTargetLanguages 从 LLM 判定的 target_languages 中提取区域语言码。
// Returns the first regional code found (zh, ja, ko), or empty string if none.
// 返回找到的第一个区域码（zh、ja、ko），没有则返回空字符串。
// This is used to determine locale-specific sources based on company region, not query language.
// 从 LLM 判定的 target_languages 中提取区域语言码（zh/ja/ko），
// 用于按公司所在地区而非查询语言选择本地化来源。
func extractRegionCodeFromTargetLanguages(targetLanguages []string) string {
	for _, lang := range targetLanguages {
		code := languageToCode(lang)
		switch code {
		case "zh", "ja", "ko":
			return code
		}
	}
	return ""
}

// originPrefetchRegionFromTargetLanguages maps entity origin language codes to prefetch region buckets.
// originPrefetchRegionFromTargetLanguages 将实体来源语言码映射到 prefetch 区域分桶。
// - zh -> cn
// - zh -> cn
// - ja -> jp
// - ja -> jp
// - ko -> kr
// - ko -> kr
func originPrefetchRegionFromTargetLanguages(targetLanguages []string) string {
	switch extractRegionCodeFromTargetLanguages(targetLanguages) {
	case "zh":
		return "cn"
	case "ja":
		return "jp"
	case "ko":
		return "kr"
	default:
		return ""
	}
}

func normalizePrefetchRegion(raw string) string {
	r := strings.ToLower(strings.TrimSpace(raw))
	switch r {
	case "cn", "china", "zh", "zh-cn", "zh-hans", "chinese":
		return "cn"
	case "jp", "japan", "ja", "ja-jp", "japanese":
		return "jp"
	case "kr", "korea", "ko", "ko-kr", "korean":
		return "kr"
	case "eu", "europe", "european", "uk", "u.k.":
		return "eu"
	case "us", "u.s.", "usa", "united states":
		return "us"
	case "global", "worldwide", "intl", "international":
		return "global"
	default:
		return ""
	}
}

func prefetchRegionsFromContext(ctx map[string]any) []string {
	if ctx == nil {
		return nil
	}

	keys := []string{"domain_analysis_regions", "prefetch_regions", "domain_prefetch_regions"}
	for _, key := range keys {
		raw, ok := ctx[key]
		if !ok || raw == nil {
			continue
		}
		seen := make(map[string]bool)
		var out []string
		add := func(v string) {
			r := normalizePrefetchRegion(v)
			if r == "" || seen[r] {
				return
			}
			seen[r] = true
			out = append(out, r)
		}

		switch v := raw.(type) {
		case []string:
			for _, s := range v {
				add(s)
			}
		case []any:
			for _, item := range v {
				if s, ok := item.(string); ok {
					add(s)
				}
			}
		case string:
			parts := strings.FieldsFunc(v, func(r rune) bool {
				return r == ',' || r == ';' || r == ' ' || r == '\t' || r == '\n'
			})
			for _, p := range parts {
				add(p)
			}
		default:
			// Ignore unknown shapes
			// 忽略未知类型
		}

		if len(out) > 0 {
			return out
		}
	}

	return nil
}

func prefetchRegionsFromQuery(query string) []string {
	q := strings.TrimSpace(query)
	if q == "" {
		return nil
	}

	seen := make(map[string]bool)
	var out []string
	add := func(region string) {
		r := normalizePrefetchRegion(region)
		if r == "" || seen[r] {
			return
		}
		seen[r] = true
		out = append(out, r)
	}

	// Chinese keywords
	// 中文关键词
	if strings.Contains(q, "日本") {
		add("jp")
	}
	if strings.Contains(q, "中国") || strings.Contains(q, "中國") {
		add("cn")
	}
	if strings.Contains(q, "美国") || strings.Contains(q, "美國") {
		add("us")
	}
	if strings.Contains(q, "欧洲") || strings.Contains(q, "歐洲") || strings.Contains(q, "欧盟") || strings.Contains(q, "歐盟") {
		add("eu")
	}
	if strings.Contains(q, "英国") || strings.Contains(q, "英國") {
		add("eu")
	}

	// English (case-sensitive for US/EU to avoid matching pronouns like "us")
	// 英文（US/EU 区分大小写，避免匹配 "us" 这类代词）
	qLower := strings.ToLower(q)
	if strings.Contains(qLower, "japan") {
		add("jp")
	}
	if strings.Contains(qLower, "china") {
		add("cn")
	}
	if strings.Contains(q, "USA") || strings.Contains(q, "U.S.") || containsAsWord(q, "US") || strings.Contains(qLower, "united states") {
		add("us")
	}
	if containsAsWord(q, "EU") || strings.Contains(qLower, "europe") || strings.Contains(qLower, "european") || containsAsWord(q, "UK") || strings.Contains(q, "U.K.") {
		add("eu")
	}

	return out
}

type domainDiscoverySearch struct {
	Key   string
	Query string
}

func buildCompanyEUDomainDiscoverySearchQuery(canonicalName string) string {
	name := strings.TrimSpace(canonicalName)
	if name == "" {
		return ""
	}
	// Keep this simple: avoid adding disambiguation terms, consistent with buildCompanyDomainDiscoverySearchQuery().
	// 保持简单：不添加消歧词，与 buildCompanyDomainDiscoverySearchQuery() 保持一致。
	return fmt.Sprintf("%s official website Europe", name)
}

func buildDomainDiscoverySearches(canonicalName string, disambiguationTerms []string, originRegion string, requestedRegions []string, researchAreas []string, officialDomains []string) []domainDiscoverySearch {
	name := strings.TrimSpace(canonicalName)
	if name == "" {
		return nil
	}

	seen := make(map[string]bool)
	var out []domainDiscoverySearch
	add := func(key, query string) {
		query = strings.TrimSpace(query)
		if query == "" || seen[query] {
			return
		}
		seen[query] = true
		out = append(out, domainDiscoverySearch{Key: key, Query: query})
	}

	// Helper to add topic-based and product searches
	// 辅助函数：添加基于主题与产品的搜索
	addTopicSearches := func() {
		// Financial IR search
		// 财务 IR 搜索
		if containsFinancialTopic(researchAreas) {
			add("ir", fmt.Sprintf("%s investor relations", name))
		}
		// Technical documentation search
		// 技术文档搜索
		if containsTechnicalTopic(researchAreas) {
			add("docs", fmt.Sprintf("%s documentation", name))
		}
		// Culture/careers search
		// 文化/招聘搜索
		if containsCultureTopic(researchAreas) {
			add("careers", fmt.Sprintf("%s careers", name))
		}
		// Sub-entity (products/subsidiaries) search
		// 子实体（产品/子公司）搜索
		if containsSubEntityTopic(researchAreas) {
			add("subentities", fmt.Sprintf("%s products subsidiaries brands official", name))
		}
		// Product hints from refiner (search grounding)
		// 来自 refiner 的产品提示（搜索 grounding）
		productHints := extractProductHints(officialDomains, canonicalName)
		if len(productHints) > maxProductHints {
			productHints = productHints[:maxProductHints]
		}
		for _, hint := range productHints {
			add("product_"+strings.ToLower(hint), fmt.Sprintf("%s official site", hint))
		}
	}

	if len(requestedRegions) > 0 {
		for _, r := range requestedRegions {
			switch r {
			case "cn":
				add("cn", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, "zh"))
			case "jp":
				add("jp", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, "ja"))
			case "kr":
				add("kr", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, "ko"))
			case "eu":
				add("eu", buildCompanyEUDomainDiscoverySearchQuery(name))
			case "us", "global":
				add("global", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, ""))
			default:
				add("global", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, ""))
			}
		}
		addTopicSearches()
		return out
	}

	// Default (multinational coverage): global + EU + CN + JP (+ KR when origin indicates Korea).
	// 默认（跨国覆盖）：global + EU + CN + JP（来源为韩国时再加 KR）。
	add("global", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, ""))
	add("eu", buildCompanyEUDomainDiscoverySearchQuery(name))
	add("cn", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, "zh"))
	add("jp", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, "ja"))
	if originRegion == "kr" {
		add("kr", buildCompanyDomainDiscoverySearchQuery(name, disambiguationTerms, "ko"))
	}
	addTopicSearches()
	return out
}

// containsFinancialTopic checks if research areas include financial-related topics.
// containsFinancialTopic 检查研究领域是否包含财务相关主题。
func containsFinancialTopic(areas []string) bool {
	financialKeywords := []string{
		"financial", "finance", "investor", "revenue", "earnings",
		"stock", "market cap", "quarterly", "annual report", "sec filing",
		"profit", "loss", "fiscal", "shareholder", "dividend",
	}
	for _, area := range areas {
		areaLower := strings.ToLower(area)
		for _, kw := range financialKeywords {
			if strings.Contains(areaLower, kw) {
				return true
			}
		}
	}
	return false
}

// containsTechnicalTopic checks if research areas include technical/documentation topics.
// containsTechnicalTopic 检查研究领域是否包含技术/文档相关主题。
func containsTechnicalTopic(areas []string) bool {
	technicalKeywords := []string{
		"api", "sdk", "integration", "developer", "documentation",
		"technical", "architecture", "implementation", "code", "library",
		"programming", "endpoint", "webhook", "oauth",
	}
	for _, area := range areas {
		areaLower := strings.ToLower(area)
		for _, kw := range technicalKeywords {
			if strings.Contains(areaLower, kw) {
				return true
			}
		}
	}
	return false
}

// containsCultureTopic checks if research areas include culture/hiring topics.
// containsCultureTopic 检查研究领域是否包含文化/招聘相关主题。
func containsCultureTopic(areas []string) bool {
	cultureKeywords := []string{
		"culture", "hiring", "career", "job", "team", "employee",
		"workplace", "values", "diversity", "benefits", "talent",
		"recruitment", "work environment", "company values",
	}
	for _, area := range areas {
		areaLower := strings.ToLower(area)
		for _, kw := range cultureKeywords {
			if strings.Contains(areaLower, kw) {
				return true
			}
		}
	}
	return false
}

// containsSubEntityTopic checks if research areas include product/subsidiary/brand topics.
// containsSubEntityTopic 检查研究领域是否包含产品/子公司/品牌相关主题。
func containsSubEntityTopic(areas []string) bool {
	subEntityKeywords := []string{
		"product", "products", "subsidiary", "subsidiaries", "brand", "brands",
		"portfolio", "offerings", "service", "services", "division", "divisions",
		"business unit", "affiliate", "affiliates", "owned", "acquisition",
	}
	for _, area := range areas {
		areaLower := strings.ToLower(area)
		for _, kw := range subEntityKeywords {
			if strings.Contains(areaLower, kw) {
				return true
			}
		}
	}
	return false
}

// classifyFocusCategories maps ResearchAreas to domain type hints.
// classifyFocusCategories 将 ResearchAreas 映射为域名类型提示。
// Returns categories like "corporate", "docs", "ir", "store" based on research area keywords.
// 根据研究领域关键词返回 "corporate"、"docs"、"ir"、"store" 等类别。
// This is used for domain prioritization, not as the main focus (which should be input.Query).
// 这用于域名优先级排序，而非主焦点（主焦点应为 input.Query）。
func classifyFocusCategories(areas []string) []string {
	categoryMap := map[string][]string{
		"ir":        {"financial", "investor", "earnings", "revenue", "stock", "fiscal", "quarterly", "annual report", "SEC"},
		"docs":      {"api", "documentation", "technical", "developer", "sdk", "integration", "reference"},
		"store":     {"product", "pricing", "purchase", "buy", "shop", "store", "catalog", "features"},
		"corporate": {"organization", "leadership", "management", "executive", "board", "history", "mission", "about"},
		"careers":   {"career", "hiring", "job", "recruitment", "talent", "employee", "culture", "workplace"},
	}

	found := make(map[string]bool)
	for _, area := range areas {
		areaLower := strings.ToLower(area)
		for category, keywords := range categoryMap {
			for _, kw := range keywords {
				if strings.Contains(areaLower, kw) {
					found[category] = true
					break
				}
			}
		}
	}

	var result []string
	for cat := range found {
		result = append(result, cat)
	}
	return result
}

// classifyDomainRole determines the type of a domain based on URL patterns.
// classifyDomainRole 根据 URL 模式判定域名类型。
// Returns: corporate, docs, ir, store, careers, support, blog_news, or other.
// 返回：corporate、docs、ir、store、careers、support、blog_news 或 other。
// This is used to provide context to LLM for better target_keywords generation.
// 用于向 LLM 提供上下文，以便更好地生成 target_keywords。
func classifyDomainRole(urlStr string) string {
	u, err := url.Parse(urlStr)
	if err != nil {
		return "other"
	}
	host := strings.ToLower(u.Host)
	path := strings.ToLower(u.Path)

	// Check host-based patterns
	// 检查基于 host 的模式
	switch {
	case strings.Contains(host, "docs.") || strings.Contains(host, "developer.") ||
		strings.Contains(host, "api.") || strings.Contains(host, "dev."):
		return "docs"
	case strings.Contains(host, "ir.") || strings.Contains(host, "investor"):
		return "ir"
	case strings.Contains(host, "store.") || strings.Contains(host, "shop.") ||
		strings.Contains(host, "buy."):
		return "store"
	case strings.Contains(host, "careers.") || strings.Contains(host, "jobs.") ||
		strings.Contains(host, "talent."):
		return "careers"
	case strings.Contains(host, "support.") || strings.Contains(host, "help."):
		return "support"
	case strings.Contains(host, "blog.") || strings.Contains(host, "news."):
		return "blog_news"
	}

	// Check path-based patterns
	// 检查基于 path 的模式
	switch {
	case strings.HasPrefix(path, "/docs") || strings.HasPrefix(path, "/developer") ||
		strings.HasPrefix(path, "/api"):
		return "docs"
	case strings.HasPrefix(path, "/ir") || strings.HasPrefix(path, "/investor"):
		return "ir"
	case strings.HasPrefix(path, "/store") || strings.HasPrefix(path, "/shop"):
		return "store"
	case strings.HasPrefix(path, "/careers") || strings.HasPrefix(path, "/jobs"):
		return "careers"
	}

	return "corporate"
}

// DomainAnalysisIntent captures the unified task intent for Domain Analysis.
// DomainAnalysisIntent 承载 Domain Analysis 的统一任务意图。
// This structure normalizes inputs from query, context, and refiner results.
// 该结构归一化来自 query、context 与 refiner 结果的输入。
type DomainAnalysisIntent struct {
	FocusText            string   // Original input.Query (primary focus) 注：原始 input.Query（主焦点）
	FocusCategories      []string // Derived from ResearchAreas (e.g., ir, docs, corporate) 注：由 ResearchAreas 推导（例如 ir、docs、corporate）
	ExplicitRegions      []string // User-specified regions from context/query 注：用户从 context/query 指定的区域
	TargetLanguages      []string // From Refiner 注：来自 Refiner
	LocalizationNeeded   bool     // From Refiner 注：来自 Refiner
	MultinationalDefault bool     // Computed: should default to multi-region discovery 注：计算所得：是否应默认多区域发现
}

// multinationalKeywords are used to detect multi-region intent from query text.
// multinationalKeywords 用于从查询文本中识别多区域意图。
var multinationalKeywords = []string{
	"全球", "跨国", "国际", "世界", "各地区", "多国", "海外",
	"worldwide", "global", "international", "multinational", "all regions",
	"across countries", "multi-region", "cross-border",
}

// BuildDomainAnalysisIntent constructs a DomainAnalysisIntent from task inputs.
// BuildDomainAnalysisIntent 根据任务输入构造 DomainAnalysisIntent。
func BuildDomainAnalysisIntent(
	query string,
	researchAreas []string,
	explicitRegions []string,
	targetLanguages []string,
	localizationNeeded bool,
) DomainAnalysisIntent {
	intent := DomainAnalysisIntent{
		FocusText:          query,
		FocusCategories:    classifyFocusCategories(researchAreas),
		ExplicitRegions:    explicitRegions,
		TargetLanguages:    targetLanguages,
		LocalizationNeeded: localizationNeeded,
	}
	intent.MultinationalDefault = intent.computeMultinational()
	return intent
}

// computeMultinational determines if the task should default to multi-region discovery.
// computeMultinational 判断任务是否应默认多区域发现。
// Returns true if:
// 满足以下任一条件时返回 true：
// - User explicitly specified multiple regions
// - 用户显式指定了多个区域
// - Refiner detected multiple target languages (len >= 2)
// - Refiner 检测到多种目标语言（len >= 2）
// - Refiner set localization_needed = true
// - Refiner 设置 localization_needed = true
// - Query contains multinational keywords
// - 查询包含跨国关键词
func (i *DomainAnalysisIntent) computeMultinational() bool {
	// Explicit multi-region
	// 显式多区域
	if len(i.ExplicitRegions) > 1 {
		return true
	}

	// Multiple target languages
	// 多种目标语言
	if len(i.TargetLanguages) >= 2 {
		return true
	}

	// Refiner detected localization need
	// Refiner 检测到本地化需求
	if i.LocalizationNeeded {
		return true
	}

	// Query contains multinational keywords
	// 查询包含跨国关键词
	queryLower := strings.ToLower(i.FocusText)
	for _, kw := range multinationalKeywords {
		if strings.Contains(queryLower, kw) {
			return true
		}
	}

	return false
}

// extractProductHints extracts product names from refiner domains for search hints.
// extractProductHints 从 refiner 域名中提取产品名，用作搜索提示。
// Only returns domains that don't match the canonical company name.
// 仅返回与规范公司名不匹配的域名。
func extractProductHints(officialDomains []string, canonicalName string) []string {
	canonical := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(canonicalName), " ", ""))
	seen := make(map[string]bool)
	var hints []string
	for _, d := range officialDomains {
		base := extractDomainBase(d)
		if base == "" || len(base) < minProductNameLength {
			continue
		}
		baseLower := strings.ToLower(base)
		// Skip if matches canonical name or already seen
		// 与规范名匹配或已见过则跳过
		if strings.Contains(canonical, baseLower) || strings.Contains(baseLower, canonical) {
			continue
		}
		if seen[baseLower] {
			continue
		}
		seen[baseLower] = true
		hints = append(hints, base)
	}
	return hints
}

// extractDomainBase extracts the base name from a domain (youtube.com → youtube)
// extractDomainBase 从域名中提取基础名（youtube.com → youtube）
func extractDomainBase(domain string) string {
	host := normalizeDomainCandidateHost(domain)
	if host == "" {
		return ""
	}
	// Remove common TLDs
	// 移除常见 TLD
	commonTLDs := []string{".com", ".org", ".net", ".io", ".ai", ".co", ".app", ".dev", ".xyz"}
	for _, tld := range commonTLDs {
		if before, ok := strings.CutSuffix(host, tld); ok {
			host = before
			break
		}
	}
	// Get first part if subdomain (docs.stripe.com → docs after TLD removal)
	// 若为子域名则取第一段（docs.stripe.com 去掉 TLD 后为 docs）
	parts := strings.Split(host, ".")
	if len(parts) > 1 {
		// Return the last part (main domain name, not subdomain)
		// 返回最后一段（主域名，而非子域名）
		return parts[len(parts)-1]
	}
	return host
}

func originRegionToDiscoveryLanguageCode(originRegion string) string {
	switch normalizePrefetchRegion(originRegion) {
	case "cn":
		return "zh"
	case "jp":
		return "ja"
	case "kr":
		return "ko"
	default:
		return ""
	}
}

func buildCompanyDomainDiscoverySearchQuery(canonicalName string, disambiguationTerms []string, regionCode string) string {
	name := strings.TrimSpace(canonicalName)
	if name == "" {
		return ""
	}

	// For domain discovery, use ONLY the company name + "official website" keywords.
	// 域名发现只用公司名 + "official website" 关键词。
	// Do NOT add disambiguation terms - they often contain LLM-generated translations
	// 不要添加消歧词——它们常含 LLM 生成的翻译
	// or explanations that pollute search results and push official domains down.
	// 或解释，会污染搜索结果并把官方域名挤下去。
	// Example: "ExampleCorp 官网 官方网站" returns jp.example.com as #1 result,
	// but "ExampleCorp 官网 官方网站 web optimization..." pushes it out of top 10.
	// 域名发现只用公司名 + "official website" 关键词，不追加消歧词——
	// LLM 生成的翻译或解释会污染搜索结果，把官网挤出前列。
	q := fmt.Sprintf("%s official website", name)
	switch regionCode {
	case "zh":
		q = fmt.Sprintf("%s 官网 官方网站", name)
	case "ja":
		q = fmt.Sprintf("%s 公式サイト official website", name)
	case "ko":
		q = fmt.Sprintf("%s 공식 사이트 official website", name)
	}

	// NOTE: Removed disambiguation term addition for domain discovery.
	// 注意：域名发现已移除消歧词的拼接。
	// The simple query "{company} official website" is more effective
	// 简单查询 "{company} official website" 比掺入额外词的查询
	// at finding official domains than queries polluted with extra terms.
	// 更能找到官方域名。

	return q
}

func stripCodeFences(s string) string {
	t := strings.TrimSpace(s)
	if after, ok := strings.CutPrefix(t, "```json"); ok {
		t = after
		t = strings.TrimPrefix(t, "```")
		if idx := strings.LastIndex(t, "```"); idx != -1 {
			t = t[:idx]
		}
		return strings.TrimSpace(t)
	}
	if after, ok := strings.CutPrefix(t, "```"); ok {
		t = after
		if idx := strings.LastIndex(t, "```"); idx != -1 {
			t = t[:idx]
		}
		return strings.TrimSpace(t)
	}
	return t
}

func registrableDomain(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.TrimPrefix(h, "www.")
	h = strings.TrimSuffix(h, ".")
	if h == "" {
		return ""
	}
	if strings.Contains(h, ":") {
		// Drop port if present.
		// 若存在端口则丢弃。
		if hh, _, err := net.SplitHostPort(h); err == nil {
			h = hh
		} else {
			// Best-effort: split on last ':' for malformed host:port
			// 尽力而为：对格式错误的 host:port 按最后一个 ':' 切分
			if i := strings.LastIndex(h, ":"); i > 0 {
				h = h[:i]
			}
		}
	}
	if h == "" {
		return ""
	}
	if ip := net.ParseIP(h); ip != nil {
		return ""
	}
	if h == "localhost" {
		return ""
	}

	labels := strings.Split(h, ".")
	if len(labels) < 2 {
		return ""
	}

	// Minimal multi-label public suffix handling for common company research TLDs.
	// 对常见公司研究 TLD 做最小化的多级公共后缀处理。
	// This avoids collapsing e.g. sony.co.jp -> co.jp via naive last-2-labels logic.
	// 对常见公司研究 TLD 做最小化的多级公共后缀处理，
	// 避免按朴素的"最后两段"逻辑把 sony.co.jp 误判为 co.jp。
	suffix2 := labels[len(labels)-2] + "." + labels[len(labels)-1]
	suffix3 := ""
	if len(labels) >= 3 {
		suffix3 = labels[len(labels)-3] + "." + suffix2
	}

	multiLabelSuffixes := map[string]struct{}{
		// Japan
		// 日本
		"co.jp": {}, "ne.jp": {}, "or.jp": {}, "ac.jp": {}, "go.jp": {}, "ed.jp": {}, "lg.jp": {},
		// China
		// 中国
		"com.cn": {}, "net.cn": {}, "org.cn": {}, "gov.cn": {}, "edu.cn": {},
		// Korea
		// 韩国
		"co.kr": {}, "or.kr": {}, "go.kr": {}, "ac.kr": {}, "re.kr": {},
		// UK/AU (common in global companies)
		// 英国/澳大利亚（全球性公司常见）
		"co.uk": {}, "org.uk": {}, "ac.uk": {},
		"com.au": {}, "net.au": {}, "org.au": {}, "edu.au": {},
	}

	if _, ok := multiLabelSuffixes[suffix3]; ok && len(labels) >= 4 {
		return strings.Join(labels[len(labels)-4:], ".")
	}
	if _, ok := multiLabelSuffixes[suffix2]; ok && len(labels) >= 3 {
		return strings.Join(labels[len(labels)-3:], ".")
	}

	return suffix2
}

func domainsFromWebSearchToolExecutionsAll(toolExecs []activities.ToolExecution) []string {
	seen := make(map[string]bool)
	var out []string

	addURL := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if !strings.Contains(raw, "://") {
			raw = "https://" + raw
		}
		pu, err := url.Parse(raw)
		if err != nil {
			return
		}

		registrable := registrableDomain(pu.Host)
		if registrable == "" {
			return
		}

		// Exclude common aggregator/social/platform domains (check against registrable)
		// 排除常见的聚合/社交/平台域名（按 registrable 判断）
		disallowed := map[string]struct{}{
			// Social & aggregator
			// 社交与聚合站
			"wikipedia.org": {}, "crunchbase.com": {}, "linkedin.com": {}, "facebook.com": {}, "x.com": {},
			"twitter.com": {}, "medium.com": {}, "youtube.com": {}, "youtu.be": {}, "instagram.com": {},
			"bloomberg.com": {}, "reuters.com": {}, "sec.gov": {}, "prtimes.jp": {},
			// Generic platforms that pollute results
			// 污染结果的通用平台
			"google.com": {}, "salesforce.com": {}, "shopify.com": {}, "optimizely.com": {},
			"zoominfo.com": {}, "g2.com": {}, "capterra.com": {}, "trustpilot.com": {},
			// Domain registrars & info sites
			// 域名注册商与信息站
			"register.domains": {}, "porkbun.com": {}, "nominus.com": {}, "squarespace.com": {},
			"github.io": {}, "github.com": {}, "githubusercontent.com": {},
			// Job boards
			// 招聘网站
			"hiredchina.com": {}, "glassdoor.com": {}, "indeed.com": {}, "zhipin.com": {},
			// App stores
			// 应用商店
			"apps.shopify.com": {}, "chromewebstore.google.com": {}, "play.google.com": {},
			// Investor info sites
			// 投资信息站
			"tracxn.com": {}, "trjcn.com": {},
		}
		if _, ok := disallowed[registrable]; ok {
			return
		}

		fullHost := strings.ToLower(strings.TrimSpace(pu.Host))
		fullHost = strings.TrimPrefix(fullHost, "www.")
		if fullHost == "" {
			return
		}

		if !seen[fullHost] {
			seen[fullHost] = true
			out = append(out, fullHost)
		}
	}

	for _, te := range toolExecs {
		if te.Tool != "web_search" || !te.Success || te.Output == nil {
			continue
		}

		switch v := te.Output.(type) {
		case map[string]any:
			if rawResults, ok := v["results"].([]any); ok {
				for _, rr := range rawResults {
					if m, ok2 := rr.(map[string]any); ok2 {
						if u, ok3 := m["url"].(string); ok3 {
							addURL(u)
						}
					}
				}
			}
		case []any:
			for _, rr := range v {
				if m, ok2 := rr.(map[string]any); ok2 {
					if u, ok3 := m["url"].(string); ok3 {
						addURL(u)
					}
				}
			}
		default:
			if s, ok := te.Output.(string); ok {
				addURL(s)
			}
		}
	}

	return out
}

func domainsFromWebSearchToolExecutionsAllV2(toolExecs []activities.ToolExecution, canonicalName string) []string {
	canonicalLower := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(canonicalName), " ", ""))

	seen := make(map[string]bool)
	var out []string

	allowDisallowed := func(fullHost, registrable string) bool {
		if canonicalLower == "" {
			return false
		}
		// For short canonical names (e.g., "X"), only allow if it matches a full label.
		// 对短规范名（例如 "X"），仅当与完整标签匹配时才允许。
		labels := strings.Split(fullHost, ".")
		if len(labels) > 0 && labels[0] == canonicalLower {
			return true
		}
		// For normal names, allow substring match in host/registrable.
		// 对常规名称，允许在 host/registrable 中做子串匹配。
		if len(canonicalLower) >= 3 && (strings.Contains(fullHost, canonicalLower) || strings.Contains(registrable, canonicalLower)) {
			return true
		}
		return false
	}

	addURL := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if !strings.Contains(raw, "://") {
			raw = "https://" + raw
		}
		pu, err := url.Parse(raw)
		if err != nil {
			return
		}

		fullHost := strings.ToLower(strings.TrimSpace(pu.Host))
		fullHost = strings.TrimPrefix(fullHost, "www.")
		fullHost = strings.TrimSuffix(fullHost, ".")
		if fullHost == "" {
			return
		}

		registrable := registrableDomain(fullHost)
		if registrable == "" {
			return
		}

		// Exclude common directory/social/news domains unless they match the canonical name.
		// 排除常见的目录/社交/新闻域名，除非它们与规范名匹配。
		disallowed := map[string]struct{}{
			"wikipedia.org": {}, "crunchbase.com": {}, "linkedin.com": {}, "facebook.com": {}, "x.com": {},
			"twitter.com": {}, "medium.com": {}, "youtube.com": {}, "youtu.be": {}, "instagram.com": {},
			"bloomberg.com": {}, "reuters.com": {}, "sec.gov": {}, "prtimes.jp": {},
			"zoominfo.com": {}, "g2.com": {}, "capterra.com": {}, "trustpilot.com": {},
		}
		if _, ok := disallowed[registrable]; ok && !allowDisallowed(fullHost, registrable) {
			return
		}

		if !seen[fullHost] {
			seen[fullHost] = true
			out = append(out, fullHost)
		}
	}

	for _, te := range toolExecs {
		if te.Tool != "web_search" || !te.Success || te.Output == nil {
			continue
		}

		switch v := te.Output.(type) {
		case map[string]any:
			if rawResults, ok := v["results"].([]any); ok {
				for _, rr := range rawResults {
					if m, ok2 := rr.(map[string]any); ok2 {
						if u, ok3 := m["url"].(string); ok3 {
							addURL(u)
						}
					}
				}
			}
		case []any:
			for _, rr := range v {
				if m, ok2 := rr.(map[string]any); ok2 {
					if u, ok3 := m["url"].(string); ok3 {
						addURL(u)
					}
				}
			}
		default:
			if s, ok := te.Output.(string); ok {
				addURL(s)
			}
		}
	}

	return out
}

func domainBucketForHost(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.TrimPrefix(h, "www.")
	h = strings.TrimSuffix(h, ".")

	// Japan
	// 日本
	if strings.HasPrefix(h, "jp.") || strings.Contains(h, ".jp.") || strings.HasSuffix(h, ".jp") || strings.HasSuffix(h, ".co.jp") || strings.HasSuffix(h, ".ne.jp") || strings.HasSuffix(h, ".or.jp") {
		return "jp"
	}

	// China
	// 中国
	if strings.HasPrefix(h, "cn.") || strings.Contains(h, ".cn.") || strings.HasSuffix(h, ".cn") || strings.HasSuffix(h, ".com.cn") {
		return "cn"
	}

	// Korea
	// 韩国
	if strings.HasPrefix(h, "kr.") || strings.Contains(h, ".kr.") || strings.HasSuffix(h, ".kr") || strings.HasSuffix(h, ".co.kr") {
		return "kr"
	}

	// Europe (heuristics): EU TLD + common European ccTLDs
	// 欧洲（启发式）：EU TLD + 常见欧洲 ccTLD
	if strings.HasPrefix(h, "eu.") || strings.Contains(h, ".eu.") || strings.HasSuffix(h, ".eu") ||
		strings.HasSuffix(h, ".co.uk") || strings.HasSuffix(h, ".org.uk") || strings.HasSuffix(h, ".ac.uk") ||
		strings.HasSuffix(h, ".de") || strings.HasSuffix(h, ".fr") || strings.HasSuffix(h, ".it") || strings.HasSuffix(h, ".es") ||
		strings.HasSuffix(h, ".nl") || strings.HasSuffix(h, ".se") || strings.HasSuffix(h, ".fi") || strings.HasSuffix(h, ".dk") {
		return "eu"
	}

	return "global"
}

func selectDomainsForPrefetch(discovered []string, requestedRegions []string, originRegion string, max int) []string {
	if max <= 0 || len(discovered) == 0 {
		return nil
	}

	// Deduplicate while preserving order
	// 去重并保持顺序
	seen := make(map[string]bool)
	var uniq []string
	for _, d := range discovered {
		d = strings.TrimSpace(d)
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		uniq = append(uniq, d)
	}

	// User-scoped: only include requested region buckets.
	// 用户限定：只保留所选区域的分桶。
	if len(requestedRegions) > 0 {
		allowed := make(map[string]bool)
		for _, r := range requestedRegions {
			rr := normalizePrefetchRegion(r)
			if rr == "" {
				continue
			}
			// Map US/global requests to the "global" bucket to match domainBucketForHost().
			// 把 US/global 请求映射到 "global" 分桶，以与 domainBucketForHost() 对齐。
			if rr == "us" || rr == "global" {
				allowed["global"] = true
				continue
			}
			allowed[rr] = true
		}
		if len(allowed) == 0 {
			return nil
		}
		var filtered []string
		for _, d := range uniq {
			if allowed[domainBucketForHost(d)] {
				filtered = append(filtered, d)
			}
		}
		if len(filtered) == 0 {
			return nil
		}
		if len(filtered) > max {
			return filtered[:max]
		}
		return filtered
	}

	// Default: ensure coverage across origin/us/global/eu/cn/jp when available.
	// 默认：尽可能覆盖 origin/us/global/eu/cn/jp。
	required := []string{}
	addReq := func(r string) {
		r = normalizePrefetchRegion(r)
		if r == "" {
			return
		}
		if slices.Contains(required, r) {
			return
		}
		required = append(required, r)
	}

	addReq(originRegion)
	addReq("us") // Treat global .com as US/global coverage 注：把 global 的 .com 视为 US/global 覆盖
	addReq("eu")
	addReq("cn")
	addReq("jp")

	picked := make(map[string]bool)
	var selected []string

	// Pick one per required bucket first
	// 先从每个必需分桶中各取一个
	for _, bucket := range required {
		matchBucket := bucket
		if bucket == "us" {
			matchBucket = "global"
		}
		for _, d := range uniq {
			if picked[d] {
				continue
			}
			if domainBucketForHost(d) == matchBucket {
				picked[d] = true
				selected = append(selected, d)
				break
			}
		}
		if len(selected) >= max {
			return selected[:max]
		}
	}

	// Fill remaining slots in discovered order
	// 按发现顺序填充剩余名额
	for _, d := range uniq {
		if picked[d] {
			continue
		}
		picked[d] = true
		selected = append(selected, d)
		if len(selected) >= max {
			break
		}
	}

	if len(selected) > max {
		return selected[:max]
	}
	return selected
}

// selectDomainsForPrefetchWithFocus extends selectDomainsForPrefetch with focus-aware scoring.
// selectDomainsForPrefetchWithFocus 在 selectDomainsForPrefetch 基础上加入焦点感知打分。
// focusCategories are domain types to prioritize (e.g., "ir", "docs", "corporate").
// 在 selectDomainsForPrefetch 基础上加入焦点感知打分，
// focusCategories 为需要优先的域名类型（如 "ir"、"docs"、"corporate"）。
func selectDomainsForPrefetchWithFocus(discovered []string, requestedRegions []string, originRegion string, max int, focusCategories []string) []string {
	if max <= 0 || len(discovered) == 0 {
		return nil
	}

	// Deduplicate while preserving order
	// 去重并保持顺序
	seen := make(map[string]bool)
	var uniq []string
	for _, d := range discovered {
		d = strings.TrimSpace(d)
		if d == "" || seen[d] {
			continue
		}
		seen[d] = true
		uniq = append(uniq, d)
	}

	// Score each domain based on focus categories
	// 按焦点类别为每个域名打分
	type scoredDomain struct {
		Domain string
		Score  int
		Bucket string
	}
	scored := make([]scoredDomain, len(uniq))
	for i, d := range uniq {
		role := classifyDomainRole("https://" + d)
		score := computeDomainFocusScore(role, focusCategories)
		scored[i] = scoredDomain{
			Domain: d,
			Score:  score,
			Bucket: domainBucketForHost(d),
		}
	}

	// User-scoped: only include requested region buckets.
	// 用户限定：只保留所选区域的分桶。
	if len(requestedRegions) > 0 {
		allowed := make(map[string]bool)
		for _, r := range requestedRegions {
			rr := normalizePrefetchRegion(r)
			if rr == "" {
				continue
			}
			if rr == "us" || rr == "global" {
				allowed["global"] = true
				continue
			}
			allowed[rr] = true
		}
		if len(allowed) == 0 {
			return nil
		}

		// Filter and sort by score
		// 过滤并按得分排序
		var filtered []scoredDomain
		for _, sd := range scored {
			if allowed[sd.Bucket] {
				filtered = append(filtered, sd)
			}
		}
		if len(filtered) == 0 {
			return nil
		}

		// Sort by score descending, preserve order for equal scores
		// 按得分降序排序，同分保持原有顺序
		sort.SliceStable(filtered, func(i, j int) bool {
			return filtered[i].Score > filtered[j].Score
		})

		result := make([]string, 0, max)
		for i := 0; i < len(filtered) && i < max; i++ {
			result = append(result, filtered[i].Domain)
		}
		return result
	}

	// Default: ensure coverage across origin/us/global/eu/cn/jp when available.
	// 默认：尽可能覆盖 origin/us/global/eu/cn/jp。
	// But sort within each bucket by focus score.
	// 但每个分桶内部按焦点得分排序。
	required := []string{}
	addReq := func(r string) {
		r = normalizePrefetchRegion(r)
		if r == "" {
			return
		}
		if slices.Contains(required, r) {
			return
		}
		required = append(required, r)
	}

	addReq(originRegion)
	addReq("us")
	addReq("eu")
	addReq("cn")
	addReq("jp")

	// Group by bucket
	// 按分桶分组
	bucketDomains := make(map[string][]scoredDomain)
	for _, sd := range scored {
		bucket := sd.Bucket
		bucketDomains[bucket] = append(bucketDomains[bucket], sd)
	}

	// Sort each bucket by score descending
	// 各分桶内按得分降序排序
	for bucket := range bucketDomains {
		sort.SliceStable(bucketDomains[bucket], func(i, j int) bool {
			return bucketDomains[bucket][i].Score > bucketDomains[bucket][j].Score
		})
	}

	picked := make(map[string]bool)
	var selected []string

	// Pick best from each required bucket first
	// 先从每个必需分桶中取最优者
	for _, bucket := range required {
		matchBucket := bucket
		if bucket == "us" {
			matchBucket = "global"
		}
		domains := bucketDomains[matchBucket]
		for _, sd := range domains {
			if picked[sd.Domain] {
				continue
			}
			picked[sd.Domain] = true
			selected = append(selected, sd.Domain)
			break
		}
		if len(selected) >= max {
			return selected[:max]
		}
	}

	// Fill remaining slots: sort all remaining by score descending
	// 填充剩余名额：所有剩余项按得分降序排序
	var remaining []scoredDomain
	for _, sd := range scored {
		if !picked[sd.Domain] {
			remaining = append(remaining, sd)
		}
	}
	sort.SliceStable(remaining, func(i, j int) bool {
		return remaining[i].Score > remaining[j].Score
	})

	for _, sd := range remaining {
		picked[sd.Domain] = true
		selected = append(selected, sd.Domain)
		if len(selected) >= max {
			break
		}
	}

	if len(selected) > max {
		return selected[:max]
	}
	return selected
}

// computeDomainFocusScore calculates how well a domain role matches focus categories.
// computeDomainFocusScore 计算域名角色与焦点类别的匹配程度。
func computeDomainFocusScore(role string, focusCategories []string) int {
	if len(focusCategories) == 0 {
		return 0
	}

	score := 0
	for _, cat := range focusCategories {
		switch {
		// Product focus: docs and store are relevant
		// 产品焦点：docs 与 store 相关
		case (cat == "store" || cat == "product") && (role == "store" || role == "docs"):
			score += 10
		// Financial focus: ir is highly relevant
		// 财务焦点：ir 高度相关
		case (cat == "ir" || cat == "financial") && role == "ir":
			score += 15
		// Organizational focus: corporate is relevant
		// 组织焦点：corporate 相关
		case (cat == "corporate" || cat == "organization") && role == "corporate":
			score += 10
		// Technical focus: docs is highly relevant
		// 技术焦点：docs 高度相关
		case (cat == "docs" || cat == "technical") && role == "docs":
			score += 15
		// Careers focus
		// 招聘焦点
		case cat == "careers" && role == "careers":
			score += 10
		// Exact match
		// 完全匹配
		case cat == role:
			score += 10
		}
	}
	return score
}

func normalizeDomainCandidateHost(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	pu, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	h := strings.ToLower(strings.TrimSpace(pu.Host))
	h = strings.TrimPrefix(h, "www.")
	h = strings.TrimSuffix(h, ".")
	if h == "" {
		return ""
	}
	// Drop port if present.
	// 若存在端口则丢弃。
	if strings.Contains(h, ":") {
		if hh, _, err := net.SplitHostPort(h); err == nil {
			h = hh
		} else if i := strings.LastIndex(h, ":"); i > 0 {
			h = h[:i]
		}
	}
	if h == "" {
		return ""
	}
	if ip := net.ParseIP(h); ip != nil {
		return ""
	}
	if h == "localhost" {
		return ""
	}
	return h
}

func tldLabel(host string) string {
	h := strings.ToLower(strings.TrimSpace(host))
	h = strings.TrimPrefix(h, "www.")
	h = strings.TrimSuffix(h, ".")
	if h == "" {
		return ""
	}
	parts := strings.Split(h, ".")
	if len(parts) < 2 {
		return ""
	}
	return parts[len(parts)-1]
}

func vettedHybridRefinementDomains(discovered []string, refinement []string, canonicalName string) []string {
	canonicalLower := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(canonicalName), " ", ""))

	discoveredRegistrables := make(map[string]bool)
	discoveredTLDs := make(map[string]bool)
	for _, d := range discovered {
		h := normalizeDomainCandidateHost(d)
		if h == "" {
			continue
		}
		reg := registrableDomain(h)
		if reg == "" {
			continue
		}
		discoveredRegistrables[reg] = true
		tld := tldLabel(reg)
		if tld != "" {
			discoveredTLDs[tld] = true
		}
	}

	seen := make(map[string]bool)
	var out []string
	for _, r := range refinement {
		h := normalizeDomainCandidateHost(r)
		if h == "" || seen[h] {
			continue
		}
		reg := registrableDomain(h)
		if reg == "" {
			continue
		}

		// (1) Anchored by registrable domain already observed in discovery.
		// (1) 由发现阶段已出现的 registrable 域名锚定。
		if discoveredRegistrables[reg] {
			seen[h] = true
			out = append(out, h)
			continue
		}

		// (2) Brand-TLD expansion: allow second-level domains under a brand TLD that matches the canonical name.
		// (2) 品牌 TLD 扩展：允许与规范名匹配的品牌 TLD 下的二级域名。
		// Example: about.google discovered -> allow blog.google when canonical is "Google".
		// 例如：已发现 about.google -> 规范名为 "Google" 时允许 blog.google。
		tld := tldLabel(h)
		if canonicalLower != "" && tld != "" && tld == canonicalLower && discoveredTLDs[tld] {
			seen[h] = true
			out = append(out, h)
			continue
		}
	}

	return out
}

func hostHasLabel(host, label string) bool {
	h := strings.ToLower(strings.TrimSpace(host))
	label = strings.ToLower(strings.TrimSpace(label))
	if h == "" || label == "" {
		return false
	}
	return strings.HasPrefix(h, label+".") || strings.Contains(h, "."+label+".")
}

func domainPrefetchScore(host, canonicalName string) int {
	h := normalizeDomainCandidateHost(host)
	if h == "" {
		return -100000
	}
	score := 0

	reg := registrableDomain(h)
	if reg != "" && reg == h {
		score += 100
	} else {
		score += 80
	}

	canonicalLower := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(canonicalName), " ", ""))
	if canonicalLower != "" && strings.Contains(h, canonicalLower) {
		score += 10
	}

	switch {
	case hostHasLabel(h, "about") || hostHasLabel(h, "company"):
		score += 35
	case hostHasLabel(h, "investor") || hostHasLabel(h, "investors") || hostHasLabel(h, "ir"):
		score += 35
	}
	if hostHasLabel(h, "leadership") || hostHasLabel(h, "management") {
		score += 20
	}
	if hostHasLabel(h, "press") || hostHasLabel(h, "newsroom") {
		score += 15
	}
	if hostHasLabel(h, "blog") {
		score += 20
	}
	if hostHasLabel(h, "cloud") {
		score += 15
	}
	if hostHasLabel(h, "store") {
		score += 10
	}

	// Down-rank functional endpoints that rarely help company overview.
	// 降低很少有助于公司概览的功能性端点的排序。
	if hostHasLabel(h, "support") || hostHasLabel(h, "accounts") {
		score -= 80
	}
	if hostHasLabel(h, "login") || hostHasLabel(h, "signin") {
		score -= 60
	}
	if hostHasLabel(h, "careers") || hostHasLabel(h, "jobs") {
		score -= 30
	}

	// Prefer shorter hosts slightly (fewer labels).
	// 略偏好较短的主机名（标签更少）。
	labels := strings.Split(h, ".")
	score -= len(labels) * 2

	return score
}

func sortDomainsForHybridPrefetch(domains []string, canonicalName string) []string {
	seen := make(map[string]bool)
	var uniq []string
	for _, d := range domains {
		h := normalizeDomainCandidateHost(d)
		if h == "" || seen[h] {
			continue
		}
		seen[h] = true
		uniq = append(uniq, h)
	}

	scores := make(map[string]int, len(uniq))
	for _, d := range uniq {
		scores[d] = domainPrefetchScore(d, canonicalName)
	}

	sort.SliceStable(uniq, func(i, j int) bool {
		si := scores[uniq[i]]
		sj := scores[uniq[j]]
		if si != sj {
			return si > sj
		}
		return uniq[i] < uniq[j]
	})

	return uniq
}

func buildPrefetchURLsFromDomains(domains []string) []string {
	seen := make(map[string]bool)
	var urls []string
	for _, d := range domains {
		raw := strings.TrimSpace(d)
		if raw == "" {
			continue
		}
		raw = strings.TrimPrefix(raw, "https://")
		raw = strings.TrimPrefix(raw, "http://")
		raw = strings.TrimPrefix(raw, "www.")
		if raw == "" {
			continue
		}
		u := "https://" + raw
		if !seen[u] {
			seen[u] = true
			urls = append(urls, u)
		}
	}
	return urls
}

func domainsFromWebSearchToolExecutions(toolExecs []activities.ToolExecution, canonicalName string) []string {
	seen := make(map[string]bool)
	var out []string

	// Normalize canonical name for matching
	// 归一化规范名以便匹配
	canonicalLower := strings.ToLower(strings.ReplaceAll(canonicalName, " ", ""))

	addURL := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		if !strings.Contains(raw, "://") {
			raw = "https://" + raw
		}
		pu, err := url.Parse(raw)
		if err != nil {
			return
		}

		// Get registrable domain for filtering (e.g., example.com from jp.example.com)
		// 取 registrable 域名用于过滤（例如从 jp.example.com 得到 example.com）
		registrable := registrableDomain(pu.Host)
		if registrable == "" {
			return
		}

		// Exclude common aggregator/social/platform domains (check against registrable)
		// 排除常见的聚合/社交/平台域名（按 registrable 判断）
		disallowed := map[string]struct{}{
			// Social & aggregator
			// 社交与聚合站
			"wikipedia.org": {}, "crunchbase.com": {}, "linkedin.com": {}, "facebook.com": {}, "x.com": {},
			"twitter.com": {}, "medium.com": {}, "youtube.com": {}, "youtu.be": {}, "instagram.com": {},
			"bloomberg.com": {}, "reuters.com": {}, "sec.gov": {}, "prtimes.jp": {},
			// Generic platforms that pollute results
			// 污染结果的通用平台
			"google.com": {}, "salesforce.com": {}, "shopify.com": {}, "optimizely.com": {},
			"zoominfo.com": {}, "g2.com": {}, "capterra.com": {}, "trustpilot.com": {},
			// Domain registrars & info sites
			// 域名注册商与信息站
			"register.domains": {}, "porkbun.com": {}, "nominus.com": {}, "squarespace.com": {},
			"github.io": {}, "github.com": {}, "githubusercontent.com": {},
			// Job boards
			// 招聘网站
			"hiredchina.com": {}, "glassdoor.com": {}, "indeed.com": {}, "zhipin.com": {},
			// App stores
			// 应用商店
			"apps.shopify.com": {}, "chromewebstore.google.com": {}, "play.google.com": {},
			// Investor info sites
			// 投资信息站
			"tracxn.com": {}, "trjcn.com": {},
		}
		if _, ok := disallowed[registrable]; ok {
			return
		}

		// Preserve full host for company research (jp.example.com, cn.example.com are different sites)
		// 为公司研究保留完整 host（jp.example.com 与 cn.example.com 是不同站点）
		// Only strip www. prefix
		// 仅去掉 www. 前缀
		fullHost := strings.ToLower(strings.TrimSpace(pu.Host))
		fullHost = strings.TrimPrefix(fullHost, "www.")
		if fullHost == "" {
			return
		}

		// Relevance check: domain should be related to canonical name
		// 相关性检查：域名应与规范名相关
		// This prevents profitmind.com from being included when searching for PTmind
		// 这样在搜索 PTmind 时不会把 profitmind.com 纳入
		hostLower := strings.ToLower(registrable)
		hostBase := strings.TrimSuffix(strings.TrimSuffix(hostLower, ".com"), ".co")
		hostBase = strings.TrimSuffix(hostBase, ".jp")
		hostBase = strings.TrimSuffix(hostBase, ".cn")

		isRelevant := false
		// Check if host contains canonical name (ptengine contains ptengine)
		// 检查 host 是否包含规范名（ptengine 含 ptengine）
		if strings.Contains(hostLower, canonicalLower) {
			isRelevant = true
		}
		// Check if canonical name contains host base (base contains base from example.com)
		// 检查规范名是否包含 host 基础名（基础名来自 example.com）
		if strings.Contains(canonicalLower, hostBase) && len(hostBase) >= 3 {
			isRelevant = true
		}
		// Allow if no canonical name provided (fallback)
		// 未提供规范名时一律放行（兜底）
		if canonicalLower == "" {
			isRelevant = true
		}

		if !isRelevant {
			return
		}

		// Add full host (preserving subdomains like jp.example.com)
		// 加入完整 host（保留 jp.example.com 这类子域名）
		if !seen[fullHost] {
			seen[fullHost] = true
			out = append(out, fullHost)
		}
	}

	for _, te := range toolExecs {
		if te.Tool != "web_search" || !te.Success || te.Output == nil {
			continue
		}

		switch v := te.Output.(type) {
		case map[string]any:
			// Common shape: {results:[{url:...}, ...]}
			// 常见结构：{results:[{url:...}, ...]}
			if rawResults, ok := v["results"].([]any); ok {
				for _, rr := range rawResults {
					if m, ok2 := rr.(map[string]any); ok2 {
						if u, ok3 := m["url"].(string); ok3 {
							addURL(u)
						}
					}
				}
			}
		case []any:
			// Alternate shape: [{url:...}, ...]
			// 另一种结构：[{url:...}, ...]
			for _, rr := range v {
				if m, ok2 := rr.(map[string]any); ok2 {
					if u, ok3 := m["url"].(string); ok3 {
						addURL(u)
					}
				}
			}
		default:
			// Fallback: best-effort string parse
			// 兜底：尽力解析字符串
			if s, ok := te.Output.(string); ok {
				addURL(s)
			}
		}
	}

	return out
}

func domainsFromDiscoveryResponse(resp string) []string {
	type domainDiscoveryResponse struct {
		Domains []string `json:"domains"`
	}

	var parsed domainDiscoveryResponse
	if err := json.Unmarshal([]byte(stripCodeFences(resp)), &parsed); err != nil {
		return nil
	}

	seen := make(map[string]bool)
	var out []string
	for _, d := range parsed.Domains {
		host := registrableDomain(d)
		if host == "" || seen[host] {
			continue
		}
		seen[host] = true
		out = append(out, host)
	}
	return out
}

func extractFirstJSONObjectContainingDomains(resp string) string {
	before, _, ok := strings.Cut(resp, "\"domains\"")
	if !ok {
		return ""
	}

	start := strings.LastIndex(before, "{")
	if start == -1 {
		return ""
	}

	depth := 0
	inString := false
	escaped := false
	for i := start; i < len(resp); i++ {
		ch := resp[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}

		switch ch {
		case '"':
			inString = true
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return resp[start : i+1]
			}
		}
	}

	return ""
}

func normalizeDiscoveryDomainCandidate(raw string) string {
	s := strings.TrimSpace(raw)
	if s == "" {
		return ""
	}

	// Best-effort: accept both domains and URLs.
	// 尽力而为：同时接受域名与 URL。
	if strings.Contains(s, "://") {
		if pu, err := url.Parse(s); err == nil {
			s = pu.Hostname()
		}
	} else {
		// Remove obvious schemes/prefixes without needing url.Parse.
		// 无需 url.Parse 即可去掉明显的协议/前缀。
		s = strings.TrimPrefix(s, "https://")
		s = strings.TrimPrefix(s, "http://")
		if idx := strings.IndexAny(s, "/?#"); idx != -1 {
			s = s[:idx]
		}
	}

	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.TrimPrefix(s, "www.")
	s = strings.TrimSuffix(s, ".")
	if s == "" {
		return ""
	}
	if strings.Contains(s, ":") {
		if hh, _, err := net.SplitHostPort(s); err == nil {
			s = hh
		} else {
			if i := strings.LastIndex(s, ":"); i > 0 {
				s = s[:i]
			}
		}
	}
	if s == "" {
		return ""
	}
	if ip := net.ParseIP(s); ip != nil {
		return ""
	}
	if s == "localhost" {
		return ""
	}
	if strings.Count(s, ".") < 1 {
		return ""
	}

	// Hard block common non-official domains.
	// 硬性屏蔽常见非官方域名。
	disallowedRegistrables := map[string]struct{}{
		"wikipedia.org": {}, "crunchbase.com": {}, "linkedin.com": {}, "facebook.com": {}, "x.com": {},
		"twitter.com": {}, "medium.com": {}, "youtube.com": {}, "youtu.be": {}, "instagram.com": {},
		"bloomberg.com": {}, "reuters.com": {}, "sec.gov": {}, "prtimes.jp": {},
		"zoominfo.com": {}, "g2.com": {}, "capterra.com": {}, "trustpilot.com": {},
		"github.com": {}, "github.io": {}, "githubusercontent.com": {},
	}
	if reg := registrableDomain(s); reg != "" {
		if _, ok := disallowedRegistrables[reg]; ok {
			return ""
		}
	}

	return s
}

func domainsFromDiscoveryResponseV2(resp string) []string {
	type domainDiscoveryResponse struct {
		Domains []string `json:"domains"`
	}

	// 1) Fast path: JSON-only response (or codefenced JSON).
	// 1) 快速路径：纯 JSON 响应（或带代码围栏的 JSON）。
	var parsed domainDiscoveryResponse
	if err := json.Unmarshal([]byte(stripCodeFences(resp)), &parsed); err != nil {
		// 2) Common case: response contains sections + a JSON block.
		// 2) 常见情形：响应包含若干段落 + 一个 JSON 块。
		obj := extractFirstJSONObjectContainingDomains(resp)
		if obj == "" {
			// 3) Also try inside the first code-fenced block, then extract.
			// 3) 也尝试在第一个代码围栏块内提取。
			obj = extractFirstJSONObjectContainingDomains(stripCodeFences(resp))
		}
		if obj == "" {
			return nil
		}
		if err2 := json.Unmarshal([]byte(obj), &parsed); err2 != nil {
			return nil
		}
	}

	seen := make(map[string]bool)
	var out []string
	for _, d := range parsed.Domains {
		host := normalizeDiscoveryDomainCandidate(d)
		if host == "" || seen[host] {
			continue
		}
		seen[host] = true
		out = append(out, host)
	}
	return out
}

// buildCompanyPrefetchURLsWithLocale constructs URLs including locale-specific sources
// buildCompanyPrefetchURLsWithLocale 构造 URL，并包含按 locale 区分的来源
func buildCompanyPrefetchURLsWithLocale(canonicalName string, officialDomains []string, detectedLanguage string) []string {
	seen := make(map[string]bool)
	var urls []string

	add := func(raw string) {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			return
		}
		// Strip scheme and common prefixes for domain-only inputs
		// 对仅含域名的输入，去掉协议与常见前缀
		if !strings.Contains(raw, "/") || strings.HasPrefix(raw, "http") {
			raw = strings.TrimPrefix(raw, "https://")
			raw = strings.TrimPrefix(raw, "http://")
			raw = strings.TrimPrefix(raw, "www.")
		}
		// For domain-only inputs, cut at first path/query/fragment separator
		// 对仅含域名的输入，在第一个 path/query/fragment 分隔符处截断
		if !strings.Contains(raw, "/") {
			if idx := strings.IndexAny(raw, "/?#"); idx != -1 {
				raw = raw[:idx]
			}
		}
		if raw == "" {
			return
		}
		url := raw
		if !strings.HasPrefix(url, "http://") && !strings.HasPrefix(url, "https://") {
			url = "https://" + raw
		}
		if !seen[url] {
			seen[url] = true
			urls = append(urls, url)
		}
	}

	// 1) Official domains (highest priority)
	// 1) 官方域名（最高优先级）
	for _, d := range officialDomains {
		add(d)
	}

	// 2) Config-based aggregator sources (Crunchbase, LinkedIn, etc.)
	// 2) 配置化的聚合来源（Crunchbase、LinkedIn 等）
	if canonicalName != "" {
		// Always include global company sources
		// 始终包含全球公司来源
		configURLs := config.GetPrefetchURLs(canonicalName, "company")
		for _, u := range configURLs {
			add(u)
		}

		// Add locale-specific sources based on detected language
		// 按检测到的语言添加 locale 专属来源
		if detectedLanguage == "ja" {
			jaURLs := config.GetPrefetchURLs(canonicalName, "company_ja")
			for _, u := range jaURLs {
				add(u)
			}
		} else if detectedLanguage == "zh" {
			zhURLs := config.GetPrefetchURLs(canonicalName, "company_zh")
			for _, u := range zhURLs {
				add(u)
			}
		}
	}

	// 3) Simple fallback: derive from canonical name when no official domains are present
	// 3) 简单兜底：没有官方域名时，从规范名推导
	if len(officialDomains) == 0 && canonicalName != "" {
		name := strings.ToLower(strings.TrimSpace(canonicalName))
		if name != "" {
			var b strings.Builder
			for _, r := range name {
				if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
					b.WriteRune(r)
				}
			}
			host := b.String()
			if host != "" {
				add(host + ".com")
			}
		}
	}

	return urls
}

// ResearchWorkflow demonstrates composed patterns for complex research tasks.
// ResearchWorkflow 演示了用于复杂研究任务的组合式 pattern。
// It combines React loops, parallel research, and reflection patterns.
// 它组合了 React 循环、并行研究与反思 pattern。
func ResearchWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("Starting ResearchWorkflow with composed patterns",
		"query", input.Query,
		"session_id", input.SessionID,
		"version", "v2",
	)

	// Configure activity options
	// 配置 activity options
	// Increased timeout from 5min to 8min for SynthesizeResultsLLM which can take 5+ minutes
	// 为 SynthesizeResultsLLM 把超时从 5 分钟延长到 8 分钟（该 activity 可能耗时 5 分钟以上）
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: 8 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// Set up workflow ID and emit context for event streaming
	// 设置 workflow ID 并生成用于事件流式推送的 context
	workflowID := input.ParentWorkflowID
	if workflowID == "" {
		workflowID = workflow.GetInfo(ctx).WorkflowExecution.ID
	}
	callerWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
	emitCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Second,
		RetryPolicy:         &temporal.RetryPolicy{MaximumAttempts: 1},
	})

	// Initialize control signal handler for pause/resume/cancel
	// 初始化暂停/恢复/取消的控制信号处理器
	// Skip SSE emissions when running as child workflow (parent already emits)
	// 作为子 workflow 运行时跳过 SSE 发送（父级已经发送）
	controlHandler := &control.SignalHandler{
		WorkflowID:  workflowID,
		AgentID:     "research",
		Logger:      logger,
		EmitCtx:     emitCtx,
		SkipSSEEmit: input.ParentWorkflowID != "",
	}
	controlHandler.Setup(ctx)

	// Prepare base context: start from SessionCtx, then overlay request Context
	// 准备基础 context：从 SessionCtx 开始，再覆盖请求的 Context
	// Per-request context must take precedence over persisted session defaults
	// 单次请求的 context 必须优先于持久化的 session 默认值
	baseContext := make(map[string]any)
	maps.Copy(baseContext, input.SessionCtx)
	maps.Copy(baseContext, input.Context)
	if input.ParentWorkflowID != "" {
		baseContext["parent_workflow_id"] = input.ParentWorkflowID
	}

	// Memory retrieval with gate precedence (hierarchical > simple session)
	// 记忆检索及其门控优先级（hierarchical 优先于 simple session）
	hierarchicalVersion := workflow.GetVersion(ctx, "memory_retrieval_v1", workflow.DefaultVersion, 1)
	sessionVersion := workflow.GetVersion(ctx, "session_memory_v1", workflow.DefaultVersion, 1)

	if hierarchicalVersion >= 1 && input.SessionID != "" {
		// Use hierarchical memory (combines recent + semantic)
		// 使用层级记忆（结合近期与语义）
		var hierMemory activities.FetchHierarchicalMemoryResult
		_ = workflow.ExecuteActivity(ctx, activities.FetchHierarchicalMemory,
			activities.FetchHierarchicalMemoryInput{
				Query:        input.Query,
				SessionID:    input.SessionID,
				TenantID:     input.TenantID,
				RecentTopK:   5,    // Fixed for determinism 注：为确定性固定
				SemanticTopK: 5,    // Fixed for determinism 注：为确定性固定
				Threshold:    0.75, // Fixed semantic threshold 注：固定的语义阈值
			}).Get(ctx, &hierMemory)

		if len(hierMemory.Items) > 0 {
			baseContext["agent_memory"] = hierMemory.Items
			logger.Info("Injected hierarchical memory into research context",
				"session_id", input.SessionID,
				"memory_items", len(hierMemory.Items),
				"sources", hierMemory.Sources,
			)
		}
	} else if sessionVersion >= 1 && input.SessionID != "" {
		// Fallback to simple session memory if hierarchical not enabled
		// 未启用层级记忆时回退到简单 session 记忆
		var sessionMemory activities.FetchSessionMemoryResult
		_ = workflow.ExecuteActivity(ctx, activities.FetchSessionMemory,
			activities.FetchSessionMemoryInput{
				SessionID: input.SessionID,
				TenantID:  input.TenantID,
				TopK:      20, // Fixed for determinism 注：为确定性固定
			}).Get(ctx, &sessionMemory)

		if len(sessionMemory.Items) > 0 {
			baseContext["agent_memory"] = sessionMemory.Items
			logger.Info("Injected session memory into research context",
				"session_id", input.SessionID,
				"memory_items", len(sessionMemory.Items),
			)
		}
	}

	// Context compression (version-gated for determinism)
	// Context 压缩（版本门控以保证确定性）
	compressionVersion := workflow.GetVersion(ctx, "context_compress_v1", workflow.DefaultVersion, 1)
	if compressionVersion >= 1 && input.SessionID != "" && len(input.History) > 20 {
		// Check if compression is needed with rate limiting
		// 检查是否需要压缩，并做频率限制
		estimatedTokens := activities.EstimateTokens(convertHistoryForAgent(input.History))
		modelTier := determineModelTier(baseContext, "medium")

		var checkResult activities.CheckCompressionNeededResult
		err := workflow.ExecuteActivity(ctx, "CheckCompressionNeeded",
			activities.CheckCompressionNeededInput{
				SessionID:       input.SessionID,
				MessageCount:    len(input.History),
				EstimatedTokens: estimatedTokens,
				ModelTier:       modelTier,
			}).Get(ctx, &checkResult)

		if err == nil && checkResult.ShouldCompress {
			logger.Info("Triggering context compression in research workflow",
				"session_id", input.SessionID,
				"reason", checkResult.Reason,
				"message_count", len(input.History),
			)

			// Compress context via activity
			// 通过 activity 压缩 context
			var compressResult activities.CompressContextResult
			err = workflow.ExecuteActivity(ctx, activities.CompressAndStoreContext,
				activities.CompressContextInput{
					SessionID:        input.SessionID,
					History:          convertHistoryMapForCompression(input.History),
					TargetTokens:     int(float64(activities.GetModelWindowSize(modelTier)) * 0.375),
					ParentWorkflowID: input.ParentWorkflowID,
				}).Get(ctx, &compressResult)

			if err == nil && compressResult.Summary != "" && compressResult.Stored {
				logger.Info("Context compressed and stored",
					"session_id", input.SessionID,
					"summary_length", len(compressResult.Summary),
				)

				// Update compression state in session
				// 更新 session 中的压缩状态
				var updateResult activities.UpdateCompressionStateResult
				_ = workflow.ExecuteActivity(ctx, "UpdateCompressionStateActivity",
					activities.UpdateCompressionStateInput{
						SessionID:    input.SessionID,
						MessageCount: len(input.History),
					}).Get(ctx, &updateResult)
			}
		}
	}

	// Step 0: Refine/expand vague research queries
	// 步骤 0：细化/扩展模糊的研究查询
	// Emit refinement start event
	// 发送细化开始事件
	emitTaskUpdate(ctx, input, activities.StreamEventAgentThinking, "research-refiner", "Refining research query")

	// Check pause/cancel before research execution
	// 研究执行前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("init", 0, 0, "")}, err
	}

	// Check pause/cancel before query refinement
	// 查询细化前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_query_refinement"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("init", 0, 0, "")}, err
	}

	var totalTokens int
	var refineResult activities.RefineResearchQueryResult
	refinedQuery := input.Query // Default to original query 注：默认使用原始查询
	err := workflow.ExecuteActivity(ctx, constants.RefineResearchQueryActivity,
		activities.RefineResearchQueryInput{
			Query:   input.Query,
			Context: baseContext,
		}).Get(ctx, &refineResult)

	if err == nil && refineResult.RefinedQuery != "" {
		logger.Info("Query refined for research",
			"original", input.Query,
			"refined", refineResult.RefinedQuery,
			"areas", refineResult.ResearchAreas,
			"tokens_used", refineResult.TokensUsed,
		)
		refinedQuery = refineResult.RefinedQuery

		// Deep Research 2.0: Fallback - extract dimension names when research_areas is empty/generic
		// Deep Research 2.0：兜底——research_areas 为空或过于笼统时提取维度名
		// LLM may return structured research_dimensions but leave research_areas sparse
		// LLM 可能返回结构化的 research_dimensions，却把 research_areas 留得过于稀疏
		// research_areas 为空或过于笼统时的兜底：从 research_dimensions 中提取维度名。
		if len(refineResult.ResearchAreas) == 0 && len(refineResult.ResearchDimensions) > 0 {
			for _, dim := range refineResult.ResearchDimensions {
				refineResult.ResearchAreas = append(refineResult.ResearchAreas, dim.Dimension)
			}
			logger.Info("Used research_dimensions fallback for research_areas",
				"dimension_count", len(refineResult.ResearchDimensions),
			)
		}
		baseContext["research_areas"] = refineResult.ResearchAreas
		baseContext["original_query"] = input.Query
		baseContext["refinement_rationale"] = refineResult.Rationale
		baseContext["refined_query"] = refinedQuery
		if refineResult.DetectedLanguage != "" {
			// Pass target_language early; synthesis embeds language instruction.
			// 提前传入 target_language；综合阶段会嵌入语言指令。
			// Post-synthesis language validation/retry has been removed.
			// 综合后的语言校验/重试已移除。
			baseContext["target_language"] = refineResult.DetectedLanguage
		}
		if refineResult.CanonicalName != "" {
			baseContext["canonical_name"] = refineResult.CanonicalName
		}
		if len(refineResult.ExactQueries) > 0 {
			baseContext["exact_queries"] = refineResult.ExactQueries
		}
		if len(refineResult.OfficialDomains) > 0 {
			baseContext["official_domains"] = refineResult.OfficialDomains
			baseContext["official_domains_source"] = "refiner_inferred"
		}
		if len(refineResult.DisambiguationTerms) > 0 {
			baseContext["disambiguation_terms"] = refineResult.DisambiguationTerms
		}
		// Deep Research 2.0: Pass query type and structured dimensions
		// Deep Research 2.0：传入查询类型与结构化维度
		if refineResult.QueryType != "" {
			baseContext["query_type"] = refineResult.QueryType
		}
		if len(refineResult.ResearchDimensions) > 0 {
			baseContext["research_dimensions"] = refineResult.ResearchDimensions
		}
		// Deep Research 2.0: Pass localization information
		// Deep Research 2.0：传入本地化信息
		if refineResult.LocalizationNeeded {
			baseContext["localization_needed"] = refineResult.LocalizationNeeded
		}
		if len(refineResult.TargetLanguages) > 0 {
			baseContext["target_languages"] = refineResult.TargetLanguages
		}
		if len(refineResult.LocalizedNames) > 0 {
			baseContext["localized_names"] = refineResult.LocalizedNames
		}
		// HITL Phase 1: Pass structured HITL output to Decompose
		// HITL 阶段 1：把结构化 HITL 输出传给 Decompose
		// These fields are only populated when confirmed_plan exists (HITL mode)
		// 仅当 confirmed_plan 存在时（HITL 模式）这些字段才会被填充
		if len(refineResult.PriorityFocus) > 0 {
			baseContext["priority_focus"] = refineResult.PriorityFocus
		}
		if len(refineResult.SecondaryFocus) > 0 {
			baseContext["secondary_focus"] = refineResult.SecondaryFocus
		}
		if len(refineResult.SkipAreas) > 0 {
			baseContext["skip_areas"] = refineResult.SkipAreas
		}
		if refineResult.UserIntent != nil {
			baseContext["user_intent"] = refineResult.UserIntent
		}
		// Account for refinement tokens in the workflow total
		// 把细化消耗的 token 计入 workflow 总量
		totalTokens += refineResult.TokensUsed

		// Record refinement token usage for accurate cost tracking
		// 记录细化阶段的 token 用量，以便准确核算成本
		if refineResult.TokensUsed > 0 {
			inTok := 0
			// We only have a total for refinement; approximate split 60/40
			// 细化只有总量；按 60/40 近似拆分
			if refineResult.TokensUsed > 0 {
				inTok = int(float64(refineResult.TokensUsed) * 0.6)
			}
			outTok := refineResult.TokensUsed - inTok
			recCtx := opts.WithTokenRecordOptions(ctx)
			_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
				UserID:       input.UserID,
				SessionID:    input.SessionID,
				TaskID:       workflowID,
				AgentID:      "research-refiner",
				Model:        refineResult.ModelUsed,
				Provider:     refineResult.Provider,
				InputTokens:  inTok,
				OutputTokens: outTok,
				Metadata:     map[string]any{"phase": "refine"},
			}).Get(recCtx, nil)
		}

		// Emit refinement complete event with details (include canonical/entity hints for diagnostics)
		// 发送细化完成事件及详情（含 canonical/实体提示，便于诊断）
		dimCount := len(refineResult.ResearchDimensions)
		if dimCount == 0 {
			dimCount = len(refineResult.ResearchAreas) // Fallback to old field count 注：回退到旧字段计数
		}
		emitTaskUpdatePayload(ctx, input, activities.StreamEventProgress, "research-refiner",
			fmt.Sprintf("Expanded query into %d research dimensions", dimCount),
			map[string]any{
				"original_query":       input.Query,
				"refined_query":        refineResult.RefinedQuery,
				"research_areas":       refineResult.ResearchAreas,
				"rationale":            refineResult.Rationale,
				"tokens_used":          refineResult.TokensUsed,
				"model_used":           refineResult.ModelUsed,
				"provider":             refineResult.Provider,
				"canonical_name":       refineResult.CanonicalName,
				"exact_queries":        refineResult.ExactQueries,
				"official_domains":     refineResult.OfficialDomains,
				"disambiguation_terms": refineResult.DisambiguationTerms,
				// Deep Research 2.0 fields
				// Deep Research 2.0 字段
				"query_type":          refineResult.QueryType,
				"research_dimensions": refineResult.ResearchDimensions,
				"localization_needed": refineResult.LocalizationNeeded,
				"target_languages":    refineResult.TargetLanguages,
			})
	} else if err != nil {
		logger.Warn("Query refinement failed, using original query", "error", err)
		// Emit warning but continue with original query
		// 发送告警，但继续使用原始查询
		emitTaskUpdate(ctx, input, activities.StreamEventProgress, "research-refiner", "Query refinement skipped, proceeding with original query")
	}

	// Check pause/cancel after query refinement - signal may have arrived during the activity
	// 查询细化后检查暂停/取消——信号可能在 activity 执行期间到达
	if err := controlHandler.CheckPausePoint(ctx, "post_query_refinement"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("refinement", totalTokens, 0, "")}, err
	}

	// Check pause/cancel before decomposition
	// 任务分解前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_decomposition"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("refinement", totalTokens, 0, "")}, err
	}

	// Decompose needs a medium-tier model (Sonnet) for reliable domain_analysis detection.
	// Decompose 需要 medium 档的模型（Sonnet），才能可靠识别 domain_analysis。
	// Strategy's agent_model_tier (e.g. "small") applies to research agents, not decompose.
	// Decompose 需要 medium 档的 model（Sonnet）才能可靠识别 domain_analysis；
	// strategy 的 agent_model_tier（如 "small"）只作用于研究 agent，不影响 decompose。
	origTier, hadTier := baseContext["model_tier"]
	baseContext["model_tier"] = "medium"

	// Step 1: Decompose the (now refined) research query
	// 步骤 1：分解（已细化的）研究查询
	var decomp activities.DecompositionResult
	err = workflow.ExecuteActivity(ctx,
		constants.DecomposeTaskActivity,
		activities.DecompositionInput{
			Query:          refinedQuery, // Use refined query here 注：这里使用细化后的查询
			Context:        baseContext,
			AvailableTools: []string{},
		}).Get(ctx, &decomp)

	// Restore original tier for research agents
	// 为研究 agent 恢复原有档位
	if hadTier {
		baseContext["model_tier"] = origTier
	} else {
		delete(baseContext, "model_tier")
	}

	if err != nil {
		logger.Error("Task decomposition failed", "error", err)
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Failed to decompose task: %v", err),
			Metadata:     buildResearchFailureMetadata("decomposition", totalTokens, 0, ""),
		}, err
	}

	// Validate task contracts before execution
	// 执行前校验任务契约
	if err := validateTaskContracts(decomp.Subtasks, logger); err != nil {
		logger.Error("Task contract validation failed", "error", err)
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Task contract validation failed: %v", err),
			Metadata:     buildResearchFailureMetadata("decomposition", totalTokens, len(decomp.Subtasks), ""),
		}, err
	}

	// Record decomposition token usage for accurate cost tracking (if provided)
	// 记录分解阶段的 token 用量，以便准确核算成本（如有提供）
	if decomp.TokensUsed > 0 || decomp.InputTokens > 0 || decomp.OutputTokens > 0 {
		inTok := decomp.InputTokens
		outTok := decomp.OutputTokens
		if inTok == 0 && outTok == 0 && decomp.TokensUsed > 0 {
			inTok = int(float64(decomp.TokensUsed) * 0.6)
			outTok = decomp.TokensUsed - inTok
		}
		recCtx := opts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:              input.UserID,
			SessionID:           input.SessionID,
			TaskID:              workflowID,
			AgentID:             "decompose",
			Model:               decomp.ModelUsed,
			Provider:            decomp.Provider,
			InputTokens:         inTok,
			OutputTokens:        outTok,
			CacheReadTokens:     decomp.CacheReadTokens,
			CacheCreationTokens: decomp.CacheCreationTokens,
			Metadata:            map[string]any{"phase": "decompose"},
		}).Get(recCtx, nil)
	}

	// Check pause/cancel after decomposition - signal may have arrived during the activity
	// 任务分解后检查暂停/取消——信号可能在 activity 执行期间到达
	if err := controlHandler.CheckPausePoint(ctx, "post_decomposition"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("decomposition", totalTokens, 0, "")}, err
	}

	// Clean up HITL review fields from baseContext after decompose has consumed them.
	// decompose 消费完 HITL 复核字段后，从 baseContext 中清理这些字段。
	// Subagents should NOT see raw review content — decompose encodes intent into subtask descriptions.
	// 子 agent 不应看到原始复核内容——意图已由 decompose 编码进 subtask 描述。
	delete(baseContext, "confirmed_plan")
	delete(baseContext, "research_brief")
	delete(baseContext, "review_conversation")
	// HITL Phase 1: Also clean up structured HITL fields
	// HITL 阶段 1：同时清理结构化 HITL 字段
	delete(baseContext, "priority_focus")
	delete(baseContext, "secondary_focus")
	delete(baseContext, "skip_areas")
	delete(baseContext, "user_intent")

	// Check for budget configuration
	// 检查是否配置了预算
	agentMaxTokens := getBudgetMax(baseContext)
	// Apply minimum budget if configured
	// 如配置了最小预算则应用
	if minBudget, ok := baseContext["budget_agent_min"].(int); ok && minBudget > agentMaxTokens {
		agentMaxTokens = minBudget
	}
	if minBudget, ok := baseContext["budget_agent_min"].(float64); ok && int(minBudget) > agentMaxTokens {
		agentMaxTokens = int(minBudget)
	}

	modelTier := determineModelTier(baseContext, "medium")
	// Ensure baseContext always has model_tier for consistent propagation
	// 确保 baseContext 始终带有 model_tier，以便一致地向下传递
	// (parallel/hybrid execution may use non-budget path which reads from context)
	// （parallel/hybrid 执行可能走非预算路径，该路径从 context 读取）
	baseContext["model_tier"] = modelTier
	var agentResults []activities.AgentExecutionResult
	var domainPrefetchResults []activities.AgentExecutionResult
	domainPrefetchTokens := 0
	var domainAnalysisResult *DomainAnalysisResult
	var domainAnalysisFuture workflow.ChildWorkflowFuture
	domainAnalysisStarted := false
	var domainAnalysisSubtasks []activities.Subtask
	var executionSubtasks []activities.Subtask

	domainAnalysisWorkflowVersion := workflow.GetVersion(ctx, "domain_analysis_workflow_v1", workflow.DefaultVersion, 2)
	if domainAnalysisWorkflowVersion >= 2 {
		for _, subtask := range decomp.Subtasks {
			if strings.EqualFold(strings.TrimSpace(subtask.TaskType), "domain_analysis") {
				domainAnalysisSubtasks = append(domainAnalysisSubtasks, subtask)
			} else {
				executionSubtasks = append(executionSubtasks, subtask)
			}
		}
		if len(domainAnalysisSubtasks) > 0 {
			ids := make([]string, 0, len(domainAnalysisSubtasks))
			for _, st := range domainAnalysisSubtasks {
				if st.ID != "" {
					ids = append(ids, st.ID)
				}
			}
			logger.Info("Detected domain analysis system subtask(s)",
				"count", len(domainAnalysisSubtasks),
				"ids", ids,
			)
		}
		if len(executionSubtasks) != len(decomp.Subtasks) {
			decomp.Subtasks = executionSubtasks
		}
	}
	if domainAnalysisWorkflowVersion >= 1 && domainAnalysisWorkflowVersion < 2 && strings.EqualFold(refineResult.QueryType, "company") {
		enabled := true
		if v, ok := baseContext["enable_domain_analysis"].(bool); ok {
			enabled = v
		} else if v, ok := baseContext["enable_domain_prefetch"].(bool); ok {
			enabled = v
		}
		if mode, ok := baseContext["domain_analysis_mode"].(string); ok && strings.EqualFold(strings.TrimSpace(mode), "off") {
			enabled = false
		}

		if enabled {
			requestedRegions := prefetchRegionsFromContext(baseContext)
			if len(requestedRegions) == 0 {
				requestedRegions = prefetchRegionsFromQuery(input.Query)
			}
			planHints := buildDomainAnalysisPlanHints(decomp.Subtasks)
			domainAnalysisMode, _ := baseContext["domain_analysis_mode"].(string)

			var childResult DomainAnalysisResult
			err := workflow.ExecuteChildWorkflow(ctx, DomainAnalysisWorkflow, DomainAnalysisInput{
				ParentWorkflowID:     workflowID,
				CallerWorkflowID:     callerWorkflowID,
				Query:                input.Query,
				CanonicalName:        refineResult.CanonicalName,
				DisambiguationTerms:  refineResult.DisambiguationTerms,
				ResearchAreas:        refineResult.ResearchAreas,
				ResearchDimensions:   refineResult.ResearchDimensions,
				OfficialDomains:      refineResult.OfficialDomains,
				ExactQueries:         refineResult.ExactQueries,
				TargetLanguages:      refineResult.TargetLanguages,
				LocalizationNeeded:   refineResult.LocalizationNeeded,
				PrefetchSubpageLimit: refineResult.PrefetchSubpageLimit,
				RequestedRegions:     requestedRegions,
				PlanHints:            planHints,
				SubtaskDescription:   "", // v1 path has no decompose subtask 注：v1 路径没有 decompose 子任务
				Context:              baseContext,
				UserID:               input.UserID,
				SessionID:            input.SessionID,
				History:              input.History,
				DomainAnalysisMode:   domainAnalysisMode,
			}).Get(ctx, &childResult)

			if err != nil {
				logger.Warn("Domain analysis workflow failed", "error", err)
			} else {
				domainAnalysisResult = &childResult
				if len(childResult.OfficialDomainsSelected) > 0 {
					domains := extractDomainsFromCoverage(childResult.OfficialDomainsSelected)
					if len(domains) > 0 {
						baseContext["official_domains"] = domains
						baseContext["official_domains_source"] = "domain_analysis"
					}
				}
			}
		}
	}

	if domainAnalysisWorkflowVersion == workflow.DefaultVersion {
		// Step 2.1: Programmatic domain prefetch for company research
		// 步骤 2.1：为公司研究做程序化域名预取
		// This uses refinement hints (canonical_name, official_domains) to force web_fetch
		// 利用细化提示（canonical_name、official_domains）在自由探索前
		// on likely official sites before freeform exploration.
		// 强制对可能的官方站点执行 web_fetch。
		domainPrefetchVersion := workflow.GetVersion(ctx, "domain_prefetch_v1", workflow.DefaultVersion, 1)
		if domainPrefetchVersion >= 1 && (strings.EqualFold(refineResult.QueryType, "company") || strings.EqualFold(refineResult.QueryType, "comparative")) {
			enabled := true
			if v, ok := baseContext["enable_domain_prefetch"].(bool); ok {
				enabled = v
			}
			if enabled {
				// Use LLM-determined target_languages (based on company region) instead of query language.
				// 使用 LLM 判定的 target_languages（基于公司地区），而非查询语言。
				// This ensures Chinese sources (tianyancha, etc.) are only used for Chinese companies.
				// 这样中文来源（天眼查等）只会用于中国公司。
				regionCode := extractRegionCodeFromTargetLanguages(refineResult.TargetLanguages)

				prefetchDiscoverOnlyVersion := workflow.GetVersion(ctx, "domain_prefetch_discover_only_v1", workflow.DefaultVersion, 3)
				hybridPrefetchEnabled := prefetchDiscoverOnlyVersion == 2

				var urls []string
				if prefetchDiscoverOnlyVersion >= 1 {
					domainDiscoveryVersion := workflow.GetVersion(ctx, "domain_discovery_search_first_v1", workflow.DefaultVersion, 1)
					if domainDiscoveryVersion >= 1 && strings.TrimSpace(refineResult.CanonicalName) != "" {
						originRegion := originPrefetchRegionFromTargetLanguages(refineResult.TargetLanguages)

						requestedRegions := prefetchRegionsFromContext(baseContext)
						if len(requestedRegions) == 0 {
							requestedRegions = prefetchRegionsFromQuery(input.Query)
						}

						// Discover-only mode (v1) / Hybrid mode (v2): reset any refinement-provided domains.
						// discover-only 模式（v1）/ hybrid 模式（v2）：重置细化阶段提供的域名。
						delete(baseContext, "official_domains")
						delete(baseContext, "official_domains_source")

						// Build DomainAnalysisIntent for multinational strategy (domain_analysis_v1)
						// 为公司研究构造 DomainAnalysisIntent，用于跨国策略（domain_analysis_v1）
						domainAnalysisVersion := workflow.GetVersion(ctx, "domain_analysis_v1", workflow.DefaultVersion, 1)
						intent := BuildDomainAnalysisIntent(
							input.Query,
							refineResult.ResearchAreas,
							requestedRegions,
							refineResult.TargetLanguages,
							refineResult.LocalizationNeeded,
						)

						// maxPrefetch based on multinational strategy
						// maxPrefetch 取决于跨国策略
						// - Non-multinational: 5 (global + origin only)
						// - 非跨国：5（仅 global + origin）
						// - Multinational: 8 (global + eu + cn + jp + topic)
						// - 跨国：8（global + eu + cn + jp + topic）
						// - With explicit regions: 5
						// - 显式指定区域：5
						// - Max limit: 15 (same as web_subpage_fetch MAX_LIMIT)
						// - 上限：15（与 web_subpage_fetch MAX_LIMIT 相同）
						maxPrefetch := 8
						if domainAnalysisVersion >= 1 {
							if len(requestedRegions) > 0 {
								maxPrefetch = 5
							} else if !intent.MultinationalDefault {
								maxPrefetch = 5 // Non-multinational: fewer prefetch slots 注：非跨国：预取名额更少
							}
						} else if len(requestedRegions) > 0 {
							maxPrefetch = 5
						}
						if v, ok := baseContext["domain_prefetch_max_urls"]; ok {
							switch t := v.(type) {
							case int:
								maxPrefetch = t
							case float64:
								maxPrefetch = int(t)
							}
						}
						if maxPrefetch < 1 {
							maxPrefetch = 1
						}
						if maxPrefetch > 15 { // Same as web_subpage_fetch MAX_LIMIT 注：与 web_subpage_fetch MAX_LIMIT 相同
							maxPrefetch = 15
						}

						// v3 (discover-only strict) reduces domain_discovery from 4 fixed region searches to:
						// v3（discover-only 严格版）把 domain_discovery 从 4 个固定区域搜索缩减为：
						// - primary (origin-first when available, else global)
						// - primary（有来源时优先来源语言，否则 global）
						// - optional global fallback (at most 2 searches total)
						// - 可选的 global 兜底（最多共 2 次搜索）
						// - topic-based searches (ir, docs, careers, product) are preserved from buildDomainDiscoverySearches
						// - 基于主题的搜索（ir、docs、careers、product）仍由 buildDomainDiscoverySearches 保留
						searches := buildDomainDiscoverySearches(refineResult.CanonicalName, refineResult.DisambiguationTerms, originRegion, requestedRegions, refineResult.ResearchAreas, refineResult.OfficialDomains)
						globalQuery := buildCompanyDomainDiscoverySearchQuery(refineResult.CanonicalName, refineResult.DisambiguationTerms, "")
						if prefetchDiscoverOnlyVersion >= 3 && len(requestedRegions) == 0 {
							originLang := originRegionToDiscoveryLanguageCode(originRegion)
							primaryQuery := buildCompanyDomainDiscoverySearchQuery(refineResult.CanonicalName, refineResult.DisambiguationTerms, originLang)
							if strings.TrimSpace(primaryQuery) == "" {
								primaryQuery = globalQuery
							}
							// Extract topic-based searches (ir, docs, careers, product_*) to preserve them
							// 提取基于主题的搜索（ir、docs、careers、product_*），以便保留它们
							var topicSearches []domainDiscoverySearch
							for _, s := range searches {
								if s.Key == "ir" || s.Key == "docs" || s.Key == "careers" || strings.HasPrefix(s.Key, "product_") {
									topicSearches = append(topicSearches, s)
								}
							}
							if strings.TrimSpace(primaryQuery) != "" {
								// Start with primary search, then append topic searches
								// 以 primary 搜索开头，再追加主题搜索
								searches = []domainDiscoverySearch{{Key: "primary", Query: primaryQuery}}
								searches = append(searches, topicSearches...)
							}
						}

						// domain_analysis_v1: Apply multinational filtering when no explicit regions
						// domain_analysis_v1：无显式区域时应用跨国过滤
						// Non-multinational companies only search global + origin + topic searches
						// 非跨国公司只搜索 global + origin + 主题搜索
						if domainAnalysisVersion >= 1 && len(requestedRegions) == 0 && !intent.MultinationalDefault {
							var filteredSearches []domainDiscoverySearch
							for _, s := range searches {
								// Keep topic searches (ir, docs, careers, product_*) and primary/global
								// 保留主题搜索（ir、docs、careers、product_*）以及 primary/global
								if s.Key == "ir" || s.Key == "docs" || s.Key == "careers" ||
									strings.HasPrefix(s.Key, "product_") ||
									s.Key == "primary" || s.Key == "global" ||
									s.Key == originRegion {
									filteredSearches = append(filteredSearches, s)
								}
							}
							if len(filteredSearches) > 0 {
								searches = filteredSearches
								logger.Info("Domain analysis: non-multinational filtering applied",
									"original_count", len(searches),
									"filtered_count", len(filteredSearches),
									"multinational", intent.MultinationalDefault,
								)
							}
						}
						discoveredBySearch := make(map[string][]string)
						var allDiscovered []string
						seenAll := make(map[string]bool)

						searchDomainsFromResults := domainsFromWebSearchToolExecutionsAll
						if prefetchDiscoverOnlyVersion >= 2 {
							searchDomainsFromResults = func(toolExecs []activities.ToolExecution) []string {
								return domainsFromWebSearchToolExecutionsAllV2(toolExecs, refineResult.CanonicalName)
							}
						}

						discoveryContext := map[string]any{
							"user_id":    input.UserID,
							"session_id": input.SessionID,
							"model_tier": "small",
							"role":       "domain_discovery", // Use specialized preset 注：使用专用 preset
							"response_format": map[string]any{
								"type": "json_object",
							},
						}
						if input.ParentWorkflowID != "" {
							discoveryContext["parent_workflow_id"] = input.ParentWorkflowID
						}

						// === BATCH DOMAIN DISCOVERY (single LLM call) ===
						// === 批量域名发现（单次 LLM 调用）===
						// Collect all search queries and execute them in a single agent call
						// 收集所有搜索查询，在一次 agent 调用中执行
						// This reduces N LLM calls to 1, significantly improving efficiency
						// 把所有搜索查询收进一次 agent 调用，将 N 次 LLM 调用压缩为 1 次，显著提升效率。
						var allQueries []string
						for _, s := range searches {
							allQueries = append(allQueries, s.Query)
						}

						// Build batch discovery prompt with all queries
						// 用所有查询构造批量发现 prompt
						var discoveryResult activities.AgentExecutionResult
						var discoveryQuery strings.Builder
						fmt.Fprintf(&discoveryQuery,
							"Find official domains for %q.\n\n"+
								"STEP 1: Execute web_search for each query:\n",
							refineResult.CanonicalName,
						)
						for _, q := range allQueries {
							fmt.Fprintf(&discoveryQuery, "- %s\n", q)
						}
						discoveryQuery.WriteString("\n" +
							"STEP 2: After ALL searches complete, respond with ONLY this JSON:\n" +
							"{\"domains\":[\"domain1.com\",\"domain2.com\"]}\n\n" +
							"RULES:\n" +
							"- Include: corporate sites, IR sites (abc.xyz), parent company sites\n" +
							"- Exclude: login/accounts, store, support, third-party (wikipedia, linkedin)\n" +
							"- Strip www prefix, no paths\n" +
							"- Max 10 domains\n\n" +
							"CRITICAL: Your response must be ONLY the JSON object, nothing else.\n")

						// Add research focus using original query (not ResearchAreas)
						// 用原始查询（而非 ResearchAreas）补充研究焦点
						// ResearchAreas come from Refiner's research_dimensions, which may not match user intent
						// ResearchAreas 来自 Refiner 的 research_dimensions，可能与用户意图不符
						fmt.Fprintf(&discoveryQuery, "\n=== RESEARCH FOCUS ===\n"+
							"Original query: %s\n"+
							"Find official domains most relevant to answering this query.\n",
							input.Query)

						// Use ResearchAreas only as category hints for domain prioritization
						// ResearchAreas 仅作为域名优先级排序的类别提示
						if len(refineResult.ResearchAreas) > 0 {
							focusCategories := classifyFocusCategories(refineResult.ResearchAreas)
							if len(focusCategories) > 0 {
								fmt.Fprintf(&discoveryQuery, "Domain type hints: %s\n",
									strings.Join(focusCategories, ", "))
							}
						}

						// Single agent call with first query as tool_parameters hint
						// 单次 agent 调用，用首个查询作为 tool_parameters 提示
						// Agent will execute multiple web_search calls based on the prompt
						// agent 会按 prompt 执行多次 web_search 调用
						discoveryErr := workflow.ExecuteActivity(ctx,
							"ExecuteAgent",
							activities.AgentExecutionInput{
								Query:     discoveryQuery.String(),
								AgentID:   "domain_discovery",
								Context:   discoveryContext,
								Mode:      "standard",
								SessionID: input.SessionID,
								UserID:    input.UserID,
								History:   convertHistoryForAgent(input.History),
								SuggestedTools: []string{
									"web_search",
								},
								ToolParameters: map[string]any{
									"tool":        "web_search",
									"query":       allQueries[0], // Primary query as hint 注：用主查询作为提示
									"max_results": 20,
								},
								ParentWorkflowID: input.ParentWorkflowID,
							},
						).Get(ctx, &discoveryResult)

						if discoveryErr != nil || !discoveryResult.Success {
							logger.Warn("Domain discovery batch search failed",
								"canonical_name", refineResult.CanonicalName,
								"queries", allQueries,
								"error", discoveryErr,
								"agent_error", discoveryResult.Error,
							)
						} else {
							// Persist domain_discovery execution for observability
							// 持久化 domain_discovery 执行记录，以便观测
							// Use workflowID (parent task ID) for consistent DB querying across discovery/prefetch
							// 用 workflowID（父任务 ID）以便 discovery/prefetch 的 DB 查询保持一致
							// Also store child_workflow_id for debugging/traceability
							// 同时存 child_workflow_id 以便调试/溯源
							childWorkflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
							persistAgentExecutionLocalWithMeta(
								ctx,
								workflowID, // Use parent task workflow ID for unified DB queries 注：用父任务 workflow ID 以便统一的 DB 查询
								"domain_discovery",
								fmt.Sprintf("Domain discovery: %s (batch, %d queries)", refineResult.CanonicalName, len(allQueries)),
								discoveryResult,
								map[string]any{
									"phase":             "domain_discovery",
									"batch_mode":        true,
									"query_count":       len(allQueries),
									"queries":           allQueries,
									"child_workflow_id": childWorkflowID, // Dual-write for traceability 注：双写以便溯源
								},
							)

							searchDomainsAll := searchDomainsFromResults(discoveryResult.ToolExecutions)
							llmDomains := domainsFromDiscoveryResponse(discoveryResult.Response)
							if prefetchDiscoverOnlyVersion >= 3 {
								llmDomains = domainsFromDiscoveryResponseV2(discoveryResult.Response)
							}

							if len(llmDomains) == 0 && prefetchDiscoverOnlyVersion >= 3 {
								logger.Warn("Domain discovery returned no parseable JSON domains",
									"canonical_name", refineResult.CanonicalName,
									"queries", allQueries,
								)
							} else {
								searchSet := make(map[string]bool)
								for _, d := range searchDomainsAll {
									searchSet[d] = true
								}

								isGrounded := func(llmDomain string) bool {
									if searchSet[llmDomain] {
										return true
									}
									suffix := "." + llmDomain
									for sd := range searchSet {
										if strings.HasSuffix(sd, suffix) {
											return true
										}
									}
									return false
								}

								// Extract grounded domains
								// 提取有搜索依据（grounded）的域名
								for _, d := range llmDomains {
									if isGrounded(d) && !seenAll[d] {
										seenAll[d] = true
										allDiscovered = append(allDiscovered, d)
									}
								}

								// Fallback to search domains if LLM domains not grounded (pre-v3 behavior)
								// LLM 给出的域名无依据时回退到搜索域名（v3 之前的行为）
								if len(allDiscovered) == 0 && prefetchDiscoverOnlyVersion < 3 {
									for _, d := range searchDomainsAll {
										if !seenAll[d] {
											seenAll[d] = true
											allDiscovered = append(allDiscovered, d)
										}
									}
								}

								// Store in discoveredBySearch for compatibility
								// 也存入 discoveredBySearch 以兼容旧逻辑
								if len(allDiscovered) > 0 {
									discoveredBySearch["batch"] = allDiscovered
								}
							}

							// Record token usage
							// 记录 token 用量
							if discoveryResult.TokensUsed > 0 || discoveryResult.InputTokens > 0 || discoveryResult.OutputTokens > 0 {
								inTok := discoveryResult.InputTokens
								outTok := discoveryResult.OutputTokens
								recCtx := opts.WithTokenRecordOptions(ctx)
								_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
									UserID:              input.UserID,
									SessionID:           input.SessionID,
									TaskID:              workflowID,
									AgentID:             "domain_discovery",
									Model:               discoveryResult.ModelUsed,
									Provider:            discoveryResult.Provider,
									InputTokens:         inTok,
									OutputTokens:        outTok,
									CacheReadTokens:     discoveryResult.CacheReadTokens,
									CacheCreationTokens: discoveryResult.CacheCreationTokens,
									Metadata:            map[string]any{"phase": "domain_discovery", "batch_mode": true, "query_count": len(allQueries)},
								}).Get(recCtx, nil)
							}
						}

						if len(allDiscovered) > 0 {
							candidateDomains := allDiscovered
							officialDomainsSource := "search_first_discovered_only_v1"
							if prefetchDiscoverOnlyVersion >= 3 {
								officialDomainsSource = "search_first_discovered_only_v3"
							}
							prefetchStrategy := "discover-only"
							var hybridAdded []string
							if hybridPrefetchEnabled {
								hybridAdded = vettedHybridRefinementDomains(allDiscovered, refineResult.OfficialDomains, refineResult.CanonicalName)
								candidateDomains = append(append([]string{}, allDiscovered...), hybridAdded...)
								candidateDomains = sortDomainsForHybridPrefetch(candidateDomains, refineResult.CanonicalName)
								officialDomainsSource = "search_first_hybrid_v2"
								prefetchStrategy = "hybrid"
							}

							// Scope official_domains to requested regions when user explicitly requested a region.
							// 用户显式指定区域时，把 official_domains 限定在所选区域。
							scoped := candidateDomains
							if len(requestedRegions) > 0 {
								scoped = selectDomainsForPrefetch(candidateDomains, requestedRegions, originRegion, 100)
								if len(scoped) == 0 {
									scoped = nil
								}
							}
							if len(scoped) > 0 {
								baseContext["official_domains"] = scoped
								baseContext["official_domains_source"] = officialDomainsSource
							}

							// Use focus-aware selection in domain_analysis_v1
							// domain_analysis_v1 中使用焦点感知的选择
							var prefetchDomains []string
							if domainAnalysisVersion >= 1 {
								prefetchDomains = selectDomainsForPrefetchWithFocus(candidateDomains, requestedRegions, originRegion, maxPrefetch, intent.FocusCategories)
							} else {
								prefetchDomains = selectDomainsForPrefetch(candidateDomains, requestedRegions, originRegion, maxPrefetch)
							}
							urls = buildPrefetchURLsFromDomains(prefetchDomains)

							logger.Info("Domain discovery (prefetch) completed",
								"canonical_name", refineResult.CanonicalName,
								"origin_region", originRegion,
								"requested_regions", requestedRegions,
								"prefetch_strategy", prefetchStrategy,
								"searches", searches,
								"discovered_by_search", discoveredBySearch,
								"all_discovered_count", len(allDiscovered),
								"hybrid_added_domains", hybridAdded,
								"candidate_domains_count", len(candidateDomains),
								"prefetch_domains", prefetchDomains,
								"prefetch_urls", urls,
							)
						} else {
							logger.Info("Domain discovery returned no domains; skipping discover-only prefetch",
								"canonical_name", refineResult.CanonicalName,
								"origin_region", originRegion,
								"requested_regions", requestedRegions,
								"searches", searches,
							)
						}
					}
				} else {
					officialDomainsForPrefetch := refineResult.OfficialDomains
					domainDiscoveryVersion := workflow.GetVersion(ctx, "domain_discovery_search_first_v1", workflow.DefaultVersion, 1)
					if domainDiscoveryVersion >= 1 && strings.TrimSpace(refineResult.CanonicalName) != "" {
						searchQuery := buildCompanyDomainDiscoverySearchQuery(refineResult.CanonicalName, refineResult.DisambiguationTerms, regionCode)
						if searchQuery != "" {
							discoveryContext := map[string]any{
								"user_id":    input.UserID,
								"session_id": input.SessionID,
								"model_tier": "small",
								"role":       "domain_discovery", // Use specialized preset 注：使用专用 preset
								"response_format": map[string]any{
									"type": "json_object",
								},
							}
							if input.ParentWorkflowID != "" {
								discoveryContext["parent_workflow_id"] = input.ParentWorkflowID
							}

							var discoveryResult activities.AgentExecutionResult
							// Build discovery query with detailed rules and research focus
							// 构造带详细规则与研究焦点的发现查询
							discoveryQuery2 := fmt.Sprintf(
								"Extract the official website domains for the company %q.\n\n"+
									"Use ONLY domains that appear in the provided web_search results (do not guess or fabricate).\n"+
									"Return JSON ONLY with this schema (no markdown, no prose):\n"+
									"{\"domains\":[\"example.com\",\"docs.example.com\",...]}\n\n"+
									"=== DOMAIN SELECTION PRIORITY (highest to lowest) ===\n"+
									"1. Corporate main site (company.com, about.company.com)\n"+
									"2. Investor relations / IR site (ir.company.com, investors.company.com)\n"+
									"3. Parent company site (if subsidiary, e.g., abc.xyz for Google/Alphabet)\n"+
									"4. Documentation / Developer hub (docs.company.com, developer.company.com)\n"+
									"5. Regional main sites ONLY if highly relevant (jp.company.com) - max 1-2\n\n"+
									"=== MANDATORY EXCLUSIONS (never include these) ===\n"+
									"- Login/account pages: accounts.*, login.*, signin.*, auth.*\n"+
									"- E-commerce/store: store.*, shop.*, buy.*\n"+
									"- News aggregators: news.* (unless it's the company's official newsroom)\n"+
									"- Productivity tools: sites.*, drive.*, calendar.*, mail.*\n"+
									"- Support/help pages: support.*, help.* (low information density)\n"+
									"- Third-party: wikipedia, linkedin, crunchbase, github.io, *.mintlify.app\n\n"+
									"=== OUTPUT RULES ===\n"+
									"- Strip \"www.\" prefix\n"+
									"- No paths (company.com/about → company.com)\n"+
									"- Return at most 5 domains, prioritizing information-rich sites\n"+
									"- If none found, return {\"domains\":[]}\n",
								refineResult.CanonicalName,
							)
							// Add research focus hint if available
							// 如有研究焦点提示则补充
							if len(refineResult.ResearchAreas) > 0 {
								focusAreas := refineResult.ResearchAreas
								if len(focusAreas) > maxResearchFocusAreas {
									focusAreas = focusAreas[:maxResearchFocusAreas]
								}
								discoveryQuery2 += fmt.Sprintf("\n=== RESEARCH FOCUS HINT ===\n"+
									"This research focuses on: %s\n"+
									"Prioritize domains containing information about these topics (e.g., IR sites for financial research).\n",
									strings.Join(focusAreas, ", "))
							}

							discoveryErr := workflow.ExecuteActivity(ctx,
								"ExecuteAgent",
								activities.AgentExecutionInput{
									Query:     discoveryQuery2,
									AgentID:   "domain_discovery",
									Context:   discoveryContext,
									Mode:      "standard",
									SessionID: input.SessionID,
									UserID:    input.UserID,
									History:   convertHistoryForAgent(input.History),
									SuggestedTools: []string{
										"web_search",
									},
									ToolParameters: map[string]any{
										"tool":        "web_search",
										"query":       searchQuery,
										"max_results": 20,
									},
									ParentWorkflowID: input.ParentWorkflowID,
								},
							).Get(ctx, &discoveryResult)

							if discoveryErr != nil || !discoveryResult.Success {
								logger.Warn("Domain discovery search-first failed; falling back to refinement domains",
									"canonical_name", refineResult.CanonicalName,
									"search_query", searchQuery,
									"error", discoveryErr,
									"agent_error", discoveryResult.Error,
								)
							} else {
								// Persist domain_discovery (global fallback) for observability
								// 持久化 domain_discovery（global 兜底）以便观测
								globalDiscoveryWfID := workflow.GetInfo(ctx).WorkflowExecution.ID
								persistAgentExecutionLocalWithMeta(
									ctx,
									globalDiscoveryWfID,
									"domain_discovery",
									fmt.Sprintf("Domain discovery (global_fallback): %s", refineResult.CanonicalName),
									discoveryResult,
									map[string]any{
										"phase":      "domain_discovery",
										"search_key": "global_fallback",
										"query":      searchQuery,
									},
								)

								searchDomains := domainsFromWebSearchToolExecutions(discoveryResult.ToolExecutions, refineResult.CanonicalName)
								llmDomains := domainsFromDiscoveryResponse(discoveryResult.Response)

								// Prefer LLM-selected domains, but only keep domains that are grounded in search result URLs.
								// 优先采用 LLM 选中的域名，但只保留能在搜索结果 URL 中找到依据的域名。
								// Build a set of search domains for exact matching
								// 构造搜索域名集合以便精确匹配
								searchSet := make(map[string]bool)
								for _, d := range searchDomains {
									searchSet[d] = true
								}

								// Helper to check if a domain is grounded:
								// 辅助函数：判断域名是否有依据：
								// - Exact match (llm said "jp.example.com", search has "jp.example.com")
								// - 完全匹配（llm 给出 "jp.example.com"，搜索中也有）
								// - LLM said root domain, search has subdomain (llm said "example.com", search has "jp.example.com")
								// - LLM 给出根域名、搜索中有子域名（llm 给 "example.com"，搜索有 "jp.example.com"）
								isGrounded := func(llmDomain string) bool {
									if searchSet[llmDomain] {
										return true
									}
									// Check if any search domain is a subdomain of llmDomain
									// 检查是否有搜索域名是 llmDomain 的子域名
									suffix := "." + llmDomain
									for sd := range searchSet {
										if strings.HasSuffix(sd, suffix) {
											return true
										}
									}
									return false
								}

								var discovered []string
								seenDiscovered := make(map[string]bool)

								// Only use LLM-selected domains (no subdomain expansion)
								// 只采用 LLM 选中的域名（不做子域扩展）
								// Subdomain expansion was adding back unwanted domains like accounts.*, store.*
								// 只采用 LLM 选中的域名，不做子域扩展——扩展会把 accounts.*、store.* 等无用域带回来。
								for _, d := range llmDomains {
									if isGrounded(d) && !seenDiscovered[d] {
										seenDiscovered[d] = true
										discovered = append(discovered, d)
									}
								}

								// Fallback: if nothing discovered, use all search domains
								// 兜底：若未发现任何域名，则使用全部搜索域名
								if len(discovered) == 0 {
									discovered = searchDomains
								}

								if len(discovered) > 0 {
									// Merge: search-discovered + refinement-guessed domains (dedup)
									// 合并：搜索发现的域名 + 细化推测的域名（去重）
									merged := discovered
									seen := make(map[string]bool)
									for _, d := range discovered {
										seen[d] = true
									}
									for _, d := range refineResult.OfficialDomains {
										if !seen[d] {
											merged = append(merged, d)
											seen[d] = true
										}
									}

									baseContext["official_domains"] = merged
									baseContext["official_domains_source"] = "search_first_merged"

									officialDomainsForPrefetch = merged

									// Multinational companies need more prefetch slots
									// 跨国公司需要更多预取名额
									maxPrefetch := 5
									if len(refineResult.TargetLanguages) > 1 {
										maxPrefetch = 8
									}
									if len(officialDomainsForPrefetch) > maxPrefetch {
										officialDomainsForPrefetch = officialDomainsForPrefetch[:maxPrefetch]
									}

									logger.Info("Domain discovery search-first succeeded",
										"canonical_name", refineResult.CanonicalName,
										"search_query", searchQuery,
										"discovered", discovered,
										"merged", merged,
										"refinement_domains", refineResult.OfficialDomains,
									)
								}

								if discoveryResult.TokensUsed > 0 || discoveryResult.InputTokens > 0 || discoveryResult.OutputTokens > 0 {
									inTok := discoveryResult.InputTokens
									outTok := discoveryResult.OutputTokens
									recCtx := opts.WithTokenRecordOptions(ctx)
									_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
										UserID:              input.UserID,
										SessionID:           input.SessionID,
										TaskID:              workflowID,
										AgentID:             "domain_discovery",
										Model:               discoveryResult.ModelUsed,
										Provider:            discoveryResult.Provider,
										InputTokens:         inTok,
										OutputTokens:        outTok,
										CacheReadTokens:     discoveryResult.CacheReadTokens,
										CacheCreationTokens: discoveryResult.CacheCreationTokens,
										Metadata:            map[string]any{"phase": "domain_discovery"},
									}).Get(recCtx, nil)
								}
							}
						}
					}

					urls = buildCompanyPrefetchURLsWithLocale(refineResult.CanonicalName, officialDomainsForPrefetch, regionCode)
					if len(urls) > 0 {
						// Cap prefetch attempts to avoid excessive tool usage (official + top aggregators)
						// 限制预取次数，避免工具调用过多（官方站 + 头部聚合站）
						if len(urls) > 5 {
							urls = urls[:5]
						}
					}
				}

				if len(urls) > 0 {

					var failedDomains []string

					// Calculate base subpage limit from refiner recommendation (default 15, clamp 10-20)
					// 按 refiner 建议计算基础子页上限（默认 15，限制在 10-20）
					baseSubpageLimit := 15
					if refineResult.PrefetchSubpageLimit > 0 {
						baseSubpageLimit = min(max(refineResult.PrefetchSubpageLimit, 10), 20)
					}

					// Helper to check if domain is primary (matches canonical name or is official)
					// 辅助函数：判断域名是否为主域名（匹配规范名或属官方域名）
					isPrimaryDomain := func(urlStr string) bool {
						host := strings.ToLower(urlStr)
						// Extract hostname
						// 提取主机名
						if idx := strings.Index(host, "://"); idx != -1 {
							host = host[idx+3:]
						}
						if idx := strings.Index(host, "/"); idx != -1 {
							host = host[:idx]
						}
						host = strings.TrimPrefix(host, "www.")

						// Check against canonical name (e.g., "ExampleCorp" -> "examplecorp")
						// 与规范名比对（例如 "ExampleCorp" -> "examplecorp"）
						canonicalLower := strings.ToLower(refineResult.CanonicalName)
						if canonicalLower != "" && strings.Contains(host, canonicalLower) {
							return true
						}

						// Check if in official_domains list
						// 检查是否在 official_domains 列表中
						for _, od := range refineResult.OfficialDomains {
							odLower := strings.ToLower(od)
							if strings.Contains(host, odLower) || strings.Contains(odLower, host) {
								return true
							}
						}
						return false
					}

					logger.Info("Running domain prefetch for company research",
						"urls", urls,
						"base_subpage_limit", baseSubpageLimit,
					)

					type prefetchPayload struct {
						Result activities.AgentExecutionResult
						URL    string
						Index  int
						Err    error
					}

					prefetchChan := workflow.NewChannel(ctx)

					for i, u := range urls {
						url := u
						idx := i + 1

						// Dynamic limit: primary domains get base limit, secondary domains get reduced
						// 动态上限：主域名用基础上限，次级域名则下调
						domainLimit := baseSubpageLimit
						if !isPrimaryDomain(url) {
							// Secondary/product domains: reduce by 5, minimum 8
							// 次级/产品域名：减 5，最少 8
							domainLimit = max(baseSubpageLimit-5, 8)
						}

						workflow.Go(ctx, func(gctx workflow.Context) {
							prefetchContext := make(map[string]any)
							maps.Copy(prefetchContext, baseContext)
							prefetchContext["research_mode"] = "prefetch"
							prefetchContext["prefetch_url"] = url
							prefetchContext["role"] = "domain_prefetch" // Use specialized preset 注：使用专用 preset
							prefetchContext["model_tier"] = "small"     // Downgrade from medium to save cost 注：从 medium 降为 small 以节省成本

							// Use station names with offset for prefetch agents
							// 预取 agent 使用带偏移的站点名
							prefetchAgentName := agents.GetAgentName(workflowID, agents.IdxDomainPrefetchBase+idx)
							var prefetchResult activities.AgentExecutionResult

							// Build query with research focus for better RELEVANCE judgment
							// 构造带研究焦点的查询，以便更好地判断相关性（RELEVANCE）
							prefetchQuery := fmt.Sprintf("Use web_subpage_fetch on %s to extract company information.", url)
							if len(refineResult.ResearchAreas) > 0 {
								prefetchQuery += fmt.Sprintf("\n\nResearch focus: %s", strings.Join(refineResult.ResearchAreas, ", "))
							}

							err := workflow.ExecuteActivity(gctx,
								"ExecuteAgent",
								activities.AgentExecutionInput{
									Query:          prefetchQuery,
									AgentID:        prefetchAgentName,
									Context:        prefetchContext,
									Mode:           "standard",
									SessionID:      input.SessionID,
									UserID:         input.UserID,
									History:        convertHistoryForAgent(input.History),
									SuggestedTools: []string{"web_subpage_fetch"},
									ToolParameters: map[string]any{
										"tool":            "web_subpage_fetch",
										"url":             url,
										"limit":           domainLimit,
										"target_keywords": "about team leadership company founders management products services",
										"target_paths": []string{
											"/about", "/about-us", "/company",
											"/ir", "/investor-relations", "/investors",
											"/team", "/leadership", "/management",
											"/products", "/services",
										},
									},
									ParentWorkflowID: input.ParentWorkflowID,
								}).Get(gctx, &prefetchResult)

							prefetchChan.Send(gctx, prefetchPayload{
								Result: prefetchResult,
								URL:    url,
								Index:  idx,
								Err:    err,
							})
						})
					}

					// Track failed domains to prevent re-fetching in subsequent iterations
					// 记录失败域名，避免后续迭代重复抓取
					for range urls {
						var payload prefetchPayload
						prefetchChan.Receive(ctx, &payload)

						if payload.Err != nil {
							logger.Warn("Domain prefetch failed", "url", payload.URL, "error", payload.Err)
							failedDomains = append(failedDomains, payload.URL)
							continue
						}
						if !payload.Result.Success {
							logger.Info("Domain prefetch completed without success", "url", payload.URL)
							failedDomains = append(failedDomains, payload.URL)
							continue
						}

						domainPrefetchResults = append(domainPrefetchResults, payload.Result)
						domainPrefetchTokens += payload.Result.TokensUsed

						// Persist agent and tool execution records with metadata for observability
						// 持久化 agent 与工具执行记录及元数据，以便观测
						prefetchAgentName := agents.GetAgentName(workflowID, agents.IdxDomainPrefetchBase+payload.Index)
						prefetchURLRole := classifyDomainRole(payload.URL) // Re-compute for metadata (deterministic) 注：为元数据重新计算（确定性）
						persistAgentExecutionLocalWithMeta(
							ctx,
							workflowID,
							prefetchAgentName,
							fmt.Sprintf("Domain prefetch: %s", payload.URL),
							payload.Result,
							map[string]any{
								"phase":    "domain_prefetch",
								"url":      payload.URL,
								"url_role": prefetchURLRole,
								"index":    payload.Index,
							},
						)

						if payload.Result.TokensUsed > 0 || payload.Result.InputTokens > 0 || payload.Result.OutputTokens > 0 {
							inTok := payload.Result.InputTokens
							outTok := payload.Result.OutputTokens
							recCtx := opts.WithTokenRecordOptions(ctx)
							_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
								UserID:       input.UserID,
								SessionID:    input.SessionID,
								TaskID:       workflowID,
								AgentID:      prefetchAgentName,
								Model:        payload.Result.ModelUsed,
								Provider:     payload.Result.Provider,
								InputTokens:  inTok,
								OutputTokens: outTok,
								Metadata:     map[string]any{"phase": "domain_prefetch"},
							}).Get(recCtx, nil)
						}
					}

					// Store failed domains in context to prevent re-fetching in subsequent iterations
					// 把失败域名写入 context，避免后续迭代重复抓取
					if len(failedDomains) > 0 {
						baseContext["failed_domains"] = failedDomains
						logger.Info("Tracked failed domains for skip in future fetches",
							"failed_count", len(failedDomains),
							"failed_urls", failedDomains,
						)
					}
				}
			}
		}

	}

	// Check pause/cancel before agent execution phase
	// agent 执行阶段前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_agent_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("pre_execution", totalTokens, 0, "")}, err
	}

	if domainAnalysisWorkflowVersion >= 2 && strings.EqualFold(refineResult.QueryType, "company") {
		enabled := true
		if v, ok := baseContext["enable_domain_analysis"].(bool); ok {
			enabled = v
		} else if v, ok := baseContext["enable_domain_prefetch"].(bool); ok {
			enabled = v
		}
		if mode, ok := baseContext["domain_analysis_mode"].(string); ok && strings.EqualFold(strings.TrimSpace(mode), "off") {
			enabled = false
		}

		if enabled {
			if len(domainAnalysisSubtasks) == 0 {
				logger.Warn("Domain analysis subtask missing; skipping domain analysis")
			} else {
				if len(domainAnalysisSubtasks) > 1 {
					logger.Warn("Multiple domain analysis subtasks found; only the first will run",
						"count", len(domainAnalysisSubtasks),
					)
				}

				requestedRegions := prefetchRegionsFromContext(baseContext)
				if len(requestedRegions) == 0 {
					requestedRegions = prefetchRegionsFromQuery(input.Query)
				}
				planHints := buildDomainAnalysisPlanHints(executionSubtasks)
				domainAnalysisMode, _ := baseContext["domain_analysis_mode"].(string)

				childCtx := ctx
				domainAnalysisWorkflowID := ""
				if stID := strings.TrimSpace(domainAnalysisSubtasks[0].ID); stID != "" {
					domainAnalysisWorkflowID = fmt.Sprintf("%s-domain-analysis-%s", callerWorkflowID, stID)
					childCtx = workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
						WorkflowID: domainAnalysisWorkflowID,
					})
				}

				domainAnalysisFuture = workflow.ExecuteChildWorkflow(childCtx, DomainAnalysisWorkflow, DomainAnalysisInput{
					ParentWorkflowID:     workflowID,
					CallerWorkflowID:     callerWorkflowID,
					Query:                input.Query,
					CanonicalName:        refineResult.CanonicalName,
					DisambiguationTerms:  refineResult.DisambiguationTerms,
					ResearchAreas:        refineResult.ResearchAreas,
					ResearchDimensions:   refineResult.ResearchDimensions,
					OfficialDomains:      refineResult.OfficialDomains,
					ExactQueries:         refineResult.ExactQueries,
					TargetLanguages:      refineResult.TargetLanguages,
					LocalizationNeeded:   refineResult.LocalizationNeeded,
					PrefetchSubpageLimit: refineResult.PrefetchSubpageLimit,
					RequestedRegions:     requestedRegions,
					PlanHints:            planHints,
					SubtaskDescription:   strings.TrimSpace(domainAnalysisSubtasks[0].Description),
					Context:              baseContext,
					UserID:               input.UserID,
					SessionID:            input.SessionID,
					History:              input.History,
					DomainAnalysisMode:   domainAnalysisMode,
				})
				domainAnalysisStarted = true
				if domainAnalysisWorkflowID != "" {
					logger.Info("Domain analysis child workflow scheduled",
						"domain_analysis_workflow_id", domainAnalysisWorkflowID,
					)
					emitTaskUpdatePayload(ctx, input, activities.StreamEventProgress, "domain_analysis",
						"Domain analysis started",
						map[string]any{
							"domain_analysis_workflow_id": domainAnalysisWorkflowID,
						},
					)
				}
			}
		}
	}

	// Step 2: Execute based on complexity
	// 步骤 2：按复杂度执行
	// Quick strategy always uses parallel execution with quick_research_agent role
	// quick 策略始终使用并行执行，角色为 quick_research_agent
	isQuickStrategy := false
	if sv, ok := baseContext["research_strategy"].(string); ok && strings.ToLower(strings.TrimSpace(sv)) == "quick" {
		isQuickStrategy = true
	}

	if !isQuickStrategy && (decomp.ComplexityScore < 0.5 || len(decomp.Subtasks) <= 1) {
		// Simple research - use React pattern for step-by-step exploration
		// 简单研究——用 React pattern 逐步探索
		logger.Info("Using React pattern for simple research",
			"complexity", decomp.ComplexityScore,
		)

		// Allow tuning ReAct iterations via context with safe clamp (2..8)
		// 允许通过 context 调节 ReAct 迭代次数，并做安全钳制（2..8）
		// Default depends on strategy: quick -> 2, otherwise 5
		// 默认值取决于策略：quick -> 2，其余 -> 5
		reactMaxIterations := 5
		if sv, ok := baseContext["research_strategy"].(string); ok {
			if strings.ToLower(strings.TrimSpace(sv)) == "quick" {
				reactMaxIterations = 2
			}
		}
		if v, ok := baseContext["react_max_iterations"]; ok {
			switch t := v.(type) {
			case int:
				reactMaxIterations = t
			case float64:
				reactMaxIterations = int(t)
			}
			if reactMaxIterations < 2 {
				reactMaxIterations = 2
			}
			if reactMaxIterations > 8 {
				reactMaxIterations = 8
			}
		}

		reactConfig := patterns.ReactConfig{
			MaxIterations:     reactMaxIterations,
			MinIterations:     2,
			ObservationWindow: 3,
			MaxObservations:   20,
			MaxThoughts:       10,
			MaxActions:        10,
		}

		reactOpts := patterns.Options{
			BudgetAgentMax: agentMaxTokens,
			SessionID:      input.SessionID,
			UserID:         input.UserID,
			EmitEvents:     true,
			ModelTier:      modelTier,
			Context:        baseContext,
		}

		reactResult, err := patterns.ReactLoop(
			ctx,
			refinedQuery,
			baseContext,
			input.SessionID,
			convertHistoryForAgent(input.History),
			reactConfig,
			reactOpts,
		)

		if err != nil {
			return TaskResult{
				Success:      false,
				ErrorMessage: fmt.Sprintf("React loop failed: %v", err),
				Metadata:     buildResearchFailureMetadata("execution", totalTokens, len(agentResults), ""),
			}, err
		}

		// Use the actual agent results from ReAct (includes tool executions for citation collection)
		// 使用 ReAct 返回的实际 agent 结果（含工具执行记录，用于收集引用）
		agentResults = append(agentResults, reactResult.AgentResults...)
		totalTokens = reactResult.TotalTokens

		// Persist agent executions from ReactLoop
		// 持久化 ReactLoop 产生的 agent 执行记录
		workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
		for i, result := range reactResult.AgentResults {
			agentID := agents.GetAgentName(workflowID, i)
			persistAgentExecutionLocal(ctx, workflowID, agentID, refinedQuery, result)
		}

	} else {
		// Complex research - check if we should use ReAct per task for deeper reasoning
		// 复杂研究——判断是否对每个 task 用 ReAct 以获得更深的推理
		useReactPerTask := false
		if v, ok := baseContext["react_per_task"].(bool); ok && v {
			useReactPerTask = true
			logger.Info("react_per_task enabled via context flag")
		}
		// Auto-enable for high complexity only when strategy is deep/academic
		// 仅当策略为 deep/academic 时，才对高复杂度任务自动启用
		if !useReactPerTask && decomp.ComplexityScore > 0.7 {
			strategy := ""
			if sv, ok := baseContext["research_strategy"].(string); ok {
				strategy = strings.ToLower(strings.TrimSpace(sv))
			}
			if strategy == "deep" || strategy == "academic" {
				useReactPerTask = true
				logger.Info("Auto-enabling react_per_task due to high complexity",
					"complexity", decomp.ComplexityScore,
					"strategy", strategy,
				)
			}
		}

		if useReactPerTask {
			// Use ReAct loop per subtask for deeper reasoning
			// 对每个子任务使用 ReAct 循环以获得更深的推理
			logger.Info("Using ReAct per subtask for deep research",
				"complexity", decomp.ComplexityScore,
				"subtasks", len(decomp.Subtasks),
			)

			// Determine execution strategy
			// 决定执行策略
			hasDependencies := false
			for _, subtask := range decomp.Subtasks {
				if len(subtask.Dependencies) > 0 {
					hasDependencies = true
					break
				}
			}

			if hasDependencies {
				// Sequential execution with ReAct per subtask, respecting dependencies
				// 带依赖关系的顺序执行，每个子任务内部用 ReAct
				logger.Info("Using sequential ReAct execution due to dependencies")

				// Build execution order via topological sort
				// 通过拓扑排序确定执行顺序
				executionOrder := topologicalSort(decomp.Subtasks)
				previousResults := make(map[string]string)

				for _, subtaskID := range executionOrder {
					// Find the subtask
					// 找到该子任务
					var subtask activities.Subtask
					for _, st := range decomp.Subtasks {
						if st.ID == subtaskID {
							subtask = st
							break
						}
					}

					// Build context with dependency results
					// 把依赖结果写入 context
					subtaskContext := make(map[string]any)
					maps.Copy(subtaskContext, baseContext)
					if contract := buildTaskContractContext(subtask); contract != nil {
						maps.Copy(subtaskContext, contract)
					}
					if len(subtask.Dependencies) > 0 {
						depResults := make(map[string]string)
						for _, depID := range subtask.Dependencies {
							if res, ok := previousResults[depID]; ok {
								depResults[depID] = res
							}
						}
						subtaskContext["previous_results"] = depResults
					}

					// Execute this subtask with ReAct
					// 用 ReAct 执行该子任务
					// Allow tuning ReAct iterations via context with safe clamp (2..8)
					// 允许通过 context 调节 ReAct 迭代次数，并做安全钳制（2..8）
					// Default depends on strategy: quick -> 2, otherwise 3
					// 默认值取决于策略：quick -> 2，其余 -> 3
					reactMaxIterations := 3
					if sv, ok := baseContext["research_strategy"].(string); ok {
						if strings.ToLower(strings.TrimSpace(sv)) == "quick" {
							reactMaxIterations = 2
						}
					}
					if v, ok := baseContext["react_max_iterations"]; ok {
						switch t := v.(type) {
						case int:
							reactMaxIterations = t
						case float64:
							reactMaxIterations = int(t)
						}
						if reactMaxIterations < 2 {
							reactMaxIterations = 2
						}
						if reactMaxIterations > 8 {
							reactMaxIterations = 8
						}
					}
					reactConfig := patterns.ReactConfig{
						MaxIterations:     reactMaxIterations,
						MinIterations:     2,
						ObservationWindow: 3,
						MaxObservations:   20,
						MaxThoughts:       10,
						MaxActions:        10,
					}

					reactOpts := patterns.Options{
						BudgetAgentMax: agentMaxTokens,
						SessionID:      input.SessionID,
						UserID:         input.UserID,
						EmitEvents:     true,
						ModelTier:      modelTier,
						Context:        subtaskContext,
					}

					reactResult, err := patterns.ReactLoop(
						ctx,
						subtask.Description,
						subtaskContext,
						input.SessionID,
						convertHistoryForAgent(input.History),
						reactConfig,
						reactOpts,
					)

					if err == nil {
						agentResults = append(agentResults, reactResult.AgentResults...)
						totalTokens += reactResult.TotalTokens
						previousResults[subtaskID] = reactResult.FinalResult

						// Persist agent executions for this subtask
						// 持久化该子任务的 agent 执行记录
						workflowID := workflow.GetInfo(ctx).WorkflowExecution.ID
						for i, result := range reactResult.AgentResults {
							agentID := agents.GetAgentName(workflowID, i)
							persistAgentExecutionLocal(ctx, workflowID, agentID, subtask.Description, result)
						}
					} else {
						logger.Warn("ReAct loop failed for subtask, continuing", "subtask_id", subtaskID, "error", err)
					}
				}

			} else {
				// Parallel execution with ReAct per subtask
				// 每个子任务都用 ReAct 的并行执行
				logger.Info("Using parallel ReAct execution (no dependencies)")

				// Determine concurrency limit from context (default 5, clamp 1..20)
				// 从 context 决定并发上限（默认 5，钳制在 1..20）
				concurrency := 5
				if v, ok := baseContext["max_concurrent_agents"]; ok {
					switch t := v.(type) {
					case int:
						concurrency = t
					case float64:
						if t > 0 {
							concurrency = int(t)
						}
					}
				}
				if concurrency < 1 {
					concurrency = 1
				}
				if concurrency > 20 {
					concurrency = 20
				}

				// Use channel to collect results and gate concurrency
				// 用 channel 收集结果并控制并发
				resultsChan := workflow.NewChannel(ctx)
				active := 0

				for _, subtask := range decomp.Subtasks {
					// Gate concurrency
					// 控制并发
					if active >= concurrency {
						var result *patterns.ReactLoopResult
						resultsChan.Receive(ctx, &result)
						if result != nil {
							agentResults = append(agentResults, result.AgentResults...)
							totalTokens += result.TotalTokens
						}
						active--
					}

					st := subtask // Capture for goroutine 注：捕获给 goroutine 使用
					workflow.Go(ctx, func(gctx workflow.Context) {
						// Allow tuning ReAct iterations via context with safe clamp (2..8)
						// 允许通过 context 调节 ReAct 迭代次数，并做安全钳制（2..8）
						// Default depends on strategy: quick -> 2, otherwise 3
						// 默认值取决于策略：quick -> 2，其余 -> 3
						reactMaxIterations := 3
						if sv, ok := baseContext["research_strategy"].(string); ok {
							if strings.ToLower(strings.TrimSpace(sv)) == "quick" {
								reactMaxIterations = 2
							}
						}
						if v, ok := baseContext["react_max_iterations"]; ok {
							switch t := v.(type) {
							case int:
								reactMaxIterations = t
							case float64:
								reactMaxIterations = int(t)
							}
							if reactMaxIterations < 2 {
								reactMaxIterations = 2
							}
							if reactMaxIterations > 8 {
								reactMaxIterations = 8
							}
						}
						reactConfig := patterns.ReactConfig{
							MaxIterations:     reactMaxIterations,
							MinIterations:     2,
							ObservationWindow: 3,
							MaxObservations:   20,
							MaxThoughts:       10,
							MaxActions:        10,
						}

						reactOpts := patterns.Options{
							BudgetAgentMax: agentMaxTokens,
							SessionID:      input.SessionID,
							UserID:         input.UserID,
							EmitEvents:     true,
							ModelTier:      modelTier,
							Context:        baseContext,
						}

						reactResult, err := patterns.ReactLoop(
							gctx,
							st.Description,
							baseContext,
							input.SessionID,
							convertHistoryForAgent(input.History),
							reactConfig,
							reactOpts,
						)

						if err == nil {
							// Persist agent executions for this parallel subtask
							// 持久化该并行子任务的 agent 执行记录
							workflowID := workflow.GetInfo(gctx).WorkflowExecution.ID
							for i, result := range reactResult.AgentResults {
								agentID := agents.GetAgentName(workflowID, i)
								persistAgentExecutionLocal(gctx, workflowID, agentID, st.Description, result)
							}
							resultsChan.Send(gctx, reactResult)
						} else {
							logger.Warn("ReAct loop failed for parallel subtask", "subtask_id", st.ID, "error", err)
							// Send nil to indicate failure
							// 发送 nil 表示失败
							resultsChan.Send(gctx, (*patterns.ReactLoopResult)(nil))
						}
					})
					active++
				}

				// Drain remaining in-flight tasks
				// 排空仍在执行中的任务
				for active > 0 {
					var result *patterns.ReactLoopResult
					resultsChan.Receive(ctx, &result)
					if result != nil {
						agentResults = append(agentResults, result.AgentResults...)
						totalTokens += result.TotalTokens
					}
					active--
				}
			}

		} else {
			// Complex research - use parallel/hybrid execution with simple agents
			// 复杂研究——用简单 agent 做 parallel/hybrid 执行
			logger.Info("Using parallel execution for complex research",
				"complexity", decomp.ComplexityScore,
				"subtasks", len(decomp.Subtasks),
			)

			// Determine execution strategy
			// 决定执行策略
			hasDependencies := false
			for _, subtask := range decomp.Subtasks {
				if len(subtask.Dependencies) > 0 {
					hasDependencies = true
					break
				}
			}

			if hasDependencies {
				// Use hybrid execution for dependency management
				// 用 hybrid 执行处理依赖关系
				logger.Info("Using hybrid execution due to dependencies")

				hybridTasks := make([]execution.HybridTask, len(decomp.Subtasks))
				for i, subtask := range decomp.Subtasks {
					role := "deep_research_agent"
					if sv, ok := baseContext["research_strategy"].(string); ok && strings.ToLower(strings.TrimSpace(sv)) == "quick" {
						role = "quick_research_agent"
					}
					if i < len(decomp.AgentTypes) && decomp.AgentTypes[i] != "" {
						role = decomp.AgentTypes[i]
					}

					hybridTasks[i] = execution.HybridTask{
						ID:               subtask.ID,
						Description:      subtask.Description,
						SuggestedTools:   subtask.SuggestedTools,
						ToolParameters:   subtask.ToolParameters,
						PersonaID:        subtask.SuggestedPersona,
						Role:             role,
						ParentArea:       subtask.ParentArea,
						Dependencies:     subtask.Dependencies,
						ContextOverrides: buildTaskContractContext(subtask),
					}
				}

				// Force inject web_search and web_fetch/web_subpage_fetch tools for research workflows
				// 为研究工作流强制注入 web_search 与 web_fetch/web_subpage_fetch 工具
				for i := range hybridTasks {
					hasWebSearch := false
					hasWebFetch := false
					hasWebSubpageFetch := false
					for _, tool := range hybridTasks[i].SuggestedTools {
						if tool == "web_search" {
							hasWebSearch = true
						}
						if tool == "web_fetch" {
							hasWebFetch = true
						}
						if tool == "web_subpage_fetch" {
							hasWebSubpageFetch = true
						}
					}
					if !hasWebSearch {
						hybridTasks[i].SuggestedTools = append(hybridTasks[i].SuggestedTools, "web_search")
					}
					if !hasWebFetch {
						hybridTasks[i].SuggestedTools = append(hybridTasks[i].SuggestedTools, "web_fetch")
					}
					if !hasWebSubpageFetch {
						hybridTasks[i].SuggestedTools = append(hybridTasks[i].SuggestedTools, "web_subpage_fetch")
					}
				}

				// Generate search routing plans for tasks with source guidance
				// 为带来源指引的任务生成搜索路由方案
				for i := range hybridTasks {
					subtask := decomp.Subtasks[i]
					if subtask.SourceGuidance != nil && len(subtask.SourceGuidance.Required) > 0 {
						var routeResult activities.SearchRouteResult
						err := workflow.ExecuteActivity(ctx,
							"RouteSearch",
							activities.SearchRouteInput{
								Query:       subtask.Description,
								Dimension:   subtask.ParentArea,
								SourceTypes: subtask.SourceGuidance.Required,
								Priority:    "high",
								Context:     baseContext,
							}).Get(ctx, &routeResult)

						if err == nil && len(routeResult.Routes) > 0 {
							if hybridTasks[i].ContextOverrides == nil {
								hybridTasks[i].ContextOverrides = make(map[string]any)
							}
							hybridTasks[i].ContextOverrides["search_routes"] = routeResult.Routes
							hybridTasks[i].ContextOverrides["search_strategy"] = routeResult.Strategy
							logger.Info("Initial research: Search routing plan generated",
								"task", subtask.ID,
								"routes", len(routeResult.Routes),
								"strategy", routeResult.Strategy,
							)
						}
					}
				}

				// Determine concurrency from context (default 5, clamp 1..20)
				// 从 context 决定并发数（默认 5，钳制在 1..20）
				hybridMax := 5
				if v, ok := baseContext["max_concurrent_agents"]; ok {
					switch t := v.(type) {
					case int:
						hybridMax = t
					case float64:
						if t > 0 {
							hybridMax = int(t)
						}
					}
				}
				if hybridMax < 1 {
					hybridMax = 1
				}
				if hybridMax > 20 {
					hybridMax = 20
				}

				hybridConfig := execution.HybridConfig{
					MaxConcurrency:           hybridMax,
					EmitEvents:               true,
					Context:                  baseContext,
					DependencyWaitTimeout:    6 * time.Minute,  // Total timeout 注：总超时
					DependencyCheckInterval:  30 * time.Second, // Check every 30s for accurate Temporal UI display 注：每 30 秒检查一次，便于 Temporal UI 准确展示
					PassDependencyResults:    true,
					ClearDependentToolParams: true,
				}

				// Check if domain_analysis is running and some tasks depend on it
				// 检查 domain_analysis 是否在运行、是否有 task 依赖它
				// If so, split tasks: run independent tasks first (in parallel with domain_analysis),
				// 若是，则拆分 task：先（与 domain_analysis 并行）执行独立 task，
				// wait for domain_analysis, then run dependent tasks with injected official_domains
				// 等 domain_analysis 完成后再执行依赖 task 并注入 official_domains
				// 当 domain_analysis 正在运行且部分 task 依赖它时，把 task 拆成两批：
				// 先并行执行独立 task，等 domain_analysis 完成后再执行依赖 task 并注入 official_domains。
				if domainAnalysisStarted && len(domainAnalysisSubtasks) > 0 {
					domainAnalysisTaskID := domainAnalysisSubtasks[0].ID

					// Split tasks into independent and task1-dependent
					// 把 task 拆分为独立 task 与依赖 task1 的 task
					var independentTasks []execution.HybridTask
					var task1DependentTasks []execution.HybridTask

					for _, task := range hybridTasks {
						dependsOnTask1 := slices.Contains(task.Dependencies, domainAnalysisTaskID)
						if dependsOnTask1 {
							task1DependentTasks = append(task1DependentTasks, task)
						} else {
							independentTasks = append(independentTasks, task)
						}
					}

					logger.Info("Split tasks for domain_analysis dependency",
						"independent_count", len(independentTasks),
						"dependent_count", len(task1DependentTasks),
						"domain_analysis_task_id", domainAnalysisTaskID,
					)

					// Collect results from all phases for cross-phase dependency resolution
					// 收集各阶段结果，用于跨阶段依赖解析
					phase1Results := make(map[string]activities.AgentExecutionResult)

					// Phase 1: Execute independent tasks (runs in parallel with DomainAnalysis which is already started)
					// 阶段 1：执行独立 task（与已启动的 DomainAnalysis 并行）
					if len(independentTasks) > 0 {
						indepResult, err := execution.ExecuteHybrid(
							ctx,
							independentTasks,
							input.SessionID,
							convertHistoryForAgent(input.History),
							hybridConfig,
							agentMaxTokens,
							input.UserID,
							modelTier,
						)
						if err != nil {
							logger.Warn("Independent tasks execution failed", "error", err)
						} else {
							for id, result := range indepResult.Results {
								agentResults = append(agentResults, result)
								phase1Results[id] = result // Collect for Phase 3 注：收集给阶段 3
							}
							totalTokens += indepResult.TotalTokens
						}
					}

					// Phase 2: Wait for DomainAnalysis to complete
					// 阶段 2：等待 DomainAnalysis 完成
					var childResult DomainAnalysisResult
					err := domainAnalysisFuture.Get(ctx, &childResult)
					if err != nil {
						logger.Warn("Domain analysis workflow failed during split execution", "error", err)
					} else {
						domainAnalysisResult = &childResult
						logger.Info("Domain analysis completed for dependent tasks",
							"digest_len", len(childResult.DomainAnalysisDigest),
							"official_domains", len(childResult.OfficialDomainsSelected),
						)

						// Inject official_domains into dependent tasks and remove task-1 dependency
						// 把 official_domains 注入依赖 task，并移除对 task1 的依赖
						if len(childResult.OfficialDomainsSelected) > 0 {
							domains := extractDomainsFromCoverage(childResult.OfficialDomainsSelected)
							for i := range task1DependentTasks {
								if task1DependentTasks[i].ContextOverrides == nil {
									task1DependentTasks[i].ContextOverrides = make(map[string]any)
								}
								task1DependentTasks[i].ContextOverrides["official_domains"] = domains
								task1DependentTasks[i].ContextOverrides["official_domains_source"] = "domain_analysis"
								// Note: We keep task.Dependencies intact so that hybrid.go can inject
								// 注意：保持 task.Dependencies 不变，好让 hybrid.go 能正常注入
								// dependency_results properly. The dependency on task-1 will be satisfied
								// dependency_results。对 task1 的依赖会通过
								// via PrefilledResults which now contains domain_analysis result.
								// PrefilledResults（现已包含 domain_analysis 结果）得到满足。
								// 保留 task.Dependencies 不动，好让 hybrid.go 正常注入 dependency_results；
								// 对 task-1 的依赖会通过已包含 domain_analysis 结果的 PrefilledResults 满足。
							}
							logger.Info("Injected official_domains into dependent tasks",
								"domains", domains,
								"task_count", len(task1DependentTasks),
							)
						}
					}

					// Phase 3: Execute dependent tasks with cross-phase dependency support
					// 阶段 3：执行依赖 task，并支持跨阶段依赖
					if len(task1DependentTasks) > 0 {
						// Build PrefilledResults from domain_analysis + Phase 1 results
						// 用 domain_analysis 结果 + 阶段 1 结果构造 PrefilledResults
						prefilledResults := make(map[string]activities.AgentExecutionResult)

						// Add domain_analysis result (sub-workflow)
						// 加入 domain_analysis 结果（子 workflow）
						if domainAnalysisResult != nil {
							prefilledResults[domainAnalysisTaskID] = activities.AgentExecutionResult{
								Response:   domainAnalysisResult.DomainAnalysisDigest,
								Success:    true,
								TokensUsed: domainAnalysisResult.Stats.DigestTokensUsed,
							}
						}

						// Add Phase 1 results (sub-agents) for cross-phase dependencies
						// 加入阶段 1 结果（子 agent），用于跨阶段依赖
						maps.Copy(prefilledResults, phase1Results)

						logger.Info("Building PrefilledResults for Phase 3",
							"domain_analysis_included", domainAnalysisResult != nil,
							"phase1_results_count", len(phase1Results),
							"total_prefilled", len(prefilledResults),
						)

						// Create config with PrefilledResults for this phase
						// 为本阶段创建带 PrefilledResults 的 config
						phase3Config := hybridConfig
						phase3Config.PrefilledResults = prefilledResults

						depResult, err := execution.ExecuteHybrid(
							ctx,
							task1DependentTasks,
							input.SessionID,
							convertHistoryForAgent(input.History),
							phase3Config,
							agentMaxTokens,
							input.UserID,
							modelTier,
						)
						if err != nil {
							logger.Warn("Dependent tasks execution failed", "error", err)
						} else {
							for _, result := range depResult.Results {
								agentResults = append(agentResults, result)
							}
							totalTokens += depResult.TotalTokens
						}
					}

					// Mark that we've already processed domainAnalysisFuture
					// 标记 domainAnalysisFuture 已处理完毕
					domainAnalysisStarted = false

				} else {
					// Original code path: no domain_analysis running or no dependent tasks
					// 原有代码路径：没有运行中的 domain_analysis，也没有依赖 task
					hybridResult, err := execution.ExecuteHybrid(
						ctx,
						hybridTasks,
						input.SessionID,
						convertHistoryForAgent(input.History),
						hybridConfig,
						agentMaxTokens,
						input.UserID,
						modelTier,
					)

					if err != nil {
						return TaskResult{
							Success:      false,
							ErrorMessage: fmt.Sprintf("Hybrid execution failed: %v", err),
							Metadata:     buildResearchFailureMetadata("execution", totalTokens, len(agentResults), ""),
						}, err
					}

					// Convert results to agent results
					// 把结果转换为 agent 结果
					for _, result := range hybridResult.Results {
						agentResults = append(agentResults, result)
					}
					totalTokens = hybridResult.TotalTokens
				}

			} else {
				// Use pure parallel execution
				// 使用纯并行执行
				logger.Info("Using pure parallel execution")

				parallelTasks := make([]execution.ParallelTask, len(decomp.Subtasks))
				for i, subtask := range decomp.Subtasks {
					role := "deep_research_agent"
					if sv, ok := baseContext["research_strategy"].(string); ok && strings.ToLower(strings.TrimSpace(sv)) == "quick" {
						role = "quick_research_agent"
					}
					if i < len(decomp.AgentTypes) && decomp.AgentTypes[i] != "" {
						role = decomp.AgentTypes[i]
					}

					parallelTasks[i] = execution.ParallelTask{
						ID:               subtask.ID,
						Description:      subtask.Description,
						SuggestedTools:   subtask.SuggestedTools,
						ToolParameters:   subtask.ToolParameters,
						PersonaID:        subtask.SuggestedPersona,
						Role:             role,
						ParentArea:       subtask.ParentArea,
						ContextOverrides: buildTaskContractContext(subtask),
					}
				}

				// Force inject web_search and web_fetch/web_subpage_fetch tools for research workflows
				// 为研究工作流强制注入 web_search 与 web_fetch/web_subpage_fetch 工具
				for i := range parallelTasks {
					hasWebSearch := false
					hasWebFetch := false
					hasWebSubpageFetch := false
					for _, tool := range parallelTasks[i].SuggestedTools {
						if tool == "web_search" {
							hasWebSearch = true
						}
						if tool == "web_fetch" {
							hasWebFetch = true
						}
						if tool == "web_subpage_fetch" {
							hasWebSubpageFetch = true
						}
					}
					if !hasWebSearch {
						parallelTasks[i].SuggestedTools = append(parallelTasks[i].SuggestedTools, "web_search")
					}
					if !hasWebFetch {
						parallelTasks[i].SuggestedTools = append(parallelTasks[i].SuggestedTools, "web_fetch")
					}
					if !hasWebSubpageFetch {
						parallelTasks[i].SuggestedTools = append(parallelTasks[i].SuggestedTools, "web_subpage_fetch")
					}
				}

				// Generate search routing plans for tasks with source guidance
				// 为带来源指引的任务生成搜索路由方案
				for i := range parallelTasks {
					subtask := decomp.Subtasks[i]
					if subtask.SourceGuidance != nil && len(subtask.SourceGuidance.Required) > 0 {
						var routeResult activities.SearchRouteResult
						err := workflow.ExecuteActivity(ctx,
							"RouteSearch",
							activities.SearchRouteInput{
								Query:       subtask.Description,
								Dimension:   subtask.ParentArea,
								SourceTypes: subtask.SourceGuidance.Required,
								Priority:    "high",
								Context:     baseContext,
							}).Get(ctx, &routeResult)

						if err == nil && len(routeResult.Routes) > 0 {
							if parallelTasks[i].ContextOverrides == nil {
								parallelTasks[i].ContextOverrides = make(map[string]any)
							}
							parallelTasks[i].ContextOverrides["search_routes"] = routeResult.Routes
							parallelTasks[i].ContextOverrides["search_strategy"] = routeResult.Strategy
							logger.Info("Initial research: Search routing plan generated",
								"task", subtask.ID,
								"routes", len(routeResult.Routes),
								"strategy", routeResult.Strategy,
							)
						}
					}
				}

				// Determine concurrency from context (default 5, clamp 1..20)
				// 从 context 决定并发数（默认 5，钳制在 1..20）
				parallelMax := 5
				if v, ok := baseContext["max_concurrent_agents"]; ok {
					switch t := v.(type) {
					case int:
						parallelMax = t
					case float64:
						if t > 0 {
							parallelMax = int(t)
						}
					}
				}
				if parallelMax < 1 {
					parallelMax = 1
				}
				if parallelMax > 20 {
					parallelMax = 20
				}

				parallelConfig := execution.ParallelConfig{
					MaxConcurrency: parallelMax,
					EmitEvents:     true,
					Context:        baseContext,
				}

				parallelResult, err := execution.ExecuteParallel(
					ctx,
					parallelTasks,
					input.SessionID,
					convertHistoryForAgent(input.History),
					parallelConfig,
					agentMaxTokens,
					input.UserID,
					modelTier,
				)

				if err != nil {
					return TaskResult{
						Success:      false,
						ErrorMessage: fmt.Sprintf("Parallel execution failed: %v", err),
						Metadata:     buildResearchFailureMetadata("execution", totalTokens, len(agentResults), ""),
					}, err
				}

				agentResults = parallelResult.Results
				totalTokens = parallelResult.TotalTokens
			}
		}
	}

	if domainAnalysisStarted {
		var childResult DomainAnalysisResult
		err := domainAnalysisFuture.Get(ctx, &childResult)
		if err != nil {
			logger.Warn("Domain analysis workflow failed", "error", err)
			// Emit warning event for user visibility
			// 发送告警事件以便用户可见
			emitTaskUpdatePayload(ctx, input, activities.StreamEventWarning, "domain_analysis",
				fmt.Sprintf("Domain analysis failed: %v", err),
				nil,
			)
		} else {
			domainAnalysisResult = &childResult
			logger.Info("Domain analysis workflow completed",
				"digest_len", len(childResult.DomainAnalysisDigest),
				"prefetch_urls", len(childResult.PrefetchURLs),
				"citations", len(childResult.Citations),
				"digest_tokens", childResult.Stats.DigestTokensUsed,
				"discovery_failed", childResult.Stats.DiscoveryFailed,
			)
			// Check for discovery phase failure and emit warning
			// 检查发现阶段是否失败并发送告警
			if childResult.Stats.DiscoveryFailed {
				logger.Warn("Domain discovery phase failed",
					"error", childResult.Stats.DiscoveryError,
				)
				emitTaskUpdatePayload(ctx, input, activities.StreamEventWarning, "domain_analysis",
					fmt.Sprintf("Domain discovery failed: %s", childResult.Stats.DiscoveryError),
					map[string]any{
						"discovery_error": childResult.Stats.DiscoveryError,
					},
				)
			}
			if len(childResult.OfficialDomainsSelected) > 0 {
				domains := extractDomainsFromCoverage(childResult.OfficialDomainsSelected)
				if len(domains) > 0 {
					baseContext["official_domains"] = domains
					baseContext["official_domains_source"] = "domain_analysis"
				}
			}
		}
	}

	// Merge domain analysis digest (v1) or legacy prefetch evidence before localized search and gap analysis.
	// 在本地化搜索与缺口分析之前，合并 domain analysis digest（v1）或旧的预取证据。
	if domainAnalysisResult != nil && strings.TrimSpace(domainAnalysisResult.DomainAnalysisDigest) != "" {
		logger.Info("Merging domain analysis digest into agent results",
			"digest_len", len(domainAnalysisResult.DomainAnalysisDigest),
		)
		agentResults = append([]activities.AgentExecutionResult{domainAnalysisDigestResult(domainAnalysisResult)}, agentResults...)
		if domainAnalysisResult.Stats.DigestTokensUsed > 0 {
			totalTokens += domainAnalysisResult.Stats.DigestTokensUsed
		}
	} else if domainAnalysisResult != nil {
		logger.Warn("Domain analysis completed but DomainAnalysisDigest is empty",
			"prefetch_urls", len(domainAnalysisResult.PrefetchURLs),
			"digest_tokens", domainAnalysisResult.Stats.DigestTokensUsed,
		)
	}
	if domainAnalysisResult == nil && len(domainPrefetchResults) > 0 {
		agentResults = append(domainPrefetchResults, agentResults...)
		totalTokens += domainPrefetchTokens
	}

	// Step 2.5: Optional localized search for multi-language coverage
	// 步骤 2.5：可选的多语言覆盖本地化搜索
	localizedSearchEnabled := false
	if v, ok := baseContext["enable_localized_search"].(bool); ok {
		localizedSearchEnabled = v
	}
	if localizedSearchEnabled {
		// Get target languages from context
		// 从 context 取目标语言
		var targetLanguages []string
		if langs, ok := baseContext["target_languages"].([]any); ok {
			for _, l := range langs {
				if ls, ok := l.(string); ok {
					targetLanguages = append(targetLanguages, ls)
				}
			}
		} else if langs, ok := baseContext["target_languages"].([]string); ok {
			targetLanguages = langs
		}

		if len(targetLanguages) > 0 {
			logger.Info("Running localized search",
				"target_languages", targetLanguages,
			)

			// Get entity name for localization
			// 取用于本地化的实体名
			entityName := ""
			if en, ok := baseContext["canonical_name"].(string); ok {
				entityName = en
			} else {
				entityName = input.Query
			}

			// Step 2.5.1: Detect entity localizations
			// 步骤 2.5.1：检测实体的本地化名称
			var localizedNames map[string][]string
			if presetNames, ok := baseContext["localized_names"].(map[string][]string); ok {
				localizedNames = presetNames
			} else {
				var locResult activities.EntityLocalizationResult
				locErr := workflow.ExecuteActivity(ctx,
					"DetectEntityLocalization",
					activities.EntityLocalizationInput{
						EntityName:       entityName,
						TargetLanguages:  targetLanguages,
						EntityType:       "company", // Default entity type 注：默认实体类型
						ParentWorkflowID: input.ParentWorkflowID,
					}).Get(ctx, &locResult)

				if locErr == nil {
					localizedNames = locResult.LocalizedNames
					totalTokens += locResult.TokensUsed
					if locResult.TokensUsed > 0 || locResult.InputTokens > 0 || locResult.OutputTokens > 0 {
						inTok := locResult.InputTokens
						outTok := locResult.OutputTokens
						if inTok == 0 && outTok == 0 && locResult.TokensUsed > 0 {
							inTok = int(float64(locResult.TokensUsed) * 0.6)
							outTok = locResult.TokensUsed - inTok
						}
						recCtx := opts.WithTokenRecordOptions(ctx)
						_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
							UserID:       input.UserID,
							SessionID:    input.SessionID,
							TaskID:       workflowID,
							AgentID:      "entity_localization",
							Model:        locResult.ModelUsed,
							Provider:     locResult.Provider,
							InputTokens:  inTok,
							OutputTokens: outTok,
							Metadata:     map[string]any{"phase": "localization"},
						}).Get(recCtx, nil)
					}
					logger.Info("Entity localization detected",
						"languages", len(localizedNames),
					)
				} else {
					logger.Warn("Entity localization failed, using original name",
						"error", locErr,
					)
					localizedNames = make(map[string][]string)
					for _, lang := range targetLanguages {
						localizedNames[lang] = []string{entityName}
					}
				}
			}

			// Step 2.5.2: Spawn parallel localized searches using Temporal-safe channels
			// 步骤 2.5.2：用 Temporal 安全的 channel 并发启动本地化搜索
			if len(localizedNames) > 0 {
				type localizedPayload struct {
					Results []activities.AgentExecutionResult
					Tokens  int
				}

				localizedChan := workflow.NewChannel(ctx)
				numLocalizedSearches := len(targetLanguages)

				for _, lang := range targetLanguages {
					names := localizedNames[lang]
					if len(names) == 0 {
						names = []string{entityName}
					}

					workflow.Go(ctx, func(gctx workflow.Context) {
						// Build localized search context
						// 构造本地化搜索 context
						localCtx := make(map[string]any)
						maps.Copy(localCtx, baseContext)
						localCtx["search_language"] = lang
						localCtx["localized_query"] = names[0] // Use first localized name 注：使用第一个本地化名称
						localCtx["is_localized_search"] = true

						// Get regional sites for this language
						// 取该语言对应的区域站点
						regionalSites := activities.GetRegionalSourceSites(lang)
						if len(regionalSites) > 0 {
							localCtx["target_sites"] = regionalSites
						}

						// Query to use for localized search
						// 本地化搜索使用的查询
						localQuery := names[0]

						// Execute the localized search
						// 执行本地化搜索
						reactConfig := patterns.ReactConfig{
							MaxIterations:     2, // Shorter for localized searches 注：本地化搜索用更少的迭代
							MinIterations:     1,
							ObservationWindow: 3,
							MaxObservations:   10,
							MaxThoughts:       5,
							MaxActions:        5,
						}
						reactOpts := patterns.Options{
							BudgetAgentMax: agentMaxTokens / 2, // Use half budget for localized 注：本地化搜索只用一半预算
							SessionID:      input.SessionID,
							ModelTier:      modelTier,
							Context:        localCtx,
						}

						result, err := patterns.ReactLoop(
							gctx,
							localQuery,
							localCtx,
							input.SessionID,
							[]string{},
							reactConfig,
							reactOpts,
						)

						payload := localizedPayload{}
						if err == nil && len(result.AgentResults) > 0 {
							// Mark results as localized
							// 把结果标记为本地化
							for i := range result.AgentResults {
								result.AgentResults[i].AgentID = fmt.Sprintf("%s-local-%s", result.AgentResults[i].AgentID, lang)
							}
							payload.Results = result.AgentResults
							payload.Tokens = result.TotalTokens
						}
						localizedChan.Send(gctx, payload)
					})
				}

				// Collect localized search results
				// 收集本地化搜索结果
				for range numLocalizedSearches {
					var payload localizedPayload
					localizedChan.Receive(ctx, &payload)
					if len(payload.Results) > 0 {
						agentResults = append(agentResults, payload.Results...)
						totalTokens += payload.Tokens
					}
				}

				logger.Info("Localized search complete",
					"total_agent_results", len(agentResults),
					"languages_searched", numLocalizedSearches,
				)
			}
		}
	}

	// Take a snapshot of all agent results BEFORE any filtering, so citation
	// 在任何过滤之前先对所有 agent 结果做快照，这样引用
	// extraction can operate on complete tool outputs regardless of later
	// 提取可以基于完整的工具输出，不受后续
	// entity filtering used for synthesis tightness.
	// 用于收紧合成的实体过滤影响。
	// 在过滤前先对所有 agent 结果做快照，使引用提取基于完整 tool 输出，
	// 不受后续用于收紧合成的实体过滤影响。
	originalAgentResults := make([]activities.AgentExecutionResult, len(agentResults))
	copy(originalAgentResults, agentResults)

	// Optional: filter out agent results that likely belong to the wrong entity
	// 可选：过滤掉大概率属于其他实体的 agent 结果
	if v, ok := baseContext["canonical_name"].(string); ok && strings.TrimSpace(v) != "" {
		aliases := []string{v}
		if eqv, ok := baseContext["exact_queries"]; ok {
			switch t := eqv.(type) {
			case []string:
				for _, q := range t {
					aliases = append(aliases, strings.Trim(q, "\""))
				}
			case []any:
				for _, it := range t {
					if s, ok := it.(string); ok {
						aliases = append(aliases, strings.Trim(s, "\""))
					}
				}
			}
		}
		// Use official_domains for additional positive matching
		// 用 official_domains 做额外的正向匹配
		var domains []string
		if dv, ok := baseContext["official_domains"]; ok {
			switch t := dv.(type) {
			case []string:
				domains = append(domains, t...)
			case []any:
				for _, it := range t {
					if s, ok := it.(string); ok {
						domains = append(domains, s)
					}
				}
			}
		}
		filtered := make([]activities.AgentExecutionResult, 0, len(agentResults))
		removed := 0
		for _, ar := range agentResults {
			txt := strings.ToLower(ar.Response)
			match := false
			for _, a := range aliases {
				if sa := strings.ToLower(strings.TrimSpace(a)); sa != "" && strings.Contains(txt, sa) {
					match = true
					break
				}
			}
			if !match && len(domains) > 0 {
				for _, d := range domains {
					sd := strings.ToLower(strings.TrimSpace(d))
					if sd != "" && strings.Contains(txt, sd) {
						match = true
						break
					}
				}
			}
			// Keep non-search reasoning, drop obvious off-entity tool-driven results
			// 保留非搜索类的推理结果，丢弃明显偏离实体的工具驱动结果
			if match || len(ar.ToolsUsed) == 0 {
				filtered = append(filtered, ar)
			} else {
				removed++
			}
		}
		if len(filtered) > 0 {
			agentResults = filtered
		}
		if removed > 0 {
			logger.Info("Entity filter removed off-entity results",
				"removed", removed,
				"kept", len(agentResults),
				"aliases", aliases,
				"domains", domains,
			)
		}
	}

	// Check pause/cancel after agent execution - signal may have arrived during agent activities
	// agent 执行后检查暂停/取消——信号可能在 agent activity 执行期间到达
	if err := controlHandler.CheckPausePoint(ctx, "post_agent_execution"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("post_execution", totalTokens, len(agentResults), "")}, err
	}

	// NOTE: Per-agent token usage (including cache stats) is recorded by the budget
	// 注意：每个 agent 的 token 用量（含 cache 统计）由 budget
	// manager via agent.go's execution path. Cache stats are now backfilled from
	// manager 在 agent.go 的执行路径中记录。cache 统计现在由
	// response metadata in runUnary (agent.go). Do NOT add a separate recording loop
	// runUnary（agent.go）从 response metadata 回填。此处不要另加记录循环
	// here — it causes duplicate DB records and double-counted costs.
	// 每个 agent 的 token 用量（含 cache 统计）由 budget manager 在 agent.go 执行路径中记录，
	// cache 统计已从 runUnary 的 response metadata 回填；此处不要再加记录循环，否则会重复写库、重复计费。

	// Check pause/cancel before synthesis
	// 综合前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_synthesis"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("pre_synthesis", totalTokens, len(agentResults), "")}, err
	}

	// Step 3: Synthesize results
	// 步骤 3：综合结果
	logger.Info("Synthesizing research results",
		"agent_count", len(agentResults),
	)

	// Fallback: if no tool executions were recorded in research phase, force a single search
	// 兜底：若研究阶段没有记录到任何工具执行，则强制做一次搜索
	if !hasSuccessfulToolExecutions(agentResults) {
		logger.Warn("No successful tool executions found in research phase; running fallback web search")

		fallbackCtx := make(map[string]any)
		maps.Copy(fallbackCtx, baseContext)
		fallbackCtx["force_research"] = true

		var fallbackResult activities.AgentExecutionResult
		err := workflow.ExecuteActivity(ctx,
			"ExecuteAgent",
			activities.AgentExecutionInput{
				Query:            fmt.Sprintf("Use web_search to gather authoritative information about: %s", refinedQuery),
				AgentID:          "fallback-search",
				Context:          fallbackCtx,
				Mode:             "standard",
				SessionID:        input.SessionID,
				UserID:           input.UserID,
				History:          convertHistoryForAgent(input.History),
				SuggestedTools:   []string{"web_search"},
				ParentWorkflowID: input.ParentWorkflowID,
			}).Get(ctx, &fallbackResult)

		if err == nil {
			agentResults = append(agentResults, fallbackResult)
			totalTokens += fallbackResult.TokensUsed
			originalAgentResults = append(originalAgentResults, fallbackResult)
			logger.Info("Fallback web search completed",
				"tokens_used", fallbackResult.TokensUsed,
			)
		} else {
			logger.Warn("Fallback web search failed", "error", err)
		}
	}

	// If fact extraction is requested, use the facts-aware synthesis template unless overridden.
	// 若要求事实抽取，则使用面向事实的合成模板（除非已被覆盖）
	if enabled, ok := baseContext["enable_fact_extraction"].(bool); ok && enabled {
		if tmpl, ok := baseContext["synthesis_template"].(string); !ok || strings.TrimSpace(tmpl) == "" {
			baseContext["synthesis_template"] = "research_with_facts"
		}
	}

	// Per-agent token usage is recorded inside execution patterns (ReactLoop/Parallel/Hybrid).
	// 每个 agent 的 token 用量在执行 pattern（ReactLoop/Parallel/Hybrid）内部记录。
	// Avoid double-counting here to prevent duplicate token_usage rows.
	// 此处不要再计一次，以免产生重复的 token_usage 行。

	// Collect citations from agent tool outputs and inject into context for synthesis/formatting
	// 从 agent 工具输出中收集引用，并注入 context 供合成/格式化使用
	// Also retain them for metadata/verification.
	// 同时保留它们，供元数据/校验使用。
	var collectedCitations []metadata.Citation
	// Build lightweight results array with tool_executions to feed metadata.CollectCitations
	// 构造带 tool_executions 的轻量结果数组，喂给 metadata.CollectCitations
	{
		var resultsForCitations []any
		// IMPORTANT: Use original (unfiltered) agent results to preserve all
		// 重要：使用原始的（未过滤）agent 结果，以保留全部
		// successful tool executions for citation extraction.
		// 成功的工具执行记录用于引用提取。
		for _, ar := range originalAgentResults {
			// Build tool_executions payload compatible with citations extractor
			// 构造与引用提取器兼容的 tool_executions 载荷
			var toolExecs []any
			if len(ar.ToolExecutions) > 0 {
				for _, te := range ar.ToolExecutions {
					toolExecs = append(toolExecs, map[string]any{
						"tool":    te.Tool,
						"success": te.Success,
						"output":  te.Output,
						"error":   te.Error,
					})
				}
			}
			resultsForCitations = append(resultsForCitations, map[string]any{
				"agent_id":        ar.AgentID,
				"tool_executions": toolExecs,
				"response":        ar.Response,
			})
		}

		// Use workflow timestamp for determinism; let collector default max to 15
		// 用 workflow 时间戳保证确定性；上限交给收集器默认的 15
		now := workflow.Now(ctx)
		citations, _ := metadata.CollectCitations(resultsForCitations, now, 0)

		// Apply entity-based filtering if canonical name is present
		// 有规范名时应用基于实体的过滤
		if len(citations) > 0 {
			canonicalName, _ := baseContext["canonical_name"].(string)
			if canonicalName != "" {
				// Extract domains and aliases for filtering
				// 提取域名与别名用于过滤
				var domains []string
				if d, ok := baseContext["official_domains"].([]string); ok {
					domains = d
				}
				var aliases []string
				if eq, ok := baseContext["exact_queries"].([]string); ok {
					aliases = eq
				}
				// Filter citations by entity relevance with automatic fallback
				// 按实体相关性过滤引用，并自动回退
				logger.Info("Applying citation entity filter",
					"pre_filter_count", len(citations),
					"canonical_name", canonicalName,
					"official_domains", domains,
					"alias_count", len(aliases),
				)
				filterResult := ApplyCitationFilterWithFallback(citations, canonicalName, aliases, domains)
				citations = filterResult.Citations
				if filterResult.Applied {
					logger.Info("Citation filter applied",
						"before", filterResult.Before,
						"after", filterResult.After,
						"retention", filterResult.Retention,
					)
				} else {
					logger.Warn("Citation filter too aggressive, keeping original",
						"before", filterResult.Before,
						"filtered", filterResult.After,
						"retention", filterResult.Retention,
					)
				}
			}
		}

		collectedCitations = citations
		if domainAnalysisResult != nil && len(domainAnalysisResult.Citations) > 0 {
			collectedCitations = mergeCitationsPreferFirst(domainAnalysisResult.Citations, collectedCitations)
		}

		if len(collectedCitations) > 0 {
			// Format into numbered list lines expected by FormatReportWithCitations
			// 格式化成 FormatReportWithCitations 期望的编号列表行
			var b strings.Builder
			for i, c := range collectedCitations {
				idx := i + 1
				title := c.Title
				if title == "" {
					title = c.Source
				}
				if c.PublishedDate != nil {
					fmt.Fprintf(&b, "[%d] %s (%s) - %s, %s\n", idx, title, c.URL, c.Source, c.PublishedDate.Format("2006-01-02"))
				} else {
					fmt.Fprintf(&b, "[%d] %s (%s) - %s\n", idx, title, c.URL, c.Source)
				}
			}
			baseContext["available_citations"] = strings.TrimRight(b.String(), "\n")
			baseContext["citation_count"] = len(collectedCitations)

			// Also store structured citations for SSE emission
			// 同时保存结构化引用供 SSE 发送
			out := make([]map[string]any, 0, len(collectedCitations))
			for _, c := range collectedCitations {
				out = append(out, map[string]any{
					"url":               c.URL,
					"title":             c.Title,
					"source":            c.Source,
					"credibility_score": c.CredibilityScore,
					"quality_score":     c.QualityScore,
					"tool_source":       c.ToolSource,
					"status_code":       c.StatusCode,
					"blocked_reason":    c.BlockedReason,
				})
			}
			baseContext["citations"] = out
		}
	}

	// Set synthesis style to comprehensive for research workflows
	// 研究工作流的合成风格设为 comprehensive
	baseContext["synthesis_style"] = "comprehensive"
	baseContext["research_areas_count"] = len(refineResult.ResearchAreas)

	// Synthesis tier: use large for deep/academic strategies, medium otherwise
	// 合成档位：deep/academic 策略用 large，其余用 medium
	// This ensures higher quality synthesis output for intensive research modes
	// 这样高强度的研究模式能获得更高质量的合成输出
	synthTier := "medium"
	if v, ok := baseContext["synthesis_model_tier"].(string); ok && strings.TrimSpace(v) != "" {
		// Explicit override takes precedence
		// 显式覆盖优先
		synthTier = strings.ToLower(strings.TrimSpace(v))
	} else {
		// Auto-select large tier for standard/deep/academic research strategies
		// 对 standard/deep/academic 研究策略自动选择 large 档
		if strategy, ok := baseContext["research_strategy"].(string); ok {
			switch strings.ToLower(strategy) {
			case "standard", "deep", "academic":
				synthTier = "large"
				logger.Info("Using large model tier for synthesis",
					"strategy", strategy,
				)
			}
		}
	}
	baseContext["model_tier"] = synthTier

	var synthesis activities.SynthesisResult
	err = workflow.ExecuteActivity(ctx,
		activities.SynthesizeResultsLLM,
		activities.SynthesisInput{
			// Pass original query through; synthesis embeds its own
			// 原样传入原始查询；合成阶段自带
			// language instruction (post-synthesis validation removed)
			// 语言指令（综合后校验已移除）
			Query:        input.Query,
			AgentResults: agentResults,
			// Ensure comprehensive report style for research synthesis unless already specified
			// 研究合成统一使用 comprehensive 报告风格（除非已指定）
			Context: func() map[string]any {
				if baseContext == nil {
					baseContext = map[string]any{}
				}
				if _, ok := baseContext["synthesis_style"]; !ok {
					baseContext["synthesis_style"] = "comprehensive"
				}
				return baseContext
			}(),
			CollectedCitations: collectedCitations,
			ParentWorkflowID:   input.ParentWorkflowID,
		}).Get(ctx, &synthesis)

	if err != nil {
		logger.Error("Synthesis failed", "error", err)
		return TaskResult{
			Success:      false,
			ErrorMessage: fmt.Sprintf("Failed to synthesize results: %v", err),
			Metadata:     buildResearchFailureMetadata("synthesis", totalTokens, len(agentResults), ""),
		}, err
	}

	totalTokens += synthesis.TokensUsed

	// Record synthesis token usage
	// 记录合成的 token 用量
	if synthesis.TokensUsed > 0 {
		inTok := synthesis.InputTokens
		outTok := synthesis.CompletionTokens
		if inTok == 0 && outTok > 0 {
			// Infer if needed
			// 必要时推断
			est := synthesis.TokensUsed - outTok
			if est > 0 {
				inTok = est
			}
		}
		recCtx := opts.WithTokenRecordOptions(ctx)
		_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
			UserID:       input.UserID,
			SessionID:    input.SessionID,
			TaskID:       workflowID,
			AgentID:      "synthesis",
			Model:        synthesis.ModelUsed,
			Provider:     synthesis.Provider,
			InputTokens:  inTok,
			OutputTokens: outTok,
			Metadata: map[string]any{
				"phase": "synthesis",
			},
		}).Get(recCtx, nil)
	}

	// Step 3.5: Deep Research 2.0 Iterative Loop OR legacy gap-filling
	// 步骤 3.5：Deep Research 2.0 迭代循环 或 旧的缺口填补
	// Check for iterative research mode first (new Deep Research 2.0 approach)
	// 先检查是否为迭代研究模式（Deep Research 2.0 的新做法）
	// Deep Research 2.0 is now enabled by default; set iterative_research_enabled=false in context to disable
	// Deep Research 2.0 默认开启；在 context 中设 iterative_research_enabled=false 可关闭。
	iterativeResearchVersion := workflow.GetVersion(ctx, "iterative_research_v1", workflow.DefaultVersion, 1)
	iterativeEnabled := true // Default to true for Deep Research 2.0 注：Deep Research 2.0 默认开启
	iterativeEnabledExplicit := false
	if v, ok := baseContext["iterative_research_enabled"]; ok {
		iterativeEnabledExplicit = true
		if b, ok := v.(bool); ok {
			iterativeEnabled = b
		}
	}

	// Fallback: quick strategy disables iterative loop (same pattern as gap_filling at line 5509)
	// 兜底：quick 策略关闭迭代循环（与第 5509 行 gap_filling 的处理一致）
	if !iterativeEnabledExplicit {
		strategy := ""
		if sv, ok := baseContext["research_strategy"].(string); ok {
			strategy = strings.ToLower(strings.TrimSpace(sv))
		}
		if strategy == "quick" {
			iterativeEnabled = false
			logger.Info("Iterative research disabled for quick strategy")
		}
	}

	if iterativeResearchVersion >= 1 && iterativeEnabled {
		// Deep Research 2.0: New iterative loop with coverage evaluation
		// Deep Research 2.0：带覆盖度评估的新迭代循环
		maxIterations := 3 // default 注：默认值
		if v, ok := baseContext["iterative_max_iterations"]; ok {
			switch t := v.(type) {
			case int:
				maxIterations = t
			case float64:
				maxIterations = int(t)
			}
			if maxIterations < 1 {
				maxIterations = 1
			}
			if maxIterations > 5 {
				maxIterations = 5
			}
		}

		logger.Info("Deep Research 2.0: Starting iterative research loop",
			"max_iterations", maxIterations,
		)

		// Track if gap-filling actually added new results
		// 记录缺口填补是否真的带来了新结果
		gapFillingOccurred := false

		// Extract research dimensions from refinement
		// 从细化结果中提取研究维度
		var researchDimensions []activities.ResearchDimension
		if dims, ok := baseContext["research_dimensions"]; ok {
			if dimSlice, ok := dims.([]activities.ResearchDimension); ok {
				researchDimensions = dimSlice
			} else if dimSlice, ok := dims.([]any); ok {
				// Convert from interface slice
				// 从 interface slice 转换
				for _, d := range dimSlice {
					if dm, ok := d.(map[string]any); ok {
						dim := activities.ResearchDimension{}
						if v, ok := dm["dimension"].(string); ok {
							dim.Dimension = v
						}
						if v, ok := dm["priority"].(string); ok {
							dim.Priority = v
						}
						if qs, ok := dm["questions"].([]any); ok {
							for _, q := range qs {
								if s, ok := q.(string); ok {
									dim.Questions = append(dim.Questions, s)
								}
							}
						}
						if sts, ok := dm["source_types"].([]any); ok {
							for _, st := range sts {
								if s, ok := st.(string); ok {
									dim.SourceTypes = append(dim.SourceTypes, s)
								}
							}
						}
						researchDimensions = append(researchDimensions, dim)
					}
				}
			}
		}

		currentSynthesis := synthesis.FinalResult
		currentKeyFindings := []string{}
		currentCoveredAreas := []string{}

		for iteration := 1; iteration <= maxIterations; iteration++ {
			// Check pause/cancel at start of each iteration
			// 每次迭代开始时检查暂停/取消
			if err := controlHandler.CheckPausePoint(ctx, fmt.Sprintf("pre_iteration_%d", iteration)); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("iterative_research", totalTokens, len(agentResults), currentSynthesis)}, err
			}

			logger.Info("Deep Research 2.0: Iteration start",
				"iteration", iteration,
				"max_iterations", maxIterations,
			)

			// Step 3.5.1: Intermediate synthesis (combine results so far)
			// 步骤 3.5.1：中间合成（合并目前已有的结果）
			if iteration > 1 && len(agentResults) > 0 {
				var intermediateSynthResult activities.IntermediateSynthesisResult
				err := workflow.ExecuteActivity(ctx,
					"IntermediateSynthesis",
					activities.IntermediateSynthesisInput{
						Query:             refinedQuery,
						Iteration:         iteration,
						MaxIterations:     maxIterations,
						AgentResults:      agentResults,
						PreviousSynthesis: currentSynthesis,
						CoverageGaps:      []string{}, // Will be filled by coverage evaluator 注：由覆盖度评估器填充
						Context:           baseContext,
						ParentWorkflowID:  input.ParentWorkflowID,
					}).Get(ctx, &intermediateSynthResult)

				if err == nil {
					currentSynthesis = intermediateSynthResult.Synthesis
					currentKeyFindings = intermediateSynthResult.KeyFindings
					currentCoveredAreas = intermediateSynthResult.CoverageAreas
					totalTokens += intermediateSynthResult.TokensUsed
					if intermediateSynthResult.TokensUsed > 0 || intermediateSynthResult.InputTokens > 0 || intermediateSynthResult.OutputTokens > 0 {
						inTok := intermediateSynthResult.InputTokens
						outTok := intermediateSynthResult.OutputTokens
						if inTok == 0 && outTok == 0 && intermediateSynthResult.TokensUsed > 0 {
							inTok = int(float64(intermediateSynthResult.TokensUsed) * 0.6)
							outTok = intermediateSynthResult.TokensUsed - inTok
						}
						recCtx := opts.WithTokenRecordOptions(ctx)
						_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
							UserID:       input.UserID,
							SessionID:    input.SessionID,
							TaskID:       workflowID,
							AgentID:      "intermediate_synthesis",
							Model:        intermediateSynthResult.ModelUsed,
							Provider:     intermediateSynthResult.Provider,
							InputTokens:  inTok,
							OutputTokens: outTok,
							Metadata:     map[string]any{"phase": "intermediate_synthesis"},
						}).Get(recCtx, nil)
					}

					logger.Info("Deep Research 2.0: Intermediate synthesis complete",
						"iteration", iteration,
						"confidence", intermediateSynthResult.ConfidenceScore,
						"key_findings", len(currentKeyFindings),
					)

					// Early exit if confidence is high enough
					// 置信度足够高时提前退出
					if intermediateSynthResult.ConfidenceScore >= 0.85 && !intermediateSynthResult.NeedsMoreResearch {
						logger.Info("Deep Research 2.0: High confidence achieved, exiting loop early",
							"iteration", iteration,
							"confidence", intermediateSynthResult.ConfidenceScore,
						)
						break
					}
				} else {
					logger.Warn("Deep Research 2.0: Intermediate synthesis failed", "error", err)
				}
			}

			// Check pause/cancel before coverage evaluation
			// 覆盖度评估前检查暂停/取消
			if err := controlHandler.CheckPausePoint(ctx, fmt.Sprintf("pre_coverage_eval_%d", iteration)); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("iterative_research", totalTokens, len(agentResults), currentSynthesis)}, err
			}

			// Step 3.5.2: Evaluate coverage and identify gaps
			// 步骤 3.5.2：评估覆盖度并识别缺口
			var coverageResult activities.CoverageEvaluationResult
			err := workflow.ExecuteActivity(ctx,
				"EvaluateCoverage",
				activities.CoverageEvaluationInput{
					Query:              refinedQuery,
					ResearchDimensions: researchDimensions,
					CurrentSynthesis:   currentSynthesis,
					CoveredAreas:       currentCoveredAreas,
					KeyFindings:        currentKeyFindings,
					Iteration:          iteration,
					MaxIterations:      maxIterations,
					Context:            baseContext,
					ParentWorkflowID:   input.ParentWorkflowID,
				}).Get(ctx, &coverageResult)

			if err != nil {
				logger.Warn("Deep Research 2.0: Coverage evaluation failed", "error", err)
				break // Exit loop on error 注：出错时退出循环
			}

			totalTokens += coverageResult.TokensUsed
			if coverageResult.TokensUsed > 0 || coverageResult.InputTokens > 0 || coverageResult.OutputTokens > 0 {
				inTok := coverageResult.InputTokens
				outTok := coverageResult.OutputTokens
				if inTok == 0 && outTok == 0 && coverageResult.TokensUsed > 0 {
					inTok = int(float64(coverageResult.TokensUsed) * 0.6)
					outTok = coverageResult.TokensUsed - inTok
				}
				recCtx := opts.WithTokenRecordOptions(ctx)
				_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
					UserID:       input.UserID,
					SessionID:    input.SessionID,
					TaskID:       workflowID,
					AgentID:      "coverage_evaluator",
					Model:        coverageResult.ModelUsed,
					Provider:     coverageResult.Provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     map[string]any{"phase": "coverage_eval"},
				}).Get(recCtx, nil)
			}

			logger.Info("Deep Research 2.0: Coverage evaluation complete",
				"iteration", iteration,
				"overall_coverage", coverageResult.OverallCoverage,
				"critical_gaps", len(coverageResult.CriticalGaps),
				"should_continue", coverageResult.ShouldContinue,
				"recommended_action", coverageResult.RecommendedAction,
			)

			// Check if we should stop
			// 判断是否应停止
			if !coverageResult.ShouldContinue || coverageResult.RecommendedAction == "complete" {
				logger.Info("Deep Research 2.0: Coverage sufficient, exiting loop",
					"iteration", iteration,
					"coverage", coverageResult.OverallCoverage,
				)
				break
			}

			// No critical gaps to fill - exit
			// 没有关键缺口需要填补——退出
			if len(coverageResult.CriticalGaps) == 0 && len(coverageResult.OptionalGaps) == 0 {
				logger.Info("Deep Research 2.0: No gaps identified, exiting loop",
					"iteration", iteration,
				)
				break
			}

			// Check pause/cancel before subquery generation
			// 生成子查询前检查暂停/取消
			if err := controlHandler.CheckPausePoint(ctx, fmt.Sprintf("pre_subquery_gen_%d", iteration)); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("iterative_research", totalTokens, len(agentResults), currentSynthesis)}, err
			}

			// Step 3.5.3: Generate subqueries to fill gaps
			// 步骤 3.5.3：生成用于填补缺口的子查询
			allGaps := append(coverageResult.CriticalGaps, coverageResult.OptionalGaps...)
			var subqueryResult activities.SubqueryGeneratorResult
			err = workflow.ExecuteActivity(ctx,
				"GenerateSubqueries",
				activities.SubqueryGeneratorInput{
					Query:            refinedQuery,
					CoverageGaps:     allGaps,
					CurrentSynthesis: currentSynthesis,
					Iteration:        iteration,
					MaxSubqueries:    3, // Limit to 3 per iteration 注：每轮最多 3 个
					Context:          baseContext,
					ParentWorkflowID: input.ParentWorkflowID,
					// Deep Research 2.0: Source-type aware search
					// Deep Research 2.0：按来源类型搜索
					EntityName:      refineResult.CanonicalName,
					QueryType:       refineResult.QueryType,
					TargetLanguages: refineResult.TargetLanguages,
				}).Get(ctx, &subqueryResult)

			if err != nil {
				logger.Warn("Deep Research 2.0: Subquery generation failed", "error", err)
				break
			}

			totalTokens += subqueryResult.TokensUsed
			if subqueryResult.TokensUsed > 0 || subqueryResult.InputTokens > 0 || subqueryResult.OutputTokens > 0 {
				inTok := subqueryResult.InputTokens
				outTok := subqueryResult.OutputTokens
				if inTok == 0 && outTok == 0 && subqueryResult.TokensUsed > 0 {
					inTok = int(float64(subqueryResult.TokensUsed) * 0.6)
					outTok = subqueryResult.TokensUsed - inTok
				}
				recCtx := opts.WithTokenRecordOptions(ctx)
				_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
					UserID:       input.UserID,
					SessionID:    input.SessionID,
					TaskID:       workflowID,
					AgentID:      "subquery_generator",
					Model:        subqueryResult.ModelUsed,
					Provider:     subqueryResult.Provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     map[string]any{"phase": "subquery_generation"},
				}).Get(recCtx, nil)
			}

			if len(subqueryResult.Subqueries) == 0 {
				logger.Info("Deep Research 2.0: No subqueries generated, exiting loop",
					"iteration", iteration,
				)
				break
			}

			logger.Info("Deep Research 2.0: Generated gap-filling subqueries",
				"iteration", iteration,
				"count", len(subqueryResult.Subqueries),
			)

			// Check pause/cancel before gap-filling execution
			// 缺口填补执行前检查暂停/取消
			if err := controlHandler.CheckPausePoint(ctx, fmt.Sprintf("pre_gap_filling_%d", iteration)); err != nil {
				return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("iterative_research", totalTokens, len(agentResults), currentSynthesis)}, err
			}

			// Step 3.5.4: Execute gap-filling agents in parallel
			// 步骤 3.5.4：并行执行缺口填补 agent
			type gapAgentResult struct {
				Results []activities.AgentExecutionResult
				Tokens  int
			}
			gapChan := workflow.NewChannel(ctx)
			numGapAgents := len(subqueryResult.Subqueries)
			sem := workflow.NewSemaphore(ctx, 3) // Max 3 concurrent 注：最多 3 个并发

			for _, sq := range subqueryResult.Subqueries {
				workflow.Go(ctx, func(gctx workflow.Context) {
					if err := sem.Acquire(gctx, 1); err != nil {
						var empty gapAgentResult
						gapChan.Send(gctx, empty)
						return
					}
					defer sem.Release(1)

					// Build context with task contract
					// 构造带任务契约的 context
					gapContext := make(map[string]any)
					maps.Copy(gapContext, baseContext)
					gapContext["research_mode"] = "gap_fill"
					gapContext["target_gap"] = sq.TargetGap
					gapContext["iteration"] = iteration
					// Override model_tier to agent tier; synthesis sets "large" in
					// 把 model_tier 覆盖为 agent 档位；合成阶段会在
					// baseContext which must not bleed into gap-filling agents.
					// baseContext 中设成 "large"，不能渗到缺口填补 agent。
					gapContext["model_tier"] = modelTier

					// Apply task contract if present
					// 如有任务契约则应用
					if sq.SourceGuidance != nil {
						gapContext["source_guidance"] = map[string]any{
							"required": sq.SourceGuidance.Required,
							"optional": sq.SourceGuidance.Optional,
							"avoid":    sq.SourceGuidance.Avoid,
						}

						// Generate search routing plan based on source guidance
						// 按来源指引生成搜索路由方案
						var routeResult activities.SearchRouteResult
						routeErr := workflow.ExecuteActivity(gctx,
							"RouteSearch",
							activities.SearchRouteInput{
								Query:       sq.Query,
								Dimension:   sq.TargetGap,
								SourceTypes: sq.SourceGuidance.Required,
								Priority:    "high", // Gap-filling is high priority 注：缺口填补为高优先级
								Context:     gapContext,
							}).Get(gctx, &routeResult)

						if routeErr == nil && len(routeResult.Routes) > 0 {
							gapContext["search_routes"] = routeResult.Routes
							gapContext["search_strategy"] = routeResult.Strategy
							logger.Info("Deep Research 2.0: Search routing plan generated",
								"query", sq.Query,
								"routes", len(routeResult.Routes),
								"strategy", routeResult.Strategy,
							)
						} else {
							logger.Warn("Deep Research 2.0: Failed to generate search routing plan",
								"query", sq.Query,
								"error", routeErr,
							)
						}
					}
					if sq.OutputFormat != nil {
						gapContext["output_format"] = map[string]any{
							"type":            sq.OutputFormat.Type,
							"required_fields": sq.OutputFormat.RequiredFields,
							"optional_fields": sq.OutputFormat.OptionalFields,
						}
					}
					if sq.SearchBudget != nil {
						gapContext["search_budget"] = map[string]any{
							"max_queries": sq.SearchBudget.MaxQueries,
							"max_fetches": sq.SearchBudget.MaxFetches,
						}
					}
					if sq.Boundaries != nil {
						gapContext["boundaries"] = map[string]any{
							"in_scope":     sq.Boundaries.InScope,
							"out_of_scope": sq.Boundaries.OutOfScope,
						}
					}

					// Execute agent with ReactLoop for deeper exploration
					// 用 ReactLoop 执行 agent 以做更深探索
					reactConfig := patterns.ReactConfig{
						MaxIterations:     2,
						MinIterations:     1,
						ObservationWindow: 3,
						MaxObservations:   10,
						MaxThoughts:       5,
						MaxActions:        5,
					}
					reactOpts := patterns.Options{
						BudgetAgentMax: agentMaxTokens,
						SessionID:      input.SessionID,
						UserID:         input.UserID,
						ModelTier:      modelTier,
						Context:        gapContext,
					}

					reactResult, err := patterns.ReactLoop(
						gctx,
						sq.Query,
						gapContext,
						input.SessionID,
						[]string{},
						reactConfig,
						reactOpts,
					)

					payload := gapAgentResult{}
					if err == nil && len(reactResult.AgentResults) > 0 {
						payload.Results = reactResult.AgentResults
						payload.Tokens = reactResult.TotalTokens
					}
					gapChan.Send(gctx, payload)
				})
			}

			// Collect gap-filling results with value gating (P1-1)
			// 收集缺口填补结果并做价值门控（P1-1）
			// Initialize URL tracking from existing agent results
			// 用已有 agent 结果初始化 URL 追踪
			existingURLs := make(map[string]bool)
			existingDomains := make(map[string]bool)
			updateExistingURLs(agentResults, existingURLs, existingDomains)

			gapMetrics := GapFillMetrics{TotalAgents: numGapAgents}

			for i := range numGapAgents {
				var payload gapAgentResult
				gapChan.Receive(ctx, &payload)

				if len(payload.Results) > 0 {
					// Evaluate incremental value before accepting
					// 接受前先评估增量价值
					accept, newURLs, newDomains := evaluateGapFillValue(
						payload.Results, existingURLs, existingDomains,
					)

					if accept {
						agentResults = append(agentResults, payload.Results...)
						originalAgentResults = append(originalAgentResults, payload.Results...)
						totalTokens += payload.Tokens
						gapFillingOccurred = true

						gapMetrics.AcceptedAgents++
						gapMetrics.NewURLsFound += newURLs
						gapMetrics.NewDomainsFound += newDomains

						// Update tracking sets
						// 更新追踪集合
						updateExistingURLs(payload.Results, existingURLs, existingDomains)
					} else {
						gapMetrics.SkippedLowValue++
						logger.Info("Gap-fill result skipped (low incremental value)",
							"new_urls", newURLs,
							"new_domains", newDomains,
							"agent_index", i,
						)
					}
				}

				// Early exit: saturation reached (enough evidence collected)
				// 提前退出：已达饱和（收集到足够证据）
				if gapMetrics.NewDomainsFound >= 5 || gapMetrics.NewURLsFound >= 15 {
					logger.Info("Gap-fill saturation reached, stopping early",
						"new_domains", gapMetrics.NewDomainsFound,
						"new_urls", gapMetrics.NewURLsFound,
						"agents_processed", i+1,
						"agents_remaining", numGapAgents-i-1,
					)
					break
				}
			}

			logger.Info("Deep Research 2.0: Gap-filling iteration complete",
				"iteration", iteration,
				"total_agent_results", len(agentResults),
				"gap_agents_total", gapMetrics.TotalAgents,
				"gap_agents_accepted", gapMetrics.AcceptedAgents,
				"gap_agents_skipped", gapMetrics.SkippedLowValue,
				"new_urls_found", gapMetrics.NewURLsFound,
				"new_domains_found", gapMetrics.NewDomainsFound,
			)
		}

		// After iterative loop, re-collect citations (always) and re-synthesize (only if gap-filling occurred)
		// 迭代循环结束后，重新收集引用（总是执行），并重新合成（仅当发生过缺口填补）
		if len(agentResults) > 0 {
			var resultsForCitations []any
			for _, ar := range originalAgentResults {
				var toolExecs []any
				for _, te := range ar.ToolExecutions {
					toolExecs = append(toolExecs, map[string]any{
						"tool":    te.Tool,
						"success": te.Success,
						"output":  te.Output,
						"error":   te.Error,
					})
				}
				resultsForCitations = append(resultsForCitations, map[string]any{
					"agent_id":        ar.AgentID,
					"tool_executions": toolExecs,
					"response":        ar.Response,
				})
			}

			now := workflow.Now(ctx)
			updatedCitations, _ := metadata.CollectCitations(resultsForCitations, now, 0)
			if len(updatedCitations) > 0 {
				// Apply entity-based filtering consistently (same as initial collection)
				// 一致地应用基于实体的过滤（与首次收集时相同）
				canonicalName, _ := baseContext["canonical_name"].(string)
				if canonicalName != "" {
					var domains []string
					if d, ok := baseContext["official_domains"].([]string); ok {
						domains = d
					}
					var aliases []string
					if eq, ok := baseContext["exact_queries"].([]string); ok {
						aliases = eq
					}
					filterResult := ApplyCitationFilterWithFallback(updatedCitations, canonicalName, aliases, domains)
					updatedCitations = filterResult.Citations
					if filterResult.Applied {
						logger.Info("Gap-filling citation filter applied",
							"before", filterResult.Before,
							"after", filterResult.After,
							"retention", filterResult.Retention,
						)
					} else {
						logger.Warn("Gap-filling citation filter too aggressive, keeping original",
							"before", filterResult.Before,
							"filtered", filterResult.After,
							"retention", filterResult.Retention,
						)
					}
				}
				collectedCitations = updatedCitations

				// Update context with new citations
				// 用新引用更新 context
				var b strings.Builder
				for idx, c := range updatedCitations {
					title := c.Title
					if title == "" {
						title = c.Source
					}
					if c.PublishedDate != nil {
						fmt.Fprintf(&b, "[%d] %s (%s) - %s, %s\n", idx+1, title, c.URL, c.Source, c.PublishedDate.Format("2006-01-02"))
					} else {
						fmt.Fprintf(&b, "[%d] %s (%s) - %s\n", idx+1, title, c.URL, c.Source)
					}
				}
				baseContext["available_citations"] = strings.TrimRight(b.String(), "\n")
				baseContext["citation_count"] = len(updatedCitations)
			}

			// Only re-synthesize if gap-filling actually added new research results
			// 仅当缺口填补确实带来新的研究结果时才重新合成
			// Skip if no gaps were filled to avoid degrading already-good synthesis
			// 若没有填补任何缺口则跳过，避免把已经不错的合成结果改坏
			if gapFillingOccurred {
				var finalSynthesis activities.SynthesisResult
				err := workflow.ExecuteActivity(ctx,
					activities.SynthesizeResultsLLM,
					activities.SynthesisInput{
						Query:              input.Query,
						AgentResults:       agentResults,
						Context:            baseContext,
						CollectedCitations: collectedCitations,
						ParentWorkflowID:   input.ParentWorkflowID,
					}).Get(ctx, &finalSynthesis)

				if err == nil {
					synthesis = finalSynthesis
					totalTokens += finalSynthesis.TokensUsed
					logger.Info("Deep Research 2.0: Final synthesis complete (with gap-filling)",
						"total_citations", len(collectedCitations),
					)
				}
			} else {
				logger.Info("Deep Research 2.0: Skipping final re-synthesis (no gap-filling occurred)",
					"total_citations", len(collectedCitations),
				)
			}
		}

		logger.Info("Deep Research 2.0: Iterative loop complete",
			"total_tokens", totalTokens,
			"total_agents", len(agentResults),
		)

	} else {
		// Legacy gap-filling loop (backward compatibility)
		// 旧的缺口填补循环（向后兼容）
		// Version-gated for safe rollout and Temporal determinism
		// 版本门控，以便安全上线并保证 Temporal 确定性
		gapFillingVersion := workflow.GetVersion(ctx, "gap_filling_v1", workflow.DefaultVersion, 1)
		if gapFillingVersion >= 1 {
			// Check if gap_filling_enabled is explicitly set in context
			// 检查 context 中是否显式设置了 gap_filling_enabled
			gapEnabled := true // default to enabled for backward compat 注：默认开启以保持向后兼容
			gapEnabledExplicit := false
			if v, ok := baseContext["gap_filling_enabled"]; ok {
				gapEnabledExplicit = true
				if b, ok := v.(bool); ok {
					gapEnabled = b
				}
			}

			// If not explicitly set, use legacy strategy-based logic (backward compat)
			// 未显式设置时，沿用旧的基于策略的逻辑（向后兼容）
			if !gapEnabledExplicit {
				strategy := ""
				if sv, ok := baseContext["research_strategy"].(string); ok {
					strategy = strings.ToLower(strings.TrimSpace(sv))
				}
				if strategy == "quick" {
					gapEnabled = false
					logger.Info("Gap-filling disabled for quick strategy (legacy logic)")
				}
			}

			// Skip gap-filling if disabled
			// 已禁用则跳过缺口填补
			if !gapEnabled {
				logger.Info("Gap-filling disabled via configuration")
			} else {
				// Check iteration count from context (prevents infinite loops)
				// 从 context 读取迭代次数（防止死循环）
				iterationCount := 0
				if baseContext != nil {
					if v, ok := baseContext["gap_iteration"].(int); ok {
						iterationCount = v
					}
				}

				// Read max iterations from context with fallback to default and clamping
				// 从 context 读取最大迭代次数，缺省则用默认值并做钳制
				maxGapIterations := 2 // default 注：默认值
				if v, ok := baseContext["gap_filling_max_iterations"]; ok {
					switch t := v.(type) {
					case int:
						maxGapIterations = t
					case float64:
						maxGapIterations = int(t)
					}
					// Clamp to reasonable range
					// 钳制到合理范围
					if maxGapIterations < 1 {
						maxGapIterations = 1
					}
					if maxGapIterations > 5 {
						maxGapIterations = 5
					}
				}

				// Only attempt gap-filling if we haven't exceeded max iterations
				// 仅在未超过最大迭代次数时尝试缺口填补
				if iterationCount < maxGapIterations {
					// Version gate for CJK gap detection phrases (for Temporal replay determinism)
					// 对 CJK 缺口检测短语做版本门控（保证 Temporal 重放确定性）
					cjkGapPhrasesVersion := workflow.GetVersion(ctx, "cjk_gap_phrases_v1", workflow.DefaultVersion, 1)
					if cjkGapPhrasesVersion >= 1 {
						baseContext["enable_cjk_gap_phrases"] = true
					}

					// Strategy-aware gap detection (pass baseContext instead of strategy string)
					// 感知策略的缺口检测（传入 baseContext 而非 strategy 字符串）
					gapAnalysis := analyzeGaps(synthesis.FinalResult, refineResult.ResearchAreas, baseContext)

					if len(gapAnalysis.UndercoveredAreas) > 0 {
						logger.Info("Detected coverage gaps; triggering targeted re-search",
							"gaps", gapAnalysis.UndercoveredAreas,
							"iteration", iterationCount,
						)

						// Build targeted search queries for gaps
						// 为缺口构造定向搜索查询
						gapQueries := buildGapQueries(gapAnalysis.UndercoveredAreas, input.Query)

						// Execute targeted searches in parallel using Temporal-safe channels
						// 用 Temporal 安全的 channel 并行执行定向搜索
						var allGapResults []activities.AgentExecutionResult
						var gapTotalTokens int

						// Define payload type once (shared between send and receive)
						// 只定义一次 payload 类型（发送与接收共用）
						type gapResultPayload struct {
							Results []activities.AgentExecutionResult
							Tokens  int
						}

						// Use Temporal-safe channel to collect gap results
						// 用 Temporal 安全的 channel 收集缺口结果
						gapResultsChan := workflow.NewChannel(ctx)
						numGapQueries := len(gapQueries)

						// Concurrency cap: limit in-flight gap searches to 3
						// 并发上限：同时进行的缺口搜索不超过 3
						sem := workflow.NewSemaphore(ctx, 3)

						for _, gapQuery := range gapQueries {

							workflow.Go(ctx, func(gctx workflow.Context) {

								// Acquire a permit; on failure, send empty payload to keep counts balanced
								// 申请许可；失败时发送空 payload 以保持计数平衡
								if err := sem.Acquire(gctx, 1); err != nil {
									var empty gapResultPayload
									gapResultsChan.Send(gctx, empty)
									return
								}
								defer sem.Release(1)

								gapContext := make(map[string]any)
								maps.Copy(gapContext, baseContext)
								gapContext["research_mode"] = "gap_fill"
								gapContext["target_area"] = gapQuery.TargetArea
								gapContext["gap_iteration"] = iterationCount + 1
								// Override model_tier to agent tier; synthesis sets "large" in
								// 把 model_tier 覆盖为 agent 档位；合成阶段会在
								// baseContext which must not bleed into gap-filling agents.
								// baseContext 中设成 "large"，不能渗到缺口填补 agent。
								gapContext["model_tier"] = modelTier

								// Use react_max_iterations from context if provided, default to 2 for gap-filling efficiency
								// 若 context 提供了 react_max_iterations 则使用，否则默认 2 以保证缺口填补效率
								gapReactMaxIterations := 2
								if v, ok := baseContext["react_max_iterations"]; ok {
									switch t := v.(type) {
									case int:
										gapReactMaxIterations = t
									case float64:
										gapReactMaxIterations = int(t)
									}
									// Clamp to reasonable range
									// 钳制到合理范围
									if gapReactMaxIterations < 1 {
										gapReactMaxIterations = 1
									}
									if gapReactMaxIterations > 10 {
										gapReactMaxIterations = 10
									}
								}

								reactConfig := patterns.ReactConfig{
									MaxIterations:     gapReactMaxIterations, // Respect react_max_iterations from strategy 注：遵循策略中的 react_max_iterations
									MinIterations:     2,
									ObservationWindow: 3,  // Keep last 3 observations in context 注：context 中保留最近 3 条观察
									MaxObservations:   20, // Prevent unbounded growth 注：防止无界增长
									MaxThoughts:       10,
									MaxActions:        10,
								}
								reactOpts := patterns.Options{
									BudgetAgentMax: agentMaxTokens,
									SessionID:      input.SessionID,
									ModelTier:      modelTier,
									Context:        gapContext,
								}

								gapResult, err := patterns.ReactLoop(
									gctx,
									gapQuery.Query,
									gapContext,
									input.SessionID,
									[]string{}, // No history for gap queries 注：缺口查询不带历史
									reactConfig,
									reactOpts,
								)

								// Send result to channel
								// 把结果发送到 channel
								payload := gapResultPayload{}
								if err == nil && len(gapResult.AgentResults) > 0 {
									payload.Results = gapResult.AgentResults
									payload.Tokens = gapResult.TotalTokens
								}
								gapResultsChan.Send(gctx, payload)
							})
						}

						// Collect all gap results from channel (Temporal-safe)
						// 从 channel 收集全部缺口结果（Temporal 安全）
						for range numGapQueries {
							var payload gapResultPayload
							gapResultsChan.Receive(ctx, &payload)
							if len(payload.Results) > 0 {
								allGapResults = append(allGapResults, payload.Results...)
								gapTotalTokens += payload.Tokens
							}
						}
						totalTokens += gapTotalTokens

						// If we got new evidence, re-collect citations and re-synthesize
						// 若拿到新证据，则重新收集引用并重新合成
						if len(allGapResults) > 0 {
							logger.Info("Gap-filling search completed",
								"gap_results", len(allGapResults),
								"iteration", iterationCount+1,
							)

							// Combine for synthesis (filtered) and for citations (unfiltered)
							// 合成用（过滤后）结果，引用用（未过滤）结果
							// Synthesis uses filtered agentResults to keep reasoning on-entity.
							// 合成使用过滤后的 agentResults，使推理聚焦实体。
							// Citations use the original (unfiltered) results to maximize evidence.
							// 引用使用原始（未过滤）结果，以最大化证据。
							// 合成使用过滤后的结果，使推理聚焦实体；引用使用未过滤的原始结果，以最大化证据。
							combinedAgentResults := append(allGapResults, agentResults...)
							combinedForCitations := append(allGapResults, originalAgentResults...)

							var resultsForCitations []any
							for _, ar := range combinedForCitations {
								var toolExecs []any
								if len(ar.ToolExecutions) > 0 {
									for _, te := range ar.ToolExecutions {
										toolExecs = append(toolExecs, map[string]any{
											"tool":    te.Tool,
											"success": te.Success,
											"output":  te.Output,
											"error":   te.Error,
										})
									}
								}
								resultsForCitations = append(resultsForCitations, map[string]any{
									"agent_id":        ar.AgentID,
									"tool_executions": toolExecs,
									"response":        ar.Response,
								})
							}

							now := workflow.Now(ctx)
							allCitations, _ := metadata.CollectCitations(resultsForCitations, now, 0) // Use 0 for default max (15) 注：用 0 表示默认上限 15

							// Apply entity-based filtering consistently
							// 一致地应用基于实体的过滤
							canonicalName, _ := baseContext["canonical_name"].(string)
							if canonicalName != "" && len(allCitations) > 0 {
								var domains []string
								if d, ok := baseContext["official_domains"].([]string); ok {
									domains = d
								}
								var aliases []string
								if eq, ok := baseContext["exact_queries"].([]string); ok {
									aliases = eq
								}
								filterResult := ApplyCitationFilterWithFallback(allCitations, canonicalName, aliases, domains)
								allCitations = filterResult.Citations
								if filterResult.Applied {
									logger.Info("Iterative gap-filling citation filter applied",
										"before", filterResult.Before,
										"after", filterResult.After,
										"retention", filterResult.Retention,
									)
								} else {
									logger.Warn("Iterative gap-filling citation filter too aggressive, keeping original",
										"before", filterResult.Before,
										"filtered", filterResult.After,
										"retention", filterResult.Retention,
									)
								}
							}

							if len(allCitations) > 0 {

								// Re-synthesize with augmented evidence
								// 用增强后的证据重新合成
								var enhancedSynthesis activities.SynthesisResult

								// Build enhanced context with new citations
								// 用新引用构造增强的 context
								enhancedContext := make(map[string]any)
								maps.Copy(enhancedContext, baseContext)
								enhancedContext["research_areas"] = refineResult.ResearchAreas
								enhancedContext["gap_iteration"] = iterationCount + 1
								// Inherit synthesis tier from initial synthesis (respects user override)
								// 继承初次合成的档位（尊重用户覆盖）
								if synthTier, ok := baseContext["model_tier"].(string); ok {
									enhancedContext["model_tier"] = synthTier
								}
								// Note: synthesis_model_tier override is already in baseContext, will be used
								// 注意：synthesis_model_tier 覆盖已在 baseContext 中，会被使用

								// Format citations for synthesis
								// 为合成格式化引用
								if len(allCitations) > 0 {
									var b strings.Builder
									for idx, c := range allCitations {
										title := c.Title
										if title == "" {
											title = c.Source
										}
										if c.PublishedDate != nil {
											fmt.Fprintf(&b, "[%d] %s (%s) - %s, %s\n", idx+1, title, c.URL, c.Source, c.PublishedDate.Format("2006-01-02"))
										} else {
											fmt.Fprintf(&b, "[%d] %s (%s) - %s\n", idx+1, title, c.URL, c.Source)
										}
									}
									enhancedContext["available_citations"] = strings.TrimRight(b.String(), "\n")
									enhancedContext["citation_count"] = len(allCitations)

									// Also store structured citations for SSE emission
									// 同时保存结构化引用供 SSE 发送
									out := make([]map[string]any, 0, len(allCitations))
									for _, c := range allCitations {
										out = append(out, map[string]any{
											"url":               c.URL,
											"title":             c.Title,
											"source":            c.Source,
											"credibility_score": c.CredibilityScore,
											"tool_source":       c.ToolSource,
											"status_code":       c.StatusCode,
											"blocked_reason":    c.BlockedReason,
											"quality_score":     c.QualityScore,
										})
									}
									enhancedContext["citations"] = out
								}

								err = workflow.ExecuteActivity(ctx,
									activities.SynthesizeResultsLLM,
									activities.SynthesisInput{
										Query:              input.Query,
										AgentResults:       combinedAgentResults, // Combined results with global dedup 注：合并后的结果（已全局去重）
										Context:            enhancedContext,
										CollectedCitations: allCitations,
										ParentWorkflowID:   input.ParentWorkflowID,
									}).Get(ctx, &enhancedSynthesis)

								if err == nil {
									synthesis = enhancedSynthesis
									collectedCitations = allCitations
									totalTokens += enhancedSynthesis.TokensUsed
									logger.Info("Gap-filling synthesis completed",
										"iteration", iterationCount+1,
										"total_citations", len(allCitations),
									)
								}
							}
						}
					} else {
						// Make it explicit that analysis ran but found nothing
						// 明确说明分析已执行但未发现缺口
						logger.Info("Gap analysis completed with no gaps detected",
							"iteration", iterationCount,
						)
					}
				}
			}
		}
	}

	// Check pause/cancel after synthesis/iteration - signal may have arrived during synthesis
	// 合成/迭代后检查暂停/取消——信号可能在合成期间到达
	if err := controlHandler.CheckPausePoint(ctx, "post_synthesis"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("post_synthesis", totalTokens, len(agentResults), synthesis.FinalResult)}, err
	}

	// Check pause/cancel before reflection
	// 反思前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_reflection"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("pre_reflection", totalTokens, len(agentResults), synthesis.FinalResult)}, err
	}

	// Step 4: Apply reflection pattern for quality improvement
	// 步骤 4：应用反思 pattern 提升质量
	// NOTE: Reflection runs BEFORE citation agent to prevent re-synthesis from losing citations
	// 反思必须在 Citation Agent 之前执行，否则重写会丢失引用。
	reflectionConfig := patterns.ReflectionConfig{
		Enabled:             true,
		MaxRetries:          2,
		ConfidenceThreshold: 0.8,
		Criteria:            []string{"accuracy", "completeness", "clarity"},
		TimeoutMs:           30000,
	}

	reflectionOpts := patterns.Options{
		BudgetAgentMax: agentMaxTokens,
		SessionID:      input.SessionID,
		ModelTier:      modelTier,
	}

	finalResult, qualityScore, reflectionTokens, err := patterns.ReflectOnResult(
		ctx,
		refinedQuery,
		synthesis.FinalResult,
		agentResults,
		baseContext,
		reflectionConfig,
		reflectionOpts,
	)

	if err != nil {
		logger.Warn("Reflection failed, using original result", "error", err)
		finalResult = synthesis.FinalResult
		qualityScore = 0.5
	}

	totalTokens += reflectionTokens

	// Check pause/cancel after reflection - signal may have arrived during reflection
	// 反思后检查暂停/取消——信号可能在反思期间到达
	if err := controlHandler.CheckPausePoint(ctx, "post_reflection"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("post_reflection", totalTokens, len(agentResults), synthesis.FinalResult)}, err
	}

	// Citation Agent: add inline citations to synthesis result (when enabled)
	// Citation Agent：为合成结果添加行内引用（启用时）
	// IMPORTANT: Runs AFTER reflection to ensure citations aren't lost during re-synthesis
	// Citation Agent 在反思之后运行，确保重写过程不会丢掉引用。
	citationAgentEnabled := true // Default: enabled 注：默认启用
	if v, ok := baseContext["enable_citation_agent"].(bool); ok {
		citationAgentEnabled = v
	}
	if citationAgentEnabled && len(collectedCitations) > 0 {
		logger.Info("CitationAgent: starting citation addition",
			"total_citations", len(collectedCitations),
		)

		// Step: Remove ## Sources section before passing to Citation Agent
		// 步骤：传给 Citation Agent 前移除 ## Sources 段落
		// (FormatReportWithCitations in synthesis.go may have added it already)
		// （synthesis.go 中的 FormatReportWithCitations 可能已加入该段）
		// The LLM may modify the Sources section (URL formatting, etc.), causing validation failure
		// LLM 可能改写 Sources 段落（URL 格式等），导致校验失败
		reportForCitation := finalResult // Use reflection output (was: synthesis.FinalResult) 注：使用反思输出（原为 synthesis.FinalResult）
		var extractedSources string
		if idx := strings.LastIndex(strings.ToLower(reportForCitation), "## sources"); idx != -1 {
			extractedSources = strings.TrimSpace(reportForCitation[idx:])
			reportForCitation = strings.TrimSpace(reportForCitation[:idx])
			logger.Info("CitationAgent: stripped Sources section before processing",
				"sources_length", len(extractedSources),
				"report_length", len(reportForCitation),
			)
		}

		var citationResult activities.CitationAgentResult
		citationCtx := workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
			StartToCloseTimeout: 360 * time.Second, // Extended for long reports with medium tier 注：为中等档的长报告延长超时
			RetryPolicy: &temporal.RetryPolicy{
				InitialInterval:    time.Second,
				BackoffCoefficient: 2.0,
				MaximumAttempts:    2,
			},
		})

		// P0-D: Metrics tracking for Citation V2 observability
		// P0-D：为 Citation V2 观测做的指标统计
		var citationMetrics struct {
			TotalCitations      int
			FetchOnlyCount      int
			ValidCount          int
			SupportedClaims     int
			TotalClaims         int
			V1SupplementEnabled bool
			V1Fallback          bool
			FinalCitationsUsed  int
		}
		citationMetrics.TotalCitations = len(collectedCitations)

		// ============================================================
		// ============================================================ 分隔线
		// Citation Agent: Add inline citations to report
		// Citation Agent：为报告添加行内引用
		// Simplified flow: LLM directly adds [n] markers to report
		// 简化流程：由 LLM 直接在报告中插入 [n] 标记
		// ============================================================
		// ============================================================ 分隔线
		logger.Info("CitationAgent: starting citation flow",
			"total_citations", len(collectedCitations),
			"report_length", len(reportForCitation),
		)

		// Convert to CitationForAgent
		// 转换为 CitationForAgent
		citationsForAgent := make([]activities.CitationForAgent, 0, len(collectedCitations))
		for _, c := range collectedCitations {
			citationsForAgent = append(citationsForAgent, activities.CitationForAgent{
				URL:              c.URL,
				Title:            c.Title,
				Source:           c.Source,
				Snippet:          c.Snippet,
				CredibilityScore: c.CredibilityScore,
				QualityScore:     c.QualityScore,
			})
		}

		// Dynamic model tier: use medium for longer reports (better instruction following)
		// 动态模型档位：较长的报告用 medium（指令遵循更好）
		// Lowered threshold from 20000 to 8000 to improve citation success rate
		// 阈值从 20000 降到 8000，以提高引用成功率
		citationModelTier := "small"
		if len(reportForCitation) > 8000 {
			citationModelTier = "medium"
		}

		logger.Info("CitationAgent: model tier selected",
			"report_length", len(reportForCitation),
			"model_tier", citationModelTier,
		)

		citationErr := workflow.ExecuteActivity(citationCtx, "AddCitations", activities.CitationAgentInput{
			Report:           reportForCitation,
			Citations:        citationsForAgent,
			ParentWorkflowID: input.ParentWorkflowID,
			Context:          baseContext,
			ModelTier:        citationModelTier,
		}).Get(citationCtx, &citationResult)

		if citationErr != nil {
			logger.Warn("CitationAgent: failed, using original synthesis", "error", citationErr)
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventProgress,
				AgentID:    "citation_agent",
				Message:    activities.MsgCitationSkipped(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
		} else if citationResult.ValidationPassed {
			// Use cited report and rebuild Sources section
			// 使用带引用的报告，并重建 Sources 段落
			citationsList := ""
			if v, ok := baseContext["available_citations"].(string); ok {
				citationsList = v
			}
			if citationsList != "" {
				finalResult = formatting.FormatReportWithCitations(citationResult.CitedReport, citationsList)
			} else {
				finalResult = citationResult.CitedReport
				if extractedSources != "" {
					finalResult = strings.TrimSpace(finalResult) + "\n\n" + extractedSources
				}
			}
			totalTokens += citationResult.TokensUsed
			citationMetrics.FinalCitationsUsed = len(citationResult.CitationsUsed)
			logger.Info("CitationAgent: completed successfully",
				"citations_used", len(citationResult.CitationsUsed),
				"model_tier", citationModelTier,
				"validation_passed", true,
			)
		} else {
			// Validation failed - return original report unchanged
			// 校验失败——原样返回原始报告
			logger.Warn("CitationAgent: validation failed, using original synthesis",
				"error", citationResult.ValidationError,
			)
			_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
				WorkflowID: workflowID,
				EventType:  activities.StreamEventProgress,
				AgentID:    "citation_agent",
				Message:    activities.MsgCitationSkipped(),
				Timestamp:  workflow.Now(ctx),
			}).Get(ctx, nil)
		}

		// Log citation metrics summary
		// 记录引用指标摘要
		logger.Info("CitationAgent: metrics summary",
			"total_citations", citationMetrics.TotalCitations,
			"final_citations_used", citationMetrics.FinalCitationsUsed,
			"model_tier", citationModelTier,
		)
	}

	// Check pause/cancel before verification
	// 校验前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_verification"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("pre_verification", totalTokens, len(agentResults), synthesis.FinalResult)}, err
	}

	// Optional: verify claims if enabled and we have citations
	// 可选：启用且有引用时校验 claim
	var verification activities.VerificationResult
	verifyEnabled := false
	if v, ok := baseContext["enable_verification"].(bool); ok {
		verifyEnabled = v
	}
	if verifyEnabled && len(collectedCitations) > 0 {
		// Convert citations to []interface{} of maps for VerifyClaimsActivity
		// 把引用转换为 []interface{} 的 map，供 VerifyClaimsActivity 使用
		var verCitations []any
		for _, c := range collectedCitations {
			m := map[string]any{
				"url":               c.URL,
				"title":             c.Title,
				"source":            c.Source,
				"content":           c.Snippet,
				"credibility_score": c.CredibilityScore,
				"quality_score":     c.QualityScore,
			}
			verCitations = append(verCitations, m)
		}
		verr := workflow.ExecuteActivity(ctx, "VerifyClaimsActivity", activities.VerifyClaimsInput{
			Answer:    finalResult,
			Citations: verCitations,
		}).Get(ctx, &verification)
		if verr != nil {
			logger.Warn("Claim verification failed, skipping verification metadata", "error", verr)
		}
	}

	// Step 5.5: Optional fact extraction
	// 步骤 5.5：可选的事实抽取
	var extractedFacts *activities.FactExtractionResult
	factExtractionEnabled := false
	if v, ok := baseContext["enable_fact_extraction"].(bool); ok {
		factExtractionEnabled = v
	}
	if factExtractionEnabled && finalResult != "" {
		logger.Info("Running fact extraction on synthesis result")

		// Convert citations to simplified format
		// 把引用转换为简化格式
		var simpleCitations []activities.FactExtractionCitation
		for _, c := range collectedCitations {
			simpleCitations = append(simpleCitations, activities.FactExtractionCitation{
				URL:    c.URL,
				Title:  c.Title,
				Source: c.Source,
			})
		}

		// Extract research areas
		// 提取研究领域
		var areas []string
		if len(refineResult.ResearchAreas) > 0 {
			areas = refineResult.ResearchAreas
		}

		err = workflow.ExecuteActivity(ctx, "ExtractFacts", activities.FactExtractionInput{
			Query:            input.Query,
			SynthesisResult:  finalResult,
			Citations:        simpleCitations,
			ResearchAreas:    areas,
			ParentWorkflowID: input.ParentWorkflowID,
		}).Get(ctx, &extractedFacts)

		if err != nil {
			logger.Warn("Fact extraction failed", "error", err)
		} else {
			totalTokens += extractedFacts.TokensUsed
			if extractedFacts.TokensUsed > 0 || extractedFacts.InputTokens > 0 || extractedFacts.OutputTokens > 0 {
				inTok := extractedFacts.InputTokens
				outTok := extractedFacts.OutputTokens
				if inTok == 0 && outTok == 0 && extractedFacts.TokensUsed > 0 {
					inTok = int(float64(extractedFacts.TokensUsed) * 0.6)
					outTok = extractedFacts.TokensUsed - inTok
				}
				recCtx := opts.WithTokenRecordOptions(ctx)
				_ = workflow.ExecuteActivity(recCtx, constants.RecordTokenUsageActivity, activities.TokenUsageInput{
					UserID:       input.UserID,
					SessionID:    input.SessionID,
					TaskID:       workflowID,
					AgentID:      "fact_extraction",
					Model:        extractedFacts.ModelUsed,
					Provider:     extractedFacts.Provider,
					InputTokens:  inTok,
					OutputTokens: outTok,
					Metadata:     map[string]any{"phase": "fact_extraction"},
				}).Get(recCtx, nil)
			}
			logger.Info("Fact extraction complete",
				"fact_count", extractedFacts.FactCount,
				"high_confidence", extractedFacts.HighConfidenceFacts,
			)
		}
	}

	// Step 6: Update session and persist results
	// 步骤 6：更新 session 并持久化结果
	// Session update moved to after usage report generation to ensure accurate cost/token tracking
	// session 更新移到用量报告生成之后，以保证成本/token 统计准确

	logger.Info("ResearchWorkflow completed successfully",
		"total_tokens", totalTokens,
		"quality_score", qualityScore,
		"agent_count", len(agentResults),
	)

	// Aggregate tool errors across agent results
	// 汇总各 agent 结果中的工具错误
	var toolErrors []map[string]string
	for _, ar := range agentResults {
		if len(ar.ToolExecutions) == 0 {
			continue
		}
		for _, te := range ar.ToolExecutions {
			if !te.Success || (te.Error != "") {
				toolErrors = append(toolErrors, map[string]string{
					"agent_id": ar.AgentID,
					"tool":     te.Tool,
					"error":    te.Error,
				})
			}
		}
	}

	meta := map[string]any{
		"version":       "v2",
		"complexity":    decomp.ComplexityScore,
		"quality_score": qualityScore,
		"agent_count":   len(agentResults),
		"patterns_used": []string{"react", "parallel", "reflection"},
	}
	logger.Info("Preparing metadata", "collected_citations_count", len(collectedCitations))
	if len(collectedCitations) > 0 {
		// Export a light citation struct to metadata
		// 把轻量引用结构导出到元数据
		out := make([]map[string]any, 0, len(collectedCitations))
		for _, c := range collectedCitations {
			out = append(out, map[string]any{
				"url":               c.URL,
				"title":             c.Title,
				"source":            c.Source,
				"credibility_score": c.CredibilityScore,
				"quality_score":     c.QualityScore,
				"tool_source":       c.ToolSource,
				"status_code":       c.StatusCode,
				"blocked_reason":    c.BlockedReason,
			})
		}
		meta["citations"] = out
		logger.Info("Added citations to metadata", "count", len(out))
	}
	if verification.TotalClaims > 0 || verification.OverallConfidence > 0 {
		meta["verification"] = verification
	}
	if len(toolErrors) > 0 {
		meta["tool_errors"] = toolErrors
	}

	// Add extracted facts to metadata if available
	// 若有抽取结果，把事实加入元数据
	if extractedFacts != nil && len(extractedFacts.Facts) > 0 {
		// Convert facts to serializable format
		// 把事实转换为可序列化格式
		factsOut := make([]map[string]any, 0, len(extractedFacts.Facts))
		for _, f := range extractedFacts.Facts {
			factsOut = append(factsOut, map[string]any{
				"id":              f.ID,
				"statement":       f.Statement,
				"category":        f.Category,
				"confidence":      f.Confidence,
				"source_citation": f.SourceCitation,
				"entity_mentions": f.EntityMentions,
				"temporal_marker": f.TemporalMarker,
				"is_quantitative": f.IsQuantitative,
			})
		}
		meta["extracted_facts"] = factsOut
		meta["fact_summary"] = map[string]any{
			"total_facts":         extractedFacts.FactCount,
			"high_confidence":     extractedFacts.HighConfidenceFacts,
			"categorized_facts":   extractedFacts.CategorizedFacts,
			"contradiction_count": extractedFacts.ContradictionCount,
		}
	}

	// Aggregate agent metadata (model, provider, tokens)
	// 汇总 agent 元数据（模型、provider、token）
	agentMeta := metadata.AggregateAgentMetadata(agentResults, synthesis.TokensUsed+reflectionTokens)
	maps.Copy(meta, agentMeta)

	// Compute cost estimate from per-phase tokens using centralized pricing
	// 用集中化定价按各阶段的 token 估算成本
	// Sum per-agent usage using splits; then add synthesis using model from synthesis result
	// 先按 input/output 拆分累加各 agent 用量；再用合成结果的模型累加合成用量
	var estCost float64
	for _, ar := range agentResults {
		if ar.InputTokens > 0 || ar.OutputTokens > 0 {
			estCost += pricing.CostForSplit(ar.ModelUsed, ar.InputTokens, ar.OutputTokens)
		} else if ar.TokensUsed > 0 {
			estCost += pricing.CostForTokens(ar.ModelUsed, ar.TokensUsed)
		}
	}
	if synthesis.TokensUsed > 0 {
		inTok := synthesis.InputTokens
		outTok := synthesis.CompletionTokens
		if inTok == 0 && outTok > 0 {
			est := synthesis.TokensUsed - outTok
			if est > 0 {
				inTok = est
			}
		}
		if synthesis.ModelUsed != "" {
			if inTok > 0 || outTok > 0 {
				estCost += pricing.CostForSplit(synthesis.ModelUsed, inTok, outTok)
			} else {
				estCost += pricing.CostForTokens(synthesis.ModelUsed, synthesis.TokensUsed)
			}
		} else {
			estCost += pricing.CostForTokens("", synthesis.TokensUsed)
		}
	}
	if estCost > 0 {
		meta["cost_usd"] = estCost
	}

	// Finalize accurate cost by aggregating recorded token usage for this task
	// 汇总本任务已记录的 token 用量，得出准确成本
	// Ensures task_executions.total_cost_usd reflects sum of per-agent and synthesis usage
	// 确保 task_executions.total_cost_usd 反映各 agent 与合成用量之和
	var report *budget.UsageReport
	{
		err := workflow.ExecuteActivity(ctx, constants.GenerateUsageReportActivity, activities.UsageReportInput{
			// Aggregate by task_id across all user_id records to avoid partial sums
			// 按 task_id 汇总所有 user_id 的记录，避免部分求和
			UserID:    "",
			SessionID: input.SessionID,
			TaskID:    workflowID,
			// Time range left empty; activity defaults to last 24h
			// 时间范围留空；activity 默认取最近 24 小时
		}).Get(ctx, &report)
		if err == nil && report != nil {
			if report.TotalCostUSD > 0 {
				meta["cost_usd"] = report.TotalCostUSD
			}
			// Note: DB aggregation of token_usage provides the accurate full-workflow totals.
			// 注意：对 token_usage 做 DB 聚合才能得到准确的全流程总量。
			// This is used by the API layer (service.go) when returning GetTaskStatus for terminal workflows.
			// API 层（service.go）在返回终态 workflow 的 GetTaskStatus 时会用到它。
		} else if err != nil {
			logger.Warn("Usage report aggregation failed", "error", err)
		}
	}

	// Step 5: Update session and persist results (Moved here to use accurate cost/token data)
	// 步骤 5：更新 session 并持久化结果（移到这里以便使用准确的成本/token 数据）
	if input.SessionID != "" {
		// Use report values if available, otherwise fallback to local estimates.
		// 有报告值就用报告值，否则回退到本地估算。
		// TokensUsed keeps OpenAI-shaped (input + output) semantics for any
		// TokensUsed 保持 OpenAI 口径（input + output），供任何
		// downstream consumer that compares against API responses;
		// 与 API response 比对的下游消费方使用；
		// CacheAwareTokensUsed carries the true total (incl. prompt cache)
		// CacheAwareTokensUsed 才是真实总量（含 prompt cache）
		// for the session quota counter and pricing.
		// 用于 session 配额计数与计价。
		// TokensUsed 保持 OpenAI 口径（input + output），供与 API response 比对的消费方使用；
		// CacheAwareTokensUsed 才是真实总量（含 prompt cache），用于 session 配额计数与计价。
		finalCost := estCost
		finalTokens := totalTokens
		var finalCacheAware int
		if report != nil {
			finalCost = report.TotalCostUSD
			if report.TotalTokens > 0 {
				finalTokens = report.TotalTokens
			}
			finalCacheAware = report.CacheAwareTotalTokens
		}

		var updRes activities.SessionUpdateResult
		err = workflow.ExecuteActivity(ctx,
			constants.UpdateSessionResultActivity,
			activities.SessionUpdateInput{
				SessionID:            input.SessionID,
				Result:               finalResult,
				TokensUsed:           finalTokens,
				CacheAwareTokensUsed: finalCacheAware,
				AgentsUsed:           len(agentResults),
				CostUSD:              finalCost, // Pass explicit cost to avoid default fallback 注：显式传入成本，避免走默认回退
			}).Get(ctx, &updRes)
		if err != nil {
			logger.Error("Failed to update session", "error", err)
		}

		// Persist to vector store (await result to prevent race condition)
		// 持久化到向量库（等待结果，避免竞态）
		_ = workflow.ExecuteActivity(ctx,
			activities.RecordQuery,
			activities.RecordQueryInput{
				SessionID: input.SessionID,
				UserID:    input.UserID,
				Query:     input.Query,
				Answer:    finalResult,
				Model:     modelTier,
				Metadata: map[string]any{
					"workflow":      "research_flow_v2",
					"complexity":    decomp.ComplexityScore,
					"quality_score": qualityScore,
					"patterns_used": []string{"react", "parallel", "reflection"},
					"tenant_id":     input.TenantID,
				},
				RedactPII: true,
			}).Get(ctx, nil)
	}

	// Include synthesis finish_reason and requested_max_tokens for observability/debugging
	// 把合成的 finish_reason 与 requested_max_tokens 写入元数据，便于观测与排查
	if synthesis.FinishReason != "" {
		meta["finish_reason"] = synthesis.FinishReason
	}
	if synthesis.RequestedMaxTokens > 0 {
		meta["requested_max_tokens"] = synthesis.RequestedMaxTokens
	}
	if synthesis.CompletionTokens > 0 {
		meta["completion_tokens"] = synthesis.CompletionTokens
	}
	if synthesis.EffectiveMaxCompletion > 0 {
		meta["effective_max_completion"] = synthesis.EffectiveMaxCompletion
	}

	// Check pause/cancel after verification - signal may have arrived during verification
	// 校验后检查暂停/取消——信号可能在校验期间到达
	if err := controlHandler.CheckPausePoint(ctx, "post_verification"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("post_verification", totalTokens, len(agentResults), synthesis.FinalResult)}, err
	}

	// Check pause/cancel before completion
	// 完成前检查暂停/取消
	if err := controlHandler.CheckPausePoint(ctx, "pre_completion"); err != nil {
		return TaskResult{Success: false, ErrorMessage: err.Error(), Metadata: buildResearchFailureMetadata("pre_completion", totalTokens, len(agentResults), synthesis.FinalResult)}, err
	}

	// Emit final clean LLM_OUTPUT for OpenAI-compatible streaming.
	// 发送最终干净的 LLM_OUTPUT，用于 OpenAI 兼容的流式输出。
	// Agent ID "final_output" signals the streamer to always show this content.
	// Agent ID 为 "final_output" 时，流式层会始终展示该内容。
	if finalResult != "" {
		_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
			WorkflowID: workflowID,
			EventType:  activities.StreamEventLLMOutput,
			AgentID:    "final_output",
			Message:    finalResult,
			Timestamp:  workflow.Now(ctx),
			Payload: map[string]any{
				"tokens_used": totalTokens,
			},
		}).Get(ctx, nil)
	}

	// Emit WORKFLOW_COMPLETED before returning
	// 返回前发送 WORKFLOW_COMPLETED
	_ = workflow.ExecuteActivity(emitCtx, "EmitTaskUpdate", activities.EmitTaskUpdateInput{
		WorkflowID: workflowID,
		EventType:  activities.StreamEventWorkflowCompleted,
		AgentID:    "research",
		Message:    activities.MsgWorkflowCompleted(),
		Timestamp:  workflow.Now(ctx),
	}).Get(ctx, nil)

	return TaskResult{
		Result:     finalResult,
		Success:    true,
		TokensUsed: totalTokens,
		Metadata:   meta,
	}, nil
}

// buildResearchFailureMetadata creates metadata for failed research workflows so that
// buildResearchFailureMetadata 为失败的研究 workflow 构造元数据，
// cost/progress visibility is preserved even when the task fails. Workflow-local only —
// 使任务失败时成本与进度仍然可见。仅在 workflow 内部——
// no DB reads. The service layer augments this with token_usage table data as it already does.
// 不读 DB。service 层会像现在这样用 token_usage 表数据补充它。
func buildResearchFailureMetadata(phase string, totalTokens int, agentCount int, partialResult string) map[string]any {
	meta := map[string]any{
		"status": "failed",
		"phase":  phase,
	}
	if totalTokens > 0 {
		meta["total_tokens"] = totalTokens
	}
	if agentCount > 0 {
		meta["agent_count"] = agentCount
	}
	if partialResult != "" {
		// Truncate to keep response size reasonable (rune-safe to avoid cutting UTF-8 mid-character)
		// 截断以保持响应体积合理（按 rune 截断，避免切断 UTF-8 字符）
		const maxRunes = 10000
		runes := []rune(partialResult)
		if len(runes) > maxRunes {
			partialResult = string(runes[:maxRunes]) + "\n\n[truncated — full intermediate result lost due to workflow failure]"
		}
		meta["partial_result"] = partialResult
	}
	return meta
}

func buildTaskContractContext(subtask activities.Subtask) map[string]any {
	contract := make(map[string]any)

	if subtask.OutputFormat != nil {
		contract["output_format"] = map[string]any{
			"type":            subtask.OutputFormat.Type,
			"required_fields": subtask.OutputFormat.RequiredFields,
			"optional_fields": subtask.OutputFormat.OptionalFields,
		}
	}

	if subtask.SourceGuidance != nil {
		contract["source_guidance"] = map[string]any{
			"required": subtask.SourceGuidance.Required,
			"optional": subtask.SourceGuidance.Optional,
			"avoid":    subtask.SourceGuidance.Avoid,
		}
	}

	if subtask.SearchBudget != nil {
		contract["search_budget"] = map[string]any{
			"max_queries": subtask.SearchBudget.MaxQueries,
			"max_fetches": subtask.SearchBudget.MaxFetches,
		}
	}

	if subtask.Boundaries != nil {
		contract["boundaries"] = map[string]any{
			"in_scope":     subtask.Boundaries.InScope,
			"out_of_scope": subtask.Boundaries.OutOfScope,
		}
	}

	if len(contract) == 0 {
		return nil
	}
	return contract
}

// GapAnalysis holds information about undercovered research areas
// GapAnalysis 保存覆盖不足的研究领域信息
type GapAnalysis struct {
	UndercoveredAreas []string
}

// GapQuery represents a targeted search query for a gap area
// GapQuery 表示针对某个缺口领域的定向搜索查询
type GapQuery struct {
	Query      string
	TargetArea string
}

// analyzeGaps detects which research areas are undercovered in the synthesis
// analyzeGaps 检测合成结果中哪些研究领域覆盖不足
// Reads configuration from context with strategy-based fallbacks for backward compatibility
// 从 context 读取配置，并保留基于策略的回退以向后兼容
func analyzeGaps(synthesisText string, researchAreas []string, context map[string]any) GapAnalysis {
	gaps := GapAnalysis{
		UndercoveredAreas: []string{},
	}

	// Determine strategy for fallback defaults
	// 确定用于回退默认值的策略
	strategy := ""
	if sv, ok := context["research_strategy"].(string); ok {
		strategy = strings.ToLower(strings.TrimSpace(sv))
	}

	// Read from context with strategy-based fallbacks
	// 从 context 读取，并带基于策略的回退
	maxGaps := 3 // default for standard/unknown 注：standard 或未知情况下的默认值
	if v, ok := context["gap_filling_max_gaps"]; ok {
		switch t := v.(type) {
		case int:
			maxGaps = t
		case float64:
			maxGaps = int(t)
		}
		// Clamp to reasonable range
		// 钳制到合理范围
		if maxGaps < 1 {
			maxGaps = 1
		}
		if maxGaps > 20 {
			maxGaps = 20
		}
	} else {
		// Fallback to strategy-based defaults for backward compatibility
		// 回退到基于策略的默认值以向后兼容
		switch strategy {
		case "deep":
			maxGaps = 2
		case "academic":
			maxGaps = 3
		default:
			maxGaps = 3 // standard or unknown 注：standard 或未知
		}
	}

	checkCitationDensity := false // disabled by default (too aggressive) 注：默认关闭（过于激进）
	if v, ok := context["gap_filling_check_citations"]; ok {
		if b, ok := v.(bool); ok {
			checkCitationDensity = b
		}
	}
	// Citation density check disabled by default to avoid false positives
	// 引用密度检查默认关闭，避免误报
	// (well-written sections without citations shouldn't trigger gap-filling)
	// （写得好的段落即使没有引用也不应触发缺口填补）

	for _, area := range researchAreas {
		// Stop if we've already found enough gaps
		// 已找到足够缺口则停止
		if len(gaps.UndercoveredAreas) >= maxGaps {
			break
		}

		areaHeading := "### " + area
		_, after, ok := strings.Cut(synthesisText, areaHeading)
		if !ok {
			// Missing section heading - this is always a gap
			// 缺少段落小标题——这一定算缺口
			gaps.UndercoveredAreas = append(gaps.UndercoveredAreas, area)
			continue
		}

		// Extract section content
		// 提取段落内容
		content := after
		nextSectionIdx := strings.Index(content, "\n### ") // Use ### for subsections 注：用 ### 识别子章节
		if nextSectionIdx == -1 {
			nextSectionIdx = strings.Index(content, "\n## ") // Fallback to ## for main sections 注：主章节回退用 ##
		}
		if nextSectionIdx == -1 {
			nextSectionIdx = len(content)
		}
		sectionContent := strings.TrimSpace(content[:nextSectionIdx])

		// Check: Explicit gap indicator phrases (high precision only)
		// 检查：显式的缺口提示短语（仅做高精度匹配）
		gapPhrases := []string{
			"limited information available",
			"insufficient data",
			"not enough information",
			"no clear evidence",
			"data unavailable",
			"no information found",
		}

		// CJK gap detection phrases (version-gated for Temporal determinism)
		// CJK 缺口检测短语（版本门控以保证 Temporal 确定性）
		if enableCJK, ok := context["enable_cjk_gap_phrases"].(bool); ok && enableCJK {
			gapPhrases = append(gapPhrases,
				"未找到足够信息", // Chinese: not enough information found 注：中文：未找到足够信息
				"数据不足",    // Chinese: insufficient data 注：中文：数据不足
				"信息不足",    // Chinese: insufficient information 注：中文：信息不足
				"情報不足",    // Japanese: information insufficient 注：日文：信息不足
				"情報が不足",   // Japanese: lacking information 注：日文：信息不足
				"정보가 부족",  // Korean: lacking information 注：韩文：信息不足
			)
		}

		hasExplicitGap := false
		for _, phrase := range gapPhrases {
			if strings.Contains(strings.ToLower(sectionContent), phrase) {
				gaps.UndercoveredAreas = append(gaps.UndercoveredAreas, area)
				hasExplicitGap = true
				break
			}
		}

		// Citation density (only for deep/academic strategies and if no explicit gap found)
		// 引用密度（仅对 deep/academic 策略，且未发现显式缺口时）
		if !hasExplicitGap && checkCitationDensity {
			citationCount := countInlineCitationsInSection(sectionContent)
			// Minimal rule: flag only if there are zero citations
			// 最小规则：仅在完全没有引用时才标记
			if citationCount == 0 {
				gaps.UndercoveredAreas = append(gaps.UndercoveredAreas, area)
			}
		}
	}

	return gaps
}

// buildGapQueries creates targeted queries for gap areas
// buildGapQueries 为缺口领域生成定向查询
func buildGapQueries(gaps []string, originalQuery string) []GapQuery {
	queries := make([]GapQuery, 0, len(gaps))
	for _, area := range gaps {
		queries = append(queries, GapQuery{
			Query:      fmt.Sprintf("Find detailed information about: %s (related to: %s)", area, originalQuery),
			TargetArea: area,
		})
	}
	return queries
}

// countInlineCitationsInSection counts unique inline citation references [n] in text
// countInlineCitationsInSection 统计文本中不重复的行内引用 [n]
func countInlineCitationsInSection(text string) int {
	re := regexp.MustCompile(`\[\d+\]`)
	matches := re.FindAllString(text, -1)
	// Deduplicate (same citation can appear multiple times)
	// 去重（同一引用可能出现多次）
	seen := make(map[string]bool)
	for _, m := range matches {
		seen[m] = true
	}
	return len(seen)
}

// topologicalSort performs topological sort on subtasks based on dependencies
// topologicalSort 根据依赖关系对子任务做拓扑排序
// Returns execution order (list of subtask IDs)
// 返回执行顺序（子任务 ID 列表）
func topologicalSort(subtasks []activities.Subtask) []string {
	// Build adjacency list and in-degree map
	// 构造邻接表与入度表
	adjList := make(map[string][]string)
	inDegree := make(map[string]int)

	// Initialize all subtasks
	// 初始化所有子任务
	for _, st := range subtasks {
		if _, ok := inDegree[st.ID]; !ok {
			inDegree[st.ID] = 0
		}
		for _, dep := range st.Dependencies {
			adjList[dep] = append(adjList[dep], st.ID)
			inDegree[st.ID]++
		}
	}

	// Find all nodes with in-degree 0
	// 找出全部入度为 0 的节点
	queue := make([]string, 0)
	for id, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, id)
		}
	}

	// Process queue
	// 处理队列
	result := make([]string, 0, len(subtasks))
	for len(queue) > 0 {
		// Pop first element
		// 弹出队首元素
		current := queue[0]
		queue = queue[1:]
		result = append(result, current)

		// Reduce in-degree of neighbors
		// 递减邻居的入度
		for _, neighbor := range adjList[current] {
			inDegree[neighbor]--
			if inDegree[neighbor] == 0 {
				queue = append(queue, neighbor)
			}
		}
	}

	// If result doesn't contain all subtasks, there's a cycle
	// 若结果未包含全部子任务，说明存在环
	// Fall back to original order
	// 回退到原始顺序
	if len(result) != len(subtasks) {
		result = make([]string, 0, len(subtasks))
		for _, st := range subtasks {
			result = append(result, st.ID)
		}
	}

	return result
}

// persistAgentExecutionLocal persists agent execution results to the database.
// persistAgentExecutionLocal 把 agent 执行结果持久化到数据库。
// This is a fire-and-forget operation that won't fail the workflow.
// 这是 fire-and-forget 操作，不会让 workflow 失败。
// It's local to avoid circular imports with the workflows package.
// 放在本地实现，避免与 workflows 包产生循环依赖。
func persistAgentExecutionLocal(ctx workflow.Context, workflowID, agentID, input string, result activities.AgentExecutionResult) {
	logger := workflow.GetLogger(ctx)

	// Use detached context for fire-and-forget persistence
	// fire-and-forget 持久化使用 detached context
	detachedCtx, _ := workflow.NewDisconnectedContext(ctx)
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	detachedCtx = workflow.WithActivityOptions(detachedCtx, activityOpts)

	// Pre-generate agent execution ID using SideEffect for replay safety
	// 用 SideEffect 预生成 agent 执行 ID，保证重放安全
	var agentExecutionID string
	workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New().String()
	}).Get(&agentExecutionID)

	// Determine state based on success
	// 根据成功与否确定状态
	state := "COMPLETED"
	if !result.Success {
		state = "FAILED"
	}

	// Truncate output for storage to avoid Temporal deadlock detection during JSON serialization.
	// 写库前截断输出，避免 JSON 序列化超过 1 秒触发 Temporal 死锁检测。
	// Large payloads (>50KB) can cause serialization to exceed 1 second, triggering TMPRL1101.
	// 大载荷（>50KB）可能使序列化耗时超过 1 秒，触发 TMPRL1101。
	// This only affects database storage; in-memory data for LLM processing remains complete.
	// 这仅影响数据库存储，供 LLM 处理的内存数据保持完整。
	// 写库前截断输出，避免 JSON 序列化超过 1 秒触发 Temporal 死锁检测（TMPRL1101）；
	// 仅影响数据库存储，供 LLM 使用的内存数据保持完整。
	const maxOutputLen = 50000
	outputForStorage := result.Response
	if len(outputForStorage) > maxOutputLen {
		outputForStorage = outputForStorage[:maxOutputLen] + "\n\n[TRUNCATED for storage - original length: " + fmt.Sprintf("%d", len(result.Response)) + "]"
		logger.Debug("Truncated agent output for storage",
			"agent_id", agentID,
			"original_len", len(result.Response),
			"truncated_len", len(outputForStorage),
		)
	}

	// Persist agent execution asynchronously
	// 异步持久化 agent 执行记录
	workflow.ExecuteActivity(detachedCtx,
		activities.PersistAgentExecutionStandalone,
		activities.PersistAgentExecutionInput{
			ID:         agentExecutionID,
			WorkflowID: workflowID,
			AgentID:    agentID,
			Input:      input,
			Output:     outputForStorage,
			State:      state,
			TokensUsed: result.TokensUsed,
			ModelUsed:  result.ModelUsed,
			DurationMs: result.DurationMs,
			Error:      result.Error,
			Metadata: map[string]any{
				"workflow": "research",
				"strategy": "react",
			},
		},
	)

	// Persist tool executions if any
	// 若有工具执行记录则一并持久化
	if len(result.ToolExecutions) > 0 {
		for _, tool := range result.ToolExecutions {
			// Convert tool output to string
			// 把工具输出转为字符串
			outputStr := ""
			if tool.Output != nil {
				switch v := tool.Output.(type) {
				case string:
					outputStr = v
				default:
					if jsonBytes, err := json.Marshal(v); err == nil {
						outputStr = string(jsonBytes)
					} else {
						outputStr = "complex output"
					}
				}
			}

			// Extract input params from tool execution
			// 从工具执行记录中提取输入参数
			inputParamsMap, _ := tool.InputParams.(map[string]any)

			workflow.ExecuteActivity(
				detachedCtx,
				activities.PersistToolExecutionStandalone,
				activities.PersistToolExecutionInput{
					WorkflowID:       workflowID,
					AgentID:          agentID,
					AgentExecutionID: agentExecutionID,
					ToolName:         tool.Tool,
					InputParams:      inputParamsMap,
					Output:           outputStr,
					Success:          tool.Success,
					TokensConsumed:   0,               // Not provided by agent 注：agent 未提供该值
					DurationMs:       tool.DurationMs, // From agent-core proto 注：来自 agent-core proto
					Error:            tool.Error,
				},
			)
		}
	}

	logger.Debug("Agent execution persisted",
		"workflow_id", workflowID,
		"agent_id", agentID,
		"state", state,
	)
}

// persistAgentExecutionLocalWithMeta persists agent execution results with extra metadata.
// persistAgentExecutionLocalWithMeta 持久化 agent 执行结果并附带额外元数据。
// This extends persistAgentExecutionLocal to support phase/url/search_key tracking
// 在 persistAgentExecutionLocal 基础上支持 phase/url/search_key 追踪，
// for domain_discovery and domain_prefetch agents.
// 供 domain_discovery 与 domain_prefetch agent 使用。
func persistAgentExecutionLocalWithMeta(ctx workflow.Context, workflowID, agentID, input string, result activities.AgentExecutionResult, extraMeta map[string]any) {
	logger := workflow.GetLogger(ctx)

	// Use detached context for fire-and-forget persistence
	// fire-and-forget 持久化使用 detached context
	detachedCtx, _ := workflow.NewDisconnectedContext(ctx)
	activityOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	detachedCtx = workflow.WithActivityOptions(detachedCtx, activityOpts)

	// Pre-generate agent execution ID using SideEffect for replay safety
	// 用 SideEffect 预生成 agent 执行 ID，保证重放安全
	var agentExecutionID string
	workflow.SideEffect(ctx, func(ctx workflow.Context) any {
		return uuid.New().String()
	}).Get(&agentExecutionID)

	// Determine state based on success
	// 根据成功与否确定状态
	state := "COMPLETED"
	if !result.Success {
		state = "FAILED"
	}

	// Merge base metadata with extra metadata
	// 合并基础元数据与额外元数据
	metadata := map[string]any{
		"workflow": "research",
		"strategy": "react",
	}
	maps.Copy(metadata, extraMeta)

	// Truncate output for storage to avoid Temporal deadlock detection during JSON serialization.
	// 写库前截断输出，避免 JSON 序列化超过 1 秒触发 Temporal 死锁检测。
	// Large payloads (>50KB) can cause serialization to exceed 1 second, triggering TMPRL1101.
	// 大载荷（>50KB）可能使序列化耗时超过 1 秒，触发 TMPRL1101。
	// This only affects database storage; in-memory data for LLM processing remains complete.
	// 这仅影响数据库存储，供 LLM 处理的内存数据保持完整。
	// 同上：仅为写库截断，内存中的完整输出不受影响。
	const maxOutputLen = 50000
	outputForStorage := result.Response
	if len(outputForStorage) > maxOutputLen {
		outputForStorage = outputForStorage[:maxOutputLen] + "\n\n[TRUNCATED for storage - original length: " + fmt.Sprintf("%d", len(result.Response)) + "]"
		logger.Debug("Truncated agent output for storage",
			"agent_id", agentID,
			"original_len", len(result.Response),
			"truncated_len", len(outputForStorage),
		)
	}

	// Persist agent execution asynchronously
	// 异步持久化 agent 执行记录
	workflow.ExecuteActivity(detachedCtx,
		activities.PersistAgentExecutionStandalone,
		activities.PersistAgentExecutionInput{
			ID:         agentExecutionID,
			WorkflowID: workflowID,
			AgentID:    agentID,
			Input:      input,
			Output:     outputForStorage,
			State:      state,
			TokensUsed: result.TokensUsed,
			ModelUsed:  result.ModelUsed,
			DurationMs: result.DurationMs,
			Error:      result.Error,
			Metadata:   metadata,
		},
	)

	// Persist tool executions if any
	// 若有工具执行记录则一并持久化
	if len(result.ToolExecutions) > 0 {
		for _, tool := range result.ToolExecutions {
			// Convert tool output to string
			// 把工具输出转为字符串
			outputStr := ""
			if tool.Output != nil {
				switch v := tool.Output.(type) {
				case string:
					outputStr = v
				default:
					if jsonBytes, err := json.Marshal(v); err == nil {
						outputStr = string(jsonBytes)
					} else {
						outputStr = "complex output"
					}
				}
			}

			// Extract input params from tool execution
			// 从工具执行记录中提取输入参数
			inputParamsMap, _ := tool.InputParams.(map[string]any)

			workflow.ExecuteActivity(
				detachedCtx,
				activities.PersistToolExecutionStandalone,
				activities.PersistToolExecutionInput{
					WorkflowID:       workflowID,
					AgentID:          agentID,
					AgentExecutionID: agentExecutionID,
					ToolName:         tool.Tool,
					InputParams:      inputParamsMap,
					Output:           outputStr,
					Success:          tool.Success,
					TokensConsumed:   0,
					DurationMs:       tool.DurationMs,
					Error:            tool.Error,
				},
			)
		}
	}

	logger.Debug("Agent execution persisted with metadata",
		"workflow_id", workflowID,
		"agent_id", agentID,
		"state", state,
		"extra_meta", extraMeta,
	)
}

// simpleLogger is a minimal logger interface for validateTaskContracts
// simpleLogger 是 validateTaskContracts 使用的最小 logger 接口
type simpleLogger interface {
	Info(msg string, keyvals ...any)
	Warn(msg string, keyvals ...any)
}

// validateTaskContracts validates decomposed subtasks against their declared contracts
// validateTaskContracts 校验分解出的子任务是否符合其声明的契约
// Returns an error if any contract violations are found
// 发现任何契约违规时返回 error
func validateTaskContracts(subtasks []activities.Subtask, logger simpleLogger) error {
	if len(subtasks) == 0 {
		logger.Warn("Task contract validation: no subtasks to validate")
		return nil // Not an error - allow fallback to simple execution 注：不算错误——允许回退到简单执行
	}

	validSourceTypes := map[string]bool{
		"official": true, "aggregator": true, "news": true, "academic": true,
		"github": true, "financial": true, "documentation": true,
		"local_cn": true, "local_jp": true, "local_kr": true,
	}

	for i, task := range subtasks {
		// Validate required fields
		// 校验必填字段
		if task.ID == "" {
			return fmt.Errorf("subtask %d: missing required field 'id'", i)
		}
		if task.Description == "" {
			return fmt.Errorf("subtask %s: missing required field 'description'", task.ID)
		}

		// Validate source types if specified
		// 如有指定则校验来源类型
		if task.SourceGuidance != nil {
			for _, sourceType := range task.SourceGuidance.Required {
				if !validSourceTypes[sourceType] {
					logger.Warn("Invalid source type in task contract",
						"task_id", task.ID,
						"source_type", sourceType,
						"valid_types", []string{"official", "aggregator", "news", "academic", "github", "financial", "documentation", "local_cn", "local_jp", "local_kr"},
					)
				}
			}
		}

		// Validate output format type is reasonable if specified
		// 如有指定则校验输出格式类型是否合理
		if task.OutputFormat != nil && len(task.OutputFormat.Type) > 100 {
			logger.Warn("Suspiciously long output format type in task contract",
				"task_id", task.ID,
				"type_length", len(task.OutputFormat.Type),
			)
		}

		// Validate dependencies reference existing task IDs
		// 校验依赖项引用的任务 ID 是否存在
		taskIDs := make(map[string]bool)
		for _, t := range subtasks {
			taskIDs[t.ID] = true
		}
		for _, dep := range task.Dependencies {
			if !taskIDs[dep] {
				return fmt.Errorf("subtask %s: dependency '%s' does not exist", task.ID, dep)
			}
		}
	}

	logger.Info("Task contract validation passed",
		"subtasks_validated", len(subtasks),
	)
	return nil
}

// Note: Post-synthesis language validation was removed.
// 注意：合成后的语言校验已移除。
// Language handling now occurs earlier (refine stage sets target_language),
// 语言处理现在更早进行（refine 阶段设置 target_language），
// and the synthesis activity embeds a language instruction.
// 合成后的语言校验已移除：语言处理提前到 refine 阶段（设置 target_language），
// 由合成 activity 内嵌语言指令。

// ============================================================================
// ============================================================ 分隔线
// Gap-filling Value Gating (P1-1)
// 缺口填补的价值门控（P1-1）
// ============================================================================
// ============================================================ 分隔线

// GapFillMetrics tracks gap-filling effectiveness
// GapFillMetrics 记录缺口填补的有效性
type GapFillMetrics struct {
	TotalAgents     int
	AcceptedAgents  int
	SkippedLowValue int
	NewURLsFound    int
	NewDomainsFound int
}

// extractURLsFromAgentResults extracts unique URLs from agent tool executions
// extractURLsFromAgentResults 从 agent 工具执行中提取去重后的 URL
func extractURLsFromAgentResults(results []activities.AgentExecutionResult) []string {
	urlSet := make(map[string]bool)
	var urls []string

	for _, r := range results {
		for _, te := range r.ToolExecutions {
			// Extract URLs from web_fetch and web_search tool inputs
			// 从 web_fetch 与 web_search 的工具输入中提取 URL
			if te.Tool == "web_fetch" || te.Tool == "web_search" {
				if params, ok := te.InputParams.(map[string]any); ok {
					if urlStr, ok := params["url"].(string); ok && urlStr != "" {
						if !urlSet[urlStr] {
							urlSet[urlStr] = true
							urls = append(urls, urlStr)
						}
					}
				}
			}

			// Also extract URLs from tool outputs (search results)
			// 同时从工具输出（搜索结果）中提取 URL
			if te.Tool == "web_search" && te.Success {
				if output, ok := te.Output.(map[string]any); ok {
					if results, ok := output["results"].([]any); ok {
						for _, result := range results {
							if resultMap, ok := result.(map[string]any); ok {
								if urlStr, ok := resultMap["url"].(string); ok && urlStr != "" {
									if !urlSet[urlStr] {
										urlSet[urlStr] = true
										urls = append(urls, urlStr)
									}
								}
							}
						}
					}
				}
			}
		}
	}

	return urls
}

// evaluateGapFillValue determines if gap-fill results add sufficient incremental value
// evaluateGapFillValue 判断缺口填补结果是否带来足够的增量价值
// Returns: (accept, newURLs, newDomains)
// 返回：(accept, newURLs, newDomains)
func evaluateGapFillValue(
	newResults []activities.AgentExecutionResult,
	existingURLs map[string]bool,
	existingDomains map[string]bool,
) (bool, int, int) {
	newURLs := 0
	newDomains := 0

	urls := extractURLsFromAgentResults(newResults)
	for _, u := range urls {
		if !existingURLs[u] {
			newURLs++
			domain, err := metadata.ExtractDomain(u)
			if err == nil && domain != "" && !existingDomains[domain] {
				newDomains++
			}
		}
	}

	// Value gate: accept if at least 2 new URLs OR 1 new domain
	// 价值门槛：至少 2 个新 URL 或 1 个新域名才接受补洞结果。
	accept := newURLs >= 2 || newDomains >= 1
	return accept, newURLs, newDomains
}

// updateExistingURLs adds URLs from results to the existing sets
// updateExistingURLs 把结果中的 URL 加入已有集合
func updateExistingURLs(
	results []activities.AgentExecutionResult,
	existingURLs map[string]bool,
	existingDomains map[string]bool,
) {
	urls := extractURLsFromAgentResults(results)
	for _, u := range urls {
		existingURLs[u] = true
		domain, err := metadata.ExtractDomain(u)
		if err == nil && domain != "" {
			existingDomains[domain] = true
		}
	}
}
