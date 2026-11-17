package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	"go.temporal.io/sdk/activity"
)

// LocalizedSearchInput is the input for a localized search across multiple languages
// LocalizedSearchInput 是按多语言做本地化检索的入参。
type LocalizedSearchInput struct {
	Query            string                 `json:"query"`
	EntityName       string                 `json:"entity_name,omitempty"`       // Primary entity being researched 注：被调研的主要实体
	TargetLanguages  []string               `json:"target_languages"`            // e.g., ["zh", "ja", "ko"] 注：例如 ["zh", "ja", "ko"]
	LocalizedNames   map[string][]string    `json:"localized_names,omitempty"`   // Language -> names 注：语言 -> 名称
	SourceTypes      []string               `json:"source_types,omitempty"`      // Source types to search 注：要检索的来源类型
	MaxResultsPerLang int                   `json:"max_results_per_lang"`
	Context          map[string]any `json:"context,omitempty"`
	ParentWorkflowID string                 `json:"parent_workflow_id,omitempty"`
}

// LocalizedSearchResult contains results from localized searches
// LocalizedSearchResult 汇总各语言本地化检索的结果。
type LocalizedSearchResult struct {
	Results          map[string][]LocalizedResult `json:"results"`           // Language -> results 注：语言 -> 结果
	TotalResults     int                          `json:"total_results"`
	LanguagesCovered []string                     `json:"languages_covered"`
	TokensUsed       int                          `json:"tokens_used"`
	ModelUsed        string                       `json:"model_used"`
	Provider         string                       `json:"provider"`
}

// LocalizedResult represents a single result from a localized search
// LocalizedResult 是本地化检索返回的单条结果。
type LocalizedResult struct {
	URL           string  `json:"url"`
	Title         string  `json:"title"`
	Snippet       string  `json:"snippet"`
	Language      string  `json:"language"`
	PublishedDate string  `json:"published_date,omitempty"`
	Score         float64 `json:"score"`
	SourceType    string  `json:"source_type"`
}

// EntityLocalizationInput is the input for detecting entity name localizations
// EntityLocalizationInput 是推断实体名各语言写法的入参。
type EntityLocalizationInput struct {
	EntityName       string                 `json:"entity_name"`
	TargetLanguages  []string               `json:"target_languages"` // e.g., ["zh", "ja", "ko"] 注：例如 ["zh", "ja", "ko"]
	EntityType       string                 `json:"entity_type,omitempty"` // "company", "person", "product" 注："company"、"person"、"product"
	Context          map[string]any `json:"context,omitempty"`
	ParentWorkflowID string         `json:"parent_workflow_id,omitempty"`
}

// EntityLocalizationResult contains detected name localizations
// EntityLocalizationResult 保存推断出的各语言名称及其置信度。
type EntityLocalizationResult struct {
	EntityName       string              `json:"entity_name"`
	LocalizedNames   map[string][]string `json:"localized_names"` // Language -> possible names 注：语言 -> 可能的名称
	Confidence       map[string]float64  `json:"confidence"`      // Language -> confidence 注：语言 -> 置信度
	TokensUsed       int                 `json:"tokens_used"`
	ModelUsed        string              `json:"model_used"`
	Provider         string              `json:"provider"`
	InputTokens      int                 `json:"input_tokens"`
	OutputTokens     int                 `json:"output_tokens"`
}

// DetectEntityLocalization detects localized names for an entity
// DetectEntityLocalization 推断某个实体在各目标语言下的名称。
func (a *Activities) DetectEntityLocalization(ctx context.Context, input EntityLocalizationInput) (*EntityLocalizationResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("DetectEntityLocalization: starting",
		"entity", input.EntityName,
		"target_languages", input.TargetLanguages,
		"entity_type", input.EntityType,
	)

	// Build prompt for entity localization detection
	// 构造实体本地化识别用的 prompt。
	systemPrompt := buildEntityLocalizationPrompt(input)
	userContent := buildEntityLocalizationContent(input)

	// Call LLM service
	// 调用 LLM 服务
	llmServiceURL := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/agent/query", llmServiceURL)

	reqBody := map[string]any{
		"query":       userContent,
		"max_tokens":  4096, // Extended for entity localization output 注：为容纳实体本地化输出而加大
		"temperature": 0.1,
		"agent_id":    "entity_localization",
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
		Timeout:   120 * time.Second, // Extended for LLM processing time 注：为 LLM 处理耗时加长
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(reqJSON)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", "entity_localization")
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

	result := &EntityLocalizationResult{
		EntityName:     input.EntityName,
		LocalizedNames: make(map[string][]string),
		Confidence:     make(map[string]float64),
		TokensUsed:     llmResp.TokensUsed,
		ModelUsed:      llmResp.ModelUsed,
		Provider:       llmResp.Provider,
		InputTokens:    llmResp.Metadata.InputTokens,
		OutputTokens:   llmResp.Metadata.OutputTokens,
	}

	// Parse structured response
	// 解析结构化 response。
	if err := parseEntityLocalizationResponse(llmResp.Response, result); err != nil {
		logger.Warn("DetectEntityLocalization: failed to parse structured response",
			"error", err,
		)
		// Fallback: use original name for all languages
		// 兜底：所有语言都直接用原始名称，置信度给低值。
		for _, lang := range input.TargetLanguages {
			result.LocalizedNames[lang] = []string{input.EntityName}
			result.Confidence[lang] = 0.3
		}
	}

	logger.Info("DetectEntityLocalization: complete",
		"languages_detected", len(result.LocalizedNames),
	)

	return result, nil
}

// buildEntityLocalizationPrompt creates the system prompt
// buildEntityLocalizationPrompt 构造 system prompt
func buildEntityLocalizationPrompt(input EntityLocalizationInput) string {
	var sb strings.Builder

	sb.WriteString(`You are a multilingual entity name expert. Your task is to identify localized versions
of entity names for targeted web searches.

## Your Goals:
1. Identify official/common names in each target language
2. Include transliterations, translations, and local variants
3. Provide confidence levels for each localization
4. Distinguish between verified names and guesses

## Guidelines:
- For companies: Look for official local subsidiaries, common press names
- For products: Consider local product names, translations, transliterations
- For people: Use standard romanization/transliteration conventions
- Include multiple variants if commonly used
- Higher confidence for official names, lower for guesses

## Response Format:
Return a JSON object:
{
  "localizations": {
    "zh": {
      "names": ["Chinese Name 1", "Chinese Name 2"],
      "confidence": 0.9,
      "notes": "Official Chinese subsidiary name"
    },
    "ja": {
      "names": ["Japanese Name"],
      "confidence": 0.7,
      "notes": "Common press transliteration"
    }
  }
}
`)

	return sb.String()
}

// buildEntityLocalizationContent builds user content
// buildEntityLocalizationContent 构造 user content
func buildEntityLocalizationContent(input EntityLocalizationInput) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "## Entity Name: %s\n", input.EntityName)

	if input.EntityType != "" {
		fmt.Fprintf(&sb, "## Entity Type: %s\n", input.EntityType)
	}

	fmt.Fprintf(&sb, "## Target Languages: %s\n\n", strings.Join(input.TargetLanguages, ", "))

	sb.WriteString("Please identify localized versions of this entity name for each target language.\n")
	sb.WriteString("Include:\n")
	sb.WriteString("- Official local names (subsidiaries, translations)\n")
	sb.WriteString("- Common press/media names\n")
	sb.WriteString("- Standard transliterations\n")
	sb.WriteString("- Any popular alternative spellings\n")

	return sb.String()
}

// parseEntityLocalizationResponse parses the LLM response
// parseEntityLocalizationResponse 解析 LLM response
func parseEntityLocalizationResponse(response string, result *EntityLocalizationResult) error {
	// Find JSON in response
	// 从 response 里截出 JSON。
	start := strings.Index(response, "{")
	end := strings.LastIndex(response, "}")

	if start == -1 || end == -1 || end <= start {
		return fmt.Errorf("no JSON object found in response")
	}

	jsonStr := response[start : end+1]

	var parsed struct {
		Localizations map[string]struct {
			Names      []string `json:"names"`
			Confidence float64  `json:"confidence"`
			Notes      string   `json:"notes"`
		} `json:"localizations"`
	}

	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	for lang, loc := range parsed.Localizations {
		result.LocalizedNames[lang] = loc.Names
		result.Confidence[lang] = loc.Confidence
	}

	return nil
}

// GetRegionalSourceSites returns recommended sites for a specific language/region
// GetRegionalSourceSites 返回指定语言/地区推荐的站点列表。
func GetRegionalSourceSites(language string) []string {
	// Default regional sites for common languages
	// 常见语言的默认地区站点。
	regionalSites := map[string][]string{
		"zh": {
			"36kr.com",        // Chinese tech news 注：中国科技新闻
			"iyiou.com",       // Chinese industry analysis 注：中国行业分析
			"tianyancha.com",  // Chinese company data 注：中国公司数据
			"pedaily.cn",      // Chinese investment news 注：中国投资新闻
			"chinaventure.com.cn",
			"baike.baidu.com", // Chinese Wikipedia 注：中文百科
		},
		"ja": {
			"nikkei.com",      // Japanese business news 注：日本商业新闻
			"prtimes.jp",      // Japanese press releases 注：日本新闻稿
			"startup-db.com",  // Japanese startup database 注：日本创业公司数据库
			"initial.inc",     // Japanese startup platform 注：日本创业平台
			"bridgej.com",     // Japanese tech news 注：日本科技新闻
		},
		"ko": {
			"platum.kr",       // Korean startup news 注：韩国创业新闻
			"thevc.kr",        // Korean VC news 注：韩国 VC 新闻
			"venturesquare.net", // Korean startup community 注：韩国创业社区
			"besuccess.com",   // Korean tech news 注：韩国科技新闻
		},
		"de": {
			"handelsblatt.com", // German business 注：德国商业
			"gruenderszene.de", // German startups 注：德国创业公司
			"t3n.de",          // German tech 注：德国科技
		},
		"fr": {
			"lesechos.fr",     // French business 注：法国商业
			"maddyness.com",   // French startups 注：法国创业公司
			"journaldunet.com", // French tech 注：法国科技
		},
	}

	if sites, ok := regionalSites[language]; ok {
		return sites
	}
	return nil
}
