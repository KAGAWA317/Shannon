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

// StructuredFact represents an extracted fact with confidence and provenance
// StructuredFact 表示一条带置信度与来源的已抽取 fact。
type StructuredFact struct {
	ID             string   `json:"id"`
	Statement      string   `json:"statement"`       // The factual claim 注：事实陈述本身
	Category       string   `json:"category"`        // e.g., "funding", "product", "market", "technical" 注：例如 "funding"、"product"、"market"、"technical"
	Confidence     float64  `json:"confidence"`      // 0.0-1.0 注：取值范围 0.0-1.0
	SourceCitation []int    `json:"source_citation"` // Citation indices [1], [2], etc. 注：citation 序号，如 [1]、[2]
	EntityMentions []string `json:"entity_mentions"` // Entities mentioned in the fact 注：该 fact 中提到的实体
	TemporalMarker string   `json:"temporal_marker"` // e.g., "2024", "Q3 2024", "recent" 注：例如 "2024"、"Q3 2024"、"recent"
	IsQuantitative bool     `json:"is_quantitative"` // Contains numbers/metrics 注：是否包含数字/指标
	Contradictions []string `json:"contradictions"`  // IDs of contradicting facts 注：与之矛盾的 fact ID
}

// FactExtractionCitation is a simplified citation for fact extraction input
// FactExtractionCitation 是 fact 抽取入参用的精简 citation
type FactExtractionCitation struct {
	URL    string `json:"url"`
	Title  string `json:"title"`
	Source string `json:"source"`
}

// FactExtractionInput is the input for fact extraction
// FactExtractionInput 是 fact 抽取的入参
type FactExtractionInput struct {
	Query            string                   `json:"query"`
	SynthesisResult  string                   `json:"synthesis_result"`
	Citations        []FactExtractionCitation `json:"citations,omitempty"`
	ResearchAreas    []string                 `json:"research_areas,omitempty"`
	Context          map[string]any           `json:"context,omitempty"`
	ParentWorkflowID string                   `json:"parent_workflow_id,omitempty"`
}

// FactExtractionResult contains the extracted facts
// FactExtractionResult 保存抽取出的 facts
type FactExtractionResult struct {
	Facts              []StructuredFact `json:"facts"`
	FactCount          int              `json:"fact_count"`
	CategorizedFacts   map[string]int   `json:"categorized_facts"`   // Count per category 注：各分类的数量
	HighConfidenceFacts int             `json:"high_confidence_facts"` // Facts with confidence >= 0.8 注：置信度 >= 0.8 的 facts
	ContradictionCount int              `json:"contradiction_count"`
	TokensUsed         int              `json:"tokens_used"`
	ModelUsed          string           `json:"model_used"`
	Provider           string           `json:"provider"`
	InputTokens        int              `json:"input_tokens"`
	OutputTokens       int              `json:"output_tokens"`
}

// ExtractFacts extracts structured facts from a synthesis result
// ExtractFacts 从 synthesis 结果中抽取结构化 facts。
func (a *Activities) ExtractFacts(ctx context.Context, input FactExtractionInput) (*FactExtractionResult, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("ExtractFacts: starting",
		"query", truncateStr(input.Query, 100),
		"synthesis_length", len(input.SynthesisResult),
		"citation_count", len(input.Citations),
	)

	// Build extraction prompt
	// 构造抽取用的 prompt
	systemPrompt := buildFactExtractionPrompt(input)
	userContent := buildFactExtractionContent(input)

	// Call LLM service
	// 调用 LLM 服务
	llmServiceURL := getenv("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/agent/query", llmServiceURL)

	reqBody := map[string]any{
		"query":       userContent,
		"max_tokens":  8192, // Extended for detailed fact extraction 注：加大到 8192，以容纳详细的 fact 抽取输出。
		"temperature": 0.1, // Low temperature for factual extraction 注：低 temperature，保证事实抽取稳定。
		"agent_id":    "fact_extractor",
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
		Timeout:   120 * time.Second, // Longer timeout for large synthesis content 注：加长 timeout，应对大段 synthesis 内容。
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(reqJSON)))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", "fact_extractor")
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

	result := &FactExtractionResult{
		TokensUsed:       llmResp.TokensUsed,
		ModelUsed:        llmResp.ModelUsed,
		Provider:         llmResp.Provider,
		InputTokens:      llmResp.Metadata.InputTokens,
		OutputTokens:     llmResp.Metadata.OutputTokens,
		CategorizedFacts: make(map[string]int),
	}

	// Parse structured response
	// 解析结构化 response
	if err := parseFactExtractionResponse(llmResp.Response, result); err != nil {
		logger.Warn("ExtractFacts: failed to parse structured response",
			"error", err,
		)
		// Return empty result with metadata
		// 返回带 metadata 的空结果，而不是报错。
		result.Facts = []StructuredFact{}
	}

	// Compute summary statistics
	// 计算汇总统计
	result.FactCount = len(result.Facts)
	for _, fact := range result.Facts {
		result.CategorizedFacts[fact.Category]++
		if fact.Confidence >= 0.8 {
			result.HighConfidenceFacts++
		}
		if len(fact.Contradictions) > 0 {
			result.ContradictionCount++
		}
	}

	logger.Info("ExtractFacts: complete",
		"fact_count", result.FactCount,
		"high_confidence", result.HighConfidenceFacts,
		"categories", len(result.CategorizedFacts),
	)

	return result, nil
}

// buildFactExtractionPrompt creates the system prompt for fact extraction
// buildFactExtractionPrompt 构造 fact 抽取用的 system prompt
func buildFactExtractionPrompt(input FactExtractionInput) string {
	var sb strings.Builder

	sb.WriteString(`You are a fact extraction specialist. Your task is to extract structured facts
from research synthesis content.

## Your Goals:
1. Extract discrete, verifiable factual claims
2. Assign confidence levels based on source quality and consensus
3. Identify which citations support each fact
4. Categorize facts by topic/domain
5. Flag potential contradictions between facts

## Extraction Guidelines:
- Each fact should be a single, atomic claim
- Avoid opinions, speculation, or hedged statements
- Include quantitative data when available
- Note temporal context (dates, time periods)
- Track entity mentions for cross-referencing

## Categories (use these or similar):
- funding: Investment, valuation, financial data
- product: Product features, launches, capabilities
- market: Market size, competition, trends
- technical: Technology, architecture, implementation
- team: People, hiring, organization
- partnership: Collaborations, integrations, deals
- regulatory: Compliance, legal, policy
- performance: Metrics, benchmarks, results

## Response Format:
Return a JSON object:
{
  "facts": [
    {
      "id": "fact-1",
      "statement": "Company X raised $50M in Series B",
      "category": "funding",
      "confidence": 0.95,
      "source_citation": [1, 3],
      "entity_mentions": ["Company X"],
      "temporal_marker": "Q2 2024",
      "is_quantitative": true,
      "contradictions": []
    }
  ],
  "extraction_notes": "Brief notes about extraction challenges"
}

## Confidence Scoring:
- 0.9-1.0: Multiple authoritative sources agree
- 0.7-0.9: Single authoritative source or multiple secondary sources
- 0.5-0.7: Secondary sources only or some uncertainty
- 0.3-0.5: Limited evidence or conflicting information
- Below 0.3: Speculation or unverified claims (exclude these)
`)

	return sb.String()
}

// buildFactExtractionContent builds the user content for fact extraction
// buildFactExtractionContent 构造 fact 抽取用的 user content
func buildFactExtractionContent(input FactExtractionInput) string {
	var sb strings.Builder

	fmt.Fprintf(&sb, "## Original Query:\n%s\n\n", input.Query)

	if len(input.ResearchAreas) > 0 {
		sb.WriteString("## Research Areas:\n")
		for _, area := range input.ResearchAreas {
			fmt.Fprintf(&sb, "- %s\n", area)
		}
		sb.WriteString("\n")
	}

	sb.WriteString("## Synthesis Content:\n")
	synthesis := input.SynthesisResult
	if len(synthesis) > 10000 {
		synthesis = synthesis[:10000] + "\n...[truncated]"
	}
	sb.WriteString(synthesis)
	sb.WriteString("\n\n")

	if len(input.Citations) > 0 {
		sb.WriteString("## Available Citations:\n")
		for i, c := range input.Citations {
			title := c.Title
			if title == "" {
				title = c.Source
			}
			fmt.Fprintf(&sb, "[%d] %s - %s\n", i+1, title, c.URL)
		}
	}

	return sb.String()
}

// parseFactExtractionResponse parses the LLM response into structured facts
// parseFactExtractionResponse 把 LLM response 解析成结构化 facts
func parseFactExtractionResponse(response string, result *FactExtractionResult) error {
	// Find JSON in response
	// 从 response 里截出 JSON
	start := strings.Index(response, "{")
	end := strings.LastIndex(response, "}")

	if start == -1 || end == -1 || end <= start {
		return fmt.Errorf("no JSON object found in response")
	}

	jsonStr := response[start : end+1]

	var parsed struct {
		Facts           []StructuredFact `json:"facts"`
		ExtractionNotes string           `json:"extraction_notes"`
	}

	if err := json.Unmarshal([]byte(jsonStr), &parsed); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	result.Facts = parsed.Facts

	// Assign IDs if not provided
	// 未提供 ID 时补上
	for i := range result.Facts {
		if result.Facts[i].ID == "" {
			result.Facts[i].ID = fmt.Sprintf("fact-%d", i+1)
		}
	}

	return nil
}
