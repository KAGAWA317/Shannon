package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/interceptors"
	"go.uber.org/zap"
)

const (
	maxTitleLength = 60
	// sessionContextKeyTitle is the key for session title in the context JSONB field
	// sessionContextKeyTitle 是 title 在 context JSONB 字段中的 key。
	sessionContextKeyTitle = "title"
)

// GenerateSessionTitleInput is the input for generating a session title
// GenerateSessionTitleInput 是生成 session title 的输入。
type GenerateSessionTitleInput struct {
	SessionID string `json:"session_id"`
	Query     string `json:"query"`
}

// GenerateSessionTitleResult is the result of title generation
// GenerateSessionTitleResult 是 title 生成的结果。
type GenerateSessionTitleResult struct {
	Title   string `json:"title"`
	Success bool   `json:"success"`
	Error   string `json:"error,omitempty"`
}

// GenerateSessionTitle generates a short title for a session based on the first query
// Uses LLM to create a concise 3-5 word title, with fallback to query truncation
// GenerateSessionTitle 基于首个 query 为 session 生成短标题。
// 用 LLM 产出 3-5 个词的简洁标题，失败时回退为截断 query。
func (a *Activities) GenerateSessionTitle(ctx context.Context, input GenerateSessionTitleInput) (GenerateSessionTitleResult, error) {
	a.logger.Info("Generating session title",
		zap.String("session_id", input.SessionID),
		zap.Int("query_length", len(input.Query)),
	)

	// Validate input
	// 校验输入。
	if input.SessionID == "" {
		return GenerateSessionTitleResult{
			Success: false,
			Error:   "session_id is required",
		}, fmt.Errorf("session_id is required")
	}

	if input.Query == "" {
		return GenerateSessionTitleResult{
			Success: false,
			Error:   "query is required",
		}, fmt.Errorf("query is required")
	}

	// Check if session exists and already has a title
	// 检查 session 是否存在、是否已有 title。
	sess, err := a.sessionManager.GetSession(ctx, input.SessionID)
	if err != nil {
		a.logger.Error("Failed to get session for title generation",
			zap.Error(err),
			zap.String("session_id", input.SessionID),
		)
		return GenerateSessionTitleResult{
			Success: false,
			Error:   fmt.Sprintf("session not found: %v", err),
		}, err
	}

	if sess == nil {
		err := fmt.Errorf("session is nil for id: %s", input.SessionID)
		a.logger.Error("Session is nil after successful fetch",
			zap.String("session_id", input.SessionID),
		)
		return GenerateSessionTitleResult{
			Success: false,
			Error:   "session not found",
		}, err
	}

	// Check if session already has a title
	// 检查 session 是否已有 title。
	if sess.Context != nil {
		if existingTitle, ok := sess.Context[sessionContextKeyTitle].(string); ok && existingTitle != "" {
			a.logger.Info("Session already has a title, skipping generation",
				zap.String("session_id", input.SessionID),
				zap.String("existing_title", existingTitle),
			)
			return GenerateSessionTitleResult{
				Title:   existingTitle,
				Success: true,
			}, nil
		}
	}

	// Try LLM-based title generation
	// 尝试用 LLM 生成 title。
	title, err := a.generateTitleWithLLM(ctx, input.Query)
	if err != nil {
		a.logger.Warn("LLM title generation failed, using fallback",
			zap.Error(err),
			zap.String("session_id", input.SessionID),
		)
		// Fallback to truncated query
		// 回退到截断后的 query。
		title = generateFallbackTitle(input.Query)
	}

	// Ensure title meets length constraints (use runes to avoid UTF-8 corruption)
	// 确保 title 满足长度限制（按 rune 计，避免破坏 UTF-8）。
	titleRunes := []rune(title)
	if len(titleRunes) > maxTitleLength {
		title = string(titleRunes[:maxTitleLength-3]) + "..."
	}

	// Update session context with the generated title
	// First update Redis via session manager
	// 把生成的 title 写入 session context。
	// 先通过 session manager 更新 Redis。
	if err := a.sessionManager.UpdateContext(ctx, input.SessionID, sessionContextKeyTitle, title); err != nil {
		a.logger.Error("Failed to update session with title in Redis",
			zap.Error(err),
			zap.String("session_id", input.SessionID),
		)
		return GenerateSessionTitleResult{
			Title:   title,
			Success: false,
			Error:   fmt.Sprintf("failed to save title to Redis: %v", err),
		}, err
	}

	// Also update Postgres so HTTP APIs can see the title
	// 同时更新 Postgres，让 HTTP API 能看到 title。
	dbClient := GetGlobalDBClient()
	if dbClient != nil {
		db := dbClient.GetDB()

		// First, fetch existing context from PostgreSQL to preserve external_id and other DB-only fields
		// 先读出 PostgreSQL 中已有的 context，保留 external_id 等仅存在于 DB 的字段。
		var existingContext json.RawMessage
		err := db.QueryRowContext(ctx, `
			SELECT context
			FROM sessions
			WHERE (id::text = $1 OR context->>'external_id' = $1) AND deleted_at IS NULL
		`, input.SessionID).Scan(&existingContext)

		// Build updated context JSON with title
		// 组装带 title 的 context JSON。
		contextData := make(map[string]interface{})

		// First load PostgreSQL context (to preserve external_id)
		// 先加载 PostgreSQL 的 context（保留 external_id）。
		if err == nil && existingContext != nil {
			if err := json.Unmarshal(existingContext, &contextData); err != nil {
				a.logger.Warn("Failed to unmarshal existing Postgres context",
					zap.Error(err),
					zap.String("session_id", input.SessionID),
				)
			}
		}

		// Then overlay Redis context (to get latest updates)
		// 再用 Redis 的 context 覆盖（取最新数据）。
		if sess.Context != nil {
			for k, v := range sess.Context {
				contextData[k] = v
			}
		}

		// Finally add the title
		// 最后写入 title。
		contextData[sessionContextKeyTitle] = title

		contextJSON, err := json.Marshal(contextData)
		if err != nil {
			a.logger.Warn("Failed to marshal context for Postgres update",
				zap.Error(err),
				zap.String("session_id", input.SessionID),
			)
		} else {
			// Update Postgres sessions.context (support both UUID and external_id)
			// 更新 Postgres 的 sessions.context（同时支持 UUID 和 external_id）。
			_, err = db.ExecContext(ctx, `
				UPDATE sessions
				SET context = $1, updated_at = NOW()
				WHERE (id::text = $2 OR context->>'external_id' = $2) AND deleted_at IS NULL
			`, contextJSON, input.SessionID)
			if err != nil {
				a.logger.Warn("Failed to update Postgres with title",
					zap.Error(err),
					zap.String("session_id", input.SessionID),
				)
			}
		}
	}

	a.logger.Info("Session title generated",
		zap.String("session_id", input.SessionID),
		zap.String("title", title),
	)

	return GenerateSessionTitleResult{
		Title:   title,
		Success: true,
	}, nil
}

// generateTitleWithLLM uses LLM to generate a concise title
// generateTitleWithLLM 用 LLM 生成简洁 title。
func (a *Activities) generateTitleWithLLM(ctx context.Context, query string) (string, error) {
	// Prepare request to LLM service
	// 准备发给 LLM service 的 request。
	// Use language-aware prompt that respects the query's original language
	// 用语言感知的 prompt，保持与 query 原始语言一致。
	prompt := fmt.Sprintf(`Generate a chat session title from this user query.

Rules:
- Use the SAME LANGUAGE as the user's query (e.g., if query is in Japanese, title must be in Japanese)
- For English: 3-5 words, Title Case
- For Chinese/Japanese/Korean: 5-15 characters (CJK languages use fewer words but more meaning per character)
- No quotes, no trailing punctuation, no emojis, no sensitive identifiers
- Output ONLY the title, nothing else

Query: %s`, query)

	llmServiceURL := getEnvOrDefaultTitle("LLM_SERVICE_URL", "http://llm-service:8000")
	url := fmt.Sprintf("%s/agent/query", llmServiceURL)

	reqBody := map[string]interface{}{
		"query":       prompt,
		"max_tokens":  9126, // Increased for long input queries + output
		// 为长 query 输入加输出留出余量。
		"temperature": 0.3,  // Low temperature for consistency
		// 低 temperature，保证输出稳定。
		"agent_id":    "title_generator",
		"context": map[string]interface{}{
			"system_prompt": "You are a multilingual title generator. Generate concise, descriptive titles for chat sessions in the SAME LANGUAGE as the user's input. For CJK languages (Chinese, Japanese, Korean), use 5-15 characters. For English and other Western languages, use 3-5 words in Title Case. No quotes, no trailing punctuation, no emojis. Output ONLY the title.",
		},
	}

	reqJSON, err := json.Marshal(reqBody)
	if err != nil {
		return "", fmt.Errorf("failed to marshal request: %w", err)
	}

	// Call LLM service
	// 调用 LLM service。
	client := &http.Client{
		Timeout:   15 * time.Second,
		Transport: interceptors.NewWorkflowHTTPRoundTripper(nil),
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(reqJSON)))
	if err != nil {
		return "", fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", "title_generator")

	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("LLM service call failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return "", fmt.Errorf("HTTP %d from LLM service", resp.StatusCode)
	}

	// Parse response
	// 解析 response。
	var result struct {
		Success  bool   `json:"success"`
		Response string `json:"response"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("failed to parse LLM response: %w", err)
	}

	if !result.Success {
		return "", fmt.Errorf("LLM service returned success=false")
	}

	// Clean up the response
	// 清理 response 内容。
	title := strings.TrimSpace(result.Response)
	title = strings.Trim(title, `"'`)

	if title == "" {
		return "", fmt.Errorf("LLM returned empty title")
	}

	// Detect error messages that shouldn't be used as titles
	// 识别不能当 title 用的错误信息。
	if strings.Contains(title, "[Incomplete response:") ||
		strings.Contains(title, "token limit") ||
		strings.Contains(title, "truncated") ||
		len(title) > 200 { // Titles should be short, if it's too long it's probably an error
		// title 应当很短，过长多半是错误信息。
		return "", fmt.Errorf("LLM returned invalid title (likely an error message)")
	}

	// Truncate by runes (characters) not bytes to avoid corrupting UTF-8
	// 按 rune（字符）而非 byte 截断，避免破坏 UTF-8。
	titleRunes := []rune(title)
	if len(titleRunes) > maxTitleLength {
		title = string(titleRunes[:maxTitleLength-3]) + "..."
	}

	return title, nil
}

// getEnvOrDefaultTitle is a helper to get environment variables
// getEnvOrDefaultTitle 是读取环境变量的辅助函数。
func getEnvOrDefaultTitle(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

// generateFallbackTitle creates a simple title from the query by truncating
// generateFallbackTitle 通过截断 query 生成简单 title。
func generateFallbackTitle(query string) string {
	// Remove leading/trailing whitespace
	// 去掉首尾空白。
	title := strings.TrimSpace(query)

	// Take first line if multiline
	// 多行时只取第一行。
	if idx := strings.Index(title, "\n"); idx > 0 {
		title = title[:idx]
	}

	// Truncate to reasonable length (use runes to handle UTF-8 properly)
	// 截到合理长度（按 rune 处理，保证 UTF-8 正确）。
	maxLen := 40
	titleRunes := []rune(title)
	if len(titleRunes) > maxLen {
		// Try to break at word boundary
		// 尽量在词边界处断开。
		truncated := string(titleRunes[:maxLen])
		if lastSpace := strings.LastIndex(truncated, " "); lastSpace > 20 {
			truncated = truncated[:lastSpace]
		}
		title = truncated + "..."
	}

	return title
}
