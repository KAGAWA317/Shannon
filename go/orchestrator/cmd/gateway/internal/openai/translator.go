package openai

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"strings"

	commonpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/common"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"google.golang.org/protobuf/types/known/structpb"
)

// TranslatedRequest contains the Shannon-native request and metadata.
// TranslatedRequest 包含 Shannon 原生请求与 metadata。
type TranslatedRequest struct {
	GRPCRequest *orchpb.SubmitTaskRequest
	SessionID   string
	ModelName   string
	Stream      bool
}

// Translator converts OpenAI requests to Shannon format.
// Translator 把 OpenAI 请求转换为 Shannon 格式。
type Translator struct {
	registry *Registry
}

// NewTranslator creates a new request translator.
// NewTranslator 创建新的请求翻译器。
func NewTranslator(registry *Registry) *Translator {
	return &Translator{registry: registry}
}

// Translate converts an OpenAI ChatCompletionRequest to a Shannon SubmitTaskRequest.
// Translate 把 OpenAI ChatCompletionRequest 转换为 Shannon 的 SubmitTaskRequest。
func (t *Translator) Translate(req *ChatCompletionRequest, userID, tenantID string) (*TranslatedRequest, error) {
	return t.TranslateWithSession(req, userID, tenantID, nil)
}

// TranslateWithSession converts an OpenAI request using an existing session result.
// TranslateWithSession 使用已有 session 结果转换 OpenAI 请求。
func (t *Translator) TranslateWithSession(req *ChatCompletionRequest, userID, tenantID string, session *SessionResult) (*TranslatedRequest, error) {
	// Validate request
	// 校验请求
	if len(req.Messages) == 0 {
		return nil, fmt.Errorf("messages array is required")
	}

	// Get model configuration
	// 获取 model 配置
	modelName := req.Model
	if modelName == "" {
		modelName = t.registry.GetDefaultModel()
	}

	modelConfig, err := t.registry.GetModel(modelName)
	if err != nil {
		return nil, fmt.Errorf("invalid model: %s", modelName)
	}

	// Extract the query from messages
	// 从 messages 中提取 query
	query := t.extractQuery(req.Messages)
	if query == "" {
		return nil, fmt.Errorf("no user message found in messages array")
	}

	// Use session from session manager
	// 使用 session manager 提供的 session
	sessionID := ""
	if session != nil {
		sessionID = session.ShannonSession
	}
	if sessionID == "" {
		sessionID = t.deriveSessionID(req, userID)
	}

	// Build context map from model config + request parameters
	// 由 model 配置 + 请求参数构建 context map
	ctxMap := t.buildContext(req, modelConfig)

	// Sanitize context for structpb compatibility (converts []string to []interface{}, etc.)
	// 清洗 context 以兼容 structpb（把 []string 转为 []interface{} 等）
	sanitizedCtx := sanitizeForStructpb(ctxMap)

	// Convert context to protobuf struct
	// 把 context 转换为 protobuf struct
	ctxStruct, err := structpb.NewStruct(sanitizedCtx)
	if err != nil {
		return nil, fmt.Errorf("failed to build context: %w", err)
	}

	// Build labels for mode routing
	// 构建用于 mode 路由的 labels
	labels := map[string]string{}
	// If context.agent is present, use "standard" mode to route through OrchestratorWorkflow
	// 若存在 context.agent，则使用 "standard" mode 走 OrchestratorWorkflow
	// which has the AgentWorkflow routing logic
	// 因为它带有 AgentWorkflow 的路由逻辑
	if _, hasAgent := ctxMap["agent"].(string); hasAgent {
		labels["mode"] = "standard"
	} else {
		switch modelConfig.WorkflowMode {
		case "simple":
			labels["mode"] = "simple"
		case "research":
			labels["mode"] = "standard"
		case "supervisor":
			labels["mode"] = "supervisor"
		default:
			labels["mode"] = "simple"
		}
	}

	// Build the gRPC request
	// 构建 gRPC 请求
	grpcReq := &orchpb.SubmitTaskRequest{
		Metadata: &commonpb.TaskMetadata{
			UserId:    userID,
			TenantId:  tenantID,
			SessionId: sessionID,
			Labels:    labels,
		},
		Query:   query,
		Context: ctxStruct,
	}

	return &TranslatedRequest{
		GRPCRequest: grpcReq,
		SessionID:   sessionID,
		ModelName:   modelName,
		Stream:      req.Stream,
	}, nil
}

// extractQuery extracts the query from the messages array.
// Uses the last user message as the primary query.
// Accepts messages with text content OR attachments (attachment-only is valid).
// extractQuery 从 messages 中提取 query：以最后一条 user 消息为主查询；
// 文本内容和附件都算有效输入（只有附件也可以）。
func (t *Translator) extractQuery(messages []ChatMessage) string {
	// Find the last user message
	// 找到最后一条 user 消息
	for _, message := range slices.Backward(messages) {
		if message.Role == "user" {
			if strings.TrimSpace(message.Content) != "" {
				return message.Content
			}
			// Attachment-only message: use attachment summary as query
			// 仅含附件的消息：用附件摘要作为 query
			if len(message.RawAttachments) > 0 {
				if message.ContentWithAttachmentSummary != "" {
					return message.ContentWithAttachmentSummary
				}
				return "[User sent file attachments]"
			}
		}
	}
	return ""
}

// deriveSessionID generates a session ID from the conversation.
// Uses hash of authenticated userID + system message + first user message for consistency.
// userID is included to prevent cross-user session collision.
// deriveSessionID 由对话内容生成 session ID：
// 使用 authenticated userID + system 消息 + 首条 user 消息 的哈希，保证同一对话稳定命中同一 session；
// 把 userID 计入哈希，避免不同用户之间发生 session 冲突。
func (t *Translator) deriveSessionID(req *ChatCompletionRequest, userID string) string {
	// If user provided a custom user ID, combine with auth user for isolation
	// 用户提供了自定义 user ID 时，再与 authentication 用户组合，以保持隔离。
	if req.User != "" {
		if userID != "" {
			return "openai-" + userID + "-" + req.User
		}
		return "openai-" + req.User
	}

	// Build a hash from authenticated user + conversation start
	// 由 authenticated 用户 + 对话起点构建哈希
	var parts []string

	// Include authenticated user ID for cross-user isolation
	// 计入 authenticated 用户 ID，以隔离不同用户
	if userID != "" {
		parts = append(parts, userID)
	}

	for _, msg := range req.Messages {
		if msg.Role == "system" {
			parts = append(parts, msg.Content)
			break
		}
	}
	// Add first user message (use attachment summary for attachment-only messages)
	// 加入首条 user 消息（仅含附件的消息用附件摘要）
	for _, msg := range req.Messages {
		if msg.Role == "user" {
			content := msg.Content
			if content == "" && msg.ContentWithAttachmentSummary != "" {
				content = msg.ContentWithAttachmentSummary
			}
			if len(content) > 100 {
				content = content[:100]
			}
			if content != "" {
				parts = append(parts, content)
			}
			break
		}
	}

	if len(parts) == 0 {
		// Fallback: generate unique session
		// 兜底：生成唯一 session
		return "openai-" + GenerateCompletionID()
	}

	// Hash the parts
	// 对各部分做哈希
	h := sha256.New()
	h.Write([]byte(strings.Join(parts, "|")))
	hash := hex.EncodeToString(h.Sum(nil))[:16]
	return "openai-" + hash
}

// buildContext creates the Shannon context map from request parameters.
// buildContext 依据 request 参数构造 Shannon 的 context map。
func (t *Translator) buildContext(req *ChatCompletionRequest, modelConfig *ModelConfig) map[string]any {
	ctx := make(map[string]any)

	// Copy model config context
	// 复制 model 配置中的 context
	maps.Copy(ctx, modelConfig.Context)

	// Apply shannon_options if present (these take precedence)
	// 先应用 shannon_options（其优先级更高）。
	if req.ShannonOptions != nil {
		// Copy context from shannon_options
		// 复制 shannon_options 中的 context
		maps.Copy(ctx, req.ShannonOptions.Context)

		// Agent routing (single-purpose deterministic agents)
		// Agent 路由（单一用途的确定性 agent）
		if req.ShannonOptions.Agent != "" {
			ctx["agent"] = req.ShannonOptions.Agent
		}

		// Agent input (can be provided with explicit agent or via model-based agent routing)
		// Agent 输入（可与显式 agent 一起提供，或经由基于 model 的 agent 路由提供）
		if req.ShannonOptions.AgentInput != nil {
			ctx["agent_input"] = req.ShannonOptions.AgentInput
		}

		// Role routing (workflow roles like browser_use)
		// Role 路由（如 browser_use 之类的 workflow 角色）
		if req.ShannonOptions.Role != "" {
			ctx["role"] = req.ShannonOptions.Role
		}

		// Research strategy
		// 研究策略
		if req.ShannonOptions.ResearchStrategy != "" {
			ctx["research_strategy"] = req.ShannonOptions.ResearchStrategy
		}

		// Model tier override
		// 覆盖 model tier
		if req.ShannonOptions.ModelTier != "" {
			ctx["model_tier"] = req.ShannonOptions.ModelTier
		}
	}

	// Apply research strategy preset defaults for chat-completion research requests.
	// 为 chat-completion 研究类请求套用研究策略预设默认值。
	applyShannonResearchPreset(ctx)

	// Apply max_tokens
	// 应用 max_tokens
	if req.MaxTokens > 0 {
		maxLimit := t.registry.GetMaxTokensLimit()
		if req.MaxTokens > maxLimit {
			ctx["max_tokens"] = maxLimit
		} else {
			ctx["max_tokens"] = req.MaxTokens
		}
	} else if modelConfig.MaxTokensDefault > 0 {
		ctx["max_tokens"] = modelConfig.MaxTokensDefault
	}

	// Apply temperature (pointer allows distinguishing "not set" from 0)
	// 应用 temperature（用指针区分“未设置”和 0）。
	if req.Temperature != nil {
		ctx["temperature"] = *req.Temperature
	}

	// Apply top_p (pointer allows distinguishing "not set" from 0)
	// 应用 top_p（用指针区分“未设置”和 0）
	if req.TopP != nil {
		ctx["top_p"] = *req.TopP
	}

	// Apply stop sequences
	// 应用 stop 序列
	if len(req.Stop) > 0 {
		ctx["stop"] = req.Stop
	}

	// Apply user ID for tracking
	// 应用 user ID 以便跟踪
	if req.User != "" {
		ctx["openai_user"] = req.User
	}

	// Build system prompt from conversation context
	// 由对话内容构建 system prompt
	systemPrompt := t.extractSystemPrompt(req.Messages)
	if systemPrompt != "" {
		ctx["system_prompt"] = systemPrompt
	}

	// Include conversation history for context (except last user message which is the query)
	// 把对话历史一并放入 context（最后一条 user 消息除外，它就是 query）。
	history := t.buildConversationHistory(req.Messages)
	if len(history) > 0 {
		ctx["conversation_history"] = history
	}

	return ctx
}

// applyShannonResearchPreset seeds model_tier defaults for research requests
// arriving via chat completions. Mirrors the task handler's applyStrategyPreset
// logic: force_research without explicit strategy defaults to "standard".
// applyShannonResearchPreset 为通过 chat completions 进来的研究 request 填充 model_tier 默认值；
// 与 task handler 的 applyStrategyPreset 逻辑一致：force_research 未显式指定 strategy 时默认 standard。
func applyShannonResearchPreset(ctx map[string]any) {
	if ctx == nil {
		return
	}

	// Default force_research to "standard" strategy when none specified
	// (matches task.go:340-344 behavior)
	// 未指定 strategy 时，把 force_research 的 strategy 默认设为 "standard"（与 task.go:340-344 行为一致）。
	rs, rsOk := ctx["research_strategy"].(string)
	if !rsOk || strings.TrimSpace(rs) == "" {
		if forceResearch, _ := ctx["force_research"].(bool); forceResearch {
			rs = "standard"
			ctx["research_strategy"] = rs
		}
	}

	if strings.TrimSpace(rs) == "" {
		return
	}
	if _, hasTier := ctx["model_tier"]; hasTier {
		return
	}

	switch strings.ToLower(strings.TrimSpace(rs)) {
	case "quick", "standard":
		ctx["model_tier"] = "small"
	case "deep", "academic":
		ctx["model_tier"] = "medium"
	}
}

// extractSystemPrompt extracts the system prompt from messages.
// extractSystemPrompt 从 messages 中提取 system prompt。
func (t *Translator) extractSystemPrompt(messages []ChatMessage) string {
	for _, msg := range messages {
		if msg.Role == "system" {
			return msg.Content
		}
	}
	return ""
}

// buildConversationHistory formats previous messages for context.
// buildConversationHistory 把先前的消息整理成 context。
func (t *Translator) buildConversationHistory(messages []ChatMessage) []map[string]string {
	var history []map[string]string

	// Skip system messages and the last user message (which is the query)
	// 跳过 system 消息和最后一条 user 消息（它就是 query）。
	lastUserIdx := -1
	for i, message := range slices.Backward(messages) {
		if message.Role == "user" {
			lastUserIdx = i
			break
		}
	}

	for i, msg := range messages {
		if msg.Role == "system" {
			continue // System prompt handled separately 注：system prompt 单独处理
		}
		if i == lastUserIdx {
			continue // Last user message is the query 注：最后一条 user 消息就是 query
		}
		// Use attachment-enriched content for history so downstream sees
		// "[Attached: ...]" summaries (from Task 2 content parsing).
		// 历史消息使用补充过附件摘要的 content，让下游能看到 "[Attached: ...]" 摘要（来自 Task 2 的内容解析）。
		content := msg.Content
		if msg.ContentWithAttachmentSummary != "" {
			content = msg.ContentWithAttachmentSummary
		}
		history = append(history, map[string]string{
			"role":    msg.Role,
			"content": content,
		})
	}

	return history
}

// sanitizeForStructpb converts values to types compatible with structpb.NewStruct.
// structpb.NewStruct only accepts:
// - nil, bool, int/float64, string
// - map[string]interface{} (recursively sanitized)
// - []interface{} (NOT []string, []int, etc.)
// sanitizeForStructpb 把值转成 structpb.NewStruct 可接受的类型。structpb 只接受：
// nil、bool、int/float64、string；map[string]interface{}（递归清洗）；[]interface{}（不接受 []string、[]int 等）。
func sanitizeForStructpb(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	result := make(map[string]any, len(m))
	for k, v := range m {
		result[k] = sanitizeValue(v)
	}
	return result
}

// sanitizeValue converts a single value to structpb-compatible type.
// sanitizeValue 把单个值转换为 structpb 兼容类型。
func sanitizeValue(v any) any {
	if v == nil {
		return nil
	}

	switch val := v.(type) {
	case bool, int, int32, int64, float32, float64, string:
		return val
	case []any:
		// Already correct type, but sanitize elements
		// 类型已正确，但仍需清洗各元素
		result := make([]any, len(val))
		for i, elem := range val {
			result[i] = sanitizeValue(elem)
		}
		return result
	case map[string]any:
		// Recursively sanitize nested maps
		// 递归清洗嵌套 map
		return sanitizeForStructpb(val)
	default:
		// Use reflection for typed slices and maps
		// 对带类型的 slice 和 map 使用反射
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Slice, reflect.Array:
			return sanitizeSliceValue(rv)
		case reflect.Map:
			return sanitizeMapValue(rv)
		default:
			// For other types, return as-is and let structpb handle/reject
			// 其它类型原样返回，交由 structpb 处理/拒绝
			return v
		}
	}
}

// sanitizeSliceValue converts typed slices ([]string, []int, etc.) to []interface{}.
// sanitizeSliceValue 把带类型的 slice（[]string、[]int 等）转换为 []interface{}。
func sanitizeSliceValue(rv reflect.Value) []any {
	length := rv.Len()
	result := make([]any, length)
	for i := range length {
		result[i] = sanitizeValue(rv.Index(i).Interface())
	}
	return result
}

// sanitizeMapValue converts typed maps (map[string]string, etc.) to map[string]interface{}.
// sanitizeMapValue 把带类型的 map（map[string]string 等）转换为 map[string]interface{}。
func sanitizeMapValue(rv reflect.Value) map[string]any {
	if rv.Type().Key().Kind() != reflect.String {
		// structpb only supports string keys
		// structpb 仅支持 string 类型的 key
		return nil
	}
	result := make(map[string]any, rv.Len())
	iter := rv.MapRange()
	for iter.Next() {
		key := iter.Key().String()
		result[key] = sanitizeValue(iter.Value().Interface())
	}
	return result
}
