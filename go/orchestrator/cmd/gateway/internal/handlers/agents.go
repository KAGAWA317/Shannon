package handlers

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	commonpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/common"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

// AgentsHandler handles agent-related HTTP requests
// AgentsHandler 处理 agent 相关的 HTTP 请求
type AgentsHandler struct {
	orchClient orchpb.OrchestratorServiceClient
	logger     *zap.Logger
}

// NewAgentsHandler creates a new agents handler
// NewAgentsHandler 创建 agents handler
func NewAgentsHandler(
	orchClient orchpb.OrchestratorServiceClient,
	logger *zap.Logger,
) *AgentsHandler {
	return &AgentsHandler{
		orchClient: orchClient,
		logger:     logger,
	}
}

// AgentInfo represents agent information for API responses
// AgentInfo 表示 API 响应中的 agent 信息
type AgentInfo struct {
	ID          string         `json:"id"`
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Category    string         `json:"category"`
	Tool        string         `json:"tool"`
	InputSchema map[string]any `json:"input_schema,omitempty"`
	CostPerCall float64        `json:"cost_per_call,omitempty"`
}

// AgentExecuteRequest represents a request to execute an agent
// AgentExecuteRequest 表示执行 agent 的请求
type AgentExecuteRequest struct {
	Input     map[string]any `json:"input"`
	SessionID string         `json:"session_id,omitempty"`
	Stream    bool           `json:"stream,omitempty"`
}

// AgentExecuteResponse represents an agent execution response
// AgentExecuteResponse 表示 agent 执行响应
type AgentExecuteResponse struct {
	TaskID        string    `json:"task_id"`
	AgentID       string    `json:"agent_id"`
	Status        string    `json:"status"`
	Result        any       `json:"result,omitempty"`
	Error         string    `json:"error,omitempty"`
	ExecutionTime int       `json:"execution_time_ms,omitempty"`
	CostUSD       float64   `json:"cost_usd,omitempty"`
	TokensUsed    int       `json:"tokens_used,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
}

// ListAgents handles GET /api/v1/agents
// ListAgents 处理 GET /api/v1/agents
func (h *AgentsHandler) ListAgents(w http.ResponseWriter, r *http.Request) {
	// Get user context from auth middleware
	// 从 auth 中间件获取用户上下文
	if _, ok := r.Context().Value(auth.UserContextKey).(*auth.UserContext); !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Load agents config
	// 加载 agents 配置
	cfg, err := activities.LoadAgentsYAMLConfig()
	if err != nil {
		h.sendError(w, fmt.Sprintf("Failed to load agents config: %v", err), http.StatusInternalServerError)
		return
	}

	// Build response
	// 构造响应
	ids := make([]string, 0, len(cfg.Agents))
	for id := range cfg.Agents {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	agents := make([]AgentInfo, 0, len(ids))
	for _, id := range ids {
		agent := cfg.Agents[id]
		agents = append(agents, AgentInfo{
			ID:          id,
			Name:        agent.Name,
			Description: agent.Description,
			Category:    agent.Category,
			Tool:        agent.Tool,
			InputSchema: agent.InputSchema,
			CostPerCall: agent.CostPerCall,
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"agents": agents,
		"count":  len(agents),
	})
}

// GetAgent handles GET /api/v1/agents/{id}
// GetAgent 处理 GET /api/v1/agents/{id}
func (h *AgentsHandler) GetAgent(w http.ResponseWriter, r *http.Request) {
	// Get user context from auth middleware
	// 从 auth 中间件获取用户上下文
	if _, ok := r.Context().Value(auth.UserContextKey).(*auth.UserContext); !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract agent ID from path
	// 从路径中取出 agent ID
	agentID := r.PathValue("id")
	if agentID == "" {
		h.sendError(w, "Agent ID is required", http.StatusBadRequest)
		return
	}

	// Get agent definition
	// 获取 agent 定义
	agent, err := activities.GetAgentDefinition(agentID)
	if err != nil {
		h.sendError(w, fmt.Sprintf("Agent not found: %s", agentID), http.StatusNotFound)
		return
	}

	info := AgentInfo{
		ID:          agentID,
		Name:        agent.Name,
		Description: agent.Description,
		Category:    agent.Category,
		Tool:        agent.Tool,
		InputSchema: agent.InputSchema,
		CostPerCall: agent.CostPerCall,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(info)
}

// ExecuteAgent handles POST /api/v1/agents/{id}
// ExecuteAgent 处理 POST /api/v1/agents/{id}
func (h *AgentsHandler) ExecuteAgent(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	// Get user context from auth middleware
	// 从 auth 中间件获取用户上下文
	userCtx, ok := ctx.Value(auth.UserContextKey).(*auth.UserContext)
	if !ok {
		h.sendError(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Extract agent ID from path
	// 从路径中取出 agent ID
	agentID := r.PathValue("id")
	if agentID == "" {
		h.sendError(w, "Agent ID is required", http.StatusBadRequest)
		return
	}

	// Validate agent exists
	// 校验 agent 是否存在
	agent, err := activities.GetAgentDefinition(agentID)
	if err != nil {
		h.sendError(w, fmt.Sprintf("Agent not found: %s", agentID), http.StatusNotFound)
		return
	}

	// Parse request body
	// 解析请求体
	var req AgentExecuteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.sendError(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	// Validate input against agent's schema
	// 按 agent 的 schema 校验输入
	if err := ValidateAgentInput(agent, req.Input); err != nil {
		h.sendError(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Generate session ID if not provided
	// 未提供时生成 session ID
	if req.SessionID == "" {
		req.SessionID = uuid.New().String()
	}

	// Build context with agent and input
	// 用 agent 与 input 构造 context
	ctxMap := map[string]any{
		"agent":       agentID,
		"agent_input": req.Input,
	}

	ctxStruct, err := structpb.NewStruct(ctxMap)
	if err != nil {
		h.sendError(w, fmt.Sprintf("Failed to build context: %v", err), http.StatusInternalServerError)
		return
	}

	// Build gRPC request
	// 构造 gRPC 请求
	grpcReq := &orchpb.SubmitTaskRequest{
		Metadata: &commonpb.TaskMetadata{
			UserId:    userCtx.UserID.String(),
			TenantId:  userCtx.TenantID.String(),
			SessionId: req.SessionID,
			Labels: map[string]string{
				"agent": agentID,
			},
		},
		Query:   fmt.Sprintf("Execute agent %s", agentID),
		Context: ctxStruct,
	}

	// Propagate auth/tracing headers to gRPC metadata
	// 把 auth/tracing header 传递到 gRPC metadata
	ctx = withGRPCMetadata(ctx, r)

	// Submit task to orchestrator
	// 把 task 提交给 orchestrator
	resp, err := h.orchClient.SubmitTask(ctx, grpcReq)
	if err != nil {
		if st, ok := status.FromError(err); ok {
			switch st.Code() {
			case codes.InvalidArgument:
				h.sendError(w, st.Message(), http.StatusBadRequest)
			case codes.ResourceExhausted:
				h.sendError(w, "Rate limit exceeded", http.StatusTooManyRequests)
			default:
				h.sendError(w, fmt.Sprintf("Failed to submit task: %v", st.Message()), http.StatusInternalServerError)
			}
		} else {
			h.sendError(w, fmt.Sprintf("Failed to submit task: %v", err), http.StatusInternalServerError)
		}
		return
	}

	h.logger.Info("Agent execution submitted",
		zap.String("task_id", resp.TaskId),
		zap.String("agent_id", agentID),
		zap.String("tool", agent.Tool),
		zap.String("user_id", userCtx.UserID.String()),
	)

	// Return response
	// 返回响应
	execResp := AgentExecuteResponse{
		TaskID:    resp.TaskId,
		AgentID:   agentID,
		Status:    resp.Status.String(),
		CreatedAt: time.Now(),
	}

	// Add workflow ID header for tracing
	// 加上 workflow ID header，便于链路追踪
	w.Header().Set("X-Workflow-ID", resp.WorkflowId)
	w.Header().Set("X-Session-ID", req.SessionID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(execResp)
}

// sendError sends an error response
// sendError 发送错误响应
func (h *AgentsHandler) sendError(w http.ResponseWriter, message string, code int) {
	writeJSON(w, code, map[string]string{"error": message})
}

// ValidateAgentInput validates input against the agent's InputSchema
// Returns nil if valid, or an error describing validation failures
// ValidateAgentInput 依据 agent 的 InputSchema 校验输入：合法返回 nil，否则返回描述校验失败原因的错误。
func ValidateAgentInput(agent *activities.AgentDefinition, input map[string]any) error {
	if agent.InputSchema == nil {
		return nil // No schema defined, allow any input 注：未定义 schema，允许任意输入
	}

	var errors []string

	// Check required fields (reject both missing and null values)
	// 检查必填字段（缺失与显式 null 都拒绝）。
	if required, ok := agent.InputSchema["required"].([]any); ok {
		for _, r := range required {
			fieldName, ok := r.(string)
			if !ok {
				continue
			}
			value, exists := input[fieldName]
			if !exists || value == nil {
				errors = append(errors, fmt.Sprintf("missing required field: %s", fieldName))
			}
		}
	}

	// Validate provided fields against properties schema
	// 按 properties schema 校验已提供的字段
	if properties, ok := agent.InputSchema["properties"].(map[string]any); ok {
		for fieldName, value := range input {
			propSchema, exists := properties[fieldName].(map[string]any)
			if !exists {
				// Field not in schema - reject unknown fields for security
				// 字段不在 schema 中——出于安全考虑拒绝未知字段。
				errors = append(errors, fmt.Sprintf("unknown field: %s (not defined in agent schema)", fieldName))
				continue
			}

			// Type validation
			// 类型校验
			if expectedType, ok := propSchema["type"].(string); ok {
				if err := validateFieldType(fieldName, value, expectedType); err != nil {
					errors = append(errors, err.Error())
				}
			}

			// Enum validation
			// 枚举校验
			if enumValues, ok := propSchema["enum"].([]any); ok {
				if !isValueInEnum(value, enumValues) {
					errors = append(errors, fmt.Sprintf("field %s: value %v not in allowed values %v", fieldName, value, enumValues))
				}
			}
		}
	}

	if len(errors) > 0 {
		return fmt.Errorf("input validation failed: %s", strings.Join(errors, "; "))
	}
	return nil
}

// validateFieldType checks if a value matches the expected JSON schema type
// validateFieldType 判断值是否符合 JSON schema 声明的类型。
func validateFieldType(fieldName string, value any, expectedType string) error {
	if value == nil {
		return nil // nil is acceptable for any type (will be caught by required check) 注：任意类型都接受 nil（必填校验会捕获）
	}

	actualType := reflect.TypeOf(value)
	valid := false

	switch expectedType {
	case "string":
		_, valid = value.(string)
	case "integer":
		switch value.(type) {
		case int, int32, int64, float64:
			// JSON numbers come as float64, accept if whole number
			// JSON 数字解析后是 float64，整数也接受。
			if f, ok := value.(float64); ok {
				valid = f == float64(int64(f))
			} else {
				valid = true
			}
		}
	case "number":
		switch value.(type) {
		case int, int32, int64, float32, float64:
			valid = true
		}
	case "boolean":
		_, valid = value.(bool)
	case "array":
		valid = actualType != nil && actualType.Kind() == reflect.Slice
	case "object":
		_, valid = value.(map[string]any)
	default:
		valid = true // Unknown type, allow 注：未知类型，放行
	}

	if !valid {
		return fmt.Errorf("field %s: expected type %s, got %T", fieldName, expectedType, value)
	}
	return nil
}

// isValueInEnum checks if a value is in the allowed enum values
// isValueInEnum 判断值是否在允许的枚举内。
func isValueInEnum(value any, enumValues []any) bool {
	for _, ev := range enumValues {
		if reflect.DeepEqual(value, ev) {
			return true
		}
	}
	return false
}
