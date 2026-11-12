package handlers

import (
	"encoding/json"
	"net/http"
)

// OpenAPIHandler serves the OpenAPI specification
type OpenAPIHandler struct {
	spec map[string]any
}

// NewOpenAPIHandler creates a new OpenAPI handler
func NewOpenAPIHandler() *OpenAPIHandler {
	return &OpenAPIHandler{
		spec: generateOpenAPISpec(),
	}
}

// ServeSpec handles GET /openapi.json
func (h *OpenAPIHandler) ServeSpec(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	json.NewEncoder(w).Encode(h.spec)
}

// generateOpenAPISpec creates the OpenAPI 3.0 specification
// generateOpenAPISpec 构建 OpenAPI 3.0 规范。
func generateOpenAPISpec() map[string]any {
	return map[string]any{
		"openapi": "3.0.0",
		"info": map[string]any{
			"title":       "Shannon Gateway API",
			"version":     "0.1.0",
			"description": "REST API for Shannon multi-agent AI platform",
		},
		"servers": []map[string]any{
			{
				"url":         "http://localhost:8080",
				"description": "Local development server",
			},
			{
				"url":         "https://api.shannon.ai",
				"description": "Production server",
			},
		},
		"security": []map[string]any{
			{"apiKey": []string{}},
		},
		"paths": map[string]any{
			"/health": map[string]any{
				"get": map[string]any{
					"summary":     "Health check",
					"description": "Check if the gateway is healthy",
					"security":    []any{},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Gateway is healthy",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/HealthResponse",
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/tasks": map[string]any{
				"get": map[string]any{
					"summary":     "List tasks",
					"description": "List tasks for the authenticated user (optionally filter by session/status)",
					"parameters": []map[string]any{
						{"name": "limit", "in": "query", "schema": map[string]any{"type": "integer", "default": 20, "minimum": 1, "maximum": 100}},
						{"name": "offset", "in": "query", "schema": map[string]any{"type": "integer", "default": 0, "minimum": 0}},
						{"name": "status", "in": "query", "schema": map[string]any{"type": "string", "enum": []string{"QUEUED", "RUNNING", "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"}}},
						{"name": "session_id", "in": "query", "schema": map[string]any{"type": "string"}},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "List of tasks",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/ListTasksResponse",
									},
								},
							},
						},
					},
				},
				"post": map[string]any{
					"summary":     "Submit a task",
					"description": "Submit a new task for processing",
					"parameters": []map[string]any{
						{
							"name":        "Idempotency-Key",
							"in":          "header",
							"description": "Unique key for idempotent requests",
							"required":    false,
							"schema": map[string]any{
								"type": "string",
							},
						},
					},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/TaskRequest",
								},
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Task submitted successfully",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/TaskResponse",
									},
								},
							},
						},
						"400": map[string]any{
							"description": "Invalid request",
						},
						"401": map[string]any{
							"description": "Unauthorized",
						},
						"429": map[string]any{
							"description": "Rate limit exceeded",
						},
					},
				},
			},
			"/api/v1/tasks/stream": map[string]any{
				"post": map[string]any{
					"summary":     "Submit task and get stream URL",
					"description": "Submit a task and immediately receive the workflow ID and SSE stream URL for real-time updates. This is a convenience endpoint that combines task submission with stream URL generation in a single call.",
					"parameters": []map[string]any{
						{
							"name":        "Idempotency-Key",
							"in":          "header",
							"description": "Unique key for idempotent requests",
							"required":    false,
							"schema": map[string]any{
								"type": "string",
							},
						},
					},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/TaskRequest",
								},
							},
						},
					},
					"responses": map[string]any{
						"201": map[string]any{
							"description": "Task submitted successfully with stream URL",
							"headers": map[string]any{
								"X-Workflow-ID": map[string]any{
									"description": "The workflow ID for this task",
									"schema": map[string]any{
										"type": "string",
									},
								},
								"X-Session-ID": map[string]any{
									"description": "The session ID associated with this task",
									"schema": map[string]any{
										"type": "string",
									},
								},
								"Link": map[string]any{
									"description": "Link header with stream URL (rel=stream)",
									"schema": map[string]any{
										"type": "string",
									},
								},
							},
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/TaskStreamResponse",
									},
								},
							},
						},
						"400": map[string]any{
							"description": "Invalid request",
						},
						"401": map[string]any{
							"description": "Unauthorized",
						},
						"429": map[string]any{
							"description": "Rate limit exceeded",
						},
					},
				},
			},
			"/api/v1/tasks/{id}": map[string]any{
				"get": map[string]any{
					"summary":     "Get task status",
					"description": "Get the status of a submitted task",
					"parameters": []map[string]any{
						{
							"name":        "id",
							"in":          "path",
							"description": "Task ID",
							"required":    true,
							"schema": map[string]any{
								"type":      "string",
								"minLength": 1,
								"maxLength": 128,
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Task status retrieved",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/TaskStatusResponse",
									},
								},
							},
						},
						"404": map[string]any{
							"description": "Task not found",
						},
					},
				},
			},
			"/api/v1/tasks/{id}/cancel": map[string]any{
				"post": map[string]any{
					"summary":     "Cancel a task",
					"description": "Cancel a running or queued task. Returns 202 Accepted for async cancellation, 409 if task is already in terminal state, 404 if not found.",
					"parameters": []map[string]any{
						{
							"name":        "id",
							"in":          "path",
							"description": "Task ID",
							"required":    true,
							"schema": map[string]any{
								"type":      "string",
								"minLength": 1,
								"maxLength": 128,
							},
						},
					},
					"requestBody": map[string]any{
						"required": false,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/CancelTaskRequest",
								},
							},
						},
					},
					"responses": map[string]any{
						"202": map[string]any{
							"description": "Cancellation accepted",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/CancelTaskResponse",
									},
								},
							},
						},
						"401": map[string]any{
							"description": "Unauthorized - Invalid or missing API key",
						},
						"403": map[string]any{
							"description": "Forbidden - User does not have permission to cancel this task",
						},
						"404": map[string]any{
							"description": "Task not found",
						},
						"409": map[string]any{
							"description": "Task already in terminal state",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/CancelTaskResponse",
									},
								},
							},
						},
						"429": map[string]any{
							"description": "Rate limit exceeded",
						},
					},
				},
			},
			"/api/v1/tasks/{id}/stream": map[string]any{
				"get": map[string]any{
					"summary":     "Stream task events",
					"description": "Stream real-time events for a task via Server-Sent Events",
					"parameters": []map[string]any{
						{
							"name":        "id",
							"in":          "path",
							"description": "Task ID",
							"required":    true,
							"schema": map[string]any{
								"type":      "string",
								"minLength": 1,
								"maxLength": 128,
							},
						},
						{
							"name":        "types",
							"in":          "query",
							"description": "Comma-separated list of event types to filter",
							"required":    false,
							"schema": map[string]any{
								"type": "string",
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Event stream established",
							"content": map[string]any{
								"text/event-stream": map[string]any{
									"schema": map[string]any{
										"type": "string",
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/stream/sse": map[string]any{
				"get": map[string]any{
					"summary":     "SSE stream",
					"description": "Server-Sent Events stream for workflow events",
					"parameters": []map[string]any{
						{
							"name":        "workflow_id",
							"in":          "query",
							"description": "Workflow ID to stream events for",
							"required":    true,
							"schema": map[string]any{
								"type":      "string",
								"minLength": 1,
								"maxLength": 128,
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Event stream",
						},
					},
				},
			},
			"/api/v1/tasks/{id}/events": map[string]any{
				"get": map[string]any{
					"summary":     "Get task events",
					"description": "Get historical events for a task",
					"parameters": []map[string]any{
						{"name": "id", "in": "path", "required": true, "schema": map[string]any{"type": "string", "minLength": 1, "maxLength": 128}},
						{"name": "limit", "in": "query", "schema": map[string]any{"type": "integer", "default": 50, "minimum": 1, "maximum": 200}},
						{"name": "offset", "in": "query", "schema": map[string]any{"type": "integer", "default": 0, "minimum": 0}},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Task events",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"events": map[string]any{"type": "array", "items": map[string]any{"$ref": "#/components/schemas/TaskEvent"}},
											"count":  map[string]any{"type": "integer"},
										},
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/tasks/{id}/timeline": map[string]any{
				"get": map[string]any{
					"summary":     "Build timeline from Temporal history",
					"description": "Derive a human-readable timeline from Temporal history; optionally persist asynchronously",
					"parameters": []map[string]any{
						{"name": "id", "in": "path", "required": true, "schema": map[string]any{"type": "string"}},
						{"name": "run_id", "in": "query", "schema": map[string]any{"type": "string"}},
						{"name": "mode", "in": "query", "schema": map[string]any{"type": "string", "enum": []string{"summary", "full"}, "default": "summary"}},
						{"name": "include_payloads", "in": "query", "schema": map[string]any{"type": "boolean", "default": false}},
						{"name": "persist", "in": "query", "schema": map[string]any{"type": "boolean", "default": true}},
					},
					"responses": map[string]any{
						"200": map[string]any{"description": "Timeline returned inline"},
						"202": map[string]any{"description": "Accepted for async persistence"},
					},
				},
			},
			"/api/v1/sessions": map[string]any{
				"get": map[string]any{
					"summary":     "List all sessions",
					"description": "Retrieve all sessions for the authenticated user with pagination support",
					"parameters": []map[string]any{
						{
							"name":        "limit",
							"in":          "query",
							"description": "Maximum number of sessions to return",
							"schema": map[string]any{
								"type":    "integer",
								"default": 20,
								"minimum": 1,
								"maximum": 100,
							},
						},
						{
							"name":        "offset",
							"in":          "query",
							"description": "Number of sessions to skip",
							"schema": map[string]any{
								"type":    "integer",
								"default": 0,
								"minimum": 0,
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Sessions retrieved successfully",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/ListSessionsResponse",
									},
								},
							},
						},
						"401": map[string]any{
							"description": "Unauthorized",
						},
					},
				},
			},
			"/api/v1/sessions/{sessionId}": map[string]any{
				"get": map[string]any{
					"summary":     "Get session metadata",
					"description": "Retrieve session information including token usage and task count",
					"parameters": []map[string]any{
						{
							"name":        "sessionId",
							"in":          "path",
							"description": "Session UUID",
							"required":    true,
							"schema": map[string]any{
								"type":   "string",
								"format": "uuid",
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Session metadata retrieved",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/SessionResponse",
									},
								},
							},
						},
						"401": map[string]any{
							"description": "Unauthorized",
						},
						"403": map[string]any{
							"description": "Forbidden - User does not have access to this session",
						},
						"404": map[string]any{
							"description": "Session not found",
						},
					},
				},
				"patch": map[string]any{
					"summary":     "Update session title",
					"description": "Update the title of an existing session (max 60 characters)",
					"parameters": []map[string]any{
						{
							"name":        "sessionId",
							"in":          "path",
							"description": "Session UUID",
							"required":    true,
							"schema": map[string]any{
								"type":   "string",
								"format": "uuid",
							},
						},
					},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/UpdateSessionTitleRequest",
								},
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Session title updated successfully",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/UpdateSessionTitleResponse",
									},
								},
							},
						},
						"400": map[string]any{"description": "Invalid request - title empty or too long"},
						"401": map[string]any{"description": "Unauthorized"},
						"403": map[string]any{"description": "Forbidden - Not the owner"},
						"404": map[string]any{"description": "Session not found"},
					},
				},
				"delete": map[string]any{
					"summary":     "Delete session (soft delete)",
					"description": "Marks the session as deleted; data remains and is filtered from reads.",
					"parameters": []map[string]any{
						{
							"name":        "sessionId",
							"in":          "path",
							"description": "Session UUID",
							"required":    true,
							"schema": map[string]any{
								"type":   "string",
								"format": "uuid",
							},
						},
					},
					"responses": map[string]any{
						"204": map[string]any{"description": "Session deleted (idempotent)"},
						"400": map[string]any{"description": "Invalid session ID"},
						"401": map[string]any{"description": "Unauthorized"},
						"403": map[string]any{"description": "Forbidden - Not the owner"},
						"404": map[string]any{"description": "Session not found"},
					},
				},
			},
			"/api/v1/sessions/{sessionId}/history": map[string]any{
				"get": map[string]any{
					"summary":     "Get session task history",
					"description": "Retrieve all tasks in a session with full execution details",
					"parameters": []map[string]any{
						{
							"name":        "sessionId",
							"in":          "path",
							"description": "Session UUID",
							"required":    true,
							"schema": map[string]any{
								"type":   "string",
								"format": "uuid",
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Session task history retrieved",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/SessionHistoryResponse",
									},
								},
							},
						},
						"401": map[string]any{
							"description": "Unauthorized",
						},
						"403": map[string]any{
							"description": "Forbidden - User does not have access to this session",
						},
						"404": map[string]any{
							"description": "Session not found",
						},
					},
				},
			},
			"/api/v1/sessions/{sessionId}/events": map[string]any{
				"get": map[string]any{
					"summary":     "Get session events grouped by turn",
					"description": "Returns chat history grouped by task/turn, with full events per turn. Excludes LLM_PARTIAL.",
					"parameters": []map[string]any{
						{
							"name":        "sessionId",
							"in":          "path",
							"description": "Session ID (UUID or external_id)",
							"required":    true,
							"schema": map[string]any{
								"type": "string",
							},
						},
						{
							"name":        "limit",
							"in":          "query",
							"description": "Maximum number of turns to return",
							"schema": map[string]any{
								"type":    "integer",
								"default": 10,
								"minimum": 1,
								"maximum": 100,
							},
						},
						{
							"name":        "offset",
							"in":          "query",
							"description": "Number of turns to skip",
							"schema": map[string]any{
								"type":    "integer",
								"default": 0,
								"minimum": 0,
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Session turns retrieved",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/SessionTurnsResponse",
									},
								},
							},
						},
						"401": map[string]any{
							"description": "Unauthorized",
						},
						"403": map[string]any{
							"description": "Forbidden - User does not have access to this session",
						},
						"404": map[string]any{
							"description": "Session not found",
						},
					},
				},
			},
			"/api/v1/approvals/decision": map[string]any{
				"post": map[string]any{
					"summary":     "Submit approval decision",
					"description": "Submit a human approval decision for a workflow requiring approval. Replaces the admin-only endpoint with a gateway-managed, authenticated endpoint.",
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"$ref": "#/components/schemas/ApprovalDecisionRequest",
								},
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Approval decision submitted successfully",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"$ref": "#/components/schemas/ApprovalDecisionResponse",
									},
								},
							},
						},
						"400": map[string]any{
							"description": "Invalid request - Missing required fields",
						},
						"401": map[string]any{
							"description": "Unauthorized - Invalid or missing API key",
						},
						"403": map[string]any{
							"description": "Forbidden - User does not have permission to approve this workflow",
						},
						"404": map[string]any{
							"description": "Workflow or approval not found",
						},
						"429": map[string]any{
							"description": "Rate limit exceeded",
						},
					},
				},
			},
			"/api/v1/tools": map[string]any{
				"get": map[string]any{
					"summary":     "List available tools",
					"description": "Returns tool schemas (name, description, parameters). Dangerous tools are excluded.",
					"tags":        []string{"Tools"},
					"parameters": []map[string]any{
						{
							"name":        "category",
							"in":          "query",
							"description": "Filter by category",
							"required":    false,
							"schema": map[string]any{
								"type": "string",
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "List of available tools",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "array",
										"items": map[string]any{
											"type": "object",
											"properties": map[string]any{
												"name": map[string]any{
													"type": "string",
												},
												"description": map[string]any{
													"type": "string",
												},
												"parameters": map[string]any{
													"type": "object",
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			"/api/v1/tools/{name}": map[string]any{
				"get": map[string]any{
					"summary":     "Get tool details",
					"description": "Returns merged metadata and parameter schema. Returns 403 for dangerous tools.",
					"tags":        []string{"Tools"},
					"parameters": []map[string]any{
						{
							"name":        "name",
							"in":          "path",
							"description": "Tool name",
							"required":    true,
							"schema": map[string]any{
								"type": "string",
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Tool details",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"name": map[string]any{
												"type": "string",
											},
											"description": map[string]any{
												"type": "string",
											},
											"parameters": map[string]any{
												"type": "object",
											},
										},
									},
								},
							},
						},
						"403": map[string]any{
							"description": "Tool not available (dangerous)",
						},
						"404": map[string]any{
							"description": "Tool not found",
						},
					},
				},
			},
			"/api/v1/tools/{name}/execute": map[string]any{
				"post": map[string]any{
					"summary":     "Execute a tool directly",
					"description": "Execute a tool without full task orchestration. Dangerous tools are blocked. Usage is recorded against quota.",
					"tags":        []string{"Tools"},
					"parameters": []map[string]any{
						{
							"name":        "name",
							"in":          "path",
							"description": "Tool name",
							"required":    true,
							"schema": map[string]any{
								"type": "string",
							},
						},
					},
					"requestBody": map[string]any{
						"required": true,
						"content": map[string]any{
							"application/json": map[string]any{
								"schema": map[string]any{
									"type":     "object",
									"required": []string{"arguments"},
									"properties": map[string]any{
										"arguments": map[string]any{
											"type":        "object",
											"description": "Tool-specific parameters",
										},
										"session_id": map[string]any{
											"type":        "string",
											"description": "Optional session ID for context",
										},
									},
								},
							},
						},
					},
					"responses": map[string]any{
						"200": map[string]any{
							"description": "Tool execution result",
							"content": map[string]any{
								"application/json": map[string]any{
									"schema": map[string]any{
										"type": "object",
										"properties": map[string]any{
											"success": map[string]any{
												"type": "boolean",
											},
											"output": map[string]any{},
											"error": map[string]any{
												"type": "string",
											},
											"metadata": map[string]any{
												"type":        "object",
												"description": "Tool-specific metadata from execution",
											},
											"execution_time_ms": map[string]any{
												"type": "integer",
											},
											"usage": map[string]any{
												"type": "object",
												"properties": map[string]any{
													"tokens": map[string]any{
														"type": "integer",
													},
													"cost_usd": map[string]any{
														"type":   "number",
														"format": "double",
													},
												},
											},
										},
									},
								},
							},
						},
						"403": map[string]any{
							"description": "Tool not available (dangerous)",
						},
						"404": map[string]any{
							"description": "Tool not found",
						},
						"429": map[string]any{
							"description": "Rate limit or quota exceeded",
						},
					},
				},
			},
		},
		"components": map[string]any{
			"securitySchemes": map[string]any{
				"apiKey": map[string]any{
					"type":        "apiKey",
					"in":          "header",
					"name":        "X-API-Key",
					"description": "API key for authentication",
				},
			},
			"schemas": map[string]any{
				"HealthResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"version": map[string]any{
							"type": "string",
						},
						"time": map[string]any{
							"type":   "string",
							"format": "date-time",
						},
						"checks": map[string]any{
							"type": "object",
							"additionalProperties": map[string]any{
								"type": "string",
							},
						},
					},
				},
				"TaskRequest": map[string]any{
					"type":     "object",
					"required": []string{"query"},
					"properties": map[string]any{
						"query": map[string]any{
							"type":        "string",
							"description": "The task query or command",
						},
						"session_id": map[string]any{
							"type":        "string",
							"description": "Session ID for continuity (UUID or custom string)",
						},
						"context": map[string]any{
							"type":        "object",
							"description": "Additional context for the task (recognized keys: role, model_tier, model_override, provider_override, system_prompt, prompt_params, template, template_name (alias), template_version, disable_ai, history_window_size, use_case_preset, primers_count, recents_count, compression_trigger_ratio, compression_target_ratio)",
							"example": map[string]any{
								"role":                      "analysis",
								"model_tier":                "large",
								"model_override":            "gpt-5-2025-08-07",
								"system_prompt":             "You are a concise assistant.",
								"prompt_params":             map[string]any{"profile_id": "49598h6e", "current_date": "2025-09-01"},
								"template":                  "research_summary",
								"template_version":          "1.0.0",
								"disable_ai":                true,
								"history_window_size":       75,
								"primers_count":             3,
								"recents_count":             20,
								"compression_trigger_ratio": 0.75,
								"compression_target_ratio":  0.375,
							},
						},
						"mode": map[string]any{
							"type":        "string",
							"enum":        []string{"simple", "standard", "complex", "supervisor"},
							"description": "Execution mode (advanced): simple|standard|complex|supervisor. Omit to auto-detect.",
							"default":     "simple",
						},
						"model_tier": map[string]any{
							"type":        "string",
							"enum":        []string{"small", "medium", "large"},
							"description": "Preferred model tier (injects into context.model_tier)",
						},
						"model_override": map[string]any{
							"type":        "string",
							"description": "Specific model override (injects into context.model_override, e.g., 'gpt-5-2025-08-07', 'gpt-5-pro-2025-10-06')",
						},
						"provider_override": map[string]any{
							"type":        "string",
							"description": "Provider override (injects into context.provider_override, e.g., 'openai', 'anthropic')",
						},
					},
				},
				"TaskResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"task_id": map[string]any{
							"type": "string",
						},
						"message": map[string]any{
							"type": "string",
						},
						"created_at": map[string]any{
							"type":   "string",
							"format": "date-time",
						},
					},
				},
				"TaskStreamResponse": map[string]any{
					"type":     "object",
					"required": []string{"workflow_id", "task_id", "stream_url"},
					"properties": map[string]any{
						"workflow_id": map[string]any{
							"type":        "string",
							"description": "Unique workflow identifier for this task",
							"example":     "task-user123-1234567890",
						},
						"task_id": map[string]any{
							"type":        "string",
							"description": "Task identifier (same as workflow_id)",
							"example":     "task-user123-1234567890",
						},
						"stream_url": map[string]any{
							"type":        "string",
							"description": "SSE endpoint URL to stream real-time events for this task",
							"example":     "/api/v1/stream/sse?workflow_id=task-user123-1234567890",
						},
					},
				},
				"TaskStatusResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"task_id": map[string]any{
							"type": "string",
						},
						"status": map[string]any{
							"type": "string",
						},
						"result": map[string]any{
							"type":        "string",
							"description": "Raw LLM response (plain text or JSON string)",
						},
						"response": map[string]any{
							"type":        "object",
							"description": "Parsed JSON response (if result is valid JSON) - for backward compatibility",
						},
						"error": map[string]any{
							"type": "string",
						},
						"query": map[string]any{
							"type": "string",
						},
						"session_id": map[string]any{
							"type": "string",
						},
						"mode": map[string]any{
							"type": "string",
						},
						"created_at": map[string]any{
							"type":   "string",
							"format": "date-time",
						},
						"updated_at": map[string]any{
							"type":   "string",
							"format": "date-time",
						},
					},
				},
				"TaskSummary": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"task_id":           map[string]any{"type": "string"},
						"query":             map[string]any{"type": "string"},
						"status":            map[string]any{"type": "string"},
						"mode":              map[string]any{"type": "string"},
						"created_at":        map[string]any{"type": "string", "format": "date-time"},
						"completed_at":      map[string]any{"type": "string", "format": "date-time"},
						"total_token_usage": map[string]any{"type": "object"},
					},
				},
				"ListTasksResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"tasks":       map[string]any{"type": "array", "items": map[string]any{"$ref": "#/components/schemas/TaskSummary"}},
						"total_count": map[string]any{"type": "integer"},
					},
				},
				"TaskEvent": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"workflow_id": map[string]any{"type": "string"},
						"type":        map[string]any{"type": "string"},
						"agent_id":    map[string]any{"type": "string"},
						"message":     map[string]any{"type": "string"},
						"timestamp":   map[string]any{"type": "string", "format": "date-time"},
						"seq":         map[string]any{"type": "integer"},
						"stream_id":   map[string]any{"type": "string"},
					},
				},
				"ListSessionsResponse": map[string]any{
					"type":     "object",
					"required": []string{"sessions", "total_count"},
					"properties": map[string]any{
						"sessions": map[string]any{
							"type":        "array",
							"description": "List of sessions",
							"items": map[string]any{
								"$ref": "#/components/schemas/SessionSummary",
							},
						},
						"total_count": map[string]any{
							"type":        "integer",
							"description": "Total number of sessions for the user",
						},
					},
				},
				"SessionSummary": map[string]any{
					"type":     "object",
					"required": []string{"session_id", "user_id", "task_count", "tokens_used", "created_at"},
					"properties": map[string]any{
						"session_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "Unique session identifier",
						},
						"user_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "User who owns this session",
						},
						"title": map[string]any{
							"type":        "string",
							"description": "Session title (auto-generated or user-defined, max 60 chars)",
							"maxLength":   60,
						},
						"task_count": map[string]any{
							"type":        "integer",
							"description": "Number of tasks in this session",
						},
						"tokens_used": map[string]any{
							"type":        "integer",
							"description": "Total tokens consumed in this session (from DB, optionally enriched by Redis)",
						},
						"token_budget": map[string]any{
							"type":        "integer",
							"description": "Token budget for the session",
						},
						"created_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Session creation timestamp",
						},
						"updated_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Session last update timestamp",
						},
						"expires_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Session expiration timestamp",
						},
						"context": map[string]any{
							"type":        "object",
							"description": "Session context metadata (includes title, external_id, and feature flags)",
						},
						"last_activity_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Timestamp of the latest completed task in this session",
						},
						"is_active": map[string]any{
							"type":        "boolean",
							"description": "Whether the session is considered active (recent activity in the last 24 hours)",
						},
						"successful_tasks": map[string]any{
							"type":        "integer",
							"description": "Number of successfully completed tasks",
						},
						"failed_tasks": map[string]any{
							"type":        "integer",
							"description": "Number of failed tasks",
						},
						"success_rate": map[string]any{
							"type":        "number",
							"format":      "double",
							"description": "Task success rate in the range 0.0-1.0 (successful_tasks / task_count)",
						},
						"total_cost_usd": map[string]any{
							"type":        "number",
							"format":      "double",
							"description": "Total cost of all tasks in this session, in USD",
						},
						"average_cost_per_task": map[string]any{
							"type":        "number",
							"format":      "double",
							"description": "Average cost per task (total_cost_usd / task_count)",
						},
						"budget_utilization": map[string]any{
							"type":        "number",
							"format":      "double",
							"description": "Fraction of token budget consumed (tokens_used / token_budget)",
						},
						"budget_remaining": map[string]any{
							"type":        "integer",
							"description": "Remaining tokens in the budget (never negative)",
						},
						"is_near_budget_limit": map[string]any{
							"type":        "boolean",
							"description": "Whether the session is nearing its token budget (utilization > 90%)",
						},
						"latest_task_query": map[string]any{
							"type":        "string",
							"description": "Query text of the most recent task in this session",
						},
						"latest_task_status": map[string]any{
							"type":        "string",
							"description": "Status of the most recent task (e.g., COMPLETED, FAILED, RUNNING)",
						},
						"is_research_session": map[string]any{
							"type":        "boolean",
							"description": "Whether this session is inferred to be a research session",
						},
						"first_task_mode": map[string]any{
							"type":        "string",
							"description": "Mode of the first task in the session (used for research detection)",
						},
					},
				},
				"SessionResponse": map[string]any{
					"type":     "object",
					"required": []string{"session_id", "user_id", "task_count", "tokens_used", "created_at"},
					"properties": map[string]any{
						"session_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "Unique session identifier",
						},
						"user_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "User who owns this session",
						},
						"context": map[string]any{
							"type":        "object",
							"description": "Session context metadata",
						},
						"token_budget": map[string]any{
							"type":        "integer",
							"description": "Token budget for the session",
						},
						"tokens_used": map[string]any{
							"type":        "integer",
							"description": "Total tokens consumed in this session",
						},
						"task_count": map[string]any{
							"type":        "integer",
							"description": "Number of tasks in this session",
						},
						"created_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Session creation timestamp",
						},
						"updated_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Session last update timestamp",
						},
						"expires_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Session expiration timestamp",
						},
					},
				},
				"TaskHistory": map[string]any{
					"type":     "object",
					"required": []string{"task_id", "workflow_id", "query", "status", "started_at"},
					"properties": map[string]any{
						"task_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "Task UUID",
						},
						"workflow_id": map[string]any{
							"type":        "string",
							"description": "Temporal workflow ID",
						},
						"query": map[string]any{
							"type":        "string",
							"description": "Task query/command",
						},
						"status": map[string]any{
							"type":        "string",
							"description": "Task execution status",
							"enum":        []string{"RUNNING", "COMPLETED", "FAILED", "CANCELLED", "TIMEOUT"},
						},
						"mode": map[string]any{
							"type":        "string",
							"description": "Execution mode",
						},
						"result": map[string]any{
							"type":        "string",
							"description": "Task result/output",
						},
						"error_message": map[string]any{
							"type":        "string",
							"description": "Error message if task failed",
						},
						"total_tokens": map[string]any{
							"type":        "integer",
							"description": "Total tokens used",
						},
						"total_cost_usd": map[string]any{
							"type":        "number",
							"format":      "double",
							"description": "Total cost in USD",
						},
						"duration_ms": map[string]any{
							"type":        "integer",
							"description": "Task duration in milliseconds",
						},
						"agents_used": map[string]any{
							"type":        "integer",
							"description": "Number of agents used",
						},
						"tools_invoked": map[string]any{
							"type":        "integer",
							"description": "Number of tools invoked",
						},
						"started_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Task start timestamp",
						},
						"completed_at": map[string]any{
							"type":        "string",
							"format":      "date-time",
							"description": "Task completion timestamp",
						},
						"metadata": map[string]any{
							"type":        "object",
							"description": "Additional task metadata",
						},
					},
				},
				"SessionHistoryResponse": map[string]any{
					"type":     "object",
					"required": []string{"session_id", "tasks", "total"},
					"properties": map[string]any{
						"session_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "Session UUID",
						},
						"tasks": map[string]any{
							"type":        "array",
							"description": "List of tasks in chronological order",
							"items": map[string]any{
								"$ref": "#/components/schemas/TaskHistory",
							},
						},
						"total": map[string]any{
							"type":        "integer",
							"description": "Total number of tasks in session",
						},
					},
				},
				"SessionTurnsResponse": map[string]any{
					"type":     "object",
					"required": []string{"session_id", "turns", "count"},
					"properties": map[string]any{
						"session_id": map[string]any{
							"type":        "string",
							"description": "Session identifier (UUID or external_id)",
						},
						"count": map[string]any{
							"type":        "integer",
							"description": "Total number of turns in session",
						},
						"turns": map[string]any{
							"type":        "array",
							"description": "Turns grouped by task in chronological order",
							"items": map[string]any{
								"$ref": "#/components/schemas/Turn",
							},
						},
					},
				},
				"Turn": map[string]any{
					"type":     "object",
					"required": []string{"turn", "task_id", "user_query", "final_output", "timestamp", "events", "metadata"},
					"properties": map[string]any{
						"turn":         map[string]any{"type": "integer"},
						"task_id":      map[string]any{"type": "string"},
						"user_query":   map[string]any{"type": "string"},
						"final_output": map[string]any{"type": "string"},
						"timestamp":    map[string]any{"type": "string", "format": "date-time"},
						"events":       map[string]any{"type": "array", "items": map[string]any{"$ref": "#/components/schemas/TaskEvent"}},
						"metadata":     map[string]any{"$ref": "#/components/schemas/TurnMetadata"},
					},
				},
				"TurnMetadata": map[string]any{
					"type":     "object",
					"required": []string{"tokens_used", "execution_time_ms", "agents_involved"},
					"properties": map[string]any{
						"tokens_used":       map[string]any{"type": "integer"},
						"execution_time_ms": map[string]any{"type": "integer"},
						"agents_involved":   map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
						"attachments": map[string]any{
							"type":        "array",
							"description": "Optional attachment metadata from task context",
							"items": map[string]any{
								"type": "object",
								"properties": map[string]any{
									"id":         map[string]any{"type": "string"},
									"media_type": map[string]any{"type": "string"},
									"filename":   map[string]any{"type": "string"},
									"size_bytes": map[string]any{"type": "integer"},
									"thumbnail":  map[string]any{"type": "string", "description": "Thumbnail data URL (<=50KB)"},
								},
							},
						},
					},
				},
				"CancelTaskRequest": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"reason": map[string]any{
							"type":        "string",
							"description": "Optional reason for cancellation",
						},
					},
				},
				"CancelTaskResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"success": map[string]any{
							"type":        "boolean",
							"description": "Whether the cancellation was successful",
						},
						"message": map[string]any{
							"type":        "string",
							"description": "Status message",
						},
						"status": map[string]any{
							"type":        "string",
							"description": "Current task status (included in 409 response)",
						},
					},
				},
				"ApprovalDecisionRequest": map[string]any{
					"type":     "object",
					"required": []string{"workflow_id", "approval_id", "approved"},
					"properties": map[string]any{
						"workflow_id": map[string]any{
							"type":        "string",
							"description": "Workflow ID requiring approval",
						},
						"run_id": map[string]any{
							"type":        "string",
							"description": "Optional run ID",
						},
						"approval_id": map[string]any{
							"type":        "string",
							"description": "Unique approval identifier",
						},
						"approved": map[string]any{
							"type":        "boolean",
							"description": "Whether the action is approved",
						},
						"feedback": map[string]any{
							"type":        "string",
							"description": "Optional feedback message",
						},
						"modified_action": map[string]any{
							"type":        "string",
							"description": "Optional modified action if approved with changes",
						},
						"approved_by": map[string]any{
							"type":        "string",
							"description": "User who approved (defaults to authenticated user)",
						},
					},
				},
				"ApprovalDecisionResponse": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"status": map[string]any{
							"type":        "string",
							"description": "Status of the approval submission",
						},
						"success": map[string]any{
							"type":        "boolean",
							"description": "Whether the approval was successfully submitted",
						},
						"message": map[string]any{
							"type":        "string",
							"description": "Status message",
						},
						"workflow_id": map[string]any{
							"type":        "string",
							"description": "Workflow ID",
						},
						"run_id": map[string]any{
							"type":        "string",
							"description": "Run ID",
						},
						"approval_id": map[string]any{
							"type":        "string",
							"description": "Approval ID",
						},
					},
				},
				"UpdateSessionTitleRequest": map[string]any{
					"type":     "object",
					"required": []string{"title"},
					"properties": map[string]any{
						"title": map[string]any{
							"type":        "string",
							"description": "New session title (max 60 characters)",
							"minLength":   1,
							"maxLength":   60,
						},
					},
				},
				"UpdateSessionTitleResponse": map[string]any{
					"type":     "object",
					"required": []string{"session_id", "title"},
					"properties": map[string]any{
						"session_id": map[string]any{
							"type":        "string",
							"format":      "uuid",
							"description": "Session identifier",
						},
						"title": map[string]any{
							"type":        "string",
							"description": "Updated session title",
						},
					},
				},
			},
		},
	}
}
