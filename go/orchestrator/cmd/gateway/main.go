package main

import (
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/handlers"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/middleware"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/openai"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/cmd/gateway/internal/proxy"
	authpkg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	cfg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/daemon"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
	orchpb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/skills"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	redisv8 "github.com/go-redis/redis/v8"
	"github.com/jmoiron/sqlx"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Initialize logger
	// 初始化 logger
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}
	defer logger.Sync()

	// Discover configuration defaults from features.yaml (best effort)
	// 从 features.yaml 读取配置默认值（尽力而为）。
	var gatewaySkipAuthDefault *bool
	if featuresCfg, err := cfg.Load(); err != nil {
		logger.Warn("Failed to load feature configuration", zap.Error(err))
	} else if featuresCfg != nil && featuresCfg.Gateway.SkipAuth != nil {
		gatewaySkipAuthDefault = featuresCfg.Gateway.SkipAuth
	}
	if envVal := os.Getenv("GATEWAY_SKIP_AUTH"); envVal != "" {
		logger.Warn("Environment variable overrides gateway authentication setting",
			zap.String("env", "GATEWAY_SKIP_AUTH"),
			zap.String("config_key", "gateway.skip_auth"),
			zap.String("value", envVal))
	} else if gatewaySkipAuthDefault != nil {
		if *gatewaySkipAuthDefault {
			_ = os.Setenv("GATEWAY_SKIP_AUTH", "1")
		} else {
			_ = os.Setenv("GATEWAY_SKIP_AUTH", "0")
		}
	}

	// Initialize database
	// 初始化数据库
	dbConfig := &db.Config{
		Host:     getEnvOrDefault("POSTGRES_HOST", "postgres"),
		Port:     getEnvOrDefaultInt("POSTGRES_PORT", 5432),
		User:     getEnvOrDefault("POSTGRES_USER", "shannon"),
		Password: getEnvOrDefault("POSTGRES_PASSWORD", "shannon"),
		Database: getEnvOrDefault("POSTGRES_DB", "shannon"),
		SSLMode:  getEnvOrDefault("POSTGRES_SSLMODE", "disable"),
	}

	dbClient, err := db.NewClient(dbConfig, logger)
	if err != nil {
		logger.Fatal("Failed to connect to database", zap.Error(err))
	}
	defer dbClient.Close()

	// Create sqlx.DB wrapper for auth service
	// 为 auth service 创建 sqlx.DB 封装
	pgDB := sqlx.NewDb(dbClient.GetDB(), "postgres")

	// Initialize Redis client for rate limiting and idempotency
	// 初始化 Redis client，用于 rate limiting 与幂等控制
	redisURL := getEnvOrDefault("REDIS_URL", "redis://redis:6379")
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		logger.Fatal("Failed to parse Redis URL", zap.Error(err))
	}
	redisClient := redis.NewClient(redisOpts)
	defer redisClient.Close()

	// Test Redis connection
	// 测试 Redis 连接
	ctx := context.Background()
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		logger.Fatal("Failed to connect to Redis", zap.Error(err))
	}

	// Initialize streaming manager with Redis so gateway can subscribe to
	// workflow events for streaming card replies.
	// Streaming manager uses go-redis/v8, gateway uses redis/v9 — create a v8 client.
	// 初始化 streaming manager（连 Redis），以便 gateway 订阅 workflow event 来推送 streaming 卡片回复。
	// streaming manager 用 go-redis/v8，而 gateway 用 redis/v9，因此单独建一个 v8 客户端。
	streamingRedis := redisv8.NewClient(&redisv8.Options{
		Addr:     redisOpts.Addr,
		Password: redisOpts.Password,
		DB:       redisOpts.DB,
	})
	defer streamingRedis.Close()
	streaming.InitializeRedis(streamingRedis, logger)

	// Initialize auth service (direct access to internal package)
	// 初始化 auth service（直接访问 internal 包）
	jwtSecret := getEnvOrDefault("JWT_SECRET", "your-secret-key")
	authService := authpkg.NewService(pgDB, logger, jwtSecret)

	// Connect to orchestrator gRPC
	// 连接 orchestrator 的 gRPC
	orchAddr := getEnvOrDefault("ORCHESTRATOR_GRPC", "orchestrator:50052")
	conn, err := grpc.Dial(orchAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(50*1024*1024)), // 50MB 注：gRPC 单条消息上限 50MB
	)
	if err != nil {
		logger.Fatal("Failed to connect to orchestrator", zap.Error(err))
	}
	defer conn.Close()

	orchClient := orchpb.NewOrchestratorServiceClient(conn)

	// Initialize skill registry
	// 初始化 skill registry
	skillRegistry := skills.NewRegistry()
	for _, dir := range skills.ResolveSkillDirs() {
		if err := skillRegistry.LoadDirectory(dir); err != nil {
			logger.Warn("Failed to load skills from directory", zap.String("path", dir), zap.Error(err))
		}
	}
	if err := skillRegistry.Finalize(); err != nil {
		logger.Fatal("Failed to finalize skill registry", zap.Error(err))
	}
	logger.Info("Skill registry initialized",
		zap.Int("total_skills", skillRegistry.Count()),
		zap.Strings("categories", skillRegistry.Categories()),
	)

	// Create session manager for persisting HITL messages to session history
	// 创建 session manager，用于把 HITL 消息持久化到 session history。
	sessionMgr, err := session.NewManager(redisOpts.Addr, logger)
	if err != nil {
		logger.Warn("Failed to create session manager for HITL persistence", zap.Error(err))
	}

	// Create handlers
	// 创建 handlers
	taskHandler := handlers.NewTaskHandler(orchClient, pgDB, redisClient, skillRegistry, logger, sessionMgr)
	sessionHandler := handlers.NewSessionHandler(pgDB, redisClient, logger)
	approvalHandler := handlers.NewApprovalHandler(orchClient, logger)
	reviewHandler := handlers.NewReviewHandler(orchClient, redisClient, logger)
	scheduleHandler := handlers.NewScheduleHandler(orchClient, pgDB, logger)
	healthHandler := handlers.NewHealthHandler(orchClient, logger)
	openapiHandler := handlers.NewOpenAPIHandler()
	agentsHandler := handlers.NewAgentsHandler(orchClient, logger)
	skillHandler := handlers.NewSkillHandler(skillRegistry, logger)
	workspaceHandler := handlers.NewWorkspaceHandler(pgDB, logger)
	memoryHandler := handlers.NewMemoryHandler(logger)
	llmModelsPath := cfg.ResolveConfigFile("MODELS_CONFIG_PATH", []string{
		"/app/config/models.yaml",
		"config/models.yaml",
		"../../config/models.yaml",
	}, "config/models.yaml")
	llmModelsHandler := handlers.NewLLMModelsHandler(llmModelsPath, logger)

	adminURL := getEnvOrDefault("ADMIN_SERVER", "http://orchestrator:8081")

	// OAuth verifier (enterprise-only)
	// Web OAuth credentials
	// OAuth 校验器（企业版功能）。Web OAuth 凭据。
	googleClientID := getEnvOrDefault("GOOGLE_OAUTH_CLIENT_ID", "")
	googleClientSecret := getEnvOrDefault("GOOGLE_OAUTH_CLIENT_SECRET", "")
	// Desktop OAuth credentials (for Tauri/native apps)
	// Desktop OAuth 凭据（供 Tauri/原生应用使用）。
	googleDesktopClientID := getEnvOrDefault("GOOGLE_DESKTOP_CLIENT_ID", "")
	googleDesktopClientSecret := getEnvOrDefault("GOOGLE_DESKTOP_CLIENT_SECRET", "")
	oauthVerifier := authpkg.NewGoogleOAuthVerifier(googleClientID, googleClientSecret, googleDesktopClientID, googleDesktopClientSecret)

	// Enterprise: Load rate limit config from YAML (or use defaults)
	// 企业版：从 YAML 加载 rate limit 配置（否则用默认值）。
	rateLimitConfigPath := cfg.ResolveConfigFile("RATE_LIMIT_CONFIG_PATH", cfg.RateLimitConfigPaths, "config/rate_limits.yaml")
	rateLimitConfig, err := middleware.LoadRateLimitConfig(rateLimitConfigPath)
	if err != nil {
		logger.Warn("Failed to load rate limit config, using defaults", zap.String("path", rateLimitConfigPath), zap.Error(err))
		rateLimitConfig = middleware.DefaultRateLimitConfig()
	} else {
		logger.Info("Loaded rate limit config", zap.String("path", rateLimitConfigPath))
	}

	// Create auth handler (needs rateLimitConfig for config-based RPM/RPH in /auth/me)
	// 创建 auth handler（/auth/me 中基于配置的 RPM/RPH 需要 rateLimitConfig）。
	authHandler := handlers.NewAuthHandler(authService, oauthVerifier, pgDB, logger, rateLimitConfig)

	// Create middlewares
	// 创建 middlewares
	authMiddleware := middleware.NewAuthMiddleware(authService, logger).Middleware
	rateLimiter := middleware.NewRateLimiterWithConfig(redisClient, logger, rateLimitConfig).Middleware
	idempotencyMiddleware := middleware.NewIdempotencyMiddleware(redisClient, logger).Middleware
	tracingMiddleware := middleware.NewTracingMiddleware(logger).Middleware
	validationMiddleware := middleware.NewValidationMiddleware(logger).Middleware

	// OpenAI-compatible API handler
	// OpenAI 兼容 API handler
	openaiHandler, err := openai.NewHandler(orchClient, pgDB, redisClient, logger, adminURL)
	if err != nil {
		logger.Warn("Failed to create OpenAI handler, endpoint disabled", zap.Error(err))
	}

	// Tool execution handler
	// tool 执行 handler
	toolsHandler := handlers.NewToolsHandler(pgDB, logger)

	// Setup HTTP mux
	// 初始化 HTTP mux
	mux := http.NewServeMux()

	// Health check (no auth required)
	// 健康检查（无需 authentication）
	mux.HandleFunc("GET /health", healthHandler.Health)
	mux.HandleFunc("GET /readiness", healthHandler.Readiness)

	// Prometheus metrics endpoint (no auth required)
	// Prometheus metrics 端点（无需 authentication）
	mux.Handle("GET /metrics", promhttp.Handler())

	// OpenAPI spec (no auth required)
	// OpenAPI spec（无需 authentication）
	mux.HandleFunc("GET /openapi.json", openapiHandler.ServeSpec)

	// Auth endpoints (no auth required for registration, auth required for me/refresh)
	// Auth endpoints
	// authentication 端点（注册无需 authentication；me/refresh 需要 authentication）。
	mux.HandleFunc("POST /api/v1/auth/register", authHandler.Register)
	mux.HandleFunc("POST /api/v1/auth/refresh", authHandler.Refresh)
	mux.Handle("GET /api/v1/auth/me",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.Me),
			),
		),
	)
	mux.Handle("POST /api/v1/auth/refresh-key",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.RefreshKey),
			),
		),
	)

	// API Key management endpoints (require auth)
	// API Key 管理端点（需要 authentication）
	mux.Handle("GET /api/v1/auth/api-keys",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.ListAPIKeys),
			),
		),
	)
	mux.Handle("POST /api/v1/auth/api-keys",
		tracingMiddleware(
			authMiddleware(
				rateLimiter(
					http.HandlerFunc(authHandler.CreateKey),
				),
			),
		),
	)
	mux.Handle("DELETE /api/v1/auth/api-keys/{id}",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(authHandler.RevokeKey),
			),
		),
	)

	// Task endpoints (require auth)
	// task 端点（需要 authentication）
	mux.Handle("POST /api/v1/tasks",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.SubmitTask),
						),
					),
				),
			),
		),
	)

	// Unified submit that returns the SSE stream URL (no long-lived stream here)
	// 统一的提交入口，只返回 SSE stream URL（不在此处维持长连接）。
	mux.Handle("POST /api/v1/tasks/stream",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.SubmitTaskAndGetStreamURL),
						),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.ListTasks),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(taskHandler.GetTaskStatus),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks/{id}/stream",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(taskHandler.StreamTask),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tasks/{id}/events",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.GetTaskEvents),
					),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/tasks/{id}/cancel",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.CancelTask),
						),
					),
				),
			),
		),
	)

	// Pause workflow
	// 暂停 workflow
	mux.Handle("POST /api/v1/tasks/{id}/pause",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.PauseTask),
						),
					),
				),
			),
		),
	)

	// Resume workflow
	// 恢复 workflow
	mux.Handle("POST /api/v1/tasks/{id}/resume",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(taskHandler.ResumeTask),
						),
					),
				),
			),
		),
	)

	// Swarm HITL: send human message to Lead
	// Swarm HITL：向 Lead 发送人工消息。
	mux.Handle("POST /api/v1/swarm/{workflowID}/message",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.SendSwarmMessage),
					),
				),
			),
		),
	)

	// Get control state
	// 获取 control state
	mux.Handle("GET /api/v1/tasks/{id}/control-state",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(taskHandler.GetControlState),
					),
				),
			),
		),
	)

	// Approval endpoints (require auth)
	// approval 端点（需要 authentication）
	mux.Handle("POST /api/v1/approvals/decision",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(approvalHandler.SubmitDecision),
						),
					),
				),
			),
		),
	)

	// HITL Research Review
	// HITL 研究评审（research review）
	mux.Handle("GET /api/v1/tasks/{workflowID}/review",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(reviewHandler.HandleGetReview),
				),
			),
		),
	)
	mux.Handle("POST /api/v1/tasks/{workflowID}/review",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(reviewHandler.HandleReview),
					),
				),
			),
		),
	)

	// Session endpoints (require auth)
	// session 端点（需要 authentication）
	mux.Handle("GET /api/v1/sessions",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(sessionHandler.ListSessions),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/sessions/{sessionId}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(sessionHandler.GetSession),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/sessions/{sessionId}/history",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(sessionHandler.GetSessionHistory),
				),
			),
		),
	)

	// Session events (chat history-like, excludes LLM_PARTIAL)
	// session event（类似聊天记录，不含 LLM_PARTIAL）。
	mux.Handle("GET /api/v1/sessions/{sessionId}/events",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(sessionHandler.GetSessionEvents),
				),
			),
		),
	)

	// Update session title
	// 更新 session 标题
	mux.Handle("PATCH /api/v1/sessions/{sessionId}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(sessionHandler.UpdateSessionTitle),
					),
				),
			),
		),
	)

	// Soft delete session (idempotent)
	// 软删除 session（幂等）
	mux.Handle("DELETE /api/v1/sessions/{sessionId}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(sessionHandler.DeleteSession),
					),
				),
			),
		),
	)

	// Session workspace file operations (require auth)
	// List files in session workspace
	// session workspace 文件操作（需 authentication）：列出 session workspace 中的文件。
	mux.Handle("GET /api/v1/sessions/{sessionId}/files",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(workspaceHandler.ListFiles),
					),
				),
			),
		),
	)

	// Download file from session workspace (path is the file path within workspace)
	// 从 session workspace 下载文件（path 为 workspace 内的文件路径）
	mux.Handle("GET /api/v1/sessions/{sessionId}/files/{path...}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(workspaceHandler.DownloadFile),
					),
				),
			),
		),
	)

	// User memory file operations (require auth, user_id from token)
	// List files in authenticated user's memory directory
	// 用户 memory 文件操作（需 authentication，user_id 取自 token）。
	mux.Handle("GET /api/v1/memory/files",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(memoryHandler.ListMemoryFiles),
					),
				),
			),
		),
	)

	// Download file from authenticated user's memory directory
	// 从已认证用户的 memory 目录下载文件
	mux.Handle("GET /api/v1/memory/files/{path...}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(memoryHandler.DownloadMemoryFile),
					),
				),
			),
		),
	)

	// Schedule endpoints (require auth)
	// schedule 端点（需要 authentication）
	mux.Handle("POST /api/v1/schedules",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.CreateSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/schedules",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(scheduleHandler.ListSchedules),
					),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/schedules/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(scheduleHandler.GetSchedule),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/schedules/{id}/runs",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					http.HandlerFunc(scheduleHandler.GetScheduleRuns),
				),
			),
		),
	)

	mux.Handle("PUT /api/v1/schedules/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.UpdateSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/schedules/{id}/pause",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.PauseSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/schedules/{id}/resume",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(scheduleHandler.ResumeSchedule),
						),
					),
				),
			),
		),
	)

	mux.Handle("DELETE /api/v1/schedules/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(scheduleHandler.DeleteSchedule),
					),
				),
			),
		),
	)

	// Agents endpoints (single-purpose deterministic agents)
	// agents 端点（单一职责的确定性 agent）。
	mux.Handle("GET /api/v1/agents",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(agentsHandler.ListAgents),
			),
		),
	)

	mux.Handle("GET /api/v1/agents/{id}",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(agentsHandler.GetAgent),
			),
		),
	)

	mux.Handle("POST /api/v1/agents/{id}",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						idempotencyMiddleware(
							http.HandlerFunc(agentsHandler.ExecuteAgent),
						),
					),
				),
			),
		),
	)

	logger.Info("Registered agents API endpoints",
		zap.String("endpoints", "/api/v1/agents, /api/v1/agents/{id}"),
	)

	// Skills endpoints (require auth)
	// skill 端点（需要 authentication）
	mux.Handle("GET /api/v1/skills",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(skillHandler.ListSkills),
			),
		),
	)

	mux.Handle("GET /api/v1/skills/{name}",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(skillHandler.GetSkill),
			),
		),
	)

	mux.Handle("GET /api/v1/skills/{name}/versions",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(skillHandler.GetSkillVersions),
			),
		),
	)

	logger.Info("Registered skills API endpoints",
		zap.String("endpoints", "/api/v1/skills, /api/v1/skills/{name}, /api/v1/skills/{name}/versions"),
	)

	// Tool execution endpoints (require auth)
	// NOTE: GET /api/v1/tools must be registered BEFORE GET /api/v1/tools/{name}
	// to avoid routing conflicts with Go 1.22 ServeMux.
	// tool 执行端点（需 authentication）。
	// 注意：GET /api/v1/tools 必须在 GET /api/v1/tools/{name} 之前注册，
	// 否则会与 Go 1.22 ServeMux 的路由规则冲突。
	mux.Handle("GET /api/v1/tools",
		tracingMiddleware(
			authMiddleware(
				rateLimiter(
					http.HandlerFunc(toolsHandler.ListTools),
				),
			),
		),
	)

	mux.Handle("GET /api/v1/tools/{name}",
		tracingMiddleware(
			authMiddleware(
				rateLimiter(
					http.HandlerFunc(toolsHandler.GetTool),
				),
			),
		),
	)

	mux.Handle("POST /api/v1/tools/{name}/execute",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					rateLimiter(
						http.HandlerFunc(toolsHandler.ExecuteTool),
					),
				),
			),
		),
	)

	logger.Info("Registered tool execution API endpoints",
		zap.String("endpoints", "/api/v1/tools, /api/v1/tools/{name}, /api/v1/tools/{name}/execute"),
	)

	// OpenAI-compatible API endpoints (/v1/*)
	// OpenAI 兼容 API 端点（/v1/*）
	if openaiHandler != nil {
		// POST /v1/chat/completions - main chat completion endpoint
		// POST /v1/chat/completions —— 主要的 chat completion 端点
		mux.Handle("POST /v1/chat/completions",
			tracingMiddleware(
				authMiddleware(
					validationMiddleware(
						rateLimiter(
							http.HandlerFunc(openaiHandler.ChatCompletions),
						),
					),
				),
			),
		)

		// POST /v1/completions - passthrough proxy to LLM service (for CLI tool execution)
		// POST /v1/completions——直通代理到 LLM service（供 CLI 执行 tool）。
		mux.Handle("POST /v1/completions",
			tracingMiddleware(
				authMiddleware(
					validationMiddleware(
						rateLimiter(
							idempotencyMiddleware(
								http.HandlerFunc(openaiHandler.Completions),
							),
						),
					),
				),
			),
		)

		// GET /v1/models - list available models
		// GET /v1/models —— 列出可用模型
		mux.Handle("GET /v1/models",
			tracingMiddleware(
				authMiddleware(
					http.HandlerFunc(openaiHandler.ListModels),
				),
			),
		)

		// GET /v1/models/{model} - get specific model info
		// GET /v1/models/{model} —— 获取指定模型信息
		mux.Handle("GET /v1/models/{model}",
			tracingMiddleware(
				authMiddleware(
					http.HandlerFunc(openaiHandler.GetModel),
				),
			),
		)

		// GET /v1/llm-models - list available LLM provider models (from models.yaml)
		// GET /v1/llm-models —— 列出可用的 LLM provider 模型（来自 models.yaml）
		mux.Handle("GET /v1/llm-models",
			tracingMiddleware(
				authMiddleware(
					http.HandlerFunc(llmModelsHandler.ListLLMModels),
				),
			),
		)

		logger.Info("OpenAI-compatible API enabled",
			zap.String("endpoints", "/v1/chat/completions, /v1/completions, /v1/models, /v1/llm-models"),
		)
	}

	// SSE/WebSocket reverse proxy to admin server
	// SSE/WebSocket 反向代理到 admin server。
	streamProxy, err := proxy.NewStreamingProxy(adminURL, logger)
	if err != nil {
		logger.Fatal("Failed to create streaming proxy", zap.Error(err))
	}

	// Proxy SSE and WebSocket endpoints
	// 代理 SSE 与 WebSocket 端点
	mux.Handle("/api/v1/stream/sse",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					streamProxy,
				),
			),
		),
	)

	mux.Handle("/api/v1/stream/ws",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					streamProxy,
				),
			),
		),
	)

	// Daemon command channel
	// daemon 命令通道
	daemonHub := daemon.NewHub(redisClient, logger)
	// Apply claim TTL from features config if loaded
	// 若已加载 features 配置，则应用其中的 claim TTL。
	if featuresCfg, err := cfg.Load(); err == nil && featuresCfg.Daemon.HeartbeatTimeoutSeconds > 0 {
		daemonHub.Claims().SetClaimTTL(time.Duration(featuresCfg.Daemon.HeartbeatTimeoutSeconds) * time.Second)
	}
	// Subscribe to dispatch requests from orchestrator (Redis stream bridge)
	// 订阅 orchestrator 的 dispatch request（Redis stream 桥接）。
	go daemonHub.SubscribeDispatches(ctx)
	approvalManager := daemon.NewApprovalManager(redisClient)
	// Token for internal admin server calls (/daemon/signal, /events).
	// Falls back to APPROVALS_AUTH_TOKEN to match orchestrator's daemon signal endpoint.
	// 内部 admin server 调用（/daemon/signal、/events）所用的 token；
	// 缺省回退到 APPROVALS_AUTH_TOKEN，与 orchestrator 的 daemon signal 端点保持一致。
	eventsAuthToken := getEnvOrDefault("EVENTS_AUTH_TOKEN", getEnvOrDefault("APPROVALS_AUTH_TOKEN", ""))
	daemonHandler := handlers.NewDaemonHandler(daemonHub, approvalManager, adminURL, eventsAuthToken, logger)

	// WebSocket endpoint for shan CLI daemons (no method prefix for WS upgrade)
	// 供 shan CLI daemon 使用的 WebSocket 端点（WS 升级不写 method 前缀）。
	mux.Handle("/v1/ws/messages",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(daemonHandler.HandleWebSocket),
			),
		),
	)

	// Daemon status API
	// daemon 状态 API
	mux.Handle("GET /api/v1/daemon/status",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(daemonHandler.HandleStatus),
			),
		),
	)

	// Blob proxy: GET /api/v1/blob/{key} -> admin /blob/{key}
	// Used to fetch large base64 screenshots stored in Redis
	// Blob 代理：GET /api/v1/blob/{key} -> admin /blob/{key}；
	// 用于拉取存放在 Redis 中的大体积 base64 截图。
	mux.Handle("GET /api/v1/blob/",
		tracingMiddleware(
			authMiddleware(
				validationMiddleware(
					streamProxy,
				),
			),
		),
	)

	// Timeline proxy: GET /api/v1/tasks/{id}/timeline -> admin /timeline
	// Timeline 代理：GET /api/v1/tasks/{id}/timeline -> admin /timeline。
	mux.Handle("GET /api/v1/tasks/{id}/timeline",
		tracingMiddleware(
			authMiddleware(
				http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					id := r.PathValue("id")
					if id == "" {
						http.Error(w, "{\"error\":\"Task ID required\"}", http.StatusBadRequest)
						return
					}
					// Rebuild target URL
					// 重建目标 URL
					target := strings.TrimRight(adminURL, "/") + "/timeline?workflow_id=" + id
					if raw := r.URL.RawQuery; raw != "" {
						target += "&" + raw
					}
					req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, target, nil)
					resp, err := http.DefaultClient.Do(req)
					if err != nil {
						logger.Error("timeline proxy error", zap.Error(err))
						http.Error(w, "{\"error\":\"upstream unavailable\"}", http.StatusBadGateway)
						return
					}
					defer resp.Body.Close()
					for k, v := range resp.Header {
						for _, vv := range v {
							w.Header().Add(k, vv)
						}
					}
					w.WriteHeader(resp.StatusCode)
					_, _ = io.Copy(w, resp.Body)
				}),
			),
		),
	)

	// CORS middleware for all routes (development friendly)
	// 所有路由共用 CORS middleware（对开发友好）。
	corsHandler := corsMiddleware(mux)

	// Create HTTP server
	// 创建 HTTP server
	port := getEnvOrDefaultInt("PORT", 8080)
	server := &http.Server{
		Addr:         ":" + strconv.Itoa(port),
		Handler:      corsHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 0,                 // No write timeout for SSE/streaming support 注：为支持 SSE/streaming 不设写超时
		IdleTimeout:  300 * time.Second, // 5 minutes idle for long-lived SSE connections 注：长连接 SSE 空闲 5 分钟
	}

	// Start server in goroutine
	// 在 goroutine 中启动 server
	go func() {
		logger.Info("Gateway starting", zap.Int("port", port))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("Failed to start gateway", zap.Error(err))
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	// 等待中断信号，优雅关闭 server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("Gateway shutting down...")

	// Stop background workers (prevents goroutine leaks)
	// 停止后台 worker（避免 goroutine 泄漏）。
	authService.Close()
	if openaiHandler != nil {
		openaiHandler.Close()
	}

	// Graceful shutdown with timeout
	// 带超时的优雅关闭
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("Gateway forced to shutdown", zap.Error(err))
	}

	logger.Info("Gateway stopped")
}

// corsMiddleware adds CORS headers for development
// corsMiddleware 为开发场景添加 CORS header
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isStreaming := strings.HasPrefix(r.URL.Path, "/api/v1/stream/")

		allowedHeaders := "Content-Type, Authorization, X-API-Key, X-User-Id, Idempotency-Key, traceparent, tracestate, Cache-Control, Last-Event-ID, If-Match"

		// Expose custom headers for frontend access (rate limits, tracing, session)
		// 暴露自定义 response 头供前端读取（rate limit、tracing、session）。
		exposedHeaders := "X-RateLimit-Limit-Minute, X-RateLimit-Remaining-Minute, X-RateLimit-Limit-Hour, X-RateLimit-Remaining-Hour, X-RateLimit-Reset, X-Trace-Id, X-Span-Id, X-Session-ID, X-Shannon-Session-ID"

		// Get allowed origins from environment (comma-separated) or default to wildcard
		// 允许的来源从环境变量读取（逗号分隔），默认通配。
		allowedOrigins := getEnvOrDefault("CORS_ALLOWED_ORIGINS", "*")

		allowMethods := "GET, POST, PUT, PATCH, DELETE, OPTIONS"
		if isStreaming {
			// Streaming endpoints (SSE/WS) are GET-only.
			// 流式端点（SSE/WS）只服务 GET。
			allowMethods = "GET, OPTIONS"
		}
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigins)
		w.Header().Set("Access-Control-Allow-Methods", allowMethods)
		w.Header().Set("Access-Control-Allow-Headers", allowedHeaders)
		w.Header().Set("Access-Control-Expose-Headers", exposedHeaders)
		w.Header().Set("Access-Control-Max-Age", "3600")

		if r.Method == http.MethodOptions {
			// Handle preflight - headers already set above
			// 处理 preflight —— 相关 header 已在上面设置
			w.WriteHeader(http.StatusOK)
			return
		}

		next.ServeHTTP(w, r)
	})
}

// Helper functions
// 辅助函数
func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getEnvOrDefaultInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if intValue, err := strconv.Atoi(value); err == nil {
			return intValue
		}
	}
	return defaultValue
}
