package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/robfig/cron/v3"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/attachments"
	auth "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/auth"
	cfg "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/config"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/db"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/degradation"
	ometrics "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/metrics"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/models"
	common "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/common"
	pb "github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pb/orchestrator"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/pricing"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/schedules"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/streaming"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows"
)

// validateCronSchedule validates a cron expression using standard 5-field format
// or Temporal-compatible descriptors (e.g., @every 1h, @hourly, @daily).
// Returns an error if the expression is invalid.
// validateCronSchedule 校验 cron 表达式，支持标准 5 段格式
// 或 Temporal 兼容描述符（如 @every 1h、@hourly、@daily）。
// 表达式非法时返回错误。
func validateCronSchedule(cronExpr string) error {
	cronExpr = strings.TrimSpace(cronExpr)
	if cronExpr == "" {
		return fmt.Errorf("empty cron expression")
	}

	// Support Temporal-compatible descriptors (e.g., @every 1h, @hourly, @daily)
	// 支持 Temporal 兼容描述符（如 @every 1h、@hourly、@daily）
	if strings.HasPrefix(cronExpr, "@") {
		// Use parser with descriptor support
		// 使用支持描述符的 parser
		parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
		_, err := parser.Parse(cronExpr)
		return err
	}

	// Standard 5-field cron (minute, hour, day-of-month, month, day-of-week)
	// 标准 5 段 cron（分、时、日、月、周）
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
	_, err := parser.Parse(cronExpr)
	return err
}

// OrchestratorService implements the Orchestrator gRPC service
// OrchestratorService 实现 Orchestrator gRPC 服务
type OrchestratorService struct {
	pb.UnimplementedOrchestratorServiceServer
	temporalClient  client.Client
	sessionManager  *session.Manager
	humanActivities *activities.HumanInterventionActivities
	dbClient        *db.Client
	logger          *zap.Logger
	degradeMgr      *degradation.Manager
	workflowConfig  *activities.WorkflowConfig
	scheduleManager *schedules.Manager

	// Optional typed configuration snapshot for defaults
	// 可选的 typed 配置快照，用于提供默认值
	shCfg *cfg.ShannonConfig

	// Provider for per-request default workflow flags
	// 为每个 request 的默认 workflow 开关提供取值的 provider
	getWorkflowDefaults func() (bypassSingle bool)
}

const taskInputPreflightLimitBytes = 1536 * 1024

type taskInputPreflightResult struct {
	InitialSizeBytes int
	FinalSizeBytes   int
	TrimmedMessages  int
}

// SessionManager returns the session manager for use by other services
// SessionManager 返回 session 管理器，供其它服务使用
func (s *OrchestratorService) SessionManager() *session.Manager {
	return s.sessionManager
}

func estimateTaskInputPayloadSizeBytes(input workflows.TaskInput) (int, error) {
	payloads, err := converter.GetDefaultDataConverter().ToPayloads(input)
	if err != nil {
		return 0, fmt.Errorf("failed to serialize task input: %w", err)
	}
	return proto.Size(payloads), nil
}

func enforceTaskInputPayloadLimit(input *workflows.TaskInput, limitBytes int) (taskInputPreflightResult, error) {
	var result taskInputPreflightResult
	if input == nil {
		return result, nil
	}

	sizeBytes, err := estimateTaskInputPayloadSizeBytes(*input)
	if err != nil {
		return result, err
	}

	result.InitialSizeBytes = sizeBytes
	result.FinalSizeBytes = sizeBytes
	if sizeBytes <= limitBytes {
		return result, nil
	}

	originalHistory := input.History
	trimmedHistory := originalHistory

	for len(trimmedHistory) > 0 && result.FinalSizeBytes > limitBytes {
		trimmedHistory = trimmedHistory[1:]

		candidate := *input
		candidate.History = trimmedHistory

		sizeBytes, err = estimateTaskInputPayloadSizeBytes(candidate)
		if err != nil {
			return result, err
		}

		result.FinalSizeBytes = sizeBytes
		result.TrimmedMessages = len(originalHistory) - len(trimmedHistory)
	}

	input.History = trimmedHistory
	if result.FinalSizeBytes > limitBytes {
		return result, fmt.Errorf(
			"task input payload %d bytes exceeds the %d-byte preflight limit even after trimming %d history messages",
			result.FinalSizeBytes,
			limitBytes,
			result.TrimmedMessages,
		)
	}

	return result, nil
}

func workflowStartStatusError(err error) error {
	if err == nil {
		return nil
	}

	message := fmt.Sprintf("failed to start workflow: %v", err)

	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, message)
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, message)
	}

	if st, ok := status.FromError(err); ok {
		return status.Error(st.Code(), message)
	}

	var invalidArgumentErr *serviceerror.InvalidArgument
	if errors.As(err, &invalidArgumentErr) {
		return status.Error(codes.InvalidArgument, message)
	}

	var deadlineErr *serviceerror.DeadlineExceeded
	if errors.As(err, &deadlineErr) {
		return status.Error(codes.DeadlineExceeded, message)
	}

	var resourceExhaustedErr *serviceerror.ResourceExhausted
	if errors.As(err, &resourceExhaustedErr) {
		return status.Error(codes.ResourceExhausted, message)
	}

	return status.Error(codes.Internal, message)
}

// Shutdown gracefully stops all background services
// Shutdown 优雅停止所有后台服务
func (s *OrchestratorService) Shutdown() error {
	if s.degradeMgr != nil {
		if err := s.degradeMgr.Stop(); err != nil {
			s.logger.Error("Failed to stop degradation manager", zap.Error(err))
		} else {
			s.logger.Info("Degradation manager stopped")
		}
	}
	return nil
}

// SetShannonConfig provides a snapshot of typed configuration (optional).
// SetShannonConfig 提供类型化配置的快照（可选）。
func (s *OrchestratorService) SetShannonConfig(c *cfg.ShannonConfig) {
	s.shCfg = c
}

// SetTemporalClient sets or replaces the Temporal client after service construction.
// SetTemporalClient 在服务构造后设置或替换 Temporal client。
func (s *OrchestratorService) SetTemporalClient(c client.Client) {
	s.temporalClient = c
}

// SetScheduleManager sets the schedule manager after service construction.
// SetScheduleManager 在服务构造后设置定时任务管理器。
func (s *OrchestratorService) SetScheduleManager(m *schedules.Manager) {
	s.scheduleManager = m
}

// RecordUsageDetails carries the full cache-aware breakdown that enterprise
// quota implementations (shannon-cloud) consume after cherry-pick. The OSS
// implementation of RecordUsage is a no-op; CacheAwareTotalTokens is
// pre-computed by the caller as
//
//	InputTokens + OutputTokens + CacheReadTokens + CacheCreationTokens
//
// so downstream code does not need to re-derive it. Enterprise overrides
// should still verify the invariant before persisting.
// RecordUsageDetails 携带完整的 cache 感知明细，供企业版配额实现
// （shannon-cloud）在 cherry-pick 后使用。OSS 的 RecordUsage
// 实现为空操作；CacheAwareTotalTokens 由调用方预先算好为
// InputTokens + OutputTokens + CacheReadTokens + CacheCreationTokens，
// 下游代码无需重新推导。企业版覆盖实现仍应在持久化前校验该不变量。
type RecordUsageDetails struct {
	InputTokens           int64
	OutputTokens          int64
	CacheReadTokens       int64
	CacheCreationTokens   int64
	CacheCreation1hTokens int64
	CacheAwareTotalTokens int64
	Model                 string
	Provider              string
}

// EnsureCacheAwareTotal back-fills CacheAwareTotalTokens from the parts when
// the caller forgot to pre-compute it. Enterprise quota recorders gate on
// CacheAwareTotalTokens, so a missing rollup would otherwise be silently
// dropped — turning the field into a footgun for new call sites.
// 调用方忘记预先计算时，EnsureCacheAwareTotal 由各分项回填 CacheAwareTotalTokens。
// 企业版配额记录以 CacheAwareTotalTokens 为门槛，缺失汇总会被静默丢弃——
// 这会让该字段成为新调用点的陷阱。
func (d *RecordUsageDetails) EnsureCacheAwareTotal() {
	if d == nil || d.CacheAwareTotalTokens > 0 {
		return
	}
	parts := d.InputTokens + d.OutputTokens + d.CacheReadTokens + d.CacheCreationTokens
	if parts > 0 {
		d.CacheAwareTotalTokens = parts
	}
}

// RecordUsage is a no-op in the open source version. shannon-cloud overrides
// this method to drive enterprise quota tracking; the RecordUsageDetails
// argument carries the full cache-aware breakdown so the override can bill
// prompt-cache cost without re-querying the DB.
// RecordUsage 在开源版本中是空操作。shannon-cloud 覆盖该方法
// 以驱动企业版配额统计；RecordUsageDetails 参数携带完整的 cache 感知
// 明细，使覆盖实现无需重查 DB 即可计算 prompt cache 成本。
func (s *OrchestratorService) RecordUsage(ctx context.Context, tenantID uuid.UUID, workflowID string, details RecordUsageDetails) {
}

// SetWorkflowDefaultsProvider sets a provider for BypassSingleResult default
// SetWorkflowDefaultsProvider 设置 BypassSingleResult 默认值的 provider
func (s *OrchestratorService) SetWorkflowDefaultsProvider(f func() bool) {
	s.getWorkflowDefaults = f
}

// ListTemplates returns summaries of loaded templates from the registry
// ListTemplates 返回注册表中已加载模板的摘要
func (s *OrchestratorService) ListTemplates(ctx context.Context, _ *pb.ListTemplatesRequest) (*pb.ListTemplatesResponse, error) {
	reg := workflows.TemplateRegistry()
	summaries := reg.List()
	out := make([]*pb.TemplateSummary, 0, len(summaries))
	for _, ts := range summaries {
		out = append(out, &pb.TemplateSummary{
			Name:        ts.Name,
			Version:     ts.Version,
			Key:         ts.Key,
			ContentHash: ts.ContentHash,
		})
	}
	return &pb.ListTemplatesResponse{Templates: out}, nil
}

// NewOrchestratorService creates a new orchestrator service
// Pass nil for sessionCfg to use default configuration
// NewOrchestratorService 创建 orchestrator 服务
// sessionCfg 传 nil 以使用默认配置
func NewOrchestratorService(temporalClient client.Client, dbClient *db.Client, logger *zap.Logger, sessionCfg *session.ManagerConfig) (*OrchestratorService, error) {
	// Initialize session manager with retry (handles startup races)
	// 带 retry 地初始化 session 管理器（应对启动期竞态）
	redisAddr := os.Getenv("REDIS_ADDR")
	if redisAddr == "" {
		redisAddr = "redis:6379"
	}

	var sessionMgr *session.Manager
	var err error
	for attempt := 1; attempt <= 15; attempt++ {
		sessionMgr, err = session.NewManagerWithConfig(redisAddr, logger, sessionCfg)
		if err == nil {
			break
		}
		// Exponential-ish backoff capped at 5s
		// 近似指数退避，上限 5s
		delay := min(time.Duration(attempt), 5)
		logger.Warn("Redis not ready for session manager, retrying",
			zap.Int("attempt", attempt),
			zap.String("redis_addr", redisAddr),
			zap.Duration("sleep", delay*time.Second),
			zap.Error(err),
		)
		time.Sleep(delay * time.Second)
	}
	if err != nil && sessionMgr == nil {
		return nil, fmt.Errorf("failed to initialize session manager after retries: %w", err)
	}

	// Create degradation manager (wire redis/db wrappers)
	// 创建降级管理器（接入 redis/db 包装）
	var redisWrapper interface{ IsCircuitBreakerOpen() bool }
	if sessionMgr != nil {
		redisWrapper = sessionMgr.RedisWrapper()
	}
	var dbWrapper interface{ IsCircuitBreakerOpen() bool }
	if dbClient != nil {
		dbWrapper = dbClient.Wrapper()
	}

	// Load workflow configuration
	// 加载 workflow 配置
	ctx := context.Background()
	workflowCfg, err := activities.GetWorkflowConfig(ctx)
	if err != nil {
		logger.Warn("Failed to load workflow config, using defaults", zap.Error(err))
		// Use default config with standard thresholds
		// 使用带标准阈值的默认配置
		workflowCfg = &activities.WorkflowConfig{
			ComplexitySimpleThreshold: 0.3,
			ComplexityMediumThreshold: 0.5,
		}
	}

	service := &OrchestratorService{
		temporalClient:  temporalClient,
		sessionManager:  sessionMgr,
		humanActivities: activities.NewHumanInterventionActivities(),
		dbClient:        dbClient,
		logger:          logger,
		degradeMgr:      degradation.NewManager(redisWrapper, dbWrapper, logger),
		workflowConfig:  workflowCfg,
	}

	// Start degradation manager background monitoring
	// 启动 degradation manager 后台监控
	if service.degradeMgr != nil {
		ctx := context.Background() // Background context for service lifecycle 注：服务生命周期使用的 background context
		if err := service.degradeMgr.Start(ctx); err != nil {
			logger.Warn("Failed to start degradation manager", zap.Error(err))
		} else {
			logger.Info("Degradation manager started successfully")
		}
	}

	return service, nil
}

// SubmitTask submits a new task for orchestration
// SubmitTask 提交新的 task 进行编排
func (s *OrchestratorService) SubmitTask(ctx context.Context, req *pb.SubmitTaskRequest) (*pb.SubmitTaskResponse, error) {
	if s.temporalClient == nil {
		return nil, status.Error(codes.Unavailable, "Temporal not ready")
	}
	// gRPC metrics timing
	// gRPC 指标计时
	grpcStart := time.Now()
	defer func() {
		ometrics.RecordGRPCMetrics("orchestrator", "SubmitTask", "OK", time.Since(grpcStart).Seconds())
	}()
	s.logger.Info("Received SubmitTask request",
		zap.String("query", req.Query),
		zap.String("user_id", req.Metadata.GetUserId()),
		zap.String("session_id", req.Metadata.GetSessionId()),
	)

	// Prefer authenticated context for identity and tenancy
	// 身份与租户优先使用通过 authentication 的 context
	var tenantID string
	var userID string
	if userCtx, err := auth.GetUserContext(ctx); err == nil {
		// Check if this is the default dev user (indicates skipAuth mode)
		// 判断是否为默认 dev 用户（表示处于 skipAuth 模式）
		if userCtx.UserID.String() == "00000000-0000-0000-0000-000000000002" && req.Metadata.GetUserId() != "" {
			// In dev/demo mode with skipAuth, prefer the userId from request metadata
			// This allows testing with different user identities
			// skipAuth 的 dev/demo 模式下，优先使用 request metadata 中的 userId
			// 以便用不同用户身份测试
			userID = req.Metadata.GetUserId()
		} else {
			userID = userCtx.UserID.String()
		}
		tenantID = userCtx.TenantID.String()
	} else {
		// Fallback to request metadata for backward compatibility
		// 为向后兼容回退到 request metadata
		userID = req.Metadata.GetUserId()
		tenantID = req.Metadata.GetTenantId()
	}

	sessionID := req.Metadata.GetSessionId()
	dbSessionID := sessionID // Use the requested ID for DB persistence/metrics 注：使用 request 中的 ID 做 DB 持久化/指标
	runtimeSessionID := sessionID // Session ID used for Redis/session manager/history 注：用于 Redis/session 管理器/历史的 session ID

	// Get or create session
	// 获取或创建 session
	var sess *session.Session
	var err error

	if sessionID != "" {
		// Try to retrieve existing session
		// 尝试取回已存在的 session
		sess, err = s.sessionManager.GetSession(ctx, sessionID)
		if err != nil && err != session.ErrSessionNotFound {
			s.logger.Warn("Failed to retrieve session", zap.Error(err))
		}

		// If session not found and we have a DB client, check for external_id alias
		// This handles the case where the client sends a UUID but the Redis session
		// was created with an external_id (or vice versa)
		// 若 session 未找到且有 DB client，则检查 external_id 别名
		// 用于处理客户端发送 UUID 而 Redis session 是用 external_id
		// 创建的情况（或反之）
		if sess == nil && s.dbClient != nil {
			var externalID sql.NullString
			row := s.dbClient.Wrapper().QueryRowContext(ctx, `
				SELECT context->>'external_id'
				FROM sessions
				WHERE id::text = $1 AND deleted_at IS NULL`, sessionID)
			if err := row.Scan(&externalID); err == nil && externalID.Valid && externalID.String != "" {
				// Try to get session using the external_id
				// 尝试用 external_id 获取 session
				aliasedSession, aliasErr := s.sessionManager.GetSession(ctx, externalID.String)
				if aliasErr == nil && aliasedSession != nil {
					sess = aliasedSession
					runtimeSessionID = aliasedSession.ID
					dbSessionID = runtimeSessionID
					s.logger.Debug("Resolved session via external_id alias",
						zap.String("requested_id", sessionID),
						zap.String("resolved_id", externalID.String))
				}
			}
		}

		// SECURITY: Validate session ownership
		// SECURITY：校验 session 归属
		if sess != nil && sess.UserID != userID {
			s.logger.Warn("User attempted to access another user's session",
				zap.String("requesting_user", userID),
				zap.String("session_owner", sess.UserID),
				zap.String("session_id", sessionID),
			)
			// Treat as if session doesn't exist - force new session creation
			// 按 session 不存在处理——强制新建 session
			sess = nil
			// Note: We don't return an error to avoid leaking session existence
			// 注意：不返回错误，以免泄漏 session 是否存在
		}
	}

	// Prepare session context for PostgreSQL persistence
	// 准备用于 PostgreSQL 持久化的 session context
	var sessionContext map[string]any

	// Create new session if needed
	// 如需要则创建新 session
	if sess == nil {
		var createErr error

		// Build initial session context with task context for first task
		// 为第一个 task 构建带 task context 的初始 session context
		sessionContext = map[string]any{
			"created_from": "orchestrator",
		}

		// Preserve first task's context (including role) in session context
		// 在 session context 中保留首个 task 的 context（含 role）
		if req.Context != nil {
			taskCtx := req.Context.AsMap()

			// Store entire task context as first_task_context for reference
			// 把整个 task context 存为 first_task_context 以便参考
			sessionContext["first_task_context"] = taskCtx

			// Explicitly preserve role field for quick access
			// 显式保留 role 字段以便快速访问
			if role, ok := taskCtx["role"].(string); ok && role != "" {
				sessionContext["role"] = role
				sessionContext["first_task_mode"] = role // Use role as first_task_mode 注：用 role 作为 first_task_mode
			}

			// Preserve research mode flags
			// 保留 research 模式标志
			if forceResearch, ok := taskCtx["force_research"].(bool); ok {
				sessionContext["force_research"] = forceResearch
			}
			if researchStrategy, ok := taskCtx["research_strategy"].(string); ok && researchStrategy != "" {
				sessionContext["research_strategy"] = researchStrategy
			}
		}

		// If a specific session ID was requested, use it; otherwise generate new
		// 若 request 指定了 session ID 则使用它，否则新建
		if sessionID != "" {
			// Create session with the requested ID
			// 按请求的 ID 创建 session
			sess, createErr = s.sessionManager.CreateSessionWithID(ctx, sessionID, userID, tenantID, sessionContext)
		} else {
			// Generate new session ID
			// 生成新的 session ID
			sess, createErr = s.sessionManager.CreateSession(ctx, userID, tenantID, sessionContext)
			sessionID = sess.ID
		}
		runtimeSessionID = sess.ID
		dbSessionID = runtimeSessionID
		if createErr != nil {
			return nil, status.Error(codes.Internal, "failed to create session")
		}
		s.logger.Info("Created new session",
			zap.String("session_id", sessionID),
			zap.Any("context", sessionContext))
	} else {
		// For existing sessions, use metadata from Redis session for PostgreSQL upsert
		// 已有 session 使用 Redis session 中的 metadata 做 PostgreSQL upsert
		if sess.Metadata != nil {
			sessionContext = sess.Metadata
		}
	}
	// Ensure session exists in PostgreSQL for FK integrity (idempotent)
	// 确保 session 在 PostgreSQL 中存在，以满足外键完整性（幂等）
	if s.dbClient != nil && dbSessionID != "" {
		// Prefer explicit userID from request; fall back to session's user
		// 优先使用 request 中的显式 userID；否则回退到 session 的 user
		dbUserID := userID
		if dbUserID == "" && sess != nil && sess.UserID != "" {
			dbUserID = sess.UserID
		}

		// Use the sessionContext we built earlier (don't read from sess.Context which may be empty)
		// 使用此前构建的 sessionContext（不要读可能为空的 sess.Context）
		dbSessionContext := sessionContext

		s.logger.Debug("Ensuring session exists in PostgreSQL",
			zap.String("session_id", dbSessionID),
			zap.String("user_id", dbUserID),
			zap.Any("context", dbSessionContext))
		if err := s.dbClient.CreateSession(ctx, dbSessionID, dbUserID, tenantID, dbSessionContext); err != nil {
			s.logger.Warn("Failed to ensure session in database",
				zap.String("session_id", dbSessionID),
				zap.Error(err))
			// Continue anyway - Redis session is available
			// 仍然继续——Redis session 可用
		}
	} else if s.dbClient == nil {
		s.logger.Debug("dbClient is nil; skipping session persistence")
	}

	// Add current query to history, enriched with attachment summaries.
	// Also persist attachment metadata so the frontend can render thumbnails
	// when loading conversation history (after Redis TTL expires).
	// 把当前查询加入历史，并附上附件摘要。
	// 同时持久化附件 metadata，使前端在加载 session 历史时（Redis TTL 过期后）
	// 仍能渲染缩略图。
	userContent := req.Query
	var msgMetadata map[string]any
	if req.Context != nil {
		ctxMap := req.Context.AsMap()
		if atts, ok := ctxMap["attachments"]; ok {
			if attList, ok := atts.([]any); ok && len(attList) > 0 {
				var descs []string
				var attsMeta []map[string]any
				for _, a := range attList {
					if am, ok := a.(map[string]any); ok {
						filename, _ := am["filename"].(string)
						if filename == "" {
							filename = "file"
						}
						mediaType, _ := am["media_type"].(string)
						descs = append(descs, fmt.Sprintf("[Attached: %s (%s)]", filename, mediaType))
						// Persist lightweight metadata (no binary data) for frontend rendering
						// 持久化轻量 metadata（不含二进制数据）供前端渲染
						meta := map[string]any{
							"filename":   filename,
							"media_type": mediaType,
						}
						if id, ok := am["id"].(string); ok {
							meta["id"] = id
						}
						if size, ok := am["size_bytes"]; ok {
							meta["size_bytes"] = size
						}
						// Frontend can optionally send a small thumbnail data URL
						// 前端可选地发送一个小体积的缩略图 data URL
						if thumb, ok := am["thumbnail"].(string); ok && len(thumb) <= attachments.MaxAttachmentThumbnailBytes {
							meta["thumbnail"] = thumb
						}
						attsMeta = append(attsMeta, meta)
					}
				}
				if len(descs) > 0 {
					userContent += "\n" + strings.Join(descs, " ")
				}
				if len(attsMeta) > 0 {
					msgMetadata = map[string]any{
						"attachments": attsMeta,
					}
				}
			}
		}
	}
	if err := s.sessionManager.AddMessage(ctx, runtimeSessionID, session.Message{
		ID:        fmt.Sprintf("msg-%d", time.Now().UnixNano()),
		Role:      "user",
		Content:   userContent,
		Timestamp: time.Now(),
		Metadata:  msgMetadata,
	}); err != nil {
		s.logger.Warn("Failed to append message to session history",
			zap.String("session_id", runtimeSessionID),
			zap.Error(err))
	}

	// Create workflow ID
	// 创建 workflow ID
	workflowID := fmt.Sprintf("task-%s-%d", userID, time.Now().Unix())

	// Build session context for workflow
	// Determine desired window size with priority:
	// 1) Request override (context.history_window_size)
	// 2) Preset (context.use_case_preset == "debugging")
	// 3) Env var HISTORY_WINDOW_MESSAGES
	// 4) Default (50)
	// 为 workflow 构建 session context
	// 按优先级确定期望的窗口大小：
	// 1) request 覆盖值（context.history_window_size）
	// 2) 预设（context.use_case_preset == "debugging"）
	// 3) 环境变量 HISTORY_WINDOW_MESSAGES
	// 4) 默认值（50）

	clamp := func(n, lo, hi int) int {
		if n < lo {
			return lo
		}
		if n > hi {
			return hi
		}
		return n
	}

	parseBoolish := func(v any) bool {
		switch val := v.(type) {
		case bool:
			return val
		case string:
			trimmed := strings.TrimSpace(val)
			if trimmed == "" {
				return false
			}
			if b, err := strconv.ParseBool(trimmed); err == nil {
				return b
			}
			lower := strings.ToLower(trimmed)
			return lower == "1" || lower == "yes" || lower == "y"
		case float64:
			return val != 0
		case int:
			return val != 0
		default:
			return false
		}
	}

	ctxMap := map[string]any{}
	if req.Context != nil {
		ctxMap = req.Context.AsMap()
	}

	templateName := ""
	templateVersion := ""
	disableAI := false

	if req.Metadata != nil {
		if labels := req.Metadata.GetLabels(); labels != nil {
			if v, ok := labels["template"]; ok {
				templateName = strings.TrimSpace(v)
			}
			if v, ok := labels["template_version"]; ok {
				templateVersion = strings.TrimSpace(v)
			}
			if v, ok := labels["disable_ai"]; ok {
				disableAI = parseBoolish(v)
			}
		}
	}

	if templateName == "" {
		if v, ok := ctxMap["template"].(string); ok {
			templateName = strings.TrimSpace(v)
		}
	}
	if templateVersion == "" {
		if v, ok := ctxMap["template_version"].(string); ok {
			templateVersion = strings.TrimSpace(v)
		}
	}
	if !disableAI {
		if v, ok := ctxMap["disable_ai"]; ok {
			disableAI = parseBoolish(v)
		}
	}

	if templateName != "" {
		ctxMap["template"] = templateName
	}
	if templateVersion != "" {
		ctxMap["template_version"] = templateVersion
	}
	if disableAI {
		ctxMap["disable_ai"] = disableAI
	}

	desiredWindow := 0
	if v, ok := ctxMap["history_window_size"]; ok {
		switch t := v.(type) {
		case float64:
			desiredWindow = int(t)
		case int:
			desiredWindow = t
		case string:
			if n, err := strconv.Atoi(strings.TrimSpace(t)); err == nil {
				desiredWindow = n
			}
		}
	} else if preset, ok := ctxMap["use_case_preset"].(string); ok && strings.EqualFold(preset, "debugging") {
		// Debugging preset uses a larger default
		// debugging preset 使用更大的默认值
		if v := os.Getenv("HISTORY_WINDOW_DEBUG_MESSAGES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				desiredWindow = n
			}
		}
		if desiredWindow == 0 {
			if s.shCfg != nil && s.shCfg.Session.ContextWindowDebugging > 0 {
				desiredWindow = s.shCfg.Session.ContextWindowDebugging
			} else {
				desiredWindow = 75
			}
		}
	}
	if desiredWindow == 0 {
		if v := os.Getenv("HISTORY_WINDOW_MESSAGES"); v != "" {
			if n, err := strconv.Atoi(v); err == nil {
				desiredWindow = n
			}
		}
	}
	if desiredWindow == 0 {
		if s.shCfg != nil && s.shCfg.Session.ContextWindowDefault > 0 {
			desiredWindow = s.shCfg.Session.ContextWindowDefault
		} else {
			desiredWindow = 50
		}
	}
	historySize := clamp(desiredWindow, 5, 200)

	history := sess.GetRecentHistory(historySize)

	if _, ok := ctxMap["primers_count"]; !ok {
		if s.shCfg != nil && s.shCfg.Session.PrimersCount >= 0 {
			ctxMap["primers_count"] = s.shCfg.Session.PrimersCount
		}
	}
	if _, ok := ctxMap["recents_count"]; !ok {
		if s.shCfg != nil && s.shCfg.Session.RecentsCount >= 0 {
			ctxMap["recents_count"] = s.shCfg.Session.RecentsCount
		}
	}
	if _, ok := ctxMap["compression_trigger_ratio"]; !ok {
		if v := os.Getenv("COMPRESSION_TRIGGER_RATIO"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				ctxMap["compression_trigger_ratio"] = f
			}
		}
	}
	if _, ok := ctxMap["compression_target_ratio"]; !ok {
		if v := os.Getenv("COMPRESSION_TARGET_RATIO"); v != "" {
			if f, err := strconv.ParseFloat(v, 64); err == nil {
				ctxMap["compression_target_ratio"] = f
			}
		}
	}

	st, _ := structpb.NewStruct(ctxMap)
	req.Context = st

	// Emit a compact context-prep event (metadata only)
	// 发出一个紧凑的 context 准备 event（仅 metadata）
	estTokens := activities.EstimateTokensFromHistory(history)
	msg := activities.MsgContextPreparing(len(history), estTokens)
	streaming.Get().Publish(workflowID, streaming.Event{
		WorkflowID: workflowID,
		Type:       string(activities.StreamEventDataProcessing),
		AgentID:    "orchestrator",
		Message:    msg,
		Timestamp:  time.Now(),
	})

	// Prepare workflow input with session context
	// 用 session context 准备 workflow input
	input := workflows.TaskInput{
		Query:           req.Query,
		UserID:          userID,
		TenantID:        tenantID,
		SessionID:       runtimeSessionID,
		Context:         ctxMap,
		Mode:            "",
		TemplateName:    templateName,
		TemplateVersion: templateVersion,
		DisableAI:       disableAI,
		History:         convertHistoryForWorkflow(history),
		SessionCtx:      sess.Context,
		RequireApproval: req.RequireApproval,
		ApprovalTimeout: 1800, // Default 30 minutes 注：默认 30 分钟
	}

	// Apply deterministic workflow behavior flags from provider/env
	// Defaults: skip evaluation, bypass single result
	// 应用来自 provider/env 的确定性 workflow 行为开关
	// 默认：跳过评估、跳过单结果汇总
	input.BypassSingleResult = true
	if s.getWorkflowDefaults != nil {
		input.BypassSingleResult = s.getWorkflowDefaults()
	}

	preflight, err := enforceTaskInputPayloadLimit(&input, taskInputPreflightLimitBytes)
	if err != nil {
		s.logger.Warn("Task input failed payload preflight",
			zap.Int("initial_payload_bytes", preflight.InitialSizeBytes),
			zap.Int("final_payload_bytes", preflight.FinalSizeBytes),
			zap.Int("history_messages_trimmed", preflight.TrimmedMessages),
			zap.Int("history_messages_remaining", len(input.History)),
			zap.Error(err),
		)
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if preflight.TrimmedMessages > 0 {
		s.logger.Warn("Trimmed task history to fit payload preflight",
			zap.Int("initial_payload_bytes", preflight.InitialSizeBytes),
			zap.Int("final_payload_bytes", preflight.FinalSizeBytes),
			zap.Int("history_messages_trimmed", preflight.TrimmedMessages),
			zap.Int("history_messages_remaining", len(input.History)),
			zap.Int("payload_limit_bytes", taskInputPreflightLimitBytes),
		)
	}

	// Always route through OrchestratorWorkflow which will analyze complexity
	// and handle simple queries efficiently
	// 始终经由 OrchestratorWorkflow，由它分析复杂度
	// 并高效处理简单查询
	var mode common.ExecutionMode
	if req.ManualDecomposition != nil {
		mode = req.ManualDecomposition.Mode
	} else {
		// Let OrchestratorWorkflow determine complexity and route appropriately
		// 由 OrchestratorWorkflow 判定复杂度并选择合适路由
		mode = common.ExecutionMode_EXECUTION_MODE_STANDARD
		s.logger.Info("Routing to OrchestratorWorkflow for complexity analysis",
			zap.String("query", req.Query),
		)
	}

	// Start appropriate workflow based on mode
	// 根据 mode 启动相应的 workflow
	var workflowExecution client.WorkflowRun
	workflowType := "OrchestratorWorkflow"
	modeStr := "standard"

	// Store metadata in workflow memo for retrieval later
	// 把 metadata 存入 workflow memo 以便后续读取
	memo := map[string]any{
		"user_id":    userID,
		"session_id": runtimeSessionID,
		"tenant_id":  tenantID,
		"query":      req.Query,
	}
	if templateName != "" {
		memo["template"] = templateName
		if templateVersion != "" {
			memo["template_version"] = templateVersion
		}
	}
	if disableAI {
		memo["disable_ai"] = disableAI
	}

	// Determine priority from metadata labels (optional)
	// 从 metadata label 确定优先级（可选）
	queue := "shannon-tasks"
	priority := "normal"   // Track priority for logging 注：记录优先级用于日志
	workflowOverride := "" // Optional workflow override via label 注：通过 label 可选的 workflow 覆盖
	forcedModeLabel := ""  // Optional mode override for router (standard|complex) 注：可选的 router 模式覆盖（standard|complex）

	// Check if priority queues are enabled
	// 检查是否启用了优先级队列
	priorityQueuesEnabled := strings.EqualFold(os.Getenv("PRIORITY_QUEUES"), "on") ||
		os.Getenv("PRIORITY_QUEUES") == "1" ||
		strings.EqualFold(os.Getenv("PRIORITY_QUEUES"), "true")

	if req.Metadata != nil {
		labels := req.Metadata.GetLabels()
		if labels != nil {
			if p, ok := labels["priority"]; ok {
				priority = p
				priorityLower := strings.ToLower(p)

				// Only route to priority queues if PRIORITY_QUEUES is enabled
				// 仅在启用 PRIORITY_QUEUES 时才路由到优先级队列
				if priorityQueuesEnabled {
					switch priorityLower {
					case "critical":
						queue = "shannon-tasks-critical"
					case "high":
						queue = "shannon-tasks-high"
					case "normal":
						queue = "shannon-tasks" // Explicitly handle normal priority 注：显式处理 normal 优先级
					case "low":
						queue = "shannon-tasks-low"
					default:
						// Warn about invalid priority value and use default queue
						// 优先级值非法时告警并使用默认队列
						s.logger.Warn("Invalid priority value provided, using default queue",
							zap.String("priority", p),
							zap.String("valid_values", "critical, high, normal, low"),
							zap.String("workflow_id", workflowID))
						priority = "normal" // Reset to normal for invalid priorities 注：非法优先级时重置为 normal
					}
				} else if priorityLower != "normal" {
					// Priority queues disabled, log override to default queue
					// 优先级队列未启用，记录降级到默认队列的覆盖行为
					s.logger.Debug("Priority label ignored in single-queue mode",
						zap.String("priority", p),
						zap.String("workflow_id", workflowID),
						zap.String("queue", queue))
				}
			}
			// Optional workflow override: labels["workflow"] = "supervisor" | "dag"
			// 可选的 workflow 覆盖：labels["workflow"] = "supervisor" | "dag"
			if wf, ok := labels["workflow"]; ok {
				workflowOverride = strings.ToLower(wf)
			} else if wf2, ok := labels["mode"]; ok {
				ml := strings.ToLower(strings.TrimSpace(wf2))
				switch ml {
				case "supervisor":
					workflowOverride = "supervisor"
				case "simple":
					workflowOverride = "simple"
				case "complex", "standard":
					forcedModeLabel = ml
				}
			}
		}
	}
	// Log queue selection for debugging
	// 记录队列选择以便调试
	if queue != "shannon-tasks" {
		s.logger.Info("Task routed to priority queue",
			zap.String("workflow_id", workflowID),
			zap.String("queue", queue),
			zap.String("priority", priority))
	}

	workflowOptions := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: queue,
		Memo:      memo,
	}

	// Check for cron schedule in metadata labels (e.g., labels["cron_schedule"] = "0 9 * * 1-5")
	// 检查 metadata labels 中的 cron 表达式（如 labels["cron_schedule"] = "0 9 * * 1-5"）
	if req.Metadata != nil {
		if labels := req.Metadata.GetLabels(); labels != nil {
			if cronSchedule, ok := labels["cron_schedule"]; ok && cronSchedule != "" {
				// Normalize and validate cron expression
				// 规范化并校验 cron 表达式
				cronSchedule = strings.TrimSpace(cronSchedule)
				if err := validateCronSchedule(cronSchedule); err != nil {
					s.logger.Error("Invalid cron schedule",
						zap.String("workflow_id", workflowID),
						zap.String("cron_schedule", cronSchedule),
						zap.Error(err))
					return nil, status.Errorf(codes.InvalidArgument, "invalid cron_schedule: %v", err)
				}
				workflowOptions.CronSchedule = cronSchedule
				s.logger.Info("Workflow configured with cron schedule",
					zap.String("workflow_id", workflowID),
					zap.String("cron_schedule", cronSchedule))
			}
		}
	}

	// Route based on explicit workflow override; otherwise use AgentDAGWorkflow
	// 按显式 workflow 覆盖路由；否则使用 AgentDAGWorkflow
	setMode := func(m string) {
		input.Mode = m
		modeStr = m
		memo["mode"] = m
	}

	wfFn := workflows.OrchestratorWorkflow
	switch workflowOverride {
	case "supervisor":
		setMode("supervisor")
		workflowType = "SupervisorWorkflow"
		wfFn = workflows.SupervisorWorkflow
		s.logger.Info("Starting SupervisorWorkflow", zap.String("workflow_id", workflowID))
	case "simple":
		setMode("simple")
		workflowType = "SimpleTaskWorkflow"
		wfFn = workflows.SimpleTaskWorkflow
		s.logger.Info("Starting SimpleTaskWorkflow", zap.String("workflow_id", workflowID))
	case "", "dag":
		// Default: route through OrchestratorWorkflow
		// 默认：经 OrchestratorWorkflow 路由
		if forcedModeLabel == "complex" || (forcedModeLabel == "" && mode == common.ExecutionMode_EXECUTION_MODE_COMPLEX) {
			setMode("complex")
		} else {
			setMode("standard")
		}
		s.logger.Info("Starting OrchestratorWorkflow (router)",
			zap.String("workflow_id", workflowID),
			zap.String("initial_mode", modeStr))
	default:
		// Unknown override: fall back to DAG
		// 未知的覆盖值：回退到 DAG
		s.logger.Warn("Unknown workflow override; falling back to router", zap.String("override", workflowOverride))
		setMode("standard")
		workflowType = "OrchestratorWorkflow"
	}

	workflowExecution, err = s.temporalClient.ExecuteWorkflow(ctx, workflowOptions, wfFn, input)

	if err != nil {
		s.logger.Error("Failed to start workflow", zap.Error(err))
		return nil, workflowStartStatusError(err)
	}

	// Write-on-submit: persist initial RUNNING record to task_executions table (idempotent by workflow_id)
	// Using synchronous save to ensure task exists before any token usage recording
	// 提交即写入：把初始 RUNNING 记录持久化到 task_executions 表（按 workflow_id 幂等）
	// 使用同步保存，确保记录 token 用量前 task 已存在
	if s.dbClient != nil {
		var uidPtr *uuid.UUID
		if userID != "" {
			if u, err := uuid.Parse(userID); err == nil {
				uidPtr = &u
			}
		}
		var tenantPtr *uuid.UUID
		if tenantID != "" {
			if t, err := uuid.Parse(tenantID); err == nil {
				tenantPtr = &t
			}
		}
		started := time.Now()

		// Generate task ID to ensure it exists for foreign key references
		// 生成 task ID，确保外键引用时已存在
		taskID := uuid.New()
		initial := &db.TaskExecution{
			ID:         taskID,
			WorkflowID: workflowExecution.GetID(),
			UserID:     uidPtr,
			SessionID:  dbSessionID,
			TenantID:   tenantPtr,
			Query:      req.Query,
			Mode:       modeStr,
			Status:     "RUNNING",
			StartedAt:  started,
			CreatedAt:  started,
		}

		// Persist submission context early so GET status can surface it while running
		// 提前持久化提交 context，使运行中的 GET status 也能返回它
		if len(ctxMap) > 0 {
			taskCtx := make(map[string]any, len(ctxMap))
			maps.Copy(taskCtx, ctxMap)
			initial.Metadata = db.JSONB{
				"task_context": taskCtx,
			}
		}

		// Synchronous save to task_executions to ensure it exists before workflow activities execute
		// This prevents foreign key violations when token_usage tries to reference the task
		// 同步保存到 task_executions，确保 workflow activity 执行前 task 已存在
		// 这可避免 token_usage 引用 task 时出现外键冲突
		if err := s.dbClient.SaveTaskExecution(ctx, initial); err != nil {
			// Log the error but don't fail the workflow - task will be saved again on completion
			// 记录错误但不让 workflow 失败——task 完成时会再次保存
			s.logger.Warn("Initial task persist failed, will retry on completion",
				zap.String("workflow_id", workflowExecution.GetID()),
				zap.String("task_id", taskID.String()),
				zap.Error(err))
		} else {
			s.logger.Debug("Initial task persisted successfully",
				zap.String("workflow_id", workflowExecution.GetID()),
				zap.String("task_id", taskID.String()))
		}

		// Start async finalizer to persist terminal state regardless of status polling
		// 启动异步终结器，使终态不依赖状态轮询也能持久化
		go s.watchAndPersist(workflowExecution.GetID(), workflowExecution.GetRunID())
	}

	// Create response with session info
	// 创建带 session 信息的 response
	response := &pb.SubmitTaskResponse{
		WorkflowId: workflowID,
		TaskId:     workflowExecution.GetID(),
		Status:     common.StatusCode_STATUS_CODE_OK,
		Message:    fmt.Sprintf("Task submitted successfully. Session: %s", sessionID),
		Decomposition: &pb.TaskDecomposition{
			Mode:            mode,
			ComplexityScore: 0.5, // Default/estimated score - actual will be determined during workflow execution 注：默认/估算分——实际分数在 workflow 执行期间确定
		},
		// Session ID is tracked internally, not returned in response for now
		// session ID 目前仅在内部跟踪，不在 response 中返回
	}

	s.logger.Info("Task submitted successfully",
		zap.String("workflow_id", workflowID),
		zap.String("run_id", workflowExecution.GetRunID()),
		zap.String("session_id", sessionID),
	)

	// Increment workflows started metric
	// 递增 workflows started 指标
	ometrics.WorkflowsStarted.WithLabelValues(workflowType, modeStr).Inc()

	return response, nil
}

// GetTaskStatus gets the status of a submitted task
// GetTaskStatus 获取已提交 task 的状态
func (s *OrchestratorService) GetTaskStatus(ctx context.Context, req *pb.GetTaskStatusRequest) (*pb.GetTaskStatusResponse, error) {
	grpcStart := time.Now()
	defer func() {
		ometrics.RecordGRPCMetrics("orchestrator", "GetTaskStatus", "OK", time.Since(grpcStart).Seconds())
	}()
	s.logger.Info("Received GetTaskStatus request", zap.String("task_id", req.TaskId))

	// Describe workflow for non-blocking status
	// Describe workflow 以非阻塞地获取状态
	desc, err := s.temporalClient.DescribeWorkflowExecution(ctx, req.TaskId, "")
	if err != nil || desc == nil || desc.WorkflowExecutionInfo == nil {
		// Only fallback to DB for NotFound errors (Temporal retention expired)
		// Other errors (outages, permission issues) should propagate as Internal
		// 仅对 NotFound 错误回退到 DB（Temporal 保留期已过）
		// 其它错误（服务中断、权限问题）应以 Internal 传播
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) && s.dbClient != nil {
			return s.getTaskStatusFromDB(ctx, req.TaskId)
		}
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to describe workflow: %v", err))
		}
		return nil, status.Error(codes.NotFound, "task not found")
	}

	// Enforce tenant ownership using memo if available
	// 若 memo 可用，按 memo 强制校验租户归属
	if desc.WorkflowExecutionInfo.Memo != nil {
		dataConverter := converter.GetDefaultDataConverter()
		if tenantField, ok := desc.WorkflowExecutionInfo.Memo.Fields["tenant_id"]; ok && tenantField != nil {
			var memoTenant string
			_ = dataConverter.FromPayload(tenantField, &memoTenant)
			if memoTenant != "" {
				if uc, err := auth.GetUserContext(ctx); err == nil && uc != nil {
					if uc.TenantID.String() != memoTenant {
						// Don't leak existence
						// 不泄漏是否存在
						return nil, status.Error(codes.NotFound, "task not found")
					}
				}
			}
		}
	}

	// Extract workflow metadata
	// 提取 workflow metadata
	workflowStartTime := desc.WorkflowExecutionInfo.StartTime
	workflowID := req.TaskId

	// Map Temporal status to API status
	// 把 Temporal 状态映射为 API 状态
	var statusOut pb.TaskStatus
	var statusStr string
	switch desc.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		statusOut = pb.TaskStatus_TASK_STATUS_COMPLETED
		statusStr = "COMPLETED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		// Default to RUNNING, but check control state for PAUSED
		// 默认 RUNNING，但需检查控制状态是否为 PAUSED
		statusOut = pb.TaskStatus_TASK_STATUS_RUNNING
		statusStr = "RUNNING"
		// Best-effort query control state to detect paused workflows
		// 尽力查询控制状态以识别已暂停的 workflow
		if ctrlResp, qErr := s.temporalClient.QueryWorkflow(ctx, req.TaskId, "", workflows.QueryControlState); qErr == nil {
			var ctrlState workflows.WorkflowControlState
			if ctrlResp.Get(&ctrlState) == nil && ctrlState.IsPaused {
				statusOut = pb.TaskStatus_TASK_STATUS_PAUSED
				statusStr = "PAUSED"
			}
		}

		// Race condition mitigation: the stream may emit WORKFLOW_COMPLETED slightly
		// before Temporal's visibility APIs show the workflow as closed.
		// Treat the stream signal as a hint and retry Describe briefly; only return
		// a terminal status once Temporal confirms it.
		// 缓解竞态：流可能比 Temporal 可见性 API 显示 workflow 已关闭
		// 稍早发出 WORKFLOW_COMPLETED。
		// 把流信号当作提示，短暂 retry Describe；仅在 Temporal 确认后
		// 才返回终态。
		if statusOut == pb.TaskStatus_TASK_STATUS_RUNNING {
			if streaming.Get().HasEmittedCompletion(ctx, req.TaskId) {
				s.logger.Debug("WORKFLOW_COMPLETED seen in stream; retrying Temporal Describe",
					zap.String("task_id", req.TaskId))

				retryCtx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
				defer cancel()

			retryLoop:
				for attempt := range 3 {
					if attempt > 0 {
						delay := time.Duration(attempt) * 50 * time.Millisecond
						timer := time.NewTimer(delay)
						select {
						case <-retryCtx.Done():
							timer.Stop()
							break retryLoop
						case <-timer.C:
						}
					}

					descAfterWait, descErr := s.temporalClient.DescribeWorkflowExecution(retryCtx, req.TaskId, "")
					if descErr != nil || descAfterWait == nil || descAfterWait.WorkflowExecutionInfo == nil {
						if retryCtx.Err() != nil {
							break
						}
						continue
					}

					// Update status only when Temporal confirms a non-running state.
					// 仅在 Temporal 确认非运行状态时才更新状态。
					if descAfterWait.WorkflowExecutionInfo.Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
						continue
					}

					desc = descAfterWait
					switch descAfterWait.WorkflowExecutionInfo.Status {
					case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
						statusOut = pb.TaskStatus_TASK_STATUS_COMPLETED
						statusStr = "COMPLETED"
					case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
						statusOut = pb.TaskStatus_TASK_STATUS_TIMEOUT
						statusStr = "TIMEOUT"
					case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
						statusOut = pb.TaskStatus_TASK_STATUS_FAILED
						statusStr = "FAILED"
					case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
						statusOut = pb.TaskStatus_TASK_STATUS_CANCELLED
						statusStr = "CANCELLED"
					case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
						statusOut = pb.TaskStatus_TASK_STATUS_FAILED
						statusStr = "FAILED"
					default:
						statusOut = pb.TaskStatus_TASK_STATUS_RUNNING
						statusStr = "RUNNING"
					}
					break retryLoop
				}
			}
		}
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		statusOut = pb.TaskStatus_TASK_STATUS_TIMEOUT
		statusStr = "TIMEOUT"
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		statusOut = pb.TaskStatus_TASK_STATUS_FAILED
		statusStr = "FAILED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		statusOut = pb.TaskStatus_TASK_STATUS_CANCELLED
		statusStr = "CANCELLED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
		statusOut = pb.TaskStatus_TASK_STATUS_FAILED
		statusStr = "FAILED"
	case enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW:
		statusOut = pb.TaskStatus_TASK_STATUS_RUNNING
		statusStr = "RUNNING"
	default:
		statusOut = pb.TaskStatus_TASK_STATUS_RUNNING
		statusStr = "RUNNING"
	}

	// Best-effort to fetch result if completed
	// 尽力在完成时获取结果
	var result workflows.TaskResult
	var resultErr error
	isTerminal := false

	if statusOut == pb.TaskStatus_TASK_STATUS_COMPLETED ||
		statusOut == pb.TaskStatus_TASK_STATUS_FAILED ||
		statusOut == pb.TaskStatus_TASK_STATUS_TIMEOUT ||
		statusOut == pb.TaskStatus_TASK_STATUS_CANCELLED {
		isTerminal = true

		if statusOut == pb.TaskStatus_TASK_STATUS_COMPLETED {
			we := s.temporalClient.GetWorkflow(ctx, req.TaskId, "")
			resultErr = we.Get(ctx, &result)
			if resultErr != nil {
				s.logger.Warn("Failed to get completed workflow result",
					zap.String("task_id", req.TaskId),
					zap.Error(resultErr))
				// Include error in response but don't fail the status request
				// response 中带上错误，但不让状态 request 失败
				result.ErrorMessage = fmt.Sprintf("Result retrieval failed: %v", resultErr)
			}
		}
	}

	// Extract session ID and other data for persistence and unified response
	// 提取 session ID 及其它数据用于持久化与统一 response
	var sessionID string
	var userID *uuid.UUID
	var tenantUUID *uuid.UUID
	var query string
	var mode string

	// Extract from workflow memo using data converter
	// 用 data converter 从 workflow memo 中提取
	if desc.WorkflowExecutionInfo != nil && desc.WorkflowExecutionInfo.Memo != nil {
		dataConverter := converter.GetDefaultDataConverter()

		// Extract user_id from memo
		// 从 memo 提取 user_id
		if userField, ok := desc.WorkflowExecutionInfo.Memo.Fields["user_id"]; ok && userField != nil {
			var userIDStr string
			if err := dataConverter.FromPayload(userField, &userIDStr); err == nil && userIDStr != "" {
				if uid, err := uuid.Parse(userIDStr); err == nil {
					userID = &uid
				}
			}
		}

		// Extract tenant_id from memo
		// 从 memo 提取 tenant_id
		if tenantField, ok := desc.WorkflowExecutionInfo.Memo.Fields["tenant_id"]; ok && tenantField != nil {
			var tenantIDStr string
			if err := dataConverter.FromPayload(tenantField, &tenantIDStr); err == nil && tenantIDStr != "" {
				if tid, err := uuid.Parse(tenantIDStr); err == nil {
					tenantUUID = &tid
				}
			}
		}

		// Extract session_id from memo
		// 从 memo 提取 session_id
		if sessionField, ok := desc.WorkflowExecutionInfo.Memo.Fields["session_id"]; ok && sessionField != nil {
			_ = dataConverter.FromPayload(sessionField, &sessionID)
		}

		// Extract query from memo
		// 从 memo 提取 query
		if queryField, ok := desc.WorkflowExecutionInfo.Memo.Fields["query"]; ok && queryField != nil {
			_ = dataConverter.FromPayload(queryField, &query)
		}

		// Extract mode from memo
		// 从 memo 提取 mode
		if modeField, ok := desc.WorkflowExecutionInfo.Memo.Fields["mode"]; ok && modeField != nil {
			_ = dataConverter.FromPayload(modeField, &mode)
		}
	}

	// Variables to store DB-aggregated metrics for terminal workflows
	// 用于保存终态 workflow DB 聚合指标的变量
	var dbTotalTokens int
	var dbTotalCost float64
	var dbPromptTokens int
	var dbCompletionTokens int
	var hasDBMetrics bool

	// Persist to database if terminal state
	// 若为终态则持久化到数据库
	if isTerminal && s.dbClient != nil {

		// Extract from result metadata if not in memo
		// memo 中没有时从结果 metadata 中提取
		if result.Metadata != nil {
			if query == "" {
				if q, ok := result.Metadata["query"].(string); ok {
					query = q
				}
			}
			if mode == "" {
				if m, ok := result.Metadata["mode"].(string); ok {
					mode = m
				}
			}
		}

		taskExecution := &db.TaskExecution{
			WorkflowID:  workflowID,
			UserID:      userID,
			SessionID:   sessionID,
			TenantID:    tenantUUID,
			Query:       query,
			Mode:        mode,
			Status:      statusStr,
			StartedAt:   workflowStartTime.AsTime(),
			TotalTokens: result.TokensUsed, // Will be overridden by metadata if available 注：若 metadata 可用则会被其覆盖
			Result:       &result.Result,
			ErrorMessage: &result.ErrorMessage,
		}

		// Set completed time if terminal (prefer Temporal CloseTime)
		// 终态时设置完成时间（优先使用 Temporal CloseTime）
		completedAt := getWorkflowEndTime(desc)
		taskExecution.CompletedAt = &completedAt

		// Calculate duration
		// 计算耗时
		if !workflowStartTime.AsTime().IsZero() {
			end := completedAt
			durationMs := int(end.Sub(workflowStartTime.AsTime()).Milliseconds())
			taskExecution.DurationMs = &durationMs
		}

		// Extract metadata from result
		// 从 result 提取 metadata
		if result.Metadata != nil {
			if complexity, ok := result.Metadata["complexity_score"].(float64); ok {
				taskExecution.ComplexityScore = complexity
			}
			if agentsUsed, ok := result.Metadata["num_agents"].(int); ok {
				taskExecution.AgentsUsed = agentsUsed
			}

			// Extract model and provider
			// 提取 model 与 provider
			if model, ok := result.Metadata["model"].(string); ok && model != "" {
				taskExecution.ModelUsed = model
			} else if model, ok := result.Metadata["model_used"].(string); ok && model != "" {
				taskExecution.ModelUsed = model
			}

			if provider, ok := result.Metadata["provider"].(string); ok && provider != "" {
				taskExecution.Provider = provider
			} else if taskExecution.ModelUsed != "" {
				// Fallback: detect provider from model name
				// 兜底：从 model 名称识别 provider
				taskExecution.Provider = detectProviderFromModel(taskExecution.ModelUsed)
			}

			// Extract token breakdown (prompt vs completion)
			// 提取 token 明细（prompt 与 completion）
			if inputTokens, ok := result.Metadata["input_tokens"].(float64); ok {
				taskExecution.PromptTokens = int(inputTokens)
			} else if inputTokens, ok := result.Metadata["input_tokens"].(int); ok {
				taskExecution.PromptTokens = inputTokens
			}

			if outputTokens, ok := result.Metadata["output_tokens"].(float64); ok {
				taskExecution.CompletionTokens = int(outputTokens)
			} else if outputTokens, ok := result.Metadata["output_tokens"].(int); ok {
				taskExecution.CompletionTokens = outputTokens
			}

			// Check if metadata has the correct total_tokens value (preferred over TokensUsed)
			// 检查 metadata 中是否有正确的 total_tokens（优先于 TokensUsed）
			if totalTokens, ok := result.Metadata["total_tokens"].(float64); ok && totalTokens > 0 {
				taskExecution.TotalTokens = int(totalTokens)
			} else if totalTokens, ok := result.Metadata["total_tokens"].(int); ok && totalTokens > 0 {
				taskExecution.TotalTokens = totalTokens
			}

			// If breakdown not available but we have total, estimate split (60% prompt, 40% completion)
			// 无明细但有总数时估算拆分（60% prompt、40% completion）
			if taskExecution.PromptTokens == 0 && taskExecution.CompletionTokens == 0 && taskExecution.TotalTokens > 0 {
				taskExecution.PromptTokens = taskExecution.TotalTokens * 6 / 10
				taskExecution.CompletionTokens = taskExecution.TotalTokens - taskExecution.PromptTokens
			}

			// Extract cost
			// 提取 cost
			if cost, ok := result.Metadata["cost_usd"].(float64); ok {
				taskExecution.TotalCostUSD = cost
			} else if cost, ok := result.Metadata["total_cost"].(float64); ok {
				taskExecution.TotalCostUSD = cost
			} else if result.TokensUsed > 0 {
				// Fallback: calculate cost from tokens
				// 兜底：按 token 计算 cost
				taskExecution.TotalCostUSD = calculateTokenCost(result.TokensUsed, result.Metadata)
			}

			// Extract tools_invoked count
			// 提取 tools_invoked 次数
			if toolsInvoked, ok := result.Metadata["tools_invoked"].(int); ok {
				taskExecution.ToolsInvoked = toolsInvoked
			} else if toolsInvoked, ok := result.Metadata["tools_invoked"].(float64); ok {
				taskExecution.ToolsInvoked = int(toolsInvoked)
			}

			// Preserve original request-level task_context fields (e.g., force_research, attachments).
			// The initial task creation stores these at task submission, but they may get lost
			// when result.Metadata overwrites them. Fetch and merge them back.
			// 保留 request 级的 task_context 字段（如 force_research、attachments）。
			// task 提交时会保存这些字段，但 result.Metadata 覆盖时可能丢失。
			// 此处重新取出并合并。
			if s.dbClient != nil {
				var origMetadataJSON sql.NullString
				row := s.dbClient.Wrapper().QueryRowContext(ctx, `
					SELECT metadata::text FROM task_executions WHERE workflow_id = $1`, workflowID)
				// Guard against nil row from circuit breaker
				// 防止 circuit breaker 返回 nil 行
				if row != nil {
					if err := row.Scan(&origMetadataJSON); err == nil && origMetadataJSON.Valid && origMetadataJSON.String != "" {
						var origMetadata map[string]any
						if jsonErr := json.Unmarshal([]byte(origMetadataJSON.String), &origMetadata); jsonErr == nil {
							if origTaskCtx, ok := origMetadata["task_context"].(map[string]any); ok {
								// Ensure metadata + task_context map exist before merge.
								// 合并前确保 metadata 与 task_context map 都存在。
								if result.Metadata == nil {
									result.Metadata = make(map[string]any)
								}
								resultTaskCtx, hasResultCtx := result.Metadata["task_context"].(map[string]any)
								if !hasResultCtx {
									resultTaskCtx = make(map[string]any)
								}
								// Preserve request-level fields that shouldn't be overwritten by runtime fields.
								// 保留不应被运行时字段覆盖的 request 级字段。
								requestFields := []string{
									"force_research",
									"synthesis_template",
									"synthesis_template_override",
									"attachments",
								}
								for _, field := range requestFields {
									if val, exists := origTaskCtx[field]; exists {
										if _, alreadySet := resultTaskCtx[field]; !alreadySet {
											resultTaskCtx[field] = val
										}
									}
								}
								result.Metadata["task_context"] = resultTaskCtx
							}
						}
					}
				}
			}

			taskExecution.Metadata = db.JSONB(result.Metadata)
		}

		// If model/provider missing, derive dominant from token_usage
		// 模型/provider 缺失时，从 token_usage 推导占比最大的那个
		if s.dbClient != nil {
			if taskExecution.ModelUsed == "" || taskExecution.Provider == "" || strings.EqualFold(taskExecution.Provider, "unknown") {
				var topModel sql.NullString
				var topProvider sql.NullString
				row := s.dbClient.Wrapper().QueryRowContext(ctx, `
                        SELECT COALESCE(model, '') AS model, COALESCE(provider, '') AS provider
                        FROM (
                            SELECT tu.model, tu.provider, SUM(tu.total_tokens) AS tt
                            FROM token_usage tu
                            JOIN task_executions te ON tu.task_id = te.id
                            WHERE te.workflow_id = $1
                            GROUP BY tu.model, tu.provider
                            ORDER BY tt DESC
                            LIMIT 1
                        ) t`, workflowID)
				// Guard against nil row from circuit breaker
				// 防止 circuit breaker 返回 nil 行
				if row != nil {
					if err := row.Scan(&topModel, &topProvider); err == nil {
						if taskExecution.ModelUsed == "" && topModel.Valid && topModel.String != "" {
							taskExecution.ModelUsed = topModel.String
						}
						if (taskExecution.Provider == "" || strings.EqualFold(taskExecution.Provider, "unknown")) && topProvider.Valid && topProvider.String != "" {
							taskExecution.Provider = topProvider.String
						}
						// Fallback: detect provider from model when still empty
						// 仍为空时，从模型探测 provider 作为兜底
						if taskExecution.Provider == "" && taskExecution.ModelUsed != "" {
							taskExecution.Provider = detectProviderFromModel(taskExecution.ModelUsed)
						}
					}
				}
			}

			// Enrich agent_usages in metadata from token_usage if missing or zero
			// Build per-agent summary (agent_id, model, provider, input/output/total, cost)
			// 缺失或为零时，用 token_usage 补全 metadata 中的 agent_usages
			// 构建按 agent 的摘要（agent_id、model、provider、input/output/total、cost）
			type agentUsageRow struct {
				AgentID               sql.NullString
				Model                 sql.NullString
				Provider              sql.NullString
				InputTokens           sql.NullInt64
				OutputTokens          sql.NullInt64
				TotalTokens           sql.NullInt64
				CacheAwareTotalTokens sql.NullInt64
				CostUSD               sql.NullFloat64
			}

			needAgentUsages := true
			if result.Metadata != nil {
				if au, ok := result.Metadata["agent_usages"]; ok {
					// If existing usages contain any non-zero totals, keep them
					// 已有 usages 含非零总数时保留它们
					if list, ok2 := au.([]map[string]any); ok2 {
						for _, m := range list {
							if v, ok := m["total_tokens"]; ok {
								switch t := v.(type) {
								case int:
									if t > 0 {
										needAgentUsages = false
									}
								case float64:
									if int(t) > 0 {
										needAgentUsages = false
									}
								}
							}
							if !needAgentUsages {
								break
							}
						}
					}
				}
			}

			if needAgentUsages {
				rows, err := s.dbClient.Wrapper().QueryContext(ctx, `
                        SELECT COALESCE(agent_id, '') AS agent_id,
                               COALESCE(model, '')     AS model,
                               COALESCE(provider, '')  AS provider,
                               COALESCE(SUM(prompt_tokens), 0)             AS input_tokens,
                               COALESCE(SUM(completion_tokens), 0)         AS output_tokens,
                               COALESCE(SUM(total_tokens), 0)              AS total_tokens,
                               COALESCE(SUM(cache_aware_total_tokens), 0)  AS cache_aware_total_tokens,
                               COALESCE(SUM(cost_usd), 0)                  AS cost_usd
                        FROM token_usage tu
                        JOIN task_executions te ON tu.task_id = te.id
                        WHERE te.workflow_id = $1
                        GROUP BY agent_id, model, provider
                        ORDER BY total_tokens DESC`, workflowID)
				if err == nil {
					defer rows.Close()
					var usages []map[string]any
					for rows.Next() {
						var r agentUsageRow
						if scanErr := rows.Scan(&r.AgentID, &r.Model, &r.Provider, &r.InputTokens, &r.OutputTokens, &r.TotalTokens, &r.CacheAwareTotalTokens, &r.CostUSD); scanErr != nil {
							continue
						}
						usage := map[string]any{}
						if r.AgentID.Valid {
							usage["agent_id"] = r.AgentID.String
						}
						if r.Model.Valid && r.Model.String != "" {
							usage["model"] = r.Model.String
						}
						if r.InputTokens.Valid {
							usage["input_tokens"] = int(r.InputTokens.Int64)
						}
						if r.OutputTokens.Valid {
							usage["output_tokens"] = int(r.OutputTokens.Int64)
						}
						if r.TotalTokens.Valid {
							usage["total_tokens"] = int(r.TotalTokens.Int64)
						}
						if r.CacheAwareTotalTokens.Valid {
							usage["cache_aware_total_tokens"] = int(r.CacheAwareTotalTokens.Int64)
						}
						if r.CostUSD.Valid {
							usage["cost_usd"] = r.CostUSD.Float64
						}
						usages = append(usages, usage)
					}
					if len(usages) > 0 {
						if result.Metadata == nil {
							result.Metadata = make(map[string]any)
						}
						result.Metadata["agent_usages"] = usages
						taskExecution.Metadata = db.JSONB(result.Metadata)
					}
				} else {
					s.logger.Debug("agent_usages enrichment query failed", zap.Error(err))
				}
			}
		}

		// Always aggregate from token_usage as the source of truth for all workflow phases
		// 始终以 token_usage 为准汇总，覆盖 workflow 的所有阶段
		if s.dbClient != nil {
			var aggCost sql.NullFloat64
			var aggTotalTokens sql.NullInt64
			var aggPromptTokens sql.NullInt64
			var aggCompletionTokens sql.NullInt64
			var aggCacheRead sql.NullInt64
			var aggCacheCreation sql.NullInt64
			var aggCacheAware sql.NullInt64
			row := s.dbClient.Wrapper().QueryRowContext(ctx, `
                SELECT
                    COALESCE(SUM(tu.cost_usd), 0),
                    COALESCE(SUM(tu.total_tokens), 0),
                    COALESCE(SUM(tu.prompt_tokens), 0),
                    COALESCE(SUM(tu.completion_tokens), 0),
                    COALESCE(SUM(tu.cache_read_tokens), 0),
                    COALESCE(SUM(tu.cache_creation_tokens), 0),
                    COALESCE(SUM(tu.cache_aware_total_tokens), 0)
                FROM token_usage tu
                JOIN task_executions te ON tu.task_id = te.id
                WHERE te.workflow_id = $1`, workflowID)
			// Guard against nil row from circuit breaker
			// 防止 circuit breaker 返回 nil 行
			if row != nil {
				if err := row.Scan(&aggCost, &aggTotalTokens, &aggPromptTokens, &aggCompletionTokens,
					&aggCacheRead, &aggCacheCreation, &aggCacheAware); err == nil {
					// Only overwrite with DB aggregates when non-zero (preserves workflow metadata when token_usage rows don't exist yet)
					// 仅在非零时用 DB 聚合值覆盖（token_usage 尚无记录时保留 workflow metadata）
					s.logger.Info("Token usage aggregation succeeded",
						zap.String("workflow_id", workflowID),
						zap.Float64("cost", aggCost.Float64),
						zap.Int64("total_tokens", aggTotalTokens.Int64),
						zap.Int64("prompt_tokens", aggPromptTokens.Int64),
						zap.Int64("completion_tokens", aggCompletionTokens.Int64),
						zap.Int64("cache_read_tokens", aggCacheRead.Int64),
						zap.Int64("cache_creation_tokens", aggCacheCreation.Int64),
						zap.Int64("cache_aware_total_tokens", aggCacheAware.Int64))
					if aggCost.Valid && aggCost.Float64 > 0 {
						taskExecution.TotalCostUSD = aggCost.Float64
						dbTotalCost = aggCost.Float64
					}
					if aggTotalTokens.Valid && aggTotalTokens.Int64 > 0 {
						taskExecution.TotalTokens = int(aggTotalTokens.Int64)
						dbTotalTokens = int(aggTotalTokens.Int64)
					}
					if aggPromptTokens.Valid && aggPromptTokens.Int64 > 0 {
						taskExecution.PromptTokens = int(aggPromptTokens.Int64)
						dbPromptTokens = int(aggPromptTokens.Int64)
					}
					if aggCompletionTokens.Valid && aggCompletionTokens.Int64 > 0 {
						taskExecution.CompletionTokens = int(aggCompletionTokens.Int64)
						dbCompletionTokens = int(aggCompletionTokens.Int64)
					}
					if aggCacheRead.Valid && aggCacheRead.Int64 > 0 {
						taskExecution.CacheReadTokens = int(aggCacheRead.Int64)
					}
					if aggCacheCreation.Valid && aggCacheCreation.Int64 > 0 {
						taskExecution.CacheCreationTokens = int(aggCacheCreation.Int64)
					}
					if aggCacheAware.Valid && aggCacheAware.Int64 > 0 {
						taskExecution.CacheAwareTotalTokens = int(aggCacheAware.Int64)
					} else {
						// Defensive fallback if migration 121 has not yet been applied
						// or the row predates it: recompute from the SUM parts.
						// 防御性兜底：迁移 121 尚未应用
						// 或记录早于该迁移时，用各分项之和重新计算。
						taskExecution.CacheAwareTotalTokens = taskExecution.TotalTokens +
							taskExecution.CacheReadTokens + taskExecution.CacheCreationTokens
					}
					// Mark that we have DB metrics available
					// 标记已有可用的 DB 指标
					if dbTotalTokens > 0 {
						hasDBMetrics = true
					}
				} else {
					s.logger.Warn("Token usage aggregation failed",
						zap.String("workflow_id", workflowID),
						zap.Error(err))
				}
			}

			// Secondary fallback: derive from agent_executions only if token_usage aggregation returned zero
			// 次级兜底：仅当 token_usage 聚合结果为零时才从 agent_executions 推导
			if taskExecution.TotalTokens == 0 {
				var aeTokens sql.NullInt64
				row2 := s.dbClient.Wrapper().QueryRowContext(ctx, `
                    SELECT COALESCE(SUM(ae.tokens_used), 0)
                    FROM agent_executions ae
                    WHERE ae.workflow_id = $1`, workflowID)
				// Guard against nil row from circuit breaker
				// 防止 circuit breaker 返回 nil 行
				if row2 != nil {
					if err2 := row2.Scan(&aeTokens); err2 == nil {
						if aeTokens.Valid && aeTokens.Int64 > 0 {
							taskExecution.TotalTokens = int(aeTokens.Int64)
							// Compute cost using model from metadata when available
							// 有 metadata 中的模型时按其计算成本
							taskExecution.TotalCostUSD = calculateTokenCost(taskExecution.TotalTokens, result.Metadata)
						}
					} else {
						s.logger.Warn("Agent execution aggregation failed",
							zap.String("workflow_id", workflowID),
							zap.Error(err2))
					}
				}
			}
		}

		// Build unified response and store in response JSONB column
		// 构建统一 response 并存入 response JSONB 列
		if result.Result != "" {
			// Compute execution time from task duration
			// 按 task 耗时计算执行时间
			var execTimeMs int64
			if taskExecution.DurationMs != nil {
				execTimeMs = int64(*taskExecution.DurationMs)
			}

			// Ensure task_id is set in metadata so unified response includes it
			// 确保 metadata 中设置 task_id，使统一 response 包含它
			if result.Metadata == nil {
				result.Metadata = make(map[string]any)
			}
			if _, ok := result.Metadata["task_id"]; !ok {
				result.Metadata["task_id"] = workflowID
			}

			// Update result.Metadata with DB aggregates before building unified response
			// 构建统一 response 之前，用 DB 聚合值更新 result.Metadata
			if hasDBMetrics {
				if result.Metadata == nil {
					result.Metadata = make(map[string]any)
				}
				if dbTotalTokens > 0 {
					result.Metadata["total_tokens"] = dbTotalTokens
				}
				if dbPromptTokens > 0 {
					result.Metadata["input_tokens"] = dbPromptTokens
				}
				if dbCompletionTokens > 0 {
					result.Metadata["output_tokens"] = dbCompletionTokens
				}
				if dbTotalCost > 0 {
					result.Metadata["cost_usd"] = dbTotalCost
				}
				result.TokensUsed = dbTotalTokens
			}

			// Ensure cache tokens from DB aggregation are reflected in metadata
			// (DB aggregation is source of truth when available)
			// 确保 DB 聚合得到的 cache token 反映到 metadata 中
			// （有 DB 聚合时以其为准）
			if taskExecution.CacheReadTokens > 0 {
				result.Metadata["cache_read_tokens"] = taskExecution.CacheReadTokens
			}
			if taskExecution.CacheCreationTokens > 0 {
				result.Metadata["cache_creation_tokens"] = taskExecution.CacheCreationTokens
			}

			// Compute cache savings using per-model pricing (accurate calculation)
			// 按各模型定价计算 cache 节省额（精确计算）
			if taskExecution.CacheReadTokens > 0 || taskExecution.CacheCreationTokens > 0 {
				if s.dbClient != nil {
					rows, qErr := s.dbClient.Wrapper().QueryContext(ctx, `
						SELECT model, provider,
						       SUM(prompt_tokens), SUM(completion_tokens),
						       SUM(cache_read_tokens), SUM(cache_creation_tokens), SUM(cost_usd)
						FROM token_usage
						WHERE task_id = (SELECT id FROM task_executions WHERE workflow_id = $1)
						  AND (cache_read_tokens > 0 OR cache_creation_tokens > 0)
						GROUP BY model, provider`, workflowID)
					if qErr == nil && rows != nil {
						var totalSavings float64
						for rows.Next() {
							var model, provider string
							var input, output, cr, cc int
							var cost float64
							if err := rows.Scan(&model, &provider, &input, &output, &cr, &cc, &cost); err == nil {
								// Anthropic/MiniMax: input excludes cache tokens, reconstruct by adding them back.
								// OpenAI/xAI/Kimi: input already includes cache tokens; use as-is.
								// Anthropic/MiniMax：input 不含 cache token，需加回来还原。
								// OpenAI/xAI/Kimi：input 已含 cache token，直接使用。
								var costWithout float64
								if provider == "anthropic" || provider == "minimax" {
									costWithout = pricing.CostForSplit(model, input+cr+cc, output)
								} else {
									costWithout = pricing.CostForSplit(model, input, output)
								}
								if costWithout > cost {
									totalSavings += costWithout - cost
								}
							}
						}
						if err := rows.Err(); err != nil {
							s.logger.Error("cache savings query: row iteration error", zap.Error(err))
						}
						rows.Close()
						if totalSavings > 0 {
							result.Metadata["cache_savings_usd"] = totalSavings
						}
					}
				}
			}

			unifiedResp := TransformToUnifiedResponse(result, sessionID, execTimeMs)
			taskExecution.Response = unifiedRespToJSONB(unifiedResp)
		}

		// Queue async write to database
		// 把异步写入排入队列
		err := s.dbClient.QueueWrite(db.WriteTypeTaskExecution, taskExecution, func(err error) {
			if err != nil {
				s.logger.Error("Failed to persist task execution",
					zap.String("workflow_id", workflowID),
					zap.Error(err))
			} else {
				s.logger.Debug("Task execution persisted",
					zap.String("workflow_id", workflowID),
					zap.String("status", statusStr))
			}
		})

		if err != nil {
			s.logger.Warn("Failed to queue task execution write",
				zap.String("workflow_id", workflowID),
				zap.Error(err))
		}

		// Record token usage for enterprise quota tracking (fire-and-forget).
		// Gate on CacheAwareTotalTokens so cache-only completions (which can
		// have TotalTokens == 0 yet nonzero cache tokens) still register.
		// 记录 token 用量供企业版配额统计（发后不等待）。
		// 以 CacheAwareTotalTokens 为门槛，使仅有 cache 的完成（TotalTokens 可能为 0
		// 但 cache token 非零）也能登记。
		if tenantUUID != nil && taskExecution.CacheAwareTotalTokens > 0 && statusStr == "COMPLETED" {
			s.RecordUsage(ctx, *tenantUUID, workflowID, RecordUsageDetails{
				InputTokens:           int64(taskExecution.PromptTokens),
				OutputTokens:          int64(taskExecution.CompletionTokens),
				CacheReadTokens:       int64(taskExecution.CacheReadTokens),
				CacheCreationTokens:   int64(taskExecution.CacheCreationTokens),
				CacheAwareTotalTokens: int64(taskExecution.CacheAwareTotalTokens),
				Model:                 taskExecution.ModelUsed,
				Provider:              taskExecution.Provider,
			})
		}
	}

	// Build metrics if we have a completed result or metadata
	// 有完成结果或 metadata 时构建指标
	var metrics *common.ExecutionMetrics
	// For terminal workflows, use DB-aggregated totals; for running workflows, use result metadata
	// 终态 workflow 使用 DB 聚合总数；运行中的 workflow 使用结果 metadata
	if hasDBMetrics || result.TokensUsed > 0 || result.Metadata != nil {
		totalTokens := result.TokensUsed
		totalCost := calculateTokenCost(result.TokensUsed, result.Metadata)

		// Override with DB aggregates for terminal workflows
		// 终态 workflow 用 DB 聚合值覆盖
		if hasDBMetrics {
			totalTokens = dbTotalTokens
			totalCost = dbTotalCost
		}

		metrics = &common.ExecutionMetrics{
			TokenUsage: &common.TokenUsage{
				TotalTokens: int32(totalTokens),
				CostUsd:     totalCost,
			},
		}

		// Populate prompt/completion breakdown for terminal workflows from DB
		// 终态 workflow 的 prompt/completion 明细由 DB 填充
		if hasDBMetrics {
			if dbPromptTokens > 0 {
				metrics.TokenUsage.PromptTokens = int32(dbPromptTokens)
			}
			if dbCompletionTokens > 0 {
				metrics.TokenUsage.CompletionTokens = int32(dbCompletionTokens)
			}
		}

		// Extract metadata values if available
		// 可用时提取 metadata 字段值
		if result.Metadata != nil {
			// Populate model/provider into metrics when available
			// 有模型/provider 时填入指标
			if m, ok := result.Metadata["model"].(string); ok && m != "" {
				metrics.TokenUsage.Model = m
			} else if mu, ok := result.Metadata["model_used"].(string); ok && mu != "" {
				metrics.TokenUsage.Model = mu
			}
			if p, ok := result.Metadata["provider"].(string); ok && p != "" {
				metrics.TokenUsage.Provider = p
			}
			// Get execution mode (using configurable thresholds)
			// 获取执行 mode（使用可配置阈值）
			if complexity, ok := result.Metadata["complexity_score"].(float64); ok {
				simpleThreshold := 0.3 // default 注：默认值
				mediumThreshold := 0.5 // default 注：默认值
				if s.workflowConfig != nil {
					if s.workflowConfig.ComplexitySimpleThreshold > 0 {
						simpleThreshold = s.workflowConfig.ComplexitySimpleThreshold
					}
					if s.workflowConfig.ComplexityMediumThreshold > 0 {
						mediumThreshold = s.workflowConfig.ComplexityMediumThreshold
					}
				}

				if complexity < simpleThreshold {
					metrics.Mode = common.ExecutionMode_EXECUTION_MODE_SIMPLE
				} else if complexity < mediumThreshold {
					metrics.Mode = common.ExecutionMode_EXECUTION_MODE_STANDARD
				} else {
					metrics.Mode = common.ExecutionMode_EXECUTION_MODE_COMPLEX
				}
			} else {
				metrics.Mode = common.ExecutionMode_EXECUTION_MODE_STANDARD
			}

			// Get agent count
			// 获取 agent 数量
			if agentsUsed, ok := result.Metadata["num_agents"].(int); ok {
				metrics.AgentsUsed = int32(agentsUsed)
			}

			// Get cache metrics if available
			// 可用时获取 cache 指标
			if cacheHit, ok := result.Metadata["cache_hit"].(bool); ok {
				metrics.CacheHit = cacheHit
			}
			if cacheScore, ok := result.Metadata["cache_score"].(float64); ok {
				metrics.CacheScore = cacheScore
			}
		}
	}

	// Compute duration for metrics and unified response
	// 为指标与统一 response 计算耗时
	durationSeconds := 0.0
	if isTerminal && workflowStartTime != nil {
		endTime := getWorkflowEndTime(desc)
		durationSeconds = endTime.Sub(workflowStartTime.AsTime()).Seconds()
	}

	// Record completed workflow metrics if terminal
	// 若为终态则记录已完成 workflow 的指标
	if isTerminal {
		// Derive mode string for labels
		// 推导用于 label 的 mode 字符串
		modeStr := "standard"
		if metrics != nil {
			switch metrics.Mode {
			case common.ExecutionMode_EXECUTION_MODE_SIMPLE:
				modeStr = "simple"
			case common.ExecutionMode_EXECUTION_MODE_COMPLEX:
				modeStr = "complex"
			default:
				modeStr = "standard"
			}
		}
		// Cost
		// 成本
		cost := 0.0
		if metrics != nil && metrics.TokenUsage != nil {
			cost = metrics.TokenUsage.CostUsd
		}
		ometrics.RecordWorkflowMetrics("AgentDAGWorkflow", modeStr, statusStr, durationSeconds, result.TokensUsed, cost)
	}

	response := &pb.GetTaskStatusResponse{
		TaskId:   req.TaskId,
		Status:   statusOut,
		Progress: 0,
		Result:   result.Result,
		Metrics:  metrics,
	}
	return response, nil
}

// authorizeTaskAccess resolves the caller and verifies workflow-memo ownership.
// authorizeTaskAccess 解析调用方并校验 workflow memo 的归属。
func (s *OrchestratorService) authorizeTaskAccess(ctx context.Context, taskID, runID string) (*auth.UserContext, *workflowservice.DescribeWorkflowExecutionResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	desc, dErr := s.temporalClient.DescribeWorkflowExecution(ctx, taskID, runID)
	if dErr != nil || desc == nil || desc.WorkflowExecutionInfo == nil {
		return nil, nil, status.Error(codes.NotFound, "task not found")
	}
	if memo := desc.WorkflowExecutionInfo.Memo; memo != nil {
		dc := converter.GetDefaultDataConverter()
		if f := memo.Fields["tenant_id"]; f != nil {
			var memoTenant string
			_ = dc.FromPayload(f, &memoTenant)
			if memoTenant != "" && uc.TenantID.String() != memoTenant {
				return nil, nil, status.Error(codes.NotFound, "task not found")
			}
		}
		if f := memo.Fields["user_id"]; f != nil {
			var memoUser string
			_ = dc.FromPayload(f, &memoUser)
			if memoUser != "" && uc.UserID.String() != memoUser {
				return nil, nil, status.Error(codes.NotFound, "task not found")
			}
		}
	}
	return uc, desc, nil
}

// CancelTask cancels a running task
// CancelTask 取消运行中的 task
func (s *OrchestratorService) CancelTask(ctx context.Context, req *pb.CancelTaskRequest) (*pb.CancelTaskResponse, error) {
	s.logger.Info("Received CancelTask request",
		zap.String("task_id", req.TaskId),
		zap.String("reason", req.Reason),
	)

	uc, _, err := s.authorizeTaskAccess(ctx, req.TaskId, "")
	if err != nil {
		return nil, err
	}

	// Send graceful cancel signal first (allows checkpoint cleanup and SSE emission)
	// 先发送优雅取消信号（便于 checkpoint 清理与发出 SSE event）
	cancelReq := workflows.CancelRequest{
		Reason:      req.Reason,
		RequestedBy: uc.UserID.String(),
	}
	// Fire-and-forget signal - don't fail cancel if signal fails
	// 发后不等待——信号失败不应导致取消失败
	_ = s.temporalClient.SignalWorkflow(ctx, req.TaskId, "", workflows.SignalCancel, cancelReq)

	// Brief delay to allow signal processing before Temporal cancellation
	// This ensures the workflow's signal handler can update state and emit SSE events
	// 短暂延迟，让信号先被处理再执行 Temporal 取消
	// 这可确保 workflow 的信号处理器能更新状态并发出 SSE event
	time.Sleep(100 * time.Millisecond)

	// Then request Temporal cancellation (fallback for stuck activities)
	// 随后请求 Temporal 取消（作为卡住的 activity 的兜底）
	if err := s.temporalClient.CancelWorkflow(ctx, req.TaskId, ""); err != nil {
		s.logger.Error("Failed to cancel workflow", zap.Error(err))
		return &pb.CancelTaskResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to cancel task: %v", err),
		}, nil
	}

	return &pb.CancelTaskResponse{
		Success: true,
		Message: "Task cancelled successfully",
	}, nil
}

// PauseTask pauses a running workflow (with ownership check)
// PauseTask 暂停运行中的 workflow（含归属校验）
func (s *OrchestratorService) PauseTask(ctx context.Context, req *pb.PauseTaskRequest) (*pb.PauseTaskResponse, error) {
	s.logger.Info("Received PauseTask request",
		zap.String("task_id", req.TaskId),
		zap.String("reason", req.Reason),
	)

	uc, desc, err := s.authorizeTaskAccess(ctx, req.TaskId, "")
	if err != nil {
		return nil, err
	}

	// Validate workflow state - cannot pause completed/failed/cancelled workflows
	// 校验 workflow 状态——已完成/失败/已取消的 workflow 不能暂停
	switch desc.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return nil, status.Error(codes.FailedPrecondition, "cannot pause completed task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return nil, status.Error(codes.FailedPrecondition, "cannot pause failed task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return nil, status.Error(codes.FailedPrecondition, "cannot pause cancelled task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return nil, status.Error(codes.FailedPrecondition, "cannot pause timed out task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		// Check if already paused
		// 检查是否已经暂停
		if ctrlResp, qErr := s.temporalClient.QueryWorkflow(ctx, req.TaskId, "", workflows.QueryControlState); qErr == nil {
			var ctrlState workflows.WorkflowControlState
			if ctrlResp.Get(&ctrlState) == nil && ctrlState.IsPaused {
				return nil, status.Error(codes.FailedPrecondition, "task is already paused")
			}
		}
	}

	// Send pause signal to Temporal
	// 向 Temporal 发送 pause signal
	pauseReq := workflows.PauseRequest{
		Reason:      req.Reason,
		RequestedBy: uc.UserID.String(),
	}
	if err := s.temporalClient.SignalWorkflow(ctx, req.TaskId, "", workflows.SignalPause, pauseReq); err != nil {
		s.logger.Error("Failed to send pause signal", zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to pause: %v", err))
	}

	// Update task status in DB
	// 更新 DB 中的 task 状态
	if s.dbClient != nil {
		_ = s.dbClient.UpdateTaskStatus(ctx, req.TaskId, "PAUSED")
	}

	s.logger.Info("Task paused",
		zap.String("task_id", req.TaskId),
		zap.String("user_id", uc.UserID.String()),
	)

	return &pb.PauseTaskResponse{
		Success: true,
		Message: "Pause signal sent",
	}, nil
}

// ResumeTask resumes a paused workflow (with ownership check)
// ResumeTask 恢复已暂停的 workflow（含归属校验）
func (s *OrchestratorService) ResumeTask(ctx context.Context, req *pb.ResumeTaskRequest) (*pb.ResumeTaskResponse, error) {
	s.logger.Info("Received ResumeTask request",
		zap.String("task_id", req.TaskId),
		zap.String("reason", req.Reason),
	)

	uc, desc, err := s.authorizeTaskAccess(ctx, req.TaskId, "")
	if err != nil {
		return nil, err
	}

	// Validate workflow state - cannot resume completed/failed/cancelled workflows
	// 校验 workflow 状态——已完成/失败/已取消的 workflow 不能恢复
	switch desc.WorkflowExecutionInfo.Status {
	case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
		return nil, status.Error(codes.FailedPrecondition, "cannot resume completed task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
		return nil, status.Error(codes.FailedPrecondition, "cannot resume failed task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
		return nil, status.Error(codes.FailedPrecondition, "cannot resume cancelled task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
		return nil, status.Error(codes.FailedPrecondition, "cannot resume timed out task")
	case enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING:
		// Check if task is actually paused
		// 检查 task 是否确实处于暂停状态
		isPaused := false
		if ctrlResp, qErr := s.temporalClient.QueryWorkflow(ctx, req.TaskId, "", workflows.QueryControlState); qErr == nil {
			var ctrlState workflows.WorkflowControlState
			if ctrlResp.Get(&ctrlState) == nil {
				isPaused = ctrlState.IsPaused
			}
		}
		if !isPaused {
			return nil, status.Error(codes.FailedPrecondition, "task is not paused")
		}
	}

	resumeReq := workflows.ResumeRequest{
		Reason:      req.Reason,
		RequestedBy: uc.UserID.String(),
	}
	if err := s.temporalClient.SignalWorkflow(ctx, req.TaskId, "", workflows.SignalResume, resumeReq); err != nil {
		s.logger.Error("Failed to send resume signal", zap.Error(err))
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to resume: %v", err))
	}

	// Update task status back to RUNNING
	// 把 task 状态改回 RUNNING
	if s.dbClient != nil {
		_ = s.dbClient.UpdateTaskStatus(ctx, req.TaskId, "RUNNING")
	}

	s.logger.Info("Task resumed",
		zap.String("task_id", req.TaskId),
		zap.String("user_id", uc.UserID.String()),
	)

	return &pb.ResumeTaskResponse{
		Success: true,
		Message: "Resume signal sent",
	}, nil
}

// GetControlState queries workflow control state (with ownership check)
// GetControlState 查询 workflow 控制状态（含归属校验）
func (s *OrchestratorService) GetControlState(ctx context.Context, req *pb.GetControlStateRequest) (*pb.GetControlStateResponse, error) {
	_, _, err := s.authorizeTaskAccess(ctx, req.TaskId, "")
	if err != nil {
		return nil, err
	}

	// Query Temporal workflow for control state
	// 查询 Temporal workflow 的 control state
	resp, err := s.temporalClient.QueryWorkflow(ctx, req.TaskId, "", workflows.QueryControlState)
	if err != nil {
		// Workflow may have completed - return default state
		// workflow 可能已完成——返回默认状态
		return &pb.GetControlStateResponse{
			IsPaused:    false,
			IsCancelled: false,
			PausedAt:    "", // Empty string for never-paused 注：从未暂停时为空字符串
		}, nil
	}

	var state workflows.WorkflowControlState
	if err := resp.Get(&state); err != nil {
		return nil, status.Error(codes.Internal, "failed to decode state")
	}

	// Handle zero time gracefully - return empty string instead of epoch
	// 优雅处理零值时间——返回空字符串而非 epoch
	pausedAtStr := ""
	if !state.PausedAt.IsZero() {
		pausedAtStr = state.PausedAt.Format(time.RFC3339)
	}

	return &pb.GetControlStateResponse{
		IsPaused:     state.IsPaused,
		IsCancelled:  state.IsCancelled,
		PausedAt:     pausedAtStr,
		PauseReason:  state.PauseReason,
		PausedBy:     state.PausedBy,
		CancelReason: state.CancelReason,
		CancelledBy:  state.CancelledBy,
	}, nil
}

// ListTasks lists tasks for a user/session
// ListTasks 列出用户/session 的 task
func (s *OrchestratorService) ListTasks(ctx context.Context, req *pb.ListTasksRequest) (*pb.ListTasksResponse, error) {
	if s.dbClient == nil {
		return &pb.ListTasksResponse{Tasks: []*pb.TaskSummary{}, TotalCount: 0}, nil
	}

	// Enforce tenant scoping when available
	// 可用时强制 tenant 范围
	var tenantFilter *uuid.UUID
	if uc, err := auth.GetUserContext(ctx); err == nil && uc != nil {
		t := uc.TenantID
		tenantFilter = &t
	}

	// Build filters
	// 构建过滤条件
	where := []string{"1=1"}
	args := []any{}
	ai := 1

	if tenantFilter != nil {
		where = append(where, fmt.Sprintf("tenant_id = $%d", ai))
		args = append(args, *tenantFilter)
		ai++
	}

	// Filter by user_id if provided
	// 若提供 user_id 则按其过滤
	if req.UserId != "" {
		if uid, err := uuid.Parse(req.UserId); err == nil {
			where = append(where, fmt.Sprintf("(user_id = $%d OR user_id IS NULL)", ai))
			args = append(args, uid)
			ai++
		}
	}
	// Filter by session_id if provided (task_executions.session_id is VARCHAR)
	// 提供 session_id 时按其过滤（task_executions.session_id 为 VARCHAR）
	if req.SessionId != "" {
		where = append(where, fmt.Sprintf("session_id = $%d", ai))
		args = append(args, req.SessionId)
		ai++
	}
	// Filter by status if provided
	// 若提供 status 则按其过滤
	if req.FilterStatus != pb.TaskStatus_TASK_STATUS_UNSPECIFIED {
		statusStr := mapProtoStatusToDB(req.FilterStatus)
		if statusStr != "" {
			where = append(where, fmt.Sprintf("UPPER(status) = UPPER($%d)", ai))
			args = append(args, statusStr)
			ai++
		}
	}

	// Pagination
	// 分页
	limit := int(req.Limit)
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	offset := max(int(req.Offset), 0)

	// Total count query
	// 总数统计查询
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM task_executions WHERE %s", strings.Join(where, " AND "))
	var total int32
	if err := s.dbClient.Wrapper().QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		s.logger.Warn("ListTasks count failed", zap.Error(err))
		total = 0
	}

	// Data query
	// 数据查询
	dataQuery := fmt.Sprintf(`
        SELECT workflow_id, query, status, mode,
               started_at, completed_at, created_at,
               total_tokens,
               total_cost_usd
        FROM task_executions
        WHERE %s
        ORDER BY COALESCE(started_at, created_at) DESC
        LIMIT %d OFFSET %d`, strings.Join(where, " AND "), limit, offset)

	rows, err := s.dbClient.Wrapper().QueryContext(ctx, dataQuery, args...)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to list tasks: %v", err))
	}
	defer rows.Close()

	summaries := make([]*pb.TaskSummary, 0, limit)
	for rows.Next() {
		summary, err := scanTaskSummary(rows)
		if err != nil {
			return nil, status.Error(codes.Internal, fmt.Sprintf("failed to scan row: %v", err))
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to iterate rows: %v", err))
	}

	return &pb.ListTasksResponse{
		Tasks:      summaries,
		TotalCount: total,
	}, nil
}

// GetSessionContext gets session context
// GetSessionContext 获取 session context
func (s *OrchestratorService) GetSessionContext(ctx context.Context, req *pb.GetSessionContextRequest) (*pb.GetSessionContextResponse, error) {
	s.logger.Info("GetSessionContext called", zap.String("session_id", req.SessionId))

	if req.SessionId == "" {
		return nil, status.Error(codes.InvalidArgument, "session_id is required")
	}

	var tenantFilter *uuid.UUID
	if uc, err := auth.GetUserContext(ctx); err == nil && uc != nil {
		t := uc.TenantID
		tenantFilter = &t
	}

	// Get session from manager
	// 从 manager 获取 session
	sess, err := s.sessionManager.GetSession(ctx, req.SessionId)
	if err != nil {
		if err == session.ErrSessionNotFound {
			return nil, status.Error(codes.NotFound, "session not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to get session: %v", err))
	}

	// Build response with session data
	// 用 session 数据构建 response
	response := &pb.GetSessionContextResponse{
		SessionId: req.SessionId,
	}

	// Add session token usage
	// 附加 session token 用量
	if sess.TotalTokensUsed > 0 {
		response.SessionTokenUsage = &common.TokenUsage{
			TotalTokens: int32(sess.TotalTokensUsed),
		}
	}

	// Add session context as Struct
	// 以 Struct 形式附加 session context
	if sess.Context != nil {
		contextStruct, err := structpb.NewStruct(sess.Context)
		if err == nil {
			response.Context = contextStruct
		}
	}

	if s.dbClient != nil {
		// Resolve both canonical UUID and external_id for dual-format session IDs
		// 同时解析规范 UUID 与 external_id，以支持双格式 session ID
		sessionIDs := []string{}
		var dbID string
		var extID sql.NullString
		row := s.dbClient.Wrapper().QueryRowContext(ctx, `
            SELECT id::text, context->>'external_id'
            FROM sessions
            WHERE (id::text = $1 OR context->>'external_id' = $1) AND deleted_at IS NULL
        `, req.SessionId)
		if err := row.Scan(&dbID, &extID); err == nil {
			sessionIDs = append(sessionIDs, dbID)
			if extID.Valid && extID.String != "" {
				sessionIDs = append(sessionIDs, extID.String)
			}
			// Best-effort tenant hint from persisted session
			// 尽力从已持久化的 session 取得租户提示
			if tenantFilter == nil {
				var sidTenant sql.NullString
				if err := s.dbClient.Wrapper().QueryRowContext(ctx,
					`SELECT tenant_id::text FROM sessions WHERE id::text = $1 OR context->>'external_id' = $1 LIMIT 1`,
					req.SessionId,
				).Scan(&sidTenant); err == nil && sidTenant.Valid {
					if tid, err := uuid.Parse(sidTenant.String); err == nil {
						tenantFilter = &tid
					}
				}
			}
		} else {
			// Fallback to the provided session ID if not resolvable in DB
			// DB 中无法解析时回退到传入的 session ID
			sessionIDs = append(sessionIDs, req.SessionId)
		}

		tasks, err := s.loadRecentSessionTasksByIDs(ctx, sessionIDs, 5, tenantFilter)
		if err != nil {
			s.logger.Warn("Failed to load recent session tasks",
				zap.String("session_id", req.SessionId),
				zap.Error(err))
		} else if len(tasks) > 0 {
			response.RecentTasks = tasks
		}
	}

	return response, nil
}

func (s *OrchestratorService) loadRecentSessionTasks(ctx context.Context, sessionID string, limit int, tenantID *uuid.UUID) ([]*pb.TaskSummary, error) {
	if sessionID == "" || limit <= 0 || s.dbClient == nil {
		return nil, nil
	}

	where := []string{"session_id = $1"}
	args := []any{sessionID, limit}
	if tenantID != nil {
		where = append(where, "tenant_id = $3")
		args = append(args, *tenantID)
	}

	query := fmt.Sprintf(`
        SELECT workflow_id, query, status, mode,
               started_at, completed_at, created_at,
               total_tokens, total_cost_usd
        FROM task_executions
        WHERE %s
        ORDER BY COALESCE(started_at, created_at) DESC
        LIMIT $2`, strings.Join(where, " AND "))

	rows, err := s.dbClient.Wrapper().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summaries := make([]*pb.TaskSummary, 0, limit)

	for rows.Next() {
		summary, err := scanTaskSummary(rows)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return summaries, nil
}

// loadRecentSessionTasksByIDs loads recent tasks for one or two possible session IDs
// to support dual-format session identifiers (UUID and external string ID).
// loadRecentSessionTasksByIDs 加载一到两个可能 session ID 的近期 task，
// 以支持双格式 session 标识（UUID 与外部字符串 ID）。
func (s *OrchestratorService) loadRecentSessionTasksByIDs(ctx context.Context, sessionIDs []string, limit int, tenantID *uuid.UUID) ([]*pb.TaskSummary, error) {
	// Normalize inputs
	// 规范化输入
	ids := make([]string, 0, 2)
	for _, id := range sessionIDs {
		if id != "" {
			ids = append(ids, id)
			if len(ids) == 2 {
				break
			}
		}
	}
	if len(ids) == 0 || limit <= 0 || s.dbClient == nil {
		return nil, nil
	}
	if len(ids) == 1 {
		return s.loadRecentSessionTasks(ctx, ids[0], limit, tenantID)
	}

	where := []string{"(session_id = $1 OR session_id = $2)"}
	args := []any{ids[0], ids[1], limit}
	if tenantID != nil {
		where = append(where, "tenant_id = $4")
		args = append(args, *tenantID)
	}

	// Build query for two IDs
	// 为两个 ID 构建查询
	query := fmt.Sprintf(`
        SELECT workflow_id, query, status, mode,
               started_at, completed_at, created_at,
               total_tokens, total_cost_usd
        FROM task_executions
        WHERE %s
        ORDER BY COALESCE(started_at, created_at) DESC
        LIMIT $3`, strings.Join(where, " AND "))

	rows, err := s.dbClient.Wrapper().QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	summaries := make([]*pb.TaskSummary, 0, limit)

	for rows.Next() {
		summary, err := scanTaskSummary(rows)
		if err != nil {
			return nil, err
		}
		summaries = append(summaries, summary)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to iterate rows: %v", err))
	}

	return summaries, nil
}

// scanTaskSummary scans one task_executions summary row and maps it to proto.
// scanTaskSummary 扫描一行 task_executions 摘要并映射为 proto。
func scanTaskSummary(rows *sql.Rows) (*pb.TaskSummary, error) {
	var (
		workflowID                    string
		queryText, statusStr, modeStr sql.NullString
		started, completed, created   sql.NullTime
		tokens                        sql.NullInt64
		costUSD                       sql.NullFloat64
	)
	if err := rows.Scan(&workflowID, &queryText, &statusStr, &modeStr, &started, &completed, &created, &tokens, &costUSD); err != nil {
		return nil, err
	}

	summary := &pb.TaskSummary{
		TaskId: workflowID,
		Query:  queryText.String,
		Status: mapDBStatusToProto(statusStr.String),
		Mode:   mapDBModeToProto(modeStr.String),
	}
	if started.Valid {
		summary.CreatedAt = timestamppb.New(started.Time)
	} else if created.Valid {
		summary.CreatedAt = timestamppb.New(created.Time)
	}
	if completed.Valid {
		summary.CompletedAt = timestamppb.New(completed.Time)
	}
	if tokens.Valid || costUSD.Valid {
		tokenUsage := &common.TokenUsage{}
		if tokens.Valid {
			tokenUsage.TotalTokens = int32(tokens.Int64)
		}
		if costUSD.Valid {
			tokenUsage.CostUsd = costUSD.Float64
		}
		summary.TotalTokenUsage = tokenUsage
	}
	return summary, nil
}

func mapDBStatusToProto(status string) pb.TaskStatus {
	switch strings.ToUpper(status) {
	case "QUEUED", "PENDING":
		return pb.TaskStatus_TASK_STATUS_QUEUED
	case "RUNNING", "IN_PROGRESS":
		return pb.TaskStatus_TASK_STATUS_RUNNING
	case "COMPLETED", "SUCCEEDED":
		return pb.TaskStatus_TASK_STATUS_COMPLETED
	case "FAILED", "ERROR", "TERMINATED":
		return pb.TaskStatus_TASK_STATUS_FAILED
	case "CANCELLED", "CANCELED":
		return pb.TaskStatus_TASK_STATUS_CANCELLED
	case "TIMEOUT", "TIMED_OUT":
		return pb.TaskStatus_TASK_STATUS_TIMEOUT
	case "PAUSED":
		return pb.TaskStatus_TASK_STATUS_PAUSED
	default:
		return pb.TaskStatus_TASK_STATUS_UNSPECIFIED
	}
}

func mapDBModeToProto(mode string) common.ExecutionMode {
	switch strings.ToLower(mode) {
	case "simple":
		return common.ExecutionMode_EXECUTION_MODE_SIMPLE
	case "complex":
		return common.ExecutionMode_EXECUTION_MODE_COMPLEX
	case "standard":
		fallthrough
	case "":
		return common.ExecutionMode_EXECUTION_MODE_STANDARD
	default:
		return common.ExecutionMode_EXECUTION_MODE_STANDARD
	}
}

func mapProtoStatusToDB(st pb.TaskStatus) string {
	switch st {
	case pb.TaskStatus_TASK_STATUS_QUEUED:
		return "QUEUED"
	case pb.TaskStatus_TASK_STATUS_RUNNING:
		return "RUNNING"
	case pb.TaskStatus_TASK_STATUS_COMPLETED:
		return "COMPLETED"
	case pb.TaskStatus_TASK_STATUS_FAILED:
		return "FAILED"
	case pb.TaskStatus_TASK_STATUS_CANCELLED:
		return "CANCELLED"
	case pb.TaskStatus_TASK_STATUS_TIMEOUT:
		return "TIMEOUT"
	case pb.TaskStatus_TASK_STATUS_PAUSED:
		return "PAUSED"
	default:
		return ""
	}
}

// watchAndPersist waits for workflow completion and persists terminal state to DB.
// It loops until the workflow reaches a terminal state (no hard timeout) to support
// long-running workflows like SupervisorWorkflow that can run for hours or days.
// watchAndPersist 等待 workflow 完成并把终态持久化到 DB。
// 它循环直到 workflow 进入终态（不设硬 timeout），以支持
// SupervisorWorkflow 这类可能运行数小时甚至数天的 workflow。
func (s *OrchestratorService) watchAndPersist(workflowID, runID string) {
	if s.temporalClient == nil || s.dbClient == nil {
		return
	}

	// Loop until workflow reaches terminal state. Use per-iteration timeouts
	// but no overall deadline - workflows can legitimately run for hours/days.
	// 循环直到 workflow 进入终态。每次迭代设置 timeout，
	// 但不设总期限——workflow 合法地可能运行数小时/数天。
	const pollInterval = 5 * time.Minute
	const maxConsecutiveErrors = 12 // Give up after ~1 hour of consecutive errors 注：连续出错约 1 小时后放弃

	consecutiveErrors := 0
	for {
		// Per-iteration timeout for the wait call
		// 单次等待调用的 timeout
		waitCtx, waitCancel := context.WithTimeout(context.Background(), pollInterval)

		// Try to wait for workflow completion
		// 尝试等待 workflow 完成
		we := s.temporalClient.GetWorkflow(waitCtx, workflowID, runID)
		var tmp any
		waitErr := we.Get(waitCtx, &tmp)
		waitCancel()

		// Check workflow status
		// 检查 workflow 状态
		descCtx, descCancel := context.WithTimeout(context.Background(), 30*time.Second)
		desc, err := s.temporalClient.DescribeWorkflowExecution(descCtx, workflowID, runID)
		descCancel()

		if err != nil || desc == nil || desc.WorkflowExecutionInfo == nil {
			consecutiveErrors++
			s.logger.Warn("watchAndPersist: describe failed",
				zap.String("workflow_id", workflowID),
				zap.Int("consecutive_errors", consecutiveErrors),
				zap.Error(err))
			if consecutiveErrors >= maxConsecutiveErrors {
				s.logger.Error("watchAndPersist: giving up after too many consecutive errors",
					zap.String("workflow_id", workflowID))
				return
			}
			time.Sleep(30 * time.Second) // Back off before retry 注：重试前退避
			continue
		}
		consecutiveErrors = 0 // Reset on successful describe describe 注：成功时重置

		st := desc.WorkflowExecutionInfo.GetStatus()
		isTerminal := st == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED ||
			st == enumspb.WORKFLOW_EXECUTION_STATUS_FAILED ||
			st == enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT ||
			st == enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED ||
			st == enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED

		if !isTerminal {
			// Workflow still running - log and continue waiting
			// workflow 仍在运行——记录日志并继续等待
			if waitErr != nil {
				s.logger.Debug("watchAndPersist: workflow still running, will retry",
					zap.String("workflow_id", workflowID),
					zap.String("status", st.String()))
			}
			continue
		}

		// Workflow reached terminal state - persist it
		// workflow 已到终态——持久化它
		statusStr := "RUNNING"
		switch st {
		case enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED:
			statusStr = "COMPLETED"
		case enumspb.WORKFLOW_EXECUTION_STATUS_FAILED:
			statusStr = "FAILED"
		case enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT:
			statusStr = "TIMEOUT"
		case enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED:
			statusStr = "CANCELLED"
		case enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED:
			statusStr = "FAILED"
		}

		s.logger.Info("watchAndPersist: workflow reached terminal state",
			zap.String("workflow_id", workflowID),
			zap.String("status", statusStr))

		start := time.Now()
		if desc.WorkflowExecutionInfo.GetStartTime() != nil {
			start = desc.WorkflowExecutionInfo.GetStartTime().AsTime()
		}
		end := getWorkflowEndTime(desc)
		durationMs := int(end.Sub(start).Milliseconds())

		// Persist terminal state
		// 持久化终态
		persistCtx, persistCancel := context.WithTimeout(context.Background(), 30*time.Second)

		// Update terminal fields only for terminal workflows
		// 仅终态 workflow 才更新终态字段
		if _, err2 := s.dbClient.Wrapper().ExecContext(
			persistCtx,
			`UPDATE task_executions
             SET status = $2,
                 completed_at = $3,
                 duration_ms = COALESCE(duration_ms, $4)
             WHERE workflow_id = $1
               AND status NOT IN ('COMPLETED', 'FAILED')`,
			workflowID, statusStr, end, durationMs,
		); err2 != nil {
			s.logger.Warn("watchAndPersist: final status update failed",
				zap.String("workflow_id", workflowID),
				zap.Error(err2))
		} else {
			s.logger.Debug("watchAndPersist: final status updated",
				zap.String("workflow_id", workflowID),
				zap.String("status", statusStr))
		}

		// Trigger rich persistence (result + token/cost aggregation)
		// 触发完整持久化（结果 + token/成本聚合）
		if _, err := s.GetTaskStatus(persistCtx, &pb.GetTaskStatusRequest{TaskId: workflowID}); err != nil {
			s.logger.Warn("watchAndPersist: GetTaskStatus persistence failed",
				zap.String("workflow_id", workflowID),
				zap.Error(err))
		} else {
			s.logger.Debug("watchAndPersist: terminal metrics persisted",
				zap.String("workflow_id", workflowID))
		}

		// Best-effort polling to tolerate visibility lag
		// 尽力轮询以容忍可见性延迟
		const maxAttempts = 6
		for range maxAttempts {
			var tokens sql.NullInt64
			var res sql.NullString
			row := s.dbClient.Wrapper().QueryRowContext(persistCtx,
				`SELECT total_tokens, result FROM task_executions WHERE workflow_id = $1`, workflowID,
			)
			if err := row.Scan(&tokens, &res); err == nil {
				hasTokens := tokens.Valid && tokens.Int64 > 0
				hasResult := res.Valid && res.String != ""
				if hasTokens || hasResult {
					s.logger.Debug("watchAndPersist: persistence confirmed",
						zap.String("workflow_id", workflowID),
						zap.Bool("has_tokens", hasTokens),
						zap.Bool("has_result", hasResult))
					break
				}
			}
			if _, err := s.GetTaskStatus(persistCtx, &pb.GetTaskStatusRequest{TaskId: workflowID}); err != nil {
				s.logger.Debug("watchAndPersist: retry GetTaskStatus failed",
					zap.String("workflow_id", workflowID),
					zap.Error(err))
			}
			time.Sleep(2 * time.Second)
		}

		persistCancel()
		return // Done - workflow persisted 注：完成——workflow 已持久化
	}
}

// getTaskStatusFromDB retrieves task status from database when Temporal workflow history has expired.
// This allows querying historical completed tasks beyond Temporal's retention period.
// getTaskStatusFromDB 在 Temporal workflow 历史过期后从数据库获取 task 状态。
// 这使得可以查询超出 Temporal 保留期的历史已完成 task。
func (s *OrchestratorService) getTaskStatusFromDB(ctx context.Context, taskID string) (*pb.GetTaskStatusResponse, error) {
	var (
		dbStatus         string
		dbResult         sql.NullString
		dbError          sql.NullString
		dbQuery          sql.NullString
		dbSessionID      sql.NullString
		dbMode           sql.NullString
		dbModelUsed      sql.NullString
		dbProvider       sql.NullString
		dbTotalTokens    sql.NullInt32
		dbPromptTokens   sql.NullInt32
		dbCompletionToks sql.NullInt32
		dbTotalCost      sql.NullFloat64
		dbTenantID       sql.NullString
	)

	err := s.dbClient.Wrapper().QueryRowContext(ctx, `
		SELECT status, result, error_message, query, session_id, mode,
		       model_used, provider, total_tokens, prompt_tokens, completion_tokens,
		       total_cost_usd, tenant_id
		FROM task_executions
		WHERE workflow_id = $1
		LIMIT 1
	`, taskID).Scan(&dbStatus, &dbResult, &dbError, &dbQuery, &dbSessionID, &dbMode,
		&dbModelUsed, &dbProvider, &dbTotalTokens, &dbPromptTokens, &dbCompletionToks,
		&dbTotalCost, &dbTenantID)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, status.Error(codes.NotFound, "task not found")
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("database error: %v", err))
	}

	// Enforce tenant ownership
	// 强制 tenant 归属校验
	if dbTenantID.Valid && dbTenantID.String != "" {
		if uc, ucErr := auth.GetUserContext(ctx); ucErr == nil && uc != nil {
			if uc.TenantID.String() != dbTenantID.String {
				return nil, status.Error(codes.NotFound, "task not found")
			}
		}
	}

	// Map DB status to proto status
	// 把 DB 状态映射为 proto 状态
	var pbStatus pb.TaskStatus
	switch dbStatus {
	case "COMPLETED":
		pbStatus = pb.TaskStatus_TASK_STATUS_COMPLETED
	case "FAILED":
		pbStatus = pb.TaskStatus_TASK_STATUS_FAILED
	case "CANCELLED":
		pbStatus = pb.TaskStatus_TASK_STATUS_CANCELLED
	case "TIMEOUT":
		pbStatus = pb.TaskStatus_TASK_STATUS_TIMEOUT
	default:
		pbStatus = pb.TaskStatus_TASK_STATUS_UNSPECIFIED
	}

	resp := &pb.GetTaskStatusResponse{
		TaskId:       taskID,
		Status:       pbStatus,
		Result:       dbResult.String,
		ErrorMessage: dbError.String,
	}

	// Note: Gateway enriches response with metrics from DB, so we only need
	// to return status/result here. The unused dbXxx vars are scanned but
	// intentionally not populated in the proto response.
	// 注意：Gateway 会用 DB 指标补全 response，因此这里只需
	// 返回状态/结果。未使用的 dbXxx 变量虽被扫描，
	// 但有意不填充到 proto response 中。

	s.logger.Info("Retrieved task status from database (Temporal history expired)",
		zap.String("task_id", taskID),
		zap.String("status", dbStatus))

	return resp, nil
}

// getWorkflowEndTime returns the workflow end time, preferring Temporal CloseTime.
// Falls back to time.Now() if CloseTime is unavailable (e.g., race or visibility lag).
// getWorkflowEndTime 返回 workflow 结束时间，优先使用 Temporal CloseTime。
// CloseTime 不可用时（如竞态或可见性延迟）回退到 time.Now()。
func getWorkflowEndTime(desc *workflowservice.DescribeWorkflowExecutionResponse) time.Time {
	if desc != nil && desc.WorkflowExecutionInfo != nil && desc.WorkflowExecutionInfo.CloseTime != nil {
		return desc.WorkflowExecutionInfo.CloseTime.AsTime()
	}
	return time.Now()
}

// RegisterOrchestratorServiceServer registers the service with the gRPC server
// RegisterOrchestratorServiceServer 把服务注册到 gRPC server
func RegisterOrchestratorServiceServer(s *grpc.Server, srv pb.OrchestratorServiceServer) {
	pb.RegisterOrchestratorServiceServer(s, srv)
}

// calculateTokenCost calculates the cost based on token count and model
// calculateTokenCost 按 token 数与模型计算成本
func calculateTokenCost(tokens int, metadata map[string]any) float64 {
	// Prefer centralized pricing config (model-specific) with sensible fallback.
	// 优先使用集中式定价配置（按模型），并带合理兜底。
	var model string
	if metadata != nil {
		if m, ok := metadata["model"].(string); ok && m != "" {
			model = m
		} else if m, ok := metadata["model_used"].(string); ok && m != "" {
			// Fallback to model_used if model is not present
			// model 缺失时回退到 model_used
			model = m
		}
	}
	return pricing.CostForTokens(model, tokens)
}

// detectProviderFromModel determines the provider based on the model name
// Delegates to shared models.DetectProvider for consistent provider detection
// detectProviderFromModel 按模型名判定 provider
// 委托给共享的 models.DetectProvider，保证判定一致
func detectProviderFromModel(model string) string {
	return models.DetectProvider(model)
}

// Helper function to convert session history for workflow
// 为 workflow 转换 session 历史的辅助函数
func convertHistoryForWorkflow(messages []session.Message) []workflows.Message {
	result := make([]workflows.Message, len(messages))
	for i, msg := range messages {
		result[i] = workflows.Message{
			Role:      msg.Role,
			Content:   msg.Content,
			Timestamp: msg.Timestamp,
		}
	}
	return result
}

// ApproveTask handles human approval for a task
// ApproveTask 处理 task 的人工审批
func (s *OrchestratorService) ApproveTask(ctx context.Context, req *pb.ApproveTaskRequest) (*pb.ApproveTaskResponse, error) {
	s.logger.Info("Received ApproveTask request",
		zap.String("approval_id", req.ApprovalId),
		zap.String("workflow_id", req.WorkflowId),
		zap.Bool("approved", req.Approved),
	)

	// Validate input
	// 校验输入
	if req.ApprovalId == "" || req.WorkflowId == "" {
		return &pb.ApproveTaskResponse{
			Success: false,
			Message: "approval_id and workflow_id are required",
		}, nil
	}

	// Enforce authentication and ownership
	// 强制认证与归属校验
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}
	desc, dErr := s.temporalClient.DescribeWorkflowExecution(ctx, req.WorkflowId, req.RunId)
	if dErr != nil || desc == nil || desc.WorkflowExecutionInfo == nil {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}
	if desc.WorkflowExecutionInfo.Memo != nil {
		dc := converter.GetDefaultDataConverter()
		if f, ok := desc.WorkflowExecutionInfo.Memo.Fields["tenant_id"]; ok && f != nil {
			var memoTenant string
			_ = dc.FromPayload(f, &memoTenant)
			if memoTenant != "" && uc.TenantID.String() != memoTenant {
				return nil, status.Error(codes.NotFound, "workflow not found")
			}
		}
		if f, ok := desc.WorkflowExecutionInfo.Memo.Fields["user_id"]; ok && f != nil {
			var memoUser string
			_ = dc.FromPayload(f, &memoUser)
			if memoUser != "" && uc.UserID.String() != memoUser {
				return nil, status.Error(codes.NotFound, "workflow not found")
			}
		}
	}

	// Create the approval result
	// 创建 approval result
	approvalResult := activities.HumanApprovalResult{
		ApprovalID:     req.ApprovalId,
		Approved:       req.Approved,
		Feedback:       req.Feedback,
		ModifiedAction: req.ModifiedAction,
		ApprovedBy:     req.ApprovedBy,
		Timestamp:      time.Now(),
	}

	// Store the approval in our activities (for tracking/audit)
	// 把审批存入 activities（用于跟踪/审计）
	if procErr := s.humanActivities.ProcessApprovalResponse(ctx, approvalResult); procErr != nil {
		s.logger.Error("Failed to process approval response", zap.Error(procErr))
	}

	// Send signal to the workflow
	// 向 workflow 发送 signal
	signalName := fmt.Sprintf("human-approval-%s", req.ApprovalId)
	err = s.temporalClient.SignalWorkflow(
		ctx,
		req.WorkflowId,
		req.RunId, // Can be empty to signal the current run 注：可为空，表示当前运行
		signalName,
		approvalResult,
	)

	if err != nil {
		s.logger.Error("Failed to signal workflow",
			zap.String("workflow_id", req.WorkflowId),
			zap.String("signal_name", signalName),
			zap.Error(err),
		)
		return &pb.ApproveTaskResponse{
			Success: false,
			Message: fmt.Sprintf("Failed to signal workflow: %v", err),
		}, nil
	}

	s.logger.Info("Successfully signaled workflow with approval",
		zap.String("workflow_id", req.WorkflowId),
		zap.String("approval_id", req.ApprovalId),
		zap.Bool("approved", req.Approved),
	)

	return &pb.ApproveTaskResponse{
		Success: true,
		Message: fmt.Sprintf("Approval %s processed successfully", req.ApprovalId),
	}, nil
}

// GetPendingApprovals gets pending approvals for a user/session
// GetPendingApprovals 获取用户/session 的待处理审批
func (s *OrchestratorService) GetPendingApprovals(ctx context.Context, req *pb.GetPendingApprovalsRequest) (*pb.GetPendingApprovalsResponse, error) {
	s.logger.Info("Received GetPendingApprovals request",
		zap.String("user_id", req.UserId),
		zap.String("session_id", req.SessionId),
	)

	// In a production system, this would query a database for pending approvals
	// For now, return an empty list as this is primarily for UI/monitoring
	// The actual approval state is maintained in the workflow and in-memory activities
	// 生产系统中这里应查询数据库获取待处理审批
	// 目前返回空列表，因为该接口主要用于 UI/监控
	// 实际的审批状态维护在 workflow 与内存中的 activities 里

	return &pb.GetPendingApprovalsResponse{
		Approvals: []*pb.PendingApproval{},
	}, nil
}

// SubmitReviewDecision handles the HITL research review approval.
// It sends a Temporal Signal to the waiting workflow with the review result.
// SubmitReviewDecision 处理 HITL 研究评审的审批。
// 它向等待中的 workflow 发送携带评审结果的 Temporal Signal。
func (s *OrchestratorService) SubmitReviewDecision(
	ctx context.Context,
	req *pb.SubmitReviewDecisionRequest,
) (*pb.SubmitReviewDecisionResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_id is required")
	}
	if req.Approved && strings.TrimSpace(req.FinalPlan) == "" {
		return nil, status.Error(codes.InvalidArgument, "final_plan is required when approved")
	}

	// Authenticate
	// 认证
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	// Validate ownership via Temporal memo (same pattern as GetTask)
	// 通过 Temporal memo 校验归属（与 GetTask 相同的模式）
	wfDesc, err := s.temporalClient.DescribeWorkflowExecution(ctx, req.WorkflowId, "")
	if err != nil {
		s.logger.Warn("Failed to describe workflow for review decision",
			zap.String("workflow_id", req.WorkflowId),
			zap.Error(err))
		return nil, status.Error(codes.NotFound, "workflow not found")
	}

	if wfDesc == nil || wfDesc.WorkflowExecutionInfo == nil || wfDesc.WorkflowExecutionInfo.Memo == nil {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}

	dc := converter.GetDefaultDataConverter()
	if tenantField, ok := wfDesc.WorkflowExecutionInfo.Memo.Fields["tenant_id"]; ok && tenantField != nil {
		var memoTenant string
		_ = dc.FromPayload(tenantField, &memoTenant)
		if memoTenant != "" && uc.TenantID.String() != memoTenant {
			return nil, status.Error(codes.NotFound, "workflow not found")
		}
	}
	if userField, ok := wfDesc.WorkflowExecutionInfo.Memo.Fields["user_id"]; ok && userField != nil {
		var memoUser string
		_ = dc.FromPayload(userField, &memoUser)
		if memoUser != "" && uc.UserID.String() != memoUser {
			return nil, status.Error(codes.NotFound, "workflow not found")
		}
	}

	// Build the Signal payload
	// 构建 Signal payload
	var conversation []activities.ReviewRound
	if req.Conversation != "" {
		if err := json.Unmarshal([]byte(req.Conversation), &conversation); err != nil {
			s.logger.Warn("Failed to parse conversation JSON", zap.Error(err))
			// Non-fatal: conversation is informational
			// 非致命：conversation 仅作参考
		}
	}

	reviewResult := activities.ResearchReviewResult{
		Approved:      req.Approved,
		FinalPlan:     req.FinalPlan,
		Conversation:  conversation,
		ResearchBrief: req.ResearchBrief,
	}

	// Send Signal to workflow
	// 向 workflow 发送 Signal
	sigName := "research-plan-approved-" + req.WorkflowId
	err = s.temporalClient.SignalWorkflow(ctx, req.WorkflowId, "", sigName, reviewResult)
	if err != nil {
		s.logger.Error("Failed to signal workflow for review decision",
			zap.String("workflow_id", req.WorkflowId),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to signal workflow: %v", err)
	}

	s.logger.Info("HITL review decision submitted",
		zap.String("workflow_id", req.WorkflowId),
		zap.Bool("approved", req.Approved),
		zap.String("approved_by", uc.UserID.String()),
	)

	return &pb.SubmitReviewDecisionResponse{
		Success: true,
		Message: "Review decision submitted",
	}, nil
}

// RecordTokenUsage records token usage from Gateway-side LLM calls (e.g., HITL feedback).
// RecordTokenUsage 记录 Gateway 侧 LLM 调用（如 HITL 反馈）的 token 用量。
func (s *OrchestratorService) RecordTokenUsage(
	ctx context.Context,
	req *pb.RecordTokenUsageRequest,
) (*pb.RecordTokenUsageResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_id is required")
	}
	if req.InputTokens < 0 || req.OutputTokens < 0 {
		return nil, status.Error(codes.InvalidArgument, "input_tokens and output_tokens must be >= 0")
	}
	if s.temporalClient == nil {
		return nil, status.Error(codes.Unavailable, "Temporal not ready")
	}

	// Authenticate (enforced by interceptor; still validate here for clarity)
	// authentication（由拦截器强制执行；此处仍校验以确保清晰）
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	// Best-effort recording — don't fail the workflow if quota recorder is unavailable
	// 尽力记录——配额记录器不可用时不使 workflow 失败
	s.logger.Info("Recording token usage from gateway",
		zap.String("workflow_id", req.WorkflowId),
		zap.String("agent_id", req.AgentId),
		zap.String("model", req.Model),
		zap.Int32("input_tokens", req.InputTokens),
		zap.Int32("output_tokens", req.OutputTokens),
	)

	// Derive tenant_id from the workflow memo (source of truth for tenancy).
	// 从 workflow memo 推导 tenant_id（租户关系的事实来源）。
	wfDesc, err := s.temporalClient.DescribeWorkflowExecution(ctx, req.WorkflowId, "")
	if err != nil || wfDesc == nil || wfDesc.WorkflowExecutionInfo == nil {
		// Best-effort: don't fail client calls on transient Temporal errors.
		// 尽力而为：Temporal 的瞬时错误不应导致客户端调用失败。
		s.logger.Warn("Failed to describe workflow for token usage recording",
			zap.String("workflow_id", req.WorkflowId),
			zap.Error(err),
		)
		return &pb.RecordTokenUsageResponse{Success: true}, nil
	}

	var memoTenant string
	var memoUser string
	if wfDesc.WorkflowExecutionInfo.Memo != nil {
		dc := converter.GetDefaultDataConverter()
		if tenantField, ok := wfDesc.WorkflowExecutionInfo.Memo.Fields["tenant_id"]; ok && tenantField != nil {
			_ = dc.FromPayload(tenantField, &memoTenant)
		}
		if userField, ok := wfDesc.WorkflowExecutionInfo.Memo.Fields["user_id"]; ok && userField != nil {
			_ = dc.FromPayload(userField, &memoUser)
		}
	}
	if memoTenant == "" {
		s.logger.Warn("Workflow memo missing tenant_id; skipping quota recording",
			zap.String("workflow_id", req.WorkflowId),
		)
		return &pb.RecordTokenUsageResponse{Success: true}, nil
	}
	if uc.TenantID.String() != memoTenant {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}
	if memoUser != "" && uc.UserID.String() != memoUser {
		return nil, status.Error(codes.NotFound, "workflow not found")
	}

	tenantUUID, err := uuid.Parse(memoTenant)
	if err != nil {
		s.logger.Warn("Invalid tenant_id in workflow memo; skipping quota recording",
			zap.String("workflow_id", req.WorkflowId),
			zap.String("tenant_id", memoTenant),
			zap.Error(err),
		)
		return &pb.RecordTokenUsageResponse{Success: true}, nil
	}

	details := RecordUsageDetails{
		InputTokens:           int64(req.InputTokens),
		OutputTokens:          int64(req.OutputTokens),
		CacheReadTokens:       int64(req.CacheReadTokens),
		CacheCreationTokens:   int64(req.CacheCreationTokens),
		CacheCreation1hTokens: int64(req.CacheCreation_1HTokens),
		Model:                 req.Model,
		Provider:              req.Provider,
	}
	details.EnsureCacheAwareTotal()
	s.RecordUsage(ctx, tenantUUID, req.WorkflowId, details)

	return &pb.RecordTokenUsageResponse{Success: true}, nil
}

// CreateSchedule creates a new scheduled task (with ownership)
// CreateSchedule 创建新的 scheduled task（含归属校验）
func (s *OrchestratorService) CreateSchedule(ctx context.Context, req *pb.CreateScheduleRequest) (*pb.CreateScheduleResponse, error) {
	// 1. Auth enforcement
	// 1. 认证强制
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	// 2. Check if schedule manager is available
	// 2. 检查定时任务管理器是否可用
	if s.scheduleManager == nil {
		return nil, status.Error(codes.Unavailable, "scheduling feature not enabled")
	}

	// 3. Validate input
	// 3. 校验输入
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "schedule name required")
	}
	if req.CronExpression == "" {
		return nil, status.Error(codes.InvalidArgument, "cron expression required")
	}
	if req.TaskQuery == "" {
		return nil, status.Error(codes.InvalidArgument, "task query required")
	}

	// 4. Set defaults
	// 4. 设置默认值
	timezone := req.Timezone
	if timezone == "" {
		timezone = "UTC"
	}
	timeoutSecs := req.TimeoutSeconds
	if timeoutSecs == 0 {
		timeoutSecs = 3600 // 1 hour default 注：默认 1 小时
	}

	// 5. Convert proto map to Go map, decoding JSON-encoded values
	// 5. 把 proto map 转为 Go map，并解码 JSON 编码的值
	taskContext := decodeTaskContext(req.TaskContext)

	// 6. Create via schedule manager
	// 6. 经 schedule manager 创建
	input := &schedules.CreateScheduleInput{
		UserID:             uc.UserID,
		TenantID:           uc.TenantID,
		Name:               req.Name,
		Description:        req.Description,
		CronExpression:     req.CronExpression,
		Timezone:           timezone,
		TaskQuery:          req.TaskQuery,
		TaskContext:        taskContext,
		MaxBudgetPerRunUSD: req.MaxBudgetPerRunUsd,
		TimeoutSeconds:     int(timeoutSecs),
	}

	schedule, err := s.scheduleManager.CreateSchedule(ctx, input)
	if err != nil {
		s.logger.Error("Failed to create schedule", zap.Error(err))
		// Map typed errors to proper gRPC codes
		// 把类型化错误映射为正确的 gRPC 错误码
		if errors.Is(err, schedules.ErrInvalidCronExpression) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrIntervalTooShort) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrScheduleLimitReached) {
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		}
		if errors.Is(err, schedules.ErrBudgetExceeded) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrInvalidTimezone) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to create schedule: %v", err))
	}

	nextRunStr := ""
	if schedule.NextRunAt != nil {
		nextRunStr = schedule.NextRunAt.Format(time.RFC3339)
	}

	return &pb.CreateScheduleResponse{
		ScheduleId: schedule.ID.String(),
		Message:    "Schedule created successfully",
		NextRunAt:  nextRunStr,
	}, nil
}

// ownedSchedule parses the id, loads the schedule, and enforces ownership.
// ownedSchedule 解析 id、加载 schedule 并强制归属校验。
func (s *OrchestratorService) ownedSchedule(ctx context.Context, uc *auth.UserContext, scheduleIDStr string) (uuid.UUID, *schedules.Schedule, error) {
	if s.scheduleManager == nil {
		return uuid.UUID{}, nil, status.Error(codes.Unavailable, "scheduling feature not enabled")
	}
	scheduleID, err := uuid.Parse(scheduleIDStr)
	if err != nil {
		return uuid.UUID{}, nil, status.Error(codes.InvalidArgument, "invalid schedule ID")
	}
	schedule, err := s.scheduleManager.GetSchedule(ctx, scheduleID)
	if err != nil {
		return uuid.UUID{}, nil, status.Error(codes.NotFound, "schedule not found")
	}
	if schedule.UserID != uc.UserID || schedule.TenantID != uc.TenantID {
		return uuid.UUID{}, nil, status.Error(codes.NotFound, "schedule not found")
	}
	return scheduleID, schedule, nil
}

// GetSchedule retrieves a schedule (with ownership check)
// GetSchedule 获取定时任务（含归属校验）
func (s *OrchestratorService) GetSchedule(ctx context.Context, req *pb.GetScheduleRequest) (*pb.GetScheduleResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	_, schedule, err := s.ownedSchedule(ctx, uc, req.ScheduleId)
	if err != nil {
		return nil, err
	}

	return &pb.GetScheduleResponse{
		Schedule: convertScheduleToProto(schedule),
	}, nil
}

// ListSchedules retrieves schedules for a user
// ListSchedules 获取某个用户的 schedule 列表
func (s *OrchestratorService) ListSchedules(ctx context.Context, req *pb.ListSchedulesRequest) (*pb.ListSchedulesResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	if s.scheduleManager == nil {
		return nil, status.Error(codes.Unavailable, "scheduling feature not enabled")
	}

	// Set defaults
	// 设置默认值
	pageSize := int(req.PageSize)
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50
	}
	page := int(req.Page)
	if page <= 0 {
		page = 1
	}

	statusFilter := req.Status
	if statusFilter != "ACTIVE" && statusFilter != "PAUSED" && statusFilter != "ALL" {
		statusFilter = "ALL"
	}

	schedulesList, totalCount, err := s.scheduleManager.ListSchedules(ctx, uc.UserID, uc.TenantID, page, pageSize, statusFilter)
	if err != nil {
		s.logger.Error("Failed to list schedules", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to list schedules")
	}

	scheduleInfos := make([]*pb.ScheduleInfo, 0, len(schedulesList))
	for _, schedule := range schedulesList {
		scheduleInfos = append(scheduleInfos, convertScheduleToProto(schedule))
	}

	return &pb.ListSchedulesResponse{
		Schedules:  scheduleInfos,
		TotalCount: int32(totalCount),
		Page:       int32(page),
		PageSize:   int32(pageSize),
	}, nil
}

// UpdateSchedule updates a schedule
// UpdateSchedule 更新 schedule
func (s *OrchestratorService) UpdateSchedule(ctx context.Context, req *pb.UpdateScheduleRequest) (*pb.UpdateScheduleResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	scheduleID, _, err := s.ownedSchedule(ctx, uc, req.ScheduleId)
	if err != nil {
		return nil, err
	}

	// Build update input
	// 构建 update input
	updateInput := &schedules.UpdateScheduleInput{
		ScheduleID: scheduleID,
	}

	// Handle TaskContext: set if provided with values, or clear if explicitly requested
	// 处理 TaskContext：提供了值则设置，显式要求清除则清空
	if req.ClearTaskContext {
		// Explicit clear: set to empty map (not nil) to overwrite existing
		// 显式清除：设为空 map（而非 nil）以覆盖现有值
		updateInput.TaskContext = make(map[string]any)
	} else if len(req.TaskContext) > 0 {
		// New values provided: decode JSON-encoded values and set
		// 提供了新值：解码 JSON 编码的值并设置
		updateInput.TaskContext = decodeTaskContext(req.TaskContext)
	}
	// If neither condition: TaskContext stays nil, preserving existing value
	// 两者都不满足：TaskContext 保持 nil，从而保留现有值

	// Only set fields that are provided
	// 仅设置已提供的字段
	if req.Name != nil {
		updateInput.Name = req.Name
	}
	if req.Description != nil {
		updateInput.Description = req.Description
	}
	if req.CronExpression != nil {
		updateInput.CronExpression = req.CronExpression
	}
	if req.Timezone != nil {
		updateInput.Timezone = req.Timezone
	}
	if req.TaskQuery != nil {
		updateInput.TaskQuery = req.TaskQuery
	}
	if req.MaxBudgetPerRunUsd != nil {
		updateInput.MaxBudgetPerRunUSD = req.MaxBudgetPerRunUsd
	}
	if req.TimeoutSeconds != nil {
		timeoutInt := int(*req.TimeoutSeconds)
		updateInput.TimeoutSeconds = &timeoutInt
	}

	nextRun, err := s.scheduleManager.UpdateSchedule(ctx, updateInput)
	if err != nil {
		s.logger.Error("Failed to update schedule", zap.Error(err))
		// Map typed errors to proper gRPC codes
		// 把类型化错误映射为正确的 gRPC 错误码
		if errors.Is(err, schedules.ErrInvalidCronExpression) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrIntervalTooShort) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrBudgetExceeded) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrInvalidTimezone) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, schedules.ErrScheduleNotFound) {
			return nil, status.Error(codes.NotFound, err.Error())
		}
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to update schedule: %v", err))
	}

	nextRunStr := ""
	if nextRun != nil {
		nextRunStr = nextRun.Format(time.RFC3339)
	}

	return &pb.UpdateScheduleResponse{
		Success:   true,
		Message:   "Schedule updated successfully",
		NextRunAt: nextRunStr,
	}, nil
}

// DeleteSchedule deletes a schedule
// DeleteSchedule 删除 schedule
func (s *OrchestratorService) DeleteSchedule(ctx context.Context, req *pb.DeleteScheduleRequest) (*pb.DeleteScheduleResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	scheduleID, _, err := s.ownedSchedule(ctx, uc, req.ScheduleId)
	if err != nil {
		return nil, err
	}

	if err := s.scheduleManager.DeleteSchedule(ctx, scheduleID); err != nil {
		s.logger.Error("Failed to delete schedule", zap.Error(err))
		return nil, status.Error(codes.Internal, "failed to delete schedule")
	}

	return &pb.DeleteScheduleResponse{
		Success: true,
		Message: "Schedule deleted successfully",
	}, nil
}

// PauseSchedule pauses a schedule
// PauseSchedule 暂停 schedule
func (s *OrchestratorService) PauseSchedule(ctx context.Context, req *pb.PauseScheduleRequest) (*pb.PauseScheduleResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	scheduleID, _, err := s.ownedSchedule(ctx, uc, req.ScheduleId)
	if err != nil {
		return nil, err
	}

	if err := s.scheduleManager.PauseSchedule(ctx, scheduleID, req.Reason); err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to pause schedule: %v", err))
	}

	return &pb.PauseScheduleResponse{
		Success: true,
		Message: "Schedule paused",
	}, nil
}

// ResumeSchedule resumes a paused schedule
// ResumeSchedule 恢复已暂停的 schedule
func (s *OrchestratorService) ResumeSchedule(ctx context.Context, req *pb.ResumeScheduleRequest) (*pb.ResumeScheduleResponse, error) {
	uc, err := auth.GetUserContext(ctx)
	if err != nil || uc == nil {
		return nil, status.Error(codes.Unauthenticated, "authentication required")
	}

	scheduleID, _, err := s.ownedSchedule(ctx, uc, req.ScheduleId)
	if err != nil {
		return nil, err
	}

	nextRun, err := s.scheduleManager.ResumeSchedule(ctx, scheduleID, req.Reason)
	if err != nil {
		return nil, status.Error(codes.Internal, fmt.Sprintf("failed to resume schedule: %v", err))
	}

	nextRunStr := ""
	if nextRun != nil {
		nextRunStr = nextRun.Format(time.RFC3339)
	}

	return &pb.ResumeScheduleResponse{
		Success:   true,
		Message:   "Schedule resumed",
		NextRunAt: nextRunStr,
	}, nil
}

// Helper to convert Schedule to proto
// 把 Schedule 转换为 proto 的辅助函数
func convertScheduleToProto(s *schedules.Schedule) *pb.ScheduleInfo {
	taskContext := make(map[string]string)
	for k, v := range s.TaskContext {
		taskContext[k] = fmt.Sprintf("%v", v)
	}

	lastRunAt := ""
	if s.LastRunAt != nil {
		lastRunAt = s.LastRunAt.Format(time.RFC3339)
	}
	nextRunAt := ""
	if s.NextRunAt != nil {
		nextRunAt = s.NextRunAt.Format(time.RFC3339)
	}

	return &pb.ScheduleInfo{
		ScheduleId:         s.ID.String(),
		Name:               s.Name,
		Description:        s.Description,
		CronExpression:     s.CronExpression,
		Timezone:           s.Timezone,
		TaskQuery:          s.TaskQuery,
		TaskContext:        taskContext,
		MaxBudgetPerRunUsd: s.MaxBudgetPerRunUSD,
		TimeoutSeconds:     int32(s.TimeoutSeconds),
		Status:             s.Status,
		CreatedAt:          s.CreatedAt.Format(time.RFC3339),
		UpdatedAt:          s.UpdatedAt.Format(time.RFC3339),
		LastRunAt:          lastRunAt,
		NextRunAt:          nextRunAt,
		TotalRuns:          int32(s.TotalRuns),
		SuccessfulRuns:     int32(s.SuccessfulRuns),
		FailedRuns:         int32(s.FailedRuns),
	}
}

// jsonEncodedPrefix marks values that were JSON-encoded during transport.
// This prefix prevents backwards-incompatible coercion of string values like "true" or "123".
// jsonEncodedPrefix 标记传输期间做过 JSON 编码的值。
// 该前缀避免把 "true"、"123" 之类字符串值做向后不兼容的强制转换。
const jsonEncodedPrefix = "\x00json:"

// decodeTaskContext converts proto map[string]string to map[string]interface{},
// decoding only values with the JSON prefix marker back to their original types.
// Plain string values (including "true", "123") are preserved as strings.
// decodeTaskContext 把 proto 的 map[string]string 转为 map[string]interface{}，
// 只把带 JSON 前缀标记的值解码回原类型。
// 普通字符串值（含 "true"、"123"）保持为字符串。
func decodeTaskContext(ctx map[string]string) map[string]any {
	if ctx == nil {
		return nil
	}
	result := make(map[string]any, len(ctx))
	for k, v := range ctx {
		if after, ok := strings.CutPrefix(v, jsonEncodedPrefix); ok {
			// JSON-encoded value: decode it
			// JSON 编码的值：解码它
			jsonStr := after
			var decoded any
			if err := json.Unmarshal([]byte(jsonStr), &decoded); err == nil {
				result[k] = decoded
			} else {
				result[k] = v // fallback to original if decode fails 注：解码失败时回退为原值
			}
		} else {
			// Plain string: preserve as-is
			// 纯字符串：原样保留
			result[k] = v
		}
	}
	return result
}

// SendSwarmMessage forwards a human message to a running SwarmWorkflow via Temporal Signal.
// SendSwarmMessage 通过 Temporal Signal 把人工消息转发给运行中的 SwarmWorkflow。
func (s *OrchestratorService) SendSwarmMessage(ctx context.Context, req *pb.SendSwarmMessageRequest) (*pb.SendSwarmMessageResponse, error) {
	if req.WorkflowId == "" {
		return nil, status.Error(codes.InvalidArgument, "workflow_id is required")
	}
	if req.Message == "" {
		return nil, status.Error(codes.InvalidArgument, "message is required")
	}

	payload := map[string]string{"message": req.Message}
	err := s.temporalClient.SignalWorkflow(ctx, req.WorkflowId, "", "human-input", payload)
	if err != nil {
		s.logger.Error("Failed to signal swarm workflow with human input",
			zap.String("workflow_id", req.WorkflowId),
			zap.Error(err))
		return nil, status.Errorf(codes.Internal, "failed to signal workflow: %v", err)
	}

	s.logger.Info("Human input sent to swarm workflow",
		zap.String("workflow_id", req.WorkflowId),
	)

	return &pb.SendSwarmMessageResponse{
		Success: true,
		Status:  "sent",
	}, nil
}
