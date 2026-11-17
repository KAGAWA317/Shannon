package registry

import (
	"database/sql"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/activities"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/constants"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/session"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/scheduled"
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/strategies"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.uber.org/zap"
)

// OrchestratorRegistry implements the Registry interface
// OrchestratorRegistry 实现 Registry 接口
type OrchestratorRegistry struct {
	config         *RegistryConfig
	logger         *zap.Logger
	db             *sql.DB
	sessionManager *session.Manager
}

// NewOrchestratorRegistry creates a new registry instance
// NewOrchestratorRegistry 创建一个新的 registry 实例
func NewOrchestratorRegistry(
	config *RegistryConfig,
	logger *zap.Logger,
	db *sql.DB,
	sessionManager *session.Manager,
) *OrchestratorRegistry {
	return &OrchestratorRegistry{
		config:         config,
		logger:         logger,
		db:             db,
		sessionManager: sessionManager,
	}
}

// RegisterWorkflows registers all workflows based on configuration
// RegisterWorkflows 按配置注册所有 workflow
func (r *OrchestratorRegistry) RegisterWorkflows(w worker.Worker) error {
	r.logger.Info("Registering workflows")

	// Core workflows - always registered
	// 核心 workflow —— 始终注册
	w.RegisterWorkflow(workflows.OrchestratorWorkflow)
	w.RegisterWorkflow(workflows.SimpleTaskWorkflow)
	w.RegisterWorkflow(workflows.SupervisorWorkflow)
	w.RegisterWorkflow(workflows.TemplateWorkflow)

	// Scheduled task workflow
	// 定时任务 workflow
	w.RegisterWorkflow(scheduled.ScheduledTaskWorkflow)

	// Cognitive workflows that need pattern migration
	// 需要 pattern 迁移的认知类 workflow
	w.RegisterWorkflow(workflows.ExploratoryWorkflow)
	w.RegisterWorkflow(workflows.ScientificWorkflow)
	r.logger.Info("Registered core workflows")

	// Optional workflows based on configuration
	// 按配置启用的可选 workflow
	if r.config.EnableStreamingWorkflows {
		w.RegisterWorkflow(workflows.StreamingWorkflow)
		w.RegisterWorkflow(workflows.ParallelStreamingWorkflow)
		r.logger.Info("Registered streaming workflows")
	}

	// Strategy workflows (pattern-based)
	// Strategy workflow（基于 pattern）
	w.RegisterWorkflow(strategies.DAGWorkflow)
	w.RegisterWorkflow(strategies.ReactWorkflow)
	w.RegisterWorkflow(strategies.ResearchWorkflow)
	w.RegisterWorkflow(strategies.DomainAnalysisWorkflow)
	w.RegisterWorkflow(strategies.BrowserUseWorkflow)
	r.logger.Info("Registered strategy workflows")

	// Agent workflow (single-purpose deterministic agents)
	// Agent workflow（单一用途的确定性 agent）
	w.RegisterWorkflow(workflows.AgentWorkflow)
	r.logger.Info("Registered agent workflow")

	// Swarm agent workflows (persistent multi-turn agents)
	// Swarm agent workflow（持久化多轮 agent）
	w.RegisterWorkflow(workflows.SwarmWorkflow)
	w.RegisterWorkflow(workflows.AgentLoop)
	r.logger.Info("Registered swarm agent workflows")

	// Enterprise features - conditionally compiled
	// 企业版特性 —— 条件编译
	r.registerEnterpriseWorkflows(w)

	r.logger.Info("All workflows registered successfully")
	return nil
}

// RegisterActivities registers all activities based on configuration
// RegisterActivities 按配置注册所有 activity
func (r *OrchestratorRegistry) RegisterActivities(w worker.Worker) error {
	r.logger.Info("Registering activities")

	// Construct activity receiver with dependencies
	// 构造带依赖的 activity receiver
	acts := activities.NewActivities(r.sessionManager, r.logger)

	// Core activities
	// 核心 activity
	w.RegisterActivity(activities.ExecuteAgent)
	w.RegisterActivity(activities.ExecuteSimpleTask) // Consolidated activity for simple tasks 注：普通任务的合并 activity
	w.RegisterActivity(activities.SynthesizeResults)
	// LLM-backed synthesis (can be selected via workflow versioning)
	// 基于 LLM 的汇总（可通过 workflow 版本控制选择）
	w.RegisterActivity(activities.SynthesizeResultsLLM)
	// Reflection activity for quality evaluation
	// 用于质量评估的 reflection activity
	w.RegisterActivity(acts.EvaluateResult)
	// Deep Research 2.0 activities
	// Deep Research 2.0 相关 activity
	w.RegisterActivityWithOptions(acts.EvaluateCoverage, activity.RegisterOptions{Name: "EvaluateCoverage"})
	w.RegisterActivityWithOptions(acts.IntermediateSynthesis, activity.RegisterOptions{Name: "IntermediateSynthesis"})
	w.RegisterActivityWithOptions(acts.GenerateSubqueries, activity.RegisterOptions{Name: "GenerateSubqueries"})
	w.RegisterActivityWithOptions(acts.ExtractFacts, activity.RegisterOptions{Name: "ExtractFacts"})
	w.RegisterActivityWithOptions(acts.DetectEntityLocalization, activity.RegisterOptions{Name: "DetectEntityLocalization"})
	w.RegisterActivityWithOptions(acts.RouteSearch, activity.RegisterOptions{Name: "RouteSearch"})
	w.RegisterActivityWithOptions(acts.MergeSearchResults, activity.RegisterOptions{Name: "MergeSearchResults"})
	// Claim verification activity (Phase 4)
	// Claim 校验 activity（Phase 4）
	w.RegisterActivityWithOptions(acts.VerifyClaimsActivity, activity.RegisterOptions{Name: "VerifyClaimsActivity"})
	// Citation Agent activity (post-synthesis citation injection)
	// Citation Agent activity（汇总后注入引用）
	w.RegisterActivityWithOptions(acts.AddCitations, activity.RegisterOptions{Name: "AddCitations"})
	// Citation V2 activities (Deep Research with Verify batch)
	// Citation V2 activity（带 Verify 批次的 Deep Research）
	w.RegisterActivityWithOptions(acts.AddCitationsWithVerify, activity.RegisterOptions{Name: "AddCitationsWithVerify"})
	w.RegisterActivityWithOptions(acts.VerifyBatch, activity.RegisterOptions{Name: "VerifyBatch"})
	// Configuration activity
	// 配置读取 activity
	w.RegisterActivity(activities.GetWorkflowConfig)
	// Context compression + store
	// 上下文压缩 + 存储
	w.RegisterActivity(activities.CompressAndStoreContext)
	// Compression rate limiting activities
	// 压缩限流 activity
	w.RegisterActivity(acts.CheckCompressionNeeded)
	w.RegisterActivity(acts.UpdateCompressionStateActivity)

	// Vector intelligence activities
	// 向量智能 activity
	w.RegisterActivity(activities.RecordQuery)
	w.RegisterActivity(activities.FetchSessionMemory)
	// Agent-scoped memory activities (agent_memory_v1)
	// agent 维度的 memory activity（agent_memory_v1）
	w.RegisterActivity(activities.FetchAgentMemory)
	w.RegisterActivity(activities.RecordAgentMemory)
	// Semantic memory activities (Phase 1.1)
	// 语义 memory activity（Phase 1.1）
	w.RegisterActivity(activities.FetchSemanticMemory)
	w.RegisterActivity(activities.FetchHierarchicalMemory)

	// Enhanced supervisor memory activities
	// 增强的 supervisor memory activity
	w.RegisterActivity(activities.FetchSupervisorMemory)
	w.RegisterActivity(activities.RecordDecomposition)
	w.RegisterActivity(activities.RecommendWorkflowStrategy)
	w.RegisterActivity(activities.RecordLearningRouterMetrics)
	// Consensus memory for debate pattern
	// 供 debate pattern 使用的共识 memory
	w.RegisterActivity(activities.PersistDebateConsensus)
	w.RegisterActivity(activities.FetchConsensusMemory)

	// Dynamic team authorization
	// 动态 team 授权
	w.RegisterActivity(activities.AuthorizeTeamAction)

	// Forced tool execution (used by auto file_write guard in swarm)
	// 强制 tool 执行（供 swarm 中的自动 file_write 保护使用）
	w.RegisterActivity(activities.ExecuteAgentWithForcedTools)

	// P2P mailbox + workspace (receiver methods)
	// P2P mailbox + workspace（receiver 方法）
	w.RegisterActivityWithOptions(acts.SendAgentMessage, activity.RegisterOptions{Name: constants.SendAgentMessageActivity})
	w.RegisterActivityWithOptions(acts.FetchAgentMessages, activity.RegisterOptions{Name: constants.FetchAgentMessagesActivity})
	w.RegisterActivityWithOptions(acts.WorkspaceAppend, activity.RegisterOptions{Name: constants.WorkspaceAppendActivity})
	w.RegisterActivityWithOptions(acts.WorkspaceList, activity.RegisterOptions{Name: constants.WorkspaceListActivity})
	w.RegisterActivityWithOptions(acts.WorkspaceListAll, activity.RegisterOptions{Name: constants.WorkspaceListAllActivity})
	// Structured protocol wrappers
	// 结构化协议封装
	w.RegisterActivityWithOptions(acts.SendTaskRequest, activity.RegisterOptions{Name: constants.SendTaskRequestActivity})
	w.RegisterActivityWithOptions(acts.SendTaskOffer, activity.RegisterOptions{Name: constants.SendTaskOfferActivity})
	w.RegisterActivityWithOptions(acts.SendTaskAccept, activity.RegisterOptions{Name: constants.SendTaskAcceptActivity})
	// File registry (swarm file tracking)
	// 文件注册表（swarm 文件追踪）
	w.RegisterActivityWithOptions(acts.RegisterFile, activity.RegisterOptions{Name: constants.RegisterFileActivity})
	w.RegisterActivityWithOptions(acts.GetFileRegistry, activity.RegisterOptions{Name: constants.GetFileRegistryActivity})
	// Workspace directory setup
	// workspace 目录初始化
	w.RegisterActivityWithOptions(acts.SetupWorkspaceDirs, activity.RegisterOptions{Name: constants.SetupWorkspaceDirsActivity})
	// TaskList activities (swarm task tracking)
	// TaskList activity（swarm 任务追踪）
	w.RegisterActivityWithOptions(acts.InitTaskList, activity.RegisterOptions{Name: constants.InitTaskListActivity})
	w.RegisterActivityWithOptions(acts.GetTaskList, activity.RegisterOptions{Name: constants.GetTaskListActivity})
	w.RegisterActivityWithOptions(acts.UpdateTaskStatus, activity.RegisterOptions{Name: constants.UpdateTaskStatusActivity})
	w.RegisterActivityWithOptions(acts.CreateTask, activity.RegisterOptions{Name: constants.CreateTaskActivity})
	w.RegisterActivityWithOptions(acts.UpdateTaskDescription, activity.RegisterOptions{Name: constants.UpdateTaskDescriptionActivity})
	w.RegisterActivityWithOptions(acts.ClaimTask, activity.RegisterOptions{Name: constants.ClaimTaskActivity})
	// Lead Agent decision activity
	// Lead Agent 决策 activity
	w.RegisterActivityWithOptions(activities.LeadDecision, activity.RegisterOptions{Name: constants.LeadDecisionActivity})
	w.RegisterActivityWithOptions(activities.ListWorkspaceFiles, activity.RegisterOptions{Name: constants.ListWorkspaceFilesActivity})
	w.RegisterActivityWithOptions(activities.ReadWorkspaceFile, activity.RegisterOptions{Name: constants.ReadWorkspaceFileActivity})
	w.RegisterActivityWithOptions(activities.LeadExecuteTool, activity.RegisterOptions{Name: constants.LeadExecuteToolActivity})

	// Session activities - register with consistent naming
	// Session activity —— 使用一致的命名注册
	w.RegisterActivityWithOptions(acts.DecomposeTask, activity.RegisterOptions{Name: constants.DecomposeTaskActivity})
	w.RegisterActivityWithOptions(acts.RefineResearchQuery, activity.RegisterOptions{Name: constants.RefineResearchQueryActivity})
	// Legacy activity name for Temporal replay compatibility
	// 为 Temporal 重放兼容保留的旧 activity 名称
	w.RegisterActivityWithOptions(acts.AnalyzeComplexity, activity.RegisterOptions{Name: constants.AnalyzeComplexityActivity})
	w.RegisterActivityWithOptions(acts.UpdateSessionResult, activity.RegisterOptions{
		Name: constants.UpdateSessionResultActivity,
	})
	w.RegisterActivityWithOptions(acts.GenerateSessionTitle, activity.RegisterOptions{Name: "GenerateSessionTitle"})

	// HITL Research Review
	// HITL 研究评审
	w.RegisterActivityWithOptions(activities.GenerateResearchPlan, activity.RegisterOptions{Name: constants.GenerateResearchPlanActivity})

	// Schedule activities
	// 调度相关 activity
	scheduleActivities := activities.NewScheduleActivities(r.db, r.config.TemporalClient, r.logger)
	w.RegisterActivityWithOptions(scheduleActivities.RecordScheduleExecutionStart, activity.RegisterOptions{
		Name: "RecordScheduleExecutionStart",
	})
	w.RegisterActivityWithOptions(scheduleActivities.RecordScheduleExecutionComplete, activity.RegisterOptions{
		Name: "RecordScheduleExecutionComplete",
	})
	w.RegisterActivityWithOptions(scheduleActivities.PauseScheduleForQuota, activity.RegisterOptions{
		Name: constants.PauseScheduleForQuotaActivity,
	})
	r.logger.Info("Registered schedule activities")

	// Daemon dispatch activity — always registered for Temporal replay safety.
	// If DaemonHub is nil, the activity returns "not configured" without dispatching.
	// Daemon 派发 activity——为 Temporal 重放安全始终注册。
	// DaemonHub 为 nil 时，该 activity 返回 "not configured" 而不派发。
	daemonActs := scheduled.NewDaemonActivities(r.config.DaemonHub)
	w.RegisterActivityWithOptions(daemonActs.DaemonDispatchActivity, activity.RegisterOptions{
		Name: constants.DaemonDispatchActivity,
	})
	r.logger.Info("Registered daemon dispatch activity", zap.Bool("hub_available", r.config.DaemonHub != nil))

	// Human intervention activities
	// 人工介入 activity
	if r.config.EnableApprovalWorkflows {
		humanActivities := activities.NewHumanInterventionActivities()
		w.RegisterActivityWithOptions(humanActivities.RequestApproval, activity.RegisterOptions{
			Name: constants.RequestApprovalActivity,
		})
		w.RegisterActivityWithOptions(humanActivities.ProcessApprovalResponse, activity.RegisterOptions{
			Name: constants.ProcessApprovalResponseActivity,
		})
		w.RegisterActivityWithOptions(humanActivities.GetApprovalStatus, activity.RegisterOptions{
			Name: constants.GetApprovalStatusActivity,
		})
		r.logger.Info("Registered human intervention activities")
	}

	// Streaming activities
	// 流式 activity
	if r.config.EnableStreamingWorkflows {
		streamingActivities := activities.NewStreamingActivities()
		w.RegisterActivityWithOptions(streamingActivities.StreamExecute, activity.RegisterOptions{
			Name: constants.StreamExecuteActivity,
		})
		w.RegisterActivityWithOptions(streamingActivities.BatchStreamExecute, activity.RegisterOptions{
			Name: constants.BatchStreamExecuteActivity,
		})
		w.RegisterActivityWithOptions(streamingActivities.GetStreamingMetrics, activity.RegisterOptions{
			Name: constants.GetStreamingMetricsActivity,
		})
		r.logger.Info("Registered streaming activities")
	}

	// Swarm agent activity (persistent agent loop step)
	// Swarm agent activity（持久化 agent 循环的单步）
	w.RegisterActivityWithOptions(activities.AgentLoopStep, activity.RegisterOptions{
		Name: constants.AgentLoopStepActivity,
	})

	// Minimal streaming_v1 event emitter (always safe to register)
	// 最简的 streaming_v1 event 发射器（始终可安全注册）
	w.RegisterActivityWithOptions(activities.EmitTaskUpdate, activity.RegisterOptions{
		Name: "EmitTaskUpdate",
	})

	// Pattern metrics activity
	// pattern 指标 activity
	w.RegisterActivityWithOptions(activities.RecordPatternMetrics, activity.RegisterOptions{
		Name: "RecordPatternMetrics",
	})

	// Enterprise features - conditionally compiled
	// 企业版特性 —— 条件编译
	r.registerEnterpriseActivities(w)

	// User-level memory extraction (Phase 2)
	// 用户级 memory 抽取（Phase 2）
	w.RegisterActivity(activities.ExtractMemoryActivity)

	// Agent selection activities (performance-based)
	// agent 选择 activity（基于性能）
	w.RegisterActivity(activities.FetchAgentPerformances)
	w.RegisterActivity(activities.SelectAgentEpsilonGreedy)
	w.RegisterActivity(activities.SelectAgentUCB1)
	w.RegisterActivity(activities.RecordAgentPerformance)

	// Persistence activities for agent and tool executions
	// These use a global dbClient that must be set during initialization
	// agent 与 tool 执行的持久化 activity
	// 它们使用初始化时必须设置的全局 dbClient
	w.RegisterActivity(activities.PersistAgentExecutionStandalone)
	w.RegisterActivity(activities.PersistToolExecutionStandalone)

	// Budget activities
	// 预算 activity
	if r.config.EnableBudgetedWorkflows {
		var budgetActivities *activities.BudgetActivities
		if r.config.DefaultTaskBudget > 0 || r.config.DefaultSessionBudget > 0 {
			budgetActivities = activities.NewBudgetActivitiesWithDefaults(r.db, r.logger, r.config.DefaultTaskBudget, r.config.DefaultSessionBudget)
		} else {
			budgetActivities = activities.NewBudgetActivities(r.db, r.logger)
		}
		w.RegisterActivityWithOptions(budgetActivities.CheckTokenBudget, activity.RegisterOptions{
			Name: constants.CheckTokenBudgetActivity,
		})
		w.RegisterActivityWithOptions(budgetActivities.CheckTokenBudgetWithBackpressure, activity.RegisterOptions{
			Name: constants.CheckTokenBudgetWithBackpressureActivity,
		})
		w.RegisterActivityWithOptions(budgetActivities.CheckTokenBudgetWithCircuitBreaker, activity.RegisterOptions{
			Name: constants.CheckTokenBudgetWithCircuitBreakerActivity,
		})
		w.RegisterActivityWithOptions(budgetActivities.RecordTokenUsage, activity.RegisterOptions{
			Name: constants.RecordTokenUsageActivity,
		})
		w.RegisterActivityWithOptions(budgetActivities.GenerateUsageReport, activity.RegisterOptions{
			Name: constants.GenerateUsageReportActivity,
		})
		// Also register ExecuteAgentWithBudget activity
		// 另外注册 ExecuteAgentWithBudget activity
		w.RegisterActivityWithOptions(budgetActivities.ExecuteAgentWithBudget, activity.RegisterOptions{
			Name: constants.ExecuteAgentWithBudgetActivity,
		})
		w.RegisterActivityWithOptions(budgetActivities.UpdateBudgetPolicy, activity.RegisterOptions{
			Name: constants.UpdateBudgetPolicyActivity,
		})
		r.logger.Info("Registered budget activities")
	}

	r.logger.Info("All activities registered successfully")
	return nil
}

// registerEnterpriseWorkflows is a stub in the open source version.
// registerEnterpriseWorkflows 在开源版本中是空实现。
func (r *OrchestratorRegistry) registerEnterpriseWorkflows(w worker.Worker) {
	// Enterprise workflows (ads research, Sagasu) are not available in OSS.
	// 企业版 workflow（广告研究、Sagasu）在 OSS 中不可用。
}

// registerEnterpriseActivities registers enterprise-only activities (stub in open source).
// registerEnterpriseActivities 注册仅企业版可用的 activity（开源版本为空实现）。
func (r *OrchestratorRegistry) registerEnterpriseActivities(w worker.Worker) {
	// Agent executor activity (single-purpose deterministic agents)
	// Agent executor activity（单一用途的确定性 agent）
	w.RegisterActivityWithOptions(activities.ExecuteAgentActivity, activity.RegisterOptions{
		Name: "ExecuteAgentActivity",
	})
	r.logger.Info("Registered agent executor activity")
}
