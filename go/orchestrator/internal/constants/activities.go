package constants

// Activity names used for workflow registration and execution.
// Using constants eliminates magic strings and ensures consistency.
// workflow 注册与执行使用的 activity 名称。
// 用常量消除魔法字符串并保证一致性。
const (
	// Session Management Activities
	// Session 管理相关 activity
	UpdateSessionResultActivity = "UpdateSessionResult"

	// Budget Management Activities
	// Budget 管理相关 activity
	CheckTokenBudgetActivity                   = "CheckTokenBudget"
	CheckTokenBudgetWithBackpressureActivity   = "CheckTokenBudgetWithBackpressure"
	CheckTokenBudgetWithCircuitBreakerActivity = "CheckTokenBudgetWithCircuitBreaker"
	RecordTokenUsageActivity                   = "RecordTokenUsage"
	GenerateUsageReportActivity                = "GenerateUsageReport"
	UpdateBudgetPolicyActivity                 = "UpdateBudgetPolicy"

	// Agent Execution Activities
	// Agent 执行相关 activity
	ExecuteAgentWithBudgetActivity = "ExecuteAgentWithBudget"

	// Planning/Decomposition Activities
	// 规划/分解相关 activity
	DecomposeTaskActivity       = "DecomposeTask"
	RefineResearchQueryActivity = "RefineResearchQuery"
	AnalyzeComplexityActivity   = "AnalyzeComplexity" // legacy compatibility for replay 注：为 replay 保留的 legacy 兼容

	// Human Intervention Activities
	// 人工介入相关 activity
	RequestApprovalActivity         = "RequestApproval"
	ProcessApprovalResponseActivity = "ProcessApprovalResponse"
	GetApprovalStatusActivity       = "GetApprovalStatus"

	// HITL Research Review Activities
	// HITL 研究评审相关 activity
	GenerateResearchPlanActivity = "GenerateResearchPlan"

	// Streaming Activities
	// 流式相关 activity
	StreamExecuteActivity       = "StreamExecute"
	BatchStreamExecuteActivity  = "BatchStreamExecute"
	GetStreamingMetricsActivity = "GetStreamingMetrics"

	// Swarm Agent Activities
	// Swarm Agent 相关 activity
	AgentLoopStepActivity = "AgentLoopStep"

	// P2P Activities
	// P2P 相关 activity
	SendAgentMessageActivity   = "SendAgentMessage"
	FetchAgentMessagesActivity = "FetchAgentMessages"
	WorkspaceAppendActivity    = "WorkspaceAppend"
	WorkspaceListActivity      = "WorkspaceList"
	WorkspaceListAllActivity   = "WorkspaceListAll"
	SendTaskRequestActivity    = "SendTaskRequest"
	SendTaskOfferActivity      = "SendTaskOffer"
	SendTaskAcceptActivity     = "SendTaskAccept"

	// File Registry Activities
	// 文件注册表相关 activity
	RegisterFileActivity    = "RegisterFile"
	GetFileRegistryActivity = "GetFileRegistry"

	// Workspace setup
	// Workspace 初始化
	SetupWorkspaceDirsActivity = "SetupWorkspaceDirs"

	// TaskList activities
	// TaskList 相关 activity
	InitTaskListActivity     = "InitTaskList"
	GetTaskListActivity      = "GetTaskList"
	UpdateTaskStatusActivity = "UpdateTaskStatus"
	CreateTaskActivity            = "CreateTask"
	UpdateTaskDescriptionActivity = "UpdateTaskDescription"
	ClaimTaskActivity             = "ClaimTask"

	// Lead Agent Activities
	// Lead Agent 相关 activity
	LeadDecisionActivity       = "LeadDecision"
	ListWorkspaceFilesActivity = "ListWorkspaceFiles"
	ReadWorkspaceFileActivity  = "ReadWorkspaceFile"
	LeadExecuteToolActivity    = "LeadExecuteTool" // direct tool execution for Lead 注：由 Lead 直接执行 tool

	// Schedule Quota Activity
	// Schedule 配额 activity
	PauseScheduleForQuotaActivity = "PauseScheduleForQuota"

	// Daemon Dispatch Activity (scheduled task → daemon execution)
	// Daemon 派发 activity（定时 task → daemon 执行）
	DaemonDispatchActivity = "DaemonDispatch"
)
