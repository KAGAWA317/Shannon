package constants

// Activity names used for workflow registration and execution.
// Using constants eliminates magic strings and ensures consistency.
// workflow 注册与执行使用的 activity 名称。
// 用常量消除魔法字符串并保证一致性。
const (
	// Session Management Activities
	UpdateSessionResultActivity = "UpdateSessionResult"

	// Budget Management Activities
	CheckTokenBudgetActivity                   = "CheckTokenBudget"
	CheckTokenBudgetWithBackpressureActivity   = "CheckTokenBudgetWithBackpressure"
	CheckTokenBudgetWithCircuitBreakerActivity = "CheckTokenBudgetWithCircuitBreaker"
	RecordTokenUsageActivity                   = "RecordTokenUsage"
	GenerateUsageReportActivity                = "GenerateUsageReport"
	UpdateBudgetPolicyActivity                 = "UpdateBudgetPolicy"

	// Agent Execution Activities
	ExecuteAgentWithBudgetActivity = "ExecuteAgentWithBudget"

	// Planning/Decomposition Activities
	DecomposeTaskActivity       = "DecomposeTask"
	RefineResearchQueryActivity = "RefineResearchQuery"
	AnalyzeComplexityActivity   = "AnalyzeComplexity" // legacy compatibility for replay

	// Human Intervention Activities
	RequestApprovalActivity         = "RequestApproval"
	ProcessApprovalResponseActivity = "ProcessApprovalResponse"
	GetApprovalStatusActivity       = "GetApprovalStatus"

	// HITL Research Review Activities
	GenerateResearchPlanActivity = "GenerateResearchPlan"

	// Streaming Activities
	StreamExecuteActivity       = "StreamExecute"
	BatchStreamExecuteActivity  = "BatchStreamExecute"
	GetStreamingMetricsActivity = "GetStreamingMetrics"

	// Swarm Agent Activities
	AgentLoopStepActivity = "AgentLoopStep"

	// P2P Activities
	SendAgentMessageActivity   = "SendAgentMessage"
	FetchAgentMessagesActivity = "FetchAgentMessages"
	WorkspaceAppendActivity    = "WorkspaceAppend"
	WorkspaceListActivity      = "WorkspaceList"
	WorkspaceListAllActivity   = "WorkspaceListAll"
	SendTaskRequestActivity    = "SendTaskRequest"
	SendTaskOfferActivity      = "SendTaskOffer"
	SendTaskAcceptActivity     = "SendTaskAccept"

	// File Registry Activities
	RegisterFileActivity    = "RegisterFile"
	GetFileRegistryActivity = "GetFileRegistry"

	// Workspace setup
	SetupWorkspaceDirsActivity = "SetupWorkspaceDirs"

	// TaskList activities
	InitTaskListActivity     = "InitTaskList"
	GetTaskListActivity      = "GetTaskList"
	UpdateTaskStatusActivity = "UpdateTaskStatus"
	CreateTaskActivity            = "CreateTask"
	UpdateTaskDescriptionActivity = "UpdateTaskDescription"
	ClaimTaskActivity             = "ClaimTask"

	// Lead Agent Activities
	LeadDecisionActivity       = "LeadDecision"
	ListWorkspaceFilesActivity = "ListWorkspaceFiles"
	ReadWorkspaceFileActivity  = "ReadWorkspaceFile"
	LeadExecuteToolActivity    = "LeadExecuteTool" // direct tool execution for Lead

	// Schedule Quota Activity
	PauseScheduleForQuotaActivity = "PauseScheduleForQuota"

	// Daemon Dispatch Activity (scheduled task → daemon execution)
	// Daemon 派发 activity（定时 task → daemon 执行）
	DaemonDispatchActivity = "DaemonDispatch"
)
