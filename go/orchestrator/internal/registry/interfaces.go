package registry

import (
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/daemon"
)

// WorkflowRegistrar defines the interface for registering workflows
// WorkflowRegistrar 定义 workflow 注册的接口
type WorkflowRegistrar interface {
	RegisterWorkflows(w worker.Worker) error
}

// ActivityRegistrar defines the interface for registering activities
// ActivityRegistrar 定义 activity 注册的接口
type ActivityRegistrar interface {
	RegisterActivities(w worker.Worker) error
}

// Registry combines both workflow and activity registration
// Registry 同时包含 workflow 与 activity 注册
type Registry interface {
	WorkflowRegistrar
	ActivityRegistrar
}

// RegistryConfig holds configuration for the registry
type RegistryConfig struct {
	// EnableBudgetedWorkflows controls whether budget-aware workflows are registered
	// EnableBudgetedWorkflows 控制是否注册 budget 感知的 workflow
	EnableBudgetedWorkflows bool
	// EnableStreamingWorkflows controls whether streaming workflows are registered
	// EnableStreamingWorkflows 控制是否注册 streaming workflow
	EnableStreamingWorkflows bool
	// EnableApprovalWorkflows controls whether human approval workflows are registered
	// EnableApprovalWorkflows 控制是否注册人工审批 workflow
	EnableApprovalWorkflows bool
	// Optional typed-config defaults for budgets (tokens)
	// budget（token 数）的可选类型化配置默认值
	DefaultTaskBudget    int
	DefaultSessionBudget int

	// Temporal client for schedule management (pause/resume from activities)
	// 用于调度管理的 Temporal client（从 activity 中暂停/恢复）
	TemporalClient client.Client

	// Daemon Hub for dispatching scheduled tasks to connected daemons (nil = skip registration)
	// 用于把定时 task 派发给已连接 daemon 的 Daemon Hub（nil = 跳过注册）
	DaemonHub *daemon.Hub
}
