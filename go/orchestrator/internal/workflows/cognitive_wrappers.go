package workflows

import (
	"github.com/Kocoro-lab/Shannon/go/orchestrator/internal/workflows/strategies"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/workflow"
)

// ExploratoryWorkflow is a wrapper for strategies.ExploratoryWorkflow
// 薄封装：真正的逻辑在 strategies 包，这里只做 input/result 转换和 child workflow 调用。
func ExploratoryWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	strategiesInput := convertToStrategiesInput(input)
	var strategiesResult strategies.TaskResult
	err := workflow.ExecuteChildWorkflow(childCtx, strategies.ExploratoryWorkflow, strategiesInput).Get(childCtx, &strategiesResult)
	if err != nil {
		return TaskResult{}, err
	}
	return convertFromStrategiesResult(strategiesResult), nil
}

// ScientificWorkflow is a wrapper for strategies.ScientificWorkflow to maintain test compatibility
// 保留同名包装是为了既有测试仍能调用；执行体同样是 strategies 下的 child workflow。
func ScientificWorkflow(ctx workflow.Context, input TaskInput) (TaskResult, error) {
	childCtx := workflow.WithChildOptions(ctx, workflow.ChildWorkflowOptions{
		ParentClosePolicy: enums.PARENT_CLOSE_POLICY_REQUEST_CANCEL,
	})
	strategiesInput := convertToStrategiesInput(input)
	var strategiesResult strategies.TaskResult
	err := workflow.ExecuteChildWorkflow(childCtx, strategies.ScientificWorkflow, strategiesInput).Get(childCtx, &strategiesResult)
	if err != nil {
		return TaskResult{}, err
	}
	return convertFromStrategiesResult(strategiesResult), nil
}

