// Package validation provides utilities for validating workflow configurations.
// validation 包提供校验 workflow 配置的工具。
package validation

import (
	"fmt"
	"strings"
)

// SubtaskInfo represents the minimal information needed for cycle detection
// SubtaskInfo 表示循环检测所需的最小信息
type SubtaskInfo struct {
	ID           string
	Dependencies []string
}

// CycleDetectionResult contains the result of cycle detection
// CycleDetectionResult 包含循环检测的结果
type CycleDetectionResult struct {
	HasCycle     bool
	CyclePath    []string // IDs involved in the cycle (if found) 注：循环涉及的 ID（若检测到）
	SortedOrder  []string // Topological order (if no cycle) 注：拓扑序（无循环时）
	ErrorMessage string
}

// DetectCyclicDependencies checks for circular dependencies in subtask dependencies
// using Kahn's algorithm (topological sort). Returns an error if a cycle is detected.
//
// Example cycle: A depends on B, B depends on C, C depends on A
// This would cause the DAG workflow to hang indefinitely.
// DetectCyclicDependencies 用 Kahn 算法（拓扑排序）检查 subtask 依赖中的循环依赖。
// 检测到循环时返回错误。
// 循环示例：A 依赖 B，B 依赖 C，C 依赖 A
// 这会让 DAG workflow 无限挂起。
func DetectCyclicDependencies(subtasks []SubtaskInfo) CycleDetectionResult {
	if len(subtasks) == 0 {
		return CycleDetectionResult{HasCycle: false, SortedOrder: []string{}}
	}

	// Build adjacency list and in-degree map
	// 构建邻接表与入度 map
	inDegree := make(map[string]int)
	graph := make(map[string][]string) // task -> tasks that depend on it 注：task -> 依赖它的 task
	allNodes := make(map[string]bool)

	// Initialize all nodes
	// 初始化所有节点
	for _, st := range subtasks {
		allNodes[st.ID] = true
		if _, exists := inDegree[st.ID]; !exists {
			inDegree[st.ID] = 0
		}
		if _, exists := graph[st.ID]; !exists {
			graph[st.ID] = []string{}
		}
	}

	// Build the graph: if A depends on B, then B -> A in graph
	// 构建图：若 A 依赖 B，则图中为 B -> A
	for _, st := range subtasks {
		for _, dep := range st.Dependencies {
			// Skip self-dependencies and non-existent dependencies
			// 跳过自依赖与不存在的依赖
			if dep == st.ID {
				continue
			}
			if !allNodes[dep] {
				// Dependency references unknown task - not a cycle, but could be an error
				// 依赖引用了未知 task——不是循环，但可能是错误
				continue
			}
			graph[dep] = append(graph[dep], st.ID)
			inDegree[st.ID]++
		}
	}

	// Kahn's algorithm: start with nodes that have no incoming edges
	// Kahn 算法：从没有入边的节点开始
	queue := []string{}
	for node, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, node)
		}
	}

	sortedOrder := []string{}
	processedCount := 0

	for len(queue) > 0 {
		// Dequeue
		// 出队
		current := queue[0]
		queue = queue[1:]

		sortedOrder = append(sortedOrder, current)
		processedCount++

		// Reduce in-degree for all dependents
		// 减少所有依赖方的入度
		for _, dependent := range graph[current] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	// If we processed all nodes, there's no cycle
	// 若所有节点都被处理，则不存在循环
	if processedCount == len(allNodes) {
		return CycleDetectionResult{
			HasCycle:    false,
			SortedOrder: sortedOrder,
		}
	}

	// Cycle detected - find the nodes involved
	// 检测到循环——找出涉及的节点
	cycleNodes := []string{}
	for node, degree := range inDegree {
		if degree > 0 {
			cycleNodes = append(cycleNodes, node)
		}
	}

	// Try to find the actual cycle path using DFS
	// 尝试用 DFS 找出实际的循环路径
	cyclePath := findCyclePath(graph, cycleNodes)

	return CycleDetectionResult{
		HasCycle:     true,
		CyclePath:    cyclePath,
		ErrorMessage: fmt.Sprintf("circular dependency detected involving tasks: %s", strings.Join(cyclePath, " -> ")),
	}
}

// findCyclePath attempts to find the actual cycle path using DFS
// findCyclePath 尝试用 DFS 找出实际的循环路径
func findCyclePath(graph map[string][]string, cycleNodes []string) []string {
	if len(cycleNodes) == 0 {
		return []string{}
	}

	// Convert to set for quick lookup
	// 转成 set 以便快速查询
	cycleSet := make(map[string]bool)
	for _, n := range cycleNodes {
		cycleSet[n] = true
	}

	// DFS from each cycle node to find the cycle
	// 从每个循环节点 DFS 以找出循环
	visited := make(map[string]bool)
	path := []string{}

	var dfs func(node string, currentPath []string) []string
	dfs = func(node string, currentPath []string) []string {
		if visited[node] {
			// Found cycle - extract the cycle portion
			// 找到循环——截取循环片段
			for i, n := range currentPath {
				if n == node {
					cycle := append(currentPath[i:], node)
					return cycle
				}
			}
			return nil
		}

		if !cycleSet[node] {
			return nil
		}

		visited[node] = true
		currentPath = append(currentPath, node)

		for _, next := range graph[node] {
			if cycleSet[next] {
				result := dfs(next, currentPath)
				if result != nil {
					return result
				}
			}
		}

		return nil
	}

	// Start DFS from each cycle node
	// 从每个循环节点启动 DFS
	for _, start := range cycleNodes {
		visited = make(map[string]bool)
		result := dfs(start, path)
		if len(result) > 1 {
			return result
		}
	}

	// Fallback: just return the cycle nodes
	// 兜底：直接返回循环节点
	return cycleNodes
}

// ValidateDAGDependencies is a convenience function that returns an error if cycles exist
// ValidateDAGDependencies 是便捷函数，存在循环时返回错误
func ValidateDAGDependencies(subtasks []SubtaskInfo) error {
	result := DetectCyclicDependencies(subtasks)
	if result.HasCycle {
		return fmt.Errorf("%s", result.ErrorMessage)
	}
	return nil
}
