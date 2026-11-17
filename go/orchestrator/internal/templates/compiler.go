package templates

import (
	"fmt"
	"maps"
	"sort"
)

// ExecutableNode represents a compiled template node ready for execution.
// ExecutableNode 表示已编译、可执行的模板节点。
type ExecutableNode struct {
	ID             string
	Type           NodeType
	Strategy       StrategyType
	BudgetMax      int
	ToolsAllowlist []string
	OnFail         *TemplateNodeFailure
	Metadata       map[string]any
	DependsOn      []string
}

// ExecutablePlan is a deterministic representation of a template ready for workflow execution.
// ExecutablePlan 是模板的可执行确定性表示，供 workflow 执行。
type ExecutablePlan struct {
	TemplateName    string
	TemplateVersion string
	Defaults        TemplateDefaults
	Nodes           map[string]ExecutableNode
	Order           []string
	Adjacency       map[string][]string
	Checksum        string
}

// CompileTemplate converts a validated template into an ExecutablePlan.
// CompileTemplate 把已校验的模板转换为 ExecutablePlan。
func CompileTemplate(tpl *Template) (*ExecutablePlan, error) {
	if tpl == nil {
		return nil, fmt.Errorf("template is nil")
	}
	if err := ValidateTemplate(tpl); err != nil {
		return nil, err
	}

	plan := &ExecutablePlan{
		TemplateName:    tpl.Name,
		TemplateVersion: tpl.Version,
		Defaults:        tpl.Defaults,
		Nodes:           make(map[string]ExecutableNode, len(tpl.Nodes)),
		Adjacency:       make(map[string][]string, len(tpl.Nodes)),
	}

	// Collect node definitions
// 收集 node 定义
	ids := make([]string, 0, len(tpl.Nodes))
	for _, node := range tpl.Nodes {
		ids = append(ids, node.ID)
		budget := tpl.Defaults.BudgetAgentMax
		if node.BudgetMax != nil {
			budget = *node.BudgetMax
		}
		plan.Nodes[node.ID] = ExecutableNode{
			ID:             node.ID,
			Type:           node.Type,
			Strategy:       node.Strategy,
			BudgetMax:      budget,
			ToolsAllowlist: append([]string(nil), node.ToolsAllowlist...),
			OnFail:         cloneFailure(node.OnFail),
			Metadata:       cloneMap(node.Metadata),
			DependsOn:      append([]string(nil), node.DependsOn...),
		}
		plan.Adjacency[node.ID] = nil
	}

	// Build adjacency and in-degree using depends_on and explicit edges
	// 用 depends_on 与显式边构建邻接表与入度
	edgeSet := make(map[string]map[string]struct{}, len(plan.Nodes))
	for id := range plan.Nodes {
		edgeSet[id] = make(map[string]struct{})
	}

	indegree := make(map[string]int, len(plan.Nodes))

	addEdge := func(from, to string) {
		if from == "" || to == "" || from == to {
			return
		}
		if _, ok := edgeSet[from][to]; ok {
			return
		}
		edgeSet[from][to] = struct{}{}
		indegree[to]++
	}

	for _, node := range tpl.Nodes {
		for _, dep := range node.DependsOn {
			addEdge(dep, node.ID)
		}
	}

	for _, edge := range tpl.Edges {
		addEdge(edge.From, edge.To)
	}

	for from, targets := range edgeSet {
		if len(targets) == 0 {
			continue
		}
		plan.Adjacency[from] = make([]string, 0, len(targets))
		for to := range targets {
			plan.Adjacency[from] = append(plan.Adjacency[from], to)
		}
		sort.Strings(plan.Adjacency[from])
	}

	for id := range plan.Nodes {
		if _, ok := indegree[id]; !ok {
			indegree[id] = 0
		}
	}

	order, err := topologicalOrder(plan.Adjacency, indegree)
	if err != nil {
		return nil, err
	}
	plan.Order = order

	return plan, nil
}

func topologicalOrder(adjacency map[string][]string, indegree map[string]int) ([]string, error) {
	zero := make([]string, 0, len(indegree))
	for id, d := range indegree {
		if d == 0 {
			zero = append(zero, id)
		}
	}
	sort.Strings(zero)

	order := make([]string, 0, len(indegree))
	for len(zero) > 0 {
		current := zero[0]
		zero = zero[1:]
		order = append(order, current)

		for _, next := range adjacency[current] {
			indegree[next]--
			if indegree[next] == 0 {
				zero = append(zero, next)
			}
		}
		sort.Strings(zero)
	}

	if len(order) != len(indegree) {
		return nil, fmt.Errorf("cycle detected in template graph")
	}
	return order, nil
}

func cloneFailure(f *TemplateNodeFailure) *TemplateNodeFailure {
	if f == nil {
		return nil
	}
	clone := *f
	return &clone
}

func cloneMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	maps.Copy(out, in)
	return out
}
