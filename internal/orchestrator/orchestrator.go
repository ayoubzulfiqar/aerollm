package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// NodeKind identifies the type of an orchestration node.
type NodeKind string

const (
	NodeKindAgent    NodeKind = "agent"
	NodeKindTool     NodeKind = "tool"
	NodeKindFunction NodeKind = "function"
)

// Node represents a single unit of work in the DAG.
type Node struct {
	ID        string
	Kind      NodeKind
	Run       func(ctx context.Context, input map[string]interface{}) (map[string]interface{}, error)
	DependsOn []string
}

// Edge represents a data flow between nodes. An edge also implies that To
// depends on From, whether or not To lists From in DependsOn.
type Edge struct {
	From   string
	To     string
	Source string // output field name
	Target string // input field name
}

// Graph is a directed acyclic graph of nodes.
type Graph struct {
	Nodes []Node
	Edges []Edge
}

// ExecutionResult holds outputs from completed nodes.
type ExecutionResult struct {
	mu      sync.RWMutex
	Outputs map[string]map[string]interface{}
}

// Set stores a node output.
func (e *ExecutionResult) Set(nodeID string, output map[string]interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.Outputs == nil {
		e.Outputs = make(map[string]map[string]interface{})
	}
	e.Outputs[nodeID] = output
}

// Get retrieves a node output.
func (e *ExecutionResult) Get(nodeID string) (map[string]interface{}, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out, ok := e.Outputs[nodeID]
	return out, ok
}

// ExecutionOptions controls DAG execution behavior.
type ExecutionOptions struct {
	MaxConcurrency int
}

// ErrCycle is returned when the graph contains a cycle.
var ErrCycle = errors.New("graph contains a cycle")

// Execute runs the DAG concurrently respecting dependencies. The graph is
// validated first (unique non-empty IDs, non-nil Run, known references, no
// cycles). A node starts only once all of its dependencies (DependsOn plus
// incoming edges) have succeeded, and a concurrency slot is taken only for
// nodes that are ready to run, so a small MaxConcurrency can never deadlock.
// The first node error cancels the remaining work and is returned; outputs of
// nodes that completed are still available in the result.
func Execute(ctx context.Context, graph Graph, opts ExecutionOptions) (*ExecutionResult, error) {
	results := &ExecutionResult{}
	if err := ValidateGraph(graph); err != nil {
		return results, err
	}
	if opts.MaxConcurrency <= 0 {
		opts.MaxConcurrency = 4
	}
	if len(graph.Nodes) == 0 {
		return results, ctx.Err()
	}

	nodes := make(map[string]Node, len(graph.Nodes))
	for _, n := range graph.Nodes {
		nodes[n.ID] = n
	}
	deps := dependencies(graph)
	upstream := make(map[string][]Edge)
	for _, e := range graph.Edges {
		upstream[e.To] = append(upstream[e.To], e)
	}
	remaining := make(map[string]int, len(nodes))
	dependents := make(map[string][]string)
	for id := range nodes {
		remaining[id] = len(deps[id])
		for _, d := range deps[id] {
			dependents[d] = append(dependents[d], id)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type done struct {
		id     string
		output map[string]interface{}
		err    error
	}
	doneCh := make(chan done, len(nodes))
	sem := make(chan struct{}, opts.MaxConcurrency)
	var wg sync.WaitGroup

	launch := func(id string) {
		node := nodes[id]
		input := resolveInput(id, deps[id], upstream[id], results)
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				doneCh <- done{id: id, err: ctx.Err()}
				return
			}
			defer func() { <-sem }()
			if err := ctx.Err(); err != nil {
				doneCh <- done{id: id, err: err}
				return
			}
			out, err := runNode(ctx, node, input)
			doneCh <- done{id: id, output: out, err: err}
		}()
	}

	for _, n := range graph.Nodes { // stable launch order
		if remaining[n.ID] == 0 {
			launch(n.ID)
		}
	}

	var firstErr error
	pending := len(nodes)
	inFlight := 0
	for _, n := range graph.Nodes {
		if remaining[n.ID] == 0 {
			inFlight++
		}
	}
	for inFlight > 0 {
		d := <-doneCh
		inFlight--
		pending--
		if d.err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("node %s: %w", d.id, d.err)
				cancel()
			}
			continue
		}
		results.Set(d.id, d.output)
		if firstErr != nil {
			continue
		}
		for _, next := range dependents[d.id] {
			remaining[next]--
			if remaining[next] == 0 {
				launch(next)
				inFlight++
			}
		}
	}
	wg.Wait()
	if firstErr != nil {
		return results, firstErr
	}
	if pending > 0 {
		// Unreachable for a validated DAG; guard against silent partial runs.
		if err := ctx.Err(); err != nil {
			return results, err
		}
		return results, fmt.Errorf("%d nodes were not executed", pending)
	}
	return results, nil
}

// runNode executes a node, converting panics into errors.
func runNode(ctx context.Context, node Node, input map[string]interface{}) (out map[string]interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			out = nil
			err = fmt.Errorf("node panicked: %v", r)
		}
	}()
	return node.Run(ctx, input)
}

// resolveInput builds a node's input from its upstream edges.
func resolveInput(id string, deps []string, edges []Edge, results *ExecutionResult) map[string]interface{} {
	if len(deps) == 0 && len(edges) == 0 {
		return nil
	}
	input := make(map[string]interface{})
	for _, edge := range edges {
		if edge.To != id || edge.Source == "" {
			continue
		}
		out, ok := results.Get(edge.From)
		if !ok {
			continue
		}
		if val, ok := out[edge.Source]; ok {
			target := edge.Target
			if target == "" {
				target = edge.Source
			}
			input[target] = val
		}
	}
	return input
}

// dependencies returns, per node, the deduplicated union of DependsOn and
// incoming edge sources.
func dependencies(graph Graph) map[string][]string {
	out := make(map[string][]string, len(graph.Nodes))
	seen := make(map[string]map[string]bool, len(graph.Nodes))
	add := func(to, from string) {
		if seen[to] == nil {
			seen[to] = make(map[string]bool)
		}
		if seen[to][from] {
			return
		}
		seen[to][from] = true
		out[to] = append(out[to], from)
	}
	for _, n := range graph.Nodes {
		for _, dep := range n.DependsOn {
			add(n.ID, dep)
		}
	}
	for _, e := range graph.Edges {
		add(e.To, e.From)
	}
	return out
}

// ValidateGraph checks that node IDs are non-empty and unique, every node has
// a Run function, all node IDs referenced by edges/dependencies exist, and the
// graph is acyclic.
func ValidateGraph(graph Graph) error {
	nodeSet := make(map[string]struct{}, len(graph.Nodes))
	for _, n := range graph.Nodes {
		if n.ID == "" {
			return errors.New("node id is required")
		}
		if _, dup := nodeSet[n.ID]; dup {
			return fmt.Errorf("duplicate node id %s", n.ID)
		}
		if n.Run == nil {
			return fmt.Errorf("node %s has no Run function", n.ID)
		}
		nodeSet[n.ID] = struct{}{}
	}
	for _, e := range graph.Edges {
		if _, ok := nodeSet[e.From]; !ok {
			return fmt.Errorf("edge references missing node %s", e.From)
		}
		if _, ok := nodeSet[e.To]; !ok {
			return fmt.Errorf("edge references missing node %s", e.To)
		}
	}
	for _, n := range graph.Nodes {
		for _, dep := range n.DependsOn {
			if _, ok := nodeSet[dep]; !ok {
				return fmt.Errorf("node %s depends on missing node %s", n.ID, dep)
			}
		}
	}
	if hasCycle(graph) {
		return ErrCycle
	}
	return nil
}

// hasCycle performs cycle detection via iterative DFS coloring (no recursion,
// so very deep graphs cannot overflow the stack).
func hasCycle(graph Graph) bool {
	const (
		white = 0
		grey  = 1
		black = 2
	)
	adj := make(map[string][]string)
	for from, tos := range reverse(dependencies(graph)) {
		adj[from] = tos
	}
	color := make(map[string]int, len(graph.Nodes))
	type frame struct {
		id   string
		next int
	}
	for _, n := range graph.Nodes {
		if color[n.ID] != white {
			continue
		}
		stack := []frame{{id: n.ID}}
		color[n.ID] = grey
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.next < len(adj[top.id]) {
				nxt := adj[top.id][top.next]
				top.next++
				switch color[nxt] {
				case grey:
					return true
				case white:
					color[nxt] = grey
					stack = append(stack, frame{id: nxt})
				}
				continue
			}
			color[top.id] = black
			stack = stack[:len(stack)-1]
		}
	}
	return false
}

// reverse converts a node->dependencies map into dependency->dependents.
func reverse(deps map[string][]string) map[string][]string {
	out := make(map[string][]string)
	for to, froms := range deps {
		for _, from := range froms {
			out[from] = append(out[from], to)
		}
	}
	return out
}
