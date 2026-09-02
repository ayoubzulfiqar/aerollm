package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
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

// Edge represents a data flow between nodes.
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

// Execute runs the DAG concurrently respecting dependencies using errgroup.
// It resolves input for each node from upstream edges and stores outputs.
func Execute(ctx context.Context, graph Graph, opts ExecutionOptions) (*ExecutionResult, error) {
	if opts.MaxConcurrency <= 0 {
		opts.MaxConcurrency = 4
	}

	results := &ExecutionResult{}
	type nodeState struct {
		done   chan struct{}
		output map[string]interface{}
		err    error
	}
	states := make(map[string]*nodeState)
	for _, node := range graph.Nodes {
		states[node.ID] = &nodeState{done: make(chan struct{})}
	}

	upstream := make(map[string][]Edge)
	for _, edge := range graph.Edges {
		upstream[edge.To] = append(upstream[edge.To], edge)
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(opts.MaxConcurrency)

	for _, node := range graph.Nodes {
		node := node
		g.Go(func() error {
			for _, dep := range node.DependsOn {
				st := states[dep]
				if st == nil {
					return fmt.Errorf("node %s depends on missing node %s", node.ID, dep)
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-st.done:
					if st.err != nil {
						return fmt.Errorf("node %s aborted due to upstream %s error: %w", node.ID, dep, st.err)
					}
				}
			}

			var input map[string]interface{}
			for _, dep := range node.DependsOn {
				out, _ := results.Get(dep)
				if input == nil {
					input = make(map[string]interface{})
				}
				for _, edge := range upstream[node.ID] {
					if edge.From == dep && edge.Source != "" {
						if val, ok := out[edge.Source]; ok {
							input[edge.Target] = val
						}
					}
				}
			}

			output, err := node.Run(ctx, input)
			results.Set(node.ID, output)
			st := states[node.ID]
			st.output = output
			st.err = err
			close(st.done)
			return err
		})
	}

	if err := g.Wait(); err != nil {
		return results, err
	}
	return results, nil
}

// ValidateGraph checks that the graph is acyclic and that all node IDs referenced
// by edges/dependencies exist.
func ValidateGraph(graph Graph) error {
	nodeSet := make(map[string]struct{})
	for _, n := range graph.Nodes {
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
		return errors.New("graph contains a cycle")
	}
	return nil
}

// hasCycle performs a basic cycle detection via DFS coloring.
func hasCycle(graph Graph) bool {
	color := make(map[string]int)
	adj := make(map[string][]string)
	for _, e := range graph.Edges {
		adj[e.From] = append(adj[e.From], e.To)
	}
	for _, n := range graph.Nodes {
		for _, dep := range n.DependsOn {
			adj[dep] = append(adj[dep], n.ID)
		}
	}
	var dfs func(id string) bool
	dfs = func(id string) bool {
		color[id] = 1
		for _, next := range adj[id] {
			c, seen := color[next]
			if seen && c == 1 {
				return true
			}
			if !seen && dfs(next) {
				return true
			}
		}
		color[id] = 2
		return false
	}
	for _, n := range graph.Nodes {
		if _, ok := color[n.ID]; !ok {
			if dfs(n.ID) {
				return true
			}
		}
	}
	return false
}
