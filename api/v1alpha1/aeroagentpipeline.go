package v1alpha1

import (
	"errors"
	"fmt"
	"strings"
)

// EdgeSeparator separates the source and target node of a pipeline edge,
// e.g. "retrieve->summarize".
const EdgeSeparator = "->"

// AeroAgentPipelineSpec defines a DAG-based agent pipeline configuration.
type AeroAgentPipelineSpec struct {
	Name    string   `json:"name,omitempty"`
	Version string   `json:"version,omitempty"`
	Nodes   []string `json:"nodes,omitempty"`
	// Edges are directed "from->to" pairs between entries of Nodes; the
	// resulting graph must be acyclic.
	Edges      []string               `json:"edges,omitempty"`
	Tools      []string               `json:"tools,omitempty"`
	Parameters map[string]interface{} `json:"parameters,omitempty"`
}

// ParseEdge splits a "from->to" edge into its endpoints.
func ParseEdge(edge string) (from, to string, err error) {
	from, to, ok := strings.Cut(edge, EdgeSeparator)
	from, to = strings.TrimSpace(from), strings.TrimSpace(to)
	if !ok || from == "" || to == "" || strings.Contains(to, EdgeSeparator) {
		return "", "", fmt.Errorf("edge %q: must have the form from%sto", edge, EdgeSeparator)
	}
	return from, to, nil
}

// Validate reports every problem with the spec, joined into one error.
func (s AeroAgentPipelineSpec) Validate() error {
	var errs []error
	if len(s.Name) > MaxNameLength {
		errs = append(errs, fmt.Errorf("name: longer than %d characters", MaxNameLength))
	}
	if len(s.Version) > maxEntryLength {
		errs = append(errs, fmt.Errorf("version: longer than %d characters", maxEntryLength))
	}
	if len(s.Nodes) == 0 {
		errs = append(errs, errors.New("nodes: at least one node is required"))
	}
	errs = append(errs, validateEntries("nodes", s.Nodes)...)
	errs = append(errs, validateEntries("tools", s.Tools)...)
	for _, n := range s.Nodes {
		if strings.Contains(n, EdgeSeparator) {
			errs = append(errs, fmt.Errorf("nodes: %q must not contain %q", n, EdgeSeparator))
		}
	}

	known := make(map[string]bool, len(s.Nodes))
	for _, n := range s.Nodes {
		known[n] = true
	}
	adj := make(map[string][]string, len(s.Nodes))
	seenEdge := make(map[string]bool, len(s.Edges))
	edgesOK := true
	for _, e := range s.Edges {
		from, to, err := ParseEdge(e)
		if err != nil {
			errs = append(errs, err)
			edgesOK = false
			continue
		}
		if !known[from] {
			errs = append(errs, fmt.Errorf("edge %q: unknown node %q", e, from))
			edgesOK = false
		}
		if !known[to] {
			errs = append(errs, fmt.Errorf("edge %q: unknown node %q", e, to))
			edgesOK = false
		}
		if from == to {
			errs = append(errs, fmt.Errorf("edge %q: self loop", e))
			edgesOK = false
			continue
		}
		key := from + EdgeSeparator + to
		if seenEdge[key] {
			errs = append(errs, fmt.Errorf("edge %q: duplicate", e))
			continue
		}
		seenEdge[key] = true
		adj[from] = append(adj[from], to)
	}
	if edgesOK {
		if cycle := findCycle(s.Nodes, adj); cycle != nil {
			errs = append(errs, fmt.Errorf("edges: cycle detected: %s", strings.Join(cycle, EdgeSeparator)))
		}
	}
	return errors.Join(errs...)
}

// findCycle returns one cycle in the graph (first node repeated at the end),
// or nil when the graph is a DAG. Iterative DFS keeps deep graphs from
// exhausting the stack.
func findCycle(nodes []string, adj map[string][]string) []string {
	const (
		white = iota
		grey
		black
	)
	color := make(map[string]int, len(nodes))
	parent := make(map[string]string, len(nodes))
	type frame struct {
		node string
		next int
	}
	for _, start := range nodes {
		if color[start] != white {
			continue
		}
		stack := []frame{{node: start}}
		color[start] = grey
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.next >= len(adj[top.node]) {
				color[top.node] = black
				stack = stack[:len(stack)-1]
				continue
			}
			child := adj[top.node][top.next]
			top.next++
			switch color[child] {
			case white:
				color[child] = grey
				parent[child] = top.node
				stack = append(stack, frame{node: child})
			case grey:
				cycle := []string{child}
				for n := top.node; n != child; n = parent[n] {
					cycle = append(cycle, n)
				}
				cycle = append(cycle, child)
				for i, j := 0, len(cycle)-1; i < j; i, j = i+1, j-1 {
					cycle[i], cycle[j] = cycle[j], cycle[i]
				}
				return cycle
			}
		}
	}
	return nil
}

// AeroAgentPipelineStatus reports last run state and errors.
type AeroAgentPipelineStatus struct {
	LastRun     string   `json:"last_run,omitempty"`
	State       string   `json:"state,omitempty"`
	ErrorCount  int      `json:"error_count,omitempty"`
	FailedNodes []string `json:"failed_nodes,omitempty"`
	// ObservedGeneration is the metadata.generation last reconciled by the
	// operator; Conditions describe the outcome.
	ObservedGeneration int64       `json:"observedGeneration,omitempty"`
	Conditions         []Condition `json:"conditions,omitempty"`
}

// AeroAgentPipeline represents an agentic DAG workflow resource.
// +kubebuilder:resource:path=aeroagentpipelines
// +kubebuilder:printcolumn:name="Version",type="string",JSONPath=".spec.version"
// +kubebuilder:printcolumn:name="State",type="string",JSONPath=".status.state"
// +kubebuilder:printcolumn:name="Errors",type="int",JSONPath=".status.error_count"
// +kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"
type AeroAgentPipeline struct {
	APIVersion string                  `json:"apiVersion,omitempty"`
	Kind       string                  `json:"kind,omitempty"`
	Metadata   map[string]interface{}  `json:"metadata,omitempty"`
	Spec       AeroAgentPipelineSpec   `json:"spec,omitempty"`
	Status     AeroAgentPipelineStatus `json:"status,omitempty"`
}

// Validate checks the object's kind, metadata and spec.
func (p *AeroAgentPipeline) Validate() error {
	if p == nil {
		return errors.New("aeroagentpipeline: nil object")
	}
	return validateObject(KindAeroAgentPipeline, p.Kind, p.Metadata, p.Spec.Validate())
}
