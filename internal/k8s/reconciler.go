package k8s

import (
	"context"
	"encoding/json"
	"sync"
)

// ResourceKind identifies managed AeroLLM resources.
type ResourceKind string

const (
	KindAeroRoute        ResourceKind = "AeroRoute"
	KindAeroBudget       ResourceKind = "AeroBudget"
	KindAeroAgentPipeline ResourceKind = "AeroAgentPipeline"
)

// ApplyResult represents the outcome of applying a resource spec.
type ApplyResult struct {
	Kind    ResourceKind
	Name    string
	Applied bool
	Message string
}

// Reconciler is a struct-based reconciler for compatibility with existing operator code.
type Reconciler struct {
	Apply ApplyFunc
}

// ApplyFunc applies a control-plane resource spec.
type ApplyFunc func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error)

// Reconcile applies a resource using the configured Apply function.
func (r *Reconciler) Reconcile(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
	if r == nil || r.Apply == nil {
		return ApplyResult{}, nil
	}
	return r.Apply(ctx, kind, name, spec)
}

// StatusWriter updates resource status after reconciliation.
type StatusWriter interface {
	UpdateStatus(ctx context.Context, object interface{}, state string) error
}

// ConfigSource emits config updates from ConfigMap/Redis/GRPC.
type ConfigSource interface {
	Run(ctx context.Context, updates chan<- []byte) error
	Name() string
}

// ReconcileResult represents reconciliation outcome.
type ReconcileResult struct {
	Object interface{}
	Error  error
	State  string
	Source string
}

// RunReconcileLoop watches sources and invokes reconciler with context cancellation.
func RunReconcileLoop(ctx context.Context, reconciler interface{ Reconcile(ctx context.Context, object interface{}) error }, writer StatusWriter, sources ...ConfigSource) <-chan ReconcileResult {
	out := make(chan ReconcileResult, 64)
	if reconciler == nil || len(sources) == 0 {
		close(out)
		return out
	}
	go func() {
		defer close(out)
		updateChs := make([]chan []byte, len(sources))
		for i, src := range sources {
			updateChs[i] = make(chan []byte, 32)
			go func(s ConfigSource, ch chan []byte) {
				_ = s.Run(ctx, ch)
			}(src, updateChs[i])
		}
		merged := fanIn(ctx, updateChs...)
		for payload := range merged {
			var wrapper struct {
				Object interface{} `json:"object"`
				State  string      `json:"state"`
				Source string      `json:"source"`
			}
			if err := json.Unmarshal(payload, &wrapper); err != nil {
				continue
			}
			result := ReconcileResult{
				Object: wrapper.Object,
				Error:  reconciler.Reconcile(ctx, wrapper.Object),
				State:  wrapper.State,
				Source: wrapper.Source,
			}
			if result.Error == nil && writer != nil && wrapper.State != "" {
				if uerr := writer.UpdateStatus(ctx, wrapper.Object, wrapper.State); uerr != nil && result.Error == nil {
					result.Error = uerr
				}
			}
			select {
			case out <- result:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

func fanIn(ctx context.Context, chs ...chan []byte) <-chan []byte {
	out := make(chan []byte)
	var wg sync.WaitGroup
	propagate := func(ch chan []byte) {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- msg:
				case <-ctx.Done():
					return
				}
			}
		}
	}
	wg.Add(len(chs))
	for _, ch := range chs {
		go propagate(ch)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
