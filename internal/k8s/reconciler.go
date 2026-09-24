package k8s

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
)

// ResourceKind identifies managed AeroLLM resources.
type ResourceKind string

const (
	KindAeroRoute         ResourceKind = "AeroRoute"
	KindAeroBudget        ResourceKind = "AeroBudget"
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

// msgNoApplyFunc is reported when no ApplyFunc is configured for a kind; the
// resource is NOT applied in that case.
const msgNoApplyFunc = "no apply func configured"

// Reconcile applies a resource using the configured Apply function. Without
// an Apply function nothing is applied and the result says so.
func (r *Reconciler) Reconcile(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return ApplyResult{Kind: kind, Name: name, Message: "cancelled"}, err
		}
	}
	if r == nil || r.Apply == nil {
		return ApplyResult{Kind: kind, Name: name, Applied: false, Message: msgNoApplyFunc}, nil
	}
	return r.Apply(ctx, kind, name, spec)
}

// StatusWriter updates resource status after reconciliation.
type StatusWriter interface {
	UpdateStatus(ctx context.Context, object interface{}, state string) error
}

// ConfigSource emits config updates from ConfigMap/Redis/GRPC.
//
// Run should block until ctx is cancelled or the source is exhausted, and
// must honour ctx when sending on updates. The reconcile loop owns the
// updates channel; a source should not close it (closing is tolerated).
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

// ObjectReconciler reconciles one decoded object.
type ObjectReconciler interface {
	Reconcile(ctx context.Context, object interface{}) error
}

// sourceMsg is either a payload or a terminal error from one source.
type sourceMsg struct {
	source  string
	payload []byte
	err     error
}

// envelope is one object to reconcile, extracted from a payload.
type envelope struct {
	Object interface{}
	State  string
	Source string
}

// RunReconcileLoop watches sources and invokes reconciler for every object
// they emit. Payloads may be:
//
//   - a wrapper: {"object": {...}, "state": "...", "source": "..."}
//   - a bare manifest with a top-level "kind"
//   - a list: {"items": [...]} or a JSON array of either form
//
// Every payload yields at least one ReconcileResult; malformed payloads and
// source failures are reported as results with Error set rather than
// dropped. The returned channel is closed once every source has returned
// (and its buffered payloads have been processed) or ctx is cancelled.
func RunReconcileLoop(ctx context.Context, reconciler ObjectReconciler, writer StatusWriter, sources ...ConfigSource) <-chan ReconcileResult {
	out := make(chan ReconcileResult, 64)
	active := make([]ConfigSource, 0, len(sources))
	for _, s := range sources {
		if s != nil {
			active = append(active, s)
		}
	}
	if reconciler == nil || len(active) == 0 {
		close(out)
		return out
	}
	go func() {
		defer close(out)
		msgs := fanIn(ctx, active)
		for m := range msgs {
			for _, res := range reconcileMessage(ctx, reconciler, writer, m) {
				select {
				case out <- res:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

func reconcileMessage(ctx context.Context, reconciler ObjectReconciler, writer StatusWriter, m sourceMsg) []ReconcileResult {
	if m.err != nil {
		return []ReconcileResult{{Error: fmt.Errorf("config source %s: %w", m.source, m.err), Source: m.source}}
	}
	envs, err := parsePayload(m.payload)
	if err != nil {
		return []ReconcileResult{{Error: fmt.Errorf("config source %s: %w", m.source, err), Source: m.source}}
	}
	results := make([]ReconcileResult, 0, len(envs))
	for _, env := range envs {
		if ctx.Err() != nil {
			return results
		}
		res := ReconcileResult{Object: env.Object, State: env.State, Source: env.Source}
		if res.Source == "" {
			res.Source = m.source
		}
		if env.Object == nil {
			res.Error = errors.New("payload has no object to reconcile")
		} else {
			res.Error = safeReconcile(ctx, reconciler, env.Object)
		}
		if res.Error == nil && writer != nil && env.State != "" {
			res.Error = writer.UpdateStatus(ctx, env.Object, env.State)
		}
		results = append(results, res)
	}
	return results
}

// safeReconcile turns a panicking reconciler into an error so one bad
// object cannot take down the loop.
func safeReconcile(ctx context.Context, r ObjectReconciler, obj interface{}) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("reconciler panic: %v", p)
		}
	}()
	return r.Reconcile(ctx, obj)
}

// parsePayload extracts the objects carried by one payload.
func parsePayload(payload []byte) ([]envelope, error) {
	trimmed := bytes.TrimSpace(payload)
	if len(trimmed) == 0 {
		return nil, errors.New("empty payload")
	}
	if trimmed[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(trimmed, &items); err != nil {
			return nil, fmt.Errorf("invalid JSON payload: %w", err)
		}
		return parseItems(items)
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(trimmed, &top); err != nil {
		return nil, fmt.Errorf("invalid JSON payload: %w", err)
	}
	if raw, ok := top["items"]; ok {
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return nil, fmt.Errorf("invalid items list: %w", err)
		}
		return parseItems(items)
	}
	env, err := parseEnvelope(top)
	if err != nil {
		return nil, err
	}
	return []envelope{env}, nil
}

func parseItems(items []json.RawMessage) ([]envelope, error) {
	envs := make([]envelope, 0, len(items))
	for i, raw := range items {
		var top map[string]json.RawMessage
		if err := json.Unmarshal(raw, &top); err != nil {
			return nil, fmt.Errorf("items[%d]: invalid JSON object: %w", i, err)
		}
		env, err := parseEnvelope(top)
		if err != nil {
			return nil, fmt.Errorf("items[%d]: %w", i, err)
		}
		envs = append(envs, env)
	}
	return envs, nil
}

func parseEnvelope(top map[string]json.RawMessage) (envelope, error) {
	if rawObj, ok := top["object"]; ok {
		var env envelope
		if err := json.Unmarshal(rawObj, &env.Object); err != nil {
			return envelope{}, fmt.Errorf("invalid wrapper object: %w", err)
		}
		if raw, ok := top["state"]; ok {
			if err := json.Unmarshal(raw, &env.State); err != nil {
				return envelope{}, fmt.Errorf("invalid wrapper state: %w", err)
			}
		}
		if raw, ok := top["source"]; ok {
			if err := json.Unmarshal(raw, &env.Source); err != nil {
				return envelope{}, fmt.Errorf("invalid wrapper source: %w", err)
			}
		}
		return env, nil
	}
	if _, ok := top["kind"]; ok {
		obj := make(map[string]interface{}, len(top))
		for k, raw := range top {
			var v interface{}
			if err := json.Unmarshal(raw, &v); err != nil {
				return envelope{}, fmt.Errorf("invalid field %q: %w", k, err)
			}
			obj[k] = v
		}
		return envelope{Object: obj}, nil
	}
	return envelope{}, errors.New(`payload has no "object", "kind" or "items"`)
}

// fanIn runs every source and merges their payloads (and terminal errors)
// into one channel, which is closed once all sources have returned and
// their buffered payloads were forwarded, or ctx is cancelled.
func fanIn(ctx context.Context, sources []ConfigSource) <-chan sourceMsg {
	out := make(chan sourceMsg)
	var wg sync.WaitGroup
	wg.Add(len(sources))
	for _, src := range sources {
		go func(src ConfigSource) {
			defer wg.Done()
			pumpSource(ctx, src, out)
		}(src)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func pumpSource(ctx context.Context, src ConfigSource, out chan<- sourceMsg) {
	name := src.Name()
	ch := make(chan []byte, 32)
	done := make(chan error, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- fmt.Errorf("source panic: %v", p)
			}
		}()
		done <- src.Run(ctx, ch)
	}()
	forward := func(m sourceMsg) bool {
		select {
		case out <- m:
			return true
		case <-ctx.Done():
			return false
		}
	}
	for {
		select {
		case b, ok := <-ch:
			if !ok {
				ch = nil // source closed its own channel; wait for Run to return
				continue
			}
			if !forward(sourceMsg{source: name, payload: b}) {
				return
			}
		case err := <-done:
			// Drain payloads buffered before Run returned.
			for ch != nil {
				select {
				case b, ok := <-ch:
					if !ok {
						ch = nil
						continue
					}
					if !forward(sourceMsg{source: name, payload: b}) {
						return
					}
				default:
					ch = nil
				}
			}
			if err != nil && ctx.Err() == nil {
				forward(sourceMsg{source: name, err: err})
			}
			return
		case <-ctx.Done():
			return
		}
	}
}
