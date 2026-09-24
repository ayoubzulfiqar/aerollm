package k8s

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type recordingReconciler struct {
	mu      sync.Mutex
	objects []interface{}
	err     error
}

func (r *recordingReconciler) Reconcile(ctx context.Context, object interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.objects = append(r.objects, object)
	return r.err
}

type funcSource struct {
	name string
	run  func(ctx context.Context, updates chan<- []byte) error
}

func (f *funcSource) Run(ctx context.Context, updates chan<- []byte) error {
	return f.run(ctx, updates)
}
func (f *funcSource) Name() string { return f.name }

type statusRecorder struct {
	mu     sync.Mutex
	states []string
}

func (s *statusRecorder) UpdateStatus(ctx context.Context, object interface{}, state string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states = append(s.states, state)
	return nil
}

// collect drains results until the channel closes or the timeout elapses.
func collect(t *testing.T, ch <-chan ReconcileResult) []ReconcileResult {
	t.Helper()
	var out []ReconcileResult
	timeout := time.After(5 * time.Second)
	for {
		select {
		case r, ok := <-ch:
			if !ok {
				return out
			}
			out = append(out, r)
		case <-timeout:
			t.Fatalf("result channel not closed; got %d results so far", len(out))
		}
	}
}

func TestRunReconcileLoopPayloadForms(t *testing.T) {
	rec := &recordingReconciler{}
	writer := &statusRecorder{}
	src := NewInMemoryConfigSource(
		[]byte(`{"object":{"kind":"AeroRoute","metadata":{"name":"r1"}},"state":"ready","source":"cm"}`),
		[]byte(`{"kind":"AeroBudget","metadata":{"name":"b1"},"spec":{"max_usd":5}}`),
		[]byte(`{"items":[{"kind":"AeroRoute","metadata":{"name":"r2"}},{"object":{"kind":"AeroRoute","metadata":{"name":"r3"}}}]}`),
		[]byte(`[{"kind":"AeroRoute","metadata":{"name":"r4"}}]`),
		[]byte(`not json`),
		[]byte(`{}`),
		[]byte(`{"object":null}`),
	)
	results := collect(t, RunReconcileLoop(context.Background(), rec, writer, src))
	if len(results) != 8 {
		t.Fatalf("expected 8 results, got %d: %+v", len(results), results)
	}
	if results[0].Error != nil || results[0].State != "ready" || results[0].Source != "cm" {
		t.Fatalf("wrapper result wrong: %+v", results[0])
	}
	if results[1].Error != nil || results[1].Source != "inmemory" {
		t.Fatalf("bare manifest result wrong (source should default to source name): %+v", results[1])
	}
	for i := 2; i <= 4; i++ {
		if results[i].Error != nil {
			t.Fatalf("list item %d failed: %v", i, results[i].Error)
		}
	}
	for i := 5; i <= 7; i++ {
		if results[i].Error == nil {
			t.Fatalf("result %d: malformed payload must be reported, got %+v", i, results[i])
		}
	}
	if len(rec.objects) != 5 {
		t.Fatalf("expected 5 reconciled objects, got %d", len(rec.objects))
	}
	if len(writer.states) != 1 || writer.states[0] != "ready" {
		t.Fatalf("expected one status update, got %v", writer.states)
	}
}

func TestRunReconcileLoopReportsSourceErrors(t *testing.T) {
	boom := errors.New("boom")
	src := &funcSource{name: "bad", run: func(ctx context.Context, updates chan<- []byte) error { return boom }}
	results := collect(t, RunReconcileLoop(context.Background(), &recordingReconciler{}, nil, src))
	if len(results) != 1 || !errors.Is(results[0].Error, boom) || results[0].Source != "bad" {
		t.Fatalf("expected source error result, got %+v", results)
	}
}

func TestRunReconcileLoopToleratesSourceClosingChannel(t *testing.T) {
	src := &funcSource{name: "closer", run: func(ctx context.Context, updates chan<- []byte) error {
		updates <- []byte(`{"kind":"AeroRoute","metadata":{"name":"r1"}}`)
		close(updates)
		return nil
	}}
	results := collect(t, RunReconcileLoop(context.Background(), &recordingReconciler{}, nil, src))
	if len(results) != 1 || results[0].Error != nil {
		t.Fatalf("expected one clean result, got %+v", results)
	}
}

func TestRunReconcileLoopRecoversPanics(t *testing.T) {
	src := &funcSource{name: "panicky", run: func(ctx context.Context, updates chan<- []byte) error {
		panic("source bug")
	}}
	results := collect(t, RunReconcileLoop(context.Background(), &recordingReconciler{}, nil, src))
	if len(results) != 1 || results[0].Error == nil || !strings.Contains(results[0].Error.Error(), "panic") {
		t.Fatalf("expected panic reported as error, got %+v", results)
	}
}

func TestRunReconcileLoopDrainsBufferedPayloads(t *testing.T) {
	src := &funcSource{name: "burst", run: func(ctx context.Context, updates chan<- []byte) error {
		for i := 0; i < 20; i++ {
			updates <- []byte(`{"kind":"AeroRoute","metadata":{"name":"r1"}}`)
		}
		return nil
	}}
	rec := &recordingReconciler{}
	results := collect(t, RunReconcileLoop(context.Background(), rec, nil, src))
	if len(results) != 20 {
		t.Fatalf("expected all 20 buffered payloads to be processed, got %d", len(results))
	}
}

func TestRunReconcileLoopStopsOnCancel(t *testing.T) {
	var running atomic.Int32
	src := &funcSource{name: "forever", run: func(ctx context.Context, updates chan<- []byte) error {
		running.Add(1)
		defer running.Add(-1)
		<-ctx.Done()
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	out := RunReconcileLoop(ctx, &recordingReconciler{}, nil, src, src)
	time.Sleep(20 * time.Millisecond)
	cancel()
	collect(t, out)
	deadline := time.Now().Add(2 * time.Second)
	for running.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if running.Load() != 0 {
		t.Fatal("sources still running after cancel")
	}
}

func TestRunReconcileLoopNilInputs(t *testing.T) {
	if _, ok := <-RunReconcileLoop(context.Background(), nil, nil, NewInMemoryConfigSource()); ok {
		t.Fatal("expected closed channel for nil reconciler")
	}
	if _, ok := <-RunReconcileLoop(context.Background(), &recordingReconciler{}, nil); ok {
		t.Fatal("expected closed channel without sources")
	}
	if _, ok := <-RunReconcileLoop(context.Background(), &recordingReconciler{}, nil, nil); ok {
		t.Fatal("expected closed channel with only nil sources")
	}
}

func TestRunReconcileLoopWithManifestReconciler(t *testing.T) {
	var applies atomic.Int32
	m := &ManifestReconciler{RouterApply: func(ctx context.Context, kind ResourceKind, name string, spec map[string]interface{}) (ApplyResult, error) {
		applies.Add(1)
		return ApplyResult{Kind: kind, Name: name, Applied: true}, nil
	}}
	payload := []byte(`{"kind":"AeroRoute","metadata":{"name":"r1"},"spec":{"strategy":"cost"}}`)
	bad := []byte(`{"kind":"AeroRoute","metadata":{"name":"r1"},"spec":{"strategy":"bogus"}}`)
	results := collect(t, RunReconcileLoop(context.Background(), m, nil, NewInMemoryConfigSource(payload, payload, bad)))
	if len(results) != 3 || results[0].Error != nil || results[1].Error != nil || results[2].Error == nil {
		t.Fatalf("unexpected results: %+v", results)
	}
	if applies.Load() != 1 {
		t.Fatalf("identical spec must be applied once, got %d", applies.Load())
	}
}
