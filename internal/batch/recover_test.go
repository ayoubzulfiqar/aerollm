package batch

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/persist"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// modelProvider answers "fast" models immediately and blocks "slow" ones
// until their context ends.
type modelProvider struct {
	total atomic.Int32
}

func (m *modelProvider) Name() string                 { return "model-provider" }
func (m *modelProvider) Type() providers.ProviderType { return providers.ProviderLocal }
func (m *modelProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: "model-provider", Healthy: true}
}
func (m *modelProvider) Close() error { return nil }

func (m *modelProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	m.total.Add(1)
	if req.Model == "slow" {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	content := "ok"
	return &models.LLMResponse{ID: "chatcmpl-x", Object: "chat.completion", Model: req.Model,
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: &content}, FinishReason: "stop"}},
		Usage:   &models.Usage{PromptTokens: 1, CompletionTokens: 1, TotalTokens: 2}}, nil
}

func resolverFor(p providers.Provider) ProviderResolver {
	return func(string) (providers.Provider, bool) { return p, true }
}

func customIDs(t *testing.T, data []byte) []string {
	t.Helper()
	var ids []string
	for _, l := range readLines(t, data) {
		ids = append(ids, l.CustomID)
	}
	sort.Strings(ids)
	return ids
}

func TestPersistentStoreRoundTrip(t *testing.T) {
	ps := persist.NewMemory()
	s, err := NewPersistentStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	b := &Batch{ID: "batch_" + strings.Repeat("a", 24), Status: StatusInProgress, Owner: "key-1",
		Metadata: map[string]string{"k": "v"}, CreatedAt: time.Now().UTC()}
	if err := s.SaveBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	b.Status = StatusCompleted
	if err := s.UpdateBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateBatch(ctx, &Batch{ID: "missing"}); !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("update of unknown batch: %v", err)
	}
	s2, err := NewPersistentStore(ps)
	if err != nil {
		t.Fatal(err)
	}
	got, err := s2.GetBatch(ctx, b.ID)
	if err != nil || got.Status != StatusCompleted || got.Owner != "key-1" || got.Metadata["k"] != "v" {
		t.Fatalf("reloaded batch: %+v %v", got, err)
	}
	if list, _ := s2.ListBatches(ctx); len(list) != 1 {
		t.Fatalf("list: %d", len(list))
	}
	if err := s2.DeleteBatch(ctx, b.ID); err != nil {
		t.Fatal(err)
	}
	if s3, _ := NewPersistentStore(ps); len(s3.mem.batches) != 0 {
		t.Fatal("deleted batch must be gone after reload")
	}
	_ = ps.Put(BatchBucket, "bogus", "not a batch")
	if s4, err := NewPersistentStore(ps); err == nil || s4 == nil {
		t.Fatalf("undecodable records must be reported but the store returned: %v", err)
	}
}

// failingPersist fails every write.
type failingPersist struct{ persist.Store }

func (failingPersist) Put(string, string, any) error { return errors.New("disk full") }

func TestPersistentStoreWriteFailureSurfaces(t *testing.T) {
	s, err := NewPersistentStore(failingPersist{persist.NewMemory()})
	if err != nil {
		t.Fatal(err)
	}
	b := &Batch{ID: "batch_" + strings.Repeat("b", 24)}
	if err := s.SaveBatch(context.Background(), b); err == nil {
		t.Fatal("write failure must be returned")
	}
	if _, err := s.GetBatch(context.Background(), b.ID); !errors.Is(err, ErrBatchNotFound) {
		t.Fatal("failed save must not be visible")
	}
}

// Graceful restart: with SuspendOnShutdown the interrupted batch stays in
// progress; the next process resumes it and only runs the requests that
// have no result yet. Results of both runs end up in one output file.
func TestRecoverResumesSuspendedBatchAfterRestart(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "state.db")
	work := filepath.Join(dir, "batches")
	ctx := context.Background()

	db, err := persist.OpenBolt(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	store, err := NewPersistentStore(db)
	if err != nil {
		t.Fatal(err)
	}
	p1 := &modelProvider{}
	cfg := BatchProcessorConfig{WorkDir: work, PersistentWorkDir: true, SuspendOnShutdown: true, Concurrency: 2}
	proc1 := NewBatchProcessor(store, resolverFor(p1), cfg)
	var lines []string
	for i := 0; i < 4; i++ {
		lines = append(lines, line("fast-"+string(rune('a'+i)), "fast"))
	}
	for i := 0; i < 3; i++ {
		lines = append(lines, line("slow-"+string(rune('a'+i)), "slow"))
	}
	b, err := proc1.CreateBatchWithOptions(ctx, CreateOptions{Data: jsonl(lines...), Owner: "tenant-a", Metadata: map[string]string{"job": "nightly"}})
	if err != nil {
		t.Fatal(err)
	}
	// The persisted progress catches up even though the remaining
	// requests block (trailing progress save).
	deadline := time.Now().Add(5 * time.Second)
	for {
		cur, _ := proc1.GetBatch(ctx, b.ID)
		if cur.RequestCounts.Completed == 4 && p1.total.Load() == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("fast requests did not complete: %+v", cur)
		}
		time.Sleep(5 * time.Millisecond)
	}
	if err := proc1.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	suspended, _ := store.GetBatch(ctx, b.ID)
	if suspended.Status != StatusInProgress || suspended.Error != "" {
		t.Fatalf("suspended batch must stay in progress: %+v", suspended)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart: new store over the same file, new processor, same work dir.
	db2, err := persist.OpenBolt(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	store2, err := NewPersistentStore(db2)
	if err != nil {
		t.Fatal(err)
	}
	p2 := &modelProvider{}
	fastOnly := resolverFor(p2)
	var results atomic.Int32
	cfg2 := cfg
	cfg2.OnResult = func(context.Context, RequestResult) { results.Add(1) }
	proc2 := NewBatchProcessor(store2, func(string) (providers.Provider, bool) { return fastOnly("fast") }, cfg2)
	defer proc2.Close()
	// The slow lines now succeed: rewrite their model through Admit.
	proc2.cfg.Admit = func(_ context.Context, _ *Batch, req *models.LLMRequest) error {
		req.Model = "fast"
		return nil
	}
	rep, err := proc2.Recover(ctx)
	if err != nil || len(rep.Resumed) != 1 || rep.Resumed[0] != b.ID {
		t.Fatalf("recover: %+v %v", rep, err)
	}
	if rep2, err := proc2.Recover(ctx); err != nil || len(rep2.Resumed) != 0 {
		t.Fatalf("a running batch must not be resumed twice: %+v %v", rep2, err)
	}
	done, err := proc2.WaitBatch(ctx, b.ID)
	if err != nil || done.Status != StatusCompleted {
		t.Fatalf("resumed batch: %+v %v", done, err)
	}
	if done.RequestCounts != (RequestCounts{Total: 7, Completed: 7}) || done.Owner != "tenant-a" || done.Metadata["job"] != "nightly" {
		t.Fatalf("unexpected final batch %+v", done)
	}
	if got := p2.total.Load(); got != 3 {
		t.Fatalf("only the 3 unfinished requests may run again, ran %d", got)
	}
	if results.Load() != 3 {
		t.Fatalf("OnResult must fire for resumed requests only, got %d", results.Load())
	}
	out, err := proc2.ReadResults(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	ids := customIDs(t, out)
	want := []string{"fast-a", "fast-b", "fast-c", "fast-d", "slow-a", "slow-b", "slow-c"}
	if strings.Join(ids, ",") != strings.Join(want, ",") {
		t.Fatalf("output custom ids %v, want %v", ids, want)
	}
	if owned, err := proc2.GetBatchForOwner(ctx, b.ID, "tenant-a"); err != nil || owned.ID != b.ID {
		t.Fatalf("owner must survive the restart: %v", err)
	}
}

// writeFile writes a private file for crash fixtures.
func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func resultLine(t *testing.T, customID string, failed bool) string {
	t.Helper()
	r := BatchResponse{ID: "batch_req_x", CustomID: customID, Response: json.RawMessage(`{"status_code":200,"request_id":"r","body":{}}`)}
	if failed {
		r.Response = json.RawMessage("null")
		r.Error = &BatchError{Code: "provider_error", Message: "boom"}
	}
	b, _ := json.Marshal(r)
	return string(b) + "\n"
}

// Crash: the process died mid-run (no shutdown bookkeeping). The store says
// in_progress with stale counts and the output file ends with a torn line.
func TestRecoverAfterCrashSkipsFinishedRequests(t *testing.T) {
	work := t.TempDir()
	ctx := context.Background()
	store, _ := NewPersistentStore(persist.NewMemory())
	id := "batch_" + strings.Repeat("c", 24)
	now := time.Now().UTC()
	expires := now.Add(time.Hour)
	b := &Batch{ID: id, Object: "batch", Endpoint: EndpointChatCompletions, Status: StatusInProgress, CreatedAt: now, InProgressAt: &now,
		ExpiresAt: &expires, RequestCounts: RequestCounts{Total: 4, Completed: 1}, Owner: "o"}
	if err := store.SaveBatch(ctx, b); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(work, "input_"+id+".jsonl"), string(jsonl(line("a", "m"), line("b", "m"), line("c", "m"), line("d", "m"))))
	torn := resultLine(t, "c", false)
	writeFile(t, filepath.Join(work, "output_"+id+".jsonl"), resultLine(t, "a", false)+torn[:len(torn)/2])
	writeFile(t, filepath.Join(work, "errors_"+id+".jsonl"), resultLine(t, "b", true))

	p := &modelProvider{}
	proc := NewBatchProcessor(store, resolverFor(p), BatchProcessorConfig{WorkDir: work, PersistentWorkDir: true})
	defer proc.Close()
	rep, err := proc.Recover(ctx)
	if err != nil || len(rep.Resumed) != 1 {
		t.Fatalf("recover: %+v %v", rep, err)
	}
	done, err := proc.WaitBatch(ctx, id)
	if err != nil || done.Status != StatusCompleted {
		t.Fatalf("resumed: %+v %v", done, err)
	}
	if done.RequestCounts != (RequestCounts{Total: 4, Completed: 3, Failed: 1}) {
		t.Fatalf("counts must be rebuilt from the files: %+v", done.RequestCounts)
	}
	if p.total.Load() != 2 {
		t.Fatalf("only c and d may run again, ran %d", p.total.Load())
	}
	out, _ := proc.ReadResults(ctx, id)
	if ids := customIDs(t, out); strings.Join(ids, ",") != "a,c,d" {
		t.Fatalf("torn line must be replaced, got %v", ids)
	}
	errs, _ := proc.ReadErrors(ctx, id)
	if ids := customIDs(t, errs); strings.Join(ids, ",") != "b" {
		t.Fatalf("error file: %v", ids)
	}
}

func TestRecoverFailsBatchWithoutInput(t *testing.T) {
	ctx := context.Background()
	store, _ := NewPersistentStore(persist.NewMemory())
	now := time.Now().UTC()
	ids := []string{"batch_" + strings.Repeat("d", 24), "batch_" + strings.Repeat("e", 24), "batch_" + strings.Repeat("f", 24)}
	_ = store.SaveBatch(ctx, &Batch{ID: ids[0], Status: StatusInProgress, CreatedAt: now, RequestCounts: RequestCounts{Total: 2}})
	_ = store.SaveBatch(ctx, &Batch{ID: ids[1], Status: StatusCancelling, CreatedAt: now.Add(time.Second)})
	_ = store.SaveBatch(ctx, &Batch{ID: ids[2], Status: StatusCompleted, CreatedAt: now.Add(2 * time.Second)})

	// Default (non-persistent) work dir: files of the previous process are gone.
	proc := NewBatchProcessor(store, resolverFor(&modelProvider{}), BatchProcessorConfig{WorkDir: t.TempDir()})
	defer proc.Close()
	rep, err := proc.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Failed) != 1 || rep.Failed[0] != ids[0] || len(rep.Cancelled) != 1 || rep.Cancelled[0] != ids[1] || len(rep.Resumed) != 0 {
		t.Fatalf("unexpected report %+v", rep)
	}
	failed, _ := store.GetBatch(ctx, ids[0])
	if failed.Status != StatusFailed || failed.FailedAt == nil || !strings.Contains(failed.Error, "interrupted by a gateway restart") ||
		len(failed.Errors) != 1 || failed.Errors[0].Code != "interrupted" {
		t.Fatalf("unexpected failed batch %+v", failed)
	}
	if _, err := proc.ReadResults(ctx, ids[0]); !errors.Is(err, ErrResultsNotReady) {
		t.Fatalf("no results for a batch without files: %v", err)
	}
	cancelled, _ := store.GetBatch(ctx, ids[1])
	if cancelled.Status != StatusCancelled || cancelled.CancelledAt == nil {
		t.Fatalf("unexpected cancelled batch %+v", cancelled)
	}
	if done, _ := store.GetBatch(ctx, ids[2]); done.Status != StatusCompleted {
		t.Fatal("finished batches must be untouched")
	}
}

func TestRecoverExpiredBatchFinishesExpired(t *testing.T) {
	work := t.TempDir()
	ctx := context.Background()
	store, _ := NewPersistentStore(persist.NewMemory())
	id := "batch_" + strings.Repeat("0", 24)
	created := time.Now().UTC().Add(-2 * time.Hour)
	expired := created.Add(time.Hour)
	_ = store.SaveBatch(ctx, &Batch{ID: id, Status: StatusValidating, CreatedAt: created, ExpiresAt: &expired, RequestCounts: RequestCounts{Total: 2}})
	writeFile(t, filepath.Join(work, "input_"+id+".jsonl"), string(jsonl(line("a", "m"), line("b", "m"))))
	p := &modelProvider{}
	proc := NewBatchProcessor(store, resolverFor(p), BatchProcessorConfig{WorkDir: work, PersistentWorkDir: true})
	defer proc.Close()
	if rep, err := proc.Recover(ctx); err != nil || len(rep.Resumed) != 1 {
		t.Fatalf("recover: %+v %v", rep, err)
	}
	done, err := proc.WaitBatch(ctx, id)
	if err != nil || done.Status != StatusExpired || done.RequestCounts.Failed != 2 {
		t.Fatalf("expected expired batch: %+v %v", done, err)
	}
	if p.total.Load() != 0 {
		t.Fatalf("expired batch must not execute requests, ran %d", p.total.Load())
	}
	errs, _ := proc.ReadErrors(ctx, id)
	for _, l := range readLines(t, errs) {
		if l.Error == nil || l.Error.Code != "batch_expired" {
			t.Fatalf("unexpected error line %+v", l)
		}
	}
}

func TestSuspendOnShutdownRequiresFlag(t *testing.T) {
	// Without SuspendOnShutdown the historical behaviour is kept.
	p := &modelProvider{}
	store, _ := NewPersistentStore(persist.NewMemory())
	proc := NewBatchProcessor(store, resolverFor(p), BatchProcessorConfig{WorkDir: t.TempDir(), PersistentWorkDir: true})
	ctx := context.Background()
	b, err := proc.CreateBatch(ctx, "f", jsonl(line("s", "slow")))
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, proc, b.ID, StatusInProgress)
	if err := proc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if got, _ := store.GetBatch(ctx, b.ID); got.Status != StatusFailed {
		t.Fatalf("expected failed, got %+v", got)
	}
}
