package batch

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// fakeProvider implements providers.Provider.
type fakeProvider struct {
	name     string
	block    bool // block until ctx is done
	fail     error
	panics   bool
	inflight atomic.Int32
	maxSeen  atomic.Int32
	calls    atomic.Int32
}

func (f *fakeProvider) Name() string                 { return f.name }
func (f *fakeProvider) Type() providers.ProviderType { return providers.ProviderLocal }
func (f *fakeProvider) Health() providers.ProviderHealth {
	return providers.ProviderHealth{Name: f.name, Healthy: true}
}
func (f *fakeProvider) Close() error { return nil }

func (f *fakeProvider) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	f.calls.Add(1)
	n := f.inflight.Add(1)
	defer f.inflight.Add(-1)
	for {
		m := f.maxSeen.Load()
		if n <= m || f.maxSeen.CompareAndSwap(m, n) {
			break
		}
	}
	if f.panics {
		panic("boom")
	}
	if f.block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	time.Sleep(2 * time.Millisecond)
	if f.fail != nil {
		return nil, f.fail
	}
	content := "ok:" + req.Model
	return &models.LLMResponse{
		ID:      "chatcmpl-" + req.Model,
		Object:  "chat.completion",
		Model:   req.Model,
		Choices: []models.Choice{{Message: models.Message{Role: models.RoleAssistant, Content: &content}, FinishReason: "stop"}},
		Usage:   &models.Usage{PromptTokens: 3, CompletionTokens: 2, TotalTokens: 5},
	}, nil
}

func line(customID, model string) string {
	return fmt.Sprintf(`{"custom_id":%q,"method":"POST","url":"/v1/chat/completions","body":{"model":%q,"messages":[{"role":"user","content":"hi"}]}}`, customID, model)
}

func jsonl(lines ...string) []byte { return []byte(strings.Join(lines, "\n") + "\n") }

func newProc(t *testing.T, p providers.Provider, cfg BatchProcessorConfig) (*BatchProcessor, *InMemoryStore) {
	t.Helper()
	if cfg.WorkDir == "" {
		cfg.WorkDir = t.TempDir()
	}
	store := NewInMemoryStore()
	proc := NewBatchProcessor(store, func(model string) (providers.Provider, bool) {
		if model == "missing" {
			return nil, false
		}
		return p, true
	}, cfg)
	t.Cleanup(func() { _ = proc.Close() })
	return proc, store
}

func waitStatus(t *testing.T, proc *BatchProcessor, id string, want Status) *Batch {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, err := proc.GetBatch(context.Background(), id)
		if err == nil && b.Status == want {
			return b
		}
		time.Sleep(5 * time.Millisecond)
	}
	b, _ := proc.GetBatch(context.Background(), id)
	t.Fatalf("batch %s did not reach %s (now %+v)", id, want, b)
	return nil
}

func readLines(t *testing.T, data []byte) []BatchResponse {
	t.Helper()
	var out []BatchResponse
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		var r BatchResponse
		if err := json.Unmarshal(sc.Bytes(), &r); err != nil {
			t.Fatalf("invalid output line %q: %v", sc.Text(), err)
		}
		out = append(out, r)
	}
	return out
}

func TestBatchValidation(t *testing.T) {
	proc, _ := newProc(t, &fakeProvider{name: "p"}, BatchProcessorConfig{MaxRequests: 3})
	ctx := context.Background()
	cases := map[string]struct {
		data []byte
		code string
	}{
		"empty":        {[]byte("  \n\n"), "empty_file"},
		"bad json":     {jsonl(`{not json`), "invalid_json_line"},
		"no custom id": {jsonl(strings.Replace(line("a", "m"), `"custom_id":"a",`, "", 1)), "missing_custom_id"},
		"duplicate":    {jsonl(line("a", "m"), line("a", "m")), "duplicate_custom_id"},
		"method":       {jsonl(strings.Replace(line("a", "m"), `"POST"`, `"GET"`, 1)), "invalid_method"},
		"url":          {jsonl(strings.Replace(line("a", "m"), `/v1/chat/completions`, `/v1/embeddings`, 1)), "invalid_url"},
		"body":         {jsonl(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":"x"}`), "invalid_body"},
		"model":        {jsonl(line("a", "")), "missing_model"},
		"messages":     {jsonl(`{"custom_id":"a","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[]}}`), "missing_messages"},
		"too many":     {jsonl(line("a", "m"), line("b", "m"), line("c", "m"), line("d", "m")), "too_many_requests"},
	}
	for name, tc := range cases {
		b, err := proc.CreateBatch(ctx, "f", tc.data)
		if err != nil {
			t.Fatalf("%s: unexpected error %v", name, err)
		}
		if b.Status != StatusFailed || b.FailedAt == nil || len(b.Errors) == 0 {
			t.Fatalf("%s: expected failed batch, got %+v", name, b)
		}
		found := false
		for _, e := range b.Errors {
			if e.Code == tc.code {
				found = true
			}
		}
		if !found {
			t.Fatalf("%s: expected error code %s, got %+v", name, tc.code, b.Errors)
		}
	}
	dup, _ := proc.CreateBatch(ctx, "f", jsonl(line("a", "m"), "", line("a", "m")))
	if dup.Errors[0].Line != 3 || !strings.Contains(dup.Errors[0].Message, "line 1") {
		t.Fatalf("line numbers wrong: %+v", dup.Errors)
	}
	// Legacy "endpoint" alias is accepted.
	legacy := `{"custom_id":"x","method":"post","endpoint":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}`
	if b, _ := proc.CreateBatch(ctx, "f", jsonl(legacy)); b.Status == StatusFailed {
		t.Fatalf("legacy endpoint rejected: %+v", b.Errors)
	}
	if _, err := proc.CreateBatchWithOptions(ctx, CreateOptions{Data: jsonl(line("a", "m")), Metadata: map[string]string{"": "x"}}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("invalid metadata must be rejected: %v", err)
	}
	small, _ := newProc(t, &fakeProvider{name: "p"}, BatchProcessorConfig{MaxInputBytes: 10})
	if _, err := small.CreateBatch(ctx, "f", jsonl(line("a", "m"))); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("oversized input must be rejected: %v", err)
	}
}

func TestBatchProcessingEndToEnd(t *testing.T) {
	p := &fakeProvider{name: "fake"}
	var results []RequestResult
	var mu sync.Mutex
	proc, _ := newProc(t, p, BatchProcessorConfig{
		Concurrency: 3,
		OnResult: func(_ context.Context, r RequestResult) {
			mu.Lock()
			results = append(results, r)
			mu.Unlock()
		},
	})
	var lines []string
	for i := 0; i < 20; i++ {
		lines = append(lines, line(fmt.Sprintf("req-%d", i), "gpt-4o"))
	}
	lines = append(lines, line("bad-model", "missing"))
	ctx := context.Background()
	b, err := proc.CreateBatchWithOptions(ctx, CreateOptions{InputFileID: "../../etc/passwd", Data: jsonl(lines...), Owner: "tenant-a", Metadata: map[string]string{"job": "nightly"}})
	if err != nil {
		t.Fatal(err)
	}
	if !ValidBatchID(b.ID) || strings.Contains(b.InputFileID, "/") {
		t.Fatalf("unsafe identifiers: %q %q", b.ID, b.InputFileID)
	}
	done, err := proc.WaitBatch(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	if done.Status != StatusCompleted || done.CompletedAt == nil || done.InProgressAt == nil || done.FinalizingAt == nil {
		t.Fatalf("unexpected final batch %+v", done)
	}
	if done.RequestCounts != (RequestCounts{Total: 21, Completed: 20, Failed: 1}) || done.CompletedRequests != 20 || done.FailedRequests != 1 {
		t.Fatalf("unexpected counts %+v", done.RequestCounts)
	}
	if max := p.maxSeen.Load(); max > 3 {
		t.Fatalf("concurrency limit exceeded: %d", max)
	}
	if done.Metadata["job"] != "nightly" || done.Owner != "tenant-a" {
		t.Fatalf("metadata/owner lost: %+v", done)
	}

	out, err := proc.ReadResults(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	okLines := readLines(t, out)
	if len(okLines) != 20 {
		t.Fatalf("expected 20 result lines, got %d", len(okLines))
	}
	var body BatchResponseBody
	if err := json.Unmarshal(okLines[0].Response, &body); err != nil || body.StatusCode != 200 || okLines[0].Error != nil || !strings.HasPrefix(okLines[0].ID, "batch_req_") {
		t.Fatalf("bad success line %+v (%v)", okLines[0], err)
	}
	errData, err := proc.ReadErrors(ctx, b.ID)
	if err != nil {
		t.Fatal(err)
	}
	errLines := readLines(t, errData)
	if len(errLines) != 1 || errLines[0].CustomID != "bad-model" || errLines[0].Error.Code != "model_not_found" || string(errLines[0].Response) != "null" {
		t.Fatalf("bad error lines %+v", errLines)
	}

	mu.Lock()
	n := len(results)
	usageOK := 0
	for _, r := range results {
		if r.Usage != nil && r.Usage.TotalTokens == 5 && r.Owner == "tenant-a" {
			usageOK++
		}
	}
	mu.Unlock()
	if n != 21 || usageOK != 20 {
		t.Fatalf("OnResult: %d results, %d with usage", n, usageOK)
	}

	// Files are private and live in the processor's work dir (api/batches.go
	// reads WorkDir()/output_<id>.jsonl).
	outPath := filepath.Join(proc.WorkDir(), "output_"+b.ID+".jsonl")
	st, err := os.Stat(outPath)
	if err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("output file missing or not private: %v %v", st, err)
	}
	dst, _ := os.Stat(proc.WorkDir())
	if dst.Mode().Perm() != 0o700 {
		t.Fatalf("work dir not private: %v", dst.Mode())
	}
}

func TestResolverCalledPerRequest(t *testing.T) {
	p := &fakeProvider{name: "fake"}
	var calls atomic.Int32
	store := NewInMemoryStore()
	proc := NewBatchProcessor(store, nil, BatchProcessorConfig{WorkDir: t.TempDir()})
	defer proc.Close()
	proc.SetResolver(func(model string) (providers.Provider, bool) {
		calls.Add(1)
		return p, true
	})
	b, err := proc.CreateBatch(context.Background(), "f", jsonl(line("a", "m"), line("b", "m"), line("c", "m")))
	if err != nil {
		t.Fatal(err)
	}
	if done, _ := proc.WaitBatch(context.Background(), b.ID); done.Status != StatusCompleted {
		t.Fatalf("unexpected %+v", done)
	}
	if calls.Load() != 3 {
		t.Fatalf("resolver must be called per request, got %d", calls.Load())
	}
}

func TestCancelBatch(t *testing.T) {
	p := &fakeProvider{name: "slow", block: true}
	proc, _ := newProc(t, p, BatchProcessorConfig{Concurrency: 2})
	ctx := context.Background()
	b, err := proc.CreateBatch(ctx, "f", jsonl(line("a", "m"), line("b", "m"), line("c", "m")))
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, proc, b.ID, StatusInProgress)
	cb, err := proc.CancelBatch(ctx, b.ID)
	if err != nil || (cb.Status != StatusCancelling && cb.Status != StatusCancelled) || cb.CancellingAt == nil {
		t.Fatalf("cancel: %+v %v", cb, err)
	}
	done := waitStatus(t, proc, b.ID, StatusCancelled)
	if done.CancelledAt == nil || done.RequestCounts.Completed != 0 {
		t.Fatalf("unexpected cancelled batch %+v", done)
	}
	if _, err := proc.CancelBatch(ctx, b.ID); !errors.Is(err, ErrNotCancellable) {
		t.Fatalf("expected ErrNotCancellable, got %v", err)
	}
	if _, err := proc.ReadResults(ctx, b.ID); err != nil {
		t.Fatalf("partial results of a cancelled batch must be readable: %v", err)
	}
}

func TestBatchExpires(t *testing.T) {
	p := &fakeProvider{name: "slow", block: true}
	proc, _ := newProc(t, p, BatchProcessorConfig{Concurrency: 1})
	ctx := context.Background()
	b, err := proc.CreateBatchWithOptions(ctx, CreateOptions{Data: jsonl(line("a", "m"), line("b", "m")), CompletionWindow: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	done := waitStatus(t, proc, b.ID, StatusExpired)
	if done.ExpiredAt == nil || done.RequestCounts.Failed != 2 {
		t.Fatalf("unexpected expired batch %+v", done.RequestCounts)
	}
	errData, _ := proc.ReadErrors(ctx, b.ID)
	lines := readLines(t, errData)
	if len(lines) != 2 || lines[0].Error.Code != "batch_expired" {
		t.Fatalf("expected batch_expired lines, got %+v", lines)
	}
}

func TestShutdownStopsProcessing(t *testing.T) {
	p := &fakeProvider{name: "slow", block: true}
	dir := t.TempDir()
	store := NewInMemoryStore()
	proc := NewBatchProcessor(store, func(string) (providers.Provider, bool) { return p, true }, BatchProcessorConfig{WorkDir: dir})
	ctx := context.Background()
	b, err := proc.CreateBatch(ctx, "f", jsonl(line("a", "m")))
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, proc, b.ID, StatusInProgress)
	sctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := proc.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}
	final, _ := store.GetBatch(ctx, b.ID)
	if final.Status != StatusFailed || final.FailedAt == nil {
		t.Fatalf("expected failed after shutdown, got %+v", final)
	}
	if _, err := os.Stat(proc.WorkDir()); !os.IsNotExist(err) {
		t.Fatalf("private work dir not removed: %v", err)
	}
	if _, err := proc.CreateBatch(ctx, "f", jsonl(line("a", "m"))); !errors.Is(err, ErrProcessorClosed) && err == nil {
		t.Fatalf("expected error after shutdown, got %v", err)
	}
}

func TestParentContextStopsProcessor(t *testing.T) {
	p := &fakeProvider{name: "slow", block: true}
	parent, cancel := context.WithCancel(context.Background())
	proc, _ := newProc(t, p, BatchProcessorConfig{Context: parent})
	b, err := proc.CreateBatch(context.Background(), "f", jsonl(line("a", "m")))
	if err != nil {
		t.Fatal(err)
	}
	waitStatus(t, proc, b.ID, StatusInProgress)
	cancel()
	waitStatus(t, proc, b.ID, StatusFailed)
}

func TestOwnershipAndListing(t *testing.T) {
	proc, _ := newProc(t, &fakeProvider{name: "p"}, BatchProcessorConfig{})
	ctx := context.Background()
	var ids []string
	for i := 0; i < 5; i++ {
		owner := "a"
		if i%2 == 1 {
			owner = "b"
		}
		b, err := proc.CreateBatchWithOptions(ctx, CreateOptions{Data: jsonl(line("x", "m")), Owner: owner})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, b.ID)
		time.Sleep(2 * time.Millisecond)
	}
	if _, err := proc.GetBatchForOwner(ctx, ids[0], "b"); !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("cross-owner access must look like not found: %v", err)
	}
	if _, err := proc.CancelBatchForOwner(ctx, ids[0], "b"); !errors.Is(err, ErrBatchNotFound) {
		t.Fatalf("cross-owner cancel must fail: %v", err)
	}
	if b, err := proc.GetBatchForOwner(ctx, ids[0], "a"); err != nil || b.ID != ids[0] {
		t.Fatalf("owner access failed: %v", err)
	}
	page, err := proc.ListBatches(ctx, ListOptions{Owner: "a", Limit: 2})
	if err != nil || len(page.Data) != 2 || !page.HasMore || page.Data[0].ID != ids[4] {
		t.Fatalf("unexpected first page %+v %v", page, err)
	}
	page2, _ := proc.ListBatches(ctx, ListOptions{Owner: "a", Limit: 2, After: page.LastID})
	if len(page2.Data) != 1 || page2.HasMore || page2.Data[0].ID != ids[0] {
		t.Fatalf("unexpected second page %+v", page2)
	}
	for _, bad := range []string{"../../etc/passwd", "batch_../../x", "", "batch_ZZZ"} {
		if _, err := proc.GetBatch(ctx, bad); !errors.Is(err, ErrBatchNotFound) {
			t.Fatalf("GetBatch(%q) = %v", bad, err)
		}
		if _, err := proc.OutputPath(bad); err == nil {
			t.Fatalf("OutputPath(%q) must fail", bad)
		}
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	s := NewInMemoryStore()
	ctx := context.Background()
	b := &Batch{ID: "batch_000000000000000000000001", Status: StatusInProgress, Metadata: map[string]string{"k": "v"}}
	_ = s.SaveBatch(ctx, b)
	b.Status = StatusCompleted
	got, _ := s.GetBatch(ctx, b.ID)
	if got.Status != StatusInProgress {
		t.Fatal("store aliased the caller's batch")
	}
	got.Metadata["k"] = "changed"
	again, _ := s.GetBatch(ctx, b.ID)
	if again.Metadata["k"] != "v" {
		t.Fatal("store returned a shared map")
	}
	if err := s.UpdateBatch(ctx, &Batch{ID: "nope"}); !errors.Is(err, ErrBatchNotFound) {
		t.Fatal("update of unknown batch must fail")
	}
}

func TestProviderErrorsAreSanitized(t *testing.T) {
	p := &fakeProvider{name: "p", fail: errors.New(`upstream 401: Authorization: Bearer sk-live-abcdefghijklmnop url=https://x/v1?key=AIzaSECRET"`)}
	proc, _ := newProc(t, p, BatchProcessorConfig{})
	ctx := context.Background()
	b, _ := proc.CreateBatch(ctx, "f", jsonl(line("a", "m")))
	done, _ := proc.WaitBatch(ctx, b.ID)
	if done.Status != StatusCompleted || done.RequestCounts.Failed != 1 {
		t.Fatalf("unexpected %+v", done.RequestCounts)
	}
	data, _ := proc.ReadErrors(ctx, b.ID)
	s := string(data)
	if strings.Contains(s, "abcdefghijklmnop") || strings.Contains(s, "AIzaSECRET") {
		t.Fatalf("secrets leaked into error file: %s", s)
	}
	if !strings.Contains(s, "provider_error") {
		t.Fatalf("missing error code: %s", s)
	}
}

func TestProviderPanicIsContained(t *testing.T) {
	proc, _ := newProc(t, &fakeProvider{name: "p", panics: true}, BatchProcessorConfig{})
	ctx := context.Background()
	b, _ := proc.CreateBatch(ctx, "f", jsonl(line("a", "m")))
	done, _ := proc.WaitBatch(ctx, b.ID)
	if done.Status != StatusCompleted || done.RequestCounts.Failed != 1 {
		t.Fatalf("panic not contained: %+v", done)
	}
}

func TestAdmitHookRejects(t *testing.T) {
	proc, _ := newProc(t, &fakeProvider{name: "p"}, BatchProcessorConfig{
		Admit: func(_ context.Context, b *Batch, req *models.LLMRequest) error {
			if req.Model == "expensive" {
				return errors.New("budget exceeded")
			}
			return nil
		},
	})
	ctx := context.Background()
	b, _ := proc.CreateBatch(ctx, "f", jsonl(line("a", "cheap"), line("b", "expensive")))
	done, _ := proc.WaitBatch(ctx, b.ID)
	if done.RequestCounts.Completed != 1 || done.RequestCounts.Failed != 1 {
		t.Fatalf("unexpected counts %+v", done.RequestCounts)
	}
	data, _ := proc.ReadErrors(ctx, b.ID)
	if !strings.Contains(string(data), "request_rejected") {
		t.Fatalf("missing rejection: %s", data)
	}
}

func TestResultsNotReadyWhileRunning(t *testing.T) {
	proc, _ := newProc(t, &fakeProvider{name: "p", block: true}, BatchProcessorConfig{})
	ctx := context.Background()
	b, _ := proc.CreateBatch(ctx, "f", jsonl(line("a", "m")))
	waitStatus(t, proc, b.ID, StatusInProgress)
	if _, err := proc.ReadResults(ctx, b.ID); !errors.Is(err, ErrResultsNotReady) {
		t.Fatalf("expected ErrResultsNotReady, got %v", err)
	}
	_, _ = proc.CancelBatch(ctx, b.ID)
	waitStatus(t, proc, b.ID, StatusCancelled)
}

func TestCleanupRemovesOldBatches(t *testing.T) {
	proc, store := newProc(t, &fakeProvider{name: "p"}, BatchProcessorConfig{})
	ctx := context.Background()
	b, _ := proc.CreateBatch(ctx, "f", jsonl(line("a", "m")))
	_, _ = proc.WaitBatch(ctx, b.ID)
	n, err := proc.Cleanup(ctx, time.Now().Add(time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("cleanup removed %d (%v)", n, err)
	}
	if _, err := store.GetBatch(ctx, b.ID); !errors.Is(err, ErrBatchNotFound) {
		t.Fatal("record not deleted")
	}
	p, _ := proc.OutputPath(b.ID)
	if _, err := os.Stat(p); !os.IsNotExist(err) {
		t.Fatal("output file not deleted")
	}
}
