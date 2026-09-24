package learning

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

// newRatedExporter returns an exporter over one ledger record rated "up".
func newRatedExporter(t *testing.T) (*flywheel.DatasetExporter, ledger.LedgerStore) {
	t.Helper()
	store := ledger.NewInMemoryLedgerStore()
	_ = store.Append(context.Background(), ledger.LedgerRecord{
		RequestPayload:  `{"prompt":"hello"}`,
		ResponsePayload: `{"id":"chatcmpl-1","text":"world"}`,
	})
	fb := flywheel.NewFeedbackExporter(store)
	if err := fb.Add(flywheel.FeedbackRecord{RequestID: "chatcmpl-1", Rating: "up"}); err != nil {
		t.Fatal(err)
	}
	return &flywheel.DatasetExporter{Ledger: store, Feedback: fb}, store
}

func TestEnqueueAndStatus(t *testing.T) {
	exporter, store := newRatedExporter(t)
	trainer := NewTrainer(exporter, store, t.TempDir())
	job, err := trainer.Enqueue(context.Background(), "model-a", "up")
	if err != nil {
		t.Fatalf("enqueue failed: %v", err)
	}
	if job.Model != "model-a" || job.Status != JobStatusQueued {
		t.Fatalf("unexpected job: %+v", job)
	}
	if !strings.Contains(job.Dataset, `"rating":"up"`) {
		t.Fatalf("dataset should contain rated records: %s", job.Dataset)
	}
	got, ok := trainer.Status(job.ID)
	if !ok || got.ID != job.ID {
		t.Fatal("expected job to be retrievable")
	}
	if err := trainer.Cancel(job.ID); err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	if got, _ := trainer.Status(job.ID); got.Status != JobStatusCancelled {
		t.Fatalf("expected cancelled, got %s", got.Status)
	}
	if err := trainer.Cancel(job.ID); err == nil {
		t.Fatal("cancelling twice should fail")
	}
	if err := trainer.Cancel("missing"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestEnqueueNoMatchingRatings(t *testing.T) {
	exporter, store := newRatedExporter(t)
	trainer := NewTrainer(exporter, store, t.TempDir())
	if _, err := trainer.Enqueue(context.Background(), "m", "down"); err == nil {
		t.Fatal("expected error when no records have the requested rating")
	}
}

func TestEnqueueWithoutFeedbackSourceFails(t *testing.T) {
	store := ledger.NewInMemoryLedgerStore()
	_ = store.Append(context.Background(), ledger.LedgerRecord{RequestPayload: "a", ResponsePayload: "b"})
	trainer := NewTrainer(&flywheel.DatasetExporter{Ledger: store}, store, t.TempDir())
	if _, err := trainer.Enqueue(context.Background(), "m", "up"); !errors.Is(err, flywheel.ErrNoFeedbackSource) {
		t.Fatalf("expected ErrNoFeedbackSource, got %v", err)
	}
}

func TestNilGuards(t *testing.T) {
	var nilTrainer *Trainer
	if _, err := nilTrainer.Enqueue(context.Background(), "m", "up"); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("expected ErrNotInitialized, got %v", err)
	}
	if _, err := nilTrainer.WriteDataset(context.Background(), "up", "x.jsonl"); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("expected ErrNotInitialized, got %v", err)
	}
	trainer := NewTrainer(nil, nil, t.TempDir())
	if _, err := trainer.Enqueue(context.Background(), "m", "up"); !errors.Is(err, ErrNotInitialized) {
		t.Fatalf("nil exporter should error, got %v", err)
	}
	if nilTrainer.Jobs() != nil {
		t.Fatal("nil trainer jobs should be nil")
	}
}

func TestEnqueueUniqueIDsAndBoundedJobs(t *testing.T) {
	exporter, store := newRatedExporter(t)
	trainer := NewTrainer(exporter, store, t.TempDir())
	var wg sync.WaitGroup
	ids := make(chan string, 200)
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			job, err := trainer.Enqueue(context.Background(), "m", "up")
			if err != nil {
				t.Errorf("enqueue: %v", err)
				return
			}
			ids <- job.ID
		}()
	}
	wg.Wait()
	close(ids)
	seen := map[string]bool{}
	for id := range ids {
		if seen[id] {
			t.Fatalf("duplicate job id %s", id)
		}
		seen[id] = true
	}
	if len(trainer.Jobs()) != 200 {
		t.Fatalf("expected 200 jobs, got %d", len(trainer.Jobs()))
	}

	for i := 0; i < MaxJobs+10; i++ {
		if _, err := trainer.Enqueue(context.Background(), "m", "up"); err != nil {
			t.Fatal(err)
		}
	}
	if n := len(trainer.Jobs()); n != MaxJobs {
		t.Fatalf("jobs not bounded: %d", n)
	}
}

func TestWriteDataset(t *testing.T) {
	exporter, store := newRatedExporter(t)
	dir := t.TempDir()
	trainer := NewTrainer(exporter, store, dir)
	path, err := trainer.WriteDataset(context.Background(), "up", "ds.jsonl")
	if err != nil {
		t.Fatalf("write dataset failed: %v", err)
	}
	if path != filepath.Join(dir, "ds.jsonl") {
		t.Fatalf("unexpected path %q", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("dataset not written: %v", err)
	}
	if runtime.GOOS != "windows" && info.Mode().Perm() != 0o600 {
		t.Fatalf("dataset should be 0600, got %v", info.Mode().Perm())
	}
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "hello") {
		t.Fatalf("unexpected contents %q", b)
	}
	// No temp files are left behind.
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Fatalf("expected only the dataset file, got %d entries", len(entries))
	}

	// Refuses to overwrite.
	if _, err := trainer.WriteDataset(context.Background(), "up", "ds.jsonl"); !errors.Is(err, ErrFileExists) {
		t.Fatalf("expected ErrFileExists, got %v", err)
	}

	// Generated names are valid and unique.
	p1, err := trainer.WriteDataset(context.Background(), "up", "")
	if err != nil {
		t.Fatalf("generated name failed: %v", err)
	}
	p2, err := trainer.WriteDataset(context.Background(), "up", "")
	if err != nil || p1 == p2 {
		t.Fatalf("generated names should be unique: %q %q %v", p1, p2, err)
	}
}

func TestWriteDatasetRejectsTraversal(t *testing.T) {
	exporter, store := newRatedExporter(t)
	parent := t.TempDir()
	dir := filepath.Join(parent, "out")
	trainer := NewTrainer(exporter, store, dir)
	bad := []string{
		"../escape.jsonl",
		"../../etc/cron.d/x.jsonl",
		"/tmp/abs.jsonl",
		"sub/dir.jsonl",
		`sub\dir.jsonl`,
		"..",
		".",
		".hidden.jsonl",
		"noext",
		".jsonl",
		"bad name.jsonl",
		strings.Repeat("a", MaxDatasetFilenameLength) + ".jsonl",
	}
	for _, name := range bad {
		if _, err := trainer.WriteDataset(context.Background(), "up", name); !errors.Is(err, ErrInvalidFilename) {
			t.Fatalf("%q: expected ErrInvalidFilename, got %v", name, err)
		}
	}
	if _, err := os.Stat(filepath.Join(parent, "escape.jsonl")); err == nil {
		t.Fatal("file escaped the output dir")
	}
}

func TestWriteDatasetSymlinkEscape(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlinks require privileges on windows")
	}
	exporter, store := newRatedExporter(t)
	outside := t.TempDir()
	dir := t.TempDir()
	// A pre-planted symlink with the target name must not be followed.
	if err := os.Symlink(filepath.Join(outside, "victim.jsonl"), filepath.Join(dir, "link.jsonl")); err != nil {
		t.Fatal(err)
	}
	trainer := NewTrainer(exporter, store, dir)
	if _, err := trainer.WriteDataset(context.Background(), "up", "link.jsonl"); !errors.Is(err, ErrFileExists) {
		t.Fatalf("expected ErrFileExists for symlink target, got %v", err)
	}
	if _, err := os.Stat(filepath.Join(outside, "victim.jsonl")); err == nil {
		t.Fatal("write followed a symlink out of the output dir")
	}
}

func TestFederatedAggregate(t *testing.T) {
	trainer := NewTrainer(nil, nil, t.TempDir())
	out, err := trainer.FederatedAggregate(context.Background(), []*federated.LoRAMatrix{
		{Rows: 1, Cols: 1, Data: []float64{1}},
		{Rows: 1, Cols: 1, Data: []float64{3}},
	})
	if err != nil || out.Data[0] != 2 {
		t.Fatalf("unexpected aggregate %v %v", out, err)
	}
}
