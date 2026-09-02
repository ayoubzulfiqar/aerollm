package learning

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

// FineTuneJob represents a background fine-tuning job.
type FineTuneJob struct {
	ID        string
	Model     string
	Dataset   string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Trainer orchestrates dataset export and fine-tuning jobs.
type Trainer struct {
	mu       sync.Mutex
	jobs     map[string]FineTuneJob
	exporter *flywheel.DatasetExporter
	ledger   ledger.LedgerStore
	outputDir string
}

// NewTrainer creates a new trainer.
func NewTrainer(exporter *flywheel.DatasetExporter, ledgerStore ledger.LedgerStore, outputDir string) *Trainer {
	if outputDir == "" {
		outputDir = "./fine-tune-jobs"
	}
	return &Trainer{
		jobs:     make(map[string]FineTuneJob),
		exporter: exporter,
		ledger:   ledgerStore,
		outputDir: outputDir,
	}
}

// Enqueue creates a new fine-tuning job from recent high-rated interactions.
func (t *Trainer) Enqueue(ctx context.Context, model, minRating string) (FineTuneJob, error) {
	if t == nil {
		return FineTuneJob{}, fmt.Errorf("trainer not initialized")
	}
	payload, err := t.exporter.ExportJSONL(ctx, minRating)
	if err != nil {
		return FineTuneJob{}, err
	}
	if payload == "" {
		return FineTuneJob{}, fmt.Errorf("no dataset available for rating=%s", minRating)
	}
	job := FineTuneJob{
		ID:        fmt.Sprintf("ft-%d", time.Now().UTC().UnixNano()),
		Model:     model,
		Dataset:   payload,
		Status:    "queued",
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}
	t.mu.Lock()
	t.jobs[job.ID] = job
	t.mu.Unlock()
	return job, nil
}

// Status returns the current fine-tuning job state.
func (t *Trainer) Status(id string) (FineTuneJob, bool) {
	if t == nil {
		return FineTuneJob{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	job, ok := t.jobs[id]
	return job, ok
}

// WriteDataset persists a dataset snapshot for external fine-tune consumers.
func (t *Trainer) WriteDataset(ctx context.Context, minRating, filename string) (string, error) {
	_ = ctx
	if t == nil {
		return "", fmt.Errorf("trainer not initialized")
	}
	payload, err := t.exporter.ExportJSONL(ctx, minRating)
	if err != nil {
		return "", err
	}
	if payload == "" {
		return "", fmt.Errorf("empty dataset for rating=%s", minRating)
	}
	if err := os.MkdirAll(t.outputDir, 0o755); err != nil {
		return "", err
	}
	if filename == "" {
		filename = fmt.Sprintf("dataset-%d.jsonl", time.Now().UTC().UnixNano())
	}
	path := filepath.Join(t.outputDir, filename)
	if err := os.WriteFile(path, []byte(payload), 0o644); err != nil {
		return "", err
	}
	return path, nil
}
