package learning

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/federated"
	"github.com/ayoubzulfiqar/aerollm/internal/flywheel"
	"github.com/ayoubzulfiqar/aerollm/internal/ledger"
)

// Limits.
const (
	// MaxJobs bounds the number of retained fine-tune jobs; the oldest are
	// evicted first.
	MaxJobs = 1000
	// MaxDatasetBytes bounds the dataset size stored in a job or written by
	// WriteDataset.
	MaxDatasetBytes = 64 << 20
	// MaxDatasetFilenameLength bounds dataset file names.
	MaxDatasetFilenameLength = 255
	// DatasetFileExt is the required dataset file extension.
	DatasetFileExt = ".jsonl"
)

// Job statuses.
const (
	JobStatusQueued    = "queued"
	JobStatusCancelled = "cancelled"
)

var (
	// ErrInvalidFilename is returned for dataset file names that are not a
	// plain, safe base name ending in ".jsonl".
	ErrInvalidFilename = errors.New("learning: invalid dataset filename")
	// ErrDatasetTooLarge is returned when a dataset exceeds MaxDatasetBytes.
	ErrDatasetTooLarge = errors.New("learning: dataset too large")
	// ErrFileExists is returned when WriteDataset would overwrite a file.
	ErrFileExists = errors.New("learning: dataset file already exists")
	// ErrJobNotFound is returned for unknown job IDs.
	ErrJobNotFound = errors.New("learning: job not found")
	// ErrNotInitialized is returned when the trainer or its exporter is nil.
	ErrNotInitialized = errors.New("learning: trainer not initialized")
)

var safeFilename = regexp.MustCompile(`^[A-Za-z0-9_-][A-Za-z0-9._-]*$`)

// FineTuneJob represents a background fine-tuning job.
type FineTuneJob struct {
	ID        string
	Model     string
	Dataset   string
	Status    string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Trainer orchestrates dataset export, fine-tuning jobs, and federated
// aggregation.
//
// Note: no fine-tuning backend is wired to the trainer. Enqueued jobs are
// recorded with status "queued" and stay that way until cancelled or picked
// up by an external consumer via Jobs/Status; the trainer never trains a
// model itself and never shells out.
type Trainer struct {
	mu         sync.Mutex
	jobs       map[string]FineTuneJob
	order      []string // job IDs, oldest first
	exporter   *flywheel.DatasetExporter
	ledger     ledger.LedgerStore
	outputDir  string
	aggregator federated.FederatedAggregator
	seq        atomic.Uint64
}

// NewTrainer creates a new trainer with a default FedAvg aggregator.
func NewTrainer(exporter *flywheel.DatasetExporter, ledgerStore ledger.LedgerStore, outputDir string) *Trainer {
	return NewTrainerWithAggregator(exporter, ledgerStore, outputDir, federated.NewFedAvgAggregator())
}

// NewTrainerWithAggregator creates a new trainer with a custom federated aggregator.
func NewTrainerWithAggregator(exporter *flywheel.DatasetExporter, ledgerStore ledger.LedgerStore, outputDir string, aggregator federated.FederatedAggregator) *Trainer {
	if outputDir == "" {
		outputDir = "./fine-tune-jobs"
	}
	if aggregator == nil {
		aggregator = federated.NewFedAvgAggregator()
	}
	return &Trainer{
		jobs:       make(map[string]FineTuneJob),
		exporter:   exporter,
		ledger:     ledgerStore,
		outputDir:  outputDir,
		aggregator: aggregator,
	}
}

func (t *Trainer) nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UTC().UnixNano(), t.seq.Add(1))
}

func (t *Trainer) exportDataset(ctx context.Context, minRating string) (string, error) {
	if t == nil || t.exporter == nil {
		return "", ErrNotInitialized
	}
	payload, err := t.exporter.ExportJSONL(ctx, minRating)
	if err != nil {
		return "", err
	}
	if payload == "" {
		return "", fmt.Errorf("learning: no dataset available for rating=%q", minRating)
	}
	if len(payload) > MaxDatasetBytes {
		return "", fmt.Errorf("%w: %d bytes > %d", ErrDatasetTooLarge, len(payload), MaxDatasetBytes)
	}
	return payload, nil
}

// Enqueue records a new fine-tuning job built from rated interactions. The
// job is only recorded; see the Trainer doc comment.
func (t *Trainer) Enqueue(ctx context.Context, model, minRating string) (FineTuneJob, error) {
	if t == nil {
		return FineTuneJob{}, ErrNotInitialized
	}
	payload, err := t.exportDataset(ctx, minRating)
	if err != nil {
		return FineTuneJob{}, err
	}
	now := time.Now().UTC()
	job := FineTuneJob{
		ID:        t.nextID("ft"),
		Model:     model,
		Dataset:   payload,
		Status:    JobStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.jobs == nil {
		t.jobs = make(map[string]FineTuneJob)
	}
	for len(t.order) >= MaxJobs {
		delete(t.jobs, t.order[0])
		t.order[0] = ""
		t.order = t.order[1:]
	}
	t.jobs[job.ID] = job
	t.order = append(t.order, job.ID)
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

// Jobs returns the retained jobs, oldest first.
func (t *Trainer) Jobs() []FineTuneJob {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]FineTuneJob, 0, len(t.order))
	for _, id := range t.order {
		if job, ok := t.jobs[id]; ok {
			out = append(out, job)
		}
	}
	return out
}

// Cancel marks a queued job as cancelled.
func (t *Trainer) Cancel(id string) error {
	if t == nil {
		return ErrNotInitialized
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	job, ok := t.jobs[id]
	if !ok {
		return ErrJobNotFound
	}
	if job.Status != JobStatusQueued {
		return fmt.Errorf("learning: job %s is %s, not %s", id, job.Status, JobStatusQueued)
	}
	job.Status = JobStatusCancelled
	job.UpdatedAt = time.Now().UTC()
	t.jobs[id] = job
	return nil
}

// validateDatasetFilename accepts only a plain base name made of
// [A-Za-z0-9._-] (not starting with '.'), at most MaxDatasetFilenameLength
// bytes, ending in ".jsonl". This rules out path separators, "..", absolute
// paths and hidden files.
func validateDatasetFilename(name string) error {
	if name == "" || len(name) > MaxDatasetFilenameLength {
		return fmt.Errorf("%w: length must be 1-%d", ErrInvalidFilename, MaxDatasetFilenameLength)
	}
	if !safeFilename.MatchString(name) || filepath.Base(name) != name {
		return fmt.Errorf("%w: only [A-Za-z0-9._-] allowed, no leading dot or path separators", ErrInvalidFilename)
	}
	if !strings.HasSuffix(name, DatasetFileExt) || name == DatasetFileExt {
		return fmt.Errorf("%w: must end in %s", ErrInvalidFilename, DatasetFileExt)
	}
	return nil
}

// WriteDataset persists a dataset snapshot for external fine-tune consumers
// and returns its path. The file is written under the trainer's output
// directory only (via os.Root, so neither ".." nor symlinks can escape it),
// atomically, with mode 0600 (datasets contain user prompts), and never
// overwrites an existing file. An empty filename generates a unique one.
func (t *Trainer) WriteDataset(ctx context.Context, minRating, filename string) (string, error) {
	if t == nil {
		return "", ErrNotInitialized
	}
	if filename == "" {
		filename = t.nextID("dataset") + DatasetFileExt
	}
	if err := validateDatasetFilename(filename); err != nil {
		return "", err
	}
	payload, err := t.exportDataset(ctx, minRating)
	if err != nil {
		return "", err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	if err := os.MkdirAll(t.outputDir, 0o700); err != nil {
		return "", fmt.Errorf("learning: creating output dir: %w", err)
	}
	root, err := os.OpenRoot(t.outputDir)
	if err != nil {
		return "", fmt.Errorf("learning: opening output dir: %w", err)
	}
	defer root.Close()

	if _, err := root.Lstat(filename); err == nil {
		return "", fmt.Errorf("%w: %s", ErrFileExists, filename)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("learning: checking dataset file: %w", err)
	}

	tmp := fmt.Sprintf(".%s.tmp-%d", filename, t.seq.Add(1))
	f, err := root.OpenFile(tmp, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", fmt.Errorf("learning: creating temp file: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			_ = root.Remove(tmp)
		}
	}()
	if _, err := f.WriteString(payload); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("learning: writing dataset: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return "", fmt.Errorf("learning: syncing dataset: %w", err)
	}
	if err := f.Close(); err != nil {
		return "", fmt.Errorf("learning: closing dataset: %w", err)
	}

	// Link fails if the destination exists, giving an atomic no-clobber
	// publish. Fall back to Rename where hard links are unsupported.
	if err := root.Link(tmp, filename); err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("%w: %s", ErrFileExists, filename)
		}
		if _, statErr := root.Lstat(filename); statErr == nil {
			return "", fmt.Errorf("%w: %s", ErrFileExists, filename)
		}
		if err := root.Rename(tmp, filename); err != nil {
			return "", fmt.Errorf("learning: publishing dataset: %w", err)
		}
		committed = true
		return filepath.Join(t.outputDir, filename), nil
	}
	_ = root.Remove(tmp)
	committed = true
	return filepath.Join(t.outputDir, filename), nil
}

// FederatedAggregate aggregates LoRA updates using the configured aggregator.
func (t *Trainer) FederatedAggregate(ctx context.Context, updates []*federated.LoRAMatrix) (*federated.LoRAMatrix, error) {
	if t == nil || t.aggregator == nil {
		return nil, fmt.Errorf("aggregator not configured")
	}
	return t.aggregator.Aggregate(ctx, updates)
}
