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

// FineTuneJob represents a fine-tuning job.
//
// Manual-queue jobs (see Trainer.EnableManualQueue) carry the exported
// Dataset. Backend jobs do not retain the dataset in memory: it lives in the
// provider's Files API as TrainingFile, and FineTunedModel/Error are filled
// in as the remote job progresses.
type FineTuneJob struct {
	ID             string
	Model          string
	Dataset        string
	Status         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
	TrainingFile   string
	FineTunedModel string
	Error          string
}

// FineTuneBackend runs fine-tuning jobs on a provider. OpenAIFineTuner
// implements it.
type FineTuneBackend interface {
	// Submit uploads a chat-format JSONL dataset and creates a job for
	// model (empty = backend default). If the job was created but a later
	// step failed, the returned record has its ID set.
	Submit(ctx context.Context, filename string, jsonl []byte, model string) (FineTuneJobRecord, error)
	// Job and Jobs return locally tracked records without network calls.
	Job(id string) (FineTuneJobRecord, bool)
	Jobs() []FineTuneJobRecord
	// GetJob refreshes a job from the provider.
	GetJob(ctx context.Context, id string) (FineTuneJobRecord, error)
	// CancelJob cancels a job on the provider.
	CancelJob(ctx context.Context, id string) (FineTuneJobRecord, error)
	// WaitForJob polls until the job is terminal or ctx is done.
	WaitForJob(ctx context.Context, id string) (FineTuneJobRecord, error)
}

// Trainer orchestrates dataset export, fine-tuning jobs, and federated
// aggregation.
//
// Fine-tuning requires a backend (SetFineTuneBackend, e.g. an
// OpenAIFineTuner): Enqueue then converts the export to chat JSONL, uploads
// it and creates a remote job. Without a backend Enqueue fails with
// ErrFineTuneNotConfigured instead of recording jobs that would never run,
// unless EnableManualQueue opts into the legacy mode where jobs are only
// recorded as "queued" for an external consumer (Jobs/Status). The trainer
// never trains a model itself and never shells out.
type Trainer struct {
	mu          sync.Mutex
	jobs        map[string]FineTuneJob
	order       []string // manual-queue job IDs, oldest first
	exporter    *flywheel.DatasetExporter
	ledger      ledger.LedgerStore
	outputDir   string
	aggregator  federated.FederatedAggregator
	seq         atomic.Uint64
	backend     FineTuneBackend
	manualQueue bool
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

// SetFineTuneBackend attaches a fine-tuning backend used by Enqueue,
// Status, Jobs, Cancel, Refresh and Wait. A nil backend detaches it.
func (t *Trainer) SetFineTuneBackend(b FineTuneBackend) {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.backend = b
	t.mu.Unlock()
}

// EnableManualQueue opts into the legacy mode used when no backend is set:
// Enqueue records jobs as "queued" (with the dataset) for an external
// consumer instead of failing with ErrFineTuneNotConfigured.
func (t *Trainer) EnableManualQueue() {
	if t == nil {
		return
	}
	t.mu.Lock()
	t.manualQueue = true
	t.mu.Unlock()
}

func (t *Trainer) mode() (FineTuneBackend, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.backend, t.manualQueue
}

func recordToJob(r FineTuneJobRecord) FineTuneJob {
	return FineTuneJob{
		ID:             r.ID,
		Model:          r.Model,
		Status:         r.Status,
		CreatedAt:      r.CreatedAt,
		UpdatedAt:      r.UpdatedAt,
		TrainingFile:   r.TrainingFile,
		FineTunedModel: r.FineTunedModel,
		Error:          r.ErrorMessage,
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

// Enqueue starts a fine-tuning job built from rated interactions. With a
// backend the dataset is converted (FlywheelToChatJSONL), uploaded and a
// remote job is created (model "" = backend default); in manual-queue mode
// the job is only recorded. Otherwise it fails with
// ErrFineTuneNotConfigured.
func (t *Trainer) Enqueue(ctx context.Context, model, minRating string) (FineTuneJob, error) {
	if t == nil || t.exporter == nil {
		return FineTuneJob{}, ErrNotInitialized
	}
	backend, manual := t.mode()
	if backend == nil && !manual {
		return FineTuneJob{}, fmt.Errorf("%w: attach a backend with SetFineTuneBackend", ErrFineTuneNotConfigured)
	}
	payload, err := t.exportDataset(ctx, minRating)
	if err != nil {
		return FineTuneJob{}, err
	}
	if backend != nil {
		data, n, err := FlywheelToChatJSONL([]byte(payload))
		if err != nil {
			return FineTuneJob{}, err
		}
		if n == 0 {
			return FineTuneJob{}, fmt.Errorf("%w: no exported record could be converted to a chat example", ErrInvalidDataset)
		}
		rec, err := backend.Submit(ctx, t.nextID("ft-dataset")+DatasetFileExt, data, model)
		if rec.ID == "" {
			return FineTuneJob{}, err
		}
		return recordToJob(rec), err
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

// Status returns the last known fine-tuning job state without network
// calls (use Refresh to query the backend).
func (t *Trainer) Status(id string) (FineTuneJob, bool) {
	if t == nil {
		return FineTuneJob{}, false
	}
	t.mu.Lock()
	job, ok := t.jobs[id]
	backend := t.backend
	t.mu.Unlock()
	if ok || backend == nil {
		return job, ok
	}
	rec, ok := backend.Job(id)
	if !ok {
		return FineTuneJob{}, false
	}
	return recordToJob(rec), true
}

// Jobs returns the retained manual-queue jobs followed by the backend's
// tracked jobs, each oldest first.
func (t *Trainer) Jobs() []FineTuneJob {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	out := make([]FineTuneJob, 0, len(t.order))
	for _, id := range t.order {
		if job, ok := t.jobs[id]; ok {
			out = append(out, job)
		}
	}
	backend := t.backend
	t.mu.Unlock()
	if backend != nil {
		for _, rec := range backend.Jobs() {
			out = append(out, recordToJob(rec))
		}
	}
	return out
}

// Refresh returns a job's current state, querying the backend for remote
// jobs.
func (t *Trainer) Refresh(ctx context.Context, id string) (FineTuneJob, error) {
	if t == nil {
		return FineTuneJob{}, ErrNotInitialized
	}
	t.mu.Lock()
	job, ok := t.jobs[id]
	backend := t.backend
	t.mu.Unlock()
	if ok {
		return job, nil
	}
	if backend == nil {
		return FineTuneJob{}, ErrJobNotFound
	}
	rec, err := backend.GetJob(ctx, id)
	if err != nil {
		return FineTuneJob{}, err
	}
	return recordToJob(rec), nil
}

// Wait blocks until a remote job is terminal or ctx is done (see
// OpenAIFineTuner.WaitForJob). Manual-queue jobs cannot be waited on.
func (t *Trainer) Wait(ctx context.Context, id string) (FineTuneJob, error) {
	if t == nil {
		return FineTuneJob{}, ErrNotInitialized
	}
	t.mu.Lock()
	job, ok := t.jobs[id]
	backend := t.backend
	t.mu.Unlock()
	if ok {
		return job, fmt.Errorf("%w: job %s is in the manual queue", ErrFineTuneNotConfigured, id)
	}
	if backend == nil {
		return FineTuneJob{}, ErrJobNotFound
	}
	rec, err := backend.WaitForJob(ctx, id)
	return recordToJob(rec), err
}

// Cancel cancels a job: manual-queue jobs are marked cancelled, remote jobs
// are cancelled on the backend. It is CancelContext with a background
// context (the backend's HTTP timeout still applies).
func (t *Trainer) Cancel(id string) error {
	return t.CancelContext(context.Background(), id)
}

// CancelContext is Cancel with a caller-supplied context for remote calls.
func (t *Trainer) CancelContext(ctx context.Context, id string) error {
	if t == nil {
		return ErrNotInitialized
	}
	t.mu.Lock()
	job, ok := t.jobs[id]
	backend := t.backend
	if !ok {
		t.mu.Unlock()
		if backend == nil {
			return ErrJobNotFound
		}
		_, err := backend.CancelJob(ctx, id)
		return err
	}
	defer t.mu.Unlock()
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
