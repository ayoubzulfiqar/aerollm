package learning

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand/v2"
	"mime/multipart"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

// OpenAI-compatible fine-tuning defaults and limits.
const (
	// DefaultOpenAIBaseURL is used when OpenAIFineTuneConfig.BaseURL is empty.
	DefaultOpenAIBaseURL = "https://api.openai.com/v1"
	// DefaultFineTunePollInterval is the first delay between status polls.
	DefaultFineTunePollInterval = 5 * time.Second
	// DefaultFineTunePollMaxInterval caps the exponential poll backoff.
	DefaultFineTunePollMaxInterval = time.Minute
	// DefaultFineTuneHTTPTimeout bounds a single HTTP exchange (uploads of
	// large datasets included) when no HTTPClient is supplied.
	DefaultFineTuneHTTPTimeout = 10 * time.Minute
	// DefaultFineTuneMaxPollFailures is the number of consecutive transient
	// poll failures (network errors, 429, 5xx) tolerated by WaitForJob.
	DefaultFineTuneMaxPollFailures = 5
	// FineTuneJobsBucket is the persist.Store bucket holding job records.
	FineTuneJobsBucket = "learning_finetune_jobs"
	// MaxFineTuneListLimit is the largest page size accepted by ListJobs.
	MaxFineTuneListLimit = 100

	maxAPIResponseBytes = 4 << 20
	maxAPIErrorMessage  = 512
	maxRetryAfter       = 5 * time.Minute
	maxRecordField      = 256
)

// Remote job statuses reported by OpenAI-compatible APIs (JobStatusQueued and
// JobStatusCancelled are shared with the manual queue).
const (
	JobStatusValidatingFiles = "validating_files"
	JobStatusRunning         = "running"
	JobStatusSucceeded       = "succeeded"
	JobStatusFailed          = "failed"
)

var (
	// ErrFineTuneNotConfigured is returned when no fine-tuning backend (API
	// key and model) is configured. Jobs are never queued silently.
	ErrFineTuneNotConfigured = errors.New("learning: fine-tuning backend not configured")
	// ErrInvalidDataset is returned for datasets that are not valid JSONL
	// (one JSON object per line) or contain no usable example.
	ErrInvalidDataset = errors.New("learning: invalid fine-tune dataset")
	// ErrInvalidJobID is returned for job or file IDs outside the allowlist.
	ErrInvalidJobID = errors.New("learning: invalid fine-tune id")
	// ErrInvalidFineTuneConfig is returned for malformed client configuration.
	ErrInvalidFineTuneConfig = errors.New("learning: invalid fine-tune config")
)

var (
	remoteIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)
	modelPattern    = regexp.MustCompile(`^[A-Za-z0-9._:/-]{1,256}$`)
	suffixPattern   = regexp.MustCompile(`^[A-Za-z0-9._-]{0,64}$`)
	orgPattern      = regexp.MustCompile(`^[A-Za-z0-9_-]{0,128}$`)
)

// IsTerminalJobStatus reports whether a remote job status is final.
func IsTerminalJobStatus(status string) bool {
	switch status {
	case JobStatusSucceeded, JobStatusFailed, JobStatusCancelled:
		return true
	}
	return false
}

// FineTuneHyperparameters are optional training hyperparameters. Nil fields
// are omitted so the provider picks its "auto" defaults.
type FineTuneHyperparameters struct {
	NEpochs                *int     `json:"n_epochs,omitempty"`
	BatchSize              *int     `json:"batch_size,omitempty"`
	LearningRateMultiplier *float64 `json:"learning_rate_multiplier,omitempty"`
}

func (h *FineTuneHyperparameters) validate() error {
	if h == nil {
		return nil
	}
	if h.NEpochs != nil && (*h.NEpochs < 1 || *h.NEpochs > 100) {
		return fmt.Errorf("%w: n_epochs must be 1-100", ErrInvalidFineTuneConfig)
	}
	if h.BatchSize != nil && (*h.BatchSize < 1 || *h.BatchSize > 1<<16) {
		return fmt.Errorf("%w: batch_size must be 1-%d", ErrInvalidFineTuneConfig, 1<<16)
	}
	if v := h.LearningRateMultiplier; v != nil && (math.IsNaN(*v) || math.IsInf(*v, 0) || *v <= 0 || *v > 100) {
		return fmt.Errorf("%w: learning_rate_multiplier must be in (0, 100]", ErrInvalidFineTuneConfig)
	}
	return nil
}

// OpenAIFineTuneConfig configures an OpenAIFineTuner. A config without an
// APIKey or Model yields a client whose remote operations fail with
// ErrFineTuneNotConfigured.
type OpenAIFineTuneConfig struct {
	// BaseURL of the OpenAI-compatible API, including the version prefix
	// (default DefaultOpenAIBaseURL). Must be http(s) without credentials,
	// query or fragment.
	BaseURL string
	// APIKey is sent as "Authorization: Bearer <key>". It is never included
	// in errors (provider messages echoing it are redacted).
	APIKey string
	// Model is the default base model to fine-tune.
	Model string
	// Organization and Project optionally set the OpenAI-Organization and
	// OpenAI-Project headers.
	Organization string
	Project      string
	// Suffix is an optional fine-tuned model name suffix (<= 64 chars of
	// [A-Za-z0-9._-]).
	Suffix string
	// Hyperparameters are optional training hyperparameters.
	Hyperparameters *FineTuneHyperparameters
	// HTTPClient overrides the default client (timeout
	// DefaultFineTuneHTTPTimeout).
	HTTPClient *http.Client
	// Store optionally persists job records (bucket FineTuneJobsBucket);
	// records are loaded on construction and written through on change.
	Store persist.Store
	// PollInterval is the initial WaitForJob delay (default
	// DefaultFineTunePollInterval); it doubles up to PollMaxInterval
	// (default DefaultFineTunePollMaxInterval).
	PollInterval    time.Duration
	PollMaxInterval time.Duration
	// MaxPollFailures bounds consecutive transient poll failures (default
	// DefaultFineTuneMaxPollFailures).
	MaxPollFailures int
	// MaxUploadBytes caps uploaded datasets (default MaxDatasetBytes).
	MaxUploadBytes int64
}

// String redacts the API key so the config can be logged safely.
func (c OpenAIFineTuneConfig) String() string {
	key := ""
	if c.APIKey != "" {
		key = "[REDACTED]"
	}
	return fmt.Sprintf("OpenAIFineTuneConfig{BaseURL:%q APIKey:%q Model:%q}", c.BaseURL, key, c.Model)
}

// GoString redacts the API key for %#v.
func (c OpenAIFineTuneConfig) GoString() string { return c.String() }

// FineTuneFile is an uploaded file as returned by the Files API.
type FineTuneFile struct {
	ID        string `json:"id"`
	Object    string `json:"object"`
	Bytes     int64  `json:"bytes"`
	CreatedAt int64  `json:"created_at"`
	Filename  string `json:"filename"`
	Purpose   string `json:"purpose"`
	Status    string `json:"status,omitempty"`
}

// FineTuneJobRecord is the locally tracked (and optionally persisted) state
// of a remote fine-tuning job.
type FineTuneJobRecord struct {
	ID             string     `json:"id"`
	Model          string     `json:"model"`
	Status         string     `json:"status"`
	TrainingFile   string     `json:"training_file,omitempty"`
	FineTunedModel string     `json:"fine_tuned_model,omitempty"`
	TrainedTokens  int64      `json:"trained_tokens,omitempty"`
	ErrorCode      string     `json:"error_code,omitempty"`
	ErrorMessage   string     `json:"error_message,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	FinishedAt     *time.Time `json:"finished_at,omitempty"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// Terminal reports whether the job reached a final status.
func (r FineTuneJobRecord) Terminal() bool { return IsTerminalJobStatus(r.Status) }

func (r FineTuneJobRecord) sameState(o FineTuneJobRecord) bool {
	sameFinish := (r.FinishedAt == nil) == (o.FinishedAt == nil)
	if sameFinish && r.FinishedAt != nil {
		sameFinish = r.FinishedAt.Equal(*o.FinishedAt)
	}
	return sameFinish && r.Model == o.Model && r.Status == o.Status &&
		r.TrainingFile == o.TrainingFile && r.FineTunedModel == o.FineTunedModel &&
		r.TrainedTokens == o.TrainedTokens && r.ErrorCode == o.ErrorCode &&
		r.ErrorMessage == o.ErrorMessage
}

// APIError is a non-2xx response from the fine-tuning API. Message has the
// API key redacted and is truncated.
type APIError struct {
	StatusCode int
	Type       string
	Code       string
	Param      string
	Message    string
	// RetryAfter is the parsed Retry-After header (0 if absent).
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "learning: fine-tune API error (status %d", e.StatusCode)
	if e.Type != "" {
		fmt.Fprintf(&sb, ", type %s", e.Type)
	}
	if e.Code != "" {
		fmt.Fprintf(&sb, ", code %s", e.Code)
	}
	sb.WriteString(")")
	if e.Message != "" {
		sb.WriteString(": ")
		sb.WriteString(e.Message)
	}
	return sb.String()
}

// Temporary reports whether retrying the request may succeed (429 or 5xx).
func (e *APIError) Temporary() bool {
	return e.StatusCode == http.StatusTooManyRequests || e.StatusCode >= 500
}

// OpenAIFineTuner is a client for the OpenAI fine-tuning API (and compatible
// providers). It uploads JSONL datasets through the Files API, creates
// fine-tuning jobs, polls them with capped exponential backoff and tracks
// job records locally (optionally persisted). It is safe for concurrent use.
type OpenAIFineTuner struct {
	cfg     OpenAIFineTuneConfig
	baseURL string
	client  *http.Client

	mu      sync.Mutex
	records map[string]FineTuneJobRecord
	order   []string // job IDs, oldest first
	store   persist.Store
}

// NewOpenAIFineTuner validates cfg and returns a client. Missing APIKey or
// Model is not an error (Configured reports false and remote calls return
// ErrFineTuneNotConfigured); malformed values are (nil client). If
// cfg.Store is set, existing job records are loaded from it; when that
// load reports an error (e.g. undecodable documents, which are skipped),
// the client is still returned, usable and write-through, together with
// the error so the caller can log it.
func NewOpenAIFineTuner(cfg OpenAIFineTuneConfig) (*OpenAIFineTuner, error) {
	cfg.BaseURL = strings.TrimSpace(cfg.BaseURL)
	if cfg.BaseURL == "" {
		cfg.BaseURL = DefaultOpenAIBaseURL
	}
	base, err := normalizeBaseURL(cfg.BaseURL)
	if err != nil {
		return nil, err
	}
	cfg.APIKey = strings.TrimSpace(cfg.APIKey)
	if strings.ContainsAny(cfg.APIKey, "\r\n") {
		return nil, fmt.Errorf("%w: api key contains line breaks", ErrInvalidFineTuneConfig)
	}
	cfg.Model = strings.TrimSpace(cfg.Model)
	if cfg.Model != "" && !modelPattern.MatchString(cfg.Model) {
		return nil, fmt.Errorf("%w: model must be 1-256 chars of [A-Za-z0-9._:/-]", ErrInvalidFineTuneConfig)
	}
	if !suffixPattern.MatchString(cfg.Suffix) {
		return nil, fmt.Errorf("%w: suffix must be at most 64 chars of [A-Za-z0-9._-]", ErrInvalidFineTuneConfig)
	}
	if !orgPattern.MatchString(cfg.Organization) || !orgPattern.MatchString(cfg.Project) {
		return nil, fmt.Errorf("%w: organization/project must be [A-Za-z0-9_-]", ErrInvalidFineTuneConfig)
	}
	if err := cfg.Hyperparameters.validate(); err != nil {
		return nil, err
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultFineTunePollInterval
	}
	if cfg.PollMaxInterval <= 0 {
		cfg.PollMaxInterval = DefaultFineTunePollMaxInterval
	}
	if cfg.PollMaxInterval < cfg.PollInterval {
		cfg.PollMaxInterval = cfg.PollInterval
	}
	if cfg.MaxPollFailures <= 0 {
		cfg.MaxPollFailures = DefaultFineTuneMaxPollFailures
	}
	if cfg.MaxUploadBytes <= 0 {
		cfg.MaxUploadBytes = MaxDatasetBytes
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: DefaultFineTuneHTTPTimeout}
	}
	c := &OpenAIFineTuner{
		cfg:     cfg,
		baseURL: base,
		client:  client,
		records: make(map[string]FineTuneJobRecord),
	}
	if cfg.Store != nil {
		if err := c.EnablePersistence(cfg.Store); err != nil {
			return c, err
		}
	}
	return c, nil
}

func normalizeBaseURL(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("%w: base URL is not a valid URL", ErrInvalidFineTuneConfig)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return "", fmt.Errorf("%w: base URL scheme must be http or https", ErrInvalidFineTuneConfig)
	}
	if u.Host == "" || u.Hostname() == "" {
		return "", fmt.Errorf("%w: base URL must include a host", ErrInvalidFineTuneConfig)
	}
	if u.User != nil {
		return "", fmt.Errorf("%w: base URL must not contain credentials", ErrInvalidFineTuneConfig)
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return "", fmt.Errorf("%w: base URL must not contain a query or fragment", ErrInvalidFineTuneConfig)
	}
	return strings.TrimRight(u.String(), "/"), nil
}

// Configured reports whether an API key and a default model are set.
func (c *OpenAIFineTuner) Configured() bool {
	return c != nil && c.cfg.APIKey != "" && c.cfg.Model != ""
}

func (c *OpenAIFineTuner) requireConfigured() error {
	if c == nil {
		return ErrFineTuneNotConfigured
	}
	if c.cfg.APIKey == "" {
		return fmt.Errorf("%w: missing API key", ErrFineTuneNotConfigured)
	}
	if c.cfg.Model == "" {
		return fmt.Errorf("%w: missing model", ErrFineTuneNotConfigured)
	}
	return nil
}

// EnablePersistence loads job records from ps (bucket FineTuneJobsBucket),
// writes the in-memory records to it and makes every later change
// write-through. Undecodable or invalid documents are skipped and reported
// after the valid ones are loaded.
func (c *OpenAIFineTuner) EnablePersistence(ps persist.Store) error {
	if c == nil {
		return ErrFineTuneNotConfigured
	}
	if ps == nil {
		return fmt.Errorf("%w: nil store", ErrInvalidFineTuneConfig)
	}
	loaded, loadErr := persist.LoadAll[FineTuneJobRecord](ps, FineTuneJobsBucket)
	c.mu.Lock()
	defer c.mu.Unlock()
	// In-memory records are newer than anything persisted: write them
	// through and let them win over stored copies.
	for _, rec := range c.records {
		if err := ps.Put(FineTuneJobsBucket, rec.ID, rec); err != nil {
			return fmt.Errorf("learning: persisting fine-tune job %s: %w", rec.ID, err)
		}
	}
	for key, rec := range loaded {
		if key != rec.ID || !remoteIDPattern.MatchString(rec.ID) {
			continue
		}
		if _, ok := c.records[rec.ID]; ok {
			continue
		}
		c.records[rec.ID] = rec
	}
	c.order = c.order[:0]
	for id := range c.records {
		c.order = append(c.order, id)
	}
	sort.Slice(c.order, func(i, j int) bool {
		a, b := c.records[c.order[i]], c.records[c.order[j]]
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.ID < b.ID
	})
	c.store = ps
	c.evictLocked()
	return loadErr
}

// evictLocked drops the oldest records beyond MaxJobs. Store deletions are
// best effort. c.mu must be held.
func (c *OpenAIFineTuner) evictLocked() {
	for len(c.order) > MaxJobs {
		id := c.order[0]
		c.order[0] = ""
		c.order = c.order[1:]
		delete(c.records, id)
		if c.store != nil {
			_ = c.store.Delete(FineTuneJobsBucket, id)
		}
	}
}

// track upserts rec and writes it through to the store when its state
// changed. It returns the stored record.
func (c *OpenAIFineTuner) track(rec FineTuneJobRecord) (FineTuneJobRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	old, exists := c.records[rec.ID]
	if exists {
		if !old.CreatedAt.IsZero() {
			rec.CreatedAt = old.CreatedAt
		}
		if old.TrainingFile != "" && rec.TrainingFile == "" {
			rec.TrainingFile = old.TrainingFile
		}
		if old.sameState(rec) {
			return old, nil
		}
	}
	rec.UpdatedAt = time.Now().UTC()
	if rec.CreatedAt.IsZero() {
		rec.CreatedAt = rec.UpdatedAt
	}
	c.records[rec.ID] = rec
	if !exists {
		c.order = append(c.order, rec.ID)
		c.evictLocked()
	}
	if c.store != nil {
		if err := c.store.Put(FineTuneJobsBucket, rec.ID, rec); err != nil {
			return rec, fmt.Errorf("learning: fine-tune job %s not persisted: %w", rec.ID, err)
		}
	}
	return rec, nil
}

// Job returns the locally tracked record of a job (no network call).
func (c *OpenAIFineTuner) Job(id string) (FineTuneJobRecord, bool) {
	if c == nil {
		return FineTuneJobRecord{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec, ok := c.records[id]
	return rec, ok
}

// Jobs returns the locally tracked records, oldest first (no network call).
func (c *OpenAIFineTuner) Jobs() []FineTuneJobRecord {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]FineTuneJobRecord, 0, len(c.order))
	for _, id := range c.order {
		if rec, ok := c.records[id]; ok {
			out = append(out, rec)
		}
	}
	return out
}

// Submit uploads jsonl (validated, see ValidateFineTuneJSONL) as a
// fine-tune file and creates a job for model (empty = configured default).
// If job creation fails the uploaded file is deleted on a best-effort basis.
// When the job was created but could not be persisted, the returned record
// is valid (ID set) and the error reports the persistence failure.
func (c *OpenAIFineTuner) Submit(ctx context.Context, filename string, jsonl []byte, model string) (FineTuneJobRecord, error) {
	if err := c.requireConfigured(); err != nil {
		return FineTuneJobRecord{}, err
	}
	ctx = orBackground(ctx)
	model, err := c.resolveModel(model)
	if err != nil {
		return FineTuneJobRecord{}, err
	}
	file, err := c.upload(ctx, filename, jsonl)
	if err != nil {
		return FineTuneJobRecord{}, err
	}
	rec, err := c.CreateJob(ctx, file.ID, model)
	if err != nil && rec.ID == "" {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		_ = c.deleteFile(cleanupCtx, file.ID)
		cancel()
	}
	return rec, err
}

func (c *OpenAIFineTuner) resolveModel(model string) (string, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		model = c.cfg.Model
	}
	if !modelPattern.MatchString(model) {
		return "", fmt.Errorf("%w: model must be 1-256 chars of [A-Za-z0-9._:/-]", ErrInvalidFineTuneConfig)
	}
	return model, nil
}

// UploadTrainingFile reads a JSONL dataset from r (at most MaxUploadBytes),
// validates it and uploads it with purpose "fine-tune". An empty filename
// defaults to "dataset.jsonl".
func (c *OpenAIFineTuner) UploadTrainingFile(ctx context.Context, filename string, r io.Reader) (FineTuneFile, error) {
	if err := c.requireConfigured(); err != nil {
		return FineTuneFile{}, err
	}
	if r == nil {
		return FineTuneFile{}, fmt.Errorf("%w: missing dataset", ErrInvalidDataset)
	}
	data, err := io.ReadAll(io.LimitReader(r, c.cfg.MaxUploadBytes+1))
	if err != nil {
		return FineTuneFile{}, fmt.Errorf("learning: reading dataset: %w", err)
	}
	return c.upload(orBackground(ctx), filename, data)
}

func (c *OpenAIFineTuner) upload(ctx context.Context, filename string, data []byte) (FineTuneFile, error) {
	if filename == "" {
		filename = "dataset" + DatasetFileExt
	}
	if err := validateDatasetFilename(filename); err != nil {
		return FineTuneFile{}, err
	}
	if int64(len(data)) > c.cfg.MaxUploadBytes {
		return FineTuneFile{}, fmt.Errorf("%w: %d bytes > %d", ErrDatasetTooLarge, len(data), c.cfg.MaxUploadBytes)
	}
	if _, err := ValidateFineTuneJSONL(data); err != nil {
		return FineTuneFile{}, err
	}

	// Build the multipart envelope around the dataset so the payload is
	// streamed from memory without a second copy and with a known length.
	var head bytes.Buffer
	mw := multipart.NewWriter(&head)
	if err := mw.WriteField("purpose", "fine-tune"); err != nil {
		return FineTuneFile{}, fmt.Errorf("learning: building upload: %w", err)
	}
	if _, err := mw.CreateFormFile("file", filename); err != nil {
		return FineTuneFile{}, fmt.Errorf("learning: building upload: %w", err)
	}
	prefix := append([]byte(nil), head.Bytes()...)
	head.Reset()
	if err := mw.Close(); err != nil {
		return FineTuneFile{}, fmt.Errorf("learning: building upload: %w", err)
	}
	suffix := append([]byte(nil), head.Bytes()...)
	body := io.MultiReader(bytes.NewReader(prefix), bytes.NewReader(data), bytes.NewReader(suffix))
	length := int64(len(prefix) + len(data) + len(suffix))

	var file FineTuneFile
	if err := c.do(ctx, http.MethodPost, "/files", body, mw.FormDataContentType(), length, &file); err != nil {
		return FineTuneFile{}, err
	}
	if !remoteIDPattern.MatchString(file.ID) {
		return FineTuneFile{}, fmt.Errorf("%w: API returned an invalid file id", ErrInvalidJobID)
	}
	return file, nil
}

func (c *OpenAIFineTuner) deleteFile(ctx context.Context, fileID string) error {
	if !remoteIDPattern.MatchString(fileID) {
		return ErrInvalidJobID
	}
	return c.do(ctx, http.MethodDelete, "/files/"+url.PathEscape(fileID), nil, "", 0, nil)
}

type createJobRequest struct {
	TrainingFile    string                   `json:"training_file"`
	Model           string                   `json:"model"`
	Suffix          string                   `json:"suffix,omitempty"`
	Hyperparameters *FineTuneHyperparameters `json:"hyperparameters,omitempty"`
}

// CreateJob creates a fine-tuning job for an uploaded training file. An
// empty model uses the configured default.
func (c *OpenAIFineTuner) CreateJob(ctx context.Context, trainingFileID, model string) (FineTuneJobRecord, error) {
	if err := c.requireConfigured(); err != nil {
		return FineTuneJobRecord{}, err
	}
	if !remoteIDPattern.MatchString(trainingFileID) {
		return FineTuneJobRecord{}, fmt.Errorf("%w: training file", ErrInvalidJobID)
	}
	model, err := c.resolveModel(model)
	if err != nil {
		return FineTuneJobRecord{}, err
	}
	payload, err := json.Marshal(createJobRequest{
		TrainingFile:    trainingFileID,
		Model:           model,
		Suffix:          c.cfg.Suffix,
		Hyperparameters: c.cfg.Hyperparameters,
	})
	if err != nil {
		return FineTuneJobRecord{}, fmt.Errorf("learning: encoding job request: %w", err)
	}
	var job openAIJob
	if err := c.do(orBackground(ctx), http.MethodPost, "/fine_tuning/jobs", bytes.NewReader(payload), "application/json", int64(len(payload)), &job); err != nil {
		return FineTuneJobRecord{}, err
	}
	rec, err := job.record()
	if err != nil {
		return FineTuneJobRecord{}, err
	}
	if rec.TrainingFile == "" {
		rec.TrainingFile = trainingFileID
	}
	return c.track(rec)
}

// GetJob fetches a job's current state from the API and updates (tracks)
// its local record.
func (c *OpenAIFineTuner) GetJob(ctx context.Context, id string) (FineTuneJobRecord, error) {
	return c.jobCall(ctx, http.MethodGet, id, "")
}

// CancelJob cancels a job and returns its updated record.
func (c *OpenAIFineTuner) CancelJob(ctx context.Context, id string) (FineTuneJobRecord, error) {
	return c.jobCall(ctx, http.MethodPost, id, "/cancel")
}

func (c *OpenAIFineTuner) jobCall(ctx context.Context, method, id, action string) (FineTuneJobRecord, error) {
	if err := c.requireConfigured(); err != nil {
		return FineTuneJobRecord{}, err
	}
	if !remoteIDPattern.MatchString(id) {
		return FineTuneJobRecord{}, ErrInvalidJobID
	}
	var job openAIJob
	err := c.do(orBackground(ctx), method, "/fine_tuning/jobs/"+url.PathEscape(id)+action, nil, "", 0, &job)
	if err != nil {
		var apiErr *APIError
		if errors.As(err, &apiErr) && apiErr.StatusCode == http.StatusNotFound {
			return FineTuneJobRecord{}, fmt.Errorf("%w: %s: %w", ErrJobNotFound, id, err)
		}
		return FineTuneJobRecord{}, err
	}
	rec, err := job.record()
	if err != nil {
		return FineTuneJobRecord{}, err
	}
	if rec.ID != id {
		return FineTuneJobRecord{}, fmt.Errorf("%w: API returned job %q for %q", ErrInvalidJobID, rec.ID, id)
	}
	return c.track(rec)
}

type listJobsResponse struct {
	Data    []openAIJob `json:"data"`
	HasMore bool        `json:"has_more"`
}

// ListJobs lists remote jobs (newest first, as returned by the API). limit
// is clamped to 1-MaxFineTuneListLimit (0 = 20); after is an optional job
// ID cursor. Only jobs already tracked locally have their records updated.
func (c *OpenAIFineTuner) ListJobs(ctx context.Context, limit int, after string) ([]FineTuneJobRecord, bool, error) {
	if err := c.requireConfigured(); err != nil {
		return nil, false, err
	}
	if after != "" && !remoteIDPattern.MatchString(after) {
		return nil, false, fmt.Errorf("%w: after cursor", ErrInvalidJobID)
	}
	switch {
	case limit <= 0:
		limit = 20
	case limit > MaxFineTuneListLimit:
		limit = MaxFineTuneListLimit
	}
	q := url.Values{}
	q.Set("limit", strconv.Itoa(limit))
	if after != "" {
		q.Set("after", after)
	}
	var resp listJobsResponse
	if err := c.do(orBackground(ctx), http.MethodGet, "/fine_tuning/jobs?"+q.Encode(), nil, "", 0, &resp); err != nil {
		return nil, false, err
	}
	out := make([]FineTuneJobRecord, 0, len(resp.Data))
	for i := range resp.Data {
		rec, err := resp.Data[i].record()
		if err != nil {
			continue
		}
		if _, tracked := c.Job(rec.ID); tracked {
			if stored, err := c.track(rec); err == nil {
				rec = stored
			}
		}
		out = append(out, rec)
	}
	return out, resp.HasMore, nil
}

// WaitForJob polls a job until it reaches a terminal status, ctx is done or
// polling fails. Delays start at PollInterval and double (with +-10%
// jitter) up to PollMaxInterval; a Retry-After hint lengthens the next
// delay. Up to MaxPollFailures consecutive transient failures (network
// errors, 429, 5xx) are tolerated. On ctx cancellation the last known
// record is returned together with ctx.Err().
func (c *OpenAIFineTuner) WaitForJob(ctx context.Context, id string) (FineTuneJobRecord, error) {
	if err := c.requireConfigured(); err != nil {
		return FineTuneJobRecord{}, err
	}
	if !remoteIDPattern.MatchString(id) {
		return FineTuneJobRecord{}, ErrInvalidJobID
	}
	ctx = orBackground(ctx)
	last, _ := c.Job(id)
	delay := c.cfg.PollInterval
	failures := 0
	for {
		rec, err := c.GetJob(ctx, id)
		wait := delay
		switch {
		case err == nil:
			failures = 0
			last = rec
			if rec.Terminal() {
				return rec, nil
			}
		case ctx.Err() != nil:
			return last, ctx.Err()
		case isTransient(err):
			failures++
			if failures > c.cfg.MaxPollFailures {
				return last, fmt.Errorf("learning: polling job %s: %d consecutive failures: %w", id, failures, err)
			}
			var apiErr *APIError
			if errors.As(err, &apiErr) && apiErr.RetryAfter > wait {
				wait = apiErr.RetryAfter
			}
		default:
			return last, err
		}
		if err := sleepCtx(ctx, jitter(wait)); err != nil {
			return last, err
		}
		delay = min(delay*2, c.cfg.PollMaxInterval)
	}
}

func isTransient(err error) bool {
	var apiErr *APIError
	if errors.As(err, &apiErr) {
		return apiErr.Temporary()
	}
	var urlErr *url.Error
	return errors.As(err, &urlErr)
}

func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	spread := int64(d) / 10
	if spread <= 0 {
		return d
	}
	return d - time.Duration(spread) + time.Duration(rand.Int64N(2*spread+1))
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func orBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

// do performs one authenticated API call and decodes a 2xx JSON response
// into out (if non-nil). Response bodies are capped at maxAPIResponseBytes.
func (c *OpenAIFineTuner) do(ctx context.Context, method, path string, body io.Reader, contentType string, length int64, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return fmt.Errorf("learning: building %s request: %w", method, err)
	}
	if body != nil {
		req.ContentLength = length
		req.Header.Set("Content-Type", contentType)
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.APIKey)
	req.Header.Set("Accept", "application/json")
	if c.cfg.Organization != "" {
		req.Header.Set("OpenAI-Organization", c.cfg.Organization)
	}
	if c.cfg.Project != "" {
		req.Header.Set("OpenAI-Project", c.cfg.Project)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("learning: fine-tune API request failed: %w", err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxAPIResponseBytes+1))
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("learning: reading fine-tune API response: %w", &url.Error{Op: method, URL: c.baseURL + path, Err: err})
	}
	if len(data) > maxAPIResponseBytes {
		return fmt.Errorf("learning: fine-tune API response exceeds %d bytes", maxAPIResponseBytes)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return c.apiError(resp, data)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("learning: decoding fine-tune API response: %w", err)
	}
	return nil
}

func (c *OpenAIFineTuner) apiError(resp *http.Response, data []byte) *APIError {
	e := &APIError{StatusCode: resp.StatusCode}
	if ra := strings.TrimSpace(resp.Header.Get("Retry-After")); ra != "" {
		if secs, err := strconv.Atoi(ra); err == nil && secs > 0 {
			e.RetryAfter = min(time.Duration(secs)*time.Second, maxRetryAfter)
		}
	}
	var env struct {
		Error json.RawMessage `json:"error"`
	}
	if err := json.Unmarshal(data, &env); err == nil && len(env.Error) > 0 {
		var detail struct {
			Message string          `json:"message"`
			Type    string          `json:"type"`
			Code    json.RawMessage `json:"code"`
			Param   json.RawMessage `json:"param"`
		}
		var msg string
		switch {
		case json.Unmarshal(env.Error, &msg) == nil:
			e.Message = msg
		case json.Unmarshal(env.Error, &detail) == nil:
			e.Message = detail.Message
			e.Type = detail.Type
			e.Code = rawScalar(detail.Code)
			e.Param = rawScalar(detail.Param)
		}
	}
	if e.Message == "" {
		e.Message = http.StatusText(resp.StatusCode)
		if body := strings.TrimSpace(string(data)); body != "" && !strings.HasPrefix(body, "<") {
			e.Message = body
		}
	}
	e.Message = truncate(c.redact(e.Message), maxAPIErrorMessage)
	e.Type = truncate(c.redact(e.Type), 64)
	e.Code = truncate(c.redact(e.Code), 64)
	e.Param = truncate(c.redact(e.Param), 64)
	return e
}

// rawScalar renders a JSON string/number/bool as text ("" for null/absent).
func rawScalar(raw json.RawMessage) string {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	if raw[0] == '{' || raw[0] == '[' {
		return ""
	}
	return string(raw)
}

func (c *OpenAIFineTuner) redact(s string) string {
	if c.cfg.APIKey != "" {
		s = strings.ReplaceAll(s, c.cfg.APIKey, "[REDACTED]")
	}
	return s
}

func truncate(s string, n int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 && r != '\t' {
			return ' '
		}
		return r
	}, s)
	if len(s) <= n {
		return s
	}
	// Cut on a rune boundary.
	cut := n
	for cut > 0 && !utf8RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "..."
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

type openAIJob struct {
	ID             string  `json:"id"`
	Model          string  `json:"model"`
	Status         string  `json:"status"`
	TrainingFile   string  `json:"training_file"`
	FineTunedModel *string `json:"fine_tuned_model"`
	CreatedAt      int64   `json:"created_at"`
	FinishedAt     *int64  `json:"finished_at"`
	TrainedTokens  *int64  `json:"trained_tokens"`
	Error          *struct {
		Code    json.RawMessage `json:"code"`
		Message string          `json:"message"`
	} `json:"error"`
}

func (j *openAIJob) record() (FineTuneJobRecord, error) {
	if !remoteIDPattern.MatchString(j.ID) {
		return FineTuneJobRecord{}, fmt.Errorf("%w: API returned an invalid job id", ErrInvalidJobID)
	}
	rec := FineTuneJobRecord{
		ID:     j.ID,
		Model:  truncate(j.Model, maxRecordField),
		Status: truncate(j.Status, 64),
	}
	if remoteIDPattern.MatchString(j.TrainingFile) {
		rec.TrainingFile = j.TrainingFile
	}
	if j.FineTunedModel != nil {
		rec.FineTunedModel = truncate(*j.FineTunedModel, maxRecordField)
	}
	if j.CreatedAt > 0 {
		rec.CreatedAt = time.Unix(j.CreatedAt, 0).UTC()
	}
	if j.FinishedAt != nil && *j.FinishedAt > 0 {
		t := time.Unix(*j.FinishedAt, 0).UTC()
		rec.FinishedAt = &t
	}
	if j.TrainedTokens != nil && *j.TrainedTokens > 0 {
		rec.TrainedTokens = *j.TrainedTokens
	}
	if j.Error != nil {
		rec.ErrorCode = truncate(rawScalar(j.Error.Code), 64)
		rec.ErrorMessage = truncate(j.Error.Message, maxAPIErrorMessage)
	}
	return rec, nil
}

// forEachJSONLLine calls fn for every non-blank line (1-based line numbers).
func forEachJSONLLine(data []byte, fn func(lineNo int, line []byte) error) error {
	lineNo := 0
	for len(data) > 0 {
		lineNo++
		var line []byte
		if i := bytes.IndexByte(data, '\n'); i >= 0 {
			line, data = data[:i], data[i+1:]
		} else {
			line, data = data, nil
		}
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		if err := fn(lineNo, line); err != nil {
			return err
		}
	}
	return nil
}

// ValidateFineTuneJSONL checks that data is JSONL with one JSON object per
// non-blank line and returns the number of examples. At least one example
// is required.
func ValidateFineTuneJSONL(data []byte) (int, error) {
	n := 0
	err := forEachJSONLLine(data, func(lineNo int, line []byte) error {
		if line[0] != '{' || !json.Valid(line) {
			return fmt.Errorf("%w: line %d is not a JSON object", ErrInvalidDataset, lineNo)
		}
		n++
		return nil
	})
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, fmt.Errorf("%w: no examples", ErrInvalidDataset)
	}
	return n, nil
}

type chatTextMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// FlywheelToChatJSONL converts the flywheel export format (one
// {"request","response","rating"} object per line, where request/response
// are the raw ledger payloads) into OpenAI chat fine-tuning JSONL
// ({"messages":[...]} per line). Lines already carrying "messages" pass
// through unchanged. Requests contribute their "messages" array, or a user
// message from "prompt"/"input"; responses contribute the assistant text
// from choices[0].message.content, choices[0].text, "text", "output_text"
// or text content blocks. Plain-text payloads are used verbatim; JSON
// payloads of unknown shape and SSE streams are skipped. It returns the
// converted JSONL and the number of examples.
func FlywheelToChatJSONL(payload []byte) ([]byte, int, error) {
	var out bytes.Buffer
	n := 0
	err := forEachJSONLLine(payload, func(lineNo int, line []byte) error {
		var obj map[string]json.RawMessage
		if err := json.Unmarshal(line, &obj); err != nil || obj == nil {
			return fmt.Errorf("%w: line %d is not a JSON object", ErrInvalidDataset, lineNo)
		}
		if _, ok := obj["messages"]; ok {
			out.Write(line)
			out.WriteByte('\n')
			n++
			return nil
		}
		var req, resp string
		if json.Unmarshal(obj["request"], &req) != nil || json.Unmarshal(obj["response"], &resp) != nil {
			return nil
		}
		msgs, ok := requestMessages(req)
		if !ok {
			return nil
		}
		answer, ok := responseText(resp)
		if !ok {
			return nil
		}
		assistant, err := json.Marshal(chatTextMessage{Role: "assistant", Content: answer})
		if err != nil {
			return fmt.Errorf("learning: encoding example: %w", err)
		}
		b, err := json.Marshal(struct {
			Messages []json.RawMessage `json:"messages"`
		}{Messages: append(msgs, assistant)})
		if err != nil {
			return fmt.Errorf("learning: encoding example: %w", err)
		}
		out.Write(b)
		out.WriteByte('\n')
		n++
		return nil
	})
	if err != nil {
		return nil, 0, err
	}
	return out.Bytes(), n, nil
}

func isSSE(s string) bool {
	return strings.HasPrefix(s, "data:") || strings.HasPrefix(s, "event:")
}

func userMessage(content string) []json.RawMessage {
	b, err := json.Marshal(chatTextMessage{Role: "user", Content: content})
	if err != nil {
		return nil
	}
	return []json.RawMessage{b}
}

func requestMessages(raw string) ([]json.RawMessage, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || isSSE(s) {
		return nil, false
	}
	if !json.Valid([]byte(s)) {
		return userMessage(s), true
	}
	var r struct {
		Messages []json.RawMessage `json:"messages"`
		Prompt   json.RawMessage   `json:"prompt"`
		Input    json.RawMessage   `json:"input"`
	}
	if json.Unmarshal([]byte(s), &r) != nil {
		var str string
		if json.Unmarshal([]byte(s), &str) == nil && strings.TrimSpace(str) != "" {
			return userMessage(str), true
		}
		return nil, false
	}
	if len(r.Messages) > 0 {
		for _, m := range r.Messages {
			var msg struct {
				Role string `json:"role"`
			}
			if json.Unmarshal(m, &msg) != nil || msg.Role == "" {
				return nil, false
			}
		}
		return append([]json.RawMessage(nil), r.Messages...), true
	}
	for _, field := range []json.RawMessage{r.Prompt, r.Input} {
		var str string
		if json.Unmarshal(field, &str) == nil && strings.TrimSpace(str) != "" {
			return userMessage(str), true
		}
	}
	return nil, false
}

// textFromContent extracts text from a string or an array of
// {"type":"text","text":...} blocks.
func textFromContent(raw json.RawMessage) string {
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(raw, &blocks) != nil {
		return ""
	}
	var sb strings.Builder
	for _, b := range blocks {
		if b.Type == "text" || b.Type == "output_text" {
			sb.WriteString(b.Text)
		}
	}
	return sb.String()
}

func responseText(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || isSSE(s) {
		return "", false
	}
	if !json.Valid([]byte(s)) {
		return s, true
	}
	var r struct {
		Choices []struct {
			Message *struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
			Text *string `json:"text"`
		} `json:"choices"`
		Text       *string         `json:"text"`
		OutputText *string         `json:"output_text"`
		Content    json.RawMessage `json:"content"`
	}
	if json.Unmarshal([]byte(s), &r) != nil {
		var str string
		if json.Unmarshal([]byte(s), &str) == nil && strings.TrimSpace(str) != "" {
			return str, true
		}
		return "", false
	}
	var candidates []string
	if len(r.Choices) > 0 {
		if m := r.Choices[0].Message; m != nil {
			candidates = append(candidates, textFromContent(m.Content))
		}
		if r.Choices[0].Text != nil {
			candidates = append(candidates, *r.Choices[0].Text)
		}
	}
	if r.Text != nil {
		candidates = append(candidates, *r.Text)
	}
	if r.OutputText != nil {
		candidates = append(candidates, *r.OutputText)
	}
	if len(r.Content) > 0 {
		candidates = append(candidates, textFromContent(r.Content))
	}
	for _, c := range candidates {
		if strings.TrimSpace(c) != "" {
			return c, true
		}
	}
	return "", false
}
