package batch

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// Status represents the lifecycle state of a batch job (OpenAI semantics):
//
//	validating -> in_progress -> finalizing -> completed
//	validating -> failed                     (invalid input)
//	in_progress -> cancelling -> cancelled   (CancelBatch)
//	in_progress -> expired                   (completion window elapsed)
//	in_progress -> failed                    (processor shut down / I/O error)
type Status string

const (
	StatusValidating Status = "validating"
	StatusInProgress Status = "in_progress"
	StatusFinalizing Status = "finalizing"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
	StatusExpired    Status = "expired"
	StatusCancelling Status = "cancelling"
	StatusCancelled  Status = "cancelled"
)

// Terminal reports whether no further transitions can happen.
func (s Status) Terminal() bool {
	switch s {
	case StatusCompleted, StatusFailed, StatusExpired, StatusCancelled:
		return true
	}
	return false
}

// Limits and defaults.
const (
	// EndpointChatCompletions is the only endpoint supported in batches.
	EndpointChatCompletions = "/v1/chat/completions"
	// DefaultMaxRequests matches OpenAI's per-batch request limit.
	DefaultMaxRequests = 50_000
	// DefaultMaxInputBytes bounds the input file size.
	DefaultMaxInputBytes = 100 << 20
	// MaxLineBytes bounds a single JSONL line.
	MaxLineBytes = 8 << 20
	// DefaultCompletionWindow is the time a batch may take before expiring.
	DefaultCompletionWindow = 24 * time.Hour
	// DefaultRequestTimeout bounds each individual upstream call.
	DefaultRequestTimeout = 10 * time.Minute

	maxValidationErrors = 100
	maxCustomIDLen      = 512
	maxMetadataKeys     = 16
	maxMetadataKeyLen   = 64
	maxMetadataValueLen = 512
	maxErrorMessageLen  = 1000
	progressSaveEvery   = 250 * time.Millisecond
)

var (
	// ErrNotCancellable is returned by CancelBatch for finished batches.
	ErrNotCancellable = errors.New("batch: batch is not in a cancellable state")
	// ErrProcessorClosed is returned after Shutdown.
	ErrProcessorClosed = errors.New("batch: processor is shut down")
	// ErrInvalidInput is returned for requests that are rejected before a
	// batch is created (oversized input, bad metadata or window).
	ErrInvalidInput = errors.New("batch: invalid input")
	// ErrResultsNotReady is returned when no output file exists yet.
	ErrResultsNotReady = errors.New("batch: results not available yet")
)

// BatchRequest is a single line in the input JSONL file.
// It mirrors the OpenAI Batch API request line format.
type BatchRequest struct {
	CustomID string `json:"custom_id"`
	Method   string `json:"method"` // "POST"
	URL      string `json:"url"`    // "/v1/chat/completions"
	// Endpoint is a legacy alias of URL.
	Endpoint string          `json:"endpoint,omitempty"`
	Body     json.RawMessage `json:"body"` // The actual LLMRequest JSON
}

// BatchError describes a validation or per-request error.
type BatchError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Line    int    `json:"line,omitempty"`
	Param   string `json:"param,omitempty"`
}

// BatchResponseBody is the "response" object of a successful output line.
type BatchResponseBody struct {
	StatusCode int             `json:"status_code"`
	RequestID  string          `json:"request_id"`
	Body       json.RawMessage `json:"body"`
}

// BatchResponse is a single line in the output (or error) JSONL file.
// @Description BatchResponse represents the result of a single batch request line.
type BatchResponse struct {
	ID       string `json:"id,omitempty"`
	CustomID string `json:"custom_id"`
	// Response is {"status_code","request_id","body"} for successful lines
	// and null for failed ones.
	// swag:ignore — swag cannot introspect json.RawMessage
	Response json.RawMessage `json:"response"`
	Error    *BatchError     `json:"error"`
}

// RequestCounts tracks per-request progress.
type RequestCounts struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

// Batch represents a batch processing job.
type Batch struct {
	ID               string            `json:"id"`
	Object           string            `json:"object"` // "batch"
	Endpoint         string            `json:"endpoint"`
	Status           Status            `json:"status"`
	InputFileID      string            `json:"input_file_id"`
	OutputFileID     string            `json:"output_file_id,omitempty"`
	ErrorFileID      string            `json:"error_file_id,omitempty"`
	CompletionWindow string            `json:"completion_window"`
	CreatedAt        time.Time         `json:"created_at"`
	InProgressAt     *time.Time        `json:"in_progress_at,omitempty"`
	FinalizingAt     *time.Time        `json:"finalizing_at,omitempty"`
	CompletedAt      *time.Time        `json:"completed_at,omitempty"`
	FailedAt         *time.Time        `json:"failed_at,omitempty"`
	ExpiresAt        *time.Time        `json:"expires_at,omitempty"`
	ExpiredAt        *time.Time        `json:"expired_at,omitempty"`
	CancellingAt     *time.Time        `json:"cancelling_at,omitempty"`
	CancelledAt      *time.Time        `json:"cancelled_at,omitempty"`
	Error            string            `json:"error,omitempty"`
	Errors           []BatchError      `json:"errors,omitempty"`
	RequestCounts    RequestCounts     `json:"request_counts"`
	Metadata         map[string]string `json:"metadata,omitempty"`
	// Flat counters kept for backward compatibility (mirror RequestCounts).
	TotalRequests     int `json:"total_requests"`
	CompletedRequests int `json:"completed_requests"`
	FailedRequests    int `json:"failed_requests"`
	// Owner identifies the creator (e.g. a tenant ID). It is never serialised.
	Owner string `json:"-"`
}

func cloneTime(t *time.Time) *time.Time {
	if t == nil {
		return nil
	}
	c := *t
	return &c
}

// Clone returns a deep copy of the batch.
func (b *Batch) Clone() *Batch {
	if b == nil {
		return nil
	}
	c := *b
	for _, p := range []**time.Time{&c.InProgressAt, &c.FinalizingAt, &c.CompletedAt, &c.FailedAt, &c.ExpiresAt, &c.ExpiredAt, &c.CancellingAt, &c.CancelledAt} {
		*p = cloneTime(*p)
	}
	c.Errors = append([]BatchError(nil), b.Errors...)
	if b.Metadata != nil {
		c.Metadata = make(map[string]string, len(b.Metadata))
		for k, v := range b.Metadata {
			c.Metadata[k] = v
		}
	}
	return &c
}

func (b *Batch) syncCounts() {
	b.TotalRequests = b.RequestCounts.Total
	b.CompletedRequests = b.RequestCounts.Completed
	b.FailedRequests = b.RequestCounts.Failed
}

// BatchStore persists batch metadata and results.
type BatchStore interface {
	SaveBatch(ctx context.Context, b *Batch) error
	GetBatch(ctx context.Context, id string) (*Batch, error)
	UpdateBatch(ctx context.Context, b *Batch) error
	ListBatches(ctx context.Context) ([]*Batch, error)
}

// ProviderResolver resolves a model string to a Provider. It is called for
// every request, so a resolver backed by a hot-reloadable registry picks
// up configuration changes mid-batch.
type ProviderResolver func(model string) (providers.Provider, bool)

// RequestResult is reported to BatchProcessorConfig.OnResult after every
// executed request line (for usage/cost accounting).
type RequestResult struct {
	BatchID  string
	Owner    string
	CustomID string
	Model    string
	Provider string
	Usage    *models.Usage
	Err      error
	Latency  time.Duration
}

// BatchProcessorConfig holds configuration for the BatchProcessor.
type BatchProcessorConfig struct {
	// WorkDir is where batch files are stored (default os.TempDir()). Unless
	// PersistentWorkDir is set, files live in a private (0700) directory
	// created inside WorkDir and removed on Shutdown.
	WorkDir           string
	PersistentWorkDir bool
	// Concurrency bounds parallel upstream calls per batch (default 4).
	Concurrency      int
	MaxRequests      int
	MaxInputBytes    int
	RequestTimeout   time.Duration
	CompletionWindow time.Duration
	// Context, when set, bounds the processor lifetime: when it is done,
	// running batches stop.
	Context context.Context
	// OnResult, if set, is called after every executed request.
	OnResult func(ctx context.Context, r RequestResult)
	// Admit, if set, is consulted before each request; an error fails that
	// line with code "request_rejected" (e.g. budget exhausted).
	Admit func(ctx context.Context, b *Batch, req *models.LLMRequest) error
	// SuspendOnShutdown leaves batches interrupted by Shutdown (or by
	// Context ending) in their in-progress state instead of failing them,
	// so a later processor over the same store and work directory resumes
	// them in Recover. Use it with a PersistentStore and PersistentWorkDir.
	SuspendOnShutdown bool
}

// CreateOptions configures CreateBatchWithOptions.
type CreateOptions struct {
	InputFileID string
	Data        []byte
	// Owner is recorded on the batch and checked by the *ForOwner accessors.
	Owner            string
	Metadata         map[string]string
	CompletionWindow time.Duration
}

// ListOptions paginates ListBatches (newest first).
type ListOptions struct {
	Owner string
	// After is the ID of the last batch of the previous page.
	After string
	Limit int // default 20, max 100
}

// ListResult is a page of batches.
type ListResult struct {
	Data    []*Batch `json:"data"`
	FirstID string   `json:"first_id,omitempty"`
	LastID  string   `json:"last_id,omitempty"`
	HasMore bool     `json:"has_more"`
}

type parsedLine struct {
	line     int
	customID string
	req      models.LLMRequest
}

type runState struct {
	mu              sync.Mutex
	batch           *Batch
	cancel          context.CancelFunc
	cancelRequested bool
	aborted         []parsedLine
	lastSave        time.Time
	trailingSave    *time.Timer
	finished        bool
}

// BatchProcessor processes batch jobs asynchronously with bounded
// concurrency, writing results to per-batch JSONL files.
type BatchProcessor struct {
	store    BatchStore
	resolver atomic.Pointer[ProviderResolver]
	cfg      BatchProcessorConfig
	workDir  string
	ownsDir  bool
	initErr  error

	baseCtx    context.Context
	baseCancel context.CancelFunc

	mu      sync.Mutex
	running map[string]*runState
	closed  bool
	wg      sync.WaitGroup

	now func() time.Time
}

// NewBatchProcessor creates a new async batch processor.
func NewBatchProcessor(store BatchStore, resolver ProviderResolver, cfg BatchProcessorConfig) *BatchProcessor {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir = os.TempDir()
	}
	if cfg.MaxRequests <= 0 {
		cfg.MaxRequests = DefaultMaxRequests
	}
	if cfg.MaxInputBytes <= 0 {
		cfg.MaxInputBytes = DefaultMaxInputBytes
	}
	if cfg.RequestTimeout <= 0 {
		cfg.RequestTimeout = DefaultRequestTimeout
	}
	if cfg.CompletionWindow <= 0 {
		cfg.CompletionWindow = DefaultCompletionWindow
	}
	if store == nil {
		store = NewInMemoryStore()
	}
	parent := cfg.Context
	if parent == nil {
		parent = context.Background()
	}
	baseCtx, cancel := context.WithCancel(parent)
	bp := &BatchProcessor{
		store:      store,
		cfg:        cfg,
		baseCtx:    baseCtx,
		baseCancel: cancel,
		running:    make(map[string]*runState),
		now:        time.Now,
	}
	bp.SetResolver(resolver)
	if cfg.PersistentWorkDir {
		bp.workDir = filepath.Clean(cfg.WorkDir)
		bp.initErr = os.MkdirAll(bp.workDir, 0o700)
	} else {
		dir, err := os.MkdirTemp(cfg.WorkDir, "aerollm-batches-")
		bp.workDir, bp.ownsDir, bp.initErr = dir, err == nil, err
		if err != nil {
			bp.workDir = filepath.Clean(cfg.WorkDir)
		}
	}
	return bp
}

// SetResolver atomically replaces the provider resolver (hot reload).
func (bp *BatchProcessor) SetResolver(r ProviderResolver) {
	if r == nil {
		bp.resolver.Store(nil)
		return
	}
	bp.resolver.Store(&r)
}

func (bp *BatchProcessor) currentResolver() ProviderResolver {
	if p := bp.resolver.Load(); p != nil {
		return *p
	}
	return nil
}

// WorkDir returns the directory where batch input/output files are stored.
// Output files are named output_<batch id>.jsonl.
func (bp *BatchProcessor) WorkDir() string {
	return bp.workDir
}

var batchIDRe = regexp.MustCompile(`^batch_[0-9a-f]{24}$`)

// ValidBatchID reports whether id has the format generated by this package.
func ValidBatchID(id string) bool { return batchIDRe.MatchString(id) }

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(fmt.Sprintf("batch: crypto/rand failed: %v", err))
	}
	return hex.EncodeToString(b)
}

func (bp *BatchProcessor) path(kind, id string) (string, error) {
	if !ValidBatchID(id) {
		return "", ErrBatchNotFound
	}
	return filepath.Join(bp.workDir, kind+"_"+id+".jsonl"), nil
}

// OutputPath returns the output (successful results) file path of a batch.
func (bp *BatchProcessor) OutputPath(id string) (string, error) { return bp.path("output", id) }

// ErrorPath returns the error file path of a batch.
func (bp *BatchProcessor) ErrorPath(id string) (string, error) { return bp.path("errors", id) }

var fileIDRe = regexp.MustCompile(`[^A-Za-z0-9._-]`)

func sanitizeFileID(s string) string {
	s = fileIDRe.ReplaceAllString(s, "_")
	if len(s) > 128 {
		s = s[:128]
	}
	if strings.Trim(s, "._") == "" {
		return "file-" + randomHex(12)
	}
	return s
}

func validateMetadata(md map[string]string) error {
	if len(md) > maxMetadataKeys {
		return fmt.Errorf("%w: at most %d metadata keys", ErrInvalidInput, maxMetadataKeys)
	}
	for k, v := range md {
		if k == "" || len(k) > maxMetadataKeyLen || len(v) > maxMetadataValueLen {
			return fmt.Errorf("%w: metadata keys must be 1-%d bytes and values at most %d bytes", ErrInvalidInput, maxMetadataKeyLen, maxMetadataValueLen)
		}
	}
	return nil
}

// CreateBatch validates the input file, creates a batch record, and launches
// the async processing goroutine.
func (bp *BatchProcessor) CreateBatch(ctx context.Context, inputFileID string, inputData []byte) (*Batch, error) {
	return bp.CreateBatchWithOptions(ctx, CreateOptions{InputFileID: inputFileID, Data: inputData})
}

// CreateBatchWithOptions validates the JSONL input and creates a batch. Input
// that fails validation yields a batch in status "failed" whose Errors list
// the offending lines (OpenAI semantics); the returned error is reserved for
// rejected options and infrastructure failures.
func (bp *BatchProcessor) CreateBatchWithOptions(ctx context.Context, opts CreateOptions) (*Batch, error) {
	if bp.initErr != nil {
		return nil, fmt.Errorf("batch: work directory unavailable: %w", bp.initErr)
	}
	if len(opts.Data) > bp.cfg.MaxInputBytes {
		return nil, fmt.Errorf("%w: input exceeds %d bytes", ErrInvalidInput, bp.cfg.MaxInputBytes)
	}
	if err := validateMetadata(opts.Metadata); err != nil {
		return nil, err
	}
	window := opts.CompletionWindow
	if window <= 0 {
		window = bp.cfg.CompletionWindow
	}
	if window > 7*24*time.Hour {
		return nil, fmt.Errorf("%w: completion window too long", ErrInvalidInput)
	}

	now := bp.now().UTC()
	expires := now.Add(window)
	b := &Batch{
		ID:               "batch_" + randomHex(12),
		Object:           "batch",
		Endpoint:         EndpointChatCompletions,
		Status:           StatusValidating,
		InputFileID:      sanitizeFileID(opts.InputFileID),
		CompletionWindow: window.String(),
		CreatedAt:        now,
		ExpiresAt:        &expires,
		Owner:            opts.Owner,
	}
	if len(opts.Metadata) > 0 {
		b.Metadata = make(map[string]string, len(opts.Metadata))
		for k, v := range opts.Metadata {
			b.Metadata[k] = v
		}
	}

	reqs, verrs := bp.parse(opts.Data)
	if len(verrs) > 0 {
		b.Status = StatusFailed
		b.FailedAt = &now
		b.Errors = verrs
		b.Error = fmt.Sprintf("input validation failed: %s", verrs[0].Message)
		if len(verrs) > 1 {
			b.Error += fmt.Sprintf(" (and %d more)", len(verrs)-1)
		}
		if err := bp.store.SaveBatch(ctx, b); err != nil {
			return nil, fmt.Errorf("saving batch: %w", err)
		}
		return b.Clone(), nil
	}
	b.RequestCounts.Total = len(reqs)
	b.syncCounts()

	if bp.isClosed() {
		return nil, ErrProcessorClosed
	}
	inPath, _ := bp.path("input", b.ID)
	if err := writeExclusive(inPath, opts.Data); err != nil {
		return nil, fmt.Errorf("writing input file: %w", err)
	}
	if err := bp.store.SaveBatch(ctx, b); err != nil {
		_ = os.Remove(inPath)
		return nil, fmt.Errorf("saving batch: %w", err)
	}
	// Register under the lock so Shutdown never misses a started batch.
	bp.mu.Lock()
	defer bp.mu.Unlock()
	if bp.closed {
		t := bp.now().UTC()
		b.Status, b.FailedAt, b.Error = StatusFailed, &t, "processor shut down before the batch started"
		_ = bp.store.UpdateBatch(ctx, b)
		return nil, ErrProcessorClosed
	}
	runCtx, cancel := context.WithDeadline(bp.baseCtx, expires)
	rs := &runState{batch: b.Clone(), cancel: cancel}
	bp.running[b.ID] = rs
	bp.wg.Add(1)
	go bp.run(runCtx, rs, reqs, false)
	return b.Clone(), nil
}

func (bp *BatchProcessor) isClosed() bool {
	bp.mu.Lock()
	defer bp.mu.Unlock()
	return bp.closed
}

// createExclusive creates a new private file, refusing to follow or
// overwrite existing paths (symlink attacks in shared temp dirs).
func createExclusive(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
}

// openAppend opens an existing private result file for appending (creating
// it if needed); it refuses anything but a regular file.
func openAppend(path string) (*os.File, error) {
	if fi, err := os.Lstat(path); err == nil && !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("batch: %s is not a regular file", filepath.Base(path))
	}
	return os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o600)
}

func writeExclusive(path string, data []byte) error {
	f, err := createExclusive(path)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return err
	}
	return f.Close()
}

// parse validates every JSONL line and returns the parsed requests, or the
// validation errors (at most maxValidationErrors).
func (bp *BatchProcessor) parse(data []byte) ([]parsedLine, []BatchError) {
	var errs []BatchError
	addErr := func(code string, line int, format string, args ...interface{}) {
		if len(errs) < maxValidationErrors {
			errs = append(errs, BatchError{Code: code, Line: line, Message: fmt.Sprintf(format, args...)})
		}
	}
	if len(bytes.TrimSpace(data)) == 0 {
		addErr("empty_file", 0, "the input file contains no requests")
		return nil, errs
	}
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64<<10), MaxLineBytes)
	seen := make(map[string]int)
	var reqs []parsedLine
	lineNo := 0
	count := 0
	for sc.Scan() {
		lineNo++
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		count++
		if count > bp.cfg.MaxRequests {
			addErr("too_many_requests", lineNo, "a batch may contain at most %d requests", bp.cfg.MaxRequests)
			break
		}
		var r BatchRequest
		if err := json.Unmarshal(line, &r); err != nil {
			addErr("invalid_json_line", lineNo, "line is not a valid JSON object")
			continue
		}
		bad := false
		switch {
		case r.CustomID == "":
			addErr("missing_custom_id", lineNo, "custom_id is required")
			bad = true
		case len(r.CustomID) > maxCustomIDLen:
			addErr("invalid_custom_id", lineNo, "custom_id must be at most %d bytes", maxCustomIDLen)
			bad = true
		default:
			if prev, dup := seen[r.CustomID]; dup {
				addErr("duplicate_custom_id", lineNo, "custom_id duplicates line %d", prev)
				bad = true
			} else {
				seen[r.CustomID] = lineNo
			}
		}
		if !strings.EqualFold(r.Method, "POST") {
			addErr("invalid_method", lineNo, "method must be POST")
			bad = true
		}
		url := r.URL
		if url == "" {
			url = r.Endpoint
		}
		if url != EndpointChatCompletions {
			addErr("invalid_url", lineNo, "url must be %s", EndpointChatCompletions)
			bad = true
		}
		body := bytes.TrimSpace(r.Body)
		var req models.LLMRequest
		if len(body) == 0 || body[0] != '{' {
			addErr("invalid_body", lineNo, "body must be a JSON object")
			bad = true
		} else if err := json.Unmarshal(body, &req); err != nil {
			addErr("invalid_body", lineNo, "body is not a valid chat completion request")
			bad = true
		} else {
			if req.Model == "" {
				addErr("missing_model", lineNo, "body.model is required")
				bad = true
			}
			if len(req.Messages) == 0 {
				addErr("missing_messages", lineNo, "body.messages must not be empty")
				bad = true
			}
		}
		if bad {
			continue
		}
		req.Stream = false // batches never stream
		reqs = append(reqs, parsedLine{line: lineNo, customID: r.CustomID, req: req})
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			addErr("line_too_long", lineNo+1, "line exceeds %d bytes", MaxLineBytes)
		} else {
			addErr("invalid_file", lineNo+1, "the input file could not be read")
		}
	}
	if len(errs) == 0 && len(reqs) == 0 {
		addErr("empty_file", 0, "the input file contains no requests")
	}
	if len(errs) > 0 {
		return nil, errs
	}
	return reqs, nil
}

// resultWriter serialises writes to the output and error files.
type resultWriter struct {
	mu   sync.Mutex
	out  *bufio.Writer
	errw *bufio.Writer
	err  error
}

func (w *resultWriter) write(toErr bool, line BatchResponse) {
	b, err := json.Marshal(line)
	w.mu.Lock()
	defer w.mu.Unlock()
	if err == nil {
		dst := w.out
		if toErr {
			dst = w.errw
		}
		_, err = dst.Write(append(b, '\n'))
		if err == nil {
			// Hand every line to the OS immediately: after a crash the
			// files then reflect (almost) every executed request, and
			// Recover does not execute them again.
			err = dst.Flush()
		}
	}
	if err != nil && w.err == nil {
		w.err = err
	}
}

func (w *resultWriter) flush() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.out.Flush(); err != nil && w.err == nil {
		w.err = err
	}
	if err := w.errw.Flush(); err != nil && w.err == nil {
		w.err = err
	}
	return w.err
}

func (bp *BatchProcessor) saveLocked(rs *runState, force bool) {
	now := bp.now()
	if wait := progressSaveEvery - now.Sub(rs.lastSave); !force && wait > 0 {
		// Throttled: make sure the latest progress is still saved soon,
		// even if no further request finishes (e.g. the rest block).
		if rs.trailingSave == nil && !rs.finished {
			rs.trailingSave = time.AfterFunc(wait, func() {
				rs.mu.Lock()
				defer rs.mu.Unlock()
				rs.trailingSave = nil
				if !rs.finished {
					bp.saveLocked(rs, true)
				}
			})
		}
		return
	}
	rs.lastSave = now
	rs.batch.syncCounts()
	_ = bp.store.UpdateBatch(context.Background(), rs.batch)
}

// run executes a validated batch. When resume is set the result files of
// an interrupted run are appended to instead of created.
func (bp *BatchProcessor) run(ctx context.Context, rs *runState, reqs []parsedLine, resume bool) {
	defer bp.wg.Done()
	defer rs.cancel()
	id := rs.batch.ID
	defer func() {
		bp.mu.Lock()
		delete(bp.running, id)
		bp.mu.Unlock()
	}()

	rs.mu.Lock()
	if rs.batch.Status == StatusValidating {
		t := bp.now().UTC()
		rs.batch.Status = StatusInProgress
		rs.batch.InProgressAt = &t
		bp.saveLocked(rs, true)
	}
	rs.mu.Unlock()

	outPath, _ := bp.OutputPath(id)
	errPath, _ := bp.ErrorPath(id)
	open := createExclusive
	if resume {
		open = openAppend
	}
	outF, err := open(outPath)
	if err != nil {
		bp.finish(ctx, rs, nil, fmt.Errorf("cannot create output file: %w", err))
		return
	}
	errF, err := open(errPath)
	if err != nil {
		outF.Close()
		bp.finish(ctx, rs, nil, fmt.Errorf("cannot create error file: %w", err))
		return
	}
	w := &resultWriter{out: bufio.NewWriterSize(outF, 64<<10), errw: bufio.NewWriterSize(errF, 16<<10)}

	jobs := make(chan parsedLine)
	var workers sync.WaitGroup
	for i := 0; i < bp.cfg.Concurrency; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for pl := range jobs {
				bp.execute(ctx, rs, w, pl)
			}
		}()
	}
	dispatched := 0
feed:
	for _, pl := range reqs {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			break feed
		case jobs <- pl:
			dispatched++
		}
	}
	close(jobs)
	workers.Wait()

	var unprocessed []parsedLine
	rs.mu.Lock()
	unprocessed = append(append(unprocessed, rs.aborted...), reqs[dispatched:]...)
	rs.mu.Unlock()
	if errors.Is(ctx.Err(), context.DeadlineExceeded) && bp.baseCtx.Err() == nil {
		// Expired: remaining requests are reported in the error file.
		for _, pl := range unprocessed {
			w.write(true, BatchResponse{ID: "batch_req_" + randomHex(12), CustomID: pl.customID, Response: json.RawMessage("null"),
				Error: &BatchError{Code: "batch_expired", Message: "this request could not be executed before the completion window expired"}})
		}
		rs.mu.Lock()
		rs.batch.RequestCounts.Failed += len(unprocessed)
		rs.mu.Unlock()
	}
	werr := w.flush()
	if cerr := outF.Close(); cerr != nil && werr == nil {
		werr = cerr
	}
	if cerr := errF.Close(); cerr != nil && werr == nil {
		werr = cerr
	}
	bp.finish(ctx, rs, w, werr)
}

// finish sets the terminal state (or, when suspending on shutdown, saves
// the final progress of the interrupted run).
func (bp *BatchProcessor) finish(ctx context.Context, rs *runState, w *resultWriter, ioErr error) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.finished = true
	if rs.trailingSave != nil {
		rs.trailingSave.Stop()
		rs.trailingSave = nil
	}
	b := rs.batch
	now := bp.now().UTC()
	if w != nil {
		b.OutputFileID = "output_" + b.ID
		b.ErrorFileID = "errors_" + b.ID
	}
	switch {
	case rs.cancelRequested:
		b.Status = StatusCancelled
		b.CancelledAt = &now
	case bp.baseCtx.Err() != nil && bp.cfg.SuspendOnShutdown && ioErr == nil:
		// Leave the batch resumable: Recover continues it after a restart.
		b.OutputFileID, b.ErrorFileID = "", ""
	case bp.baseCtx.Err() != nil:
		b.Status = StatusFailed
		b.FailedAt = &now
		b.Error = "processor shut down before the batch completed"
		b.Errors = append(b.Errors, BatchError{Code: "shutdown", Message: b.Error})
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		b.Status = StatusExpired
		b.ExpiredAt = &now
	case ioErr != nil:
		b.Status = StatusFailed
		b.FailedAt = &now
		b.Error = "failed to write batch results"
		b.Errors = append(b.Errors, BatchError{Code: "io_error", Message: b.Error})
		b.OutputFileID, b.ErrorFileID = "", ""
	default:
		b.Status = StatusFinalizing
		b.FinalizingAt = &now
		bp.saveLocked(rs, true)
		done := bp.now().UTC()
		b.Status = StatusCompleted
		b.CompletedAt = &done
	}
	bp.saveLocked(rs, true)
}

var secretPatterns = []struct {
	re   *regexp.Regexp
	repl string
}{
	{regexp.MustCompile(`(?i)\b(bearer)\s+[A-Za-z0-9._~+/=-]+`), "$1 [REDACTED]"},
	{regexp.MustCompile(`(?i)\b(api[_-]?key|key|token|secret|password|sig)=([^&\s"']+)`), "$1=[REDACTED]"},
	{regexp.MustCompile(`\b(sk|pk|rk)-[A-Za-z0-9_-]{8,}`), "$1-[REDACTED]"},
}

// sanitizeErrorMessage strips credentials from upstream error messages
// and bounds their length before they are written to result files.
func sanitizeErrorMessage(msg string) string {
	for _, p := range secretPatterns {
		msg = p.re.ReplaceAllString(msg, p.repl)
	}
	if len(msg) > maxErrorMessageLen {
		msg = msg[:maxErrorMessageLen] + "..."
	}
	return msg
}

func (bp *BatchProcessor) snapshot(rs *runState) *Batch {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.batch.Clone()
}

// execute runs one request line and records its outcome.
func (bp *BatchProcessor) execute(ctx context.Context, rs *runState, w *resultWriter, pl parsedLine) {
	start := bp.now()
	req := pl.req
	result := RequestResult{BatchID: rs.batch.ID, Owner: rs.batch.Owner, CustomID: pl.customID, Model: req.Model}

	fail := func(code, msg string, err error) {
		w.write(true, BatchResponse{ID: "batch_req_" + randomHex(12), CustomID: pl.customID, Response: json.RawMessage("null"),
			Error: &BatchError{Code: code, Message: sanitizeErrorMessage(msg)}})
		rs.mu.Lock()
		rs.batch.RequestCounts.Failed++
		bp.saveLocked(rs, false)
		rs.mu.Unlock()
		result.Err = err
		result.Latency = bp.now().Sub(start)
		bp.report(ctx, result)
	}

	if bp.cfg.Admit != nil {
		if err := bp.cfg.Admit(ctx, bp.snapshot(rs), &req); err != nil {
			fail("request_rejected", err.Error(), err)
			return
		}
	}
	resolve := bp.currentResolver()
	var provider providers.Provider
	ok := false
	if resolve != nil {
		provider, ok = resolve(req.Model)
	}
	if !ok || provider == nil {
		fail("model_not_found", fmt.Sprintf("model %q is not available", req.Model), fmt.Errorf("model %q not found", req.Model))
		return
	}
	result.Provider = provider.Name()

	resp, err := func() (resp *models.LLMResponse, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("provider panicked: %v", r)
			}
		}()
		reqCtx, cancel := context.WithTimeout(ctx, bp.cfg.RequestTimeout)
		defer cancel()
		return provider.ChatCompletions(reqCtx, &req)
	}()
	if err != nil {
		if ctx.Err() != nil {
			// Batch-level stop (cancel/expiry/shutdown): not a request failure.
			rs.mu.Lock()
			rs.aborted = append(rs.aborted, pl)
			rs.mu.Unlock()
			return
		}
		code := "provider_error"
		if errors.Is(err, context.DeadlineExceeded) {
			code = "request_timeout"
		}
		fail(code, err.Error(), err)
		return
	}
	if resp == nil {
		fail("provider_error", "provider returned an empty response", errors.New("empty response"))
		return
	}
	body, err := json.Marshal(resp)
	if err != nil {
		fail("internal_error", "failed to encode the response", err)
		return
	}
	requestID := resp.ID
	if requestID == "" {
		requestID = "req_" + randomHex(12)
	}
	envelope, _ := json.Marshal(BatchResponseBody{StatusCode: 200, RequestID: requestID, Body: body})
	w.write(false, BatchResponse{ID: "batch_req_" + randomHex(12), CustomID: pl.customID, Response: envelope})
	rs.mu.Lock()
	rs.batch.RequestCounts.Completed++
	bp.saveLocked(rs, false)
	rs.mu.Unlock()
	result.Usage = resp.Usage
	result.Latency = bp.now().Sub(start)
	bp.report(ctx, result)
}

func (bp *BatchProcessor) report(ctx context.Context, r RequestResult) {
	if bp.cfg.OnResult == nil {
		return
	}
	defer func() { _ = recover() }()
	bp.cfg.OnResult(context.WithoutCancel(ctx), r)
}

// GetBatch returns a batch by ID.
func (bp *BatchProcessor) GetBatch(ctx context.Context, id string) (*Batch, error) {
	if !ValidBatchID(id) {
		return nil, ErrBatchNotFound
	}
	return bp.store.GetBatch(ctx, id)
}

func ownerMatches(b *Batch, owner string) bool {
	return subtle.ConstantTimeCompare([]byte(b.Owner), []byte(owner)) == 1
}

// GetBatchForOwner returns a batch only if it belongs to owner; batches of
// other owners are reported as not found (no existence oracle).
func (bp *BatchProcessor) GetBatchForOwner(ctx context.Context, id, owner string) (*Batch, error) {
	b, err := bp.GetBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ownerMatches(b, owner) {
		return nil, ErrBatchNotFound
	}
	return b, nil
}

// ListBatches returns a page of batches (newest first), optionally
// restricted to one owner.
func (bp *BatchProcessor) ListBatches(ctx context.Context, opts ListOptions) (*ListResult, error) {
	limit := opts.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	all, err := bp.store.ListBatches(ctx)
	if err != nil {
		return nil, err
	}
	sortNewestFirst(all)
	res := &ListResult{Data: []*Batch{}}
	started := opts.After == ""
	for _, b := range all {
		if opts.Owner != "" && !ownerMatches(b, opts.Owner) {
			continue
		}
		if !started {
			if b.ID == opts.After {
				started = true
			}
			continue
		}
		if len(res.Data) == limit {
			res.HasMore = true
			break
		}
		res.Data = append(res.Data, b)
	}
	if n := len(res.Data); n > 0 {
		res.FirstID, res.LastID = res.Data[0].ID, res.Data[n-1].ID
	}
	return res, nil
}

// CancelBatch cancels a validating/in-progress batch. The batch moves to
// "cancelling" immediately and to "cancelled" once in-flight requests stop;
// results produced so far remain available. Cancelling an already
// cancelling batch is a no-op; finished batches return ErrNotCancellable.
func (bp *BatchProcessor) CancelBatch(ctx context.Context, id string) (*Batch, error) {
	if !ValidBatchID(id) {
		return nil, ErrBatchNotFound
	}
	bp.mu.Lock()
	rs := bp.running[id]
	bp.mu.Unlock()
	if rs == nil {
		b, err := bp.store.GetBatch(ctx, id)
		if err != nil {
			return nil, err
		}
		if b.Status.Terminal() {
			return b, ErrNotCancellable
		}
		// Not running in this process (e.g. store shared across restarts).
		now := bp.now().UTC()
		b.Status, b.CancelledAt = StatusCancelled, &now
		if err := bp.store.UpdateBatch(ctx, b); err != nil {
			return nil, err
		}
		return b, nil
	}
	rs.mu.Lock()
	if rs.batch.Status.Terminal() {
		b := rs.batch.Clone()
		rs.mu.Unlock()
		return b, ErrNotCancellable
	}
	if !rs.cancelRequested {
		now := bp.now().UTC()
		rs.cancelRequested = true
		rs.batch.Status = StatusCancelling
		rs.batch.CancellingAt = &now
		bp.saveLocked(rs, true)
	}
	b := rs.batch.Clone()
	rs.mu.Unlock()
	rs.cancel()
	return b, nil
}

// CancelBatchForOwner is CancelBatch restricted to the batch owner.
func (bp *BatchProcessor) CancelBatchForOwner(ctx context.Context, id, owner string) (*Batch, error) {
	if _, err := bp.GetBatchForOwner(ctx, id, owner); err != nil {
		return nil, err
	}
	return bp.CancelBatch(ctx, id)
}

// WaitBatch blocks until the batch reaches a terminal state or ctx is done.
func (bp *BatchProcessor) WaitBatch(ctx context.Context, id string) (*Batch, error) {
	t := time.NewTicker(10 * time.Millisecond)
	defer t.Stop()
	for {
		b, err := bp.GetBatch(ctx, id)
		if err != nil {
			return nil, err
		}
		if b.Status.Terminal() {
			return b, nil
		}
		select {
		case <-ctx.Done():
			return b, ctx.Err()
		case <-t.C:
		}
	}
}

func (bp *BatchProcessor) openFile(ctx context.Context, id, kind string) (io.ReadCloser, error) {
	b, err := bp.GetBatch(ctx, id)
	if err != nil {
		return nil, err
	}
	if !b.Status.Terminal() || b.OutputFileID == "" {
		return nil, ErrResultsNotReady
	}
	p, err := bp.path(kind, id)
	if err != nil {
		return nil, err
	}
	f, err := os.Open(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrResultsNotReady
	}
	return f, err
}

// OpenResults opens the output JSONL (successful requests) of a finished
// batch. Completed, cancelled and expired batches have (partial) results.
func (bp *BatchProcessor) OpenResults(ctx context.Context, id string) (io.ReadCloser, error) {
	return bp.openFile(ctx, id, "output")
}

// OpenErrors opens the error JSONL (failed requests) of a finished batch.
func (bp *BatchProcessor) OpenErrors(ctx context.Context, id string) (io.ReadCloser, error) {
	return bp.openFile(ctx, id, "errors")
}

// ReadResults returns the output JSONL of a finished batch.
func (bp *BatchProcessor) ReadResults(ctx context.Context, id string) ([]byte, error) {
	rc, err := bp.OpenResults(ctx, id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// ReadErrors returns the error JSONL of a finished batch.
func (bp *BatchProcessor) ReadErrors(ctx context.Context, id string) ([]byte, error) {
	rc, err := bp.OpenErrors(ctx, id)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

// batchDeleter is implemented by stores that can delete records.
type batchDeleter interface {
	DeleteBatch(ctx context.Context, id string) error
}

// Cleanup deletes the files (and, when the store supports it, the records)
// of finished batches created before olderThan. It returns how many
// batches were removed.
func (bp *BatchProcessor) Cleanup(ctx context.Context, olderThan time.Time) (int, error) {
	all, err := bp.store.ListBatches(ctx)
	if err != nil {
		return 0, err
	}
	n := 0
	for _, b := range all {
		if !b.Status.Terminal() || !b.CreatedAt.Before(olderThan) || !ValidBatchID(b.ID) {
			continue
		}
		for _, kind := range []string{"input", "output", "errors"} {
			if p, err := bp.path(kind, b.ID); err == nil {
				_ = os.Remove(p)
			}
		}
		if d, ok := bp.store.(batchDeleter); ok {
			_ = d.DeleteBatch(ctx, b.ID)
		}
		n++
	}
	return n, nil
}

// Shutdown stops accepting batches, cancels running ones (they end in
// status "failed") and waits for them until ctx is done. A private work
// directory is removed afterwards.
func (bp *BatchProcessor) Shutdown(ctx context.Context) error {
	bp.mu.Lock()
	if bp.closed {
		bp.mu.Unlock()
		return nil
	}
	bp.closed = true
	bp.mu.Unlock()
	bp.baseCancel()
	done := make(chan struct{})
	go func() {
		bp.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}
	if bp.ownsDir {
		return os.RemoveAll(bp.workDir)
	}
	return nil
}

// Close shuts the processor down, waiting up to 30s.
func (bp *BatchProcessor) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return bp.Shutdown(ctx)
}
