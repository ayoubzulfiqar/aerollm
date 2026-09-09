package batch

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// Status represents the lifecycle state of a batch job.
type Status string

const (
	StatusValidating  Status = "validating"
	StatusInProgress  Status = "in_progress"
	StatusCompleted   Status = "completed"
	StatusFailed      Status = "failed"
	StatusCancelled   Status = "cancelled"
)

// BatchRequest is a single line in the input JSONL file.
// It mirrors the OpenAI Batch API request line format.
type BatchRequest struct {
	CustomID string          `json:"custom_id"`
	Method   string          `json:"method"`   // "POST"
	Endpoint string          `json:"endpoint"` // "/v1/chat/completions"
	Body     json.RawMessage `json:"body"`     // The actual LLMRequest JSON
}

// BatchResponse is a single line in the output JSONL file.
// @Description BatchResponse represents the result of a single batch request line.
type BatchResponse struct {
	CustomID string `json:"custom_id"`
	// Response is the raw JSON response from the provider (LLMResponse or error).
	// swag:ignore — swag cannot introspect json.RawMessage
	Response json.RawMessage `json:"response"`
}

// Batch represents a batch processing job.
type Batch struct {
	ID          string        `json:"id"`
	Status      Status        `json:"status"`
	Object      string        `json:"object"` // "batch"
	CreatedAt   time.Time     `json:"created_at"`
	CompletedAt *time.Time    `json:"completed_at,omitempty"`
	InputFileID string        `json:"input_file_id"`
	OutputFileID string       `json:"output_file_id,omitempty"`
	Error       string        `json:"error,omitempty"`
	TotalRequests int         `json:"total_requests"`
	CompletedRequests int    `json:"completed_requests"`
	FailedRequests int       `json:"failed_requests"`
}

// BatchStore persists batch metadata and results.
type BatchStore interface {
	SaveBatch(ctx context.Context, b *Batch) error
	GetBatch(ctx context.Context, id string) (*Batch, error)
	UpdateBatch(ctx context.Context, b *Batch) error
	ListBatches(ctx context.Context) ([]*Batch, error)
}

// ProviderResolver resolves a model string to a Provider.
type ProviderResolver func(model string) (providers.Provider, bool)

// BatchProcessor processes batch jobs asynchronously.
// It reads a JSONL input file, sends each line through the LLM provider
// pipeline (respecting FinOps, Guardrails, etc. via the router), and writes
// results to an output JSONL file.
type BatchProcessor struct {
	store       BatchStore
	resolver    ProviderResolver
	workDir     string
	concurrency int
}

// BatchProcessorConfig holds configuration for the BatchProcessor.
type BatchProcessorConfig struct {
	WorkDir     string
	Concurrency int
}

// NewBatchProcessor creates a new async batch processor.
func NewBatchProcessor(store BatchStore, resolver ProviderResolver, cfg BatchProcessorConfig) *BatchProcessor {
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 4
	}
	if cfg.WorkDir == "" {
		cfg.WorkDir = os.TempDir()
	}
	return &BatchProcessor{
		store:       store,
		resolver:    resolver,
		workDir:     cfg.WorkDir,
		concurrency: cfg.Concurrency,
	}
}

// WorkDir returns the working directory where batch input/output files are stored.
func (bp *BatchProcessor) WorkDir() string {
	return bp.workDir
}

// CreateBatch validates the input file, creates a batch record, and launches
// the async processing goroutine.
func (bp *BatchProcessor) CreateBatch(ctx context.Context, inputFileID string, inputData []byte) (*Batch, error) {
	// Persist the input file to disk.
	inputPath := filepath.Join(bp.workDir, inputFileID+".jsonl")
	if err := os.WriteFile(inputPath, inputData, 0644); err != nil {
		return nil, fmt.Errorf("writing input file: %w", err)
	}

	// Parse to count total requests.
	total, err := countLines(inputData)
	if err != nil {
		return nil, fmt.Errorf("parsing input file: %w", err)
	}

	batch := &Batch{
		ID:              "batch_" + inputFileID,
		Status:          StatusValidating,
		Object:          "batch",
		CreatedAt:       time.Now().UTC(),
		InputFileID:     inputFileID,
		TotalRequests:   total,
	}

	if err := bp.store.SaveBatch(ctx, batch); err != nil {
		return nil, fmt.Errorf("saving batch: %w", err)
	}

	// Launch async processing.
	go bp.processBatch(batch, inputPath)

	return batch, nil
}

// processBatch runs the full batch lifecycle: validating → in_progress → completed/failed.
func (bp *BatchProcessor) processBatch(batch *Batch, inputPath string) {
	ctx := context.Background()

	// Phase 1: Validating
	batch.Status = StatusValidating
	_ = bp.store.UpdateBatch(ctx, batch)

	// Read and validate input file.
	file, err := os.Open(inputPath)
	if err != nil {
		batch.Status = StatusFailed
		batch.Error = fmt.Sprintf("cannot open input file: %v", err)
		now := time.Now().UTC()
		batch.CompletedAt = &now
		_ = bp.store.UpdateBatch(ctx, batch)
		return
	}
	defer file.Close()

	// Phase 2: In Progress
	batch.Status = StatusInProgress
	_ = bp.store.UpdateBatch(ctx, batch)

	// Prepare output file.
	outputPath := filepath.Join(bp.workDir, "output_"+batch.ID+".jsonl")
	outputFile, err := os.Create(outputPath)
	if err != nil {
		batch.Status = StatusFailed
		batch.Error = fmt.Sprintf("cannot create output file: %v", err)
		now := time.Now().UTC()
		batch.CompletedAt = &now
		_ = bp.store.UpdateBatch(ctx, batch)
		return
	}
	defer outputFile.Close()

	var (
		completed int64
		failed    int64
		wg        sync.WaitGroup
		sem       = make(chan struct{}, bp.concurrency)
	)

	// Stream JSONL lines and process concurrently with bounded parallelism.
	// Each line is a BatchRequest JSON object.
	fileContent, err := os.ReadFile(inputPath)
	if err != nil {
		batch.Status = StatusFailed
		batch.Error = fmt.Sprintf("cannot read input file: %v", err)
		now := time.Now().UTC()
		batch.CompletedAt = &now
		_ = bp.store.UpdateBatch(ctx, batch)
		return
	}

	// Split into lines and process concurrently.
	for _, line := range splitLines(string(fileContent)) {
		line = trimSpace(line)
		if line == "" {
			continue
		}

		wg.Add(1)
		sem <- struct{}{} // acquire slot
		go func(line string) {
			defer wg.Done()
			defer func() { <-sem }() // release slot

			var req BatchRequest
			if err := json.Unmarshal([]byte(line), &req); err != nil {
				batch.addResult(outputFile, BatchResponse{
					CustomID: req.CustomID,
					Response: []byte(`{"error":"invalid JSON line"}`),
				})
				atomic.AddInt64(&failed, 1)
				return
			}

			// Only POST /v1/chat/completions is supported in batches.
			var llmReq models.LLMRequest
			if err := json.Unmarshal(req.Body, &llmReq); err != nil {
				batch.addResult(outputFile, BatchResponse{
					CustomID: req.CustomID,
					Response: []byte(`{"error":"invalid request body"}`),
				})
				atomic.AddInt64(&failed, 1)
				return
			}

			// Resolve provider via the resolver (goes through router + circuit breaker).
			provider, ok := bp.resolver(llmReq.Model)
			if !ok {
				batch.addResult(outputFile, BatchResponse{
					CustomID: req.CustomID,
					Response: []byte(`{"error":"model not found"}`),
				})
				atomic.AddInt64(&failed, 1)
				return
			}

			// Execute the LLM call (this goes through the full provider pipeline).
			resp, err := provider.ChatCompletions(ctx, &llmReq)
			if err != nil {
				batch.addResult(outputFile, BatchResponse{
					CustomID: req.CustomID,
					Response: []byte(`{"error":"` + err.Error() + `"}`),
				})
				atomic.AddInt64(&failed, 1)
				return
			}

			respBytes, err := json.Marshal(resp)
			if err != nil {
				batch.addResult(outputFile, BatchResponse{
					CustomID: req.CustomID,
					Response: []byte(`{"error":"failed to marshal response"}`),
				})
				atomic.AddInt64(&failed, 1)
				return
			}

			batch.addResult(outputFile, BatchResponse{
				CustomID: req.CustomID,
				Response: respBytes,
			})
			atomic.AddInt64(&completed, 1)
		}(line)
	}

	wg.Wait()

	// Phase 3: Complete
	batch.CompletedRequests = int(completed)
	batch.FailedRequests = int(failed)
	batch.Status = StatusCompleted
	batch.OutputFileID = "output_" + batch.ID
	now := time.Now().UTC()
	batch.CompletedAt = &now
	_ = bp.store.UpdateBatch(ctx, batch)
	_ = outputFile.Close()
}

// addResult safely writes a single JSON line to the output file.
func (b *Batch) addResult(f *os.File, resp BatchResponse) {
	respBytes, _ := json.Marshal(resp)
	respBytes = append(respBytes, '\n')
	f.Write(respBytes)
}

// splitLines splits a string into lines (newline-delimited).
func splitLines(s string) []string {
	var lines []string
	current := ""
	for _, ch := range s {
		if ch == '\n' {
			lines = append(lines, current)
			current = ""
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		lines = append(lines, current)
	}
	return lines
}

// trimSpace removes leading/trailing whitespace.
func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && (s[start] == ' ' || s[start] == '\t' || s[start] == '\r' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '\t' || s[end-1] == '\r' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}

// countLines counts non-empty lines in JSONL data.
func countLines(data []byte) (int, error) {
	count := 0
	for _, line := range splitLines(string(data)) {
		if trimSpace(line) != "" {
			count++
		}
	}
	return count, nil
}
