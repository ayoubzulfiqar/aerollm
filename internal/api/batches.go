package api

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/batch"
)

// BatchHandler handles the OpenAI-compatible Batch API endpoints.
type BatchHandler struct {
	Processor *batch.BatchProcessor
	Store     batch.BatchStore
}

// NewBatchHandler creates a new BatchHandler.
func NewBatchHandler(proc *batch.BatchProcessor, store batch.BatchStore) *BatchHandler {
	return &BatchHandler{Processor: proc, Store: store}
}

// BatchObject is the response shape for batch endpoints.
type BatchObject struct {
	ID                string     `json:"id"`
	Object            string     `json:"object"` // "batch"
	Endpoint          string     `json:"endpoint"`
	Status            string     `json:"status"`
	CreatedAt         int64      `json:"created_at"`
	CompletedAt       *int64     `json:"completed_at,omitempty"`
	FailedAt          *int64     `json:"failed_at,omitempty"`
	Errors            []string   `json:"errors,omitempty"`
	InputFileID       string     `json:"input_file_id"`
	OutputFileID      string     `json:"output_file_id,omitempty"`
	Error             string     `json:"error,omitempty"`
	TotalRequests     int        `json:"total_requests"`
	CompletedRequests int        `json:"completed_requests"`
	FailedRequests    int        `json:"failed_requests"`
}

// toBatchObject converts internal Batch to the API response shape.
func toBatchObject(b *batch.Batch) BatchObject {
	bo := BatchObject{
		ID:                b.ID,
		Object:            b.Object,
		Endpoint:          "/v1/chat/completions",
		Status:            string(b.Status),
		CreatedAt:         b.CreatedAt.Unix(),
		InputFileID:       b.InputFileID,
		TotalRequests:     b.TotalRequests,
		CompletedRequests: b.CompletedRequests,
		FailedRequests:    b.FailedRequests,
	}
	if b.CompletedAt != nil {
		ts := b.CompletedAt.Unix()
		bo.CompletedAt = &ts
	}
	if b.Status == batch.StatusFailed {
		bo.Errors = []string{b.Error}
	}
	if b.OutputFileID != "" {
		bo.OutputFileID = b.OutputFileID
	}
	return bo
}

// CreateBatch handles POST /v1/batches.
// Accepts a multipart/form-data upload of a .jsonl file.
// @Summary Create batch
// @Description Create a batch job from an uploaded JSONL file. Each line is a chat completion request.
// @Tags batches
// @Accept multipart/form-data
// @Produce json
// @Param file formData file true "JSONL file containing batch requests"
// @Success 200 {object} BatchObject
// @Failure 400 {object} map[string]string
// @Router /v1/batches [post]
func (h *BatchHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Parse multipart form (max 10MB upload).
	if err := r.ParseMultipartForm(10 << 20); err != nil {
		http.Error(w, `{"error":"failed to parse multipart form"}`, http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, `{"error":"no file uploaded"}`, http.StatusBadRequest)
		return
	}
	defer file.Close()

	// Read the uploaded file content.
	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, `{"error":"failed to read file"}`, http.StatusInternalServerError)
		return
	}

	// Generate a unique file ID from the original filename + timestamp.
	fileID := fmt.Sprintf("%s_%d", sanitizeFilename(header.Filename), time.Now().UnixNano())

	ctx := r.Context()
	b, err := h.Processor.CreateBatch(ctx, fileID, data)
	if err != nil {
		http.Error(w, `{"error":"`+err.Error()+`"}`, http.StatusInternalServerError)
		return
	}

	resp := toBatchObject(b)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(resp)
}

// GetBatch handles GET /v1/batches/{batch_id}.
// @Summary Get batch status
// @Description Returns the current status of a batch job.
// @Tags batches
// @Produce json
// @Param batch_id path string true "Batch ID"
// @Success 200 {object} BatchObject
// @Failure 404 {object} map[string]string
// @Router /v1/batches/{batch_id} [get]
func (h *BatchHandler) GetBatch(w http.ResponseWriter, r *http.Request) {
	batchID := extractBatchID(r.URL.Path)
	if batchID == "" {
		http.Error(w, `{"error":"batch_id is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	b, err := h.Store.GetBatch(ctx, batchID)
	if err != nil {
		http.Error(w, `{"error":"batch not found"}`, http.StatusNotFound)
		return
	}

	resp := toBatchObject(b)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

// GetBatchResults handles GET /v1/batches/{batch_id}/results.
// Returns the .jsonl output file once the batch is completed.
// @Summary Get batch results
// @Description Returns the JSONL results file for a completed batch.
// @Tags batches
// @Produce json
// @Param batch_id path string true "Batch ID"
// @Success 200 {array} batch.BatchResponse
// @Failure 404 {object} map[string]string
// @Router /v1/batches/{batch_id}/results [get]
func (h *BatchHandler) GetBatchResults(w http.ResponseWriter, r *http.Request) {
	batchID := extractBatchID(r.URL.Path)
	if batchID == "" {
		http.Error(w, `{"error":"batch_id is required"}`, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	b, err := h.Store.GetBatch(ctx, batchID)
	if err != nil {
		http.Error(w, `{"error":"batch not found"}`, http.StatusNotFound)
		return
	}

	if b.Status != batch.StatusCompleted {
		http.Error(w, `{"error":"batch not yet completed, status: "}`+string(b.Status), http.StatusBadRequest)
		return
	}

	// Read the output file.
	outputPath := fmt.Sprintf("%s/output_%s.jsonl", h.Processor.WorkDir(), b.ID)
	data, err := readFile(outputPath)
	if err != nil {
		http.Error(w, `{"error":"output file not available"}`, http.StatusInternalServerError)
		return
	}

	// Parse JSONL lines into BatchResponse objects.
	responses := parseBatchResults(string(data))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Disposition", `attachment; filename="batch_results.jsonl"`)
	json.NewEncoder(w).Encode(responses)
}

// extractBatchID extracts the batch_id from a URL path like /v1/batches/{batch_id} or /v1/batches/{batch_id}/results.
func extractBatchID(path string) string {
	// Match /v1/batches/{batch_id} or /v1/batches/{batch_id}/results
	parts := splitPath(path)
	for i, p := range parts {
		if p == "batches" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	return ""
}

// splitPath splits a URL path into segments.
func splitPath(path string) []string {
	result := []string{}
	current := ""
	for _, ch := range path {
		if ch == '/' {
			if current != "" {
				result = append(result, current)
				current = ""
			}
		} else {
			current += string(ch)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

// sanitizeFilename removes path separators from a filename.
func sanitizeFilename(name string) string {
	safe := ""
	for _, ch := range name {
		if ch == '/' || ch == '\\' || ch == ':' {
			safe += "_"
		} else {
			safe += string(ch)
		}
	}
	return safe
}

// readFile is a helper that reads a file (wraps os.ReadFile for testability).
func readFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}

// parseBatchResults parses a JSONL string into a slice of BatchResponse.
func parseBatchResults(data string) []batch.BatchResponse {
	var responses []batch.BatchResponse
	for _, line := range splitLines(data) {
		line = trimSpace(line)
		if line == "" {
			continue
		}
		var resp batch.BatchResponse
		if err := json.Unmarshal([]byte(line), &resp); err == nil {
			responses = append(responses, resp)
		}
	}
	return responses
}

// splitLines splits a string into lines.
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
	for start < end && (s[start] == ' ' || s[start] == '	' || s[start] == '\r' || s[start] == '\n') {
		start++
	}
	for end > start && (s[end-1] == ' ' || s[end-1] == '	' || s[end-1] == '\r' || s[end-1] == '\n') {
		end--
	}
	return s[start:end]
}
