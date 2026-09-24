package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/batch"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

// maxBatchUpload caps uploaded JSONL input.
const maxBatchUpload = 100 << 20

// BatchHandler handles the OpenAI-compatible Batch API endpoints. Batches
// are owned by the key that created them; admin keys can see every batch.
type BatchHandler struct {
	Processor *batch.BatchProcessor
	Store     batch.BatchStore
}

// NewBatchHandler creates a new BatchHandler.
func NewBatchHandler(proc *batch.BatchProcessor, store batch.BatchStore) *BatchHandler {
	return &BatchHandler{Processor: proc, Store: store}
}

// BatchObject is the OpenAI-compatible response shape for batch endpoints.
type BatchObject struct {
	ID               string              `json:"id"`
	Object           string              `json:"object"`
	Endpoint         string              `json:"endpoint"`
	Status           string              `json:"status"`
	InputFileID      string              `json:"input_file_id"`
	OutputFileID     string              `json:"output_file_id,omitempty"`
	ErrorFileID      string              `json:"error_file_id,omitempty"`
	CompletionWindow string              `json:"completion_window,omitempty"`
	CreatedAt        int64               `json:"created_at"`
	InProgressAt     *int64              `json:"in_progress_at,omitempty"`
	FinalizingAt     *int64              `json:"finalizing_at,omitempty"`
	CompletedAt      *int64              `json:"completed_at,omitempty"`
	FailedAt         *int64              `json:"failed_at,omitempty"`
	ExpiresAt        *int64              `json:"expires_at,omitempty"`
	ExpiredAt        *int64              `json:"expired_at,omitempty"`
	CancellingAt     *int64              `json:"cancelling_at,omitempty"`
	CancelledAt      *int64              `json:"cancelled_at,omitempty"`
	Errors           []batch.BatchError  `json:"errors,omitempty"`
	Error            string              `json:"error,omitempty"`
	RequestCounts    batch.RequestCounts `json:"request_counts"`
	Metadata         map[string]string   `json:"metadata,omitempty"`
	// Flat counters kept for backward compatibility.
	TotalRequests     int `json:"total_requests"`
	CompletedRequests int `json:"completed_requests"`
	FailedRequests    int `json:"failed_requests"`
}

func unixPtr(t *time.Time) *int64 {
	if t == nil || t.IsZero() {
		return nil
	}
	v := t.Unix()
	return &v
}

func toBatchObject(b *batch.Batch) BatchObject {
	endpoint := b.Endpoint
	if endpoint == "" {
		endpoint = "/v1/chat/completions"
	}
	return BatchObject{
		ID: b.ID, Object: "batch", Endpoint: endpoint, Status: string(b.Status),
		InputFileID: b.InputFileID, OutputFileID: b.OutputFileID, ErrorFileID: b.ErrorFileID,
		CompletionWindow: b.CompletionWindow, CreatedAt: b.CreatedAt.Unix(),
		InProgressAt: unixPtr(b.InProgressAt), FinalizingAt: unixPtr(b.FinalizingAt),
		CompletedAt: unixPtr(b.CompletedAt), FailedAt: unixPtr(b.FailedAt),
		ExpiresAt: unixPtr(b.ExpiresAt), ExpiredAt: unixPtr(b.ExpiredAt),
		CancellingAt: unixPtr(b.CancellingAt), CancelledAt: unixPtr(b.CancelledAt),
		Errors: b.Errors, Error: b.Error, RequestCounts: b.RequestCounts, Metadata: b.Metadata,
		TotalRequests: b.TotalRequests, CompletedRequests: b.CompletedRequests, FailedRequests: b.FailedRequests,
	}
}

// owner returns the caller's owner id and whether it may see all batches.
func batchOwner(r *http.Request) (owner string, all bool) {
	if p, ok := middleware.PrincipalFromContext(r.Context()); ok {
		return p.KeyID, p.Admin
	}
	if key := middleware.APIKeyFromRequest(r); key != "" {
		return middleware.KeyID(key), false
	}
	return "", false
}

func writeBatchError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, batch.ErrBatchNotFound):
		writeError(w, http.StatusNotFound, "batch not found")
	case errors.Is(err, batch.ErrNotCancellable):
		writeError(w, http.StatusConflict, "batch is not in a cancellable state")
	case errors.Is(err, batch.ErrResultsNotReady):
		writeError(w, http.StatusConflict, "batch results are not available yet")
	case errors.Is(err, batch.ErrInvalidInput):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, batch.ErrProcessorClosed):
		writeError(w, http.StatusServiceUnavailable, "batch processor is shutting down")
	default:
		writeError(w, http.StatusBadRequest, err.Error())
	}
}

// ServeHTTP routes the Batch API:
//
//	POST /v1/batches                 create (multipart "file" or JSON {"input": "<jsonl>"})
//	GET  /v1/batches                 list (?after=&limit=)
//	GET  /v1/batches/{id}            retrieve
//	POST /v1/batches/{id}/cancel     cancel
//	GET  /v1/batches/{id}/results    output JSONL
//	GET  /v1/batches/{id}/errors     error JSONL
func (h *BatchHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rest := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/batches"), "/")
	if rest == "" {
		switch r.Method {
		case http.MethodPost:
			h.CreateBatch(w, r)
		case http.MethodGet:
			h.ListBatches(w, r)
		default:
			methodNotAllowed(w, http.MethodGet, http.MethodPost)
		}
		return
	}
	parts := strings.Split(rest, "/")
	switch {
	case len(parts) == 1:
		h.GetBatch(w, r)
	case len(parts) == 2 && parts[1] == "cancel":
		h.CancelBatch(w, r)
	case len(parts) == 2 && (parts[1] == "results" || parts[1] == "output"):
		h.GetBatchResults(w, r)
	case len(parts) == 2 && parts[1] == "errors":
		h.getBatchFile(w, r, true)
	default:
		writeError(w, http.StatusNotFound, "unknown batch endpoint")
	}
}

// CreateBatch handles POST /v1/batches.
// @Summary Create batch
// @Description Create a batch job from JSONL input (multipart field "file", or JSON {"input": "<jsonl>", "metadata": {}, "completion_window": "24h"}). Each line is {"custom_id","method":"POST","url":"/v1/chat/completions","body":{...}}.
// @Tags batches
// @Accept multipart/form-data
// @Produce json
// @Param file formData file true "JSONL file containing batch requests"
// @Success 200 {object} BatchObject
// @Failure 400 {object} map[string]string
// @Router /v1/batches [post]
func (h *BatchHandler) CreateBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxBatchUpload)
	opts := batch.CreateOptions{}
	opts.Owner, _ = batchOwner(r)

	if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		if err := r.ParseMultipartForm(32 << 20); err != nil {
			writeError(w, http.StatusBadRequest, "failed to parse multipart form")
			return
		}
		defer func() {
			if r.MultipartForm != nil {
				_ = r.MultipartForm.RemoveAll()
			}
		}()
		file, header, err := r.FormFile("file")
		if err != nil {
			writeError(w, http.StatusBadRequest, "no file uploaded (multipart field \"file\")")
			return
		}
		defer file.Close()
		data, err := io.ReadAll(file)
		if err != nil {
			writeError(w, http.StatusRequestEntityTooLarge, "failed to read uploaded file")
			return
		}
		opts.Data = data
		opts.InputFileID = "file-" + sanitizeFilename(header.Filename)
		if cw := r.FormValue("completion_window"); cw != "" {
			d, err := time.ParseDuration(cw)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid completion_window")
				return
			}
			opts.CompletionWindow = d
		}
	} else {
		var body struct {
			Input            string            `json:"input"`
			InputFileID      string            `json:"input_file_id"`
			Metadata         map[string]string `json:"metadata"`
			CompletionWindow string            `json:"completion_window"`
		}
		if !decodeJSON(w, r, &body) {
			return
		}
		if strings.TrimSpace(body.Input) == "" {
			writeError(w, http.StatusBadRequest, "input (JSONL) is required; file uploads use multipart field \"file\"")
			return
		}
		opts.Data = []byte(body.Input)
		opts.InputFileID = body.InputFileID
		opts.Metadata = body.Metadata
		if body.CompletionWindow != "" {
			d, err := time.ParseDuration(body.CompletionWindow)
			if err != nil {
				writeError(w, http.StatusBadRequest, "invalid completion_window")
				return
			}
			opts.CompletionWindow = d
		}
	}

	b, err := h.Processor.CreateBatchWithOptions(r.Context(), opts)
	if err != nil {
		writeBatchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchObject(b))
}

func (h *BatchHandler) load(w http.ResponseWriter, r *http.Request) (*batch.Batch, bool) {
	id := extractBatchID(r.URL.Path)
	if id == "" {
		writeError(w, http.StatusBadRequest, "batch_id is required")
		return nil, false
	}
	owner, all := batchOwner(r)
	var (
		b   *batch.Batch
		err error
	)
	if all {
		b, err = h.Processor.GetBatch(r.Context(), id)
	} else {
		b, err = h.Processor.GetBatchForOwner(r.Context(), id, owner)
	}
	if err != nil {
		writeBatchError(w, err)
		return nil, false
	}
	return b, true
}

// GetBatch handles GET /v1/batches/{batch_id}.
// @Summary Get batch status
// @Tags batches
// @Produce json
// @Param batch_id path string true "Batch ID"
// @Success 200 {object} BatchObject
// @Failure 404 {object} map[string]string
// @Router /v1/batches/{batch_id} [get]
func (h *BatchHandler) GetBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	if b, ok := h.load(w, r); ok {
		writeJSON(w, http.StatusOK, toBatchObject(b))
	}
}

// ListBatches handles GET /v1/batches.
// @Summary List batches
// @Tags batches
// @Produce json
// @Param after query string false "Cursor: last batch ID of the previous page"
// @Param limit query int false "Page size (1-100, default 20)"
// @Router /v1/batches [get]
func (h *BatchHandler) ListBatches(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	owner, all := batchOwner(r)
	opts := batch.ListOptions{After: r.URL.Query().Get("after")}
	if !all {
		opts.Owner = owner
	}
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "limit must be between 1 and 100")
			return
		}
		opts.Limit = n
	}
	res, err := h.Processor.ListBatches(r.Context(), opts)
	if err != nil {
		writeBatchError(w, err)
		return
	}
	data := make([]BatchObject, 0, len(res.Data))
	for _, b := range res.Data {
		data = append(data, toBatchObject(b))
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"object": "list", "data": data, "first_id": res.FirstID, "last_id": res.LastID, "has_more": res.HasMore,
	})
}

// CancelBatch handles POST /v1/batches/{batch_id}/cancel.
// @Summary Cancel batch
// @Tags batches
// @Produce json
// @Param batch_id path string true "Batch ID"
// @Success 200 {object} BatchObject
// @Router /v1/batches/{batch_id}/cancel [post]
func (h *BatchHandler) CancelBatch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		methodNotAllowed(w, http.MethodPost)
		return
	}
	b, ok := h.load(w, r)
	if !ok {
		return
	}
	cancelled, err := h.Processor.CancelBatch(r.Context(), b.ID)
	if err != nil {
		writeBatchError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toBatchObject(cancelled))
}

// GetBatchResults handles GET /v1/batches/{batch_id}/results.
// @Summary Get batch results
// @Description Returns the output JSONL (available for completed, cancelled and expired batches).
// @Tags batches
// @Produce application/jsonl
// @Param batch_id path string true "Batch ID"
// @Router /v1/batches/{batch_id}/results [get]
func (h *BatchHandler) GetBatchResults(w http.ResponseWriter, r *http.Request) {
	h.getBatchFile(w, r, false)
}

func (h *BatchHandler) getBatchFile(w http.ResponseWriter, r *http.Request, errorsFile bool) {
	if r.Method != http.MethodGet {
		methodNotAllowed(w, http.MethodGet)
		return
	}
	b, ok := h.load(w, r)
	if !ok {
		return
	}
	var (
		rc  io.ReadCloser
		err error
	)
	if errorsFile {
		rc, err = h.Processor.OpenErrors(r.Context(), b.ID)
	} else {
		rc, err = h.Processor.OpenResults(r.Context(), b.ID)
	}
	if err != nil {
		writeBatchError(w, err)
		return
	}
	defer rc.Close()
	kind := "output"
	if errorsFile {
		kind = "errors"
	}
	w.Header().Set("Content-Type", "application/jsonl")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s_%s.jsonl"`, b.ID, kind))
	w.WriteHeader(http.StatusOK)
	_, _ = io.Copy(w, rc)
}

// extractBatchID extracts the batch_id from /v1/batches/{batch_id}[/...].
func extractBatchID(path string) string {
	rest := strings.Trim(strings.TrimPrefix(path, "/v1/batches"), "/")
	if rest == "" {
		return ""
	}
	return strings.SplitN(rest, "/", 2)[0]
}

// sanitizeFilename keeps a conservative character set for file IDs.
func sanitizeFilename(name string) string {
	var b strings.Builder
	for _, ch := range name {
		switch {
		case ch >= 'a' && ch <= 'z', ch >= 'A' && ch <= 'Z', ch >= '0' && ch <= '9', ch == '.', ch == '-', ch == '_':
			b.WriteRune(ch)
		default:
			b.WriteRune('_')
		}
		if b.Len() >= 64 {
			break
		}
	}
	if b.Len() == 0 {
		return "upload"
	}
	return b.String()
}
