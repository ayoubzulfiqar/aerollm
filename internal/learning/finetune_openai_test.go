package learning

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

const testAPIKey = "sk-test-SECRET-0123456789abcdef"

type fakeUpload struct {
	Purpose     string
	Filename    string
	Content     []byte
	ContentType string
}

type fakeJob struct {
	ID           string
	Model        string
	TrainingFile string
	Status       string
	Created      int64
	// Statuses are returned by successive GETs; the last one sticks.
	Statuses []string
	gets     int
}

// fakeOpenAI is a minimal in-process OpenAI files + fine-tuning API.
type fakeOpenAI struct {
	t   *testing.T
	srv *httptest.Server

	mu           sync.Mutex
	requests     int
	uploads      []fakeUpload
	createBodies []map[string]any
	jobs         map[string]*fakeJob
	jobOrder     []string
	deleted      []string
	// defaultStatuses is assigned to new jobs.
	defaultStatuses []string
	// failGets makes the next N job GETs return 503.
	failGets int
	// createError forces job creation to fail with this status.
	createError int
	// finalError is reported for jobs that end in "failed".
	finalError bool
}

func newFakeOpenAI(t *testing.T) *fakeOpenAI {
	t.Helper()
	f := &fakeOpenAI{t: t, jobs: map[string]*fakeJob{}}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/files", f.handleUpload)
	mux.HandleFunc("DELETE /v1/files/{id}", f.handleDeleteFile)
	mux.HandleFunc("POST /v1/fine_tuning/jobs", f.handleCreate)
	mux.HandleFunc("GET /v1/fine_tuning/jobs", f.handleList)
	mux.HandleFunc("GET /v1/fine_tuning/jobs/{id}", f.handleGet)
	mux.HandleFunc("POST /v1/fine_tuning/jobs/{id}/cancel", f.handleCancel)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.requests++
		f.mu.Unlock()
		if got := r.Header.Get("Authorization"); got != "Bearer "+testAPIKey {
			// Mimic OpenAI, which echoes (part of) the presented key.
			presented := strings.TrimPrefix(got, "Bearer ")
			writeFakeError(w, http.StatusUnauthorized, "Incorrect API key provided: "+presented+".", "invalid_request_error", "invalid_api_key")
			return
		}
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeOpenAI) baseURL() string { return f.srv.URL + "/v1" }

func (f *fakeOpenAI) requestCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

func writeFakeError(w http.ResponseWriter, status int, msg, typ, code string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": msg, "type": typ, "code": code, "param": nil}})
}

func (f *fakeOpenAI) handleUpload(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		writeFakeError(w, http.StatusBadRequest, "bad multipart: "+err.Error(), "invalid_request_error", "")
		return
	}
	file, hdr, err := r.FormFile("file")
	if err != nil {
		writeFakeError(w, http.StatusBadRequest, "missing file", "invalid_request_error", "")
		return
	}
	defer file.Close()
	content, _ := io.ReadAll(file)
	f.mu.Lock()
	f.uploads = append(f.uploads, fakeUpload{
		Purpose:     r.FormValue("purpose"),
		Filename:    hdr.Filename,
		Content:     content,
		ContentType: r.Header.Get("Content-Type"),
	})
	id := fmt.Sprintf("file-%d", len(f.uploads))
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{
		"id": id, "object": "file", "bytes": len(content), "created_at": 1700000000,
		"filename": hdr.Filename, "purpose": r.FormValue("purpose"),
	})
}

func (f *fakeOpenAI) handleDeleteFile(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.deleted = append(f.deleted, r.PathValue("id"))
	f.mu.Unlock()
	_ = json.NewEncoder(w).Encode(map[string]any{"id": r.PathValue("id"), "object": "file", "deleted": true})
}

func (f *fakeOpenAI) jobJSON(j *fakeJob) map[string]any {
	out := map[string]any{
		"id": j.ID, "object": "fine_tuning.job", "model": j.Model, "status": j.Status,
		"training_file": j.TrainingFile, "created_at": j.Created,
		"fine_tuned_model": nil, "finished_at": nil, "trained_tokens": nil, "error": nil,
	}
	switch j.Status {
	case JobStatusSucceeded:
		out["fine_tuned_model"] = "ft:" + j.Model + ":org::" + j.ID
		out["finished_at"] = j.Created + 60
		out["trained_tokens"] = 1234
	case JobStatusFailed:
		out["finished_at"] = j.Created + 30
		if f.finalError {
			out["error"] = map[string]any{"code": "invalid_training_file", "message": "Training file has too few examples", "param": "training_file"}
		}
	}
	return out
}

func (f *fakeOpenAI) handleCreate(w http.ResponseWriter, r *http.Request) {
	var body map[string]any
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeFakeError(w, http.StatusBadRequest, "bad json", "invalid_request_error", "")
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createBodies = append(f.createBodies, body)
	if f.createError != 0 {
		writeFakeError(w, f.createError, "Model gpt-bogus is not available for fine-tuning", "invalid_request_error", "model_not_available")
		return
	}
	id := fmt.Sprintf("ftjob-%d", len(f.jobOrder)+1)
	model, _ := body["model"].(string)
	tf, _ := body["training_file"].(string)
	j := &fakeJob{ID: id, Model: model, TrainingFile: tf, Status: JobStatusValidatingFiles, Created: 1700000000 + int64(len(f.jobOrder)), Statuses: append([]string(nil), f.defaultStatuses...)}
	f.jobs[id] = j
	f.jobOrder = append(f.jobOrder, id)
	_ = json.NewEncoder(w).Encode(f.jobJSON(j))
}

func (f *fakeOpenAI) handleGet(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failGets > 0 {
		f.failGets--
		w.Header().Set("Retry-After", "0")
		writeFakeError(w, http.StatusServiceUnavailable, "temporarily unavailable", "server_error", "")
		return
	}
	j, ok := f.jobs[r.PathValue("id")]
	if !ok {
		writeFakeError(w, http.StatusNotFound, "No such fine-tuning job", "invalid_request_error", "fine_tune_not_found")
		return
	}
	if len(j.Statuses) > 0 {
		idx := min(j.gets, len(j.Statuses)-1)
		j.Status = j.Statuses[idx]
	}
	j.gets++
	_ = json.NewEncoder(w).Encode(f.jobJSON(j))
}

func (f *fakeOpenAI) handleCancel(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.jobs[r.PathValue("id")]
	if !ok {
		writeFakeError(w, http.StatusNotFound, "No such fine-tuning job", "invalid_request_error", "fine_tune_not_found")
		return
	}
	if IsTerminalJobStatus(j.Status) {
		writeFakeError(w, http.StatusBadRequest, "Job has already completed", "invalid_request_error", "")
		return
	}
	j.Status = JobStatusCancelled
	j.Statuses = nil
	_ = json.NewEncoder(w).Encode(f.jobJSON(j))
}

func (f *fakeOpenAI) handleList(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	after := r.URL.Query().Get("after")
	// Newest first, like the real API.
	var ids []string
	for i := len(f.jobOrder) - 1; i >= 0; i-- {
		ids = append(ids, f.jobOrder[i])
	}
	if after != "" {
		for i, id := range ids {
			if id == after {
				ids = ids[i+1:]
				break
			}
		}
	}
	hasMore := false
	if limit > 0 && len(ids) > limit {
		ids, hasMore = ids[:limit], true
	}
	data := make([]map[string]any, 0, len(ids))
	for _, id := range ids {
		data = append(data, f.jobJSON(f.jobs[id]))
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"object": "list", "data": data, "has_more": hasMore})
}

func newTestTuner(t *testing.T, f *fakeOpenAI, mutate func(*OpenAIFineTuneConfig)) *OpenAIFineTuner {
	t.Helper()
	cfg := OpenAIFineTuneConfig{
		BaseURL:         f.baseURL(),
		APIKey:          testAPIKey,
		Model:           "gpt-4o-mini-2024-07-18",
		PollInterval:    time.Millisecond,
		PollMaxInterval: 4 * time.Millisecond,
	}
	if mutate != nil {
		mutate(&cfg)
	}
	c, err := NewOpenAIFineTuner(cfg)
	if err != nil {
		t.Fatalf("NewOpenAIFineTuner: %v", err)
	}
	return c
}

const sampleChatJSONL = `{"messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"}]}
{"messages":[{"role":"user","content":"2+2?"},{"role":"assistant","content":"4"}]}
`

func TestOpenAIFineTunerSubmitAndWait(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusValidatingFiles, JobStatusQueued, JobStatusRunning, JobStatusRunning, JobStatusSucceeded}
	epochs := 3
	c := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) {
		cfg.Suffix = "aerollm"
		cfg.Hyperparameters = &FineTuneHyperparameters{NEpochs: &epochs}
	})
	if !c.Configured() {
		t.Fatal("expected configured client")
	}
	rec, err := c.Submit(context.Background(), "train.jsonl", []byte(sampleChatJSONL), "")
	if err != nil {
		t.Fatalf("submit: %v", err)
	}
	if rec.ID != "ftjob-1" || rec.TrainingFile != "file-1" || rec.Status != JobStatusValidatingFiles {
		t.Fatalf("unexpected record %+v", rec)
	}

	f.mu.Lock()
	if len(f.uploads) != 1 {
		t.Fatalf("expected 1 upload, got %d", len(f.uploads))
	}
	up := f.uploads[0]
	body := f.createBodies[0]
	f.mu.Unlock()
	if up.Purpose != "fine-tune" || up.Filename != "train.jsonl" || string(up.Content) != sampleChatJSONL {
		t.Fatalf("unexpected upload %+v", up)
	}
	if !strings.HasPrefix(up.ContentType, "multipart/form-data; boundary=") {
		t.Fatalf("unexpected content type %q", up.ContentType)
	}
	if body["training_file"] != "file-1" || body["model"] != "gpt-4o-mini-2024-07-18" || body["suffix"] != "aerollm" {
		t.Fatalf("unexpected create body %v", body)
	}
	hp, _ := body["hyperparameters"].(map[string]any)
	if hp["n_epochs"] != float64(3) || len(hp) != 1 {
		t.Fatalf("unexpected hyperparameters %v", body["hyperparameters"])
	}

	final, err := c.WaitForJob(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("wait: %v", err)
	}
	if final.Status != JobStatusSucceeded || !final.Terminal() {
		t.Fatalf("expected succeeded, got %+v", final)
	}
	if final.FineTunedModel != "ft:gpt-4o-mini-2024-07-18:org::ftjob-1" || final.TrainedTokens != 1234 || final.FinishedAt == nil {
		t.Fatalf("unexpected final record %+v", final)
	}
	if final.TrainingFile != "file-1" || final.CreatedAt.Unix() != 1700000000 {
		t.Fatalf("record lost fields: %+v", final)
	}
	local, ok := c.Job(rec.ID)
	if !ok || local.Status != JobStatusSucceeded {
		t.Fatalf("local record not updated: %+v %v", local, ok)
	}
	if jobs := c.Jobs(); len(jobs) != 1 || jobs[0].ID != rec.ID {
		t.Fatalf("unexpected jobs %+v", jobs)
	}
}

func TestOpenAIFineTunerFailedJob(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusRunning, JobStatusFailed}
	f.finalError = true
	c := newTestTuner(t, f, nil)
	rec, err := c.Submit(context.Background(), "", []byte(sampleChatJSONL), "custom-model")
	if err != nil {
		t.Fatal(err)
	}
	if rec.Model != "custom-model" {
		t.Fatalf("per-call model not used: %+v", rec)
	}
	final, err := c.WaitForJob(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("a failed job is a terminal state, not a poll error: %v", err)
	}
	if final.Status != JobStatusFailed || final.ErrorCode != "invalid_training_file" || !strings.Contains(final.ErrorMessage, "too few examples") {
		t.Fatalf("unexpected failed record %+v", final)
	}
	f.mu.Lock()
	name := f.uploads[0].Filename
	f.mu.Unlock()
	if name != "dataset.jsonl" {
		t.Fatalf("default filename not applied: %q", name)
	}
}

func TestOpenAIFineTunerCancel(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusRunning}
	c := newTestTuner(t, f, nil)
	rec, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.CancelJob(context.Background(), rec.ID)
	if err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got.Status != JobStatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
	if local, _ := c.Job(rec.ID); local.Status != JobStatusCancelled {
		t.Fatalf("local record not cancelled: %+v", local)
	}
	// Cancelling a finished job surfaces the provider error.
	_, err = c.CancelJob(context.Background(), rec.ID)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusBadRequest || !strings.Contains(apiErr.Message, "already completed") {
		t.Fatalf("expected 400 APIError, got %v", err)
	}
	if _, err := c.CancelJob(context.Background(), "ftjob-404"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestOpenAIFineTunerListAndGet(t *testing.T) {
	f := newFakeOpenAI(t)
	c := newTestTuner(t, f, nil)
	for i := 0; i < 3; i++ {
		if _, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), ""); err != nil {
			t.Fatal(err)
		}
	}
	page, more, err := c.ListJobs(context.Background(), 2, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 2 || !more || page[0].ID != "ftjob-3" || page[1].ID != "ftjob-2" {
		t.Fatalf("unexpected first page %+v more=%v", page, more)
	}
	page, more, err = c.ListJobs(context.Background(), 2, page[1].ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 1 || more || page[0].ID != "ftjob-1" {
		t.Fatalf("unexpected second page %+v more=%v", page, more)
	}
	if _, _, err := c.ListJobs(context.Background(), 10, "../../x"); !errors.Is(err, ErrInvalidJobID) {
		t.Fatalf("expected ErrInvalidJobID for bad cursor, got %v", err)
	}

	got, err := c.GetJob(context.Background(), "ftjob-2")
	if err != nil || got.ID != "ftjob-2" {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := c.GetJob(context.Background(), "ftjob-99"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
	before := f.requestCount()
	for _, bad := range []string{"", "../files", "a/b", "ftjob-1?x=1", strings.Repeat("a", 129), "id with space"} {
		if _, err := c.GetJob(context.Background(), bad); !errors.Is(err, ErrInvalidJobID) {
			t.Fatalf("%q: expected ErrInvalidJobID, got %v", bad, err)
		}
		if _, err := c.CancelJob(context.Background(), bad); !errors.Is(err, ErrInvalidJobID) {
			t.Fatalf("%q: expected ErrInvalidJobID, got %v", bad, err)
		}
	}
	if f.requestCount() != before {
		t.Fatal("invalid IDs must be rejected before any request")
	}
}

func TestOpenAIFineTunerWaitCancelledContext(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusRunning}
	c := newTestTuner(t, f, nil)
	rec, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	last, err := c.WaitForJob(ctx, rec.ID)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline exceeded, got %v", err)
	}
	if last.Status != JobStatusRunning {
		t.Fatalf("expected last known running record, got %+v", last)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("wait did not honour ctx")
	}

	// Already-cancelled context returns promptly.
	ctx2, cancel2 := context.WithCancel(context.Background())
	cancel2()
	if _, err := c.WaitForJob(ctx2, rec.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestOpenAIFineTunerTransientPollFailures(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusSucceeded}
	c := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) { cfg.MaxPollFailures = 3 })
	rec, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), "")
	if err != nil {
		t.Fatal(err)
	}
	f.mu.Lock()
	f.failGets = 2
	f.mu.Unlock()
	final, err := c.WaitForJob(context.Background(), rec.ID)
	if err != nil || final.Status != JobStatusSucceeded {
		t.Fatalf("expected recovery after transient failures: %+v %v", final, err)
	}

	strict := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) { cfg.MaxPollFailures = 1 })
	f.mu.Lock()
	f.failGets = 5
	f.mu.Unlock()
	_, err = strict.WaitForJob(context.Background(), rec.ID)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.StatusCode != http.StatusServiceUnavailable || !apiErr.Temporary() {
		t.Fatalf("expected 503 APIError after exhausting retries, got %v", err)
	}
}

func TestOpenAIFineTunerAPIErrorsRedactKey(t *testing.T) {
	f := newFakeOpenAI(t)
	// The fake echoes the presented key back like OpenAI does.
	c := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) { cfg.APIKey = "sk-wrong-KEY-should-not-leak-42" })
	_, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), "")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %v", err)
	}
	if apiErr.StatusCode != http.StatusUnauthorized || apiErr.Type != "invalid_request_error" || apiErr.Code != "invalid_api_key" {
		t.Fatalf("error not parsed: %+v", apiErr)
	}
	if strings.Contains(err.Error(), "sk-wrong-KEY-should-not-leak-42") || !strings.Contains(err.Error(), "[REDACTED]") {
		t.Fatalf("API key leaked or not redacted: %v", err)
	}

	// Transport failures do not leak the key either.
	f.srv.Close()
	c2 := newTestTuner(t, f, nil)
	_, err = c2.GetJob(context.Background(), "ftjob-1")
	if err == nil || strings.Contains(err.Error(), testAPIKey) {
		t.Fatalf("expected transport error without key, got %v", err)
	}
	if !isTransient(err) {
		t.Fatalf("transport errors should be transient: %v", err)
	}
}

func TestOpenAIFineTunerCreateFailureDeletesUpload(t *testing.T) {
	f := newFakeOpenAI(t)
	f.createError = http.StatusBadRequest
	c := newTestTuner(t, f, nil)
	rec, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), "gpt-bogus")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "model_not_available" || rec.ID != "" {
		t.Fatalf("expected create APIError, got %+v %v", rec, err)
	}
	f.mu.Lock()
	deleted := append([]string(nil), f.deleted...)
	f.mu.Unlock()
	if len(deleted) != 1 || deleted[0] != "file-1" {
		t.Fatalf("orphaned upload not deleted: %v", deleted)
	}
	if len(c.Jobs()) != 0 {
		t.Fatal("failed creation must not be tracked")
	}
}

func TestOpenAIFineTunerErrorBodies(t *testing.T) {
	cases := []struct {
		name, body string
		status     int
		wantMsg    string
		wantCode   string
	}{
		{"string error", `{"error":"model overloaded"}`, 503, "model overloaded", ""},
		{"numeric code", `{"error":{"message":"rate limited","type":"requests","code":429}}`, 429, "rate limited", "429"},
		{"plain text", `upstream exploded`, 502, "upstream exploded", ""},
		{"html", `<html>bad gateway</html>`, 502, "Bad Gateway", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			c, err := NewOpenAIFineTuner(OpenAIFineTuneConfig{BaseURL: srv.URL, APIKey: testAPIKey, Model: "m"})
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.GetJob(context.Background(), "ftjob-1")
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("expected APIError, got %v", err)
			}
			if apiErr.StatusCode != tc.status || apiErr.Message != tc.wantMsg || apiErr.Code != tc.wantCode {
				t.Fatalf("unexpected APIError %+v", apiErr)
			}
		})
	}
}

func TestOpenAIFineTunerNotConfigured(t *testing.T) {
	f := newFakeOpenAI(t)
	for name, cfg := range map[string]OpenAIFineTuneConfig{
		"no key":   {BaseURL: f.baseURL(), Model: "m"},
		"no model": {BaseURL: f.baseURL(), APIKey: testAPIKey},
	} {
		t.Run(name, func(t *testing.T) {
			c, err := NewOpenAIFineTuner(cfg)
			if err != nil {
				t.Fatalf("missing credentials must not be a construction error: %v", err)
			}
			if c.Configured() {
				t.Fatal("expected not configured")
			}
			ctx := context.Background()
			checks := map[string]error{}
			_, checks["submit"] = c.Submit(ctx, "a.jsonl", []byte(sampleChatJSONL), "m")
			_, checks["upload"] = c.UploadTrainingFile(ctx, "a.jsonl", strings.NewReader(sampleChatJSONL))
			_, checks["create"] = c.CreateJob(ctx, "file-1", "m")
			_, checks["get"] = c.GetJob(ctx, "ftjob-1")
			_, checks["cancel"] = c.CancelJob(ctx, "ftjob-1")
			_, checks["wait"] = c.WaitForJob(ctx, "ftjob-1")
			_, _, checks["list"] = c.ListJobs(ctx, 1, "")
			for op, err := range checks {
				if !errors.Is(err, ErrFineTuneNotConfigured) {
					t.Fatalf("%s: expected ErrFineTuneNotConfigured, got %v", op, err)
				}
			}
			if f.requestCount() != 0 {
				t.Fatal("unconfigured client must not contact the API")
			}
		})
	}
	var nilClient *OpenAIFineTuner
	if nilClient.Configured() {
		t.Fatal("nil client is not configured")
	}
	if _, err := nilClient.GetJob(context.Background(), "ftjob-1"); !errors.Is(err, ErrFineTuneNotConfigured) {
		t.Fatalf("nil client: %v", err)
	}
}

func TestOpenAIFineTunerConfigValidation(t *testing.T) {
	bad := []OpenAIFineTuneConfig{
		{BaseURL: "ftp://example.com/v1"},
		{BaseURL: "https://user:pass@example.com/v1"},
		{BaseURL: "https://example.com/v1?x=1"},
		{BaseURL: "https:///v1"},
		{BaseURL: "://bad"},
		{Model: "bad model"},
		{Suffix: "has space"},
		{Suffix: strings.Repeat("a", 65)},
		{Organization: "org\r\nX-Evil: 1"},
		{APIKey: "sk-abc\r\nX-Evil: 1"},
		{Hyperparameters: &FineTuneHyperparameters{NEpochs: new(0)}},
		{Hyperparameters: &FineTuneHyperparameters{BatchSize: new(-1)}},
		{Hyperparameters: &FineTuneHyperparameters{LearningRateMultiplier: new(-0.5)}},
	}
	for i, cfg := range bad {
		if _, err := NewOpenAIFineTuner(cfg); !errors.Is(err, ErrInvalidFineTuneConfig) {
			t.Fatalf("case %d: expected ErrInvalidFineTuneConfig, got %v", i, err)
		}
	}
	c, err := NewOpenAIFineTuner(OpenAIFineTuneConfig{APIKey: testAPIKey, Model: "m"})
	if err != nil {
		t.Fatal(err)
	}
	if c.baseURL != DefaultOpenAIBaseURL {
		t.Fatalf("default base URL not applied: %q", c.baseURL)
	}
	c, err = NewOpenAIFineTuner(OpenAIFineTuneConfig{BaseURL: "http://localhost:8080/v1/", APIKey: testAPIKey, Model: "m"})
	if err != nil || c.baseURL != "http://localhost:8080/v1" {
		t.Fatalf("trailing slash not trimmed: %q %v", c.baseURL, err)
	}
	cfg := OpenAIFineTuneConfig{APIKey: testAPIKey, Model: "m"}
	for _, s := range []string{fmt.Sprint(cfg), fmt.Sprintf("%+v", cfg), fmt.Sprintf("%#v", cfg)} {
		if strings.Contains(s, testAPIKey) {
			t.Fatalf("config formatting leaks key: %s", s)
		}
	}
}

func TestOpenAIFineTunerUploadValidation(t *testing.T) {
	f := newFakeOpenAI(t)
	c := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) { cfg.MaxUploadBytes = 256 })
	ctx := context.Background()
	if _, err := c.UploadTrainingFile(ctx, "a.jsonl", strings.NewReader(strings.Repeat("{}\n", 200))); !errors.Is(err, ErrDatasetTooLarge) {
		t.Fatalf("expected ErrDatasetTooLarge, got %v", err)
	}
	if _, err := c.UploadTrainingFile(ctx, "../a.jsonl", strings.NewReader(sampleChatJSONL)); !errors.Is(err, ErrInvalidFilename) {
		t.Fatalf("expected ErrInvalidFilename, got %v", err)
	}
	if _, err := c.UploadTrainingFile(ctx, "a.jsonl", strings.NewReader("not json\n")); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("expected ErrInvalidDataset, got %v", err)
	}
	if _, err := c.UploadTrainingFile(ctx, "a.jsonl", nil); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("expected ErrInvalidDataset for nil reader, got %v", err)
	}
	if f.requestCount() != 0 {
		t.Fatal("invalid uploads must not reach the API")
	}
	file, err := c.UploadTrainingFile(ctx, "ok.jsonl", strings.NewReader(`{"messages":[]}`))
	if err != nil || file.ID != "file-1" || file.Purpose != "fine-tune" {
		t.Fatalf("upload: %+v %v", file, err)
	}
	if _, err := c.CreateJob(ctx, "file/../x", ""); !errors.Is(err, ErrInvalidJobID) {
		t.Fatalf("expected ErrInvalidJobID for bad file id, got %v", err)
	}
}

func TestOpenAIFineTunerPersistence(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusRunning, JobStatusSucceeded}
	store := persist.NewMemory()
	c := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) { cfg.Store = store })
	rec, err := c.Submit(context.Background(), "a.jsonl", []byte(sampleChatJSONL), "")
	if err != nil {
		t.Fatal(err)
	}
	var stored FineTuneJobRecord
	if ok, err := store.Get(FineTuneJobsBucket, rec.ID, &stored); !ok || err != nil || stored.Status != JobStatusValidatingFiles {
		t.Fatalf("job not written through on create: %+v %v %v", stored, ok, err)
	}
	if _, err := c.WaitForJob(context.Background(), rec.ID); err != nil {
		t.Fatal(err)
	}

	// A restart restores the terminal state without any API call.
	before := f.requestCount()
	restarted := newTestTuner(t, f, func(cfg *OpenAIFineTuneConfig) { cfg.Store = store })
	got, ok := restarted.Job(rec.ID)
	if !ok || got.Status != JobStatusSucceeded || got.FineTunedModel == "" || got.TrainingFile != "file-1" {
		t.Fatalf("record not restored: %+v %v", got, ok)
	}
	if f.requestCount() != before {
		t.Fatal("restore must not call the API")
	}

	// EnablePersistence on a running client writes its records through and
	// merges stored ones; corrupt documents are skipped and reported.
	other := persist.NewMemory()
	_ = other.Put(FineTuneJobsBucket, "ftjob-old", FineTuneJobRecord{ID: "ftjob-old", Status: JobStatusFailed, CreatedAt: time.Unix(1, 0).UTC()})
	_ = other.Put(FineTuneJobsBucket, "mismatch", FineTuneJobRecord{ID: "ftjob-x", Status: JobStatusFailed})
	_ = other.Put(FineTuneJobsBucket, "corrupt", "not an object")
	err = restarted.EnablePersistence(other)
	if err == nil || !strings.Contains(err.Error(), "corrupt") {
		t.Fatalf("expected corrupt-document report, got %v", err)
	}
	jobs := restarted.Jobs()
	if len(jobs) != 2 || jobs[0].ID != "ftjob-old" || jobs[1].ID != rec.ID {
		t.Fatalf("unexpected merged jobs %+v", jobs)
	}
	if ok, _ := other.Get(FineTuneJobsBucket, rec.ID, &stored); !ok {
		t.Fatal("in-memory record not written through to new store")
	}

	// The constructor stays usable when loading reports an error.
	c3, err := NewOpenAIFineTuner(OpenAIFineTuneConfig{BaseURL: f.baseURL(), APIKey: testAPIKey, Model: "m", Store: other})
	if err == nil || c3 == nil || len(c3.Jobs()) != 2 {
		t.Fatalf("expected usable client plus load error, got %v %v", c3, err)
	}
	if err := c3.EnablePersistence(nil); !errors.Is(err, ErrInvalidFineTuneConfig) {
		t.Fatalf("nil store: %v", err)
	}
}

func TestValidateFineTuneJSONL(t *testing.T) {
	if n, err := ValidateFineTuneJSONL([]byte(sampleChatJSONL + "\n\n")); err != nil || n != 2 {
		t.Fatalf("valid dataset: %d %v", n, err)
	}
	for _, bad := range []string{"", "\n  \n", "[1,2]\n", `{"a":1}` + "\n{broken\n", `"str"`} {
		if _, err := ValidateFineTuneJSONL([]byte(bad)); !errors.Is(err, ErrInvalidDataset) {
			t.Fatalf("%q: expected ErrInvalidDataset, got %v", bad, err)
		}
	}
}

func TestFlywheelToChatJSONL(t *testing.T) {
	line := func(req, resp string) string {
		b, _ := json.Marshal(map[string]string{"request": req, "response": resp, "rating": "up"})
		return string(b)
	}
	input := strings.Join([]string{
		line(`{"prompt":"hello"}`, `{"id":"x","text":"world"}`),
		line(`{"model":"m","messages":[{"role":"system","content":"be nice"},{"role":"user","content":"hi"}]}`,
			`{"choices":[{"message":{"role":"assistant","content":"hey"}}]}`),
		line(`plain question`, `plain answer`),
		line(`{"input":"q"}`, `{"content":[{"type":"text","text":"a1"},{"type":"text","text":"a2"}]}`),
		line(`{"prompt":"x"}`, "data: {\"chunk\":1}\n\ndata: [DONE]"),
		line(`{"unknown":"shape"}`, `{"text":"t"}`),
		line(`{"prompt":"x"}`, `{"unknown":"shape"}`),
		`{"messages":[{"role":"user","content":"pass"},{"role":"assistant","content":"through"}]}`,
		`{"request":"","response":"r"}`,
	}, "\n")
	out, n, err := FlywheelToChatJSONL([]byte(input))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("expected 5 examples, got %d:\n%s", n, out)
	}
	if _, err := ValidateFineTuneJSONL(out); err != nil {
		t.Fatalf("converted output invalid: %v", err)
	}
	type msg struct{ Role, Content string }
	var examples [][]msg
	for _, l := range bytes.Split(bytes.TrimSpace(out), []byte("\n")) {
		var ex struct{ Messages []msg }
		if err := json.Unmarshal(l, &ex); err != nil {
			t.Fatal(err)
		}
		examples = append(examples, ex.Messages)
	}
	want := [][]msg{
		{{"user", "hello"}, {"assistant", "world"}},
		{{"system", "be nice"}, {"user", "hi"}, {"assistant", "hey"}},
		{{"user", "plain question"}, {"assistant", "plain answer"}},
		{{"user", "q"}, {"assistant", "a1a2"}},
		{{"user", "pass"}, {"assistant", "through"}},
	}
	if fmt.Sprint(examples) != fmt.Sprint(want) {
		t.Fatalf("unexpected conversion:\n got %v\nwant %v", examples, want)
	}
	if _, _, err := FlywheelToChatJSONL([]byte("not json")); !errors.Is(err, ErrInvalidDataset) {
		t.Fatalf("expected ErrInvalidDataset, got %v", err)
	}
}

func TestTrainerWithOpenAIBackend(t *testing.T) {
	f := newFakeOpenAI(t)
	f.defaultStatuses = []string{JobStatusQueued, JobStatusRunning, JobStatusSucceeded}
	exporter, store := newRatedExporter(t)
	trainer := NewTrainer(exporter, store, t.TempDir())

	// Without a backend, jobs are refused rather than queued forever.
	if _, err := trainer.Enqueue(context.Background(), "m", "up"); !errors.Is(err, ErrFineTuneNotConfigured) {
		t.Fatalf("expected ErrFineTuneNotConfigured, got %v", err)
	}
	// An unconfigured client is equally honest.
	unconfigured, err := NewOpenAIFineTuner(OpenAIFineTuneConfig{BaseURL: f.baseURL()})
	if err != nil {
		t.Fatal(err)
	}
	trainer.SetFineTuneBackend(unconfigured)
	if _, err := trainer.Enqueue(context.Background(), "m", "up"); !errors.Is(err, ErrFineTuneNotConfigured) {
		t.Fatalf("expected ErrFineTuneNotConfigured, got %v", err)
	}
	if len(trainer.Jobs()) != 0 {
		t.Fatal("no job should be recorded")
	}

	tuner := newTestTuner(t, f, nil)
	trainer.SetFineTuneBackend(tuner)
	job, err := trainer.Enqueue(context.Background(), "", "up")
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if job.ID != "ftjob-1" || job.TrainingFile != "file-1" || job.Model != "gpt-4o-mini-2024-07-18" || job.Dataset != "" {
		t.Fatalf("unexpected job %+v", job)
	}
	f.mu.Lock()
	up := f.uploads[0]
	f.mu.Unlock()
	if !strings.HasPrefix(up.Filename, "ft-dataset-") || !strings.HasSuffix(up.Filename, ".jsonl") {
		t.Fatalf("unexpected upload filename %q", up.Filename)
	}
	wantLine := `{"messages":[{"role":"user","content":"hello"},{"role":"assistant","content":"world"}]}` + "\n"
	if string(up.Content) != wantLine {
		t.Fatalf("uploaded dataset not in chat format: %q", up.Content)
	}

	if got, ok := trainer.Status(job.ID); !ok || got.ID != job.ID {
		t.Fatalf("status: %+v %v", got, ok)
	}
	if jobs := trainer.Jobs(); len(jobs) != 1 || jobs[0].ID != job.ID {
		t.Fatalf("jobs: %+v", jobs)
	}
	refreshed, err := trainer.Refresh(context.Background(), job.ID)
	if err != nil || refreshed.Status != JobStatusQueued {
		t.Fatalf("refresh: %+v %v", refreshed, err)
	}
	done, err := trainer.Wait(context.Background(), job.ID)
	if err != nil || done.Status != JobStatusSucceeded || done.FineTunedModel == "" {
		t.Fatalf("wait: %+v %v", done, err)
	}

	// Remote cancel goes through the backend.
	f.mu.Lock()
	f.defaultStatuses = []string{JobStatusRunning}
	f.mu.Unlock()
	job2, err := trainer.Enqueue(context.Background(), "", "up")
	if err != nil {
		t.Fatal(err)
	}
	if err := trainer.Cancel(job2.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if got, _ := trainer.Status(job2.ID); got.Status != JobStatusCancelled {
		t.Fatalf("expected cancelled, got %+v", got)
	}
	if err := trainer.Cancel("ftjob-404"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}

	// Manual-queue jobs cannot be waited on.
	manual := NewTrainer(exporter, store, t.TempDir())
	manual.EnableManualQueue()
	mj, err := manual.Enqueue(context.Background(), "m", "up")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manual.Wait(context.Background(), mj.ID); !errors.Is(err, ErrFineTuneNotConfigured) {
		t.Fatalf("expected ErrFineTuneNotConfigured, got %v", err)
	}
	if _, err := manual.Refresh(context.Background(), "nope"); !errors.Is(err, ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}
