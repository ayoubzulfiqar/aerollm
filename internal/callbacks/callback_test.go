package callbacks

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/webhooks"
)

// mockCallback is a test implementation of CallbackHandler.
type mockCallback struct {
	name         string
	successCount atomic.Int32
	errorCount   atomic.Int32
	block        chan struct{}
	mu           sync.Mutex
	lastReq      *CallbackRequestData
	lastResp     *CallbackResponseData
	mutate       bool
	panics       bool
}

func newMockCallback(name string) *mockCallback { return &mockCallback{name: name} }

func (m *mockCallback) OnSuccess(_ context.Context, req *CallbackRequestData, resp *CallbackResponseData) error {
	if m.block != nil {
		<-m.block
	}
	if m.panics {
		panic("boom")
	}
	if m.mutate && req.Metadata != nil {
		req.Metadata["mutated"] = true
	}
	m.mu.Lock()
	m.lastReq, m.lastResp = req, resp
	m.mu.Unlock()
	m.successCount.Add(1)
	return nil
}

func (m *mockCallback) OnError(_ context.Context, req *CallbackRequestData, _ error) error {
	m.mu.Lock()
	m.lastReq = req
	m.mu.Unlock()
	m.errorCount.Add(1)
	return nil
}

func (m *mockCallback) Name() string { return m.name }

func TestCallbackManagerRegister(t *testing.T) {
	mgr := NewCallbackManager(5 * time.Second)
	defer mgr.Close()
	mock := newMockCallback("test_handler")
	mgr.Register(mock)

	req := &CallbackRequestData{RequestID: "req_123", Model: "gpt-4o", Provider: "openai", Timestamp: time.Now()}
	resp := &CallbackResponseData{ResponseID: "resp_123", LatencyMs: 150, TokenCount: map[string]int{"input": 10, "output": 20}}

	mgr.FireSuccess(req, resp)
	mgr.Wait()

	if mock.successCount.Load() != 1 {
		t.Errorf("expected 1 success call, got %d", mock.successCount.Load())
	}
	if st := mgr.Stats(); st.Succeeded != 1 || st.Pending != 0 {
		t.Errorf("unexpected stats %+v", st)
	}
}

func TestCallbackManagerError(t *testing.T) {
	mgr := NewCallbackManager(5 * time.Second)
	defer mgr.Close()
	mock := newMockCallback("error_handler")
	mgr.Register(mock)

	mgr.FireError(&CallbackRequestData{RequestID: "req_456", Model: "claude-3"}, errors.New("rate limit exceeded"))
	mgr.FireError(nil, nil) // must not panic
	mgr.Wait()

	if mock.errorCount.Load() != 2 {
		t.Errorf("expected 2 error calls, got %d", mock.errorCount.Load())
	}
	if mock.successCount.Load() != 0 {
		t.Errorf("expected 0 success calls, got %d", mock.successCount.Load())
	}
}

func TestNoOpCallback(t *testing.T) {
	noop := &NoOpCallback{}
	if noop.Name() != "noop" {
		t.Errorf("expected name 'noop', got %s", noop.Name())
	}
	req := &CallbackRequestData{RequestID: "test"}
	resp := &CallbackResponseData{ResponseID: "test"}
	if err := noop.OnSuccess(context.Background(), req, resp); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := noop.OnError(context.Background(), req, errors.New("test")); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

func TestCallbackManagerMultipleHandlers(t *testing.T) {
	mgr := NewCallbackManager(5 * time.Second)
	defer mgr.Close()
	mock1 := newMockCallback("handler_1")
	mock2 := newMockCallback("handler_2")
	mgr.Register(mock1)
	mgr.Register(mock2)

	mgr.FireSuccess(&CallbackRequestData{RequestID: "req_multi", Model: "gpt-4o"}, &CallbackResponseData{ResponseID: "resp_multi", LatencyMs: 100})
	mgr.Wait()

	if mock1.successCount.Load() != 1 || mock2.successCount.Load() != 1 {
		t.Errorf("expected both handlers to fire once: %d %d", mock1.successCount.Load(), mock2.successCount.Load())
	}
}

func TestCallbackManagerBoundedQueueDrops(t *testing.T) {
	mgr := NewCallbackManagerWithOptions(time.Second, ManagerOptions{Workers: 1, QueueSize: 2})
	mock := newMockCallback("slow")
	mock.block = make(chan struct{})
	mgr.Register(mock)
	for i := 0; i < 50; i++ {
		mgr.FireSuccess(&CallbackRequestData{}, &CallbackResponseData{})
	}
	if mgr.Stats().Dropped == 0 {
		t.Fatalf("expected drops, got %+v", mgr.Stats())
	}
	close(mock.block)
	mgr.Wait()
	if err := mgr.Close(); err != nil {
		t.Fatal(err)
	}
	mgr.FireSuccess(&CallbackRequestData{}, &CallbackResponseData{})
	st := mgr.Stats()
	if st.Pending != 0 || st.Succeeded+st.Dropped != 51 {
		t.Fatalf("unexpected final stats %+v", st)
	}
}

func TestCallbackManagerRecoversPanicsAndReportsErrors(t *testing.T) {
	var reported atomic.Int32
	mgr := NewCallbackManagerWithOptions(time.Second, ManagerOptions{OnHandlerError: func(string, error) { reported.Add(1) }})
	defer mgr.Close()
	bad := newMockCallback("bad")
	bad.panics = true
	mgr.Register(bad)
	mgr.FireSuccess(&CallbackRequestData{}, &CallbackResponseData{})
	mgr.Wait()
	if mgr.Stats().Failed != 1 || reported.Load() != 1 {
		t.Fatalf("panic not recovered/reported: %+v", mgr.Stats())
	}
}

func TestCallbackManagerSanitizesAndIsolates(t *testing.T) {
	mgr := NewCallbackManagerWithOptions(time.Second, ManagerOptions{RedactContent: true})
	defer mgr.Close()
	a := newMockCallback("a")
	a.mutate = true
	b := newMockCallback("b")
	mgr.Register(a)
	mgr.Register(b)
	req := &CallbackRequestData{
		Messages: []map[string]interface{}{{"role": "user", "content": "my ssn is 123"}},
		Metadata: map[string]interface{}{"api_key": "sk-secret", "Authorization": "Bearer x", "nested": map[string]interface{}{"password": "p", "ok": 1}, "team": "t1"},
	}
	resp := &CallbackResponseData{Output: "secret answer", Metadata: map[string]interface{}{"x-api-key": "k"}}
	for i := 0; i < 20; i++ {
		mgr.FireSuccess(req, resp)
	}
	mgr.Wait()

	if _, ok := req.Metadata["mutated"]; ok {
		t.Fatal("handler mutation leaked into the caller's data")
	}
	if req.Metadata["api_key"] != "sk-secret" {
		t.Fatal("caller's data must not be modified by sanitisation")
	}
	b.mu.Lock()
	got, gotResp := b.lastReq, b.lastResp
	b.mu.Unlock()
	if got.Metadata["api_key"] != redacted || got.Metadata["Authorization"] != redacted || got.Metadata["team"] != "t1" {
		t.Fatalf("metadata not sanitized: %+v", got.Metadata)
	}
	if got.Metadata["nested"].(map[string]interface{})["password"] != redacted {
		t.Fatal("nested secrets not redacted")
	}
	if got.Messages[0]["content"] != redacted || gotResp.Output != redacted || gotResp.Metadata["x-api-key"] != redacted {
		t.Fatal("content not redacted")
	}
}

func TestWebhookCallbackSignedAndRetried(t *testing.T) {
	var attempts atomic.Int32
	type captured struct {
		body []byte
		h    http.Header
	}
	got := make(chan captured, 5)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if attempts.Add(1) == 1 {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		got <- captured{b, r.Header.Clone()}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	cb := NewWebhookCallback(WebhookConfig{URL: srv.URL, Secret: "whsec", Retries: 3, RetryDelay: 5 * time.Millisecond})
	if err := cb.OnSuccess(context.Background(), &CallbackRequestData{RequestID: "r1"}, &CallbackResponseData{}); err != nil {
		t.Fatal(err)
	}
	c := <-got
	if attempts.Load() != 2 {
		t.Fatalf("expected retry after 502, got %d attempts", attempts.Load())
	}
	if err := webhooks.Verify("whsec", c.h.Get(webhooks.SignatureHeader), c.h.Get(webhooks.TimestampHeader), c.body, time.Minute, time.Now()); err != nil {
		t.Fatalf("signature invalid: %v", err)
	}
	if strings.Contains(string(c.body), "whsec") {
		t.Fatal("secret leaked into payload")
	}
}

func TestWebhookCallbackNoRetryOn4xxAndBadURL(t *testing.T) {
	var attempts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer srv.Close()
	cb := NewWebhookCallback(WebhookConfig{URL: srv.URL, Retries: 5, RetryDelay: time.Millisecond})
	if err := cb.OnError(context.Background(), &CallbackRequestData{}, errors.New("x")); err == nil {
		t.Fatal("expected error")
	}
	if attempts.Load() != 1 {
		t.Fatalf("4xx must not be retried: %d", attempts.Load())
	}
	if err := NewWebhookCallback(WebhookConfig{URL: "ftp://x"}).OnSuccess(context.Background(), nil, nil); err == nil {
		t.Fatal("non-http URL must be rejected")
	}
}

func TestLangfuseIngestionFormat(t *testing.T) {
	var body map[string]interface{}
	var auth, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, path = r.Header.Get("Authorization"), r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&body)
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write([]byte(`{"successes":[{"id":"1","status":201}],"errors":[]}`))
	}))
	defer srv.Close()

	lf := NewLangfuseCallbackWithKeys("pk-lf-1", "sk-lf-2", srv.URL+"/")
	req := &CallbackRequestData{RequestID: "abc", Model: "gpt-4o", Provider: "openai", CostUSD: 0.01, Messages: []map[string]interface{}{{"role": "user", "content": "hi"}}, Metadata: map[string]interface{}{"team": "t"}}
	resp := &CallbackResponseData{Usage: map[string]interface{}{"prompt_tokens": 3, "completion_tokens": 4}, LatencyMs: 20, Output: "hello"}
	if err := lf.OnSuccess(context.Background(), req, resp); err != nil {
		t.Fatal(err)
	}
	if path != "/api/public/ingestion" {
		t.Fatalf("wrong path %q", path)
	}
	if auth != "Basic "+base64.StdEncoding.EncodeToString([]byte("pk-lf-1:sk-lf-2")) {
		t.Fatalf("wrong auth header %q", auth)
	}
	batch := body["batch"].([]interface{})
	if len(batch) != 2 {
		t.Fatalf("expected 2 events, got %d", len(batch))
	}
	ev0, ev1 := batch[0].(map[string]interface{}), batch[1].(map[string]interface{})
	if ev0["type"] != "trace-create" || ev1["type"] != "generation-create" {
		t.Fatalf("unexpected event types %v %v", ev0["type"], ev1["type"])
	}
	gen := ev1["body"].(map[string]interface{})
	usage := gen["usage"].(map[string]interface{})
	if gen["traceId"] != "aerollm-abc" || usage["input"] != float64(3) || usage["output"] != float64(4) || usage["total"] != float64(7) {
		t.Fatalf("unexpected generation body %v", gen)
	}
	raw, _ := json.Marshal(body)
	if strings.Contains(string(raw), "sk-lf-2") {
		t.Fatal("secret key leaked into payload")
	}

	// OnError must not mutate shared metadata.
	if err := lf.OnError(context.Background(), req, errors.New("upstream down")); err != nil {
		t.Fatal(err)
	}
	if _, ok := req.Metadata["error"]; ok {
		t.Fatal("OnError mutated the request metadata")
	}
}

func TestLangfuseReportsPartialFailures(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMultiStatus)
		_, _ = w.Write([]byte(`{"successes":[],"errors":[{"id":"1","status":400,"message":"invalid body"}]}`))
	}))
	defer srv.Close()
	if err := NewLangfuseCallback("pk:sk", srv.URL, "").OnSuccess(context.Background(), nil, nil); err == nil || !strings.Contains(err.Error(), "invalid body") {
		t.Fatalf("expected partial failure error, got %v", err)
	}
	if err := NewLangfuseCallbackWithKeys("", "", srv.URL).OnSuccess(context.Background(), nil, nil); err == nil {
		t.Fatal("missing keys must error")
	}
	legacy := NewLangfuseCallback("sk-lf-x", "", "pk-lf-y")
	if legacy.publicKey != "pk-lf-y" || legacy.secretKey != "sk-lf-x" || legacy.baseURL != DefaultLangfuseURL {
		t.Fatalf("legacy constructor mapping wrong: %+v", legacy)
	}
}

func TestDatadogPayloadsAndAuth(t *testing.T) {
	var mu sync.Mutex
	bodies := map[string][]byte{}
	var keys []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		bodies[r.URL.Path] = b
		keys = append(keys, r.Header.Get("DD-API-KEY"))
		mu.Unlock()
		if r.Header.Get("Authorization") != "" {
			t.Errorf("Authorization header must not be used")
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	dd := NewDatadogCallback("dd-key-123", srv.URL, "")
	req := &CallbackRequestData{RequestID: "r-1", Model: "GPT 4o", Provider: "openai", CostUSD: 0.5}
	resp := &CallbackResponseData{LatencyMs: 42, TokenCount: map[string]int{"input": 1, "output": 2}}
	if err := dd.OnSuccess(context.Background(), req, resp); err != nil {
		t.Fatal(err)
	}
	var metrics struct {
		Series []ddSeries `json:"series"`
	}
	if err := json.Unmarshal(bodies["/api/v2/series"], &metrics); err != nil || len(metrics.Series) != 5 {
		t.Fatalf("bad series payload: %s (%v)", bodies["/api/v2/series"], err)
	}
	for _, s := range metrics.Series {
		for _, tag := range s.Tags {
			if strings.HasPrefix(tag, "request_id:") {
				t.Fatal("request_id must not be a metric tag")
			}
			if strings.ContainsAny(tag, " ,") {
				t.Fatalf("unsanitized tag %q", tag)
			}
		}
		if s.Type != ddTypeCount && s.Type != ddTypeGauge {
			t.Fatalf("invalid metric type %d", s.Type)
		}
	}
	var logs []ddLog
	if err := json.Unmarshal(bodies["/api/v2/logs"], &logs); err != nil || len(logs) != 1 || logs[0].RequestID != "r-1" || logs[0].DDSource != "aerollm" {
		t.Fatalf("bad logs payload: %s", bodies["/api/v2/logs"])
	}
	for _, k := range keys {
		if k != "dd-key-123" {
			t.Fatalf("DD-API-KEY missing: %q", k)
		}
	}
	for _, b := range bodies {
		if strings.Contains(string(b), "dd-key-123") {
			t.Fatal("API key leaked into payload")
		}
	}
	// OnError used to panic because the error parameter was shadowed.
	if err := dd.OnError(context.Background(), req, errors.New("boom")); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(bodies["/api/v2/logs"]), "boom") {
		t.Fatal("error message missing from log")
	}
}

func TestDatadogErrorsAndSite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	defer srv.Close()
	if err := NewDatadogCallback("k", srv.URL, "").OnSuccess(context.Background(), nil, nil); err == nil {
		t.Fatal("403 must be an error")
	}
	d := NewDatadogCallback("k", "", "evil.com/#")
	if _, _, err := d.endpoints(); err == nil {
		t.Fatal("malformed site must be rejected")
	}
	m, l, err := NewDatadogCallback("k", "", "datadoghq.eu").endpoints()
	if err != nil || m != "https://api.datadoghq.eu/api/v2/series" || l != "https://http-intake.logs.datadoghq.eu/api/v2/logs" {
		t.Fatalf("unexpected endpoints %q %q %v", m, l, err)
	}
	if err := NewDatadogCallback("", srv.URL, "").OnSuccess(context.Background(), nil, nil); err == nil {
		t.Fatal("missing api key must error")
	}
}
