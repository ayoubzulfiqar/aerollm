package schedule

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type capturedRequest struct {
	method string
	header http.Header
	body   []byte
}

// recordingServer returns a test server that records requests and replies
// with status.
func recordingServer(t *testing.T, status int) (*httptest.Server, func() []capturedRequest) {
	t.Helper()
	var mu sync.Mutex
	var reqs []capturedRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		reqs = append(reqs, capturedRequest{method: r.Method, header: r.Header.Clone(), body: body})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)
	return srv, func() []capturedRequest {
		mu.Lock()
		defer mu.Unlock()
		return append([]capturedRequest(nil), reqs...)
	}
}

// localExecutor allows the loopback, plain-HTTP test server.
func localExecutor(t *testing.T, opts WebhookExecutorOptions) Executor {
	t.Helper()
	opts.AllowInsecureHTTP = true
	opts.AllowPrivateNetworks = true
	exec, err := NewWebhookExecutor(opts)
	if err != nil {
		t.Fatal(err)
	}
	return exec
}

func webhookTask(url, extra string) ScheduledTask {
	return ScheduledTask{
		ID:      "task_1",
		Name:    "nightly-report",
		Payload: `{"type":"webhook","url":"` + url + `"` + extra + `}`,
		NextRun: utc(2026, 1, 1, 3, 0),
	}
}

func TestWebhookExecutorDelivers(t *testing.T) {
	srv, reqs := recordingServer(t, http.StatusAccepted)
	secret := []byte("s3cret")
	exec := localExecutor(t, WebhookExecutorOptions{SigningSecret: secret})
	task := webhookTask(srv.URL+"/hook?x=1", `,"headers":{"Authorization":"Bearer abc","x-custom":"v"},"body":{"report":"daily","n":3}`)
	if err := exec(context.Background(), task); err != nil {
		t.Fatal(err)
	}
	got := reqs()
	if len(got) != 1 {
		t.Fatalf("expected 1 request, got %d", len(got))
	}
	r := got[0]
	if r.method != http.MethodPost {
		t.Fatalf("method %s", r.method)
	}
	if string(r.body) != `{"report":"daily","n":3}` {
		t.Fatalf("body %q", r.body)
	}
	checks := map[string]string{
		"Content-Type":    "application/json",
		"User-Agent":      DefaultWebhookUserAgent,
		"Authorization":   "Bearer abc",
		"X-Custom":        "v",
		HeaderTaskID:      "task_1",
		HeaderScheduledAt: "2026-01-01T03:00:00Z",
	}
	for k, want := range checks {
		if v := r.header.Get(k); v != want {
			t.Errorf("header %s = %q, want %q", k, v, want)
		}
	}
	ts := r.header.Get(HeaderTimestamp)
	sig := strings.TrimPrefix(r.header.Get(HeaderSignature), "v1=")
	if ts == "" || !hmac.Equal([]byte(sig), []byte(SignWebhook(secret, ts, r.body))) {
		t.Fatalf("invalid signature %q for timestamp %q", r.header.Get(HeaderSignature), ts)
	}
}

func TestWebhookExecutorDefaultEventBody(t *testing.T) {
	srv, reqs := recordingServer(t, http.StatusOK)
	exec := localExecutor(t, WebhookExecutorOptions{})
	if err := exec(context.Background(), webhookTask(srv.URL, "")); err != nil {
		t.Fatal(err)
	}
	r := reqs()[0]
	var ev map[string]any
	if err := json.Unmarshal(r.body, &ev); err != nil {
		t.Fatalf("body %q: %v", r.body, err)
	}
	if ev["event"] != "schedule.task.run" || ev["task_id"] != "task_1" || ev["task_name"] != "nightly-report" || ev["scheduled_at"] != "2026-01-01T03:00:00Z" {
		t.Fatalf("unexpected event %v", ev)
	}
	if r.header.Get(HeaderSignature) != "" {
		t.Fatal("unsigned executor must not send a signature")
	}
}

func TestWebhookExecutorFailures(t *testing.T) {
	failing, _ := recordingServer(t, http.StatusServiceUnavailable)
	exec := localExecutor(t, WebhookExecutorOptions{})
	if err := exec(context.Background(), webhookTask(failing.URL, "")); err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("expected status error, got %v", err)
	}

	// Redirects are not followed.
	target, targetReqs := recordingServer(t, http.StatusOK)
	redirect := httptest.NewServer(http.RedirectHandler(target.URL, http.StatusFound))
	t.Cleanup(redirect.Close)
	if err := exec(context.Background(), webhookTask(redirect.URL, "")); err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("expected redirect error, got %v", err)
	}
	if len(targetReqs()) != 0 {
		t.Fatal("redirect was followed")
	}

	// Timeouts.
	block := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(block); slow.Close() })
	fast := localExecutor(t, WebhookExecutorOptions{Timeout: 50 * time.Millisecond})
	start := time.Now()
	if err := fast(context.Background(), webhookTask(slow.URL, "")); err == nil {
		t.Fatal("expected timeout")
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("timeout not enforced")
	}
	// Context cancellation (runner stop / TaskTimeout).
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := exec(ctx, webhookTask(slow.URL, "")); err == nil {
		t.Fatal("expected context error")
	}

	// Errors never echo the URL (which may carry tokens).
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closedAddr := ln.Addr().String()
	ln.Close()
	err = exec(context.Background(), webhookTask("http://"+closedAddr+"/hook/SECRET-PATH?token=SECRET-TOKEN", ""))
	if err == nil || strings.Contains(err.Error(), "SECRET") {
		t.Fatalf("expected connection error without URL secrets, got %v", err)
	}
}

func TestWebhookExecutorRejectsNonWebhookTasks(t *testing.T) {
	exec := localExecutor(t, WebhookExecutorOptions{})
	for _, payload := range []string{"", "hello", "{}", `{"type":"email","url":"https://x"}`, `[1,2]`} {
		err := exec(context.Background(), ScheduledTask{ID: "t", Payload: payload})
		if !errors.Is(err, ErrNotWebhookTask) {
			t.Errorf("payload %q: expected ErrNotWebhookTask, got %v", payload, err)
		}
	}
}

func TestWebhookExecutorSSRF(t *testing.T) {
	exec, err := NewWebhookExecutor(WebhookExecutorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	blocked := []string{
		"https://127.0.0.1/hook",
		"https://[::1]/hook",
		"https://10.1.2.3/hook",
		"https://169.254.169.254/latest/meta-data",
		"https://[::ffff:127.0.0.1]/hook",
		"https://localhost/hook",
		"https://LOCALHOST./hook",
		"https://api.localhost/hook",
		"https://metadata.google.internal/computeMetadata",
		"https://2130706433/hook",
		"https://0x7f.1/hook",
		"https://0177.0.0.1/hook",
	}
	for _, u := range blocked {
		err := exec(context.Background(), webhookTask(u, ""))
		if !errors.Is(err, ErrBlockedDestination) {
			t.Errorf("%s: expected ErrBlockedDestination, got %v", u, err)
		}
	}
	// Plain HTTP needs an explicit opt-in.
	if err := exec(context.Background(), webhookTask("http://example.com/hook", "")); err == nil || !strings.Contains(err.Error(), "https") {
		t.Fatalf("expected https requirement, got %v", err)
	}
	// Plain HTTP allowed, private networks not: the loopback test server is
	// refused (literal host check) before any request is sent.
	srv, reqs := recordingServer(t, http.StatusOK)
	insecure, err := NewWebhookExecutor(WebhookExecutorOptions{AllowInsecureHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := insecure(context.Background(), webhookTask(srv.URL, "")); !errors.Is(err, ErrBlockedDestination) {
		t.Fatalf("expected loopback to be blocked, got %v", err)
	}
	if len(reqs()) != 0 {
		t.Fatal("request reached a blocked destination")
	}

	// Dial-time check (covers DNS names resolving to private space and
	// rebinding).
	ctl := dialControl(false)
	for _, addr := range []string{"127.0.0.1:443", "[::1]:443", "10.0.0.1:80", "192.168.1.1:80", "169.254.169.254:80", "[fd00::1]:443", "0.0.0.0:80", "bogus"} {
		if err := ctl("tcp", addr, nil); !errors.Is(err, ErrBlockedDestination) {
			t.Errorf("dial %s: expected block, got %v", addr, err)
		}
	}
	for _, addr := range []string{"8.8.8.8:443", "[2606:4700:4700::1111]:443"} {
		if err := ctl("tcp", addr, nil); err != nil {
			t.Errorf("dial %s: unexpected block %v", addr, err)
		}
	}
	if err := dialControl(true)("tcp", "127.0.0.1:80", nil); err != nil {
		t.Fatalf("allowPrivate must permit loopback: %v", err)
	}
}

func TestWebhookExecutorAllowedHosts(t *testing.T) {
	srv, reqs := recordingServer(t, http.StatusOK)
	exec := localExecutor(t, WebhookExecutorOptions{AllowedHosts: []string{"hooks.example.com"}})
	if err := exec(context.Background(), webhookTask(srv.URL, "")); err == nil || !strings.Contains(err.Error(), "allowed host") {
		t.Fatalf("expected allowlist rejection, got %v", err)
	}
	if len(reqs()) != 0 {
		t.Fatal("request sent to a host outside the allowlist")
	}
	ok := localExecutor(t, WebhookExecutorOptions{AllowedHosts: []string{" 127.0.0.1 "}})
	if err := ok(context.Background(), webhookTask(srv.URL, "")); err != nil {
		t.Fatalf("allowlisted host rejected: %v", err)
	}
}

func TestNewWebhookExecutorOptions(t *testing.T) {
	bad := []WebhookExecutorOptions{
		{Timeout: -time.Second},
		{Timeout: time.Hour},
		{MaxResponseBytes: -1},
		{UserAgent: "x\r\nInjected: 1"},
	}
	for i, o := range bad {
		if _, err := NewWebhookExecutor(o); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
	env := map[string]string{
		"AEROLLM_SCHEDULE_WEBHOOK_SECRET":        "k",
		"AEROLLM_SCHEDULE_WEBHOOK_TIMEOUT":       "15s",
		"AEROLLM_SCHEDULE_WEBHOOK_ALLOWED_HOSTS": "a.example.com, b.example.com,,",
	}
	o, err := WebhookExecutorOptionsFromEnv(func(k string) string { return env[k] })
	if err != nil {
		t.Fatal(err)
	}
	if string(o.SigningSecret) != "k" || o.Timeout != 15*time.Second || len(o.AllowedHosts) != 2 || o.AllowedHosts[1] != "b.example.com" {
		t.Fatalf("unexpected options %+v", o)
	}
	if o.AllowInsecureHTTP || o.AllowPrivateNetworks {
		t.Fatal("env helper must never relax SSRF protections")
	}
	env["AEROLLM_SCHEDULE_WEBHOOK_TIMEOUT"] = "forever"
	if _, err := WebhookExecutorOptionsFromEnv(func(k string) string { return env[k] }); err == nil {
		t.Fatal("expected timeout parse error")
	}
	if _, err := WebhookExecutorOptionsFromEnv(nil); err == nil {
		t.Fatal("expected error for nil getenv")
	}
}

func TestStoreValidatesWebhookPayloads(t *testing.T) {
	s := fixedStore(utc(2026, 1, 1, 0, 0))
	bad := []string{
		`{"type":"webhook"}`,
		`{"type":"webhook","url":"ftp://example.com/x"}`,
		`{"type":"webhook","url":"https://user:pw@example.com/x"}`,
		`{"type":"webhook","url":"/relative"}`,
		`{"type":"webhook","url":"https://example.com:99999/x"}`,
		`{"type":"webhook","url":"https://example.com/x","headers":{"Host":"evil"}}`,
		`{"type":"webhook","url":"https://example.com/x","headers":{"X-AeroLLM-Signature":"forged"}}`,
		`{"type":"webhook","url":"https://example.com/x","headers":{"X-Ok":"a\r\nInjected: 1"}}`,
		`{"type":"webhook","url":"https://example.com/x","headers":{"Bad Name":"v"}}`,
		`{"type":"webhook","url":"https://example.com/x","methd":"GET"}`,
	}
	for _, p := range bad {
		_, err := s.Create(ScheduledTask{Name: "w", Schedule: "@daily", Payload: p})
		var verr *ValidationError
		if !errors.As(err, &verr) {
			t.Errorf("payload %s: expected validation error, got %v", p, err)
		}
	}
	good := []string{
		`{"type":"webhook","url":"https://hooks.example.com/run","headers":{"Authorization":"Bearer x"},"body":{"a":1}}`,
		`{"type":"webhook","url":"http://internal.example/run"}`, // enforced by the executor, not the store
		`{"type":"not-a-webhook","anything":true}`,
		`plain text payload`,
	}
	for _, p := range good {
		if _, err := s.Create(ScheduledTask{Name: "w", Schedule: "@daily", Payload: p}); err != nil {
			t.Errorf("payload %s: unexpected error %v", p, err)
		}
	}
}

// TestRunnerWithWebhookExecutor wires the default executor into a Runner.
func TestRunnerWithWebhookExecutor(t *testing.T) {
	srv, reqs := recordingServer(t, http.StatusNoContent)
	start := utc(2026, 1, 1, 0, 0)
	s, clk := newClockStore(start)
	task, err := s.Create(ScheduledTask{Name: "ping", Type: TaskOneTime, RunAt: start.Add(time.Second),
		Payload: `{"type":"webhook","url":"` + srv.URL + `","body":{"ping":true}}`})
	if err != nil {
		t.Fatal(err)
	}
	other, err := s.Create(ScheduledTask{Name: "not-webhook", Type: TaskOneTime, RunAt: start.Add(time.Second), Payload: "noop"})
	if err != nil {
		t.Fatal(err)
	}
	startRunner(t, s, localExecutor(t, WebhookExecutorOptions{}), RunnerOptions{})
	clk.Advance(2 * time.Second)
	waitFor(t, "webhook runs", func() bool {
		a, _ := s.Get(task.ID)
		b, _ := s.Get(other.ID)
		return a.Status == TaskCompleted && b.Status == TaskFailed
	})
	if got := reqs(); len(got) != 1 || string(got[0].body) != `{"ping":true}` || got[0].header.Get(HeaderScheduledAt) != "2026-01-01T00:00:01Z" {
		t.Fatalf("unexpected deliveries %+v", got)
	}
	if b, _ := s.Get(other.ID); !strings.Contains(b.LastError, "not a webhook task") {
		t.Fatalf("unexpected last error %q", b.LastError)
	}
}
