package main

import (
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

const batchJSONL = `{"custom_id":"r1","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"hi"}]}}
{"custom_id":"r2","method":"POST","url":"/v1/chat/completions","body":{"model":"m","messages":[{"role":"user","content":"yo"}]}}
`

const batchObj = `{"id":"batch_abc","object":"batch","status":"validating","created_at":1700000000,"completion_window":"12h0m0s","request_counts":{"total":2,"completed":0,"failed":0}}`

func TestBatchesCreateJSON(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/batches", 200, batchObj)
	in := filepath.Join(t.TempDir(), "requests.jsonl")
	if err := os.WriteFile(in, []byte(batchJSONL), 0o600); err != nil {
		t.Fatal(err)
	}

	out, stderr, err := g.run(t, "", "batches", "create", "--file", in, "--metadata", "team=search", "--completion-window", "12h")
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	if req.Method != http.MethodPost || req.Path != "/v1/batches" || req.Auth != "Bearer sk-test-master-key" {
		t.Fatalf("request = %s %s auth=%q", req.Method, req.Path, req.Auth)
	}
	if ct := req.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q", ct)
	}
	body := decodeBody(t, req.Body)
	if body["input"] != batchJSONL || body["completion_window"] != "12h" || body["metadata"].(map[string]any)["team"] != "search" {
		t.Fatalf("body = %s", req.Body)
	}
	if !strings.Contains(out, `"id": "batch_abc"`) || !strings.Contains(stderr, "created batch batch_abc with 2 request(s)") {
		t.Fatalf("out=%q stderr=%q", out, stderr)
	}

	// Stdin when --file is omitted; table output formats timestamps/counts.
	out, _, err = g.run(t, batchJSONL, "batches", "create", "-o", "table")
	if err != nil {
		t.Fatal(err)
	}
	if decodeBody(t, g.last(t).Body)["input"] != batchJSONL {
		t.Fatalf("stdin input not sent: %s", g.last(t).Body)
	}
	if !strings.Contains(out, "2023-11-14T22:13:20Z") || !strings.Contains(out, "total=2 completed=0 failed=0") {
		t.Fatalf("table = %q", out)
	}
}

func TestBatchesCreateMultipart(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	var gotFile, gotName, gotWindow string
	g.handle("/v1/batches", func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseMultipartForm(1 << 20); err != nil {
			http.Error(w, `{"error":"not multipart"}`, http.StatusBadRequest)
			return
		}
		f, hdr, err := r.FormFile("file")
		if err != nil {
			http.Error(w, `{"error":"no file"}`, http.StatusBadRequest)
			return
		}
		b, _ := io.ReadAll(f)
		gotFile, gotName, gotWindow = string(b), hdr.Filename, r.FormValue("completion_window")
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, batchObj)
	})
	in := filepath.Join(t.TempDir(), "big.jsonl")
	_ = os.WriteFile(in, []byte(batchJSONL), 0o600)
	if _, _, err := g.run(t, "", "batches", "create", "--file", in, "--upload", "multipart", "--completion-window", "24h"); err != nil {
		t.Fatal(err)
	}
	if gotFile != batchJSONL || gotName != "big.jsonl" || gotWindow != "24h" {
		t.Fatalf("multipart: file=%q name=%q window=%q", gotFile, gotName, gotWindow)
	}
	if ct := g.last(t).Header.Get("Content-Type"); !strings.HasPrefix(ct, "multipart/form-data; boundary=") {
		t.Fatalf("Content-Type = %q", ct)
	}
}

func TestBatchesCreateValidation(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/batches", 200, batchObj)
	cases := map[string]struct {
		stdin string
		args  []string
		want  string
	}{
		"not json":           {"{nope\n", nil, "line 1"},
		"array line":         {"[1]\n", nil, "line 1"},
		"missing custom_id":  {`{"method":"POST"}` + "\n", nil, "custom_id is required"},
		"duplicate":          {`{"custom_id":"a"}` + "\n\n" + `{"custom_id":"a"}` + "\n", nil, "duplicates line 1"},
		"empty":              {"\n\n", nil, "no requests"},
		"bad window":         {batchJSONL, []string{"--completion-window", "forever"}, "completion-window"},
		"window too long":    {batchJSONL, []string{"--completion-window", "200h"}, "at most"},
		"bad metadata":       {batchJSONL, []string{"--metadata", "novalue"}, "key=value"},
		"metadata multipart": {batchJSONL, []string{"--metadata", "a=b", "--upload", "multipart"}, "JSON upload"},
		"bad upload":         {batchJSONL, []string{"--upload", "carrier-pigeon"}, "--upload"},
	}
	for name, c := range cases {
		_, _, err := g.run(t, c.stdin, append([]string{"batches", "create"}, c.args...)...)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want %q", name, err, c.want)
		}
	}
	if n := len(g.all()); n != 0 {
		t.Fatalf("invalid input must not be sent; %d request(s) made", n)
	}

	// A batch that fails server-side validation is printed and exits non-zero.
	g.json("/v1/batches", 200, `{"id":"batch_bad","status":"failed","errors":[{"code":"invalid_url","message":"url must be /v1/chat/completions","line":1}]}`)
	out, _, err := g.run(t, batchJSONL, "batches", "create")
	if err == nil || !strings.Contains(err.Error(), "failed validation (1 error(s))") || !strings.Contains(out, "invalid_url") {
		t.Fatalf("failed batch: out=%q err=%v", out, err)
	}
}

func TestBatchesListGetCancel(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/batches", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("after") == "batch_1" {
			_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"batch_2","status":"failed","created_at":1700000000,"request_counts":{"total":1,"completed":0,"failed":1}}],"last_id":"batch_2","has_more":false}`)
			return
		}
		_, _ = io.WriteString(w, `{"object":"list","data":[{"id":"batch_1","status":"completed","created_at":1700000000,"request_counts":{"total":3,"completed":3,"failed":0}}],"first_id":"batch_1","last_id":"batch_1","has_more":true}`)
	})
	out, stderr, err := g.run(t, "", "batches", "list", "--limit", "10", "--after", "batch_0")
	if err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodGet || req.Query != "after=batch_0&limit=10" {
		t.Fatalf("list request = %s ?%s", req.Method, req.Query)
	}
	if !strings.Contains(out, "batch_1") || !strings.Contains(out, "2023-11-14T22:13:20Z") || !strings.Contains(stderr, "has_more=true") {
		t.Fatalf("list out=%q stderr=%q", out, stderr)
	}
	out, _, err = g.run(t, "", "batches", "list", "--all", "-o", "json")
	if err != nil || !strings.Contains(out, "batch_1") || !strings.Contains(out, "batch_2") || len(g.all()) != 3 {
		t.Fatalf("list --all: out=%q err=%v requests=%d", out, err, len(g.all()))
	}
	if _, _, err := g.run(t, "", "batches", "list", "--limit", "500"); err == nil {
		t.Fatal("expected --limit validation error")
	}

	g.json("/v1/batches/batch_1", 200, `{"id":"batch_1","status":"completed"}`)
	out, _, err = g.run(t, "", "batches", "get", "batch_1")
	if err != nil || !strings.Contains(out, `"status": "completed"`) || g.last(t).Method != http.MethodGet {
		t.Fatalf("get: %q %v", out, err)
	}
	g.json("/v1/batches/batch_1/cancel", 200, `{"id":"batch_1","status":"cancelling"}`)
	out, _, err = g.run(t, "", "batches", "cancel", "batch_1")
	if err != nil || !strings.Contains(out, "cancelling") || g.last(t).Method != http.MethodPost {
		t.Fatalf("cancel: %q %v", out, err)
	}
	g.json("/v1/batches/batch_9/cancel", http.StatusConflict, `{"error":{"message":"batch is not in a cancellable state"}}`)
	if _, _, err := g.run(t, "", "batches", "cancel", "batch_9"); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("cancel conflict: %v", err)
	}

	before := len(g.all())
	for _, bad := range []string{"../etc", "a/b", "", "x y"} {
		if _, _, err := g.run(t, "", "batches", "get", bad); err == nil {
			t.Errorf("get %q: expected invalid id error", bad)
		}
	}
	if len(g.all()) != before {
		t.Fatal("invalid batch ids must not be requested")
	}
}

func TestBatchesResultsAndErrors(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	const results = `{"id":"r1","custom_id":"r1","response":{"status_code":200}}` + "\n"
	const errs = `{"id":"r2","custom_id":"r2","error":{"code":"x"}}` + "\n"
	for path, body := range map[string]string{"/v1/batches/batch_1/results": results, "/v1/batches/batch_1/errors": errs} {
		body := body
		g.handle(path, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/jsonl")
			_, _ = io.WriteString(w, body)
		})
	}
	out, _, err := g.run(t, "", "batches", "results", "batch_1")
	if err != nil || out != results {
		t.Fatalf("results to stdout: %q %v", out, err)
	}
	out, _, err = g.run(t, "", "batches", "errors", "batch_1")
	if err != nil || out != errs || g.last(t).Path != "/v1/batches/batch_1/errors" {
		t.Fatalf("errors to stdout: %q %v", out, err)
	}

	dest := filepath.Join(t.TempDir(), "out.jsonl")
	out, stderr, err := g.run(t, "", "batches", "results", "batch_1", "--out", dest)
	if err != nil || out != "" || !strings.Contains(stderr, "wrote") {
		t.Fatalf("results --out: out=%q stderr=%q err=%v", out, stderr, err)
	}
	if b, _ := os.ReadFile(dest); string(b) != results {
		t.Fatalf("file = %q", b)
	}
	assertPerm(t, dest, 0o600)
	n := len(g.all())
	if _, _, err := g.run(t, "", "batches", "results", "batch_1", "--out", dest); err == nil || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("expected overwrite refusal, got %v", err)
	}
	if len(g.all()) != n {
		t.Fatal("existing --out must be rejected before downloading")
	}
	if _, _, err := g.run(t, "", "batches", "errors", "batch_1", "--out", dest, "--force"); err != nil {
		t.Fatal(err)
	}
	if b, _ := os.ReadFile(dest); string(b) != errs {
		t.Fatalf("--force did not overwrite: %q", b)
	}

	g.json("/v1/batches/batch_2/results", http.StatusConflict, `{"error":{"message":"batch results are not available yet"}}`)
	missing := filepath.Join(t.TempDir(), "never.jsonl")
	if _, _, err := g.run(t, "", "batches", "results", "batch_2", "--out", missing); err == nil || !strings.Contains(err.Error(), "not available yet") {
		t.Fatalf("expected 409 error, got %v", err)
	}
	if _, err := os.Stat(missing); err == nil {
		t.Fatal("no file may be created when the download fails")
	}
}

func TestBatchesWait(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	var polls atomic.Int32
	g.handle("/v1/batches/batch_1", func(w http.ResponseWriter, r *http.Request) {
		n := polls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch {
		case n == 1:
			http.Error(w, `{"error":"try later"}`, http.StatusServiceUnavailable)
		case n < 4:
			_, _ = io.WriteString(w, `{"id":"batch_1","status":"in_progress","request_counts":{"total":2,"completed":1,"failed":0}}`)
		default:
			_, _ = io.WriteString(w, `{"id":"batch_1","status":"completed","request_counts":{"total":2,"completed":2,"failed":0}}`)
		}
	})
	out, stderr, err := g.run(t, "", "batches", "wait", "batch_1", "--interval", "1ms")
	if err != nil {
		t.Fatalf("wait: %v\n%s", err, stderr)
	}
	if polls.Load() != 4 || !strings.Contains(out, `"status": "completed"`) {
		t.Fatalf("polls=%d out=%q", polls.Load(), out)
	}
	if !strings.Contains(stderr, "retrying") || strings.Count(stderr, "in_progress (completed 1/2") != 1 || !strings.Contains(stderr, "completed (completed 2/2") {
		t.Fatalf("progress = %q", stderr)
	}

	g.json("/v1/batches/batch_2", 200, `{"id":"batch_2","status":"expired"}`)
	if _, _, err := g.run(t, "", "batches", "wait", "batch_2", "--interval", "1ms"); err == nil || !strings.Contains(err.Error(), `status "expired"`) {
		t.Fatalf("expected non-completed error, got %v", err)
	}
	g.json("/v1/batches/batch_3", 200, `{"id":"batch_3","status":"in_progress"}`)
	if _, _, err := g.run(t, "", "batches", "wait", "batch_3", "--interval", "5ms", "--max-wait", "40ms"); err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout, got %v", err)
	}
	g.json("/v1/batches/batch_4", http.StatusNotFound, `{"error":{"message":"batch not found"}}`)
	if _, _, err := g.run(t, "", "batches", "wait", "batch_4", "--interval", "1ms"); err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("expected immediate 404, got %v", err)
	}
	// An unreachable server fails fast instead of retrying until --max-wait.
	if _, _, err := runCLI(t, "", "--server", "http://127.0.0.1:1", "batches", "wait", "batch_1", "--interval", "1ms"); err == nil {
		t.Fatal("expected connection error")
	}
}
