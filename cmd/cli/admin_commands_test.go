package main

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
)

const (
	testKeyHash = "3f9a000000000000000000000000000000000000000000000000000000000001"
	testKeyID   = "key_0123456789abcdef"
	rawTestKey  = "sk-raw-user-key-0123456789abcdef"
)

// noLeak fails if any of the secrets appears in the given strings.
func noLeak(t *testing.T, where string, secrets []string, texts ...string) {
	t.Helper()
	all := strings.Join(texts, "\n")
	for _, s := range secrets {
		if strings.Contains(all, s) {
			t.Fatalf("%s: secret %q leaked:\n%s", where, s, all)
		}
	}
}

func TestKeysList(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/key/list", 200, `{"keys":[{"key_hash":"h1","prefix":"sk-ab","status":"active","blocked":false,"team_id":"t1",
		"spend":1.5,"max_budget":10,"expires_at":"0001-01-01T00:00:00Z","key":"`+rawTestKey+`"}]}`)
	out, _, err := g.run(t, "", "keys", "list", "--team-id", "t1", "--include-revoked")
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	if req.Method != http.MethodGet || req.Query != "include_revoked=true&team_id=t1" {
		t.Fatalf("request = %s ?%s", req.Method, req.Query)
	}
	if !regexp.MustCompile(`h1\s+sk-ab\s+active\s+false\s+t1`).MatchString(out) || !strings.Contains(out, "never") {
		t.Fatalf("table = %q", out)
	}
	jsonOut, _, err := g.run(t, "", "keys", "list", "-o", "json")
	if err != nil || !strings.Contains(jsonOut, `"keys"`) {
		t.Fatalf("json: %q %v", jsonOut, err)
	}
	noLeak(t, "keys list", []string{rawTestKey}, out, jsonOut)
}

func TestKeysUpdate(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/key/update", 200, `{"key_hash":"h1","status":"active","max_budget":50,"token":"`+rawTestKey+`"}`)

	out, stderr, err := g.run(t, "", "keys", "update", "--key-hash", testKeyHash, "--max-budget", "50",
		"--models", "", "--blocked=false", "--role", "member", "--metadata", `{"env":"prod"}`)
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	if req.Method != http.MethodPost || req.Path != "/key/update" {
		t.Fatalf("request = %s %s", req.Method, req.Path)
	}
	body := decodeBody(t, req.Body)
	if body["key_hash"] != testKeyHash || body["max_budget"] != 50.0 || body["blocked"] != false || body["role"] != "" {
		t.Fatalf("body = %s", req.Body)
	}
	if m, ok := body["models"].([]any); !ok || len(m) != 0 {
		t.Fatalf(`--models "" must send an empty list: %s`, req.Body)
	}
	for _, unset := range []string{"duration", "rate_limit_rps", "rate_limit_tpm", "aliases", "reset_spend"} {
		if _, ok := body[unset]; ok {
			t.Errorf("unchanged field %q was sent: %s", unset, req.Body)
		}
	}
	noLeak(t, "keys update", []string{rawTestKey}, out, stderr)

	// Key read from stdin goes in the body, never the URL or output.
	out, stderr, err = g.run(t, rawTestKey+"\n", "keys", "update", "-", "--rps", "2.5", "--reset-spend")
	if err != nil {
		t.Fatal(err)
	}
	req = g.last(t)
	if b := decodeBody(t, req.Body); b["key"] != rawTestKey || b["rate_limit_rps"] != 2.5 || b["reset_spend"] != true || req.Query != "" {
		t.Fatalf("stdin update = ?%s %s", req.Query, req.Body)
	}
	noLeak(t, "keys update stdin", []string{rawTestKey}, out, stderr)

	n := len(g.all())
	for _, bad := range [][]string{
		{"keys", "update", "--key-hash", testKeyHash},
		{"keys", "update", "--key-hash", testKeyHash, "--max-budget", "-1"},
		{"keys", "update", "--key-hash", testKeyHash, "--role", "root"},
		{"keys", "update", "--key-hash", testKeyHash, "--metadata", "[1]"},
		{"keys", "update", "--max-budget", "1"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
	if len(g.all()) != n {
		t.Fatal("invalid updates must not be sent")
	}
}

func TestKeysBlockUnblock(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	for _, p := range []string{"/key/block", "/key/unblock"} {
		g.handle(p, func(w http.ResponseWriter, r *http.Request) {
			if strings.Contains(readBody(r), "sk-missing") {
				http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"key_hash":"h","blocked":true}`)
		})
	}
	out, stderr, err := g.run(t, rawTestKey+"\n", "keys", "block", "-", "--key-hash", testKeyHash)
	if err != nil {
		t.Fatal(err)
	}
	reqs := g.all()
	if len(reqs) != 2 || reqs[0].Path != "/key/block" || decodeBody(t, reqs[0].Body)["key_hash"] != testKeyHash ||
		decodeBody(t, reqs[1].Body)["key"] != rawTestKey {
		t.Fatalf("block requests = %+v", reqs)
	}
	if !strings.Contains(out, "blocked key hash "+testKeyHash) || !strings.Contains(out, "blocked key sk-raw...cdef") {
		t.Fatalf("stdout = %q", out)
	}
	noLeak(t, "keys block", []string{rawTestKey}, out, stderr)

	out, stderr, err = g.run(t, "", "keys", "unblock", "sk-missing-key-000000000", "--key-hash", testKeyHash)
	if err == nil || !strings.Contains(err.Error(), "1 of 2 key(s) could not be unblocked") {
		t.Fatalf("expected partial failure, got %v", err)
	}
	if g.last(t).Path != "/key/unblock" || !strings.Contains(out, "unblocked key hash") || !strings.Contains(stderr, "404") {
		t.Fatalf("out=%q stderr=%q", out, stderr)
	}
	noLeak(t, "keys unblock", []string{"sk-missing-key-000000000"}, out, stderr, err.Error())
}

func readBody(r *http.Request) string {
	b, _ := io.ReadAll(r.Body)
	return string(b)
}

func TestKeysRegenerate(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	const newKey = "sk-new-rotated-key-9876543210"
	g.json("/key/regenerate", 200, `{"key_hash":"h2","key":"`+newKey+`","expires":"2027-01-01T00:00:00Z"}`)
	out, stderr, err := g.run(t, rawTestKey+"\n", "keys", "regenerate", "-")
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	if req.Method != http.MethodPost || decodeBody(t, req.Body)["key"] != rawTestKey {
		t.Fatalf("request = %s %s", req.Method, req.Body)
	}
	if strings.Count(out, newKey) != 1 || !strings.Contains(stderr, "previous key has been revoked") {
		t.Fatalf("out=%q stderr=%q", out, stderr)
	}
	noLeak(t, "keys regenerate", []string{rawTestKey}, out, stderr)
	out, _, err = g.run(t, "", "keys", "regenerate", "--key-hash", testKeyHash, "-o", "json")
	if err != nil || !strings.Contains(out, `"key": "`+newKey+`"`) || decodeBody(t, g.last(t).Body)["key_hash"] != testKeyHash {
		t.Fatalf("json: %q %v", out, err)
	}
	if _, _, err := g.run(t, "", "keys", "regenerate", "a", "--key-hash", testKeyHash); err == nil {
		t.Fatal("expected exactly-one-key error")
	}
}

func TestBudgets(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/budgets", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodDelete {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"key_id":"`+r.URL.Query().Get("key_id")+`","has_limit":true,"limit_usd":25,"spend_usd":1.25,"remaining_usd":23.75,"exceeded":false,"period":"monthly"}`)
	})

	out, _, err := g.run(t, "", "budgets", "get", "--key-id", testKeyID)
	if err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodGet || req.Query != "key_id="+testKeyID || req.Auth != "Bearer sk-test-master-key" {
		t.Fatalf("get = %s ?%s", req.Method, req.Query)
	}
	if !regexp.MustCompile(`limit_usd\s+25`).MatchString(out) {
		t.Fatalf("table = %q", out)
	}

	// A raw key is converted to its key ID locally and never sent or printed.
	wantID := middleware.KeyID(rawTestKey)
	out, stderr, err := g.run(t, rawTestKey+"\n", "budgets", "get", "--key", "-", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Query != "key_id="+wantID {
		t.Fatalf("query = %q, want key_id=%s", req.Query, wantID)
	}
	if !strings.Contains(stderr, wantID) || !strings.Contains(stderr, "sk-raw...cdef") {
		t.Fatalf("stderr = %q", stderr)
	}
	noLeak(t, "budgets get --key", []string{rawTestKey}, out, stderr, g.last(t).Query, g.last(t).Body)

	_, stderr, err = g.run(t, "", "budgets", "set", "--key", rawTestKey, "--max-usd", "25", "--period", "Monthly")
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	body := decodeBody(t, req.Body)
	if req.Method != http.MethodPut || body["key_id"] != wantID || body["max_usd"] != 25.0 || body["period"] != "monthly" {
		t.Fatalf("set = %s %s", req.Method, req.Body)
	}
	if _, ok := body["key"]; ok {
		t.Fatal("raw key sent to the server")
	}
	noLeak(t, "budgets set", []string{rawTestKey}, stderr, req.Body, req.Query)

	out, _, err = g.run(t, "", "budgets", "delete", "--key-id", testKeyID)
	if err != nil || strings.TrimSpace(out) != "removed budget for "+testKeyID {
		t.Fatalf("delete: %q %v", out, err)
	}
	if req := g.last(t); req.Method != http.MethodDelete || req.Query != "key_id="+testKeyID {
		t.Fatalf("delete = %s ?%s", req.Method, req.Query)
	}
	out, _, err = g.run(t, "", "budgets", "delete", "--key-id", testKeyID, "-o", "json")
	if err != nil || !strings.Contains(out, `"deleted": true`) {
		t.Fatalf("delete json: %q %v", out, err)
	}

	n := len(g.all())
	for _, bad := range [][]string{
		{"budgets", "get"},
		{"budgets", "get", "--key-id", "key_nothex"},
		{"budgets", "get", "--key-id", testKeyID, "--key", rawTestKey},
		{"budgets", "set", "--key-id", testKeyID},
		{"budgets", "set", "--key-id", testKeyID, "--max-usd", "-1"},
		{"budgets", "set", "--key-id", testKeyID, "--max-usd", "NaN"},
		{"budgets", "set", "--key-id", testKeyID, "--max-usd", "1", "--period", "weekly"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
	if len(g.all()) != n {
		t.Fatal("invalid budget commands must not be sent")
	}
}

func TestRAGDocuments(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/rag/documents", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodGet:
			_, _ = io.WriteString(w, `{"count":3}`)
		case http.MethodDelete:
			if r.URL.Query().Get("id") == "missing" {
				http.Error(w, `{"error":{"message":"document not found","code":404}}`, http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, `{"deleted":true}`)
		default:
			_, _ = io.WriteString(w, `{"indexed":1,"ids":["d1"]}`)
		}
	})

	out, _, err := g.run(t, "", "rag", "add", "--text", "Refunds take 5 days.", "--id", "d1", "--source", "faq.md", "--metadata", `{"lang":"en"}`)
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	docs := decodeBody(t, req.Body)["documents"].([]any)
	d := docs[0].(map[string]any)
	if req.Method != http.MethodPost || len(docs) != 1 || d["id"] != "d1" || d["content"] != "Refunds take 5 days." ||
		d["source"] != "faq.md" || d["metadata"].(map[string]any)["lang"] != "en" {
		t.Fatalf("add body = %s", req.Body)
	}
	if !regexp.MustCompile(`indexed\s+1`).MatchString(out) {
		t.Fatalf("add out = %q", out)
	}

	dir := t.TempDir()
	arr := filepath.Join(dir, "docs.json")
	_ = os.WriteFile(arr, []byte(`[{"content":"a"},{"id":"x","content":"b","source":"s"}]`), 0o600)
	if _, _, err := g.run(t, "", "rag", "add", "--file", arr); err != nil {
		t.Fatal(err)
	}
	if docs := decodeBody(t, g.last(t).Body)["documents"].([]any); len(docs) != 2 {
		t.Fatalf("file docs = %v", docs)
	}
	if _, _, err := g.run(t, `{"documents":[{"content":"from stdin"}]}`, "rag", "add", "--file", "-"); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(g.last(t).Body, "from stdin") {
		t.Fatalf("stdin body = %s", g.last(t).Body)
	}

	n := len(g.all())
	typo := filepath.Join(dir, "typo.json")
	_ = os.WriteFile(typo, []byte(`[{"text":"oops"}]`), 0o600)
	for _, bad := range [][]string{
		{"rag", "add"},
		{"rag", "add", "--text", "   "},
		{"rag", "add", "--text", "a", "--file", arr},
		{"rag", "add", "--file", arr, "--id", "x"},
		{"rag", "add", "--file", typo},
		{"rag", "add", "--text", "a", "--metadata", "nope"},
		{"rag", "add", "--text", "a", "--id", strings.Repeat("x", 300)},
		{"rag", "delete"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
	if len(g.all()) != n {
		t.Fatal("invalid rag commands must not be sent")
	}

	out, _, err = g.run(t, "", "rag", "delete", "--id", "d 1")
	if err != nil || strings.TrimSpace(out) != "deleted document d 1" {
		t.Fatalf("delete: %q %v", out, err)
	}
	if req := g.last(t); req.Method != http.MethodDelete || req.Query != "id=d+1" {
		t.Fatalf("delete = %s ?%s", req.Method, req.Query)
	}
	if _, _, err := g.run(t, "", "rag", "delete", "--id", "missing"); err == nil || !strings.Contains(err.Error(), "document not found") {
		t.Fatalf("expected 404, got %v", err)
	}
	out, _, err = g.run(t, "", "rag", "count")
	if err != nil || strings.TrimSpace(out) != "3" || g.last(t).Method != http.MethodGet {
		t.Fatalf("count: %q %v", out, err)
	}
	out, _, err = g.run(t, "", "rag", "count", "-o", "json")
	if err != nil || !strings.Contains(out, `"count": 3`) {
		t.Fatalf("count json: %q %v", out, err)
	}
}

func TestRSIStatsAndRollback(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/rsi/stats", 200, `{"cycles":4,"deployed":1}`)
	g.json("/v1/rsi/rollback", 200, `{"status":"rolled_back"}`)
	out, _, err := g.run(t, "", "rsi", "stats")
	if err != nil || !strings.Contains(out, `"cycles": 4`) || g.last(t).Method != http.MethodGet {
		t.Fatalf("stats: %q %v", out, err)
	}
	out, _, err = g.run(t, "", "rsi", "rollback")
	if err != nil || !strings.Contains(out, "rolled_back") || g.last(t).Method != http.MethodPost {
		t.Fatalf("rollback: %q %v", out, err)
	}
	g.json("/v1/rsi/rollback", http.StatusConflict, `{"error":{"message":"no deployment to roll back"}}`)
	if _, _, err := g.run(t, "", "rsi", "rollback"); err == nil || !strings.Contains(err.Error(), "no deployment to roll back") {
		t.Fatalf("expected 409, got %v", err)
	}
}

func TestNotifySend(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/notification/send", 200, `{"status":"sent"}`)
	out, _, err := g.run(t, "", "notify", "send", "--channel-id", "oncall", "--title", "Deploy", "--text", "v2 is live",
		"--severity", "WARNING", "--label", "team=search")
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	b := decodeBody(t, req.Body)
	if req.Method != http.MethodPost || b["channel_id"] != "oncall" || b["title"] != "Deploy" || b["text"] != "v2 is live" ||
		b["severity"] != "warning" || b["labels"].(map[string]any)["team"] != "search" {
		t.Fatalf("body = %s", req.Body)
	}
	if _, ok := b["alert_id"]; ok {
		t.Fatal("alert_id must be omitted when --channel-id is used")
	}
	if strings.TrimSpace(out) != "notification sent to channel oncall" {
		t.Fatalf("out = %q", out)
	}
	if _, _, err := g.run(t, "from stdin\n", "notify", "send", "--alert-id", "budget-80", "--title", "Budget", "--text", "-"); err != nil {
		t.Fatal(err)
	}
	if b := decodeBody(t, g.last(t).Body); b["alert_id"] != "budget-80" || b["text"] != "from stdin\n" {
		t.Fatalf("alert body = %s", g.last(t).Body)
	}
	n := len(g.all())
	for _, bad := range [][]string{
		{"notify", "send", "--title", "t", "--text", "x"},
		{"notify", "send", "--channel-id", "c", "--alert-id", "a", "--title", "t", "--text", "x"},
		{"notify", "send", "--channel-id", "c", "--text", "x"},
		{"notify", "send", "--channel-id", "c", "--title", "t"},
		{"notify", "send", "--channel-id", "c", "--title", "t", "--text", "x", "--severity", "!!"},
		{"notify", "send", "--channel-id", "c", "--title", "t", "--text", "x", "--label", "novalue"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
	if len(g.all()) != n {
		t.Fatal("invalid notifications must not be sent")
	}
}

func TestShadowResults(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/shadow/results", 200, `[{"provider":"https://shadow.example","latency":1500000,"status_code":200,"timestamp":"2026-01-01T00:00:00Z"}]`)
	out, _, err := g.run(t, "", "shadow", "results")
	if err != nil || !regexp.MustCompile(`https://shadow.example\s+200\s+1.5ms`).MatchString(out) {
		t.Fatalf("results: %q %v", out, err)
	}
	g.json("/v1/shadow/results", 200, `{"results":[{"provider":"p","latency":2000000000,"error":"timeout"}],"count":1}`)
	out, stderr, err := g.run(t, "", "shadow", "results")
	if err != nil || !strings.Contains(out, "2s") || !strings.Contains(out, "timeout") || !strings.Contains(stderr, "count=1") {
		t.Fatalf("enveloped results: out=%q stderr=%q err=%v", out, stderr, err)
	}
	out, _, err = g.run(t, "", "shadow", "results", "-o", "json")
	if err != nil || !strings.Contains(out, `"latency": 2000000000`) {
		t.Fatalf("json: %q %v", out, err)
	}
}

func TestCacheCommands(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/cache/stats", 200, `{"exact":{"total_entries":4,"hits":10,"misses":2,"hit_rate":0.83},"semantic":{"enabled":true,"total_entries":1,"active_entries":1}}`)
	out, _, err := g.run(t, "", "cache", "stats")
	if err != nil || !regexp.MustCompile(`exact\.hits\s+10`).MatchString(out) || !regexp.MustCompile(`semantic\.enabled\s+true`).MatchString(out) {
		t.Fatalf("stats: %q %v", out, err)
	}
	g.json("/v1/cache", 200, `{"semantic_cleared":true}`)
	out, _, err = g.run(t, "", "cache", "clear", "--type", "semantic")
	if err != nil || strings.TrimSpace(out) != "cleared: semantic" {
		t.Fatalf("clear: %q %v", out, err)
	}
	if req := g.last(t); req.Method != http.MethodDelete || req.Query != "type=semantic" {
		t.Fatalf("clear = %s ?%s", req.Method, req.Query)
	}
	if _, _, err := g.run(t, "", "cache", "clear", "--type", "everything"); err == nil {
		t.Fatal("expected --type validation error")
	}
	g.json("/v1/cache", http.StatusInternalServerError, `{"exact_error":"clear failed"}`)
	if _, _, err := g.run(t, "", "cache", "clear"); err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("expected 500 error, got %v", err)
	}

	g.handle("/v1/cache/inspect", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("cursor") == "7" {
			_, _ = io.WriteString(w, `{"entries":[{"key":"k2","model":"m","semantic":true,"token_count":5,"created_at":"2026-01-01T00:00:00Z"}],"cursor":"0"}`)
			return
		}
		_, _ = io.WriteString(w, `{"entries":[{"key":"k1","model":"m","semantic":true,"token_count":3,"created_at":"2026-01-01T00:00:00Z"}],"cursor":"7"}`)
	})
	out, stderr, err := g.run(t, "", "cache", "inspect", "--type", "semantic", "--cursor", "5", "--page-size", "10")
	if err != nil || !strings.Contains(out, "k1") || !strings.Contains(stderr, "cursor=7") {
		t.Fatalf("inspect: out=%q stderr=%q err=%v", out, stderr, err)
	}
	if q := g.last(t).Query; q != "cursor=5&page_size=10&type=semantic" {
		t.Fatalf("inspect query = %q", q)
	}
	before := len(g.all())
	out, _, err = g.run(t, "", "cache", "inspect", "--all", "-o", "json")
	if err != nil || !strings.Contains(out, "k1") || !strings.Contains(out, "k2") || len(g.all())-before != 2 {
		t.Fatalf("inspect --all: %q %v", out, err)
	}
	if _, _, err := g.run(t, "", "cache", "inspect", "--page-size", "0"); err == nil {
		t.Fatal("expected --page-size validation error")
	}
}

func TestSpendReportAndLogs(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/global/spend/report", 200, `{"total_cost_usd":3.5,"total_requests":7,"total_tokens":{"input":10,"output":20,"total":30},
		"by_model":{"gpt-4o":{"cost_usd":3,"requests":5},"mini":{"cost_usd":0.5,"requests":2}},
		"by_api_key":{"`+rawTestKey+`":{"cost_usd":1,"requests":1}},
		"time_range":{"start":"2026-09-01T00:00:00Z","end":"2026-09-02T00:00:00Z"}}`)
	out, _, err := g.run(t, "", "spend", "report", "--start", "2026-09-01", "--end", "2026-09-02T00:00:00Z", "--group-by", "all")
	if err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodGet || req.Query != "end=2026-09-02T00%3A00%3A00Z&group_by=all&start=2026-09-01T00%3A00%3A00Z" {
		t.Fatalf("report query = %q", req.Query)
	}
	if !regexp.MustCompile(`total_cost_usd\s+3.5`).MatchString(out) || !regexp.MustCompile(`(?s)model\s+gpt-4o\s+3\s+5.*model\s+mini\s+0.5`).MatchString(out) {
		t.Fatalf("report table = %q", out)
	}
	jsonOut, _, err := g.run(t, "", "spend", "report", "--since", "7d", "-o", "json")
	if err != nil || !strings.Contains(jsonOut, `"by_model"`) || !strings.Contains(g.last(t).Query, "start=") {
		t.Fatalf("report json: %q %v", jsonOut, err)
	}
	noLeak(t, "spend report", []string{rawTestKey}, out, jsonOut)
	for _, bad := range [][]string{
		{"spend", "report", "--start", "yesterday"},
		{"spend", "report", "--start", "2026-09-02", "--end", "2026-09-01"},
		{"spend", "report", "--since", "7d", "--start", "2026-09-01"},
		{"spend", "report", "--group-by", "planet"},
		{"spend", "report", "--since", "-3d"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}

	g.json("/global/spend/logs", 200, `{"logs":[{"request_id":"r1","api_key":"`+rawTestKey+`","model":"gpt-4o","cost_usd":0.25,"timestamp":"2026-09-01T10:00:00Z"},
		{"request_id":"r2","api_key":"`+testKeyID+`","model":"mini","cost_usd":0.01}],"total":2,"page":1,"page_size":50,"has_more":false}`)
	out, stderr, err := g.run(t, "", "spend", "logs", "--filter", "team-search", "--page", "2", "--page-size", "100")
	if err != nil {
		t.Fatal(err)
	}
	if q := g.last(t).Query; q != "filter=team-search&page=2&page_size=100" {
		t.Fatalf("logs query = %q", q)
	}
	if !strings.Contains(out, "r1") || !strings.Contains(out, testKeyID) || !strings.Contains(stderr, "total=2") {
		t.Fatalf("logs: out=%q stderr=%q", out, stderr)
	}
	jsonOut, _, err = g.run(t, rawTestKey+"\n", "spend", "logs", "--key", "-", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	if q := g.last(t).Query; q != "filter="+middleware.KeyID(rawTestKey) {
		t.Fatalf("logs --key query = %q", q)
	}
	noLeak(t, "spend logs", []string{rawTestKey}, out, stderr, jsonOut, g.last(t).Query)
	n := len(g.all())
	for _, bad := range [][]string{
		{"spend", "logs", "--filter", rawTestKey},
		{"spend", "logs", "--page-size", "600"},
		{"spend", "logs", "--page", "0"},
		{"spend", "logs", "--filter", "a", "--key", "b"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
	if len(g.all()) != n {
		t.Fatal("invalid spend log commands must not be sent")
	}
}

func TestConfigCommands(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/config/yaml", 200, `{"server":{"port":8080},"providers":[{"name":"openai","api_key":"sk-leaked-provider-secret","base_url":"https://api.openai.com"}],
		"redis":{"url":"redis://user:hunter2-redis-pass@cache:6379/0","password":"***REDACTED***"},"security":{"admin_keys":["sk-admin-leak-1234567"]},"env_ref":{"api_key":"${OPENAI_API_KEY}"}}`)
	out, _, err := g.run(t, "", "config", "get")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, `"port": 8080`) || !strings.Contains(out, "${OPENAI_API_KEY}") || !strings.Contains(out, "https://api.openai.com") {
		t.Fatalf("config = %q", out)
	}
	tableOut, _, err := g.run(t, "", "config", "get", "-o", "table")
	if err != nil || !regexp.MustCompile(`server\.port\s+8080`).MatchString(tableOut) {
		t.Fatalf("config table: %q %v", tableOut, err)
	}
	noLeak(t, "config get", []string{"sk-leaked-provider-secret", "hunter2-redis-pass", "sk-admin-leak-1234567"}, out, tableOut)

	g.json("/config/update", 200, `{"status":"reloaded","providers":2}`)
	patch := filepath.Join(t.TempDir(), "patch.json")
	_ = os.WriteFile(patch, []byte(`{"rate_limit":{"default_tpm":1000}}`), 0o600)
	out, _, err = g.run(t, "", "config", "update", "--file", patch)
	if err != nil || strings.TrimSpace(out) != "configuration reloaded (2 provider(s))" {
		t.Fatalf("update: %q %v", out, err)
	}
	if req := g.last(t); req.Method != http.MethodPost || req.Body != `{"rate_limit":{"default_tpm":1000}}` ||
		req.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("update = %s %s", req.Method, req.Body)
	}
	if _, _, err := g.run(t, `{"logging":{"level":"debug"}}`, "config", "update"); err != nil || g.last(t).Body != `{"logging":{"level":"debug"}}` {
		t.Fatalf("stdin update: %v %s", err, g.last(t).Body)
	}
	n := len(g.all())
	for _, stdin := range []string{"", "{}", "[1]", "null", "{broken"} {
		if _, _, err := g.run(t, stdin, "config", "update"); err == nil {
			t.Errorf("stdin %q: expected error", stdin)
		}
	}
	if len(g.all()) != n {
		t.Fatal("invalid config must not be sent")
	}
	g.json("/config/update", http.StatusBadRequest, `{"error":{"message":"invalid provider type"}}`)
	if _, _, err := g.run(t, `{"providers":[]}`, "config", "update"); err == nil || !strings.Contains(err.Error(), "invalid provider type") {
		t.Fatalf("expected 400, got %v", err)
	}

	g.json("/model/info", 200, `{"data":[{"model":"gpt-4o","provider":"openai","provider_type":"openai","capabilities":["chat","vision"],"context_window":128000}],"count":1}`)
	for _, args := range [][]string{{"models", "info"}, {"config", "models"}} {
		out, stderr, err := g.run(t, "", args...)
		if err != nil || !regexp.MustCompile(`gpt-4o\s+openai\s+openai\s+chat,vision\s+128000`).MatchString(out) || !strings.Contains(stderr, "count=1") {
			t.Fatalf("%v: out=%q stderr=%q err=%v", args, out, stderr, err)
		}
		if req := g.last(t); req.Method != http.MethodGet || req.Path != "/model/info" {
			t.Fatalf("%v hit %s %s", args, req.Method, req.Path)
		}
	}
}

func TestOpenStandardTargetsGateway(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/marketplace/openstandard/capability", http.StatusAccepted, `{"version":"1.0"}`)
	g.json("/v1/marketplace/openstandard/receipt", http.StatusCreated, `{"receipt_id":"r1"}`)
	if _, _, err := g.run(t, "", "openstandard", "capability", "--capabilities", "mesh"); err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	if req.Method != http.MethodPost || req.Path != "/v1/marketplace/openstandard/capability" || req.Auth != "Bearer sk-test-master-key" {
		t.Fatalf("capability = %s %s auth=%q", req.Method, req.Path, req.Auth)
	}
	out, _, err := g.run(t, "", "openstandard", "receipt", "--customer", "acme", "--event", "token", "--value", "3")
	if err != nil || !strings.Contains(out, "r1") {
		t.Fatalf("receipt: %q %v", out, err)
	}
	req = g.last(t)
	b := decodeBody(t, req.Body)
	if req.Path != "/v1/marketplace/openstandard/receipt" || b["customer_id"] != "acme" || b["value"] != 3.0 || b["provider_id"] != "server" {
		t.Fatalf("receipt = %s %s", req.Path, req.Body)
	}
}

// TestNewCommandsMethodPathAuthAndErrors checks, for every command added for
// the gateway's current API surface, the HTTP method, path and auth header it
// sends, that a non-2xx response exits 1 with the server's status on stderr,
// and that the API key is never echoed.
func TestNewCommandsMethodPathAuthAndErrors(t *testing.T) {
	isolateEnv(t)
	cases := []struct {
		args         []string
		stdin        string
		method, path string
	}{
		{[]string{"keys", "list"}, "", http.MethodGet, "/key/list"},
		{[]string{"keys", "update", "--key-hash", testKeyHash, "--tpm", "10"}, "", http.MethodPost, "/key/update"},
		{[]string{"keys", "block", "--key-hash", testKeyHash}, "", http.MethodPost, "/key/block"},
		{[]string{"keys", "unblock", "--key-hash", testKeyHash}, "", http.MethodPost, "/key/unblock"},
		{[]string{"keys", "regenerate", "--key-hash", testKeyHash}, "", http.MethodPost, "/key/regenerate"},
		{[]string{"batches", "create"}, batchJSONL, http.MethodPost, "/v1/batches"},
		{[]string{"batches", "list"}, "", http.MethodGet, "/v1/batches"},
		{[]string{"batches", "get", "batch_1"}, "", http.MethodGet, "/v1/batches/batch_1"},
		{[]string{"batches", "cancel", "batch_1"}, "", http.MethodPost, "/v1/batches/batch_1/cancel"},
		{[]string{"batches", "results", "batch_1"}, "", http.MethodGet, "/v1/batches/batch_1/results"},
		{[]string{"batches", "errors", "batch_1"}, "", http.MethodGet, "/v1/batches/batch_1/errors"},
		{[]string{"batches", "wait", "batch_1", "--interval", "1ms", "--max-wait", "30ms"}, "", http.MethodGet, "/v1/batches/batch_1"},
		{[]string{"budgets", "get", "--key-id", testKeyID}, "", http.MethodGet, "/v1/budgets"},
		{[]string{"budgets", "set", "--key-id", testKeyID, "--max-usd", "5"}, "", http.MethodPut, "/v1/budgets"},
		{[]string{"budgets", "delete", "--key-id", testKeyID}, "", http.MethodDelete, "/v1/budgets"},
		{[]string{"rag", "add", "--text", "hello"}, "", http.MethodPost, "/v1/rag/documents"},
		{[]string{"rag", "delete", "--id", "d1"}, "", http.MethodDelete, "/v1/rag/documents"},
		{[]string{"rag", "count"}, "", http.MethodGet, "/v1/rag/documents"},
		{[]string{"rsi", "stats"}, "", http.MethodGet, "/v1/rsi/stats"},
		{[]string{"rsi", "rollback"}, "", http.MethodPost, "/v1/rsi/rollback"},
		{[]string{"notify", "send", "--channel-id", "c1", "--title", "t", "--text", "x"}, "", http.MethodPost, "/v1/notification/send"},
		{[]string{"shadow", "results"}, "", http.MethodGet, "/v1/shadow/results"},
		{[]string{"cache", "stats"}, "", http.MethodGet, "/v1/cache/stats"},
		{[]string{"cache", "clear"}, "", http.MethodDelete, "/v1/cache"},
		{[]string{"cache", "inspect"}, "", http.MethodGet, "/v1/cache/inspect"},
		{[]string{"spend", "report"}, "", http.MethodGet, "/global/spend/report"},
		{[]string{"spend", "logs"}, "", http.MethodGet, "/global/spend/logs"},
		{[]string{"config", "get"}, "", http.MethodGet, "/config/yaml"},
		{[]string{"config", "update"}, `{"a":1}`, http.MethodPost, "/config/update"},
		{[]string{"models", "info"}, "", http.MethodGet, "/model/info"},
		{[]string{"openstandard", "capability"}, "", http.MethodPost, "/v1/marketplace/openstandard/capability"},
		{[]string{"openstandard", "receipt"}, "", http.MethodPost, "/v1/marketplace/openstandard/receipt"},
	}
	const apiKey = "sk-test-master-key"
	for _, c := range cases {
		name := strings.Join(c.args[:2], " ")
		g := newFakeGateway(t)
		g.json(c.path, http.StatusInternalServerError, `{"error":{"message":"boom for `+apiKey+`"}}`)
		var stdout, stderr bytes.Buffer
		code := run(context.Background(), append([]string{"--server", g.URL, "--api-key", apiKey}, c.args...),
			strings.NewReader(c.stdin), &stdout, &stderr)
		if code != 1 {
			t.Errorf("%s: exit code = %d, want 1 (stderr=%q)", name, code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "500") || !strings.Contains(stderr.String(), "boom") {
			t.Errorf("%s: stderr lacks server error: %q", name, stderr.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), apiKey) {
			t.Errorf("%s: API key echoed", name)
		}
		reqs := g.all()
		if len(reqs) == 0 {
			t.Errorf("%s: no request sent", name)
			continue
		}
		req := reqs[0]
		if req.Method != c.method || req.Path != c.path || req.Auth != "Bearer "+apiKey {
			t.Errorf("%s: sent %s %s auth=%q, want %s %s", name, req.Method, req.Path, req.Auth, c.method, c.path)
		}
	}
}
