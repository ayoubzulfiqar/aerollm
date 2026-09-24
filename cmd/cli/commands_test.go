package main

import (
	"encoding/json"
	"net/http"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
	"github.com/ayoubzulfiqar/aerollm/internal/retention"
)

func TestModelsList(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/models", 200, `{"object":"list","data":[
		{"id":"gpt-4o","object":"model","created":1700000000,"owned_by":"openai"},
		{"id":"claude-3-5-sonnet","object":"model","owned_by":"anthropic"}]}`)

	out, stderr, err := g.run(t, "", "models", "list")
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "ID") {
		t.Fatalf("unexpected table:\n%s", out)
	}
	if !strings.HasPrefix(lines[1], "claude-3-5-sonnet") || !strings.Contains(lines[2], "2023-11-14T22:13:20Z") {
		t.Fatalf("rows not sorted/formatted:\n%s", out)
	}
	if !strings.Contains(stderr, "2 model(s)") {
		t.Fatalf("stderr = %q", stderr)
	}
	out, _, err = g.run(t, "", "models", "list", "-o", "json")
	if err != nil || !strings.Contains(out, `"owned_by": "openai"`) {
		t.Fatalf("json: out=%q err=%v", out, err)
	}
}

func TestHealth(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/health", 200, `{"status":"ok"}`)
	g.json("/ready", 200, `{"ready":"true"}`)
	out, _, err := g.run(t, "", "health")
	if err != nil {
		t.Fatalf("healthy gateway reported error: %v\n%s", err, out)
	}
	if !strings.Contains(out, "/health") || !strings.Contains(out, "/ready") {
		t.Fatalf("output = %q", out)
	}

	// Not ready (explicit false) -> non-zero.
	g.json("/ready", 200, `{"ready":false}`)
	if _, _, err := g.run(t, "", "health"); err == nil {
		t.Fatal("expected error when not ready")
	}
	// 503 -> non-zero, JSON output still printed.
	g.json("/ready", http.StatusServiceUnavailable, `{"error":"redis down"}`)
	out, _, err = g.run(t, "", "health", "-o", "json")
	if err == nil {
		t.Fatal("expected error on 503")
	}
	var res struct {
		OK     bool          `json:"ok"`
		Checks []probeResult `json:"checks"`
	}
	if jerr := json.Unmarshal([]byte(out), &res); jerr != nil || res.OK || !strings.Contains(res.Checks[1].Error, "redis down") {
		t.Fatalf("json output = %q (%v)", out, jerr)
	}
	// Degraded status string -> non-zero.
	g.json("/ready", 200, `{"ready":true}`)
	g.json("/health", 200, `{"status":"degraded"}`)
	if _, _, err := g.run(t, "", "health"); err == nil {
		t.Fatal("expected error for degraded status")
	}
}

func TestKeysGenerate(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/key/generate", 200, `{"key_hash":"abc123","key":"sk-live-0123456789abcdef","expires":"","token":"sk-live-0123456789abcdef"}`)

	out, stderr, err := g.run(t, "", "keys", "generate", "--models", "gpt-4o,claude", "--duration", "30d",
		"--max-budget", "25", "--metadata", `{"env":"prod"}`, "--role", "team_admin", "--team-id", "t1")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(out, "sk-live-0123456789abcdef") != 1 {
		t.Fatalf("key must be printed exactly once:\n%s", out)
	}
	if !strings.Contains(out, "never") || !strings.Contains(stderr, "cannot be retrieved again") {
		t.Fatalf("out=%q stderr=%q", out, stderr)
	}
	body := decodeBody(t, g.last(t).Body)
	if body["duration"] != "30d" || body["max_budget"] != 25.0 || body["role"] != "team_admin" || body["team_id"] != "t1" {
		t.Fatalf("request body = %s", g.last(t).Body)
	}
	if models := body["models"].([]any); len(models) != 2 {
		t.Fatalf("models = %v", models)
	}
	if body["metadata"].(map[string]any)["env"] != "prod" {
		t.Fatalf("metadata = %v", body["metadata"])
	}

	for _, bad := range [][]string{
		{"keys", "generate", "--metadata", "not json"},
		{"keys", "generate", "--max-budget", "-1"},
		{"keys", "generate", "--role", "root"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected error", bad)
		}
	}
}

func TestKeysInfoNeverPutsKeyInURL(t *testing.T) {
	isolateEnv(t)
	const key = "sk-secretkey-abcdefghijklmnop"
	g := newFakeGateway(t)
	g.handle("/key/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"key_hash":"h","status":"active","models":["gpt-4o"],"token":"` + key + `"}`))
	})

	out, _, err := g.run(t, key+"\n", "keys", "info", "-")
	if err != nil {
		t.Fatal(err)
	}
	req := g.last(t)
	if req.Method != http.MethodPost || req.Query != "" || decodeBody(t, req.Body)["key"] != key {
		t.Fatalf("expected POST with key in body, got %s ?%s %s", req.Method, req.Query, req.Body)
	}
	if strings.Contains(out, key) {
		t.Fatalf("full key echoed in output:\n%s", out)
	}

	// Server only supports GET: fall back to ?key_hash=<sha256>, never ?key=.
	g.handle("/key/info", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"key_hash":"h","status":"active"}`))
	})
	if _, _, err := g.run(t, "", "keys", "info", key); err != nil {
		t.Fatal(err)
	}
	req = g.last(t)
	if req.Method != http.MethodGet || strings.Contains(req.Query, key) || !strings.Contains(req.Query, "key_hash="+keymanager.HashKey(key)) {
		t.Fatalf("fallback request leaked key or used wrong hash: %s ?%s", req.Method, req.Query)
	}
}

func TestKeysDelete(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/key/delete", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["key"] == "sk-missing-key-000000000" {
			http.Error(w, `{"error":"key not found"}`, http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"deleted"}`))
	})
	out, stderr, err := g.run(t, "", "keys", "delete", "sk-first-key-1111111111", "sk-missing-key-000000000", "--key-hash", "deadbeef")
	if err == nil || !strings.Contains(err.Error(), "1 of 3") {
		t.Fatalf("expected partial failure error, got %v", err)
	}
	if strings.Contains(out+stderr, "sk-first-key-1111111111") || strings.Contains(out+stderr, "sk-missing-key-000000000") {
		t.Fatalf("full keys echoed:\n%s\n%s", out, stderr)
	}
	if !strings.Contains(out, "deleted key sk-fir...1111") || !strings.Contains(out, "hash deadbeef") {
		t.Fatalf("stdout = %q", out)
	}
	reqs := g.all()
	if len(reqs) != 3 {
		t.Fatalf("expected one request per key, got %d", len(reqs))
	}
	first := decodeBody(t, reqs[1].Body) // hashes are sent first
	if first["key"] != "sk-first-key-1111111111" || len(first["keys"].([]any)) != 1 {
		t.Fatalf("delete body = %s", reqs[1].Body)
	}
	if _, _, err := g.run(t, "", "keys", "delete"); err == nil {
		t.Fatal("expected error with no keys")
	}
}

const promSampleText = `# HELP aerollm_requests_total Total requests.
# TYPE aerollm_requests_total counter
aerollm_requests_total{provider="openai",status="200"} 42
aerollm_requests_total{provider="anthropic",status="500"} 3
# HELP aerollm_latency_seconds Latency.
aerollm_latency_seconds_sum 12.5
go_goroutines 17
weird_metric{path="/a\"b",msg="x,y"} NaN
`

func TestMetrics(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(promSampleText))
	})
	out, _, err := g.run(t, "", "metrics")
	if err != nil || out != promSampleText {
		t.Fatalf("raw output mismatch: err=%v\n%s", err, out)
	}
	out, _, err = g.run(t, "", "metrics", "--grep", `provider="openai"|go_`)
	if err != nil {
		t.Fatal(err)
	}
	if out != "aerollm_requests_total{provider=\"openai\",status=\"200\"} 42\ngo_goroutines 17\n" {
		t.Fatalf("grep output = %q", out)
	}
	out, _, err = g.run(t, "", "metrics", "--grep", "aerollm_requests", "-o", "json")
	if err != nil {
		t.Fatal(err)
	}
	var samples []promSample
	if err := json.Unmarshal([]byte(out), &samples); err != nil || len(samples) != 2 || samples[1].Labels["status"] != "500" || samples[0].Value != 42 {
		t.Fatalf("json samples = %s (%v)", out, err)
	}
	if _, _, err := g.run(t, "", "metrics", "--grep", "("); err == nil {
		t.Fatal("expected invalid regexp error")
	}
}

func TestParsePromLabels(t *testing.T) {
	got, err := parsePromLabels(`path="/a\"b",msg="x,y",nl="a\nb"`)
	if err != nil {
		t.Fatal(err)
	}
	if got["path"] != `/a"b` || got["msg"] != "x,y" || got["nl"] != "a\nb" {
		t.Fatalf("labels = %#v", got)
	}
	if _, err := parsePromLabels(`a=unquoted`); err == nil {
		t.Fatal("expected error for unquoted value")
	}
	if _, err := parsePromLabels(`a="open`); err == nil {
		t.Fatal("expected error for unterminated value")
	}
	all, err := parsePromText(strings.NewReader(promSampleText), nil)
	if err != nil || len(all) != 4 { // NaN sample dropped
		t.Fatalf("parsePromText = %v, %v", all, err)
	}
}

func TestPolicyBodyIsProperlyEncoded(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/policy", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		b := make([]byte, 4096)
		n, _ := r.Body.Read(b)
		_, _ = w.Write(b[:n])
	})
	// Quotes in the expression used to break (or inject into) the JSON body.
	expr := `deny","severity":"low`
	if _, _, err := g.run(t, "", "policy", "--id", "r1", "--expr", expr, "--severity", "HIGH"); err != nil {
		t.Fatal(err)
	}
	body := decodeBody(t, g.last(t).Body)
	if body["expression"] != expr || body["severity"] != "high" || body["name"] != "r1" {
		t.Fatalf("body = %s", g.last(t).Body)
	}
	if _, _, err := g.run(t, "", "policy", "--id", "r1", "--expr", "deny", "--severity", "urgent"); err == nil {
		t.Fatal("expected severity validation error")
	}
	if _, _, err := g.run(t, "", "policy", "--id", "r1"); err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodGet || req.Query != "id=r1" {
		t.Fatalf("get by id = %s ?%s", req.Method, req.Query)
	}
}

func TestFlags(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/flags", 200, `{"key":"darkmode","enabled":true}`)
	g.json("/v1/flags/a b", 200, `{"key":"a b"}`)
	if _, _, err := g.run(t, "", "flags", "--set", `{"enabled":true,"strategy":"global"}`, "--key", "darkmode"); err != nil {
		t.Fatal(err)
	}
	body := decodeBody(t, g.last(t).Body)
	if body["key"] != "darkmode" || body["enabled"] != true {
		t.Fatalf("body = %s", g.last(t).Body)
	}
	if _, _, err := g.run(t, "", "flags", "--set", `{"key":"x"}`, "--key", "y"); err == nil {
		t.Fatal("expected key mismatch error")
	}
	if _, _, err := g.run(t, "", "flags", "--set", `{"key":"x","percentage":150}`); err == nil {
		t.Fatal("expected percentage validation error")
	}
	if _, _, err := g.run(t, "", "flags", "--key", "a b"); err != nil {
		t.Fatal(err)
	}
	if p := g.last(t).Path; p != "/v1/flags/a b" {
		t.Fatalf("path = %q", p)
	}
}

func TestSecretsMaskedAndReadFromStdin(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.handle("/v1/secrets", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method == http.MethodPost {
			b := make([]byte, 4096)
			n, _ := r.Body.Read(b)
			_, _ = w.Write(b[:n]) // server echoes the secret
			return
		}
		_, _ = w.Write([]byte(`[{"id":"sec_gh","name":"gh","type":"token","value":"ghp_supersecret"}]`))
	})
	out, stderr, err := g.run(t, "ghp_fromstdin\n", "secrets", "--name", "gh", "--value-stdin")
	if err != nil {
		t.Fatal(err)
	}
	if decodeBody(t, g.last(t).Body)["value"] != "ghp_fromstdin" {
		t.Fatalf("value not sent (trailing newline must be trimmed): %s", g.last(t).Body)
	}
	if strings.Contains(out+stderr, "ghp_fromstdin") {
		t.Fatalf("secret echoed: %s", out)
	}
	out, _, err = g.run(t, "", "secrets")
	if err != nil || strings.Contains(out, "ghp_supersecret") || !strings.Contains(out, maskedValue) {
		t.Fatalf("list not masked: %q (%v)", out, err)
	}
	out, _, err = g.run(t, "", "secrets", "-o", "json")
	if err != nil || strings.Contains(out, "ghp_supersecret") {
		t.Fatalf("json list not masked: %q", out)
	}
	out, _, err = g.run(t, "", "secrets", "--reveal", "-o", "json")
	if err != nil || !strings.Contains(out, "ghp_supersecret") {
		t.Fatalf("--reveal should show values: %q", out)
	}
	_, stderr, err = g.run(t, "", "secrets", "--name", "x", "--value", "v")
	if err != nil || !strings.Contains(stderr, "warning") {
		t.Fatalf("expected --value warning, stderr=%q err=%v", stderr, err)
	}
	if _, _, err := g.run(t, "v", "secrets", "--name", "x", "--value", "v", "--value-stdin"); err == nil {
		t.Fatal("expected error for multiple value sources")
	}
}

func TestRetentionTTLIsConvertedFromHours(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/retention", 200, `{}`)
	if _, _, err := g.run(t, "", "retention", "--id", "logs", "--resource", "request_logs", "--ttl", "24"); err != nil {
		t.Fatal(err)
	}
	// Decode with the server's own type: before the fix "--ttl 24" was sent
	// as 24ns (time.Duration JSON); the wire format is owned by the package.
	var p retention.RetentionPolicy
	if err := json.Unmarshal([]byte(g.last(t).Body), &p); err != nil {
		t.Fatal(err)
	}
	if p.TTL != 24*time.Hour || p.MaxItems != 1000 || p.Resource != "request_logs" {
		t.Fatalf("policy = %+v (body %s)", p, g.last(t).Body)
	}
	if _, _, err := g.run(t, "", "retention", "--id", "logs", "--resource", "r", "--ttl", "0"); err == nil {
		t.Fatal("expected ttl validation error")
	}
}

func TestRegionResources(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/region/regions", 200, `[]`)
	g.json("/v1/region/routes", 200, `[{"id":"r1","region":"eu","providers":["openai","anthropic"],"priority":1,"enabled":true}]`)
	g.json("/v1/region/residency", 200, `{}`)

	out, _, err := g.run(t, "", "region", "-r", "route")
	if err != nil || !strings.Contains(out, "openai,anthropic") || g.last(t).Path != "/v1/region/routes" {
		t.Fatalf("route list: %q %v", out, err)
	}
	if _, _, err := g.run(t, "", "region", "--name", "us-east-1", "--endpoint", "https://us.example.com", "--primary"); err != nil {
		t.Fatal(err)
	}
	body := decodeBody(t, g.last(t).Body)
	if body["id"] != "us-east-1" || body["primary"] != true {
		t.Fatalf("region body = %s", g.last(t).Body)
	}
	if _, _, err := g.run(t, "", "region", "-r", "residency", "--id", "eu-pii", "--region", "eu", "--data-type", "pii", "--required"); err != nil {
		t.Fatal(err)
	}
	if b := decodeBody(t, g.last(t).Body); b["data_type"] != "pii" || b["required"] != true {
		t.Fatalf("residency body = %v", b)
	}
	if _, _, err := g.run(t, "", "region", "--name", "x", "--endpoint", "not a url"); err == nil {
		t.Fatal("expected endpoint validation error")
	}
	if _, _, err := g.run(t, "", "region", "-r", "planet"); err == nil {
		t.Fatal("expected resource validation error")
	}
	g.handle("/v1/region/regions", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) })
	out, _, err = g.run(t, "", "region", "--id", "us-east-1", "--delete")
	if err != nil || g.last(t).Method != http.MethodDelete || !strings.Contains(out, "deleted region us-east-1") {
		t.Fatalf("delete: %q %v", out, err)
	}
}

func TestScheduleIncidentNotificationRequests(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/schedule", 201, `{"id":"task_1"}`)
	g.json("/v1/incidents", 201, `{"id":"inc_1"}`)
	g.json("/v1/notification/channels", 200, `{"id":"oncall"}`)

	if _, _, err := g.run(t, "", "schedule", "--name", `nightly "report"`, "--schedule", "0 3 * * *", "--payload", `{"a":1}`); err != nil {
		t.Fatal(err)
	}
	if b := decodeBody(t, g.last(t).Body); b["name"] != `nightly "report"` || b["type"] != "cron" || b["payload"] != `{"a":1}` {
		t.Fatalf("schedule body = %v", b)
	}
	if _, _, err := g.run(t, "", "schedule", "--name", "x", "--schedule", "1m", "--type", "hourly"); err == nil {
		t.Fatal("expected schedule type validation error")
	}
	if _, _, err := g.run(t, "", "schedule", "--id", "task_1", "--set-status", "completed"); err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodPut || req.Query != "id=task_1" || decodeBody(t, req.Body)["status"] != "completed" {
		t.Fatalf("schedule update = %s ?%s %s", req.Method, req.Query, req.Body)
	}

	if _, _, err := g.run(t, "", "incident", "--title", "5xx spike", "--severity", "high", "--desc", `say "hi"`); err != nil {
		t.Fatal(err)
	}
	if b := decodeBody(t, g.last(t).Body); b["severity"] != "high" || b["description"] != `say "hi"` || b["title"] != "5xx spike" {
		t.Fatalf("incident body = %v", b)
	}
	if _, _, err := g.run(t, "", "incident", "--title", "x", "--severity", "catastrophic"); err == nil {
		t.Fatal("expected severity validation error")
	}
	if _, _, err := g.run(t, "", "incident", "--id", "inc_1", "--resolve"); err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodPost || !strings.Contains(req.Query, "resolve=true") {
		t.Fatalf("resolve request = %s ?%s", req.Method, req.Query)
	}
	if _, _, err := g.run(t, "", "incident", "--id", "inc_1", "--status", "investigating"); err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodPatch || decodeBody(t, req.Body)["status"] != "investigating" {
		t.Fatalf("transition request = %s %s", req.Method, req.Body)
	}

	if _, _, err := g.run(t, "", "notification", "-r", "channel", "--name", "oncall", "--type", "slack", "--target", "https://hooks.slack.com/services/T/B/X"); err != nil {
		t.Fatal(err)
	}
	if b := decodeBody(t, g.last(t).Body); b["type"] != "slack" || b["id"] != "oncall" || b["enabled"] != true {
		t.Fatalf("channel body = %v", b)
	}
	if _, _, err := g.run(t, "", "notification", "-r", "channel", "--name", "x", "--type", "webhook", "--target", "javascript:alert(1)"); err == nil {
		t.Fatal("expected webhook target validation error")
	}
}

func TestEvalUsesServer(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/eval/judge", 200, `{"score":85}`)
	g.json("/v1/eval/benchmark", 200, `{"Total":2}`)
	out, _, err := g.run(t, "", "eval", "--kind", "judge", "--prompt", "hi", "--response", "hello", "--model", "m1")
	if err != nil || !strings.Contains(out, "85") {
		t.Fatalf("judge: %q %v", out, err)
	}
	if b := decodeBody(t, g.last(t).Body); b["prompt"] != "hi" || b["response"] != "hello" {
		t.Fatalf("judge body = %v", b)
	}
	if _, _, err := g.run(t, "", "eval", "--kind", "judge"); err == nil {
		t.Fatal("expected missing prompt error")
	}
	if _, _, err := g.run(t, "", "eval", "--kind", "vibes"); err == nil {
		t.Fatal("expected kind validation error")
	}
	if _, _, err := g.run(t, "{\"prompt\":\"a\"}\n", "eval", "--kind", "benchmark", "--dataset", "-"); err != nil {
		t.Fatal(err)
	}
	if b := decodeBody(t, g.last(t).Body); b["dataset"] != "{\"prompt\":\"a\"}\n" {
		t.Fatalf("benchmark body = %v", b)
	}
}

func TestRSICommands(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/rsi/cycles", 200, `[{"id":1,"dimension":"latency","improvement_pct":12.5,"deployed":true,"timestamp":"2026-01-01T00:00:00Z"}]`)
	g.json("/v1/rsi/cycle", http.StatusPartialContent, `{"id":2}`)
	g.json("/v1/rsi/config", 200, `{"status":"ok"}`)

	out, _, err := g.run(t, "", "rsi", "cycles")
	if err != nil || !regexp.MustCompile(`1\s+latency\s+12.5\s+true`).MatchString(out) {
		t.Fatalf("cycles table: %q %v", out, err)
	}
	_, stderr, err := g.run(t, "", "rsi", "trigger", "--wait", "5s")
	if err != nil || !strings.Contains(stderr, "partial") {
		t.Fatalf("trigger: stderr=%q err=%v", stderr, err)
	}
	if _, _, err := g.run(t, "", "rsi", "config", "set", "--json", "{broken"); err == nil {
		t.Fatal("expected invalid JSON error")
	}
	if req := g.last(t); req.Path == "/v1/rsi/config" {
		t.Fatal("invalid JSON must not be sent")
	}
	if _, _, err := g.run(t, "", "rsi", "config", "set", "--json", `{"enabled":true}`); err != nil {
		t.Fatal(err)
	}
	if req := g.last(t); req.Method != http.MethodPut || req.Body != `{"enabled":true}` {
		t.Fatalf("config set = %s %s", req.Method, req.Body)
	}
}

func TestServerStubCommandsNowCallServer(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/admission/validate", 200, `{"allowed":true}`)
	g.json("/v1/audit/events", 200, `[{"Policy":"a","Decision":"allow"},{"Policy":"b","Decision":"deny"}]`)
	g.json("/backpressure/status", 200, `{"inflight":3}`)
	g.json("/v1/chaos/fault", 202, `{"type":"latency"}`)
	g.json("/v1/meter/usage", 200, `[]`)
	g.json("/v1/quota", 200, `{"ID":"q1"}`)
	g.json("/v1/slo/budget", 200, `{"target":"availability"}`)
	g.json("/v1/trace/metrics", 200, `{"service":"aerollm"}`)
	g.json("/v1/shadow", 200, `{"shadow":"accepted"}`)

	cases := []struct {
		args []string
		path string
		want string
	}{
		{[]string{"admission", "validate", "--method", "get"}, "/v1/admission/validate", "allowed"},
		{[]string{"audit", "events", "--limit", "1"}, "/v1/audit/events", "allow"},
		{[]string{"backpressure"}, "/backpressure/status", "inflight"},
		{[]string{"chaos", "fault", "--type", "latency", "--percent", "10", "--duration", "250ms"}, "/v1/chaos/fault", "latency"},
		{[]string{"meter", "usage", "--provider", "openai", "--model", "gpt-4o", "--tokens-in", "5"}, "/v1/meter/usage", ""},
		{[]string{"quota", "--id", "q1", "--target", "t1", "--scope", "team"}, "/v1/quota", "q1"},
		{[]string{"slo", "budget", "--target", "availability"}, "/v1/slo/budget", "availability"},
		{[]string{"trace", "metrics"}, "/v1/trace/metrics", "aerollm"},
		{[]string{"traffic", "shadow", "--model", "m", "--prompt", "p"}, "/v1/shadow", "accepted"},
	}
	for _, c := range cases {
		out, _, err := g.run(t, "", c.args...)
		if err != nil {
			t.Errorf("%v: %v", c.args, err)
			continue
		}
		if p := g.last(t).Path; p != c.path {
			t.Errorf("%v: hit %s, want %s", c.args, p, c.path)
		}
		if !strings.Contains(out, c.want) {
			t.Errorf("%v: output %q missing %q", c.args, out, c.want)
		}
	}
	// audit --limit truncates client side.
	out, _, _ := g.run(t, "", "audit", "events", "--limit", "1", "-o", "json")
	if strings.Contains(out, `"deny"`) {
		t.Errorf("audit limit not applied: %s", out)
	}
	// Quota fields must use the struct's field names (TargetID was dropped before).
	g.run(t, "", "quota", "--id", "q1", "--target", "t1")
	if b := decodeBody(t, g.find(t, "/v1/quota").Body); lookupField(b, "target_id") != "t1" {
		t.Errorf("quota body = %v", b)
	}
	if h := g.find(t, "/v1/slo/budget").Header.Get("x-slo-target"); h != "availability" {
		t.Errorf("x-slo-target = %q", h)
	}
	for _, bad := range [][]string{
		{"chaos", "fault", "--percent", "150"},
		{"chaos", "fault", "--type", "meteor"},
		{"quota", "--id", "q", "--target", "t", "--scope", "galaxy"},
		{"meter", "usage", "--model", "m"},
		{"admission", "validate", "--method", "TRACE"},
	} {
		if _, _, err := g.run(t, "", bad...); err == nil {
			t.Errorf("%v: expected validation error", bad)
		}
	}
}

// find returns the most recent request to path.
func (g *fakeGateway) find(t *testing.T, path string) recordedRequest {
	t.Helper()
	reqs := g.all()
	for i := len(reqs) - 1; i >= 0; i-- {
		if reqs[i].Path == path {
			return reqs[i]
		}
	}
	t.Fatalf("no request to %s", path)
	return recordedRequest{}
}

func TestEdgeCommands(t *testing.T) {
	isolateEnv(t)
	g := newFakeGateway(t)
	g.json("/v1/edge/capabilities", 200, `{"peer_id":"p1"}`)
	g.json("/v1/marketplace/openstandard/capability/self", 200, `{"version":"1.0"}`)
	g.json("/v1/marketplace/openstandard/receipt", 200, `{"event_name":"token"}`)
	g.json("/v1/edge/pqc/handshake", 200, `{"algorithm":"hybrid-ed25519+mldsa-65","public_key":"AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="}`)

	// EDGE_LISTEN is a listen address such as ":7910" or "0.0.0.0:7910".
	port := g.URL[strings.LastIndex(g.URL, ":")+1:]
	t.Setenv("EDGE_LISTEN", "0.0.0.0:"+port)
	t.Setenv("AEROLLM_API_KEY", "sk-gateway-secret")
	out, _, err := runCLI(t, "", "edge", "status")
	if err != nil || !strings.Contains(out, "p1") {
		t.Fatalf("edge status: %q %v", out, err)
	}
	if auth := g.last(t).Auth; auth != "" {
		t.Fatalf("gateway API key sent to edge node: %q", auth)
	}
	out, _, err = runCLI(t, "", "edge", "capability")
	if err != nil || !strings.Contains(out, `"version": "1.0"`) {
		t.Fatalf("edge capability: %q %v", out, err)
	}
	if _, _, err := runCLI(t, "", "edge", "receipt", "--customer", "acme", "--event", "token", "--value", "2"); err != nil {
		t.Fatal(err)
	}
	b := decodeBody(t, g.last(t).Body)
	if b["customer_id"] != "acme" || !strings.HasPrefix(b["receipt_id"].(string), "cli-") || b["value"] != 2.0 {
		t.Fatalf("receipt body = %v", b)
	}
	if _, _, err := runCLI(t, "", "edge", "receipt", "--currency", "dollars"); err == nil {
		t.Fatal("expected currency validation error")
	}
	out, _, err = runCLI(t, "", "edge", "pqc", "handshake")
	if err != nil || !strings.Contains(out, "public_key_len=32") {
		t.Fatalf("handshake: %q %v", out, err)
	}
	out, _, err = runCLI(t, "", "edge", "--edge-url", g.URL, "federated", "aggregate", "--input",
		`[{"Rows":1,"Cols":2,"Data":[1,2],"Owner":"e1"},{"Rows":1,"Cols":2,"Data":[3,4],"Owner":"e2"}]`)
	if err != nil || !strings.Contains(out, "received 2 updates") || !strings.Contains(out, "aggregated rows=1 cols=2") {
		t.Fatalf("edge federated aggregate: %q %v", out, err)
	}
	out, _, err = runCLI(t, "", "edge", "spatial", "stream", "--anchor", `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`)
	if err != nil || !strings.Contains(out, "parsed anchors=1") {
		t.Fatalf("edge spatial: %q %v", out, err)
	}
}

func TestEdgeBaseURL(t *testing.T) {
	isolateEnv(t)
	root := newRootCmd()
	edge, _, err := root.Find([]string{"edge", "status"})
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]string{
		"":                  defaultEdgeURL,
		":7910":             "http://localhost:7910",
		"0.0.0.0:9000":      "http://localhost:9000",
		"10.0.0.5:7910":     "http://10.0.0.5:7910",
		"https://edge.test": "https://edge.test",
	}
	for env, want := range cases {
		t.Setenv("EDGE_LISTEN", env)
		if got := edgeBaseURL(edge); got != want {
			t.Errorf("EDGE_LISTEN=%q: got %q, want %q", env, got, want)
		}
	}
}
