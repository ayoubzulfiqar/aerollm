package cache

import (
	"context"
	"encoding/json"
	"path"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/redis/go-redis/v9"
)

// fakeRedis is an in-memory RedisClient supporting GET/SET/DEL/SCAN.
type fakeRedis struct {
	mu   sync.Mutex
	data map[string]string
}

func newFakeRedis() *fakeRedis { return &fakeRedis{data: map[string]string{}} }

func (f *fakeRedis) Get(ctx context.Context, key string) *redis.StringCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	cmd := redis.NewStringCmd(ctx)
	if v, ok := f.data[key]; ok {
		cmd.SetVal(v)
	} else {
		cmd.SetErr(redis.Nil)
	}
	return cmd
}

func (f *fakeRedis) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) *redis.StatusCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch v := value.(type) {
	case string:
		f.data[key] = v
	case []byte:
		f.data[key] = string(v)
	}
	cmd := redis.NewStatusCmd(ctx)
	cmd.SetVal("OK")
	return cmd
}

func (f *fakeRedis) Del(ctx context.Context, keys ...string) *redis.IntCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, k := range keys {
		if _, ok := f.data[k]; ok {
			delete(f.data, k)
			n++
		}
	}
	cmd := redis.NewIntCmd(ctx)
	cmd.SetVal(int64(n))
	return cmd
}

func (f *fakeRedis) Keys(ctx context.Context, pattern string) *redis.StringSliceCmd {
	panic("KEYS must not be used (blocks Redis)")
}

// Scan returns every matching key in one page (cursor always ends at 0).
func (f *fakeRedis) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	f.mu.Lock()
	defer f.mu.Unlock()
	var keys []string
	for k := range f.data {
		if ok, _ := path.Match(match, k); ok {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	return redis.NewScanCmdResult(keys, 0, nil)
}

func (f *fakeRedis) Close() error { return nil }

func sp(s string) *string   { return &s }
func fp(f float64) *float64 { return &f }
func ip(i int) *int         { return &i }
func baseReq() models.LLMRequest {
	return models.LLMRequest{
		Model:    "gpt-4o",
		Messages: []models.Message{{Role: models.RoleUser, Content: sp("hello")}},
	}
}

func TestKeyForRequestCoversOutputAffectingFields(t *testing.T) {
	base := baseReq()
	k0 := KeyForRequestNS("", &base)
	if !strings.HasPrefix(k0, ExactKeyPrefix) {
		t.Fatalf("key %q lacks prefix", k0)
	}
	if KeyForRequest(&base) != k0 {
		t.Fatal("KeyForRequest must equal KeyForRequestNS with empty namespace")
	}
	mutations := map[string]func(r *models.LLMRequest){
		"model":           func(r *models.LLMRequest) { r.Model = "gpt-4o-mini" },
		"content":         func(r *models.LLMRequest) { r.Messages[0].Content = sp("hello!") },
		"role":            func(r *models.LLMRequest) { r.Messages[0].Role = models.RoleSystem },
		"temperature":     func(r *models.LLMRequest) { r.Temperature = fp(0.2) },
		"top_p":           func(r *models.LLMRequest) { r.TopP = fp(0.5) },
		"max_tokens":      func(r *models.LLMRequest) { r.MaxTokens = ip(10) },
		"stop":            func(r *models.LLMRequest) { r.Stop = []string{"\n"} },
		"presence":        func(r *models.LLMRequest) { r.PresencePenalty = fp(1) },
		"frequency":       func(r *models.LLMRequest) { r.FrequencyPenalty = fp(1) },
		"rag":             func(r *models.LLMRequest) { r.RagEnabled = true },
		"response_format": func(r *models.LLMRequest) { r.ResponseFormat = &models.ResponseFormat{Type: "json_object"} },
		"tools": func(r *models.LLMRequest) {
			r.Tools = []models.ToolDefinition{{Name: "f", Parameters: map[string]interface{}{"type": "object"}}}
		},
		"tool_calls": func(r *models.LLMRequest) {
			r.Messages = append(r.Messages, models.Message{Role: models.RoleAssistant, ToolCalls: []models.ToolCall{{ID: "1", Type: "function", Function: models.ToolFunction{Name: "f", Arguments: "{}"}}}})
		},
		"tool_result": func(r *models.LLMRequest) {
			r.Messages = append(r.Messages, models.Message{Role: models.RoleTool, ToolCallID: sp("1"), Content: sp("42")})
		},
	}
	seen := map[string]string{k0: "base"}
	for name, mut := range mutations {
		r := baseReq()
		mut(&r)
		k := KeyForRequestNS("", &r)
		if prev, dup := seen[k]; dup {
			t.Fatalf("mutation %q produced same key as %q", name, prev)
		}
		seen[k] = name
	}

	streamed := baseReq()
	streamed.Stream = true
	if KeyForRequestNS("", &streamed) != k0 {
		t.Fatal("stream flag must not change the cache key")
	}
	// Large integers (e.g. seeds) must not lose precision in the key.
	var a, b models.LLMRequest
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[],"seed":9007199254740993}`), &a)
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[],"seed":9007199254740992}`), &b)
	if KeyForRequestNS("", &a) == KeyForRequestNS("", &b) {
		t.Fatal("distinct seeds collided")
	}
	var n1, n2 models.LLMRequest
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[],"n":1}`), &n1)
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[],"n":2}`), &n2)
	if KeyForRequestNS("", &n1) == KeyForRequestNS("", &n2) {
		t.Fatal("n must be part of the key")
	}
}

func TestKeyForRequestNamespaceIsolationAndDeterminism(t *testing.T) {
	r := baseReq()
	r.Tools = []models.ToolDefinition{{Name: "f", Parameters: map[string]interface{}{"b": 1, "a": 2, "c": map[string]interface{}{"z": 1, "y": 2}}}}
	a := KeyForRequestNS(TenantNamespace("sk-a"), &r)
	b := KeyForRequestNS(TenantNamespace("sk-b"), &r)
	if a == b {
		t.Fatal("different tenants must get different keys")
	}
	for i := 0; i < 20; i++ {
		if KeyForRequestNS(TenantNamespace("sk-a"), &r) != a {
			t.Fatal("key not deterministic")
		}
	}
	if strings.Contains(a, "sk-a") || strings.Contains(TenantNamespace("sk-a"), "sk-a") {
		t.Fatal("raw API key leaked into key/namespace")
	}
	if KeyForRequestNS("x", nil) != "" {
		t.Fatal("nil request must produce empty (uncacheable) key")
	}
}

func TestIsCacheable(t *testing.T) {
	r := baseReq()
	if !IsCacheable(&r) {
		t.Fatal("expected cacheable")
	}
	if IsCacheable(nil) || IsCacheable(&models.LLMRequest{Model: "m"}) {
		t.Fatal("nil/empty requests are not cacheable")
	}
	var multi models.LLMRequest
	_ = json.Unmarshal([]byte(`{"model":"m","messages":[{"role":"user","content":"x"}],"n":3}`), &multi)
	if IsCacheable(&multi) {
		t.Fatal("n>1 must not be cacheable")
	}
}

func TestExactCacheRoundTripAndStats(t *testing.T) {
	fr := newFakeRedis()
	c := NewRedisCache(fr, time.Hour)
	ctx := context.Background()
	r := baseReq()
	key := KeyForRequestNS(TenantNamespace("sk-a"), &r)

	if e, err := c.GetExactCtx(ctx, key); err != nil || e != nil {
		t.Fatalf("expected miss, got %v %v", e, err)
	}
	resp := []byte(`{"id":"1","choices":[]}`)
	if err := c.SetExactWithMeta(ctx, key, resp, EntryMeta{Model: "gpt-4o", TokenCount: 12}); err != nil {
		t.Fatal(err)
	}
	e, err := c.GetExact(key)
	if err != nil || e == nil {
		t.Fatalf("expected hit: %v", err)
	}
	if string(e.Response) != string(resp) || e.Model != "gpt-4o" || e.TokenCount != 12 || e.CreatedAt.IsZero() {
		t.Fatalf("unexpected entry %+v", e)
	}
	// Non-JSON payloads survive too.
	_ = c.SetExact(key+"b", []byte("plain"), 1)
	if e, _ := c.GetExact(key + "b"); e == nil || string(e.Response) != "plain" {
		t.Fatalf("binary payload roundtrip failed: %+v", e)
	}
	// Legacy raw values are returned as-is.
	fr.data[ExactKeyPrefix+"legacy"] = `{"model":"m"}`
	if e, _ := c.GetExact(ExactKeyPrefix + "legacy"); e == nil || string(e.Response) != `{"model":"m"}` {
		t.Fatalf("legacy value not readable: %+v", e)
	}
	// Empty key is never cached.
	_ = c.SetExact("", resp, 0)
	if _, ok := fr.data[""]; ok {
		t.Fatal("empty key must not be stored")
	}

	st, err := c.ExactStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if st.Entries != 3 || st.Hits != 3 || st.Misses != 1 {
		t.Fatalf("unexpected stats %+v", st)
	}
	m, _ := c.Stats(ctx)
	if _, ok := m["exact_total_entries"].(int); !ok {
		t.Fatal("exact_total_entries must stay an int")
	}

	entries, cur, err := c.Inspect(ctx, "0", 10)
	if err != nil || cur != "0" || len(entries) != 3 {
		t.Fatalf("inspect: %v %q %d", err, cur, len(entries))
	}
	for _, ie := range entries {
		if ie.Key == key && (ie.Model != "gpt-4o" || ie.TokenCount != 12) {
			t.Fatalf("inspect lost metadata: %+v", ie)
		}
	}
}

func TestExactCacheClearAndNamespace(t *testing.T) {
	fr := newFakeRedis()
	c := NewRedisCache(fr, time.Hour)
	ctx := context.Background()
	r := baseReq()
	ka := KeyForRequestNS("tenant-a", &r)
	kb := KeyForRequestNS("tenant-b", &r)
	_ = c.SetExact(ka, []byte(`{}`), 0)
	_ = c.SetExact(kb, []byte(`{}`), 0)
	fr.data["unrelated"] = "keep"

	n, err := c.ClearNamespace(ctx, "tenant-a")
	if err != nil || n != 1 {
		t.Fatalf("ClearNamespace: %d %v", n, err)
	}
	if _, ok := fr.data[kb]; !ok {
		t.Fatal("other namespace must survive")
	}
	if err := c.ClearExact(ctx); err != nil {
		t.Fatal(err)
	}
	if len(fr.data) != 1 || fr.data["unrelated"] != "keep" {
		t.Fatalf("ClearExact must only delete cache keys: %v", fr.data)
	}
}

func TestNilCacheIsSafe(t *testing.T) {
	var c *RedisCache
	if e, err := c.GetExact("k"); e != nil || err != nil {
		t.Fatal("nil cache must miss")
	}
	if err := c.SetExact("k", []byte("x"), 0); err != nil {
		t.Fatal(err)
	}
	c2 := NewRedisCache(nil, 0)
	if _, err := c2.Stats(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := c2.ClearExact(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestLegacySemanticCacheMatchesContentNotLength(t *testing.T) {
	c := NewRedisCache(nil, time.Hour)
	_ = c.SetSemantic("what is the capital of france", []byte("paris"), 0)
	// Same token count, different content: must not hit.
	if e, _ := c.GetSemantic("how do airplanes fly so high", 0.9); e != nil {
		t.Fatalf("unrelated prompt returned cached response %q", e.Response)
	}
	if e, _ := c.GetSemantic("What is the capital of France?", 0.9); e == nil || string(e.Response) != "paris" {
		t.Fatal("expected hit for the same prompt")
	}
	// Threshold 0 must not turn every query into a hit.
	if e, _ := c.GetSemantic("completely different words here now", 0); e != nil {
		t.Fatal("threshold 0 must fall back to default")
	}
}

func TestStoredEnvelopeIsJSON(t *testing.T) {
	fr := newFakeRedis()
	c := NewRedisCache(fr, time.Hour)
	_ = c.SetExact(ExactKeyPrefix+"k", []byte(`{"a":1}`), 3)
	var env map[string]interface{}
	if err := json.Unmarshal([]byte(fr.data[ExactKeyPrefix+"k"]), &env); err != nil {
		t.Fatal(err)
	}
	if env["aerollm_cache"] != float64(1) {
		t.Fatalf("unexpected envelope %v", env)
	}
}
