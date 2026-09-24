package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/keymanager"
	"github.com/ayoubzulfiqar/aerollm/internal/ratelimit"
)

type fakeVirtual struct {
	keys map[string]*keymanager.VirtualKey
}

func (f *fakeVirtual) Validate(_ context.Context, key string) (*keymanager.VirtualKey, error) {
	if key == "sk-broke" {
		return nil, keymanager.ErrBudgetExceeded
	}
	if vk, ok := f.keys[key]; ok {
		return vk, nil
	}
	return nil, errors.New("invalid")
}

func TestAuthenticatorOverBudgetKeyGets402(t *testing.T) {
	auth := NewAuthenticator(nil, nil, &fakeVirtual{})
	w := serve(auth.RequireKey()(http.HandlerFunc(ok200)), withKey(httptest.NewRequest("POST", "/", nil), "sk-broke"))
	if w.Code != http.StatusPaymentRequired {
		t.Fatalf("over-budget key: %d", w.Code)
	}
}

func ok200(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }

func serve(h http.Handler, r *http.Request) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func withKey(r *http.Request, key string) *http.Request {
	r.Header.Set("Authorization", "Bearer "+key)
	return r
}

func TestChainOrder(t *testing.T) {
	var order []string
	mk := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name)
				next.ServeHTTP(w, r)
			})
		}
	}
	serve(Chain(http.HandlerFunc(ok200), mk("a"), nil, mk("b")), httptest.NewRequest("GET", "/", nil))
	if strings.Join(order, ",") != "a,b" {
		t.Fatalf("unexpected order %v", order)
	}
}

func TestAuthenticatorStaticAdminAndVirtual(t *testing.T) {
	vk := &keymanager.VirtualKey{TeamID: "t1"}
	auth := NewAuthenticator([]string{"admin-key"}, []string{"sk-demo"}, &fakeVirtual{keys: map[string]*keymanager.VirtualKey{"sk-virtual": vk}})
	var got *Principal
	capture := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got, _ = PrincipalFromContext(r.Context())
		w.WriteHeader(http.StatusOK)
	})
	h := auth.RequireKey()(capture)

	if w := serve(h, httptest.NewRequest("POST", "/", nil)); w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") == "" {
		t.Fatalf("missing key: %d", w.Code)
	}
	if w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "wrong")); w.Code != http.StatusUnauthorized {
		t.Fatalf("wrong key: %d", w.Code)
	}
	// A static key with the "sk-" prefix must not be routed to virtual validation.
	if w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "sk-demo")); w.Code != http.StatusOK || got == nil || got.Admin || got.Virtual != nil {
		t.Fatalf("static sk- key: code=%d principal=%+v", w.Code, got)
	}
	if w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "sk-virtual")); w.Code != http.StatusOK || got.Virtual != vk {
		t.Fatalf("virtual key: code=%d principal=%+v", w.Code, got)
	}
	if got.KeyID == "" || strings.Contains(got.KeyID, "sk-virtual") {
		t.Fatalf("KeyID must be a non-reversible id, got %q", got.KeyID)
	}
	r := httptest.NewRequest("POST", "/", nil)
	r.Header.Set("X-API-Key", "admin-key")
	if w := serve(h, r); w.Code != http.StatusOK || !got.Admin {
		t.Fatalf("x-api-key admin: code=%d", w.Code)
	}
}

func TestRequireAdmin(t *testing.T) {
	auth := NewAuthenticator([]string{"admin-key"}, []string{"client-key"}, nil)
	h := auth.RequireAdmin()(http.HandlerFunc(ok200))
	if w := serve(h, withKey(httptest.NewRequest("GET", "/", nil), "client-key")); w.Code != http.StatusForbidden {
		t.Fatalf("client key on admin route: %d", w.Code)
	}
	if w := serve(h, withKey(httptest.NewRequest("GET", "/", nil), "admin-key")); w.Code != http.StatusOK {
		t.Fatalf("admin key: %d", w.Code)
	}
	if w := serve(h, withKey(httptest.NewRequest("GET", "/", nil), "")); w.Code != http.StatusUnauthorized {
		t.Fatalf("empty key: %d", w.Code)
	}
}

func TestErrorBodyIsValidJSON(t *testing.T) {
	w := httptest.NewRecorder()
	WriteJSONError(w, http.StatusBadRequest, `bad "quote" \ and newline`+"\n", "")
	var body struct {
		Error struct {
			Message string `json:"message"`
			Type    string `json:"type"`
			Code    int    `json:"code"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("invalid JSON %q: %v", w.Body.String(), err)
	}
	if body.Error.Type != "invalid_request_error" || body.Error.Code != 400 || !strings.Contains(body.Error.Message, `"quote"`) {
		t.Fatalf("unexpected body %+v", body)
	}
}

func TestRequestIDPropagation(t *testing.T) {
	var seen string
	h := RequestID()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { seen = RequestIDFromContext(r.Context()) }))
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Request-ID", "abc-123")
	if w := serve(h, r); w.Header().Get("X-Request-ID") != "abc-123" || seen != "abc-123" {
		t.Fatalf("inbound id not propagated: %q %q", w.Header().Get("X-Request-ID"), seen)
	}
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("X-Request-ID", "bad id\nwith injection")
	if w := serve(h, r); len(w.Header().Get("X-Request-ID")) != 32 {
		t.Fatalf("malformed id should be replaced, got %q", w.Header().Get("X-Request-ID"))
	}
}

func TestRecoverWritesJSON500(t *testing.T) {
	h := Recover(nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { panic("boom") }))
	w := serve(h, httptest.NewRequest("GET", "/", nil))
	if w.Code != 500 || !strings.Contains(w.Body.String(), "internal server error") {
		t.Fatalf("got %d %q", w.Code, w.Body.String())
	}
}

func TestBodyLimit(t *testing.T) {
	h := BodyLimit(8)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if _, err := io.ReadAll(r.Body); err != nil {
			WriteJSONError(w, http.StatusRequestEntityTooLarge, "too large", "")
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	if w := serve(h, httptest.NewRequest("POST", "/", strings.NewReader("12345678"))); w.Code != 200 {
		t.Fatalf("within limit: %d", w.Code)
	}
	if w := serve(h, httptest.NewRequest("POST", "/", strings.NewReader("123456789"))); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("over limit: %d", w.Code)
	}
	r := httptest.NewRequest("POST", "/", io.NopCloser(strings.NewReader("123456789")))
	r.ContentLength = -1
	if w := serve(h, r); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked over limit: %d", w.Code)
	}
}

func TestCORS(t *testing.T) {
	h := CORS([]string{"https://app.example.com"})(http.HandlerFunc(ok200))
	r := httptest.NewRequest("OPTIONS", "/v1/chat/completions", nil)
	r.Header.Set("Origin", "https://app.example.com")
	r.Header.Set("Access-Control-Request-Method", "POST")
	w := serve(h, r)
	if w.Code != http.StatusNoContent || w.Header().Get("Access-Control-Allow-Origin") != "https://app.example.com" {
		t.Fatalf("preflight: %d %v", w.Code, w.Header())
	}
	r = httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "https://evil.example.com")
	if w := serve(h, r); w.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("disallowed origin must not get CORS headers")
	}
}

func TestMethods(t *testing.T) {
	h := Methods(http.MethodPost)(http.HandlerFunc(ok200))
	w := serve(h, httptest.NewRequest("GET", "/", nil))
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "POST" {
		t.Fatalf("got %d allow=%q", w.Code, w.Header().Get("Allow"))
	}
}

func TestRateLimitEnforcesPerKey(t *testing.T) {
	rl := ratelimit.NewTokenBucketLimiter(1, 2) // burst 2
	auth := NewAuthenticator(nil, []string{"a", "b"}, nil)
	h := Chain(http.HandlerFunc(ok200), auth.RequireKey(), RateLimit(RateLimitOptions{Limiter: rl}))

	for i := 0; i < 2; i++ {
		w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "a"))
		if w.Code != 200 {
			t.Fatalf("request %d: %d", i, w.Code)
		}
		if w.Header().Get("X-RateLimit-Limit-Requests") != "2" {
			t.Fatalf("limit header: %q", w.Header().Get("X-RateLimit-Limit-Requests"))
		}
	}
	w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "a"))
	if w.Code != http.StatusTooManyRequests || w.Header().Get("Retry-After") == "" || w.Header().Get("X-RateLimit-Remaining-Requests") != "0" {
		t.Fatalf("expected 429 with Retry-After, got %d %v", w.Code, w.Header())
	}
	if w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "b")); w.Code != 200 {
		t.Fatalf("other key must have its own bucket: %d", w.Code)
	}
}

func TestRateLimitVirtualKeyOverride(t *testing.T) {
	rl := ratelimit.NewTokenBucketLimiter(100, 1)
	vk := &keymanager.VirtualKey{Metadata: map[string]interface{}{"rate_limit_rps": 1.0}}
	auth := NewAuthenticator(nil, nil, &fakeVirtual{keys: map[string]*keymanager.VirtualKey{"sk-v": vk}})
	h := Chain(http.HandlerFunc(ok200), auth.RequireKey(), RateLimit(RateLimitOptions{Limiter: rl}))
	if w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "sk-v")); w.Code != 200 {
		t.Fatalf("first: %d", w.Code)
	}
	if w := serve(h, withKey(httptest.NewRequest("POST", "/", nil), "sk-v")); w.Code != http.StatusTooManyRequests {
		t.Fatalf("per-key rps=1 should reject the second request, got %d", w.Code)
	}
}

func TestVirtualKeyAuthMiddlewareStaticSkKey(t *testing.T) {
	m := NewVirtualKeyAuthMiddleware(ok200, nil, map[string]bool{"sk-demo": true})
	if w := serve(m, withKey(httptest.NewRequest("POST", "/", nil), "sk-demo")); w.Code != 200 {
		t.Fatalf("static sk- key must authenticate, got %d", w.Code)
	}
	if w := serve(m, withKey(httptest.NewRequest("POST", "/", nil), "sk-other")); w.Code != 401 {
		t.Fatalf("unknown key: %d", w.Code)
	}
}

type flushRecorder struct {
	*httptest.ResponseRecorder
	flushed bool
}

func (f *flushRecorder) Flush() { f.flushed = true }

func TestWrappersPreserveFlusher(t *testing.T) {
	h := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := http.NewResponseController(w).Flush(); err != nil {
			t.Errorf("flush through middleware failed: %v", err)
		}
	}), Recover(nil), AccessLog(&testLogger{}))
	fr := &flushRecorder{ResponseRecorder: httptest.NewRecorder()}
	h.ServeHTTP(fr, httptest.NewRequest("GET", "/", nil))
	if !fr.flushed {
		t.Fatal("flush did not reach the underlying writer")
	}
}
