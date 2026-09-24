package traffic

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func mustTester(t *testing.T, cfg ShadowTesterConfig) *ShadowTester {
	t.Helper()
	s, err := NewShadowTesterWithConfig(cfg)
	if err != nil {
		t.Fatalf("NewShadowTesterWithConfig: %v", err)
	}
	return s
}

func TestShadowTesterRunAsync(t *testing.T) {
	s := NewShadowTester()
	models_ := make(chan string, 1)
	auth := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req models.LLMRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		models_ <- req.Model + "|" + r.URL.Path + "|" + r.Header.Get("X-AeroLLM-Shadow")
		auth <- r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(models.LLMResponse{Model: "gpt-4"})
	}))
	defer server.Close()

	if err := s.RunAsync(context.Background(), server.URL, "sk-test", &models.LLMRequest{Model: "gpt-4"}); err != nil {
		t.Fatalf("RunAsync: %v", err)
	}
	s.Wait()
	select {
	case got := <-models_:
		if got != "gpt-4|/v1/chat/completions|1" {
			t.Fatalf("unexpected request seen by shadow: %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shadow request never arrived")
	}
	if a := <-auth; a != "Bearer sk-test" {
		t.Fatalf("expected bearer auth to loopback target, got %q", a)
	}
	var found bool
	for _, r := range s.Results() {
		if r.Provider == server.URL && r.StatusCode == http.StatusOK && r.Error == nil {
			found = true
		}
	}
	if !found {
		t.Fatalf("result not recorded: %+v", s.Results())
	}
}

func TestRunAsyncDoesNotBlockAndSurvivesCancel(t *testing.T) {
	release := make(chan struct{})
	var completed atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		completed.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	s := mustTester(t, ShadowTesterConfig{TargetURL: server.URL, Timeout: 5 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	if err := s.RunAsync(ctx, "", "", &models.LLMRequest{Model: "m"}); err != nil {
		t.Fatalf("RunAsync: %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("RunAsync blocked on the shadow call")
	}
	cancel() // the originating handler returns
	time.Sleep(50 * time.Millisecond)
	close(release)
	s.Wait()
	if !completed.Load() {
		t.Fatalf("detached shadow call was cancelled with the caller context")
	}
	res := s.Results()
	if len(res) != 1 || res[0].Error != nil || res[0].StatusCode != http.StatusOK {
		t.Fatalf("unexpected results: %+v", res)
	}
}

func TestRunAsyncCancelledContext(t *testing.T) {
	s := mustTester(t, ShadowTesterConfig{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.RunAsync(ctx, "http://127.0.0.1:1", "", map[string]string{"a": "b"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestShadowTimeout(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-done:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	defer close(done)
	s := mustTester(t, ShadowTesterConfig{Timeout: 100 * time.Millisecond})
	start := time.Now()
	if err := s.RunAsync(context.Background(), server.URL, "", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if time.Since(start) > 3*time.Second {
		t.Fatalf("timeout not applied")
	}
	res := s.Results()
	if len(res) != 1 || !errors.Is(res[0].Error, context.DeadlineExceeded) || res[0].ErrorMessage == "" {
		t.Fatalf("expected deadline exceeded result, got %+v", res)
	}
}

func TestShadowBusy(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	s := mustTester(t, ShadowTesterConfig{MaxInFlight: 1, Timeout: 5 * time.Second})
	if err := s.RunAsync(context.Background(), server.URL, "", map[string]int{"n": 1}); err != nil {
		t.Fatal(err)
	}
	if err := s.RunAsync(context.Background(), server.URL, "", map[string]int{"n": 2}); !errors.Is(err, ErrShadowBusy) {
		t.Fatalf("expected ErrShadowBusy, got %v", err)
	}
	close(release)
	s.Wait()
	if err := s.RunAsync(context.Background(), server.URL, "", map[string]int{"n": 3}); err != nil {
		t.Fatalf("slot not released: %v", err)
	}
	s.Wait()
}

func TestInvalidTargetsAndRequests(t *testing.T) {
	s := mustTester(t, ShadowTesterConfig{})
	req := map[string]string{"model": "m"}
	cases := map[string]error{
		"":                                    ErrNoShadowTarget,
		"   ":                                 ErrNoShadowTarget,
		"ftp://example.com":                   ErrInvalidShadowTarget,
		"http://":                             ErrInvalidShadowTarget,
		"example.com":                         ErrInvalidShadowTarget,
		"/relative":                           ErrInvalidShadowTarget,
		"http://user:pw@example.com":          ErrInvalidShadowTarget,
		"http://exa mple.com":                 ErrInvalidShadowTarget,
		"http://example.com/\x00":             ErrInvalidShadowTarget,
		"http://example.com/a\nb":             ErrInvalidShadowTarget,
		"http://example.com?x=1":              ErrInvalidShadowTarget,
		"http://example.com#frag":             ErrInvalidShadowTarget,
		"http://example.com:0":                ErrInvalidShadowTarget,
		"http://example.com:99999":            ErrInvalidShadowTarget,
		"://bad":                              ErrInvalidShadowTarget,
		"mailto:a@b.c":                        ErrInvalidShadowTarget,
		"http://" + strings.Repeat("a", 3000): ErrInvalidShadowTarget,
	}
	for target, want := range cases {
		if err := s.RunAsync(context.Background(), target, "k", req); !errors.Is(err, want) {
			t.Errorf("target %q: expected %v, got %v", target, want, err)
		}
	}
	if err := s.RunAsync(context.Background(), "http://127.0.0.1:1", "", nil); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("nil request: expected ErrInvalidRequest, got %v", err)
	}
	var typedNil *models.LLMRequest
	if err := s.RunAsync(context.Background(), "http://127.0.0.1:1", "", typedNil); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("typed nil request: expected ErrInvalidRequest, got %v", err)
	}
	if err := s.RunAsync(context.Background(), "http://127.0.0.1:1", "", make(chan int)); !errors.Is(err, ErrInvalidRequest) {
		t.Errorf("unmarshalable request: expected ErrInvalidRequest, got %v", err)
	}
	huge := map[string]string{"x": strings.Repeat("a", MaxPayloadBytes)}
	if err := s.RunAsync(context.Background(), "http://127.0.0.1:1", "", huge); !errors.Is(err, ErrPayloadTooLarge) {
		t.Errorf("huge request: expected ErrPayloadTooLarge, got %v", err)
	}
	if len(s.Results()) != 0 {
		t.Fatalf("invalid input must not dispatch")
	}
	if _, err := NewShadowTesterWithConfig(ShadowTesterConfig{TargetURL: "ftp://x"}); !errors.Is(err, ErrInvalidShadowTarget) {
		t.Fatalf("invalid configured target accepted: %v", err)
	}
}

func TestConfiguredTargetEnforced(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	s := mustTester(t, ShadowTesterConfig{TargetURL: server.URL})
	if err := s.RunAsync(context.Background(), "http://169.254.169.254", "", map[string]string{}); !errors.Is(err, ErrInvalidShadowTarget) {
		t.Fatalf("expected mismatch rejection, got %v", err)
	}
	if err := s.RunAsync(context.Background(), server.URL+"/", "", map[string]string{}); err != nil {
		t.Fatalf("configured target (trailing slash) rejected: %v", err)
	}
	if err := s.RunAsync(context.Background(), "", "", map[string]string{}); err != nil {
		t.Fatalf("empty target should use configured one: %v", err)
	}
	s.Wait()
	if hits.Load() != 2 {
		t.Fatalf("expected 2 hits, got %d", hits.Load())
	}
}

func TestAuthOnlyOverTLSOrLoopback(t *testing.T) {
	auth := make(chan string, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth <- r.Header.Get("Authorization")
	}))
	defer server.Close()

	// Route a non-loopback hostname to the local test server.
	s := mustTester(t, ShadowTesterConfig{APIKey: "sk-secret"})
	addr := server.Listener.Addr().String()
	s.client = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}}
	if err := s.RunAsync(context.Background(), "http://shadow.example.com", "", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if got := <-auth; got != "" {
		t.Fatalf("API key sent in clear text to remote host: %q", got)
	}
	if res := s.Results(); len(res) != 1 || !res[0].AuthDropped {
		t.Fatalf("expected AuthDropped result, got %+v", res)
	}

	// Empty key: no Authorization header at all.
	s2 := mustTester(t, ShadowTesterConfig{})
	if err := s2.RunAsync(context.Background(), server.URL, "", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	s2.Wait()
	if got := <-auth; got != "" {
		t.Fatalf("unexpected Authorization for empty key: %q", got)
	}

	for raw, want := range map[string]bool{
		"https://example.com":   true,
		"http://127.0.0.1:8080": true,
		"http://[::1]:8080":     true,
		"http://localhost":      true,
		"http://example.com":    false,
		"http://10.0.0.1":       false,
	} {
		u, _ := url.Parse(raw)
		if authAllowed(u) != want {
			t.Errorf("authAllowed(%s) != %v", raw, want)
		}
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var followed atomic.Bool
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Store(true)
	}))
	defer final.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusTemporaryRedirect)
	}))
	defer server.Close()
	s := mustTester(t, ShadowTesterConfig{})
	if err := s.RunAsync(context.Background(), server.URL, "k", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if followed.Load() {
		t.Fatalf("redirect was followed")
	}
	res := s.Results()
	if len(res) != 1 || res[0].StatusCode != http.StatusTemporaryRedirect || res[0].Error == nil {
		t.Fatalf("unexpected result %+v", res)
	}
}

func TestResultsRingBounded(t *testing.T) {
	var n atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200 + int(n.Add(1))) // 201, 202, ...
	}))
	defer server.Close()
	s := mustTester(t, ShadowTesterConfig{ResultBuffer: 3})
	for i := 0; i < 5; i++ {
		if err := s.RunAsync(context.Background(), server.URL, "", map[string]int{"i": i}); err != nil {
			t.Fatal(err)
		}
		s.Wait()
	}
	res := s.Results()
	if len(res) != 3 {
		t.Fatalf("expected 3 results, got %d", len(res))
	}
	for i, want := range []int{203, 204, 205} {
		if res[i].StatusCode != want {
			t.Fatalf("result %d: status %d want %d", i, res[i].StatusCode, want)
		}
	}
}

func TestNewShadowTesterSharesLimiter(t *testing.T) {
	a, b := NewShadowTester(), NewShadowTester()
	if a.pool != b.pool {
		t.Fatalf("default testers must share one pool")
	}
}

func TestZeroValueTesterIsSafe(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
	}))
	defer server.Close()
	var s ShadowTester
	if err := s.RunAsync(context.Background(), server.URL, "", map[string]string{}); err != nil {
		t.Fatal(err)
	}
	s.Wait()
	if hits.Load() != 1 {
		t.Fatalf("expected 1 hit, got %d", hits.Load())
	}
	var nilTester *ShadowTester
	if err := nilTester.RunAsync(context.Background(), server.URL, "", map[string]string{}); err == nil {
		t.Fatalf("nil tester must return an error")
	}
}
