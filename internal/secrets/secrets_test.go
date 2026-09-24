package secrets

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestSecretsStore(t *testing.T) {
	store := NewStore()
	if err := store.Upsert(Secret{Name: "api-key", Value: "secret123", Type: "token"}); err != nil {
		t.Fatal(err)
	}
	if len(store.List()) != 1 {
		t.Fatalf("expected 1 secret, got %d", len(store.List()))
	}
	if !store.Delete(store.List()[0].ID) {
		t.Fatalf("expected delete to succeed")
	}
}

func TestSecretsWebhook(t *testing.T) {
	mux := http.NewServeMux()
	store := NewStore()
	mux.HandleFunc("/v1/secrets", WebhookHandler(store))

	body := `{"name":"api-key","value":"secret123","type":"token"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/secrets", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "secret123") {
		t.Fatalf("create response must not echo the value: %s", rec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/secrets", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), `"type":"token"`) {
		t.Fatalf("expected token type in body, got: %s", listRec.Body.String())
	}
	if strings.Contains(listRec.Body.String(), "secret123") {
		t.Fatalf("list must never contain values: %s", listRec.Body.String())
	}

	get := func(q string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/v1/secrets?"+q, nil)
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if w := get("id=sec_api-key"); w.Code != http.StatusOK || strings.Contains(w.Body.String(), "secret123") {
		t.Fatalf("metadata get: %d %s", w.Code, w.Body.String())
	}
	w := get("id=sec_api-key&reveal=true")
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"value":"secret123"`) || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("reveal: %d %s", w.Code, w.Body.String())
	}
	if w := get("id=missing&reveal=true"); w.Code != http.StatusNotFound {
		t.Fatalf("reveal missing: %d", w.Code)
	}
	if w := get("id=sec_api-key&reveal=true&version=-1"); w.Code != http.StatusBadRequest {
		t.Fatalf("bad version: %d", w.Code)
	}

	for _, bad := range []string{
		`{"name":"../etc/passwd","value":"x"}`,
		`{"name":"ok","value":""}`,
		`{"value":"x"}`,
		`{"name":"a b","value":"x"}`,
		`not json`,
	} {
		r := httptest.NewRequest(http.MethodPost, "/v1/secrets", strings.NewReader(bad))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest || w.Header().Get("Content-Type") != "application/json" {
			t.Errorf("%s: expected 400 json, got %d", bad, w.Code)
		}
	}
	r := httptest.NewRequest(http.MethodPatch, "/v1/secrets", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") == "" {
		t.Fatalf("expected 405 with Allow, got %d", w.Code)
	}
	big := `{"name":"big","value":"` + strings.Repeat("a", MaxBodyBytes) + `"}`
	r = httptest.NewRequest(http.MethodPost, "/v1/secrets", strings.NewReader(big))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
}

func TestEncryptionAtRestAndVersions(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s, err := NewStoreWithKey(key)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Upsert(Secret{Name: "db", Value: "hunter2-plaintext"}); err != nil {
		t.Fatal(err)
	}
	e := s.secrets["sec_db"]
	if bytes.Contains(e.versions[0].ciphertext, []byte("hunter2")) {
		t.Fatal("value stored in plaintext")
	}
	// Rotation creates a new version; old versions stay revealable.
	_ = s.Upsert(Secret{Name: "db", Value: "v2"})
	sec, err := s.Reveal("sec_db")
	if err != nil || sec.Value != "v2" || sec.Version != 2 {
		t.Fatalf("latest: %v %+v", err, sec)
	}
	old, err := s.RevealVersion("sec_db", 1)
	if err != nil || old.Value != "hunter2-plaintext" {
		t.Fatalf("v1: %v %+v", err, old)
	}
	if got, _ := s.Get("sec_db"); got.Value != "" {
		t.Fatal("Get must not return the value")
	}
	// Tampering / swapping ciphertexts is detected (AAD binds id+version).
	s.secrets["sec_db"].versions[1].ciphertext[0] ^= 1
	if _, err := s.Reveal("sec_db"); !errors.Is(err, ErrDecrypt) {
		t.Fatalf("expected decrypt error, got %v", err)
	}
	s.secrets["sec_db"].versions[1].ciphertext[0] ^= 1

	// Version retention.
	s.SetMaxVersions(2)
	_ = s.Upsert(Secret{Name: "db", Value: "v3"})
	if m, _ := s.Metadata("sec_db"); len(m.Versions) != 2 || m.Versions[0] != 2 || m.Version != 3 {
		t.Fatalf("unexpected versions %+v", m)
	}

	// Key rotation keeps values readable.
	newKey := make([]byte, 32)
	_, _ = rand.Read(newKey)
	if err := s.RotateKey(newKey); err != nil {
		t.Fatal(err)
	}
	if sec, err := s.Reveal("sec_db"); err != nil || sec.Value != "v3" {
		t.Fatalf("after key rotation: %v %+v", err, sec)
	}
}

func TestEnvKeyHandling(t *testing.T) {
	t.Setenv(EnvKey, "")
	if s := NewStore(); !s.IsEphemeral() || s.KeyError() != nil {
		t.Fatal("unset key should give an ephemeral store")
	}
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	t.Setenv(EnvKey, hex.EncodeToString(key))
	s := NewStore()
	if s.IsEphemeral() || s.KeyError() != nil {
		t.Fatal("hex key should be accepted")
	}
	t.Setenv(EnvKey, "too-short")
	s = NewStore()
	if s.KeyError() == nil {
		t.Fatal("invalid key must be reported")
	}
	if err := s.Upsert(Secret{Name: "x", Value: "y"}); err == nil {
		t.Fatal("store with invalid key must refuse writes")
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := NewStore()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _ = s.Upsert(Secret{Name: "k", Value: "v"}) }()
		go func() { defer wg.Done(); _, _ = s.Reveal("sec_k"); _ = s.List() }()
		go func() {
			defer wg.Done()
			k := make([]byte, 32)
			_, _ = rand.Read(k)
			_ = s.RotateKey(k)
		}()
	}
	wg.Wait()
	if sec, err := s.Reveal("sec_k"); err != nil || sec.Value != "v" {
		t.Fatalf("final reveal: %v", err)
	}
}
