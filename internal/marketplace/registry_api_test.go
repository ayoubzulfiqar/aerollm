package marketplace_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
)

func newKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}

func signed(t *testing.T, priv ed25519.PrivateKey, id, version, creator string) marketplace.PublishRequest {
	t.Helper()
	req, err := marketplace.SignManifest(marketplace.PublishRequest{
		ID: id, Name: "Test Plugin", Version: version, CreatorID: creator,
		WASMHash: marketplace.HashWASM([]byte(id + version)),
	}, priv)
	if err != nil {
		t.Fatal(err)
	}
	return req
}

func newRegistryServer(t *testing.T) (*marketplace.RegistryService, *httptest.Server) {
	t.Helper()
	s := marketplace.NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return s, server
}

func post(t *testing.T, url string, body string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func publish(t *testing.T, server *httptest.Server, req marketplace.PublishRequest) *http.Response {
	t.Helper()
	b, _ := json.Marshal(req)
	return post(t, server.URL+"/v1/marketplace/plugins", string(b))
}

func TestRegistryServicePublishAndGet(t *testing.T) {
	_, server := newRegistryServer(t)
	priv := newKey(t)
	resp := publish(t, server, signed(t, priv, "plugin-1", "1.0.0", "creator"))
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("unexpected content type %q", ct)
	}

	getResp, err := http.Get(server.URL + "/v1/marketplace/plugins/plugin-1")
	if err != nil {
		t.Fatalf("get request failed: %v", err)
	}
	defer getResp.Body.Close()
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
	var body struct {
		Metadata marketplace.Metadata         `json:"metadata"`
		Manifest marketplace.VerifiedManifest `json:"manifest"`
	}
	if err := json.NewDecoder(getResp.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Metadata.ID != "plugin-1" || body.Manifest.Verify() != nil {
		t.Fatalf("stored manifest does not re-verify: %+v", body)
	}
}

func TestRegistryServiceRejectsUnsignedAndForged(t *testing.T) {
	_, server := newRegistryServer(t)
	// The original scaffolding accepted this with fake key material.
	fake := `{"id":"plugin-1","name":"Test Plugin","version":"1.0.0","creator_id":"creator","wasm_hash":"abc","public_key":"key","signature":"sig"}`
	if resp := post(t, server.URL+"/v1/marketplace/plugins", fake); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for fake signature, got %d", resp.StatusCode)
	}
	req := signed(t, newKey(t), "plugin-1", "1.0.0", "creator")
	req.Name = "Tampered"
	if resp := publish(t, server, req); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for tampered manifest, got %d", resp.StatusCode)
	}
	if resp := post(t, server.URL+"/v1/marketplace/plugins", `{"id":"x","extra":1}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for unknown field, got %d", resp.StatusCode)
	}
	huge := `{"name":"` + strings.Repeat("a", marketplace.MaxManifestBytes) + `"}`
	if resp := post(t, server.URL+"/v1/marketplace/plugins", huge); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized body, got %d", resp.StatusCode)
	}
}

func TestRegistryServiceOwnershipVersionsAndIdempotency(t *testing.T) {
	_, server := newRegistryServer(t)
	alice, mallory := newKey(t), newKey(t)

	v1 := signed(t, alice, "plugin-1", "1.0.0", "alice")
	if resp := publish(t, server, v1); resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish v1: %d", resp.StatusCode)
	}
	if resp := publish(t, server, v1); resp.StatusCode != http.StatusOK {
		t.Fatalf("idempotent re-publish should be 200, got %d", resp.StatusCode)
	}
	// Another creator cannot take over the plugin ID.
	if resp := publish(t, server, signed(t, mallory, "plugin-1", "2.0.0", "mallory")); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for ownership takeover, got %d", resp.StatusCode)
	}
	// Impersonating the creator with a different key fails (key pinned on first use).
	if resp := publish(t, server, signed(t, mallory, "plugin-1", "2.0.0", "alice")); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for impersonation, got %d", resp.StatusCode)
	}
	if resp := publish(t, server, signed(t, mallory, "plugin-2", "1.0.0", "alice")); resp.StatusCode != http.StatusForbidden {
		t.Fatalf("expected 403 for impersonation on new plugin, got %d", resp.StatusCode)
	}
	// Downgrade / replay of an older version is refused.
	v2 := signed(t, alice, "plugin-1", "2.0.0", "alice")
	if resp := publish(t, server, v2); resp.StatusCode != http.StatusCreated {
		t.Fatalf("publish v2: %d", resp.StatusCode)
	}
	if resp := publish(t, server, v1); resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for downgrade replay, got %d", resp.StatusCode)
	}
}

func TestRegistryServiceTrustSurvivesRestart(t *testing.T) {
	store := marketplace.NewInMemoryStore()
	alice, mallory := newKey(t), newKey(t)
	s1 := marketplace.NewRegistryService(nil, store)
	if _, _, err := s1.Publish(context.Background(), signed(t, alice, "plugin-1", "1.0.0", "alice")); err != nil {
		t.Fatal(err)
	}
	// A fresh service over the same store (process restart) must still know
	// alice's key rather than trusting the next key it sees.
	s2 := marketplace.NewRegistryService(nil, store)
	_, _, err := s2.Publish(context.Background(), signed(t, mallory, "plugin-9", "1.0.0", "alice"))
	if !errors.Is(err, marketplace.ErrUntrustedKey) {
		t.Fatalf("expected ErrUntrustedKey after restart, got %v", err)
	}
	if _, err := s2.VerifyManifest(context.Background(), "plugin-1"); err != nil {
		t.Fatalf("verify stored manifest: %v", err)
	}
}

func TestRegistryServiceStrictTrustStore(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	s.SetTrustStore(marketplace.NewTrustStore())
	_, _, err := s.Publish(context.Background(), signed(t, newKey(t), "plugin-1", "1.0.0", "alice"))
	if !errors.Is(err, marketplace.ErrUntrustedKey) {
		t.Fatalf("expected strict store to reject unpinned key, got %v", err)
	}
}

func TestRegistryServiceConcurrentPublish(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	priv := newKey(t)
	var wg sync.WaitGroup
	created := make(chan bool, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, ok, err := s.Publish(context.Background(), signed(t, priv, "plugin-1", "1.0.0", "alice"))
			if err == nil {
				created <- ok
			}
		}()
	}
	wg.Wait()
	close(created)
	n := 0
	for ok := range created {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("expected exactly one creating publish, got %d", n)
	}
}

func TestRegistryServiceList(t *testing.T) {
	s, server := newRegistryServer(t)
	priv := newKey(t)
	for i := 0; i < 5; i++ {
		if _, _, err := s.Publish(context.Background(), signed(t, priv, fmt.Sprintf("plugin-%d", i), "1.0.0", "creator")); err != nil {
			t.Fatal(err)
		}
	}

	resp, err := http.Get(server.URL + "/v1/marketplace/plugins?limit=2&offset=1")
	if err != nil {
		t.Fatalf("list request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
	var items []marketplace.Metadata
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 || items[0].ID != "plugin-1" || items[1].ID != "plugin-2" {
		t.Fatalf("unexpected page: %+v", items)
	}
	if resp.Header.Get("X-Total-Count") != "5" || resp.Header.Get("X-Next-Offset") != "3" {
		t.Fatalf("unexpected pagination headers: %v", resp.Header)
	}

	for _, q := range []string{"limit=0", "limit=1000", "limit=abc", "offset=-1", "creator_id=../x"} {
		r, err := http.Get(server.URL + "/v1/marketplace/plugins?" + q)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", q, r.StatusCode)
		}
	}

	empty, err := http.Get(server.URL + "/v1/marketplace/plugins?offset=100")
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Body.Close()
	var none []marketplace.Metadata
	if err := json.NewDecoder(empty.Body).Decode(&none); err != nil || none == nil || len(none) != 0 {
		t.Fatalf("expected empty JSON array past the end, got %v %v", none, err)
	}
}

func TestRegistryServiceMissingFields(t *testing.T) {
	_, server := newRegistryServer(t)
	body := `{"id":"plugin-1","name":"Test","version":"1.0.0","creator_id":"","wasm_hash":"abc","public_key":"key","signature":"sig"}`
	if resp := post(t, server.URL+"/v1/marketplace/plugins", body); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestRegistryServiceMethodsAndPaths(t *testing.T) {
	_, server := newRegistryServer(t)
	cases := []struct {
		method, path string
		want         int
		allow        string
	}{
		{http.MethodDelete, "/v1/marketplace/plugins", http.StatusMethodNotAllowed, "GET, HEAD, POST"},
		{http.MethodPut, "/v1/marketplace/plugins/p1", http.StatusMethodNotAllowed, "GET, HEAD"},
		{http.MethodGet, "/v1/marketplace/openstandard/capability", http.StatusMethodNotAllowed, "POST"},
		{http.MethodGet, "/v1/marketplace/plugins/missing", http.StatusNotFound, ""},
		{http.MethodGet, "/v1/marketplace/plugins/", http.StatusBadRequest, ""},
		{http.MethodGet, "/v1/marketplace/plugins/a/b", http.StatusBadRequest, ""},
		{http.MethodGet, "/v1/marketplace/plugins/a:b", http.StatusBadRequest, ""},
	}
	for _, c := range cases {
		req, _ := http.NewRequest(c.method, server.URL+c.path, nil)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != c.want {
			t.Errorf("%s %s: expected %d, got %d", c.method, c.path, c.want, resp.StatusCode)
		}
		if c.allow != "" && resp.Header.Get("Allow") != c.allow {
			t.Errorf("%s %s: Allow=%q want %q", c.method, c.path, resp.Header.Get("Allow"), c.allow)
		}
		if resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s %s: expected JSON error body", c.method, c.path)
		}
	}
}

func TestPluginByIDHandlerMountedElsewhereDoesNotPanic(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	rec := httptest.NewRecorder()
	s.PluginByIDHandler()(rec, httptest.NewRequest(http.MethodGet, "/x", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", rec.Code)
	}
}

type failingStore struct{ marketplace.Store }

func (failingStore) List(context.Context) ([]marketplace.Metadata, error) {
	return nil, errors.New("redis: connection refused 10.0.0.5:6379")
}

func (failingStore) Lookup(context.Context, string) (marketplace.VerifiedManifest, marketplace.Metadata, error) {
	return marketplace.VerifiedManifest{}, marketplace.Metadata{}, errors.New("redis: connection refused 10.0.0.5:6379")
}

func (f failingStore) Get(ctx context.Context, id string) (marketplace.VerifiedManifest, marketplace.Metadata, bool) {
	return marketplace.VerifiedManifest{}, marketplace.Metadata{}, false
}

func TestRegistryServiceStoreErrorsAreNotLeaked(t *testing.T) {
	s := marketplace.NewRegistryService(nil, failingStore{})
	for _, path := range []string{"/v1/marketplace/plugins", "/v1/marketplace/plugins/p1"} {
		rec := httptest.NewRecorder()
		h := s.PluginsHandler()
		if strings.Count(path, "/") > 3 {
			h = s.PluginByIDHandler()
		}
		h(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusInternalServerError {
			t.Fatalf("%s: expected 500, got %d", path, rec.Code)
		}
		if strings.Contains(rec.Body.String(), "10.0.0.5") {
			t.Fatalf("%s: internal error leaked: %s", path, rec.Body.String())
		}
	}
}

func TestVerifyManifestNotFound(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	_, err := s.VerifyManifest(context.Background(), "missing")
	if !errors.Is(err, marketplace.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestVerifyManifestDetectsStoreTampering(t *testing.T) {
	store := marketplace.NewInMemoryStore()
	s := marketplace.NewRegistryService(nil, store)
	if _, _, err := s.Publish(context.Background(), signed(t, newKey(t), "plugin-1", "1.0.0", "alice")); err != nil {
		t.Fatal(err)
	}
	m, meta, _ := store.Get(context.Background(), "plugin-1")
	m.WASMHash = marketplace.HashWASM([]byte("evil"))
	if err := store.Put(context.Background(), m, meta); err != nil {
		t.Fatal(err)
	}
	if _, err := s.VerifyManifest(context.Background(), "plugin-1"); err == nil {
		t.Fatal("expected tampered stored manifest to fail verification")
	}
}
