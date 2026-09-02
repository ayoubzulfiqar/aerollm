package marketplace_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
)

func TestRegistryServicePublishAndGet(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)

	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"id":"plugin-1","name":"Test Plugin","version":"1.0.0","creator_id":"creator","wasm_hash":"abc","public_key":"key","signature":"sig"}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/plugins", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}

	getResp, err := http.Get(server.URL + "/v1/marketplace/plugins/plugin-1")
	if err != nil {
		t.Fatalf("get request failed: %v", err)
	}
	if getResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", getResp.StatusCode)
	}
}

func TestRegistryServiceList(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/marketplace/plugins")
	if err != nil {
		t.Fatalf("list request failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestRegistryServiceMissingFields(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"id":"plugin-1","name":"Test","version":"1.0.0","creator_id":"","wasm_hash":"abc","public_key":"key","signature":"sig"}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/plugins", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("publish request failed: %v", err)
	}
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", resp.StatusCode)
	}
}

func TestVerifyManifestNotFound(t *testing.T) {
	s := marketplace.NewRegistryService(nil, nil)
	_, err := s.VerifyManifest(context.Background(), "missing")
	if err == nil {
		t.Fatal("expected error for missing manifest")
	}
}
