package pqc

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandshakeHandlerReturnsPublicKey(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	h := HandshakeHandler(km)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/handshake", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected json content type")
	}
}

func TestHandshakeHandlerServerErrorOnBadAlgorithm(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmPQCMLKEM768)
	h := HandshakeHandler(km)
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/handshake", nil)
	h.ServeHTTP(w, r)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
}
