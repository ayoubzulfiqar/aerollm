package pqc

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func call(t *testing.T, h http.HandlerFunc, method, body string) (*httptest.ResponseRecorder, KeyResponse) {
	t.Helper()
	var r *http.Request
	if body == "" {
		r = httptest.NewRequest(method, "/handshake", nil)
	} else {
		r = httptest.NewRequest(method, "/handshake", strings.NewReader(body))
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	var resp KeyResponse
	_ = json.Unmarshal(w.Body.Bytes(), &resp)
	return w, resp
}

func TestHandshakeHandlerReturnsPublicKey(t *testing.T) {
	km := NewQuantumSafeKeyManager(AlgorithmHybridEd25519MLDSA65)
	h := HandshakeHandler(km)
	w, resp := call(t, h, http.MethodGet, "")
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected json content type")
	}
	if len(resp.PublicKey) == 0 || resp.PostQuantumSignature || !resp.PostQuantumKEM {
		t.Fatalf("unexpected response %+v", resp)
	}
	// The server identity is stable (no per-request key generation).
	_, resp2 := call(t, h, http.MethodPost, "")
	if !bytes.Equal(resp.PublicKey, resp2.PublicKey) {
		t.Fatal("server key regenerated per request")
	}
	body := w.Body.String()
	if strings.Contains(body, "shared_secret") || strings.Contains(body, "private") {
		t.Fatalf("response leaks secrets: %s", body)
	}
}

func TestHandshakeHandlerMethodsAndLimits(t *testing.T) {
	h := HandshakeHandler(NewQuantumSafeKeyManager(AlgorithmHybridMLKEM768X25519Ed25519))
	w, _ := call(t, h, http.MethodDelete, "")
	if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != "GET, POST" {
		t.Fatalf("expected 405 with Allow, got %d %q", w.Code, w.Header().Get("Allow"))
	}
	w, _ = call(t, h, http.MethodPost, `{"public_key":"`+strings.Repeat("A", 70<<10)+`"}`)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", w.Code)
	}
	w, _ = call(t, h, http.MethodPost, `{bad json`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
	w, _ = call(t, h, http.MethodPost, `{"public_key":"c2hvcnQ="}`)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("invalid key: expected 400, got %d", w.Code)
	}
}

func TestHandshakeHandlerErrors(t *testing.T) {
	w, _ := call(t, HandshakeHandler(NewQuantumSafeKeyManager(AlgorithmPQCMLDSA65)), http.MethodGet, "")
	if w.Code != http.StatusNotImplemented {
		t.Fatalf("ML-DSA: expected 501, got %d", w.Code)
	}
	w, _ = call(t, HandshakeHandler(NewQuantumSafeKeyManager("bogus")), http.MethodGet, "")
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("bogus: expected 500, got %d", w.Code)
	}
	w, _ = call(t, HandshakeHandler(nil), http.MethodGet, "")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil km: expected 503, got %d", w.Code)
	}
	// ML-KEM-only suite works (KEM without signatures).
	w, resp := call(t, HandshakeHandler(NewQuantumSafeKeyManager(AlgorithmPQCMLKEM768)), http.MethodGet, "")
	if w.Code != http.StatusOK || resp.SignatureAlgorithm != "" {
		t.Fatalf("mlkem: %d %+v", w.Code, resp)
	}
}

func TestHandshakeKeyExchangeBothDirections(t *testing.T) {
	ctx := context.Background()
	server := NewQuantumSafeKeyManager(AlgorithmHybridMLKEM768X25519Ed25519)
	client := NewQuantumSafeKeyManager(AlgorithmHybridMLKEM768X25519Ed25519)
	h := HandshakeHandler(server)

	// Flow 1: server encapsulates to the client's key.
	cPub, cPriv, _ := client.GenerateKeyPair(ctx)
	reqBody, _ := json.Marshal(HandshakeRequest{PublicKey: cPub})
	w, resp := call(t, h, http.MethodPost, string(reqBody))
	if w.Code != http.StatusOK || resp.SessionID == "" || len(resp.Ciphertext) == 0 {
		t.Fatalf("flow1: %d %s", w.Code, w.Body.String())
	}
	if err := VerifyHandshake(ctx, client, &resp, cPub, resp.Ciphertext); err != nil {
		t.Fatalf("flow1 transcript signature: %v", err)
	}
	clientSecret, err := client.Decapsulate(ctx, resp.Ciphertext, cPriv)
	if err != nil {
		t.Fatal(err)
	}
	serverSecret, ok := server.TakeSessionSecret(resp.SessionID)
	if !ok || !bytes.Equal(clientSecret, serverSecret) {
		t.Fatal("flow1: secrets differ")
	}
	if _, ok := server.TakeSessionSecret(resp.SessionID); ok {
		t.Fatal("session secret must be single-use")
	}

	// Flow 2: client encapsulates to the server's key.
	_, id := call(t, h, http.MethodGet, "")
	ct, secret, err := client.Encapsulate(ctx, id.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	reqBody, _ = json.Marshal(HandshakeRequest{Ciphertext: ct})
	w, resp = call(t, h, http.MethodPost, string(reqBody))
	if w.Code != http.StatusOK {
		t.Fatalf("flow2: %d %s", w.Code, w.Body.String())
	}
	if err := VerifyHandshake(ctx, client, &resp, nil, ct); err != nil {
		t.Fatalf("flow2 transcript signature: %v", err)
	}
	got, ok := server.TakeSessionSecret(resp.SessionID)
	if !ok || !bytes.Equal(got, secret) {
		t.Fatal("flow2: secrets differ")
	}
}
