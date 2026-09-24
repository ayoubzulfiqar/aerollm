package zk

import (
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type fakeTEE struct{ err error }

func (f fakeTEE) Evaluate(_ context.Context, ct []byte) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := make([]byte, len(ct))
	for i, b := range ct {
		out[i] = b ^ 0xff
	}
	return out, nil
}

func serve(t *testing.T, c ConfidentialCompute, header string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	called := false
	h := Middleware(c)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		called = true
		w.WriteHeader(http.StatusOK)
	}))
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	if header != "" {
		r.Header.Set(HeaderEncryptedPayload, header)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w, called
}

func TestPassThroughWithoutHeader(t *testing.T) {
	if w, called := serve(t, nil, ""); !called || w.Code != http.StatusOK {
		t.Fatalf("expected pass-through, got %d", w.Code)
	}
}

func TestNoBackendIsHonest(t *testing.T) {
	payload := base64.StdEncoding.EncodeToString([]byte("attacker-controlled"))
	w, called := serve(t, nil, payload)
	if called || w.Code != http.StatusNotImplemented {
		t.Fatalf("expected 501 without backend, got %d (called=%v)", w.Code, called)
	}
	if strings.Contains(w.Body.String(), payload) {
		t.Fatal("payload must not be echoed back")
	}
}

func TestBackendRoundTripAndErrors(t *testing.T) {
	w, _ := serve(t, fakeTEE{}, base64.StdEncoding.EncodeToString([]byte{1, 2, 3}))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), base64.StdEncoding.EncodeToString([]byte{0xfe, 0xfd, 0xfc})) {
		t.Fatalf("round trip: %d %s", w.Code, w.Body.String())
	}
	if w, _ := serve(t, fakeTEE{}, "!!!not-base64"); w.Code != http.StatusBadRequest {
		t.Fatalf("bad base64: %d", w.Code)
	}
	big := base64.StdEncoding.EncodeToString(make([]byte, MaxCiphertextBytes+1))
	if w, _ := serve(t, fakeTEE{}, big); w.Code != http.StatusBadRequest {
		t.Fatalf("oversized: %d", w.Code)
	}
	w, _ = serve(t, fakeTEE{err: errors.New("enclave at 10.0.0.5 exploded")}, base64.StdEncoding.EncodeToString([]byte{1}))
	if w.Code != http.StatusBadGateway || strings.Contains(w.Body.String(), "10.0.0.5") {
		t.Fatalf("backend errors must not leak internals: %d %s", w.Code, w.Body.String())
	}
}
