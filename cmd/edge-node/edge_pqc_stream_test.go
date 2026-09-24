package main

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
)

func TestStreamEncrypterRoundTrip(t *testing.T) {
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridMLKEM768X25519Ed25519)
	if _, _, err := km.GenerateKeyPair(nil); err != nil {
		t.Fatalf("keygen: %v", err)
	}
	shared := []byte("pqc-stream-secret")
	payload := []byte(`{"type":"spatial_anchor","x":1,"y":2,"z":3}`)

	src := io.NopCloser(bytes.NewReader(payload))
	enc := pqc.NewStreamEncrypter(shared, src)
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, enc); err != nil {
		t.Fatalf("copy enc: %v", err)
	}
	if string(buf.Bytes()) == string(payload) {
		t.Fatalf("expected transformed stream")
	}
	if err := enc.Close(); err != nil {
		t.Fatalf("close enc: %v", err)
	}

	var out bytes.Buffer
	dec := pqc.NewStreamDecrypter(shared, &out)
	if _, err := dec.Write(buf.Bytes()); err != nil {
		t.Fatalf("write dec: %v", err)
	}
	if string(out.Bytes()) != string(payload) {
		t.Fatalf("round trip failed: %s", out.Bytes())
	}
}

func TestEdgePQCHandshakeRoute(t *testing.T) {
	_, srv := newTestEdge(t, nil)

	resp, body := doReq(t, http.MethodPost, srv.URL+"/v1/edge/pqc/handshake", "", nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
	if !strings.Contains(body, `"algorithm":"hybrid-mlkem768x25519+ed25519"`) {
		t.Fatalf("unexpected body: %s", body)
	}
	if resp, _ := doReq(t, http.MethodGet, srv.URL+"/v1/edge/pqc/handshake", "", nil); resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "POST" {
		t.Fatalf("expected 405 with Allow: POST, got %d", resp.StatusCode)
	}
}

func TestEdgeSpatialStreamRoute(t *testing.T) {
	_, srv := newTestEdge(t, func(c *edgeConfig) { c.maxStreamBytes = 1024 })

	body := `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`
	resp, got := doReq(t, http.MethodPost, srv.URL+"/v1/edge/spatial/stream", body, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status=%d", resp.StatusCode)
	}
	if got != body {
		t.Fatalf("stream body mismatch: %q", got)
	}
	if resp.Header.Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("unexpected content type %q", resp.Header.Get("Content-Type"))
	}

	if resp, _ := doReq(t, http.MethodPost, srv.URL+"/v1/edge/spatial/stream", strings.Repeat("x", 4096), nil); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for oversized declared body, got %d", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodGet, srv.URL+"/v1/edge/spatial/stream", "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}
