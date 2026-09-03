package main

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/pqc"
)

func TestStreamEncrypterRoundTrip(t *testing.T) {
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)
	if _, _, err := km.GenerateKeyPair(nil); err != nil { t.Fatalf("keygen: %v", err) }
	shared := []byte("pqc-stream-secret")
	payload := []byte(`{"type":"spatial_anchor","x":1,"y":2,"z":3}`)

	src := io.NopCloser(bytes.NewReader(payload))
	enc := pqc.NewStreamEncrypter(shared, src)
	buf := new(bytes.Buffer)
	if _, err := io.Copy(buf, enc); err != nil { t.Fatalf("copy enc: %v", err) }
	if string(buf.Bytes()) == string(payload) { t.Fatalf("expected transformed stream") }
	if err := enc.Close(); err != nil { t.Fatalf("close enc: %v", err) }

	var out bytes.Buffer
	dec := pqc.NewStreamDecrypter(shared, &out)
	if _, err := dec.Write(buf.Bytes()); err != nil { t.Fatalf("write dec: %v", err) }
	if string(out.Bytes()) != string(payload) { t.Fatalf("round trip failed: %s", out.Bytes()) }
}

func TestEdgePQCHandshakeRoute(t *testing.T) {
	mux := http.NewServeMux()
	km := pqc.NewQuantumSafeKeyManager(pqc.AlgorithmHybridEd25519MLDSA65)
	mux.HandleFunc("/v1/edge/pqc/handshake", pqc.HandshakeHandler(km))

	srv := httptest.NewServer(mux)
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/edge/pqc/handshake", nil)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatalf("request: %v", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { t.Fatalf("status=%d", resp.StatusCode) }

	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), `"algorithm":"hybrid-ed25519+mldsa-65"`) {
		t.Fatalf("unexpected body: %s", string(b))
	}
}

func TestEdgeSpatialStreamRoute(t *testing.T) {
	mux := http.NewServeMux()
	handler := &fakeVideo3DStreamHandler{}
	mux.HandleFunc("/v1/edge/spatial/stream", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		handler.StreamResponse(w, r, r.Body)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	body := `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":0.1}`
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/edge/spatial/stream", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatalf("request: %v", err) }
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK { t.Fatalf("status=%d", resp.StatusCode) }
	if !strings.Contains(body, handler.payload) { t.Fatalf("stream body mismatch: %s", handler.payload) }
}

type fakeVideo3DStreamHandler struct{ payload string }
func (f *fakeVideo3DStreamHandler) StreamResponse(_ http.ResponseWriter, _ *http.Request, body io.Reader) {
	b, _ := io.ReadAll(body)
	f.payload = string(b)
}
