package spatial

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseSpatialAnchors(t *testing.T) {
	text := `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":-0.3}
not spatial
{"type":"spatial_anchor","x":0,"y":1,"z":2}`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 2 { t.Fatalf("expected 2 anchors, got %d", len(anchors)) }
	if anchors[0].X != 1.2 || anchors[0].Y != 0.5 || anchors[0].Z != -0.3 { t.Fatalf("unexpected first anchor: %v", anchors[0]) }
	if anchors[1].X != 0 || anchors[1].Y != 1 || anchors[1].Z != 2 { t.Fatalf("unexpected second anchor: %v", anchors[1]) }
}

func TestToWebXR(t *testing.T) {
	anchors := []SpatialAnchor{{Type: "spatial_anchor", X: 1, Y: 2, Z: 3}}
	payload := ToWebXR(anchors, "s1")
	if payload.Version != "1.0" { t.Fatalf("unexpected version: %s", payload.Version) }
	if payload.SessionID != "s1" { t.Fatalf("unexpected session id: %s", payload.SessionID) }
	if len(payload.Anchors) != 1 || payload.Anchors[0].X != 1 { t.Fatalf("unexpected anchor: %v", payload.Anchors[0]) }
}

func TestStreamResponseChunks(t *testing.T) {
	payload := "hello world"
	body := strings.NewReader(payload)
	h := NewVideo3DStreamHandler()
	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	h.StreamResponse(w, r, body)
	resp := w.Result()
	if resp.StatusCode != http.StatusOK { t.Fatalf("expected 200, got %d", resp.StatusCode) }
	if resp.Header.Get("Content-Type") != "application/octet-stream" { t.Fatalf("unexpected content type") }
	got := w.Body.String()
	if got != payload { t.Fatalf("unexpected body: %s", got) }
}

func TestStreamChunkerEmpty(t *testing.T) {
	chunker := NewStreamChunker(strings.NewReader(""), 1024)
	chunks := 0
	for range chunker.Chunks(context.Background()) {
		chunks++
	}
	if chunks != 0 { t.Fatalf("expected 0 chunks for empty input, got %d", chunks) }
}

func TestStreamChunkerRespectsContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	chunker := NewStreamChunker(strings.NewReader("abc"), 1)
	cancel()
	for range chunker.Chunks(ctx) {
	}
}
