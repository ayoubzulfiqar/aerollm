package spatial

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// SpatialAnchor represents a parsed spatial object from LLM output.
type SpatialAnchor struct {
	Type      string      `json:"type"`
	X         float64     `json:"x"`
	Y         float64     `json:"y"`
	Z         float64     `json:"z"`
	Raw       interface{} `json:"raw,omitempty"`
}

// WebXRPayload is the standardized AR/VR payload.
type WebXRPayload struct {
	Version   string          `json:"version"`
	Anchors   []SpatialAnchor `json:"anchors"`
	SessionID string          `json:"session_id,omitempty"`
}

// ParseSpatialAnchors scans text for spatial anchor JSON objects.
func ParseSpatialAnchors(text string) []SpatialAnchor {
	var out []SpatialAnchor
	scanner := bufio.NewScanner(strings.NewReader(text))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if !strings.Contains(line, `"type":"spatial_anchor"`) && !strings.Contains(line, `"type": "spatial_anchor"`) {
			continue
		}
		var candidate map[string]interface{}
		if err := json.Unmarshal([]byte(line), &candidate); err != nil {
			continue
		}
		anchor := SpatialAnchor{Raw: candidate}
		if t, ok := candidate["type"].(string); ok {
			anchor.Type = t
		}
		if x, ok := candidate["x"].(float64); ok {
			anchor.X = x
		}
		if y, ok := candidate["y"].(float64); ok {
			anchor.Y = y
		}
		if z, ok := candidate["z"].(float64); ok {
			anchor.Z = z
		}
		out = append(out, anchor)
	}
	return out
}

// ToWebXR converts anchors to WebXR payload.
func ToWebXR(anchors []SpatialAnchor, sessionID string) WebXRPayload {
	return WebXRPayload{Version: "1.0", Anchors: anchors, SessionID: sessionID}
}

// StreamChunker splits an io.Reader into fixed-size chunks without full buffering.
type StreamChunker struct {
	chunkSize int
	reader    io.Reader
}

// NewStreamChunker creates a new chunker.
func NewStreamChunker(r io.Reader, chunkSize int) *StreamChunker {
	return &StreamChunker{chunkSize: chunkSize, reader: r}
}

// Chunks yields byte chunks from the underlying reader.
func (c *StreamChunker) Chunks(ctx context.Context) <-chan []byte {
	out := make(chan []byte)
	go func() {
		defer close(out)
		buf := make([]byte, c.chunkSize)
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			n, err := c.reader.Read(buf)
			if n > 0 {
				select {
				case out <- append([]byte(nil), buf[:n]...):
				case <-ctx.Done():
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	return out
}

// Video3DStreamHandler streams provider responses with zero-copy chunked semantics.
type Video3DStreamHandler struct{}

// NewVideo3DStreamHandler creates a new handler.
func NewVideo3DStreamHandler() *Video3DStreamHandler {
	return &Video3DStreamHandler{}
}

// StreamResponse writes chunked provider payload to the response writer.
func (h *Video3DStreamHandler) StreamResponse(w http.ResponseWriter, r *http.Request, body io.Reader) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, `{"error":"streaming unsupported"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	chunker := NewStreamChunker(body, 64*1024)
	for chunk := range chunker.Chunks(r.Context()) {
		if _, err := w.Write(chunk); err != nil {
			return
		}
		flusher.Flush()
	}
}
