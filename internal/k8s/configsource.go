package k8s

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileConfigSource watches a local file and emits its contents on changes.
type FileConfigSource struct {
	Path     string
	Interval time.Duration
}

func (f *FileConfigSource) interval() time.Duration {
	if f.Interval > 0 { return f.Interval }
	return 2 * time.Second
}

// Name returns the source name.
func (f *FileConfigSource) Name() string { return "file:" + f.Path }

// Run watches the file and streams updates.
func (f *FileConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	if f.Path == "" { return fmt.Errorf("file config source: missing path") }
	initial, err := os.ReadFile(f.Path)
	if err != nil { return err }
	ticker := time.NewTicker(f.interval())
	defer ticker.Stop()
	last := initial
	select {
	case <-ctx.Done():
		return nil
	case updates <- last:
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			b, err := os.ReadFile(f.Path)
			if err != nil { continue }
			if string(b) == string(last) { continue }
			last = b
			select {
			case <-ctx.Done():
				return nil
			case updates <- last:
			}
		}
	}
}

// HTTPConfigSource polls an HTTP endpoint for config payloads.
type HTTPConfigSource struct {
	URL string
}

// Name returns the source name.
func (h *HTTPConfigSource) Name() string { return "http:" + h.URL }

// Run polls the HTTP endpoint and streams the body.
func (h *HTTPConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	if h.URL == "" { return fmt.Errorf("http config source: missing url") }
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()
	client := &http.Client{Timeout: 2 * time.Second}
	var last []byte
	emitIfChanged := func(b []byte) bool {
		if string(b) == string(last) { return false }
		last = b
		select {
		case <-ctx.Done():
			return false
		case updates <- b:
			return true
		}
	}
	emitInitial := func() bool {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
		if err != nil { return false }
		resp, err := client.Do(req)
		if err != nil { return false }
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return emitIfChanged(b)
	}
	if !emitInitial() {
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if emitInitial() { break }
			}
		}
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			emitInitial()
		}
	}
}

// InMemoryConfigSource emits predefined payloads for tests.
type InMemoryConfigSource struct {
	Payloads [][]byte
	mu       sync.Mutex
	idx      int
}

// NewInMemoryConfigSource creates an in-memory config source.
func NewInMemoryConfigSource(payloads ...[]byte) *InMemoryConfigSource {
	if payloads == nil { payloads = [][]byte{[]byte("{}")} }
	return &InMemoryConfigSource{Payloads: payloads}
}

// Name returns the source name.
func (s *InMemoryConfigSource) Name() string { return "inmemory" }

// Run streams in-memory payloads.
func (s *InMemoryConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	if s == nil || len(s.Payloads) == 0 { return nil }
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, p := range s.Payloads {
		select {
		case <-ctx.Done():
			return nil
		case updates <- p:
		}
	}
	return nil
}

// DefaultOperatorConfig returns a default operator configuration path.
func DefaultOperatorConfig() string {
	if p := os.Getenv("AEROLLM_OPERATOR_CONFIG"); p != "" { return p }
	return filepath.Join(".", "operator-config.yaml")
}
