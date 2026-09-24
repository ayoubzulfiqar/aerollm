package k8s

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// MaxConfigBytes bounds a single config payload read from a file or HTTP
// endpoint so a runaway or hostile source cannot exhaust memory.
const MaxConfigBytes = 10 << 20

// ErrConfigTooLarge is returned when a config payload exceeds MaxConfigBytes.
var ErrConfigTooLarge = fmt.Errorf("config payload exceeds %d bytes", MaxConfigBytes)

// FileConfigSource watches a local file and emits its contents on changes.
type FileConfigSource struct {
	Path     string
	Interval time.Duration
}

func (f *FileConfigSource) interval() time.Duration {
	if f.Interval > 0 {
		return f.Interval
	}
	return 2 * time.Second
}

// Name returns the source name.
func (f *FileConfigSource) Name() string { return "file:" + f.Path }

// Run watches the file and streams updates. A missing or unreadable file at
// startup is an error; transient read errors afterwards keep the last
// emitted payload.
func (f *FileConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	if f.Path == "" {
		return errors.New("file config source: missing path")
	}
	initial, err := readFileLimited(f.Path)
	if err != nil {
		return fmt.Errorf("file config source: %w", err)
	}
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
			b, err := readFileLimited(f.Path)
			if err != nil || bytes.Equal(b, last) {
				continue
			}
			last = b
			select {
			case <-ctx.Done():
				return nil
			case updates <- last:
			}
		}
	}
}

func readFileLimited(path string) ([]byte, error) {
	fh, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer fh.Close()
	return readLimited(fh)
}

func readLimited(r io.Reader) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, MaxConfigBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > MaxConfigBytes {
		return nil, ErrConfigTooLarge
	}
	return b, nil
}

// HTTPConfigSource polls an HTTP endpoint for config payloads. Only 2xx
// responses are treated as config; the body is capped at MaxConfigBytes.
// The URL is operator-supplied configuration and is trusted.
type HTTPConfigSource struct {
	URL      string
	Interval time.Duration
	// Client overrides the HTTP client (default: 5s timeout, no redirects to
	// other hosts).
	Client *http.Client
}

// Name returns the source name.
func (h *HTTPConfigSource) Name() string { return "http:" + h.URL }

func (h *HTTPConfigSource) interval() time.Duration {
	if h.Interval > 0 {
		return h.Interval
	}
	return 3 * time.Second
}

func (h *HTTPConfigSource) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return &http.Client{
		Timeout: 5 * time.Second,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("too many redirects")
			}
			if req.URL.Host != via[0].URL.Host {
				return errors.New("redirect to a different host refused")
			}
			return nil
		},
	}
}

// fetch performs one GET and returns the body of a 2xx response.
func (h *HTTPConfigSource) fetch(ctx context.Context, client *http.Client) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, h.URL, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		return nil, fmt.Errorf("http config source: unexpected status %d", resp.StatusCode)
	}
	return readLimited(resp.Body)
}

// Run polls the HTTP endpoint and streams the body whenever it changes.
// Fetch errors (network failures, non-2xx statuses, oversized bodies) are
// skipped and retried on the next tick.
func (h *HTTPConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	if h.URL == "" {
		return errors.New("http config source: missing url")
	}
	if _, err := http.NewRequest(http.MethodGet, h.URL, nil); err != nil {
		return fmt.Errorf("http config source: %w", err)
	}
	client := h.client()
	ticker := time.NewTicker(h.interval())
	defer ticker.Stop()
	var last []byte
	emitted := false
	poll := func() bool {
		b, err := h.fetch(ctx, client)
		if err != nil || (emitted && bytes.Equal(b, last)) {
			return true
		}
		last, emitted = b, true
		select {
		case <-ctx.Done():
			return false
		case updates <- b:
			return true
		}
	}
	if !poll() {
		return nil
	}
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if !poll() {
				return nil
			}
		}
	}
}

// InMemoryConfigSource emits predefined payloads for tests.
type InMemoryConfigSource struct {
	Payloads [][]byte
	mu       sync.Mutex
}

// NewInMemoryConfigSource creates an in-memory config source.
func NewInMemoryConfigSource(payloads ...[]byte) *InMemoryConfigSource {
	if payloads == nil {
		payloads = [][]byte{[]byte("{}")}
	}
	return &InMemoryConfigSource{Payloads: payloads}
}

// Name returns the source name.
func (s *InMemoryConfigSource) Name() string { return "inmemory" }

// Run streams in-memory payloads.
func (s *InMemoryConfigSource) Run(ctx context.Context, updates chan<- []byte) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	payloads := append([][]byte(nil), s.Payloads...)
	s.mu.Unlock()
	for _, p := range payloads {
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
	if p := os.Getenv("AEROLLM_OPERATOR_CONFIG"); p != "" {
		return p
	}
	return filepath.Join(".", "operator-config.json")
}
