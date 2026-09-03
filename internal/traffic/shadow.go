package traffic

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// ShadowConfig holds shadow testing configuration.
type ShadowConfig struct {
	Enabled      bool
	ShadowModels []string
}

// ShadowResult captures shadow execution metrics.
type ShadowResult struct {
	Provider string
	Latency  time.Duration
	Error    error
}

// ShadowTester runs asynchronous shadow requests against secondary providers.
type ShadowTester struct {
	client *http.Client
	mu     sync.RWMutex
}

// NewShadowTester creates a new shadow tester.
func NewShadowTester() *ShadowTester {
	return &ShadowTester{client: &http.Client{Timeout: 30 * time.Second}}
}

// RunAsync sends the same payload to a shadow provider asynchronously.
func (s *ShadowTester) RunAsync(ctx context.Context, shadowURL, apiKey string, req interface{}) error {
	var (
		latency time.Duration
		err     error
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		start := time.Now()
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequestWithContext(ctx, http.MethodPost, shadowURL+"/v1/chat/completions", bytes.NewReader(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Authorization", "Bearer "+apiKey)
		resp, e := s.client.Do(httpReq)
		latency = time.Since(start)
		if resp != nil {
			resp.Body.Close()
		}
		err = e
	}()

	select {
	case <-done:
	case <-ctx.Done():
		return ctx.Err()
	}

	result := ShadowResult{Provider: shadowURL, Latency: latency, Error: err}
	s.mu.Lock()
	_ = result
	s.mu.Unlock()
	return err
}
