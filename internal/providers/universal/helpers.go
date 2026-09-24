package universal

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func jsonUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// EventStreamReader reads SSE payloads from an HTTP body with bounded memory.
type EventStreamReader struct {
	r *providers.SSEReader
}

// NewEventStreamReader creates a new reader.
func NewEventStreamReader(r io.Reader) *EventStreamReader {
	return &EventStreamReader{r: providers.NewSSEReader(r)}
}

// ReadEvent returns the data of the next event (multi-line data joined with
// "\n"). It returns io.EOF at the end of the stream.
func (r *EventStreamReader) ReadEvent() (string, error) {
	for {
		ev, err := r.r.Next()
		if err != nil {
			return "", err
		}
		if strings.TrimSpace(ev.Data) != "" {
			return ev.Data, nil
		}
	}
}

// toAeroStream adapts an OpenAI-style chunk stream to the legacy
// AeroStreamChunk format.
func toAeroStream(ctx context.Context, provider string, in <-chan models.StreamChunk) <-chan AeroStreamChunk {
	out := make(chan AeroStreamChunk)
	go func() {
		defer close(out)
		for c := range in {
			ac := AeroStreamChunk{Provider: provider, ContentType: "text/event-stream", Err: c.Err}
			for _, ch := range c.Choices {
				ac.Delta += ch.Delta.Content
				if ch.FinishReason != nil && *ch.FinishReason != "" {
					ac.Finish = true
				}
			}
			if c.Err != nil {
				ac.Finish = true
			} else {
				ac.Data, _ = json.Marshal(c)
			}
			select {
			case out <- ac:
			case <-ctx.Done():
				return // the producer observes ctx too and exits
			}
		}
	}()
	return out
}

// singleResponseStream serves Stream() for adapters that cannot stream by
// running a non-streaming completion.
func singleResponseStream(ctx context.Context, provider string, call func() (*models.LLMResponse, error)) <-chan AeroStreamChunk {
	out := make(chan AeroStreamChunk, 1)
	go func() {
		defer close(out)
		resp, err := call()
		var ac AeroStreamChunk
		if err != nil {
			ac = AeroStreamChunk{Provider: provider, Finish: true, Err: err}
		} else {
			b, _ := json.Marshal(resp)
			text := ""
			if len(resp.Choices) > 0 {
				text = resp.Choices[0].Message.TextContent()
			}
			ac = AeroStreamChunk{Provider: provider, Delta: text, Finish: true, ContentType: "application/json", Data: b}
		}
		select {
		case out <- ac:
		case <-ctx.Done():
		}
	}()
	return out
}

// healthMap renders a providers.HealthTracker snapshot in the adapter
// Health() map format.
func healthMap(name, typ string, h *providers.HealthTracker, configErr error) map[string]interface{} {
	s := h.Snapshot(name, providers.ProviderType(typ))
	m := map[string]interface{}{
		"name":         name,
		"type":         typ,
		"healthy":      s.Healthy && configErr == nil,
		"latency_ms":   s.LatencyMs,
		"failures":     s.Failures,
		"last_checked": s.LastChecked,
	}
	if configErr != nil {
		m["error"] = configErr.Error()
	}
	return m
}

// configError wraps an adapter misconfiguration as a non-retryable-by-client
// upstream error (500) that never includes credentials.
func configError(provider string, err error) error {
	return &providers.UpstreamError{Provider: provider, StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: err.Error()}
}

// timed runs fn and records its outcome on h.
func timed[T any](h *providers.HealthTracker, fn func() (T, error)) (T, error) {
	start := time.Now()
	v, err := fn()
	h.Observe(time.Since(start), err)
	return v, err
}

var errNilRequest = errors.New("request is nil")
