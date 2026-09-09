package callbacks

import (
	"context"
	"sync"
	"time"
)

// CallbackHandler defines the interface for observability callbacks.
// Implementations fire asynchronously after a response is sent to the client,
// ensuring zero latency impact on the user request path.
type CallbackHandler interface {
	// OnSuccess is called when a provider call completes successfully.
	OnSuccess(ctx context.Context, req *CallbackRequestData, resp *CallbackResponseData) error
	// OnError is called when a provider call fails.
	OnError(ctx context.Context, req *CallbackRequestData, err error) error
	// Name returns a unique identifier for the callback handler.
	Name() string
}

// CallbackRequestData captures the request details needed for tracing/logging.
type CallbackRequestData struct {
	RequestID   string                 `json:"request_id"`
	Model       string                 `json:"model"`
	Provider    string                 `json:"provider"`
	Messages    []map[string]interface{} `json:"messages"`
	Metadata    map[string]interface{} `json:"metadata"`
	Timestamp   time.Time              `json:"timestamp"`
	CostUSD     float64                `json:"cost_usd"`
}

// CallbackResponseData captures the response details for tracing/logging.
type CallbackResponseData struct {
	ResponseID  string                 `json:"response_id"`
	Usage       map[string]interface{} `json:"usage"`
	Model       string                 `json:"model"`
	Provider    string                 `json:"provider"`
	LatencyMs   int64                  `json:"latency_ms"`
	TokenCount  map[string]int         `json:"token_count"`
	Metadata    map[string]interface{} `json:"metadata"`
	Timestamp   time.Time              `json:"timestamp"`
}

// CallbackManager dispatches callbacks asynchronously after responses are sent.
// It reads the active configuration and fires registered callbacks in goroutines.
type CallbackManager struct {
	handlers []CallbackHandler
	wg       sync.WaitGroup
	timeout  time.Duration
}

// NewCallbackManager creates a new callback manager with the given timeout.
func NewCallbackManager(timeout time.Duration) *CallbackManager {
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return &CallbackManager{
		handlers: make([]CallbackHandler, 0),
		timeout:  timeout,
	}
}

// Register adds a callback handler to the manager.
func (m *CallbackManager) Register(handler CallbackHandler) {
	m.handlers = append(m.handlers, handler)
}

// FireSuccess dispatches OnSuccess to all registered handlers asynchronously.
// The response is already sent to the client by the caller — these goroutines
// fire in the background with a timeout context.
func (m *CallbackManager) FireSuccess(req *CallbackRequestData, resp *CallbackResponseData) {
	for _, h := range m.handlers {
		m.wg.Add(1)
		go func(handler CallbackHandler) {
			defer m.wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
			defer cancel()
			if err := handler.OnSuccess(ctx, req, resp); err != nil {
				// Log error but don't block the user request path.
			}
		}(h)
	}
}

// FireError dispatches OnError to all registered handlers asynchronously.
func (m *CallbackManager) FireError(req *CallbackRequestData, err error) {
	for _, h := range m.handlers {
		m.wg.Add(1)
		go func(handler CallbackHandler) {
			defer m.wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
			defer cancel()
			_ = handler.OnError(ctx, req, err)
		}(h)
	}
}

// Wait blocks until all pending callbacks complete (useful for graceful shutdown).
func (m *CallbackManager) Wait() {
	m.wg.Wait()
}

// NoOpCallback is a callback handler that does nothing.
// Useful as a default when no callbacks are configured.
type NoOpCallback struct{}

// OnSuccess implements CallbackHandler.
func (n *NoOpCallback) OnSuccess(_ context.Context, _ *CallbackRequestData, _ *CallbackResponseData) error {
	return nil
}

// OnError implements CallbackHandler.
func (n *NoOpCallback) OnError(_ context.Context, _ *CallbackRequestData, _ error) error {
	return nil
}

// Name implements CallbackHandler.
func (n *NoOpCallback) Name() string { return "noop" }

// Ensure the interface is satisfied at compile time.
var _ CallbackHandler = (*NoOpCallback)(nil)
