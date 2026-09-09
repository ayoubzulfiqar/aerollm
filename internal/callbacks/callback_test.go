package callbacks

import (
	"context"
	"errors"
	"testing"
	"time"
)

// mockCallback is a test implementation of CallbackHandler.
type mockCallback struct {
	name      string
	successCount int
	errorCount   int
	mu        chan struct{}
}

func newMockCallback(name string) *mockCallback {
	return &mockCallback{
		name: name,
		mu:   make(chan struct{}, 100),
	}
}

func (m *mockCallback) OnSuccess(_ context.Context, _ *CallbackRequestData, _ *CallbackResponseData) error {
	m.mu <- struct{}{}
	m.successCount++
	return nil
}

func (m *mockCallback) OnError(_ context.Context, _ *CallbackRequestData, _ error) error {
	m.mu <- struct{}{}
	m.errorCount++
	return nil
}

func (m *mockCallback) Name() string { return m.name }

// TestCallbackManagerRegister verifies that registered handlers are called.
func TestCallbackManagerRegister(t *testing.T) {
	mgr := NewCallbackManager(5 * time.Second)
	mock := newMockCallback("test_handler")
	mgr.Register(mock)

	req := &CallbackRequestData{
		RequestID: "req_123",
		Model:     "gpt-4o",
		Provider:  "openai",
		Timestamp: time.Now(),
	}
	resp := &CallbackResponseData{
		ResponseID: "resp_123",
		LatencyMs:  150,
		TokenCount: map[string]int{"input": 10, "output": 20},
	}

	mgr.FireSuccess(req, resp)
	mgr.Wait()

	if mock.successCount != 1 {
		t.Errorf("expected 1 success call, got %d", mock.successCount)
	}
}

// TestCallbackManagerError verifies error callbacks are dispatched.
func TestCallbackManagerError(t *testing.T) {
	mgr := NewCallbackManager(5 * time.Second)
	mock := newMockCallback("error_handler")
	mgr.Register(mock)

	req := &CallbackRequestData{
		RequestID: "req_456",
		Model:     "claude-3",
		Provider:  "anthropic",
		Timestamp: time.Now(),
	}

	mgr.FireError(req, errors.New("rate limit exceeded"))
	mgr.Wait()

	if mock.errorCount != 1 {
		t.Errorf("expected 1 error call, got %d", mock.errorCount)
	}
	if mock.successCount != 0 {
		t.Errorf("expected 0 success calls, got %d", mock.successCount)
	}
}

// TestNoOpCallback verifies the no-op callback works correctly.
func TestNoOpCallback(t *testing.T) {
	noop := &NoOpCallback{}
	if noop.Name() != "noop" {
		t.Errorf("expected name 'noop', got %s", noop.Name())
	}
	req := &CallbackRequestData{RequestID: "test"}
	resp := &CallbackResponseData{ResponseID: "test"}
	if err := noop.OnSuccess(context.Background(), req, resp); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if err := noop.OnError(context.Background(), req, errors.New("test")); err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
}

// TestCallbackManagerMultipleHandlers verifies multiple handlers all fire.
func TestCallbackManagerMultipleHandlers(t *testing.T) {
	mgr := NewCallbackManager(5 * time.Second)
	mock1 := newMockCallback("handler_1")
	mock2 := newMockCallback("handler_2")
	mgr.Register(mock1)
	mgr.Register(mock2)

	req := &CallbackRequestData{RequestID: "req_multi", Model: "gpt-4o"}
	resp := &CallbackResponseData{ResponseID: "resp_multi", LatencyMs: 100}

	mgr.FireSuccess(req, resp)
	mgr.Wait()

	if mock1.successCount != 1 {
		t.Errorf("handler 1: expected 1 success, got %d", mock1.successCount)
	}
	if mock2.successCount != 1 {
		t.Errorf("handler 2: expected 1 success, got %d", mock2.successCount)
	}
}
