package webhooks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
	"sync"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/pkg/telemetry"
)

func TestWebhookDispatcherRegisterAndDispatch(t *testing.T) {
	received := make(chan Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		if err := json.NewDecoder(r.Body).Decode(&evt); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		received <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:     server.URL,
		Secret:  "s",
		Timeout: 2 * time.Second,
	})

	evt := Event{ID: "1", Type: EventBudgetExceeded, Timestamp: time.Now(), Payload: map[string]interface{}{"api_key": "sk-1"}}
	d.DispatchAsync(context.Background(), evt)

	select {
	case got := <-received:
		if got.Type != EventBudgetExceeded {
			t.Fatalf("unexpected event type: %s", got.Type)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("webhook dispatch timed out")
	}
}

func TestWebhookDispatcherError(t *testing.T) {
	before := telemetry.ErrorCount()
	d := NewWebhookDispatcher()
	d.Register(EventShadowTestCompleted, WebhookConfig{URL: "http://127.0.0.1:1", Timeout: 100 * time.Millisecond})
	d.DispatchAsync(context.Background(), Event{ID: "1", Type: EventShadowTestCompleted})
	time.Sleep(300 * time.Millisecond)
	if telemetry.ErrorCount() == before {
		t.Fatal("expected error count to increase")
	}
}

func TestWebhookDispatcherRetrySuccessAfterFailure(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if attempts == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:        server.URL,
		Secret:     "",
		Timeout:    2 * time.Second,
		Retries:    3,
		RetryDelay: 50 * time.Millisecond,
	})

	d.DispatchAsync(context.Background(), Event{ID: "retry-1", Type: EventBudgetExceeded, Timestamp: time.Now()})
	time.Sleep(400 * time.Millisecond)
	if attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", attempts)
	}
}

type fakeWebhookQueue struct {
	events []Event
	mu     sync.Mutex
	idx    int
}

func (f *fakeWebhookQueue) Enqueue(_ context.Context, event Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
	return nil
}

func (f *fakeWebhookQueue) Dequeue(_ context.Context) (Event, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.idx >= len(f.events) {
		return Event{}, fmt.Errorf("no events")
	}
	event := f.events[f.idx]
	f.idx++
	return event, nil
}

func TestWebhookDispatcherStartWorker(t *testing.T) {
	received := make(chan Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		_ = json.NewDecoder(r.Body).Decode(&evt)
		received <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:     server.URL,
		Secret:  "",
		Timeout: 2 * time.Second,
	})

	fq := &fakeWebhookQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	d.StartWorker(ctx, fq)
	_ = fq.Enqueue(ctx, Event{ID: "q-1", Type: EventBudgetExceeded, Timestamp: time.Now()})

	select {
	case got := <-received:
		if got.ID != "q-1" {
			t.Fatalf("expected event ID q-1, got %s", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not dispatch queued event")
	}
	cancel()
}

func TestWebhookDispatcherStartWorkerCancelStops(t *testing.T) {
	received := make(chan Event, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var evt Event
		_ = json.NewDecoder(r.Body).Decode(&evt)
		received <- evt
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	d := NewWebhookDispatcher()
	d.Register(EventBudgetExceeded, WebhookConfig{
		URL:     server.URL,
		Secret:  "",
		Timeout: 2 * time.Second,
	})

	fq := &fakeWebhookQueue{}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	d.StartWorkerWithWaitGroup(ctx, fq, &wg)
	_ = fq.Enqueue(ctx, Event{ID: "q-2", Type: EventBudgetExceeded, Timestamp: time.Now()})

	select {
	case got := <-received:
		if got.ID != "q-2" {
			t.Fatalf("unexpected event ID: %s", got.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not dispatch queued event")
	}
	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}
