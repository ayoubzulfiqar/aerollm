package webhooks

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
