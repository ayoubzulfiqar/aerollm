package notification

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNotificationStore(t *testing.T) {
	store := NewStore()
	store.UpsertChannel(Channel{ID: "c1", Name: "ops-webhook", Type: ChannelWebhook, Target: "https://example.com/alerts", Enabled: true})
	if _, ok := store.GetChannel("c1"); !ok {
		t.Fatalf("expected channel c1")
	}
	if len(store.ListChannels()) != 1 {
		t.Fatalf("expected 1 channel, got %d", len(store.ListChannels()))
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/notification/channels", WebhookHandler(store))
	req := httptest.NewRequest(http.MethodGet, "/v1/notification/channels", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"type":"webhook"`) {
		t.Fatalf("expected webhook type in body, got: %s", rec.Body.String())
	}
}
