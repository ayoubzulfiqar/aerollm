package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/notification"
)

func TestNotificationRoutes(t *testing.T) {
	mux := http.NewServeMux()
	store := notification.NewStore()
	mux.HandleFunc("/v1/notification/channels", notification.WebhookHandler(store))
	mux.HandleFunc("/v1/notification/subscriptions", notification.WebhookHandler(store))

	channelBody := `{"id":"c1","name":"ops","type":"webhook","target":"https://example.com/alerts","enabled":true}`
	createReq := httptest.NewRequest(http.MethodPost, "/v1/notification/channels", strings.NewReader(channelBody))
	createReq.Header.Set("Content-Type", "application/json")
	createRec := httptest.NewRecorder()
	mux.ServeHTTP(createRec, createReq)
	if createRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on create channel, got %d: %s", createRec.Code, createRec.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/notification/channels", nil)
	listRec := httptest.NewRecorder()
	mux.ServeHTTP(listRec, listReq)
	if listRec.Code != http.StatusOK {
		t.Fatalf("expected 200 on list channels, got %d: %s", listRec.Code, listRec.Body.String())
	}
	if !strings.Contains(listRec.Body.String(), `"type":"webhook"`) {
		t.Fatalf("expected webhook type in body, got: %s", listRec.Body.String())
	}
}
