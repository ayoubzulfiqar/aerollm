package notification

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func decodeSend(t *testing.T, rec *httptest.ResponseRecorder) SendResponse {
	t.Helper()
	var resp SendResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode %q: %v", rec.Body.String(), err)
	}
	return resp
}

func TestSendHandlerChannelAndAlert(t *testing.T) {
	var got atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		got.Store(string(b))
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "hook", Name: "hook", Type: ChannelWebhook, Target: srv.URL + "/secret-path-token-1234567890abcdef", Enabled: true})
	_, _ = store.UpsertChannel(Channel{ID: "mail", Name: "mail", Type: ChannelEmail, Target: "ops@example.com", Enabled: true})
	_, _ = store.UpsertChannel(Channel{ID: "off", Name: "off", Type: ChannelWebhook, Target: srv.URL, Enabled: false})
	_, _ = store.UpsertSubscription(Subscription{AlertID: "cpu", ChannelID: "hook", Enabled: true})
	_, _ = store.UpsertSubscription(Subscription{AlertID: "mixed", ChannelID: "hook", Enabled: true})
	_, _ = store.UpsertSubscription(Subscription{AlertID: "mixed", ChannelID: "mail", Enabled: true})
	_, _ = store.UpsertSubscription(Subscription{AlertID: "mailonly", ChannelID: "mail", Enabled: true})
	h := SendHandler(testDispatcher(store))

	rec := serve(h, http.MethodPost, SendPath, `{"channel_id":"hook","title":"Deploy","text":"done","severity":"INFO","labels":{"env":"prod"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("channel send: %d %s", rec.Code, rec.Body.String())
	}
	if resp := decodeSend(t, rec); resp.Status != "delivered" || resp.Delivered != 1 || resp.ChannelID != "hook" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	var msg Message
	if err := json.Unmarshal([]byte(got.Load().(string)), &msg); err != nil {
		t.Fatal(err)
	}
	if msg.Title != "Deploy" || msg.Severity != "info" || msg.Labels["env"] != "prod" || msg.AlertID != manualAlertID {
		t.Fatalf("unexpected payload: %+v", msg)
	}

	rec = serve(h, http.MethodPost, SendPath, `{"alert_id":"cpu","text":"hot"}`)
	if rec.Code != http.StatusOK || decodeSend(t, rec).Delivered != 1 {
		t.Fatalf("alert send: %d %s", rec.Code, rec.Body.String())
	}

	rec = serve(h, http.MethodPost, SendPath, `{"alert_id":"mixed","text":"x"}`)
	resp := decodeSend(t, rec)
	if rec.Code != http.StatusBadGateway || resp.Status != "partial" || resp.Delivered != 1 || resp.Failed != 1 {
		t.Fatalf("partial send: %d %s", rec.Code, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), srv.URL) || strings.Contains(rec.Body.String(), "secret-path") || strings.Contains(rec.Body.String(), "ops@example.com") {
		t.Fatalf("response leaks targets: %s", rec.Body.String())
	}

	rec = serve(h, http.MethodPost, SendPath, `{"alert_id":"mailonly","text":"x"}`)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("email without backend: %d %s", rec.Code, rec.Body.String())
	}
	rec = serve(h, http.MethodPost, SendPath, `{"channel_id":"mail","text":"x"}`)
	if rec.Code != http.StatusNotImplemented {
		t.Fatalf("email channel without backend: %d %s", rec.Code, rec.Body.String())
	}
	rec = serve(h, http.MethodPost, SendPath, `{"channel_id":"off","text":"x"}`)
	if rec.Code != http.StatusConflict {
		t.Fatalf("disabled channel: %d %s", rec.Code, rec.Body.String())
	}
}

func TestSendHandlerErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte("upstream says: token=abc"))
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "bad", Name: "bad", Type: ChannelWebhook, Target: srv.URL + "/hook?token=zzz", Enabled: true})
	h := SendHandler(testDispatcher(store))

	cases := []struct {
		method, body string
		want         int
	}{
		{http.MethodGet, "", http.StatusMethodNotAllowed},
		{http.MethodPost, `not json`, http.StatusBadRequest},
		{http.MethodPost, `{"title":"x"}`, http.StatusBadRequest},
		{http.MethodPost, `{"channel_id":"bad","alert_id":"a","title":"x"}`, http.StatusBadRequest},
		{http.MethodPost, `{"channel_id":"bad"}`, http.StatusBadRequest},
		{http.MethodPost, `{"channel_id":"bad","title":"x","severity":"bad severity!"}`, http.StatusBadRequest},
		{http.MethodPost, `{"channel_id":"bad","title":"` + strings.Repeat("x", 300) + `"}`, http.StatusBadRequest},
		{http.MethodPost, `{"channel_id":"missing","title":"x"}`, http.StatusNotFound},
		{http.MethodPost, `{"alert_id":"nobody","title":"x"}`, http.StatusNotFound},
		{http.MethodPost, `{"channel_id":"bad","title":"x"}`, http.StatusBadGateway},
	}
	for _, tc := range cases {
		rec := serve(h, tc.method, SendPath, tc.body)
		if rec.Code != tc.want {
			t.Errorf("%s %s: got %d want %d (%s)", tc.method, tc.body, rec.Code, tc.want, rec.Body.String())
		}
		if strings.Contains(rec.Body.String(), "zzz") || strings.Contains(rec.Body.String(), "upstream says") {
			t.Errorf("response leaks secrets/upstream body: %s", rec.Body.String())
		}
		if tc.want == http.StatusMethodNotAllowed && rec.Header().Get("Allow") != http.MethodPost {
			t.Errorf("missing Allow header")
		}
	}

	rec := serve(SendHandler(nil), http.MethodPost, SendPath, `{"alert_id":"a","title":"x"}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil dispatcher: %d", rec.Code)
	}
}

func TestSendHandlerBlockedDestination(t *testing.T) {
	// A channel stored while private networks were allowed must still be
	// refused by a production dispatcher.
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "local", Name: "l", Type: ChannelWebhook, Target: "https://127.0.0.1/x", Enabled: true})
	h := SendHandler(NewDispatcher(store, DispatcherOptions{}))
	rec := serve(h, http.MethodPost, SendPath, `{"channel_id":"local","title":"x"}`)
	if rec.Code != http.StatusConflict || strings.Contains(rec.Body.String(), "127.0.0.1") {
		t.Fatalf("got %d %s", rec.Code, rec.Body.String())
	}
}

func TestNotifyWithResults(t *testing.T) {
	store := NewStore()
	_, _ = store.UpsertChannel(Channel{ID: "m", Name: "m", Type: ChannelEmail, Target: "ops@example.com", Enabled: true})
	_, _ = store.UpsertSubscription(Subscription{AlertID: "a", ChannelID: "m", Enabled: true})
	d := NewDispatcher(store, DispatcherOptions{EmailSender: emailFunc(func(context.Context, string, Message) error { return nil })})
	res, err := d.NotifyWithResults(context.Background(), "a", Message{Title: "x"})
	if err != nil || len(res) != 1 || !res[0].Delivered || res[0].ChannelID != "m" || res[0].Type != ChannelEmail {
		t.Fatalf("unexpected: %+v %v", res, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	d = NewDispatcher(store, DispatcherOptions{Parallelism: 1})
	res, err = d.NotifyWithResults(ctx, "a", Message{})
	if err == nil || len(res) != 1 || res[0].Delivered {
		t.Fatalf("expected failure result, got %+v %v", res, err)
	}
}
