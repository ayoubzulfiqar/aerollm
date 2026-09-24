package notification

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testDispatcher(store *Store) *Dispatcher {
	return NewDispatcher(store, DispatcherOptions{
		AllowPrivateNetworks: true,
		AllowInsecureHTTP:    true,
		BaseBackoff:          time.Millisecond,
		MaxBackoff:           5 * time.Millisecond,
		Timeout:              2 * time.Second,
	})
}

func localStore() *Store {
	return NewStoreWithOptions(Options{AllowInsecureHTTP: true, AllowPrivateNetworks: true})
}

func TestWebhookDeliverySignedPayload(t *testing.T) {
	var (
		mu      sync.Mutex
		gotBody []byte
		gotHdr  http.Header
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		mu.Lock()
		gotBody, gotHdr = b, r.Header.Clone()
		mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	store := localStore()
	if _, err := store.UpsertChannel(Channel{ID: "w", Name: "hook", Type: ChannelWebhook, Target: srv.URL + "/hook", Enabled: true, Metadata: map[string]string{SigningSecretKey: "s3cret"}}); err != nil {
		t.Fatal(err)
	}
	d := testDispatcher(store)
	msg := Message{AlertID: "cpu", Title: "CPU high", Text: "95%", Severity: "critical"}
	if err := d.Send(context.Background(), "w", msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	var decoded Message
	if err := json.Unmarshal(gotBody, &decoded); err != nil {
		t.Fatalf("body not JSON: %v", err)
	}
	if decoded.AlertID != "cpu" || decoded.Title != "CPU high" || decoded.Timestamp.IsZero() {
		t.Fatalf("unexpected payload %+v", decoded)
	}
	ts := gotHdr.Get(HeaderTimestamp)
	if ts == "" {
		t.Fatalf("missing timestamp header")
	}
	if want := Sign("s3cret", ts, gotBody); gotHdr.Get(HeaderSignature) != want {
		t.Fatalf("bad signature %q want %q", gotHdr.Get(HeaderSignature), want)
	}
	if strings.Contains(string(gotBody), "s3cret") || strings.Contains(gotHdr.Get(HeaderSignature), "s3cret") {
		t.Fatalf("secret sent over the wire")
	}
	if gotHdr.Get("Content-Type") != "application/json" {
		t.Fatalf("content type %q", gotHdr.Get("Content-Type"))
	}
}

func TestSlackPayloadFormat(t *testing.T) {
	bodies := make(chan []byte, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		bodies <- b
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "s", Name: "slack", Type: ChannelSlack, Target: srv.URL + "/services/T/B/X", Enabled: true})
	err := testDispatcher(store).Send(context.Background(), "s", Message{Title: "a<b>&c", Text: "details", Severity: "high"})
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.Unmarshal(<-bodies, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload) != 1 {
		t.Fatalf("slack payload must only contain text: %v", payload)
	}
	if payload["text"] != "[HIGH] *a&lt;b&gt;&amp;c*\ndetails" {
		t.Fatalf("unexpected slack text %q", payload["text"])
	}
}

func TestRetryOn500ThenSuccess(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < 3 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "w", Name: "hook", Type: ChannelWebhook, Target: srv.URL, Enabled: true})
	if err := testDispatcher(store).Send(context.Background(), "w", Message{Title: "x"}); err != nil {
		t.Fatalf("expected success after retries: %v", err)
	}
	if calls.Load() != 3 {
		t.Fatalf("expected 3 attempts, got %d", calls.Load())
	}
}

func TestNoRetryOn400AndNoURLInError(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer srv.Close()
	store := localStore()
	target := srv.URL + "/hook?token=verysecretvalue"
	_, _ = store.UpsertChannel(Channel{ID: "w", Name: "hook", Type: ChannelWebhook, Target: target, Enabled: true})
	err := testDispatcher(store).Send(context.Background(), "w", Message{Title: "x"})
	if err == nil {
		t.Fatalf("expected error")
	}
	if calls.Load() != 1 {
		t.Fatalf("expected a single attempt, got %d", calls.Load())
	}
	if strings.Contains(err.Error(), "verysecretvalue") {
		t.Fatalf("error leaks target secret: %v", err)
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var followed atomic.Bool
	final := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		followed.Store(true)
	}))
	defer final.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, final.URL, http.StatusTemporaryRedirect)
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "w", Name: "hook", Type: ChannelWebhook, Target: srv.URL, Enabled: true})
	err := testDispatcher(store).Send(context.Background(), "w", Message{Title: "x"})
	if err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("expected redirect error, got %v", err)
	}
	if followed.Load() {
		t.Fatalf("redirect was followed")
	}
}

func TestDialTimeSSRFBlock(t *testing.T) {
	var hit atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hit.Store(true)
	}))
	defer srv.Close()
	// Bypass validation by inserting directly (simulates a name that
	// resolves into private space / DNS rebinding after validation).
	store := NewStore()
	store.channels["w"] = Channel{ID: "w", Name: "hook", Type: ChannelWebhook, Target: srv.URL, Enabled: true}
	d := NewDispatcher(store, DispatcherOptions{AllowInsecureHTTP: true, BaseBackoff: time.Millisecond})
	// Validation inside Deliver already refuses the literal loopback IP...
	if err := d.Send(context.Background(), "w", Message{Title: "x"}); err == nil {
		t.Fatalf("expected loopback target to be refused")
	}
	// ...and the dialer refuses loopback even when validation can't see it:
	// "localhost" style names are rejected by name, so exercise the dial hook
	// directly through postOnce with a hostname target.
	err := d.postOnce(context.Background(), "w", strings.Replace(srv.URL, "127.0.0.1", "localhost", 1), "[redacted]", []byte(`{}`), http.Header{})
	if err == nil || !errors.Is(err, ErrBlockedDestination) {
		t.Fatalf("expected ErrBlockedDestination from dial hook, got %v", err)
	}
	if hit.Load() {
		t.Fatalf("request reached a loopback server")
	}
}

func TestEmailSMSNotImplemented(t *testing.T) {
	store := NewStore()
	_, _ = store.UpsertChannel(Channel{ID: "e", Name: "mail", Type: ChannelEmail, Target: "ops@example.com", Enabled: true})
	_, _ = store.UpsertChannel(Channel{ID: "p", Name: "sms", Type: ChannelSMS, Target: "+14155550100", Enabled: true})
	d := NewDispatcher(store, DispatcherOptions{})
	for _, id := range []string{"e", "p"} {
		if err := d.Send(context.Background(), id, Message{Title: "x"}); !errors.Is(err, ErrChannelNotImplemented) {
			t.Fatalf("%s: expected ErrChannelNotImplemented, got %v", id, err)
		}
	}
	var sent []string
	d = NewDispatcher(store, DispatcherOptions{EmailSender: emailFunc(func(_ context.Context, to string, _ Message) error {
		sent = append(sent, to)
		return nil
	})})
	if err := d.Send(context.Background(), "e", Message{Title: "x"}); err != nil {
		t.Fatalf("email sender: %v", err)
	}
	if len(sent) != 1 || sent[0] != "ops@example.com" {
		t.Fatalf("email not routed to sender: %v", sent)
	}
}

type emailFunc func(context.Context, string, Message) error

func (f emailFunc) SendEmail(ctx context.Context, to string, m Message) error { return f(ctx, to, m) }

func TestSendDisabledAndMissing(t *testing.T) {
	store := NewStore()
	_, _ = store.UpsertChannel(Channel{ID: "e", Name: "mail", Type: ChannelEmail, Target: "ops@example.com", Enabled: false})
	d := NewDispatcher(store, DispatcherOptions{})
	if err := d.Send(context.Background(), "e", Message{}); !errors.Is(err, ErrChannelDisabled) {
		t.Fatalf("expected ErrChannelDisabled, got %v", err)
	}
	if err := d.Send(context.Background(), "nope", Message{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestNotifyFanOut(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "a", Name: "a", Type: ChannelWebhook, Target: srv.URL + "/a", Enabled: true})
	_, _ = store.UpsertChannel(Channel{ID: "b", Name: "b", Type: ChannelSlack, Target: srv.URL + "/b", Enabled: true})
	_, _ = store.UpsertChannel(Channel{ID: "off", Name: "off", Type: ChannelWebhook, Target: srv.URL + "/off", Enabled: false})
	_, _ = store.UpsertChannel(Channel{ID: "mail", Name: "mail", Type: ChannelEmail, Target: "ops@example.com", Enabled: true})
	for _, s := range []Subscription{
		{AlertID: "cpu", ChannelID: "a", Enabled: true},
		{AlertID: "cpu", ChannelID: "a", Enabled: true}, // duplicate channel
		{AlertID: "cpu", ChannelID: "b", Enabled: true},
		{AlertID: "cpu", ChannelID: "off", Enabled: true},
		{AlertID: "cpu", ChannelID: "b", Enabled: false},
		{AlertID: "mem", ChannelID: "a", Enabled: true},
		{AlertID: "cpu", ChannelID: "mail", Enabled: true},
	} {
		if _, err := store.UpsertSubscription(s); err != nil {
			t.Fatal(err)
		}
	}
	err := testDispatcher(store).Notify(context.Background(), "cpu", Message{Title: "t"})
	if !errors.Is(err, ErrChannelNotImplemented) {
		t.Fatalf("expected joined email error, got %v", err)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected 2 HTTP deliveries, got %d", calls.Load())
	}
	if err := testDispatcher(store).Notify(context.Background(), "none", Message{}); err != nil {
		t.Fatalf("no subscriptions should be a no-op: %v", err)
	}
}

func TestRetryHonoursContext(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	store := localStore()
	_, _ = store.UpsertChannel(Channel{ID: "w", Name: "hook", Type: ChannelWebhook, Target: srv.URL, Enabled: true})
	d := NewDispatcher(store, DispatcherOptions{AllowPrivateNetworks: true, AllowInsecureHTTP: true, BaseBackoff: time.Second, MaxBackoff: time.Second, MaxAttempts: 5})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := d.Send(ctx, "w", Message{}); err == nil {
		t.Fatalf("expected error")
	}
	if time.Since(start) > 900*time.Millisecond {
		t.Fatalf("retry loop ignored context cancellation")
	}
}
