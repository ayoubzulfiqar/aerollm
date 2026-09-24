package notification

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// Built by concatenation so the fake SID never appears as a literal that
	// secret scanners mistake for a real Twilio credential.
	testTwilioSID     = "AC" + "0123456789abcdef" + "0123456789abcdef"
	testTwilioService = "MG" + "0123456789abcdef" + "0123456789abcdef"
	testTwilioToken   = "super-secret-token-value"
)

func testTwilio(t *testing.T, h http.HandlerFunc) *TwilioSender {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	s, err := NewTwilioSender(TwilioOptions{AccountSID: testTwilioSID, AuthToken: testTwilioToken, From: "+14155550199", Timeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	s.baseURL = srv.URL
	return s
}

func TestTwilioSendsForm(t *testing.T) {
	var got atomic.Value
	s := testTwilio(t, func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != testTwilioSID || pass != testTwilioToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/2010-04-01/Accounts/"+testTwilioSID+"/Messages.json" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if ct := r.Header.Get("Content-Type"); ct != "application/x-www-form-urlencoded" {
			w.WriteHeader(http.StatusUnsupportedMediaType)
			return
		}
		if err := r.ParseForm(); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		got.Store(r.PostForm)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"sid":"SM1","status":"queued"}`))
	})
	err := s.SendSMS(context.Background(), "+14155550100", Message{Title: "Disk full", Text: "node-1 at 99%", Severity: "critical"})
	if err != nil {
		t.Fatalf("send: %v", err)
	}
	form, _ := got.Load().(url.Values)
	if form == nil {
		t.Fatal("no request recorded")
	}
	if form["To"][0] != "+14155550100" || form["From"][0] != "+14155550199" {
		t.Fatalf("bad form: %v", form)
	}
	if body := form["Body"][0]; body != "[CRITICAL] Disk full: node-1 at 99%" {
		t.Fatalf("bad body: %q", body)
	}
}

func TestTwilioErrorMappingNeverLeaksToken(t *testing.T) {
	s := testTwilio(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":21211,"message":"The 'To' number is not valid. token=` + testTwilioToken + `","status":400}`))
	})
	err := s.SendSMS(context.Background(), "+14155550100", Message{Title: "x"})
	if !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("expected ErrDeliveryFailed, got %v", err)
	}
	if !strings.Contains(err.Error(), "21211") || !strings.Contains(err.Error(), "status 400") {
		t.Fatalf("expected Twilio code in error: %v", err)
	}
	if strings.Contains(err.Error(), testTwilioToken) {
		t.Fatalf("auth token leaked: %v", err)
	}

	s = testTwilio(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	if err := s.SendSMS(context.Background(), "+14155550100", Message{Title: "x"}); err == nil || !strings.Contains(err.Error(), "status 503") {
		t.Fatalf("expected 503 error, got %v", err)
	}

	// Transport failure: the error must not contain the URL or token.
	s = testTwilio(t, func(w http.ResponseWriter, r *http.Request) {})
	s.baseURL = "http://127.0.0.1:1"
	err = s.SendSMS(context.Background(), "+14155550100", Message{Title: "x"})
	if err == nil || strings.Contains(err.Error(), testTwilioToken) || strings.Contains(err.Error(), testTwilioSID) {
		t.Fatalf("unexpected transport error: %v", err)
	}
}

func TestTwilioNoRedirectsAndTimeout(t *testing.T) {
	var hits atomic.Int32
	s := testTwilio(t, func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) > 1 {
			t.Error("redirect was followed")
		}
		http.Redirect(w, r, "http://169.254.169.254/latest", http.StatusFound)
	})
	if err := s.SendSMS(context.Background(), "+14155550100", Message{Title: "x"}); err == nil {
		t.Fatal("expected redirect to be an error")
	}

	block := make(chan struct{})
	s = testTwilio(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-block:
		case <-r.Context().Done():
		}
	})
	defer close(block)
	s.opts.Timeout = 100 * time.Millisecond
	start := time.Now()
	if err := s.SendSMS(context.Background(), "+14155550100", Message{Title: "x"}); err == nil {
		t.Fatal("expected timeout")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("request not bounded by timeout")
	}
}

func TestTwilioValidation(t *testing.T) {
	bad := []TwilioOptions{
		{AccountSID: "AC123", AuthToken: "t", From: "+14155550100"},
		{AccountSID: testTwilioSID, AuthToken: "", From: "+14155550100"},
		{AccountSID: testTwilioSID, AuthToken: "t", From: "4155550100"},
		{AccountSID: testTwilioSID + "/../x", AuthToken: "t", From: "+14155550100"},
	}
	for i, o := range bad {
		if _, err := NewTwilioSender(o); err == nil {
			t.Errorf("case %d: expected error", i)
		}
	}
	s, err := NewTwilioSender(TwilioOptions{AccountSID: testTwilioSID, AuthToken: "t", From: testTwilioService})
	if err != nil {
		t.Fatalf("messaging service sid: %v", err)
	}
	if s.baseURL != "https://api.twilio.com" {
		t.Fatalf("base URL must be fixed: %s", s.baseURL)
	}
	var verr *ValidationError
	if err := s.SendSMS(context.Background(), "12345", Message{}); !errors.As(err, &verr) {
		t.Fatalf("expected validation error, got %v", err)
	}
}

func TestSMSTextBounded(t *testing.T) {
	txt := smsText(Message{Text: strings.Repeat("é", 5000)})
	if n := len([]rune(txt)); n != maxSMSBodyRunes {
		t.Fatalf("expected %d runes, got %d", maxSMSBodyRunes, n)
	}
	if got := smsText(Message{AlertID: "cpu"}); got != "AeroLLM alert cpu" {
		t.Fatalf("fallback text: %q", got)
	}
}

func TestDispatcherUsesTwilioSender(t *testing.T) {
	var calls atomic.Int32
	sender := testTwilio(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusCreated)
	})
	store := NewStore()
	if _, err := store.UpsertChannel(Channel{ID: "pager", Name: "pager", Type: ChannelSMS, Target: "+14155550100", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(store, DispatcherOptions{SMSSender: sender})
	if err := d.Send(context.Background(), "pager", Message{Title: "x"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("expected 1 Twilio call, got %d", calls.Load())
	}
}

func TestTwilioOptionsFromEnv(t *testing.T) {
	t.Setenv(EnvTwilioAccountSID, testTwilioSID)
	t.Setenv(EnvTwilioAuthToken, "")
	t.Setenv(EnvTwilioFrom, "+14155550100")
	if _, ok := TwilioOptionsFromEnv(); ok {
		t.Fatal("expected ok=false without token")
	}
	t.Setenv(EnvTwilioAuthToken, "tok")
	o, ok := TwilioOptionsFromEnv()
	if !ok || o.AccountSID != testTwilioSID || o.AuthToken != "tok" || o.From != "+14155550100" {
		t.Fatalf("unexpected: %v %+v", ok, o)
	}
}
