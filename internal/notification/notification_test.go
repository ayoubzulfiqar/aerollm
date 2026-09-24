package notification

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/netip"
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

func TestValidateChannel(t *testing.T) {
	strict := Options{}
	cases := []struct {
		name string
		ch   Channel
		opts Options
		ok   bool
	}{
		{"valid webhook", Channel{Name: "a", Type: ChannelWebhook, Target: "https://example.com/hook"}, strict, true},
		{"valid slack", Channel{Name: "a", Type: ChannelSlack, Target: "https://hooks.slack.com/services/T0/B0/xyz"}, strict, true},
		{"missing name", Channel{Type: ChannelWebhook, Target: "https://example.com"}, strict, false},
		{"long name", Channel{Name: strings.Repeat("n", 129), Type: ChannelWebhook, Target: "https://example.com"}, strict, false},
		{"missing type", Channel{Name: "a", Target: "https://example.com"}, strict, false},
		{"bad type", Channel{Name: "a", Type: "pager", Target: "https://example.com"}, strict, false},
		{"bad id", Channel{ID: "../x", Name: "a", Type: ChannelWebhook, Target: "https://example.com"}, strict, false},
		{"http rejected", Channel{Name: "a", Type: ChannelWebhook, Target: "http://example.com"}, strict, false},
		{"http allowed", Channel{Name: "a", Type: ChannelWebhook, Target: "http://example.com"}, Options{AllowInsecureHTTP: true}, true},
		{"loopback v4", Channel{Name: "a", Type: ChannelWebhook, Target: "https://127.0.0.1/x"}, strict, false},
		{"loopback v6", Channel{Name: "a", Type: ChannelWebhook, Target: "https://[::1]:8443/x"}, strict, false},
		{"private 10/8", Channel{Name: "a", Type: ChannelWebhook, Target: "https://10.1.2.3/x"}, strict, false},
		{"private 192.168", Channel{Name: "a", Type: ChannelSlack, Target: "https://192.168.1.1/x"}, strict, false},
		{"metadata", Channel{Name: "a", Type: ChannelWebhook, Target: "https://169.254.169.254/latest/meta-data"}, strict, false},
		{"metadata v6", Channel{Name: "a", Type: ChannelWebhook, Target: "https://[fd00:ec2::254]/"}, strict, false},
		{"gcp metadata", Channel{Name: "a", Type: ChannelWebhook, Target: "https://metadata.google.internal/"}, strict, false},
		{"cgnat", Channel{Name: "a", Type: ChannelWebhook, Target: "https://100.64.0.1/"}, strict, false},
		{"unspecified", Channel{Name: "a", Type: ChannelWebhook, Target: "https://0.0.0.0/"}, strict, false},
		{"mapped v4", Channel{Name: "a", Type: ChannelWebhook, Target: "https://[::ffff:127.0.0.1]/"}, strict, false},
		{"localhost", Channel{Name: "a", Type: ChannelWebhook, Target: "https://localhost:9000/"}, strict, false},
		{"sub.localhost", Channel{Name: "a", Type: ChannelWebhook, Target: "https://foo.localhost/"}, strict, false},
		{"decimal ip", Channel{Name: "a", Type: ChannelWebhook, Target: "https://2130706433/"}, strict, false},
		{"hex ip", Channel{Name: "a", Type: ChannelWebhook, Target: "https://0x7f000001/"}, strict, false},
		{"userinfo", Channel{Name: "a", Type: ChannelWebhook, Target: "https://user:pw@example.com/"}, strict, false},
		{"relative", Channel{Name: "a", Type: ChannelWebhook, Target: "/hook"}, strict, false},
		{"ftp", Channel{Name: "a", Type: ChannelWebhook, Target: "ftp://example.com/"}, strict, false},
		{"whitespace", Channel{Name: "a", Type: ChannelWebhook, Target: "https://example.com/a b"}, strict, false},
		{"no host", Channel{Name: "a", Type: ChannelWebhook, Target: "https:///x"}, strict, false},
		{"bad port", Channel{Name: "a", Type: ChannelWebhook, Target: "https://example.com:99999/"}, strict, false},
		{"private allowed", Channel{Name: "a", Type: ChannelWebhook, Target: "http://127.0.0.1:8080/"}, Options{AllowInsecureHTTP: true, AllowPrivateNetworks: true}, true},
		{"email ok", Channel{Name: "a", Type: ChannelEmail, Target: "ops@example.com"}, strict, true},
		{"email display name", Channel{Name: "a", Type: ChannelEmail, Target: "Ops <ops@example.com>"}, strict, false},
		{"email list", Channel{Name: "a", Type: ChannelEmail, Target: "a@example.com, b@example.com"}, strict, false},
		{"email bad", Channel{Name: "a", Type: ChannelEmail, Target: "not-an-email"}, strict, false},
		{"sms ok", Channel{Name: "a", Type: ChannelSMS, Target: "+14155550100"}, strict, true},
		{"sms bad", Channel{Name: "a", Type: ChannelSMS, Target: "4155550100"}, strict, false},
		{"too much metadata", Channel{Name: "a", Type: ChannelSMS, Target: "+14155550100", Metadata: bigMeta(33)}, strict, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateChannel(tc.ch, tc.opts)
			if tc.ok && err != nil {
				t.Fatalf("expected valid, got %v", err)
			}
			if !tc.ok && err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func bigMeta(n int) map[string]string {
	m := make(map[string]string, n)
	for i := 0; i < n; i++ {
		m[strings.Repeat("k", i+1)] = "v"
	}
	return m
}

func TestIsBlockedIP(t *testing.T) {
	blocked := []string{"127.0.0.1", "::1", "10.0.0.1", "172.16.0.1", "192.168.0.1", "169.254.169.254", "fe80::1", "fc00::1", "100.64.0.1", "0.0.0.0", "::", "224.0.0.1", "::ffff:10.0.0.1", "2002:7f00:0001::1", "64:ff9b::7f00:1", "255.255.255.255"}
	for _, s := range blocked {
		if !IsBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("expected %s to be blocked", s)
		}
	}
	allowed := []string{"93.184.216.34", "8.8.8.8", "2606:4700:4700::1111", "2002:5db8:d822::1"}
	for _, s := range allowed {
		if IsBlockedIP(netip.MustParseAddr(s)) {
			t.Errorf("expected %s to be allowed", s)
		}
	}
}

func serve(h http.Handler, method, target, body string) *httptest.ResponseRecorder {
	var req *http.Request
	if body == "" {
		req = httptest.NewRequest(method, target, nil)
	} else {
		req = httptest.NewRequest(method, target, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestChannelREST(t *testing.T) {
	store := NewStore()
	h := WebhookHandler(store)

	// Create without id: server generates one.
	rec := serve(h, http.MethodPost, channelsPath, `{"name":"ops","type":"webhook","target":"https://example.com/alerts","enabled":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content-type %q", ct)
	}
	var created Channel
	if err := json.Unmarshal(rec.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(created.ID, "ch_") || len(created.ID) != len("ch_")+16 {
		t.Fatalf("unexpected generated id %q", created.ID)
	}

	// Get by query id and by path id.
	if rec := serve(h, http.MethodGet, channelsPath+"?id="+created.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("get query: %d", rec.Code)
	}
	if rec := serve(h, http.MethodGet, channelsPath+"/"+created.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("get path: %d", rec.Code)
	}
	if rec := serve(h, http.MethodGet, channelsPath+"?id=missing", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("get missing: %d", rec.Code)
	}
	if rec := serve(h, http.MethodGet, channelsPath+"/a?id=b", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("conflicting ids: %d", rec.Code)
	}
	if rec := serve(h, http.MethodGet, channelsPath+"/a/b", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("nested path: %d", rec.Code)
	}

	// Invalid create.
	if rec := serve(h, http.MethodPost, channelsPath, `{"name":"x","type":"webhook","target":"https://127.0.0.1/"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("ssrf create: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(h, http.MethodPost, channelsPath, `{"name":`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: %d", rec.Code)
	}
	if rec := serve(h, http.MethodPost, channelsPath, `{"name":"a"} {"x":1}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("trailing json: %d", rec.Code)
	}
	big := `{"name":"` + strings.Repeat("a", maxBodyBytes+10) + `"}`
	if rec := serve(h, http.MethodPost, channelsPath, big); rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("big body: %d", rec.Code)
	}

	// PUT replaces; unknown id 404.
	rec = serve(h, http.MethodPut, channelsPath+"?id="+created.ID, `{"name":"ops2","type":"sms","target":"+14155550100","enabled":false}`)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"name":"ops2"`) {
		t.Fatalf("put: %d %s", rec.Code, rec.Body.String())
	}
	if rec := serve(h, http.MethodPut, channelsPath+"?id=nope", `{"name":"x","type":"sms","target":"+14155550100"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("put missing: %d", rec.Code)
	}
	if rec := serve(h, http.MethodPut, channelsPath, `{"name":"x"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("put without id: %d", rec.Code)
	}

	// PATCH partial update keeps other fields and validates.
	rec = serve(h, http.MethodPatch, channelsPath+"/"+created.ID, `{"enabled":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("patch: %d %s", rec.Code, rec.Body.String())
	}
	got, _ := store.GetChannel(created.ID)
	if !got.Enabled || got.Name != "ops2" || got.Type != ChannelSMS {
		t.Fatalf("patch result %+v", got)
	}
	if rec := serve(h, http.MethodPatch, channelsPath+"/"+created.ID, `{"target":"nope"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("invalid patch: %d", rec.Code)
	}

	// 405 with Allow.
	rec = serve(h, http.MethodOptions, channelsPath, "")
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") == "" {
		t.Fatalf("405: %d allow=%q", rec.Code, rec.Header().Get("Allow"))
	}

	// Unknown sub-path.
	if rec := serve(h, http.MethodGet, "/v1/notification/other", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown path: %d", rec.Code)
	}
}

func TestDeleteChannelCascades(t *testing.T) {
	store := NewStore()
	h := WebhookHandler(store)
	if _, err := store.UpsertChannel(Channel{ID: "c1", Name: "a", Type: ChannelSMS, Target: "+14155550100", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	rec := serve(h, http.MethodPost, subscriptionsPath, `{"alert_id":"cpu","channel_id":"c1","enabled":true}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create sub: %d %s", rec.Code, rec.Body.String())
	}
	var sub Subscription
	_ = json.Unmarshal(rec.Body.Bytes(), &sub)
	if !strings.HasPrefix(sub.ID, "sub_") {
		t.Fatalf("sub id %q", sub.ID)
	}
	if rec := serve(h, http.MethodPost, subscriptionsPath, `{"alert_id":"cpu","channel_id":"ghost"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown channel: %d", rec.Code)
	}
	if rec := serve(h, http.MethodPost, subscriptionsPath, `{"channel_id":"c1"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing alert id: %d", rec.Code)
	}
	if rec := serve(h, http.MethodPatch, subscriptionsPath+"?id="+sub.ID, `{"enabled":false}`); rec.Code != http.StatusOK {
		t.Fatalf("patch sub: %d", rec.Code)
	}
	if s, _ := store.GetSubscription(sub.ID); s.Enabled {
		t.Fatalf("patch did not apply")
	}
	if rec := serve(h, http.MethodPut, subscriptionsPath+"?id="+sub.ID, `{"alert_id":"mem","channel_id":"c1","enabled":true}`); rec.Code != http.StatusOK {
		t.Fatalf("put sub: %d", rec.Code)
	}
	if rec := serve(h, http.MethodDelete, channelsPath+"?id=c1", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("delete: %d", rec.Code)
	}
	if len(store.ListSubscriptions()) != 0 {
		t.Fatalf("expected cascade delete of subscriptions")
	}
	if rec := serve(h, http.MethodDelete, channelsPath+"?id=c1", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete again: %d", rec.Code)
	}
	if rec := serve(h, http.MethodDelete, subscriptionsPath+"?id="+sub.ID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete sub again: %d", rec.Code)
	}
}

func TestStoreBounded(t *testing.T) {
	store := NewStoreWithOptions(Options{MaxChannels: 2, MaxSubscriptions: 1})
	for i, id := range []string{"a", "b"} {
		if _, err := store.UpsertChannel(Channel{ID: id, Name: "n", Type: ChannelSMS, Target: "+14155550100"}); err != nil {
			t.Fatalf("%d: %v", i, err)
		}
	}
	if _, err := store.UpsertChannel(Channel{ID: "c", Name: "n", Type: ChannelSMS, Target: "+14155550100"}); !errors.Is(err, ErrStoreFull) {
		t.Fatalf("expected ErrStoreFull, got %v", err)
	}
	// Updating an existing entry is still allowed when full.
	if _, err := store.UpsertChannel(Channel{ID: "a", Name: "renamed", Type: ChannelSMS, Target: "+14155550100"}); err != nil {
		t.Fatalf("update when full: %v", err)
	}
	if _, err := store.UpsertSubscription(Subscription{AlertID: "x", ChannelID: "a"}); err != nil {
		t.Fatal(err)
	}
	rec := serve(WebhookHandler(store), http.MethodPost, subscriptionsPath, `{"alert_id":"y","channel_id":"a"}`)
	if rec.Code != http.StatusInsufficientStorage {
		t.Fatalf("expected 507, got %d", rec.Code)
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	store := NewStore()
	_, _ = store.UpsertChannel(Channel{ID: "c1", Name: "n", Type: ChannelSMS, Target: "+14155550100", Metadata: map[string]string{"team": "a"}})
	ch, _ := store.GetChannel("c1")
	ch.Metadata["team"] = "mutated"
	again, _ := store.GetChannel("c1")
	if again.Metadata["team"] != "a" {
		t.Fatalf("store leaked internal map")
	}
}

func TestRedaction(t *testing.T) {
	store := NewStore()
	h := WebhookHandler(store)
	body := `{"id":"s1","name":"slack","type":"slack","target":"https://hooks.slack.com/services/T000/B000/SECRETTOKENVALUE","enabled":true,"metadata":{"signing_secret":"hunter2","team":"sre"}}`
	rec := serve(h, http.MethodPost, channelsPath, body)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body.String())
	}
	for _, r := range []*httptest.ResponseRecorder{rec, serve(h, http.MethodGet, channelsPath, ""), serve(h, http.MethodGet, channelsPath+"?id=s1", "")} {
		b := r.Body.String()
		if strings.Contains(b, "SECRETTOKENVALUE") || strings.Contains(b, "hunter2") || strings.Contains(b, "T000") {
			t.Fatalf("secret leaked: %s", b)
		}
		if !strings.Contains(b, `"team":"sre"`) || !strings.Contains(b, "hooks.slack.com") {
			t.Fatalf("non-secret data missing: %s", b)
		}
	}
	// Echoing the redacted object back must keep the stored secrets.
	var red Channel
	_ = json.Unmarshal(rec.Body.Bytes(), &red)
	red.Name = "slack-renamed"
	echo, _ := json.Marshal(red)
	if rec := serve(h, http.MethodPut, channelsPath+"?id=s1", string(echo)); rec.Code != http.StatusOK {
		t.Fatalf("put redacted echo: %d %s", rec.Code, rec.Body.String())
	}
	got, _ := store.GetChannel("s1")
	if got.Target != "https://hooks.slack.com/services/T000/B000/SECRETTOKENVALUE" || got.Metadata["signing_secret"] != "hunter2" || got.Name != "slack-renamed" {
		t.Fatalf("secrets not preserved: %+v", got)
	}

	webhook := Channel{Type: ChannelWebhook, Target: "https://example.com/hooks/abcdef0123456789abcdef?token=abc&x=1"}
	r := webhook.Redacted().Target
	if strings.Contains(r, "abcdef0123456789abcdef") || strings.Contains(r, "abc&") || strings.Contains(r, "=1") {
		t.Fatalf("webhook not redacted: %s", r)
	}
	if !strings.HasPrefix(r, "https://example.com/hooks/") {
		t.Fatalf("webhook over-redacted: %s", r)
	}
}
