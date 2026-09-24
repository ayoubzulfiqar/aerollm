package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/hardware"
	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"go.etcd.io/bbolt"
)

// newTestEdge builds the real edge route tree over a temp bbolt DB.
func newTestEdge(t *testing.T, mutate func(*edgeConfig)) (*edgeServer, *httptest.Server) {
	t.Helper()
	cfg := edgeConfig{listenAddr: "127.0.0.1:0", maxStreamBytes: 1 << 20, shutdownTimeout: time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	db := openTestDB(t)
	peerID, err := localPeerID(db)
	if err != nil {
		t.Fatal(err)
	}
	s, err := newEdgeServer(context.Background(), cfg, db, peerID, []hardware.Capability{{Name: "cpu", Available: true}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
	srv := httptest.NewServer(s.routes())
	t.Cleanup(srv.Close)
	return s, srv
}

func doReq(t *testing.T, method, url, body string, hdr map[string]string) (*http.Response, string) {
	t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequestWithContext(context.Background(), method, url, r)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range hdr {
		if k == "Host" {
			req.Host = v
			continue
		}
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

const capabilityBody = `{"version":"1.0","hardware":{"has_local_gpu":true,"gpu_name":"cuda","os":"linux","memory_gb":16},"billing":{"supports_metered":true,"currency":"USD"},"capabilities":["mesh"]}`

func TestOpenStandardRoutes(t *testing.T) {
	_, srv := newTestEdge(t, nil)

	resp, _ := doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/capability", capabilityBody, nil)
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}

	rbody := `{"receipt_id":"r-1","customer_id":"c1","provider_id":"p1","event_name":"token","value":1,"currency":"USD"}`
	resp, _ = doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/receipt", rbody, nil)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
	// Idempotent replay of the same receipt.
	resp, _ = doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/receipt", rbody, nil)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for identical replay, got %d", resp.StatusCode)
	}
	// Same receipt ID, different content: conflict, not a silent overwrite.
	resp, _ = doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/receipt", strings.Replace(rbody, `"value":1`, `"value":999`, 1), nil)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("expected 409 for conflicting receipt, got %d", resp.StatusCode)
	}

	for name, body := range map[string]string{
		"negative value": `{"receipt_id":"r-2","event_name":"token","value":-1,"currency":"USD"}`,
		"unknown field":  `{"receipt_id":"r-2","event_name":"token","value":1,"currency":"USD","admin":true}`,
		"bad json":       `{`,
	} {
		resp, body := doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/receipt", body, nil)
		if resp.StatusCode != http.StatusBadRequest || resp.Header.Get("Content-Type") != "application/json" {
			t.Errorf("%s: expected JSON 400, got %d %q", name, resp.StatusCode, body)
		}
	}
	huge := `{"receipt_id":"` + strings.Repeat("a", 2*marketplace.MaxOpenStandardBytes) + `"}`
	if resp, _ := doReq(t, http.MethodPost, srv.URL+"/v1/marketplace/openstandard/receipt", huge, nil); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodGet, srv.URL+"/v1/marketplace/openstandard/receipt", "", nil); resp.StatusCode != http.StatusMethodNotAllowed || resp.Header.Get("Allow") != "POST" {
		t.Fatalf("expected 405 with Allow, got %d %q", resp.StatusCode, resp.Header.Get("Allow"))
	}
}

func TestCapabilitySelfUpdateIsPersistedAndRaceFree(t *testing.T) {
	s, srv := newTestEdge(t, nil)
	resp, body := doReq(t, http.MethodGet, srv.URL+"/v1/marketplace/openstandard/capability/self", "", nil)
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `"currency":"USD"`) {
		t.Fatalf("self GET: %d %s", resp.StatusCode, body)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			doReq(t, http.MethodGet, srv.URL+"/v1/marketplace/openstandard/capability/self", "", nil)
		}
	}()
	for i := 0; i < 20; i++ {
		if resp, body := doReq(t, http.MethodPut, srv.URL+"/v1/marketplace/openstandard/capability/self", capabilityBody, nil); resp.StatusCode != http.StatusAccepted {
			t.Fatalf("self PUT: %d %s", resp.StatusCode, body)
		}
	}
	<-done

	stored, ok, err := loadCapabilityManifest(s.db)
	if err != nil || !ok || stored.Hardware.GPUName != "cuda" || stored.Hardware.MemoryGB != 16 {
		t.Fatalf("manifest not persisted: %+v %v %v", stored, ok, err)
	}
	if resp, _ := doReq(t, http.MethodPut, srv.URL+"/v1/marketplace/openstandard/capability/self", `{"version":""}`, nil); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for invalid manifest, got %d", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodDelete, srv.URL+"/v1/marketplace/openstandard/capability/self", "", nil); resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", resp.StatusCode)
	}
}

func TestEdgeAuthAndHostGuard(t *testing.T) {
	token := strings.Repeat("s", 32)
	_, srv := newTestEdge(t, func(c *edgeConfig) { c.apiToken = token })

	if resp, _ := doReq(t, http.MethodGet, srv.URL+"/v1/edge/capabilities", "", nil); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 without token, got %d", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodGet, srv.URL+"/v1/edge/capabilities", "", map[string]string{"Authorization": "Bearer wrong-token-wrong-token"}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("expected 401 with wrong token, got %d", resp.StatusCode)
	}
	resp, body := doReq(t, http.MethodGet, srv.URL+"/v1/edge/capabilities", "", map[string]string{"Authorization": "Bearer " + token})
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 with token, got %d", resp.StatusCode)
	}
	var caps map[string]interface{}
	if err := json.Unmarshal([]byte(body), &caps); err != nil || caps["peer_id"] == "" || caps["wallet"] != "0.00" {
		t.Fatalf("unexpected capabilities body %s (%v)", body, err)
	}

	// Loopback listener: a rebinding attacker's Host header is refused.
	_, open := newTestEdge(t, nil)
	if resp, _ := doReq(t, http.MethodGet, open.URL+"/v1/edge/capabilities", "", map[string]string{"Host": "evil.example:7910"}); resp.StatusCode != http.StatusMisdirectedRequest {
		t.Fatalf("expected 421 for foreign Host, got %d", resp.StatusCode)
	}
	if resp, _ := doReq(t, http.MethodGet, open.URL+"/v1/edge/capabilities", "", map[string]string{"Host": "localhost:7910"}); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 for localhost Host, got %d", resp.StatusCode)
	}
}

func TestBboltReceiptQueue(t *testing.T) {
	db, err := bbolt.Open(t.TempDir()+"/edge.db", 0o600, &bbolt.Options{Timeout: time.Second})
	if err != nil {
		t.Fatalf("open bbolt: %v", err)
	}
	defer db.Close()

	rec := marketplace.BillingReceipt{ReceiptID: "rec-1", EventName: "token", Currency: "USD"}
	if err := queueReceipt(db, rec); err != nil {
		t.Fatalf("queue receipt: %v", err)
	}

	var stored marketplace.BillingReceipt
	_ = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("edge"))
		if b == nil {
			return nil
		}
		r := b.Bucket([]byte("receipts"))
		if r == nil {
			return nil
		}
		if v := r.Get([]byte("rec-1")); v != nil {
			// Stored as real JSON (previously fmt "%v" output).
			return json.Unmarshal(v, &stored)
		}
		return nil
	})
	if stored.ReceiptID != "rec-1" || stored.Currency != "USD" {
		t.Fatalf("expected receipt stored as JSON, got %+v", stored)
	}
	if err := queueReceipt(db, rec); err != nil {
		t.Fatalf("identical re-queue must be idempotent: %v", err)
	}
	rec.Value = 5
	if err := queueReceipt(db, rec); !errors.Is(err, errReceiptConflict) {
		t.Fatalf("expected conflict, got %v", err)
	}
	if err := queueReceipt(db, marketplace.BillingReceipt{ReceiptID: "../x", EventName: "t", Currency: "USD"}); err == nil {
		t.Fatal("expected invalid receipt to be rejected")
	}
}
