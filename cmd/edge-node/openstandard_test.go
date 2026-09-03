package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"go.etcd.io/bbolt"
)

func TestOpenStandardRoutes(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/marketplace/openstandard/capability", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var m marketplace.CapabilityManifest
		if err := json.NewDecoder(r.Body).Decode(&m); err != nil { http.Error(w, "invalid request", http.StatusBadRequest); return }
		m.UpdatedAt = timeNow()
		if err := m.Validate(); err != nil { http.Error(w, "invalid manifest", http.StatusBadRequest); return }
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(m)
	})
	mux.HandleFunc("/v1/marketplace/openstandard/receipt", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost { http.Error(w, "method not allowed", http.StatusMethodNotAllowed); return }
		var rec marketplace.BillingReceipt
		if err := json.NewDecoder(r.Body).Decode(&rec); err != nil { http.Error(w, "invalid request", http.StatusBadRequest); return }
		rec.RecordedAt = timeNow()
		if err := rec.Validate(); err != nil { http.Error(w, "invalid receipt", http.StatusBadRequest); return }
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(rec)
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"version":"1.0","hardware":{"has_local_gpu":true,"os":"linux","memory_gb":16},"billing":{"supports_metered":true,"currency":"USD"},"capabilities":["mesh"]}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/openstandard/capability", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil { t.Fatalf("request failed: %v", err) }
	if resp.StatusCode != http.StatusAccepted { t.Fatalf("expected 202, got %d", resp.StatusCode) }

	rbody := `{"receipt_id":"r-1","customer_id":"c1","provider_id":"p1","event_name":"token","value":1,"currency":"USD"}`
	req2, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/openstandard/receipt", strings.NewReader(rbody))
	req2.Header.Set("Content-Type", "application/json")
	resp2, err := http.DefaultClient.Do(req2)
	if err != nil { t.Fatalf("receipt request failed: %v", err) }
	if resp2.StatusCode != http.StatusCreated { t.Fatalf("expected 201, got %d", resp2.StatusCode) }
}

func TestBboltReceiptQueue(t *testing.T) {
	db, err := bbolt.Open(t.TempDir()+"/edge.db", 0o600, &bbolt.Options{Timeout: 1})
	if err != nil { t.Fatalf("open bbolt: %v", err) }
	defer db.Close()

	rec := marketplace.BillingReceipt{ReceiptID: "rec-1", EventName: "token", Currency: "USD"}
	if err := queueReceipt(db, rec); err != nil { t.Fatalf("queue receipt: %v", err) }

	var found bool
	_ = db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket([]byte("edge"))
		if b == nil { return nil }
		r := b.Bucket([]byte("receipts"))
		if r == nil { return nil }
		v := r.Get([]byte("rec-1"))
		found = v != nil
		return nil
	})
	if !found { t.Fatal("expected receipt queued in bbolt") }
}

func timeNow() time.Time { return time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC) }
