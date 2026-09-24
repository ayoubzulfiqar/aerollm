package marketplace

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestRegistryServiceOpenStandardCapabilityManifest(t *testing.T) {
	s := NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"version":"1.0","hardware":{"has_local_gpu":true,"os":"linux","memory_gb":16},"billing":{"supports_metered":true,"currency":"USD"},"capabilities":["mesh"]}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/openstandard/capability", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("expected 202, got %d", resp.StatusCode)
	}
}

func TestRegistryServiceOpenStandardBillingReceipt(t *testing.T) {
	s := NewRegistryService(nil, nil)
	mux := http.NewServeMux()
	s.RegisterRoutes(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"receipt_id":"r-1","customer_id":"c1","provider_id":"p1","event_name":"token","value":1,"currency":"USD"}`
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, server.URL+"/v1/marketplace/openstandard/receipt", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("expected 201, got %d", resp.StatusCode)
	}
}

type recordingSink struct {
	mu       sync.Mutex
	receipts map[string]BillingReceipt
	fail     error
}

func (s *recordingSink) RecordReceipt(ctx context.Context, rec BillingReceipt) (BillingReceipt, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail != nil {
		return BillingReceipt{}, false, s.fail
	}
	if rec.CustomerID == "someone-else" {
		return BillingReceipt{}, false, ErrReceiptForbidden
	}
	if prev, ok := s.receipts[rec.ReceiptID]; ok {
		cmp := prev
		cmp.RecordedAt = rec.RecordedAt
		if cmp != rec {
			return BillingReceipt{}, false, ErrReceiptConflict
		}
		return prev, false, nil
	}
	s.receipts[rec.ReceiptID] = rec
	return rec, true, nil
}

func TestBillingReceiptHandlerWithSink(t *testing.T) {
	s := NewRegistryService(nil, nil)
	sink := &recordingSink{receipts: map[string]BillingReceipt{}}
	s.SetReceiptSink(sink)
	server := httptest.NewServer(s.BillingReceiptHandler())
	defer server.Close()
	post := func(body string) int {
		resp, err := http.Post(server.URL, "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	rec := `{"receipt_id":"r-1","customer_id":"c1","event_name":"token","value":1,"currency":"USD"}`
	if code := post(rec); code != http.StatusCreated {
		t.Fatalf("first: %d", code)
	}
	if code := post(rec); code != http.StatusOK {
		t.Fatalf("replay: %d", code)
	}
	if code := post(strings.Replace(rec, `"value":1`, `"value":2`, 1)); code != http.StatusConflict {
		t.Fatalf("conflict: %d", code)
	}
	if code := post(strings.Replace(rec, `"c1"`, `"someone-else"`, 1)); code != http.StatusForbidden {
		t.Fatalf("forbidden: %d", code)
	}
	if code := post(`{"receipt_id":"r-2","event_name":"token","value":-1,"currency":"USD"}`); code != http.StatusBadRequest {
		t.Fatalf("invalid receipt must not reach the sink: %d", code)
	}
	sink.fail = errors.New("db down")
	if code := post(strings.Replace(rec, "r-1", "r-3", 1)); code != http.StatusInternalServerError {
		t.Fatalf("sink failure: %d", code)
	}
	if len(sink.receipts) != 1 {
		t.Fatalf("unexpected stored receipts: %+v", sink.receipts)
	}
	s.SetReceiptSink(nil)
	if code := post(strings.Replace(rec, "r-1", "r-4", 1)); code != http.StatusCreated {
		t.Fatalf("without sink the handler validates and echoes: %d", code)
	}
}
