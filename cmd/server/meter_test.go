package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/meter"
)

func TestMeterUsageRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/meter/usage", func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
			return
		}
		defer r.Body.Close()
		var req meter.UsageRecord
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
			return
		}
		recorder := meter.NewRecorder()
		recorder.Record(req)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(recorder.Records())
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	client := server.Client()
	body := strings.NewReader(`{"api_key":"k1","provider":"p1","model":"m1","tokens_in":10,"tokens_out":20,"latency_ms":100}`)
	req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/meter/usage", body)
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("meter usage request failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	buf := make([]byte, 4096)
	n, _ := resp.Body.Read(buf)
	output := string(buf[:n])
	if !strings.Contains(output, `"APIKey":"k1"`) {
		t.Fatalf("expected APIKey in output, got: %s", output)
	}
}
