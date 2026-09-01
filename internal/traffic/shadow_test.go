package traffic

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestShadowTesterRunAsync(t *testing.T) {
	s := NewShadowTester()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req models.LLMRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode error: %v", err)
		}
		if req.Model != "gpt-4" {
			t.Fatalf("unexpected model: %s", req.Model)
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(models.LLMResponse{Model: "gpt-4"})
	}))
	defer server.Close()

	done := make(chan struct{})
	go func() {
		s.RunAsync(context.Background(), server.URL, "sk-test", &models.LLMRequest{Model: "gpt-4"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shadow test timed out")
	}
}
