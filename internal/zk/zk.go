package zk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
)

// ConfidentialCompute defines the mock interface for privacy-preserving computation.
type ConfidentialCompute interface {
	// Evaluate processes an encrypted payload and returns an encrypted result.
	Evaluate(ctx context.Context, ciphertext []byte) ([]byte, error)
}

// middlewareCompute is the default no-op confidential compute implementation.
type middlewareCompute struct{}

func (middlewareCompute) Evaluate(_ context.Context, ciphertext []byte) ([]byte, error) {
	return ciphertext, nil
}

// Middleware returns an HTTP middleware that handles encrypted payloads without decryption.
func Middleware(compute ConfidentialCompute) func(http.Handler) http.Handler {
	if compute == nil {
		compute = &middlewareCompute{}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ct := r.Header.Get("X-Encrypted-Payload")
			if ct == "" {
				next.ServeHTTP(w, r)
				return
			}
			ciphertext, err := base64.StdEncoding.DecodeString(ct)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"invalid encrypted payload: %v"}`, err), http.StatusBadRequest)
				return
			}
			out, err := compute.Evaluate(r.Context(), ciphertext)
			if err != nil {
				http.Error(w, fmt.Sprintf(`{"error":"confidential compute failed: %v"}`, err), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]string{
				"ciphertext": base64.StdEncoding.EncodeToString(out),
			})
		})
	}
}
