// Package zk routes opaque encrypted payloads to a pluggable confidential
// compute backend.
//
// Despite the package name, this package implements NO zero-knowledge
// proofs and NO homomorphic encryption. It only provides a hook: when a
// request carries an X-Encrypted-Payload header, the base64-decoded
// ciphertext is handed to a ConfidentialCompute implementation (for example
// a TEE/enclave client) and its (still encrypted) result is returned. If no
// backend is configured such requests are rejected with 501 instead of being
// silently echoed back or processed in plaintext. Requests without the
// header pass through unchanged.
package zk

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"
)

// HeaderEncryptedPayload carries a base64 ciphertext.
const HeaderEncryptedPayload = "X-Encrypted-Payload"

// MaxCiphertextBytes caps the decoded ciphertext size.
const MaxCiphertextBytes = 64 << 10

// DefaultTimeout bounds a single confidential compute evaluation.
const DefaultTimeout = 30 * time.Second

// ErrNotConfigured is returned when no confidential compute backend exists.
var ErrNotConfigured = errors.New("zk: confidential compute backend not configured")

// ConfidentialCompute processes an encrypted payload and returns an
// encrypted result without the gateway ever seeing plaintext.
type ConfidentialCompute interface {
	// Evaluate processes an encrypted payload and returns an encrypted result.
	Evaluate(ctx context.Context, ciphertext []byte) ([]byte, error)
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeCiphertext(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if base64.StdEncoding.DecodedLen(len(s)) > MaxCiphertextBytes+3 {
		return nil, errors.New("too large")
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil {
			if len(b) == 0 || len(b) > MaxCiphertextBytes {
				return nil, errors.New("invalid size")
			}
			return b, nil
		}
	}
	return nil, errors.New("invalid base64")
}

// Middleware returns an HTTP middleware that forwards X-Encrypted-Payload
// ciphertexts to compute. With a nil compute such requests get 501. Mount it
// behind authentication: a configured backend answers the request itself.
func Middleware(compute ConfidentialCompute) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ct := r.Header.Get(HeaderEncryptedPayload)
			if ct == "" {
				next.ServeHTTP(w, r)
				return
			}
			if compute == nil {
				writeJSON(w, http.StatusNotImplemented, map[string]string{
					"error": "encrypted payloads are not supported: no confidential compute backend is configured",
				})
				return
			}
			ciphertext, err := decodeCiphertext(ct)
			if err != nil {
				writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid encrypted payload"})
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), DefaultTimeout)
			defer cancel()
			out, err := compute.Evaluate(ctx, ciphertext)
			if err != nil {
				status := http.StatusBadGateway
				if errors.Is(err, ErrNotConfigured) {
					status = http.StatusNotImplemented
				} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
					status = http.StatusGatewayTimeout
				}
				writeJSON(w, status, map[string]string{"error": "confidential compute failed"})
				return
			}
			writeJSON(w, http.StatusOK, map[string]string{
				"ciphertext": base64.StdEncoding.EncodeToString(out),
			})
		})
	}
}
