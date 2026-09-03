package pqc

import (
	"encoding/json"
	"net/http"
)

// KeyResponse represents the JSON payload for key material responses.
type KeyResponse struct {
	Algorithm  string `json:"algorithm"`
	PublicKey  []byte `json:"public_key,omitempty"`
	Ciphertext []byte `json:"ciphertext,omitempty"`
	SharedSecret []byte `json:"shared_secret,omitempty"`
}

// HandshakeHandler returns an HTTP handler that performs hybrid key exchange.
func HandshakeHandler(km *QuantumSafeKeyManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		pub, _, err := km.GenerateKeyPair(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(w).Encode(KeyResponse{Algorithm: AlgorithmHybridEd25519MLDSA65})
			return
		}
		_ = json.NewEncoder(w).Encode(KeyResponse{Algorithm: AlgorithmHybridEd25519MLDSA65, PublicKey: pub})
	}
}
