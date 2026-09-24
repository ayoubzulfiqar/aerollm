package federated

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	// MaxRegisterBodyBytes caps the node registration request body.
	MaxRegisterBodyBytes = 64 << 10
	// MaxAggregateBodyBytes caps the aggregate request body. JSON-encoded
	// floats need roughly 20 bytes each, so this admits a few 1<<20-element
	// matrices or many small ones while bounding memory per request.
	MaxAggregateBodyBytes = 32 << 20
)

// RegisterNodeRequest is the request payload for node registration.
// PublicKey is a 32-byte ed25519 public key encoded as hex or base64
// (standard or URL alphabet, padded or raw).
//
// The attestation fields are used by RegisterNodeHandlerWithAuth:
// OperatorSignature (hex or base64) is an operator's ed25519 signature over
// RegistrationPayload(registration, IssuedAt, ExpiresAt), with IssuedAt and
// ExpiresAt in Unix seconds.
type RegisterNodeRequest struct {
	NodeID            string   `json:"node_id"`
	Endpoint          string   `json:"endpoint"`
	PublicKey         string   `json:"public_key"`
	Algorithms        []string `json:"algorithms"`
	IssuedAt          int64    `json:"issued_at,omitempty"`
	ExpiresAt         int64    `json:"expires_at,omitempty"`
	OperatorSignature string   `json:"operator_signature,omitempty"`
}

// writeJSONError writes {"error": msg} with the given status.
func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// allowMethods writes 405 with an Allow header when r.Method is not allowed.
func allowMethods(w http.ResponseWriter, r *http.Request, methods ...string) bool {
	if r != nil {
		for _, m := range methods {
			if r.Method == m {
				return true
			}
		}
	}
	w.Header().Set("Allow", strings.Join(methods, ", "))
	writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
	return false
}

// decodeJSONBody decodes a single JSON value from a size-capped body and
// writes the appropriate error response on failure.
func decodeJSONBody(w http.ResponseWriter, r *http.Request, limit int64, dst interface{}) bool {
	if r.Body == nil {
		writeJSONError(w, http.StatusBadRequest, "missing body")
		return false
	}
	defer r.Body.Close()
	body := http.MaxBytesReader(w, r.Body, limit)
	dec := json.NewDecoder(body)
	if err := dec.Decode(dst); err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return false
		}
		writeJSONError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	// Reject trailing data after the JSON value.
	if _, err := dec.Token(); err != io.EOF {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return false
		}
		writeJSONError(w, http.StatusBadRequest, "unexpected data after JSON body")
		return false
	}
	return true
}

// DecodePublicKey decodes a 32-byte ed25519 public key given as hex or
// base64 (standard/URL alphabet, padded or raw). An empty string yields nil.
func DecodePublicKey(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if b, err := hex.DecodeString(s); err == nil && len(b) == ed25519.PublicKeySize {
		return b, nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) == ed25519.PublicKeySize {
			return b, nil
		}
	}
	return nil, fmt.Errorf("%w: public_key must be a hex or base64 encoded %d-byte ed25519 key", ErrInvalidRegistration, ed25519.PublicKeySize)
}

// decodeSignature decodes a 64-byte ed25519 signature given as hex or base64
// (standard/URL alphabet, padded or raw). An empty string yields nil.
func decodeSignature(s string) ([]byte, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}
	if b, err := hex.DecodeString(s); err == nil && len(b) == ed25519.SignatureSize {
		return b, nil
	}
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(s); err == nil && len(b) == ed25519.SignatureSize {
			return b, nil
		}
	}
	return nil, fmt.Errorf("%w: operator_signature must be a hex or base64 encoded %d-byte ed25519 signature", ErrInvalidRegistration, ed25519.SignatureSize)
}

// writeRegisterError maps registry errors to HTTP responses.
func writeRegisterError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNodeKeyConflict):
		writeJSONError(w, http.StatusConflict, "node already registered with a different public key")
	case errors.Is(err, ErrRegistryFull):
		writeJSONError(w, http.StatusServiceUnavailable, "node registry is full")
	case errors.Is(err, ErrAttestationRevoked):
		writeJSONError(w, http.StatusUnauthorized, "registration not authorized")
	case errors.Is(err, ErrPersistence):
		writeJSONError(w, http.StatusServiceUnavailable, "registry persistence unavailable")
	case errors.Is(err, ErrInvalidRegistration):
		// Validation messages are generated by this package and contain
		// no internal state, so they are safe to return.
		writeJSONError(w, http.StatusBadRequest, err.Error())
	default:
		writeJSONError(w, http.StatusBadRequest, "register failed")
	}
}

// RegisterNodeHandler returns an HTTP handler (POST) that registers a
// federated node. It does not authenticate the caller; mount it behind the
// gateway's admin auth middleware, or use RegisterNodeHandlerWithAuth for
// node self-registration.
func RegisterNodeHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodPost) {
			return
		}
		if registry == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "registry not initialized")
			return
		}
		var req RegisterNodeRequest
		if !decodeJSONBody(w, r, MaxRegisterBodyBytes, &req) {
			return
		}
		pub, err := DecodePublicKey(req.PublicKey)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid public_key")
			return
		}
		err = registry.Register(r.Context(), &NodeRegistration{
			NodeID:     req.NodeID,
			Endpoint:   req.Endpoint,
			PublicKey:  pub,
			Algorithms: req.Algorithms,
		})
		if err != nil {
			writeRegisterError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
	}
}

// RegisterNodeHandlerWithAuth returns an HTTP handler (POST) that registers a
// federated node only if the request carries a valid credential:
//
//   - a registration token in the X-AeroLLM-Federation-Token header
//     (compared in constant time against the configured tokens), or
//   - an operator attestation in the body (operator_signature, issued_at,
//     expires_at) signed by a pre-trusted operator key over
//     RegistrationPayload. Attested registrations are refused if the node
//     was deregistered at or after issued_at.
//
// Authentication failures yield 401 with a generic message. A nil auth fails
// closed with 503. Because the credential is checked here, the handler can be
// mounted outside the gateway's admin auth so nodes can self-register.
func RegisterNodeHandlerWithAuth(registry *GatewayRegistry, auth *RegistrationAuth) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodPost) {
			return
		}
		if registry == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "registry not initialized")
			return
		}
		if auth == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "registration authentication not configured")
			return
		}
		var req RegisterNodeRequest
		if !decodeJSONBody(w, r, MaxRegisterBodyBytes, &req) {
			return
		}
		pub, err := DecodePublicKey(req.PublicKey)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid public_key")
			return
		}
		sig, err := decodeSignature(req.OperatorSignature)
		if err != nil {
			writeJSONError(w, http.StatusBadRequest, "invalid operator_signature")
			return
		}
		reg := &NodeRegistration{
			NodeID:     req.NodeID,
			Endpoint:   req.Endpoint,
			PublicKey:  pub,
			Algorithms: req.Algorithms,
		}
		method, err := auth.Authorize(reg, RegistrationCredentials{
			Token:             r.Header.Get(RegistrationTokenHeader),
			OperatorSignature: sig,
			IssuedAt:          req.IssuedAt,
			ExpiresAt:         req.ExpiresAt,
		})
		if err != nil {
			if errors.Is(err, ErrInvalidRegistration) {
				writeJSONError(w, http.StatusBadRequest, err.Error())
				return
			}
			writeJSONError(w, http.StatusUnauthorized, "registration not authorized")
			return
		}
		if method == AuthMethodOperator {
			err = registry.RegisterAttested(r.Context(), reg, time.Unix(req.IssuedAt, 0))
		} else {
			err = registry.Register(r.Context(), reg)
		}
		if err != nil {
			writeRegisterError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "registered", "auth": string(method)})
	}
}

// LatestNodeHandler returns an HTTP handler (GET/HEAD) that serves the latest
// registered node.
func LatestNodeHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
			return
		}
		node := registry.Latest()
		if node == nil {
			writeJSONError(w, http.StatusNotFound, "no nodes")
			return
		}
		writeJSON(w, http.StatusOK, node)
	}
}

// NodeHistoryHandler returns an HTTP handler (GET/HEAD) that serves the
// retained registration history.
func NodeHistoryHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
			return
		}
		history := registry.History()
		if history == nil {
			history = []*NodeRegistration{}
		}
		writeJSON(w, http.StatusOK, history)
	}
}

// NodeHandler returns an HTTP handler (GET/HEAD) that serves a node by ID
// (?id=...).
func NodeHandler(registry *GatewayRegistry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodGet, http.MethodHead) {
			return
		}
		nodeID := ""
		if r.URL != nil {
			nodeID = r.URL.Query().Get("id")
		}
		if nodeID == "" {
			writeJSONError(w, http.StatusBadRequest, "missing id")
			return
		}
		node, ok := registry.Node(nodeID)
		if !ok {
			writeJSONError(w, http.StatusNotFound, "not found")
			return
		}
		writeJSON(w, http.StatusOK, node)
	}
}

// AggregateHandler returns a hardened HTTP handler (POST) that decodes a JSON
// array of LoRAMatrix updates (body capped at MaxAggregateBodyBytes) and
// aggregates them. Validation failures yield 400, oversized bodies 413.
func AggregateHandler(agg FederatedAggregator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !allowMethods(w, r, http.MethodPost) {
			return
		}
		if agg == nil {
			writeJSONError(w, http.StatusServiceUnavailable, "aggregator not initialized")
			return
		}
		var updates []*LoRAMatrix
		if !decodeJSONBody(w, r, MaxAggregateBodyBytes, &updates) {
			return
		}
		if len(updates) > MaxUpdates {
			writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("too many updates (max %d)", MaxUpdates))
			return
		}
		out, err := agg.Aggregate(r.Context(), updates)
		if err != nil {
			switch {
			case errors.Is(err, ErrNoUpdates), errors.Is(err, ErrTooManyUpdates),
				errors.Is(err, ErrInvalidMatrix), errors.Is(err, ErrDimensionMismatch),
				errors.Is(err, ErrInvalidWeights):
				writeJSONError(w, http.StatusBadRequest, err.Error())
			default:
				writeJSONError(w, http.StatusBadRequest, "aggregate failed")
			}
			return
		}
		writeJSON(w, http.StatusOK, out)
	}
}

// PrintLatestNode prints the latest node registration.
func PrintLatestNode(registry *GatewayRegistry) {
	node := registry.Latest()
	if node == nil {
		fmt.Println("no nodes registered")
		return
	}
	fmt.Printf("latest node=%s endpoint=%s algorithms=%v\n", node.NodeID, node.Endpoint, node.Algorithms)
}

// PrintNodeHistory prints registration history.
func PrintNodeHistory(registry *GatewayRegistry) {
	history := registry.History()
	if len(history) == 0 {
		fmt.Println("no history")
		return
	}
	for i, n := range history {
		fmt.Printf("%d: node=%s endpoint=%s\n", i, n.NodeID, n.Endpoint)
	}
}
