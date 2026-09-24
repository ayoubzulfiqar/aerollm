package pqc

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"
)

// KeyResponse is the JSON payload of the handshake endpoint. It never
// contains private keys or shared secrets.
type KeyResponse struct {
	Algorithm string `json:"algorithm"`
	PublicKey []byte `json:"public_key,omitempty"`
	// Ciphertext is the KEM ciphertext encapsulated to the client's key.
	Ciphertext []byte `json:"ciphertext,omitempty"`
	// SharedSecret is never populated; it is kept for wire compatibility.
	//
	// Deprecated: shared secrets are never sent over the wire.
	SharedSecret []byte `json:"shared_secret,omitempty"`

	KEMAlgorithm         string `json:"kem_algorithm,omitempty"`
	SignatureAlgorithm   string `json:"signature_algorithm,omitempty"`
	PostQuantumKEM       bool   `json:"post_quantum_kem"`
	PostQuantumSignature bool   `json:"post_quantum_signature"`
	// SigningPublicKey is the server's Ed25519 key (hybrid suites).
	SigningPublicKey []byte `json:"signing_public_key,omitempty"`
	// SessionID identifies the server-side session secret established by a
	// POST handshake.
	SessionID string `json:"session_id,omitempty"`
	// Signature is the server's signature over the handshake transcript.
	Signature []byte `json:"signature,omitempty"`
	Error     string `json:"error,omitempty"`
}

// HandshakeRequest is the optional POST body of the handshake endpoint.
// Exactly one of PublicKey (client KEM public key: the server encapsulates
// to it) or Ciphertext (a ciphertext the client encapsulated to the server's
// public key) may be set. An empty body returns the server's public key.
type HandshakeRequest struct {
	PublicKey  []byte `json:"public_key,omitempty"`
	Ciphertext []byte `json:"ciphertext,omitempty"`
}

const (
	maxHandshakeBody = 64 << 10
	maxSessions      = 1024
	// SessionTTL bounds how long an established handshake secret is kept.
	SessionTTL = 5 * time.Minute
)

type serverIdentity struct {
	pub  PublicKey
	priv PrivateKey
}

type session struct {
	secret  []byte
	expires time.Time
}

// identity returns the manager's long-lived server key pair, generating it
// once. Generation failures are not cached, so a transient failure can be
// retried, but a successful identity is never regenerated per request.
func (k *QuantumSafeKeyManager) serverIdentity(ctx context.Context) (*serverIdentity, error) {
	k.idMu.Lock()
	defer k.idMu.Unlock()
	if k.identity != nil {
		return k.identity, nil
	}
	pub, priv, err := k.GenerateKeyPair(ctx)
	if err != nil {
		return nil, err
	}
	k.identity = &serverIdentity{pub: pub, priv: priv}
	return k.identity, nil
}

// ServerPublicKey returns the manager's long-lived public key used by
// HandshakeHandler.
func (k *QuantumSafeKeyManager) ServerPublicKey(ctx context.Context) (PublicKey, error) {
	id, err := k.serverIdentity(ctx)
	if err != nil {
		return nil, err
	}
	return append(PublicKey(nil), id.pub...), nil
}

func (k *QuantumSafeKeyManager) storeSession(secret []byte) (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	id := hex.EncodeToString(b)
	now := time.Now()
	k.sessMu.Lock()
	defer k.sessMu.Unlock()
	for sid, s := range k.sessions {
		if now.After(s.expires) {
			clear(s.secret)
			delete(k.sessions, sid)
		}
	}
	if len(k.sessions) >= maxSessions {
		// Evict the session closest to expiry.
		var oldest string
		var oldestExp time.Time
		for sid, s := range k.sessions {
			if oldest == "" || s.expires.Before(oldestExp) {
				oldest, oldestExp = sid, s.expires
			}
		}
		clear(k.sessions[oldest].secret)
		delete(k.sessions, oldest)
	}
	k.sessions[id] = session{secret: secret, expires: now.Add(SessionTTL)}
	return id, nil
}

// TakeSessionSecret returns and forgets the shared secret established by a
// handshake. Each secret can be taken once; expired sessions return false.
func (k *QuantumSafeKeyManager) TakeSessionSecret(id string) ([]byte, bool) {
	k.sessMu.Lock()
	defer k.sessMu.Unlock()
	s, ok := k.sessions[id]
	if !ok {
		return nil, false
	}
	delete(k.sessions, id)
	if time.Now().After(s.expires) {
		clear(s.secret)
		return nil, false
	}
	return s.secret, true
}

func transcript(sessionID string, serverPub, clientMaterial, ciphertext []byte) []byte {
	h := sha256.New()
	h.Write([]byte("aerollm-pqc-handshake-v1"))
	for _, part := range [][]byte{[]byte(sessionID), serverPub, clientMaterial, ciphertext} {
		var l [8]byte
		binary.BigEndian.PutUint64(l[:], uint64(len(part)))
		h.Write(l[:])
		h.Write(part)
	}
	return h.Sum(nil)
}

func writeKeyJSON(w http.ResponseWriter, status int, v KeyResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// HandshakeHandler returns an HTTP handler for post-quantum key exchange.
//
//	GET  (or POST with empty body): the server's long-lived public key and
//	     suite description. The key pair is generated once per manager, not
//	     per request.
//	POST {"public_key": <client KEM public key>}: the server encapsulates a
//	     fresh secret to the client key and returns the ciphertext.
//	POST {"ciphertext": <encapsulated to the server key>}: the server
//	     decapsulates it.
//
// In both POST flows the secret is kept server-side under the returned
// session_id (see TakeSessionSecret) and the transcript is signed with the
// server's Ed25519 key when the suite supports signatures. Private keys and
// shared secrets are never returned. Other methods get 405; bodies are
// capped at 64 KiB.
func HandshakeHandler(km *QuantumSafeKeyManager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodPost {
			w.Header().Set("Allow", "GET, POST")
			writeKeyJSON(w, http.StatusMethodNotAllowed, KeyResponse{Error: "method not allowed"})
			return
		}
		if km == nil {
			writeKeyJSON(w, http.StatusServiceUnavailable, KeyResponse{Error: "pqc key manager not configured"})
			return
		}
		suite, err := km.Suite()
		if err != nil {
			status := http.StatusInternalServerError
			if errors.Is(err, ErrMLDSAUnavailable) {
				status = http.StatusNotImplemented
			}
			writeKeyJSON(w, status, KeyResponse{Algorithm: km.Algorithm(), Error: err.Error()})
			return
		}
		id, err := km.serverIdentity(r.Context())
		if err != nil {
			writeKeyJSON(w, http.StatusInternalServerError, KeyResponse{Algorithm: km.Algorithm(), Error: "key generation failed"})
			return
		}
		resp := KeyResponse{
			Algorithm:            km.Algorithm(),
			PublicKey:            id.pub,
			KEMAlgorithm:         suite.KEM,
			SignatureAlgorithm:   suite.Signature,
			PostQuantumKEM:       suite.PostQuantumKEM,
			PostQuantumSignature: suite.PostQuantumSignature,
		}
		if edPub, _, ok := decodeComposite(magicPublic, id.pub); ok {
			resp.SigningPublicKey = edPub
		}

		var req HandshakeRequest
		if r.Method == http.MethodPost && r.Body != nil && r.Body != http.NoBody {
			body, err := io.ReadAll(io.LimitReader(r.Body, maxHandshakeBody+1))
			if err != nil {
				writeKeyJSON(w, http.StatusBadRequest, KeyResponse{Error: "failed to read body"})
				return
			}
			if len(body) > maxHandshakeBody {
				writeKeyJSON(w, http.StatusRequestEntityTooLarge, KeyResponse{Error: "request body too large"})
				return
			}
			if len(body) > 0 {
				if err := json.Unmarshal(body, &req); err != nil {
					writeKeyJSON(w, http.StatusBadRequest, KeyResponse{Error: "invalid JSON body"})
					return
				}
			}
		}
		if len(req.PublicKey) > 0 && len(req.Ciphertext) > 0 {
			writeKeyJSON(w, http.StatusBadRequest, KeyResponse{Error: "send either public_key or ciphertext, not both"})
			return
		}
		if len(req.PublicKey) == 0 && len(req.Ciphertext) == 0 {
			writeKeyJSON(w, http.StatusOK, resp)
			return
		}

		var secret, clientMaterial, ct []byte
		if len(req.PublicKey) > 0 {
			ct, secret, err = km.Encapsulate(r.Context(), req.PublicKey)
			clientMaterial = req.PublicKey
			resp.Ciphertext = ct
		} else {
			secret, err = km.Decapsulate(r.Context(), req.Ciphertext, id.priv)
			clientMaterial, ct = nil, req.Ciphertext
		}
		if err != nil {
			writeKeyJSON(w, http.StatusBadRequest, KeyResponse{Algorithm: km.Algorithm(), Error: "invalid key material"})
			return
		}
		sid, err := km.storeSession(secret)
		if err != nil {
			writeKeyJSON(w, http.StatusInternalServerError, KeyResponse{Error: "session setup failed"})
			return
		}
		resp.SessionID = sid
		if suite.Signature != "" {
			if sig, err := km.Sign(r.Context(), id.priv, transcript(sid, id.pub, clientMaterial, ct)); err == nil {
				resp.Signature = sig
			}
		}
		writeKeyJSON(w, http.StatusOK, resp)
	}
}

// VerifyHandshake lets a client check the server's transcript signature on
// a POST handshake response. clientPublicKey is the key the client sent (nil
// for the ciphertext flow) and ciphertext the KEM ciphertext.
func VerifyHandshake(ctx context.Context, km KeyManager, resp *KeyResponse, clientPublicKey, ciphertext []byte) error {
	if resp == nil || len(resp.Signature) == 0 {
		return errors.New("pqc: unsigned handshake")
	}
	return km.Verify(ctx, resp.PublicKey, transcript(resp.SessionID, resp.PublicKey, clientPublicKey, ciphertext), resp.Signature)
}
