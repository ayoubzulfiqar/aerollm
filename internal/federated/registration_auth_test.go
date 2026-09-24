package federated

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/ayoubzulfiqar/aerollm/internal/persist"
)

const testToken = "0123456789abcdef-registration"

func TestNewRegistrationAuthValidation(t *testing.T) {
	_, err := NewRegistrationAuth(RegistrationAuthConfig{})
	require.ErrorIs(t, err, ErrRegistrationAuthNotConfigured)
	_, err = NewRegistrationAuth(RegistrationAuthConfig{Tokens: []string{"short"}})
	require.Error(t, err)
	_, err = NewRegistrationAuth(RegistrationAuthConfig{OperatorKeys: []ed25519.PublicKey{[]byte("bad")}})
	require.Error(t, err)
	_, err = NewRegistrationAuth(RegistrationAuthConfig{Tokens: []string{testToken}, MaxClockSkew: -1})
	require.Error(t, err)

	var nilAuth *RegistrationAuth
	_, err = nilAuth.Authorize(&NodeRegistration{NodeID: "n1"}, RegistrationCredentials{Token: testToken})
	require.ErrorIs(t, err, ErrRegistrationAuthNotConfigured)
}

func TestRegistrationAuthToken(t *testing.T) {
	auth, err := NewRegistrationAuth(RegistrationAuthConfig{Tokens: []string{"another-token-0000000", testToken}})
	require.NoError(t, err)
	reg := &NodeRegistration{NodeID: "n1"}

	m, err := auth.Authorize(reg, RegistrationCredentials{Token: testToken})
	require.NoError(t, err)
	require.Equal(t, AuthMethodToken, m)

	for _, bad := range []string{"", "0123456789abcdef-registratioN", testToken + "x", testToken[:len(testToken)-1]} {
		_, err := auth.Authorize(reg, RegistrationCredentials{Token: bad})
		require.ErrorIs(t, err, ErrRegistrationUnauthorized, "token %q", bad)
	}
	// Malformed registrations are reported as such, not as auth failures.
	_, err = auth.Authorize(&NodeRegistration{NodeID: "bad id"}, RegistrationCredentials{Token: testToken})
	require.ErrorIs(t, err, ErrInvalidRegistration)
	// The token is not retained in clear text.
	require.NotContains(t, fmt.Sprintf("%#v", auth), testToken)
}

func TestRegistrationAuthOperatorAttestation(t *testing.T) {
	opPub, opPriv, _ := ed25519.GenerateKey(nil)
	_, otherPriv, _ := ed25519.GenerateKey(nil)
	nodePub, _, _ := ed25519.GenerateKey(nil)
	clk := newFakeClock()
	auth, err := NewRegistrationAuth(RegistrationAuthConfig{
		OperatorKeys:           []ed25519.PublicKey{opPub},
		MaxAttestationValidity: time.Hour,
		MaxClockSkew:           time.Minute,
		Now:                    clk.Now,
	})
	require.NoError(t, err)
	reg := &NodeRegistration{NodeID: "n1", Endpoint: "https://n1.example", PublicKey: nodePub, Algorithms: []string{"fedavg"}}
	now := clk.Now()
	attest := func(priv ed25519.PrivateKey, r *NodeRegistration, iss, exp time.Time) RegistrationCredentials {
		sig, err := SignRegistration(priv, r, iss, exp)
		require.NoError(t, err)
		return RegistrationCredentials{OperatorSignature: sig, IssuedAt: iss.Unix(), ExpiresAt: exp.Unix()}
	}

	good := attest(opPriv, reg, now, now.Add(30*time.Minute))
	m, err := auth.Authorize(reg, good)
	require.NoError(t, err)
	require.Equal(t, AuthMethodOperator, m)

	fail := map[string]struct {
		reg  *NodeRegistration
		cred RegistrationCredentials
	}{
		"wrong operator":       {reg, attest(otherPriv, reg, now, now.Add(time.Minute))},
		"expired":              {reg, attest(opPriv, reg, now.Add(-2*time.Hour), now.Add(-90*time.Minute))},
		"issued in the future": {reg, attest(opPriv, reg, now.Add(10*time.Minute), now.Add(20*time.Minute))},
		"validity too long":    {reg, attest(opPriv, reg, now, now.Add(2*time.Hour))},
		"endpoint swapped": {
			&NodeRegistration{NodeID: "n1", Endpoint: "https://evil.example", PublicKey: nodePub, Algorithms: []string{"fedavg"}},
			good,
		},
		"key swapped": {
			&NodeRegistration{NodeID: "n1", Endpoint: "https://n1.example", PublicKey: opPub, Algorithms: []string{"fedavg"}},
			good,
		},
		"node renamed": {
			&NodeRegistration{NodeID: "n2", Endpoint: "https://n1.example", PublicKey: nodePub, Algorithms: []string{"fedavg"}},
			good,
		},
		"expiry extended": {reg, RegistrationCredentials{OperatorSignature: good.OperatorSignature, IssuedAt: good.IssuedAt, ExpiresAt: good.ExpiresAt + 60}},
		"no key":          {&NodeRegistration{NodeID: "n1", Endpoint: "https://n1.example", Algorithms: []string{"fedavg"}}, good},
		"short signature": {reg, RegistrationCredentials{OperatorSignature: good.OperatorSignature[:10], IssuedAt: good.IssuedAt, ExpiresAt: good.ExpiresAt}},
		"overflowing validity": {reg, RegistrationCredentials{
			OperatorSignature: good.OperatorSignature, IssuedAt: 1, ExpiresAt: 1 << 62,
		}},
		"token not configured": {reg, RegistrationCredentials{Token: testToken}},
	}
	for name, tc := range fail {
		t.Run(name, func(t *testing.T) {
			_, err := auth.Authorize(tc.reg, tc.cred)
			require.ErrorIs(t, err, ErrRegistrationUnauthorized)
		})
	}

	// The attestation expires.
	clk.Advance(31 * time.Minute)
	_, err = auth.Authorize(reg, good)
	require.ErrorIs(t, err, ErrRegistrationUnauthorized)

	// SignRegistration input validation.
	_, err = SignRegistration(nil, reg, now, now.Add(time.Minute))
	require.Error(t, err)
	_, err = SignRegistration(opPriv, &NodeRegistration{NodeID: "n1"}, now, now.Add(time.Minute))
	require.ErrorIs(t, err, ErrInvalidRegistration)
	_, err = SignRegistration(opPriv, reg, now, now)
	require.ErrorIs(t, err, ErrInvalidRegistration)
}

func TestRegistrationPayloadFormat(t *testing.T) {
	reg := &NodeRegistration{NodeID: "n1", Endpoint: "https://n1", PublicKey: []byte{0xab, 0xcd}, Algorithms: []string{"a", "b"}}
	require.Equal(t,
		"aerollm/federated/register/v1\nnode_id=n1\nendpoint=https://n1\npublic_key=abcd\nalgorithms=a,b\nissued_at=10\nexpires_at=20",
		string(RegistrationPayload(reg, 10, 20)))
	require.Nil(t, RegistrationPayload(nil, 0, 0))
	// URLs with control characters are rejected, so fields cannot inject lines.
	require.Error(t, (&NodeRegistration{NodeID: "n1", Endpoint: "https://n1\nissued_at=1"}).Validate())
}

func TestRegisterAttestedRevocation(t *testing.T) {
	g := NewGatewayRegistry()
	ctx := context.Background()
	pub, _, _ := ed25519.GenerateKey(nil)
	reg := &NodeRegistration{NodeID: "n1", PublicKey: pub}
	issued := time.Now().Add(-time.Minute)

	require.Error(t, g.RegisterAttested(ctx, reg, time.Time{}))
	require.NoError(t, g.RegisterAttested(ctx, reg, issued))
	ok, err := g.DeregisterNode(ctx, "n1")
	require.NoError(t, err)
	require.True(t, ok)
	_, has := g.DeregisteredAt("n1")
	require.True(t, has)

	// Replaying the old attestation after deregistration is refused.
	require.ErrorIs(t, g.RegisterAttested(ctx, reg, issued), ErrAttestationRevoked)
	// A fresh attestation (issued after the deregistration) works.
	require.NoError(t, g.RegisterAttested(ctx, reg, time.Now().Add(time.Second)))
	// Token/admin registrations are not subject to tombstones.
	ok, _ = g.DeregisterNode(ctx, "n1")
	require.True(t, ok)
	require.NoError(t, g.Register(ctx, reg))

	ok, err = g.DeregisterNode(ctx, "missing")
	require.NoError(t, err)
	require.False(t, ok)
}

func TestGatewayRegistryPersistence(t *testing.T) {
	ps := persist.NewMemory()
	ctx := context.Background()
	pub1, _, _ := ed25519.GenerateKey(nil)
	pub2, _, _ := ed25519.GenerateKey(nil)

	g := NewGatewayRegistry()
	require.NoError(t, g.EnablePersistence(ps))
	require.Error(t, g.EnablePersistence(ps))
	require.NoError(t, g.Register(ctx, &NodeRegistration{NodeID: "n1", Endpoint: "https://n1", PublicKey: pub1, Algorithms: []string{"fedavg"}}))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, g.Register(ctx, &NodeRegistration{NodeID: "n2", PublicKey: pub2}))
	time.Sleep(2 * time.Millisecond)
	require.NoError(t, g.Register(ctx, &NodeRegistration{NodeID: "n3"}))
	require.True(t, g.Deregister(ctx, "n3"))

	g2 := NewGatewayRegistry()
	require.NoError(t, g2.EnablePersistence(ps))
	require.Equal(t, 2, g2.Len())
	n1, ok := g2.Node("n1")
	require.True(t, ok)
	require.Equal(t, "https://n1", n1.Endpoint)
	require.Equal(t, []string{"fedavg"}, n1.Algorithms)
	k, ok := g2.PublicKey("n1")
	require.True(t, ok)
	require.True(t, bytes.Equal(pub1, k))
	require.Equal(t, "n2", g2.Latest().NodeID)
	require.Len(t, g2.History(), 2)
	// Key pinning survives restarts.
	require.ErrorIs(t, g2.Register(ctx, &NodeRegistration{NodeID: "n1", PublicKey: pub2}), ErrNodeKeyConflict)
	// Tombstones survive restarts.
	_, has := g2.DeregisteredAt("n3")
	require.True(t, has)
	require.ErrorIs(t, g2.RegisterAttested(ctx, &NodeRegistration{NodeID: "n3"}, time.Now().Add(-time.Hour)), ErrAttestationRevoked)

	// A non-empty registry cannot enable persistence.
	g3 := NewGatewayRegistry()
	require.NoError(t, g3.Register(ctx, &NodeRegistration{NodeID: "x"}))
	require.Error(t, g3.EnablePersistence(persist.NewMemory()))

	// Undecodable documents are skipped and reported; corrupt tombstones
	// fail closed.
	bad := persist.NewMemory()
	require.NoError(t, bad.Put(bucketNodes, "n1", persistedNode{NodeID: "n1"}))
	require.NoError(t, bad.Put(bucketNodes, "n2", "garbage"))
	require.NoError(t, bad.Put(bucketNodes, "n3", persistedNode{NodeID: "other"}))
	require.NoError(t, bad.Put(bucketTombstones, "n4", "garbage"))
	g4 := NewGatewayRegistry()
	err := g4.EnablePersistence(bad)
	require.Error(t, err)
	require.Equal(t, 1, g4.Len())
	_, has = g4.DeregisteredAt("n4")
	require.True(t, has)
}

func TestGatewayRegistryPersistenceFailure(t *testing.T) {
	ps := newFailingStore()
	ctx := context.Background()
	g := NewGatewayRegistry()
	require.NoError(t, g.EnablePersistence(ps))
	require.NoError(t, g.Register(ctx, &NodeRegistration{NodeID: "n1"}))

	ps.failPut.Store(true)
	require.ErrorIs(t, g.Register(ctx, &NodeRegistration{NodeID: "n2"}), ErrPersistence)
	require.Equal(t, 1, g.Len())
	ok, err := g.DeregisterNode(ctx, "n1")
	require.ErrorIs(t, err, ErrPersistence)
	require.False(t, ok)
	require.False(t, g.Deregister(ctx, "n1"))
	require.Equal(t, 1, g.Len(), "failed deregistration leaves the node registered")

	ps.failPut.Store(false)
	ps.failDelete.Store(true)
	_, err = g.DeregisterNode(ctx, "n1")
	require.ErrorIs(t, err, ErrPersistence)
	require.Equal(t, 1, g.Len())
}

func TestRegisterNodeHandlerWithAuth(t *testing.T) {
	opPub, opPriv, _ := ed25519.GenerateKey(nil)
	nodePub, _, _ := ed25519.GenerateKey(nil)
	auth, err := NewRegistrationAuth(RegistrationAuthConfig{Tokens: []string{testToken}, OperatorKeys: []ed25519.PublicKey{opPub}})
	require.NoError(t, err)
	registry := NewGatewayRegistry()
	h := RegisterNodeHandlerWithAuth(registry, auth)

	do := func(body string, token string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/federated/nodes/register", strings.NewReader(body))
		if token != "" {
			req.Header.Set(RegistrationTokenHeader, token)
		}
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}
	status := func(rec *httptest.ResponseRecorder) map[string]string {
		var out map[string]string
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		return out
	}

	// No credentials.
	rec := do(`{"node_id":"n1"}`, "")
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, "registration not authorized", status(rec)["error"])
	require.Equal(t, 0, registry.Len())
	// Wrong token.
	require.Equal(t, http.StatusUnauthorized, do(`{"node_id":"n1"}`, "wrong-token-00000000").Code)
	// Token.
	rec = do(`{"node_id":"n1","public_key":"`+hex.EncodeToString(nodePub)+`"}`, testToken)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "token", status(rec)["auth"])

	// Operator attestation.
	pub2, _, _ := ed25519.GenerateKey(nil)
	reg := &NodeRegistration{NodeID: "n2", Endpoint: "https://n2.example", PublicKey: pub2}
	iss := time.Now().Add(-time.Second)
	exp := iss.Add(time.Hour)
	sig, err := SignRegistration(opPriv, reg, iss, exp)
	require.NoError(t, err)
	body := fmt.Sprintf(`{"node_id":"n2","endpoint":"https://n2.example","public_key":%q,"issued_at":%d,"expires_at":%d,"operator_signature":%q}`,
		base64.StdEncoding.EncodeToString(pub2), iss.Unix(), exp.Unix(), base64.StdEncoding.EncodeToString(sig))
	rec = do(body, "")
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "operator_signature", status(rec)["auth"])

	// After deregistration the same attestation is revoked.
	require.True(t, registry.Deregister(context.Background(), "n2"))
	rec = do(body, "")
	require.Equal(t, http.StatusUnauthorized, rec.Code)
	require.Equal(t, 1, registry.Len())

	// Tampered attestation.
	tampered := strings.Replace(body, "https://n2.example", "https://evil.example", 1)
	require.Equal(t, http.StatusUnauthorized, do(tampered, "").Code)

	// Malformed input.
	require.Equal(t, http.StatusBadRequest, do(`{"node_id":"n3","operator_signature":"zz"}`, testToken).Code)
	require.Equal(t, http.StatusBadRequest, do(`{"node_id":"n3","public_key":"zz"}`, testToken).Code)
	require.Equal(t, http.StatusBadRequest, do(`{"node_id":"bad id"}`, testToken).Code)
	require.Equal(t, http.StatusBadRequest, do(`{`, testToken).Code)

	// Key takeover via token is still blocked by pinning.
	other, _, _ := ed25519.GenerateKey(nil)
	require.Equal(t, http.StatusConflict, do(`{"node_id":"n1","public_key":"`+hex.EncodeToString(other)+`"}`, testToken).Code)

	// Fail closed without auth or registry; method check.
	rec = httptest.NewRecorder()
	RegisterNodeHandlerWithAuth(registry, nil)(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"node_id":"n9"}`)))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	rec = httptest.NewRecorder()
	RegisterNodeHandlerWithAuth(nil, auth)(rec, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`)))
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	require.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}
