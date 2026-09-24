package marketplace

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func testKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("keygen: %v", err)
	}
	return pub, priv
}

func signedRequest(t *testing.T, priv ed25519.PrivateKey, id, version, creator string) PublishRequest {
	t.Helper()
	req, err := SignManifest(PublishRequest{
		ID:        id,
		Name:      "Plugin " + id,
		Version:   version,
		CreatorID: creator,
		WASMHash:  HashWASM([]byte("wasm-" + id + version)),
	}, priv)
	if err != nil {
		t.Fatalf("sign: %v", err)
	}
	return req
}

func TestSignAndVerifyManifestRoundTrip(t *testing.T) {
	pub, priv := testKey(t)
	req := signedRequest(t, priv, "plugin-1", "1.0.0", "creator")
	m, err := VerifyPublishRequest(req)
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !bytes.Equal(m.PublicKey, pub) || m.ID != "plugin-1" || len(m.Signature) != ed25519.SignatureSize {
		t.Fatalf("unexpected manifest: %+v", m)
	}
	if err := m.Verify(); err != nil {
		t.Fatalf("re-verify: %v", err)
	}

	raw, _ := json.Marshal(req)
	parsed, err := ParseAndVerifyManifest(raw)
	if err != nil {
		t.Fatalf("parse and verify: %v", err)
	}
	if !bytes.Equal(parsed.Payload, m.Payload) {
		t.Fatalf("payload mismatch")
	}
}

func TestVerifyRejectsTamperingAndBypasses(t *testing.T) {
	_, priv := testKey(t)
	good := signedRequest(t, priv, "plugin-1", "1.0.0", "creator")
	_, otherPriv := testKey(t)
	other := signedRequest(t, otherPriv, "plugin-1", "1.0.0", "creator")

	cases := map[string]func(p *PublishRequest){
		"empty signature":      func(p *PublishRequest) { p.Signature = "" },
		"empty public key":     func(p *PublishRequest) { p.PublicKey = "" },
		"garbage signature":    func(p *PublishRequest) { p.Signature = "c2ln" },
		"zero signature":       func(p *PublishRequest) { p.Signature = base64.StdEncoding.EncodeToString(make([]byte, 64)) },
		"short key":            func(p *PublishRequest) { p.PublicKey = base64.StdEncoding.EncodeToString([]byte("pk")) },
		"tampered version":     func(p *PublishRequest) { p.Version = "1.0.1" },
		"tampered name":        func(p *PublishRequest) { p.Name = "Evil" },
		"tampered creator":     func(p *PublishRequest) { p.CreatorID = "mallory" },
		"tampered hash":        func(p *PublishRequest) { p.WASMHash = HashWASM([]byte("evil")) },
		"swapped key":          func(p *PublishRequest) { p.PublicKey = other.PublicKey },
		"swapped signature":    func(p *PublishRequest) { p.Signature = other.Signature },
		"non-canonical b64":    func(p *PublishRequest) { p.Signature = strings.TrimRight(p.Signature, "=") },
		"path traversal id":    func(p *PublishRequest) { p.ID = "../etc" },
		"key injection id":     func(p *PublishRequest) { p.ID = "a:meta:b" },
		"non-semver version":   func(p *PublishRequest) { p.Version = "latest" },
		"uppercase hash":       func(p *PublishRequest) { p.WASMHash = strings.ToUpper(p.WASMHash) },
		"control char in name": func(p *PublishRequest) { p.Name = "a\nb" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			p := good
			mutate(&p)
			if _, err := VerifyPublishRequest(p); err == nil {
				t.Fatalf("expected verification failure")
			}
		})
	}
}

func TestParseAndVerifyManifestStrictDecoding(t *testing.T) {
	_, priv := testKey(t)
	req := signedRequest(t, priv, "plugin-1", "1.0.0", "creator")
	raw, _ := json.Marshal(req)

	// Unknown / alternate fields are rejected rather than ignored.
	withExtra := bytes.Replace(raw, []byte(`{`), []byte(`{"sig":"x",`), 1)
	if _, err := ParseAndVerifyManifest(withExtra); err == nil {
		t.Fatal("expected unknown field rejection")
	}
	// Trailing documents are rejected.
	if _, err := ParseAndVerifyManifest(append(append([]byte{}, raw...), []byte(`{}`)...)); err == nil {
		t.Fatal("expected trailing data rejection")
	}
	// Oversized documents are rejected.
	huge := append([]byte(`{"name":"`), bytes.Repeat([]byte("a"), MaxManifestBytes)...)
	if _, err := ParseAndVerifyManifest(huge); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
	// Empty input.
	if _, err := ParseAndVerifyManifest(nil); err == nil {
		t.Fatal("expected error for empty input")
	}
	// Duplicate keys: the verified values are the ones decoded, so a
	// duplicate that changes a field breaks the signature.
	dup := append(bytes.TrimSuffix(bytes.TrimSpace(raw), []byte(`}`)), []byte(`,"version":"9.9.9"}`)...)
	if _, err := ParseAndVerifyManifest(dup); err == nil {
		t.Fatal("expected duplicate-key tampering to fail verification")
	}
}

func TestTrustStorePinningAndTOFU(t *testing.T) {
	pubA, _ := testKey(t)
	pubB, _ := testKey(t)

	strict := NewTrustStore()
	if err := strict.Check("alice", pubA); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("strict store must reject unknown creator, got %v", err)
	}
	if err := strict.Pin("alice", pubA); err != nil {
		t.Fatal(err)
	}
	if err := strict.Check("alice", pubA); err != nil {
		t.Fatalf("pinned key rejected: %v", err)
	}
	if err := strict.Check("alice", pubB); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("unpinned key accepted: %v", err)
	}
	strict.Revoke("alice", pubA)
	if err := strict.Check("alice", pubA); err == nil {
		t.Fatal("revoked key accepted")
	}

	tofu := NewTOFUTrustStore()
	if err := tofu.Check("bob", pubA); err != nil {
		t.Fatalf("tofu first use: %v", err)
	}
	if err := tofu.Check("bob", pubB); !errors.Is(err, ErrUntrustedKey) {
		t.Fatalf("tofu must reject a second key, got %v", err)
	}
	if err := tofu.Check("bob", []byte("short")); err == nil {
		t.Fatal("malformed key accepted")
	}
	var nilStore *TrustStore
	if err := nilStore.Check("x", pubA); err == nil {
		t.Fatal("nil store must fail closed")
	}
}

func TestParseTrustedKeys(t *testing.T) {
	pub, _ := testKey(t)
	spec := "alice:" + base64.StdEncoding.EncodeToString(pub) + ", bob:" + base64.StdEncoding.EncodeToString(pub)
	ts, err := ParseTrustedKeys(spec)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if ts.Check("alice", pub) != nil || ts.Check("bob", pub) != nil {
		t.Fatal("expected both creators trusted")
	}
	for _, bad := range []string{"alice", "alice:notbase64!", "alice:" + base64.StdEncoding.EncodeToString([]byte("short")), "../x:" + base64.StdEncoding.EncodeToString(pub)} {
		if _, err := ParseTrustedKeys(bad); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
	if ts, err := ParseTrustedKeys(""); err != nil || ts == nil {
		t.Fatalf("empty spec should give empty store: %v", err)
	}
}

func TestCompareVersions(t *testing.T) {
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.0.1", "1.0.0", 1},
		{"1.10.0", "1.9.0", 1},
		{"2.0.0", "10.0.0", -1},
		{"1.0.0-alpha", "1.0.0", -1},
		{"1.0.0-alpha", "1.0.0-alpha.1", -1},
		{"1.0.0-alpha.1", "1.0.0-alpha.beta", -1},
		{"1.0.0-beta.2", "1.0.0-beta.11", -1},
		{"1.0.0-rc.1", "1.0.0-beta.11", 1},
		{"v1.2.3", "1.2.3", 0},
		{"1.0.0+build1", "1.0.0+build2", 0},
		{"99999999999999999999.0.0", "1.0.0", 1},
	}
	for _, c := range cases {
		got, ok := CompareVersions(c.a, c.b)
		if !ok || got != c.want {
			t.Errorf("CompareVersions(%q,%q)=%d,%v want %d", c.a, c.b, got, ok, c.want)
		}
	}
	if _, ok := CompareVersions("latest", "1.0.0"); ok {
		t.Error("expected invalid version to be reported")
	}
}

func TestVerifyWASM(t *testing.T) {
	_, priv := testKey(t)
	wasm := []byte("\x00asm\x01\x00\x00\x00")
	req, err := SignManifest(PublishRequest{ID: "p", Name: "P", Version: "1.0.0", CreatorID: "c", WASMHash: HashWASM(wasm)}, priv)
	if err != nil {
		t.Fatal(err)
	}
	m, err := VerifyPublishRequest(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.VerifyWASM(wasm); err != nil {
		t.Fatalf("expected hash match: %v", err)
	}
	if err := m.VerifyWASM(append(wasm, 0)); err == nil {
		t.Fatal("expected hash mismatch")
	}
}

func TestSignManifestRejectsBadInput(t *testing.T) {
	_, priv := testKey(t)
	if _, err := SignManifest(PublishRequest{ID: "p"}, priv); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := SignManifest(PublishRequest{ID: "p", Name: "P", Version: "1.0.0", CreatorID: "c", WASMHash: HashWASM(nil)}, ed25519.PrivateKey("short")); err == nil {
		t.Fatal("expected bad key error")
	}
}
