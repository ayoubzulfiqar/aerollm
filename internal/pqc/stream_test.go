package pqc

import (
	"bytes"
	"crypto/rand"
	"errors"
	"io"
	"testing"
)

func encryptAll(t *testing.T, key, pt []byte) []byte {
	t.Helper()
	enc := NewStreamEncrypter(key, io.NopCloser(bytes.NewReader(pt)))
	out, err := io.ReadAll(enc)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	return out
}

func TestStreamEncrypterWriteRead(t *testing.T) {
	key := []byte("pqc-secret-at-least-16")
	for _, size := range []int{0, 1, 100, StreamChunkSize, StreamChunkSize*3 + 17} {
		pt := make([]byte, size)
		_, _ = rand.Read(pt)
		ct := encryptAll(t, key, pt)
		if size >= 16 && bytes.Contains(ct, pt[:16]) {
			t.Fatalf("size %d: plaintext visible in ciphertext", size)
		}
		// Decrypt with awkward chunking.
		var out bytes.Buffer
		dec := NewStreamDecrypter(key, &out)
		for i := 0; i < len(ct); i += 7 {
			if _, err := dec.Write(ct[i:min(i+7, len(ct))]); err != nil {
				t.Fatalf("size %d: write: %v", size, err)
			}
		}
		if err := dec.Close(); err != nil {
			t.Fatalf("size %d: close: %v", size, err)
		}
		if !bytes.Equal(out.Bytes(), pt) {
			t.Fatalf("size %d: round trip mismatch", size)
		}
	}
}

func TestStreamNoncesAreUnique(t *testing.T) {
	key := []byte("pqc-secret-at-least-16")
	pt := []byte("same plaintext")
	if bytes.Equal(encryptAll(t, key, pt), encryptAll(t, key, pt)) {
		t.Fatal("two encryptions of the same plaintext must differ")
	}
}

func TestStreamTamperingDetected(t *testing.T) {
	key := []byte("pqc-secret-at-least-16")
	pt := bytes.Repeat([]byte("x"), StreamChunkSize+10)
	ct := encryptAll(t, key, pt)

	cases := map[string][]byte{}
	flipped := append([]byte(nil), ct...)
	flipped[len(flipped)/2] ^= 1
	cases["bitflip"] = flipped
	cases["truncated"] = ct[:len(ct)-20]
	cases["final-frame-dropped"] = ct[:streamHeaderLen+4+StreamChunkSize+16]
	cases["trailing-garbage"] = append(append([]byte(nil), ct...), 0, 0, 0, 0)
	for name, c := range cases {
		var out bytes.Buffer
		dec := NewStreamDecrypter(key, &out)
		_, werr := dec.Write(c)
		cerr := dec.Close()
		if werr == nil && cerr == nil {
			t.Errorf("%s: tampering not detected", name)
		}
	}
	var out bytes.Buffer
	dec := NewStreamDecrypter([]byte("wrong-key-at-least-16!"), &out)
	if _, err := dec.Write(ct); !errors.Is(err, ErrStreamAuth) {
		t.Fatalf("wrong key: expected auth error, got %v", err)
	}
	if out.Len() != 0 {
		t.Fatal("no unauthenticated plaintext may be released")
	}
}

func TestStreamShortKeyAndNilInner(t *testing.T) {
	enc := NewStreamEncrypter([]byte("short"), io.NopCloser(bytes.NewReader([]byte("x"))))
	if _, err := io.ReadAll(enc); !errors.Is(err, ErrStreamKeyTooShort) {
		t.Fatalf("expected short key error, got %v", err)
	}
	if err := NewStreamEncrypter([]byte("pqc-secret-at-least-16"), nil).Close(); err != nil {
		t.Fatalf("close with nil inner: %v", err)
	}
}
