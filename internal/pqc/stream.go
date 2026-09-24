package pqc

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
)

// Stream format (STREAM construction, AES-256-GCM):
//
//	header: "AQS1" | salt(16)
//	frame:  u32 BE (bit 31 = final flag, bits 0-30 = ciphertext length) | ciphertext
//
// The per-stream key is HKDF-SHA256(secret=key, salt, info). Frame i uses
// nonce = final(1) | 0(3) | u64 BE i, so frames cannot be reordered, dropped,
// duplicated or have their final flag flipped without detection, and the
// header is bound as additional data. Every stream ends with exactly one
// final frame, so truncation is detected by StreamDecrypter.Close.

const (
	streamMagic     = "AQS1"
	streamSaltSize  = 16
	streamHeaderLen = len(streamMagic) + streamSaltSize
	// StreamChunkSize is the maximum plaintext per frame.
	StreamChunkSize = 32 << 10
	maxFrameCT      = StreamChunkSize + 16
	finalFlag       = 1 << 31
	// MinStreamKeyLen is the minimum length of a stream key.
	MinStreamKeyLen = 16
)

// Stream errors.
var (
	ErrStreamKeyTooShort = errors.New("pqc: stream key must be at least 16 bytes")
	ErrStreamAuth        = errors.New("pqc: stream authentication failed")
	ErrStreamTruncated   = errors.New("pqc: stream truncated")
	ErrStreamFormat      = errors.New("pqc: malformed stream")
)

func streamAEAD(key, salt []byte) (cipher.AEAD, error) {
	if len(key) < MinStreamKeyLen {
		return nil, ErrStreamKeyTooShort
	}
	k, err := hkdf.Key(sha256.New, key, salt, "aerollm/pqc/stream/v1", 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(k)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func frameNonce(counter uint64, final bool) []byte {
	n := make([]byte, 12)
	if final {
		n[0] = 1
	}
	binary.BigEndian.PutUint64(n[4:], counter)
	return n
}

// StreamEncrypter wraps a ReadCloser; reading from it yields the
// authenticated-encrypted form of the inner stream.
type StreamEncrypter struct {
	key     []byte
	inner   io.ReadCloser
	aead    cipher.AEAD
	header  []byte
	pending []byte
	buf     []byte
	counter uint64
	started bool
	done    bool
	err     error
}

// NewStreamEncrypter creates an encrypter. key should be a shared secret of
// at least MinStreamKeyLen bytes (e.g. from Encapsulate); a short key makes
// Read fail with ErrStreamKeyTooShort.
func NewStreamEncrypter(key []byte, inner io.ReadCloser) *StreamEncrypter {
	return &StreamEncrypter{key: append([]byte(nil), key...), inner: inner}
}

func (s *StreamEncrypter) init() error {
	salt := make([]byte, streamSaltSize)
	if _, err := rand.Read(salt); err != nil {
		return err
	}
	aead, err := streamAEAD(s.key, salt)
	if err != nil {
		return err
	}
	s.aead = aead
	s.header = append([]byte(streamMagic), salt...)
	s.pending = append(s.pending, s.header...)
	s.buf = make([]byte, StreamChunkSize)
	return nil
}

func (s *StreamEncrypter) seal(pt []byte, final bool) {
	ct := s.aead.Seal(nil, frameNonce(s.counter, final), pt, s.header)
	s.counter++
	l := uint32(len(ct))
	if final {
		l |= finalFlag
	}
	s.pending = binary.BigEndian.AppendUint32(s.pending, l)
	s.pending = append(s.pending, ct...)
}

// Read returns encrypted stream bytes.
func (s *StreamEncrypter) Read(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	if !s.started {
		s.started = true
		if s.inner == nil {
			s.err = errors.New("pqc: nil inner reader")
			return 0, s.err
		}
		if err := s.init(); err != nil {
			s.err = err
			return 0, err
		}
	}
	for len(s.pending) == 0 {
		if s.done {
			return 0, io.EOF
		}
		n, err := s.inner.Read(s.buf)
		if n > 0 {
			s.seal(s.buf[:n], false)
		}
		switch {
		case err == io.EOF:
			s.seal(nil, true)
			s.done = true
		case err != nil:
			s.err = err
			return 0, err
		}
	}
	n := copy(p, s.pending)
	s.pending = s.pending[n:]
	return n, nil
}

// Close closes the inner reader.
func (s *StreamEncrypter) Close() error {
	if s.inner == nil {
		return nil
	}
	return s.inner.Close()
}

// StreamDecrypter is a Writer that authenticates and decrypts a stream
// produced by StreamEncrypter, writing plaintext to inner as soon as each
// frame is verified. Call Close to detect truncated streams.
type StreamDecrypter struct {
	key      []byte
	inner    io.Writer
	aead     cipher.AEAD
	header   []byte
	buf      bytes.Buffer
	counter  uint64
	finished bool
	err      error
}

// NewStreamDecrypter creates a decrypter.
func NewStreamDecrypter(key []byte, inner io.Writer) *StreamDecrypter {
	return &StreamDecrypter{key: append([]byte(nil), key...), inner: inner}
}

func (s *StreamDecrypter) fail(err error) (int, error) {
	s.err = err
	s.buf.Reset()
	return 0, err
}

// Write consumes encrypted bytes (any chunking) and writes verified
// plaintext to the inner writer.
func (s *StreamDecrypter) Write(p []byte) (int, error) {
	if s.err != nil {
		return 0, s.err
	}
	if s.inner == nil {
		return s.fail(errors.New("pqc: nil inner writer"))
	}
	if s.finished && len(p) > 0 {
		return s.fail(ErrStreamFormat) // data after the final frame
	}
	s.buf.Write(p)
	if s.aead == nil {
		if s.buf.Len() < streamHeaderLen {
			return len(p), nil
		}
		hdr := s.buf.Next(streamHeaderLen)
		if string(hdr[:len(streamMagic)]) != streamMagic {
			return s.fail(ErrStreamFormat)
		}
		s.header = append([]byte(nil), hdr...)
		aead, err := streamAEAD(s.key, s.header[len(streamMagic):])
		if err != nil {
			return s.fail(err)
		}
		s.aead = aead
	}
	for !s.finished && s.buf.Len() >= 4 {
		raw := binary.BigEndian.Uint32(s.buf.Bytes()[:4])
		final := raw&finalFlag != 0
		l := int(raw &^ finalFlag)
		if l < s.aead.Overhead() || l > maxFrameCT {
			return s.fail(ErrStreamFormat)
		}
		if s.buf.Len() < 4+l {
			break
		}
		s.buf.Next(4)
		ct := s.buf.Next(l)
		pt, err := s.aead.Open(nil, frameNonce(s.counter, final), ct, s.header)
		if err != nil {
			return s.fail(ErrStreamAuth)
		}
		s.counter++
		if len(pt) > 0 {
			if _, err := s.inner.Write(pt); err != nil {
				return s.fail(err)
			}
		}
		if final {
			s.finished = true
		}
	}
	if s.finished && s.buf.Len() > 0 {
		return s.fail(ErrStreamFormat)
	}
	return len(p), nil
}

// Close reports ErrStreamTruncated if the final frame was never received.
// It does not close the inner writer.
func (s *StreamDecrypter) Close() error {
	if s.err != nil {
		return s.err
	}
	if !s.finished {
		return ErrStreamTruncated
	}
	return nil
}
