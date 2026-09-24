package webhooks

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Header names used on every outbound webhook delivery.
const (
	// SignatureHeader carries "sha256=<hex>" where hex is
	// HMAC-SHA256(secret, "<timestamp>.<raw body>"). Multiple comma-separated
	// signatures are accepted by Verify to support secret rotation.
	SignatureHeader = "X-AeroLLM-Signature"
	// TimestampHeader carries the Unix timestamp (seconds) that was signed.
	TimestampHeader = "X-AeroLLM-Timestamp"
	// EventHeader carries the event type.
	EventHeader = "X-AeroLLM-Event"
	// DeliveryHeader carries the event ID so receivers can de-duplicate retries.
	DeliveryHeader = "X-AeroLLM-Delivery"

	// DefaultSignatureTolerance is the maximum accepted clock skew / replay
	// window used by VerifyRequest when a non-positive tolerance is given.
	DefaultSignatureTolerance = 5 * time.Minute

	signaturePrefix = "sha256="
)

var (
	// ErrMissingSignature is returned when the signature or timestamp header is absent.
	ErrMissingSignature = errors.New("webhooks: missing signature or timestamp")
	// ErrInvalidSignature is returned when no provided signature matches.
	ErrInvalidSignature = errors.New("webhooks: invalid signature")
	// ErrSignatureExpired is returned when the signed timestamp is outside the tolerance window.
	ErrSignatureExpired = errors.New("webhooks: signature timestamp outside tolerance")
)

// Sign returns the signature header value ("sha256=<hex>") for body signed at
// the given Unix timestamp. The signed message is "<timestamp>.<body>", which
// binds the timestamp to the payload and prevents replaying an old body with a
// fresh timestamp.
func Sign(secret string, timestamp int64, body []byte) string {
	return signaturePrefix + hex.EncodeToString(computeMAC(secret, timestamp, body))
}

func computeMAC(secret string, timestamp int64, body []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(strconv.FormatInt(timestamp, 10)))
	mac.Write([]byte{'.'})
	mac.Write(body)
	return mac.Sum(nil)
}

// SetSignatureHeaders signs body with secret at time now and sets the
// signature and timestamp headers on h. It is a no-op when secret is empty.
func SetSignatureHeaders(h http.Header, secret string, body []byte, now time.Time) {
	if secret == "" {
		return
	}
	ts := now.Unix()
	h.Set(TimestampHeader, strconv.FormatInt(ts, 10))
	h.Set(SignatureHeader, Sign(secret, ts, body))
}

// Verify checks a signature header value against body using constant-time
// comparison. timestampHeader is the raw TimestampHeader value. tolerance
// bounds |now - timestamp|; a non-positive tolerance disables the age check
// (not recommended).
func Verify(secret, signatureHeader, timestampHeader string, body []byte, tolerance time.Duration, now time.Time) error {
	if secret == "" {
		return errors.New("webhooks: empty verification secret")
	}
	signatureHeader = strings.TrimSpace(signatureHeader)
	timestampHeader = strings.TrimSpace(timestampHeader)
	if signatureHeader == "" || timestampHeader == "" {
		return ErrMissingSignature
	}
	ts, err := strconv.ParseInt(timestampHeader, 10, 64)
	if err != nil {
		return fmt.Errorf("%w: malformed timestamp", ErrInvalidSignature)
	}
	if tolerance > 0 {
		skew := now.Sub(time.Unix(ts, 0))
		if skew < 0 {
			skew = -skew
		}
		if skew > tolerance {
			return ErrSignatureExpired
		}
	}
	expected := computeMAC(secret, ts, body)
	for _, part := range strings.Split(signatureHeader, ",") {
		part = strings.TrimSpace(part)
		if !strings.HasPrefix(part, signaturePrefix) {
			continue
		}
		got, err := hex.DecodeString(strings.TrimPrefix(part, signaturePrefix))
		if err != nil {
			continue
		}
		if hmac.Equal(got, expected) {
			return nil
		}
	}
	return ErrInvalidSignature
}

// VerifyRequest reads (at most maxBody bytes of) the request body and verifies
// its signature headers. It returns the raw body on success so the caller can
// decode it. maxBody <= 0 defaults to 1 MiB; tolerance <= 0 defaults to
// DefaultSignatureTolerance.
func VerifyRequest(r *http.Request, secret string, tolerance time.Duration, maxBody int64) ([]byte, error) {
	if r == nil || r.Body == nil {
		return nil, ErrMissingSignature
	}
	if maxBody <= 0 {
		maxBody = 1 << 20
	}
	if tolerance <= 0 {
		tolerance = DefaultSignatureTolerance
	}
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		return nil, fmt.Errorf("webhooks: read body: %w", err)
	}
	if int64(len(body)) > maxBody {
		return nil, fmt.Errorf("webhooks: body exceeds %d bytes", maxBody)
	}
	if err := Verify(secret, r.Header.Get(SignatureHeader), r.Header.Get(TimestampHeader), body, tolerance, time.Now()); err != nil {
		return nil, err
	}
	return body, nil
}
