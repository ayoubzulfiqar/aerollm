package callbacks

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// maxErrorBody bounds how much of an error response body is read.
const maxErrorBody = 4 << 10

// newHTTPClient returns a client that never follows redirects (so a
// compromised endpoint cannot bounce credentials or payloads elsewhere).
func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout:       timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// validateEndpoint requires an absolute http(s) URL.
func validateEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("callbacks: invalid url: %w", err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("callbacks: url must be absolute http(s), got %q", u.Redacted())
	}
	return nil
}

// httpStatusError is returned for non-2xx responses.
type httpStatusError struct {
	Status     int
	Body       string
	RetryAfter time.Duration
}

func (e *httpStatusError) Error() string {
	if e.Body == "" {
		return fmt.Sprintf("http status %d", e.Status)
	}
	return fmt.Sprintf("http status %d: %s", e.Status, e.Body)
}

// retryable reports whether a delivery error is worth retrying.
func retryable(err error) bool {
	var se *httpStatusError
	if errors.As(err, &se) {
		return se.Status == http.StatusTooManyRequests || se.Status == http.StatusRequestTimeout || se.Status >= 500
	}
	return !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded)
}

// doRequest sends one request and returns the (bounded) response body for
// 2xx responses or an *httpStatusError otherwise.
func doRequest(ctx context.Context, client *http.Client, method, endpoint string, body []byte, header http.Header) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, 0, err
	}
	for k, vs := range header {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		se := &httpStatusError{Status: resp.StatusCode}
		if len(b) > maxErrorBody {
			b = b[:maxErrorBody]
		}
		se.Body = string(b)
		if s := resp.Header.Get("Retry-After"); s != "" {
			if secs, err := strconv.Atoi(s); err == nil && secs > 0 {
				se.RetryAfter = time.Duration(secs) * time.Second
			}
		}
		return nil, resp.StatusCode, se
	}
	return b, resp.StatusCode, nil
}

// backoff returns the jittered delay before retry attempt n (n >= 1).
func backoff(base time.Duration, attempt int, maxDelay time.Duration) time.Duration {
	d := base
	for i := 1; i < attempt && d < maxDelay; i++ {
		d *= 2
	}
	if d > maxDelay || d <= 0 {
		d = maxDelay
	}
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(mrand.Int64N(int64(half)+1))
}

// sleepCtx waits for d or until ctx is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func randomID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	// RFC 4122 version 4 layout.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:32]
}
