package providers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"time"
)

// DefaultProbeTimeout bounds one active health probe when the caller's
// context has no earlier deadline.
const DefaultProbeTimeout = 10 * time.Second

// ErrProbeNotSupported is returned by Probe implementations that forward to
// a provider without an active health probe.
var ErrProbeNotSupported = errors.New("health probe not supported by provider")

// ProbeEndpoint performs an active health probe: an authenticated GET of
// endpoint (typically a model listing), bounded by DefaultProbeTimeout. It
// returns nil when the upstream proves it can serve traffic:
//
//   - 2xx;
//   - 404/405: the server answered and accepted the request, it just has no
//     listing endpoint (some OpenAI-compatible servers);
//   - 429: rate limited, but up and authenticated.
//
// 401/403 (bad credentials), 408, 5xx and transport failures are returned as
// *UpstreamError / *TransportError. The response body is discarded.
func ProbeEndpoint(ctx context.Context, client *http.Client, provider, endpoint string, header http.Header) error {
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, DefaultProbeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return &UpstreamError{Provider: provider, StatusCode: http.StatusInternalServerError, Type: "configuration_error", Message: "invalid probe URL"}
	}
	for k, vs := range header {
		req.Header[k] = vs
	}
	if req.Header.Get("Accept") == "" {
		req.Header.Set("Accept", "application/json")
	}
	if client == nil {
		client = http.DefaultClient
	}
	resp, err := client.Do(req)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil && errors.Is(err, ctxErr) {
			if errors.Is(ctxErr, context.DeadlineExceeded) {
				return &TransportError{Provider: provider, Err: context.DeadlineExceeded}
			}
			return ctxErr
		}
		return &TransportError{Provider: provider, Err: err}
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300,
		resp.StatusCode == http.StatusNotFound,
		resp.StatusCode == http.StatusMethodNotAllowed,
		resp.StatusCode == http.StatusTooManyRequests:
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBodyBytes))
		return nil
	}
	return redactSecrets(NewUpstreamError(provider, resp), req.Header)
}

// RunProbe runs fn as an active health probe and records its outcome on h
// (see HealthTracker.ObserveProbe).
func RunProbe(h *HealthTracker, fn func() error) error {
	start := time.Now()
	err := fn()
	h.ObserveProbe(time.Since(start), err)
	return err
}
