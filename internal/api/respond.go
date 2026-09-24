package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/middleware"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
	"github.com/ayoubzulfiqar/aerollm/internal/router"
)

// defaultMaxBody caps JSON request bodies when no BodyLimit middleware is in
// front of the handler.
const defaultMaxBody = 10 << 20

// writeError writes an OpenAI-compatible error body.
func writeError(w http.ResponseWriter, status int, message string) {
	middleware.WriteJSONError(w, status, message, "")
}

// writeErrorType writes an OpenAI-compatible error body with an explicit type.
func writeErrorType(w http.ResponseWriter, status int, message, errType string) {
	middleware.WriteJSONError(w, status, message, errType)
}

// writeJSON encodes v with the given status.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// methodNotAllowed writes 405 with an Allow header.
func methodNotAllowed(w http.ResponseWriter, allowed ...string) {
	w.Header().Set("Allow", strings.Join(allowed, ", "))
	writeError(w, http.StatusMethodNotAllowed, "method not allowed")
}

// decodeJSON decodes a size-capped JSON body into v. It writes the error
// response itself and returns false on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v interface{}) bool {
	if r.Body == nil {
		writeError(w, http.StatusBadRequest, "missing request body")
		return false
	}
	body := http.MaxBytesReader(w, r.Body, defaultMaxBody)
	dec := json.NewDecoder(body)
	if err := dec.Decode(v); err != nil {
		var mbe *http.MaxBytesError
		switch {
		case errors.As(err, &mbe):
			writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		case errors.Is(err, io.EOF):
			writeError(w, http.StatusBadRequest, "missing request body")
		default:
			writeError(w, http.StatusBadRequest, "invalid JSON: "+sanitizeDecodeError(err))
		}
		return false
	}
	if dec.More() {
		writeError(w, http.StatusBadRequest, "invalid JSON: unexpected data after object")
		return false
	}
	return true
}

// sanitizeDecodeError keeps decode errors short and free of request echoes.
func sanitizeDecodeError(err error) string {
	var se *json.SyntaxError
	var te *json.UnmarshalTypeError
	switch {
	case errors.As(err, &se):
		return "syntax error"
	case errors.As(err, &te):
		if te.Field != "" {
			return "wrong type for field " + te.Field
		}
		return "wrong type for value"
	}
	msg := err.Error()
	if len(msg) > 120 {
		msg = msg[:120]
	}
	return msg
}

// upstreamStatus maps a provider/routing error to a client status code and
// message. Upstream auth failures become 502: the caller's credentials were
// fine, the gateway's provider credentials were not.
func upstreamStatus(err error) (int, string, string) {
	if err == nil {
		return http.StatusOK, "", ""
	}
	if errors.Is(err, context.Canceled) {
		return 499, "request cancelled", "api_error"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return http.StatusGatewayTimeout, "upstream timed out", "timeout"
	}
	var mi *agent.MaxIterationsError
	if errors.As(err, &mi) {
		return http.StatusBadGateway, "tool loop exceeded the maximum number of iterations", "agent_error"
	}
	if errors.Is(err, agent.ErrNoProvider) {
		return http.StatusServiceUnavailable, "agent has no provider configured", "service_unavailable"
	}
	var np *router.NoProviderError
	if errors.As(err, &np) {
		// The router cannot distinguish "no providers", "all circuits open"
		// and "model not served", so report the gateway as unavailable.
		return http.StatusServiceUnavailable, np.Error(), "service_unavailable"
	}
	var ue *providers.UpstreamError
	if errors.As(err, &ue) {
		msg := ue.Message
		if msg == "" {
			msg = http.StatusText(ue.StatusCode)
		}
		switch {
		case ue.StatusCode == http.StatusUnauthorized || ue.StatusCode == http.StatusForbidden:
			return http.StatusBadGateway, "upstream provider rejected the gateway's credentials", "upstream_auth_error"
		case ue.StatusCode == http.StatusTooManyRequests:
			return http.StatusTooManyRequests, msg, "rate_limit_error"
		case ue.StatusCode >= 400 && ue.StatusCode < 500:
			return ue.StatusCode, msg, "invalid_request_error"
		default:
			return http.StatusBadGateway, msg, "upstream_error"
		}
	}
	if errors.Is(err, providers.ErrCircuitOpen) {
		return http.StatusServiceUnavailable, "all providers are temporarily unavailable", "service_unavailable"
	}
	if providers.IsRetryable(err) {
		return http.StatusBadGateway, "upstream provider unavailable", "upstream_error"
	}
	return http.StatusInternalServerError, "internal error", "api_error"
}

// writeUpstreamError writes the client response for a failed completion,
// forwarding an upstream Retry-After when present.
func writeUpstreamError(w http.ResponseWriter, err error) {
	var ue *providers.UpstreamError
	if errors.As(err, &ue) && ue.RetryAfter > 0 {
		secs := int(ue.RetryAfter.Seconds())
		if secs < 1 {
			secs = 1
		}
		w.Header().Set("Retry-After", strconv.Itoa(secs))
	}
	status, msg, typ := upstreamStatus(err)
	if status == 499 {
		// Client is gone; nothing useful to write.
		return
	}
	writeErrorType(w, status, msg, typ)
}
