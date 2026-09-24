// Package admission implements a validating admission webhook: callers POST
// an AdmissionRequest and receive an allow/deny AdmissionResponse from a
// pluggable Validator.
package admission

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
)

// RequestKind categorizes admission requests.
type RequestKind string

const (
	RequestCreate RequestKind = "create"
	RequestUpdate RequestKind = "update"
	RequestDelete RequestKind = "delete"
)

// MaxBodyBytes caps admission review bodies.
const MaxBodyBytes = 1 << 20

// AdmissionRequest describes an incoming admission review.
type AdmissionRequest struct {
	Kind     RequestKind       `json:"kind"`
	Resource string            `json:"resource"`
	Path     string            `json:"path"`
	Method   string            `json:"method"`
	Headers  map[string]string `json:"headers"`
	Body     string            `json:"body"`
}

// AdmissionResponse captures the admission decision.
type AdmissionResponse struct {
	Allowed bool   `json:"allowed"`
	Reason  string `json:"reason"`
}

// ResponseWriter serializes admission responses.
type ResponseWriter struct {
	w http.ResponseWriter
}

// Write sends the admission response as JSON.
func (r ResponseWriter) Write(resp AdmissionResponse) {
	r.WriteStatus(http.StatusOK, resp)
}

// WriteStatus sends the admission response as JSON with a status code.
func (r ResponseWriter) WriteStatus(status int, resp AdmissionResponse) {
	r.w.Header().Set("Content-Type", "application/json")
	r.w.WriteHeader(status)
	_ = json.NewEncoder(r.w).Encode(resp)
}

// Deny writes a denied admission response.
func (r *ResponseWriter) Deny(reason string) {
	r.Write(AdmissionResponse{Allowed: false, Reason: reason})
}

// Allow writes an allowed admission response.
func (r *ResponseWriter) Allow() {
	r.Write(AdmissionResponse{Allowed: true, Reason: "allowed"})
}

// Validator validates admission requests.
type Validator interface {
	Validate(req AdmissionRequest) AdmissionResponse
}

// ValidatorFunc adapts a function to Validator.
type ValidatorFunc func(req AdmissionRequest) AdmissionResponse

// Validate calls the underlying function.
func (f ValidatorFunc) Validate(req AdmissionRequest) AdmissionResponse {
	return f(req)
}

// Chain returns a Validator that allows a request only if every validator
// allows it; the first denial is returned. An empty chain denies.
func Chain(validators ...Validator) Validator {
	return ValidatorFunc(func(req AdmissionRequest) AdmissionResponse {
		if len(validators) == 0 {
			return AdmissionResponse{Allowed: false, Reason: "no validators configured"}
		}
		last := AdmissionResponse{Allowed: true, Reason: "allowed"}
		for _, v := range validators {
			if v == nil {
				return AdmissionResponse{Allowed: false, Reason: "nil validator"}
			}
			resp := v.Validate(req)
			if !resp.Allowed {
				return resp
			}
			last = resp
		}
		return last
	})
}

// DenyResources returns a Validator denying requests for the named
// resources (case-insensitive) and allowing everything else.
func DenyResources(resources ...string) Validator {
	return ValidatorFunc(func(req AdmissionRequest) AdmissionResponse {
		for _, r := range resources {
			if strings.EqualFold(strings.TrimSpace(r), strings.TrimSpace(req.Resource)) {
				return AdmissionResponse{Allowed: false, Reason: "resource " + r + " is not admitted"}
			}
		}
		return AdmissionResponse{Allowed: true, Reason: "allowed"}
	})
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// validate runs v, converting panics into a (fail-closed) denial.
func validate(v Validator, req AdmissionRequest) (resp AdmissionResponse, ok bool) {
	defer func() {
		if recover() != nil {
			resp, ok = AdmissionResponse{Allowed: false, Reason: "validator error"}, false
		}
	}()
	return v.Validate(req), true
}

// WebhookHandler returns an HTTP handler for admission validation. Only POST
// is accepted (405 otherwise); bodies are capped at MaxBodyBytes. A missing
// kind is derived from the method. A nil or panicking validator fails closed
// (allowed=false, HTTP 500).
func WebhookHandler(v Validator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			return
		}
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if r.Body == nil || r.Body == http.NoBody {
			writeError(w, http.StatusBadRequest, "missing body")
			return
		}
		defer r.Body.Close()
		var req AdmissionRequest
		dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, MaxBodyBytes))
		if err := dec.Decode(&req); err != nil {
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			writeError(w, http.StatusBadRequest, "bad request")
			return
		}
		if err := dec.Decode(&struct{}{}); err != io.EOF {
			writeError(w, http.StatusBadRequest, "bad request")
			return
		}
		req.Method = strings.ToUpper(strings.TrimSpace(req.Method))
		if req.Method == "" {
			req.Method = http.MethodPost
		}
		req.Kind = RequestKind(strings.ToLower(strings.TrimSpace(string(req.Kind))))
		switch req.Kind {
		case "":
			req.Kind = KindFromHTTPMethod(req.Method)
		case RequestCreate, RequestUpdate, RequestDelete:
		default:
			writeError(w, http.StatusBadRequest, "invalid kind")
			return
		}
		rw := ResponseWriter{w: w}
		if v == nil {
			rw.WriteStatus(http.StatusInternalServerError, AdmissionResponse{Allowed: false, Reason: "no validator configured"})
			return
		}
		resp, ok := validate(v, req)
		if !ok {
			rw.WriteStatus(http.StatusInternalServerError, resp)
			return
		}
		rw.Write(resp)
	}
}

// KindFromHTTPMethod maps HTTP methods to admission kinds. Methods other
// than PUT/PATCH/DELETE map to RequestCreate.
func KindFromHTTPMethod(method string) RequestKind {
	switch strings.ToUpper(method) {
	case http.MethodPost:
		return RequestCreate
	case http.MethodPut, http.MethodPatch:
		return RequestUpdate
	case http.MethodDelete:
		return RequestDelete
	default:
		return RequestCreate
	}
}
