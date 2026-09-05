package admission

import (
	"encoding/json"
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

// AdmissionRequest describes an incoming admission review.
type AdmissionRequest struct {
	Kind     RequestKind
	Resource string
	Path     string
	Method   string
	Headers  map[string]string
	Body     string
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
	r.w.Header().Set("Content-Type", "application/json")
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

// WebhookHandler returns an HTTP handler for admission validation.
func WebhookHandler(v Validator) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r == nil || r.Body == nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"missing body"}`))
			return
		}
		defer r.Body.Close()
		var req AdmissionRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"bad request"}`))
			return
		}
		req.Method = strings.ToUpper(req.Method)
		if req.Method == "" {
			req.Method = http.MethodPost
		}
		resp := v.Validate(req)
		ResponseWriter{w: w}.Write(resp)
	}
}

// KindFromHTTPMethod maps HTTP methods to admission kinds.
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
