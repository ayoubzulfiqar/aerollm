// Package guardrails provides request guardrails for the gateway: PII and
// secret redaction, a prompt-injection shield and per-API-key scoping
// (allowed models, IP allowlists, budgets).
//
// All middlewares that inspect the request body read at most MaxBodyBytes
// (413 beyond that), and always restore r.Body, r.ContentLength and
// r.GetBody so downstream handlers see a complete body.
package guardrails

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"
)

// MaxBodyBytes caps the request body buffered by the guardrail middlewares.
// Set it before the server starts handling requests.
var MaxBodyBytes int64 = 10 << 20

var errBodyTooLarge = errors.New("request body too large")

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func hasBody(r *http.Request) bool {
	return r.Body != nil && r.Body != http.NoBody && r.ContentLength != 0
}

// readRequestBody reads the whole body (bounded by MaxBodyBytes). The caller
// must restore it with setRequestBody.
func readRequestBody(r *http.Request) ([]byte, error) {
	if !hasBody(r) {
		return nil, nil
	}
	limit := MaxBodyBytes
	if limit <= 0 {
		limit = 10 << 20
	}
	if r.ContentLength > limit {
		return nil, errBodyTooLarge
	}
	b, err := io.ReadAll(io.LimitReader(r.Body, limit+1))
	_ = r.Body.Close()
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errBodyTooLarge
	}
	return b, nil
}

// setRequestBody installs b as the request body for the next handler.
func setRequestBody(r *http.Request, b []byte) {
	r.Body = io.NopCloser(bytes.NewReader(b))
	r.ContentLength = int64(len(b))
	r.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(b)), nil }
	r.TransferEncoding = nil
	if r.Header.Get("Content-Length") != "" {
		r.Header.Set("Content-Length", strconv.Itoa(len(b)))
	}
}

// bufferBody reads and immediately restores the body. It writes an error
// response and returns ok=false on failure.
func bufferBody(w http.ResponseWriter, r *http.Request) ([]byte, bool) {
	if !hasBody(r) {
		return nil, true
	}
	b, err := readRequestBody(r)
	if err != nil {
		if errors.Is(err, errBodyTooLarge) {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
		} else {
			writeJSONError(w, http.StatusBadRequest, "failed to read request body")
		}
		return nil, false
	}
	setRequestBody(r, b)
	return b, true
}

// decodeJSON decodes a single JSON value, preserving number precision.
func decodeJSON(b []byte) (interface{}, bool) {
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	var v interface{}
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); err != io.EOF {
		return nil, false
	}
	return v, true
}

func encodeJSON(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

func mediaType(r *http.Request) string {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil {
		return ""
	}
	return strings.ToLower(mt)
}

// Keys whose string values are identifiers or binary payloads rather than
// natural-language content.
var nonContentKeys = map[string]bool{
	"model": true, "role": true, "type": true, "id": true, "tool_call_id": true,
	"name": true, "url": true, "image_url": true, "b64_json": true, "data": true,
	"input_audio": true, "file_data": true, "detail": true, "format": true,
	"response_format": true, "encoding_format": true, "voice": true,
}

// ---------------------------------------------------------------------------
// PII middleware
// ---------------------------------------------------------------------------

type piiMappingKey struct{}

// PIIMappingFromContext returns the placeholder -> original mapping of PII
// redacted from the request by PIIMiddleware (nil if nothing was redacted).
// Use RestoreWithMapping to re-identify a response if required. The mapping
// contains sensitive data: never log it.
func PIIMappingFromContext(ctx context.Context) map[string]string {
	m, _ := ctx.Value(piiMappingKey{}).(map[string]string)
	return m
}

type redactState struct {
	red      *PIIRedactor
	counters map[PIIKind]int
	byValue  map[string]string
	mapping  map[string]string
}

func newRedactState(red *PIIRedactor) *redactState {
	return &redactState{red: red, counters: map[PIIKind]int{}, byValue: map[string]string{}, mapping: map[string]string{}}
}

func (st *redactState) redact(text string) (string, bool) {
	matches := st.red.Detect(text)
	if len(matches) == 0 {
		return text, false
	}
	var b strings.Builder
	b.Grow(len(text))
	last := 0
	for _, m := range matches {
		key := string(m.Kind) + "\x00" + m.Value
		ph, ok := st.byValue[key]
		if !ok {
			st.counters[m.Kind]++
			ph = "<PII_" + string(m.Kind) + "_" + strconv.Itoa(st.counters[m.Kind]) + ">"
			st.byValue[key] = ph
			st.mapping[ph] = m.Value
		}
		b.WriteString(text[last:m.Start])
		b.WriteString(ph)
		last = m.End
	}
	b.WriteString(text[last:])
	return b.String(), true
}

func (st *redactState) redactValue(v interface{}) (interface{}, bool) {
	switch t := v.(type) {
	case string:
		return st.redact(t)
	case map[string]interface{}:
		changed := false
		for k, val := range t {
			if nonContentKeys[k] {
				continue
			}
			if nv, ch := st.redactValue(val); ch {
				t[k] = nv
				changed = true
			}
		}
		return t, changed
	case []interface{}:
		changed := false
		for i, val := range t {
			if nv, ch := st.redactValue(val); ch {
				t[i] = nv
				changed = true
			}
		}
		return t, changed
	}
	return v, false
}

// RedactPayload redacts PII from a request payload. JSON payloads are
// redacted structurally (only string values; identifiers such as "model"
// are left untouched) so the result is always valid JSON. Plain-text
// payloads are redacted directly; other content types are returned
// unchanged. The returned mapping is nil if nothing was redacted.
func (p *PIIRedactor) RedactPayload(body []byte, contentType string) ([]byte, map[string]string) {
	if len(body) == 0 {
		return body, nil
	}
	st := newRedactState(p)
	if v, ok := decodeJSON(body); ok {
		nv, changed := st.redactValue(v)
		if !changed {
			return body, nil
		}
		out, err := encodeJSON(nv)
		if err != nil {
			return body, nil
		}
		return out, st.mapping
	}
	ct := strings.ToLower(contentType)
	if (ct == "" || strings.HasPrefix(ct, "text/")) && utf8.Valid(body) {
		out, changed := st.redact(string(body))
		if changed {
			return []byte(out), st.mapping
		}
	}
	return body, nil
}

// PIIMiddleware returns a handler that redacts PII and secrets from the
// request body before it reaches next. The placeholder mapping is available
// to downstream handlers via PIIMappingFromContext.
func PIIMiddleware(next http.HandlerFunc) http.HandlerFunc {
	redactor := NewPIIRedactor()
	return func(w http.ResponseWriter, r *http.Request) {
		if !hasBody(r) {
			next(w, r)
			return
		}
		body, err := readRequestBody(r)
		if err != nil {
			if errors.Is(err, errBodyTooLarge) {
				writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			} else {
				writeJSONError(w, http.StatusBadRequest, "failed to read request body")
			}
			return
		}
		redacted, mapping := redactor.RedactPayload(body, mediaType(r))
		setRequestBody(r, redacted)
		if len(mapping) > 0 {
			r = r.WithContext(context.WithValue(r.Context(), piiMappingKey{}, mapping))
		}
		next(w, r)
	}
}

// ---------------------------------------------------------------------------
// Injection shield middleware
// ---------------------------------------------------------------------------

// Top-level request fields that carry developer-controlled configuration
// rather than end-user content; they are not scanned for injections.
var developerKeys = map[string]bool{
	"system": true, "instructions": true, "tools": true, "functions": true,
	"tool_choice": true, "function_call": true, "response_format": true,
	"metadata": true, "logit_bias": true, "stop": true, "user": true,
}

func isDeveloperRole(v interface{}) bool {
	m, ok := v.(map[string]interface{})
	if !ok {
		return false
	}
	role, _ := m["role"].(string)
	role = strings.ToLower(role)
	return role == "system" || role == "developer"
}

func collectStrings(v interface{}, out *[]string) {
	switch t := v.(type) {
	case string:
		*out = append(*out, t)
	case map[string]interface{}:
		for k, val := range t {
			if !nonContentKeys[k] {
				collectStrings(val, out)
			}
		}
	case []interface{}:
		for _, val := range t {
			collectStrings(val, out)
		}
	}
}

// ScannableText extracts the end-user controlled text of a request body:
// for JSON chat/completions/responses payloads, all string content except
// system/developer messages and developer configuration (tools, system,
// instructions); for non-JSON bodies, the raw body.
func ScannableText(body []byte) string {
	if len(body) == 0 {
		return ""
	}
	v, ok := decodeJSON(body)
	if !ok {
		return string(body)
	}
	var texts []string
	obj, isObj := v.(map[string]interface{})
	if !isObj {
		collectStrings(v, &texts)
		return strings.Join(texts, "\n")
	}
	for k, val := range obj {
		if developerKeys[k] || nonContentKeys[k] {
			continue
		}
		if k == "messages" || k == "input" {
			if arr, ok := val.([]interface{}); ok {
				for _, item := range arr {
					if !isDeveloperRole(item) {
						collectStrings(item, &texts)
					}
				}
				continue
			}
		}
		collectStrings(val, &texts)
	}
	return strings.Join(texts, "\n")
}

// InjectionShieldMiddleware returns a handler that blocks prompt injection
// with 403. Every request body is inspected regardless of Content-Type or
// Content-Length (chunked bodies included).
func InjectionShieldMiddleware(next http.HandlerFunc) http.HandlerFunc {
	shield := NewPromptInjectionShield()
	return func(w http.ResponseWriter, r *http.Request) {
		body, ok := bufferBody(w, r)
		if !ok {
			return
		}
		if len(body) > 0 && shield.Scan(ScannableText(body)) {
			writeJSONError(w, http.StatusForbidden, "prompt injection detected")
			return
		}
		next(w, r)
	}
}

// ---------------------------------------------------------------------------
// API key scoping middleware
// ---------------------------------------------------------------------------

func requestAPIKey(r *http.Request) string {
	if k := bearerToken(r.Header.Get("Authorization")); k != "" {
		return k
	}
	return strings.TrimSpace(r.Header.Get("X-Api-Key"))
}

// modelsFromBody extracts the "model" field(s) from JSON, urlencoded or
// multipart bodies.
func modelsFromBody(body []byte, r *http.Request) []string {
	if len(body) == 0 {
		return nil
	}
	mt := mediaType(r)
	switch {
	case mt == "multipart/form-data":
		_, params, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || params["boundary"] == "" {
			return nil
		}
		mr := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		var out []string
		for {
			part, err := mr.NextPart()
			if err != nil {
				break
			}
			if part.FormName() == "model" {
				v, _ := io.ReadAll(io.LimitReader(part, 1024))
				out = append(out, string(v))
			}
			_ = part.Close()
		}
		return out
	case mt == "application/x-www-form-urlencoded":
		q, err := url.ParseQuery(string(body))
		if err != nil {
			return nil
		}
		return q["model"]
	}
	var payload struct {
		Model *string `json:"model"`
	}
	if err := json.Unmarshal(body, &payload); err != nil || payload.Model == nil {
		return nil
	}
	return []string{*payload.Model}
}

// APIKeyScopingMiddleware enforces APIKeyScope constraints.
//
// Behaviour:
//   - The key is taken from Authorization ("Bearer <key>" or bare) or
//     x-api-key; a request without any key gets 401.
//   - Keys WITHOUT a registered scope pass through untouched.
//   - For scoped keys with AllowedModels, every model named in the request
//     (JSON/form/multipart body field "model" and the "model" query
//     parameter) must be allowed; a request with a body but no model is
//     rejected. The body is restored afterwards.
//   - The IP allowlist is checked against r.RemoteAddr (port stripped);
//     X-Forwarded-For is only honoured when RemoteAddr is a proxy configured
//     with SetTrustedProxies.
//   - Violations return 403 with a JSON error.
func APIKeyScopingMiddleware(scoper *APIKeyScoper) func(http.HandlerFunc) http.HandlerFunc {
	return func(next http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			apiKey := requestAPIKey(r)
			if apiKey == "" {
				writeJSONError(w, http.StatusUnauthorized, "missing api key")
				return
			}
			if scoper == nil {
				next(w, r)
				return
			}
			scope, ok := scoper.scopeFor(apiKey)
			if !ok {
				next(w, r)
				return
			}
			var models []string
			requireModel := false
			if len(scope.AllowedModels) > 0 {
				if hasBody(r) {
					body, ok := bufferBody(w, r)
					if !ok {
						return
					}
					models = append(models, modelsFromBody(body, r)...)
					requireModel = true
				}
				if q := r.URL.Query()["model"]; len(q) > 0 {
					models = append(models, q...)
				}
			}
			clientIP := ""
			if addr, ok := scoper.ClientIP(r); ok {
				clientIP = addr.String()
			}
			if len(models) == 0 {
				// A request without a body (e.g. GET) cannot select a model,
				// so only the IP and budget constraints apply.
				if err := scoper.validate(apiKey, "", clientIP, !requireModel); err != nil {
					writeJSONError(w, http.StatusForbidden, err.Error())
					return
				}
			}
			for _, m := range models {
				if err := scoper.validate(apiKey, m, clientIP, false); err != nil {
					writeJSONError(w, http.StatusForbidden, err.Error())
					return
				}
			}
			next(w, r)
		}
	}
}
