package spatial

import (
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

const (
	// DefaultMaxParseBytes caps how much text is inspected for spatial anchors.
	// ParseSpatialAnchors silently ignores anything beyond this many bytes;
	// ParseSpatialAnchorsReader and ParseHandler reject larger inputs.
	DefaultMaxParseBytes = 1 << 20
	// DefaultMaxAnchors caps how many anchors are returned for a single input.
	DefaultMaxAnchors = 1024

	// anchorType is the discriminator value identifying a spatial anchor object.
	anchorType = "spatial_anchor"
	// maxCandidateBytes caps the size of a nested JSON object that is decoded
	// as a potential anchor. Top-level objects may be as large as the input.
	maxCandidateBytes = 64 << 10
	// maxScanDepth caps brace nesting tracked by the scanner; deeper objects are
	// still balanced but are not evaluated individually.
	maxScanDepth = 16
	// maxEmbeddedDepth caps recursion into JSON string values that themselves
	// contain anchor JSON (e.g. OpenAI-style "content" fields).
	maxEmbeddedDepth = 2
	// maxWalkDepth caps recursion when walking decoded JSON values.
	maxWalkDepth = 64
)

// ErrInputTooLarge is returned when an input exceeds the configured size cap.
var ErrInputTooLarge = errors.New("spatial: input exceeds size limit")

// SpatialAnchor represents a parsed spatial object from LLM output.
type SpatialAnchor struct {
	Type string      `json:"type"`
	X    float64     `json:"x"`
	Y    float64     `json:"y"`
	Z    float64     `json:"z"`
	Raw  interface{} `json:"raw,omitempty"`
}

// WebXRPayload is the standardized AR/VR payload.
type WebXRPayload struct {
	Version   string          `json:"version"`
	Anchors   []SpatialAnchor `json:"anchors"`
	SessionID string          `json:"session_id,omitempty"`
}

// ParseSpatialAnchors scans text for JSON objects whose "type" is
// "spatial_anchor". Anchors may appear one per line, embedded in prose, nested
// inside other JSON objects (up to 16 levels deep), or JSON-encoded inside
// string values of a JSON document (such as a chat completion "content"
// field). Coordinates that are present must be finite numbers; missing or null
// coordinates default to 0. When an anchor contains another anchor, only the
// outermost one is reported.
//
// At most DefaultMaxParseBytes of text are inspected and at most
// DefaultMaxAnchors anchors are returned. The result is never nil.
func ParseSpatialAnchors(text string) []SpatialAnchor {
	if len(text) > DefaultMaxParseBytes {
		text = text[:DefaultMaxParseBytes]
	}
	p := newAnchorParser(len(text))
	p.scan(text, 0)
	return p.anchors()
}

// ParseSpatialAnchorsReader reads at most maxBytes from r and parses spatial
// anchors like ParseSpatialAnchors. It returns ErrInputTooLarge when r holds
// more than maxBytes, and propagates read errors (including
// *http.MaxBytesError). A maxBytes <= 0 selects DefaultMaxParseBytes.
func ParseSpatialAnchorsReader(r io.Reader, maxBytes int64) ([]SpatialAnchor, error) {
	if r == nil {
		return []SpatialAnchor{}, nil
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxParseBytes
	}
	limit := maxBytes
	if limit < math.MaxInt64 {
		limit++
	}
	b, err := io.ReadAll(io.LimitReader(r, limit))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > maxBytes {
		return nil, ErrInputTooLarge
	}
	p := newAnchorParser(len(b))
	p.scan(string(b), 0)
	return p.anchors(), nil
}

// ToWebXR converts anchors to WebXR payload.
func ToWebXR(anchors []SpatialAnchor, sessionID string) WebXRPayload {
	if anchors == nil {
		anchors = []SpatialAnchor{}
	}
	return WebXRPayload{Version: "1.0", Anchors: anchors, SessionID: sessionID}
}

// ParseHandler returns a ready-to-mount handler for anchor parsing.
//
// It accepts POST only (405 with an Allow header otherwise), caps the request
// body at DefaultMaxParseBytes (413 when exceeded) and replies with a JSON
// array of anchors. With ?format=webxr it replies with a WebXRPayload instead,
// using the optional session_id query parameter.
func ParseHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
			return
		}
		if r.Body == nil {
			writeJSONError(w, http.StatusBadRequest, "missing body")
			return
		}
		if r.ContentLength > DefaultMaxParseBytes {
			writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		body := http.MaxBytesReader(w, r.Body, DefaultMaxParseBytes)
		defer body.Close()
		anchors, err := ParseSpatialAnchorsReader(body, DefaultMaxParseBytes)
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) || errors.Is(err, ErrInputTooLarge) {
				writeJSONError(w, http.StatusRequestEntityTooLarge, "request body too large")
				return
			}
			writeJSONError(w, http.StatusBadRequest, "failed to read request body")
			return
		}
		var payload interface{} = anchors
		if strings.EqualFold(r.URL.Query().Get("format"), "webxr") {
			payload = ToWebXR(anchors, sanitizeSessionID(r.URL.Query().Get("session_id")))
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(payload)
	}
}

// maxSessionIDLen bounds the session identifier reflected into responses.
const maxSessionIDLen = 256

// sanitizeSessionID drops session identifiers that are too long or contain
// control characters. The value is only ever emitted JSON-encoded.
func sanitizeSessionID(id string) string {
	if len(id) > maxSessionIDLen {
		return ""
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return id
}

func writeJSONError(w http.ResponseWriter, code int, msg string) {
	h := w.Header()
	h.Del("Content-Length")
	h.Set("Content-Type", "application/json")
	h.Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{Error: msg})
}

// anchorParser extracts anchors from free-form text. Candidate objects are
// found in one linear pass; each top-level object is decoded once, and objects
// nested inside it are only decoded when the top-level object is not itself an
// anchor, outermost first, skipping anything inside an accepted anchor. With
// nesting tracked to maxScanDepth this bounds decoding work to a small
// multiple of the input size.
type anchorParser struct {
	max int
	out []SpatialAnchor
	// nestedBudget bounds the bytes decoded for nested candidates so that
	// adversarial nesting degrades gracefully instead of costing
	// maxScanDepth/2 times the input size.
	nestedBudget int
}

// nestedBudgetFactor multiplies the input size to obtain nestedBudget.
const nestedBudgetFactor = 4

func newAnchorParser(inputLen int) *anchorParser {
	return &anchorParser{max: DefaultMaxAnchors, nestedBudget: nestedBudgetFactor*inputLen + maxCandidateBytes}
}

// span is a balanced {...} region of the scanned text.
type span struct{ start, end int }

// maxNestedCandidates caps nested candidates remembered per top-level object.
const maxNestedCandidates = 8 * DefaultMaxAnchors

func (p *anchorParser) full() bool { return len(p.out) >= p.max }

func (p *anchorParser) anchors() []SpatialAnchor {
	if p.out == nil {
		return []SpatialAnchor{}
	}
	return p.out
}

func (p *anchorParser) add(a SpatialAnchor) {
	if !p.full() {
		p.out = append(p.out, a)
	}
}

// scan walks text tracking balanced JSON objects. Quotes are only honored
// inside a candidate object, so stray quotes in surrounding prose are
// harmless; a raw newline inside a string (illegal in JSON) abandons the
// current candidate so a malformed fragment cannot swallow later anchors.
func (p *anchorParser) scan(text string, embedDepth int) {
	if p.full() || !strings.Contains(text, anchorType) {
		return
	}
	hits := tokenOffsets(text, anchorType)
	var (
		stack    []int
		nested   []span
		overflow int
		inString bool
		escaped  bool
	)
	abandon := func() {
		// The enclosing object can never be valid JSON, but objects that
		// already closed inside it may still be anchors.
		p.considerNested(text, nested)
		stack, nested = stack[:0], nested[:0]
		overflow, inString, escaped = 0, false, false
	}
	for i := 0; i < len(text) && !p.full(); i++ {
		c := text[i]
		if len(stack) == 0 {
			if c == '{' {
				stack = append(stack, i)
			}
			continue
		}
		if inString {
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			case c == '\n':
				abandon()
			}
			continue
		}
		switch c {
		case '"':
			inString = true
		case '{':
			if len(stack) >= maxScanDepth {
				overflow++
				continue
			}
			stack = append(stack, i)
		case '}':
			if overflow > 0 {
				overflow--
				continue
			}
			s := span{start: stack[len(stack)-1], end: i + 1}
			stack = stack[:len(stack)-1]
			if len(stack) > 0 {
				if s.end-s.start <= maxCandidateBytes && len(nested) < maxNestedCandidates && containsToken(hits, s, len(anchorType)) {
					nested = append(nested, s)
				}
				continue
			}
			p.considerTop(text, s, hits, nested, embedDepth)
			nested = nested[:0]
		}
	}
	if len(stack) > 0 && !p.full() {
		abandon()
	}
}

func (p *anchorParser) considerTop(text string, s span, hits []int, nested []span, embedDepth int) {
	if !containsToken(hits, s, len(anchorType)) {
		return // nothing inside can contain the token either
	}
	b := []byte(text[s.start:s.end])
	if a, ok := decodeAnchor(b); ok {
		p.add(a) // outermost anchor wins; nested objects are part of it
		return
	}
	p.considerNested(text, nested)
	if embedDepth >= maxEmbeddedDepth {
		return
	}
	var obj interface{}
	if err := json.Unmarshal(b, &obj); err == nil {
		p.walkStrings(obj, embedDepth+1, 0)
	}
}

// considerNested evaluates nested candidates outermost first. Candidates are
// recorded in closing order; since spans either nest or are disjoint, sorting
// by start puts every object before the objects it contains.
func (p *anchorParser) considerNested(text string, nested []span) {
	sort.Slice(nested, func(i, j int) bool { return nested[i].start < nested[j].start })
	acceptedEnd := -1
	for _, s := range nested {
		if p.full() {
			return
		}
		if s.start < acceptedEnd {
			continue // inside an accepted anchor
		}
		if p.nestedBudget < s.end-s.start {
			return
		}
		p.nestedBudget -= s.end - s.start
		if a, ok := decodeAnchor([]byte(text[s.start:s.end])); ok {
			p.add(a)
			acceptedEnd = s.end
		}
	}
}

// walkStrings looks for anchor JSON encoded inside string values.
func (p *anchorParser) walkStrings(v interface{}, embedDepth, depth int) {
	if p.full() || depth > maxWalkDepth {
		return
	}
	switch vv := v.(type) {
	case string:
		if strings.Contains(vv, anchorType) {
			p.scan(vv, embedDepth)
		}
	case map[string]interface{}:
		keys := make([]string, 0, len(vv))
		for k := range vv {
			keys = append(keys, k)
		}
		sort.Strings(keys) // deterministic output order
		for _, k := range keys {
			p.walkStrings(vv[k], embedDepth, depth+1)
		}
	case []interface{}:
		for _, e := range vv {
			p.walkStrings(e, embedDepth, depth+1)
		}
	}
}

// anchorProbe is decoded first so that rejecting a candidate never requires
// building a full map (keeps adversarial nesting cheap). Key matching follows
// encoding/json rules (case-insensitive).
type anchorProbe struct {
	Type string          `json:"type"`
	X    json.RawMessage `json:"x"`
	Y    json.RawMessage `json:"y"`
	Z    json.RawMessage `json:"z"`
}

// decodeAnchor returns the anchor encoded by the JSON object b when its type
// is "spatial_anchor" and every present, non-null coordinate is a finite
// number.
func decodeAnchor(b []byte) (SpatialAnchor, bool) {
	var probe anchorProbe
	if err := json.Unmarshal(b, &probe); err != nil || probe.Type != anchorType {
		return SpatialAnchor{}, false
	}
	a := SpatialAnchor{Type: anchorType}
	if !parseCoord(probe.X, &a.X) || !parseCoord(probe.Y, &a.Y) || !parseCoord(probe.Z, &a.Z) {
		return SpatialAnchor{}, false
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(b, &raw); err != nil {
		return SpatialAnchor{}, false
	}
	a.Raw = raw
	return a, true
}

func parseCoord(raw json.RawMessage, dst *float64) bool {
	if len(raw) == 0 || string(raw) == "null" {
		return true
	}
	if c := raw[0]; c != '-' && (c < '0' || c > '9') {
		return false
	}
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil || math.IsNaN(f) || math.IsInf(f, 0) {
		return false
	}
	*dst = f
	return true
}

func tokenOffsets(text, token string) []int {
	var out []int
	for off := 0; ; {
		i := strings.Index(text[off:], token)
		if i < 0 {
			return out
		}
		out = append(out, off+i)
		off += i + len(token)
	}
}

func containsToken(hits []int, s span, tokenLen int) bool {
	i := sort.SearchInts(hits, s.start)
	return i < len(hits) && hits[i]+tokenLen <= s.end
}
