package spatial

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseSpatialAnchors(t *testing.T) {
	text := `{"type":"spatial_anchor","x":1.2,"y":0.5,"z":-0.3}
not spatial
{"type":"spatial_anchor","x":0,"y":1,"z":2}`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 2 {
		t.Fatalf("expected 2 anchors, got %d", len(anchors))
	}
	if anchors[0].X != 1.2 || anchors[0].Y != 0.5 || anchors[0].Z != -0.3 {
		t.Fatalf("unexpected first anchor: %v", anchors[0])
	}
	if anchors[1].X != 0 || anchors[1].Y != 1 || anchors[1].Z != 2 {
		t.Fatalf("unexpected second anchor: %v", anchors[1])
	}
}

func TestParseSpatialAnchorsLongLineDoesNotStopParsing(t *testing.T) {
	// bufio.Scanner used to give up on lines > 64KiB, dropping later anchors.
	long := strings.Repeat("a", 100<<10)
	text := long + "\n" + `{"type":"spatial_anchor","x":1,"y":2,"z":3}`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 1 || anchors[0].Z != 3 {
		t.Fatalf("expected anchor after long line, got %+v", anchors)
	}
}

func TestParseSpatialAnchorsEmbeddedInProse(t *testing.T) {
	text := `Place it here: {"type" : "spatial_anchor", "x": 4, "y": 5, "z": 6} and "quoted {stuff}" then {"type":"spatial_anchor","x":7}.`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 2 {
		t.Fatalf("expected 2 anchors, got %+v", anchors)
	}
	if anchors[0].X != 4 || anchors[0].Y != 5 || anchors[0].Z != 6 {
		t.Fatalf("unexpected first anchor: %+v", anchors[0])
	}
	if anchors[1].X != 7 || anchors[1].Y != 0 {
		t.Fatalf("unexpected second anchor: %+v", anchors[1])
	}
}

func TestParseSpatialAnchorsNestedAndEncoded(t *testing.T) {
	// Anchor nested in a JSON document, and anchor JSON-encoded inside a string
	// value (OpenAI-style chat completion content).
	text := `{"choices":[{"message":{"content":"Here {\"type\":\"spatial_anchor\",\"x\":9,\"y\":8,\"z\":7} done"}}],"extra":{"type":"spatial_anchor","x":1}}`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 2 {
		t.Fatalf("expected 2 anchors, got %+v", anchors)
	}
	seen := map[float64]bool{}
	for _, a := range anchors {
		seen[a.X] = true
	}
	if !seen[9] || !seen[1] {
		t.Fatalf("missing anchors: %+v", anchors)
	}
}

func TestParseSpatialAnchorsOutermostWins(t *testing.T) {
	text := `{"type":"spatial_anchor","x":1,"child":{"type":"spatial_anchor","x":2}} {"type":"spatial_anchor","x":3}`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 2 || anchors[0].X != 1 || anchors[1].X != 3 {
		t.Fatalf("expected outer anchors only, got %+v", anchors)
	}
}

func TestParseSpatialAnchorsRejectsInvalid(t *testing.T) {
	cases := []string{
		`{"type":"spatial_anchor","x":"1","y":2,"z":3}`,            // non-numeric coord
		`{"type":"spatial_anchor","x":1e999}`,                      // out of range number
		`{"type":"spatial_anchorX","x":1}`,                         // wrong type
		`{"kind":"spatial_anchor","x":1}`,                          // no type field
		`{"type":"spatial_anchor","x":1,}`,                         // malformed JSON
		`{"type":"spatial_anchor","x":[1]}`,                        // array coord
		`"type":"spatial_anchor","x":1}`,                           // no opening brace
		`{"type":"spatial_anchor","x":1`,                           // unterminated
		`{"type":"spatial_anchor","x":{"type":"nested"}}`,          // object coord
		`{"type":"spatial_anchor","note":"bad` + "\n" + `","x":1}`, // newline in string
	}
	for _, c := range cases {
		if got := ParseSpatialAnchors(c); len(got) != 0 {
			t.Errorf("expected no anchors for %q, got %+v", c, got)
		}
	}
}

func TestParseSpatialAnchorsRecoversAfterMalformedFragment(t *testing.T) {
	text := `broken {"note":"oops` + "\n" + `{"type":"spatial_anchor","x":5}`
	anchors := ParseSpatialAnchors(text)
	if len(anchors) != 1 || anchors[0].X != 5 {
		t.Fatalf("expected recovery after malformed fragment, got %+v", anchors)
	}
}

func TestParseSpatialAnchorsNeverNil(t *testing.T) {
	if got := ParseSpatialAnchors(""); got == nil {
		t.Fatal("expected empty non-nil slice")
	}
	b, _ := json.Marshal(ParseSpatialAnchors("nothing here"))
	if string(b) != "[]" {
		t.Fatalf("expected [], got %s", b)
	}
}

func TestParseSpatialAnchorsMaxAnchors(t *testing.T) {
	var sb strings.Builder
	for i := 0; i < DefaultMaxAnchors+50; i++ {
		sb.WriteString(`{"type":"spatial_anchor","x":1}` + "\n")
	}
	if got := ParseSpatialAnchors(sb.String()); len(got) != DefaultMaxAnchors {
		t.Fatalf("expected %d anchors, got %d", DefaultMaxAnchors, len(got))
	}
}

func TestParseSpatialAnchorsInputCap(t *testing.T) {
	text := strings.Repeat(" ", DefaultMaxParseBytes) + `{"type":"spatial_anchor","x":1}`
	if got := ParseSpatialAnchors(text); len(got) != 0 {
		t.Fatalf("expected anchors beyond the cap to be ignored, got %+v", got)
	}
}

func TestParseSpatialAnchorsAdversarialNestingIsFast(t *testing.T) {
	// Deeply nested objects each containing the anchor token must not cause
	// quadratic work, whether or not they are balanced.
	const open = `{"type":"spatial_anchor","a":`
	var unbalanced, balanced strings.Builder
	for unbalanced.Len() < DefaultMaxParseBytes-64 {
		unbalanced.WriteString(open)
	}
	group := strings.Repeat(open, 40) + "1" + strings.Repeat("}", 40)
	for balanced.Len()+len(group) < DefaultMaxParseBytes {
		balanced.WriteString(group)
	}
	deep := strings.Repeat(open, 30000) + "1" + strings.Repeat("}", 30000)
	// Valid JSON at every level but never a valid anchor (string coordinate),
	// forcing every nested candidate to be decoded.
	var invalidAnchors strings.Builder
	badGroup := strings.Repeat(`{"type":"spatial_anchor","x":"s","a":`, 20) + "1" + strings.Repeat("}", 20)
	for invalidAnchors.Len()+len(badGroup) < DefaultMaxParseBytes {
		invalidAnchors.WriteString(badGroup)
	}
	for _, in := range []string{unbalanced.String(), balanced.String(), deep, invalidAnchors.String()} {
		start := time.Now()
		_ = ParseSpatialAnchors(in)
		if d := time.Since(start); d > 10*time.Second {
			t.Fatalf("parsing adversarial input took %v", d)
		}
	}
}

func TestParseSpatialAnchorsReader(t *testing.T) {
	anchors, err := ParseSpatialAnchorsReader(strings.NewReader(`{"type":"spatial_anchor","x":1}`), 0)
	if err != nil || len(anchors) != 1 {
		t.Fatalf("unexpected result: %+v %v", anchors, err)
	}
	if _, err := ParseSpatialAnchorsReader(strings.NewReader(strings.Repeat("x", 11)), 10); !errors.Is(err, ErrInputTooLarge) {
		t.Fatalf("expected ErrInputTooLarge, got %v", err)
	}
	if got, err := ParseSpatialAnchorsReader(strings.NewReader(strings.Repeat("x", 10)), 10); err != nil || len(got) != 0 {
		t.Fatalf("exactly-at-cap input should be accepted: %v %v", got, err)
	}
	readErr := errors.New("boom")
	if _, err := ParseSpatialAnchorsReader(io.MultiReader(strings.NewReader("abc"), errReader{readErr}), 0); !errors.Is(err, readErr) {
		t.Fatalf("expected read error to propagate, got %v", err)
	}
	if got, err := ParseSpatialAnchorsReader(nil, 0); err != nil || got == nil {
		t.Fatalf("nil reader: %v %v", got, err)
	}
}

type errReader struct{ err error }

func (e errReader) Read([]byte) (int, error) { return 0, e.err }

func TestToWebXR(t *testing.T) {
	anchors := []SpatialAnchor{{Type: "spatial_anchor", X: 1, Y: 2, Z: 3}}
	payload := ToWebXR(anchors, "s1")
	if payload.Version != "1.0" {
		t.Fatalf("unexpected version: %s", payload.Version)
	}
	if payload.SessionID != "s1" {
		t.Fatalf("unexpected session id: %s", payload.SessionID)
	}
	if len(payload.Anchors) != 1 || payload.Anchors[0].X != 1 {
		t.Fatalf("unexpected anchor: %v", payload.Anchors[0])
	}
	if ToWebXR(nil, "").Anchors == nil {
		t.Fatal("expected non-nil anchors slice")
	}
}

func TestParseHandler(t *testing.T) {
	h := ParseHandler()

	t.Run("method", func(t *testing.T) {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodGet, "/v1/spatial/parse", nil))
		if w.Code != http.StatusMethodNotAllowed || w.Header().Get("Allow") != http.MethodPost {
			t.Fatalf("expected 405 with Allow, got %d %q", w.Code, w.Header().Get("Allow"))
		}
		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Fatalf("expected JSON error, got %q", ct)
		}
	})

	t.Run("ok", func(t *testing.T) {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodPost, "/v1/spatial/parse", strings.NewReader(`see {"type":"spatial_anchor","x":1,"y":2,"z":3}`)))
		if w.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d", w.Code)
		}
		var got []SpatialAnchor
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got) != 1 || got[0].Y != 2 {
			t.Fatalf("unexpected body %s (%v)", w.Body.String(), err)
		}
	})

	t.Run("empty", func(t *testing.T) {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodPost, "/v1/spatial/parse", strings.NewReader("nothing")))
		if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != "[]" {
			t.Fatalf("expected [] got %d %s", w.Code, w.Body.String())
		}
	})

	t.Run("webxr", func(t *testing.T) {
		w := httptest.NewRecorder()
		h(w, httptest.NewRequest(http.MethodPost, "/v1/spatial/parse?format=webxr&session_id=s9", strings.NewReader(`{"type":"spatial_anchor","x":1}`)))
		var got WebXRPayload
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || got.SessionID != "s9" || len(got.Anchors) != 1 {
			t.Fatalf("unexpected webxr body %s (%v)", w.Body.String(), err)
		}
	})

	t.Run("too large", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/spatial/parse", strings.NewReader(strings.Repeat("x", DefaultMaxParseBytes+1)))
		h(w, r)
		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413, got %d", w.Code)
		}
	})

	t.Run("too large without content length", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/spatial/parse", io.NopCloser(strings.NewReader(strings.Repeat("x", DefaultMaxParseBytes+1))))
		r.ContentLength = -1
		h(w, r)
		if w.Code != http.StatusRequestEntityTooLarge {
			t.Fatalf("expected 413, got %d", w.Code)
		}
	})

	t.Run("read error", func(t *testing.T) {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "/v1/spatial/parse", errReader{errors.New("broken")})
		r.ContentLength = -1
		h(w, r)
		if w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), "broken") {
			t.Fatalf("expected 400 without internals, got %d %s", w.Code, w.Body.String())
		}
	})
}

func TestSanitizeSessionID(t *testing.T) {
	if sanitizeSessionID("abc-123") != "abc-123" {
		t.Fatal("valid id rejected")
	}
	if sanitizeSessionID(strings.Repeat("a", maxSessionIDLen+1)) != "" {
		t.Fatal("oversized id accepted")
	}
	if sanitizeSessionID("a\nb") != "" {
		t.Fatal("control characters accepted")
	}
}
