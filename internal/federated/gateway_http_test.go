package federated

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRegisterNodeHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/federated/nodes/register", RegisterNodeHandler(registry))

	pub, _, _ := ed25519.GenerateKey(nil)
	body := `{"node_id":"n1","endpoint":"http://n1","public_key":"` + base64.StdEncoding.EncodeToString(pub) + `","algorithms":["a"]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/federated/nodes/register", strings.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"status":"registered"`) {
		t.Fatalf("expected registered status, got: %s", rec.Body.String())
	}
	k, ok := registry.PublicKey("n1")
	if !ok || string(k) != string(pub) {
		t.Fatalf("public key not decoded and stored")
	}
}

func TestRegisterNodeHandlerKeyEncodings(t *testing.T) {
	pub, _, _ := ed25519.GenerateKey(nil)
	for name, enc := range map[string]string{
		"hex":        hex.EncodeToString(pub),
		"std":        base64.StdEncoding.EncodeToString(pub),
		"raw url":    base64.RawURLEncoding.EncodeToString(pub),
		"url padded": base64.URLEncoding.EncodeToString(pub),
	} {
		t.Run(name, func(t *testing.T) {
			b, err := DecodePublicKey(enc)
			if err != nil || string(b) != string(pub) {
				t.Fatalf("decode failed: %v", err)
			}
		})
	}
	if _, err := DecodePublicKey("pk"); err == nil {
		t.Fatal("expected error for invalid key")
	}
	if b, err := DecodePublicKey(""); err != nil || b != nil {
		t.Fatal("empty key should decode to nil without error")
	}
}

func TestRegisterNodeHandlerErrors(t *testing.T) {
	registry := NewGatewayRegistry()
	h := RegisterNodeHandler(registry)
	pub, _, _ := ed25519.GenerateKey(nil)
	pub2, _, _ := ed25519.GenerateKey(nil)

	do := func(method, body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, "/v1/federated/nodes/register", strings.NewReader(body))
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec
	}

	rec := do(http.MethodGet, "")
	if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("expected 405 with Allow: POST, got %d %q", rec.Code, rec.Header().Get("Allow"))
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Fatalf("expected JSON error content type, got %q", ct)
	}

	cases := map[string]struct {
		body string
		code int
	}{
		"bad json":       {`{`, http.StatusBadRequest},
		"trailing data":  {`{"node_id":"n1"} {}`, http.StatusBadRequest},
		"empty id":       {`{"node_id":""}`, http.StatusBadRequest},
		"bad key":        {`{"node_id":"n1","public_key":"pk"}`, http.StatusBadRequest},
		"bad endpoint":   {`{"node_id":"n1","endpoint":"gopher://x"}`, http.StatusBadRequest},
		"too large body": {`{"node_id":"` + strings.Repeat("a", MaxRegisterBodyBytes) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			rec := do(http.MethodPost, tc.body)
			if rec.Code != tc.code {
				t.Fatalf("expected %d, got %d: %s", tc.code, rec.Code, rec.Body.String())
			}
			var payload map[string]string
			if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil || payload["error"] == "" {
				t.Fatalf("expected JSON error body, got %s", rec.Body.String())
			}
		})
	}

	ok := do(http.MethodPost, `{"node_id":"n1","public_key":"`+hex.EncodeToString(pub)+`"}`)
	if ok.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", ok.Code)
	}
	conflict := do(http.MethodPost, `{"node_id":"n1","public_key":"`+hex.EncodeToString(pub2)+`"}`)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("expected 409 on key takeover, got %d", conflict.Code)
	}

	nilReg := httptest.NewRecorder()
	RegisterNodeHandler(nil)(nilReg, httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`)))
	if nilReg.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for nil registry, got %d", nilReg.Code)
	}
}

func TestLatestNodeHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	_ = registry.Register(context.Background(), &NodeRegistration{NodeID: "n1", Endpoint: "http://n1"})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/federated/nodes/latest", LatestNodeHandler(registry))
	req := httptest.NewRequest(http.MethodGet, "/v1/federated/nodes/latest", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"NodeID":"n1"`) {
		t.Fatalf("expected node id in response, got: %s", rec.Body.String())
	}

	post := httptest.NewRecorder()
	LatestNodeHandler(registry)(post, httptest.NewRequest(http.MethodPost, "/", nil))
	if post.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", post.Code)
	}

	empty := httptest.NewRecorder()
	LatestNodeHandler(NewGatewayRegistry())(empty, httptest.NewRequest(http.MethodGet, "/", nil))
	if empty.Code != http.StatusNotFound || empty.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("expected JSON 404, got %d %q", empty.Code, empty.Header().Get("Content-Type"))
	}
}

func TestNodeHistoryHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	_ = registry.Register(context.Background(), &NodeRegistration{NodeID: "n1"})
	_ = registry.Register(context.Background(), &NodeRegistration{NodeID: "n2"})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/federated/nodes/history", NodeHistoryHandler(registry))
	req := httptest.NewRequest(http.MethodGet, "/v1/federated/nodes/history", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var history []*NodeRegistration
	if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
		t.Fatalf("invalid json: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(history))
	}

	emptyRec := httptest.NewRecorder()
	NodeHistoryHandler(NewGatewayRegistry())(emptyRec, httptest.NewRequest(http.MethodGet, "/", nil))
	if strings.TrimSpace(emptyRec.Body.String()) != "[]" {
		t.Fatalf("expected empty JSON array, got %s", emptyRec.Body.String())
	}
}

func TestNodeHandler(t *testing.T) {
	registry := NewGatewayRegistry()
	_ = registry.Register(context.Background(), &NodeRegistration{NodeID: "n1"})
	h := NodeHandler(registry)

	rec := httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/?id=n1", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for missing id, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h(rec, httptest.NewRequest(http.MethodGet, "/?id=zzz", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	h(rec, nil)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("nil request must not panic; got %d", rec.Code)
	}
}

func TestAggregateHandler(t *testing.T) {
	h := AggregateHandler(NewFedAvgAggregator())
	do := func(method, body string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		h(rec, httptest.NewRequest(method, "/v1/federated/aggregate", strings.NewReader(body)))
		return rec
	}

	rec := do(http.MethodPost, `[{"Rows":1,"Cols":2,"Data":[1,2],"Owner":"e1"},{"Rows":1,"Cols":2,"Data":[3,4],"Owner":"e2"}]`)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var out LoRAMatrix
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || out.Data[0] != 2 || out.Data[1] != 3 {
		t.Fatalf("unexpected result %s (%v)", rec.Body.String(), err)
	}

	if rec := do(http.MethodGet, ""); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("expected 405 with Allow header, got %d", rec.Code)
	}
	for name, body := range map[string]string{
		"mismatch": `[{"Rows":1,"Cols":2,"Data":[1,2]},{"Rows":2,"Cols":1,"Data":[3,4]}]`,
		"bad len":  `[{"Rows":2,"Cols":2,"Data":[1,2]}]`,
		"empty":    `[]`,
		"bad json": `[{`,
	} {
		t.Run(name, func(t *testing.T) {
			rec := do(http.MethodPost, body)
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
			}
		})
	}

	big := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("["+strings.Repeat("1,", MaxAggregateBodyBytes/2)+"1]"))
	h(big, req)
	if big.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", big.Code)
	}
}
