package mcp

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestMCPInitialize(t *testing.T) {
	s := NewServer()
	req := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(`{"jsonrpc":"2.0","id":1,"method":"initialize"}`))
	w := httptest.NewRecorder()
	s.HandleHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), `"protocolVersion":"2024-11-05"`) {
		t.Fatalf("expected protocolVersion in response, got: %s", w.Body.String())
	}
}

func TestMCPToolsListAndCall(t *testing.T) {
	s := NewServer()
	s.RegisterTool(ToolDefinition{
		Name:        "echo",
		Description: "echoes input",
		InputSchema: map[string]interface{}{"type": "object"},
		Handler: func(ctx context.Context, args map[string]interface{}) (interface{}, error) {
			return args, nil
		},
	})
	listReq := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(`{"jsonrpc":"2.0","id":2,"method":"tools/list"}`))
	w := httptest.NewRecorder()
	s.HandleHTTP(w, listReq)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), `"name":"echo"`) {
		t.Fatalf("expected tool listing, got: %s", w.Body.String())
	}

	callReq := httptest.NewRequest(http.MethodPost, "/mcp", bytes.NewBufferString(`{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"echo","arguments":{"x":1}}}`))
	w = httptest.NewRecorder()
	s.HandleHTTP(w, callReq)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if !contains(w.Body.String(), "map[x:1]") {
		t.Fatalf("expected echoed args, got: %s", w.Body.String())
	}
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle || len(haystack) > 0 && containsSubstr(haystack, needle))
}

func containsSubstr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
