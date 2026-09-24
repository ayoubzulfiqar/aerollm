package universal

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

func TestAnthropicAdapterResponseFormat(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body providers.AnthropicRequest
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body.ToolChoice == nil || body.ToolChoice.Type != "tool" || body.ToolChoice.Name != providers.AnthropicJSONToolName || len(body.Tools) != 1 {
			http.Error(w, `{"type":"error","error":{"type":"invalid_request_error","message":"no forced tool"}}`, 400)
			return
		}
		if body.Stream {
			w.Header().Set("Content-Type", "text/event-stream")
			io.WriteString(w, "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"m\",\"model\":\"claude\",\"usage\":{\"input_tokens\":2}}}\n\n"+
				"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"t\",\"name\":\"json_response\",\"input\":{}}}\n\n"+
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"ok\\\":\"}}\n\n"+
				"event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"true}\"}}\n\n"+
				"event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n"+
				"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":3}}\n\n"+
				"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n")
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"id":"m","model":"claude","content":[{"type":"tool_use","id":"t","name":"json_response","input":{"ok":true}}],"stop_reason":"tool_use","usage":{"input_tokens":2,"output_tokens":3}}`)
	}))
	defer srv.Close()
	a := NewAnthropicAdapterV2("k", srv.URL)
	req := userReq("claude", "json please")
	req.ResponseFormat = &models.ResponseFormat{Type: "json_object"}

	resp, err := a.ChatCompletions(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if c := resp.Choices[0]; *c.Message.Content != `{"ok":true}` || c.FinishReason != "stop" || len(c.Message.ToolCalls) != 0 {
		t.Fatalf("non-streaming: %+v", c)
	}

	ch, err := a.StreamChatCompletions(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var content, finish string
	for _, c := range collectChunks(t, ch) {
		if c.Err != nil {
			t.Fatal(c.Err)
		}
		for _, chc := range c.Choices {
			content += chc.Delta.Content
			if len(chc.Delta.ToolCalls) > 0 {
				t.Fatal("no tool call deltas in JSON mode")
			}
			if chc.FinishReason != nil {
				finish = *chc.FinishReason
			}
		}
	}
	if content != `{"ok":true}` || finish != "stop" {
		t.Fatalf("streaming: %q %q", content, finish)
	}

	req.Tools = []models.ToolDefinition{{Type: "function", Name: "lookup"}}
	if _, err := a.ChatCompletions(context.Background(), req); providers.StatusCode(err) != http.StatusBadRequest {
		t.Fatalf("response_format + tools must be a 400: %v", err)
	}
}
