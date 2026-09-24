package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func jsonReq(rf *models.ResponseFormat) *models.LLMRequest {
	return &models.LLMRequest{Model: "claude", Messages: []models.Message{{Role: "user", Content: strp("give me json")}}, ResponseFormat: rf}
}

var personSchema = &models.ResponseFormat{Type: "json_schema", JSONSchema: &models.JSONSchema{
	Name:   "person",
	Schema: map[string]any{"type": "object", "properties": map[string]any{"name": map[string]any{"type": "string"}}, "required": []any{"name"}},
}}

func TestAnthropicResponseFormatRequestMapping(t *testing.T) {
	out, err := BuildAnthropicRequest("anthropic", jsonReq(personSchema), "", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(out.Tools) != 1 || out.Tools[0].Name != AnthropicJSONToolName || out.JSONResponseTool() != AnthropicJSONToolName {
		t.Fatalf("tools: %+v", out.Tools)
	}
	if out.ToolChoice == nil || out.ToolChoice.Type != "tool" || out.ToolChoice.Name != AnthropicJSONToolName {
		t.Fatalf("tool_choice: %+v", out.ToolChoice)
	}
	if out.Tools[0].InputSchema["type"] != "object" || out.Tools[0].InputSchema["required"] == nil || !strings.Contains(out.Tools[0].Description, "person") {
		t.Fatalf("schema: %+v", out.Tools[0])
	}
	// The emulation detail must not leak into the wire body.
	b, _ := json.Marshal(out)
	if strings.Contains(string(b), "jsonTool") {
		t.Fatalf("internal field serialized: %s", b)
	}

	obj, err := BuildAnthropicRequest("anthropic", jsonReq(&models.ResponseFormat{Type: "json_object"}), "", true)
	if err != nil || len(obj.Tools) != 1 || obj.Tools[0].InputSchema["type"] != "object" || obj.ToolChoice.Name != AnthropicJSONToolName {
		t.Fatalf("json_object: %+v %v", obj, err)
	}
	// A schema without an explicit root type is completed as an object.
	noType, err := BuildAnthropicRequest("anthropic", jsonReq(&models.ResponseFormat{Type: "json_schema", JSONSchema: &models.JSONSchema{Schema: map[string]any{"properties": map[string]any{}}}}), "", false)
	if err != nil || noType.Tools[0].InputSchema["type"] != "object" {
		t.Fatalf("schema without type: %v", err)
	}
	text, err := BuildAnthropicRequest("anthropic", jsonReq(&models.ResponseFormat{Type: "text"}), "", false)
	if err != nil || len(text.Tools) != 0 || text.JSONResponseTool() != "" {
		t.Fatalf("text format must be a no-op: %+v %v", text, err)
	}

	bad := []*models.LLMRequest{
		jsonReq(&models.ResponseFormat{Type: "json_schema"}),
		jsonReq(&models.ResponseFormat{Type: "json_schema", JSONSchema: &models.JSONSchema{Name: "x"}}),
		jsonReq(&models.ResponseFormat{Type: "json_schema", JSONSchema: &models.JSONSchema{Schema: map[string]any{"type": "array"}}}),
		jsonReq(&models.ResponseFormat{Type: "yaml"}),
	}
	withTools := jsonReq(personSchema)
	withTools.Tools = []models.ToolDefinition{{Type: "function", Name: "lookup", Parameters: map[string]interface{}{"type": "object"}}}
	bad = append(bad, withTools)
	for i, r := range bad {
		_, err := BuildAnthropicRequest("anthropic", r, "", false)
		if StatusCode(err) != http.StatusBadRequest || IsRetryable(err) {
			t.Fatalf("case %d: want 400, got %v", i, err)
		}
	}
	if _, err := BuildAnthropicRequest("anthropic", withTools, "", false); !strings.Contains(err.Error(), "cannot be combined with tools") {
		t.Fatalf("tools conflict message: %v", err)
	}
}

func TestAnthropicResponseFormatNonStreaming(t *testing.T) {
	var got AnthropicRequest
	var body atomic.Value
	body.Store(`{"id":"msg_1","type":"message","role":"assistant","model":"claude","content":[{"type":"tool_use","id":"toolu_1","name":"json_response","input":{"name":"Ada"}}],"stop_reason":"tool_use","usage":{"input_tokens":5,"output_tokens":7}}`)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body.Load().(string))
	}))
	defer srv.Close()
	p := NewAnthropicProvider(srv.URL, "k", "claude")
	resp, err := p.ChatCompletions(context.Background(), jsonReq(personSchema))
	if err != nil {
		t.Fatal(err)
	}
	if got.ToolChoice == nil || got.ToolChoice.Name != AnthropicJSONToolName || len(got.Tools) != 1 {
		t.Fatalf("wire request: %+v", got)
	}
	c := resp.Choices[0]
	if c.Message.Content == nil || *c.Message.Content != `{"name":"Ada"}` || len(c.Message.ToolCalls) != 0 || c.FinishReason != "stop" {
		t.Fatalf("converted: %+v", c)
	}
	var v map[string]string
	if json.Unmarshal([]byte(*c.Message.Content), &v) != nil || v["name"] != "Ada" {
		t.Fatal("content must be valid JSON")
	}

	// Truncated generation keeps finish_reason "length".
	body.Store(`{"id":"msg_2","model":"claude","content":[{"type":"tool_use","id":"t","name":"json_response","input":{}}],"stop_reason":"max_tokens","usage":{}}`)
	resp, err = p.ChatCompletions(context.Background(), jsonReq(&models.ResponseFormat{Type: "json_object"}))
	if err != nil || resp.Choices[0].FinishReason != "length" || *resp.Choices[0].Message.Content != "{}" {
		t.Fatalf("max_tokens: %+v %v", resp, err)
	}

	// Without response_format a json_response tool call stays a tool call.
	body.Store(`{"id":"m","model":"claude","content":[{"type":"tool_use","id":"t","name":"json_response","input":{"a":1}}],"stop_reason":"tool_use","usage":{}}`)
	plain := jsonReq(nil)
	plain.Tools = []models.ToolDefinition{{Type: "function", Name: "json_response"}}
	resp, err = p.ChatCompletions(context.Background(), plain)
	if err != nil || len(resp.Choices[0].Message.ToolCalls) != 1 || resp.Choices[0].FinishReason != "tool_calls" {
		t.Fatalf("plain tool call: %+v %v", resp, err)
	}
}

func anthropicJSONStream(deltas ...string) string {
	s := "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"id\":\"msg_j\",\"model\":\"claude\",\"usage\":{\"input_tokens\":4}}}\n\n" +
		"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_j\",\"name\":\"json_response\",\"input\":{}}}\n\n"
	for _, d := range deltas {
		b, _ := json.Marshal(d)
		s += "event: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":" + string(b) + "}}\n\n"
	}
	return s + "event: content_block_stop\ndata: {\"type\":\"content_block_stop\",\"index\":0}\n\n" +
		"event: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"tool_use\"},\"usage\":{\"output_tokens\":9}}\n\n" +
		"event: message_stop\ndata: {\"type\":\"message_stop\"}\n\n"
}

func TestAnthropicResponseFormatStreaming(t *testing.T) {
	for _, tc := range []struct {
		deltas []string
		want   string
	}{
		{[]string{`{"na`, `me": "A`, `da"}`}, `{"name": "Ada"}`},
		{[]string{"", ""}, `{}`}, // the model produced an empty object
	} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var got AnthropicRequest
			_ = json.NewDecoder(r.Body).Decode(&got)
			if got.ToolChoice == nil || got.ToolChoice.Name != AnthropicJSONToolName || !got.Stream {
				http.Error(w, "missing forced tool", 400)
				return
			}
			sse(w, anthropicJSONStream(tc.deltas...))
		}))
		ch, err := NewAnthropicProvider(srv.URL, "k", "claude").StreamChatCompletions(context.Background(), jsonReq(personSchema))
		if err != nil {
			srv.Close()
			t.Fatal(err)
		}
		var content, finish string
		var usage *models.Usage
		for _, c := range collect(t, ch) {
			if c.Err != nil {
				t.Fatal(c.Err)
			}
			for _, chc := range c.Choices {
				if len(chc.Delta.ToolCalls) > 0 {
					t.Fatalf("the emulation tool must not surface as a tool call: %+v", chc.Delta)
				}
				content += chc.Delta.Content
				if chc.FinishReason != nil {
					finish = *chc.FinishReason
				}
			}
			if c.Usage != nil {
				usage = c.Usage
			}
		}
		srv.Close()
		if content != tc.want || finish != "stop" || usage == nil || usage.CompletionTokens != 9 {
			t.Fatalf("content=%q finish=%q usage=%+v", content, finish, usage)
		}
	}
}
