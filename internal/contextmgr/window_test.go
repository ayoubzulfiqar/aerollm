package contextmgr

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

func TestContextWindowTable(t *testing.T) {
	cases := map[string]int{
		"gpt-4o":                               128000,
		"gpt-4o-mini-2024-07-18":               128000,
		"gpt-4.1-nano":                         1047576,
		"gpt-4":                                8192,
		"gpt-4-0613":                           8192,
		"gpt-4-turbo-preview":                  128000,
		"openai/gpt-4o":                        128000,
		"claude-3-5-sonnet-20241022":           200000,
		"claude-sonnet-4-20250514":             200000,
		"anthropic.claude-3-haiku-20240307-v1": 200000,
		"gemini-1.5-pro-002":                   2097152,
		"models/gemini-2.5-flash":              1048576,
		"llama3.1:8b":                          131072,
		"llama3:8b":                            8192,
		"meta-llama/Llama-3.3-70B-Instruct":    131072,
		"Llama-3-8b":                           8192,
		"totally-unknown-model":                DefaultContextWindow,
		"":                                     DefaultContextWindow,
	}
	for model, want := range cases {
		if got := ContextWindow(model); got != want {
			t.Errorf("ContextWindow(%q) = %d, want %d", model, got, want)
		}
	}
	if got := NewContextManager(map[string]int{"custom": 1234}).Limit("custom"); got != 1234 {
		t.Fatalf("override ignored: %d", got)
	}
	if got := NewContextManager(nil).Limit("gpt-4o"); got != 128000 {
		t.Fatalf("nil overrides should fall back to the table, got %d", got)
	}
}

func TestEstimateTokens(t *testing.T) {
	if EstimateTokens("") != 0 || EstimateTokens("   ") != 0 {
		t.Fatal("blank text has no tokens")
	}
	if got := EstimateTokens(strings.Repeat("a", 400)); got != 100 {
		t.Fatalf("expected ~4 chars/token, got %d", got)
	}
	if got := EstimateTokens("你好世界"); got != 4 {
		t.Fatalf("expected one token per CJK character, got %d", got)
	}
	msgs := []models.Message{{Role: models.RoleUser, Content: strPtr(strings.Repeat("a", 40))}}
	if got := EstimateMessagesTokens(msgs); got != 10+messageOverheadTokens+replyPrimingTokens {
		t.Fatalf("unexpected message estimate %d", got)
	}
	withTools := []models.Message{{Role: models.RoleAssistant, ToolCalls: []models.ToolCall{{ID: "c1", Function: models.ToolFunction{Name: "lookup", Arguments: strings.Repeat("x", 400)}}}}}
	if EstimateMessagesTokens(withTools) < 100 {
		t.Fatal("tool call arguments must be counted")
	}
}

func text(n int) *string { s := strings.Repeat("word ", n); return &s }

func toolTurn(id string) []models.Message {
	return []models.Message{
		{Role: models.RoleAssistant, ToolCalls: []models.ToolCall{{ID: id, Type: "function", Function: models.ToolFunction{Name: "f", Arguments: "{}"}}}},
		{Role: models.RoleTool, ToolCallID: &id, Content: text(50)},
	}
}

func TestTrimToFitKeepsSystemAndRecentTurns(t *testing.T) {
	var msgs []models.Message
	msgs = append(msgs, models.Message{Role: models.RoleSystem, Content: strPtr("you are helpful")})
	for i := 0; i < 50; i++ {
		msgs = append(msgs, models.Message{Role: models.RoleUser, Content: text(400)})
		msgs = append(msgs, toolTurn(fmt.Sprintf("c%d", i))...)
		msgs = append(msgs, models.Message{Role: models.RoleAssistant, Content: text(100)})
	}
	// gpt-4 has an 8192 token window; reserve 1000 for output.
	out := TrimToFit(msgs, "gpt-4", 1000)
	if EstimateMessagesTokens(out) > 8192-1000 {
		t.Fatalf("trimmed prompt still too large: %d tokens", EstimateMessagesTokens(out))
	}
	if out[0].Role != models.RoleSystem || *out[0].Content != "you are helpful" {
		t.Fatal("system message must be kept")
	}
	if out[1].Role != models.RoleUser {
		t.Fatalf("history should restart at a user turn, got %s", out[1].Role)
	}
	last := out[len(out)-1]
	if last.Content != msgs[len(msgs)-1].Content {
		t.Fatal("most recent message must be kept")
	}
	assertToolPairsIntact(t, out)
	if len(msgs) != 201 {
		t.Fatal("input must not be modified")
	}
}

func assertToolPairsIntact(t *testing.T, msgs []models.Message) {
	t.Helper()
	pending := map[string]bool{}
	for i, m := range msgs {
		switch m.Role {
		case models.RoleAssistant:
			for _, tc := range m.ToolCalls {
				pending[tc.ID] = true
			}
		case models.RoleTool:
			if m.ToolCallID == nil || !pending[*m.ToolCallID] {
				t.Fatalf("message %d is a tool result without its assistant tool_calls message", i)
			}
			delete(pending, *m.ToolCallID)
		}
	}
	if len(pending) != 0 {
		t.Fatalf("assistant tool_calls without results: %v", pending)
	}
}

func TestTrimToFitNeverSplitsToolUnits(t *testing.T) {
	msgs := []models.Message{{Role: models.RoleUser, Content: text(10)}}
	msgs = append(msgs, toolTurn("a")...)
	msgs = append(msgs, toolTurn("b")...)
	// Budget so small that only the last unit survives.
	out := NewContextManager(map[string]int{"tiny": 50}).TrimToFit(msgs, "tiny", 0)
	assertToolPairsIntact(t, out)
	if len(out) != 2 || out[0].Role != models.RoleAssistant {
		t.Fatalf("expected only the last tool unit, got %+v", out)
	}
}

func TestTrimToFitNoopWhenFits(t *testing.T) {
	msgs := []models.Message{{Role: models.RoleUser, Content: strPtr("hi")}}
	if out := TrimToFit(msgs, "gpt-4o", 100); len(out) != 1 {
		t.Fatal("small conversation must be untouched")
	}
	if out := TrimToFit(nil, "gpt-4o", 100); len(out) != 0 {
		t.Fatal("nil input must stay empty")
	}
	if out := TrimToFit(msgs, "gpt-4", 1<<30); len(out) != 1 {
		t.Fatal("the latest turn must always be kept")
	}
}

func TestMaybeSummarizeKeepsSystemAndToolPairs(t *testing.T) {
	cm := NewContextManager(map[string]int{"m": 2000})
	msgs := []models.Message{{Role: models.RoleSystem, Content: strPtr("rules")}}
	for i := 0; i < 30; i++ {
		msgs = append(msgs, models.Message{Role: models.RoleUser, Content: text(60)})
		msgs = append(msgs, toolTurn(fmt.Sprintf("t%d", i))...)
	}
	res, err := cm.MaybeSummarize(context.Background(), "m", msgs)
	if err != nil {
		t.Fatal(err)
	}
	if !res.Summarized {
		t.Fatal("expected summarization")
	}
	if res.Messages[0].Role != models.RoleSystem || *res.Messages[0].Content != "rules" {
		t.Fatal("original system prompt must stay first and verbatim")
	}
	if res.Messages[1].Role != models.RoleSystem || !strings.Contains(*res.Messages[1].Content, "Summary") {
		t.Fatalf("summary should follow the system prompt, got %+v", res.Messages[1])
	}
	assertToolPairsIntact(t, res.Messages[2:])
	if EstimateMessagesTokens(res.Messages) > 2000 {
		t.Fatalf("summarized prompt exceeds window: %d", EstimateMessagesTokens(res.Messages))
	}
	if EstimateTokens(*res.Summary.Content) > 260 {
		t.Fatalf("summary exceeds its budget: %d tokens", EstimateTokens(*res.Summary.Content))
	}
}
