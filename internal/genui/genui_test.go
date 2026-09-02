package genui

import (
	"strings"
	"testing"
)

func TestInterceptPlainText(t *testing.T) {
	chunks := Intercept("hello world")
	if len(chunks) != 1 || chunks[0].Event != EventText || chunks[0].Data != "hello world" {
		t.Fatalf("unexpected chunks: %+v", chunks)
	}
}

func TestInterceptExtractsSchema(t *testing.T) {
	chunks := Intercept("prefix {\"type\":\"aerollm_ui\",\"components\":[]} suffix")
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d: %+v", len(chunks), chunks)
	}
	if chunks[0].Event != EventText || chunks[0].Data != "prefix" {
		t.Fatalf("unexpected prefix chunk: %+v", chunks[0])
	}
	if chunks[1].Event != EventUISchema {
		t.Fatalf("expected ui_schema event, got %s", chunks[1].Event)
	}
	if chunks[2].Event != EventText || chunks[2].Data != "suffix" {
		t.Fatalf("unexpected suffix chunk: %+v", chunks[2])
	}
}

func TestInterceptSchemaOnly(t *testing.T) {
	chunks := Intercept(`{"type":"aerollm_ui","components":[]}`)
	if len(chunks) != 1 || chunks[0].Event != EventUISchema {
		t.Fatalf("expected single ui_schema event, got %+v", chunks)
	}
}

func TestInterceptMalformedJSONFallsBack(t *testing.T) {
	chunks := Intercept("text {\"type\":\"aerollm_ui\", bad} more")
	if len(chunks) != 1 || chunks[0].Event != EventText {
		t.Fatalf("expected plain text fallback, got %+v", chunks)
	}
}

func TestStringifyFormatsChunks(t *testing.T) {
	out := Stringify([]SSEChunk{
		{Event: EventText, Data: "hello"},
		{Event: EventUISchema, Data: UISchema{Type: "aerollm_ui"}},
	})
	if !strings.Contains(out, "[text]hello") || !strings.Contains(out, "[ui-schema]") {
		t.Fatalf("unexpected stringify output: %s", out)
	}
}
