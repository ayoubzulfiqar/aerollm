package genui

import (
	"encoding/json"
	"fmt"
	"strings"
)

// UISchemaMarker is the sentinel JSON object the interceptor looks for.
const UISchemaMarker = `"type":"aerollm_ui"`

// UISchema represents the structured UI payload emitted by the LLM.
type UISchema struct {
	Type       string        `json:"type"`
	Components []Component   `json:"components"`
}

// Component represents a single frontend UI component.
type Component struct {
	Kind      string                 `json:"kind"`
	Props     map[string]interface{} `json:"props,omitempty"`
	Children  []Component            `json:"children,omitempty"`
}

// SSEChunk is a normalized UI event for frontend streaming.
type SSEChunk struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
}

// EventUISchema is emitted when a UI schema is detected.
const EventUISchema = "ui_schema"

// EventUIFragment is emitted for partial UI chunks.
const EventUIFragment = "ui_fragment"

// EventText is emitted for plain text chunks.
const EventText = "text"

// Intercept scans text for an embedded UI schema and returns normalized SSE chunks.
func Intercept(text string) []SSEChunk {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}

	if !strings.Contains(trimmed, UISchemaMarker) {
		return []SSEChunk{{Event: EventText, Data: trimmed}}
	}

	start := strings.Index(trimmed, "{")
	end := strings.LastIndex(trimmed, "}")
	if start < 0 || end < 0 || end <= start {
		return []SSEChunk{{Event: EventText, Data: trimmed}}
	}

	var chunks []SSEChunk
	prefix := strings.TrimSpace(trimmed[:start])
	if prefix != "" {
		chunks = append(chunks, SSEChunk{Event: EventText, Data: prefix})
	}

	var schema UISchema
	if err := json.Unmarshal([]byte(trimmed[start:end+1]), &schema); err != nil {
		return []SSEChunk{{Event: EventText, Data: trimmed}}
	}

	chunks = append(chunks, SSEChunk{Event: EventUISchema, Data: schema})

	suffix := strings.TrimSpace(trimmed[end+1:])
	if suffix != "" {
		chunks = append(chunks, SSEChunk{Event: EventText, Data: suffix})
	}

	return chunks
}

// Stringify converts SSE chunks into a simple text representation for logging/debug.
func Stringify(chunks []SSEChunk) string {
	var b strings.Builder
	for _, c := range chunks {
		switch c.Event {
		case EventUISchema:
			b.WriteString("[ui-schema]")
		case EventUIFragment:
			b.WriteString("[ui-fragment]")
		default:
			b.WriteString("[text]")
		}
		b.WriteString(fmt.Sprintf("%v ", c.Data))
	}
	return strings.TrimSpace(b.String())
}
