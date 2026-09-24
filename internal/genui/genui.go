package genui

import (
	"bytes"
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
)

// UISchemaMarker is the sentinel JSON object the interceptor looks for.
const UISchemaMarker = `"type":"aerollm_ui"`

// markerPattern matches the marker with optional whitespace around the colon.
var markerPattern = regexp.MustCompile(`"type"\s*:\s*"aerollm_ui"`)

// UISchema represents the structured UI payload emitted by the LLM.
type UISchema struct {
	Type       string      `json:"type"`
	Components []Component `json:"components"`
}

// Component represents a single frontend UI component.
type Component struct {
	Kind     string                 `json:"kind"`
	Props    map[string]interface{} `json:"props,omitempty"`
	Children []Component            `json:"children,omitempty"`
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

// maxInterceptBytes bounds the text scanned for a UI schema; maxSchemaStarts
// bounds how many candidate '{' positions are tried (avoids quadratic work).
const (
	maxInterceptBytes = 1 << 20
	maxSchemaStarts   = 32
)

// Intercept scans text for an embedded UI schema (a JSON object whose "type"
// is "aerollm_ui") and returns normalized chunks: optional leading text, the
// schema, and optional trailing text. Markdown code fences around the schema
// are dropped. Text without a valid schema is returned as a single text chunk.
func Intercept(text string) []SSEChunk {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return nil
	}
	if len(trimmed) > maxInterceptBytes {
		return []SSEChunk{{Event: EventText, Data: trimmed}}
	}
	loc := markerPattern.FindStringIndex(trimmed)
	if loc == nil {
		return []SSEChunk{{Event: EventText, Data: trimmed}}
	}

	// Try each '{' before the marker (nearest first) as the start of the
	// schema object; the decoded object must span the marker.
	attempts := 0
	for start := strings.LastIndex(trimmed[:loc[0]], "{"); start >= 0 && attempts < maxSchemaStarts; start = strings.LastIndex(trimmed[:start], "{") {
		attempts++
		dec := json.NewDecoder(strings.NewReader(trimmed[start:]))
		var raw json.RawMessage
		if err := dec.Decode(&raw); err != nil {
			continue
		}
		end := start + int(dec.InputOffset())
		if end < loc[1] {
			continue
		}
		var schema UISchema
		if err := json.Unmarshal(raw, &schema); err != nil || schema.Type != "aerollm_ui" {
			continue
		}
		if schema.Components == nil {
			schema.Components = []Component{}
		}
		var chunks []SSEChunk
		if prefix := stripFence(trimmed[:start], true); prefix != "" {
			chunks = append(chunks, SSEChunk{Event: EventText, Data: prefix})
		}
		chunks = append(chunks, SSEChunk{Event: EventUISchema, Data: schema})
		if suffix := stripFence(trimmed[end:], false); suffix != "" {
			chunks = append(chunks, SSEChunk{Event: EventText, Data: suffix})
		}
		return chunks
	}
	return []SSEChunk{{Event: EventText, Data: trimmed}}
}

// stripFence trims whitespace and removes a markdown code fence adjacent to
// the schema (an opening ``` / ```json at the end of the prefix, or a closing
// ``` at the start of the suffix).
func stripFence(s string, prefix bool) string {
	s = strings.TrimSpace(s)
	if prefix {
		if i := strings.LastIndex(s, "```"); i >= 0 {
			rest := strings.TrimSpace(s[i+3:])
			if rest == "" || rest == "json" {
				s = strings.TrimSpace(s[:i])
			}
		}
		return s
	}
	if strings.HasPrefix(s, "```") {
		s = strings.TrimSpace(s[3:])
	}
	return s
}

// HasUISchema reports whether chunks contain a UI schema event.
func HasUISchema(chunks []SSEChunk) bool {
	for _, c := range chunks {
		if c.Event == EventUISchema {
			return true
		}
	}
	return false
}

// EncodeSSE renders a chunk as a Server-Sent Events frame
// ("event: <name>\ndata: <json>\n\n").
func EncodeSSE(c SSEChunk) ([]byte, error) {
	data, err := json.Marshal(c.Data)
	if err != nil {
		return nil, err
	}
	var buf bytes.Buffer
	buf.WriteString("event: ")
	buf.WriteString(strings.NewReplacer("\n", " ", "\r", " ").Replace(c.Event))
	buf.WriteString("\ndata: ")
	buf.Write(data)
	buf.WriteString("\n\n")
	return buf.Bytes(), nil
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
