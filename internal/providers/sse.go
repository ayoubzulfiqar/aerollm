package providers

import (
	"bufio"
	"errors"
	"io"
	"strings"
)

// MaxSSELineBytes bounds a single Server-Sent Events line.
const MaxSSELineBytes = 8 << 20

// MaxSSEEventBytes bounds the accumulated data of a single event.
const MaxSSEEventBytes = 16 << 20

// ErrSSEEventTooLarge is returned when an event exceeds MaxSSEEventBytes.
var ErrSSEEventTooLarge = errors.New("sse event too large")

// SSEEvent is one Server-Sent Event.
type SSEEvent struct {
	// Event is the event type ("" for the default "message" type).
	Event string
	// Data is the event data; multiple data lines are joined with "\n".
	Data string
	// ID is the last event ID field seen for this event.
	ID string
}

// SSEReader parses a text/event-stream body with bounded memory use.
type SSEReader struct {
	sc *bufio.Scanner
}

// NewSSEReader returns a reader for r.
func NewSSEReader(r io.Reader) *SSEReader {
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64<<10), MaxSSELineBytes)
	return &SSEReader{sc: sc}
}

// Next returns the next event that carries data or an event name. Comment
// lines are skipped. A bare JSON line (as sent by some non-conforming
// servers) is returned as a data-only event. It returns io.EOF at the end of
// the stream (a trailing event without a terminating blank line is still
// returned first) and bufio.ErrTooLong for oversized lines.
func (r *SSEReader) Next() (SSEEvent, error) {
	var ev SSEEvent
	var data strings.Builder
	hasData := false
	for r.sc.Scan() {
		line := r.sc.Text()
		if line == "" {
			if hasData || ev.Event != "" {
				ev.Data = data.String()
				return ev, nil
			}
			continue
		}
		switch line[0] {
		case ':':
			continue
		case '{', '[':
			if !hasData && ev.Event == "" {
				return SSEEvent{Data: line}, nil
			}
		}
		field, value, found := strings.Cut(line, ":")
		if found && strings.HasPrefix(value, " ") {
			value = value[1:]
		}
		switch field {
		case "data":
			if hasData {
				data.WriteByte('\n')
			}
			if data.Len()+len(value) > MaxSSEEventBytes {
				return SSEEvent{}, ErrSSEEventTooLarge
			}
			data.WriteString(value)
			hasData = true
		case "event":
			ev.Event = value
		case "id":
			ev.ID = value
		default:
			// "retry" and unknown fields are ignored per the SSE spec.
		}
	}
	if err := r.sc.Err(); err != nil {
		return SSEEvent{}, err
	}
	if hasData || ev.Event != "" {
		ev.Data = data.String()
		return ev, nil
	}
	return SSEEvent{}, io.EOF
}
