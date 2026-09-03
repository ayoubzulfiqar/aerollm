package universal

import (
	"bufio"
	"encoding/json"
	"io"
	"strings"
)

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func jsonUnmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}

// EventStreamReader reads line-delimited SSE-style payloads from an HTTP body.
type EventStreamReader struct {
	reader *bufio.Reader
}

// NewEventStreamReader creates a new reader.
func NewEventStreamReader(r io.Reader) *EventStreamReader {
	return &EventStreamReader{reader: bufio.NewReader(r)}
}

// ReadEvent returns the next non-empty event payload.
func (r *EventStreamReader) ReadEvent() (string, error) {
	for {
		line, err := r.reader.ReadString('\n')
		if err != nil {
			return "", err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "event:") {
			continue
		}
		if strings.HasPrefix(line, "data:") {
			return strings.TrimPrefix(line, "data:"), nil
		}
		return line, nil
	}
}
