package universal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

// AWS event-stream framing (application/vnd.amazon.eventstream), as used by
// Bedrock's ConverseStream:
//
//	[total length u32][headers length u32][prelude CRC32 u32]
//	[headers ...][payload ...][message CRC32 u32]
//
// All integers are big-endian; both CRCs are CRC-32 (IEEE). The prelude CRC
// covers the first 8 bytes, the message CRC everything before it. Each
// header is [name length u8][name][value type u8][value].

const (
	esPreludeLen = 12
	esCRCLen     = 4
	// esMinMessageLen is a message with no headers and no payload.
	esMinMessageLen = esPreludeLen + esCRCLen
	// Limits mirror the AWS SDKs: 128 KiB of headers, 16 MiB of payload.
	esMaxHeadersLen = 128 << 10
	esMaxPayloadLen = 16 << 20
	esMaxMessageLen = esMinMessageLen + esMaxHeadersLen + esMaxPayloadLen
)

// Event-stream header value types.
const (
	esTypeBoolTrue  = 0
	esTypeBoolFalse = 1
	esTypeByte      = 2
	esTypeShort     = 3
	esTypeInt       = 4
	esTypeLong      = 5
	esTypeBytes     = 6
	esTypeString    = 7
	esTypeTimestamp = 8
	esTypeUUID      = 9
)

var (
	// errEventStreamCRC reports a prelude or message checksum mismatch.
	errEventStreamCRC = errors.New("event-stream: checksum mismatch")
	// errEventStreamTruncated reports a message cut off mid-frame.
	errEventStreamTruncated = errors.New("event-stream: truncated message")
	// errEventStreamFrame reports an invalid or oversized frame.
	errEventStreamFrame = errors.New("event-stream: invalid frame")
)

// esMessage is one decoded event-stream message. Header values are bool,
// int8, int16, int32, int64, []byte, string, time.Time or [16]byte.
type esMessage struct {
	Headers map[string]interface{}
	Payload []byte
}

// header returns a string header value ("" when absent or not a string).
func (m *esMessage) header(name string) string {
	s, _ := m.Headers[name].(string)
	return s
}

// eventStreamDecoder reads event-stream messages with bounded memory.
type eventStreamDecoder struct {
	r io.Reader
}

func newEventStreamDecoder(r io.Reader) *eventStreamDecoder {
	return &eventStreamDecoder{r: r}
}

// Next returns the next message. It returns io.EOF at a clean end of stream
// (between messages), errEventStreamTruncated when the stream ends inside a
// message, errEventStreamCRC on checksum mismatch and errEventStreamFrame for
// malformed or oversized frames. Other errors come from the reader.
func (d *eventStreamDecoder) Next() (*esMessage, error) {
	var prelude [esPreludeLen]byte
	if _, err := io.ReadFull(d.r, prelude[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errEventStreamTruncated
		}
		return nil, err // io.EOF: clean end
	}
	total := binary.BigEndian.Uint32(prelude[0:4])
	headersLen := binary.BigEndian.Uint32(prelude[4:8])
	if crc32.ChecksumIEEE(prelude[:8]) != binary.BigEndian.Uint32(prelude[8:12]) {
		return nil, fmt.Errorf("%w (prelude)", errEventStreamCRC)
	}
	if total < esMinMessageLen || total > esMaxMessageLen {
		return nil, fmt.Errorf("%w: message length %d", errEventStreamFrame, total)
	}
	if headersLen > esMaxHeadersLen || uint64(headersLen)+esMinMessageLen > uint64(total) {
		return nil, fmt.Errorf("%w: headers length %d", errEventStreamFrame, headersLen)
	}
	if payloadLen := total - headersLen - esMinMessageLen; payloadLen > esMaxPayloadLen {
		return nil, fmt.Errorf("%w: payload length %d", errEventStreamFrame, payloadLen)
	}
	rest := make([]byte, total-esPreludeLen)
	if _, err := io.ReadFull(d.r, rest); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, errEventStreamTruncated
		}
		return nil, err
	}
	body := rest[:len(rest)-esCRCLen]
	crc := crc32.Update(crc32.ChecksumIEEE(prelude[:]), crc32.IEEETable, body)
	if crc != binary.BigEndian.Uint32(rest[len(rest)-esCRCLen:]) {
		return nil, fmt.Errorf("%w (message)", errEventStreamCRC)
	}
	headers, err := parseEventStreamHeaders(body[:headersLen])
	if err != nil {
		return nil, err
	}
	return &esMessage{Headers: headers, Payload: body[headersLen:]}, nil
}

// parseEventStreamHeaders decodes a header block, checking every length.
func parseEventStreamHeaders(b []byte) (map[string]interface{}, error) {
	h := make(map[string]interface{})
	bad := func(what string) error { return fmt.Errorf("%w: %s", errEventStreamFrame, what) }
	for len(b) > 0 {
		nameLen := int(b[0])
		b = b[1:]
		if nameLen == 0 || len(b) < nameLen+1 {
			return nil, bad("header name")
		}
		name := string(b[:nameLen])
		typ := b[nameLen]
		b = b[nameLen+1:]
		need := func(n int) ([]byte, error) {
			if len(b) < n {
				return nil, bad("header " + name + " value")
			}
			v := b[:n]
			b = b[n:]
			return v, nil
		}
		var val interface{}
		switch typ {
		case esTypeBoolTrue:
			val = true
		case esTypeBoolFalse:
			val = false
		case esTypeByte:
			v, err := need(1)
			if err != nil {
				return nil, err
			}
			val = int8(v[0])
		case esTypeShort:
			v, err := need(2)
			if err != nil {
				return nil, err
			}
			val = int16(binary.BigEndian.Uint16(v))
		case esTypeInt:
			v, err := need(4)
			if err != nil {
				return nil, err
			}
			val = int32(binary.BigEndian.Uint32(v))
		case esTypeLong:
			v, err := need(8)
			if err != nil {
				return nil, err
			}
			val = int64(binary.BigEndian.Uint64(v))
		case esTypeBytes, esTypeString:
			l, err := need(2)
			if err != nil {
				return nil, err
			}
			v, err := need(int(binary.BigEndian.Uint16(l)))
			if err != nil {
				return nil, err
			}
			if typ == esTypeString {
				val = string(v)
			} else {
				val = append([]byte(nil), v...)
			}
		case esTypeTimestamp:
			v, err := need(8)
			if err != nil {
				return nil, err
			}
			val = time.UnixMilli(int64(binary.BigEndian.Uint64(v))).UTC()
		case esTypeUUID:
			v, err := need(16)
			if err != nil {
				return nil, err
			}
			var u [16]byte
			copy(u[:], v)
			val = u
		default:
			return nil, bad(fmt.Sprintf("header %s has unknown type %d", name, typ))
		}
		h[name] = val
	}
	return h, nil
}
