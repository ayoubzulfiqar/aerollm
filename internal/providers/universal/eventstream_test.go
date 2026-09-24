package universal

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"hash/crc32"
	"io"
	"strings"
	"testing"
	"time"
)

// esHeader is a test header: typ is an event-stream value type and val the
// raw encoded value (without the type byte).
type esHeader struct {
	name string
	typ  byte
	val  []byte
}

func esString(name, v string) esHeader {
	b := make([]byte, 2+len(v))
	binary.BigEndian.PutUint16(b, uint16(len(v)))
	copy(b[2:], v)
	return esHeader{name: name, typ: esTypeString, val: b}
}

// encodeES builds one event-stream message with valid CRCs.
func encodeES(headers []esHeader, payload []byte) []byte {
	var hb bytes.Buffer
	for _, h := range headers {
		hb.WriteByte(byte(len(h.name)))
		hb.WriteString(h.name)
		hb.WriteByte(h.typ)
		hb.Write(h.val)
	}
	total := esPreludeLen + hb.Len() + len(payload) + esCRCLen
	msg := make([]byte, 0, total)
	msg = binary.BigEndian.AppendUint32(msg, uint32(total))
	msg = binary.BigEndian.AppendUint32(msg, uint32(hb.Len()))
	msg = binary.BigEndian.AppendUint32(msg, crc32.ChecksumIEEE(msg[:8]))
	msg = append(msg, hb.Bytes()...)
	msg = append(msg, payload...)
	return binary.BigEndian.AppendUint32(msg, crc32.ChecksumIEEE(msg))
}

// esEvent builds a ConverseStream event message.
func esEvent(eventType, payload string) []byte {
	return encodeES([]esHeader{
		esString(":message-type", "event"),
		esString(":event-type", eventType),
		esString(":content-type", "application/json"),
	}, []byte(payload))
}

// esException builds a ConverseStream exception message.
func esException(exceptionType, payload string) []byte {
	return encodeES([]esHeader{
		esString(":message-type", "exception"),
		esString(":exception-type", exceptionType),
		esString(":content-type", "application/json"),
	}, []byte(payload))
}

func TestEventStreamDecodesAllHeaderTypes(t *testing.T) {
	ts := time.Date(2026, 9, 24, 10, 0, 0, 0, time.UTC)
	u16 := func(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }
	u32 := func(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
	u64 := func(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
	uuid := bytes.Repeat([]byte{0xAB}, 16)
	raw := encodeES([]esHeader{
		{"t", esTypeBoolTrue, nil},
		{"f", esTypeBoolFalse, nil},
		{"b", esTypeByte, []byte{0xFF}},
		{"s", esTypeShort, u16(0xFFFE)},
		{"i", esTypeInt, u32(7)},
		{"l", esTypeLong, u64(1 << 40)},
		{"ba", esTypeBytes, append(u16(3), 1, 2, 3)},
		esString("str", "hello"),
		{"ts", esTypeTimestamp, u64(uint64(ts.UnixMilli()))},
		{"u", esTypeUUID, uuid},
	}, []byte(`{"x":1}`))
	raw = append(raw, esEvent("messageStart", `{}`)...)

	d := newEventStreamDecoder(bytes.NewReader(raw))
	m, err := d.Next()
	if err != nil {
		t.Fatal(err)
	}
	h := m.Headers
	if h["t"] != true || h["f"] != false || h["b"] != int8(-1) || h["s"] != int16(-2) || h["i"] != int32(7) || h["l"] != int64(1<<40) {
		t.Fatalf("scalar headers: %#v", h)
	}
	if !bytes.Equal(h["ba"].([]byte), []byte{1, 2, 3}) || m.header("str") != "hello" || !h["ts"].(time.Time).Equal(ts) {
		t.Fatalf("variable headers: %#v", h)
	}
	if u := h["u"].([16]byte); !bytes.Equal(u[:], uuid) {
		t.Fatalf("uuid: %v", u)
	}
	if string(m.Payload) != `{"x":1}` {
		t.Fatalf("payload: %q", m.Payload)
	}
	if m, err = d.Next(); err != nil || m.header(":event-type") != "messageStart" {
		t.Fatalf("second message: %v %v", m, err)
	}
	if _, err = d.Next(); err != io.EOF {
		t.Fatalf("want clean EOF, got %v", err)
	}
}

func TestEventStreamRejectsCorruptFrames(t *testing.T) {
	good := esEvent("messageStart", `{"role":"assistant"}`)
	mutate := func(f func(b []byte) []byte) []byte {
		return f(append([]byte(nil), good...))
	}
	cases := []struct {
		name string
		raw  []byte
		want error
	}{
		{"prelude crc", mutate(func(b []byte) []byte { b[8] ^= 0xFF; return b }), errEventStreamCRC},
		{"message crc", mutate(func(b []byte) []byte { b[len(b)-1] ^= 0xFF; return b }), errEventStreamCRC},
		{"payload bit flip", mutate(func(b []byte) []byte { b[len(b)-6] ^= 0x01; return b }), errEventStreamCRC},
		{"truncated prelude", good[:7], errEventStreamTruncated},
		{"truncated body", good[:len(good)-3], errEventStreamTruncated},
		{"oversized", func() []byte {
			b := binary.BigEndian.AppendUint32(nil, esMaxMessageLen+1)
			b = binary.BigEndian.AppendUint32(b, 0)
			return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
		}(), errEventStreamFrame},
		{"too short", func() []byte {
			b := binary.BigEndian.AppendUint32(nil, 8)
			b = binary.BigEndian.AppendUint32(b, 0)
			return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
		}(), errEventStreamFrame},
		{"headers exceed message", func() []byte {
			b := binary.BigEndian.AppendUint32(nil, 32)
			b = binary.BigEndian.AppendUint32(b, 64)
			return binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
		}(), errEventStreamFrame},
		{"bad header type", encodeES([]esHeader{{"x", 42, nil}}, nil), errEventStreamFrame},
		{"header value overrun", encodeES([]esHeader{{"x", esTypeString, []byte{0, 50, 'a'}}}, nil), errEventStreamFrame},
		{"empty header name", encodeES([]esHeader{{"", esTypeBoolTrue, nil}}, nil), errEventStreamFrame},
	}
	for _, c := range cases {
		_, err := newEventStreamDecoder(bytes.NewReader(c.raw)).Next()
		if !errors.Is(err, c.want) {
			t.Errorf("%s: got %v, want %v", c.name, err, c.want)
		}
	}
	// A huge declared length must be rejected before allocating it.
	huge := binary.BigEndian.AppendUint32(nil, 0xFFFFFFFF)
	huge = binary.BigEndian.AppendUint32(huge, 0)
	huge = binary.BigEndian.AppendUint32(huge, crc32.ChecksumIEEE(huge))
	if _, err := newEventStreamDecoder(io.MultiReader(bytes.NewReader(huge), strings.NewReader(strings.Repeat("x", 100)))).Next(); !errors.Is(err, errEventStreamFrame) {
		t.Fatalf("huge frame: %v", err)
	}
}

func TestEventStreamAWSSpecVector(t *testing.T) {
	// "empty_message" from the AWS event-stream specification test data.
	raw, _ := hex.DecodeString("000000100000000005c248eb7d98c8ff")
	m, err := newEventStreamDecoder(bytes.NewReader(raw)).Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Headers) != 0 || len(m.Payload) != 0 {
		t.Fatalf("empty message: %+v", m)
	}
	if !bytes.Equal(encodeES(nil, nil), raw) {
		t.Fatal("test encoder disagrees with the spec vector")
	}
}
