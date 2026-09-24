package multimodal

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

type fakeTranscriber struct {
	text string
	err  error
}

func (f *fakeTranscriber) Transcribe(ctx context.Context, contentType string, audio []byte) (string, error) {
	return f.text, f.err
}

type fakeVision struct {
	text string
	err  error
}

func (f *fakeVision) Describe(ctx context.Context, contentType string, image []byte) (string, error) {
	return f.text, f.err
}

func newMultipartRequest(contentType, filename string, content []byte) *http.Request {
	boundary := "formdata-boundary"
	var buf bytes.Buffer
	buf.WriteString("--" + boundary + "\r\n")
	buf.WriteString(`Content-Disposition: form-data; name="file"; filename="` + filename + `"` + "\r\n")
	buf.WriteString("Content-Type: " + contentType + "\r\n\r\n")
	buf.Write(content)
	buf.WriteString("\r\n--" + boundary + "--\r\n")
	req := httptest.NewRequest(http.MethodPost, "/", &buf)
	req.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	return req
}

func TestProcessRequestSkipsWhenNil(t *testing.T) {
	p := NewPreprocessor(nil, nil)
	req := &models.LLMRequest{Messages: []models.Message{}}
	if err := p.ProcessRequest(context.Background(), req, nil); err != nil {
		t.Fatalf("expected nil error for nil request/processor, got %v", err)
	}
	if len(req.Messages) != 0 {
		t.Fatalf("expected no messages injected, got %d", len(req.Messages))
	}
}

func TestProcessRequestInjectsAudioTranscript(t *testing.T) {
	p := NewPreprocessor(&fakeTranscriber{text: "hello world"}, nil)
	r := newMultipartRequest("audio/wav", "audio.wav", []byte{0x01, 0x02})
	req := &models.LLMRequest{Messages: []models.Message{}}
	if err := p.ProcessRequest(context.Background(), req, r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req.Messages) != 1 {
		t.Fatalf("expected 1 injected message, got %d", len(req.Messages))
	}
	if req.Messages[0].Role != models.RoleSystem {
		t.Fatalf("expected system role, got %s", req.Messages[0].Role)
	}
	if req.Messages[0].Content == nil || !strings.Contains(*req.Messages[0].Content, "hello world") {
		t.Fatalf("expected transcript in injected message, got %v", req.Messages[0].Content)
	}
}

func TestProcessRequestInjectsVisionDescription(t *testing.T) {
	p := NewPreprocessor(nil, &fakeVision{text: "a cat"})
	r := newMultipartRequest("image/png", "photo.png", pngBytes)
	req := &models.LLMRequest{Messages: []models.Message{}}
	if err := p.ProcessRequest(context.Background(), req, r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(req.Messages) != 1 {
		t.Fatalf("expected 1 injected message, got %d", len(req.Messages))
	}
	if req.Messages[0].Content == nil || !strings.Contains(*req.Messages[0].Content, "a cat") {
		t.Fatalf("expected vision description in injected message, got %v", req.Messages[0].Content)
	}
}

var pngBytes = []byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n', 0, 0, 0, 0x0d, 'I', 'H', 'D', 'R'}

func TestProcessRequestRejectsMismatchedImage(t *testing.T) {
	p := NewPreprocessor(nil, &fakeVision{text: "should not be called"})
	r := newMultipartRequest("image/png", "evil.png", []byte("<html><script>alert(1)</script></html>"))
	req := &models.LLMRequest{}
	err := p.ProcessRequest(context.Background(), req, r)
	if !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("expected ErrUnsupportedMedia, got %v", err)
	}
	if len(req.Messages) != 0 {
		t.Fatalf("nothing should be injected, got %+v", req.Messages)
	}
}

func TestProcessRequestRejectsDisallowedTypes(t *testing.T) {
	p := NewPreprocessor(&fakeTranscriber{text: "x"}, &fakeVision{text: "x"})
	for _, ct := range []string{"image/svg+xml", "audio/x-evil"} {
		r := newMultipartRequest(ct, "f", []byte("data"))
		if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); !errors.Is(err, ErrUnsupportedMedia) {
			t.Fatalf("%s: expected ErrUnsupportedMedia, got %v", ct, err)
		}
	}
	// Declared audio but content is HTML.
	r := newMultipartRequest("audio/wav", "a.wav", []byte("<html><body>hi</body></html>"))
	if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("expected ErrUnsupportedMedia for html audio, got %v", err)
	}
}

func TestProcessRequestFileAndBodyLimits(t *testing.T) {
	p := NewPreprocessor(&fakeTranscriber{text: "x"}, nil)
	p.MaxFileBytes = 4
	r := newMultipartRequest("audio/wav", "a.wav", []byte{1, 2, 3, 4, 5, 6})
	if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge for file, got %v", err)
	}

	p = NewPreprocessor(&fakeTranscriber{text: "x"}, nil)
	p.MaxBodyBytes = 64
	r = newMultipartRequest("audio/wav", "a.wav", bytes.Repeat([]byte{1}, 1024))
	if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge for body, got %v", err)
	}
	// Unknown ContentLength (chunked) must still be capped while reading.
	r = newMultipartRequest("audio/wav", "a.wav", bytes.Repeat([]byte{1}, 1024))
	r.ContentLength = -1
	if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge for streamed body, got %v", err)
	}
}

func TestProcessRequestTooManyFiles(t *testing.T) {
	boundary := "b"
	var buf bytes.Buffer
	for i := 0; i < 3; i++ {
		buf.WriteString("--" + boundary + "\r\nContent-Disposition: form-data; name=\"f\"; filename=\"a.wav\"\r\nContent-Type: audio/wav\r\n\r\n\x01\x02\r\n")
	}
	buf.WriteString("--" + boundary + "--\r\n")
	r := httptest.NewRequest(http.MethodPost, "/", &buf)
	r.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	p := NewPreprocessor(&fakeTranscriber{text: "x"}, nil)
	p.MaxFiles = 2
	if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); !errors.Is(err, ErrTooManyFiles) {
		t.Fatalf("expected ErrTooManyFiles, got %v", err)
	}
}

func TestProcessRequestReportsServiceErrorsButKeepsSuccesses(t *testing.T) {
	boundary := "b"
	var buf bytes.Buffer
	buf.WriteString("--" + boundary + "\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"a.wav\"\r\nContent-Type: audio/wav\r\n\r\n\x01\x02\r\n")
	buf.WriteString("--" + boundary + "\r\nContent-Disposition: form-data; name=\"image\"; filename=\"p.png\"\r\nContent-Type: image/png\r\n\r\n")
	buf.Write(pngBytes)
	buf.WriteString("\r\n--" + boundary + "--\r\n")
	r := httptest.NewRequest(http.MethodPost, "/", &buf)
	r.Header.Set("Content-Type", "multipart/form-data; boundary="+boundary)
	p := NewPreprocessor(&fakeTranscriber{err: errors.New("asr down")}, &fakeVision{text: "a dog"})
	req := &models.LLMRequest{}
	err := p.ProcessRequest(context.Background(), req, r)
	if err == nil || !strings.Contains(err.Error(), "asr down") {
		t.Fatalf("expected transcription error to be reported, got %v", err)
	}
	if len(req.Messages) != 1 || !strings.Contains(*req.Messages[0].Content, "a dog") {
		t.Fatalf("expected successful vision result injected, got %+v", req.Messages)
	}
}

func TestProcessRequestIgnoresNonMultipart(t *testing.T) {
	p := NewPreprocessor(&fakeTranscriber{text: "x"}, nil)
	r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{}`))
	r.Header.Set("Content-Type", "application/json")
	if err := p.ProcessRequest(context.Background(), &models.LLMRequest{}, r); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDecodeDataURL(t *testing.T) {
	enc := base64.StdEncoding.EncodeToString(pngBytes)
	ct, data, err := DecodeDataURL("data:image/png;base64,"+enc, 1024)
	if err != nil || ct != "image/png" || !bytes.Equal(data, pngBytes) {
		t.Fatalf("decode failed: %v %q", err, ct)
	}
	if _, _, err := DecodeDataURL("data:image/png;base64,"+enc, 4); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("expected ErrTooLarge, got %v", err)
	}
	if _, _, err := DecodeDataURL("data:text/html;base64,"+base64.StdEncoding.EncodeToString([]byte("<b>")), 1024); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("expected ErrUnsupportedMedia, got %v", err)
	}
	if _, _, err := DecodeDataURL("data:image/png;base64,"+base64.StdEncoding.EncodeToString([]byte("GIF89a....")), 1024); !errors.Is(err, ErrUnsupportedMedia) {
		t.Fatalf("expected mismatch error, got %v", err)
	}
	for _, bad := range []string{"http://x/y.png", "data:image/png,rawbytes", "data:image/png;base64", "data:image/png;base64,!!!!"} {
		if _, _, err := DecodeDataURL(bad, 1024); err == nil {
			t.Fatalf("expected error for %q", bad)
		}
	}
}
