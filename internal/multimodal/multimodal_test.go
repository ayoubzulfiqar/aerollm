package multimodal

import (
	"bytes"
	"context"
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
	r := newMultipartRequest("image/png", "photo.png", []byte{0x89, 0x50})
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
