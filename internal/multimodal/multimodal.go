package multimodal

import (
	"context"
	"io"
	"mime/multipart"
	"net/http"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// TranscriptionService converts audio bytes to text.
type TranscriptionService interface {
	Transcribe(ctx context.Context, contentType string, audio []byte) (string, error)
}

// VisionService converts image bytes to text tokens/description.
type VisionService interface {
	Describe(ctx context.Context, contentType string, image []byte) (string, error)
}

// Preprocessor inspects multipart requests and injects extracted text into the LLM payload.
type Preprocessor struct {
	Transcriber TranscriptionService
	Vision      VisionService
	fileOpener  func(fh *multipart.FileHeader) (multipart.File, error)
}

// NewPreprocessor creates a new multimodal preprocessor.
func NewPreprocessor(t TranscriptionService, v VisionService) *Preprocessor {
	return &Preprocessor{Transcriber: t, Vision: v, fileOpener: defaultFileOpener}
}

func defaultFileOpener(fh *multipart.FileHeader) (multipart.File, error) {
	return fh.Open()
}

// ProcessRequest handles multipart form uploads and augments the request with extracted text.
// It will parse the multipart form automatically if not already parsed.
func (p *Preprocessor) ProcessRequest(ctx context.Context, req *models.LLMRequest, r *http.Request) error {
	if p == nil || req == nil || r == nil {
		return nil
	}
	if !strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
		return nil
	}
	if r.MultipartForm == nil {
		if err := r.ParseMultipartForm(1024 * 1024); err != nil {
			return err
		}
	}
	var appended []string
	if p.Transcriber != nil {
		for _, fhs := range r.MultipartForm.File {
			for _, fh := range fhs {
				ct := fh.Header.Get("Content-Type")
				if strings.HasPrefix(ct, "audio/") || strings.HasPrefix(ct, "video/") {
					f, err := fh.Open()
					if err != nil {
						return err
					}
					audio, err := io.ReadAll(f)
					f.Close()
					if err != nil {
						return err
					}
					text, err := p.Transcriber.Transcribe(ctx, ct, audio)
					if err == nil && strings.TrimSpace(text) != "" {
						appended = append(appended, "[transcript] "+text)
					}
				}
			}
		}
	}
	if p.Vision != nil {
		for _, fhs := range r.MultipartForm.File {
			for _, fh := range fhs {
				ct := fh.Header.Get("Content-Type")
				if strings.HasPrefix(ct, "image/") {
					f, err := fh.Open()
					if err != nil {
						return err
					}
					image, err := io.ReadAll(f)
					f.Close()
					if err != nil {
						return err
					}
					text, err := p.Vision.Describe(ctx, ct, image)
					if err == nil && strings.TrimSpace(text) != "" {
						appended = append(appended, "[vision] "+text)
					}
				}
			}
		}
	}
	if len(appended) == 0 {
		return nil
	}
	summary := strings.Join(appended, "\n")
	req.Messages = append([]models.Message{{
		Role:    models.RoleSystem,
		Content: &summary,
	}}, req.Messages...)
	return nil
}
