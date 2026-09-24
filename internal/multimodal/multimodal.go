package multimodal

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"sort"
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

// Default limits applied when the corresponding Preprocessor field is <= 0.
const (
	DefaultMaxBodyBytes   int64 = 32 << 20 // whole multipart body
	DefaultMaxFileBytes   int64 = 20 << 20 // any single uploaded file
	DefaultMaxFiles             = 8
	DefaultMaxMemoryBytes int64 = 1 << 20 // multipart parts kept in memory; rest spills to temp files
)

var (
	// ErrTooLarge is returned when the body or a file exceeds its limit.
	ErrTooLarge = errors.New("multimodal: upload too large")
	// ErrTooManyFiles is returned when more than MaxFiles files are uploaded.
	ErrTooManyFiles = errors.New("multimodal: too many files")
	// ErrUnsupportedMedia is returned for disallowed or mismatched content types.
	ErrUnsupportedMedia = errors.New("multimodal: unsupported media type")
)

// allowedImageTypes are the image types accepted; the uploaded bytes must be
// sniffed as the same type that was declared.
var allowedImageTypes = map[string]bool{
	"image/png":  true,
	"image/jpeg": true,
	"image/gif":  true,
	"image/webp": true,
}

// allowedAudioTypes are the audio/video types accepted for transcription.
// Many of these cannot be reliably sniffed, so the declared type is checked
// against the allowlist and the content is rejected only when it sniffs as
// something clearly different (text, HTML, images, archives...).
var allowedAudioTypes = map[string]bool{
	"audio/wav":       true,
	"audio/x-wav":     true,
	"audio/wave":      true,
	"audio/mpeg":      true,
	"audio/mp3":       true,
	"audio/mp4":       true,
	"audio/m4a":       true,
	"audio/x-m4a":     true,
	"audio/aac":       true,
	"audio/ogg":       true,
	"audio/opus":      true,
	"audio/webm":      true,
	"audio/flac":      true,
	"audio/x-flac":    true,
	"video/mp4":       true,
	"video/webm":      true,
	"video/mpeg":      true,
	"video/quicktime": true,
	"video/ogg":       true,
}

// Preprocessor inspects multipart requests and injects extracted text into the LLM payload.
type Preprocessor struct {
	Transcriber TranscriptionService
	Vision      VisionService

	// MaxBodyBytes caps the whole multipart body (default DefaultMaxBodyBytes).
	MaxBodyBytes int64
	// MaxFileBytes caps each uploaded file (default DefaultMaxFileBytes).
	MaxFileBytes int64
	// MaxFiles caps the number of uploaded files (default DefaultMaxFiles).
	MaxFiles int

	fileOpener func(fh *multipart.FileHeader) (multipart.File, error)
}

// NewPreprocessor creates a new multimodal preprocessor.
func NewPreprocessor(t TranscriptionService, v VisionService) *Preprocessor {
	return &Preprocessor{Transcriber: t, Vision: v, fileOpener: defaultFileOpener}
}

func defaultFileOpener(fh *multipart.FileHeader) (multipart.File, error) {
	return fh.Open()
}

func (p *Preprocessor) limits() (body, file int64, files int) {
	body, file, files = p.MaxBodyBytes, p.MaxFileBytes, p.MaxFiles
	if body <= 0 {
		body = DefaultMaxBodyBytes
	}
	if file <= 0 {
		file = DefaultMaxFileBytes
	}
	if files <= 0 {
		files = DefaultMaxFiles
	}
	return body, file, files
}

type upload struct {
	field string
	fh    *multipart.FileHeader
}

// ProcessRequest handles multipart form uploads and augments the request with
// extracted text. It parses the multipart form (with the body capped at
// MaxBodyBytes) if not already parsed, validates each file's size and content
// type, and runs audio/video through the Transcriber and images through the
// Vision service. Successful extractions are injected as one system message;
// failures of individual files are returned joined (errors.Join) after the
// successful results have been injected.
func (p *Preprocessor) ProcessRequest(ctx context.Context, req *models.LLMRequest, r *http.Request) error {
	if p == nil || req == nil || r == nil {
		return nil
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "multipart/form-data" {
		return nil
	}
	maxBody, maxFile, maxFiles := p.limits()
	if r.MultipartForm == nil {
		if r.ContentLength > maxBody {
			return fmt.Errorf("%w: body exceeds %d bytes", ErrTooLarge, maxBody)
		}
		// A nil ResponseWriter is safe for MaxBytesReader.
		r.Body = http.MaxBytesReader(nil, r.Body, maxBody)
		memory := DefaultMaxMemoryBytes
		if memory > maxBody {
			memory = maxBody
		}
		if err := r.ParseMultipartForm(memory); err != nil {
			var mbe *http.MaxBytesError
			if errors.As(err, &mbe) {
				return fmt.Errorf("%w: body exceeds %d bytes", ErrTooLarge, maxBody)
			}
			return fmt.Errorf("multimodal: parse multipart form: %w", err)
		}
	}
	if r.MultipartForm == nil {
		return nil
	}

	// Deterministic order: sort field names.
	fields := make([]string, 0, len(r.MultipartForm.File))
	for name := range r.MultipartForm.File {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	var uploads []upload
	for _, name := range fields {
		for _, fh := range r.MultipartForm.File[name] {
			uploads = append(uploads, upload{field: name, fh: fh})
		}
	}
	if len(uploads) > maxFiles {
		return fmt.Errorf("%w: %d files, limit %d", ErrTooManyFiles, len(uploads), maxFiles)
	}

	opener := p.fileOpener
	if opener == nil {
		opener = defaultFileOpener
	}
	var appended []string
	var errs []error
	for _, u := range uploads {
		if err := ctx.Err(); err != nil {
			return err
		}
		declared := declaredType(u.fh)
		isImage := strings.HasPrefix(declared, "image/")
		isAudio := strings.HasPrefix(declared, "audio/") || strings.HasPrefix(declared, "video/")
		if (isImage && p.Vision == nil) || (isAudio && p.Transcriber == nil) || (!isImage && !isAudio) {
			continue // nothing can process this file
		}
		if u.fh.Size > maxFile {
			errs = append(errs, fmt.Errorf("%w: file %q exceeds %d bytes", ErrTooLarge, u.fh.Filename, maxFile))
			continue
		}
		data, err := readLimited(opener, u.fh, maxFile)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if isImage {
			if err := validateImage(declared, data); err != nil {
				errs = append(errs, fmt.Errorf("file %q: %w", u.fh.Filename, err))
				continue
			}
			text, err := p.Vision.Describe(ctx, declared, data)
			if err != nil {
				errs = append(errs, fmt.Errorf("vision failed for %q: %w", u.fh.Filename, err))
				continue
			}
			if strings.TrimSpace(text) != "" {
				appended = append(appended, "[vision] "+text)
			}
			continue
		}
		if err := validateAudio(declared, data); err != nil {
			errs = append(errs, fmt.Errorf("file %q: %w", u.fh.Filename, err))
			continue
		}
		text, err := p.Transcriber.Transcribe(ctx, declared, data)
		if err != nil {
			errs = append(errs, fmt.Errorf("transcription failed for %q: %w", u.fh.Filename, err))
			continue
		}
		if strings.TrimSpace(text) != "" {
			appended = append(appended, "[transcript] "+text)
		}
	}
	if len(appended) > 0 {
		summary := strings.Join(appended, "\n")
		req.Messages = append([]models.Message{{
			Role:    models.RoleSystem,
			Content: &summary,
		}}, req.Messages...)
	}
	return errors.Join(errs...)
}

// declaredType returns the normalized media type declared for an uploaded part.
func declaredType(fh *multipart.FileHeader) string {
	mt, _, err := mime.ParseMediaType(fh.Header.Get("Content-Type"))
	if err != nil {
		return ""
	}
	return strings.ToLower(mt)
}

func readLimited(open func(*multipart.FileHeader) (multipart.File, error), fh *multipart.FileHeader, maxFile int64) ([]byte, error) {
	f, err := open(fh)
	if err != nil {
		return nil, fmt.Errorf("open %q: %w", fh.Filename, err)
	}
	defer f.Close()
	data, err := io.ReadAll(io.LimitReader(f, maxFile+1))
	if err != nil {
		return nil, fmt.Errorf("read %q: %w", fh.Filename, err)
	}
	if int64(len(data)) > maxFile {
		return nil, fmt.Errorf("%w: file %q exceeds %d bytes", ErrTooLarge, fh.Filename, maxFile)
	}
	return data, nil
}

// sniff returns the detected media type (without parameters).
func sniff(data []byte) string {
	mt, _, _ := mime.ParseMediaType(http.DetectContentType(data))
	return mt
}

func validateImage(declared string, data []byte) error {
	if !allowedImageTypes[declared] {
		return fmt.Errorf("%w: %s", ErrUnsupportedMedia, declared)
	}
	if sniffed := sniff(data); sniffed != declared {
		return fmt.Errorf("%w: declared %s but content is %s", ErrUnsupportedMedia, declared, sniffed)
	}
	return nil
}

func validateAudio(declared string, data []byte) error {
	if !allowedAudioTypes[declared] {
		return fmt.Errorf("%w: %s", ErrUnsupportedMedia, declared)
	}
	if len(data) == 0 {
		return fmt.Errorf("%w: empty file", ErrUnsupportedMedia)
	}
	sniffed := sniff(data)
	switch {
	case strings.HasPrefix(sniffed, "audio/"), strings.HasPrefix(sniffed, "video/"),
		sniffed == "application/octet-stream", sniffed == "application/ogg":
		return nil
	default:
		return fmt.Errorf("%w: declared %s but content is %s", ErrUnsupportedMedia, declared, sniffed)
	}
}

// DecodeDataURL decodes a base64 data URL ("data:<mime>;base64,<payload>")
// whose media type is an allowlisted image or audio/video type. The decoded
// size is checked against maxBytes BEFORE decoding, and image payloads must
// sniff as the declared type. maxBytes <= 0 uses DefaultMaxFileBytes.
func DecodeDataURL(s string, maxBytes int) (contentType string, data []byte, err error) {
	limit := int64(maxBytes)
	if limit <= 0 {
		limit = DefaultMaxFileBytes
	}
	const prefix = "data:"
	if len(s) < len(prefix) || !strings.EqualFold(s[:len(prefix)], prefix) {
		return "", nil, errors.New("multimodal: not a data URL")
	}
	rest := s[len(prefix):]
	comma := strings.IndexByte(rest, ',')
	if comma < 0 {
		return "", nil, errors.New("multimodal: malformed data URL")
	}
	meta, payload := rest[:comma], rest[comma+1:]
	parts := strings.Split(meta, ";")
	if len(parts) < 2 || !strings.EqualFold(strings.TrimSpace(parts[len(parts)-1]), "base64") {
		return "", nil, errors.New("multimodal: data URL must be base64-encoded")
	}
	mt, _, perr := mime.ParseMediaType(strings.Join(parts[:len(parts)-1], ";"))
	if perr != nil {
		return "", nil, fmt.Errorf("%w: invalid media type", ErrUnsupportedMedia)
	}
	mt = strings.ToLower(mt)
	if !allowedImageTypes[mt] && !allowedAudioTypes[mt] {
		return "", nil, fmt.Errorf("%w: %s", ErrUnsupportedMedia, mt)
	}
	// Reject oversized payloads before allocating/decoding.
	if int64(base64.StdEncoding.DecodedLen(len(payload))) > limit+3 {
		return "", nil, fmt.Errorf("%w: data URL exceeds %d bytes", ErrTooLarge, limit)
	}
	enc := base64.StdEncoding
	if !strings.HasSuffix(payload, "=") && len(payload)%4 != 0 {
		enc = base64.RawStdEncoding
	}
	data, err = enc.DecodeString(payload)
	if err != nil {
		return "", nil, fmt.Errorf("multimodal: invalid base64 payload: %w", err)
	}
	if int64(len(data)) > limit {
		return "", nil, fmt.Errorf("%w: data URL exceeds %d bytes", ErrTooLarge, limit)
	}
	if allowedImageTypes[mt] {
		if err := validateImage(mt, data); err != nil {
			return "", nil, err
		}
	} else if err := validateAudio(mt, data); err != nil {
		return "", nil, err
	}
	return mt, data, nil
}
