package universal

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"net/url"
	"strings"
	"sync"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// MaxAudioBytes caps decoded audio uploads (OpenAI's limit is 25 MB).
const MaxAudioBytes = 25 << 20

// OpenAICompatibleAdapter is a reusable adapter for OpenAI-compatible APIs
// (OpenAI, Azure OpenAI v1, Groq, DeepSeek, Gemini's and Cohere's
// OpenAI-compatibility endpoints, vLLM, Ollama, ...).
type OpenAICompatibleAdapter struct {
	name         string
	providerType string
	cfg          AdapterConfig
	http         *http.Client

	base       *url.URL
	baseErr    error
	authHeader string // header carrying the key
	authPrefix string // prefix before the key (e.g. "Bearer ")

	mu                 sync.RWMutex
	includeStreamUsage bool

	// Azure OpenAI (see NewAzureAdapter). apiVersion is sent as
	// ?api-version=; in deployment mode base is the resource root and
	// requests go to /openai/deployments/{deployment}/...
	azureDeployments bool
	azureDeployment  string // fixed deployment taken from the base URL
	apiVersion       string

	health providers.HealthTracker
}

// defaultBaseURL returns the default endpoint for a provider type.
func defaultBaseURL(providerType string) string {
	switch providerType {
	case "groq":
		return "https://api.groq.com/openai/v1"
	case "deepseek":
		return "https://api.deepseek.com/v1"
	case "google", "gemini":
		return "https://generativelanguage.googleapis.com/v1beta/openai"
	case "cohere":
		return "https://api.cohere.com/compatibility/v1"
	case "azure":
		return ""
	default:
		return "https://api.openai.com/v1"
	}
}

// defaultAPIPath is appended to a configured base URL that has no path.
func defaultAPIPath(providerType string) string {
	switch providerType {
	case "google", "gemini":
		return "/v1beta/openai"
	case "cohere":
		return "/compatibility/v1"
	case "azure":
		return "/openai/v1"
	default:
		return "/v1"
	}
}

// NewOpenAICompatibleAdapter creates a new OpenAI-compatible adapter. baseURL
// may be given with or without its API path (e.g. "https://api.openai.com"
// or "https://api.openai.com/v1"); an empty baseURL uses the provider type's
// default endpoint.
func NewOpenAICompatibleAdapter(name, providerType, apiKey, baseURL string) *OpenAICompatibleAdapter {
	cfg := NewDefaultAdapterConfig(apiKey, baseURL)
	if strings.TrimSpace(baseURL) == "" {
		baseURL = defaultBaseURL(providerType)
	}
	base, err := providers.NormalizeBaseURL(baseURL, defaultAPIPath(providerType))
	if err != nil {
		err = fmt.Errorf("invalid base URL for provider %s: %w", name, err)
	}
	a := &OpenAICompatibleAdapter{
		name:               name,
		providerType:       providerType,
		cfg:                cfg,
		http:               cfg.HTTP,
		base:               base,
		baseErr:            err,
		authHeader:         "Authorization",
		authPrefix:         "Bearer ",
		includeStreamUsage: true,
	}
	switch providerType {
	case "azure":
		a.authHeader, a.authPrefix = "api-key", ""
	case "cohere":
		// Cohere's Compatibility API does not document stream_options, so
		// it is not sent (usage is still read from any chunk carrying it and
		// the gateway estimates it otherwise). Enable with
		// SetIncludeStreamUsage(true); an upstream that rejects the field
		// is retried without it.
		a.includeStreamUsage = false
	}
	// Gemini's OpenAI compatibility layer documents
	// stream_options.include_usage, so it keeps the default (true).
	return a
}

// Name returns the adapter name.
func (a *OpenAICompatibleAdapter) Name() string { return a.name }

// Type returns the adapter provider type as a string.
func (a *OpenAICompatibleAdapter) Type() string { return a.providerType }

// ProviderType returns the provider type.
func (a *OpenAICompatibleAdapter) ProviderType() providers.ProviderType {
	return providers.ProviderType(a.providerType)
}

// BaseURL returns the normalized API base URL ("" when misconfigured).
func (a *OpenAICompatibleAdapter) BaseURL() string {
	if a.base == nil {
		return ""
	}
	return a.base.String()
}

// SetHTTPClient replaces the HTTP client (e.g. to change timeouts).
func (a *OpenAICompatibleAdapter) SetHTTPClient(c *http.Client) {
	if c != nil {
		a.http = c
		a.cfg.HTTP = c
	}
}

// SetIncludeStreamUsage controls whether streams request a final usage
// chunk via stream_options.include_usage. When false, stream_options is not
// sent at all (not even when the client set it): the gateway synthesizes the
// usage chunk for clients that asked for one. It defaults to true except for
// Cohere, whose compatibility API does not document the field.
func (a *OpenAICompatibleAdapter) SetIncludeStreamUsage(v bool) {
	a.mu.Lock()
	a.includeStreamUsage = v
	a.mu.Unlock()
}

func (a *OpenAICompatibleAdapter) streamUsage() bool {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.includeStreamUsage
}

// IncludeStreamUsage reports whether streams request usage via
// stream_options.include_usage.
func (a *OpenAICompatibleAdapter) IncludeStreamUsage() bool { return a.streamUsage() }

// endpoint returns the URL for an operation that is not bound to a model
// deployment (e.g. /models, /responses).
func (a *OpenAICompatibleAdapter) endpoint(path string) (string, error) {
	if a.baseErr != nil {
		return "", configError(a.name, a.baseErr)
	}
	if a.azureDeployments {
		u := *a.base
		u.Path = strings.TrimRight(u.Path, "/") + "/openai/" + strings.TrimLeft(path, "/")
		u.RawPath = ""
		return withAPIVersion(&u, a.apiVersion), nil
	}
	ep := providers.JoinURL(a.base, path)
	if a.apiVersion == "" {
		return ep, nil
	}
	u, err := url.Parse(ep)
	if err != nil {
		return "", configError(a.name, err)
	}
	return withAPIVersion(u, a.apiVersion), nil
}

// modelEndpoint returns the URL for a model operation (chat, embeddings,
// images, audio). For Azure deployment URLs the deployment is model (the
// upstream model name after alias resolution) unless the base URL fixes one.
func (a *OpenAICompatibleAdapter) modelEndpoint(path, model string) (string, error) {
	if a.baseErr != nil {
		return "", configError(a.name, a.baseErr)
	}
	if !a.azureDeployments {
		return a.endpoint(path)
	}
	dep := a.azureDeployment
	if dep == "" {
		dep = strings.TrimSpace(model)
	}
	if err := validAzureDeployment(dep); err != nil {
		return "", &providers.UpstreamError{Provider: a.name, StatusCode: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()}
	}
	u := *a.base
	u.Path = strings.TrimRight(u.Path, "/") + "/openai/deployments/" + dep + "/" + strings.TrimLeft(path, "/")
	u.RawPath = ""
	return withAPIVersion(&u, a.apiVersion), nil
}

// withAPIVersion sets ?api-version= on u when version is non-empty.
func withAPIVersion(u *url.URL, version string) string {
	if version != "" {
		q := u.Query()
		q.Set("api-version", version)
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// validAzureDeployment checks an Azure OpenAI deployment name before it is
// placed in the URL path (Azure allows letters, digits, '-', '_' and '.').
func validAzureDeployment(dep string) error {
	if dep == "" {
		return fmt.Errorf("model (the Azure OpenAI deployment name) is required")
	}
	if len(dep) > 256 || dep == "." || dep == ".." {
		return fmt.Errorf("invalid Azure OpenAI deployment name %q", truncateStr(dep, 64))
	}
	for i := 0; i < len(dep); i++ {
		c := dep[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
			return fmt.Errorf("invalid Azure OpenAI deployment name %q: only letters, digits, '-', '_' and '.' are allowed", truncateStr(dep, 64))
		}
	}
	return nil
}

func (a *OpenAICompatibleAdapter) headers() http.Header {
	h := http.Header{}
	if a.cfg.APIKey != "" {
		h.Set(a.authHeader, a.authPrefix+a.cfg.APIKey)
	}
	return h
}

// ChatCompletions sends a chat completion request. response_format (JSON
// mode / Structured Outputs), tools and the other OpenAI parameters are
// forwarded as-is.
func (a *OpenAICompatibleAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	if req == nil {
		return nil, errNilRequest
	}
	endpoint, err := a.modelEndpoint("/chat/completions", req.Model)
	if err != nil {
		return nil, err
	}
	return timed(&a.health, func() (*models.LLMResponse, error) {
		return providers.OpenAIChatCompletion(ctx, a.http, a.name, endpoint, a.headers(), providers.NewOpenAIChatRequest(req, "", false, false))
	})
}

// StreamChatCompletions implements providers.StreamingProvider. Usage is
// requested with stream_options.include_usage unless disabled (see
// SetIncludeStreamUsage); if the upstream rejects stream_options with a
// 400/422, the stream is retried once without it and the option is turned
// off for this adapter.
func (a *OpenAICompatibleAdapter) StreamChatCompletions(ctx context.Context, req *models.LLMRequest) (<-chan models.StreamChunk, error) {
	if req == nil {
		return nil, errNilRequest
	}
	endpoint, err := a.modelEndpoint("/chat/completions", req.Model)
	if err != nil {
		return nil, err
	}
	usage := a.streamUsage()
	body := providers.NewOpenAIChatRequest(req, "", true, usage)
	if !usage {
		body.StreamOptions = nil
	}
	ch, err := providers.OpenAIChatStream(ctx, a.http, a.name, endpoint, a.headers(), body, a.health.Observe)
	if err != nil && body.StreamOptions != nil && rejectsStreamOptions(err) && ctx.Err() == nil {
		a.SetIncludeStreamUsage(false)
		body.StreamOptions = nil
		return providers.OpenAIChatStream(ctx, a.http, a.name, endpoint, a.headers(), body, a.health.Observe)
	}
	return ch, err
}

// rejectsStreamOptions reports whether err is an upstream validation error
// about the stream_options field.
func rejectsStreamOptions(err error) bool {
	var ue *providers.UpstreamError
	if !errors.As(err, &ue) || (ue.StatusCode != http.StatusBadRequest && ue.StatusCode != http.StatusUnprocessableEntity) {
		return false
	}
	msg := strings.ToLower(ue.Message)
	return strings.Contains(msg, "stream_options") || strings.Contains(msg, "include_usage")
}

// Stream sends a streaming chat completion request (legacy chunk format).
func (a *OpenAICompatibleAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	ch, err := a.StreamChatCompletions(ctx, req)
	if err != nil {
		return nil, err
	}
	return toAeroStream(ctx, a.name, ch), nil
}

// Embeddings sends an embeddings request. String and array inputs are both
// supported; base64-encoded responses are decoded.
func (a *OpenAICompatibleAdapter) Embeddings(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error) {
	if req == nil {
		return nil, errNilRequest
	}
	endpoint, err := a.modelEndpoint("/embeddings", req.Model)
	if err != nil {
		return nil, err
	}
	return timed(&a.health, func() (*models.EmbeddingResponse, error) {
		var out models.EmbeddingResponse
		if err := providers.DoJSON(ctx, a.http, a.name, endpoint, a.headers(), req, &out); err != nil {
			return nil, err
		}
		return &out, nil
	})
}

// ImageGenerations sends an image generation request.
func (a *OpenAICompatibleAdapter) ImageGenerations(ctx context.Context, req *models.ImageRequest) (*models.ImageResponse, error) {
	if req == nil {
		return nil, errNilRequest
	}
	endpoint, err := a.modelEndpoint("/images/generations", req.Model)
	if err != nil {
		return nil, err
	}
	return timed(&a.health, func() (*models.ImageResponse, error) {
		var out models.ImageResponse
		if err := providers.DoJSON(ctx, a.http, a.name, endpoint, a.headers(), req, &out); err != nil {
			return nil, err
		}
		return &out, nil
	})
}

// decodeAudioFile accepts a data URL or raw base64 audio and returns the
// bytes plus a filename with a suitable extension.
func decodeAudioFile(s string) ([]byte, string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, "", fmt.Errorf("file is required")
	}
	name := "audio"
	ext := ""
	if mt, data, ok := providers.ParseDataURL(s); ok {
		s = data
		switch strings.TrimPrefix(mt, "audio/") {
		case "mpeg", "mp3":
			ext = ".mp3"
		case "wav", "x-wav", "wave":
			ext = ".wav"
		case "ogg":
			ext = ".ogg"
		case "webm":
			ext = ".webm"
		case "flac", "x-flac":
			ext = ".flac"
		case "mp4", "m4a", "x-m4a":
			ext = ".m4a"
		}
	}
	if base64.StdEncoding.DecodedLen(len(s)) > MaxAudioBytes+3 {
		return nil, "", fmt.Errorf("audio file exceeds %d bytes", MaxAudioBytes)
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, "", fmt.Errorf("file must be base64-encoded audio or a base64 data URL")
	}
	if len(b) > MaxAudioBytes {
		return nil, "", fmt.Errorf("audio file exceeds %d bytes", MaxAudioBytes)
	}
	if ext == "" {
		ext = sniffAudioExt(b)
	}
	return b, name + ext, nil
}

func sniffAudioExt(b []byte) string {
	switch {
	case len(b) >= 12 && string(b[:4]) == "RIFF" && string(b[8:12]) == "WAVE":
		return ".wav"
	case len(b) >= 4 && string(b[:4]) == "OggS":
		return ".ogg"
	case len(b) >= 4 && string(b[:4]) == "fLaC":
		return ".flac"
	case len(b) >= 3 && string(b[:3]) == "ID3", len(b) >= 2 && b[0] == 0xFF && b[1]&0xE0 == 0xE0:
		return ".mp3"
	case len(b) >= 4 && b[0] == 0x1A && b[1] == 0x45 && b[2] == 0xDF && b[3] == 0xA3:
		return ".webm"
	case len(b) >= 8 && string(b[4:8]) == "ftyp":
		return ".m4a"
	}
	return ".wav"
}

// AudioTranscriptions uploads audio (req.File: base64 or a base64 data URL)
// as multipart/form-data to /audio/transcriptions.
func (a *OpenAICompatibleAdapter) AudioTranscriptions(ctx context.Context, req *models.AudioRequest) (*models.AudioResponse, error) {
	if req == nil {
		return nil, errNilRequest
	}
	endpoint, err := a.modelEndpoint("/audio/transcriptions", req.Model)
	if err != nil {
		return nil, err
	}
	audio, filename, err := decodeAudioFile(req.File)
	if err != nil {
		return nil, &providers.UpstreamError{Provider: a.name, StatusCode: http.StatusBadRequest, Type: "invalid_request_error", Message: err.Error()}
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("model", req.Model)
	if req.Prompt != "" {
		_ = mw.WriteField("prompt", req.Prompt)
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Disposition", fmt.Sprintf(`form-data; name="file"; filename=%q`, filename))
	h.Set("Content-Type", "application/octet-stream")
	fw, err := mw.CreatePart(h)
	if err != nil {
		return nil, err
	}
	if _, err := fw.Write(audio); err != nil {
		return nil, err
	}
	if err := mw.Close(); err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, &buf)
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", a.name, err)
	}
	for k, vs := range a.headers() {
		httpReq.Header[k] = vs
	}
	httpReq.Header.Set("Content-Type", mw.FormDataContentType())
	httpReq.Header.Set("Accept", "application/json")
	return timed(&a.health, func() (*models.AudioResponse, error) {
		var out models.AudioResponse
		if err := providers.DoRequest(a.http, a.name, httpReq, &out); err != nil {
			return nil, err
		}
		return &out, nil
	})
}

// responsesTool renders a tool in the Responses API's flat format.
func responsesTool(t models.ToolDefinition) interface{} {
	if !t.IsFunction() {
		return t // non-function tools pass through verbatim
	}
	m := map[string]interface{}{"type": "function", "name": t.Name}
	if t.Description != "" {
		m["description"] = t.Description
	}
	if t.Parameters != nil {
		m["parameters"] = t.Parameters
	}
	if t.Strict != nil {
		m["strict"] = *t.Strict
	}
	return m
}

// Responses sends an OpenAI Responses API request. The upstream response is
// passed through losslessly.
func (a *OpenAICompatibleAdapter) Responses(ctx context.Context, req *models.ResponsesRequest) (*models.ResponsesResponse, error) {
	if req == nil {
		return nil, errNilRequest
	}
	endpoint, err := a.endpoint("/responses")
	if err != nil {
		return nil, err
	}
	payload := map[string]interface{}{"model": req.Model}
	if len(req.InputItems) > 0 {
		payload["input"] = req.InputItems
	} else {
		payload["input"] = req.Input
	}
	if req.Previous != nil {
		payload["previous_response_id"] = *req.Previous
	}
	if len(req.Tools) > 0 {
		tools := make([]interface{}, 0, len(req.Tools))
		for _, t := range req.Tools {
			tools = append(tools, responsesTool(t))
		}
		payload["tools"] = tools
	}
	return timed(&a.health, func() (*models.ResponsesResponse, error) {
		var out models.ResponsesResponse
		if err := providers.DoJSON(ctx, a.http, a.name, endpoint, a.headers(), payload, &out); err != nil {
			return nil, err
		}
		return &out, nil
	})
}

// Probe implements providers.Prober: an authenticated GET of the model list
// ({base}/models; /openai/models?api-version= for Azure deployment URLs),
// bounded by providers.DefaultProbeTimeout. Its outcome is reflected in
// Health until a later probe or successful call. It is opt-in: nothing
// probes unless the caller runs Probe (e.g. ProviderRegistry.ProbeAll).
func (a *OpenAICompatibleAdapter) Probe(ctx context.Context) error {
	return providers.RunProbe(&a.health, func() error {
		endpoint, err := a.endpoint("/models")
		if err != nil {
			return err
		}
		return providers.ProbeEndpoint(ctx, a.http, a.name, endpoint, a.headers())
	})
}

// Health returns the adapter's health derived from recent calls.
func (a *OpenAICompatibleAdapter) Health() map[string]interface{} {
	return healthMap(a.name, a.providerType, &a.health, a.baseErr)
}

// Close releases idle connections.
func (a *OpenAICompatibleAdapter) Close() error {
	if a.http != nil {
		a.http.CloseIdleConnections()
	}
	return nil
}
