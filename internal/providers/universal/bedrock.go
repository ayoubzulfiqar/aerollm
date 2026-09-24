package universal

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// BedrockAdapter calls Amazon Bedrock's Converse API
// (POST /model/{modelId}/converse) and translates OpenAI-style requests and
// responses. Requests are authenticated either with AWS Signature V4 or with
// a Bedrock API key (bearer token); unsigned requests are never sent.
//
// Credentials (first match wins):
//   - apiKey "ACCESS_KEY_ID:SECRET_ACCESS_KEY[:SESSION_TOKEN]" → SigV4
//   - any other non-empty apiKey → Bedrock API key (Authorization: Bearer)
//   - AWS_BEARER_TOKEN_BEDROCK → Bedrock API key
//   - AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY [/ AWS_SESSION_TOKEN] → SigV4
//
// The region is taken from the endpoint host
// (bedrock-runtime.<region>.amazonaws.com), else Region, else AWS_REGION /
// AWS_DEFAULT_REGION. Streaming uses ConverseStream
// (POST /model/{modelId}/converse-stream), whose AWS event-stream framing is
// decoded natively (see StreamChatCompletions).
type BedrockAdapter struct {
	name   string
	apiKey string
	http   *http.Client

	base    *url.URL
	baseErr error

	mu sync.RWMutex
	// region overrides region detection when set (see SetRegion).
	region string
	now    func() time.Time

	health providers.HealthTracker
}

// NewBedrockAdapter returns an AWS Bedrock provider adapter. baseURL may be
// the runtime endpoint (https://bedrock-runtime.us-east-1.amazonaws.com); a
// control-plane host (bedrock.<region>.amazonaws.com) is rewritten to the
// runtime host.
func NewBedrockAdapter(apiKey, baseURL string) *BedrockAdapter {
	a := &BedrockAdapter{name: "bedrock", apiKey: apiKey, http: &http.Client{Timeout: DefaultTimeout}, now: time.Now}
	if strings.TrimSpace(baseURL) == "" {
		region := os.Getenv("AWS_REGION")
		if region == "" {
			region = os.Getenv("AWS_DEFAULT_REGION")
		}
		if region == "" {
			region = "us-east-1"
		}
		baseURL = "https://bedrock-runtime." + region + ".amazonaws.com"
	}
	a.base, a.baseErr = providers.NormalizeBaseURL(baseURL, "")
	if a.baseErr == nil {
		labels := strings.Split(a.base.Hostname(), ".")
		if len(labels) >= 4 && labels[0] == "bedrock" && strings.HasSuffix(a.base.Hostname(), ".amazonaws.com") {
			labels[0] = "bedrock-runtime"
			host := strings.Join(labels, ".")
			if p := a.base.Port(); p != "" {
				host += ":" + p
			}
			a.base.Host = host
		}
	}
	return a
}

// Name returns the adapter name.
func (a *BedrockAdapter) Name() string { return a.name }

// Type returns the adapter type.
func (a *BedrockAdapter) Type() string { return "aws" }

// SetRegion sets the signing region (overrides endpoint detection).
func (a *BedrockAdapter) SetRegion(region string) {
	a.mu.Lock()
	a.region = region
	a.mu.Unlock()
}

// SetHTTPClient replaces the HTTP client.
func (a *BedrockAdapter) SetHTTPClient(c *http.Client) {
	if c != nil {
		a.http = c
	}
}

// Region returns the signing region or "" when it cannot be determined.
func (a *BedrockAdapter) Region() string {
	a.mu.RLock()
	r := a.region
	a.mu.RUnlock()
	if r != "" {
		return r
	}
	if a.base != nil {
		labels := strings.Split(a.base.Hostname(), ".")
		for i, l := range labels {
			if (l == "bedrock-runtime" || l == "bedrock-runtime-fips" || l == "bedrock") && i+1 < len(labels) && labels[i+1] != "amazonaws" {
				return labels[i+1]
			}
		}
	}
	if r = os.Getenv("AWS_REGION"); r != "" {
		return r
	}
	return os.Getenv("AWS_DEFAULT_REGION")
}

type bedrockAuth struct {
	bearer string
	creds  *awsCredentials
}

// ErrBedrockNoCredentials is returned when no AWS credentials are available.
var ErrBedrockNoCredentials = errors.New("bedrock: no AWS credentials configured (set api_key to ACCESS_KEY_ID:SECRET_ACCESS_KEY[:SESSION_TOKEN] or a Bedrock API key, or the AWS_* environment variables)")

func (a *BedrockAdapter) auth() (bedrockAuth, error) {
	key := strings.TrimSpace(a.apiKey)
	if key != "" {
		if strings.Contains(key, ":") {
			parts := strings.SplitN(key, ":", 3)
			if parts[0] == "" || len(parts) < 2 || parts[1] == "" {
				return bedrockAuth{}, errors.New("bedrock: api_key must be ACCESS_KEY_ID:SECRET_ACCESS_KEY[:SESSION_TOKEN]")
			}
			c := &awsCredentials{AccessKeyID: parts[0], SecretAccessKey: parts[1]}
			if len(parts) == 3 {
				c.SessionToken = parts[2]
			}
			return bedrockAuth{creds: c}, nil
		}
		return bedrockAuth{bearer: key}, nil
	}
	if t := os.Getenv("AWS_BEARER_TOKEN_BEDROCK"); t != "" {
		return bedrockAuth{bearer: t}, nil
	}
	ak, sk := os.Getenv("AWS_ACCESS_KEY_ID"), os.Getenv("AWS_SECRET_ACCESS_KEY")
	if ak != "" && sk != "" {
		return bedrockAuth{creds: &awsCredentials{AccessKeyID: ak, SecretAccessKey: sk, SessionToken: os.Getenv("AWS_SESSION_TOKEN")}}, nil
	}
	return bedrockAuth{}, ErrBedrockNoCredentials
}

// ---- Converse wire types

type converseRequest struct {
	Messages        []converseMessage   `json:"messages"`
	System          []converseContent   `json:"system,omitempty"`
	InferenceConfig *converseInference  `json:"inferenceConfig,omitempty"`
	ToolConfig      *converseToolConfig `json:"toolConfig,omitempty"`
}

type converseMessage struct {
	Role    string            `json:"role"`
	Content []converseContent `json:"content"`
}

type converseContent struct {
	Text       string              `json:"text,omitempty"`
	Image      *converseImage      `json:"image,omitempty"`
	ToolUse    *converseToolUse    `json:"toolUse,omitempty"`
	ToolResult *converseToolResult `json:"toolResult,omitempty"`
}

type converseImage struct {
	Format string `json:"format"`
	Source struct {
		Bytes string `json:"bytes"`
	} `json:"source"`
}

type converseToolUse struct {
	ToolUseID string          `json:"toolUseId"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
}

type converseToolResult struct {
	ToolUseID string            `json:"toolUseId"`
	Content   []converseContent `json:"content"`
}

type converseInference struct {
	MaxTokens     *int     `json:"maxTokens,omitempty"`
	Temperature   *float64 `json:"temperature,omitempty"`
	TopP          *float64 `json:"topP,omitempty"`
	StopSequences []string `json:"stopSequences,omitempty"`
}

type converseToolConfig struct {
	Tools      []converseTool         `json:"tools"`
	ToolChoice map[string]interface{} `json:"toolChoice,omitempty"`
}

type converseTool struct {
	ToolSpec struct {
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
		InputSchema struct {
			JSON map[string]interface{} `json:"json"`
		} `json:"inputSchema"`
	} `json:"toolSpec"`
}

type converseResponse struct {
	Output struct {
		Message converseMessage `json:"message"`
	} `json:"output"`
	StopReason string        `json:"stopReason"`
	Usage      converseUsage `json:"usage"`
}

type converseUsage struct {
	InputTokens          int `json:"inputTokens"`
	OutputTokens         int `json:"outputTokens"`
	TotalTokens          int `json:"totalTokens"`
	CacheReadInputTokens int `json:"cacheReadInputTokens"`
}

// toUsage converts Converse usage to OpenAI-style usage.
func (u converseUsage) toUsage() *models.Usage {
	total := u.TotalTokens
	if total == 0 {
		total = u.InputTokens + u.OutputTokens
	}
	usage := &models.Usage{PromptTokens: u.InputTokens, CompletionTokens: u.OutputTokens, TotalTokens: total}
	if u.CacheReadInputTokens > 0 {
		usage.PromptTokensDetails = &models.PromptTokensDetails{CachedTokens: u.CacheReadInputTokens}
	}
	return usage
}

func (a *BedrockAdapter) badRequest(format string, args ...interface{}) error {
	return &providers.UpstreamError{Provider: a.name, StatusCode: http.StatusBadRequest, Type: "invalid_request_error", Message: fmt.Sprintf(format, args...)}
}

func imageFormat(mediaType string) string {
	switch strings.TrimPrefix(strings.ToLower(mediaType), "image/") {
	case "png":
		return "png"
	case "jpeg", "jpg":
		return "jpeg"
	case "gif":
		return "gif"
	case "webp":
		return "webp"
	}
	return ""
}

func (a *BedrockAdapter) buildConverse(req *models.LLMRequest) (*converseRequest, error) {
	if req.N != nil && *req.N > 1 {
		return nil, a.badRequest("n > 1 is not supported by bedrock")
	}
	out := &converseRequest{}
	inf := &converseInference{MaxTokens: req.EffectiveMaxTokens(), Temperature: req.Temperature, TopP: req.TopP, StopSequences: req.Stop}
	if inf.MaxTokens != nil || inf.Temperature != nil || inf.TopP != nil || len(inf.StopSequences) > 0 {
		out.InferenceConfig = inf
	}
	for _, m := range req.Messages {
		var blocks []converseContent
		switch m.Role {
		case models.RoleSystem, models.RoleDeveloper:
			if t := m.TextContent(); t != "" {
				out.System = append(out.System, converseContent{Text: t})
			}
			continue
		case models.RoleTool:
			if m.ToolCallID == nil || *m.ToolCallID == "" {
				return nil, a.badRequest("tool messages require tool_call_id")
			}
			text := m.TextContent()
			if text == "" && m.ToolResult != nil {
				text = *m.ToolResult
			}
			if text == "" {
				text = " "
			}
			blocks = append(blocks, converseContent{ToolResult: &converseToolResult{ToolUseID: *m.ToolCallID, Content: []converseContent{{Text: text}}}})
		case models.RoleUser, models.RoleAssistant:
			if parts := m.EffectiveContentParts(); len(parts) > 0 {
				for _, p := range parts {
					switch p.Type {
					case models.ContentPartText:
						if p.Text != "" {
							blocks = append(blocks, converseContent{Text: p.Text})
						}
					case models.ContentPartImageURL:
						if p.ImageURL == nil {
							return nil, a.badRequest("image_url part has no url")
						}
						mt, data, ok := providers.ParseDataURL(p.ImageURL.URL)
						if !ok || imageFormat(mt) == "" {
							return nil, a.badRequest("bedrock images must be base64 data URLs (png, jpeg, gif or webp)")
						}
						img := &converseImage{Format: imageFormat(mt)}
						img.Source.Bytes = data
						blocks = append(blocks, converseContent{Image: img})
					default:
						return nil, a.badRequest("content part type %q is not supported by bedrock", p.Type)
					}
				}
			} else if t := m.TextContent(); t != "" {
				blocks = append(blocks, converseContent{Text: t})
			}
			for _, tc := range m.ToolCalls {
				input := json.RawMessage("{}")
				if s := strings.TrimSpace(tc.Function.Arguments); s != "" && json.Valid([]byte(s)) && strings.HasPrefix(s, "{") {
					input = json.RawMessage(s)
				}
				blocks = append(blocks, converseContent{ToolUse: &converseToolUse{ToolUseID: tc.ID, Name: tc.Function.Name, Input: input}})
			}
		default:
			return nil, a.badRequest("unsupported message role %q", m.Role)
		}
		if len(blocks) == 0 {
			continue
		}
		role := "user"
		if m.Role == models.RoleAssistant {
			role = "assistant"
		}
		if n := len(out.Messages); n > 0 && out.Messages[n-1].Role == role {
			out.Messages[n-1].Content = append(out.Messages[n-1].Content, blocks...)
			continue
		}
		out.Messages = append(out.Messages, converseMessage{Role: role, Content: blocks})
	}
	if len(out.Messages) == 0 {
		return nil, a.badRequest("at least one non-system message is required")
	}
	if len(req.Tools) > 0 {
		tc := &converseToolConfig{}
		for _, t := range req.Tools {
			if !t.IsFunction() || t.Name == "" {
				return nil, a.badRequest("only named function tools are supported by bedrock")
			}
			var ct converseTool
			ct.ToolSpec.Name = t.Name
			ct.ToolSpec.Description = t.Description
			ct.ToolSpec.InputSchema.JSON = t.Parameters
			if ct.ToolSpec.InputSchema.JSON == nil {
				ct.ToolSpec.InputSchema.JSON = map[string]interface{}{"type": "object", "properties": map[string]interface{}{}}
			}
			tc.Tools = append(tc.Tools, ct)
		}
		if c := req.ToolChoice; c != nil {
			switch {
			case c.Function != "":
				tc.ToolChoice = map[string]interface{}{"tool": map[string]string{"name": c.Function}}
			case c.Mode == models.ToolChoiceRequired:
				tc.ToolChoice = map[string]interface{}{"any": map[string]string{}}
			case c.Mode == models.ToolChoiceAuto:
				tc.ToolChoice = map[string]interface{}{"auto": map[string]string{}}
			}
			// "none" has no Converse equivalent; tools stay declared (they
			// are required when the history contains tool use).
		}
		out.ToolConfig = tc
	}
	return out, nil
}

func bedrockFinishReason(stop string) string {
	switch stop {
	case "max_tokens", "model_context_window_exceeded":
		return "length"
	case "tool_use":
		return "tool_calls"
	case "guardrail_intervened", "content_filtered":
		return "content_filter"
	default:
		return "stop"
	}
}

func (a *BedrockAdapter) toLLMResponse(model string, cr *converseResponse) *models.LLMResponse {
	msg := models.Message{Role: models.RoleAssistant}
	var text strings.Builder
	hasText := false
	for _, c := range cr.Output.Message.Content {
		if c.Text != "" {
			text.WriteString(c.Text)
			hasText = true
		}
		if c.ToolUse != nil {
			args := string(c.ToolUse.Input)
			if args == "" || args == "null" {
				args = "{}"
			}
			msg.ToolCalls = append(msg.ToolCalls, models.ToolCall{ID: c.ToolUse.ToolUseID, Type: "function", Function: models.ToolFunction{Name: c.ToolUse.Name, Arguments: args}})
		}
	}
	if hasText || len(msg.ToolCalls) == 0 {
		s := text.String()
		msg.Content = &s
	}
	usage := cr.Usage.toUsage()
	return &models.LLMResponse{
		ID:      "chatcmpl-" + models.GenerateTraceID()[:24],
		Object:  "chat.completion",
		Created: a.now().Unix(),
		Model:   model,
		Choices: []models.Choice{{Index: 0, Message: msg, FinishReason: bedrockFinishReason(cr.StopReason)}},
		Usage:   usage,
	}
}

// validBedrockModelID rejects model IDs that cannot be a Bedrock model or
// inference-profile ID/ARN before they are placed in the URL path.
func validBedrockModelID(model string) error {
	if strings.TrimSpace(model) == "" {
		return errors.New("model is required")
	}
	if len(model) > 2048 {
		return errors.New("model id is too long")
	}
	if model == "." || model == ".." {
		return errors.New("invalid model id")
	}
	for _, r := range model {
		if r < 0x21 || r == 0x7f || r > 0x7e {
			return errors.New("model id contains invalid characters")
		}
	}
	return nil
}

// converseURL builds the Converse URL with the model ID escaped as a single
// path segment (model IDs contain ':' and ARNs contain '/').
func (a *BedrockAdapter) converseURL(model string) *url.URL {
	return a.modelURL(model, "converse")
}

// modelURL builds /model/{modelId}/{action} with the model ID escaped as a
// single path segment.
func (a *BedrockAdapter) modelURL(model, action string) *url.URL {
	u := *a.base
	basePath := strings.TrimRight(u.Path, "/")
	escBase := strings.TrimRight(a.base.EscapedPath(), "/")
	u.Path = basePath + "/model/" + model + "/" + action
	u.RawPath = escBase + "/model/" + awsURIEncode(model) + "/" + action
	return &u
}

// newConverseRequest validates req and builds the authenticated (SigV4 or
// bearer) POST to /model/{modelId}/{action}.
func (a *BedrockAdapter) newConverseRequest(ctx context.Context, req *models.LLMRequest, action, accept string) (*http.Request, error) {
	if req == nil {
		return nil, errNilRequest
	}
	if a.baseErr != nil {
		return nil, configError(a.name, a.baseErr)
	}
	if err := validBedrockModelID(req.Model); err != nil {
		return nil, a.badRequest("%s", err.Error())
	}
	region := a.Region()
	if region == "" {
		return nil, configError(a.name, errors.New("bedrock: cannot determine AWS region; use a bedrock-runtime.<region>.amazonaws.com endpoint or set AWS_REGION"))
	}
	auth, err := a.auth()
	if err != nil {
		return nil, configError(a.name, err)
	}
	body, err := a.buildConverse(req)
	if err != nil {
		return nil, err
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("%s: marshal request: %w", a.name, err)
	}
	u := a.modelURL(req.Model, action)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("%s: build request: %w", a.name, err)
	}
	httpReq.URL = u
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", accept)
	if auth.creds != nil {
		signSigV4(httpReq, payload, *auth.creds, region, "bedrock", a.now())
	} else {
		httpReq.Header.Set("Authorization", "Bearer "+auth.bearer)
	}
	return httpReq, nil
}

// ChatCompletions calls the Converse API.
func (a *BedrockAdapter) ChatCompletions(ctx context.Context, req *models.LLMRequest) (*models.LLMResponse, error) {
	httpReq, err := a.newConverseRequest(ctx, req, "converse", "application/json")
	if err != nil {
		return nil, err
	}
	return timed(&a.health, func() (*models.LLMResponse, error) {
		var cr converseResponse
		if err := providers.DoRequest(a.http, a.name, httpReq, &cr); err != nil {
			return nil, err
		}
		return a.toLLMResponse(req.Model, &cr), nil
	})
}

// Stream streams in the legacy chunk format via ConverseStream. A stream
// that cannot start is reported as a single error chunk.
func (a *BedrockAdapter) Stream(ctx context.Context, req *models.LLMRequest) (<-chan AeroStreamChunk, error) {
	ch, err := a.StreamChatCompletions(ctx, req)
	if err != nil {
		return singleResponseStream(ctx, a.name, func() (*models.LLMResponse, error) { return nil, err }), nil
	}
	return toAeroStream(ctx, a.name, ch), nil
}

// Health returns the adapter's health derived from recent calls.
func (a *BedrockAdapter) Health() map[string]interface{} {
	cfgErr := a.baseErr
	if cfgErr == nil {
		if _, err := a.auth(); err != nil {
			cfgErr = err
		}
	}
	return healthMap(a.name, "aws", &a.health, cfgErr)
}

// Close releases idle connections.
func (a *BedrockAdapter) Close() error {
	a.http.CloseIdleConnections()
	return nil
}
