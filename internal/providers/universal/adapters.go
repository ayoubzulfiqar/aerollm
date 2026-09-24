package universal

import (
	"fmt"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/providers"
)

// NewGeminiAdapter returns a Gemini adapter using Google's OpenAI-compatible
// endpoint (https://generativelanguage.googleapis.com/v1beta/openai). A base
// URL without a path gets "/v1beta/openai" appended.
func NewGeminiAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("gemini", "google", apiKey, baseURL)
}

// NewAzureOpenAIAdapter returns an Azure OpenAI adapter using the v1 API
// (https://<resource>.openai.azure.com/openai/v1) with the api-key header.
// When baseURL is empty it is derived from azureResource. Requests must use
// the deployment name as the model.
func NewAzureOpenAIAdapter(apiKey, baseURL, azureResource string) *OpenAICompatibleAdapter {
	if strings.TrimSpace(baseURL) == "" && azureResource != "" {
		baseURL = "https://" + azureResource + ".openai.azure.com/openai/v1"
	}
	return NewOpenAICompatibleAdapter(fmt.Sprintf("azure/%s", azureResource), "azure", apiKey, baseURL)
}

// NewAzureAdapter returns an Azure OpenAI adapter named name (api-key
// header auth). The URL scheme follows baseURL and apiVersion:
//
//   - baseURL ending in "/openai/v1" (the v1 API): requests go to
//     {base}/chat/completions etc., with ?api-version= only when apiVersion is
//     set (e.g. "preview");
//   - baseURL containing "/openai/deployments/{name}": that deployment serves
//     every request; apiVersion is required;
//   - any other baseURL (typically https://<resource>.openai.azure.com) with
//     an apiVersion: deployment URLs
//     {root}/openai/deployments/{deployment}/chat/completions?api-version=...
//     where the deployment is the request's (upstream) model name, so a model
//     entry "gpt-4o=my-gpt4o-deployment" maps a public name to a deployment;
//   - otherwise (no apiVersion): the v1 API at {base}/openai/v1, as before.
//
// An "api-version" query parameter in baseURL is used when apiVersion is
// empty. Embeddings, image generation and audio transcription use the same
// deployment scheme; the Responses API and probes use
// {root}/openai/{responses|models}?api-version=... in deployment mode.
func NewAzureAdapter(name, apiKey, baseURL, apiVersion string) *OpenAICompatibleAdapter {
	a := NewOpenAICompatibleAdapter(name, "azure", apiKey, baseURL)
	if a.baseErr != nil {
		return a
	}
	raw, err := providers.NormalizeBaseURL(baseURL, "")
	if err != nil {
		a.baseErr = fmt.Errorf("invalid base URL for provider %s: %w", name, err)
		return a
	}
	apiVersion = strings.TrimSpace(apiVersion)
	q := raw.Query()
	if apiVersion == "" {
		apiVersion = strings.TrimSpace(q.Get("api-version"))
	}
	q.Del("api-version")
	raw.RawQuery = q.Encode()
	lower := strings.ToLower(raw.Path)
	const depMarker = "/openai/deployments/"
	switch {
	case strings.HasSuffix(lower, "/openai/v1"):
		a.base, a.apiVersion = raw, apiVersion
	case strings.Contains(lower, depMarker):
		i := strings.Index(lower, depMarker)
		dep, _, _ := strings.Cut(raw.Path[i+len(depMarker):], "/")
		if err := validAzureDeployment(dep); err != nil {
			a.baseErr = fmt.Errorf("provider %s: base URL: %w", name, err)
			return a
		}
		if apiVersion == "" {
			a.baseErr = fmt.Errorf("provider %s: azure deployment URLs require api_version", name)
			return a
		}
		raw.Path = raw.Path[:i]
		a.base, a.apiVersion, a.azureDeployments, a.azureDeployment = raw, apiVersion, true, dep
	case apiVersion != "":
		if strings.HasSuffix(lower, "/openai") {
			raw.Path = raw.Path[:len(raw.Path)-len("/openai")]
		}
		a.base, a.apiVersion, a.azureDeployments = raw, apiVersion, true
	}
	return a
}

// AzureDeploymentMode reports whether the adapter uses Azure OpenAI
// deployment URLs (see NewAzureAdapter).
func (a *OpenAICompatibleAdapter) AzureDeploymentMode() bool { return a.azureDeployments }

// NewGroqAdapter returns a Groq provider adapter.
func NewGroqAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	if strings.TrimSpace(baseURL) == "" {
		baseURL = "https://api.groq.com/openai/v1"
	}
	return NewOpenAICompatibleAdapter("groq", "groq", apiKey, baseURL)
}

// NewCohereAdapter returns a Cohere adapter using Cohere's OpenAI
// Compatibility API (https://api.cohere.com/compatibility/v1). A base URL
// without a path gets "/compatibility/v1" appended.
func NewCohereAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("cohere", "cohere", apiKey, baseURL)
}

// NewDeepSeekAdapter returns a DeepSeek provider adapter.
func NewDeepSeekAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("deepseek", "deepseek", apiKey, baseURL)
}

// NewAnthropicAdapter returns an adapter for Anthropic's native Messages API.
// (It previously returned an OpenAI-compatible adapter, which sent
// /v1/chat/completions requests with Bearer auth to Anthropic.)
func NewAnthropicAdapter(apiKey, baseURL string) *AnthropicAdapter {
	return NewAnthropicAdapterV2(apiKey, baseURL)
}
