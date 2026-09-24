package universal

import (
	"fmt"
	"strings"
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
