package universal

import (
	"fmt"
)

// NewGeminiAdapter returns a Gemini provider adapter.
func NewGeminiAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("gemini", "google", apiKey, baseURL)
}

// NewBedrockAdapter returns an AWS Bedrock provider adapter.
func NewBedrockAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("bedrock", "aws", apiKey, baseURL)
}

// NewAzureOpenAIAdapter returns an Azure OpenAI provider adapter.
func NewAzureOpenAIAdapter(apiKey, baseURL, azureResource string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter(fmt.Sprintf("azure/%s", azureResource), "azure", apiKey, baseURL)
}

// NewGroqAdapter returns a Groq provider adapter.
func NewGroqAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("groq", "groq", apiKey, baseURL)
}

// NewCohereAdapter returns a Cohere provider adapter.
func NewCohereAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("cohere", "cohere", apiKey, baseURL)
}

// NewDeepSeekAdapter returns a DeepSeek provider adapter.
func NewDeepSeekAdapter(apiKey, baseURL string) *OpenAICompatibleAdapter {
	return NewOpenAICompatibleAdapter("deepseek", "deepseek", apiKey, baseURL)
}
