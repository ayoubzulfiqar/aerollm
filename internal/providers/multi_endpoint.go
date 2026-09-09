package providers

import (
	"context"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// MultiEndpointProvider extends providers.Provider with additional endpoints.
// Implement this interface on providers that support embeddings, images, audio,
// and OpenAI-compatible responses/messages routes.
type MultiEndpointProvider interface {
	Provider

	// Embeddings returns embedding vectors for the request.
	Embeddings(ctx context.Context, req *models.EmbeddingRequest) (*models.EmbeddingResponse, error)

	// ImageGenerations returns generated images for the request.
	ImageGenerations(ctx context.Context, req *models.ImageRequest) (*models.ImageResponse, error)

	// AudioTranscriptions returns transcriptions for audio inputs.
	AudioTranscriptions(ctx context.Context, req *models.AudioRequest) (*models.AudioResponse, error)

	// Responses handles OpenAI-compatible responses endpoints.
	Responses(ctx context.Context, req *models.ResponsesRequest) (*models.ResponsesResponse, error)
}
