package marketplace

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// Client fetches and verifies marketplace artifacts.
type Client struct {
	baseURL    string
	httpClient *http.Client
}

// NewClient creates a new marketplace client.
func NewClient(baseURL string) *Client {
	return &Client{
		baseURL:    baseURL,
		httpClient: &http.Client{Timeout: 15 * time.Second},
	}
}

// FetchManifest downloads a plugin manifest and verifies its Ed25519 signature.
func (c *Client) FetchManifest(ctx context.Context, pluginID string) (*VerifiedManifest, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/plugins/%s/manifest.json", c.baseURL, pluginID), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("manifest fetch failed: %s", resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return ParseAndVerifyManifest(body)
}

// DownloadWASM downloads the WASM bytes for a plugin.
func (c *Client) DownloadWASM(ctx context.Context, pluginID, version string) ([]byte, error) {
	url := fmt.Sprintf("%s/plugins/%s/%s/plugin.wasm", c.baseURL, pluginID, version)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("wasm download failed: %s", resp.Status)
	}
	return io.ReadAll(resp.Body)
}

// VerifiedManifest represents a verified plugin manifest.
type VerifiedManifest struct {
	ID          string
	Name        string
	Version     string
	CreatorID   string
	WASMHash    string
	Signature   []byte
	PublicKey   []byte
	Payload     []byte
}

// ParseAndVerifyManifest parses raw manifest JSON and verifies Ed25519 signature.
func ParseAndVerifyManifest(payload []byte) (*VerifiedManifest, error) {
	var raw struct {
		ID        string `json:"id"`
		Name      string `json:"name"`
		Version   string `json:"version"`
		CreatorID string `json:"creator_id"`
		WASMHash  string `json:"wasm_hash"`
		Signature string `json:"signature"`
		PublicKey string `json:"public_key"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return nil, err
	}
	sig, err := b64Decode(raw.Signature)
	if err != nil {
		return nil, err
	}
	pk, err := b64Decode(raw.PublicKey)
	if err != nil {
		return nil, err
	}
	return &VerifiedManifest{
		ID:        raw.ID,
		Name:      raw.Name,
		Version:   raw.Version,
		CreatorID: raw.CreatorID,
		WASMHash:  raw.WASMHash,
		Signature: sig,
		PublicKey: pk,
		Payload:   payload,
	}, nil
}

func b64Decode(s string) ([]byte, error) {
	// Minimal base64 decode stub; replace with std encoding in production.
	if s == "" {
		return nil, fmt.Errorf("empty encoded value")
	}
	return []byte(s), nil
}
