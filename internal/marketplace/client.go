package marketplace

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client fetches and verifies marketplace artifacts from a remote registry.
//
// The registry base URL must use https unless WithInsecureHTTP is given.
// Every response body is size-capped, requests are bounded by timeouts, and
// plugin IDs / versions are validated and path-escaped before being placed in
// a URL so they cannot traverse to other registry paths.
type Client struct {
	baseURL       string
	base          *url.URL
	initErr       error
	httpClient    *http.Client
	trust         *TrustStore
	allowInsecure bool
	maxManifest   int64
	maxWASM       int64
}

// ClientOption customises a Client.
type ClientOption func(*Client)

// WithHTTPClient replaces the HTTP client (e.g. for custom TLS roots in tests).
// Its CheckRedirect is replaced to enforce the redirect policy.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *Client) {
		if hc != nil {
			cp := *hc
			c.httpClient = &cp
		}
	}
}

// WithTrustStore pins publisher keys: fetched manifests must be signed by a
// key trusted for their creator_id.
func WithTrustStore(t *TrustStore) ClientOption {
	return func(c *Client) { c.trust = t }
}

// WithInsecureHTTP allows a plain-http registry URL (development only).
func WithInsecureHTTP() ClientOption {
	return func(c *Client) { c.allowInsecure = true }
}

// WithMaxWASMBytes overrides the WASM download cap (default MaxWASMBytes).
func WithMaxWASMBytes(n int64) ClientOption {
	return func(c *Client) {
		if n > 0 {
			c.maxWASM = n
		}
	}
}

// NewClient creates a new marketplace client. URL problems are reported by
// Err and by every request method.
func NewClient(baseURL string, opts ...ClientOption) *Client {
	c := &Client{
		baseURL:     strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		maxManifest: MaxManifestBytes,
		maxWASM:     MaxWASMBytes,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(c)
		}
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: 30 * time.Second, Transport: defaultTransport()}
	}
	c.httpClient.CheckRedirect = c.checkRedirect
	c.base, c.initErr = c.parseBase(c.baseURL)
	return c
}

func defaultTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.TLSHandshakeTimeout = 10 * time.Second
	t.ResponseHeaderTimeout = 15 * time.Second
	t.MaxResponseHeaderBytes = 64 << 10
	if t.TLSClientConfig == nil {
		t.TLSClientConfig = &tls.Config{}
	}
	t.TLSClientConfig.MinVersion = tls.VersionTLS12
	t.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	return t
}

func (c *Client) parseBase(raw string) (*url.URL, error) {
	if raw == "" {
		return nil, errors.New("marketplace: registry URL is empty")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("marketplace: invalid registry URL: %w", err)
	}
	if u.Host == "" || u.Opaque != "" {
		return nil, errors.New("marketplace: registry URL must be absolute")
	}
	if u.User != nil {
		return nil, errors.New("marketplace: registry URL must not embed credentials")
	}
	if u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("marketplace: registry URL must not contain a query or fragment")
	}
	if err := c.checkScheme(u); err != nil {
		return nil, err
	}
	return u, nil
}

func (c *Client) checkScheme(u *url.URL) error {
	switch strings.ToLower(u.Scheme) {
	case "https":
		return nil
	case "http":
		if c.allowInsecure {
			return nil
		}
		return fmt.Errorf("marketplace: refusing insecure registry URL scheme %q (use https or WithInsecureHTTP)", u.Scheme)
	default:
		return fmt.Errorf("marketplace: unsupported registry URL scheme %q", u.Scheme)
	}
}

func (c *Client) checkRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 5 {
		return errors.New("marketplace: too many redirects")
	}
	// Never follow a redirect that downgrades to plain http.
	if err := c.checkScheme(req.URL); err != nil {
		return err
	}
	if len(via) > 0 && strings.EqualFold(via[0].URL.Scheme, "https") && !strings.EqualFold(req.URL.Scheme, "https") {
		return errors.New("marketplace: refusing https→http redirect")
	}
	return nil
}

// Err reports a configuration error (for example an invalid or insecure base URL).
func (c *Client) Err() error {
	if c == nil {
		return errors.New("marketplace: nil client")
	}
	return c.initErr
}

// BaseURL returns the configured registry base URL.
func (c *Client) BaseURL() string { return c.baseURL }

// endpoint builds base/segments... with every segment path-escaped.
func (c *Client) endpoint(segments ...string) (string, error) {
	if err := c.Err(); err != nil {
		return "", err
	}
	u := *c.base
	escaped := make([]string, 0, len(segments))
	for _, s := range segments {
		escaped = append(escaped, url.PathEscape(s))
	}
	rawPath := strings.TrimRight(u.EscapedPath(), "/") + "/" + strings.Join(escaped, "/")
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return "", err
	}
	u.Path, u.RawPath = path, rawPath
	return u.String(), nil
}

func (c *Client) get(ctx context.Context, endpoint string, limit int64) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4<<10))
		if resp.StatusCode == http.StatusNotFound {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("marketplace: registry returned %s", resp.Status)
	}
	if resp.ContentLength > limit {
		return nil, fmt.Errorf("marketplace: response of %d bytes exceeds limit %d", resp.ContentLength, limit)
	}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if n > limit {
		return nil, fmt.Errorf("marketplace: response exceeds limit of %d bytes", limit)
	}
	return buf.Bytes(), nil
}

// FetchManifest downloads a plugin manifest and verifies its Ed25519
// signature over the canonical encoding. The manifest must be for pluginID
// and, when a trust store is configured, signed by a key pinned for its creator.
func (c *Client) FetchManifest(ctx context.Context, pluginID string) (*VerifiedManifest, error) {
	if err := ValidatePluginID(pluginID); err != nil {
		return nil, err
	}
	endpoint, err := c.endpoint("plugins", pluginID, "manifest.json")
	if err != nil {
		return nil, err
	}
	body, err := c.get(ctx, endpoint, c.maxManifest)
	if err != nil {
		return nil, err
	}
	m, err := ParseAndVerifyManifest(body)
	if err != nil {
		return nil, err
	}
	// A registry (or an attacker in front of it) must not be able to answer
	// with a different, validly signed manifest.
	if m.ID != pluginID {
		return nil, fmt.Errorf("%w: registry returned manifest for %q, requested %q", ErrInvalidManifest, m.ID, pluginID)
	}
	if c.trust != nil {
		if err := c.trust.Check(m.CreatorID, m.PublicKey); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// DownloadWASM downloads the WASM bytes for a plugin (capped at the client's
// WASM size limit). Prefer DownloadVerifiedWASM, which also checks the hash.
func (c *Client) DownloadWASM(ctx context.Context, pluginID, version string) ([]byte, error) {
	if err := ValidatePluginID(pluginID); err != nil {
		return nil, err
	}
	if err := ValidateVersion(version); err != nil {
		return nil, err
	}
	endpoint, err := c.endpoint("plugins", pluginID, version, "plugin.wasm")
	if err != nil {
		return nil, err
	}
	return c.get(ctx, endpoint, c.maxWASM)
}

// DownloadVerifiedWASM downloads the module described by a verified manifest
// and checks it against the manifest's SHA-256 wasm_hash.
func (c *Client) DownloadVerifiedWASM(ctx context.Context, m *VerifiedManifest) ([]byte, error) {
	if m == nil {
		return nil, fmt.Errorf("%w: nil manifest", ErrInvalidManifest)
	}
	b, err := c.DownloadWASM(ctx, m.ID, m.Version)
	if err != nil {
		return nil, err
	}
	if err := m.VerifyWASM(b); err != nil {
		return nil, err
	}
	return b, nil
}

// VerifiedManifest represents a plugin manifest whose Ed25519 signature has
// been checked. Payload holds the canonical signed bytes (without the domain
// prefix) so the signature can be re-verified independently.
type VerifiedManifest struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Version   string `json:"version"`
	CreatorID string `json:"creator_id"`
	WASMHash  string `json:"wasm_hash"`
	Signature []byte `json:"signature"`
	PublicKey []byte `json:"public_key"`
	Payload   []byte `json:"payload"`
}

// ParseAndVerifyManifest strictly parses raw manifest JSON (at most
// MaxManifestBytes, no unknown fields) and verifies its Ed25519 signature over
// the canonical encoding. It rejects missing, malformed or invalid signatures.
func ParseAndVerifyManifest(payload []byte) (*VerifiedManifest, error) {
	if len(payload) > MaxManifestBytes {
		return nil, fmt.Errorf("%w: %w: exceeds %d bytes", ErrInvalidManifest, ErrTooLarge, MaxManifestBytes)
	}
	req, err := DecodePublishRequest(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	return VerifyPublishRequest(req)
}
