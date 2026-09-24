package mcp

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
)

// CodeResourceNotFound is the MCP error code for an unknown resource URI.
const CodeResourceNotFound = -32002

// DefaultMaxResourceBytes caps the total size of one resources/read result.
const DefaultMaxResourceBytes = 8 << 20

// maxResourceURILen bounds URIs accepted by resources/read.
const maxResourceURILen = 4096

// ErrResourceNotFound is returned by a ResourceProvider for unknown URIs; the
// server maps it to the MCP "resource not found" error (-32002).
var ErrResourceNotFound = errors.New("mcp: resource not found")

// Resource describes a resource listed by resources/list.
type Resource struct {
	URI         string `json:"uri"`
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	MimeType    string `json:"mimeType,omitempty"`
	// Size is the raw content size in bytes, when known.
	Size int64 `json:"size,omitempty"`
}

// ResourceContents is one item of a resources/read result. Text content is
// sent as "text"; when Blob is non-nil it is sent base64-encoded as "blob"
// instead.
type ResourceContents struct {
	URI      string
	MimeType string
	Text     string
	Blob     []byte
}

// MarshalJSON implements json.Marshaler (exactly one of text/blob is sent).
func (c ResourceContents) MarshalJSON() ([]byte, error) {
	m := map[string]interface{}{"uri": c.URI}
	if c.MimeType != "" {
		m["mimeType"] = c.MimeType
	}
	if c.Blob != nil {
		m["blob"] = base64.StdEncoding.EncodeToString(c.Blob)
	} else {
		m["text"] = c.Text
	}
	return json.Marshal(m)
}

func (c ResourceContents) size() int {
	if c.Blob != nil {
		return base64.StdEncoding.EncodedLen(len(c.Blob))
	}
	return len(c.Text)
}

// ResourceProvider backs the MCP resources capability. Implementations must
// be safe for concurrent use and should honour ctx cancellation. The request
// context carries the gateway principal and SessionIDFromContext, so
// providers can scope what a caller may see.
type ResourceProvider interface {
	// ListResources returns the resources available to the caller. The
	// server paginates the result.
	ListResources(ctx context.Context) ([]Resource, error)
	// ReadResource returns the contents of uri, or an error wrapping
	// ErrResourceNotFound when it does not exist (or is not visible).
	ReadResource(ctx context.Context, uri string) ([]ResourceContents, error)
}

// StaticResourceProvider is a concurrency-safe in-memory ResourceProvider.
type StaticResourceProvider struct {
	mu        sync.RWMutex
	resources map[string]staticResource
}

type staticResource struct {
	res      Resource
	contents ResourceContents
}

// NewStaticResourceProvider returns an empty StaticResourceProvider.
func NewStaticResourceProvider() *StaticResourceProvider {
	return &StaticResourceProvider{resources: make(map[string]staticResource)}
}

// AddText registers (or replaces) a text resource. The URI must be absolute
// (have a scheme); an empty Name defaults to the URI.
func (p *StaticResourceProvider) AddText(res Resource, text string) error {
	return p.add(res, ResourceContents{URI: res.URI, MimeType: res.MimeType, Text: text}, int64(len(text)))
}

// AddBlob registers (or replaces) a binary resource.
func (p *StaticResourceProvider) AddBlob(res Resource, data []byte) error {
	if data == nil {
		data = []byte{}
	}
	return p.add(res, ResourceContents{URI: res.URI, MimeType: res.MimeType, Blob: append([]byte(nil), data...)}, int64(len(data)))
}

func (p *StaticResourceProvider) add(res Resource, c ResourceContents, size int64) error {
	if err := validateResourceURI(res.URI); err != nil {
		return err
	}
	if strings.TrimSpace(res.Name) == "" {
		res.Name = res.URI
	}
	res.Size = size
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.resources == nil {
		p.resources = make(map[string]staticResource)
	}
	p.resources[res.URI] = staticResource{res: res, contents: c}
	return nil
}

// Remove unregisters a resource and reports whether it existed.
func (p *StaticResourceProvider) Remove(uri string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.resources[uri]
	delete(p.resources, uri)
	return ok
}

// ListResources implements ResourceProvider (sorted by URI).
func (p *StaticResourceProvider) ListResources(context.Context) ([]Resource, error) {
	p.mu.RLock()
	out := make([]Resource, 0, len(p.resources))
	for _, r := range p.resources {
		out = append(out, r.res)
	}
	p.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].URI < out[j].URI })
	return out, nil
}

// ReadResource implements ResourceProvider.
func (p *StaticResourceProvider) ReadResource(_ context.Context, uri string) ([]ResourceContents, error) {
	p.mu.RLock()
	r, ok := p.resources[uri]
	p.mu.RUnlock()
	if !ok {
		return nil, ErrResourceNotFound
	}
	c := r.contents
	if c.Blob != nil {
		c.Blob = append([]byte(nil), c.Blob...)
	}
	return []ResourceContents{c}, nil
}

func validateResourceURI(uri string) error {
	if uri == "" || len(uri) > maxResourceURILen {
		return fmt.Errorf("mcp: resource URI must be 1-%d bytes", maxResourceURILen)
	}
	u, err := url.Parse(uri)
	if err != nil || u.Scheme == "" {
		return fmt.Errorf("mcp: resource URI %q must be absolute", TruncateString(uri, 128))
	}
	return nil
}

// listResources handles resources/list.
func (s *Server) listResources(ctx context.Context, params json.RawMessage) (interface{}, *rpcError) {
	var p struct {
		Cursor *string `json:"cursor"`
	}
	if e := decodeParams(params, &p, false); e != nil {
		return nil, e
	}
	var list []Resource
	err := s.callProvider(ctx, func(ctx context.Context) error {
		var err error
		list, err = s.resources.ListResources(ctx)
		return err
	})
	if err != nil {
		return nil, &rpcError{Code: CodeInternalError, Message: "failed to list resources"}
	}
	start, end, next, perr := paginate(len(list), p.Cursor, s.pageSize())
	if perr != nil {
		return nil, perr
	}
	page := make([]Resource, 0, end-start)
	for _, r := range list[start:end] {
		if r.Name == "" {
			r.Name = r.URI
		}
		page = append(page, r)
	}
	out := map[string]interface{}{"resources": page}
	if next != "" {
		out["nextCursor"] = next
	}
	return out, nil
}

// readResource handles resources/read.
func (s *Server) readResource(ctx context.Context, params json.RawMessage) (interface{}, *rpcError) {
	var p struct {
		URI *string `json:"uri"`
	}
	if e := decodeParams(params, &p, true); e != nil {
		return nil, e
	}
	if p.URI == nil || *p.URI == "" {
		return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: uri is required"}
	}
	uri := *p.URI
	if len(uri) > maxResourceURILen {
		return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: uri too long"}
	}
	var contents []ResourceContents
	err := s.callProvider(ctx, func(ctx context.Context) error {
		var err error
		contents, err = s.resources.ReadResource(ctx, uri)
		return err
	})
	if errors.Is(err, ErrResourceNotFound) {
		return nil, &rpcError{Code: CodeResourceNotFound, Message: "Resource not found", Data: map[string]string{"uri": uri}}
	}
	if err != nil {
		return nil, &rpcError{Code: CodeInternalError, Message: "failed to read resource"}
	}
	limit := s.MaxResourceBytes
	if limit <= 0 {
		limit = DefaultMaxResourceBytes
	}
	total := 0
	for i := range contents {
		if contents[i].URI == "" {
			contents[i].URI = uri
		}
		total += contents[i].size()
	}
	if total > limit {
		return nil, &rpcError{Code: CodeInternalError, Message: "resource too large"}
	}
	if contents == nil {
		contents = []ResourceContents{}
	}
	return map[string]interface{}{"contents": contents}, nil
}
