package k8s

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/api/v1alpha1"
)

// Response size limits. List pages and single objects are bounded so a
// misbehaving API server (or an enormous collection) cannot exhaust memory.
const (
	MaxListPageBytes   = 64 << 20
	MaxObjectBytes     = 16 << 20
	MaxWatchEventBytes = 16 << 20
	maxErrorBodyBytes  = 64 << 10
	// MaxListItems bounds the total number of objects ListAll accumulates.
	MaxListItems = 100000

	defaultRequestTimeout = 30 * time.Second
	defaultPageSize       = 500
	// tokenRefreshInterval matches client-go: projected service-account
	// tokens are rotated on disk and must be re-read.
	tokenRefreshInterval = time.Minute
)

// GroupVersionResource names a collection of API objects.
type GroupVersionResource struct {
	Group    string
	Version  string
	Resource string
}

func (g GroupVersionResource) String() string {
	if g.Group == "" {
		return g.Version + "/" + g.Resource
	}
	return g.Group + "/" + g.Version + "/" + g.Resource
}

// CollectionPath returns the REST path of the collection, namespaced when
// namespace is non-empty (all namespaces otherwise).
func (g GroupVersionResource) CollectionPath(namespace string) string {
	var b strings.Builder
	if g.Group == "" {
		b.WriteString("/api/")
		b.WriteString(url.PathEscape(g.Version))
	} else {
		b.WriteString("/apis/")
		b.WriteString(url.PathEscape(g.Group))
		b.WriteString("/")
		b.WriteString(url.PathEscape(g.Version))
	}
	if namespace != "" {
		b.WriteString("/namespaces/")
		b.WriteString(url.PathEscape(namespace))
	}
	b.WriteString("/")
	b.WriteString(url.PathEscape(g.Resource))
	return b.String()
}

// ObjectPath returns the REST path of one object (plus optional
// subresource such as "status").
func (g GroupVersionResource) ObjectPath(namespace, name, subresource string) string {
	p := g.CollectionPath(namespace) + "/" + url.PathEscape(name)
	if subresource != "" {
		p += "/" + url.PathEscape(subresource)
	}
	return p
}

// Resources of the AeroLLM custom resources.
var (
	AeroRouteResource         = GroupVersionResource{Group: v1alpha1.GroupName, Version: v1alpha1.Version, Resource: v1alpha1.PluralAeroRoute}
	AeroBudgetResource        = GroupVersionResource{Group: v1alpha1.GroupName, Version: v1alpha1.Version, Resource: v1alpha1.PluralAeroBudget}
	AeroAgentPipelineResource = GroupVersionResource{Group: v1alpha1.GroupName, Version: v1alpha1.Version, Resource: v1alpha1.PluralAeroAgentPipeline}
	secretsResource           = GroupVersionResource{Version: "v1", Resource: "secrets"}
)

// ResourceForKind maps an AeroLLM kind to its resource.
func ResourceForKind(kind ResourceKind) (GroupVersionResource, bool) {
	switch kind {
	case KindAeroRoute:
		return AeroRouteResource, true
	case KindAeroBudget:
		return AeroBudgetResource, true
	case KindAeroAgentPipeline:
		return AeroAgentPipelineResource, true
	}
	return GroupVersionResource{}, false
}

// StatusError is a non-2xx API server response (decoded metav1.Status when
// available).
type StatusError struct {
	Code    int
	Reason  string
	Message string
}

func (e *StatusError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Code)
	}
	if e.Reason != "" {
		return fmt.Sprintf("k8s: %d %s: %s", e.Code, e.Reason, msg)
	}
	return fmt.Sprintf("k8s: %d: %s", e.Code, msg)
}

func statusCode(err error) (int, string) {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code, se.Reason
	}
	return 0, ""
}

// IsGone reports a 410 Gone / Expired error: the requested resourceVersion
// (or continue token) is too old and the client must relist.
func IsGone(err error) bool {
	code, reason := statusCode(err)
	return code == http.StatusGone || reason == "Expired" || reason == "Gone"
}

// IsNotFound reports a 404 error.
func IsNotFound(err error) bool { code, _ := statusCode(err); return code == http.StatusNotFound }

// IsConflict reports a 409 error.
func IsConflict(err error) bool { code, _ := statusCode(err); return code == http.StatusConflict }

func decodeStatusError(code int, body []byte) *StatusError {
	se := &StatusError{Code: code}
	var st struct {
		Kind    string `json:"kind"`
		Code    int    `json:"code"`
		Reason  string `json:"reason"`
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &st) == nil && st.Kind == "Status" {
		if st.Code != 0 {
			se.Code = st.Code
		}
		se.Reason, se.Message = st.Reason, st.Message
	}
	se.Message = truncate(se.Message, 1024)
	return se
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// Client is a minimal Kubernetes REST client (list, watch, get, status
// patch) built on net/http. It is safe for concurrent use.
type Client struct {
	cfg     RESTConfig
	host    string
	hc      *http.Client
	timeout time.Duration
	now     func() time.Time

	mu          sync.Mutex
	token       string
	tokenReadAt time.Time
}

// NewClient validates cfg and builds a client.
func NewClient(cfg *RESTConfig) (*Client, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	tlsCfg, err := cfg.TLSConfig()
	if err != nil {
		return nil, err
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSClientConfig:       tlsCfg,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConnsPerHost:   10,
		ForceAttemptHTTP2:     true,
	}
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	c := &Client{
		cfg:  *cfg,
		host: strings.TrimRight(cfg.Host, "/"),
		hc: &http.Client{
			Transport: transport,
			// Never follow redirects: they could carry the bearer token
			// to another host.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		timeout: timeout,
		now:     time.Now,
	}
	if cfg.BearerTokenFile != "" {
		if _, err := c.bearer(); err != nil {
			return nil, fmt.Errorf("k8s: token file: %w", err)
		}
	}
	return c, nil
}

// Namespace returns the default namespace of the underlying config.
func (c *Client) Namespace() string { return c.cfg.Namespace }

// Host returns the API server base URL.
func (c *Client) Host() string { return c.host }

// bearer returns the current token, re-reading BearerTokenFile at most once
// per tokenRefreshInterval. A failed re-read keeps the last good token.
func (c *Client) bearer() (string, error) {
	if c.cfg.BearerTokenFile == "" {
		return c.cfg.BearerToken, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.now()
	if c.token != "" && now.Sub(c.tokenReadAt) < tokenRefreshInterval {
		return c.token, nil
	}
	tok, err := readTokenFile(c.cfg.BearerTokenFile)
	if err != nil || tok == "" {
		if c.token != "" {
			return c.token, nil
		}
		if err == nil {
			err = errors.New("token file is empty")
		}
		return "", err
	}
	c.token, c.tokenReadAt = tok, now
	return tok, nil
}

// invalidateToken forces the next request to re-read the token file (after
// a 401, e.g. because the token rotated).
func (c *Client) invalidateToken() {
	c.mu.Lock()
	c.tokenReadAt = time.Time{}
	c.mu.Unlock()
}

func (c *Client) newRequest(ctx context.Context, method, path string, query url.Values, body []byte, contentType string) (*http.Request, error) {
	u := c.host + path
	if len(query) > 0 {
		u += "?" + query.Encode()
	}
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, u, rd)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "aero-operator (stdlib k8s client)")
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	tok, err := c.bearer()
	if err != nil {
		return nil, fmt.Errorf("k8s: bearer token: %w", err)
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	return req, nil
}

// do performs a non-watch request bounded by the client timeout and
// returns the body of a 2xx response (at most limit bytes).
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body []byte, contentType string, limit int64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	req, err := c.newRequest(ctx, method, path, query, body, contentType)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("k8s: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		if resp.StatusCode == http.StatusUnauthorized {
			c.invalidateToken()
		}
		return nil, decodeStatusError(resp.StatusCode, b)
	}
	b, err := io.ReadAll(io.LimitReader(resp.Body, limit+1))
	if err != nil {
		return nil, fmt.Errorf("k8s: %s %s: read body: %w", method, path, err)
	}
	if int64(len(b)) > limit {
		return nil, fmt.Errorf("k8s: %s %s: response exceeds %d bytes", method, path, limit)
	}
	return b, nil
}

// ListOptions controls one List call.
type ListOptions struct {
	Limit           int64
	Continue        string
	ResourceVersion string
	LabelSelector   string
}

// ObjectList is one page of a list response.
type ObjectList struct {
	ResourceVersion string
	Continue        string
	Items           []map[string]interface{}
}

// List fetches one page of a collection.
func (c *Client) List(ctx context.Context, gvr GroupVersionResource, namespace string, opts ListOptions) (*ObjectList, error) {
	q := url.Values{}
	if opts.Limit > 0 {
		q.Set("limit", strconv.FormatInt(opts.Limit, 10))
	}
	if opts.Continue != "" {
		q.Set("continue", opts.Continue)
	}
	if opts.ResourceVersion != "" {
		q.Set("resourceVersion", opts.ResourceVersion)
	}
	if opts.LabelSelector != "" {
		q.Set("labelSelector", opts.LabelSelector)
	}
	body, err := c.do(ctx, http.MethodGet, gvr.CollectionPath(namespace), q, nil, "", MaxListPageBytes)
	if err != nil {
		return nil, err
	}
	var raw struct {
		Metadata struct {
			ResourceVersion string `json:"resourceVersion"`
			Continue        string `json:"continue"`
		} `json:"metadata"`
		Items []map[string]interface{} `json:"items"`
	}
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("k8s: list %s: invalid JSON: %w", gvr, err)
	}
	return &ObjectList{ResourceVersion: raw.Metadata.ResourceVersion, Continue: raw.Metadata.Continue, Items: raw.Items}, nil
}

// ListAll pages through a collection (pageSize <= 0 uses 500) and returns
// every item plus the list resourceVersion to start a watch from. An
// expired continue token restarts the list once.
func (c *Client) ListAll(ctx context.Context, gvr GroupVersionResource, namespace string, pageSize int64) ([]map[string]interface{}, string, error) {
	if pageSize <= 0 {
		pageSize = defaultPageSize
	}
	restarted := false
	for {
		var items []map[string]interface{}
		opts := ListOptions{Limit: pageSize}
		rv := ""
		for {
			page, err := c.List(ctx, gvr, namespace, opts)
			if err != nil {
				if IsGone(err) && opts.Continue != "" && !restarted {
					restarted = true
					items = nil
					break
				}
				return nil, "", err
			}
			items = append(items, page.Items...)
			if len(items) > MaxListItems {
				return nil, "", fmt.Errorf("k8s: list %s: more than %d items", gvr, MaxListItems)
			}
			rv = page.ResourceVersion
			if page.Continue == "" {
				return items, rv, nil
			}
			opts.Continue = page.Continue
		}
	}
}

// Get fetches one object.
func (c *Client) Get(ctx context.Context, gvr GroupVersionResource, namespace, name string) (map[string]interface{}, error) {
	body, err := c.do(ctx, http.MethodGet, gvr.ObjectPath(namespace, name, ""), nil, nil, "", MaxObjectBytes)
	if err != nil {
		return nil, err
	}
	var obj map[string]interface{}
	if err := json.Unmarshal(body, &obj); err != nil {
		return nil, fmt.Errorf("k8s: get %s %s/%s: invalid JSON: %w", gvr, namespace, name, err)
	}
	return obj, nil
}

// PatchStatus applies a JSON merge patch (RFC 7386) to the object's status
// subresource and returns the updated object.
func (c *Client) PatchStatus(ctx context.Context, gvr GroupVersionResource, namespace, name string, mergePatch []byte) (map[string]interface{}, error) {
	body, err := c.do(ctx, http.MethodPatch, gvr.ObjectPath(namespace, name, "status"), nil, mergePatch, "application/merge-patch+json", MaxObjectBytes)
	if err != nil {
		return nil, err
	}
	var obj map[string]interface{}
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &obj); err != nil {
			return nil, fmt.Errorf("k8s: patch status %s %s/%s: invalid JSON: %w", gvr, namespace, name, err)
		}
	}
	return obj, nil
}

// SecretValue returns data[key] of a core/v1 Secret. The value is never
// logged or included in errors.
func (c *Client) SecretValue(ctx context.Context, namespace, name, key string) ([]byte, error) {
	body, err := c.do(ctx, http.MethodGet, secretsResource.ObjectPath(namespace, name, ""), nil, nil, "", MaxObjectBytes)
	if err != nil {
		return nil, err
	}
	var sec struct {
		Data map[string]string `json:"data"`
	}
	if err := json.Unmarshal(body, &sec); err != nil {
		return nil, fmt.Errorf("k8s: secret %s/%s: invalid JSON", namespace, name)
	}
	enc, ok := sec.Data[key]
	if !ok {
		return nil, fmt.Errorf("k8s: secret %s/%s has no key %q", namespace, name, key)
	}
	v, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, fmt.Errorf("k8s: secret %s/%s key %q: invalid base64", namespace, name, key)
	}
	return v, nil
}

// WatchEventType is the type of a watch event.
type WatchEventType string

// Watch event types.
const (
	EventAdded    WatchEventType = "ADDED"
	EventModified WatchEventType = "MODIFIED"
	EventDeleted  WatchEventType = "DELETED"
	EventBookmark WatchEventType = "BOOKMARK"
	EventError    WatchEventType = "ERROR"
)

// WatchEvent is one event from a watch stream. For EventError, Object is
// the metav1.Status; use StatusError to decode it.
type WatchEvent struct {
	Type   WatchEventType
	Object map[string]interface{}
}

// StatusError converts an ERROR event's Status object into an error.
func (e WatchEvent) StatusError() *StatusError {
	se := &StatusError{Code: http.StatusInternalServerError}
	if e.Object == nil {
		return se
	}
	if c, ok := e.Object["code"].(float64); ok && c > 0 {
		se.Code = int(c)
	}
	se.Reason, _ = e.Object["reason"].(string)
	msg, _ := e.Object["message"].(string)
	se.Message = truncate(msg, 1024)
	return se
}

// WatchOptions controls a watch request.
type WatchOptions struct {
	// ResourceVersion to start from ("" = most recent).
	ResourceVersion string
	// TimeoutSeconds asks the server to end the watch after this long
	// (default 300). The client also aborts a stream that stays silent for
	// TimeoutSeconds+60s, which detects half-open connections.
	TimeoutSeconds int64
	// AllowBookmarks requests BOOKMARK events.
	AllowBookmarks bool
	LabelSelector  string
}

// ErrWatchIdle is returned by Watcher.Next when the stream was silent for
// longer than the idle limit.
var ErrWatchIdle = errors.New("k8s: watch stream idle")

// Watcher reads events from one watch stream.
type Watcher struct {
	body   io.ReadCloser
	dec    *json.Decoder
	lim    *boundedReader
	cancel context.CancelFunc
	timer  *time.Timer
	idle   time.Duration

	mu     sync.Mutex
	idled  bool
	closed bool
}

// Watch opens a watch stream on a collection. Use Next to read events and
// Close to release the connection.
func (c *Client) Watch(ctx context.Context, gvr GroupVersionResource, namespace string, opts WatchOptions) (*Watcher, error) {
	timeoutSec := opts.TimeoutSeconds
	if timeoutSec <= 0 {
		timeoutSec = 300
	}
	q := url.Values{}
	q.Set("watch", "true")
	q.Set("timeoutSeconds", strconv.FormatInt(timeoutSec, 10))
	if opts.ResourceVersion != "" {
		q.Set("resourceVersion", opts.ResourceVersion)
	}
	if opts.AllowBookmarks {
		q.Set("allowWatchBookmarks", "true")
	}
	if opts.LabelSelector != "" {
		q.Set("labelSelector", opts.LabelSelector)
	}
	wctx, cancel := context.WithCancel(ctx)
	req, err := c.newRequest(wctx, http.MethodGet, gvr.CollectionPath(namespace), q, nil, "")
	if err != nil {
		cancel()
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("k8s: watch %s: %w", gvr, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxErrorBodyBytes))
		resp.Body.Close()
		cancel()
		if resp.StatusCode == http.StatusUnauthorized {
			c.invalidateToken()
		}
		return nil, decodeStatusError(resp.StatusCode, b)
	}
	w := &Watcher{body: resp.Body, cancel: cancel, idle: time.Duration(timeoutSec)*time.Second + time.Minute}
	w.lim = &boundedReader{r: resp.Body, max: MaxWatchEventBytes}
	w.dec = json.NewDecoder(w.lim)
	w.timer = time.AfterFunc(w.idle, func() {
		w.mu.Lock()
		w.idled = true
		w.mu.Unlock()
		cancel()
	})
	return w, nil
}

// Next blocks for the next event. It returns io.EOF when the server ended
// the stream normally.
func (w *Watcher) Next() (WatchEvent, error) {
	var raw struct {
		Type   WatchEventType  `json:"type"`
		Object json.RawMessage `json:"object"`
	}
	w.lim.reset()
	if err := w.dec.Decode(&raw); err != nil {
		w.mu.Lock()
		idled, closed := w.idled, w.closed
		w.mu.Unlock()
		switch {
		case idled:
			return WatchEvent{}, ErrWatchIdle
		case closed:
			return WatchEvent{}, io.EOF
		case errors.Is(err, io.EOF):
			return WatchEvent{}, io.EOF
		case errors.Is(err, errEventTooLarge):
			return WatchEvent{}, fmt.Errorf("k8s: watch event exceeds %d bytes", MaxWatchEventBytes)
		}
		return WatchEvent{}, fmt.Errorf("k8s: watch stream: %w", err)
	}
	w.timer.Reset(w.idle)
	ev := WatchEvent{Type: raw.Type}
	if len(raw.Object) > 0 && !bytes.Equal(raw.Object, []byte("null")) {
		if err := json.Unmarshal(raw.Object, &ev.Object); err != nil {
			return WatchEvent{}, fmt.Errorf("k8s: watch event object: %w", err)
		}
	}
	switch ev.Type {
	case EventAdded, EventModified, EventDeleted, EventBookmark, EventError:
	default:
		return WatchEvent{}, fmt.Errorf("k8s: unknown watch event type %q", ev.Type)
	}
	return ev, nil
}

// Close stops the stream. It is safe to call more than once.
func (w *Watcher) Close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	w.mu.Unlock()
	w.timer.Stop()
	w.cancel()
	w.body.Close()
}

var errEventTooLarge = errors.New("watch event too large")

// boundedReader fails once more than max bytes were read since the last
// reset, bounding the memory a single watch event can consume.
type boundedReader struct {
	r   io.Reader
	n   int64
	max int64
}

func (b *boundedReader) reset() { b.n = 0 }

func (b *boundedReader) Read(p []byte) (int, error) {
	if b.n >= b.max {
		return 0, errEventTooLarge
	}
	if rem := b.max - b.n; int64(len(p)) > rem {
		p = p[:rem]
	}
	n, err := b.r.Read(p)
	b.n += int64(n)
	return n, err
}
