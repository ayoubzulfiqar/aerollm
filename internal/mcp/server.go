// Package mcp implements a Model Context Protocol server over the Streamable
// HTTP transport (JSON-RPC 2.0 messages POSTed to a single endpoint).
package mcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/ayoubzulfiqar/aerollm/internal/agent"
	"github.com/ayoubzulfiqar/aerollm/internal/models"
)

// LatestProtocolVersion is the newest MCP protocol revision this server speaks.
const LatestProtocolVersion = "2025-11-25"

// SupportedProtocolVersions lists the protocol revisions accepted during
// initialize negotiation and in the MCP-Protocol-Version header.
var SupportedProtocolVersions = []string{"2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"}

// EnvAllowedOrigins names the environment variable holding a comma-separated
// list of browser origins allowed to call the MCP endpoint cross-origin
// ("*" allows any). Requests without an Origin header and same-origin requests
// are always accepted.
const EnvAllowedOrigins = "AEROLLM_MCP_ALLOWED_ORIGINS"

// Defaults for Server limits.
const (
	DefaultMaxBodyBytes       int64 = 4 << 20
	DefaultToolTimeout              = 60 * time.Second
	DefaultMaxToolOutputBytes       = 1 << 20
	DefaultMaxBatchSize             = 64
	serverName                      = "aerollm-mcp"
	serverVersion                   = "1.0.0"
)

// JSON-RPC 2.0 error codes.
const (
	CodeParseError     = -32700
	CodeInvalidRequest = -32600
	CodeMethodNotFound = -32601
	CodeInvalidParams  = -32602
	CodeInternalError  = -32603
)

// ToolHandler executes an MCP tool call.
type ToolHandler func(ctx context.Context, arguments map[string]interface{}) (interface{}, error)

// ToolDefinition describes an MCP-exposed tool.
type ToolDefinition struct {
	Name        string
	Description string
	InputSchema map[string]interface{}
	Handler     ToolHandler
}

// Server implements a Model Context Protocol server using the Streamable
// HTTP transport. It is stateless: every POST carries one JSON-RPC message or
// a batch, and responses are returned as application/json.
//
// Exported configuration fields must be set before the server starts
// serving requests.
type Server struct {
	mu       sync.RWMutex
	tools    map[string]ToolDefinition
	registry *agent.ToolRegistry

	// AllowedOrigins lists cross-origin browser origins permitted to call the
	// endpoint (DNS-rebinding protection). Defaults to AEROLLM_MCP_ALLOWED_ORIGINS.
	AllowedOrigins []string
	// MaxBodyBytes caps the request body (default 4 MiB).
	MaxBodyBytes int64
	// ToolTimeout bounds a single tools/call execution (default 60s).
	ToolTimeout time.Duration
	// MaxToolOutputBytes caps the text returned by a tool (default 1 MiB).
	MaxToolOutputBytes int
	// MaxBatchSize caps the number of messages in a JSON-RPC batch (default 64).
	MaxBatchSize int
}

// Session represents an MCP client session.
//
// Deprecated: the server is stateless and no longer opens SSE sessions; the
// type is kept for API compatibility.
type Session struct {
	ID      string
	Server  *Server
	Writer  http.ResponseWriter
	Flusher http.Flusher
	mu      sync.Mutex
	closed  bool
}

// EventHub manages SSE event broadcasting to sessions.
//
// Deprecated: kept for API compatibility; the Streamable HTTP server does not
// offer a server-initiated SSE stream.
type EventHub struct {
	mu      sync.RWMutex
	session map[*Session]struct{}
}

// NewEventHub creates an empty EventHub.
func NewEventHub() *EventHub {
	return &EventHub{session: make(map[*Session]struct{})}
}

// Add registers a session.
func (h *EventHub) Add(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.session[s] = struct{}{}
}

// Remove unregisters a session.
func (h *EventHub) Remove(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.session, s)
}

// Broadcast writes event to every open session as an SSE "message" event.
// Sessions whose writes fail are marked closed.
func (h *EventHub) Broadcast(event map[string]interface{}) {
	data, err := json.Marshal(event)
	if err != nil {
		return
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.session {
		s.mu.Lock()
		if !s.closed && s.Writer != nil {
			if _, err := fmt.Fprintf(s.Writer, "event: message\ndata: %s\n\n", data); err != nil {
				s.closed = true
			} else if s.Flusher != nil {
				s.Flusher.Flush()
			}
		}
		s.mu.Unlock()
	}
}

// NewServer creates a new MCP server with no tools.
func NewServer() *Server {
	return &Server{
		tools:              make(map[string]ToolDefinition),
		AllowedOrigins:     parseOrigins(os.Getenv(EnvAllowedOrigins)),
		MaxBodyBytes:       DefaultMaxBodyBytes,
		ToolTimeout:        DefaultToolTimeout,
		MaxToolOutputBytes: DefaultMaxToolOutputBytes,
		MaxBatchSize:       DefaultMaxBatchSize,
	}
}

// NewServerWithRegistry creates an MCP server that exposes the tools of an
// agent ToolRegistry. Tools are resolved at request time, so tools registered
// later are picked up automatically. Tools that require human approval
// (agent.RequiresApproval) are never exposed, since MCP calls would bypass
// the HITL gate.
func NewServerWithRegistry(reg *agent.ToolRegistry) *Server {
	s := NewServer()
	s.AttachRegistry(reg)
	return s
}

// AttachRegistry exposes reg's tools through this server (see
// NewServerWithRegistry). Statically registered tools win on name conflicts.
func (s *Server) AttachRegistry(reg *agent.ToolRegistry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registry = reg
}

// RegisterTool adds a tool to the MCP server. Definitions without a name or
// handler are ignored; use AddTool to get an error instead.
func (s *Server) RegisterTool(def ToolDefinition) {
	_ = s.AddTool(def)
}

// AddTool adds a tool to the MCP server, replacing any tool of the same name.
func (s *Server) AddTool(def ToolDefinition) error {
	if strings.TrimSpace(def.Name) == "" {
		return errors.New("mcp: tool name is required")
	}
	if def.Handler == nil {
		return fmt.Errorf("mcp: tool %q has no handler", def.Name)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tools == nil {
		s.tools = make(map[string]ToolDefinition)
	}
	s.tools[def.Name] = def
	return nil
}

// Tools returns the names of all exposed tools, sorted.
func (s *Server) Tools() []string {
	defs := s.listTools()
	out := make([]string, 0, len(defs))
	for _, d := range defs {
		out = append(out, d.Name)
	}
	return out
}

// listTools returns static tools plus exposable registry tools, sorted.
func (s *Server) listTools() []ToolDefinition {
	s.mu.RLock()
	out := make([]ToolDefinition, 0, len(s.tools))
	seen := make(map[string]struct{}, len(s.tools))
	for _, d := range s.tools {
		out = append(out, d)
		seen[d.Name] = struct{}{}
	}
	reg := s.registry
	s.mu.RUnlock()
	if reg != nil {
		for _, t := range reg.List() {
			if _, dup := seen[t.Name()]; dup || agent.RequiresApproval(t) {
				continue
			}
			out = append(out, registryToolDefinition(t))
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// lookupTool resolves an exposed tool by name.
func (s *Server) lookupTool(name string) (ToolDefinition, bool) {
	s.mu.RLock()
	def, ok := s.tools[name]
	reg := s.registry
	s.mu.RUnlock()
	if ok {
		return def, true
	}
	if reg == nil {
		return ToolDefinition{}, false
	}
	t, ok := reg.Get(name)
	if !ok || agent.RequiresApproval(t) {
		return ToolDefinition{}, false
	}
	return registryToolDefinition(t), true
}

func registryToolDefinition(t agent.Tool) ToolDefinition {
	def := agent.ToolDefinitionOf(t)
	return ToolDefinition{
		Name:        def.Name,
		Description: def.Description,
		InputSchema: def.Parameters,
		Handler:     t.Execute,
	}
}

// ServeHTTP implements http.Handler.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.HandleHTTP(w, r)
}

// HandleHTTP implements the MCP Streamable HTTP endpoint. Only POST is
// supported: this server does not offer a server-initiated SSE stream, so GET
// (and every other method) yields 405.
func (s *Server) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeHTTPError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}
	if !originAllowed(r, s.AllowedOrigins) {
		writeHTTPError(w, http.StatusForbidden, CodeInvalidRequest, "origin not allowed")
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			writeHTTPError(w, http.StatusUnsupportedMediaType, CodeInvalidRequest, "content type must be application/json")
			return
		}
	}
	if v := r.Header.Get("MCP-Protocol-Version"); v != "" && !protocolSupported(v) {
		writeHTTPError(w, http.StatusBadRequest, CodeInvalidRequest, "unsupported MCP protocol version")
		return
	}

	limit := s.MaxBodyBytes
	if limit <= 0 {
		limit = DefaultMaxBodyBytes
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			writeHTTPError(w, http.StatusRequestEntityTooLarge, CodeInvalidRequest, "request body too large")
			return
		}
		writeHTTPError(w, http.StatusBadRequest, CodeParseError, "failed to read request body")
		return
	}
	body = bytes.TrimSpace(body)
	if len(body) == 0 || !json.Valid(body) {
		writeHTTPError(w, http.StatusBadRequest, CodeParseError, "parse error")
		return
	}

	ctx := r.Context()
	if body[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(body, &items); err != nil {
			writeHTTPError(w, http.StatusBadRequest, CodeParseError, "parse error")
			return
		}
		maxBatch := s.MaxBatchSize
		if maxBatch <= 0 {
			maxBatch = DefaultMaxBatchSize
		}
		if len(items) == 0 {
			writeHTTPError(w, http.StatusBadRequest, CodeInvalidRequest, "empty batch")
			return
		}
		if len(items) > maxBatch {
			writeHTTPError(w, http.StatusBadRequest, CodeInvalidRequest, "batch too large")
			return
		}
		responses := make([]*rpcResponse, 0, len(items))
		for _, item := range items {
			if resp := s.handleMessage(ctx, item); resp != nil {
				responses = append(responses, resp)
			}
		}
		if len(responses) == 0 {
			w.WriteHeader(http.StatusAccepted)
			return
		}
		writeJSON(w, http.StatusOK, responses)
		return
	}

	resp := s.handleMessage(ctx, body)
	if resp == nil {
		// Notifications and client responses: accepted, no body.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

// JSONRPCRequest is a minimal JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      interface{}            `json:"id,omitempty"`
	Method  string                 `json:"method"`
	Params  map[string]interface{} `json:"params,omitempty"`
}

type rpcError struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

var nullID = json.RawMessage("null")

func errorResponse(id json.RawMessage, code int, msg string) *rpcResponse {
	if id == nil {
		id = nullID
	}
	return &rpcResponse{JSONRPC: "2.0", ID: id, Error: &rpcError{Code: code, Message: msg}}
}

// handleMessage processes one JSON-RPC message and returns the response, or
// nil when no response must be sent (notifications, client responses).
func (s *Server) handleMessage(ctx context.Context, raw json.RawMessage) *rpcResponse {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return errorResponse(nil, CodeInvalidRequest, "invalid request")
	}

	idRaw, hasID := fields["id"]
	idValid := hasID && validID(idRaw)
	respID := nullID
	if idValid {
		respID = idRaw
	}

	var version string
	if v, ok := fields["jsonrpc"]; !ok || json.Unmarshal(v, &version) != nil || version != "2.0" {
		return errorResponse(respID, CodeInvalidRequest, `invalid request: jsonrpc must be "2.0"`)
	}

	methodRaw, hasMethod := fields["method"]
	if !hasMethod {
		_, isResult := fields["result"]
		_, isError := fields["error"]
		if isResult || isError {
			return nil // a response from the client; nothing to answer
		}
		return errorResponse(respID, CodeInvalidRequest, "invalid request: missing method")
	}
	var method string
	if err := json.Unmarshal(methodRaw, &method); err != nil || method == "" {
		return errorResponse(respID, CodeInvalidRequest, "invalid request: method must be a non-empty string")
	}
	if hasID && !idValid {
		return errorResponse(nullID, CodeInvalidRequest, "invalid request: id must be a string or number")
	}
	if !hasID {
		// Notification (e.g. notifications/initialized, notifications/cancelled).
		// JSON-RPC forbids replying; request methods sent without an id are
		// ignored rather than executed.
		return nil
	}

	result, rpcErr := s.dispatch(ctx, method, fields["params"])
	if rpcErr != nil {
		return &rpcResponse{JSONRPC: "2.0", ID: respID, Error: rpcErr}
	}
	return &rpcResponse{JSONRPC: "2.0", ID: respID, Result: result}
}

// validID reports whether raw is a JSON string or number (MCP forbids null).
func validID(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 {
		return false
	}
	switch raw[0] {
	case '"':
		var s string
		return json.Unmarshal(raw, &s) == nil
	case '-', '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
		var n json.Number
		return json.Unmarshal(raw, &n) == nil
	}
	return false
}

// decodeParams unmarshals params into dst, requiring a JSON object when
// present. required reports whether absent params are an error.
func decodeParams(raw json.RawMessage, dst interface{}, required bool) *rpcError {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		if required {
			return &rpcError{Code: CodeInvalidParams, Message: "invalid params: params object is required"}
		}
		return nil
	}
	if raw[0] != '{' {
		return &rpcError{Code: CodeInvalidParams, Message: "invalid params: params must be an object"}
	}
	if err := json.Unmarshal(raw, dst); err != nil {
		return &rpcError{Code: CodeInvalidParams, Message: "invalid params"}
	}
	return nil
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (interface{}, *rpcError) {
	switch method {
	case "initialize":
		var p struct {
			ProtocolVersion json.RawMessage `json:"protocolVersion"`
		}
		if e := decodeParams(params, &p, false); e != nil {
			return nil, e
		}
		version := LatestProtocolVersion
		if len(p.ProtocolVersion) > 0 && !bytes.Equal(p.ProtocolVersion, []byte("null")) {
			var requested string
			if err := json.Unmarshal(p.ProtocolVersion, &requested); err != nil {
				return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: protocolVersion must be a string"}
			}
			if protocolSupported(requested) {
				version = requested
			}
		}
		return map[string]interface{}{
			"protocolVersion": version,
			"capabilities": map[string]interface{}{
				"tools": map[string]interface{}{"listChanged": false},
			},
			"serverInfo": map[string]interface{}{
				"name":    serverName,
				"version": serverVersion,
			},
		}, nil

	case "ping":
		return struct{}{}, nil

	case "tools/list":
		var p struct {
			Cursor *string `json:"cursor"`
		}
		if e := decodeParams(params, &p, false); e != nil {
			return nil, e
		}
		defs := s.listTools()
		tools := make([]map[string]interface{}, 0, len(defs))
		for _, d := range defs {
			schema := d.InputSchema
			if schema == nil {
				schema = map[string]interface{}{"type": "object"}
			}
			tools = append(tools, map[string]interface{}{
				"name":        d.Name,
				"description": d.Description,
				"inputSchema": schema,
			})
		}
		return map[string]interface{}{"tools": tools}, nil

	case "tools/call":
		var p struct {
			Name      *string         `json:"name"`
			Arguments json.RawMessage `json:"arguments"`
		}
		if e := decodeParams(params, &p, true); e != nil {
			return nil, e
		}
		if p.Name == nil || strings.TrimSpace(*p.Name) == "" {
			return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: tool name is required"}
		}
		args := map[string]interface{}{}
		if a := bytes.TrimSpace(p.Arguments); len(a) > 0 && !bytes.Equal(a, []byte("null")) {
			if a[0] != '{' || json.Unmarshal(a, &args) != nil {
				return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: arguments must be an object"}
			}
		}
		def, ok := s.lookupTool(*p.Name)
		if !ok {
			return nil, &rpcError{Code: CodeInvalidParams, Message: "unknown tool: " + TruncateString(*p.Name, 128)}
		}
		return s.callTool(ctx, def, args), nil

	default:
		return nil, &rpcError{Code: CodeMethodNotFound, Message: "method not found: " + TruncateString(method, 128)}
	}
}

// callTool executes a tool with a timeout and panic recovery and converts the
// outcome into an MCP CallToolResult. Tool failures are reported in-band with
// isError=true, as the protocol requires.
func (s *Server) callTool(ctx context.Context, def ToolDefinition, args map[string]interface{}) map[string]interface{} {
	timeout := s.ToolTimeout
	if timeout <= 0 {
		timeout = DefaultToolTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type outcome struct {
		val interface{}
		err error
	}
	ch := make(chan outcome, 1)
	go func() {
		defer func() {
			if rec := recover(); rec != nil {
				ch <- outcome{err: errToolPanicked}
			}
		}()
		v, err := def.Handler(ctx, args)
		ch <- outcome{val: v, err: err}
	}()

	var res outcome
	select {
	case res = <-ch:
	case <-ctx.Done():
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return toolResult("tool execution timed out", true)
		}
		return toolResult("tool execution cancelled", true)
	}
	limit := s.MaxToolOutputBytes
	if limit <= 0 {
		limit = DefaultMaxToolOutputBytes
	}
	if res.err != nil {
		return toolResult(TruncateString(res.err.Error(), limit), true)
	}
	return toolResult(TruncateString(stringify(res.val), limit), false)
}

var errToolPanicked = errors.New("tool execution failed")

func toolResult(text string, isError bool) map[string]interface{} {
	return map[string]interface{}{
		"content": []map[string]interface{}{{"type": "text", "text": text}},
		"isError": isError,
	}
}

// stringify renders a tool result as text: strings pass through, everything
// else is JSON-encoded.
func stringify(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case []byte:
		if utf8.Valid(t) {
			return string(t)
		}
	case fmt.Stringer:
		return t.String()
	}
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return string(b)
}

func protocolSupported(v string) bool {
	for _, s := range SupportedProtocolVersions {
		if s == v {
			return true
		}
	}
	return false
}

func parseOrigins(v string) []string {
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// originAllowed validates the Origin header to prevent DNS-rebinding and
// cross-site requests: absent (non-browser) and same-host origins are
// accepted; others must be listed in allowed ("*" allows any).
func originAllowed(r *http.Request, allowed []string) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	validURL := err == nil && u.Host != ""
	if validURL && strings.EqualFold(u.Host, r.Host) {
		return true
	}
	for _, a := range allowed {
		a = strings.ToLower(strings.TrimRight(strings.TrimSpace(a), "/"))
		if a == "*" {
			return true
		}
		if !validURL || a == "" {
			continue
		}
		host := strings.ToLower(u.Host)
		if a == strings.ToLower(u.Scheme)+"://"+host || a == host {
			return true
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	data, err := json.Marshal(v)
	if err != nil {
		status = http.StatusInternalServerError
		data = []byte(`{"jsonrpc":"2.0","id":null,"error":{"code":-32603,"message":"internal error"}}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func writeHTTPError(w http.ResponseWriter, status, code int, msg string) {
	writeJSON(w, status, errorResponse(nil, code, msg))
}

// ToMCPTool converts an AeroLLM ToolDefinition into an MCP tool description.
func ToMCPTool(def models.ToolDefinition, handler ToolHandler) ToolDefinition {
	return ToolDefinition{
		Name:        def.Name,
		Description: def.Description,
		InputSchema: def.Parameters,
		Handler:     handler,
	}
}

// TruncateString shortens v to at most limit bytes, appending "..." when it
// was cut. It never splits a UTF-8 sequence.
func TruncateString(v string, limit int) string {
	if limit <= 0 {
		return ""
	}
	if len(v) <= limit {
		return v
	}
	if limit <= 3 {
		return strings.Repeat(".", limit)
	}
	cut := limit - 3
	for cut > 0 && !utf8.RuneStart(v[cut]) {
		cut--
	}
	return v[:cut] + "..."
}
