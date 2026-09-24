// Package mcp implements a Model Context Protocol server over the Streamable
// HTTP transport (JSON-RPC 2.0 messages POSTed to a single endpoint).
//
// The server speaks protocol revisions 2024-11-05 through 2025-11-25
// (negotiated at initialize) and offers tools (backed by static definitions
// and/or an agent.ToolRegistry, optionally billed through an
// agent.ToolCallBiller), resources (backed by a ResourceProvider) and prompt
// templates. Sessions (Mcp-Session-Id) are issued at initialize, required on
// later requests, expire when idle and are terminated with DELETE; they can
// be disabled for stateless multi-replica deployments.
package mcp

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
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

// EnvStateless names the environment variable that, when set to a true
// value, disables MCP sessions (see WithoutSessions). Use it when several
// gateway replicas serve /mcp without sticky routing: sessions live in the
// memory of the replica that issued them.
const EnvStateless = "AEROLLM_MCP_STATELESS"

// Defaults for Server limits.
const (
	DefaultMaxBodyBytes       int64 = 4 << 20
	DefaultToolTimeout              = 60 * time.Second
	DefaultMaxToolOutputBytes       = 1 << 20
	DefaultMaxBatchSize             = 64
	// DefaultPageSize is the page size of resources/list and prompts/list.
	DefaultPageSize = 100
	serverName      = "aerollm-mcp"
	serverVersion   = "1.0.0"
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
// HTTP transport. Every POST carries one JSON-RPC message or a batch, and
// responses are returned as application/json; the server does not offer a
// server-initiated SSE stream (GET yields 405).
//
// Exported configuration fields must be set before the server starts
// serving requests.
type Server struct {
	mu       sync.RWMutex
	tools    map[string]ToolDefinition
	registry *agent.ToolRegistry
	prompts  map[string]PromptDefinition

	sessions     *sessionManager // nil in stateless mode
	sessionOwner func(*http.Request) string
	resources    ResourceProvider
	biller       agent.ToolCallBiller
	instructions string

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
	// MaxResourceBytes caps one resources/read result (default 8 MiB).
	MaxResourceBytes int
	// PageSize is the page size of resources/list and prompts/list
	// (default DefaultPageSize).
	PageSize int
}

// Option configures a Server.
type Option func(*Server)

// WithToolBilling routes every tools/call through b before the tool runs.
// The request context (carrying the gateway principal and the MCP session
// ID) is passed to BillToolCall; when it returns an error the tool is not
// executed and the call returns an isError result ("tool call rejected:
// billing failed"). Charges are not refunded when the tool itself fails.
func WithToolBilling(b agent.ToolCallBiller) Option {
	return func(s *Server) { s.biller = b }
}

// WithResourceProvider enables the resources capability backed by p.
func WithResourceProvider(p ResourceProvider) Option {
	return func(s *Server) { s.resources = p }
}

// WithoutSessions disables sessions: no Mcp-Session-Id is issued or
// required and DELETE yields 405. Use it for multi-replica deployments
// without sticky routing.
func WithoutSessions() Option {
	return func(s *Server) { s.sessions = nil }
}

// WithSessions (re-)enables sessions, overriding AEROLLM_MCP_STATELESS.
func WithSessions() Option {
	return func(s *Server) {
		if s.sessions == nil {
			s.sessions = newSessionManager()
		}
	}
}

// WithSessionIdleTimeout sets how long an unused session stays valid
// (default 30 minutes). Non-positive values are ignored.
func WithSessionIdleTimeout(d time.Duration) Option {
	return func(s *Server) {
		if s.sessions != nil && d > 0 {
			s.sessions.idle = d
		}
	}
}

// WithMaxSessions bounds the number of live sessions (default 10000). When
// full, the least recently used session is evicted; its client receives 404
// and re-initializes. Non-positive values are ignored.
func WithMaxSessions(n int) Option {
	return func(s *Server) {
		if s.sessions != nil && n > 0 {
			s.sessions.max = n
		}
	}
}

// WithSessionOwner sets the function deriving the owner a session is bound
// to; requests from a different owner get 404 for it. The default uses the
// key ID of the gateway principal set by the auth middleware.
func WithSessionOwner(fn func(*http.Request) string) Option {
	return func(s *Server) {
		if fn != nil {
			s.sessionOwner = fn
		}
	}
}

// WithInstructions sets the instructions returned by initialize.
func WithInstructions(text string) Option {
	return func(s *Server) { s.instructions = text }
}

// Session represents an MCP client session.
//
// Deprecated: sessions are tracked internally (see SessionInfo and
// SessionIDFromContext) and no SSE stream is opened; the type is kept for
// API compatibility.
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

// NewServer creates a new MCP server with no tools. Sessions are enabled
// unless AEROLLM_MCP_STATELESS is true; opts are applied last.
func NewServer(opts ...Option) *Server {
	s := &Server{
		tools:              make(map[string]ToolDefinition),
		prompts:            make(map[string]PromptDefinition),
		sessionOwner:       defaultSessionOwner,
		AllowedOrigins:     parseOrigins(os.Getenv(EnvAllowedOrigins)),
		MaxBodyBytes:       DefaultMaxBodyBytes,
		ToolTimeout:        DefaultToolTimeout,
		MaxToolOutputBytes: DefaultMaxToolOutputBytes,
		MaxBatchSize:       DefaultMaxBatchSize,
		MaxResourceBytes:   DefaultMaxResourceBytes,
		PageSize:           DefaultPageSize,
	}
	if stateless, _ := strconv.ParseBool(os.Getenv(EnvStateless)); !stateless {
		s.sessions = newSessionManager()
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	return s
}

// NewServerWithRegistry creates an MCP server that exposes the tools of an
// agent ToolRegistry. Tools are resolved at request time, so tools registered
// later are picked up automatically. Tools that require human approval
// (agent.RequiresApproval) are never exposed, since MCP calls would bypass
// the HITL gate.
func NewServerWithRegistry(reg *agent.ToolRegistry, opts ...Option) *Server {
	s := NewServer(opts...)
	s.AttachRegistry(reg)
	return s
}

// SessionCount returns the number of live sessions (0 in stateless mode).
func (s *Server) SessionCount() int {
	if s.sessions == nil {
		return 0
	}
	return s.sessions.count()
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

// HandleHTTP implements the MCP Streamable HTTP endpoint:
//
//   - POST carries one JSON-RPC message or a batch. A successful initialize
//     returns an Mcp-Session-Id header; every later request must carry it
//     (400 without it, 404 once the session is unknown, expired, terminated
//     or bound to another principal). Pings are accepted without a session.
//   - DELETE with Mcp-Session-Id terminates the session (204).
//   - GET and other methods yield 405: no server-initiated SSE stream is
//     offered.
//
// In stateless mode (WithoutSessions) no session is issued or checked and
// only POST is allowed.
func (s *Server) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && (r.Method != http.MethodDelete || s.sessions == nil) {
		w.Header().Set("Allow", s.allowedMethods())
		writeHTTPError(w, http.StatusMethodNotAllowed, CodeInvalidRequest, "method not allowed")
		return
	}
	if !originAllowed(r, s.AllowedOrigins) {
		writeHTTPError(w, http.StatusForbidden, CodeInvalidRequest, "origin not allowed")
		return
	}
	if r.Method == http.MethodDelete {
		s.handleDelete(w, r)
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		mt, _, err := mime.ParseMediaType(ct)
		if err != nil || mt != "application/json" {
			writeHTTPError(w, http.StatusUnsupportedMediaType, CodeInvalidRequest, "content type must be application/json")
			return
		}
	}
	if !acceptsJSON(r) {
		writeHTTPError(w, http.StatusNotAcceptable, CodeInvalidRequest, "client must accept application/json")
		return
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

	items := []json.RawMessage{body}
	batch := body[0] == '['
	if batch {
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
	}

	st := &callState{batch: batch}
	ctx := r.Context()
	if s.sessions != nil {
		initialize := !batch && peekMethod(body) == "initialize"
		if !initialize {
			sid := r.Header.Get(HeaderSessionID)
			switch {
			case sid != "":
				info, ok := s.sessions.touch(sid, s.sessionOwner(r))
				if !ok {
					writeHTTPError(w, http.StatusNotFound, CodeSessionNotFound, "session not found")
					return
				}
				st.session = &info
				ctx = withSessionID(ctx, info.ID)
			case !onlyPings(items):
				writeHTTPError(w, http.StatusBadRequest, CodeSessionRequired, "bad request: "+HeaderSessionID+" header is required")
				return
			}
		}
	}

	if !batch {
		resp := s.handleMessage(ctx, st, body)
		if resp == nil {
			// Notifications and client responses: accepted, no body.
			w.WriteHeader(http.StatusAccepted)
			return
		}
		if st.initOK && s.sessions != nil {
			info, err := s.sessions.create(s.sessionOwner(r), st.initVersion, st.clientName, st.clientVersion)
			if err != nil {
				writeHTTPError(w, http.StatusInternalServerError, CodeInternalError, "failed to create session")
				return
			}
			w.Header().Set(HeaderSessionID, info.ID)
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}

	responses := make([]*rpcResponse, 0, len(items))
	for _, item := range items {
		if resp := s.handleMessage(ctx, st, item); resp != nil {
			responses = append(responses, resp)
		}
	}
	if len(responses) == 0 {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, http.StatusOK, responses)
}

// handleDelete terminates a session (sessions enabled only).
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	sid := r.Header.Get(HeaderSessionID)
	if sid == "" {
		writeHTTPError(w, http.StatusBadRequest, CodeSessionRequired, "bad request: "+HeaderSessionID+" header is required")
		return
	}
	if !s.sessions.remove(sid, s.sessionOwner(r)) {
		writeHTTPError(w, http.StatusNotFound, CodeSessionNotFound, "session not found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *Server) allowedMethods() string {
	if s.sessions != nil {
		return "POST, DELETE"
	}
	return http.MethodPost
}

// callState carries per-HTTP-request state through message handling.
type callState struct {
	batch   bool
	session *SessionInfo

	// Set by a successful initialize.
	initOK        bool
	initVersion   string
	clientName    string
	clientVersion string
}

// peekMethod returns the "method" of a JSON-RPC message ("" if absent).
func peekMethod(raw json.RawMessage) string {
	var m struct {
		Method json.RawMessage `json:"method"`
	}
	if json.Unmarshal(raw, &m) != nil || len(m.Method) == 0 {
		return ""
	}
	var name string
	if json.Unmarshal(m.Method, &name) != nil {
		return ""
	}
	return name
}

// onlyPings reports whether every message is a ping request, which the spec
// allows before (and therefore without) a session.
func onlyPings(items []json.RawMessage) bool {
	for _, it := range items {
		if peekMethod(it) != "ping" {
			return false
		}
	}
	return len(items) > 0
}

// acceptsJSON reports whether the Accept header (if any) admits
// application/json responses.
func acceptsJSON(r *http.Request) bool {
	values := r.Header.Values("Accept")
	if len(values) == 0 {
		return true
	}
	for _, v := range values {
		for _, part := range strings.Split(v, ",") {
			mt, _, err := mime.ParseMediaType(strings.TrimSpace(part))
			if err != nil {
				continue
			}
			switch mt {
			case "application/json", "application/*", "*/*":
				return true
			}
		}
	}
	return false
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
func (s *Server) handleMessage(ctx context.Context, st *callState, raw json.RawMessage) *rpcResponse {
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

	result, rpcErr := s.dispatch(ctx, st, method, fields["params"])
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

func (s *Server) dispatch(ctx context.Context, st *callState, method string, params json.RawMessage) (interface{}, *rpcError) {
	switch method {
	case "initialize":
		return s.initialize(st, params)

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
		if err := s.bill(ctx, def.Name); err != nil {
			return toolResult("tool call rejected: billing failed", true), nil
		}
		return s.callTool(ctx, def, args), nil

	case "resources/list":
		if s.resources == nil {
			break
		}
		return s.listResources(ctx, params)

	case "resources/read":
		if s.resources == nil {
			break
		}
		return s.readResource(ctx, params)

	case "resources/templates/list":
		if s.resources == nil {
			break
		}
		var p struct {
			Cursor *string `json:"cursor"`
		}
		if e := decodeParams(params, &p, false); e != nil {
			return nil, e
		}
		return map[string]interface{}{"resourceTemplates": []interface{}{}}, nil

	case "prompts/list":
		return s.listPrompts(params)

	case "prompts/get":
		return s.getPrompt(ctx, params)
	}
	return nil, &rpcError{Code: CodeMethodNotFound, Message: "method not found: " + TruncateString(method, 128)}
}

// initialize negotiates the protocol version and advertises capabilities.
// Capabilities are only advertised for configured features: resources with
// a ResourceProvider, prompts once at least one prompt is registered.
func (s *Server) initialize(st *callState, params json.RawMessage) (interface{}, *rpcError) {
	if st.batch {
		return nil, &rpcError{Code: CodeInvalidRequest, Message: "invalid request: initialize must not be part of a batch"}
	}
	var p struct {
		ProtocolVersion json.RawMessage `json:"protocolVersion"`
		ClientInfo      struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
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
	caps := map[string]interface{}{
		"tools": map[string]interface{}{"listChanged": false},
	}
	if s.resources != nil {
		caps["resources"] = map[string]interface{}{"subscribe": false, "listChanged": false}
	}
	if s.hasPrompts() {
		caps["prompts"] = map[string]interface{}{"listChanged": false}
	}
	result := map[string]interface{}{
		"protocolVersion": version,
		"capabilities":    caps,
		"serverInfo": map[string]interface{}{
			"name":    serverName,
			"version": serverVersion,
		},
	}
	if s.instructions != "" {
		result["instructions"] = s.instructions
	}
	st.initOK = true
	st.initVersion = version
	st.clientName = TruncateString(p.ClientInfo.Name, 256)
	st.clientVersion = TruncateString(p.ClientInfo.Version, 64)
	return result, nil
}

// bill charges a tool call through the configured biller (if any). Panics
// and timeouts are treated as billing failures.
func (s *Server) bill(ctx context.Context, tool string) error {
	if s.biller == nil {
		return nil
	}
	return s.callProvider(ctx, func(ctx context.Context) error {
		return s.biller.BillToolCall(ctx, tool)
	})
}

// callProvider runs fn (a resource/prompt provider or the biller) with the
// tool timeout, converting panics into errors.
func (s *Server) callProvider(ctx context.Context, fn func(context.Context) error) (err error) {
	timeout := s.ToolTimeout
	if timeout <= 0 {
		timeout = DefaultToolTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	defer func() {
		if rec := recover(); rec != nil {
			err = errProviderPanicked
		}
	}()
	return fn(ctx)
}

func (s *Server) pageSize() int {
	if s.PageSize > 0 {
		return s.PageSize
	}
	return DefaultPageSize
}

// paginate resolves an opaque cursor into the [start, end) window of a list
// of total items and returns the cursor of the next page ("" on the last).
func paginate(total int, cursor *string, pageSize int) (start, end int, next string, rerr *rpcError) {
	if cursor != nil && *cursor != "" {
		raw, err := base64.RawURLEncoding.DecodeString(*cursor)
		n, perr := strconv.Atoi(strings.TrimPrefix(string(raw), "o:"))
		if err != nil || perr != nil || !strings.HasPrefix(string(raw), "o:") || n < 0 {
			return 0, 0, "", &rpcError{Code: CodeInvalidParams, Message: "invalid params: invalid cursor"}
		}
		start = n
	}
	if start > total {
		start = total
	}
	end = start + pageSize
	if end >= total {
		return start, total, "", nil
	}
	return start, end, base64.RawURLEncoding.EncodeToString([]byte("o:" + strconv.Itoa(end))), nil
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
