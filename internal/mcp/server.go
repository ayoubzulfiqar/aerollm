package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
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

// Server implements a lightweight Model Context Protocol server.
// It exposes AeroLLM tools to MCP-compatible clients over HTTP+SSE.
type Server struct {
	mu        sync.RWMutex
	tools     map[string]ToolDefinition
	sessions  map[string]*Session
	eventHub  *EventHub
}

// Session represents an MCP client session.
type Session struct {
	ID        string
	Server    *Server
	Writer    http.ResponseWriter
	Flusher   http.Flusher
	mu        sync.Mutex
	closed    bool
}

// EventHub manages SSE event broadcasting to sessions.
type EventHub struct {
	mu      sync.RWMutex
	session map[*Session]struct{}
}

func NewEventHub() *EventHub {
	return &EventHub{session: make(map[*Session]struct{})}
}

func (h *EventHub) Add(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.session[s] = struct{}{}
}

func (h *EventHub) Remove(s *Session) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.session, s)
}

func (h *EventHub) Broadcast(event map[string]interface{}) {
	data, _ := json.Marshal(event)
	h.mu.RLock()
	defer h.mu.RUnlock()
	for s := range h.session {
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			continue
		}
		fmt.Fprintf(s.Writer, "event: message\ndata: %s\n\n", data)
		if s.Flusher != nil {
			s.Flusher.Flush()
		}
		s.mu.Unlock()
	}
}

// NewServer creates a new MCP server.
func NewServer() *Server {
	return &Server{
		tools:    make(map[string]ToolDefinition),
		sessions: make(map[string]*Session),
		eventHub: NewEventHub(),
	}
}

// RegisterTool adds a tool to the MCP server.
func (s *Server) RegisterTool(def ToolDefinition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tools[def.Name] = def
}

// Tools returns registered tool names.
func (s *Server) Tools() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.tools))
	for name := range s.tools {
		out = append(out, name)
	}
	return out
}

// HandleHTTP implements the MCP HTTP endpoint at /mcp.
func (s *Server) HandleHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/mcp" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.handleSSE(w, r)
	case http.MethodPost:
		s.handleJSONRPC(w, r)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		w.WriteHeader(http.StatusHTTPVersionNotSupported)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	sessionID := fmt.Sprintf("sess-%d", time.Now().UnixNano())
	session := &Session{
		ID:      sessionID,
		Server:  s,
		Writer:  w,
		Flusher: flusher,
	}
	s.mu.Lock()
	s.sessions[sessionID] = session
	s.eventHub.Add(session)
	s.mu.Unlock()

	// Keepalive ticker.
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			session.mu.Lock()
			if session.closed {
				session.mu.Unlock()
				return
			}
			fmt.Fprintf(session.Writer, ": keepalive\n\n")
			session.Flusher.Flush()
			session.mu.Unlock()
		}
	}
}

func (s *Server) handleJSONRPC(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	var req JSONRPCRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSONRPCError(w, nil, -32600, "invalid request")
		return
	}
	switch req.Method {
	case "initialize":
		writeJSONRPCResult(w, req.ID, map[string]interface{}{
			"protocolVersion": "2024-11-05",
			"capabilities": map[string]interface{}{
				"tools": map[string]interface{}{},
			},
			"serverInfo": map[string]interface{}{
				"name":    "aerollm-mcp",
				"version": "1.0.0",
			},
		})
	case "tools/list":
		var toolDefs []map[string]interface{}
		for _, tool := range s.Tools() {
			s.mu.RLock()
			def, _ := s.tools[tool]
			s.mu.RUnlock()
			toolDefs = append(toolDefs, map[string]interface{}{
				"name":        def.Name,
				"description": def.Description,
				"inputSchema": def.InputSchema,
			})
		}
		writeJSONRPCResult(w, req.ID, map[string]interface{}{"tools": toolDefs})
	case "tools/call":
		if req.Params == nil {
			writeJSONRPCError(w, req.ID, -32602, "missing params")
			return
		}
		name, _ := req.Params["name"].(string)
		args, _ := req.Params["arguments"].(map[string]interface{})
		if name == "" {
			writeJSONRPCError(w, req.ID, -32602, "missing tool name")
			return
		}
		s.mu.RLock()
		def, ok := s.tools[name]
		s.mu.RUnlock()
		if !ok {
			writeJSONRPCError(w, req.ID, -32601, fmt.Sprintf("unknown tool %s", name))
			return
		}
		res, err := def.Handler(r.Context(), args)
		if err != nil {
			writeJSONRPCError(w, req.ID, -32000, err.Error())
			return
		}
		content := []map[string]interface{}{
			{"type": "text", "text": fmt.Sprintf("%v", res)},
		}
		writeJSONRPCResult(w, req.ID, map[string]interface{}{"content": content})
	default:
		writeJSONRPCError(w, req.ID, -32601, fmt.Sprintf("unsupported method %s", req.Method))
	}
}

// JSONRPCRequest is a minimal JSON-RPC 2.0 request.
type JSONRPCRequest struct {
	JSONRPC string                 `json:"jsonrpc"`
	ID      interface{}            `json:"id,omitempty"`
	Method  string                 `json:"method"`
	Params  map[string]interface{} `json:"params,omitempty"`
}

func writeJSONRPCResult(w http.ResponseWriter, id interface{}, result map[string]interface{}) {
	writeJSONRPC(w, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"result":  result,
	})
}

func writeJSONRPCError(w http.ResponseWriter, id interface{}, code int, message string) {
	writeJSONRPC(w, map[string]interface{}{
		"jsonrpc": "2.0",
		"id":      id,
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	})
}

func writeJSONRPC(w http.ResponseWriter, payload map[string]interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(payload)
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

// SSE endpoint helper.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.HandleHTTP(w, r)
}

// Truncate long output fields safely.
func TruncateString(v string, limit int) string {
	if len(v) <= limit {
		return v
	}
	if limit <= 3 {
		return strings.Repeat(".", limit)
	}
	return v[:limit-3] + "..."
}
