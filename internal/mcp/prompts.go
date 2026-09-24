package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// Prompt message roles.
const (
	RoleUser      = "user"
	RoleAssistant = "assistant"
)

var (
	promptNamePattern = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)
	promptArgPattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)
	// placeholderPattern matches {{name}} (whitespace inside braces allowed).
	placeholderPattern = regexp.MustCompile(`\{\{\s*([A-Za-z0-9_.-]{1,64})\s*\}\}`)
)

// PromptArgument describes an argument of a prompt template.
type PromptArgument struct {
	Name        string `json:"name"`
	Title       string `json:"title,omitempty"`
	Description string `json:"description,omitempty"`
	Required    bool   `json:"required,omitempty"`
}

// PromptMessage is a text message of a prompt. In PromptDefinition.Messages
// the text is a template: every {{name}} placeholder naming a declared
// argument is replaced by the argument's value (empty when an optional
// argument is omitted). Substitution is single-pass, so argument values are
// never themselves expanded; undeclared placeholders are left untouched.
type PromptMessage struct {
	// Role is RoleUser or RoleAssistant.
	Role string
	Text string
}

// PromptHandler renders a prompt dynamically. args contains only declared
// arguments, with every required argument present.
type PromptHandler func(ctx context.Context, args map[string]string) ([]PromptMessage, error)

// PromptDefinition is a prompt exposed through prompts/list and prompts/get.
type PromptDefinition struct {
	Name        string
	Title       string
	Description string
	Arguments   []PromptArgument
	// Messages are the templates rendered by prompts/get.
	Messages []PromptMessage
	// Handler, when set, renders the prompt instead of Messages.
	Handler PromptHandler
}

// RegisterPrompt adds a prompt, ignoring invalid definitions; use AddPrompt
// to get the validation error.
func (s *Server) RegisterPrompt(def PromptDefinition) {
	_ = s.AddPrompt(def)
}

// AddPrompt adds (or replaces, by name) a prompt. The name and argument
// names must match [A-Za-z0-9_.-]; argument names must be unique; message
// roles must be "user" or "assistant"; and the prompt needs Messages or a
// Handler.
func (s *Server) AddPrompt(def PromptDefinition) error {
	if !promptNamePattern.MatchString(def.Name) {
		return fmt.Errorf("mcp: invalid prompt name %q", TruncateString(def.Name, 128))
	}
	if def.Handler == nil && len(def.Messages) == 0 {
		return fmt.Errorf("mcp: prompt %q has no messages or handler", def.Name)
	}
	seen := make(map[string]bool, len(def.Arguments))
	for _, a := range def.Arguments {
		if !promptArgPattern.MatchString(a.Name) {
			return fmt.Errorf("mcp: prompt %q: invalid argument name %q", def.Name, TruncateString(a.Name, 64))
		}
		if seen[a.Name] {
			return fmt.Errorf("mcp: prompt %q: duplicate argument %q", def.Name, a.Name)
		}
		seen[a.Name] = true
	}
	for _, m := range def.Messages {
		if m.Role != RoleUser && m.Role != RoleAssistant {
			return fmt.Errorf("mcp: prompt %q: invalid message role %q", def.Name, TruncateString(m.Role, 32))
		}
	}
	def.Arguments = append([]PromptArgument(nil), def.Arguments...)
	def.Messages = append([]PromptMessage(nil), def.Messages...)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.prompts == nil {
		s.prompts = make(map[string]PromptDefinition)
	}
	s.prompts[def.Name] = def
	return nil
}

// RemovePrompt removes a prompt and reports whether it existed.
func (s *Server) RemovePrompt(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.prompts[name]
	delete(s.prompts, name)
	return ok
}

// Prompts returns the names of the registered prompts, sorted.
func (s *Server) Prompts() []string {
	defs := s.listPromptDefs()
	out := make([]string, len(defs))
	for i, d := range defs {
		out[i] = d.Name
	}
	return out
}

func (s *Server) listPromptDefs() []PromptDefinition {
	s.mu.RLock()
	out := make([]PromptDefinition, 0, len(s.prompts))
	for _, d := range s.prompts {
		out = append(out, d)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (s *Server) hasPrompts() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.prompts) > 0
}

// listPrompts handles prompts/list.
func (s *Server) listPrompts(params json.RawMessage) (interface{}, *rpcError) {
	var p struct {
		Cursor *string `json:"cursor"`
	}
	if e := decodeParams(params, &p, false); e != nil {
		return nil, e
	}
	defs := s.listPromptDefs()
	start, end, next, perr := paginate(len(defs), p.Cursor, s.pageSize())
	if perr != nil {
		return nil, perr
	}
	prompts := make([]map[string]interface{}, 0, end-start)
	for _, d := range defs[start:end] {
		entry := map[string]interface{}{"name": d.Name}
		if d.Title != "" {
			entry["title"] = d.Title
		}
		if d.Description != "" {
			entry["description"] = d.Description
		}
		if len(d.Arguments) > 0 {
			entry["arguments"] = d.Arguments
		}
		prompts = append(prompts, entry)
	}
	out := map[string]interface{}{"prompts": prompts}
	if next != "" {
		out["nextCursor"] = next
	}
	return out, nil
}

// getPrompt handles prompts/get.
func (s *Server) getPrompt(ctx context.Context, params json.RawMessage) (interface{}, *rpcError) {
	var p struct {
		Name      *string                    `json:"name"`
		Arguments map[string]json.RawMessage `json:"arguments"`
	}
	if e := decodeParams(params, &p, true); e != nil {
		return nil, e
	}
	if p.Name == nil || *p.Name == "" {
		return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: prompt name is required"}
	}
	s.mu.RLock()
	def, ok := s.prompts[*p.Name]
	s.mu.RUnlock()
	if !ok {
		return nil, &rpcError{Code: CodeInvalidParams, Message: "unknown prompt: " + TruncateString(*p.Name, 128)}
	}

	declared := make(map[string]bool, len(def.Arguments))
	for _, a := range def.Arguments {
		declared[a.Name] = true
	}
	args := make(map[string]string, len(def.Arguments))
	for name, raw := range p.Arguments {
		var v string
		if err := json.Unmarshal(raw, &v); err != nil {
			return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: argument values must be strings"}
		}
		if declared[name] { // undeclared arguments are ignored
			args[name] = v
		}
	}
	for _, a := range def.Arguments {
		if _, ok := args[a.Name]; a.Required && !ok {
			return nil, &rpcError{Code: CodeInvalidParams, Message: "invalid params: missing required argument: " + a.Name}
		}
	}

	var msgs []PromptMessage
	if def.Handler != nil {
		err := s.callProvider(ctx, func(ctx context.Context) error {
			var err error
			msgs, err = def.Handler(ctx, args)
			return err
		})
		if err != nil {
			return nil, &rpcError{Code: CodeInternalError, Message: "failed to render prompt"}
		}
	} else {
		msgs = make([]PromptMessage, len(def.Messages))
		for i, m := range def.Messages {
			msgs[i] = PromptMessage{Role: m.Role, Text: substitute(m.Text, declared, args)}
		}
	}

	limit := s.MaxToolOutputBytes
	if limit <= 0 {
		limit = DefaultMaxToolOutputBytes
	}
	out := make([]map[string]interface{}, 0, len(msgs))
	total := 0
	for _, m := range msgs {
		if m.Role != RoleUser && m.Role != RoleAssistant {
			return nil, &rpcError{Code: CodeInternalError, Message: "failed to render prompt"}
		}
		total += len(m.Text)
		if total > limit {
			return nil, &rpcError{Code: CodeInternalError, Message: "rendered prompt too large"}
		}
		out = append(out, map[string]interface{}{
			"role":    m.Role,
			"content": map[string]interface{}{"type": "text", "text": m.Text},
		})
	}
	result := map[string]interface{}{"messages": out}
	if def.Description != "" {
		result["description"] = def.Description
	}
	return result, nil
}

// substitute replaces {{name}} placeholders of declared arguments in one pass.
func substitute(tmpl string, declared map[string]bool, args map[string]string) string {
	if !strings.Contains(tmpl, "{{") {
		return tmpl
	}
	return placeholderPattern.ReplaceAllStringFunc(tmpl, func(match string) string {
		name := placeholderPattern.FindStringSubmatch(match)[1]
		if !declared[name] {
			return match
		}
		return args[name]
	})
}

var errProviderPanicked = errors.New("mcp: provider panicked")
