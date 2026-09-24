// Built-in tool implementations for the AeroLLM agent loop.
//
// This package provides default tools that are available to the
// Advanced Agent Loop without requiring external provider registration.
// Tools are registered into the [agent.ToolRegistry] at startup.
//
// Available tools:
//   - calculator: Evaluate arithmetic expressions with a safe, bounded parser.
//   - get_weather: Current weather via an injectable backend. Without a
//     backend it reports that no weather data is available (it never
//     fabricates readings).
//   - get_current_time: Current time in a given IANA timezone.
//   - search: Web search via an injectable backend. Without a backend it
//     reports that search is unavailable (it never fabricates results).
//   - echo: Echo back input arguments (for debugging the agent loop).
package tools

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
	_ "time/tzdata" // embed the IANA database so timezones work in minimal containers
)

// Argument size limits (bytes).
const (
	maxExpressionLen = 1024
	maxExprDepth     = 64
	maxLocationLen   = 256
	maxQueryLen      = 1024
	maxTimezoneLen   = 64
	maxEchoLen       = 4096
)

// stringArg extracts a bounded string argument.
func stringArg(args map[string]interface{}, key string, maxLen int, required bool) (string, error) {
	v, present := args[key]
	if !present || v == nil {
		if required {
			return "", fmt.Errorf("missing '%s' argument", key)
		}
		return "", nil
	}
	s, ok := v.(string)
	if !ok {
		return "", fmt.Errorf("'%s' argument must be a string", key)
	}
	s = strings.TrimSpace(s)
	if required && s == "" {
		return "", fmt.Errorf("missing '%s' argument", key)
	}
	if len(s) > maxLen {
		return "", fmt.Errorf("'%s' argument too long (max %d bytes)", key, maxLen)
	}
	return s, nil
}

func ctxErr(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

// CalculatorTool evaluates simple arithmetic expressions.
//
// Accepts an `expression` argument (string) and returns the evaluated
// result. It uses a bounded recursive-descent parser restricted to numbers,
// + - * / % ^ (or **), unary signs and parentheses; nothing is executed.
type CalculatorTool struct{}

func (t *CalculatorTool) Name() string { return "calculator" }

func (t *CalculatorTool) Description() string {
	return "Evaluate an arithmetic expression. Supports numbers (incl. decimals and scientific notation), +, -, *, /, % (modulo), ^ or ** (power, right-associative), unary signs and parentheses."
}

func (t *CalculatorTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"expression": map[string]interface{}{
				"type":        "string",
				"description": "A valid arithmetic expression to evaluate, e.g. '(2 + 3) * 4 ^ 2'",
				"maxLength":   maxExpressionLen,
			},
		},
		"required": []string{"expression"},
	}
}

func (t *CalculatorTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	expr, err := stringArg(args, "expression", maxExpressionLen, true)
	if err != nil {
		return nil, err
	}

	result, err := evaluateExpression(expr)
	if err != nil {
		return nil, fmt.Errorf("calculator error: %w", err)
	}

	return formatNumber(result), nil
}

// formatNumber renders a float without precision loss (shortest exact form).
func formatNumber(v float64) string {
	if v == 0 {
		return "0" // also normalizes -0
	}
	if a := math.Abs(v); a >= 1e-6 && a < 1e21 {
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// evaluateExpression parses and evaluates an arithmetic expression.
// Supports +, -, *, /, % and ^ (exponent), unary signs, parentheses, and
// decimal/scientific numbers. Input length and nesting depth are bounded,
// and non-finite results are rejected.
func evaluateExpression(expr string) (float64, error) {
	if len(expr) > maxExpressionLen {
		return 0, fmt.Errorf("expression too long (max %d bytes)", maxExpressionLen)
	}
	p := &arithParser{input: expr}
	result, err := p.parseExpression()
	if err != nil {
		return 0, err
	}
	p.skipWhitespace()
	if p.pos < len(p.input) {
		return 0, fmt.Errorf("unexpected character %q at position %d", p.input[p.pos], p.pos)
	}
	return result, nil
}

// arithParser is a minimal recursive-descent arithmetic parser.
//
//	expr    := term (('+' | '-') term)*
//	term    := unary (('*' | '/' | '%') unary)*
//	unary   := ('+' | '-') unary | power
//	power   := primary (('^' | '**') unary)?      // right-associative
//	primary := number | '(' expr ')'
type arithParser struct {
	input string
	pos   int
	depth int
}

var errUnbalanced = errors.New("unbalanced parentheses")

func (p *arithParser) skipWhitespace() {
	for p.pos < len(p.input) {
		switch p.input[p.pos] {
		case ' ', '\t', '\n', '\r':
			p.pos++
		default:
			return
		}
	}
}

func (p *arithParser) peek() byte {
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

// enter/leave bound recursion depth (parentheses and unary chains).
func (p *arithParser) enter() error {
	p.depth++
	if p.depth > maxExprDepth {
		return fmt.Errorf("expression nested too deeply (max %d)", maxExprDepth)
	}
	return nil
}

func (p *arithParser) leave() { p.depth-- }

func checkFinite(v float64) (float64, error) {
	if math.IsNaN(v) {
		return 0, errors.New("result is undefined")
	}
	if math.IsInf(v, 0) {
		return 0, errors.New("result out of range")
	}
	return v, nil
}

func (p *arithParser) parseExpression() (float64, error) {
	left, err := p.parseTerm()
	if err != nil {
		return 0, err
	}
	for {
		switch p.peek() {
		case '+':
			p.pos++
			right, err := p.parseTerm()
			if err != nil {
				return 0, err
			}
			if left, err = checkFinite(left + right); err != nil {
				return 0, err
			}
		case '-':
			p.pos++
			right, err := p.parseTerm()
			if err != nil {
				return 0, err
			}
			if left, err = checkFinite(left - right); err != nil {
				return 0, err
			}
		default:
			return left, nil
		}
	}
}

func (p *arithParser) parseTerm() (float64, error) {
	left, err := p.parseUnary()
	if err != nil {
		return 0, err
	}
	for {
		c := p.peek()
		if c == '*' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '*' {
			// "**" (power) is always consumed by parsePower; reaching it
			// here would be a parser bug, so fail closed.
			return 0, fmt.Errorf("unexpected '**' at position %d", p.pos)
		}
		switch c {
		case '*':
			p.pos++
			right, err := p.parseUnary()
			if err != nil {
				return 0, err
			}
			if left, err = checkFinite(left * right); err != nil {
				return 0, err
			}
		case '/':
			p.pos++
			right, err := p.parseUnary()
			if err != nil {
				return 0, err
			}
			if right == 0 {
				return 0, errors.New("division by zero")
			}
			if left, err = checkFinite(left / right); err != nil {
				return 0, err
			}
		case '%':
			p.pos++
			right, err := p.parseUnary()
			if err != nil {
				return 0, err
			}
			if right == 0 {
				return 0, errors.New("modulo by zero")
			}
			if left, err = checkFinite(math.Mod(left, right)); err != nil {
				return 0, err
			}
		default:
			return left, nil
		}
	}
}

func (p *arithParser) parseUnary() (float64, error) {
	switch p.peek() {
	case '+', '-':
		neg := p.input[p.pos] == '-'
		p.pos++
		if err := p.enter(); err != nil {
			return 0, err
		}
		v, err := p.parseUnary()
		p.leave()
		if err != nil {
			return 0, err
		}
		if neg {
			v = -v
		}
		return v, nil
	default:
		return p.parsePower()
	}
}

func (p *arithParser) parsePower() (float64, error) {
	base, err := p.parsePrimary()
	if err != nil {
		return 0, err
	}
	c := p.peek()
	isPow := c == '^'
	if c == '*' && p.pos+1 < len(p.input) && p.input[p.pos+1] == '*' {
		isPow = true
		p.pos++ // consume first '*'
	}
	if !isPow {
		return base, nil
	}
	p.pos++
	if err := p.enter(); err != nil {
		return 0, err
	}
	exp, err := p.parseUnary() // right-associative: 2^3^2 = 2^(3^2)
	p.leave()
	if err != nil {
		return 0, err
	}
	if base == 0 && exp < 0 {
		return 0, errors.New("division by zero")
	}
	return checkFinite(math.Pow(base, exp))
}

func (p *arithParser) parsePrimary() (float64, error) {
	c := p.peek()
	if c == '(' {
		p.pos++
		if err := p.enter(); err != nil {
			return 0, err
		}
		result, err := p.parseExpression()
		p.leave()
		if err != nil {
			return 0, err
		}
		if p.peek() != ')' {
			return 0, errUnbalanced
		}
		p.pos++
		return result, nil
	}
	if c == ')' {
		return 0, fmt.Errorf("unexpected ')' at position %d", p.pos)
	}
	return p.parseNumber()
}

// parseNumber scans digits, one optional '.', and an optional exponent.
func (p *arithParser) parseNumber() (float64, error) {
	start := p.pos
	digits := 0
	for p.pos < len(p.input) && isDigit(p.input[p.pos]) {
		p.pos++
		digits++
	}
	if p.pos < len(p.input) && p.input[p.pos] == '.' {
		p.pos++
		for p.pos < len(p.input) && isDigit(p.input[p.pos]) {
			p.pos++
			digits++
		}
	}
	if digits == 0 {
		p.pos = start
		if start >= len(p.input) {
			return 0, errors.New("unexpected end of expression")
		}
		return 0, fmt.Errorf("expected number at position %d, got %q", start, p.input[start])
	}
	if p.pos < len(p.input) && (p.input[p.pos] == 'e' || p.input[p.pos] == 'E') {
		save := p.pos
		p.pos++
		if p.pos < len(p.input) && (p.input[p.pos] == '+' || p.input[p.pos] == '-') {
			p.pos++
		}
		expDigits := 0
		for p.pos < len(p.input) && isDigit(p.input[p.pos]) {
			p.pos++
			expDigits++
		}
		if expDigits == 0 {
			p.pos = save // not an exponent; leave 'e' as trailing garbage
		}
	}
	v, err := strconv.ParseFloat(p.input[start:p.pos], 64)
	if err != nil {
		var numErr *strconv.NumError
		if errors.As(err, &numErr) && errors.Is(numErr.Err, strconv.ErrRange) {
			return 0, errors.New("number out of range")
		}
		return 0, fmt.Errorf("invalid number %q", p.input[start:p.pos])
	}
	return v, nil
}

func isDigit(c byte) bool { return c >= '0' && c <= '9' }

// WeatherTool returns current weather for a location.
//
// Real data requires a backend: set Fetch to call a weather API. Without a
// backend the tool returns a result explicitly marked as unavailable
// ({"available": false, "stub": true, ...}) and never fabricates readings.
type WeatherTool struct {
	// Fetch retrieves current conditions for a location. Optional.
	Fetch func(ctx context.Context, location string) (map[string]interface{}, error)
}

func (t *WeatherTool) Name() string { return "get_weather" }

func (t *WeatherTool) Description() string {
	if t.Fetch == nil {
		return "Get the current weather for a location. NOTE: no weather backend is configured on this gateway, so this tool returns no real weather data (it reports available=false)."
	}
	return "Get the current weather for a location. Returns temperature, conditions, and humidity."
}

func (t *WeatherTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"location": map[string]interface{}{
				"type":        "string",
				"description": "The city and state, e.g. 'San Francisco, CA'",
				"maxLength":   maxLocationLen,
			},
		},
		"required": []string{"location"},
	}
}

func (t *WeatherTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	location, err := stringArg(args, "location", maxLocationLen, true)
	if err != nil {
		return nil, err
	}
	if t.Fetch == nil {
		return map[string]interface{}{
			"location":  location,
			"available": false,
			"stub":      true,
			"message":   "weather lookup is not configured on this gateway; no real weather data is available",
		}, nil
	}
	data, err := t.Fetch(ctx, location)
	if err != nil {
		return nil, fmt.Errorf("weather lookup failed: %w", err)
	}
	return data, nil
}

// CurrentTimeTool returns the current time in an IANA timezone.
type CurrentTimeTool struct {
	// Now overrides the clock (for tests). Optional.
	Now func() time.Time
}

func (t *CurrentTimeTool) Name() string { return "get_current_time" }

func (t *CurrentTimeTool) Description() string {
	return "Get the current time. Optionally specify an IANA timezone (e.g., 'Asia/Tokyo'); defaults to UTC."
}

func (t *CurrentTimeTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"timezone": map[string]interface{}{
				"type":        "string",
				"description": "IANA timezone name, e.g. 'Asia/Tokyo'. Defaults to UTC.",
				"maxLength":   maxTimezoneLen,
			},
		},
		"required": []string{},
	}
}

// validTimezoneName allows only characters that appear in IANA zone names.
func validTimezoneName(tz string) bool {
	for i := 0; i < len(tz); i++ {
		c := tz[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9',
			c == '/', c == '_', c == '-', c == '+':
		default:
			return false
		}
	}
	return !strings.Contains(tz, "..") && !strings.HasPrefix(tz, "/")
}

func (t *CurrentTimeTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	tz, err := stringArg(args, "timezone", maxTimezoneLen, false)
	if err != nil {
		return nil, err
	}
	if tz == "" {
		tz = "UTC"
	}
	// "Local" would expose the server's zone; only accept real IANA names.
	if tz == "Local" || !validTimezoneName(tz) {
		return nil, fmt.Errorf("invalid timezone %q", tz)
	}
	loc, err := time.LoadLocation(tz)
	if err != nil {
		return nil, fmt.Errorf("unknown timezone %q", tz)
	}

	now := time.Now()
	if t.Now != nil {
		now = t.Now()
	}
	local := now.In(loc)
	return map[string]interface{}{
		"timezone":   loc.String(),
		"timestamp":  local.Format(time.RFC3339),
		"utc":        now.UTC().Format(time.RFC3339),
		"unix":       now.Unix(),
		"utc_offset": local.Format("-07:00"),
	}, nil
}

// SearchTool performs a web search through an injectable backend.
//
// Without a backend it returns a result explicitly marked as unavailable
// ({"available": false, "stub": true, ...}) and never fabricates results.
type SearchTool struct {
	// Search runs the query against a real search API. Optional.
	Search func(ctx context.Context, query string) ([]map[string]interface{}, error)
}

func (t *SearchTool) Name() string { return "search" }

func (t *SearchTool) Description() string {
	if t.Search == nil {
		return "Search the web for information. NOTE: no search backend is configured on this gateway, so this tool returns no results (it reports available=false)."
	}
	return "Search the web for information. Returns top results with titles and snippets."
}

func (t *SearchTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{
				"type":        "string",
				"description": "The search query string",
				"maxLength":   maxQueryLen,
			},
		},
		"required": []string{"query"},
	}
}

func (t *SearchTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	query, err := stringArg(args, "query", maxQueryLen, true)
	if err != nil {
		return nil, err
	}
	if t.Search == nil {
		return map[string]interface{}{
			"query":     query,
			"available": false,
			"stub":      true,
			"results":   []map[string]interface{}{},
			"message":   "web search is not configured on this gateway; no search results are available",
		}, nil
	}
	results, err := t.Search(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("search failed: %w", err)
	}
	if results == nil {
		results = []map[string]interface{}{}
	}
	return map[string]interface{}{
		"query":   query,
		"results": results,
	}, nil
}

// EchoTool echoes back the input arguments.
//
// Useful for testing the agent loop without side effects.
type EchoTool struct{}

func (t *EchoTool) Name() string { return "echo" }

func (t *EchoTool) Description() string {
	return "Echo back the provided arguments. Useful for debugging."
}

func (t *EchoTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"message": map[string]interface{}{
				"type":        "string",
				"description": "The message to echo back",
				"maxLength":   maxEchoLen,
			},
		},
		"required": []string{"message"},
	}
}

func (t *EchoTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	if err := ctxErr(ctx); err != nil {
		return nil, err
	}
	v, present := args["message"]
	msg, ok := v.(string)
	if !present || !ok {
		return nil, fmt.Errorf("'message' argument must be a string")
	}
	if len(msg) > maxEchoLen {
		return nil, fmt.Errorf("'message' argument too long (max %d bytes)", maxEchoLen)
	}
	return map[string]interface{}{
		"echoed": msg,
	}, nil
}

// All returns all built-in tool instances.
//
// These can be registered individually via registry.Register(). The
// weather and search tools have no backend configured; construct them with
// Fetch/Search set to provide real data.
func All() []Tool {
	return []Tool{
		&CalculatorTool{},
		&WeatherTool{},
		&CurrentTimeTool{},
		&SearchTool{},
		&EchoTool{},
	}
}

// Tool is a local copy of the agent.Tool interface to avoid circular imports.
//
// The agent package imports tools (via these implementations), so we cannot
// import agent back into tools. Instead, we define a compatible interface here.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]interface{}
	Execute(ctx context.Context, args map[string]interface{}) (interface{}, error)
}

// RegisterAll registers all built-in tools into the provided RegisterableToolRegistry.
//
// This is provided for convenience; in most cases you will use All() and
// register each tool individually so that error handling is explicit.
func RegisterAll(registry interface {
	Register(t Tool) error
}) error {
	for _, tool := range All() {
		if err := registry.Register(tool); err != nil {
			return err
		}
	}
	return nil
}
