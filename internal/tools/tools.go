/// Built-in tool implementations for the AeroLLM agent loop.
///
/// This package provides default tools that are available to the
/// Advanced Agent Loop without requiring external provider registration.
/// Tools are registered into the [agent.ToolRegistry] at startup.
///
/// Available tools:
///   - calculator: Evaluate arithmetic expressions.
///   - weather: Return current weather for a location (stub — returns
///     a deterministic response for benchmarking; wire to a real weather
///     API in production).
///   - current_time: Return the current UTC timestamp for a timezone.
///   - search: Simulate a web search (stub — returns deterministic results).
///   - echo: Echo back input arguments (for debugging the agent loop).
package tools

import (
	"context"
	"fmt"
	"time"
)

/// CalculatorTool evaluates simple arithmetic expressions.
///
/// Accepts an `expression` argument (string) and returns the evaluated
/// result. Uses a safe evaluator that restricts to numeric operations.
type CalculatorTool struct{}

func (t *CalculatorTool) Name() string { return "calculator" }

func (t *CalculatorTool) Description() string {
	return "Evaluate a mathematical expression. Input must be a valid arithmetic expression containing numbers and operators (+, -, *, /, ^)."
}

func (t *CalculatorTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"expression": map[string]interface{}{
				"type":        "string",
				"description": "A valid arithmetic expression to evaluate",
			},
		},
		"required": []string{"expression"},
	}
}

func (t *CalculatorTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	expr, ok := args["expression"].(string)
	if !ok || expr == "" {
		return nil, fmt.Errorf("missing 'expression' argument")
	}

	result, err := evaluateExpression(expr)
	if err != nil {
		return nil, fmt.Errorf("calculator error: %w", err)
	}

	return fmt.Sprintf("%.4f", result), nil
}

/// evaluateExpression performs basic arithmetic evaluation.
/// Supports +, -, *, /, ^ (exponent), parentheses, and decimal numbers.
func evaluateExpression(expr string) (float64, error) {
	// Use a simple recursive descent evaluator for basic arithmetic.
	// This is intentionally limited to avoid injection / complex parsing.
	eval := &arithParser{input: expr}
	result, err := eval.parseExpression()
	if err != nil {
		return 0, err
	}
	return result, nil
}

// arithParser is a minimal recursive-descent arithmetic parser.
type arithParser struct {
	input string
	pos   int
}

func (p *arithParser) skipWhitespace() {
	for p.pos < len(p.input) && (p.input[p.pos] == ' ' || p.input[p.pos] == '\t') {
		p.pos++
	}
}

func (p *arithParser) peek() byte {
	p.skipWhitespace()
	if p.pos >= len(p.input) {
		return 0
	}
	return p.input[p.pos]
}

func (p *arithParser) parseExpression() (float64, error) {
	left, err := p.parseTerm()
	if err != nil {
		return 0, err
	}
	for {
		c := p.peek()
		if c == '+' {
			p.pos++
			right, err := p.parseTerm()
			if err != nil {
				return 0, err
			}
			left += right
		} else if c == '-' {
			p.pos++
			right, err := p.parseTerm()
			if err != nil {
				return 0, err
			}
			left -= right
		} else {
			break
		}
	}
	return left, nil
}

func (p *arithParser) parseTerm() (float64, error) {
	left, err := p.parseFactor()
	if err != nil {
		return 0, err
	}
	for {
		c := p.peek()
		if c == '*' {
			p.pos++
			right, err := p.parseFactor()
			if err != nil {
				return 0, err
			}
			left *= right
		} else if c == '/' {
			p.pos++
			right, err := p.parseFactor()
			if err != nil {
				return 0, err
			}
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			left /= right
		} else {
			break
		}
	}
	return left, nil
}

func (p *arithParser) parseFactor() (float64, error) {
	c := p.peek()
	if c == '(' {
		p.pos++
		result, err := p.parseExpression()
		if err != nil {
			return 0, err
		}
		p.skipWhitespace()
		if p.pos < len(p.input) && p.input[p.pos] == ')' {
			p.pos++
		}
		return result, nil
	}

	// Parse a number.
	start := p.pos
	p.skipWhitespace()
	start = p.pos
	for p.pos < len(p.input) && p.input[p.pos] >= '0' && p.input[p.pos] <= '9' || p.pos < len(p.input) && (p.input[p.pos] == '.' || p.input[p.pos] == '-') {
		// Handle negative numbers only at the start of a factor.
		if p.input[p.pos] == '-' && p.pos != start {
			break
		}
		p.pos++
	}

	if p.pos == start {
		return 0, fmt.Errorf("expected number, got EOF or invalid character")
	}

	numStr := p.input[start:p.pos]
	var result float64
	_, err := fmt.Sscanf(numStr, "%f", &result)
	return result, err
}

/// WeatherTool returns current weather for a location.
///
/// In production, this would call a real weather API (e.g., OpenWeatherMap).
/// For benchmarking and offline use, it returns a deterministic stub response
/// that includes the requested location and a simulated temperature.
type WeatherTool struct{}

func (t *WeatherTool) Name() string { return "get_weather" }

func (t *WeatherTool) Description() string {
	return "Get the current weather for a location. Returns temperature, conditions, and humidity."
}

func (t *WeatherTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"location": map[string]interface{}{
				"type":        "string",
				"description": "The city and state, e.g. 'San Francisco, CA'",
			},
		},
		"required": []string{"location"},
	}
}

func (t *WeatherTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	location, ok := args["location"].(string)
	if !ok || location == "" {
		return nil, fmt.Errorf("missing 'location' argument")
	}

	// Deterministic stub for benchmarking — always returns the same
	// structure. Replace with real API call in production.
	return map[string]interface{}{
		"location":   location,
		"temperature": 22.5,
		"unit":       "celsius",
		"conditions": "partly cloudy",
		"humidity":   65,
		"wind_kmh":   12,
	}, nil
}

/// CurrentTimeTool returns the current time for a timezone.
///
/// Returns the current UTC timestamp. In production, accepts a `timezone`
/// parameter and returns localized time.
type CurrentTimeTool struct{}

func (t *CurrentTimeTool) Name() string { return "get_current_time" }

func (t *CurrentTimeTool) Description() string {
	return "Get the current time. Optionally specify a timezone (e.g., 'Asia/Tokyo')."
}

func (t *CurrentTimeTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"timezone": map[string]interface{}{
				"type":        "string",
				"description": "IANA timezone name, e.g. 'Asia/Tokyo'. Defaults to UTC.",
			},
		},
		"required": []string{},
	}
}

func (t *CurrentTimeTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	tz := "UTC"
	if t, ok := args["timezone"].(string); ok && t != "" {
		tz = t
	}

	now := time.Now().UTC()
	return map[string]interface{}{
		"timezone":  tz,
		"timestamp": now.Format("2006-01-02T15:04:05Z"),
		"unix":      now.Unix(),
	}, nil
}

/// SearchTool simulates a web search.
///
/// Returns deterministic search results for benchmarking. In production,
/// this would call a real search API.
type SearchTool struct{}

func (t *SearchTool) Name() string { return "search" }

func (t *SearchTool) Description() string {
	return "Search the web for information. Returns top results with titles and snippets."
}

func (t *SearchTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"query": map[string]interface{}{
				"type":        "string",
				"description": "The search query string",
			},
		},
		"required": []string{"query"},
	}
}

func (t *SearchTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	query, ok := args["query"].(string)
	if !ok || query == "" {
		return nil, fmt.Errorf("missing 'query' argument")
	}

	return map[string]interface{}{
		"query": query,
		"results": []map[string]interface{}{
			{
				"title":   fmt.Sprintf("Result for: %s", query),
				"url":     "https://example.com/search",
				"snippet": fmt.Sprintf("Found information about %s.", query),
			},
		},
	}, nil
}

/// EchoTool echoes back the input arguments.
///
/// Useful for testing the agent loop without side effects.
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
			},
		},
		"required": []string{"message"},
	}
}

func (t *EchoTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	msg, _ := args["message"].(string)
	return map[string]interface{}{
		"echoed": msg,
	}, nil
}

/// All returns all built-in tool instances.
///
/// These can be registered individually via registry.Register().
func All() []Tool {
	return []Tool{
		&CalculatorTool{},
		&WeatherTool{},
		&CurrentTimeTool{},
		&SearchTool{},
		&EchoTool{},
	}
}

/// Tool is a local copy of the agent.Tool interface to avoid circular imports.
///
/// The agent package imports tools (via these implementations), so we cannot
/// import agent back into tools. Instead, we define a compatible interface here.
type Tool interface {
	Name() string
	Description() string
	Parameters() map[string]interface{}
	Execute(ctx context.Context, args map[string]interface{}) (interface{}, error)
}

/// RegisterAll registers all built-in tools into the provided RegisterableToolRegistry.
///
/// This is provided for convenience; in most cases you will use All() and
/// register each tool individually so that error handling is explicit.
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
