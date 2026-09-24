package agent

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// EchoTool returns the input message.
type EchoTool struct{}

func (e *EchoTool) Name() string        { return "echo" }
func (e *EchoTool) Description() string { return "Echoes the input message back to the user" }
func (e *EchoTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"message": map[string]interface{}{
				"type":        "string",
				"description": "The message to echo",
			},
		},
		"required": []string{"message"},
	}
}
func (e *EchoTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	_ = ctx
	msg, _ := args["message"].(string)
	if msg == "" {
		msg = "(empty)"
	}
	return map[string]interface{}{"echo": msg}, nil
}

// CalculatorTool evaluates simple arithmetic expressions.
type CalculatorTool struct{}

func (c *CalculatorTool) Name() string { return "calculator" }
func (c *CalculatorTool) Description() string {
	return "Evaluates arithmetic expressions with + - * / and parentheses, e.g. (2+3)*4"
}
func (c *CalculatorTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type": "object",
		"properties": map[string]interface{}{
			"expression": map[string]interface{}{
				"type":        "string",
				"description": "Math expression, e.g. 2+2*3",
			},
		},
		"required": []string{"expression"},
	}
}
func (c *CalculatorTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	_ = ctx
	expr, _ := args["expression"].(string)
	if expr == "" {
		return nil, fmt.Errorf("expression is required")
	}
	result, err := evaluateSimpleExpr(expr)
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{"expression": expr, "result": result}, nil
}

// TimeTool returns current UTC time.
type TimeTool struct{}

func (t *TimeTool) Name() string        { return "current_time" }
func (t *TimeTool) Description() string { return "Returns the current UTC time" }
func (t *TimeTool) Parameters() map[string]interface{} {
	return map[string]interface{}{
		"type":       "object",
		"properties": map[string]interface{}{},
	}
}
func (t *TimeTool) Execute(ctx context.Context, args map[string]interface{}) (interface{}, error) {
	_ = ctx
	_ = args
	return map[string]interface{}{"utc": time.Now().UTC().Format(time.RFC3339)}, nil
}

// maxExprLen bounds calculator input; maxExprDepth bounds parenthesis nesting.
const (
	maxExprLen   = 512
	maxExprDepth = 64
)

// evaluateSimpleExpr evaluates an arithmetic expression with +, -, *, /,
// unary minus/plus, decimal numbers and parentheses, honouring the usual
// precedence. Any other character is rejected (it is never silently dropped,
// which would change the meaning of the expression).
func evaluateSimpleExpr(expr string) (float64, error) {
	if strings.TrimSpace(expr) == "" {
		return 0, fmt.Errorf("empty expression")
	}
	if len(expr) > maxExprLen {
		return 0, fmt.Errorf("expression too long (max %d characters)", maxExprLen)
	}
	p := &exprParser{s: expr}
	v, err := p.parseExpr(0)
	if err != nil {
		return 0, err
	}
	p.skipSpace()
	if p.pos != len(p.s) {
		return 0, fmt.Errorf("unexpected character %q at position %d", p.s[p.pos], p.pos)
	}
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, fmt.Errorf("result is not a finite number")
	}
	return v, nil
}

type exprParser struct {
	s   string
	pos int
}

func (p *exprParser) skipSpace() {
	for p.pos < len(p.s) && (p.s[p.pos] == ' ' || p.s[p.pos] == '\t') {
		p.pos++
	}
}

func (p *exprParser) parseExpr(depth int) (float64, error) {
	left, err := p.parseTerm(depth)
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.s) || (p.s[p.pos] != '+' && p.s[p.pos] != '-') {
			return left, nil
		}
		op := p.s[p.pos]
		p.pos++
		right, err := p.parseTerm(depth)
		if err != nil {
			return 0, err
		}
		if op == '+' {
			left += right
		} else {
			left -= right
		}
	}
}

func (p *exprParser) parseTerm(depth int) (float64, error) {
	left, err := p.parseFactor(depth)
	if err != nil {
		return 0, err
	}
	for {
		p.skipSpace()
		if p.pos >= len(p.s) || (p.s[p.pos] != '*' && p.s[p.pos] != '/') {
			return left, nil
		}
		op := p.s[p.pos]
		p.pos++
		right, err := p.parseFactor(depth)
		if err != nil {
			return 0, err
		}
		if op == '*' {
			left *= right
		} else {
			if right == 0 {
				return 0, fmt.Errorf("division by zero")
			}
			left /= right
		}
	}
}

func (p *exprParser) parseFactor(depth int) (float64, error) {
	if depth > maxExprDepth {
		return 0, fmt.Errorf("expression nested too deeply")
	}
	p.skipSpace()
	if p.pos >= len(p.s) {
		return 0, fmt.Errorf("unexpected end of expression")
	}
	switch c := p.s[p.pos]; {
	case c == '-' || c == '+':
		p.pos++
		v, err := p.parseFactor(depth + 1)
		if err != nil {
			return 0, err
		}
		if c == '-' {
			return -v, nil
		}
		return v, nil
	case c == '(':
		p.pos++
		v, err := p.parseExpr(depth + 1)
		if err != nil {
			return 0, err
		}
		p.skipSpace()
		if p.pos >= len(p.s) || p.s[p.pos] != ')' {
			return 0, fmt.Errorf("missing closing parenthesis")
		}
		p.pos++
		return v, nil
	case (c >= '0' && c <= '9') || c == '.':
		start := p.pos
		for p.pos < len(p.s) && ((p.s[p.pos] >= '0' && p.s[p.pos] <= '9') || p.s[p.pos] == '.') {
			p.pos++
		}
		v, err := strconv.ParseFloat(p.s[start:p.pos], 64)
		if err != nil {
			return 0, fmt.Errorf("invalid number %q", p.s[start:p.pos])
		}
		return v, nil
	default:
		return 0, fmt.Errorf("unexpected character %q at position %d", c, p.pos)
	}
}

// BuiltinTools returns the default built-in tools.
func BuiltinTools() []Tool {
	return []Tool{
		&EchoTool{},
		&CalculatorTool{},
		&TimeTool{},
	}
}
