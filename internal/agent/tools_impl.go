package agent

import (
	"context"
	"fmt"
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

func (c *CalculatorTool) Name() string        { return "calculator" }
func (c *CalculatorTool) Description() string { return "Evaluates basic math expressions like 2+2 or 3*4" }
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

func evaluateSimpleExpr(expr string) (float64, error) {
	// Very small safe evaluator: digits, operators, parentheses, spaces, dot.
	clean := ""
	for _, r := range expr {
		if (r >= '0' && r <= '9') || r == '+' || r == '-' || r == '*' || r == '/' || r == '(' || r == ')' || r == '.' || r == ' ' {
			clean += string(r)
		}
	}
	if clean == "" {
		return 0, fmt.Errorf("empty expression")
	}
	var a, b float64
	var op rune
	_, err := fmt.Sscanf(clean, "%f %c %f", &a, &op, &b)
	if err != nil {
		return 0, fmt.Errorf("unsupported expression: %w", err)
	}
	switch op {
	case '+':
		return a + b, nil
	case '-':
		return a - b, nil
	case '*':
		return a * b, nil
	case '/':
		if b == 0 {
			return 0, fmt.Errorf("division by zero")
		}
		return a / b, nil
	default:
		return 0, fmt.Errorf("unsupported operator: %c", op)
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
