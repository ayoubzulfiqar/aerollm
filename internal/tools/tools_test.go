package tools

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestCalculatorValid(t *testing.T) {
	cases := map[string]string{
		"1+2":               "3",
		"2 + 3 * 4":         "14",
		"(2 + 3) * 4":       "20",
		"2^3^2":             "512", // right-associative
		"2**10":             "1024",
		"-2^2":              "-4",
		"2^-1":              "0.5",
		"--3":               "3",
		"+4":                "4",
		"10 % 4":            "2",
		"7 / 2":             "3.5",
		".5 + 5.":           "5.5",
		"1e3 + 1E-3":        "1000.001",
		"0.1 + 0.2":         "0.30000000000000004",
		" ( ( 1 ) ) \t\n":   "1",
		"-0":                "0",
		"123456789 * 1000":  "123456789000",
		"(1+2)*(3+4)/(5-6)": "-21",
	}
	calc := &CalculatorTool{}
	for expr, want := range cases {
		got, err := calc.Execute(context.Background(), map[string]interface{}{"expression": expr})
		if err != nil {
			t.Errorf("%q: unexpected error %v", expr, err)
			continue
		}
		if got != want {
			t.Errorf("%q: got %v want %s", expr, got, want)
		}
	}
}

func TestCalculatorRejectsMaliciousAndInvalid(t *testing.T) {
	calc := &CalculatorTool{}
	cases := []string{
		"", "   ", "2+2abc", "()", "(1+2", "1+2)", ")", "1/0", "5 % 0", "0^-1",
		"1e999", "2^99999", "(-8)^(1/3)", "1..2", "1.2.3", "2e", "0x10", "NaN", "Inf",
		"os.exit(1)", "__import__('os')", "1;2", "2 3", "*3", "3*", "1+*2",
		"2\x00+garbage", "9e307*10", "1 ** ** 2",
		strings.Repeat("(", 100) + "1" + strings.Repeat(")", 100),
		strings.Repeat("-", 200) + "1",
		strings.Repeat("2^", 100) + "1",
		strings.Repeat("1+", 600) + "1", // > max length
	}
	for _, expr := range cases {
		if got, err := calc.Execute(context.Background(), map[string]interface{}{"expression": expr}); err == nil {
			t.Errorf("%q: expected error, got %v", expr, got)
		}
	}
	if _, err := calc.Execute(context.Background(), map[string]interface{}{"expression": 42}); err == nil {
		t.Error("expected error for non-string expression")
	}
	if _, err := calc.Execute(context.Background(), map[string]interface{}{}); err == nil {
		t.Error("expected error for missing expression")
	}
}

func TestCalculatorDepthBoundary(t *testing.T) {
	calc := &CalculatorTool{}
	ok := strings.Repeat("(", 60) + "1" + strings.Repeat(")", 60)
	if _, err := calc.Execute(context.Background(), map[string]interface{}{"expression": ok}); err != nil {
		t.Fatalf("depth 60 should be accepted: %v", err)
	}
}

func TestCalculatorCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (&CalculatorTool{}).Execute(ctx, map[string]interface{}{"expression": "1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func FuzzCalculator(f *testing.F) {
	for _, s := range []string{"1+2", "(3*4)^2", "-(-1)", "2**3", "1e5%7"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, expr string) {
		_, _ = evaluateExpression(expr) // must never panic or hang
	})
}

func TestWeatherToolHonestStub(t *testing.T) {
	w := &WeatherTool{}
	if !strings.Contains(w.Description(), "no weather backend") {
		t.Fatalf("description must disclose missing backend: %q", w.Description())
	}
	out, err := w.Execute(context.Background(), map[string]interface{}{"location": "Paris"})
	if err != nil {
		t.Fatal(err)
	}
	m := out.(map[string]interface{})
	if m["available"] != false || m["stub"] != true {
		t.Fatalf("stub result must be labeled unavailable: %v", m)
	}
	for _, k := range []string{"temperature", "conditions", "humidity"} {
		if _, ok := m[k]; ok {
			t.Fatalf("stub must not fabricate %q", k)
		}
	}
	if _, err := w.Execute(context.Background(), map[string]interface{}{"location": strings.Repeat("x", maxLocationLen+1)}); err == nil {
		t.Fatal("expected error for oversized location")
	}
	if _, err := w.Execute(context.Background(), map[string]interface{}{}); err == nil {
		t.Fatal("expected error for missing location")
	}
}

func TestWeatherToolBackend(t *testing.T) {
	w := &WeatherTool{Fetch: func(ctx context.Context, loc string) (map[string]interface{}, error) {
		return map[string]interface{}{"location": loc, "temperature": 18.0}, nil
	}}
	out, err := w.Execute(context.Background(), map[string]interface{}{"location": "Oslo"})
	if err != nil || out.(map[string]interface{})["temperature"] != 18.0 {
		t.Fatalf("expected backend data, got %v %v", out, err)
	}
	w.Fetch = func(ctx context.Context, loc string) (map[string]interface{}, error) {
		return nil, errors.New("boom")
	}
	if _, err := w.Execute(context.Background(), map[string]interface{}{"location": "Oslo"}); err == nil {
		t.Fatal("expected backend error to propagate")
	}
}

func TestSearchToolHonestStubAndBackend(t *testing.T) {
	s := &SearchTool{}
	if !strings.Contains(s.Description(), "no search backend") {
		t.Fatalf("description must disclose missing backend: %q", s.Description())
	}
	out, err := s.Execute(context.Background(), map[string]interface{}{"query": "golang"})
	if err != nil {
		t.Fatal(err)
	}
	m := out.(map[string]interface{})
	if m["available"] != false || len(m["results"].([]map[string]interface{})) != 0 {
		t.Fatalf("stub must return no results: %v", m)
	}
	s.Search = func(ctx context.Context, q string) ([]map[string]interface{}, error) {
		return []map[string]interface{}{{"title": q}}, nil
	}
	out, err = s.Execute(context.Background(), map[string]interface{}{"query": "golang"})
	if err != nil || len(out.(map[string]interface{})["results"].([]map[string]interface{})) != 1 {
		t.Fatalf("expected backend results, got %v %v", out, err)
	}
	if _, err := s.Execute(context.Background(), map[string]interface{}{"query": strings.Repeat("q", maxQueryLen+1)}); err == nil {
		t.Fatal("expected error for oversized query")
	}
}

func TestCurrentTimeTool(t *testing.T) {
	fixed := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	tool := &CurrentTimeTool{Now: func() time.Time { return fixed }}

	out, err := tool.Execute(context.Background(), map[string]interface{}{"timezone": "Asia/Tokyo"})
	if err != nil {
		t.Fatal(err)
	}
	m := out.(map[string]interface{})
	if m["timestamp"] != "2026-01-02T12:04:05+09:00" || m["utc_offset"] != "+09:00" || m["unix"] != fixed.Unix() {
		t.Fatalf("unexpected result: %v", m)
	}
	out, err = tool.Execute(context.Background(), map[string]interface{}{})
	if err != nil || out.(map[string]interface{})["timestamp"] != "2026-01-02T03:04:05Z" {
		t.Fatalf("expected UTC default, got %v %v", out, err)
	}
	for _, bad := range []string{"Mars/Olympus", "../../etc/passwd", "/etc/localtime", "Local", "Asia/Tokyo;rm", strings.Repeat("A", 100)} {
		if _, err := tool.Execute(context.Background(), map[string]interface{}{"timezone": bad}); err == nil {
			t.Errorf("expected error for timezone %q", bad)
		}
	}
}

func TestEchoTool(t *testing.T) {
	e := &EchoTool{}
	out, err := e.Execute(context.Background(), map[string]interface{}{"message": "hi"})
	if err != nil || out.(map[string]interface{})["echoed"] != "hi" {
		t.Fatalf("unexpected: %v %v", out, err)
	}
	if _, err := e.Execute(context.Background(), map[string]interface{}{"message": strings.Repeat("x", maxEchoLen+1)}); err == nil {
		t.Fatal("expected error for oversized message")
	}
	if _, err := e.Execute(context.Background(), map[string]interface{}{"message": 5}); err == nil {
		t.Fatal("expected error for non-string message")
	}
}

type fakeRegistry struct{ names []string }

func (f *fakeRegistry) Register(t Tool) error {
	f.names = append(f.names, t.Name())
	return nil
}

func TestAllAndRegisterAll(t *testing.T) {
	reg := &fakeRegistry{}
	if err := RegisterAll(reg); err != nil {
		t.Fatal(err)
	}
	want := []string{"calculator", "get_weather", "get_current_time", "search", "echo"}
	if strings.Join(reg.names, ",") != strings.Join(want, ",") {
		t.Fatalf("got %v want %v", reg.names, want)
	}
}
