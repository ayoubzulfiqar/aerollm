package v1alpha1

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

func TestValidateName(t *testing.T) {
	cases := map[string]bool{
		"r1":                     true,
		"my-route.prod":          true,
		"":                       false,
		"UPPER":                  false,
		"-lead":                  false,
		"trail-":                 false,
		"has space":              false,
		"a/b":                    false,
		strings.Repeat("a", 254): false,
		strings.Repeat("a", 253): true,
	}
	for name, ok := range cases {
		if err := ValidateName(name); (err == nil) != ok {
			t.Errorf("ValidateName(%q) err=%v, want ok=%v", name, err, ok)
		}
	}
}

func TestAeroBudgetSpecValidate(t *testing.T) {
	cases := []struct {
		name string
		spec AeroBudgetSpec
		ok   bool
		want string
	}{
		{"zero", AeroBudgetSpec{}, true, ""},
		{"valid", AeroBudgetSpec{MaxUSD: 100, MonthlyCap: 1000, AlertWebhook: "https://hooks.example.com/x"}, true, ""},
		{"negative max", AeroBudgetSpec{MaxUSD: -1}, false, "max_usd"},
		{"negative cap", AeroBudgetSpec{MonthlyCap: -0.01}, false, "monthly_cap"},
		{"nan", AeroBudgetSpec{MaxUSD: math.NaN()}, false, "finite"},
		{"inf", AeroBudgetSpec{MonthlyCap: math.Inf(1)}, false, "finite"},
		{"http webhook", AeroBudgetSpec{AlertWebhook: "http://hooks.example.com"}, false, "https"},
		{"relative webhook", AeroBudgetSpec{AlertWebhook: "/hook"}, false, "alert_webhook"},
		{"creds webhook", AeroBudgetSpec{AlertWebhook: "https://u:p@hooks.example.com"}, false, "credentials"},
		{"long key", AeroBudgetSpec{APIKey: strings.Repeat("k", MaxAPIKeyLength+1)}, false, "api_key"},
		{"secret ref", AeroBudgetSpec{APIKeySecretRef: "llm-keys/openai"}, true, ""},
		{"bad secret ref", AeroBudgetSpec{APIKeySecretRef: "../etc"}, false, "api_key_secret_ref"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("err=%v, want ok=%v", err, tc.ok)
			}
			if err != nil && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestAeroBudgetSpecReportsAllErrors(t *testing.T) {
	err := AeroBudgetSpec{MaxUSD: -1, MonthlyCap: -2, AlertWebhook: "ftp://x"}.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	for _, want := range []string{"max_usd", "monthly_cap", "alert_webhook"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestAeroRouteSpecValidate(t *testing.T) {
	cases := []struct {
		name string
		spec AeroRouteSpec
		ok   bool
	}{
		{"empty", AeroRouteSpec{}, true},
		{"valid", AeroRouteSpec{Strategy: "cost", Models: []string{"gpt-4"}, Providers: []string{"openai", "anthropic"}}, true},
		{"bad strategy", AeroRouteSpec{Strategy: "random"}, false},
		{"empty provider", AeroRouteSpec{Providers: []string{""}}, false},
		{"dup model", AeroRouteSpec{Models: []string{"a", "a"}}, false},
		{"long fallback", AeroRouteSpec{Fallback: []string{strings.Repeat("x", 129)}}, false},
		{"negative breaker", AeroRouteSpec{BreakerConfig: map[string]interface{}{"failure_threshold": -1.0}}, false},
		{"nan breaker", AeroRouteSpec{BreakerConfig: map[string]interface{}{"ratio": math.NaN()}}, false},
		{"string breaker", AeroRouteSpec{BreakerConfig: map[string]interface{}{"mode": "half-open"}}, true},
		{"weighted ok", AeroRouteSpec{Strategy: "weighted", Providers: []string{"a", "b"}, Weights: map[string]float64{"a": 3, "b": 1}}, true},
		{"weighted missing weights", AeroRouteSpec{Strategy: "weighted", Providers: []string{"a"}}, false},
		{"weight unknown provider", AeroRouteSpec{Providers: []string{"a"}, Weights: map[string]float64{"b": 1}}, false},
		{"negative weight", AeroRouteSpec{Providers: []string{"a", "b"}, Weights: map[string]float64{"a": -1, "b": 2}}, false},
		{"inf weight", AeroRouteSpec{Providers: []string{"a"}, Weights: map[string]float64{"a": math.Inf(1)}}, false},
		{"zero sum", AeroRouteSpec{Providers: []string{"a", "b"}, Weights: map[string]float64{"a": 0, "b": 0}}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.spec.Validate(); (err == nil) != tc.ok {
				t.Fatalf("err=%v, want ok=%v", err, tc.ok)
			}
		})
	}
}

func TestAeroAgentPipelineSpecValidate(t *testing.T) {
	cases := []struct {
		name string
		spec AeroAgentPipelineSpec
		ok   bool
		want string
	}{
		{"valid dag", AeroAgentPipelineSpec{Nodes: []string{"a", "b", "c"}, Edges: []string{"a->b", "b->c", "a->c"}}, true, ""},
		{"no nodes", AeroAgentPipelineSpec{}, false, "at least one node"},
		{"dup node", AeroAgentPipelineSpec{Nodes: []string{"a", "a"}}, false, "duplicate"},
		{"bad edge", AeroAgentPipelineSpec{Nodes: []string{"a", "b"}, Edges: []string{"a-b"}}, false, "form"},
		{"unknown node", AeroAgentPipelineSpec{Nodes: []string{"a"}, Edges: []string{"a->z"}}, false, "unknown node"},
		{"self loop", AeroAgentPipelineSpec{Nodes: []string{"a"}, Edges: []string{"a->a"}}, false, "self loop"},
		{"cycle", AeroAgentPipelineSpec{Nodes: []string{"a", "b", "c"}, Edges: []string{"a->b", "b->c", "c->a"}}, false, "cycle"},
		{"empty tool", AeroAgentPipelineSpec{Nodes: []string{"a"}, Tools: []string{" "}}, false, "tools"},
		{"chained arrows", AeroAgentPipelineSpec{Nodes: []string{"a", "b", "c"}, Edges: []string{"a->b->c"}}, false, "form"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.spec.Validate()
			if (err == nil) != tc.ok {
				t.Fatalf("err=%v, want ok=%v", err, tc.ok)
			}
			if err != nil && !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err=%v, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestFindCycleReportsPath(t *testing.T) {
	spec := AeroAgentPipelineSpec{Nodes: []string{"x", "a", "b", "c"}, Edges: []string{"x->a", "a->b", "b->c", "c->a"}}
	err := spec.Validate()
	if err == nil || !strings.Contains(err.Error(), "a->b->c->a") {
		t.Fatalf("expected cycle a->b->c->a, got %v", err)
	}
}

func TestLargeDAGDoesNotOverflow(t *testing.T) {
	const n = 5000
	spec := AeroAgentPipelineSpec{}
	for i := 0; i < n; i++ {
		spec.Nodes = append(spec.Nodes, "n"+strconv.Itoa(i))
		if i > 0 {
			spec.Edges = append(spec.Edges, "n"+strconv.Itoa(i-1)+"->n"+strconv.Itoa(i))
		}
	}
	if err := spec.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestObjectValidate(t *testing.T) {
	route := &AeroRoute{Kind: KindAeroRoute, Metadata: map[string]interface{}{"name": "r1"}}
	if err := route.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	wrongKind := &AeroRoute{Kind: KindAeroBudget, Metadata: map[string]interface{}{"name": "r1"}}
	if err := wrongKind.Validate(); err == nil {
		t.Fatal("expected kind mismatch error")
	}
	noName := &AeroBudget{Kind: KindAeroBudget}
	if err := noName.Validate(); err == nil || !strings.Contains(err.Error(), "metadata.name") {
		t.Fatalf("expected missing name error, got %v", err)
	}
	badSpec := &AeroAgentPipeline{Metadata: map[string]interface{}{"name": "p"}, Spec: AeroAgentPipelineSpec{}}
	if err := badSpec.Validate(); err == nil || !strings.Contains(err.Error(), "spec:") {
		t.Fatalf("expected spec error, got %v", err)
	}
	var nilRoute *AeroRoute
	if err := nilRoute.Validate(); err == nil {
		t.Fatal("expected nil object error")
	}
}
