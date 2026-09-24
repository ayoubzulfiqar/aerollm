package mesh_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
)

func TestPNCounterSaturatesInsteadOfOverflowing(t *testing.T) {
	c := mesh.NewPNCounterForNode("a")
	c.Increment("k", math.MaxInt64)
	c.Increment("k", math.MaxInt64)
	if got := c.Value("k"); got != math.MaxInt64 {
		t.Fatalf("expected saturation at MaxInt64, got %d", got)
	}
	c.Decrement("k", math.MaxInt64)
	c.Decrement("k", 5)
	if got := c.Value("k"); got != 0 {
		t.Fatalf("expected 0 after saturated decrement, got %d", got)
	}

	// Two nodes each at MaxInt64 must not overflow when summed.
	b := mesh.NewPNCounterForNode("b")
	b.Increment("k", math.MaxInt64)
	c2 := mesh.NewPNCounterForNode("c")
	c2.Increment("k", math.MaxInt64)
	raw, _ := b.Snapshot()
	if err := c2.Merge(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if got := c2.Value("k"); got != math.MaxInt64 {
		t.Fatalf("expected saturated sum, got %d", got)
	}
}

func TestPNCounterIgnoresNonPositiveDeltas(t *testing.T) {
	c := mesh.NewPNCounter()
	c.Increment("k", 0)
	c.Increment("k", -5)
	c.Decrement("k", -5)
	if got := c.Value("k"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestPNCounterSnapshotKeepsDecrementOnlyKeys(t *testing.T) {
	a := mesh.NewPNCounterForNode("a")
	a.Decrement("only-neg", 3)
	raw, err := a.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	b := mesh.NewPNCounterForNode("b")
	if err := b.Merge(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if got := b.Value("only-neg"); got != -3 {
		t.Fatalf("decrement-only key lost in snapshot: got %d", got)
	}
}

func TestPNCounterSameKeyDifferentNodesSum(t *testing.T) {
	// The old implementation merged by max per key, so two nodes that each
	// incremented by 5 converged to 5 instead of 10.
	a := mesh.NewPNCounterForNode("a")
	b := mesh.NewPNCounterForNode("b")
	a.Increment("k", 5)
	b.Increment("k", 5)
	ra, _ := a.Snapshot()
	rb, _ := b.Snapshot()
	if err := a.Merge(context.Background(), rb); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(context.Background(), ra); err != nil {
		t.Fatal(err)
	}
	if a.Value("k") != 10 || b.Value("k") != 10 {
		t.Fatalf("expected 10 on both replicas, got %d and %d", a.Value("k"), b.Value("k"))
	}
}

func TestPNCounterMergeRejectsBadPayloads(t *testing.T) {
	c := mesh.NewPNCounterForNode("a")
	c.Increment("k", 7)
	before, _ := c.Snapshot()

	cases := map[string]string{
		"empty":         ``,
		"not json":      `nope`,
		"negative p":    `{"k":{"n1":{"p":-1,"n":0}}}`,
		"negative n":    `{"k":{"n1":{"p":1,"n":-4}}}`,
		"empty node":    `{"k":{"":{"p":1,"n":0}}}`,
		"legacy format": `{"k":{"p":1,"n":0}}`,
		"long node":     fmt.Sprintf(`{"k":{%q:{"p":1,"n":0}}}`, strings.Repeat("x", mesh.MaxNodeIDLen+1)),
		"long key":      fmt.Sprintf(`{%q:{"n":{"p":1,"n":0}}}`, strings.Repeat("k", mesh.MaxCRDTKeyLen+1)),
		// A valid entry followed by an invalid one must not be partially applied.
		"partial": `{"a":{"n1":{"p":100,"n":0}},"b":{"n1":{"p":-1,"n":0}}}`,
	}
	for name, payload := range cases {
		if err := c.Merge(context.Background(), json.RawMessage(payload)); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
	after, _ := c.Snapshot()
	if string(before) != string(after) {
		t.Fatalf("rejected merges mutated state: %s -> %s", before, after)
	}

	huge := make([]byte, mesh.MaxMergePayloadBytes+1)
	if err := c.Merge(context.Background(), huge); !errors.Is(err, mesh.ErrPayloadTooLarge) {
		t.Fatalf("expected ErrPayloadTooLarge, got %v", err)
	}
}

func TestLWWRegisterTieBreakIsDeterministic(t *testing.T) {
	a := mesh.NewLWWRWRegister("node-a")
	b := mesh.NewLWWRWRegister("node-b")
	a.Set(5, []byte("from-a"))
	b.Set(5, []byte("from-b"))
	ra, _ := a.Snapshot()
	rb, _ := b.Snapshot()
	if err := a.Merge(context.Background(), rb); err != nil {
		t.Fatal(err)
	}
	if err := b.Merge(context.Background(), ra); err != nil {
		t.Fatal(err)
	}
	_, va := a.Get()
	_, vb := b.Get()
	if string(va) != "from-b" || string(vb) != "from-b" {
		t.Fatalf("expected higher node id to win tie on both replicas, got %q and %q", va, vb)
	}
	if a.Writer() != "node-b" {
		t.Fatalf("expected writer node-b, got %q", a.Writer())
	}
}

func TestLWWRegisterRejectsNegativeAndOlder(t *testing.T) {
	r := mesh.NewLWWRWRegister("n")
	r.Set(10, []byte("v10"))
	r.Set(3, []byte("v3"))
	r.Set(-1, []byte("neg"))
	if ts, v := r.Get(); ts != 10 || string(v) != "v10" {
		t.Fatalf("unexpected state %d %q", ts, v)
	}
	if err := r.Merge(context.Background(), json.RawMessage(`{"node":"x","timestamp":-5,"value":"eA=="}`)); err == nil {
		t.Fatal("expected negative timestamp to be rejected")
	}
}

func TestLWWRegisterGetReturnsCopy(t *testing.T) {
	r := mesh.NewLWWRWRegister("n")
	in := []byte("abc")
	r.Set(1, in)
	in[0] = 'X'
	_, v := r.Get()
	v[1] = 'Y'
	if _, v2 := r.Get(); string(v2) != "abc" {
		t.Fatalf("register aliased caller memory: %q", v2)
	}
}

func TestLWWRegisterConcurrentAccess(t *testing.T) {
	r := mesh.NewLWWRWRegister("n")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				r.Set(int64(j), []byte{byte(i)})
				_, _ = r.Get()
				raw, _ := r.Snapshot()
				_ = r.Merge(context.Background(), raw)
			}
		}(i)
	}
	wg.Wait()
	if ts, _ := r.Get(); ts != 199 {
		t.Fatalf("expected final timestamp 199, got %d", ts)
	}
}

func TestLWWElementSetTombstonesConverge(t *testing.T) {
	// Replica A removes a key it never saw added; replica B adds it with an
	// older timestamp. Both must agree the key is absent. The old snapshot
	// dropped removal-only keys, so B never learned about the removal.
	a := mesh.NewLWWElementSet("a")
	b := mesh.NewLWWElementSet("b")
	a.Remove("plugin", 10)
	b.Add("plugin", 5, []byte("v"))
	ra, _ := a.Snapshot()
	rb, _ := b.Snapshot()
	if err := b.Merge(context.Background(), ra); err != nil {
		t.Fatal(err)
	}
	if err := a.Merge(context.Background(), rb); err != nil {
		t.Fatal(err)
	}
	if _, ok := a.Lookup("plugin"); ok {
		t.Fatal("replica a should not contain removed key")
	}
	if _, ok := b.Lookup("plugin"); ok {
		t.Fatal("replica b should not contain removed key")
	}
}

func TestLWWElementSetAddWinsTie(t *testing.T) {
	s := mesh.NewLWWElementSet("n")
	s.Remove("k", 7)
	s.Add("k", 7, []byte("v"))
	if v, ok := s.Lookup("k"); !ok || string(v) != "v" {
		t.Fatalf("expected add to win tie, got %q %v", v, ok)
	}
	s.Remove("k", 8)
	if _, ok := s.Lookup("k"); ok {
		t.Fatal("expected newer remove to win")
	}
}

func TestLWWElementSetMergeRejectsBadPayloads(t *testing.T) {
	s := mesh.NewLWWElementSet("n")
	for name, payload := range map[string]string{
		"object":        `{"key":"a"}`,
		"negative ts":   `[{"key":"a","timestamp":-1}]`,
		"long key":      fmt.Sprintf(`[{"key":%q,"timestamp":1}]`, strings.Repeat("k", mesh.MaxCRDTKeyLen+1)),
		"partial apply": `[{"key":"ok","timestamp":1,"value":"eA=="},{"key":"bad","timestamp":-2}]`,
	} {
		if err := s.Merge(context.Background(), json.RawMessage(payload)); err == nil {
			t.Fatalf("%s: expected error", name)
		}
	}
	if len(s.Elements()) != 0 {
		t.Fatalf("rejected payloads mutated state: %v", s.Elements())
	}
}

func TestVectorClockCompare(t *testing.T) {
	cases := []struct {
		a, b map[string]int64
		want mesh.ClockOrdering
	}{
		{map[string]int64{}, map[string]int64{}, mesh.ClockEqual},
		{map[string]int64{"a": 1}, map[string]int64{"a": 1, "b": 0}, mesh.ClockEqual},
		{map[string]int64{"a": 1}, map[string]int64{"a": 2}, mesh.ClockBefore},
		{map[string]int64{"a": 1}, map[string]int64{"a": 1, "b": 1}, mesh.ClockBefore},
		{map[string]int64{"a": 3, "b": 1}, map[string]int64{"a": 2}, mesh.ClockAfter},
		{map[string]int64{"a": 2}, map[string]int64{"b": 1}, mesh.ClockConcurrent},
		{map[string]int64{"a": -4}, map[string]int64{}, mesh.ClockEqual},
	}
	for i, c := range cases {
		if got := mesh.CompareVectorClocks(c.a, c.b); got != c.want {
			t.Fatalf("case %d: got %v want %v", i, got, c.want)
		}
	}
	v := mesh.NewVectorClock()
	v.Tick("a")
	if v.Compare(map[string]int64{"a": 1}) != mesh.ClockEqual {
		t.Fatal("expected equal")
	}
}

func TestVectorClockMergeIgnoresNegativeAndEmpty(t *testing.T) {
	v := mesh.NewVectorClock()
	v.Tick("a")
	v.Merge(map[string]int64{"a": -10, "b": -1, "": 5, "c": 2})
	got := v.Clock()
	if len(got) != 2 || got["a"] != 1 || got["c"] != 2 {
		t.Fatalf("unexpected clock %v", got)
	}
	dst := mesh.MergeVectorClock(nil, map[string]int64{"x": -1, "y": 3})
	if len(dst) != 1 || dst["y"] != 3 {
		t.Fatalf("unexpected merged clock %v", dst)
	}
}

func TestLogicalClockMonotonic(t *testing.T) {
	prev := int64(math.MaxInt64 - 1)
	next := mesh.LogicalClock(prev)
	if next <= prev {
		t.Fatalf("expected strictly increasing, got %d after %d", next, prev)
	}
	if mesh.LogicalClock(math.MaxInt64) != math.MaxInt64 {
		t.Fatal("expected saturation at MaxInt64")
	}
}

func TestPluginRegistrySyncNilSafe(t *testing.T) {
	var p *mesh.PluginRegistrySync
	if _, err := p.LocalSnapshot(context.Background()); err == nil {
		t.Fatal("expected error for nil sync")
	}
	if err := (&mesh.PluginRegistrySync{}).Merge(context.Background(), json.RawMessage(`[]`)); err == nil {
		t.Fatal("expected error for nil registry")
	}
	s := mesh.NewPluginRegistrySync()
	s.Registry.Add("p1", 1, []byte(`{"name":"x"}`))
	raw, err := s.LocalSnapshot(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	o := mesh.NewPluginRegistrySyncForNode("other")
	if err := o.Merge(context.Background(), raw); err != nil {
		t.Fatal(err)
	}
	if _, ok := o.Registry.Lookup("p1"); !ok {
		t.Fatal("expected p1 after merge")
	}
}
