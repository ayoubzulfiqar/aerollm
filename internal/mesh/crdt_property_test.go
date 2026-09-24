package mesh_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/mesh"
)

// stateCRDT is the subset of MeshState the law checks need.
type stateCRDT interface {
	Merge(ctx context.Context, remote json.RawMessage) error
	LocalSnapshot(ctx context.Context) (json.RawMessage, error)
}

// vcState adapts VectorClock (whose Merge takes a map) to stateCRDT.
type vcState struct{ v *mesh.VectorClock }

func (s vcState) Merge(_ context.Context, raw json.RawMessage) error {
	var m map[string]int64
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	s.v.Merge(m)
	return nil
}

func (s vcState) LocalSnapshot(context.Context) (json.RawMessage, error) {
	return json.Marshal(s.v.Clock())
}

func snap(t *testing.T, c stateCRDT) json.RawMessage {
	t.Helper()
	b, err := c.LocalSnapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	return b
}

func mergeInto(t *testing.T, c stateCRDT, snaps ...json.RawMessage) {
	t.Helper()
	for _, s := range snaps {
		if err := c.Merge(context.Background(), s); err != nil {
			t.Fatalf("merge %s: %v", s, err)
		}
	}
}

// join merges snaps, in order, into a fresh replica and returns its snapshot.
func join(t *testing.T, fresh func() stateCRDT, snaps ...json.RawMessage) json.RawMessage {
	t.Helper()
	c := fresh()
	mergeInto(t, c, snaps...)
	return snap(t, c)
}

func requireSame(t *testing.T, seed int64, law string, a, b json.RawMessage) {
	t.Helper()
	if !bytes.Equal(a, b) {
		t.Fatalf("seed %d: %s violated:\n  %s\n  %s", seed, law, a, b)
	}
}

// checkLaws asserts commutativity, associativity and idempotency of Merge over
// the given replica snapshots, and that every replica converges to the same
// state after merging all others in a random order.
func checkLaws(t *testing.T, seed int64, rng *rand.Rand, fresh func() stateCRDT, replicas []stateCRDT) {
	t.Helper()
	snaps := make([]json.RawMessage, len(replicas))
	for i, r := range replicas {
		snaps[i] = snap(t, r)
	}
	for trial := 0; trial < 6; trial++ {
		a := snaps[rng.Intn(len(snaps))]
		b := snaps[rng.Intn(len(snaps))]
		c := snaps[rng.Intn(len(snaps))]

		requireSame(t, seed, "commutativity", join(t, fresh, a, b), join(t, fresh, b, a))
		requireSame(t, seed, "associativity",
			join(t, fresh, join(t, fresh, a, b), c),
			join(t, fresh, a, join(t, fresh, b, c)))
		requireSame(t, seed, "idempotency (a⊔a = a)", join(t, fresh, a, a), join(t, fresh, a))
		ab := join(t, fresh, a, b)
		requireSame(t, seed, "idempotency (a⊔b⊔b = a⊔b)", join(t, fresh, ab, b), ab)
	}

	// Order independence over all snapshots.
	all := join(t, fresh, snaps...)
	for trial := 0; trial < 4; trial++ {
		perm := rng.Perm(len(snaps))
		ordered := make([]json.RawMessage, len(snaps))
		for i, p := range perm {
			ordered[i] = snaps[p]
		}
		requireSame(t, seed, "order independence", join(t, fresh, ordered...), all)
	}

	// Replica convergence: each replica merges every snapshot (including its
	// own, again) in its own random order.
	for _, r := range replicas {
		for _, p := range rng.Perm(len(snaps)) {
			mergeInto(t, r, snaps[p])
		}
	}
	for i, r := range replicas {
		requireSame(t, seed, fmt.Sprintf("convergence of replica %d", i), snap(t, r), all)
	}
}

// gossipRandomly performs a random pairwise merge between replicas so that the
// replicas share partial history before the law checks.
func gossipRandomly(t *testing.T, rng *rand.Rand, replicas []stateCRDT) {
	i, j := rng.Intn(len(replicas)), rng.Intn(len(replicas))
	mergeInto(t, replicas[i], snap(t, replicas[j]))
}

const propertySeeds = 60

func TestPNCounterCRDTLaws(t *testing.T) {
	keys := []string{"a", "b", "c"}
	for seed := int64(1); seed <= propertySeeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 3 + rng.Intn(3)
		counters := make([]*mesh.PNCounter, n)
		replicas := make([]stateCRDT, n)
		for i := range counters {
			counters[i] = mesh.NewPNCounterForNode(fmt.Sprintf("node-%d", i))
			replicas[i] = counters[i]
		}
		want := map[string]int64{}
		for step := 0; step < 80; step++ {
			c := counters[rng.Intn(n)]
			k := keys[rng.Intn(len(keys))]
			d := rng.Int63n(50) + 1
			switch rng.Intn(4) {
			case 0, 1:
				c.Increment(k, d)
				want[k] += d
			case 2:
				c.Decrement(k, d)
				want[k] -= d
			case 3:
				gossipRandomly(t, rng, replicas)
			}
		}
		checkLaws(t, seed, rng, func() stateCRDT { return mesh.NewPNCounterForNode("observer") }, replicas)
		for _, c := range counters {
			for _, k := range keys {
				if got := c.Value(k); got != want[k] {
					t.Fatalf("seed %d: key %q converged to %d, want %d", seed, k, got, want[k])
				}
			}
		}
	}
}

type regWrite struct {
	ts    int64
	node  string
	value string
}

func TestLWWRegisterCRDTLaws(t *testing.T) {
	values := []string{"", "x", "y", "zz"}
	for seed := int64(1); seed <= propertySeeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 3 + rng.Intn(3)
		regs := make([]*mesh.LWWRWRegister, n)
		replicas := make([]stateCRDT, n)
		for i := range regs {
			regs[i] = mesh.NewLWWRWRegister(fmt.Sprintf("node-%d", i))
			replicas[i] = regs[i]
		}
		var best *regWrite
		for step := 0; step < 60; step++ {
			i := rng.Intn(n)
			if rng.Intn(3) == 0 {
				gossipRandomly(t, rng, replicas)
				continue
			}
			w := regWrite{ts: rng.Int63n(5), node: fmt.Sprintf("node-%d", i), value: values[rng.Intn(len(values))]}
			regs[i].Set(w.ts, []byte(w.value))
			if best == nil || w.ts > best.ts ||
				(w.ts == best.ts && (w.node > best.node || (w.node == best.node && w.value > best.value))) {
				cp := w
				best = &cp
			}
		}
		checkLaws(t, seed, rng, func() stateCRDT { return mesh.NewLWWRWRegister("observer") }, replicas)
		if best == nil {
			continue
		}
		for _, r := range regs {
			ts, v := r.Get()
			if ts != best.ts || string(v) != best.value || r.Writer() != best.node {
				t.Fatalf("seed %d: register converged to (%d,%q,%q), want (%d,%q,%q)",
					seed, ts, r.Writer(), v, best.ts, best.node, best.value)
			}
		}
	}
}

func TestLWWElementSetCRDTLaws(t *testing.T) {
	keys := []string{"p1", "p2", "p3", "p4"}
	values := []string{"", "v1", "v2"}
	for seed := int64(1); seed <= propertySeeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 3 + rng.Intn(3)
		sets := make([]*mesh.LWWElementSet, n)
		replicas := make([]stateCRDT, n)
		for i := range sets {
			sets[i] = mesh.NewLWWElementSet(fmt.Sprintf("node-%d", i))
			replicas[i] = sets[i]
		}
		bestAdd := map[string]regWrite{}
		maxRemove := map[string]int64{}
		for step := 0; step < 80; step++ {
			i := rng.Intn(n)
			k := keys[rng.Intn(len(keys))]
			ts := rng.Int63n(6)
			switch rng.Intn(4) {
			case 0, 1:
				v := values[rng.Intn(len(values))]
				sets[i].Add(k, ts, []byte(v))
				w := regWrite{ts: ts, node: fmt.Sprintf("node-%d", i), value: v}
				cur, ok := bestAdd[k]
				if !ok || w.ts > cur.ts || (w.ts == cur.ts && (w.node > cur.node || (w.node == cur.node && w.value > cur.value))) {
					bestAdd[k] = w
				}
			case 2:
				sets[i].Remove(k, ts)
				if cur, ok := maxRemove[k]; !ok || ts > cur {
					maxRemove[k] = ts
				}
			case 3:
				gossipRandomly(t, rng, replicas)
			}
		}
		checkLaws(t, seed, rng, func() stateCRDT { return mesh.NewLWWElementSet("observer") }, replicas)

		want := map[string]string{}
		for k, a := range bestAdd {
			if rm, ok := maxRemove[k]; ok && rm > a.ts {
				continue // remove strictly newer than add
			}
			want[k] = a.value // add wins ties
		}
		for _, s := range sets {
			got := s.Elements()
			if len(got) != len(want) {
				t.Fatalf("seed %d: elements %v, want %v", seed, stringify(got), want)
			}
			for k, v := range want {
				if gv, ok := got[k]; !ok || string(gv) != v {
					t.Fatalf("seed %d: elements %v, want %v", seed, stringify(got), want)
				}
			}
		}
	}
}

func stringify(m map[string][]byte) map[string]string {
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = string(v)
	}
	return out
}

func TestVectorClockCRDTLaws(t *testing.T) {
	for seed := int64(1); seed <= propertySeeds; seed++ {
		rng := rand.New(rand.NewSource(seed))
		n := 3 + rng.Intn(3)
		clocks := make([]*mesh.VectorClock, n)
		replicas := make([]stateCRDT, n)
		for i := range clocks {
			clocks[i] = mesh.NewVectorClock()
			replicas[i] = vcState{clocks[i]}
		}
		ticks := map[string]int64{}
		for step := 0; step < 60; step++ {
			i := rng.Intn(n)
			if rng.Intn(3) == 0 {
				gossipRandomly(t, rng, replicas)
				continue
			}
			node := fmt.Sprintf("node-%d", i)
			clocks[i].Tick(node)
			ticks[node]++
		}
		checkLaws(t, seed, rng, func() stateCRDT { return vcState{mesh.NewVectorClock()} }, replicas)
		for _, c := range clocks {
			got := c.Clock()
			if len(got) != len(ticks) {
				t.Fatalf("seed %d: clock %v, want %v", seed, got, ticks)
			}
			for node, want := range ticks {
				if got[node] != want {
					t.Fatalf("seed %d: clock %v, want %v", seed, got, ticks)
				}
			}
			if c.Compare(ticks) != mesh.ClockEqual {
				t.Fatalf("seed %d: expected converged clock to equal tick totals", seed)
			}
		}
	}
}

func TestLWWElementSetSnapshotIsSorted(t *testing.T) {
	s := mesh.NewLWWElementSet("n")
	for _, k := range []string{"z", "a", "m"} {
		s.Add(k, 1, []byte(k))
		s.Remove(k, 0)
	}
	raw, err := s.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	var ops []struct {
		Key     string `json:"key"`
		Removed bool   `json:"removed"`
	}
	if err := json.Unmarshal(raw, &ops); err != nil {
		t.Fatal(err)
	}
	if !sort.SliceIsSorted(ops, func(i, j int) bool {
		if ops[i].Key != ops[j].Key {
			return ops[i].Key < ops[j].Key
		}
		return !ops[i].Removed && ops[j].Removed
	}) {
		t.Fatalf("snapshot not canonical: %s", raw)
	}
}
