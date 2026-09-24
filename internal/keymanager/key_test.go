package keymanager

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestGenerateAndValidateKey tests the full key lifecycle: generate, validate, info, delete.
func TestGenerateAndValidateKey(t *testing.T) {
	store := NewInMemoryKeyStore()
	mgr := NewManager(store, "test-master-key")
	ctx := context.Background()

	req := &GenerateRequest{
		Models:    []string{"gpt-4o", "claude-3-sonnet"},
		Duration:  "24h",
		MaxBudget: 100.0,
		Metadata:  map[string]interface{}{"app": "test"},
		UserID:    "user_123",
		TeamID:    "team_abc",
	}
	resp, err := mgr.Generate(ctx, req)
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	if resp.Key == "" || resp.KeyHash == "" {
		t.Fatal("expected non-empty key and hash")
	}
	if !IsVirtualKey(resp.Key) {
		t.Fatalf("expected virtual key prefix %s, got %q", VirtualKeyPrefix, resp.Key)
	}
	if resp.Expires == "" {
		t.Fatal("expected expiry for 24h key")
	}

	vk, err := mgr.Validate(ctx, resp.Key)
	if err != nil {
		t.Fatalf("Validate failed: %v", err)
	}
	if vk.Status != StatusActive || len(vk.Models) != 2 {
		t.Fatalf("unexpected key: %+v", vk)
	}
	// Bearer prefix is tolerated.
	if _, err := mgr.Validate(ctx, "Bearer "+resp.Key); err != nil {
		t.Fatalf("Validate with Bearer prefix failed: %v", err)
	}

	info, err := mgr.Info(ctx, resp.KeyHash)
	if err != nil {
		t.Fatalf("Info failed: %v", err)
	}
	if info.MaxBudget != 100.0 {
		t.Fatalf("expected max_budget 100, got %f", info.MaxBudget)
	}

	if err := mgr.Delete(ctx, resp.KeyHash); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if _, err = mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("expected ErrKeyRevoked after deletion, got %v", err)
	}
}

func TestKeyExpiry(t *testing.T) {
	mgr := NewManager(NewInMemoryKeyStore(), "test-master-key")
	ctx := context.Background()
	resp, err := mgr.Generate(ctx, &GenerateRequest{Models: []string{"gpt-4o"}, Duration: "1ms"})
	if err != nil {
		t.Fatalf("Generate failed: %v", err)
	}
	time.Sleep(3 * time.Millisecond)
	if _, err = mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrKeyExpired) {
		t.Fatalf("expected ErrKeyExpired, got %v", err)
	}
	info, _ := mgr.Info(ctx, resp.KeyHash)
	if info.Status != StatusExpired {
		t.Fatalf("expected info status expired, got %s", info.Status)
	}
}

func TestKeyFormatEntropyAndHashOnlyStorage(t *testing.T) {
	store := NewInMemoryKeyStore()
	mgr := NewManager(store, "")
	ctx := context.Background()
	seen := map[string]bool{}
	for i := 0; i < 50; i++ {
		resp, err := mgr.Generate(ctx, &GenerateRequest{})
		if err != nil {
			t.Fatal(err)
		}
		if seen[resp.Key] {
			t.Fatal("duplicate key generated")
		}
		seen[resp.Key] = true
		// 32 random bytes -> 43 base64url characters.
		if got := len(resp.Key) - len(VirtualKeyPrefix); got != 43 {
			t.Fatalf("expected 43 random chars, got %d (%q)", got, resp.Key)
		}
		if resp.KeyHash != HashKey(resp.Key) {
			t.Fatal("key hash must be SHA-256 of the key")
		}
	}
	keys, _ := store.List(ctx)
	for _, k := range keys {
		for plain := range seen {
			if strings.Contains(k.KeyHash+k.HashedKey+k.Prefix, plain) {
				t.Fatal("plaintext key found in store")
			}
		}
		if len(k.Prefix) != prefixHintLen {
			t.Fatalf("unexpected prefix hint %q", k.Prefix)
		}
	}
}

func TestIsVirtualKeySemantics(t *testing.T) {
	cases := map[string]bool{
		"sk-demo":                   false,
		"Bearer sk-demo":            false,
		"sk-proj-abc":               false,
		"sk-aero-":                  false,
		"sk-aero-abc":               true,
		"Bearer sk-aero-abc":        true,
		"bearer   sk-aero-abc  ":    true,
		"":                          false,
		"Bearer ":                   false,
		"xsk-aero-abc":              false,
		"Bearer sk-aero-" + "x":     true,
		"Basic c2stYWVyby1hYmM=":    false,
		"sk-aero-with spaces":       true,
		"  sk-aero-leading-space":   true,
		"SK-AERO-uppercase-not-key": false,
	}
	for in, want := range cases {
		if got := IsVirtualKey(in); got != want {
			t.Errorf("IsVirtualKey(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestGenerateValidation(t *testing.T) {
	mgr := NewManager(nil, "")
	ctx := context.Background()
	bad := []*GenerateRequest{
		{Duration: "banana"},
		{Duration: "-1h"},
		{Duration: "100000d"},
		{MaxBudget: -1},
		{MaxBudget: math.NaN()},
		{MaxBudget: math.Inf(1)},
		{Models: []string{""}},
		{Models: []string{"a\x00b"}},
		{RateLimitRPS: -5},
		{RateLimitRPS: math.NaN()},
		{RateLimitTPM: -1},
		{Role: "superuser"},
		{Role: RoleTeamAdmin}, // without team
		{Metadata: map[string]interface{}{"rate_limit_rps": "fast"}},
		{BudgetDuration: "never"},
	}
	for i, req := range bad {
		if _, err := mgr.Generate(ctx, req); err == nil {
			t.Errorf("case %d: expected validation error for %+v", i, req)
		} else {
			var ve *ValidationError
			if !errors.As(err, &ve) {
				t.Errorf("case %d: expected ValidationError, got %T %v", i, err, err)
			}
		}
	}
	// Day suffix and "0" are accepted.
	resp, err := mgr.Generate(ctx, &GenerateRequest{Duration: "30d"})
	if err != nil || resp.Expires == "" {
		t.Fatalf("30d: %v %+v", err, resp)
	}
	resp, err = mgr.Generate(ctx, &GenerateRequest{Duration: "0"})
	if err != nil || resp.Expires != "" {
		t.Fatalf("0 duration should never expire: %v %+v", err, resp)
	}
}

func TestRateLimitMetadataMirroring(t *testing.T) {
	mgr := NewManager(nil, "")
	ctx := context.Background()
	resp, err := mgr.Generate(ctx, &GenerateRequest{RateLimitRPS: 5, RateLimitTPM: 1000})
	if err != nil {
		t.Fatal(err)
	}
	vk, err := mgr.Validate(ctx, resp.Key)
	if err != nil {
		t.Fatal(err)
	}
	if rps, ok := vk.Metadata["rate_limit_rps"].(float64); !ok || rps != 5 {
		t.Fatalf("rate_limit_rps metadata = %#v", vk.Metadata["rate_limit_rps"])
	}
	if tpm, ok := vk.Metadata["rate_limit_tpm"].(int); !ok || tpm != 1000 {
		t.Fatalf("rate_limit_tpm metadata = %#v", vk.Metadata["rate_limit_tpm"])
	}

	// Limits supplied only through metadata (as JSON numbers) are adopted.
	resp, err = mgr.Generate(ctx, &GenerateRequest{Metadata: map[string]interface{}{"rate_limit_rps": 2.5, "rate_limit_tpm": 500.0}})
	if err != nil {
		t.Fatal(err)
	}
	vk, _ = mgr.Validate(ctx, resp.Key)
	if vk.RateLimitRPS != 2.5 || vk.RateLimitTPM != 500 {
		t.Fatalf("expected typed limits from metadata, got %v %v", vk.RateLimitRPS, vk.RateLimitTPM)
	}
	if _, ok := vk.Metadata["rate_limit_tpm"].(int); !ok {
		t.Fatalf("tpm metadata should be normalised to int, got %T", vk.Metadata["rate_limit_tpm"])
	}
}

func TestAllowsModel(t *testing.T) {
	vk := &VirtualKey{}
	if !vk.AllowsModel("anything") {
		t.Fatal("unrestricted key should allow all models")
	}
	vk.Models = []string{"GPT-4o", "claude-3*"}
	for model, want := range map[string]bool{
		"gpt-4o":            true,
		"claude-3-sonnet":   true,
		"claude-2":          false,
		"gpt-4o-mini":       false,
		"":                  false,
		"  gpt-4o  ":        true,
		"gpt-4o\x00-hacked": false,
	} {
		if got := vk.AllowsModel(model); got != want {
			t.Errorf("AllowsModel(%q) = %v, want %v", model, got, want)
		}
	}
	vk.Models = []string{"*"}
	if !vk.AllowsModel("x") {
		t.Fatal("* should allow everything")
	}
	var nilKey *VirtualKey
	if nilKey.AllowsModel("x") {
		t.Fatal("nil key allows nothing")
	}
}

func TestSpendTrackingAndBudget(t *testing.T) {
	mgr := NewManager(nil, "")
	ctx := context.Background()
	resp, err := mgr.Generate(ctx, &GenerateRequest{MaxBudget: 1.0})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecordSpend(ctx, resp.Key, 0.4); err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecordSpend(ctx, resp.KeyHash, 0.4); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []float64{-1, math.NaN(), math.Inf(1)} {
		if err := mgr.RecordSpend(ctx, resp.Key, bad); !errors.Is(err, ErrInvalidAmount) {
			t.Fatalf("RecordSpend(%v) = %v, want ErrInvalidAmount", bad, err)
		}
	}
	if err := mgr.CheckBudget(ctx, resp.Key); err != nil {
		t.Fatalf("budget should not be exceeded yet: %v", err)
	}
	vk, err := mgr.Validate(ctx, resp.Key)
	if err != nil {
		t.Fatal(err)
	}
	if rem, ok := vk.RemainingBudget(); !ok || math.Abs(rem-0.2) > 1e-9 {
		t.Fatalf("remaining = %v %v", rem, ok)
	}
	if err := mgr.RecordSpend(ctx, resp.Key, 0.3); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CheckBudget(ctx, resp.Key); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected ErrBudgetExceeded, got %v", err)
	}
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("Validate should reject over-budget key, got %v", err)
	}
	if err := mgr.RecordSpend(ctx, "sk-aero-unknown", 1); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestBudgetPeriodReset(t *testing.T) {
	mgr := NewManager(nil, "")
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	mgr.now = func() time.Time { return now }
	ctx := context.Background()
	resp, err := mgr.Generate(ctx, &GenerateRequest{MaxBudget: 1, BudgetDuration: "1d"})
	if err != nil {
		t.Fatal(err)
	}
	_ = mgr.RecordSpend(ctx, resp.Key, 1)
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected exceeded, got %v", err)
	}
	now = now.Add(49 * time.Hour)
	vk, err := mgr.Validate(ctx, resp.Key)
	if err != nil {
		t.Fatalf("budget should reset after period: %v", err)
	}
	if vk.Spend != 0 {
		t.Fatalf("expected reset spend, got %v", vk.Spend)
	}
	vk, err = mgr.RecordSpendByHash(ctx, resp.KeyHash, 0.25)
	if err != nil {
		t.Fatal(err)
	}
	if vk.Spend != 0.25 || !vk.BudgetResetAt.After(now) {
		t.Fatalf("unexpected state after reset: spend=%v resetAt=%v now=%v", vk.Spend, vk.BudgetResetAt, now)
	}
}

func TestConcurrentSpendIsAtomic(t *testing.T) {
	mgr := NewManager(nil, "")
	ctx := context.Background()
	resp, err := mgr.Generate(ctx, &GenerateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = mgr.RecordSpend(ctx, resp.Key, 0.01)
		}()
		go func() {
			defer wg.Done()
			if vk, err := mgr.Validate(ctx, resp.Key); err == nil {
				_ = vk.Metadata["x"] // read while others write
				vk.Metadata["mutate"] = true
			}
		}()
	}
	wg.Wait()
	info, _ := mgr.Info(ctx, resp.KeyHash)
	if math.Abs(info.Spend-1.0) > 1e-9 {
		t.Fatalf("expected spend 1.0, got %v", info.Spend)
	}
	if _, ok := info.Metadata["mutate"]; ok {
		t.Fatal("mutating a validated key must not change stored state")
	}
}

func TestBlockUnblockAndRegenerate(t *testing.T) {
	mgr := NewManager(nil, "")
	ctx := context.Background()
	resp, _ := mgr.Generate(ctx, &GenerateRequest{Models: []string{"gpt-4o"}, MaxBudget: 5})
	if err := mgr.Block(ctx, resp.KeyHash); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrKeyBlocked) {
		t.Fatalf("expected ErrKeyBlocked, got %v", err)
	}
	if err := mgr.Unblock(ctx, resp.KeyHash); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, resp.Key); err != nil {
		t.Fatalf("unblocked key should validate: %v", err)
	}
	_ = mgr.RecordSpend(ctx, resp.Key, 1)
	rot, err := mgr.Regenerate(ctx, resp.KeyHash)
	if err != nil {
		t.Fatal(err)
	}
	if rot.Key == resp.Key || rot.KeyHash == resp.KeyHash {
		t.Fatal("regenerated key must differ")
	}
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrKeyRevoked) {
		t.Fatalf("old key must be revoked, got %v", err)
	}
	nk, err := mgr.Validate(ctx, rot.Key)
	if err != nil {
		t.Fatal(err)
	}
	if nk.Spend != 1 || nk.MaxBudget != 5 || !nk.AllowsModel("gpt-4o") || nk.AllowsModel("gpt-3.5") {
		t.Fatalf("settings not carried over: %+v", nk)
	}
}

func TestUpdate(t *testing.T) {
	mgr := NewManager(nil, "")
	ctx := context.Background()
	resp, _ := mgr.Generate(ctx, &GenerateRequest{RateLimitRPS: 3})
	models := []string{"gpt-4o"}
	budget := 10.0
	tpm := 999
	info, err := mgr.Update(ctx, resp.KeyHash, &UpdateRequest{Models: &models, MaxBudget: &budget, RateLimitTPM: &tpm})
	if err != nil {
		t.Fatal(err)
	}
	if info.MaxBudget != 10 || len(info.Models) != 1 || info.RateLimitTPM != 999 || info.RateLimitRPS != 3 {
		t.Fatalf("unexpected info %+v", info)
	}
	if info.Metadata["rate_limit_tpm"] != 999 || info.Metadata["rate_limit_rps"] != 3.0 {
		t.Fatalf("metadata not mirrored: %#v", info.Metadata)
	}
	neg := -1.0
	if _, err := mgr.Update(ctx, resp.KeyHash, &UpdateRequest{MaxBudget: &neg}); err == nil {
		t.Fatal("expected validation error")
	}
	if _, err := mgr.Update(ctx, strings.Repeat("0", 64), &UpdateRequest{}); !errors.Is(err, ErrKeyNotFound) {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestMasterKeyStrength(t *testing.T) {
	for _, weak := range []string{"", "default-master-key", "short", "sk-demo", "DEFAULT-MASTER-KEY"} {
		m := NewManager(nil, weak)
		if m.MasterKeyUsable() || m.IsMasterKey(weak) {
			t.Errorf("weak master key %q must not be usable", weak)
		}
	}
	m := NewManager(nil, "a-very-strong-master-key-123")
	if !m.MasterKeyUsable() || !m.IsMasterKey("Bearer a-very-strong-master-key-123") || m.IsMasterKey("a-very-strong-master-key-124") {
		t.Fatal("strong master key comparison failed")
	}
}

func TestParseKeyOrHash(t *testing.T) {
	mgr := NewManager(nil, "")
	h := strings.Repeat("ab", 32)
	if got := ParseKeyOrHash(mgr, "", strings.ToUpper(h), ""); got != h {
		t.Fatalf("hash should be normalised, got %q", got)
	}
	if got := ParseKeyOrHash(mgr, "", "not-a-hash", ""); got != "" {
		t.Fatalf("malformed hash must be rejected, got %q", got)
	}
	if got := ParseKeyOrHash(mgr, "sk-aero-x", "", ""); got != HashKey("sk-aero-x") {
		t.Fatal("key should be hashed")
	}
	if got := ParseKeyOrHash(mgr, "", "", ""); got != "" {
		t.Fatal("expected empty")
	}
}

func TestStoreReturnsCopies(t *testing.T) {
	s := NewInMemoryKeyStore()
	ctx := context.Background()
	vk := &VirtualKey{KeyHash: "h", Models: []string{"a"}, Metadata: map[string]interface{}{"k": 1}}
	if err := s.Create(ctx, vk); err != nil {
		t.Fatal(err)
	}
	vk.Models[0] = "mutated"
	got, _ := s.Get(ctx, "h")
	got.Metadata["k"] = 2
	again, _ := s.Get(ctx, "h")
	if again.Models[0] != "a" || again.Metadata["k"] != 1 {
		t.Fatal("store must not alias caller data")
	}
	if err := s.Create(ctx, vk); !errors.Is(err, ErrKeyExists) {
		t.Fatalf("expected ErrKeyExists, got %v", err)
	}
}
