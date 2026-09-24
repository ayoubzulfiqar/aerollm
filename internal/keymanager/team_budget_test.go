package keymanager

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"sync"
	"testing"
	"time"
)

func newTeamManager(t *testing.T, budget float64, duration string) (*Manager, *InMemoryTeamStore, string) {
	t.Helper()
	mgr := NewManager(nil, "")
	teams := NewInMemoryTeamStore()
	mgr.SetTeamStore(teams)
	team := &Team{ID: "team_a", Name: "A", Budget: budget, BudgetDuration: duration, CreatedAt: time.Now().UTC()}
	if err := teams.Create(context.Background(), team); err != nil {
		t.Fatal(err)
	}
	return mgr, teams, team.ID
}

func TestTeamBudgetErrorMatchesBudgetExceeded(t *testing.T) {
	if !errors.Is(ErrTeamBudgetExceeded, ErrBudgetExceeded) {
		t.Fatal("ErrTeamBudgetExceeded must match ErrBudgetExceeded")
	}
	if !errors.Is(ErrTeamBudgetExceeded, ErrTeamBudgetExceeded) {
		t.Fatal("ErrTeamBudgetExceeded must match itself")
	}
	if errors.Is(ErrBudgetExceeded, ErrTeamBudgetExceeded) {
		t.Fatal("a key budget error is not a team budget error")
	}
}

func TestTeamBudgetEnforcedAcrossKeys(t *testing.T) {
	mgr, _, teamID := newTeamManager(t, 1.0, "")
	ctx := context.Background()
	a, err := mgr.Generate(ctx, &GenerateRequest{TeamID: teamID})
	if err != nil {
		t.Fatal(err)
	}
	b, err := mgr.Generate(ctx, &GenerateRequest{TeamID: teamID, MaxBudget: 100})
	if err != nil {
		t.Fatal(err)
	}
	other, err := mgr.Generate(ctx, &GenerateRequest{TeamID: "team_unknown"})
	if err != nil {
		t.Fatal(err)
	}
	if err := mgr.RecordSpend(ctx, a.Key, 0.6); err != nil {
		t.Fatal(err)
	}
	if err := mgr.CheckBudget(ctx, b.Key); err != nil {
		t.Fatalf("team still has budget: %v", err)
	}
	if err := mgr.RecordSpend(ctx, b.KeyHash, 0.4); err != nil {
		t.Fatal(err)
	}
	for _, k := range []GenerateResponse{*a, *b} {
		if _, err := mgr.Validate(ctx, k.Key); !errors.Is(err, ErrBudgetExceeded) || !errors.Is(err, ErrTeamBudgetExceeded) {
			t.Fatalf("Validate(%s) = %v, want team budget exceeded", k.Prefix, err)
		}
		if err := mgr.CheckBudget(ctx, k.Key); !errors.Is(err, ErrTeamBudgetExceeded) {
			t.Fatalf("CheckBudget = %v, want team budget exceeded", err)
		}
	}
	// Keys of unknown teams have no team budget.
	if err := mgr.RecordSpend(ctx, other.Key, 50); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, other.Key); err != nil {
		t.Fatalf("unknown team must not block: %v", err)
	}
	info, err := mgr.TeamInfo(ctx, teamID)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(info.Spend-1.0) > 1e-9 {
		t.Fatalf("team spend = %v, want 1.0", info.Spend)
	}
	// Raising the budget unblocks the keys.
	if _, err := mgr.UpdateTeam(ctx, teamID, func(tm *Team) error { tm.Budget = 2; return nil }); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, a.Key); err != nil {
		t.Fatalf("budget raised: %v", err)
	}
	if err := mgr.ResetTeamSpend(ctx, teamID); err != nil {
		t.Fatal(err)
	}
	if info, _ := mgr.TeamInfo(ctx, teamID); info.Spend != 0 {
		t.Fatalf("spend after reset = %v", info.Spend)
	}
}

func TestTeamSpendConcurrentAccrualIsAtomic(t *testing.T) {
	mgr, _, teamID := newTeamManager(t, 0, "")
	ctx := context.Background()
	var keys []string
	for i := 0; i < 5; i++ {
		resp, err := mgr.Generate(ctx, &GenerateRequest{TeamID: teamID})
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, resp.Key)
	}
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			if err := mgr.RecordSpend(ctx, keys[i%len(keys)], 0.01); err != nil {
				t.Error(err)
			}
		}(i)
		go func() {
			defer wg.Done()
			// Concurrent partial updates must not clobber accrued spend.
			_, _ = mgr.UpdateTeam(ctx, teamID, func(tm *Team) error { tm.Name = "renamed"; return nil })
		}()
	}
	wg.Wait()
	info, err := mgr.TeamInfo(ctx, teamID)
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(info.Spend-2.0) > 1e-9 {
		t.Fatalf("team spend = %v, want 2.0", info.Spend)
	}
}

// nonAtomicTeamStore hides UpdateFunc so the manager's own serialization is
// exercised.
type nonAtomicTeamStore struct{ inner *InMemoryTeamStore }

func (s nonAtomicTeamStore) Create(ctx context.Context, t *Team) error { return s.inner.Create(ctx, t) }
func (s nonAtomicTeamStore) Get(ctx context.Context, id string) (*Team, error) {
	return s.inner.Get(ctx, id)
}
func (s nonAtomicTeamStore) Update(ctx context.Context, t *Team) error { return s.inner.Update(ctx, t) }

func TestTeamSpendNonAtomicStoreIsSerialized(t *testing.T) {
	mgr := NewManager(nil, "")
	ts := nonAtomicTeamStore{inner: NewInMemoryTeamStore()}
	mgr.SetTeamStore(ts)
	ctx := context.Background()
	_ = ts.Create(ctx, &Team{ID: "t1", Name: "t"})
	resp, _ := mgr.Generate(ctx, &GenerateRequest{TeamID: "t1"})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mgr.RecordSpend(ctx, resp.Key, 0.5)
		}()
	}
	wg.Wait()
	info, _ := mgr.TeamInfo(ctx, "t1")
	if info.Spend != 50 {
		t.Fatalf("team spend = %v, want 50", info.Spend)
	}
}

func TestTeamBudgetPeriodReset(t *testing.T) {
	mgr, _, teamID := newTeamManager(t, 1, "")
	now := time.Date(2026, 3, 1, 0, 0, 0, 0, time.UTC)
	mgr.now = func() time.Time { return now }
	ctx := context.Background()
	// Enable a daily team budget; the period starts at the first accrual.
	if _, err := mgr.UpdateTeam(ctx, teamID, func(tm *Team) error { tm.BudgetDuration = "1d"; return nil }); err != nil {
		t.Fatal(err)
	}
	resp, _ := mgr.Generate(ctx, &GenerateRequest{TeamID: teamID})
	if err := mgr.RecordSpend(ctx, resp.Key, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Validate(ctx, resp.Key); !errors.Is(err, ErrTeamBudgetExceeded) {
		t.Fatalf("expected team budget exceeded, got %v", err)
	}
	info, _ := mgr.TeamInfo(ctx, teamID)
	if !info.BudgetResetAt.Equal(now.Add(24 * time.Hour)) {
		t.Fatalf("reset at = %v", info.BudgetResetAt)
	}
	now = now.Add(49 * time.Hour)
	if _, err := mgr.Validate(ctx, resp.Key); err != nil {
		t.Fatalf("team budget should reset after the period: %v", err)
	}
	if info, _ := mgr.TeamInfo(ctx, teamID); info.Spend != 0 || !info.BudgetResetAt.After(now) {
		t.Fatalf("info after period: spend=%v reset=%v", info.Spend, info.BudgetResetAt)
	}
	if err := mgr.RecordSpend(ctx, resp.Key, 0.25); err != nil {
		t.Fatal(err)
	}
	info, _ = mgr.TeamInfo(ctx, teamID)
	if info.Spend != 0.25 || !info.BudgetResetAt.Equal(time.Date(2026, 3, 4, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("after reset: spend=%v reset=%v", info.Spend, info.BudgetResetAt)
	}
}

// failingTeamStore fails every lookup.
type failingTeamStore struct{ nonAtomicTeamStore }

func (failingTeamStore) Get(context.Context, string) (*Team, error) {
	return nil, errors.New("backend down")
}

func TestTeamLookupFailureFailsClosed(t *testing.T) {
	mgr := NewManager(nil, "")
	mgr.SetTeamStore(failingTeamStore{})
	ctx := context.Background()
	resp, _ := mgr.Generate(ctx, &GenerateRequest{TeamID: "t"})
	if _, err := mgr.Validate(ctx, resp.Key); err == nil {
		t.Fatal("team store failure must reject the key")
	}
	// Keys without a team are unaffected.
	solo, _ := mgr.Generate(ctx, &GenerateRequest{})
	if _, err := mgr.Validate(ctx, solo.Key); err != nil {
		t.Fatal(err)
	}
	// Spend is still recorded on the key; the team failure is reported.
	vk, err := mgr.RecordSpendByHash(ctx, resp.KeyHash, 1)
	if err == nil || vk == nil || vk.Spend != 1 {
		t.Fatalf("RecordSpendByHash = %+v, %v", vk, err)
	}
}

func TestHandlerTeamBudgetAndInfo(t *testing.T) {
	h := newTestHandler(t)
	if h.Manager.Teams() != h.Teams {
		t.Fatal("NewKeyHandler must attach its team store to the manager")
	}
	rec := do(t, h.TeamCreate, http.MethodPost, testMaster, `{"name":"Alpha","budget":1,"budget_duration":"30d","spend":99}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("team create: %d %s", rec.Code, rec.Body.String())
	}
	var team Team
	_ = json.Unmarshal(rec.Body.Bytes(), &team)
	if team.Spend != 0 || team.BudgetDuration != "30d" || team.BudgetResetAt.IsZero() {
		t.Fatalf("created team: %+v", team)
	}
	if rec := do(t, h.TeamCreate, http.MethodPost, testMaster, `{"name":"x","budget_duration":"bogus"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad budget_duration: %d", rec.Code)
	}
	admin := mustGenerate(t, h, testMaster, `{"team_id":"`+team.ID+`","role":"team_admin"}`)
	member := mustGenerate(t, h, admin.Key, `{}`)
	outsider := mustGenerate(t, h, testMaster, `{}`)

	ctx := context.Background()
	if err := h.Manager.RecordSpend(ctx, member.Key, 0.75); err != nil {
		t.Fatal(err)
	}
	rec = do(t, h.TeamInfo, http.MethodPost, member.Key, `{}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("own team info: %d %s", rec.Code, rec.Body.String())
	}
	var info struct {
		ID              string   `json:"id"`
		Spend           float64  `json:"spend"`
		Budget          float64  `json:"budget"`
		RemainingBudget *float64 `json:"remaining_budget"`
		BudgetExceeded  bool     `json:"budget_exceeded"`
		Keys            int      `json:"keys"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}
	if info.ID != team.ID || info.Spend != 0.75 || info.RemainingBudget == nil || math.Abs(*info.RemainingBudget-0.25) > 1e-9 || info.BudgetExceeded || info.Keys != 2 {
		t.Fatalf("team info: %s", rec.Body.String())
	}
	if rec := do(t, h.TeamInfo, http.MethodGet, outsider.Key, ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("outsider without team: %d", rec.Code)
	}
	if rec := do(t, h.TeamInfo, http.MethodPost, outsider.Key, `{"team_id":"`+team.ID+`"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("outsider must not see other teams: %d", rec.Code)
	}
	if rec := do(t, h.TeamInfo, http.MethodPost, testMaster, `{"team_id":"nope"}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown team: %d", rec.Code)
	}

	if err := h.Manager.RecordSpend(ctx, admin.Key, 0.25); err != nil {
		t.Fatal(err)
	}
	if rec := do(t, h.InfoKey, http.MethodGet, member.Key, ""); rec.Code != http.StatusOK {
		t.Fatalf("management API must stay reachable over team budget: %d", rec.Code)
	}
	if _, err := h.Manager.Validate(ctx, member.Key); !errors.Is(err, ErrBudgetExceeded) {
		t.Fatalf("expected team budget exceeded, got %v", err)
	}
	// Team admins cannot touch the budget or reset spend.
	for _, body := range []string{
		`{"id":"` + team.ID + `","budget":10}`,
		`{"id":"` + team.ID + `","budget_duration":"1d"}`,
		`{"id":"` + team.ID + `","reset_spend":true}`,
	} {
		if rec := do(t, h.TeamUpdate, http.MethodPost, admin.Key, body); rec.Code != http.StatusForbidden {
			t.Fatalf("team admin %s: %d", body, rec.Code)
		}
	}
	if rec := do(t, h.TeamUpdate, http.MethodPost, admin.Key, `{"id":"`+team.ID+`","name":"Renamed"}`); rec.Code != http.StatusOK {
		t.Fatalf("team admin rename: %d %s", rec.Code, rec.Body.String())
	}
	rec = do(t, h.TeamUpdate, http.MethodPost, testMaster, `{"id":"`+team.ID+`","reset_spend":true,"budget_duration":"0"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("admin reset: %d %s", rec.Code, rec.Body.String())
	}
	var updated Team
	_ = json.Unmarshal(rec.Body.Bytes(), &updated)
	if updated.Spend != 0 || updated.BudgetDuration != "" || !updated.BudgetResetAt.IsZero() || updated.Name != "Renamed" || updated.Budget != 1 {
		t.Fatalf("admin update: %+v", updated)
	}
	if _, err := h.Manager.Validate(ctx, member.Key); err != nil {
		t.Fatalf("after reset: %v", err)
	}
}
