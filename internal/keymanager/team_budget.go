package keymanager

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

// Errors for users and teams. Stores return errors matching these (errors.Is)
// so callers can distinguish "missing" from backend failures.
var (
	ErrUserNotFound = errors.New("keymanager: user not found")
	ErrUserExists   = errors.New("keymanager: user already exists")
	ErrTeamNotFound = errors.New("keymanager: team not found")
	ErrTeamExists   = errors.New("keymanager: team already exists")

	// ErrConflict is returned by the Redis stores when an optimistic
	// transaction kept conflicting with concurrent writers.
	ErrConflict = errors.New("keymanager: too many concurrent update conflicts")
)

// ErrTeamBudgetExceeded is returned by Validate and CheckBudget when the
// key's team has exhausted its budget. errors.Is(err, ErrBudgetExceeded)
// also reports true for it, so existing callers treat it as an exhausted
// budget.
var ErrTeamBudgetExceeded error = teamBudgetExceededError{}

type teamBudgetExceededError struct{}

func (teamBudgetExceededError) Error() string { return "keymanager: team budget exceeded" }

// Is makes errors.Is(ErrTeamBudgetExceeded, ErrBudgetExceeded) true.
func (teamBudgetExceededError) Is(target error) bool { return target == ErrBudgetExceeded }

func cloneTeamPtr(t *Team) *Team {
	if t == nil {
		return nil
	}
	c := cloneTeam(*t)
	return &c
}

// periodDue reports whether a periodic budget must be reset at now.
func (t *Team) periodDue(now time.Time) bool {
	return t.BudgetDuration != "" && !t.BudgetResetAt.IsZero() && !now.Before(t.BudgetResetAt)
}

// EffectiveSpend returns the spend that counts against the team budget at
// time now, taking a pending budget-period reset into account.
func (t *Team) EffectiveSpend(now time.Time) float64 {
	if t == nil || t.periodDue(now) {
		return 0
	}
	return t.Spend
}

func (t *Team) overBudget(now time.Time) bool {
	return t != nil && t.Budget > 0 && t.EffectiveSpend(now) >= t.Budget
}

// BudgetExceeded reports whether the team has a budget and has spent all of
// it in the current period.
func (t *Team) BudgetExceeded() bool { return t.overBudget(time.Now()) }

func (t *Team) remaining(now time.Time) (float64, bool) {
	if t == nil || t.Budget <= 0 {
		return 0, false
	}
	return math.Max(0, t.Budget-t.EffectiveSpend(now)), true
}

// RemainingBudget returns the team's remaining budget in USD. ok is false
// when the team has no budget limit.
func (t *Team) RemainingBudget() (remaining float64, ok bool) { return t.remaining(time.Now()) }

// rollPeriod resets Spend and advances BudgetResetAt past now when the
// budget period is due. A periodic budget without a reset time is started
// at now.
func (t *Team) rollPeriod(now time.Time) {
	if t.BudgetDuration == "" {
		return
	}
	period, err := ParseDuration(t.BudgetDuration)
	if err != nil || period <= 0 {
		return
	}
	if t.BudgetResetAt.IsZero() {
		t.BudgetResetAt = now.Add(period)
		return
	}
	if now.Before(t.BudgetResetAt) {
		return
	}
	t.Spend = 0
	elapsed := now.Sub(t.BudgetResetAt)
	t.BudgetResetAt = t.BudgetResetAt.Add((elapsed/period + 1) * period)
}

// normalizeSpend makes a team copy report the state of the current period
// (for display).
func (t *Team) normalizeSpend(now time.Time) {
	if t.periodDue(now) {
		t.rollPeriod(now)
	}
}

type teamStoreRef struct{ ts TeamStore }

// SetTeamStore configures the team store used to enforce team budgets.
// Passing nil disables team budget enforcement. NewKeyHandler calls it with
// the handler's team store when the manager has none, so /team/* budgets
// are enforced by default.
func (m *Manager) SetTeamStore(ts TeamStore) {
	if ts == nil {
		m.teams.Store(nil)
		return
	}
	m.teams.Store(&teamStoreRef{ts: ts})
}

// Teams returns the team store used for team budgets (nil if none).
func (m *Manager) Teams() TeamStore {
	if ref := m.teams.Load(); ref != nil {
		return ref.ts
	}
	return nil
}

// updateTeamIn applies fn to team id of ts atomically: through
// AtomicTeamUpdater when available, otherwise serialized by the manager (so
// all updates made through this manager and its handlers are race-free).
func (m *Manager) updateTeamIn(ctx context.Context, ts TeamStore, id string, fn func(*Team) error) (*Team, error) {
	if ts == nil {
		return nil, ErrTeamNotFound
	}
	if au, ok := ts.(AtomicTeamUpdater); ok {
		return au.UpdateFunc(ctx, id, fn)
	}
	m.teamMu.Lock()
	defer m.teamMu.Unlock()
	t, err := ts.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrTeamNotFound
	}
	next := cloneTeamPtr(t)
	if err := fn(next); err != nil {
		return nil, err
	}
	next.ID, next.CreatedAt = t.ID, t.CreatedAt
	if err := ts.Update(ctx, cloneTeamPtr(next)); err != nil {
		return nil, err
	}
	return next, nil
}

// UpdateTeam atomically applies fn to a copy of the team and stores it.
func (m *Manager) UpdateTeam(ctx context.Context, id string, fn func(*Team) error) (*Team, error) {
	return m.updateTeamIn(ctx, m.Teams(), id, fn)
}

// TeamInfo returns a copy of the team whose Spend reflects the current
// budget period.
func (m *Manager) TeamInfo(ctx context.Context, id string) (*Team, error) {
	ts := m.Teams()
	if ts == nil {
		return nil, ErrTeamNotFound
	}
	t, err := ts.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if t == nil {
		return nil, ErrTeamNotFound
	}
	t = cloneTeamPtr(t)
	t.normalizeSpend(m.now())
	return t, nil
}

// ResetTeamSpend sets the team's spend in the current period to zero.
func (m *Manager) ResetTeamSpend(ctx context.Context, id string) error {
	_, err := m.UpdateTeam(ctx, id, func(t *Team) error {
		t.Spend = 0
		return nil
	})
	return err
}

// CheckTeamBudget returns ErrTeamBudgetExceeded if the team has exhausted
// its budget. Unknown teams (and managers without a team store) have no
// team budget.
func (m *Manager) CheckTeamBudget(ctx context.Context, teamID string) error {
	return m.checkTeamBudget(ctx, teamID, m.now())
}

func (m *Manager) checkTeamBudget(ctx context.Context, teamID string, now time.Time) error {
	ts := m.Teams()
	if ts == nil || teamID == "" {
		return nil
	}
	t, err := ts.Get(ctx, teamID)
	if err != nil {
		if errors.Is(err, ErrTeamNotFound) {
			return nil
		}
		return fmt.Errorf("keymanager: team lookup: %w", err)
	}
	if t.overBudget(now) {
		return ErrTeamBudgetExceeded
	}
	return nil
}

// accrueTeamSpend atomically adds usd to the team's spend (resetting the
// budget period when due). A key whose team does not exist accrues nothing.
func (m *Manager) accrueTeamSpend(ctx context.Context, teamID string, usd float64) error {
	ts := m.Teams()
	if ts == nil || teamID == "" || usd == 0 {
		return nil
	}
	_, err := m.updateTeamIn(ctx, ts, teamID, func(t *Team) error {
		t.rollPeriod(m.now().UTC())
		t.Spend += usd
		if math.IsInf(t.Spend, 0) {
			t.Spend = math.MaxFloat64
		}
		return nil
	})
	if errors.Is(err, ErrTeamNotFound) {
		return nil
	}
	return err
}
