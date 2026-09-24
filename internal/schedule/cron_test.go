package schedule

import (
	"strings"
	"testing"
	"time"
	_ "time/tzdata" // DST tests must not depend on the host's zoneinfo
)

func utc(y int, mo time.Month, d, h, mi int) time.Time {
	return time.Date(y, mo, d, h, mi, 0, 0, time.UTC)
}

func mustCron(t *testing.T, expr string) *CronSchedule {
	t.Helper()
	cs, err := ParseCron(expr)
	if err != nil {
		t.Fatalf("ParseCron(%q): %v", expr, err)
	}
	return cs
}

func TestParseCronErrors(t *testing.T) {
	cases := []string{
		"",
		"   ",
		"* * * *",
		"* * * * * *",
		"60 * * * *",
		"* 24 * * *",
		"* * 0 * *",
		"* * 32 * *",
		"* * * 0 *",
		"* * * 13 *",
		"* * * * 8",
		"*/0 * * * *",
		"*/61 * * * *",
		"5-1 * * * *",
		"1,,2 * * * *",
		",1 * * * *",
		"a * * * *",
		"-1 * * * *",
		"+5 * * * *",
		"1- * * * *",
		"1-2-3 * * * *",
		"* * * FOO *",
		"* * * * FUNDAY",
		"? * * * *",
		"* ? * * *",
		"1/ * * * *",
		"*/x * * * *",
		"@reboot",
		"@every",
		"@every 0s",
		"@every 500ms",
		"@every 400d",
		"@every banana",
		"0 0 1 1 * " + strings.Repeat("x", 300),
	}
	for _, expr := range cases {
		if _, err := ParseCron(expr); err == nil {
			t.Errorf("ParseCron(%q): expected error", expr)
		}
	}
}

func TestCronNext(t *testing.T) {
	cases := []struct {
		name  string
		expr  string
		after time.Time
		want  []time.Time // successive activations
	}{
		{"every minute truncates seconds", "* * * * *",
			time.Date(2026, 1, 1, 10, 0, 30, 5, time.UTC),
			[]time.Time{utc(2026, 1, 1, 10, 1), utc(2026, 1, 1, 10, 2)}},
		{"step */15", "*/15 * * * *", utc(2026, 1, 1, 10, 7),
			[]time.Time{utc(2026, 1, 1, 10, 15), utc(2026, 1, 1, 10, 30), utc(2026, 1, 1, 10, 45), utc(2026, 1, 1, 11, 0)}},
		{"range with step", "0 9-17/4 * * *", utc(2026, 1, 1, 0, 0),
			[]time.Time{utc(2026, 1, 1, 9, 0), utc(2026, 1, 1, 13, 0), utc(2026, 1, 1, 17, 0), utc(2026, 1, 2, 9, 0)}},
		{"start with step", "5/20 * * * *", utc(2026, 1, 1, 0, 0),
			[]time.Time{utc(2026, 1, 1, 0, 5), utc(2026, 1, 1, 0, 25), utc(2026, 1, 1, 0, 45), utc(2026, 1, 1, 1, 5)}},
		{"list", "0 0 1,15 * *", utc(2026, 1, 1, 0, 0),
			[]time.Time{utc(2026, 1, 15, 0, 0), utc(2026, 2, 1, 0, 0), utc(2026, 2, 15, 0, 0)}},
		{"names and dow range (AND with star dom)", "0 12 * JAN,jul MON-fri", utc(2026, 1, 30, 13, 0),
			// 2026-01-30 is a Friday; next weekday in Jan/Jul is 2026-07-01 (Wed).
			[]time.Time{utc(2026, 7, 1, 12, 0), utc(2026, 7, 2, 12, 0), utc(2026, 7, 3, 12, 0), utc(2026, 7, 6, 12, 0)}},
		{"dow 7 is sunday", "0 0 * * 7", utc(2026, 1, 1, 0, 0),
			[]time.Time{utc(2026, 1, 4, 0, 0), utc(2026, 1, 11, 0, 0)}},
		{"dow range through 7", "0 0 * * 5-7", utc(2026, 1, 1, 0, 0),
			[]time.Time{utc(2026, 1, 2, 0, 0), utc(2026, 1, 3, 0, 0), utc(2026, 1, 4, 0, 0), utc(2026, 1, 9, 0, 0)}},
		{"question mark", "0 0 ? * MON", utc(2026, 1, 1, 0, 0),
			[]time.Time{utc(2026, 1, 5, 0, 0), utc(2026, 1, 12, 0, 0)}},
		{"month end 31st skips short months", "0 0 31 * *", utc(2026, 1, 31, 0, 0),
			[]time.Time{utc(2026, 3, 31, 0, 0), utc(2026, 5, 31, 0, 0), utc(2026, 7, 31, 0, 0), utc(2026, 8, 31, 0, 0)}},
		{"feb 29 next leap year", "0 0 29 2 *", utc(2025, 1, 1, 0, 0),
			[]time.Time{utc(2028, 2, 29, 0, 0), utc(2032, 2, 29, 0, 0)}},
		{"feb 29 across non-leap century", "0 0 29 2 *", utc(2096, 3, 1, 0, 0),
			[]time.Time{utc(2104, 2, 29, 0, 0)}},
		{"feb 29 leap century 2000", "0 0 29 2 *", utc(1997, 1, 1, 0, 0),
			[]time.Time{utc(2000, 2, 29, 0, 0)}},
		{"dom/dow OR semantics", "0 0 13 * FRI", utc(2026, 1, 1, 0, 0),
			// 2026-01-02, -09, -16 are Fridays; the 13th is a Tuesday.
			[]time.Time{utc(2026, 1, 2, 0, 0), utc(2026, 1, 9, 0, 0), utc(2026, 1, 13, 0, 0), utc(2026, 1, 16, 0, 0)}},
		{"stepped star dom keeps AND semantics", "0 0 */2 * FRI", utc(2026, 1, 1, 0, 0),
			// Odd days that are Fridays: Jan 9, Jan 23.
			[]time.Time{utc(2026, 1, 9, 0, 0), utc(2026, 1, 23, 0, 0)}},
		{"year rollover", "59 23 31 12 *", utc(2026, 12, 31, 23, 59),
			[]time.Time{utc(2027, 12, 31, 23, 59)}},
		{"@hourly", "@hourly", utc(2026, 1, 1, 10, 0), []time.Time{utc(2026, 1, 1, 11, 0)}},
		{"@daily", "@daily", utc(2026, 1, 1, 10, 0), []time.Time{utc(2026, 1, 2, 0, 0)}},
		{"@midnight", "@MIDNIGHT", utc(2026, 1, 1, 10, 0), []time.Time{utc(2026, 1, 2, 0, 0)}},
		{"@weekly", "@weekly", utc(2026, 1, 1, 10, 0), []time.Time{utc(2026, 1, 4, 0, 0)}},
		{"@monthly", "@monthly", utc(2026, 1, 1, 10, 0), []time.Time{utc(2026, 2, 1, 0, 0)}},
		{"@yearly", "@yearly", utc(2026, 1, 1, 10, 0), []time.Time{utc(2027, 1, 1, 0, 0)}},
		{"@annually", "@annually", utc(2026, 1, 1, 0, 0), []time.Time{utc(2027, 1, 1, 0, 0)}},
		{"@every", "@every 90s", utc(2026, 1, 1, 0, 0),
			[]time.Time{time.Date(2026, 1, 1, 0, 1, 30, 0, time.UTC), utc(2026, 1, 1, 0, 3)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cs := mustCron(t, tc.expr)
			cur := tc.after
			for i, want := range tc.want {
				got := cs.Next(cur)
				if !got.Equal(want) {
					t.Fatalf("activation %d after %s: got %s, want %s", i, cur, got, want)
				}
				cur = got
			}
		})
	}
}

func TestCronNeverFires(t *testing.T) {
	for _, expr := range []string{"0 0 30 2 *", "0 0 31 4 *", "0 0 31 6,9,11 *"} {
		if got := mustCron(t, expr).Next(utc(2026, 1, 1, 0, 0)); !got.IsZero() {
			t.Errorf("%q: expected zero time, got %s", expr, got)
		}
	}
	var nilSched *CronSchedule
	if !nilSched.Next(time.Now()).IsZero() {
		t.Fatal("nil schedule must never fire")
	}
}

func TestCronUsesArgumentLocationByDefault(t *testing.T) {
	loc := time.FixedZone("UTC+5", 5*3600)
	got := mustCron(t, "0 9 * * *").Next(time.Date(2026, 1, 1, 10, 0, 0, 0, loc))
	want := time.Date(2026, 1, 2, 9, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestCronDSTSpringForward(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	cs, err := ParseCronInLocation("30 2 * * *", ny)
	if err != nil {
		t.Fatal(err)
	}
	// 2026-03-08 02:00 EST jumps to 03:00 EDT, so 02:30 does not exist.
	got := cs.Next(time.Date(2026, 3, 8, 0, 0, 0, 0, ny))
	want := time.Date(2026, 3, 9, 2, 30, 0, 0, ny)
	if !got.Equal(want) {
		t.Fatalf("got %s, want %s", got, want)
	}

	// Every-minute schedules skip the missing hour without firing twice.
	every := mustCron(t, "* * * * *")
	before := time.Date(2026, 3, 8, 1, 59, 0, 0, ny)
	next := every.Next(before)
	if next.Sub(before) != time.Minute || next.Hour() != 3 || next.Minute() != 0 {
		t.Fatalf("expected 03:00 EDT one real minute later, got %s", next)
	}
}

func TestCronDSTFallBack(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	cs, err := ParseCronInLocation("30 1 * * *", ny)
	if err != nil {
		t.Fatal(err)
	}
	// 2026-11-01 01:00-01:59 occurs twice (EDT then EST).
	first := cs.Next(time.Date(2026, 11, 1, 0, 0, 0, 0, ny))
	if want := time.Date(2026, 11, 1, 5, 30, 0, 0, time.UTC); !first.Equal(want) { // 01:30 EDT
		t.Fatalf("first: got %s, want %s", first.UTC(), want)
	}
	second := cs.Next(first)
	if want := time.Date(2026, 11, 2, 6, 30, 0, 0, time.UTC); !second.Equal(want) { // next day 01:30 EST
		t.Fatalf("second: got %s, want %s (must not fire again at 01:30 EST)", second.UTC(), want)
	}

	// Wildcard minute schedule keeps its real-time cadence: after 01:59 EDT
	// the next activation is 01:00 EST, one real minute later.
	every := mustCron(t, "* * * * *")
	lastEDT := time.Date(2026, 11, 1, 5, 59, 0, 0, time.UTC).In(ny)
	if got, want := every.Next(lastEDT), time.Date(2026, 11, 1, 6, 0, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got.UTC(), want)
	}

	// Starting inside the repeated (EST) hour still finds the later occurrence.
	inSecondPass := time.Date(2026, 11, 1, 6, 10, 0, 0, time.UTC).In(ny) // 01:10 EST
	if got, want := every.Next(inSecondPass), time.Date(2026, 11, 1, 6, 11, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("got %s, want %s", got.UTC(), want)
	}
}

func TestParseInterval(t *testing.T) {
	good := map[string]time.Duration{"30s": 30 * time.Second, "@every 5m": 5 * time.Minute, "@EVERY 1h30m": 90 * time.Minute, " 1s ": time.Second}
	for in, want := range good {
		got, err := ParseInterval(in)
		if err != nil || got != want {
			t.Errorf("ParseInterval(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"", "0s", "-5m", "999ms", "9000h", "soon", "@every"} {
		if _, err := ParseInterval(in); err == nil {
			t.Errorf("ParseInterval(%q): expected error", in)
		}
	}
}

// activations returns the first n activations strictly after `after`.
func activations(cs *CronSchedule, after time.Time, n int) []time.Time {
	out := make([]time.Time, 0, n)
	cur := after
	for i := 0; i < n; i++ {
		next := cs.Next(cur)
		if next.IsZero() {
			break
		}
		out = append(out, next)
		cur = next
	}
	return out
}

// TestCronDSTZones exercises spring-forward gaps and fall-back overlaps of
// 30 minutes (Lord Howe), 1 hour (New York, London) and 2 hours (Troll).
// Expected instants are given in UTC to make the real-time spacing explicit.
func TestCronDSTZones(t *testing.T) {
	u := func(mo time.Month, d, h, mi int) time.Time { return time.Date(2026, mo, d, h, mi, 0, 0, time.UTC) }
	cases := []struct {
		name  string
		zone  string
		expr  string
		after time.Time // UTC
		want  []time.Time
	}{
		// America/New_York: 2026-03-08 02:00 EST -> 03:00 EDT (07:00Z);
		// 2026-11-01 02:00 EDT -> 01:00 EST (06:00Z).
		{"NY gap fixed skipped", "America/New_York", "30 2 * * *", u(3, 8, 5, 0),
			[]time.Time{u(3, 9, 6, 30)}},
		{"NY gap every 15m", "America/New_York", "*/15 * * * *", u(3, 8, 6, 40),
			[]time.Time{u(3, 8, 6, 45), u(3, 8, 7, 0), u(3, 8, 7, 15)}},
		{"NY overlap fixed runs once", "America/New_York", "30 1 * * *", u(11, 1, 4, 0),
			[]time.Time{u(11, 1, 5, 30), u(11, 2, 6, 30)}},
		{"NY overlap hourly runs each real hour", "America/New_York", "@hourly", u(11, 1, 4, 30),
			[]time.Time{u(11, 1, 5, 0), u(11, 1, 6, 0), u(11, 1, 7, 0)}},
		{"NY overlap fixed list", "America/New_York", "30 1,2 * * *", u(11, 1, 4, 0),
			[]time.Time{u(11, 1, 5, 30), u(11, 1, 7, 30), u(11, 2, 6, 30)}},

		// Europe/London: 2026-03-29 01:00 GMT -> 02:00 BST (01:00Z);
		// 2026-10-25 02:00 BST -> 01:00 GMT (01:00Z).
		{"London gap fixed skipped", "Europe/London", "30 1 * * *", u(3, 28, 12, 0),
			[]time.Time{u(3, 30, 0, 30)}},
		{"London overlap fixed runs once", "Europe/London", "15 1 * * *", u(10, 24, 12, 0),
			[]time.Time{u(10, 25, 0, 15), u(10, 26, 1, 15)}},
		{"London overlap every 30m", "Europe/London", "*/30 * * * *", u(10, 25, 0, 15),
			[]time.Time{u(10, 25, 0, 30), u(10, 25, 1, 0), u(10, 25, 1, 30), u(10, 25, 2, 0)}},

		// Australia/Lord_Howe (30-minute DST): 2026-04-05 02:00 +11 -> 01:30
		// +1030 (2026-04-04 15:00Z); 2026-10-04 02:00 +1030 -> 02:30 +11
		// (2026-10-03 15:30Z).
		{"Lord Howe 30m gap fixed skipped", "Australia/Lord_Howe", "15 2 * * *", u(10, 3, 12, 0),
			[]time.Time{u(10, 4, 15, 15)}}, // 2026-10-05 02:15 +11
		{"Lord Howe 30m gap every 15m", "Australia/Lord_Howe", "*/15 * * * *", u(10, 3, 15, 10),
			// 01:45 +1030 -> 02:30 +11 -> 02:45 +11
			[]time.Time{u(10, 3, 15, 15), u(10, 3, 15, 30), u(10, 3, 15, 45)}},
		{"Lord Howe 30m overlap fixed runs once", "Australia/Lord_Howe", "45 1 * * *", u(4, 4, 12, 0),
			// 01:45 +11 once, then 2026-04-06 01:45 +1030.
			[]time.Time{u(4, 4, 14, 45), u(4, 5, 15, 15)}},
		{"Lord Howe 30m overlap every 15m", "Australia/Lord_Howe", "*/15 * * * *", u(4, 4, 14, 40),
			// 01:45 +11, 01:30 +1030, 01:45 +1030, 02:00 +1030
			[]time.Time{u(4, 4, 14, 45), u(4, 4, 15, 0), u(4, 4, 15, 15), u(4, 4, 15, 30)}},

		// Antarctica/Troll (2-hour DST): 2026-03-29 01:00 +00 -> 03:00 +02
		// (01:00Z); 2026-10-25 03:00 +02 -> 01:00 +00 (01:00Z).
		{"Troll 2h gap fixed skipped", "Antarctica/Troll", "30 2 * * *", u(3, 28, 12, 0),
			[]time.Time{u(3, 30, 0, 30)}},
		{"Troll 2h gap hourly", "Antarctica/Troll", "@hourly", u(3, 28, 23, 30),
			// 00:00 +00, then 03:00 +02 (one real hour later), 04:00 +02
			[]time.Time{u(3, 29, 0, 0), u(3, 29, 1, 0), u(3, 29, 2, 0)}},
		{"Troll 2h overlap fixed runs once", "Antarctica/Troll", "30 1 * * *", u(10, 24, 12, 0),
			// 01:30 +02 once, not again at 01:30 +00, then 2026-10-26 01:30 +00.
			[]time.Time{u(10, 24, 23, 30), u(10, 26, 1, 30)}},
		{"Troll 2h overlap fixed range runs once per wall hour", "Antarctica/Troll", "0 1-3 * * *", u(10, 24, 22, 30),
			// 01:00 +02, 02:00 +02, (01:00/02:00 +00 repeat skipped), 03:00 +00
			[]time.Time{u(10, 24, 23, 0), u(10, 25, 0, 0), u(10, 25, 3, 0)}},
		{"Troll 2h overlap hourly", "Antarctica/Troll", "@hourly", u(10, 24, 23, 30),
			[]time.Time{u(10, 25, 0, 0), u(10, 25, 1, 0), u(10, 25, 2, 0), u(10, 25, 3, 0)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			loc, err := time.LoadLocation(tc.zone)
			if err != nil {
				t.Skipf("tzdata unavailable for %s: %v", tc.zone, err)
			}
			cs, err := ParseCronInLocation(tc.expr, loc)
			if err != nil {
				t.Fatal(err)
			}
			got := activations(cs, tc.after, len(tc.want))
			if len(got) != len(tc.want) {
				t.Fatalf("got %d activations %v, want %v", len(got), got, tc.want)
			}
			for i := range got {
				if !got[i].Equal(tc.want[i]) {
					t.Fatalf("activation %d: got %s (%s), want %s", i, got[i].UTC().Format(time.RFC3339), got[i].Format("15:04 -0700"), tc.want[i].Format(time.RFC3339))
				}
				if got[i].Location() != loc {
					t.Fatalf("activation %d not in schedule location: %s", i, got[i].Location())
				}
				if got[i].Second() != 0 || got[i].Nanosecond() != 0 {
					t.Fatalf("activation %d not on a wall-clock minute: %s", i, got[i])
				}
			}
		})
	}
}

// TestCronNextFromInsideOverlap starts inside the second pass of a fall-back
// overlap: a fixed-time job whose wall time already occurred in the first
// pass must not fire again, while a wildcard job continues normally.
func TestCronNextFromInsideOverlap(t *testing.T) {
	troll, err := time.LoadLocation("Antarctica/Troll")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	inSecondPass := time.Date(2026, 10, 25, 1, 10, 0, 0, time.UTC) // 01:10 +00, second pass
	fixed, _ := ParseCronInLocation("30 2 * * *", troll)
	if got, want := fixed.Next(inSecondPass), time.Date(2026, 10, 26, 2, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("fixed: got %s, want %s", got.UTC(), want)
	}
	wild, _ := ParseCronInLocation("*/20 * * * *", troll)
	if got, want := wild.Next(inSecondPass), time.Date(2026, 10, 25, 1, 20, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("wildcard: got %s, want %s", got.UTC(), want)
	}
	// A fixed job at a wall time after the overlap still fires the same day.
	late, _ := ParseCronInLocation("30 3 * * *", troll)
	if got, want := late.Next(inSecondPass), time.Date(2026, 10, 25, 3, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("late: got %s, want %s", got.UTC(), want)
	}
}

// TestCronHistoricalOffsets covers zones whose offsets are not whole minutes
// (local mean time) and far-future years that rely on the zone's rule
// extension rather than explicit transitions.
func TestCronHistoricalOffsets(t *testing.T) {
	ams, err := time.LoadLocation("Europe/Amsterdam")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	cs, _ := ParseCronInLocation("0 12 * * *", ams)
	// Amsterdam used +00:19:32 in 1920.
	got := cs.Next(time.Date(1920, 6, 1, 0, 0, 0, 0, time.UTC))
	if l := got.In(ams); l.Hour() != 12 || l.Minute() != 0 || l.Second() != 0 || l.Day() != 1 {
		t.Fatalf("unexpected LMT activation %s", l)
	}
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("tzdata unavailable: %v", err)
	}
	gap, _ := ParseCronInLocation("30 2 * * *", ny)
	// 2090-03-12 is the second Sunday of March 2090.
	got = gap.Next(time.Date(2090, 3, 11, 12, 0, 0, 0, time.UTC))
	if want := time.Date(2090, 3, 13, 6, 30, 0, 0, time.UTC); !got.Equal(want) {
		t.Fatalf("far-future gap: got %s, want %s", got.UTC(), want)
	}
}
