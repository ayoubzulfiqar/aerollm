package schedule

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	// maxCronExprLen bounds the length of a schedule expression.
	maxCronExprLen = 256
	// maxSearchYears bounds the Next search. Feb 29 schedules need at most
	// 8 years (e.g. 2096 -> 2104); anything beyond this never fires.
	maxSearchYears = 12
	// MinInterval and MaxInterval bound interval ("@every") schedules.
	MinInterval = time.Second
	MaxInterval = 366 * 24 * time.Hour
)

// CronSchedule is a parsed 5-field cron expression
// (minute hour day-of-month month day-of-week) or an "@every" interval.
type CronSchedule struct {
	expr    string
	minute  uint64
	hour    uint64
	dom     uint64
	month   uint64
	dow     uint64
	domStar bool
	dowStar bool
	// fixedTime is set when neither the minute nor the hour field is a
	// wildcard or step ("*", "*/n"); such jobs run once per wall-clock time
	// during a DST fall-back overlap.
	fixedTime bool
	every     time.Duration
	loc       *time.Location
}

type cronField struct {
	name    string
	min     int
	max     int
	wildMax int // upper bound used for "*" (differs from max for day-of-week, where 7 aliases 0)
	names   map[string]int
	allowQ  bool
}

var (
	monthNames = map[string]int{
		"jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
		"jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
	}
	dowNames = map[string]int{
		"sun": 0, "mon": 1, "tue": 2, "wed": 3, "thu": 4, "fri": 5, "sat": 6,
	}

	minuteField = cronField{name: "minute", min: 0, max: 59, wildMax: 59}
	hourField   = cronField{name: "hour", min: 0, max: 23, wildMax: 23}
	domField    = cronField{name: "day-of-month", min: 1, max: 31, wildMax: 31, allowQ: true}
	monthField  = cronField{name: "month", min: 1, max: 12, wildMax: 12, names: monthNames}
	dowField    = cronField{name: "day-of-week", min: 0, max: 7, wildMax: 6, names: dowNames, allowQ: true}

	cronMacros = map[string]string{
		"@yearly":   "0 0 1 1 *",
		"@annually": "0 0 1 1 *",
		"@monthly":  "0 0 1 * *",
		"@weekly":   "0 0 * * 0",
		"@daily":    "0 0 * * *",
		"@midnight": "0 0 * * *",
		"@hourly":   "0 * * * *",
	}
)

// ParseCron parses a cron expression evaluated in the location of the time
// passed to Next. See ParseCronInLocation.
func ParseCron(expr string) (*CronSchedule, error) {
	return ParseCronInLocation(expr, nil)
}

// ParseCronInLocation parses a standard 5-field cron expression
// ("minute hour day-of-month month day-of-week") evaluated in loc (nil means
// the location of the time passed to Next).
//
// Supported syntax: "*" and "?" (day fields only), single values, ranges
// "a-b", steps "*/n", "a-b/n" and "a/n" (a through the field maximum), comma
// separated lists, case-insensitive month names (JAN-DEC) and day names
// (SUN-SAT), day-of-week 7 as an alias for Sunday, the macros @yearly,
// @annually, @monthly, @weekly, @daily, @midnight and @hourly, and
// "@every <duration>" for fixed intervals.
//
// Day matching follows Vixie cron: when both day-of-month and day-of-week are
// restricted (neither field starts with "*" or "?") a day matches if EITHER
// field matches; otherwise both must match.
func ParseCronInLocation(expr string, loc *time.Location) (*CronSchedule, error) {
	s := strings.TrimSpace(expr)
	if s == "" {
		return nil, errors.New("cron: empty expression")
	}
	if len(s) > maxCronExprLen {
		return nil, fmt.Errorf("cron: expression longer than %d characters", maxCronExprLen)
	}
	if strings.HasPrefix(s, "@") {
		lower := strings.ToLower(s)
		if lower == "@every" || strings.HasPrefix(lower, "@every ") || strings.HasPrefix(lower, "@every\t") {
			d, err := ParseInterval(s)
			if err != nil {
				return nil, err
			}
			return &CronSchedule{expr: s, every: d, loc: loc}, nil
		}
		std, ok := cronMacros[lower]
		if !ok {
			return nil, fmt.Errorf("cron: unknown macro %q", s)
		}
		cs, err := parseFields(std, loc)
		if err != nil {
			return nil, err
		}
		cs.expr = s
		return cs, nil
	}
	cs, err := parseFields(s, loc)
	if err != nil {
		return nil, err
	}
	cs.expr = s
	return cs, nil
}

func parseFields(s string, loc *time.Location) (*CronSchedule, error) {
	fields := strings.Fields(s)
	if len(fields) != 5 {
		return nil, fmt.Errorf("cron: expected 5 fields (minute hour day-of-month month day-of-week), got %d", len(fields))
	}
	cs := &CronSchedule{loc: loc}
	var err error
	if cs.minute, err = parseField(fields[0], minuteField); err != nil {
		return nil, err
	}
	if cs.hour, err = parseField(fields[1], hourField); err != nil {
		return nil, err
	}
	if cs.dom, err = parseField(fields[2], domField); err != nil {
		return nil, err
	}
	if cs.month, err = parseField(fields[3], monthField); err != nil {
		return nil, err
	}
	if cs.dow, err = parseField(fields[4], dowField); err != nil {
		return nil, err
	}
	// Day-of-week 7 is Sunday.
	if cs.dow&(1<<7) != 0 {
		cs.dow = (cs.dow | 1) &^ (1 << 7)
	}
	cs.domStar = isStar(fields[2])
	cs.dowStar = isStar(fields[4])
	cs.fixedTime = !isStar(fields[0]) && !isStar(fields[1])
	return cs, nil
}

func isStar(field string) bool {
	return strings.HasPrefix(field, "*") || strings.HasPrefix(field, "?")
}

func parseField(text string, f cronField) (uint64, error) {
	var bits uint64
	for _, part := range strings.Split(text, ",") {
		if part == "" {
			return 0, fmt.Errorf("cron: %s: empty list element in %q", f.name, text)
		}
		b, err := parsePart(part, f)
		if err != nil {
			return 0, err
		}
		bits |= b
	}
	return bits, nil
}

func parsePart(part string, f cronField) (uint64, error) {
	rangePart, stepPart, hasStep := strings.Cut(part, "/")
	step := 1
	if hasStep {
		n, err := parseNumber(stepPart)
		if err != nil {
			return 0, fmt.Errorf("cron: %s: invalid step %q", f.name, stepPart)
		}
		if n < 1 {
			return 0, fmt.Errorf("cron: %s: step must be at least 1 in %q", f.name, part)
		}
		if n > f.max-f.min+1 {
			return 0, fmt.Errorf("cron: %s: step %d too large in %q", f.name, n, part)
		}
		step = n
	}

	var lo, hi int
	switch {
	case rangePart == "*" || (rangePart == "?" && f.allowQ):
		lo, hi = f.min, f.wildMax
	case strings.Contains(rangePart, "-"):
		a, b, _ := strings.Cut(rangePart, "-")
		var err error
		if lo, err = f.value(a); err != nil {
			return 0, err
		}
		if hi, err = f.value(b); err != nil {
			return 0, err
		}
		if lo > hi {
			return 0, fmt.Errorf("cron: %s: reversed range %q", f.name, rangePart)
		}
	default:
		v, err := f.value(rangePart)
		if err != nil {
			return 0, err
		}
		lo, hi = v, v
		if hasStep {
			hi = f.wildMax
			if lo > hi {
				hi = lo
			}
		}
	}

	var bits uint64
	for v := lo; v <= hi; v += step {
		bits |= 1 << uint(v)
	}
	return bits, nil
}

func (f cronField) value(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("cron: %s: missing value", f.name)
	}
	if f.names != nil {
		if v, ok := f.names[strings.ToLower(s)]; ok {
			return v, nil
		}
	}
	v, err := parseNumber(s)
	if err != nil {
		return 0, fmt.Errorf("cron: %s: invalid value %q", f.name, s)
	}
	if v < f.min || v > f.max {
		return 0, fmt.Errorf("cron: %s: value %d out of range [%d-%d]", f.name, v, f.min, f.max)
	}
	return v, nil
}

// parseNumber parses a short unsigned decimal (no signs, no spaces).
func parseNumber(s string) (int, error) {
	if s == "" || len(s) > 4 {
		return 0, errors.New("invalid number")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, errors.New("invalid number")
		}
	}
	return strconv.Atoi(s)
}

// ParseInterval parses an interval schedule: a Go duration ("30s", "5m",
// "1h30m") optionally prefixed with "@every ". The interval must be between
// MinInterval and MaxInterval.
func ParseInterval(spec string) (time.Duration, error) {
	s := strings.TrimSpace(spec)
	if len(s) > maxCronExprLen {
		return 0, fmt.Errorf("interval: expression longer than %d characters", maxCronExprLen)
	}
	if len(s) >= len("@every") && strings.EqualFold(s[:len("@every")], "@every") {
		s = strings.TrimSpace(s[len("@every"):])
	}
	if s == "" {
		return 0, errors.New("interval: missing duration")
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("interval: invalid duration %q", s)
	}
	if d < MinInterval {
		return 0, fmt.Errorf("interval: must be at least %s", MinInterval)
	}
	if d > MaxInterval {
		return 0, fmt.Errorf("interval: must be at most %s", MaxInterval)
	}
	return d, nil
}

// String returns the original expression.
func (c *CronSchedule) String() string {
	if c == nil {
		return ""
	}
	return c.expr
}

// Interval returns the fixed interval for "@every" schedules, or 0.
func (c *CronSchedule) Interval() time.Duration {
	if c == nil {
		return 0
	}
	return c.every
}

// Location returns the evaluation location, or nil when Next uses the
// location of its argument.
func (c *CronSchedule) Location() *time.Location {
	if c == nil {
		return nil
	}
	return c.loc
}

// Next returns the first activation time strictly after `after` (seconds are
// truncated), or the zero time if the schedule never fires within the search
// horizon (e.g. "0 0 30 2 *"). For "@every" schedules Next returns
// after + interval.
//
// Times are evaluated on the wall clock of the schedule's location using the
// location's real UTC offsets, so DST shifts of any size (30 minutes on Lord
// Howe Island, 2 hours at Troll station, historical double summer time) are
// handled the same way:
//
//   - Gaps (spring forward): wall-clock times that do not exist are skipped
//     entirely; the job does not run late to make up for them. A job at
//     02:30 in America/New_York does not run on the day clocks jump from
//     02:00 to 03:00, and an every-15-minutes job goes 01:45 -> 03:00 EDT
//     (15 real minutes later).
//   - Overlaps (fall back): a fixed-time job (neither the minute nor the hour
//     field starts with "*", e.g. "30 1 * * *" or "0 9-17 * * *") runs once,
//     at the first occurrence of the repeated wall-clock time. A job with a
//     wildcard/step minute or hour field (e.g. "*/15 * * * *", "@hourly")
//     keeps its real-time cadence and also fires during the repeated period,
//     like Vixie cron's wildcard jobs.
//
// Consecutive calls that feed the previous activation back in therefore never
// fire a fixed-time job twice for the same wall-clock time.
func (c *CronSchedule) Next(after time.Time) time.Time {
	if c == nil {
		return time.Time{}
	}
	if c.every > 0 {
		return after.Truncate(time.Second).Add(c.every)
	}
	loc := c.loc
	if loc == nil {
		loc = after.Location()
	}
	local := after.In(loc)
	maxYear := local.Year() + maxSearchYears

	// The search walks the zone segments of loc (intervals with a constant
	// UTC offset) in chronological order. Within a segment wall-clock time is
	// a linear function of real time, so the first matching wall-clock time
	// inside the segment is the first activation in it.
	lo := civil(local).Truncate(time.Minute).Add(time.Minute)
	segStart, segEnd := local.ZoneBounds()
	_, offset := local.Zone()

	// seenUntil is the latest wall-clock time already reached before the
	// current segment. After a backward transition wall-clock times before it
	// repeat; fixed-time jobs must not fire for them again.
	var seenUntil time.Time
	if !segStart.IsZero() {
		seenUntil = civil(segStart.Add(-time.Nanosecond).In(loc))
	}
	for {
		if c.fixedTime && lo.Before(seenUntil) {
			lo = ceilMinute(seenUntil)
		}
		var hi time.Time // exclusive wall-clock bound of this segment
		if !segEnd.IsZero() {
			// The wall-clock reading the current offset would show at segEnd.
			hi = segEnd.UTC().Add(time.Duration(offset) * time.Second)
		}
		if w, ok := c.searchWall(lo, hi, maxYear); ok {
			return w.Add(-time.Duration(offset) * time.Second).In(loc)
		}
		if segEnd.IsZero() {
			return time.Time{}
		}
		if hi.After(seenUntil) {
			seenUntil = hi
		}
		next := segEnd.In(loc)
		if next.Year() > maxYear {
			return time.Time{}
		}
		lo = ceilMinute(civil(next))
		_, offset = next.Zone()
		_, segEnd = next.ZoneBounds()
	}
}

// civil returns t's wall-clock reading as a UTC time, which makes wall-clock
// arithmetic and comparisons independent of zone offsets.
func civil(t time.Time) time.Time {
	y, mo, d := t.Date()
	h, mi, s := t.Clock()
	return time.Date(y, mo, d, h, mi, s, t.Nanosecond(), time.UTC)
}

func ceilMinute(t time.Time) time.Time {
	if tr := t.Truncate(time.Minute); !tr.Equal(t) {
		return tr.Add(time.Minute)
	}
	return t
}

// searchWall returns the first wall-clock time w (a civil time, see civil)
// with lo <= w < hi that matches the schedule. A zero hi means unbounded.
// The search stops after maxYear.
func (c *CronSchedule) searchWall(lo, hi time.Time, maxYear int) (time.Time, bool) {
	sy, smo, sd := lo.Date()
	sh, smi := lo.Hour(), lo.Minute()
	bounded := !hi.IsZero()
	for year := sy; year <= maxYear; year++ {
		if bounded && year > hi.Year() {
			return time.Time{}, false
		}
		mStart := time.January
		if year == sy {
			mStart = smo
		}
		for month := mStart; month <= time.December; month++ {
			if c.month&(1<<uint(month)) == 0 {
				continue
			}
			firstMonth := year == sy && month == smo
			dStart := 1
			if firstMonth {
				dStart = sd
			}
			last := daysIn(month, year)
			for day := dStart; day <= last; day++ {
				if bounded && !time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Before(hi) {
					return time.Time{}, false
				}
				if !c.dayMatches(year, month, day) {
					continue
				}
				firstDay := firstMonth && day == sd
				hStart := 0
				if firstDay {
					hStart = sh
				}
				for hour := hStart; hour < 24; hour++ {
					if c.hour&(1<<uint(hour)) == 0 {
						continue
					}
					miStart := 0
					if firstDay && hour == sh {
						miStart = smi
					}
					for minute := miStart; minute < 60; minute++ {
						if c.minute&(1<<uint(minute)) == 0 {
							continue
						}
						w := time.Date(year, month, day, hour, minute, 0, 0, time.UTC)
						if bounded && !w.Before(hi) {
							return time.Time{}, false
						}
						return w, true
					}
				}
			}
		}
	}
	return time.Time{}, false
}

func (c *CronSchedule) dayMatches(year int, month time.Month, day int) bool {
	domOK := c.dom&(1<<uint(day)) != 0
	wd := time.Date(year, month, day, 12, 0, 0, 0, time.UTC).Weekday()
	dowOK := c.dow&(1<<uint(wd)) != 0
	if c.domStar || c.dowStar {
		return domOK && dowOK
	}
	return domOK || dowOK
}

func daysIn(month time.Month, year int) int {
	switch month {
	case time.February:
		if isLeap(year) {
			return 29
		}
		return 28
	case time.April, time.June, time.September, time.November:
		return 30
	default:
		return 31
	}
}

func isLeap(year int) bool {
	return year%4 == 0 && (year%100 != 0 || year%400 == 0)
}
