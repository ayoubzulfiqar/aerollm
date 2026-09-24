package guardrails

import (
	"regexp"
	"slices"
	"strings"
)

// PIIKind identifies a category of sensitive data. It is used in placeholders
// such as <PII_EMAIL_1>.
type PIIKind string

const (
	PIIEmail      PIIKind = "EMAIL"
	PIIPhone      PIIKind = "PHONE"
	PIISSN        PIIKind = "SSN"
	PIICreditCard PIIKind = "CC"
	PIIIBAN       PIIKind = "IBAN"
	PIISecret     PIIKind = "SECRET"
)

// PIIMatch is a detected span of sensitive data in a text.
type PIIMatch struct {
	Kind  PIIKind
	Start int // byte offset
	End   int // byte offset (exclusive)
	Value string
}

// PIIPatterns defines regex patterns for common PII. It is kept for
// backward compatibility; PIIRedactor additionally validates candidates
// (Luhn for cards, mod-97 for IBANs, SSA rules for SSNs) and detects secrets.
type PIIPatterns struct {
	Email      *regexp.Regexp
	PhoneUS    *regexp.Regexp
	SSN        *regexp.Regexp
	CreditCard *regexp.Regexp
}

var (
	emailRe = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?(?:\.[A-Za-z0-9](?:[A-Za-z0-9\-]{0,61}[A-Za-z0-9])?)*\.[A-Za-z]{2,24}\b`)
	// North-American numbers require separators so bare 10-digit numbers
	// (timestamps, IDs) are not flagged.
	phoneNANPRe = regexp.MustCompile(`(?:\+?1[\s.\-]?)?(?:\(\s*[2-9]\d{2}\s*\)|\b[2-9]\d{2})[\s.\-]?\d{3}[\s.\-]\d{4}\b`)
	// International numbers must start with '+'; digit count is validated.
	phoneIntlRe = regexp.MustCompile(`\+[1-9][\d\s.\-()]{6,22}\d\b`)
	ssnRe       = regexp.MustCompile(`\b\d{3}-\d{2}-\d{4}\b`)
	cardRe      = regexp.MustCompile(`\b(?:\d[ \-]?){12,18}\d\b`)
	ibanRe      = regexp.MustCompile(`\b[A-Z]{2}\d{2}(?: ?[A-Z0-9]{4}){2,7}(?: ?[A-Z0-9]{1,4})?\b`)

	secretRes = []*regexp.Regexp{
		regexp.MustCompile(`-----BEGIN [A-Z0-9 ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z0-9 ]*PRIVATE KEY-----`),
		// OpenAI / Anthropic / AeroLLM style keys: sk-..., sk-proj-..., sk-ant-...
		regexp.MustCompile(`\bsk-[A-Za-z0-9_\-]{16,}`),
		// Stripe style keys.
		regexp.MustCompile(`\b[rs]k_(?:live|test)_[A-Za-z0-9]{16,}`),
		// AWS access key IDs.
		regexp.MustCompile(`\b(?:AKIA|ASIA|AGPA|AIDA|AROA|ANPA|ANVA|AIPA)[0-9A-Z]{16}\b`),
		// GitHub tokens.
		regexp.MustCompile(`\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})\b`),
		// Slack tokens.
		regexp.MustCompile(`\bxox[abposr]-[A-Za-z0-9\-]{10,}`),
		// Google API keys.
		regexp.MustCompile(`\bAIza[0-9A-Za-z_\-]{35}`),
		// JSON Web Tokens.
		regexp.MustCompile(`\beyJ[A-Za-z0-9_\-]{8,}\.eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}`),
	}
	// Secrets whose value is in capture group 1 (the label is kept).
	secretGroupRes = []*regexp.Regexp{
		regexp.MustCompile(`(?i)\baws_?secret_?(?:access_?)?key["']?\s*[:=]\s*["']?([A-Za-z0-9/+]{40})\b`),
		regexp.MustCompile(`(?i)\bbearer\s+([A-Za-z0-9._~+/\-]{20,}=*)`),
		regexp.MustCompile(`(?i)\b(?:api[_-]?key|secret|password|passwd|token)["']?\s*[:=]\s*["']?([^\s"',;]{12,})`),
	}
)

// DefaultPIIPatterns returns compiled regexes for common PII types.
func DefaultPIIPatterns() PIIPatterns {
	return PIIPatterns{
		Email:      emailRe,
		PhoneUS:    phoneNANPRe,
		SSN:        ssnRe,
		CreditCard: cardRe,
	}
}

// PIIRedactor masks PII in text and can restore it. It is stateless and safe
// for concurrent use.
type PIIRedactor struct {
	patterns PIIPatterns
}

// NewPIIRedactor creates a new redactor with default patterns.
func NewPIIRedactor() *PIIRedactor {
	return &PIIRedactor{patterns: DefaultPIIPatterns()}
}

func digitsOnly(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for i := 0; i < len(s); i++ {
		if s[i] >= '0' && s[i] <= '9' {
			b.WriteByte(s[i])
		}
	}
	return b.String()
}

// luhnValid reports whether a digit string passes the Luhn checksum.
func luhnValid(digits string) bool {
	if len(digits) < 2 {
		return false
	}
	sum := 0
	double := false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i] - '0')
		if d < 0 || d > 9 {
			return false
		}
		if double {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		double = !double
	}
	return sum%10 == 0
}

func validCard(s string) bool {
	d := digitsOnly(s)
	if len(d) < 13 || len(d) > 19 {
		return false
	}
	// Major network IIN ranges start with 2-6.
	if d[0] < '2' || d[0] > '6' {
		return false
	}
	// Separators must be consistent (all spaces or all dashes).
	if strings.Contains(s, " ") && strings.Contains(s, "-") {
		return false
	}
	return luhnValid(d)
}

func validSSN(s string) bool {
	d := digitsOnly(s)
	if len(d) != 9 {
		return false
	}
	area, group, serial := d[:3], d[3:5], d[5:]
	if area == "000" || area == "666" || area[0] == '9' || group == "00" || serial == "0000" {
		return false
	}
	return true
}

func validIntlPhone(s string) bool {
	n := len(digitsOnly(s))
	return n >= 8 && n <= 15
}

// ibanLengths lists the IBAN length for common countries; unknown countries
// only need a plausible length and a valid checksum.
var ibanLengths = map[string]int{
	"AD": 24, "AE": 23, "AT": 20, "BE": 16, "BG": 22, "BH": 22, "BR": 29, "CH": 21,
	"CY": 28, "CZ": 24, "DE": 22, "DK": 18, "EE": 20, "ES": 24, "FI": 18, "FR": 27,
	"GB": 22, "GR": 27, "HR": 21, "HU": 28, "IE": 22, "IL": 23, "IS": 26, "IT": 27,
	"KW": 30, "KZ": 20, "LI": 21, "LT": 20, "LU": 20, "LV": 21, "MC": 27, "MT": 31,
	"NL": 18, "NO": 15, "PK": 24, "PL": 28, "PT": 25, "QA": 29, "RO": 24, "RS": 22,
	"SA": 24, "SE": 24, "SI": 19, "SK": 24, "SM": 27, "TR": 26, "UA": 29,
}

func validIBAN(s string) bool {
	iban := strings.ReplaceAll(s, " ", "")
	if len(iban) < 15 || len(iban) > 34 {
		return false
	}
	if want, ok := ibanLengths[iban[:2]]; ok && want != len(iban) {
		return false
	}
	rearranged := iban[4:] + iban[:4]
	rem := 0
	for i := 0; i < len(rearranged); i++ {
		c := rearranged[i]
		switch {
		case c >= '0' && c <= '9':
			rem = (rem*10 + int(c-'0')) % 97
		case c >= 'A' && c <= 'Z':
			v := int(c-'A') + 10
			rem = (rem*100 + v) % 97
		default:
			return false
		}
	}
	return rem == 1
}

type span struct {
	kind       PIIKind
	start, end int
}

// Detect returns all PII found in text, ordered by position. Overlapping
// candidates are resolved by priority: secrets, cards, IBANs, SSNs, emails,
// then phone numbers.
func (p *PIIRedactor) Detect(text string) []PIIMatch {
	if text == "" {
		return nil
	}
	var accepted []span
	overlaps := func(s, e int) bool {
		for _, a := range accepted {
			if s < a.end && e > a.start {
				return true
			}
		}
		return false
	}
	add := func(kind PIIKind, s, e int) {
		if s < e && !overlaps(s, e) {
			accepted = append(accepted, span{kind, s, e})
		}
	}
	addAll := func(kind PIIKind, re *regexp.Regexp, valid func(string) bool) {
		for _, loc := range re.FindAllStringIndex(text, -1) {
			if valid == nil || valid(text[loc[0]:loc[1]]) {
				add(kind, loc[0], loc[1])
			}
		}
	}

	for _, re := range secretRes {
		addAll(PIISecret, re, nil)
	}
	for _, re := range secretGroupRes {
		for _, loc := range re.FindAllStringSubmatchIndex(text, -1) {
			if len(loc) >= 4 && loc[2] >= 0 {
				add(PIISecret, loc[2], loc[3])
			}
		}
	}
	addAll(PIICreditCard, cardRe, validCard)
	addAll(PIIIBAN, ibanRe, validIBAN)
	addAll(PIISSN, ssnRe, validSSN)
	addAll(PIIEmail, emailRe, nil)
	addAll(PIIPhone, phoneIntlRe, validIntlPhone)
	addAll(PIIPhone, phoneNANPRe, nil)

	slices.SortFunc(accepted, func(a, b span) int { return a.start - b.start })
	out := make([]PIIMatch, len(accepted))
	for i, a := range accepted {
		out[i] = PIIMatch{Kind: a.kind, Start: a.start, End: a.end, Value: text[a.start:a.end]}
	}
	return out
}

// ContainsPII reports whether text contains any detectable PII or secrets.
func (p *PIIRedactor) ContainsPII(text string) bool {
	return len(p.Detect(text)) > 0
}

// RedactWithMapping masks PII with placeholders like <PII_EMAIL_1> and
// returns the placeholder -> original mapping (nil if nothing was found).
// Identical values of the same kind share a placeholder.
func (p *PIIRedactor) RedactWithMapping(text string) (string, map[string]string) {
	st := newRedactState(p)
	out, changed := st.redact(text)
	if !changed {
		return text, nil
	}
	return out, st.mapping
}

// Redact masks PII with placeholders like <PII_EMAIL_1>.
func (p *PIIRedactor) Redact(text string) string {
	out, _ := p.RedactWithMapping(text)
	return out
}

// RestoreWithMapping replaces placeholders in redacted with their originals.
func RestoreWithMapping(redacted string, mapping map[string]string) string {
	if len(mapping) == 0 {
		return redacted
	}
	pairs := make([]string, 0, len(mapping)*2)
	// Longest placeholders first so <PII_EMAIL_10> is not clobbered by
	// <PII_EMAIL_1>.
	keys := make([]string, 0, len(mapping))
	for k := range mapping {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b string) int {
		if len(a) != len(b) {
			return len(b) - len(a)
		}
		return strings.Compare(a, b)
	})
	for _, k := range keys {
		pairs = append(pairs, k, mapping[k])
	}
	return strings.NewReplacer(pairs...).Replace(redacted)
}

// Restore reverses redaction of original. Because redaction is deterministic
// the placeholder mapping is recomputed from original.
func (p *PIIRedactor) Restore(original, redacted string) string {
	if original == "" {
		return redacted
	}
	_, mapping := p.RedactWithMapping(original)
	return RestoreWithMapping(redacted, mapping)
}
