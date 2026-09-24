// Package notification manages notification channels (webhook, Slack, email,
// SMS), alert subscriptions and their delivery.
//
// Security notes:
//   - webhook and Slack targets must be absolute https URLs without embedded
//     credentials; hosts in loopback/private/link-local/CGNAT/metadata space
//     are rejected at validation time and again at dial time (after DNS
//     resolution) unless private networks are explicitly allowed.
//   - Channel targets and metadata can carry secrets (Slack webhook URLs embed
//     a token in their path, signing secrets live in Metadata). The stores keep
//     the full values, but HTTP responses only ever contain the Redacted form.
//     Sending a redacted value back (e.g. "***") on update keeps the stored
//     secret unchanged.
package notification

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net/mail"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// ChannelType defines notification channel types.
type ChannelType string

const (
	ChannelWebhook ChannelType = "webhook"
	ChannelEmail   ChannelType = "email"
	ChannelSlack   ChannelType = "slack"
	ChannelSMS     ChannelType = "sms"
)

// Channel represents a notification destination.
type Channel struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Type     ChannelType       `json:"type"`
	Target   string            `json:"target"`
	Enabled  bool              `json:"enabled"`
	Metadata map[string]string `json:"metadata"`
}

// Subscription links an alert to a channel.
type Subscription struct {
	ID        string `json:"id"`
	AlertID   string `json:"alert_id"`
	ChannelID string `json:"channel_id"`
	Enabled   bool   `json:"enabled"`
}

// Limits applied to stored objects.
const (
	DefaultMaxChannels      = 1000
	DefaultMaxSubscriptions = 10000

	maxNameLen          = 128
	maxAlertIDLen       = 256
	maxTargetLen        = 2048
	maxMetadataEntries  = 32
	maxMetadataKeyLen   = 64
	maxMetadataValueLen = 1024

	// RedactedValue replaces secrets in API responses.
	RedactedValue = "***"
)

// Sentinel errors returned by the store.
var (
	ErrNotFound       = errors.New("notification: not found")
	ErrStoreFull      = errors.New("notification: store is full")
	ErrUnknownChannel = errors.New("notification: channel_id does not reference an existing channel")
)

// ValidationError describes invalid input.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	if e.Field == "" {
		return "notification: " + e.Message
	}
	return "notification: " + e.Field + ": " + e.Message
}

func invalid(field, format string, args ...any) error {
	return &ValidationError{Field: field, Message: fmt.Sprintf(format, args...)}
}

// Options configures validation and capacity of a Store.
type Options struct {
	// AllowInsecureHTTP permits http:// webhook/slack targets.
	AllowInsecureHTTP bool
	// AllowPrivateNetworks permits targets in loopback/private/link-local
	// space (for local development and tests only).
	AllowPrivateNetworks bool
	// MaxChannels caps stored channels (default DefaultMaxChannels).
	MaxChannels int
	// MaxSubscriptions caps stored subscriptions (default DefaultMaxSubscriptions).
	MaxSubscriptions int
}

// Store manages notifications in memory. It is safe for concurrent use and
// always hands out copies.
type Store struct {
	mu            sync.RWMutex
	channels      map[string]Channel
	subscriptions map[string]Subscription
	opts          Options
}

// NewStore creates a notification store with strict (production) validation.
func NewStore() *Store {
	return NewStoreWithOptions(Options{})
}

// NewStoreWithOptions creates a notification store with the given options.
func NewStoreWithOptions(opts Options) *Store {
	if opts.MaxChannels <= 0 {
		opts.MaxChannels = DefaultMaxChannels
	}
	if opts.MaxSubscriptions <= 0 {
		opts.MaxSubscriptions = DefaultMaxSubscriptions
	}
	return &Store{
		channels:      make(map[string]Channel),
		subscriptions: make(map[string]Subscription),
		opts:          opts,
	}
}

// Options returns the store's options.
func (s *Store) Options() Options {
	return s.opts
}

var idRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)
var e164Re = regexp.MustCompile(`^\+[1-9][0-9]{6,14}$`)

func newID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand never fails on supported platforms; fail loudly
		// rather than fall back to a guessable id.
		panic("notification: crypto/rand failed: " + err.Error())
	}
	return prefix + "_" + hex.EncodeToString(b[:])
}

func validID(id string) bool {
	return idRe.MatchString(id)
}

// ValidateChannel checks a channel definition against the given options.
func ValidateChannel(ch Channel, opts Options) error {
	if ch.ID != "" && !validID(ch.ID) {
		return invalid("id", "must match %s", idRe.String())
	}
	name := strings.TrimSpace(ch.Name)
	if name == "" {
		return invalid("name", "is required")
	}
	if len(ch.Name) > maxNameLen {
		return invalid("name", "must be at most %d characters", maxNameLen)
	}
	switch ch.Type {
	case ChannelWebhook, ChannelSlack:
		if err := validateHTTPTarget(ch.Target, opts.AllowInsecureHTTP, opts.AllowPrivateNetworks); err != nil {
			if errors.Is(err, ErrBlockedDestination) {
				return &ValidationError{Field: "target", Message: "destination is not allowed (loopback/private/link-local/metadata address)"}
			}
			return invalid("target", "%s", err.Error())
		}
	case ChannelEmail:
		if err := validateEmail(ch.Target); err != nil {
			return err
		}
	case ChannelSMS:
		if !e164Re.MatchString(ch.Target) {
			return invalid("target", "must be an E.164 phone number such as +14155550100")
		}
	case "":
		return invalid("type", "is required (webhook|slack|email|sms)")
	default:
		return invalid("type", "must be one of webhook|slack|email|sms")
	}
	if len(ch.Metadata) > maxMetadataEntries {
		return invalid("metadata", "must have at most %d entries", maxMetadataEntries)
	}
	for k, v := range ch.Metadata {
		if k == "" || len(k) > maxMetadataKeyLen {
			return invalid("metadata", "keys must be 1-%d characters", maxMetadataKeyLen)
		}
		if len(v) > maxMetadataValueLen {
			return invalid("metadata", "value of %q exceeds %d characters", k, maxMetadataValueLen)
		}
	}
	return nil
}

func validateEmail(target string) error {
	if target == "" || len(target) > 254 {
		return invalid("target", "must be a valid email address")
	}
	addr, err := mail.ParseAddress(target)
	if err != nil || addr.Address != target || addr.Name != "" {
		return invalid("target", "must be a bare email address such as ops@example.com")
	}
	if strings.ContainsAny(target, "\r\n") {
		return invalid("target", "must be a valid email address")
	}
	return nil
}

// ValidateSubscription checks a subscription definition (not its channel reference).
func ValidateSubscription(sub Subscription) error {
	if sub.ID != "" && !validID(sub.ID) {
		return invalid("id", "must match %s", idRe.String())
	}
	if strings.TrimSpace(sub.AlertID) == "" {
		return invalid("alert_id", "is required")
	}
	if len(sub.AlertID) > maxAlertIDLen {
		return invalid("alert_id", "must be at most %d characters", maxAlertIDLen)
	}
	if strings.TrimSpace(sub.ChannelID) == "" {
		return invalid("channel_id", "is required")
	}
	return nil
}

func copyChannel(ch Channel) Channel {
	if ch.Metadata != nil {
		m := make(map[string]string, len(ch.Metadata))
		for k, v := range ch.Metadata {
			m[k] = v
		}
		ch.Metadata = m
	}
	return ch
}

// preserveSecrets restores secrets that a client echoed back in redacted form.
func preserveSecrets(incoming *Channel, existing Channel) {
	if incoming.Target != "" && incoming.Target != existing.Target &&
		incoming.Target == existing.Redacted().Target && strings.Contains(incoming.Target, RedactedValue) {
		incoming.Target = existing.Target
	}
	for k, v := range incoming.Metadata {
		if v == RedactedValue {
			if old, ok := existing.Metadata[k]; ok {
				incoming.Metadata[k] = old
			}
		}
	}
}

// UpsertChannel validates and adds or replaces a channel. A missing ID is
// generated. It returns the stored channel.
func (s *Store) UpsertChannel(channel Channel) (Channel, error) {
	channel = copyChannel(channel)
	s.mu.Lock()
	defer s.mu.Unlock()
	if channel.ID == "" {
		channel.ID = newID("ch")
	}
	existing, exists := s.channels[channel.ID]
	if exists {
		preserveSecrets(&channel, existing)
	}
	if err := ValidateChannel(channel, s.opts); err != nil {
		return Channel{}, err
	}
	if !exists && len(s.channels) >= s.opts.MaxChannels {
		return Channel{}, ErrStoreFull
	}
	s.channels[channel.ID] = channel
	return copyChannel(channel), nil
}

// UpdateChannel replaces an existing channel; ErrNotFound when id is unknown.
func (s *Store) UpdateChannel(id string, channel Channel) (Channel, error) {
	return s.ModifyChannel(id, func(c *Channel) error {
		*c = copyChannel(channel)
		return nil
	})
}

// ModifyChannel applies fn to a copy of the channel under the store lock,
// validates the result and stores it atomically.
func (s *Store) ModifyChannel(id string, fn func(*Channel) error) (Channel, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.channels[id]
	if !ok {
		return Channel{}, ErrNotFound
	}
	next := copyChannel(existing)
	if err := fn(&next); err != nil {
		return Channel{}, err
	}
	next.ID = id
	preserveSecrets(&next, existing)
	if err := ValidateChannel(next, s.opts); err != nil {
		return Channel{}, err
	}
	s.channels[id] = next
	return copyChannel(next), nil
}

// GetChannel retrieves a channel by id.
func (s *Store) GetChannel(id string) (Channel, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ch, ok := s.channels[id]
	return copyChannel(ch), ok
}

// ListChannels returns all channels ordered by id.
func (s *Store) ListChannels() []Channel {
	s.mu.RLock()
	out := make([]Channel, 0, len(s.channels))
	for _, ch := range s.channels {
		out = append(out, copyChannel(ch))
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// DeleteChannel removes a channel and every subscription referencing it.
func (s *Store) DeleteChannel(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.channels[id]; !ok {
		return false
	}
	delete(s.channels, id)
	for sid, sub := range s.subscriptions {
		if sub.ChannelID == id {
			delete(s.subscriptions, sid)
		}
	}
	return true
}

// UpsertSubscription validates and adds or replaces a subscription. The
// referenced channel must exist. A missing ID is generated.
func (s *Store) UpsertSubscription(sub Subscription) (Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub.ID == "" {
		sub.ID = newID("sub")
	}
	if err := ValidateSubscription(sub); err != nil {
		return Subscription{}, err
	}
	if _, ok := s.channels[sub.ChannelID]; !ok {
		return Subscription{}, ErrUnknownChannel
	}
	if _, exists := s.subscriptions[sub.ID]; !exists && len(s.subscriptions) >= s.opts.MaxSubscriptions {
		return Subscription{}, ErrStoreFull
	}
	s.subscriptions[sub.ID] = sub
	return sub, nil
}

// UpdateSubscription replaces an existing subscription; ErrNotFound when id is unknown.
func (s *Store) UpdateSubscription(id string, sub Subscription) (Subscription, error) {
	return s.ModifySubscription(id, func(cur *Subscription) error {
		*cur = sub
		return nil
	})
}

// ModifySubscription applies fn to a copy of the subscription under the store
// lock, validates the result and stores it atomically.
func (s *Store) ModifySubscription(id string, fn func(*Subscription) error) (Subscription, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	existing, ok := s.subscriptions[id]
	if !ok {
		return Subscription{}, ErrNotFound
	}
	next := existing
	if err := fn(&next); err != nil {
		return Subscription{}, err
	}
	next.ID = id
	if err := ValidateSubscription(next); err != nil {
		return Subscription{}, err
	}
	if _, ok := s.channels[next.ChannelID]; !ok {
		return Subscription{}, ErrUnknownChannel
	}
	s.subscriptions[id] = next
	return next, nil
}

// GetSubscription retrieves a subscription by id.
func (s *Store) GetSubscription(id string) (Subscription, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	sub, ok := s.subscriptions[id]
	return sub, ok
}

// ListSubscriptions returns all subscriptions ordered by id.
func (s *Store) ListSubscriptions() []Subscription {
	s.mu.RLock()
	out := make([]Subscription, 0, len(s.subscriptions))
	for _, sub := range s.subscriptions {
		out = append(out, sub)
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// SubscriptionsForAlert returns the enabled subscriptions for alertID.
func (s *Store) SubscriptionsForAlert(alertID string) []Subscription {
	s.mu.RLock()
	var out []Subscription
	for _, sub := range s.subscriptions {
		if sub.Enabled && sub.AlertID == alertID {
			out = append(out, sub)
		}
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// DeleteSubscription removes a subscription.
func (s *Store) DeleteSubscription(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subscriptions[id]; !ok {
		return false
	}
	delete(s.subscriptions, id)
	return true
}

// Redacted returns a copy of the channel that is safe to expose over the API:
// URL credentials, query values, Slack/secret-looking path tokens and
// secret-looking metadata values are replaced with RedactedValue.
func (c Channel) Redacted() Channel {
	out := copyChannel(c)
	if c.Type == ChannelWebhook || c.Type == ChannelSlack {
		out.Target = redactURL(c.Target, c.Type == ChannelSlack)
	}
	for k, v := range out.Metadata {
		if v != "" && isSecretKey(k) {
			out.Metadata[k] = RedactedValue
		}
	}
	return out
}

var secretKeyParts = []string{"secret", "token", "password", "passwd", "key", "auth", "credential", "signature", "cookie", "session"}

func isSecretKey(k string) bool {
	k = strings.ToLower(k)
	for _, p := range secretKeyParts {
		if strings.Contains(k, p) {
			return true
		}
	}
	return false
}

// redactURL keeps scheme, host and non-secret path segments of a URL.
func redactURL(raw string, slack bool) string {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		if raw == "" {
			return ""
		}
		return RedactedValue
	}
	var b strings.Builder
	b.WriteString(u.Scheme + "://")
	if u.User != nil {
		b.WriteString(RedactedValue + "@")
	}
	b.WriteString(u.Host)
	segs := strings.Split(strings.TrimPrefix(u.EscapedPath(), "/"), "/")
	host := strings.ToLower(u.Hostname())
	isSlackHost := host == "hooks.slack.com" || host == "hooks.slack-gov.com"
	for i, seg := range segs {
		switch {
		case (slack || isSlackHost) && i > 0:
			// hooks.slack.com/services/T000/B000/XXXX: everything after the
			// first segment identifies and authorises the webhook.
			segs[i] = RedactedValue
		case looksLikeToken(seg):
			segs[i] = RedactedValue
		}
	}
	if u.Path != "" {
		b.WriteString("/" + strings.Join(segs, "/"))
	}
	if u.RawQuery != "" {
		q := u.Query()
		keys := make([]string, 0, len(q))
		for k := range q {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			parts = append(parts, url.QueryEscape(k)+"="+RedactedValue)
		}
		b.WriteString("?" + strings.Join(parts, "&"))
	}
	return b.String()
}

// looksLikeToken flags long, high-entropy looking path segments (e.g. the
// token of Discord/Teams style webhook URLs).
func looksLikeToken(seg string) bool {
	if len(seg) >= 32 {
		return true
	}
	if len(seg) < 16 {
		return false
	}
	var letters, digits bool
	for _, c := range seg {
		switch {
		case c >= '0' && c <= '9':
			digits = true
		case (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z'):
			letters = true
		}
	}
	return letters && digits
}
