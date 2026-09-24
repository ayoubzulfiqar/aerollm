package notification

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ErrChannelNotImplemented is returned when a channel type has no delivery
// backend configured (email and SMS need an EmailSender / SMSSender).
var ErrChannelNotImplemented = errors.New("notification: channel type not implemented")

// ErrChannelDisabled is returned when sending to a disabled channel.
var ErrChannelDisabled = errors.New("notification: channel is disabled")

// Headers set on webhook deliveries.
const (
	HeaderSignature = "X-AeroLLM-Signature"
	HeaderTimestamp = "X-AeroLLM-Timestamp"
	// SigningSecretKey is the channel Metadata key holding the HMAC secret.
	SigningSecretKey = "signing_secret"
)

const (
	defaultAttemptTimeout = 10 * time.Second
	defaultMaxAttempts    = 3
	defaultBaseBackoff    = 500 * time.Millisecond
	defaultMaxBackoff     = 5 * time.Second
	defaultNotifyParallel = 8
	maxResponseDrain      = 64 << 10
	maxAttemptsCap        = 10
)

// Message is a notification payload. Webhook channels receive it as JSON.
type Message struct {
	AlertID   string            `json:"alert_id"`
	Title     string            `json:"title"`
	Text      string            `json:"text"`
	Severity  string            `json:"severity,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
	Labels    map[string]string `json:"labels,omitempty"`
}

// EmailSender delivers email notifications.
type EmailSender interface {
	SendEmail(ctx context.Context, to string, msg Message) error
}

// SMSSender delivers SMS notifications.
type SMSSender interface {
	SendSMS(ctx context.Context, to string, msg Message) error
}

// DispatcherOptions configures delivery.
type DispatcherOptions struct {
	// Timeout bounds each HTTP attempt (default 10s).
	Timeout time.Duration
	// MaxAttempts is the number of tries per delivery (default 3, max 10).
	MaxAttempts int
	// BaseBackoff and MaxBackoff shape exponential backoff with jitter
	// between retries (defaults 500ms / 5s).
	BaseBackoff time.Duration
	MaxBackoff  time.Duration
	// AllowPrivateNetworks disables the loopback/private/metadata address
	// block (local development and tests only).
	AllowPrivateNetworks bool
	// AllowInsecureHTTP permits http:// targets.
	AllowInsecureHTTP bool
	// EmailSender / SMSSender back the email and sms channel types. When nil,
	// sends to such channels fail with ErrChannelNotImplemented. SMTPSender
	// and TwilioSender implement them; SendersFromEnv builds both from the
	// environment.
	EmailSender EmailSender
	SMSSender   SMSSender
	// UserAgent sent with HTTP deliveries.
	UserAgent string
	// Parallelism bounds concurrent deliveries in Notify (default 8).
	Parallelism int
}

// Dispatcher delivers messages to channels stored in a Store.
type Dispatcher struct {
	store  *Store
	opts   DispatcherOptions
	client *http.Client
}

// NewDispatcher creates a dispatcher. The HTTP client never follows
// redirects, ignores proxy environment variables (a proxy would hide the
// real destination from the SSRF dial check) and refuses to connect to
// blocked addresses after DNS resolution.
func NewDispatcher(store *Store, opts DispatcherOptions) *Dispatcher {
	if opts.Timeout <= 0 {
		opts.Timeout = defaultAttemptTimeout
	}
	if opts.MaxAttempts <= 0 {
		opts.MaxAttempts = defaultMaxAttempts
	}
	if opts.MaxAttempts > maxAttemptsCap {
		opts.MaxAttempts = maxAttemptsCap
	}
	if opts.BaseBackoff <= 0 {
		opts.BaseBackoff = defaultBaseBackoff
	}
	if opts.MaxBackoff <= 0 {
		opts.MaxBackoff = defaultMaxBackoff
	}
	if opts.MaxBackoff < opts.BaseBackoff {
		opts.MaxBackoff = opts.BaseBackoff
	}
	if opts.UserAgent == "" {
		opts.UserAgent = "AeroLLM-Notifier/1.0"
	}
	if opts.Parallelism <= 0 {
		opts.Parallelism = defaultNotifyParallel
	}
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,
		KeepAlive: 30 * time.Second,
		Control:   ssrfControl(opts.AllowPrivateNetworks),
	}
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           dialer.DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: opts.Timeout,
		ExpectContinueTimeout: time.Second,
	}
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &Dispatcher{store: store, opts: opts, client: client}
}

// Send delivers msg to a single channel by id.
func (d *Dispatcher) Send(ctx context.Context, channelID string, msg Message) error {
	if d == nil || d.store == nil {
		return errors.New("notification: dispatcher has no store")
	}
	ch, ok := d.store.GetChannel(channelID)
	if !ok {
		return fmt.Errorf("channel %q: %w", channelID, ErrNotFound)
	}
	return d.Deliver(ctx, ch, msg)
}

// Notify delivers msg to every enabled subscription of alertID whose channel
// exists and is enabled. Deliveries run concurrently (bounded); failures are
// combined with errors.Join. No subscriptions means nothing to do (nil).
func (d *Dispatcher) Notify(ctx context.Context, alertID string, msg Message) error {
	_, err := d.NotifyWithResults(ctx, alertID, msg)
	return err
}

// DeliveryResult reports the outcome of delivering a message to one channel.
// Error is a short, secret-free classification (see ErrorClass).
type DeliveryResult struct {
	ChannelID string      `json:"channel_id"`
	Type      ChannelType `json:"type"`
	Delivered bool        `json:"delivered"`
	Error     string      `json:"error,omitempty"`
}

// ErrorClass maps a delivery error to a stable, secret-free label:
// "not_implemented", "disabled", "not_found", "blocked_destination",
// "invalid_channel", "timeout", "canceled" or "delivery_failed" ("" for nil).
func ErrorClass(err error) string {
	var verr *ValidationError
	switch {
	case err == nil:
		return ""
	case errors.Is(err, ErrChannelNotImplemented):
		return "not_implemented"
	case errors.Is(err, ErrChannelDisabled):
		return "disabled"
	case errors.Is(err, ErrNotFound):
		return "not_found"
	case errors.Is(err, ErrBlockedDestination):
		return "blocked_destination"
	case errors.As(err, &verr):
		return "invalid_channel"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return "delivery_failed"
	}
}

// NotifyWithResults is Notify that also reports the per-channel outcome, in
// channel order of the matching subscriptions. An empty result means no
// enabled subscription with an existing, enabled channel matched alertID.
func (d *Dispatcher) NotifyWithResults(ctx context.Context, alertID string, msg Message) ([]DeliveryResult, error) {
	if d == nil || d.store == nil {
		return nil, errors.New("notification: dispatcher has no store")
	}
	if msg.AlertID == "" {
		msg.AlertID = alertID
	}
	subs := d.store.SubscriptionsForAlert(alertID)
	seen := make(map[string]bool, len(subs))
	var targets []Channel
	for _, sub := range subs {
		if seen[sub.ChannelID] {
			continue // one delivery per channel even if subscribed twice
		}
		seen[sub.ChannelID] = true
		ch, ok := d.store.GetChannel(sub.ChannelID)
		if !ok || !ch.Enabled {
			continue
		}
		targets = append(targets, ch)
	}
	if len(targets) == 0 {
		return nil, nil
	}
	results := make([]DeliveryResult, len(targets))
	for i, ch := range targets {
		results[i] = DeliveryResult{ChannelID: ch.ID, Type: ch.Type}
	}
	var (
		mu   sync.Mutex
		errs []error
		wg   sync.WaitGroup
		sem  = make(chan struct{}, d.opts.Parallelism)
	)
	for i, ch := range targets {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			mu.Lock()
			errs = append(errs, ctx.Err())
			mu.Unlock()
			wg.Wait()
			for j := i; j < len(targets); j++ {
				results[j].Error = ErrorClass(ctx.Err())
			}
			return results, errors.Join(errs...)
		}
		wg.Add(1)
		go func(i int, ch Channel) {
			defer wg.Done()
			defer func() { <-sem }()
			err := d.Deliver(ctx, ch, msg)
			results[i].Delivered = err == nil
			results[i].Error = ErrorClass(err)
			if err != nil {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(i, ch)
	}
	wg.Wait()
	return results, errors.Join(errs...)
}

// Deliver sends msg to the given channel definition.
func (d *Dispatcher) Deliver(ctx context.Context, ch Channel, msg Message) error {
	if !ch.Enabled {
		return fmt.Errorf("channel %q: %w", ch.ID, ErrChannelDisabled)
	}
	if msg.Timestamp.IsZero() {
		msg.Timestamp = time.Now().UTC()
	}
	opts := Options{AllowInsecureHTTP: d.opts.AllowInsecureHTTP, AllowPrivateNetworks: d.opts.AllowPrivateNetworks}
	if err := ValidateChannel(ch, opts); err != nil {
		return fmt.Errorf("channel %q: %w", ch.ID, err)
	}
	switch ch.Type {
	case ChannelWebhook:
		body, err := json.Marshal(msg)
		if err != nil {
			return fmt.Errorf("channel %q: encode message: %w", ch.ID, err)
		}
		headers := http.Header{}
		ts := strconv.FormatInt(time.Now().Unix(), 10)
		headers.Set(HeaderTimestamp, ts)
		if secret := ch.Metadata[SigningSecretKey]; secret != "" {
			headers.Set(HeaderSignature, Sign(secret, ts, body))
		}
		return d.post(ctx, ch, body, headers)
	case ChannelSlack:
		body, err := json.Marshal(map[string]string{"text": slackText(msg)})
		if err != nil {
			return fmt.Errorf("channel %q: encode message: %w", ch.ID, err)
		}
		return d.post(ctx, ch, body, http.Header{})
	case ChannelEmail:
		if d.opts.EmailSender == nil {
			return fmt.Errorf("channel %q (email): %w: no EmailSender configured", ch.ID, ErrChannelNotImplemented)
		}
		if err := d.opts.EmailSender.SendEmail(ctx, ch.Target, msg); err != nil {
			return fmt.Errorf("channel %q (email): %w", ch.ID, err)
		}
		return nil
	case ChannelSMS:
		if d.opts.SMSSender == nil {
			return fmt.Errorf("channel %q (sms): %w: no SMSSender configured", ch.ID, ErrChannelNotImplemented)
		}
		if err := d.opts.SMSSender.SendSMS(ctx, ch.Target, msg); err != nil {
			return fmt.Errorf("channel %q (sms): %w", ch.ID, err)
		}
		return nil
	default:
		return fmt.Errorf("channel %q (%s): %w", ch.ID, ch.Type, ErrChannelNotImplemented)
	}
}

// Sign computes the webhook signature header value:
// "sha256=" + hex(HMAC-SHA256(secret, timestamp + "." + body)).
// Receivers should recompute it and compare with hmac.Equal, and reject
// stale timestamps to prevent replay.
func Sign(secret, timestamp string, body []byte) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(timestamp))
	mac.Write([]byte("."))
	mac.Write(body)
	return "sha256=" + hex.EncodeToString(mac.Sum(nil))
}

// slackEscape applies Slack's mrkdwn control character escaping.
func slackEscape(s string) string {
	return strings.NewReplacer("&", "&amp;", "<", "&lt;", ">", "&gt;").Replace(s)
}

func slackText(msg Message) string {
	var b strings.Builder
	if msg.Severity != "" {
		b.WriteString("[" + strings.ToUpper(slackEscape(msg.Severity)) + "] ")
	}
	if msg.Title != "" {
		b.WriteString("*" + slackEscape(msg.Title) + "*")
	}
	if msg.Text != "" {
		if b.Len() > 0 {
			b.WriteString("\n")
		}
		b.WriteString(slackEscape(msg.Text))
	}
	if b.Len() == 0 {
		b.WriteString(slackEscape(msg.AlertID))
	}
	return b.String()
}

// deliveryError is a delivery failure that never contains the raw target URL.
type deliveryError struct {
	channelID string
	target    string // redacted
	msg       string
	retryable bool
	err       error
}

func (e *deliveryError) Error() string {
	s := fmt.Sprintf("notification: deliver to channel %q (%s): %s", e.channelID, e.target, e.msg)
	return s
}

func (e *deliveryError) Unwrap() error { return e.err }

func (d *Dispatcher) post(ctx context.Context, ch Channel, body []byte, headers http.Header) error {
	redacted := ch.Redacted().Target
	var lastErr error
	for attempt := 1; attempt <= d.opts.MaxAttempts; attempt++ {
		if attempt > 1 {
			wait := d.backoff(attempt - 1)
			var de *deliveryError
			if errors.As(lastErr, &de) && de.err != nil {
				var ra retryAfter
				if errors.As(de.err, &ra) && time.Duration(ra) > wait && time.Duration(ra) <= d.opts.MaxBackoff {
					wait = time.Duration(ra)
				}
			}
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return errors.Join(lastErr, ctx.Err())
			case <-t.C:
			}
		}
		err := d.postOnce(ctx, ch.ID, ch.Target, redacted, body, headers)
		if err == nil {
			return nil
		}
		lastErr = err
		var de *deliveryError
		if !errors.As(err, &de) || !de.retryable || ctx.Err() != nil {
			break
		}
	}
	return lastErr
}

// retryAfter carries a server supplied Retry-After delay.
type retryAfter time.Duration

func (r retryAfter) Error() string { return "retry after " + time.Duration(r).String() }

func (d *Dispatcher) backoff(retry int) time.Duration {
	b := d.opts.BaseBackoff << (retry - 1)
	if b <= 0 || b > d.opts.MaxBackoff {
		b = d.opts.MaxBackoff
	}
	// Full jitter in [b/2, b].
	half := b / 2
	if half <= 0 {
		return b
	}
	return half + rand.N(half+1)
}

func (d *Dispatcher) postOnce(ctx context.Context, channelID, target, redacted string, body []byte, headers http.Header) error {
	actx, cancel := context.WithTimeout(ctx, d.opts.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(actx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		return &deliveryError{channelID: channelID, target: redacted, msg: "invalid request"}
	}
	for k, vs := range headers {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", d.opts.UserAgent)
	resp, err := d.client.Do(req)
	if err != nil {
		// Strip *url.Error, whose message embeds the full (secret) URL.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		if errors.Is(err, ErrBlockedDestination) {
			return &deliveryError{channelID: channelID, target: redacted, msg: "destination address is not allowed", err: ErrBlockedDestination}
		}
		retry := ctx.Err() == nil
		return &deliveryError{channelID: channelID, target: redacted, msg: "request failed: " + sanitizeNetErr(err, target), retryable: retry, err: err}
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxResponseDrain))
	_ = resp.Body.Close()
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return &deliveryError{channelID: channelID, target: redacted, msg: fmt.Sprintf("unexpected redirect (status %d); redirects are not followed", resp.StatusCode)}
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
		de := &deliveryError{channelID: channelID, target: redacted, msg: fmt.Sprintf("status %d", resp.StatusCode), retryable: true}
		if s := resp.Header.Get("Retry-After"); s != "" {
			if secs, err := strconv.Atoi(s); err == nil && secs >= 0 {
				de.err = retryAfter(time.Duration(secs) * time.Second)
			}
		}
		return de
	default:
		return &deliveryError{channelID: channelID, target: redacted, msg: fmt.Sprintf("status %d", resp.StatusCode)}
	}
}

// sanitizeNetErr renders a transport error without the target URL.
func sanitizeNetErr(err error, target string) string {
	s := err.Error()
	if target != "" {
		s = strings.ReplaceAll(s, target, "[target]")
	}
	return s
}
