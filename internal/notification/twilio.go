package notification

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

// Environment variables read by TwilioOptionsFromEnv.
const (
	EnvTwilioAccountSID = "AEROLLM_TWILIO_ACCOUNT_SID"
	EnvTwilioAuthToken  = "AEROLLM_TWILIO_AUTH_TOKEN"
	EnvTwilioFrom       = "AEROLLM_TWILIO_FROM" // E.164 number or Messaging Service SID (MG...)
)

const (
	twilioBaseURL        = "https://api.twilio.com"
	defaultTwilioTimeout = 10 * time.Second
	maxSMSBodyRunes      = 1600 // Twilio's limit for a (concatenated) message
	maxTwilioResponse    = 64 << 10
)

var (
	twilioAccountSIDRe = regexp.MustCompile(`^AC[0-9a-fA-F]{32}$`)
	twilioServiceSIDRe = regexp.MustCompile(`^MG[0-9a-fA-F]{32}$`)
)

// TwilioOptions configures TwilioSender.
type TwilioOptions struct {
	// AccountSID ("AC" + 32 hex characters) and AuthToken authenticate
	// requests with HTTP basic auth.
	AccountSID string
	AuthToken  string
	// From is the sending E.164 number or a Messaging Service SID ("MG...").
	From string
	// Timeout bounds each API request (default 10s).
	Timeout time.Duration
}

// TwilioOptionsFromEnv builds TwilioOptions from AEROLLM_TWILIO_* variables.
// ok is false unless the account SID, auth token and sender are all set.
func TwilioOptionsFromEnv() (TwilioOptions, bool) {
	o := TwilioOptions{
		AccountSID: strings.TrimSpace(os.Getenv(EnvTwilioAccountSID)),
		AuthToken:  strings.TrimSpace(os.Getenv(EnvTwilioAuthToken)),
		From:       strings.TrimSpace(os.Getenv(EnvTwilioFrom)),
	}
	return o, o.AccountSID != "" && o.AuthToken != "" && o.From != ""
}

// TwilioSender delivers SMS notifications through the Twilio Messages API.
// It only ever talks to api.twilio.com over https, never follows redirects,
// and its errors never contain the auth token. It implements SMSSender.
type TwilioSender struct {
	opts    TwilioOptions
	client  *http.Client
	baseURL string // overridden in tests only
}

// NewTwilioSender validates opts and returns a sender.
func NewTwilioSender(opts TwilioOptions) (*TwilioSender, error) {
	if !twilioAccountSIDRe.MatchString(opts.AccountSID) {
		return nil, errors.New("notification: twilio: AccountSID must be AC followed by 32 hex characters")
	}
	if opts.AuthToken == "" || strings.ContainsAny(opts.AuthToken, "\r\n\x00") {
		return nil, errors.New("notification: twilio: AuthToken is required")
	}
	if !e164Re.MatchString(opts.From) && !twilioServiceSIDRe.MatchString(opts.From) {
		return nil, errors.New("notification: twilio: From must be an E.164 number or a Messaging Service SID")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultTwilioTimeout
	}
	transport := &http.Transport{
		// The destination host is fixed, so an egress proxy cannot be abused
		// to reach arbitrary targets.
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          4,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: opts.Timeout,
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   opts.Timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return &TwilioSender{opts: opts, client: client, baseURL: twilioBaseURL}, nil
}

// twilioAPIError is Twilio's JSON error body.
type twilioAPIError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// SendSMS implements SMSSender. to must be an E.164 number.
func (t *TwilioSender) SendSMS(ctx context.Context, to string, msg Message) error {
	if !e164Re.MatchString(to) {
		return invalid("target", "must be an E.164 phone number such as +14155550100")
	}
	form := url.Values{}
	form.Set("To", to)
	if twilioServiceSIDRe.MatchString(t.opts.From) {
		form.Set("MessagingServiceSid", t.opts.From)
	} else {
		form.Set("From", t.opts.From)
	}
	form.Set("Body", smsText(msg))
	endpoint := t.baseURL + "/2010-04-01/Accounts/" + t.opts.AccountSID + "/Messages.json"

	rctx, cancel := context.WithTimeout(ctx, t.opts.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(rctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("%w: twilio: build request", ErrDeliveryFailed)
	}
	req.SetBasicAuth(t.opts.AccountSID, t.opts.AuthToken)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "AeroLLM-Notifier/1.0")
	resp, err := t.client.Do(req)
	if err != nil {
		// *url.Error embeds the request URL; keep only the cause.
		var uerr *url.Error
		if errors.As(err, &uerr) {
			err = uerr.Err
		}
		return fmt.Errorf("%w: twilio: request failed: %s", ErrDeliveryFailed, t.scrub(err.Error()))
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, maxTwilioResponse))
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil
	}
	var apiErr twilioAPIError
	if json.Unmarshal(data, &apiErr) == nil && (apiErr.Code != 0 || apiErr.Message != "") {
		return fmt.Errorf("%w: twilio: status %d: error %d: %s", ErrDeliveryFailed, resp.StatusCode, apiErr.Code, t.scrub(headerSafe(apiErr.Message, 200)))
	}
	return fmt.Errorf("%w: twilio: status %d", ErrDeliveryFailed, resp.StatusCode)
}

// scrub removes the auth token from text that may echo request details.
func (t *TwilioSender) scrub(s string) string {
	if t.opts.AuthToken != "" {
		s = strings.ReplaceAll(s, t.opts.AuthToken, RedactedValue)
	}
	return s
}

func smsText(msg Message) string {
	var b strings.Builder
	if msg.Severity != "" {
		b.WriteString("[" + strings.ToUpper(msg.Severity) + "] ")
	}
	if msg.Title != "" {
		b.WriteString(msg.Title)
	}
	if msg.Text != "" {
		if b.Len() > 0 {
			b.WriteString(": ")
		}
		b.WriteString(msg.Text)
	}
	if b.Len() == 0 {
		b.WriteString("AeroLLM alert " + msg.AlertID)
	}
	s := strings.ToValidUTF8(b.String(), "")
	if utf8.RuneCountInString(s) > maxSMSBodyRunes {
		s = string([]rune(s)[:maxSMSBodyRunes-1]) + "…"
	}
	return s
}
