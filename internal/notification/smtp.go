package notification

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"mime"
	"mime/quotedprintable"
	"net"
	"net/mail"
	"net/smtp"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

// Environment variables read by SMTPOptionsFromEnv.
const (
	EnvSMTPAddr        = "AEROLLM_SMTP_ADDR"         // host:port of the SMTP server (required)
	EnvSMTPUser        = "AEROLLM_SMTP_USER"         // AUTH PLAIN user (optional)
	EnvSMTPPassword    = "AEROLLM_SMTP_PASSWORD"     // AUTH PLAIN password (optional)
	EnvSMTPFrom        = "AEROLLM_SMTP_FROM"         // envelope/header sender (required)
	EnvSMTPImplicitTLS = "AEROLLM_SMTP_IMPLICIT_TLS" // "true" for SMTPS (default: true only on port 465)
	EnvSMTPHost        = "AEROLLM_SMTP_TLS_SERVER_NAME"
)

const (
	defaultSMTPTimeout = 30 * time.Second
	maxSMTPDialTimeout = 10 * time.Second
	maxEmailSubject    = 200      // runes
	maxEmailBodyBytes  = 64 << 10 // of message text
)

var (
	// ErrDeliveryFailed wraps failures reported by an email or SMS backend
	// (connection problems, rejected recipients, provider errors).
	ErrDeliveryFailed = errors.New("notification: delivery failed")
	// ErrSMTPTLSRequired is returned when the SMTP server does not offer
	// STARTTLS: mail (and credentials) are never sent in plaintext.
	ErrSMTPTLSRequired = errors.New("notification: SMTP server does not support STARTTLS; refusing plaintext delivery")
)

// SMTPOptions configures SMTPSender.
type SMTPOptions struct {
	// Addr is the server's host:port, e.g. "smtp.example.com:587".
	Addr string
	// Host is the TLS server name (and the name AUTH PLAIN is bound to).
	// Defaults to the host part of Addr.
	Host string
	// Username and Password enable AUTH PLAIN, which is only ever attempted
	// over TLS. Leave Username empty for unauthenticated relays.
	Username string
	Password string
	// From is the sender address ("alerts@example.com" or
	// "AeroLLM <alerts@example.com>").
	From string
	// ImplicitTLS connects with TLS from the first byte (SMTPS, port 465).
	// Otherwise STARTTLS is mandatory.
	ImplicitTLS bool
	// Timeout bounds the whole SMTP session (default 30s).
	Timeout time.Duration
	// LocalName is sent in EHLO (default "localhost").
	LocalName string
	// TLSConfig optionally overrides TLS settings (e.g. RootCAs in tests).
	// ServerName defaults to Host; MinVersion is raised to TLS 1.2.
	TLSConfig *tls.Config
}

// SMTPOptionsFromEnv builds SMTPOptions from AEROLLM_SMTP_* variables. ok is
// false when AEROLLM_SMTP_ADDR or AEROLLM_SMTP_FROM is unset. Implicit TLS is
// enabled by AEROLLM_SMTP_IMPLICIT_TLS=true, or by default on port 465.
func SMTPOptionsFromEnv() (SMTPOptions, bool) {
	o := SMTPOptions{
		Addr:     strings.TrimSpace(os.Getenv(EnvSMTPAddr)),
		Host:     strings.TrimSpace(os.Getenv(EnvSMTPHost)),
		Username: os.Getenv(EnvSMTPUser),
		Password: os.Getenv(EnvSMTPPassword),
		From:     strings.TrimSpace(os.Getenv(EnvSMTPFrom)),
	}
	if v := strings.TrimSpace(os.Getenv(EnvSMTPImplicitTLS)); v != "" {
		o.ImplicitTLS, _ = strconv.ParseBool(v)
	} else if _, port, err := net.SplitHostPort(o.Addr); err == nil && port == "465" {
		o.ImplicitTLS = true
	}
	return o, o.Addr != "" && o.From != ""
}

// SMTPSender delivers email notifications over SMTP with mandatory TLS. It
// implements EmailSender and is safe for concurrent use (one connection per
// message).
type SMTPSender struct {
	opts SMTPOptions
	from *mail.Address
}

// NewSMTPSender validates opts and returns a sender.
func NewSMTPSender(opts SMTPOptions) (*SMTPSender, error) {
	host, port, err := net.SplitHostPort(opts.Addr)
	if err != nil || host == "" {
		return nil, errors.New("notification: smtp: Addr must be host:port")
	}
	if n, err := strconv.Atoi(port); err != nil || n <= 0 || n > 65535 {
		return nil, errors.New("notification: smtp: invalid port in Addr")
	}
	if opts.Host == "" {
		opts.Host = host
	}
	for name, v := range map[string]string{"Addr": opts.Addr, "Host": opts.Host, "Username": opts.Username, "Password": opts.Password, "From": opts.From, "LocalName": opts.LocalName} {
		if strings.ContainsAny(v, "\r\n\x00") {
			return nil, fmt.Errorf("notification: smtp: %s must not contain CR, LF or NUL", name)
		}
	}
	if opts.Username == "" && opts.Password != "" {
		return nil, errors.New("notification: smtp: Password set without Username")
	}
	from, err := mail.ParseAddress(opts.From)
	if err != nil {
		return nil, errors.New("notification: smtp: From is not a valid address")
	}
	if opts.Timeout <= 0 {
		opts.Timeout = defaultSMTPTimeout
	}
	if opts.LocalName == "" {
		opts.LocalName = "localhost"
	}
	return &SMTPSender{opts: opts, from: from}, nil
}

func (s *SMTPSender) tlsConfig() *tls.Config {
	var cfg *tls.Config
	if s.opts.TLSConfig != nil {
		cfg = s.opts.TLSConfig.Clone()
	} else {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" {
		cfg.ServerName = s.opts.Host
	}
	if cfg.MinVersion < tls.VersionTLS12 {
		cfg.MinVersion = tls.VersionTLS12
	}
	return cfg
}

// SendEmail implements EmailSender. to must be a bare address.
func (s *SMTPSender) SendEmail(ctx context.Context, to string, msg Message) error {
	if err := validateEmail(to); err != nil {
		return err
	}
	body, err := s.buildMessage(to, msg, time.Now())
	if err != nil {
		return err
	}
	if err := s.send(ctx, to, body); err != nil {
		return fmt.Errorf("%w: smtp: %w", ErrDeliveryFailed, err)
	}
	return nil
}

func (s *SMTPSender) send(ctx context.Context, to string, body []byte) (err error) {
	deadline := time.Now().Add(s.opts.Timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	dialer := &net.Dialer{Timeout: min(s.opts.Timeout, maxSMTPDialTimeout), Deadline: deadline}
	raw, err := dialer.DialContext(ctx, "tcp", s.opts.Addr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer raw.Close()
	if err := raw.SetDeadline(deadline); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	// Abort blocked I/O as soon as ctx is cancelled.
	stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Unix(1, 0)) })
	defer stop()
	defer func() {
		if err != nil && ctx.Err() != nil {
			err = errors.Join(err, ctx.Err())
		}
	}()

	conn := raw
	if s.opts.ImplicitTLS {
		tc := tls.Client(raw, s.tlsConfig())
		if err := tc.HandshakeContext(ctx); err != nil {
			return fmt.Errorf("tls handshake: %w", err)
		}
		conn = tc
	}
	c, err := smtp.NewClient(conn, s.opts.Host)
	if err != nil {
		return fmt.Errorf("greeting: %w", err)
	}
	defer c.Close()
	if err := c.Hello(s.opts.LocalName); err != nil {
		return fmt.Errorf("ehlo: %w", err)
	}
	if !s.opts.ImplicitTLS {
		if ok, _ := c.Extension("STARTTLS"); !ok {
			return ErrSMTPTLSRequired
		}
		if err := c.StartTLS(s.tlsConfig()); err != nil {
			return fmt.Errorf("starttls: %w", err)
		}
	}
	if _, ok := c.TLSConnectionState(); !ok {
		return ErrSMTPTLSRequired
	}
	if s.opts.Username != "" {
		if ok, _ := c.Extension("AUTH"); !ok {
			return errors.New("server does not support AUTH")
		}
		if err := c.Auth(smtp.PlainAuth("", s.opts.Username, s.opts.Password, s.opts.Host)); err != nil {
			return fmt.Errorf("auth: %w", err)
		}
	}
	if err := c.Mail(s.from.Address); err != nil {
		return fmt.Errorf("mail from: %w", err)
	}
	if err := c.Rcpt(to); err != nil {
		return fmt.Errorf("rcpt to: %w", err)
	}
	w, err := c.Data()
	if err != nil {
		return fmt.Errorf("data: %w", err)
	}
	if _, err := w.Write(body); err != nil {
		_ = w.Close()
		return fmt.Errorf("data: %w", err)
	}
	if err := w.Close(); err != nil {
		return fmt.Errorf("data: %w", err)
	}
	// The message is accepted once DATA completes; a failed QUIT is harmless.
	_ = c.Quit()
	return nil
}

// headerSafe strips control characters (including CR/LF) so user supplied
// text can never start a new header line, and bounds its length.
func headerSafe(s string, maxRunes int) string {
	s = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > maxRunes {
		s = string([]rune(s)[:maxRunes-1]) + "…"
	}
	return s
}

func emailSubject(msg Message) string {
	var b strings.Builder
	if msg.Severity != "" {
		b.WriteString("[" + strings.ToUpper(msg.Severity) + "] ")
	}
	switch {
	case msg.Title != "":
		b.WriteString(msg.Title)
	case msg.AlertID != "":
		b.WriteString("Alert " + msg.AlertID)
	default:
		b.WriteString("AeroLLM notification")
	}
	return headerSafe(b.String(), maxEmailSubject)
}

func emailText(msg Message) string {
	var b strings.Builder
	if msg.Title != "" {
		b.WriteString(msg.Title + "\n\n")
	}
	text := msg.Text
	if len(text) > maxEmailBodyBytes {
		text = strings.ToValidUTF8(text[:maxEmailBodyBytes], "") + "\n[truncated]"
	}
	if text != "" {
		b.WriteString(text + "\n\n")
	}
	if msg.Severity != "" {
		b.WriteString("Severity: " + msg.Severity + "\n")
	}
	if msg.AlertID != "" {
		b.WriteString("Alert: " + msg.AlertID + "\n")
	}
	if !msg.Timestamp.IsZero() {
		b.WriteString("Time: " + msg.Timestamp.UTC().Format(time.RFC3339) + "\n")
	}
	if len(msg.Labels) > 0 {
		keys := make([]string, 0, len(msg.Labels))
		for k := range msg.Labels {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		b.WriteString("Labels:\n")
		for _, k := range keys {
			b.WriteString("  " + k + ": " + msg.Labels[k] + "\n")
		}
	}
	return b.String()
}

// buildMessage renders an RFC 5322 message. Every header value is either
// validated (addresses), generated, or stripped of control characters and
// RFC 2047 encoded (subject), so message content cannot inject headers.
// The body is quoted-printable; dot-stuffing is done by smtp's DATA writer.
func (s *SMTPSender) buildMessage(to string, msg Message, now time.Time) ([]byte, error) {
	var id [12]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, fmt.Errorf("notification: smtp: message id: %w", err)
	}
	domain := "aerollm.local"
	if at := strings.LastIndexByte(s.from.Address, '@'); at >= 0 && at < len(s.from.Address)-1 {
		domain = s.from.Address[at+1:]
	}
	var buf bytes.Buffer
	hdr := func(k, v string) { buf.WriteString(k + ": " + v + "\r\n") }
	hdr("From", s.from.String())
	hdr("To", (&mail.Address{Address: to}).String())
	hdr("Subject", mime.QEncoding.Encode("utf-8", emailSubject(msg)))
	hdr("Date", now.Format(time.RFC1123Z))
	hdr("Message-ID", "<"+hex.EncodeToString(id[:])+"@"+domain+">")
	hdr("MIME-Version", "1.0")
	hdr("Content-Type", `text/plain; charset="utf-8"`)
	hdr("Content-Transfer-Encoding", "quoted-printable")
	hdr("Auto-Submitted", "auto-generated")
	buf.WriteString("\r\n")
	qp := quotedprintable.NewWriter(&buf)
	if _, err := qp.Write([]byte(emailText(msg))); err != nil {
		return nil, fmt.Errorf("notification: smtp: encode body: %w", err)
	}
	if err := qp.Close(); err != nil {
		return nil, fmt.Errorf("notification: smtp: encode body: %w", err)
	}
	buf.WriteString("\r\n")
	return buf.Bytes(), nil
}
