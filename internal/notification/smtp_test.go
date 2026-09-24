package notification

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"errors"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// testTLS returns a server certificate for "localhost"/127.0.0.1 and a
// client config trusting it.
func testTLS(t *testing.T) (server, client *tls.Config) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.ParseIP("127.0.0.1")},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	server = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key, Leaf: cert}}, MinVersion: tls.VersionTLS12}
	client = &tls.Config{RootCAs: pool}
	return server, client
}

// fakeSMTP is a minimal SMTP server for tests.
type fakeSMTP struct {
	t           *testing.T
	ln          net.Listener
	tlsConfig   *tls.Config
	offerTLS    bool // advertise STARTTLS
	implicitTLS bool
	user, pass  string
	stall       bool // never send the greeting

	mu       sync.Mutex
	sessions []smtpSession
	wg       sync.WaitGroup
}

type smtpSession struct {
	tls       bool
	authed    bool
	authTried bool
	from      string
	rcpt      []string
	data      string
	commands  []string
}

func newFakeSMTP(t *testing.T, f *fakeSMTP) *fakeSMTP {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f.t = t
	if f.implicitTLS {
		ln = tls.NewListener(ln, f.tlsConfig)
	}
	f.ln = ln
	f.wg.Add(1)
	go func() {
		defer f.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			f.wg.Add(1)
			go func() {
				defer f.wg.Done()
				f.serve(c)
			}()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		f.wg.Wait()
	})
	return f
}

func (f *fakeSMTP) addr() string { return f.ln.Addr().String() }

func (f *fakeSMTP) last() smtpSession {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.sessions) == 0 {
		return smtpSession{}
	}
	return f.sessions[len(f.sessions)-1]
}

func (f *fakeSMTP) serve(conn net.Conn) {
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	sess := smtpSession{tls: f.implicitTLS}
	defer func() {
		f.mu.Lock()
		f.sessions = append(f.sessions, sess)
		f.mu.Unlock()
	}()
	if f.stall {
		buf := make([]byte, 1)
		_, _ = conn.Read(buf)
		return
	}
	r := bufio.NewReader(conn)
	w := func(s string) { _, _ = conn.Write([]byte(s + "\r\n")) }
	w("220 fake ESMTP")
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		verb := strings.ToUpper(strings.SplitN(line, " ", 2)[0])
		sess.commands = append(sess.commands, verb)
		switch verb {
		case "EHLO", "HELO":
			lines := []string{"250-fake"}
			if f.offerTLS && !sess.tls {
				lines = append(lines, "250-STARTTLS")
			}
			if sess.tls {
				lines = append(lines, "250-AUTH PLAIN")
			}
			lines = append(lines, "250 8BITMIME")
			for _, l := range lines {
				w(l)
			}
		case "STARTTLS":
			w("220 go ahead")
			tc := tls.Server(conn, f.tlsConfig)
			if err := tc.Handshake(); err != nil {
				return
			}
			conn = tc
			r = bufio.NewReader(conn)
			sess.tls = true
		case "AUTH":
			sess.authTried = true
			parts := strings.Fields(line)
			if len(parts) != 3 || !strings.EqualFold(parts[1], "PLAIN") {
				w("504 unsupported")
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(parts[2])
			if err == nil && string(raw) == "\x00"+f.user+"\x00"+f.pass {
				sess.authed = true
				w("235 ok")
			} else {
				w("535 5.7.8 authentication failed")
			}
		case "MAIL":
			sess.from = line
			w("250 ok")
		case "RCPT":
			sess.rcpt = append(sess.rcpt, line)
			w("250 ok")
		case "DATA":
			w("354 end with .")
			var b strings.Builder
			for {
				l, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if l == ".\r\n" {
					break
				}
				b.WriteString(l)
			}
			sess.data = b.String()
			w("250 queued")
		case "QUIT":
			w("221 bye")
			return
		default:
			w("502 unknown")
		}
	}
}

func TestSMTPStartTLSAuthAndMessage(t *testing.T) {
	srvTLS, cliTLS := testTLS(t)
	f := newFakeSMTP(t, &fakeSMTP{tlsConfig: srvTLS, offerTLS: true, user: "u", pass: "p@ss"})
	s, err := NewSMTPSender(SMTPOptions{Addr: f.addr(), Host: "localhost", Username: "u", Password: "p@ss", From: "AeroLLM <alerts@example.com>", TLSConfig: cliTLS, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	msg := Message{AlertID: "cpu", Title: "High CPU ✓\r\nBcc: evil@example.com", Text: "line1\n.\nline3 with a very long text " + strings.Repeat("x", 200), Severity: "critical", Timestamp: time.Unix(0, 0), Labels: map[string]string{"region": "eu"}}
	if err := s.SendEmail(context.Background(), "ops@example.com", msg); err != nil {
		t.Fatalf("send: %v", err)
	}
	sess := f.last()
	if !sess.tls || !sess.authed {
		t.Fatalf("expected TLS + auth, got %+v", sess)
	}
	if sess.from != "MAIL FROM:<alerts@example.com>" && !strings.HasPrefix(sess.from, "MAIL FROM:<alerts@example.com> ") {
		t.Fatalf("envelope from: %q", sess.from)
	}
	if len(sess.rcpt) != 1 || !strings.HasPrefix(sess.rcpt[0], "RCPT TO:<ops@example.com>") {
		t.Fatalf("rcpt: %v", sess.rcpt)
	}
	headers, body, ok := strings.Cut(sess.data, "\r\n\r\n")
	if !ok {
		t.Fatalf("no header/body separator:\n%s", sess.data)
	}
	for _, l := range strings.Split(headers, "\r\n") {
		if strings.HasPrefix(strings.ToLower(l), "bcc:") {
			t.Fatalf("header injection: %q", l)
		}
		if len(l) > 998 {
			t.Fatalf("header line too long")
		}
	}
	if !strings.Contains(headers, "Subject: =?utf-8?q?") {
		t.Fatalf("subject not RFC 2047 encoded:\n%s", headers)
	}
	for _, want := range []string{"From: \"AeroLLM\" <alerts@example.com>", "To: <ops@example.com>", "Content-Transfer-Encoding: quoted-printable", "Message-ID: <"} {
		if !strings.Contains(headers, want) {
			t.Fatalf("missing %q in headers:\n%s", want, headers)
		}
	}
	// The lone "." line must have been dot-stuffed on the wire (and
	// un-stuffed lines must never terminate DATA early).
	if !strings.Contains(body, "\r\n..\r\n") {
		t.Fatalf("expected dot-stuffing in body:\n%s", body)
	}
	if !strings.Contains(body, "region: eu") {
		t.Fatalf("labels missing from body:\n%s", body)
	}
}

func TestSMTPRefusesPlaintext(t *testing.T) {
	srvTLS, cliTLS := testTLS(t)
	f := newFakeSMTP(t, &fakeSMTP{tlsConfig: srvTLS, offerTLS: false, user: "u", pass: "p"})
	s, err := NewSMTPSender(SMTPOptions{Addr: f.addr(), Host: "localhost", Username: "u", Password: "p", From: "alerts@example.com", TLSConfig: cliTLS, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	err = s.SendEmail(context.Background(), "ops@example.com", Message{Title: "x"})
	if !errors.Is(err, ErrSMTPTLSRequired) || !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("expected ErrSMTPTLSRequired, got %v", err)
	}
	sess := f.last()
	if sess.authTried || sess.from != "" || sess.data != "" {
		t.Fatalf("credentials or mail sent over plaintext: %+v", sess)
	}
}

func TestSMTPImplicitTLS(t *testing.T) {
	srvTLS, cliTLS := testTLS(t)
	f := newFakeSMTP(t, &fakeSMTP{tlsConfig: srvTLS, implicitTLS: true, user: "u", pass: "p"})
	s, err := NewSMTPSender(SMTPOptions{Addr: f.addr(), Host: "localhost", Username: "u", Password: "p", From: "alerts@example.com", ImplicitTLS: true, TLSConfig: cliTLS, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.SendEmail(context.Background(), "ops@example.com", Message{Title: "x", Text: "y"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	sess := f.last()
	if !sess.tls || !sess.authed || sess.data == "" {
		t.Fatalf("unexpected session: %+v", sess)
	}
	for _, c := range sess.commands {
		if c == "STARTTLS" {
			t.Fatal("STARTTLS must not be used with implicit TLS")
		}
	}
}

func TestSMTPBadCertificateAndAuthFailure(t *testing.T) {
	srvTLS, _ := testTLS(t)
	f := newFakeSMTP(t, &fakeSMTP{tlsConfig: srvTLS, offerTLS: true, user: "u", pass: "right"})
	// Untrusted certificate: handshake fails, nothing is authenticated.
	s, _ := NewSMTPSender(SMTPOptions{Addr: f.addr(), Host: "localhost", Username: "u", Password: "right", From: "alerts@example.com", Timeout: 5 * time.Second})
	if err := s.SendEmail(context.Background(), "ops@example.com", Message{Title: "x"}); err == nil || !strings.Contains(err.Error(), "starttls") {
		t.Fatalf("expected starttls failure, got %v", err)
	}
	_, cliTLS := testTLS(t) // a different CA
	s, _ = NewSMTPSender(SMTPOptions{Addr: f.addr(), Host: "localhost", Username: "u", Password: "right", From: "alerts@example.com", TLSConfig: cliTLS, Timeout: 5 * time.Second})
	if err := s.SendEmail(context.Background(), "ops@example.com", Message{Title: "x"}); err == nil {
		t.Fatal("expected certificate verification failure")
	}

	srvTLS2, cliTLS2 := testTLS(t)
	f2 := newFakeSMTP(t, &fakeSMTP{tlsConfig: srvTLS2, offerTLS: true, user: "u", pass: "right"})
	s, _ = NewSMTPSender(SMTPOptions{Addr: f2.addr(), Host: "localhost", Username: "u", Password: "wrong-secret", From: "alerts@example.com", TLSConfig: cliTLS2, Timeout: 5 * time.Second})
	err := s.SendEmail(context.Background(), "ops@example.com", Message{Title: "x"})
	if err == nil || !strings.Contains(err.Error(), "auth") {
		t.Fatalf("expected auth failure, got %v", err)
	}
	if strings.Contains(err.Error(), "wrong-secret") {
		t.Fatalf("password leaked in error: %v", err)
	}
}

func TestSMTPHonoursContext(t *testing.T) {
	f := newFakeSMTP(t, &fakeSMTP{stall: true})
	s, _ := NewSMTPSender(SMTPOptions{Addr: f.addr(), From: "alerts@example.com", Timeout: 10 * time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := s.SendEmail(ctx, "ops@example.com", Message{Title: "x"})
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected deadline error, got %v", err)
	}
	if time.Since(start) > 3*time.Second {
		t.Fatal("SendEmail ignored context deadline")
	}
}

func TestSMTPValidation(t *testing.T) {
	bad := []SMTPOptions{
		{Addr: "no-port", From: "a@example.com"},
		{Addr: "h:0", From: "a@example.com"},
		{Addr: "h:25", From: "not an address"},
		{Addr: "h:25", From: "a@example.com\r\nBcc: x@example.com"},
		{Addr: "h:25", From: "a@example.com", Username: "u\r\n"},
		{Addr: "h:25", From: "a@example.com", Password: "p"},
	}
	for i, o := range bad {
		if _, err := NewSMTPSender(o); err == nil {
			t.Errorf("case %d: expected error for %+v", i, o)
		}
	}
	s, err := NewSMTPSender(SMTPOptions{Addr: "127.0.0.1:1", From: "a@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	for _, to := range []string{"ops@example.com\r\nBcc: x@example.com", "Ops <ops@example.com>", "", "a@b.com, c@d.com"} {
		var verr *ValidationError
		if err := s.SendEmail(context.Background(), to, Message{}); !errors.As(err, &verr) {
			t.Errorf("recipient %q: expected validation error, got %v", to, err)
		}
	}
}

func TestSMTPOptionsFromEnv(t *testing.T) {
	t.Setenv(EnvSMTPAddr, "")
	t.Setenv(EnvSMTPFrom, "")
	if _, ok := SMTPOptionsFromEnv(); ok {
		t.Fatal("expected ok=false without addr/from")
	}
	t.Setenv(EnvSMTPAddr, "smtp.example.com:465")
	t.Setenv(EnvSMTPFrom, "alerts@example.com")
	t.Setenv(EnvSMTPUser, "u")
	t.Setenv(EnvSMTPPassword, "p")
	o, ok := SMTPOptionsFromEnv()
	if !ok || !o.ImplicitTLS || o.Username != "u" || o.Password != "p" {
		t.Fatalf("unexpected options: ok=%v %+v", ok, o)
	}
	t.Setenv(EnvSMTPAddr, "smtp.example.com:587")
	if o, _ := SMTPOptionsFromEnv(); o.ImplicitTLS {
		t.Fatal("port 587 must default to STARTTLS")
	}
	t.Setenv(EnvSMTPImplicitTLS, "true")
	if o, _ := SMTPOptionsFromEnv(); !o.ImplicitTLS {
		t.Fatal("explicit implicit TLS ignored")
	}
}

func TestSendersFromEnv(t *testing.T) {
	for _, k := range []string{EnvSMTPAddr, EnvSMTPFrom, EnvSMTPUser, EnvSMTPPassword, EnvTwilioAccountSID, EnvTwilioAuthToken, EnvTwilioFrom} {
		t.Setenv(k, "")
	}
	email, sms, err := SendersFromEnv()
	if err != nil || email != nil || sms != nil {
		t.Fatalf("unconfigured: %v %v %v", email, sms, err)
	}
	t.Setenv(EnvSMTPAddr, "smtp.example.com:587")
	if _, _, err := SendersFromEnv(); err == nil {
		t.Fatal("expected error for incomplete SMTP config")
	}
	t.Setenv(EnvSMTPFrom, "alerts@example.com")
	t.Setenv(EnvTwilioAccountSID, "AC"+strings.Repeat("a", 32))
	t.Setenv(EnvTwilioAuthToken, "tok")
	t.Setenv(EnvTwilioFrom, "+14155550100")
	email, sms, err = SendersFromEnv()
	if err != nil || email == nil || sms == nil {
		t.Fatalf("configured: %v %v %v", email, sms, err)
	}
	t.Setenv(EnvTwilioAccountSID, "bogus")
	if _, _, err := SendersFromEnv(); err == nil {
		t.Fatal("expected error for invalid Twilio SID")
	}
}

func TestDispatcherUsesSMTPSender(t *testing.T) {
	srvTLS, cliTLS := testTLS(t)
	f := newFakeSMTP(t, &fakeSMTP{tlsConfig: srvTLS, offerTLS: true})
	sender, err := NewSMTPSender(SMTPOptions{Addr: f.addr(), Host: "localhost", From: "alerts@example.com", TLSConfig: cliTLS, Timeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	store := NewStore()
	if _, err := store.UpsertChannel(Channel{ID: "mail", Name: "mail", Type: ChannelEmail, Target: "ops@example.com", Enabled: true}); err != nil {
		t.Fatal(err)
	}
	d := NewDispatcher(store, DispatcherOptions{EmailSender: sender})
	if err := d.Send(context.Background(), "mail", Message{Title: "hello"}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if sess := f.last(); !sess.tls || !strings.Contains(sess.data, "hello") {
		t.Fatalf("unexpected session: %+v", sess)
	}
}
