package universal

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"sort"
	"strings"
	"time"
)

// awsCredentials are static AWS credentials for SigV4 signing.
type awsCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

const (
	sigV4Algorithm  = "AWS4-HMAC-SHA256"
	amzDateFormat   = "20060102T150405Z"
	amzShortDateFmt = "20060102"
)

// awsURIEncode percent-encodes s per AWS SigV4 rules: every byte except the
// RFC 3986 unreserved characters is encoded (uppercase hex).
func awsURIEncode(s string) string {
	const hexDigits = "0123456789ABCDEF"
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		if ('A' <= c && c <= 'Z') || ('a' <= c && c <= 'z') || ('0' <= c && c <= '9') || c == '-' || c == '_' || c == '.' || c == '~' {
			b.WriteByte(c)
			continue
		}
		b.WriteByte('%')
		b.WriteByte(hexDigits[c>>4])
		b.WriteByte(hexDigits[c&0x0F])
	}
	return b.String()
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func hmacSHA256(key []byte, data string) []byte {
	m := hmac.New(sha256.New, key)
	m.Write([]byte(data))
	return m.Sum(nil)
}

// canonicalURI builds the SigV4 canonical URI for non-S3 services: each
// segment of the (already escaped) request path is URI-encoded again.
func canonicalURI(escapedPath string) string {
	if escapedPath == "" || escapedPath == "/" {
		return "/"
	}
	segs := strings.Split(escapedPath, "/")
	for i, s := range segs {
		segs[i] = awsURIEncode(s)
	}
	return strings.Join(segs, "/")
}

func canonicalQuery(r *http.Request) string {
	q := r.URL.Query()
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		vs := append([]string(nil), q[k]...)
		sort.Strings(vs)
		for _, v := range vs {
			parts = append(parts, awsURIEncode(k)+"="+awsURIEncode(v))
		}
	}
	return strings.Join(parts, "&")
}

func canonicalHeaderValue(v string) string {
	return strings.Join(strings.Fields(v), " ")
}

// signSigV4 signs r (whose body is payload) in place with AWS Signature
// Version 4, adding X-Amz-Date, X-Amz-Security-Token (when a session token is
// present) and Authorization headers. It signs host, x-amz-date and any of
// content-type, x-amz-content-sha256, x-amz-security-token and x-amz-target
// present on the request.
func signSigV4(r *http.Request, payload []byte, creds awsCredentials, region, service string, now time.Time) {
	now = now.UTC()
	amzDate := now.Format(amzDateFormat)
	shortDate := now.Format(amzShortDateFmt)
	r.Header.Set("X-Amz-Date", amzDate)
	if creds.SessionToken != "" {
		r.Header.Set("X-Amz-Security-Token", creds.SessionToken)
	}
	host := r.Host
	if host == "" {
		host = r.URL.Host
	}

	headers := map[string]string{"host": host, "x-amz-date": amzDate}
	for _, h := range []string{"Content-Type", "X-Amz-Content-Sha256", "X-Amz-Security-Token", "X-Amz-Target"} {
		if v := r.Header.Get(h); v != "" {
			headers[strings.ToLower(h)] = canonicalHeaderValue(v)
		}
	}
	names := make([]string, 0, len(headers))
	for k := range headers {
		names = append(names, k)
	}
	sort.Strings(names)
	var canonHeaders strings.Builder
	for _, k := range names {
		canonHeaders.WriteString(k)
		canonHeaders.WriteByte(':')
		canonHeaders.WriteString(headers[k])
		canonHeaders.WriteByte('\n')
	}
	signedHeaders := strings.Join(names, ";")

	canonicalRequest := strings.Join([]string{
		r.Method,
		canonicalURI(r.URL.EscapedPath()),
		canonicalQuery(r),
		canonHeaders.String(),
		signedHeaders,
		sha256Hex(payload),
	}, "\n")

	scope := shortDate + "/" + region + "/" + service + "/aws4_request"
	stringToSign := sigV4Algorithm + "\n" + amzDate + "\n" + scope + "\n" + sha256Hex([]byte(canonicalRequest))

	kDate := hmacSHA256([]byte("AWS4"+creds.SecretAccessKey), shortDate)
	kRegion := hmacSHA256(kDate, region)
	kService := hmacSHA256(kRegion, service)
	kSigning := hmacSHA256(kService, "aws4_request")
	signature := hex.EncodeToString(hmacSHA256(kSigning, stringToSign))

	r.Header.Set("Authorization", sigV4Algorithm+" Credential="+creds.AccessKeyID+"/"+scope+
		", SignedHeaders="+signedHeaders+", Signature="+signature)
}
