package notification

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"regexp"
	"strings"
	"syscall"
)

// ErrBlockedDestination is returned when a notification target resolves to
// (or literally names) an address that is not reachable from the public
// internet: loopback, RFC 1918 / ULA private ranges, link-local, CGNAT,
// multicast, cloud metadata endpoints and similar. Such targets are refused
// unless private networks were explicitly allowed.
var ErrBlockedDestination = errors.New("notification: destination address is not allowed")

// blockedPrefixes lists special-purpose ranges not covered by the netip
// Is* helpers.
var blockedPrefixes = func() []netip.Prefix {
	cidrs := []string{
		"0.0.0.0/8",          // "this" network
		"100.64.0.0/10",      // carrier-grade NAT
		"192.0.0.0/24",       // IETF protocol assignments
		"192.0.2.0/24",       // TEST-NET-1
		"198.18.0.0/15",      // benchmarking
		"198.51.100.0/24",    // TEST-NET-2
		"203.0.113.0/24",     // TEST-NET-3
		"240.0.0.0/4",        // reserved + broadcast
		"64:ff9b::/96",       // NAT64 (may embed internal IPv4)
		"64:ff9b:1::/48",     // local-use NAT64
		"100::/64",           // discard-only
		"2001::/32",          // Teredo (embeds arbitrary IPv4)
		"2001:db8::/32",      // documentation
		"fd00:ec2::254/128",  // AWS IMDS over IPv6 (also inside fc00::/7)
		"169.254.169.254/32", // cloud metadata (also link-local)
	}
	out := make([]netip.Prefix, 0, len(cidrs))
	for _, c := range cidrs {
		out = append(out, netip.MustParsePrefix(c))
	}
	return out
}()

// blockedHostnames are names that always point at the local host or a cloud
// metadata service.
var blockedHostnames = map[string]bool{
	"localhost":                  true,
	"metadata":                   true,
	"metadata.google.internal":   true,
	"metadata.goog":              true,
	"instance-data":              true,
	"instance-data.ec2.internal": true,
}

// numericHostRe matches hosts that some resolvers interpret as IPv4 addresses
// in non-canonical notation (decimal "2130706433", octal "0177.0.0.1", hex
// "0x7f.1"). They are rejected outright because their meaning is resolver
// dependent.
var numericHostRe = regexp.MustCompile(`^(0x[0-9a-f]+|[0-9]+)(\.(0x[0-9a-f]+|[0-9]+)){0,3}\.?$`)

// IsBlockedIP reports whether addr belongs to a range that outbound
// notifications must never reach unless private networks are allowed.
func IsBlockedIP(addr netip.Addr) bool {
	if !addr.IsValid() {
		return true
	}
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsPrivate() || addr.IsUnspecified() ||
		addr.IsLinkLocalUnicast() || addr.IsLinkLocalMulticast() ||
		addr.IsInterfaceLocalMulticast() || addr.IsMulticast() {
		return true
	}
	for _, p := range blockedPrefixes {
		if p.Contains(addr) {
			return true
		}
	}
	// 6to4 (2002::/16) embeds an IPv4 address in bytes 2..5.
	if addr.Is6() {
		b := addr.As16()
		if b[0] == 0x20 && b[1] == 0x02 {
			v4 := netip.AddrFrom4([4]byte{b[2], b[3], b[4], b[5]})
			if IsBlockedIP(v4) {
				return true
			}
		}
	}
	return false
}

// checkHost validates the host part of a URL without resolving it. DNS based
// attacks (rebinding, names resolving to private space) are handled at dial
// time by ssrfControl.
func checkHost(host string, allowPrivate bool) error {
	h := strings.ToLower(strings.TrimSuffix(host, "."))
	if h == "" {
		return errors.New("target host is required")
	}
	if allowPrivate {
		return nil
	}
	if addr, err := netip.ParseAddr(strings.Trim(h, "[]")); err == nil {
		if IsBlockedIP(addr) {
			return fmt.Errorf("target host %s: %w", h, ErrBlockedDestination)
		}
		return nil
	}
	if blockedHostnames[h] || strings.HasSuffix(h, ".localhost") {
		return fmt.Errorf("target host %s: %w", h, ErrBlockedDestination)
	}
	if numericHostRe.MatchString(h) {
		return fmt.Errorf("target host %s: ambiguous numeric host: %w", h, ErrBlockedDestination)
	}
	return nil
}

// validateHTTPTarget validates a webhook/slack destination URL.
func validateHTTPTarget(raw string, allowInsecure, allowPrivate bool) error {
	if raw == "" {
		return errors.New("target is required")
	}
	if len(raw) > maxTargetLen {
		return fmt.Errorf("target exceeds %d characters", maxTargetLen)
	}
	if strings.ContainsAny(raw, " \t\r\n") {
		return errors.New("target must not contain whitespace")
	}
	u, err := url.Parse(raw)
	if err != nil {
		return errors.New("target is not a valid URL")
	}
	switch u.Scheme {
	case "https":
	case "http":
		if !allowInsecure {
			return errors.New("target must use https")
		}
	default:
		return errors.New("target must be an absolute https URL")
	}
	if u.User != nil {
		return errors.New("target must not embed credentials (userinfo)")
	}
	if u.Opaque != "" {
		return errors.New("target is not a valid URL")
	}
	if port := u.Port(); port != "" {
		if n, err := parsePort(port); err != nil || n == 0 {
			return errors.New("target port is invalid")
		}
	}
	return checkHost(u.Hostname(), allowPrivate)
}

func parsePort(p string) (int, error) {
	n := 0
	for _, c := range p {
		if c < '0' || c > '9' {
			return 0, errors.New("invalid port")
		}
		n = n*10 + int(c-'0')
		if n > 65535 {
			return 0, errors.New("invalid port")
		}
	}
	return n, nil
}

// ssrfControl returns a net.Dialer Control hook that refuses connections to
// blocked addresses. It runs after DNS resolution, for every address the
// dialer tries, which defeats DNS rebinding and names that resolve into
// private space.
func ssrfControl(allowPrivate bool) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, _ syscall.RawConn) error {
		if allowPrivate {
			return nil
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return ErrBlockedDestination
		}
		addr, err := netip.ParseAddr(host)
		if err != nil {
			return ErrBlockedDestination
		}
		if IsBlockedIP(addr) {
			return ErrBlockedDestination
		}
		return nil
	}
}
