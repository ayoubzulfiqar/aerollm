package autoscale

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"sync/atomic"
)

// Environment variables the gateway reads (via BootstrapConfigFromEnv) to
// configure node bootstrap. All three are required; without them bootstrap
// generation is refused.
const (
	// EnvBootstrapURL is the https URL of the pinned Edge Companion
	// installer artifact.
	EnvBootstrapURL = "AEROLLM_AUTOSCALE_BOOTSTRAP_URL"
	// EnvBootstrapSHA256 is the hex SHA-256 of that artifact.
	EnvBootstrapSHA256 = "AEROLLM_AUTOSCALE_BOOTSTRAP_SHA256"
	// EnvBootstrapVersion is the pinned Edge Companion version.
	EnvBootstrapVersion = "AEROLLM_AUTOSCALE_BOOTSTRAP_VERSION"
)

// Bootstrap input limits.
const (
	// MaxBootstrapURLLength bounds BootstrapConfig.ArtifactURL.
	MaxBootstrapURLLength = 2048
	// MaxMeshPeers bounds the number of mesh peers in a bootstrap script.
	MaxMeshPeers = 64
)

var (
	// ErrBootstrapNotConfigured is returned when no pinned artifact URL,
	// SHA-256 and version are configured. Bootstrap generation fails closed:
	// there is no unpinned fallback.
	ErrBootstrapNotConfigured = errors.New("autoscale: node bootstrap not configured (pinned artifact URL, SHA-256 and version are required)")
	// ErrInvalidBootstrapConfig is returned for a malformed BootstrapConfig.
	ErrInvalidBootstrapConfig = errors.New("autoscale: invalid bootstrap config")
)

var (
	// sha256Pattern accepts exactly 64 lowercase hex characters.
	sha256Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
	// versionPattern accepts semver-like versions (e.g. v1.2.3, 1.4.0-rc.1+b7).
	versionPattern = regexp.MustCompile(`^[vV]?[0-9A-Za-z][0-9A-Za-z._+-]{0,63}$`)
	// artifactURLPattern is a strict allowlist for the artifact URL: an
	// https scheme, a DNS/IPv4 host, an optional port, an unreserved-char
	// path and an optional query. It excludes quotes, whitespace, `$`, `` ` ``,
	// `\`, `;`, `|`, `(`, `)`, `<`, `>`, `#` and control characters.
	artifactURLPattern = regexp.MustCompile(`^https://[A-Za-z0-9](?:[A-Za-z0-9.-]{0,251}[A-Za-z0-9])?(?::[0-9]{1,5})?(?:/[A-Za-z0-9._~%+@:/-]*)?(?:\?[A-Za-z0-9._~%+=&-]*)?$`)
	// nodeIDPattern is the allowlist for node IDs.
	nodeIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`)
	// meshPeerPattern is the allowlist for a mesh peer (host[:port], [v6]:port
	// or a URL without query/fragment).
	meshPeerPattern = regexp.MustCompile(`^[A-Za-z0-9._:/@\[\]-]{1,253}$`)
)

// BootstrapConfig pins the Edge Companion installer that a provisioned node
// downloads. The generated script downloads ArtifactURL over https, verifies
// its SHA-256 against SHA256 and only then executes it, so a compromised or
// mutable download location cannot run arbitrary code on the node.
type BootstrapConfig struct {
	// ArtifactURL is the https URL of the version-pinned installer script,
	// e.g. https://releases.example.com/aerollm-edge/v1.4.0/install.sh.
	ArtifactURL string `json:"artifact_url"`
	// SHA256 is the lowercase hex SHA-256 of the artifact.
	SHA256 string `json:"sha256"`
	// Version is the pinned Edge Companion version, exported to the
	// installer as AEROLLM_EDGE_VERSION.
	Version string `json:"version"`
}

// IsZero reports whether no field is set.
func (c BootstrapConfig) IsZero() bool {
	return c.ArtifactURL == "" && c.SHA256 == "" && c.Version == ""
}

// Validate checks every field against a strict allowlist. It returns
// ErrBootstrapNotConfigured when a field is missing and
// ErrInvalidBootstrapConfig when a field is malformed. Error messages never
// echo the rejected value.
func (c BootstrapConfig) Validate() error {
	if c.ArtifactURL == "" || c.SHA256 == "" || c.Version == "" {
		return ErrBootstrapNotConfigured
	}
	if err := validateArtifactURL(c.ArtifactURL); err != nil {
		return err
	}
	if !sha256Pattern.MatchString(c.SHA256) {
		return fmt.Errorf("%w: sha256 must be exactly 64 lowercase hex characters", ErrInvalidBootstrapConfig)
	}
	if !versionPattern.MatchString(c.Version) {
		return fmt.Errorf("%w: version must be 1-64 chars of [0-9A-Za-z._+-] (optional leading v)", ErrInvalidBootstrapConfig)
	}
	return nil
}

func validateArtifactURL(raw string) error {
	if len(raw) > MaxBootstrapURLLength {
		return fmt.Errorf("%w: artifact url longer than %d bytes", ErrInvalidBootstrapConfig, MaxBootstrapURLLength)
	}
	if !artifactURLPattern.MatchString(raw) {
		return fmt.Errorf("%w: artifact url must be an https URL of [A-Za-z0-9._~%%+@:/?=&-] characters", ErrInvalidBootstrapConfig)
	}
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%w: artifact url does not parse", ErrInvalidBootstrapConfig)
	}
	if u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("%w: artifact url must be https with a host and no credentials or fragment", ErrInvalidBootstrapConfig)
	}
	return nil
}

// BootstrapConfigFromEnv builds a BootstrapConfig from EnvBootstrapURL,
// EnvBootstrapSHA256 and EnvBootstrapVersion using getenv (os.Getenv in
// production). Values are trimmed and the SHA-256 is lowercased. When none
// of the variables is set it returns ErrBootstrapNotConfigured; a partial or
// malformed configuration is reported by Validate.
func BootstrapConfigFromEnv(getenv func(string) string) (BootstrapConfig, error) {
	if getenv == nil {
		return BootstrapConfig{}, ErrBootstrapNotConfigured
	}
	cfg := BootstrapConfig{
		ArtifactURL: strings.TrimSpace(getenv(EnvBootstrapURL)),
		SHA256:      strings.ToLower(strings.TrimSpace(getenv(EnvBootstrapSHA256))),
		Version:     strings.TrimSpace(getenv(EnvBootstrapVersion)),
	}
	if err := cfg.Validate(); err != nil {
		return BootstrapConfig{}, err
	}
	return cfg, nil
}

var defaultBootstrap atomic.Pointer[BootstrapConfig]

// SetDefaultBootstrapConfig validates cfg and installs it as the
// process-wide configuration used by BootstrapScript and BootstrapScriptE.
// Passing the zero BootstrapConfig clears it (generation is then refused).
func SetDefaultBootstrapConfig(cfg BootstrapConfig) error {
	if cfg.IsZero() {
		defaultBootstrap.Store(nil)
		return nil
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	c := cfg
	defaultBootstrap.Store(&c)
	return nil
}

// DefaultBootstrapConfig returns the process-wide bootstrap configuration and
// whether one is installed.
func DefaultBootstrapConfig() (BootstrapConfig, bool) {
	c := defaultBootstrap.Load()
	if c == nil {
		return BootstrapConfig{}, false
	}
	return *c, true
}

// shellQuote returns s as a single-quoted POSIX shell word. Callers must
// validate s against an allowlist first; quoting is defence in depth.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

func validateBootstrapInputs(meshPeers []string, nodeID string) error {
	if !nodeIDPattern.MatchString(nodeID) {
		return fmt.Errorf("%w: node id must be 1-128 chars of [A-Za-z0-9._-]", ErrInvalidSpec)
	}
	if len(meshPeers) > MaxMeshPeers {
		return fmt.Errorf("%w: at most %d mesh peers", ErrInvalidSpec, MaxMeshPeers)
	}
	for i, p := range meshPeers {
		if !meshPeerPattern.MatchString(p) {
			return fmt.Errorf("%w: mesh peer %d must be 1-253 chars of [A-Za-z0-9._:/@[]-]", ErrInvalidSpec, i)
		}
	}
	return nil
}

// GenerateBootstrapScript returns a cloud-init bash script that installs the
// pinned AeroLLM Edge Companion on a new node. The script:
//
//   - runs with `set -euo pipefail` and a private umask,
//   - downloads cfg.ArtifactURL (https only, including redirects) into a
//     mktemp directory that is removed on exit,
//   - verifies the download against cfg.SHA256 with sha256sum or shasum and
//     aborts on mismatch or when no checksum tool exists,
//   - only then executes the verified installer (never `curl | bash`) and
//     enables the aerollm-edge service.
//
// Every interpolated value is validated against a strict allowlist and
// single-quoted. It returns ErrBootstrapNotConfigured when cfg is incomplete,
// ErrInvalidBootstrapConfig when cfg is malformed and ErrInvalidSpec for a bad
// node ID or mesh peer.
func GenerateBootstrapScript(cfg BootstrapConfig, meshPeers []string, nodeID string) (string, error) {
	if err := cfg.Validate(); err != nil {
		return "", err
	}
	if err := validateBootstrapInputs(meshPeers, nodeID); err != nil {
		return "", err
	}
	return fmt.Sprintf(`#!/bin/bash
# AeroLLM Edge Companion bootstrap (generated by the AeroLLM gateway).
# Downloads a version-pinned installer, verifies its SHA-256 and only then
# executes it. Nothing downloaded is executed before verification.
set -euo pipefail
umask 077

readonly AEROLLM_EDGE_VERSION=%s
readonly AEROLLM_EDGE_URL=%s
readonly AEROLLM_EDGE_SHA256=%s
export AEROLLM_EDGE_VERSION
export AEROLLM_MESH_ENABLED=true
export AEROLLM_MESH_NODE_ID=%s
export AEROLLM_MESH_PEERS=%s

workdir="$(mktemp -d)"
trap 'rm -rf -- "$workdir"' EXIT
artifact="$workdir/aerollm-edge-installer"

curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL --retry 3 --max-time 300 -o "$artifact" -- "$AEROLLM_EDGE_URL"

if command -v sha256sum >/dev/null 2>&1; then
	actual="$(sha256sum -- "$artifact" | awk '{print $1}')"
elif command -v shasum >/dev/null 2>&1; then
	actual="$(shasum -a 256 -- "$artifact" | awk '{print $1}')"
else
	echo "aerollm-bootstrap: sha256sum or shasum is required to verify the installer" >&2
	exit 1
fi
if [ "$actual" != "$AEROLLM_EDGE_SHA256" ]; then
	echo "aerollm-bootstrap: checksum mismatch for AeroLLM Edge $AEROLLM_EDGE_VERSION; refusing to install" >&2
	exit 1
fi

bash "$artifact"
systemctl enable --now aerollm-edge
`,
		shellQuote(cfg.Version),
		shellQuote(cfg.ArtifactURL),
		shellQuote(cfg.SHA256),
		shellQuote(nodeID),
		shellQuote(strings.Join(meshPeers, ",")),
	), nil
}

// refusedBootstrapScript is emitted by the legacy BootstrapScript when
// generation is refused. It exits non-zero without downloading anything and
// never echoes caller-supplied values.
func refusedBootstrapScript(err error) string {
	reason := "invalid bootstrap input"
	if errors.Is(err, ErrBootstrapNotConfigured) {
		reason = "bootstrap not configured (set " + EnvBootstrapURL + ", " + EnvBootstrapSHA256 + " and " + EnvBootstrapVersion + ")"
	}
	return "#!/bin/sh\necho " + shellQuote("aerollm-bootstrap: refused: "+reason) + " >&2\nexit 1\n"
}

// BootstrapScript generates the node bootstrap script using the
// process-wide configuration (SetDefaultBootstrapConfig).
//
// Deprecated: use GenerateBootstrapScript (or BootstrapScriptE), which
// return an error. Because this function cannot report errors, it fails
// closed: when bootstrap is not configured or nodeID/meshPeers are invalid it
// returns a script that exits 1 without downloading or executing anything.
func BootstrapScript(meshPeers []string, nodeID string) string {
	s, err := BootstrapScriptE(meshPeers, nodeID)
	if err != nil {
		return refusedBootstrapScript(err)
	}
	return s
}

// BootstrapScriptE generates the node bootstrap script using the
// process-wide configuration (SetDefaultBootstrapConfig). It returns
// ErrBootstrapNotConfigured when none is installed and ErrInvalidSpec for a
// malformed nodeID ([A-Za-z0-9._-], 1-128 chars) or mesh peer (at most
// MaxMeshPeers, each 1-253 chars of [A-Za-z0-9._:/@[]-]).
func BootstrapScriptE(meshPeers []string, nodeID string) (string, error) {
	cfg, ok := DefaultBootstrapConfig()
	if !ok {
		return "", ErrBootstrapNotConfigured
	}
	return GenerateBootstrapScript(cfg, meshPeers, nodeID)
}
