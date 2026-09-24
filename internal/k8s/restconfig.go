package k8s

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.yaml.in/yaml/v3"
)

// DefaultServiceAccountDir is where Kubernetes mounts the pod's service
// account token, CA bundle and namespace.
const DefaultServiceAccountDir = "/var/run/secrets/kubernetes.io/serviceaccount"

// maxCredentialFileBytes bounds kubeconfig, token and certificate files.
const maxCredentialFileBytes = 4 << 20

// ErrNotInCluster is returned by InClusterConfig when the process is not
// running in a Kubernetes pod (KUBERNETES_SERVICE_HOST/PORT unset).
var ErrNotInCluster = errors.New("k8s: not running in a cluster (KUBERNETES_SERVICE_HOST/KUBERNETES_SERVICE_PORT unset)")

// RESTConfig describes how to reach and authenticate to a Kubernetes API
// server. It is a minimal, stdlib-only subset of client-go's rest.Config.
type RESTConfig struct {
	// Host is the API server base URL, e.g. https://10.0.0.1:443.
	Host string
	// CAData is a PEM bundle used to verify the server. Empty uses the
	// system roots.
	CAData []byte
	// TLSServerName overrides the name used to verify the server cert.
	TLSServerName string
	// Insecure disables server certificate verification. LoadKubeconfig
	// only sets it when explicitly allowed.
	Insecure bool

	// BearerToken is a static token. BearerTokenFile, when set, takes
	// precedence and is re-read periodically so rotated (projected)
	// service-account tokens are picked up.
	BearerToken     string
	BearerTokenFile string

	// ClientCertData/ClientKeyData are PEM client credentials (mTLS).
	ClientCertData []byte
	ClientKeyData  []byte

	// Namespace is the default namespace from the kubeconfig context or the
	// in-cluster service account ("" if unknown).
	Namespace string

	// Timeout bounds non-watch requests (default 30s).
	Timeout time.Duration
}

// String never includes credentials.
func (c *RESTConfig) String() string {
	if c == nil {
		return "<nil>"
	}
	auth := "none"
	switch {
	case c.BearerTokenFile != "":
		auth = "token-file"
	case c.BearerToken != "":
		auth = "token"
	case len(c.ClientCertData) > 0:
		auth = "client-cert"
	}
	return fmt.Sprintf("host=%s auth=%s namespace=%q", c.Host, auth, c.Namespace)
}

// TLSConfig builds the client TLS configuration.
func (c *RESTConfig) TLSConfig() (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: c.TLSServerName}
	if c.Insecure {
		cfg.InsecureSkipVerify = true //nolint:gosec // explicit, opt-in only
	}
	if len(c.CAData) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(c.CAData) {
			return nil, errors.New("k8s: CA data contains no valid PEM certificates")
		}
		cfg.RootCAs = pool
	}
	if len(c.ClientCertData) > 0 || len(c.ClientKeyData) > 0 {
		if len(c.ClientCertData) == 0 || len(c.ClientKeyData) == 0 {
			return nil, errors.New("k8s: client certificate and key must both be set")
		}
		cert, err := tls.X509KeyPair(c.ClientCertData, c.ClientKeyData)
		if err != nil {
			return nil, fmt.Errorf("k8s: client certificate: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func (c *RESTConfig) validate() error {
	if c == nil {
		return errors.New("k8s: nil REST config")
	}
	u, err := url.Parse(c.Host)
	if err != nil || u.Host == "" {
		return fmt.Errorf("k8s: invalid API server host %q", c.Host)
	}
	if u.Scheme != "https" && u.Scheme != "http" {
		return fmt.Errorf("k8s: API server host must be http(s), got %q", u.Scheme)
	}
	if u.User != nil {
		return errors.New("k8s: API server URL must not embed credentials")
	}
	if u.Scheme == "http" && (c.BearerToken != "" || c.BearerTokenFile != "") && !isLoopbackHost(u.Hostname()) {
		return errors.New("k8s: refusing to send a bearer token over plain http to a non-loopback API server")
	}
	return nil
}

func isLoopbackHost(h string) bool {
	if h == "localhost" {
		return true
	}
	ip := net.ParseIP(h)
	return ip != nil && ip.IsLoopback()
}

// InClusterOptions makes InClusterConfigFrom testable.
type InClusterOptions struct {
	// Dir holds token, ca.crt and namespace (default DefaultServiceAccountDir).
	Dir string
	// Getenv looks up KUBERNETES_SERVICE_HOST/PORT (default os.Getenv).
	Getenv func(string) string
}

// InClusterConfig returns the config for the pod's service account.
func InClusterConfig() (*RESTConfig, error) { return InClusterConfigFrom(InClusterOptions{}) }

// InClusterConfigFrom is InClusterConfig with injectable paths and env.
func InClusterConfigFrom(opts InClusterOptions) (*RESTConfig, error) {
	getenv := opts.Getenv
	if getenv == nil {
		getenv = os.Getenv
	}
	dir := opts.Dir
	if dir == "" {
		dir = DefaultServiceAccountDir
	}
	host, port := strings.TrimSpace(getenv("KUBERNETES_SERVICE_HOST")), strings.TrimSpace(getenv("KUBERNETES_SERVICE_PORT"))
	if host == "" || port == "" {
		return nil, ErrNotInCluster
	}
	tokenFile := filepath.Join(dir, "token")
	token, err := readTokenFile(tokenFile)
	if err != nil {
		return nil, fmt.Errorf("k8s: in-cluster token: %w", err)
	}
	if token == "" {
		return nil, fmt.Errorf("k8s: in-cluster token file %s is empty", tokenFile)
	}
	ca, err := readCredentialFile(filepath.Join(dir, "ca.crt"))
	if err != nil {
		return nil, fmt.Errorf("k8s: in-cluster CA: %w", err)
	}
	cfg := &RESTConfig{
		Host:            "https://" + net.JoinHostPort(host, port),
		CAData:          ca,
		BearerTokenFile: tokenFile,
	}
	if ns, err := readCredentialFile(filepath.Join(dir, "namespace")); err == nil {
		cfg.Namespace = strings.TrimSpace(string(ns))
	}
	return cfg, cfg.validate()
}

// KubeconfigOptions controls LoadKubeconfig.
type KubeconfigOptions struct {
	// Context selects a context; empty uses current-context.
	Context string
	// AllowInsecure permits clusters with insecure-skip-tls-verify: true.
	// Without it such kubeconfigs are refused.
	AllowInsecure bool
}

// DefaultKubeconfigPath returns the first entry of $KUBECONFIG, else
// ~/.kube/config ("" if the home directory is unknown).
func DefaultKubeconfigPath() string {
	if env := os.Getenv("KUBECONFIG"); env != "" {
		for _, p := range filepath.SplitList(env) {
			if p != "" {
				return p
			}
		}
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ""
	}
	return filepath.Join(home, ".kube", "config")
}

type kubeconfigFile struct {
	CurrentContext string `yaml:"current-context"`
	Clusters       []struct {
		Name    string `yaml:"name"`
		Cluster struct {
			Server                   string `yaml:"server"`
			CertificateAuthority     string `yaml:"certificate-authority"`
			CertificateAuthorityData string `yaml:"certificate-authority-data"`
			InsecureSkipTLSVerify    bool   `yaml:"insecure-skip-tls-verify"`
			TLSServerName            string `yaml:"tls-server-name"`
			ProxyURL                 string `yaml:"proxy-url"`
		} `yaml:"cluster"`
	} `yaml:"clusters"`
	Contexts []struct {
		Name    string `yaml:"name"`
		Context struct {
			Cluster   string `yaml:"cluster"`
			User      string `yaml:"user"`
			Namespace string `yaml:"namespace"`
		} `yaml:"context"`
	} `yaml:"contexts"`
	Users []struct {
		Name string `yaml:"name"`
		User struct {
			Token                 string    `yaml:"token"`
			TokenFile             string    `yaml:"tokenFile"`
			ClientCertificate     string    `yaml:"client-certificate"`
			ClientCertificateData string    `yaml:"client-certificate-data"`
			ClientKey             string    `yaml:"client-key"`
			ClientKeyData         string    `yaml:"client-key-data"`
			Username              string    `yaml:"username"`
			Exec                  yaml.Node `yaml:"exec"`
			AuthProvider          yaml.Node `yaml:"auth-provider"`
		} `yaml:"user"`
	} `yaml:"users"`
}

// LoadKubeconfig reads a kubeconfig file and resolves one context into a
// RESTConfig. Supported: server, certificate-authority(-data),
// tls-server-name, token, tokenFile, client-certificate(-data) and
// client-key(-data). exec and auth-provider plugins, basic auth and
// proxy-url are rejected with a clear error.
func LoadKubeconfig(path string, opts KubeconfigOptions) (*RESTConfig, error) {
	if path == "" {
		return nil, errors.New("k8s: empty kubeconfig path")
	}
	data, err := readCredentialFile(path)
	if err != nil {
		return nil, fmt.Errorf("k8s: kubeconfig: %w", err)
	}
	return ParseKubeconfig(data, filepath.Dir(path), opts)
}

// ParseKubeconfig is LoadKubeconfig for in-memory data. Relative file
// references are resolved against baseDir.
func ParseKubeconfig(data []byte, baseDir string, opts KubeconfigOptions) (*RESTConfig, error) {
	var kc kubeconfigFile
	if err := yaml.Unmarshal(data, &kc); err != nil {
		return nil, fmt.Errorf("k8s: kubeconfig: invalid YAML: %w", err)
	}
	ctxName := opts.Context
	if ctxName == "" {
		ctxName = kc.CurrentContext
	}
	if ctxName == "" {
		return nil, errors.New("k8s: kubeconfig: no context selected and current-context is empty")
	}
	var clusterName, userName, namespace string
	found := false
	for _, c := range kc.Contexts {
		if c.Name == ctxName {
			clusterName, userName, namespace = c.Context.Cluster, c.Context.User, c.Context.Namespace
			found = true
			break
		}
	}
	if !found {
		return nil, fmt.Errorf("k8s: kubeconfig: context %q not found", ctxName)
	}
	resolve := func(p string) string {
		if p == "" || filepath.IsAbs(p) {
			return p
		}
		return filepath.Join(baseDir, p)
	}

	cfg := &RESTConfig{Namespace: namespace}
	found = false
	for _, c := range kc.Clusters {
		if c.Name != clusterName {
			continue
		}
		found = true
		cl := c.Cluster
		cfg.Host = strings.TrimRight(cl.Server, "/")
		cfg.TLSServerName = cl.TLSServerName
		if cl.ProxyURL != "" {
			return nil, fmt.Errorf("k8s: kubeconfig: cluster %q: proxy-url is not supported (use HTTPS_PROXY)", clusterName)
		}
		if cl.InsecureSkipTLSVerify {
			if !opts.AllowInsecure {
				return nil, fmt.Errorf("k8s: kubeconfig: cluster %q sets insecure-skip-tls-verify; refusing without explicit opt-in", clusterName)
			}
			cfg.Insecure = true
		}
		switch {
		case cl.CertificateAuthorityData != "":
			ca, err := base64.StdEncoding.DecodeString(strings.TrimSpace(cl.CertificateAuthorityData))
			if err != nil {
				return nil, fmt.Errorf("k8s: kubeconfig: cluster %q: invalid certificate-authority-data", clusterName)
			}
			cfg.CAData = ca
		case cl.CertificateAuthority != "":
			ca, err := readCredentialFile(resolve(cl.CertificateAuthority))
			if err != nil {
				return nil, fmt.Errorf("k8s: kubeconfig: cluster %q: certificate-authority: %w", clusterName, err)
			}
			cfg.CAData = ca
		}
		break
	}
	if !found {
		return nil, fmt.Errorf("k8s: kubeconfig: cluster %q not found", clusterName)
	}
	if cfg.Host == "" {
		return nil, fmt.Errorf("k8s: kubeconfig: cluster %q has no server", clusterName)
	}

	if userName != "" {
		found = false
		for _, u := range kc.Users {
			if u.Name != userName {
				continue
			}
			found = true
			us := u.User
			if !us.Exec.IsZero() {
				return nil, fmt.Errorf("k8s: kubeconfig: user %q uses an exec credential plugin, which is not supported (use a token or client certificate)", userName)
			}
			if !us.AuthProvider.IsZero() {
				return nil, fmt.Errorf("k8s: kubeconfig: user %q uses an auth-provider plugin, which is not supported", userName)
			}
			if us.Username != "" {
				return nil, fmt.Errorf("k8s: kubeconfig: user %q uses basic auth, which is not supported", userName)
			}
			cfg.BearerToken = strings.TrimSpace(us.Token)
			cfg.BearerTokenFile = resolve(us.TokenFile)
			var err error
			if cfg.ClientCertData, err = dataOrFile(us.ClientCertificateData, resolve(us.ClientCertificate)); err != nil {
				return nil, fmt.Errorf("k8s: kubeconfig: user %q: client certificate: %w", userName, err)
			}
			if cfg.ClientKeyData, err = dataOrFile(us.ClientKeyData, resolve(us.ClientKey)); err != nil {
				return nil, fmt.Errorf("k8s: kubeconfig: user %q: client key: %w", userName, err)
			}
			break
		}
		if !found {
			return nil, fmt.Errorf("k8s: kubeconfig: user %q not found", userName)
		}
	}
	if cfg.BearerTokenFile != "" {
		if _, err := readTokenFile(cfg.BearerTokenFile); err != nil {
			return nil, fmt.Errorf("k8s: kubeconfig: tokenFile: %w", err)
		}
	}
	if _, err := cfg.TLSConfig(); err != nil {
		return nil, err
	}
	return cfg, cfg.validate()
}

func dataOrFile(b64, path string) ([]byte, error) {
	if b64 != "" {
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
		if err != nil {
			return nil, errors.New("invalid base64 data")
		}
		return b, nil
	}
	if path != "" {
		return readCredentialFile(path)
	}
	return nil, nil
}

func readCredentialFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var buf bytes.Buffer
	n, err := buf.ReadFrom(io.LimitReader(f, maxCredentialFileBytes+1))
	if err != nil {
		return nil, err
	}
	if n > maxCredentialFileBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, maxCredentialFileBytes)
	}
	return buf.Bytes(), nil
}

func readTokenFile(path string) (string, error) {
	b, err := readCredentialFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}
