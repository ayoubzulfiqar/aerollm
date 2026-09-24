package main

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/spf13/cobra"
)

func newPluginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Build WASM plugins and publish signed plugin manifests",
	}

	cmd.AddCommand(newPluginBuildCmd())
	cmd.AddCommand(newPluginPublishCmd())
	return cmd
}

func newPluginBuildCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "build SOURCE",
		Short:   "Build a Go source file or package into a WASM (wasip1) plugin",
		Example: "  aerollm plugin build ./plugin.go -o weather.wasm",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			out, _ := cmd.Flags().GetString("output")
			if out == "" {
				out = "plugin.wasm"
			}
			src := args[0]
			// Reject values that the go tool would parse as flags
			// (e.g. -toolexec=...).
			if strings.HasPrefix(src, "-") || strings.HasPrefix(out, "-") {
				return errors.New("source and output paths must not start with '-'")
			}
			if _, err := os.Stat(src); err != nil {
				return fmt.Errorf("build failed: %w", err)
			}
			buildCmd := exec.CommandContext(cmd.Context(), "go", "build", "-o", out, src)
			buildCmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
			buildCmd.Stdout = cmd.ErrOrStderr()
			buildCmd.Stderr = cmd.ErrOrStderr()
			if err := buildCmd.Run(); err != nil {
				return fmt.Errorf("build failed: %w", err)
			}
			_, err := fmt.Fprintf(cmd.OutOrStdout(), "built %s -> %s\n", src, out)
			return err
		},
	}
	cmd.Flags().StringP("output", "o", "plugin.wasm", "output wasm path")
	return cmd
}

func newPluginPublishCmd() *cobra.Command {
	var id, name, version, creator, registryURL, registryToken string
	cmd := &cobra.Command{
		Use:   "publish WASM",
		Short: "Sign a WASM plugin and publish its manifest to a marketplace registry",
		Long: `Compute the plugin's SHA-256, sign the canonical manifest (id, name,
version, creator, wasm hash, public key) with your Ed25519 private key and
print it (public key + signature only; the private key is never printed or
sent). With --registry-url (or
$AEROLLM_MARKETPLACE_URL) the manifest is POSTed to <registry>/v1/marketplace/plugins
using --registry-token / $AEROLLM_MARKETPLACE_TOKEN.

The private key (--private-key or $AEROLLM_PLUGIN_PRIVATE_KEY) may be a path
to, or the contents of, a PKCS#8 PEM key, or a hex/base64 32-byte seed or
64-byte private key.`,
		Example: "  aerollm plugin publish weather.wasm --private-key ~/.aerollm/plugin.pem --version 1.2.0 --creator acme",
		Args:    cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			keySpec, _ := cmd.Flags().GetString("private-key")
			if keySpec == "" {
				keySpec = os.Getenv("AEROLLM_PLUGIN_PRIVATE_KEY")
			}
			if keySpec == "" {
				return errors.New("missing private key; use --private-key or AEROLLM_PLUGIN_PRIVATE_KEY")
			}
			priv, err := loadEd25519PrivateKey(keySpec)
			if err != nil {
				return fmt.Errorf("loading private key: %w", err)
			}
			wasmPath := args[0]
			wasm, err := os.ReadFile(wasmPath)
			if err != nil {
				return fmt.Errorf("reading plugin: %w", err)
			}
			if len(wasm) < 4 || string(wasm[:4]) != "\x00asm" {
				return fmt.Errorf("%s is not a WebAssembly module", wasmPath)
			}
			if id == "" {
				id = strings.TrimSuffix(filepath.Base(wasmPath), filepath.Ext(wasmPath))
			}
			if name == "" {
				name = id
			}
			if creator == "" {
				creator = os.Getenv("AEROLLM_PLUGIN_CREATOR")
			}
			if creator == "" {
				return errors.New("--creator (or AEROLLM_PLUGIN_CREATOR) is required")
			}
			manifest, err := marketplace.SignManifest(marketplace.PublishRequest{
				ID:        id,
				Name:      name,
				Version:   version,
				CreatorID: creator,
				WASMHash:  marketplace.HashWASM(wasm),
			}, priv)
			if err != nil {
				return fmt.Errorf("signing manifest: %w", err)
			}
			b, err := json.Marshal(manifest)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "prepared manifest: %s\n", b)

			if registryURL == "" {
				registryURL = os.Getenv("AEROLLM_MARKETPLACE_URL")
			}
			if registryURL == "" {
				return nil
			}
			if registryToken == "" {
				registryToken = os.Getenv("AEROLLM_MARKETPLACE_TOKEN")
			}
			client, err := newAPIClient(registryURL, registryToken, resolveTimeout(cmd), cmd.ErrOrStderr())
			if err != nil {
				return fmt.Errorf("registry: %w", err)
			}
			data, err := client.call(cmd.Context(), http.MethodPost, "/v1/marketplace/plugins", nil, manifest)
			if err != nil {
				return fmt.Errorf("registry publish failed: %w", err)
			}
			fmt.Fprintln(w, "published to registry")
			return renderResult(cmd, data, formatJSON, nil, nil)
		},
	}
	cmd.Flags().StringP("private-key", "k", "", "Ed25519 private key (path or key material)")
	cmd.Flags().StringVar(&id, "id", "", "plugin id (default: wasm file name)")
	cmd.Flags().StringVar(&name, "name", "", "plugin display name (default: id)")
	cmd.Flags().StringVar(&version, "version", "0.1.0", "plugin version")
	cmd.Flags().StringVar(&creator, "creator", "", "creator id (env AEROLLM_PLUGIN_CREATOR)")
	cmd.Flags().StringVarP(&registryURL, "registry-url", "r", "", "marketplace registry URL (env AEROLLM_MARKETPLACE_URL)")
	cmd.Flags().StringVar(&registryToken, "registry-token", "", "registry bearer token (env AEROLLM_MARKETPLACE_TOKEN)")
	return cmd
}

// loadEd25519PrivateKey accepts a file path or inline key material: a PEM
// PKCS#8 key, or hex/base64 of a 32-byte seed or 64-byte private key.
func loadEd25519PrivateKey(spec string) (ed25519.PrivateKey, error) {
	material := strings.TrimSpace(spec)
	if fi, err := os.Stat(material); err == nil && fi.Mode().IsRegular() {
		b, err := os.ReadFile(material)
		if err != nil {
			return nil, err
		}
		material = strings.TrimSpace(string(b))
	}
	if block, _ := pem.Decode([]byte(material)); block != nil {
		key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("parsing PEM key: %w", err)
		}
		priv, ok := key.(ed25519.PrivateKey)
		if !ok {
			return nil, fmt.Errorf("PEM key is %T, not Ed25519", key)
		}
		return priv, nil
	}
	raw, err := decodeKeyBytes(material)
	if err != nil {
		return nil, errors.New("key is not a readable file, PEM, hex or base64")
	}
	switch len(raw) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(raw), nil
	case ed25519.PrivateKeySize:
		priv := ed25519.NewKeyFromSeed(raw[:ed25519.SeedSize])
		if !priv.Public().(ed25519.PublicKey).Equal(ed25519.PublicKey(raw[ed25519.SeedSize:])) {
			return nil, errors.New("inconsistent 64-byte Ed25519 key (public half does not match seed)")
		}
		return priv, nil
	default:
		return nil, fmt.Errorf("Ed25519 key must be %d or %d bytes, got %d", ed25519.SeedSize, ed25519.PrivateKeySize, len(raw))
	}
}
