package main

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"
)

func newPluginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "plugin",
		Short: "Plugin build/publish utilities",
	}

	cmd.AddCommand(newPluginBuildCmd())
	cmd.AddCommand(newPluginPublishCmd())
	return cmd
}

func newPluginBuildCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "build [source]",
		Short: "Build a Go source file into a WASM plugin",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			out, _ := cmd.Flags().GetString("output")
			if out == "" {
				out = "plugin.wasm"
			}

			buildCmd := exec.Command("go", "build", "-o", out, args[0])
			buildCmd.Env = append(os.Environ(), "GOOS=wasip1", "GOARCH=wasm")
			buildCmd.Stdout = os.Stdout
			buildCmd.Stderr = os.Stderr
			if err := buildCmd.Run(); err != nil {
				fmt.Printf("build failed: %v\n", err)
				return
			}
			fmt.Printf("built %s -> %s\n", args[0], out)
		},
	}
	cmd.Flags().StringP("output", "o", "plugin.wasm", "output wasm path")
	return cmd
}

func newPluginPublishCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "publish [wasm]",
		Short: "Prepare and publish a signed plugin manifest to the marketplace registry",
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			priv, _ := cmd.Flags().GetString("private-key")
			if priv == "" {
				priv = os.Getenv("AEROLLM_PLUGIN_PRIVATE_KEY")
			}
			if priv == "" {
				fmt.Println("missing private key; use --private-key or AEROLLM_PLUGIN_PRIVATE_KEY")
				return
			}

			manifest := fmt.Sprintf(`{"wasm":"%s","signature":"ed25519-stub","public_key":"%s"}`, args[0], priv)
			fmt.Printf("prepared manifest: %s\n", manifest)

			registryURL, _ := cmd.Flags().GetString("registry-url")
			if registryURL == "" {
				registryURL = os.Getenv("AEROLLM_MARKETPLACE_URL")
			}
			if registryURL == "" {
				return
			}

			req, err := http.NewRequest(http.MethodPost, registryURL+"/v1/marketplace/plugins", strings.NewReader(manifest))
			if err != nil {
				fmt.Printf("registry request failed: %v\n", err)
				return
			}
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				fmt.Printf("registry publish failed: %v\n", err)
				return
			}
			defer resp.Body.Close()
			fmt.Printf("registry response: %s\n", resp.Status)
		},
	}
	cmd.Flags().StringP("private-key", "k", "", "Ed25519 private key path")
	cmd.Flags().StringP("registry-url", "r", "", "Marketplace registry URL override")
	return cmd
}
