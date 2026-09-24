package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
)

const initConfigTemplate = `# AeroLLM configuration. API keys are read from the environment:
# export OPENAI_API_KEY / ANTHROPIC_API_KEY before starting the gateway.
server:
  port: 8080
  read_timeout: 15s
  write_timeout: 15s

redis:
  addr: localhost:6379
  db: 0

router:
  strategy: cost

providers:
  - name: openai
    type: openai
    api_key: ${OPENAI_API_KEY}
  - name: anthropic
    type: anthropic
    api_key: ${ANTHROPIC_API_KEY}

finops:
  enabled: true
  default_max_usd: 100

mesh:
  enabled: false
  bind_address: /ip4/0.0.0.0/tcp/0
  gossip_interval: 5s

studio:
  enabled: true

economy:
  enabled: true
  currency: USD
`

const initComposeTemplate = `services:
  redis:
    image: redis:7-alpine
    ports:
      - "127.0.0.1:6379:6379"
    volumes:
      - redis_data:/data

  aerollm:
    build: .
    ports:
      - "8080:8080"
    environment:
      - AEROLLM_REDIS_ADDR=redis:6379
      - AEROLLM_API_KEY=${AEROLLM_API_KEY}
      - AEROLLM_LICENSE_KEY=${AEROLLM_LICENSE_KEY}
      - OPENAI_API_KEY=${OPENAI_API_KEY}
      - ANTHROPIC_API_KEY=${ANTHROPIC_API_KEY}
    depends_on:
      - redis
    volumes:
      - ./config.yaml:/app/config.yaml:ro

volumes:
  redis_data:
`

func newInitCmd() *cobra.Command {
	var target, pluginKind string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Generate starter config.yaml, docker-compose.yml and a WASM plugin module",
		Long: `Scaffold a new AeroLLM project in --dir: config.yaml (mode 0600),
docker-compose.yml and plugin/, a standalone WASM plugin module (see
"aerollm plugin init"). Existing files are never overwritten unless --force
is given.`,
		Example: "  aerollm init --dir myproject\n  aerollm init --dir myproject --plugin-kind tool",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if target == "" {
				target = "."
			}
			pluginFiles, kind, err := pluginScaffold(pluginKind, "", "plugin")
			if err != nil {
				return fmt.Errorf("--plugin-kind: %w", err)
			}
			files := append([]scaffoldFile{
				{"config.yaml", initConfigTemplate, 0o600},
				{"docker-compose.yml", initComposeTemplate, 0o644},
			}, pluginFiles...)
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("creating %s: %w", target, err)
			}
			if err := writeScaffold(target, files, force); err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "created config.yaml, docker-compose.yml, and plugin/ (%s plugin: go.mod, main.go)\n", kind)
			_, err = fmt.Fprintf(w, "build the plugin with: aerollm plugin build %s -o plugin.wasm\n", filepath.Join(target, "plugin"))
			return err
		},
	}
	cmd.Flags().StringVarP(&target, "dir", "d", ".", "target directory")
	cmd.Flags().StringVar(&pluginKind, "plugin-kind", "hook", "plugin template: hook|tool")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}
