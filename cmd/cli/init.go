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

const initPluginTemplate = `package main

import (
	"context"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/plugins"
)

// WeatherPlugin is a sample AeroLLM plugin.
type WeatherPlugin struct{}

func (p *WeatherPlugin) ID() string      { return "weather" }
func (p *WeatherPlugin) Name() string    { return "Weather" }
func (p *WeatherPlugin) Enabled() bool   { return true }

func (p *WeatherPlugin) Invoke(ctx context.Context, hook plugins.Hook, payload map[string]interface{}) (map[string]interface{}, error) {
	switch hook {
	case plugins.HookOnToolCall:
		return map[string]interface{}{
			"tool":    "weather",
			"status":  "ok",
			"payload": payload,
		}, nil
	default:
		return payload, nil
	}
}

// Metadata returns plugin metadata for the registry.
func Metadata() plugins.Metadata {
	return plugins.Metadata{
		ID:       "weather",
		Name:     "Weather",
		Version:  "0.1.0",
		Enabled:  true,
		Filename: "plugin.wasm",
	}
}

func main() {
	fmt.Println("weather plugin loaded")
}
`

func newInitCmd() *cobra.Command {
	var target string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Generate starter config.yaml, docker-compose.yml and plugin template",
		Long: `Scaffold a new AeroLLM project in --dir. Existing files are never
overwritten unless --force is given. config.yaml is created with mode 0600.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if target == "" {
				target = "."
			}
			if err := os.MkdirAll(target, 0o755); err != nil {
				return fmt.Errorf("creating %s: %w", target, err)
			}
			files := []struct {
				name    string
				content string
				perm    os.FileMode
			}{
				{"config.yaml", initConfigTemplate, 0o600},
				{"docker-compose.yml", initComposeTemplate, 0o644},
				{"plugin.go", initPluginTemplate, 0o644},
			}
			// Check everything first so a refusal leaves no partial scaffold.
			if !force {
				for _, f := range files {
					p := filepath.Join(target, f.name)
					if _, err := os.Lstat(p); err == nil {
						return fmt.Errorf("%s already exists (use --force to overwrite)", p)
					}
				}
			}
			for _, f := range files {
				if err := writeFileSafely(filepath.Join(target, f.name), []byte(f.content), f.perm, force); err != nil {
					return err
				}
			}
			_, err := fmt.Fprintln(cmd.OutOrStdout(), "created config.yaml, docker-compose.yml, and plugin.go")
			return err
		},
	}
	cmd.Flags().StringVarP(&target, "dir", "d", ".", "target directory")
	cmd.Flags().BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}
