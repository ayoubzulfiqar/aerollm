package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	var target string
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Generate starter config, docker-compose, and plugin template",
		Run: func(cmd *cobra.Command, args []string) {
			if target == "" {
				target = "."
			}

			config := `server:
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
  default_budget_usd: 100
  currency: USD

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
			_ = os.WriteFile(target+"/config.yaml", []byte(config), 0644)

			dockerCompose := `version: "3.8"
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data

  aerollm:
    build: .
    ports:
      - "8080:8080"
    environment:
      - AEROLLM_REDIS_ADDR=redis:6379
      - AEROLLM_LICENSE_KEY=${AEROLLM_LICENSE_KEY}
    depends_on:
      - redis
    volumes:
      - ./config.yaml:/app/config.yaml

volumes:
  redis_data:
`
			_ = os.WriteFile(target+"/docker-compose.yml", []byte(dockerCompose), 0644)

			pluginTemplate := `package main

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
		SizeBytes: 0,
		CreatedAt: 0,
		UpdatedAt: 0,
	}
}

func main() {
	fmt.Println("weather plugin loaded")
}
`
			_ = os.WriteFile(target+"/plugin.go", []byte(pluginTemplate), 0644)

			fmt.Println("created config.yaml, docker-compose.yml, and plugin.go")
		},
	}
	cmd.Flags().StringVarP(&target, "dir", "d", ".", "target directory")
	return cmd
}
