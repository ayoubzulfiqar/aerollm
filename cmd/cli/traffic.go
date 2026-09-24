package main

import (
	"errors"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

func newTrafficCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "traffic",
		Short: "Traffic and shadow testing utilities",
		Long:  "Dispatch shadow traffic to compare providers.",
	}

	cmd.AddCommand(newTrafficShadowCmd())
	return cmd
}

func newTrafficShadowCmd() *cobra.Command {
	var model, prompt string
	cmd := &cobra.Command{
		Use:     "shadow",
		Short:   "Dispatch a request as shadow traffic (POST /v1/shadow)",
		Example: `  aerollm traffic shadow --model gpt-4o-mini --prompt "hello"`,
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if model == "" {
				return errors.New("--model is required")
			}
			text, err := readValueArg(cmd, prompt)
			if err != nil {
				return err
			}
			if strings.TrimSpace(text) == "" {
				return errors.New("--prompt is required")
			}
			req := chatRequest{Model: model, Messages: []chatMessage{{Role: "user", Content: text}}}
			return serverRequest(cmd, http.MethodPost, "/v1/shadow", nil, req, formatJSON, nil, nil)
		},
	}
	addDeprecatedAddrFlag(cmd)
	cmd.Flags().StringVarP(&model, "model", "m", "", "model to shadow")
	cmd.Flags().StringVarP(&prompt, "prompt", "p", "", "prompt to send (literal, @file, or - for stdin)")
	return cmd
}
