package main

import (
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/spf13/cobra"
)

func newEvalCmd() *cobra.Command {
	var kind, prompt, response, model, provider, promptVersion, dataset, rubric string
	cmd := &cobra.Command{
		Use:   "eval",
		Short: "Run LLM-as-judge, regression and benchmark evaluations on the server",
		Example: `  aerollm eval --kind judge --prompt "2+2?" --response "4" --model gpt-4o
  aerollm eval --kind regression
  aerollm eval --kind benchmark --dataset @bench.jsonl --model gpt-4o --provider openai`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			k, err := requireOneOf("kind", kind, "judge", "regression", "benchmark")
			if err != nil {
				return err
			}
			switch k {
			case "judge":
				if strings.TrimSpace(prompt) == "" || strings.TrimSpace(response) == "" {
					return errors.New("--prompt and --response are required for --kind judge")
				}
				body := map[string]string{
					"prompt":         prompt,
					"response":       response,
					"model":          model,
					"provider":       provider,
					"prompt_version": promptVersion,
					"rubric":         rubric,
				}
				return serverRequest(cmd, http.MethodPost, "/v1/eval/judge", nil, body, formatTable, nil, nil)
			case "regression":
				return serverRequest(cmd, http.MethodGet, "/v1/eval/regression", nil, nil, formatJSON, nil, nil)
			default: // benchmark
				if dataset == "" {
					return errors.New("--dataset is required for --kind benchmark")
				}
				data, err := readValueArg(cmd, dataset)
				if err != nil {
					return err
				}
				if strings.TrimSpace(data) == "" {
					return errors.New("--dataset is empty")
				}
				body := map[string]string{"dataset": data, "model": model, "provider": provider, "rubric": rubric}
				if err := serverRequest(cmd, http.MethodPost, "/v1/eval/benchmark", nil, body, formatJSON, nil, nil); err != nil {
					return fmt.Errorf("benchmark: %w", err)
				}
				return nil
			}
		},
	}
	cmd.Flags().StringVarP(&kind, "kind", "k", "", "evaluation kind: judge|regression|benchmark")
	cmd.Flags().StringVarP(&prompt, "prompt", "p", "", "prompt text (judge)")
	cmd.Flags().StringVarP(&response, "response", "r", "", "response text to score (judge)")
	cmd.Flags().StringVarP(&model, "model", "m", "", "model")
	cmd.Flags().StringVarP(&provider, "provider", "", "", "provider")
	cmd.Flags().StringVarP(&promptVersion, "prompt-version", "", "", "prompt version")
	cmd.Flags().StringVarP(&dataset, "dataset", "d", "", "JSONL dataset (literal, @file, or - for stdin)")
	cmd.Flags().StringVarP(&rubric, "rubric", "", "general", "rubric")

	return cmd
}
