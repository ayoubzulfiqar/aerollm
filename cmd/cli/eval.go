package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/eval"
	"github.com/spf13/cobra"
)

func newEvalCmd() *cobra.Command {
	var kind, prompt, response, model, provider, promptVersion, dataset, rubric string
	cmd := &cobra.Command{
		Use:   "eval",
		Short: "Run evaluation workflows",
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch kind {
			case "judge":
				pipeline := eval.NewJudgePipeline(&cliJudgeClient{}, eval.NewInMemoryScoreStore(), rubric)
				score, err := pipeline.ScoreRequest(cmd.Context(), prompt, response, model, provider, promptVersion)
				if err != nil {
					return err
				}
				fmt.Println(score)
			case "regression":
				detector := eval.NewRegressionDetector(eval.NewInMemoryScoreStore())
				regressions, err := detector.Detect(cmd.Context(), eval.ScoreFilter{})
				if err != nil {
					return err
				}
				fmt.Println(len(regressions))
			case "benchmark":
				runner := eval.NewBenchmarkRunner(&cliJudgeClient{}, eval.NewInMemoryScoreStore())
				result, err := runner.Run(cmd.Context(), strings.NewReader(dataset), model, provider, rubric)
				if err != nil {
					return err
				}
				fmt.Println(result.Total)
			default:
				return fmt.Errorf("unknown eval kind: %s", kind)
			}
			return nil
		},
	}
	cmd.Flags().StringVarP(&kind, "kind", "k", "", "kind: judge|regression|benchmark")
	cmd.Flags().StringVarP(&prompt, "prompt", "p", "", "prompt text")
	cmd.Flags().StringVarP(&response, "response", "r", "", "response text")
	cmd.Flags().StringVarP(&model, "model", "m", "", "model")
	cmd.Flags().StringVarP(&provider, "provider", "", "", "provider")
	cmd.Flags().StringVarP(&promptVersion, "prompt-version", "", "", "prompt version")
	cmd.Flags().StringVarP(&dataset, "dataset", "d", "", "jsonl dataset")
	cmd.Flags().StringVarP(&rubric, "rubric", "", "general", "rubric")

	return cmd
}

type cliJudgeClient struct{}

func (f *cliJudgeClient) ChatCompletion(ctx context.Context, prompt string) (string, error) {
	return "85", nil
}
