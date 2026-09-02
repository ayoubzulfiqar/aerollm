package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newGitOpsCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "sync",
		Short: "Trigger a GitOps sync for prompt templates and policies",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println("gitops sync triggered")
		},
	}
}
