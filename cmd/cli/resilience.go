package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newResilienceCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "resilience",
		Short: "Show local resilience/status snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(`{"state":"ok"}`)
		},
	}
}
