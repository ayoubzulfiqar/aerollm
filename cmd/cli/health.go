package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

func newHealthCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "health",
		Short: "Print a local health snapshot",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Println(`{"status":"ok"}`)
		},
	}
}
