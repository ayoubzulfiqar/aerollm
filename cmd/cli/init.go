package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

func newInitCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "init",
		Short: "Generate starter config and plugin template",
		Run: func(cmd *cobra.Command, args []string) {
			_ = os.WriteFile("config.yaml", []byte("server:\n  port: 8080\n"), 0644)
			_ = os.WriteFile("plugin.go", []byte("package main\n\nimport (\n\t\"fmt\"\n)\n\nfunc main() { fmt.Println(\"hello wasm\") }\n"), 0644)
			fmt.Println("created config.yaml and plugin.go")
		},
	}
}
