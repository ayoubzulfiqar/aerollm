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
		Short: "Generate starter config and plugin template",
		Run: func(cmd *cobra.Command, args []string) {
			if target == "" {
				target = "."
			}
			_ = os.WriteFile(target+"/config.yaml", []byte("server:\n  port: 8080\n"), 0644)
			_ = os.WriteFile(target+"/plugin.go", []byte("package main\n\nimport (\n\t\"fmt\"\n)\n\nfunc main() { fmt.Println(\"hello wasm\") }\n"), 0644)
			fmt.Println("created config.yaml and plugin.go")
		},
	}
	cmd.Flags().StringVarP(&target, "dir", "d", ".", "target directory")
	return cmd
}
