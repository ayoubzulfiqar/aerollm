package main

import (
	"encoding/json"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/spf13/cobra"
)

func newSpatialCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spatial",
		Short: "Spatial reality utilities",
		Long:  "Parse spatial anchors and inspect WebXR translation.",
	}

	cmd.AddCommand(newSpatialParseCmd())
	return cmd
}

func newSpatialParseCmd() *cobra.Command {
	var raw string
	cmd := &cobra.Command{
		Use:   "parse",
		Short: "Parse spatial anchors from JSON text",
		Run: func(_ *cobra.Command, _ []string) {
			if raw == "" {
				fmt.Println(`error: --text is required`)
				return
			}
			anchors := spatial.ParseSpatialAnchors(raw)
			b, _ := json.MarshalIndent(anchors, "", "  ")
			fmt.Println(string(b))
			xr := spatial.ToWebXR(anchors, "")
			out, _ := json.MarshalIndent(xr, "", "  ")
			fmt.Println("---")
			fmt.Println(string(out))
		},
	}
	cmd.Flags().StringVarP(&raw, "text", "t", "", "JSON text to parse")
	return cmd
}
