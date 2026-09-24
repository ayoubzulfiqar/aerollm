package main

import (
	"errors"
	"fmt"

	"github.com/ayoubzulfiqar/aerollm/internal/spatial"
	"github.com/spf13/cobra"
)

func newSpatialCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "spatial",
		Short: "Spatial reality utilities (parse anchors, WebXR translation)",
		Long:  "Parse spatial anchors and inspect WebXR translation.",
	}

	cmd.AddCommand(newSpatialParseCmd())
	return cmd
}

func newSpatialParseCmd() *cobra.Command {
	var raw string
	cmd := &cobra.Command{
		Use:   "parse",
		Short: "Parse spatial anchors from JSON text and show the WebXR translation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if raw == "" {
				return errors.New("--text is required")
			}
			text, err := readValueArg(cmd, raw)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			anchors := spatial.ParseSpatialAnchors(text)
			if err := writeJSON(w, anchors); err != nil {
				return err
			}
			fmt.Fprintln(w, "---")
			return writeJSON(w, spatial.ToWebXR(anchors, ""))
		},
	}
	cmd.Flags().StringVarP(&raw, "text", "t", "", "JSON text to parse (literal, @file, or - for stdin)")
	return cmd
}
