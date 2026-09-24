package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/spf13/cobra"
)

// modelList is the OpenAI list response of GET /v1/models.
type modelList struct {
	Object string `json:"object"`
	Data   []struct {
		ID      string `json:"id"`
		Object  string `json:"object"`
		Created int64  `json:"created"`
		OwnedBy string `json:"owned_by"`
	} `json:"data"`
}

func newModelsCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "models",
		Short: "List the models served by the gateway",
	}
	cmd.AddCommand(&cobra.Command{
		Use:     "list",
		Short:   "List available models (GET /v1/models)",
		Example: "  aerollm models list\n  aerollm models list -o json",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			format, err := outputFormat(cmd, formatTable)
			if err != nil {
				return err
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodGet, "/v1/models", nil, nil)
			if err != nil {
				return err
			}
			if format == formatJSON {
				return writeRawJSON(cmd.OutOrStdout(), data)
			}
			var list modelList
			if err := json.Unmarshal(data, &list); err != nil {
				return fmt.Errorf("decoding /v1/models response: %w", err)
			}
			if len(list.Data) == 0 {
				_, err := fmt.Fprintln(cmd.OutOrStdout(), "(no models)")
				return err
			}
			sort.SliceStable(list.Data, func(i, j int) bool { return list.Data[i].ID < list.Data[j].ID })
			rows := make([][]string, 0, len(list.Data))
			for _, m := range list.Data {
				created := ""
				if m.Created > 0 {
					created = time.Unix(m.Created, 0).UTC().Format(time.RFC3339)
				}
				rows = append(rows, []string{m.ID, m.OwnedBy, created})
			}
			if err := writeTable(cmd.OutOrStdout(), []string{"ID", "OWNED_BY", "CREATED"}, rows); err != nil {
				return err
			}
			_, err = fmt.Fprintln(cmd.ErrOrStderr(), strconv.Itoa(len(rows))+" model(s)")
			return err
		},
	})
	return cmd
}
