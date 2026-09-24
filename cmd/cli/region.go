package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ayoubzulfiqar/aerollm/internal/region"
	"github.com/spf13/cobra"
)

func newRegionCmd() *cobra.Command {
	var (
		resource, id, name, endpoint, regionID, dataType string
		providers                                        []string
		primary, required, enabled, del                  bool
		priority                                         int
	)
	cmd := &cobra.Command{
		Use:   "region",
		Short: "Manage regions, residency policies and region route rules",
		Long: `List or upsert regions (/v1/region/regions), data-residency policies
(/v1/region/residency) and route rules (/v1/region/routes).

Without creation flags the selected --resource is listed (or fetched with
--id); --delete --id removes it.`,
		Example: `  aerollm region
  aerollm region --name us-east-1 --endpoint https://us.example.com --primary
  aerollm region -r residency --id eu-pii --region eu-west-1 --data-type pii --required
  aerollm region -r route --id eu-route --region eu-west-1 --providers openai,anthropic --priority 1`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := requireOneOf("resource", resource, "region", "residency", "route")
			if err != nil {
				return err
			}
			path := map[string]string{
				"region":    "/v1/region/regions",
				"residency": "/v1/region/residency",
				"route":     "/v1/region/routes",
			}[res]
			if del {
				if id == "" {
					return errors.New("--delete requires --id")
				}
				if err := serverRequest(cmd, http.MethodDelete, path, idQuery(id), nil, formatJSON, nil, nil); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted %s %s\n", res, id)
				return nil
			}
			switch res {
			case "region":
				if anyFlagChanged(cmd, "name", "endpoint", "primary") {
					if name == "" {
						return errors.New("--name is required to create a region")
					}
					if endpoint != "" {
						if err := validateHTTPURL(endpoint); err != nil {
							return fmt.Errorf("--endpoint: %w", err)
						}
					}
					if id == "" {
						id = name
					}
					body := region.Region{ID: id, Name: name, Endpoint: endpoint, Primary: primary}
					return serverRequest(cmd, http.MethodPost, path, nil, body, formatJSON, nil, nil)
				}
				if id != "" {
					return serverRequest(cmd, http.MethodGet, path, idQuery(id), nil, formatJSON, nil, nil)
				}
				return serverRequest(cmd, http.MethodGet, path, nil, nil, formatTable,
					[]string{"id", "name", "endpoint", "primary"}, nil)
			case "residency":
				if anyFlagChanged(cmd, "region", "data-type", "required") {
					if id == "" || regionID == "" || dataType == "" {
						return errors.New("--id, --region and --data-type are required to create a residency policy")
					}
					body := region.ResidencyPolicy{ID: id, Region: regionID, DataType: dataType, Required: required}
					return serverRequest(cmd, http.MethodPost, path, nil, body, formatJSON, nil, nil)
				}
				if id != "" {
					return serverRequest(cmd, http.MethodGet, path, idQuery(id), nil, formatJSON, nil, nil)
				}
				return serverRequest(cmd, http.MethodGet, path, nil, nil, formatTable,
					[]string{"id", "region", "data_type", "required"}, nil)
			default: // route
				if anyFlagChanged(cmd, "region", "providers", "priority", "enabled") {
					if id == "" || regionID == "" || len(providers) == 0 {
						return errors.New("--id, --region and --providers are required to create a route rule")
					}
					body := region.RouteRule{ID: id, Region: regionID, Providers: providers, Priority: priority, Enabled: enabled}
					return serverRequest(cmd, http.MethodPost, path, nil, body, formatJSON, nil, nil)
				}
				if id != "" {
					return serverRequest(cmd, http.MethodGet, path, idQuery(id), nil, formatJSON, nil, nil)
				}
				return serverRequest(cmd, http.MethodGet, path, nil, nil, formatTable,
					[]string{"id", "region", "providers", "priority", "enabled"}, nil)
			}
		},
	}
	f := cmd.Flags()
	f.StringVarP(&resource, "resource", "r", "region", "resource: region|residency|route")
	f.StringVar(&id, "id", "", "resource id (regions default to --name)")
	f.StringVarP(&name, "name", "n", "", "region name")
	f.StringVarP(&endpoint, "endpoint", "e", "", "region endpoint URL")
	f.BoolVarP(&primary, "primary", "p", false, "mark the region as primary")
	f.StringVar(&regionID, "region", "", "region id (residency and route)")
	f.StringVar(&dataType, "data-type", "", "data type covered by a residency policy, e.g. pii")
	f.BoolVar(&required, "required", false, "residency is mandatory")
	f.StringSliceVar(&providers, "providers", nil, "providers for a route rule (comma separated)")
	f.IntVar(&priority, "priority", 0, "route rule priority")
	f.BoolVar(&enabled, "enabled", true, "route rule enabled")
	f.BoolVar(&del, "delete", false, "delete the resource identified by --id")

	return cmd
}

// validateHTTPURL checks that s is an absolute http(s) URL.
func validateHTTPURL(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%q is not an absolute http(s) URL", s)
	}
	return nil
}
