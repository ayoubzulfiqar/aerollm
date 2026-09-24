package main

import (
	"fmt"
	"net/http"
	"runtime"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/marketplace"
	"github.com/spf13/cobra"
)

func newOpenStandardCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "openstandard",
		Short: "Publish Open Standard capability manifests and receipts to the gateway (/v1/marketplace/openstandard)",
	}
	cmd.AddCommand(newOpenStandardCapabilityCmd())
	cmd.AddCommand(newOpenStandardReceiptCmd())
	return cmd
}

func newOpenStandardCapabilityCmd() *cobra.Command {
	var (
		manifestFile, version, gpuName, currency string
		gpu, metered                             bool
		memoryGB                                 int
		capabilities                             []string
	)
	cmd := &cobra.Command{
		Use:   "capability",
		Short: "Publish a capability manifest (POST /v1/marketplace/openstandard/capability)",
		Example: `  aerollm openstandard capability --gpu --gpu-name "RTX 4090" --memory-gb 64 --capabilities mesh,wasm
  aerollm openstandard capability --file manifest.json`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			var m marketplace.CapabilityManifest
			if manifestFile != "" {
				raw, err := readValueArg(cmd, "@"+manifestFile)
				if err != nil {
					return err
				}
				if err := jsonUnmarshalStrict([]byte(raw), &m); err != nil {
					return fmt.Errorf("--file: %w", err)
				}
			} else {
				m = marketplace.CapabilityManifest{
					Version: version,
					Hardware: marketplace.Hardware{
						HasLocalGPU: gpu,
						GPUName:     gpuName,
						OS:          runtime.GOOS,
						MemoryGB:    memoryGB,
					},
					Billing:      marketplace.Billing{SupportsMetered: metered, Currency: strings.ToUpper(currency)},
					Capabilities: capabilities,
				}
			}
			if err := m.Validate(); err != nil {
				return fmt.Errorf("invalid manifest: %w", err)
			}
			return serverRequest(cmd, http.MethodPost, "/v1/marketplace/openstandard/capability", nil, m, formatJSON, nil, nil)
		},
	}
	f := cmd.Flags()
	f.StringVar(&manifestFile, "file", "", "read the manifest from a JSON file instead of flags")
	f.StringVar(&version, "manifest-version", "1.0", "manifest schema version")
	f.BoolVar(&gpu, "gpu", false, "node has a local GPU")
	f.StringVar(&gpuName, "gpu-name", "", "GPU model")
	f.IntVar(&memoryGB, "memory-gb", 0, "memory in GB")
	f.BoolVar(&metered, "metered", true, "supports metered billing")
	f.StringVar(&currency, "currency", "USD", "billing currency")
	f.StringSliceVar(&capabilities, "capabilities", []string{"mesh", "wasm"}, "advertised capabilities")
	return cmd
}

func newOpenStandardReceiptCmd() *cobra.Command {
	var rf receiptFlags
	cmd := &cobra.Command{
		Use:   "receipt",
		Short: "Publish a billing receipt (POST /v1/marketplace/openstandard/receipt)",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			rec, err := rf.build("server")
			if err != nil {
				return err
			}
			return serverRequest(cmd, http.MethodPost, "/v1/marketplace/openstandard/receipt", nil, rec, formatJSON, nil, nil)
		},
	}
	rf.register(cmd)
	return cmd
}
