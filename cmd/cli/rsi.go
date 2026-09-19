package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/spf13/cobra"
)

// newRSICmd creates the RSI (Recursive Self-Improvement) CLI command group.
func newRSICmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "rsi",
		Short: "RSI engine — recursive self-improvement cycles",
		Long:  "Inspect headroom assessments, list cycle history, and trigger RSI cycles.",
	}
	cmd.AddCommand(newRSIHeadroomCmd())
	cmd.AddCommand(newRSICyclesCmd())
	cmd.AddCommand(newRSIITriggerCmd())
	return cmd
}

var rsiAddr string

func init() {
	// Register the rsi command group on the root command.
}

// --- rsi headroom ---

func newRSIHeadroomCmd() *cobra.Command {
	var addr string
	cmd := &cobra.Command{
		Use:   "headroom",
		Short: "Print HCI headroom assessment for all dimensions",
		Run: func(_ *cobra.Command, _ []string) {
			if addr == "" {
				addr = "http://localhost:8080"
			}
			resp, err := http.Get(addr + "/v1/rsi/headroom")
			if err != nil {
				fmt.Printf("error: %v\n", err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var out map[string]interface{}
			if err := json.Unmarshal(body, &out); err != nil {
				fmt.Println(string(body))
				return
			}
			pretty, _ := json.MarshalIndent(out, "", "  ")
			fmt.Println(string(pretty))
		},
	}
	cmd.Flags().StringVarP(&addr, "addr", "a", "", "base address of the aerollm server")
	return cmd
}

// --- rsi cycles ---

func newRSICyclesCmd() *cobra.Command {
	var addr string
	cmd := &cobra.Command{
		Use:   "cycles",
		Short: "List RSI cycle history",
		Run: func(_ *cobra.Command, _ []string) {
			if addr == "" {
				addr = "http://localhost:8080"
			}
			resp, err := http.Get(addr + "/v1/rsi/cycles")
			if err != nil {
				fmt.Printf("error: %v\n", err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)
			var out []map[string]interface{}
			if err := json.Unmarshal(body, &out); err != nil {
				fmt.Println(string(body))
				return
			}
			for _, c := range out {
				id, _ := json.Marshal(c["id"])
				dim, _ := json.Marshal(c["dimension"])
				imp, _ := json.Marshal(c["improvement_pct"])
				deployed, _ := json.Marshal(c["deployed"])
				ts, _ := json.Marshal(c["timestamp"])
				fmt.Printf("cycle %s | dim=%s | improvement=%s%% | deployed=%s | ts=%s\n",
					id, dim, imp, deployed, ts)
			}
		},
	}
	cmd.Flags().StringVarP(&addr, "addr", "a", "", "base address of the aerollm server")
	return cmd
}

// --- rsi trigger ---

func newRSIITriggerCmd() *cobra.Command {
	var addr string
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "trigger",
		Short: "Trigger an RSI cycle on the server",
		Run: func(_ *cobra.Command, _ []string) {
			if addr == "" {
				addr = "http://localhost:8080"
			}
			resp, err := http.Post(addr+"/v1/rsi/cycle", "application/json", nil)
			if err != nil {
				fmt.Printf("error: %v\n", err)
				return
			}
			defer resp.Body.Close()
			body, _ := io.ReadAll(resp.Body)

			if wait > 0 {
				// Poll for completion.
				go func() {
					time.Sleep(wait)
				}()
			}
			fmt.Println(string(body))
		},
	}
	cmd.Flags().StringVarP(&addr, "addr", "a", "", "base address of the aerollm server")
	cmd.Flags().DurationVarP(&wait, "wait", "w", 0, "wait for cycle to complete (duration)")
	return cmd
}
