package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/models"
	"github.com/ayoubzulfiqar/aerollm/internal/traffic"
	"github.com/spf13/cobra"
)

func newTrafficCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "traffic",
		Short: "Traffic and shadow testing utilities",
		Long:  "Dispatch shadow traffic, inspect shadow results, and compare providers.",
	}

	cmd.AddCommand(newTrafficShadowCmd())
	return cmd
}

func newTrafficShadowCmd() *cobra.Command {
	var addr string
	cmd := &cobra.Command{
		Use:   "shadow",
		Short: "Run a local shadow test against /v1/shadow",
		Run: func(_ *cobra.Command, _ []string) {
			if addr == "" {
				addr = "http://localhost:8080"
			}
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/shadow", func(w http.ResponseWriter, r *http.Request) {
				if r == nil || r.Body == nil {
					http.Error(w, `{"error":"missing body"}`, http.StatusBadRequest)
					return
				}
				defer r.Body.Close()
				var req models.LLMRequest
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					http.Error(w, `{"error":"bad request"}`, http.StatusBadRequest)
					return
				}
				shadow := traffic.NewShadowTester()
				err := shadow.RunAsync(r.Context(), "http://127.0.0.1:1", "sk-test", &req)
				if err != nil {
					http.Error(w, `{"error":"shadow dispatch failed"}`, http.StatusBadGateway)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"shadow":"accepted"}`))
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			body := strings.NewReader(`{"model":"shadow-model"}`)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/shadow", body)
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			defer resp.Body.Close()
			buf := new(strings.Builder)
			_, _ = readAll(resp.Body, buf)
			fmt.Println(strings.TrimSpace(buf.String()))
		},
	}
	cmd.Flags().StringVarP(&addr, "addr", "a", "", "base address for shadow endpoint")
	return cmd
}

func readAll(src interface{ Read([]byte) (int, error) }, dst *strings.Builder) (int64, error) {
	buf := make([]byte, 4096)
	var total int64
	for {
		n, err := src.Read(buf)
		total += int64(n)
		if n > 0 {
			dst.Write(buf[:n])
		}
		if err != nil {
			return total, err
		}
	}
}
