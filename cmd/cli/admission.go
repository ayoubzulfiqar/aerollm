package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/admission"
	"github.com/spf13/cobra"
)

func newAdmissionCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "admission",
		Short: "Admission webhook utilities",
		Long:  "Validate admission requests and inspect policy decisions.",
	}
	cmd.AddCommand(newAdmissionValidateCmd())
	return cmd
}

func newAdmissionValidateCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "validate",
		Short: "Run a local admission validation test against /v1/admission/validate",
		Run: func(_ *cobra.Command, _ []string) {
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/admission/validate", admission.WebhookHandler(admission.ValidatorFunc(func(req admission.AdmissionRequest) admission.AdmissionResponse {
				return admission.AdmissionResponse{Allowed: true, Reason: "admission allow"}
			})))

			server := httptest.NewServer(mux)
			defer server.Close()

			client := server.Client()
			body := strings.NewReader(`{"resource":"models","path":"/v1/models","method":"POST"}`)
			req, _ := http.NewRequest(http.MethodPost, server.URL+"/v1/admission/validate", body)
			req.Header.Set("Content-Type", "application/json")
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println("error: " + err.Error())
				return
			}
			defer resp.Body.Close()
			buf := make([]byte, 4096)
			n, _ := resp.Body.Read(buf)
			fmt.Println(strings.TrimSpace(string(buf[:n])))
		},
	}
	return cmd
}
