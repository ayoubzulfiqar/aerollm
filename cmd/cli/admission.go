package main

import (
	"net/http"
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
	var resource, path, method, body string
	cmd := &cobra.Command{
		Use:     "validate",
		Short:   "Ask the gateway to validate a request (POST /v1/admission/validate)",
		Example: "  aerollm admission validate --resource models --path /v1/models --method POST",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			m, err := requireOneOf("method", method, "get", "head", "post", "put", "patch", "delete")
			if err != nil {
				return err
			}
			reqBody, err := readValueArg(cmd, body)
			if err != nil {
				return err
			}
			req := admission.AdmissionRequest{
				Resource: resource,
				Path:     path,
				Method:   strings.ToUpper(m),
				Body:     reqBody,
			}
			return serverRequest(cmd, http.MethodPost, "/v1/admission/validate", nil, req, formatJSON, nil, nil)
		},
	}
	cmd.Flags().StringVar(&resource, "resource", "models", "resource being accessed")
	cmd.Flags().StringVar(&path, "path", "/v1/models", "request path to validate")
	cmd.Flags().StringVar(&method, "method", "POST", "HTTP method to validate")
	cmd.Flags().StringVar(&body, "body", "", "request body to validate (literal, @file, or - for stdin)")
	return cmd
}
