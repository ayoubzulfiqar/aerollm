package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/ayoubzulfiqar/aerollm/internal/incident"
	"github.com/spf13/cobra"
)

func newIncidentCmd() *cobra.Command {
	var (
		id, title, description, severity, status, source string
		resolve                                          bool
	)
	cmd := &cobra.Command{
		Use:   "incident",
		Short: "List, open, inspect and transition incidents (/v1/incidents)",
		Example: `  aerollm incident
  aerollm incident --status open --severity high
  aerollm incident --title "OpenAI 5xx spike" --severity high --desc "error rate 12%"
  aerollm incident --id inc_123 --status investigating
  aerollm incident --id inc_123 --resolve`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			sev := incident.Severity(strings.ToLower(severity))
			if cmd.Flags().Changed("severity") && !incident.ValidSeverity(sev) {
				return fmt.Errorf("invalid --severity %q", severity)
			}
			st := incident.Status(strings.ToLower(status))
			if cmd.Flags().Changed("status") && !incident.ValidStatus(st) {
				return fmt.Errorf("invalid --status %q", status)
			}
			switch {
			case resolve:
				if id == "" {
					return errors.New("--resolve requires --id")
				}
				q := url.Values{"resolve": {"true"}, "id": {id}}
				return serverRequest(cmd, http.MethodPost, "/v1/incidents", q, nil, formatJSON, nil, nil)
			case title != "":
				if !cmd.Flags().Changed("severity") {
					sev = incident.SeverityMedium
				}
				inc := incident.Incident{ID: id, Title: title, Description: description, Severity: sev, Source: source}
				if cmd.Flags().Changed("status") {
					inc.Status = st
				}
				return serverRequest(cmd, http.MethodPost, "/v1/incidents", nil, inc, formatJSON, nil, nil)
			case id != "" && anyFlagChanged(cmd, "status", "severity", "desc", "source"):
				patch := incident.Patch{}
				if cmd.Flags().Changed("status") {
					patch.Status = &st
				}
				if cmd.Flags().Changed("severity") {
					patch.Severity = &sev
				}
				if cmd.Flags().Changed("desc") {
					patch.Description = &description
				}
				if cmd.Flags().Changed("source") {
					patch.Source = &source
				}
				return serverRequest(cmd, http.MethodPatch, "/v1/incidents", idQuery(id), patch, formatJSON, nil, nil)
			case id != "":
				return serverRequest(cmd, http.MethodGet, "/v1/incidents", idQuery(id), nil, formatJSON, nil, nil)
			default:
				q := url.Values{}
				if cmd.Flags().Changed("status") {
					q.Set("status", string(st))
				}
				if cmd.Flags().Changed("severity") {
					q.Set("severity", string(sev))
				}
				return serverRequest(cmd, http.MethodGet, "/v1/incidents", q, nil, formatTable,
					[]string{"id", "title", "severity", "status", "created_at"}, nil)
			}
		},
	}
	f := cmd.Flags()
	f.StringVar(&id, "id", "", "incident id (get, update, resolve)")
	f.StringVarP(&title, "title", "t", "", "incident title (opens a new incident)")
	f.StringVarP(&description, "desc", "d", "", "incident description")
	f.StringVarP(&severity, "severity", "s", "medium", "severity: low|medium|high|critical (also filters the list)")
	f.StringVar(&status, "status", "open", "status: open|acknowledged|investigating|resolved|closed (also filters the list)")
	f.StringVar(&source, "source", "cli", "incident source")
	f.BoolVar(&resolve, "resolve", false, "resolve incident --id")

	return cmd
}
