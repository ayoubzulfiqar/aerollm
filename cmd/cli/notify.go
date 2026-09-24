package main

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

var severityPattern = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)

func newNotifyCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "notify",
		Short: "Send ad-hoc notifications through configured channels (admin)",
		Long: `Send a notification through the gateway's notification dispatcher, either
to one channel or to every channel subscribed to an alert id. Manage the
channels and subscriptions themselves with "aerollm notification".`,
	}
	cmd.AddCommand(newNotifySendCmd())
	return cmd
}

func newNotifySendCmd() *cobra.Command {
	var (
		channelID, alertID, title, text, severity string
		labels                                    []string
	)
	cmd := &cobra.Command{
		Use:   "send",
		Short: "Send a notification (POST /v1/notification/send)",
		Example: `  aerollm notify send --channel-id oncall --title "Deploy" --text "v2.3 is live"
  aerollm notify send --alert-id budget-80 --title "Budget" --text @message.txt --severity warning --label team=search`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			channelID, alertID = strings.TrimSpace(channelID), strings.TrimSpace(alertID)
			if (channelID == "") == (alertID == "") {
				return errors.New("exactly one of --channel-id or --alert-id is required")
			}
			title = strings.TrimSpace(title)
			if title == "" {
				return errors.New("--title is required")
			}
			body, err := readValueArg(cmd, text)
			if err != nil {
				return err
			}
			if strings.TrimSpace(body) == "" {
				return errors.New("--text is required")
			}
			msg := map[string]any{"title": title, "text": body}
			if channelID != "" {
				msg["channel_id"] = channelID
			} else {
				msg["alert_id"] = alertID
			}
			if severity != "" {
				s := strings.ToLower(strings.TrimSpace(severity))
				if !severityPattern.MatchString(s) {
					return fmt.Errorf("invalid --severity %q (e.g. info, warning, critical)", severity)
				}
				msg["severity"] = s
			}
			if len(labels) > 0 {
				lm := make(map[string]string, len(labels))
				for _, kv := range labels {
					k, v, ok := strings.Cut(kv, "=")
					if k = strings.TrimSpace(k); !ok || k == "" {
						return fmt.Errorf("invalid --label %q: want key=value", kv)
					}
					lm[k] = v
				}
				msg["labels"] = lm
			}
			client, err := newServerClient(cmd)
			if err != nil {
				return err
			}
			data, err := client.call(cmd.Context(), http.MethodPost, "/v1/notification/send", nil, msg)
			if err != nil {
				return err
			}
			target := "channel " + channelID
			if alertID != "" {
				target = "subscribers of alert " + alertID
			}
			return printDone(cmd, data, "notification sent to "+target, map[string]any{"status": "sent"})
		},
	}
	f := cmd.Flags()
	f.StringVar(&channelID, "channel-id", "", "send to this channel")
	f.StringVar(&alertID, "alert-id", "", "send to every channel subscribed to this alert")
	f.StringVar(&title, "title", "", "notification title")
	f.StringVar(&text, "text", "", "notification text (literal, @file, or - for stdin)")
	f.StringVar(&severity, "severity", "", "severity label, e.g. info|warning|critical")
	f.StringArrayVar(&labels, "label", nil, "label key=value (repeatable)")
	return cmd
}
