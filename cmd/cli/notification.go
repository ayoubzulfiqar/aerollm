package main

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/ayoubzulfiqar/aerollm/internal/notification"
	"github.com/spf13/cobra"
)

func newNotificationCmd() *cobra.Command {
	var (
		resource, id, name, chType, target, alertID, channelID string
		enabled, del                                           bool
	)
	cmd := &cobra.Command{
		Use:   "notification",
		Short: "Manage notification channels and alert subscriptions",
		Example: `  aerollm notification -r channel
  aerollm notification -r channel --name oncall --type slack --target https://hooks.slack.com/services/...
  aerollm notification -r subscription --alert-id budget-80 --channel-id oncall
  aerollm notification -r channel --id oncall --delete`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			res, err := requireOneOf("resource", resource, "channel", "subscription")
			if err != nil {
				return err
			}
			path := "/v1/notification/subscriptions"
			if res == "channel" {
				path = "/v1/notification/channels"
			}
			switch {
			case del:
				if id == "" {
					return errors.New("--delete requires --id")
				}
				if err := serverRequest(cmd, http.MethodDelete, path, idQuery(id), nil, formatJSON, nil, nil); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted %s %s\n", res, id)
				return nil
			case res == "channel" && anyFlagChanged(cmd, "name", "type", "target"):
				if name == "" || target == "" {
					return errors.New("--name and --target are required to create a channel")
				}
				typ, err := requireOneOf("type", chType, "webhook", "email", "slack", "sms")
				if err != nil {
					return err
				}
				if typ == "webhook" || typ == "slack" {
					if err := validateHTTPURL(target); err != nil {
						return fmt.Errorf("--target: %w", err)
					}
				}
				ch := notification.Channel{ID: id, Name: name, Type: notification.ChannelType(typ), Target: target, Enabled: enabled}
				if ch.ID == "" {
					ch.ID = name
				}
				return serverRequest(cmd, http.MethodPost, path, nil, ch, formatJSON, nil, nil)
			case res == "subscription" && anyFlagChanged(cmd, "alert-id", "channel-id"):
				if alertID == "" || channelID == "" {
					return errors.New("--alert-id and --channel-id are required to create a subscription")
				}
				sub := notification.Subscription{ID: id, AlertID: alertID, ChannelID: channelID, Enabled: enabled}
				return serverRequest(cmd, http.MethodPost, path, nil, sub, formatJSON, nil, nil)
			case id != "":
				return serverRequest(cmd, http.MethodGet, path, idQuery(id), nil, formatJSON, nil, nil)
			case res == "channel":
				return serverRequest(cmd, http.MethodGet, path, nil, nil, formatTable,
					[]string{"id", "name", "type", "target", "enabled"}, nil)
			default:
				return serverRequest(cmd, http.MethodGet, path, nil, nil, formatTable,
					[]string{"id", "alert_id", "channel_id", "enabled"}, nil)
			}
		},
	}
	f := cmd.Flags()
	f.StringVarP(&resource, "resource", "r", "subscription", "resource: channel|subscription")
	f.StringVar(&id, "id", "", "resource id (get/delete; optional on create)")
	f.StringVar(&name, "name", "", "channel name")
	f.StringVar(&chType, "type", "webhook", "channel type: webhook|email|slack|sms")
	f.StringVar(&target, "target", "", "channel target (URL, email address or phone number)")
	f.StringVar(&alertID, "alert-id", "", "alert id for a subscription")
	f.StringVar(&channelID, "channel-id", "", "channel id for a subscription")
	f.BoolVar(&enabled, "enabled", true, "enable the channel/subscription")
	f.BoolVar(&del, "delete", false, "delete the resource identified by --id")

	return cmd
}
