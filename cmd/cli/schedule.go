package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/ayoubzulfiqar/aerollm/internal/schedule"
	"github.com/spf13/cobra"
)

func newScheduleCmd() *cobra.Command {
	var (
		id, name, taskType, cron, payload, timezone, setStatus string
		del                                                    bool
	)
	cmd := &cobra.Command{
		Use:   "schedule",
		Short: "List, create, update or delete scheduled automation tasks (/v1/schedule)",
		Long: `Manage scheduled tasks.

  --type cron      --schedule is a cron expression (evaluated in --timezone)
  --type interval  --schedule is a duration such as 30s or "@every 5m"
  --type onetime   --schedule is an RFC 3339 timestamp in the future`,
		Example: `  aerollm schedule
  aerollm schedule --name nightly-report --type cron --schedule "0 3 * * *" --payload @job.json
  aerollm schedule --id task_123 --set-status completed
  aerollm schedule --id task_123 --delete`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			switch {
			case del:
				if id == "" {
					return errors.New("--delete requires --id")
				}
				if err := serverRequest(cmd, http.MethodDelete, "/v1/schedule", idQuery(id), nil, formatJSON, nil, nil); err != nil {
					return err
				}
				fmt.Fprintf(cmd.OutOrStdout(), "deleted task %s\n", id)
				return nil
			case setStatus != "":
				if id == "" {
					return errors.New("--set-status requires --id")
				}
				st, err := requireOneOf("set-status", setStatus, "pending", "running", "completed", "failed")
				if err != nil {
					return err
				}
				status := schedule.TaskStatus(st)
				return serverRequest(cmd, http.MethodPut, "/v1/schedule", idQuery(id),
					schedule.TaskUpdate{Status: &status}, formatJSON, nil, nil)
			case name != "" || cron != "":
				if name == "" || cron == "" {
					return errors.New("--name and --schedule are required to create a task")
				}
				typ, err := requireOneOf("type", taskType, "cron", "interval", "onetime")
				if err != nil {
					return err
				}
				body, err := readValueArg(cmd, payload)
				if err != nil {
					return err
				}
				task := schedule.ScheduledTask{
					ID:       id,
					Name:     name,
					Type:     schedule.TaskType(typ),
					Schedule: cron,
					Payload:  body,
					Timezone: timezone,
				}
				return serverRequest(cmd, http.MethodPost, "/v1/schedule", nil, task, formatJSON, nil, nil)
			case id != "":
				return serverRequest(cmd, http.MethodGet, "/v1/schedule", idQuery(id), nil, formatJSON, nil, nil)
			default:
				q := url.Values{}
				if cmd.Flags().Changed("type") {
					q.Set("type", taskType)
				}
				return serverRequest(cmd, http.MethodGet, "/v1/schedule", q, nil, formatTable,
					[]string{"id", "name", "type", "schedule", "status", "next_run"}, nil)
			}
		},
	}
	f := cmd.Flags()
	f.StringVar(&id, "id", "", "task id (get, update, delete; optional on create)")
	f.StringVarP(&name, "name", "n", "", "task name")
	f.StringVarP(&taskType, "type", "t", "cron", "task type: cron|interval|onetime (also filters the list)")
	f.StringVarP(&cron, "schedule", "s", "", "cron expression, interval or RFC 3339 time")
	f.StringVarP(&payload, "payload", "p", "{}", "task payload (literal, @file, or - for stdin)")
	f.StringVar(&timezone, "timezone", "", "IANA timezone for cron schedules (default UTC)")
	f.StringVar(&setStatus, "set-status", "", "update the status of task --id")
	f.BoolVar(&del, "delete", false, "delete task --id")

	return cmd
}
