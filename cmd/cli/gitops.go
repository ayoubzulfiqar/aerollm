package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/gitops"
	"github.com/spf13/cobra"
)

var gitRefPattern = regexp.MustCompile(`^[A-Za-z0-9._/-]{1,200}$`)

func newGitOpsCmd() *cobra.Command {
	var repo, dir, branch string
	cmd := &cobra.Command{
		Use:   "sync",
		Short: "Sync prompt templates from a GitOps repository into a local checkout",
		Long: `Clone (or fast-forward) the prompt-template repository and list the prompt
versions found under prompts/. The repository comes from --repo or
$AEROLLM_GITOPS_REPO.`,
		Example: "  aerollm sync --repo https://github.com/acme/prompts.git --dir ./prompts --branch main",
		Args:    cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if repo == "" {
				repo = strings.TrimSpace(os.Getenv("AEROLLM_GITOPS_REPO"))
			}
			if repo == "" {
				return errors.New("--repo (or AEROLLM_GITOPS_REPO) is required")
			}
			// Prevent option injection into git (e.g. --upload-pack=...) and
			// the command-executing ext:: transport.
			if strings.HasPrefix(repo, "-") || strings.HasPrefix(strings.ToLower(repo), "ext::") {
				return fmt.Errorf("refusing unsafe repository URL %q", repo)
			}
			if strings.HasPrefix(branch, "-") || !gitRefPattern.MatchString(branch) || strings.Contains(branch, "..") {
				return fmt.Errorf("invalid --branch %q", branch)
			}
			if strings.HasPrefix(dir, "-") {
				return fmt.Errorf("invalid --dir %q", dir)
			}
			dir = filepath.Clean(dir)
			commit, err := gitSync(cmd.Context(), repo, dir, branch)
			if err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "gitops sync complete: %s@%s -> %s (commit %s)\n", repo, branch, dir, commit)
			versions, err := gitops.NewGitPromptStore(repo, dir, branch, time.Minute).List()
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					fmt.Fprintln(w, "no prompts/ directory in repository")
					return nil
				}
				return err
			}
			fmt.Fprintf(w, "prompt versions: %d\n", len(versions))
			for _, v := range versions {
				fmt.Fprintln(w, "- "+v)
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&repo, "repo", "", "git repository URL (env AEROLLM_GITOPS_REPO)")
	cmd.Flags().StringVar(&dir, "dir", "aerollm-prompts", "local checkout directory")
	cmd.Flags().StringVar(&branch, "branch", "main", "branch to track")
	return cmd
}

// gitSync clones repo into dir (or fast-forwards an existing checkout) and
// returns the checked-out commit.
func gitSync(ctx context.Context, repo, dir, branch string) (string, error) {
	safe := []string{"-c", "protocol.ext.allow=never"}
	run := func(args ...string) (string, error) {
		c := exec.CommandContext(ctx, "git", append(safe, args...)...)
		c.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0")
		out, err := c.CombinedOutput()
		if err != nil {
			return "", fmt.Errorf("git %s failed: %s: %w", args[0], strings.TrimSpace(string(out)), err)
		}
		return strings.TrimSpace(string(out)), nil
	}
	if _, err := os.Stat(filepath.Join(dir, ".git")); errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
			return "", err
		}
		if _, err := run("clone", "--branch", branch, "--single-branch", "--", repo, dir); err != nil {
			return "", err
		}
	} else if err != nil {
		return "", err
	} else if _, err := run("-C", dir, "pull", "--ff-only", "origin", branch); err != nil {
		return "", err
	}
	return run("-C", dir, "rev-parse", "HEAD")
}
