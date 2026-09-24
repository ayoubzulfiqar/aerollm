package main

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

// The plugin templates are standalone Go programs (their own module, no
// aerollm imports) implementing the WASM protocols of internal/plugins:
// the hook envelope of WasmHost.RunHook and the tool protocol of WasmTool.

//go:embed plugintemplates/hook.go.tmpl
var hookPluginTemplate string

//go:embed plugintemplates/tool.go.tmpl
var toolPluginTemplate string

// pluginNamePattern is valid as a plugin id, an agent tool name and a Go
// module path.
var pluginNamePattern = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_-]{0,63}$`)

// scaffoldFile is one generated file, relative to the target directory.
type scaffoldFile struct {
	name    string
	content string
	perm    os.FileMode
}

func defaultPluginName(kind string) string {
	if kind == "tool" {
		return "word_count"
	}
	return "example-hook"
}

// pluginScaffold returns the files of a standalone plugin module of the
// given kind (hook|tool), placed under subdir.
func pluginScaffold(kind, name, subdir string) ([]scaffoldFile, string, error) {
	k, err := requireOneOf("kind", kind, "hook", "tool")
	if err != nil {
		return nil, "", err
	}
	if name == "" {
		name = defaultPluginName(k)
	}
	if !pluginNamePattern.MatchString(name) {
		return nil, "", fmt.Errorf("invalid plugin name %q: use a letter followed by up to 63 letters, digits, '_' or '-'", name)
	}
	src := hookPluginTemplate
	if k == "tool" {
		src = toolPluginTemplate
	}
	src = strings.ReplaceAll(src, "__PLUGIN_NAME__", name)
	gomod := "module " + name + "\n\ngo 1.21\n"
	return []scaffoldFile{
		{filepath.Join(subdir, "go.mod"), gomod, 0o644},
		{filepath.Join(subdir, "main.go"), src, 0o644},
	}, k, nil
}

// writeScaffold writes files under dir. Unless force is set it first checks
// that none exists, so a refusal leaves no partial scaffold.
func writeScaffold(dir string, files []scaffoldFile, force bool) error {
	if !force {
		for _, f := range files {
			p := filepath.Join(dir, f.name)
			if _, err := os.Lstat(p); err == nil {
				return fmt.Errorf("%s already exists (use --force to overwrite)", p)
			}
		}
	}
	for _, f := range files {
		p := filepath.Join(dir, f.name)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			return fmt.Errorf("creating %s: %w", filepath.Dir(p), err)
		}
		if err := writeFileSafely(p, []byte(f.content), f.perm, force); err != nil {
			return err
		}
	}
	return nil
}

func newPluginInitCmd() *cobra.Command {
	var kind, name, dir string
	var force bool
	cmd := &cobra.Command{
		Use:   "init",
		Short: "Scaffold a standalone WASM hook or tool plugin module",
		Long: `Generate go.mod and main.go for a standalone WASM plugin (no aerollm
imports), built with:

  GOOS=wasip1 GOARCH=wasm go build -o plugin.wasm

--kind hook: reads {"version":1,"plugin_id":...,"hook":...,"payload":{...}} on
stdin and writes {"payload":{...}}, {} (unchanged) or {"error":"..."}.
--kind tool: reads the JSON arguments object on stdin and writes one JSON
value; failures go to stderr with a non-zero exit code.`,
		Example: `  aerollm plugin init --kind hook --name redact-pii
  aerollm plugin init --kind tool --name word_count --dir ./tools/wc
  aerollm plugin build ./tools/wc -o word_count.wasm`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			files, k, err := pluginScaffold(kind, name, "")
			if err != nil {
				return err
			}
			if name == "" {
				name = defaultPluginName(k)
			}
			if dir == "" {
				dir = name
			}
			if err := writeScaffold(dir, files, force); err != nil {
				return err
			}
			w := cmd.OutOrStdout()
			fmt.Fprintf(w, "created %s %s plugin in %s (go.mod, main.go)\n", k, name, dir)
			_, err = fmt.Fprintf(w, "build it with: aerollm plugin build %s -o %s.wasm\n", dir, name)
			return err
		},
	}
	f := cmd.Flags()
	f.StringVar(&kind, "kind", "hook", "plugin kind: hook|tool")
	f.StringVar(&name, "name", "", "plugin name, also the Go module path (default example-hook / word_count)")
	f.StringVarP(&dir, "dir", "d", "", "target directory (default: ./<name>)")
	f.BoolVar(&force, "force", false, "overwrite existing files")
	return cmd
}
