// Command guest is the WebAssembly test guest shared by the wasmrt, plugins
// and sandbox tests. wasmrttest.BuildGuest compiles it at test time with
// GOOS=wasip1 GOARCH=wasm, so the fixture is reproducible from source.
//
// It reads one JSON object from stdin and selects a behaviour by "mode"
// (top level, or inside "payload" for plugin hook envelopes).
package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

type request struct {
	Mode     string                 `json:"mode"`
	Text     string                 `json:"text"`
	N        int                    `json:"n"`
	Hook     string                 `json:"hook"`
	PluginID string                 `json:"plugin_id"`
	Payload  map[string]interface{} `json:"payload"`
}

func main() {
	in, err := io.ReadAll(os.Stdin)
	if err != nil {
		fmt.Fprintln(os.Stderr, "read stdin:", err)
		os.Exit(4)
	}
	var req request
	if len(in) > 0 {
		if err := json.Unmarshal(in, &req); err != nil {
			fmt.Fprintln(os.Stderr, "bad input:", err)
			os.Exit(4)
		}
	}
	mode := req.Mode
	if mode == "" && req.Payload != nil {
		mode, _ = req.Payload["mode"].(string)
	}

	switch mode {
	case "", "echo":
		_, _ = os.Stdout.Write(in)
	case "upper":
		emit(map[string]interface{}{"text": strings.ToUpper(req.Text), "len": len(req.Text)})
	case "hook":
		p := req.Payload
		if p == nil {
			p = map[string]interface{}{}
		}
		p["seen_by"] = req.PluginID
		p["hook"] = req.Hook
		if s, ok := p["text"].(string); ok {
			p["text"] = strings.ToUpper(s)
		}
		emit(map[string]interface{}{"payload": p})
	case "hook-append":
		p := req.Payload
		steps, _ := p["steps"].(string)
		p["steps"] = steps + req.PluginID
		emit(map[string]interface{}{"payload": p})
	case "hook-error":
		emit(map[string]interface{}{"error": "denied by policy"})
	case "hook-nochange":
		// No output: the payload passes through unchanged.
	case "hook-bad":
		fmt.Print("[1,2,3]")
	case "not-json":
		fmt.Print("hello, world")
	case "loop":
		for {
		}
	case "alloc":
		var keep [][]byte
		for {
			b := make([]byte, 8<<20)
			for i := 0; i < len(b); i += 4096 {
				b[i] = 1
			}
			keep = append(keep, b)
		}
	case "bigout":
		chunk := strings.Repeat("x", 64<<10)
		for {
			if _, err := os.Stdout.WriteString(chunk); err != nil {
				os.Exit(5)
			}
		}
	case "bigerr":
		chunk := strings.Repeat("e", 1<<10)
		for i := 0; i < req.N; i++ {
			_, _ = os.Stderr.WriteString(chunk)
		}
		emit(map[string]interface{}{"ok": true})
	case "exit":
		fmt.Fprintln(os.Stderr, "guest: refusing to continue")
		os.Exit(3)
	case "panic":
		panic("guest panic")
	case "fs":
		var errs []string
		if f, err := os.Open("/etc/passwd"); err == nil {
			f.Close()
			errs = append(errs, "OPENED /etc/passwd")
		}
		if _, err := os.ReadDir("/"); err == nil {
			errs = append(errs, "LISTED /")
		}
		if _, err := os.ReadDir("."); err == nil {
			errs = append(errs, "LISTED .")
		}
		if f, err := os.Create("pwned.txt"); err == nil {
			f.Close()
			errs = append(errs, "CREATED pwned.txt")
		}
		emit(map[string]interface{}{"escapes": errs})
	case "env":
		emit(map[string]interface{}{"args": os.Args, "env": os.Environ()})
	case "clock":
		emit(map[string]interface{}{"unix": time.Now().Unix()})
	case "random":
		b := make([]byte, 16)
		_, _ = rand.Read(b)
		emit(map[string]interface{}{"random": hex.EncodeToString(b)})
	case "sleep":
		time.Sleep(time.Duration(req.N) * time.Millisecond)
		emit(map[string]interface{}{"slept": req.N})
	default:
		fmt.Fprintln(os.Stderr, "unknown mode:", mode)
		os.Exit(6)
	}
}

func emit(v interface{}) {
	if err := json.NewEncoder(os.Stdout).Encode(v); err != nil {
		fmt.Fprintln(os.Stderr, "encode:", err)
		os.Exit(7)
	}
}
