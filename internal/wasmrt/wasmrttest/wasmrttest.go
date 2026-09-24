// Package wasmrttest provides WebAssembly fixtures for tests of wasmrt and
// its users (plugins, sandbox). It must only be imported from _test files.
//
// Two kinds of fixtures exist:
//
//   - Guest builds a real Go program (testdata/guest, embedded here) with
//     GOOS=wasip1 GOARCH=wasm at test time, so the fixture is reproducible
//     from source rather than a committed binary. The build runs once per
//     test binary (the go build cache makes repeat runs fast), and the result
//     is compiled once into a never-closed runtime so the process-wide wazero
//     engine keeps its machine code: later Runtimes in the same test binary
//     get it without recompiling (which is slow under -race).
//   - Tiny hand-assembled WASI command modules (Nop, Loop, Hello, Grow,
//     Exit, Import) that need no toolchain and compile instantly.
package wasmrttest

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/ayoubzulfiqar/aerollm/internal/wasmrt"
)

//go:embed testdata/guest/main.go
var guestSource []byte

var (
	guestOnce sync.Once
	guestWasm []byte
	guestErr  error
	guestSkip bool
	// guestPin keeps the guest's compiled code referenced for the lifetime of
	// the test binary. It is intentionally never closed.
	guestPin *wasmrt.Runtime
)

// Guest returns the compiled test guest (see testdata/guest/main.go for the
// supported modes). It skips the test only if no Go toolchain able to
// target wasip1/wasm is available; any other build failure fails the test.
func Guest(tb testing.TB) []byte {
	tb.Helper()
	guestOnce.Do(func() {
		guestWasm, guestSkip, guestErr = buildGuest()
		if guestErr != nil {
			return
		}
		if guestPin, guestErr = wasmrt.New(wasmrt.Config{}); guestErr == nil {
			_, guestErr = guestPin.Compile(context.Background(), guestWasm)
		}
	})
	if guestSkip {
		tb.Skipf("wasip1 guest unavailable: %v", guestErr)
	}
	if guestErr != nil {
		tb.Fatalf("building wasip1 guest: %v", guestErr)
	}
	return guestWasm
}

func buildGuest() (wasm []byte, skip bool, err error) {
	goBin, err := exec.LookPath("go")
	if err != nil {
		return nil, true, fmt.Errorf("go toolchain not found: %w", err)
	}
	if out, err := exec.Command(goBin, "tool", "dist", "list").Output(); err != nil || !bytes.Contains(out, []byte("wasip1/wasm")) {
		return nil, true, errors.New("toolchain cannot target wasip1/wasm")
	}
	dir, err := os.MkdirTemp("", "wasmrt-guest-")
	if err != nil {
		return nil, false, err
	}
	defer os.RemoveAll(dir)
	if err := os.WriteFile(filepath.Join(dir, "main.go"), guestSource, 0o600); err != nil {
		return nil, false, err
	}
	if err := os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module guest\n\ngo 1.21\n"), 0o600); err != nil {
		return nil, false, err
	}
	out := filepath.Join(dir, "guest.wasm")
	cmd := exec.Command(goBin, "build", "-trimpath", "-ldflags=-s -w", "-o", out, ".")
	cmd.Dir = dir
	cmd.Env = append(filteredEnv(), "GOOS=wasip1", "GOARCH=wasm", "CGO_ENABLED=0",
		"GOFLAGS=", "GOWORK=off", "GOTOOLCHAIN=local", "GO111MODULE=on")
	if b, err := cmd.CombinedOutput(); err != nil {
		return nil, false, fmt.Errorf("go build: %w\n%s", err, b)
	}
	wasm, err = os.ReadFile(out)
	return wasm, false, err
}

// filteredEnv drops variables that would redirect or break the guest build.
func filteredEnv() []string {
	var env []string
	for _, kv := range os.Environ() {
		k, _, _ := strings.Cut(kv, "=")
		switch strings.ToUpper(k) {
		case "GOOS", "GOARCH", "CGO_ENABLED", "GOFLAGS", "GOWORK", "GOTOOLCHAIN", "GO111MODULE", "GOEXPERIMENT":
			continue
		}
		env = append(env, kv)
	}
	return env
}

// ---- hand-assembled modules ----

const (
	secType     = 1
	secImport   = 2
	secFunction = 3
	secMemory   = 5
	secExport   = 7
	secCode     = 10
	secData     = 11

	i32 = 0x7f
)

func uleb(v uint64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if v != 0 {
			out = append(out, b|0x80)
			continue
		}
		return append(out, b)
	}
}

func sleb(v int64) []byte {
	var out []byte
	for {
		b := byte(v & 0x7f)
		v >>= 7
		if (v == 0 && b&0x40 == 0) || (v == -1 && b&0x40 != 0) {
			return append(out, b)
		}
		out = append(out, b|0x80)
	}
}

func vec(items ...[]byte) []byte {
	out := uleb(uint64(len(items)))
	for _, it := range items {
		out = append(out, it...)
	}
	return out
}

func name(s string) []byte { return append(uleb(uint64(len(s))), s...) }

func section(id byte, content []byte) []byte {
	return append(append([]byte{id}, uleb(uint64(len(content)))...), content...)
}

func funcType(params, results []byte) []byte {
	return append(append([]byte{0x60}, append(uleb(uint64(len(params))), params...)...),
		append(uleb(uint64(len(results))), results...)...)
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// importSpec is a function import; typ indexes spec.types.
type importSpec struct {
	module, name string
	typ          uint32
}

// spec describes a module with a single defined function exported as
// _start (type ()->()) and an optional memory exported as "memory".
type spec struct {
	types   [][]byte // type 0 is always ()->()
	imports []importSpec
	memory  bool
	memMin  uint32
	body    []byte // instructions of _start, without the final end
	data    []byte // active data segment at offset 0
}

func (s spec) encode() []byte {
	types := append([][]byte{funcType(nil, nil)}, s.types...)
	out := []byte{0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00}
	out = append(out, section(secType, vec(types...))...)
	if len(s.imports) > 0 {
		var imps [][]byte
		for _, im := range s.imports {
			imps = append(imps, cat(name(im.module), name(im.name), []byte{0x00}, uleb(uint64(im.typ))))
		}
		out = append(out, section(secImport, vec(imps...))...)
	}
	out = append(out, section(secFunction, vec(uleb(0)))...)
	if s.memory {
		out = append(out, section(secMemory, vec(cat([]byte{0x00}, uleb(uint64(s.memMin)))))...)
	}
	startIdx := uint64(len(s.imports))
	exports := [][]byte{cat(name("_start"), []byte{0x00}, uleb(startIdx))}
	if s.memory {
		exports = append(exports, cat(name("memory"), []byte{0x02, 0x00}))
	}
	out = append(out, section(secExport, vec(exports...))...)
	body := cat([]byte{0x00}, s.body, []byte{0x0b}) // no locals
	out = append(out, section(secCode, vec(cat(uleb(uint64(len(body))), body)))...)
	if len(s.data) > 0 {
		seg := cat([]byte{0x00, 0x41, 0x00, 0x0b}, uleb(uint64(len(s.data))), s.data)
		out = append(out, section(secData, vec(seg))...)
	}
	return out
}

// Nop is a WASI command whose _start returns immediately (exit code 0, no
// output).
func Nop() []byte { return spec{memory: true, memMin: 1}.encode() }

// Loop is a WASI command whose _start spins forever: loop { br 0 }.
func Loop() []byte {
	return spec{memory: true, memMin: 1, body: []byte{0x03, 0x40, 0x0c, 0x00, 0x0b}}.encode()
}

// Hello is a WASI command that writes msg to stdout with fd_write and
// exits normally.
func Hello(msg string) []byte {
	// Memory layout: iovec{buf=16,len} at 0, nwritten at 8, msg at 16.
	data := make([]byte, 16+len(msg))
	binary.LittleEndian.PutUint32(data[0:], 16)
	binary.LittleEndian.PutUint32(data[4:], uint32(len(msg)))
	copy(data[16:], msg)
	pages := uint32(len(data)/65536 + 1)
	body := cat(
		[]byte{0x41}, sleb(1), // fd = stdout
		[]byte{0x41}, sleb(0), // iovs
		[]byte{0x41}, sleb(1), // iovs_len
		[]byte{0x41}, sleb(8), // nwritten
		[]byte{0x10}, uleb(0), // call fd_write
		[]byte{0x1a}, // drop errno
	)
	return spec{
		types:   [][]byte{funcType([]byte{i32, i32, i32, i32}, []byte{i32})},
		imports: []importSpec{{"wasi_snapshot_preview1", "fd_write", 1}},
		memory:  true, memMin: pages, body: body, data: data,
	}.encode()
}

// Grow is a WASI command that grows its memory (initially 1 page) by pages
// and traps (unreachable) if memory.grow fails.
func Grow(pages uint32) []byte {
	body := cat(
		[]byte{0x41}, sleb(int64(int32(pages))),
		[]byte{0x40, 0x00}, // memory.grow 0
		[]byte{0x41}, sleb(-1),
		[]byte{0x46},       // i32.eq
		[]byte{0x04, 0x40}, // if
		[]byte{0x00},       // unreachable
		[]byte{0x0b},       // end
	)
	return spec{memory: true, memMin: 1, body: body}.encode()
}

// MemoryMin is a WASI command whose memory starts at pages pages.
func MemoryMin(pages uint32) []byte { return spec{memory: true, memMin: pages}.encode() }

// Exit is a WASI command that calls proc_exit(code).
func Exit(code uint32) []byte {
	body := cat([]byte{0x41}, sleb(int64(int32(code))), []byte{0x10}, uleb(0))
	return spec{
		types:   [][]byte{funcType([]byte{i32}, nil)},
		imports: []importSpec{{"wasi_snapshot_preview1", "proc_exit", 1}},
		memory:  true, memMin: 1, body: body,
	}.encode()
}

// Import is a module that imports module.name as a ()->() function.
func Import(module, fn string) []byte {
	return spec{imports: []importSpec{{module, fn, 0}}, memory: true, memMin: 1}.encode()
}

// NoStart is a valid module that does not export _start.
func NoStart() []byte {
	b := spec{memory: true, memMin: 1}.encode()
	// Rename the "_start" export to "_other" (same length keeps sizes valid).
	return bytes.Replace(b, []byte("_start"), []byte("_other"), 1)
}
