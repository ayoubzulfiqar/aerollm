package hardware

import (
	"errors"
	"io/fs"
	"runtime"
	"testing"
)

func TestParseMemInfoTotal(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
		ok   bool
	}{
		{"MemTotal:       16318324 kB\nMemFree: 1 kB\n", 16318324 * 1024, true},
		{"MemFree: 1 kB\nMemTotal: 2048 kB", 2048 * 1024, true},
		{"MemTotal: 4096", 4096, true},
		{"MemTotal:", 0, false},
		{"MemTotal: abc kB", 0, false},
		{"MemTotal: 0 kB", 0, false},
		{"MemTotal: 18446744073709551615 kB", 0, false}, // overflow
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := parseMemInfoTotal([]byte(c.in))
		if got != c.want || ok != c.ok {
			t.Errorf("parseMemInfoTotal(%q) = %d,%v want %d,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestParseCgroupLimit(t *testing.T) {
	if _, ok := parseCgroupLimit([]byte("max\n")); ok {
		t.Fatal("max must mean unlimited")
	}
	if _, ok := parseCgroupLimit([]byte("9223372036854771712")); ok {
		t.Fatal("v1 sentinel must mean unlimited")
	}
	if v, ok := parseCgroupLimit([]byte("536870912\n")); !ok || v != 512<<20 {
		t.Fatalf("unexpected: %d %v", v, ok)
	}
	if _, ok := parseCgroupLimit([]byte("-1")); ok {
		t.Fatal("negative must be rejected")
	}
}

func fakeReader(files map[string]string) func(string) ([]byte, error) {
	return func(p string) ([]byte, error) {
		if v, ok := files[p]; ok {
			return []byte(v), nil
		}
		return nil, fs.ErrNotExist
	}
}

func TestDetectSystemInfoLinux(t *testing.T) {
	info := detectSystemInfo("linux", fakeReader(map[string]string{
		"/proc/meminfo":             "MemTotal: 8388608 kB\n",
		"/sys/fs/cgroup/memory.max": "2147483648\n",
	}), nil)
	if !info.MemoryKnown || info.TotalMemoryBytes != 2<<30 || info.MemoryGB() != 2 {
		t.Fatalf("expected cgroup-limited 2GiB, got %+v", info)
	}
	if info.NumCPU != runtime.NumCPU() || info.OS != "linux" {
		t.Fatalf("unexpected info: %+v", info)
	}

	info = detectSystemInfo("linux", fakeReader(map[string]string{
		"/proc/meminfo":             "MemTotal: 8388608 kB\n",
		"/sys/fs/cgroup/memory.max": "max\n",
	}), nil)
	if info.MemoryGB() != 8 {
		t.Fatalf("expected 8GiB, got %+v", info)
	}
}

func TestDetectSystemInfoMissingProc(t *testing.T) {
	native := func() (uint64, bool) { return 16 << 30, true }
	info := detectSystemInfo("linux", func(string) ([]byte, error) { return nil, errors.New("no /proc") }, native)
	if info.MemoryKnown || info.MemoryGB() != 0 || info.NumCPU < 1 {
		t.Fatalf("expected unknown memory without panic (linux never uses native), got %+v", info)
	}
	for _, goos := range []string{"darwin", "windows", "freebsd"} {
		if info := detectSystemInfo(goos, fakeReader(nil), nil); info.MemoryKnown {
			t.Fatalf("%s: memory must be reported unknown without native detection, got %+v", goos, info)
		}
		if info := detectSystemInfo(goos, fakeReader(nil), func() (uint64, bool) { return 0, true }); info.MemoryKnown {
			t.Fatalf("%s: zero memory must be reported unknown, got %+v", goos, info)
		}
	}
}

func TestDetectSystemInfoNative(t *testing.T) {
	for _, goos := range []string{"darwin", "windows"} {
		info := detectSystemInfo(goos, fakeReader(nil), func() (uint64, bool) { return 16 << 30, true })
		if !info.MemoryKnown || info.TotalMemoryBytes != 16<<30 || info.MemoryGB() != 16 || info.OS != goos {
			t.Fatalf("%s: expected 16GiB from native detection, got %+v", goos, info)
		}
	}
}

func TestDecodeLittleEndianUint64(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
		ok   bool
	}{
		// 16 GiB = 0x0000000400000000; syscall.Sysctl strips the trailing
		// (most significant) NUL byte, leaving 7 bytes.
		{"\x00\x00\x00\x00\x04\x00\x00", 16 << 30, true},
		{"\x00\x00\x00\x00\x04\x00\x00\x00", 16 << 30, true},
		{"\x01", 1, true},
		{"\xff\xff\xff\xff\xff\xff\xff\xff", ^uint64(0), true},
		{"", 0, false},
		{"123456789", 0, false},
	}
	for _, c := range cases {
		got, ok := decodeLittleEndianUint64(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("decodeLittleEndianUint64(%q) = %d,%v want %d,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestDetectSystemInfoHost(t *testing.T) {
	info := DetectSystemInfo()
	if info.NumCPU < 1 || info.OS != runtime.GOOS {
		t.Fatalf("unexpected host info: %+v", info)
	}
	if b, ok := TotalMemoryBytes(); runtime.GOOS == "linux" && (!ok || b == 0) {
		t.Logf("memory unknown on this linux host (restricted /proc?): %d %v", b, ok)
	}
}
