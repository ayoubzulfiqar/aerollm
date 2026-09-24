package hardware

import (
	"runtime"
	"testing"
)

// TestNativeTotalMemoryHost exercises the real platform detection on the OS
// running the tests: sysctl on macOS, GlobalMemoryStatusEx on Windows and
// /proc/meminfo on Linux. Other platforms skip.
func TestNativeTotalMemoryHost(t *testing.T) {
	switch runtime.GOOS {
	case "darwin", "windows":
		total, ok := nativeTotalMemory()
		if !ok {
			t.Skipf("native memory detection unavailable on this %s host", runtime.GOOS)
		}
		// Any machine able to run the test suite has more than 64 MiB and less
		// than 1 PiB of RAM; anything else means the value was mis-decoded.
		if total < 64<<20 || total > 1<<50 {
			t.Fatalf("implausible total memory %d bytes", total)
		}
		info := DetectSystemInfo()
		if !info.MemoryKnown || info.TotalMemoryBytes != total {
			t.Fatalf("DetectSystemInfo = %+v, want %d bytes", info, total)
		}
	case "linux":
		if _, ok := nativeTotalMemory(); ok {
			t.Fatal("linux must use /proc detection, not the native stub")
		}
		info := DetectSystemInfo()
		if !info.MemoryKnown {
			t.Skip("memory unknown on this linux host (restricted /proc?)")
		}
		if info.TotalMemoryBytes < 64<<20 {
			t.Fatalf("implausible total memory %d bytes", info.TotalMemoryBytes)
		}
	default:
		t.Skipf("memory detection not implemented on %s", runtime.GOOS)
	}
}
