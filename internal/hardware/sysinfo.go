package hardware

import (
	"bufio"
	"bytes"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
)

// maxProcFileBytes bounds reads of /proc and /sys pseudo-files.
const maxProcFileBytes = 64 << 10

// SystemInfo summarizes host resources relevant to local inference.
type SystemInfo struct {
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	NumCPU int    `json:"num_cpu"`
	// TotalMemoryBytes is the usable memory (the smaller of physical memory
	// and any cgroup limit); 0 when unknown.
	TotalMemoryBytes uint64 `json:"total_memory_bytes,omitempty"`
	// MemoryKnown is false on platforms where memory cannot be determined
	// without cgo or external tools (currently everything except Linux).
	MemoryKnown bool `json:"memory_known"`
}

// MemoryGB returns TotalMemoryBytes in whole GiB (0 when unknown).
func (s SystemInfo) MemoryGB() int {
	if !s.MemoryKnown {
		return 0
	}
	gb := s.TotalMemoryBytes >> 30
	if gb > math.MaxInt32 {
		return math.MaxInt32
	}
	return int(gb)
}

// DetectSystemInfo reports OS, architecture, CPU count and memory.
func DetectSystemInfo() SystemInfo {
	return detectSystemInfo(runtime.GOOS, readFileLimited)
}

// TotalMemoryBytes returns usable memory in bytes and whether it is known.
func TotalMemoryBytes() (uint64, bool) {
	info := DetectSystemInfo()
	return info.TotalMemoryBytes, info.MemoryKnown
}

func detectSystemInfo(goos string, read func(string) ([]byte, error)) SystemInfo {
	info := SystemInfo{OS: goos, Arch: runtime.GOARCH, NumCPU: runtime.NumCPU()}
	if goos != "linux" && goos != "android" {
		return info
	}
	data, err := read("/proc/meminfo")
	if err != nil {
		return info
	}
	total, ok := parseMemInfoTotal(data)
	if !ok {
		return info
	}
	// Containers: honor cgroup v2, then v1, memory limits when lower.
	for _, p := range []string{"/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory/memory.limit_in_bytes"} {
		raw, err := read(p)
		if err != nil {
			continue
		}
		if limit, ok := parseCgroupLimit(raw); ok && limit < total {
			total = limit
		}
		break
	}
	info.TotalMemoryBytes = total
	info.MemoryKnown = true
	return info
}

// parseMemInfoTotal extracts MemTotal (reported in kB) from /proc/meminfo.
func parseMemInfoTotal(data []byte) (uint64, bool) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := sc.Text()
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(strings.TrimPrefix(line, "MemTotal:"))
		if len(fields) == 0 {
			return 0, false
		}
		kb, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil || kb == 0 {
			return 0, false
		}
		mult := uint64(1)
		if len(fields) > 1 && strings.EqualFold(fields[1], "kB") {
			mult = 1024
		}
		if kb > math.MaxUint64/mult {
			return 0, false
		}
		return kb * mult, true
	}
	return 0, false
}

// parseCgroupLimit parses a cgroup memory limit; "max" and absurdly large v1
// sentinel values mean "unlimited".
func parseCgroupLimit(data []byte) (uint64, bool) {
	s := strings.TrimSpace(string(data))
	if s == "" || s == "max" {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 || v >= 1<<62 {
		return 0, false
	}
	return v, true
}

// readFileLimited reads at most maxProcFileBytes from a fixed system path.
func readFileLimited(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, maxProcFileBytes))
}
