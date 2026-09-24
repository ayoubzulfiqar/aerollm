package hardware

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

// Capability describes a detected hardware capability.
type Capability struct {
	Name      string
	Available bool
	Detail    string
}

// Detector discovers local compute capabilities.
type Detector interface {
	Detect() []Capability
}

// DefaultDetectCacheTTL is how long NewLocalDetector caches detection results.
const DefaultDetectCacheTTL = 30 * time.Second

// LocalDetector performs lightweight, offline hardware detection by probing
// well-known device nodes and driver libraries. It never executes external
// programs, opens network connections or reads user-controlled paths, and a
// missing /proc, /sys or /dev entry simply means "not detected".
//
// The zero value is usable (no caching, host OS, real filesystem).
type LocalDetector struct {
	goos     string
	arch     string
	stat     func(string) (os.FileInfo, error)
	getenv   func(string) string
	now      func() time.Time
	cacheTTL time.Duration

	mu       sync.Mutex
	cached   []Capability
	cachedAt time.Time
}

// NewLocalDetector creates a hardware detector that caches results for
// DefaultDetectCacheTTL.
func NewLocalDetector() *LocalDetector {
	return &LocalDetector{
		goos:     runtime.GOOS,
		arch:     runtime.GOARCH,
		stat:     os.Stat,
		getenv:   os.Getenv,
		now:      time.Now,
		cacheTTL: DefaultDetectCacheTTL,
	}
}

// Detect returns currently detectable compute capabilities in a stable order:
// cuda, metal, rocm, vulkan, ollama, cpu. The returned slice is a copy.
func (d *LocalDetector) Detect() []Capability {
	if d == nil {
		return (&LocalDetector{}).detectAll()
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := d.nowFn()()
	if d.cached != nil && d.cacheTTL > 0 && now.Sub(d.cachedAt) < d.cacheTTL && !now.Before(d.cachedAt) {
		return cloneCaps(d.cached)
	}
	caps := d.detectAll()
	d.cached = caps
	d.cachedAt = now
	return cloneCaps(caps)
}

// Refresh discards cached results and re-runs detection.
func (d *LocalDetector) Refresh() []Capability {
	if d == nil {
		return (&LocalDetector{}).detectAll()
	}
	d.mu.Lock()
	d.cached = nil
	d.mu.Unlock()
	return d.Detect()
}

func (d *LocalDetector) detectAll() []Capability {
	return []Capability{
		d.detectCUDA(),
		d.detectMetal(),
		d.detectROCm(),
		d.detectVulkan(),
		d.detectOllama(),
		d.detectCPU(),
	}
}

func cloneCaps(in []Capability) []Capability {
	out := make([]Capability, len(in))
	copy(out, in)
	return out
}

func (d *LocalDetector) osName() string {
	if d.goos != "" {
		return d.goos
	}
	return runtime.GOOS
}

func (d *LocalDetector) archName() string {
	if d.arch != "" {
		return d.arch
	}
	return runtime.GOARCH
}

func (d *LocalDetector) statFn() func(string) (os.FileInfo, error) {
	if d.stat != nil {
		return d.stat
	}
	return os.Stat
}

func (d *LocalDetector) env(key string) string {
	if d.getenv != nil {
		return d.getenv(key)
	}
	return os.Getenv(key)
}

func (d *LocalDetector) nowFn() func() time.Time {
	if d.now != nil {
		return d.now
	}
	return time.Now
}

// firstExisting returns the first path that exists.
func (d *LocalDetector) firstExisting(paths ...string) (string, bool) {
	stat := d.statFn()
	for _, p := range paths {
		if p == "" {
			continue
		}
		if _, err := stat(p); err == nil {
			return p, true
		}
	}
	return "", false
}

// linuxLibDirs are the multiarch/library directories probed on Linux-like
// systems (x86_64, aarch64, RPM-style lib64, Arch, WSL2).
var linuxLibDirs = []string{
	"/usr/lib/x86_64-linux-gnu",
	"/usr/lib/aarch64-linux-gnu",
	"/usr/lib64",
	"/usr/lib",
	"/lib/x86_64-linux-gnu",
	"/lib/aarch64-linux-gnu",
	"/usr/lib/wsl/lib",
}

func libPaths(names ...string) []string {
	out := make([]string, 0, len(linuxLibDirs)*len(names))
	for _, dir := range linuxLibDirs {
		for _, n := range names {
			out = append(out, dir+"/"+n)
		}
	}
	return out
}

// windowsSystem32 returns the System32 directory without shelling out.
func (d *LocalDetector) windowsSystem32() string {
	root := strings.TrimSpace(d.env("SystemRoot"))
	if root == "" {
		root = strings.TrimSpace(d.env("windir"))
	}
	if root == "" {
		root = `C:\Windows`
	}
	return filepath.Join(root, "System32")
}

func (d *LocalDetector) detectCUDA() Capability {
	switch d.osName() {
	case "windows":
		if p, ok := d.firstExisting(filepath.Join(d.windowsSystem32(), "nvcuda.dll")); ok {
			return Capability{Name: "cuda", Available: true, Detail: p}
		}
	case "darwin":
		// NVIDIA dropped macOS CUDA support; never report it available.
	default:
		// Driver evidence only: device nodes, the loaded kernel module, or the
		// driver-installed libcuda (the toolkit alone does not imply a GPU).
		paths := []string{
			"/dev/nvidia0",
			"/proc/driver/nvidia/version",
			"/dev/nvhost-ctrl-gpu", // Jetson / Tegra
		}
		paths = append(paths, libPaths("libcuda.so.1", "libcuda.so")...)
		paths = append(paths, "/usr/lib/aarch64-linux-gnu/tegra/libcuda.so")
		if p, ok := d.firstExisting(paths...); ok {
			return Capability{Name: "cuda", Available: true, Detail: p}
		}
	}
	return Capability{Name: "cuda", Available: false}
}

func (d *LocalDetector) detectMetal() Capability {
	if d.osName() != "darwin" {
		return Capability{Name: "metal", Available: false}
	}
	if p, ok := d.firstExisting("/System/Library/Frameworks/Metal.framework"); ok {
		detail := p
		if d.archName() == "arm64" {
			detail = "apple-silicon," + p
		}
		return Capability{Name: "metal", Available: true, Detail: detail}
	}
	return Capability{Name: "metal", Available: false}
}

func (d *LocalDetector) detectROCm() Capability {
	switch d.osName() {
	case "windows":
		sys := d.windowsSystem32()
		if p, ok := d.firstExisting(filepath.Join(sys, "amdhip64.dll"), filepath.Join(sys, "amdhip64_6.dll")); ok {
			return Capability{Name: "rocm", Available: true, Detail: p}
		}
	case "darwin":
	default:
		// /dev/kfd is the AMD compute kernel driver; without it the ROCm
		// userland (common in container images) cannot run anything.
		if p, ok := d.firstExisting("/dev/kfd"); ok {
			return Capability{Name: "rocm", Available: true, Detail: p}
		}
		runtimePaths := []string{"/opt/rocm"}
		runtimePaths = append(runtimePaths, libPaths("libamdhip64.so", "librocr.so", "libhsa-runtime64.so.1")...)
		if p, ok := d.firstExisting(runtimePaths...); ok {
			return Capability{Name: "rocm", Available: false, Detail: "runtime at " + p + " but /dev/kfd missing"}
		}
	}
	return Capability{Name: "rocm", Available: false}
}

func (d *LocalDetector) detectVulkan() Capability {
	var paths []string
	switch d.osName() {
	case "windows":
		paths = []string{filepath.Join(d.windowsSystem32(), "vulkan-1.dll")}
	case "darwin":
		paths = []string{
			"/opt/homebrew/lib/libMoltenVK.dylib",
			"/usr/local/lib/libMoltenVK.dylib",
			"/opt/homebrew/lib/libvulkan.1.dylib",
			"/usr/local/lib/libvulkan.1.dylib",
		}
	default:
		paths = append(libPaths("libvulkan.so.1", "libvulkan.so"), "/system/lib64/libvulkan.so")
	}
	if p, ok := d.firstExisting(paths...); ok {
		return Capability{Name: "vulkan", Available: true, Detail: p}
	}
	return Capability{Name: "vulkan", Available: false}
}

func (d *LocalDetector) detectOllama() Capability {
	// Offline, opt-in only: Detect never starts processes or probes the
	// network, so an Ollama daemon is only reported when explicitly enabled.
	if strings.EqualFold(strings.TrimSpace(d.env("AEROLLM_OLLAMA_ENABLED")), "true") {
		return Capability{Name: "ollama", Available: true, Detail: "env"}
	}
	if strings.TrimSpace(d.env("OLLAMA_HOST")) != "" {
		return Capability{Name: "ollama", Available: false, Detail: "OLLAMA_HOST set; not probed (set AEROLLM_OLLAMA_ENABLED=true)"}
	}
	return Capability{Name: "ollama", Available: false}
}

func (d *LocalDetector) detectCPU() Capability {
	return Capability{
		Name:      "cpu",
		Available: true,
		Detail:    d.archName() + "/" + d.osName(),
	}
}

// AdvertisedCapabilities converts detected capabilities into mesh metadata.
func AdvertisedCapabilities(caps []Capability) map[string]string {
	out := map[string]string{}
	for _, c := range caps {
		if c.Name == "" {
			continue
		}
		val := "false"
		if c.Available {
			val = "true"
		}
		if c.Detail != "" {
			val = val + "," + c.Detail
		}
		out["has_"+c.Name] = val
	}
	return out
}
