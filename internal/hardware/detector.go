package hardware

import (
	"os"
	"runtime"
	"strings"
)

// Capability describes a detected hardware capability.
type Capability struct {
	Name         string
	Available    bool
	Detail       string
}

// Detector discovers local compute capabilities.
type Detector interface {
	Detect() []Capability
}

// LocalDetector performs lightweight OS-level hardware detection.
// It avoids heavy dependencies and keeps edge-node startup fast.
type LocalDetector struct{}

// NewLocalDetector creates a hardware detector.
func NewLocalDetector() *LocalDetector {
	return &LocalDetector{}
}

// Detect returns currently detectable compute capabilities.
// Extend each helper when CGO or vendor tooling is available.
func (d *LocalDetector) Detect() []Capability {
	return []Capability{
		d.detectCUDA(),
		d.detectMetal(),
		d.detectROCm(),
		d.detectVulkan(),
		d.detectOllama(),
		d.detectCPU(),
	}
}

func (d *LocalDetector) detectCUDA() Capability {
	// Check common CUDA paths without invoking nvidia-smi.
	paths := []string{
		"/usr/local/cuda",
		"/usr/lib/x86_64-linux-gnu/libcuda.so",
		"/usr/lib/x86_64-linux-gnu/libcuda.so.1",
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return Capability{Name: "cuda", Available: true, Detail: p}
		}
	}
	return Capability{Name: "cuda", Available: false}
}

func (d *LocalDetector) detectMetal() Capability {
	if runtime.GOOS == "darwin" {
		return Capability{Name: "metal", Available: true, Detail: "darwin"}
	}
	return Capability{Name: "metal", Available: false}
}

func (d *LocalDetector) detectROCm() Capability {
	paths := []string{
		"/opt/rocm",
		"/usr/lib/x86_64-linux-gnu/librocr.so",
		"/usr/lib64/librocr.so",
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return Capability{Name: "rocm", Available: true, Detail: p}
		}
	}
	return Capability{Name: "rocm", Available: false}
}

func (d *LocalDetector) detectVulkan() Capability {
	paths := []string{
		"/usr/lib/x86_64-linux-gnu/libvulkan.so",
		"/usr/lib64/libvulkan.so",
		"/system/lib64/libvulkan.so",
	}
	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			return Capability{Name: "vulkan", Available: true, Detail: p}
		}
	}
	return Capability{Name: "vulkan", Available: false}
}

func (d *LocalDetector) detectOllama() Capability {
	// Best-effort detection: check environment or common runtime paths.
	// Do not start processes here to keep edge-node lightweight.
	if strings.EqualFold(os.Getenv("AEROLLM_OLLAMA_ENABLED"), "true") {
		return Capability{Name: "ollama", Available: true, Detail: "env"}
	}
	return Capability{Name: "ollama", Available: false}
}

func (d *LocalDetector) detectCPU() Capability {
	return Capability{
		Name:      "cpu",
		Available: true,
		Detail:    runtime.GOARCH + "/" + runtime.GOOS,
	}
}

// AdvertisedCapabilities converts detected capabilities into mesh metadata.
func AdvertisedCapabilities(caps []Capability) map[string]string {
	out := map[string]string{}
	for _, c := range caps {
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
