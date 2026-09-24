package hardware

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/aerollm/internal/intelligence"
)

func TestLocalDetectorReturnsCPUAlways(t *testing.T) {
	d := NewLocalDetector()
	caps := d.Detect()
	found := false
	for _, c := range caps {
		if c.Name == "cpu" && c.Available {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected cpu capability, got %+v", caps)
	}
}

func TestHardwareAwareSelectorFallsBack(t *testing.T) {
	sel := NewHardwareAwareSelector(intelligence.NewHeuristicSelector(), NewLocalDetector())
	_, err := sel.Select(nil, []intelligence.ModelOption{{Provider: "a", Model: "m", Cost: 0, Latency: 0, Quality: 0.1}}, intelligence.Policy{MinQuality: 0}) //nolint:staticcheck // nil ctx must be tolerated
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// fakeFS answers stat calls from a fixed set of existing paths.
type fakeFS struct {
	mu    sync.Mutex
	paths map[string]bool
	calls int
}

func newFakeFS(paths ...string) *fakeFS {
	f := &fakeFS{paths: map[string]bool{}}
	for _, p := range paths {
		f.paths[p] = true
	}
	return f
}

func (f *fakeFS) stat(p string) (os.FileInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.paths[p] {
		return nil, nil
	}
	return nil, fs.ErrNotExist
}

func testDetector(goos, arch string, f *fakeFS, env map[string]string) *LocalDetector {
	return &LocalDetector{
		goos:   goos,
		arch:   arch,
		stat:   f.stat,
		getenv: func(k string) string { return env[k] },
	}
}

func capByName(caps []Capability, name string) Capability {
	for _, c := range caps {
		if c.Name == name {
			return c
		}
	}
	return Capability{}
}

func TestDetectorNothingPresent(t *testing.T) {
	for _, goos := range []string{"linux", "darwin", "windows", "freebsd"} {
		caps := testDetector(goos, "amd64", newFakeFS(), nil).Detect()
		if len(caps) != 6 {
			t.Fatalf("%s: expected 6 capabilities, got %d", goos, len(caps))
		}
		for _, c := range caps {
			if c.Name != "cpu" && c.Available {
				t.Fatalf("%s: %s unexpectedly available", goos, c.Name)
			}
		}
		if cpu := capByName(caps, "cpu"); !cpu.Available || cpu.Detail != "amd64/"+goos {
			t.Fatalf("%s: unexpected cpu capability %+v", goos, cpu)
		}
	}
}

func TestDetectorLinuxAarch64CUDAAndVulkan(t *testing.T) {
	f := newFakeFS("/usr/lib/aarch64-linux-gnu/libcuda.so.1", "/usr/lib/aarch64-linux-gnu/libvulkan.so.1")
	caps := testDetector("linux", "arm64", f, nil).Detect()
	if c := capByName(caps, "cuda"); !c.Available || c.Detail != "/usr/lib/aarch64-linux-gnu/libcuda.so.1" {
		t.Fatalf("unexpected cuda: %+v", c)
	}
	if c := capByName(caps, "vulkan"); !c.Available {
		t.Fatalf("unexpected vulkan: %+v", c)
	}
}

func TestDetectorLinuxNvidiaDeviceNode(t *testing.T) {
	caps := testDetector("linux", "amd64", newFakeFS("/proc/driver/nvidia/version"), nil).Detect()
	if c := capByName(caps, "cuda"); !c.Available {
		t.Fatalf("expected cuda from driver proc entry: %+v", c)
	}
	// The CUDA toolkit alone does not imply a GPU.
	caps = testDetector("linux", "amd64", newFakeFS("/usr/local/cuda"), nil).Detect()
	if c := capByName(caps, "cuda"); c.Available {
		t.Fatalf("toolkit dir must not imply cuda: %+v", c)
	}
}

func TestDetectorROCmRequiresKFD(t *testing.T) {
	caps := testDetector("linux", "amd64", newFakeFS("/opt/rocm"), nil).Detect()
	c := capByName(caps, "rocm")
	if c.Available || !strings.Contains(c.Detail, "/dev/kfd missing") {
		t.Fatalf("expected runtime-without-device detail, got %+v", c)
	}
	caps = testDetector("linux", "amd64", newFakeFS("/dev/kfd", "/opt/rocm"), nil).Detect()
	if c := capByName(caps, "rocm"); !c.Available || c.Detail != "/dev/kfd" {
		t.Fatalf("expected rocm available, got %+v", c)
	}
}

func TestDetectorWindows(t *testing.T) {
	f := &fakeFS{paths: map[string]bool{}}
	stat := func(p string) (os.FileInfo, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.calls++
		if strings.HasPrefix(p, `D:\Win`) && (strings.HasSuffix(p, "nvcuda.dll") || strings.HasSuffix(p, "vulkan-1.dll")) {
			return nil, nil
		}
		return nil, fs.ErrNotExist
	}
	d := &LocalDetector{goos: "windows", arch: "amd64", stat: stat, getenv: func(k string) string {
		if k == "SystemRoot" {
			return `D:\Win`
		}
		return ""
	}}
	caps := d.Detect()
	if c := capByName(caps, "cuda"); !c.Available || !strings.HasSuffix(c.Detail, "nvcuda.dll") {
		t.Fatalf("unexpected cuda: %+v", c)
	}
	if c := capByName(caps, "vulkan"); !c.Available {
		t.Fatalf("unexpected vulkan: %+v", c)
	}
	if c := capByName(caps, "rocm"); c.Available {
		t.Fatalf("unexpected rocm: %+v", c)
	}
	// Missing SystemRoot falls back to C:\Windows without panicking.
	d.getenv = func(string) string { return "" }
	if got := d.windowsSystem32(); !strings.HasPrefix(got, `C:\Windows`) {
		t.Fatalf("unexpected fallback system32: %s", got)
	}
}

func TestDetectorDarwinMetal(t *testing.T) {
	caps := testDetector("darwin", "arm64", newFakeFS("/System/Library/Frameworks/Metal.framework"), nil).Detect()
	if c := capByName(caps, "metal"); !c.Available || !strings.HasPrefix(c.Detail, "apple-silicon") {
		t.Fatalf("unexpected metal: %+v", c)
	}
	if c := capByName(caps, "cuda"); c.Available {
		t.Fatal("cuda must never be reported on darwin")
	}
	caps = testDetector("linux", "amd64", newFakeFS("/System/Library/Frameworks/Metal.framework"), nil).Detect()
	if c := capByName(caps, "metal"); c.Available {
		t.Fatal("metal must only be reported on darwin")
	}
}

func TestDetectorOllamaOptIn(t *testing.T) {
	caps := testDetector("linux", "amd64", newFakeFS(), map[string]string{"AEROLLM_OLLAMA_ENABLED": " TRUE "}).Detect()
	if c := capByName(caps, "ollama"); !c.Available {
		t.Fatalf("expected ollama enabled: %+v", c)
	}
	caps = testDetector("linux", "amd64", newFakeFS(), map[string]string{"OLLAMA_HOST": "http://secret-host:11434"}).Detect()
	c := capByName(caps, "ollama")
	if c.Available || strings.Contains(c.Detail, "secret-host") {
		t.Fatalf("OLLAMA_HOST alone must not enable or leak: %+v", c)
	}
}

func TestDetectorCaching(t *testing.T) {
	f := newFakeFS()
	now := time.Unix(1000, 0)
	d := testDetector("linux", "amd64", f, nil)
	d.cacheTTL = time.Minute
	d.now = func() time.Time { return now }

	first := d.Detect()
	calls := f.calls
	first[0].Name = "mutated"
	second := d.Detect()
	if f.calls != calls {
		t.Fatal("expected cached result within TTL")
	}
	if second[0].Name != "cuda" {
		t.Fatal("cached slice was mutated through a returned copy")
	}
	now = now.Add(2 * time.Minute)
	d.Detect()
	if f.calls == calls {
		t.Fatal("expected re-detection after TTL")
	}
	calls = f.calls
	d.Refresh()
	if f.calls == calls {
		t.Fatal("expected Refresh to re-detect")
	}
}

func TestDetectorZeroValueAndNil(t *testing.T) {
	var zero LocalDetector
	if len(zero.Detect()) != 6 {
		t.Fatal("zero-value detector should work")
	}
	var nilDet *LocalDetector
	if len(nilDet.Detect()) != 6 || len(nilDet.Refresh()) != 6 {
		t.Fatal("nil detector should not panic")
	}
}

func TestDetectorConcurrent(t *testing.T) {
	d := NewLocalDetector()
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = d.Detect()
			_ = d.Refresh()
		}()
	}
	wg.Wait()
}

func TestAdvertisedCapabilities(t *testing.T) {
	got := AdvertisedCapabilities([]Capability{
		{Name: "cuda", Available: true, Detail: "/dev/nvidia0"},
		{Name: "rocm"},
		{Name: ""},
	})
	if got["has_cuda"] != "true,/dev/nvidia0" || got["has_rocm"] != "false" || len(got) != 2 {
		t.Fatalf("unexpected advertised capabilities: %v", got)
	}
}

// fakeDetector returns a fixed capability set.
type fakeDetector struct{ caps []Capability }

func (f fakeDetector) Detect() []Capability { return f.caps }

type fakeBase struct {
	opt   intelligence.ModelOption
	err   error
	calls int
}

func (f *fakeBase) Select(context.Context, []intelligence.ModelOption, intelligence.Policy) (intelligence.ModelOption, error) {
	f.calls++
	return f.opt, f.err
}

var (
	gpuCaps = []Capability{{Name: "vulkan", Available: true}, {Name: "cuda", Available: true}, {Name: "cpu", Available: true}}
	cpuOnly = []Capability{{Name: "cuda"}, {Name: "cpu", Available: true}}
	remote  = intelligence.ModelOption{Provider: "openai", Model: "gpt", Quality: 0.9}
)

func TestSelectorZeroPolicyUsesBaseEvenWithGPU(t *testing.T) {
	// Regression: MaxLatencyMs == 0 (unset) used to force local routing.
	base := &fakeBase{opt: remote}
	sel := NewHardwareAwareSelector(base, fakeDetector{gpuCaps})
	got, err := sel.Select(context.Background(), []intelligence.ModelOption{remote}, intelligence.Policy{})
	if err != nil || got != remote || base.calls != 1 {
		t.Fatalf("expected remote selection, got %+v %v", got, err)
	}
}

func TestSelectorForceLocal(t *testing.T) {
	base := &fakeBase{opt: remote}
	sel := &HardwareAwareSelector{Base: base, Detector: fakeDetector{gpuCaps}, ForceLocal: true, LocalModel: "llama3", LocalProvider: "ollama"}
	got, err := sel.Select(context.Background(), nil, intelligence.Policy{MinQuality: 0.99})
	if err != nil || got.Provider != "ollama" || got.Model != "llama3" || got.Quality != DefaultLocalQuality || base.calls != 0 {
		t.Fatalf("unexpected: %+v %v (base calls %d)", got, err, base.calls)
	}

	sel.Detector = fakeDetector{cpuOnly}
	if _, err := sel.Select(context.Background(), nil, intelligence.Policy{}); !errors.Is(err, ErrNoLocalHardware) {
		t.Fatalf("expected ErrNoLocalHardware (never fall back to remote), got %v", err)
	}
	if base.calls != 0 {
		t.Fatal("ForceLocal must not consult the remote selector")
	}
	sel.AllowCPU = true
	if got, err := sel.Select(context.Background(), nil, intelligence.Policy{}); err != nil || got.Model != "llama3" {
		t.Fatalf("expected cpu local target with AllowCPU: %+v %v", got, err)
	}
}

func TestSelectorPreferLocalRespectsPolicy(t *testing.T) {
	base := &fakeBase{opt: remote}
	sel := &HardwareAwareSelector{Base: base, Detector: fakeDetector{gpuCaps}, PreferLocal: true}
	got, err := sel.Select(context.Background(), nil, intelligence.Policy{MinQuality: 0.4})
	if err != nil || got.Provider != "edge" || got.Model != "cuda" {
		t.Fatalf("expected local cuda (priority over vulkan), got %+v %v", got, err)
	}
	got, err = sel.Select(context.Background(), nil, intelligence.Policy{MinQuality: 0.8})
	if err != nil || got != remote {
		t.Fatalf("local quality below MinQuality must fall back to base, got %+v %v", got, err)
	}
}

func TestSelectorBaseFailureFallsBackToLocal(t *testing.T) {
	base := &fakeBase{err: errors.New("no remote options")}
	sel := &HardwareAwareSelector{Base: base, Detector: fakeDetector{gpuCaps}}
	if got, err := sel.Select(context.Background(), nil, intelligence.Policy{}); err != nil || got.Model != "cuda" {
		t.Fatalf("expected local fallback, got %+v %v", got, err)
	}
	sel.Detector = fakeDetector{cpuOnly}
	if _, err := sel.Select(context.Background(), nil, intelligence.Policy{}); err == nil || err.Error() != "no remote options" {
		t.Fatalf("expected base error, got %v", err)
	}
}

func TestSelectorNoBase(t *testing.T) {
	sel := &HardwareAwareSelector{Detector: fakeDetector{cpuOnly}}
	if _, err := sel.Select(context.Background(), nil, intelligence.Policy{}); !errors.Is(err, ErrNoLocalHardware) {
		t.Fatalf("expected ErrNoLocalHardware, got %v", err)
	}
	sel = &HardwareAwareSelector{} // nil detector
	if _, err := sel.Select(context.Background(), nil, intelligence.Policy{}); err == nil {
		t.Fatal("expected error with nil detector and nil base")
	}
	var nilSel *HardwareAwareSelector
	if _, err := nilSel.Select(context.Background(), nil, intelligence.Policy{}); err == nil {
		t.Fatal("expected error for nil selector")
	}
}

func TestSelectorCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	sel := &HardwareAwareSelector{Base: &fakeBase{opt: remote}, Detector: fakeDetector{gpuCaps}, ForceLocal: true}
	if _, err := sel.Select(ctx, nil, intelligence.Policy{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestSelectorCustomAcceleratorAndQuality(t *testing.T) {
	sel := &HardwareAwareSelector{Detector: fakeDetector{[]Capability{{Name: "npu", Available: true}}}, PreferLocal: true, LocalQuality: 0.9}
	got, err := sel.Select(context.Background(), nil, intelligence.Policy{MinQuality: 0.85})
	if err != nil || got.Model != "npu" || got.Quality != 0.9 {
		t.Fatalf("unexpected: %+v %v", got, err)
	}
	sel.LocalQuality = 7 // invalid → default
	if got, _ := sel.Select(context.Background(), nil, intelligence.Policy{}); got.Quality != DefaultLocalQuality {
		t.Fatalf("invalid LocalQuality not defaulted: %v", got.Quality)
	}
}
