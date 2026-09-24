//go:build !darwin && !windows

package hardware

// nativeTotalMemory is not used on Linux (see detectSystemInfo) and is not
// implemented on other platforms.
func nativeTotalMemory() (uint64, bool) { return 0, false }
