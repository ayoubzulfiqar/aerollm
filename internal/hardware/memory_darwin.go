//go:build darwin

package hardware

import "syscall"

// nativeTotalMemory returns physical memory from sysctl hw.memsize (a
// little-endian uint64 on every Apple platform Go supports).
func nativeTotalMemory() (uint64, bool) {
	raw, err := syscall.Sysctl("hw.memsize")
	if err != nil {
		return 0, false
	}
	total, ok := decodeLittleEndianUint64(raw)
	if !ok || total == 0 {
		return 0, false
	}
	return total, true
}
