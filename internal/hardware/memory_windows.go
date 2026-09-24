//go:build windows

package hardware

import (
	"syscall"
	"unsafe"
)

// memoryStatusEx mirrors the Win32 MEMORYSTATUSEX structure.
type memoryStatusEx struct {
	Length               uint32
	MemoryLoad           uint32
	TotalPhys            uint64
	AvailPhys            uint64
	TotalPageFile        uint64
	AvailPageFile        uint64
	TotalVirtual         uint64
	AvailVirtual         uint64
	AvailExtendedVirtual uint64
}

// kernel32 is a KnownDLL, so loading it by name cannot be hijacked through
// the DLL search path.
var procGlobalMemoryStatusEx = syscall.NewLazyDLL("kernel32.dll").NewProc("GlobalMemoryStatusEx")

// nativeTotalMemory returns physical memory from GlobalMemoryStatusEx.
func nativeTotalMemory() (uint64, bool) {
	if err := procGlobalMemoryStatusEx.Find(); err != nil {
		return 0, false
	}
	var ms memoryStatusEx
	ms.Length = uint32(unsafe.Sizeof(ms))
	ok, _, _ := procGlobalMemoryStatusEx.Call(uintptr(unsafe.Pointer(&ms)))
	if ok == 0 || ms.TotalPhys == 0 {
		return 0, false
	}
	return ms.TotalPhys, true
}
