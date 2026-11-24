//go:build windows

package platformlock

import (
	"errors"
	"os"
	"syscall"
	"unsafe"
)

var kernel = syscall.NewLazyDLL("kernel32.dll")
var lockFileEx = kernel.NewProc("LockFileEx")
var unlockFileEx = kernel.NewProc("UnlockFileEx")

func Lock(f *os.File, wait bool) error {
	flags := uintptr(2)
	if !wait {
		flags |= 1
	}
	var o syscall.Overlapped
	r, _, e := lockFileEx.Call(f.Fd(), flags, 0, 1, 0, uintptr(unsafe.Pointer(&o)))
	if r == 0 {
		return e
	}
	return nil
}
func Unlock(f *os.File) error {
	var o syscall.Overlapped
	r, _, e := unlockFileEx.Call(f.Fd(), 0, 1, 0, uintptr(unsafe.Pointer(&o)))
	if r == 0 {
		return e
	}
	return nil
}
func IsContended(err error) bool { return errors.Is(err, syscall.Errno(33)) }
