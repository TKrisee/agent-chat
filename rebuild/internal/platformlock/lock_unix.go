//go:build !windows

package platformlock

import (
	"errors"
	"os"
	"syscall"
)

func Lock(f *os.File, wait bool) error {
	mode := syscall.LOCK_EX
	if !wait {
		mode |= syscall.LOCK_NB
	}
	return syscall.Flock(int(f.Fd()), mode)
}
func Unlock(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }
func IsContended(err error) bool {
	return errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN)
}
