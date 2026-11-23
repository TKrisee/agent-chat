//go:build windows

package codexruntime

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

var moveFileEx = syscall.NewLazyDLL("kernel32.dll").NewProc("MoveFileExW")

const moveFileReplaceExisting = 0x1
const moveFileWriteThrough = 0x8

func moveJournalFile(from, to string, flags uintptr) error {
	a, err := syscall.UTF16PtrFromString(from)
	if err != nil {
		return err
	}
	b, err := syscall.UTF16PtrFromString(to)
	if err != nil {
		return err
	}
	ok, _, e := moveFileEx.Call(uintptr(unsafe.Pointer(a)), uintptr(unsafe.Pointer(b)), flags|moveFileWriteThrough)
	if ok == 0 {
		return &os.LinkError{Op: "MoveFileEx", Old: from, New: to, Err: e}
	}
	return nil
}

// Flush contents before publishing each record with a write-through move.
func syncFile(path string, data []byte) error {
	f, err := os.CreateTemp(filepath.Dir(path), ".journal-*")
	if err != nil {
		return err
	}
	name := f.Name()
	defer os.Remove(name)
	if _, err = f.Write(data); err != nil {
		f.Close()
		return err
	}
	if err = f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err = f.Close(); err != nil {
		return err
	}
	return moveJournalFile(name, path, 0)
}

// Win32 does not support a POSIX-style fsync of a directory handle. File
// records are flushed and published by MoveFileEx; this is not a guarantee
// that every directory metadata update survives sudden power loss.
func syncDir(_ string) error { return nil }

// Rename the proof away before launching, so a process crash cannot expose it
// as retryable after the new child has started.
func consumeRetryMarker(path string) error {
	return moveJournalFile(path, path+".consumed", moveFileReplaceExisting)
}

func syncResultFile(path string) error {
	// FlushFileBuffers requires a handle opened for writing on Windows.
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}
