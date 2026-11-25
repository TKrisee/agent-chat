//go:build windows

package codexruntime

import (
	"fmt"
	"os/exec"
	"sync"
	"syscall"
	"unsafe"
)

var k32 = syscall.NewLazyDLL("kernel32.dll")
var createJob = k32.NewProc("CreateJobObjectW")
var assignJob = k32.NewProc("AssignProcessToJobObject")
var terminateJob = k32.NewProc("TerminateJobObject")
var setJob = k32.NewProc("SetInformationJobObject")
var createSnapshot = k32.NewProc("CreateToolhelp32Snapshot")
var threadFirst = k32.NewProc("Thread32First")
var threadNext = k32.NewProc("Thread32Next")
var openThread = k32.NewProc("OpenThread")
var threadProcessID = k32.NewProc("GetProcessIdOfThread")
var resumeThread = k32.NewProc("ResumeThread")
var jobs sync.Map

// JOBOBJECT_EXTENDED_LIMIT_INFORMATION: the fixed-width fields and pointer-sized fields follow Windows ABI.
type basicLimits struct {
	PerProcessUserTimeLimit int64
	PerJobUserTimeLimit     int64
	LimitFlags              uint32
	MinimumWorkingSetSize   uintptr
	MaximumWorkingSetSize   uintptr
	ActiveProcessLimit      uint32
	Affinity                uintptr
	PriorityClass           uint32
	SchedulingClass         uint32
}
type ioCounters struct{ ReadOperationCount, WriteOperationCount, OtherOperationCount, ReadTransferCount, WriteTransferCount, OtherTransferCount uint64 }
type extendedLimits struct {
	BasicLimitInformation                                                        basicLimits
	IoInfo                                                                       ioCounters
	ProcessMemoryLimit, JobMemoryLimit, PeakProcessMemoryUsed, PeakJobMemoryUsed uintptr
}

// Create the process suspended so no user code can execute or spawn children
// before the kill-on-close job owns it.
func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x00000004}
}

type threadEntry struct {
	Size           uint32
	Usage          uint32
	ThreadID       uint32
	OwnerProcessID uint32
	BasePriority   int32
	DeltaPriority  int32
	Flags          uint32
}

// The suspended process must have exactly one thread. Recheck the owner after
// opening the thread handle so a recycled thread ID cannot resume another process.
func initialThread(pid uint32) (syscall.Handle, error) {
	snapshot, _, err := createSnapshot.Call(0x00000004, 0)
	if snapshot == ^uintptr(0) {
		return 0, err
	}
	defer syscall.CloseHandle(syscall.Handle(snapshot))
	entry := threadEntry{Size: uint32(unsafe.Sizeof(threadEntry{}))}
	var id uint32
	r, _, e := threadFirst.Call(snapshot, uintptr(unsafe.Pointer(&entry)))
	for r != 0 {
		if entry.OwnerProcessID == pid {
			if id != 0 {
				return 0, fmt.Errorf("suspended process has multiple threads")
			}
			id = entry.ThreadID
		}
		entry.Size = uint32(unsafe.Sizeof(threadEntry{}))
		r, _, e = threadNext.Call(snapshot, uintptr(unsafe.Pointer(&entry)))
	}
	if e != syscall.Errno(18) {
		return 0, e
	} // ERROR_NO_MORE_FILES
	if id == 0 {
		return 0, fmt.Errorf("suspended process thread not found")
	}
	h, _, e := openThread.Call(0x0002|0x0800, 0, uintptr(id))
	if h == 0 {
		return 0, e
	}
	owner, _, e := threadProcessID.Call(h)
	if owner == 0 || uint32(owner) != pid {
		syscall.CloseHandle(syscall.Handle(h))
		if owner == 0 {
			return 0, e
		}
		return 0, fmt.Errorf("thread owner changed")
	}
	return syscall.Handle(h), nil
}
func containProcess(cmd *exec.Cmd) (func(), error) {
	h, _, err := createJob.Call(0, 0)
	if h == 0 {
		return nil, err
	}
	closeJob := func() { syscall.CloseHandle(syscall.Handle(h)) }
	info := extendedLimits{}
	info.BasicLimitInformation.LimitFlags = 0x2000
	if r, _, e := setJob.Call(h, 9, uintptr(unsafe.Pointer(&info)), unsafe.Sizeof(info)); r == 0 {
		closeJob()
		return nil, e
	}
	p, e := syscall.OpenProcess(0x0100|0x0001, false, uint32(cmd.Process.Pid))
	if e != nil {
		closeJob()
		return nil, e
	}
	defer syscall.CloseHandle(p)
	if r, _, e := assignJob.Call(h, uintptr(p)); r == 0 {
		closeJob()
		return nil, e
	}
	thread, e := initialThread(uint32(cmd.Process.Pid))
	if e != nil {
		closeJob()
		return nil, e
	}
	defer syscall.CloseHandle(thread)
	previous, _, e := resumeThread.Call(uintptr(thread))
	if previous == ^uintptr(0)&0xffffffff {
		closeJob()
		return nil, e
	}
	if previous != 1 {
		closeJob()
		return nil, fmt.Errorf("unexpected initial thread suspend count: %d", previous)
	}
	jobs.Store(cmd, h)
	return func() { jobs.Delete(cmd); closeJob() }, nil
}
func killProcess(cmd *exec.Cmd) error {
	if h, ok := jobs.Load(cmd); ok {
		r, _, e := terminateJob.Call(h.(uintptr), 1)
		if r == 0 {
			return e
		}
		return nil
	}
	return cmd.Process.Kill()
}

func processFailureCode(cmd *exec.Cmd) string {
	if cmd.ProcessState != nil {
		if code := cmd.ProcessState.ExitCode(); code > 0 {
			return fmt.Sprintf("exit:%d", code)
		}
	}
	return "wait_failed"
}
