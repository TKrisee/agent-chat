//go:build !windows

package codexruntime

import (
	"fmt"
	"os/exec"
	"syscall"
)

func configureProcess(cmd *exec.Cmd)  { cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true} }
func killProcess(cmd *exec.Cmd) error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }

func containProcess(cmd *exec.Cmd) (func(), error) { return func() {}, nil }

func processFailureCode(cmd *exec.Cmd) string {
	if cmd.ProcessState != nil {
		if status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus); ok && status.Signaled() {
			return fmt.Sprintf("signal:%d", status.Signal())
		}
		if code := cmd.ProcessState.ExitCode(); code > 0 {
			return fmt.Sprintf("exit:%d", code)
		}
	}
	return "wait_failed"
}
