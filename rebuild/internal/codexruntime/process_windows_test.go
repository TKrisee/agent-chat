//go:build windows

package codexruntime

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestWindowsContainmentHelper(t *testing.T) {
	path := os.Getenv("AGENTCHAT_SUSPENDED_MARKER")
	if path == "" {
		return
	}
	if err := os.WriteFile(path, []byte("started"), 0600); err != nil {
		t.Fatal(err)
	}
}

func TestWindowsProcessStartsOnlyAfterContainment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	marker := filepath.Join(t.TempDir(), "started")
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestWindowsContainmentHelper$")
	cmd.Env = append(os.Environ(), "AGENTCHAT_SUSPENDED_MARKER="+marker)
	configureProcess(cmd)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := false
	defer func() {
		if !done {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	}()
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(marker); !os.IsNotExist(err) {
		t.Fatalf("suspended process ran before assignment: %v", err)
	}
	cleanup, err := containProcess(cmd)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	err = cmd.Wait()
	done = true
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("resumed process did not run: %v", err)
	}
}
