package codexruntime

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"agentchat/src/internal/runtimeapi"
)

func (r *Runner) StartSetup(ctx context.Context) {
	r.setupMu.Lock()
	if r.setupCtx != nil {
		r.setupMu.Unlock()
		return
	}
	r.setupCtx = ctx
	if r.setupState.Source != "managed" || r.setupState.State == "error" {
		r.setupMu.Unlock()
		return
	}
	r.setupMu.Unlock()
	r.beginSetup()
}
func (r *Runner) beginSetup() {
	r.setupMu.RLock()
	generation := r.setupGeneration
	if r.setupState.Source != "managed" {
		r.setupMu.RUnlock()
		return
	}
	r.setupMu.RUnlock()
	go func() {
		r.setupMu.RLock()
		ctx := r.setupCtx
		r.setupMu.RUnlock()
		if ctx == nil {
			return
		}
		ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
		defer cancel()
		path, version, err := resolveManaged(ctx, r.cfg.StateDir, "")
		r.setupMu.Lock()
		defer r.setupMu.Unlock()
		if r.setupGeneration != generation || r.setupState.Source != "managed" {
			return
		}
		if err != nil {
			r.setupState.State = "error"
			r.setupState.Message = setupError(err)
			return
		}
		r.setupPath = path
		r.setupState.Version = version
		r.setupState.Message = ""
		r.setupState.State = "ready"
	}()
}

// SelectSource never changes an already-running process. A generation prevents
// a prior managed download from overwriting a later selection.
func (r *Runner) SelectSource(source string) {
	r.setupMu.Lock()
	if r.setupState.Source == source {
		r.setupMu.Unlock()
		return
	}
	r.setupGeneration++
	r.setupState.Source = source
	r.setupState.Version = ""
	r.setupState.Message = ""
	r.setupPath = ""
	if source == "override" {
		if r.cfg.CodexPath == "" {
			r.setupState.State = "error"
			r.setupState.Message = "Configured Codex executable is unavailable."
		} else if _, err := exec.LookPath(r.cfg.CodexPath); err != nil {
			r.setupState.State = "error"
			r.setupState.Message = "Configured Codex executable is unavailable."
		} else {
			r.setupState.State = "ready"
			r.setupPath = r.cfg.CodexPath
		}
		r.setupMu.Unlock()
		return
	}
	if !r.setupState.CanSelectManaged {
		r.setupState.State = "error"
		r.setupState.Message = "This platform has no managed Codex package."
		r.setupMu.Unlock()
		return
	}
	r.setupState.State = "downloading"
	r.setupState.Version = r.setupState.ManagedVersion
	started := r.setupCtx != nil
	r.setupMu.Unlock()
	if started {
		r.beginSetup()
	}
}
func setupError(err error) string {
	s := err.Error()
	switch {
	case strings.Contains(s, "digest mismatch"):
		return "Codex download failed integrity verification."
	case strings.Contains(s, "version mismatch"):
		return "Codex package version did not match the pinned version."
	case strings.Contains(s, "incomplete"), strings.Contains(s, "entry"), strings.Contains(s, "archive"), strings.Contains(s, "expansion"):
		return "Codex package or cache is incomplete or invalid."
	case strings.Contains(s, "unsupported Codex platform"):
		return "This platform has no managed Codex package."
	default:
		return "Codex setup could not complete. Check connectivity and retry."
	}
}
func (r *Runner) SetupStatus() runtimeapi.CodexSetupStatus {
	r.setupMu.RLock()
	defer r.setupMu.RUnlock()
	return r.setupState
}
func (r *Runner) RetrySetup() bool {
	r.setupMu.Lock()
	if r.setupState.State != "error" || r.setupCtx == nil || r.setupCtx.Err() != nil {
		r.setupMu.Unlock()
		return false
	}
	if r.setupState.Source == "override" {
		if _, err := exec.LookPath(r.cfg.CodexPath); err != nil {
			r.setupMu.Unlock()
			return false
		}
		r.setupState.State = "ready"
		r.setupState.Message = ""
		r.setupPath = r.cfg.CodexPath
		r.setupMu.Unlock()
		return true
	}
	r.setupState.State = "downloading"
	r.setupState.Message = ""
	r.setupMu.Unlock()
	r.beginSetup()
	return true
}
func (r *Runner) readyExecutable() (string, error) {
	path, _, err := r.readyExecutableSource()
	return path, err
}
func (r *Runner) readyExecutableSource() (string, string, error) {
	r.setupMu.RLock()
	defer r.setupMu.RUnlock()
	if r.setupState.State != "ready" || r.setupPath == "" {
		return "", "", errors.New("Codex setup is not ready")
	}
	return r.setupPath, r.setupState.Source, nil
}

// ResolveCLI prepares the same managed package for explicit version/login commands.
func ResolveCLI(ctx context.Context, dataDir, override string) (string, error) {
	if override != "" {
		if _, err := exec.LookPath(override); err != nil {
			return "", err
		}
		return override, nil
	}
	dir, err := filepath.Abs(filepath.Join(dataDir, "codex"))
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(dir, 0700); err != nil {
		return "", err
	}
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || !privateStateDirectory(info) {
		return "", errors.New("Codex state directory must be private")
	}
	p, _, err := resolveManaged(ctx, dir, "")
	return p, err
}
func resolveManaged(ctx context.Context, stateDir, platform string) (string, string, error) {
	if platform == "" {
		platform = runtime.GOOS + "-" + runtime.GOARCH
	}
	b, err := lockedBundle(platform)
	if err != nil {
		return "", "", err
	}
	p, err := b.prepare(ctx, stateDir)
	return p, b.version, err
}
