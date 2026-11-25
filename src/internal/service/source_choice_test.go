package service

import (
	"context"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"agentchat/src/internal/codexruntime"
)

func TestCodexSourceChoicePersistenceAndSafety(t *testing.T) {
	if !(runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" || runtime.GOOS == "linux" && runtime.GOARCH == "amd64" || runtime.GOOS == "windows" && runtime.GOARCH == "amd64") {
		t.Skip("managed package unavailable")
	}
	rootDir := t.TempDir()
	runtimeDir := filepath.Join(rootDir, "codex")
	if err := os.Mkdir(runtimeDir, 0700); err != nil {
		t.Fatal(err)
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	newRunner := func() *codexruntime.Runner {
		r, err := codexruntime.New(codexruntime.Config{StateDir: runtimeDir, CodexPath: exe, JQPath: "jq"})
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	s := openTest(t, filepath.Join(rootDir, "data"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := newRunner()
	if err := s.ConfigureRuntime(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.SetupStatus().Source != "override" {
		t.Fatal("explicit override must be default")
	}
	if _, err := s.userMessage(context.Background(), newID(), root, "", "queued", "", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.userMessage(context.Background(), newID(), root, "", "also queued", "", nil); err != nil {
		t.Fatal(err)
	}
	send := func(source string) int {
		form := url.Values{"source": {source}}
		req := httptest.NewRequest("POST", "/api/codex-setup/source", strings.NewReader(form.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		w := httptest.NewRecorder()
		s.codexSetupSource(w, req)
		return w.Code
	}
	s.runningJobs = 1
	if code := send("managed"); code != 409 {
		t.Fatalf("in-flight job: %d", code)
	}
	s.runningJobs = 0
	if _, err := s.db.Exec("UPDATE jobs SET status='submitted' WHERE rowid=(SELECT min(rowid) FROM jobs)"); err != nil {
		t.Fatal(err)
	}
	if code := send("managed"); code != 409 {
		t.Fatalf("submitted job: %d", code)
	}
	if _, err := s.db.Exec("UPDATE jobs SET status='uncertain' WHERE status='submitted'"); err != nil {
		t.Fatal(err)
	}
	if code := send("managed"); code != 202 {
		t.Fatalf("historical uncertain: %d", code)
	}
	if status := r.SetupStatus(); status.Source != "managed" || status.State == "ready" {
		t.Fatalf("managed setup admitted before ready: %+v", status)
	}
	if job, err := s.claim(nil); err != nil || job != nil {
		t.Fatalf("claimed while setup unavailable: %+v %v", job, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, filepath.Join(rootDir, "data"))
	defer s.Close()
	next := newRunner()
	if err := s.ConfigureRuntime(ctx, next); err != nil {
		t.Fatal(err)
	}
	if status := next.SetupStatus(); status.Source != "managed" || status.State == "ready" {
		t.Fatalf("selection not restored: %+v", status)
	}
	var uncertain int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE status='uncertain'").Scan(&uncertain); err != nil || uncertain != 1 {
		t.Fatalf("uncertain job changed: %d %v", uncertain, err)
	}
	var pending int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE status='pending'").Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("pending work changed: %d %v", pending, err)
	}
	if code := send("override"); code != 202 {
		t.Fatalf("restore override: %d", code)
	}
	if status := next.SetupStatus(); status.Source != "override" || status.State != "ready" {
		t.Fatalf("override not ready: %+v", status)
	}
	w := httptest.NewRecorder()
	s.codexSetupStatus(w, httptest.NewRequest("GET", "/api/codex-setup", nil))
	if w.Header().Get("X-AgentChat-Codex-Can-Select-Managed") != "true" || w.Header().Get("X-AgentChat-Codex-Can-Select-Override") != "true" || w.Header().Get("X-AgentChat-Codex-Source") != "override" {
		t.Fatalf("source headers: %v", w.Header())
	}
}
