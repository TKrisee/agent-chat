package updater

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestApplyWaitsForDrainAndRejectsConcurrentRequests(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	if err := os.MkdirAll(filepath.Join(stage, "payload"), 0700); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	stopped := make(chan struct{}, 1)
	h := &Handler{Root: root, Version: "1.0.0", state: "ready", stage: stage, release: Release{Version: "1.1.0"}, Stop: func() { stopped <- struct{}{} }, Drain: func() (<-chan struct{}, func(), error) { return ready, func() {}, nil }}
	w := &recorder{header: make(http.Header)}
	h.ServeHTTP(w, httptestRequest(http.MethodPost, "/api/update/apply"))
	if w.status != http.StatusAccepted || w.header.Get("X-AgentChat-Update-State") != "draining" {
		t.Fatalf("apply status %d", w.status)
	}
	if _, err := os.Stat(filepath.Join(root, "update-request")); !os.IsNotExist(err) {
		t.Fatalf("restart requested before drain: %v", err)
	}
	for _, path := range []string{"/api/update/apply", "/api/update/check"} {
		w = &recorder{header: make(http.Header)}
		h.ServeHTTP(w, httptestRequest(http.MethodPost, path))
		if w.status != http.StatusConflict {
			t.Fatalf("%s status %d", path, w.status)
		}
	}
	close(ready)
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("no restart after drain")
	}
	if _, err := os.Stat(filepath.Join(root, "update-request")); err != nil {
		t.Fatal(err)
	}
}

func TestApplyDrainCancelsWhenServiceStops(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	if err := os.MkdirAll(filepath.Join(stage, "payload"), 0700); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	released := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	h := &Handler{Root: root, Version: "1.0.0", state: "ready", stage: stage, release: Release{Version: "1.1.0"}, Lifetime: ctx, Stop: func() { stopped <- struct{}{} }, Drain: func() (<-chan struct{}, func(), error) { return ready, func() { released <- struct{}{} }, nil }}
	w := &recorder{header: make(http.Header)}
	h.ServeHTTP(w, httptestRequest(http.MethodPost, "/api/update/apply"))
	if w.status != http.StatusAccepted {
		t.Fatalf("apply status %d", w.status)
	}
	cancel()
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("drain not released")
	}
	close(ready)
	if _, err := os.Stat(filepath.Join(root, "update-request")); !os.IsNotExist(err) {
		t.Fatalf("unexpected restart request: %v", err)
	}
	select {
	case <-stopped:
		t.Fatal("update requested shutdown")
	default:
	}
}

func TestApplyFailureReleasesDrain(t *testing.T) {
	root := t.TempDir()
	stage := filepath.Join(root, "stage")
	if err := os.MkdirAll(filepath.Join(stage, "payload"), 0700); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	released := make(chan struct{}, 1)
	stopped := make(chan struct{}, 1)
	h := &Handler{Root: root, Version: "1.0.0", state: "ready", stage: stage, release: Release{Version: "1.1.0"}, Stop: func() { stopped <- struct{}{} }, Drain: func() (<-chan struct{}, func(), error) { return ready, func() { released <- struct{}{} }, nil }}
	w := &recorder{header: make(http.Header)}
	h.ServeHTTP(w, httptestRequest(http.MethodPost, "/api/update/apply"))
	if w.status != http.StatusAccepted {
		t.Fatalf("apply status %d", w.status)
	}
	if err := os.Remove(filepath.Join(stage, "payload")); err != nil {
		t.Fatal(err)
	}
	close(ready)
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("drain not released on error")
	}
	select {
	case <-stopped:
		t.Fatal("shutdown after failure")
	default:
	}
}
