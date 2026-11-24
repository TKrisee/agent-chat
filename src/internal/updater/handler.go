package updater

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Handler provides user-initiated updates only under the managed launcher.
type Handler struct {
	Client        Client
	Root, Version string
	Stop          context.CancelFunc
	mu            sync.Mutex
	state         string
	message       string
	release       Release
	stage         string
}
type Status struct {
	Available bool   `json:"available"`
	Current   string `json:"current"`
	Version   string `json:"version,omitempty"`
	State     string `json:"state"`
	Error     string `json:"error,omitempty"`
}

func (h *Handler) status() Status {
	s := Status{Available: h.Root != "" && versionPattern.MatchString(h.Version), Current: h.Version, Version: h.release.Version, State: h.state, Error: h.message}
	if s.State == "" {
		s.State = "idle"
	}
	return s
}
func newer(a, b string) bool {
	aa := strings.Split(a, ".")
	bb := strings.Split(b, ".")
	if len(aa) != 3 || len(bb) != 3 {
		return false
	}
	for i := range aa {
		x, e := strconv.ParseUint(aa[i], 10, 64)
		if e != nil {
			return false
		}
		y, e := strconv.ParseUint(bb[i], 10, 64)
		if e != nil {
			return false
		}
		if x != y {
			return x > y
		}
	}
	return false
}
func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	h.mu.Lock()
	defer h.mu.Unlock()
	if r.Method == http.MethodGet && r.URL.Path == "/api/update" {
		writeStatus(w, h.status(), http.StatusOK)
		return
	}
	if r.Method != http.MethodPost || h.Root == "" || !versionPattern.MatchString(h.Version) {
		http.Error(w, "managed updates unavailable", http.StatusForbidden)
		return
	}
	switch r.URL.Path {
	case "/api/update/check":
		if h.state == "checking" || h.state == "downloading" {
			http.Error(w, "update busy", http.StatusConflict)
			return
		}
		h.state = "checking"
		h.message = ""
		h.release = Release{}
		if h.stage != "" {
			os.RemoveAll(h.stage)
			h.stage = ""
		}
		go h.check()
	case "/api/update/download":
		if h.state != "available" {
			http.Error(w, "no update available", http.StatusConflict)
			return
		}
		h.state = "downloading"
		go h.download(h.release)
	case "/api/update/apply":
		if h.state != "ready" || h.stage == "" {
			http.Error(w, "update not ready", http.StatusConflict)
			return
		}
		versions := filepath.Join(h.Root, "versions")
		if err := os.MkdirAll(versions, 0700); err != nil {
			h.message = err.Error()
			http.Error(w, "cannot prepare update", 500)
			return
		}
		dest := filepath.Join(versions, h.release.Version)
		if _, err := os.Lstat(dest); !os.IsNotExist(err) {
			http.Error(w, "version already installed", 409)
			return
		}
		if err := os.Rename(filepath.Join(h.stage, "payload"), dest); err != nil {
			h.message = err.Error()
			http.Error(w, "cannot install update", 500)
			return
		}
		os.Remove(h.stage)
		h.stage = ""
		tmp := filepath.Join(h.Root, "update-request.tmp")
		if err := os.WriteFile(tmp, []byte(h.release.Version+"\n"), 0600); err != nil {
			http.Error(w, "cannot request restart", 500)
			return
		}
		if err := os.Rename(tmp, filepath.Join(h.Root, "update-request")); err != nil {
			http.Error(w, "cannot request restart", 500)
			return
		}
		h.state = "restarting"
		go h.Stop()
	default:
		http.NotFound(w, r)
		return
	}
	writeStatus(w, h.status(), http.StatusAccepted)
}
func (h *Handler) check() {
	rel, err := h.Client.Check(context.Background())
	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		h.state = "error"
		h.message = err.Error()
		return
	}
	if !newer(rel.Version, h.Version) {
		h.state = "current"
		return
	}
	h.release = rel
	h.state = "available"
}
func (h *Handler) download(rel Release) {
	stage, err := h.Client.Download(context.Background(), rel, h.Root)
	h.mu.Lock()
	defer h.mu.Unlock()
	if err != nil {
		h.state = "error"
		h.message = fmt.Sprint(err)
		return
	}
	h.stage = stage
	h.state = "ready"
}

func writeStatus(w http.ResponseWriter, s Status, code int) {
	w.Header().Set("X-AgentChat-Update-Available", strconv.FormatBool(s.Available))
	w.Header().Set("X-AgentChat-Update-Current", s.Current)
	w.Header().Set("X-AgentChat-Update-Version", s.Version)
	w.Header().Set("X-AgentChat-Update-State", s.State)
	w.WriteHeader(code)
	if s.Error != "" {
		_, _ = fmt.Fprint(w, s.Error)
	}
}
