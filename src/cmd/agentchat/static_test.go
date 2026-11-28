package main

import (
	"agentchat/src/internal/service"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestBrowserDeepLinksServeShellWithoutMaskingAssetsOrAPI(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("app shell"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "assets"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets", "site.js"), []byte("asset"), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := service.Open(service.Options{Directory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := s.Handler(staticHandler(dir))
	for _, path := range []string{"/", "/root", "/settings", "/settings/models", "/settings/providers", "/settings/usage", "/settings/codex", "/settings/updates", "/into-the-rot", "/groups/into-the-rot", "/agents/12345678-1234-1234-1234-123456789abc"} {
		t.Run(path, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, path, nil)
			r.Header.Set("If-None-Match", `"stale"`)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != 200 || w.Body.String() != "app shell" || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("shell status=%d cache=%q", w.Code, w.Header().Get("Cache-Control"))
			}
		})
	}
	for _, path := range []string{"/api/unknown", "/assets/missing.js", "/files", "/attachments", "/settings/unknown", "/groups/a/b", "/bad.slug"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != 404 || strings.Contains(w.Body.String(), "app shell") {
			t.Fatalf("%s unexpectedly returned shell: %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api", nil))
	if w.Code != http.StatusTemporaryRedirect || w.Header().Get("Location") != "/api/" {
		t.Fatalf("api redirect %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/assets/site.js", nil))
	if w.Code != 200 || w.Body.String() != "asset" {
		t.Fatalf("asset %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodHead, "/settings/models", nil))
	if w.Code != 200 || w.Body.Len() != 0 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("head %d", w.Code)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/settings/models", nil))
	if w.Code != 404 {
		t.Fatalf("post %d", w.Code)
	}
	r := httptest.NewRequest(http.MethodGet, "/settings/models", nil)
	r.Header.Set("Origin", "https://untrusted.example")
	w = httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatalf("origin guard %d", w.Code)
	}
}
