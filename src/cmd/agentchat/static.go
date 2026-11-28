package main

import (
	"net/http"
	"regexp"
	"strings"
)

var browserSlug = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var browserAgentID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)

// browserRoute recognizes navigable app paths, not API or asset paths.
func browserRoute(path string) bool {
	switch path {
	case "/", "/root", "/settings", "/settings/models", "/settings/providers", "/settings/usage", "/settings/codex", "/settings/updates":
		return true
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	if len(parts) == 2 && parts[0] == "groups" {
		return browserSlug.MatchString(parts[1])
	}
	if len(parts) == 2 && parts[0] == "agents" {
		return browserAgentID.MatchString(parts[1])
	}
	if len(parts) != 1 || !browserSlug.MatchString(parts[0]) {
		return false
	}
	switch parts[0] {
	case "api", "assets", "files", "attachments", "groups", "agents", "settings", "root":
		return false
	}
	return true
}

func staticHandler(dir string) http.Handler {
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if (r.Method == http.MethodGet || r.Method == http.MethodHead) && browserRoute(r.URL.Path) {
			w.Header().Set("Cache-Control", "no-store")
			r = r.Clone(r.Context())
			r.URL.Path = "/"
			r.URL.RawPath = ""
			r.Header.Del("If-Modified-Since")
			r.Header.Del("If-None-Match")
		} else if r.URL.Path == "/index.html" {
			w.Header().Set("Cache-Control", "no-store")
			r = r.Clone(r.Context())
			r.Header.Del("If-Modified-Since")
			r.Header.Del("If-None-Match")
		}
		files.ServeHTTP(w, r)
	})
}
