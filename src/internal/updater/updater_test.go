package updater

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type transport func(*http.Request) (*http.Response, error)

func (f transport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func fixture(h http.HandlerFunc) *http.Client {
	return &http.Client{Transport: transport(func(r *http.Request) (*http.Response, error) {
		w := &recorder{header: make(http.Header)}
		h(w, r)
		if w.status == 0 {
			w.status = 200
		}
		return &http.Response{StatusCode: w.status, Status: fmt.Sprint(w.status), Header: w.header, Body: io.NopCloser(&w.body)}, nil
	})}
}

type recorder struct {
	header http.Header
	body   bytes.Buffer
	status int
}

func (w *recorder) Header() http.Header         { return w.header }
func (w *recorder) Write(b []byte) (int, error) { return w.body.Write(b) }
func (w *recorder) WriteHeader(s int)           { w.status = s }
func archive(t *testing.T, malicious bool) []byte {
	t.Helper()
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)
	tw := tar.NewWriter(gz)
	entries := map[string]string{"agentchat": "binary", "web/index.html": "ui"}
	if malicious {
		entries["../escape"] = "bad"
	}
	for name, content := range entries {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0755, Size: int64(len(content))}); err != nil {
			t.Fatal(err)
		}
		if _, err := tw.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	return b.Bytes()
}
func TestDownloadVerifiesAndStages(t *testing.T) {
	for _, tc := range []struct {
		name               string
		badHash, malicious bool
	}{{"valid", false, false}, {"checksum", true, false}, {"traversal", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			data := archive(t, tc.malicious)
			hash := sha256.Sum256(data)
			digest := fmt.Sprintf("%x", hash)
			if tc.badHash {
				digest = strings.Repeat("0", 64)
			}
			httpClient := fixture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/latest/download/agentchat-update.txt":
					fmt.Fprintf(w, "version 1.2.3\n%s agentchat-linux-amd64.tar.gz\n%s agentchat-darwin-arm64.tar.gz\n%s agentchat-windows-amd64.zip\n", digest, digest, digest)
				case "/download/v1.2.3/agentchat-darwin-arm64.tar.gz":
					w.Write(data)
				default:
					http.NotFound(w, r)
				}
			}))
			client := Client{BaseURL: "https://fixture", HTTP: httpClient, OS: "darwin", Arch: "arm64"}
			rel, err := client.Check(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			parent := t.TempDir()
			stage, err := client.Download(context.Background(), rel, parent)
			if tc.badHash || tc.malicious {
				if err == nil {
					t.Fatal("expected error")
				}
				entries, _ := os.ReadDir(parent)
				if len(entries) != 0 {
					t.Fatal("failed stage not cleaned")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, err = os.Stat(filepath.Join(stage, "payload", "web", "index.html")); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestRejectsUnsupportedAndMalformedManifest(t *testing.T) {
	httpClient := fixture(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, "version ../../bad\n") }))
	if _, err := (Client{BaseURL: "https://fixture", HTTP: httpClient, OS: "darwin", Arch: "amd64"}).Check(context.Background()); err == nil {
		t.Fatal("unsupported platform accepted")
	}
	if _, err := (Client{BaseURL: "https://fixture", HTTP: httpClient, OS: "darwin", Arch: "arm64"}).Check(context.Background()); err == nil {
		t.Fatal("bad manifest accepted")
	}
}

func TestManagedApplyRequiresStagedUpdateAndRequestsShutdown(t *testing.T) {
	root := t.TempDir()
	stage, err := os.MkdirTemp(root, "stage-")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.Mkdir(filepath.Join(stage, "payload"), 0700); err != nil {
		t.Fatal(err)
	}
	stopped := make(chan struct{}, 1)
	h := &Handler{Root: root, Version: "1.0.0", Stop: func() { stopped <- struct{}{} }, state: "ready", release: Release{Version: "1.1.0"}, stage: stage}
	w := &recorder{header: make(http.Header)}
	h.ServeHTTP(w, httptestRequest(http.MethodPost, "/api/update/apply"))
	if w.status != http.StatusAccepted {
		t.Fatalf("status %d body %s", w.status, w.body.String())
	}
	if w.header.Get("X-AgentChat-Update-State") != "restarting" {
		t.Fatal("wrong state")
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("no shutdown requested")
	}
	b, err := os.ReadFile(filepath.Join(root, "update-request"))
	if err != nil || string(b) != "1.1.0\n" {
		t.Fatalf("update request %q: %v", b, err)
	}
	if _, err = os.Stat(filepath.Join(root, "versions", "1.1.0")); err != nil {
		t.Fatal(err)
	}
}
func httptestRequest(method, path string) *http.Request {
	r, _ := http.NewRequest(method, "http://localhost"+path, nil)
	return r
}

func TestCommittedStatusRequiresActiveVersionAndNoRequest(t *testing.T) {
	root := t.TempDir()
	h := &Handler{Root: root, Version: "2.0.0"}
	check := func(want string) {
		t.Helper()
		w := &recorder{header: make(http.Header)}
		h.ServeHTTP(w, httptestRequest(http.MethodGet, "/api/update"))
		if got := w.header.Get("X-AgentChat-Update-Committed"); got != want {
			t.Fatalf("committed = %q, want %q", got, want)
		}
	}
	check("false")
	if err := os.WriteFile(filepath.Join(root, "active-version"), []byte("1.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	check("false")
	if err := os.WriteFile(filepath.Join(root, "active-version"), []byte("2.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "update-request"), []byte("2.0.0\n"), 0600); err != nil {
		t.Fatal(err)
	}
	check("false")
	if err := os.Remove(filepath.Join(root, "update-request")); err != nil {
		t.Fatal(err)
	}
	check("true")
}
