//go:build !windows

package codexruntime

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"testing"
)

type bundleTransport func(*http.Request) (*http.Response, error)

func (f bundleTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestManagedBundleDownloadAndReuse(t *testing.T) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	files := map[string]string{"codex-package.json": "{}", "bin/codex": "#!/bin/sh\nprintf 'codex-cli 0.172.0\\n'\n", "bin/codex-code-mode-host": "x", "bin/codex-exec-server": "x", "codex-path/rg": "x"}
	if runtime.GOOS == "linux" {
		files["codex-resources/bwrap"] = "x"
	}
	for name, body := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0755, Size: int64(len(body))}); err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(tw, body); err != nil {
			t.Fatal(err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		t.Fatal(err)
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(buf.Bytes()))
	calls := 0
	client := &http.Client{Transport: bundleTransport(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Body: io.NopCloser(bytes.NewReader(buf.Bytes())), Header: make(http.Header)}, nil
	})}
	spec := &bundleSpec{asset: "fixture.tar.gz", digest: digest, version: "0.172.0", client: client}
	root := t.TempDir()
	first, err := spec.prepare(context.Background(), root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := spec.prepare(context.Background(), root)
	if err != nil || first != second || calls != 1 {
		t.Fatalf("reuse: %q %q calls %d: %v", first, second, calls, err)
	}
	spec.digest = fmt.Sprintf("%064d", 1)
	if _, err := spec.prepare(context.Background(), root); err == nil {
		t.Fatal("accepted mismatched digest")
	}
}
