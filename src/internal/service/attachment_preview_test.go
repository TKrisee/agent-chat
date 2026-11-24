package service

import (
	"bytes"
	"database/sql"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestResultCanAttachSevenEvidenceFiles(t *testing.T) {
	workspace := t.TempDir()
	refs := []runtimeapi.Attachment{}
	for i := 0; i < 7; i++ {
		name := fmt.Sprintf("evidence-%d.txt", i)
		path := filepath.Join(workspace, name)
		if err := os.WriteFile(path, []byte(name), 0600); err != nil {
			t.Fatal(err)
		}
		refs = append(refs, runtimeapi.Attachment{Path: path, Name: name})
	}
	files, err := readAttachments(workspace, refs)
	if err != nil || len(files) != len(refs) {
		t.Fatalf("load seven evidence files: files=%d err=%v", len(files), err)
	}
	for i, file := range files {
		if file.Name != refs[i].Name || string(file.Content) != refs[i].Name {
			t.Fatalf("evidence file changed: %+v", file)
		}
	}
}

func TestAttachmentPreviewAndForcedDownload(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	server := httptest.NewServer(s.Handler(nil))
	defer server.Close()
	png, err := base64.StdEncoding.DecodeString("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVQIW2P8z8DwHwAFgAI/ScLttAAAAABJRU5ErkJggg==")
	if err != nil {
		t.Fatal(err)
	}
	type file struct {
		id, name, stored, preview string
		data                      []byte
	}
	files := []file{
		{name: "pixel.png", stored: "image/png", preview: "image/png", data: png},
		{name: "meeting notes.pdf", stored: "application/octet-stream", preview: "application/pdf", data: []byte("%PDF-1.4\n")},
		{name: "table.csv", stored: "text/csv", preview: "text/plain; charset=utf-8", data: []byte("name,status\nlaunch,ready\n")},
		{name: "report.json", stored: "application/json", preview: "text/plain; charset=utf-8", data: []byte("{\"ready\":true}\n")},
		{name: "sound.wav", stored: "audio/wav", preview: "audio/wave", data: []byte("RIFF\x00\x00\x00\x00WAVEfmt ")},
		{name: "movie.mp4", stored: "application/octet-stream", preview: "video/mp4", data: []byte("\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom")},
		{name: "script.js", stored: "text/javascript", preview: "text/plain; charset=utf-8", data: []byte("alert('display as source');\n")},
		{name: "spoof.png", stored: "image/png", data: []byte("<!DOCTYPE html><script>alert(1)</script>")},
		{name: "icon.svg", stored: "image/svg+xml", data: []byte("<svg xmlns=\"http://www.w3.org/2000/svg\"></svg>")},
		{name: "unknown.bin", stored: "application/octet-stream", data: []byte("unknown plaintext-like bytes")},
		{name: "spoof.txt", stored: "text/plain", data: append(bytes.Repeat([]byte("x"), 520), 0x00)},
	}
	if err := s.write(func(tx *sql.Tx) error {
		message := newID()
		if err := addMessage(tx, message, root, operator, "root", "Preview fixtures", "", ""); err != nil {
			return err
		}
		for i := range files {
			f := &files[i]
			f.id = newID()
			if _, err := tx.Exec("INSERT INTO attachments VALUES(?,?,?,?,?)", f.id, message, f.name, f.stored, f.data); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	get := func(path, byteRange string, wantStatus int) (http.Header, []byte) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, server.URL+"/api/attachments/"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		if byteRange != "" {
			req.Header.Set("Range", byteRange)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil || resp.StatusCode != wantStatus {
			t.Fatalf("GET %s: status=%d want=%d body=%q err=%v", path, resp.StatusCode, wantStatus, body, err)
		}
		if resp.Header.Get("X-Content-Type-Options") != "nosniff" ||
			resp.Header.Get("Content-Security-Policy") != "sandbox; default-src 'none'" ||
			resp.Header.Get("Cache-Control") != "no-store" {
			t.Fatalf("unsafe response headers: %v", resp.Header)
		}
		return resp.Header, body
	}
	for _, f := range files {
		t.Run(f.name, func(t *testing.T) {
			if f.preview == "" {
				headers, _ := get(f.id+"/preview", "", http.StatusUnsupportedMediaType)
				if headers.Get("Content-Type") != "application/json" || headers.Get("Content-Disposition") != "" {
					t.Fatalf("unexpected unsupported preview headers: %v", headers)
				}
				return
			}
			headers, body := get(f.id+"/preview", "", http.StatusOK)
			disposition := "inline; filename=" + f.name
			if f.name == "meeting notes.pdf" {
				disposition = `inline; filename="meeting notes.pdf"`
			}
			if headers.Get("Content-Type") != f.preview || headers.Get("Content-Disposition") != disposition ||
				!bytes.Equal(body, f.data) {
				t.Fatalf("unexpected preview: headers=%v body=%q", headers, body)
			}
		})
	}

	image := files[0]
	headers, body := get(image.id+"?download=1", "", http.StatusOK)
	if headers.Get("Content-Type") != "application/octet-stream" ||
		headers.Get("Content-Disposition") != "attachment; filename=pixel.png" || !bytes.Equal(body, png) {
		t.Fatalf("unexpected forced download: headers=%v body=%q", headers, body)
	}
	headers, _ = get(image.id, "", http.StatusOK)
	if headers.Get("Content-Type") != "image/png" || headers.Get("Content-Disposition") != "inline; filename=pixel.png" {
		t.Fatalf("legacy thumbnail response changed: %v", headers)
	}
	headers, body = get(image.id+"/preview", "bytes=2-5", http.StatusPartialContent)
	if headers.Get("Content-Range") != fmt.Sprintf("bytes 2-5/%d", len(png)) ||
		headers.Get("Accept-Ranges") != "bytes" || headers.Get("Content-Length") != "4" || !bytes.Equal(body, png[2:6]) {
		t.Fatalf("preview range changed: headers=%v body=%q", headers, body)
	}
}
