package service

import (
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestProjectWorkspace(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if _, err := s.db.Exec("INSERT INTO projects(id,name,po_id,workspace) VALUES('atlas','atlas','po','/tmp/atlas')"); err != nil {
		t.Fatal(err)
	}
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	post := func(path string) int {
		r := httptest.NewRequest("POST", "/", strings.NewReader(url.Values{"path": {path}}.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		r.SetPathValue("id", "atlas")
		w := httptest.NewRecorder()
		s.setProjectWorkspace(w, r)
		return w.Code
	}
	if post("relative") != 400 || post(root) != 200 {
		t.Fatal("workspace validation failed")
	}
	req := runtimeapi.Request{ID: "job-1", ProjectID: "atlas", Kind: "work", Workspace: filepath.Join(root, "job-1")}
	if _, err := s.db.Exec("INSERT INTO project_workspace_jobs(job_id,path) VALUES(?,?)", req.ID, req.Workspace); err != nil {
		t.Fatal(err)
	}
	if err := s.materialize(req); err != nil {
		t.Fatal(err)
	}
	if err := s.materialize(req); err != nil {
		t.Fatal("retry:", err)
	}
	if err := os.Remove(req.Workspace); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), req.Workspace); err != nil {
		t.Fatal(err)
	}
	if err := s.materialize(req); err == nil {
		t.Fatal("accepted substituted symlink")
	}
}
