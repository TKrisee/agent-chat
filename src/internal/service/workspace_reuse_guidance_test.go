package service

import (
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
)

func TestConfiguredWorkspaceReuseRequiresExactPOAssignment(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO project_workspace_roots(project_id,path) VALUES('alpha',?)`, root); err != nil {
			return err
		}
		a, err := getAgent(tx, worker)
		if err != nil {
			return err
		}
		source := newID()
		if err := addMessage(tx, source, po, worker, "po", "Review the assigned item.", "alpha", task); err != nil {
			return err
		}
		return s.enqueue(tx, a, "alpha", task, source, "work", "Review the assigned item.")
	})
	if err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("claim: %+v %v", j, err)
	}
	if filepath.Dir(j.Request.Workspace) != root || filepath.Base(j.Request.Workspace) != j.ID {
		t.Fatalf("fresh workspace %q", j.Request.Workspace)
	}
	for _, text := range []string{"default isolated workspace", "PO explicitly assigns its exact authorized path", "same worker, repository and work lane", "no concurrent owner or unresolved work", "All other job workspaces remain out of scope"} {
		if !strings.Contains(j.Request.Instructions, text) {
			t.Errorf("missing workspace boundary %q", text)
		}
	}
}
