package service

import (
	"agentchat/src/internal/runtimeapi"
	"database/sql"
	"strings"
	"testing"
)

func TestProjectAndAgentNames(t *testing.T) {
	for _, name := range []string{" name", "name ", "a b", "a\tb", "a\nb", "a\u00a0b", ""} {
		if validateDisplayName(name) == nil {
			t.Errorf("accepted %q", name)
		}
	}
	for _, name := range []string{"Atlas-PO", "café", "猫"} {
		if err := validateDisplayName(name); err != nil {
			t.Errorf("rejected %q: %v", name, err)
		}
	}
	s := openTest(t, t.TempDir())
	defer s.Close()
	action := runtimeapi.Action{Type: "create_project", Project: "atlas", Title: "A task with spaces", Body: "Body with spaces"}
	err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, "source", operator, root, "operator", "request", "", ""); err != nil {
			return err
		}
		return s.rootAssignment(tx, agent{ID: root, Role: "root"}, job{Source: "source"}, action)
	})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := s.db.Query("SELECT name FROM agents WHERE project_id=?", "atlas")
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		if err := validateDisplayName(name); err != nil {
			t.Fatal(err)
		}
		count++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if count != 1 {
		t.Fatalf("got %d generated agents, want 1 PO", count)
	}
	action.Project = "bad\u00a0name"
	err = s.write(func(tx *sql.Tx) error { return s.rootAssignment(tx, agent{ID: root, Role: "root"}, job{}, action) })
	if err == nil || !strings.Contains(err.Error(), "must not contain whitespace") {
		t.Fatalf("unexpected error: %v", err)
	}
}
