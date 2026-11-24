package service

import (
	"agentchat/src/internal/runtimeapi"
	"database/sql"
	"testing"
)

func TestProjectGoalLifecycleAndAuthority(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec(`INSERT INTO projects(id,name,po_id,workspace) VALUES ('atlas','atlas','atlas-po','/tmp/atlas')`); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO agents VALUES ('atlas-po','atlas-po','po','atlas','fixture','high')`)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	action := runtimeapi.Action{Type: "set_project_goal", Project: "atlas", Body: "Finish the Hospital slice"}
	if err := s.write(func(tx *sql.Tx) error {
		return s.applyAction(tx, agent{ID: "atlas-po", Role: "po", Project: "atlas"}, job{Project: "atlas", Kind: "assignment"}, action)
	}); err == nil {
		t.Fatal("PO could set a goal")
	}
	apply := func(a runtimeapi.Action) error {
		return s.write(func(tx *sql.Tx) error {
			return s.applyAction(tx, agent{ID: root, Role: "root"}, job{Kind: "operator_message"}, a)
		})
	}
	if err := apply(action); err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{"paused", "active", "complete"} {
		if err := apply(runtimeapi.Action{Type: "set_project_goal_status", Project: "atlas", Body: status}); err != nil {
			t.Fatal(err)
		}
		var got, objective string
		if err := s.db.QueryRow(`SELECT status,objective FROM project_goals WHERE project_id='atlas'`).Scan(&got, &objective); err != nil {
			t.Fatal(err)
		}
		if got != status || objective != action.Body {
			t.Fatalf("got %q %q", got, objective)
		}
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("goal unexpectedly dispatched work: %d %v", count, err)
	}
}
