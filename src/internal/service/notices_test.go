package service

import (
	"database/sql"
	"slices"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestWorkflowNoticesReflectCommittedTransitions(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	source := newID()
	rootJob := job{ID: newID(), Source: source}
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, source, operator, root, "operator", "private request", "", ""); err != nil {
			return err
		}
		return s.rootAssignment(tx, agent{ID: root, Role: "root"}, rootJob, runtimeapi.Action{Type: "create_project", Project: "atlas", Title: "private title", Body: "secret handoff"})
	}); err != nil {
		t.Fatal(err)
	}
	var task, po, worker, workID string
	if err := s.db.QueryRow("SELECT id,owner_id FROM tasks WHERE project_id='atlas'").Scan(&task, &po); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT id FROM agents WHERE project_id='atlas' AND role='worker'").Scan(&worker); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		var before, after int
		if err := tx.QueryRow("SELECT count(*) FROM jobs").Scan(&before); err != nil {
			return err
		}
		if err := s.applyAction(tx, agent{ID: po, Role: "po", Project: "atlas"}, job{Kind: "assignment", Project: "atlas", Task: task}, runtimeapi.Action{Type: "post_group", Project: "atlas", Body: "PO coordination"}); err != nil {
			return err
		}
		if err := tx.QueryRow("SELECT count(*) FROM jobs").Scan(&after); err != nil {
			return err
		}
		if before != after {
			t.Fatal("group post dispatched work")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		return s.queueWorker(tx, job{ID: newID(), Project: "atlas", Task: task}, agent{ID: po, Role: "po", Project: "atlas"}, runtimeapi.Action{Type: "queue_worker", Body: "secret worker instructions"})
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT id FROM jobs WHERE task_id=? AND kind='work'", task).Scan(&workID); err != nil {
		t.Fatal(err)
	}
	before, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(before.Messages, func(m Message) bool { return strings.Contains(m.Text, "Worker run launched") }) {
		t.Fatal("claimed start before provider start")
	}
	if _, err = s.db.Exec("UPDATE jobs SET status='submitted' WHERE id=?", workID); err != nil {
		t.Fatal(err)
	}
	j := job{ID: workID, Project: "atlas", Agent: worker, Task: task, Kind: "work"}
	if err = s.workerStarted(j); err != nil {
		t.Fatal(err)
	}
	if err = s.workerStarted(j); err != nil {
		t.Fatal(err)
	}
	var status string
	if err = s.db.QueryRow("SELECT status FROM jobs WHERE id=?", workID).Scan(&status); err != nil || status != "running" {
		t.Fatalf("start status %q: %v", status, err)
	}
	if err = s.write(func(tx *sql.Tx) error {
		if _, err := updateStop(tx, "atlas", true); err != nil {
			return err
		}
		_, err := updateStop(tx, "atlas", true)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	var startID string
	starts, stops := 0, 0
	for _, m := range view.Messages {
		if m.RoomID != "group:atlas" {
			continue
		}
		if strings.Contains(m.Text, "secret") {
			t.Fatal("private instructions leaked")
		}
		if strings.Contains(m.Text, "Worker run launched") {
			starts++
			startID = m.ID
		}
		if strings.Contains(m.Text, "New dispatch is paused") {
			stops++
			if !strings.Contains(m.Text, "1 job is active or unresolved") {
				t.Fatalf("misleading stop notice: %s", m.Text)
			}
		}
	}
	if starts != 1 || stops != 1 {
		t.Fatalf("dedup failed: starts %d stops %d", starts, stops)
	}
	if err = s.markOperatorRead([]string{startID}); err != nil {
		t.Fatal(err)
	}
	if err = s.write(func(tx *sql.Tx) error {
		ctx, err := s.runtimeContext(tx, agent{ID: worker, Role: "worker", Project: "atlas"}, "atlas", task, "", "work")
		if err != nil {
			return err
		}
		if !slices.ContainsFunc(ctx, func(m runtimeapi.Message) bool { return m.ID == startID }) {
			t.Fatal("authorized worker lacks shared notice")
		}
		if !slices.ContainsFunc(ctx, func(m runtimeapi.Message) bool { return m.Sender == po && m.Text == "PO coordination" }) {
			t.Fatal("authorized worker lacks authored group post")
		}

		return nil
	}); err != nil {
		t.Fatal(err)
	}
	privateDM := newID()
	if err = s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, privateDM, operator, worker, "operator", "private operator worker DM", "atlas", ""); err != nil {
			return err
		}
		ctx, err := s.runtimeContext(tx, agent{ID: po, Role: "po", Project: "atlas"}, "atlas", task, "", "review")
		if err != nil {
			return err
		}
		if slices.ContainsFunc(ctx, func(m runtimeapi.Message) bool { return m.ID == privateDM }) {
			t.Fatal("operator-worker DM leaked into PO context")
		}
		ctx, err = s.runtimeContext(tx, agent{ID: po, Role: "po", Project: "atlas"}, "atlas", "", "", groupMessageKind)
		if err != nil {
			return err
		}
		if slices.ContainsFunc(ctx, func(m runtimeapi.Message) bool { return m.ID == privateDM }) {
			t.Fatal("operator-worker DM leaked into group context")
		}
		if err := postGroupAction(tx, agent{ID: worker, Role: "worker", Project: "atlas"}, j, runtimeapi.Action{Type: "post_group", Project: "other", Body: "bad"}); err == nil {
			t.Fatal("cross-project post accepted")
		}
		dmAction := runtimeapi.Action{Type: "post_group", Project: "atlas", Body: "private DM promoted"}
		if err := s.applyAction(tx, agent{ID: po, Role: "po", Project: "atlas"}, job{Kind: "operator_message", Project: "atlas"}, dmAction); err == nil {
			t.Fatal("PO operator DM promoted to group")
		}
		if err := s.workerResult(tx, job{Kind: "operator_message", Project: "atlas"}, agent{ID: worker, Role: "worker", Project: "atlas"}, runtimeapi.Result{Text: "private reply", Actions: []runtimeapi.Action{dmAction}}, nil); err == nil {
			t.Fatal("worker operator DM promoted to group")
		}

		return s.workerResult(tx, j, agent{ID: worker, Role: "worker", Project: "atlas"}, runtimeapi.Result{Text: "private result", Actions: []runtimeapi.Action{{Type: "post_group", Project: "atlas", Body: "Coordination update"}}}, nil)
	}); err != nil {
		t.Fatal(err)
	}
	view, err = s.state()
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(view.Messages, func(m Message) bool {
		return m.RoomID == "group:atlas" && m.Sender == worker && m.Text == "Coordination update"
	}) {
		t.Fatal("authored group update missing")
	}
}
