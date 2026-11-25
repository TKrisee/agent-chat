package service

import (
	"database/sql"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestPOProgressIsPrivateAndTaskLinked(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	privateDM, privateReply := newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, privateDM, operator, po, "operator", "Private DM", "alpha", ""); err != nil {
			return err
		}
		return addMessage(tx, privateReply, po, operator, "po", "Private reply", "alpha", "")
	}); err != nil {
		t.Fatal(err)
	}
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Text: "Routine PO task update.", Actions: []runtimeapi.Action{{Type: "report_progress", Project: "alpha", Body: "Work is underway."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var id, room, status string
	if err := s.db.QueryRow(`SELECT m.id,m.room_id FROM messages m JOIN po_progress_notes n ON n.message_id=m.id WHERE m.sender=? AND m.recipient='root' AND m.project_id='alpha' AND m.task_id=?`, po, task).Scan(&id, &room); err != nil {
		t.Fatal(err)
	}
	if room != directRoom(po, root) {
		t.Fatalf("room=%q", room)
	}
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", task).Scan(&status); err != nil || status != "assigned" {
		t.Fatalf("status=%s %v", status, err)
	}
	var leaks int
	if err := s.db.QueryRow("SELECT count(*) FROM messages WHERE text IN ('Work is underway.','Routine PO task update.') AND recipient IN ('operator','group:alpha')").Scan(&leaks); err != nil || leaks != 0 {
		t.Fatalf("leaks=%d %v", leaks, err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		ctx, err := s.runtimeContext(tx, agent{ID: root, Role: "root"}, "", "", "", "operator_message")
		if err != nil {
			return err
		}
		found := false
		for _, m := range ctx {
			if m.ID == privateDM || m.ID == privateReply {
				t.Fatal("ordinary operator DM leaked into Root context")
			}
			if m.ID == id && strings.Contains(m.Text, task) && strings.Contains(m.Text, "alpha") {
				found = true
			}
		}
		if !found {
			t.Fatal("Root did not receive project/task progress")
		}
		other, err := s.runtimeContext(tx, agent{ID: po, Role: "po", Project: "alpha"}, "alpha", "", "", groupMessageKind)
		if err != nil {
			return err
		}
		for _, m := range other {
			if m.ID == id {
				t.Fatal("group context leaked private progress")
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestPOProgressRejectsCrossProject(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report_progress", Project: "other", Body: "Wrong scope"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM po_progress_notes").Scan(&n); err != nil || n != 0 {
		t.Fatalf("notes=%d %v", n, err)
	}
}
