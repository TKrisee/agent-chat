package service

import (
	"context"
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestGroupReplyWakesOriginalAgent(t *testing.T) {
	s, _, po, worker := continuationFixture(t)
	defer s.Close()
	parent, serviceParent, operatorParent := newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		child := newID()
		if _, err := tx.Exec(`INSERT INTO tasks VALUES(?,'alpha','one-shot child','completed',?)`, child, po); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO one_shot_workers(agent_id,child_task_id) VALUES(?,?)`, worker, child); err != nil {
			return err
		}
		if err := addMessage(tx, parent, worker, "group:alpha", "worker", "original", "alpha", ""); err != nil {
			return err
		}
		if err := addMessage(tx, serviceParent, "service", "group:alpha", "system", "notice", "alpha", ""); err != nil {
			return err
		}
		return addMessage(tx, operatorParent, operator, "group:alpha", "operator", "original", "alpha", "")
	}); err != nil {
		t.Fatal(err)
	}
	id := newID()
	for i := 0; i < 2; i++ {
		created, err := s.userMessage(context.Background(), id, "", "alpha", "reply", parent, nil)
		if err != nil || created != (i == 0) {
			t.Fatalf("created=%v err=%v", created, err)
		}
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM message_mentions WHERE message_id=?`, id).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invented mention: %d %v", count, err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Agent != worker || j.Kind != groupMessageKind {
		t.Fatalf("target %v %v", j, err)
	}
	if err := s.complete(*j, runtimeapi.Result{Text: "answer"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM messages m JOIN message_replies r ON r.message_id=m.id WHERE m.sender=? AND r.parent_id=?`, worker, id).Scan(&count); err != nil || count != 1 {
		t.Fatalf("reply outcome %d %v", count, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE agent_id=? AND kind='work'`, worker).Scan(&count); err != nil || count != 0 {
		t.Fatalf("reply dispatched work: %d %v", count, err)
	}
	explicit := newID()
	if _, err := s.userMessage(context.Background(), explicit, "", "alpha", "tagged", parent, []string{po}); err != nil {
		t.Fatal(err)
	}
	j, err = s.claim(nil)
	if err != nil || j == nil || j.Agent != po {
		t.Fatalf("explicit override %v %v", j, err)
	}
	if err := s.complete(*j, runtimeapi.Result{Text: "answer"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{serviceParent, operatorParent, ""} {
		if _, err := s.userMessage(context.Background(), newID(), "", "alpha", "no target", p, nil); err != nil {
			t.Fatal(err)
		}
		j, err = s.claim(nil)
		if err != nil || j == nil || j.Agent != po {
			t.Fatalf("PO fallback %v %v", j, err)
		}
		if err := s.complete(*j, runtimeapi.Result{Text: "answer"}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE kind=? AND status='pending'`, groupMessageKind).Scan(&count); err != nil || count != 0 {
		t.Fatalf("unaddressable reply dispatched: %d %v", count, err)
	}
	if _, err := s.userMessage(context.Background(), newID(), "", "other", "cross room", parent, nil); err == nil {
		t.Fatal("cross-room reply accepted")
	}
}
