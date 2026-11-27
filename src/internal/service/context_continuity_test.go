package service

import (
	"bytes"
	"context"
	"database/sql"
	"testing"
)

func TestFollowupPinsOnlySameTaskSuccessfulReceipt(t *testing.T) {
	s, review, _, worker, other := childReviewFixture(t)
	defer s.Close()
	source := newID()
	if err := s.write(func(tx *sql.Tx) error {
		assignment, err := s.poAssignmentSource(tx, review, review.Agent)
		if err != nil {
			return err
		}
		if err := addMessage(tx, source, "service", review.Agent, "system", "continue review", review.Project, review.Task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id) VALUES(?,?,1,'review',?)`, source, review.ID, assignment); err != nil {
			return err
		}
		if err := addMessage(tx, newID(), other, review.Agent, "worker", "unproven foreign receipt", review.Project, review.Task); err != nil {
			return err
		}
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), review.Agent, review.Agent, "po", "routine", review.Project, review.Task); err != nil {
				return err
			}
		}
		po, err := getAgent(tx, review.Agent)
		if err != nil {
			return err
		}
		return s.enqueue(tx, po, review.Project, review.Task, source, "po_followup", "continue")
	}); err != nil {
		t.Fatal(err)
	}
	var before []byte
	if err := s.db.QueryRow(`SELECT request FROM jobs WHERE source_id=? AND kind='po_followup'`, source).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(before, []byte(review.Source)) != 1 || bytes.Contains(before, []byte("unproven foreign receipt")) {
		t.Fatal("receipt missing, duplicated or foreign message exposed")
	}
	if _, err := s.db.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','uncertain',?,0)`, newID(), worker, review.Task, review.Source, []byte("{}")); err != nil {
		t.Fatal(err)
	}
	uncertainSource := newID()
	if err := s.write(func(tx *sql.Tx) error {
		assignment, err := s.poAssignmentSource(tx, review, review.Agent)
		if err != nil {
			return err
		}
		if err := addMessage(tx, uncertainSource, "service", review.Agent, "system", "new followup", review.Project, review.Task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id) VALUES(?,?,2,'review',?)`, uncertainSource, review.ID, assignment); err != nil {
			return err
		}
		po, err := getAgent(tx, review.Agent)
		if err != nil {
			return err
		}
		return s.enqueue(tx, po, review.Project, review.Task, uncertainSource, "po_followup", "continue")
	}); err != nil {
		t.Fatal(err)
	}
	var uncertainRequest []byte
	if err := s.db.QueryRow(`SELECT request FROM jobs WHERE source_id=?`, uncertainSource).Scan(&uncertainRequest); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(uncertainRequest, []byte(review.Source)) {
		t.Fatal("uncertain worker receipt newly pinned")
	}
	if _, err := s.db.Exec(`UPDATE jobs SET status='completed' WHERE source_id=?`, uncertainSource); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE jobs SET status='completed' WHERE id=?`, review.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE jobs SET status='submitted' WHERE source_id=? AND kind='po_followup'`, source); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.claim(nil)
	if err != nil || claimed == nil || claimed.Source != source || claimed.Status != "submitted" {
		t.Fatalf("recovery claim: %v", err)
	}
	var after []byte
	if err := s.db.QueryRow(`SELECT request FROM jobs WHERE source_id=? AND kind='po_followup'`, source).Scan(&after); err != nil || !bytes.Equal(before, after) {
		t.Fatalf("saved request changed: %v", err)
	}
}

func TestOperatorReplyPinsOnlySameRoomThread(t *testing.T) {
	s, _, _, worker := continuationFixture(t)
	defer s.Close()
	ancestor, parent, source, foreign := newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, foreign, operator, root, "operator", "foreign private observation", "", ""); err != nil {
			return err
		}
		if err := addMessage(tx, ancestor, operator, worker, "operator", "earlier explicit observation", "alpha", ""); err != nil {
			return err
		}
		if err := addMessage(tx, parent, worker, operator, "worker", "prior answer", "alpha", ""); err != nil {
			return err
		}
		for _, edge := range [][2]string{{ancestor, foreign}, {parent, ancestor}} {
			if _, err := tx.Exec(`INSERT INTO message_replies(message_id,parent_id) VALUES(?,?)`, edge[0], edge[1]); err != nil {
				return err
			}
		}
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), operator, worker, "operator", "unrelated newer message", "alpha", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.userMessage(context.Background(), source, worker, "", "current reply", parent, nil); err != nil {
		t.Fatal(err)
	}
	var saved []byte
	if err := s.db.QueryRow(`SELECT request FROM jobs WHERE source_id=? AND kind='operator_message'`, source).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(saved, []byte("earlier explicit observation")) != 1 || bytes.Count(saved, []byte("prior answer")) != 1 || bytes.Contains(saved, []byte("foreign private observation")) {
		t.Fatal("thread pin was missing, duplicated or crossed rooms")
	}
}

func TestRootReplyPinsProjectLinkedSameRoomThread(t *testing.T) {
	s, task, _, _ := continuationFixture(t)
	defer s.Close()
	ancestor, parent, source := newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, ancestor, operator, root, "operator", "earlier project decision", "alpha", task); err != nil {
			return err
		}
		if err := addMessage(tx, parent, root, operator, "root", "project escalation", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO message_replies(message_id,parent_id) VALUES(?,?)`, parent, ancestor); err != nil {
			return err
		}
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), operator, root, "operator", "newer general message", "", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.userMessage(context.Background(), source, root, "", "reply to escalation", parent, nil); err != nil {
		t.Fatal(err)
	}
	var saved []byte
	if err := s.db.QueryRow(`SELECT request FROM jobs WHERE source_id=? AND kind='operator_message'`, source).Scan(&saved); err != nil {
		t.Fatal(err)
	}
	if bytes.Count(saved, []byte("earlier project decision")) != 1 || bytes.Count(saved, []byte("project escalation")) != 1 {
		t.Fatal("same-room project thread was not pinned exactly once")
	}
}
