package service

import (
	"database/sql"
	"fmt"
	"testing"
)

func TestParentAmbiguityRecovery(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		s, parent, _, worker := parentReportFixture(t)
		installReviewInspector(t, s)
		var po string
		if err := s.db.QueryRow(`SELECT po_id FROM projects WHERE id='alpha'`).Scan(&po); err != nil {
			t.Fatal(err)
		}
		worker2, child, receipt := newID(), newID(), newID()
		if _, err := s.db.Exec(`INSERT INTO agents VALUES(?,'Second','worker','alpha','model','medium')`, worker2); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO tasks VALUES(?,'alpha','second','completed',?)`, child, po); err != nil {
			t.Fatal(err)
		}
		if _, err := s.db.Exec(`INSERT INTO po_work_items VALUES(?,?)`, parent, child); err != nil {
			t.Fatal(err)
		}
		if err := s.write(func(tx *sql.Tx) error {
			if err := addMessage(tx, receipt, worker2, po, "worker", "receipt", "alpha", child); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'review','completed',?,0)`, newID(), po, child, receipt, []byte("{}")); err != nil {
				return err
			}
			return addMessage(tx, newID(), po, po, "po", "reviewed", "alpha", child)
		}); err != nil {
			t.Fatal(err)
		}
		payload := fmt.Sprintf(`{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"child","body":"accepted","recipient":%q},{"type":"report","project":"alpha","title":"child","body":"reviewed","recipient":""},{"type":"accept_result","project":"alpha","title":"second","body":"accepted","recipient":%q},{"type":"report","project":"alpha","title":"second","body":"reviewed","recipient":""}],"attachments":[]}`, worker, worker2)
		if _, err := s.db.Exec(`UPDATE jobs SET diagnostic='invalid runtime result: completed child recipient is ambiguous',result=? WHERE id=(SELECT id FROM jobs WHERE task_id=? ORDER BY rowid DESC LIMIT 1)`, []byte(payload), parent); err != nil {
			t.Fatal(err)
		}
		if stopped {
			if _, err := s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`); err != nil {
				t.Fatal(err)
			}
		}
		id := newID()
		want := 202
		if stopped {
			want = 409
		}
		if got := postContinuationID(s, parent, "Continue independent work.", id); got != want {
			t.Fatalf("stopped=%v status=%d", stopped, got)
		}
		if !stopped {
			if got := postContinuationID(s, parent, "Continue independent work.", id); got != 202 {
				t.Fatalf("retry=%d", got)
			}
			follow, err := s.claim(nil)
			if err != nil || follow == nil || !follow.Request.ParentPlanning {
				t.Fatalf("planning=%+v err=%v", follow, err)
			}
		}
		_ = s.Close()
	}
}
