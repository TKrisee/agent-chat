package service

import (
	"database/sql"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func parentReportFixture(t *testing.T) (*Service, string, string, string) {
	t.Helper()
	s, parent, po, worker := continuationFixture(t)
	child, workSource, receipt, review, prior, failed, source := newID(), newID(), newID(), newID(), newID(), newID(), newID()
	err := s.write(func(tx *sql.Tx) error {
		var assignment, assignmentJob string
		if err := tx.QueryRow(`SELECT source_id,id FROM jobs WHERE task_id=? AND kind='assignment'`, parent).Scan(&assignment, &assignmentJob); err != nil {
			return err
		}
		uncertain := newID()
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'scope_review','uncertain',?,0)`, uncertain, po, parent, assignment, []byte("{}")); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO scope_draft_confirmations(request_id,task_id,review_job_id,draft_message_id,assignment_job_id,confirmed_by,effects_unknown_ack,created_at) VALUES(?,?,?,?,?,'operator',1,0)`, newID(), parent, uncertain, assignment, assignmentJob); err != nil {
			return err
		}
		if _, err := tx.Exec(`UPDATE tasks SET status='blocked' WHERE id=?`, parent); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO tasks VALUES(?,'alpha','child','completed',?)`, child, po); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO po_work_items VALUES(?,?)`, parent, child); err != nil {
			return err
		}
		if err := addMessage(tx, workSource, po, worker, "po", "work", "alpha", child); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','completed',?,0)`, newID(), worker, child, workSource, []byte("{}")); err != nil {
			return err
		}
		if err := addMessage(tx, receipt, worker, po, "worker", "worker receipt", "alpha", child); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'review','completed',?,0)`, review, po, child, receipt, []byte("{}")); err != nil {
			return err
		}
		if err := addMessage(tx, newID(), po, po, "po", "accepted child report", "alpha", child); err != nil {
			return err
		}
		if err := addMessage(tx, source, "service", po, "system", "recorded child", "alpha", parent); err != nil {
			return err
		}
		priorSource := newID()
		if err := addMessage(tx, priorSource, "service", po, "system", "prior", "alpha", parent); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'po_followup','completed',?,0)`, prior, po, parent, priorSource, []byte("{}")); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id) VALUES(?,?,1,'assigned',?)`, source, prior, assignment); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,diagnostic,next_at) VALUES(?,?,'alpha',?,?,'po_followup','completed',?,'invalid runtime result: active worker result unavailable',0)`, failed, po, parent, source, []byte("{}"))
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	return s, parent, child, worker
}

func TestParentReportRecovery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Service, string, string)
		want   int
	}{
		{"valid", nil, 202},
		{"stopped", func(s *Service, p, c string) { _, _ = s.db.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`) }, 409},
		{"incomplete child", func(s *Service, p, c string) { _, _ = s.db.Exec(`UPDATE tasks SET status='review' WHERE id=?`, c) }, 409},
		{"uncertain child", func(s *Service, p, c string) {
			_, _ = s.db.Exec(`UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='work'`, c)
		}, 409},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, parent, child, _ := parentReportFixture(t)
			defer s.Close()
			if tc.mutate != nil {
				tc.mutate(s, parent, child)
			}
			id := newID()
			if got := postContinuationID(s, parent, "Report the existing receipt.", id); got != tc.want {
				t.Fatalf("status %d", got)
			}
			if tc.want != 202 {
				return
			}
			if got := postContinuationID(s, parent, "Report the existing receipt.", id); got != 202 {
				t.Fatalf("idempotency %d", got)
			}
			var uncertain int
			if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='scope_review' AND status='uncertain'`, parent).Scan(&uncertain); err != nil || uncertain != 1 {
				t.Fatalf("prior uncertainty lost: %d %v", uncertain, err)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil {
				t.Fatalf("claim %v %v", j, err)
			}
			if !strings.Contains(j.Request.Instructions, "accepted child report") {
				t.Fatal("missing exact child report")
			}
			if !j.Request.ReportOnly {
				t.Fatal("missing report-only runtime constraint")
			}
			if err := s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Title: "new", Body: "repeat", Recipient: "Worker"}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var n int
			_ = s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='work'`, child).Scan(&n)
			if n != 1 {
				t.Fatalf("replayed work: %d", n)
			}
		})
	}
}
