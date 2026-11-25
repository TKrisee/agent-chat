package service

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func childReviewFixture(t *testing.T) (*Service, job, string, string, string) {
	t.Helper()
	s, parent, po, worker := continuationFixture(t)
	child, source, receipt, review, other := newID(), newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO agents VALUES(?,'Other','worker','alpha','model','medium')", other); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','child','review',?)", child, po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO po_work_items VALUES(?,?)", parent, child); err != nil {
			return err
		}
		if err := addMessage(tx, source, po, worker, "po", "work", "alpha", child); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','completed',?,0)`, newID(), worker, child, source, []byte("{}")); err != nil {
			return err
		}
		if err := addMessage(tx, receipt, worker, po, "worker", "receipt", "alpha", child); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'review','submitted',?,0)`, review, po, child, receipt, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return s, job{ID: review, Agent: po, Project: "alpha", Task: child, Source: receipt, Kind: "review", Request: runtimeapi.Request{Role: "po"}}, parent, worker, other
}

func TestLegacyChildReviewRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		want        int
	}{{"eligible", "", 202}, {"stop", "stop", 409}, {"wrong owner", "owner", 409}, {"uncertain", "uncertain", 409}, {"approval", "approval", 409}, {"wrong worker", "worker", 409}, {"unsafe action", "action", 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, j, parent, worker, other := childReviewFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			recipient := worker
			if tc.fence == "worker" {
				recipient = other
			}
			payload := fmt.Sprintf(`{"text":"","actions":[{"type":"accept_result","project":"alpha","title":"","body":"","recipient":%q},{"type":"report","project":"alpha","title":"review","body":"Reviewed.","recipient":%q},{"type":"queue_work_item","project":"alpha","title":"Sibling","body":"Independent work","recipient":%q}],"attachments":[]}`, worker, recipient, other)
			if tc.fence == "action" {
				payload = strings.Replace(payload, `"queue_work_item"`, `"stop_project"`, 1)
			}
			if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?", legacyChildReportDiagnostic, []byte(payload), j.ID); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", j.Task); err != nil {
				t.Fatal(err)
			}
			var err error
			switch tc.fence {
			case "stop":
				_, err = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "owner":
				_, err = s.db.Exec("UPDATE tasks SET owner_id=? WHERE id=?", other, parent)
			case "uncertain":
				_, err = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='work'", j.Task)
			case "approval":
				_, err = s.db.Exec("INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')", newID(), j.Task, j.Agent)
			}
			if err != nil {
				t.Fatal(err)
			}
			id := newID()
			if code := postContinuationID(s, j.Task, "Continue this exact child review.", id); code != tc.want {
				t.Fatalf("continuation=%d want=%d", code, tc.want)
			}
			if tc.want != 202 {
				return
			}
			if code := postContinuationID(s, j.Task, "Continue this exact child review.", id); code != 202 {
				t.Fatalf("retry=%d", code)
			}
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND source_id=?", j.Task, id).Scan(&count); err != nil || count != 1 {
				t.Fatalf("jobs=%d: %v", count, err)
			}
			follow, err := s.claim(nil)
			if err != nil || follow == nil || follow.Task != j.Task || follow.Kind != "po_followup" {
				t.Fatalf("followup %+v: %v", follow, err)
			}
			found := false
			for _, m := range follow.Request.Context {
				if m.ID == j.Source {
					found = true
				}
			}
			if !found {
				t.Fatal("exact worker receipt missing")
			}
		})
	}
}

func TestChildReviewReportAndSiblingWork(t *testing.T) {
	for _, tc := range []struct {
		name, recipient, fence string
		valid                  bool
	}{{"exact worker", "worker", "", true}, {"implicit", "", "", true}, {"wrong worker", "other", "", false}, {"stopped", "worker", "stop", false}, {"wrong parent owner", "worker", "owner", false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, j, parent, worker, other := childReviewFixture(t)
			defer s.Close()
			var err error
			switch tc.fence {
			case "stop":
				_, err = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "owner":
				_, err = s.db.Exec("UPDATE tasks SET owner_id=? WHERE id=?", other, parent)
			}
			if err != nil {
				t.Fatal(err)
			}
			recipient := tc.recipient
			if recipient == "worker" {
				recipient = worker
			}
			if recipient == "other" {
				recipient = other
			}
			result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Project: "alpha", Title: "Sibling", Body: "Independent work", Recipient: other}, {Type: "accept_result", Project: "alpha", Recipient: worker}, {Type: "report", Project: "alpha", Recipient: recipient, Body: "Reviewed."}}}
			if err = s.complete(j, result, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var siblings int
			if err = s.db.QueryRow("SELECT count(*) FROM po_work_items WHERE parent_task_id=?", parent).Scan(&siblings); err != nil {
				t.Fatal(err)
			}
			want := 1
			if tc.valid {
				want = 2
			}
			if siblings != want {
				t.Fatalf("children=%d want=%d", siblings, want)
			}
			var status string
			if err = s.db.QueryRow("SELECT status FROM tasks WHERE id=?", j.Task).Scan(&status); err != nil {
				t.Fatal(err)
			}
			if tc.valid && status != "completed" {
				t.Fatalf("child status %s", status)
			}
			if !tc.valid && status == "completed" {
				t.Fatal("rejected batch applied child completion")
			}
		})
	}
}
