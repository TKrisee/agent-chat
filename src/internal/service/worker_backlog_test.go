package service

import (
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestWorkerBacklogSerializesAndFencesUncertainty(t *testing.T) {
	for _, scenario := range []string{"completes", "becomes-uncertain", "already-uncertain"} {
		t.Run(scenario, func(t *testing.T) {
			s, parent, po, firstWorker := continuationFixture(t)
			defer s.Close()
			secondWorker := newID()
			if _, err := s.db.Exec("INSERT INTO agents VALUES(?,'Other','worker','alpha','model','medium')", secondWorker); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("INSERT INTO project_workspace_roots(project_id,path) VALUES('alpha',?)", t.TempDir()); err != nil {
				t.Fatal(err)
			}
			assignment := activeAssignment(t, s, parent, po)
			actions := []runtimeapi.Action{{Type: "queue_work_item", Title: "First", Body: "First work", Recipient: firstWorker}, {Type: "queue_work_item", Title: "Later", Body: "Later work", Recipient: firstWorker}, {Type: "queue_work_item", Title: "Independent", Body: "Independent work", Recipient: secondWorker}}
			if err := s.complete(assignment, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var firstID, laterID, otherID string
			for _, entry := range []struct {
				title string
				id    *string
			}{{"First", &firstID}, {"Later", &laterID}, {"Independent", &otherID}} {
				if err := s.db.QueryRow(`SELECT j.id FROM jobs j JOIN tasks t ON t.id=j.task_id WHERE t.title=? AND j.kind='work'`, entry.title).Scan(entry.id); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "already-uncertain" {
				if _, err := s.db.Exec("UPDATE jobs SET status='uncertain' WHERE id=?", firstID); err != nil {
					t.Fatal(err)
				}
			}
			active := map[string]bool{po: true}
			one, err := s.claim(active)
			if err != nil || one == nil {
				t.Fatalf("first claim %+v: %v", one, err)
			}
			if scenario == "already-uncertain" {
				if one.ID != otherID {
					t.Fatalf("uncertain worker was claimed: %+v", one)
				}
				active[secondWorker] = true
				if j, e := s.claim(active); e != nil || j != nil {
					t.Fatalf("later escaped uncertainty: %+v %v", j, e)
				}
				return
			}
			active[one.Agent] = true
			two, err := s.claim(active)
			if err != nil || two == nil {
				t.Fatalf("second claim %+v: %v", two, err)
			}
			if !((one.ID == firstID && two.ID == otherID) || (one.ID == otherID && two.ID == firstID)) {
				t.Fatalf("claimed %s and %s, want first and independent", one.ID, two.ID)
			}
			active[two.Agent] = true
			if j, e := s.claim(active); e != nil || j != nil {
				t.Fatalf("same worker ran concurrently: %+v %v", j, e)
			}
			if scenario == "becomes-uncertain" {
				if _, err := s.db.Exec("UPDATE jobs SET status='uncertain' WHERE id=?", firstID); err != nil {
					t.Fatal(err)
				}
				delete(active, firstWorker)
				if j, e := s.claim(active); e != nil || j != nil {
					t.Fatalf("later escaped new uncertainty: %+v %v", j, e)
				}
				return
			}
			if _, err := s.db.Exec("UPDATE jobs SET status='completed' WHERE id=?", firstID); err != nil {
				t.Fatal(err)
			}
			delete(active, firstWorker)
			j, err := s.claim(active)
			if err != nil || j == nil || j.ID != laterID {
				t.Fatalf("later not admitted after completion: %+v %v", j, err)
			}
		})
	}
}

func TestWorkerBacklogRejectsAlreadyUncertain(t *testing.T) {
	s, parent, po, worker := continuationFixture(t)
	defer s.Close()
	source := newID()
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, source, po, worker, "po", "Earlier work", "alpha", parent); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) VALUES(?,?,'alpha',?,?,'work','uncertain',?,0)`, newID(), worker, parent, source, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	err := s.write(func(tx *sql.Tx) error {
		return s.queuePOWorkItem(tx, job{Project: "alpha", Task: parent}, agent{ID: po, Role: "po", Project: "alpha"}, runtimeapi.Action{Title: "New", Body: "New work", Recipient: worker})
	})
	if err == nil {
		t.Fatal("queued new work for an already uncertain worker")
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM po_work_items WHERE parent_task_id=?", parent).Scan(&n); err != nil || n != 0 {
		t.Fatalf("children=%d: %v", n, err)
	}
}

func TestLegacyBusyPlanningRecovery(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		want        int
	}{{"eligible", "", 202}, {"stop", "stop", 409}, {"uncertain child", "uncertain", 409}, {"unsettled child", "unsettled", 409}, {"approval", "approval", 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, parent, po, worker := continuationFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			if _, err := s.db.Exec("UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE task_id=? AND kind='assignment'", legacyTitledPlanningDiagnostic, []byte(titledRejectedResult), parent); err != nil {
				t.Fatal(err)
			}
			if _, err := s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", parent); err != nil {
				t.Fatal(err)
			}
			if code := postContinuation(s, parent, "Continue the approved assignment."); code != 202 {
				t.Fatalf("initial continuation %d", code)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil {
				t.Fatalf("claim %+v %v", j, err)
			}
			if err = s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Title: "First", Body: "Trace source", Recipient: worker}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			var child, work string
			if err = s.db.QueryRow("SELECT child_task_id FROM po_work_items WHERE parent_task_id=?", parent).Scan(&child); err != nil {
				t.Fatal(err)
			}
			if err = s.db.QueryRow("SELECT id FROM jobs WHERE task_id=? AND kind='work'", child).Scan(&work); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("UPDATE jobs SET status='completed' WHERE id=?", work); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("UPDATE tasks SET status='completed',owner_id=? WHERE id=?", po, child); err != nil {
				t.Fatal(err)
			}
			if err = s.write(func(tx *sql.Tx) error {
				if err := addMessage(tx, newID(), worker, po, "worker", "Receipt", "alpha", child); err != nil {
					return err
				}
				return addMessage(tx, newID(), po, po, "po", "Reviewed", "alpha", child)
			}); err != nil {
				t.Fatal(err)
			}
			var plan string
			if err = s.db.QueryRow("SELECT id FROM jobs WHERE task_id=? AND kind='po_followup' AND status='pending'", parent).Scan(&plan); err != nil {
				t.Fatal(err)
			}
			payload := `{"text":"","actions":[{"type":"queue_work_item","project":"alpha","title":"Next","body":"Trace next source.","recipient":"worker"}],"attachments":[]}`
			if _, err = s.db.Exec("UPDATE jobs SET status='completed',diagnostic=?,result=? WHERE id=?", legacyBusyPlanDiagnostic, []byte(payload), plan); err != nil {
				t.Fatal(err)
			}
			if _, err = s.db.Exec("UPDATE tasks SET status='blocked' WHERE id=?", parent); err != nil {
				t.Fatal(err)
			}
			switch tc.fence {
			case "stop":
				_, err = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "uncertain":
				_, err = s.db.Exec("UPDATE jobs SET status='uncertain' WHERE id=?", work)
			case "unsettled":
				_, err = s.db.Exec("UPDATE tasks SET status='review' WHERE id=?", child)
			case "approval":
				_, err = s.db.Exec("INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')", newID(), parent, po)
			}
			if err != nil {
				t.Fatal(err)
			}
			id := newID()
			if code := postContinuationID(s, parent, "Continue the approved backlog.", id); code != tc.want {
				t.Fatalf("continuation=%d want=%d", code, tc.want)
			}
			if tc.want == 202 {
				if code := postContinuationID(s, parent, "Continue the approved backlog.", id); code != 202 {
					t.Fatalf("idempotent=%d", code)
				}
				var n int
				if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE source_id=?", id).Scan(&n); err != nil || n != 1 {
					t.Fatalf("jobs=%d: %v", n, err)
				}
			}
		})
	}
}
