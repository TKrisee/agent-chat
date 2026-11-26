package service

import (
	"database/sql"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func uncertainScopeDraftFixture(t *testing.T) (*Service, string, string, string, string) {
	t.Helper()
	s := openTest(t, t.TempDir())
	po, task, origin, draft, review := newID(), newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects VALUES('alpha','Alpha',?,'/tmp',0)", po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO agents VALUES(?, 'PO','po','alpha','model','medium')", po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,'alpha','draft','blocked',?)", task, po); err != nil {
			return err
		}
		if err := addMessage(tx, origin, operator, root, "operator", "Original request", "alpha", task); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO task_origins VALUES(?,?)", task, origin); err != nil {
			return err
		}
		if err := addMessage(tx, draft, root, po, "root", "Original Root draft", "alpha", task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,diagnostic,next_at) VALUES(?,?,'alpha',?,?,'scope_review','uncertain',?,'Native output contained an action outside the recorded role; inspect the result before any new run.',0)`, review, po, task, draft, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	return s, task, po, draft, review
}

func TestConfirmScopeDraftConcurrentOnce(t *testing.T) {
	s, task, _, draft, review := uncertainScopeDraftFixture(t)
	defer s.Close()
	codes := make([]int, 2)
	var wg sync.WaitGroup
	for i := range codes {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = confirmScopeDraft(t, s, task, newID(), review, draft, "effects-unknown")
		}(i)
	}
	wg.Wait()
	if !((codes[0] == 202 && codes[1] == 409) || (codes[0] == 409 && codes[1] == 202)) {
		t.Fatalf("concurrent codes=%v", codes)
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='assignment'", task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("assignments=%d: %v", n, err)
	}
}

func confirmScopeDraft(t *testing.T, s *Service, task, id, review, draft, confirmation string) int {
	t.Helper()
	v := url.Values{"id": {id}, "review_job_id": {review}, "draft_message_id": {draft}, "confirm": {confirmation}}
	r := httptest.NewRequest("POST", "/api/tasks/"+task+"/confirm-scope-draft", strings.NewReader(v.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.Handler(nil).ServeHTTP(w, r)
	return w.Code
}

func TestStateProjectsOnlyConfirmedScopeReviewSupersession(t *testing.T) {
	s, task, _, draft, review := uncertainScopeDraftFixture(t)
	defer s.Close()
	if code := confirmScopeDraft(t, s, task, newID(), review, draft, "effects-unknown"); code != 202 {
		t.Fatalf("confirmation status %d", code)
	}
	other := newID()
	if _, err := s.db.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at,diagnostic) SELECT ?,agent_id,project_id,task_id,source_id,'po_followup','uncertain',request,0,'unknown' FROM jobs WHERE id=?`, other, review); err != nil {
		t.Fatal(err)
	}
	state, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]Job{}
	for _, job := range state.Jobs {
		seen[job.ID] = job
	}
	if !seen[review].Superseded || seen[review].Status != "uncertain" || seen[other].Superseded || seen[other].Status != "uncertain" {
		t.Fatalf("unexpected receipt projection: review=%+v other=%+v", seen[review], seen[other])
	}
}

func TestConfirmScopeDraftPreservesUncertaintyAndFences(t *testing.T) {
	for _, tc := range []struct {
		name, fence string
		want        int
	}{{"confirmed", "", 202}, {"confirmation required", "confirm", 400}, {"stopped", "stop", 409}, {"owner changed", "owner", 409}, {"wrong phase", "phase", 409}, {"wrong rejection", "diagnostic", 409}, {"other uncertain", "uncertain", 409}, {"wrong draft", "draft", 409}, {"prior assignment", "assignment", 409}, {"prior work", "work", 409}, {"approval", "approval", 409}} {
		t.Run(tc.name, func(t *testing.T) {
			s, task, po, draft, review := uncertainScopeDraftFixture(t)
			defer s.Close()
			confirm := "effects-unknown"
			var err error
			switch tc.fence {
			case "confirm":
				confirm = ""
			case "stop":
				_, err = s.db.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
			case "owner":
				_, err = s.db.Exec("UPDATE tasks SET owner_id='root' WHERE id=?", task)
			case "phase":
				_, err = s.db.Exec("UPDATE tasks SET status='assigned' WHERE id=?", task)
			case "diagnostic":
				_, err = s.db.Exec("UPDATE jobs SET diagnostic='other' WHERE id=?", review)
			case "uncertain":
				_, err = s.db.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) SELECT ?,agent_id,project_id,task_id,source_id,'po_followup','uncertain',request,0 FROM jobs WHERE id=?`, newID(), review)
			case "draft":
				draft = newID()
			case "assignment", "work":
				_, err = s.db.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) SELECT ?,agent_id,project_id,task_id,source_id,?,'completed',request,0 FROM jobs WHERE id=?`, newID(), tc.fence, review)
			case "approval":
				_, err = s.db.Exec("INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')", newID(), task, po)
			}
			if err != nil {
				t.Fatal(err)
			}
			id := newID()
			if code := confirmScopeDraft(t, s, task, id, review, draft, confirm); code != tc.want {
				t.Fatalf("status=%d want=%d", code, tc.want)
			}
			if tc.want != 202 {
				return
			}
			if code := confirmScopeDraft(t, s, task, id, review, draft, confirm); code != 202 {
				t.Fatalf("idempotent=%d", code)
			}
			if code := confirmScopeDraft(t, s, task, newID(), review, draft, confirm); code != 409 {
				t.Fatalf("duplicate confirmation=%d", code)
			}
			var status string
			var confirmer string
			var acknowledged int
			if err := s.db.QueryRow("SELECT confirmed_by,effects_unknown_ack FROM scope_draft_confirmations WHERE request_id=?", id).Scan(&confirmer, &acknowledged); err != nil || confirmer != "operator" || acknowledged != 1 {
				t.Fatalf("audit=%q/%d: %v", confirmer, acknowledged, err)
			}
			if err := s.db.QueryRow("SELECT status FROM jobs WHERE id=?", review).Scan(&status); err != nil || status != "uncertain" {
				t.Fatalf("original status=%s %v", status, err)
			}
			var count int
			if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND agent_id=? AND kind='assignment' AND status='pending'", task, po).Scan(&count); err != nil || count != 1 {
				t.Fatalf("assignments=%d %v", count, err)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "assignment" || j.Source != draft || j.Task != task {
				t.Fatalf("same-task assignment %+v: %v", j, err)
			}
			if err = s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "create_worker", Title: "Audit-Worker"}, {Type: "queue_work_item", Title: "Audit evidence", Body: "Read the existing evidence without replay.", Recipient: "Audit-Worker"}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			if err = s.db.QueryRow("SELECT count(*) FROM po_work_items WHERE parent_task_id=?", task).Scan(&count); err != nil || count != 1 {
				t.Fatalf("new work items=%d: %v", count, err)
			}
		})
	}
}
