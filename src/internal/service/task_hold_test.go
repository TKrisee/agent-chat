package service

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func heldTaskFixture(t *testing.T) (*Service, string, string, string) {
	t.Helper()
	s, task, po, _ := continuationFixture(t)
	if err := s.write(func(tx *sql.Tx) error {
		source := newID()
		if err := addMessage(tx, source, po, root, "po", "old assessment", "alpha", task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,diagnostic,next_at) VALUES(?,?,'alpha',?,?,'scope_decision','completed',?,'invalid runtime result: task owner or status changed',0)`, newID(), root, task, source, []byte("{}"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if code := postContinuation(s, task, "Private operator-to-PO context."); code != 202 {
		t.Fatalf("continuation %d", code)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Kind != "po_continue" {
		t.Fatalf("PO claim %+v %v", j, err)
	}
	if err := s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Body: "A private decision is needed."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	rootJob, err := s.claim(nil)
	if err != nil || rootJob == nil || rootJob.Kind != "task_blocked" {
		t.Fatalf("Root escalation %+v %v", rootJob, err)
	}
	if err := s.complete(*rootJob, runtimeapi.Result{Text: "May I continue this held task?"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var rootMessage string
	if err := s.db.QueryRow("SELECT root_message_id FROM task_holds WHERE task_id=?", task).Scan(&rootMessage); err != nil {
		t.Fatal(err)
	}
	return s, task, po, rootMessage
}
func TestRootHoldResolutionFreshContextAndPrivacy(t *testing.T) {
	s, task, po, escalation := heldTaskFixture(t)
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error {
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), operator, root, "operator", "older operator text", "", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	id := newID()
	if _, err := s.userMessage(context.Background(), id, root, "", "go ahead", "", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		for i := 0; i < 30; i++ {
			if err := addMessage(tx, newID(), operator, root, "operator", "later operator text", "", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil || j.Kind != "operator_message" {
		t.Fatalf("Root claim %+v %v", j, err)
	}
	var pinned, sourcePinned bool
	for _, m := range j.Request.Context {
		if m.ID == escalation {
			pinned = true
		}
		if m.ID == id {
			sourcePinned = true
		}
		if m.Text == "Private operator-to-PO context." || m.Text == "A private decision is needed." {
			t.Fatal("private PO context leaked to Root")
		}
	}
	if !pinned || !sourcePinned || !strings.Contains(j.Request.Instructions, "active_hold=true") {
		t.Fatal("active hold or Root escalation missing")
	}
	if strings.Contains(j.Request.Instructions, "rejected_task_owner_or_status_changed") {
		t.Fatal("historical rejection presented as current")
	}
	result := runtimeapi.Result{Text: "I will ask the project owner to continue.", Actions: []runtimeapi.Action{{Type: "resolve_hold", Task: task, Project: "alpha", Body: "go ahead"}}}
	if err := s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		snapshot, err := s.statusSnapshot(tx)
		if err != nil {
			return err
		}
		if strings.Contains(snapshot, "rejected_task_owner_or_status_changed") {
			t.Fatal("old rejection resurfaced after resolution")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	next, err := s.claim(nil)
	if err != nil || next == nil || next.Kind != "po_followup" || next.Task != task || next.Agent != po {
		t.Fatalf("PO continuation %+v %v", next, err)
	}
	var old, newNote bool
	for _, m := range next.Request.Context {
		if m.Text == "Private operator-to-PO context." {
			old = true
		}
		if m.Text == "go ahead" && m.Sender == operator {
			newNote = true
		}
	}
	if !old || !newNote {
		t.Fatalf("PO instructions old=%v new=%v", old, newNote)
	}
	var n int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='po_followup'", task).Scan(&n); err != nil || n != 1 {
		t.Fatalf("duplicate %d %v", n, err)
	}
}

func TestHoldAuthorizationLanguage(t *testing.T) {
	task := "11111111-1111-4111-8111-111111111111"
	for _, text := range []string{"go ahead", "sure, go ahead", "I do not know what you are talking about re-signing but sure go ahead", "continue " + task + " with the already authorized plan", "continue " + task + " do not touch production"} {
		if !clearHoldAuthorization(text, task) {
			t.Errorf("rejected clear authorization %q", text)
		}
	}
	for _, text := range []string{"don't go ahead", "do not go ahead", "go ahead if approved", "go ahead when approved", "yes, but do not proceed", "no, but go ahead", "continue " + task + " do not proceed", "continue " + task + " no, do not approve", "do not approve but go ahead", "don’t approve but sure go ahead", "maybe", "no"} {
		if clearHoldAuthorization(text, task) {
			t.Errorf("accepted unclear authorization %q", text)
		}
	}
}
func TestLegacyStructuredHoldRecovery(t *testing.T) {
	s, task, _, _ := heldTaskFixture(t)
	defer s.Close()
	installReviewInspector(t, s)
	if _, err := s.db.Exec("DELETE FROM task_holds WHERE task_id=?", task); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		h, ok, err := s.activeTaskHold(tx, task)
		if err != nil {
			return err
		}
		if !ok || !h.legacy || h.rootMessage == "" {
			t.Fatalf("legacy hold not linked: %+v %v", h, ok)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	id := newID()
	text := "I do not know what you are talking about re-signing but sure go ahead"
	if _, err := s.userMessage(context.Background(), id, root, "", text, "", nil); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("claim %+v %v", j, err)
	}
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "resolve_hold", Project: "alpha", Task: task, Body: text}}}
	if err := s.complete(*j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next, err := s.claim(nil)
	if err != nil || next == nil || next.Kind != "po_followup" || next.Task != task {
		t.Fatalf("legacy resume %+v %v", next, err)
	}
	var found bool
	for _, m := range next.Request.Context {
		if m.Text == text && m.Sender == operator {
			found = true
		}
	}
	if !found {
		t.Fatal("operator answer not passed to PO")
	}
}
func TestHoldResolutionFences(t *testing.T) {
	for _, fence := range []string{"stopped", "approval", "uncertain", "ambiguous", "negative"} {
		t.Run(fence, func(t *testing.T) {
			s, task, po, escalation := heldTaskFixture(t)
			defer s.Close()
			id := newID()
			text := "go ahead"
			if fence == "negative" {
				text = "do not go ahead"
			}
			if err := s.write(func(tx *sql.Tx) error {
				if fence == "stopped" {
					_, err := tx.Exec("UPDATE projects SET stopped=1 WHERE id='alpha'")
					if err != nil {
						return err
					}
				}
				if fence == "approval" {
					_, err := tx.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'decision','body','pending')`, newID(), task, po)
					if err != nil {
						return err
					}
				}
				if fence == "uncertain" {
					_, err := tx.Exec(`UPDATE jobs SET status='uncertain' WHERE task_id=? AND kind='po_continue'`, task)
					if err != nil {
						return err
					}
				}
				if fence == "ambiguous" {
					if err := addMessage(tx, newID(), root, operator, "root", "Unrelated question", "", ""); err != nil {
						return err
					}
				}
				return addMessage(tx, id, operator, root, "operator", text, "", "")
			}); err != nil {
				t.Fatal(err)
			}
			err := s.write(func(tx *sql.Tx) error {
				return s.resolveHold(tx, job{Agent: root, Kind: "operator_message", Source: id}, runtimeapi.Action{Type: "resolve_hold", Task: task, Project: "alpha", Body: text})
			})
			if err == nil {
				t.Fatalf("%s accepted (escalation %s)", fence, escalation)
			}
			var status string
			if err := s.db.QueryRow("SELECT status FROM task_holds WHERE task_id=?", task).Scan(&status); err != nil || status != "active" {
				t.Fatalf("hold changed %q %v", status, err)
			}
		})
	}
}

func TestLegacyReviewHoldPreservesWorkerAndPriorNote(t *testing.T) {
	s, review, _ := reviewConflictFixture(t)
	defer s.Close()
	installReviewInspector(t, s)
	if err := s.complete(review, runtimeapi.Result{Text: "Reviewing."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	po := followupClaim(t, s)
	if err := s.complete(po, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "hold_task", Project: "alpha", Body: "Waiting for the operator decision."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	rootJob, err := s.claim(nil)
	if err != nil || rootJob == nil || rootJob.Kind != "task_blocked" {
		t.Fatalf("root escalation %+v %v", rootJob, err)
	}
	if err := s.complete(*rootJob, runtimeapi.Result{Text: "May I proceed with this task?"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("DELETE FROM task_holds WHERE task_id=?", review.Task); err != nil {
		t.Fatal(err)
	}
	id := newID()
	if _, err := s.userMessage(context.Background(), id, root, "", "go ahead", "", nil); err != nil {
		t.Fatal(err)
	}
	request, err := s.claim(nil)
	if err != nil || request == nil {
		t.Fatalf("request %+v %v", request, err)
	}
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "resolve_hold", Task: review.Task, Project: "alpha", Body: "go ahead"}}}
	if err := s.complete(*request, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	next, err := s.claim(nil)
	if err != nil || next == nil || next.Kind != "po_followup" {
		t.Fatalf("next %+v %v", next, err)
	}
	var receipt, prior, answer bool
	for _, m := range next.Request.Context {
		if m.ID == review.Source {
			receipt = true
		}
		if m.Text == "Use a fresh isolated workspace." {
			prior = true
		}
		if m.Text == "go ahead" && m.Sender == operator {
			answer = true
		}
	}
	if !receipt || !prior || !answer {
		t.Fatalf("receipt=%v prior=%v answer=%v", receipt, prior, answer)
	}
}
