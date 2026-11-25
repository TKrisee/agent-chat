package service

import (
	"context"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

// A PO can build a team, run independent work, and receive the parent back
// only after both reviewed children complete.
func TestPOTeamBacklogRoundTrip(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if _, err := s.userMessage(context.Background(), newID(), root, "", "Create a project to review approved art and map sources.", "", nil); err != nil {
		t.Fatal(err)
	}
	rootJob, err := s.claim(nil)
	if err != nil || rootJob == nil || rootJob.Kind != "operator_message" {
		t.Fatalf("root %+v: %v", rootJob, err)
	}
	if err := s.complete(*rootJob, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "create_project", Project: "alpha", Title: "Review approved sources", Body: "Review approved art and map sources."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	scope, err := s.claim(nil)
	if err != nil || scope == nil || scope.Kind != "scope_review" {
		t.Fatalf("scope %+v: %v", scope, err)
	}
	parent, po := scope.Task, scope.Agent
	if err := s.complete(*scope, runtimeapi.Result{Text: "The sources can be checked independently."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	decision, err := s.claim(nil)
	if err != nil || decision == nil || decision.Kind != "scope_decision" {
		t.Fatalf("decision %+v: %v", decision, err)
	}
	if err := s.complete(*decision, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "Review approved sources", Body: "Review art and map sources, then report."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("INSERT INTO project_workspace_roots(project_id,path) VALUES('alpha',?)", t.TempDir()); err != nil {
		t.Fatal(err)
	}
	assignmentJob, err := s.claim(nil)
	if err != nil || assignmentJob == nil || assignmentJob.Kind != "assignment" || assignmentJob.Agent != po {
		t.Fatalf("assignment %+v: %v", assignmentJob, err)
	}
	assignment := *assignmentJob
	actions := []runtimeapi.Action{
		{Type: "create_worker", Title: "Character-Art"},
		{Type: "create_worker", Title: "World"},
		{Type: "queue_work_item", Title: "art", Body: "Inspect the approved art.", Recipient: "Character-Art"},
	}
	if err := s.complete(assignment, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	first, err := s.claim(nil)
	if err != nil || first == nil || first.Kind != "work" {
		t.Fatalf("first work %+v: %v", first, err)
	}
	planning := followupClaim(t, s)
	if err := s.complete(planning, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Title: "map", Body: "Inspect the approved map.", Recipient: "World"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	second, err := s.claim(nil)
	if err != nil || second == nil || second.Kind != "work" {
		t.Fatalf("second work %+v: %v", second, err)
	}
	wait := followupClaim(t, s)
	if err := s.complete(wait, runtimeapi.Result{Text: "Waiting for both results."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var count, owners int
	if err := s.db.QueryRow(`SELECT count(*),count(DISTINCT t.owner_id) FROM po_work_items i JOIN tasks t ON t.id=i.child_task_id WHERE i.parent_task_id=? AND t.status='queued'`, parent).Scan(&count, &owners); err != nil || count != 2 || owners != 2 {
		t.Fatalf("children=%d owners=%d err=%v", count, owners, err)
	}
	var parentStatus string
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", parent).Scan(&parentStatus); err != nil || parentStatus != "assigned" {
		t.Fatalf("parent %s: %v", parentStatus, err)
	}
	workers := []job{*first, *second}
	if workers[0].Agent == workers[1].Agent || workers[0].Request.Workspace == workers[1].Request.Workspace {
		t.Fatal("workers did not receive distinct owners and workspaces")
	}
	var artID, model string
	if err := s.db.QueryRow("SELECT id,model FROM agents WHERE project_id='alpha' AND name='Character-Art'").Scan(&artID, &model); err != nil || model != s.opts.WorkerModel {
		t.Fatalf("worker model %q: %v", model, err)
	}
	for _, w := range workers {
		if err := s.complete(w, runtimeapi.Result{Text: "Evidence gathered."}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
		r, err := s.claim(nil)
		if err != nil || r == nil || r.Kind != "review" || r.Task != w.Task {
			t.Fatalf("review %+v: %v", r, err)
		}
		if err := s.complete(*r, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha"}, {Type: "report", Project: "alpha", Recipient: root, Body: "Reviewed evidence."}}}, nil, nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	var completed int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items i JOIN tasks t ON t.id=i.child_task_id WHERE i.parent_task_id=? AND t.status='completed'`, parent).Scan(&completed); err != nil || completed != 2 {
		t.Fatalf("completed=%d: %v", completed, err)
	}
	var pending int
	if err := s.db.QueryRow(`SELECT count(*) FROM jobs WHERE task_id=? AND kind='po_followup' AND status='pending'`, parent).Scan(&pending); err != nil || pending != 1 {
		t.Fatalf("parent followups=%d: %v", pending, err)
	}
	follow := followupClaim(t, s)
	if follow.Task != parent {
		t.Fatal("followup belongs to another parent")
	}
	if err := s.complete(follow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_work_item", Title: "art followup", Body: "Check one remaining approved source.", Recipient: "Character-Art"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	third, err := s.claim(nil)
	if err != nil || third == nil || third.Kind != "work" || third.Agent != artID {
		t.Fatalf("follow-on worker %+v: %v", third, err)
	}
	wait = followupClaim(t, s)
	if err := s.complete(wait, runtimeapi.Result{Text: "Waiting for the follow-on result."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.complete(*third, runtimeapi.Result{Text: "Follow-on evidence."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	review, err := s.claim(nil)
	if err != nil || review == nil || review.Kind != "review" || review.Task != third.Task {
		t.Fatalf("follow-on review %+v: %v", review, err)
	}
	if err := s.complete(*review, runtimeapi.Result{Text: "Reviewing the evidence."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	childFollow := followupClaim(t, s)
	if childFollow.Task != third.Task {
		t.Fatal("followup belongs to wrong child")
	}
	var assignmentSeen, receiptSeen bool
	for _, m := range childFollow.Request.Context {
		if m.ID == assignment.Source {
			assignmentSeen = true
		}
		if m.ID == review.Source {
			receiptSeen = true
		}
	}
	if !assignmentSeen || !receiptSeen {
		t.Fatalf("followup parent assignment=%v child receipt=%v", assignmentSeen, receiptSeen)
	}
	if err := s.complete(childFollow, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Project: "alpha"}, {Type: "report", Project: "alpha", Recipient: root, Body: "Follow-on reviewed."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	final := followupClaim(t, s)
	if err := s.complete(final, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report", Project: "alpha", Recipient: root, Body: "All approved work complete."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow("SELECT status FROM tasks WHERE id=?", parent).Scan(&parentStatus); err != nil || parentStatus != "completed" {
		var diagnostic string
		_ = s.db.QueryRow("SELECT COALESCE(diagnostic,'') FROM jobs WHERE id=?", final.ID).Scan(&diagnostic)
		t.Fatalf("final parent %s: %v; diagnostic=%s", parentStatus, err, diagnostic)
	}
	var reports int
	if err := s.db.QueryRow("SELECT count(*) FROM jobs WHERE task_id=? AND kind='project_report'", parent).Scan(&reports); err != nil || reports != 1 {
		t.Fatalf("root reports=%d: %v", reports, err)
	}
}
