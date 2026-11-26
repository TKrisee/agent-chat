package service

import (
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestWorkDependenciesWaitForReviewedCompletion(t *testing.T) {
	s, parent, po, worker := continuationFixture(t)
	defer s.Close()
	other := newID()
	if _, err := s.db.Exec(`INSERT INTO agents VALUES(?,'Dependency worker','worker','alpha','model','medium')`, other); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO project_workspace_roots(project_id,path) VALUES('alpha',?)`, t.TempDir()); err != nil {
		t.Fatal(err)
	}
	assignment := activeAssignment(t, s, parent, po)
	actions := []runtimeapi.Action{
		{Type: "queue_work_item", Title: "Prerequisite", Body: "Investigate", Recipient: other},
		{Type: "queue_work_item", Title: "Dependent", Body: "Use reviewed outcome", Recipient: worker, DependsOn: []string{"Prerequisite"}},
		{Type: "queue_work_item", Title: "Independent", Body: "Work independently", Recipient: worker},
	}
	if err := s.complete(assignment, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var prerequisite, dependent string
	if err := s.db.QueryRow(`SELECT id FROM tasks WHERE title='Prerequisite'`).Scan(&prerequisite); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT id FROM tasks WHERE title='Dependent'`).Scan(&dependent); err != nil {
		t.Fatal(err)
	}
	state, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range state.Tasks {
		if task.ID == dependent && !task.WaitingOnDependencies {
			t.Fatal("dependent not marked waiting")
		}
	}
	first, err := s.claim(map[string]bool{po: true})
	if err != nil || first == nil || first.Task != prerequisite {
		t.Fatalf("prerequisite claim %+v: %v", first, err)
	}
	independent, err := s.claim(map[string]bool{po: true, other: true})
	if err != nil || independent == nil || independent.Task == dependent {
		t.Fatalf("independent claim %+v: %v", independent, err)
	}
	if err := s.complete(*first, runtimeapi.Result{Text: "Finished"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if job, err := s.claim(map[string]bool{po: true, worker: true}); err != nil || job != nil {
		t.Fatalf("unreviewed dependent claimed %+v: %v", job, err)
	}
	planning, err := s.claim(map[string]bool{worker: true})
	if err != nil || planning == nil || planning.Kind != "po_followup" {
		t.Fatalf("planning wake: %v", err)
	}
	if err := s.complete(*planning, runtimeapi.Result{Text: "Waiting for outcomes"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	review, err := s.claim(map[string]bool{worker: true})
	if err != nil || review == nil || review.Kind != "review" {
		t.Fatalf("review %+v: %v", review, err)
	}
	if err := s.complete(*review, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Recipient: other}, {Type: "report", Body: "Reviewed"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE jobs SET status='completed' WHERE id=?`, independent.ID); err != nil {
		t.Fatal(err)
	}
	job, err := s.claim(map[string]bool{po: true})
	if err != nil || job == nil || job.Task != dependent {
		t.Fatalf("reviewed dependency did not release work %+v: %v", job, err)
	}
}

func TestWorkDependenciesRejectUnknownReferenceAtomically(t *testing.T) {
	s, parent, po, worker := continuationFixture(t)
	defer s.Close()
	assignment := activeAssignment(t, s, parent, po)
	result := runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_work_item", Title: "First", Body: "Work", Recipient: worker},
		{Type: "queue_work_item", Title: "Bad", Body: "Work", Recipient: worker, DependsOn: []string{"Missing"}},
	}}
	if err := s.complete(assignment, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, parent).Scan(&count); err != nil || count != 0 {
		t.Fatalf("invalid batch left %d items: %v", count, err)
	}
}

func TestBlockedPrerequisiteWakesParentAndKeepsDependentWaiting(t *testing.T) {
	s, parent, po, worker := continuationFixture(t)
	defer s.Close()
	assignment := activeAssignment(t, s, parent, po)
	if err := s.complete(assignment, runtimeapi.Result{Actions: []runtimeapi.Action{
		{Type: "queue_work_item", Title: "First", Body: "Work", Recipient: worker},
		{Type: "queue_work_item", Title: "Later", Body: "Work", Recipient: worker, DependsOn: []string{"First"}},
	}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	planning, err := s.claim(map[string]bool{worker: true})
	if err != nil || planning == nil || planning.Kind != "po_followup" {
		t.Fatalf("planning: %v", err)
	}
	if err := s.complete(*planning, runtimeapi.Result{Text: "Waiting"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	work, err := s.claim(map[string]bool{po: true})
	if err != nil || work == nil || work.Kind != "work" {
		t.Fatalf("work: %v", err)
	}
	if err := s.complete(*work, runtimeapi.Result{Text: "Done"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	review, err := s.claim(map[string]bool{worker: true})
	if err != nil || review == nil || review.Kind != "review" {
		t.Fatalf("review: %v", err)
	}
	if err := s.complete(*review, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report", Body: "Invalid before acceptance"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var status string
	if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, work.Task).Scan(&status); err != nil || status != "blocked" {
		t.Fatalf("predecessor %s: %v", status, err)
	}
	if job, err := s.claim(map[string]bool{po: true}); err != nil || job != nil {
		t.Fatalf("dependent escaped blocked prerequisite: %v", err)
	}
	wake, err := s.claim(map[string]bool{worker: true})
	if err != nil || wake == nil || wake.Kind != "po_followup" || wake.Task != parent {
		t.Fatalf("parent not woken after blocker: %v", err)
	}
}
