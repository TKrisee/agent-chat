package service

import (
	"path/filepath"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestReviewCanReferencePinnedAttachmentWithoutReport(t *testing.T) {
	s, j, _, worker, _ := childReviewFixture(t)
	defer s.Close()
	id := newID()
	if _, err := s.db.Exec(`INSERT INTO attachments VALUES(?,?,?,'text/markdown',?)`, id, j.Source, "evidence", []byte("proof")); err != nil {
		t.Fatal(err)
	}
	pinned := runtimeapi.Attachment{Path: filepath.Join(s.dir, "attachments", id), Name: "evidence", MediaType: "text/markdown"}
	unrelated, err := s.uploadAttachments(s.db, j, runtimeapi.Result{Attachments: []runtimeapi.Attachment{pinned}})
	if err != nil || len(unrelated) != 1 {
		t.Fatalf("unrelated no-report result was exempted: %v %v", unrelated, err)
	}
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Recipient: worker}, {Type: "report_progress", Recipient: root, Body: "Receipt accepted."}}, Attachments: []runtimeapi.Attachment{pinned}}
	foreign := result
	foreign.Attachments = []runtimeapi.Attachment{{Path: filepath.Join(s.dir, "attachments", newID()), Name: "evidence", MediaType: "text/markdown"}}
	refs, err := s.uploadAttachments(s.db, j, foreign)
	if err != nil || len(refs) != 1 {
		t.Fatalf("foreign ref was exempted: %v %v", refs, err)
	}
	if _, err := readAttachments(j.Request.Workspace, refs); err == nil {
		t.Fatal("foreign managed path escaped workspace validation")
	}
	if err := s.complete(j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var status, diagnostic string
	if err := s.db.QueryRow(`SELECT status,COALESCE(diagnostic,'') FROM jobs WHERE id=?`, j.ID).Scan(&status, &diagnostic); err != nil || status != "completed" || diagnostic != "" {
		t.Fatalf("review %q diagnostic %q: %v", status, diagnostic, err)
	}
	var accepted, progress, links int
	if err := s.db.QueryRow(`SELECT count(*) FROM tasks WHERE id=? AND status='accepted'`, j.Task).Scan(&accepted); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE task_id=? AND sender=? AND recipient=? AND text='Receipt accepted.'`, j.Task, j.Agent, root).Scan(&progress); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM message_attachments WHERE attachment_id=?`, id).Scan(&links); err != nil {
		t.Fatal(err)
	}
	if accepted != 1 || progress != 1 || links != 1 {
		t.Fatalf("accepted=%d progress=%d attachment links=%d", accepted, progress, links)
	}
}
