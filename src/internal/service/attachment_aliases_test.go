package service

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestPinnedAttachmentAliasFirstClaim(t *testing.T) {
	for _, tc := range []struct {
		name               string
		duplicate, foreign bool
	}{{"unique", false, false}, {"ambiguous", true, false}, {"foreign", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			s, j, _, worker, _ := childReviewFixture(t)
			defer s.Close()
			installReviewInspector(t, s)
			id := newID()
			path := "/old/worker/evidence.md"
			if _, err := s.db.Exec(`INSERT INTO attachments VALUES(?,?,?,'text/markdown',?)`, id, j.Source, "evidence", []byte("proof")); err != nil {
				t.Fatal(err)
			}
			if tc.duplicate {
				if _, err := s.db.Exec(`INSERT INTO attachments VALUES(?,?,?,'text/markdown',?)`, newID(), j.Source, "evidence", []byte("other")); err != nil {
					t.Fatal(err)
				}
			}
			declared := path
			payload := fmt.Sprintf(`{"text":"done","actions":[],"attachments":[{"path":%q,"name":"evidence","media_type":"text/markdown"}]}`, declared)
			if _, err := s.db.Exec(`UPDATE jobs SET result=? WHERE task_id=? AND agent_id=? AND kind='work'`, []byte(payload), j.Task, worker); err != nil {
				t.Fatal(err)
			}
			ref := runtimeapi.AttachmentRef{ID: id, Name: "evidence", MediaType: "text/markdown", Path: filepath.Join(s.dir, "attachments", id)}
			messageID := j.Source
			if tc.foreign {
				messageID = newID()
			}
			j.Request.Context = []runtimeapi.Message{{ID: messageID, Attachments: []runtimeapi.AttachmentRef{ref}}}
			if err := s.write(func(tx *sql.Tx) error { return s.attachmentAliases(tx, &j) }); err != nil {
				t.Fatal(err)
			}
			got := j.Request.Context[0].Attachments[0].SourcePath
			want := declared
			if tc.duplicate || tc.foreign {
				want = ""
			}
			if got != want {
				t.Fatalf("alias=%q want=%q", got, want)
			}
		})
	}
}

func TestFollowupRecognizesOnlyPinnedManagedAttachment(t *testing.T) {
	s, j, parent, _, _ := childReviewFixture(t)
	defer s.Close()
	attachment, followSource := newID(), newID()
	if _, err := s.db.Exec(`INSERT INTO attachments VALUES(?,?,?,'text/markdown',?)`, attachment, j.Source, "evidence", []byte("proof")); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		var assignment string
		if err := tx.QueryRow(`SELECT source_id FROM jobs WHERE task_id=? AND kind='assignment'`, parent).Scan(&assignment); err != nil {
			return err
		}
		if err := addMessage(tx, followSource, "service", j.Agent, "system", "review", j.Project, j.Task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,review_id) VALUES(?,?,1,'review',?,?)`, followSource, j.ID, assignment, j.Source)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	j.Kind, j.Source = "po_followup", followSource
	canonical := runtimeapi.Attachment{Path: filepath.Join(s.dir, "attachments", attachment), Name: "evidence", MediaType: "text/markdown"}
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "report"}}, Attachments: []runtimeapi.Attachment{canonical}}
	uploads, err := s.uploadAttachments(s.db, j, result)
	if err != nil || len(uploads) != 0 {
		t.Fatalf("pinned upload=%v err=%v", uploads, err)
	}
	result.Attachments[0].Path = filepath.Join(s.dir, "attachments", newID())
	uploads, err = s.uploadAttachments(s.db, j, result)
	if err != nil || len(uploads) != 1 {
		t.Fatalf("foreign upload=%v err=%v", uploads, err)
	}
}

func TestAliasBindsAtFirstClaimOnly(t *testing.T) {
	s, j, _, worker, _ := childReviewFixture(t)
	defer s.Close()
	installReviewInspector(t, s)
	id := newID()
	if _, err := s.db.Exec(`INSERT INTO attachments VALUES(?,?,?,'text/markdown',?)`, id, j.Source, "evidence", []byte("proof")); err != nil {
		t.Fatal(err)
	}
	payload := `{"text":"done","actions":[],"attachments":[{"path":"/old/evidence.md","name":"evidence","media_type":"text/markdown"}]}`
	if _, err := s.db.Exec(`UPDATE jobs SET result=? WHERE task_id=? AND agent_id=? AND kind='work'`, []byte(payload), j.Task, worker); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE jobs SET status='completed' WHERE id=?`, j.ID); err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		po, err := getAgent(tx, j.Agent)
		if err != nil {
			return err
		}
		var assignment string
		if err := tx.QueryRow(`SELECT source_id FROM jobs WHERE kind='assignment' LIMIT 1`).Scan(&assignment); err != nil {
			return err
		}
		source := newID()
		if err := addMessage(tx, source, "service", j.Agent, "system", "review", j.Project, j.Task); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO po_followups(source_id,trigger_id,ordinal,phase,assignment_id,review_id) VALUES(?,?,1,'review',?,?)`, source, j.ID, assignment, j.Source); err != nil {
			return err
		}
		return s.enqueue(tx, po, j.Project, j.Task, source, "po_followup", "review")
	}); err != nil {
		t.Fatal(err)
	}
	claimed, err := s.claim(nil)
	if err != nil || claimed == nil {
		t.Fatalf("claim=%+v err=%v", claimed, err)
	}
	found := false
	for _, m := range claimed.Request.Context {
		if m.ID == j.Source {
			for _, a := range m.Attachments {
				if a.ID == id && a.SourcePath == "/old/evidence.md" {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatal("first claim did not bind exact alias")
	}
	var marker int
	if err := s.db.QueryRow(`SELECT count(*) FROM attachment_alias_first_claim WHERE job_id=?`, claimed.ID).Scan(&marker); err != nil || marker != 0 {
		t.Fatalf("marker=%d err=%v", marker, err)
	}
}
