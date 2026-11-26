package service

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"
	"unicode/utf8"

	"agentchat/src/internal/runtimeapi"
)

func TestOversizedParentOutcomesCommitChildReview(t *testing.T) {
	s, j, parent, worker, _ := childReviewFixture(t)
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error {
		for i := 0; i < 100; i++ {
			child := newID()
			if _, err := tx.Exec(`INSERT INTO tasks VALUES(?,'alpha',?,'completed',?)`, child, fmt.Sprintf("Earlier %03d", i), j.Agent); err != nil {
				return err
			}
			if _, err := tx.Exec(`INSERT INTO po_work_items VALUES(?,?)`, parent, child); err != nil {
				return err
			}
			if err := addMessage(tx, newID(), j.Agent, j.Agent, "po", strings.Repeat("é", 1100), "alpha", child); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "accept_result", Recipient: worker}, {Type: "report", Body: "Latest reviewed outcome"}}}
	if err := s.complete(j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var jobStatus, taskStatus, summary string
	if err := s.db.QueryRow(`SELECT status FROM jobs WHERE id=?`, j.ID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT status FROM tasks WHERE id=?`, j.Task).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := s.db.QueryRow(`SELECT m.text FROM jobs q JOIN messages m ON m.id=q.source_id WHERE q.task_id=? AND q.kind='po_followup' AND q.status='pending'`, parent).Scan(&summary); err != nil {
		t.Fatal(err)
	}
	if jobStatus != "completed" || taskStatus != "completed" || len(summary) > 64000 || !utf8.ValidString(summary) || !strings.Contains(summary, "other outcomes omitted") || !strings.Contains(summary, "Earlier 099") || !strings.Contains(summary, "Latest reviewed outcome") {
		t.Fatalf("job=%s task=%s bytes=%d valid=%v", jobStatus, taskStatus, len(summary), utf8.ValidString(summary))
	}
	var count int
	if err := s.db.QueryRow(`SELECT count(*) FROM po_work_items WHERE parent_task_id=?`, parent).Scan(&count); err != nil || count != 101 {
		t.Fatalf("records=%d err=%v", count, err)
	}
}
