package service

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"testing"
)

func TestStateFeedOnlyProjectsChangedRows(t *testing.T) {
	s, task, po, worker := continuationFixture(t)
	defer s.Close()
	historical := newID()
	if err := s.write(func(tx *sql.Tx) error {
		return addMessage(tx, historical, root, operator, "root", "Earlier conversation", "", "")
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := s.state()
	if err != nil || len(snapshot.Messages) != 1 || len(snapshot.Jobs) != 1 || !snapshot.Tasks[0].ContinuePOEligible {
		t.Fatalf("bootstrap omitted existing history or eligibility: %+v, %v", snapshot, err)
	}
	empty, err := s.stateAfter(snapshot.Cursor)
	if err != nil || empty.More || empty.Cursor != snapshot.Cursor || len(empty.Jobs)+len(empty.Tasks)+len(empty.Messages) != 0 {
		t.Fatalf("unchanged poll: %+v, %v", empty, err)
	}

	public, private, approval := newID(), newID(), newID()
	if err = s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, private, worker, po, "worker", "Private evidence", "alpha", task); err != nil {
			return err
		}
		if err := addMessage(tx, public, worker, operator, "worker", "Visible evidence", "alpha", ""); err != nil {
			return err
		}
		if _, err := tx.Exec(`INSERT INTO attachments VALUES(?,?,?,?,?)`, newID(), public, "note.txt", "text/plain", []byte("evidence")); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Confirm','Decision','pending')`, approval, task, po)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	changes, err := s.stateAfter(snapshot.Cursor)
	if err != nil {
		t.Fatal(err)
	}
	if len(changes.Messages) != 1 || changes.Messages[0].ID != public || len(changes.Messages[0].Attachments) != 1 ||
		len(changes.Jobs) != 0 || len(changes.Tasks) != 1 || changes.Tasks[0].ID != task || changes.Tasks[0].ContinuePOEligible ||
		len(changes.Approvals) != 1 || changes.Approvals[0].ID != approval || len(changes.Removed) != 0 {
		t.Fatalf("incorrect scoped changes: %+v", changes)
	}

	if err = s.markOperatorRead([]string{historical}); err != nil {
		t.Fatal(err)
	}
	receipt, err := s.stateAfter(changes.Cursor)
	if err != nil || len(receipt.Messages) != 1 || receipt.Messages[0].ID != historical || receipt.Messages[0].OperatorReadAt == nil ||
		len(receipt.Tasks)+len(receipt.Jobs) != 0 {
		t.Fatalf("read receipt was not a single-message change: %+v, %v", receipt, err)
	}
	rolledBack := errors.New("cancel transaction")
	err = s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, newID(), root, operator, "root", "Rolled-back chat", "", ""); err != nil {
			return err
		}
		return rolledBack
	})
	if !errors.Is(err, rolledBack) {
		t.Fatal(err)
	}
	if _, err = s.db.Exec(`UPDATE jobs SET next_at=next_at+1 WHERE id=?`, snapshot.Jobs[0].ID); err != nil {
		t.Fatal(err)
	}
	empty, err = s.stateAfter(receipt.Cursor)
	if err != nil || empty.More || empty.Cursor != receipt.Cursor {
		t.Fatalf("rollback or private scheduler write advanced the cursor: %+v, %v", empty, err)
	}
	if _, err = s.stateAfter("00000000000000000000000000000000.1"); !errors.Is(err, errConflict) {
		t.Fatalf("different database cursor: %v", err)
	}
}

func TestStateFeedPagesCoalescedRowsInMessageOrder(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, 0, statePageSize+4)
	if err = s.write(func(tx *sql.Tx) error {
		for i := 0; i < cap(ids); i++ {
			id := newID()
			ids = append(ids, id)
			if err := addMessage(tx, id, root, operator, "root", fmt.Sprintf("Message %d", i), "", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err = s.markOperatorRead(ids[:1]); err != nil {
		t.Fatal(err)
	}
	first, err := s.stateAfter(view.Cursor)
	if err != nil || !first.More || len(first.Messages) != statePageSize || first.Messages[0].ID != ids[1] {
		t.Fatalf("expected bounded page with first revision moved forward: messages=%d more=%t err=%v", len(first.Messages), first.More, err)
	}
	second, err := s.stateAfter(first.Cursor)
	if err != nil || second.More || len(second.Messages) != 4 || second.Messages[0].ID != ids[0] || second.Messages[0].OperatorReadAt == nil {
		t.Fatalf("expected remaining ordered messages and coalesced receipt: %+v, err=%v", second, err)
	}
	merged := append(first.Messages, second.Messages...)
	slices.SortFunc(merged, func(a, b Message) int {
		return int(a.Position - b.Position)
	})
	for i, msg := range merged {
		if msg.ID != ids[i] {
			t.Fatalf("history order mismatch at %d", i)
		}
	}
}
