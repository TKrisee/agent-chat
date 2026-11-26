package service

import (
	"database/sql"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestPOApprovalDeliveryIsDurableAndPrivate(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error {
		return addMessage(tx, newID(), operator, po, "operator", "private sentinel", "alpha", task)
	}); err != nil {
		t.Fatal(err)
	}
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "request_approval", Project: "alpha", Title: "Choose roster", Body: "Choose A or B."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var id, message, text string
	if err := s.db.QueryRow(`SELECT a.id,d.message_id,m.text FROM approvals a JOIN approval_deliveries d ON d.approval_id=a.id JOIN messages m ON m.id=d.message_id WHERE a.task_id=? AND m.sender='root' AND m.recipient='operator'`, task).Scan(&id, &message, &text); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, "Choose roster") || !strings.Contains(text, "Choose A or B.") || strings.Contains(text, "private sentinel") {
		t.Fatalf("incorrect delivery %q", text)
	}
	state, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	linked := false
	for _, m := range state.Messages {
		if m.ID == message && m.ApprovalID == id {
			linked = true
		}
		if m.Sender == root && strings.Contains(m.Text, "private sentinel") {
			t.Fatal("private message exposed")
		}
	}
	if !linked {
		t.Fatal("approval ID absent from visible delivery")
	}
	if err := s.write(deliverPendingApprovals); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM approval_deliveries WHERE approval_id=? AND message_id=?`, id, message).Scan(&n); err != nil || n != 1 {
		t.Fatalf("duplicates=%d %v", n, err)
	}
}

func TestRejectedMixedApprovalDoesNotDeliver(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	j := activeAssignment(t, s, task, po)
	result := runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "queue_worker", Project: "alpha", Body: "work"}, {Type: "request_approval", Project: "alpha", Title: "Decision", Body: "Choose."}}}
	if err := s.complete(j, result, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM approval_deliveries`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("deliveries=%d %v", n, err)
	}
}

func TestPendingApprovalBackfillAndPriorDelivery(t *testing.T) {
	for _, prior := range []bool{false, true} {
		t.Run(map[bool]string{false: "new", true: "existing"}[prior], func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			id := newID()
			if err := s.write(func(tx *sql.Tx) error {
				if _, err := tx.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Exact choices','pending')`, id, task, po); err != nil {
					return err
				}
				if prior {
					return addMessage(tx, newID(), root, operator, "root", "Decision: Exact choices", "alpha", task)
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			for i := 0; i < 2; i++ {
				if err := s.write(deliverPendingApprovals); err != nil {
					t.Fatal(err)
				}
			}
			var n int
			want := 1
			if prior {
				want = 2
			}
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient='operator' AND task_id=? AND instr(text,'Exact choices')>0`, task).Scan(&n); err != nil || n != want {
				t.Fatalf("deliveries=%d %v", n, err)
			}
			if prior {
				var linked string
				if err := s.db.QueryRow(`SELECT m.text FROM approval_deliveries d JOIN messages m ON m.id=d.message_id WHERE d.approval_id=?`, id).Scan(&linked); err != nil || linked == "Decision: Exact choices" {
					t.Fatalf("unrelated message linked: %q %v", linked, err)
				}
			}
		})
	}
}
