package service

import (
	"database/sql"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
)

func postOperatorHold(s *Service, task, project, note, id string) int {
	form := url.Values{"id": {id}, "project": {project}, "note": {note}}
	r := httptest.NewRequest("POST", "/api/tasks/"+task+"/resolve-hold", strings.NewReader(form.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	r.SetPathValue("id", task)
	w := httptest.NewRecorder()
	s.operatorResolveHold(w, r)
	return w.Code
}
func TestOperatorHoldResolution(t *testing.T) {
	t.Run("replay and competing decisions", func(t *testing.T) {
		s, task, po, _ := heldTaskFixture(t)
		defer s.Close()
		ids := []string{newID(), newID()}
		codes := make([]int, 2)
		var wg sync.WaitGroup
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				codes[i] = postOperatorHold(s, task, "alpha", "Continue within the existing production stop.", ids[i])
			}(i)
		}
		wg.Wait()
		winner := -1
		for i, code := range codes {
			if code == 202 {
				if winner >= 0 {
					t.Fatalf("two accepted decisions: %v", codes)
				}
				winner = i
			} else if code != 409 {
				t.Fatalf("unexpected outcomes: %v", codes)
			}
		}
		if winner < 0 {
			t.Fatalf("no accepted decision: %v", codes)
		}
		if code := postOperatorHold(s, task, "alpha", "Continue within the existing production stop.", ids[winner]); code != 202 {
			t.Fatalf("idempotent replay: %d", code)
		}
		if code := postOperatorHold(s, task, "alpha", "Changed decision", ids[winner]); code != 409 {
			t.Fatalf("changed replay: %d", code)
		}
		var count int
		var recipient, room string
		if err := s.db.QueryRow(`SELECT count(*) FROM po_followups f JOIN task_holds h ON h.job_id=f.trigger_id WHERE h.task_id=? AND h.status='resolved'`, task).Scan(&count); err != nil || count != 1 {
			t.Fatalf("followups=%d err=%v", count, err)
		}
		if err := s.db.QueryRow(`SELECT recipient,room_id FROM messages WHERE id=?`, ids[winner]).Scan(&recipient, &room); err != nil || recipient != po || room != directRoom(operator, po) {
			t.Fatalf("private note recipient=%s room=%s err=%v", recipient, room, err)
		}
	})
	t.Run("legacy structured hold", func(t *testing.T) {
		s, task, _, _ := heldTaskFixture(t)
		defer s.Close()
		installReviewInspector(t, s)
		if _, err := s.db.Exec("DELETE FROM task_holds WHERE task_id=?", task); err != nil {
			t.Fatal(err)
		}
		id := newID()
		if code := postOperatorHold(s, task, "alpha", "Proceed within the existing constraints.", id); code != 202 {
			t.Fatalf("legacy: %d", code)
		}
		if code := postOperatorHold(s, task, "alpha", "Proceed within the existing constraints.", id); code != 202 {
			t.Fatalf("legacy replay: %d", code)
		}
	})
	t.Run("stopped and unheld tasks", func(t *testing.T) {
		s, task, _, _ := heldTaskFixture(t)
		defer s.Close()
		if err := s.write(func(tx *sql.Tx) error {
			_, err := tx.Exec(`UPDATE projects SET stopped=1 WHERE id='alpha'`)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		if code := postOperatorHold(s, task, "alpha", "Proceed", newID()); code != 409 {
			t.Fatalf("stopped: %d", code)
		}
		if code := postOperatorHold(s, newID(), "alpha", "Proceed", newID()); code != 409 {
			t.Fatalf("unheld: %d", code)
		}
	})
}
