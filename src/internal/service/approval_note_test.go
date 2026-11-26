package service

import (
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestApprovalDecisionNoteIsDurableAndPrivate(t *testing.T) {
	for _, decision := range []string{"approved", "denied"} {
		t.Run(decision, func(t *testing.T) {
			s, task, po, _ := continuationFixture(t)
			defer s.Close()
			id := newID()
			if err := s.write(func(tx *sql.Tx) error {
				_, err := tx.Exec(`INSERT INTO approvals VALUES(?,'alpha',?,?,'Decision','Choose.','pending')`, id, task, po)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			post := func(payload string) int {
				r := httptest.NewRequest("POST", "/api/approvals/"+id, strings.NewReader(payload))
				r.Header.Set("Content-Type", "application/json")
				r.SetPathValue("id", id)
				w := httptest.NewRecorder()
				s.decideApproval(w, r)
				return w.Code
			}
			if got := post(`{"decision":"` + decision + `","note":"  scoped note  "}`); got != 200 {
				t.Fatalf("decision status %d", got)
			}
			if got := post(`{"decision":"` + decision + `","note":"different retry"}`); got != 409 {
				t.Fatalf("retry %d", got)
			}
			if got := post(`{"decision":"` + decision + `"}`); got != 200 {
				t.Fatalf("legacy retry %d", got)
			}
			state, err := s.state()
			if err != nil {
				t.Fatal(err)
			}
			if len(state.Approvals) != 1 || state.Approvals[0].DecisionNote != "scoped note" {
				t.Fatalf("notes %+v", state.Approvals)
			}
			j, err := s.claim(nil)
			if err != nil || j == nil || j.Kind != "approval_"+decision {
				t.Fatalf("job %+v %v", j, err)
			}
			found := strings.Contains(j.Request.Instructions, "scoped note")
			for _, m := range j.Request.Context {
				if strings.Contains(m.Text, "scoped note") {
					found = true
				}
			}
			if !found {
				t.Fatal("decision note missing from PO context")
			}
			var n int
			if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE room_id='group:alpha' AND instr(text,'scoped note')>0`).Scan(&n); err != nil || n != 0 {
				t.Fatalf("group leak %d %v", n, err)
			}
		})
	}
}
