package service

import (
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestDelegationReplyUsesOnlyRecipientVisibleConversation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		target string
		linked bool
	}{{"same room", "same", true}, {"foreign room", "foreign", false}, {"missing", "missing", false}} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, po, _ := continuationFixture(t)
			defer s.Close()
			origin, visible, foreign := newID(), newID(), newID()
			if err := s.write(func(tx *sql.Tx) error {
				if err := addMessage(tx, origin, operator, root, "operator", "Assign an independent review", "", ""); err != nil {
					return err
				}
				if err := addMessage(tx, visible, po, root, "po", "Previous PO observation", "alpha", ""); err != nil {
					return err
				}
				if err := addMessage(tx, foreign, root, operator, "root", "Private operator message", "alpha", ""); err != nil {
					return err
				}
				target := newID()
				if tc.target == "same" {
					target = visible
				}
				if tc.target == "foreign" {
					target = foreign
				}
				return s.rootAssignment(tx, agent{ID: root, Role: "root"}, job{Source: origin}, runtimeapi.Action{Type: "delegate", Project: "alpha", Title: "Independent review", Body: "Review the evidence.", ReplyTo: target})
			}); err != nil {
				t.Fatal(err)
			}
			scope, err := s.claim(nil)
			if err != nil || scope == nil || scope.Kind != "scope_review" {
				t.Fatalf("scope review: %v", err)
			}
			var parent string
			err = s.db.QueryRow(`SELECT parent_id FROM message_replies WHERE message_id=?`, scope.Source).Scan(&parent)
			if tc.linked && (err != nil || parent != visible) {
				t.Fatalf("missing authorized quote: %v", err)
			}
			if !tc.linked && err != sql.ErrNoRows {
				t.Fatalf("unexpected quote: %v", err)
			}
			seen := false
			for _, m := range scope.Request.Context {
				if m.ID == visible {
					seen = true
				}
				if m.ID == foreign {
					t.Fatal("foreign message exposed")
				}
			}
			if tc.linked && !seen {
				t.Fatal("authorized quoted message missing from PO context")
			}
			if err := s.complete(*scope, runtimeapi.Result{Text: "Scope assessment."}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			decision, err := s.claim(nil)
			if err != nil || decision == nil || decision.Kind != "scope_decision" {
				t.Fatalf("scope decision: %v", err)
			}
			if err := s.complete(*decision, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "Final review", Body: "Proceed with review.", ReplyTo: decision.Source}}}, nil, nil, nil); err != nil {
				t.Fatal(err)
			}
			assignment, err := s.claim(nil)
			if err != nil || assignment == nil || assignment.Kind != "assignment" {
				t.Fatalf("assignment: %v", err)
			}
			if err := s.db.QueryRow(`SELECT parent_id FROM message_replies WHERE message_id=?`, assignment.Source).Scan(&parent); err != nil || parent != decision.Source {
				t.Fatalf("final delegation quote: %v", err)
			}
			seen = false
			for _, m := range assignment.Request.Context {
				if m.ID == decision.Source {
					seen = true
				}
			}
			if !seen {
				t.Fatal("quoted assessment missing from assignment context")
			}
		})
	}
}
