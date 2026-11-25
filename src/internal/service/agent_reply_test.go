package service

import (
	"database/sql"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestAgentGroupReplyRoomBoundary(t *testing.T) {
	for _, role := range []string{"po", "worker"} {
		for _, kind := range []string{"same-room", "cross-room", "stale"} {
			t.Run(role+"/"+kind, func(t *testing.T) {
				s, task, po, worker := continuationFixture(t)
				defer s.Close()
				id := po
				if role == "worker" {
					id = worker
				}
				parent := newID()
				if err := s.write(func(tx *sql.Tx) error {
					if kind == "stale" {
						return nil
					}
					if kind == "cross-room" {
						return addMessage(tx, parent, operator, id, "operator", "private", "alpha", "")
					}
					return addMessage(tx, parent, operator, "group:alpha", "operator", "group", "alpha", "")
				}); err != nil {
					t.Fatal(err)
				}
				if err := s.write(func(tx *sql.Tx) error {
					return postGroupAction(tx, agent{ID: id, Role: role, Project: "alpha"}, job{Project: "alpha", Task: task}, runtimeapi.Action{Type: "post_group", Project: "alpha", Body: "Reply", ReplyTo: parent})
				}); err != nil {
					t.Fatal(err)
				}
				var reply string
				if err := s.db.QueryRow(`SELECT COALESCE((SELECT parent_id FROM message_replies r WHERE r.message_id=m.id),'') FROM messages m WHERE m.sender=? AND m.recipient='group:alpha' AND m.text='Reply'`, id).Scan(&reply); err != nil {
					t.Fatal(err)
				}
				want := ""
				if kind == "same-room" {
					want = parent
				}
				if reply != want {
					t.Fatalf("reply=%q want=%q", reply, want)
				}
			})
		}
	}
}

func TestAddressedGroupResponseRepliesToExactSource(t *testing.T) {
	for _, role := range []string{"po", "worker"} {
		t.Run(role, func(t *testing.T) {
			s, _, po, worker := continuationFixture(t)
			defer s.Close()
			id := po
			if role == "worker" {
				id = worker
			}
			source := newID()
			if err := s.write(func(tx *sql.Tx) error {
				if err := addMessage(tx, source, operator, "group:alpha", "operator", "Question", "alpha", ""); err != nil {
					return err
				}
				_, err := tx.Exec("INSERT INTO message_mentions(message_id,agent_id) VALUES(?,?)", source, id)
				return err
			}); err != nil {
				t.Fatal(err)
			}
			if err := s.write(func(tx *sql.Tx) error {
				return groupMessageResult(tx, agent{ID: id, Role: role, Project: "alpha"}, job{Source: source, Project: "alpha", Kind: groupMessageKind}, runtimeapi.Result{Text: "Answer"}, nil)
			}); err != nil {
				t.Fatal(err)
			}
			var reply string
			if err := s.db.QueryRow(`SELECT r.parent_id FROM messages m JOIN message_replies r ON r.message_id=m.id WHERE m.sender=? AND m.recipient='group:alpha' AND m.text='Answer'`, id).Scan(&reply); err != nil || reply != source {
				t.Fatalf("reply=%q: %v", reply, err)
			}
		})
	}
}
