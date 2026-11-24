package service

import (
	"database/sql"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestOperatorReplyAndReadReceipts(t *testing.T) {
	dir := t.TempDir()
	s := openTest(t, dir)
	server := httptest.NewServer(s.Handler(nil))
	po, task := newID(), newID()
	olderIncoming, olderOutgoing, unread := newID(), newID(), newID()
	poIncoming, poOutgoing, internal, artifact := newID(), newID(), newID(), newID()
	if err := s.write(func(tx *sql.Tx) error {
		if _, err := tx.Exec("INSERT INTO projects(id,name,po_id,workspace) VALUES(?,?,?,?)",
			"atlas", "Atlas", po, filepath.Join(dir, "atlas")); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO agents VALUES(?,?,?,?,?,?)", po, "Atlas PO", "po", "atlas", "fixture", "high"); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO tasks VALUES(?,?,?,'completed',?)", task, "atlas", "Prior work", po); err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT INTO approvals VALUES(?,?,?,?,?,?,'pending')", newID(), nil, nil, root, "Decision", "Still pending"); err != nil {
			return err
		}
		for _, row := range []struct{ id, from, to, role, text, project, task string }{
			{olderIncoming, root, operator, "root", "Older exact project report", "atlas", task},
			{olderOutgoing, operator, root, "operator", "Earlier operator text", "", ""},
			{poIncoming, po, operator, "po", "A PO reply", "atlas", ""},
			{poOutgoing, operator, po, "operator", "The operator's PO message", "atlas", ""},
			{internal, po, root, "po", "An internal report", "atlas", task},
			{unread, root, operator, "root", "Keep this unread", "", ""},
		} {
			if err := addMessage(tx, row.id, row.from, row.to, row.role, row.text, row.project, row.task); err != nil {
				return err
			}
		}
		if _, err := tx.Exec("INSERT INTO attachments VALUES(?,?,?,?,?)", artifact, olderIncoming, "report.txt", "text/plain", []byte("original report")); err != nil {
			return err
		}
		for i := range 26 {
			if err := addMessage(tx, newID(), root, operator, "root", fmt.Sprintf("Recent message %d", i), "", ""); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	snapshot := func() State { return await(t, server.URL, func(State) bool { return true }) }
	message := func(state State, id string) Message {
		t.Helper()
		for _, m := range state.Messages {
			if m.ID == id {
				return m
			}
		}
		t.Fatalf("message %s missing", id)
		return Message{}
	}
	mark := func(status int, ids ...string) {
		t.Helper()
		if body := call(t, server.URL+"/api/messages/read", "POST", map[string][]string{"message_ids": ids}, status); status == 204 && len(body) != 0 {
			t.Fatalf("read receipt returned a body: %q", body)
		}
	}
	reply := func(status int, id, recipient, body, parent string) {
		t.Helper()
		call(t, server.URL+"/api/messages", "POST", map[string]string{
			"id": id, "recipient": recipient, "text": body, "reply_to": parent,
		}, status)
	}
	before := snapshot()
	mark(204, olderIncoming, poIncoming, olderIncoming)
	read := snapshot()
	rootRead := message(read, olderIncoming).OperatorReadAt
	poRead := message(read, poIncoming).OperatorReadAt
	if rootRead == nil || poRead == nil || rootRead.Location() != time.UTC {
		t.Fatalf("missing UTC read receipt: root=%v PO=%v", rootRead, poRead)
	}
	if len(read.Messages) != len(before.Messages) || len(read.Jobs) != 0 ||
		len(read.Approvals) != 1 || read.Approvals[0].Status != "pending" {
		t.Fatalf("read receipts affected jobs, chat or approval: %+v", read)
	}
	mark(404, unread, olderOutgoing)
	mark(404, unread, internal)
	mark(404, unread, newID())
	mark(400, "not-a-uuid")
	oversized := make([]string, maxOperatorReadBatch+1)
	for i := range oversized {
		oversized[i] = unread
	}
	mark(400, oversized...)
	time.Sleep(3 * time.Millisecond)
	mark(204, olderIncoming)
	afterRead := snapshot()
	if !rootRead.Equal(*message(afterRead, olderIncoming).OperatorReadAt) ||
		message(afterRead, unread).OperatorReadAt != nil || message(afterRead, olderOutgoing).OperatorReadAt != nil ||
		message(afterRead, internal).OperatorReadAt != nil {
		t.Fatal("read batch changed an ineligible receipt or rewrote its original timestamp")
	}

	replyID := newID()
	reply(201, replyID, root, "Follow up on the prior report", olderIncoming)
	reply(200, replyID, root, "Follow up on the prior report", olderIncoming)
	reply(409, replyID, root, "Follow up on the prior report", olderOutgoing)
	reply(409, replyID, root, "Follow up on the prior report", "")
	reply(404, newID(), root, "Wrong conversation", poIncoming)
	reply(404, newID(), po, "Wrong conversation", olderIncoming)
	reply(404, newID(), root, "Internal report is outside the conversation", internal)
	reply(404, newID(), root, "Unknown parent", newID())
	rootOutgoingReply, poIncomingReply, poOutgoingReply, plain := newID(), newID(), newID(), newID()
	reply(201, rootOutgoingReply, root, "Follow up to my own text", olderOutgoing)
	reply(201, poIncomingReply, po, "Follow up to the PO", poIncoming)
	reply(201, poOutgoingReply, po, "Follow up to my own PO text", poOutgoing)
	post(t, server.URL, plain, root, "An ordinary message", 201)
	server.Close()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	s = openTest(t, dir)
	defer s.Close()
	server = httptest.NewServer(s.Handler(nil))
	defer server.Close()
	reply(200, replyID, root, "Follow up on the prior report", olderIncoming)
	reopened := snapshot()
	for id, parent := range map[string]string{
		replyID: olderIncoming, rootOutgoingReply: olderOutgoing, poIncomingReply: poIncoming,
		poOutgoingReply: poOutgoing, plain: "",
	} {
		m := message(reopened, id)
		if m.ReplyTo != parent || m.OperatorReadAt != nil {
			t.Fatalf("reply/read state did not persist for %s: %+v", id, m)
		}
	}
	if len(reopened.Messages) != len(before.Messages)+5 || len(reopened.Jobs) != 5 ||
		!rootRead.Equal(*message(reopened, olderIncoming).OperatorReadAt) ||
		!poRead.Equal(*message(reopened, poIncoming).OperatorReadAt) {
		t.Fatal("unexpected persistence, replay, or marking effects")
	}

	j, err := s.claim(nil)
	if err != nil || j == nil || j.Source != replyID {
		t.Fatalf("exact reply job missing: job=%+v err=%v", j, err)
	}
	if len(j.Request.Context) != 25 || !strings.Contains(j.Request.Instructions, "replied to message "+olderIncoming) {
		t.Fatalf("older parent was not pinned outside rolling history: %+v", j.Request)
	}
	parent := j.Request.Context[0]
	if parent.ID != olderIncoming || parent.Text != "Older exact project report" || len(parent.Attachments) != 1 ||
		parent.Attachments[0].ID != artifact || parent.Attachments[0].Path != filepath.Join(s.dir, "attachments", artifact) {
		t.Fatalf("runtime parent content or attachment differs: %+v", parent)
	}
}
