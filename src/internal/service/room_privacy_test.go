package service

import (
	"agentchat/src/internal/runtimeapi"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"slices"
	"testing"
)

func TestRoomPrivacyAndRuntimeContext(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	po, worker, other := newID(), newID(), newID()
	group, dm, private := newID(), newID(), newID()
	file := newID()
	err := s.write(func(tx *sql.Tx) error {
		if _, e := tx.Exec("INSERT INTO projects(id,name,po_id,workspace) VALUES('alpha','Alpha',?,'/tmp')", po); e != nil {
			return e
		}
		for _, a := range []struct{ id, role string }{{po, "po"}, {worker, "worker"}, {other, "worker"}} {
			if _, e := tx.Exec("INSERT INTO agents VALUES(?,?,?,'alpha','model','high')", a.id, a.role, a.role); e != nil {
				return e
			}
		}
		for _, m := range []struct{ id, from, to, role, text string }{{group, po, "group:alpha", "po", "group"}, {dm, po, operator, "po", "dm"}, {private, po, worker, "po", "private"}} {
			if e := addMessage(tx, m.id, m.from, m.to, m.role, m.text, "alpha", ""); e != nil {
				return e
			}
		}
		_, e := tx.Exec("INSERT INTO attachments VALUES(?,?,?,?,?)", file, private, "secret.txt", "text/plain", []byte("secret"))
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(view.Messages, func(m Message) bool { return m.ID == private }) {
		t.Fatal("operator saw another pair's DM")
	}
	for _, m := range view.Messages {
		if m.ID == group && m.RoomID != "group:alpha" {
			t.Fatal(m.RoomID)
		}
		if m.ID == dm && m.RoomID != directRoom(po, operator) {
			t.Fatal(m.RoomID)
		}
	}
	req := httptest.NewRequest("GET", "/api/attachments/"+file, nil)
	req.SetPathValue("id", file)
	w := httptest.NewRecorder()
	s.serveAttachment(w, req, false)
	if w.Code != 404 {
		t.Fatalf("private attachment returned %d", w.Code)
	}
	if err = s.markOperatorRead([]string{private}); err == nil {
		t.Fatal("private read marker accepted")
	}
	err = s.write(func(tx *sql.Tx) error {
		if e := checkReplyParent(tx, group, groupPrefix+"alpha", "alpha"); e != nil {
			return e
		}
		if e := checkReplyParent(tx, group, po, ""); e == nil {
			t.Fatal("cross-room reply allowed")
		}
		a := agent{ID: other, Role: "worker", Project: "alpha"}
		got, e := s.runtimeContext(tx, a, "alpha", "", "", groupMessageKind)
		if e != nil {
			return e
		}
		if !slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == group }) {
			t.Fatal("unmentioned group post missing")
		}
		if slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == private || m.ID == dm }) {
			t.Fatal("unrelated direct message leaked")
		}
		got, e = s.runtimeContext(tx, a, "alpha", "", private, "operator_message")
		if e != nil {
			return e
		}
		if slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == private || m.ID == dm }) {
			t.Fatal("cross-room parent exposed private content")
		}
		member := agent{ID: worker, Role: "worker", Project: "alpha"}
		got, e = s.runtimeContext(tx, member, "alpha", "", "", "work")
		if e != nil {
			return e
		}
		if !slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == private }) {
			t.Fatal("participant did not receive direct message")
		}
		if slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == dm }) {
			t.Fatal("other pair's DM leaked")
		}
		poAgent := agent{ID: po, Role: "po", Project: "alpha"}
		got, e = s.runtimeContext(tx, poAgent, "alpha", "", "", "operator_message")
		if e != nil {
			return e
		}
		if !slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == dm }) {
			t.Fatal("operator DM missing")
		}
		if slices.ContainsFunc(got, func(m runtimeapi.Message) bool { return m.ID == group || m.ID == private }) {
			t.Fatal("another room leaked into operator DM context")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRoomMigrationPreservesUnambiguousAndQuarantinesUnknown(t *testing.T) {
	dir := t.TempDir()
	db, err := sql.Open("sqlite3", filepath.Join(dir, "agentchat.db"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(schema); err != nil {
		t.Fatal(err)
	}
	po := newID()
	if _, err = db.Exec("INSERT INTO projects(id,name,po_id,workspace) VALUES('alpha','Alpha',?,'/tmp')", po); err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("INSERT INTO agents VALUES(?,?,'po','alpha','model','high')", po, "PO"); err != nil {
		t.Fatal(err)
	}
	ids := []string{newID(), newID(), newID()}
	rows := []struct{ id, from, to, role string }{{ids[0], po, "group:alpha", "po"}, {ids[1], po, operator, "po"}, {ids[2], "unknown", operator, "worker"}}
	for _, m := range rows {
		if _, err = db.Exec("INSERT INTO messages(id,sender,recipient,role,text,at,project_id) VALUES(?,?,?,?,?,1,'alpha')", m.id, m.from, m.to, m.role, "legacy"); err != nil {
			t.Fatal(err)
		}
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	s := openTest(t, dir)
	defer s.Close()
	var group, dm string
	var quarantined sql.NullString
	if err = s.db.QueryRow("SELECT room_id FROM messages WHERE id=?", ids[0]).Scan(&group); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow("SELECT room_id FROM messages WHERE id=?", ids[1]).Scan(&dm); err != nil {
		t.Fatal(err)
	}
	if err = s.db.QueryRow("SELECT room_id FROM messages WHERE id=?", ids[2]).Scan(&quarantined); err != nil {
		t.Fatal(err)
	}
	if group != "group:alpha" || dm != directRoom(po, operator) || quarantined.Valid {
		t.Fatalf("unexpected migration: %q %q %+v", group, dm, quarantined)
	}
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(view.Messages, func(m Message) bool { return m.ID == ids[2] }) {
		t.Fatal("ambiguous row displayed")
	}
}
