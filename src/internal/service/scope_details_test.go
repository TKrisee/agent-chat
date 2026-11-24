package service

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"agentchat/src/internal/runtimeapi"
)

func TestScopeNoticeDetailsRequireUniqueFormalHandoff(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	origin := newID()
	body := "Formal scope only\nPreserve exact constraints."
	if err := s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, origin, operator, root, "operator", "original request", "", ""); err != nil {
			return err
		}
		return s.rootAssignment(tx, agent{ID: root, Role: "root"}, job{ID: newID(), Source: origin}, runtimeapi.Action{Type: "create_project", Project: "alpha", Title: "Original title", Body: body})
	}); err != nil {
		t.Fatal(err)
	}
	var notice, task, scopeJob, po string
	if err := s.db.QueryRow(`SELECT n.message_id,t.id,j.id,p.po_id FROM workflow_notices n JOIN tasks t ON n.event_key='scope:'||t.id JOIN projects p ON p.id=t.project_id JOIN jobs j ON j.task_id=t.id AND j.kind='scope_review'`).Scan(&notice, &task, &scopeJob, &po); err != nil {
		t.Fatal(err)
	}
	view, err := s.state()
	if err != nil {
		t.Fatal(err)
	}
	eligible := false
	for _, m := range view.Messages {
		if m.ID == notice {
			eligible = m.NoticeDetail == "scope_request"
		}
		if strings.Contains(m.Text, body) {
			t.Fatal("private scope appeared in operator timeline")
		}
	}
	if !eligible {
		t.Fatal("eligible historical notice lacks capability")
	}
	get := func(id string) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodGet, "/api/workflow-notices/"+id+"/scope-request", nil)
		s.Handler(nil).ServeHTTP(w, r)
		return w
	}
	w := get(notice)
	if w.Code != 200 || w.Body.String() != body || w.Header().Get("X-AgentChat-Scope-Project") != "alpha" || w.Header().Get("X-AgentChat-Scope-Task") != task || w.Header().Get("X-AgentChat-Scope-Job") != scopeJob || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("bad scope response: status=%d headers=%v", w.Code, w.Header())
	}
	if w = get(origin); w.Code != 404 {
		t.Fatalf("ordinary message detail returned %d", w.Code)
	}
	// A second plausible job for the same task makes provenance ambiguous; both
	// capability and detail must disappear without deleting either record.
	otherSource := newID()
	if err = s.write(func(tx *sql.Tx) error {
		if err := addMessage(tx, otherSource, root, po, "root", "other private text", "alpha", task); err != nil {
			return err
		}
		_, err := tx.Exec(`INSERT INTO jobs(id,agent_id,project_id,task_id,source_id,kind,status,request,next_at) SELECT ?,agent_id,project_id,task_id,?,'scope_review','pending',request,next_at FROM jobs WHERE id=?`, newID(), otherSource, scopeJob)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if w = get(notice); w.Code != 404 {
		t.Fatalf("ambiguous packet returned %d", w.Code)
	}
	view, err = s.state()
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range view.Messages {
		if m.ID == notice && m.NoticeDetail != "" {
			t.Fatal("ambiguous capability remained visible")
		}
	}
}
