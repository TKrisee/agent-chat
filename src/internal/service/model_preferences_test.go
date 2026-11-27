package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"database/sql"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestPreferredModelIsAdvisoryOnWorkerDispatch(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	s.setupMu.Lock()
	s.models = []string{"gpt-terra", "gpt-astra"}
	s.modelsSource = "fixture"
	s.modelsAt = time.Now()
	s.setupMu.Unlock()
	r := httptest.NewRequest("POST", "/api/model-preferences", strings.NewReader(`{"preferences":{"reasoning":"gpt-terra","implementation":"","precise_edits":"","research":"","routine_checks":""}}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	s.setModelPreferences(w, r)
	if w.Code != 204 {
		t.Fatalf("save %d %s", w.Code, w.Body.String())
	}
	if _, err := s.userMessage(context.Background(), newID(), root, "", "Create a project for evidence.", "", nil); err != nil {
		t.Fatal(err)
	}
	j, err := s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("root %v", err)
	}
	if err = s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "create_project", Project: "alpha", Title: "Evidence", Body: "Review evidence."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	j, err = s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("scope %v", err)
	}
	if err = s.complete(*j, runtimeapi.Result{Text: "Assess."}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	j, err = s.claim(nil)
	if err != nil || j == nil {
		t.Fatalf("decision %v", err)
	}
	if err = s.complete(*j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "delegate", Project: "alpha", Title: "Evidence", Body: "Review evidence."}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	j, err = s.claim(nil)
	if err != nil || j == nil || !strings.Contains(j.Request.Instructions, "reasoning: gpt-terra") {
		t.Fatalf("assignment preferences unavailable: %v", err)
	}
	actions := []runtimeapi.Action{{Type: "create_worker", Title: "Analyst", Model: "gpt-astra"}, {Type: "queue_work_item", Title: "Assess", Body: "Inspect evidence.", Recipient: "Analyst", Category: "reasoning", ModelReason: "More suitable for this analysis."}}
	if err = s.complete(*j, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var model string
	var notices, approvals int
	if err = s.db.QueryRow(`SELECT a.model FROM jobs j JOIN agents a ON a.id=j.agent_id WHERE j.kind='work' AND a.name='Analyst'`).Scan(&model); err != nil || model != "gpt-astra" {
		t.Fatalf("model %q %v", model, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient='operator' AND role='system' AND text LIKE 'Model choice for project alpha,%'`).Scan(&notices); err != nil || notices != 1 {
		t.Fatalf("notices %d %v", notices, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM approvals`).Scan(&approvals); err != nil || approvals != 0 {
		t.Fatalf("approvals %d %v", approvals, err)
	}
}
func TestModelPreferencesRemainClearableWhenCatalogUnavailable(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if _, err := s.db.Exec(`UPDATE model_preferences SET research='old-model' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	post := func(body string) int {
		r := httptest.NewRequest("POST", "/api/model-preferences", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.setModelPreferences(w, r)
		return w.Code
	}
	if got := post(`{"preferences":{"reasoning":"new-model","research":"old-model"}}`); got != 400 {
		t.Fatalf("new unavailable choice: %d", got)
	}
	if got := post(`{"preferences":{"research":"old-model"}}`); got != 204 {
		t.Fatalf("retain saved: %d", got)
	}
	if got := post(`{"preferences":{}}`); got != 204 {
		t.Fatalf("clear: %d", got)
	}
	p, err := readModelPreferences(s.db)
	if err != nil || p.Research != "" {
		t.Fatalf("clear persisted: %+v %v", p, err)
	}
}
func TestModelDepartureMissingCategoryIsHonestAndNonblocking(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if _, err := s.db.Exec(`UPDATE model_preferences SET reasoning='preferred' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`INSERT INTO agents VALUES('worker-x','Worker X','worker',NULL,'alternate','high')`); err != nil {
		t.Fatal(err)
	}
	err := s.write(func(tx *sql.Tx) error { return s.notifyModelDeparture(tx, "", "", "", "alternate", "") })
	if err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		if err := s.notifyModelDeparture(tx, "", "", "research", "alternate", ""); err != nil {
			return err
		}
		return s.notifyModelDeparture(tx, "", "", "", "preferred", "")
	}); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient='operator' AND role='system'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("unexpected notices %d %v", n, err)
	}
	var text string
	if err = s.db.QueryRow(`SELECT text FROM messages WHERE sender='root' AND recipient='operator' AND role='system' ORDER BY seq DESC LIMIT 1`).Scan(&text); err != nil || !strings.Contains(text, "no matching task category") || !strings.Contains(text, "Reason was not provided") {
		t.Fatalf("notice %q %v", text, err)
	}
}
