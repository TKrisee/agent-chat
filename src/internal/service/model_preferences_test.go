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
	s.modelDetails = map[string]runtimeapi.ModelDescriptor{"gpt-terra": {ID: "gpt-terra", DefaultEffort: "medium", Efforts: []string{"medium", "high"}}, "gpt-astra": {ID: "gpt-astra", DefaultEffort: "medium", Efforts: []string{"medium", "ultra"}}}
	s.modelsSource = "fixture"
	s.modelsAt = time.Now()
	s.setupMu.Unlock()
	r := httptest.NewRequest("POST", "/api/model-preferences", strings.NewReader(`{"preferences":{"reasoning":"gpt-terra","implementation":"","precise_edits":"","research":"","routine_checks":""},"reasoning_efforts":{"reasoning":"high"}}`))
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
	if err != nil || j == nil || !strings.Contains(j.Request.Instructions, "reasoning: gpt-terra (reasoning high)") || !strings.Contains(j.Request.Instructions, "gpt-astra [reasoning: medium, ultra]") {
		t.Fatalf("assignment preferences unavailable: %v", err)
	}
	actions := []runtimeapi.Action{{Type: "create_worker", Title: "Analyst", Model: "gpt-astra", Effort: "ultra"}, {Type: "queue_work_item", Title: "Assess", Body: "Inspect evidence.", Recipient: "Analyst", Category: "reasoning", ModelReason: "More suitable for this analysis."}}
	if err = s.complete(*j, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var model, effort string
	var notices, approvals int
	if err = s.db.QueryRow(`SELECT a.model,a.effort FROM jobs j JOIN agents a ON a.id=j.agent_id WHERE j.kind='work' AND a.name='Analyst'`).Scan(&model, &effort); err != nil || model != "gpt-astra" || effort != "ultra" {
		t.Fatalf("model %q effort %q %v", model, effort, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient='operator' AND role='system' AND text LIKE 'Model choice for project alpha,%'`).Scan(&notices); err != nil || notices != 1 {
		t.Fatalf("notices %d %v", notices, err)
	}
	if err = s.db.QueryRow(`SELECT count(*) FROM approvals`).Scan(&approvals); err != nil || approvals != 0 {
		t.Fatalf("approvals %d %v", approvals, err)
	}
	work, err := s.claim(map[string]bool{j.Agent: true, root: true})
	if err != nil || work == nil || work.Kind != "work" || work.Request.Model != "gpt-astra" || work.Request.Effort != "ultra" {
		t.Fatalf("worker request %+v: %v", work, err)
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
	err := s.write(func(tx *sql.Tx) error { return s.notifyModelDeparture(tx, "", "", "", "alternate", "", "", "") })
	if err != nil {
		t.Fatal(err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		if err := s.notifyModelDeparture(tx, "", "", "research", "alternate", "", "", ""); err != nil {
			return err
		}
		return s.notifyModelDeparture(tx, "", "", "", "preferred", "", "", "")
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
func TestReasoningPreferencesValidateAndPreserveExactPair(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	s.setupMu.Lock()
	s.models = []string{"one", "two"}
	s.modelsSource = "fixture"
	s.modelsAt = time.Now()
	s.modelDetails = map[string]runtimeapi.ModelDescriptor{"one": {ID: "one", Efforts: []string{"low", "high"}}, "two": {ID: "two", Efforts: []string{"low"}}}
	s.setupMu.Unlock()
	post := func(body string) int {
		r := httptest.NewRequest("POST", "/api/model-preferences", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.setModelPreferences(w, r)
		return w.Code
	}
	if got := post(`{"preferences":{"reasoning":"one"},"reasoning_efforts":{"reasoning":"high"}}`); got != 204 {
		t.Fatalf("valid %d", got)
	}
	if got := post(`{"preferences":{"reasoning":"two"},"reasoning_efforts":{"reasoning":"high"}}`); got != 400 {
		t.Fatalf("unsupported pair %d", got)
	}
	if got := post(`{"preferences":{"reasoning":"two"}}`); got != 204 {
		t.Fatalf("legacy switch %d", got)
	}
	p, err := readReasoningPreferences(s.db)
	if err != nil || p.Reasoning != "" {
		t.Fatalf("stale effort %q %v", p.Reasoning, err)
	}
	if got := post(`{"preferences":{"reasoning":"one"},"reasoning_efforts":{"reasoning":"high"}}`); got != 204 {
		t.Fatal(got)
	}
	s.setupMu.Lock()
	s.modelsAt = time.Time{}
	s.setupMu.Unlock()
	if got := post(`{"preferences":{"reasoning":"one"},"reasoning_efforts":{"reasoning":"high"}}`); got != 204 {
		t.Fatalf("preserve stale %d", got)
	}
	if got := post(`{"preferences":{"reasoning":"one"},"reasoning_efforts":{}}`); got != 204 {
		t.Fatalf("clear stale %d", got)
	}
}
func TestReasoningNoticeUsesKnownEffectiveLevel(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if _, err := s.db.Exec(`UPDATE model_preferences SET reasoning='one' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(`UPDATE reasoning_preferences SET reasoning='high' WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	s.setupMu.Lock()
	s.models = []string{"one"}
	s.modelsSource = "fixture"
	s.modelsAt = time.Now()
	s.modelDetails = map[string]runtimeapi.ModelDescriptor{"one": {ID: "one", DefaultEffort: "high", Efforts: []string{"high", "ultra"}}}
	s.setupMu.Unlock()
	if err := s.write(func(tx *sql.Tx) error { return s.notifyModelDeparture(tx, "", "", "reasoning", "one", "", "", "") }); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient='operator' AND role='system'`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("equivalent default should not notify: %d %v", n, err)
	}
	if err := s.write(func(tx *sql.Tx) error { return s.notifyModelDeparture(tx, "", "", "reasoning", "one", "ultra", "", "") }); err != nil {
		t.Fatal(err)
	}
	var text string
	if err := s.db.QueryRow(`SELECT text FROM messages WHERE sender='root' AND recipient='operator' AND role='system'`).Scan(&text); err != nil || !strings.Contains(text, "preferred reasoning high; selected reasoning ultra") {
		t.Fatalf("notice %q %v", text, err)
	}
}
func TestUnavailableExplicitEffortCreatesOneHonestNotice(t *testing.T) {
	s := openTest(t, t.TempDir())
	defer s.Close()
	if err := s.write(func(tx *sql.Tx) error { return s.notifyModelDeparture(tx, "", "", "", "model", "", "ultra", "") }); err != nil {
		t.Fatal(err)
	}
	var n int
	var text string
	if err := s.db.QueryRow(`SELECT count(*),COALESCE(max(text),'') FROM messages WHERE sender='root' AND recipient='operator' AND role='system'`).Scan(&n, &text); err != nil || n != 1 || !strings.Contains(text, "reasoning ultra was unavailable; model default is used") {
		t.Fatalf("notice=%d %q %v", n, text, err)
	}
}
func TestUnavailableEffortNoticeFollowsActualDispatch(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	s.setupMu.Lock()
	s.models = []string{"model"}
	s.modelsSource = "fixture"
	s.modelsAt = time.Now()
	s.modelDetails = map[string]runtimeapi.ModelDescriptor{"model": {ID: "model", Efforts: []string{"low"}}}
	s.setupMu.Unlock()
	j := activeAssignment(t, s, task, po)
	actions := []runtimeapi.Action{{Type: "create_worker", Title: "EffortWorker", Model: "model", Effort: "ultra"}, {Type: "queue_work_item", Title: "Task", Body: "Authorized work.", Recipient: "EffortWorker", Category: "reasoning"}}
	if err := s.complete(j, runtimeapi.Result{Actions: actions}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var n int
	var text string
	if err := s.db.QueryRow(`SELECT count(*),COALESCE(max(text),'') FROM messages WHERE sender='root' AND recipient='operator' AND role='system' AND text LIKE '%reasoning ultra was unavailable%'`).Scan(&n, &text); err != nil || n != 1 {
		t.Fatalf("notice=%d %q %v", n, text, err)
	}
	if err := s.write(func(tx *sql.Tx) error {
		owner, err := getAgent(tx, po)
		if err != nil {
			return err
		}
		return s.queuePOWorkItem(tx, j, owner, runtimeapi.Action{Title: "Second task", Body: "Independent work.", Recipient: "EffortWorker", Category: "reasoning"})
	}); err != nil {
		t.Fatal(err)
	}
	var pending int
	if err := s.db.QueryRow(`SELECT count(*) FROM worker_effort_fallbacks`).Scan(&pending); err != nil || pending != 0 {
		t.Fatalf("fallback receipt not consumed: %d %v", pending, err)
	}
	if err := s.db.QueryRow(`SELECT count(*) FROM messages WHERE sender='root' AND recipient='operator' AND role='system' AND text LIKE '%reasoning ultra was unavailable%'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("reused worker duplicated fallback notice: %d %v", n, err)
	}
	work, err := s.claim(map[string]bool{po: true, root: true})
	if err != nil || work == nil || work.Kind != "work" || work.Request.Effort != "" {
		t.Fatalf("expected model default request: %+v %v", work, err)
	}
}
