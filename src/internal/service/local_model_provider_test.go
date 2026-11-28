package service

import (
	"agentchat/src/internal/runtimeapi"
	"context"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
)

type providerTestCatalog struct{}

func (providerTestCatalog) Models(context.Context) ([]string, error) {
	return []string{"shared-model"}, nil
}
func (providerTestCatalog) LocalModels(context.Context, int) ([]string, error) {
	return []string{"shared-model", "mlx-community/Qwen"}, nil
}
func TestLocalProviderCatalogBindingAndPendingFence(t *testing.T) {
	s, task, po, _ := continuationFixture(t)
	defer s.Close()
	s.setupMu.Lock()
	s.modelCatalog = providerTestCatalog{}
	s.setupMu.Unlock()
	req := httptest.NewRequest("POST", "/api/model-providers/local", strings.NewReader(`{"id":"","name":"Local one","enabled":true,"port":8000}`))
	req.Header.Set("Content-Type", "application/json")
	res := httptest.NewRecorder()
	s.setLocalModelProvider(res, req)
	if res.Code != 200 {
		t.Fatalf("provider save %d %s", res.Code, res.Body.String())
	}
	var provider string
	if err := s.db.QueryRow(`SELECT id FROM local_model_providers WHERE name='Local one'`).Scan(&provider); err != nil {
		t.Fatal(err)
	}
	models, ok := s.refreshModels(context.Background())
	if !ok {
		t.Fatal("catalog unavailable")
	}
	localID := qualifiedModelID(provider, "shared-model")
	foundDefault, foundLocal := false, false
	for _, m := range models {
		foundDefault = foundDefault || m == "shared-model"
		foundLocal = foundLocal || m == localID
	}
	if !foundDefault || !foundLocal || localID == "shared-model" {
		t.Fatalf("missing distinct models %v", models)
	}
	j := activeAssignment(t, s, task, po)
	if err := s.complete(j, runtimeapi.Result{Actions: []runtimeapi.Action{{Type: "create_worker", Title: "LocalWorker", Provider: provider, Model: "shared-model"}, {Type: "queue_work_item", Title: "Local work", Body: "Scoped work", Recipient: "LocalWorker"}}}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	var worker string
	if err := s.db.QueryRow(`SELECT id FROM agents WHERE name='LocalWorker'`).Scan(&worker); err != nil {
		t.Fatal(err)
	}
	change := func(enabled bool, port int) {
		t.Helper()
		body := fmt.Sprintf(`{"id":%q,"name":"Local one","enabled":%t,"port":%d}`, provider, enabled, port)
		r := httptest.NewRequest("POST", "/api/model-providers/local", strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		s.setLocalModelProvider(w, r)
		if w.Code != 200 {
			t.Fatalf("provider update: %d %s", w.Code, w.Body.String())
		}
	}
	blocked := map[string]bool{po: true, root: true}
	for _, p := range []struct {
		enabled bool
		port    int
	}{{false, 8000}, {true, 8001}} {
		change(p.enabled, p.port)
		view, err := s.state()
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, job := range view.Jobs {
			if job.Agent == worker && job.Status == "pending" && job.ProviderUnavailable {
				found = true
			}
		}
		if !found {
			t.Fatal("pending provider unavailability was not projected")
		}
		j, err := s.claim(blocked)
		if err != nil || j != nil {
			t.Fatalf("changed/disabled route claimed: %v %v", j, err)
		}
	}
	change(true, 8000)
	claimed, err := s.claim(blocked)
	if err != nil || claimed == nil {
		t.Fatalf("unchanged restored route not claimed: %v", err)
	}
	if claimed.Agent != worker || claimed.Request.Provider != "agentchat-local" || claimed.Request.ProviderID != provider || claimed.Request.ProviderPort != 8000 || claimed.Request.Model != "shared-model" {
		t.Fatalf("wrong bound request: %+v", claimed.Request)
	}
}
