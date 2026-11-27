package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode"
)

type modelPreferences struct {
	Reasoning      string `json:"reasoning"`
	Implementation string `json:"implementation"`
	PreciseEdits   string `json:"precise_edits"`
	Research       string `json:"research"`
	RoutineChecks  string `json:"routine_checks"`
}

func (p modelPreferences) values() []string {
	return []string{p.Reasoning, p.Implementation, p.PreciseEdits, p.Research, p.RoutineChecks}
}
func readModelPreferences(q interface{ QueryRow(string, ...any) *sql.Row }) (p modelPreferences, err error) {
	err = q.QueryRow(`SELECT reasoning,implementation,precise_edits,research,routine_checks FROM model_preferences WHERE id=1`).Scan(&p.Reasoning, &p.Implementation, &p.PreciseEdits, &p.Research, &p.RoutineChecks)
	return
}
func (s *Service) currentModelSource() string {
	if setup := s.runtimeSetup(); setup != nil {
		st := setup.SetupStatus()
		if st.State != "ready" {
			return ""
		}
		return st.Source
	}
	return "fixture"
}
func (s *Service) cachedModels() ([]string, bool) {
	source := s.currentModelSource()
	s.usageMu.RLock()
	generation := s.usageGeneration
	s.usageMu.RUnlock()
	s.setupMu.RLock()
	defer s.setupMu.RUnlock()
	if source == "" || source != s.modelsSource || generation != s.modelsGeneration || time.Since(s.modelsAt) > 2*time.Minute || s.modelsAt.IsZero() {
		return nil, false
	}
	return append([]string(nil), s.models...), true
}
func (s *Service) refreshModels(ctx context.Context) ([]string, bool) {
	source := s.currentModelSource()
	s.usageMu.RLock()
	generation := s.usageGeneration
	s.usageMu.RUnlock()
	s.setupMu.RLock()
	catalog := s.modelCatalog
	s.setupMu.RUnlock()
	if source == "" || catalog == nil {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	models, err := catalog.Models(ctx)
	s.usageMu.RLock()
	currentGeneration := s.usageGeneration
	s.usageMu.RUnlock()
	if err != nil || len(models) == 0 || source != s.currentModelSource() || generation != currentGeneration {
		return nil, false
	}
	seen := map[string]bool{}
	clean := make([]string, 0, len(models))
	for _, id := range models {
		if id == "" || len(id) > 200 || strings.TrimSpace(id) != id || seen[id] {
			continue
		}
		seen[id] = true
		clean = append(clean, id)
	}
	if len(clean) == 0 {
		return nil, false
	}
	s.setupMu.Lock()
	s.models = append([]string(nil), clean...)
	s.modelsSource = source
	s.modelsGeneration = generation
	s.modelsAt = time.Now()
	s.setupMu.Unlock()
	return clean, true
}
func (s *Service) getModelPreferences(w http.ResponseWriter, r *http.Request) {
	p, err := readModelPreferences(s.db)
	if err != nil {
		http.Error(w, "Preferences unavailable.", 500)
		return
	}
	models, ok := s.cachedModels()
	if !ok {
		models, ok = s.refreshModels(r.Context())
	}
	type model struct {
		ID string `json:"id"`
	}
	out := make([]model, 0, len(models))
	for _, m := range models {
		out = append(out, model{m})
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Preferences      modelPreferences `json:"preferences"`
		Models           []model          `json:"models"`
		CatalogAvailable bool             `json:"catalog_available"`
	}{p, out, ok})
}
func (s *Service) setModelPreferences(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Preferences modelPreferences `json:"preferences"`
	}
	if err := requestJSON(w, r, &in); err != nil {
		http.Error(w, "Invalid preferences.", 400)
		return
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	models, ok := s.cachedModels()
	allowed := map[string]bool{}
	for _, m := range models {
		allowed[m] = true
	}
	p := in.Preferences
	err := s.write(func(tx *sql.Tx) error {
		old, err := readModelPreferences(tx)
		if err != nil {
			return err
		}
		before := old.values()
		for i, v := range p.values() {
			if v != "" && v != before[i] && (!ok || !allowed[v]) {
				return errInvalid
			}
		}
		_, err = tx.Exec(`UPDATE model_preferences SET reasoning=?,implementation=?,precise_edits=?,research=?,routine_checks=? WHERE id=1`, p.Reasoning, p.Implementation, p.PreciseEdits, p.Research, p.RoutineChecks)
		return err
	})
	if err != nil {
		if err == errInvalid {
			http.Error(w, "Selected model is unavailable.", 400)
			return
		}
		http.Error(w, "Preferences unavailable.", 500)
		return
	}
	w.WriteHeader(204)
}
func (s *Service) preferredWorkerModel(tx *sql.Tx, requested string) (string, error) {
	if requested == "" {
		return s.opts.WorkerModel, nil
	}
	models, ok := s.cachedModels()
	if !ok {
		return s.opts.WorkerModel, nil
	}
	available := false
	for _, v := range models {
		available = available || v == requested
	}
	if !available {
		return s.opts.WorkerModel, nil
	}
	return requested, nil
}
func (s *Service) pollModels(ctx context.Context) {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		s.refreshModels(ctx)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}
func (s *Service) modelPreferenceGuidance(tx *sql.Tx, project string) (string, error) {
	p, err := readModelPreferences(tx)
	if err != nil {
		return "", err
	}
	models, ok := s.cachedModels()
	if !ok {
		return "", nil
	}
	allowed := map[string]bool{}
	for _, m := range models {
		allowed[m] = true
	}
	labels := []string{"reasoning", "implementation", "precise edits", "research", "routine checks"}
	var entries []string
	for i, v := range p.values() {
		if v != "" && allowed[v] {
			entries = append(entries, labels[i]+": "+v)
		}
	}
	roster, err := querySlice(tx, `SELECT name,model FROM agents WHERE project_id=? AND role='worker' ORDER BY name`, func(rows *sql.Rows) (string, error) {
		var name, model string
		err := rows.Scan(&name, &model)
		return name + " (" + model + ")", err
	}, project)
	if err != nil {
		return "", err
	}
	return "\nPreferred models for new workers: " + strings.Join(entries, "; ") + ". Use judgment for each task and reuse a suitable existing worker where possible. Available models: " + strings.Join(models, ", ") + ". Select any suitable available model by exact ID in the model field when creating a worker, or leave it empty for the configured default. On queue_work_item, include category (reasoning, implementation, precise_edits, research, routine_checks) and, if choosing a different model, a concise model_reason suitable for the operator; do not include private inputs. Preferences are not restrictions. Existing worker models: " + strings.Join(roster, ", ") + ".", nil
}
func modelCategory(p modelPreferences, key string) string {
	switch key {
	case "reasoning":
		return p.Reasoning
	case "implementation":
		return p.Implementation
	case "precise_edits":
		return p.PreciseEdits
	case "research":
		return p.Research
	case "routine_checks":
		return p.RoutineChecks
	}
	return ""
}
func (s *Service) notifyModelDeparture(tx *sql.Tx, project, task, category, chosen, reason string) error {
	p, err := readModelPreferences(tx)
	if err != nil {
		return err
	}
	preferred := modelCategory(p, category)
	var text string
	if preferred != "" && preferred != chosen {
		text = fmt.Sprintf("Model choice for project %s, task %s: preferred %s; selected %s.", project, task, preferred, chosen)
	} else if category != "reasoning" && category != "implementation" && category != "precise_edits" && category != "research" && category != "routine_checks" {
		differentPreference := false
		for _, v := range p.values() {
			differentPreference = differentPreference || (v != "" && v != chosen)
		}
		if !differentPreference {
			return nil
		}
		text = fmt.Sprintf("Model choice for project %s, task %s: selected %s; no matching task category was supplied for the saved preferences.", project, task, chosen)
	} else {
		return nil
	}
	reason = strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, reason))
	if reason != "" && len(reason) <= 300 {
		text += " Reason: " + reason
	} else {
		text += " Reason was not provided."
	}
	return addMessage(tx, newID(), root, operator, "system", text, project, task)
}
