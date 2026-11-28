package service

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"
	"unicode"

	"agentchat/src/internal/runtimeapi"
)

type modelPreferences struct {
	Reasoning      string `json:"reasoning"`
	Implementation string `json:"implementation"`
	PreciseEdits   string `json:"precise_edits"`
	Research       string `json:"research"`
	RoutineChecks  string `json:"routine_checks"`
}

var localModelName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*(/[A-Za-z0-9][A-Za-z0-9._-]*)*$`)

func validLocalModel(model string) bool {
	return len(model) <= 200 && localModelName.MatchString(model)
}
func qualifiedModelID(provider, model string) string {
	return "@provider:" + base64.RawURLEncoding.EncodeToString([]byte(provider)) + ":" + base64.RawURLEncoding.EncodeToString([]byte(model))
}
func splitModelID(id string) (string, string) {
	if !strings.HasPrefix(id, "@provider:") {
		return "", id
	}
	parts := strings.Split(strings.TrimPrefix(id, "@provider:"), ":")
	if len(parts) != 2 {
		return "", ""
	}
	p, e1 := base64.RawURLEncoding.DecodeString(parts[0])
	m, e2 := base64.RawURLEncoding.DecodeString(parts[1])
	if e1 != nil || e2 != nil || qualifiedModelID(string(p), string(m)) != id {
		return "", ""
	}
	return string(p), string(m)
}

type localProvider struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Enabled   bool   `json:"enabled"`
	Port      int    `json:"port"`
	Available bool   `json:"available"`
}

func readLocalProviders(q querier) ([]localProvider, error) {
	return querySlice(q, `SELECT id,name,enabled,port FROM local_model_providers ORDER BY name,id`, func(r *sql.Rows) (localProvider, error) {
		var v localProvider
		err := r.Scan(&v.ID, &v.Name, &v.Enabled, &v.Port)
		return v, err
	})
}
func (s *Service) setLocalModelProvider(w http.ResponseWriter, r *http.Request) {
	var v localProvider
	if err := requestJSON(w, r, &v); err != nil || v.Port < 1 || v.Port > 65535 || len(v.Name) > 80 || strings.TrimSpace(v.Name) == "" || strings.IndexFunc(v.Name, unicode.IsControl) >= 0 || (v.ID != "" && (!strings.HasPrefix(v.ID, "local:") || !clientID.MatchString(strings.TrimPrefix(v.ID, "local:")))) {
		http.Error(w, "Invalid local provider settings.", 400)
		return
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	created := v.ID == ""
	if created {
		v.ID = "local:" + newID()
	}
	err := s.write(func(tx *sql.Tx) error {
		if created {
			var n int
			if err := tx.QueryRow(`SELECT count(*) FROM local_model_providers`).Scan(&n); err != nil {
				return err
			}
			if n >= 8 {
				return errConflict
			}
			_, err := tx.Exec(`INSERT INTO local_model_providers(id,name,enabled,port) VALUES(?,?,?,?)`, v.ID, v.Name, v.Enabled, v.Port)
			return err
		}
		res, err := tx.Exec(`UPDATE local_model_providers SET name=?,enabled=?,port=? WHERE id=?`, v.Name, v.Enabled, v.Port, v.ID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n != 1 {
			return errAbsent
		}
		if _, err = tx.Exec(`INSERT INTO state_feed(kind,entity) SELECT 'agent',b.agent_id FROM agent_model_providers b WHERE b.provider=? ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version`, v.ID); err != nil {
			return err
		}
		if _, err = tx.Exec(`INSERT INTO state_feed(kind,entity) SELECT 'job',j.id FROM jobs j JOIN agent_model_providers b ON b.agent_id=j.agent_id WHERE b.provider=? AND j.status='pending' ON CONFLICT(kind,entity) DO UPDATE SET version=excluded.version`, v.ID); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		httpFailure(w, err)
		return
	}
	s.setupMu.Lock()
	s.modelsConfigGeneration++
	s.modelsAt = time.Time{}
	s.setupMu.Unlock()
	s.signal()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		ID string `json:"id"`
	}{v.ID})
}

func (p modelPreferences) values() []string {
	return []string{p.Reasoning, p.Implementation, p.PreciseEdits, p.Research, p.RoutineChecks}
}
func readModelPreferences(q interface{ QueryRow(string, ...any) *sql.Row }) (p modelPreferences, err error) {
	err = q.QueryRow(`SELECT reasoning,implementation,precise_edits,research,routine_checks FROM model_preferences WHERE id=1`).Scan(&p.Reasoning, &p.Implementation, &p.PreciseEdits, &p.Research, &p.RoutineChecks)
	return
}
func readReasoningPreferences(q interface{ QueryRow(string, ...any) *sql.Row }) (p modelPreferences, err error) {
	err = q.QueryRow(`SELECT reasoning,implementation,precise_edits,research,routine_checks FROM reasoning_preferences WHERE id=1`).Scan(&p.Reasoning, &p.Implementation, &p.PreciseEdits, &p.Research, &p.RoutineChecks)
	return
}
func modelEffortAllowed(details map[string]runtimeapi.ModelDescriptor, id, effort string) bool {
	if effort == "" {
		return true
	}
	for _, v := range details[id].Efforts {
		if v == effort {
			return true
		}
	}
	return false
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
	if source == "" || source != s.modelsSource || generation != s.modelsGeneration || s.modelsConfigGeneration != s.modelsLoadedConfigGeneration || time.Since(s.modelsAt) > 2*time.Minute || s.modelsAt.IsZero() {
		return nil, false
	}
	return append([]string(nil), s.models...), true
}
func (s *Service) cachedModelDetails() map[string]runtimeapi.ModelDescriptor {
	if _, ok := s.cachedModels(); !ok {
		return nil
	}
	s.setupMu.RLock()
	defer s.setupMu.RUnlock()
	out := make(map[string]runtimeapi.ModelDescriptor, len(s.modelDetails))
	for id, d := range s.modelDetails {
		d.Efforts = append([]string(nil), d.Efforts...)
		out[id] = d
	}
	return out
}
func (s *Service) refreshModels(ctx context.Context) ([]string, bool) {
	source := s.currentModelSource()
	s.usageMu.RLock()
	generation := s.usageGeneration
	s.usageMu.RUnlock()
	s.setupMu.RLock()
	catalog := s.modelCatalog
	configGeneration := s.modelsConfigGeneration
	s.setupMu.RUnlock()
	if source == "" || catalog == nil {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	providers, err := readLocalProviders(s.db)
	if err != nil {
		return nil, false
	}
	type catalogResult struct {
		provider string
		models   []string
		details  []runtimeapi.ModelDescriptor
	}
	results := make(chan catalogResult, len(providers)+1)
	count := 1
	go func() {
		if detailed, ok := catalog.(runtimeapi.DetailedModelCatalog); ok {
			details, err := detailed.ModelDetails(ctx)
			if err == nil && len(details) > 0 {
				models := make([]string, 0, len(details))
				for _, d := range details {
					models = append(models, d.ID)
				}
				results <- catalogResult{"", models, details}
				return
			}
		}
		models, err := catalog.Models(ctx)
		if err != nil {
			models = nil
		}
		results <- catalogResult{"", models, nil}
	}()
	if local, ok := catalog.(runtimeapi.LocalModelCatalog); ok {
		for _, p := range providers {
			if !p.Enabled {
				continue
			}
			count++
			go func(p localProvider) {
				models, err := local.LocalModels(ctx, p.Port)
				if err != nil {
					models = nil
				}
				results <- catalogResult{p.ID, models, nil}
			}(p)
		}
	}
	var models []string
	details := map[string]runtimeapi.ModelDescriptor{}
collect:
	for i := 0; i < count; i++ {
		select {
		case result := <-results:
			for _, d := range result.details {
				details[d.ID] = d
			}
			for _, model := range result.models {
				if result.provider == "" {
					models = append(models, model)
				} else if validLocalModel(model) {
					models = append(models, qualifiedModelID(result.provider, model))
				}
			}
		case <-ctx.Done():
			break collect
		}
	}

	s.usageMu.RLock()
	currentGeneration := s.usageGeneration
	s.usageMu.RUnlock()
	if len(models) == 0 || source != s.currentModelSource() || generation != currentGeneration {
		return nil, false
	}
	seen := map[string]bool{}
	clean := make([]string, 0, len(models))
	for _, id := range models {
		if id == "" || len(id) > 600 || strings.TrimSpace(id) != id || seen[id] {
			continue
		}
		seen[id] = true
		clean = append(clean, id)
	}
	if len(clean) == 0 {
		return nil, false
	}
	s.setupMu.Lock()
	if configGeneration != s.modelsConfigGeneration {
		s.setupMu.Unlock()
		return nil, false
	}
	s.models = append([]string(nil), clean...)
	s.modelDetails = details
	s.modelsSource = source
	s.modelsGeneration = generation
	s.modelsLoadedConfigGeneration = configGeneration
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
	efforts, err := readReasoningPreferences(s.db)
	if err != nil {
		http.Error(w, "Preferences unavailable.", 500)
		return
	}
	localProviders, err := readLocalProviders(s.db)
	if err != nil {
		http.Error(w, "Provider settings unavailable.", 500)
		return
	}

	models, ok := s.cachedModels()
	if !ok {
		models, ok = s.refreshModels(r.Context())
	}
	details := s.cachedModelDetails()
	type model struct {
		ID                     string   `json:"id"`
		Model                  string   `json:"model"`
		ProviderID             string   `json:"provider_id"`
		ProviderLabel          string   `json:"provider_label"`
		Available              bool     `json:"available"`
		ReasoningEfforts       []string `json:"reasoning_efforts"`
		DefaultReasoningEffort string   `json:"default_reasoning_effort,omitempty"`
	}
	out := make([]model, 0, len(models))
	seen := map[string]bool{}
	labels := map[string]string{"": "Default Codex"}
	for _, p := range localProviders {
		labels[p.ID] = p.Name
	}
	defaultAvailable := false
	appendModel := func(id string, available bool) {
		if id == "" || seen[id] {
			return
		}
		provider, slug := splitModelID(id)
		if slug == "" {
			return
		}
		label := labels[provider]
		if label == "" {
			label = "Unavailable provider"
		}
		d := details[id]
		levels := append([]string{}, d.Efforts...)
		out = append(out, model{id, slug, provider, label, available, levels, d.DefaultEffort})
		seen[id] = true
		if available && provider == "" {
			defaultAvailable = true
		}
		if available {
			for i := range localProviders {
				if localProviders[i].ID == provider {
					localProviders[i].Available = true
				}
			}
		}
	}
	for _, id := range models {
		appendModel(id, true)
	}
	for _, id := range p.values() {
		appendModel(id, false)
	}
	type providerStatus struct {
		ID        string `json:"id"`
		Label     string `json:"label"`
		Available bool   `json:"available"`
	}
	providers := []providerStatus{{"", "Default Codex", defaultAvailable}}
	for _, p := range localProviders {
		providers = append(providers, providerStatus{p.ID, p.Name, p.Available})
	}

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(struct {
		Preferences      modelPreferences `json:"preferences"`
		ReasoningEfforts modelPreferences `json:"reasoning_efforts"`
		Models           []model          `json:"models"`
		CatalogAvailable bool             `json:"catalog_available"`
		LocalProviders   []localProvider  `json:"local_providers"`
		Providers        []providerStatus `json:"providers"`
	}{p, efforts, out, ok, localProviders, providers})
}
func (s *Service) setModelPreferences(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Preferences      modelPreferences  `json:"preferences"`
		ReasoningEfforts *modelPreferences `json:"reasoning_efforts"`
	}
	if err := requestJSON(w, r, &in); err != nil {
		http.Error(w, "Invalid preferences.", 400)
		return
	}
	s.startMu.Lock()
	defer s.startMu.Unlock()
	models, ok := s.cachedModels()
	details := s.cachedModelDetails()
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
		oldEfforts, err := readReasoningPreferences(tx)
		if err != nil {
			return err
		}
		newEfforts := oldEfforts.values()
		if in.ReasoningEfforts != nil {
			newEfforts = in.ReasoningEfforts.values()
		} else {
			for i, v := range p.values() {
				if v != before[i] {
					newEfforts[i] = ""
				}
			}
		}
		for i, v := range p.values() {
			if v != "" && v != before[i] && (!ok || !allowed[v]) {
				return errInvalid
			}
			effort := newEfforts[i]
			if effort != "" && (v == "" || ((v != before[i] || effort != oldEfforts.values()[i]) && (!ok || !modelEffortAllowed(details, v, effort)))) {
				return errInvalid
			}
		}
		_, err = tx.Exec(`UPDATE model_preferences SET reasoning=?,implementation=?,precise_edits=?,research=?,routine_checks=? WHERE id=1`, p.Reasoning, p.Implementation, p.PreciseEdits, p.Research, p.RoutineChecks)
		if err != nil {
			return err
		}
		_, err = tx.Exec(`UPDATE reasoning_preferences SET reasoning=?,implementation=?,precise_edits=?,research=?,routine_checks=? WHERE id=1`, newEfforts[0], newEfforts[1], newEfforts[2], newEfforts[3], newEfforts[4])
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
	effortPrefs, err := readReasoningPreferences(tx)
	if err != nil {
		return "", err
	}
	details := s.cachedModelDetails()
	allowed := map[string]bool{}
	for _, m := range models {
		allowed[m] = true
	}
	labels := []string{"reasoning", "implementation", "precise edits", "research", "routine checks"}
	var entries []string
	for i, v := range p.values() {
		if v != "" && allowed[v] {
			value := labels[i] + ": " + displayModelID(v)
			effort := effortPrefs.values()[i]
			if effort != "" && modelEffortAllowed(details, v, effort) {
				value += " (reasoning " + effort + ")"
			}
			entries = append(entries, value)
		}
	}
	roster, err := querySlice(tx, `SELECT a.name,a.model,COALESCE(p.provider,''),a.effort FROM agents a LEFT JOIN agent_model_providers p ON p.agent_id=a.id WHERE a.project_id=? AND a.role='worker' ORDER BY a.name`, func(rows *sql.Rows) (string, error) {
		var name, model, provider, effort string
		err := rows.Scan(&name, &model, &provider, &effort)
		if provider != "" {
			model = provider + " / " + model
		}
		if effort == "" {
			effort = "model default"
		}
		return name + " (" + model + ", reasoning " + effort + ")", err
	}, project)
	if err != nil {
		return "", err
	}
	for i := range models {
		id := models[i]
		models[i] = displayModelID(id)
		if len(details[id].Efforts) > 0 {
			models[i] += " [reasoning: " + strings.Join(details[id].Efforts, ", ") + "]"
		}
	}
	return "\nPreferred models for new workers: " + strings.Join(entries, "; ") + ". Use judgment for each task and reuse a suitable existing worker where possible. Available models: " + strings.Join(models, ", ") + ". For a named provider use its exact ID in provider and its exact model slug in model; for default Codex leave provider empty. For a new worker, optional effort must be an advertised level for that exact provider/model; leave it empty for model default. On queue_work_item, include category (reasoning, implementation, precise_edits, research, routine_checks) and, if choosing a different model or reasoning level, a concise model_reason suitable for the operator; do not include private inputs. Preferences are not restrictions. Existing worker models: " + strings.Join(roster, ", ") + ".", nil
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
func (s *Service) notifyModelDeparture(tx *sql.Tx, project, task, category, chosen, chosenEffort, omittedEffort, reason string) error {
	p, err := readModelPreferences(tx)
	if err != nil {
		return err
	}
	preferred := modelCategory(p, category)
	effortPrefs, err := readReasoningPreferences(tx)
	if err != nil {
		return err
	}
	preferredEffort := modelCategory(effortPrefs, category)
	effectiveEffort := chosenEffort
	if effectiveEffort == "" {
		effectiveEffort = s.cachedModelDetails()[chosen].DefaultEffort
	}
	var text string
	if preferred != "" && preferred != chosen {
		text = fmt.Sprintf("Model choice for project %s, task %s: preferred %s; selected %s.", project, task, displayModelID(preferred), displayModelID(chosen))
		if preferredEffort != "" {
			actual := effectiveEffort
			if actual == "" {
				actual = "model default"
			}
			text += fmt.Sprintf(" Preferred reasoning %s; selected reasoning %s.", preferredEffort, actual)
		}
	} else if preferred != "" && preferred == chosen && preferredEffort != "" && effectiveEffort != "" && preferredEffort != effectiveEffort {
		text = fmt.Sprintf("Model choice for project %s, task %s: %s; preferred reasoning %s; selected reasoning %s.", project, task, displayModelID(chosen), preferredEffort, effectiveEffort)
	} else if preferred != "" && preferred == chosen && preferredEffort != "" && effectiveEffort == "" {
		text = fmt.Sprintf("Model choice for project %s, task %s: %s; preferred reasoning %s; selected model default (unverified).", project, task, displayModelID(chosen), preferredEffort)
	} else if category != "reasoning" && category != "implementation" && category != "precise_edits" && category != "research" && category != "routine_checks" {
		differentPreference := false
		for _, v := range p.values() {
			differentPreference = differentPreference || (v != "" && v != chosen)
		}
		if differentPreference {
			text = fmt.Sprintf("Model choice for project %s, task %s: selected %s; no matching task category was supplied for the saved preferences.", project, task, displayModelID(chosen))
		}
	}
	if omittedEffort != "" {
		if text == "" {
			text = fmt.Sprintf("Model choice for project %s, task %s: selected %s.", project, task, displayModelID(chosen))
		}
		if len(omittedEffort) > 32 || strings.IndexFunc(omittedEffort, func(r rune) bool { return !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '-' || r == '_') }) >= 0 {
			omittedEffort = "requested"
		}
		text += fmt.Sprintf(" Requested reasoning %s was unavailable; model default is used.", omittedEffort)
	}
	if text == "" {
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
func displayModelID(id string) string {
	provider, model := splitModelID(id)
	if provider != "" {
		return provider + " / " + model
	}
	return model
}
